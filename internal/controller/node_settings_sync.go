package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

const nodeSettingsFinalizer = "nodes.example.com/settings-cleanup"

type poolScopeError struct {
	cause error
}

func (e *poolScopeError) Error() string { return e.cause.Error() }
func (e *poolScopeError) Unwrap() error { return e.cause }

func isPoolScopeError(err error) bool {
	var scoped *poolScopeError
	return errors.As(err, &scoped)
}

func joinNodeFailures(failures map[string]error) error {
	var result []error
	for _, name := range nodeSettingsKeys(failures, nil) {
		result = append(result, failures[name])
	}
	return errors.Join(result...)
}

func (r *NodePoolReconciler) syncNodeSettings(
	ctx context.Context, pool *nodesv1alpha1.NodePool,
) (map[string]error, error) {
	// Membership alone cannot find Nodes that have left the pool.
	var nodes corev1.NodeList
	if err := r.APIReader.List(ctx, &nodes); err != nil {
		return nil, fmt.Errorf("list Nodes for settings synchronization: %w", err)
	}
	sort.Slice(nodes.Items, func(i, j int) bool {
		return nodes.Items[i].Name < nodes.Items[j].Name
	})
	// Names fence even a replacement UID for this pass; every retry reads fresh state.
	failures := make(map[string]error)
	for i := range nodes.Items {
		if err := ctx.Err(); err != nil {
			return failures, err
		}
		listed := &nodes.Items[i]
		_, recorded := listed.Annotations[nodeSettingsAnnotation]
		_, cordoned := listed.Annotations[nodeCordonAnnotation]
		if listed.Labels[membershipLabel] != pool.Name && !recorded && !cordoned {
			continue
		}
		if err := r.syncListedNodeSettings(ctx, pool, listed); err != nil {
			// A changed pool invalidates the whole pass. Node-local failures
			// are collected so other independent Nodes can still converge.
			if isPoolScopeError(err) {
				return failures, err
			}
			if ctx.Err() != nil {
				return failures, ctx.Err()
			}
			failures[listed.Name] = err
		}
	}
	return failures, nil
}

func (r *NodePoolReconciler) syncListedNodeSettings(
	ctx context.Context, pool *nodesv1alpha1.NodePool, listed *corev1.Node,
) error {
	var node corev1.Node
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: listed.Name}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read Node %q (%s): %w", listed.Name, listed.UID, err)
	}
	if node.UID != listed.UID {
		return fmt.Errorf("Node %q was replaced during synchronization; retry with its new UID", node.Name)
	}
	if err := r.cleanupNodeCordon(ctx, pool, &node); err != nil {
		return fmt.Errorf("clean up Node %q (%s) cordon: %w", node.Name, node.UID, err)
	}
	if err := r.syncOneNodeSettings(ctx, pool, &node); err != nil {
		return fmt.Errorf("synchronize Node %q (%s): %w", node.Name, node.UID, err)
	}
	return nil
}

func (r *NodePoolReconciler) syncOneNodeSettings(
	ctx context.Context, pool *nodesv1alpha1.NodePool, node *corev1.Node,
) error {
	deleting := !pool.DeletionTimestamp.IsZero()
	// The assignment limits pool-deletion cleanup to still-Ready Nodes.
	if deleting && !nodeIsReady(node) {
		return nil
	}
	member := node.Labels[membershipLabel] == pool.Name
	owned, err := nodeSettingsOwnedBy(node, pool.UID)
	if err != nil {
		if member || nodeSettingsPreviouslyObserved(pool, node.UID) {
			return err
		}
		// Do not block this pool on an unreadable record we cannot attribute to it.
		log.FromContext(ctx).Info("Skipping unrelated Node with unreadable settings ownership",
			"node", node.Name, "error", err)
		return nil
	}

	var labels map[string]string
	var taints []corev1.Taint
	if !deleting && member {
		// Ready=False, Ready=Unknown and missing Ready do not prevent API writes.
		labels, taints = pool.Spec.Labels, pool.Spec.Taints
	} else if !owned {
		return nil
	}
	if protectedNode(node) && owned && len(labels)+len(taints) == 0 && !nodeIsReady(node) {
		return fmt.Errorf("protected Node %q requires Ready=True before owned settings cleanup", node.Name)
	}
	desired, err := planNodeSettings(node, pool.UID, labels, taints)
	if err != nil {
		return err
	}
	if equality.Semantic.DeepEqual(node, desired) {
		return nil
	}

	// Fence known pool changes before a write; cross-object writes are not atomic.
	if err := r.checkSettingsPool(ctx, pool); err != nil {
		return err
	}
	// The Node version check rejects intervening edits. Settings and their
	// ownership record are written together, never as two separate requests.
	if err := r.Patch(ctx, desired, client.MergeFromWithOptions(
		node.DeepCopy(), client.MergeFromWithOptimisticLock{},
	)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func nodeSettingsOwnedBy(node *corev1.Node, poolUID types.UID) (bool, error) {
	raw, found := node.Annotations[nodeSettingsAnnotation]
	if !found {
		return false, nil
	}
	var ownership nodeSettingsOwnership
	if err := nodeSettingsDecode(raw, &ownership); err != nil {
		return false, fmt.Errorf("invalid settings ownership: %w", err)
	}
	if ownership.PoolUID == "" {
		return false, fmt.Errorf("settings ownership has no pool UID")
	}
	return ownership.PoolUID == poolUID, nil
}

func nodeSettingsPreviouslyObserved(pool *nodesv1alpha1.NodePool, uid types.UID) bool {
	for _, previous := range pool.Status.Nodes {
		if previous.UID == string(uid) {
			return true
		}
	}
	for _, pending := range pool.Status.PendingSettingsCleanup {
		if pending.UID == string(uid) {
			return true
		}
	}
	return false
}

// Retain departed Node UIDs across failed passes so unreadable ownership can
// still be attributed to this pool after those Nodes leave the membership list.
func pendingSettingsCleanup(
	pool *nodesv1alpha1.NodePool, members []corev1.Node, incomplete bool,
) []nodesv1alpha1.NodePoolNodeReference {
	if !incomplete {
		return nil
	}
	current := make(map[string]bool, len(members))
	for _, member := range members {
		current[string(member.UID)] = true
	}
	pending := make(map[string]nodesv1alpha1.NodePoolNodeReference)
	for _, reference := range pool.Status.PendingSettingsCleanup {
		if !current[reference.UID] {
			pending[reference.UID] = reference
		}
	}
	for _, previous := range pool.Status.Nodes {
		if !current[previous.UID] {
			pending[previous.UID] = nodesv1alpha1.NodePoolNodeReference{
				Name: previous.Name, UID: previous.UID,
			}
		}
	}
	var result []nodesv1alpha1.NodePoolNodeReference
	for _, reference := range pending {
		result = append(result, reference)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].UID < result[j].UID
	})
	return result
}

func (r *NodePoolReconciler) checkSettingsPool(
	ctx context.Context, expected *nodesv1alpha1.NodePool,
) error {
	var current nodesv1alpha1.NodePool
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(expected), &current); err != nil {
		return &poolScopeError{cause: fmt.Errorf("recheck NodePool before Node patch: %w", err)}
	}
	if current.UID != expected.UID ||
		current.Generation != expected.Generation ||
		!equality.Semantic.DeepEqual(current.Spec, expected.Spec) ||
		current.DeletionTimestamp.IsZero() != expected.DeletionTimestamp.IsZero() ||
		!controllerutil.ContainsFinalizer(&current, nodeSettingsFinalizer) {
		return &poolScopeError{cause: fmt.Errorf("NodePool changed before Node patch; retry with fresh settings")}
	}
	return nil
}
