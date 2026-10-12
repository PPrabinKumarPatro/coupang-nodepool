package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

const nodeCordonAnnotation = "nodes.example.com/cordon-owner"

func (r *NodePoolReconciler) cleanupNodeCordon(
	ctx context.Context, pool *nodesv1alpha1.NodePool, node *corev1.Node,
) error {
	if node.Annotations[nodeCordonAnnotation] != string(pool.UID) || !node.DeletionTimestamp.IsZero() {
		return nil
	}
	if protectedNode(node) && !nodeIsReady(node) && pool.DeletionTimestamp.IsZero() {
		return fmt.Errorf("protected Node %q requires Ready=True before owned cordon cleanup", node.Name)
	}
	if !pool.DeletionTimestamp.IsZero() {
		if !nodeIsReady(node) {
			return nil
		}
	} else if node.Labels[membershipLabel] == pool.Name &&
		!nodeIsReady(node) && pool.Spec.UnreadyPolicy.Action != "Ignore" {
		return nil
	}
	desired := node.DeepCopy()
	desired.Spec.Unschedulable = false
	delete(desired.Annotations, nodeCordonAnnotation)
	if err := r.checkSettingsPool(ctx, pool); err != nil {
		return err
	}
	if err := r.Patch(ctx, desired, client.MergeFromWithOptions(
		node.DeepCopy(), client.MergeFromWithOptimisticLock{},
	)); err != nil {
		return client.IgnoreNotFound(err)
	}
	*node = *desired
	r.emit(ctx, pool, corev1.EventTypeNormal, "CordonReleased", "Released owned cordon on Node "+node.Name)
	return nil
}

// Recheck the persisted reservation and live Node before disruptive actions.
// These reads reduce stale-state races, but are not a cross-object transaction.
func (r *NodePoolReconciler) authorizedNode(
	ctx context.Context, pool *nodesv1alpha1.NodePool, state *nodesv1alpha1.NodePoolNodeStatus,
	policy lifecyclePolicy,
) (*corev1.Node, error) {
	if err := r.checkSettingsPool(ctx, pool); err != nil {
		return nil, err
	}
	var current nodesv1alpha1.NodePool
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(pool), &current); err != nil {
		return nil, &poolScopeError{cause: fmt.Errorf("read NodePool reservation: %w", err)}
	}
	// A concurrent status writer must not invalidate the persisted reservation.
	if current.ResourceVersion != pool.ResourceVersion {
		return nil, &poolScopeError{cause: fmt.Errorf("NodePool changed after lifecycle status was persisted")}
	}
	var node corev1.Node
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: state.Name}, &node); err != nil {
		return nil, err
	}
	if string(node.UID) != state.UID || node.Labels[membershipLabel] != pool.Name {
		return nil, fmt.Errorf("Node identity or membership changed before lifecycle action")
	}
	if !node.DeletionTimestamp.IsZero() {
		return &node, nil
	}
	if nodeIsReady(&node) || protectedNode(&node) ||
		unreadyIntervalChanged(node.UID, nodeReadyTransition(&node), *state) ||
		state.NotReadySince == nil || r.now().Before(state.NotReadySince.Add(policy.grace)) {
		return nil, fmt.Errorf("Node no longer qualifies for lifecycle action")
	}
	if owner := node.Annotations[nodeCordonAnnotation]; owner != "" && owner != string(pool.UID) {
		return nil, fmt.Errorf("Node cordon is owned by another pool UID")
	}
	if raw, exists := node.Annotations[nodeSettingsAnnotation]; exists {
		owned, err := nodeSettingsOwnedBy(&node, pool.UID)
		if err != nil || !owned {
			return nil, fmt.Errorf("Node settings ownership no longer authorizes this pool (record length %d)", len(raw))
		}
	}
	return &node, nil
}

func (r *NodePoolReconciler) ensureCordon(
	ctx context.Context, pool *nodesv1alpha1.NodePool, node *corev1.Node,
) (bool, error) {
	owner := node.Annotations[nodeCordonAnnotation]
	if owner != "" && owner != string(pool.UID) {
		return false, fmt.Errorf("cordon belongs to another pool UID")
	}
	if node.Spec.Unschedulable {
		// Do not claim a preexisting cordon: cleanup must not undo another
		// actor's decision to keep the Node unschedulable.
		return false, nil
	}
	if owner != "" {
		return false, fmt.Errorf("owned cordon was cleared externally; refusing to overwrite that change")
	}
	desired := node.DeepCopy()
	if desired.Annotations == nil {
		desired.Annotations = make(map[string]string)
	}
	desired.Annotations[nodeCordonAnnotation] = string(pool.UID)
	desired.Spec.Unschedulable = true
	if err := r.Patch(ctx, desired, client.MergeFromWithOptions(
		node.DeepCopy(), client.MergeFromWithOptimisticLock{},
	)); err != nil {
		return false, err
	}
	r.emit(ctx, pool, corev1.EventTypeNormal, "NodeCordoned", "Cordoned Node "+node.Name)
	return true, nil
}

func blockNode(state *nodesv1alpha1.NodePoolNodeStatus, reason, message string) {
	state.Blocked, state.Reason, state.Message = true, reason, message
}

func markNodeDeletionPending(state *nodesv1alpha1.NodePoolNodeStatus, node *corev1.Node) {
	state.Reason, state.Message, state.Blocked = "NodeDeletionPending", "Waiting for Node disappearance", false
	if len(node.Finalizers) != 0 {
		blockNode(state, "NodeFinalizersBlocked", "Waiting for Node finalizers; the controller never removes them")
	}
}

func (r *NodePoolReconciler) advanceNode(
	ctx context.Context, pool *nodesv1alpha1.NodePool, state *nodesv1alpha1.NodePoolNodeStatus,
	policy lifecyclePolicy,
) error {
	if state.Reason == "ProtectedNode" {
		return nil
	}
	cordon := pool.Spec.UnreadyPolicy.Action == "Cordon" && state.Phase == "NotReady" &&
		state.Reason != "GracePeriod" && state.Reason != "ProtectedNode" && state.Reason != "ClockSkew"
	if !cordon && !isRemovalPhase(state.Phase) {
		return nil
	}
	if state.NotReadySince != nil && r.now().Before(state.NotReadySince.Add(policy.grace)) {
		return nil
	}
	node, err := r.authorizedNode(ctx, pool, state, policy)
	if apierrors.IsNotFound(err) {
		state.Reason, state.Message = "NodeGone", "Node no longer exists"
		return nil
	}
	if err != nil {
		return err
	}
	if !node.DeletionTimestamp.IsZero() {
		markNodeDeletionPending(state, node)
		return nil
	}
	changed, err := r.ensureCordon(ctx, pool, node)
	if err != nil {
		return err
	}
	if cordon {
		state.Reason, state.Message, state.Blocked = "Cordoned", "", false
		return nil
	}
	if pool.Spec.UnreadyPolicy.Action != "Remove" {
		return fmt.Errorf("removal policy changed")
	}
	if changed {
		// Re-observe the cordoned Node before evicting any Pods.
		state.Reason, state.Message, state.Blocked = "NodeCordoned", "", false
		return nil
	}
	pods, reason, message, err := r.drainPods(ctx, node.Name)
	if err != nil {
		return err
	}
	if reason != "" {
		blockNode(state, reason, message)
		return nil
	}
	// Accepted evictions may finish after the deadline. Completion is still
	// allowed once no blocking Pods remain; only new evictions are timed out.
	if len(pods) == 0 {
		if state.Phase == "Draining" {
			// Return so reconcileLivePool persists Removing before a later
			// pass requests deletion. A restart can then resume that intent.
			state.Phase, state.Reason, state.Message, state.Blocked = "Removing", "DrainComplete", "", false
			return nil
		}
		node, err = r.authorizedNode(ctx, pool, state, policy)
		if err != nil {
			return client.IgnoreNotFound(err)
		}
		if !node.Spec.Unschedulable {
			return fmt.Errorf("Node was uncordoned before deletion")
		}
		uid, version := node.UID, node.ResourceVersion
		// Reject deletion of a same-name replacement or an intervening Node edit.
		if err := r.Delete(ctx, node, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID: &uid, ResourceVersion: &version,
		}}); err != nil {
			return client.IgnoreNotFound(err)
		}
		markNodeDeletionPending(state, node)
		return nil
	}
	if state.Phase == "Removing" {
		state.Phase = "Draining"
		blockNode(state, "PodsAppeared", "Pods appeared after draining; Node deletion is postponed")
		return nil
	}
	if state.DrainStartedAt == nil || state.DrainStartedAt.Sub(r.now()) > time.Second {
		blockNode(state, "ClockSkew", "Missing or future drain timestamp; refusing eviction")
		return nil
	}
	if state.DrainStartedAt.After(r.now()) {
		return nil
	}
	if !r.now().Before(state.DrainStartedAt.Add(policy.timeout)) {
		blockNode(state, "DrainTimedOut", fmt.Sprintf("%d Pods still present; no force deletion; resolve blockers or cancel removal", len(pods)))
		return nil
	}
	state.Reason, state.Message, state.Blocked = "Evicting", "Waiting for Pods to leave the Node", false
	for i := range pods {
		pod := &pods[i]
		if !pod.DeletionTimestamp.IsZero() {
			// An accepted eviction is not completed shutdown. Wait for the
			// Pod to disappear rather than re-evicting or force-deleting it.
			continue
		}
		if !r.now().Before(state.DrainStartedAt.Add(policy.timeout)) {
			blockNode(state, "DrainTimedOut", "Drain deadline reached; no further evictions or force deletion")
			return nil
		}
		currentNode, err := r.authorizedNode(ctx, pool, state, policy)
		if err != nil {
			return err
		}
		if !currentNode.Spec.Unschedulable || !currentNode.DeletionTimestamp.IsZero() {
			return fmt.Errorf("Node was uncordoned or deleted before eviction")
		}
		uid, version := pod.UID, pod.ResourceVersion
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
			DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{
				UID: &uid, ResourceVersion: &version,
			}},
		}
		if !r.now().Before(state.DrainStartedAt.Add(policy.timeout)) {
			blockNode(state, "DrainTimedOut", "Drain deadline reached during authorization; no further evictions")
			return nil
		}
		err = r.SubResource("eviction").Create(ctx, pod, eviction)
		if apierrors.IsNotFound(err) {
			continue
		}
		if apierrors.IsTooManyRequests(err) {
			blockNode(state, "EvictionBlocked", "Eviction refused or throttled; disruption budgets are not bypassed: "+err.Error())
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *NodePoolReconciler) drainPods(
	ctx context.Context, nodeName string,
) ([]corev1.Pod, string, string, error) {
	var list corev1.PodList
	if err := r.APIReader.List(ctx, &list, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		return nil, "", "", err
	}
	sort.Slice(list.Items, func(i, j int) bool {
		a, b := list.Items[i], list.Items[j]
		return a.Namespace+"/"+a.Name < b.Namespace+"/"+b.Name
	})
	var remaining []corev1.Pod
	for i := range list.Items {
		pod := &list.Items[i]
		// Terminal phase alone is insufficient for an ordinary deleting Pod:
		// finalizers may still hold cleanup, so keep it as a Node-deletion blocker.
		if pod.DeletionTimestamp.IsZero() &&
			(pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed) {
			continue
		}
		if _, mirror := pod.Annotations[corev1.MirrorPodAnnotationKey]; mirror {
			continue
		}
		owner := metav1.GetControllerOf(pod)
		if owner != nil && owner.Kind == "DaemonSet" && owner.APIVersion == "apps/v1" {
			var daemonSet appsv1.DaemonSet
			err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: owner.Name}, &daemonSet)
			// A recreated same-name DaemonSet is not this Pod's original owner.
			if apierrors.IsNotFound(err) || (err == nil && daemonSet.UID != owner.UID) {
				return nil, "OrphanedDaemonSet", "Pod " + pod.Namespace + "/" + pod.Name + " has no matching DaemonSet; manual intervention required", nil
			}
			if err != nil {
				return nil, "", "", err
			}
			continue
		}
		// This is not a general rescheduling-safety check: ordinary owners are
		// not fetched, and PVC/PV/CSI portability is not validated below.
		if owner == nil || owner.UID == "" {
			return nil, "UnmanagedPod", "Pod " + pod.Namespace + "/" + pod.Name + " is unmanaged; manual intervention required", nil
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.EmptyDir != nil || volume.HostPath != nil {
				return nil, "LocalStoragePod", "Pod " + pod.Namespace + "/" + pod.Name + " uses local storage; manual intervention required", nil
			}
		}
		remaining = append(remaining, *pod)
	}
	return remaining, "", "", nil
}

func (r *NodePoolReconciler) podPoolRequests(ctx context.Context, object client.Object) []reconcile.Request {
	pod, ok := object.(*corev1.Pod)
	if !ok || pod.Spec.NodeName == "" {
		return nil
	}
	var node corev1.Node
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, &node); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "Cannot map Pod event to NodePool", "node", pod.Spec.NodeName)
		}
		return nil
	}
	return memberPoolRequests(ctx, &node)
}
