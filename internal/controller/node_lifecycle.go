package controller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

const (
	lifecyclePollInterval = 10 * time.Second
	controlPlaneRoleLabel = "node-role.kubernetes.io/control-plane"
	legacyMasterRoleLabel = "node-role.kubernetes.io/master"
)

var policyDurationPattern = regexp.MustCompile(`^[0-9]{1,6}(s|m|h)$`)
var drainDurationPattern = regexp.MustCompile(`^[1-9][0-9]{0,5}(s|m|h)$`)

type lifecyclePolicy struct {
	grace   time.Duration
	timeout time.Duration
}

func parseLifecyclePolicy(pool *nodesv1alpha1.NodePool) (lifecyclePolicy, error) {
	var result lifecyclePolicy
	policy := pool.Spec.UnreadyPolicy
	if pool.Spec.Image == "" || policy.MaxConcurrentRemovals < 1 {
		return result, fmt.Errorf("image and a positive maxConcurrentRemovals are required")
	}
	switch policy.Action {
	case "Ignore", "Cordon", "Remove":
	default:
		return result, fmt.Errorf("unsupported unreadyPolicy.action %q", policy.Action)
	}
	if !policyDurationPattern.MatchString(policy.GracePeriod) {
		return result, fmt.Errorf("gracePeriod must be whole seconds, minutes or hours")
	}
	var err error
	result.grace, err = time.ParseDuration(policy.GracePeriod)
	if err != nil {
		return result, fmt.Errorf("parse gracePeriod: %w", err)
	}
	timeout := policy.DrainTimeout
	if timeout == "" {
		timeout = "5m"
	}
	if !drainDurationPattern.MatchString(timeout) {
		return result, fmt.Errorf("drainTimeout must be whole seconds, minutes or hours")
	}
	result.timeout, err = time.ParseDuration(timeout)
	if err != nil || result.timeout <= 0 {
		return result, fmt.Errorf("drainTimeout must be positive and representable")
	}
	if err := nodeSettingsValidateLabels(pool.Spec.Labels); err != nil {
		return result, err
	}
	if _, err := nodeSettingsTaintMap(pool.Spec.Taints, false); err != nil {
		return result, err
	}
	return result, nil
}

func (r *NodePoolReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}

func lifecycleTimestamp(now time.Time) metav1.Time {
	// metav1.Time persists whole seconds. Round up so serialization cannot
	// shorten a grace period or drain timeout.
	if now.Nanosecond() != 0 {
		now = now.Truncate(time.Second).Add(time.Second)
	}
	return metav1.NewTime(now)
}

func isRemovalPhase(phase string) bool {
	return phase == "Draining" || phase == "Removing"
}

func protectedNode(node *corev1.Node) bool {
	for _, key := range []string{controlPlaneRoleLabel, legacyMasterRoleLabel} {
		if _, exists := node.Labels[key]; exists {
			return true
		}
	}
	return false
}

func planLifecycle(pool *nodesv1alpha1.NodePool, members []corev1.Node,
	policy lifecyclePolicy, now time.Time, allowActions bool, settingsFailures map[string]error,
) ([]nodesv1alpha1.NodePoolNodeStatus, int32) {
	previous := make(map[string]nodesv1alpha1.NodePoolNodeStatus)
	for _, state := range pool.Status.Nodes {
		previous[state.UID] = state
	}
	var states []nodesv1alpha1.NodePoolNodeStatus
	var ready int32
	active := 0
	for i := range members {
		node := &members[i]
		old := previous[string(node.UID)]
		state := observeNode(node, old, lifecycleTimestamp(now))
		intervalChanged := unreadyIntervalChanged(node.UID, state.ReadyTransitionTime, old)
		if nodeIsReady(node) {
			ready++
		}
		switch {
		case !node.DeletionTimestamp.IsZero():
			state.Phase = "Removing"
			state.DrainStartedAt = old.DrainStartedAt.DeepCopy()
			markNodeDeletionPending(&state, node)
			active++
		case protectedNode(node):
			// A protection change stops execution without renewing an unresolved removal.
			if pool.Spec.UnreadyPolicy.Action == "Remove" && isRemovalPhase(old.Phase) && !nodeIsReady(node) {
				state.Phase = old.Phase
				state.DrainStartedAt = old.DrainStartedAt.DeepCopy()
				active++
			}
		case state.Phase == "Ready":
			state.Reason = "NodeReady"
		case pool.Spec.UnreadyPolicy.Action == "Ignore":
			state.Reason = "PolicyIgnored"
		case state.NotReadySince.Sub(now) > time.Second:
			state.Reason, state.Message, state.Blocked = "ClockSkew", "NotReadySince is in the future; refusing early action", true
		case now.Before(state.NotReadySince.Add(policy.grace)):
			state.Reason = "GracePeriod"
		case pool.Spec.UnreadyPolicy.Action == "Cordon":
			state.Reason = "CordonPending"
			if node.Spec.Unschedulable {
				state.Reason = "Cordoned"
			} else if old.Blocked {
				state.Reason, state.Message, state.Blocked = old.Reason, old.Message, old.Blocked
			}
		case isRemovalPhase(old.Phase):
			state.Phase = old.Phase
			state.DrainStartedAt = old.DrainStartedAt.DeepCopy()
			if state.DrainStartedAt == nil {
				start := lifecycleTimestamp(now)
				state.DrainStartedAt = &start
			}
			state.Reason, state.Message, state.Blocked = old.Reason, old.Message, old.Blocked
			active++
		default:
			state.Reason = "WaitingForRemovalSlot"
		}
		if !protectedNode(node) && (state.Reason == "SettingsFailed" ||
			state.Reason == "ActionFailed" || state.Reason == "ProtectedNode") {
			state.Reason, state.Message, state.Blocked = "Retrying", "", false
			if pool.Spec.UnreadyPolicy.Action == "Cordon" {
				state.Reason = "CordonPending"
			}
		}
		if protectedNode(node) {
			blockNode(&state, "ProtectedNode", "Control-plane Nodes cannot receive desired settings or lifecycle disruption; remove invalid pool membership and review owned cleanup")
		}
		readinessWait := state.Reason == "ClockSkew" ||
			state.Reason == "GracePeriod" && (intervalChanged ||
				old.Reason == "ReadinessGracePeriod" || old.Reason == "ClockSkew")
		if pool.Spec.UnreadyPolicy.Action == "Remove" && isRemovalPhase(old.Phase) &&
			!isRemovalPhase(state.Phase) && readinessWait {
			// Suspected recovery can delay actions, but cannot renew a drain
			// deadline or release a still-unresolved removal reservation.
			state.Phase = old.Phase
			state.DrainStartedAt = old.DrainStartedAt.DeepCopy()
			if state.Reason == "GracePeriod" {
				state.Reason = "ReadinessGracePeriod"
				state.Message = "Readiness transition changed; waiting through grace with the original drain deadline"
			}
			active++
		}
		if failure := settingsFailures[node.Name]; failure != nil {
			if pool.Spec.UnreadyPolicy.Action == "Remove" && !nodeIsReady(node) &&
				isRemovalPhase(old.Phase) && !isRemovalPhase(state.Phase) {
				state.Phase = old.Phase
				state.DrainStartedAt = old.DrainStartedAt.DeepCopy()
				active++
			}
			if !protectedNode(node) {
				blockNode(&state, "SettingsFailed", failure.Error())
			}
		}
		states = append(states, state)
	}
	// Preserve in-flight reservations first, even after a limit reduction.
	sort.Slice(states, func(i, j int) bool {
		a, b := states[i], states[j]
		if (a.NotReadySince == nil) != (b.NotReadySince == nil) {
			return a.NotReadySince != nil
		}
		if a.NotReadySince != nil && b.NotReadySince != nil &&
			!a.NotReadySince.Equal(b.NotReadySince) {
			return a.NotReadySince.Before(b.NotReadySince)
		}
		return a.Name < b.Name
	})
	if allowActions && pool.Spec.UnreadyPolicy.Action == "Remove" {
		for i := range states {
			if states[i].Blocked || states[i].Reason != "WaitingForRemovalSlot" ||
				active >= int(pool.Spec.UnreadyPolicy.MaxConcurrentRemovals) {
				continue
			}
			start := lifecycleTimestamp(now)
			states[i].Phase = "Draining"
			states[i].DrainStartedAt = &start
			states[i].Reason = "DrainStarted"
			active++
		}
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Name < states[j].Name })
	return states, ready
}

func (r *NodePoolReconciler) reconcileLivePool(
	ctx context.Context, pool *nodesv1alpha1.NodePool,
) (ctrl.Result, error) {
	policy, poolErr := parseLifecyclePolicy(pool)
	var settingsFailures map[string]error
	if poolErr == nil {
		settingsFailures, poolErr = r.syncNodeSettings(ctx, pool)
	}
	memberErr := joinNodeFailures(settingsFailures)
	var members corev1.NodeList
	if err := r.APIReader.List(ctx, &members, client.MatchingLabels{membershipLabel: pool.Name}); err != nil {
		return ctrl.Result{}, r.reportReconcileFailure(ctx, pool, errors.Join(poolErr, memberErr, err))
	}
	desired := pool.DeepCopy()
	desired.Status.Nodes, desired.Status.ReadyNodes = planLifecycle(
		pool, members.Items, policy, r.now(), poolErr == nil, settingsFailures)
	desired.Status.PendingSettingsCleanup = pendingSettingsCleanup(pool, members.Items, poolErr != nil || memberErr != nil)
	setLifecycleConditions(desired, poolErr, memberErr, false)
	// Persist removal slots before any cordon, eviction or Node deletion.
	if err := r.persistPoolStatus(ctx, pool, desired); err != nil {
		return ctrl.Result{}, errors.Join(poolErr, memberErr, err)
	}
	if poolErr != nil {
		return ctrl.Result{}, errors.Join(poolErr, memberErr)
	}
	desired = pool.DeepCopy()
	for i := range desired.Status.Nodes {
		state := &desired.Status.Nodes[i]
		if err := ctx.Err(); err != nil {
			poolErr = err
			break
		}
		if settingsFailures[state.Name] != nil {
			continue
		}
		if err := r.advanceNode(ctx, pool, state, policy); err != nil {
			state.Blocked, state.Reason, state.Message = true, "ActionFailed", err.Error()
			if isPoolScopeError(err) || ctx.Err() != nil {
				poolErr = err
				break
			}
			memberErr = errors.Join(memberErr, fmt.Errorf("advance Node %q (%s): %w", state.Name, state.UID, err))
		}
	}
	if poolErr == nil && memberErr == nil {
		desired.Status.ObservedGeneration = pool.Generation
	}
	setLifecycleConditions(desired, poolErr, memberErr, false)
	if err := r.persistPoolStatus(ctx, pool, desired); err != nil {
		return ctrl.Result{}, errors.Join(poolErr, memberErr, err)
	}
	if poolErr != nil || memberErr != nil {
		return ctrl.Result{}, errors.Join(poolErr, memberErr)
	}
	delay := lifecyclePollInterval
	for _, state := range pool.Status.Nodes {
		if (state.Reason == "GracePeriod" || state.Reason == "ReadinessGracePeriod") &&
			state.NotReadySince != nil {
			remaining := state.NotReadySince.Add(policy.grace).Sub(r.now())
			if remaining > 0 && remaining < delay {
				delay = remaining
			}
		}
	}
	return ctrl.Result{RequeueAfter: delay}, nil
}
