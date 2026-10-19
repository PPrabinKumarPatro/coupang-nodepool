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

// Scenario walkthrough and flow diagrams:
// /home/devuser/coupang-nodepool/CODE-WALKTHROUGH.md (Lessons 4 and 5, combined source trace).
const (
	lifecyclePollInterval = 10 * time.Second
	controlPlaneRoleLabel = "node-role.kubernetes.io/control-plane"
	legacyMasterRoleLabel = "node-role.kubernetes.io/master"
)

// Accept single-unit whole durations, not fractions or compound forms such as 1h30m.
// Grace may be zero; a drain timeout must always be positive.
var policyDurationPattern = regexp.MustCompile(`^[0-9]{1,6}(s|m|h)$`)
var drainDurationPattern = regexp.MustCompile(`^[1-9][0-9]{0,5}(s|m|h)$`)

type lifecyclePolicy struct {
	grace   time.Duration
	timeout time.Duration
}

func parseLifecyclePolicy(pool *nodesv1alpha1.NodePool) (lifecyclePolicy, error) {
	// Validate settings and lifecycle policy together before either can mutate Nodes.
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
		// An omitted timeout still gives draining a bounded execution window.
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
	// An injected clock lets tests cross grace and drain deadlines without sleeping.
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
	// Both draining and awaiting Node removal consume the same concurrency budget.
	return phase == "Draining" || phase == "Removing"
}

func protectedNode(node *corev1.Node) bool {
	// Role-label presence is sufficient; Kubernetes role labels commonly have empty values.
	for _, key := range []string{controlPlaneRoleLabel, legacyMasterRoleLabel} {
		if _, exists := node.Labels[key]; exists {
			return true
		}
	}
	return false
}

// Plan status without changing Nodes. The caller must persist these removal
// reservations before advanceNode is allowed to cordon, evict or delete.
//
// Planning has two passes: first observe every member and retain existing work,
// then admit eligible new removals into the remaining capacity. This prevents
// iteration order from granting a slot already held by a later member.
//
// Typical Remove path: NotReady -> grace -> waiting for a slot -> Draining ->
// Removing -> absent from membership. Confirmed Ready recovery cancels pending
// work unless Kubernetes has already accepted Node deletion.
func planLifecycle(pool *nodesv1alpha1.NodePool, members []corev1.Node,
	policy lifecyclePolicy, now time.Time, allowActions bool, settingsFailures map[string]error,
) ([]nodesv1alpha1.NodePoolNodeStatus, int32) {
	// A replacement Node can reuse a name, but must not inherit the old UID's timers.
	previous := make(map[string]nodesv1alpha1.NodePoolNodeStatus)
	for _, state := range pool.Status.Nodes {
		previous[state.UID] = state
	}
	var states []nodesv1alpha1.NodePoolNodeStatus
	var ready int32
	// Count durable reservations, not actions performed during this reconcile.
	active := 0
	for i := range members {
		node := &members[i]
		old := previous[string(node.UID)]
		// Rebuild health from current observations, then selectively restore durable
		// removal state. Restarting the process must not restart an existing drain.
		state := observeNode(node, old, lifecycleTimestamp(now))
		intervalChanged := unreadyIntervalChanged(node.UID, state.ReadyTransitionTime, old)
		if nodeIsReady(node) {
			// This count reports readiness, not eligibility for lifecycle actions.
			ready++
		}
		// Ordering matters: deletion/protection take precedence over policy,
		// and confirmed recovery cancels removal before new work is considered.
		switch {
		case !node.DeletionTimestamp.IsZero():
			// An accepted deletion cannot be cancelled; retain its slot until gone.
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
			// observeNode returned fresh state without the previous unhealthy/drain clocks.
			state.Reason = "NodeReady"
		case pool.Spec.UnreadyPolicy.Action == "Ignore":
			// Ignore does not disable the separate settings-synchronization pass.
			state.Reason = "PolicyIgnored"
		case state.NotReadySince.Sub(now) > time.Second:
			// Upward timestamp rounding explains at most one second of apparent skew.
			state.Reason, state.Message, state.Blocked = "ClockSkew", "NotReadySince is in the future; refusing early action", true
		case now.Before(state.NotReadySince.Add(policy.grace)):
			// Grace expiry only permits further checks; it does not guarantee removal.
			state.Reason = "GracePeriod"
		case pool.Spec.UnreadyPolicy.Action == "Cordon":
			// Switching from Remove drops drain state but keeps the scheduling block.
			state.Reason = "CordonPending"
			if node.Spec.Unschedulable {
				state.Reason = "Cordoned"
			} else if old.Blocked {
				state.Reason, state.Message, state.Blocked = old.Reason, old.Message, old.Blocked
			}
		case isRemovalPhase(old.Phase):
			// Resume the existing reservation and deadline instead of granting a new timeout.
			state.Phase = old.Phase
			state.DrainStartedAt = old.DrainStartedAt.DeepCopy()
			if state.DrainStartedAt == nil {
				// Initialize older incomplete status once, before any eviction.
				start := lifecycleTimestamp(now)
				state.DrainStartedAt = &start
			}
			state.Reason, state.Message, state.Blocked = old.Reason, old.Message, old.Blocked
			active++
		default:
			// Eligibility is established here; capacity is allocated after all members are counted.
			state.Reason = "WaitingForRemovalSlot"
		}
		if !protectedNode(node) && (state.Reason == "SettingsFailed" ||
			state.Reason == "ActionFailed" || state.Reason == "ProtectedNode") {
			// Re-evaluate transient blockers so repair can resume the existing work.
			state.Reason, state.Message, state.Blocked = "Retrying", "", false
			if pool.Spec.UnreadyPolicy.Action == "Cordon" {
				state.Reason = "CordonPending"
			}
		}
		if protectedNode(node) {
			// Report protection regardless of which policy or health branch ran above.
			blockNode(&state, "ProtectedNode", "Control-plane Nodes cannot receive desired settings or lifecycle disruption; remove invalid pool membership and review owned cleanup")
		}
		// A changed readiness transition may hide a recovery we never observed.
		// Distinguish that conservative wait from an ordinary first grace period.
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
			// A failed member gets no new slot, but an active removal keeps its
			// reservation and deadline; an error must not grant extra capacity.
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
	// Admit new removals oldest-first, with stable name-based tie-breaking.
	// Active slots were counted above and survive a reduction of the limit.
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
	// Pool-wide errors still permit observation, but cannot admit new removals.
	if allowActions && pool.Spec.UnreadyPolicy.Action == "Remove" {
		for i := range states {
			if states[i].Blocked || states[i].Reason != "WaitingForRemovalSlot" ||
				active >= int(pool.Spec.UnreadyPolicy.MaxConcurrentRemovals) {
				continue
			}
			// Reserving a slot starts its drain clock now, not on the first successful eviction.
			// A later PDB or API blocker must not keep extending the removal's timeout.
			start := lifecycleTimestamp(now)
			states[i].Phase = "Draining"
			states[i].DrainStartedAt = &start
			states[i].Reason = "DrainStarted"
			active++
		}
	}
	// Keep stored status stable even when oldest-first admission order changes.
	sort.Slice(states, func(i, j int) bool { return states[i].Name < states[j].Name })
	return states, ready
}

// reconcileLivePool separates settings synchronization, durable planning and
// lifecycle execution. Each invocation reconstructs progress from Kubernetes;
// it does not hold an in-memory workflow open while a Node drains.
func (r *NodePoolReconciler) reconcileLivePool(
	ctx context.Context, pool *nodesv1alpha1.NodePool,
) (ctrl.Result, error) {
	policy, poolErr := parseLifecyclePolicy(pool)
	var settingsFailures map[string]error
	if poolErr == nil {
		settingsFailures, poolErr = r.syncNodeSettings(ctx, pool)
	}
	// Separate member failures from pool-wide failures so healthy peers can progress.
	memberErr := joinNodeFailures(settingsFailures)
	// Settings cleanup can inspect departed Nodes, but lifecycle planning only
	// considers Nodes currently carrying this pool's membership label.
	var members corev1.NodeList
	if err := r.APIReader.List(ctx, &members, client.MatchingLabels{membershipLabel: pool.Name}); err != nil {
		return ctrl.Result{}, r.reportReconcileFailure(ctx, pool, errors.Join(poolErr, memberErr, err))
	}
	desired := pool.DeepCopy()
	desired.Status.Nodes, desired.Status.ReadyNodes = planLifecycle(
		pool, members.Items, policy, r.now(), poolErr == nil, settingsFailures)
	// Preserve attribution for former members if cleanup could not finish this pass.
	desired.Status.PendingSettingsCleanup = pendingSettingsCleanup(pool, members.Items, poolErr != nil || memberErr != nil)
	setLifecycleConditions(desired, poolErr, memberErr, false)
	// Persist removal slots before any cordon, eviction or Node deletion.
	if err := r.persistPoolStatus(ctx, pool, desired); err != nil {
		return ctrl.Result{}, errors.Join(poolErr, memberErr, err)
	}
	if poolErr != nil {
		// Observation was saved, but a shared failure forbids executing the plan.
		return ctrl.Result{}, errors.Join(poolErr, memberErr)
	}
	// Execution updates the saved plan; persist its outcomes in the second status write.
	desired = pool.DeepCopy()
	for i := range desired.Status.Nodes {
		state := &desired.Status.Nodes[i]
		if err := ctx.Err(); err != nil {
			poolErr = err
			break
		}
		if settingsFailures[state.Name] != nil {
			// Unresolved settings ownership also blocks lifecycle actions on this Node.
			continue
		}
		// advanceNode handles action-specific safety checks and updates this member's
		// outcome; admission to Draining alone is not authorization to evict or delete.
		if err := r.advanceNode(ctx, pool, state, policy); err != nil {
			state.Blocked, state.Reason, state.Message = true, "ActionFailed", err.Error()
			if isPoolScopeError(err) || ctx.Err() != nil {
				// Shared authorization is no longer valid; do not advance later members.
				poolErr = err
				break
			}
			// Report this member's failure without starving unrelated members.
			memberErr = errors.Join(memberErr, fmt.Errorf("advance Node %q (%s): %w", state.Name, state.UID, err))
		}
	}
	// Acknowledge processing, not universal health: handled safety blockers
	// may remain, but actual pool/member errors must not acknowledge a new spec.
	if poolErr == nil && memberErr == nil {
		desired.Status.ObservedGeneration = pool.Generation
	}
	setLifecycleConditions(desired, poolErr, memberErr, false)
	// Save both successful progress and blockers so a retry can explain and resume work.
	if err := r.persistPoolStatus(ctx, pool, desired); err != nil {
		return ctrl.Result{}, errors.Join(poolErr, memberErr, err)
	}
	if poolErr != nil || memberErr != nil {
		// Return real failures to controller-runtime rather than masking them with a timed retry.
		return ctrl.Result{}, errors.Join(poolErr, memberErr)
	}
	// PDB changes and elapsed deadlines may produce no watched Node/Pod event.
	// Requeue instead of sleeping, leaving the worker free for other pools.
	delay := lifecyclePollInterval
	for _, state := range pool.Status.Nodes {
		if (state.Reason == "GracePeriod" || state.Reason == "ReadinessGracePeriod") &&
			state.NotReadySince != nil {
			remaining := state.NotReadySince.Add(policy.grace).Sub(r.now())
			if remaining > 0 && remaining < delay {
				// Wake near the earliest grace deadline instead of waiting a full poll interval.
				delay = remaining
			}
		}
	}
	return ctrl.Result{RequeueAfter: delay}, nil
}
