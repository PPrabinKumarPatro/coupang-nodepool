package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

func setLifecycleConditions(pool *nodesv1alpha1.NodePool, reconcileErr, memberErr error, observationFailed bool) {
	// Ready reports observed Node health, not successful settings convergence
	// or permission to disrupt those Nodes.
	// An empty pool is Unknown: no member health has been observed.
	ready, readyReason, readyMessage := metav1.ConditionUnknown, "NoMembers", "No member Nodes were observed"
	if len(pool.Status.Nodes) > 0 {
		ready, readyReason, readyMessage = metav1.ConditionFalse, "MembersNotReady", "At least one member lacks Ready=True"
		if int(pool.Status.ReadyNodes) == len(pool.Status.Nodes) {
			ready, readyReason, readyMessage = metav1.ConditionTrue, "MembersReady", "All observed members report Ready=True"
		}
	}
	if observationFailed {
		ready, readyReason, readyMessage = metav1.ConditionUnknown, "ObservationFailed", "Unable to refresh member health"
	}
	progress, progressReason, progressMessage := metav1.ConditionFalse, "Stable", "No lifecycle work is pending"
	failed, failedReason, failedMessage := metav1.ConditionFalse, "NoFailure", "No reconciliation failure or safety blocker"
	// One blocked member can coexist with useful work on another, so Failed
	// and Progressing are independent rather than mutually exclusive.
	for _, state := range pool.Status.Nodes {
		if state.Blocked {
			failed, failedReason, failedMessage = metav1.ConditionTrue, "LifecycleBlocked", state.Name+": "+state.Reason+": "+state.Message
		} else if isRemovalPhase(state.Phase) || state.Reason == "GracePeriod" ||
			state.Reason == "CordonPending" || state.Reason == "WaitingForRemovalSlot" {
			progress, progressReason, progressMessage = metav1.ConditionTrue, "Reconciling", "Lifecycle work is pending"
		}
	}
	if failed == metav1.ConditionTrue && progress == metav1.ConditionFalse {
		progressReason, progressMessage = "Blocked", "Lifecycle work is blocked; safety constraints are not bypassed"
	}
	if reconcileErr != nil {
		// Shared failure stops progress; a member-only failure below can coexist
		// with progress already observed on other members.
		failed, failedReason, failedMessage = metav1.ConditionTrue, "ReconcileFailed", reconcileErr.Error()
		progress, progressReason, progressMessage = metav1.ConditionFalse, "ReconcileFailed", "Retrying after a reconciliation error"
	} else if memberErr != nil {
		failed, failedReason, failedMessage = metav1.ConditionTrue, "MemberReconcileFailed", memberErr.Error()
		if progress == metav1.ConditionFalse {
			progressReason, progressMessage = "Blocked", "Retrying failed members; no other lifecycle work is progressing"
		}
	}
	for _, condition := range []metav1.Condition{
		{Type: "Ready", Status: ready, Reason: readyReason, Message: readyMessage},
		{Type: "Progressing", Status: progress, Reason: progressReason, Message: progressMessage},
		{Type: "Failed", Status: failed, Reason: failedReason, Message: failedMessage},
	} {
		// Conditions describe the current spec even when the top-level
		// successful-processing observedGeneration cannot advance.
		condition.ObservedGeneration = pool.Generation
		meta.SetStatusCondition(&pool.Status.Conditions, condition)
	}
}

func (r *NodePoolReconciler) persistPoolStatus(
	ctx context.Context, pool, desired *nodesv1alpha1.NodePool,
) error {
	if equality.Semantic.DeepEqual(pool.Status, desired.Status) {
		// Avoid version churn that would invalidate otherwise-current reservations.
		return nil
	}
	before := pool.DeepCopy()
	if err := r.Status().Patch(ctx, desired, client.MergeFromWithOptions(
		before, client.MergeFromWithOptimisticLock{},
	)); err != nil {
		return err
	}
	// Carry the API response's resource version into subsequent authorization
	// checks, which must validate the reservation that was actually persisted.
	*pool = *desired
	// Emit only after status is durable. Events are best-effort: a crash here
	// can lose an Event, so recovery relies on status rather than Event history.
	r.emitStatusTransitions(ctx, before, pool)
	return nil
}

func (r *NodePoolReconciler) reportReconcileFailure(
	ctx context.Context, pool *nodesv1alpha1.NodePool, failure error,
) error {
	desired := pool.DeepCopy()
	setLifecycleConditions(desired, failure, nil, true)
	return errors.Join(failure, r.persistPoolStatus(ctx, pool, desired))
}

func (r *NodePoolReconciler) emit(ctx context.Context, pool *nodesv1alpha1.NodePool, eventType, reason, message string) {
	log.FromContext(ctx).Info("NodePool transition", "pool", pool.Name, "reason", reason, "message", message)
	if r.Recorder != nil {
		r.Recorder.Eventf(pool, nil, eventType, reason, "Reconcile", "%s", message)
	}
}

func (r *NodePoolReconciler) emitStatusTransitions(
	ctx context.Context, before, after *nodesv1alpha1.NodePool,
) {
	previous := make(map[string]nodesv1alpha1.NodePoolNodeStatus)
	for _, state := range before.Status.Nodes {
		previous[state.UID] = state
	}
	for _, state := range after.Status.Nodes {
		old, exists := previous[state.UID]
		delete(previous, state.UID)
		if exists && old.Phase == state.Phase && old.Reason == state.Reason && old.Blocked == state.Blocked {
			continue
		}
		eventType := corev1.EventTypeNormal
		if state.Blocked {
			eventType = corev1.EventTypeWarning
		}
		r.emit(ctx, after, eventType, state.Reason,
			fmt.Sprintf("Node %s (%s): %s; %s", state.Name, state.UID, state.Phase, state.Message))
	}
	for _, state := range previous {
		reason := "NodeLeftPool"
		if state.Phase == "Removing" {
			// Leaving the membership list is not proof of deletion. Confirm
			// the old Node UID is gone before reporting NodeRemoved.
			var node corev1.Node
			err := r.APIReader.Get(ctx, types.NamespacedName{Name: state.Name}, &node)
			switch {
			case apierrors.IsNotFound(err), err == nil && string(node.UID) != state.UID:
				reason = "NodeRemoved"
			case err != nil:
				log.FromContext(ctx).Error(err, "Cannot confirm removal for transition Event", "node", state.Name)
				reason = "NodeNoLongerObserved"
			}
		}
		r.emit(ctx, after, corev1.EventTypeNormal, reason, "Node "+state.Name+" ("+state.UID+") is no longer a member")
	}
	for _, condition := range after.Status.Conditions {
		old := meta.FindStatusCondition(before.Status.Conditions, condition.Type)
		// Message/timestamp-only changes are not new transitions; avoid retry noise.
		if old == nil || old.Status != condition.Status || old.Reason != condition.Reason {
			eventType := corev1.EventTypeNormal
			if condition.Type == "Failed" && condition.Status == metav1.ConditionTrue {
				eventType = corev1.EventTypeWarning
			}
			r.emit(ctx, after, eventType, condition.Reason, condition.Type+": "+condition.Message)
		}
	}
}
