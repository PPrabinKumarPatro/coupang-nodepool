/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

const membershipLabel = "nodes.example.com/nodepool"

type NodePoolReconciler struct {
	client.Client
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  events.EventRecorder
	Now       func() time.Time
}

// +kubebuilder:rbac:groups=nodes.example.com,resources=nodepools,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=nodes.example.com,resources=nodepools/finalizers,verbs=update;patch
// +kubebuilder:rbac:groups=nodes.example.com,resources=nodepools/status,verbs=get;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

func (r *NodePoolReconciler) Reconcile(
	ctx context.Context, req ctrl.Request,
) (ctrl.Result, error) {
	// Watch events trigger work, but cached observations cannot authorize a
	// destructive action; pool and Node checks use the uncached API reader.
	if r.APIReader == nil {
		return ctrl.Result{}, fmt.Errorf("NodePoolReconciler requires an uncached APIReader")
	}
	var pool nodesv1alpha1.NodePool
	if err := r.APIReader.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// Deleting a pool cleans up owned settings, not its machines or workloads.
	// Keep the finalizer until eligible cleanup and its status update succeed.
	if !pool.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(&pool, nodeSettingsFinalizer) {
			// Do not adopt cleanup responsibility after deletion has already started.
			return ctrl.Result{}, nil
		}
		failures, syncErr := r.syncNodeSettings(ctx, &pool)
		if err := errors.Join(syncErr, joinNodeFailures(failures)); err != nil {
			return ctrl.Result{}, r.reportReconcileFailure(ctx, &pool, err)
		}
		desired := pool.DeepCopy()
		desired.Status.PendingSettingsCleanup = nil
		desired.Status.ObservedGeneration = pool.Generation
		for _, conditionType := range []string{"Failed", "Progressing"} {
			meta.SetStatusCondition(&desired.Status.Conditions, metav1.Condition{
				Type: conditionType, Status: metav1.ConditionFalse,
				Reason: "CleanupComplete", Message: "Eligible settings and owned cordons have been cleaned",
				ObservedGeneration: pool.Generation,
			})
		}
		if err := r.persistPoolStatus(ctx, &pool, desired); err != nil {
			return ctrl.Result{}, err
		}
		desired = pool.DeepCopy()
		controllerutil.RemoveFinalizer(desired, nodeSettingsFinalizer)
		if err := r.Patch(ctx, desired, client.MergeFromWithOptions(
			pool.DeepCopy(), client.MergeFromWithOptimisticLock{},
		)); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		r.emit(ctx, &pool, corev1.EventTypeNormal, "PoolFinalized", "Eligible settings and cordons cleaned; finalizer released")
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&pool, nodeSettingsFinalizer) {
		desired := pool.DeepCopy()
		controllerutil.AddFinalizer(desired, nodeSettingsFinalizer)
		if err := r.Patch(ctx, desired, client.MergeFromWithOptions(
			pool.DeepCopy(), client.MergeFromWithOptimisticLock{},
		)); err != nil {
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		// Record cleanup responsibility before any Node mutation. A later pass
		// rechecks the pool instead of acting on the pre-finalizer snapshot.
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return r.reconcileLivePool(ctx, &pool)
}

func observeNode(
	node *corev1.Node,
	previous nodesv1alpha1.NodePoolNodeStatus,
	now metav1.Time,
) nodesv1alpha1.NodePoolNodeStatus {
	current := nodesv1alpha1.NodePoolNodeStatus{
		Name:                node.Name,
		UID:                 string(node.UID),
		Phase:               "NotReady",
		ReadyTransitionTime: nodeReadyTransition(node),
	}

	if nodeIsReady(node) {
		current.Phase = "Ready"
		// A fresh struct clears any previous NotReadySince.
		return current
	}

	// Reuse the saved clock for the same unhealthy interval, including after
	// a restart. A replacement UID or changed transition starts fresh grace.
	if !unreadyIntervalChanged(node.UID, current.ReadyTransitionTime, previous) {
		current.NotReadySince = previous.NotReadySince.DeepCopy()
	} else {
		current.NotReadySince = now.DeepCopy()
	}
	if current.ReadyTransitionTime != nil &&
		current.ReadyTransitionTime.After(current.NotReadySince.Time) {
		// A future Node clock must not authorize action using an older local clock.
		start := lifecycleTimestamp(current.ReadyTransitionTime.Time)
		current.NotReadySince = &start
	}
	return current
}

func nodeReadyTransition(node *corev1.Node) *metav1.Time {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			if condition.LastTransitionTime.IsZero() {
				return nil
			}
			return condition.LastTransitionTime.DeepCopy()
		}
	}
	return nil
}

func unreadyIntervalChanged(
	uid types.UID, transition *metav1.Time, previous nodesv1alpha1.NodePoolNodeStatus,
) bool {
	if previous.UID != string(uid) || previous.NotReadySince == nil {
		return true
	}
	if previous.ReadyTransitionTime != nil {
		// False/Unknown changes and clock regressions are ambiguous too:
		// conservatively restart grace, rather than assume continuous failure.
		return transition == nil || !transition.Equal(previous.ReadyTransitionTime)
	}
	// Older status entries have no transition baseline. Keep their clock only
	// when the available transition predates the recorded unready interval.
	return transition != nil && !transition.Before(previous.NotReadySince)
}

func nodeIsReady(node *corev1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	// False, Unknown, and a missing Ready condition are not ready.
	return false
}

func (r *NodePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// The mapping handler processes both old and new Nodes on an update:
	// the former pool needs cleanup when the membership label changes.
	return ctrl.NewControllerManagedBy(mgr).
		For(&nodesv1alpha1.NodePool{}).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(memberPoolRequests),
		).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.podPoolRequests)).
		Named("nodepool").
		Complete(r)
}

func memberPoolRequests(
	_ context.Context, object client.Object,
) []reconcile.Request {
	poolName := object.GetLabels()[membershipLabel]
	if poolName == "" {
		return nil
	}
	// NodePools are cluster-scoped; leave Namespace empty.
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Name: poolName},
	}}
}
