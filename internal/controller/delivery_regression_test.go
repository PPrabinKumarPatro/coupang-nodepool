package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLifecycleExtendedTimeoutPreservesStart(t *testing.T) {
	r, store, writer, now, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	writer.evictionError = apierrors.NewTooManyRequests("PDB", 1)
	lifecycleRun(t, r)
	start := lifecycleState(t, store, "worker-0").DrainStartedAt.DeepCopy()
	*now = now.Add(31 * time.Second)
	lifecycleRun(t, r)
	if lifecycleState(t, store, "worker-0").Reason != "DrainTimedOut" {
		t.Fatal("expected timeout")
	}
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.DrainTimeout = "1m"
	pool.Generation++
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	writer.evictionError = nil
	lifecycleRun(t, r)
	if !lifecycleState(t, store, "worker-0").DrainStartedAt.Equal(start) ||
		lifecycleCallCount(writer, "evict:") != 1 {
		t.Fatal("timeout extension must resume from the original start, not reset it")
	}
}

func TestLifecycleReplacementGetsIndependentGrace(t *testing.T) {
	r, store, writer, now, _ := lifecycleFixture(t, "Cordon", 1)
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.GracePeriod = "30s"
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	*now = now.Add(29 * time.Second)
	node := readSettingsNode(t, store, "worker-0")
	if err := store.Delete(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	node.UID, node.ResourceVersion = "replacement-uid", ""
	if err := store.Create(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	state := lifecycleState(t, store, node.Name)
	if state.UID != "replacement-uid" || !state.NotReadySince.Time.Equal(*now) {
		t.Fatal("replacement inherited its predecessor's clock")
	}
	*now = now.Add(time.Second)
	lifecycleRun(t, r)
	if lifecycleCallCount(writer, "patch-node:") != 0 {
		t.Fatal("replacement was cordoned too early")
	}
}

func TestLifecycleEvictionConflictPreservesPodReplacement(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	lifecycleRun(t, r)
	writer.beforeEviction = func(ctx context.Context, pod *corev1.Pod) error {
		if err := store.Delete(ctx, pod); err != nil {
			return err
		}
		replacement := pod.DeepCopy()
		replacement.UID, replacement.ResourceVersion = "replacement-pod", ""
		if err := store.Create(ctx, replacement); err != nil {
			return err
		}
		return apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, pod.Name, errors.New("UID precondition"))
	}
	if _, err := settingsReconcile(r); !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict: %v", err)
	}
	var current corev1.Pod
	if err := store.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "job"}, &current); err != nil {
		t.Fatal(err)
	}
	if current.UID != "replacement-pod" || lifecycleCallCount(writer, "delete-node:") != 0 ||
		!lifecycleState(t, store, "worker-0").Blocked {
		t.Fatal("replacement conflict was not preserved/reported")
	}
}

func TestLifecycleDisappearingPodDoesNotBlockOtherEvictions(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("a"), lifecyclePod("b"))
	lifecycleRun(t, r)
	writer.beforeEviction = func(ctx context.Context, pod *corev1.Pod) error {
		if pod.Name != "a" {
			return nil
		}
		if err := store.Delete(ctx, pod); err != nil {
			return err
		}
		return apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, pod.Name)
	}
	lifecycleRun(t, r)
	if lifecycleCallCount(writer, "evict:") != 2 || lifecycleState(t, store, "worker-0").Blocked {
		t.Fatal("Pod disappearance prevented independent eviction")
	}
	lifecycleRun(t, r)
	if lifecycleState(t, store, "worker-0").Phase != "Removing" {
		t.Fatal("drain did not finish")
	}
}

func TestLifecycleReadFailureKeepsReservation(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	lifecycleRun(t, r)
	start := lifecycleState(t, store, "worker-0").DrainStartedAt.DeepCopy()
	r.APIReader = &settingsReadClient{Reader: store, listError: errors.New("temporary API outage")}
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("API outage was hidden")
	}
	if !lifecycleState(t, store, "worker-0").DrainStartedAt.Equal(start) ||
		lifecycleCallCount(writer, "evict:") != 0 {
		t.Fatal("outage lost state or caused disruption")
	}
	r.APIReader = store
	lifecycleRun(t, r)
	if lifecycleCallCount(writer, "evict:") != 1 {
		t.Fatal("retry failed to resume")
	}
}

func TestSettingsPreexistingNodeAndPoolHandoff(t *testing.T) {
	ctx := context.Background()
	pool := settingsPool()
	node := settingsNode("worker", corev1.ConditionTrue)
	r, store, _ := settingsFixture(t, pool, node)
	current := readSettingsPool(t, store)
	current.Finalizers = nil
	if err := store.Update(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	if readSettingsNode(t, store, node.Name).Annotations[nodeSettingsAnnotation] != "" {
		t.Fatal("missing pool must not claim a preexisting Node")
	}
	pool.ResourceVersion = ""
	if err := store.Create(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	second := settingsPool()
	second.Name, second.UID, second.ResourceVersion = "second", "second-uid", ""
	second.Spec.Labels["workload"] = "second"
	if err := store.Create(ctx, second); err != nil {
		t.Fatal(err)
	}
	node = readSettingsNode(t, store, node.Name)
	node.Labels[membershipLabel] = second.Name
	if err := store.Update(ctx, node); err != nil {
		t.Fatal(err)
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: second.Name}}
	if _, err := r.Reconcile(ctx, request); err == nil {
		t.Fatal("new pool stole the old pool's receipt")
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, request); err != nil {
		t.Fatal(err)
	}
	node = readSettingsNode(t, store, node.Name)
	owned, err := nodeSettingsOwnedBy(node, second.UID)
	if err != nil || !owned || node.Labels["workload"] != "second" {
		t.Fatal("handoff did not converge after old-owner cleanup")
	}
}
