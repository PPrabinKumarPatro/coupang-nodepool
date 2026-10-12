package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

func scenarioReadiness(t *testing.T, store client.Client, name string, status corev1.ConditionStatus, transition time.Time) {
	t.Helper()
	node := readSettingsNode(t, store, name)
	node.Status.Conditions = nil
	if status != "" {
		node.Status.Conditions = []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: status, LastTransitionTime: metav1.NewTime(transition),
		}}
	}
	if err := store.Status().Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
}

func TestScenarioReadinessTransitionGrace(t *testing.T) {
	for _, scenario := range []string{"new failure", "Unknown", "clock regression", "missing condition", "unchanged", "legacy older", "legacy newer"} {
		t.Run(scenario, func(t *testing.T) {
			r, store, _, now, _ := lifecycleFixture(t, "Cordon", 1)
			base := *now
			pool := readSettingsPool(t, store)
			pool.Spec.UnreadyPolicy.GracePeriod = "30s"
			if err := store.Update(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			scenarioReadiness(t, store, "worker-0", corev1.ConditionFalse, base.Add(-time.Minute))
			lifecycleRun(t, r)
			if scenario == "legacy older" || scenario == "legacy newer" {
				pool = readSettingsPool(t, store)
				pool.Status.Nodes[0].ReadyTransitionTime = nil
				if err := store.Status().Update(context.Background(), pool); err != nil {
					t.Fatal(err)
				}
			}
			*now = base.Add(31 * time.Second)
			status, transition := corev1.ConditionFalse, base.Add(30*time.Second)
			switch scenario {
			case "Unknown":
				status = corev1.ConditionUnknown
			case "clock regression":
				transition = base.Add(-2 * time.Minute)
			case "missing condition":
				status = ""
			case "unchanged", "legacy older":
				transition = base.Add(-time.Minute)
			}
			scenarioReadiness(t, store, "worker-0", status, transition)
			lifecycleRun(t, r)
			reset := scenario != "unchanged" && scenario != "legacy older"
			state := lifecycleState(t, store, "worker-0")
			if !reset {
				if !state.NotReadySince.Time.Equal(base) || !readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
					t.Fatal("unchanged/older metadata reset a continuous failure")
				}
				return
			}
			if !state.NotReadySince.Time.Equal(*now) || readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
				t.Fatal("new or ambiguous interval inherited an expired grace period")
			}
			*now = now.Add(29 * time.Second)
			lifecycleRun(t, r)
			if readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
				t.Fatal("cordoned before fresh grace elapsed")
			}
			*now = now.Add(time.Second)
			lifecycleRun(t, r)
			if !readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
				t.Fatal("did not cordon at the new grace boundary")
			}
		})
	}
}

func TestScenarioReadinessWaitRetainsRemovalBudget(t *testing.T) {
	for _, future := range []bool{false, true} {
		t.Run(map[bool]string{false: "changed transition", true: "future transition"}[future], func(t *testing.T) {
			r, store, writer, now, _ := lifecycleFixture(t, "Remove", 2, lifecyclePod("job"))
			base := *now
			pool := readSettingsPool(t, store)
			pool.Spec.UnreadyPolicy.GracePeriod = "20s"
			if err := store.Update(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			scenarioReadiness(t, store, "worker-0", corev1.ConditionFalse, base.Add(-time.Minute))
			lifecycleRun(t, r)
			*now = base.Add(20 * time.Second)
			lifecycleRun(t, r)
			start := lifecycleState(t, store, "worker-0").DrainStartedAt.DeepCopy()
			*now = base.Add(51 * time.Second)
			transition := now.Add(-time.Second)
			if future {
				transition = now.Add(10 * time.Second)
			}
			scenarioReadiness(t, store, "worker-0", corev1.ConditionUnknown, transition)
			lifecycleRun(t, r)
			state := lifecycleState(t, store, "worker-0")
			if state.Phase != "Draining" || !state.DrainStartedAt.Equal(start) ||
				readSettingsNode(t, store, "worker-1").Spec.Unschedulable {
				t.Fatal("readiness uncertainty renewed the budget or released its slot")
			}
			if future && state.Reason != "ClockSkew" {
				t.Fatal("future clock was not blocked")
			}
			*now = state.NotReadySince.Add(19 * time.Second)
			lifecycleRun(t, r)
			state = lifecycleState(t, store, "worker-0")
			if state.Reason != "ReadinessGracePeriod" || !state.DrainStartedAt.Equal(start) {
				t.Fatal("repeated waiting lost the original reservation")
			}
			*now = now.Add(time.Second)
			lifecycleRun(t, r)
			if lifecycleState(t, store, "worker-0").Reason != "DrainTimedOut" ||
				lifecycleCallCount(writer, "evict:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 {
				t.Fatal("fresh grace must not renew an expired eviction window")
			}
			scenarioReadiness(t, store, "worker-0", corev1.ConditionTrue, *now)
			lifecycleRun(t, r)
			state = lifecycleState(t, store, "worker-0")
			if state.Phase != "Ready" || state.DrainStartedAt != nil ||
				readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
				t.Fatal("confirmed recovery did not clear the reservation and owned cordon")
			}
		})
	}
}

func TestScenarioFreshTransitionFencesEviction(t *testing.T) {
	r, store, writer, now, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	scenarioReadiness(t, store, "worker-0", corev1.ConditionFalse, now.Add(-time.Minute))
	lifecycleRun(t, r)
	*now = now.Add(2 * time.Second)
	reads := 0
	r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
		if _, ok := object.(*corev1.Node); ok {
			reads++
			if reads == 3 {
				scenarioReadiness(t, store, key.Name, corev1.ConditionUnknown, *now)
			}
		}
		return nil
	}}
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("new transition did not fence the old authorization")
	}
	if reads != 3 || lifecycleCallCount(writer, "evict:") != 0 ||
		lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("transition changed during authorization but disruption continued")
	}
}

func TestScenarioSlowAuthorizationCannotEvictAfterDeadline(t *testing.T) {
	r, store, writer, now, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	lifecycleRun(t, r)
	*now = now.Add(29 * time.Second)
	reads := 0
	r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(_ context.Context, _ client.ObjectKey, object client.Object) error {
		if _, ok := object.(*corev1.Node); ok {
			reads++
			if reads == 3 {
				*now = now.Add(2 * time.Second)
			}
		}
		return nil
	}}
	lifecycleRun(t, r)
	state := lifecycleState(t, store, "worker-0")
	if reads != 3 || state.Reason != "DrainTimedOut" || !state.Blocked ||
		lifecycleCallCount(writer, "evict:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("authorization crossed the deadline without blocking eviction")
	}
}

func TestScenarioPendingCleanupSurvivesRetries(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "live repair", true: "repair during deletion"}[deleting], func(t *testing.T) {
			pool := settingsPool()
			pool.Finalizers = append(pool.Finalizers, "example.com/other")
			node := ownedSettingsNode(t, settingsNode("worker", corev1.ConditionTrue), pool)
			receipt := node.Annotations[nodeSettingsAnnotation]
			node.Labels[membershipLabel], node.Annotations[nodeSettingsAnnotation] = "other", "{"
			pool.Status.Nodes = []nodesv1alpha1.NodePoolNodeStatus{{Name: node.Name, UID: string(node.UID), Phase: "Ready"}}
			r, store, _ := settingsFixture(t, pool, node)
			for i := 0; i < 3; i++ {
				if _, err := settingsReconcile(r); err == nil {
					t.Fatal("corruption became unrelated on retry")
				}
				current := readSettingsPool(t, store)
				if len(current.Status.Nodes) != 0 || len(current.Status.PendingSettingsCleanup) != 1 ||
					current.Status.PendingSettingsCleanup[0].UID != string(node.UID) ||
					!meta.IsStatusConditionTrue(current.Status.Conditions, "Failed") {
					t.Fatal("pending responsibility or failure was lost")
				}
			}
			if deleting {
				if err := store.Delete(context.Background(), readSettingsPool(t, store)); err != nil {
					t.Fatal(err)
				}
				if _, err := settingsReconcile(r); err == nil {
					t.Fatal("deletion bypassed corrupt ownership")
				}
				if !controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) {
					t.Fatal("released finalizer prematurely")
				}
			}
			node = readSettingsNode(t, store, node.Name)
			node.Annotations[nodeSettingsAnnotation] = receipt
			if err := store.Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			if _, err := settingsReconcile(r); err != nil {
				t.Fatal(err)
			}
			current := readSettingsPool(t, store)
			node = readSettingsNode(t, store, node.Name)
			if len(current.Status.PendingSettingsCleanup) != 0 || node.Annotations[nodeSettingsAnnotation] != "" ||
				node.Labels["workload"] != "" || node.Labels["unrelated"] != "keep" {
				t.Fatal("repair did not complete ownership-safe cleanup")
			}
			if meta.IsStatusConditionTrue(current.Status.Conditions, "Failed") {
				t.Fatal("completed cleanup retained a stale failure")
			}
			if deleting && (controllerutil.ContainsFinalizer(current, nodeSettingsFinalizer) ||
				!controllerutil.ContainsFinalizer(current, "example.com/other")) {
				t.Fatal("incorrect finalizer cleanup")
			}
		})
	}
}

func TestScenarioPendingCleanupDoesNotClaimReplacement(t *testing.T) {
	pool := settingsPool()
	pool.Status.PendingSettingsCleanup = []nodesv1alpha1.NodePoolNodeReference{{Name: "worker", UID: "old-uid"}}
	node := settingsNode("worker", corev1.ConditionTrue)
	node.UID = "replacement-uid"
	node.Labels[membershipLabel], node.Annotations[nodeSettingsAnnotation] = "other", "{"
	r, store, writes := settingsFixture(t, pool, node)
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	if len(writes.patches) != 0 || len(readSettingsPool(t, store).Status.PendingSettingsCleanup) != 0 ||
		readSettingsNode(t, store, node.Name).Annotations[nodeSettingsAnnotation] != "{" {
		t.Fatal("pending reference claimed a different Node UID")
	}
}
