package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

func TestProtectedSettingsAndIndependentWorkers(t *testing.T) {
	for _, role := range []string{"node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master"} {
		for _, readiness := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown, ""} {
			for _, action := range []string{"Ignore", "Cordon", "Remove"} {
				t.Run(fmt.Sprintf("%s/%s/%s", role, readiness, action), func(t *testing.T) {
					pool := settingsPool()
					pool.Spec.UnreadyPolicy.Action = action
					pool.Spec.Taints[0].Effect = corev1.TaintEffectNoExecute
					protected := settingsNode("a-protected", readiness)
					protected.Labels[role] = ""
					worker := settingsNode("z-worker", corev1.ConditionTrue)
					r, store, writes := settingsFixture(t, pool, protected, worker)
					before := readSettingsNode(t, store, protected.Name)
					if _, err := settingsReconcile(r); err == nil {
						t.Fatal("protected desired settings were not rejected")
					}
					if !reflect.DeepEqual(before, readSettingsNode(t, store, protected.Name)) {
						t.Fatal("protected Node was mutated")
					}
					got := readSettingsNode(t, store, worker.Name)
					if got.Labels["workload"] != "batch" || len(got.Spec.Taints) != 2 ||
						got.Spec.Taints[1].Effect != corev1.TaintEffectNoExecute {
						t.Fatal("independent worker settings did not converge")
					}
					state := lifecycleState(t, store, protected.Name)
					current := readSettingsPool(t, store)
					if !state.Blocked || state.Reason != "ProtectedNode" ||
						!meta.IsStatusConditionTrue(current.Status.Conditions, "Failed") ||
						current.Status.ObservedGeneration != 0 {
						t.Fatalf("protection or partial failure was hidden: %+v", current.Status)
					}
					wantReady := int32(1)
					if readiness == corev1.ConditionTrue {
						wantReady++
					}
					if current.Status.ReadyNodes != wantReady || len(writes.patches) != 1 {
						t.Fatal("authorization failure changed health counting or patched the protected Node")
					}
				})
			}
		}
	}
}

func TestProtectedEmptySettingsStayVisible(t *testing.T) {
	for _, action := range []string{"Ignore", "Cordon", "Remove"} {
		t.Run(action, func(t *testing.T) {
			r, store, writer, _, _ := lifecycleFixture(t, action, 1)
			node := readSettingsNode(t, store, "worker-0")
			node.Labels[controlPlaneRoleLabel] = ""
			if err := store.Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			lifecycleRun(t, r)
			state := lifecycleState(t, store, node.Name)
			if state.Reason != "ProtectedNode" || !state.Blocked || lifecycleCallCount(writer, "patch-node:") != 0 ||
				lifecycleCallCount(writer, "delete-node:") != 0 || lifecycleCallCount(writer, "evict:") != 0 {
				t.Fatal("protected member was hidden or disrupted")
			}
		})
	}
}

func TestReservedRoleLabelsAndLegacyReceipts(t *testing.T) {
	for _, role := range []string{"node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master"} {
		t.Run(role, func(t *testing.T) {
			node := settingsNode("worker", corev1.ConditionTrue)
			if _, err := planNodeSettings(node, "pool-uid", map[string]string{role: ""}, nil); err == nil {
				t.Fatal("desired role label was accepted")
			}
			node.Labels[role] = ""
			record, err := json.Marshal(nodeSettingsOwnership{PoolUID: "pool-uid", Labels: map[string]string{role: ""}})
			if err != nil {
				t.Fatal(err)
			}
			node.Annotations[nodeSettingsAnnotation] = string(record)
			before := node.DeepCopy()
			if planned, err := planNodeSettings(node, "pool-uid", nil, nil); err == nil ||
				!strings.Contains(err.Error(), "manual repair") || planned != nil {
				t.Fatal("legacy authority receipt was silently cleaned")
			}
			if !reflect.DeepEqual(before, node) {
				t.Fatal("failed planner mutated its input")
			}
		})
	}
}

func TestProtectedCleanupBoundary(t *testing.T) {
	for _, mode := range []string{"leave", "clear settings", "delete pool"} {
		for _, readiness := range []corev1.ConditionStatus{corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown, ""} {
			t.Run(mode+"/"+string(readiness), func(t *testing.T) {
				pool := settingsPool()
				node := ownedSettingsNode(t, settingsNode("worker", readiness), pool)
				node.Labels[controlPlaneRoleLabel] = ""
				node.Annotations[nodeCordonAnnotation] = string(pool.UID)
				switch mode {
				case "leave":
					node.Labels[membershipLabel] = "other"
				case "clear settings":
					pool.Spec.Labels, pool.Spec.Taints = nil, nil
				case "delete pool":
					now := metav1.Now()
					pool.DeletionTimestamp = &now
					pool.Finalizers = append(pool.Finalizers, "example.com/test-hold")
				}
				r, store, _ := settingsFixture(t, pool, node)
				before := readSettingsNode(t, store, node.Name)
				_, err := settingsReconcile(r)
				clean := readiness == corev1.ConditionTrue
				if (err != nil) != (!clean && mode != "delete pool") {
					t.Fatalf("unexpected cleanup result: %v", err)
				}
				got := readSettingsNode(t, store, node.Name)
				if clean {
					if got.Labels["workload"] != "" || got.Annotations[nodeSettingsAnnotation] != "" ||
						got.Annotations[nodeCordonAnnotation] != "" || got.Spec.Unschedulable ||
						len(got.Spec.Taints) != 1 || got.Labels["unrelated"] != "keep" {
						t.Fatal("eligible owned cleanup did not preserve unrelated state")
					}
					if _, present := got.Labels[controlPlaneRoleLabel]; !present {
						t.Fatal("cleanup removed the protection marker")
					}
				} else if !reflect.DeepEqual(before, got) {
					t.Fatal("unready protected cleanup mutated the Node")
				}
				if mode == "delete pool" && controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) {
					t.Fatal("assignment's unready-skip contract did not permit finalization")
				}
			})
		}
	}
}

func TestMemberFailureDoesNotAcquireNewSlot(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 2)
	node := readSettingsNode(t, store, "worker-0")
	node.Annotations[nodeSettingsAnnotation] = "{"
	if err := store.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("member error was hidden")
	}
	bad, good := lifecycleState(t, store, "worker-0"), lifecycleState(t, store, "worker-1")
	if bad.Reason != "SettingsFailed" || !bad.Blocked || bad.DrainStartedAt != nil ||
		good.Phase != "Draining" || good.Blocked || !readSettingsNode(t, store, "worker-1").Spec.Unschedulable {
		t.Fatalf("failed member starved valid work: bad=%+v good=%+v", bad, good)
	}
	current := readSettingsPool(t, store)
	if !meta.IsStatusConditionTrue(current.Status.Conditions, "Failed") ||
		!meta.IsStatusConditionTrue(current.Status.Conditions, "Progressing") ||
		current.Status.ObservedGeneration != 0 || lifecycleCallCount(writer, "patch-node:") != 1 {
		t.Fatal("partial progress or generation semantics are incorrect")
	}
}

func TestActiveMemberFailureRetainsSlotAndRecovers(t *testing.T) {
	for _, failure := range []string{"settings", "protected"} {
		t.Run(failure, func(t *testing.T) {
			r, store, writer, now, _ := lifecycleFixture(t, "Remove", 2, lifecyclePod("job"))
			lifecycleRun(t, r)
			start := lifecycleState(t, store, "worker-0").DrainStartedAt.DeepCopy()
			node := readSettingsNode(t, store, "worker-0")
			if failure == "settings" {
				node.Annotations[nodeSettingsAnnotation] = "{"
			} else {
				node.Labels[controlPlaneRoleLabel] = ""
			}
			if err := store.Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(5 * time.Second)
			_, err := settingsReconcile(r)
			if failure == "settings" && err == nil {
				t.Fatal("settings error was hidden")
			}
			state := lifecycleState(t, store, node.Name)
			if state.Phase != "Draining" || !state.Blocked || !state.DrainStartedAt.Equal(start) ||
				readSettingsNode(t, store, "worker-1").Spec.Unschedulable ||
				lifecycleCallCount(writer, "evict:") != 0 {
				t.Fatal("failure lost its reservation or allowed disruption")
			}
			node = readSettingsNode(t, store, node.Name)
			delete(node.Annotations, nodeSettingsAnnotation)
			delete(node.Labels, controlPlaneRoleLabel)
			if err := store.Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			lifecycleRun(t, r)
			if lifecycleCallCount(writer, "evict:") != 1 ||
				!lifecycleState(t, store, node.Name).DrainStartedAt.Equal(start) {
				t.Fatal("repair did not resume the old deadline")
			}
		})
	}
}

func TestMemberActionFailureAllowsOtherEviction(t *testing.T) {
	first, second := lifecyclePod("a"), lifecyclePod("b")
	second.Spec.NodeName = "worker-1"
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 2, first, second)
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.MaxConcurrentRemovals = 2
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	pool = readSettingsPool(t, store)
	previousGeneration := pool.Status.ObservedGeneration
	pool.Generation++
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	writer.beforeEviction = func(_ context.Context, pod *corev1.Pod) error {
		if pod.Name == "a" {
			return apierrors.NewForbidden(schema.GroupResource{Resource: "pods/eviction"}, pod.Name, errors.New("member failure"))
		}
		return nil
	}
	if _, err := settingsReconcile(r); !apierrors.IsForbidden(err) {
		t.Fatalf("member error lost its API identity: %v", err)
	}
	var remaining corev1.Pod
	if err := store.Get(context.Background(), client.ObjectKeyFromObject(first), &remaining); err != nil {
		t.Fatal("failed member's Pod disappeared")
	}
	if err := store.Get(context.Background(), client.ObjectKeyFromObject(second), &remaining); !apierrors.IsNotFound(err) {
		t.Fatalf("other member did not progress: %v", err)
	}
	pool = readSettingsPool(t, store)
	if lifecycleState(t, store, "worker-0").Reason != "ActionFailed" ||
		!meta.IsStatusConditionTrue(pool.Status.Conditions, "Progressing") ||
		!meta.IsStatusConditionTrue(pool.Status.Conditions, "Failed") ||
		pool.Status.ObservedGeneration != previousGeneration {
		t.Fatal("partial action failure was reported as global failure or full success")
	}
}

func TestPoolFailureStopsAllLifecycleActions(t *testing.T) {
	first, second := lifecyclePod("a"), lifecyclePod("b")
	second.Spec.NodeName = "worker-1"
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 2, first, second)
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.MaxConcurrentRemovals = 2
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	reads := 0
	r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(_ context.Context, _ client.ObjectKey, object client.Object) error {
		if _, ok := object.(*nodesv1alpha1.NodePool); ok {
			reads++
			if reads > 1 {
				return errors.New("pool authorization unavailable")
			}
		}
		return nil
	}}
	if _, err := settingsReconcile(r); err == nil || !isPoolScopeError(err) {
		t.Fatalf("shared authorization failure not classified: %v", err)
	}
	if lifecycleCallCount(writer, "evict:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 ||
		!meta.IsStatusConditionFalse(readSettingsPool(t, store).Status.Conditions, "Progressing") {
		t.Fatal("shared failure allowed further actions or claimed progress")
	}
}

func TestDeletionCleanupContinuesAfterMemberFailure(t *testing.T) {
	pool := settingsPool()
	first := ownedSettingsNode(t, settingsNode("a-bad", corev1.ConditionTrue), pool)
	second := ownedSettingsNode(t, settingsNode("z-good", corev1.ConditionTrue), pool)
	first.Annotations[nodeSettingsAnnotation] = "{"
	now := metav1.Now()
	pool.DeletionTimestamp = &now
	pool.Finalizers = append(pool.Finalizers, "example.com/test-hold")
	r, store, _ := settingsFixture(t, pool, first, second)
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("cleanup failure hidden")
	}
	good := readSettingsNode(t, store, second.Name)
	if good.Labels["workload"] != "" || good.Annotations[nodeSettingsAnnotation] != "" ||
		good.Labels["unrelated"] != "keep" {
		t.Fatal("later eligible cleanup did not complete")
	}
	if !controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) ||
		readSettingsNode(t, store, first.Name).Annotations[nodeSettingsAnnotation] != "{" {
		t.Fatal("failed responsibility was discarded")
	}
}

func TestFreshProtectedRoleAndNodeReadIsolation(t *testing.T) {
	for _, fault := range []string{"role appeared", "read forbidden"} {
		t.Run(fault, func(t *testing.T) {
			pool := settingsPool()
			first, second := settingsNode("a-worker", corev1.ConditionTrue), settingsNode("z-worker", corev1.ConditionTrue)
			r, store, _ := settingsFixture(t, pool, first, second)
			r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
				if _, ok := object.(*corev1.Node); !ok || key.Name != first.Name {
					return nil
				}
				if fault == "read forbidden" {
					return apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, key.Name, errors.New("read denied"))
				}
				node := readSettingsNode(t, store, first.Name)
				node.Labels[controlPlaneRoleLabel] = ""
				return store.Update(ctx, node)
			}}
			if _, err := settingsReconcile(r); err == nil {
				t.Fatal("injected member failure was hidden")
			}
			if readSettingsNode(t, store, first.Name).Labels["workload"] != "" ||
				readSettingsNode(t, store, second.Name).Labels["workload"] != "batch" {
				t.Fatal("stale classification permitted mutation or failure blocked another member")
			}
		})
	}
}

func TestSharedErrorUnwrapAndDeterministicMemberErrors(t *testing.T) {
	err := apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, "n", errors.New("changed"))
	wrapped := fmt.Errorf("outer: %w", &poolScopeError{cause: err})
	if !isPoolScopeError(wrapped) || !apierrors.IsConflict(wrapped) || isPoolScopeError(err) {
		t.Fatal("error scope or underlying API identity was lost")
	}
	joined := joinNodeFailures(map[string]error{"z": errors.New("last"), "a": errors.New("first")})
	if joined.Error() != "first\nlast" || joinNodeFailures(nil) != nil {
		t.Fatal("member error aggregation is unstable")
	}
}

func TestProtectedPolicyCancellationDoesNotRenewRemoval(t *testing.T) {
	r, store, _, _, _ := lifecycleFixture(t, "Remove", 1)
	lifecycleRun(t, r)
	node := readSettingsNode(t, store, "worker-0")
	node.Labels[legacyMasterRoleLabel] = ""
	if err := store.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.Action = "Ignore"
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	// The unsafe unready cordon cleanup is reported, but Ignore cancels removal intent.
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("unready protected cordon cleanup was hidden")
	}
	state := lifecycleState(t, store, "worker-0")
	if state.Phase != "NotReady" || state.DrainStartedAt != nil || state.Reason != "ProtectedNode" {
		t.Fatalf("policy cancellation retained removal intent: %+v", state)
	}
}

func TestMemberReplacementFailureDoesNotActOnNewUID(t *testing.T) {
	pool := settingsPool()
	pool.Spec.UnreadyPolicy.Action, pool.Spec.UnreadyPolicy.GracePeriod = "Remove", "0s"
	first, second := settingsNode("a-worker", corev1.ConditionFalse), settingsNode("z-worker", corev1.ConditionTrue)
	r, store, _ := settingsFixture(t, pool, first, second)
	replaced := false
	r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
		if _, ok := object.(*corev1.Node); !ok || key.Name != first.Name || replaced {
			return nil
		}
		replaced = true
		node := readSettingsNode(t, store, first.Name)
		if err := store.Delete(ctx, node); err != nil {
			return err
		}
		node.UID, node.ResourceVersion = types.UID("replacement"), ""
		return store.Create(ctx, node)
	}}
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("replacement race hidden")
	}
	state := lifecycleState(t, store, first.Name)
	if state.UID != "replacement" || state.Reason != "SettingsFailed" || state.DrainStartedAt != nil ||
		readSettingsNode(t, store, first.Name).Annotations[nodeSettingsAnnotation] != "" ||
		readSettingsNode(t, store, second.Name).Labels["workload"] != "batch" {
		t.Fatal("replacement inherited authorization or blocked independent work")
	}
}
