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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

type lifecycleWriter struct {
	client.Client
	calls            []string
	evictionError    error
	statusError      error
	beforeNodeDelete func(context.Context, client.Object) error
	beforeEviction   func(context.Context, *corev1.Pod) error
}

func optimisticTestPatch(object client.Object, patch client.Patch) error {
	data, err := patch.Data(object)
	if err != nil {
		return err
	}
	var body struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return err
	}
	if body.Metadata.ResourceVersion == "" {
		return errors.New("missing optimistic lock")
	}
	return nil
}

func (w *lifecycleWriter) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if err := optimisticTestPatch(object, patch); err != nil {
		return err
	}
	if node, ok := object.(*corev1.Node); ok {
		var pool nodesv1alpha1.NodePool
		if err := w.Client.Get(ctx, types.NamespacedName{Name: "pool"}, &pool); err != nil {
			return err
		}
		if pool.Spec.UnreadyPolicy.Action == "Remove" && node.Spec.Unschedulable {
			reserved := false
			for _, state := range pool.Status.Nodes {
				if state.UID == string(node.UID) && isRemovalPhase(state.Phase) && state.DrainStartedAt != nil {
					reserved = true
				}
			}
			if !reserved {
				return errors.New("Node mutation preceded durable removal reservation")
			}
		}
		w.calls = append(w.calls, "patch-node:"+node.Name)
	}
	return w.Client.Patch(ctx, object, patch, options...)
}

func (w *lifecycleWriter) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if _, pod := object.(*corev1.Pod); pod {
		return errors.New("direct Pod deletion is forbidden")
	}
	if node, ok := object.(*corev1.Node); ok {
		opts := &client.DeleteOptions{}
		for _, option := range options {
			option.ApplyToDelete(opts)
		}
		if opts.Preconditions == nil || opts.Preconditions.UID == nil ||
			*opts.Preconditions.UID != node.UID || opts.Preconditions.ResourceVersion == nil ||
			*opts.Preconditions.ResourceVersion != node.ResourceVersion {
			return errors.New("Node deletion lacks identity/version preconditions")
		}
		if w.beforeNodeDelete != nil {
			if err := w.beforeNodeDelete(ctx, object); err != nil {
				return err
			}
		}
		w.calls = append(w.calls, "delete-node:"+node.Name)
	}
	return w.Client.Delete(ctx, object, options...)
}

func (w *lifecycleWriter) Status() client.SubResourceWriter {
	return &lifecycleStatusWriter{SubResourceWriter: w.Client.Status(), owner: w}
}

type lifecycleStatusWriter struct {
	client.SubResourceWriter
	owner *lifecycleWriter
}

func (w *lifecycleStatusWriter) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.SubResourcePatchOption) error {
	if err := optimisticTestPatch(object, patch); err != nil {
		return err
	}
	if w.owner.statusError != nil {
		return w.owner.statusError
	}
	w.owner.calls = append(w.owner.calls, "status")
	return w.SubResourceWriter.Patch(ctx, object, patch, options...)
}

func (w *lifecycleWriter) SubResource(name string) client.SubResourceClient {
	return &lifecycleSubresource{SubResourceClient: w.Client.SubResource(name), owner: w, name: name}
}

type lifecycleSubresource struct {
	client.SubResourceClient
	owner *lifecycleWriter
	name  string
}

func (w *lifecycleSubresource) Create(ctx context.Context, object, subresource client.Object, options ...client.SubResourceCreateOption) error {
	if w.name != "eviction" {
		return w.SubResourceClient.Create(ctx, object, subresource, options...)
	}
	pod, ok := object.(*corev1.Pod)
	eviction, correctType := subresource.(*policyv1.Eviction)
	if !ok || !correctType || eviction.DeleteOptions == nil {
		return errors.New("invalid eviction request")
	}
	opts := eviction.DeleteOptions
	if opts.GracePeriodSeconds != nil || opts.Preconditions == nil ||
		opts.Preconditions.UID == nil || *opts.Preconditions.UID != pod.UID ||
		opts.Preconditions.ResourceVersion == nil || *opts.Preconditions.ResourceVersion != pod.ResourceVersion {
		return errors.New("eviction overrides grace or lacks identity/version preconditions")
	}
	w.owner.calls = append(w.owner.calls, "evict:"+pod.Name)
	if w.owner.beforeEviction != nil {
		if err := w.owner.beforeEviction(ctx, pod); err != nil {
			return err
		}
	}
	if w.owner.evictionError != nil {
		return w.owner.evictionError
	}
	return w.owner.Client.Delete(ctx, pod)
}

func lifecycleFixture(t *testing.T, action string, nodes int, objects ...client.Object) (
	*NodePoolReconciler, client.Client, *lifecycleWriter, *time.Time, *events.FakeRecorder,
) {
	t.Helper()
	pool := settingsPool()
	pool.Spec.Labels, pool.Spec.Taints = nil, nil
	pool.Spec.UnreadyPolicy = nodesv1alpha1.UnreadyPolicy{
		Action: action, GracePeriod: "0s", DrainTimeout: "30s", MaxConcurrentRemovals: 1,
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, policyv1.AddToScheme, nodesv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	initial := []client.Object{pool}
	for i := 0; i < nodes; i++ {
		node := settingsNode(fmt.Sprintf("worker-%d", i), corev1.ConditionFalse)
		node.Spec.Unschedulable = false
		initial = append(initial, node)
	}
	initial = append(initial, objects...)
	store := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&nodesv1alpha1.NodePool{}, &corev1.Node{}, &corev1.Pod{}).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(object client.Object) []string {
			return []string{object.(*corev1.Pod).Spec.NodeName}
		}).WithObjects(initial...).Build()
	writer := &lifecycleWriter{Client: store}
	now := time.Date(2026, 10, 10, 20, 0, 0, 0, time.UTC)
	recorder := events.NewFakeRecorder(256)
	r := &NodePoolReconciler{Client: writer, APIReader: store, Scheme: scheme, Recorder: recorder, Now: func() time.Time { return now }}
	return r, store, writer, &now, recorder
}

func lifecyclePod(name string) *corev1.Pod {
	controller := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", UID: types.UID(name + "-uid"), ResourceVersion: "1",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "workload", UID: "workload-uid", Controller: &controller,
			}},
		},
		Spec:   corev1.PodSpec{NodeName: "worker-0"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func lifecycleRun(t *testing.T, r *NodePoolReconciler) {
	t.Helper()
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
}

func lifecycleState(t *testing.T, store client.Reader, name string) nodesv1alpha1.NodePoolNodeStatus {
	t.Helper()
	for _, state := range readSettingsPool(t, store).Status.Nodes {
		if state.Name == name {
			return state
		}
	}
	t.Fatalf("no status for %s", name)
	return nodesv1alpha1.NodePoolNodeStatus{}
}

func lifecycleCallCount(w *lifecycleWriter, prefix string) int {
	count := 0
	for _, call := range w.calls {
		if strings.HasPrefix(call, prefix) {
			count++
		}
	}
	return count
}

func TestLifecycleGraceClockAndRecovery(t *testing.T) {
	for _, readiness := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown, ""} {
		t.Run(string(readiness), func(t *testing.T) {
			r, store, writer, now, _ := lifecycleFixture(t, "Cordon", 1)
			pool := readSettingsPool(t, store)
			pool.Spec.UnreadyPolicy.GracePeriod = "30s"
			if err := store.Update(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			node := readSettingsNode(t, store, "worker-0")
			node.Status.Conditions = nil
			if readiness != "" {
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: readiness}}
			}
			if err := store.Status().Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			lifecycleRun(t, r)
			start := lifecycleState(t, store, node.Name).NotReadySince.DeepCopy()
			*now = now.Add(29 * time.Second)
			restarted := *r
			lifecycleRun(t, &restarted)
			if lifecycleCallCount(writer, "patch-node:") != 0 ||
				!lifecycleState(t, store, node.Name).NotReadySince.Equal(start) {
				t.Fatal("grace clock was reset or action occurred too early")
			}
			*now = now.Add(time.Second)
			lifecycleRun(t, &restarted)
			node = readSettingsNode(t, store, node.Name)
			if !node.Spec.Unschedulable || node.Annotations[nodeCordonAnnotation] != "pool-uid" {
				t.Fatal("exact grace boundary did not cordon")
			}
			node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
			if err := store.Status().Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			lifecycleRun(t, r)
			node = readSettingsNode(t, store, node.Name)
			state := lifecycleState(t, store, node.Name)
			if node.Spec.Unschedulable || node.Annotations[nodeCordonAnnotation] != "" ||
				state.Phase != "Ready" || state.NotReadySince != nil || state.DrainStartedAt != nil {
				t.Fatal("recovery did not release only our cordon and clear timers")
			}
		})
	}
}

func TestLifecycleRemovalSequenceAndRestart(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	lifecycleRun(t, r)
	if state := lifecycleState(t, store, "worker-0"); state.Phase != "Draining" || state.DrainStartedAt == nil {
		t.Fatal("removal reservation is not durable")
	}
	if writer.calls[0] != "status" || lifecycleCallCount(writer, "evict:") != 0 {
		t.Fatal("cordon/reservation pass performed eviction")
	}
	restarted := *r
	lifecycleRun(t, &restarted)
	if lifecycleCallCount(writer, "evict:") != 1 || lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("drain must evict without deleting the Node")
	}
	lifecycleRun(t, &restarted)
	if lifecycleState(t, store, "worker-0").Phase != "Removing" || lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("Removing must be persisted before Node deletion")
	}
	lifecycleRun(t, &restarted)
	lifecycleRun(t, &restarted)
	if len(readSettingsPool(t, store).Status.Nodes) != 0 || lifecycleCallCount(writer, "delete-node:") != 1 {
		t.Fatal("removal did not converge")
	}
}

func TestLifecycleBlockedRemovalAndConcurrency(t *testing.T) {
	r, store, writer, now, recorder := lifecycleFixture(t, "Remove", 2, lifecyclePod("job"))
	writer.evictionError = apierrors.NewTooManyRequests("PDB denies disruption", 10)
	lifecycleRun(t, r)
	lifecycleRun(t, r)
	if state := lifecycleState(t, store, "worker-0"); !state.Blocked || state.Reason != "EvictionBlocked" {
		t.Fatal("PDB rejection was not reported")
	}
	if readSettingsNode(t, store, "worker-1").Spec.Unschedulable ||
		lifecycleState(t, store, "worker-1").Reason != "WaitingForRemovalSlot" {
		t.Fatal("blocked removal released its concurrency slot")
	}
	*now = now.Add(31 * time.Second)
	lifecycleRun(t, r)
	if state := lifecycleState(t, store, "worker-0"); !state.Blocked || state.Reason != "DrainTimedOut" {
		t.Fatal("expired drain did not report a blocked removal")
	}
	evictions := lifecycleCallCount(writer, "evict:")
	version := readSettingsPool(t, store).ResourceVersion
	eventsBefore := len(recorder.Events)
	lifecycleRun(t, r)
	if lifecycleCallCount(writer, "evict:") != evictions || lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("timed-out removal continued eviction or deleted the Node")
	}
	if readSettingsPool(t, store).ResourceVersion != version || len(recorder.Events) != eventsBefore {
		t.Fatal("stable blocked state generated writes or transition spam")
	}
	pod := lifecyclePod("job")
	if err := store.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	if lifecycleState(t, store, "worker-0").Phase != "Removing" {
		t.Fatal("resolved blocker did not resume without resetting the timeout")
	}
}

func TestLifecyclePoliciesAndProtection(t *testing.T) {
	for _, scenario := range []string{"Ignore", "preexisting cordon", "control-plane", "master", "foreign cordon", "cleared cordon"} {
		t.Run(scenario, func(t *testing.T) {
			action := "Cordon"
			if scenario == "Ignore" {
				action = "Ignore"
			}
			r, store, writer, _, _ := lifecycleFixture(t, action, 1)
			node := readSettingsNode(t, store, "worker-0")
			switch scenario {
			case "preexisting cordon":
				node.Spec.Unschedulable = true
			case "control-plane", "master":
				node.Labels["node-role.kubernetes.io/"+scenario] = ""
			case "foreign cordon":
				node.Annotations[nodeCordonAnnotation] = "foreign-uid"
			case "cleared cordon":
				node.Annotations[nodeCordonAnnotation] = "pool-uid"
			}
			if err := store.Update(context.Background(), node); err != nil {
				t.Fatal(err)
			}
			before := readSettingsNode(t, store, node.Name)
			_, err := settingsReconcile(r)
			wantError := scenario == "foreign cordon" || scenario == "cleared cordon"
			if (err != nil) != wantError {
				t.Fatalf("unexpected error: %v", err)
			}
			if lifecycleCallCount(writer, "patch-node:") != 0 ||
				!reflect.DeepEqual(before, readSettingsNode(t, store, node.Name)) {
				t.Fatal("policy/protection overwrote a Node")
			}
		})
	}
}

func TestLifecycleCancellation(t *testing.T) {
	for _, scenario := range []string{"Ignore", "Cordon", "leave", "recover", "extend grace"} {
		t.Run(scenario, func(t *testing.T) {
			r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
			lifecycleRun(t, r)
			node := readSettingsNode(t, store, "worker-0")
			pool := readSettingsPool(t, store)
			switch scenario {
			case "Ignore", "Cordon":
				pool.Spec.UnreadyPolicy.Action = scenario
				pool.Generation++
				if err := store.Update(context.Background(), pool); err != nil {
					t.Fatal(err)
				}
			case "extend grace":
				pool.Spec.UnreadyPolicy.GracePeriod = "1h"
				pool.Generation++
				if err := store.Update(context.Background(), pool); err != nil {
					t.Fatal(err)
				}
			case "leave":
				node.Labels[membershipLabel] = "other"
				if err := store.Update(context.Background(), node); err != nil {
					t.Fatal(err)
				}
			case "recover":
				node.Status.Conditions[0].Status = corev1.ConditionTrue
				if err := store.Status().Update(context.Background(), node); err != nil {
					t.Fatal(err)
				}
			}
			lifecycleRun(t, r)
			if lifecycleCallCount(writer, "evict:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 {
				t.Fatal("cancelled action continued disruption")
			}
			if scenario == "Ignore" || scenario == "leave" || scenario == "recover" {
				if readSettingsNode(t, store, node.Name).Spec.Unschedulable {
					t.Fatal("owned cordon was not released")
				}
			}
			for _, state := range readSettingsPool(t, store).Status.Nodes {
				if state.DrainStartedAt != nil {
					t.Fatal("cancelled reservation retained its timer")
				}
			}
		})
	}
}

func TestLifecycleStatusConflictPreventsMutation(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1)
	writer.statusError = apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, "pool", errors.New("concurrent writer"))
	if _, err := settingsReconcile(r); !apierrors.IsConflict(err) {
		t.Fatalf("expected conflict: %v", err)
	}
	if readSettingsNode(t, store, "worker-0").Spec.Unschedulable || len(writer.calls) != 0 {
		t.Fatal("mutation occurred without persisted reservation")
	}
}

func TestLifecyclePodClassification(t *testing.T) {
	for _, scenario := range []string{"managed", "terminating", "unmanaged", "emptyDir", "hostPath", "mirror", "completed", "daemonset", "orphaned daemonset"} {
		t.Run(scenario, func(t *testing.T) {
			pod := lifecyclePod("job")
			var extras []client.Object
			switch scenario {
			case "terminating":
				stamp := metav1.NewTime(time.Now())
				pod.DeletionTimestamp, pod.Finalizers = &stamp, []string{"example.com/hold"}
			case "unmanaged":
				pod.OwnerReferences = nil
			case "emptyDir":
				pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}
			case "hostPath":
				pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/data"}}}}
			case "mirror":
				pod.Annotations = map[string]string{corev1.MirrorPodAnnotationKey: "mirror"}
			case "completed":
				pod.Status.Phase = corev1.PodSucceeded
			case "daemonset", "orphaned daemonset":
				pod.OwnerReferences[0].Kind = "DaemonSet"
				if scenario == "daemonset" {
					extras = append(extras, &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "default", UID: "workload-uid"}})
				}
			}
			extras = append(extras, pod)
			r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, extras...)
			lifecycleRun(t, r)
			lifecycleRun(t, r)
			state := lifecycleState(t, store, "worker-0")
			blocked := scenario == "unmanaged" || scenario == "emptyDir" || scenario == "hostPath" || scenario == "orphaned daemonset"
			if state.Blocked != blocked {
				t.Fatalf("unexpected classification: %+v", state)
			}
			wantEvictions := 0
			if scenario == "managed" {
				wantEvictions = 1
			}
			if lifecycleCallCount(writer, "evict:") != wantEvictions {
				t.Fatal("incorrect eviction behavior")
			}
		})
	}
}

func TestLifecycleSettingsFailureStillObservesHealth(t *testing.T) {
	r, store, _, _, _ := lifecycleFixture(t, "Remove", 1)
	node := readSettingsNode(t, store, "worker-0")
	node.Annotations[nodeSettingsAnnotation] = "{"
	if err := store.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("expected settings error")
	}
	pool := readSettingsPool(t, store)
	if len(pool.Status.Nodes) != 1 || pool.Status.Nodes[0].NotReadySince == nil ||
		pool.Status.Nodes[0].Phase != "NotReady" || pool.Status.ObservedGeneration != 0 ||
		!meta.IsStatusConditionTrue(pool.Status.Conditions, "Failed") {
		t.Fatal("settings failure hid health or admitted a removal")
	}
}

func TestLifecycleDeletionCordonCleanup(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprint(ready), func(t *testing.T) {
			r, store, writer, _, _ := lifecycleFixture(t, "Cordon", 1)
			lifecycleRun(t, r)
			node := readSettingsNode(t, store, "worker-0")
			if ready {
				node.Status.Conditions[0].Status = corev1.ConditionTrue
				if err := store.Status().Update(context.Background(), node); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Delete(context.Background(), readSettingsPool(t, store)); err != nil {
				t.Fatal(err)
			}
			lifecycleRun(t, r)
			node = readSettingsNode(t, store, node.Name)
			if node.Spec.Unschedulable == ready || (node.Annotations[nodeCordonAnnotation] != "") == ready {
				t.Fatal("deletion cleanup violated Ready-only eligibility")
			}
			if lifecycleCallCount(writer, "delete-node:") != 0 {
				t.Fatal("pool deletion removed a Node")
			}
		})
	}
}

func TestLifecyclePoolChangeFence(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1)
	reads := 0
	r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
		if _, pool := object.(*nodesv1alpha1.NodePool); !pool {
			return nil
		}
		reads++
		if reads != 2 {
			return nil
		}
		current := readSettingsPool(t, store)
		current.Spec.UnreadyPolicy.Action = "Ignore"
		current.Generation++
		return store.Update(ctx, current)
	}}
	if _, err := settingsReconcile(r); err == nil {
		t.Fatal("expected pool change fence")
	}
	if lifecycleCallCount(writer, "patch-node:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("pool change did not fence actions")
	}
}

func TestLifecyclePolicyValidation(t *testing.T) {
	for _, scenario := range []string{"valid", "negative grace", "fraction", "compound", "zero timeout", "leading-zero timeout", "invalid action", "zero limit"} {
		t.Run(scenario, func(t *testing.T) {
			pool := settingsPool()
			switch scenario {
			case "negative grace":
				pool.Spec.UnreadyPolicy.GracePeriod = "-1s"
			case "fraction":
				pool.Spec.UnreadyPolicy.GracePeriod = "0.5s"
			case "compound":
				pool.Spec.UnreadyPolicy.GracePeriod = "1m30s"
			case "zero timeout":
				pool.Spec.UnreadyPolicy.DrainTimeout = "0s"
			case "leading-zero timeout":
				pool.Spec.UnreadyPolicy.DrainTimeout = "01s"
			case "invalid action":
				pool.Spec.UnreadyPolicy.Action = "Destroy"
			case "zero limit":
				pool.Spec.UnreadyPolicy.MaxConcurrentRemovals = 0
			}
			_, err := parseLifecyclePolicy(pool)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("unexpected validation: %v", err)
			}
		})
	}
}

func TestLifecycleSubsecondGraceNeverActsEarly(t *testing.T) {
	r, store, _, now, _ := lifecycleFixture(t, "Cordon", 1)
	base := *now
	*now = base.Add(900 * time.Millisecond)
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.GracePeriod = "1s"
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	*now = base.Add(1800 * time.Millisecond)
	lifecycleRun(t, r)
	if readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
		t.Fatal("serialization shortened the grace period")
	}
	*now = base.Add(2 * time.Second)
	lifecycleRun(t, r)
	if !readSettingsNode(t, store, "worker-0").Spec.Unschedulable {
		t.Fatal("elapsed grace period did not act")
	}
}

func TestLifecycleFutureClockAndOldestReservation(t *testing.T) {
	pool := settingsPool()
	pool.Spec.UnreadyPolicy.Action, pool.Spec.UnreadyPolicy.GracePeriod = "Remove", "0s"
	now := time.Date(2026, 10, 10, 20, 0, 0, 0, time.UTC)
	older, newer, future := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(-time.Minute)), metav1.NewTime(now.Add(time.Hour))
	pool.Status.Nodes = []nodesv1alpha1.NodePoolNodeStatus{
		{Name: "z-oldest", UID: "z-oldest-uid", Phase: "NotReady", NotReadySince: &older},
		{Name: "a-newer", UID: "a-newer-uid", Phase: "NotReady", NotReadySince: &newer},
		{Name: "future", UID: "future-uid", Phase: "NotReady", NotReadySince: &future},
	}
	policy, err := parseLifecyclePolicy(pool)
	if err != nil {
		t.Fatal(err)
	}
	members := []corev1.Node{
		*settingsNode("middle-ready", corev1.ConditionTrue), *settingsNode("a-newer", corev1.ConditionFalse),
		*settingsNode("future", corev1.ConditionFalse), *settingsNode("z-oldest", corev1.ConditionFalse),
	}
	states, ready := planLifecycle(pool, members, policy, now, true, nil)
	if ready != 1 {
		t.Fatal("incorrect ready count")
	}
	for _, state := range states {
		if (state.Phase == "Draining") != (state.Name == "z-oldest") {
			t.Fatalf("unfair reservation: %+v", states)
		}
		if state.Name == "future" && (!state.Blocked || state.Reason != "ClockSkew") {
			t.Fatal("future clock was not blocked")
		}
	}
}

func TestLifecycleLimitReductionPreservesExistingSlots(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 3)
	pool := readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.MaxConcurrentRemovals = 2
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	pool = readSettingsPool(t, store)
	pool.Spec.UnreadyPolicy.MaxConcurrentRemovals = 1
	pool.Generation++
	if err := store.Update(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	lifecycleRun(t, r)
	if lifecycleCallCount(writer, "patch-node:") != 2 || readSettingsNode(t, store, "worker-2").Spec.Unschedulable {
		t.Fatal("limit reduction admitted another Node")
	}
	active := 0
	for _, state := range readSettingsPool(t, store).Status.Nodes {
		if isRemovalPhase(state.Phase) {
			active++
		}
	}
	if active != 2 {
		t.Fatal("already-started reservations were lost")
	}
}

func TestLifecycleNodeFinalizersBlockWithoutReleasingSlot(t *testing.T) {
	r, store, _, _, _ := lifecycleFixture(t, "Remove", 2)
	node := readSettingsNode(t, store, "worker-0")
	node.Finalizers = []string{"example.com/hold"}
	if err := store.Update(context.Background(), node); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		lifecycleRun(t, r)
	}
	node = readSettingsNode(t, store, node.Name)
	state := lifecycleState(t, store, node.Name)
	if node.DeletionTimestamp.IsZero() || len(node.Finalizers) != 1 ||
		!state.Blocked || state.Reason != "NodeFinalizersBlocked" {
		t.Fatal("Node finalizers were bypassed or not reported")
	}
	if readSettingsNode(t, store, "worker-1").Spec.Unschedulable {
		t.Fatal("pending Node deletion released its slot")
	}
}

func TestLifecycleLatePodsAndMembershipLeave(t *testing.T) {
	for _, scenario := range []string{"late Pod", "membership leave"} {
		t.Run(scenario, func(t *testing.T) {
			r, store, writer, _, recorder := lifecycleFixture(t, "Remove", 1)
			lifecycleRun(t, r)
			lifecycleRun(t, r)
			if lifecycleState(t, store, "worker-0").Phase != "Removing" {
				t.Fatal("expected persisted Removing")
			}
			for len(recorder.Events) > 0 {
				<-recorder.Events
			}
			if scenario == "late Pod" {
				pod := lifecyclePod("late")
				pod.ResourceVersion = ""
				if err := store.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			} else {
				node := readSettingsNode(t, store, "worker-0")
				node.Labels[membershipLabel] = "other"
				if err := store.Update(context.Background(), node); err != nil {
					t.Fatal(err)
				}
			}
			lifecycleRun(t, r)
			if lifecycleCallCount(writer, "delete-node:") != 0 {
				t.Fatal("late change did not stop deletion")
			}
			if scenario == "late Pod" {
				state := lifecycleState(t, store, "worker-0")
				if state.Phase != "Draining" || state.Reason != "PodsAppeared" {
					t.Fatal("late Pod was not reported")
				}
			} else {
				var recorded []string
				for len(recorder.Events) > 0 {
					recorded = append(recorded, <-recorder.Events)
				}
				if strings.Contains(strings.Join(recorded, "\n"), "NodeRemoved") {
					t.Fatal("membership leave was falsely reported as Node deletion")
				}
			}
		})
	}
}

func TestLifecycleFreshReadinessAndCordonFence(t *testing.T) {
	for _, scenario := range []string{"recovered", "uncordoned"} {
		t.Run(scenario, func(t *testing.T) {
			r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
			lifecycleRun(t, r)
			reads := 0
			r.APIReader = &settingsReadClient{Reader: store, beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
				if _, node := object.(*corev1.Node); !node {
					return nil
				}
				reads++
				target := 2
				if scenario == "uncordoned" {
					target = 3
				}
				if reads != target {
					return nil
				}
				current := readSettingsNode(t, store, key.Name)
				if scenario == "recovered" {
					current.Status.Conditions[0].Status = corev1.ConditionTrue
					return store.Status().Update(ctx, current)
				}
				current.Spec.Unschedulable = false
				return store.Update(ctx, current)
			}}
			if _, err := settingsReconcile(r); err == nil {
				t.Fatal("expected fresh Node fence")
			}
			if lifecycleCallCount(writer, "evict:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 {
				t.Fatal("fresh Node change did not stop disruption")
			}
		})
	}
}

func TestLifecycleDeadlineCheckedBetweenEvictions(t *testing.T) {
	r, store, writer, now, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("a"), lifecyclePod("b"))
	lifecycleRun(t, r)
	writer.beforeEviction = func(context.Context, *corev1.Pod) error {
		*now = now.Add(31 * time.Second)
		return nil
	}
	lifecycleRun(t, r)
	if lifecycleCallCount(writer, "evict:") != 1 || lifecycleState(t, store, "worker-0").Reason != "DrainTimedOut" {
		t.Fatal("eviction continued after the deadline")
	}
}

func TestLifecycleEvictionErrorsAreReported(t *testing.T) {
	r, store, writer, _, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("job"))
	lifecycleRun(t, r)
	writer.evictionError = apierrors.NewForbidden(schema.GroupResource{Resource: "pods/eviction"}, "job", errors.New("denied"))
	if _, err := settingsReconcile(r); !apierrors.IsForbidden(err) {
		t.Fatalf("missing API error: %v", err)
	}
	pool := readSettingsPool(t, store)
	if !meta.IsStatusConditionTrue(pool.Status.Conditions, "Failed") || !pool.Status.Nodes[0].Blocked ||
		lifecycleCallCount(writer, "delete-node:") != 0 {
		t.Fatal("eviction failure was hidden")
	}
}
