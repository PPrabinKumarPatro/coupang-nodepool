package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

type settingsPatchClient struct {
	client.Client
	beforePatch func(context.Context, client.Object) error
	patches     []string
}

func (c *settingsPatchClient) Get(
	context.Context, client.ObjectKey, client.Object, ...client.GetOption,
) error {
	return errors.New("settings reconciliation must not read through the cached client")
}

func (c *settingsPatchClient) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("settings reconciliation must not list through the cached client")
}

func (c *settingsPatchClient) Patch(
	ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption,
) error {
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
		return errors.New("mutation patch has no optimistic resourceVersion precondition")
	}
	c.patches = append(c.patches, fmt.Sprintf("%T/%s", object, object.GetName()))
	if c.beforePatch != nil {
		if err := c.beforePatch(ctx, object); err != nil {
			return err
		}
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

type settingsReadClient struct {
	client.Reader
	beforeGet func(context.Context, client.ObjectKey, client.Object) error
	listError error
}

func (r *settingsReadClient) Get(
	ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption,
) error {
	if r.beforeGet != nil {
		if err := r.beforeGet(ctx, key, object); err != nil {
			return err
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func (r *settingsReadClient) List(
	ctx context.Context, list client.ObjectList, options ...client.ListOption,
) error {
	if r.listError != nil {
		return r.listError
	}
	return r.Reader.List(ctx, list, options...)
}

func settingsPool() *nodesv1alpha1.NodePool {
	return &nodesv1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pool", UID: "pool-uid", ResourceVersion: "1", Generation: 1,
			Finalizers: []string{nodeSettingsFinalizer},
		},
		Spec: nodesv1alpha1.NodePoolSpec{
			Image:  "ubuntu-test",
			Labels: map[string]string{"workload": "batch"},
			Taints: []corev1.Taint{{
				Key: "dedicated", Value: "batch", Effect: corev1.TaintEffectNoSchedule,
			}},
			UnreadyPolicy: nodesv1alpha1.UnreadyPolicy{
				GracePeriod: "5m", Action: "Ignore", MaxConcurrentRemovals: 1,
			},
		},
	}
}

func settingsNode(name string, ready corev1.ConditionStatus) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, UID: types.UID(name + "-uid"), ResourceVersion: "1",
			Labels:      map[string]string{membershipLabel: "pool", "unrelated": "keep"},
			Annotations: map[string]string{"unrelated.example.com/note": "keep"},
		},
		Spec: corev1.NodeSpec{
			Unschedulable: true,
			ProviderID:    "example://" + name,
			Taints: []corev1.Taint{{
				Key: "unrelated.example.com/keep", Effect: corev1.TaintEffectNoSchedule,
			}},
		},
	}
	if ready != "" {
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}
	}
	return node
}

func settingsFixture(
	t *testing.T, pool *nodesv1alpha1.NodePool, nodes ...*corev1.Node,
) (*NodePoolReconciler, client.Client, *settingsPatchClient) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := nodesv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objects := []client.Object{pool}
	for _, node := range nodes {
		objects = append(objects, node)
	}
	store := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&nodesv1alpha1.NodePool{}, &corev1.Node{}).
		WithObjects(objects...).Build()
	writes := &settingsPatchClient{Client: store}
	return &NodePoolReconciler{Client: writes, APIReader: store, Scheme: scheme}, store, writes
}

func settingsReconcile(r *NodePoolReconciler) (ctrl.Result, error) {
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pool"},
	})
}

func readSettingsNode(t *testing.T, store client.Reader, name string) *corev1.Node {
	t.Helper()
	var node corev1.Node
	if err := store.Get(context.Background(), types.NamespacedName{Name: name}, &node); err != nil {
		t.Fatal(err)
	}
	return &node
}

func readSettingsPool(t *testing.T, store client.Reader) *nodesv1alpha1.NodePool {
	t.Helper()
	var pool nodesv1alpha1.NodePool
	if err := store.Get(context.Background(), types.NamespacedName{Name: "pool"}, &pool); err != nil {
		t.Fatal(err)
	}
	return &pool
}

func ownedSettingsNode(t *testing.T, node *corev1.Node, pool *nodesv1alpha1.NodePool) *corev1.Node {
	t.Helper()
	owned, err := planNodeSettings(node, pool.UID, pool.Spec.Labels, pool.Spec.Taints)
	if err != nil {
		t.Fatal(err)
	}
	return owned
}

func TestSettingsFinalizerBeforeMutation(t *testing.T) {
	pool := settingsPool()
	pool.Finalizers = []string{"example.com/other"}
	node := settingsNode("worker", corev1.ConditionTrue)
	r, store, writes := settingsFixture(t, pool, node)
	before := readSettingsNode(t, store, node.Name)
	result, err := settingsReconcile(r)
	if err != nil || result.RequeueAfter <= 0 {
		t.Fatalf("finalizer pass: result=%+v error=%v", result, err)
	}
	persisted := readSettingsPool(t, store)
	if !controllerutil.ContainsFinalizer(persisted, nodeSettingsFinalizer) ||
		!controllerutil.ContainsFinalizer(persisted, "example.com/other") {
		t.Fatal("finalizer was not durably added without removing the other finalizer")
	}
	if len(writes.patches) != 1 || !reflect.DeepEqual(readSettingsNode(t, store, node.Name), before) {
		t.Fatal("first pass must patch only the pool, leaving the Node untouched")
	}
	writes.beforePatch = func(ctx context.Context, object client.Object) error {
		if _, ok := object.(*corev1.Node); ok {
			live := readSettingsPool(t, store)
			if !controllerutil.ContainsFinalizer(live, nodeSettingsFinalizer) {
				return errors.New("Node patch preceded durable finalizer")
			}
		}
		return nil
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	got := readSettingsNode(t, store, node.Name)
	if got.Labels["workload"] != "batch" || got.Annotations[nodeSettingsAnnotation] == "" {
		t.Fatal("second pass did not apply and record settings")
	}
	count := len(writes.patches)
	poolVersion := readSettingsPool(t, store).ResourceVersion
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	if len(writes.patches) != count || readSettingsPool(t, store).ResourceVersion != poolVersion {
		t.Fatal("unchanged reconciliation should not patch Nodes or status")
	}
}

func TestSettingsFinalizerAddConflict(t *testing.T) {
	pool := settingsPool()
	pool.Finalizers = nil
	node := settingsNode("worker", corev1.ConditionTrue)
	r, store, writes := settingsFixture(t, pool, node)
	before := readSettingsNode(t, store, node.Name)
	writes.beforePatch = func(context.Context, client.Object) error {
		return apierrors.NewConflict(schema.GroupResource{Group: "nodes.example.com", Resource: "nodepools"},
			pool.Name, errors.New("concurrent update"))
	}
	if _, err := settingsReconcile(r); !apierrors.IsConflict(err) {
		t.Fatalf("expected finalizer conflict, got %v", err)
	}
	if len(writes.patches) != 1 || !reflect.DeepEqual(readSettingsNode(t, store, node.Name), before) {
		t.Fatal("failed finalizer write must not mutate a Node")
	}
}

func TestSettingsApplyRegardlessOfReadiness(t *testing.T) {
	for _, ready := range []corev1.ConditionStatus{
		corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionUnknown, "",
	} {
		t.Run(string(ready), func(t *testing.T) {
			pool := settingsPool()
			node := settingsNode("worker", ready)
			r, store, _ := settingsFixture(t, pool, node)
			if _, err := settingsReconcile(r); err != nil {
				t.Fatal(err)
			}
			got := readSettingsNode(t, store, node.Name)
			if got.Labels["workload"] != "batch" || len(got.Spec.Taints) != 2 ||
				got.Annotations["unrelated.example.com/note"] != "keep" ||
				!got.Spec.Unschedulable || got.Spec.ProviderID != node.Spec.ProviderID ||
				!reflect.DeepEqual(got.Status, node.Status) {
				t.Fatal("settings were not applied independently of health while preserving unrelated state")
			}
			status := readSettingsPool(t, store).Status
			wantReady := int32(0)
			if ready == corev1.ConditionTrue {
				wantReady = 1
			}
			if status.ReadyNodes != wantReady || status.ObservedGeneration != pool.Generation {
				t.Fatalf("incorrect health count or observedGeneration: %+v", status)
			}
		})
	}
}

func TestSettingsSpecChangeAndMembershipLeave(t *testing.T) {
	pool := settingsPool()
	node := settingsNode("worker", corev1.ConditionFalse)
	r, store, _ := settingsFixture(t, pool, node)
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	livePool := readSettingsPool(t, store)
	livePool.Spec.Labels = map[string]string{"replacement": "value"}
	livePool.Spec.Taints = nil
	livePool.Generation++
	if err := store.Update(context.Background(), livePool); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	liveNode := readSettingsNode(t, store, node.Name)
	if _, found := liveNode.Labels["workload"]; found || liveNode.Labels["replacement"] != "value" ||
		len(liveNode.Spec.Taints) != 1 {
		t.Fatal("spec changes did not remove formerly owned fields")
	}
	liveNode.Labels[membershipLabel] = "another-pool"
	liveNode.Labels["replacement"] = "human-value"
	if err := store.Update(context.Background(), liveNode); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	got := readSettingsNode(t, store, node.Name)
	if got.Annotations[nodeSettingsAnnotation] != "" || got.Labels["replacement"] != "human-value" ||
		got.Labels[membershipLabel] != "another-pool" || len(readSettingsPool(t, store).Status.Nodes) != 0 {
		t.Fatal("leaving a pool must release ownership without overwriting another actor")
	}
}

func TestSettingsDeletionEligibility(t *testing.T) {
	pool := settingsPool()
	pool.Finalizers = append(pool.Finalizers, "example.com/other")
	ready := ownedSettingsNode(t, settingsNode("ready", corev1.ConditionTrue), pool)
	notReady := ownedSettingsNode(t, settingsNode("not-ready", corev1.ConditionFalse), pool)
	unknown := ownedSettingsNode(t, settingsNode("unknown", corev1.ConditionUnknown), pool)
	missing := ownedSettingsNode(t, settingsNode("missing", ""), pool)
	former := ownedSettingsNode(t, settingsNode("former", corev1.ConditionTrue), pool)
	former.Labels[membershipLabel] = "another-pool"
	external := ownedSettingsNode(t, settingsNode("external", corev1.ConditionTrue), pool)
	external.Labels["workload"] = "human-value"
	external.Spec.Taints[1].Value = "human-value"
	foreignPool := settingsPool()
	foreignPool.UID = "other-pool-uid"
	foreign := ownedSettingsNode(t, settingsNode("foreign", corev1.ConditionTrue), foreignPool)
	unowned := settingsNode("unowned", corev1.ConditionTrue)
	unowned.Labels["workload"] = "batch"
	unowned.Spec.Taints = append(unowned.Spec.Taints, pool.Spec.Taints...)
	r, store, _ := settingsFixture(t, pool, ready, notReady, unknown, missing, former, external, foreign, unowned)
	ineligible := []*corev1.Node{notReady, unknown, missing, foreign, unowned}
	for i, node := range ineligible {
		ineligible[i] = readSettingsNode(t, store, node.Name)
	}
	if err := store.Delete(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	for _, node := range []*corev1.Node{ready, former} {
		got := readSettingsNode(t, store, node.Name)
		if got.Annotations[nodeSettingsAnnotation] != "" || got.Labels["workload"] != "" ||
			len(got.Spec.Taints) != 1 {
			t.Fatalf("Ready UID-owned Node %s was not cleaned", node.Name)
		}
	}
	for _, node := range ineligible {
		if !reflect.DeepEqual(readSettingsNode(t, store, node.Name), node) {
			t.Fatalf("ineligible Node %s was changed", node.Name)
		}
	}
	got := readSettingsNode(t, store, external.Name)
	if got.Labels["workload"] != "human-value" || got.Spec.Taints[1].Value != "human-value" ||
		got.Annotations[nodeSettingsAnnotation] != "" {
		t.Fatal("deletion cleanup did not preserve externally modified values")
	}
	livePool := readSettingsPool(t, store)
	if controllerutil.ContainsFinalizer(livePool, nodeSettingsFinalizer) ||
		!controllerutil.ContainsFinalizer(livePool, "example.com/other") {
		t.Fatal("cleanup must release only this controller's finalizer")
	}
}

func TestSettingsDeletionRetryAndFinalizerRemovalConflict(t *testing.T) {
	pool := settingsPool()
	pool.Finalizers = append(pool.Finalizers, "example.com/other")
	node := ownedSettingsNode(t, settingsNode("worker", corev1.ConditionTrue), pool)
	r, store, writes := settingsFixture(t, pool, node)
	if err := store.Delete(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	writes.beforePatch = func(ctx context.Context, object client.Object) error {
		if _, ok := object.(*corev1.Node); ok {
			return apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, node.Name,
				errors.New("concurrent Node change"))
		}
		return nil
	}
	if _, err := settingsReconcile(r); !apierrors.IsConflict(err) {
		t.Fatalf("expected cleanup conflict, got %v", err)
	}
	if !controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) {
		t.Fatal("failed cleanup released the finalizer")
	}
	writes.beforePatch = func(ctx context.Context, object client.Object) error {
		if _, ok := object.(*nodesv1alpha1.NodePool); ok {
			return apierrors.NewConflict(schema.GroupResource{Resource: "nodepools"}, pool.Name,
				errors.New("concurrent pool change"))
		}
		return nil
	}
	if _, err := settingsReconcile(r); !apierrors.IsConflict(err) {
		t.Fatalf("expected finalizer removal conflict, got %v", err)
	}
	if readSettingsNode(t, store, node.Name).Annotations[nodeSettingsAnnotation] != "" ||
		!controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) {
		t.Fatal("cleanup progress or finalizer was lost")
	}
	writes.beforePatch = nil
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	if controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) {
		t.Fatal("idempotent retry did not complete finalization")
	}
}

func TestSettingsOwnershipConflicts(t *testing.T) {
	for _, scenario := range []string{"foreign UID", "malformed", "external change", "known former malformed"} {
		t.Run(scenario, func(t *testing.T) {
			pool := settingsPool()
			node := settingsNode("worker", corev1.ConditionTrue)
			switch scenario {
			case "foreign UID":
				node.Annotations[nodeSettingsAnnotation] = `{"poolUID":"other-uid"}`
			case "malformed":
				node.Annotations[nodeSettingsAnnotation] = `{`
			case "external change":
				node = ownedSettingsNode(t, node, pool)
				node.Labels["workload"] = "human-value"
			case "known former malformed":
				node.Labels[membershipLabel] = "another-pool"
				node.Annotations[nodeSettingsAnnotation] = `{`
				pool.Status.Nodes = []nodesv1alpha1.NodePoolNodeStatus{{
					Name: node.Name, UID: string(node.UID), Phase: "Ready",
				}}
			}
			r, store, writes := settingsFixture(t, pool, node)
			before := readSettingsNode(t, store, node.Name)
			if _, err := settingsReconcile(r); err == nil {
				t.Fatal("expected ownership conflict")
			}
			if len(writes.patches) != 0 || !reflect.DeepEqual(readSettingsNode(t, store, node.Name), before) {
				t.Fatal("ownership conflict must not change the Node")
			}
		})
	}
}

func TestSettingsFreshNodeReads(t *testing.T) {
	for _, scenario := range []string{"membership changed", "deleted", "replaced UID"} {
		t.Run(scenario, func(t *testing.T) {
			pool := settingsPool()
			node := settingsNode("worker", corev1.ConditionTrue)
			r, store, writes := settingsFixture(t, pool, node)
			changed := false
			r.APIReader = &settingsReadClient{
				Reader: store,
				beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
					if _, ok := object.(*corev1.Node); !ok || changed {
						return nil
					}
					changed = true
					live := readSettingsNode(t, store, node.Name)
					if scenario == "membership changed" {
						live.Labels[membershipLabel] = "another-pool"
						return store.Update(ctx, live)
					}
					if err := store.Delete(ctx, live); err != nil {
						return err
					}
					if scenario == "replaced UID" {
						replacement := settingsNode(node.Name, corev1.ConditionTrue)
						replacement.UID = "replacement-uid"
						replacement.ResourceVersion = ""
						return store.Create(ctx, replacement)
					}
					return nil
				},
			}
			_, err := settingsReconcile(r)
			if (err != nil) != (scenario == "replaced UID") {
				t.Fatalf("scenario %s: unexpected error %v", scenario, err)
			}
			if len(writes.patches) != 0 {
				t.Fatal("a stale Node-list entry caused a mutation")
			}
		})
	}
}

func TestSettingsPoolFence(t *testing.T) {
	for _, scenario := range []string{"spec changed", "deletion started", "finalizer removed", "pool replaced"} {
		t.Run(scenario, func(t *testing.T) {
			pool := settingsPool()
			node := settingsNode("worker", corev1.ConditionTrue)
			r, store, writes := settingsFixture(t, pool, node)
			before := readSettingsNode(t, store, node.Name)
			reads := 0
			r.APIReader = &settingsReadClient{
				Reader: store,
				beforeGet: func(ctx context.Context, key client.ObjectKey, object client.Object) error {
					if _, ok := object.(*nodesv1alpha1.NodePool); !ok {
						return nil
					}
					reads++
					if reads != 2 {
						return nil
					}
					live := readSettingsPool(t, store)
					switch scenario {
					case "spec changed":
						live.Spec.Labels["workload"] = "new-spec"
						live.Generation++
					case "deletion started":
						return store.Delete(ctx, live)
					case "finalizer removed":
						live.Finalizers = nil
					case "pool replaced":
						live.Finalizers = nil
						if err := store.Update(ctx, live); err != nil {
							return err
						}
						if err := store.Delete(ctx, live); err != nil {
							return err
						}
						replacement := settingsPool()
						replacement.UID = "replacement-pool-uid"
						replacement.ResourceVersion = ""
						return store.Create(ctx, replacement)
					}
					return store.Update(ctx, live)
				},
			}
			if _, err := settingsReconcile(r); err == nil {
				t.Fatal("expected fresh-pool fence to abort the write")
			}
			if len(writes.patches) != 0 || !reflect.DeepEqual(readSettingsNode(t, store, node.Name), before) {
				t.Fatal("Node changed after the pool ceased to authorize the planned write")
			}
		})
	}
}

func TestSettingsDeletionWithoutOurFinalizer(t *testing.T) {
	pool := settingsPool()
	pool.Finalizers = []string{"example.com/other"}
	node := ownedSettingsNode(t, settingsNode("worker", corev1.ConditionTrue), pool)
	r, store, writes := settingsFixture(t, pool, node)
	before := readSettingsNode(t, store, node.Name)
	if err := store.Delete(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	if len(writes.patches) != 0 || !reflect.DeepEqual(readSettingsNode(t, store, node.Name), before) {
		t.Fatal("must not acquire a finalizer or mutate Nodes after deletion without our finalizer")
	}
}

func TestSettingsDeletionUnreadableOwnership(t *testing.T) {
	for _, scenario := range []string{"Ready member", "NotReady member", "unrelated Ready Node"} {
		t.Run(scenario, func(t *testing.T) {
			pool := settingsPool()
			pool.Finalizers = append(pool.Finalizers, "example.com/other")
			node := settingsNode("worker", corev1.ConditionTrue)
			node.Annotations[nodeSettingsAnnotation] = "{"
			if scenario == "NotReady member" {
				node.Status.Conditions[0].Status = corev1.ConditionFalse
			}
			if scenario == "unrelated Ready Node" {
				node.Labels[membershipLabel] = "another-pool"
			}
			r, store, _ := settingsFixture(t, pool, node)
			before := readSettingsNode(t, store, node.Name)
			if err := store.Delete(context.Background(), pool); err != nil {
				t.Fatal(err)
			}
			_, err := settingsReconcile(r)
			blocked := scenario == "Ready member"
			if (err != nil) != blocked {
				t.Fatalf("unexpected error: %v", err)
			}
			if controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) != blocked {
				t.Fatal("incorrect finalizer retention for unreadable ownership")
			}
			if !reflect.DeepEqual(readSettingsNode(t, store, node.Name), before) {
				t.Fatal("unreadable ownership must not be changed")
			}
		})
	}
}

func TestSettingsNoExecuteIsNotSuppressedByIgnore(t *testing.T) {
	pool := settingsPool()
	pool.Spec.Taints[0].Effect = corev1.TaintEffectNoExecute
	node := settingsNode("worker", corev1.ConditionFalse)
	r, store, _ := settingsFixture(t, pool, node)
	if _, err := settingsReconcile(r); err != nil {
		t.Fatal(err)
	}
	got := readSettingsNode(t, store, node.Name)
	if len(got.Spec.Taints) != 2 || got.Spec.Taints[1].Effect != corev1.TaintEffectNoExecute {
		t.Fatal("Ignore is not a switch disabling declared taint synchronization")
	}
}

func TestSettingsReaderFailures(t *testing.T) {
	for _, scenario := range []string{"missing APIReader", "list failed"} {
		t.Run(scenario, func(t *testing.T) {
			pool := settingsPool()
			node := settingsNode("worker", corev1.ConditionTrue)
			r, store, writes := settingsFixture(t, pool, node)
			if scenario == "missing APIReader" {
				r.APIReader = nil
			} else {
				r.APIReader = &settingsReadClient{Reader: store, listError: errors.New("API unavailable")}
			}
			if _, err := settingsReconcile(r); err == nil || len(writes.patches) != 0 {
				t.Fatalf("expected reader failure without writes, got %v", err)
			}
			if !controllerutil.ContainsFinalizer(readSettingsPool(t, store), nodeSettingsFinalizer) {
				t.Fatal("reader failure removed finalizer")
			}
		})
	}
}
