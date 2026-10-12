package controller

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestPlanNodeSettings(t *testing.T) {
	uid := types.UID("pool-uid")
	labels := map[string]string{"workload": "batch"}
	updatedLabels := map[string]string{"workload": "interactive"}
	a := corev1.Taint{Key: "dedicated", Value: "batch", Effect: corev1.TaintEffectNoSchedule}
	b := a
	b.Value = "interactive"
	execute := a
	execute.Effect = corev1.TaintEffectNoExecute
	stamped := a
	stamp := metav1.NewTime(time.Unix(100, 0).UTC())
	stamped.TimeAdded = &stamp

	record := func(ls map[string]string, ts []corev1.Taint) string {
		t.Helper()
		data, err := json.Marshal(nodeSettingsOwnership{PoolUID: uid, Labels: ls, Taints: ts})
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	base := func() *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "worker-1", UID: "node-uid", ResourceVersion: "17",
				Labels:      map[string]string{membershipLabel: "pool", "unrelated": "keep"},
				Annotations: map[string]string{"unrelated.example.com/note": "keep"},
				Finalizers:  []string{"unrelated.example.com/finalizer"},
			},
			Spec: corev1.NodeSpec{
				ProviderID: "example://worker-1", Unschedulable: true,
				Taints: []corev1.Taint{{
					Key: "node.kubernetes.io/not-ready", Effect: corev1.TaintEffectNoSchedule,
				}},
			},
		}
	}
	add := func(n *corev1.Node) {
		n.Labels["workload"] = "batch"
		n.Spec.Taints = append(n.Spec.Taints, a)
	}
	ownedRecord := record(labels, []corev1.Taint{a})
	own := func(n *corev1.Node) {
		add(n)
		n.Annotations[nodeSettingsAnnotation] = ownedRecord
	}
	annotation := func(raw string) func(*corev1.Node) {
		return func(n *corev1.Node) { n.Annotations[nodeSettingsAnnotation] = raw }
	}

	cases := []struct {
		name       string
		setup      func(*corev1.Node)
		labels     map[string]string
		taints     []corev1.Taint
		wantLabels map[string]string
		wantTaints []corev1.Taint
		wantOwner  string
		bad        bool
	}{
		{name: "no changes"},
		{name: "apply", labels: labels, taints: []corev1.Taint{a},
			wantLabels: labels, wantTaints: []corev1.Taint{a}, wantOwner: ownedRecord},
		{name: "matching preexisting remains unowned", setup: add,
			labels: labels, taints: []corev1.Taint{a}, wantLabels: labels, wantTaints: []corev1.Taint{a}},
		{name: "cleanup owned", setup: own},
		{name: "remove owned taint only", setup: own, labels: labels,
			wantLabels: labels, wantOwner: record(labels, nil)},
		{name: "update owned", setup: own, labels: updatedLabels, taints: []corev1.Taint{b},
			wantLabels: updatedLabels, wantTaints: []corev1.Taint{b},
			wantOwner: record(updatedLabels, []corev1.Taint{b})},
		{name: "reapply missing owned fields", setup: annotation(ownedRecord),
			labels: labels, taints: []corev1.Taint{a},
			wantLabels: labels, wantTaints: []corev1.Taint{a}, wantOwner: ownedRecord},
		{name: "cleanup missing owned fields", setup: annotation(ownedRecord)},
		{name: "external changes survive cleanup", setup: func(n *corev1.Node) {
			own(n)
			n.Labels["workload"] = "interactive"
			n.Spec.Taints[1] = b
		}, wantLabels: updatedLabels, wantTaints: []corev1.Taint{b}},
		{name: "external label change conflicts", setup: func(n *corev1.Node) {
			own(n)
			n.Labels["workload"] = "interactive"
		}, labels: updatedLabels, bad: true},
		{name: "external taint change conflicts", setup: func(n *corev1.Node) {
			own(n)
			n.Spec.Taints[1] = b
		}, taints: []corev1.Taint{b}, bad: true},
		{name: "foreign label conflicts", setup: add, labels: updatedLabels, bad: true},
		{name: "foreign taint conflicts after label planning", setup: add,
			labels: map[string]string{"new": "value"}, taints: []corev1.Taint{b}, bad: true},
		{name: "effect distinguishes identity", taints: []corev1.Taint{a, execute},
			wantTaints: []corev1.Taint{execute, a}, wantOwner: record(nil, []corev1.Taint{execute, a})},
		{name: "preserve existing order", setup: own, taints: []corev1.Taint{execute, a},
			wantTaints: []corev1.Taint{a, execute}, wantOwner: record(nil, []corev1.Taint{execute, a})},
		{name: "unchanged record preserves bytes", setup: func(n *corev1.Node) {
			own(n)
			n.Annotations[nodeSettingsAnnotation] = " \n" + ownedRecord + "\n"
		}, labels: labels, taints: []corev1.Taint{a}, wantLabels: labels,
			wantTaints: []corev1.Taint{a}, wantOwner: " \n" + ownedRecord + "\n"},
		{name: "empty owner removed",
			setup: annotation(`{"poolUID":"pool-uid","labels":{},"taints":[]}`)},
		{name: "timestamped foreign match stays unowned", setup: func(n *corev1.Node) {
			n.Spec.Taints = append(n.Spec.Taints, stamped)
		}, taints: []corev1.Taint{a}, wantTaints: []corev1.Taint{stamped}},
		{name: "owned timestamp change survives cleanup", setup: func(n *corev1.Node) {
			own(n)
			n.Spec.Taints[1] = stamped
		}, wantTaints: []corev1.Taint{stamped}},
		{name: "owned timestamp change conflicts", setup: func(n *corev1.Node) {
			own(n)
			n.Spec.Taints[1] = stamped
		}, taints: []corev1.Taint{a}, bad: true},
		{name: "foreign UID", setup: annotation(`{"poolUID":"other"}`), bad: true},
		{name: "missing UID", setup: annotation(`{}`), bad: true},
		{name: "malformed JSON", setup: annotation(`{"poolUID":`), bad: true},
		{name: "null JSON", setup: annotation(`null`), bad: true},
		{name: "duplicate JSON key",
			setup: annotation(`{"poolUID":"pool-uid","poolUID":"pool-uid"}`), bad: true},
		{name: "duplicate nested JSON key",
			setup: annotation(`{"poolUID":"pool-uid","labels":{"x":"a","x":"b"}}`), bad: true},
		{name: "unknown JSON field",
			setup: annotation(`{"poolUID":"pool-uid","unexpected":true}`), bad: true},
		{name: "trailing JSON", setup: annotation(ownedRecord + `{}`), bad: true},
		{name: "reserved ownership",
			setup: annotation(record(map[string]string{membershipLabel: "pool"}, nil)), bad: true},
		{name: "reserved desired label", labels: map[string]string{membershipLabel: "pool"}, bad: true},
		{name: "invalid label key", labels: map[string]string{"bad key": "value"}, bad: true},
		{name: "invalid label value", labels: map[string]string{"valid": "bad value"}, bad: true},
		{name: "invalid taint key",
			taints: []corev1.Taint{{Key: "bad key", Effect: a.Effect}}, bad: true},
		{name: "invalid taint value",
			taints: []corev1.Taint{{Key: a.Key, Value: "bad value", Effect: a.Effect}}, bad: true},
		{name: "invalid taint effect", taints: []corev1.Taint{{Key: a.Key, Effect: "Invalid"}}, bad: true},
		{name: "duplicate desired taint", taints: []corev1.Taint{a, a}, bad: true},
		{name: "duplicate recorded taint", setup: annotation(record(nil, []corev1.Taint{a, a})), bad: true},
		{name: "duplicate existing taint", setup: func(n *corev1.Node) {
			n.Spec.Taints = append(n.Spec.Taints, a, a)
		}, bad: true},
		{name: "desired timestamp rejected", taints: []corev1.Taint{stamped}, bad: true},
		{name: "recorded timestamp rejected", setup: annotation(record(nil, []corev1.Taint{stamped})), bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := base()
			if tc.setup != nil {
				tc.setup(node)
			}
			before := node.DeepCopy()
			desired := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Labels: tc.labels},
				Spec:       corev1.NodeSpec{Taints: tc.taints},
			}
			desiredBefore := desired.DeepCopy()
			got, err := planNodeSettings(node, uid, tc.labels, tc.taints)
			if !reflect.DeepEqual(node, before) || !reflect.DeepEqual(desired, desiredBefore) {
				t.Fatal("planner modified an input")
			}
			if tc.bad {
				if err == nil || got != nil {
					t.Fatalf("expected nil result and error; got %v, %v", got, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := base()
			for key, value := range tc.wantLabels {
				want.Labels[key] = value
			}
			want.Spec.Taints = append(want.Spec.Taints, tc.wantTaints...)
			if tc.wantOwner != "" {
				want.Annotations[nodeSettingsAnnotation] = tc.wantOwner
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("unexpected node\n got: %#v\nwant: %#v", got, want)
			}
			again, err := planNodeSettings(got, uid, tc.labels, tc.taints)
			if err != nil || !reflect.DeepEqual(again, got) {
				t.Fatalf("not idempotent: %v", err)
			}
			if tc.wantOwner == "" {
				clean, err := planNodeSettings(got, uid, nil, nil)
				if err != nil || !reflect.DeepEqual(clean, got) {
					t.Fatalf("cleanup changed unowned settings: %v", err)
				}
			}
			got.Labels["unrelated"] = "changed"
			got.Annotations["unrelated.example.com/note"] = "changed"
			got.Spec.Taints[0].Value = "changed"
			got.Finalizers[0] = "changed"
			if !reflect.DeepEqual(node, before) {
				t.Fatal("result aliases input")
			}
		})
	}
	t.Run("nil node and empty UID", func(t *testing.T) {
		for _, node := range []*corev1.Node{nil, base()} {
			if got, err := planNodeSettings(node, "", nil, nil); err == nil || got != nil {
				t.Fatal("expected rejection")
			}
		}
	})
	t.Run("nil collections remain stable", func(t *testing.T) {
		node := &corev1.Node{}
		got, err := planNodeSettings(node, uid, nil, nil)
		if err != nil || got == node || !reflect.DeepEqual(got, node) {
			t.Fatalf("empty node changed or aliased: %v", err)
		}
	})
}
