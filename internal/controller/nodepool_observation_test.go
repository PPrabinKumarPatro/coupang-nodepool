package controller

import (
	"context"
	"reflect"
	"testing"
	"time"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestObserveNode(t *testing.T) {
	old := metav1.NewTime(time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC))
	now := metav1.NewTime(old.Add(time.Hour))

	tests := []struct {
		name        string
		ready       corev1.ConditionStatus
		uid         string
		previousUID string
		since       *metav1.Time
		wantPhase   string
		wantSince   *metav1.Time
	}{
		{
			name: "ready has no timer", ready: corev1.ConditionTrue,
			uid: "uid-1", wantPhase: "Ready",
		},
		{
			name: "false starts first timer", ready: corev1.ConditionFalse,
			uid: "uid-1", previousUID: "uid-1",
			wantPhase: "NotReady", wantSince: &now,
		},
		{
			name: "unknown is unready", ready: corev1.ConditionUnknown,
			uid: "uid-1", wantPhase: "NotReady", wantSince: &now,
		},
		{
			name: "missing Ready is unready",
			uid:  "uid-1", wantPhase: "NotReady", wantSince: &now,
		},
		{
			name: "same UID preserves timer", ready: corev1.ConditionFalse,
			uid: "uid-1", previousUID: "uid-1", since: &old,
			wantPhase: "NotReady", wantSince: &old,
		},
		{
			name: "recovered Ready clears timer", ready: corev1.ConditionTrue,
			uid: "uid-1", previousUID: "uid-1", since: &old,
			wantPhase: "Ready",
		},
		{
			name: "same name new UID resets timer", ready: corev1.ConditionFalse,
			uid: "uid-2", previousUID: "uid-1", since: &old,
			wantPhase: "NotReady", wantSince: &now,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "worker-1",
					UID:  types.UID(tt.uid),
				},
			}
			// An unrelated condition must not count as NodeReady.
			node.Status.Conditions = []corev1.NodeCondition{
				{Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse},
			}
			if tt.ready != "" {
				node.Status.Conditions = append(node.Status.Conditions,
					corev1.NodeCondition{Type: corev1.NodeReady, Status: tt.ready})
			}
			previous := nodesv1alpha1.NodePoolNodeStatus{
				Name: "worker-1", UID: tt.previousUID,
				Phase: "NotReady", NotReadySince: tt.since,
			}
			nodeBefore := node.DeepCopy()
			previousBefore := previous
			if previous.NotReadySince != nil {
				previousBefore.NotReadySince = previous.NotReadySince.DeepCopy()
			}

			got := observeNode(node, previous, now)
			want := nodesv1alpha1.NodePoolNodeStatus{
				Name: "worker-1", UID: tt.uid,
				Phase: tt.wantPhase, NotReadySince: tt.wantSince,
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("observeNode() = %+v; want %+v", got, want)
			}
			if !reflect.DeepEqual(node, nodeBefore) ||
				!reflect.DeepEqual(previous, previousBefore) {
				t.Error("observeNode mutated caller state")
			}
			if got.NotReadySince != nil &&
				got.NotReadySince == previous.NotReadySince {
				t.Error("returned timestamp shares the previous timestamp pointer")
			}
		})
	}
}

func TestMemberPoolRequests(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{name: "unlabeled"},
		{name: "unrelated label", labels: map[string]string{"other": "pool-a"}},
		{name: "empty membership", labels: map[string]string{membershipLabel: ""}},
		{name: "member", labels: map[string]string{membershipLabel: "pool-a"}, want: "pool-a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: "worker-1", Labels: tt.labels,
			}}
			before := node.DeepCopy()
			got := memberPoolRequests(context.Background(), node)
			if !reflect.DeepEqual(node, before) {
				t.Error("memberPoolRequests mutated caller state")
			}
			if tt.want == "" {
				if got != nil {
					t.Errorf("requests = %#v; want nil", got)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("got %d requests; want 1", len(got))
			}
			want := types.NamespacedName{Name: tt.want}
			if got[0].NamespacedName != want {
				t.Errorf("target = %#v; want %#v", got[0].NamespacedName, want)
			}
		})
	}
}
