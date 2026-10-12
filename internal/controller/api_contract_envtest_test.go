package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

var _ = Describe("Generated NodePool API contract", func() {
	It("rejects omitted required fields, invalid types and duplicate taint identities", func() {
		base := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "nodes.example.com/v1alpha1", "kind": "NodePool",
			"metadata": map[string]any{"generateName": "contract-invalid-"},
			"spec": map[string]any{
				"image": "informational",
				"unreadyPolicy": map[string]any{
					"action": "Ignore", "gracePeriod": "30s", "maxConcurrentRemovals": int64(1),
				},
			},
		}}
		for _, path := range [][]string{
			{"spec"}, {"spec", "image"}, {"spec", "unreadyPolicy"},
			{"spec", "unreadyPolicy", "action"}, {"spec", "unreadyPolicy", "gracePeriod"},
			{"spec", "unreadyPolicy", "maxConcurrentRemovals"},
		} {
			invalid := base.DeepCopy()
			unstructured.RemoveNestedField(invalid.Object, path...)
			Expect(apierrors.IsInvalid(k8sClient.Create(context.Background(), invalid))).To(BeTrue(), "%v must be required", path)
		}
		for _, value := range []struct {
			path  []string
			value any
		}{
			{[]string{"spec", "image"}, ""},
			{[]string{"spec", "unreadyPolicy", "maxConcurrentRemovals"}, int64(-1)},
			{[]string{"spec", "unreadyPolicy", "gracePeriod"}, "1.5s"},
			{[]string{"spec", "unreadyPolicy", "gracePeriod"}, "1m30s"},
			{[]string{"spec", "unreadyPolicy", "gracePeriod"}, "1000000s"},
			{[]string{"spec", "unreadyPolicy", "drainTimeout"}, "-1s"},
			{[]string{"spec", "labels"}, map[string]any{"workload": int64(7)}},
			{[]string{"spec", "taints"}, []any{
				map[string]any{"key": "dedicated", "effect": "NoSchedule", "value": "first"},
				map[string]any{"key": "dedicated", "effect": "NoSchedule", "value": "second"},
			}},
		} {
			invalid := base.DeepCopy()
			Expect(unstructured.SetNestedField(invalid.Object, value.value, value.path...)).To(Succeed())
			Expect(apierrors.IsInvalid(k8sClient.Create(context.Background(), invalid))).To(BeTrue(), "%v must be rejected", value.path)
		}
	})

	It("defaults drain timeout and isolates status from spec updates", func() {
		ctx := context.Background()
		pool := &nodesv1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "contract-valid-"},
			Spec: nodesv1alpha1.NodePoolSpec{
				Image:         "informational",
				UnreadyPolicy: nodesv1alpha1.UnreadyPolicy{Action: "Ignore", GracePeriod: "0s", MaxConcurrentRemovals: 1},
			},
			Status: nodesv1alpha1.NodePoolStatus{ReadyNodes: 7},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pool))).To(Succeed()) })
		Expect(pool.Namespace).To(BeEmpty())
		Expect(pool.Spec.UnreadyPolicy.DrainTimeout).To(Equal("5m"))
		Expect(pool.Status.ReadyNodes).To(BeZero(), "status on normal create must be ignored")
		generation := pool.Generation
		pool.Status.ReadyNodes = 2
		Expect(k8sClient.Status().Update(ctx, pool)).To(Succeed())
		Expect(pool.Generation).To(Equal(generation), "status does not change spec generation")
		pool.Spec.Image = "new-informational-value"
		pool.Status.ReadyNodes = 9
		Expect(k8sClient.Update(ctx, pool)).To(Succeed())
		Expect(pool.Status.ReadyNodes).To(Equal(int32(2)), "normal update cannot overwrite status")
		Expect(pool.Generation).To(Equal(generation + 1))
	})
})
