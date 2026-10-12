package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

var _ = Describe("Scenario fixes against the real isolated API", func() {
	var pool *nodesv1alpha1.NodePool
	var node *corev1.Node
	var now time.Time
	var r *NodePoolReconciler
	ctx := context.Background()
	readPool := func() *nodesv1alpha1.NodePool {
		current := &nodesv1alpha1.NodePool{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), current)).To(Succeed())
		return current
	}
	readNode := func() *corev1.Node {
		current := &corev1.Node{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(node), current)).To(Succeed())
		return current
	}
	run := func() error {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
		return err
	}
	BeforeEach(func() {
		now = time.Now().UTC().Truncate(time.Second)
		pool = &nodesv1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "scenario-pool-", Finalizers: []string{nodeSettingsFinalizer}},
			Spec:       nodesv1alpha1.NodePoolSpec{Image: "test", UnreadyPolicy: nodesv1alpha1.UnreadyPolicy{Action: "Cordon", GracePeriod: "30s", MaxConcurrentRemovals: 1}},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		DeferCleanup(func() {
			current := &nodesv1alpha1.NodePool{}
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pool), current)
			if apierrors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			controllerutil.RemoveFinalizer(current, nodeSettingsFinalizer)
			controllerutil.RemoveFinalizer(current, "example.com/scenario-test-hold")
			Expect(k8sClient.Update(ctx, current)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		})
		node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{GenerateName: "scenario-node-", Labels: map[string]string{membershipLabel: pool.Name}}}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, node))).To(Succeed()) })
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-time.Minute))}}
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		r = &NodePoolReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return now }}
	})

	It("persists transition metadata and enforces fresh grace after a missed recovery", func() {
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].ReadyTransitionTime.Equal(&node.Status.Conditions[0].LastTransitionTime)).To(BeTrue())
		now = now.Add(31 * time.Second)
		node = readNode()
		node.Status.Conditions[0].LastTransitionTime = metav1.NewTime(now)
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		r = &NodePoolReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return now }}
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].NotReadySince.Time.Equal(now)).To(BeTrue())
		Expect(readNode().Spec.Unschedulable).To(BeFalse())
		now = now.Add(29 * time.Second)
		Expect(run()).To(Succeed())
		Expect(readNode().Spec.Unschedulable).To(BeFalse())
		now = now.Add(time.Second)
		Expect(run()).To(Succeed())
		Expect(readNode().Spec.Unschedulable).To(BeTrue())
	})

	It("persists former-member cleanup through errors and deletion, then clears the repaired failure", func() {
		Expect(run()).To(Succeed())
		node = readNode()
		node.Labels[membershipLabel] = "other"
		node.Annotations = map[string]string{nodeSettingsAnnotation: "{"}
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		for i := 0; i < 3; i++ {
			Expect(run()).NotTo(Succeed())
			current := readPool()
			Expect(current.Status.Nodes).To(BeEmpty())
			Expect(current.Status.PendingSettingsCleanup).To(Equal([]nodesv1alpha1.NodePoolNodeReference{{Name: node.Name, UID: string(node.UID)}}))
		}
		node = readNode()
		node.Status.Conditions[0].Status = corev1.ConditionTrue
		Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
		current := readPool()
		controllerutil.AddFinalizer(current, "example.com/scenario-test-hold")
		Expect(k8sClient.Update(ctx, current)).To(Succeed())
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		Expect(run()).NotTo(Succeed())
		Expect(controllerutil.ContainsFinalizer(readPool(), nodeSettingsFinalizer)).To(BeTrue())
		node = readNode()
		// Explicit operator repair: this fixture never had any managed settings.
		delete(node.Annotations, nodeSettingsAnnotation)
		Expect(k8sClient.Update(ctx, node)).To(Succeed())
		Expect(run()).To(Succeed())
		current = readPool()
		Expect(current.Status.PendingSettingsCleanup).To(BeEmpty())
		Expect(meta.IsStatusConditionFalse(current.Status.Conditions, "Failed")).To(BeTrue())
		Expect(meta.IsStatusConditionFalse(current.Status.Conditions, "Progressing")).To(BeTrue())
		Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
		Expect(controllerutil.ContainsFinalizer(current, nodeSettingsFinalizer)).To(BeFalse())
		Expect(controllerutil.ContainsFinalizer(current, "example.com/scenario-test-hold")).To(BeTrue())
	})
})
