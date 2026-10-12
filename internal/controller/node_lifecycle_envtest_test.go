package controller

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

type lifecycleDeleteRaceClient struct {
	client.Client
	beforeDelete func(context.Context, client.Object) error
}

func (c *lifecycleDeleteRaceClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if c.beforeDelete != nil {
		if err := c.beforeDelete(ctx, object); err != nil {
			return err
		}
	}
	return c.Client.Delete(ctx, object, options...)
}

var _ = Describe("NodePool lifecycle against an isolated API server", func() {
	var pool *nodesv1alpha1.NodePool
	var node *corev1.Node
	var reconciler *NodePoolReconciler
	var now time.Time
	testContext := context.Background()

	run := func() error {
		_, err := reconciler.Reconcile(testContext, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: pool.Name},
		})
		return err
	}
	readPool := func() *nodesv1alpha1.NodePool {
		current := &nodesv1alpha1.NodePool{}
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(pool), current)).To(Succeed())
		return current
	}
	readNode := func() *corev1.Node {
		current := &corev1.Node{}
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(node), current)).To(Succeed())
		return current
	}
	BeforeEach(func() {
		now = time.Now().UTC().Truncate(time.Second)
		pool = &nodesv1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "lifecycle-pool-"},
			Spec: nodesv1alpha1.NodePoolSpec{
				Image: "test",
				UnreadyPolicy: nodesv1alpha1.UnreadyPolicy{
					Action: "Remove", GracePeriod: "0s", DrainTimeout: "30s", MaxConcurrentRemovals: 1,
				},
			},
		}
		Expect(k8sClient.Create(testContext, pool)).To(Succeed())
		DeferCleanup(func() {
			current := &nodesv1alpha1.NodePool{}
			err := k8sClient.Get(testContext, client.ObjectKeyFromObject(pool), current)
			if apierrors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			controllerutil.RemoveFinalizer(current, nodeSettingsFinalizer)
			Expect(k8sClient.Update(testContext, current)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, current))).To(Succeed())
		})
		node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			GenerateName: "lifecycle-worker-", Labels: map[string]string{membershipLabel: pool.Name},
		}}
		Expect(k8sClient.Create(testContext, node)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, node))).To(Succeed())
		})
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		Expect(k8sClient.Status().Update(testContext, node)).To(Succeed())
		reconciler = &NodePoolReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return now },
		}
	})

	createPod := func() *corev1.Pod {
		namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "lifecycle-pods-"}}
		Expect(k8sClient.Create(testContext, namespace)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(testContext, namespace)).To(Succeed()) })
		serviceAccount := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: namespace.Name}}
		Expect(k8sClient.Create(testContext, serviceAccount)).To(Succeed())
		labels := map[string]string{"app": pool.Name}
		replicas := int32(1)
		replicaSet := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: namespace.Name},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "test", Image: "example.invalid/test:unused"}}},
				},
			},
		}
		Expect(k8sClient.Create(testContext, replicaSet)).To(Succeed())
		controller, grace := true, int64(30)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "job", Namespace: namespace.Name, Labels: labels,
				Finalizers: []string{"example.com/test-hold"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller,
				}},
			},
			Spec: corev1.PodSpec{
				NodeName: node.Name, TerminationGracePeriodSeconds: &grace,
				Containers: []corev1.Container{{Name: "test", Image: "example.invalid/test:unused"}},
			},
		}
		Expect(k8sClient.Create(testContext, pod)).To(Succeed())
		DeferCleanup(func() {
			current := &corev1.Pod{}
			err := k8sClient.Get(testContext, client.ObjectKeyFromObject(pod), current)
			if apierrors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			current.Finalizers = nil
			Expect(k8sClient.Update(testContext, current)).To(Succeed())
			// Fixture cleanup only: the controller never uses direct Pod deletion.
			Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, current, client.GracePeriodSeconds(0)))).To(Succeed())
		})
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		Expect(k8sClient.Status().Update(testContext, pod)).To(Succeed())
		return pod
	}

	It("persists Draining and Removing before deleting an empty Node", func() {
		Expect(run()).To(Succeed())
		Expect(readNode().Spec.Unschedulable).To(BeFalse())
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].Phase).To(Equal("Draining"))
		Expect(readPool().Status.Nodes[0].DrainStartedAt).NotTo(BeNil())
		Expect(readNode().Spec.Unschedulable).To(BeTrue())
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].Phase).To(Equal("Removing"))
		Expect(readNode().UID).To(Equal(node.UID))
		Expect(run()).To(Succeed())
		Expect(apierrors.IsNotFound(k8sClient.Get(testContext, client.ObjectKeyFromObject(node), &corev1.Node{}))).To(BeTrue())
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes).To(BeEmpty())
		Expect(readPool().Status.ObservedGeneration).To(Equal(pool.Generation))
	})

	It("respects a real PDB rejection and blocks on timeout without force deletion", func() {
		pod := createPod()
		minimum := intstr.FromInt32(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "protect", Namespace: pod.Namespace},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minimum, Selector: &metav1.LabelSelector{MatchLabels: pod.Labels},
			},
		}
		Expect(k8sClient.Create(testContext, pdb)).To(Succeed())
		pdb.Status = policyv1.PodDisruptionBudgetStatus{
			ObservedGeneration: pdb.Generation, CurrentHealthy: 1, DesiredHealthy: 1, ExpectedPods: 1, DisruptionsAllowed: 0,
		}
		Expect(k8sClient.Status().Update(testContext, pdb)).To(Succeed())
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].Reason).To(Equal("EvictionBlocked"))
		Expect(readPool().Status.Nodes[0].Blocked).To(BeTrue())
		now = now.Add(31 * time.Second)
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].Reason).To(Equal("DrainTimedOut"))
		current := &corev1.Pod{}
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(pod), current)).To(Succeed())
		Expect(current.UID).To(Equal(pod.UID))
		Expect(current.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(readNode().UID).To(Equal(node.UID))
	})

	It("honors Pod grace and finalizers when eviction succeeds but no kubelet completes termination", func() {
		pod := createPod()
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		current := &corev1.Pod{}
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(pod), current)).To(Succeed())
		Expect(current.DeletionTimestamp.IsZero()).To(BeFalse())
		Expect(current.DeletionGracePeriodSeconds).NotTo(BeNil())
		Expect(*current.DeletionGracePeriodSeconds).To(BeNumerically(">", 0))
		Expect(current.Finalizers).To(ContainElement("example.com/test-hold"))
		now = now.Add(31 * time.Second)
		Expect(run()).To(Succeed())
		Expect(readPool().Status.Nodes[0].Reason).To(Equal("DrainTimedOut"))
		Expect(readNode().UID).To(Equal(node.UID))
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(pod), current)).To(Succeed())
		Expect(current.Finalizers).To(ContainElement("example.com/test-hold"))
	})

	It("rejects a stale Node deletion and preserves the concurrent edit", func() {
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		writer := &lifecycleDeleteRaceClient{Client: k8sClient}
		writer.beforeDelete = func(ctx context.Context, object client.Object) error {
			current := readNode()
			current.Labels["human.example.com/edit"] = "preserve"
			return k8sClient.Update(ctx, current)
		}
		reconciler.Client = writer
		Expect(apierrors.IsConflict(run())).To(BeTrue())
		Expect(readNode().Labels).To(HaveKeyWithValue("human.example.com/edit", "preserve"))
		writer.beforeDelete = nil
		Expect(run()).To(Succeed())
		Expect(apierrors.IsNotFound(k8sClient.Get(testContext, client.ObjectKeyFromObject(node), &corev1.Node{}))).To(BeTrue())
	})

	It("does not delete a same-name replacement Node", func() {
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		Expect(run()).To(Succeed())
		writer := &lifecycleDeleteRaceClient{Client: k8sClient}
		writer.beforeDelete = func(ctx context.Context, object client.Object) error {
			Expect(k8sClient.Delete(ctx, readNode())).To(Succeed())
			replacement := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: node.Name, Labels: map[string]string{membershipLabel: pool.Name},
			}}
			return k8sClient.Create(ctx, replacement)
		}
		reconciler.Client = writer
		Expect(apierrors.IsConflict(run())).To(BeTrue())
		Expect(readNode().UID).NotTo(Equal(node.UID))
		Expect(readNode().Spec.Unschedulable).To(BeFalse())
	})

	It("rejects invalid policy values through the generated API schema", func() {
		for _, policy := range []nodesv1alpha1.UnreadyPolicy{
			{Action: "Remove", GracePeriod: "-1s", DrainTimeout: "30s", MaxConcurrentRemovals: 1},
			{Action: "Remove", GracePeriod: "0s", DrainTimeout: "0s", MaxConcurrentRemovals: 1},
			{Action: "Destroy", GracePeriod: "0s", DrainTimeout: "30s", MaxConcurrentRemovals: 1},
			{Action: "Remove", GracePeriod: "0s", DrainTimeout: "30s", MaxConcurrentRemovals: 0},
		} {
			invalid := &nodesv1alpha1.NodePool{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "invalid-lifecycle-"},
				Spec:       nodesv1alpha1.NodePoolSpec{Image: "test", UnreadyPolicy: policy},
			}
			Expect(apierrors.IsInvalid(k8sClient.Create(testContext, invalid))).To(BeTrue())
		}
	})
})
