package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

type realEvictionRaceClient struct {
	client.Client
	before func(context.Context, *corev1.Pod) error
}

func (c *realEvictionRaceClient) SubResource(name string) client.SubResourceClient {
	return &realEvictionRaceSubresource{SubResourceClient: c.Client.SubResource(name), owner: c, name: name}
}

type realEvictionRaceSubresource struct {
	client.SubResourceClient
	owner *realEvictionRaceClient
	name  string
}

func (c *realEvictionRaceSubresource) Create(ctx context.Context, object, subresource client.Object, options ...client.SubResourceCreateOption) error {
	if c.name == "eviction" && c.owner.before != nil {
		if err := c.owner.before(ctx, object.(*corev1.Pod)); err != nil {
			return err
		}
	}
	return c.SubResourceClient.Create(ctx, object, subresource, options...)
}

type reservationRaceClient struct {
	client.Client
	arrivals atomic.Int32
	gate     chan struct{}
}

func (c *reservationRaceClient) Status() client.SubResourceWriter {
	return &reservationRaceStatus{SubResourceWriter: c.Client.Status(), owner: c}
}

type reservationRaceStatus struct {
	client.SubResourceWriter
	owner *reservationRaceClient
}

func (w *reservationRaceStatus) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.SubResourcePatchOption) error {
	arrival := w.owner.arrivals.Add(1)
	if arrival <= 2 {
		if arrival == 2 {
			close(w.owner.gate)
		}
		select {
		case <-w.owner.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return w.SubResourceWriter.Patch(ctx, object, patch, options...)
}

var _ = Describe("Approved design boundaries against the isolated API", func() {
	var pool *nodesv1alpha1.NodePool
	var node *corev1.Node
	var reconciler *NodePoolReconciler
	var now time.Time
	testContext := context.Background()
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
	run := func() error {
		_, err := reconciler.Reconcile(testContext, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
		return err
	}
	BeforeEach(func() {
		now = time.Now().UTC().Truncate(time.Second)
		pool = &nodesv1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "design-pool-", Finalizers: []string{nodeSettingsFinalizer}},
			Spec: nodesv1alpha1.NodePoolSpec{Image: "test", UnreadyPolicy: nodesv1alpha1.UnreadyPolicy{
				Action: "Remove", GracePeriod: "0s", DrainTimeout: "1m", MaxConcurrentRemovals: 1,
			}},
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
		node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{GenerateName: "design-worker-", Labels: map[string]string{membershipLabel: pool.Name}}}
		Expect(k8sClient.Create(testContext, node)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, node))).To(Succeed()) })
		node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		Expect(k8sClient.Status().Update(testContext, node)).To(Succeed())
		reconciler = &NodePoolReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return now }}
	})

	createPod := func() *corev1.Pod {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "design-pods-"}}
		Expect(k8sClient.Create(testContext, ns)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, ns))).To(Succeed()) })
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: ns.Name}}
		Expect(k8sClient.Create(testContext, sa)).To(Succeed())
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: ns.Name},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: ptr.To(int32(1)), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "design"}},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "design"}},
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "test", Image: "example.invalid/test:unused"}}}},
			},
		}
		Expect(k8sClient.Create(testContext, rs)).To(Succeed())
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: ns.Name, Labels: map[string]string{"app": "design"},
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(rs, appsv1.SchemeGroupVersion.WithKind("ReplicaSet"))}},
			Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Name: "test", Image: "example.invalid/test:unused"}}},
		}
		Expect(k8sClient.Create(testContext, pod)).To(Succeed())
		DeferCleanup(func() {
			current := &corev1.Pod{}
			err := k8sClient.Get(testContext, client.ObjectKeyFromObject(pod), current)
			if !apierrors.IsNotFound(err) {
				Expect(err).NotTo(HaveOccurred())
				Expect(k8sClient.Delete(testContext, current, client.GracePeriodSeconds(0))).To(Succeed())
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, rs))).To(Succeed())
		})
		return pod
	}

	DescribeTable("rejects actual stale eviction preconditions", func(replace bool) {
		pod := createPod()
		Expect(run()).To(Succeed())
		var replacementUID types.UID
		reconciler.Client = &realEvictionRaceClient{Client: k8sClient, before: func(ctx context.Context, observed *corev1.Pod) error {
			current := &corev1.Pod{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(observed), current); err != nil {
				return err
			}
			if replace {
				if err := k8sClient.Delete(ctx, current, client.GracePeriodSeconds(0)); err != nil {
					return err
				}
				current = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: current.Name, Namespace: current.Namespace,
					Labels: current.Labels, OwnerReferences: current.OwnerReferences}, Spec: *current.Spec.DeepCopy()}
				if err := k8sClient.Create(ctx, current); err != nil {
					return err
				}
			} else {
				current.Annotations = map[string]string{"example.com/concurrent": "change"}
				if err := k8sClient.Update(ctx, current); err != nil {
					return err
				}
			}
			replacementUID = current.UID
			return nil
		}}
		err := run()
		Expect(apierrors.IsConflict(err)).To(BeTrue(), "real eviction endpoint must reject stale preconditions: %v", err)
		current := &corev1.Pod{}
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(pod), current)).To(Succeed())
		Expect(current.UID).To(Equal(replacementUID))
		Expect(current.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(current.UID == pod.UID).To(Equal(!replace))
		Expect(readNode().UID).To(Equal(node.UID))
		Expect(readPool().Status.Nodes[0].Phase).To(Equal("Draining"))
		Expect(readPool().Status.Nodes[0].Blocked).To(BeTrue())
	}, Entry("after same-name replacement", true), Entry("after a same-UID version update", false))

	It("rejects a settings patch when a role appears after its fresh read", func() {
		pool.Spec.Labels = map[string]string{"workload": "batch"}
		pool.Spec.UnreadyPolicy.Action = "Ignore"
		Expect(k8sClient.Update(testContext, pool)).To(Succeed())
		intercepted := false
		reconciler.Client = &settingsPatchClient{Client: k8sClient, beforePatch: func(ctx context.Context, object client.Object) error {
			if _, ok := object.(*corev1.Node); !ok || intercepted {
				return nil
			}
			intercepted = true
			current := readNode()
			current.Labels[controlPlaneRoleLabel] = ""
			return k8sClient.Update(ctx, current)
		}}
		err := run()
		Expect(intercepted).To(BeTrue())
		Expect(apierrors.IsConflict(err)).To(BeTrue())
		Expect(readNode().Labels).NotTo(HaveKey("workload"))
		Expect(readNode().Labels).To(HaveKey(controlPlaneRoleLabel))
		Expect(readPool().Status.Nodes[0].Reason).To(Equal("ProtectedNode"))
		Expect(run()).NotTo(Succeed())
		Expect(readNode().Annotations).NotTo(HaveKey(nodeSettingsAnnotation))
	})

	It("allows only one competing status reservation before disruption", func() {
		other := &corev1.Node{ObjectMeta: metav1.ObjectMeta{GenerateName: "design-other-", Labels: map[string]string{membershipLabel: pool.Name}}}
		Expect(k8sClient.Create(testContext, other)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, other))).To(Succeed()) })
		other.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		Expect(k8sClient.Status().Update(testContext, other)).To(Succeed())
		writer := &reservationRaceClient{Client: k8sClient, gate: make(chan struct{})}
		raceContext, stop := context.WithTimeout(testContext, 15*time.Second)
		defer stop()
		results := make(chan error, 2)
		var group sync.WaitGroup
		for i := 0; i < 2; i++ {
			group.Add(1)
			go func() {
				defer group.Done()
				r := &NodePoolReconciler{Client: writer, APIReader: k8sClient, Scheme: k8sClient.Scheme(), Now: func() time.Time { return now }}
				_, err := r.Reconcile(raceContext, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pool)})
				results <- err
			}()
		}
		group.Wait()
		close(results)
		success, conflicts := 0, 0
		for err := range results {
			if err == nil {
				success++
			} else if apierrors.IsConflict(err) {
				conflicts++
			} else {
				Fail("unexpected concurrent reconciliation error: " + err.Error())
			}
		}
		Expect(success).To(Equal(1))
		Expect(conflicts).To(Equal(1))
		active := 0
		for _, state := range readPool().Status.Nodes {
			if isRemovalPhase(state.Phase) {
				active++
			}
		}
		Expect(active).To(Equal(1))
		currentOther := &corev1.Node{}
		Expect(k8sClient.Get(testContext, client.ObjectKeyFromObject(other), currentOther)).To(Succeed())
		Expect(readNode().Spec.Unschedulable != currentOther.Spec.Unschedulable).To(BeTrue())
	})

	It("observes Node and Pod events before the polling fallback", func() {
		pool.Spec.UnreadyPolicy.Action = "Ignore"
		Expect(k8sClient.Update(testContext, pool)).To(Succeed())
		manager, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme: k8sClient.Scheme(), Metrics: metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0", LeaderElection: false,
		})
		Expect(err).NotTo(HaveOccurred())
		var reconciliations atomic.Uint64
		var lastReconcile atomic.Int64
		watched := &NodePoolReconciler{Client: manager.GetClient(), APIReader: manager.GetAPIReader(), Scheme: manager.GetScheme(),
			Now: func() time.Time {
				lastReconcile.Store(time.Now().UnixNano())
				reconciliations.Add(1)
				return time.Now().UTC()
			}}
		Expect(watched.SetupWithManager(manager)).To(Succeed())
		managerContext, stop := context.WithCancel(testContext)
		finished := make(chan error, 1)
		go func() { finished <- manager.Start(managerContext) }()
		DeferCleanup(func() {
			stop()
			Eventually(finished, 10*time.Second).Should(Receive(BeNil()))
		})
		Expect(manager.GetCache().WaitForCacheSync(managerContext)).To(BeTrue())
		Eventually(func() int { return len(readPool().Status.Nodes) }, 5*time.Second).Should(Equal(1))
		quiet := func() uint64 {
			Eventually(func() bool {
				// Two seconds of quiescence leave the ten-second requeue outside the assertion window.
				return reconciliations.Load() > 0 && time.Since(time.Unix(0, lastReconcile.Load())) >= 2*time.Second
			}, 5*time.Second, 25*time.Millisecond).Should(BeTrue())
			return reconciliations.Load()
		}
		quiet()
		current := readNode()
		current.Status.Conditions[0].Status = corev1.ConditionTrue
		Expect(k8sClient.Status().Update(testContext, current)).To(Succeed())
		Eventually(func() int32 { return readPool().Status.ReadyNodes }, 3*time.Second, 25*time.Millisecond).Should(Equal(int32(1)))
		before := quiet()
		pod := createPod()
		Eventually(reconciliations.Load, 3*time.Second, 25*time.Millisecond).Should(BeNumerically(">", before))
		before = quiet()
		Expect(k8sClient.Delete(testContext, pod, client.GracePeriodSeconds(0))).To(Succeed())
		Eventually(reconciliations.Load, 3*time.Second, 25*time.Millisecond).Should(BeNumerically(">", before))
		quiet()
		current = readNode()
		current.Labels[membershipLabel] = "missing-other-pool"
		Expect(k8sClient.Update(testContext, current)).To(Succeed())
		Eventually(func() int { return len(readPool().Status.Nodes) }, 3*time.Second, 25*time.Millisecond).Should(BeZero())
	})
})
