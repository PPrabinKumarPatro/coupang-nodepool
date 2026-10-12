//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	authorizationv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

var _ = Describe("NodePool assignment flow", func() {
	It("proves real watches, RBAC, original-Pod retention, restart recovery and live-kubelet eviction", func() {
		ctx := context.Background()
		workerStopped, workerRemoved := false, false
		DeferCleanup(func() {
			if workerStopped && !workerRemoved {
				cleanupCtx, cancel := context.WithTimeout(ctx, time.Minute)
				defer cancel()
				output, err := exec.CommandContext(cleanupCtx, "docker", "exec", worker, "systemctl", "start", "kubelet").CombinedOutput()
				Expect(err).NotTo(HaveOccurred(), string(output))
			}
		})
		readNode := func(name string) *corev1.Node {
			node := &corev1.Node{}
			Expect(api.Get(ctx, client.ObjectKey{Name: name}, node)).To(Succeed())
			return node
		}
		pool := &nodesv1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-pool"},
			Spec: nodesv1alpha1.NodePoolSpec{
				Image:  "informational-only",
				Labels: map[string]string{"workload": "e2e"},
				Taints: []corev1.Taint{{Key: "dedicated", Value: "e2e", Effect: corev1.TaintEffectNoSchedule}},
				UnreadyPolicy: nodesv1alpha1.UnreadyPolicy{
					Action: "Remove", GracePeriod: "0s", DrainTimeout: "45s", MaxConcurrentRemovals: 1,
				},
			},
		}
		readPool := func() *nodesv1alpha1.NodePool {
			current := &nodesv1alpha1.NodePool{}
			Expect(api.Get(ctx, client.ObjectKeyFromObject(pool), current)).To(Succeed())
			return current
		}
		stateForWorker := func() nodesv1alpha1.NodePoolNodeStatus {
			for _, state := range readPool().Status.Nodes {
				if state.Name == worker {
					return state
				}
			}
			return nodesv1alpha1.NodePoolNodeStatus{}
		}
		By("1. Verifying the deployed service account can evict, but cannot directly delete or patch Pods")
		for _, operation := range []struct {
			verb, resource, subresource string
			allowed                     bool
		}{
			{"create", "pods", "eviction", true},
			{"delete", "pods", "", false},
			{"patch", "pods", "", false},
			{"delete", "nodes", "", true},
		} {
			review := &authorizationv1.SubjectAccessReview{Spec: authorizationv1.SubjectAccessReviewSpec{
				User: "system:serviceaccount:" + managerNamespace + ":" + managerName,
				ResourceAttributes: &authorizationv1.ResourceAttributes{
					Verb: operation.verb, Resource: operation.resource, Subresource: operation.subresource,
				},
			}}
			Expect(api.Create(ctx, review)).To(Succeed())
			Expect(review.Status.Allowed).To(Equal(operation.allowed))
		}

		By("2. Creating a NodePool and simulating infrastructure join with kubectl label")
		Expect(api.Create(ctx, pool)).To(Succeed())
		for _, name := range []string{worker, qaCluster + "-worker2"} {
			kubectl("label", "node", name, "nodes.example.com/nodepool="+pool.Name, "external.example.com/keep=yes")
		}
		Eventually(func(g Gomega) {
			current := &nodesv1alpha1.NodePool{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(pool), current)).To(Succeed())
			g.Expect(current.Status.ReadyNodes).To(Equal(int32(2)))
			g.Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
			for _, name := range []string{worker, qaCluster + "-worker2"} {
				node := &corev1.Node{}
				g.Expect(api.Get(ctx, client.ObjectKey{Name: name}, node)).To(Succeed())
				g.Expect(node.Labels).To(HaveKeyWithValue("workload", "e2e"))
				g.Expect(node.Annotations).To(HaveKey("nodes.example.com/managed-settings"))
				g.Expect(node.Spec.Taints).To(ContainElement(pool.Spec.Taints[0]))
			}
		}).Should(Succeed())

		By("3. Changing labels and taints while preserving unrelated metadata")
		pool = readPool()
		pool.Spec.Labels = map[string]string{"tier": "premium"}
		pool.Spec.Taints[0].Value = "updated"
		Expect(api.Update(ctx, pool)).To(Succeed())
		Eventually(func(g Gomega) {
			for _, name := range []string{worker, qaCluster + "-worker2"} {
				node := &corev1.Node{}
				g.Expect(api.Get(ctx, client.ObjectKey{Name: name}, node)).To(Succeed())
				g.Expect(node.Labels).NotTo(HaveKey("workload"))
				g.Expect(node.Labels).To(HaveKeyWithValue("tier", "premium"))
				g.Expect(node.Labels).To(HaveKeyWithValue("external.example.com/keep", "yes"))
				g.Expect(node.Spec.Taints).To(ContainElement(pool.Spec.Taints[0]))
			}
		}).Should(Succeed())

		By("4. Running two cached-image Pods, one per worker, protected by a PDB")
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "nodepool-e2e-workloads"}}
		Expect(api.Create(ctx, ns)).To(Succeed())
		labels := map[string]string{"app": "nodepool-e2e"}
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: ns.Name},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec: corev1.PodSpec{
						NodeSelector: map[string]string{"nodes.example.com/e2e-worker": "true"},
						SecurityContext: &corev1.PodSecurityContext{
							RunAsNonRoot: ptr.To(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
						},
						Tolerations: []corev1.Toleration{
							{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
							{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(600))},
							{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To(int64(600))},
						},
						Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
								LabelSelector: &metav1.LabelSelector{MatchLabels: labels}, TopologyKey: "kubernetes.io/hostname",
							}},
						}},
						Containers: []corev1.Container{{
							Name: "pause", Image: pauseImage, ImagePullPolicy: corev1.PullNever,
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: ptr.To(false), ReadOnlyRootFilesystem: ptr.To(true),
								Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							},
						}},
					},
				},
			},
		}
		Expect(api.Create(ctx, deployment)).To(Succeed())
		minimum := intstr.FromInt32(2)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Name: "protect", Namespace: ns.Name},
			Spec:       policyv1.PodDisruptionBudgetSpec{MinAvailable: &minimum, Selector: &metav1.LabelSelector{MatchLabels: labels}},
		}
		Expect(api.Create(ctx, pdb)).To(Succeed())
		Eventually(func(g Gomega) {
			current := &appsv1.Deployment{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(deployment), current)).To(Succeed())
			g.Expect(current.Status.ReadyReplicas).To(Equal(int32(2)))
			currentPDB := &policyv1.PodDisruptionBudget{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(pdb), currentPDB)).To(Succeed())
			g.Expect(currentPDB.Status.CurrentHealthy).To(Equal(int32(2)))
			g.Expect(currentPDB.Status.DisruptionsAllowed).To(BeZero())
		}).Should(Succeed())

		By("Recording original Pod identities before injecting any failure")
		var baseline corev1.PodList
		Expect(api.List(ctx, &baseline, client.InNamespace(ns.Name), client.MatchingLabels(labels))).To(Succeed())
		Expect(baseline.Items).To(HaveLen(2))
		originals := make(map[types.NamespacedName]corev1.Pod, len(baseline.Items))
		originalNodes := make(map[string]bool)
		var victim corev1.Pod
		for _, pod := range baseline.Items {
			Expect(pod.UID).NotTo(BeEmpty())
			Expect(pod.Spec.NodeName).To(BeElementOf(worker, qaCluster+"-worker2"))
			Expect(originalNodes[pod.Spec.NodeName]).To(BeFalse(), "one original Pod per worker")
			originalNodes[pod.Spec.NodeName] = true
			Expect(pod.Status.Phase).To(Equal(corev1.PodRunning))
			Expect(pod.Status.ContainerStatuses).To(HaveLen(1))
			Expect(pod.Status.ContainerStatuses[0].Ready).To(BeTrue())
			Expect(pod.Status.ContainerStatuses[0].State.Running).NotTo(BeNil())
			owner := metav1.GetControllerOf(&pod)
			Expect(owner).NotTo(BeNil())
			Expect(owner.Kind).To(Equal("ReplicaSet"))
			replicaSet := &appsv1.ReplicaSet{}
			Expect(api.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: owner.Name}, replicaSet)).To(Succeed())
			Expect(replicaSet.UID).To(Equal(owner.UID))
			deploymentOwner := metav1.GetControllerOf(replicaSet)
			Expect(deploymentOwner).NotTo(BeNil())
			Expect(deploymentOwner.UID).To(Equal(deployment.UID))
			originals[client.ObjectKeyFromObject(&pod)] = *pod.DeepCopy()
			By(fmt.Sprintf("Original Pod %s/%s UID=%s node=%s", pod.Namespace, pod.Name, pod.UID, pod.Spec.NodeName))
			if pod.Spec.NodeName == worker {
				victim = *pod.DeepCopy()
			}
		}
		Expect(victim.UID).NotTo(BeEmpty())
		assertOriginalPods := func() {
			var current corev1.PodList
			Expect(api.List(ctx, &current, client.InNamespace(ns.Name), client.MatchingLabels(labels))).To(Succeed())
			Expect(current.Items).To(HaveLen(len(originals)))
			for key, original := range originals {
				pod := &corev1.Pod{}
				Expect(api.Get(ctx, key, pod)).To(Succeed(), "original Pod %s", key)
				Expect(pod.UID).To(Equal(original.UID), "replacement Pods are not survival evidence")
				Expect(pod.Spec.NodeName).To(Equal(original.Spec.NodeName))
				Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
			}
		}
		assertOriginalPods()

		By("5. Stopping only the verified disposable worker kubelet; checking PDB blocking")
		assertWorker()
		workerStopped = true
		command("docker", "exec", worker, "systemctl", "stop", "kubelet")
		Eventually(func() string { return stateForWorker().Reason }).Should(Equal("EvictionBlocked"))
		before := stateForWorker()
		Expect(before.DrainStartedAt).NotTo(BeNil())
		Expect(before.NotReadySince).NotTo(BeNil())
		By("Persisted clocks: NotReadySince=" + before.NotReadySince.UTC().Format(time.RFC3339) +
			"; DrainStartedAt=" + before.DrainStartedAt.UTC().Format(time.RFC3339))
		assertOriginalPods()

		By("6. Restarting the real manager and verifying persisted timers survive")
		kubectl("rollout", "restart", "deployment/"+managerName, "-n", managerNamespace)
		kubectl("rollout", "status", "deployment/"+managerName, "-n", managerNamespace, "--timeout=120s")
		Eventually(func() string { return stateForWorker().Reason }, 2*time.Minute).Should(Equal("DrainTimedOut"))
		after := stateForWorker()
		Expect(after.DrainStartedAt.Equal(before.DrainStartedAt)).To(BeTrue())
		Expect(after.NotReadySince.Equal(before.NotReadySince)).To(BeTrue())
		Expect(readNode(worker).UID).To(Equal(types.UID(before.UID)))
		Expect(readNode(qaCluster + "-worker2").Spec.Unschedulable).To(BeFalse())
		assertOriginalPods()

		By("7. Restoring the worker; recovery cancels removal and releases our cordon")
		command("docker", "exec", worker, "systemctl", "start", "kubelet")
		workerStopped = false
		Eventually(func() string { return stateForWorker().Phase }).Should(Equal("Ready"))
		Eventually(func() bool { return readNode(worker).Spec.Unschedulable }).Should(BeFalse())
		Expect(stateForWorker().DrainStartedAt).To(BeNil())

		By("8. Preparing an allowing-PDB drain of the original workload with a healthy kubelet")
		assertOriginalPods()
		Eventually(func(g Gomega) {
			node := readNode(worker)
			for _, taint := range node.Spec.Taints {
				g.Expect(taint.Key).NotTo(BeElementOf("node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable"))
			}
			current := &appsv1.Deployment{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(deployment), current)).To(Succeed())
			g.Expect(current.Status.ReadyReplicas).To(Equal(int32(2)))
		}).Should(Succeed())
		pool = readPool()
		pool.Spec.UnreadyPolicy.DrainTimeout = "240s"
		Expect(api.Update(ctx, pool)).To(Succeed())
		Expect(api.Get(ctx, client.ObjectKeyFromObject(pdb), pdb)).To(Succeed())
		allowMinimum := intstr.FromInt32(0)
		pdb.Spec.MinAvailable = &allowMinimum
		Expect(api.Update(ctx, pdb)).To(Succeed())
		Eventually(func(g Gomega) {
			current := &policyv1.PodDisruptionBudget{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(pdb), current)).To(Succeed())
			g.Expect(current.Status.ObservedGeneration).To(Equal(current.Generation))
			g.Expect(current.Status.DisruptionsAllowed).To(BeNumerically(">=", 1))
			g.Expect(readPool().Status.ObservedGeneration).To(Equal(pool.Generation))
		}).Should(Succeed())

		// A UID-scoped admission rule prevents the healthy kubelet from racing our
		// Ready=False injection. It does not block Pod status, leases or CRI work.
		// Unsupported/disabled admission policy is a hard failure, never a skip.
		assertWorker()
		oldUID := readNode(worker).UID
		policyName := "e2e-hold-readiness-" + string(oldUID)
		holdFinalizer := "e2e.nodes.example.com/observe-eviction"
		readinessPolicy := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
			"metadata": map[string]interface{}{"name": policyName},
			"spec": map[string]interface{}{
				"failurePolicy": "Fail",
				"matchConstraints": map[string]interface{}{
					"resourceRules": []interface{}{map[string]interface{}{
						"apiGroups": []interface{}{""}, "apiVersions": []interface{}{"v1"},
						"operations": []interface{}{"UPDATE"}, "resources": []interface{}{"nodes/status"},
						"resourceNames": []interface{}{worker},
					}},
				},
				"validations": []interface{}{map[string]interface{}{
					"expression": fmt.Sprintf("object.metadata.uid != '%s' || !object.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')", oldUID),
					"message":    "Disposable E2E worker readiness is held until live-kubelet drain evidence is collected",
				}},
			},
		}}
		readinessBinding := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
			"metadata": map[string]interface{}{"name": policyName},
			"spec": map[string]interface{}{
				"policyName": policyName, "validationActions": []interface{}{"Deny"},
			},
		}}
		policyCreated, bindingCreated, holdAdded, readinessInjected := false, false, false, false
		DeferCleanup(func() error {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			var failures []error
			for _, entry := range []struct {
				object  *unstructured.Unstructured
				created bool
			}{{readinessBinding, bindingCreated}, {readinessPolicy, policyCreated}} {
				if entry.created {
					uid := entry.object.GetUID()
					if err := client.IgnoreNotFound(api.Delete(cleanupCtx, entry.object, client.Preconditions{UID: &uid})); err != nil {
						failures = append(failures, err)
					}
				}
			}
			// On failure, let the live kubelet restore truthful readiness before
			// releasing the observation finalizer. Never delete a workload for cleanup.
			if readinessInjected && !workerRemoved {
				recoveryCtx, recoveryCancel := context.WithTimeout(cleanupCtx, 45*time.Second)
				if err := waitForE2ERecovery(recoveryCtx, worker, oldUID); err != nil {
					failures = append(failures, err)
				}
				recoveryCancel()
			}
			if holdAdded {
				if err := releaseE2EFinalizer(cleanupCtx, client.ObjectKeyFromObject(&victim), victim.UID, holdFinalizer); err != nil {
					failures = append(failures, err)
				}
			}
			return errors.Join(failures...)
		})
		Expect(api.Create(ctx, readinessPolicy)).To(Succeed(), "requires Kubernetes v1 ValidatingAdmissionPolicy support")
		policyCreated = true
		Expect(api.Create(ctx, readinessBinding)).To(Succeed())
		bindingCreated = true
		Eventually(func(g Gomega) {
			node := readNode(worker)
			g.Expect(node.UID).To(Equal(oldUID))
			for i := range node.Status.Conditions {
				if node.Status.Conditions[i].Type == corev1.NodeReady {
					node.Status.Conditions[i].Status = corev1.ConditionTrue
				}
			}
			err := api.Status().Update(ctx, node, &client.SubResourceUpdateOptions{
				UpdateOptions: client.UpdateOptions{DryRun: []string{metav1.DryRunAll}},
			})
			g.Expect(err).To(HaveOccurred(), "wait for admission enforcement before injecting readiness")
			if err != nil {
				g.Expect(apierrors.IsInvalid(err) || apierrors.IsForbidden(err)).To(BeTrue(), err.Error())
				g.Expect(err.Error()).To(ContainSubstring(policyName))
			}
		}, 45*time.Second, time.Second).Should(Succeed())

		Expect(api.Get(ctx, client.ObjectKeyFromObject(&victim), &victim)).To(Succeed())
		Expect(victim.UID).To(Equal(originals[client.ObjectKeyFromObject(&victim)].UID))
		Expect(victim.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(victim.Status.ContainerStatuses).To(HaveLen(1))
		containerID := victim.Status.ContainerStatuses[0].ContainerID
		_, runtimeID, found := strings.Cut(containerID, "://")
		Expect(found).To(BeTrue())
		Expect(runtimeID).NotTo(BeEmpty())
		running, err := e2eRunningContainers(ctx, worker, victim.UID)
		Expect(err).NotTo(HaveOccurred())
		Expect(running).To(ContainElement(runtimeID), "original workload must actually be running before eviction")
		victim.Finalizers = append(victim.Finalizers, holdFinalizer)
		Expect(api.Update(ctx, &victim)).To(Succeed())
		holdAdded = true

		By("9. Holding only the disposable Node's Ready status; the deployed manager must request eviction")
		readinessInjected = true
		Eventually(func(g Gomega) {
			node := readNode(worker)
			g.Expect(node.UID).To(Equal(oldUID))
			foundReady := false
			for i := range node.Status.Conditions {
				condition := &node.Status.Conditions[i]
				if condition.Type == corev1.NodeReady {
					foundReady = true
					condition.Status = corev1.ConditionFalse
					condition.Reason = "E2EBoundedReadinessInjection"
					condition.Message = "Healthy kubelet; UID-scoped admission hold for controller eviction proof"
					condition.LastTransitionTime = metav1.Now()
				}
			}
			g.Expect(foundReady).To(BeTrue())
			g.Expect(api.Status().Update(ctx, node)).To(Succeed())
		}, 20*time.Second, time.Second).Should(Succeed())
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(&victim), pod)).To(Succeed())
			g.Expect(pod.UID).To(Equal(victim.UID))
			g.Expect(pod.Spec.NodeName).To(Equal(worker))
			g.Expect(pod.DeletionTimestamp.IsZero()).To(BeFalse())
			g.Expect(pod.Finalizers).To(ContainElement(holdFinalizer))
			evicted := false
			for _, condition := range pod.Status.Conditions {
				if condition.Type == corev1.DisruptionTarget && condition.Status == corev1.ConditionTrue &&
					condition.Reason == "EvictionByEvictionAPI" {
					evicted = true
				}
			}
			g.Expect(evicted).To(BeTrue(), "an accepted Eviction API request, not native taint eviction")
			g.Expect(pod.Status.Phase).To(BeElementOf(corev1.PodSucceeded, corev1.PodFailed))
			g.Expect(pod.Status.ContainerStatuses).To(HaveLen(1))
			status := pod.Status.ContainerStatuses[0]
			g.Expect(status.ContainerID).To(Equal(containerID))
			g.Expect(status.State.Terminated).NotTo(BeNil())
			if status.State.Terminated != nil {
				g.Expect(status.State.Terminated.FinishedAt.IsZero()).To(BeFalse())
			}
			node := readNode(worker)
			g.Expect(node.UID).To(Equal(oldUID))
			g.Expect(node.DeletionTimestamp.IsZero()).To(BeTrue())
			g.Expect(node.Spec.Unschedulable).To(BeTrue())
			running, err := e2eRunningContainers(ctx, worker, victim.UID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(running).To(BeEmpty(), "CRI must confirm no running container for this original Pod UID")
		}, 90*time.Second, time.Second).Should(Succeed())
		By(fmt.Sprintf("Accepted API eviction and kubelet-reported termination, confirmed by CRI: Pod UID=%s container=%s Node UID=%s", victim.UID, containerID, oldUID))
		Consistently(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(api.Get(ctx, client.ObjectKeyFromObject(&victim), pod)).To(Succeed())
			g.Expect(pod.UID).To(Equal(victim.UID))
			g.Expect(pod.DeletionTimestamp.IsZero()).To(BeFalse())
			g.Expect(pod.Finalizers).To(ContainElement(holdFinalizer))
			node := readNode(worker)
			g.Expect(node.UID).To(Equal(oldUID))
			g.Expect(node.DeletionTimestamp.IsZero()).To(BeTrue())
			g.Expect(stateForWorker().Phase).To(Equal("Draining"))
		}, 12*time.Second, time.Second).Should(Succeed())

		By("Releasing only the finalizer on the already-evicted, runtime-terminated Pod; no Pod or Deployment deletion")
		Expect(releaseE2EFinalizer(ctx, client.ObjectKeyFromObject(&victim), victim.UID, holdFinalizer)).To(Succeed())
		holdAdded = false
		Eventually(func() bool {
			return apierrors.IsNotFound(api.Get(ctx, client.ObjectKeyFromObject(&victim), &corev1.Pod{}))
		}, 30*time.Second, time.Second).Should(BeTrue())
		Eventually(func(g Gomega) {
			node := &corev1.Node{}
			err := api.Get(ctx, client.ObjectKey{Name: worker}, node)
			if !apierrors.IsNotFound(err) {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(node.UID).NotTo(Equal(oldUID), "a live kubelet may register a new Node; the old UID must be gone")
			}
		}, 45*time.Second, time.Second).Should(Succeed())
		workerRemoved = true
		By(fmt.Sprintf("Original Pod UID=%s disappeared before observing removal of old Node UID=%s", victim.UID, oldUID))
		survivingDeployment := &appsv1.Deployment{}
		Expect(api.Get(ctx, client.ObjectKeyFromObject(deployment), survivingDeployment)).To(Succeed())
		Expect(survivingDeployment.UID).To(Equal(deployment.UID))
		for key, original := range originals {
			if original.Spec.NodeName != worker {
				pod := &corev1.Pod{}
				Expect(api.Get(ctx, key, pod)).To(Succeed())
				Expect(pod.UID).To(Equal(original.UID))
				Expect(pod.Spec.NodeName).To(Equal(original.Spec.NodeName))
				Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
			}
		}
		Eventually(func(g Gomega) {
			current := readPool()
			g.Expect(current.Status.ReadyNodes).To(Equal(int32(1)))
			for _, state := range current.Status.Nodes {
				g.Expect(state.UID).NotTo(Equal(string(oldUID)))
			}
		}).Should(Succeed())

		By("10. Confirming real Events and deleting the pool with ownership-safe Ready-node cleanup")
		Eventually(func(g Gomega) {
			var recorded eventsv1.EventList
			g.Expect(api.List(ctx, &recorded)).To(Succeed())
			found := false
			for _, event := range recorded.Items {
				if event.Regarding.UID == pool.UID && event.Reason == "DrainTimedOut" {
					found = true
				}
			}
			g.Expect(found).To(BeTrue())
		}).Should(Succeed())
		Expect(api.Delete(ctx, readPool())).To(Succeed())
		Eventually(func() bool {
			return apierrors.IsNotFound(api.Get(ctx, client.ObjectKeyFromObject(pool), &nodesv1alpha1.NodePool{}))
		}).Should(BeTrue())
		survivor := readNode(qaCluster + "-worker2")
		Expect(survivor.Labels).NotTo(HaveKey("tier"))
		Expect(survivor.Labels).To(HaveKeyWithValue("external.example.com/keep", "yes"))
		Expect(survivor.Annotations).NotTo(HaveKey("nodes.example.com/managed-settings"))
		for _, taint := range survivor.Spec.Taints {
			Expect(taint.Key).NotTo(Equal("dedicated"))
		}
		// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness
		// +kubebuilder:scaffold:e2e-webhooks-checks
	})
})

// The kubelet's terminal status and this independent CRI observation are both
// required: stopping kubelet or merely losing a Node/Pod object is not termination.
func e2eRunningContainers(ctx context.Context, node string, uid types.UID) ([]string, error) {
	commandCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(commandCtx, "docker", "exec", node, "crictl", "ps",
		"--label", "io.kubernetes.pod.uid="+string(uid), "-o", "json").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("read running CRI containers: %w: %s", err, output)
	}
	var response struct {
		Containers *[]struct {
			ID string `json:"id"`
		} `json:"containers"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		return nil, err
	}
	if response.Containers == nil {
		return nil, fmt.Errorf("CRI response did not contain a containers list")
	}
	ids := make([]string, 0, len(*response.Containers))
	for _, container := range *response.Containers {
		ids = append(ids, container.ID)
	}
	return ids, nil
}

func releaseE2EFinalizer(ctx context.Context, key client.ObjectKey, uid types.UID, finalizer string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		pod := &corev1.Pod{}
		if err := api.Get(ctx, key, pod); err != nil {
			return client.IgnoreNotFound(err)
		}
		if pod.UID != uid {
			return fmt.Errorf("refusing to release finalizer on replacement Pod %s", key)
		}
		finalizers := make([]string, 0, len(pod.Finalizers))
		for _, current := range pod.Finalizers {
			if current != finalizer {
				finalizers = append(finalizers, current)
			}
		}
		if len(finalizers) == len(pod.Finalizers) {
			return nil
		}
		pod.Finalizers = finalizers
		if err := api.Update(ctx, pod); !apierrors.IsConflict(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitForE2ERecovery(ctx context.Context, name string, uid types.UID) error {
	for {
		node := &corev1.Node{}
		err := api.Get(ctx, client.ObjectKey{Name: name}, node)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if node.UID != uid {
			return nil
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("live kubelet did not restore Ready after admission cleanup: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
