/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

var _ = Describe("NodePool settings reconciliation against envtest", func() {
	var pool *nodesv1alpha1.NodePool
	var node *corev1.Node
	var reconciler *NodePoolReconciler
	testContext := context.Background()

	reconcilePool := func() error {
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
		pool = &nodesv1alpha1.NodePool{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "settings-pool-"},
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
		Expect(k8sClient.Create(testContext, pool)).To(Succeed())
		DeferCleanup(func() {
			current := &nodesv1alpha1.NodePool{}
			err := k8sClient.Get(testContext, client.ObjectKeyFromObject(pool), current)
			if errors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			controllerutil.RemoveFinalizer(current, nodeSettingsFinalizer)
			Expect(k8sClient.Update(testContext, current)).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, current))).To(Succeed())
		})
		node = &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "settings-worker-",
				Labels: map[string]string{
					membershipLabel: pool.Name, "unrelated.example.com/label": "keep",
				},
			},
		}
		Expect(k8sClient.Create(testContext, node)).To(Succeed())
		DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(testContext, node))).To(Succeed())
		})
		node.Status.Conditions = []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
		}}
		Expect(k8sClient.Status().Update(testContext, node)).To(Succeed())
		reconciler = &NodePoolReconciler{
			Client: k8sClient, APIReader: k8sClient, Scheme: k8sClient.Scheme(),
		}
	})

	It("persists the finalizer before settings and avoids unchanged writes", func() {
		Expect(reconcilePool()).To(Succeed())
		Expect(controllerutil.ContainsFinalizer(readPool(), nodeSettingsFinalizer)).To(BeTrue())
		Expect(readNode().Labels).NotTo(HaveKey("workload"))
		Expect(readNode().Annotations).NotTo(HaveKey(nodeSettingsAnnotation))

		Expect(reconcilePool()).To(Succeed())
		currentNode := readNode()
		currentPool := readPool()
		Expect(currentNode.Labels).To(HaveKeyWithValue("workload", "batch"))
		Expect(currentNode.Spec.Taints).To(ContainElement(pool.Spec.Taints[0]))
		Expect(currentNode.Annotations).To(HaveKey(nodeSettingsAnnotation))
		Expect(currentPool.Status.ReadyNodes).To(Equal(int32(1)))
		Expect(currentPool.Status.Nodes).To(HaveLen(1))
		Expect(currentPool.Status.Nodes[0].UID).To(Equal(string(node.UID)))
		Expect(currentPool.Status.ObservedGeneration).To(Equal(currentPool.Generation))

		Expect(reconcilePool()).To(Succeed())
		Expect(readNode().ResourceVersion).To(Equal(currentNode.ResourceVersion))
		Expect(readPool().ResourceVersion).To(Equal(currentPool.ResourceVersion))
	})

	It("propagates spec changes to a NotReady member", func() {
		unownedTaints := readNode().Spec.Taints
		Expect(reconcilePool()).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		currentNode := readNode()
		currentNode.Status.Conditions[0].Status = corev1.ConditionFalse
		Expect(k8sClient.Status().Update(testContext, currentNode)).To(Succeed())
		currentPool := readPool()
		currentPool.Spec.Labels = map[string]string{"new-setting": "new-value"}
		currentPool.Spec.Taints = nil
		Expect(k8sClient.Update(testContext, currentPool)).To(Succeed())
		Expect(reconcilePool()).To(Succeed())

		currentNode = readNode()
		Expect(currentNode.Labels).NotTo(HaveKey("workload"))
		Expect(currentNode.Labels).To(HaveKeyWithValue("new-setting", "new-value"))
		Expect(currentNode.Labels).To(HaveKeyWithValue("unrelated.example.com/label", "keep"))
		Expect(currentNode.Spec.Taints).To(Equal(unownedTaints))
		Expect(readPool().Status.ReadyNodes).To(BeZero())
		Expect(readPool().Status.Nodes[0].NotReadySince).NotTo(BeNil())
	})

	It("cleans owned settings when a Node leaves membership", func() {
		Expect(reconcilePool()).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		currentNode := readNode()
		delete(currentNode.Labels, membershipLabel)
		Expect(k8sClient.Update(testContext, currentNode)).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		Expect(readNode().Labels).NotTo(HaveKey("workload"))
		Expect(readNode().Annotations).NotTo(HaveKey(nodeSettingsAnnotation))
		Expect(readNode().Labels).To(HaveKeyWithValue("unrelated.example.com/label", "keep"))
		Expect(readPool().Status.Nodes).To(BeEmpty())
	})

	It("cleans a Ready owned Node before allowing pool deletion", func() {
		unownedTaints := readNode().Spec.Taints
		Expect(reconcilePool()).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		Expect(k8sClient.Delete(testContext, readPool())).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		Expect(readNode().Labels).NotTo(HaveKey("workload"))
		Expect(readNode().Spec.Taints).To(Equal(unownedTaints))
		Expect(readNode().Annotations).NotTo(HaveKey(nodeSettingsAnnotation))
		err := k8sClient.Get(testContext, client.ObjectKeyFromObject(pool), &nodesv1alpha1.NodePool{})
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("leaves NotReady ownership intact when finalizing pool deletion", func() {
		Expect(reconcilePool()).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		currentNode := readNode()
		currentNode.Status.Conditions[0].Status = corev1.ConditionUnknown
		Expect(k8sClient.Status().Update(testContext, currentNode)).To(Succeed())
		before := readNode()
		Expect(k8sClient.Delete(testContext, readPool())).To(Succeed())
		Expect(reconcilePool()).To(Succeed())
		Expect(readNode().ResourceVersion).To(Equal(before.ResourceVersion))
		Expect(readNode().Annotations).To(HaveKey(nodeSettingsAnnotation))
		err := k8sClient.Get(testContext, client.ObjectKeyFromObject(pool), &nodesv1alpha1.NodePool{})
		Expect(errors.IsNotFound(err)).To(BeTrue())
	})

	It("rejects a stale Node patch and preserves the concurrent actor on retry", func() {
		Expect(reconcilePool()).To(Succeed())
		writer := &settingsPatchClient{Client: k8sClient}
		writer.beforePatch = func(ctx context.Context, object client.Object) error {
			if _, ok := object.(*corev1.Node); !ok {
				return fmt.Errorf("unexpected mutation type %T", object)
			}
			current := &corev1.Node{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(node), current); err != nil {
				return err
			}
			current.Labels["human.example.com/label"] = "preserved"
			return k8sClient.Update(ctx, current)
		}
		reconciler.Client = writer
		err := reconcilePool()
		Expect(errors.IsConflict(err)).To(BeTrue())
		Expect(readNode().Labels).NotTo(HaveKey("workload"))
		Expect(readNode().Labels).To(HaveKeyWithValue("human.example.com/label", "preserved"))
		Expect(controllerutil.ContainsFinalizer(readPool(), nodeSettingsFinalizer)).To(BeTrue())

		writer.beforePatch = nil
		Expect(reconcilePool()).To(Succeed())
		Expect(readNode().Labels).To(HaveKeyWithValue("workload", "batch"))
		Expect(readNode().Labels).To(HaveKeyWithValue("human.example.com/label", "preserved"))
	})
})
