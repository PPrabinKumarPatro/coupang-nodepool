package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
)

func TestTerminalPodDeletionMustFinish(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		for _, terminating := range []bool{false, true} {
			name := string(phase) + "/completed"
			if terminating {
				name = string(phase) + "/terminating"
			}
			t.Run(name, func(t *testing.T) {
				pod := lifecyclePod("terminal")
				pod.Status.Phase = phase
				if terminating {
					stamp := metav1.Now()
					pod.DeletionTimestamp = &stamp
					pod.Finalizers = []string{"test.example.com/observe-termination"}
				}
				r, store, writer, now, _ := lifecycleFixture(t, "Remove", 1, pod)
				for range 4 {
					lifecycleRun(t, r)
				}
				if !terminating {
					if lifecycleCallCount(writer, "delete-node:") != 1 {
						t.Fatal("a completed non-terminating Pod unexpectedly blocked removal")
					}
					return
				}
				if lifecycleCallCount(writer, "evict:") != 0 || lifecycleCallCount(writer, "delete-node:") != 0 {
					t.Fatal("terminating terminal Pod was evicted again or its Node deleted")
				}
				start := lifecycleState(t, store, "worker-0").DrainStartedAt.DeepCopy()
				*now = now.Add(time.Minute)
				lifecycleRun(t, r)
				state := lifecycleState(t, store, "worker-0")
				if state.Phase != "Draining" || state.Reason != "DrainTimedOut" ||
					!state.DrainStartedAt.Equal(start) {
					t.Fatalf("termination did not retain its reservation and deadline: %+v", state)
				}
				current := &corev1.Pod{}
				if err := store.Get(context.Background(), client.ObjectKeyFromObject(pod), current); err != nil {
					t.Fatal(err)
				}
				current.Finalizers = nil
				if err := store.Update(context.Background(), current); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					lifecycleRun(t, r)
				}
				if lifecycleCallCount(writer, "delete-node:") != 1 {
					t.Fatal("resolved termination did not permit removal")
				}
			})
		}
	}
}

// Forward the successful write, then lose its response: retries must use persisted reality.
type lostWriteResponseClient struct {
	client.Client
	point string
	fired bool
}

func (c *lostWriteResponseClient) lose(point string, err error) error {
	if err == nil && !c.fired && c.point == point {
		c.fired = true
		return errors.New("test: write committed, response lost")
	}
	return err
}

func (c *lostWriteResponseClient) Patch(ctx context.Context, object client.Object, patch client.Patch, opts ...client.PatchOption) error {
	err := c.Client.Patch(ctx, object, patch, opts...)
	if _, ok := object.(*corev1.Node); ok {
		return c.lose("cordon", err)
	}
	return err
}

func (c *lostWriteResponseClient) Delete(ctx context.Context, object client.Object, opts ...client.DeleteOption) error {
	return c.lose("node-delete", c.Client.Delete(ctx, object, opts...))
}

func (c *lostWriteResponseClient) Status() client.SubResourceWriter {
	return &lostStatusResponse{SubResourceWriter: c.Client.Status(), owner: c}
}

type lostStatusResponse struct {
	client.SubResourceWriter
	owner *lostWriteResponseClient
}

func (s *lostStatusResponse) Patch(ctx context.Context, object client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	err := s.SubResourceWriter.Patch(ctx, object, patch, opts...)
	pool, ok := object.(*nodesv1alpha1.NodePool)
	if ok && len(pool.Status.Nodes) > 0 {
		switch pool.Status.Nodes[0].Phase {
		case "Draining":
			return s.owner.lose("reservation", err)
		case "Removing":
			return s.owner.lose("removing", err)
		}
	}
	return err
}

func (c *lostWriteResponseClient) SubResource(name string) client.SubResourceClient {
	return &lostSubresourceResponse{SubResourceClient: c.Client.SubResource(name), owner: c, name: name}
}

type lostSubresourceResponse struct {
	client.SubResourceClient
	owner *lostWriteResponseClient
	name  string
}

func (s *lostSubresourceResponse) Create(ctx context.Context, object, body client.Object, opts ...client.SubResourceCreateOption) error {
	err := s.SubResourceClient.Create(ctx, object, body, opts...)
	if s.name == "eviction" {
		return s.owner.lose("eviction", err)
	}
	return err
}

func TestLifecycleRecoversFromCommittedWriteLostResponse(t *testing.T) {
	for _, point := range []string{"reservation", "cordon", "eviction", "removing", "node-delete"} {
		t.Run(point, func(t *testing.T) {
			r, store, writer, now, _ := lifecycleFixture(t, "Remove", 1, lifecyclePod("work"))
			fault := &lostWriteResponseClient{Client: writer, point: point}
			r.Client = fault
			for attempts := 0; !fault.fired && attempts < 8; attempts++ {
				_, err := settingsReconcile(r)
				if fault.fired && err == nil {
					t.Fatal("lost response was not surfaced")
				}
				if !fault.fired && err != nil {
					t.Fatal(err)
				}
			}
			if !fault.fired {
				t.Fatal("target write was never exercised")
			}
			before := readSettingsPool(t, store)
			start := before.Status.Nodes[0].DrainStartedAt.DeepCopy()
			*now = now.Add(2 * time.Second)
			restarted := &NodePoolReconciler{Client: writer, APIReader: store, Scheme: r.Scheme, Now: r.Now}
			lifecycleRun(t, restarted)
			after := readSettingsPool(t, store)
			if len(after.Status.Nodes) > 0 && !after.Status.Nodes[0].DrainStartedAt.Equal(start) {
				t.Fatal("retry reset the persisted drain deadline")
			}
			for range 6 {
				lifecycleRun(t, restarted)
			}
			if lifecycleCallCount(writer, "evict:") != 1 ||
				lifecycleCallCount(writer, "delete-node:") != 1 {
				t.Fatalf("retry duplicated or skipped committed work: %v", writer.calls)
			}
			err := store.Get(context.Background(), client.ObjectKey{Name: "worker-0"}, &corev1.Node{})
			if !apierrors.IsNotFound(err) {
				t.Fatalf("Node removal did not converge: %v", err)
			}
		})
	}
}
