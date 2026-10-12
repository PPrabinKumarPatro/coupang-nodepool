//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nodesv1alpha1 "example.com/nodepool-controller/api/v1alpha1"
	"example.com/nodepool-controller/test/utils"
)

const managerNamespace = "coupang-nodepool-system"
const managerName = "coupang-nodepool-controller-manager"
const qaCluster = "coupang-nodepool-e2e"

var (
	api        client.Client
	kubeconfig string
	worker     string
	pauseImage string
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodePool real-manager E2E and demonstration")
}

func command(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	output, err := utils.Run(exec.CommandContext(ctx, name, args...))
	Expect(err).NotTo(HaveOccurred())
	return output
}

func kubectl(args ...string) string {
	return command("kubectl", append([]string{"--kubeconfig", kubeconfig}, args...)...)
}

var _ = BeforeSuite(func() {
	Expect(os.Getenv("COUPANG_ALLOW_DISRUPTIVE_E2E")).To(Equal("yes"), "explicit disruption opt-in required")
	Expect(os.Getenv("E2E_KIND_CLUSTER")).To(Equal(qaCluster))
	kubeconfig, worker, pauseImage = os.Getenv("E2E_KUBECONFIG"), os.Getenv("E2E_WORKER"), os.Getenv("E2E_PAUSE_IMAGE")
	Expect(kubeconfig).NotTo(BeEmpty())
	Expect(worker).To(Equal(qaCluster + "-worker"))
	Expect(pauseImage).To(HavePrefix("registry.k8s.io/pause:"))
	raw, err := clientcmd.LoadFromFile(kubeconfig)
	Expect(err).NotTo(HaveOccurred())
	Expect(raw.CurrentContext).To(Equal("kind-" + qaCluster))
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	Expect(err).NotTo(HaveOccurred())
	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(nodesv1alpha1.AddToScheme(scheme)).To(Succeed())
	api, err = client.New(config, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
	var nodes corev1.NodeList
	Expect(api.List(context.Background(), &nodes)).To(Succeed())
	Expect(nodes.Items).To(HaveLen(3))
	for _, node := range nodes.Items {
		Expect(node.Name).To(BeElementOf(qaCluster+"-control-plane", worker, qaCluster+"-worker2"))
		Expect(node.Spec.ProviderID).To(HavePrefix("kind://"))
	}
	assertWorker()
	SetDefaultEventuallyTimeout(3 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)
})

func assertWorker() {
	var node corev1.Node
	Expect(api.Get(context.Background(), client.ObjectKey{Name: worker}, &node)).To(Succeed())
	Expect(node.Labels).NotTo(HaveKey("node-role.kubernetes.io/control-plane"))
	Expect(node.Labels).NotTo(HaveKey("node-role.kubernetes.io/master"))
	Expect(command("docker", "inspect", worker, "--format", `{{index .Config.Labels "io.x-k8s.kind.cluster"}}`)).
		To(Equal(qaCluster + "\n"))
}
