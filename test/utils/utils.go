package utils

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/onsi/ginkgo/v2"
)

// Run is for nonsecret test commands only; never pass tokens or kubeconfig contents.
func Run(cmd *exec.Cmd) (string, error) {
	fmt.Fprintf(ginkgo.GinkgoWriter, "$ %s\n", strings.Join(cmd.Args, " "))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s: %w\n%s", cmd.Path, err, output)
	}
	return string(output), nil
}
