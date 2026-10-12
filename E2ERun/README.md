# Live end-to-end validation

**PASS:** one complete scenario, no failures or skipped specs. Test execution
took 130.60 seconds, excluding cluster creation, image build and cleanup.
The test began October 12, 2026 at 03:18:01 UTC (October 11 Pacific).

This run used the submitted Go source and scripts, Go 1.27.2 and Kubernetes
1.37.0 in a new three-node kind cluster named `coupang-nodepool-e2e`.
The controller was built locally; dependencies and images were cached.

| Evidence | What it establishes |
|---|---|
| [tests.log](20261012T031801Z/tests.log) | Real service-account permissions, worker joins/settings, PDB-blocked original Pods, persisted clocks across restart, recovery, accepted eviction, runtime termination and ownership-safe cleanup |
| [runner.log](20261012T031801Z/runner.log) | Build, three Ready Nodes, controller rollout, complete test outcome and successful disposable-cluster deletion |
| [versions.json](20261012T031801Z/versions.json) | Actual Kubernetes client/server versions |
| [events.txt](20261012T031801Z/events.txt), [manager.log](20261012T031801Z/manager.log) | Kubernetes transitions and controller diagnostics |
| [nodes.txt](20261012T031801Z/nodes.txt), [pods.txt](20261012T031801Z/pods.txt), [pools.yaml](20261012T031801Z/pools.yaml) | Final snapshots collected after the scenario, before cluster teardown; these are not baseline snapshots |
| [SOURCE-SHA256SUMS](SOURCE-SHA256SUMS) | Tested source, dependencies, scripts, build specification and deployment configuration |
| [SHA256SUMS](SHA256SUMS) | Integrity of the recorded run files |

Reproduce from the repository root:

```bash
COUPANG_ALLOW_DISRUPTIVE_E2E=yes COUPANG_ALLOW_IMAGE_PULL=yes bash hack/demo.sh
```

Set `COUPANG_ALLOW_IMAGE_PULL=no` to require the pinned kind image already cached.
The script refuses an existing cluster of the same name and verifies ownership
before deletion. It creates a private kubeconfig without changing the default
context. Kubeconfig contents are not included; command logs retain its local path.

Verify the recorded bytes from the repository root:

```bash
(cd E2ERun && sha256sum -c SHA256SUMS)
sha256sum -c E2ERun/SOURCE-SHA256SUMS
```

For the successful-drain phase, a temporary admission policy holds only the
target Node UID's Ready condition while its kubelet stays alive. The controller
must obtain an allowed eviction; terminal Pod status and CRI confirm process
termination. A temporary Pod finalizer holds deletion while the test asserts
the old Node remains. Only then is that finalizer released and Node removal
observed. The workload Deployment is not deleted to manufacture an empty drain.

This is a local run, not remote CI or independent attestation. It does not prove
safe storage failover, machine fencing, application networking or exhaustive HA
behavior. The original manually captured evidence remains in `DemoRun/`.
