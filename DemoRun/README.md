# Recorded demonstration and test evidence

`20261012T015458Z/` contains the original user-run terminal outputs, captured on
October 11, 2026 Pacific time. The folder name is UTC. Files 01-97 are the manual
VM/container walkthrough; file 98 is the fresh local automated build/test run.
Use the tables below to open the evidence for each scenario. The
[main README](../README.md#validation-environments) includes the lab diagrams.

## Test scenarios

### Manual validation: three Ubuntu VMs

One control-plane and two workers, Kubernetes 1.37.0, with the manager on the control-plane and pool membership limited to workers.
Ignore policy plus a NoSchedule taint; no VM kubelet was stopped and no VM Node was removed.

| Grouped scenario | Observed result | Evidence |
|---|---|---|
| Health, settings and scheduling boundary | PASS: three Ready Nodes, healthy Pods, one workload Pod per worker; untolerated Pod rejected and cleaned up | 01-10: [Nodes](20261012T015458Z/01-nodes.txt), [settings](20261012T015458Z/05-node-taints.txt), [rejection](20261012T015458Z/07-untolerated-description.txt), [PDB](20261012T015458Z/10-pdb-baseline.yaml) |
| Membership leave/rejoin | PASS: owned settings/receipt removed and restored; original Pod UIDs unchanged | 11-20: [cleanup](20261012T015458Z/14-settings-after-leave.txt), [receipt](20261012T015458Z/19-settings-after-rejoin.txt), [Pods](20261012T015458Z/20-workloads-after-rejoin.txt) |
| Spec update and restoration | PASS: both workers converge, generation acknowledgement advances, original workloads remain | 21-29: [updated pool](20261012T015458Z/24-pool-after-spec-change.yaml), [Pods](20261012T015458Z/25-workloads-after-spec-change.txt), [restoration](20261012T015458Z/29-restored-pool.txt) |
| Pool deletion, ownership preservation and recreation | PASS: owned settings cleaned, external label/Nodes/Pods preserved; new pool UID recorded in receipts | 30-40: [external label](20261012T015458Z/32-nodes-after-pool-deletion.txt), [cleaned receipts](20261012T015458Z/33-settings-after-pool-deletion.txt), [new pool](20261012T015458Z/38-recreated-pool.yaml), [new receipts](20261012T015458Z/39-recreated-settings.txt) |

### Manual validation: three disposable kind containers

One control-plane and two workers, Kubernetes 1.37.0, with a separate kubeconfig and verified local controller image.
Remove policy: 15s grace, 120s drain timeout, one removal slot; two Pods with minAvailable=2 PDB and indefinite native health-taint tolerations.

| Grouped scenario | Observed result | Evidence |
|---|---|---|
| Cluster, image, deployment and baseline | PASS: healthy Nodes/controller, verified imported image, two worker Pods, zero allowed PDB disruptions | 41-60: [Nodes](20261012T015458Z/44-manual-nodes.txt), [image](20261012T015458Z/52-imported-image-details.txt), [Pod UIDs](20261012T015458Z/58-manual-pod-baseline.txt), [PDB](20261012T015458Z/59-manual-pdb-baseline.yaml) |
| Worker failure and PDB enforcement | PASS: target worker cordoned; API refuses eviction; original Pods remain non-terminating | 61-65: [blocked pool](20261012T015458Z/63-blocked-pool.yaml), [Nodes](20261012T015458Z/64-blocked-nodes.txt), [Pods](20261012T015458Z/65-blocked-pods.txt) |
| Timeout and manager restart | PASS: no forced deletion; original failure/drain clocks and Pod identities survive restart | 66-73: [before](20261012T015458Z/67-timeout-before-restart.yaml), [after](20261012T015458Z/70-timeout-after-restart.yaml), [retained Pods](20261012T015458Z/72-pods-after-timeout.txt) |
| Worker recovery | PASS: Ready restored, owned cordon/clocks/failure cleared, original Pods retained | 74-80: [Nodes](20261012T015458Z/78-recovered-nodes.txt), [pool](20261012T015458Z/79-recovered-pool.yaml), [Pods](20261012T015458Z/80-recovered-pods.txt) |
| Empty-worker removal | PASS: after explicit healthy-workload teardown, second failure produces controller-driven Node removal | 81-88: [teardown](20261012T015458Z/81-workload-teardown.txt), [remaining Nodes](20261012T015458Z/86-nodes-after-removal.txt), [Events](20261012T015458Z/88-removal-events.txt) |
| Finalization and cleanup | PASS: survivor settings cleaned; disposable cluster removed; original cluster/context and VM workloads preserved | 89-97: [worker cleanup](20261012T015458Z/90-surviving-worker-cleanup.yaml), [cluster cleanup](20261012T015458Z/93-manual-cluster-cleanup.txt), [VM Nodes](20261012T015458Z/96-preserved-vm-nodes.txt), [VM Pods](20261012T015458Z/97-preserved-vm-workloads.txt) |

### Automated validation

The unit/API output uses Go 1.27.2, race detection, shuffled test order and isolated Kubernetes 1.37.0 API assets.
A separate live E2E run deploys the actual controller in three-node kind; its logs supplement, rather than replace, the original manual evidence.

| Grouped checks | Submitted result / scope | Evidence or entry point |
|---|---|---|
| Build, formatting and static analysis | PASS: static manager build, gofmt check and go vet | [98-local-automated-validation.txt](20261012T015458Z/98-local-automated-validation.txt) |
| Unit and real isolated-API tests | PASS: 62 unit functions, 175 named subtests, 21 API specs; no race failure. Ownership/protection, member isolation, clocks, lost responses, real eviction preconditions, reservations and watches | [Same complete log](20261012T015458Z/98-local-automated-validation.txt) |
| Live automated E2E | PASS: 1/1 scenario, 130.60s test time; actual RBAC, settings, PDB blocking, persisted clocks/restart, recovery, accepted eviction, CRI-confirmed termination, Node retention/removal and cleanup | [Run notes](../E2ERun/README.md), [test output](../E2ERun/20261012T031801Z/tests.log), [complete runner/cleanup output](../E2ERun/20261012T031801Z/runner.log) |

## Evidence integrity and interpretation

These outputs are preserved byte-for-byte from the working project's evidence.
`SHA256SUMS` records their hashes. Additional local validation does not overwrite
these records. The new [live E2E evidence](../E2ERun/README.md) is stored separately.

```bash
cd DemoRun
sha256sum -c SHA256SUMS
```

Important interpretation:

- File 74 is empty because a successful `systemctl start kubelet` prints nothing.
  Subsequent readiness/status checks establish recovery.
- Manual removal occurs only after explicit workload teardown (file 81). It
  proves empty-worker removal, not successful eviction of a running workload.
- The supplied automated E2E separately tests accepted eviction, kubelet/CRI
  termination and retained-Node ordering; it is not part of these manual logs.
- File 98 contains 62 passed top-level unit functions, 175 named nested subtests
  and 21 passed isolated API specs, followed by `LOCAL VALIDATION: PASS`.
  The Go shuffle seed is `1791773871333879135`.
- Runtime import digests differ from Docker OCI-index digests. File 52 maps the
  imported controller to configuration digest
  `sha256:708eaf84b37dad8d75d3ea6905be131b460c3e0ee8c8b4f8e8df836eea4c98f7`.
- Logs contain synthetic object UIDs, private lab addresses and local filesystem
  paths for context; they do not include kubeconfig contents, tokens or private
  keys. They are local records, not independent attestation or remote CI output.

The tests use disposable stateless pause containers. They do not prove
application networking, persistent-storage failover, infrastructure fencing or
every distributed-system failure. Hashes establish file integrity, not independent
verification of the environment.
