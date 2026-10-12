# NodePool controller

Kubernetes runs applications on worker machines called **nodes**. A **NodePool**
groups nodes that should share the same settings and rules for handling failures.
Instead of configuring each node individually, you describe those settings once
for the pool.

The **NodePool controller** is a background service that keeps those settings in
sync as nodes join or the pool configuration changes. It applies labels and
taints: tags and scheduling rules that help Kubernetes decide which applications
can run on each node. It also monitors node health. Depending on the selected
policy, an unhealthy node can be left alone, blocked from receiving new work, or
removed from Kubernetes after a waiting period and workload-drain safety checks.
The controller reports its progress and explains when a safety check blocks action.

Built in Go with Kubebuilder, it exposes the cluster-scoped
`nodes.example.com/v1alpha1` NodePool API. Membership is assigned through the
Node label `nodes.example.com/nodepool=<pool-name>`.
**It manages Kubernetes records, not the underlying machines:** it does not
create or power off machines, and deleting a Node record does not stop that
machine from running. `spec.image` records information for the infrastructure
layer; this controller does not install or change operating-system images.

## Architecture and safety contract

```text
NodePool / Node / Pod events + timed requeue
    -> read current pool, membership and ownership
    -> synchronize settings; isolate member-specific failures
    -> observe health and persist clocks/removal reservations
    -> reauthorize -> cordon -> PDB-aware eviction -> guarded Node deletion
    -> persist status and emit best-effort transition Events
```

| Concern | Contract |
|---|---|
| Membership | External Node label; pool and Node UIDs prevent same-name identity reuse |
| Settings | Apply declared labels/taints to eligible members, including unhealthy workers; preserve unowned/external state |
| Policies | Ignore: no unhealthy-node lifecycle action. Cordon: stop new scheduling after grace. Remove: cordon, drain through eviction, then delete the Node object |
| Clocks / slots | Persist NotReady and drain start timestamps; reserve removal capacity before disruption; blocked active removals retain their slots |
| Partial failures | Member errors allow independent sequential progress; shared authorization, observation and status-write failures stop shared work |
| Protection | Control-plane/master Nodes reject nonempty desired settings and lifecycle disruption; role-label keys are reserved |
| Pool deletion | Finalizer cleans eligible owned settings/cordons on still-Ready Nodes, including former members; unready Nodes are deliberately skipped |
| Reporting | Ready, Progressing and Failed conditions; per-Node phase/reason; successful-processing `observedGeneration`; Events are best-effort |

`Ignore` does **not** disable settings sync. Explicit worker **NoExecute** taints
can trigger native Kubernetes eviction outside this operator's PDB, grace,
timeout and removal-slot protections. NoSchedule alone does not evict existing Pods.

## Build and run

### Prerequisites

- **Operating environment:** Linux amd64, including Ubuntu inside WSL2.
- **Build and local tests:** Go, Bash, Make and a C compiler for race detection.
- **Test-asset preparation:** Python 3, curl, sha256sum and sha512sum.
- **Live Kubernetes demonstration:** Docker, kind, kubectl and Python 3.
- **Regenerating CRD/RBAC/DeepCopy files only:** controller-gen.

**Validated versions** (pinned in [hack/versions.env](hack/versions.env)):

| Tool / component | Version |
|---|---|
| Go | 1.27.2 |
| Kubernetes server / isolated API test assets | 1.37.0 |
| kubectl | 1.37.1 |
| kind | 0.33.0 |
| controller-gen | 0.22.0 |

**Compatibility:** the assignment requires Kubernetes 1.35 or newer; validation
used 1.37.0. Other versions and architectures have not been verified.
`go.mod` declares Go 1.26.0; the validated toolchain is Go 1.27.2.

### Local build and isolated API tests

From the repository root, with network access for initial dependency/tool setup:

```bash
go mod download
go mod verify
export KUBEBUILDER_ASSETS="$(bash hack/envtest-assets.sh)"
make build lint test
```

If envtest assets are already installed, set `KUBEBUILDER_ASSETS` to that directory
instead. `make test` uses an isolated API server and etcd, not an existing cluster.
`make build` writes a static `bin/manager`; `make lint` checks formatting and
runs `go vet`. Neither installs tools or regenerates source implicitly.

For a cached-only environment, set `GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local`
and use existing envtest assets. For verbose, fresh, shuffled evidence:

```bash
USE_EXISTING_CLUSTER=false KUBECONFIG=/dev/null \
  go test -mod=readonly -race -shuffle=on -count=1 -timeout=4m \
  -v ./... -ginkgo.v -ginkgo.no-color
```

To intentionally regenerate the committed CRD/RBAC/DeepCopy files:

```bash
source hack/versions.env
GOBIN="$PWD/bin" go install "sigs.k8s.io/controller-tools/cmd/controller-gen@$CONTROLLER_GEN_VERSION"
make generate manifests
```

### Reproducible disposable demonstration

**This is disruptive, but only within a new script-owned kind cluster.**
It refuses an existing `coupang-nodepool-e2e` cluster, uses a private kubeconfig,
verifies container identity before cleanup, and does not change the global
default context. Allow approximately 7 GiB plus host/build overhead for its
three container Nodes.

```bash
COUPANG_ALLOW_DISRUPTIVE_E2E=yes \
COUPANG_ALLOW_IMAGE_PULL=yes \
  bash hack/demo.sh
```

For a cached pinned kind image, use `COUPANG_ALLOW_IMAGE_PULL=no`.
The runner builds the controller locally using a scratch image; it does not push
to a registry. The workload uses the pause image embedded in the pinned kind
image with `imagePullPolicy: Never`.

The narrated script demonstrates pool creation, worker joins, settings changes,
PDB blocking, timeout, manager restart, worker recovery, successful workload
eviction, Node-object removal and pool deletion. For successful draining, a
temporary admission policy holds one worker UID's Ready status while its kubelet
stays alive. Actual eviction, kubelet/CRI termination and a finalizer-held
Node-retention checkpoint are asserted before old Node UID removal.
Only that disposable worker is targeted; unsupported prerequisites fail explicitly.

The main evidence file is `artifacts/e2e/tests.log`; diagnostics include Node/Pod
snapshots, Events and manager logs. These are generated outputs, not committed
source. On partial creation or an ownership mismatch, cleanup refuses blind
deletion and prints recovery instructions. A retained private kubeconfig is a
credential: never commit or upload it.

### Deploy without running the disruptive demonstration

Use a dedicated development cluster and explicit kubeconfig. For example, on an
existing kind cluster you own:

```bash
export KUBECONFIG=/absolute/path/to/dedicated-kubeconfig
kubectl --kubeconfig "$KUBECONFIG" config current-context
make docker-build IMG=coupang-nodepool:dev
kind load docker-image coupang-nodepool:dev --name YOUR_KIND_CLUSTER
kubectl --kubeconfig "$KUBECONFIG" apply -k config/default
kubectl --kubeconfig "$KUBECONFIG" -n coupang-nodepool-system \
  rollout status deployment/coupang-nodepool-controller-manager --timeout=180s
```

Replace the path and cluster-name placeholders; do not use a production cluster.
The default manifest references `coupang-nodepool:dev`; setting `IMG` alone does
not rewrite the Deployment. Rebuilding a mutable tag also does not restart an
existing Pod: use an explicit image update/rollout and verify the running image ID.
For non-kind runtimes, import/publish the image through that runtime's normal path.
`config/lab` is an optional offline control-plane-placement overlay using
`coupang-nodepool:final-20261011`; import that tag before using it.

Create a pool using [the sample](config/samples/nodes_v1alpha1_nodepool.yaml),
review its settings/policy, then enroll only an intended worker:

```bash
kubectl --kubeconfig "$KUBECONFIG" create -f config/samples/nodes_v1alpha1_nodepool.yaml
# Replace both placeholders with the chosen worker and pool:
kubectl --kubeconfig "$KUBECONFIG" label node WORKER_NAME nodes.example.com/nodepool=POOL_NAME
kubectl --kubeconfig "$KUBECONFIG" get nodepools -o yaml
kubectl --kubeconfig "$KUBECONFIG" get nodes -L nodes.example.com/nodepool
```

Deleting a pool triggers eligible cleanup, not worker deletion. Do not strip
finalizers to hide cleanup failures. Remove workloads/pools responsibly before
uninstalling the controller; do not remove the CRD while cleanup remains.

## Validation environments

Both configurations use one control-plane and two workers on Kubernetes 1.37.0.
The controller runs on the control-plane, but only workers are enrolled in a
NodePool. Separate kubeconfigs isolate each target from the original cluster.

### Three Ubuntu VMs with Multipass: non-disruptive validation

Multipass on Windows uses Hyper-V to run three separate Ubuntu guests with their
own kernels, disks and services. Kubernetes/containerd and Flannel were installed
inside the guests; Multipass alone does not supply a Kubernetes cluster.

```text
Windows host (all blocks below run on this host)

+---------------------------+       +---------------------------+
| Ubuntu WSL2               |       | Windows TCP relay         |
| Project, Go, kubectl      |------>| Kubernetes API forwarding |
| Separate VM kubeconfig    |       | TLS/auth preserved        |
+---------------------------+       +-------------+-------------+
                                                  |
                                                  v
+---------------------------------------------------------------+
| Multipass / Hyper-V: three separate Ubuntu VMs                |
| Hyper-V virtual network; Flannel for Pod networking           |
|                                                               |
|  +---------------------------------------------------------+  |
|  | Control-plane VM                                        |  |
|  | API server, etcd, scheduler, Kubernetes controllers     |  |
|  | NodePool manager (not a pool member)                    |  |
|  +---------------------------------------------------------+  |
|                ^                            ^                 |
|                |       Kubernetes API       |                 |
|                v                            v                 |
|  +--------------------------+  +--------------------------+   |
|  | Worker-1 VM              |  | Worker-2 VM              |   |
|  | kubelet + containerd     |  | kubelet + containerd     |   |
|  | Demo Pod; pool member    |  | Demo Pod; pool member    |   |
|  +--------------------------+  +--------------------------+   |
+---------------------------------------------------------------+

+---------------------------------------------------------------+
| Docker Desktop / original kind cluster: separate, preserved   |
+---------------------------------------------------------------+
```

The VM lab uses Ubuntu 24.04.5, containerd 2.2.6 and Flannel; each VM was allocated
2 vCPUs, 4 GiB RAM and 25 GiB virtual disk. The relay forwards only the Kubernetes
API connection across the local virtual networks; it does not imply access to all
Pod/Service ports. Component traffic and Pod networking are simplified in the
diagram and are not one-way.

### Three-node kind: disposable failure testing

Docker containers act as Kubernetes Nodes and share the Docker engine's Linux
kernel/resources; these are not three independent VMs.

```text
Windows host (all blocks below run on this host)

+---------------------------------------------------------------+
| Ubuntu WSL2                                                   |
| Project, Go, kind, kubectl; separate disposable kubeconfig    |
+---------------------------------------------------------------+
                 |                            |
           kind creates Nodes           kubectl uses API
                 |                            |
                 v                            v
+---------------------------------------------------------------+
| Docker Desktop Linux engine: disposable three-node kind       |
| Shared Linux kernel and Docker resource pool                  |
|                                                               |
|  +---------------------------------------------------------+  |
|  | Control-plane container                                 |  |
|  | API server, etcd, scheduler, Kubernetes controllers     |  |
|  | NodePool manager (not a pool member)                    |  |
|  +---------------------------------------------------------+  |
|                ^                            ^                 |
|                |       Kubernetes API       |                 |
|                v                            v                 |
|  +--------------------------+  +--------------------------+   |
|  | Worker container         |  | Worker2 container        |   |
|  | kubelet + containerd     |  | kubelet + containerd     |   |
|  | Test Pod; pool member    |  | Test Pod; pool member    |   |
|  +--------------------------+  +--------------------------+   |
+---------------------------------------------------------------+

+---------------------------------------------------------------+
| Original kind cluster: separate containers, same engine       |
| Preserved; not targeted by the disposable-cluster tests       |
+---------------------------------------------------------------+
```

The manual cluster was `coupang-nodepool-manual`; the automated runner uses
`coupang-nodepool-e2e`. Worker kubelet failure, PDB blocking, timeout and Node
removal are confined to these disposable environments, with cleanup afterward.
Container-based testing exercises real Kubernetes APIs and controllers but does
not reproduce independent-machine boot, kernel or hardware failure behavior.
Arrows show administration/API connections, not every Pod-network traffic path.

| Environment | Why it was used | Boundary |
|---|---|---|
| Three Multipass VMs | Settings, scheduling, ownership, membership and finalizer checks on independent guests | No VM kubelet stop or Node removal; no application-networking or storage-failover claim |
| Three-node kind | Repeatable, disposable worker-failure and removal experiments | Shared host/kernel; Node deletion is not container shutdown |

## Test scenarios and submitted evidence

Evidence was captured on October 11, 2026 (Pacific); the folder
[`DemoRun/20261012T015458Z/`](DemoRun/20261012T015458Z/) uses UTC.
The grouped file ranges below cover all 97 manual checkpoints; file 98 records
the fresh local automated run. [Evidence notes and hashes](DemoRun/README.md)
describe provenance, reproduction and limitations.

### Manual validation: three Ubuntu VMs

One control-plane and two workers, Kubernetes 1.37.0, with the manager on the control-plane and pool membership limited to workers.
Ignore policy plus a NoSchedule taint; no VM kubelet was stopped and no VM Node was removed.

| Grouped scenario | Observed result | Evidence |
|---|---|---|
| Health, settings and scheduling boundary | PASS: three Ready Nodes, healthy Pods, one workload Pod per worker; untolerated Pod rejected and cleaned up | 01-10: [Nodes](DemoRun/20261012T015458Z/01-nodes.txt), [settings](DemoRun/20261012T015458Z/05-node-taints.txt), [rejection](DemoRun/20261012T015458Z/07-untolerated-description.txt), [PDB](DemoRun/20261012T015458Z/10-pdb-baseline.yaml) |
| Membership leave/rejoin | PASS: owned settings/receipt removed and restored; original Pod UIDs unchanged | 11-20: [cleanup](DemoRun/20261012T015458Z/14-settings-after-leave.txt), [receipt](DemoRun/20261012T015458Z/19-settings-after-rejoin.txt), [Pods](DemoRun/20261012T015458Z/20-workloads-after-rejoin.txt) |
| Spec update and restoration | PASS: both workers converge, generation acknowledgement advances, original workloads remain | 21-29: [updated pool](DemoRun/20261012T015458Z/24-pool-after-spec-change.yaml), [Pods](DemoRun/20261012T015458Z/25-workloads-after-spec-change.txt), [restoration](DemoRun/20261012T015458Z/29-restored-pool.txt) |
| Pool deletion, ownership preservation and recreation | PASS: owned settings cleaned, external label/Nodes/Pods preserved; new pool UID recorded in receipts | 30-40: [external label](DemoRun/20261012T015458Z/32-nodes-after-pool-deletion.txt), [cleaned receipts](DemoRun/20261012T015458Z/33-settings-after-pool-deletion.txt), [new pool](DemoRun/20261012T015458Z/38-recreated-pool.yaml), [new receipts](DemoRun/20261012T015458Z/39-recreated-settings.txt) |

### Manual validation: three disposable kind containers

One control-plane and two workers, Kubernetes 1.37.0, with a separate kubeconfig and verified local controller image.
Remove policy: 15s grace, 120s drain timeout, one removal slot; two Pods with minAvailable=2 PDB and indefinite native health-taint tolerations.

| Grouped scenario | Observed result | Evidence |
|---|---|---|
| Cluster, image, deployment and baseline | PASS: healthy Nodes/controller, verified imported image, two worker Pods, zero allowed PDB disruptions | 41-60: [Nodes](DemoRun/20261012T015458Z/44-manual-nodes.txt), [image](DemoRun/20261012T015458Z/52-imported-image-details.txt), [Pod UIDs](DemoRun/20261012T015458Z/58-manual-pod-baseline.txt), [PDB](DemoRun/20261012T015458Z/59-manual-pdb-baseline.yaml) |
| Worker failure and PDB enforcement | PASS: target worker cordoned; API refuses eviction; original Pods remain non-terminating | 61-65: [blocked pool](DemoRun/20261012T015458Z/63-blocked-pool.yaml), [Nodes](DemoRun/20261012T015458Z/64-blocked-nodes.txt), [Pods](DemoRun/20261012T015458Z/65-blocked-pods.txt) |
| Timeout and manager restart | PASS: no forced deletion; original failure/drain clocks and Pod identities survive restart | 66-73: [before](DemoRun/20261012T015458Z/67-timeout-before-restart.yaml), [after](DemoRun/20261012T015458Z/70-timeout-after-restart.yaml), [retained Pods](DemoRun/20261012T015458Z/72-pods-after-timeout.txt) |
| Worker recovery | PASS: Ready restored, owned cordon/clocks/failure cleared, original Pods retained | 74-80: [Nodes](DemoRun/20261012T015458Z/78-recovered-nodes.txt), [pool](DemoRun/20261012T015458Z/79-recovered-pool.yaml), [Pods](DemoRun/20261012T015458Z/80-recovered-pods.txt) |
| Empty-worker removal | PASS: after explicit healthy-workload teardown, second failure produces controller-driven Node removal | 81-88: [teardown](DemoRun/20261012T015458Z/81-workload-teardown.txt), [remaining Nodes](DemoRun/20261012T015458Z/86-nodes-after-removal.txt), [Events](DemoRun/20261012T015458Z/88-removal-events.txt) |
| Finalization and cleanup | PASS: survivor settings cleaned; disposable cluster removed; original cluster/context and VM workloads preserved | 89-97: [worker cleanup](DemoRun/20261012T015458Z/90-surviving-worker-cleanup.yaml), [cluster cleanup](DemoRun/20261012T015458Z/93-manual-cluster-cleanup.txt), [VM Nodes](DemoRun/20261012T015458Z/96-preserved-vm-nodes.txt), [VM Pods](DemoRun/20261012T015458Z/97-preserved-vm-workloads.txt) |

### Automated validation

The unit/API output uses Go 1.27.2, race detection, shuffled test order and isolated Kubernetes 1.37.0 API assets.
A separate live E2E run deploys the actual controller in three-node kind; its logs supplement, rather than replace, the original manual evidence.

| Grouped checks | Submitted result / scope | Evidence or entry point |
|---|---|---|
| Build, formatting and static analysis | PASS: static manager build, gofmt check and go vet | [98-local-automated-validation.txt](DemoRun/20261012T015458Z/98-local-automated-validation.txt) |
| Unit and real isolated-API tests | PASS: 62 unit functions, 175 named subtests, 21 API specs; no race failure. Ownership/protection, member isolation, clocks, lost responses, real eviction preconditions, reservations and watches | [Same complete log](DemoRun/20261012T015458Z/98-local-automated-validation.txt) |
| Live automated E2E | PASS: 1/1 scenario, 130.60s test time; actual RBAC, settings, PDB blocking, persisted clocks/restart, recovery, accepted eviction, CRI-confirmed termination, Node retention/removal and cleanup | [Run notes](E2ERun/README.md), [test output](E2ERun/20261012T031801Z/tests.log), [complete runner/cleanup output](E2ERun/20261012T031801Z/runner.log) |

**What the evidence demonstrates**

- **Manual removal:** the workload was explicitly removed first. This proves
  removal of an empty worker, not draining a worker with running workloads.
- **Automated E2E:** verifies accepted Pod eviction and container termination,
  confirms the Node remains while Pod deletion is held, and observes Node
  removal only after the original Pod disappears.

**What these tests do not guarantee**

- **Production-wide coverage:** these local scenarios do not establish storage
  failover, high availability (HA), large-scale behavior or application networking.
- **Protection from every disruption:** PodDisruptionBudgets (PDBs) limit
  supported voluntary evictions; they do not prevent machine failures or every
  other source of disruption.
- **Machine shutdown or isolation:** deleting a Kubernetes Node record does not
  power off or fence the underlying machine.

## Eight design decisions

### 1. Label/taint ownership

> **Q1. How does your controller distinguish labels/taints it applied from ones
> applied by other actors?**
>
> **A1.**
> - **Tracking ownership:** the controller records the pool's unique ID (UID)
>   and the label/taint values it writes in the Node annotation
>   `nodes.example.com/managed-settings`. Each taint is identified by its
>   `(key, effect)` pair.
> - **Update flow:** the controller compares the pool's requested settings with
>   the Node's current settings and the recorded ownership. It updates the Node
>   settings and ownership record in a single patch, using a resource-version
>   check to reject the write if the Node changed in the meantime.
> - **Handling conflicts:** if a requested setting conflicts with an existing
>   value the controller does not own, it reports the conflict rather than
>   overwriting that value.
> - **Cleanup flow:** the controller removes a setting only when it owns that
>   setting and the current value still matches its recorded value. Values
>   changed by another actor are preserved.
> - **When ownership data is missing:** the controller cannot safely infer
>   which settings it previously wrote. References to former members identify
>   Nodes that left the pool while cleanup was pending, but do not tell it which
>   label or taint values it previously owned.
>
> **Q2. What happens when a human sets a matching label manually?**
>
> **A2.**
> - **When the label already exists:** if the value matches but the controller
>   does not own it, the controller leaves it unowned and preserves it during
>   cleanup.
> - **When a human rewrites an owned label:** writing the same value leaves no
>   detectable difference, so the existing ownership record remains.
> - **What the record cannot prove:** it tracks cooperative ownership, not who
>   last wrote an identical value. It is not a security boundary.

### 2. Reconciliation triggers

> **Q1. What Watches does your controller set up?**
>
> **A1.**
> - **Triggering a check:** the controller watches NodePool, Node and Pod changes
>   and queues the relevant pool for reconciliation: comparing the requested
>   configuration with the current cluster state and applying any needed changes.
> - **Scheduled checks:** after a successful pass, it checks again in ten seconds
>   or sooner when grace expires. This revisits deadlines and PDB changes without
>   watching PDBs directly.
> - **Retry flow:** controller-runtime schedules retries after errors. Each retry
>   reads the current state instead of assuming earlier events are still accurate.
>
> **Q2. How does the controller find the owning NodePool from a Node event?**
>
> **A2.**
> - **Finding the pool:** the controller reads the pool name from the Node's
>   `nodes.example.com/nodepool` label. For example, a value of `gpu-pool` queues
>   that NodePool. NodePools belong to the whole cluster rather than a namespace,
>   so the reconcile request contains the pool name without a namespace.
> - **Handling a pool change:** it checks both the old and new membership labels,
>   allowing both pools to reconcile when a Node moves.
> - **Following a Pod event:** for a Pod assigned to a Node, it looks up that Node
>   and uses the same membership label to find the pool.

### 3. Grace-period clock

> **Q1. Where does the "how long has this node been NotReady" timer live:
> status field, Node condition timestamp, or in-memory?**
>
> **A1.**
> - **Storing the clock:** each Node UID has an entry in the NodePool's
>   `status.nodes`. This is stored in Kubernetes, not just in process memory.
> - **Recording failure time:** `notReadySince` records the start of the observed
>   unhealthy interval. The controller uses it to decide when grace has expired.
> - **Recording related timestamps:** `readyTransitionTime` tracks the last
>   observed Ready-condition transition; `drainStartedAt` records when a removal
>   slot was reserved and starts the drain-timeout clock.
> - **Starting the grace period:** timing starts when the controller observes
>   the failure, not at an earlier kubelet timestamp. The Node condition timestamp
>   helps detect changes but is not the sole clock.
> - **Preserving the deadline:** timestamps round up to whole seconds so saving
>   them does not shorten the waiting period. Recovery does not depend on an
>   in-memory timer.
>
> **Q2. Which survives a controller restart correctly?**
>
> **A2.**
> - **Restart flow:** the controller reloads the saved status. A restart alone
>   does not reset the grace period or drain deadline.
> - **Handling uncertain transitions:** changed or ambiguous Ready transition
>   information can mean the Node recovered between checks. The controller
>   conservatively starts fresh grace rather than assuming continuous failure.
>   An unresolved removal retains its original drain deadline and slot, so this
>   uncertainty does not grant unlimited new drain time or removal capacity.
> - **Handling recovery or replacement:** confirmed Ready recovery cancels removal
>   and clears the clocks. A replacement Node UID starts with independent state.
> - **What the clock cannot capture:** current-state APIs cannot reveal every
>   recovery that happened between observations. System-clock adjustments also
>   remain a limitation.

### 4. Ready=False vs Ready=Unknown

> **Q1. Do you treat these differently?**
>
> **A1.**
> - **Handling either condition:** the controller treats `False`, `Unknown` and
>   a missing Ready condition as NotReady, using the same grace period and
>   disruption checks.
> - **Continuing settings updates:** worker labels and taints are synchronized
>   independently of readiness.
>
> **Q2. Why or why not?**
>
> **A2.**
> - **Reading the signal:** `False` reports an unhealthy Node; `Unknown` commonly
>   means Kubernetes has stopped receiving its heartbeats, the periodic health
>   reports sent by the kubelet on that Node.
> - **Choosing the safe response:** neither condition proves the machine is
>   powered off, so neither allows the controller to bypass removal safeguards.

### 5. Force-delete safety

> **Q1. After eviction times out, do you force-delete pods?**
>
> **A1.**
> - **Timeout flow:** no force deletion is performed. The controller stops new
>   evictions and reports `DrainTimedOut` while ordinary Pods remain. Eviction is
>   a request for Kubernetes to gracefully remove a Pod, not proof that its
>   process has already stopped.
> - **Resuming progress:** if the blockers later disappear, removal can finish
>   without issuing new eviction requests after the deadline.
>
> **Q2. What's the risk?**
>
> **A2.**
> - **Losing track of a running process:** deleting a Pod's API record does not
>   prove that its process stopped on an unreachable machine.
> - **Running two copies:** a replacement could start while the old process is
>   still active. Both could believe they are the active instance and write data
>   independently (split-brain), causing conflicting writes or data loss.
> - **Reusing storage too early:** deleting a Node record does not isolate
>   the machine (fence it) or prove its attached storage is safe to reuse.
>
> **Q3. What guardrails do you add?**
>
> **A3.**
> - **Eviction flow:** use `policy/v1` eviction with checks on the Pod's UID and
>   resource version, so the request targets the observed Pod rather than a
>   replacement or changed object. Never override its shutdown grace period or
>   directly delete it. Manager permissions (RBAC) also exclude Pod patch/delete.
> - **Respecting disruption limits:** a PodDisruptionBudget (PDB) can reject an
>   eviction to protect workload availability. Rejection or API request throttling
>   produces `EvictionBlocked`; the controller does not bypass the restriction.
> - **Stopping for unsafe workloads:** Pods without a controller owner or owner
>   UID block draining. So do `emptyDir`/`hostPath` local volumes and DaemonSet
>   references whose owner is missing or has a different UID.
> - **Skipping excluded workloads:** completed Pods (`Succeeded`/`Failed`) with
>   no deletion timestamp do not block draining. Neither do mirror Pods, which
>   represent kubelet-managed static Pods, or Pods owned by a matching live
>   DaemonSet.
> - **Waiting before Node deletion:** ordinary terminating Pods retain the Node
>   until those Pods disappear, even after their phase becomes terminal.
>   Finalizers are deletion holds for required cleanup; the controller never
>   strips Pod or Node finalizers to force progress.
> - **Recognizing remaining gaps:** ordinary owner references do not prove an
>   owner still exists or can safely recreate a workload. The controller does not
>   inspect persistent-volume claims (PVCs), persistent volumes (PVs) or storage
>   driver (CSI) capabilities to establish safe relocation, and does not fence
>   machines. Unsupported workloads can still pass its checks and receive
>   eviction; broader enforcement is deferred.

### 6. Failure modes

> **Q1. If the controller crashes mid-removal, what state does it come back to?**
>
> **A1.**
> - **Saving progress before acting:** record Draining and its removal reservation
>   before cordoning (blocking new scheduling) or evicting Pods. Record Removing
>   after drain checks pass and before requesting Node deletion.
> - **Restart flow:** read the saved phase and timestamps alongside fresh Node
>   and Pod state, rather than starting the removal clock again.
>
> **Q2. Does it resume correctly?**
>
> **A2.**
> - **Retry flow:** reread current state from the API, avoid re-evicting terminating
>   Pods and tolerate actions that already completed. Node deletion checks the
>   UID and resource version to avoid deleting a replaced or changed Node.
> - **Coordinating controller instances:** the Deployment enables leader election
>   so instances can select an active manager. Do not run an independent
>   manager without compatible election settings.
> - **Cancellation flow:** recovery, Ignore or leaving the pool stops further
>   removal and releases only the controller-owned scheduling block (cordon).
>   Cordon policy stops removal but keeps the Node cordoned.
> - **What recovery cannot undo:** an accepted eviction or deletion cannot be
>   reversed.

### 7. Concurrency

> **Q1. Two NodePools accidentally match the same Node (the bootstrap label was
> set twice, or matching is looser than a single label). What happens?**
>
> **A1.**
> - **Assigning membership:** this implementation uses one exact label value,
>   not overlapping selectors. The label cannot hold two pool names at once;
>   changing its value moves membership to the new pool.
> - **Handoff flow:** pool UID records prevent silent takeover during a move or
>   recreation. The old pool cleans its owned settings, while the new pool retries
>   conflicts. Handoff stays blocked if the old pool cannot complete cleanup.
> - **Managing removal slots:** active removals keep their reserved slots even
>   when blocked. A slot counts against the pool's `maxConcurrentRemovals` limit,
>   preventing another removal from replacing a still-active blocked operation.
>   Lowering the limit does not cancel existing reservations.
> - **Choosing the next removal:** prefer the oldest observed failure and break
>   ties deterministically.
> - **Handling a member failure:** other independent members can continue
>   sequentially. A member with a known reconciliation failure receives no new
>   slot, but keeps any existing active reservation.
> - **Handling a shared failure:** pool authorization, listing or status-write
>   failures stop shared work. Failed and Progressing can both be True; actual
>   member errors prevent top-level `observedGeneration` from advancing. That
>   field identifies the configuration revision successfully processed, so a
>   partial failure must not acknowledge a new revision as fully processed.
> - **Limits of concurrent checks:** fresh reads and version-checked writes do
>   not make changes across multiple objects atomic. Privileged edits or direct
>   Pod binding can still occur after the last check.

### 8. Composition with kube-controller-manager

> **Q1. node-lifecycle-controller already taints NotReady nodes with
> `node.kubernetes.io/unreachable:NoExecute` and evicts pods. What does your
> controller add on top?**
>
> **A1.**
> - **Leaving native behavior in place:** Kubernetes continues to monitor health,
>   manage health taints, recreate workloads and perform garbage collection.
> - **Adding pool-level control:** this controller manages setting ownership,
>   failure policies, grace periods and saved removal limits. Its voluntary
>   drain flow respects PDBs and the documented safety checks.
> - **Keeping eviction paths separate:** native taint eviction is independent.
>   A NoExecute taint can cause Kubernetes to evict Pods that do not tolerate it.
>   These taints remain supported on workers even under Ignore, which disables
>   this controller's unhealthy-node actions, not settings synchronization.
>   Native taint eviction is not bounded by this controller's PDB checks or clocks.
> - **Protecting control-plane Nodes:** control-plane/master role labels prevent
>   desired settings and lifecycle disruption, even when the Node is Ready.
> - **Cleanup flow for protected Nodes:** valid owned cleanup requires Ready=True.
>   Old ownership records that claim control-plane/master role labels need
>   manual repair rather than automatic role-label removal. Protection depends
>   on those labels and cannot prevent a privileged administrator from changing
>   them.

## Limitations and next work

- Workers and known restart-tolerant workloads are the supported demonstration
  scope. Broader owner/storage enforcement is deferred; unsupported workloads
  are not necessarily prevented from eviction.
- Node removal does not stop/fence infrastructure or prevent kubelet re-registration.
  Provisioning/replacement belongs to a separate infrastructure controller.
- Unready Nodes skipped during pool-deletion cleanup may retain receipts/settings;
  later recovery needs deliberate operator-reviewed cleanup.
- Ready describes observed health, not authorization or completed removal.
  `observedGeneration` acknowledges successful spec processing; known blockers
  can coexist with equality. Events are best-effort, not atomic with status.
- Settings scans/direct API reads favor clarity; no production-scale load/latency
  benchmark, exhaustive multi-controller crash proof or multi-version matrix is claimed.
- With more time: explicit owner/storage policy, HA/leader-handoff tests, broader
  PDB/termination/policy-transition matrices, receipt fuzzing, API-load benchmarks
  and version compatibility coverage.

## Source and delivery map

Paths are relative to the repository root. Click a file or directory to inspect it.

| What to review | File / directory | What it contains |
|---|---|---|
| Go/Kubebuilder source | [api/](api/), [cmd/](cmd/), [internal/](internal/), [PROJECT](PROJECT), [go.mod](go.mod), [go.sum](go.sum) | API types, controller entry point, reconciliation logic and project dependencies. |
| CRD manifest | [config/crd/bases/nodes.example.com_nodepools.yaml](config/crd/bases/nodes.example.com_nodepools.yaml) | NodePool fields, validation rules, cluster scope and the `/status` subresource. |
| Controller permissions | [config/rbac/role.yaml](config/rbac/role.yaml) | Allowed operations on Nodes, Pods, evictions, NodePools and Events. |
| Permission binding | [config/rbac/role_binding.yaml](config/rbac/role_binding.yaml), [service_account.yaml](config/rbac/service_account.yaml) | The controller's identity and its connection to the controller permissions. |
| Controller Deployment | [config/manager/manager.yaml](config/manager/manager.yaml) | Container configuration, leader election, health checks, resource limits and security settings. |
| Deployment assembly | [config/default/kustomization.yaml](config/default/kustomization.yaml) | Combines the CRD, RBAC and Deployment with the final namespace and name prefix. |
| Demo entry point | [hack/demo.sh](hack/demo.sh) | Starts the reproducible demonstration by calling the E2E runner. |
| Demo setup and cleanup | [hack/e2e.sh](hack/e2e.sh), [config/kind/e2e.yaml](config/kind/e2e.yaml) | Creates isolated three-node kind, builds/deploys the controller, runs the scenario and cleans up. |
| Actual demo scenario | [test/e2e/e2e_test.go](test/e2e/e2e_test.go) | Numbered steps and assertions for pool creation, joins, settings, failure, eviction/removal and pool deletion. |
| Unit/API tests and CI | [internal/controller/](internal/controller/), [.github/workflows/](.github/workflows/), [CI.md](CI.md) | Unit and isolated-API tests in `*_test.go` files, plus workflows for static checks, unit/API tests and live E2E. |
| Build/run and design documentation | [README.md](README.md) | Setup commands, all eight design answers, safety boundaries and limitations. |
| Recorded manual/local test evidence | [DemoRun/README.md](DemoRun/README.md) | Manual VM/kind checkpoints and local automated-test output, with integrity checksums. |
| Recorded live E2E evidence | [E2ERun/README.md](E2ERun/README.md) | Test and cleanup logs, cluster diagnostics, versions and tested-source checksums. |

The runtime uses controller-runtime v0.25.0 and Kubernetes libraries v0.37.0;
tests use Go testing, Ginkgo, Gomega and envtest. The controller calls the
eviction API directly; it does not embed `kubectl drain`.

Framework references: [Kubebuilder](https://book.kubebuilder.io/),
[controller-runtime](https://pkg.go.dev/sigs.k8s.io/controller-runtime),
[Kubernetes eviction API](https://kubernetes.io/docs/concepts/scheduling-eviction/api-eviction/)
and [conditions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties).
Kubebuilder supplies the project scaffolding; lifecycle and settings logic are
implemented here, not delegated to an existing NodePool operator.
