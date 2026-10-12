#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
source hack/versions.env
[[ "${COUPANG_ALLOW_DISRUPTIVE_E2E:-}" == yes ]] || {
  echo 'Set COUPANG_ALLOW_DISRUPTIVE_E2E=yes to allow disposable QA worker disruption.' >&2
  exit 2
}
cluster=coupang-nodepool-e2e
image=coupang-nodepool:dev
artifacts="$root/artifacts/e2e"
mkdir -p "$artifacts"
for tool in docker kind kubectl go python3; do
  command -v "$tool" >/dev/null || { printf 'Required tool missing: %s\n' "$tool" >&2; exit 2; }
done
clusters="$(kind get clusters)"
if /usr/bin/grep -Fxq "$cluster" <<<"$clusters"; then
  echo 'QA cluster already exists; refusing to reuse or delete an unowned cluster.' >&2
  exit 2
fi
if ! docker image inspect "$KIND_NODE_IMAGE" >/dev/null 2>&1; then
  [[ "${COUPANG_ALLOW_IMAGE_PULL:-}" == yes ]] || {
    echo 'Pinned kind image is not cached. Pull it explicitly or approve COUPANG_ALLOW_IMAGE_PULL=yes.' >&2
    exit 2
  }
  docker pull "$KIND_NODE_IMAGE"
fi
go test -mod=readonly -tags=e2e ./test/e2e -run '^$'
make docker-build IMG="$image"
private="$artifacts/private-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
(umask 077; mkdir -- "$private")
export KUBECONFIG="$private/kubeconfig"
export E2E_KUBECONFIG="$KUBECONFIG"
export E2E_KIND_CLUSTER="$cluster"
export E2E_WORKER="$cluster-worker"
created=no
create_started=no
control_plane_id=
cleanup() {
  status=$?
  trap - EXIT
  # A failed kind create may leave containers behind. The shared name and kind
  # labels alone cannot establish ownership, so never infer an ID after failure.
  if [[ "$create_started" == yes && ( "$created" != yes || -z "$control_plane_id" ) ]]; then
    echo 'kind creation did not establish ownership; partial QA resources may remain.' >&2
    echo "No automatic deletion attempted. Manually inspect only $cluster; preserve all other clusters." >&2
    echo "Private kubeconfig, if created, retained at $KUBECONFIG (directory mode 700)." >&2
    [[ ! -f "$KUBECONFIG" ]] || chmod 600 "$KUBECONFIG"
    exit 1
  fi
  if [[ "$created" == yes ]]; then
    current_id="$(docker inspect --format '{{.Id}}' "$cluster-control-plane" 2>/dev/null)" || current_id=
    if [[ -z "$control_plane_id" || "$current_id" != "$control_plane_id" ]]; then
      echo 'QA cleanup refused: cannot confirm this run still owns the control-plane container.' >&2
      echo "Private kubeconfig retained for manual recovery: $KUBECONFIG" >&2
      exit 1
    fi
    for resource in nodes pods; do
      kubectl --request-timeout=20s --kubeconfig "$KUBECONFIG" get "$resource" -A \
        >"$artifacts/$resource.txt" 2>&1 || echo "Diagnostic collection failed: $resource" >&2
    done
    kubectl --request-timeout=20s --kubeconfig "$KUBECONFIG" get nodepools -o yaml \
      >"$artifacts/pools.yaml" 2>&1 || echo 'Diagnostic collection failed: NodePools' >&2
    kubectl --request-timeout=20s --kubeconfig "$KUBECONFIG" get events -A \
      >"$artifacts/events.txt" 2>&1 || echo 'Diagnostic collection failed: Events' >&2
    kubectl --request-timeout=20s --kubeconfig "$KUBECONFIG" logs deployment/coupang-nodepool-controller-manager \
      -n coupang-nodepool-system --tail=500 >"$artifacts/manager.log" 2>&1 || echo 'Diagnostic collection failed: manager logs' >&2
    current_id="$(docker inspect --format '{{.Id}}' "$cluster-control-plane" 2>/dev/null)" || current_id=
    if [[ "$current_id" != "$control_plane_id" ]]; then
      echo 'QA cleanup refused: control-plane identity changed during diagnostics.' >&2
      echo "Private kubeconfig retained for manual recovery: $KUBECONFIG" >&2
      exit 1
    elif ! kind delete cluster --name "$cluster"; then
      echo "QA cleanup failed; inspect only $cluster. Private kubeconfig retained: $KUBECONFIG" >&2
      exit 1
    fi
  fi
  if ! rm -f -- "$KUBECONFIG"; then
    echo 'Private kubeconfig cleanup failed.' >&2
    status=1
  fi
  rmdir "$private" || { echo 'Private directory cleanup failed.' >&2; status=1; }
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
create_started=yes
kind create cluster --name "$cluster" --image "$KIND_NODE_IMAGE" --config config/kind/e2e.yaml \
  --kubeconfig "$KUBECONFIG" --wait 180s
created=yes
control_plane_id="$(docker inspect --format '{{.Id}}' "$cluster-control-plane")"
chmod 600 "$KUBECONFIG"
[[ "$(kubectl --kubeconfig "$KUBECONFIG" config current-context)" == "kind-$cluster" ]]
docker update --cpus 2 --memory 3g --memory-swap 3g "$cluster-control-plane" >/dev/null
for node in "$cluster-worker" "$cluster-worker2"; do
  docker update --cpus 2 --memory 2g --memory-swap 2g "$node" >/dev/null
done
kubectl --kubeconfig "$KUBECONFIG" wait nodes --all --for=condition=Ready --timeout=120s
kubectl --kubeconfig "$KUBECONFIG" wait pods -n kube-system --all --for=condition=Ready --timeout=120s
kubectl --kubeconfig "$KUBECONFIG" version -o json >"$artifacts/versions.json"
# Use the pause image already embedded in this exact kind node image; never pull a workload image.
export E2E_PAUSE_IMAGE
E2E_PAUSE_IMAGE="$(docker exec "$E2E_WORKER" crictl images -o json | python3 -c '
import json,sys
images=json.load(sys.stdin)["images"]
matches=sorted({tag for image in images for tag in image.get("repoTags",[]) if tag.startswith("registry.k8s.io/pause:")})
if len(matches)!=1: raise SystemExit("Expected one cached pause image; refusing an implicit pull")
print(matches[0])
')"
kind load docker-image "$image" --name "$cluster"
kubectl --kubeconfig "$KUBECONFIG" apply -k config/e2e
kubectl --kubeconfig "$KUBECONFIG" rollout status deployment/coupang-nodepool-controller-manager \
  -n coupang-nodepool-system --timeout=180s
printf '%s\n' \
  'Evidence includes original Pod UIDs at PDB-blocked/deadline checkpoints and a separate allowing-PDB live-kubelet eviction.' \
  'The latter requires v1 ValidatingAdmissionPolicy: a temporary old-Node-UID rule holds Ready=False, not kubelet execution.' \
  'A finalizer holds the already-evicted Pod while terminal kubelet status + CRI and Node retention are checked; then it is released.' \
  'Unsupported admission or missing runtime evidence fails the test; it never falls back to deleting workloads.'
go test -mod=readonly -tags=e2e ./test/e2e -count=1 -timeout=15m -v -ginkgo.v -ginkgo.no-color 2>&1 | tee "$artifacts/tests.log"
