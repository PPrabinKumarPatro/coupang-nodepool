#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$root/hack/versions.env"
if [[ -n "${KUBEBUILDER_ASSETS:-}" ]]; then
  for binary in kube-apiserver etcd kubectl; do
    [[ -x "$KUBEBUILDER_ASSETS/$binary" ]] || { printf 'Missing %s\n' "$binary" >&2; exit 2; }
  done
  [[ "$("$KUBEBUILDER_ASSETS/kube-apiserver" --version)" == "Kubernetes v$KUBERNETES_VERSION" ]]
  printf '%s\n' "$KUBEBUILDER_ASSETS"
  exit 0
fi
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || { echo 'This pinned asset helper supports Linux amd64.' >&2; exit 2; }
dest="${XDG_CACHE_HOME:-$HOME/.cache}/coupang-nodepool/envtest/$KUBERNETES_VERSION-linux-amd64"
if [[ -x "$dest/kube-apiserver" && -x "$dest/etcd" && -x "$dest/kubectl" ]]; then
  [[ "$("$dest/kube-apiserver" --version)" == "Kubernetes v$KUBERNETES_VERSION" ]]
  printf '%s\n' "$dest"
  exit 0
fi
[[ ! -e "$dest" ]] || { echo 'Incomplete envtest cache; inspect it before retrying. Existing files are not overwritten.' >&2; exit 2; }
parent="$(dirname "$dest")"
mkdir -p "$parent"
stage="$(mktemp -d "$parent/.stage.XXXXXXXX")"
archive="$(mktemp)"
cleanup() {
  status=$?
  trap - EXIT
  rm -f -- "$archive"
  if [[ -d "$stage" ]]; then
    rm -f -- "$stage/kube-apiserver" "$stage/etcd" "$stage/kubectl"
    rmdir "$stage" || { echo 'Asset staging cleanup failed.' >&2; status=1; }
  fi
  exit "$status"
}
trap cleanup EXIT
url="https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v$KUBERNETES_VERSION/envtest-v$KUBERNETES_VERSION-linux-amd64.tar.gz"
curl --fail --location --retry 3 --output "$archive" "$url" >&2
printf '%s  %s\n' "$ENVTEST_SHA512" "$archive" | sha512sum --check >&2
python3 - "$archive" "$stage" <<'PY'
import pathlib, sys, tarfile
dest = pathlib.Path(sys.argv[2])
required = {"kube-apiserver", "etcd", "kubectl"}
found = set()
with tarfile.open(sys.argv[1], "r:gz") as bundle:
    for entry in bundle:
        name = pathlib.PurePosixPath(entry.name)
        if name.is_absolute() or ".." in name.parts:
            raise SystemExit("Unsafe archive path")
        if name.name not in required:
            continue
        if not entry.isfile() or name.name in found:
            raise SystemExit("Unexpected binary entry")
        source = bundle.extractfile(entry)
        with source, (dest / name.name).open("xb") as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)
        (dest / name.name).chmod(0o755)
        found.add(name.name)
if found != required:
    raise SystemExit("Incomplete envtest bundle")
PY
[[ "$("$stage/kube-apiserver" --version)" == "Kubernetes v$KUBERNETES_VERSION" ]]
mv --no-clobber -T "$stage" "$dest"
[[ ! -d "$stage" ]] || { echo 'Another process created the envtest cache; refusing to overwrite it.' >&2; exit 2; }
printf '%s\n' "$dest"
