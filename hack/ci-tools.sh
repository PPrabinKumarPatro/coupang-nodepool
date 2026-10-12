#!/usr/bin/env bash
set -euo pipefail
[[ "${GITHUB_ACTIONS:-}" == true ]] || { echo 'This installer is only for GitHub Actions runners.' >&2; exit 2; }
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$root/hack/versions.env"
[[ "$(uname -s)" == Linux && "$(uname -m)" == x86_64 ]] || { echo 'Linux amd64 runner required.' >&2; exit 2; }
dest="$RUNNER_TEMP/nodepool-tools"
mkdir -p "$dest"
cd "$dest"
curl --fail --location --retry 3 -o kind-linux-amd64 "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-linux-amd64"
printf '%s  kind-linux-amd64\n' "$KIND_SHA256" | sha256sum --check
mv kind-linux-amd64 kind
curl --fail --location --retry 3 -o kubectl "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl"
printf '%s  kubectl\n' "$KUBECTL_SHA256" | sha256sum --check
chmod 755 kind kubectl
printf '%s\n' "$dest" >> "$GITHUB_PATH"
