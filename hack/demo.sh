#!/usr/bin/env bash
set -euo pipefail
# The asserted E2E sequence is also the narrated demo; one implementation avoids drift.
# This is disposable-QA-only, not a deployment entry point for an existing lab.
# It checks original Pod UIDs, then an allowing-PDB live-kubelet eviction with
# bounded readiness injection and an observation finalizer (never workload deletion).
# Kubernetes v1 ValidatingAdmissionPolicy and real CRI termination evidence are required.
exec bash "$(dirname "${BASH_SOURCE[0]}")/e2e.sh"
