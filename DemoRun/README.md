# Recorded demonstration and test evidence

`20261012T015458Z/` contains the original user-run terminal outputs, captured on
October 11, 2026 Pacific time. The folder name is UTC. Files 01-97 are the manual
VM/container walkthrough; file 98 is the fresh local automated build/test run.
See the [compact scenario tables](../README.md#test-scenarios-and-submitted-evidence).

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
