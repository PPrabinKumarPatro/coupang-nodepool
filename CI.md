# Continuous integration

The assignment asks for unit and E2E tests that validate the controller in CI.
Three workflows are supplied for pushes, pull requests and manual dispatch.
They use Linux amd64 Ubuntu 24.04 runners, pinned tool versions and SHA-pinned
actions.

**Remote status: NOT RUN.** GitHub Actions is intentionally disabled in this
repository for the submission. The workflows remain available for a later run.
Local success does not establish that GitHub runner downloads, permissions or
resource limits work.

| Workflow | Checks | Evidence |
|---|---|---|
| Formatting and static checks | gofmt, go vet, tagged E2E compilation and shell syntax | Actions step output |
| Unit and API integration tests | Dependency verification, generated-file drift, static build and race-enabled isolated API tests | `unit-test-evidence` artifact |
| Disposable kind E2E | Actual manager/RBAC, PDB blocking, restart/recovery, workload eviction/runtime termination, Node removal and cleanup | `e2e-nonsecret-evidence` artifact |

## Running remote CI later

1. If remote validation is desired, a repository administrator can enable
   GitHub Actions under **Settings -> Actions -> General**. Ensure repository
   policy permits the pinned actions; do not bypass an organizational restriction.
2. Keep workflow permissions at `contents: read`; broad write permissions,
   repository secrets and registry pushes are not required.
3. Open **Actions** and manually dispatch each of the three workflows against
   the intended branch. After Actions is enabled, subsequent pushes and pull
   requests also trigger them. Enabling Actions does not prove an earlier
   commit was tested.
4. Check that the E2E test actually ran (not just its compile-only check), that
   tests and cluster cleanup succeeded, and download the designated evidence.
   Record the commit SHA and run URLs; do not replace failed output with old logs.

No repository secrets, cloud account or external container registry are needed
by these workflows. The runner does need internet access for Go/tool/image
downloads and working Docker. Budget roughly 15-40 minutes including queues;
download, resource or test failures may require more time.

The E2E artifact step explicitly allowlists diagnostic files and never uploads
its private kubeconfig directory, including on partial-creation failure.
Artifacts expire after seven days; save the nonsecret evidence if required for
review. Shell steps use Bash pipefail so `tee` cannot hide failed test commands.
