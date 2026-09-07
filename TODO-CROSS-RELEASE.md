# Cross-release live matrix (deferred)

**Status: deferred.** Do not run the cross-release live matrix during the first
release. Resume only when the implementation is complete and a first genuine
published Muxe release is available as the predecessor. A synthetic predecessor,
a checked-out build, or a host-free fixture does not satisfy this condition.

The current release gate runs the target-only real-host smoke on each release
architecture. That gate remains required. The cross-release work below is saved
for the first published baseline; existing tools and tests remain in place for
that work and must not be deleted or treated as newly run.

## Resume prerequisites

Before enabling the matrix, confirm all of the following:

- A genuine predecessor release is published and downloadable for the tested
  architecture. Do not fabricate a published predecessor or use a synthetic
  published release.
- The target is the staged release-candidate archive produced by the current
  release workflow. It need not be published before verification. Do not
  substitute a synthetic old or target fixture for either archive.
- Use the actual predecessor and staged target archives. Record each archive's
  SHA-256, release provenance/attestation, and compatibility record before
  running the matrix.
- The run has explicit approval for owned live hosts. Every case uses a fresh
  owner-controlled `TempDir`, retained owned `Child` processes, explicit host
  endpoints, minimal permissions, and preserved diagnostics. It must never use a
  default user host, ambient socket, process-name cleanup, or global cleanup.
- The pinned Zellij 0.46.0 and Herdr 0.8.2 host inputs are available with their
  verified digests. The four release rows remain Linux x64 (musl), Linux arm64
  (musl), macOS Intel, and macOS arm64:
  `ubuntu-24.04`, `ubuntu-24.04-arm`, `macos-15-intel`, and `macos-15`.

## Deferred live cases

Run the existing ignored live-host tests with the real predecessor and target
archives. The matrix must cover these cases:

1. Upgrade from the real previous release to the real target release, then
   roll back to the previous release. Use `upgrade_and_rollback` in
   [`crates/muxe/tests/live_hosts.rs`](crates/muxe/tests/live_hosts.rs).
2. Run at least two simultaneous Zellij sessions sharing the canonical bridge,
   with at least two clients attached to one session. Cover every fresh bridge
   registration and every membership transition.
3. Prove a successful upgrade changes every bridge and broker to the target
   without restarting the existing hosts. Include the Herdr subscription
   handoff to the target broker without restarting the Herdr server.
4. Inject failure during the final session reload before it becomes unit-ready.
   Verify that one old bridge backup and every old broker are restored. Use
   `final_session_reload_failure` and retain its failure diagnostics.
5. Capture the actual old and target archive SHA-256 values, release provenance,
   and compatibility records as run evidence. A passing fixture-only test is
   not evidence for this published-release matrix.

The requirements above come from DESIGN.md, Activation verification (lines
1781–1783). The permanent tests use independently versioned old and target
fixtures; those synthetic old/target
fixtures and the current host-free activation regressions remain useful and
must stay. They are distinct from this deferred published-release live matrix.

## Existing inputs and tooling

The existing test names and typed inputs are the implementation to reuse:

- `target_only_smoke` is the current target-only smoke and requires
  `MUXE_TARGET_INSTALLATION`, `MUXE_HERDR_BINARY`, `MUXE_ZELLIJ_BINARY`,
  `MUXE_ZELLIJ_FOREGROUND_BINARY`, `MUXE_ZELLIJ_BOOTSTRAP_BINARY`, and
  `MUXE_ZELLIJ_PERMISSION_SEEDER`.
- `upgrade_and_rollback` additionally requires
  `MUXE_OLD_INSTALLATION` and `MUXE_ZELLIJ_FAULT_INJECTOR`.
- `final_session_reload_failure` uses the same old/target and fault-injector
  inputs. Every live case requires `MUXE_LIVE_HOSTS_APPROVED=true`.
- [`tools/release/live-upgrade.sh`](tools/release/live-upgrade.sh) already stages
  a target archive, downloads a real predecessor, verifies versions and bridge
  files, builds the fixture binaries, and invokes the two cross-release tests.
  Re-enable that driver only after the resume prerequisites are met; do not
  change its current fail-closed predecessor behavior merely to make a first
  release pass.
- The current target-only release job follows the staging and invocation pattern
  in [`.github/workflows/ci.yml`](.github/workflows/ci.yml). The release rows
  retain the four native architectures, pinned hosts, verified digests, explicit
  approval guard, and fail-closed input checks.

See the [README live-host test gates](README.md#live-host-test-gates) for the
owned-host rules and typed inputs. This file records planning only; it does not
claim that any live host, cross-release, or published-release check has passed.
