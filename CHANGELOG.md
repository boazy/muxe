## Unreleased

### 🐛 Bug Fixes

- *(bridge)* Request a fresh client census on timer ticks while an event subscription waits for registration under granted permissions.
- *(ci)* Use the same pinned `cargo-codspeed` task to build and run benchmark comparisons.
- *(ci)* Run the standalone scope-guard script through `/bin/sh` to avoid Linux executable-file-busy errors. Keep its environment isolation checks unchanged.
- *(ci)* Set accepted bootstrap test sockets to blocking mode so macOS can drain render output. Preserve send errors and reap the helper before reporting failures.
- *(ci)* Wait for missing initial Zellij client-list output within the existing startup deadline. Keep malformed output and missing post-detach observations fatal. Reap timed-out CLI children and retain their diagnostics.
- *(ci)* Wait for missing Zellij client-list responses before and after initial bridge registration, within one bootstrap deadline. Keep observed membership and installation identity checks strict.
- *(ci)* Wait for installation-matched bridge registration before Zellij fixture handoff, and retain the bootstrap client until all initial PTY clients attach without reusing its ID. Preserve legacy receipt support through the selected native record and receipt digest authority.
- *(input)* Map parsed terminal key enums directly to core named keys, keeping Kitty wire-code decoding in the terminal parser and preserving key-binding behavior
- *(ci)* Use existing Zellij adapter recovery before two-client smoke admission, discard stale epoch/generation coverage, and retain exact addressed origin checks with fail-fast post-admission loss and no request replay
- *(ci)* Retry unavailable Zellij census queries within the original two-client admission deadline without accepting changed membership, and log both bounded CLI stderr tails before recovery or shutdown discards them
- *(broker)* Require the selected host's registry authority for cold-start adoption, reuse, stale removal, and retirement; reject foreign persisted rows before mutation
- *(activation)* Spawn targets from the typed journal unit instead of a registry label or socket filename
- *(activation)* Keep bridge preflight, reload, and Ready proof under the selected host policy; accept unchanged bridge membership regardless of registry insertion order
- *(zellij)* Reuse a receipt-backed loaded bridge on ordinary coldstart, reload only an absent bridge before spawn, refresh pane-origin census after host pane transitions, and defer activation target subscriptions until durable replacement reload
- *(zellij)* Keep spawned brokers outside the launching pane's process group; document mode-specific root bindings that publish an observed prior mode before the first UI capture
- *(cli)* Create and validate the cache root owner-only before audit logging; refuse unsafe existing directories
- *(ui)* Honor menu-scoped inactivity timeouts and reject binding-scoped inactivity settings
- *(broker)* Reconcile authenticated cold-start endpoints before spawning, preserving live registration ownership and refusing ambiguous recovery
- *(ui)* Honor per-invocation theme and color-scheme overrides without changing the pinned menu-session choice
- *(broker)* Reject requests for replaced host incarnations and rebuild native-binding availability after reconnect
- *(activation)* Require exact target-incarnation Ready authority and replay crash recovery through ordered barriers
- *(activation)* Apply Herdr's supported-version policy using the runtime's typed server version
- *(integration)* Keep artifact digests typed through installation and recovery. Reject malformed journal digests before changing the journal or artifacts, preserving corrupt transactions for diagnosis.
- *(zellij)* Retain typed client, pane, session, execution, and lease identities in adapter state and capture APIs. Keep archived UI session IDs typed through attachment and event routing without changing wire formats.
- *(activation)* Use the existing host-neutral `ClientId` for both registered and observed readiness membership, preserving client text and lexical ordering through wire serialization.
- *(zellij)* Give resume attempts and installed pipe-child epochs distinct types so freshness tags cannot be swapped across lifecycle domains.
- *(zellij)* Bind uninstall configuration ownership to the recorded path
- *(zellij)* Preserve receipt-owned configuration provenance across reinstalls
- *(broker)* Propagate retirement and registry cleanup failures
- *(herdr)* Close only the leased pending pane and retain uncertain cleanup ownership
- *(herdr)* Retain typed pane IDs in Herdr post-dismissal queues, removing string round-trips while preserving close-event and confirmed-absence dispatch.
- *(herdr)* Serialize unary requests and multi-step host transactions per runtime incarnation
- *(config)* Reject unknown underscore-prefixed fields
- *(config)* Serialize concurrent reloads before publishing newer contents
- *(cli)* Persist payload-free native command failure events
- *(zellij)* Preflight uninstall journal and artifact authority before mutation
- *(zellij)* Filter Muxe-owned panes from eligible focus targets
- *(broker)* Retain dispatch ownership through shutdown and release timed-out UI work
- *(herdr)* Fix tab swaps when stable public tab numbers differ from row positions. Preserve number-based selectors, use the pinned host's insertion boundaries, and verify the resulting order by tab identity.
- *(broker)* Publish Unix sockets atomically only after securing their owner-only permissions. Preserve existing endpoints on collision and keep strict startup identity checks.

### Miscellaneous Tasks

- *(broker)* Type cleanup task claims and key test gates by their existing identities, preserving retry/requeue ownership for pending panes and captures.
- *(core)* Type modifier-set flags and preserve canonical key matching without raw integer mutation.
- *(cli)* Parse nonzero activation handoffs once at the validated CLI boundary and pass typed IDs into both broker serve paths.
- *(cli)* Preserve typed Herdr workspace, tab, and pane identities through launcher-origin selection and saved tuples.
- *(core)* Resolve portable actions once into typed execution values across the broker and host adapters. Preserve OS command paths and retain typed Herdr pane and split directions through dispatch.
- *(core)* Carry inline-menu identity as typed lowering metadata without serialized string markers or copied syntax trees.
- *(theme)* Retain resolved colors and distinct theme and color-scheme identities. Reuse canonical color formatting and borrowed scalar values while preserving duplicate policies, deferred validation, and diagnostic precedence.
- *(zellij)* Retain SDK terminal/plugin pane variants, distinct tab positions and stable tab IDs, and capture-lease identities through bridge focus and pending dismissal state.
- *(lifecycle)* Convert persisted registry rows into immutable concrete host lifecycle state while preserving legacy rows, independent selected mutation authority, and exact live/snapshot/endpoint checks.

## [0.1.4] - 2026-09-12

### 🚀 Features

- Add effective menu dump command
- *(config)* Check all host configurations

### 🐛 Bug Fixes

- *(broker)* Recover dead coldstart endpoints safely
- *(ui)* Execute hidden menu bindings
- *(ci)* Satisfy strict menu dump lints
- *(cli)* Refresh config check completions

### 📚 Documentation

- Add menu dump changelog entry

### 🧪 Testing

- *(herdr)* Cover dead stale broker recovery
- *(coldstart)* Stabilize stale socket setup

### ⚙️ Miscellaneous Tasks

- Regenerate menu dump completions
- Remove crate publishing on release
## [0.1.3] - 2026-09-11

### 🚀 Features

- *(core)* Define portable creation fields
- *(broker)* Define the post-dismissal dispatch contract
- *(zellij)* Add post-dismissal creation
- *(herdr)* Add deferred creation and outcome certainty

### 🐛 Bug Fixes

- *(test)* Stabilize cancellation fixture readiness
- Satisfy strict clippy across creation paths
- Preserve creation diagnostics and origin ownership

### 📚 Documentation

- Document portable creation support

### ⚙️ Miscellaneous Tasks

- *(agents)* Update agent rules to reflect jj commit policy

### 💼 Other

- V0.1.3
## [0.1.2] - 2026-09-10

### 🐛 Bug Fixes

- *(release)* Clear stale bridge identity outputs
- *(zellij)* Harden registration turnover
- *(zellij)* Preserve newer pending capture
- *(zellij)* Await capture restoration before replacement
- *(protocol)* Complete bridge contract cutover
- *(zellij)* Close unsolicited event set
- *(zellij)* Preserve restore barriers across resets
- *(zellij)* Compensate canceled pending capture
- *(test)* Encode typed live-host subscription

### 🚜 Refactor

- *(zellij)* Type pipe envelope identifiers
- *(zellij)* Correlate requests by typed registration
- *(protocol)* Share bridge envelopes and identifiers
- *(protocol)* Complete shared bridge scalars
- *(protocol)* Consolidate bridge lifecycle schema
- *(protocol)* Type bridge lifecycle identifiers
- *(zellij)* Migrate typed lifecycle callers

### 🎨 Styling

- *(zellij)* Format typed bridge lifecycle

### 🧪 Testing

- *(zellij)* Cover registration-scoped correlation

### 💼 Other

- *(zellij)* Disable unused ULID generator features
- V0.1.2
## [0.1.1] - 2026-09-09

### 🐛 Bug Fixes

- *(zellij)* Invalidate replaced registration
- *(herdr)* Reject unknown menu before launch
- *(test)* Avoid Linux proof exec race
- *(ui)* Isolate terminal input nonblocking mode

### 📚 Documentation

- Align supported release platforms

### ⚡ Performance

- *(ci)* Cache external Zellij builds

### 💼 Other

- V0.1.1
## [0.1.0] - 2026-09-09

### 🐛 Bug Fixes

- Forward verified WASM and bootstrap fixes
- Preserve quiet Herdr subscriptions
- *(release)* Correct prompts and allow CI override
- *(release)* Keep Stop as cursor-only default
- *(ci)* Tolerate absent macOS quarantine attribute
- *(ci)* Wait for Zellij session discovery
- *(ci)* Preserve Zellij client identity
- *(zellij)* Refresh missed startup subscriptions
- *(zellij)* Report initial census identities
- *(zellij)* Revalidate bridge subscriptions
- *(release)* Drop Intel macOS artifacts

### 📚 Documentation

- Host-support menu:open distinguishes portable menu from source-proven zellij launcher path; reference regenerated
- Rewrite README for clarity and new user onboarding

### ⚡ Performance

- *(ci)* Shorten release critical path
- *(ci)* Parallelize deterministic gates
- *(ci)* Use native target caching
- *(ci)* Cache divergent cargo targets

### 🎨 Styling

- Format teardown error helper

### 🧪 Testing

- Preserve live body and teardown failures

### ⚙️ Miscellaneous Tasks

- Add planning docs
- Install pinned Rust components
- Require recorded approval before PR host install
- Gate PR host smoke on recorded approval
- Run multiline mise checks with bash
- Keep CodSpeed upload advisory only
- Gate full matrix on scheduled runs
- Ignore omp dir

### 💼 Other

- Herdr dev-dep on zellij adapter (common adapter_contract suite), muxe bin async-trait dep for UiControl; lock refresh
- Workspace getrandom 0.2.17 + muxe-zellij-wasm direct dep for WASI OS randomness in bridge registration IDs; lock refresh
- Herdr dev-only edge on muxe-zellij-protocol for typed PipeEvent/encode_event_line/BridgeIdentity in recorded Zellij case; no production dep
- Divan dev-dep for parser/session benches in terminal-input + ui; lock adds divan tree
- Explicit harness=false bench targets parser + session for divan entries
- Workspace parking_lot 0.12.5 + muxe bin dep for fixture/test lock sites and coordinator state
- Divan parser + session entries; both binaries ran green
- Final lifecycle acceptance
- Use zellij vendored curl for static musl
- Scope vendored curl to musl
- V0.1.0
