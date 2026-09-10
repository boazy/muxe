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
