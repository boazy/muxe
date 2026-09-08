## [0.1.0] - 2026-09-08

### 🐛 Bug Fixes

- Forward verified WASM and bootstrap fixes
- Preserve quiet Herdr subscriptions
- *(release)* Correct prompts and allow CI override
- *(release)* Keep Stop as cursor-only default

### 📚 Documentation

- Host-support menu:open distinguishes portable menu from source-proven zellij launcher path; reference regenerated
- Rewrite README for clarity and new user onboarding

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
