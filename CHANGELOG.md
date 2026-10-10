## [0.3.2] - 2026-10-10

### 🚀 Features

- *(herdr)* Invoke configured commands from native bindings

### 🐛 Bug Fixes

- *(runtime)* Explain broker and adapter failures without internal shorthand
- *(lifecycle)* Explain activation and recovery failures
- *(cli)* Explain activation results and user-facing failures

### 📚 Documentation

- *(herdr)* Document configured-command bindings

### 🧪 Testing

- *(herdr)* Handle reset after read-only retirement
- *(lifecycle)* Assert bridge preflight outcomes without wording pins
## [0.3.1] - 2026-10-09

### 🐛 Bug Fixes

- *(ui)* Align themed shortcut columns and connected badges
- *(themes)* Restore canonical semantic color roles

### 📚 Documentation

- *(themes)* Explain aligned layouts and semantic color roles

### 💼 Other

- V0.3.1
## [0.3.0] - 2026-10-09

### 🚀 Features

- *(themes)* Embed curated color palettes
- *(ui)* Add built-in display themes and catalog

### 🐛 Bug Fixes

- *(core)* Preserve configured navigation bindings
- *(broker)* Retain context and unfinished execution evidence
- *(lifecycle)* Distinguish refused activation from restored hosts
- *(runtime)* Render captured diagnostics and unknown outcomes
- *(core)* Compare binding defaults by canonical key identity

### 🧪 Testing

- *(runtime)* Prove navigation on an owned Herdr terminal
- *(themes)* Verify built-in catalog and document selection

### 💼 Other

- V0.3.0
## [0.2.3] - 2026-10-08

### 🐛 Bug Fixes

- *(activate)* Validate effective config with selected host

### 💼 Other

- V0.2.3
## [0.2.2] - 2026-10-08

### 🚀 Features

- *(config)* Filter menus and bindings by host

### 🐛 Bug Fixes

- *(herdr)* Dispatch creation after natural menu exit
- *(ci)* Satisfy strict lint for Herdr menu smoke

### 💼 Other

- V0.2.2
## [0.2.1] - 2026-10-07

### 🐛 Bug Fixes

- *(herdr)* Gate compatibility on the minimum server release
- *(herdr)* Decode subscription events from the event/data envelope

### 💼 Other

- V0.2.1
## [0.2.0] - 2026-10-06

### 🐛 Bug Fixes

- *(muxe)* Honor Herdr split dimensions
- Bound UTF-8 diagnostics and reject malformed colors
- *(muxe)* Harden planned KDL config commits
- *(muxe)* Validate receipt ownership before uninstall
- *(muxe)* Bind uninstall ownership to receipt config paths
- Propagate broker retirement failures
- *(herdr)* Bind pending cleanup to its host incarnation
- *(core)* Reject unknown underscore fields
- *(paths)* Preserve exact configuration ownership spelling
- *(broker)* Serialize concurrent config reloads
- *(logging)* Persist native command failures
- *(zellij)* Preserve receipt ownership on reinstall
- *(zellij)* Preflight native uninstall authority
- *(zellij)* Filter Muxe-owned panes from focus targets
- *(ui)* Centralize status transitions
- *(broker)* Retain attachment cleanup ownership
- *(broker)* Honor dismissal policy during activation drain
- *(core)* Lower and type-check CEL once with one evaluator
- *(core)* Prove compiled themes satisfy the renderer contract
- *(herdr)* Validate the emitted portable request at load time
- *(zellij)* Type unresolved context values against the generated schema
- *(core)* Accept CEL hexadecimal integers through the parsed AST
- *(core)* Scan theme statements in one forward pass
- *(input)* Apply configured lock modifiers when matching keys
- *(input)* Decode Kitty event types for direct and tilde functional keys
- *(broker)* Isolate slow UI event delivery
- *(herdr)* Gate host operations on one adapter lifecycle state
- *(zellij)* Capture session and active-tab metadata in origins
- *(ui)* Arbitrate expired Escape deadlines deterministically
- *(ui)* Degrade component render failures to a usable menu
- *(core)* Give named and inline menus distinct validated identities
- *(tooling)* Make host pins and schema digests the single source of truth
- *(zellij)* Expire gate capture when broker traffic stops
- *(ui)* Render through the ratatui crossterm backend
- *(zellij)* Classify focus before adopting the origin pane
- *(ui)* Count unsupported input as user activity
- *(herdr)* Reject cache-normalized candidates before cache lookup
- *(zellij)* Complete multi-request executions as one aggregate outcome
- *(tooling)* Isolate completion-check staging per invocation
- *(host)* Retire the broker after a bounded host-loss grace
- *(ui)* Preserve breadcrumb tails on narrow surfaces
- *(ui)* Apply resolved style to menu headings
- *(ui)* Preserve terminal cleanup failures
- *(host)* Separate discovery from Zellij incarnation
- *(broker)* Apply per-session theme overrides
- *(herdr)* Guard requests by live socket incarnation
- *(herdr)* Serialize unary sends per runtime incarnation
- *(broker)* Supervise reconnect compatibility rebuilds
- *(ui)* Honor menu-scoped inactivity timeouts
- *(lifecycle)* Make activation and startup recovery transactional
- *(ci)* Restore covered startup and owner-only cache roots
- *(zellij)* Retain bridge authority across activation and menu coldstart
- *(ci)* Recover two-client smoke startup transport loss
- *(input)* Map typed terminal keys directly to named keys
- *(activation)* Use the typed Herdr server version
- *(herdr)* Key post-dismissal requests by PaneId
- *(integration)* Retain typed artifact digests across transactions
- *(zellij)* Retain typed adapter and UI identities
- *(zellij)* Type resume and pipe-child epochs
- *(ci)* Retain Zellij bootstrap until bridge-ready handoff
- *(lifecycle)* Separate persisted rows from immutable host-validated registry state
- *(broker)* Publish owner-only sockets atomically
- *(ci)* Activate pinned CodSpeed runner for comparisons
- *(test)* Keep scoped bootstrap handoffs bounded and portable
- *(ci)* Repair CodSpeed and host fixture gates
- *(ci)* Bound bootstrap readiness census
- *(bridge)* Refresh pending subscription census on timer
- *(ci)* Isolate script writers and reap failed host startup
- *(zellij)* Pin upstream client lifecycle correction
- *(bench)* Isolate compiler measurements and share configuration fixture
- *(herdr)* Wait for pane placement before capturing UI context

### 📚 Documentation

- Clarify documentation precedence
- Define isolated workspace cleanup policy
- *(condition)* Document fallible entry points
- *(input)* Document lock-modifier matching
- *(herdr)* Record pinned keyboard capability limits
- *(agents)* Require host-polymorphic behavior
- Record remaining domain-boundary cutovers
- *(ci)* Clarify workflow labels and release diagnostics

### ⚡ Performance

- *(ui)* Make template padding linear in produced output
- *(ui)* Retain compact checked routing metadata

### 🚜 Refactor

- *(core)* Derive portable actions from schema
- *(core)* Satisfy strict compiler lints
- *(broker)* Centralize execution ownership
- Simplify execution and test lifecycle helpers
- *(launch)* Share canonical UI argv recognition across adapters
- *(test)* Let owned host fixture apply activation environment
- *(activation)* Select concrete host policy for bridge transactions
- *(lifecycle)* Make coldstart and activation spawning host-owned
- *(core)* Retain typed portable action resolution
- *(broker)* Type cleanup task claims and gate keys
- *(core)* Type core modifier flags and set operations
- *(cli)* Pass typed handoff IDs from validated arguments
- *(cli)* Retain typed launcher origin IDs
- *(herdr)* Distinguish public tab numbers from row positions
- *(core)* Lower inline menus with typed metadata
- *(theme)* Retain resolved colors and typed catalog names
- *(zellij)* Retain pane, tab and capture lease identities in bridge state

### 🎨 Styling

- Satisfy the strict clippy gate
- *(herdr)* Satisfy the strict clippy gate
- *(ui)* Satisfy the strict clippy gate

### 🧪 Testing

- *(ci)* Use owned bridge transaction roots

### ⚙️ Miscellaneous Tasks

- *(audit)* Add Jev host-polymorphism scanner
- *(bench)* Measure divan benches with CodSpeed

### 💼 Other

- V0.2.0
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

### 💼 Other

- V0.1.4
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
