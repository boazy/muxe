# Muxe workspace rules

`../muxe-design` is retired and is not authoritative. `docs/planning/DESIGN.md` and `docs/planning/CLI.md` record only the initial design and are not authoritative. Current non-planning documentation—`README.md`, `REFERENCE.md`, other non-planning docs, and code documentation/comments—takes precedence. If current code conflicts with those sources and the desired behavior cannot be established, stop that specific implementation item and report it as blocked; do not use planning docs to decide it.

- Read `skill://tech-writing-style` on demand for documentation, not code-only
  work.
- Run live-host tests only with a fresh `TempDir`, explicit endpoints, and a
  retained owned `Child`.
- Never use process-name or global cleanup. Never run tests against a default
  user host.
- Preserve diagnostics on failure.

## Host polymorphism

- Host-independent production and test code must not branch on a concrete muxer
  name or kind to select behavior. No `is_zellij` flags, host-name comparisons,
  or host-kind `if`/`match` ladders in shared broker, lifecycle, configuration,
  or test logic. Put host-specific behavior behind required trait methods
  implemented by each concrete adapter or fixed-identity test adapter. A new
  muxer should not require edits to shared control flow.
- A composition boundary may inspect an explicit CLI argument or detected host
  once to select and construct the concrete adapter or validator. Wire-format
  conversion may exhaustively map a closed protocol enum when the protocol
  requires it. Do not pass a host tag downstream to reselect behavior.
  Branching on host-independent lifecycle states or capabilities is allowed.

## VCS

- Use `jj` with frequent path-scoped checkpoints. One coordinator owns shared
  manifest and lockfile changes.
- Keep source ownership disjoint. For overlap, use an isolated `jj` workspace or
  hand ownership to one editor.
- Before pushing to remote, make sure you clean up noisy `jj` changeset history,
  and make the commit headers compatible with conventional commit style.
- Conventional commit headers are _not necessary_ for local changesets that are
  not pushed to remote yet. It's often a good idea to reorganize the commits and
  rewrite the messages anyway before push, to reflect the actual changes that we
  published, not the local history of our work that contains trials, inline
  fixes (that should be squashed/absorbed) etc.


## Isolated workspace lifecycle

- Run at most three concurrent work items in separate JJ workspaces.
- After verification and review, clean, squash, or reorder agent-owned history as needed. Rebase integrated work onto the latest `main`, then advance the `main` bookmark.
- Always clean up completed isolated workspaces after integration. When all work is integrated and no unique unmerged or unknown files remain, remove the workspace registration, directory, and build output through the supported JJ/`wt` workflow. Account for ignored and untracked files; never use raw directory deletion.
- Never delete active or blocked workspaces, or user-owned unmerged work.
- Monitor free disk space. If it falls below 25 GB, pause builds and clean Rust `target` directories before continuing.
