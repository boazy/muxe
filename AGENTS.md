# Muxe workspace rules

`../muxe-design` is retired and is not authoritative. `docs/planning/DESIGN.md` and `docs/planning/CLI.md` record only the initial design and are not authoritative. Current non-planning documentation—`README.md`, `REFERENCE.md`, other non-planning docs, and code documentation/comments—takes precedence. If current code conflicts with those sources and the desired behavior cannot be established, stop that specific implementation item and report it as blocked; do not use planning docs to decide it.

- Read `skill://tech-writing-style` on demand for documentation, not code-only
  work.
- Run live-host tests only with a fresh `TempDir`, explicit endpoints, and a
  retained owned `Child`.
- Never use process-name or global cleanup. Never run tests against a default
  user host.
- Preserve diagnostics on failure.

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

