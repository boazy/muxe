# Muxe workspace rules

`../muxe-design/DESIGN.md` is authoritative. `../muxe-design/CLI.md` records the public command inventory.

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

