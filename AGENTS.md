# Muxe workspace rules

`../muxe-design/DESIGN.md` is authoritative. `../muxe-design/CLI.md` records the public command inventory.

- Use `jj` with frequent path-scoped checkpoints. One coordinator owns shared manifest and lockfile changes.
- Keep source ownership disjoint. For overlap, use an isolated `jj` workspace or hand ownership to one editor.
- Read `skill://tech-writing-style` on demand for documentation, not code-only work.
- Run live-host tests only with a fresh `TempDir`, explicit endpoints, and a retained owned `Child`.
- Never use process-name or global cleanup. Never run tests against a default user host.
- Preserve diagnostics on failure.
