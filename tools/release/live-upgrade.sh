#!/usr/bin/env bash
# Previous-to-target live upgrade/rollback driver.
#
# Usage:
#   live-upgrade.sh <PREV_TAG> <TARGET_TAG> <TARGET_ARCHIVES_DIR> <WORK_DIR>
#
# Phase 1 records the target stack built by this workflow (linux-x64 archive
#   extracted from TARGET_ARCHIVES_DIR).
# Phase 2 downloads the predecessor linux-x64 archive from its published
#   release and records the same fields. With no predecessor release the
#   upgrade matrix cannot run; that fails closed here, never as a waiver.
# Phase 3 hands the staged stacks to the broker-owned concrete Rust runner
#   with fixed old/target/host arguments (Herdr vertical first). Until that
#   entrypoint lands in this checkout the driver stops here naming it; the
#   library suites are not wired as a surrogate for it. The live session
#   handoff across real sessions and clients is part of that same
#   broker-owned scenario, which runs only under the approvals gated by the
#   calling workflow before this driver ever executes.
#
# Every phase prints the facts it established. Nothing here echoes success
# for work that did not run.
set -euo pipefail
file_digest() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1;
  elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "$1" | cut -d' ' -f1;
  else echo "MISSING: no sha256sum or shasum on this runner"; return 1; fi
}

fail() {
  echo "BLOCKED: $1"
  exit 1
}

test "$#" -eq 4 || { echo "usage: live-upgrade.sh <PREV_TAG> <TARGET_TAG> <TARGET_ARCHIVES_DIR> <WORK_DIR>" >&2; exit 2; }
PREV="$1"
TARGET="$2"
ARCHIVES="$3"
WORK="$4"

command -v gh >/dev/null 2>&1 || fail "gh CLI is required to fetch the predecessor archive"
command -v tar >/dev/null 2>&1 || fail "tar is required to extract release archives"

TARGET_ARCHIVE="$ARCHIVES/muxe-${TARGET}-linux-x64.tar.gz"
test -f "$TARGET_ARCHIVE" || fail "target archive $TARGET_ARCHIVE is absent; the assets job did not publish it"
TARGET_DIR="$WORK/target"
mkdir -p "$TARGET_DIR"
tar -xzf "$TARGET_ARCHIVE" -C "$TARGET_DIR"
TARGET_ROOT="$TARGET_DIR/muxe-${TARGET}-linux-x64"
test -x "$TARGET_ROOT/muxe" || fail "target archive has no executable muxe"
test -f "$TARGET_ROOT/lib/muxe/muxe-zellij.wasm" || fail "target archive has no bridge WASM"
TARGET_VERSION="$("$TARGET_ROOT/muxe" --version | cut -d' ' -f2)"
test "$TARGET_VERSION" = "${TARGET#v}" || fail "target binary reports $TARGET_VERSION, want ${TARGET#v}"
TARGET_WASM="$(file_digest "$TARGET_ROOT/lib/muxe/muxe-zellij.wasm")"
echo "target: tag=$TARGET binary=$TARGET_VERSION wasm_sha256=$TARGET_WASM"

PREV_DIR="$WORK/old"
mkdir -p "$PREV_DIR"
PREV_ARCHIVE="$PREV_DIR/muxe-${PREV}-linux-x64.tar.gz"
gh release download "$PREV" \
  --repo "$GITHUB_REPOSITORY" \
  --pattern "muxe-${PREV}-linux-x64.tar.gz" \
  --output "$PREV_ARCHIVE" \
  || fail "no predecessor archive for $PREV; without a prior release artifact the upgrade matrix cannot run"
tar -xzf "$PREV_ARCHIVE" -C "$PREV_DIR"
PREV_ROOT="$PREV_DIR/muxe-${PREV}-linux-x64"
test -x "$PREV_ROOT/muxe" || fail "predecessor archive has no executable muxe"
PREV_VERSION="$("$PREV_ROOT/muxe" --version | cut -d' ' -f2)"
test "$PREV_VERSION" = "${PREV#v}" || fail "predecessor binary reports $PREV_VERSION, want ${PREV#v}"
test "$PREV_VERSION" != "$TARGET_VERSION" || fail "predecessor and target report the same version $PREV_VERSION; no upgrade to rehearse"
PREV_WASM="$(file_digest "$PREV_ROOT/lib/muxe/muxe-zellij.wasm")"
echo "predecessor: tag=$PREV binary=$PREV_VERSION wasm_sha256=$PREV_WASM"
echo "staged inputs: old=$PREV_ROOT@$PREV_VERSION target=$TARGET_ROOT@$TARGET_VERSION"
fail "no upgrade rehearsal entrypoint exists in this checkout: the broker-owned concrete Rust runner with fixed old/target/host arguments (Herdr vertical first) has not landed; wire its literal invocation here once the broker owner confirms the target name"
