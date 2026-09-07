#!/usr/bin/env bash
# Previous-to-target live upgrade/rollback driver.
#
# Usage:
#   live-upgrade.sh <PREV_TAG> <TARGET_TAG> <TARGET_ARCHIVES_DIR> <WORK_DIR> <ASSET>
# ASSET is the release matrix asset name (linux-x64, linux-arm64,
# macos-arm64, macos-x64); both stacks must carry that asset or the
# driver fails naming it.
#
# Phase 1 records the target stack built by this workflow (per-asset archive
#   extracted from TARGET_ARCHIVES_DIR).
# Phase 2 downloads the predecessor per-asset archive from its published
#   upgrade matrix cannot run; that fails closed here, never as a waiver.
# Phase 3 hands the staged stacks to the release-owned live-host runner:
# the upgrade/rollback matrix (`--exact upgrade_and_rollback`) and the
# final-session reload failure (`--exact final_session_reload_failure`,
# DESIGN 1783), both with MUXE_OLD/MUXE_TARGET_INSTALLATION plus host
# binaries, built fixture binaries, and the calling workflow's recorded
# approval. Either scenario failing fails the driver; no skip, no
# masking. The runner source is ready; its transfer gate stays red
# until the core-owned post-bind census ordering lands (the runner
# fails closed inside its bounded timeouts, never deadlocks silently).
# The library suites are not a surrogate for it. The live session
# handoff across real sessions and clients is part of that same runner,
# which runs only under the approvals gated by the calling workflow
# before this driver ever executes.
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

test "$#" -eq 5 || { echo "usage: live-upgrade.sh <PREV_TAG> <TARGET_TAG> <TARGET_ARCHIVES_DIR> <WORK_DIR> <ASSET>" >&2; exit 2; }
PREV="$1"
TARGET="$2"
ARCHIVES="$3"
WORK="$4"
ASSET="$5"

command -v gh >/dev/null 2>&1 || fail "gh CLI is required to fetch the predecessor archive"
command -v tar >/dev/null 2>&1 || fail "tar is required to extract release archives"

TARGET_ARCHIVE="$ARCHIVES/muxe-${TARGET}-${ASSET}.tar.gz"
test -f "$TARGET_ARCHIVE" || fail "target archive $TARGET_ARCHIVE is absent; the assets job did not publish it"
TARGET_DIR="$WORK/target"
mkdir -p "$TARGET_DIR"
tar -xzf "$TARGET_ARCHIVE" -C "$TARGET_DIR"
TARGET_ROOT="$TARGET_DIR/muxe-${TARGET}-${ASSET}"
test -x "$TARGET_ROOT/muxe" || fail "target archive has no executable muxe"
test -f "$TARGET_ROOT/lib/muxe/muxe-zellij.wasm" || fail "target archive has no bridge WASM"
TARGET_VERSION="$("$TARGET_ROOT/muxe" --version | cut -d' ' -f2)"
test "$TARGET_VERSION" = "${TARGET#v}" || fail "target binary reports $TARGET_VERSION, want ${TARGET#v}"
TARGET_WASM="$(file_digest "$TARGET_ROOT/lib/muxe/muxe-zellij.wasm")"
echo "target: tag=$TARGET asset=$ASSET binary=$TARGET_VERSION wasm_sha256=$TARGET_WASM"

PREV_DIR="$WORK/old"
mkdir -p "$PREV_DIR"
PREV_ARCHIVE="$PREV_DIR/muxe-${PREV}-${ASSET}.tar.gz"
gh release download "$PREV" \
  --repo "$GITHUB_REPOSITORY" \
  --pattern "muxe-${PREV}-${ASSET}.tar.gz" \
  --output "$PREV_ARCHIVE" \
  || fail "no predecessor archive for $PREV; without a prior release artifact the upgrade matrix cannot run"
tar -xzf "$PREV_ARCHIVE" -C "$PREV_DIR"
PREV_ROOT="$PREV_DIR/muxe-${PREV}-${ASSET}"
test -x "$PREV_ROOT/muxe" || fail "predecessor archive has no executable muxe"
PREV_VERSION="$("$PREV_ROOT/muxe" --version | cut -d' ' -f2)"
test "$PREV_VERSION" = "${PREV#v}" || fail "predecessor binary reports $PREV_VERSION, want ${PREV#v}"
test "$PREV_VERSION" != "$TARGET_VERSION" || fail "predecessor and target report the same version $PREV_VERSION; no upgrade to rehearse"
PREV_WASM="$(file_digest "$PREV_ROOT/lib/muxe/muxe-zellij.wasm")"
echo "predecessor: tag=$PREV binary=$PREV_VERSION wasm_sha256=$PREV_WASM"
echo "staged inputs: old=$PREV_ROOT@$PREV_VERSION target=$TARGET_ROOT@$TARGET_VERSION"
echo "phase 3: live upgrade/rollback matrix"
command -v cargo >/dev/null 2>&1 || fail "cargo is required to run the live-host runner"
command -v zellij >/dev/null 2>&1 || fail "pinned zellij not on PATH for the live matrix"
command -v herdr >/dev/null 2>&1 || fail "pinned herdr not on PATH for the live matrix"
cargo build --locked --manifest-path tools/muxe-zellij-foreground/Cargo.toml --bins \
  || fail "fixture binaries did not build"
FX="$PWD/tools/muxe-zellij-foreground/target/debug"
for bin in muxe-zellij-foreground muxe-zellij-bootstrap muxe-zellij-permit muxe-zellij-fault-injector; do
  test -x "$FX/$bin" || fail "fixture binary $bin missing after build"
done
MUXE_OLD_INSTALLATION="$PREV_ROOT" \
MUXE_TARGET_INSTALLATION="$TARGET_ROOT" \
MUXE_HERDR_BINARY="$(command -v herdr)" \
MUXE_ZELLIJ_BINARY="$(command -v zellij)" \
MUXE_ZELLIJ_FOREGROUND_BINARY="$FX/muxe-zellij-foreground" \
MUXE_ZELLIJ_BOOTSTRAP_BINARY="$FX/muxe-zellij-bootstrap" \
MUXE_ZELLIJ_PERMISSION_SEEDER="$FX/muxe-zellij-permit" \
MUXE_ZELLIJ_FAULT_INJECTOR="$FX/muxe-zellij-fault-injector" \
MUXE_LIVE_HOSTS_APPROVED=true \
cargo test --locked -p muxe --test live_hosts -- --ignored --exact upgrade_and_rollback
echo "phase 4: final-session reload failure (DESIGN 1783)"
MUXE_OLD_INSTALLATION="$PREV_ROOT" \
MUXE_TARGET_INSTALLATION="$TARGET_ROOT" \
MUXE_HERDR_BINARY="$(command -v herdr)" \
MUXE_ZELLIJ_BINARY="$(command -v zellij)" \
MUXE_ZELLIJ_FOREGROUND_BINARY="$FX/muxe-zellij-foreground" \
MUXE_ZELLIJ_BOOTSTRAP_BINARY="$FX/muxe-zellij-bootstrap" \
MUXE_ZELLIJ_PERMISSION_SEEDER="$FX/muxe-zellij-permit" \
MUXE_ZELLIJ_FAULT_INJECTOR="$FX/muxe-zellij-fault-injector" \
MUXE_LIVE_HOSTS_APPROVED=true \
cargo test --locked -p muxe --test live_hosts -- --ignored --exact final_session_reload_failure
