#!/usr/bin/env bash
# Test live upgrades and rollback between release installations.
#
# Usage:
#   live-upgrade.sh <PREV_TAG> <TARGET_TAG> <TARGET_ARCHIVES_DIR> <WORK_DIR> <ASSET>
# ASSET selects linux-x64, linux-arm64, or macos-arm64. Both releases must
# provide an archive for that platform.
#
# Extract and check the target archive, then download and check the predecessor
# archive. A first release has no predecessor and cannot run these tests.
# Run upgrade_and_rollback and final_session_reload_failure with both
# installations, pinned hosts, test helper binaries, and explicit approval.
# Either test failing fails this driver. Unit tests do not replace these live
# host tests. The caller must obtain approval before invoking the driver.
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
test "$PREV_VERSION" != "$TARGET_VERSION" || fail "predecessor and target report the same version $PREV_VERSION; an upgrade requires different versions"
PREV_WASM="$(file_digest "$PREV_ROOT/lib/muxe/muxe-zellij.wasm")"
echo "predecessor: tag=$PREV binary=$PREV_VERSION wasm_sha256=$PREV_WASM"
echo "staged inputs: old=$PREV_ROOT@$PREV_VERSION target=$TARGET_ROOT@$TARGET_VERSION"
echo "Running live upgrade and rollback tests"
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
echo "Running final-session reload failure test"
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
