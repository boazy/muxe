#!/usr/bin/env bash
# Selects the preceding stable release tag for a target tag.
#
# Usage:
#   gh release list --json tagName --jq '.[].tagName' | select-predecessor.sh <TARGET_TAG>
#
# Prints the greatest stable vMAJOR.MINOR.PATCH tag strictly below the
# target in numeric version order and exits 0. Lexicographic `sort|last`
# is wrong here (v0.9.0 beats v0.10.0 lexically); GNU `sort -V` applies.
# Non-stable tags (prereleases, drafts, junk), the target itself, and
# anything at or above the target (future tags) never select. With no
# releases at all this fails explicitly: a first release has no
# predecessor and the upgrade matrix cannot run, never as a waiver.
set -euo pipefail

test "$#" -eq 1 || { echo "usage: select-predecessor.sh <TARGET_TAG>" >&2; exit 2; }
TARGET="$1"

below_target() {
  # True iff $1 is strictly below $2 in version order.
  first="$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n 1)"
  test "$first" = "$1" && test "$1" != "$2"
}

prev=""
while IFS= read -r tag || test -n "$tag"; do
  [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
  [[ "$tag" == "$TARGET" ]] && continue
  below_target "$tag" "$TARGET" || continue
  if [[ -z "$prev" ]]; then
    prev="$tag"
  else
    first="$(printf '%s\n%s\n' "$prev" "$tag" | sort -V | head -n 1)"
    [[ "$first" == "$prev" ]] && prev="$tag" || true
  fi
done

if [[ -z "$prev" ]]; then
  echo "BLOCKED: no predecessor release exists for $TARGET." \
    "The previous-to-target live upgrade/rollback matrix cannot run" \
    "on a first release and is a failing gate, not a waiver." >&2
  exit 1
fi
printf '%s\n' "$prev"
