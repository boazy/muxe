# Muxe

Muxe opens configured root menus as focused modal terminal UI on Zellij and
Herdr. Root hotkeys stay in native host configuration. Muxe never generates,
rewrites, or owns host key configuration.

## Installation

Install through mise:

```text
mise use -g github:boazy/muxe
```

Or download a target archive from GitHub Releases and extract it. Tags use
`v{major}.{minor}.{patch}`. Asset names use
`muxe-v{version}-{os}-{arch}.tar.gz`, where `{os}-{arch}` is `linux-x64`,
`linux-arm64`, `macos-x64`, or `macos-arm64`. Linux archives target musl for
static distribution.

Each archive is a complete installation:

```text
muxe
lib/muxe/muxe-zellij.wasm
share/muxe/completions/
LICENSE
```

Preserve the extracted layout. Copying only `muxe` is not a complete Zellij
installation: the native binary locates packaged assets relative to its
installation root.

For direct-archive installations, keep each release in its own versioned
directory, verify before extracting, and retarget a stable symlink:

```sh
version=0.1.0
asset=linux-x64
tar -xzf "muxe-v${version}-${asset}.tar.gz"
mkdir -p ~/.local/muxe
mv "muxe-v${version}-${asset}" ~/.local/muxe/
ln -sfn ~/.local/muxe/"muxe-v${version}-${asset}" ~/.local/muxe/current
export PATH="$HOME/.local/muxe/current:$PATH"
```

Run `muxe activate` after retargeting, and retain the previous versioned
directory until activation commits. After successful activation, remove the
prior directory. The same retention rule applies to mise: prune the prior
tool version only after activation commits.

## Verification

Every release publishes `SHA256SUMS` covering all target archives and GitHub
artifact attestations covering each archive. The attestations are created
before publication; a missing checksum or attestation is a failed release,
not an optional step. Verify both before installing:

```sh
shasum -a 256 -c SHA256SUMS
gh attestation verify muxe-v0.1.0-linux-x64.tar.gz --repo boazy/muxe
```

Release publication requires the current target-only real-host verification on
all four release architectures. The real cross-release upgrade, rollback, and
fault matrix is deferred until the implementation is complete and a first
genuine published release is available as the live predecessor. See
[`TODO-CROSS-RELEASE.md`](TODO-CROSS-RELEASE.md). This documentation does not
claim that any live check has passed.

## Setup

Create the starter configuration:

```sh
muxe init
```

This writes `$CONFIG_DIR/config.yml` plus theme and color-scheme directories.
The starter configuration is embedded in the binary. The command fails
without changing anything when `config.yml` already exists. Package
installation never creates, replaces, merges, or deletes user configuration.

`$CONFIG_DIR` is `$XDG_CONFIG_HOME/muxe`, falling back to `~/.config/muxe`.
`$CACHE_DIR` is `$XDG_CACHE_HOME/muxe`, falling back to `~/.cache/muxe`.
Linux and macOS both use these locations.

## Zellij bridge

Materialize the bridge and optionally configure Zellij KDL nodes:

```sh
muxe integration install zellij
```

This copies the packaged `lib/muxe/muxe-zellij.wasm` to
`$CONFIG_DIR/integrations/zellij/muxe-zellij.wasm` and records an owner-only
receipt. A destination that matches the receipt digest can be replaced.
Unrecognized bytes or a receipt mismatch are never overwritten: the command
reports both digests and waits for manual resolution.

By default an interactive terminal asks whether to add or correct these nodes
in the selected `config.kdl`:

```kdl
plugins {
    muxe location="file:/absolute/path/to/muxe-zellij.wasm"
}

load_plugins {
    muxe
}
```

Options:

- `-q`, `--quiet`: silence prompts and normal output. Without a policy flag, the
  bridge is materialized without editing configuration. A managed node the user
  modified since installation still warns on stderr.
- `--always-configure`: apply the safe edit without prompting.
- `--never-configure`: never edit and never prompt.
- `--zellij-config {path}`: inspect or edit this file instead of the
  discovered default.

A non-interactive invocation without a policy flag behaves as
`--never-configure`. Install never changes the selected config file's parent
directory mode: an existing `0755` parent is accepted as-is. Package
installation and `muxe init` never touch the bridge or Zellij configuration.

Remove receipt-owned artifacts:

```sh
muxe broker retire --host all
muxe integration uninstall zellij
```

Uninstall removes created nodes and restores replaced nodes only when they
are unchanged since installation, restoring the exact previous bytes. A clean
restore removes the bridge and the receipt. When a managed node was
user-modified, uninstall keeps the user's text and the receipt, removes the
bridge, and names the artifact on stderr. Observed nodes, user-modified nodes, and
root keybindings are never removed.

## Herdr

Muxe installs as the native `muxe` executable, not as a Herdr manifest
plugin. There is no `muxe integration install herdr` command. Add one
`[[keys.command]]` block per root hotkey to the Herdr configuration
(`HERDR_CONFIG_PATH` when set, otherwise `~/.config/herdr/config.toml` on
Linux and macOS):

```toml
[[keys.command]]
key = "prefix+m"
type = "shell"
command = "muxe menu open --pane-type split --direction down --height 30% main"
description = "Open Muxe"
```

Then run `herdr server reload-config`. The `muxe` executable must be on the
`PATH` inherited by the Herdr server. Muxe never edits `config.toml`; remove
the blocks manually when they are no longer wanted.

## Root launchers

Zellij bindings start `muxe ui menu {root}` directly with the native `Run`
action. A borderless floating pane across the bottom is recommended:

```kdl
keybinds {
    shared_among "normal" "locked" {
        bind "Alt m" {
            Run "muxe" "ui" "menu" "main" {
                floating true
                x "0"
                y "70%"
                width "100%"
                height "30%"
                borderless true
                close_on_exit true
                start_suspended false
            }
        }
    }
}
```

A downward split keeps the menu in the tiled layout:

```kdl
keybinds {
    shared_among "normal" "locked" {
        bind "Alt m" {
            Run "muxe" "ui" "menu" "main" {
                direction "Down"
                close_on_exit true
                start_suspended false
            }
        }
    }
}
```

Check that Zellij inherits a `PATH` containing the mise shim with
`command -v muxe` from a Zellij pane.

## Activation and rollback

Changing the selected version and activating it are separate operations:

```sh
mise upgrade --no-prune github:boazy/muxe
muxe activate
```

Roll back by selecting a prior version and running the same transaction:

```sh
mise use -g github:boazy/muxe@{version}
muxe activate
```

`muxe activate` preflights every selected unit before mutating any of them.
One Herdr broker is one unit. All live Zellij brokers sharing one stable
bridge form one atomic group. Units commit independently after preflight.
The final report names every committed, unchanged, rolled-back, and failed
unit. Run explicit `muxe activate` before the next hotkey so upgrade work
happens outside the modal path.

## Compatibility

```sh
muxe compatibility
muxe compatibility --json
```

The first release supports Zellij 0.46.0 and Herdr 0.8.2 as both the
minimum-supported and latest-verified versions.

The report exposes two complementary bridge identities. `hosts.zellij.bridge_build_id`
is the generated pre-link identity shared by the native binary and bridge
registration handshake. `packaged_wasm.sha256` is the SHA-256 of the staged
WASM artifact and proves that the installed bridge bytes match the packaged
release. The native side verifies both values locally; the host channel uses
`bridge_build_id` for registration compatibility and does not attest arbitrary
filesystem bytes.

The JSON report is the release compatibility contract. It records the pinned
host version and source revision, generated action and protocol fingerprints,
the bridge build ID, and the packaged-WASM SHA-256.

## Uninstall and purge

Uninstall in this order:

1. `muxe broker retire --host all`.
2. While `muxe` remains installed, `muxe integration uninstall zellij`.
3. Remove the mise-managed tool version.
4. Remove root keybindings that are no longer wanted.

`config.yml`, themes, caches, and logs are preserved by default. `muxe purge`
removes retained data explicitly:

```sh
muxe purge --config --cache --yes
```

At least one of `--config` or `--cache` is required. Without `--yes` the
command prints every resolved path and asks for confirmation. `--cache`
refuses while an activation journal is live, needs recovery, or an active
lifecycle participant still holds the cache lease. The command never edits
Zellij or Herdr keybindings.

## Completions

Bash, Zsh, and Fish files ship under `share/muxe/completions/` in every
archive as `muxe.bash`, `_muxe`, and `muxe.fish`. The Zsh file uses the
conventional fpath name. They are generated from the command definition by
`tools/codegen-completions` and committed. CI fails on any byte-for-byte
difference. There is no runtime `muxe completions` command in V1.

## Live-host test gates

The live-host runners use owned fixture processes under one fresh
temporary directory per test. No default user socket, configuration, or
process is ever touched. Each test requires explicit approval before
any spawn:

- `MUXE_LIVE_HOSTS_APPROVED` must be exactly `true`. Any other value
  fails the test before anything launches.
- The bridge permission grant is separate from that guard. The runner
  seeds exactly three permissions (`ReadApplicationState`,
  `ChangeApplicationState`, `ReadCliPipes`) for the exact managed
  bridge path into the owned Zellij permission cache. The guard alone
  never implies the grant.

The three tests and their inputs:

- `target_only_smoke` needs `MUXE_TARGET_INSTALLATION`,
  `MUXE_HERDR_BINARY`, `MUXE_ZELLIJ_BINARY`,
  `MUXE_ZELLIJ_FOREGROUND_BINARY`, `MUXE_ZELLIJ_BOOTSTRAP_BINARY`,
  and `MUXE_ZELLIJ_PERMISSION_SEEDER`. No predecessor is required.
- `upgrade_and_rollback` and `final_session_reload_failure` add
  `MUXE_OLD_INSTALLATION` and `MUXE_ZELLIJ_FAULT_INJECTOR`. The fault test
  also replays a real downgrade failure.

CI runs `target_only_smoke` as a real target-only smoke on Linux pull
requests and as a required current-target gate on all four release
architectures. Missing inputs or approvals fail the gate. The real
cross-release upgrade, rollback, and fault matrix is deferred until the first
genuine published predecessor; see
[`TODO-CROSS-RELEASE.md`](TODO-CROSS-RELEASE.md). Passing fixture-only suites
never stand in for the required target-only live run.
