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

For direct-archive installations, extract each release into its own versioned
directory, atomically retarget a stable `muxe` symlink, run `muxe activate`,
and retain the previous directory until activation commits.

## Verification

Every release publishes `SHA256SUMS` covering all target archives and GitHub
artifact attestations covering each archive. Verify both before installing:

```sh
shasum -a 256 -c SHA256SUMS
gh attestation verify muxe-v0.1.0-linux-x64.tar.gz --repo boazy/muxe
```

A missing checksum or attestation is a failed release, not an optional step.

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

- `-q`, `--quiet`: suppress prompts and output. Without a policy flag, the
  bridge is materialized without editing configuration.
- `--always-configure`: apply the safe edit without prompting.
- `--never-configure`: never edit and never prompt.
- `--zellij-config {path}`: inspect or edit this file instead of the
  discovered default.

A non-interactive invocation without a policy flag behaves as
`--never-configure`. Package installation and `muxe init` never touch the
bridge or Zellij configuration.

Remove receipt-owned artifacts:

```sh
muxe broker retire --host all
muxe integration uninstall zellij
```

Uninstall removes created nodes and restores replaced nodes only when they
are unchanged since installation. Observed nodes, user-modified nodes, and
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
minimum-supported and latest-verified versions. The JSON form reports the
embedded record with stable snake_case field names.

The bridge registration digest is reported as blocked, not as a hash. The
host channel gives the running bridge no way to attest its own bytes, so the
native side verifies packaged and installed digests while the registration
half awaits a design decision. Release metadata is not claimed complete
while that decision is pending.

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
refuses while an activation journal is live or needs recovery. The command
never edits Zellij or Herdr keybindings.

## Completions

Bash, Zsh, and Fish files ship under `share/muxe/completions/` in every
archive. They are generated from the command definition by
`tools/codegen-completions` and committed. There is no runtime
`muxe completions` command in V1.
