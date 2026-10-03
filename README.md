# Muxe

Muxe is a modal menu system for terminal multiplexers, inspired by `which-key`. When you press a configured hotkey in your multiplexer, Muxe opens a temporary menu bar that displays available keys, nested submenus, and actions.

Muxe supports **Zellij** (version 0.46.0) and **Herdr** (version 0.8.2) as both the minimum-supported and latest-verified versions.

## How it works

- **Host-owned hotkeys**: You configure your root hotkey (such as `Alt m` or `prefix+m`) in your multiplexer's native configuration. Muxe never generates, modifies, or manages your multiplexer's keybindings.
- **Responsive menu bar**: When triggered, Muxe opens inside a dedicated pane—typically positioned across the bottom of the screen or as a floating overlay.
- **Colour control**: Muxe honours `NO_COLOR` for colour output through ratatui's crossterm backend. Attributes such as bold are not disabled when `NO_COLOR` is set.
- **Nested menus**: Pressing a key can execute an action, open an inline submenu, or open another named menu as a submenu.
- **Built-in navigation**: By default, `Esc` dismisses the active menu stack; `Backspace` returns to the parent menu (or exits if at a root menu); and `Left`/`Right` or `Page Up`/`Page Down` navigate pages in multi-page menus.
- **Safe key handling**: Any unknown key delivered to Muxe is swallowed: it is not forwarded to the underlying pane, does not alter the menu stack, and resets the inactivity timer. On Zellij, configured Locked-mode host bindings execute before Muxe receives the key and are reserved to the host.
- **Inactivity timeout**: The visible menu closes after 10 seconds without a key by default. Set `settings.timeout` globally or on a menu to change its interval; `off` disables it. The timer pauses while Muxe awaits an action.
- **Action completion**: Executing an action closes the menu by default. If an awaited action fails, Muxe remains in the active menu and displays the error. Post-action behavior is configurable per binding, per menu, or globally via `settings.after_action`.

## Quick start

### 1. Install Muxe

Install through [mise](https://mise.jdx.dev/):

```sh
mise use -g github:boazy/muxe
```

Or download a prebuilt archive from [GitHub Releases](https://github.com/boazy/muxe/releases). See [Installation](#installation) for instructions.

### 2. Initialize configuration

Generate the starter configuration files:

```sh
muxe init
```

This writes `$CONFIG_DIR/config.yml` and creates or retains `themes/` and `color-schemes/` directories.

### 3. Add menu bindings

The starter `config.yml` initializes with an empty `main` menu (`bindings: {}`). Add at least one binding to `$CONFIG_DIR/config.yml` (default: `~/.config/muxe/config.yml`):

```yaml
version: 1

menus:
  main:
    title: Muxe
    bindings:
      p:
        label: split pane
        action: pane:split direction=down
```

See [Configuration example](#configuration-example) for an example with submenus and tab actions.

### 4. Connect your multiplexer

Follow the setup steps for your multiplexer:

- **Zellij**: Install the bridge plugin and add a root keybinding in `config.kdl`. See [Zellij setup](#zellij-setup).
- **Herdr**: Add a command keybinding in `config.toml`. See [Herdr setup](#herdr-setup).

### 5. Open a menu

Press your configured hotkey in your multiplexer session to open the Muxe menu.

## Installation

### Package manager (mise)

```sh
mise use -g github:boazy/muxe
```

### Prebuilt release archives

Release tags follow `v{major}.{minor}.{patch}`. Archive filenames follow `muxe-v{version}-{os}-{arch}.tar.gz`, where `{os}-{arch}` is one of:

- `linux-x64` (statically linked with musl)
- `linux-arm64` (statically linked with musl)
- `macos-arm64` (macOS Apple Silicon)

Each archive contains a complete installation:

```text
muxe
lib/muxe/muxe-zellij.wasm
share/muxe/completions/
LICENSE
```

Preserve the extracted directory layout. Copying only the `muxe` binary does not provide a complete installation: the binary locates packaged assets (such as the Zellij WASM bridge) relative to its installation root.

### Release verification and manual installation

Every release publishes `SHA256SUMS` covering all target archives, as well as GitHub artifact attestations for each archive. The attestations are generated before publication; a missing checksum or attestation indicates a failed release, not an optional step.

Download your selected target archive and `SHA256SUMS`, verify both, and extract to a versioned directory:

```sh
version=0.1.0
asset=linux-x64

# Download archive and checksum manifest
curl -LO "https://github.com/boazy/muxe/releases/download/v${version}/muxe-v${version}-${asset}.tar.gz"
curl -LO "https://github.com/boazy/muxe/releases/download/v${version}/SHA256SUMS"

# Verify the target archive checksum
grep "muxe-v${version}-${asset}.tar.gz" SHA256SUMS | shasum -a 256 -c -

# Verify the GitHub artifact attestation
gh attestation verify "muxe-v${version}-${asset}.tar.gz" --repo boazy/muxe

# Extract to a versioned directory and update the stable symlink
mkdir -p ~/.local/muxe
tar -xzf "muxe-v${version}-${asset}.tar.gz" -C ~/.local/muxe/
ln -sfn ~/.local/muxe/"muxe-v${version}-${asset}" ~/.local/muxe/current
export PATH="$HOME/.local/muxe/current:$PATH"
```

When updating a direct-archive installation, keep the previous versioned directory until `muxe activate` commits. After activation succeeds, remove the prior directory. If you install through mise, retain the previous tool version until `muxe activate` commits before running `mise prune`.

Release publication requires the current target-only real-host verification on all three release architectures. The real cross-release upgrade, rollback, and fault matrix is deferred until the implementation is complete and a first genuine published release is available as the live predecessor. See [`TODO-CROSS-RELEASE.md`](TODO-CROSS-RELEASE.md). This documentation does not claim that any live check has passed.

## Configuration

### Directory paths

Muxe stores configuration and cache files in standard platform directories on both Linux and macOS:

- Configuration (`$CONFIG_DIR`): `$XDG_CONFIG_HOME/muxe`, falling back to `~/.config/muxe`.
- Cache (`$CACHE_DIR`): `$XDG_CACHE_HOME/muxe`, falling back to `~/.cache/muxe`.

### Configuration files

Muxe reads its configuration from `$CONFIG_DIR`:

- `config.yml`: Required base configuration file for all hosts.
- `zellij.yml`: Optional override file applied when running inside Zellij.
- `herdr.yml`: Optional override file applied when running inside Herdr.

Muxe recursively merges the active host override file into `config.yml`. Override files inherit `version: 1` and cannot change it.

Automatic configuration watching is enabled by default. A valid reload applies immediately to newly opened menus, while open menus retain their pinned configuration generation. If an edit produces invalid YAML or unsupported actions, Muxe logs the error and keeps the last valid configuration active.

### Themes and color schemes

Set `theme` and `color-scheme` to the corresponding filenames without `.yml`, under `$CONFIG_DIR/themes/` and `$CONFIG_DIR/color-schemes/`. Both default to `default`. The catalogs are independent: the same name can identify a theme and a color scheme. A scheme's `title` is display text, not its selection name.

Palette values must be `#rgb` or `#rrggbb` literals. Semantic colors may reference palette entries or other semantic colors; a semantic name takes precedence when it also exists in the palette. Muxe rejects unknown references and alias cycles, including unused entries in the selected scheme. The built-in default scheme inherits terminal colors; user-defined semantic color values cannot use `inherit`.

Muxe validates every style in a selected theme/scheme pair, then retains its resolved colors for rendering. All asset files must first parse successfully as supported YAML; syntax errors prevent configuration loading even in an unselected asset. Validation errors in unselected theme or color-scheme definitions are deferred until selection. An attachment's theme or scheme override does not change other menus that are already attached.

In configuration style fields, `inherit` is an alias name, not a reset keyword. Omitting foreground or background leaves that part of the underlying terminal style unchanged; an inherited color from the default scheme explicitly resets it to the terminal default.

### Inactivity timeout

Set `settings.timeout` globally or in a menu to a duration in milliseconds (`250ms`), seconds (`10s`), or minutes (`2m`). The default is `10s`. Use `off` to disable inactivity dismissal. A menu inherits the global setting unless it specifies its own value; a finite menu value re-enables a global `off`.

Opening a submenu starts its full interval. Returning starts the caller menu's full interval. Each received key resets the visible menu's timer, including an unknown or unavailable key. Protocol responses and malformed terminal input do not reset it.

The timer pauses while Muxe awaits an action. Keys received while pending reset its paused interval. A successful action that stays in the menu resumes the remaining interval; a failed action starts a full interval. Open menus keep their timeout policies from the pinned configuration generation across reloads.

Binding-level `settings.timeout` is invalid. Use `settings.execution.timeout` on a binding to limit action execution; it does not control menu inactivity.

See [`REFERENCE.md`](REFERENCE.md) for the complete inventory of portable actions, native actions, parameter schemas, and canonical key syntax.

### Creating starter configuration

Create the initial configuration files:

```sh
muxe init
```

This writes `$CONFIG_DIR/config.yml` plus `themes/` and `color-schemes/` directories. The starter configuration is embedded in the binary. If `config.yml` already exists, the command exits with an error and leaves all files unchanged. Package installation never creates, replaces, merges, or deletes user configuration.

### Configuration example

Here is an example `config.yml` defining a root menu with a nested submenu and portable terminal actions:

```yaml
version: 1

menus:
  main:
    title: Muxe
    tags: [root]
    bindings:
      t:
        label: tabs
        action: menu:open tabs
      p:
        label: split pane
        action: pane:split direction=down

  tabs:
    title: Tabs
    bindings:
      t:
        label: new tab
        action: tab:create
      r:
        label: rename tab to work
        action: tab:rename name=work
      c:
        label: close tab
        action: tab:close
```

## Zellij setup

Setting up Muxe in Zellij requires two steps: installing the WASM bridge plugin and adding a root keybinding to your Zellij configuration.

### 1. Install the Zellij bridge

Materialize the bridge plugin and optionally update Zellij configuration:

```sh
muxe integration install zellij
```

This command copies `lib/muxe/muxe-zellij.wasm` to `$CONFIG_DIR/integrations/zellij/muxe-zellij.wasm` and records an owner-only receipt. If the destination file matches the receipt digest, the command replaces it. If the file contains unrecognized bytes or does not match the receipt, the command halts without modifying the file.

In an interactive terminal, the command prompts to add or update these nodes in your `config.kdl`:

```kdl
plugins {
    muxe location="file:/absolute/path/to/muxe-zellij.wasm"
}

load_plugins {
    muxe
}
```
Command options:

- `-q`, `--quiet`: Silence prompts and normal output. Without a policy flag, the bridge file is copied without editing `config.kdl`. If a managed node was modified since installation, a warning prints to stderr.
- `--always-configure`: Apply the configuration edit without prompting.
- `--never-configure`: Never edit configuration and never prompt.
- `--zellij-config <path>`: Inspect or edit this configuration file instead of the discovered default.

In a non-interactive shell without a policy flag, the command behaves as `--never-configure`. Package installation and `muxe init` never modify the bridge or Zellij configuration. Ancestor directories above `$CONFIG_DIR/integrations/zellij` may retain existing ambient permissions; the managed integration directory itself must be owner-only (`0700`).

When Zellij loads the bridge plugin, it displays a host-owned permission prompt. Review and grant these requested permissions in Zellij for Muxe to operate.

### 2. Configure a root keybinding in Zellij

Zellij keybindings run `muxe ui menu {root}` directly using the native `Run` action. The integration command targets `--zellij-config <path>` if provided, then `$ZELLIJ_CONFIG_DIR/config.kdl`, then `$XDG_CONFIG_HOME/zellij/config.kdl` (falling back to `~/.config/zellij/config.kdl`). For manual root keybindings, add this binding to whichever configuration file your running Zellij instance loads.

Merge the `normal` and `locked` blocks into your existing `keybinds` section, choosing an unused hotkey. Each binding issues `SwitchToMode` for its own active mode before `Run`, so the bridge observes that mode rather than guessing a prior mode. The modal menu restores the observed mode after dismissal. While Locked mode is active, this key is reserved to Zellij.

Replace an older `shared_among "normal" "locked"` Run-only binding rather than keeping both versions of `Alt m`. The pinned host does not guarantee an initial `ModeUpdate` to an autoloaded bridge; without the same-mode action, the first menu may fail closed while waiting to capture Locked mode.

To open Muxe as a borderless floating pane across the bottom (recommended), add this binding:

```kdl
keybinds {
    normal {
        bind "Alt m" {
            SwitchToMode "Normal"
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
    locked {
        bind "Alt m" {
            SwitchToMode "Locked"
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

`floating true` and positioning options (`x`, `y`, `width`, `height`) apply only to floating Run panes.

To open Muxe as a tiled pane split downward instead (using Zellij's initial split sizing), use this binding:
```kdl
keybinds {
    normal {
        bind "Alt m" {
            SwitchToMode "Normal"
            Run "muxe" "ui" "menu" "main" {
                direction "Down"
                close_on_exit true
                start_suspended false
            }
        }
    }
    locked {
        bind "Alt m" {
            SwitchToMode "Locked"
            Run "muxe" "ui" "menu" "main" {
                direction "Down"
                close_on_exit true
                start_suspended false
            }
        }
    }
}
```

To verify that Zellij inherits a `PATH` containing `muxe`, run `command -v muxe` inside a Zellij pane.

### Uninstalling the Zellij bridge

To remove the bridge plugin and clean up managed configuration:

```sh
muxe broker retire --host all
muxe integration uninstall zellij
```

The uninstall command removes nodes created by Muxe, or restores the pre-installation text of nodes Muxe replaced, only when their current text and semantic structure still match the installed state recorded in the receipt. User-modified nodes and root keybindings are never deleted automatically.

Before changing KDL, bridge, or the receipt, uninstall validates every selected KDL plan and the stable bridge and rollback copy against the receipt. It refuses while an interrupted install or the exact bridge-sharing activation unit has a journal, including a corrupt journal. Uninstall retains staging artifacts for the transaction that owns them. Bridge files are removed only when their recorded paths and digests match the receipt.

Receipt ownership for `--zellij-config` uses its recorded absolute, normalized KDL path spelling. Muxe removes `.` components and refuses `..`; other spellings are not collapsed through symlinks. This identity differs from the canonical physical bridge-directory identity used to group Zellij brokers, so a symlink alias cannot inherit receipt authority.

## Herdr setup

Muxe installs as a native executable, not as a Herdr manifest plugin. There is no `muxe integration install herdr` command.

### 1. Configure a root keybinding in Herdr

Add one `[[keys.command]]` block per root hotkey to your Herdr configuration (`HERDR_CONFIG_PATH` if set, otherwise `~/.config/herdr/config.toml` on Linux and macOS):

```toml
[[keys.command]]
key = "prefix+m"
type = "shell"
command = "muxe menu open --pane-type split --direction down --height 30% main"
description = "Open Muxe"
```

Herdr evaluates `command` as shell text. Quote each root, theme, or color-scheme value that contains whitespace or shell metacharacters. For example, to pass a multi-word theme name:

```toml
command = "muxe menu open --theme 'high contrast' --pane-type split --direction down --height 30% main"
```

Replace `'high contrast'` with your configured theme name.

### 2. Reload Herdr configuration

Apply the configuration change:

```sh
herdr server reload-config
```

The `muxe` executable must be on the `PATH` inherited by the Herdr server process. Muxe never edits `config.toml`. When you no longer need Muxe, remove the `[[keys.command]]` block manually.

## Troubleshooting

If the menu does not open when pressing your hotkey:

- **Check PATH**: Verify that the multiplexer process inherits a `PATH` containing `muxe`. Run `command -v muxe` from a pane inside your multiplexer session.
- **Test direct execution**: Run `muxe ui menu main` (in Zellij) or `muxe menu open main` (in Herdr) directly from a shell prompt to observe immediate error output.
- **Inspect logs**: Check persistent logs at `$CACHE_DIR/logs/muxe.jsonl` (default: `~/.cache/muxe/logs/muxe.jsonl`). Native command failures add a payload-free event; stderr retains the complete diagnostic. Herdr notifications are best-effort; the log file is authoritative.
- **Zellij permissions**: In Zellij, confirm that you accepted the host-owned permission prompt displayed when the bridge plugin loaded.

## Upgrades, activation, and rollback

Upgrading Muxe separates updating installed binary files from activating running brokers.

By default, `muxe activate` updates every live host recorded in Muxe's current-user broker registry and dismisses its active menus. The `--host` options are `all` (the default), `current`, `zellij`, and `herdr`. Use `--host zellij` or `--host herdr` to select one multiplexer kind, or `--host current` to select the host managing the current shell. In Zellij, brokers with the same canonical stable bridge identity form one activation group. A broker is the on-demand Muxe process associated with a live multiplexer session.

Upgrade the binary and activate running brokers:

```sh
mise upgrade --no-prune github:boazy/muxe
muxe activate
```

To roll back to a previous version, select that version and run `muxe activate`:

```sh
mise use -g github:boazy/muxe@{version}
muxe activate
```

### How activation works

`muxe activate` preflights every selected unit before mutation:

- One Herdr broker is one unit.
- All live Zellij brokers with the same canonical stable bridge identity form one atomic group.

Units commit independently after global preflight. The final output reports each unit as committed, unchanged, rolled back, or failed. Run `muxe activate` from your shell before using the menu hotkey.

When a menu launch needs a broker, Muxe checks the unit's activation journal before probing the normal endpoint. It verifies `Status` and peer credentials on the same control connection, then rechecks the socket identity before consulting the registry. Muxe reuses, adopts, or relocates only the authenticated registration. Its token, process ID, and start time bind cleanup to that exact row; a legacy peer without a token may reuse only its unchanged row. A durable startup claim prevents concurrent launchers from starting competing children. Muxe removes a stale row only after a registry-locked recheck proves that the exact process is dead and the endpoint is absent or refused. Pending activation and ambiguous endpoints fail closed.

Only a running ordinary or `TargetCommitted` broker with matching host identity may serve an attachment. A `TargetGated` broker and a legacy peer with an ambiguous handoff are not attachable.

The `control-json-v1` protocol is additive: unknown fields are ignored and new status fields may be absent. Activation still requires the old broker to advertise `CoordinatorSuppliedV1`; a legacy broker without this Prepare capability is refused before drain or journal creation.

New activation journals use schema v5. Schemas v3 and v4 remain readable for rollback before Ready. Earlier Ready journals lack exact target-incarnation proof and remain preserved instead of authorizing a commit. Corrupt or unsupported journals are also preserved for diagnosis.

For Zellij, the coordinator holds the bridge-unit readiness gate while checking every target at one OS-wide monotonic as-of tick. The v5 Ready proof records the unit epoch ID separately from the tick, the exact member set, and each target's process-scoped registration identity. Herdr Ready records its exact target identity too. This is a broker-observed snapshot, not simultaneous physical host membership: `list-clients` is a snapshot, not a host-issued lease, and clients can attach or detach around it. Later heartbeat expiry does not change the earlier proof. An endpoint and handoff match alone do not establish the recorded target incarnation.

For a Zellij activation, the target broker binds its gated control endpoint without opening bridge pipes. After the coordinator durably records that the replacement bridge reloaded in every selected session, the target opens its request and event pipes and establishes a fresh client census. A journal with different authority or a rollback decision rejects the wait; a predecessor bridge subscription cannot authorize Ready.

On ordinary menu coldstart, Muxe keeps an already loaded, receipt-backed bridge so its observed prior pane survives; if the bridge is absent, Muxe loads the stable bridge before spawning the broker. Zellij reports the loaded plugin's alias rather than its in-memory URL: Muxe checks the receipt, current KDL mapping, and installed bytes, then requires a fresh compatible bridge registration. A loaded incompatible plugin fails closed; the config mapping alone does not attest its running code.

After durable Ready, recovery commits only the original certified target. If that target is missing or replaced, Muxe preserves the journal and does not start a substitute; the old brokers remain stopped or drained as applicable.

Before Ready, recovery persists the rollback decision, shuts down targets, restores the old bridge and receipt, reloads the old bridge in every recorded Zellij session, and then resumes the old brokers. It reports the original failure and each rollback diagnostic; an ambiguous barrier preserves its journal. Each activation attempt checks outstanding journals before a new transaction. If recovery reports an unresolved unit, leave its journal and transaction-owned artifacts unchanged for operator follow-up.

## Compatibility

Print the compatibility metadata embedded in this Muxe binary:

```sh
muxe compatibility
muxe compatibility --json
```

Muxe supports Zellij 0.46.0 and Herdr 0.8.2 as both the minimum-supported and latest-verified versions.

This command renders the embedded compatibility record; it does not probe running multiplexer hosts or verify installed disk files. The compatibility report records two bridge identifiers:

- `hosts.zellij.bridge_build_id`: The deterministic build identifier shared by the native binary and the bridge registration handshake.
- `packaged_wasm.sha256`: The expected SHA-256 digest used by installation and activation to verify bridge files.

The JSON report forms the release compatibility contract. It records the pinned host version and source revision, generated action and protocol fingerprints, the bridge build ID, and the packaged-WASM SHA-256.

Origins now carry session and active-tab metadata: an upgraded native binary rejects a previously installed bridge fail-closed at the registration handshake, so refresh installed bridges with `muxe activate` after upgrading.

## Uninstallation and purge

Standard uninstallation removes the tool and bridge while preserving your configuration files, custom themes, caches, and logs.

Uninstall in this order so that the integration receipt remains available to cleanly restore host configuration:

1. Retire running brokers:
   ```sh
   muxe broker retire --host all
   ```

2. While `muxe` remains installed, remove the Zellij bridge:
   ```sh
   muxe integration uninstall zellij
   ```
3. *(Optional)* If you also want to delete retained configuration files, themes, caches, and logs, run `muxe purge` before removing the executable:
   ```sh
   muxe purge --config --cache
   ```
   - At least one of `--config` or `--cache` is required.
   - Without `--yes`, the command lists every resolved path and prompts for interactive confirmation. Pass `--yes` for non-interactive automation.
   - `--config` removes the entire configuration directory, including user-authored themes and any remaining materialized bridge files.
   - `--cache` refuses while an activation journal is live, needs recovery, or an active lifecycle participant still holds the cache lease.
   - `muxe purge` never modifies Zellij or Herdr keybindings.
4. Remove the installed tool version or delete the versioned archive directory and symlink.
5. Remove any Muxe keybindings from your Zellij or Herdr configuration files.

`muxe broker retire` drains active menus and foreground work and removes the broker endpoint without starting a replacement. Detached generic command children remain under supervisor-only processes until reaped; retirement does not terminate those commands.

## Shell completions

Pre-generated completion scripts for Bash, Zsh, and Fish ship under `share/muxe/completions/` in every release archive:

- Bash: `share/muxe/completions/muxe.bash`
- Zsh: `share/muxe/completions/_muxe`
- Fish: `share/muxe/completions/muxe.fish`

Copy or link the appropriate completion file to your shell's completion directory.

## Live-host test gates

Integration tests use isolated fixture processes under a fresh temporary directory per test. Tests never access default user sockets, configuration files, or processes.

Running live-host tests requires explicit environment approval:

- `MUXE_LIVE_HOSTS_APPROVED=true`: Required before any fixture process launches. Any other value fails the test immediately.
- Zellij permission grants: The test runner seeds the bridge's full 11-permission contract (`ReadApplicationState`, `ChangeApplicationState`, `RunActionsAsUser`, `OpenFiles`, `OpenTerminalsOrPlugins`, `RunCommands`, `WriteToStdin`, `WriteToClipboard`, `Reconfigure`, `FullHdAccess`, and `ReadCliPipes`) directly into an isolated Zellij permission cache for the managed bridge path. The contract lives once in `muxe-zellij-protocol`; the runner passes it, never a retyped subset.

Test suites and their required inputs:

- `target_only_smoke`: Verifies the current target installation against live fixtures. Requires `MUXE_TARGET_INSTALLATION`, `MUXE_HERDR_BINARY`, `MUXE_ZELLIJ_BINARY`, `MUXE_ZELLIJ_FOREGROUND_BINARY`, `MUXE_ZELLIJ_BOOTSTRAP_BINARY`, and `MUXE_ZELLIJ_PERMISSION_SEEDER`. No predecessor release is required.
- `upgrade_and_rollback` and `final_session_reload_failure`: Add `MUXE_OLD_INSTALLATION` and `MUXE_ZELLIJ_FAULT_INJECTOR`. The fault test also replays a real downgrade failure.

The target-only smoke covers two current-target scenarios:

- A one-client activation starts both pinned hosts and one Zellij PTY client, invokes same-version `muxe activate`, and checks both broker identities.
- A fresh two-client Zellij rig performs read-only typed origin routing. It verifies exact two-client census coverage, routes requests by `(client_id, registration)`, matches each release with its origin snapshot, and observes post-route heartbeats from both clients.

The two-client probe uses the production adapter's transport recovery during startup. Before sending requests, it requires a fresh exact census and compatible registrations from one current subprocess epoch and wire generation. A reconnect discards earlier registration coverage. Once routing begins, transport loss or an epoch or generation change fails the probe; requests are never replayed.

Both anchor acquisition and the final census recheck retry typed `Unavailable` results within their original deadlines. These results never count as a census; persistent unavailability times out. Other typed query errors and a changed admission census fail immediately. Admission retries recheck the current epoch and adapter readiness. Fixture-only channel wrappers log both bounded 4 KiB stderr tails on transport errors and before either child is parked, replaced, or closed, including adapter shutdown.

At the pinned Zellij revision, delayed cleanup of a discovery probe can disconnect a new CLI pipe that reused the probe's client ID. A controlled scheduling test reproduces this failure while the host and both PTY clients stay alive. The original CI failure log did not capture that IPC sequence, so its exact interleaving remains inferred. The fixture uses existing production recovery for startup loss without changing the host pin or weakening routing checks.

CI supplies the pinned host binaries and the explicit `MUXE_LIVE_HOSTS_APPROVED=true` authorization for these live checks. They are not part of the default local test command and must not use local default host state.

CI runs `target_only_smoke` as a real target-only smoke on Linux pull requests. Manual dispatches and the weekly schedule run the full matrix on Ubuntu and macOS. The release workflow runs the current-target gate on all three release architectures. Missing inputs or approvals fail the gate. The real cross-release upgrade, rollback, and fault matrix remains deferred until the first genuine published predecessor; see [`TODO-CROSS-RELEASE.md`](TODO-CROSS-RELEASE.md). Passing fixture-only suites never stand in for this live matrix.
