# Muxe CLI Inventory

This file is a concise implementation inventory derived from `DESIGN.md`. `DESIGN.md` remains authoritative for behavior, safety, host integration, and lifecycle semantics.

> **Implementation note:** This inventory is not a frozen CLI contract. Implementing agents may add commands or arguments when the implementation requires them. Keep additions consistent with the design, document them here, and cover user-visible behavior with the appropriate CLI checks.

## Command tree

```text
muxe
├── init
├── menu
│   └── open
├── pane
│   └── open
├── integration
│   ├── install zellij
│   └── uninstall zellij
├── activate
├── broker
│   └── retire
├── compatibility
├── purge
└── ui
    └── menu
```

The native executable also needs an internal broker-serving mode. Its exact private command name and bootstrap arguments are intentionally left to the implementation.

## Global behavior

- `-h`, `--help`: Clap-generated help for each command level.
- `-V`, `--version`: print the Muxe version.
- Errors return a nonzero status and use the persistent logging rules in `DESIGN.md`.
- Host detection uses inherited `ZELLIJ_*` or `HERDR_*` values where `--host auto` applies.
- Bash, Zsh, and Fish completions are generated and packaged. V1 does not need a runtime `muxe completions` command.

## Commands

### `muxe init`

```text
muxe init
```

Creates the starter configuration and companion theme and color-scheme directories. It must not overwrite an existing `config.yml`. It does not install the Zellij bridge or edit host configuration.

### `muxe menu open`

```text
muxe menu open [OPTIONS] <ROOT>
```

Opens a configured root menu through the selected host. Herdr root keybindings call this command; native Zellij root keybindings call `muxe ui menu` directly.

| Argument or option | Values | Default | Notes |
|---|---|---|---|
| `<ROOT>` | Menu ID | Required | Passed to `muxe ui menu` as an ordinary argument. |
| `--host` | `auto`, `zellij`, `herdr` | `auto` | Detects the host from inherited environment values. |
| `--theme` | Theme name | Configuration | Per-invocation override. |
| `--color-scheme` | Color-scheme name | Configuration | Per-invocation override. |
| `--pane-type` | `split`, `overlay`, `popup` | `split` | Unsupported host/type combinations fail before pane creation. |
| `--parent-pane` | Pane ID or `current` | `current` | `current` means the captured origin pane. |
| `--direction` | `down`, `up`, `left`, `right` | `down` | Herdr V1 accepts only `down` and `right`. |
| `--width` | Cells or percentage | Host/type-specific | Example: `80` or `80%`. |
| `--height` | Cells or percentage | Host/type-specific | Example: `10` or `30%`. |
| `--position` | `<X>,<Y>` in cells | Host/type-specific | Applies only to floating or overlay panes. |

`muxe menu open` deliberately has no `--no-focus`: a modal menu must receive focus. It also has no `--cwd`; the menu process always uses the captured origin working directory. It forwards the supported placement options to `muxe pane open`.

### `muxe pane open`

```text
muxe pane open [OPTIONS] -- <COMMAND> [ARGUMENTS...]
```

Opens a generic command pane while preserving the command's argument boundaries.

| Argument or option | Values | Default | Notes |
|---|---|---|---|
| `--host` | `auto`, `zellij`, `herdr` | `auto` | Detects the host when omitted. |
| `--pane-type` | `split`, `overlay`, `popup` | `split` | Herdr V1 supports `split` only. |
| `--parent-pane` | Pane ID or `current` | `current` | Target or origin pane. |
| `--direction` | `down`, `up`, `left`, `right` | `down` | Herdr V1 accepts only `down` and `right`. |
| `--width` | Cells or percentage | Host/type-specific | Meaning depends on pane type. |
| `--height` | Cells or percentage | Host/type-specific | Meaning depends on pane type. |
| `--position` | `<X>,<Y>` in cells | Host/type-specific | Floating or overlay panes only. |
| `--no-focus` | Boolean flag | Off | Valid for generic panes, not menu panes. |
| `--cwd` | Path | Not yet specified | Explicit working-directory override. `DESIGN.md` does not define the omitted behavior for generic panes. |
| `<COMMAND>` | Executable | Required | Must follow `--`. |
| `[ARGUMENTS...]` | Exact argument vector | Empty | Forwarded without joining at the Muxe API boundary. |

On Herdr, this command uses the transient-tab trampoline: `layout.apply` starts the exact command, then `pane.move` places the running pane.

### `muxe integration install zellij`

```text
muxe integration install zellij [OPTIONS]
```

Materializes the bundled bridge at the stable Muxe integration path and optionally adds or corrects Muxe's Zellij KDL nodes.

| Option | Effect |
|---|---|
| `-q`, `--quiet` | Suppress prompts and normal output. Without another policy flag, do not edit `config.kdl`. |
| `--always-configure` | Apply the safe KDL edit without prompting. |
| `--never-configure` | Do not edit KDL and do not prompt. |
| `--zellij-config <PATH>` | Inspect or edit this Zellij configuration instead of the discovered default. |

`--always-configure` and `--never-configure` are mutually exclusive. `--quiet --always-configure` still edits because the explicit policy wins; `--quiet --never-configure` performs silent bridge materialization only. A non-interactive invocation without a policy flag behaves as `--never-configure`.

The command maintains the owner-only integration receipt and refuses to overwrite bridge bytes that do not match that receipt.

### `muxe integration uninstall zellij`

```text
muxe integration uninstall zellij [OPTIONS]
```

Removes receipt-owned Zellij integration artifacts. It removes or restores KDL nodes only when the receipt proves ownership and the nodes remain unchanged.

Options and prompt semantics match `muxe integration install zellij`:

- `-q`, `--quiet`
- `--always-configure`
- `--never-configure`
- `--zellij-config <PATH>`

It never removes user-owned root keybindings or KDL nodes recorded as merely observed.

### `muxe activate`

```text
muxe activate [--host <SCOPE>]
```

Activates the currently selected Muxe executable and assets in live host sessions through the transactional cross-version handoff.

| Option | Values | Default | Notes |
|---|---|---|---|
| `--host` | `all`, `current`, `zellij`, `herdr` | `all` | `current` requires invocation from a managed host. A current Zellij host expands to every live broker sharing the stable bridge path. |

An ordinary Muxe invocation may run the same transaction for its current host when it detects an old broker or bridge. Explicit activation remains the recommended upgrade and rollback path.

### `muxe broker retire`

```text
muxe broker retire [--host <SCOPE>]
```

Drains active Muxe menus and foreground work, then retires brokers without starting replacements. Detached generic children remain under supervisor-only processes until reaped.

`--host` accepts `all`, `current`, `zellij`, or `herdr`. `DESIGN.md` does not currently assign a default, and uninstall instructions pass `--host all` explicitly.

### `muxe compatibility`

```text
muxe compatibility [--json]
```

Prints the compatibility record embedded in the native executable.

- Default: human-readable output.
- `--json`: stable snake_case JSON containing Muxe build data, protocol fingerprints, host ranges, Zellij source and bridge metadata, and Herdr schema metadata.

### `muxe purge`

```text
muxe purge [--config] [--cache] [--yes]
```

Explicitly removes retained Muxe data. At least one of `--config` or `--cache` is required.

| Option | Effect |
|---|---|
| `--config` | Remove the complete Muxe configuration tree, including user-authored themes and any remaining materialized bridge. |
| `--cache` | Remove compiled schemas and logs. Refuse while an activation journal is live or needs recovery. |
| `--yes` | Authorize non-interactive deletion. Without it, print resolved paths and ask for confirmation. |

This command never edits Zellij or Herdr keybindings.

### `muxe ui menu`

```text
muxe ui menu [--theme <NAME>] [--color-scheme <NAME>] <ROOT>
```

Runs the native terminal UI for one root menu.

| Argument or option | Meaning |
|---|---|
| `<ROOT>` | Required root menu ID. |
| `--theme <NAME>` | Per-invocation theme override. |
| `--color-scheme <NAME>` | Per-invocation color-scheme override. |

Zellij `Run` bindings invoke this command directly. The Herdr trampoline starts it through structured `layout.apply` argv.

For a gated Herdr launch, the process also receives `MUXE_PENDING_LAUNCH_TOKEN` and origin bootstrap values through its environment. These are internal coordination values, not alternate CLI sources for `<ROOT>`, `--theme`, or `--color-scheme`.

## Intentionally absent commands

- No `muxe integration install herdr`: Herdr needs no bridge or manifest registration.
- No `herdr plugin install` step for Muxe.
- No runtime completion generator in V1; completion files ship in release archives.
