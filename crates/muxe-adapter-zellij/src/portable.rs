//! Portable-action mapping for Zellij 0.46.0 with pinned-source evidence.
//!
//! Every row names the exact pinned `Action` variant (or the precise reason no
//! row exists) from `fixtures/zellij/0.46.0/action-inventory.rs`, itself derived
//! from revision `81f56e1aed4e17b822af5cb382a8f524e35f3eae`.
//!
//! ## Targeting contract
//!
//! Pinned `run_action` routes with the bridge plugin as the originating pane
//! (`zellij-server/src/plugins/zellij_exports.rs`, `route_action(…, client_id,
//! …, Some(PaneId::Plugin(plugin_id)), …)`); its `context` map becomes plugin
//! context, never a focus or pane target. Focus-relative `Action` variants
//! therefore execute against the currently focused pane, which is the Muxe UI
//! pane while a menu is open. This module never dispatches a focus-relative
//! action and claims it targets the immutable origin. Instead:
//!
//! - Pane-scoped portables use the ID-targeted `*ByPaneId` variants with the
//!   captured origin pane (`CloseFocusByPaneId`, `ResizeByPaneId`,
//!   `ToggleFocusFullscreenByPaneId`, `ToggleFocusNoUiFullscreenByPaneId`,
//!   `TogglePaneEmbedOrFloatingByPaneId`, `MovePaneByPaneId`,
//!   `WriteToPaneId`/`WriteCharsToPaneId`). Origin pane IDs use the pinned
//!   text format (`terminal_<u32>`, `plugin_<u32>`, or bare `<u32>`).
//! - Directional pane focus cannot use `MoveFocus` (menu-relative), so it
//!   resolves bridge-side against the tracked inventory and geometry
//!   ([`FocusRequest::Neighbor`]); indexed pane focus resolves against the
//!   tracked manifest order ([`FocusRequest::ByIndex`]).
//! - Tab-scoped portables use tab-index variants (`GoToTab`, `RenameTab`).
//!   They execute against the focused tab, which the menu preserves: a Zellij
//!   `Run` pane opens in the origin tab and v1 never switches tabs while a
//!   menu is open.
//! - Session-scoped portables share the live session with the menu, so
//!   `RenameSession`, `SwitchSession`, `Detach`, and `KillSessions` need no
//!   pane targeting.
//!
//! ## Evidence notes
//!
//! - `menu:*`, `config:reload`, and `command:execute` are broker-owned on both
//!   hosts: no Zellij `Action` or shim equivalent exists (`config:reload` is not
//!   `shim::reconfigure`, which requires a Zellij config payload; `run_command*`
//!   is classified background/unsupported).
//! - `tab:create` without `workspace-id` uses host `NewTab`; Zellij 0.46 has no
//!   workspace concept, so a supplied `workspace-id` fails precisely.
//! - Bare `tab:rename` has no host prompt mapping: pinned `RenameTab` requires
//!   `tab_index: u32` plus `name: Vec<u8>`, and no prompt variant exists. The
//!   index comes from the immutable origin (`origin.tab.index`).
//! - `pane:zoom`, `pane:fullscreen`, and `pane:floating` map only their
//!   argument-less form to the matching `*ByPaneId` toggle; an explicit boolean
//!   cannot be honored idempotently and fails precisely.
//! - `pane:frame` has no pane-targeted primitive (`TogglePaneFrames` acts on
//!   the focused menu pane), so it fails precisely.
//! - `pane:resize` maps a missing amount to one `Resize::Increase` step; the
//!   pinned `Resize` type carries no magnitude, so a supplied amount fails precisely.
//! - `pane:move` with an index fails precisely: `MovePaneByPaneId` takes an
//!   optional direction whose `None` is not a positional destination, and no
//!   positional move primitive exists.
//! - `pane:swap` and `tab:swap` fail precisely: no swap primitive exists in the
//!   pinned inventory (only layout swaps, which reorder layouts, not panes).
//! - `tab:move` with an index fails precisely: both `MoveTab` and
//!   `MoveTabByTabId` are directional, and emulating a positional move with
//!   repeated directional moves is non-atomic.
//! - `session:kill` maps to `kill_sessions([origin.session])`, preserving the
//!   quit/kill distinction (`quit_zellij` terminates the whole server, so
//!   `session:quit` has no mapping). Killing the live session ends the broker
//!   with it; that is the portable semantic, stated loudly.
//! - `session:create` has no one-to-one plugin API.

use std::path::{Path, PathBuf};

use muxe_core::{
    ActionScalar, CanonicalKey, CommandWord, ConfigValueKind, ContextType, Direction,
    KeyboardAction, OriginContext, PaneAction, PortableAction, ResolvedCreateCommand,
    ResolvedKeyboardAction, ResolvedPaneAction, ResolvedPaneTarget, ResolvedPortableAction,
    ResolvedSessionAction, ResolvedTabAction, ResolvedTabTarget, SessionAction, SessionName,
    TabAction,
};
use muxe_zellij_protocol::generated::{RawNativeCommand, raw, validated};
use thiserror::Error;

use crate::keyboard::{KeyboardError, map_canonical_key};

/// Broker-owned actions need no host dispatch; host-mapped actions carry
/// generated raw mirrors the bridge revalidates before dispatch.
#[derive(Clone, Debug, PartialEq)]
pub enum PortableMapping {
    /// Handled entirely by the broker (menu state, config reload, supervised commands).
    BrokerOwned,
    /// Dispatch through the typed `run_action` path. A keyboard key list is
    /// concatenated into one `WriteToPaneId` command because schema v1 defines
    /// no timing or key-boundary semantics.
    HostAction {
        /// Generated raw mirrors; current portable mappings produce one command.
        commands: Vec<RawNativeCommand>,
    },
    /// Resolve bridge-side against the tracked pane inventory.
    BridgeFocus {
        /// Inventory-resolved focus operation.
        request: FocusRequest,
    },
}

/// Bridge-resolved focus operations for targets with no pinned primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FocusRequest {
    /// Focus the indexed pane of the active tab in manifest order.
    ByIndex {
        /// Position in the active tab's manifest pane order.
        index: u32,
    },
    /// Focus the nearest pane from the origin pane in one direction.
    Neighbor {
        /// Cardinal direction from the origin pane.
        direction: Cardinal,
    },
}

/// Cardinal host direction shared by focused moves, resizes, and neighbors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinal {
    /// Left.
    Left,
    /// Right.
    Right,
    /// Up.
    Up,
    /// Down.
    Down,
}

impl Cardinal {
    /// Parses a portable direction literal into a cardinal host direction.
    ///
    /// # Errors
    ///
    /// Returns [`PortableError`] when the scalar is not one of left, right, up,
    /// or down, or when a context marker reaches this point.
    pub fn parse(action: &'static str, scalar: &ActionScalar) -> Result<Self, PortableError> {
        match &scalar.value.kind {
            ConfigValueKind::String(text) => match text.as_str() {
                "left" => Ok(Self::Left),
                "right" => Ok(Self::Right),
                "up" => Ok(Self::Up),
                "down" => Ok(Self::Down),
                _ => Err(PortableError::InvalidScalar {
                    action,
                    parameter: "direction",
                    reason: "expected one of left, right, up, or down",
                }),
            },
            ConfigValueKind::Context(_) => Err(PortableError::UnresolvedContext {
                action,
                parameter: "direction",
            }),
            _ => Err(PortableError::InvalidScalar {
                action,
                parameter: "direction",
                reason: "expected a direction string",
            }),
        }
    }

    /// Maps a resolved direction, rejecting non-cardinal values at the host boundary.
    ///
    /// # Errors
    ///
    /// Returns [`PortableError::InvalidScalar`] for next or previous.
    pub const fn from_direction(
        action: &'static str,
        direction: Direction,
    ) -> Result<Self, PortableError> {
        match direction {
            Direction::Left => Ok(Self::Left),
            Direction::Right => Ok(Self::Right),
            Direction::Up => Ok(Self::Up),
            Direction::Down => Ok(Self::Down),
            Direction::Next | Direction::Previous => Err(PortableError::InvalidScalar {
                action,
                parameter: "direction",
                reason: "expected one of left, right, up, or down",
            }),
        }
    }

    /// Pinned mirror direction.
    #[must_use]
    pub const fn into_mirror(self) -> raw::Direction {
        match self {
            Self::Left => raw::Direction::Left,
            Self::Right => raw::Direction::Right,
            Self::Up => raw::Direction::Up,
            Self::Down => raw::Direction::Down,
        }
    }

    /// Pipe neighbor direction.
    #[must_use]
    pub const fn into_neighbor(self) -> muxe_zellij_protocol::NeighborDirection {
        use muxe_zellij_protocol::NeighborDirection;
        match self {
            Self::Left => NeighborDirection::Left,
            Self::Right => NeighborDirection::Right,
            Self::Up => NeighborDirection::Up,
            Self::Down => NeighborDirection::Down,
        }
    }
}

/// Portable mapping failure with an actionable reason.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PortableError {
    /// No pinned host primitive implements this portable action.
    #[error("portable {action} has no Zellij mapping: {reason}")]
    Incompatible {
        /// Portable action discriminator (for example `tab:swap`).
        action: &'static str,
        /// Pinned-source reason.
        reason: &'static str,
    },
    /// A supplied scalar cannot be interpreted for the target field.
    #[error("portable {action} parameter '{parameter}' is invalid: {reason}")]
    InvalidScalar {
        /// Portable action discriminator.
        action: &'static str,
        /// Parameter name.
        parameter: &'static str,
        /// What was wrong.
        reason: &'static str,
    },
    /// A config/schema marker cannot be accepted for the field's required type.
    #[error(
        "context reference in portable {action} parameter '{parameter}' is unresolved or incompatible with the parameter's required type"
    )]
    UnresolvedContext {
        /// Portable action discriminator.
        action: &'static str,
        /// Parameter name.
        parameter: &'static str,
    },
}

fn scalar_string(
    action: &'static str,
    parameter: &'static str,
    scalar: &ActionScalar,
) -> Result<String, PortableError> {
    match &scalar.value.kind {
        ConfigValueKind::String(text) => Ok(text.clone()),
        ConfigValueKind::Context(_) => Err(PortableError::UnresolvedContext { action, parameter }),
        _ => Err(PortableError::InvalidScalar {
            action,
            parameter,
            reason: "expected a string",
        }),
    }
}

fn scalar_index_u32(
    action: &'static str,
    parameter: &'static str,
    scalar: &ActionScalar,
) -> Result<u32, PortableError> {
    match &scalar.value.kind {
        ConfigValueKind::Integer(number) => {
            u32::try_from(*number).map_err(|_| PortableError::InvalidScalar {
                action,
                parameter,
                reason: "index must be an integer from 0 to 4294967295",
            })
        }
        ConfigValueKind::Context(_) => Err(PortableError::UnresolvedContext { action, parameter }),
        _ => Err(PortableError::InvalidScalar {
            action,
            parameter,
            reason: "expected a non-negative integer",
        }),
    }
}

fn scalar_bool(
    action: &'static str,
    parameter: &'static str,
    scalar: &ActionScalar,
) -> Result<bool, PortableError> {
    match &scalar.value.kind {
        ConfigValueKind::Boolean(flag) => Ok(*flag),
        ConfigValueKind::Context(_) => Err(PortableError::UnresolvedContext { action, parameter }),
        _ => Err(PortableError::InvalidScalar {
            action,
            parameter,
            reason: "expected a boolean",
        }),
    }
}

/// Accepts a context marker only when its path type fits the target field.
fn check_marker(
    action: &'static str,
    parameter: &'static str,
    scalar: &ActionScalar,
    allowed: &[ContextType],
    allow_context: bool,
) -> Result<(), PortableError> {
    if let ConfigValueKind::Context(reference) = &scalar.value.kind {
        if allow_context && allowed.contains(&reference.path.value_type()) {
            return Ok(());
        }
        return Err(PortableError::UnresolvedContext { action, parameter });
    }
    Ok(())
}

/// Parses an origin pane ID with the exact pinned text format
/// (`terminal_<u32>`, `plugin_<u32>`, or bare `<u32>` meaning terminal).
fn parse_origin_pane(text: &str) -> Option<raw::PaneId> {
    if let Some(number) = text.strip_prefix("terminal_") {
        return number.parse::<u32>().ok().map(raw::PaneId::Terminal);
    }
    if let Some(number) = text.strip_prefix("plugin_") {
        return number.parse::<u32>().ok().map(raw::PaneId::Plugin);
    }
    text.parse::<u32>().ok().map(raw::PaneId::Terminal)
}

/// Resolves the captured origin pane to its pinned mirror. A missing pane is
/// `context_unavailable`, never an implicit current pane.
fn origin_pane(action: &'static str, origin: &OriginContext) -> Result<raw::PaneId, PortableError> {
    let pane = origin.pane_id.as_ref().ok_or(PortableError::Incompatible {
        action,
        reason: "the captured origin has no pane, so the action's target is unavailable",
    })?;
    parse_origin_pane(pane.as_str()).ok_or(PortableError::InvalidScalar {
        action,
        parameter: "origin.pane.id",
        reason: "origin pane ID must be terminal_<n>, plugin_<n>, or a bare number, where n is an integer from 0 to 4294967295",
    })
}

fn wrap(action: raw::Action) -> PortableMapping {
    PortableMapping::HostAction {
        commands: vec![RawNativeCommand::RunAction {
            action,
            context: Vec::new(),
        }],
    }
}

fn keyboard_error(action: &'static str, error: &KeyboardError) -> PortableError {
    let KeyboardError::Unsupported { reason, .. } = error;
    PortableError::InvalidScalar {
        action,
        parameter: "keys",
        reason,
    }
}

/// Validates that a portable action is structurally mappable.
///
/// Concrete literals face their real range and literal constraints here
/// (index bounds, direction spellings, toggle types, key ★ table); context
/// markers are accepted only where the originating path type fits the target
/// field. No payload is constructed: validation proves constraints, never
/// substitutes dummy values.
///
/// # Errors
///
/// Returns [`PortableError`] for structurally unmappable actions; see the
/// variant reasons on [`PortableError`].
pub fn validate_portable_structure(action: &PortableAction) -> Result<(), PortableError> {
    match action {
        PortableAction::Menu(_) | PortableAction::Config(_) | PortableAction::Command(_) => Ok(()),
        PortableAction::Keyboard(keyboard) => validate_keyboard_structure(keyboard),
        PortableAction::Tab(tab) => validate_tab_structure(tab),
        PortableAction::Pane(pane) => validate_pane_structure(pane),
        PortableAction::Session(session) => validate_session_structure(session),
    }
}
/// Validates the keyboard subgroup structurally.
fn validate_keyboard_structure(keyboard: &KeyboardAction) -> Result<(), PortableError> {
    match keyboard {
        KeyboardAction::SendText(text) => check_concrete_string("keyboard:send", "text", text),
        KeyboardAction::SendKeys(keys) => {
            for key in keys {
                check_keyboard_key(key)?;
            }
            Ok(())
        }
    }
}

/// Validates one sendable key: markers pass through, literals face the key table.
fn check_keyboard_key(key: &ActionScalar) -> Result<(), PortableError> {
    check_marker("keyboard:send", "keys", key, &[ContextType::String], true)?;
    if is_marker(key) {
        return Ok(());
    }
    let text = scalar_string("keyboard:send", "keys", key)?;
    let key = CanonicalKey::parse(&text).map_err(|_| PortableError::InvalidScalar {
        action: "keyboard:send",
        parameter: "keys",
        reason: "invalid canonical key",
    })?;
    map_canonical_key(&key).map_err(|error| keyboard_error("keyboard:send", &error))?;
    Ok(())
}

/// Validates the tab subgroup structurally.
fn validate_tab_structure(tab: &TabAction) -> Result<(), PortableError> {
    match tab {
        TabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        } => {
            if workspace_id.is_some() {
                return Err(PortableError::Incompatible {
                    action: "tab:create",
                    reason: "Zellij 0.46 does not support workspaces; omit workspace-id to create a tab",
                });
            }
            if let Some(name) = name {
                check_concrete_string("tab:create", "name", name)?;
            }
            check_create_focus("tab:create", focus.as_ref())?;
            check_create_command("tab:create", command, true)
        }
        TabAction::Close => Ok(()),
        TabAction::Rename { name } => {
            let Some(name) = name else {
                return Err(PortableError::Incompatible {
                    action: "tab:rename",
                    reason: "Zellij requires a tab name and cannot prompt for one",
                });
            };
            check_concrete_string("tab:rename", "name", name)
        }
        TabAction::Focus(target) => match target {
            muxe_core::IndexOrDirection::Index(index) => {
                check_concrete_index("tab:focus", "index", index)
            }
            muxe_core::IndexOrDirection::Direction(direction) => {
                check_tab_focus_direction(direction)
            }
        },
        TabAction::Move(target) => match target {
            muxe_core::IndexOrDirection::Direction(direction) => {
                Cardinal::parse("tab:move", direction).map(|_| ())
            }
            muxe_core::IndexOrDirection::Index(_) => Err(PortableError::Incompatible {
                action: "tab:move",
                reason: "Zellij can move a tab by direction but cannot move it to a specified index",
            }),
        },
        TabAction::Swap(_) => Err(PortableError::Incompatible {
            action: "tab:swap",
            reason: "Zellij has no command that swaps two tabs atomically",
        }),
    }
}

/// Validates the pane subgroup structurally.
fn validate_pane_structure(pane: &PaneAction) -> Result<(), PortableError> {
    match pane {
        PaneAction::Create | PaneAction::Close => Ok(()),
        PaneAction::Split {
            direction,
            focus,
            command,
        } => {
            if let Some(direction) = direction {
                Cardinal::parse("pane:split", direction).map(|_| ())?;
            }
            check_create_focus("pane:split", focus.as_ref())?;
            check_create_command("pane:split", command, false)
        }
        PaneAction::Focus(target) => match target {
            muxe_core::IndexOrDirection::Direction(direction) => {
                Cardinal::parse("pane:focus", direction).map(|_| ())
            }
            muxe_core::IndexOrDirection::Index(index) => {
                check_concrete_index("pane:focus", "index", index)
            }
        },
        PaneAction::Move(target) => match target {
            muxe_core::IndexOrDirection::Direction(direction) => {
                Cardinal::parse("pane:move", direction).map(|_| ())
            }
            muxe_core::IndexOrDirection::Index(_) => Err(PortableError::Incompatible {
                action: "pane:move",
                reason: "Zellij can move a pane by direction but cannot move it to a specified index",
            }),
        },
        PaneAction::Swap(_) => Err(PortableError::Incompatible {
            action: "pane:swap",
            reason: "Zellij has no command that swaps two panes",
        }),
        PaneAction::Resize { direction, amount } => {
            if let Some(amount) = amount {
                check_marker("pane:resize", "amount", amount, &[], true)?;
                return Err(PortableError::Incompatible {
                    action: "pane:resize",
                    reason: "Zellij cannot resize by a specified amount; omit amount for one increase step",
                });
            }
            Cardinal::parse("pane:resize", direction).map(|_| ())
        }
        PaneAction::Zoom { enabled } => check_toggle("pane:zoom", enabled.as_ref()),
        PaneAction::Fullscreen { enabled } => check_toggle("pane:fullscreen", enabled.as_ref()),
        PaneAction::Floating { enabled } => check_toggle("pane:floating", enabled.as_ref()),
        PaneAction::Frame { .. } => Err(PortableError::Incompatible {
            action: "pane:frame",
            reason: "Zellij cannot change frames for a specified pane; its frame toggle acts on the focused menu pane",
        }),
    }
}

/// Validates the session subgroup structurally.
fn validate_session_structure(session: &SessionAction) -> Result<(), PortableError> {
    match session {
        SessionAction::Attach { name } | SessionAction::Switch { name } => {
            check_concrete_string("session:switch", "name", name)
        }
        SessionAction::Rename { name } => check_concrete_string("session:rename", "name", name),
        SessionAction::Detach | SessionAction::Kill => Ok(()),
        SessionAction::Create => Err(PortableError::Incompatible {
            action: "session:create",
            reason: "Zellij's plugin API has no command that creates a session",
        }),
        SessionAction::Quit => Err(PortableError::Incompatible {
            action: "session:quit",
            reason: "Zellij's quit command terminates the server and cannot implement portable session quit",
        }),
    }
}

fn check_create_focus(
    action: &'static str,
    focus: Option<&ActionScalar>,
) -> Result<(), PortableError> {
    if let Some(focus) = focus {
        scalar_bool(action, "focus", focus)?;
    }
    Ok(())
}

fn check_create_command(
    action: &'static str,
    command: &muxe_core::CreateCommand,
    cwd_without_program: bool,
) -> Result<(), PortableError> {
    if let Some(program) = &command.program {
        check_concrete_string(action, "program", program)?;
    }
    for argument in &command.args {
        check_concrete_string(action, "args", argument)?;
    }
    if let Some(cwd) = &command.cwd {
        check_marker(
            action,
            "cwd",
            cwd,
            &[ContextType::AbsolutePath, ContextType::String],
            true,
        )?;
        if !is_marker(cwd) {
            scalar_string(action, "cwd", cwd)?;
        }
        if command.program.is_none() && !cwd_without_program {
            return Err(PortableError::Incompatible {
                action,
                reason: "Zellij can set a split's working directory only when program is supplied",
            });
        }
    }
    if command.program.is_none() && !command.args.is_empty() {
        return Err(PortableError::InvalidScalar {
            action,
            parameter: "args",
            reason: "args requires a program",
        });
    }
    Ok(())
}

/// Accepts a context marker whose path type fits, else proves a concrete string.
fn check_concrete_string(
    action: &'static str,
    parameter: &'static str,
    scalar: &ActionScalar,
) -> Result<(), PortableError> {
    check_marker(action, parameter, scalar, &[ContextType::String], true)?;
    if is_marker(scalar) {
        return Ok(());
    }
    scalar_string(action, parameter, scalar).map(|_| ())
}

/// Accepts a context marker whose path type fits, else proves a concrete index.
fn check_concrete_index(
    action: &'static str,
    parameter: &'static str,
    scalar: &ActionScalar,
) -> Result<(), PortableError> {
    check_marker(
        action,
        parameter,
        scalar,
        &[ContextType::UnsignedInteger],
        true,
    )?;
    if is_marker(scalar) {
        return Ok(());
    }
    scalar_index_u32(action, parameter, scalar).map(|_| ())
}

fn is_marker(scalar: &ActionScalar) -> bool {
    matches!(scalar.value.kind, ConfigValueKind::Context(_))
}

fn check_toggle(action: &'static str, enabled: Option<&ActionScalar>) -> Result<(), PortableError> {
    if let Some(scalar) = enabled {
        // No boolean-typed context path exists, so any marker here is unresolvable.
        if is_marker(scalar) {
            return Err(PortableError::UnresolvedContext {
                action,
                parameter: "enabled",
            });
        }
        scalar_bool(action, "enabled", scalar)?;
        return Err(PortableError::Incompatible {
            action,
            reason: "Zellij can only toggle this setting; it cannot set enabled to an explicit true or false",
        });
    }
    Ok(())
}

fn check_tab_focus_direction(scalar: &ActionScalar) -> Result<(), PortableError> {
    match &scalar.value.kind {
        ConfigValueKind::String(text) => match text.as_str() {
            "next" | "previous" => Ok(()),
            "left" | "right" | "up" | "down" => Err(PortableError::Incompatible {
                action: "tab:focus",
                reason: "Zellij tab focus supports only index, next, and previous",
            }),
            _ => Err(PortableError::InvalidScalar {
                action: "tab:focus",
                parameter: "direction",
                reason: "expected next or previous for tab focus",
            }),
        },
        ConfigValueKind::Context(_) => Err(PortableError::UnresolvedContext {
            action: "tab:focus",
            parameter: "direction",
        }),
        _ => Err(PortableError::InvalidScalar {
            action: "tab:focus",
            parameter: "direction",
            reason: "expected a direction string",
        }),
    }
}

/// Maps an owned, resolved execution action against the immutable captured origin.
///
/// # Errors
///
/// Returns [`PortableError`] for unsupported pinned host forms or missing origin context.
pub fn map_portable(
    action: &ResolvedPortableAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match action {
        ResolvedPortableAction::Menu(_)
        | ResolvedPortableAction::Config(_)
        | ResolvedPortableAction::Command(_) => Ok(PortableMapping::BrokerOwned),
        ResolvedPortableAction::Keyboard(keyboard) => map_keyboard_action(keyboard, origin),
        ResolvedPortableAction::Tab(tab) => map_tab_action(tab, origin),
        ResolvedPortableAction::Pane(pane) => map_pane_action(pane, origin),
        ResolvedPortableAction::Session(session) => map_session_action(session, origin),
    }
}

/// Maps a focused creation after the UI pane is absent. Tiled creation selects
/// the captured client with `near_current_pane: false`, never the bridge pane.
///
/// # Errors
///
/// Returns [`PortableError`] for an unsupported creation or focus=false.
pub fn map_post_dismissal_creation(
    action: &ResolvedPortableAction,
    origin: &OriginContext,
) -> Result<RawNativeCommand, PortableError> {
    match action {
        ResolvedPortableAction::Tab(tab @ ResolvedTabAction::Create { focus, .. }) => {
            if !focus.unwrap_or(true) {
                return Err(PortableError::Incompatible {
                    action: "tab:create",
                    reason: "post-dismissal dispatch requires focus=true",
                });
            }
            let PortableMapping::HostAction { mut commands } = map_tab_action(tab, origin)? else {
                unreachable!("tab:create always maps to one host action")
            };
            commands.pop().ok_or(PortableError::Incompatible {
                action: "tab:create",
                reason: "tab:create produced no host action",
            })
        }
        ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
            direction,
            focus,
            command,
        }) => {
            if !focus.unwrap_or(true) {
                return Err(PortableError::Incompatible {
                    action: "pane:split",
                    reason: "post-dismissal dispatch requires focus=true",
                });
            }
            map_split(command, *direction, false, true).map(|action| RawNativeCommand::RunAction {
                action,
                context: Vec::new(),
            })
        }
        _ => Err(PortableError::Incompatible {
            action: "creation",
            reason: "post-dismissal dispatch is only valid for tab:create or pane:split",
        }),
    }
}

/// Whether a portable creation must be held until the Muxe UI is gone.
///
/// This performs no host mapping: the ordinary adapter path rejects focused
/// creations so focus-relative creation cannot originate from the bridge pane.
#[must_use]
pub fn creation_requires_post_dismissal(action: &ResolvedPortableAction) -> bool {
    match action {
        ResolvedPortableAction::Tab(ResolvedTabAction::Create { focus, .. })
        | ResolvedPortableAction::Pane(ResolvedPaneAction::Split { focus, .. }) => {
            focus.unwrap_or(true)
        }
        _ => false,
    }
}

/// Maps parsed keyboard actions against the origin pane.
fn map_keyboard_action(
    keyboard: &ResolvedKeyboardAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match keyboard {
        ResolvedKeyboardAction::SendText(text) => {
            let pane = origin_pane("keyboard:send", origin)?;
            Ok(wrap(raw::Action::WriteCharsToPaneId {
                chars: text.clone(),
                pane_id: pane,
            }))
        }
        ResolvedKeyboardAction::SendKeys(keys) => {
            if keys.is_empty() {
                return Err(PortableError::InvalidScalar {
                    action: "keyboard:send",
                    parameter: "keys",
                    reason: "expected at least one key",
                });
            }
            let pane = origin_pane("keyboard:send", origin)?;
            let mut bytes = Vec::new();
            for key in keys {
                let mapped = map_canonical_key(key)
                    .map_err(|error| keyboard_error("keyboard:send", &error))?;
                bytes.extend_from_slice(&mapped.bytes);
            }
            Ok(wrap(raw::Action::WriteToPaneId {
                bytes,
                pane_id: pane,
            }))
        }
    }
}

fn pinned_index(action: &'static str, index: u64) -> Result<u32, PortableError> {
    u32::try_from(index).map_err(|_| PortableError::InvalidScalar {
        action,
        parameter: "index",
        reason: "index must be an integer from 0 to 4294967295",
    })
}

/// Maps tab actions; rename uses the immutable origin index.
fn map_tab_action(
    tab: &ResolvedTabAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match tab {
        ResolvedTabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        } => {
            if workspace_id.is_some() {
                return Err(PortableError::Incompatible {
                    action: "tab:create",
                    reason: "Zellij 0.46 does not support workspaces; omit workspace-id to create a tab",
                });
            }
            let (command, cwd) = map_create_command("tab:create", command)?;
            Ok(wrap(raw::Action::NewTab {
                tiled_layout: None,
                floating_layouts: Vec::new(),
                swap_tiled_layouts: None,
                swap_floating_layouts: None,
                tab_name: name.clone(),
                should_change_focus_to_new_tab: focus.unwrap_or(true),
                cwd: cwd.map(Path::to_path_buf),
                initial_panes: command.map(|command| vec![raw::CommandOrPlugin::Command(command)]),
                first_pane_unblock_condition: None,
            }))
        }
        ResolvedTabAction::Close => Ok(wrap(raw::Action::CloseTab)),
        ResolvedTabAction::Rename { name } => {
            let Some(name) = name else {
                return Err(PortableError::Incompatible {
                    action: "tab:rename",
                    reason: "Zellij requires a tab name and cannot prompt for one",
                });
            };
            let Some(tab_index) = origin.tab_index else {
                return Err(PortableError::Incompatible {
                    action: "tab:rename",
                    reason: "the captured origin has no tab index, so the tab to rename is unavailable",
                });
            };
            let tab_index = u32::try_from(tab_index).map_err(|_| PortableError::InvalidScalar {
                action: "tab:rename",
                parameter: "origin.tab.index",
                reason: "tab index must be an integer from 0 to 4294967295",
            })?;
            Ok(wrap(raw::Action::RenameTab {
                tab_index,
                name: name.as_bytes().to_vec(),
            }))
        }
        ResolvedTabAction::Focus(target) => match target {
            ResolvedTabTarget::Index(index) => Ok(wrap(raw::Action::GoToTab {
                index: pinned_index("tab:focus", index.get())?,
            })),
            ResolvedTabTarget::Direction(Direction::Next) => Ok(wrap(raw::Action::GoToNextTab)),
            ResolvedTabTarget::Direction(Direction::Previous) => {
                Ok(wrap(raw::Action::GoToPreviousTab))
            }
            ResolvedTabTarget::Direction(_) => Err(PortableError::Incompatible {
                action: "tab:focus",
                reason: "Zellij tab focus supports only index, next, and previous",
            }),
        },
        ResolvedTabAction::Move(target) => match target {
            ResolvedTabTarget::Direction(direction) => Ok(wrap(raw::Action::MoveTab {
                direction: Cardinal::from_direction("tab:move", *direction)?.into_mirror(),
            })),
            ResolvedTabTarget::Index(_) => Err(PortableError::Incompatible {
                action: "tab:move",
                reason: "Zellij can move a tab by direction but cannot move it to a specified index",
            }),
        },
        ResolvedTabAction::Swap(_) => Err(PortableError::Incompatible {
            action: "tab:swap",
            reason: "Zellij has no command that swaps two tabs atomically",
        }),
    }
}

fn command_text<'a>(
    action: &'static str,
    parameter: &'static str,
    word: &'a CommandWord,
) -> Result<&'a str, PortableError> {
    word.as_os_str()
        .to_str()
        .ok_or(PortableError::InvalidScalar {
            action,
            parameter,
            reason: "path cannot be represented as UTF-8 text",
        })
}

fn map_create_command<'a>(
    action: &'static str,
    command: &'a ResolvedCreateCommand,
) -> Result<(Option<raw::RunCommandAction>, Option<&'a Path>), PortableError> {
    let cwd = command
        .cwd
        .as_ref()
        .map(|cwd| {
            let path = cwd.as_path();
            path.to_str().ok_or(PortableError::InvalidScalar {
                action,
                parameter: "cwd",
                reason: "path cannot be represented as UTF-8 text",
            })?;
            Ok::<_, PortableError>(path)
        })
        .transpose()?;
    let Some(program) = &command.program else {
        if command.args.is_empty() {
            return Ok((None, cwd));
        }
        return Err(PortableError::InvalidScalar {
            action,
            parameter: "args",
            reason: "args requires a program",
        });
    };
    command_text(action, "program", program)?;
    let command = raw::RunCommandAction {
        command: PathBuf::from(program.as_os_str()),
        args: command
            .args
            .iter()
            .map(|argument| command_text(action, "args", argument).map(str::to_owned))
            .collect::<Result<_, _>>()?,
        cwd: cwd.map(Path::to_path_buf),
        direction: None,
        hold_on_close: false,
        hold_on_start: false,
        originating_plugin: None,
        use_terminal_title: false,
    };
    Ok((Some(command), cwd))
}

fn map_split(
    command: &ResolvedCreateCommand,
    direction: Option<Direction>,
    near_current_pane: bool,
    focus: bool,
) -> Result<raw::Action, PortableError> {
    let (command, cwd) = map_create_command("pane:split", command)?;
    if command.is_none() && cwd.is_some() {
        return Err(PortableError::Incompatible {
            action: "pane:split",
            reason: "Zellij can set a split's working directory only when program is supplied",
        });
    }
    if !focus {
        return Err(PortableError::Incompatible {
            action: "pane:split",
            reason: "Zellij cannot place an unfocused split against the captured origin while the Muxe UI remains open",
        });
    }
    let direction = direction
        .map(|direction| {
            Cardinal::from_direction("pane:split", direction).map(Cardinal::into_mirror)
        })
        .transpose()?;
    Ok(raw::Action::NewTiledPane {
        direction,
        command,
        pane_name: None,
        near_current_pane,
        no_focus: false,
        borderless: None,
        border_style: None,
        tab_id: None,
    })
}

/// Maps pane creation and immutable-origin-targeted operations.
fn map_pane_action(
    pane: &ResolvedPaneAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match pane {
        ResolvedPaneAction::Create => Ok(wrap(raw::Action::NewPane {
            direction: None,
            pane_name: None,
            start_suppressed: false,
        })),
        ResolvedPaneAction::Split {
            direction,
            focus,
            command,
        } => Ok(wrap(map_split(
            command,
            *direction,
            true,
            focus.unwrap_or(true),
        )?)),
        ResolvedPaneAction::Close => Ok(wrap(raw::Action::CloseFocusByPaneId {
            pane_id: origin_pane("pane:close", origin)?,
        })),
        ResolvedPaneAction::Focus(target) => match target {
            ResolvedPaneTarget::Direction(direction) => Ok(PortableMapping::BridgeFocus {
                request: FocusRequest::Neighbor {
                    direction: Cardinal::from_direction("pane:focus", *direction)?,
                },
            }),
            ResolvedPaneTarget::Index(index) => Ok(PortableMapping::BridgeFocus {
                request: FocusRequest::ByIndex {
                    index: pinned_index("pane:focus", index.get())?,
                },
            }),
        },
        ResolvedPaneAction::Move(target) => match target {
            ResolvedPaneTarget::Direction(direction) => {
                let cardinal = Cardinal::from_direction("pane:move", *direction)?;
                Ok(wrap(raw::Action::MovePaneByPaneId {
                    pane_id: origin_pane("pane:move", origin)?,
                    direction: Some(cardinal.into_mirror()),
                }))
            }
            ResolvedPaneTarget::Index(_) => Err(PortableError::Incompatible {
                action: "pane:move",
                reason: "Zellij can move a pane by direction but cannot move it to a specified index",
            }),
        },
        ResolvedPaneAction::Swap(_) => Err(PortableError::Incompatible {
            action: "pane:swap",
            reason: "Zellij has no command that swaps two panes",
        }),
        ResolvedPaneAction::Resize { direction, amount } => {
            if amount.is_some() {
                return Err(PortableError::Incompatible {
                    action: "pane:resize",
                    reason: "Zellij cannot resize by a specified amount; omit amount for one increase step",
                });
            }
            let cardinal = Cardinal::from_direction("pane:resize", *direction)?;
            Ok(wrap(raw::Action::ResizeByPaneId {
                pane_id: origin_pane("pane:resize", origin)?,
                resize: raw::Resize::Increase,
                direction: Some(cardinal.into_mirror()),
            }))
        }
        ResolvedPaneAction::Zoom { enabled } => map_toggle("pane:zoom", *enabled, origin, |pane| {
            raw::Action::ToggleFocusFullscreenByPaneId { pane_id: pane }
        }),
        ResolvedPaneAction::Fullscreen { enabled } => {
            map_toggle("pane:fullscreen", *enabled, origin, |pane| {
                raw::Action::ToggleFocusNoUiFullscreenByPaneId { pane_id: pane }
            })
        }
        ResolvedPaneAction::Floating { enabled } => {
            map_toggle("pane:floating", *enabled, origin, |pane| {
                raw::Action::TogglePaneEmbedOrFloatingByPaneId { pane_id: pane }
            })
        }
        ResolvedPaneAction::Frame { .. } => Err(PortableError::Incompatible {
            action: "pane:frame",
            reason: "Zellij cannot change frames for a specified pane; its frame toggle acts on the focused menu pane",
        }),
    }
}

/// Maps sessions; kill targets the immutable captured live session.
fn map_session_action(
    session: &ResolvedSessionAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match session {
        ResolvedSessionAction::Attach { name } | ResolvedSessionAction::Switch { name } => {
            Ok(switch_session(name))
        }
        ResolvedSessionAction::Rename { name } => Ok(wrap(raw::Action::RenameSession {
            name: name.as_str().to_owned(),
        })),
        ResolvedSessionAction::Detach => Ok(wrap(raw::Action::Detach)),
        ResolvedSessionAction::Kill => {
            let session = origin
                .session_id
                .as_ref()
                .ok_or(PortableError::Incompatible {
                    action: "session:kill",
                    reason: "the captured origin has no session name, so the action's target is unavailable",
                })?;
            Ok(PortableMapping::HostAction {
                commands: vec![RawNativeCommand::KillSessions {
                    session_names: vec![session.as_str().to_owned()],
                }],
            })
        }
        ResolvedSessionAction::Create => Err(PortableError::Incompatible {
            action: "session:create",
            reason: "Zellij's plugin API has no command that creates a session",
        }),
        ResolvedSessionAction::Quit => Err(PortableError::Incompatible {
            action: "session:quit",
            reason: "Zellij's quit command terminates the server and cannot implement portable session quit",
        }),
    }
}

/// Unwraps the session name only at the pinned wire boundary.
fn switch_session(name: &SessionName) -> PortableMapping {
    wrap(raw::Action::SwitchSession {
        name: name.as_str().to_owned(),
        tab_position: None,
        pane_id: None,
        layout: None,
        cwd: None,
    })
}

fn map_toggle(
    action: &'static str,
    enabled: Option<bool>,
    origin: &OriginContext,
    build: impl FnOnce(raw::PaneId) -> raw::Action,
) -> Result<PortableMapping, PortableError> {
    if enabled.is_some() {
        return Err(PortableError::Incompatible {
            action,
            reason: "Zellij can only toggle this setting; it cannot set enabled to an explicit true or false",
        });
    }
    Ok(wrap(build(origin_pane(action, origin)?)))
}

/// Converts a mapped host action into its validated mirror, taking ownership.
///
/// Callers that retain no raw form use this; the dispatch path validates
/// through the same generated conversion at its own ownership boundary.
///
/// # Errors
///
/// Returns the generated [`ValidationError`](muxe_zellij_protocol::generated::ValidationError)
/// when the raw mirror fails generated conversion.
pub fn to_validated_action(
    raw_action: raw::Action,
) -> Result<validated::Action, muxe_zellij_protocol::generated::ValidationError> {
    raw_action.try_into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_core::{ConfigValue, SourceId, SourceSpan};

    fn span() -> SourceSpan {
        SourceSpan::new(SourceId::new("<test>"), 0, 1)
    }

    fn scalar(kind: ConfigValueKind) -> ActionScalar {
        ActionScalar::new(ConfigValue { span: span(), kind })
    }

    fn text(value: &str) -> ActionScalar {
        scalar(ConfigValueKind::String(value.to_owned()))
    }

    fn integer(value: i64) -> ActionScalar {
        scalar(ConfigValueKind::Integer(value))
    }

    fn marker(path: &str) -> ActionScalar {
        scalar(ConfigValueKind::Context(
            muxe_core::ContextReference::parse(path, span()).expect("valid path"),
        ))
    }

    /// Production-shaped captured origin: session, tab id, and tab index all
    /// arrive in the bridge frame (`ZellijOrigin.session_name`,
    /// `active_tab_id`, `active_tab_index`) — never hand-filled after capture.
    /// Values mirror `origin::snapshot_builds_immutable_origin`.
    fn production_captured_origin() -> OriginContext {
        crate::origin::build_origin_context(
            &muxe_zellij_protocol::ZellijOrigin {
                client_id: "client-1".to_owned(),
                session_name: Some("session-alpha".to_owned()),
                active_tab_index: Some(3),
                active_tab_id: Some(11),
                prior_pane_id: Some("terminal_4".to_owned()),
                ui_pane_id: "plugin-9".to_owned(),
                prior_pane_cwd: None,
                prior_pane_is_plugin: Some(false),
            },
            &muxe_adapter_api::HostDiscoveryKey::parse("session-alpha")
                .expect("test discovery key"),
            &muxe_adapter_api::LiveServerIncarnationId::parse("incarnation-alpha")
                .expect("test incarnation"),
            "plugin-9",
            None,
            None,
            None,
        )
        .expect("production-shaped snapshot builds")
    }

    fn test_origin() -> OriginContext {
        production_captured_origin()
    }

    #[test]
    fn production_captured_origin_resolves_tab_and_session_operations() {
        let origin = production_captured_origin();
        assert_eq!(
            origin.session_id.as_ref().map(muxe_core::SessionId::as_str),
            Some("session-alpha")
        );
        assert_eq!(origin.tab_index, Some(3));
        assert_eq!(
            origin.tab_id.as_ref().map(muxe_core::TabId::as_str),
            Some("11")
        );
        // Named tab rename resolves its index from the captured origin.
        let PortableMapping::HostAction { commands } = map_portable(
            &ResolvedPortableAction::Tab(ResolvedTabAction::Rename {
                name: Some("logs".to_owned()),
            }),
            &origin,
        )
        .expect("captured origin resolves rename") else {
            panic!("expected host action");
        };
        assert!(matches!(
            &commands[..],
            [RawNativeCommand::RunAction { action, .. }]
                if matches!(action, raw::Action::RenameTab { tab_index: 3, .. })
        ));
        // Session kill resolves its target from the captured origin.
        let PortableMapping::HostAction { commands } = map_portable(
            &ResolvedPortableAction::Session(ResolvedSessionAction::Kill),
            &origin,
        )
        .expect("captured origin resolves kill") else {
            panic!("expected host action");
        };
        assert!(matches!(
            &commands[..],
            [RawNativeCommand::KillSessions { session_names }]
                if session_names == &vec!["session-alpha".to_owned()]
        ));
    }

    fn map(action: &PortableAction) -> Result<PortableMapping, PortableError> {
        let origin = test_origin();
        map_portable(
            &action
                .resolve_context(&origin)
                .expect("valid resolved input"),
            &origin,
        )
    }

    fn single_host(action: &PortableAction) -> raw::Action {
        let PortableMapping::HostAction { commands } = map(action).expect("maps") else {
            panic!("expected host action");
        };
        assert_eq!(commands.len(), 1, "expected one command");
        let RawNativeCommand::RunAction { action, .. } = commands.into_iter().next().expect("one")
        else {
            panic!("expected run-action wrap");
        };
        action
    }

    #[test]
    fn broker_owned_actions_need_no_host() {
        assert_eq!(
            map(&PortableAction::Menu(muxe_core::MenuAction::Quit)),
            Ok(PortableMapping::BrokerOwned)
        );
        assert_eq!(
            map(&PortableAction::Config(muxe_core::ConfigAction::Reload)),
            Ok(PortableMapping::BrokerOwned)
        );
        assert_eq!(
            map(&PortableAction::Command(muxe_core::CommandAction {
                program: text("cargo"),
                args: Vec::new(),
                cwd: None,
                env: std::collections::BTreeMap::new(),
            })),
            Ok(PortableMapping::BrokerOwned)
        );
        assert!(
            validate_portable_structure(&PortableAction::Menu(muxe_core::MenuAction::Quit)).is_ok()
        );
    }

    #[test]
    fn pane_actions_target_the_origin_pane() {
        let action = single_host(&PortableAction::Pane(PaneAction::Close));
        assert!(matches!(
            action,
            raw::Action::CloseFocusByPaneId {
                pane_id: raw::PaneId::Terminal(4)
            }
        ));

        let action = single_host(&PortableAction::Pane(PaneAction::Resize {
            direction: text("right"),
            amount: None,
        }));
        assert!(matches!(
            action,
            raw::Action::ResizeByPaneId {
                pane_id: raw::PaneId::Terminal(4),
                resize: raw::Resize::Increase,
                ..
            }
        ));

        let action = single_host(&PortableAction::Pane(PaneAction::Zoom { enabled: None }));
        assert!(matches!(
            action,
            raw::Action::ToggleFocusFullscreenByPaneId { .. }
        ));
    }

    #[test]
    fn missing_origin_pane_fails_closed_with_context_unavailable() {
        // This is the adapter boundary shape emitted by the bridge when the
        // requesting UI pane is also the only adopted pane: prior_pane_id
        // (and therefore the resolved origin pane) is absent.
        let mut origin = test_origin();
        origin.pane_id = None;

        let error = map_portable(
            &ResolvedPortableAction::Pane(ResolvedPaneAction::Close),
            &origin,
        )
        .expect_err("missing origin pane must not target the UI pane");

        assert!(matches!(
            error,
            PortableError::Incompatible {
                action: "pane:close",
                ..
            }
        ));
    }

    #[test]
    fn keyboard_targets_the_origin_pane() {
        let action = single_host(&PortableAction::Keyboard(KeyboardAction::SendText(text(
            "hi",
        ))));
        assert!(matches!(
            action,
            raw::Action::WriteCharsToPaneId {
                pane_id: raw::PaneId::Terminal(4),
                ..
            }
        ));

        let PortableMapping::HostAction { commands } =
            map(&PortableAction::Keyboard(KeyboardAction::SendKeys(vec![
                text("a"),
                text("ctrl+c"),
            ])))
            .expect("key sequence maps")
        else {
            panic!("expected host action");
        };
        assert_eq!(commands.len(), 1);
        let RawNativeCommand::RunAction { action, .. } = &commands[0] else {
            panic!("expected run action");
        };
        assert!(matches!(
            action,
            raw::Action::WriteToPaneId { bytes, .. } if bytes == b"a\x03"
        ));
    }
    #[test]
    fn unmappable_keys_fail_precisely() {
        let error = map(&PortableAction::Keyboard(KeyboardAction::SendKeys(vec![
            text("super+a"),
        ])))
        .expect_err("super has no byte encoding");
        assert!(matches!(error, PortableError::InvalidScalar { .. }));
        assert!(
            validate_portable_structure(&PortableAction::Keyboard(KeyboardAction::SendKeys(vec![
                text("super+a")
            ])))
            .is_err()
        );
    }

    #[test]
    fn tab_create_without_workspace_uses_host_new_tab() {
        let action = single_host(&PortableAction::Tab(TabAction::Create {
            workspace_id: None,
            name: None,
            focus: None,
            command: muxe_core::CreateCommand::default(),
        }));
        assert!(matches!(action, raw::Action::NewTab { .. }));
        for action in [
            PortableAction::Tab(TabAction::Create {
                workspace_id: Some(text("ws")),
                name: None,
                focus: None,
                command: muxe_core::CreateCommand::default(),
            }),
            PortableAction::Tab(TabAction::Create {
                workspace_id: Some(marker("origin.workspace.id")),
                name: None,
                focus: None,
                command: muxe_core::CreateCommand::default(),
            }),
        ] {
            let mut origin = test_origin();
            origin.workspace_id = Some(muxe_core::WorkspaceId::new("ws"));
            let resolved = action
                .resolve_context(&origin)
                .expect("workspace input resolves");
            assert!(matches!(
                map_portable(&resolved, &origin),
                Err(PortableError::Incompatible {
                    action: "tab:create",
                    ..
                })
            ));
            assert!(validate_portable_structure(&action).is_err());
        }
    }

    #[test]
    fn only_focused_creations_require_post_dismissal_dispatch() {
        let focused = PortableAction::Tab(TabAction::Create {
            workspace_id: None,
            name: None,
            focus: None,
            command: muxe_core::CreateCommand::default(),
        });
        let unfocused = PortableAction::Pane(PaneAction::Split {
            direction: Some(text("right")),
            focus: Some(scalar(ConfigValueKind::Boolean(false))),
            command: muxe_core::CreateCommand::default(),
        });

        let origin = test_origin();
        assert!(creation_requires_post_dismissal(
            &focused
                .resolve_context(&origin)
                .expect("focused input resolves")
        ));
        assert!(!creation_requires_post_dismissal(
            &unfocused
                .resolve_context(&origin)
                .expect("unfocused input resolves")
        ));
    }

    #[test]
    fn post_dismissal_split_uses_origin_client_and_exact_command_fields() {
        let command = muxe_core::CreateCommand {
            program: Some(text("tool")),
            args: vec![text("--literal"), text("two words")],
            cwd: Some(text("/workspace")),
        };
        let raw = map_post_dismissal_creation(
            &PortableAction::Pane(PaneAction::Split {
                direction: Some(text("right")),
                focus: Some(scalar(ConfigValueKind::Boolean(true))),
                command,
            })
            .resolve_context(&test_origin())
            .expect("split input resolves"),
            &test_origin(),
        )
        .expect("focused split maps after dismissal");
        let RawNativeCommand::RunAction {
            action:
                raw::Action::NewTiledPane {
                    direction,
                    command: Some(command),
                    near_current_pane,
                    no_focus,
                    ..
                },
            ..
        } = raw
        else {
            panic!("expected a tiled pane action");
        };
        assert_eq!(direction, Some(raw::Direction::Right));
        assert!(!near_current_pane);
        assert!(!no_focus);
        assert_eq!(command.command, PathBuf::from("tool"));
        assert_eq!(command.args, vec!["--literal", "two words"]);
        assert_eq!(command.cwd, Some(PathBuf::from("/workspace")));
    }

    #[test]
    fn rename_uses_origin_index_or_fails() {
        let error = map(&PortableAction::Tab(TabAction::Rename { name: None }))
            .expect_err("bare rename has no prompt mapping");
        assert!(matches!(
            error,
            PortableError::Incompatible {
                action: "tab:rename",
                ..
            }
        ));

        let action = single_host(&PortableAction::Tab(TabAction::Rename {
            name: Some(text("logs")),
        }));
        let validated: validated::Action = action
            .try_into()
            .expect("generated conversion accepts rename");
        assert!(matches!(
            validated,
            validated::Action::RenameTab { tab_index: 3, .. }
        ));

        // Negative path derives from the production shape with no TabUpdate
        // observed: the bridge frame carries active_tab_index None, so capture
        // leaves tab_index None without any post-capture mutation.
        let untabbed = crate::origin::build_origin_context(
            &muxe_zellij_protocol::ZellijOrigin {
                client_id: "client-1".to_owned(),
                session_name: Some("session-alpha".to_owned()),
                active_tab_index: None,
                active_tab_id: None,
                prior_pane_id: Some("terminal_4".to_owned()),
                ui_pane_id: "plugin-9".to_owned(),
                prior_pane_cwd: None,
                prior_pane_is_plugin: Some(false),
            },
            &muxe_adapter_api::HostDiscoveryKey::parse("session-alpha")
                .expect("test discovery key"),
            &muxe_adapter_api::LiveServerIncarnationId::parse("incarnation-alpha")
                .expect("test incarnation"),
            "plugin-9",
            None,
            None,
            None,
        )
        .expect("untabbed snapshot builds");
        assert_eq!(untabbed.tab_index, None);
        let error = map_portable(
            &ResolvedPortableAction::Tab(ResolvedTabAction::Rename {
                name: Some("logs".to_owned()),
            }),
            &untabbed,
        )
        .expect_err("missing origin index fails");
        assert!(matches!(
            error,
            PortableError::Incompatible {
                action: "tab:rename",
                ..
            }
        ));
    }

    #[test]
    fn focus_resolves_bridge_side() {
        assert_eq!(
            map(&PortableAction::Pane(PaneAction::Focus(
                muxe_core::IndexOrDirection::Index(integer(1))
            ))),
            Ok(PortableMapping::BridgeFocus {
                request: FocusRequest::ByIndex { index: 1 }
            })
        );
        assert_eq!(
            map(&PortableAction::Pane(PaneAction::Focus(
                muxe_core::IndexOrDirection::Direction(text("left"))
            ))),
            Ok(PortableMapping::BridgeFocus {
                request: FocusRequest::Neighbor {
                    direction: Cardinal::Left
                }
            })
        );
    }

    #[test]
    fn literal_ranges_are_checked_at_validation() {
        let oversized = PortableAction::Tab(TabAction::Focus(muxe_core::IndexOrDirection::Index(
            integer(i64::from(u32::MAX) + 1),
        )));
        assert!(matches!(
            validate_portable_structure(&oversized),
            Err(PortableError::InvalidScalar { .. })
        ));
        assert!(map(&oversized).is_err());

        let bad_direction = PortableAction::Tab(TabAction::Focus(
            muxe_core::IndexOrDirection::Direction(text("sideways")),
        ));
        assert!(validate_portable_structure(&bad_direction).is_err());

        // Fitting markers resolve before concrete mapping.
        let marked = PortableAction::Tab(TabAction::Focus(muxe_core::IndexOrDirection::Index(
            marker("origin.tab.index"),
        )));
        assert!(validate_portable_structure(&marked).is_ok());
        assert_eq!(single_host(&marked), raw::Action::GoToTab { index: 3 });

        // Mismatched marker types fail validation without guessing.
        let mismatched = PortableAction::Tab(TabAction::Focus(muxe_core::IndexOrDirection::Index(
            marker("origin.pane.id"),
        )));
        assert!(matches!(
            validate_portable_structure(&mismatched),
            Err(PortableError::UnresolvedContext { .. })
        ));
    }

    #[test]
    fn toggle_and_frame_rules_hold() {
        assert!(matches!(
            single_host(&PortableAction::Pane(PaneAction::Zoom { enabled: None })),
            raw::Action::ToggleFocusFullscreenByPaneId {
                pane_id: raw::PaneId::Terminal(4)
            }
        ));
        let error = map(&PortableAction::Pane(PaneAction::Zoom {
            enabled: Some(scalar(ConfigValueKind::Boolean(true))),
        }))
        .expect_err("explicit zoom boolean is incompatible");
        assert!(matches!(error, PortableError::Incompatible { .. }));

        let error = map(&PortableAction::Pane(PaneAction::Frame { visible: None }))
            .expect_err("frame has no pane-targeted primitive");
        assert!(matches!(
            error,
            PortableError::Incompatible {
                action: "pane:frame",
                ..
            }
        ));

        let error = map(&PortableAction::Pane(PaneAction::Move(
            muxe_core::IndexOrDirection::Index(integer(2)),
        )))
        .expect_err("indexed move has no primitive");
        assert!(matches!(
            error,
            PortableError::Incompatible {
                action: "pane:move",
                ..
            }
        ));
    }

    #[test]
    fn session_lifecycle_maps_with_distinctions() {
        assert_eq!(
            single_host(&PortableAction::Session(SessionAction::Switch {
                name: text("dev")
            })),
            raw::Action::SwitchSession {
                name: "dev".to_owned(),
                tab_position: None,
                pane_id: None,
                layout: None,
                cwd: None,
            }
        );
        assert_eq!(
            single_host(&PortableAction::Session(SessionAction::Detach)),
            raw::Action::Detach
        );

        let PortableMapping::HostAction { commands } =
            map(&PortableAction::Session(SessionAction::Kill)).expect("kill maps")
        else {
            panic!("expected host action");
        };
        assert!(matches!(
            &commands[..],
            [RawNativeCommand::KillSessions { session_names }] if session_names == &vec!["session-alpha".to_owned()]
        ));

        for (action, name) in [
            (
                PortableAction::Session(SessionAction::Create),
                "session:create",
            ),
            (PortableAction::Session(SessionAction::Quit), "session:quit"),
        ] {
            let error = map(&action).expect_err("genuine gap fails");
            assert!(
                matches!(&error, PortableError::Incompatible { action, .. } if *action == name)
            );
            assert!(validate_portable_structure(&action).is_err());
        }
    }

    #[test]
    fn resolved_indices_keep_full_width_until_pinned_bounds() {
        let mut origin = test_origin();
        for index in [u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX] {
            origin.tab_index = Some(index);
            let input = PortableAction::Tab(TabAction::Focus(muxe_core::IndexOrDirection::Index(
                marker("origin.tab.index"),
            )));
            let action = input
                .resolve_context(&origin)
                .expect("unsigned origin index resolves");
            let ResolvedPortableAction::Tab(ResolvedTabAction::Focus(ResolvedTabTarget::Index(
                resolved,
            ))) = &action
            else {
                panic!("expected resolved tab index");
            };
            assert_eq!(resolved.get(), index);
            let pane = ResolvedPortableAction::Pane(ResolvedPaneAction::Focus(
                ResolvedPaneTarget::Index(muxe_core::PaneIndex::new(index)),
            ));
            if index == u64::from(u32::MAX) {
                assert_eq!(
                    map_portable(&action, &origin),
                    Ok(wrap(raw::Action::GoToTab { index: u32::MAX }))
                );
                assert_eq!(
                    map_portable(&pane, &origin),
                    Ok(PortableMapping::BridgeFocus {
                        request: FocusRequest::ByIndex { index: u32::MAX },
                    })
                );
            } else {
                for (action, name) in [(&action, "tab:focus"), (&pane, "pane:focus")] {
                    assert!(matches!(
                        map_portable(action, &origin),
                        Err(PortableError::InvalidScalar {
                            action: actual,
                            parameter: "index",
                            ..
                        }) if actual == name
                    ));
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn creation_rejects_non_utf8_paths_before_json_encoding() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        let path =
            muxe_core::AbsolutePath::new(PathBuf::from(OsString::from_vec(b"/tool-\xff".to_vec())))
                .expect("absolute OS path");
        for parameter in ["program", "args", "cwd"] {
            let mut command = ResolvedCreateCommand {
                program: Some(CommandWord::Text("tool".to_owned())),
                args: Vec::new(),
                cwd: None,
            };
            match parameter {
                "program" => command.program = Some(CommandWord::Path(path.clone())),
                "args" => command.args.push(CommandWord::Path(path.clone())),
                "cwd" => command.cwd = Some(muxe_core::CommandCwd::Origin(path.clone())),
                _ => unreachable!(),
            }
            let tab = ResolvedPortableAction::Tab(ResolvedTabAction::Create {
                workspace_id: None,
                name: None,
                focus: None,
                command: command.clone(),
            });
            let split = ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
                direction: Some(Direction::Right),
                focus: None,
                command,
            });
            for (action, name) in [(&tab, "tab:create"), (&split, "pane:split")] {
                for error in [
                    map_portable(action, &test_origin()).expect_err("non-UTF-8 creation must fail"),
                    map_post_dismissal_creation(action, &test_origin())
                        .expect_err("non-UTF-8 creation must fail after dismissal"),
                ] {
                    assert!(matches!(
                        error,
                        PortableError::InvalidScalar {
                            action: actual_action,
                            parameter: actual_parameter,
                            ..
                        } if actual_action == name && actual_parameter == parameter
                    ));
                }
            }
        }
    }

    #[test]
    fn incompatible_creation_diagnostics_and_routing_remain_distinct() {
        let origin = test_origin();
        let unfocused = ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
            direction: Some(Direction::Next),
            focus: Some(false),
            command: ResolvedCreateCommand::default(),
        });
        assert!(!creation_requires_post_dismissal(&unfocused));
        assert!(matches!(
            map_portable(&unfocused, &origin),
            Err(PortableError::Incompatible {
                action: "pane:split",
                ..
            })
        ));
        assert!(matches!(
            map_post_dismissal_creation(&unfocused, &origin),
            Err(PortableError::Incompatible {
                action: "pane:split",
                ..
            })
        ));
        let cwd_only = ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
            direction: None,
            focus: Some(false),
            command: ResolvedCreateCommand {
                cwd: Some(muxe_core::CommandCwd::Literal(PathBuf::from("/workspace"))),
                ..ResolvedCreateCommand::default()
            },
        });
        assert!(matches!(
            map_portable(&cwd_only, &origin),
            Err(PortableError::Incompatible {
                action: "pane:split",
                ..
            })
        ));
        let focused = ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
            direction: Some(Direction::Next),
            focus: Some(true),
            command: ResolvedCreateCommand::default(),
        });
        for error in [
            map_portable(&focused, &origin).expect_err("invalid direction must fail"),
            map_post_dismissal_creation(&focused, &origin)
                .expect_err("invalid direction must fail after dismissal"),
        ] {
            assert!(matches!(
                error,
                PortableError::InvalidScalar {
                    action: "pane:split",
                    parameter: "direction",
                    ..
                }
            ));
        }
    }
}
