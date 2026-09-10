//! Portable-action mapping for Zellij 0.46.0 with pinned-source evidence.
//!
//! Every row names the exact pinned `Action` variant (or the precise reason no
//! row exists) from `fixtures/zellij/0.46.0/action-inventory.rs`, itself derived
//! from revision `af38660c5884f50bb3726682fb92961326c4268f`.
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

use muxe_core::{
    ActionScalar, ConfigValueKind, ContextType, KeyboardAction, OriginContext, PaneAction,
    PortableAction, SessionAction, TabAction,
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
    /// A context marker survived to concrete mapping time.
    #[error(
        "portable {action} parameter '{parameter}' still carries an unresolved context reference"
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
                reason: "index must be a non-negative integer fitting in u32",
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
        reason: "origin carries no pane; the target is context_unavailable",
    })?;
    parse_origin_pane(pane.as_str()).ok_or(PortableError::InvalidScalar {
        action,
        parameter: "origin.pane.id",
        reason: "origin pane ID is not a pinned terminal_<n> or plugin_<n> identity",
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
    map_canonical_key(&text).map_err(|error| keyboard_error("keyboard:send", &error))?;
    Ok(())
}

/// Validates the tab subgroup structurally.
fn validate_tab_structure(tab: &TabAction) -> Result<(), PortableError> {
    match tab {
        TabAction::Create { workspace_id } => {
            if workspace_id.is_some() {
                return Err(PortableError::Incompatible {
                    action: "tab:create",
                    reason: "Zellij 0.46 has no workspace concept; omit workspace-id to use host NewTab",
                });
            }
            Ok(())
        }
        TabAction::Close => Ok(()),
        TabAction::Rename { name } => {
            let Some(name) = name else {
                return Err(PortableError::Incompatible {
                    action: "tab:rename",
                    reason: "pinned RenameTab requires a name; no host prompt variant exists",
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
                reason: "pinned MoveTab and MoveTabByTabId are directional; positional moves have no host primitive",
            }),
        },
        TabAction::Swap(_) => Err(PortableError::Incompatible {
            action: "tab:swap",
            reason: "no atomic pinned primitive swaps two tabs",
        }),
    }
}

/// Validates the pane subgroup structurally.
fn validate_pane_structure(pane: &PaneAction) -> Result<(), PortableError> {
    match pane {
        PaneAction::Create | PaneAction::Close => Ok(()),
        PaneAction::Split { direction } => {
            if let Some(direction) = direction {
                Cardinal::parse("pane:split", direction).map(|_| ())?;
            }
            Ok(())
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
                reason: "MovePaneByPaneId takes an optional direction, not a positional destination",
            }),
        },
        PaneAction::Swap(_) => Err(PortableError::Incompatible {
            action: "pane:swap",
            reason: "no pinned primitive swaps two panes",
        }),
        PaneAction::Resize { direction, amount } => {
            if let Some(amount) = amount {
                check_marker("pane:resize", "amount", amount, &[], true)?;
                return Err(PortableError::Incompatible {
                    action: "pane:resize",
                    reason: "pinned Resize carries no magnitude; omit amount for one Increase step",
                });
            }
            Cardinal::parse("pane:resize", direction).map(|_| ())
        }
        PaneAction::Zoom { enabled } => check_toggle("pane:zoom", enabled.as_ref()),
        PaneAction::Fullscreen { enabled } => check_toggle("pane:fullscreen", enabled.as_ref()),
        PaneAction::Floating { enabled } => check_toggle("pane:floating", enabled.as_ref()),
        PaneAction::Frame { .. } => Err(PortableError::Incompatible {
            action: "pane:frame",
            reason: "no pane-targeted frame primitive exists; TogglePaneFrames acts on the focused menu pane",
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
            reason: "no one-to-one plugin API creates a session",
        }),
        SessionAction::Quit => Err(PortableError::Incompatible {
            action: "session:quit",
            reason: "quit_zellij terminates the server; it is not a portable session quit",
        }),
    }
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
            reason: "the host exposes only a toggle; an explicit boolean cannot be honored idempotently",
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
                reason: "expected one of next or previous for tab focus",
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

/// Maps a fully resolved portable action to its host payload against the
/// immutable origin captured at attach time.
///
/// Every scalar must be concrete; a surviving context marker fails closed
/// rather than substituting an implicit current pane.
///
/// # Errors
///
/// Returns [`PortableError`] when a scalar is unresolvable, incompatible with
/// the pinned host surface, or references missing origin context.
pub fn map_portable(
    action: &PortableAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match action {
        PortableAction::Menu(_) | PortableAction::Config(_) | PortableAction::Command(_) => {
            Ok(PortableMapping::BrokerOwned)
        }
        PortableAction::Keyboard(keyboard) => map_keyboard_action(keyboard, origin),
        PortableAction::Tab(tab) => map_tab_action(tab, origin),
        PortableAction::Pane(pane) => map_pane_action(pane, origin),
        PortableAction::Session(session) => map_session_action(session, origin),
    }
}

/// Maps keyboard actions against the origin pane.
fn map_keyboard_action(
    keyboard: &KeyboardAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match keyboard {
        KeyboardAction::SendText(text) => {
            let chars = scalar_string("keyboard:send", "text", text)?;
            let pane = origin_pane("keyboard:send", origin)?;
            Ok(wrap(raw::Action::WriteCharsToPaneId {
                chars,
                pane_id: pane,
            }))
        }
        KeyboardAction::SendKeys(keys) => {
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
                let text = scalar_string("keyboard:send", "keys", key)?;
                let mapped = map_canonical_key(&text)
                    .map_err(|error| keyboard_error("keyboard:send", &error))?;
                bytes.extend_from_slice(&mapped.bytes);
            }
            Ok(wrap(raw::Action::WriteToPaneId { bytes, pane_id: pane }))
        }
    }
}

/// Maps tab actions; rename resolves its index from the immutable origin.
fn map_tab_action(
    tab: &TabAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match tab {
        TabAction::Create { workspace_id } => {
            if workspace_id.is_some() {
                return Err(PortableError::Incompatible {
                    action: "tab:create",
                    reason: "Zellij 0.46 has no workspace concept; omit workspace-id to use host NewTab",
                });
            }
            Ok(wrap(raw::Action::NewTab {
                tiled_layout: None,
                floating_layouts: Vec::new(),
                swap_tiled_layouts: None,
                swap_floating_layouts: None,
                tab_name: None,
                should_change_focus_to_new_tab: true,
                cwd: None,
                initial_panes: None,
                first_pane_unblock_condition: None,
            }))
        }
        TabAction::Close => Ok(wrap(raw::Action::CloseTab)),
        TabAction::Rename { name } => {
            let Some(name) = name else {
                return Err(PortableError::Incompatible {
                    action: "tab:rename",
                    reason: "pinned RenameTab requires a name; no host prompt variant exists",
                });
            };
            // Pinned RenameTab needs both index and name. The broker resolves
            // origin.tab.index into the immutable origin before dispatch, so the
            // index comes from there rather than from a rename parameter.
            let Some(tab_index) = origin.tab_index else {
                return Err(PortableError::Incompatible {
                    action: "tab:rename",
                    reason: "origin carries no tab index; the rename target is context_unavailable",
                });
            };
            let tab_index = u32::try_from(tab_index).map_err(|_| PortableError::InvalidScalar {
                action: "tab:rename",
                parameter: "origin.tab.index",
                reason: "tab index must fit in u32",
            })?;
            let name = scalar_string("tab:rename", "name", name)?.into_bytes();
            Ok(wrap(raw::Action::RenameTab { tab_index, name }))
        }
        TabAction::Focus(target) => match target {
            muxe_core::IndexOrDirection::Index(index) => {
                let position = scalar_index_u32("tab:focus", "index", index)?;
                Ok(wrap(raw::Action::GoToTab { index: position }))
            }
            muxe_core::IndexOrDirection::Direction(direction) => {
                match scalar_direction_text("tab:focus", direction)? {
                    "next" => Ok(wrap(raw::Action::GoToNextTab)),
                    "previous" => Ok(wrap(raw::Action::GoToPreviousTab)),
                    _ => Err(PortableError::Incompatible {
                        action: "tab:focus",
                        reason: "Zellij tab focus supports only index, next, and previous",
                    }),
                }
            }
        },
        TabAction::Move(target) => match target {
            muxe_core::IndexOrDirection::Direction(direction) => {
                let cardinal = Cardinal::parse("tab:move", direction)?;
                Ok(wrap(raw::Action::MoveTab {
                    direction: cardinal.into_mirror(),
                }))
            }
            muxe_core::IndexOrDirection::Index(_) => Err(PortableError::Incompatible {
                action: "tab:move",
                reason: "pinned MoveTab and MoveTabByTabId are directional; positional moves have no host primitive",
            }),
        },
        TabAction::Swap(_) => Err(PortableError::Incompatible {
            action: "tab:swap",
            reason: "no atomic pinned primitive swaps two tabs",
        }),
    }
}

/// A plain new pane with no direction, name, or suppression.
fn new_plain_pane() -> PortableMapping {
    wrap(raw::Action::NewPane {
        direction: None,
        pane_name: None,
        start_suppressed: false,
    })
}

/// Maps pane actions, split into creation and targeted operations.
fn map_pane_action(
    pane: &PaneAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match pane {
        PaneAction::Create | PaneAction::Split { .. } | PaneAction::Close => {
            map_pane_lifecycle(pane, origin)
        }
        PaneAction::Focus(_)
        | PaneAction::Move(_)
        | PaneAction::Swap(_)
        | PaneAction::Resize { .. }
        | PaneAction::Zoom { .. }
        | PaneAction::Fullscreen { .. }
        | PaneAction::Floating { .. }
        | PaneAction::Frame { .. } => map_pane_operation(pane, origin),
    }
}

/// Maps pane creation, split, and close.
fn map_pane_lifecycle(
    pane: &PaneAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match pane {
        PaneAction::Create => Ok(new_plain_pane()),
        PaneAction::Split { direction } => {
            let Some(direction) = direction else {
                return Ok(new_plain_pane());
            };
            let cardinal = Cardinal::parse("pane:split", direction)?;
            Ok(wrap(raw::Action::NewTiledPane {
                direction: Some(cardinal.into_mirror()),
                command: None,
                pane_name: None,
                near_current_pane: true,
                no_focus: false,
                borderless: None,
                tab_id: None,
            }))
        }
        PaneAction::Close => {
            let pane = origin_pane("pane:close", origin)?;
            Ok(wrap(raw::Action::CloseFocusByPaneId { pane_id: pane }))
        }
        _ => unreachable!("pane lifecycle dispatch covers only creation, split, and close"),
    }
}

/// Maps targeted operations on existing panes.
fn map_pane_operation(
    pane: &PaneAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match pane {
        PaneAction::Focus(target) => match target {
            muxe_core::IndexOrDirection::Direction(direction) => {
                let cardinal = Cardinal::parse("pane:focus", direction)?;
                Ok(PortableMapping::BridgeFocus {
                    request: FocusRequest::Neighbor {
                        direction: cardinal,
                    },
                })
            }
            muxe_core::IndexOrDirection::Index(index) => {
                let position = scalar_index_u32("pane:focus", "index", index)?;
                Ok(PortableMapping::BridgeFocus {
                    request: FocusRequest::ByIndex { index: position },
                })
            }
        },
        PaneAction::Move(target) => match target {
            muxe_core::IndexOrDirection::Direction(direction) => {
                let cardinal = Cardinal::parse("pane:move", direction)?;
                let pane = origin_pane("pane:move", origin)?;
                Ok(wrap(raw::Action::MovePaneByPaneId {
                    pane_id: pane,
                    direction: Some(cardinal.into_mirror()),
                }))
            }
            muxe_core::IndexOrDirection::Index(_) => Err(PortableError::Incompatible {
                action: "pane:move",
                reason: "MovePaneByPaneId takes an optional direction, not a positional destination",
            }),
        },
        PaneAction::Swap(_) => Err(PortableError::Incompatible {
            action: "pane:swap",
            reason: "no pinned primitive swaps two panes",
        }),
        PaneAction::Resize { direction, amount } => {
            if amount.is_some() {
                return Err(PortableError::Incompatible {
                    action: "pane:resize",
                    reason: "pinned Resize carries no magnitude; omit amount for one Increase step",
                });
            }
            let cardinal = Cardinal::parse("pane:resize", direction)?;
            let pane = origin_pane("pane:resize", origin)?;
            Ok(wrap(raw::Action::ResizeByPaneId {
                pane_id: pane,
                resize: raw::Resize::Increase,
                direction: Some(cardinal.into_mirror()),
            }))
        }
        PaneAction::Zoom { enabled } => map_toggle("pane:zoom", enabled.as_ref(), origin, |pane| {
            raw::Action::ToggleFocusFullscreenByPaneId { pane_id: pane }
        }),
        PaneAction::Fullscreen { enabled } => {
            map_toggle("pane:fullscreen", enabled.as_ref(), origin, |pane| {
                raw::Action::ToggleFocusNoUiFullscreenByPaneId { pane_id: pane }
            })
        }
        PaneAction::Floating { enabled } => {
            map_toggle("pane:floating", enabled.as_ref(), origin, |pane| {
                raw::Action::TogglePaneEmbedOrFloatingByPaneId { pane_id: pane }
            })
        }
        PaneAction::Frame { .. } => Err(PortableError::Incompatible {
            action: "pane:frame",
            reason: "no pane-targeted frame primitive exists; TogglePaneFrames acts on the focused menu pane",
        }),
        _ => unreachable!("pane operation dispatch covers only targeted operations"),
    }
}

/// Maps session actions; kill resolves its target from the immutable origin.
fn map_session_action(
    session: &SessionAction,
    origin: &OriginContext,
) -> Result<PortableMapping, PortableError> {
    match session {
        SessionAction::Attach { name } | SessionAction::Switch { name } => Ok(switch_session(
            scalar_string("session:switch", "name", name)?,
        )),
        SessionAction::Rename { name } => Ok(wrap(raw::Action::RenameSession {
            name: scalar_string("session:rename", "name", name)?,
        })),
        SessionAction::Detach => Ok(wrap(raw::Action::Detach)),
        SessionAction::Kill => {
            let session = origin
                .session_id
                .as_ref()
                .ok_or(PortableError::Incompatible {
                    action: "session:kill",
                    reason: "origin carries no session; the target is context_unavailable",
                })?;
            Ok(PortableMapping::HostAction {
                commands: vec![RawNativeCommand::KillSessions {
                    session_names: vec![session.as_str().to_owned()],
                }],
            })
        }
        SessionAction::Create => Err(PortableError::Incompatible {
            action: "session:create",
            reason: "no one-to-one plugin API creates a session",
        }),
        SessionAction::Quit => Err(PortableError::Incompatible {
            action: "session:quit",
            reason: "quit_zellij terminates the server; it is not a portable session quit",
        }),
    }
}

fn scalar_direction_text<'a>(
    action: &'static str,
    scalar: &'a ActionScalar,
) -> Result<&'a str, PortableError> {
    match &scalar.value.kind {
        ConfigValueKind::String(text) => match text.as_str() {
            "left" | "right" | "up" | "down" | "next" | "previous" => Ok(text.as_str()),
            _ => Err(PortableError::InvalidScalar {
                action,
                parameter: "direction",
                reason: "expected a direction literal",
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

/// Wraps a resolved session name in the pinned switch-session payload.
fn switch_session(name: String) -> PortableMapping {
    wrap(raw::Action::SwitchSession {
        name,
        tab_position: None,
        pane_id: None,
        layout: None,
        cwd: None,
    })
}

fn map_toggle(
    action: &'static str,
    enabled: Option<&ActionScalar>,
    origin: &OriginContext,
    build: impl FnOnce(raw::PaneId) -> raw::Action,
) -> Result<PortableMapping, PortableError> {
    if let Some(scalar) = enabled {
        if is_marker(scalar) {
            return Err(PortableError::UnresolvedContext {
                action,
                parameter: "enabled",
            });
        }
        scalar_bool(action, "enabled", scalar)?;
        return Err(PortableError::Incompatible {
            action,
            reason: "the host exposes only a toggle; an explicit boolean cannot be honored idempotently",
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

    fn test_origin() -> OriginContext {
        OriginContext {
            host_kind: muxe_core::OriginHostKind::Zellij,
            server_id: muxe_core::ServerId::new("session-alpha"),
            client_id: Some(muxe_core::ClientId::new("client-1")),
            session_id: Some(muxe_core::SessionId::new("session-alpha")),
            workspace_id: None,
            tab_id: Some(muxe_core::TabId::new("tab-0")),
            tab_index: Some(3),
            pane_id: Some(muxe_core::PaneId::new("terminal_4")),
            pane_type: None,
            pane_cwd: None,
            selection_text: None,
            invocation_source: muxe_core::OriginInvocationSource::RootBinding,
            worktree_id: None,
            worktree_path: None,
            agent_id: None,
            link_url: None,
            link_handler_id: None,
        }
    }

    fn map(action: &PortableAction) -> Result<PortableMapping, PortableError> {
        map_portable(action, &test_origin())
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
        }));
        assert!(matches!(action, raw::Action::NewTab { .. }));
        for action in [
            PortableAction::Tab(TabAction::Create {
                workspace_id: Some(text("ws")),
            }),
            PortableAction::Tab(TabAction::Create {
                workspace_id: Some(marker("origin.workspace.id")),
            }),
        ] {
            let error = map_portable(&action, &test_origin()).expect_err("no workspaces");
            assert!(matches!(
                error,
                PortableError::Incompatible {
                    action: "tab:create",
                    ..
                }
            ));
            assert!(validate_portable_structure(&action).is_err());
        }
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

        let mut origin = test_origin();
        origin.tab_index = None;
        let error = map_portable(
            &PortableAction::Tab(TabAction::Rename {
                name: Some(text("logs")),
            }),
            &origin,
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
        assert!(map_portable(&oversized, &test_origin()).is_err());

        let bad_direction = PortableAction::Tab(TabAction::Focus(
            muxe_core::IndexOrDirection::Direction(text("sideways")),
        ));
        assert!(validate_portable_structure(&bad_direction).is_err());

        // Fitting markers pass validation but fail concrete mapping.
        let marked = PortableAction::Tab(TabAction::Focus(muxe_core::IndexOrDirection::Index(
            marker("origin.tab.index"),
        )));
        assert!(validate_portable_structure(&marked).is_ok());
        assert!(matches!(
            map_portable(&marked, &test_origin()),
            Err(PortableError::UnresolvedContext { .. })
        ));

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
        assert!(map(&PortableAction::Pane(PaneAction::Zoom { enabled: None })).is_ok());
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
        assert!(
            map(&PortableAction::Session(SessionAction::Switch {
                name: text("dev"),
            }))
            .is_ok()
        );
        assert!(map(&PortableAction::Session(SessionAction::Detach)).is_ok());

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
    fn mapped_raw_validates_through_generated_conversion() {
        let action = single_host(&PortableAction::Tab(TabAction::Close));
        let validated = to_validated_action(action).expect("generated conversion accepts");
        assert_eq!(validated, validated::Action::CloseTab);
    }
}
