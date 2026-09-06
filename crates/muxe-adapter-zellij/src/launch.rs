//! Host-side pane launching for Zellij (`muxe menu open`, `muxe pane open`).
//!
//! Zellij launchers create UI panes directly through the host: there is no
//! transient-tab trampoline. Pane creation goes over the same typed request
//! pipe as any other dispatch, targeting the origin client. Placement uses the
//! pinned directional and floating action variants with exact evidence:
//!
//! - Split (`down`, `up`, `left`, `right`) maps to `Action::NewTiledPane`
//!   with an explicit direction. All four directions are host-supported,
//!   unlike Herdr's right/down restriction.
//! - Overlay and popup map to `Action::NewFloatingPane` with explicit
//!   coordinates. There is no direct floating form of the plugin `Run`
//!   action carrying coordinates, so the floating variant is the precise one.
//! - The command travels as `RunCommandAction` with exact argv boundaries:
//!   `command` holds the program, `args` the untouched argument vector, and
//!   `cwd` the origin-derived working directory. No shell joins the vector.
//! - Menu launches always take focus: a modal menu must receive keys, and the
//!   CLI exposes no `--no-focus` for menus. Generic panes honor `--no-focus`
//!   through `no_focus`.
//!
//! Sizes accept cells (`80`) or percentages (`80%`), matching the CLI
//! contract. Percentages above 100 and zero cells are rejected before
//! anything reaches the host.

use std::path::PathBuf;

use muxe_adapter_api::{AdapterError, AdapterErrorKind};
use muxe_core::PaneId;
use muxe_zellij_protocol::generated::{raw, RawNativeCommand};
use thiserror::Error;

/// Split direction. Zellij supports all four, unlike Herdr's right/down pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZellijSplitDirection {
    /// Split downward.
    Down,
    /// Split upward.
    Up,
    /// Split leftward.
    Left,
    /// Split rightward.
    Right,
}

impl ZellijSplitDirection {
    const fn into_mirror(self) -> raw::Direction {
        match self {
            Self::Down => raw::Direction::Down,
            Self::Up => raw::Direction::Up,
            Self::Left => raw::Direction::Left,
            Self::Right => raw::Direction::Right,
        }
    }
}

/// One cells-or-percentage dimension from CLI `--width`, `--height`, or `--position`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeSpec {
    /// Terminal cells; must be positive.
    Cells(u32),
    /// Percentage of the containing area; 1 through 100.
    Percent(u8),
}

impl SizeSpec {
    const fn into_mirror(self) -> raw::PercentOrFixed {
        match self {
            Self::Cells(cells) => raw::PercentOrFixed::Fixed(cells as usize),
            Self::Percent(percent) => raw::PercentOrFixed::Percent(percent as usize),
        }
    }
}

/// Parses one CLI dimension (`80` cells or `80%` percent).
///
/// # Errors
///
/// Returns [`LaunchError`] for empty input, non-numeric cells, out-of-range
/// percentages, or zero cells.
pub fn parse_size(text: &str) -> Result<SizeSpec, LaunchError> {
    parse_sized(text, false)
}

fn parse_sized(text: &str, allow_zero: bool) -> Result<SizeSpec, LaunchError> {
    if let Some(percent) = text.strip_suffix('%') {
        let value: u8 = percent.parse().map_err(|_| LaunchError::InvalidSize {
            value: text.to_owned(),
            reason: "percentage must be a number from 1 to 100",
        })?;
        if !(1..=100).contains(&value) {
            return Err(LaunchError::InvalidSize {
                value: text.to_owned(),
                reason: "percentage must be a number from 1 to 100",
            });
        }
        return Ok(SizeSpec::Percent(value));
    }
    let value: u32 = text.parse().map_err(|_| LaunchError::InvalidSize {
        value: text.to_owned(),
        reason: "cells must be a positive number",
    })?;
    if value == 0 && !allow_zero {
        return Err(LaunchError::InvalidSize {
            value: text.to_owned(),
            reason: "cells must be a positive number",
        });
    }
    Ok(SizeSpec::Cells(value))
}

/// Parses one CLI `--position` value (`<X>,<Y>` in cells or percentages).
/// Origins admit zero; dimensions parsed elsewhere do not.
///
/// # Errors
///
/// Returns [`LaunchError`] when the shape is not two comma-separated values.
pub fn parse_position(text: &str) -> Result<(SizeSpec, SizeSpec), LaunchError> {
    let (x, y) = text.split_once(',').ok_or_else(|| LaunchError::InvalidSize {
        value: text.to_owned(),
        reason: "position must be <X>,<Y> in cells",
    })?;
    Ok((parse_sized(x.trim(), true)?, parse_sized(y.trim(), true)?))
}

/// Host placement for the new pane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZellijPlacement {
    /// Tiled split in one direction from the client's focused pane.
    Split {
        /// Split direction.
        direction: ZellijSplitDirection,
    },
    /// Floating pane at explicit coordinates.
    Floating {
        /// Horizontal origin.
        x: Option<SizeSpec>,
        /// Vertical origin.
        y: Option<SizeSpec>,
        /// Width.
        width: Option<SizeSpec>,
        /// Height.
        height: Option<SizeSpec>,
    },
}

/// What is being launched: a modal menu (focus mandatory) or a generic pane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaunchKind {
    /// Modal menu pane; `focus: false` is rejected.
    Menu,
    /// Generic command pane; `--no-focus` maps to `no_focus`.
    Generic,
}

/// Which client receives the new pane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchTarget {
    /// Target a client directly (broker-resolved origin client).
    Client(String),
    /// Resolve the unique client owning this pane through active
    /// registrations; fails rather than guessing when unavailable.
    UiPane(PaneId),
}

/// A fully specified host pane launch. The broker normalizes CLI options,
/// origin working directory, and the exact command vector before calling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZellijPaneLaunch {
    /// Menu (focus mandatory) or generic pane.
    pub kind: LaunchKind,
    /// Target client or owning-pane resolution.
    pub target: LaunchTarget,
    /// Working directory for the new pane. Menu launches always derive this
    /// from the captured origin; generic launches use `--cwd` or host default.
    pub cwd: Option<PathBuf>,
    /// Program to execute; never empty.
    pub program: PathBuf,
    /// Exact argument vector; boundaries preserved, never joined.
    pub args: Vec<String>,
    /// Host placement.
    pub placement: ZellijPlacement,
    /// Whether the new pane takes focus.
    pub focus: bool,
}

/// Launch specification failure, before anything reaches the host.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum LaunchError {
    /// Empty program.
    #[error("launch program must not be empty")]
    EmptyProgram,
    /// Menu without focus.
    #[error("menu panes must take focus; menu open exposes no --no-focus")]
    MenuRequiresFocus,
    /// Relative working directory.
    #[error("launch working directory must be absolute")]
    RelativeCwd,
    /// Bad dimension.
    #[error("invalid size '{value}': {reason}")]
    InvalidSize {
        /// Offending CLI value.
        value: String,
        /// Why it is invalid.
        reason: &'static str,
    },
}

impl LaunchError {
    /// Converts the failure into an adapter error for broker dispatch paths.
    pub fn into_adapter_error(self) -> AdapterError {
        AdapterError::new(AdapterErrorKind::InvalidRequest, self.to_string())
    }
}

/// Builds the pinned `RunAction` command for a validated launch.
///
/// The low-level action path dispatches through `run_action`, so completion
/// correlates through the bridge's `ActionComplete` echo like any other
/// native action.
///
/// # Errors
///
/// Returns [`LaunchError`] for empty programs, unfocused menus, relative
/// working directories, or invalid sizes (sizes arrive pre-parsed).
pub fn build_launch_command(launch: &ZellijPaneLaunch) -> Result<RawNativeCommand, LaunchError> {
    if launch.program.as_os_str().is_empty() {
        return Err(LaunchError::EmptyProgram);
    }
    if launch.kind == LaunchKind::Menu && !launch.focus {
        return Err(LaunchError::MenuRequiresFocus);
    }
    if let Some(cwd) = &launch.cwd
        && !cwd.is_absolute()
    {
        return Err(LaunchError::RelativeCwd);
    }
    let command = raw::RunCommandAction {
        command: launch.program.clone(),
        args: launch.args.clone(),
        cwd: launch.cwd.clone(),
        direction: None,
        hold_on_close: false,
        hold_on_start: false,
        originating_plugin: None,
        use_terminal_title: false,
    };
    let action = match launch.placement {
        ZellijPlacement::Split { direction } => raw::Action::NewTiledPane {
            direction: Some(direction.into_mirror()),
            command: Some(command),
            pane_name: None,
            near_current_pane: true,
            no_focus: !launch.focus,
            borderless: None,
            tab_id: None,
        },
        ZellijPlacement::Floating { x, y, width, height } => raw::Action::NewFloatingPane {
            command: Some(command),
            pane_name: None,
            coordinates: Some(raw::FloatingPaneCoordinates {
                x: x.map(SizeSpec::into_mirror),
                y: y.map(SizeSpec::into_mirror),
                width: width.map(SizeSpec::into_mirror),
                height: height.map(SizeSpec::into_mirror),
                pinned: None,
                borderless: None,
            }),
            near_current_pane: true,
            no_focus: !launch.focus,
            tab_id: None,
        },
    };
    Ok(RawNativeCommand::RunAction { action, context: Vec::new() })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn menu_launch() -> ZellijPaneLaunch {
        ZellijPaneLaunch {
            kind: LaunchKind::Menu,
            target: LaunchTarget::Client("client-1".to_owned()),
            cwd: Some(PathBuf::from("/work")),
            program: PathBuf::from("muxe"),
            args: vec!["ui".to_owned(), "menu".to_owned(), "main".to_owned()],
            placement: ZellijPlacement::Split { direction: ZellijSplitDirection::Down },
            focus: true,
        }
    }

    #[test]
    fn menu_split_preserves_exact_argv() {
        let raw = build_launch_command(&menu_launch()).expect("builds");
        let RawNativeCommand::RunAction { action, .. } = raw else {
            panic!("expected run-action wrap");
        };
        match action {
            raw::Action::NewTiledPane { direction, command, no_focus, .. } => {
                assert_eq!(direction, Some(raw::Direction::Down));
                assert!(!no_focus);
                let command = command.expect("command");
                assert_eq!(command.command, PathBuf::from("muxe"));
                assert_eq!(command.args, vec!["ui", "menu", "main"]);
                assert_eq!(command.cwd, Some(PathBuf::from("/work")));
            }
            _ => panic!("expected tiled split"),
        }
    }

    #[test]
    fn floating_carries_coordinates_and_no_focus() {
        let launch = ZellijPaneLaunch {
            kind: LaunchKind::Generic,
            target: LaunchTarget::Client("c".to_owned()),
            cwd: None,
            program: PathBuf::from("htop"),
            args: Vec::new(),
            placement: ZellijPlacement::Floating {
                x: Some(SizeSpec::Cells(0)),
                y: Some(SizeSpec::Percent(70)),
                width: Some(SizeSpec::Percent(100)),
                height: Some(SizeSpec::Percent(30)),
            },
            focus: false,
        };
        let raw = build_launch_command(&launch).expect("builds");
        let RawNativeCommand::RunAction { action, .. } = raw else {
            panic!("expected run-action wrap");
        };
        match action {
            raw::Action::NewFloatingPane { coordinates, no_focus, command, .. } => {
                assert!(no_focus);
                let coordinates = coordinates.expect("coordinates");
                assert_eq!(
                    coordinates.width,
                    Some(raw::PercentOrFixed::Percent(100))
                );
                assert_eq!(
                    coordinates.y,
                    Some(raw::PercentOrFixed::Percent(70))
                );
                assert_eq!(command.expect("command").command, PathBuf::from("htop"));
            }
            _ => panic!("expected floating pane"),
        }
    }

    #[test]
    fn menu_without_focus_is_rejected() {
        let mut launch = menu_launch();
        launch.focus = false;
        assert!(matches!(
            build_launch_command(&launch),
            Err(LaunchError::MenuRequiresFocus)
        ));
    }

    #[test]
    fn sizes_reject_garbage() {
        assert_eq!(parse_size("80"), Ok(SizeSpec::Cells(80)));
        assert_eq!(parse_size("80%"), Ok(SizeSpec::Percent(80)));
        assert!(parse_size("0").is_err());
        assert!(parse_size("101%").is_err());
        assert!(parse_size("wide").is_err());
        assert!(parse_size("").is_err());
        assert_eq!(
            parse_position("0,70%"),
            Ok((SizeSpec::Cells(0), SizeSpec::Percent(70)))
        );
        assert!(parse_position("70").is_err());
    }

    #[test]
    fn empty_program_and_relative_cwd_fail() {
        let mut launch = menu_launch();
        launch.program = PathBuf::new();
        assert!(matches!(
            build_launch_command(&launch),
            Err(LaunchError::EmptyProgram)
        ));
        let mut launch = menu_launch();
        launch.cwd = Some(PathBuf::from("relative/path"));
        assert!(matches!(
            build_launch_command(&launch),
            Err(LaunchError::RelativeCwd)
        ));
    }

    #[test]
    fn built_launch_validates_through_generated_conversion() {
        use muxe_zellij_protocol::generated::ValidatedNativeCommand;
        let raw = build_launch_command(&menu_launch()).expect("builds");
        ValidatedNativeCommand::try_from(raw).expect("generated conversion accepts launch");
    }
}
