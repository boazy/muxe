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
//! - Overlay and popup both map to `Action::NewFloatingPane` with explicit
//!   coordinates. The pinned host exposes a single floating form, so the
//!   overlay/popup distinction is retained for diagnostics only; neither form
//!   invents host fields.
//! - The command travels as `RunCommandAction` with exact argv boundaries:
//!   `command` holds the program, `args` the untouched argument vector, and
//!   `cwd` the origin-derived working directory. No shell joins the vector.
//! - Menu launches always take focus: a modal menu must receive keys, and the
//!   CLI exposes no `--no-focus` for menus. Generic panes honor `--no-focus`
//!   through `no_focus`.
//! - Menu launches require the captured absolute origin working directory.
//!   There is no host-default fallback for menus: `cwd: None` fails as
//!   [`LaunchError::MissingCwd`] before anything reaches the host.
//! - Tiled launches carry no size: pinned Zellij ignores `x`, `y`, `width`,
//!   and `height` for the tiled `Run` branch and uses the host's initial
//!   split size (DESIGN, root launcher examples). Any `--width`, `--height`,
//!   or `--position` on a split pane fails as
//!   [`LaunchError::UnsupportedPlacement`] instead of pretending the host
//!   honored it.
//! - `--position` is cells from the top-left, never percentages (CLI
//!   contract). [`normalize_placement`] therefore takes the position as a
//!   plain `(u32, u32)` cell pair; percentage positions have no representation
//!   here. Width and height accept cells or percentages through [`SizeSpec`],
//!   matching the pinned `PercentOrFixed` host type.
//! - String parsing of dimensions lives in the typed CLI layer, which already
//!   validates shapes. This module validates host ranges (percent 1-100,
//!   positive cells) and host/type compatibility on typed values.
//!
//! Completion follows the normal dispatch path: [`ZellijPaneLaunch::into_command`]
//! builds a `RunAction` dispatched through `run_action`, and the bridge's
//! `ActionComplete` echo means Zellij finished dispatching the action, not
//! that the launched process succeeded. Callers must never claim
//! process success from dispatch acceptance.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use muxe_adapter_api::{AdapterError, AdapterErrorKind};
use muxe_core::PaneId;
use muxe_zellij_protocol::generated::{RawNativeCommand, raw};
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

/// One cells-or-percentage dimension for `--width` or `--height`, already
/// shape-parsed by the typed CLI layer. Range checks (percent 1-100, positive
/// cells) run in [`normalize_placement`], which owns the host contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeSpec {
    /// Terminal cells.
    Cells(u32),
    /// Percentage of the containing area.
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

/// Host pane form requested by `--pane-type`. The broker maps the CLI value
/// one-to-one onto this enum; overlay and popup share the pinned floating
/// host form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZellijPaneKind {
    /// Tiled split pane.
    Split,
    /// Floating overlay pane.
    Overlay,
    /// Floating popup pane.
    Popup,
}

/// Host placement for the new pane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZellijPlacement {
    /// Tiled split in one direction from the client's focused pane. Carries
    /// no size: the host ignores dimensions on this branch.
    Split {
        /// Split direction.
        direction: ZellijSplitDirection,
    },
    /// Floating pane at explicit coordinates.
    Floating {
        /// Horizontal origin in cells.
        x: Option<u32>,
        /// Vertical origin in cells.
        y: Option<u32>,
        /// Width in cells or percent.
        width: Option<SizeSpec>,
        /// Height in cells or percent.
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
///
/// `ParentPane::Current` maps to `Client` with the broker-resolved origin
/// client. An explicit `--parent-pane <id>` maps to `UiPane` with that pane
/// ID: the adapter resolves the live client owning the pane and fails
/// predispatch when no live registration owns it. Placement stays relative
/// to that client's focused pane because the pinned `NewTiledPane` and
/// `NewFloatingPane` actions expose `near_current_pane` and `tab_id` only;
/// there is no parent-pane host field, so an explicit parent selects the
/// destination client scope rather than an exact anchor pane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchTarget {
    /// Target a client directly (broker-resolved origin client).
    Client(String),
    /// Resolve the unique client owning this pane through active
    /// registrations; fails rather than guessing when unavailable.
    UiPane(PaneId),
}

/// A fully specified host pane launch. The broker normalizes CLI options,
/// origin working directory, and the exact command vector before calling:
/// [`normalize_placement`] for the placement, [`resolve_launch_cwd`] for the
/// working directory, and [`split_argv`] for the structured argv.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZellijPaneLaunch {
    /// Menu (focus mandatory) or generic pane.
    pub kind: LaunchKind,
    /// Target client or owning-pane resolution.
    pub target: LaunchTarget,
    /// Working directory for the new pane. Menu launches require `Some`
    /// absolute captured origin; generic launches carry the launcher-resolved
    /// absolute path (`None` means host default and never reaches production).
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

impl ZellijPaneLaunch {
    /// Validates the launch and moves its fields into the pinned `RunAction`
    /// command, returning the command with the untouched target for client
    /// resolution. Moving (not cloning) preserves exact argv boundaries with
    /// no whole-vector copy.
    ///
    /// The low-level action path dispatches through `run_action`, so
    /// completion correlates through the bridge's `ActionComplete` echo like
    /// any other native action: dispatch acceptance, never process success.
    ///
    /// # Errors
    ///
    /// Returns [`LaunchError`] for empty programs, unfocused menus, missing
    /// or relative working directories.
    pub fn into_command(self) -> Result<(RawNativeCommand, LaunchTarget), LaunchError> {
        if self.program.as_os_str().is_empty() {
            return Err(LaunchError::EmptyProgram);
        }
        if self.kind == LaunchKind::Menu && !self.focus {
            return Err(LaunchError::MenuRequiresFocus);
        }
        match &self.cwd {
            None if self.kind == LaunchKind::Menu => return Err(LaunchError::MissingCwd),
            Some(cwd) if !cwd.is_absolute() => return Err(LaunchError::RelativeCwd),
            _ => {}
        }
        let command = raw::RunCommandAction {
            command: self.program,
            args: self.args,
            cwd: self.cwd,
            direction: None,
            hold_on_close: false,
            hold_on_start: false,
            originating_plugin: None,
            use_terminal_title: false,
        };
        let action = match self.placement {
            ZellijPlacement::Split { direction } => raw::Action::NewTiledPane {
                direction: Some(direction.into_mirror()),
                command: Some(command),
                pane_name: None,
                near_current_pane: true,
                no_focus: !self.focus,
                borderless: None,
                tab_id: None,
            },
            ZellijPlacement::Floating {
                x,
                y,
                width,
                height,
            } => raw::Action::NewFloatingPane {
                command: Some(command),
                pane_name: None,
                coordinates: Some(raw::FloatingPaneCoordinates {
                    x: x.map(|cells| raw::PercentOrFixed::Fixed(cells as usize)),
                    y: y.map(|cells| raw::PercentOrFixed::Fixed(cells as usize)),
                    width: width.map(SizeSpec::into_mirror),
                    height: height.map(SizeSpec::into_mirror),
                    pinned: None,
                    borderless: None,
                }),
                near_current_pane: true,
                no_focus: !self.focus,
                tab_id: None,
            },
        };
        Ok((
            RawNativeCommand::RunAction {
                action,
                context: Vec::new(),
            },
            self.target,
        ))
    }
}

/// Launch specification failure, before anything reaches the host.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum LaunchError {
    /// Empty program.
    #[error("launch program must not be empty")]
    EmptyProgram,
    /// No argv at all (missing `-- <command>`).
    #[error("launch argv must name a program after --")]
    EmptyArgv,
    /// Non-UTF-8 argv entry: the Zellij pipe protocol carries string
    /// arguments, so the exact offending index is reported predispatch.
    #[error("launch argv entry {index} is not valid UTF-8")]
    NonUtf8Arg {
        /// Position in the original argv (0 is the program).
        index: usize,
    },
    /// Menu without focus.
    #[error("menu panes must take focus; menu open exposes no --no-focus")]
    MenuRequiresFocus,
    /// Menu without a captured origin working directory.
    #[error("menu launches require the captured absolute origin working directory")]
    MissingCwd,
    /// Relative working directory.
    #[error("launch working directory must be absolute")]
    RelativeCwd,
    /// Captured origin working directory is not absolute, so neither the
    /// default nor a relative override can resolve against it.
    #[error("captured origin working directory must be absolute")]
    RelativeOriginCwd,
    /// Bad dimension range on an otherwise well-shaped typed value.
    #[error("invalid size '{value}': {reason}")]
    InvalidSize {
        /// Offending value rendered back to CLI form.
        value: String,
        /// Why it is invalid.
        reason: &'static str,
    },
    /// A CLI flag the host/pane-type combination cannot honor. The option
    /// name and the pinned host reason travel together so the broker reports
    /// the exact dropped flag instead of silently discarding it.
    #[error("unsupported {option} for this Zellij pane type: {reason}")]
    UnsupportedPlacement {
        /// CLI option that cannot be honored (e.g. `--width`).
        option: &'static str,
        /// Pinned host reason.
        reason: &'static str,
    },
}

impl LaunchError {
    /// Converts the failure into an adapter error for broker dispatch paths.
    #[must_use]
    pub fn into_adapter_error(self) -> AdapterError {
        AdapterError::new(AdapterErrorKind::InvalidRequest, self.to_string())
    }
}

/// Normalizes typed CLI placement values into host placement, enforcing the
/// pinned host contract before pane creation.
///
/// - `Split` with any `width`, `height`, or `position` fails: the pinned
///   tiled `Run` branch ignores those fields, so accepting them would pretend
///   the host honored them.
/// - `Overlay` and `Popup` share the single pinned floating form and carry
///   every supplied coordinate.
/// - Width/height ranges are enforced here: cells must be positive,
///   percentages 1 through 100. Positions are cells from the top-left;
///   origins admit zero.
///
/// # Errors
///
/// Returns [`LaunchError::UnsupportedPlacement`] for dimensions on a split
/// pane and [`LaunchError::InvalidSize`] for out-of-range typed values.
pub fn normalize_placement(
    kind: ZellijPaneKind,
    direction: ZellijSplitDirection,
    width: Option<SizeSpec>,
    height: Option<SizeSpec>,
    position: Option<(u32, u32)>,
) -> Result<ZellijPlacement, LaunchError> {
    // Unsupported-axis rejection comes first: even a well-formed value would
    // be silently ignored by the host on this branch, so the flag itself is
    // the error rather than its range.
    if kind == ZellijPaneKind::Split {
        if width.is_some() {
            return Err(LaunchError::UnsupportedPlacement {
                option: "--width",
                reason: "pinned Zellij ignores dimensions on tiled panes",
            });
        }
        if height.is_some() {
            return Err(LaunchError::UnsupportedPlacement {
                option: "--height",
                reason: "pinned Zellij ignores dimensions on tiled panes",
            });
        }
        if position.is_some() {
            return Err(LaunchError::UnsupportedPlacement {
                option: "--position",
                reason: "pinned Zellij ignores coordinates on tiled panes",
            });
        }
        return Ok(ZellijPlacement::Split { direction });
    }
    if let Some(width) = width {
        check_dimension(width)?;
    }
    if let Some(height) = height {
        check_dimension(height)?;
    }
    let (x, y) = position.unwrap_or((0, 0));
    Ok(ZellijPlacement::Floating {
        x: Some(x),
        y: Some(y),
        width,
        height,
    })
}

fn check_dimension(size: SizeSpec) -> Result<(), LaunchError> {
    match size {
        SizeSpec::Cells(0) => Err(LaunchError::InvalidSize {
            value: "0".to_owned(),
            reason: "cells must be a positive number",
        }),
        SizeSpec::Percent(percent) if !(1..=100).contains(&percent) => {
            Err(LaunchError::InvalidSize {
                value: format!("{percent}%"),
                reason: "percentage must be a number from 1 to 100",
            })
        }
        _ => Ok(()),
    }
}

/// Resolves `muxe pane open --cwd` exactly once at the launcher boundary.
/// The captured live origin cwd is the only base: omitted uses it, an
/// absolute override replaces it, and a relative override is joined to it
/// once here (never re-resolved downstream). No ambient broker directory,
/// home, or root fallback is permitted.
///
/// # Errors
///
/// Returns [`LaunchError::RelativeOriginCwd`] when the captured origin is
/// not absolute.
pub fn resolve_launch_cwd(
    captured_origin: &Path,
    override_cwd: Option<&Path>,
) -> Result<PathBuf, LaunchError> {
    if !captured_origin.is_absolute() {
        return Err(LaunchError::RelativeOriginCwd);
    }
    Ok(match override_cwd {
        None => captured_origin.to_path_buf(),
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => captured_origin.join(path),
    })
}

/// Splits the structured `-- <argv>` tail into its program and exact
/// argument vector without joining. Entries travel as UTF-8 strings over the
/// pipe protocol, so non-UTF-8 input fails predispatch with its index.
///
/// # Errors
///
/// Returns [`LaunchError::EmptyArgv`] for an empty tail and
/// [`LaunchError::NonUtf8Arg`] for the first non-UTF-8 entry.
pub fn split_argv(argv: Vec<OsString>) -> Result<(PathBuf, Vec<String>), LaunchError> {
    let mut argv = argv.into_iter().enumerate();
    let (_, program) = argv.next().ok_or(LaunchError::EmptyArgv)?;
    if program.is_empty() {
        return Err(LaunchError::EmptyProgram);
    }
    let program_text = program
        .into_string()
        .map_err(|_| LaunchError::NonUtf8Arg { index: 0 })?;
    let mut arguments = Vec::new();
    for (index, argument) in argv {
        arguments.push(
            argument
                .into_string()
                .map_err(|_| LaunchError::NonUtf8Arg { index })?,
        );
    }
    Ok((PathBuf::from(program_text), arguments))
}

/// Reports whether a `pane open -- <argv>` trailing vector invokes the
/// canonical Muxe UI (`muxe ui menu ...`, the only `UiSubcommand` on the
/// parsed CLI surface). The native launcher routes such invocations into
/// the token-minted UI path instead of a generic command pane. Matching is
/// structural: program file name `muxe`, first argument `ui`, second
/// `menu`. Anything else — including a bare `muxe` with other subcommands —
/// stays a generic launch.
#[must_use]
pub fn is_ui_argv(argv: &[String]) -> bool {
    let [program, first, second, ..] = argv else {
        return false;
    };
    Path::new(program)
        .file_name()
        .is_some_and(|name| name == "muxe")
        && first == "ui"
        && second == "menu"
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
            placement: ZellijPlacement::Split {
                direction: ZellijSplitDirection::Down,
            },
            focus: true,
        }
    }

    #[test]
    fn menu_split_preserves_exact_argv() {
        let launch = menu_launch();
        let expected_args = launch.args.clone();
        let (raw, target) = launch.into_command().expect("builds");
        assert_eq!(target, LaunchTarget::Client("client-1".to_owned()));
        let RawNativeCommand::RunAction { action, .. } = raw else {
            panic!("expected run-action wrap");
        };
        match action {
            raw::Action::NewTiledPane {
                direction,
                command,
                no_focus,
                ..
            } => {
                assert_eq!(direction, Some(raw::Direction::Down));
                assert!(!no_focus);
                let command = command.expect("command");
                assert_eq!(command.command, PathBuf::from("muxe"));
                assert_eq!(command.args, expected_args);
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
                x: Some(0),
                y: Some(7),
                width: Some(SizeSpec::Percent(100)),
                height: Some(SizeSpec::Percent(30)),
            },
            focus: false,
        };
        let (raw, _) = launch.into_command().expect("builds");
        let RawNativeCommand::RunAction { action, .. } = raw else {
            panic!("expected run-action wrap");
        };
        match action {
            raw::Action::NewFloatingPane {
                coordinates,
                no_focus,
                command,
                ..
            } => {
                assert!(no_focus);
                let coordinates = coordinates.expect("coordinates");
                assert_eq!(coordinates.width, Some(raw::PercentOrFixed::Percent(100)));
                assert_eq!(coordinates.y, Some(raw::PercentOrFixed::Fixed(7)));
                assert_eq!(command.expect("command").command, PathBuf::from("htop"));
            }
            _ => panic!("expected floating pane"),
        }
    }

    #[test]
    fn menu_without_focus_or_cwd_is_rejected() {
        let mut launch = menu_launch();
        launch.focus = false;
        assert!(matches!(
            launch.into_command(),
            Err(LaunchError::MenuRequiresFocus)
        ));
        let mut launch = menu_launch();
        launch.cwd = None;
        assert!(matches!(
            launch.into_command(),
            Err(LaunchError::MissingCwd)
        ));
    }

    #[test]
    fn split_rejects_dimensions_before_creation() {
        assert!(matches!(
            normalize_placement(
                ZellijPaneKind::Split,
                ZellijSplitDirection::Down,
                Some(SizeSpec::Cells(80)),
                None,
                None,
            ),
            Err(LaunchError::UnsupportedPlacement {
                option: "--width",
                ..
            })
        ));
        assert!(matches!(
            normalize_placement(
                ZellijPaneKind::Split,
                ZellijSplitDirection::Right,
                None,
                Some(SizeSpec::Percent(30)),
                None,
            ),
            Err(LaunchError::UnsupportedPlacement {
                option: "--height",
                ..
            })
        ));
        assert!(matches!(
            normalize_placement(
                ZellijPaneKind::Split,
                ZellijSplitDirection::Up,
                None,
                None,
                Some((10, 10)),
            ),
            Err(LaunchError::UnsupportedPlacement {
                option: "--position",
                ..
            })
        ));
        let placement = normalize_placement(
            ZellijPaneKind::Split,
            ZellijSplitDirection::Left,
            None,
            None,
            None,
        )
        .expect("bare split normalizes");
        assert_eq!(
            placement,
            ZellijPlacement::Split {
                direction: ZellijSplitDirection::Left
            }
        );
    }

    #[test]
    fn overlay_and_popup_share_the_floating_form() {
        for kind in [ZellijPaneKind::Overlay, ZellijPaneKind::Popup] {
            let placement = normalize_placement(
                kind,
                ZellijSplitDirection::Down,
                Some(SizeSpec::Percent(100)),
                Some(SizeSpec::Percent(30)),
                Some((0, 7)),
            )
            .expect("floating normalizes");
            assert_eq!(
                placement,
                ZellijPlacement::Floating {
                    x: Some(0),
                    y: Some(7),
                    width: Some(SizeSpec::Percent(100)),
                    height: Some(SizeSpec::Percent(30)),
                }
            );
        }
    }

    #[test]
    fn dimensions_reject_out_of_range_values() {
        assert!(matches!(
            normalize_placement(
                ZellijPaneKind::Overlay,
                ZellijSplitDirection::Down,
                Some(SizeSpec::Cells(0)),
                None,
                None,
            ),
            Err(LaunchError::InvalidSize { .. })
        ));
        assert!(matches!(
            normalize_placement(
                ZellijPaneKind::Popup,
                ZellijSplitDirection::Down,
                Some(SizeSpec::Percent(0)),
                None,
                None,
            ),
            Err(LaunchError::InvalidSize { .. })
        ));
        assert!(matches!(
            normalize_placement(
                ZellijPaneKind::Overlay,
                ZellijSplitDirection::Down,
                None,
                Some(SizeSpec::Percent(101)),
                None,
            ),
            Err(LaunchError::InvalidSize { .. })
        ));
    }

    #[test]
    fn cwd_resolution_prefers_origin_then_override() {
        let origin = Path::new("/origin");
        assert_eq!(
            resolve_launch_cwd(origin, None).expect("omitted uses origin"),
            PathBuf::from("/origin")
        );
        assert_eq!(
            resolve_launch_cwd(origin, Some(Path::new("/elsewhere"))).expect("absolute wins"),
            PathBuf::from("/elsewhere")
        );
        assert_eq!(
            resolve_launch_cwd(origin, Some(Path::new("sub/dir"))).expect("relative joins once"),
            PathBuf::from("/origin/sub/dir")
        );
        assert!(matches!(
            resolve_launch_cwd(Path::new("relative"), None),
            Err(LaunchError::RelativeOriginCwd)
        ));
    }

    #[test]
    fn argv_split_preserves_boundaries_and_rejects_non_utf8() {
        let (program, args) = split_argv(vec![
            OsString::from("muxe"),
            OsString::from("ui"),
            OsString::from("menu main"),
        ])
        .expect("splits");
        assert_eq!(program, PathBuf::from("muxe"));
        assert_eq!(args, vec!["ui".to_owned(), "menu main".to_owned()]);
        assert!(matches!(
            split_argv(Vec::new()),
            Err(LaunchError::EmptyArgv)
        ));
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let bad = OsString::from_vec(vec![0xff, 0xfe]);
            assert!(matches!(
                split_argv(vec![OsString::from("prog"), bad]),
                Err(LaunchError::NonUtf8Arg { index: 1 })
            ));
        }
    }

    #[test]
    fn ui_argv_matches_canonical_menu_invocation_only() {
        let ui: Vec<String> = ["muxe", "ui", "menu", "main"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert!(is_ui_argv(&ui));
        let pathed: Vec<String> = ["/opt/mise/shims/muxe", "ui", "menu", "main"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        assert!(is_ui_argv(&pathed));
        for argv in [
            Vec::new(),
            vec!["muxe".to_owned()],
            vec!["muxe".to_owned(), "ui".to_owned()],
            vec!["muxe".to_owned(), "menu".to_owned(), "main".to_owned()],
            vec!["muxe".to_owned(), "ui".to_owned(), "other".to_owned()],
            vec!["sh".to_owned(), "ui".to_owned(), "menu".to_owned()],
            vec!["muxe".to_owned(), "ui".to_owned(), "menu main".to_owned()],
        ] {
            assert!(!is_ui_argv(&argv), "not a UI invocation: {argv:?}");
        }
    }

    #[test]
    fn empty_program_and_relative_cwd_fail() {
        let mut launch = menu_launch();
        launch.program = PathBuf::new();
        assert!(matches!(
            launch.into_command(),
            Err(LaunchError::EmptyProgram)
        ));
        let mut launch = menu_launch();
        launch.cwd = Some(PathBuf::from("relative/path"));
        assert!(matches!(
            launch.into_command(),
            Err(LaunchError::RelativeCwd)
        ));
    }

    #[test]
    fn built_launch_validates_through_generated_conversion() {
        use muxe_zellij_protocol::generated::ValidatedNativeCommand;
        let (raw, _) = menu_launch().into_command().expect("builds");
        ValidatedNativeCommand::try_from(raw).expect("generated conversion accepts launch");
    }
}
