use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use muxe_adapter_api::launch::is_ui_argv;
use muxe_adapter_api::{AdapterError, AdapterErrorKind};
use muxe_core::{Dimension, ExecutablePaneType, PaneId, TabId, WorkspaceId};
use serde_json::{Map, Value, json};

use crate::{
    HerdrExecutionDiagnostic, HerdrResponse, HerdrRuntime,
    runtime::{HerdrRequestAuthority, IncarnationEpoch, RuntimeTransactionAuthority},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UiSplitDirection {
    Right,
    Down,
}

impl UiSplitDirection {
    pub(crate) fn wire(self) -> &'static str {
        match self {
            Self::Right => "right",
            Self::Down => "down",
        }
    }
}

/// The concrete, broker-gated Herdr launch request. Its command is constrained to the canonical
/// `muxe ui menu [--theme name] [--color-scheme name] root` argv; only those ordinary UI
/// arguments vary, while the broker bootstrap uses the explicit environment map.
#[derive(Clone, Debug, PartialEq)]
pub struct UiPaneLaunch {
    pub origin_workspace: WorkspaceId,
    pub origin_tab: TabId,
    pub origin_pane: PaneId,
    pub cwd: PathBuf,
    pub argv: Vec<String>,
    pub bootstrap_env: BTreeMap<String, String>,
    pub direction: UiSplitDirection,
    pub ratio: f64,
    pub focus: bool,
}

/// The concrete identities returned by the first `layout.apply` phase. Callers must register this
/// pane with the broker before asking Herdr to move it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedUiPane {
    pub temporary_tab: TabId,
    pub ui_pane: PaneId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UiPanePlacement {
    pub temporary_tab: TabId,
    pub ui_pane: PaneId,
}

/// Immutable live origin captured from `session.snapshot` for a direct command-pane launch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FocusedPane {
    pub workspace: WorkspaceId,
    pub tab: TabId,
    pub pane: PaneId,
    pub cwd: PathBuf,
    pub columns: u16,
    pub rows: u16,
}

/// A direct generic command-pane launch. Unlike [`UiPaneLaunch`], its argv is the exact user
/// command vector rather than Muxe's constrained UI command line.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandPaneLaunch {
    /// Immutable launch-time cwd source.
    pub origin: FocusedPane,
    /// Validated live parent pane that receives the moved command pane.
    pub destination: FocusedPane,
    pub cwd: PathBuf,
    pub argv: Vec<String>,
    pub direction: UiSplitDirection,
    pub ratio: f64,
    pub focus: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandPanePlacement {
    pub temporary_tab: TabId,
    pub pane: PaneId,
}

/// A direct command tab launch. The tab and its root pane are one
/// `layout.apply` request so program, argv, cwd, label, workspace, and focus
/// remain host-owned rather than relying on the transient current pane.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandTabLaunch {
    pub workspace: WorkspaceId,
    pub label: Option<String>,
    pub cwd: PathBuf,
    pub argv: Vec<String>,
    pub focus: bool,
}

/// Converts a CLI or portable popup dimension through the pinned Herdr schema.
/// Cell bounds remain host-owned for the CLI; portable validation requires positive cells.
///
/// # Errors
///
/// Returns an adapter error for percentages outside the host's 1%-100% grammar.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the shared boundary error; boxing this pure converter would add allocation"
)]
pub fn popup_size(
    dimension: Dimension,
) -> Result<crate::generated::request_types::PopupSize, AdapterError> {
    use crate::generated::request_types::PopupSize;
    match dimension {
        Dimension::Cells(cells) => Ok(PopupSize::Integer(cells)),
        Dimension::Percent(percent) => format!("{percent}%")
            .parse()
            .map(PopupSize::String)
            .map_err(|_| invalid("popup percentages must be between 1% and 100%")),
    }
}

/// Validates portable-only dimension bounds without introducing position fields.
/// An unresolved existing direction postpones the split-axis check until dispatch.
pub(crate) fn validate_portable_dimensions(
    pane_type: ExecutablePaneType,
    direction: Option<UiSplitDirection>,
    width: Option<Dimension>,
    height: Option<Dimension>,
) -> Result<(), &'static str> {
    match pane_type {
        ExecutablePaneType::Overlay if width.is_some() || height.is_some() => {
            return Err("Herdr executable overlays do not support width or height; use popup");
        }
        ExecutablePaneType::Split => {
            if width.is_some() && height.is_some() {
                return Err(
                    "Herdr splits support only width for rightward splits or height for downward splits",
                );
            }
            if matches!(direction, Some(UiSplitDirection::Down)) && width.is_some()
                || matches!(direction, Some(UiSplitDirection::Right)) && height.is_some()
            {
                return Err(
                    "Herdr splits support only width for rightward splits or height for downward splits",
                );
            }
        }
        _ => {}
    }
    for (dimension, message) in [
        (
            width,
            "pane:open width must be positive cells or an integer percentage from 1% to 100%",
        ),
        (
            height,
            "pane:open height must be positive cells or an integer percentage from 1% to 100%",
        ),
    ] {
        if dimension.is_some_and(|value| !value.is_valid_size()) {
            return Err(message);
        }
        if pane_type == ExecutablePaneType::Split
            && matches!(dimension, Some(Dimension::Percent(percent)) if !(10..=90).contains(&percent))
        {
            return Err("Herdr split percentages must be between 10% and 90%");
        }
    }
    Ok(())
}

/// Returns the pinned host ratio for an exact CLI/portable new-pane size.
///
/// # Errors
///
/// Returns an adapter error for the unsupported axis or sizes outside 10%-90%
/// of the saved parent pane's live split axis.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the shared boundary error; boxing the ratio validator would add allocation"
)]
pub fn command_pane_ratio(
    width: Option<Dimension>,
    height: Option<Dimension>,
    destination: &FocusedPane,
    direction: UiSplitDirection,
) -> Result<f64, AdapterError> {
    let (requested, unsupported, available) = match direction {
        UiSplitDirection::Down => (height, width, destination.rows),
        UiSplitDirection::Right => (width, height, destination.columns),
    };
    if unsupported.is_some() {
        return Err(invalid(
            "use --height for a downward Herdr split and --width for a rightward split; the other dimension is unsupported",
        ));
    }
    let requested_ratio = match requested {
        None => return Ok(0.5),
        Some(Dimension::Percent(percent)) if (10..=90).contains(&percent) => {
            f64::from(percent) / 100.0
        }
        Some(Dimension::Percent(_)) => {
            return Err(invalid(
                "Herdr split percentage must be between 10% and 90%",
            ));
        }
        Some(Dimension::Cells(cells)) => {
            if cells == 0 || cells >= available {
                return Err(invalid(format!(
                    "the requested Herdr split size is {cells} cells; it must be greater than zero and smaller than the parent pane's {available} cells in the split direction"
                )));
            }
            let cells = u32::from(cells);
            let available = u32::from(available);
            if cells * 10 < available || cells * 10 > available * 9 {
                return Err(invalid(format!(
                    "the requested Herdr split size is {cells} of {available} parent-pane cells; it must occupy between 10% and 90% in the split direction"
                )));
            }
            f64::from(cells) / f64::from(available)
        }
    };
    // The host assigns ratio to the old first child; the requested size names the new second child.
    Ok(1.0 - requested_ratio)
}
/// Persistent Herdr plugin used only for real executable overlays and popups.
pub const COMMAND_PANE_PLUGIN_ID: &str = "muxe.command-panes";
pub const COMMAND_PANE_ENTRYPOINT: &str = "command";
/// Exact argv transport consumed and removed by `muxe pane exec` before OS execution.
pub const COMMAND_PANE_ARGV_ENV: &str = "MUXE_COMMAND_PANE_ARGV";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginCommandPaneType {
    Overlay,
    Popup,
}

/// Popup terminals have no public pane ID; never invent one for them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginCommandPanePlacement {
    Pane(PaneId),
    Popup,
}

#[derive(Clone, Debug)]
pub struct PluginCommandPaneLaunch {
    pub origin: FocusedPane,
    pub cwd: PathBuf,
    pub argv: Vec<String>,
    pub pane_type: PluginCommandPaneType,
    pub width: Option<crate::generated::request_types::PopupSize>,
    pub height: Option<crate::generated::request_types::PopupSize>,
}

/// Opens an exact command through Muxe's automatically managed persistent plugin.
/// Registration is verified/repaired inside the same ordered host transaction;
/// the host owns the command terminal and its lifetime.
///
/// # Errors
///
/// Returns [`AdapterError`] for unsupported host methods, invalid command or
/// placement values, an inactive saved origin, host rejection, or transport
/// failure. A request that may have executed or a successful response with
/// invalid placement evidence is reported as an unknown outcome.
pub async fn open_plugin_command_pane(
    runtime: &HerdrRuntime,
    launch: PluginCommandPaneLaunch,
) -> Result<PluginCommandPanePlacement, AdapterError> {
    runtime.validate_method_set(&[
        "session.snapshot",
        "plugin.list",
        "plugin.link",
        "plugin.pane.open",
    ])?;
    let lease = runtime.lease(IncarnationEpoch::INITIAL);
    let cache = runtime.cache().clone();
    runtime
        .run_ordered(move |invoker| async move {
            let authority = RuntimeTransactionAuthority::new(invoker, lease);
            open_plugin_command_pane_with(&authority, &cache, launch).await
        })
        .await?
}

pub(crate) async fn open_plugin_command_pane_with(
    authority: &dyn HerdrRequestAuthority,
    cache: &crate::HerdrCache,
    launch: PluginCommandPaneLaunch,
) -> Result<PluginCommandPanePlacement, AdapterError> {
    if !launch.cwd.is_absolute() || launch.argv.first().is_none_or(String::is_empty) {
        return Err(invalid(
            "A plugin command pane requires an absolute working directory and a nonempty program",
        ));
    }
    if launch.pane_type == PluginCommandPaneType::Overlay
        && (launch.width.is_some() || launch.height.is_some())
    {
        return Err(invalid(
            "Herdr executable overlays do not support dimensions",
        ));
    }
    if launch.argv.iter().any(|word| word.contains('\0')) {
        return Err(invalid("Command argv cannot contain NUL bytes"));
    }
    let cwd = launch.cwd.to_str().ok_or_else(|| invalid(
        "The Herdr plugin command-pane working directory must be UTF-8 because the JSON socket API represents paths as strings",
    ))?;
    if cwd.contains('\0') {
        return Err(invalid(
            "The Herdr plugin command-pane working directory cannot contain NUL bytes",
        ));
    }
    let active = focused_pane_with(authority).await?;
    if active.workspace != launch.origin.workspace
        || active.tab != launch.origin.tab
        || active.pane != launch.origin.pane
    {
        return Err(
            invalid("The saved origin pane, tab, and workspace must be focused before Herdr can open an overlay or popup")
                .with_safe_diagnostic(HerdrExecutionDiagnostic::OriginNotActive.safe()),
        );
    }
    let argv = serde_json::to_string(&launch.argv).map_err(|error| {
        invalid(format!(
            "Could not encode the command's argument list as JSON: {error}"
        ))
    })?;
    let placement = match launch.pane_type {
        PluginCommandPaneType::Overlay => "overlay",
        PluginCommandPaneType::Popup => "popup",
    };
    crate::managed_plugin::ensure_with(authority, cache).await?;
    // Setup can make several host requests. Another client may change focus during repair;
    // confirm the saved origin again immediately before opening the executable terminal.
    let active = focused_pane_with(authority).await?;
    if active.workspace != launch.origin.workspace
        || active.tab != launch.origin.tab
        || active.pane != launch.origin.pane
    {
        return Err(
            invalid("The saved origin pane, tab, and workspace must be focused before Herdr can open an overlay or popup")
                .with_safe_diagnostic(HerdrExecutionDiagnostic::OriginNotActive.safe()),
        );
    }
    let response = authority
        .request(
            "plugin.pane.open",
            json!({
                "plugin_id": COMMAND_PANE_PLUGIN_ID,
                "entrypoint": COMMAND_PANE_ENTRYPOINT,
                "placement": placement,
                "focus": true,
                "cwd": cwd,
                "env": { COMMAND_PANE_ARGV_ENV: argv },
                "width": launch.width,
                "height": launch.height,
            }),
        )
        .await?;
    plugin_command_pane_response(response, launch.pane_type)
}

/// A success envelope follows a mutating request: malformed placement evidence cannot
/// establish that the command did not launch, and must never invite an automatic repeat.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the shared adapter boundary error; boxing only this decoder would add allocation without changing its callers' error API"
)]
fn plugin_command_pane_response(
    response: HerdrResponse,
    pane_type: PluginCommandPaneType,
) -> Result<PluginCommandPanePlacement, AdapterError> {
    let uncertain = |cause: String| {
        AdapterError::new(
            AdapterErrorKind::OutcomeUnknown,
            format!(
                "Herdr reported success for plugin.pane.open, but Muxe could not confirm where the command pane opened: {cause}. \
            The command may already be running. Inspect Herdr before deciding whether to repeat it."
            ),
        ).with_safe_diagnostic(HerdrExecutionDiagnostic::CommandPaneOutcomeUnknown.safe())
    };
    match response {
        HerdrResponse::Success(result) => {
            if pane_type == PluginCommandPaneType::Popup {
                if result.get("type").and_then(Value::as_str) != Some("ok") {
                    return Err(uncertain(
                        "the popup response did not have type ok".to_owned(),
                    ));
                }
                Ok(PluginCommandPanePlacement::Popup)
            } else {
                if result.get("type").and_then(Value::as_str) != Some("plugin_pane_opened") {
                    return Err(uncertain(
                        "the overlay response did not have type plugin_pane_opened".to_owned(),
                    ));
                }
                let pane = result
                    .get("plugin_pane")
                    .and_then(|pane| pane.get("pane"))
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        uncertain(
                            "the overlay response did not contain a plugin_pane.pane object"
                                .to_owned(),
                        )
                    })?;
                let id = required_id(pane, "pane_id", "plugin.pane.open")
                    .map_err(|error| uncertain(error.message))?;
                Ok(PluginCommandPanePlacement::Pane(PaneId::new(id)))
            }
        }
        HerdrResponse::Error { code, .. } => {
            let cause = match code.as_str() {
                "plugin_not_found" => HerdrExecutionDiagnostic::CommandPanePluginMissing,
                "plugin_disabled" => HerdrExecutionDiagnostic::CommandPanePluginDisabled,
                _ => HerdrExecutionDiagnostic::CommandPaneRejected,
            };
            Err(
                AdapterError::new(AdapterErrorKind::DispatchFailed, cause.safe().message())
                    .with_safe_diagnostic(cause.safe()),
            )
        }
    }
}

/// Reads one schema-validated snapshot and captures the focused workspace/tab/pane/cwd tuple.
/// A direct launcher never substitutes a broker process cwd, home directory, or root directory
/// when Herdr has no absolute current-pane directory.
///
/// # Errors
///
/// Returns `AdapterError` when the snapshot cannot be read or has no focused
/// pane with an absolute cwd and live geometry.
pub async fn focused_pane(runtime: &HerdrRuntime) -> Result<FocusedPane, AdapterError> {
    focused_pane_with(runtime).await
}

pub(crate) async fn focused_pane_with(
    authority: &dyn HerdrRequestAuthority,
) -> Result<FocusedPane, AdapterError> {
    let snapshot = session_snapshot(authority).await?;
    let workspace = WorkspaceId::new(required_id(
        &snapshot,
        "focused_workspace_id",
        "session.snapshot",
    )?);
    let tab = TabId::new(required_id(
        &snapshot,
        "focused_tab_id",
        "session.snapshot",
    )?);
    let pane = PaneId::new(required_id(
        &snapshot,
        "focused_pane_id",
        "session.snapshot",
    )?);
    pane_from_snapshot(&snapshot, workspace, tab, pane)
}

/// Validates a launcher-provided pane identity against one fresh snapshot and enriches it with
/// only that live pane's absolute cwd. It never falls back to whatever pane later acquired focus.
///
/// # Errors
///
/// Returns `AdapterError` when the snapshot cannot be read or the given
/// identity has no live pane.
pub async fn pane_by_identity(
    runtime: &HerdrRuntime,
    workspace: WorkspaceId,
    tab: TabId,
    pane: PaneId,
) -> Result<FocusedPane, AdapterError> {
    pane_by_identity_with(runtime, workspace, tab, pane).await
}

pub(crate) async fn pane_by_identity_with(
    authority: &dyn HerdrRequestAuthority,
    workspace: WorkspaceId,
    tab: TabId,
    pane: PaneId,
) -> Result<FocusedPane, AdapterError> {
    let snapshot = session_snapshot(authority).await?;
    pane_from_snapshot(&snapshot, workspace, tab, pane)
}

/// Resolves an explicit launcher parent-pane ID through one fresh host inventory snapshot.
///
/// # Errors
///
/// Returns `AdapterError` when the pane ID is empty, the snapshot cannot be
/// read, or the pane is not live.
pub async fn pane_by_id(
    runtime: &HerdrRuntime,
    pane_id: &str,
) -> Result<FocusedPane, AdapterError> {
    pane_by_id_with(runtime, pane_id).await
}

pub(crate) async fn pane_by_id_with(
    authority: &dyn HerdrRequestAuthority,
    pane_id: &str,
) -> Result<FocusedPane, AdapterError> {
    if pane_id.is_empty() {
        return Err(invalid("Herdr parent pane ID must not be empty"));
    }
    let snapshot = session_snapshot(authority).await?;
    let pane = snapshot
        .get("panes")
        .and_then(Value::as_array)
        .and_then(|panes| {
            panes
                .iter()
                .filter_map(Value::as_object)
                .find(|candidate| candidate.get("pane_id").and_then(Value::as_str) == Some(pane_id))
        })
        .ok_or_else(|| {
            invalid("The specified Herdr parent pane is not present in the live session snapshot")
        })?;
    let workspace = WorkspaceId::new(required_id(pane, "workspace_id", "session.snapshot")?);
    let tab = TabId::new(required_id(pane, "tab_id", "session.snapshot")?);
    pane_from_snapshot(&snapshot, workspace, tab, PaneId::new(pane_id))
}
async fn session_snapshot(
    authority: &dyn HerdrRequestAuthority,
) -> Result<Map<String, Value>, AdapterError> {
    let result = result_object(
        &invoke(authority, "session.snapshot", Value::Object(Map::new())).await?,
        "session.snapshot",
    )?;
    if result.get("type").and_then(Value::as_str) != Some("session_snapshot") {
        return Err(invalid(
            "Herdr session.snapshot result must have type \"session_snapshot\"",
        ));
    }
    result
        .get("snapshot")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| invalid("Herdr session.snapshot result must contain a snapshot object"))
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn pane_from_snapshot(
    snapshot: &Map<String, Value>,
    workspace: WorkspaceId,
    tab: TabId,
    pane: PaneId,
) -> Result<FocusedPane, AdapterError> {
    let captured_pane = snapshot
        .get("panes")
        .and_then(Value::as_array)
        .and_then(|panes| {
            panes.iter().filter_map(Value::as_object).find(|candidate| {
                candidate.get("workspace_id").and_then(Value::as_str) == Some(workspace.as_str())
                    && candidate.get("tab_id").and_then(Value::as_str) == Some(tab.as_str())
                    && candidate.get("pane_id").and_then(Value::as_str) == Some(pane.as_str())
            })
        })
        .ok_or_else(|| {
            invalid("The saved Herdr origin pane was not found in its workspace and tab in the session snapshot")
                .with_safe_diagnostic(HerdrExecutionDiagnostic::OriginUnavailable.safe())
        })?;
    let cwd = captured_pane
        .get("cwd")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|cwd| cwd.is_absolute())
        .ok_or_else(|| {
            invalid("Herdr did not report an absolute working directory for the saved origin pane")
                .with_safe_diagnostic(HerdrExecutionDiagnostic::CapturedCwdUnavailable.safe())
        })?;
    let (columns, rows) = snapshot
        .get("layouts")
        .and_then(Value::as_array)
        .and_then(|layouts| {
            layouts.iter().filter_map(Value::as_object).find(|layout| {
                layout.get("workspace_id").and_then(Value::as_str) == Some(workspace.as_str())
                    && layout.get("tab_id").and_then(Value::as_str) == Some(tab.as_str())
            })
        })
        .and_then(|layout| layout.get("panes").and_then(Value::as_array))
        .and_then(|panes| {
            panes
                .iter()
                .filter_map(Value::as_object)
                .find_map(|candidate| {
                    (candidate.get("pane_id").and_then(Value::as_str) == Some(pane.as_str()))
                        .then(|| candidate.get("rect").and_then(Value::as_object))
                        .flatten()
                })
        })
        .and_then(|rect| {
            let columns = rect.get("width").and_then(Value::as_u64)?;
            let rows = rect.get("height").and_then(Value::as_u64)?;
            Some((u16::try_from(columns).ok()?, u16::try_from(rows).ok()?))
        })
        .filter(|(columns, rows)| *columns > 0 && *rows > 0)
        .ok_or_else(|| {
            invalid("Herdr did not report positive row and column counts for the saved origin pane")
                .with_safe_diagnostic(HerdrExecutionDiagnostic::OriginGeometryUnavailable.safe())
        })?;
    Ok(FocusedPane {
        workspace,
        tab,
        pane,
        cwd,
        columns,
        rows,
    })
}

/// Opens an exact user command in a focused-origin split. The launch is still a two-step
/// transaction: only the returned temporary tab is cleaned up if the move fails.
///
/// # Errors
///
/// Returns `AdapterError` when the launch is invalid or the layout or move
/// requests fail.
pub async fn open_command_pane(
    runtime: &HerdrRuntime,
    launch: CommandPaneLaunch,
) -> Result<CommandPanePlacement, AdapterError> {
    runtime.validate_method_set(&["layout.apply", "pane.move", "tab.close"])?;
    let lease = runtime.lease(IncarnationEpoch::INITIAL);
    runtime
        .run_ordered(move |invoker| async move {
            let authority = RuntimeTransactionAuthority::new(invoker, lease);
            open_command_pane_with(&authority, launch).await
        })
        .await?
}

pub(crate) async fn open_command_pane_with(
    authority: &dyn HerdrRequestAuthority,
    launch: CommandPaneLaunch,
) -> Result<CommandPanePlacement, AdapterError> {
    if !launch.cwd.is_absolute() {
        return Err(invalid(
            "The Herdr command pane's working directory must be an absolute path",
        ));
    }
    if launch.argv.first().is_none_or(String::is_empty) {
        return Err(invalid(
            "The Herdr command pane's argument list must begin with a nonempty program",
        ));
    }
    if !(launch.ratio.is_finite() && 0.0 < launch.ratio && launch.ratio < 1.0) {
        return Err(invalid(
            "Herdr command-pane split ratio must be strictly between zero and one",
        ));
    }
    let layout = invoke(
        authority,
        "layout.apply",
        json!({
            "focus": false,
            "workspace_id": launch.destination.workspace.as_str(),
            "root": {
                "type": "pane",
                "command": launch.argv,
                "cwd": launch.cwd,
                "env": {},
            },
        }),
    )
    .await?;
    let layout = layout_apply_result(&layout)?;
    let root = layout
        .get("root")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Herdr layout.apply result must contain a root object",
            )
        })?;
    let prepared = PreparedUiPane {
        temporary_tab: TabId::new(required_id(&layout, "tab_id", "layout.apply")?),
        ui_pane: PaneId::new(required_id(root, "pane_id", "layout.apply root")?),
    };
    let moved = invoke(
        authority,
        "pane.move",
        json!({
            "pane_id": prepared.ui_pane.as_str(),
            "focus": launch.focus,
            "destination": {
                "type": "tab",
                "tab_id": launch.destination.tab.as_str(),
                "target_pane_id": launch.destination.pane.as_str(),
                "split": launch.direction.wire(),
                "ratio": launch.ratio,
            },
        }),
    )
    .await;
    let moved = match moved {
        Ok(value) => changed(&value, "pane.move"),
        Err(error) => Err(error),
    };
    match moved {
        Ok(()) => Ok(CommandPanePlacement {
            temporary_tab: prepared.temporary_tab,
            pane: prepared.ui_pane,
        }),
        Err(move_error) => {
            if let Err(cleanup_error) =
                close_transient_tab_with(authority, &prepared.temporary_tab).await
            {
                return Err(AdapterError::new(
                    combined_move_error_kind(&move_error, &cleanup_error),
                    format!(
                        "The Herdr command-pane move request failed: {move_error}. Closing the temporary tab returned by Herdr also failed: {cleanup_error}"
                    ),
                ));
            }
            Err(move_error)
        }
    }
}

/// Opens one exact user command as the root pane of a new tab.
///
/// # Errors
///
/// Returns [`AdapterError`] when the command or cwd is invalid, or Herdr
/// rejects the single `layout.apply` request.
pub async fn open_command_tab(
    runtime: &HerdrRuntime,
    launch: CommandTabLaunch,
) -> Result<(), AdapterError> {
    runtime.validate_method_set(&["layout.apply"])?;
    let lease = runtime.lease(IncarnationEpoch::INITIAL);
    runtime
        .run_ordered(move |invoker| async move {
            let authority = RuntimeTransactionAuthority::new(invoker, lease);
            open_command_tab_with(&authority, launch).await
        })
        .await?
}

pub(crate) async fn open_command_tab_with(
    authority: &dyn HerdrRequestAuthority,
    launch: CommandTabLaunch,
) -> Result<(), AdapterError> {
    if !launch.cwd.is_absolute() {
        return Err(invalid(
            "The Herdr command tab's working directory must be an absolute path",
        ));
    }
    if launch.argv.first().is_none_or(String::is_empty) {
        return Err(invalid(
            "The Herdr command tab's argument list must begin with a nonempty program",
        ));
    }
    let layout = invoke(
        authority,
        "layout.apply",
        json!({
            "focus": launch.focus,
            "workspace_id": launch.workspace.as_str(),
            "tab_label": launch.label,
            "root": {
                "type": "pane",
                "command": launch.argv,
                "cwd": launch.cwd,
                "env": {},
            },
        }),
    )
    .await?;
    let _ = layout_apply_result(&layout)?;
    Ok(())
}

/// Executes only trampoline step 2. The returned pane is still in the temporary tab and must be
/// registered with the pending-launch gate before it is moved.
///
/// # Errors
///
/// Returns `AdapterError` when the launch is invalid or the layout request fails.
pub async fn prepare_ui_pane(
    runtime: &HerdrRuntime,
    launch: &UiPaneLaunch,
) -> Result<PreparedUiPane, AdapterError> {
    prepare_ui_pane_with(runtime, launch).await
}

pub(crate) async fn prepare_ui_pane_with(
    authority: &dyn HerdrRequestAuthority,
    launch: &UiPaneLaunch,
) -> Result<PreparedUiPane, AdapterError> {
    if !launch.cwd.is_absolute() {
        return Err(invalid(
            "The Herdr UI launch working directory must be an absolute path",
        ));
    }
    validate_ui_argv(&launch.argv)?;
    validate_bootstrap_env(&launch.bootstrap_env)?;
    if !(launch.ratio.is_finite() && 0.0 < launch.ratio && launch.ratio < 1.0) {
        return Err(invalid(
            "Herdr UI launch split ratio must be strictly between zero and one",
        ));
    }

    let layout = invoke(
        authority,
        "layout.apply",
        json!({
            "focus": false,
            "workspace_id": launch.origin_workspace.as_str(),
            "root": {
                "type": "pane",
                "command": launch.argv,
                "cwd": launch.cwd,
                "env": launch.bootstrap_env,
            },
        }),
    )
    .await?;
    let layout = layout_apply_result(&layout)?;
    let root = layout
        .get("root")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Herdr layout.apply result must contain a root object",
            )
        })?;
    Ok(PreparedUiPane {
        temporary_tab: TabId::new(required_id(&layout, "tab_id", "layout.apply")?),
        ui_pane: PaneId::new(required_id(root, "pane_id", "layout.apply root")?),
    })
}

/// Executes trampoline step 4. Failure closes only the temporary tab returned by this launch.
///
/// # Errors
///
/// Returns `AdapterError` when the pane move fails; the transient tab is closed
/// on failure and a failed cleanup is reported instead.
pub async fn move_prepared_ui_pane(
    runtime: &HerdrRuntime,
    launch: &UiPaneLaunch,
    prepared: PreparedUiPane,
) -> Result<UiPanePlacement, AdapterError> {
    runtime.validate_method_set(&["pane.move", "tab.close"])?;
    let lease = runtime.lease(IncarnationEpoch::INITIAL);
    let launch = launch.clone();
    runtime
        .run_ordered(move |invoker| async move {
            let authority = RuntimeTransactionAuthority::new(invoker, lease);
            move_prepared_ui_pane_with(&authority, &launch, prepared).await
        })
        .await?
}

pub(crate) async fn move_prepared_ui_pane_with(
    authority: &dyn HerdrRequestAuthority,
    launch: &UiPaneLaunch,
    prepared: PreparedUiPane,
) -> Result<UiPanePlacement, AdapterError> {
    let moved = invoke(
        authority,
        "pane.move",
        json!({
            "pane_id": prepared.ui_pane.as_str(),
            "focus": launch.focus,
            "destination": {
                "type": "tab",
                "tab_id": launch.origin_tab.as_str(),
                "target_pane_id": launch.origin_pane.as_str(),
                "split": launch.direction.wire(),
                "ratio": launch.ratio,
            },
        }),
    )
    .await;
    let moved = match moved {
        Ok(value) => changed(&value, "pane.move"),
        Err(error) => Err(error),
    };
    match moved {
        Ok(()) => Ok(UiPanePlacement {
            temporary_tab: prepared.temporary_tab,
            ui_pane: prepared.ui_pane,
        }),
        Err(move_error) => {
            if let Err(cleanup_error) =
                close_transient_tab_with(authority, &prepared.temporary_tab).await
            {
                return Err(AdapterError::new(
                    combined_move_error_kind(&move_error, &cleanup_error),
                    format!(
                        "The Herdr UI pane move request failed: {move_error}. Closing the temporary tab returned by Herdr also failed: {cleanup_error}"
                    ),
                ).with_safe_diagnostic(HerdrExecutionDiagnostic::CleanupFailed.safe()));
            }
            Err(move_error)
        }
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn validate_ui_argv(argv: &[String]) -> Result<(), AdapterError> {
    let executable_is_allowed = argv
        .first()
        .is_some_and(|value| value == "muxe" || Path::new(value).is_absolute());
    if !is_ui_argv(argv) || !executable_is_allowed {
        return Err(invalid(
            "The Herdr UI launch command must begin with `muxe ui menu` or an absolute executable path followed by `ui menu`",
        ));
    }
    let mut position = 3;
    for flag in ["--theme", "--color-scheme"] {
        if argv.get(position).map(String::as_str) == Some(flag) {
            let Some(value) = argv.get(position + 1).filter(|value| !value.is_empty()) else {
                return Err(invalid(format!(
                    "Herdr UI launch {flag} requires a nonempty value"
                )));
            };
            if value.starts_with('-') {
                return Err(invalid(format!(
                    "Herdr UI launch {flag} value must not be another option"
                )));
            }
            position += 2;
        }
    }
    let Some(root) = argv.get(position).filter(|value| !value.is_empty()) else {
        return Err(invalid("Herdr UI launch requires a nonempty root argument"));
    };
    if root.starts_with('-') || argv.len() != position + 1 {
        return Err(invalid(
            "Herdr UI launch accepts only theme, color-scheme, and root arguments",
        ));
    }
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn validate_bootstrap_env(env: &BTreeMap<String, String>) -> Result<(), AdapterError> {
    const REQUIRED: [&str; 4] = [
        "MUXE_HERDR_ORIGIN_WORKSPACE_ID",
        "MUXE_HERDR_ORIGIN_TAB_ID",
        "MUXE_HERDR_ORIGIN_PANE_ID",
        "MUXE_PENDING_LAUNCH_TOKEN",
    ];
    const OPTIONAL: &str = "MUXE_HERDR_ORIGIN_PANE_CWD";
    for key in REQUIRED {
        if env.get(key).is_none_or(String::is_empty) {
            return Err(invalid(format!(
                "Herdr UI launch requires a nonempty {key} environment variable"
            )));
        }
    }
    if env
        .get(OPTIONAL)
        .is_some_and(|cwd| cwd.is_empty() || !PathBuf::from(cwd).is_absolute())
    {
        return Err(invalid(
            "The Herdr UI launch origin working directory must be an absolute path when provided",
        ));
    }
    if env
        .keys()
        .any(|key| !REQUIRED.contains(&key.as_str()) && key != OPTIONAL)
    {
        return Err(invalid(
            "Herdr UI launch bootstrap environment contains an unrecognized variable",
        ));
    }
    Ok(())
}

/// Closes one transient tab created by a two-step launch.
///
/// # Errors
///
/// Returns `AdapterError` when the `tab.close` request fails.
pub async fn close_transient_tab(runtime: &HerdrRuntime, tab: &TabId) -> Result<(), AdapterError> {
    close_transient_tab_with(runtime, tab).await
}

pub(crate) async fn close_transient_tab_with(
    authority: &dyn HerdrRequestAuthority,
    tab: &TabId,
) -> Result<(), AdapterError> {
    invoke(authority, "tab.close", json!({ "tab_id": tab.as_str() }))
        .await
        .map(|_| ())
}

async fn invoke(
    authority: &dyn HerdrRequestAuthority,
    method: &str,
    params: Value,
) -> Result<Value, AdapterError> {
    match authority.request(method, params).await? {
        HerdrResponse::Success(result) => Ok(result),
        HerdrResponse::Error { code, message } => Err(AdapterError::new(
            AdapterErrorKind::DispatchFailed,
            format!("Herdr rejected {method} ({code}): {message}"),
        )),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn result_object(value: &Value, method: &str) -> Result<Map<String, Value>, AdapterError> {
    value.as_object().cloned().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::DispatchFailed,
            format!("Herdr {method} result is not an object"),
        )
    })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn layout_apply_result(value: &Value) -> Result<Map<String, Value>, AdapterError> {
    let result = result_object(value, "layout.apply")?;
    if result.get("type").and_then(Value::as_str) != Some("layout_apply") {
        return Err(AdapterError::new(
            AdapterErrorKind::DispatchFailed,
            "Herdr layout.apply result must have type \"layout_apply\"",
        ));
    }
    result
        .get("layout")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Herdr layout.apply result must contain a layout object",
            )
        })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn required_id(
    result: &Map<String, Value>,
    field: &str,
    method: &str,
) -> Result<String, AdapterError> {
    result
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} result must contain a nonempty {field} string"),
            )
        })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn changed(value: &Value, method: &str) -> Result<(), AdapterError> {
    let result = result_object(value, method)?;
    if result.get("type").and_then(Value::as_str) != Some("pane_move") {
        return Err(AdapterError::new(
            AdapterErrorKind::DispatchFailed,
            format!("Herdr {method} result must have type \"pane_move\""),
        ));
    }
    let move_result = result
        .get("move_result")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} result must contain a move_result object"),
            )
        })?;
    (move_result.get("changed").and_then(Value::as_bool) == Some(true))
        .then_some(())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} did not confirm that the pane moved: move_result.changed must be true"),
            )
        })
}

/// A move whose outcome may be unknown outranks a follow-up cleanup failure:
/// the caller must keep treating the host mutation as possibly applied.
fn combined_move_error_kind(
    move_error: &AdapterError,
    cleanup_error: &AdapterError,
) -> AdapterErrorKind {
    if move_error.kind == AdapterErrorKind::OutcomeUnknown
        || cleanup_error.kind == AdapterErrorKind::OutcomeUnknown
    {
        AdapterErrorKind::OutcomeUnknown
    } else {
        AdapterErrorKind::DispatchFailed
    }
}

fn invalid(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn popup_conversion_reuses_cli_grammar_without_global_cell_bounds() {
        for (dimension, expected) in [
            (Dimension::Percent(80), json!("80%")),
            (Dimension::Percent(100), json!("100%")),
            (Dimension::Cells(12), json!(12)),
            // The old CLI forwards zero cells to Herdr; only portable input rejects it.
            (Dimension::Cells(0), json!(0)),
        ] {
            assert_eq!(
                serde_json::to_value(popup_size(dimension).unwrap()).unwrap(),
                expected
            );
        }
        assert!(popup_size(Dimension::Percent(0)).is_err());
        assert!(popup_size(Dimension::Percent(101)).is_err());
    }

    #[test]
    fn split_dimensions_use_new_child_axis_and_live_parent_bounds() {
        let parent = FocusedPane {
            workspace: WorkspaceId::new("workspace"),
            tab: TabId::new("tab"),
            pane: PaneId::new("pane"),
            cwd: PathBuf::from("/tmp"),
            columns: 100,
            rows: 40,
        };
        for (direction, width, height, ratio) in [
            (
                UiSplitDirection::Right,
                Some(Dimension::Percent(80)),
                None,
                0.2,
            ),
            (
                UiSplitDirection::Down,
                None,
                Some(Dimension::Cells(12)),
                0.7,
            ),
            (
                UiSplitDirection::Right,
                Some(Dimension::Cells(10)),
                None,
                0.9,
            ),
            (
                UiSplitDirection::Right,
                Some(Dimension::Cells(90)),
                None,
                0.1,
            ),
        ] {
            assert!(
                (command_pane_ratio(width, height, &parent, direction).unwrap() - ratio).abs()
                    < f64::EPSILON
            );
        }
        for value in [
            Dimension::Cells(0),
            Dimension::Cells(9),
            Dimension::Cells(91),
            Dimension::Cells(100),
            Dimension::Percent(9),
            Dimension::Percent(91),
        ] {
            assert!(
                command_pane_ratio(Some(value), None, &parent, UiSplitDirection::Right).is_err()
            );
        }
        assert!(
            command_pane_ratio(
                Some(Dimension::Percent(80)),
                Some(Dimension::Percent(70)),
                &parent,
                UiSplitDirection::Right
            )
            .is_err()
        );
        assert!(
            command_pane_ratio(
                Some(Dimension::Cells(20)),
                None,
                &parent,
                UiSplitDirection::Down
            )
            .is_err()
        );
    }

    #[test]
    fn portable_dimension_placement_rejects_unsupported_axes_before_bounds() {
        assert!(
            validate_portable_dimensions(
                ExecutablePaneType::Popup,
                None,
                Some(Dimension::Percent(80)),
                Some(Dimension::Percent(70))
            )
            .is_ok()
        );
        assert_eq!(
            validate_portable_dimensions(
                ExecutablePaneType::Overlay,
                None,
                Some(Dimension::Cells(0)),
                None
            ),
            validate_portable_dimensions(
                ExecutablePaneType::Overlay,
                None,
                Some(Dimension::Cells(20)),
                None
            ),
        );
        assert!(
            validate_portable_dimensions(
                ExecutablePaneType::Split,
                Some(UiSplitDirection::Down),
                Some(Dimension::Percent(80)),
                None
            )
            .is_err()
        );
        assert!(
            validate_portable_dimensions(
                ExecutablePaneType::Split,
                Some(UiSplitDirection::Right),
                Some(Dimension::Percent(10)),
                None
            )
            .is_ok()
        );
        assert!(
            validate_portable_dimensions(
                ExecutablePaneType::Split,
                None,
                Some(Dimension::Percent(100)),
                None
            )
            .is_err()
        );
        assert!(
            validate_portable_dimensions(
                ExecutablePaneType::Popup,
                None,
                Some(Dimension::Percent(0)),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn live_pane_missing_cwd_is_not_classified_as_a_vanished_origin() {
        for cwd in [None, Some("relative-secret-cwd")] {
            let mut live_pane = json!({
                "workspace_id":"workspace", "tab_id":"tab", "pane_id":"pane",
            });
            if let Some(cwd) = cwd {
                live_pane
                    .as_object_mut()
                    .unwrap()
                    .insert("cwd".to_owned(), json!(cwd));
            }
            let snapshot = json!({"panes":[live_pane]}).as_object().unwrap().clone();
            let error = pane_from_snapshot(
                &snapshot,
                WorkspaceId::new("workspace"),
                TabId::new("tab"),
                PaneId::new("pane"),
            )
            .unwrap_err();
            assert_eq!(error.kind, AdapterErrorKind::InvalidRequest);
            assert_eq!(
                error.safe_message(),
                HerdrExecutionDiagnostic::CapturedCwdUnavailable
                    .safe()
                    .message()
            );
            assert_ne!(
                error.safe_message(),
                HerdrExecutionDiagnostic::OriginUnavailable.safe().message()
            );
            assert!(!error.safe_message().contains("relative-secret-cwd"));
        }
        let absent = json!({"panes":[]}).as_object().unwrap().clone();
        let error = pane_from_snapshot(
            &absent,
            WorkspaceId::new("workspace"),
            TabId::new("tab"),
            PaneId::new("pane"),
        )
        .unwrap_err();
        assert_eq!(
            error.safe_message(),
            HerdrExecutionDiagnostic::OriginUnavailable.safe().message()
        );
    }

    #[test]
    fn command_pane_durable_context_classifies_without_retaining_host_payload() {
        for (code, expected) in [
            (
                "plugin_not_found",
                HerdrExecutionDiagnostic::CommandPanePluginMissing,
            ),
            (
                "plugin_disabled",
                HerdrExecutionDiagnostic::CommandPanePluginDisabled,
            ),
            (
                "unknown-secret-code",
                HerdrExecutionDiagnostic::CommandPaneRejected,
            ),
        ] {
            let error = plugin_command_pane_response(
                HerdrResponse::Error {
                    code: code.to_owned(),
                    message: "secret argv and environment sentinel".to_owned(),
                },
                PluginCommandPaneType::Popup,
            )
            .unwrap_err();
            assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
            assert_eq!(error.safe_message(), expected.safe().message());
            assert!(!error.safe_message().contains("secret"));
        }
        let error = plugin_command_pane_response(
            HerdrResponse::Success(json!({"type":"secret-host-result"})),
            PluginCommandPaneType::Popup,
        )
        .unwrap_err();
        assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
        assert_eq!(
            error.safe_message(),
            HerdrExecutionDiagnostic::CommandPaneOutcomeUnknown
                .safe()
                .message()
        );
    }

    fn bootstrap_env() -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                "MUXE_HERDR_ORIGIN_WORKSPACE_ID".to_owned(),
                "workspace".to_owned(),
            ),
            ("MUXE_HERDR_ORIGIN_TAB_ID".to_owned(), "tab".to_owned()),
            ("MUXE_HERDR_ORIGIN_PANE_ID".to_owned(), "pane".to_owned()),
            (
                "MUXE_PENDING_LAUNCH_TOKEN".to_owned(),
                "launch-token".to_owned(),
            ),
        ])
    }

    #[test]
    fn accepts_only_the_canonical_ui_argv() {
        validate_ui_argv(&[
            "muxe".to_owned(),
            "ui".to_owned(),
            "menu".to_owned(),
            "--theme".to_owned(),
            "night".to_owned(),
            "--color-scheme".to_owned(),
            "solarized".to_owned(),
            "main".to_owned(),
        ])
        .expect("canonical UI argv is accepted");

        validate_ui_argv(&[
            "/opt/muxe/bin/muxe".to_owned(),
            "ui".to_owned(),
            "menu".to_owned(),
            "main".to_owned(),
        ])
        .expect("absolute muxe executable path is accepted");

        assert!(
            validate_ui_argv(&[
                "./muxe".to_owned(),
                "ui".to_owned(),
                "menu".to_owned(),
                "main".to_owned(),
            ])
            .is_err()
        );
        assert!(
            validate_ui_argv(&[
                "/opt/muxe/bin/not-muxe".to_owned(),
                "ui".to_owned(),
                "menu".to_owned(),
                "main".to_owned(),
            ])
            .is_err()
        );
        assert!(
            validate_ui_argv(&[
                "muxe".to_owned(),
                "ui".to_owned(),
                "menu".to_owned(),
                "--cwd".to_owned(),
                "/tmp".to_owned(),
                "main".to_owned(),
            ])
            .is_err()
        );
    }

    #[test]
    fn bootstrap_environment_is_closed_and_origin_bound() {
        let mut env = bootstrap_env();
        env.insert(
            "MUXE_HERDR_ORIGIN_PANE_CWD".to_owned(),
            "/worktree".to_owned(),
        );
        validate_bootstrap_env(&env).expect("declared bootstrap environment is accepted");

        env.insert("UNRELATED".to_owned(), "value".to_owned());
        assert!(validate_bootstrap_env(&env).is_err());
    }

    #[test]
    fn combined_move_error_kind_never_downgrades_unknown_moves() {
        let unknown = AdapterError::new(
            AdapterErrorKind::OutcomeUnknown,
            "move request closed after flush",
        );
        let failed = AdapterError::new(AdapterErrorKind::DispatchFailed, "cleanup rejected");

        assert_eq!(
            combined_move_error_kind(&unknown, &failed),
            AdapterErrorKind::OutcomeUnknown,
            "a possibly applied move must stay unknown when cleanup fails"
        );
        assert_eq!(
            combined_move_error_kind(&failed, &unknown),
            AdapterErrorKind::OutcomeUnknown,
            "a possibly applied cleanup must keep the mutation unknown"
        );
        assert_eq!(
            combined_move_error_kind(&failed, &failed),
            AdapterErrorKind::DispatchFailed
        );
    }
    #[test]
    fn malformed_plugin_creation_success_preserves_may_have_run_outcome() {
        for (pane_type, result) in [
            (PluginCommandPaneType::Popup, json!({"type":"other"})),
            (PluginCommandPaneType::Overlay, json!({"type":"ok"})),
            (
                PluginCommandPaneType::Overlay,
                json!({"type":"plugin_pane_opened"}),
            ),
            (
                PluginCommandPaneType::Overlay,
                json!({"type":"plugin_pane_opened","plugin_pane":{"pane":{}}}),
            ),
            (
                PluginCommandPaneType::Overlay,
                json!({"type":"plugin_pane_opened","plugin_pane":{"pane":{"pane_id":""}}}),
            ),
        ] {
            let error = plugin_command_pane_response(HerdrResponse::Success(result), pane_type)
                .expect_err("mutating success with missing placement evidence is uncertain");
            assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
        }
        assert_eq!(
            plugin_command_pane_response(
                HerdrResponse::Success(json!({"type":"ok"})),
                PluginCommandPaneType::Popup
            )
            .expect("known popup success"),
            PluginCommandPanePlacement::Popup
        );
        assert_eq!(
            plugin_command_pane_response(
                HerdrResponse::Success(
                    json!({"type":"plugin_pane_opened","plugin_pane":{"pane":{"pane_id":"w1:p2"}}})
                ),
                PluginCommandPaneType::Overlay
            )
            .expect("known overlay success"),
            PluginCommandPanePlacement::Pane(PaneId::new("w1:p2"))
        );
    }

    #[test]
    fn known_plugin_rejections_are_bounded_and_never_advise_manual_registration() {
        for code in [
            "plugin_not_found",
            "plugin_disabled",
            "invalid_cwd",
            "plugin_pane_open_failed",
            "stale_context",
        ] {
            let error = plugin_command_pane_response(
                HerdrResponse::Error {
                    code: code.to_owned(),
                    message: "specific secret host cause".to_owned(),
                },
                PluginCommandPaneType::Popup,
            )
            .expect_err("known rejection is not success or an unknown outcome");
            assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
            assert!(!error.message.contains("secret"));
        }
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn non_utf8_plugin_cwd_fails_before_any_host_request() {
        use std::os::unix::ffi::OsStringExt;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct NoRequestAuthority(AtomicUsize);
        #[async_trait::async_trait]
        impl HerdrRequestAuthority for NoRequestAuthority {
            async fn request(&self, _: &str, _: Value) -> Result<HerdrResponse, AdapterError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(invalid("unexpected host request"))
            }
        }
        let authority = NoRequestAuthority(AtomicUsize::new(0));
        let temp = tempfile::TempDir::new().unwrap();
        let cache = crate::HerdrCache::new(temp.path());
        for pane_type in [PluginCommandPaneType::Overlay, PluginCommandPaneType::Popup] {
            let error = open_plugin_command_pane_with(
                &authority,
                &cache,
                PluginCommandPaneLaunch {
                    origin: FocusedPane {
                        workspace: WorkspaceId::new("w1"),
                        tab: TabId::new("w1:t1"),
                        pane: PaneId::new("w1:p1"),
                        cwd: "/captured".into(),
                        columns: 80,
                        rows: 24,
                    },
                    cwd: PathBuf::from(std::ffi::OsString::from_vec(b"/invalid/\xff".to_vec())),
                    argv: vec!["/bin/echo".to_owned()],
                    pane_type,
                    width: None,
                    height: None,
                },
            )
            .await
            .expect_err("non-UTF-8 cwd is a diagnostic, never a JSON panic");
            assert_eq!(error.kind, AdapterErrorKind::InvalidRequest);
            assert_eq!(
                authority.0.load(Ordering::SeqCst),
                0,
                "invalid encoding must fail before snapshot or mutating requests"
            );
        }
    }
}
