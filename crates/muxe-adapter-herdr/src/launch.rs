use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use muxe_adapter_api::{AdapterError, AdapterErrorKind};
use muxe_core::{PaneId, TabId, WorkspaceId};
use serde_json::{Map, Value, json};

use crate::{ApiSchema, HerdrResponse, HerdrSocketClient, generated::method_metadata};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UiSplitDirection {
    Right,
    Down,
}

impl UiSplitDirection {
    fn wire(self) -> &'static str {
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

/// Reads one schema-validated snapshot and captures the focused workspace/tab/pane/cwd tuple.
/// A direct launcher never substitutes a broker process cwd, home directory, or root directory
/// when Herdr has no absolute current-pane directory.
///
/// # Errors
///
/// Returns `AdapterError` when the snapshot cannot be read or has no focused
/// pane with an absolute cwd and live geometry.
pub async fn focused_pane(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
) -> Result<FocusedPane, AdapterError> {
    let snapshot = session_snapshot(client, schema).await?;
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
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    workspace: WorkspaceId,
    tab: TabId,
    pane: PaneId,
) -> Result<FocusedPane, AdapterError> {
    let snapshot = session_snapshot(client, schema).await?;
    pane_from_snapshot(&snapshot, workspace, tab, pane)
}

/// Resolves an explicit launcher parent-pane ID through one fresh host inventory snapshot.
///
/// # Errors
///
/// Returns `AdapterError` when the pane ID is empty, the snapshot cannot be
/// read, or the pane is not live.
pub async fn pane_by_id(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    pane_id: &str,
) -> Result<FocusedPane, AdapterError> {
    if pane_id.is_empty() {
        return Err(invalid("Herdr parent pane ID must not be empty"));
    }
    let snapshot = session_snapshot(client, schema).await?;
    let pane = snapshot
        .get("panes")
        .and_then(Value::as_array)
        .and_then(|panes| {
            panes
                .iter()
                .filter_map(Value::as_object)
                .find(|candidate| candidate.get("pane_id").and_then(Value::as_str) == Some(pane_id))
        })
        .ok_or_else(|| invalid("Herdr explicit parent pane is not present in the live snapshot"))?;
    let workspace = WorkspaceId::new(required_id(pane, "workspace_id", "session.snapshot")?);
    let tab = TabId::new(required_id(pane, "tab_id", "session.snapshot")?);
    pane_from_snapshot(&snapshot, workspace, tab, PaneId::new(pane_id))
}
async fn session_snapshot(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
) -> Result<Map<String, Value>, AdapterError> {
    let result = result_object(
        &invoke(
            client,
            schema,
            "session.snapshot",
            Value::Object(Map::new()),
        )
        .await?,
        "session.snapshot",
    )?;
    if result.get("type").and_then(Value::as_str) != Some("session_snapshot") {
        return Err(invalid(
            "Herdr session.snapshot result has an unexpected type",
        ));
    }
    result
        .get("snapshot")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| invalid("Herdr session.snapshot result lacks snapshot"))
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
    let cwd = snapshot
        .get("panes")
        .and_then(Value::as_array)
        .and_then(|panes| {
            panes
                .iter()
                .filter_map(Value::as_object)
                .find_map(|candidate| {
                    (candidate.get("workspace_id").and_then(Value::as_str)
                        == Some(workspace.as_str())
                        && candidate.get("tab_id").and_then(Value::as_str) == Some(tab.as_str())
                        && candidate.get("pane_id").and_then(Value::as_str) == Some(pane.as_str()))
                    .then(|| candidate.get("cwd").and_then(Value::as_str))
                    .flatten()
                })
        })
        .map(PathBuf::from)
        .filter(|cwd| cwd.is_absolute())
        .ok_or_else(|| invalid("Herdr pane tuple has no absolute captured working directory"))?;
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
        .ok_or_else(|| invalid("Herdr pane tuple has no positive live layout geometry"))?;
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
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    launch: CommandPaneLaunch,
) -> Result<CommandPanePlacement, AdapterError> {
    if !launch.cwd.is_absolute() {
        return Err(invalid("Herdr command-pane cwd must be absolute"));
    }
    if launch.argv.first().is_none_or(String::is_empty) {
        return Err(invalid(
            "Herdr command-pane argv requires a nonempty program",
        ));
    }
    if !(launch.ratio.is_finite() && 0.0 < launch.ratio && launch.ratio < 1.0) {
        return Err(invalid(
            "Herdr command-pane split ratio must be strictly between zero and one",
        ));
    }
    let layout = invoke(
        client,
        schema,
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
                "Herdr layout.apply result lacks root object",
            )
        })?;
    let prepared = PreparedUiPane {
        temporary_tab: TabId::new(required_id(&layout, "tab_id", "layout.apply")?),
        ui_pane: PaneId::new(required_id(root, "pane_id", "layout.apply root")?),
    };
    let moved = invoke(
        client,
        schema,
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
                close_transient_tab(client, schema, &prepared.temporary_tab).await
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::DispatchFailed,
                    format!(
                        "Herdr command-pane move failed ({move_error}); closing its returned temporary tab also failed ({cleanup_error})"
                    ),
                ));
            }
            Err(move_error)
        }
    }
}

/// Executes only trampoline step 2. The returned pane is still in the temporary tab and must be
/// registered with the pending-launch gate before it is moved.
///
/// # Errors
///
/// Returns `AdapterError` when the launch is invalid or the layout request fails.
pub async fn prepare_ui_pane(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    launch: &UiPaneLaunch,
) -> Result<PreparedUiPane, AdapterError> {
    if !launch.cwd.is_absolute() {
        return Err(invalid("Herdr UI launch cwd must be absolute"));
    }
    validate_ui_argv(&launch.argv)?;
    validate_bootstrap_env(&launch.bootstrap_env)?;
    if !(launch.ratio.is_finite() && 0.0 < launch.ratio && launch.ratio < 1.0) {
        return Err(invalid(
            "Herdr UI launch split ratio must be strictly between zero and one",
        ));
    }

    let layout = invoke(
        client,
        schema,
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
                "Herdr layout.apply result lacks root object",
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
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    launch: &UiPaneLaunch,
    prepared: PreparedUiPane,
) -> Result<UiPanePlacement, AdapterError> {
    let moved = invoke(
        client,
        schema,
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
                close_transient_tab(client, schema, &prepared.temporary_tab).await
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::DispatchFailed,
                    format!(
                        "Herdr UI pane move failed ({move_error}); closing its returned temporary tab also failed ({cleanup_error})"
                    ),
                ));
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
    let executable_is_muxe = argv.first().is_some_and(|value| {
        if value == "muxe" {
            true
        } else {
            let path = Path::new(value);
            path.is_absolute() && path.file_name().is_some_and(|name| name == "muxe")
        }
    });
    if !executable_is_muxe
        || argv.get(1).map(String::as_str) != Some("ui")
        || argv.get(2).map(String::as_str) != Some("menu")
    {
        return Err(invalid(
            "Herdr UI launch command must begin with `muxe ui menu`",
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
            return Err(invalid(format!("Herdr UI launch lacks nonempty {key}")));
        }
    }
    if env
        .get(OPTIONAL)
        .is_some_and(|cwd| cwd.is_empty() || !PathBuf::from(cwd).is_absolute())
    {
        return Err(invalid(
            "Herdr UI launch origin cwd must be an absolute path when present",
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
pub async fn close_transient_tab(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    tab: &TabId,
) -> Result<(), AdapterError> {
    invoke(
        client,
        schema,
        "tab.close",
        json!({ "tab_id": tab.as_str() }),
    )
    .await
    .map(|_| ())
}

async fn invoke(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    method: &str,
    params: Value,
) -> Result<Value, AdapterError> {
    let metadata = method_metadata(method)
        .ok_or_else(|| incompatible(format!("bundled Herdr metadata does not declare {method}")))?;
    schema
        .validate_method(metadata.method, &params)
        .map_err(|error| incompatible(format!("active Herdr schema rejects {method}: {error}")))?;
    match client
        .unary(metadata, params)
        .await
        .map_err(|error| socket_error(&error))?
    {
        HerdrResponse::Success(result) => Ok(result),
        HerdrResponse::Error { code, message } => Err(AdapterError::new(
            AdapterErrorKind::DispatchFailed,
            format!("Herdr rejected {method} with {code}: {message}"),
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
            "Herdr layout.apply result has unexpected type",
        ));
    }
    result
        .get("layout")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Herdr layout.apply result lacks layout object",
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
                format!("Herdr {method} result lacks nonempty {field}"),
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
            format!("Herdr {method} result has unexpected type"),
        ));
    }
    let move_result = result
        .get("move_result")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} result lacks move_result object"),
            )
        })?;
    (move_result.get("changed").and_then(Value::as_bool) == Some(true))
        .then_some(())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} move_result did not report changed:true"),
            )
        })
}

fn socket_error(error: &crate::SocketError) -> AdapterError {
    AdapterError::new(
        if error.delivery() == crate::DeliveryState::MayHaveReachedHost {
            AdapterErrorKind::OutcomeUnknown
        } else {
            AdapterErrorKind::Unavailable
        },
        error.to_string(),
    )
}

fn incompatible(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Incompatible, message)
}

fn invalid(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
