use std::path::PathBuf;

use muxe_adapter_api::{
    AdapterError, AdapterErrorKind, HostCallerIdentity, OriginCaptureRequest, OriginHintSource,
    UntrustedOriginHint,
};
use muxe_core::{
    OriginContext, OriginHostKind, OriginInvocationSource, PaneId, ServerId, TabId, WorkspaceId,
};
use serde_json::{Map, Value};

use crate::{HerdrResponse, HerdrSocketClient, SocketError, generated::method_metadata};

/// Validates the launcher-captured origin and the UI's caller identity against a fresh Herdr
/// snapshot. Neither tuple is inferred from focus or from this process environment.
pub async fn capture_origin(
    client: &HerdrSocketClient,
    request: &OriginCaptureRequest,
    server_id: ServerId,
) -> Result<OriginContext, AdapterError> {
    let origin_hint = request.origin_hint.as_ref().ok_or_else(|| {
        context_error("Herdr AttachUi did not provide the saved origin bootstrap tuple")
    })?;
    let caller_identity = request.caller_identity.as_ref().ok_or_else(|| {
        context_error("Herdr AttachUi did not provide the UI caller identity tuple")
    })?;
    if caller_identity.pane_id != request.ui_pane {
        return Err(context_error(
            "Herdr caller pane does not match the pane that is attaching the UI",
        ));
    }

    let snapshot_method = method_metadata("session.snapshot").ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "bundled Herdr metadata has no session.snapshot method",
        )
    })?;
    let snapshot_result = match client
        .unary(snapshot_method, Value::Object(Map::new()))
        .await
        .map_err(socket_error)?
    {
        HerdrResponse::Success(result) => result,
        HerdrResponse::Error { code, message } => {
            return Err(context_error(format!(
                "Herdr rejected session.snapshot with {code}: {message}"
            )));
        }
    };
    let snapshot = snapshot_from_result(&snapshot_result)?;

    let saved_origin = validate_tuple(snapshot, origin_hint, "saved origin")?;
    validate_caller(snapshot, caller_identity, "UI caller")?;

    Ok(OriginContext {
        host_kind: OriginHostKind::Herdr,
        server_id,
        client_id: None,
        session_id: None,
        workspace_id: Some(origin_hint.workspace_id.clone()),
        tab_id: Some(origin_hint.tab_id.clone()),
        tab_index: saved_origin.tab_index,
        pane_id: Some(origin_hint.pane_id.clone()),
        pane_type: None,
        pane_cwd: saved_origin.cwd,
        selection_text: None,
        invocation_source: match origin_hint.source {
            OriginHintSource::LauncherBootstrap => OriginInvocationSource::RootBinding,
            OriginHintSource::DirectInvocation => OriginInvocationSource::CommandLine,
        },
        worktree_id: None,
        worktree_path: saved_origin.worktree_path,
        agent_id: None,
        link_url: None,
        link_handler_id: None,
    })
}

struct ValidatedPane {
    tab_index: Option<u64>,
    cwd: Option<PathBuf>,
    worktree_path: Option<PathBuf>,
}

fn snapshot_from_result(result: &Value) -> Result<&Map<String, Value>, AdapterError> {
    let result = result
        .as_object()
        .ok_or_else(|| context_error("Herdr session.snapshot response result is not an object"))?;
    if required_string(result, "type")? != "session_snapshot" {
        return Err(context_error(
            "Herdr session.snapshot response has an unexpected result type",
        ));
    }
    result
        .get("snapshot")
        .and_then(Value::as_object)
        .ok_or_else(|| context_error("Herdr session.snapshot response has no snapshot object"))
}

fn validate_caller(
    snapshot: &Map<String, Value>,
    caller: &HostCallerIdentity,
    description: &str,
) -> Result<(), AdapterError> {
    validate_tuple_values(
        snapshot,
        &caller.workspace_id,
        &caller.tab_id,
        &caller.pane_id,
        caller.cwd.as_ref(),
        description,
    )
    .map(|_| ())
}

fn validate_tuple(
    snapshot: &Map<String, Value>,
    hint: &UntrustedOriginHint,
    description: &str,
) -> Result<ValidatedPane, AdapterError> {
    validate_tuple_values(
        snapshot,
        &hint.workspace_id,
        &hint.tab_id,
        &hint.pane_id,
        hint.cwd.as_ref(),
        description,
    )
}

fn validate_tuple_values(
    snapshot: &Map<String, Value>,
    workspace_id: &WorkspaceId,
    tab_id: &TabId,
    pane_id: &PaneId,
    expected_cwd: Option<&PathBuf>,
    description: &str,
) -> Result<ValidatedPane, AdapterError> {
    if let Some(cwd) = expected_cwd {
        if !cwd.is_absolute() {
            return Err(context_error(format!(
                "{description} working directory is not an absolute path"
            )));
        }
    }
    let workspaces = required_array(snapshot, "workspaces")?;
    let workspace = workspaces
        .iter()
        .filter_map(Value::as_object)
        .find(|workspace| string_field_is(workspace, "workspace_id", workspace_id.as_str()))
        .ok_or_else(|| context_error(format!("{description} workspace is not live")))?;
    let tabs = required_array(snapshot, "tabs")?;
    let tab = tabs
        .iter()
        .filter_map(Value::as_object)
        .find(|tab| {
            string_field_is(tab, "tab_id", tab_id.as_str())
                && string_field_is(tab, "workspace_id", workspace_id.as_str())
        })
        .ok_or_else(|| context_error(format!("{description} tab is not live in its workspace")))?;
    let panes = required_array(snapshot, "panes")?;
    let pane = panes
        .iter()
        .filter_map(Value::as_object)
        .find(|pane| {
            string_field_is(pane, "pane_id", pane_id.as_str())
                && string_field_is(pane, "tab_id", tab_id.as_str())
                && string_field_is(pane, "workspace_id", workspace_id.as_str())
        })
        .ok_or_else(|| context_error(format!("{description} pane is not live in its tab")))?;
    let cwd = pane
        .get("cwd")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|cwd| cwd.is_absolute());
    if expected_cwd.is_some() && cwd.as_ref() != expected_cwd {
        return Err(context_error(format!(
            "{description} working directory does not match the live pane"
        )));
    }

    let tab_index = tab.get("number").and_then(Value::as_u64);
    let worktree_path = workspace
        .get("worktree")
        .and_then(Value::as_object)
        .and_then(|worktree| worktree.get("checkout_path"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute());
    Ok(ValidatedPane {
        tab_index,
        cwd,
        worktree_path,
    })
}

fn required_array<'a>(
    snapshot: &'a Map<String, Value>,
    name: &str,
) -> Result<&'a [Value], AdapterError> {
    snapshot
        .get(name)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| context_error(format!("Herdr session.snapshot has no {name} array")))
}

fn required_string<'a>(
    value: &'a Map<String, Value>,
    name: &str,
) -> Result<&'a str, AdapterError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| context_error(format!("Herdr response has no string {name} field")))
}

fn string_field_is(value: &Map<String, Value>, name: &str, expected: &str) -> bool {
    value.get(name).and_then(Value::as_str) == Some(expected)
}

fn socket_error(error: SocketError) -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::Unavailable,
        format!("Herdr session.snapshot transport failure: {error}"),
    )
}

fn context_error(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::ContextUnavailable, message)
}
