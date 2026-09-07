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
///
/// Capture boundary: the workspace/tab/pane identifier tuples are validated strictly against
/// the live snapshot, while working directories are *enriched* from the live pane, never used
/// as an equality gate. DESIGN 1194/1991 mandates validating origin identifiers and enriching
/// the snapshot, not exact-string cwd comparison. The live pane cwd wins because the saved
/// hint is explicitly untrusted, the shell may `cd` between launcher environment capture and
/// `AttachUi`, and Herdr canonicalizes paths server-side (observed on 0.8.2: a pane created
/// with cwd `/tmp` reports `/private/tmp`). The hint cwd is still required to be absolute when
/// present and fills the gap only when the live pane reports no absolute cwd. Nothing here
/// substitutes the UI caller pane or current focus: both tuples must resolve to live panes.
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
        .map_err(|error| socket_error(&error))?
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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn validate_tuple_values(
    snapshot: &Map<String, Value>,
    workspace_id: &WorkspaceId,
    tab_id: &TabId,
    pane_id: &PaneId,
    hint_cwd: Option<&PathBuf>,
    description: &str,
) -> Result<ValidatedPane, AdapterError> {
    // The hint cwd keeps its absolute-path typing obligation, but its value is advisory:
    // the live pane is authoritative (see `capture_origin` boundary docs).
    if let Some(cwd) = hint_cwd
        && !cwd.is_absolute()
    {
        return Err(context_error(format!(
            "{description} working directory is not an absolute path"
        )));
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
    // Live pane cwd wins; the saved hint only fills the gap when the live pane reports no
    // absolute cwd. A textual mismatch is legitimate (launcher-to-attach `cd`, server-side
    // canonicalization such as /tmp becoming /private/tmp), never a capture failure.
    let live_cwd = pane
        .get("cwd")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|cwd| cwd.is_absolute());
    let cwd = live_cwd.or_else(|| hint_cwd.cloned());

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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn required_string<'a>(value: &'a Map<String, Value>, name: &str) -> Result<&'a str, AdapterError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| context_error(format!("Herdr response has no string {name} field")))
}

fn string_field_is(value: &Map<String, Value>, name: &str, expected: &str) -> bool {
    value.get(name).and_then(Value::as_str) == Some(expected)
}

fn socket_error(error: &SocketError) -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::Unavailable,
        format!("Herdr session.snapshot transport failure: {error}"),
    )
}

fn context_error(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::ContextUnavailable, message)
}

#[cfg(test)]
mod tests {
    use muxe_adapter_api::{OriginCaptureRequest, OriginHintSource, UiSessionId};
    use muxe_core::{PaneId, ServerId, TabId, WorkspaceId};
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    use super::*;

    /// Live pane reports the server-canonicalized cwd (/private/tmp); the launcher hint
    /// carries the textual alias (/tmp). Real Herdr canonicalizes exactly this way
    /// (workspace.create with cwd /tmp reports /private/tmp over the socket).
    const LIVE_CWD: &str = "/private/tmp";
    const HINT_ALIAS_CWD: &str = "/tmp";

    fn snapshot_body() -> Value {
        json!({
            "workspaces": [{"workspace_id": "w1"}],
            "tabs": [{"tab_id": "w1:t1", "workspace_id": "w1", "number": 1}],
            "panes": [{
                "pane_id": "w1:p1",
                "tab_id": "w1:t1",
                "workspace_id": "w1",
                "cwd": LIVE_CWD,
            }],
        })
    }

    fn serve_snapshot(path: &std::path::Path, body: Value) -> tokio::task::JoinHandle<()> {
        let listener = UnixListener::bind(path).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut request = Vec::new();
            reader.read_until(b'\n', &mut request).await.unwrap();
            let id = serde_json::from_slice::<Value>(&request).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned();
            let response = json!({
                "id": id,
                "result": {"type": "session_snapshot", "snapshot": body},
            });
            reader
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        })
    }

    fn request_with_hint_cwd(hint_cwd: Option<&str>) -> OriginCaptureRequest {
        OriginCaptureRequest {
            ui_session: UiSessionId::new("ui-1"),
            ui_pane: PaneId::new("w1:p1"),
            origin_hint: Some(UntrustedOriginHint {
                workspace_id: WorkspaceId::new("w1"),
                tab_id: TabId::new("w1:t1"),
                pane_id: PaneId::new("w1:p1"),
                cwd: hint_cwd.map(PathBuf::from),
                source: OriginHintSource::LauncherBootstrap,
            }),
            caller_identity: Some(HostCallerIdentity {
                workspace_id: WorkspaceId::new("w1"),
                tab_id: TabId::new("w1:t1"),
                pane_id: PaneId::new("w1:p1"),
                cwd: hint_cwd.map(PathBuf::from),
            }),
        }
    }

    #[tokio::test]
    async fn accepts_alias_cwd_hint_and_enriches_from_live_pane() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let server = serve_snapshot(&path, snapshot_body());
        let client = HerdrSocketClient::new(&path);
        let origin = capture_origin(
            &client,
            &request_with_hint_cwd(Some(HINT_ALIAS_CWD)),
            ServerId::new("test-server"),
        )
        .await
        .expect("aliased hint cwd must not reject capture");
        server.await.unwrap();
        assert_eq!(origin.pane_cwd, Some(PathBuf::from(LIVE_CWD)));
        assert_eq!(origin.pane_id, Some(PaneId::new("w1:p1")));
    }

    #[tokio::test]
    async fn accepts_changed_cwd_hint_and_prefers_live_pane_cwd() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let server = serve_snapshot(&path, snapshot_body());
        let client = HerdrSocketClient::new(&path);
        let origin = capture_origin(
            &client,
            &request_with_hint_cwd(Some("/somewhere/else")),
            ServerId::new("test-server"),
        )
        .await
        .expect("a cwd that changed between launcher capture and attach must not reject capture");
        server.await.unwrap();
        assert_eq!(origin.pane_cwd, Some(PathBuf::from(LIVE_CWD)));
    }

    #[tokio::test]
    async fn falls_back_to_hint_cwd_when_live_pane_reports_none() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let mut body = snapshot_body();
        body["panes"][0].as_object_mut().unwrap().remove("cwd");
        let server = serve_snapshot(&path, body);
        let client = HerdrSocketClient::new(&path);
        let origin = capture_origin(
            &client,
            &request_with_hint_cwd(Some(HINT_ALIAS_CWD)),
            ServerId::new("test-server"),
        )
        .await
        .expect("missing live cwd must fall back to the absolute hint cwd");
        server.await.unwrap();
        assert_eq!(origin.pane_cwd, Some(PathBuf::from(HINT_ALIAS_CWD)));
    }

    #[tokio::test]
    async fn rejects_relative_hint_cwd() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let server = serve_snapshot(&path, snapshot_body());
        let client = HerdrSocketClient::new(&path);
        let error = capture_origin(
            &client,
            &request_with_hint_cwd(Some("relative/path")),
            ServerId::new("test-server"),
        )
        .await
        .expect_err("relative hint cwd must still be rejected");
        server.await.unwrap();
        assert_eq!(error.kind, AdapterErrorKind::ContextUnavailable);
    }

    #[tokio::test]
    async fn rejects_unknown_pane_identifiers() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let server = serve_snapshot(&path, snapshot_body());
        let client = HerdrSocketClient::new(&path);
        let mut request = request_with_hint_cwd(None);
        request.origin_hint.as_mut().unwrap().pane_id = PaneId::new("w1:gone");
        request.caller_identity.as_mut().unwrap().pane_id = PaneId::new("w1:gone");
        request.ui_pane = PaneId::new("w1:gone");
        let error = capture_origin(&client, &request, ServerId::new("test-server"))
            .await
            .expect_err("unknown pane ids must still be rejected");
        server.await.unwrap();
        assert_eq!(error.kind, AdapterErrorKind::ContextUnavailable);
    }
}
