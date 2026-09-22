mod support {
    pub mod production_connect;
    pub mod recorded_socket;
}

use std::{
    fs,
    future::{Future, poll_fn},
    os::unix::fs::symlink,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use muxe_adapter_api::{
    AdapterError, AdapterErrorKind, AdapterHealthEvent, DispatchCompletion, HostAdapter,
    HostCallerIdentity, NativeCompatibilityOutcome, OriginCaptureRequest, OriginHintSource,
    PendingPaneRegistration, PortableDispatchRequest, PostDismissalPortableDispatchRequest,
    ResolvedPortableAction, UiSessionId, UntrustedOriginHint,
};
use muxe_adapter_herdr::{
    CommandPaneLaunch, CommandTabLaunch, FocusedPane, HerdrAdapter, HerdrResponse, HerdrRuntime,
    PreparedUiPane, UiPaneLaunch, UiSplitDirection, focused_pane, move_prepared_ui_pane,
    open_command_pane, open_command_tab,
};
use muxe_core::{
    ActionScalar, ActionValidator, ConfigValue, ConfigValueKind, CreateCommand, ExecutionId,
    NativeActionCandidate, PaneAction, PaneId, PortableAction, SourceId, SourceSpan, TabAction,
    TabId, WorkspaceId,
};
use serde_json::json;
use support::{
    production_connect::ProductionConnectFixture,
    recorded_socket::{RecordedExchange, RecordedResponse, ResponseBarrier},
};

#[tokio::test]
async fn connects_the_production_adapter_through_schema_ping_probe_and_retained_subscription() {
    let fixture = ProductionConnectFixture::start().expect("owned fake-native fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connect accepts the recorded child and socket handshake");
    let actual = HostAdapter::identity(adapter.as_ref())
        .await
        .expect("retained subscription keeps the adapter healthy");
    assert_eq!(actual.kind, muxe_adapter_api::HostKind::Herdr);
    assert_eq!(
        actual.discovery_key.as_str(),
        fixture.socket().display().to_string()
    );
    assert!(!actual.live_server_id.as_str().is_empty());
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![json!("ping"), json!("events.subscribe")],
        "production connect must use its schema child, ping, then retained subscription"
    );
    drop(adapter);
    drop(fixture);
}

fn lifecycle_snapshot() -> serde_json::Value {
    json!({
        "workspaces": [{ "workspace_id": "workspace-1" }],
        "tabs": [{ "tab_id": "tab-1", "workspace_id": "workspace-1", "number": 0 }],
        "panes": [
            {
                "pane_id": "pane-1",
                "tab_id": "tab-1",
                "workspace_id": "workspace-1",
                "cwd": "/saved/origin",
            },
            {
                "pane_id": "pane-2",
                "tab_id": "tab-1",
                "workspace_id": "workspace-1",
                "cwd": "/ui/caller",
            },
        ],
        "layouts": [{
            "workspace_id": "workspace-1",
            "tab_id": "tab-1",
            "panes": [{
                "pane_id": "pane-1",
                "rect": { "width": 80, "height": 24 },
            }],
        }],
    })
}

fn lifecycle_capture_request() -> OriginCaptureRequest {
    OriginCaptureRequest {
        ui_session: UiSessionId::new("ui-lifecycle"),
        ui_pane: PaneId::new("pane-2"),
        origin_hint: Some(UntrustedOriginHint {
            workspace_id: WorkspaceId::new("workspace-1"),
            tab_id: TabId::new("tab-1"),
            pane_id: PaneId::new("pane-1"),
            cwd: Some("/saved/origin".into()),
            source: OriginHintSource::LauncherBootstrap,
        }),
        caller_identity: Some(HostCallerIdentity {
            workspace_id: WorkspaceId::new("workspace-1"),
            tab_id: TabId::new("tab-1"),
            pane_id: PaneId::new("pane-2"),
            cwd: Some("/ui/caller".into()),
        }),
    }
}

fn lifecycle_scalar(value: &str) -> ActionScalar {
    ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
        value.to_owned(),
    )))
}

async fn wait_for_lifecycle_requests(
    fixture: &ProductionConnectFixture,
    minimum: usize,
    stage: &str,
) {
    if tokio::time::timeout(Duration::from_secs(1), fixture.wait_for_requests(minimum))
        .await
        .is_err()
    {
        let methods = fixture
            .requests()
            .await
            .into_iter()
            .filter_map(|request| {
                request
                    .get("method")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();
        panic!("timed out waiting for {stage}; received {methods:?}");
    }
}

fn pending_pane_registration() -> PendingPaneRegistration {
    PendingPaneRegistration {
        ui_session: UiSessionId::new("pending-ui"),
        pane: PaneId::new("pane-a"),
        temporary_tab: Some(TabId::new("temporary-tab")),
    }
}

fn pending_pane_info() -> serde_json::Value {
    json!({
        "type": "pane_info",
        "pane": {
            "pane_id": "pane-a",
            "tab_id": "temporary-tab",
            "workspace_id": "workspace-1",
        },
    })
}

fn pending_pane_get() -> RecordedExchange {
    RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Result(pending_pane_info()),
    }
}

#[tokio::test]
async fn pending_cleanup_closes_only_the_registered_pane_in_a_shared_temporary_tab() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(pending_pane_get());
    script.push(pending_pane_get());
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Result(json!({ "type": "pane_closed", "pane_id": "pane-a" })),
    });
    script.push(ProductionConnectFixture::ping_exchange());
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "pane-b" }),
        response: RecordedResponse::Result(json!({
            "type": "pane_info",
            "pane": {
                "pane_id": "pane-b",
                "tab_id": "temporary-tab",
                "workspace_id": "workspace-1",
            },
        })),
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned pending cleanup fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects to the owned fake endpoint");
    let registration = pending_pane_registration();
    let lease = adapter
        .register_pending_pane(registration.clone())
        .await
        .expect("the fake host records pane A in the temporary tab also containing pane B");

    adapter
        .close_pending_pane(registration, lease)
        .await
        .expect("cleanup closes the registered pane");

    let runtime = HerdrRuntime::connect(fixture.adapter_config())
        .await
        .expect("a second guarded runtime connects to the same recorded incarnation");
    let response = runtime
        .invoke_response("pane.get", json!({ "pane_id": "pane-b" }))
        .await
        .expect("the unrelated pane remains queryable after cleanup");
    assert!(matches!(
        &response,
        HerdrResponse::Success(pane)
            if pane["pane"]["pane_id"] == json!("pane-b")
                && pane["pane"]["tab_id"] == json!("temporary-tab")
    ));

    let requests = fixture.requests().await;
    assert!(
        requests.iter().all(|request| {
            request.get("method").and_then(serde_json::Value::as_str) != Some("tab.close")
        }),
        "cleanup must not close pane B by closing its shared temporary tab"
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| {
                request.get("method").and_then(serde_json::Value::as_str) == Some("pane.close")
            })
            .map(|request| request["params"].clone())
            .collect::<Vec<_>>(),
        vec![json!({ "pane_id": "pane-a" })],
        "cleanup sends one close for its registered pane only"
    );
    adapter
        .shutdown()
        .await
        .expect("owned adapter monitor stops");
}

#[tokio::test]
async fn pending_cleanup_keeps_the_lease_retryable_when_close_schema_rejects_dispatch() {
    let mut runtime_schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/herdr/herdr-api.schema.json"
    ))
    .expect("bundled schema JSON");
    let close_request = runtime_schema["schemas"]["request"]["oneOf"]
        .as_array_mut()
        .expect("bundled schema declares request branches")
        .iter_mut()
        .find(|branch| branch.pointer("/properties/method/const") == Some(&json!("pane.close")))
        .expect("bundled schema declares pane.close");
    close_request["properties"]["params"] = json!({ "type": "string" });

    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(pending_pane_get());
    script.push(pending_pane_get());
    script.push(pending_pane_get());
    let fixture = ProductionConnectFixture::start_scripted_with_schema(script, &runtime_schema)
        .expect("owned pending cleanup fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects to the owned fake endpoint");
    let registration = pending_pane_registration();
    let lease = adapter
        .register_pending_pane(registration.clone())
        .await
        .expect("the pending pane registers");

    let first = adapter
        .close_pending_pane(registration.clone(), lease.clone())
        .await
        .expect_err("the incompatible runtime schema prevents pane.close dispatch");
    let retry = adapter
        .close_pending_pane(registration, lease)
        .await
        .expect_err("a pre-dispatch schema failure keeps the cleanup retryable");
    assert_eq!(first.kind, AdapterErrorKind::Incompatible);
    assert_eq!(
        retry, first,
        "both attempts report the same pre-dispatch incompatibility, not an unknown outcome"
    );
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .filter_map(|request| request["method"].as_str().map(str::to_owned))
            .collect::<Vec<_>>(),
        vec![
            "ping",
            "events.subscribe",
            "pane.get",
            "pane.get",
            "pane.get",
        ],
        "schema rejection sends no pane.close bytes"
    );
    adapter
        .shutdown()
        .await
        .expect("owned adapter monitor stops");
}

#[tokio::test]
async fn pending_cleanup_converges_after_lost_close_response_and_typed_absence() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(pending_pane_get());
    script.push(pending_pane_get());
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Close,
    });
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Error {
            code: "pane_not_found".to_owned(),
            message: "pane-a has already closed".to_owned(),
        },
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned pending cleanup fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects to the owned fake endpoint");
    let registration = pending_pane_registration();
    let lease = adapter
        .register_pending_pane(registration.clone())
        .await
        .expect("the pending pane registers");

    let error = adapter
        .close_pending_pane(registration.clone(), lease.clone())
        .await
        .expect_err("a lost close response has an unknown outcome");
    assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
    adapter
        .close_pending_pane(registration.clone(), lease.clone())
        .await
        .expect("typed pane absence proves the previous close converged");
    let error = adapter
        .close_pending_pane(registration, lease)
        .await
        .expect_err("converged cleanup consumes the lease exactly once");
    assert_eq!(error.kind, AdapterErrorKind::ContextUnavailable);
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .filter_map(|request| request["method"].as_str().map(str::to_owned))
            .collect::<Vec<_>>(),
        vec![
            "ping",
            "events.subscribe",
            "pane.get",
            "pane.get",
            "pane.close",
            "pane.get",
        ],
        "the recovery observes typed absence and never sends a second close"
    );
    adapter
        .shutdown()
        .await
        .expect("owned adapter monitor stops");
}

#[tokio::test]
async fn pending_cleanup_keeps_a_reused_pane_id_unknown_after_a_lost_close_response() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(pending_pane_get());
    script.push(pending_pane_get());
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Close,
    });
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Result(json!({
            "type": "pane_info",
            "pane": {
                "pane_id": "pane-a",
                "tab_id": "reused-tab",
                "workspace_id": "workspace-1",
            },
        })),
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned pending cleanup fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects to the owned fake endpoint");
    let registration = pending_pane_registration();
    let lease = adapter
        .register_pending_pane(registration.clone())
        .await
        .expect("the pending pane registers");

    let error = adapter
        .close_pending_pane(registration.clone(), lease.clone())
        .await
        .expect_err("a lost close response has an unknown outcome");
    assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
    let error = adapter
        .close_pending_pane(registration, lease)
        .await
        .expect_err("a pane ID observed after the lost response remains unknown");
    assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .filter_map(|request| request["method"].as_str().map(str::to_owned))
            .filter(|method| method == "pane.close")
            .count(),
        1,
        "cleanup does not close a pane ID observed after an unknown close outcome"
    );
    adapter
        .shutdown()
        .await
        .expect("owned adapter monitor stops");
}

#[tokio::test]
async fn pending_cleanup_rejects_a_rebound_endpoint_before_probing_typed_absence() {
    let mut first_host_script = ProductionConnectFixture::initial_handshake();
    first_host_script.push(pending_pane_get());
    let first_host = ProductionConnectFixture::start_scripted(first_host_script)
        .expect("first owned pending cleanup endpoint starts");
    let second_host = ProductionConnectFixture::start_scripted(vec![RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Error {
            code: "pane_not_found".to_owned(),
            message: "a different host reports no pane-a".to_owned(),
        },
    }])
    .expect("replacement owned pending cleanup endpoint starts");
    let endpoint_dir = tempfile::tempdir().expect("owned endpoint alias directory");
    let endpoint = endpoint_dir.path().join("herdr.sock");
    symlink(first_host.socket(), &endpoint).expect("alias initially selects the first host");
    let adapter = HerdrAdapter::connect(first_host.adapter_config_at(endpoint.clone()))
        .await
        .expect("production adapter connects through the owned endpoint alias");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let registration = pending_pane_registration();
    let lease = adapter
        .register_pending_pane(registration.clone())
        .await
        .expect("the pending pane registers against the first host");

    fs::remove_file(&endpoint).expect("replace the owned endpoint alias");
    symlink(second_host.socket(), &endpoint).expect("alias now selects the replacement host");
    let error = adapter
        .close_pending_pane(registration, lease.clone())
        .await
        .expect_err("a replaced endpoint must not report its pane absence as convergence");
    assert_eq!(error.kind, AdapterErrorKind::Unavailable);
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    adapter.release_pending_pane(lease.clone());
    adapter.release_pending_pane(lease);
    assert!(
        second_host.requests().await.is_empty(),
        "the endpoint check must fail before the replacement host receives pane.get or pane.close"
    );
    adapter
        .shutdown()
        .await
        .expect("owned adapter monitor stops");
}

#[tokio::test]
async fn admitted_request_rejects_replacement_before_write_and_reports_one_lease_loss() {
    let first_host =
        ProductionConnectFixture::start().expect("first owned request endpoint starts");
    let second_host = ProductionConnectFixture::start_scripted(vec![pending_pane_get()])
        .expect("replacement owned request endpoint starts");
    let endpoint_dir = tempfile::tempdir().expect("owned endpoint alias directory");
    let endpoint = endpoint_dir.path().join("herdr.sock");
    symlink(first_host.socket(), &endpoint).expect("alias initially selects the first host");
    let adapter = HerdrAdapter::connect(first_host.adapter_config_at(endpoint.clone()))
        .await
        .expect("production adapter connects through the owned endpoint alias");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));

    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_request_connect_wait_hook(Some(Arc::clone(&hook)));
    let request_waiting = hook.entered.notified();
    tokio::pin!(request_waiting);
    let request = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.modal_scope(&PaneId::new("pane-a")).await })
    };
    tokio::time::timeout(Duration::from_secs(2), &mut request_waiting)
        .await
        .expect("request reaches its exact pre-connect guard");
    adapter.set_request_connect_wait_hook(None);
    let queued_pane = PaneId::new("pane-b");
    let mut queued = Box::pin(adapter.modal_scope(&queued_pane));
    poll_fn(|context| match queued.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("queued old-epoch request completed while the first was blocked"),
    })
    .await;
    drop(queued);
    fs::remove_file(&endpoint).expect("replace the owned endpoint alias");
    symlink(second_host.socket(), &endpoint).expect("alias now selects the replacement host");
    hook.release.notify_one();

    let error = request
        .await
        .expect("guarded request task joins")
        .expect_err("replacement rejects the admitted request");
    assert_eq!(error.kind, AdapterErrorKind::Unavailable);
    assert!(
        second_host.requests().await.is_empty(),
        "the replacement receives no request byte and no replay"
    );
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    adapter.shutdown().await.expect("lost adapter shuts down");
}

#[tokio::test]
async fn pending_cleanup_preserves_its_lease_after_an_unrelated_rpc_failure() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(pending_pane_get());
    script.push(pending_pane_get());
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Error {
            code: "permission_denied".to_owned(),
            message: "pending cleanup was rejected".to_owned(),
        },
    });
    script.push(pending_pane_get());
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-a" }),
        response: RecordedResponse::Result(json!({ "type": "pane_closed", "pane_id": "pane-a" })),
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned pending cleanup fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects to the owned fake endpoint");
    let registration = pending_pane_registration();
    let lease = adapter
        .register_pending_pane(registration.clone())
        .await
        .expect("the pending pane registers");

    let error = adapter
        .close_pending_pane(registration.clone(), lease.clone())
        .await
        .expect_err("an unrelated host rejection is not idempotent absence");
    assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
    adapter
        .close_pending_pane(registration, lease)
        .await
        .expect("the retained lease permits a later confirmed close");
    adapter
        .shutdown()
        .await
        .expect("owned adapter monitor stops");
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .filter_map(|request| request["method"].as_str().map(str::to_owned))
            .filter(|method| method == "pane.close")
            .count(),
        2,
        "the explicit rejection preserves the lease for one later close attempt"
    );
}

#[tokio::test]
async fn defers_focused_tab_creation_until_the_retained_ui_close_event() {
    let snapshot = lifecycle_snapshot();
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(RecordedExchange {
        method: "tab.create",
        params: json!({
            "workspace_id": "workspace-1",
            "label": "logs",
            "focus": true,
            "cwd": null,
        }),
        response: RecordedResponse::Result(json!({
            "type": "tab_created",
            "tab_id": "logs-tab",
            "workspace_id": "workspace-1",
        })),
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned lifecycle fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));

    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures from the owned snapshot");
    let execution = ExecutionId(71);
    adapter
        .dispatch_portable_after_ui_dismissal(PostDismissalPortableDispatchRequest {
            execution,
            action: ResolvedPortableAction {
                action: PortableAction::Tab(TabAction::Create {
                    workspace_id: Some(lifecycle_scalar("workspace-1")),
                    name: Some(lifecycle_scalar("logs")),
                    focus: None,
                    command: CreateCommand::default(),
                }),
            },
            origin,
            ui_pane: PaneId::new("pane-2"),
        })
        .await
        .expect("focused creation is armed while the UI pane is still live");

    wait_for_lifecycle_requests(&fixture, 4, "the UI-live snapshot").await;
    let before_close = fixture.requests().await;
    assert!(
        before_close.iter().all(|request| {
            !matches!(
                request.get("method").and_then(serde_json::Value::as_str),
                Some("tab.create" | "layout.apply")
            )
        }),
        "no creation RPC may run while the UI pane remains in the snapshot"
    );

    fixture
        .send_retained_event(json!({ "type": "pane_closed", "pane_id": "pane-2" }))
        .expect("retained subscription accepts the UI close event");
    wait_for_lifecycle_requests(&fixture, 5, "tab.create after pane_closed").await;
    let requests = fixture.requests().await;
    let creation = requests
        .last()
        .expect("creation request follows pane_closed");
    assert_eq!(creation["method"], json!("tab.create"));
    assert_eq!(
        creation["params"],
        json!({
            "workspace_id": "workspace-1",
            "label": "logs",
            "focus": true,
            "cwd": null,
        })
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_health_event())
            .await
            .expect("creation completion arrives")
            .expect("adapter reports completion"),
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded {
            execution: completed,
        }) if completed == execution
    ));
    adapter
        .shutdown()
        .await
        .expect("adapter stops its retained monitor");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn deferred_preflight_failure_emits_one_retained_terminal() {
    let snapshot = lifecycle_snapshot();
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned preflight fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));

    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures from the owned snapshot");
    let execution = ExecutionId(74);
    adapter
        .dispatch_portable_after_ui_dismissal(PostDismissalPortableDispatchRequest {
            execution,
            action: ResolvedPortableAction {
                action: PortableAction::Pane(PaneAction::Split {
                    direction: None,
                    focus: None,
                    command: CreateCommand::default(),
                }),
            },
            origin,
            ui_pane: PaneId::new("pane-2"),
        })
        .await
        .expect("invalid deferred action is admitted while its UI remains live");
    wait_for_lifecycle_requests(&fixture, 4, "the UI-live snapshot").await;

    fixture
        .send_retained_event(json!({ "type": "pane_closed", "pane_id": "pane-2" }))
        .expect("retained subscription accepts the UI close event");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_health_event())
            .await
            .expect("preflight failure arrives")
            .expect("adapter reports preflight completion"),
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Failed {
            execution: completed,
            error,
        }) if completed == execution
            && error.kind == AdapterErrorKind::Incompatible
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), adapter.next_health_event())
            .await
            .is_err(),
        "one preflight failure produces exactly one retained terminal"
    );
    adapter
        .shutdown()
        .await
        .expect("adapter stops its retained monitor");
    drop(adapter);
    drop(fixture);
}
#[tokio::test]
async fn reports_unknown_outcome_when_command_tab_layout_closes_after_flush() {
    let snapshot = lifecycle_snapshot();
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(RecordedExchange {
        method: "layout.apply",
        params: json!({
            "focus": true,
            "workspace_id": "workspace-1",
            "tab_label": "logs",
            "root": {
                "type": "pane",
                "command": ["tool", "--literal"],
                "cwd": "/command",
                "env": {},
            },
        }),
        response: RecordedResponse::Close,
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned outcome fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));
    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures from the owned snapshot");
    let execution = ExecutionId(72);
    adapter
        .dispatch_portable_after_ui_dismissal(PostDismissalPortableDispatchRequest {
            execution,
            action: ResolvedPortableAction {
                action: PortableAction::Tab(TabAction::Create {
                    workspace_id: Some(lifecycle_scalar("workspace-1")),
                    name: Some(lifecycle_scalar("logs")),
                    focus: None,
                    command: CreateCommand {
                        program: Some(lifecycle_scalar("tool")),
                        args: vec![lifecycle_scalar("--literal")],
                        cwd: Some(lifecycle_scalar("/command")),
                    },
                }),
            },
            origin,
            ui_pane: PaneId::new("pane-2"),
        })
        .await
        .expect("focused command creation is armed while the UI pane is live");

    wait_for_lifecycle_requests(&fixture, 4, "the UI-live snapshot").await;
    fixture
        .send_retained_event(json!({ "type": "pane_closed", "pane_id": "pane-2" }))
        .expect("retained subscription accepts the UI close event");
    wait_for_lifecycle_requests(&fixture, 5, "layout.apply after pane_closed").await;
    assert_eq!(
        fixture
            .requests()
            .await
            .last()
            .and_then(|request| request.get("method"))
            .cloned(),
        Some(json!("layout.apply")),
        "the layout request reached the host before its response socket closed"
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_health_event())
            .await
            .expect("unknown completion arrives")
            .expect("adapter reports completion"),
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
            execution: completed,
            ..
        }) if completed == execution
    ));
    adapter
        .shutdown()
        .await
        .expect("adapter stops its retained monitor");
    drop(adapter);
    drop(fixture);
}

#[expect(
    clippy::too_many_lines,
    reason = "one linear fault script: layout.apply, post-flush pane.move close, then rejected cleanup; the exchange order is the assertion"
)]
#[tokio::test]
async fn reports_unknown_outcome_when_command_split_move_closes_and_cleanup_fails() {
    let snapshot = lifecycle_snapshot();
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(ProductionConnectFixture::snapshot_exchange(&snapshot));
    script.push(RecordedExchange {
        method: "layout.apply",
        params: json!({
            "focus": false,
            "workspace_id": "workspace-1",
            "root": {
                "type": "pane",
                "command": ["tool"],
                "cwd": "/command",
                "env": {},
            },
        }),
        response: RecordedResponse::Result(json!({
            "type": "layout_apply",
            "layout": {
                "workspace_id": "workspace-1",
                "tab_id": "temporary-tab",
                "zoomed": false,
                "focused_pane_id": "new-pane",
                "root": { "type": "pane", "pane_id": "new-pane" },
            },
        })),
    });
    script.push(RecordedExchange {
        method: "pane.move",
        params: json!({
            "pane_id": "new-pane",
            "focus": true,
            "destination": {
                "type": "tab",
                "tab_id": "tab-1",
                "target_pane_id": "pane-1",
                "split": "right",
                "ratio": 0.5,
            },
        }),
        response: RecordedResponse::Close,
    });
    script.push(RecordedExchange {
        method: "tab.close",
        params: json!({ "tab_id": "temporary-tab" }),
        response: RecordedResponse::Error {
            code: "cleanup-failed".to_owned(),
            message: "temporary tab remains".to_owned(),
        },
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned outcome fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));
    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures from the owned snapshot");
    let execution = ExecutionId(73);
    adapter
        .dispatch_portable_after_ui_dismissal(PostDismissalPortableDispatchRequest {
            execution,
            action: ResolvedPortableAction {
                action: PortableAction::Pane(PaneAction::Split {
                    direction: Some(lifecycle_scalar("right")),
                    focus: None,
                    command: CreateCommand {
                        program: Some(lifecycle_scalar("tool")),
                        args: Vec::new(),
                        cwd: Some(lifecycle_scalar("/command")),
                    },
                }),
            },
            origin,
            ui_pane: PaneId::new("pane-2"),
        })
        .await
        .expect("focused command split is armed while the UI pane is live");

    wait_for_lifecycle_requests(&fixture, 4, "the UI-live snapshot").await;
    fixture
        .send_retained_event(json!({ "type": "pane_closed", "pane_id": "pane-2" }))
        .expect("retained subscription accepts the UI close event");
    wait_for_lifecycle_requests(&fixture, 8, "split cleanup after the lost move response").await;
    assert_eq!(
        fixture
            .requests()
            .await
            .last()
            .and_then(|request| request.get("method"))
            .cloned(),
        Some(json!("tab.close")),
        "the failed cleanup follows the post-flush pane.move request"
    );
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_health_event())
            .await
            .expect("unknown completion arrives")
            .expect("adapter reports completion"),
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
            execution: completed,
            ..
        }) if completed == execution
    ));
    adapter
        .shutdown()
        .await
        .expect("adapter stops its retained monitor");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn runtime_connect_reuses_then_heals_the_cached_normalized_request_representation() {
    let script = vec![
        ProductionConnectFixture::ping_exchange(),
        ProductionConnectFixture::ping_exchange(),
        ProductionConnectFixture::ping_exchange(),
    ];
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned runtime-cache fixture starts");
    let cache = tempfile::tempdir().expect("owned shared cache");
    let mut config = fixture.adapter_config();
    config.cache_dir = cache.path().join("cache");

    let first = HerdrRuntime::connect(config.clone())
        .await
        .expect("first production runtime connect succeeds");
    assert!(!first.used_cached_schema_representation());
    let second = HerdrRuntime::connect(config.clone())
        .await
        .expect("second production runtime connect succeeds");
    assert!(
        second.used_cached_schema_representation(),
        "the second production runtime must parse the cache-verified normalized request"
    );

    let entry = std::fs::read_dir(config.cache_dir.join("herdr"))
        .expect("runtime cache directory")
        .map(Result::unwrap)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("normalized-"))
        })
        .expect("normalized representation entry");
    let mut corrupted: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&entry).expect("read normalized entry"))
            .expect("cache entry JSON");
    corrupted["normalized_request"] = json!({"oneOf": []});
    std::fs::write(&entry, corrupted.to_string()).expect("corrupt only stored representation");

    let healed = HerdrRuntime::connect(config)
        .await
        .expect("production runtime must heal a corrupt normalized representation");
    assert!(
        !healed.used_cached_schema_representation(),
        "a hash-mismatched stored representation is a miss, never trusted"
    );
    drop(fixture);
}

#[tokio::test]
async fn runtime_schema_drift_misses_the_cached_normalized_request_representation() {
    let runtime_schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/herdr/herdr-api.schema.json"
    ))
    .expect("bundled schema JSON");
    let first_fixture = ProductionConnectFixture::start_scripted_with_schema(
        vec![ProductionConnectFixture::ping_exchange()],
        &runtime_schema,
    )
    .expect("first owned runtime-schema fixture starts");
    let cache = tempfile::tempdir().expect("owned shared cache");
    let mut first_config = first_fixture.adapter_config();
    first_config.cache_dir = cache.path().join("cache");
    let first = HerdrRuntime::connect(first_config.clone())
        .await
        .expect("first production runtime connect succeeds");
    assert!(!first.used_cached_schema_representation());
    drop(first);
    drop(first_fixture);

    let mut drifted_schema = runtime_schema;
    drifted_schema["schema_version"] = json!(2);
    let drifted_fixture = ProductionConnectFixture::start_scripted_with_schema(
        vec![ProductionConnectFixture::ping_exchange()],
        &drifted_schema,
    )
    .expect("drifted owned runtime-schema fixture starts");
    let mut drifted_config = drifted_fixture.adapter_config();
    drifted_config.cache_dir = cache.path().join("cache");
    let drifted = HerdrRuntime::connect(drifted_config)
        .await
        .expect("production runtime accepts compatible schema drift");
    assert!(
        !drifted.used_cached_schema_representation(),
        "a runtime-schema hash change must miss the normalized request representation cache"
    );
    drop(drifted);
    drop(drifted_fixture);
}

#[tokio::test]
async fn production_native_batch_cache_keeps_mixed_outcomes_in_current_candidate_order() {
    let fixture = ProductionConnectFixture::start().expect("owned production batch fixture starts");
    let mut config = fixture.adapter_config();
    let cache = tempfile::tempdir().expect("owned comparison cache");
    config.cache_dir = cache.path().join("cache");
    let adapter = HerdrAdapter::connect(config)
        .await
        .expect("production adapter connect succeeds before native batch validation");
    let candidate = |type_name: &str, source: &str| NativeActionCandidate {
        type_name: type_name.to_owned(),
        type_span: SourceSpan::new(SourceId::new(source), 10, 20),
        fields: Vec::new(),
    };
    let first_valid = candidate("native.herdr.agent:list", "first-valid");
    let first_invalid = candidate("native.herdr.not-real:reject", "first-invalid");
    let first = [&first_valid, &first_invalid];
    let first_diagnostics = adapter
        .validate_native_batch(&first)
        .expect_err("the first effective native set is intentionally mixed");
    assert_eq!(first_diagnostics.len(), 1);
    assert_eq!(
        first_diagnostics[0].labels[0].span.source.as_str(),
        "first-invalid"
    );

    let reordered_invalid = candidate("native.herdr.not-real:reject", "reordered-invalid");
    let reordered_valid = candidate("native.herdr.agent:list", "reordered-valid");
    let reordered = [&reordered_invalid, &reordered_valid];
    let reordered_diagnostics = adapter
        .validate_native_batch(&reordered)
        .expect_err("the reordered effective native set remains intentionally mixed");
    assert_eq!(reordered_diagnostics.len(), 1);
    assert_eq!(
        reordered_diagnostics[0].labels[0].span.source.as_str(),
        "reordered-invalid",
        "reordered outcomes must not be borrowed from the positional first-set cache entry"
    );
    drop(adapter);
    drop(fixture);
}
#[tokio::test]
async fn production_structural_batch_reports_every_candidate_like_the_uncached_validator() {
    let fixture =
        ProductionConnectFixture::start().expect("owned production structural fixture starts");
    let mut config = fixture.adapter_config();
    let cache = tempfile::tempdir().expect("owned structural comparison cache");
    let schema_binary = config.herdr_binary.clone();
    config.cache_dir = cache.path().join("cache");
    let adapter = HerdrAdapter::connect(config)
        .await
        .expect("production adapter connect succeeds before structural batch validation");
    let field = |source: &str, name: &str| muxe_core::ConfigField {
        name: name.to_owned(),
        name_span: SourceSpan::new(SourceId::new(source), 0, 1),
        value: ConfigValue::string("w1:p3"),
    };
    let kebab = NativeActionCandidate {
        type_name: "native.herdr.pane:close".to_owned(),
        type_span: SourceSpan::new(SourceId::new("warm-kebab"), 10, 20),
        fields: vec![field("warm-kebab", "pane-id")],
    };
    let snake = NativeActionCandidate {
        type_name: "native.herdr.pane:close".to_owned(),
        type_span: SourceSpan::new(SourceId::new("mixed-structural"), 10, 20),
        fields: vec![field("mixed-structural", "pane_id")],
    };
    let unknown = NativeActionCandidate {
        type_name: "native.herdr.not-real:reject".to_owned(),
        type_span: SourceSpan::new(SourceId::new("mixed-unknown"), 30, 40),
        fields: Vec::new(),
    };
    // Warm the comparison cache with the kebab-spelled batch. The old
    // wire-normalizing hash erased the kebab/snake distinction, so this key
    // collides with the snake-spelled batch below; the warm call must report
    // only the unknown-method diagnostic while storing [ok, rejection].
    let warm = [&kebab, &unknown];
    let warm_diagnostics = adapter
        .validate_native_batch(&warm)
        .expect_err("the warm kebab batch reports only the unknown method");
    assert_eq!(
        warm_diagnostics.len(),
        1,
        "the warm batch stores one rejection for the unknown method"
    );
    assert_eq!(
        warm_diagnostics[0].labels[0].span.source.as_str(),
        "mixed-unknown",
        "the warm diagnostic pins the unknown candidate"
    );
    // Collide with the snake spelling of the same batch. Pre-fix this hits the
    // warm key and returns only the unknown-method diagnostic; post-fix the
    // structural pre-check rejects before any lookup and both are reported.
    let batch = [&snake, &unknown];
    let cached = adapter
        .validate_native_batch(&batch)
        .expect_err("a structurally mixed batch must report every candidate");
    assert_eq!(
        cached.len(),
        2,
        "the cached adapter path must not drop the semantically invalid candidate"
    );
    assert_eq!(
        cached[0].labels[0].span.source.as_str(),
        "mixed-structural",
        "structurally invalid diagnostics stay in input order"
    );
    assert_eq!(
        cached[1].labels[0].span.source.as_str(),
        "mixed-unknown",
        "semantically invalid diagnostics stay in input order"
    );
    let uncached = muxe_adapter_herdr::HerdrConfigValidator::load(&schema_binary, cache.path())
        .await
        .expect("config validator loads the installed schema")
        .validate_native_batch(&batch)
        .expect_err("the uncached validator reports the same mixed batch");
    assert_eq!(
        cached, uncached,
        "the structural batch must match the uncached path"
    );
    drop(adapter);
    drop(fixture);
}

async fn wait_until_suspended(adapter: &HerdrAdapter) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !adapter.suspended_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("suspend reaches its incarnation invalidation");
}

async fn wait_until_shutdown_started(adapter: &HerdrAdapter) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !adapter.shutdown_started_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("shutdown reaches its incarnation invalidation");
}
#[tokio::test]
async fn reconnects_the_production_adapter_only_after_retained_subscription_loss() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned fake-native reconnect fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial production adapter connection succeeds");
    fixture.wait_for_requests(2).await;
    assert!(matches!(
        HostAdapter::next_health_event(adapter.as_ref())
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));

    fixture.lose_retained_subscriptions();
    assert!(matches!(
        HostAdapter::next_health_event(adapter.as_ref())
            .await
            .expect("subscription loss is reported"),
        AdapterHealthEvent::Unhealthy {
            modal_scope: None,
            ..
        }
    ));
    assert!(matches!(
        HostAdapter::next_health_event(adapter.as_ref())
            .await
            .expect("reconnected event"),
        AdapterHealthEvent::Reconnected { .. }
    ));
    fixture.wait_for_requests(4).await;
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![
            json!("ping"),
            json!("events.subscribe"),
            json!("ping"),
            json!("events.subscribe"),
        ]
    );
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn reconnect_snapshot_tracks_schema_a_to_b_to_a_with_exact_cached_diagnostics() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned schema-drift reconnect fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial schema A connects");
    assert!(matches!(
        adapter.next_health_event().await.expect("initial health"),
        AdapterHealthEvent::Healthy { .. }
    ));
    let initial = adapter
        .native_compatibility_snapshot()
        .expect("initial compatibility snapshot");
    let candidate = |type_name: &str, source: &str| NativeActionCandidate {
        type_name: type_name.to_owned(),
        type_span: SourceSpan::new(SourceId::new(source), 10, 20),
        fields: Vec::new(),
    };
    let agents = candidate("native.herdr.agent:list", "agents");
    let workspaces = candidate("native.herdr.workspace:list", "workspaces");
    assert!(matches!(
        initial.validate_native(&agents),
        NativeCompatibilityOutcome::Compatible(_)
    ));
    assert!(matches!(
        initial.validate_native(&workspaces),
        NativeCompatibilityOutcome::Compatible(_)
    ));

    let schema_a: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/herdr/herdr-api.schema.json"
    ))
    .expect("bundled schema A");
    let mut schema_b = schema_a.clone();
    schema_b["schemas"]["request"]["oneOf"]
        .as_array_mut()
        .expect("request alternatives")
        .retain(|method| {
            method["properties"]["method"]["const"].as_str() != Some("workspace.list")
        });
    fixture
        .replace_schema(&schema_b)
        .expect("atomically install schema B");
    fixture.lose_retained_subscriptions();
    assert!(matches!(
        adapter.next_health_event().await.expect("schema A loss"),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    let AdapterHealthEvent::Reconnected {
        compatibility: schema_b_snapshot,
        ..
    } = adapter
        .next_health_event()
        .await
        .expect("schema B reconnect")
    else {
        panic!("expected schema B Reconnected event")
    };
    assert!(schema_b_snapshot.identity().continuity() > initial.identity().continuity());
    assert_ne!(
        schema_b_snapshot.identity().schema(),
        initial.identity().schema()
    );
    assert!(matches!(
        schema_b_snapshot.validate_native(&agents),
        NativeCompatibilityOutcome::Compatible(_)
    ));
    let first_blocked = match schema_b_snapshot.validate_native(&workspaces) {
        NativeCompatibilityOutcome::Blocked(diagnostics) => diagnostics,
        NativeCompatibilityOutcome::Compatible(_) => panic!("schema B removed workspace.list"),
    };
    let cached_blocked = match schema_b_snapshot.validate_native(&workspaces) {
        NativeCompatibilityOutcome::Blocked(diagnostics) => diagnostics,
        NativeCompatibilityOutcome::Compatible(_) => {
            panic!("cached schema B result cannot enable workspace.list")
        }
    };
    assert_eq!(
        cached_blocked, first_blocked,
        "cached negative reconstructs the exact source-aware diagnostic"
    );
    assert!(
        !first_blocked[0]
            .message
            .contains("cached Herdr compatibility rejection")
    );

    fixture
        .replace_schema(&schema_a)
        .expect("atomically restore schema A");
    fixture.lose_retained_subscriptions();
    assert!(matches!(
        adapter.next_health_event().await.expect("schema B loss"),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    let AdapterHealthEvent::Reconnected {
        compatibility: restored,
        ..
    } = adapter.next_health_event().await.expect("schema A restore")
    else {
        panic!("expected restored schema A Reconnected event")
    };
    assert!(restored.identity().continuity() > schema_b_snapshot.identity().continuity());
    assert_eq!(restored.identity().schema(), initial.identity().schema());
    assert!(matches!(
        restored.validate_native(&workspaces),
        NativeCompatibilityOutcome::Compatible(_)
    ));
    adapter.shutdown().await.expect("adapter shutdown");
}
#[tokio::test]
async fn reconnect_install_cannot_revive_a_completed_suspend_invalidation() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned reconnect-suspend fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_reconnect_install_wait_hook(Some(Arc::clone(&hook)));
    let install_waiting = hook.entered.notified();
    tokio::pin!(install_waiting);

    fixture.lose_retained_subscriptions();
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), &mut install_waiting)
        .await
        .expect("reconnect reaches the exact pre-install barrier");
    let suspending = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.suspend_for_activation().await })
    };
    wait_until_suspended(&adapter).await;
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), suspending)
        .await
        .expect("suspend joins the monitor handoff")
        .expect("suspend task joins")
        .expect("suspend succeeds");

    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    assert!(
        matches!(
            adapter.identity().await,
            Err(error) if error.kind == AdapterErrorKind::Unavailable
        ),
        "rejected reconnect leaves the suspended incarnation unhealthy"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
            .await
            .is_err(),
        "reconnect must not publish Reconnected after suspend"
    );
    adapter
        .shutdown()
        .await
        .expect("shutdown joins parked monitor");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn reconnect_install_cannot_revive_a_completed_shutdown_invalidation() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned reconnect-shutdown fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_reconnect_install_wait_hook(Some(Arc::clone(&hook)));
    let install_waiting = hook.entered.notified();
    tokio::pin!(install_waiting);

    fixture.lose_retained_subscriptions();
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), &mut install_waiting)
        .await
        .expect("reconnect reaches the exact pre-install barrier");
    let shutting_down = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.shutdown().await })
    };
    wait_until_shutdown_started(&adapter).await;
    hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), shutting_down)
        .await
        .expect("shutdown joins the blocked reconnect monitor")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");

    let drained = adapter.drain_health_queue_for_test().await;
    assert!(
        !drained
            .iter()
            .any(|event| matches!(event, AdapterHealthEvent::Reconnected { .. })),
        "rejected reconnect publishes no Reconnected event"
    );
    assert!(adapter.shutdown_started_for_test());
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn activation_resume_cannot_succeed_after_racing_shutdown() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned resume-shutdown fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    adapter
        .suspend_for_activation()
        .await
        .expect("activation suspend succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_resume_install_wait_hook(Some(Arc::clone(&hook)));
    let install_waiting = hook.entered.notified();
    tokio::pin!(install_waiting);
    let drain_hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_resume_drain_wait_hook(Some(Arc::clone(&drain_hook)));
    let drain_waiting = drain_hook.entered.notified();
    tokio::pin!(drain_waiting);
    let resuming = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.resume_after_activation_abort().await })
    };
    tokio::time::timeout(Duration::from_secs(2), &mut install_waiting)
        .await
        .expect("resume reaches the exact pre-install barrier");
    let shutting_down = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.shutdown().await })
    };
    tokio::time::timeout(Duration::from_secs(2), &mut drain_waiting)
        .await
        .expect("shutdown enables its resume-drained notification before checking the registry");
    wait_until_shutdown_started(&adapter).await;
    hook.release.notify_one();
    drain_hook.release.notify_one();
    let resume = tokio::time::timeout(Duration::from_secs(2), resuming)
        .await
        .expect("racing resume finishes")
        .expect("resume task joins");
    assert!(
        matches!(resume, Err(error) if error.kind == AdapterErrorKind::Shutdown),
        "shutdown wins the resume install race"
    );
    tokio::time::timeout(Duration::from_secs(2), shutting_down)
        .await
        .expect("shutdown waits for the resume candidate to be dropped")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");
    let drained = adapter.drain_health_queue_for_test().await;
    assert!(
        !drained
            .iter()
            .any(|event| matches!(event, AdapterHealthEvent::Reconnected { .. })),
        "failed resume publishes no Reconnected event"
    );
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn cancelled_suspend_is_joined_by_retry_and_does_not_release_the_next_generation() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned suspend-cancellation fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));

    let first_hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_suspend_release_wait_hook(Some(Arc::clone(&first_hook)));
    let first_release_waiting = first_hook.entered.notified();
    tokio::pin!(first_release_waiting);
    let first_suspend = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.suspend_for_activation().await })
    };
    tokio::time::timeout(Duration::from_secs(2), &mut first_release_waiting)
        .await
        .expect("monitor observes the exact first suspension generation");
    first_suspend.abort();
    first_suspend
        .await
        .expect_err("the first suspend caller is cancelled");

    let retry = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.suspend_for_activation().await })
    };
    first_hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), retry)
        .await
        .expect("retry joins the durable first attempt")
        .expect("retry task joins")
        .expect("retry observes exact stream release");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    adapter
        .resume_after_activation_abort()
        .await
        .expect("resume installs a fresh incarnation");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Reconnected { .. }
    ));

    let next_hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_suspend_release_wait_hook(Some(Arc::clone(&next_hook)));
    let next_release_waiting = next_hook.entered.notified();
    tokio::pin!(next_release_waiting);
    let next_suspend = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.suspend_for_activation().await })
    };
    tokio::time::timeout(Duration::from_secs(2), &mut next_release_waiting)
        .await
        .expect("a later suspension reaches its own exact release barrier");
    assert!(
        !next_suspend.is_finished(),
        "the later generation cannot consume the earlier release"
    );
    next_hook.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), next_suspend)
        .await
        .expect("later suspension completes")
        .expect("later suspend task joins")
        .expect("later exact stream is released");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    adapter.shutdown().await.expect("parked adapter shuts down");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn concurrent_suspend_callers_join_one_generation_and_publish_once() {
    let fixture =
        ProductionConnectFixture::start().expect("owned concurrent-suspend fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_suspend_release_wait_hook(Some(Arc::clone(&hook)));
    let release_waiting = hook.entered.notified();
    tokio::pin!(release_waiting);
    let first = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.suspend_for_activation().await })
    };
    tokio::time::timeout(Duration::from_secs(2), &mut release_waiting)
        .await
        .expect("monitor reaches the shared generation release barrier");
    let second = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.suspend_for_activation().await })
    };
    tokio::task::yield_now().await;
    hook.release.notify_one();
    for caller in [first, second] {
        tokio::time::timeout(Duration::from_secs(2), caller)
            .await
            .expect("concurrent suspend completes")
            .expect("concurrent suspend task joins")
            .expect("concurrent caller observes release");
    }
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), adapter.next_health_event())
            .await
            .is_err(),
        "one suspend generation publishes exactly one health event"
    );
    adapter.shutdown().await.expect("parked adapter shuts down");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn shutdown_cancels_resume_after_server_accepts_ping_without_replying() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(RecordedExchange {
        method: "ping",
        params: json!({}),
        response: RecordedResponse::Hang,
    });
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned hanging-resume fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    adapter
        .suspend_for_activation()
        .await
        .expect("activation suspend succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));

    let resuming = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.resume_after_activation_abort().await })
    };
    fixture.wait_for_requests(3).await;
    let shutting_down = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.shutdown().await })
    };
    wait_until_shutdown_started(&adapter).await;
    tokio::time::timeout(Duration::from_secs(2), shutting_down)
        .await
        .expect("shutdown cancels the accepted no-pong resume")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");
    let resume = tokio::time::timeout(Duration::from_secs(2), resuming)
        .await
        .expect("cancelled resume returns")
        .expect("resume task joins");
    assert!(matches!(
        resume,
        Err(error) if error.kind == AdapterErrorKind::Shutdown
    ));
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn explicit_shutdown_of_live_subscription_emits_no_host_loss() {
    let fixture = ProductionConnectFixture::start().expect("owned shutdown fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects before shutdown");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));

    adapter
        .shutdown()
        .await
        .expect("shutdown closes the retained subscription");
    fixture.lose_retained_subscriptions();
    assert!(
        matches!(
            adapter.next_health_event().await,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
        ),
        "intentional shutdown terminates health delivery without Unhealthy or HostLost"
    );
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn production_reconnect_emits_one_terminal_host_loss_after_bounded_grace() {
    let fixture =
        ProductionConnectFixture::start_scripted(ProductionConnectFixture::initial_handshake())
            .expect("owned fake-native terminal-loss fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial production adapter connection succeeds");
    assert!(matches!(
        HostAdapter::next_health_event(adapter.as_ref())
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));
    tokio::time::pause();

    fixture.lose_retained_subscriptions();
    assert!(matches!(
        HostAdapter::next_health_event(adapter.as_ref())
            .await
            .expect("subscription loss is reported"),
        AdapterHealthEvent::Unhealthy { .. }
    ));

    let waiting = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { HostAdapter::next_health_event(adapter.as_ref()).await })
    };
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    let terminal = tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("bounded host-loss reconnect emits a terminal event")
        .expect("terminal health task joins")
        .expect("terminal health event is delivered");
    assert!(matches!(terminal, AdapterHealthEvent::HostLost { .. }));

    let attempts_at_terminal = fixture.requests().await.len();
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        fixture.requests().await.len(),
        attempts_at_terminal,
        "the monitor must stop reconnecting after HostLost"
    );

    let duplicate = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { HostAdapter::next_health_event(adapter.as_ref()).await })
    };
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), duplicate)
            .await
            .is_err(),
        "host loss must be emitted exactly once"
    );
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn suspend_after_terminal_monitor_exit_returns_typed_host_loss() {
    let fixture =
        ProductionConnectFixture::start_scripted(ProductionConnectFixture::initial_handshake())
            .expect("owned terminal-suspend fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    tokio::time::pause();
    fixture.lose_retained_subscriptions();
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    let terminal = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.next_health_event().await })
    };
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(11)).await;
    assert!(matches!(
        terminal.await.unwrap().unwrap(),
        AdapterHealthEvent::HostLost { .. }
    ));

    let suspend = tokio::time::timeout(Duration::from_secs(1), adapter.suspend_for_activation())
        .await
        .expect("suspend observes the exited monitor outcome");
    assert!(matches!(
        suspend,
        Err(error) if error.kind == AdapterErrorKind::Unavailable
    ));
    adapter
        .shutdown()
        .await
        .expect("shutdown joins exited monitor");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn suspend_racing_full_queue_host_loss_preserves_event_and_completes() {
    let fixture =
        ProductionConnectFixture::start_scripted(ProductionConnectFixture::initial_handshake())
            .expect("owned full-queue suspend fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    assert_eq!(adapter.fill_health_queue_for_test(), 64);
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_host_lost_reserve_pending_hook(Some(Arc::clone(&hook)));
    let reserve_pending = hook.entered.notified();
    tokio::pin!(reserve_pending);
    tokio::time::pause();

    fixture.lose_retained_subscriptions();
    tokio::time::timeout(Duration::from_secs(2), async {
        while adapter.identity().await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription loss invalidates the incarnation");
    for _ in 0..11 {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    tokio::time::timeout(Duration::from_secs(1), &mut reserve_pending)
        .await
        .expect("HostLost reserve is genuinely pending on the full queue");

    let suspend = tokio::time::timeout(Duration::from_secs(1), adapter.suspend_for_activation())
        .await
        .expect("suspend receives terminal monitor outcome");
    assert!(matches!(
        suspend,
        Err(error) if error.kind == AdapterErrorKind::Unavailable
    ));
    for _ in 0..64 {
        assert!(
            matches!(
                adapter.next_health_event().await.unwrap(),
                AdapterHealthEvent::Unhealthy { .. }
            ),
            "bounded health events retain FIFO order ahead of HostLost"
        );
    }
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::HostLost { .. }
    ));
    adapter
        .shutdown()
        .await
        .expect("shutdown joins exited monitor");
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn shutdown_interrupts_terminal_host_loss_when_health_queue_is_full() {
    let fixture =
        ProductionConnectFixture::start_scripted(ProductionConnectFixture::initial_handshake())
            .expect("owned full-health-queue fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("initial adapter connection succeeds");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    assert_eq!(
        adapter.fill_health_queue_for_test(),
        64,
        "test fills the bounded health queue exactly"
    );
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_host_lost_reserve_pending_hook(Some(Arc::clone(&hook)));
    let host_lost_waiting = hook.entered.notified();
    tokio::pin!(host_lost_waiting);
    tokio::time::pause();

    fixture.lose_retained_subscriptions();
    tokio::time::timeout(Duration::from_secs(2), async {
        while adapter.identity().await.is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription loss invalidates the incarnation");
    for _ in 0..11 {
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    tokio::time::timeout(Duration::from_secs(1), &mut host_lost_waiting)
        .await
        .expect("bounded grace reaches terminal HostLost publication");

    let shutting_down = {
        let adapter = Arc::clone(&adapter);
        tokio::spawn(async move { adapter.shutdown().await })
    };
    wait_until_shutdown_started(&adapter).await;
    tokio::time::timeout(Duration::from_secs(2), shutting_down)
        .await
        .expect("shutdown interrupts the full-queue HostLost send")
        .expect("shutdown task joins")
        .expect("shutdown joins the monitor without deadlock");
    let drained = adapter.drain_health_queue_for_test().await;
    assert!(
        !drained
            .iter()
            .any(|event| matches!(event, AdapterHealthEvent::HostLost { .. })),
        "shutdown cancellation publishes no stale HostLost"
    );
    drop(adapter);
    drop(fixture);
}

fn command_launch_exchanges() -> Vec<RecordedExchange> {
    vec![
        RecordedExchange {
            method: "session.snapshot",
            params: json!({}),
            response: RecordedResponse::Result(json!({
                "type": "session_snapshot",
                "snapshot": {
                    "focused_workspace_id": "workspace-1",
                    "focused_tab_id": "tab-1",
                    "focused_pane_id": "pane-1",
                    "panes": [{
                        "workspace_id": "workspace-1",
                        "tab_id": "tab-1",
                        "pane_id": "pane-1",
                        "cwd": "/captured/origin",
                    }],
                    "layouts": [{
                        "workspace_id": "workspace-1",
                        "tab_id": "tab-1",
                        "panes": [{
                            "pane_id": "pane-1",
                            "rect": { "width": 80, "height": 24 },
                        }],
                    }],
                },
            })),
        },
        RecordedExchange {
            method: "layout.apply",
            params: json!({
                "focus": false,
                "workspace_id": "workspace-1",
                "root": {
                    "type": "pane",
                    "command": ["tool", "--literal"],
                    "cwd": "/captured/origin",
                    "env": {},
                },
            }),
            response: RecordedResponse::Result(json!({
                "type": "layout_apply",
                "layout": {
                    "workspace_id": "workspace-1",
                    "tab_id": "temporary-tab",
                    "zoomed": false,
                    "focused_pane_id": "new-pane",
                    "root": { "type": "pane", "pane_id": "new-pane" },
                },
            })),
        },
        RecordedExchange {
            method: "pane.move",
            params: json!({
                "pane_id": "new-pane",
                "focus": true,
                "destination": {
                    "type": "tab",
                    "tab_id": "tab-1",
                    "target_pane_id": "pane-1",
                    "split": "down",
                    "ratio": 0.5,
                },
            }),
            response: RecordedResponse::Result(json!({
                "type": "pane_move",
                "move_result": { "changed": true },
            })),
        },
    ]
}

#[tokio::test]
async fn launches_an_exact_command_from_the_live_focused_origin() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.extend(command_launch_exchanges());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned command-pane fixture starts");
    let runtime = HerdrRuntime::connect(fixture.adapter_config())
        .await
        .expect("guarded runtime connects");

    let origin = focused_pane(&runtime)
        .await
        .expect("focused origin is captured from the live snapshot");
    assert_eq!(
        origin,
        FocusedPane {
            workspace: muxe_core::WorkspaceId::new("workspace-1"),
            tab: muxe_core::TabId::new("tab-1"),
            pane: muxe_core::PaneId::new("pane-1"),
            cwd: "/captured/origin".into(),
            columns: 80,
            rows: 24,
        }
    );
    let placement = open_command_pane(
        &runtime,
        CommandPaneLaunch {
            destination: origin.clone(),
            cwd: "/captured/origin".into(),
            origin,
            argv: vec!["tool".to_owned(), "--literal".to_owned()],
            direction: UiSplitDirection::Down,
            ratio: 0.5,
            focus: true,
        },
    )
    .await
    .expect("captured command launches");
    assert_eq!(placement.pane.as_str(), "new-pane");
    assert_eq!(fixture.requests().await.len(), 4);
    drop(runtime);
    drop(fixture);
}
#[tokio::test]
async fn command_pane_transaction_excludes_an_already_accepted_unrelated_unary() {
    let barrier = Arc::new(ResponseBarrier::new());
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.push(RecordedExchange {
        method: "layout.apply",
        params: json!({
            "focus": false,
            "workspace_id": "workspace-1",
            "root": {
                "type": "pane",
                "command": ["tool", "--literal"],
                "cwd": "/captured/origin",
                "env": {},
            },
        }),
        response: RecordedResponse::Barrier {
            barrier: Arc::clone(&barrier),
            result: json!({
                "type": "layout_apply",
                "layout": {
                    "workspace_id": "workspace-1",
                    "tab_id": "temporary-tab",
                    "zoomed": false,
                    "focused_pane_id": "new-pane",
                    "root": { "type": "pane", "pane_id": "new-pane" },
                },
            }),
        },
    });
    script.push(RecordedExchange {
        method: "pane.move",
        params: json!({
            "pane_id": "new-pane",
            "focus": true,
            "destination": {
                "type": "tab",
                "tab_id": "tab-1",
                "target_pane_id": "pane-1",
                "split": "down",
                "ratio": 0.5,
            },
        }),
        response: RecordedResponse::Result(json!({
            "type": "pane_move",
            "move_result": { "changed": true },
        })),
    });
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "unrelated" }),
        response: RecordedResponse::Result(json!({
            "type": "pane_info",
            "pane": {
                "pane_id": "unrelated",
                "tab_id": "tab-1",
                "workspace_id": "workspace-1",
            },
        })),
    });
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned composite-ordering fixture starts");
    let runtime = Arc::new(
        HerdrRuntime::connect(fixture.adapter_config())
            .await
            .expect("guarded runtime connects"),
    );
    let origin = FocusedPane {
        workspace: WorkspaceId::new("workspace-1"),
        tab: TabId::new("tab-1"),
        pane: PaneId::new("pane-1"),
        cwd: "/captured/origin".into(),
        columns: 80,
        rows: 24,
    };
    let launch = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            open_command_pane(
                runtime.as_ref(),
                CommandPaneLaunch {
                    destination: origin.clone(),
                    cwd: "/captured/origin".into(),
                    origin,
                    argv: vec!["tool".to_owned(), "--literal".to_owned()],
                    direction: UiSplitDirection::Down,
                    ratio: 0.5,
                    focus: true,
                },
            )
            .await
        })
    };
    barrier.wait_until_blocked().await;

    let unrelated = runtime.invoke_response("pane.get", json!({ "pane_id": "unrelated" }));
    tokio::pin!(unrelated);
    poll_fn(|context| match unrelated.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("unrelated unary completed while the composite was blocked"),
    })
    .await;
    barrier.release();

    launch
        .await
        .expect("command-pane task joins")
        .expect("exclusive command-pane transaction completes");
    unrelated
        .await
        .expect("accepted unrelated unary completes after the transaction");
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![
            json!("ping"),
            json!("layout.apply"),
            json!("pane.move"),
            json!("pane.get"),
        ],
    );
}

#[tokio::test]
async fn prepared_move_cleanup_failure_excludes_an_accepted_unrelated_unary() {
    let barrier = Arc::new(ResponseBarrier::new());
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.push(RecordedExchange {
        method: "pane.move",
        params: json!({
            "pane_id": "ui-pane",
            "focus": true,
            "destination": {
                "type": "tab",
                "tab_id": "origin-tab",
                "target_pane_id": "origin-pane",
                "split": "down",
                "ratio": 0.5,
            },
        }),
        response: RecordedResponse::BarrierError {
            barrier: Arc::clone(&barrier),
            code: "move_failed".to_owned(),
            message: "move rejected".to_owned(),
        },
    });
    script.push(RecordedExchange {
        method: "tab.close",
        params: json!({ "tab_id": "temporary-tab" }),
        response: RecordedResponse::Error {
            code: "cleanup_failed".to_owned(),
            message: "temporary tab remains".to_owned(),
        },
    });
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "unrelated" }),
        response: RecordedResponse::Result(json!({
            "type": "pane_info",
            "pane": {
                "pane_id": "unrelated",
                "tab_id": "origin-tab",
                "workspace_id": "workspace-1",
            },
        })),
    });
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned prepared-move rollback fixture starts");
    let runtime = Arc::new(
        HerdrRuntime::connect(fixture.adapter_config())
            .await
            .expect("guarded runtime connects"),
    );
    let moving = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            move_prepared_ui_pane(
                runtime.as_ref(),
                &UiPaneLaunch {
                    origin_workspace: WorkspaceId::new("workspace-1"),
                    origin_tab: TabId::new("origin-tab"),
                    origin_pane: PaneId::new("origin-pane"),
                    cwd: "/origin".into(),
                    argv: Vec::new(),
                    bootstrap_env: std::collections::BTreeMap::new(),
                    direction: UiSplitDirection::Down,
                    ratio: 0.5,
                    focus: true,
                },
                PreparedUiPane {
                    temporary_tab: TabId::new("temporary-tab"),
                    ui_pane: PaneId::new("ui-pane"),
                },
            )
            .await
        })
    };
    barrier.wait_until_blocked().await;

    let unrelated = runtime.invoke_response("pane.get", json!({ "pane_id": "unrelated" }));
    tokio::pin!(unrelated);
    poll_fn(|context| match unrelated.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("unrelated unary completed while prepared move was blocked"),
    })
    .await;
    barrier.release();

    let error = moving
        .await
        .expect("prepared move task joins")
        .expect_err("move and rollback both fail");
    assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
    assert!(error.message.contains("move rejected"));
    assert!(error.message.contains("temporary tab remains"));
    unrelated
        .await
        .expect("unrelated unary runs after rollback finishes");
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![
            json!("ping"),
            json!("pane.move"),
            json!("tab.close"),
            json!("pane.get"),
        ],
    );
}

#[tokio::test]
async fn accepted_unary_survives_caller_cancellation_in_fifo_order() {
    let barrier = Arc::new(ResponseBarrier::new());
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "first" }),
        response: RecordedResponse::Barrier {
            barrier: Arc::clone(&barrier),
            result: json!({
                "type": "pane_info",
                "pane": {
                    "pane_id": "first",
                    "tab_id": "tab-1",
                    "workspace_id": "workspace-1",
                },
            }),
        },
    });
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "cancelled-caller" }),
        response: RecordedResponse::Result(json!({
            "type": "pane_info",
            "pane": {
                "pane_id": "cancelled-caller",
                "tab_id": "tab-1",
                "workspace_id": "workspace-1",
            },
        })),
    });
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned cancellation-ordering fixture starts");
    let runtime = Arc::new(
        HerdrRuntime::connect(fixture.adapter_config())
            .await
            .expect("guarded runtime connects"),
    );
    let first = {
        let runtime = Arc::clone(&runtime);
        tokio::spawn(async move {
            runtime
                .invoke_response("pane.get", json!({ "pane_id": "first" }))
                .await
        })
    };
    barrier.wait_until_blocked().await;

    let mut cancelled =
        Box::pin(runtime.invoke_response("pane.get", json!({ "pane_id": "cancelled-caller" })));
    poll_fn(|context| match cancelled.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("second unary completed while the first response was blocked"),
    })
    .await;
    drop(cancelled);
    barrier.release();

    first
        .await
        .expect("first unary task joins")
        .expect("first unary completes");
    wait_for_lifecycle_requests(&fixture, 3, "cancelled caller's accepted unary").await;
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["params"]["pane_id"].clone())
            .collect::<Vec<_>>(),
        vec![json!(null), json!("first"), json!("cancelled-caller")],
        "caller cancellation after queue insertion cannot retract accepted work",
    );
}

#[tokio::test]
async fn schema_valid_long_wait_has_no_local_response_deadline() {
    let barrier = Arc::new(ResponseBarrier::new());
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.push(RecordedExchange {
        method: "agent.wait",
        params: json!({ "target": "agent-1", "timeout_ms": 300_000 }),
        response: RecordedResponse::Barrier {
            barrier: Arc::clone(&barrier),
            result: json!({ "type": "agent_wait", "status": "completed" }),
        },
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned long-wait fixture");
    let runtime = Arc::new(
        HerdrRuntime::connect(fixture.adapter_config())
            .await
            .expect("guarded runtime connects"),
    );
    let waiting = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move {
            runtime
                .invoke_response(
                    "agent.wait",
                    json!({ "target": "agent-1", "timeout_ms": 300_000 }),
                )
                .await
        }
    });
    barrier.wait_until_blocked().await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(301)).await;
    tokio::task::yield_now().await;
    assert!(
        !waiting.is_finished(),
        "a schema-valid long wait must remain owned until Herdr replies or the runtime retires"
    );

    barrier.release();
    assert!(matches!(
        waiting.await.expect("long-wait task joins").unwrap(),
        HerdrResponse::Success(_)
    ));
}

#[tokio::test]
async fn explicit_response_timeout_remains_authoritative() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.push(RecordedExchange {
        method: "pane.get",
        params: json!({ "pane_id": "hung" }),
        response: RecordedResponse::Hang,
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned explicit-timeout fixture");
    let runtime = HerdrRuntime::connect(fixture.adapter_config())
        .await
        .expect("guarded runtime connects");
    tokio::time::pause();

    let error = runtime
        .invoke_response_with_timeout(
            "pane.get",
            json!({ "pane_id": "hung" }),
            Duration::from_secs(10),
        )
        .await
        .expect_err("explicit response deadline expires");
    assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
    assert!(error.message.contains("request deadline"));
    assert_eq!(fixture.requests().await.len(), 2);
}

#[tokio::test]
async fn shutdown_interrupts_a_hung_inflight_dispatch_without_server_release() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(
        &lifecycle_snapshot(),
    ));
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-1" }),
        response: RecordedResponse::Hang,
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned hung-shutdown fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures before dispatch");
    let execution = ExecutionId(9_200_001);
    adapter
        .dispatch_portable(PortableDispatchRequest {
            execution,
            action: ResolvedPortableAction {
                action: PortableAction::Pane(PaneAction::Close),
            },
            origin,
        })
        .await
        .expect("hung dispatch is accepted");
    wait_for_lifecycle_requests(&fixture, 4, "hung dispatch request").await;

    tokio::time::timeout(Duration::from_secs(2), adapter.shutdown())
        .await
        .expect("shutdown interrupts a hung response read")
        .expect("shutdown joins its retired queue owner");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
            execution: completed,
            ..
        }) if completed == execution
    ));
}

#[tokio::test]
async fn suspend_interrupts_a_hung_inflight_dispatch_without_server_release() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(
        &lifecycle_snapshot(),
    ));
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-1" }),
        response: RecordedResponse::Hang,
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned hung-suspend fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures before dispatch");
    let execution = ExecutionId(9_200_002);
    adapter
        .dispatch_portable(PortableDispatchRequest {
            execution,
            action: ResolvedPortableAction {
                action: PortableAction::Pane(PaneAction::Close),
            },
            origin,
        })
        .await
        .expect("hung dispatch is accepted");
    wait_for_lifecycle_requests(&fixture, 4, "hung dispatch request").await;

    tokio::time::timeout(Duration::from_secs(2), adapter.suspend_for_activation())
        .await
        .expect("suspend interrupts a hung response read")
        .expect("suspend joins its retired queue owner");
    let mut saw_unknown = false;
    let mut saw_unhealthy = false;
    for _ in 0..2 {
        match adapter.next_health_event().await.unwrap() {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution: completed,
                ..
            }) if completed == execution => saw_unknown = true,
            AdapterHealthEvent::Unhealthy { .. } => saw_unhealthy = true,
            _ => panic!("unexpected suspend terminal event"),
        }
    }
    assert!(saw_unknown);
    assert!(saw_unhealthy);
    adapter
        .shutdown()
        .await
        .expect("suspended adapter shuts down");
}

#[tokio::test]
async fn launches_an_exact_command_as_a_labeled_unfocused_tab() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.truncate(1);
    script.push(RecordedExchange {
        method: "layout.apply",
        params: json!({
            "focus": false,
            "workspace_id": "workspace-1",
            "tab_label": "logs",
            "root": {
                "type": "pane",
                "command": ["tail", "--follow"],
                "cwd": "/captured/origin",
                "env": {},
            },
        }),
        response: RecordedResponse::Result(json!({
            "type": "layout_apply",
            "layout": {
                "workspace_id": "workspace-1",
                "tab_id": "logs-tab",
                "zoomed": false,
                "focused_pane_id": "logs-pane",
                "root": { "type": "pane", "pane_id": "logs-pane" },
            },
        })),
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned command-tab fixture starts");
    let runtime = HerdrRuntime::connect(fixture.adapter_config())
        .await
        .expect("guarded runtime connects");

    open_command_tab(
        &runtime,
        CommandTabLaunch {
            workspace: muxe_core::WorkspaceId::new("workspace-1"),
            label: Some("logs".to_owned()),
            cwd: "/captured/origin".into(),
            argv: vec!["tail".to_owned(), "--follow".to_owned()],
            focus: false,
        },
    )
    .await
    .expect("exact tab command launches");
    assert_eq!(fixture.requests().await.len(), 2);
    drop(runtime);
    drop(fixture);
}

#[tokio::test]
async fn shutdown_joins_queue_owner_and_settles_each_accepted_dispatch_once() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(
        &lifecycle_snapshot(),
    ));
    script.push(RecordedExchange {
        method: "pane.close",
        params: json!({ "pane_id": "pane-1" }),
        response: RecordedResponse::Hang,
    });
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned shutdown-drain fixture");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter.next_health_event().await.unwrap(),
        AdapterHealthEvent::Healthy { .. }
    ));
    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures before dispatch admission");
    let first = ExecutionId(9_100_001);
    let second = ExecutionId(9_100_002);
    for execution in [first, second] {
        adapter
            .dispatch_portable(PortableDispatchRequest {
                execution,
                action: ResolvedPortableAction {
                    action: PortableAction::Pane(PaneAction::Close),
                },
                origin: origin.clone(),
            })
            .await
            .expect("dispatch is accepted into the bounded runtime queue");
    }
    wait_for_lifecycle_requests(&fixture, 4, "in-flight dispatch before shutdown").await;
    tokio::time::timeout(Duration::from_secs(2), adapter.shutdown())
        .await
        .expect("shutdown settles accepted work within two seconds")
        .expect("shutdown joins the send-queue owner");

    let mut first_outcome_unknown = false;
    let mut second_failed_not_sent = false;
    for _ in 0..2 {
        match adapter
            .next_health_event()
            .await
            .expect("accepted dispatch has one deterministic terminal")
        {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution,
                ..
            }) if execution == first => first_outcome_unknown = true,
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Failed {
                execution,
                error,
            }) if execution == second
                && matches!(
                    error.kind,
                    AdapterErrorKind::Shutdown | AdapterErrorKind::Unavailable
                ) =>
            {
                second_failed_not_sent = true;
            }
            _ => panic!("unexpected accepted-dispatch terminal"),
        }
    }
    assert!(first_outcome_unknown);
    assert!(second_failed_not_sent);
    assert!(matches!(
        adapter.next_health_event().await,
        Err(AdapterError {
            kind: AdapterErrorKind::Shutdown,
            ..
        })
    ));
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![
            json!("ping"),
            json!("events.subscribe"),
            json!("session.snapshot"),
            json!("pane.close"),
        ],
        "shutdown lets the in-flight request settle and fails queued accepted work without a write",
    );
}

/// One-consumer invariant: `next_health_event` is called only by the broker
/// monitor. The dispatch wake must be registered, pinned, and enabled BEFORE
/// checking for queued terminal outcomes or finalized shutdown status. If
/// shutdown finalization runs and calls `notify_waiters()` between wake
/// registration and `select!`, the pre-registered notification is preserved:
/// queued terminal outcomes drain first, and subsequent calls exit bounded with
/// `AdapterErrorKind::Shutdown`.
#[tokio::test]
async fn shutdown_finalization_between_wake_registration_and_select_drains_terminals_bounded() {
    let fixture = ProductionConnectFixture::start().expect("owned fake-native fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connect accepts the recorded child and socket handshake");
    // Consume the initial Healthy event published during connect.
    let initial = adapter
        .next_health_event()
        .await
        .expect("initial health event");
    assert!(matches!(initial, AdapterHealthEvent::Healthy { .. }));

    let hook = Arc::new(muxe_adapter_herdr::WaitHook::new());
    adapter.set_health_wait_hook(Some(Arc::clone(&hook)));

    let execution = ExecutionId(77);
    let next_task = tokio::spawn({
        let adapter = Arc::clone(&adapter);
        async move { adapter.next_health_event().await }
    });
    hook.entered.notified().await;

    adapter.send_dispatch_terminal_for_test(execution, DispatchCompletion::Succeeded { execution });
    let shutdown_task = tokio::spawn({
        let adapter = Arc::clone(&adapter);
        async move { adapter.shutdown().await }
    });
    hook.release.notify_one();
    let event = tokio::time::timeout(Duration::from_secs(2), next_task)
        .await
        .expect("next_health_event returns within bounded time")
        .expect("next_health_event task joins")
        .expect("next_health_event returns ok event");
    assert!(matches!(
        event,
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded { execution: e }) if e == execution
    ));

    // 2. Shutdown must complete boundedly without deadlocking on retained senders.
    tokio::time::timeout(Duration::from_secs(2), shutdown_task)
        .await
        .expect("shutdown finishes within bounded time")
        .expect("shutdown task joins")
        .expect("shutdown succeeds");

    // 3. Once fully drained, `next_health_event` returns Shutdown boundedly.
    adapter.set_health_wait_hook(None);
    let terminal = tokio::time::timeout(Duration::from_secs(2), adapter.next_health_event())
        .await
        .expect("final next_health_event returns within bounded time");
    assert!(matches!(
        terminal,
        Err(AdapterError {
            kind: AdapterErrorKind::Shutdown,
            ..
        })
    ));
}

/// M31: after `shutdown`, every host-bound operation fails closed and the
/// recorded server receives no new request bytes. The fixture counts requests
/// before/after; the only forced disconnect is the retained-stream drop at
/// shutdown, never a new JSON-RPC request.
#[tokio::test]
async fn shutdown_leaves_no_host_bound_operation_reaching_the_recorded_server() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(
        &lifecycle_snapshot(),
    ));
    // One pane.get proves register reaches the host before shutdown; no
    // further host request may follow shutdown.
    script.push(pending_pane_get());
    let fixture =
        ProductionConnectFixture::start_scripted(script).expect("owned shutdown fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));
    let origin = adapter
        .capture_origin(lifecycle_capture_request())
        .await
        .expect("origin captures before shutdown");
    let lease = adapter
        .register_pending_pane(pending_pane_registration())
        .await
        .expect("register reaches the host before shutdown");
    // Handshake plus capture plus register: ping + subscribe + snapshot +
    // pane.get. The scripted script must cover exactly those exchanges.
    fixture.wait_for_requests(4).await;
    let before = fixture.requests().await.len();
    assert_eq!(before, 4, "handshake plus capture plus register only");

    adapter.shutdown().await.expect("shutdown succeeds");
    let after_shutdown = fixture.requests().await.len();
    assert_eq!(
        after_shutdown, before,
        "shutdown itself sends no JSON-RPC request"
    );

    let modal = adapter.modal_scope(&PaneId::new("pane-1")).await;
    assert!(
        matches!(
            &modal,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
                || error.kind == AdapterErrorKind::Unavailable
        ),
        "modal_scope fails closed after shutdown, got {modal:?}"
    );
    let capture = adapter.capture_origin(lifecycle_capture_request()).await;
    assert!(
        matches!(
            &capture,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
                || error.kind == AdapterErrorKind::Unavailable
        ),
        "capture_origin fails closed after shutdown, got {capture:?}"
    );
    let register = adapter
        .register_pending_pane(pending_pane_registration())
        .await;
    assert!(
        matches!(
            &register,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
                || error.kind == AdapterErrorKind::Unavailable
        ),
        "register_pending_pane fails closed after shutdown, got {register:?}"
    );
    let close = adapter
        .close_pending_pane(pending_pane_registration(), lease)
        .await;
    assert!(
        matches!(
            &close,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
                || error.kind == AdapterErrorKind::Unavailable
        ),
        "close_pending_pane fails closed after shutdown, got {close:?}"
    );
    let post_dismissal = adapter
        .dispatch_portable_after_ui_dismissal(PostDismissalPortableDispatchRequest {
            execution: ExecutionId(7_310_001),
            action: ResolvedPortableAction {
                action: PortableAction::Tab(TabAction::Create {
                    workspace_id: Some(lifecycle_scalar("workspace-1")),
                    name: Some(lifecycle_scalar("logs")),
                    focus: None,
                    command: CreateCommand::default(),
                }),
            },
            origin,
            ui_pane: PaneId::new("pane-2"),
        })
        .await;
    assert!(
        matches!(
            &post_dismissal,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
                || error.kind == AdapterErrorKind::Unavailable
        ),
        "post-dismissal dispatch fails closed after shutdown, got {post_dismissal:?}"
    );
    // `identity` stays gated by continuity (not ungated): with continuity
    // invalidated it must fail closed rather than return the retained value.
    let identity = adapter.identity().await;
    assert!(
        matches!(
            &identity,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
                || error.kind == AdapterErrorKind::Unavailable
        ),
        "identity fails closed after shutdown, got {identity:?}"
    );
    // `capabilities` is local-only: it may succeed, but must send no bytes.
    let _ = adapter.capabilities().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        fixture.requests().await.len(),
        before,
        "no host-bound operation after shutdown reached the recorded server"
    );
    drop(adapter);
    drop(fixture);
}

/// M31: a shutdown raced against a suspended adapter cannot be undone by a
/// later resume: the resume fails closed and installs no subscription, so no
/// subscribe/ping bytes reach the fixture beyond the initial handshake.
#[tokio::test]
async fn shutdown_while_suspended_cannot_be_revived_by_resume() {
    let fixture =
        ProductionConnectFixture::start_scripted(ProductionConnectFixture::initial_handshake())
            .expect("owned suspend-shutdown fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connects before suspend");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("initial health event"),
        AdapterHealthEvent::Healthy { .. }
    ));
    tokio::time::timeout(Duration::from_secs(10), adapter.suspend_for_activation())
        .await
        .expect("suspend completes")
        .expect("suspend stops the retained subscription");
    fixture.wait_for_requests(2).await;
    let before = fixture.requests().await.len();

    adapter
        .shutdown()
        .await
        .expect("shutdown succeeds while suspended");
    let resume = adapter.resume_after_activation_abort().await;
    assert!(
        matches!(
            &resume,
            Err(error) if error.kind == AdapterErrorKind::Shutdown
        ),
        "resume after shutdown fails closed with Shutdown, got {resume:?}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        fixture.requests().await.len(),
        before,
        "no resume subscribe/ping reached the recorded server after shutdown"
    );
    drop(adapter);
    drop(fixture);
}
