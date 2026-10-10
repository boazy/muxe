async fn configured_command_adapter(
    temp: &tempfile::TempDir,
) -> (Arc<HerdrAdapter>, UnixListener, UnixListener, tokio::net::UnixStream, OriginContext) {
    let raw = crate::commands::fixture::schema_json();
    let protocol = raw["protocol"].as_u64().unwrap();
    std::fs::write(temp.path().join("schema.json"), raw.to_string()).unwrap();
    let binary = temp.path().join("herdr");
    crate::generated_executable::write_executable_script(&binary, |writer| {
        std::io::Write::write_all(writer, b"#!/bin/sh\n[ \"$1\" = api ] && [ \"$2\" = schema ] && [ \"$3\" = --json ] && [ \"$#\" = 3 ] || exit 64\nexec cat \"$(dirname \"$0\")/schema.json\"\n")
    }).unwrap();
    let socket = temp.path().join("endpoint.sock");
    let api_listener = UnixListener::bind(&socket).unwrap();
    let client_listener = UnixListener::bind(temp.path().join("endpoint-client.sock")).unwrap();
    let handshake = async {
        let mut retained = None;
        for method in ["ping", "events.subscribe"] {
            let (stream, _) = api_listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            let request: Value = serde_json::from_slice(&line).unwrap();
            assert_eq!(request["method"], method);
            let result = if method == "ping" {
                json!({ "type": "pong", "protocol": protocol, "version": "0.8.2" })
            } else {
                json!({ "type": "subscription_started" })
            };
            let response = json!({ "id": request["id"], "result": result });
            reader.get_mut().write_all(format!("{response}\n").as_bytes()).await.unwrap();
            if method == "events.subscribe" { retained = Some(reader.into_inner()); }
        }
        retained.unwrap()
    };
    let config = HerdrAdapterConfig {
        socket_path: socket,
        herdr_binary: binary,
        cache_dir: temp.path().join("cache"),
    };
    let (adapter, retained) = tokio::join!(HerdrAdapter::connect(config), handshake);
    let adapter = adapter.unwrap();
    let mut captured = origin();
    captured.server_id = ServerId::new(origin_epoch_token(
        adapter.runtime().identity(), IncarnationEpoch::INITIAL,
    ));
    captured.workspace_id = Some(muxe_core::WorkspaceId::new("captured-workspace"));
    captured.tab_id = Some(muxe_core::TabId::new("captured-tab"));
    captured.pane_id = Some(muxe_core::PaneId::new("captured-pane"));
    (adapter, api_listener, client_listener, retained, captured)
}

async fn accept_configured_command(
    api_listener: &UnixListener,
    client_listener: &UnixListener,
) -> (tokio::net::UnixStream, Value) {
    use crate::commands::fixture;
    let (mut stream, _) = client_listener.accept().await.unwrap();
    fixture::accept_inactive_hello(&mut stream).await;
    fixture::write_control(&mut stream, "endpoint.welcome.v1", &fixture::welcome()).await;
    fixture::write_control(&mut stream, "shell.snapshot.v1", &fixture::snapshot("fixture-configured-command")).await;
    drop(stream);
    let (mut stream, _) = api_listener.accept().await.unwrap();
    let request = fixture::read_request(&mut stream).await;
    assert_eq!(request["method"], "command.invoke");
    assert_eq!(request["params"], json!({
        "command_id": "fixture-configured-command",
        "workspace_id": "captured-workspace",
        "tab_id": "captured-tab",
        "pane_id": "captured-pane",
    }));
    (stream, request)
}

async fn next_dispatch_completion(adapter: &HerdrAdapter) -> DispatchCompletion {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let AdapterHealthEvent::DispatchCompleted(completion) = adapter.next_health_event().await.unwrap() {
                return completion;
            }
        }
    }).await.expect("accepted native execution settles")
}

#[tokio::test]
async fn configured_command_dispatch_settles_success_rejection_and_unconfirmed_send() {
    for outcome in ["success", "remote-error", "eof"] {
        let temp = tempfile::tempdir().unwrap();
        let (adapter, api_listener, client_listener, retained, captured) = configured_command_adapter(&temp).await;
        let execution = muxe_core::ExecutionId(7_710_001);
        let accepted = adapter.dispatch_native(NativeDispatchRequest {
            execution,
            action: muxe_adapter_api::ResolvedNativeAction {
                candidate: configured_command_candidate(ConfigValueKind::String("prefix+shift+u".to_owned())),
            },
            origin: captured,
        }).await.unwrap();
        assert_eq!(accepted.execution, execution);
        assert_eq!(accepted.capabilities, ExecutionCapabilities::ASYNCHRONOUS);
        assert_eq!(adapter.cancel(execution).await.unwrap_err().kind, AdapterErrorKind::CancelUnsupported);
        let (mut stream, request) = accept_configured_command(&api_listener, &client_listener).await;
        if outcome != "eof" {
            let response = if outcome == "success" {
                json!({ "id": request["id"], "result": { "type": "ok" } })
            } else {
                json!({ "id": request["id"], "error": {
                    "code": "configured_command_denied", "message": "owned remote cause",
                } })
            };
            crate::commands::fixture::write_response(&mut stream, &response).await;
        }
        drop(stream);
        match (outcome, next_dispatch_completion(&adapter).await) {
            ("success", DispatchCompletion::Succeeded { execution: actual }) => assert_eq!(actual, execution),
            ("remote-error", DispatchCompletion::Failed { execution: actual, error }) => {
                assert_eq!(actual, execution);
                assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
                assert!(error.to_string().contains("owned remote cause"));
            }
            ("eof", DispatchCompletion::OutcomeUnknown { execution: actual, error }) => {
                assert_eq!(actual, execution);
                assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
            }
            (_, completion) => panic!("wrong configured command terminal: {completion:?}"),
        }
        adapter.shutdown().await.unwrap();
        drop(retained);
    }
}

#[tokio::test]
async fn configured_command_dispatch_serializes_with_raw_native_requests() {
    let temp = tempfile::tempdir().unwrap();
    let (adapter, api_listener, client_listener, retained, captured) = configured_command_adapter(&temp).await;
    let first = muxe_core::ExecutionId(7_710_002);
    let second = muxe_core::ExecutionId(7_710_003);
    adapter.dispatch_native(NativeDispatchRequest {
        execution: first,
        action: muxe_adapter_api::ResolvedNativeAction {
            candidate: configured_command_candidate(ConfigValueKind::String("prefix+shift+u".to_owned())),
        },
        origin: captured.clone(),
    }).await.unwrap();
    let (mut stream, request) = accept_configured_command(&api_listener, &client_listener).await;
    let mut direct = configured_command_candidate(ConfigValueKind::Null);
    direct.type_name = "native.herdr.agent:list".to_owned();
    direct.fields.clear();
    adapter.dispatch_native(NativeDispatchRequest {
        execution: second,
        action: muxe_adapter_api::ResolvedNativeAction { candidate: direct },
        origin: captured,
    }).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(25), api_listener.accept()).await.is_err(),
        "raw native request cannot overtake the unacknowledged configured invocation");
    let response = json!({ "id": request["id"], "result": { "type": "ok" } });
    crate::commands::fixture::write_response(&mut stream, &response).await;
    let (raw, _) = api_listener.accept().await.unwrap();
    let mut raw = BufReader::new(raw);
    let mut line = Vec::new();
    raw.read_until(b'\n', &mut line).await.unwrap();
    let request: Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(request["method"], "agent.list");
    assert_eq!(request["params"], json!({}));
    let response = json!({ "id": request["id"], "result": { "type": "agent_list", "agents": [] } });
    crate::commands::fixture::write_response(raw.get_mut(), &response).await;
    assert!(matches!(next_dispatch_completion(&adapter).await, DispatchCompletion::Succeeded { execution } if execution == first));
    assert!(matches!(next_dispatch_completion(&adapter).await, DispatchCompletion::Succeeded { execution } if execution == second));
    adapter.shutdown().await.unwrap();
    drop(retained);
}

#[tokio::test]
async fn configured_command_guard_refusals_send_no_mutation_and_keep_origin_lease() {
    for rebind in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (adapter, _api_listener, client_listener, retained, captured) = configured_command_adapter(&temp).await;
        let hook = Arc::new(WaitHook::new());
        adapter.set_request_connect_wait_hook(Some(Arc::clone(&hook)));
        let execution = muxe_core::ExecutionId(7_710_004);
        let mut stale = captured.clone();
        stale.server_id = ServerId::new("stale-server");
        let candidate = configured_command_candidate(ConfigValueKind::String("prefix+shift+u".to_owned()));
        assert_eq!(adapter.dispatch_native(NativeDispatchRequest {
            execution,
            action: muxe_adapter_api::ResolvedNativeAction { candidate: candidate.clone() },
            origin: stale,
        }).await.unwrap_err().kind, AdapterErrorKind::ContextUnavailable);
        adapter.dispatch_native(NativeDispatchRequest {
            execution,
            action: muxe_adapter_api::ResolvedNativeAction { candidate },
            origin: captured,
        }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), hook.entered.notified()).await.unwrap();
        let replacement = if rebind {
            std::fs::remove_file(&adapter.config.socket_path).unwrap();
            Some(UnixListener::bind(&adapter.config.socket_path).unwrap())
        } else {
            adapter.runtime().retire_send_queue();
            None
        };
        hook.release.notify_one();
        assert!(matches!(next_dispatch_completion(&adapter).await, DispatchCompletion::Failed { execution: actual, .. } if actual == execution));
        assert!(tokio::time::timeout(Duration::from_millis(25), client_listener.accept()).await.is_err());
        if let Some(replacement) = replacement {
            // The continuity monitor may independently start a read-only ping.
            // Refusal prohibits mutation, not that existing recovery probe.
            if let Ok(Ok((probe, _))) =
                tokio::time::timeout(Duration::from_millis(25), replacement.accept()).await
            {
                let mut probe = BufReader::new(probe);
                let mut line = Vec::new();
                tokio::time::timeout(Duration::from_secs(2), probe.read_until(b'\n', &mut line)).await.unwrap().unwrap();
                let request: Value = serde_json::from_slice(&line).expect("only a read-only JSON probe may reach the replacement");
                assert_eq!(request["method"], "ping");
                assert_eq!(request["params"], json!({}));
            }
        }
        adapter.shutdown().await.unwrap();
        drop(retained);
    }
}

#[tokio::test]
async fn configured_command_shutdown_after_send_retains_unknown_terminal() {
    let temp = tempfile::tempdir().unwrap();
    let (adapter, api_listener, client_listener, retained, captured) =
        configured_command_adapter(&temp).await;
    let execution = muxe_core::ExecutionId(7_710_005);
    adapter.dispatch_native(NativeDispatchRequest {
        execution,
        action: muxe_adapter_api::ResolvedNativeAction {
            candidate: configured_command_candidate(ConfigValueKind::String("prefix+shift+u".to_owned())),
        },
        origin: captured,
    }).await.unwrap();
    // Keep the API stream alive with no response: consuming the correlated
    // request proves the host may have acted before retirement wins.
    let (stream, _request) = accept_configured_command(&api_listener, &client_listener).await;
    tokio::time::timeout(Duration::from_secs(2), adapter.shutdown())
        .await.expect("shutdown joins the accepted command forwarder").unwrap();
    match next_dispatch_completion(&adapter).await {
        DispatchCompletion::OutcomeUnknown { execution: actual, error } => {
            assert_eq!(actual, execution);
            assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
        }
        completion => panic!("shutdown lost or reclassified the sent invocation: {completion:?}"),
    }
    assert_eq!(
        adapter.next_health_event().await.err().expect("the stopped adapter reports shutdown").kind,
        AdapterErrorKind::Shutdown
    );
    drop(stream);
    drop(retained);
}
