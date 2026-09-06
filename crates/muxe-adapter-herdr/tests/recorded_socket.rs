mod support {
    pub mod production_connect;
    pub mod recorded_socket;
}

use muxe_core::{ActionValidator, NativeActionCandidate, SourceId, SourceSpan};
use muxe_adapter_api::{AdapterHealthEvent, HostAdapter};
use muxe_adapter_herdr::{
    ApiSchema, CommandPaneLaunch, FocusedPane, HerdrAdapter, HerdrResponse, HerdrRuntime,
    HerdrSocketClient, UiSplitDirection, focused_pane, generated::method_metadata,
    open_command_pane,
};
use serde_json::json;
use support::{
    production_connect::ProductionConnectFixture,
    recorded_socket::{RecordedExchange, RecordedResponse, RecordedUnixServer},
};

#[tokio::test]
async fn captures_the_client_generated_id_for_an_exact_ping_exchange() {
    let server = RecordedUnixServer::start(
        tempfile::tempdir().expect("owned fixture directory"),
        vec![RecordedExchange {
            method: "ping",
            params: json!({}),
            response: RecordedResponse::Result(json!({
                "type": "pong",
                "protocol": 20,
                "version": "0.8.2",
            })),
        }],
    )
    .await
    .expect("recorded server starts");
    let client = HerdrSocketClient::new(server.socket());
    let metadata = method_metadata("ping").expect("bundled ping metadata");

    let response = client
        .unary(metadata, json!({}))
        .await
        .expect("exact recorded request succeeds");
    assert!(matches!(response, HerdrResponse::Success(_)));

    let requests = server.finish().await.expect("recorded exchange finishes");
    assert_eq!(requests.len(), 1);
    assert!(requests[0]["id"].as_str().is_some_and(|id| !id.is_empty()));
}

#[tokio::test]
async fn connects_the_production_adapter_through_schema_ping_probe_and_retained_subscription() {
    let fixture = ProductionConnectFixture::start()
        .await
        .expect("owned fake-native fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production adapter connect accepts the recorded child and socket handshake");
    let actual = HostAdapter::identity(adapter.as_ref())
        .await
        .expect("retained subscription keeps the adapter healthy");
    let expected = fixture
        .raw_identity()
        .await
        .expect("fixture can observe the same raw host identity");
    assert_eq!(actual, expected);
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![json!("ping"), json!("events.subscribe")],
        "production connect must use its schema child, ping, raw probe, then retained subscription"
    );
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
        .await
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
    let runtime_schema: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/herdr/herdr-api.schema.json"))
            .expect("bundled schema JSON");
    let first_fixture =
        ProductionConnectFixture::start_scripted_with_schema(
            vec![ProductionConnectFixture::ping_exchange()],
            runtime_schema.clone(),
        )
        .await
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
        drifted_schema,
    )
    .await
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
    let fixture = ProductionConnectFixture::start()
        .await
        .expect("owned production batch fixture starts");
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
    assert_eq!(first_diagnostics[0].labels[0].span.source.as_str(), "first-invalid");

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
async fn reconnects_the_production_adapter_only_after_retained_subscription_loss() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.extend(ProductionConnectFixture::initial_handshake());
    let fixture = ProductionConnectFixture::start_scripted(script)
        .await
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
async fn launches_an_exact_command_from_the_live_focused_origin() {
    let server = RecordedUnixServer::start(
        tempfile::tempdir().expect("owned fixture directory"),
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
                    "tab_id": "temporary-tab",
                    "pane_id": "new-pane",
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
                response: RecordedResponse::Result(json!({ "changed": true })),
            },
        ],
    )
    .await
    .expect("recorded server starts");
    let client = HerdrSocketClient::new(server.socket());
    let schema = ApiSchema::parse(
        serde_json::from_str(include_str!("../../../fixtures/herdr/herdr-api.schema.json"))
            .expect("bundled schema JSON"),
    )
    .expect("bundled schema parses");

    let origin = focused_pane(&client, &schema)
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
        &client,
        &schema,
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
    assert_eq!(
        server
            .finish()
            .await
            .expect("all recorded requests complete")
            .len(),
        3
    );
}
