mod support {
    #[expect(
        dead_code,
        reason = "the suspend test drives start_scripted plus snapshot exchanges; start, raw_identity, and reconnect helpers belong to the shared production-connect surface"
    )]
    pub mod production_connect;
    #[expect(
        dead_code,
        reason = "the suspend test shares the complete recorded socket surface with focused transport tests"
    )]
    pub mod recorded_socket;
}

use std::time::Duration;

use muxe_adapter_api::{
    AdapterErrorKind, AdapterHealthEvent, HostAdapter, HostCallerIdentity, OriginCaptureRequest,
    OriginHintSource, UiSessionId, UntrustedOriginHint,
};
use muxe_adapter_herdr::HerdrAdapter;
use muxe_core::{PaneId, TabId, WorkspaceId};
use serde_json::{Value, json};
use support::production_connect::ProductionConnectFixture;

fn origin_snapshot() -> Value {
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
    })
}

fn capture_request() -> OriginCaptureRequest {
    OriginCaptureRequest {
        ui_session: UiSessionId::new("ui-suspend-test"),
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

#[tokio::test]
async fn suspend_proves_stream_release_and_resume_starts_a_fresh_epoch() {
    let mut script = ProductionConnectFixture::initial_handshake();
    script.push(ProductionConnectFixture::snapshot_exchange(
        &origin_snapshot(),
    ));
    script.extend(ProductionConnectFixture::initial_handshake());
    script.push(ProductionConnectFixture::snapshot_exchange(
        &origin_snapshot(),
    ));
    let fixture = ProductionConnectFixture::start_scripted(script)
        .expect("owned fake-native suspend fixture starts");
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

    let before = adapter
        .capture_origin(capture_request())
        .await
        .expect("origin captures before suspend");

    // The timeout only fails the test loudly if the monitor never acks; the
    // ack itself is sent strictly after the old subscription socket is dropped.
    tokio::time::timeout(Duration::from_secs(10), adapter.suspend_for_activation())
        .await
        .expect("suspend completes instead of hanging on a blocked read")
        .expect("suspend stops the retained subscription");
    let suspended = adapter
        .identity()
        .await
        .expect_err("no host-bound operation succeeds while suspended");
    assert_eq!(suspended.kind, AdapterErrorKind::Unavailable);

    adapter
        .resume_after_activation_abort()
        .await
        .expect("abort resumes the retained subscription");
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("suspend health event"),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("resume health event"),
        AdapterHealthEvent::Reconnected { .. }
    ));

    let after = adapter
        .capture_origin(capture_request())
        .await
        .expect("origin captures after resume");
    assert_ne!(
        before.server_id, after.server_id,
        "resume mints a fresh continuity epoch; raw endpoint equality never proves continuity"
    );

    // The fresh target-side subscription exists only after the old stream was
    // released: suspend was awaited before resume ran.
    fixture.wait_for_requests(6).await;
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
            json!("ping"),
            json!("events.subscribe"),
            json!("session.snapshot"),
        ]
    );
    drop(adapter);
    drop(fixture);
}

#[tokio::test]
async fn failed_resume_leaves_the_adapter_unhealthy() {
    let fixture =
        ProductionConnectFixture::start_scripted(ProductionConnectFixture::initial_handshake())
            .expect("owned fake-native resume-failure fixture starts");
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

    // The whole fake host is gone: schema child, socket, and listener.
    drop(fixture);
    let failed = adapter
        .resume_after_activation_abort()
        .await
        .expect_err("resume fails without the retained host");
    assert_eq!(failed.kind, AdapterErrorKind::Unavailable);
    assert!(
        adapter.identity().await.is_err(),
        "a failed resume never claims a healthy rollback"
    );

    assert!(matches!(
        adapter
            .next_health_event()
            .await
            .expect("suspend health event"),
        AdapterHealthEvent::Unhealthy { .. }
    ));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), adapter.next_health_event())
            .await
            .is_err(),
        "no reconnect is reported after a failed resume"
    );
    drop(adapter);
}
