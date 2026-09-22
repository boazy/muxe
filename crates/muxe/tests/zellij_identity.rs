use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use muxe::lifecycle::control::ControlClient;
use muxe_adapter_api::{AdapterError, HostAdapter, HostIdentity};
use muxe_adapter_zellij::{
    MembershipSource, PipeChannel, ScriptedChannel, ZellijAdapter, ZellijAdapterConfig,
};
use muxe_broker::{
    ActivationBootstrap, Broker, BrokerClient, BrokerServer, ClientError, RuntimeEndpoint,
};
use muxe_core::{CompiledGeneration, KeyCapabilities, SourceId};
use muxe_protocol::{HostKind, LiveServerIdentity, PeerRole, ServerId};
use muxe_zellij_protocol::{
    BRIDGE_PROTOCOL_VERSION, BridgeEvent, BridgeIdentity, ChannelGeneration, PipeEvent,
    PipeEventKind, RegistrationId, ZellijRegistration, bridge_build_id,
    bridge_protocol_fingerprint, decode_event_subscription, encode_event_line,
    generated_action_fingerprint, pinned_source_revision,
};
use tokio::sync::watch;

struct SingleClientMembership;

#[async_trait]
impl MembershipSource for SingleClientMembership {
    async fn snapshot_members(&self) -> Result<Vec<String>, AdapterError> {
        Ok(vec!["client-1".to_owned()])
    }
}

fn register_event(seed: u8, generation: ChannelGeneration) -> PipeEvent {
    PipeEvent {
        protocol: BRIDGE_PROTOCOL_VERSION,
        request_id: None,
        channel_generation: generation,
        registration: RegistrationId::from_random_bytes([seed; 16])
            .expect("test registration is nonzero"),
        event: PipeEventKind::Event(BridgeEvent::Register {
            registration: ZellijRegistration {
                client_id: "client-1".to_owned(),
                current_pane: Some("terminal_2".to_owned()),
                plugin_id: Some(3),
                identity: BridgeIdentity {
                    muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
                    source_revision: pinned_source_revision().to_owned(),
                    action_fingerprint: generated_action_fingerprint().0,
                    protocol_fingerprint: bridge_protocol_fingerprint().0,
                    bridge_build_id: Some(bridge_build_id()),
                },
            },
        }),
    }
}

fn push_registration(event: &ScriptedChannel, seed: u8) {
    let generation = event
        .initial_payload()
        .map(|payload| {
            decode_event_subscription(&payload)
                .expect("subscription payload decodes")
                .channel_generation()
        })
        .unwrap_or(ChannelGeneration::INITIAL);
    event
        .push_line(encode_event_line(&register_event(seed, generation)).expect("register encodes"));
}

async fn await_identity(adapter: &ZellijAdapter) -> HostIdentity {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(identity) = adapter.identity().await {
                return identity;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("covered identity becomes available")
}

fn wire_identity(identity: &HostIdentity) -> LiveServerIdentity {
    LiveServerIdentity {
        host: HostKind::Zellij,
        discovery_key: identity.discovery_key.as_str().to_owned(),
        server_id: ServerId::new(identity.live_server_id.as_str()),
    }
}

#[tokio::test]
async fn zellij_rotation_rejects_old_hello_and_accepts_current_status_identity() {
    let request = ScriptedChannel::new();
    let event = ScriptedChannel::new();
    let adapter = Arc::new(ZellijAdapter::new_with_membership(
        ZellijAdapterConfig {
            session_name: "session-alpha".to_owned(),
            zellij_exe: PathBuf::from("/nonexistent/zellij"),
        },
        Arc::clone(&request) as Arc<dyn PipeChannel>,
        Arc::clone(&event) as Arc<dyn PipeChannel>,
        Arc::new(SingleClientMembership),
    ));

    push_registration(&event, 7);
    let identity_a = await_identity(&adapter).await;
    adapter
        .suspend_for_activation()
        .await
        .expect("suspend invalidates identity A");
    let previous_epoch = event.install_epoch().await.expect("initial event epoch");
    let resume = tokio::spawn({
        let adapter = Arc::clone(&adapter);
        async move { adapter.resume_after_activation_abort().await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if event
                .install_epoch()
                .await
                .is_some_and(|epoch| epoch > previous_epoch)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("resume installs a fresh event epoch");
    push_registration(&event, 10);
    resume
        .await
        .expect("resume task joins")
        .expect("fresh coverage resumes adapter");
    let identity_b = await_identity(&adapter).await;
    assert_eq!(identity_a.discovery_key, identity_b.discovery_key);
    assert_ne!(identity_a.live_server_id, identity_b.live_server_id);

    let directory = tempfile::tempdir().expect("owned broker config directory");
    let runtime = tempfile::tempdir().expect("owned broker runtime directory");
    let config_path = directory.path().join("config.yml");
    let config = muxe_core::compile_yaml(
        CompiledGeneration(1),
        SourceId::new("<zellij identity rotation>"),
        "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
        KeyCapabilities::default(),
        Some(adapter.as_ref()),
    )
    .expect("compile broker config");
    let broker = Broker::from_compiled(
        Arc::clone(&adapter) as Arc<dyn HostAdapter>,
        &config_path,
        config,
    );
    let endpoint = RuntimeEndpoint::in_runtime_dir(
        runtime.path(),
        HostKind::Zellij,
        identity_b.discovery_key.as_str(),
    )
    .expect("derive owned Zellij endpoint");
    let record = muxe::compatibility::embedded_record()
        .expect("embedded compatibility record")
        .handoff;
    let server = BrokerServer::start_activation(
        Arc::clone(&broker),
        endpoint.clone(),
        ActivationBootstrap::Running { current: record },
        None,
    )
    .await
    .expect("start owned Zellij broker");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server_task = tokio::spawn(server.run(shutdown_rx));

    let mut control = ControlClient::connect(endpoint.socket())
        .await
        .expect("connect broker control status");
    let status = control.status().await.expect("read broker control status");
    assert_eq!(status.live_server, wire_identity(&identity_b));

    let rejected = match BrokerClient::connect(
        endpoint.socket(),
        PeerRole::Ui,
        env!("CARGO_PKG_VERSION"),
        wire_identity(&identity_a),
    )
    .await
    {
        Ok(_) => panic!("status-A identity is rejected after continuity rotation"),
        Err(error) => error,
    };
    assert!(matches!(
        rejected,
        ClientError::ConnectionClosed | ClientError::IdentityMismatch
    ));
    let current = BrokerClient::connect(
        endpoint.socket(),
        PeerRole::Ui,
        env!("CARGO_PKG_VERSION"),
        status.live_server,
    )
    .await
    .expect("fresh status-B identity completes Hello");
    drop(current);

    shutdown_tx.send(true).expect("stop owned broker server");
    tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("broker server stops")
        .expect("broker task joins")
        .expect("broker exits cleanly");
    adapter.shutdown().await.expect("adapter shutdown");
}
