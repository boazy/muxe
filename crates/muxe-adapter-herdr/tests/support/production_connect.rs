use std::{io, os::unix::fs::PermissionsExt, path::PathBuf};

use muxe_adapter_api::{HostIdentity, HostKind};
use muxe_adapter_herdr::{
    EndpointIdentity, HerdrAdapterConfig,
    generated::BUNDLED_PROTOCOL,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::UnixStream;

use super::recorded_socket::{RecordedExchange, RecordedResponse, RecordedUnixServer};

/// A fake-native vertical fixture for the production Herdr connection path.
///
/// It owns a concrete executable that accepts only `herdr api schema --json`, a Unix socket
/// recording `system.ping`, the raw endpoint probe, and a retained `events.subscribe` stream.
/// It never starts or contacts a real Herdr host, and callers must create the adapter through
/// `HerdrAdapter::connect(fixture.adapter_config())` rather than injecting internal parts.
pub struct ProductionConnectFixture {
    _schema_temp: TempDir,
    schema_binary: PathBuf,
    cache_dir: PathBuf,
    server: RecordedUnixServer,
}

impl ProductionConnectFixture {
    /// Starts only the concrete production connection handshake.
    pub async fn start() -> io::Result<Self> {
        Self::start_scripted(Self::initial_handshake()).await
    }

    /// Starts a production `HerdrAdapter::connect` fixture followed by exact additional RPC
    /// exchanges. Every `ping` exchange is followed by the raw endpoint-probe connection made by
    /// `HerdrRuntime::connect`; `KeepOpen` subscriptions can be closed with
    /// [`Self::lose_retained_subscriptions`] to drive a reconnect.
    pub async fn start_scripted(exchanges: Vec<RecordedExchange>) -> io::Result<Self> {
        let schema: Value =
            serde_json::from_str(include_str!("../../../../fixtures/herdr/herdr-api.schema.json"))
                .expect("bundled fixture schema is valid JSON");
        Self::start_scripted_with_schema(exchanges, schema).await
    }

    /// Uses an injected complete runtime schema document while retaining the same concrete schema
    /// child executable, Unix transport, endpoint probe, and exact request recording.
    pub async fn start_scripted_with_schema(
        exchanges: Vec<RecordedExchange>,
        schema: Value,
    ) -> io::Result<Self> {
        let schema_temp = tempfile::tempdir()?;
        let schema_binary = schema_temp.path().join("herdr");
        let schema_json = schema_temp.path().join("schema.json");
        std::fs::write(&schema_json, schema.to_string())?;
        std::fs::write(
            &schema_binary,
            "#!/bin/sh\nif [ \"$1\" != api ] || [ \"$2\" != schema ] || [ \"$3\" != --json ] || [ \"$#\" != 3 ]; then\n  exit 64\nfi\nexec cat \"$(dirname \"$0\")/schema.json\"\n",
        )?;
        std::fs::set_permissions(&schema_binary, std::fs::Permissions::from_mode(0o700))?;
        let server =
            RecordedUnixServer::start_with_endpoint_probe(tempfile::tempdir()?, exchanges).await?;
        let cache_dir = schema_temp.path().join("cache");
        Ok(Self {
            _schema_temp: schema_temp,
            schema_binary,
            cache_dir,
            server,
        })
    }

    /// The exact initial `HerdrAdapter::connect` transport sequence after its schema child:
    /// one ping, raw endpoint probe, and one retained tab-focus subscription.
    pub fn initial_handshake() -> Vec<RecordedExchange> {
        vec![Self::ping_exchange(), Self::subscription_exchange()]
    }

    pub fn ping_exchange() -> RecordedExchange {
        RecordedExchange {
            method: "ping",
            params: json!({}),
            response: RecordedResponse::Result(json!({
                "type": "pong",
                "protocol": BUNDLED_PROTOCOL,
                "version": "0.8.2",
            })),
        }
    }

    pub fn subscription_exchange() -> RecordedExchange {
        RecordedExchange {
            method: "events.subscribe",
            params: json!({
                "subscriptions": [{ "type": "tab.focused" }],
            }),
            response: RecordedResponse::KeepOpen(json!({ "subscribed": true })),
        }
    }

    /// Wraps one exact snapshot body in the generated `session.snapshot` result shape.
    pub fn snapshot_exchange(snapshot: Value) -> RecordedExchange {
        RecordedExchange {
            method: "session.snapshot",
            params: json!({}),
            response: RecordedResponse::Result(json!({
                "type": "session_snapshot",
                "snapshot": snapshot,
            })),
        }
    }

    pub fn adapter_config(&self) -> HerdrAdapterConfig {
        HerdrAdapterConfig {
            socket_path: self.server.socket().to_path_buf(),
            herdr_binary: self.schema_binary.clone(),
            cache_dir: self.cache_dir.clone(),
        }
    }

    /// Captures the same raw OS host identity that the adapter reports after its production
    /// handshake. It is valid only after `HerdrAdapter::connect` has completed, so it cannot
    /// steal the scripted ping or endpoint-probe connections.
    pub async fn raw_identity(&self) -> io::Result<HostIdentity> {
        let stream = UnixStream::connect(self.server.socket()).await?;
        let endpoint = EndpointIdentity::capture(self.server.socket(), &stream)?;
        drop(stream);
        Ok(HostIdentity {
            kind: HostKind::Herdr,
            discovery_key: self.server.socket().display().to_string(),
            live_server_id: endpoint.live_server_id(BUNDLED_PROTOCOL, "0.8.2"),
        })
    }

    pub async fn requests(&self) -> Vec<Value> {
        self.server.requests().await
    }

    /// Waits until at least `minimum` production RPCs have reached the recording server.
    pub async fn wait_for_requests(&self, minimum: usize) {
        self.server.wait_for_requests(minimum).await;
    }

    /// Drops retained subscription streams without closing the listener, so the adapter's
    /// production monitor must reconnect through later scripted ping/probe/subscribe exchanges.
    pub fn lose_retained_subscriptions(&self) {
        self.server.close_retained_streams();
    }
}
