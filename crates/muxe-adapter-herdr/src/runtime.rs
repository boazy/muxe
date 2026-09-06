use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use muxe_adapter_api::{AdapterError, AdapterErrorKind, HostIdentity, HostKind};
use serde_json::{Map, Value};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
};

use crate::{
    ApiSchema, DeliveryState, HerdrCache, HerdrResponse, HerdrSocketClient, SocketError,
    generated::{BUNDLED_PROTOCOL, method_metadata},
    transport::EndpointIdentity,
};

/// Wall-clock bound for one `herdr api schema --json` invocation. Exceeding it fails
/// closed; the owned child is explicitly killed and reaped (see `runtime_schema`).
const SCHEMA_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound for reaping the owned schema child after killing it on timeout. SIGKILL
/// cannot be caught, so this only guards against a wedged reaper, not the child.
const REAP_TIMEOUT: Duration = Duration::from_secs(5);
/// Absolute cap on accepted schema stdout. Anything larger is rejected before parsing.
const MAX_SCHEMA_BYTES: u64 = 8 * 1024 * 1024;
/// Retained stderr bytes for failure diagnostics; the remainder is discarded.
const MAX_DIAGNOSTIC_BYTES: u64 = 8 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HerdrAdapterConfig {
    /// Explicit socket of the selected Herdr server. No default or ambient endpoint is used.
    pub socket_path: PathBuf,
    /// Exact installed Herdr executable used to obtain its runtime request schema.
    pub herdr_binary: PathBuf,
    /// Cache base; runtime schema records are stored beneath its `herdr` child.
    pub cache_dir: PathBuf,
}

pub struct HerdrRuntime {
    client: Arc<HerdrSocketClient>,
    schema: Arc<ApiSchema>,
    identity: HostIdentity,
    endpoint: EndpointIdentity,
}
impl HerdrRuntime {
    /// Acquires one runtime schema from the configured executable, verifies protocol compatibility,
    /// records its normalized cache key, and then probes the exact server behind `socket_path`,
    /// retaining its endpoint incarnation as the continuity boundary.
    pub async fn connect(config: HerdrAdapterConfig) -> Result<Self, AdapterError> {
        let raw_schema = runtime_schema(&config.herdr_binary).await?;
        let schema = ApiSchema::parse(raw_schema.clone()).map_err(|error| {
            incompatible(format!("installed Herdr API schema is invalid: {error}"))
        })?;
        if schema.protocol() != BUNDLED_PROTOCOL {
            return Err(incompatible(format!(
                "Herdr protocol {} is incompatible with required protocol {}",
                schema.protocol(),
                BUNDLED_PROTOCOL
            )));
        }
        HerdrCache::new(&config.cache_dir)
            .normalized_schema(schema.protocol(), schema.schema_version(), &raw_schema)
            .map_err(|error| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!("could not update Herdr runtime-schema cache: {error}"),
                )
            })?;

        let client = Arc::new(HerdrSocketClient::new(config.socket_path.clone()));
        let (identity, endpoint) = probe_endpoint_identity(&client).await?;
        Ok(Self {
            client,
            schema: Arc::new(schema),
            identity,
            endpoint,
        })
    }

    pub fn client(&self) -> &Arc<HerdrSocketClient> {
        &self.client
    }

    pub fn schema(&self) -> &Arc<ApiSchema> {
        &self.schema
    }

    pub fn identity(&self) -> &HostIdentity {
        &self.identity
    }

    /// The retained OS-visible incarnation of the server this runtime connected to.
    /// The broker compares fresh probes with [`EndpointIdentity::proven_replacement`]:
    /// a proved change fails closed host-bound dispatch and never reuses stale
    /// origins, leases, or pending cleanups against coincident IDs. An unchanged
    /// record proves nothing on its own (inode numbers may be recycled); the
    /// continuity authority is the retained subscription stream plus a new local
    /// epoch after any loss.
    pub fn endpoint(&self) -> &EndpointIdentity {
        &self.endpoint
    }
}

async fn runtime_schema(binary: &PathBuf) -> Result<Value, AdapterError> {
    let mut child = Command::new(binary)
        .arg("api")
        .arg("schema")
        .arg("--json")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // `kill_on_drop` is only a backstop: the timeout path below explicitly
        // kills and reaps exactly this owned child.
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!(
                    "could not execute {} api schema --json: {error}",
                    binary.display()
                ),
            )
        })?;
    let outcome = tokio::time::timeout(SCHEMA_TIMEOUT, collect_schema_output(&mut child)).await;
    let (status, stdout, diagnostics) = match outcome {
        Ok(collected) => collected?,
        Err(_) => {
            // Explicitly kill and reap exactly the owned child spawned above.
            // `start_kill` signals only this retained handle and `wait` reaps it,
            // so no zombie remains and no other process can be affected: there is
            // no name or PID search, no process-group signal, and no global
            // cleanup. `kill_on_drop` remains only as a backstop.
            let _ = child.start_kill();
            let _ = tokio::time::timeout(REAP_TIMEOUT, child.wait()).await;
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!(
                    "{} api schema --json timed out after {}s",
                    binary.display(),
                    SCHEMA_TIMEOUT.as_secs()
                ),
            ));
        }
    };
    if !status.success() {
        return Err(AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!(
                "{} api schema --json exited with {status}: {}",
                binary.display(),
                String::from_utf8_lossy(&diagnostics)
            ),
        ));
    }
    if stdout.len() as u64 > MAX_SCHEMA_BYTES {
        return Err(incompatible(format!(
            "{} api schema --json emitted {} bytes, above the {}-byte bound",
            binary.display(),
            stdout.len(),
            MAX_SCHEMA_BYTES
        )));
    }
    serde_json::from_slice(&stdout).map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Incompatible,
            format!(
                "{} api schema --json emitted invalid JSON: {error}",
                binary.display()
            ),
        )
    })
}

/// Reads bounded stdout/stderr from an already spawned schema child and waits for its
/// exit. Stdout beyond [`MAX_SCHEMA_BYTES`] is still fully consumed (so the child is
/// never left blocked on a full pipe) but rejected by the caller before parsing.
async fn collect_schema_output(
    child: &mut Child,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), AdapterError> {
    let mut stdout = child.stdout.take().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr schema child has no captured stdout",
        )
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr schema child has no captured stderr",
        )
    })?;
    let diagnostics = tokio::spawn(async move {
        let mut drained = Vec::new();
        let mut limited = (&mut stderr).take(MAX_DIAGNOSTIC_BYTES + 1);
        let _ = limited.read_to_end(&mut drained).await;
        drained
    });
    let mut schema = Vec::new();
    let mut limited = (&mut stdout).take(MAX_SCHEMA_BYTES + 1);
    limited.read_to_end(&mut schema).await.map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!("could not read Herdr schema stdout: {error}"),
        )
    })?;
    drop(limited);
    drop(stdout);
    let status = child.wait().await.map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!("could not reap Herdr schema child: {error}"),
        )
    })?;
    let diagnostics = diagnostics.await.unwrap_or_default();
    Ok((status, schema, diagnostics))
}

pub async fn probe_live_identity(client: &HerdrSocketClient) -> Result<HostIdentity, AdapterError> {
    probe_endpoint_identity(client).await.map(|(identity, _)| identity)
}

/// Probes the live server and returns its opaque identity together with the OS-visible
/// endpoint incarnation the identity is bound to. The broker retains the endpoint and
/// compares it on every health check: inequality proves server replacement, even when
/// the replacement reports the same protocol and version on the same socket path.
pub async fn probe_endpoint_identity(
    client: &HerdrSocketClient,
) -> Result<(HostIdentity, EndpointIdentity), AdapterError> {
    let metadata = method_metadata("ping")
        .ok_or_else(|| incompatible("bundled Herdr metadata does not declare ping"))?;
    let result = match client
        .unary(metadata, Value::Object(Map::new()))
        .await
        .map_err(socket_error)?
    {
        HerdrResponse::Success(result) => result,
        HerdrResponse::Error { code, message } => {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!("Herdr rejected ping with {code}: {message}"),
            ));
        }
    };
    let endpoint = client.probe_endpoint().await.map_err(socket_error)?;
    let identity = identity_from_ping_result(
        client.socket().display().to_string(),
        &endpoint,
        result,
    )?;
    Ok((identity, endpoint))
}

fn identity_from_ping_result(
    discovery_key: String,
    endpoint: &EndpointIdentity,
    result: Value,
) -> Result<HostIdentity, AdapterError> {
    let object = result
        .as_object()
        .ok_or_else(|| incompatible("Herdr ping result is not an object"))?;
    if object.get("type").and_then(Value::as_str) != Some("pong") {
        return Err(incompatible("Herdr ping result has unexpected type"));
    }
    let protocol = object
        .get("protocol")
        .and_then(Value::as_u64)
        .ok_or_else(|| incompatible("Herdr ping result lacks integer protocol"))?;
    if protocol != BUNDLED_PROTOCOL {
        return Err(incompatible(format!(
            "live Herdr protocol {protocol} is incompatible with required protocol {BUNDLED_PROTOCOL}"
        )));
    }
    let version = object
        .get("version")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| incompatible("Herdr ping result lacks nonempty version"))?;
    // Protocol 20 exposes no per-server identifier in a pong: only type, protocol,
    // and version. The server-selection boundary stays the explicitly configured
    // socket, but the identity is bound to the OS-visible endpoint incarnation
    // (socket-file device/inode plus best-effort peer credentials), so a replacement
    // server rebound to the same path never compares equal to the old one. The
    // resulting string is opaque: consumers only equality-compare it.
    Ok(HostIdentity {
        kind: HostKind::Herdr,
        discovery_key,
        live_server_id: endpoint.live_server_id(protocol, version),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn capture_at(path: &std::path::Path) -> EndpointIdentity {
        let listener = tokio::net::UnixListener::bind(path).unwrap();
        let stream = tokio::net::UnixStream::connect(path).await.unwrap();
        let endpoint = EndpointIdentity::capture(path, &stream).unwrap();
        drop(listener);
        endpoint
    }

    fn pong() -> Value {
        serde_json::json!({ "type": "pong", "protocol": BUNDLED_PROTOCOL, "version": "0.8.2" })
    }

    #[tokio::test]
    async fn pong_identity_binds_to_endpoint_incarnation() {
        let temp = tempfile::TempDir::new().unwrap();
        let endpoint = capture_at(&temp.path().join("herdr.sock")).await;
        let identity =
            identity_from_ping_result("/owned/socket".to_owned(), &endpoint, pong())
                .expect("protocol 20 pong has type, version, and protocol");

        assert_eq!(identity.discovery_key, "/owned/socket");
        assert_eq!(
            identity.live_server_id,
            endpoint.live_server_id(BUNDLED_PROTOCOL, "0.8.2")
        );
    }

    #[tokio::test]
    async fn rebound_server_never_matches_old_incarnation() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let first = capture_at(&path).await;
        std::fs::remove_file(&path).unwrap();
        let second = capture_at(&path).await;
        assert_ne!(
            first.live_server_id(BUNDLED_PROTOCOL, "0.8.2"),
            second.live_server_id(BUNDLED_PROTOCOL, "0.8.2"),
            "a replacement server on the same path must never reuse the old identity \
             even when it reports the same protocol and version"
        );
    }

    /// Exercises the production `HerdrRuntime::connect` path with only injected
    /// fixtures: a throwaway schema-command executable standing in for the exact
    /// installed Herdr binary, and a fake socket server answering `ping`. No real
    /// host binary, socket, or endpoint is ever touched.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_uses_injected_schema_child_and_fake_server() {
        use std::os::unix::fs::PermissionsExt;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

        let temp = tempfile::TempDir::new().unwrap();
        let script = temp.path().join("herdr");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s' '{\"protocol\":20,\"schema_version\":1,\"schemas\":{\"request\":{\"oneOf\":[{\"properties\":{\"method\":{\"const\":\"ping\"},\"params\":{}}}]}}}'",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();

        let socket = temp.path().join("herdr.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let server = tokio::spawn(async move {
            // The unary ping: read exactly one request line, answer it, then close
            // so the client observes the required end-of-stream.
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(stream);
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            let id = serde_json::from_slice::<Value>(&line).unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned();
            reader
                .write_all(
                    format!(
                        "{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\",\"protocol\":20,\"version\":\"0.8.2\"}}}}\n"
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            drop(reader);
            // The endpoint probe opens a second connection only for OS peer
            // credentials; accepting and closing it is the complete service.
            let (peer, _) = listener.accept().await.unwrap();
            drop(peer);
        });

        let cache_dir = temp.path().join("cache");
        let runtime = HerdrRuntime::connect(HerdrAdapterConfig {
            socket_path: socket.clone(),
            herdr_binary: script,
            cache_dir,
        })
        .await
        .expect("injected fixtures must satisfy the production connect path");
        server.await.unwrap();

        assert_eq!(runtime.identity().discovery_key, socket.display().to_string());
        assert_eq!(
            runtime.identity().live_server_id,
            runtime.endpoint().live_server_id(20, "0.8.2"),
            "identity must be bound to the retained endpoint incarnation"
        );
        assert_eq!(runtime.schema().protocol(), BUNDLED_PROTOCOL);
        assert!(
            runtime.endpoint().recheck(),
            "the fake server outlives connect, so the stat gate must hold"
        );
    }
}

fn socket_error(error: SocketError) -> AdapterError {
    AdapterError::new(
        if error.delivery() == DeliveryState::MayHaveReachedHost {
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
