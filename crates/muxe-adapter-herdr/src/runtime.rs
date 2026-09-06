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
    schema_cache_hit: bool,
    identity: HostIdentity,
    endpoint: EndpointIdentity,
}
impl HerdrRuntime {
    /// Acquires one runtime schema from the configured executable, verifies protocol compatibility,
    /// records its normalized cache key, and probes the exact server currently accepting at
    /// `socket_path`. The resulting OS observation is not continuity authority.
    pub async fn connect(config: HerdrAdapterConfig) -> Result<Self, AdapterError> {
        let raw_schema = runtime_schema(&config.herdr_binary).await?;
        let (protocol, schema_version) = ApiSchema::metadata(&raw_schema).map_err(|error| {
            incompatible(format!("installed Herdr API schema is invalid: {error}"))
        })?;
        if protocol != BUNDLED_PROTOCOL {
            return Err(incompatible(format!(
                "Herdr protocol {protocol} is incompatible with required protocol {BUNDLED_PROTOCOL}"
            )));
        }
        let (normalized_request, schema_cache_hit) = HerdrCache::new(&config.cache_dir)
            .normalized_schema(protocol, schema_version, &raw_schema)
            .map_err(|error| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!("could not update Herdr runtime-schema cache: {error}"),
                )
            })?;
        let normalized_request = serde_json::from_slice(&normalized_request).map_err(|error| {
            incompatible(format!(
                "Herdr runtime-schema cache returned an invalid normalized request representation: {error}"
            ))
        })?;
        let schema = ApiSchema::parse_with_request(raw_schema, &normalized_request).map_err(
            |error| incompatible(format!("installed Herdr API schema is invalid: {error}")),
        )?;

        let client = Arc::new(HerdrSocketClient::new(config.socket_path.clone()));
        let (identity, endpoint) = probe_endpoint_identity(&client).await?;
        Ok(Self {
            client,
            schema: Arc::new(schema),
            schema_cache_hit,
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

    /// Whether this connection parsed the cache-verified normalized request representation rather
    /// than freshly canonicalizing the same request surface.
    pub fn used_cached_schema_representation(&self) -> bool {
        self.schema_cache_hit
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
    runtime_schema_with_timeouts(binary, SCHEMA_TIMEOUT, REAP_TIMEOUT).await
}

async fn runtime_schema_with_timeouts(
    binary: &PathBuf,
    schema_timeout: Duration,
    reap_timeout: Duration,
) -> Result<Value, AdapterError> {
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
    let outcome = tokio::time::timeout(schema_timeout, collect_schema_output(&mut child)).await;
    let (status, stdout, diagnostics) = match outcome {
        Ok(collected) => collected?,
        Err(_) => {
            // Explicitly signal and then reap exactly this retained child handle. There is no
            // name or PID search, no process-group signal, and no global cleanup.
            // `kill_on_drop` remains only as a backstop. A failed kill or reap is surfaced
            // instead of pretending the owned child is gone.
            child.start_kill().map_err(|error| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!("could not kill timed-out Herdr schema child: {error}"),
                )
            })?;
            match tokio::time::timeout(reap_timeout, child.wait()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Unavailable,
                        format!("could not reap timed-out Herdr schema child: {error}"),
                    ));
                }
                Err(_) => {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Unavailable,
                        format!(
                            "timed out after {}s while reaping the owned Herdr schema child",
                            reap_timeout.as_secs()
                        ),
                    ));
                }
            }
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!(
                    "{} api schema --json timed out after {}s",
                    binary.display(),
                    schema_timeout.as_secs()
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

/// Probes the live server and returns its opaque host identity together with an OS-visible
/// endpoint observation. An observed inequality can prove replacement even when the
/// replacement reports the same protocol and version on the same socket path. Equality
/// never proves continuity; the retained subscription stream and local epoch do.
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
    // and version. The configured socket identifies the selected host, while the
    // OS-visible endpoint observation contributes an opaque shared host identity.
    // Its equality cannot establish continuity because POSIX can recycle inode and
    // process identifiers; consumers must not use it as a continuity token.
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
    async fn identical_endpoint_observation_is_not_a_continuity_claim() {
        let temp = tempfile::TempDir::new().unwrap();
        let endpoint = capture_at(&temp.path().join("herdr.sock")).await;

        assert!(
            !endpoint.proven_replacement(&endpoint),
            "an equal observation is deliberately inconclusive, not continuity proof"
        );
    }

    /// The timeout path owns one concrete nonterminating schema child, kills it, and reaps it
    /// before returning. The short injected bounds exercise the production algorithm without a
    /// ten-second wall-clock test.
    #[cfg(unix)]
    #[tokio::test]
    async fn schema_timeout_kills_and_reaps_the_owned_child() {
        use nix::{
            errno::Errno,
            sys::wait::{WaitPidFlag, waitpid},
            unistd::Pid,
        };
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::TempDir::new().unwrap();
        let script = temp.path().join("herdr");
        let pid_file = temp.path().join("pid");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s' \"$$\" > \"$(dirname \"$0\")/pid\"\nexec sleep 3600\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let worker = tokio::spawn({
            let script = script.clone();
            async move {
                runtime_schema_with_timeouts(
                    &script,
                    Duration::from_millis(50),
                    Duration::from_secs(1),
                )
                .await
            }
        });
        let mut child_pid = None;
        for _ in 0..100 {
            if let Ok(pid) = std::fs::read_to_string(&pid_file)
                && let Ok(pid) = pid.trim().parse::<i32>()
            {
                child_pid = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let pid = Pid::from_raw(
            child_pid.expect("schema child writes its PID marker before its timeout"),
        );

        let error = worker
            .await
            .expect("schema worker task does not panic")
            .expect_err("a nonterminating schema child must time out");
        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
        assert!(
            error.message.contains("api schema --json timed out"),
            "the timeout must only return after the owned child is reaped"
        );
        assert_eq!(
            waitpid(pid, Some(WaitPidFlag::WNOHANG)),
            Err(Errno::ECHILD),
            "the owned schema child must be reaped rather than left as a zombie"
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
