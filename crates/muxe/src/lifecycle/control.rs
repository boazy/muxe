//! Coordinator-side client for the stable `control-json-v1` protocol.
//!
//! The broker owns server-side control serving; this module owns the
//! coordinator end. A connection sends the endian-stable prelude with codec
//! `control-json-v1` and peer role `activation-coordinator`, then exchanges
//! four-byte big-endian length-prefixed JSON frames capped at 64 KiB. Unknown
//! fields are ignored for additive evolution; the typed record governs the
//! connection, so the schema fingerprint field stays zero.
//!
//! Request IDs are unique 128-bit nonces; responses must echo the request ID.
//! A broker `Error` result, an ID mismatch, a malformed frame, or a timeout
//! fails the operation without any coordinator-side mutation.

use std::{
    fs::File,
    io::Read,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use muxe_protocol::{
    MAX_CONTROL_FRAME_LEN,
    control::{
        ActivationStatus, CompatibilityRecord, ControlDecoder, ControlMessage, ControlOperation,
        ControlPolicy, ControlRequest, ControlRequestId, ControlResponse, ControlResult, HandoffId,
    },
    frame::Prelude,
    wire::PeerRole,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::{Duration, timeout},
};

/// Per-operation coordinator timeout. Coarse hang detection, not a benchmark.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("control connection to {} failed: {source}", socket.display())]
    Connect {
        socket: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("control IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("control frame decode failed: {0}")]
    Decode(#[from] muxe_protocol::control::ControlDecodeError),
    #[error("broker closed the control connection")]
    Closed,
    #[error("broker rejected the operation: {diagnostic}")]
    Rejected { diagnostic: String },
    #[error("broker response carried a mismatched request ID")]
    IdMismatch,
    #[error("broker sent an unexpected {0} result")]
    UnexpectedResult(&'static str),
    #[error("control operation timed out")]
    Timeout,
}

/// Generates a unique 128-bit request nonce.
///
/// Reads from the OS entropy source; if it is unavailable, mixes an atomic
/// counter with the process ID and nanosecond time. Coordinator request IDs
/// need uniqueness (duplicates are rejected), not secrecy.
fn new_request_id(counter: &AtomicU64) -> ControlRequestId {
    let mut bytes = [0u8; 16];
    if let Ok(mut entropy) = File::open("/dev/urandom") {
        if entropy.read_exact(&mut bytes).is_ok() && bytes != [0; 16] {
            return ControlRequestId(bytes);
        }
    }
    let count = counter.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    bytes[..8].copy_from_slice(&nanos.to_be_bytes()[..8]);
    bytes[8..12].copy_from_slice(&std::process::id().to_be_bytes());
    bytes[12..].copy_from_slice(&count.to_be_bytes()[..4]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    ControlRequestId(bytes)
}

/// Decodes a 32-hex-character handoff ID from journal form.
pub fn handoff_from_hex(hex: &str) -> Result<HandoffId, ControlError> {
    if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ControlError::Rejected {
            diagnostic: "malformed handoff ID".to_owned(),
        });
    }
    let mut raw = [0u8; 16];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| ControlError::Rejected {
            diagnostic: "malformed handoff ID".to_owned(),
        })?;
        raw[index] = u8::from_str_radix(text, 16).map_err(|_| ControlError::Rejected {
            diagnostic: "malformed handoff ID".to_owned(),
        })?;
    }
    Ok(HandoffId(raw))
}

/// Coordinator control connection to one broker.
pub struct ControlClient {
    stream: UnixStream,
    decoder: ControlDecoder,
    counter: AtomicU64,
}

impl ControlClient {
    /// Connects to a broker control socket and sends the coordinator prelude.
    pub async fn connect(socket: &Path) -> Result<Self, ControlError> {
        let mut stream = UnixStream::connect(socket).await.map_err(|source| {
            ControlError::Connect {
                socket: socket.to_path_buf(),
                source,
            }
        })?;
        let prelude = Prelude::control(PeerRole::ActivationCoordinator).encode();
        stream.write_all(&prelude).await?;
        stream.flush().await?;
        Ok(Self {
            stream,
            decoder: ControlDecoder::new(ControlPolicy::coordinator()),
            counter: AtomicU64::new(1),
        })
    }

    /// Sends `status` and returns the broker's activation status.
    pub async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
        match self.round_trip(ControlOperation::Status).await? {
            ControlResult::Status(status) => Ok(status),
            _ => Err(ControlError::UnexpectedResult("non-status")),
        }
    }

    /// Sends `prepare` with the target compatibility record.
    pub async fn prepare(
        &mut self,
        target: CompatibilityRecord,
    ) -> Result<ActivationStatus, ControlError> {
        match self
            .round_trip(ControlOperation::Prepare { target })
            .await?
        {
            ControlResult::Prepared(status) => Ok(status),
            _ => Err(ControlError::UnexpectedResult("non-prepared")),
        }
    }

    /// Sends `commit` for a handoff ID from the journal.
    pub async fn commit(&mut self, handoff_id: HandoffId) -> Result<ActivationStatus, ControlError> {
        match self.round_trip(ControlOperation::Commit { handoff_id }).await? {
            ControlResult::Committed(status) => Ok(status),
            _ => Err(ControlError::UnexpectedResult("non-committed")),
        }
    }

    /// Sends `abort` for a handoff ID from the journal.
    pub async fn abort(&mut self, handoff_id: HandoffId) -> Result<ActivationStatus, ControlError> {
        match self.round_trip(ControlOperation::Abort { handoff_id }).await? {
            ControlResult::Aborted(status) => Ok(status),
            _ => Err(ControlError::UnexpectedResult("non-aborted")),
        }
    }

    /// Sends `retire` to drain a broker without a replacement.
    pub async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
        match self.round_trip(ControlOperation::Retire).await? {
            ControlResult::Retired(status) => Ok(status),
            _ => Err(ControlError::UnexpectedResult("non-retired")),
        }
    }

    async fn round_trip(
        &mut self,
        operation: ControlOperation,
    ) -> Result<ControlResult, ControlError> {
        let request_id = new_request_id(&self.counter);
        let message = ControlMessage::Request(ControlRequest {
            request_id,
            operation,
        });
        let payload = serde_json::to_vec(&message).map_err(|_| ControlError::Rejected {
            diagnostic: "cannot encode control request".to_owned(),
        })?;
        if payload.len() > MAX_CONTROL_FRAME_LEN as usize {
            return Err(ControlError::Rejected {
                diagnostic: "control request exceeds frame cap".to_owned(),
            });
        }
        timeout(OPERATION_TIMEOUT, async {
            self.stream
                .write_all(&(payload.len() as u32).to_be_bytes())
                .await?;
            self.stream.write_all(&payload).await?;
            self.stream.flush().await?;
            loop {
                let mut chunk = [0u8; 4096];
                let read = self.stream.read(&mut chunk).await?;
                if read == 0 {
                    return Err(ControlError::Closed);
                }
                let mut response: Option<ControlResponse> = None;
                self.decoder.push(&chunk[..read], |message| {
                    if let ControlMessage::Response(candidate) = message {
                        response = Some(candidate);
                    }
                })?;
                if let Some(candidate) = response {
                    if candidate.request_id != request_id {
                        return Err(ControlError::IdMismatch);
                    }
                    return match candidate.result {
                        ControlResult::Error { diagnostic } => {
                            Err(ControlError::Rejected { diagnostic })
                        }
                        result => Ok(result),
                    };
                }
            }
        })
        .await
        .map_err(|_| ControlError::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_protocol::control::LifecycleState;
    use muxe_protocol::wire::{HostKind, LiveServerIdentity, ServerId};
    use tokio::net::UnixListener;
    fn test_status() -> ActivationStatus {
        ActivationStatus {
            lifecycle: LifecycleState::Running,
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "server".to_owned(),
                server_id: ServerId::new("id"),
            },
            current: current_record(),
            target: None,
            handoff_id: None,
        }
    }
    fn current_record() -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: "0.1.0".to_owned(),
            target_triple: "test".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        }
    }

    /// Waits until a test server has bound its socket.
    async fn wait_for_socket(socket: &Path) {
        for _ in 0..200 {
            if socket.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("test server never bound {}", socket.display());
    }

    /// Minimal test control server speaking the real framing: broker prelude,
    /// length-prefixed JSON, typed operations. Stands in for the broker-owned
    /// server while it lands; the wire contract is the shared protocol crate.
    async fn serve_once(socket: &Path, handle: impl Fn(ControlOperation) -> ControlResult) {
        use muxe_protocol::control::ControlPolicy as Policy;
        let listener = UnixListener::bind(socket).unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        // The broker speaks first: its prelude identifies the peer role for
        // the coordinator's decoder before any frame flows.
        {
            use tokio::io::AsyncWriteExt;
            let prelude =
                muxe_protocol::frame::Prelude::control(muxe_protocol::wire::PeerRole::Broker)
                    .encode();
            stream.write_all(&prelude).await.unwrap();
            stream.flush().await.unwrap();
        }
        let mut decoder = ControlDecoder::new(Policy::broker());
        let mut buffer = [0u8; 8192];
        let mut pending: Vec<ControlRequest> = Vec::new();
        // Read until one full request arrives.
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0);
            decoder
                .push(&buffer[..read], |message| {
                    if let ControlMessage::Request(request) = message {
                        pending.push(request);
                    }
                })
                .unwrap();
            if !pending.is_empty() {
                break;
            }
        }
        let request = pending.remove(0);
        let result = handle(request.operation);
        let response = ControlMessage::Response(ControlResponse {
            request_id: request.request_id,
            result,
        });
        let payload = serde_json::to_vec(&response).unwrap();
        stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();
        // Hold the connection briefly so the client reads before close.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn status_round_trip_over_real_framing() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let socket = temp.path().join("control.sock");
        // Broker prelude check: the server validates our coordinator prelude
        // through the shared decoder before answering.
        let server = tokio::spawn({
            let socket = socket.clone();
            async move {
                serve_once(&socket, |operation| {
                    assert_eq!(operation, ControlOperation::Status);
                    ControlResult::Status(test_status())
                })
                .await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        let status = client.status().await.unwrap();
        assert_eq!(status.lifecycle, LifecycleState::Running);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn broker_error_maps_to_rejection() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let socket = temp.path().join("control.sock");
        let server = tokio::spawn({
            let socket = socket.clone();
            async move {
                serve_once(&socket, |_| ControlResult::Error {
                    diagnostic: "activation_in_progress".to_owned(),
                })
                .await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        let error = client.status().await.unwrap_err();
        assert!(matches!(error, ControlError::Rejected { .. }));
        assert!(error.to_string().contains("activation_in_progress"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mismatched_result_kind_is_unexpected() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let socket = temp.path().join("control.sock");
        let server = tokio::spawn({
            let socket = socket.clone();
            async move {
                serve_once(&socket, |_| ControlResult::Retired(test_status())).await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        assert!(matches!(
            client.status().await,
            Err(ControlError::UnexpectedResult(_))
        ));
        server.await.unwrap();
    }

    #[test]
    fn handoff_hex_round_trip() {
        let id = handoff_from_hex(&"ab".repeat(16)).unwrap();
        assert_eq!(id.0, [0xab; 16]);
        assert!(handoff_from_hex("short").is_err());
    }

    #[test]
    fn request_ids_are_unique() {
        let counter = AtomicU64::new(1);
        let first = new_request_id(&counter);
        let second = new_request_id(&counter);
        assert_ne!(first, second);
        assert_ne!(first.0, [0; 16]);
    }
}
