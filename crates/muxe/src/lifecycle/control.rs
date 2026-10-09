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
//! Encoding and frame-limit failures occur before any request bytes are sent.
//! Only a received broker `Error` result is a remote rejection. A response type
//! mismatch and missing readiness proof are separate failures; neither proves
//! that the broker left its state unchanged.

use std::{
    fs::{self, File},
    io::Read,
    num::NonZeroU32,
    os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
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
use nix::unistd::Uid;
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
    #[error("The broker rejected {operation}: {diagnostic}")]
    Rejected {
        operation: &'static str,
        diagnostic: String,
        prepare_refusal: Option<muxe_protocol::control::PrepareRefusalEvidence>,
    },
    #[error("Invalid handoff ID: expected 32 hexadecimal characters")]
    InvalidHandoffId,
    #[error("Muxe could not encode the control request. The request was not sent: {0}")]
    Encode(#[source] serde_json::Error),
    #[error("The control request exceeds the 64 KiB frame limit. The request was not sent")]
    RequestTooLarge,
    #[error("This control peer does not support the required StatusAt readiness proof")]
    UnsupportedStatusAt,
    #[error("broker response carried a mismatched request ID")]
    IdMismatch,
    #[error("The broker did not return the expected {expected} response. Received: {actual}")]
    UnexpectedResult {
        expected: ControlResultKind,
        actual: ControlResultKind,
    },
    #[error(
        "The broker's StatusAt response does not contain the requested handoff ID and readiness proof epoch"
    )]
    ReadinessProofMismatch {
        expected_handoff: HandoffId,
        actual_handoff: Option<HandoffId>,
        expected_epoch: muxe_protocol::UnitReadinessEpochId,
        actual_epoch: Option<muxe_protocol::UnitReadinessEpochId>,
    },
    #[error("control operation timed out")]
    Timeout,
    #[error("broker control peer has wrong UID or lacks a process identity")]
    PeerIdentity,
    #[error("broker control endpoint is not an owner-only socket: {}", .0.display())]
    InvalidEndpoint(PathBuf),
    #[error("broker control endpoint changed during verification: {}", .0.display())]
    EndpointReplaced(PathBuf),
}
impl ControlError {
    /// Returns whether connecting failed because the endpoint path is absent.
    ///
    /// A refusal, close, or any failure after connecting does not prove that
    /// the broker is gone: another listener may still own the endpoint.
    pub(crate) fn is_absent_endpoint(&self) -> bool {
        matches!(
            self,
            Self::Connect { source, .. }
                if source.kind() == std::io::ErrorKind::NotFound
        )
    }
}

/// The response variant observed on the control connection, without its payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlResultKind {
    Status,
    StatusAt,
    Prepared,
    Committed,
    Aborted,
    Retired,
    Error,
}

impl ControlResultKind {
    fn of(result: &ControlResult) -> Self {
        match result {
            ControlResult::Status(_) => Self::Status,
            ControlResult::StatusAt(_) => Self::StatusAt,
            ControlResult::Prepared(_) => Self::Prepared,
            ControlResult::Committed(_) => Self::Committed,
            ControlResult::Aborted(_) => Self::Aborted,
            ControlResult::Retired(_) => Self::Retired,
            ControlResult::Error { .. } => Self::Error,
        }
    }

    fn unexpected(self, result: &ControlResult) -> ControlError {
        ControlError::UnexpectedResult {
            expected: self,
            actual: Self::of(result),
        }
    }
}

impl std::fmt::Display for ControlResultKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Status => "Status",
            Self::StatusAt => "StatusAt",
            Self::Prepared => "Prepared",
            Self::Committed => "Committed",
            Self::Aborted => "Aborted",
            Self::Retired => "Retired",
            Self::Error => "Error",
        })
    }
}

fn operation_name(operation: &ControlOperation) -> &'static str {
    match operation {
        ControlOperation::Status => "Status",
        ControlOperation::StatusAt { .. } => "StatusAt",
        ControlOperation::Prepare { .. } => "Prepare",
        ControlOperation::Commit { .. } => "Commit",
        ControlOperation::Abort { .. } => "Abort",
        ControlOperation::Retire => "Retire",
    }
}

/// Encodes and bounds a request before any bytes are written to the peer.
fn encode_request(message: &impl serde::Serialize) -> Result<Vec<u8>, ControlError> {
    let payload = serde_json::to_vec(message).map_err(ControlError::Encode)?;
    if payload.len() > MAX_CONTROL_FRAME_LEN as usize {
        return Err(ControlError::RequestTooLarge);
    }
    Ok(payload)
}

/// Generates a unique 128-bit request nonce.
///
/// Reads from the OS entropy source; if it is unavailable, mixes an atomic
/// counter with the process ID and nanosecond time. Coordinator request IDs
/// need uniqueness (duplicates are rejected), not secrecy.
fn new_request_id(counter: &AtomicU64) -> ControlRequestId {
    let mut bytes = [0u8; 16];
    if let Ok(mut entropy) = File::open("/dev/urandom")
        && entropy.read_exact(&mut bytes).is_ok()
        && bytes != [0; 16]
    {
        return ControlRequestId(bytes);
    }
    let count = counter.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    bytes[..8].copy_from_slice(&nanos.to_be_bytes()[..8]);
    bytes[8..12].copy_from_slice(&std::process::id().to_be_bytes());
    bytes[12..].copy_from_slice(&count.to_be_bytes()[..4]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    ControlRequestId(bytes)
}

/// Decodes a 32-hex-character handoff ID from journal form.
///
/// # Errors
///
/// Returns [`ControlError::InvalidHandoffId`] when `hex` is not 32 hexadecimal characters.
pub fn handoff_from_hex(hex: &str) -> Result<HandoffId, ControlError> {
    if hex.len() != 32 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ControlError::InvalidHandoffId);
    }
    let mut raw = [0u8; 16];
    for (index, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| ControlError::InvalidHandoffId)?;
        raw[index] = u8::from_str_radix(text, 16).map_err(|_| ControlError::InvalidHandoffId)?;
    }
    Ok(HandoffId(raw))
}

/// Process identity authenticated from the retained control stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrokerProcessId(NonZeroU32);

impl BrokerProcessId {
    #[must_use]
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// Nofollow filesystem identity of the endpoint observed by one control stream.
#[derive(Clone, Debug, Eq, PartialEq)]
struct EndpointIdentity {
    path: PathBuf,
    device: u64,
    inode: u64,
    owner: u32,
    mode: u32,
}

impl EndpointIdentity {
    fn inspect(path: &Path) -> Result<Self, ControlError> {
        let metadata = fs::symlink_metadata(path).map_err(|source| ControlError::Connect {
            socket: path.to_path_buf(),
            source,
        })?;
        let mode = metadata.permissions().mode() & 0o777;
        if !metadata.file_type().is_socket()
            || metadata.uid() != Uid::current().as_raw()
            || mode != 0o600
        {
            return Err(ControlError::InvalidEndpoint(path.to_path_buf()));
        }
        Ok(Self {
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
            owner: metadata.uid(),
            mode,
        })
    }
}

/// Authority bound to one retained authenticated control connection.
#[derive(Clone, Debug)]
pub struct ControlAuthority {
    process: BrokerProcessId,
    endpoint: EndpointIdentity,
}

impl ControlAuthority {
    #[must_use]
    pub fn process(&self) -> BrokerProcessId {
        self.process
    }

    #[must_use]
    pub fn endpoint(&self) -> &Path {
        &self.endpoint.path
    }

    /// Rechecks the same owner-only socket inode before registry mutation.
    ///
    /// # Errors
    ///
    /// Any replacement, symlink, disappearance, or owner/mode drift fails closed.
    pub fn verify_path(&self) -> Result<(), ControlError> {
        match EndpointIdentity::inspect(&self.endpoint.path) {
            Ok(current) if current == self.endpoint => Ok(()),
            Ok(_) | Err(_) => Err(ControlError::EndpointReplaced(self.endpoint.path.clone())),
        }
    }
}

/// Status plus same-stream peer and path attestation.
#[derive(Clone, Debug)]
pub struct VerifiedControlStatus {
    pub status: ActivationStatus,
    pub authority: ControlAuthority,
}

/// Coordinator control connection to one broker.
pub struct ControlClient {
    stream: UnixStream,
    decoder: ControlDecoder,
    authority: ControlAuthority,
    counter: AtomicU64,
}

impl ControlClient {
    /// Connects to a broker control socket and sends the coordinator prelude.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError::Connect`] when the socket cannot be reached, or [`ControlError::Io`] when the prelude write fails.
    pub async fn connect(socket: &Path) -> Result<Self, ControlError> {
        let endpoint = EndpointIdentity::inspect(socket)?;
        let mut stream =
            UnixStream::connect(socket)
                .await
                .map_err(|source| ControlError::Connect {
                    socket: socket.to_path_buf(),
                    source,
                })?;
        let credentials = stream.peer_cred().map_err(|_| ControlError::PeerIdentity)?;
        if credentials.uid() != Uid::current().as_raw() {
            return Err(ControlError::PeerIdentity);
        }
        let process = credentials
            .pid()
            .and_then(|pid| u32::try_from(pid).ok())
            .and_then(NonZeroU32::new)
            .map(BrokerProcessId)
            .ok_or(ControlError::PeerIdentity)?;
        if EndpointIdentity::inspect(socket)? != endpoint {
            return Err(ControlError::EndpointReplaced(socket.to_path_buf()));
        }
        let prelude = Prelude::control(PeerRole::ActivationCoordinator).encode();
        stream.write_all(&prelude).await?;
        stream.flush().await?;
        Ok(Self {
            stream,
            decoder: ControlDecoder::new(ControlPolicy::coordinator()),
            counter: AtomicU64::new(1),
            authority: ControlAuthority { process, endpoint },
        })
    }

    /// Sends `status` and returns the broker's activation status.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError`] when the broker reports an error, closes, mismatches the request, or the operation times out.
    pub async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
        match self.round_trip(ControlOperation::Status).await? {
            ControlResult::Status(status) => Ok(status),
            result => Err(ControlResultKind::Status.unexpected(&result)),
        }
    }

    /// Returns status and peer/path authority observed on this same stream.
    ///
    /// # Errors
    ///
    /// Rejects an endpoint rebind after the response.
    pub async fn verified_status(&mut self) -> Result<VerifiedControlStatus, ControlError> {
        let status = self.status().await?;
        self.authority.verify_path()?;
        Ok(VerifiedControlStatus {
            status,
            authority: self.authority.clone(),
        })
    }

    /// Requests readiness at one coordinator-owned monotonic unit epoch.
    ///
    /// # Errors
    ///
    /// Rejects peers that lack `StatusAt` or omit the exact proof epoch.
    pub async fn status_at(
        &mut self,
        handoff_id: HandoffId,
        epoch: muxe_protocol::UnitReadinessEpochId,
        as_of: muxe_protocol::AsOfTick,
    ) -> Result<ActivationStatus, ControlError> {
        match self
            .round_trip(ControlOperation::StatusAt {
                handoff_id,
                epoch,
                as_of,
            })
            .await?
        {
            ControlResult::StatusAt(status)
                if status.handoff_id == Some(handoff_id)
                    && status
                        .ready
                        .as_ref()
                        .is_some_and(|ready| ready.proof_epoch == Some(epoch)) =>
            {
                Ok(status)
            }
            ControlResult::StatusAt(status) => Err(ControlError::ReadinessProofMismatch {
                expected_handoff: handoff_id,
                actual_handoff: status.handoff_id,
                expected_epoch: epoch,
                actual_epoch: status.ready.and_then(|ready| ready.proof_epoch),
            }),
            result => Err(ControlResultKind::StatusAt.unexpected(&result)),
        }
    }

    /// Sends `prepare` with the target compatibility record.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError`] when the broker reports an error, closes, mismatches the request, or the operation times out.
    pub async fn prepare(
        &mut self,
        target: CompatibilityRecord,
        handoff_id: HandoffId,
    ) -> Result<ActivationStatus, ControlError> {
        match self
            .round_trip(ControlOperation::Prepare {
                target: Box::new(target),
                handoff_id,
            })
            .await?
        {
            ControlResult::Prepared(status) => Ok(status),
            result => Err(ControlResultKind::Prepared.unexpected(&result)),
        }
    }

    /// Sends `commit` for a handoff ID from the journal.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError`] when the broker reports an error, closes, mismatches the request, or the operation times out.
    pub async fn commit(
        &mut self,
        handoff_id: HandoffId,
    ) -> Result<ActivationStatus, ControlError> {
        match self
            .round_trip(ControlOperation::Commit { handoff_id })
            .await?
        {
            ControlResult::Committed(status) => Ok(status),
            result => Err(ControlResultKind::Committed.unexpected(&result)),
        }
    }

    /// Sends `abort` for a handoff ID from the journal.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError`] when the broker reports an error, closes, mismatches the request, or the operation times out.
    pub async fn abort(&mut self, handoff_id: HandoffId) -> Result<ActivationStatus, ControlError> {
        match self
            .round_trip(ControlOperation::Abort { handoff_id })
            .await?
        {
            ControlResult::Aborted(status) => Ok(status),
            result => Err(ControlResultKind::Aborted.unexpected(&result)),
        }
    }

    /// Sends `retire` to drain a broker without a replacement.
    ///
    /// # Errors
    ///
    /// Returns [`ControlError`] when the broker reports an error, closes, mismatches the request, or the operation times out.
    pub async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
        match self.round_trip(ControlOperation::Retire).await? {
            ControlResult::Retired(status) => Ok(status),
            result => Err(ControlResultKind::Retired.unexpected(&result)),
        }
    }

    async fn round_trip(
        &mut self,
        operation: ControlOperation,
    ) -> Result<ControlResult, ControlError> {
        let request_id = new_request_id(&self.counter);
        let operation_name = operation_name(&operation);
        let message = ControlMessage::Request(ControlRequest {
            request_id,
            operation,
        });
        let payload = encode_request(&message)?;
        let len = u32::try_from(payload.len()).map_err(|_| ControlError::RequestTooLarge)?;
        timeout(OPERATION_TIMEOUT, async {
            self.stream.write_all(&len.to_be_bytes()).await?;
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
                        ControlResult::Error {
                            diagnostic,
                            prepare_refusal,
                        } => Err(ControlError::Rejected {
                            operation: operation_name,
                            diagnostic,
                            prepare_refusal,
                        }),
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
            phase: muxe_protocol::control::ActivationPhase::Ordinary,
            registration: None,
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "server".to_owned(),
                server_id: ServerId::new("id"),
            },
            current: current_record(),
            target: None,
            handoff_id: None,
            bridge_unit: None,
            ready: None,
            prepare_handoff: Some(
                muxe_protocol::control::PrepareHandoffProtocol::CoordinatorSuppliedV1,
            ),
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
            if std::fs::symlink_metadata(socket)
                .is_ok_and(|metadata| metadata.permissions().mode() & 0o777 == 0o600)
            {
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
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)).unwrap();
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
        let len = u32::try_from(payload.len()).unwrap();
        stream.write_all(&len.to_be_bytes()).await.unwrap();
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();
        // Hold the connection briefly so the client reads before close.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn status_round_trip_over_real_framing() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
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
        let verified = client.verified_status().await.unwrap();
        assert_eq!(verified.status.lifecycle, LifecycleState::Running);
        assert_eq!(verified.authority.process().get(), std::process::id());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn same_stream_status_rejects_socket_rebind_before_registry_use() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = temp.path().join("control.sock");
        let server = tokio::spawn({
            let socket = socket.clone();
            async move {
                let replacement = socket.clone();
                serve_once(&socket, move |operation| {
                    assert_eq!(operation, ControlOperation::Status);
                    std::fs::remove_file(&replacement).unwrap();
                    let rebound = UnixListener::bind(&replacement).unwrap();
                    std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    drop(rebound);
                    ControlResult::Status(test_status())
                })
                .await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        assert!(matches!(
            client.verified_status().await,
            Err(ControlError::EndpointReplaced(path)) if path == socket
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn status_at_requires_exact_epoch_echo_from_peer() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let handoff = HandoffId([3; 16]);
        let epoch = muxe_protocol::UnitReadinessEpochId::from_bytes([4; 16]).unwrap();
        let as_of = muxe_protocol::AsOfTick::from_millis(100).unwrap();
        let socket = temp.path().join("legacy.sock");
        let legacy = tokio::spawn({
            let socket = socket.clone();
            async move {
                serve_once(&socket, |operation| {
                    assert!(matches!(operation, ControlOperation::StatusAt { .. }));
                    ControlResult::Status(test_status())
                })
                .await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        assert!(matches!(
            client.status_at(handoff, epoch, as_of).await,
            Err(ControlError::UnexpectedResult {
                expected: ControlResultKind::StatusAt,
                actual: ControlResultKind::Status,
            })
        ));
        legacy.await.unwrap();

        let socket = temp.path().join("certified.sock");
        let certified = tokio::spawn({
            let socket = socket.clone();
            async move {
                serve_once(&socket, |operation| {
                    assert!(matches!(
                        operation,
                        ControlOperation::StatusAt {
                            handoff_id,
                            epoch: received,
                            as_of: received_tick,
                        } if handoff_id == handoff && received == epoch && received_tick == as_of
                    ));
                    let mut status = test_status();
                    status.phase = muxe_protocol::control::ActivationPhase::TargetGated;
                    status.handoff_id = Some(handoff);
                    status.ready = Some(muxe_protocol::TargetReadiness {
                        registered_clients: Vec::new(),
                        member_clients: 0,
                        member_ids: Some(Vec::new()),
                        proof_epoch: Some(epoch),
                    });
                    ControlResult::StatusAt(status)
                })
                .await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        assert_eq!(
            client
                .status_at(handoff, epoch, as_of)
                .await
                .unwrap()
                .ready
                .unwrap()
                .proof_epoch,
            Some(epoch)
        );
        certified.await.unwrap();
    }

    #[tokio::test]
    async fn owned_control_peer_hanging_before_broker_prelude_obeys_proof_deadline() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let socket = temp.path().join("silent.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let (accepted, entered) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = accepted.send(());
            let _stream = stream;
            std::future::pending::<()>().await;
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(80);
        let result = tokio::time::timeout_at(deadline, async {
            let mut client = ControlClient::connect(&socket).await?;
            entered.await.unwrap();
            client
                .status_at(
                    HandoffId([3; 16]),
                    muxe_protocol::UnitReadinessEpochId::from_bytes([4; 16]).unwrap(),
                    muxe_protocol::AsOfTick::from_millis(100).unwrap(),
                )
                .await
        })
        .await;
        assert!(
            result.is_err(),
            "silent owned peer cannot extend the proof deadline"
        );
        peer.abort();
    }

    #[tokio::test]
    async fn broker_error_maps_to_rejection() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let socket = temp.path().join("control.sock");
        let server = tokio::spawn({
            let socket = socket.clone();
            async move {
                serve_once(&socket, |_| ControlResult::Error {
                    diagnostic: "activation_in_progress".to_owned(),
                    prepare_refusal: None,
                })
                .await;
            }
        });
        wait_for_socket(&socket).await;
        let mut client = ControlClient::connect(&socket).await.unwrap();
        let error = client.status().await.unwrap_err();
        assert!(matches!(
            error,
            ControlError::Rejected { operation: "Status", diagnostic, prepare_refusal: None }
                if diagnostic == "activation_in_progress"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn mismatched_result_kind_is_unexpected() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
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
            Err(ControlError::UnexpectedResult {
                expected: ControlResultKind::Status,
                actual: ControlResultKind::Retired,
            })
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn lifecycle_operations_retain_expected_and_received_response_kinds() {
        for expected in [
            ControlResultKind::Prepared,
            ControlResultKind::Committed,
            ControlResultKind::Aborted,
            ControlResultKind::Retired,
        ] {
            let temp = tempfile::tempdir().unwrap();
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let socket = temp.path().join("control.sock");
            let server = tokio::spawn({
                let socket = socket.clone();
                async move {
                    serve_once(&socket, |_| ControlResult::Status(test_status())).await;
                }
            });
            wait_for_socket(&socket).await;
            let mut client = ControlClient::connect(&socket).await.unwrap();
            let handoff = HandoffId([3; 16]);
            let result = match expected {
                ControlResultKind::Prepared => client.prepare(current_record(), handoff).await,
                ControlResultKind::Committed => client.commit(handoff).await,
                ControlResultKind::Aborted => client.abort(handoff).await,
                ControlResultKind::Retired => client.retire().await,
                _ => unreachable!("only lifecycle acknowledgements are in this table"),
            };
            assert!(matches!(
                result,
                Err(ControlError::UnexpectedResult { expected: observed, actual: ControlResultKind::Status })
                    if observed == expected
            ));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn status_at_proof_failures_retain_missing_and_mismatched_values() {
        let handoff = HandoffId([3; 16]);
        let epoch = muxe_protocol::UnitReadinessEpochId::from_bytes([4; 16]).unwrap();
        let foreign_epoch = muxe_protocol::UnitReadinessEpochId::from_bytes([5; 16]).unwrap();
        let as_of = muxe_protocol::AsOfTick::from_millis(100).unwrap();
        for (actual_handoff, actual_epoch, has_ready) in [
            (None, Some(epoch), true),
            (Some(HandoffId([6; 16])), Some(epoch), true),
            (Some(handoff), None, false),
            (Some(handoff), None, true),
            (Some(handoff), Some(foreign_epoch), true),
        ] {
            let temp = tempfile::tempdir().unwrap();
            std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
            let socket = temp.path().join("control.sock");
            let server = tokio::spawn({
                let socket = socket.clone();
                async move {
                    serve_once(&socket, |_| {
                        let mut status = test_status();
                        status.phase = muxe_protocol::control::ActivationPhase::Legacy;
                        status.handoff_id = actual_handoff;
                        if has_ready {
                            status.ready = Some(muxe_protocol::TargetReadiness {
                                registered_clients: Vec::new(),
                                member_clients: 0,
                                member_ids: Some(Vec::new()),
                                proof_epoch: actual_epoch,
                            });
                        }
                        ControlResult::StatusAt(status)
                    })
                    .await;
                }
            });
            wait_for_socket(&socket).await;
            let mut client = ControlClient::connect(&socket).await.unwrap();
            assert!(matches!(
                client.status_at(handoff, epoch, as_of).await,
                Err(ControlError::ReadinessProofMismatch {
                    expected_handoff,
                    actual_handoff: observed_handoff,
                    expected_epoch,
                    actual_epoch: observed_epoch,
                }) if expected_handoff == handoff
                    && expected_epoch == epoch
                    && observed_handoff == actual_handoff
                    && observed_epoch == actual_epoch
            ));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn oversized_request_is_rejected_locally_before_any_request_bytes() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let socket = temp.path().join("control.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut client = ControlClient::connect(&socket).await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut prelude = [0; muxe_protocol::frame::PRELUDE_LEN];
        peer.read_exact(&mut prelude).await.unwrap();
        assert_eq!(
            prelude,
            Prelude::control(PeerRole::ActivationCoordinator).encode()
        );
        let mut target = current_record();
        target.muxe_version = "a".repeat(MAX_CONTROL_FRAME_LEN as usize);
        assert!(matches!(
            client.prepare(target, HandoffId([3; 16])).await,
            Err(ControlError::RequestTooLarge)
        ));
        let mut byte = [0];
        assert!(
            timeout(Duration::from_millis(30), peer.read(&mut byte))
                .await
                .is_err()
        );
    }

    #[test]
    fn encoding_failure_is_local_and_retains_the_serializer_cause() {
        struct Unencodable;
        impl serde::Serialize for Unencodable {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("fixture serialization failure"))
            }
        }
        let error = encode_request(&Unencodable).unwrap_err();
        assert!(matches!(&error, ControlError::Encode(_)));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn handoff_hex_round_trip() {
        let id = handoff_from_hex(&"ab".repeat(16)).unwrap();
        assert_eq!(id.0, [0xab; 16]);
        for invalid in [
            "short",
            "gggggggggggggggggggggggggggggggg",
            "éééééééééééééééé",
        ] {
            assert!(matches!(
                handoff_from_hex(invalid),
                Err(ControlError::InvalidHandoffId)
            ));
        }
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
