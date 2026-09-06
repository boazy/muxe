use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

use crate::generated::{MethodMetadata, MethodTransport};

const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// Whether bytes can have reached the Herdr server. Callers must never replay a mutation after
/// `MayHaveReachedHost`; the broker reports that case as `outcome_unknown`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryState {
    NotSent,
    MayHaveReachedHost,
}

/// The correlated outcome of one Herdr request.
#[derive(Clone, Debug, PartialEq)]
pub enum HerdrResponse {
    Success(Value),
    Error { code: String, message: String },
}

#[derive(Debug, Error)]
pub enum SocketError {
    #[error("Herdr method {method:?} is an event stream, not a unary request")]
    StreamingMethod { method: String },
    #[error("serialized Herdr request exceeds {MAX_MESSAGE_BYTES} bytes")]
    RequestTooLarge,
    #[error("Herdr request ID sequence is exhausted")]
    RequestIdExhausted,
    #[error("could not connect to Herdr socket {socket}")]
    Connect {
        socket: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("could not write Herdr request")]
    Write {
        delivery: DeliveryState,
        #[source]
        source: io::Error,
    },
    #[error("Herdr closed the socket before a complete response line")]
    EarlyEof { delivery: DeliveryState },
    #[error("Herdr response exceeds {MAX_MESSAGE_BYTES} bytes")]
    ResponseTooLarge { delivery: DeliveryState },
    #[error("Herdr did not answer within the request deadline")]
    Timeout { delivery: DeliveryState },
    #[error("could not capture the Herdr endpoint identity at {socket}")]
    Endpoint {
        socket: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("could not read Herdr response")]
    Read {
        delivery: DeliveryState,
        #[source]
        source: io::Error,
    },
    #[error("Herdr response is not valid JSON")]
    InvalidJson {
        delivery: DeliveryState,
        #[source]
        source: serde_json::Error,
    },
    #[error("Herdr protocol error: {message}")]
    Protocol {
        delivery: DeliveryState,
        message: String,
    },
}

impl SocketError {
    pub const fn delivery(&self) -> DeliveryState {
        match self {
            Self::StreamingMethod { .. }
            | Self::RequestTooLarge
            | Self::RequestIdExhausted => DeliveryState::NotSent,
            Self::Connect { .. } | Self::Endpoint { .. } => DeliveryState::NotSent,
            Self::Write { delivery, .. }
            | Self::EarlyEof { delivery }
            | Self::ResponseTooLarge { delivery }
            | Self::Timeout { delivery }
            | Self::Read { delivery, .. }
            | Self::InvalidJson { delivery, .. }
            | Self::Protocol { delivery, .. } => *delivery,
        }
    }
}

/// A direct, one-request-per-connection Herdr client. It intentionally owns no retry policy:
/// higher layers may retry read-only requests only after observing `NotSent`.
#[derive(Debug)]
pub struct HerdrSocketClient {
    socket: PathBuf,
    next_request: AtomicU64,
}

impl HerdrSocketClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            next_request: AtomicU64::new(0),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Sends one generated unary method and verifies that the server closes the connection after
    /// exactly one response with the generated request ID.
    pub async fn unary(
        &self,
        metadata: &MethodMetadata,
        params: Value,
    ) -> Result<HerdrResponse, SocketError> {
        let (reader, id) = self.send_request(metadata, params).await?;
        Self::finish_unary(reader, &id).await
    }

    /// Sends one generated unary method, then waits for its single response only until
    /// `timeout` elapses. The deadline is a transport wait bound, not a Muxe detach:
    /// the request bytes were already flushed, so an elapsed deadline reports
    /// `MayHaveReachedHost` and the caller must surface `outcome_unknown` instead of
    /// retrying a state-changing request.
    pub async fn unary_with_timeout(
        &self,
        metadata: &MethodMetadata,
        params: Value,
        timeout: Duration,
    ) -> Result<HerdrResponse, SocketError> {
        let (mut reader, id) = self.send_request(metadata, params).await?;
        let response_line =
            tokio::time::timeout(timeout, read_response_line(&mut reader))
                .await
                .map_err(|_| SocketError::Timeout {
                    delivery: DeliveryState::MayHaveReachedHost,
                })??;
        let response =
            serde_json::from_slice(&response_line).map_err(|source| SocketError::InvalidJson {
                delivery: DeliveryState::MayHaveReachedHost,
                source,
            })?;
        let outcome = parse_response(response, &id)?;
        reject_trailing_data(&mut reader).await?;
        Ok(outcome)
    }

    /// Opens one fresh connection without sending bytes. The subscription monitor uses this to
    /// hold its own long-lived `events.subscribe` stream; no ordinary request is multiplexed
    /// onto that stream.
    pub async fn connect_stream(&self) -> Result<UnixStream, SocketError> {
        UnixStream::connect(&self.socket)
            .await
            .map_err(|source| SocketError::Connect {
                socket: self.socket.clone(),
                source,
            })
    }

    /// Captures the OS-visible identity of the exact server behind the socket: canonical path,
    /// socket-file device and inode, and best-effort peer credentials from one fresh
    /// connection. A replacement server rebound to the same path carries a new inode, so it
    /// can never compare equal to the old incarnation even when it reports the same
    /// protocol and version.
    pub async fn probe_endpoint(&self) -> Result<EndpointIdentity, SocketError> {
        let stream = self.connect_stream().await?;
        EndpointIdentity::capture(&self.socket, &stream).map_err(|source| SocketError::Endpoint {
            socket: self.socket.clone(),
            source,
        })
    }

    /// Encodes, size-checks, and fully flushes one request line on a fresh connection,
    /// returning the buffered stream and the generated request ID for response correlation.
    pub(crate) async fn send_request(
        &self,
        metadata: &MethodMetadata,
        params: Value,
    ) -> Result<(BufReader<UnixStream>, String), SocketError> {
        if metadata.transport != MethodTransport::Unary {
            return Err(SocketError::StreamingMethod {
                method: metadata.method.to_owned(),
            });
        }
        let id = self.next_id()?;
        let mut line = encode_request(metadata.method, &id, params)?;
        line.push(b'\n');
        let mut stream = self.connect_stream().await?;
        write_line(&mut stream, &line).await?;
        Ok((BufReader::new(stream), id))
    }

    async fn finish_unary(
        mut reader: BufReader<UnixStream>,
        id: &str,
    ) -> Result<HerdrResponse, SocketError> {
        let response_line = read_response_line(&mut reader).await?;
        let response =
            serde_json::from_slice(&response_line).map_err(|source| SocketError::InvalidJson {
                delivery: DeliveryState::MayHaveReachedHost,
                source,
            })?;
        let outcome = parse_response(response, id)?;
        reject_trailing_data(&mut reader).await?;
        Ok(outcome)
    }

    pub(crate) fn next_id(&self) -> Result<String, SocketError> {
        let sequence = self
            .next_request
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map_err(|_| SocketError::RequestIdExhausted)?;
        Ok(format!("muxe-herdr-{}-{sequence}", std::process::id()))
    }
}

/// Encodes one JSON-RPC-style request line (without the trailing newline) and rejects
/// oversize payloads before any byte reaches the host.
pub(crate) fn encode_request(
    method: &str,
    id: &str,
    params: Value,
) -> Result<Vec<u8>, SocketError> {
    let request = json!({
        "id": id,
        "method": method,
        "params": params,
    });
    let encoded = serde_json::to_vec(&request).map_err(|source| SocketError::InvalidJson {
        delivery: DeliveryState::NotSent,
        source,
    })?;
    if encoded.len() + 1 > MAX_MESSAGE_BYTES {
        return Err(SocketError::RequestTooLarge);
    }
    Ok(encoded)
}

/// Flushes one complete request line, tracking whether any byte may have reached the
/// host so callers never replay a mutation after a partial write.
pub(crate) async fn write_line(stream: &mut UnixStream, line: &[u8]) -> Result<(), SocketError> {
    let mut sent = 0;
    while sent < line.len() {
        match stream.write(&line[sent..]).await {
            Ok(0) => {
                return Err(SocketError::Write {
                    delivery: delivery_after(sent),
                    source: io::Error::new(
                        io::ErrorKind::WriteZero,
                        "Herdr socket accepted zero bytes",
                    ),
                });
            }
            Ok(written) => sent += written,
            Err(source) => {
                return Err(SocketError::Write {
                    delivery: delivery_after(sent),
                    source,
                });
            }
        }
    }
    stream.flush().await.map_err(|source| SocketError::Write {
        delivery: DeliveryState::MayHaveReachedHost,
        source,
    })
}

/// Credentials the OS reports for the peer of one Herdr connection. They are advisory
/// defense-in-depth beside the socket-file device/inode boundary: a replacement server
/// rebound to the same path normally compares unequal through its fresh inode, but
/// inode numbers may be recycled, so peer evidence is never continuity proof alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerIdentity {
    pub uid: u32,
    pub gid: u32,
    pub pid: Option<u32>,
}

/// The OS-visible identity of one Herdr server incarnation behind a socket path.
///
/// Comparison semantics follow the continuity rule: inequality is sound proof of
/// replacement (the live socket file or its peer changed), while equality is
/// observation only and never continuity proof, because POSIX may recycle inode
/// numbers after unlink. The continuity authority is the retained subscription
/// stream staying alive plus a new local epoch after any loss; this record only
/// ever proves change, never sameness. See [`EndpointIdentity::proven_replacement`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointIdentity {
    socket: PathBuf,
    device: u64,
    inode: u64,
    peer: Option<PeerIdentity>,
}

impl EndpointIdentity {
    /// Stats the socket path and attaches best-effort peer credentials from one already
    /// connected stream. A missing or unreadable socket path fails; unavailable peer
    /// credentials degrade to `None` and are reported through
    /// [`EndpointIdentity::has_peer_evidence`] so the broker can fail closed where its
    /// policy requires peer evidence.
    pub fn capture(socket: &Path, stream: &UnixStream) -> io::Result<Self> {
        let canonical = fs::canonicalize(socket).unwrap_or_else(|_| socket.to_path_buf());
        let peer = stream.peer_cred().ok().map(|credentials| PeerIdentity {
            uid: credentials.uid(),
            gid: credentials.gid(),
            pid: credentials.pid().and_then(|pid| u32::try_from(pid).ok()),
        });
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = fs::metadata(&canonical)?;
            return Ok(Self {
                socket: canonical,
                device: metadata.dev(),
                inode: metadata.ino(),
                peer,
            });
        }
        #[cfg(not(unix))]
        {
            let _ = fs::metadata(&canonical)?;
            return Ok(Self {
                socket: canonical,
                device: 0,
                inode: 0,
                peer,
            });
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub fn device(&self) -> u64 {
        self.device
    }

    pub fn inode(&self) -> u64 {
        self.inode
    }

    pub fn peer(&self) -> Option<PeerIdentity> {
        self.peer
    }

    /// Whether OS peer credentials were available at capture. Without them the
    /// device/inode boundary still observes replacement, but broker policy may
    /// require this evidence before trusting even the observation.
    pub fn has_peer_evidence(&self) -> bool {
        self.peer.is_some()
    }

    /// Reports whether `other` provably identifies a different server incarnation.
    /// Inequality is sound proof of replacement: the live socket file or its peer
    /// changed. Equality is explicitly NOT proof of continuity: POSIX may recycle
    /// inode numbers after unlink, so a rebound server can in theory present the
    /// same device, inode, and peer. The continuity authority is the retained
    /// subscription stream staying alive plus a new local epoch after any loss;
    /// callers must never treat `!proven_replacement(a, b)` as proof that `a`
    /// and `b` are the same live server.
    pub fn proven_replacement(&self, other: &Self) -> bool {
        self != other
    }

    /// Re-stats the captured socket path and reports whether its device and inode still
    /// match. `false` proves replacement; `true` is only a cheap gate and never a
    /// continuity proof on its own.
    pub fn recheck(&self) -> bool {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            fs::metadata(&self.socket).is_ok_and(|metadata| {
                metadata.dev() == self.device && metadata.ino() == self.inode
            })
        }
        #[cfg(not(unix))]
        {
            fs::metadata(&self.socket).is_ok()
        }
    }

    /// Renders the opaque server-incarnation identifier carried in `HostIdentity`.
    /// The value is only equality-compared; no consumer may parse or persist structure
    /// from it. Equality is necessary but never sufficient for continuity: a changed
    /// value proves replacement, an unchanged value proves nothing (see
    /// [`EndpointIdentity::proven_replacement`]).
    pub fn live_server_id(&self, protocol: u64, version: &str) -> String {
        let peer = match self.peer {
            Some(peer) => match peer.pid {
                Some(pid) => format!("peer-pid-{pid}-uid-{}-gid-{}", peer.uid, peer.gid),
                None => format!("peer-uid-{}-gid-{}-nopid", peer.uid, peer.gid),
            },
            None => "peer-unavailable".to_owned(),
        };
        format!(
            "herdr/dev:{}-ino:{}/proto:{protocol}/ver:{version}/{peer}",
            self.device, self.inode
        )
    }
}

fn delivery_after(written: usize) -> DeliveryState {
    if written == 0 {
        DeliveryState::NotSent
    } else {
        DeliveryState::MayHaveReachedHost
    }
}
pub(crate) async fn read_response_line(
    reader: &mut BufReader<UnixStream>,
) -> Result<Vec<u8>, SocketError> {
    let mut response = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|source| SocketError::Read {
                delivery: DeliveryState::MayHaveReachedHost,
                source,
            })?;
        if available.is_empty() {
            return Err(SocketError::EarlyEof {
                delivery: DeliveryState::MayHaveReachedHost,
            });
        }
        let consumed = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |position| position + 1);
        if response.len() + consumed > MAX_MESSAGE_BYTES {
            return Err(SocketError::ResponseTooLarge {
                delivery: DeliveryState::MayHaveReachedHost,
            });
        }
        let has_newline = available[consumed - 1] == b'\n';
        response.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if has_newline {
            response.pop();
            return Ok(response);
        }
    }
}
pub(crate) fn parse_response(response: Value, expected_id: &str) -> Result<HerdrResponse, SocketError> {
    let object = response
        .as_object()
        .ok_or_else(|| protocol("response must be a JSON object"))?;
    let id = required_string(object, "id")?;
    if id != expected_id {
        return Err(protocol(format!(
            "response id {id:?} does not match request id {expected_id:?}"
        )));
    }
    match (object.get("result"), object.get("error")) {
        (Some(_), Some(_)) => Err(protocol("response must not contain both result and error")),
        (Some(result), None) if result.is_object() => Ok(HerdrResponse::Success(result.clone())),
        (Some(_), None) => Err(protocol("response result must be an object")),
        (None, Some(error)) => {
            let error = error
                .as_object()
                .ok_or_else(|| protocol("response error must be an object"))?;
            Ok(HerdrResponse::Error {
                code: required_string(error, "code")?.to_owned(),
                message: required_string(error, "message")?.to_owned(),
            })
        }
        (None, None) => Err(protocol(
            "response must contain exactly one of result or error",
        )),
    }
}

async fn reject_trailing_data(reader: &mut BufReader<UnixStream>) -> Result<(), SocketError> {
    if !reader.buffer().is_empty() {
        return Err(protocol("response has trailing bytes"));
    }
    let mut byte = [0_u8; 1];
    let bytes = reader
        .read(&mut byte)
        .await
        .map_err(|source| SocketError::Read {
            delivery: DeliveryState::MayHaveReachedHost,
            source,
        })?;
    if bytes == 0 {
        Ok(())
    } else {
        Err(protocol("response has trailing bytes"))
    }
}

fn required_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, SocketError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol(format!("response field {field:?} must be a string")))
}

fn protocol(message: impl Into<String>) -> SocketError {
    SocketError::Protocol {
        delivery: DeliveryState::MayHaveReachedHost,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::{UnixListener, UnixStream},
    };

    use super::*;

    fn ping() -> &'static MethodMetadata {
        crate::generated::method_metadata("ping").unwrap()
    }

    async fn listen(temp: &TempDir) -> (UnixListener, PathBuf) {
        let path = temp.path().join("herdr.sock");
        (UnixListener::bind(&path).unwrap(), path)
    }

    async fn read_request(stream: UnixStream) -> (BufReader<UnixStream>, String) {
        let mut reader = BufReader::new(stream);
        let mut request = Vec::new();
        reader.read_until(b'\n', &mut request).await.unwrap();
        let id = serde_json::from_slice::<Value>(&request).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        (reader, id)
    }

    #[tokio::test]
    async fn accepts_one_correlated_response_and_eof() {
        let temp = TempDir::new().unwrap();
        let (listener, path) = listen(&temp).await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, id) = read_request(stream).await;
            reader
                .write_all(
                    format!("{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\"}}}}\n").as_bytes(),
                )
                .await
                .unwrap();
        });
        let response = HerdrSocketClient::new(path)
            .unary(ping(), json!({}))
            .await
            .unwrap();
        server.await.unwrap();
        assert_eq!(response, HerdrResponse::Success(json!({"type": "pong"})));
    }

    #[tokio::test]
    async fn rejects_an_uncorrelated_response() {
        let temp = TempDir::new().unwrap();
        let (listener, path) = listen(&temp).await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _) = read_request(stream).await;
            reader
                .write_all(b"{\"id\":\"different\",\"result\":{}}\n")
                .await
                .unwrap();
        });
        let error = HerdrSocketClient::new(path)
            .unary(ping(), json!({}))
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(matches!(error, SocketError::Protocol { .. }));
        assert_eq!(error.delivery(), DeliveryState::MayHaveReachedHost);
    }

    #[tokio::test]
    async fn rejects_trailing_response_bytes() {
        let temp = TempDir::new().unwrap();
        let (listener, path) = listen(&temp).await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, id) = read_request(stream).await;
            reader
                .write_all(format!("{{\"id\":\"{id}\",\"result\":{{}}}}\ntrailing").as_bytes())
                .await
                .unwrap();
        });
        let error = HerdrSocketClient::new(path)
            .unary(ping(), json!({}))
            .await
            .unwrap_err();
        server.await.unwrap();
        assert!(matches!(error, SocketError::Protocol { .. }));
    }

    #[tokio::test]
    async fn refuses_a_generated_event_stream_as_unary() {
        let client = HerdrSocketClient::new(Path::new("/does/not/matter"));
        let stream = crate::generated::method_metadata("events.subscribe").unwrap();
        let error = client.unary(stream, json!({})).await.unwrap_err();
        assert!(matches!(error, SocketError::StreamingMethod { .. }));
        assert_eq!(error.delivery(), DeliveryState::NotSent);
    }

    #[tokio::test]
    async fn wait_deadline_reports_unknown_outcome_after_flush() {
        tokio::time::pause();
        let temp = TempDir::new().unwrap();
        let (listener, path) = listen(&temp).await;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (_reader, _id) = read_request(stream).await;
            // Accept the mutation bytes, then never answer: the client must give
            // up waiting without ever replaying the request.
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        });
        let waiting = tokio::spawn({
            let path = path.clone();
            async move {
                HerdrSocketClient::new(path)
                    .unary_with_timeout(ping(), json!({}), std::time::Duration::from_millis(50))
                    .await
            }
        });
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_millis(200)).await;
        let error = waiting.await.unwrap().unwrap_err();
        server.abort();
        assert!(matches!(error, SocketError::Timeout { .. }));
        assert_eq!(
            error.delivery(),
            DeliveryState::MayHaveReachedHost,
            "an elapsed wait deadline must surface outcome_unknown, never a retry"
        );
    }

    #[tokio::test]
    async fn endpoint_identity_distinguishes_rebound_server() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let first_stream = UnixStream::connect(&path).await.unwrap();
        let first = EndpointIdentity::capture(&path, &first_stream).unwrap();
        assert!(first.recheck());
        drop(listener);
        drop(first_stream);
        std::fs::remove_file(&path).unwrap();
        assert!(
            !first.recheck(),
            "a removed socket must fail the cheap stat gate"
        );
        // A replacement server rebound to the same canonical path carries a fresh
        // inode even though it reports the same protocol and version.
        let replacement = UnixListener::bind(&path).unwrap();
        let second_stream = UnixStream::connect(&path).await.unwrap();
        let second = EndpointIdentity::capture(&path, &second_stream).unwrap();
        drop(replacement);
        assert!(second.recheck());
        assert_ne!(
            first, second,
            "rebound servers must never compare equal across incarnations"
        );
        assert_ne!(
            first.live_server_id(20, "0.8.2"),
            second.live_server_id(20, "0.8.2")
        );
        assert!(
            first.proven_replacement(&second),
            "endpoint inequality is sound proof of replacement"
        );
        assert!(
            !second.proven_replacement(&second),
            "equality never proves continuity on its own"
        );
    }
}
