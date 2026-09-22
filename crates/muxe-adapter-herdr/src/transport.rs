use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[cfg(test)]
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::{Map, Value, json};
use thiserror::Error;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::watch,
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
    #[error("Herdr endpoint at {socket} was replaced before the request was sent")]
    EndpointReplaced { socket: PathBuf },
    #[error("Herdr runtime retired while the request was in flight")]
    RuntimeRetired { delivery: DeliveryState },
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
    #[must_use]
    pub const fn delivery(&self) -> DeliveryState {
        match self {
            Self::StreamingMethod { .. }
            | Self::RequestTooLarge
            | Self::RequestIdExhausted
            | Self::Connect { .. }
            | Self::Endpoint { .. }
            | Self::EndpointReplaced { .. } => DeliveryState::NotSent,
            Self::Write { delivery, .. }
            | Self::EarlyEof { delivery }
            | Self::ResponseTooLarge { delivery }
            | Self::Timeout { delivery }
            | Self::RuntimeRetired { delivery }
            | Self::Read { delivery, .. }
            | Self::InvalidJson { delivery, .. }
            | Self::Protocol { delivery, .. } => *delivery,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SocketFileIdentity {
    socket: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFileIdentity {
    fn capture(socket: &Path) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let canonical = fs::canonicalize(socket).unwrap_or_else(|_| socket.to_path_buf());
        let metadata = fs::metadata(&canonical)?;
        Ok(Self {
            socket: canonical,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

/// One typed observation of the exact socket file and peer joined by an
/// observed connect. Inequality proves replacement; equality is only an
/// observation because the OS may recycle inode and process identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EndpointContinuityToken {
    socket_file: SocketFileIdentity,
    peer: PeerIdentity,
}

impl EndpointContinuityToken {
    fn capture(socket_file: SocketFileIdentity, stream: &UnixStream) -> io::Result<Self> {
        Ok(Self {
            socket_file,
            peer: PeerIdentity::capture(stream)?,
        })
    }

    #[must_use]
    pub(crate) fn proven_replacement(&self, other: &Self) -> bool {
        self != other
    }

    pub(crate) fn verify_socket_file(&self, socket: &Path) -> Result<(), SocketError> {
        let actual =
            SocketFileIdentity::capture(socket).map_err(|source| SocketError::Endpoint {
                socket: socket.to_path_buf(),
                source,
            })?;
        if self.socket_file == actual {
            Ok(())
        } else {
            Err(SocketError::EndpointReplaced {
                socket: socket.to_path_buf(),
            })
        }
    }

    #[must_use]
    pub(crate) fn live_server_id(&self, protocol: u64, version: &str) -> String {
        live_server_id(
            self.socket_file.device,
            self.socket_file.inode,
            self.peer,
            protocol,
            version,
        )
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ObservedConnectHook {
    entered: Notify,
    release: Notify,
}

struct PreparedUnary {
    id: String,
    line: Vec<u8>,
}

enum ResponseDeadline {
    Unbounded,
    Bounded(Duration),
}

#[cfg(test)]
impl ObservedConnectHook {
    fn new() -> Self {
        Self::default()
    }
}

/// A direct, one-request-per-connection Herdr client. It intentionally owns no retry policy:
/// higher layers may retry read-only requests only after observing `NotSent`.
#[derive(Debug)]
pub(crate) struct HerdrSocketClient {
    socket: PathBuf,
    next_request: AtomicU64,
    #[cfg(test)]
    observed_connect_hook: StdMutex<Option<Arc<ObservedConnectHook>>>,
}

impl HerdrSocketClient {
    pub(crate) fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            next_request: AtomicU64::new(0),
            #[cfg(test)]
            observed_connect_hook: StdMutex::new(None),
        }
    }

    pub(crate) fn socket(&self) -> &Path {
        &self.socket
    }

    #[cfg(test)]
    async fn unary(
        &self,
        metadata: &MethodMetadata,
        params: Value,
    ) -> Result<HerdrResponse, SocketError> {
        self.establish_unary(metadata, params)
            .await
            .map(|(response, _)| response)
    }

    /// Establishes one endpoint observation by sending the request on the
    /// exact stream whose socket file and peer form the returned token.
    pub(crate) async fn establish_unary(
        &self,
        metadata: &MethodMetadata,
        params: Value,
    ) -> Result<(HerdrResponse, EndpointContinuityToken), SocketError> {
        let request = self.prepare_unary(metadata, &params)?;
        let (stream, token) = self.observed_connect().await?;
        let response = Self::exchange(stream, request, ResponseDeadline::Unbounded).await?;
        Ok((response, token))
    }

    /// Sends only when a fresh observed connection matches `expected`.
    /// A mismatch drops the still-unwritten stream and reports `NotSent`.
    #[cfg(test)]
    pub(crate) async fn unary_on_expected_token(
        &self,
        metadata: &MethodMetadata,
        params: Value,
        expected: &EndpointContinuityToken,
    ) -> Result<HerdrResponse, SocketError> {
        let request = self.prepare_unary(metadata, &params)?;
        let (stream, actual) = self.observed_connect().await?;
        if expected.proven_replacement(&actual) {
            return Err(SocketError::EndpointReplaced {
                socket: self.socket.clone(),
            });
        }
        Self::exchange(stream, request, ResponseDeadline::Unbounded).await
    }

    /// Sends one guarded request and waits for its response without imposing a
    /// local deadline. Retirement interrupts connect, write, or response
    /// processing and preserves exact delivery evidence.
    pub(crate) async fn unary_on_expected_token_guarded<F>(
        &self,
        metadata: &MethodMetadata,
        params: Value,
        expected: &EndpointContinuityToken,
        retirement: watch::Receiver<bool>,
        on_replacement: F,
    ) -> Result<HerdrResponse, SocketError>
    where
        F: Fn(),
    {
        self.unary_on_expected_token_guarded_deadline(
            metadata,
            params,
            expected,
            ResponseDeadline::Unbounded,
            retirement,
            on_replacement,
        )
        .await
    }

    /// Sends one guarded request with an explicit caller-provided response
    /// deadline. Retirement remains independently cancellation-selectable.
    pub(crate) async fn unary_on_expected_token_guarded_with_timeout<F>(
        &self,
        metadata: &MethodMetadata,
        params: Value,
        expected: &EndpointContinuityToken,
        timeout: Duration,
        retirement: watch::Receiver<bool>,
        on_replacement: F,
    ) -> Result<HerdrResponse, SocketError>
    where
        F: Fn(),
    {
        self.unary_on_expected_token_guarded_deadline(
            metadata,
            params,
            expected,
            ResponseDeadline::Bounded(timeout),
            retirement,
            on_replacement,
        )
        .await
    }

    async fn unary_on_expected_token_guarded_deadline<F>(
        &self,
        metadata: &MethodMetadata,
        params: Value,
        expected: &EndpointContinuityToken,
        deadline: ResponseDeadline,
        mut retirement: watch::Receiver<bool>,
        on_replacement: F,
    ) -> Result<HerdrResponse, SocketError>
    where
        F: Fn(),
    {
        let request = self.prepare_unary(metadata, &params)?;
        if *retirement.borrow() {
            return Err(SocketError::RuntimeRetired {
                delivery: DeliveryState::NotSent,
            });
        }
        let observed = tokio::select! {
            biased;
            changed = retirement.changed() => {
                let _ = changed;
                return Err(SocketError::RuntimeRetired {
                    delivery: DeliveryState::NotSent,
                });
            }
            observed = self.observed_connect() => observed,
        };
        let (stream, actual) = match observed {
            Ok(observed) => observed,
            Err(error @ SocketError::EndpointReplaced { .. }) => {
                on_replacement();
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if expected.proven_replacement(&actual) {
            on_replacement();
            return Err(SocketError::EndpointReplaced {
                socket: self.socket.clone(),
            });
        }
        if *retirement.borrow() {
            return Err(SocketError::RuntimeRetired {
                delivery: DeliveryState::NotSent,
            });
        }
        Self::exchange_guarded(stream, request, deadline, retirement).await
    }

    #[cfg(test)]
    async fn unary_with_timeout(
        &self,
        metadata: &MethodMetadata,
        params: Value,
        timeout: Duration,
    ) -> Result<HerdrResponse, SocketError> {
        let request = self.prepare_unary(metadata, &params)?;
        let (stream, _) = self.observed_connect().await?;
        Self::exchange(stream, request, ResponseDeadline::Bounded(timeout)).await
    }

    /// Opens one observed stream only when it matches `expected`. The stream
    /// remains unwritten so the subscription layer can encode and send its
    /// retained request after this guard.
    pub(crate) async fn connect_on_expected_token(
        &self,
        expected: &EndpointContinuityToken,
    ) -> Result<UnixStream, SocketError> {
        let (stream, actual) = self.observed_connect().await?;
        if expected.proven_replacement(&actual) {
            return Err(SocketError::EndpointReplaced {
                socket: self.socket.clone(),
            });
        }
        Ok(stream)
    }

    /// One observed connect. The socket path is stat'ed before and after
    /// `connect`; only an unchanged device/inode pair may be joined to peer
    /// credentials from that exact stream. Inequality proves replacement and
    /// drops the stream before any request write. Equality is still only an
    /// observation because the OS may recycle both file and process IDs.
    async fn observed_connect(&self) -> Result<(UnixStream, EndpointContinuityToken), SocketError> {
        let before =
            SocketFileIdentity::capture(&self.socket).map_err(|source| SocketError::Endpoint {
                socket: self.socket.clone(),
                source,
            })?;
        #[cfg(test)]
        {
            let hook = self
                .observed_connect_hook
                .lock()
                .expect("observed-connect hook is not poisoned")
                .clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }
        let stream =
            UnixStream::connect(&self.socket)
                .await
                .map_err(|source| SocketError::Connect {
                    socket: self.socket.clone(),
                    source,
                })?;
        let Ok(after) = SocketFileIdentity::capture(&self.socket) else {
            return Err(SocketError::EndpointReplaced {
                socket: self.socket.clone(),
            });
        };
        if before != after {
            return Err(SocketError::EndpointReplaced {
                socket: self.socket.clone(),
            });
        }
        let token = EndpointContinuityToken::capture(after, &stream).map_err(|source| {
            SocketError::Endpoint {
                socket: self.socket.clone(),
                source,
            }
        })?;
        Ok((stream, token))
    }

    fn prepare_unary(
        &self,
        metadata: &MethodMetadata,
        params: &Value,
    ) -> Result<PreparedUnary, SocketError> {
        if metadata.transport != MethodTransport::Unary {
            return Err(SocketError::StreamingMethod {
                method: metadata.method.to_owned(),
            });
        }
        let id = self.next_id()?;
        let mut line = encode_request(metadata.method, &id, params)?;
        line.push(b'\n');
        Ok(PreparedUnary { id, line })
    }

    async fn exchange(
        mut stream: UnixStream,
        request: PreparedUnary,
        deadline: ResponseDeadline,
    ) -> Result<HerdrResponse, SocketError> {
        write_line(&mut stream, &request.line).await?;
        let mut reader = BufReader::new(stream);
        let response_line = match deadline {
            ResponseDeadline::Unbounded => read_response_line(&mut reader).await?,
            ResponseDeadline::Bounded(timeout) => {
                tokio::time::timeout(timeout, read_response_line(&mut reader))
                    .await
                    .map_err(|_| SocketError::Timeout {
                        delivery: DeliveryState::MayHaveReachedHost,
                    })??
            }
        };
        let response =
            serde_json::from_slice(&response_line).map_err(|source| SocketError::InvalidJson {
                delivery: DeliveryState::MayHaveReachedHost,
                source,
            })?;
        let outcome = parse_response(&response, &request.id)?;
        reject_trailing_data(&mut reader).await?;
        Ok(outcome)
    }

    async fn exchange_guarded(
        mut stream: UnixStream,
        request: PreparedUnary,
        deadline: ResponseDeadline,
        mut retirement: watch::Receiver<bool>,
    ) -> Result<HerdrResponse, SocketError> {
        write_line_guarded(&mut stream, &request.line, &mut retirement).await?;
        let response = async {
            let mut reader = BufReader::new(stream);
            let response_line = read_response_line(&mut reader).await?;
            let response = serde_json::from_slice(&response_line).map_err(|source| {
                SocketError::InvalidJson {
                    delivery: DeliveryState::MayHaveReachedHost,
                    source,
                }
            })?;
            let outcome = parse_response(&response, &request.id)?;
            reject_trailing_data(&mut reader).await?;
            Ok(outcome)
        };
        match deadline {
            ResponseDeadline::Unbounded => {
                tokio::select! {
                    biased;
                    changed = retirement.changed() => {
                        let _ = changed;
                        Err(SocketError::RuntimeRetired {
                            delivery: DeliveryState::MayHaveReachedHost,
                        })
                    }
                    result = response => result,
                }
            }
            ResponseDeadline::Bounded(timeout) => {
                tokio::select! {
                    biased;
                    changed = retirement.changed() => {
                        let _ = changed;
                        Err(SocketError::RuntimeRetired {
                            delivery: DeliveryState::MayHaveReachedHost,
                        })
                    }
                    result = tokio::time::timeout(timeout, response) => {
                        result.map_err(|_| SocketError::Timeout {
                            delivery: DeliveryState::MayHaveReachedHost,
                        })?
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn set_observed_connect_hook(&self, hook: Option<Arc<ObservedConnectHook>>) {
        *self
            .observed_connect_hook
            .lock()
            .expect("observed-connect hook is writable") = hook;
    }

    #[cfg(test)]
    pub(crate) async fn observed_token(&self) -> Result<EndpointContinuityToken, SocketError> {
        self.observed_connect().await.map(|(_, token)| token)
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
    params: &Value,
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

async fn write_line_guarded(
    stream: &mut UnixStream,
    line: &[u8],
    retirement: &mut watch::Receiver<bool>,
) -> Result<(), SocketError> {
    let mut sent = 0;
    while sent < line.len() {
        let write = tokio::select! {
            biased;
            changed = retirement.changed() => {
                let _ = changed;
                return Err(SocketError::RuntimeRetired {
                    delivery: delivery_after(sent),
                });
            }
            write = stream.write(&line[sent..]) => write,
        };
        match write {
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
    tokio::select! {
        biased;
        changed = retirement.changed() => {
            let _ = changed;
            Err(SocketError::RuntimeRetired {
                delivery: DeliveryState::MayHaveReachedHost,
            })
        }
        result = stream.flush() => result.map_err(|source| SocketError::Write {
            delivery: DeliveryState::MayHaveReachedHost,
            source,
        }),
    }
}

/// Credentials the OS reports for the peer of one Herdr connection. They are advisory
/// defense-in-depth beside the socket-file device/inode boundary: a replacement server
/// rebound to the same path normally compares unequal through its fresh inode, but
/// inode numbers may be recycled, so peer evidence is never continuity proof alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PeerIdentity {
    uid: u32,
    gid: u32,
    pid: Option<u32>,
}

impl PeerIdentity {
    fn capture(stream: &UnixStream) -> io::Result<Self> {
        let credentials = stream.peer_cred()?;
        Ok(Self {
            uid: credentials.uid(),
            gid: credentials.gid(),
            pid: credentials.pid().and_then(|pid| u32::try_from(pid).ok()),
        })
    }
}

fn live_server_id(
    device: u64,
    inode: u64,
    peer: PeerIdentity,
    protocol: u64,
    version: &str,
) -> String {
    let peer = match peer.pid {
        Some(pid) => format!("peer-pid-{pid}-uid-{}-gid-{}", peer.uid, peer.gid),
        None => format!("peer-uid-{}-gid-{}-nopid", peer.uid, peer.gid),
    };
    format!("herdr/dev:{device}-ino:{inode}/proto:{protocol}/ver:{version}/{peer}")
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
pub(crate) fn parse_response(
    response: &Value,
    expected_id: &str,
) -> Result<HerdrResponse, SocketError> {
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
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{UnixListener, UnixStream},
    };

    use super::*;

    fn ping() -> &'static MethodMetadata {
        crate::generated::method_metadata("ping").unwrap()
    }

    fn listen(temp: &TempDir) -> (UnixListener, PathBuf) {
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

    async fn answer_ping(listener: Arc<UnixListener>, version: &'static str) {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, id) = read_request(stream).await;
        reader
            .write_all(
                format!(
                    "{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\",\"protocol\":20,\"version\":\"{version}\"}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    async fn accept_zero_bytes(listener: UnixListener) -> Vec<u8> {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut bytes))
            .await
            .expect("mismatched observed stream closes promptly")
            .unwrap();
        bytes
    }

    #[tokio::test]
    async fn accepts_one_correlated_response_and_eof() {
        let temp = TempDir::new().unwrap();
        let (listener, path) = listen(&temp);
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
        let (listener, path) = listen(&temp);
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
        let (listener, path) = listen(&temp);
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
        let (listener, path) = listen(&temp);
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (_reader, _id) = read_request(stream).await;
            // Accept the mutation bytes, then never answer: the client must give
            // up waiting without ever replaying the request.
            tokio::time::sleep(std::time::Duration::from_hours(1)).await;
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
    async fn establishment_ping_binds_response_and_token_to_one_observed_stream() {
        use std::os::unix::fs::MetadataExt;

        let temp = TempDir::new().unwrap();
        let (listener, path) = listen(&temp);
        let metadata = std::fs::metadata(&path).unwrap();
        let listener = Arc::new(listener);
        let server = tokio::spawn(answer_ping(Arc::clone(&listener), "server-a"));
        let client = HerdrSocketClient::new(path);

        let (response, token) = client.establish_unary(ping(), json!({})).await.unwrap();
        server.await.unwrap();

        assert_eq!(
            response,
            HerdrResponse::Success(json!({
                "type": "pong",
                "protocol": 20,
                "version": "server-a"
            }))
        );
        assert_eq!(token.socket_file.device, metadata.dev());
        assert_eq!(token.socket_file.inode, metadata.ino());
        assert!(
            !token
                .live_server_id(20, "server-a")
                .contains("peer-unavailable"),
            "the establishing stream contributes required peer credentials"
        );
        drop(listener);
    }

    #[tokio::test]
    async fn expected_unary_rejects_rebound_socket_before_writing() {
        let temp = TempDir::new().unwrap();
        let (listener_a, path) = listen(&temp);
        let listener_a = Arc::new(listener_a);
        let server_a = tokio::spawn(answer_ping(Arc::clone(&listener_a), "server-a"));
        let client = HerdrSocketClient::new(path.clone());
        let (_, expected) = client.establish_unary(ping(), json!({})).await.unwrap();
        server_a.await.unwrap();

        std::fs::remove_file(&path).unwrap();
        let listener_b = UnixListener::bind(&path).unwrap();
        let server_b = tokio::spawn(accept_zero_bytes(listener_b));
        let error = client
            .unary_on_expected_token(ping(), json!({}), &expected)
            .await
            .unwrap_err();

        assert!(matches!(error, SocketError::EndpointReplaced { .. }));
        assert_eq!(error.delivery(), DeliveryState::NotSent);
        assert!(
            server_b.await.unwrap().is_empty(),
            "replacement receives a connection close with zero request bytes"
        );
        drop(listener_a);
    }

    #[tokio::test]
    async fn observed_connect_rejects_pre_stat_connect_post_stat_rebind_without_write() {
        let temp = TempDir::new().unwrap();
        let (listener_a, path) = listen(&temp);
        let hook = Arc::new(ObservedConnectHook::new());
        let client = Arc::new(HerdrSocketClient::new(path.clone()));
        client.set_observed_connect_hook(Some(Arc::clone(&hook)));
        let connecting = tokio::spawn({
            let client = Arc::clone(&client);
            async move { client.establish_unary(ping(), json!({})).await }
        });
        tokio::time::timeout(Duration::from_secs(2), hook.entered.notified())
            .await
            .expect("connect pauses after the pre-stat observation");

        std::fs::remove_file(&path).unwrap();
        let listener_b = UnixListener::bind(&path).unwrap();
        let server_b = tokio::spawn(accept_zero_bytes(listener_b));
        client.set_observed_connect_hook(None);
        hook.release.notify_one();
        let error = connecting
            .await
            .expect("connect task joins")
            .expect_err("pre/post socket identity mismatch rejects establishment");

        assert!(matches!(error, SocketError::EndpointReplaced { .. }));
        assert_eq!(error.delivery(), DeliveryState::NotSent);
        assert!(
            server_b.await.unwrap().is_empty(),
            "the post-stat mismatch drops the stream before request write"
        );
        drop(listener_a);
    }
}
