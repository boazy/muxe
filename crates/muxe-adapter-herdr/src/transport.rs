use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
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
            Self::StreamingMethod { .. } | Self::RequestTooLarge | Self::RequestIdExhausted => {
                DeliveryState::NotSent
            }
            Self::Connect { .. } => DeliveryState::NotSent,
            Self::Write { delivery, .. }
            | Self::EarlyEof { delivery }
            | Self::ResponseTooLarge { delivery }
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
        if metadata.transport != MethodTransport::Unary {
            return Err(SocketError::StreamingMethod {
                method: metadata.method.to_owned(),
            });
        }
        let id = self.next_id()?;
        let request = json!({
            "id": id,
            "method": metadata.method,
            "params": params,
        });
        let encoded = serde_json::to_vec(&request).map_err(|source| SocketError::InvalidJson {
            delivery: DeliveryState::NotSent,
            source,
        })?;
        if encoded.len() + 1 > MAX_MESSAGE_BYTES {
            return Err(SocketError::RequestTooLarge);
        }

        let mut stream = UnixStream::connect(&self.socket)
            .await
            .map_err(|source| SocketError::Connect {
                socket: self.socket.clone(),
                source,
            })?;
        let mut sent = 0;
        let mut line = encoded;
        line.push(b'\n');
        while sent < line.len() {
            match stream.write(&line[sent..]).await {
                Ok(0) => {
                    return Err(SocketError::Write {
                        delivery: delivery_after(sent),
                        source: io::Error::new(io::ErrorKind::WriteZero, "Herdr socket accepted zero bytes"),
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
        })?;

        let mut reader = BufReader::new(stream);
        let response_line = read_response_line(&mut reader).await?;
        let response = serde_json::from_slice(&response_line).map_err(|source| SocketError::InvalidJson {
            delivery: DeliveryState::MayHaveReachedHost,
            source,
        })?;
        let outcome = parse_response(response, &id)?;
        reject_trailing_data(&mut reader).await?;
        Ok(outcome)
    }

    fn next_id(&self) -> Result<String, SocketError> {
        let sequence = self
            .next_request
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map_err(|_| SocketError::RequestIdExhausted)?;
        Ok(format!("muxe-herdr-{}-{sequence}", std::process::id()))
    }
}

fn delivery_after(written: usize) -> DeliveryState {
    if written == 0 {
        DeliveryState::NotSent
    } else {
        DeliveryState::MayHaveReachedHost
    }
}

async fn read_response_line(reader: &mut BufReader<UnixStream>) -> Result<Vec<u8>, SocketError> {
    let mut response = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(|source| SocketError::Read {
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

fn parse_response(response: Value, expected_id: &str) -> Result<HerdrResponse, SocketError> {
    let object = response.as_object().ok_or_else(|| protocol("response must be a JSON object"))?;
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
        (None, None) => Err(protocol("response must contain exactly one of result or error")),
    }
}

async fn reject_trailing_data(reader: &mut BufReader<UnixStream>) -> Result<(), SocketError> {
    if !reader.buffer().is_empty() {
        return Err(protocol("response has trailing bytes"));
    }
    let mut byte = [0_u8; 1];
    let bytes = reader.read(&mut byte).await.map_err(|source| SocketError::Read {
        delivery: DeliveryState::MayHaveReachedHost,
        source,
    })?;
    if bytes == 0 {
        Ok(())
    } else {
        Err(protocol("response has trailing bytes"))
    }
}

fn required_string<'a>(object: &'a Map<String, Value>, field: &str) -> Result<&'a str, SocketError> {
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
                .write_all(format!("{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\"}}}}\n").as_bytes())
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
}
