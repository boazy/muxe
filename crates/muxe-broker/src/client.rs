use std::{
    collections::VecDeque,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use muxe_protocol::{
    ArchivedFrame, BrokerEvent, BrokerResponse, ClientRequest, ConnectionDecoder, ConnectionPolicy,
    DecodeError, Hello, LiveServerIdentity, PeerRole, Prelude, RequestId, SchemaFingerprint,
    WireMessage, encode_frame,
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

/// Sequential request client for the broker's owner-only Unix socket. It preserves response
/// archives so the UI can retain a checked `UiAttached` snapshot without rebuilding a menu graph.
pub struct BrokerClient {
    stream: UnixStream,
    decoder: ConnectionDecoder,
    frames: VecDeque<ArchivedFrame>,
    events: VecDeque<BrokerEvent>,
    next_request: AtomicU64,
}

impl BrokerClient {
    pub async fn connect(
        socket: impl AsRef<Path>,
        role: PeerRole,
        process_version: impl Into<String>,
        live_server: LiveServerIdentity,
    ) -> Result<Self, ClientError> {
        if !matches!(role, PeerRole::Launcher | PeerRole::Ui) {
            return Err(ClientError::UnsupportedRole(role));
        }
        let mut stream = UnixStream::connect(socket).await?;
        stream
            .write_all(&Prelude::rkyv(role, SchemaFingerprint::application()).encode())
            .await?;
        let mut client = Self {
            stream,
            decoder: ConnectionDecoder::new(ConnectionPolicy::client(
                PeerRole::Broker,
                SchemaFingerprint::application(),
            )),
            frames: VecDeque::new(),
            events: VecDeque::new(),
            next_request: AtomicU64::new(1),
        };
        let request_id = client.next_request_id();
        client
            .send(&WireMessage::Hello {
                request_id,
                hello: Hello {
                    process_version: process_version.into(),
                    live_server: live_server.clone(),
                },
            })
            .await?;
        let frame = client.next_frame().await?;
        let message = frame.deserialize()?;
        match message {
            WireMessage::Welcome {
                request_id: response,
                welcome,
            } if response == request_id && welcome.live_server == live_server => Ok(client),
            WireMessage::Welcome { .. } => Err(ClientError::IdentityMismatch),
            _ => Err(ClientError::ExpectedWelcome),
        }
    }

    /// Sends one request and returns its checked response frame. Callers attaching the terminal UI
    /// should pass this frame directly to `UiRuntime::attach` rather than deserialize it.
    pub async fn request_frame(
        &mut self,
        request: ClientRequest,
    ) -> Result<ArchivedFrame, ClientError> {
        let request_id = self.next_request_id();
        self.send(&WireMessage::Request {
            request_id,
            request,
        })
        .await?;
        loop {
            let frame = self.next_frame().await?;
            match frame.deserialize()? {
                WireMessage::Response {
                    request_id: response,
                    ..
                } if response == request_id => return Ok(frame),
                WireMessage::Response {
                    request_id: response,
                    ..
                } => {
                    return Err(ClientError::UnexpectedResponse {
                        expected: request_id,
                        actual: response,
                    });
                }
                WireMessage::Event { event, .. } => self.events.push_back(event),
                _ => return Err(ClientError::ExpectedResponse),
            }
        }
    }

    pub async fn request(&mut self, request: ClientRequest) -> Result<BrokerResponse, ClientError> {
        let frame = self.request_frame(request).await?;
        let message = frame.deserialize()?;
        match message {
            WireMessage::Response { response, .. } => Ok(response),
            _ => Err(ClientError::ExpectedResponse),
        }
    }

    /// Receives the next broker event without dropping events that arrived while a correlated
    /// request response was in flight. Callers invoke this only when they have no request pending.
    pub async fn next_event(&mut self) -> Result<BrokerEvent, ClientError> {
        if let Some(event) = self.events.pop_front() {
            return Ok(event);
        }
        let frame = self.next_frame().await?;
        match frame.deserialize()? {
            WireMessage::Event { event, .. } => Ok(event),
            WireMessage::Response { .. } => Err(ClientError::ExpectedEvent),
            _ => Err(ClientError::ExpectedEvent),
        }
    }

    async fn send(&mut self, message: &WireMessage) -> Result<(), ClientError> {
        let frame = encode_frame(message)?;
        self.stream.write_all(frame.prefix()).await?;
        self.stream.write_all(frame.payload()).await?;
        Ok(())
    }

    async fn next_frame(&mut self) -> Result<ArchivedFrame, ClientError> {
        if let Some(frame) = self.frames.pop_front() {
            return Ok(frame);
        }
        let mut bytes = [0; 16 * 1024];
        loop {
            let count = self.stream.read(&mut bytes).await?;
            if count == 0 {
                return Err(ClientError::ConnectionClosed);
            }
            self.decoder
                .push(&bytes[..count], |frame| self.frames.push_back(frame))?;
            if let Some(frame) = self.frames.pop_front() {
                return Ok(frame);
            }
        }
    }

    fn next_request_id(&self) -> RequestId {
        let mut bytes = [0; 16];
        bytes[8..].copy_from_slice(
            &self
                .next_request
                .fetch_add(1, Ordering::Relaxed)
                .to_be_bytes(),
        );
        RequestId(bytes)
    }
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid broker frame: {0}")]
    Decode(#[from] DecodeError),
    #[error("peer role {0:?} cannot be a broker client")]
    UnsupportedRole(PeerRole),
    #[error("broker accepted a different live-server identity")]
    IdentityMismatch,
    #[error("broker closed the connection before a complete response")]
    ConnectionClosed,
    #[error("broker did not send its welcome response")]
    ExpectedWelcome,
    #[error("broker did not send a request response")]
    ExpectedResponse,
    #[error("received response for unexpected request ID (expected {expected:?}, got {actual:?})")]
    UnexpectedResponse {
        expected: RequestId,
        actual: RequestId,
    },
    #[error("expected broker event while no request was pending")]
    ExpectedEvent,
}

