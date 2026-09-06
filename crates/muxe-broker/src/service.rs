use std::{collections::HashSet, fs, io, sync::Arc, time::Duration};

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use nix::unistd::Uid;
use muxe_protocol::{
    ArchivedFrame, BrokerResponse, ClientRequest, ConnectionDecoder, ConnectionPolicy,
    DecodeError, PeerRole, PendingLaunchToken, Prelude, RequestId, SchemaFingerprint, UiSessionId,
    WireMessage, encode_frame,
};
use notify::{RecursiveMode, Watcher};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{mpsc, watch},
    task::JoinHandle,
};

use crate::{Broker, BrokerError, RequestResult, RuntimeEndpoint, RuntimeError};


pub struct BrokerServer {
    broker: Arc<Broker>,
    endpoint: RuntimeEndpoint,
    listener: UnixListener,
    socket_device: u64,
    socket_inode: u64,
    _config_watch: ConfigWatch,
}

impl BrokerServer {
    pub async fn start(
        broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
    ) -> Result<Self, ServerError> {
        let startup_lock = endpoint.acquire_startup_lock()?;
        endpoint.remove_validated_stale_socket()?;
        let listener = endpoint.bind_listener()?;
        let socket_metadata = fs::symlink_metadata(endpoint.socket())?;
        if !socket_metadata.file_type().is_socket() {
            return Err(io::Error::other("broker listener path is not a Unix socket").into());
        }
        let config_watch = ConfigWatch::start(Arc::clone(&broker)).await?;
        // Binding the owner-only listener and installing the watcher make this server ready for
        // the handshake. The lock serializes only that startup transaction, never its lifetime.
        drop(startup_lock);
        Ok(Self {
            broker,
            endpoint,
            listener,
            socket_device: socket_metadata.dev(),
            socket_inode: socket_metadata.ino(),
            _config_watch: config_watch,
        })
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    /// Serves the owner-only socket until the supplied shutdown signal changes to true.
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        let health = tokio::spawn(Arc::clone(&self.broker).monitor(shutdown.clone()));
        let mut expiry = tokio::time::interval(Duration::from_millis(100));
        let result = loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break Ok(());
                    }
                }
                _ = expiry.tick() => self.broker.expire_pending().await,
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted.map_err(ServerError::Io)?;
                    let broker = Arc::clone(&self.broker);
                    tokio::spawn(async move {
                        if let Err(error) = serve_connection(broker, stream).await {
                            tracing::debug!(%error, "broker client disconnected");
                        }
                    });
                }
            }
        };
        health.abort();
        result
    }
}
impl Drop for BrokerServer {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(self.endpoint.socket()) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.socket_device
            && metadata.ino() == self.socket_inode
        {
            let _ = fs::remove_file(self.endpoint.socket());
        }
    }
}

struct ConnectionResources {
    role: PeerRole,
    handshaken: bool,
    request_ids: HashSet<RequestId>,
    launcher_tokens: HashSet<PendingLaunchToken>,
    attached_session: Option<UiSessionId>,
    pending_ui_token: Option<PendingLaunchToken>,
}

impl ConnectionResources {
    fn new(role: PeerRole) -> Self {
        Self {
            role,
            handshaken: false,
            request_ids: HashSet::new(),
            launcher_tokens: HashSet::new(),
            attached_session: None,
            pending_ui_token: None,
        }
    }

    fn validate_request(&self, request: &ClientRequest) -> Result<(), &'static str> {
        match request {
            ClientRequest::AttachUi(_) if self.role == PeerRole::Ui => self
                .attached_session
                .is_none()
                .then_some(())
                .ok_or("one UI connection may attach at most one session"),
            ClientRequest::InvokeBinding(request) if self.role == PeerRole::Ui => self
                .attached_session
                .as_ref()
                .is_some_and(|session| session == &request.session)
                .then_some(())
                .ok_or("UI connection does not own the invoked session"),
            ClientRequest::MenuControl(request) if self.role == PeerRole::Ui => self
                .attached_session
                .as_ref()
                .is_some_and(|session| session == &request.session)
                .then_some(())
                .ok_or("UI connection does not own the controlled session"),
            ClientRequest::DetachUi(request) if self.role == PeerRole::Ui => self
                .attached_session
                .as_ref()
                .is_some_and(|session| session == &request.session)
                .then_some(())
                .ok_or("UI connection does not own the detached session"),
            ClientRequest::RegisterPendingPane(request) if self.role == PeerRole::Launcher => self
                .launcher_tokens
                .contains(&request.token)
                .then_some(())
                .ok_or("launcher connection does not own the pending launch token"),
            ClientRequest::CommitUiLaunch(request) if self.role == PeerRole::Launcher => self
                .launcher_tokens
                .contains(&request.token)
                .then_some(())
                .ok_or("launcher connection does not own the pending launch token"),
            ClientRequest::AbortUiLaunch(request) if self.role == PeerRole::Launcher => self
                .launcher_tokens
                .contains(&request.token)
                .then_some(())
                .ok_or("launcher connection does not own the pending launch token"),
            _ => Ok(()),
        }
    }

    fn record_response(&mut self, request: &ClientRequest, response: &BrokerResponse) {
        match (request, response) {
            (ClientRequest::PrepareUiLaunch(_), BrokerResponse::LaunchPrepared { token, .. }) => {
                self.launcher_tokens.insert(*token);
            }
            (ClientRequest::AttachUi(_), BrokerResponse::UiAttached { session, .. }) => {
                self.attached_session = Some(session.clone());
            }
            (ClientRequest::CommitUiLaunch(request), BrokerResponse::Acknowledged) => {
                self.launcher_tokens.remove(&request.token);
            }
            (ClientRequest::AbortUiLaunch(request), BrokerResponse::Acknowledged) => {
                self.launcher_tokens.remove(&request.token);
            }
            _ => {}
        }
    }

    fn record_pending_attachment(
        &mut self,
        request: &ClientRequest,
        session: UiSessionId,
    ) -> Result<(), &'static str> {
        let ClientRequest::AttachUi(request) = request else {
            return Err("only AttachUi may wait for a launch commit");
        };
        if self.attached_session.is_some() {
            return Err("one UI connection may attach at most one session");
        }
        self.attached_session = Some(session);
        self.pending_ui_token = request.pending_launch;
        Ok(())
    }
}

async fn serve_connection(broker: Arc<Broker>, mut stream: UnixStream) -> Result<(), ServerError> {
    verify_same_user_peer(&stream)?;
    let mut prelude = [0; muxe_protocol::PRELUDE_LEN];
    stream.read_exact(&mut prelude).await?;
    let received = Prelude::decode(prelude)?;
    let role = received.role;
    if !matches!(role, PeerRole::Launcher | PeerRole::Ui) {
        return Err(ServerError::UnsupportedRole(role));
    }
    let mut decoder = ConnectionDecoder::new(ConnectionPolicy::broker(
        role,
        SchemaFingerprint::application(),
    ));
    let mut initial = Vec::new();
    decoder.push(&prelude, |frame| initial.push(frame))?;

    let (mut reader, mut writer) = stream.into_split();
    writer
        .write_all(&Prelude::rkyv(PeerRole::Broker, SchemaFingerprint::application()).encode())
        .await?;
    let (outbox, mut outbound) = mpsc::channel::<WireMessage>(32);
    let writer_task = tokio::spawn(async move {
        while let Some(message) = outbound.recv().await {
            write_message(&mut writer, &message).await?;
        }
        Ok::<(), io::Error>(())
    });

    let mut resources = ConnectionResources::new(role);
    let result = async {
        process_frames(&broker, &outbox, &mut resources, initial).await?;
        let mut bytes = [0; 16 * 1024];
        loop {
            let read = reader.read(&mut bytes).await?;
            if read == 0 {
                return Ok(());
            }
            let mut frames = Vec::new();
            decoder.push(&bytes[..read], |frame| frames.push(frame))?;
            process_frames(&broker, &outbox, &mut resources, frames).await?;
        }
    }
    .await;
    disconnect_resources(&broker, &mut resources).await;
    drop(outbox);
    writer_task.abort();
    result
}

async fn process_frames(
    broker: &Arc<Broker>,
    outbox: &mpsc::Sender<WireMessage>,
    resources: &mut ConnectionResources,
    frames: Vec<ArchivedFrame>,
) -> Result<(), ServerError> {
    for frame in frames {
        let message = frame.deserialize()?;
        match message {
            WireMessage::Hello { request_id, hello } => {
                if resources.handshaken || !broker.serves_identity(&hello.live_server).await? {
                    return Err(ServerError::IdentityMismatch);
                }
                resources.handshaken = true;
                outbox
                    .send(WireMessage::Welcome {
                        request_id,
                        welcome: muxe_protocol::Welcome {
                            broker_version: env!("CARGO_PKG_VERSION").to_owned(),
                            live_server: broker.live_identity().await?,
                            accepted_frame_len: muxe_protocol::MAX_FRAME_LEN,
                        },
                    })
                    .await
                    .map_err(|_| ServerError::WriterClosed)?;
            }
            WireMessage::Request {
                request_id,
                request,
            } => {
                if !resources.handshaken {
                    return Err(ServerError::UnexpectedMessage);
                }
                if !resources.request_ids.insert(request_id.clone()) {
                    return Err(ServerError::DuplicateRequestId);
                }
                if let Err(message) = resources.validate_request(&request) {
                    outbox
                        .send(WireMessage::Response {
                            request_id,
                            response: ownership_error(message),
                        })
                        .await
                        .map_err(|_| ServerError::WriterClosed)?;
                    continue;
                }
                let tracking = request.clone();
                match broker.handle(resources.role, request, outbox.clone()).await {
                    Ok(RequestResult::Immediate(response)) => {
                        resources.record_response(&tracking, &response);
                        outbox
                            .send(WireMessage::Response {
                                request_id,
                                response,
                            })
                            .await
                            .map_err(|_| ServerError::WriterClosed)?;
                    }
                    Ok(RequestResult::WaitForAttachment(pending)) => {
                        resources
                            .record_pending_attachment(&tracking, pending.session().clone())
                            .map_err(|_| ServerError::UnexpectedMessage)?;
                        let outbox = outbox.clone();
                        tokio::spawn(async move {
                            let _ = outbox
                                .send(WireMessage::Response {
                                    request_id,
                                    response: pending.wait().await,
                                })
                                .await;
                        });
                    }
                    Err(error) => {
                        outbox
                            .send(WireMessage::Response {
                                request_id,
                                response: BrokerResponse::Error(error_diagnostic(error)),
                            })
                            .await
                            .map_err(|_| ServerError::WriterClosed)?;
                    }
                }
            }
            WireMessage::Welcome { .. }
            | WireMessage::Response { .. }
            | WireMessage::Event { .. } => return Err(ServerError::UnexpectedMessage),
        }
    }
    Ok(())
}

async fn disconnect_resources(broker: &Arc<Broker>, resources: &mut ConnectionResources) {
    if let Some(token) = resources.pending_ui_token.take() {
        let _ = broker.abort(token).await;
    } else {
        broker.disconnect(resources.attached_session.as_ref()).await;
    }
    for token in resources.launcher_tokens.drain() {
        let _ = broker.abort(token).await;
    }
}

fn verify_same_user_peer(stream: &UnixStream) -> Result<(), ServerError> {
    let peer = stream.peer_cred()?;
    (peer.uid() == Uid::current().as_raw())
        .then_some(())
        .ok_or(ServerError::PeerCredential)
}

fn ownership_error(message: &str) -> BrokerResponse {
    BrokerResponse::Error(muxe_protocol::ProtocolDiagnostic {
        code: muxe_protocol::DiagnosticCode::ProtocolViolation,
        message: message.to_owned(),
    })
}

async fn write_message(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    message: &WireMessage,
) -> Result<(), io::Error> {
    let frame = encode_frame(message).map_err(io::Error::other)?;
    writer.write_all(frame.prefix()).await?;
    writer.write_all(frame.payload()).await
}

fn error_diagnostic(error: BrokerError) -> muxe_protocol::ProtocolDiagnostic {
    let message = error.to_string();
    let message = if message.len() <= muxe_protocol::MAX_DIAGNOSTIC_LEN {
        message
    } else {
        let mut end = muxe_protocol::MAX_DIAGNOSTIC_LEN;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message[..end].to_owned()
    };
    muxe_protocol::ProtocolDiagnostic {
        code: muxe_protocol::DiagnosticCode::InvalidRequest,
        message,
    }
}

pub struct ConfigWatch {
    _watcher: Option<notify::RecommendedWatcher>,
    task: Option<JoinHandle<()>>,
}

impl ConfigWatch {
    async fn start(broker: Arc<Broker>) -> Result<Self, ServerError> {
        let mut spec = broker.config_watch_spec().await;
        if !spec.settings.watch {
            return Ok(Self {
                _watcher: None,
                task: None,
            });
        }
        let (changed, mut changes) = mpsc::channel(8);
        let watched_inputs = spec
            .inputs
            .iter()
            .map(|path| canonical_watch_path(path))
            .collect::<Vec<_>>();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
                Ok(event) => {
                    if event.paths.iter().any(|changed_path| {
                        let changed_path = canonical_watch_path(changed_path);
                        watched_inputs.iter().any(|input| {
                            input == &changed_path
                                || changed_path.starts_with(input)
                                || input.starts_with(&changed_path)
                        })
                    }) {
                        let _ = changed.try_send(());
                    }
                }
                Err(error) => tracing::warn!(%error, "configuration watcher failed"),
            })
            .map_err(ServerError::Watch)?;
        watcher
            .watch(&spec.root, RecursiveMode::Recursive)
            .map_err(ServerError::Watch)?;
        let task = tokio::spawn(async move {
            while changes.recv().await.is_some() {
                tokio::time::sleep(spec.settings.debounce).await;
                while changes.try_recv().is_ok() {}
                match broker.reload().await {
                    Ok(_) => {
                        spec = broker.config_watch_spec().await;
                        if !spec.settings.watch {
                            return;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "configuration reload rejected; keeping active generation");
                    }
                }
            }
        });
        Ok(Self {
            _watcher: Some(watcher),
            task: Some(task),
        })
    }
}

impl Drop for ConfigWatch {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn canonical_watch_path(path: &std::path::Path) -> std::path::PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| {
        path.parent()
            .and_then(|parent| fs::canonicalize(parent).ok())
            .zip(path.file_name())
            .map(|(parent, name)| parent.join(name))
            .unwrap_or_else(|| path.to_path_buf())
    })
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("runtime endpoint error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid IPC frame: {0}")]
    Decode(#[from] DecodeError),
    #[error("broker operation failed: {0}")]
    Broker(#[from] BrokerError),
    #[error("client role {0:?} cannot connect to the broker socket")]
    UnsupportedRole(PeerRole),
    #[error("client live-host identity does not match the active broker adapter")]
    IdentityMismatch,
    #[error("client sent a broker-to-peer message")]
    UnexpectedMessage,
    #[error("broker socket peer credentials do not belong to the current user")]
    PeerCredential,
    #[error("client reused a request ID on one connection")]
    DuplicateRequestId,
    #[error("client connection closed while response was queued")]
    WriterClosed,
    #[error("cannot watch configuration path {0}")]
    WatchPath(std::path::PathBuf),

    #[error("configuration watcher failed: {0}")]
    Watch(#[source] notify::Error),
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use crate::BrokerClient;

    use async_trait::async_trait;
    use muxe_adapter_api::{
        AdapterCapabilities, AdapterError, AdapterHealthEvent, CaptureLease, CaptureReleaseReason,
        CaptureRequest, DispatchAccepted, ExecutionCorrelationId, HostAdapter, HostIdentity,
        KeyboardCapabilities, ModalScopeId, NativeDispatchRequest, OriginCaptureRequest,
        PendingPaneRegistration, PortableDispatchRequest,
    };
    use muxe_core::{
        ActionValidation, ActionValidator, CompiledGeneration, ConfigDiagnostic, KeyCapabilities,
        OriginContext, OriginHostKind, OriginInvocationSource, PaneId, ServerId, SourceId,
    };
    use muxe_protocol::{
        AttachUi, BrokerResponse, ClientRequest, HostKind, HostPaneId, LiveServerIdentity,
        PeerRole, Prelude, SchemaFingerprint, ServerId as WireServerId,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
        sync::watch,
    };

    use super::*;

    struct SmokeAdapter;

    impl ActionValidator for SmokeAdapter {
        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _action_span: &muxe_core::SourceSpan,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
            Ok(vec![
                ActionValidation {
                    execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
                };
                candidates.len()
            ])
        }
    }

    #[async_trait]
    impl HostAdapter for SmokeAdapter {
        async fn identity(&self) -> Result<HostIdentity, AdapterError> {
            Ok(HostIdentity {
                kind: muxe_adapter_api::HostKind::Herdr,
                discovery_key: "owned-fake-host".to_owned(),
                live_server_id: "owned-fake-server".to_owned(),
            })
        }

        async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
            Ok(AdapterCapabilities {
                keyboard: KeyboardCapabilities {
                    kitty_baseline: false,
                    kitty_event_types: false,
                    kitty_alternate_keys: false,
                    kitty_all_keys_as_escape_codes: false,
                },
                supports_capture: false,
                supports_notifications: false,
                supports_native_cancellation: false,
            })
        }

        async fn modal_scope(&self, _ui_pane: &PaneId) -> Result<ModalScopeId, AdapterError> {
            Ok(ModalScopeId::new("owned-fake-scope"))
        }

        async fn begin_capture(
            &self,
            _request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            unreachable!("smoke adapter declares no capture support")
        }

        async fn end_capture(
            &self,
            _lease: CaptureLease,
            _reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn close_pending_pane(
            &self,
            _registration: PendingPaneRegistration,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn capture_origin(
            &self,
            _request: OriginCaptureRequest,
        ) -> Result<OriginContext, AdapterError> {
            Ok(OriginContext {
                host_kind: OriginHostKind::Herdr,
                server_id: ServerId::new("owned-fake-server"),
                client_id: None,
                session_id: None,
                workspace_id: None,
                tab_id: None,
                tab_index: None,
                pane_id: Some(PaneId::new("owned-ui-pane")),
                pane_type: None,
                pane_cwd: None,
                selection_text: None,
                invocation_source: OriginInvocationSource::RootBinding,
                worktree_id: None,
                worktree_path: None,
                agent_id: None,
                link_url: None,
                link_handler_id: None,
            })
        }

        async fn dispatch_portable(
            &self,
            request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("owned-fake-portable"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn dispatch_native(
            &self,
            request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("owned-fake-native"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn cancel(
            &self,
            _execution: muxe_core::ExecutionId,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
            std::future::pending().await
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn owned_unix_server_accepts_and_detaches_a_fake_host_ui() {
        let directory = tempfile::tempdir().expect("owned broker runtime directory");
        let config_path = directory.path().join("config.yml");
        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
        )
        .expect("write owned broker config");
        let adapter = Arc::new(SmokeAdapter);
        let config_source = std::fs::read_to_string(&config_path).expect("read owned broker config");
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<owned broker smoke>"),
            config_source,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("compile owned broker config");
        let broker = Broker::from_compiled(adapter, &config_path, config);
        let endpoint = RuntimeEndpoint::in_runtime_dir(
            directory.path(),
            HostKind::Herdr,
            "owned-fake-host",
        )
        .expect("derive owned endpoint");
        let live_server = broker.live_identity().await.expect("fake host identity");
        let server = BrokerServer::start(Arc::clone(&broker), endpoint.clone())
            .await
            .expect("start owned broker listener");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server_task = tokio::spawn(server.run(shutdown_rx));

        let client_identity = LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: "owned-fake-host".to_owned(),
            server_id: WireServerId::new(live_server.server_id.as_str()),
        };
        let mut client = BrokerClient::connect(
            endpoint.socket(),
            PeerRole::Ui,
            "owned-smoke-ui",
            client_identity.clone(),
        )
        .await
        .expect("connect over the owned broker socket");
        let attached = client
            .request(ClientRequest::AttachUi(AttachUi {
                root: muxe_protocol::MenuId::new("main"),
                pane: HostPaneId::new("owned-ui-pane"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }))
            .await
            .expect("attach fake-host UI over broker IPC");
        let BrokerResponse::UiAttached { session, .. } = attached else {
            panic!("owned fake-host UI must attach before terminal setup");
        };
        let mut intruder = BrokerClient::connect(
            endpoint.socket(),
            PeerRole::Ui,
            "owned-intruder-ui",
            client_identity.clone(),
        )
        .await
        .expect("connect second owned UI peer");
        assert!(matches!(
            intruder
                .request(ClientRequest::DetachUi(muxe_protocol::DetachUi {
                    session: session.clone(),
                }))
                .await
                .expect("cross-session request receives a local protocol error"),
            BrokerResponse::Error(muxe_protocol::ProtocolDiagnostic {
                code: muxe_protocol::DiagnosticCode::ProtocolViolation,
                ..
            })
        ));
        assert!(matches!(
            client
                .request(ClientRequest::Heartbeat)
                .await
                .expect("valid attached peer survives cross-session rejection"),
            BrokerResponse::Acknowledged
        ));
        drop(intruder);

        let mut malformed = UnixStream::connect(endpoint.socket())
            .await
            .expect("connect isolated malformed peer");
        malformed
            .write_all(&Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application()).encode())
            .await
            .expect("write malformed peer prelude");
        let mut broker_prelude = [0; muxe_protocol::PRELUDE_LEN];
        malformed
            .read_exact(&mut broker_prelude)
            .await
            .expect("read broker prelude before malformed frame");
        malformed
            .write_all(&0_u32.to_be_bytes())
            .await
            .expect("write invalid zero-length frame");
        let mut eof = [0; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), malformed.read(&mut eof))
                .await
                .expect("malformed peer is closed")
                .expect("read malformed peer closure"),
            0,
            "malformed connection closes only itself"
        );
        assert!(matches!(
            client
                .request(ClientRequest::Heartbeat)
                .await
                .expect("valid peer survives malformed peer closure"),
            BrokerResponse::Acknowledged
        ));
        assert!(matches!(
            client
                .request(ClientRequest::DetachUi(muxe_protocol::DetachUi { session }))
                .await
                .expect("detach over broker IPC"),
            BrokerResponse::Detached
        ));

        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: updated\n        action: menu:quit\n",
        )
        .expect("replace watched owned broker config");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if broker.generation().await == CompiledGeneration(2) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("recursive watcher publishes a new immutable generation");

        shutdown_tx.send(true).expect("stop owned broker listener");
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("owned broker server exits")
            .expect("broker task joins")
            .expect("broker server has no error");
        assert!(
            !endpoint.socket().exists(),
            "owned broker listener unlinks only its own endpoint on shutdown"
        );
    }
}
