use std::{
    collections::HashMap,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};

use muxe_adapter_api::{
    AdapterError, AdapterHealthEvent, CaptureLease, CaptureLossReason, CaptureReleaseReason,
    CaptureRequest, DispatchCompletion, HostAdapter, HostCallerIdentity, OriginCaptureRequest,
    OriginHintSource, PendingPaneRegistration, PortableDispatchRequest, ResolvedNativeAction,
    ResolvedPortableAction, UntrustedOriginHint,
};
use muxe_core::{
    ActionSpec, CommandAction, CompiledConfig, CompiledGeneration, ConfigAction,
    ExecutionId as CoreExecutionId, ExecutionPolicy, MenuAction, PaneId, TimeoutAction,
};
use muxe_protocol::{
    AbortUiLaunch, AttachUi, BrokerEvent, BrokerResponse, ClientRequest, DiagnosticCode, EventId,
    ExecutionId, ExecutionOutcome, HostKind, HostPaneId, InvocationDisposition, InvokeBinding,
    LiveServerIdentity, MenuControl, ModalScopeId, PendingLaunchToken, ProtocolDiagnostic,
    RegisterPendingPane, UiMenuControl, UiSessionId, WireMessage,
};
use thiserror::Error;
use tokio::{
    process::{Child, Command},
    sync::{Mutex, mpsc, watch},
};

use crate::{
    config::{ConfigError, ConfigStore, ConfigWatchSpec},
    gate::{
        AttachDisposition, GateError, LaunchGate, OsTokenSource, PendingLaunch, RegisteredPane,
        ScopeOwner,
    },
    wire,
};

pub struct Broker {
    adapter: Arc<dyn HostAdapter>,
    config: ConfigStore,
    state: Arc<Mutex<BrokerState>>,
    sessions: Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    generic: Arc<GenericSupervisor>,
    token_source: Mutex<OsTokenSource>,
    next_session: AtomicU64,
    next_execution: AtomicU64,
    next_event: Arc<AtomicU64>,
}

#[derive(Default)]
struct BrokerState {
    gate: LaunchGate,
    pending_sessions: HashMap<PendingLaunchToken, UiSessionId>,
    executions: HashMap<UiSessionId, ExecutionRecord>,
}

struct SessionRecord {
    config: Arc<CompiledConfig>,
    root: muxe_core::MenuId,
    scope: muxe_adapter_api::ModalScopeId,
    origin: muxe_core::OriginContext,
    capture: Option<CaptureLease>,
    readiness: watch::Sender<SessionReadiness>,
    events: mpsc::Sender<muxe_protocol::WireMessage>,
}

#[derive(Clone)]
enum SessionReadiness {
    Pending,
    Ready(BrokerResponse),
    Failed(ProtocolDiagnostic),
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ExecutionOwner {
    Adapter,
    GenericProcess,
}

#[derive(Clone)]
struct ExecutionRecord {
    wire: ExecutionId,
    core: CoreExecutionId,
    cancellable: bool,
    on_menu_control: muxe_core::MenuControlAction,
    owner: ExecutionOwner,
    pending_control: Option<MenuControl>,
}

#[derive(Default)]
struct GenericSupervisor {
    processes: Mutex<HashMap<CoreExecutionId, GenericProcess>>,
}

struct GenericProcess {
    cancellation: watch::Sender<Option<GenericCancellation>>,
}
#[derive(Clone, Copy)]
enum GenericCancellation {
    UserRequested,
}

pub enum RequestResult {
    Immediate(BrokerResponse),
    WaitForAttachment(PendingAttachment),
}

pub struct PendingAttachment {
    session: UiSessionId,
    receiver: watch::Receiver<SessionReadiness>,
}

impl PendingAttachment {

    pub fn session(&self) -> &UiSessionId {
        &self.session
    }
    pub async fn wait(mut self) -> BrokerResponse {
        loop {
            match self.receiver.borrow().clone() {
                SessionReadiness::Ready(response) => return response,
                SessionReadiness::Failed(diagnostic) => return BrokerResponse::Error(diagnostic),
                SessionReadiness::Pending => {}
            }
            if self.receiver.changed().await.is_err() {
                return BrokerResponse::Error(diagnostic(
                    DiagnosticCode::LaunchAborted,
                    "UI launch was cancelled before attachment completed",
                ));
            }
        }
    }
}

impl Broker {
    pub fn from_compiled(
        adapter: Arc<dyn HostAdapter>,
        config_path: impl Into<std::path::PathBuf>,
        config: CompiledConfig,
    ) -> Arc<Self> {
        Arc::new(Self {
            adapter,
            config: ConfigStore::from_compiled(config_path, config),
            state: Arc::new(Mutex::new(BrokerState::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            generic: Arc::new(GenericSupervisor::default()),
            token_source: Mutex::new(OsTokenSource),
            next_session: AtomicU64::new(1),
            next_execution: AtomicU64::new(1),
            next_event: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn load(
        adapter: Arc<dyn HostAdapter>,
        config_path: impl Into<std::path::PathBuf>,
    ) -> Result<Arc<Self>, ConfigError> {
        let config = ConfigStore::load(config_path, adapter.as_ref()).await?;
        Ok(Arc::new(Self {
            adapter,
            config,
            state: Arc::new(Mutex::new(BrokerState::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            generic: Arc::new(GenericSupervisor::default()),
            token_source: Mutex::new(OsTokenSource),
            next_session: AtomicU64::new(1),
            next_execution: AtomicU64::new(1),
            next_event: Arc::new(AtomicU64::new(1)),
        }))
    }

    pub fn config_path(&self) -> &std::path::Path {
        self.config.path()
    }

    pub async fn generation(&self) -> CompiledGeneration {
        self.config.snapshot().await.config.generation
    }

    pub(crate) async fn config_watch_spec(&self) -> ConfigWatchSpec {
        self.config.watch_spec().await
    }


    /// Keeps the previous immutable generation active if parsing, compilation, or active-host
    /// validation fails.
    pub async fn reload(&self) -> Result<CompiledGeneration, BrokerError> {
        self.config
            .reload(self.adapter.as_ref())
            .await
            .map_err(BrokerError::Configuration)
    }

    pub async fn live_identity(&self) -> Result<LiveServerIdentity, BrokerError> {
        let identity = self
            .adapter
            .identity()
            .await
            .map_err(BrokerError::Adapter)?;
        Ok(LiveServerIdentity {
            host: match identity.kind {
                muxe_adapter_api::HostKind::Zellij => HostKind::Zellij,
                muxe_adapter_api::HostKind::Herdr => HostKind::Herdr,
            },
            discovery_key: identity.discovery_key,
            server_id: muxe_protocol::ServerId::new(identity.live_server_id),
        })
    }

    pub async fn serves_identity(&self, claimed: &LiveServerIdentity) -> Result<bool, BrokerError> {
        Ok(self.live_identity().await? == *claimed)
    }



    pub async fn handle(
        &self,
        role: muxe_protocol::PeerRole,
        request: ClientRequest,
        events: mpsc::Sender<muxe_protocol::WireMessage>,
    ) -> Result<RequestResult, BrokerError> {
        match request {
            ClientRequest::PrepareUiLaunch(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.prepare(request).await
            }
            ClientRequest::RegisterPendingPane(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.register(request).await?;
                Ok(RequestResult::Immediate(
                    BrokerResponse::PendingPaneRegistered,
                ))
            }
            ClientRequest::CommitUiLaunch(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.commit(request.token, request.pane).await?;
                Ok(RequestResult::Immediate(BrokerResponse::Acknowledged))
            }
            ClientRequest::AbortUiLaunch(AbortUiLaunch { token }) => {
                self.require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.abort(token).await?;
                Ok(RequestResult::Immediate(BrokerResponse::Acknowledged))
            }
            ClientRequest::AttachUi(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.attach(request, events).await
            }
            ClientRequest::InvokeBinding(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.invoke(request).await
            }
            ClientRequest::MenuControl(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.control(request).await
            }
            ClientRequest::DetachUi(request) => {
                self.require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.detach(&request.session, CaptureReleaseReason::UiDismissed)
                    .await?;
                Ok(RequestResult::Immediate(BrokerResponse::Detached))
            }
            ClientRequest::Heartbeat => Ok(RequestResult::Immediate(BrokerResponse::Acknowledged)),
        }
    }

    pub async fn disconnect(&self, session: Option<&UiSessionId>) {
        if let Some(session) = session {
            let _ = self
                .detach(session, CaptureReleaseReason::UiDismissed)
                .await;
        }
    }

    pub async fn expire_pending(&self) {
        let expired = {
            let mut state = self.state.lock().await;
            state
                .gate
                .expire(Instant::now())
                .into_iter()
                .map(|launch| {
                    let session = state.pending_sessions.remove(&launch.token);
                    (launch, session)
                })
                .collect::<Vec<_>>()
        };
        for (launch, session) in expired {
            if let Some(session) = session {
                self.fail_pending(launch, &session, CaptureReleaseReason::LeaseExpired)
                    .await;
            }
        }
    }

    /// Translates adapter lifecycle and dispatch completions into session-scoped broker events.
    /// Adapter implementations gate host-bound dispatch against their continuity epoch; a health
    /// transition keeps the session's immutable menu/configuration alive for local interaction.
    pub async fn monitor(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                event = self.adapter.next_health_event() => match event {
                    Ok(event) => self.handle_health_event(event).await,
                    Err(error) => {
                        self.broadcast_health(false, Some(error)).await;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    async fn handle_health_event(&self, event: AdapterHealthEvent) {
        match event {
            AdapterHealthEvent::Healthy { .. } | AdapterHealthEvent::CaptureReady { .. } => {
                self.broadcast_health(true, None).await;
            }
            AdapterHealthEvent::Unhealthy { error, .. } => {
                self.broadcast_health(false, Some(error)).await;
            }
            AdapterHealthEvent::Reconnected { .. } => {
                self.broadcast_health(true, None).await;
            }
            AdapterHealthEvent::CaptureLost { lease, reason } => {
                let session = {
                    let sessions = self.sessions.lock().await;
                    sessions.iter().find_map(|(session, record)| {
                        record
                            .capture
                            .as_ref()
                            .is_some_and(|capture| capture.id == lease.id)
                            .then(|| session.clone())
                    })
                };
                if let Some(session) = session {
                    let reason = match reason {
                        CaptureLossReason::LeaseReplaced => CaptureReleaseReason::Replaced,
                        CaptureLossReason::UserModeChanged => CaptureReleaseReason::UserModeChanged,
                        CaptureLossReason::AdapterHealth => CaptureReleaseReason::AdapterShutdown,
                    };
                    let _ = self.detach(&session, reason).await;
                }
            }
            AdapterHealthEvent::DispatchCompleted(completion) => {
                self.dispatch_completed(completion).await;
            }
        }
    }

    async fn dispatch_completed(&self, completion: DispatchCompletion) {
        let (core, outcome, diagnostic) = match completion {
            DispatchCompletion::Succeeded { execution } => {
                (execution, ExecutionOutcome::Succeeded, None)
            }
            DispatchCompletion::Failed { execution, error } => (
                execution,
                ExecutionOutcome::Failed,
                Some(diagnostic(
                    DiagnosticCode::ActionBlocked,
                    &error.to_string(),
                )),
            ),
            DispatchCompletion::OutcomeUnknown { execution, error } => (
                execution,
                ExecutionOutcome::OutcomeUnknown,
                Some(diagnostic(
                    DiagnosticCode::OutcomeUnknown,
                    &error.to_string(),
                )),
            ),
        };
        let (session, record) = {
            let mut state = self.state.lock().await;
            let Some((session, _)) = state
                .executions
                .iter()
                .find(|(_, record)| record.core == core)
            else {
                return;
            };
            let session = session.clone();
            let record = state.executions.remove(&session).expect("entry was found");
            (session, record)
        };
        if record.pending_control.is_some() {
            return;
        }
        let events = {
            let sessions = self.sessions.lock().await;
            sessions.get(&session).map(|record| record.events.clone())
        };
        if let Some(events) = events {
            let _ = events
                .send(WireMessage::Event {
                    event_id: self.new_event_id(),
                    event: BrokerEvent::ExecutionCompleted {
                        session,
                        execution: record.wire,
                        outcome,
                        diagnostic,
                    },
                })
                .await;
        }
    }

    async fn emit_execution_completed(
        &self,
        session: UiSessionId,
        execution: ExecutionId,
        outcome: ExecutionOutcome,
        diagnostic: Option<ProtocolDiagnostic>,
    ) {
        let events = {
            let sessions = self.sessions.lock().await;
            sessions.get(&session).map(|record| record.events.clone())
        };
        if let Some(events) = events {
            let _ = events
                .send(WireMessage::Event {
                    event_id: self.new_event_id(),
                    event: BrokerEvent::ExecutionCompleted {
                        session,
                        execution,
                        outcome,
                        diagnostic,
                    },
                })
                .await;
        }
    }

    async fn broadcast_health(&self, healthy: bool, error: Option<AdapterError>) {
        let diagnostic =
            error.map(|error| diagnostic(DiagnosticCode::HostUnavailable, &error.to_string()));
        let events = self
            .sessions
            .lock()
            .await
            .values()
            .map(|record| record.events.clone())
            .collect::<Vec<_>>();
        for events in events {
            let _ = events
                .send(WireMessage::Event {
                    event_id: self.new_event_id(),
                    event: BrokerEvent::AdapterHealthChanged {
                        healthy,
                        diagnostic: diagnostic.clone(),
                    },
                })
                .await;
        }
    }

    fn require_role(
        &self,
        role: muxe_protocol::PeerRole,
        expected: muxe_protocol::PeerRole,
    ) -> Result<(), BrokerError> {
        (role == expected).then_some(()).ok_or(BrokerError::Role {
            expected,
            actual: role,
        })
    }

    async fn prepare(
        &self,
        request: muxe_protocol::PrepareUiLaunch,
    ) -> Result<RequestResult, BrokerError> {
        let (pending, replaced, replaced_pending, replaced_session) = {
            let mut tokens = self.token_source.lock().await;
            let mut state = self.state.lock().await;
            let prepared = state.gate.prepare(
                &mut *tokens,
                request.modal_scope,
                request.root,
                Instant::now(),
                Duration::from_millis(u64::from(request.lease_millis)),
            )?;
            let replaced_session = prepared
                .replaced_pending
                .as_ref()
                .and_then(|launch| state.pending_sessions.remove(&launch.token));
            state
                .pending_sessions
                .insert(prepared.pending.token, self.new_session_id());
            (
                prepared.pending,
                prepared.replaced,
                prepared.replaced_pending,
                replaced_session,
            )
        };

        if let (Some(replaced), Some(session)) = (replaced_pending, replaced_session) {
            self.fail_pending(replaced, &session, CaptureReleaseReason::Replaced)
                .await;
        }
        if let Some(ScopeOwner::Ready(session)) = replaced {
            self.detach(&session, CaptureReleaseReason::Replaced)
                .await?;
        }

        Ok(RequestResult::Immediate(BrokerResponse::LaunchPrepared {
            token: pending.token,
            lease_millis: request.lease_millis,
        }))
    }

    async fn register(&self, request: RegisterPendingPane) -> Result<(), BrokerError> {
        let mut state = self.state.lock().await;
        state.gate.register_pending_pane(
            request.token,
            RegisteredPane {
                pane: request.pane,
                temporary_tab: request.temporary_tab,
            },
        )?;
        Ok(())
    }

    async fn attach(
        &self,
        request: AttachUi,
        events: mpsc::Sender<muxe_protocol::WireMessage>,
    ) -> Result<RequestResult, BrokerError> {
        let pane = PaneId::new(request.pane.as_str());
        let scope = self
            .adapter
            .modal_scope(&pane)
            .await
            .map_err(BrokerError::Adapter)?;
        let wire_scope = ModalScopeId::new(scope.as_str());
        let config = self.config.snapshot().await.config;
        if config
            .menu(&muxe_core::MenuId::new(request.root.as_str()))
            .is_none()
        {
            return Err(BrokerError::UnknownMenu(request.root));
        }

        let origin_hint = request.origin.as_ref().map(|origin| UntrustedOriginHint {
            workspace_id: muxe_core::WorkspaceId::new(origin.workspace.as_str()),
            tab_id: muxe_core::TabId::new(origin.tab.as_str()),
            pane_id: PaneId::new(origin.pane.as_str()),
            cwd: Some(std::path::PathBuf::from(&origin.cwd)),
            source: OriginHintSource::LauncherBootstrap,
        });
        let caller_identity = request
            .caller_identity
            .as_ref()
            .map(|caller| HostCallerIdentity {
                workspace_id: muxe_core::WorkspaceId::new(caller.workspace.as_str()),
                tab_id: muxe_core::TabId::new(caller.tab.as_str()),
                pane_id: PaneId::new(caller.pane.as_str()),
                cwd: Some(std::path::PathBuf::from(&caller.cwd)),
            });

        let (session, pending) = {
            let mut state = self.state.lock().await;
            let session = match request.pending_launch {
                Some(token) => {
                    let launch = state.gate.pending(token).ok_or(GateError::UnknownToken)?;
                    if launch.root != request.root || launch.scope != wire_scope {
                        return Err(BrokerError::Gate(GateError::ScopeMismatch));
                    }
                    if launch
                        .registered_pane
                        .as_ref()
                        .is_some_and(|registered| registered.pane != request.pane)
                    {
                        return Err(BrokerError::Gate(GateError::PaneMismatch));
                    }
                    state
                        .pending_sessions
                        .get(&token)
                        .cloned()
                        .ok_or(GateError::UnknownToken)?
                }
                None => self.new_session_id(),
            };
            let pending = matches!(
                state
                    .gate
                    .attach(session.clone(), wire_scope, request.pending_launch)?,
                AttachDisposition::WaitingForCommit { .. }
            );
            (session, pending)
        };

        let origin = match self
            .adapter
            .capture_origin(OriginCaptureRequest {
                ui_session: adapter_session(&session),
                ui_pane: pane.clone(),
                origin_hint,
                caller_identity,
            })
            .await
        {
            Ok(origin) => origin,
            Err(error) => {
                let mut state = self.state.lock().await;
                state.gate.detach(&session);
                return Err(BrokerError::Adapter(error));
            }
        };
        let (readiness, receiver) = watch::channel(SessionReadiness::Pending);
        let record = SessionRecord {
            config: Arc::clone(&config),
            root: muxe_core::MenuId::new(request.root.as_str()),
            scope,
            origin,
            capture: None,
            readiness,
            events,
        };
        self.sessions.lock().await.insert(session.clone(), record);

        if pending {
            return Ok(RequestResult::WaitForAttachment(PendingAttachment {
                session,
                receiver,
            }));
        }
        self.begin_capture(&session).await?;
        Ok(RequestResult::Immediate(
            self.attached_response(&session).await?,
        ))
    }

    async fn commit(&self, token: PendingLaunchToken, pane: HostPaneId) -> Result<(), BrokerError> {
        let (session, registration) = {
            let mut state = self.state.lock().await;
            let registration = state
                .gate
                .pending(token)
                .and_then(|launch| launch.registered_pane.clone());
            let session = state.gate.commit(token, &pane)?;
            state.pending_sessions.remove(&token);
            (session, registration)
        };
        if let Err(error) = self.begin_capture(&session).await {
            self.detach(&session, CaptureReleaseReason::UiDismissed)
                .await?;
            if let Some(registration) = registration {
                self.close_registered(&session, registration).await?;
            }
            return Err(error);
        }
        let response = self.attached_response(&session).await?;
        let sessions = self.sessions.lock().await;
        if let Some(record) = sessions.get(&session) {
            let _ = record.readiness.send(SessionReadiness::Ready(response));
        }
        Ok(())
    }

    pub(crate) async fn abort(&self, token: PendingLaunchToken) -> Result<(), BrokerError> {
        let (registration, session) = {
            let mut state = self.state.lock().await;
            let session = state
                .pending_sessions
                .remove(&token)
                .ok_or(GateError::UnknownToken)?;
            let registration = state.gate.abort(token)?;
            (registration, session)
        };
        self.fail_session(&session, CaptureReleaseReason::UiDismissed)
            .await;
        if let Some(registration) = registration {
            self.close_registered(&session, registration).await?;
        }
        Ok(())
    }

    async fn begin_capture(&self, session: &UiSessionId) -> Result<(), BrokerError> {
        if !self
            .adapter
            .capabilities()
            .await
            .map_err(BrokerError::Adapter)?
            .supports_capture
        {
            return Ok(());
        }
        let scope = {
            let sessions = self.sessions.lock().await;
            sessions
                .get(session)
                .ok_or_else(|| BrokerError::UnknownSession(session.clone()))?
                .scope
                .clone()
        };
        let capture = self
            .adapter
            .begin_capture(CaptureRequest {
                ui_session: adapter_session(session),
                modal_scope: scope,
            })
            .await
            .map_err(BrokerError::Adapter)?;
        let mut sessions = self.sessions.lock().await;
        let record = sessions
            .get_mut(session)
            .ok_or_else(|| BrokerError::UnknownSession(session.clone()))?;
        record.capture = Some(capture);
        Ok(())
    }

    async fn attached_response(
        &self,
        session: &UiSessionId,
    ) -> Result<BrokerResponse, BrokerError> {
        let sessions = self.sessions.lock().await;
        let record = sessions
            .get(session)
            .ok_or_else(|| BrokerError::UnknownSession(session.clone()))?;
        let view = record.config.attachment_view(&record.root).ok_or_else(|| {
            BrokerError::UnknownMenu(muxe_protocol::MenuId::new(record.root.as_str()))
        })?;
        Ok(BrokerResponse::UiAttached {
            session: session.clone(),
            snapshot: wire::attachment(&view),
        })
    }

    async fn invoke(&self, request: InvokeBinding) -> Result<RequestResult, BrokerError> {
        let (config, origin) = {
            let sessions = self.sessions.lock().await;
            let record = sessions
                .get(&request.session)
                .ok_or_else(|| BrokerError::UnknownSession(request.session.clone()))?;
            (Arc::clone(&record.config), record.origin.clone())
        };
        let binding = config
            .binding(
                CompiledGeneration(request.generation),
                muxe_core::BindingId::new(
                    CompiledGeneration(request.binding.generation),
                    request.binding.ordinal,
                ),
            )
            .cloned()
            .ok_or(BrokerError::StaleGeneration)?;
        let serial = self.next_execution.fetch_add(1, Ordering::Relaxed);
        let execution = self.new_execution_id(serial);
        let core_execution = CoreExecutionId(serial);
        let on_menu_control = binding.settings.execution.on_menu_control;
        let awaitable = binding.settings.execution.mode == muxe_core::ExecutionMode::Await;
        let accepted_capabilities = match binding.action {
            ActionSpec::Portable(muxe_core::PortableAction::Config(ConfigAction::Reload)) => {
                self.reload().await?;
                let disposition = if awaitable {
                    self.emit_execution_completed(
                        request.session.clone(),
                        execution,
                        ExecutionOutcome::Succeeded,
                        None,
                    )
                    .await;
                    InvocationDisposition::Awaited
                } else {
                    InvocationDisposition::Detached
                };
                return Ok(RequestResult::Immediate(
                    BrokerResponse::InvocationAccepted {
                        execution,
                        disposition,
                    },
                ));
            }
            ActionSpec::Portable(muxe_core::PortableAction::Menu(MenuAction::Quit)) => {
                self.detach(&request.session, CaptureReleaseReason::UiDismissed)
                    .await?;
                None
            }
            ActionSpec::Portable(muxe_core::PortableAction::Menu(_)) => {
                return Err(BrokerError::LocalMenuAction);
            }
            ActionSpec::Portable(action) => {
                let command_cwd_from_context = matches!(
                    &action,
                    muxe_core::PortableAction::Command(command)
                        if command.cwd.as_ref().is_some_and(|cwd| matches!(
                            &cwd.value.kind,
                            muxe_core::ConfigValueKind::Context(_)
                        ))
                );
                let action =
                    ResolvedPortableAction::from_origin(&action, &origin).map_err(|error| {
                        match error {
                            muxe_core::PortableActionResolutionError::Context(_) => {
                                BrokerError::ContextUnavailable
                            }
                            muxe_core::PortableActionResolutionError::InvalidValue {
                                parameter,
                                message,
                                ..
                            } => BrokerError::PortableResolution { parameter, message },
                        }
                    })?;
                match action.action {
                    muxe_core::PortableAction::Command(command) => {
                        self.execute_command(
                            request.session.clone(),
                            execution,
                            core_execution,
                            command,
                            &origin,
                            command_cwd_from_context,
                            binding.settings.execution.clone(),
                        )
                        .await?;
                        return Ok(RequestResult::Immediate(
                            BrokerResponse::InvocationAccepted {
                                execution,
                                disposition: if binding.settings.execution.mode
                                    == muxe_core::ExecutionMode::Await
                                {
                                    InvocationDisposition::Awaited
                                } else {
                                    InvocationDisposition::Detached
                                },
                            },
                        ));
                    }
                    action => {
                        let accepted = self
                            .adapter
                            .dispatch_portable(PortableDispatchRequest {
                                execution: core_execution,
                                action: ResolvedPortableAction { action },
                                origin,
                            })
                            .await
                            .map_err(BrokerError::Adapter)?;
                        if accepted.execution != core_execution {
                            return Err(BrokerError::MismatchedExecution);
                        }
                        Some(accepted.capabilities)
                    }
                }
            }
            ActionSpec::Native(candidate) => {
                let action = ResolvedNativeAction::from_origin(&candidate, &origin)
                    .map_err(|_| BrokerError::ContextUnavailable)?;
                let accepted = self
                    .adapter
                    .dispatch_native(muxe_adapter_api::NativeDispatchRequest {
                        execution: core_execution,
                        action,
                        origin,
                    })
                    .await
                    .map_err(BrokerError::Adapter)?;
                if accepted.execution != core_execution {
                    return Err(BrokerError::MismatchedExecution);
                }
                Some(accepted.capabilities)
            }
        };
        let disposition = if accepted_capabilities
            .as_ref()
            .is_some_and(|capabilities| awaitable && capabilities.awaitable)
        {
            InvocationDisposition::Awaited
        } else {
            InvocationDisposition::Detached
        };
        if let Some(capabilities) =
            accepted_capabilities.filter(|_| disposition == InvocationDisposition::Awaited)
        {
            self.state.lock().await.executions.insert(
                request.session.clone(),
                ExecutionRecord {
                    wire: execution,
                    core: core_execution,
                    cancellable: capabilities.cancellable,
                    on_menu_control,
                    owner: ExecutionOwner::Adapter,
                    pending_control: None,
                },
            );
        }
        Ok(RequestResult::Immediate(
            BrokerResponse::InvocationAccepted {
                execution,
                disposition,
            },
        ))
    }

    async fn execute_command(
        &self,
        session: UiSessionId,
        wire: ExecutionId,
        core: CoreExecutionId,
        command: CommandAction,
        origin: &muxe_core::OriginContext,
        cwd_from_context: bool,
        policy: ExecutionPolicy,
    ) -> Result<(), BrokerError> {
        let program = command_string(&command.program, "command program")?;
        if program.is_empty() {
            return Err(BrokerError::GenericProcess(
                "command program must not be empty".to_owned(),
            ));
        }
        let mut child_command = Command::new(program);
        for argument in &command.args {
            child_command.arg(command_string(argument, "command argument")?);
        }
        let cwd = resolve_command_cwd(origin, command.cwd.as_ref(), cwd_from_context)?;
        child_command.current_dir(cwd);
        for (name, value) in &command.env {
            child_command.env(name, command_string(value, "command environment value")?);
        }
        #[cfg(unix)]
        child_command.process_group(0);
        child_command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = child_command.spawn().map_err(|error| {
            BrokerError::GenericProcess(format!("could not spawn {program:?}: {error}"))
        })?;
        let Some(process_group) = child.id().and_then(|id| i32::try_from(id).ok()) else {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(BrokerError::GenericProcess(
                "spawned command has no valid process-group leader identity".to_owned(),
            ));
        };
        let (cancellation, cancellation_rx) = watch::channel(None);
        self.generic
            .processes
            .lock()
            .await
            .insert(core, GenericProcess { cancellation });
        if policy.mode == muxe_core::ExecutionMode::Await {
            self.state.lock().await.executions.insert(
                session.clone(),
                ExecutionRecord {
                    wire,
                    core,
                    cancellable: true,
                    on_menu_control: policy.on_menu_control,
                    owner: ExecutionOwner::GenericProcess,
                    pending_control: None,
                },
            );
        }
        tokio::spawn(supervise_generic_child(
            child,
            process_group,
            cancellation_rx,
            policy.timeout,
            policy.on_timeout,
            session,
            core,
            Arc::clone(&self.state),
            Arc::clone(&self.sessions),
            Arc::clone(&self.generic),
            Arc::clone(&self.next_event),
        ));
        Ok(())
    }
    async fn cancel_generic(&self, execution: CoreExecutionId) -> Result<(), BrokerError> {
        let cancellation = self
            .generic
            .processes
            .lock()
            .await
            .get(&execution)
            .map(|process| process.cancellation.clone())
            .ok_or(BrokerError::CancelUnsupported)?;
        cancellation
            .send(Some(GenericCancellation::UserRequested))
            .map_err(|_| {
                BrokerError::GenericProcess(
                    "generic command exited before its cancellation request was delivered"
                        .to_owned(),
                )
            })
    }

    async fn control(&self, request: UiMenuControl) -> Result<RequestResult, BrokerError> {
        let pending = {
            let mut state = self.state.lock().await;
            let Some(record) = state.executions.get_mut(&request.session) else {
                return Ok(RequestResult::Immediate(BrokerResponse::Acknowledged));
            };
            if record.pending_control.is_some() {
                return Err(BrokerError::PendingControlInFlight);
            }
            record.pending_control = Some(request.control);
            record.clone()
        };
        let accepted = match pending.on_menu_control {
            muxe_core::MenuControlAction::Detach => Ok(()),
            muxe_core::MenuControlAction::Cancel if pending.cancellable => match pending.owner {
                ExecutionOwner::Adapter => self
                    .adapter
                    .cancel(pending.core)
                    .await
                    .map_err(BrokerError::Adapter),
                ExecutionOwner::GenericProcess => self.cancel_generic(pending.core).await,
            },
            muxe_core::MenuControlAction::Cancel => Err(BrokerError::CancelUnsupported),
        };
        if let Err(error) = accepted {
            if self
                .clear_pending_control(&request.session, pending.core, request.control)
                .await
            {
                return Err(error);
            }
        }
        Ok(RequestResult::Immediate(
            BrokerResponse::PendingControlCompleted {
                execution: pending.wire,
                control: request.control,
            },
        ))
    }

    async fn clear_pending_control(
        &self,
        session: &UiSessionId,
        core: CoreExecutionId,
        control: MenuControl,
    ) -> bool {
        let mut state = self.state.lock().await;
        let Some(record) = state.executions.get_mut(session) else {
            return false;
        };
        if record.core != core || record.pending_control != Some(control) {
            return false;
        }
        record.pending_control = None;
        true
    }
    async fn detach(
        &self,
        session: &UiSessionId,
        reason: CaptureReleaseReason,
    ) -> Result<(), BrokerError> {
        let record = self.sessions.lock().await.remove(session);
        let pending = {
            let mut state = self.state.lock().await;
            state.gate.detach(session);
            state.executions.remove(session)
        };
        if let Some(pending) = pending {
            if pending.pending_control.is_none()
                && pending.on_menu_control == muxe_core::MenuControlAction::Cancel
                && pending.cancellable
            {
                match pending.owner {
                    ExecutionOwner::Adapter => {
                        let _ = self.adapter.cancel(pending.core).await;
                    }
                    ExecutionOwner::GenericProcess => {
                        let _ = self.cancel_generic(pending.core).await;
                    }
                }
            }
        }
        if let Some(record) = record {
            let _ = record.readiness.send(SessionReadiness::Failed(diagnostic(
                DiagnosticCode::LaunchAborted,
                "UI session detached",
            )));
            if let Some(capture) = record.capture {
                self.adapter
                    .end_capture(capture, reason)
                    .await
                    .map_err(BrokerError::Adapter)?;
            }
        }
        Ok(())
    }

    async fn fail_pending(
        &self,
        launch: PendingLaunch,
        session: &UiSessionId,
        reason: CaptureReleaseReason,
    ) {
        self.fail_session(session, reason).await;
        let _ = self.close_pending_launch(&launch, session).await;
    }

    async fn fail_session(&self, session: &UiSessionId, reason: CaptureReleaseReason) {
        let _ = self.detach(session, reason).await;
    }

    async fn close_pending_launch(
        &self,
        launch: &PendingLaunch,
        session: &UiSessionId,
    ) -> Result<(), BrokerError> {
        let Some(registration) = launch.registered_pane.clone() else {
            return Ok(());
        };
        self.close_registered(session, registration).await
    }

    async fn close_registered(
        &self,
        session: &UiSessionId,
        registration: RegisteredPane,
    ) -> Result<(), BrokerError> {
        self.adapter
            .close_pending_pane(PendingPaneRegistration {
                ui_session: adapter_session(session),
                pane: PaneId::new(registration.pane.as_str()),
                temporary_tab: registration
                    .temporary_tab
                    .map(|tab| muxe_core::TabId::new(tab.as_str())),
            })
            .await
            .map_err(BrokerError::Adapter)
    }

    fn new_session_id(&self) -> UiSessionId {
        UiSessionId::new(format!(
            "ui-{}",
            self.next_session.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn new_execution_id(&self, counter: u64) -> ExecutionId {
        let mut bytes = [0; 16];
        bytes[8..].copy_from_slice(&counter.to_be_bytes());
        ExecutionId(bytes)
    }

    fn new_event_id(&self) -> EventId {
        new_event_id(&self.next_event)
    }
}

fn adapter_session(session: &UiSessionId) -> muxe_adapter_api::UiSessionId {
    muxe_adapter_api::UiSessionId::new(session.as_str())
}

fn diagnostic(code: DiagnosticCode, message: &str) -> ProtocolDiagnostic {
    let message = if message.len() <= muxe_protocol::MAX_DIAGNOSTIC_LEN {
        message.to_owned()
    } else {
        let mut end = muxe_protocol::MAX_DIAGNOSTIC_LEN;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message[..end].to_owned()
    };
    ProtocolDiagnostic { code, message }
}

const GENERIC_CANCEL_GRACE: Duration = Duration::from_secs(2);

enum GenericWait {
    Exited(std::io::Result<std::process::ExitStatus>),
    Cancelled(GenericCancellation),
    TimedOut,
}

async fn supervise_generic_child(
    mut child: Child,
    process_group: i32,
    mut cancellation: watch::Receiver<Option<GenericCancellation>>,
    timeout: Option<Duration>,
    on_timeout: TimeoutAction,
    session: UiSessionId,
    core: CoreExecutionId,
    state: Arc<Mutex<BrokerState>>,
    sessions: Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    generic: Arc<GenericSupervisor>,
    next_event: Arc<AtomicU64>,
) {
    let wait = match timeout {
        Some(timeout) => {
            tokio::select! {
                status = child.wait() => GenericWait::Exited(status),
                cancellation = await_generic_cancellation(&mut cancellation) => GenericWait::Cancelled(cancellation),
                _ = tokio::time::sleep(timeout) => GenericWait::TimedOut,
            }
        }
        None => {
            tokio::select! {
                status = child.wait() => GenericWait::Exited(status),
                cancellation = await_generic_cancellation(&mut cancellation) => GenericWait::Cancelled(cancellation),
            }
        }
    };
    match wait {
        GenericWait::Exited(status) => {
            let (outcome, diagnostic) = generic_exit_outcome(status);
            finish_generic(
                &session,
                core,
                outcome,
                diagnostic,
                true,
                &state,
                &sessions,
                &generic,
                &next_event,
            )
            .await;
        }
        GenericWait::Cancelled(_reason) => {
            let completion = terminate_generic_child(&mut child, process_group).await;
            let (outcome, diagnostic) = match completion {
                Ok(_) => (ExecutionOutcome::Cancelled, None),
                Err(error) => (
                    ExecutionOutcome::Failed,
                    Some(diagnostic(DiagnosticCode::ActionBlocked, &error)),
                ),
            };
            finish_generic(
                &session,
                core,
                outcome,
                diagnostic,
                true,
                &state,
                &sessions,
                &generic,
                &next_event,
            )
            .await;
        }
        GenericWait::TimedOut if on_timeout == TimeoutAction::Detach => {
            finish_generic(
                &session,
                core,
                ExecutionOutcome::Detached,
                Some(diagnostic(
                    DiagnosticCode::ActionBlocked,
                    "generic command exceeded its timeout and continues detached",
                )),
                false,
                &state,
                &sessions,
                &generic,
                &next_event,
            )
            .await;
            let _ = child.wait().await;
            generic.processes.lock().await.remove(&core);
        }
        GenericWait::TimedOut => {
            let completion = terminate_generic_child(&mut child, process_group).await;
            let (outcome, diagnostic) = match completion {
                Ok(_) => (ExecutionOutcome::TimedOut, None),
                Err(error) => (
                    ExecutionOutcome::Failed,
                    Some(diagnostic(DiagnosticCode::ActionBlocked, &error)),
                ),
            };
            finish_generic(
                &session,
                core,
                outcome,
                diagnostic,
                true,
                &state,
                &sessions,
                &generic,
                &next_event,
            )
            .await;
        }
    }
}

async fn await_generic_cancellation(
    cancellation: &mut watch::Receiver<Option<GenericCancellation>>,
) -> GenericCancellation {
    let _ = cancellation.changed().await;
    cancellation
        .borrow()
        .as_ref()
        .copied()
        .unwrap_or(GenericCancellation::UserRequested)
}

async fn terminate_generic_child(
    child: &mut Child,
    process_group: i32,
) -> Result<std::process::ExitStatus, String> {
    #[cfg(unix)]
    {
        // Do not reap the group leader between TERM and KILL. Its unreaped PID is the process
        // group ID, so retaining it prevents that numeric group ID from being recycled before
        // bounded escalation completes. A leader that exits on TERM must not leave a TERM-resistant
        // descendant outside the cancellation contract.
        if signal_process_group(process_group, Signal::SIGTERM)? {
            tokio::time::sleep(GENERIC_CANCEL_GRACE).await;
            let _ = signal_process_group(process_group, Signal::SIGKILL)?;
        }
        child
            .wait()
            .await
            .map_err(|error| format!("could not reap cancelled generic command: {error}"))
    }
    #[cfg(not(unix))]
    {
        let _ = process_group;
        child
            .kill()
            .await
            .map_err(|error| format!("could not kill generic command: {error}"))?;
        child
            .wait()
            .await
            .map_err(|error| format!("could not reap killed generic command: {error}"))
    }
}

#[cfg(unix)]
fn signal_process_group(process_group: i32, signal: Signal) -> Result<bool, String> {
    match killpg(Pid::from_raw(process_group), signal) {
        Ok(()) => Ok(true),
        Err(Errno::ESRCH) => Ok(false),
        Err(error) => Err(format!(
            "could not send {signal:?} to generic command process group {process_group}: {error}"
        )),
    }
}

fn generic_exit_outcome(
    status: std::io::Result<std::process::ExitStatus>,
) -> (ExecutionOutcome, Option<ProtocolDiagnostic>) {
    match status {
        Ok(status) if status.success() => (ExecutionOutcome::Succeeded, None),
        Ok(status) => (
            ExecutionOutcome::Failed,
            Some(diagnostic(
                DiagnosticCode::ActionBlocked,
                &format!("generic command exited with {status}"),
            )),
        ),
        Err(error) => (
            ExecutionOutcome::Failed,
            Some(diagnostic(
                DiagnosticCode::ActionBlocked,
                &format!("could not reap generic command: {error}"),
            )),
        ),
    }
}

async fn finish_generic(
    session: &UiSessionId,
    core: CoreExecutionId,
    outcome: ExecutionOutcome,
    diagnostic: Option<ProtocolDiagnostic>,
    remove_process: bool,
    state: &Arc<Mutex<BrokerState>>,
    sessions: &Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    generic: &Arc<GenericSupervisor>,
    next_event: &Arc<AtomicU64>,
) {
    if remove_process {
        generic.processes.lock().await.remove(&core);
    }
    let record = {
        let mut state = state.lock().await;
        match state.executions.get(session) {
            Some(record)
                if record.owner == ExecutionOwner::GenericProcess && record.core == core =>
            {
                state.executions.remove(session)
            }
            _ => None,
        }
    };
    let Some(record) = record else {
        return;
    };
    if record.pending_control.is_some() {
        return;
    }
    let events = sessions
        .lock()
        .await
        .get(session)
        .map(|record| record.events.clone());
    if let Some(events) = events {
        let _ = events
            .send(WireMessage::Event {
                event_id: new_event_id(next_event),
                event: BrokerEvent::ExecutionCompleted {
                    session: session.clone(),
                    execution: record.wire,
                    outcome,
                    diagnostic,
                },
            })
            .await;
    }
}

fn command_string<'a>(
    scalar: &'a muxe_core::ActionScalar,
    parameter: &str,
) -> Result<&'a str, BrokerError> {
    scalar.value.as_str().ok_or_else(|| {
        BrokerError::GenericProcess(format!("{parameter} must be a resolved string"))
    })
}

fn resolve_command_cwd(
    origin: &muxe_core::OriginContext,
    configured: Option<&muxe_core::ActionScalar>,
    configured_from_context: bool,
) -> Result<std::path::PathBuf, BrokerError> {
    let captured = || {
        origin
            .pane_cwd
            .as_ref()
            .filter(|cwd| cwd.is_absolute())
            .ok_or(BrokerError::ContextUnavailable)
    };
    match configured {
        None => Ok(captured()?.to_path_buf()),
        Some(value) => {
            let configured = std::path::PathBuf::from(command_string(value, "command cwd")?);
            if configured.is_absolute() {
                Ok(configured)
            } else if configured_from_context {
                Err(BrokerError::GenericProcess(
                    "origin-derived command cwd must resolve to an absolute path".to_owned(),
                ))
            } else {
                Ok(captured()?.join(configured))
            }
        }
    }
}

fn new_event_id(next_event: &AtomicU64) -> EventId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&next_event.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    EventId(bytes)
}
#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use muxe_adapter_api::{
        AdapterCapabilities, AdapterHealthEvent, CaptureLeaseId, DispatchAccepted,
        ExecutionCorrelationId, HostIdentity, KeyboardCapabilities, ModalScopeId,
        NativeDispatchRequest,
    };
    use muxe_core::{
        ActionScalar, ActionValidation, ActionValidator, ConfigDiagnostic, ConfigValue,
        ConfigValueKind, KeyCapabilities, OriginContext, OriginHostKind, OriginInvocationSource,
        ServerId, SourceId,
    };
    use muxe_protocol::{BindingId, PeerRole};
    use tokio::sync::mpsc;

    use super::*;

    struct CountingAdapter {
        portable_dispatches: AtomicUsize,
    }

    impl CountingAdapter {
        fn origin_without_cwd() -> OriginContext {
            OriginContext {
                host_kind: OriginHostKind::Herdr,
                server_id: ServerId::new("server"),
                client_id: None,
                session_id: None,
                workspace_id: None,
                tab_id: None,
                tab_index: None,
                pane_id: Some(PaneId::new("origin-pane")),
                pane_type: None,
                pane_cwd: None,
                selection_text: None,
                invocation_source: OriginInvocationSource::RootBinding,
                worktree_id: None,
                worktree_path: None,
                agent_id: None,
                link_url: None,
                link_handler_id: None,
            }
        }
    }

    impl ActionValidator for CountingAdapter {
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
    impl HostAdapter for CountingAdapter {
        async fn identity(&self) -> Result<HostIdentity, AdapterError> {
            Ok(HostIdentity {
                kind: muxe_adapter_api::HostKind::Herdr,
                discovery_key: "test".to_owned(),
                live_server_id: "server".to_owned(),
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
            Ok(ModalScopeId::new("scope"))
        }

        async fn begin_capture(
            &self,
            request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            Ok(CaptureLease {
                id: CaptureLeaseId::new("unexpected"),
                ui_session: request.ui_session,
                modal_scope: request.modal_scope,
            })
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
            Ok(Self::origin_without_cwd())
        }

        async fn dispatch_portable(
            &self,
            request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            self.portable_dispatches.fetch_add(1, Ordering::SeqCst);
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("unexpected"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn dispatch_native(
            &self,
            request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("native"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn cancel(&self, _execution: CoreExecutionId) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Shutdown,
                "test adapter has no events",
            ))
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn missing_portable_context_does_not_dispatch_to_host() {
        let adapter = Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
        });
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<broker regression>"),
            r#"
version: 1
menus:
  main:
    bindings:
      c:
        label: requires origin workspace
        action:
          type: tab:create
          workspace-id:
            $context: origin.workspace.id
"#,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let root = muxe_core::MenuId::new("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("test binding is visible");
        let directory = tempfile::tempdir().expect("test directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        let (events, _events_rx) = mpsc::channel(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new("muxe-pane"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events.clone(),
            )
            .await
            .expect("UI attaches without a host capture lease");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected immediate UI attachment");
        };
        let result = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::InvokeBinding(InvokeBinding {
                    session,
                    generation: 1,
                    binding: BindingId {
                        generation: binding.generation().0,
                        ordinal: binding.ordinal(),
                    },
                }),
                events,
            )
            .await;

        assert!(matches!(result, Err(BrokerError::ContextUnavailable)));
        assert_eq!(adapter.portable_dispatches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn awaited_config_reload_emits_a_terminal_completion() {
        let adapter = Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
        });
        let yaml = r#"
version: 1
menus:
  main:
    bindings:
      r:
        label: reload
        action: config:reload
        settings:
          execution:
            mode: await
"#;
        let directory = tempfile::tempdir().expect("owned configuration directory");
        let config_path = directory.path().join("config.yml");
        std::fs::write(&config_path, yaml).expect("write reloadable configuration");
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<awaited reload regression>"),
            yaml,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let root = muxe_core::MenuId::new("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("reload binding is visible");
        let broker = Broker::from_compiled(adapter, &config_path, config);
        let (events, mut events_rx) = mpsc::channel(2);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new("muxe-pane"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events.clone(),
            )
            .await
            .expect("UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected immediate attachment");
        };

        let response = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::InvokeBinding(InvokeBinding {
                    session: session.clone(),
                    generation: 1,
                    binding: BindingId {
                        generation: binding.generation().0,
                        ordinal: binding.ordinal(),
                    },
                }),
                events,
            )
            .await
            .expect("awaited reload is accepted");
        let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
            execution,
            disposition: InvocationDisposition::Awaited,
        }) = response
        else {
            panic!("expected an awaited invocation acceptance");
        };
        let Some(WireMessage::Event {
            event:
                BrokerEvent::ExecutionCompleted {
                    session: completed_session,
                    execution: completed_execution,
                    outcome: ExecutionOutcome::Succeeded,
                    diagnostic: None,
                },
            ..
        }) = events_rx.recv().await
        else {
            panic!("expected awaited reload terminal completion");
        };
        assert_eq!(completed_session, session);
        assert_eq!(completed_execution, execution);
    }

    #[tokio::test]
    async fn host_continuity_loss_keeps_the_pinned_ui_session_attached() {
        let adapter = Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
        });
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<continuity regression>"),
            r#"
version: 1
menus:
  main:
    bindings:
      r:
        label: reload
        action: config:reload
"#,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let directory = tempfile::tempdir().expect("owned configuration directory");
        let broker = Broker::from_compiled(adapter, directory.path().join("config.yml"), config);
        let (events, _events_rx) = mpsc::channel(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new("muxe-pane"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events,
            )
            .await
            .expect("UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected immediate attachment");
        };

        broker
            .handle_health_event(AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "subscription lost",
                ),
            })
            .await;

        assert!(
            broker.sessions.lock().await.contains_key(&session),
            "continuity loss must preserve the immutable session menu while the adapter blocks stale host dispatch"
        );
    }

    #[test]
    fn command_cwd_resolves_only_from_the_captured_origin() {
        let mut origin = CountingAdapter::origin_without_cwd();
        origin.pane_cwd = Some(PathBuf::from("/captured"));
        let literal_relative = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
            "child".to_owned(),
        )));

        assert_eq!(
            resolve_command_cwd(&origin, None, false).expect("default uses captured cwd"),
            PathBuf::from("/captured")
        );
        assert_eq!(
            resolve_command_cwd(&origin, Some(&literal_relative), false)
                .expect("literal relative cwd joins captured cwd"),
            PathBuf::from("/captured/child")
        );
        assert!(matches!(
            resolve_command_cwd(&origin, Some(&literal_relative), true),
            Err(BrokerError::GenericProcess(_))
        ));

        let missing = CountingAdapter::origin_without_cwd();
        assert!(matches!(
            resolve_command_cwd(&missing, None, false),
            Err(BrokerError::ContextUnavailable)
        ));
        let mut relative = CountingAdapter::origin_without_cwd();
        relative.pane_cwd = Some(PathBuf::from("not-absolute"));
        assert!(matches!(
            resolve_command_cwd(&relative, Some(&literal_relative), false),
            Err(BrokerError::ContextUnavailable)
        ));
    }

    #[tokio::test]
    async fn detached_generic_command_is_reaped_by_its_owned_process_group() {
        let adapter = Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
        });
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<generic supervision regression>"),
            r#"
version: 1
menus:
  main:
    bindings:
      x:
        label: no-op
        action: config:reload
"#,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let directory = tempfile::tempdir().expect("owned command cwd");
        let broker = Broker::from_compiled(adapter, directory.path().join("config.yml"), config);
        let mut origin = CountingAdapter::origin_without_cwd();
        origin.pane_cwd = Some(directory.path().to_path_buf());
        broker
            .execute_command(
                UiSessionId::new("generic-test"),
                ExecutionId([7; 16]),
                CoreExecutionId(7),
                muxe_core::CommandAction {
                    program: ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
                        "/usr/bin/true".to_owned(),
                    ))),
                    args: Vec::new(),
                    cwd: None,
                    env: Default::default(),
                },
                &origin,
                false,
                muxe_core::ExecutionPolicy {
                    mode: muxe_core::ExecutionMode::Detach,
                    timeout: None,
                    on_timeout: TimeoutAction::Detach,
                    on_menu_control: muxe_core::MenuControlAction::Detach,
                },
            )
            .await
            .expect("owned generic child starts");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if broker.generic.processes.lock().await.is_empty() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("generic child is reaped");
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn generic_cancellation_escalates_after_its_leader_exits_on_term() {
        let directory = tempfile::tempdir().expect("owned generic-process directory");
        let script = directory.path().join("term-resistant-group.sh");
        let descendant_pid = directory.path().join("descendant.pid");
        std::fs::write(
            &script,
            r#"(
    trap '' TERM
    while :; do sleep 1; done
) &
printf '%s\n' "$!" > "$1"
trap 'exit 0' TERM
while :; do sleep 1; done
"#,
        )
        .expect("write owned generic-process script");

        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .arg(&script)
            .arg(&descendant_pid)
            .current_dir(directory.path())
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().expect("start exact owned group leader");
        let process_group = child
            .id()
            .and_then(|pid| i32::try_from(pid).ok())
            .expect("owned group leader PID fits i32");
        let descendant = match tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(pid) = std::fs::read_to_string(&descendant_pid) {
                    if let Ok(pid) = pid.trim().parse::<i32>() {
                        return pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        {
            Ok(pid) => pid,
            Err(error) => {
                let _ = terminate_generic_child(&mut child, process_group).await;
                panic!("owned descendant did not publish its exact PID: {error}");
            }
        };

        let leader = terminate_generic_child(&mut child, process_group)
            .await
            .expect("TERM/KILL escalation reaps the exact group leader");
        assert!(
            leader.success(),
            "the leader traps TERM and exits before the descendant escalation"
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match nix::sys::signal::kill(Pid::from_raw(descendant), None) {
                    Err(Errno::ESRCH) => return,
                    Ok(()) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(error) => panic!("could not inspect exact owned descendant {descendant}: {error}"),
                }
            }
        })
        .await
        .expect("TERM-resistant descendant receives group SIGKILL and is reaped");
    }
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("configuration failed: {0}")]
    Configuration(#[from] ConfigError),
    #[error("host adapter failed: {0}")]
    Adapter(#[from] AdapterError),
    #[error("launch gate rejected request: {0}")]
    Gate(#[from] GateError),
    #[error("peer role {actual:?} cannot make this request; expected {expected:?}")]
    Role {
        expected: muxe_protocol::PeerRole,
        actual: muxe_protocol::PeerRole,
    },
    #[error("requested root menu does not exist: {0:?}")]
    UnknownMenu(muxe_protocol::MenuId),
    #[error("unknown UI session: {0:?}")]
    UnknownSession(UiSessionId),
    #[error("binding belongs to a stale configuration generation")]
    StaleGeneration,
    #[error("binding action is UI-local and must not be dispatched to the broker")]
    LocalMenuAction,
    #[error("adapter acknowledged a different execution identity")]
    MismatchedExecution,
    #[error("native action could not resolve against its immutable origin")]
    ContextUnavailable,
    #[error(
        "portable action parameter {parameter:?} is invalid after origin resolution: {message}"
    )]
    PortableResolution {
        parameter: &'static str,
        message: String,
    },
    #[error("generic command supervision failed: {0}")]
    GenericProcess(String),
    #[error("the active host cannot cancel this pending execution")]
    CancelUnsupported,
    #[error("a menu control is already pending for this execution")]
    PendingControlInFlight,
}
