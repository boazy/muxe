use std::{
    collections::{HashMap, HashSet},
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
    OriginHintSource, PendingPaneRegistration, PortableDispatchRequest,
    PostDismissalPortableDispatchRequest, ResolvedNativeAction, ResolvedPortableAction,
    UntrustedOriginHint,
};
use muxe_core::{
    ActionSpec, CommandAction, CompiledConfig, CompiledGeneration, ConfigAction,
    ExecutionId as CoreExecutionId, ExecutionPolicy, MenuAction, PaneId, TabId, TimeoutAction,
};
use muxe_protocol::{
    AbortUiLaunch, AttachUi, BrokerEvent, BrokerResponse, ClientRequest, DiagnosticCode, EventId,
    ExecutionId, ExecutionOutcome, HostKind, HostPaneId, HostTabId, InvocationDisposition,
    InvokeBinding, LiveServerIdentity, MenuControl, ModalScopeId, PendingLaunchToken,
    ProtocolDiagnostic, RegisterPendingPane, UiMenuControl, UiSessionId, WireMessage,
};
use thiserror::Error;
use tokio::{
    process::{Child, Command},
    sync::{Mutex, mpsc, watch},
};

use crate::{
    config::{ConfigError, ConfigStore, ConfigWatchSpec},
    gate::{GateError, LaunchGate, OsTokenSource, PendingLaunch, RegisteredPane, ScopeOwner},
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
    registering: HashSet<PendingLaunchToken>,
    pending_sessions: HashMap<PendingLaunchToken, UiSessionId>,
    executions: HashMap<UiSessionId, ExecutionRecord>,
    deferred: HashMap<UiSessionId, DeferredDispatch>,
    detached_executions: HashMap<CoreExecutionId, ExecutionId>,
    // Set by `drain_for_activation` before anything is torn down and cleared only when
    // the broker returns to Running. While set, no new launch or execution is admitted:
    // admissions check it atomically with insertion, and a dispatch accepted across
    // the boundary is cancelled immediately instead of leaking unsupervised past handoff.
    activation_sealed: bool,
}

struct SessionRecord {
    config: Arc<CompiledConfig>,
    root: muxe_core::MenuId,
    scope: muxe_adapter_api::ModalScopeId,
    origin: muxe_core::OriginContext,
    ui_pane: PaneId,
    capture: Option<CaptureLease>,
    readiness: watch::Sender<SessionReadiness>,
    events: mpsc::Sender<muxe_protocol::WireMessage>,
}

#[derive(Clone)]
enum SessionReadiness {
    Pending,
    Ready(Box<BrokerResponse>),
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

#[derive(Clone)]
struct DeferredDispatch {
    wire: ExecutionId,
    core: CoreExecutionId,
    request: PostDismissalPortableDispatchRequest,
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

#[expect(
    clippy::large_enum_variant,
    reason = "transient per-request disposition; Immediate carries the protocol attachment snapshot by design and boxing it would churn a dozen construction sites for a stack temporary"
)]
pub enum RequestResult {
    Immediate(BrokerResponse),
    WaitForAttachment(Box<PendingAttachment>),
}

pub struct PendingAttachment {
    session: UiSessionId,
    receiver: watch::Receiver<SessionReadiness>,
}

impl PendingAttachment {
    #[must_use]
    pub fn session(&self) -> &UiSessionId {
        &self.session
    }
    pub async fn wait(mut self) -> BrokerResponse {
        loop {
            match self.receiver.borrow().clone() {
                SessionReadiness::Ready(response) => return *response,
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

    /// Loads and host-validates the broker configuration.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` when the configuration cannot be read, parsed, or
    /// validated against the host adapter.
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

    /// Closes every menu-owned pending pane and releases every UI capture before an activation
    /// coordinator drops this broker's listener. Detached generic children remain supervised.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::ActivationDrainRefused` while a non-cancellable host
    /// execution is in flight, or the first cancellation/detach failure (which
    /// reopens admission so the coordinator can retry).
    pub async fn drain_for_activation(&self) -> Result<(), BrokerError> {
        let executions = {
            let mut state = self.state.lock().await;
            // Seal first: from this point no new launch or execution is admitted.
            state.activation_sealed = true;
            let deferred = state
                .deferred
                .iter()
                .next()
                .map(|(session, deferred)| (session.as_str().to_owned(), deferred.core.0));
            if let Some((session, core)) = deferred {
                state.activation_sealed = false;
                return Err(BrokerError::ActivationDrainRefused(format!(
                    "session {session} has post-dismissal execution {core} pending host acknowledgement",
                )));
            }
            let refused = state
                .executions
                .iter()
                .find(|(_, record)| record.owner == ExecutionOwner::Adapter && !record.cancellable)
                .map(|(session, record)| (session.as_str().to_owned(), record.core.0));
            if let Some((session, core)) = refused {
                // Refuse before mutating: an unresolved non-cancellable host mutation
                // must never be silently dropped across a handoff.
                state.activation_sealed = false;
                return Err(BrokerError::ActivationDrainRefused(format!(
                    "session {session} has a non-cancellable host execution {core} in flight",
                )));
            }
            std::mem::take(&mut state.executions)
        };
        let drained = async {
            for (_, record) in executions {
                match record.owner {
                    ExecutionOwner::Adapter => {
                        self.adapter
                            .cancel(record.core)
                            .await
                            .map_err(BrokerError::from)?;
                    }
                    ExecutionOwner::GenericProcess => {
                        let _ = self.cancel_generic(record.core).await;
                    }
                }
            }
            let (pending, pending_sessions) = {
                let mut state = self.state.lock().await;
                let pending = state.gate.drain();
                let pending_sessions = std::mem::take(&mut state.pending_sessions);
                (pending, pending_sessions)
            };
            for launch in &pending {
                if let Some(session) = pending_sessions.get(&launch.token) {
                    self.close_pending_launch(launch, session).await?;
                }
            }
            let sessions = self
                .sessions
                .lock()
                .await
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for session in sessions {
                self.detach(&session, CaptureReleaseReason::UiDismissed)
                    .await?;
            }
            Ok(())
        }
        .await;
        if drained.is_err() {
            // The broker stays Running after a failed drain, so reopen admission;
            // the coordinator can retry Prepare (drain is idempotent).
            self.state.lock().await.activation_sealed = false;
        }
        drained
    }

    /// Reports whether detached generic children are still under supervision.
    /// The supervisor-only linger after activation stop uses this to exit only
    /// after every remaining child is reaped.
    #[must_use]
    pub(crate) async fn has_supervised_children(&self) -> bool {
        !self.generic.processes.lock().await.is_empty()
    }

    /// Reopens launch and execution admission after a failed Prepare or a successful
    /// Abort returned the broker to Running. The coordinator enters Running first and
    /// then calls this; a concurrent admission landing in between is spuriously
    /// rejected (fail-closed) rather than wrongly admitted.
    pub async fn reopen_dispatch(&self) {
        self.state.lock().await.activation_sealed = false;
    }

    /// Admits an accepted execution unless the activation seal closed first. A dispatch
    /// accepted across the drain boundary is cancelled immediately and rejected: it
    /// must never run unsupervised past a handoff.
    async fn admit_execution(
        &self,
        session: UiSessionId,
        record: ExecutionRecord,
    ) -> Result<(), BrokerError> {
        let sealed = {
            let mut state = self.state.lock().await;
            if state.activation_sealed {
                Some(record)
            } else {
                state.executions.insert(session, record);
                None
            }
        };
        if let Some(record) = sealed {
            match record.owner {
                ExecutionOwner::Adapter => {
                    let _ = self.adapter.cancel(record.core).await;
                }
                ExecutionOwner::GenericProcess => {
                    let _ = self.cancel_generic(record.core).await;
                }
            }
            return Err(BrokerError::ActivationInProgress);
        }
        Ok(())
    }
    /// Stops the retained host subscription after UI drain and before the activation
    /// coordinator releases this broker's endpoint. The await proves the old stream
    /// is closed before any target connects; an explicit unsupported error fails
    /// the whole activation group closed instead of silently retaining the host.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the host cannot suspend for activation.
    pub async fn suspend_host_for_activation(&self) -> Result<(), BrokerError> {
        self.adapter
            .suspend_for_activation()
            .await
            .map_err(BrokerError::from)
    }

    /// Re-establishes the old host subscription after an activation abort, before
    /// the old endpoint accepts dispatch again. A failed resume leaves the adapter
    /// unhealthy; the caller must not reopen the endpoint as healthy.
    ///
    /// # Errors
    ///
    /// Returns the adapter error when the old host subscription cannot resume.
    pub async fn resume_host_after_abort(&self) -> Result<(), BrokerError> {
        self.adapter
            .resume_after_activation_abort()
            .await
            .map_err(BrokerError::from)
    }
    pub(crate) async fn config_watch_spec(&self) -> ConfigWatchSpec {
        self.config.watch_spec().await
    }

    /// Stops the host adapter after the broker has been retired. The service
    /// stop barrier awaits this before unlinking its endpoint.
    ///
    /// # Errors
    ///
    /// Returns the adapter shutdown error.
    pub async fn shutdown_host_adapter(&self) -> Result<(), BrokerError> {
        self.adapter.shutdown().await.map_err(BrokerError::from)
    }
    /// Keeps the previous immutable generation active if parsing, compilation, or active-host
    /// validation fails.
    /// # Errors
    ///
    /// Returns `BrokerError::Configuration` when parsing, compilation, or active-host
    /// validation fails; the previous generation stays active.
    pub async fn reload(&self) -> Result<CompiledGeneration, BrokerError> {
        self.config
            .reload(self.adapter.as_ref())
            .await
            .map_err(|error| BrokerError::Configuration(Box::new(error)))
    }

    /// Captures the live host identity for endpoint and activation decisions.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Adapter` when host identity is unavailable, including
    /// a suspended activation continuity.
    pub async fn live_identity(&self) -> Result<LiveServerIdentity, BrokerError> {
        let identity = self.adapter.identity().await.map_err(BrokerError::from)?;
        Ok(LiveServerIdentity {
            host: match identity.kind {
                muxe_adapter_api::HostKind::Zellij => HostKind::Zellij,
                muxe_adapter_api::HostKind::Herdr => HostKind::Herdr,
            },
            discovery_key: identity.discovery_key,
            server_id: muxe_protocol::ServerId::new(identity.live_server_id),
        })
    }

    /// Reports per-client commit-gate evidence, mapped from the adapter's native
    /// type. `None` when the adapter reports no per-client evidence or the query
    /// fails: status must stay observable even then, and the coordinator applies
    /// the host-appropriate gate.
    pub(crate) async fn activation_readiness(&self) -> Option<muxe_protocol::TargetReadiness> {
        match self.adapter.activation_readiness().await {
            Ok(Some(evidence)) => Some(muxe_protocol::TargetReadiness {
                registered_clients: evidence.registered_clients,
                member_clients: u64::try_from(evidence.member_clients.len()).unwrap_or(u64::MAX),
                member_ids: Some(evidence.member_clients),
            }),
            _ => None,
        }
    }

    /// Compares the claimed identity without disturbing host state.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Adapter` when host identity is unavailable.
    pub async fn serves_identity(&self, claimed: &LiveServerIdentity) -> Result<bool, BrokerError> {
        Ok(self.live_identity().await? == *claimed)
    }

    /// Dispatches one validated client request by peer role.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` for role violations, unknown menus or sessions, stale
    /// generations, gate rejections, adapter failures, or sealed activation.
    pub async fn handle(
        &self,
        role: muxe_protocol::PeerRole,
        request: ClientRequest,
        events: mpsc::Sender<muxe_protocol::WireMessage>,
    ) -> Result<RequestResult, BrokerError> {
        match request {
            ClientRequest::PrepareUiLaunch(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.prepare(request).await
            }
            ClientRequest::RegisterPendingPane(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.register(request).await?;
                Ok(RequestResult::Immediate(
                    BrokerResponse::PendingPaneRegistered,
                ))
            }
            ClientRequest::CommitUiLaunch(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.commit(request.token, request.pane).await?;
                Ok(RequestResult::Immediate(BrokerResponse::Acknowledged))
            }
            ClientRequest::AbortUiLaunch(AbortUiLaunch { token }) => {
                Self::require_role(role, muxe_protocol::PeerRole::Launcher)?;
                self.abort(token).await?;
                Ok(RequestResult::Immediate(BrokerResponse::Acknowledged))
            }
            ClientRequest::AttachUi(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.attach(request, events).await
            }
            ClientRequest::InvokeBinding(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.invoke(request).await
            }
            ClientRequest::MenuControl(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Ui)?;
                self.control(request).await
            }
            ClientRequest::DetachUi(request) => {
                Self::require_role(role, muxe_protocol::PeerRole::Ui)?;
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
            AdapterHealthEvent::Healthy { .. }
            | AdapterHealthEvent::CaptureReady { .. }
            | AdapterHealthEvent::Reconnected { .. } => {
                self.broadcast_health(true, None).await;
            }
            AdapterHealthEvent::Unhealthy { modal_scope, error } => match modal_scope {
                None => {
                    self.broadcast_health(false, Some(error)).await;
                }
                Some(scope) => {
                    self.scope_unhealthy(scope, error).await;
                }
            },
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
            if let Some((session, _)) = state
                .executions
                .iter()
                .find(|(_, record)| record.core == core)
            {
                let session = session.clone();
                let record = state.executions.remove(&session).expect("entry was found");
                (session, record)
            } else if let Some(wire) = state.detached_executions.remove(&core) {
                match outcome {
                    ExecutionOutcome::Succeeded => {
                        tracing::debug!(
                            ?wire,
                            execution = core.0,
                            "post-dismissal dispatch completed"
                        );
                    }
                    _ => {
                        tracing::error!(
                            ?wire,
                            execution = core.0,
                            diagnostic = ?diagnostic,
                            "post-dismissal dispatch did not complete successfully"
                        );
                    }
                }
                return;
            } else {
                return;
            }
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

    /// Invalidates only one expired modal scope. Scoped sessions are reported
    /// unhealthy and then detached (captures released, readiness failed, scope
    /// registration freed); adapter-owned in-flight executions fail closed and
    /// pending launches in the scope are aborted. Sessions in other scopes
    /// observe nothing and stay dispatchable, while a whole-host `None` loss
    /// keeps the existing global broadcast. Recovery precedence follows from
    /// full invalidation: a later global Healthy reaches only live sessions,
    /// so an expired client is never revived by another client's heartbeat.
    async fn scope_unhealthy(&self, scope: muxe_adapter_api::ModalScopeId, error: AdapterError) {
        let wire_scope = ModalScopeId::new(scope.as_str());
        let scoped = {
            self.sessions
                .lock()
                .await
                .iter()
                .filter(|(_, record)| record.scope == scope)
                .map(|(session, record)| (session.clone(), record.events.clone()))
                .collect::<Vec<_>>()
        };
        let message = error.to_string();
        for (_, events) in &scoped {
            let _ = events
                .send(WireMessage::Event {
                    event_id: self.new_event_id(),
                    event: BrokerEvent::AdapterHealthChanged {
                        healthy: false,
                        diagnostic: Some(diagnostic(DiagnosticCode::HostUnavailable, &message)),
                    },
                })
                .await;
        }
        let failed = {
            let state = self.state.lock().await;
            scoped
                .iter()
                .filter_map(|(session, _)| {
                    state
                        .executions
                        .get(session)
                        .map(|record| (session.clone(), record.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (session, record) in &failed {
            if record.owner == ExecutionOwner::Adapter && record.pending_control.is_none() {
                self.emit_execution_completed(
                    session.clone(),
                    record.wire,
                    ExecutionOutcome::Failed,
                    Some(diagnostic(DiagnosticCode::HostUnavailable, &message)),
                )
                .await;
            }
        }
        for (session, _) in &scoped {
            let _ = self
                .detach(session, CaptureReleaseReason::LeaseExpired)
                .await;
        }
        let tokens = {
            self.state
                .lock()
                .await
                .gate
                .pending_tokens_in_scope(&wire_scope)
        };
        for token in tokens {
            let (session, registration) = {
                let mut state = self.state.lock().await;
                let session = state.pending_sessions.remove(&token);
                let registration = state.gate.abort(token).ok().flatten();
                (session, registration)
            };
            if let Some(session) = session {
                self.fail_session(&session, CaptureReleaseReason::LeaseExpired)
                    .await;
                if let Some(registration) = registration {
                    let _ = self.close_registered(&session, registration).await;
                }
            }
        }
        self.state
            .lock()
            .await
            .gate
            .release_dangling_owner(&wire_scope);
    }

    /// Rejects a request arriving on the wrong connection role.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::Role` when `actual` differs from `expected`.
    fn require_role(
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
        let config = self.config.snapshot().await.config;
        if config
            .menu(&muxe_core::MenuId::new(request.root.as_str()))
            .is_none()
        {
            return Err(BrokerError::UnknownMenu(request.root));
        }
        let (pending, replaced, replaced_pending, replaced_session) = {
            let mut tokens = self.token_source.lock().await;
            let mut state = self.state.lock().await;
            // Atomic with gate insertion under the same lock: no launch token can be
            // minted across the drain boundary.
            if state.activation_sealed {
                return Err(BrokerError::ActivationInProgress);
            }
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
        let registration = {
            let mut state = self.state.lock().await;
            if state.activation_sealed {
                return Err(BrokerError::ActivationInProgress);
            }
            let session = state
                .pending_sessions
                .get(&request.token)
                .cloned()
                .ok_or(GateError::UnknownToken)?;
            let registered = RegisteredPane {
                pane: request.pane.clone(),
                temporary_tab: request.temporary_tab.clone(),
                lease: None,
            };
            state
                .gate
                .register_pending_pane(request.token, registered)?;
            let existing = state.gate.registered_pane(request.token)?;
            if existing.as_ref().is_some_and(|pane| pane.lease.is_some()) {
                return Ok(());
            }
            if !state.registering.insert(request.token) {
                return Err(BrokerError::Gate(GateError::RegistrationInProgress));
            }
            PendingPaneRegistration {
                ui_session: adapter_session(&session),
                pane: PaneId::new(request.pane.as_str()),
                temporary_tab: request
                    .temporary_tab
                    .as_ref()
                    .map(|tab| TabId::new(tab.as_str())),
            }
        };
        let lease = match self
            .adapter
            .register_pending_pane(registration.clone())
            .await
        {
            Ok(lease) => lease,
            Err(error) => {
                self.state.lock().await.registering.remove(&request.token);
                return Err(BrokerError::from(error));
            }
        };
        let mut state = self.state.lock().await;
        let valid = !state.activation_sealed
            && state.pending_sessions.contains_key(&request.token)
            && state
                .gate
                .registered_pane(request.token)
                .ok()
                .flatten()
                .is_some_and(|pane| {
                    pane.pane.as_str() == request.pane.as_str()
                        && pane.temporary_tab.as_ref().map(HostTabId::as_str)
                            == request.temporary_tab.as_ref().map(HostTabId::as_str)
                });
        state.registering.remove(&request.token);
        if !valid {
            drop(state);
            let _ = self.adapter.close_pending_pane(registration, lease).await;
            return Err(BrokerError::Gate(GateError::UnknownToken));
        }
        state.gate.bind_pending_lease(request.token, lease)?;
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "attachment transaction keeps gate publication, capture cleanup, and response ordering auditable"
    )]
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
            .map_err(BrokerError::from)?;
        let wire_scope = ModalScopeId::new(scope.as_str());
        let config = self.config.snapshot().await.config;
        if config
            .menu(&muxe_core::MenuId::new(request.root.as_str()))
            .is_none()
        {
            return Err(BrokerError::UnknownMenu(request.root));
        }

        let (origin_hint, caller_identity) = Self::origin_hints(&request);
        let session = {
            let mut state = self.state.lock().await;
            if state.activation_sealed {
                return Err(BrokerError::ActivationInProgress);
            }
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
            state.gate.attach(
                session.clone(),
                wire_scope.clone(),
                request.pane.clone(),
                request.pending_launch,
            )?;
            session
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
                let registration = {
                    let mut state = self.state.lock().await;
                    if let Some(token) = request.pending_launch {
                        state.pending_sessions.remove(&token);
                        if let Ok(registration) = state.gate.abort(token) {
                            registration
                        } else {
                            state.gate.detach(&session);
                            None
                        }
                    } else {
                        state.gate.detach(&session);
                        None
                    }
                };
                if let Some(registration) = registration
                    && let Err(cleanup_error) = self.close_registered(&session, registration).await
                {
                    tracing::error!(
                        %cleanup_error,
                        "origin capture failed and registered pane cleanup also failed"
                    );
                }
                return Err(BrokerError::from(error));
            }
        };

        let mut registration = None;
        if let Some(token) = request.pending_launch {
            let state = self.state.lock().await;
            let valid = state
                .gate
                .pending(token)
                .is_some_and(|launch| launch.attached_ui.as_ref() == Some(&session))
                && state.pending_sessions.get(&token) == Some(&session);
            if !valid {
                return Err(BrokerError::Gate(GateError::UnknownToken));
            }
            registration = state
                .gate
                .pending(token)
                .and_then(|launch| launch.registered_pane.clone());
        }

        let (readiness, receiver) = watch::channel(SessionReadiness::Pending);
        let record = SessionRecord {
            config: Arc::clone(&config),
            root: muxe_core::MenuId::new(request.root.as_str()),
            scope,
            origin,
            ui_pane: pane,
            capture: None,
            readiness,
            events,
        };
        self.sessions.lock().await.insert(session.clone(), record);

        let (ready, finalization_error): (bool, Option<BrokerError>) = {
            let mut state = self.state.lock().await;
            match request.pending_launch {
                Some(token) => {
                    let latest = state
                        .gate
                        .pending(token)
                        .and_then(|launch| launch.registered_pane.clone());
                    if state.activation_sealed {
                        let cleanup = state.gate.abort(token).ok().flatten();
                        state.pending_sessions.remove(&token);
                        if cleanup.is_some() {
                            registration = cleanup;
                        } else {
                            registration = latest;
                        }
                        (false, Some(BrokerError::ActivationInProgress))
                    } else {
                        match state.gate.publish_attached(token, &session) {
                            Ok(ready) => {
                                registration = latest;
                                if ready {
                                    state.pending_sessions.remove(&token);
                                }
                                (ready, None)
                            }
                            Err(error) => {
                                let cleanup = state.gate.abort(token).ok().flatten();
                                state.pending_sessions.remove(&token);
                                registration = cleanup.or(latest);
                                (false, Some(BrokerError::Gate(error)))
                            }
                        }
                    }
                }
                None => {
                    if state.activation_sealed {
                        (false, Some(BrokerError::ActivationInProgress))
                    } else if matches!(
                        state.gate.owner(&wire_scope),
                        Some(ScopeOwner::Ready(active)) if active == &session
                    ) {
                        (true, None)
                    } else {
                        (false, Some(BrokerError::Gate(GateError::ScopeMismatch)))
                    }
                }
            }
        };
        if let Some(error) = finalization_error {
            self.sessions.lock().await.remove(&session);
            if let Some(registration) = registration
                && let Err(cleanup_error) = self.close_registered(&session, registration).await
            {
                tracing::error!(
                    %cleanup_error,
                    "session publication failed and registered pane cleanup also failed"
                );
            }
            return Err(error);
        }
        if !ready {
            return Ok(RequestResult::WaitForAttachment(Box::new(
                PendingAttachment { session, receiver },
            )));
        }
        if let Err(error) = self.begin_capture(&session).await {
            if let Err(cleanup_error) = self
                .detach(&session, CaptureReleaseReason::UiDismissed)
                .await
            {
                tracing::error!(
                    %cleanup_error,
                    "capture initialization failed and session detach also failed"
                );
            }
            if let Some(registration) = registration
                && let Err(cleanup_error) = self.close_registered(&session, registration).await
            {
                tracing::error!(
                    %cleanup_error,
                    "capture initialization failed and registered pane cleanup also failed"
                );
            }
            return Err(error);
        }
        let response = match self.attached_response(&session).await {
            Ok(response) => response,
            Err(error) => {
                if let Err(cleanup_error) = self
                    .detach(&session, CaptureReleaseReason::UiDismissed)
                    .await
                {
                    tracing::error!(
                        %cleanup_error,
                        "attachment response failed and session detach also failed"
                    );
                }
                if let Some(registration) = registration
                    && let Err(cleanup_error) = self.close_registered(&session, registration).await
                {
                    tracing::error!(
                        %cleanup_error,
                        "attachment response failed and registered pane cleanup also failed"
                    );
                }
                return Err(error);
            }
        };
        if let Some(lease) = registration.as_ref().and_then(|pane| pane.lease.clone()) {
            self.adapter
                .release_pending_pane(lease)
                .await
                .map_err(BrokerError::from)?;
        }
        Ok(RequestResult::Immediate(response))
    }

    /// Maps the launcher-captured origin and caller identity into adapter hints.
    fn origin_hints(
        request: &AttachUi,
    ) -> (Option<UntrustedOriginHint>, Option<HostCallerIdentity>) {
        let origin_hint = request.origin.as_ref().map(|origin| UntrustedOriginHint {
            workspace_id: muxe_core::WorkspaceId::new(origin.workspace.as_str()),
            tab_id: muxe_core::TabId::new(origin.tab.as_str()),
            pane_id: PaneId::new(origin.pane.as_str()),
            cwd: origin.cwd.as_ref().map(std::path::PathBuf::from),
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
        (origin_hint, caller_identity)
    }

    async fn commit(&self, token: PendingLaunchToken, pane: HostPaneId) -> Result<(), BrokerError> {
        let (session, registration) = {
            let mut state = self.state.lock().await;
            let registration = state
                .gate
                .pending(token)
                .and_then(|launch| launch.registered_pane.clone());
            let session = state.gate.commit(token, &pane)?;
            let session = if let Some(session) = session {
                state.pending_sessions.remove(&token);
                Some(session)
            } else {
                None
            };
            (session, registration)
        };
        let Some(session) = session else {
            return Ok(());
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
            let _ = record
                .readiness
                .send(SessionReadiness::Ready(Box::new(response)));
        }
        drop(sessions);
        if let Some(lease) = registration.and_then(|pane| pane.lease) {
            self.adapter
                .release_pending_pane(lease)
                .await
                .map_err(BrokerError::from)?;
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
    /// Launcher disconnect only aborts launches that have not committed their
    /// placement. The gate transition and pending-session removal are atomic
    /// under one state lock; host cleanup happens only after unlocking.
    pub(crate) async fn abort_on_launcher_disconnect(
        &self,
        token: PendingLaunchToken,
    ) -> Result<(), BrokerError> {
        let outcome = {
            let mut state = self.state.lock().await;
            if state.gate.placement_committed(token) {
                None
            } else {
                let registration = state.gate.abort_if_uncommitted(token)?;
                let session = state
                    .pending_sessions
                    .remove(&token)
                    .ok_or(GateError::UnknownToken)?;
                Some((session, registration))
            }
        };
        let Some((session, registration)) = outcome else {
            return Ok(());
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
            .map_err(BrokerError::from)?
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
            .map_err(BrokerError::from)?;
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

    #[expect(
        clippy::too_many_lines,
        reason = "invocation ordering stays co-located: portable resolution, post-dismissal deferral, reload and menu short-circuits, capability gating, and cross-drain admission form one auditable sequence"
    )]
    async fn invoke(&self, request: InvokeBinding) -> Result<RequestResult, BrokerError> {
        let (origin, binding) = self.resolve_invoke_binding(&request).await?;
        let serial = self.next_execution.fetch_add(1, Ordering::Relaxed);
        let execution = Self::new_execution_id(serial);
        let core_execution = CoreExecutionId(serial);
        if let ActionSpec::Portable(action) = &binding.action {
            let action =
                ResolvedPortableAction::from_origin(action, &origin).map_err(
                    |error| match error {
                        muxe_core::PortableActionResolutionError::Context(_) => {
                            BrokerError::ContextUnavailable
                        }
                        muxe_core::PortableActionResolutionError::InvalidValue {
                            parameter,
                            message,
                            ..
                        } => BrokerError::PortableResolution { parameter, message },
                    },
                )?;
            if requires_post_dismissal(&action.action) {
                let ui_pane = {
                    let sessions = self.sessions.lock().await;
                    sessions
                        .get(&request.session)
                        .ok_or_else(|| BrokerError::UnknownSession(request.session.clone()))?
                        .ui_pane
                        .clone()
                };
                let deferred = DeferredDispatch {
                    wire: execution,
                    core: core_execution,
                    request: PostDismissalPortableDispatchRequest {
                        execution: core_execution,
                        action,
                        origin,
                        ui_pane,
                    },
                };
                let mut state = self.state.lock().await;
                if state.activation_sealed {
                    return Err(BrokerError::ActivationInProgress);
                }
                state.deferred.insert(request.session.clone(), deferred);
                return Ok(RequestResult::Immediate(
                    BrokerResponse::InvocationAccepted {
                        execution,
                        disposition: InvocationDisposition::Dismissed,
                    },
                ));
            }
        }
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
            ActionSpec::Portable(action) => match self
                .dispatch_portable(
                    &request.session,
                    execution,
                    core_execution,
                    origin,
                    action,
                    &binding.settings.execution,
                )
                .await?
            {
                std::ops::ControlFlow::Break(response) => return Ok(response),
                std::ops::ControlFlow::Continue(capabilities) => capabilities,
            },
            ActionSpec::Native(candidate) => {
                self.dispatch_native(core_execution, origin, &candidate)
                    .await?
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
            // A dispatch accepted across the drain boundary is cancelled at once and
            // rejected; only a pre-seal acceptance is admitted.
            self.admit_execution(
                request.session.clone(),
                ExecutionRecord {
                    wire: execution,
                    core: core_execution,
                    cancellable: capabilities.cancellable,
                    on_menu_control,
                    owner: ExecutionOwner::Adapter,
                    pending_control: None,
                },
            )
            .await?;
        }
        Ok(RequestResult::Immediate(
            BrokerResponse::InvocationAccepted {
                execution,
                disposition,
            },
        ))
    }

    /// Resolves the session config, immutable origin, and binding for one invocation.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` for unknown sessions or stale generations.
    async fn resolve_invoke_binding(
        &self,
        request: &InvokeBinding,
    ) -> Result<(muxe_core::OriginContext, muxe_core::CompiledBinding), BrokerError> {
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
        Ok((origin, binding))
    }

    /// Dispatches one portable action: command panes through the generic
    /// supervisor (which answers inline), everything else through the host adapter.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` for unresolvable origins, failed spawns, adapter
    /// rejections, or execution mismatches.
    async fn dispatch_portable(
        &self,
        session: &UiSessionId,
        execution: ExecutionId,
        core_execution: CoreExecutionId,
        origin: muxe_core::OriginContext,
        action: muxe_core::PortableAction,
        policy: &ExecutionPolicy,
    ) -> Result<
        std::ops::ControlFlow<RequestResult, Option<muxe_core::ExecutionCapabilities>>,
        BrokerError,
    > {
        let command_cwd_from_context = matches!(
            &action,
            muxe_core::PortableAction::Command(command)
                if command.cwd.as_ref().is_some_and(|cwd| matches!(
                    &cwd.value.kind,
                    muxe_core::ConfigValueKind::Context(_)
                ))
        );
        let action =
            ResolvedPortableAction::from_origin(&action, &origin).map_err(|error| match error {
                muxe_core::PortableActionResolutionError::Context(_) => {
                    BrokerError::ContextUnavailable
                }
                muxe_core::PortableActionResolutionError::InvalidValue {
                    parameter,
                    message,
                    ..
                } => BrokerError::PortableResolution { parameter, message },
            })?;
        match action.action {
            muxe_core::PortableAction::Command(command) => {
                self.execute_command(CommandLaunch {
                    session: session.clone(),
                    wire: execution,
                    core: core_execution,
                    command,
                    origin,
                    cwd_from_context: command_cwd_from_context,
                    policy: policy.clone(),
                })
                .await?;
                let disposition = if policy.mode == muxe_core::ExecutionMode::Await {
                    InvocationDisposition::Awaited
                } else {
                    InvocationDisposition::Detached
                };
                Ok(std::ops::ControlFlow::Break(RequestResult::Immediate(
                    BrokerResponse::InvocationAccepted {
                        execution,
                        disposition,
                    },
                )))
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
                    .map_err(BrokerError::from)?;
                if accepted.execution != core_execution {
                    return Err(BrokerError::MismatchedExecution);
                }
                Ok(std::ops::ControlFlow::Continue(Some(accepted.capabilities)))
            }
        }
    }

    /// Dispatches one native action through the host adapter.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError` for unresolvable origins, adapter rejections, or
    /// execution mismatches.
    async fn dispatch_native(
        &self,
        core_execution: CoreExecutionId,
        origin: muxe_core::OriginContext,
        candidate: &muxe_core::NativeActionCandidate,
    ) -> Result<Option<muxe_core::ExecutionCapabilities>, BrokerError> {
        let action = ResolvedNativeAction::from_origin(candidate, &origin)
            .map_err(|_| BrokerError::ContextUnavailable)?;
        let accepted = self
            .adapter
            .dispatch_native(muxe_adapter_api::NativeDispatchRequest {
                execution: core_execution,
                action,
                origin,
            })
            .await
            .map_err(BrokerError::from)?;
        if accepted.execution != core_execution {
            return Err(BrokerError::MismatchedExecution);
        }
        Ok(Some(accepted.capabilities))
    }

    pub(crate) async fn execute_command(&self, launch: CommandLaunch) -> Result<(), BrokerError> {
        let CommandLaunch {
            session,
            wire,
            core,
            command,
            origin,
            cwd_from_context,
            policy,
        } = launch;
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
        let cwd = resolve_command_cwd(&origin, command.cwd.as_ref(), cwd_from_context)?;
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
            // A child spawned across the drain boundary is signalled at once for its
            // supervisor to reap, and the invocation is rejected.
            self.admit_execution(
                session.clone(),
                ExecutionRecord {
                    wire,
                    core,
                    cancellable: true,
                    on_menu_control: policy.on_menu_control,
                    owner: ExecutionOwner::GenericProcess,
                    pending_control: None,
                },
            )
            .await?;
        }
        tokio::spawn(supervise_generic_child(GenericChildSpec {
            child,
            process_group,
            cancellation: cancellation_rx,
            timeout: policy.timeout,
            on_timeout: policy.on_timeout,
            session,
            core,
            state: Arc::clone(&self.state),
            sessions: Arc::clone(&self.sessions),
            generic: Arc::clone(&self.generic),
            next_event: Arc::clone(&self.next_event),
        }));
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
                    .map_err(BrokerError::from),
                ExecutionOwner::GenericProcess => self.cancel_generic(pending.core).await,
            },
            muxe_core::MenuControlAction::Cancel => Err(BrokerError::CancelUnsupported),
        };
        if let Err(error) = accepted
            && self
                .clear_pending_control(&request.session, pending.core, request.control)
                .await
        {
            return Err(error);
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

    async fn schedule_post_dismissal(&self, deferred: DeferredDispatch) {
        {
            self.state
                .lock()
                .await
                .detached_executions
                .insert(deferred.core, deferred.wire);
        }
        let expected = deferred.core;
        match self
            .adapter
            .dispatch_portable_after_ui_dismissal(deferred.request)
            .await
        {
            Ok(accepted) if accepted.execution == expected => {}
            Ok(accepted) => {
                self.state
                    .lock()
                    .await
                    .detached_executions
                    .remove(&expected);
                tracing::error!(
                    expected_execution = expected.0,
                    received_execution = accepted.execution.0,
                    "post-dismissal adapter dispatch acknowledged a different execution"
                );
            }
            Err(error) => {
                self.state
                    .lock()
                    .await
                    .detached_executions
                    .remove(&expected);
                tracing::error!(
                    execution = expected.0,
                    %error,
                    "post-dismissal portable dispatch failed"
                );
            }
        }
    }

    async fn detach(
        &self,
        session: &UiSessionId,
        reason: CaptureReleaseReason,
    ) -> Result<(), BrokerError> {
        let record = self.sessions.lock().await.remove(session);
        let (pending, deferred) = {
            let mut state = self.state.lock().await;
            state.gate.detach(session);
            (
                state.executions.remove(session),
                state.deferred.remove(session),
            )
        };
        if let Some(pending) = pending
            && pending.pending_control.is_none()
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
        if let Some(record) = record {
            let _ = record.readiness.send(SessionReadiness::Failed(diagnostic(
                DiagnosticCode::LaunchAborted,
                "UI session detached",
            )));
            if let Some(capture) = record.capture {
                self.adapter
                    .end_capture(capture, reason)
                    .await
                    .map_err(BrokerError::from)?;
            }
        }
        if let Some(deferred) = deferred {
            self.schedule_post_dismissal(deferred).await;
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
        let Some(lease) = registration.lease.clone() else {
            return Err(BrokerError::Gate(GateError::PaneMismatch));
        };
        self.adapter
            .close_pending_pane(
                PendingPaneRegistration {
                    ui_session: adapter_session(session),
                    pane: PaneId::new(registration.pane.as_str()),
                    temporary_tab: registration
                        .temporary_tab
                        .map(|tab| muxe_core::TabId::new(tab.as_str())),
                },
                lease,
            )
            .await
            .map_err(BrokerError::from)
    }

    fn new_session_id(&self) -> UiSessionId {
        UiSessionId::new(format!(
            "ui-{}",
            self.next_session.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn new_execution_id(counter: u64) -> ExecutionId {
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

/// Owned inputs for one generic command-pane launch.
pub(crate) struct CommandLaunch {
    pub(crate) session: UiSessionId,
    pub(crate) wire: ExecutionId,
    pub(crate) core: CoreExecutionId,
    pub(crate) command: CommandAction,
    pub(crate) origin: muxe_core::OriginContext,
    pub(crate) cwd_from_context: bool,
    pub(crate) policy: ExecutionPolicy,
}

/// Owned inputs for one supervised generic child.
struct GenericChildSpec {
    child: Child,
    process_group: i32,
    cancellation: watch::Receiver<Option<GenericCancellation>>,
    timeout: Option<Duration>,
    on_timeout: TimeoutAction,
    session: UiSessionId,
    core: CoreExecutionId,
    state: Arc<Mutex<BrokerState>>,
    sessions: Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    generic: Arc<GenericSupervisor>,
    next_event: Arc<AtomicU64>,
}

async fn supervise_generic_child(spec: GenericChildSpec) {
    let GenericChildSpec {
        mut child,
        process_group,
        mut cancellation,
        timeout,
        on_timeout,
        session,
        core,
        state,
        sessions,
        generic,
        next_event,
    } = spec;
    let handles = SupervisorHandles {
        state,
        sessions,
        generic,
        next_event,
    };
    let wait = await_child_exit(&mut child, &mut cancellation, timeout).await;
    match wait {
        GenericWait::Exited(status) => {
            let (outcome, diagnostic) = generic_exit_outcome(status);
            finish_generic(&session, core, outcome, diagnostic, true, &handles).await;
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
            finish_generic(&session, core, outcome, diagnostic, true, &handles).await;
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
                &handles,
            )
            .await;
            let _ = child.wait().await;
            handles.generic.processes.lock().await.remove(&core);
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
            finish_generic(&session, core, outcome, diagnostic, true, &handles).await;
        }
    }
}

/// Waits for a generic child to exit, be cancelled, or time out.
async fn await_child_exit(
    child: &mut Child,
    cancellation: &mut watch::Receiver<Option<GenericCancellation>>,
    timeout: Option<Duration>,
) -> GenericWait {
    match timeout {
        Some(timeout) => {
            tokio::select! {
                status = child.wait() => GenericWait::Exited(status),
                cancellation = await_generic_cancellation(cancellation) => GenericWait::Cancelled(cancellation),
                () = tokio::time::sleep(timeout) => GenericWait::TimedOut,
            }
        }
        None => {
            tokio::select! {
                status = child.wait() => GenericWait::Exited(status),
                cancellation = await_generic_cancellation(cancellation) => GenericWait::Cancelled(cancellation),
            }
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

/// Shared supervision handles for completion bookkeeping.
struct SupervisorHandles {
    state: Arc<Mutex<BrokerState>>,
    sessions: Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    generic: Arc<GenericSupervisor>,
    next_event: Arc<AtomicU64>,
}

async fn finish_generic(
    session: &UiSessionId,
    core: CoreExecutionId,
    outcome: ExecutionOutcome,
    diagnostic: Option<ProtocolDiagnostic>,
    remove_process: bool,
    handles: &SupervisorHandles,
) {
    let SupervisorHandles {
        state,
        sessions,
        generic,
        next_event,
    } = handles;
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

fn requires_post_dismissal(action: &muxe_core::PortableAction) -> bool {
    let (muxe_core::PortableAction::Tab(muxe_core::TabAction::Create { focus, .. })
    | muxe_core::PortableAction::Pane(muxe_core::PaneAction::Split { focus, .. })) = action
    else {
        return false;
    };
    focus
        .as_ref()
        .is_none_or(|focus| matches!(focus.value.kind, muxe_core::ConfigValueKind::Boolean(true)))
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
        None => Ok(captured()?.clone()),
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
            atomic::{AtomicBool, AtomicUsize, Ordering},
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
    use muxe_protocol::{BindingId, HostTabId, PeerRole};
    use tokio::sync::{Barrier, Notify, mpsc};

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

        async fn register_pending_pane(
            &self,
            registration: PendingPaneRegistration,
        ) -> Result<muxe_adapter_api::PendingPaneLease, AdapterError> {
            Ok(muxe_adapter_api::PendingPaneLease {
                id: muxe_adapter_api::PendingPaneLeaseId::new(format!(
                    "counting:{}",
                    registration.ui_session
                )),
                ui_session: registration.ui_session,
            })
        }

        async fn close_pending_pane(
            &self,
            _registration: PendingPaneRegistration,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn release_pending_pane(
            &self,
            _lease: muxe_adapter_api::PendingPaneLease,
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

        async fn dispatch_portable_after_ui_dismissal(
            &self,
            request: muxe_adapter_api::PostDismissalPortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            self.portable_dispatches.fetch_add(1, Ordering::SeqCst);
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("post-dismissal"),
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

    struct ScopedTestAdapter {
        dispatches: AtomicUsize,
        pending_releases: AtomicUsize,
        ended_captures: Mutex<Vec<(String, CaptureReleaseReason)>>,
        closed_panes: Mutex<Vec<(String, Option<String>)>>,
        origin_entered: Arc<Notify>,
        origin_release: Arc<Notify>,
        block_origin: AtomicBool,
        fail_origin: AtomicBool,
    }

    impl ActionValidator for ScopedTestAdapter {
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
    impl HostAdapter for ScopedTestAdapter {
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
                supports_capture: true,
                supports_notifications: false,
                supports_native_cancellation: false,
            })
        }

        async fn modal_scope(&self, ui_pane: &PaneId) -> Result<ModalScopeId, AdapterError> {
            Ok(ModalScopeId::new(ui_pane.as_str()))
        }

        async fn begin_capture(
            &self,
            request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            Ok(CaptureLease {
                id: CaptureLeaseId::new(request.ui_session.as_str()),
                ui_session: request.ui_session,
                modal_scope: request.modal_scope,
            })
        }

        async fn end_capture(
            &self,
            lease: CaptureLease,
            reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            self.ended_captures
                .lock()
                .await
                .push((lease.id.as_str().to_owned(), reason));
            Ok(())
        }

        async fn register_pending_pane(
            &self,
            registration: PendingPaneRegistration,
        ) -> Result<muxe_adapter_api::PendingPaneLease, AdapterError> {
            Ok(muxe_adapter_api::PendingPaneLease {
                id: muxe_adapter_api::PendingPaneLeaseId::new(format!(
                    "scoped:{}",
                    registration.ui_session
                )),
                ui_session: registration.ui_session,
            })
        }

        async fn close_pending_pane(
            &self,
            registration: PendingPaneRegistration,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), AdapterError> {
            self.closed_panes.lock().await.push((
                registration.pane.as_str().to_owned(),
                registration
                    .temporary_tab
                    .as_ref()
                    .map(|tab| tab.as_str().to_owned()),
            ));
            Ok(())
        }

        async fn release_pending_pane(
            &self,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), AdapterError> {
            self.pending_releases.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn capture_origin(
            &self,
            _request: OriginCaptureRequest,
        ) -> Result<OriginContext, AdapterError> {
            if self.block_origin.load(Ordering::SeqCst) {
                self.origin_entered.notify_one();
                self.origin_release.notified().await;
            }
            if self.fail_origin.load(Ordering::SeqCst) {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "origin capture failed in test",
                ));
            }
            Ok(CountingAdapter::origin_without_cwd())
        }

        async fn dispatch_portable(
            &self,
            request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            self.dispatches.fetch_add(1, Ordering::SeqCst);
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
            self.dispatches.fetch_add(1, Ordering::SeqCst);
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
            r"
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
",
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
    async fn focused_creation_arms_only_after_ui_detach() {
        let adapter = Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
        });
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<broker creation lifecycle>"),
            r"
version: 1
menus:
  main:
    bindings:
      t:
        label: create tab
        action: tab:create
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("creation configuration compiles");
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
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = broker
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
            .expect("UI attaches")
        else {
            panic!("expected immediate UI attachment");
        };
        let accepted = broker
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
                events.clone(),
            )
            .await
            .expect("focused creation is accepted");
        assert!(matches!(
            accepted,
            RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                disposition: InvocationDisposition::Dismissed,
                ..
            })
        ));
        assert_eq!(adapter.portable_dispatches.load(Ordering::SeqCst), 0);

        assert!(matches!(
            broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::DetachUi(muxe_protocol::DetachUi { session }),
                    events,
                )
                .await
                .expect("UI detaches"),
            RequestResult::Immediate(BrokerResponse::Detached)
        ));
        assert_eq!(adapter.portable_dispatches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn awaited_config_reload_emits_a_terminal_completion() {
        let adapter = Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
        });
        let yaml = r"
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
";
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
            r"
version: 1
menus:
  main:
    bindings:
      r:
        label: reload
        action: config:reload
",
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

    async fn prepared_registered_launch(
        broker: &Broker,
        pane: &str,
        temporary_tab: Option<&str>,
    ) -> PendingLaunchToken {
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new(pane),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 60_000,
            })
            .await
            .expect("launcher prepares");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = pending else {
            panic!("expected a prepared launch token");
        };
        broker
            .register(RegisterPendingPane {
                token,
                pane: HostPaneId::new(pane),
                temporary_tab: temporary_tab.map(HostTabId::new),
            })
            .await
            .expect("launcher registers the host pane");
        token
    }

    #[tokio::test]
    async fn prepare_rejects_unknown_menu_before_launching_a_pane() {
        let (_adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let result = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("unknown-root"),
                root: muxe_protocol::MenuId::new("missing"),
                lease_millis: 60_000,
            })
            .await;
        assert!(matches!(
            result,
            Err(BrokerError::UnknownMenu(menu)) if menu.as_str() == "missing"
        ));
    }

    #[tokio::test]
    async fn commit_before_attach_keeps_real_launch_pending_until_barrier_released() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "commit-first", Some("temporary-tab")).await;
        let barrier = Arc::new(Barrier::new(2));
        let commit_broker = Arc::clone(&broker);
        let commit_barrier = Arc::clone(&barrier);
        let commit = tokio::spawn(async move {
            commit_barrier.wait().await;
            commit_broker
                .commit(token, HostPaneId::new("commit-first"))
                .await
                .expect("placement commit succeeds before UI attach");
        });
        barrier.wait().await;
        commit.await.expect("commit task completes");
        {
            let state = broker.state.lock().await;
            assert!(state.gate.pending(token).is_some());
            assert!(state.pending_sessions.contains_key(&token));
        }
        let (events, _events_rx) = mpsc::channel(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new("commit-first"),
                    pending_launch: Some(token),
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events,
            )
            .await
            .expect("UI attaches after placement commit");
        assert!(matches!(
            attached,
            RequestResult::Immediate(BrokerResponse::UiAttached { .. })
        ));
        let state = broker.state.lock().await;
        assert_eq!(adapter.pending_releases.load(Ordering::SeqCst), 1);
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
    }

    #[tokio::test]
    async fn attach_before_register_rejects_wrong_pane_then_completes_exact_pane() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("attach-first"),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 60_000,
            })
            .await
            .expect("launcher prepares");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = pending else {
            panic!("expected a prepared launch token");
        };
        let barrier = Arc::new(Barrier::new(2));
        let attach_broker = Arc::clone(&broker);
        let attach_barrier = Arc::clone(&barrier);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_barrier.wait().await;
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("attach-first"),
                        pending_launch: Some(token),
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
                .expect("UI attaches before launcher registration")
        });
        barrier.wait().await;
        let RequestResult::WaitForAttachment(pending_attachment) =
            attach.await.expect("attach task completes")
        else {
            panic!("expected attach to wait for placement commit");
        };
        assert!(
            broker
                .commit(token, HostPaneId::new("attach-first"))
                .await
                .is_err(),
            "commit-before-register is rejected without consuming the launch"
        );
        assert!(
            broker
                .register(RegisterPendingPane {
                    token,
                    pane: HostPaneId::new("wrong-pane"),
                    temporary_tab: None,
                })
                .await
                .is_err()
        );
        broker
            .register(RegisterPendingPane {
                token,
                pane: HostPaneId::new("attach-first"),
                temporary_tab: Some(HostTabId::new("temporary-tab")),
            })
            .await
            .expect("exact attached pane registers");
        broker
            .commit(token, HostPaneId::new("attach-first"))
            .await
            .expect("exact pane commits after attach");
        assert!(matches!(
            pending_attachment.wait().await,
            BrokerResponse::UiAttached { .. }
        ));
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
        assert_eq!(adapter.pending_releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn commit_during_attach_completes_once_both_barriers_pass() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token = prepared_registered_launch(&broker, "commit-race", None).await;
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("commit-race"),
                        pending_launch: Some(token),
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
                .expect("UI attach reaches the origin barrier")
        });
        adapter.origin_entered.notified().await;
        broker
            .commit(token, HostPaneId::new("commit-race"))
            .await
            .expect("placement commit records while origin capture is blocked");
        {
            let state = broker.state.lock().await;
            assert!(
                state
                    .gate
                    .pending(token)
                    .is_some_and(|launch| launch.placement_committed)
            );
            drop(state);
            assert!(broker.sessions.lock().await.is_empty());
        }
        adapter.origin_release.notify_one();
        let attached = attach.await.expect("attach task completes");
        assert!(matches!(
            attached,
            RequestResult::Immediate(BrokerResponse::UiAttached { .. })
        ));
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
    }
    #[tokio::test]
    async fn attach_rechecks_activation_seal_at_final_publication() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token = prepared_registered_launch(&broker, "seal-race", Some("temporary-tab")).await;
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("seal-race"),
                        pending_launch: Some(token),
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
        });
        adapter.origin_entered.notified().await;
        broker.state.lock().await.activation_sealed = true;
        adapter.origin_release.notify_one();
        let result = attach.await.expect("sealed attach task completes");
        assert!(matches!(result, Err(BrokerError::ActivationInProgress)));
        assert!(broker.sessions.lock().await.is_empty());
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
    }

    #[tokio::test]
    async fn unscoped_attach_revalidates_scope_after_blocked_origin_capture() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("none-replace"),
                        pending_launch: None,
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
        });
        adapter.origin_entered.notified().await;
        let replacement = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("none-replace"),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 60_000,
            })
            .await
            .expect("replacement launch prepares");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = replacement
        else {
            panic!("expected replacement token");
        };
        adapter.origin_release.notify_one();
        assert!(
            attach
                .await
                .expect("blocked attach task completes")
                .is_err()
        );
        assert!(broker.sessions.lock().await.is_empty());
        broker.abort(token).await.expect("replacement aborts");
    }

    #[tokio::test]
    async fn abort_while_origin_capture_is_blocked_cleans_registered_pane() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "abort-blocked", Some("temporary-tab")).await;
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("abort-blocked"),
                        pending_launch: Some(token),
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
        });
        adapter.origin_entered.notified().await;
        broker
            .abort(token)
            .await
            .expect("abort owns blocked launch cleanup");
        adapter.origin_release.notify_one();
        assert!(
            attach
                .await
                .expect("blocked attach task completes")
                .is_err()
        );
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![("abort-blocked".to_owned(), Some("temporary-tab".to_owned()))]
        );
        assert!(broker.sessions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn expiry_while_origin_capture_is_blocked_cleans_registered_pane() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("expire-blocked"),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 1,
            })
            .await
            .expect("launcher prepares");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = pending else {
            panic!("expected launch token");
        };
        broker
            .register(RegisterPendingPane {
                token,
                pane: HostPaneId::new("expire-blocked"),
                temporary_tab: Some(HostTabId::new("temporary-tab")),
            })
            .await
            .expect("launcher registers pane");
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("expire-blocked"),
                        pending_launch: Some(token),
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
        });
        adapter.origin_entered.notified().await;
        tokio::time::sleep(Duration::from_millis(3)).await;
        broker.expire_pending().await;
        adapter.origin_release.notify_one();
        assert!(
            attach
                .await
                .expect("blocked attach task completes")
                .is_err()
        );
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![(
                "expire-blocked".to_owned(),
                Some("temporary-tab".to_owned())
            )]
        );
        assert!(broker.sessions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn replacement_while_origin_capture_is_blocked_cannot_resurrect_old_scope() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let old =
            prepared_registered_launch(&broker, "replace-blocked", Some("temporary-tab")).await;
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::new("main"),
                        pane: HostPaneId::new("replace-blocked"),
                        pending_launch: Some(old),
                        origin: None,
                        caller_identity: None,
                        theme: None,
                        color_scheme: None,
                    }),
                    events,
                )
                .await
        });
        adapter.origin_entered.notified().await;
        let replacement = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("replace-blocked"),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 60_000,
            })
            .await
            .expect("replacement launch prepares");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared {
            token: replacement_token,
            ..
        }) = replacement
        else {
            panic!("expected replacement token");
        };
        adapter.origin_release.notify_one();
        assert!(
            attach
                .await
                .expect("blocked attach task completes")
                .is_err()
        );
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![(
                "replace-blocked".to_owned(),
                Some("temporary-tab".to_owned())
            )]
        );
        broker
            .abort(replacement_token)
            .await
            .expect("replacement aborts");
        assert!(broker.sessions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn origin_capture_failure_closes_registered_pane_and_detaches_session() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "origin-fails", Some("temporary-tab")).await;
        adapter.fail_origin.store(true, Ordering::SeqCst);
        let (events, _events_rx) = mpsc::channel(1);
        let result = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new("origin-fails"),
                    pending_launch: Some(token),
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events,
            )
            .await;
        assert!(result.is_err());
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![("origin-fails".to_owned(), Some("temporary-tab".to_owned()))]
        );
        assert!(broker.sessions.lock().await.is_empty());
    }

    #[tokio::test]
    async fn abort_registered_launch_releases_real_pending_session_and_pane_state() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token = prepared_registered_launch(&broker, "abort-real", Some("temporary-tab")).await;
        assert!(
            broker
                .commit(token, HostPaneId::new("wrong-pane"))
                .await
                .is_err()
        );
        assert!(
            broker
                .state
                .lock()
                .await
                .gate
                .pending(token)
                .is_some_and(|launch| !launch.placement_committed)
        );
        broker
            .commit(token, HostPaneId::new("abort-real"))
            .await
            .expect("placement commit remains pending before UI attachment");
        assert!(
            broker
                .state
                .lock()
                .await
                .gate
                .pending(token)
                .is_some_and(|launch| launch.placement_committed)
        );
        broker
            .abort(token)
            .await
            .expect("abort closes registered pane");
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(
            state
                .gate
                .owner(&muxe_protocol::ModalScopeId::new("abort-real"))
                .is_none()
        );
        assert!(!state.pending_sessions.contains_key(&token));
        drop(state);
        assert!(broker.sessions.lock().await.is_empty());
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![("abort-real".to_owned(), Some("temporary-tab".to_owned()))]
        );
    }

    #[tokio::test]
    async fn expiry_after_registered_move_releases_real_pending_session_and_pane_state() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("expire-real"),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 1,
            })
            .await
            .expect("launcher prepares");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = pending else {
            panic!("expected a prepared launch token");
        };
        broker
            .register(RegisterPendingPane {
                token,
                pane: HostPaneId::new("expire-real"),
                temporary_tab: Some(HostTabId::new("temporary-tab")),
            })
            .await
            .expect("launcher registers moved pane");
        broker
            .commit(token, HostPaneId::new("expire-real"))
            .await
            .expect("placement commit remains pending before UI attachment");
        assert!(
            broker
                .state
                .lock()
                .await
                .gate
                .pending(token)
                .is_some_and(|launch| launch.placement_committed)
        );
        tokio::time::sleep(Duration::from_millis(3)).await;
        broker.expire_pending().await;
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(
            state
                .gate
                .owner(&muxe_protocol::ModalScopeId::new("expire-real"))
                .is_none()
        );
        assert!(!state.pending_sessions.contains_key(&token));
        drop(state);
        assert!(broker.sessions.lock().await.is_empty());
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![("expire-real".to_owned(), Some("temporary-tab".to_owned()))]
        );
    }
    async fn prepare_pending_scope(
        broker: &Broker,
    ) -> (PendingLaunchToken, PendingAttachment, UiSessionId) {
        // A launcher flow in a third scope, still uncommitted when expiry hits.
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("client-c"),
                root: muxe_protocol::MenuId::new("main"),
                lease_millis: 60_000,
            })
            .await
            .expect("launcher prepares in the pending scope");
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = pending else {
            panic!("expected a prepared launch token");
        };
        let (pending_events, _pending_rx) = mpsc::channel(8);
        let RequestResult::WaitForAttachment(pending_attachment) = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new("client-c"),
                    pending_launch: Some(token),
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                pending_events,
            )
            .await
            .expect("pending UI attaches to its launch")
        else {
            panic!("expected a pending attachment");
        };
        let pending_session = pending_attachment.session().clone();
        (token, *pending_attachment, pending_session)
    }

    fn scoped_two_client_fixture() -> (
        Arc<ScopedTestAdapter>,
        Arc<Broker>,
        muxe_core::BindingId,
        tempfile::TempDir,
    ) {
        let adapter = Arc::new(ScopedTestAdapter {
            dispatches: AtomicUsize::new(0),
            pending_releases: AtomicUsize::new(0),
            ended_captures: Mutex::new(Vec::new()),
            closed_panes: Mutex::new(Vec::new()),
            origin_entered: Arc::new(Notify::new()),
            origin_release: Arc::new(Notify::new()),
            block_origin: AtomicBool::new(false),
            fail_origin: AtomicBool::new(false),
        });
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<scoped health regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      n:
        label: probe
        action: native.test:probe
",
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
            .expect("probe binding is visible");
        let directory = tempfile::tempdir().expect("owned configuration directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        (adapter, broker, binding, directory)
    }

    async fn attach_ready(
        broker: &Broker,
        pane: &str,
    ) -> (UiSessionId, mpsc::Receiver<WireMessage>) {
        let (events, events_rx) = mpsc::channel(8);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::new("main"),
                    pane: HostPaneId::new(pane),
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
        (session, events_rx)
    }

    async fn assert_expired_isolated(
        broker: &Broker,
        adapter: &ScopedTestAdapter,
        expired: &UiSessionId,
        expired_rx: &mut mpsc::Receiver<WireMessage>,
    ) {
        let Some(WireMessage::Event {
            event:
                BrokerEvent::AdapterHealthChanged {
                    healthy: expired_reported,
                    ..
                },
            ..
        }) = expired_rx.recv().await
        else {
            panic!("expired client observes its health loss");
        };
        assert!(!expired_reported, "expired client is reported unavailable");
        assert!(
            !broker.sessions.lock().await.contains_key(expired),
            "expired client session is detached"
        );
        let (ended_len, ended_id, ended_reason) = {
            let ended = adapter.ended_captures.lock().await;
            (
                ended.len(),
                ended.first().map(|lease| lease.0.clone()),
                ended.first().map(|lease| lease.1),
            )
        };
        assert_eq!(ended_len, 1, "only the expired capture is released");
        assert_eq!(
            ended_id.as_deref(),
            Some(expired.as_str()),
            "the released capture belongs to the expired client"
        );
        assert!(
            matches!(ended_reason, Some(CaptureReleaseReason::LeaseExpired)),
            "the expired capture ends with the lease reason"
        );
        {
            let state = broker.state.lock().await;
            assert!(
                state
                    .gate
                    .owner(&muxe_protocol::ModalScopeId::new("client-a"))
                    .is_none(),
                "expired scope registration is freed for a fresh launcher flow"
            );
            assert!(
                matches!(
                    state
                        .gate
                        .owner(&muxe_protocol::ModalScopeId::new("client-b")),
                    Some(ScopeOwner::Ready(_))
                ),
                "healthy scope registration is retained"
            );
        }
    }

    async fn expire_pending_scope(
        broker: &Broker,
        token: PendingLaunchToken,
        pending_attachment: PendingAttachment,
        pending_session: &UiSessionId,
    ) {
        // The uncommitted launcher flow in the third scope is aborted by its
        // own expiry: its queue entry can never attach or commit afterwards.
        broker
            .handle_health_event(AdapterHealthEvent::Unhealthy {
                modal_scope: Some(muxe_adapter_api::ModalScopeId::new("client-c")),
                error: AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "heartbeat lease expired",
                ),
            })
            .await;
        {
            let state = broker.state.lock().await;
            assert!(
                state
                    .gate
                    .owner(&muxe_protocol::ModalScopeId::new("client-c"))
                    .is_none(),
                "pending scope registration is freed"
            );
            assert!(
                !state.pending_sessions.contains_key(&token),
                "pending launch in the expired scope is aborted"
            );
        }
        assert!(
            broker
                .commit(token, HostPaneId::new("client-c"))
                .await
                .is_err(),
            "aborted pending launch can no longer commit"
        );
        assert!(
            !broker.sessions.lock().await.contains_key(pending_session),
            "uncommitted session in the expired scope is detached"
        );
        assert!(
            matches!(pending_attachment.wait().await, BrokerResponse::Error(_)),
            "waiters on the aborted launch fail closed instead of hanging"
        );
    }

    async fn assert_healthy_unaffected(
        broker: &Broker,
        adapter: &ScopedTestAdapter,
        healthy: &UiSessionId,
        healthy_rx: &mut mpsc::Receiver<WireMessage>,
        binding: muxe_core::BindingId,
    ) {
        assert!(
            healthy_rx.try_recv().is_err(),
            "healthy client observes nothing from the other scope's expiry"
        );
        assert!(
            broker.sessions.lock().await.contains_key(healthy),
            "healthy client session stays attached"
        );
        let accepted = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::InvokeBinding(InvokeBinding {
                    session: healthy.clone(),
                    generation: 1,
                    binding: BindingId {
                        generation: binding.generation().0,
                        ordinal: binding.ordinal(),
                    },
                }),
                mpsc::channel(1).0,
            )
            .await
            .expect("healthy client stays dispatchable");
        assert!(
            matches!(
                accepted,
                RequestResult::Immediate(BrokerResponse::InvocationAccepted { .. })
            ),
            "healthy invocation is accepted after the other scope expired"
        );
        assert_eq!(
            adapter.dispatches.load(Ordering::SeqCst),
            1,
            "healthy dispatch reaches the adapter without a pipe restart"
        );
    }

    async fn recover_expired_scope(
        broker: &Broker,
        adapter: &ScopedTestAdapter,
        expired: &UiSessionId,
        expired_rx: &mut mpsc::Receiver<WireMessage>,
        healthy_rx: &mut mpsc::Receiver<WireMessage>,
    ) {
        broker
            .handle_health_event(AdapterHealthEvent::Healthy {
                identity: adapter.identity().await.expect("test adapter identity"),
            })
            .await;
        let Some(WireMessage::Event {
            event:
                BrokerEvent::AdapterHealthChanged {
                    healthy: recovered, ..
                },
            ..
        }) = healthy_rx.recv().await
        else {
            panic!("healthy client observes recovery");
        };
        assert!(recovered, "healthy client observes recovery");
        assert!(
            expired_rx.try_recv().is_err(),
            "expired client is never revived by another heartbeat"
        );
        assert!(
            !broker.sessions.lock().await.contains_key(expired),
            "expired session stays gone after recovery"
        );
        assert!(
            broker
                .prepare(muxe_protocol::PrepareUiLaunch {
                    modal_scope: muxe_protocol::ModalScopeId::new("client-a"),
                    root: muxe_protocol::MenuId::new("main"),
                    lease_millis: 60_000,
                })
                .await
                .is_ok(),
            "fresh launcher flow works in the expired scope after recovery"
        );
        let ended = adapter
            .ended_captures
            .try_lock()
            .expect("no leaked capture-log holder at test end");
        assert_eq!(ended.len(), 1, "recovery ends no further captures");
    }

    #[tokio::test]
    async fn scoped_health_loss_isolates_only_the_expired_client() {
        let (adapter, broker, binding, _directory) = scoped_two_client_fixture();
        let (expired, mut expired_rx) = attach_ready(&broker, "client-a").await;
        let (healthy, mut healthy_rx) = attach_ready(&broker, "client-b").await;
        let (token, pending_attachment, pending_session) = prepare_pending_scope(&broker).await;

        broker
            .handle_health_event(AdapterHealthEvent::Unhealthy {
                modal_scope: Some(muxe_adapter_api::ModalScopeId::new("client-a")),
                error: AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "heartbeat lease expired",
                ),
            })
            .await;
        assert_expired_isolated(&broker, &adapter, &expired, &mut expired_rx).await;

        expire_pending_scope(&broker, token, pending_attachment, &pending_session).await;
        assert_healthy_unaffected(&broker, &adapter, &healthy, &mut healthy_rx, binding).await;
        recover_expired_scope(
            &broker,
            &adapter,
            &expired,
            &mut expired_rx,
            &mut healthy_rx,
        )
        .await;
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
            r"
version: 1
menus:
  main:
    bindings:
      x:
        label: no-op
        action: config:reload
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let directory = tempfile::tempdir().expect("owned command cwd");
        let broker = Broker::from_compiled(adapter, directory.path().join("config.yml"), config);
        let mut origin = CountingAdapter::origin_without_cwd();
        origin.pane_cwd = Some(directory.path().to_path_buf());
        broker
            .execute_command(CommandLaunch {
                session: UiSessionId::new("generic-test"),
                wire: ExecutionId([7; 16]),
                core: CoreExecutionId(7),
                command: muxe_core::CommandAction {
                    program: ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
                        "/usr/bin/true".to_owned(),
                    ))),
                    args: Vec::new(),
                    cwd: None,
                    env: std::collections::BTreeMap::default(),
                },
                origin,
                cwd_from_context: false,
                policy: muxe_core::ExecutionPolicy {
                    mode: muxe_core::ExecutionMode::Detach,
                    timeout: None,
                    on_timeout: TimeoutAction::Detach,
                    on_menu_control: muxe_core::MenuControlAction::Detach,
                },
            })
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
        let descendant_ready = directory.path().join("descendant.ready");
        std::fs::write(
            &script,
            r#"trap 'exit 0' TERM
(
    trap '' TERM HUP
    : > "$2"
    while :; do sleep 1; done
) &
descendant="$!"
while [ ! -e "$2" ]; do sleep 0.05; done
printf '%s\n' "$descendant" > "$1"
while :; do sleep 1; done
"#,
        )
        .expect("write owned generic-process script");

        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .arg(&script)
            .arg(&descendant_pid)
            .arg(&descendant_ready)
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
                if let Ok(pid) = std::fs::read_to_string(&descendant_pid)
                    && let Ok(pid) = pid.trim().parse::<i32>()
                {
                    return pid;
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
                    Err(error) => {
                        panic!("could not inspect exact owned descendant {descendant}: {error}")
                    }
                }
            }
        })
        .await
        .expect("TERM-resistant descendant receives group SIGKILL and is reaped");
    }
}

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("activation is in progress; this broker is not accepting new launches or executions")]
    ActivationInProgress,
    #[error("configuration failed: {0}")]
    Configuration(Box<ConfigError>),
    #[error("host adapter failed: {0}")]
    Adapter(Box<AdapterError>),
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
    #[error("the active host cannot cancel this pending execution")]
    CancelUnsupported,
    #[error("activation drain refused with a non-cancellable host execution in flight: {0}")]
    ActivationDrainRefused(String),
    #[error("a menu control is already pending for this execution")]
    PendingControlInFlight,
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
}

impl From<AdapterError> for BrokerError {
    fn from(error: AdapterError) -> Self {
        Self::Adapter(Box::new(error))
    }
}

impl From<ConfigError> for BrokerError {
    fn from(error: ConfigError) -> Self {
        Self::Configuration(Box::new(error))
    }
}
