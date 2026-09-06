//! Native broker-side Zellij adapter.
//!
//! One adapter serves one live Zellij session. It owns the shared request/event
//! [`PipeChannel`] pair (constructor-injected so the adapter-contract suite can
//! simulate the split-pipe protocol), the per-client registration table, the
//! per-client capture table, per-client FIFO dispatch queues scheduled
//! deterministically onto the single request pipe, and the async [`HostAdapter`] face.
//!
//! Transport discipline, from the pinned CLI state machine
//! (`zellij-client/src/cli_client.rs`, `pipe_client`):
//!
//! - Exactly one global in-flight transport request. The broker does not write
//!   the next request until the matching `RequestReleased` acknowledgement
//!   arrives on the event pipe; that acknowledgement is not action success.
//! - A release deadline protects the queue: a stuck request pipe is replaced
//!   alone, and a state-changing request whose line may have reached the target
//!   becomes `outcome_unknown`, never replayed.
//! - Event-pipe failure pauses all new requests, replaces the subscription,
//!   invalidates every registration, and waits for fresh registrations before
//!   resuming each client. Late events from displaced registrations are ignored.
//!
//! Pane-to-client bootstrap works by fan-out: each bridge declines UI panes
//! outside its client and snapshots the prior pane when its currently focused
//! pane is the attaching UI pane. No unique match fails the bootstrap rather
//! than guessing.

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use muxe_adapter_api::{
    AdapterCapabilities, AdapterError, AdapterErrorKind, AdapterHealthEvent,
    CaptureLease as ApiCaptureLease, CaptureLeaseId, CaptureReleaseReason, CaptureRequest,
    DispatchAccepted, DispatchCompletion, ExecutionCorrelationId, HostAdapter, HostIdentity,
    HostKind as ApiHostKind, KeyboardCapabilities, ModalScopeId, NativeDispatchRequest,
    OriginCaptureRequest, PendingPaneRegistration, PortableDispatchRequest, UiSessionId,
};
use muxe_core::{
    ActionValidation, ActionValidator, ConfigDiagnostic, ExecutionCapabilities, ExecutionId,
    NativeActionCandidate, OriginContext, PaneId, PortableAction, SourceSpan,
};
use muxe_zellij_protocol::{
    BridgeRequest, CaptureEndReason, PipeEventKind, PipeRequest, ZellijOrigin,
    bridge_protocol_fingerprint, decode_event_line, encode_request_line,
    generated::{RawNativeCommand, ValidatedNativeCommand},
    generated_action_fingerprint, pinned_source_revision,
};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};

use crate::{
    ZellijValidator,
    capture::CaptureTable,
    origin::{OriginError, build_origin_context},
    parse::candidate_to_raw,
    pipes::{PipeChannel, PipeTransportError, RELEASE_TIMEOUT, channel_names},
    portable::{PortableError, PortableMapping, map_portable},
    registry::ZellijRegistry,
};

/// How long bootstrap retries the claim fan-out before failing rather than guessing.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(15);
/// How long capture waits for the bridge to confirm Locked mode.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long origin fan-out waits per client attempt before trying the next one.
const ORIGIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

type CaptureReady = Result<String, AdapterError>;
type CaptureWaiters = BTreeMap<[u8; 16], oneshot::Sender<CaptureReady>>;

/// Static adapter configuration. Live-server identity beyond the session name
/// is verified against bridge registrations, never assumed.
#[derive(Clone, Debug)]
pub struct ZellijAdapterConfig {
    /// Live Zellij session name (`ZELLIJ_SESSION_NAME`).
    pub session_name: String,
    /// Zellij executable used to spawn pipe children in production.
    pub zellij_exe: PathBuf,
}

impl ZellijAdapterConfig {
    /// Validates static configuration without touching any host.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the session name is empty.
    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the public host-adapter error; boxing it would burden every caller"
    )]
    pub fn validate(&self) -> Result<(), AdapterError> {
        if self.session_name.is_empty() {
            return Err(AdapterError::new(
                AdapterErrorKind::InvalidRequest,
                "Zellij session name must not be empty",
            ));
        }
        Ok(())
    }
}

/// Reads the live Zellij session name for host-first launcher selection.
///
/// The single source is `ZELLIJ_SESSION_NAME`; no `HERDR_*` value is read on
/// this path, so an explicit or auto-detected Zellij selection can never be
/// routed by Herdr environment. The broker may instead pass a session name
/// it already verified (current-session startup) directly into
/// [`ZellijAdapterConfig`]; this helper covers launchers that only inherit
/// the host environment.
///
/// # Errors
///
/// Returns [`AdapterError`] when the variable is missing, empty, or not
/// valid UTF-8.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the launcher-facing error contract; boxing it would add an allocation"
)]
pub fn zellij_session_from_env() -> Result<String, AdapterError> {
    match std::env::var("ZELLIJ_SESSION_NAME") {
        Ok(name) if !name.is_empty() => Ok(name),
        Ok(_) => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "ZELLIJ_SESSION_NAME must not be empty for a Zellij launch",
        )),
        Err(std::env::VarError::NotPresent) => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "ZELLIJ_SESSION_NAME is required for a Zellij launch",
        )),
        Err(std::env::VarError::NotUnicode(_)) => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "ZELLIJ_SESSION_NAME must be valid UTF-8",
        )),
    }
}

/// Resolves the `zellij` executable for pipe-child production spawns.
///
/// The resolver searches `PATH` in order and returns the first `zellij`
/// entry with readable metadata, mirroring the Herdr launcher's binary
/// search. No directory, alias, or fallback executable is consulted.
///
/// # Errors
///
/// Returns [`AdapterError`] when `PATH` is missing or no `zellij` entry is
/// found in it.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the launcher-facing error contract; boxing it would add an allocation"
)]
pub fn resolve_zellij_exe() -> Result<PathBuf, AdapterError> {
    let path = std::env::var_os("PATH").ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "PATH is required to resolve the installed Zellij executable",
        )
    })?;
    find_zellij_in_dirs(std::env::split_paths(&path)).ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "could not find a zellij executable on PATH",
        )
    })
}

/// Searches explicit directories for a `zellij` executable entry. Pure core
/// of [`resolve_zellij_exe`], kept separate so tests cover the search
/// without mutating the process environment.
fn find_zellij_in_dirs(directories: impl Iterator<Item = PathBuf>) -> Option<PathBuf> {
    directories
        .map(|directory| directory.join("zellij"))
        .find(|candidate| std::fs::metadata(candidate).is_ok())
}

/// One queued pipe payload. Dispatches carry an execution for completion
/// correlation; lifecycle lines (capture, origin) correlate through their own
/// waiters instead.
struct QueuedItem {
    request_id: [u8; 16],
    execution: Option<ExecutionId>,
    client_id: String,
    /// Taken for encoding; restored on retry so no clone is needed.
    payload: Option<BridgeRequest>,
}

struct InFlight {
    request_id: [u8; 16],
    generation: u64,
    registration: [u8; 16],
    execution: Option<ExecutionId>,
}

struct AdapterInner {
    config: ZellijAdapterConfig,
    validator: ZellijValidator,
    request: Arc<dyn PipeChannel>,
    event: Arc<dyn PipeChannel>,
    registry: Mutex<ZellijRegistry>,
    captures: Mutex<CaptureTable>,
    queues: Mutex<BTreeMap<String, VecDeque<QueuedItem>>>,
    in_flight: Mutex<Option<InFlight>>,
    live_executions: Mutex<HashSet<u64>>,
    pending_origin: Mutex<BTreeMap<String, oneshot::Sender<Result<ZellijOrigin, OriginError>>>>,
    pending_capture: Mutex<CaptureWaiters>,
    pane_claims: Mutex<BTreeMap<String, String>>,
    snapshots: Mutex<BTreeMap<String, ZellijOrigin>>,
    generation: AtomicU64,
    next_id: AtomicU64,
    next_correlation: AtomicU64,
    events_tx: mpsc::Sender<AdapterHealthEvent>,
    events_rx: Mutex<mpsc::Receiver<AdapterHealthEvent>>,
    shutdown: AtomicBool,
    /// Monotonic base for heartbeat-lease timestamps. Lease times are elapsed
    /// milliseconds on this clock, never wall-clock time.
    started: tokio::time::Instant,
}

/// The native Zellij adapter. Construct with injected channels; use
/// [`ZellijAdapter::connect`] for the production subprocess pair.
#[derive(Clone)]
pub struct ZellijAdapter {
    inner: Arc<AdapterInner>,
}

impl ZellijAdapter {
    /// Builds the adapter over injected channels and starts the event loop.
    ///
    /// The contract suite injects scripted channels with recorded lines;
    /// production callers use [`ZellijAdapter::connect`].
    pub fn new(
        config: ZellijAdapterConfig,
        request: Arc<dyn PipeChannel>,
        event: Arc<dyn PipeChannel>,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::channel(256);
        let adapter = Self {
            inner: Arc::new(AdapterInner {
                config,
                validator: ZellijValidator,
                request,
                event,
                registry: Mutex::new(ZellijRegistry::new()),
                captures: Mutex::new(CaptureTable::new()),
                queues: Mutex::new(BTreeMap::new()),
                in_flight: Mutex::new(None),
                live_executions: Mutex::new(HashSet::new()),
                pending_origin: Mutex::new(BTreeMap::new()),
                pending_capture: Mutex::new(BTreeMap::new()),
                pane_claims: Mutex::new(BTreeMap::new()),
                snapshots: Mutex::new(BTreeMap::new()),
                generation: AtomicU64::new(1),
                next_id: AtomicU64::new(1),
                next_correlation: AtomicU64::new(1),
                events_tx,
                events_rx: Mutex::new(events_rx),
                shutdown: AtomicBool::new(false),
                started: tokio::time::Instant::now(),
            }),
        };
        let worker = adapter.clone();
        tokio::spawn(async move {
            worker.event_loop().await;
        });
        adapter
    }

    /// Builds the adapter over the production `zellij pipe` child pair.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when static configuration is invalid or either
    /// child cannot be spawned. No host state is modified beyond spawning the
    /// two CLI children.
    pub async fn connect(config: ZellijAdapterConfig) -> Result<Self, AdapterError> {
        use crate::pipes::SubprocessChannel;
        config.validate()?;
        let (request_name, event_name) = channel_names(&config.session_name);
        let request = SubprocessChannel::launch(
            config.zellij_exe.clone(),
            config.session_name.clone(),
            request_name,
            None,
        )
        .await
        .map_err(transport_error)?;
        let subscribe = format!(
            "{{\"muxe\":\"subscribe\",\"protocol\":{}}}",
            muxe_zellij_protocol::BRIDGE_PROTOCOL_VERSION
        );
        let event = SubprocessChannel::launch(
            config.zellij_exe.clone(),
            config.session_name.clone(),
            event_name,
            Some(subscribe),
        )
        .await
        .map_err(transport_error)?;
        Ok(Self::new(config, request, event))
    }

    /// Injection point for the contract suite: the request channel under test.
    pub fn request_channel(&self) -> &Arc<dyn PipeChannel> {
        &self.inner.request
    }

    /// Injection point for the contract suite: the event channel under test.
    pub fn event_channel(&self) -> &Arc<dyn PipeChannel> {
        &self.inner.event
    }

    fn mint_id(&self) -> [u8; 16] {
        let counter = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&counter.to_le_bytes());
        id[8..].copy_from_slice(&counter.to_be_bytes());
        if id == [0; 16] {
            id[0] = 1;
        }
        id
    }

    /// Milliseconds elapsed on the monotonic adapter clock, for lease times.
    fn clock_millis(&self) -> u64 {
        self.inner
            .started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn correlation(&self) -> ExecutionCorrelationId {
        ExecutionCorrelationId::new(format!(
            "zellij-{}",
            self.inner.next_correlation.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn emit(&self, event: AdapterHealthEvent) {
        let _ = self.inner.events_tx.send(event).await;
    }

    async fn event_loop(&self) {
        // Consecutive failures back off so a dead channel can never busy-spin
        // the runtime and starve dispatch work on the same thread.
        let mut failures: u32 = 0;
        loop {
            if self.inner.shutdown.load(Ordering::Relaxed) {
                return;
            }
            match self.inner.event.next_line().await {
                Ok(line) => {
                    failures = 0;
                    self.handle_event_line(&line).await;
                }
                Err(_) => {
                    if self.inner.shutdown.load(Ordering::Relaxed) {
                        return;
                    }
                    failures = failures.saturating_add(1);
                    self.restart_whole_pipe().await;
                    let backoff = Duration::from_millis(10).saturating_mul(failures.min(100));
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    async fn handle_event_line(&self, line: &str) {
        let event = match decode_event_line(line) {
            Ok(event) => event,
            Err(_) => {
                self.restart_whole_pipe().await;
                return;
            }
        };
        match event.event {
            PipeEventKind::Register {
                client_id,
                current_pane,
                registration,
                identity,
                ..
            } => {
                self.on_register(client_id, current_pane, registration, identity)
                    .await
            }
            PipeEventKind::RequestReleased {
                request_id,
                channel_generation,
                registration,
            } => {
                self.on_released(request_id, channel_generation, registration)
                    .await;
            }
            PipeEventKind::DispatchAccepted { .. } => {}
            PipeEventKind::DispatchCompleted {
                request_id,
                execution,
                outcome,
            } => {
                self.on_completed(request_id, execution, outcome).await;
            }
            PipeEventKind::OriginSnapshot { ui_session, origin } => {
                if let Some(sender) = self.inner.pending_origin.lock().await.remove(&ui_session) {
                    let _ = sender.send(Ok(origin));
                }
            }
            PipeEventKind::OriginDeclined { ui_session, .. } => {
                if let Some(sender) = self.inner.pending_origin.lock().await.remove(&ui_session) {
                    let _ = sender.send(Err(OriginError::InvalidId {
                        field: "ui-pane",
                        reason: "bridge declined ownership of the pane",
                    }));
                }
            }
            PipeEventKind::CaptureReady { lease, prior_mode } => {
                if let Some(sender) = self.inner.pending_capture.lock().await.remove(&lease) {
                    let _ = sender.send(Ok(prior_mode));
                }
            }
            PipeEventKind::CaptureLost { lease, reason } => {
                self.inner.pending_capture.lock().await.remove(&lease);
                let loss = match reason {
                    muxe_zellij_protocol::CaptureLostReason::UserModeChanged => {
                        muxe_adapter_api::CaptureLossReason::UserModeChanged
                    }
                    muxe_zellij_protocol::CaptureLostReason::BridgeUnloading => {
                        muxe_adapter_api::CaptureLossReason::AdapterHealth
                    }
                };
                self.emit(AdapterHealthEvent::CaptureLost {
                    lease: ApiCaptureLease {
                        id: CaptureLeaseId::new(hex_id(&lease)),
                        ui_session: UiSessionId::new("unknown"),
                        modal_scope: ModalScopeId::new("unknown"),
                    },
                    reason: loss,
                })
                .await;
            }
            PipeEventKind::Heartbeat {
                registration,
                client_id,
            } => {
                let now = self.clock_millis();
                let _ = self
                    .inner
                    .registry
                    .lock()
                    .await
                    .heartbeat(&client_id, registration, now);
            }
        }
    }

    async fn on_register(
        &self,
        client_id: String,
        current_pane: Option<String>,
        registration: [u8; 16],
        identity: muxe_zellij_protocol::BridgeIdentity,
    ) {
        // A bridge that self-attests NativeVerified is rejected: native
        // verification is established locally from packaged/stable bytes, and
        // the plugin SDK exposes no digest of its own loaded bytes. Only
        // Unattested is an honest bridge report; anything else fails closed
        // until the user decides the artifact-hash proposal.
        let honestly_attested = matches!(
            identity.artifact,
            muxe_zellij_protocol::BridgeArtifact::Unattested
        );
        let compatible = honestly_attested
            && identity.source_revision == pinned_source_revision()
            && identity.action_fingerprint == generated_action_fingerprint().0
            && identity.protocol_fingerprint == bridge_protocol_fingerprint().0;
        let now = self.clock_millis();
        let displaced = self.inner.registry.lock().await.register(
            &client_id,
            registration,
            current_pane,
            identity.muxe_version,
            compatible,
            now,
        );
        if displaced.is_err() {
            return;
        }
        if compatible {
            self.emit(AdapterHealthEvent::Healthy {
                identity: self.host_identity(),
            })
            .await;
        } else {
            self.emit(AdapterHealthEvent::Unhealthy {
                modal_scope: Some(Self::scope_for_client(&client_id)),
                error: AdapterError::new(
                    AdapterErrorKind::Incompatible,
                    "Zellij bridge handshake fingerprints do not match the native record",
                ),
            })
            .await;
        }
        self.pump_all().await;
    }

    async fn on_released(
        &self,
        request_id: [u8; 16],
        channel_generation: u64,
        registration: [u8; 16],
    ) {
        let matches = self
            .inner
            .in_flight
            .lock()
            .await
            .as_ref()
            .is_some_and(|pending| {
                pending.request_id == request_id
                    && pending.generation == channel_generation
                    && pending.registration == registration
            });
        if !matches {
            // Late acknowledgement from a restarted channel: ignore.
            return;
        }
        *self.inner.in_flight.lock().await = None;
        self.pump_all().await;
    }

    async fn on_completed(
        &self,
        request_id: [u8; 16],
        execution: String,
        outcome: muxe_zellij_protocol::CommandOutcome,
    ) {
        let _ = request_id;
        let execution_id: u64 = execution.parse().unwrap_or(u64::MAX);
        if !self
            .inner
            .live_executions
            .lock()
            .await
            .remove(&execution_id)
        {
            // Completion for a forgotten execution (restart cleared it): ignore.
            return;
        }
        let completion = match outcome.status {
            muxe_zellij_protocol::CommandStatus::Succeeded => DispatchCompletion::Succeeded {
                execution: ExecutionId(execution_id),
            },
            muxe_zellij_protocol::CommandStatus::Failed => DispatchCompletion::Failed {
                execution: ExecutionId(execution_id),
                error: AdapterError::new(AdapterErrorKind::DispatchFailed, outcome.detail),
            },
        };
        self.emit(AdapterHealthEvent::DispatchCompleted(completion))
            .await;
    }

    async fn enqueue(&self, item: QueuedItem) {
        if let Some(execution) = item.execution {
            self.inner.live_executions.lock().await.insert(execution.0);
        }
        self.inner
            .queues
            .lock()
            .await
            .entry(item.client_id.clone())
            .or_default()
            .push_back(item);
        self.pump_all().await;
    }

    async fn pump_all(&self) {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return;
        }
        if self.inner.in_flight.lock().await.is_some() {
            return;
        }
        // Deterministic client order keeps simulation stable and prevents a
        // busy client from starving another.
        let order: Vec<String> = {
            let queues = self.inner.queues.lock().await;
            let mut clients: Vec<String> = queues
                .iter()
                .filter(|(_, queue)| !queue.is_empty())
                .map(|(client, _)| client.clone())
                .collect();
            clients.sort();
            clients
        };
        for client in order {
            if self.pump_one(&client).await {
                return;
            }
        }
    }

    async fn pump_one(&self, client_id: &str) -> bool {
        // One respawn retry inline: after replacing the child the same head
        // item is attempted again without recursing through pump_all.
        for _ in 0..2 {
            let registration = {
                let registry = self.inner.registry.lock().await;
                match registry.get(client_id) {
                    Some(record) if record.compatible => record.registration,
                    _ => return false,
                }
            };
            let item = {
                let mut queues = self.inner.queues.lock().await;
                match queues
                    .get_mut(client_id)
                    .and_then(|queue| queue.pop_front())
                {
                    Some(item) => item,
                    None => return false,
                }
            };
            if self.send_item(client_id, registration, item).await {
                return true;
            }
        }
        false
    }

    /// Sends one queued item; returns whether the pipe accepted it.
    async fn send_item(
        &self,
        client_id: &str,
        registration: [u8; 16],
        mut item: QueuedItem,
    ) -> bool {
        item.client_id = client_id.to_owned();
        let generation = self.inner.generation.load(Ordering::Relaxed);
        let request_id = item.request_id;
        let Some(payload) = item.payload.take() else {
            self.emit(AdapterHealthEvent::Unhealthy {
                modal_scope: Some(Self::scope_for_client(client_id)),
                error: AdapterError::new(
                    AdapterErrorKind::InvalidRequest,
                    "queued item lost its payload",
                ),
            })
            .await;
            return false;
        };
        let request = PipeRequest {
            protocol: muxe_zellij_protocol::BRIDGE_PROTOCOL_VERSION,
            request_id,
            channel_generation: generation,
            target: muxe_zellij_protocol::BridgeTarget {
                client_id: client_id.to_owned(),
                registration,
            },
            payload,
        };
        let line = match encode_request_line(&request) {
            Ok(line) => line,
            Err(error) => {
                self.emit(AdapterHealthEvent::Unhealthy {
                    modal_scope: Some(Self::scope_for_client(client_id)),
                    error: AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string()),
                })
                .await;
                if let Some(execution) = item.execution {
                    self.inner.live_executions.lock().await.remove(&execution.0);
                }
                return false;
            }
        };
        let PipeRequest { payload, .. } = request;
        item.payload = Some(payload);
        if let Err(error) = self.inner.request.send_line(line).await {
            // The line may still have reached the host: at-most-once means a
            // dispatch becomes outcome_unknown here, never replayed. Lifecycle
            // lines have their own waiters time out; the pipe is still replaced.
            if let Some(execution) = item.execution {
                self.inner.live_executions.lock().await.remove(&execution.0);
                self.emit(AdapterHealthEvent::DispatchCompleted(
                    DispatchCompletion::OutcomeUnknown {
                        execution,
                        error: transport_error(error),
                    },
                ))
                .await;
            }
            // Requeue the lifecycle head for the retry pass; dispatches already
            // completed as unknown and must not replay.
            if item.execution.is_none() {
                self.inner
                    .queues
                    .lock()
                    .await
                    .entry(client_id.to_owned())
                    .or_default()
                    .push_front(item);
            }
            self.replace_request_child().await;
            return false;
        }
        *self.inner.in_flight.lock().await = Some(InFlight {
            request_id,
            generation,
            registration,
            execution: item.execution,
        });
        self.watch_release(request_id, generation);
        true
    }

    fn watch_release(&self, request_id: [u8; 16], generation: u64) {
        // A release deadline protects the global queue: a bridge that never
        // releases (crashed between receipt and acknowledgement) must not wedge
        // every client. Completions still arrive on the event pipe afterwards.
        let adapter = self.clone();
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep(RELEASE_TIMEOUT).await;
            let stuck = inner
                .in_flight
                .lock()
                .await
                .as_ref()
                .is_some_and(|pending| {
                    pending.request_id == request_id && pending.generation == generation
                });
            if !stuck {
                return;
            }
            let pending = inner.in_flight.lock().await.take();
            if let Some(pending) = pending
                && let Some(execution) = pending.execution
            {
                inner.live_executions.lock().await.remove(&execution.0);
                let _ = inner
                    .events_tx
                    .send(AdapterHealthEvent::DispatchCompleted(
                        DispatchCompletion::OutcomeUnknown {
                            execution,
                            error: AdapterError::new(
                                AdapterErrorKind::OutcomeUnknown,
                                "Zellij bridge did not release the request pipe in time",
                            ),
                        },
                    ))
                    .await;
            }
            let _ = inner.request.respawn().await;
            adapter.pump_all().await;
        });
    }

    async fn replace_request_child(&self) {
        // No pump here: the caller's retry loop re-attempts the head item, so
        // replacing never recurses back through pump_all.
        if self.inner.request.respawn().await.is_err() {
            self.restart_whole_pipe().await;
        }
    }

    async fn restart_whole_pipe(&self) {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return;
        }
        self.inner.generation.fetch_add(1, Ordering::Relaxed);
        // The in-flight request may have reached the host: outcome unknown, never replayed.
        if let Some(pending) = self.inner.in_flight.lock().await.take()
            && let Some(execution) = pending.execution
        {
            self.inner.live_executions.lock().await.remove(&execution.0);
            self.emit(AdapterHealthEvent::DispatchCompleted(
                DispatchCompletion::OutcomeUnknown {
                    execution,
                    error: AdapterError::new(
                        AdapterErrorKind::OutcomeUnknown,
                        "event pipe failed while a request was in flight",
                    ),
                },
            ))
            .await;
        }
        self.inner.registry.lock().await.invalidate_all();
        self.inner.captures.lock().await.invalidate_all_clients();
        self.inner.pane_claims.lock().await.clear();
        self.inner.snapshots.lock().await.clear();
        let request_ok = self.inner.request.respawn().await.is_ok();
        let event_ok = self.inner.event.respawn().await.is_ok();
        if !request_ok || !event_ok {
            self.emit(AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Zellij pipe children failed; waiting for fresh registrations",
                ),
            })
            .await;
        }
    }

    fn host_identity(&self) -> HostIdentity {
        HostIdentity {
            kind: ApiHostKind::Zellij,
            discovery_key: self.inner.config.session_name.clone(),
            live_server_id: format!("zellij-session:{}", self.inner.config.session_name),
        }
    }

    fn scope_for_client(client_id: &str) -> ModalScopeId {
        ModalScopeId::new(format!("zellij-client:{client_id}"))
    }

    #[expect(
        clippy::result_large_err,
        reason = "the HostAdapter implementation returns AdapterError without allocations"
    )]
    fn client_for_scope(scope: &ModalScopeId) -> Result<String, AdapterError> {
        scope
            .as_str()
            .strip_prefix("zellij-client:")
            .map(str::to_owned)
            .ok_or_else(|| invalid_request("modal scope is not a Zellij client scope"))
    }

    async fn active_registration(&self, client_id: &str) -> Result<[u8; 16], AdapterError> {
        let registry = self.inner.registry.lock().await;
        match registry.get(client_id) {
            Some(record) if record.compatible => Ok(record.registration),
            Some(_) => Err(AdapterError::new(
                AdapterErrorKind::Incompatible,
                format!("Zellij client {client_id} bridge is incompatible"),
            )),
            None => Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!("no active Zellij bridge for client {client_id}"),
            )),
        }
    }

    /// Resolves the unique client owning a pane through the claim fan-out.
    ///
    /// Each active compatible bridge is asked for a snapshot; the bridge whose
    /// focused pane is the UI pane answers, the rest decline. Retries until the
    /// bootstrap budget expires (focus may not have settled yet), then fails
    /// rather than guessing.
    async fn resolve_client_for_pane(
        &self,
        ui_session: &str,
        ui_pane: &str,
    ) -> Result<String, AdapterError> {
        if let Some(client) = self.inner.pane_claims.lock().await.get(ui_pane).cloned() {
            return Ok(client);
        }
        if let Some(snapshot) = self.inner.snapshots.lock().await.get(ui_pane).cloned() {
            return Ok(snapshot.client_id);
        }
        let deadline = tokio::time::Instant::now() + BOOTSTRAP_TIMEOUT;
        loop {
            let clients: Vec<(String, [u8; 16])> = {
                let registry = self.inner.registry.lock().await;
                registry
                    .client_ids()
                    .into_iter()
                    .filter_map(|client| {
                        registry
                            .get(&client)
                            .filter(|record| record.compatible)
                            .map(|record| (client, record.registration))
                    })
                    .collect()
            };
            for (client_id, registration) in clients {
                match self
                    .request_origin_from(&client_id, registration, ui_session, ui_pane)
                    .await
                {
                    Ok(snapshot) => {
                        self.inner
                            .pane_claims
                            .lock()
                            .await
                            .insert(ui_pane.to_owned(), client_id.clone());
                        self.inner
                            .snapshots
                            .lock()
                            .await
                            .insert(ui_pane.to_owned(), snapshot);
                        return Ok(client_id);
                    }
                    Err(_) => continue,
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "no unique Zellij client owns the UI pane",
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Requests an origin snapshot from one client with a bounded wait.
    async fn request_origin_from(
        &self,
        client_id: &str,
        registration: [u8; 16],
        ui_session: &str,
        ui_pane: &str,
    ) -> Result<ZellijOrigin, OriginError> {
        let pipe_request = PipeRequest {
            protocol: muxe_zellij_protocol::BRIDGE_PROTOCOL_VERSION,
            request_id: self.mint_id(),
            channel_generation: self.inner.generation.load(Ordering::Relaxed),
            target: muxe_zellij_protocol::BridgeTarget {
                client_id: client_id.to_owned(),
                registration,
            },
            payload: BridgeRequest::RequestOrigin {
                ui_session: ui_session.to_owned(),
                ui_pane: ui_pane.to_owned(),
            },
        };
        let line = encode_request_line(&pipe_request).map_err(|_| OriginError::InvalidId {
            field: "request",
            reason: "could not encode origin request",
        })?;
        // Origin requests bypass the dispatch queue: they run during attach,
        // before any UI session exists to order against. The single-flight
        // rule still applies because attach is serialized per modal scope.
        if self.inner.request.send_line(line).await.is_err() {
            return Err(OriginError::InvalidId {
                field: "transport",
                reason: "request pipe unavailable",
            });
        }
        let (sender, receiver) = oneshot::channel();
        self.inner
            .pending_origin
            .lock()
            .await
            .insert(ui_session.to_owned(), sender);
        let result = timeout(ORIGIN_ATTEMPT_TIMEOUT, receiver).await;
        self.inner.pending_origin.lock().await.remove(ui_session);
        match result {
            Ok(Ok(snapshot)) => {
                // The bridge must still own this registration when answering.
                if self
                    .inner
                    .registry
                    .lock()
                    .await
                    .check(client_id, registration)
                    .is_err()
                {
                    return Err(OriginError::InvalidId {
                        field: "registration",
                        reason: "bridge registration turned over during capture",
                    });
                }
                snapshot
            }
            _ => Err(OriginError::InvalidId {
                field: "ui-pane",
                reason: "origin capture timed out for this client",
            }),
        }
    }

    async fn dispatch_commands(
        &self,
        execution: ExecutionId,
        origin: &OriginContext,
        commands: Vec<RawNativeCommand>,
    ) -> Result<DispatchAccepted, AdapterError> {
        let client_id = origin
            .client_id
            .as_ref()
            .map(|id| id.as_str().to_owned())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Zellij dispatch requires the captured origin client",
                )
            })?;
        self.dispatch_to_client(execution, client_id, commands)
            .await
    }

    async fn dispatch_to_client(
        &self,
        execution: ExecutionId,
        client_id: String,
        commands: Vec<RawNativeCommand>,
    ) -> Result<DispatchAccepted, AdapterError> {
        // Gate acceptance on a live compatible registration: pump re-resolves
        // the registration at send time, but an unknown or incompatible
        // client must fail here instead of queueing forever.
        self.active_registration(&client_id).await?;
        for raw in commands {
            // Mandatory immediate runtime validation of the fully resolved
            // candidate. The single clone per command is structural: the typed
            // pipe payload retains the raw mirror while the generated `TryFrom`
            // conversion is the validation check, and both are needed.
            ValidatedNativeCommand::try_from(raw.clone()).map_err(|error| {
                AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string())
            })?;
            self.enqueue(QueuedItem {
                request_id: self.mint_id(),
                // Keystroke sequences share one broker execution across their
                // ordered pipe requests; each request still completes
                // individually on the event pipe.
                execution: Some(execution),
                client_id: client_id.clone(),
                payload: Some(BridgeRequest::Dispatch {
                    execution: execution.0.to_string(),
                    command: raw,
                }),
            })
            .await;
        }
        Ok(DispatchAccepted {
            correlation: self.correlation(),
            execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    /// Launches one host pane for `muxe menu open` / `muxe pane open`.
    ///
    /// The launch is validated purely predispatch (program, menu focus, menu
    /// working directory, placement) before any host contact, then resolved
    /// to its target client and dispatched with the same correlation as any
    /// native action. Menu launches require focus and the captured absolute
    /// origin working directory; the constructor enforces both before
    /// anything reaches the host. An explicit parent pane (`UiPane`) selects
    /// the destination client scope through live registrations and fails
    /// rather than guessing; placement stays relative to that client's
    /// focused pane because the pinned actions expose no parent-pane field.
    /// The returned acceptance means Zellij took the dispatch, never that
    /// the launched process succeeded.
    ///
    /// This method performs no focus substitution: the target is always the
    /// explicit client or the resolved owning client, never a guessed
    /// currently focused client.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] for invalid launch specs, unresolvable
    /// targets, or clients without a live compatible bridge.
    pub async fn launch_pane(
        &self,
        execution: ExecutionId,
        launch: crate::launch::ZellijPaneLaunch,
    ) -> Result<DispatchAccepted, AdapterError> {
        let (raw, target) = launch
            .into_command()
            .map_err(|error| error.into_adapter_error())?;
        let client_id = match target {
            crate::launch::LaunchTarget::Client(client) => client,
            crate::launch::LaunchTarget::UiPane(pane) => {
                let probe = format!("launch-{}", hex_id(&self.mint_id()));
                self.resolve_client_for_pane(&probe, pane.as_str()).await?
            }
        };
        self.dispatch_to_client(execution, client_id, vec![raw])
            .await
    }

    async fn enqueue_lifecycle(&self, client_id: String, payload: BridgeRequest) {
        self.enqueue(QueuedItem {
            request_id: self.mint_id(),
            execution: None,
            client_id,
            payload: Some(payload),
        })
        .await;
    }
}

fn hex_id(id: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(32);
    for byte in id {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 0x0f) as usize] as char);
    }
    text
}

#[expect(
    clippy::result_large_err,
    reason = "this parser feeds the allocation-free HostAdapter error path"
)]
fn parse_hex_lease(text: &str) -> Result<[u8; 16], AdapterError> {
    if text.len() != 32 || !text.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(invalid_request("capture lease is not a 128-bit hex ID"));
    }
    let mut id = [0u8; 16];
    for (index, chunk) in text.as_bytes().chunks(2).enumerate() {
        let chunk = std::str::from_utf8(chunk).map_err(|_| invalid_request("bad lease hex"))?;
        id[index] = u8::from_str_radix(chunk, 16).map_err(|_| invalid_request("bad lease hex"))?;
    }
    Ok(id)
}

fn transport_error(error: PipeTransportError) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Unavailable, error.to_string())
}

fn invalid_request(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidRequest, message)
}

/// Parses a Zellij pane ID string using the exact pinned text format
/// (`terminal_<u32>`, `plugin_<u32>`, or a bare `<u32>` meaning terminal), from
/// `zellij-utils/src/data.rs` (`FromStr for PaneId`).
#[expect(
    clippy::result_large_err,
    reason = "this parser feeds the allocation-free HostAdapter error path"
)]
fn parse_pane_id(text: &str) -> Result<muxe_zellij_protocol::generated::raw::PaneId, AdapterError> {
    use muxe_zellij_protocol::generated::raw::PaneId;
    if let Some(number) = text.strip_prefix("terminal_") {
        return number
            .parse::<u32>()
            .map(PaneId::Terminal)
            .map_err(|_| invalid_request(format!("invalid Zellij pane ID '{text}'")));
    }
    if let Some(number) = text.strip_prefix("plugin_") {
        return number
            .parse::<u32>()
            .map(PaneId::Plugin)
            .map_err(|_| invalid_request(format!("invalid Zellij pane ID '{text}'")));
    }
    text.parse::<u32>()
        .map(PaneId::Terminal)
        .map_err(|_| invalid_request(format!("invalid Zellij pane ID '{text}'")))
}

impl ActionValidator for ZellijAdapter {
    fn validate_portable(
        &self,
        action: &PortableAction,
        action_span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        self.inner.validator.validate_portable(action, action_span)
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        self.inner.validator.validate_native_batch(candidates)
    }
}

#[async_trait]
impl HostAdapter for ZellijAdapter {
    async fn identity(&self) -> Result<HostIdentity, AdapterError> {
        Ok(self.host_identity())
    }

    async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
        Ok(AdapterCapabilities {
            keyboard: KeyboardCapabilities {
                kitty_baseline: true,
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
        // A throwaway session namespace keeps the claim fan-out keyed without
        // allocating a broker session.
        let probe_session = format!("claim-{}", hex_id(&self.mint_id()));
        let client = self
            .resolve_client_for_pane(&probe_session, ui_pane.as_str())
            .await?;
        Ok(Self::scope_for_client(&client))
    }

    async fn begin_capture(
        &self,
        request: CaptureRequest,
    ) -> Result<ApiCaptureLease, AdapterError> {
        let client_id = Self::client_for_scope(&request.modal_scope)?;
        // Guard: a live compatible bridge must own the client before capture.
        let _registration = self.active_registration(&client_id).await?;
        let lease = self.mint_id();
        {
            let mut captures = self.inner.captures.lock().await;
            captures
                .begin(&client_id, request.ui_session.as_str(), lease)
                .map_err(|error| {
                    AdapterError::new(AdapterErrorKind::Unavailable, error.to_string())
                })?;
        }
        let (sender, receiver) = oneshot::channel();
        self.inner
            .pending_capture
            .lock()
            .await
            .insert(lease, sender);
        self.enqueue_lifecycle(
            client_id.clone(),
            BridgeRequest::BeginCapture {
                lease,
                ui_session: request.ui_session.as_str().to_owned(),
            },
        )
        .await;
        match timeout(CAPTURE_TIMEOUT, receiver).await {
            Ok(Ok(Ok(prior_mode))) => {
                self.inner
                    .captures
                    .lock()
                    .await
                    .confirm(&client_id, lease, prior_mode)
                    .map_err(|error| {
                        AdapterError::new(AdapterErrorKind::Unavailable, error.to_string())
                    })?;
                Ok(ApiCaptureLease {
                    id: CaptureLeaseId::new(hex_id(&lease)),
                    ui_session: request.ui_session,
                    modal_scope: request.modal_scope,
                })
            }
            _ => {
                self.inner.pending_capture.lock().await.remove(&lease);
                let _ = self
                    .inner
                    .captures
                    .lock()
                    .await
                    .release(&client_id, lease, false);
                Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "timed out waiting for Zellij Locked-mode capture",
                ))
            }
        }
    }

    async fn end_capture(
        &self,
        lease: ApiCaptureLease,
        reason: CaptureReleaseReason,
    ) -> Result<(), AdapterError> {
        let client_id = Self::client_for_scope(&lease.modal_scope)?;
        let lease_id = parse_hex_lease(lease.id.as_str())?;
        // Guarded restoration decision first: only a current lease owner still
        // in Locked mode restores; the bridge applies the same guard.
        {
            let mut captures = self.inner.captures.lock().await;
            match captures.release(
                &client_id,
                lease_id,
                matches!(
                    reason,
                    CaptureReleaseReason::UiDismissed | CaptureReleaseReason::Replaced
                ),
            ) {
                Ok(_) | Err(crate::capture::CaptureError::StaleLease)
                | Err(crate::capture::CaptureError::NotCaptured)
                // `Busy` only comes from `begin`; listed for exhaustiveness.
                | Err(crate::capture::CaptureError::Busy) => {}
            }
        }
        if self.active_registration(&client_id).await.is_ok() {
            let end_reason = match reason {
                CaptureReleaseReason::UiDismissed => CaptureEndReason::UiDismissed,
                CaptureReleaseReason::Replaced => CaptureEndReason::Replaced,
                CaptureReleaseReason::LeaseExpired => CaptureEndReason::LeaseExpired,
                CaptureReleaseReason::UserModeChanged => CaptureEndReason::UserModeChanged,
                CaptureReleaseReason::AdapterShutdown => CaptureEndReason::AdapterShutdown,
            };
            self.enqueue_lifecycle(
                client_id,
                BridgeRequest::EndCapture {
                    lease: lease_id,
                    reason: end_reason,
                },
            )
            .await;
        }
        Ok(())
    }

    async fn close_pending_pane(
        &self,
        registration: PendingPaneRegistration,
    ) -> Result<(), AdapterError> {
        // Zellij launchers create UI panes directly through host Run bindings;
        // there is no transient-tab trampoline. Idempotent cleanup closes only
        // the strictly validated registered pane, and only when a live bridge
        // owns its client; an unknown pane is already gone.
        if registration.temporary_tab.is_some() {
            return Err(invalid_request(
                "Zellij has no transient-tab launch; refusing to close a temporary tab",
            ));
        }
        let pane = parse_pane_id(registration.pane.as_str())?;
        let owner = self
            .inner
            .registry
            .lock()
            .await
            .client_for_pane(registration.pane.as_str())
            .map(|record| record.client_id.clone());
        if let Some(client) = owner {
            self.enqueue_lifecycle(
                client,
                BridgeRequest::Dispatch {
                    execution: format!("cleanup-{}", hex_id(&self.mint_id())),
                    command: RawNativeCommand::ClosePaneWithId { pane_id: pane },
                },
            )
            .await;
        }
        Ok(())
    }

    async fn capture_origin(
        &self,
        request: OriginCaptureRequest,
    ) -> Result<OriginContext, AdapterError> {
        let hint = request.origin_hint.as_ref().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Zellij AttachUi did not provide the saved origin bootstrap tuple",
            )
        })?;
        let caller = request.caller_identity.as_ref().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Zellij AttachUi did not provide the UI caller identity tuple",
            )
        })?;
        if caller.pane_id != request.ui_pane {
            return Err(AdapterError::new(
                AdapterErrorKind::InvalidRequest,
                "Zellij caller pane does not match the pane that is attaching the UI",
            ));
        }
        let ui_pane = request.ui_pane.as_str().to_owned();
        // The claim fan-out in modal_scope usually cached the snapshot already;
        // reuse it so attach costs no second host round-trip.
        let snapshot =
            if let Some(snapshot) = self.inner.snapshots.lock().await.get(&ui_pane).cloned() {
                snapshot
            } else {
                let ui_session = format!("origin-{}", hex_id(&self.mint_id()));
                self.resolve_client_for_pane(&ui_session, &ui_pane).await?;
                self.inner
                    .snapshots
                    .lock()
                    .await
                    .get(&ui_pane)
                    .cloned()
                    .ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::Unavailable,
                            "origin snapshot missing after claim",
                        )
                    })?
            };
        if snapshot.ui_pane_id != ui_pane {
            return Err(AdapterError::new(
                AdapterErrorKind::InvalidRequest,
                "origin snapshot is for a different UI pane",
            ));
        }
        match &snapshot.prior_pane_id {
            Some(prior) if prior == hint.pane_id.as_str() => {}
            Some(_) => {
                return Err(AdapterError::new(
                    AdapterErrorKind::InvalidRequest,
                    "saved origin pane does not match the live bridge prior pane",
                ));
            }
            None => {
                return Err(AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "bridge tracked no prior pane for origin",
                ));
            }
        }
        build_origin_context(
            &snapshot,
            &self.inner.config.session_name,
            &ui_pane,
            None,
            None,
            None,
        )
        .map_err(|error| AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string()))
    }

    async fn dispatch_portable(
        &self,
        request: PortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        match map_portable(&request.action.action, &request.origin) {
            Ok(PortableMapping::BrokerOwned) => Err(invalid_request(
                "broker-owned portable action must not reach the host adapter",
            )),
            Ok(PortableMapping::HostAction { commands }) => {
                self.dispatch_commands(request.execution, &request.origin, commands)
                    .await
            }
            Ok(PortableMapping::BridgeFocus { request: focus }) => {
                let client_id = request
                    .origin
                    .client_id
                    .as_ref()
                    .map(|id| id.as_str().to_owned())
                    .ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::ContextUnavailable,
                            "Zellij focus requires the captured origin client",
                        )
                    })?;
                self.active_registration(&client_id).await?;
                let payload = match focus {
                    crate::FocusRequest::ByIndex { index } => BridgeRequest::FocusPaneByIndex {
                        execution: request.execution.0.to_string(),
                        index,
                    },
                    crate::FocusRequest::Neighbor { direction } => {
                        BridgeRequest::FocusPaneNeighbor {
                            execution: request.execution.0.to_string(),
                            direction: direction.into_neighbor(),
                        }
                    }
                };
                self.inner
                    .live_executions
                    .lock()
                    .await
                    .insert(request.execution.0);
                self.enqueue(QueuedItem {
                    request_id: self.mint_id(),
                    execution: Some(request.execution),
                    client_id,
                    payload: Some(payload),
                })
                .await;
                Ok(DispatchAccepted {
                    correlation: self.correlation(),
                    execution: request.execution,
                    capabilities: ExecutionCapabilities::ASYNCHRONOUS,
                })
            }
            Err(PortableError::Incompatible { reason, .. }) => Err(AdapterError::new(
                AdapterErrorKind::Incompatible,
                reason.to_owned(),
            )),
            Err(error) => Err(invalid_request(error.to_string())),
        }
    }

    async fn dispatch_native(
        &self,
        request: NativeDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        let candidate = &request.action.candidate;
        let raw =
            candidate_to_raw(&candidate.type_name, &candidate.fields, false).map_err(|error| {
                AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string())
            })?;
        self.dispatch_commands(request.execution, &request.origin, vec![raw])
            .await
    }

    async fn cancel(&self, _execution: ExecutionId) -> Result<(), AdapterError> {
        Err(AdapterError::new(
            AdapterErrorKind::CancelUnsupported,
            "Zellij native actions cannot be cancelled once dispatched",
        ))
    }

    async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
        self.inner
            .events_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| AdapterError::new(AdapterErrorKind::Shutdown, "adapter shut down"))
    }

    async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
        Err(AdapterError::new(
            AdapterErrorKind::Unsupported,
            "Zellij cannot suspend for activation: bridge-sharing group activation needs whole-group coordinator support",
        ))
    }

    async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
        Err(AdapterError::new(
            AdapterErrorKind::Unsupported,
            "Zellij cannot resume after an activation abort: no suspendible subscription exists",
        ))
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        self.inner.shutdown.store(true, Ordering::Relaxed);
        self.inner.request.close().await;
        self.inner.event.close().await;
        Ok(())
    }
}

impl CaptureTable {
    /// Drops every client's capture state on whole-pipe replacement.
    fn invalidate_all_clients(&mut self) {
        *self = Self::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipes::testing::ScriptedChannel;
    use muxe_zellij_protocol::{
        BridgeArtifact, BridgeIdentity, CommandOutcome, PipeEvent, PipeEventKind,
        decode_request_line, encode_event_line,
    };

    fn test_origin() -> OriginContext {
        OriginContext {
            host_kind: muxe_core::OriginHostKind::Zellij,
            server_id: muxe_core::ServerId::new("session-alpha"),
            client_id: Some(muxe_core::ClientId::new("client-1")),
            session_id: Some(muxe_core::SessionId::new("session-alpha")),
            workspace_id: None,
            tab_id: None,
            tab_index: None,
            pane_id: Some(muxe_core::PaneId::new("terminal_2")),
            pane_type: None,
            pane_cwd: None,
            selection_text: None,
            invocation_source: muxe_core::OriginInvocationSource::RootBinding,
            worktree_id: None,
            worktree_path: None,
            agent_id: None,
            link_url: None,
            link_handler_id: None,
        }
    }

    fn candidate() -> NativeActionCandidate {
        NativeActionCandidate {
            type_name: "native.zellij.command:close-focus".to_owned(),
            type_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("<test>"), 0, 1),
            fields: Vec::new(),
        }
    }

    fn register_event(registration: [u8; 16], artifact: BridgeArtifact) -> PipeEvent {
        PipeEvent {
            sequence: 1,
            event: PipeEventKind::Register {
                client_id: "client-1".to_owned(),
                current_pane: Some("terminal_2".to_owned()),
                registration,
                plugin_id: Some(3),
                identity: BridgeIdentity {
                    muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
                    source_revision: pinned_source_revision().to_owned(),
                    action_fingerprint: generated_action_fingerprint().0,
                    protocol_fingerprint: bridge_protocol_fingerprint().0,
                    artifact,
                },
            },
        }
    }

    fn test_adapter(request: &Arc<ScriptedChannel>, event: &Arc<ScriptedChannel>) -> ZellijAdapter {
        ZellijAdapter::new(
            ZellijAdapterConfig {
                session_name: "session-alpha".to_owned(),
                zellij_exe: PathBuf::from("/nonexistent/zellij"),
            },
            Arc::clone(request) as Arc<dyn PipeChannel>,
            Arc::clone(event) as Arc<dyn PipeChannel>,
        )
    }

    async fn poll_outbound(channel: &ScriptedChannel) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let outbound = channel.take_outbound();
            if let Some(line) = outbound.into_iter().next() {
                return line;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out waiting for outbound request line");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn next_event(adapter: &ZellijAdapter) -> AdapterHealthEvent {
        tokio::time::timeout(Duration::from_secs(2), adapter.next_health_event())
            .await
            .expect("health arrives")
            .expect("event ok")
    }

    /// Recorded Register → request → RequestReleased → DispatchCompleted flow:
    /// the exact script the common adapter-contract suite replays.
    #[tokio::test]
    async fn recorded_dispatch_flow_targets_active_registration() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);

        event.push_line(
            encode_event_line(&register_event([7; 16], BridgeArtifact::Unattested))
                .expect("register encodes"),
        );
        // Wait until the registration lands: a dispatch to an unknown client
        // fails, so success below proves the record is active.
        let accepted = loop {
            let result = adapter
                .dispatch_native(NativeDispatchRequest {
                    execution: ExecutionId(7),
                    action: muxe_adapter_api::ResolvedNativeAction {
                        candidate: candidate(),
                    },
                    origin: test_origin(),
                })
                .await;
            if let Ok(accepted) = result {
                break accepted;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(accepted.execution, ExecutionId(7));

        let line = poll_outbound(&request).await;
        let frame = decode_request_line(&line).expect("typed request frame");
        assert_eq!(frame.target.client_id, "client-1");
        assert_eq!(frame.target.registration, [7; 16]);
        let request_id = frame.request_id;
        assert!(matches!(frame.payload, BridgeRequest::Dispatch { .. }));

        // Transport release unblocks the pipe; completion follows on events.
        event.push_line(
            encode_event_line(&PipeEvent {
                sequence: 2,
                event: PipeEventKind::RequestReleased {
                    request_id,
                    channel_generation: 1,
                    registration: [7; 16],
                },
            })
            .expect("release encodes"),
        );
        event.push_line(
            encode_event_line(&PipeEvent {
                sequence: 3,
                event: PipeEventKind::DispatchCompleted {
                    request_id,
                    execution: "7".to_owned(),
                    outcome: CommandOutcome::succeeded(),
                },
            })
            .expect("completion encodes"),
        );
        // The first health event is the Healthy registration report; the
        // completion follows it.
        let completion = match next_event(&adapter).await {
            AdapterHealthEvent::Healthy { .. } => next_event(&adapter).await,
            other => other,
        };
        match completion {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded {
                execution: ExecutionId(7),
            }) => {}
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded { execution }) => {
                panic!("wrong execution: {}", execution.0)
            }
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Failed {
                execution,
                error,
            }) => panic!("failed {}: {error}", execution.0),
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution,
                error,
            }) => panic!("unknown {}: {error}", execution.0),
            AdapterHealthEvent::Healthy { .. } => panic!("second healthy"),
            AdapterHealthEvent::Unhealthy { error, .. } => panic!("unhealthy: {error}"),
            AdapterHealthEvent::Reconnected { .. } => panic!("reconnected"),
            AdapterHealthEvent::CaptureReady { .. } => panic!("capture ready"),
            AdapterHealthEvent::CaptureLost { .. } => panic!("capture lost"),
        }
    }

    /// A self-attested NativeVerified registration is contained: the bridge is
    /// recorded but flagged incompatible, so no dispatch line reaches the pipe.
    #[tokio::test]
    async fn self_attested_artifact_is_incompatible() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        event.push_line(
            encode_event_line(&register_event(
                [9; 16],
                BridgeArtifact::NativeVerified { sha256: [1; 32] },
            ))
            .expect("encodes"),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let result = adapter
                .dispatch_native(NativeDispatchRequest {
                    execution: ExecutionId(8),
                    action: muxe_adapter_api::ResolvedNativeAction {
                        candidate: candidate(),
                    },
                    origin: test_origin(),
                })
                .await;
            if let Err(error) = &result
                && error.to_string().contains("incompatible")
            {
                break;
            }
            // Before the registration lands the error is Unavailable; either
            // way no dispatch line may reach the pipe.
            if tokio::time::Instant::now() >= deadline {
                panic!("attested registration was not contained: {result:?}");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(request.take_outbound().is_empty());
        adapter.shutdown().await.expect("shutdown");
    }

    /// Launch dispatch targets the origin client with the exact command
    /// vector: a menu split carries program, argv, cwd, direction, and focus.
    #[tokio::test]
    async fn launch_pane_targets_origin_client() {
        use crate::launch::{
            LaunchKind, LaunchTarget, ZellijPaneLaunch, ZellijPlacement, ZellijSplitDirection,
        };
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        event.push_line(
            encode_event_line(&register_event([7; 16], BridgeArtifact::Unattested))
                .expect("register encodes"),
        );
        let accepted = loop {
            let result = adapter
                .launch_pane(
                    ExecutionId(21),
                    ZellijPaneLaunch {
                        kind: LaunchKind::Menu,
                        target: LaunchTarget::Client("client-1".to_owned()),
                        cwd: Some(std::path::PathBuf::from("/work")),
                        program: std::path::PathBuf::from("muxe"),
                        args: vec!["ui".to_owned(), "menu".to_owned(), "main".to_owned()],
                        placement: ZellijPlacement::Split {
                            direction: ZellijSplitDirection::Down,
                        },
                        focus: true,
                    },
                )
                .await;
            if let Ok(accepted) = result {
                break accepted;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(accepted.execution, ExecutionId(21));
        let line = poll_outbound(&request).await;
        let frame = decode_request_line(&line).expect("typed request frame");
        assert_eq!(frame.target.client_id, "client-1");
        match frame.payload {
            BridgeRequest::Dispatch { execution, command } => {
                assert_eq!(execution, "21");
                let RawNativeCommand::RunAction { action, .. } = command else {
                    panic!("expected run-action wrap");
                };
                assert!(matches!(
                    action,
                    muxe_zellij_protocol::generated::raw::Action::NewTiledPane { .. }
                ));
            }
            _ => panic!("expected dispatch payload"),
        }
        adapter.shutdown().await.expect("shutdown");
    }
    #[tokio::test]
    async fn activation_suspend_and_resume_are_explicitly_unsupported() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let suspend = adapter
            .suspend_for_activation()
            .await
            .expect_err("suspend fails");
        assert_eq!(suspend.kind, AdapterErrorKind::Unsupported);
        let resume = adapter
            .resume_after_activation_abort()
            .await
            .expect_err("resume fails");
        assert_eq!(resume.kind, AdapterErrorKind::Unsupported);
        adapter.shutdown().await.expect("shutdown");
    }
    fn origin_request(
        ui_pane: &str,
        hint_pane: Option<&str>,
        caller_pane: Option<&str>,
    ) -> OriginCaptureRequest {
        use muxe_adapter_api::{HostCallerIdentity, OriginHintSource, UntrustedOriginHint};
        OriginCaptureRequest {
            ui_session: UiSessionId::new("session-1"),
            ui_pane: PaneId::new(ui_pane),
            origin_hint: hint_pane.map(|pane| UntrustedOriginHint {
                workspace_id: muxe_core::WorkspaceId::new("workspace-1"),
                tab_id: muxe_core::TabId::new("tab-1"),
                pane_id: PaneId::new(pane),
                cwd: Some(std::path::PathBuf::from("/work")),
                source: OriginHintSource::LauncherBootstrap,
            }),
            caller_identity: caller_pane.map(|pane| HostCallerIdentity {
                workspace_id: muxe_core::WorkspaceId::new("workspace-1"),
                tab_id: muxe_core::TabId::new("tab-1"),
                pane_id: PaneId::new(pane),
                cwd: Some(std::path::PathBuf::from("/work")),
            }),
        }
    }

    fn origin_snapshot(ui_pane: &str, prior: Option<&str>) -> muxe_zellij_protocol::ZellijOrigin {
        muxe_zellij_protocol::ZellijOrigin {
            client_id: "client-1".to_owned(),
            session_name: Some("session-alpha".to_owned()),
            prior_pane_id: prior.map(str::to_owned),
            ui_pane_id: ui_pane.to_owned(),
            prior_pane_cwd: Some("/work".to_owned()),
        }
    }

    #[tokio::test]
    async fn capture_origin_requires_hint_and_caller() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let missing_hint = adapter
            .capture_origin(origin_request("plugin-9", None, Some("plugin-9")))
            .await
            .expect_err("missing hint fails");
        assert_eq!(missing_hint.kind, AdapterErrorKind::ContextUnavailable);
        let missing_caller = adapter
            .capture_origin(origin_request("plugin-9", Some("terminal_2"), None))
            .await
            .expect_err("missing caller fails");
        assert_eq!(missing_caller.kind, AdapterErrorKind::ContextUnavailable);
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn capture_origin_validates_caller_and_prior_pane() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        adapter.inner.snapshots.lock().await.insert(
            "plugin-9".to_owned(),
            origin_snapshot("plugin-9", Some("terminal_2")),
        );
        let caller_mismatch = adapter
            .capture_origin(origin_request(
                "plugin-9",
                Some("terminal_2"),
                Some("terminal_7"),
            ))
            .await
            .expect_err("caller mismatch fails");
        assert_eq!(caller_mismatch.kind, AdapterErrorKind::InvalidRequest);
        let prior_mismatch = adapter
            .capture_origin(origin_request(
                "plugin-9",
                Some("terminal_9"),
                Some("plugin-9"),
            ))
            .await
            .expect_err("prior mismatch fails");
        assert_eq!(prior_mismatch.kind, AdapterErrorKind::InvalidRequest);
        let matched = adapter
            .capture_origin(origin_request(
                "plugin-9",
                Some("terminal_2"),
                Some("plugin-9"),
            ))
            .await
            .expect("matched hint and caller capture");
        assert_eq!(
            matched.pane_id.as_ref().map(|pane| pane.as_str()),
            Some("plugin-9")
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[test]
    fn zellij_exe_search_uses_owned_dir_entries() {
        let dir = std::env::temp_dir().join(format!("muxe-exe-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let exe = dir.join("zellij");
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").expect("fake exe");
        let found =
            super::find_zellij_in_dirs([dir.clone()].into_iter()).expect("finds owned fake");
        assert_eq!(found, exe);
        assert!(
            super::find_zellij_in_dirs(
                [std::env::temp_dir().join("muxe-exe-test-missing-dir")].into_iter()
            )
            .is_none()
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
