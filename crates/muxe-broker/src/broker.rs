use std::{
    collections::{HashMap, HashSet},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex, Weak,
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
    sync::{Mutex, Notify, mpsc, oneshot, watch},
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
    cleanup: Arc<CleanupSupervisor>,
    execution_transitions: Arc<Notify>,
    diagnostics_tx: mpsc::UnboundedSender<BrokerDiagnostic>,
    diagnostics_rx: Mutex<Option<mpsc::UnboundedReceiver<BrokerDiagnostic>>>,
    token_source: Mutex<OsTokenSource>,
    next_session: AtomicU64,
    next_execution: AtomicU64,
    next_event: Arc<AtomicU64>,
    /// Self reference for slow-consumer teardown. Delivery sites only hold
    /// `&self` (or detached supervisor handles), so a full queue upgrades this
    /// to spawn `detach` on its own task instead of awaiting client I/O.
    self_weak: Weak<Self>,
    #[cfg(test)]
    cleanup_enqueue_hook: StdMutex<Option<Arc<CleanupEnqueueHook>>>,
    #[cfg(test)]
    commit_ui_launch_hook: StdMutex<Option<Arc<WaitHook>>>,
}

#[cfg(test)]
#[derive(Default)]
pub struct WaitHook {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

#[cfg(test)]
impl WaitHook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}
#[cfg(test)]
pub(crate) struct CleanupEnqueueHook {
    entered: Notify,
    release: Notify,
}

#[cfg(test)]
impl CleanupEnqueueHook {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}

/// A payload-safe terminal diagnostic for externally dispatched work that has
/// outlived the UI session that initiated it. The composition root must use its
/// typed outcome and code to select broker-authored log text; host error payloads
/// never cross this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BrokerDiagnostic {
    pub execution: ExecutionId,
    pub outcome: ExecutionOutcome,
    pub code: DiagnosticCode,
}

#[derive(Default)]
struct BrokerState {
    gate: LaunchGate,
    registering: HashSet<PendingLaunchToken>,
    pending_sessions: HashMap<PendingLaunchToken, UiSessionId>,
    /// Committed gated ownership remains retained until the service confirms
    /// the successful `UiAttached` response publication. This keeps the exact
    /// registered pane available if disconnect wins after readiness but before
    /// publication confirmation.
    gated_sessions: HashMap<PendingLaunchToken, GatedSession>,
    /// Broker-owned cleanup backlog: every retained `RegisteredPane` or
    /// acquired `CaptureLease` whose host close/end has not yet succeeded.
    /// Entries are claimed under the state lock, called outside all locks,
    /// and removed only on confirmed success, so a cancelled disconnect
    /// future can never lose retry provenance.
    cleanup: CleanupRegistry,
    /// The one authoritative registry for accepted external work. It is keyed by
    /// the broker's typed execution identity, never by a UI session: a session is
    /// only an optional notification/capture attachment to work the broker owns.
    executions: HashMap<CoreExecutionId, ExecutionRecord>,
    /// An awaited UI can own exactly one pending execution. This is an admission
    /// index, not the execution registry.
    awaiting: HashMap<UiSessionId, CoreExecutionId>,
    // Set by `drain_for_activation` before anything is torn down and cleared only when
    // the broker returns to Running. While set, no new launch or execution is admitted.
    activation_sealed: bool,
}
#[derive(Clone)]
struct GatedSession {
    session: UiSessionId,
    registration: Option<RegisteredPane>,
}
/// Broker-owned backlog of host cleanups whose adapter call has not yet
/// succeeded. Entries live in `BrokerState` under the state lock; the retry
/// path claims due `Ready` entries to `InFlight`, clones the payload, drops
/// every lock, calls the adapter, then removes on success or records the
/// failure and backoff. Provenance therefore survives a cancelled caller.
#[derive(Default)]
struct CleanupRegistry {
    pending_panes: HashMap<muxe_adapter_api::PendingPaneLeaseId, PendingPaneCleanup>,
    captures: HashMap<muxe_adapter_api::CaptureLeaseId, CaptureCleanup>,
}

/// One retained pending-pane close. `registration` keeps the full typed
/// provenance (including the lease) until `close_pending_pane` succeeds.
struct PendingPaneCleanup {
    session: UiSessionId,
    registration: RegisteredPane,
    phase: CleanupPhase,
    attempt: u32,
    next_retry: Instant,
    primary: Option<String>,
    last_cleanup_error: Option<AdapterError>,
}

/// One retained capture end. `lease` keeps the acquired lease until
/// `end_capture` succeeds.
struct CaptureCleanup {
    lease: CaptureLease,
    reason: CaptureReleaseReason,
    phase: CleanupPhase,
    attempt: u32,
    next_retry: Instant,
    primary: Option<String>,
    last_cleanup_error: Option<AdapterError>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupPhase {
    Ready,
    InFlight,
}
/// Cancellation-safe host-cleanup supervisor. Request paths enqueue retained
/// payloads into `BrokerState::cleanup` and wake an existing task, or spawn one
/// real task for the typed lease identity. The task owns every adapter await,
/// so aborting the requesting future cannot strand an `InFlight` entry.
///
/// Liveness invariant: an entry present in `BrokerState::cleanup` always has a
/// live task owning its key. The task's terminal step re-checks for an entry
/// while holding the broker state lock (`release_slot_unless_requeued`) and
/// only removes its slot when no entry exists; the enqueue insert holds the
/// same lock, so exit and re-enqueue are mutually exclusive.
struct CleanupSupervisor {
    tasks: StdMutex<HashMap<CleanupTaskKey, CleanupTask>>,
    /// Serializes the check/spawn/insert sequence. A spawned task waits for its
    /// handle to be installed under the typed key before it can finish and remove it.
    launch: StdMutex<()>,
    next_claim: AtomicU64,
    wake: Notify,
}

struct CleanupTask {
    claim: u64,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum CleanupTaskKey {
    PendingPane(muxe_adapter_api::PendingPaneLeaseId),
    Capture(muxe_adapter_api::CaptureLeaseId),
}

impl Default for CleanupSupervisor {
    fn default() -> Self {
        Self {
            tasks: StdMutex::new(HashMap::new()),
            launch: StdMutex::new(()),
            next_claim: AtomicU64::new(1),
            wake: Notify::new(),
        }
    }
}

impl CleanupSupervisor {
    fn next_claim(&self) -> u64 {
        self.next_claim.fetch_add(1, Ordering::Relaxed)
    }

    fn has_live_task(&self, key: &CleanupTaskKey) -> bool {
        self.tasks
            .lock()
            .expect("cleanup tasks are not poisoned")
            .get(key)
            .is_some_and(|task| !task.handle.is_finished())
    }

    fn remove_if_claim(&self, key: &CleanupTaskKey, claim: u64) {
        let mut tasks = self.tasks.lock().expect("cleanup tasks are not poisoned");
        if tasks.get(key).is_some_and(|task| task.claim == claim) {
            tasks.remove(key);
        }
    }

    /// Atomically decides the task's terminal step under the broker state
    /// lock: while holding the state guard, checks whether a cleanup entry
    /// still exists for `key`. When one exists (a re-enqueue landed before
    /// this check) the slot is kept and the caller must loop to process it.
    /// Only when no entry exists is this task's slot removed and the caller
    /// allowed to exit. The enqueue insert holds the same state lock, so the
    /// exit check and any re-enqueue insert are mutually exclusive: an entry
    /// present in `state.cleanup` always has a live task owning its key.
    /// Takes only the synchronous tasks std mutex inside the guard; no
    /// `.await` runs while either lock is held.
    async fn release_slot_unless_requeued(
        &self,
        state: &Arc<Mutex<BrokerState>>,
        key: &CleanupTaskKey,
        claim: u64,
    ) -> bool {
        // Test gate: pauses the task exactly between entry removal and slot
        // removal, i.e. inside the B2 race window. Every terminal arm routes
        // through this handshake, so arming it for a key forces the
        // interleaving deterministically.
        #[cfg(test)]
        crate::cleanup_task_hooks::exit_gate(key).await;
        let guard = state.lock().await;
        let requeued = match key {
            CleanupTaskKey::PendingPane(lease) => guard.cleanup.pending_panes.contains_key(lease),
            CleanupTaskKey::Capture(lease) => guard.cleanup.captures.contains_key(lease),
        };
        if requeued {
            return false;
        }
        let mut tasks = self.tasks.lock().expect("cleanup tasks are not poisoned");
        if tasks.get(key).is_some_and(|task| task.claim == claim) {
            tasks.remove(key);
        }
        true
    }

    #[cfg(test)]
    fn task_count(&self) -> usize {
        self.tasks
            .lock()
            .expect("cleanup tasks are not poisoned")
            .len()
    }

    fn notify(&self) {
        self.wake.notify_waiters();
    }
}

/// Deterministic retry delays: 0ms, 25ms, 100ms, 500ms, then 1s capped.
fn cleanup_retry_delay(attempt: u32) -> Duration {
    match attempt {
        0 => Duration::from_millis(0),
        1 => Duration::from_millis(25),
        2 => Duration::from_millis(100),
        3 => Duration::from_millis(500),
        _ => Duration::from_secs(1),
    }
}
/// Bounded deadline for `drain_for_activation` to observe confirmation of the
/// broker-owned host cleanup it initiated. Cleanup retries back off to 1s, so
/// the deadline spans several attempts without stalling activation forever.
///
/// Determinism: the wait uses `tokio::time::Instant` (not `std::time::Instant`)
/// consistently with `tokio::time::sleep`, so `#[tokio::test(start_paused =
/// true)]` advances the same clock the deadline reads and the wait completes
/// without wall-clock sleeps. The `#[cfg(test)]` deadline is shortened so the
/// failure-path regression completes in milliseconds; wakeups still drive the
/// fast path and this tick only bounds check staleness.
#[cfg(not(test))]
const DRAIN_CLEANUP_DEADLINE: Duration = Duration::from_secs(5);
#[cfg(test)]
const DRAIN_CLEANUP_DEADLINE: Duration = Duration::from_millis(200);
/// Re-check cadence while waiting for cleanup confirmation: wakeups drive the
/// fast path, this tick bounds the staleness of the empty check.
const DRAIN_CLEANUP_POLL: Duration = Duration::from_millis(10);

fn wire_menu_id_to_core(value: &muxe_protocol::MenuId) -> Option<muxe_core::MenuId> {
    match value {
        muxe_protocol::MenuId::Named(name) => muxe_core::MenuName::parse(name.as_str())
            .ok()
            .map(muxe_core::MenuId::named),
        muxe_protocol::MenuId::Inline { parent, ordinal } => {
            muxe_core::MenuName::parse(parent.as_str())
                .ok()
                .map(|owner| {
                    muxe_core::MenuId::inline(muxe_core::InlineMenuId::new(owner, *ordinal))
                })
        }
    }
}

fn core_menu_id_to_wire(value: &muxe_core::MenuId) -> muxe_protocol::MenuId {
    match value {
        muxe_core::MenuId::Named(name) => muxe_protocol::MenuId::named(name.as_str()),
        muxe_core::MenuId::Inline(id) => {
            muxe_protocol::MenuId::inline(id.owner().as_str(), id.ordinal())
        }
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExecutionPhase {
    /// The broker admitted this typed identity before it can reach an adapter or
    /// a process spawn. Only a proven pre-dispatch failure may remove it.
    Reserved,
    Adapter,
    Generic,
    Terminal,
}

#[derive(Clone)]
struct ExecutionRecord {
    wire: ExecutionId,
    core: CoreExecutionId,
    /// The UI that can receive a completion. Detached work deliberately clears
    /// this without surrendering broker ownership.
    session: Option<UiSessionId>,
    awaiting: bool,
    cancellable: bool,
    on_menu_control: muxe_core::MenuControlAction,
    timeout: Option<Duration>,
    on_timeout: TimeoutAction,
    owner: ExecutionOwner,
    phase: ExecutionPhase,
    /// Focus-sensitive host work remains attached to this exact owner while the
    /// UI is dismissed; no session-keyed registry owns the payload.
    deferred: Option<PostDismissalPortableDispatchRequest>,
    pending_control: Option<MenuControl>,
    termination_requested: bool,
    deadline_scheduled: bool,
}

#[derive(Default)]
struct GenericSupervisor {
    /// Installed synchronously before its reaper task is spawned. This lets a
    /// concurrent seal signal an owned child without an await-sized gap.
    processes: StdMutex<HashMap<CoreExecutionId, GenericProcess>>,
}

struct GenericProcess {
    cancellation: watch::Sender<Option<GenericCancellation>>,
}
#[derive(Clone, Copy)]
enum GenericCancellation {
    UserRequested,
    Timeout,
}

struct ExecutionDeadline {
    core: CoreExecutionId,
    timeout: Duration,
    on_timeout: TimeoutAction,
    state: Arc<Mutex<BrokerState>>,
    sessions: Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    adapter: Arc<dyn HostAdapter>,
    generic: Arc<GenericSupervisor>,
    next_event: Arc<AtomicU64>,
    /// Lets terminal delivery spawn slow-consumer teardown without an `Arc`
    /// at the call site. See `deliver_session_event_to`.
    broker: Weak<Broker>,
}

/// Sends one broker event to a session without ever waiting on client I/O.
///
/// The per-connection outbox is bounded (see the `mpsc::channel(32)` in
/// `service.rs`), and its writer task stops consuming while the socket blocks.
/// Awaiting `send()` here would therefore let one stalled UI park the shared
/// adapter monitor (or a generic reaper) and wedge completions and health for
/// every unrelated session. `try_send` keeps delivery non-blocking and
/// ordered: a healthy session's events still arrive in order and once each,
/// while a session that is being torn down may have its tail dropped.
async fn deliver_session_event_to(
    sessions: &Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    session: &UiSessionId,
    event_id: EventId,
    event: BrokerEvent,
    kind: &'static str,
    broker: &Weak<Broker>,
) {
    let events = sessions
        .lock()
        .await
        .get(session)
        .map(|record| record.events.clone());
    let Some(events) = events else {
        return;
    };
    note_send_outcome(
        &events.try_send(WireMessage::Event { event_id, event }),
        broker,
        session,
        kind,
    );
}

/// Records the outcome of one non-blocking session delivery. `Full` is an
/// explicit slow-consumer failure, not backpressure: the session is torn down
/// so its queue, capture, and connection are released instead of wedging
/// shared state. `Closed` means the connection is already gone, so there is
/// nothing to do.
fn note_send_outcome(
    result: &Result<(), tokio::sync::mpsc::error::TrySendError<WireMessage>>,
    broker: &Weak<Broker>,
    session: &UiSessionId,
    kind: &'static str,
) {
    match result {
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            tracing::error!(
                session = ?session,
                kind,
                "slow UI filled its bounded event queue; detaching the session",
            );
            spawn_slow_consumer_teardown(broker, session.clone());
        }
        Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
    }
}

/// Tears a slow consumer down off the delivery path. `detach` awaits adapter
/// cleanup, so it must never run inline in the shared monitor or a reaper;
/// teardown runs on its own task instead. Detachment is effectively once per
/// session: `detach` removes the session record under the sessions lock, so a
/// racing second teardown finds no record and enqueues no duplicate capture
/// cleanup (and lease ids are never reused, so a stale teardown cannot catch
/// a newer session).
fn spawn_slow_consumer_teardown(broker: &Weak<Broker>, session: UiSessionId) {
    let Some(broker) = broker.upgrade() else {
        return;
    };
    tokio::spawn(async move {
        // Candidates for the release reason: `UiDismissed` (the UI is going
        // away and its capture must be released), `LeaseExpired`,
        // `AdapterShutdown`, `Replaced`, `UserModeChanged`. Rejected:
        // `LeaseExpired` (the lease is still valid; the consumer is slow),
        // `AdapterShutdown`/`Replaced`/`UserModeChanged` (no host transition
        // happened). `UiDismissed` is the closest existing variant: from the
        // host's perspective the UI is being disconnected.
        let _ = broker
            .detach(&session, CaptureReleaseReason::UiDismissed)
            .await;
    });
}

async fn supervise_execution_deadline(deadline: ExecutionDeadline) {
    tokio::time::sleep(deadline.timeout).await;
    let timeout = {
        let mut state = deadline.state.lock().await;
        let (owner, session, wire, awaiting, outcome) = {
            let Some(record) = state.executions.get_mut(&deadline.core) else {
                return;
            };
            if matches!(
                record.phase,
                ExecutionPhase::Reserved | ExecutionPhase::Terminal
            ) || record.termination_requested
            {
                return;
            }
            let session = record.session.take();
            let awaiting = std::mem::replace(&mut record.awaiting, false);
            let wire = record.wire;
            match deadline.on_timeout {
                TimeoutAction::Detach => {
                    (None, session, wire, awaiting, ExecutionOutcome::Detached)
                }
                TimeoutAction::Cancel => {
                    record.termination_requested = true;
                    (
                        Some(record.owner),
                        session,
                        wire,
                        awaiting,
                        ExecutionOutcome::TimedOut,
                    )
                }
            }
        };
        if awaiting && let Some(session) = &session {
            state.awaiting.remove(session);
        }
        Some((owner, session, wire, outcome))
    };
    let Some((owner, session, wire, outcome)) = timeout else {
        return;
    };
    let event_delivery = async {
        if let Some(session) = &session {
            deliver_session_event_to(
                &deadline.sessions,
                session,
                new_event_id(&deadline.next_event),
                BrokerEvent::ExecutionCompleted {
                    session: session.clone(),
                    execution: wire,
                    outcome,
                    diagnostic: Some(diagnostic(
                        DiagnosticCode::ActionBlocked,
                        "execution exceeded its configured timeout",
                    )),
                },
                "ExecutionCompleted",
                &deadline.broker,
            )
            .await;
        }
    };
    // A full UI queue must never delay the owner-side stop. Delivery itself is
    // non-blocking now; the join keeps the existing shape while adapter
    // cancellation crosses its host boundary.
    tokio::join!(event_delivery, cancel_deadline_owner(&deadline, owner),);
}

async fn cancel_deadline_owner(deadline: &ExecutionDeadline, owner: Option<ExecutionOwner>) {
    if let Some(owner) = owner {
        match owner {
            ExecutionOwner::Adapter => {
                let _ = deadline.adapter.cancel(deadline.core).await;
            }
            ExecutionOwner::GenericProcess => {
                if let Some(cancellation) = deadline
                    .generic
                    .processes
                    .lock()
                    .expect("generic supervisor registry is not poisoned")
                    .get(&deadline.core)
                    .map(|process| process.cancellation.clone())
                {
                    let _ = cancellation.send(Some(GenericCancellation::Timeout));
                }
            }
        }
    }
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
    token: PendingLaunchToken,
    session: UiSessionId,
    receiver: watch::Receiver<SessionReadiness>,
}

impl PendingAttachment {
    #[must_use]
    pub fn session(&self) -> &UiSessionId {
        &self.session
    }
    #[must_use]
    pub fn token(&self) -> PendingLaunchToken {
        self.token
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
        let (diagnostics_tx, diagnostics_rx) = mpsc::unbounded_channel();
        Arc::new_cyclic(|self_weak| Self {
            adapter,
            config: ConfigStore::from_compiled(config_path, config),
            state: Arc::new(Mutex::new(BrokerState::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            generic: Arc::new(GenericSupervisor::default()),
            cleanup: Arc::new(CleanupSupervisor::default()),
            execution_transitions: Arc::new(Notify::new()),
            diagnostics_tx,
            diagnostics_rx: Mutex::new(Some(diagnostics_rx)),
            token_source: Mutex::new(OsTokenSource),
            next_session: AtomicU64::new(1),
            next_execution: AtomicU64::new(1),
            next_event: Arc::new(AtomicU64::new(1)),
            self_weak: self_weak.clone(),
            #[cfg(test)]
            cleanup_enqueue_hook: StdMutex::new(None),
            #[cfg(test)]
            commit_ui_launch_hook: StdMutex::new(None),
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
        let (diagnostics_tx, diagnostics_rx) = mpsc::unbounded_channel();
        Ok(Arc::new_cyclic(|self_weak| Self {
            adapter,
            config,
            state: Arc::new(Mutex::new(BrokerState::default())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            generic: Arc::new(GenericSupervisor::default()),
            execution_transitions: Arc::new(Notify::new()),
            cleanup: Arc::new(CleanupSupervisor::default()),
            diagnostics_tx,
            diagnostics_rx: Mutex::new(Some(diagnostics_rx)),
            token_source: Mutex::new(OsTokenSource),
            next_session: AtomicU64::new(1),
            next_execution: AtomicU64::new(1),
            next_event: Arc::new(AtomicU64::new(1)),
            self_weak: self_weak.clone(),
            #[cfg(test)]
            cleanup_enqueue_hook: StdMutex::new(None),
            #[cfg(test)]
            commit_ui_launch_hook: StdMutex::new(None),
        }))
    }

    /// Transfers the one process-lifetime sink for detached terminal diagnostics.
    ///
    /// The composition root owns persistence and logging; the broker retains no
    /// UI-dependent fallback for detached execution failures.
    pub async fn take_diagnostics(&self) -> Option<mpsc::UnboundedReceiver<BrokerDiagnostic>> {
        self.diagnostics_rx.lock().await.take()
    }

    pub fn config_path(&self) -> &std::path::Path {
        self.config.path()
    }

    pub async fn generation(&self) -> CompiledGeneration {
        self.config.snapshot().await.config.generation
    }
    #[cfg(test)]
    pub(crate) fn set_cleanup_enqueue_hook(&self, hook: Option<Arc<CleanupEnqueueHook>>) {
        *self
            .cleanup_enqueue_hook
            .lock()
            .expect("cleanup enqueue hook is not poisoned") = hook;
    }
    #[cfg(test)]
    pub(crate) fn set_commit_ui_launch_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .commit_ui_launch_hook
            .lock()
            .expect("commit UI launch hook is not poisoned") = hook;
    }

    /// Closes every menu-owned pending pane and releases every UI capture before an activation
    /// coordinator drops this broker's listener. Session-owned executions follow the same
    /// authoritative dismissal transition as quit and disconnect: Detach-policy work keeps its
    /// supervised child, while Cancel-policy cancellable work is stopped.
    /// Enqueueing is not enough: drain waits, with a bounded deadline, until
    /// the cleanup entries it created are confirmed gone, and returns
    /// `BrokerError::ActivationCleanupUnconfirmed` (reopening admission)
    /// carrying the recorded `last_cleanup_error` when the deadline passes.
    ///
    /// # Errors
    ///
    /// Returns `BrokerError::ActivationDrainRefused` while a non-cancellable host
    /// execution is in flight, `BrokerError::ActivationCleanupUnconfirmed` when
    /// initiated host cleanup is not confirmed before the bounded deadline, or
    /// the first cancellation/detach failure (each of which reopens admission
    /// so the coordinator can retry).
    pub async fn drain_for_activation(&self) -> Result<(), BrokerError> {
        // Register the notification before observing state so a reservation
        // cannot transition between the observation and the await. The seal is
        // deliberately retained until every pre-existing dispatch reservation
        // either becomes a cancellable owner or proves that no dispatch occurred.
        loop {
            let notified = self.execution_transitions.notified();
            tokio::pin!(notified);
            // `enable` registers before reading the state; a transition that
            // lands immediately afterwards cannot lose its wake-up.
            notified.as_mut().enable();
            let reserved = {
                let mut state = self.state.lock().await;
                state.activation_sealed = true;
                state.executions.values().any(|record| {
                    record.phase == ExecutionPhase::Reserved && record.deferred.is_none()
                })
            };
            if !reserved {
                break;
            }
            notified.await;
        }
        let (adapter_executions, deferred) = {
            let mut state = self.state.lock().await;
            let refused = state.executions.values().find_map(|record| {
                (record.phase == ExecutionPhase::Adapter && !record.cancellable).then(|| {
                    let owner = record.session.as_ref().map_or_else(
                        || "detached".to_owned(),
                        |session| session.as_str().to_owned(),
                    );
                    (owner, record.core.0)
                })
            });
            if let Some((owner, core)) = refused {
                state.activation_sealed = false;
                return Err(BrokerError::ActivationDrainRefused(format!(
                    "{owner} host execution {core} is non-cancellable and still in flight",
                )));
            }
            let adapter_executions = state
                .executions
                .values()
                .filter(|record| record.phase == ExecutionPhase::Adapter)
                .map(|record| record.core)
                .collect::<Vec<_>>();
            let deferred = state
                .executions
                .values()
                .filter(|record| record.deferred.is_some())
                .map(|record| record.core)
                .collect::<Vec<_>>();
            (adapter_executions, deferred)
        };
        let drained = async {
            for execution in &adapter_executions {
                self.request_execution_stop(*execution).await?;
            }
            for execution in adapter_executions {
                self.await_adapter_execution_terminal(execution).await;
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
                self.emit_broker_retiring(&session).await;
                self.detach(&session, CaptureReleaseReason::UiDismissed)
                    .await?;
            }
            for execution in deferred {
                self.request_execution_stop(execution).await?;
                self.await_adapter_execution_terminal(execution).await;
            }
            self.await_cleanup_drain().await?;
            Ok(())
        }
        .await;
        if drained.is_err() {
            self.state.lock().await.activation_sealed = false;
        }
        drained
    }

    /// Bounded wait for broker-owned host cleanup initiated by this drain.
    /// Polls the cleanup registry for emptiness while listening on the
    /// supervisor wake notification; no host call runs under the state lock.
    /// Returns `ActivationCleanupUnconfirmed` with the recorded
    /// `last_cleanup_error` payloads when `DRAIN_CLEANUP_DEADLINE` passes
    /// with entries still present.
    async fn await_cleanup_drain(&self) -> Result<(), BrokerError> {
        // tokio clock throughout: `start_paused` tests advance the same clock
        // the deadline reads, so the failure path completes without
        // wall-clock sleeps (see the `DRAIN_CLEANUP_DEADLINE` comment).
        let deadline = tokio::time::Instant::now() + DRAIN_CLEANUP_DEADLINE;
        loop {
            let pending = {
                let state = self.state.lock().await;
                let panes = state.cleanup.pending_panes.len();
                let captures = state.cleanup.captures.len();
                if panes == 0 && captures == 0 {
                    return Ok(());
                }
                let mut details = Vec::new();
                for (lease, entry) in &state.cleanup.pending_panes {
                    details.push(format!(
                        "pending pane {} attempt {}: {}",
                        lease.as_str(),
                        entry.attempt,
                        entry.last_cleanup_error.as_ref().map_or_else(
                            || "awaiting first attempt".to_owned(),
                            ToString::to_string
                        )
                    ));
                }
                for (lease, entry) in &state.cleanup.captures {
                    details.push(format!(
                        "capture {} attempt {}: {}",
                        lease.as_str(),
                        entry.attempt,
                        entry.last_cleanup_error.as_ref().map_or_else(
                            || "awaiting first attempt".to_owned(),
                            ToString::to_string
                        )
                    ));
                }
                details.sort();
                details.join("; ")
            };
            if tokio::time::Instant::now() >= deadline {
                return Err(BrokerError::ActivationCleanupUnconfirmed(pending));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let notified = self.cleanup.wake.notified();
            tokio::pin!(notified);
            // Re-check on every wake or timeout tick: the notification only
            // wakes, the state remains authoritative.
            tokio::select! {
                () = tokio::time::sleep(remaining.min(DRAIN_CLEANUP_POLL)) => {}
                () = &mut notified => {}
            }
        }
    }

    /// Reports whether detached generic children are still under supervision.
    /// The supervisor-only linger after activation stop uses this to exit only
    /// after every remaining child is reaped.
    #[must_use]
    pub(crate) async fn has_supervised_children(&self) -> bool {
        !self
            .generic
            .processes
            .lock()
            .expect("generic supervisor registry is not poisoned")
            .is_empty()
    }

    /// Reopens launch and execution admission after a failed Prepare or a successful
    /// Abort returned the broker to Running. The coordinator enters Running first and
    /// then calls this; a concurrent admission landing in between is spuriously
    /// rejected (fail-closed) rather than wrongly admitted.
    pub async fn reopen_dispatch(&self) {
        self.state.lock().await.activation_sealed = false;
    }

    /// Reserves ownership before *any* effectful dispatch or spawn. The awaited
    /// index is checked and inserted under the same lock as the activation seal.
    async fn reserve_execution(
        &self,
        session: UiSessionId,
        wire: ExecutionId,
        core: CoreExecutionId,
        owner: ExecutionOwner,
        deferred: Option<PostDismissalPortableDispatchRequest>,
        policy: &ExecutionPolicy,
    ) -> Result<(), BrokerError> {
        let mut state = self.state.lock().await;
        if state.activation_sealed {
            return Err(BrokerError::ActivationInProgress);
        }
        let awaiting = policy.mode == muxe_core::ExecutionMode::Await;
        if awaiting && state.awaiting.contains_key(&session) {
            return Err(BrokerError::PendingExecutionInFlight);
        }
        state.executions.insert(
            core,
            ExecutionRecord {
                wire,
                core,
                session: Some(session.clone()),
                awaiting,
                cancellable: owner == ExecutionOwner::GenericProcess,
                on_menu_control: policy.on_menu_control,
                timeout: policy.timeout,
                on_timeout: policy.on_timeout,
                owner,
                phase: ExecutionPhase::Reserved,
                deadline_scheduled: false,
                deferred,
                pending_control: None,
                termination_requested: false,
            },
        );
        if awaiting {
            state.awaiting.insert(session, core);
        }
        Ok(())
    }

    /// A rejected spawn/dispatch is the only path that proves no external work
    /// exists, so it alone may return a reservation to admission.
    async fn release_reservation(&self, core: CoreExecutionId) {
        let released = {
            let mut state = self.state.lock().await;
            if !state
                .executions
                .get(&core)
                .is_some_and(|record| record.phase == ExecutionPhase::Reserved)
            {
                false
            } else {
                let record = state
                    .executions
                    .remove(&core)
                    .expect("reserved execution exists");
                if record.awaiting
                    && let Some(session) = record.session
                {
                    state.awaiting.remove(&session);
                }
                true
            }
        };
        if released {
            self.execution_transitions.notify_waiters();
        }
    }

    /// Publishes the exact owner after its adapter acceptance or child
    /// supervision is established. A seal that won after reservation retains the
    /// owner and requests one shared cancellation transition.
    async fn activate_execution(
        &self,
        core: CoreExecutionId,
        phase: ExecutionPhase,
        cancellable: bool,
    ) -> Result<(), BrokerError> {
        let (sealed, deadline) = {
            let mut state = self.state.lock().await;
            let Some(record) = state.executions.get_mut(&core) else {
                let sealed = state.activation_sealed;
                self.execution_transitions.notify_waiters();
                if sealed {
                    return Err(BrokerError::ActivationInProgress);
                }
                return Ok(());
            };
            record.phase = phase;
            record.cancellable = cancellable;
            let deadline = (!record.deadline_scheduled)
                .then(|| record.timeout.map(|timeout| (timeout, record.on_timeout)))
                .flatten();
            record.deadline_scheduled |= deadline.is_some();
            (state.activation_sealed, deadline)
        };
        self.execution_transitions.notify_waiters();
        if let Some((timeout, on_timeout)) = deadline {
            tokio::spawn(supervise_execution_deadline(ExecutionDeadline {
                core,
                timeout,
                on_timeout,
                state: Arc::clone(&self.state),
                sessions: Arc::clone(&self.sessions),
                adapter: Arc::clone(&self.adapter),
                generic: Arc::clone(&self.generic),
                next_event: Arc::clone(&self.next_event),
                broker: Weak::clone(&self.self_weak),
            }));
        }
        if sealed {
            return Err(BrokerError::ActivationInProgress);
        }
        Ok(())
    }
    async fn request_execution_stop(&self, core: CoreExecutionId) -> Result<(), BrokerError> {
        let record = {
            let mut state = self.state.lock().await;
            let Some(record) = state.executions.get_mut(&core) else {
                return Ok(());
            };
            if record.termination_requested {
                return Ok(());
            }
            record.termination_requested = true;
            record.clone()
        };
        let result = if record.phase == ExecutionPhase::Reserved
            && record.owner == ExecutionOwner::GenericProcess
        {
            if self
                .generic
                .processes
                .lock()
                .expect("generic supervisor registry is not poisoned")
                .contains_key(&core)
            {
                self.cancel_generic(core).await
            } else {
                Ok(())
            }
        } else {
            match record.owner {
                ExecutionOwner::Adapter => {
                    self.adapter.cancel(core).await.map_err(BrokerError::from)
                }
                ExecutionOwner::GenericProcess => self.cancel_generic(core).await,
            }
        };
        if result.is_err() {
            let mut state = self.state.lock().await;
            if let Some(record) = state.executions.get_mut(&core)
                && record.phase != ExecutionPhase::Terminal
            {
                record.termination_requested = false;
            }
        }
        result
    }
    async fn await_adapter_execution_terminal(&self, core: CoreExecutionId) {
        loop {
            let notified = self.execution_transitions.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.state.lock().await.executions.contains_key(&core) {
                return;
            }
            notified.await;
        }
    }
    /// Converts an accepted detached action from a temporary admission hold into
    /// background-owned work. Its terminal failure is logged, not sent to a UI
    /// that deliberately chose not to await it.
    async fn detach_execution_ui(&self, core: CoreExecutionId) {
        let mut state = self.state.lock().await;
        let session = {
            let Some(record) = state.executions.get_mut(&core) else {
                return;
            };
            record.awaiting = false;
            record.session.take()
        };
        if let Some(session) = session {
            state.awaiting.remove(&session);
        }
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
    /// Resolves a gated UI disconnect under one state lock: while the launch
    /// token is still pending *for this session* the token branch owns
    /// cleanup; once commit or publication consumed the token, the attached
    /// session branch owns it. Exactly one branch runs, after unlocking, so a
    /// commit racing disconnect converges instead of aborting a consumed
    /// token and leaking the session. A mismatched (token, session) pair
    /// touches nothing: stale disconnects must never abort or detach
    /// unrelated ownership.
    pub async fn disconnect_gated(&self, token: PendingLaunchToken, session: UiSessionId) {
        enum Cleanup {
            Pending {
                session: UiSessionId,
                registration: Option<RegisteredPane>,
            },
            Attached {
                session: UiSessionId,
                registration: Option<RegisteredPane>,
            },
            Stale,
        }
        let cleanup = {
            let mut state = self.state.lock().await;
            let pending_matches = state.pending_sessions.get(&token) == Some(&session)
                && state
                    .gate
                    .pending(token)
                    .is_some_and(|launch| launch.attached_ui.as_ref() == Some(&session));
            if pending_matches {
                let registration = state.gate.abort(token).ok().flatten();
                state.pending_sessions.remove(&token);
                Cleanup::Pending {
                    session,
                    registration,
                }
            } else if state
                .gated_sessions
                .get(&token)
                .is_some_and(|owner| owner.session == session)
            {
                let registration = state
                    .gated_sessions
                    .remove(&token)
                    .and_then(|owner| owner.registration);
                Cleanup::Attached {
                    session,
                    registration,
                }
            } else {
                Cleanup::Stale
            }
        };
        match cleanup {
            Cleanup::Pending {
                session,
                registration,
            } => {
                self.fail_session(&session, CaptureReleaseReason::UiDismissed)
                    .await;
                if let Some(registration) = registration {
                    self.close_registered(&session, registration).await;
                }
            }
            Cleanup::Attached {
                session,
                registration,
            } => {
                self.disconnect(Some(&session)).await;
                if let Some(registration) = registration {
                    self.close_registered(&session, registration).await;
                }
            }
            Cleanup::Stale => {}
        }
    }
    /// Confirms that the service successfully published a gated `UiAttached`
    /// response. The state claim and synchronous adapter provenance release are
    /// adjacent, so cancellation cannot strand a claimed lease.
    pub(crate) async fn confirm_gated_attachment(
        &self,
        token: PendingLaunchToken,
        session: &UiSessionId,
    ) {
        let registration = {
            let mut state = self.state.lock().await;
            let owner_matches = state
                .gated_sessions
                .get(&token)
                .is_some_and(|owner| owner.session == *session);
            owner_matches
                .then(|| state.gated_sessions.remove(&token))
                .flatten()
                .and_then(|owner| owner.registration)
        };
        if let Some(lease) = registration.and_then(|pane| pane.lease) {
            self.adapter.release_pending_pane(lease);
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
                    Err(error) if error.kind == muxe_adapter_api::AdapterErrorKind::Shutdown => {
                        return;
                    }
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
                        CaptureLossReason::BrokerLeaseExpired
                        | CaptureLossReason::AdapterHealth => CaptureReleaseReason::AdapterShutdown,
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
        let record = {
            let mut state = self.state.lock().await;
            let Some(record) = state.executions.get_mut(&core) else {
                return;
            };
            if record.owner != ExecutionOwner::Adapter
                || !matches!(
                    record.phase,
                    ExecutionPhase::Reserved | ExecutionPhase::Adapter
                )
            {
                return;
            }
            record.phase = ExecutionPhase::Terminal;
            let record = state
                .executions
                .remove(&core)
                .expect("adapter owner exists");
            if record.awaiting
                && let Some(session) = &record.session
            {
                state.awaiting.remove(session);
            }
            record
        };
        self.execution_transitions.notify_waiters();
        if !record.awaiting {
            if let Some(diagnostic) = diagnostic {
                let _ = self.diagnostics_tx.send(BrokerDiagnostic {
                    execution: record.wire,
                    outcome,
                    code: diagnostic.code,
                });
                tracing::error!(
                    ?record.wire,
                    execution = core.0,
                    diagnostic = ?diagnostic,
                    "detached adapter execution did not complete successfully"
                );
            }
            return;
        }
        let Some(session) = record.session else {
            return;
        };
        if record.pending_control.is_some() {
            return;
        }
        deliver_session_event_to(
            &self.sessions,
            &session,
            self.new_event_id(),
            BrokerEvent::ExecutionCompleted {
                session: session.clone(),
                execution: record.wire,
                outcome,
                diagnostic,
            },
            "ExecutionCompleted",
            &self.self_weak,
        )
        .await;
    }

    async fn emit_execution_completed(
        &self,
        session: UiSessionId,
        execution: ExecutionId,
        outcome: ExecutionOutcome,
        diagnostic: Option<ProtocolDiagnostic>,
    ) {
        deliver_session_event_to(
            &self.sessions,
            &session,
            self.new_event_id(),
            BrokerEvent::ExecutionCompleted {
                session: session.clone(),
                execution,
                outcome,
                diagnostic,
            },
            "ExecutionCompleted",
            &self.self_weak,
        )
        .await;
    }

    async fn emit_broker_retiring(&self, session: &UiSessionId) {
        let events = {
            let sessions = self.sessions.lock().await;
            sessions.get(session).map(|record| record.events.clone())
        };
        if let Some(events) = events {
            // A full UI event queue must never delay retirement teardown; the
            // connection close remains the backstop, so this notification is best effort.
            let _ = events.try_send(WireMessage::Event {
                event_id: self.new_event_id(),
                event: BrokerEvent::BrokerRetiring,
            });
        }
    }

    async fn broadcast_health(&self, healthy: bool, error: Option<AdapterError>) {
        let diagnostic =
            error.map(|error| diagnostic(DiagnosticCode::HostUnavailable, &error.to_string()));
        let events = self
            .sessions
            .lock()
            .await
            .iter()
            .map(|(session, record)| (session.clone(), record.events.clone()))
            .collect::<Vec<_>>();
        for (session, events) in &events {
            note_send_outcome(
                &events.try_send(WireMessage::Event {
                    event_id: self.new_event_id(),
                    event: BrokerEvent::AdapterHealthChanged {
                        healthy,
                        diagnostic: diagnostic.clone(),
                    },
                }),
                &self.self_weak,
                session,
                "AdapterHealthChanged",
            );
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
        for (session, events) in &scoped {
            note_send_outcome(
                &events.try_send(WireMessage::Event {
                    event_id: self.new_event_id(),
                    event: BrokerEvent::AdapterHealthChanged {
                        healthy: false,
                        diagnostic: Some(diagnostic(DiagnosticCode::HostUnavailable, &message)),
                    },
                }),
                &self.self_weak,
                session,
                "AdapterHealthChanged",
            );
        }
        let failed = {
            let state = self.state.lock().await;
            state
                .executions
                .values()
                .filter(|record| {
                    record.owner == ExecutionOwner::Adapter
                        && record.pending_control.is_none()
                        && record.session.as_ref().is_some_and(|session| {
                            scoped.iter().any(|(scoped, _)| scoped == session)
                        })
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for record in &failed {
            if let Some(session) = &record.session {
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
                    self.close_registered(&session, registration).await;
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
        let root = wire_menu_id_to_core(&request.root).filter(|root| config.menu(root).is_some());
        if root.is_none() {
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
            let retained = RegisteredPane {
                pane: HostPaneId::new(registration.pane.as_str()),
                temporary_tab: registration
                    .temporary_tab
                    .as_ref()
                    .map(|tab| HostTabId::new(tab.as_str())),
                lease: Some(lease),
            };
            let session = UiSessionId::new(registration.ui_session.as_str());
            drop(state);
            self.close_registered(&session, retained).await;
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
        if wire_menu_id_to_core(&request.root).is_none_or(|root| config.menu(&root).is_none()) {
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
                if let Some(registration) = registration {
                    self.enqueue_pending_pane_close(
                        &session,
                        registration,
                        Some(error.to_string()),
                    )
                    .await;
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
        let core_root = wire_menu_id_to_core(&request.root)
            .ok_or_else(|| BrokerError::UnknownMenu(request.root.clone()))?;
        let record = SessionRecord {
            config: Arc::clone(&config),
            root: core_root,
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
                                    state.gated_sessions.insert(
                                        token,
                                        GatedSession {
                                            session: session.clone(),
                                            registration: registration.clone(),
                                        },
                                    );
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
            if let Some(registration) = registration {
                self.enqueue_pending_pane_close(&session, registration, Some(error.to_string()))
                    .await;
            }
            return Err(error);
        }
        if !ready {
            return Ok(RequestResult::WaitForAttachment(Box::new(
                PendingAttachment {
                    token: request
                        .pending_launch
                        .expect("gated attachment waiter always has a launch token"),
                    session,
                    receiver,
                },
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
                return Err(error);
            }
        };
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
        let session = {
            let mut state = self.state.lock().await;
            let registration = state
                .gate
                .pending(token)
                .and_then(|launch| launch.registered_pane.clone());
            let session = state.gate.commit(token, &pane)?;
            if let Some(session) = &session {
                state.pending_sessions.remove(&token);
                state.gated_sessions.insert(
                    token,
                    GatedSession {
                        session: session.clone(),
                        registration,
                    },
                );
            }
            session
        };
        #[cfg(test)]
        {
            let hook = self
                .commit_ui_launch_hook
                .lock()
                .expect("commit UI launch hook is not poisoned")
                .clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }
        let Some(session) = session else {
            return Ok(());
        };
        if let Err(error) = self.begin_capture(&session).await {
            let detach_error = self
                .detach(&session, CaptureReleaseReason::UiDismissed)
                .await
                .err();
            if let Some(cleanup_error) = detach_error {
                tracing::error!(
                    %cleanup_error,
                    "capture initialization failed and session detach also failed"
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
                        "commit response failed and session detach also failed"
                    );
                }
                return Err(error);
            }
        };
        {
            let sessions = self.sessions.lock().await;
            if let Some(record) = sessions.get(&session) {
                let _ = record
                    .readiness
                    .send(SessionReadiness::Ready(Box::new(response)));
            }
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
            self.close_registered(&session, registration).await;
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
            self.close_registered(&session, registration).await;
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
        if let Some(record) = sessions.get_mut(session) {
            record.capture = Some(capture);
            Ok(())
        } else {
            // A concurrent detach consumed the session while the host
            // round-trip was in flight. The freshly acquired lease is still
            // ours exactly once: enqueue it before reporting the miss so the
            // supervised cleanup task can retry without the caller retaining
            // host provenance.
            drop(sessions);
            self.enqueue_capture_cleanup(
                capture,
                CaptureReleaseReason::UiDismissed,
                Some(format!(
                    "UI session {session:?} detached during capture start"
                )),
            )
            .await;
            Err(BrokerError::UnknownSession(session.clone()))
        }
    }

    async fn attached_response(
        &self,
        session: &UiSessionId,
    ) -> Result<BrokerResponse, BrokerError> {
        let sessions = self.sessions.lock().await;
        let record = sessions
            .get(session)
            .ok_or_else(|| BrokerError::UnknownSession(session.clone()))?;
        let view = record
            .config
            .attachment_view(&record.root)
            .ok_or_else(|| BrokerError::UnknownMenu(core_menu_id_to_wire(&record.root)))?;
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
                let deferred = PostDismissalPortableDispatchRequest {
                    execution: core_execution,
                    action,
                    origin,
                    ui_pane,
                };
                self.reserve_execution(
                    request.session.clone(),
                    execution,
                    core_execution,
                    ExecutionOwner::Adapter,
                    Some(deferred),
                    &binding.settings.execution,
                )
                .await?;
                return Ok(RequestResult::Immediate(
                    BrokerResponse::InvocationAccepted {
                        execution,
                        disposition: InvocationDisposition::Dismissed,
                    },
                ));
            }
        }
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
                self.reserve_execution(
                    request.session.clone(),
                    execution,
                    core_execution,
                    ExecutionOwner::Adapter,
                    None,
                    &binding.settings.execution,
                )
                .await?;
                match self
                    .dispatch_native(core_execution, origin, &candidate)
                    .await
                {
                    Ok(accepted) if accepted.execution == core_execution => {
                        Some(accepted.capabilities)
                    }
                    Ok(accepted) => {
                        self.activate_execution(
                            core_execution,
                            ExecutionPhase::Adapter,
                            accepted.capabilities.cancellable,
                        )
                        .await?;
                        return Err(BrokerError::MismatchedExecution);
                    }
                    Err(error) => {
                        self.release_reservation(core_execution).await;
                        return Err(error);
                    }
                }
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
        if disposition == InvocationDisposition::Detached {
            self.detach_execution_ui(core_execution).await;
        }
        if let Some(capabilities) = accepted_capabilities {
            self.activate_execution(
                core_execution,
                ExecutionPhase::Adapter,
                capabilities.cancellable,
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
                self.reserve_execution(
                    session.clone(),
                    execution,
                    core_execution,
                    ExecutionOwner::GenericProcess,
                    None,
                    policy,
                )
                .await?;
                if let Err(error) = self
                    .execute_command(CommandLaunch {
                        session: session.clone(),
                        wire: execution,
                        core: core_execution,
                        command,
                        origin,
                        cwd_from_context: command_cwd_from_context,
                        policy: policy.clone(),
                    })
                    .await
                {
                    self.release_reservation(core_execution).await;
                    return Err(error);
                }
                let disposition = if policy.mode == muxe_core::ExecutionMode::Await {
                    InvocationDisposition::Awaited
                } else {
                    self.detach_execution_ui(core_execution).await;
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
                self.reserve_execution(
                    session.clone(),
                    execution,
                    core_execution,
                    ExecutionOwner::Adapter,
                    None,
                    policy,
                )
                .await?;
                let accepted = match self
                    .adapter
                    .dispatch_portable(PortableDispatchRequest {
                        execution: core_execution,
                        action: ResolvedPortableAction { action },
                        origin,
                    })
                    .await
                {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        self.release_reservation(core_execution).await;
                        return Err(BrokerError::from(error));
                    }
                };
                if accepted.execution != core_execution {
                    self.activate_execution(
                        core_execution,
                        ExecutionPhase::Adapter,
                        accepted.capabilities.cancellable,
                    )
                    .await?;
                    return Err(BrokerError::MismatchedExecution);
                }
                Ok(std::ops::ControlFlow::Continue(Some(accepted.capabilities)))
            }
        }
    }

    async fn dispatch_native(
        &self,
        core_execution: CoreExecutionId,
        origin: muxe_core::OriginContext,
        candidate: &muxe_core::NativeActionCandidate,
    ) -> Result<muxe_adapter_api::DispatchAccepted, BrokerError> {
        let action = ResolvedNativeAction::from_origin(candidate, &origin)
            .map_err(|_| BrokerError::ContextUnavailable)?;
        self.adapter
            .dispatch_native(muxe_adapter_api::NativeDispatchRequest {
                execution: core_execution,
                action,
                origin,
            })
            .await
            .map_err(BrokerError::from)
    }

    pub(crate) async fn execute_command(&self, launch: CommandLaunch) -> Result<(), BrokerError> {
        let CommandLaunch {
            wire: _,
            session,
            core,
            command,
            origin,
            policy: _,
            cwd_from_context,
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
        let (cancellation, cancellation_rx) = watch::channel(None);
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
        // This synchronous insertion and the reaper task are established before
        // any fallible state transition. A seal can therefore only request
        // cancellation of a child that is already guaranteed to be reaped.
        let cancellation_for_seal = cancellation.clone();
        self.generic
            .processes
            .lock()
            .expect("generic supervisor registry is not poisoned")
            .insert(core, GenericProcess { cancellation });
        let supervisor = tokio::spawn(supervise_generic_child(GenericChildSpec {
            child,
            process_group,
            cancellation: cancellation_rx,
            session: session.clone(),
            core,
            state: Arc::clone(&self.state),
            sessions: Arc::clone(&self.sessions),
            generic: Arc::clone(&self.generic),
            next_event: Arc::clone(&self.next_event),
            diagnostics_tx: self.diagnostics_tx.clone(),
            broker: Weak::clone(&self.self_weak),
        }));
        let sealed_before_activation = self
            .state
            .lock()
            .await
            .executions
            .get(&core)
            .is_some_and(|record| record.termination_requested);
        if sealed_before_activation {
            let _ = cancellation_for_seal.send(Some(GenericCancellation::UserRequested));
        }
        let activation = self
            .activate_execution(core, ExecutionPhase::Generic, true)
            .await;
        if let Err(error) = activation {
            let _ = cancellation_for_seal.send(Some(GenericCancellation::UserRequested));
            let _ = supervisor.await;
            self.generic
                .processes
                .lock()
                .expect("generic supervisor registry is not poisoned")
                .remove(&core);
            {
                let mut state = self.state.lock().await;
                state.executions.remove(&core);
                state.awaiting.remove(&session);
            }
            self.execution_transitions.notify_waiters();
            return Err(error);
        }
        Ok(())
    }

    async fn cancel_generic(&self, execution: CoreExecutionId) -> Result<(), BrokerError> {
        let cancellation = self
            .generic
            .processes
            .lock()
            .expect("generic supervisor registry is not poisoned")
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
            let Some(core) = state.awaiting.get(&request.session).copied() else {
                return Ok(RequestResult::Immediate(BrokerResponse::Acknowledged));
            };
            let Some(record) = state.executions.get_mut(&core) else {
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
            muxe_core::MenuControlAction::Cancel if pending.cancellable => {
                self.request_execution_stop(pending.core).await
            }
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
        if state.awaiting.get(session) != Some(&core) {
            return false;
        }
        let Some(record) = state.executions.get_mut(&core) else {
            return false;
        };
        if record.pending_control != Some(control) {
            return false;
        }
        record.pending_control = None;
        true
    }

    async fn schedule_post_dismissal(&self, expected: CoreExecutionId) {
        let request = {
            let mut state = self.state.lock().await;
            let Some(record) = state.executions.get_mut(&expected) else {
                return;
            };
            record.deferred.take()
        };
        let Some(request) = request else {
            return;
        };
        match self
            .adapter
            .dispatch_portable_after_ui_dismissal(request)
            .await
        {
            Ok(accepted) => {
                self.detach_execution_ui(expected).await;
                if let Err(error) = self
                    .activate_execution(
                        expected,
                        ExecutionPhase::Adapter,
                        accepted.capabilities.cancellable,
                    )
                    .await
                {
                    tracing::error!(execution = expected.0, %error, "post-dismissal execution was sealed");
                }
                if accepted.execution != expected {
                    tracing::error!(
                        expected_execution = expected.0,
                        received_execution = accepted.execution.0,
                        "post-dismissal adapter dispatch acknowledged a different execution"
                    );
                }
            }
            Err(error) => {
                self.release_reservation(expected).await;
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
        // Provenance enqueue is atomic with the ownership unlink: the
        // capture and gated-registration cleanup entries are inserted under
        // the same state lock that removes them, so cancellation between the
        // lock release and the supervised spawns below cannot drop them. Only
        // the readiness send and the `.await`-holding spawns happen outside.
        let (cancel, deferred) = {
            let mut state = self.state.lock().await;
            state.gate.detach(session);
            let tokens = state
                .gated_sessions
                .iter()
                .filter(|(_, owner)| owner.session == *session)
                .map(|(token, _)| *token)
                .collect::<Vec<_>>();
            let registrations = tokens
                .into_iter()
                .filter_map(|token| state.gated_sessions.remove(&token))
                .filter_map(|owner| owner.registration)
                .collect::<Vec<_>>();
            state.awaiting.remove(session);
            if let Some(capture) = record.as_ref().and_then(|record| record.capture.clone()) {
                self.insert_capture_cleanup_locked(&mut state, &capture, reason, None);
            }
            for registration in &registrations {
                self.insert_pending_pane_cleanup_locked(&mut state, session, registration, None);
            }
            let mut cancel = Vec::new();
            let mut deferred = Vec::new();
            for execution in state.executions.values_mut() {
                if execution.session.as_ref() != Some(session) {
                    continue;
                }
                execution.awaiting = false;
                if execution.pending_control.is_none()
                    && execution.on_menu_control == muxe_core::MenuControlAction::Cancel
                    && execution.cancellable
                {
                    cancel.push(execution.core);
                }
                if execution.deferred.is_some() {
                    deferred.push(execution.core);
                }
                execution.session = None;
            }
            (cancel, deferred)
        };
        for execution in cancel {
            let _ = self.request_execution_stop(execution).await;
        }
        if let Some(record) = record {
            // The capture entry was already inserted atomically with the
            // unlink above; only the readiness failure is sent here.
            let _ = record.readiness.send(SessionReadiness::Failed(diagnostic(
                DiagnosticCode::LaunchAborted,
                "UI session detached",
            )));
        }
        for execution in deferred {
            self.schedule_post_dismissal(execution).await;
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
        self.close_registered(session, registration).await;
        Ok(())
    }

    /// Enqueues a retained pending-pane close and starts or wakes the
    /// supervised task for its typed lease identity. This method only mutates
    /// broker state and never awaits the adapter.
    async fn close_registered(&self, session: &UiSessionId, registration: RegisteredPane) {
        self.enqueue_pending_pane_close(session, registration, None)
            .await;
    }

    /// Inserts a pending-pane cleanup entry under the caller's state lock and
    /// starts (or wakes) its supervised task. The caller must hold the state
    /// guard: the insert is atomic with the ownership unlink, so no `.await`
    /// can interleave and drop provenance. Spawning only takes synchronous
    /// std mutexes, so it is safe inside the critical section.
    fn insert_pending_pane_cleanup_locked(
        &self,
        state: &mut BrokerState,
        session: &UiSessionId,
        registration: &RegisteredPane,
        primary: Option<String>,
    ) {
        let primary = primary.map(bounded_cleanup_message);
        let Some(lease) = registration.lease.clone() else {
            return;
        };
        let key = CleanupTaskKey::PendingPane(lease.id.clone());
        let entry = state
            .cleanup
            .pending_panes
            .entry(lease.id)
            .or_insert_with(|| PendingPaneCleanup {
                session: session.clone(),
                registration: registration.clone(),
                phase: CleanupPhase::Ready,
                attempt: 0,
                next_retry: Instant::now(),
                primary: primary.clone(),
                last_cleanup_error: None,
            });
        if entry.primary.is_none() {
            entry.primary = primary;
        }
        let should_start = if entry.phase == CleanupPhase::InFlight {
            false
        } else {
            entry.phase = CleanupPhase::InFlight;
            entry.next_retry = Instant::now();
            true
        };
        if should_start {
            self.spawn_pending_pane_cleanup(key);
        } else {
            self.cleanup.notify();
        }
    }

    /// Inserts a capture cleanup entry under the caller's state lock. Same
    /// atomicity contract as `insert_pending_pane_cleanup_locked`.
    fn insert_capture_cleanup_locked(
        &self,
        state: &mut BrokerState,
        lease: &CaptureLease,
        reason: CaptureReleaseReason,
        primary: Option<String>,
    ) {
        let primary = primary.map(bounded_cleanup_message);
        let lease_id = lease.id.clone();
        let key = CleanupTaskKey::Capture(lease_id.clone());
        let entry = state
            .cleanup
            .captures
            .entry(lease_id)
            .or_insert_with(|| CaptureCleanup {
                lease: lease.clone(),
                reason,
                phase: CleanupPhase::Ready,
                attempt: 0,
                next_retry: Instant::now(),
                primary: primary.clone(),
                last_cleanup_error: None,
            });
        if entry.primary.is_none() {
            entry.primary = primary;
        }
        let should_start = if entry.phase == CleanupPhase::InFlight {
            false
        } else {
            entry.phase = CleanupPhase::InFlight;
            entry.next_retry = Instant::now();
            true
        };
        if should_start {
            self.spawn_capture_cleanup(key);
        } else {
            self.cleanup.notify();
        }
    }

    async fn enqueue_pending_pane_close(
        &self,
        session: &UiSessionId,
        registration: RegisteredPane,
        primary: Option<String>,
    ) {
        {
            let mut state = self.state.lock().await;
            self.insert_pending_pane_cleanup_locked(&mut state, session, &registration, primary);
        }
        #[cfg(test)]
        {
            let hook = self
                .cleanup_enqueue_hook
                .lock()
                .expect("cleanup enqueue hook is not poisoned")
                .clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }
    }
    async fn enqueue_capture_cleanup(
        &self,
        lease: CaptureLease,
        reason: CaptureReleaseReason,
        primary: Option<String>,
    ) {
        {
            let mut state = self.state.lock().await;
            self.insert_capture_cleanup_locked(&mut state, &lease, reason, primary);
        }
    }

    fn spawn_pending_pane_cleanup(&self, key: CleanupTaskKey) {
        let CleanupTaskKey::PendingPane(_) = &key else {
            unreachable!("pending-pane cleanup spawned with a capture key");
        };
        let supervisor = Arc::clone(&self.cleanup);
        let _launch = supervisor
            .launch
            .lock()
            .expect("cleanup launch lock is not poisoned");
        if supervisor.has_live_task(&key) {
            supervisor.notify();
            return;
        }
        let claim = supervisor.next_claim();
        let adapter = Arc::clone(&self.adapter);
        let state = Arc::clone(&self.state);
        let task_supervisor = Arc::clone(&supervisor);
        let task_key = key.clone();
        let (start, started) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = started.await;
            run_pending_pane_cleanup(adapter, state, task_supervisor, task_key, claim).await;
        });
        supervisor
            .tasks
            .lock()
            .expect("cleanup tasks are not poisoned")
            .insert(key, CleanupTask { claim, handle });
        let _ = start.send(());
    }

    fn spawn_capture_cleanup(&self, key: CleanupTaskKey) {
        let CleanupTaskKey::Capture(_) = &key else {
            unreachable!("capture cleanup spawned with a pending-pane key");
        };
        let supervisor = Arc::clone(&self.cleanup);
        let _launch = supervisor
            .launch
            .lock()
            .expect("cleanup launch lock is not poisoned");
        if supervisor.has_live_task(&key) {
            supervisor.notify();
            return;
        }
        let claim = supervisor.next_claim();
        let adapter = Arc::clone(&self.adapter);
        let state = Arc::clone(&self.state);
        let task_supervisor = Arc::clone(&supervisor);
        let task_key = key.clone();
        let (start, started) = oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = started.await;
            run_capture_cleanup(adapter, state, task_supervisor, task_key, claim).await;
        });
        supervisor
            .tasks
            .lock()
            .expect("cleanup tasks are not poisoned")
            .insert(key, CleanupTask { claim, handle });
        let _ = start.send(());
    }

    fn new_session_id(&self) -> UiSessionId {
        UiSessionId::new(format!(
            "ui-{}",
            self.next_session.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn new_execution_id(counter: u64) -> ExecutionId {
        // Reserved-namespace proof: broker ids are a bare u64 counter and
        // stay below `LOCAL_EXECUTION_CEILING` (`1 << 63`); the Zellij
        // adapter mints at or above the ceiling and rejects the reserved
        // range at admission, so the spaces cannot collide.
        debug_assert!(
            counter < (1 << 63),
            "broker execution counter must stay below the adapter-reserved ceiling"
        );
        let mut bytes = [0; 16];
        bytes[8..].copy_from_slice(&counter.to_be_bytes());
        ExecutionId(bytes)
    }

    fn new_event_id(&self) -> EventId {
        new_event_id(&self.next_event)
    }
}

enum CleanupWaitOutcome {
    Gone,
    Wait(Duration),
}

enum CleanupOutcome {
    Missing,
    Retry(Duration),
}

#[expect(
    clippy::too_many_lines,
    reason = "pending-pane cleanup retry state machine keeps ownership and retry ordering auditable"
)]
async fn run_pending_pane_cleanup(
    adapter: Arc<dyn HostAdapter>,
    state: Arc<Mutex<BrokerState>>,
    supervisor: Arc<CleanupSupervisor>,
    key: CleanupTaskKey,
    claim: u64,
) {
    let CleanupTaskKey::PendingPane(lease_id) = key.clone() else {
        unreachable!("pending-pane cleanup task received a capture key");
    };
    loop {
        let payload = {
            let mut guard = state.lock().await;
            let Some(entry) = guard.cleanup.pending_panes.get_mut(&lease_id) else {
                drop(guard);
                if supervisor
                    .release_slot_unless_requeued(&state, &key, claim)
                    .await
                {
                    break;
                }
                continue;
            };
            if entry.phase == CleanupPhase::Ready && entry.next_retry <= Instant::now() {
                entry.phase = CleanupPhase::InFlight;
            }
            (entry.phase == CleanupPhase::InFlight).then(|| {
                (
                    entry.session.clone(),
                    entry.registration.clone(),
                    entry.primary.clone(),
                )
            })
        };
        let Some((session, registration, primary)) = payload else {
            let outcome = {
                let guard = state.lock().await;
                match guard.cleanup.pending_panes.get(&lease_id) {
                    None => CleanupWaitOutcome::Gone,
                    Some(entry) => CleanupWaitOutcome::Wait(
                        entry.next_retry.saturating_duration_since(Instant::now()),
                    ),
                }
            };
            let wait = match outcome {
                CleanupWaitOutcome::Gone => {
                    if supervisor
                        .release_slot_unless_requeued(&state, &key, claim)
                        .await
                    {
                        break;
                    }
                    continue;
                }
                CleanupWaitOutcome::Wait(wait) => wait,
            };
            let notified = supervisor.wake.notified();
            tokio::pin!(notified);
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = &mut notified => {}
            }
            continue;
        };
        let Some(lease) = registration.lease.clone() else {
            // A lease-less registration carries no host provenance: drop the
            // entry, then exit only when the terminal check confirms no
            // re-enqueue replaced it; otherwise loop to process the new entry.
            {
                let mut guard = state.lock().await;
                guard.cleanup.pending_panes.remove(&lease_id);
            }
            if supervisor
                .release_slot_unless_requeued(&state, &key, claim)
                .await
            {
                break;
            }
            continue;
        };
        let request = PendingPaneRegistration {
            ui_session: adapter_session(&session),
            pane: PaneId::new(registration.pane.as_str()),
            temporary_tab: registration
                .temporary_tab
                .as_ref()
                .map(|tab| TabId::new(tab.as_str())),
        };
        match adapter.close_pending_pane(request, lease).await {
            Ok(()) => {
                // Success removes the entry; exit only when the terminal
                // check (under the state lock) confirms nothing was
                // re-enqueued in the meantime, otherwise loop again.
                {
                    let mut guard = state.lock().await;
                    guard.cleanup.pending_panes.remove(&lease_id);
                }
                if supervisor
                    .release_slot_unless_requeued(&state, &key, claim)
                    .await
                {
                    break;
                }
            }
            Err(error) => {
                let error = bounded_adapter_error(error);
                let outcome = {
                    let mut guard = state.lock().await;
                    match guard.cleanup.pending_panes.get_mut(&lease_id) {
                        None => CleanupOutcome::Missing,
                        Some(entry) => {
                            entry.phase = CleanupPhase::Ready;
                            entry.attempt = entry.attempt.saturating_add(1);
                            entry.next_retry = Instant::now() + cleanup_retry_delay(entry.attempt);
                            entry.last_cleanup_error = Some(error.clone());
                            tracing::error!(
                                cleanup = "pending pane",
                                ?lease_id,
                                attempt = entry.attempt,
                                primary = ?primary,
                                error = %error,
                                "host cleanup failed; retaining provenance for deterministic retry"
                            );
                            CleanupOutcome::Retry(cleanup_retry_delay(entry.attempt))
                        }
                    }
                };
                let delay = match outcome {
                    CleanupOutcome::Missing => {
                        if supervisor
                            .release_slot_unless_requeued(&state, &key, claim)
                            .await
                        {
                            break;
                        }
                        continue;
                    }
                    CleanupOutcome::Retry(delay) => delay,
                };
                let notified = supervisor.wake.notified();
                tokio::pin!(notified);
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = &mut notified => {}
                }
            }
        }
    }
    supervisor.remove_if_claim(&key, claim);
}

#[expect(
    clippy::too_many_lines,
    reason = "capture cleanup retry state machine keeps ownership and retry ordering auditable"
)]
async fn run_capture_cleanup(
    adapter: Arc<dyn HostAdapter>,
    state: Arc<Mutex<BrokerState>>,
    supervisor: Arc<CleanupSupervisor>,
    key: CleanupTaskKey,
    claim: u64,
) {
    let CleanupTaskKey::Capture(lease_id) = key.clone() else {
        unreachable!("capture cleanup task received a pending-pane key");
    };
    loop {
        let payload = {
            let mut guard = state.lock().await;
            let Some(entry) = guard.cleanup.captures.get_mut(&lease_id) else {
                drop(guard);
                if supervisor
                    .release_slot_unless_requeued(&state, &key, claim)
                    .await
                {
                    break;
                }
                continue;
            };
            if entry.phase == CleanupPhase::Ready && entry.next_retry <= Instant::now() {
                entry.phase = CleanupPhase::InFlight;
            }
            (entry.phase == CleanupPhase::InFlight)
                .then(|| (entry.lease.clone(), entry.reason, entry.primary.clone()))
        };
        let Some((lease, reason, primary)) = payload else {
            let outcome = {
                let guard = state.lock().await;
                match guard.cleanup.captures.get(&lease_id) {
                    None => CleanupWaitOutcome::Gone,
                    Some(entry) => CleanupWaitOutcome::Wait(
                        entry.next_retry.saturating_duration_since(Instant::now()),
                    ),
                }
            };
            let wait = match outcome {
                CleanupWaitOutcome::Gone => {
                    if supervisor
                        .release_slot_unless_requeued(&state, &key, claim)
                        .await
                    {
                        break;
                    }
                    continue;
                }
                CleanupWaitOutcome::Wait(wait) => wait,
            };
            let notified = supervisor.wake.notified();
            tokio::pin!(notified);
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = &mut notified => {}
            }
            continue;
        };
        match adapter.end_capture(lease, reason).await {
            Ok(()) => {
                {
                    let mut guard = state.lock().await;
                    guard.cleanup.captures.remove(&lease_id);
                }
                if supervisor
                    .release_slot_unless_requeued(&state, &key, claim)
                    .await
                {
                    break;
                }
            }
            Err(error) => {
                let error = bounded_adapter_error(error);
                let outcome = {
                    let mut guard = state.lock().await;
                    match guard.cleanup.captures.get_mut(&lease_id) {
                        None => CleanupOutcome::Missing,
                        Some(entry) => {
                            entry.phase = CleanupPhase::Ready;
                            entry.attempt = entry.attempt.saturating_add(1);
                            entry.next_retry = Instant::now() + cleanup_retry_delay(entry.attempt);
                            entry.last_cleanup_error = Some(error.clone());
                            tracing::error!(
                                cleanup = "capture lease",
                                ?lease_id,
                                attempt = entry.attempt,
                                primary = ?primary,
                                error = %error,
                                "host cleanup failed; retaining provenance for deterministic retry"
                            );
                            CleanupOutcome::Retry(cleanup_retry_delay(entry.attempt))
                        }
                    }
                };
                let delay = match outcome {
                    CleanupOutcome::Missing => {
                        if supervisor
                            .release_slot_unless_requeued(&state, &key, claim)
                            .await
                        {
                            break;
                        }
                        continue;
                    }
                    CleanupOutcome::Retry(delay) => delay,
                };
                let notified = supervisor.wake.notified();
                tokio::pin!(notified);
                tokio::select! {
                    () = tokio::time::sleep(delay) => {}
                    () = &mut notified => {}
                }
            }
        }
    }
    supervisor.remove_if_claim(&key, claim);
}

fn adapter_session(session: &UiSessionId) -> muxe_adapter_api::UiSessionId {
    muxe_adapter_api::UiSessionId::new(session.as_str())
}
fn bounded_cleanup_message(mut message: String) -> String {
    muxe_protocol::truncate_utf8(&mut message, muxe_protocol::MAX_DIAGNOSTIC_LEN);
    message
}
fn bounded_adapter_error(mut error: AdapterError) -> AdapterError {
    muxe_protocol::truncate_utf8(&mut error.message, muxe_protocol::MAX_DIAGNOSTIC_LEN);
    error
}

fn diagnostic(code: DiagnosticCode, message: &str) -> ProtocolDiagnostic {
    let mut message = message.to_owned();
    muxe_protocol::truncate_utf8(&mut message, muxe_protocol::MAX_DIAGNOSTIC_LEN);
    ProtocolDiagnostic { code, message }
}

const GENERIC_CANCEL_GRACE: Duration = Duration::from_secs(2);

enum GenericWait {
    Exited(std::io::Result<std::process::ExitStatus>),
    Cancelled(GenericCancellation),
    TimedOut,
}

/// Owned inputs for one generic command-pane launch.
#[expect(
    dead_code,
    reason = "test-only direct generic-launch fixtures construct the public(crate) input with its full execution contract"
)]
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
    session: UiSessionId,
    core: CoreExecutionId,
    state: Arc<Mutex<BrokerState>>,
    sessions: Arc<Mutex<HashMap<UiSessionId, SessionRecord>>>,
    generic: Arc<GenericSupervisor>,
    next_event: Arc<AtomicU64>,
    diagnostics_tx: mpsc::UnboundedSender<BrokerDiagnostic>,
    broker: Weak<Broker>,
}

async fn supervise_generic_child(spec: GenericChildSpec) {
    let GenericChildSpec {
        mut child,
        process_group,
        mut cancellation,
        session,
        core,
        state,
        sessions,
        generic,
        next_event,
        diagnostics_tx,
        broker,
    } = spec;
    let handles = SupervisorHandles {
        state,
        sessions,
        generic,
        next_event,
        diagnostics_tx,
        broker,
    };
    match await_child_exit(&mut child, &mut cancellation, None).await {
        GenericWait::Exited(status) => {
            let (outcome, diagnostic) = generic_exit_outcome(status);
            finish_generic(&session, core, outcome, diagnostic, true, &handles).await;
        }
        GenericWait::Cancelled(reason) => {
            let completion = terminate_generic_child(&mut child, process_group).await;
            let (outcome, diagnostic) = match completion {
                Ok(_) if matches!(reason, GenericCancellation::Timeout) => {
                    (ExecutionOutcome::TimedOut, None)
                }
                Ok(_) => (ExecutionOutcome::Cancelled, None),
                Err(error) => (
                    ExecutionOutcome::Failed,
                    Some(diagnostic(DiagnosticCode::ActionBlocked, &error)),
                ),
            };
            finish_generic(&session, core, outcome, diagnostic, true, &handles).await;
        }
        GenericWait::TimedOut => unreachable!("generic timeout is owned by execution deadlines"),
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
    if let Some(reason) = cancellation.borrow().as_ref().copied() {
        return reason;
    }
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
    diagnostics_tx: mpsc::UnboundedSender<BrokerDiagnostic>,
    broker: Weak<Broker>,
}

struct GenericCompletionGuard {
    generic: Arc<GenericSupervisor>,
    core: CoreExecutionId,
    remove: bool,
}

impl Drop for GenericCompletionGuard {
    fn drop(&mut self) {
        if self.remove {
            self.generic
                .processes
                .lock()
                .expect("generic supervisor registry is not poisoned")
                .remove(&self.core);
        }
    }
}

async fn finish_generic(
    _session: &UiSessionId,
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
        diagnostics_tx,
        broker,
    } = handles;
    let _process_guard = GenericCompletionGuard {
        generic: Arc::clone(generic),
        core,
        remove: remove_process,
    };
    let record = {
        let mut state = state.lock().await;
        let Some(record) = state.executions.get_mut(&core) else {
            return;
        };
        if record.owner != ExecutionOwner::GenericProcess {
            return;
        }
        record.phase = ExecutionPhase::Terminal;
        let record = state
            .executions
            .remove(&core)
            .expect("generic owner exists");
        if record.awaiting
            && let Some(session) = &record.session
        {
            state.awaiting.remove(session);
        }
        record
    };
    if record.pending_control.is_some() {
        return;
    }
    if !record.awaiting {
        if let Some(diagnostic) = diagnostic {
            let _ = diagnostics_tx.send(BrokerDiagnostic {
                execution: record.wire,
                outcome,
                code: diagnostic.code,
            });
            tracing::error!(
                ?record.wire,
                execution = core.0,
                diagnostic = ?diagnostic,
                "detached generic execution did not complete successfully"
            );
        }
        return;
    }
    let Some(session) = record.session else {
        return;
    };
    deliver_session_event_to(
        sessions,
        &session,
        new_event_id(next_event),
        BrokerEvent::ExecutionCompleted {
            session: session.clone(),
            execution: record.wire,
            outcome,
            diagnostic,
        },
        "ExecutionCompleted",
        broker,
    )
    .await;
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

    fn named(name: &str) -> muxe_core::MenuId {
        muxe_core::MenuId::named(muxe_core::MenuName::parse(name).expect("fixture menu name"))
    }

    struct CountingAdapter {
        portable_dispatches: AtomicUsize,
        cancellable: AtomicBool,
        cancellations: AtomicUsize,
        ended_captures: AtomicUsize,
        dispatch_entered: Arc<Notify>,
        dispatch_release: Arc<Notify>,
        block_dispatch: AtomicBool,
        mismatch_post_dismissal: AtomicBool,
        fail_cancellation: AtomicBool,
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
        fn execution_capabilities(&self) -> muxe_core::ExecutionCapabilities {
            muxe_core::ExecutionCapabilities {
                cancellable: self.cancellable.load(Ordering::SeqCst),
                ..muxe_core::ExecutionCapabilities::ASYNCHRONOUS
            }
        }
    }
    fn counting_adapter(cancellable: bool) -> Arc<CountingAdapter> {
        Arc::new(CountingAdapter {
            portable_dispatches: AtomicUsize::new(0),
            cancellable: AtomicBool::new(cancellable),
            cancellations: AtomicUsize::new(0),
            ended_captures: AtomicUsize::new(0),
            dispatch_entered: Arc::new(Notify::new()),
            dispatch_release: Arc::new(Notify::new()),
            block_dispatch: AtomicBool::new(false),
            mismatch_post_dismissal: AtomicBool::new(false),
            fail_cancellation: AtomicBool::new(false),
        })
    }
    async fn dispatch_detached_adapter_execution(
        cancellable: bool,
    ) -> (Arc<CountingAdapter>, Arc<Broker>, CoreExecutionId) {
        let adapter = counting_adapter(cancellable);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<activation drain regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: detach
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("detached adapter configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("test binding is visible");
        let broker = Broker::from_compiled(
            adapter.clone(),
            PathBuf::from("<activation-drain-regression>"),
            config,
        );
        let (events, _events_rx) = mpsc::channel(1);
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
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
        assert!(matches!(
            broker
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
                .expect("fake adapter accepts detached dispatch"),
            RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                disposition: InvocationDisposition::Detached,
                ..
            })
        ));
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
        let core = CoreExecutionId(1);
        let state = broker.state.lock().await;
        let record = state
            .executions
            .get(&core)
            .expect("accepted noncompleting adapter dispatch remains owned");
        assert!(matches!(record.phase, ExecutionPhase::Adapter));
        assert!(record.session.is_none());
        drop(state);
        assert_eq!(adapter.portable_dispatches.load(Ordering::SeqCst), 1);
        (adapter, broker, core)
    }

    impl ActionValidator for CountingAdapter {
        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _action_span: &muxe_core::SourceSpan,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: self.execution_capabilities(),
            })
        }

        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
            Ok(vec![
                ActionValidation {
                    execution: self.execution_capabilities(),
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
                supports_capture: true,
                supports_notifications: false,
                supports_native_cancellation: self.cancellable.load(Ordering::SeqCst),
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
            _lease: CaptureLease,
            _reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            self.ended_captures.fetch_add(1, Ordering::SeqCst);
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

        fn release_pending_pane(&self, _lease: muxe_adapter_api::PendingPaneLease) {}

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
            if self.block_dispatch.load(Ordering::SeqCst) {
                self.dispatch_entered.notify_one();
                self.dispatch_release.notified().await;
            }
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("unexpected"),
                execution: request.execution,
                capabilities: self.execution_capabilities(),
            })
        }

        async fn dispatch_portable_after_ui_dismissal(
            &self,
            request: muxe_adapter_api::PostDismissalPortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            self.portable_dispatches.fetch_add(1, Ordering::SeqCst);
            let execution = if self.mismatch_post_dismissal.load(Ordering::SeqCst) {
                CoreExecutionId(request.execution.0 + 1000)
            } else {
                request.execution
            };
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("post-dismissal"),
                execution,
                capabilities: self.execution_capabilities(),
            })
        }

        async fn dispatch_native(
            &self,
            request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("native"),
                execution: request.execution,
                capabilities: self.execution_capabilities(),
            })
        }

        async fn cancel(&self, _execution: CoreExecutionId) -> Result<(), AdapterError> {
            self.cancellations.fetch_add(1, Ordering::SeqCst);
            if self.fail_cancellation.load(Ordering::SeqCst) {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "counting adapter cancellation failed",
                ));
            }
            if self.cancellable.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::CancelUnsupported,
                    "counting adapter dispatch is not cancellable",
                ))
            }
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
        cancellable: AtomicBool,
        block_cancel: AtomicBool,
        cancel_entered: Arc<Notify>,
        cancel_release: Arc<Notify>,
        capture_entered: Arc<Notify>,
        capture_release: Arc<Notify>,
        block_capture: AtomicBool,
        ended_captures: Mutex<Vec<(String, CaptureReleaseReason)>>,
        closed_panes: Mutex<Vec<(String, Option<String>)>>,
        close_entered: Arc<Notify>,
        close_release: Arc<Notify>,
        block_close: AtomicBool,
        fail_close_once: AtomicBool,
        fail_close_always: AtomicBool,
        block_end: AtomicBool,
        end_entered: Arc<Notify>,
        end_release: Arc<Notify>,
        fail_end_once: AtomicBool,
        fail_end_always: AtomicBool,
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
                execution: muxe_core::ExecutionCapabilities {
                    cancellable: self.cancellable.load(Ordering::SeqCst),
                    ..muxe_core::ExecutionCapabilities::ASYNCHRONOUS
                },
            })
        }

        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
            let cancellable = self.cancellable.load(Ordering::SeqCst);
            Ok(vec![
                ActionValidation {
                    execution: muxe_core::ExecutionCapabilities {
                        cancellable,
                        ..muxe_core::ExecutionCapabilities::ASYNCHRONOUS
                    },
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
            if self.block_capture.load(Ordering::SeqCst) {
                self.capture_entered.notify_one();
                self.capture_release.notified().await;
            }
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
            if self.block_end.load(Ordering::SeqCst) {
                self.end_entered.notify_one();
                self.end_release.notified().await;
            }
            if self.fail_end_always.load(Ordering::SeqCst)
                || self.fail_end_once.swap(false, Ordering::SeqCst)
            {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "injected capture cleanup failure",
                ));
            }
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
            if self.block_close.load(Ordering::SeqCst) {
                self.close_entered.notify_one();
                self.close_release.notified().await;
            }
            if self.fail_close_always.load(Ordering::SeqCst)
                || self.fail_close_once.swap(false, Ordering::SeqCst)
            {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "injected pane cleanup failure",
                ));
            }
            self.closed_panes.lock().await.push((
                registration.pane.as_str().to_owned(),
                registration
                    .temporary_tab
                    .as_ref()
                    .map(|tab| tab.as_str().to_owned()),
            ));
            Ok(())
        }

        fn release_pending_pane(&self, _lease: muxe_adapter_api::PendingPaneLease) {
            self.pending_releases.fetch_add(1, Ordering::SeqCst);
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
                capabilities: muxe_core::ExecutionCapabilities {
                    cancellable: self.cancellable.load(Ordering::SeqCst),
                    ..muxe_core::ExecutionCapabilities::ASYNCHRONOUS
                },
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
                capabilities: muxe_core::ExecutionCapabilities {
                    cancellable: self.cancellable.load(Ordering::SeqCst),
                    ..muxe_core::ExecutionCapabilities::ASYNCHRONOUS
                },
            })
        }

        async fn cancel(&self, _execution: CoreExecutionId) -> Result<(), AdapterError> {
            if self.block_cancel.load(Ordering::SeqCst) {
                self.cancel_entered.notify_one();
                self.cancel_release.notified().await;
            }
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
        let adapter = counting_adapter(false);
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
        let root = named("main");
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
                    root: muxe_protocol::MenuId::named("main"),
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
    async fn drain_refuses_detached_non_cancellable_adapter_execution() {
        let (adapter, broker, core) = dispatch_detached_adapter_execution(false).await;

        assert!(matches!(
            broker.drain_for_activation().await,
            Err(BrokerError::ActivationDrainRefused(message))
                if message.contains("detached host execution 1")
        ));
        assert_eq!(
            adapter.cancellations.load(Ordering::SeqCst),
            0,
            "drain must refuse before attempting unsupported cancellation"
        );

        let state = broker.state.lock().await;
        assert!(
            state.executions.contains_key(&core),
            "refused drain retains the detached host owner"
        );
        assert!(
            !state.activation_sealed,
            "a refused drain returns the broker to usable admission"
        );
    }
    #[tokio::test]
    async fn reserved_dispatch_rejects_a_second_valid_invoke_before_acceptance() {
        let adapter = counting_adapter(false);
        adapter.block_dispatch.store(true, Ordering::SeqCst);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<atomic execution admission regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: await
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let root = named("main");
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
        let (session, _events_rx) = attach_ready(&broker, "atomic-admission").await;
        let make_request = || {
            ClientRequest::InvokeBinding(InvokeBinding {
                session: session.clone(),
                generation: 1,
                binding: BindingId {
                    generation: binding.generation().0,
                    ordinal: binding.ordinal(),
                },
            })
        };
        let first_request = make_request();
        let second_request = make_request();

        let entered = adapter.dispatch_entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        let first_broker = Arc::clone(&broker);
        let first = tokio::spawn(async move {
            first_broker
                .handle(PeerRole::Ui, first_request, mpsc::channel(1).0)
                .await
        });
        entered.await;

        let second = broker
            .handle(PeerRole::Ui, second_request, mpsc::channel(1).0)
            .await;
        assert!(
            matches!(second, Err(BrokerError::PendingExecutionInFlight)),
            "a second awaited invocation cannot cross the reserved owner"
        );
        assert_eq!(
            adapter.portable_dispatches.load(Ordering::SeqCst),
            1,
            "the rejected second invocation never reaches the host"
        );

        adapter.block_dispatch.store(false, Ordering::SeqCst);
        adapter.dispatch_release.notify_waiters();
        assert!(matches!(
            first
                .await
                .expect("first invocation task joins")
                .expect("first invocation succeeds"),
            RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                disposition: InvocationDisposition::Awaited,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn activation_seal_during_reserved_dispatch_reaps_owner_and_reopens() {
        let adapter = counting_adapter(true);
        adapter.block_dispatch.store(true, Ordering::SeqCst);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<reserved activation seal regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: await
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let root = named("main");
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
        let (session, _events_rx) = attach_ready(&broker, "reserved-seal").await;
        let request = ClientRequest::InvokeBinding(InvokeBinding {
            session: session.clone(),
            generation: 1,
            binding: BindingId {
                generation: binding.generation().0,
                ordinal: binding.ordinal(),
            },
        });
        let invoking = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move {
                broker
                    .handle(PeerRole::Ui, request, mpsc::channel(1).0)
                    .await
            }
        });
        let entered = adapter.dispatch_entered.notified();
        tokio::pin!(entered);
        entered.as_mut().enable();
        entered.await;

        let draining = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move { broker.drain_for_activation().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if broker.state.lock().await.activation_sealed {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain seals while dispatch remains reserved");
        adapter.block_dispatch.store(false, Ordering::SeqCst);
        adapter.dispatch_release.notify_waiters();
        assert!(matches!(
            invoking.await.expect("reserved invocation task joins"),
            Err(BrokerError::ActivationInProgress)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.cancellations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sealed owner is cancelled after acceptance");
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(1),
            })
            .await;
        draining
            .await
            .expect("activation drain task joins")
            .expect("sealed owner reaches a terminal completion");
        assert!(
            broker.state.lock().await.activation_sealed,
            "successful drain remains sealed until the coordinator reopens it"
        );

        broker.reopen_dispatch().await;
        let (fresh_session, _events_rx) = attach_ready(&broker, "reserved-seal-fresh").await;
        let accepted = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::InvokeBinding(InvokeBinding {
                    session: fresh_session,
                    generation: 1,
                    binding: BindingId {
                        generation: binding.generation().0,
                        ordinal: binding.ordinal(),
                    },
                }),
                mpsc::channel(1).0,
            )
            .await;
        assert!(matches!(
            accepted.expect("reopened broker accepts a fresh dispatch"),
            RequestResult::Immediate(BrokerResponse::InvocationAccepted { .. })
        ));
        assert_eq!(
            adapter.portable_dispatches.load(Ordering::SeqCst),
            2,
            "reopened admission reaches the adapter"
        );
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(2),
            })
            .await;
    }

    #[tokio::test(start_paused = true)]
    async fn execution_deadlines_detach_or_cancel_once_and_ignore_late_old_completion() {
        for (on_timeout, expected_outcome, expected_cancellations) in [
            ("cancel", ExecutionOutcome::TimedOut, 1),
            ("detach", ExecutionOutcome::Detached, 0),
        ] {
            let adapter = counting_adapter(true);
            let yaml = format!(
                r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: await
            timeout: 10ms
            on-timeout: {on_timeout}
            on-menu-control: cancel
"
            );
            let config = muxe_core::compile_yaml(
                CompiledGeneration(1),
                SourceId::new("<execution deadline regression>"),
                yaml.as_str(),
                KeyCapabilities::default(),
                Some(adapter.as_ref()),
            )
            .expect("deadline configuration compiles");
            let root = named("main");
            let binding = config
                .attachment_view(&root)
                .and_then(|view| {
                    view.menu
                        .menu(&root)
                        .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
                })
                .expect("deadline binding is visible");
            let directory = tempfile::tempdir().expect("deadline test directory");
            let broker =
                Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
            let (session, mut events_rx) = attach_ready(&broker, "deadline").await;
            let invoke = |session: UiSessionId| {
                ClientRequest::InvokeBinding(InvokeBinding {
                    session,
                    generation: 1,
                    binding: BindingId {
                        generation: binding.generation().0,
                        ordinal: binding.ordinal(),
                    },
                })
            };
            let accepted = broker
                .handle(PeerRole::Ui, invoke(session.clone()), mpsc::channel(1).0)
                .await
                .expect("deadline invocation is accepted");
            let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                execution: first_execution,
                disposition: InvocationDisposition::Awaited,
            }) = accepted
            else {
                panic!("deadline invocation remains awaited");
            };

            tokio::time::advance(Duration::from_millis(10)).await;
            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            let Some(WireMessage::Event {
                event:
                    BrokerEvent::ExecutionCompleted {
                        execution, outcome, ..
                    },
                ..
            }) = events_rx.recv().await
            else {
                panic!("deadline emits one completion event");
            };
            assert_eq!(execution, first_execution);
            assert_eq!(outcome, expected_outcome);
            for _ in 0..3 {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                adapter.cancellations.load(Ordering::SeqCst),
                expected_cancellations,
                "timeout policy requests exactly its configured host cancellation"
            );

            let accepted = broker
                .handle(PeerRole::Ui, invoke(session), mpsc::channel(1).0)
                .await
                .expect("the timed-out UI can start a newer execution");
            let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                execution: second_execution,
                disposition: InvocationDisposition::Awaited,
            }) = accepted
            else {
                panic!("new execution remains awaited");
            };
            assert_ne!(
                first_execution, second_execution,
                "late completion identities must not be reused"
            );

            broker
                .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                    execution: CoreExecutionId(1),
                })
                .await;
            {
                let state = broker.state.lock().await;
                assert!(
                    state.executions.contains_key(&CoreExecutionId(2)),
                    "late completion of the timed-out owner leaves the newer owner intact"
                );
            }
            broker
                .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                    execution: CoreExecutionId(2),
                })
                .await;
            let Some(WireMessage::Event {
                event:
                    BrokerEvent::ExecutionCompleted {
                        execution,
                        outcome: ExecutionOutcome::Succeeded,
                        ..
                    },
                ..
            }) = events_rx.recv().await
            else {
                panic!("new execution emits its own completion event");
            };
            assert_eq!(execution, second_execution);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn full_ui_event_queue_does_not_delay_timeout_cancellation_or_owner_release() {
        let adapter = counting_adapter(true);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<full timeout event queue regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: await
            timeout: 10ms
            on-timeout: cancel
            on-menu-control: cancel
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("full-queue timeout configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("full-queue binding is visible");
        let directory = tempfile::tempdir().expect("full-queue test directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        let (events, mut events_rx) = mpsc::channel(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("full-queue"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events.clone(),
            )
            .await
            .expect("full-queue UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected full-queue attachment");
        };
        events
            .try_send(WireMessage::Event {
                event_id: EventId([9; 16]),
                event: BrokerEvent::AdapterHealthChanged {
                    healthy: true,
                    diagnostic: None,
                },
            })
            .expect("test fills the bounded UI event queue");
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
            .expect("full-queue invocation is accepted");
        let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
            execution: _first_execution,
            disposition: InvocationDisposition::Awaited,
        }) = accepted
        else {
            panic!("full-queue invocation remains awaited");
        };
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        // The blocked timeout delivery is now non-blocking: the owner-side
        // stop still crosses the adapter, but the full queue is an explicit
        // slow-consumer failure, so the session is torn down on its own task
        // instead of receiving the timed-out completion after the drain.
        assert_eq!(
            adapter.cancellations.load(Ordering::SeqCst),
            1,
            "timeout cancellation crosses the adapter while UI delivery is blocked"
        );
        {
            let state = broker.state.lock().await;
            let record = state
                .executions
                .get(&CoreExecutionId(1))
                .expect("timed-out owner remains until its late terminal");
            assert!(matches!(record.owner, ExecutionOwner::Adapter));
            assert!(record.termination_requested);
            assert!(!record.awaiting);
        }
        // The pre-filled event is the only item the dead queue ever held; its
        // drain proves the teardown ran, because a slow-consumer detach leaves
        // a disconnected queue while the test still holds the only receiver.
        let Some(WireMessage::Event {
            event: BrokerEvent::AdapterHealthChanged { .. },
            ..
        }) = events_rx.recv().await
        else {
            panic!("expected initial health event");
        };
        // Paused clock: poll with yields (no timers) until the spawned
        // teardown detaches the session.
        for _ in 0..1000 {
            if !broker.sessions.lock().await.contains_key(&session) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            !broker.sessions.lock().await.contains_key(&session),
            "slow-consumer teardown detaches the full session"
        );
        assert!(
            broker
                .state
                .lock()
                .await
                .executions
                .get(&CoreExecutionId(1))
                .is_none_or(|record| record.session.is_none()),
            "torn-down session owns no in-flight execution UI"
        );
        // The detached session can no longer admit work: detach is effectively
        // once per session and the record is gone, so the slot is not reused.
        let stale = broker
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
            .await;
        assert!(
            matches!(stale, Err(BrokerError::UnknownSession(_))),
            "detached slow consumer admits no further invocations"
        );
        // The teardown ends the session capture exactly once through the
        // supervised cleanup path.
        for _ in 0..1000 {
            if adapter.ended_captures.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            adapter.ended_captures.load(Ordering::SeqCst),
            1,
            "slow-consumer teardown ends the session capture exactly once"
        );
        // The timed-out session is gone, so the newer owner lives in a second
        // session: the released owner slot must still admit fresh work.
        let (second_events, mut second_rx) = mpsc::channel(8);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("full-queue-second"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                second_events.clone(),
            )
            .await
            .expect("second UI attaches after the slow consumer is gone");
        let RequestResult::Immediate(BrokerResponse::UiAttached {
            session: second_session,
            ..
        }) = attached
        else {
            panic!("expected second attachment");
        };
        let accepted = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::InvokeBinding(InvokeBinding {
                    session: second_session.clone(),
                    generation: 1,
                    binding: BindingId {
                        generation: binding.generation().0,
                        ordinal: binding.ordinal(),
                    },
                }),
                second_events.clone(),
            )
            .await
            .expect("released owner slot admits a newer owner");
        let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
            execution: second_execution,
            disposition: InvocationDisposition::Awaited,
        }) = accepted
        else {
            panic!("new owner remains awaited");
        };
        // A late completion for the torn-down owner cannot remove the newer
        // owner: the terminal arrived after the old session's removal.
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(1),
            })
            .await;
        assert!(
            broker
                .state
                .lock()
                .await
                .executions
                .contains_key(&CoreExecutionId(2)),
            "late completion cannot remove the newer owner"
        );
        assert!(
            !broker
                .state
                .lock()
                .await
                .executions
                .contains_key(&CoreExecutionId(1)),
            "late completion of the torn-down owner leaves no execution behind"
        );
        // The healthy second session still receives its own terminal through
        // the same delivery path that dropped the slow consumer.
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(2),
            })
            .await;
        let Some(WireMessage::Event {
            event:
                BrokerEvent::ExecutionCompleted {
                    execution: succeeded_execution,
                    outcome: ExecutionOutcome::Succeeded,
                    ..
                },
            ..
        }) = second_rx.recv().await
        else {
            panic!("expected second execution succeeded completion");
        };
        assert_eq!(succeeded_execution, second_execution);
        assert!(
            events_rx.try_recv().is_err(),
            "slow-consumer teardown delivers no timed-out completion"
        );
        assert_eq!(
            adapter.ended_captures.load(Ordering::SeqCst),
            1,
            "no second teardown ends another capture"
        );
    }

    #[tokio::test]
    async fn slow_ui_does_not_block_unrelated_session_completion() {
        let adapter = counting_adapter(true);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<slow consumer isolation regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: await
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("isolation configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("isolation binding is visible");
        let directory = tempfile::tempdir().expect("isolation test directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        let invoke = |session: UiSessionId| {
            ClientRequest::InvokeBinding(InvokeBinding {
                session,
                generation: 1,
                binding: BindingId {
                    generation: binding.generation().0,
                    ordinal: binding.ordinal(),
                },
            })
        };
        // The slow session's outbox (capacity 1) is filled and never drained.
        let (slow_events, _slow_rx) = mpsc::channel::<WireMessage>(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("slow-ui"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                slow_events.clone(),
            )
            .await
            .expect("slow UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached {
            session: slow_session,
            ..
        }) = attached
        else {
            panic!("expected slow attachment");
        };
        slow_events
            .try_send(WireMessage::Event {
                event_id: EventId([7; 16]),
                event: BrokerEvent::AdapterHealthChanged {
                    healthy: true,
                    diagnostic: None,
                },
            })
            .expect("test fills the slow UI queue");
        // A healthy session shares the same adapter monitor.
        let (fast_events, mut fast_rx) = mpsc::channel::<WireMessage>(8);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("fast-ui"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                fast_events.clone(),
            )
            .await
            .expect("healthy UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached {
            session: fast_session,
            ..
        }) = attached
        else {
            panic!("expected healthy attachment");
        };
        let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
            execution: slow_execution,
            disposition: InvocationDisposition::Awaited,
        }) = broker
            .handle(
                PeerRole::Ui,
                invoke(slow_session.clone()),
                slow_events.clone(),
            )
            .await
            .expect("slow invocation is accepted")
        else {
            panic!("slow invocation remains awaited");
        };
        let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
            execution: fast_execution,
            disposition: InvocationDisposition::Awaited,
        }) = broker
            .handle(
                PeerRole::Ui,
                invoke(fast_session.clone()),
                fast_events.clone(),
            )
            .await
            .expect("healthy invocation is accepted")
        else {
            panic!("healthy invocation remains awaited");
        };
        assert_ne!(slow_execution, fast_execution);
        // One unhealthy broadcast must not wedge the healthy session: the slow
        // queue is torn down while every other session still gets its event.
        broker.broadcast_health(false, None).await;
        let Some(WireMessage::Event {
            event:
                BrokerEvent::AdapterHealthChanged {
                    healthy: fast_healthy,
                    ..
                },
            ..
        }) = fast_rx.recv().await
        else {
            panic!("healthy session observes the broadcast");
        };
        assert!(
            !fast_healthy,
            "healthy session observes the unhealthy broadcast"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !broker.sessions.lock().await.contains_key(&slow_session) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("slow consumer is torn down exactly once");
        // The other session's execution still completes through the same
        // monitor that just dropped the slow consumer.
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(2),
            })
            .await;
        let Some(WireMessage::Event {
            event:
                BrokerEvent::ExecutionCompleted {
                    execution: completed,
                    outcome: ExecutionOutcome::Succeeded,
                    ..
                },
            ..
        }) = fast_rx.recv().await
        else {
            panic!("healthy session receives its completion");
        };
        assert_eq!(completed, fast_execution);
    }

    #[tokio::test]
    async fn slow_ui_completion_teardown_runs_once_and_delivers_nothing_further() {
        let adapter = counting_adapter(true);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<slow consumer teardown regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      f:
        label: focus tab
        action: tab:focus index=1
        settings:
          execution:
            mode: await
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("teardown configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("teardown binding is visible");
        let directory = tempfile::tempdir().expect("teardown test directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        let (events, mut events_rx) = mpsc::channel::<WireMessage>(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("slow-teardown"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events.clone(),
            )
            .await
            .expect("slow UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected slow attachment");
        };
        events
            .try_send(WireMessage::Event {
                event_id: EventId([7; 16]),
                event: BrokerEvent::AdapterHealthChanged {
                    healthy: true,
                    diagnostic: None,
                },
            })
            .expect("test fills the slow UI queue");
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
            .expect("slow invocation is accepted");
        let RequestResult::Immediate(BrokerResponse::InvocationAccepted {
            disposition: InvocationDisposition::Awaited,
            ..
        }) = accepted
        else {
            panic!("slow invocation remains awaited");
        };
        // The full queue is a slow-consumer failure: delivering this terminal
        // must tear the session down instead of queueing behind the filler.
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(1),
            })
            .await;
        // The filler is the only event the dead queue ever held; draining it
        // proves the teardown ran while this test still holds the receiver.
        let Some(WireMessage::Event {
            event: BrokerEvent::AdapterHealthChanged { .. },
            ..
        }) = events_rx.recv().await
        else {
            panic!("expected filler health event");
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !broker.sessions.lock().await.contains_key(&session) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("slow consumer is detached");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if adapter.ended_captures.load(Ordering::SeqCst) == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("slow-consumer teardown ends the session capture");
        assert_eq!(
            adapter.ended_captures.load(Ordering::SeqCst),
            1,
            "slow-consumer teardown ends the session capture exactly once"
        );
        // No further events are attempted for the torn-down session: a second
        // terminal for the same execution finds no owner and no UI to notify.
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded {
                execution: CoreExecutionId(1),
            })
            .await;
        assert!(
            !broker.sessions.lock().await.contains_key(&session),
            "repeat delivery attempts no second teardown"
        );
        assert!(
            events_rx.try_recv().is_err(),
            "torn-down session receives no completion"
        );
        assert_eq!(
            adapter.ended_captures.load(Ordering::SeqCst),
            1,
            "repeat delivery attempts no second capture end"
        );
        assert!(
            !broker
                .state
                .lock()
                .await
                .executions
                .contains_key(&CoreExecutionId(1)),
            "terminal removes the torn-down execution exactly once"
        );
    }

    #[tokio::test]
    async fn slow_ui_generic_reaper_still_leaves_supervision() {
        let adapter = counting_adapter(false);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<slow generic reaper regression>"),
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
        .expect("reaper configuration compiles");
        let directory = tempfile::tempdir().expect("owned command cwd");
        let broker = Broker::from_compiled(adapter, directory.path().join("config.yml"), config);
        // Awed execution whose queue (capacity 1) is filled before the child
        // exits: the reaper's terminal delivery must not block on it.
        let (events, _events_rx) = mpsc::channel::<WireMessage>(1);
        let attached = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("slow-generic"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events.clone(),
            )
            .await
            .expect("slow UI attaches");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected slow attachment");
        };
        events
            .try_send(WireMessage::Event {
                event_id: EventId([7; 16]),
                event: BrokerEvent::AdapterHealthChanged {
                    healthy: true,
                    diagnostic: None,
                },
            })
            .expect("test fills the slow UI queue");
        let wire = ExecutionId([3; 16]);
        let core = CoreExecutionId(3);
        broker
            .reserve_execution(
                session.clone(),
                wire,
                core,
                ExecutionOwner::GenericProcess,
                None,
                &muxe_core::ExecutionPolicy {
                    mode: muxe_core::ExecutionMode::Await,
                    timeout: None,
                    on_timeout: TimeoutAction::Detach,
                    on_menu_control: muxe_core::MenuControlAction::Detach,
                },
            )
            .await
            .expect("slow generic execution reserves");
        let mut origin = CountingAdapter::origin_without_cwd();
        origin.pane_cwd = Some(directory.path().to_path_buf());
        broker
            .execute_command(CommandLaunch {
                session: session.clone(),
                wire,
                core,
                command: CommandAction {
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
                    mode: muxe_core::ExecutionMode::Await,
                    timeout: None,
                    on_timeout: TimeoutAction::Detach,
                    on_menu_control: muxe_core::MenuControlAction::Detach,
                },
            })
            .await
            .expect("slow generic child starts");
        assert!(
            broker.has_supervised_children().await,
            "slow generic child is supervised while it runs"
        );
        // The reaper must finish even though the UI never reads: the child
        // leaves supervision (so the activation linger cannot wait on it) and
        // the wedged session is torn down instead of blocking the reaper.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !broker.has_supervised_children().await {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("slow-UI reaper leaves supervision");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !broker.sessions.lock().await.contains_key(&session) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("slow consumer is detached by its reaper");
    }

    #[tokio::test]
    async fn drain_waits_for_detached_cancellable_adapter_completion() {
        let (adapter, broker, core) = dispatch_detached_adapter_execution(true).await;
        let draining = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move { broker.drain_for_activation().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.cancellations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain requests cancellation from the owned fake adapter");
        let mut draining = draining;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut draining)
                .await
                .is_err(),
            "drain must wait for the exact detached adapter terminal transition"
        );

        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded { execution: core })
            .await;
        draining
            .await
            .expect("drain task completes")
            .expect("completion settles the detached adapter owner");
        assert!(
            !broker.state.lock().await.executions.contains_key(&core),
            "terminal completion releases the adapter owner"
        );
    }

    #[tokio::test]
    async fn drain_retries_cancellation_after_adapter_cancel_failure() {
        let (adapter, broker, core) = dispatch_detached_adapter_execution(true).await;
        assert_eq!(adapter.cancellations.load(Ordering::SeqCst), 0);
        adapter.fail_cancellation.store(true, Ordering::SeqCst);
        let first_drain_result = broker.drain_for_activation().await;
        assert!(
            first_drain_result.is_err(),
            "first drain must fail when adapter cancellation fails"
        );
        assert_eq!(
            adapter.cancellations.load(Ordering::SeqCst),
            1,
            "first drain must have attempted cancellation once"
        );
        assert!(
            !broker.state.lock().await.activation_sealed,
            "failed drain clears the activation seal"
        );

        broker.reopen_dispatch().await;
        adapter.fail_cancellation.store(false, Ordering::SeqCst);

        let draining = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move { broker.drain_for_activation().await }
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.cancellations.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second drain retries adapter cancellation");

        let mut draining = draining;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut draining)
                .await
                .is_err(),
            "drain must wait for the exact detached adapter terminal transition"
        );

        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded { execution: core })
            .await;
        draining.await.expect("second drain task joins").expect(
            "second drain completes successfully after retried cancellation and completion",
        );

        assert!(
            !broker.state.lock().await.executions.contains_key(&core),
            "terminal completion releases the adapter owner"
        );
    }

    #[tokio::test]
    async fn post_dismissal_mismatch_does_not_strand_activation_drain() {
        let adapter = counting_adapter(true);
        adapter
            .mismatch_post_dismissal
            .store(true, Ordering::SeqCst);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<post dismissal mismatch regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      s:
        label: split pane
        action: pane:split
        settings:
          execution:
            mode: await
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("config compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("binding is visible");
        let directory = tempfile::tempdir().expect("test directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        let (session, _events_rx) = attach_ready(&broker, "post-dismissal").await;
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
                mpsc::channel(1).0,
            )
            .await
            .expect("invoke binding accepted");
        assert!(matches!(
            accepted,
            RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                disposition: InvocationDisposition::Dismissed,
                ..
            })
        ));

        // Detach UI triggers schedule_post_dismissal which encounters the execution mismatch.
        broker
            .detach(&session, CaptureReleaseReason::UiDismissed)
            .await
            .expect("detach UI succeeds");

        // The expected owner was transitioned out of Reserved to Adapter phase despite the mismatch.
        let core = CoreExecutionId(1);
        {
            let state = broker.state.lock().await;
            let record = state.executions.get(&core).expect("record exists");
            assert_eq!(record.phase, ExecutionPhase::Adapter);
        }

        // Activation drain must not hang or be stranded:
        let draining = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move { broker.drain_for_activation().await }
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.cancellations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drain requests cancellation for the transitioned adapter owner");

        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded { execution: core })
            .await;

        tokio::time::timeout(Duration::from_secs(1), draining)
            .await
            .expect("drain completes within bound and is not stranded")
            .expect("drain task joins")
            .expect("drain succeeds");
    }

    #[tokio::test]
    async fn detached_failure_reaches_payload_safe_persistent_diagnostic_sink() {
        let (_adapter, broker, core) = dispatch_detached_adapter_execution(false).await;
        let mut diagnostics = broker
            .take_diagnostics()
            .await
            .expect("composition root claims the one diagnostic sink");
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Failed {
                execution: core,
                error: AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::DispatchFailed,
                    "host payload sentinel: secret request body",
                ),
            })
            .await;

        let diagnostic = tokio::time::timeout(Duration::from_secs(1), diagnostics.recv())
            .await
            .expect("detached failure is persisted")
            .expect("diagnostic sender remains live");
        assert_eq!(diagnostic.execution, Broker::new_execution_id(core.0));
        assert_eq!(diagnostic.outcome, ExecutionOutcome::Failed);
        assert_eq!(diagnostic.code, DiagnosticCode::ActionBlocked);
        assert!(
            !format!("{diagnostic:?}").contains("secret request body"),
            "persistent diagnostic records never include host-provided payload"
        );
    }

    #[tokio::test]
    async fn focused_creation_arms_only_after_ui_detach() {
        let adapter = counting_adapter(false);
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
        let root = named("main");
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
                    root: muxe_protocol::MenuId::named("main"),
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
    async fn deferred_creation_remains_owned_through_activation_drain() {
        let adapter = counting_adapter(true);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<deferred activation drain regression>"),
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
        .expect("deferred configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("deferred binding is visible");
        let directory = tempfile::tempdir().expect("deferred test directory");
        let broker =
            Broker::from_compiled(adapter.clone(), directory.path().join("config.yml"), config);
        let (session, _events_rx) = attach_ready(&broker, "deferred-drain").await;
        assert!(matches!(
            broker
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
                    mpsc::channel(1).0,
                )
                .await
                .expect("deferred invocation is accepted"),
            RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                disposition: InvocationDisposition::Dismissed,
                ..
            })
        ));
        let core = CoreExecutionId(1);
        {
            let state = broker.state.lock().await;
            assert!(
                state
                    .executions
                    .get(&core)
                    .is_some_and(|record| record.deferred.is_some()),
                "deferred ownership is registered before UI dismissal can schedule it"
            );
        }

        let draining = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move { broker.drain_for_activation().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.cancellations.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("activation drain cancels the deferred adapter owner");
        let mut draining = draining;
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut draining)
                .await
                .is_err(),
            "drain waits for the deferred owner terminal transition"
        );
        broker
            .dispatch_completed(muxe_adapter_api::DispatchCompletion::Succeeded { execution: core })
            .await;
        draining
            .await
            .expect("deferred drain task joins")
            .expect("deferred owner terminal completion finishes drain");
        let state = broker.state.lock().await;
        assert!(
            !state.executions.contains_key(&core),
            "deferred owner is released exactly at its terminal completion"
        );
        assert!(
            state.activation_sealed,
            "successful drain seals before handoff"
        );
    }

    #[tokio::test]
    async fn awaited_config_reload_emits_a_terminal_completion() {
        let adapter = counting_adapter(false);
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
        let root = named("main");
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
                    root: muxe_protocol::MenuId::named("main"),
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
        let adapter = counting_adapter(false);
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
                    root: muxe_protocol::MenuId::named("main"),
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
                root: muxe_protocol::MenuId::named("main"),
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

    #[test]
    fn whitespace_compiled_view_passes_wire_validation_and_round_trips() {
        use muxe_core::{CompiledGeneration, Compiler, SourceId, ThemeAssets};
        let base = muxe_core::ConfigDocument::parse(
            SourceId::new("whitespace.yml"),
            "version: 1\nmenus:\n  \"my menu\":\n    bindings:\n      x:\n        label: tools\n        action:\n          type: menu:open\n          submenu:\n            bindings:\n              q:\n                label: quit\n                action: menu:quit\n",
        )
        .expect("whitespace config parses");
        let config = Compiler
            .compile(
                muxe_core::CompileInput {
                    generation: CompiledGeneration(9),
                    base,
                    host_override: None,
                    key_capabilities: KeyCapabilities::default(),
                    theme_assets: ThemeAssets::default(),
                },
                None,
            )
            .expect("quoted whitespace menu name compiles");
        let root = named("my menu");
        let view = config.attachment_view(&root).expect("whitespace root view");
        assert_eq!(view.menu.root, root);
        // Core -> wire preserves the variant; the owned view validates
        // (whitespace names are in the wire domain, matching core).
        let wire_view = wire::attachment(&view);
        muxe_protocol::Validate::validate(&wire_view)
            .expect("compiled whitespace view passes wire validation");
        // Wire -> core round-trips both identities losslessly by variant.
        let back_root = wire_menu_id_to_core(&wire_view.menu.root).expect("root converts back");
        assert_eq!(back_root, root);
        let inline = view
            .menu
            .menus
            .iter()
            .find_map(|menu| menu.id.inline_id().cloned())
            .expect("compiled inline submenu");
        let wire_inline = core_menu_id_to_wire(&muxe_core::MenuId::inline(inline.clone()));
        let back_inline = wire_menu_id_to_core(&wire_inline).expect("inline converts back");
        assert_eq!(back_inline, muxe_core::MenuId::inline(inline));
        assert_ne!(back_inline, named("my menu#0"));
    }

    #[tokio::test]
    async fn prepare_rejects_unknown_menu_before_launching_a_pane() {
        let (_adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let result = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("unknown-root"),
                root: muxe_protocol::MenuId::named("missing"),
                lease_millis: 60_000,
            })
            .await;
        assert!(matches!(
            result,
            Err(BrokerError::UnknownMenu(menu)) if menu.display() == "missing"
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
                    root: muxe_protocol::MenuId::named("main"),
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
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected immediate gated attachment");
        };
        let state = broker.state.lock().await;
        assert_eq!(adapter.pending_releases.load(Ordering::SeqCst), 0);
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
        drop(state);
        broker.confirm_gated_attachment(token, &session).await;
        assert_eq!(adapter.pending_releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn attach_before_register_rejects_wrong_pane_then_completes_exact_pane() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("attach-first"),
                root: muxe_protocol::MenuId::named("main"),
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
                        root: muxe_protocol::MenuId::named("main"),
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
        let pending_token = pending_attachment.token();
        let BrokerResponse::UiAttached { session, .. } = pending_attachment.wait().await else {
            panic!("expected gated attachment readiness");
        };
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
        assert_eq!(adapter.pending_releases.load(Ordering::SeqCst), 0);
        drop(state);
        broker
            .confirm_gated_attachment(pending_token, &session)
            .await;
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
                        root: muxe_protocol::MenuId::named("main"),
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
    async fn gated_disconnect_after_commit_detaches_session_instead_of_aborting_token() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "commit-disconnect", Some("temporary-tab")).await;
        // Drive the gated attach through the blocked origin barrier so the
        // test observes the same (pending token, session) association the
        // connection layer tracks while the launch is unpublished.
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::named("main"),
                        pane: HostPaneId::new("commit-disconnect"),
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
        let pending_session = {
            let state = broker.state.lock().await;
            state
                .pending_sessions
                .get(&token)
                .cloned()
                .expect("blocked attach owns a pending session")
        };
        // A launcher commit racing the blocked attach consumes the token the
        // same way a hostile commit/disconnect interleave would; the
        // disconnect below must then take the attached-session branch, not
        // abort the already-consumed token.
        broker
            .commit(token, HostPaneId::new("commit-disconnect"))
            .await
            .expect("placement commit records while origin capture is blocked");
        adapter.origin_release.notify_one();
        let attached = attach.await.expect("attach task completes");
        let RequestResult::Immediate(BrokerResponse::UiAttached { session, .. }) = attached else {
            panic!("expected immediate attachment after commit");
        };
        assert_eq!(session, pending_session);
        assert_eq!(adapter.pending_releases.load(Ordering::SeqCst), 0);
        broker
            .disconnect_gated(token, UiSessionId::new("stale-session"))
            .await;
        assert!(
            broker.sessions.lock().await.contains_key(&session),
            "mismatched consumed token/session does not detach the live session"
        );
        // The exact H06 failure was `abort(consumed token)` here: it errored
        // on the unknown token and skipped session cleanup. The broker-atomic
        // classification must detach the published session instead.
        broker.disconnect_gated(token, session.clone()).await;
        assert!(
            broker.sessions.lock().await.get(&session).is_none(),
            "committed session is detached exactly once"
        );
        assert_eq!(
            adapter.pending_releases.load(Ordering::SeqCst),
            0,
            "disconnect claims unconfirmed pane ownership instead of releasing it"
        );
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(
            state
                .gate
                .owner(&muxe_protocol::ModalScopeId::new("commit-disconnect"))
                .is_none(),
            "committed scope is released"
        );
        drop(state);
        wait_for_ended_capture(&adapter, session.as_str()).await;
        let ended = adapter.ended_captures.lock().await;
        assert_eq!(ended.len(), 1, "exactly one capture ends for the session");
        assert_eq!(ended[0].0, session.as_str());
        assert_eq!(ended[0].1, CaptureReleaseReason::UiDismissed);
    }

    #[tokio::test]
    async fn pending_disconnect_before_commit_aborts_token_and_closes_pane() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "pending-disconnect", Some("temporary-tab")).await;
        adapter.block_origin.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            let (events, _events_rx) = mpsc::channel(1);
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::named("main"),
                        pane: HostPaneId::new("pending-disconnect"),
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
        let pending_session = {
            broker
                .state
                .lock()
                .await
                .pending_sessions
                .get(&token)
                .cloned()
                .expect("blocked attach owns a pending session")
        };
        // A mismatched pending pair is stale and must not abort the launch.
        broker
            .disconnect_gated(token, UiSessionId::new("stale-session"))
            .await;
        assert!(broker.state.lock().await.gate.pending(token).is_some());
        // Disconnect while the token is still pending must take the token
        // branch: abort the launch and close the registered pane.
        broker
            .disconnect_gated(token, pending_session.clone())
            .await;
        adapter.origin_release.notify_one();
        assert!(attach.await.expect("blocked attach completes").is_err());
        let state = broker.state.lock().await;
        assert!(state.gate.pending(token).is_none());
        assert!(!state.pending_sessions.contains_key(&token));
        drop(state);
        assert!(broker.sessions.lock().await.is_empty());
        wait_for_closed_pane(&adapter, "pending-disconnect").await;
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![(
                "pending-disconnect".to_owned(),
                Some("temporary-tab".to_owned())
            )]
        );
    }

    #[tokio::test]
    async fn blocked_capture_detach_ends_acquired_lease_exactly_once() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let (events, _events_rx) = mpsc::channel(1);
        // An unscoped attach inserts its session record before capture starts;
        // a detach racing the blocked begin_capture removes it first, so the
        // newly acquired lease must be ended exactly once on install miss.
        adapter.block_capture.store(true, Ordering::SeqCst);
        let attach_broker = Arc::clone(&broker);
        let attach = tokio::spawn(async move {
            attach_broker
                .handle(
                    PeerRole::Ui,
                    ClientRequest::AttachUi(AttachUi {
                        root: muxe_protocol::MenuId::named("main"),
                        pane: HostPaneId::new("capture-race"),
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
        adapter.capture_entered.notified().await;
        let racing_session = {
            broker
                .sessions
                .lock()
                .await
                .keys()
                .next()
                .cloned()
                .expect("blocked capture owns a session record")
        };
        broker
            .detach(&racing_session, CaptureReleaseReason::UiDismissed)
            .await
            .expect("racing detach wins while capture is blocked");
        adapter.capture_release.notify_one();
        let result = attach.await.expect("blocked attach completes");
        assert!(
            matches!(result, Err(BrokerError::UnknownSession(_))),
            "install-miss attach reports the detached session"
        );
        wait_for_ended_capture(&adapter, racing_session.as_str()).await;
        let ended = adapter.ended_captures.lock().await;
        assert_eq!(ended.len(), 1, "the orphaned lease ends exactly once");
        assert!(broker.sessions.lock().await.is_empty());
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
                        root: muxe_protocol::MenuId::named("main"),
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
                        root: muxe_protocol::MenuId::named("main"),
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
                root: muxe_protocol::MenuId::named("main"),
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
                        root: muxe_protocol::MenuId::named("main"),
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
        wait_for_closed_pane(&adapter, "abort-blocked").await;
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
                root: muxe_protocol::MenuId::named("main"),
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
                        root: muxe_protocol::MenuId::named("main"),
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
        wait_for_closed_pane(&adapter, "expire-blocked").await;
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
                        root: muxe_protocol::MenuId::named("main"),
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
                root: muxe_protocol::MenuId::named("main"),
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
        wait_for_closed_pane(&adapter, "replace-blocked").await;
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
                    root: muxe_protocol::MenuId::named("main"),
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
        wait_for_closed_pane(&adapter, "origin-fails").await;
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
        wait_for_closed_pane(&adapter, "abort-real").await;
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
                root: muxe_protocol::MenuId::named("main"),
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
        wait_for_closed_pane(&adapter, "expire-real").await;
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![("expire-real".to_owned(), Some("temporary-tab".to_owned()))]
        );
    }
    #[tokio::test]
    async fn pending_pane_cleanup_retries_after_one_failure() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token = prepared_registered_launch(&broker, "retry-pane", Some("temporary-tab")).await;
        adapter.fail_close_once.store(true, Ordering::SeqCst);
        broker
            .abort(token)
            .await
            .expect("abort enqueues pane cleanup without awaiting host I/O");
        wait_for_closed_pane(&adapter, "retry-pane").await;
        assert_eq!(
            *adapter.closed_panes.lock().await,
            vec![("retry-pane".to_owned(), Some("temporary-tab".to_owned()))]
        );
        assert!(
            broker.state.lock().await.cleanup.pending_panes.is_empty(),
            "successful retry consumes retained pane provenance"
        );
    }

    #[tokio::test]
    async fn capture_cleanup_retries_after_one_failure() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let (session, _events) = attach_ready(&broker, "retry-capture").await;
        adapter.fail_end_once.store(true, Ordering::SeqCst);
        broker
            .detach(&session, CaptureReleaseReason::UiDismissed)
            .await
            .expect("detach enqueues capture cleanup");
        wait_for_ended_capture(&adapter, session.as_str()).await;
        assert_eq!(
            adapter.ended_captures.lock().await.len(),
            1,
            "one retained capture lease is ended after retry"
        );
        assert!(broker.state.lock().await.cleanup.captures.is_empty());
    }

    #[tokio::test]
    async fn requeued_pane_entry_keeps_live_task_through_terminal_window() {
        // B2 pane variant: parks the task between success entry-removal and
        // slot removal, re-enqueues the same lease in that window, and proves
        // the entry keeps a live owning task that completes the cleanup.
        // Pre-fix (plain `remove_if_claim` exit) this orphans the re-enqueued
        // entry: no task owns the key, so the close never happens.
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "requeue-pane", Some("temporary-tab")).await;
        broker
            .abort(token)
            .await
            .expect("abort enqueues pane cleanup");
        let key = {
            let state = broker.state.lock().await;
            let lease = state
                .cleanup
                .pending_panes
                .keys()
                .next()
                .expect("abort retains a pane entry")
                .clone();
            crate::broker::CleanupTaskKey::PendingPane(lease)
        };
        // Block the first close so the task cannot finish before the gate
        // is armed, then wait until it owns the key.
        adapter.block_close.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !broker.cleanup.has_live_task(&key) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cleanup task owns its key");
        tokio::time::timeout(Duration::from_secs(1), adapter.close_entered.notified())
            .await
            .expect("pane task reaches the blocked adapter close");
        let (entered, release) = crate::cleanup_task_hooks::arm(&key);
        // Release the first close: it succeeds, the task removes the entry,
        // then parks in the exit gate before removing its slot.
        adapter.block_close.store(false, Ordering::SeqCst);
        adapter.close_release.notify_one();
        // Capture the SAME provenance before the terminal pass consumes it:
        // re-enqueueing this exact (session, registration) reuses the same
        // lease id, hence the same CleanupTaskKey the parked task owns.
        let (session, registration) = {
            let state = broker.state.lock().await;
            let (session, entry) = state
                .cleanup
                .pending_panes
                .iter()
                .next()
                .map(|(_, entry)| (entry.session.clone(), entry.registration.clone()))
                .expect("abort retains a pane entry");
            (session, entry)
        };
        // Trigger the terminal pass: the task removes the entry and parks in
        // the exit gate between entry removal and slot removal.
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("task parks in the terminal window");
        // Re-enqueue the SAME key inside the window: the slot is still live
        // (handle not finished), so the enqueue wakes instead of spawning.
        broker
            .enqueue_pending_pane_close(&session, registration, None)
            .await;
        assert!(
            broker.cleanup.has_live_task(&key),
            "re-enqueued entry keeps its live owning task"
        );
        release.notify_one();
        // Disarm immediately: the re-processing pass also routes through the
        // handshake, and must not park a second time.
        crate::cleanup_task_hooks::clear();
        wait_for_closed_pane(&adapter, "requeue-pane").await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !broker.state.lock().await.cleanup.pending_panes.is_empty()
                || broker.cleanup.task_count() != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("re-enqueued pane cleanup completes and the slot is released");
        crate::cleanup_task_hooks::clear();
    }

    #[tokio::test]
    async fn requeued_capture_entry_keeps_live_task_through_terminal_window() {
        // B2 capture variant: same terminal-window interleaving for captures.
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let (session, _events) = attach_ready(&broker, "requeue-capture").await;
        let lease_id = {
            broker
                .detach(&session, CaptureReleaseReason::UiDismissed)
                .await
                .expect("detach enqueues capture cleanup");
            let state = broker.state.lock().await;
            state
                .cleanup
                .captures
                .keys()
                .next()
                .expect("detach retains a capture entry")
                .clone()
        };
        let key = crate::broker::CleanupTaskKey::Capture(lease_id);
        // Block the first end so the task cannot finish before the gate is
        // armed, then wait until it owns the key.
        adapter.block_end.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !broker.cleanup.has_live_task(&key) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("capture task owns its key");
        tokio::time::timeout(Duration::from_secs(1), adapter.end_entered.notified())
            .await
            .expect("capture task reaches the blocked adapter end");
        let (entered, release) = crate::cleanup_task_hooks::arm(&key);
        // Release the first end: it succeeds, the task removes the entry,
        // then parks in the exit gate before removing its slot.
        adapter.block_end.store(false, Ordering::SeqCst);
        adapter.end_release.notify_one();
        // Capture the SAME lease before the terminal pass consumes it:
        // re-enqueueing this exact lease reuses the same CaptureLeaseId,
        // hence the same CleanupTaskKey the parked task owns.
        let (lease, reason) = {
            let state = broker.state.lock().await;
            state
                .cleanup
                .captures
                .values()
                .next()
                .map(|entry| (entry.lease.clone(), entry.reason))
                .expect("detach retains a capture entry")
        };
        // The task removes the entry and parks in the exit gate between
        // entry removal and slot removal.
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("capture task parks in the terminal window");
        // Re-enqueue the SAME key inside the window: the slot is still live
        // (handle not finished), so the enqueue wakes instead of spawning.
        broker.enqueue_capture_cleanup(lease, reason, None).await;
        assert!(
            broker.cleanup.has_live_task(&key),
            "re-enqueued capture keeps its live owning task"
        );
        release.notify_one();
        crate::cleanup_task_hooks::clear();
        wait_for_ended_capture(&adapter, session.as_str()).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !broker.state.lock().await.cleanup.captures.is_empty()
                || broker.cleanup.task_count() != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("re-enqueued capture cleanup completes and the slot is released");
        crate::cleanup_task_hooks::clear();
    }

    #[tokio::test]
    async fn drain_fails_closed_when_pane_close_never_confirms() {
        // B3 pane variant: every close fails, so the registry never drains;
        // drain must return ActivationCleanupUnconfirmed (carrying the
        // recorded error) and reopen admission (seal cleared).
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token = prepared_registered_launch(&broker, "drain-pane", Some("temporary-tab")).await;
        adapter.fail_close_always.store(true, Ordering::SeqCst);
        broker
            .abort(token)
            .await
            .expect("abort enqueues pane cleanup");
        let result = broker.drain_for_activation().await;
        let detail = match result {
            Err(BrokerError::ActivationCleanupUnconfirmed(detail)) => detail,
            other => panic!("drain must not succeed with unconfirmed cleanup: {other:?}"),
        };
        assert!(
            detail.contains("injected pane cleanup failure"),
            "drain error carries the recorded last_cleanup_error: {detail}"
        );
        assert!(
            !broker.state.lock().await.activation_sealed,
            "failed drain reopens admission so the coordinator can retry"
        );
        // Registry still holds the entry for the retry path.
        assert_eq!(
            broker.state.lock().await.cleanup.pending_panes.len(),
            1,
            "unconfirmed entry is retained for retry"
        );
    }

    #[tokio::test]
    async fn drain_fails_closed_when_capture_end_never_confirms() {
        // B3 capture variant.
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let (session, _events) = attach_ready(&broker, "drain-capture").await;
        adapter.fail_end_always.store(true, Ordering::SeqCst);
        broker
            .detach(&session, CaptureReleaseReason::UiDismissed)
            .await
            .expect("detach enqueues capture cleanup");
        let result = broker.drain_for_activation().await;
        let detail = match result {
            Err(BrokerError::ActivationCleanupUnconfirmed(detail)) => detail,
            other => panic!("drain must not succeed with unconfirmed capture: {other:?}"),
        };
        assert!(
            detail.contains("injected capture cleanup failure"),
            "drain error carries the recorded last_cleanup_error: {detail}"
        );
        assert!(
            !broker.state.lock().await.activation_sealed,
            "failed drain reopens admission so the coordinator can retry"
        );
        assert_eq!(
            broker.state.lock().await.cleanup.captures.len(),
            1,
            "unconfirmed capture is retained for retry"
        );
    }

    #[tokio::test]
    async fn drain_confirms_cleanup_and_returns_ok_promptly() {
        // B3 success case: cooperating adapter, drain observes confirmation.
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token = prepared_registered_launch(&broker, "drain-ok", Some("temporary-tab")).await;
        broker
            .abort(token)
            .await
            .expect("abort enqueues pane cleanup");
        tokio::time::timeout(Duration::from_secs(1), broker.drain_for_activation())
            .await
            .expect("drain completes promptly with a cooperating adapter")
            .expect("drain returns Ok once cleanup is confirmed");
        assert!(
            broker.state.lock().await.cleanup.pending_panes.is_empty(),
            "confirmed entries are gone after drain"
        );
        wait_for_closed_pane(&adapter, "drain-ok").await;
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the cancellation barrier scenario keeps unlink and cleanup ordering together"
    )]
    async fn cancelled_detach_cannot_drop_cleanup_provenance() {
        // B4: provenance is inserted atomically with the unlink under the
        // state lock, so cancelling detach between the unlink and the
        // `request_execution_stop` await cannot lose it. Pre-fix the capture
        // and pane enqueues sit after that await, so nothing is inserted at
        // the barrier and this test fails there.
        let (adapter, broker, binding, _directory) = scoped_two_client_fixture();
        // Gated flow: prepare + register + attach(wait) + commit, so the
        // session holds both a capture lease and a gated registration.
        let token =
            prepared_registered_launch(&broker, "cancel-detach", Some("temporary-tab")).await;
        let (events, _events_rx) = mpsc::channel(8);
        let RequestResult::WaitForAttachment(pending) = broker
            .handle(
                PeerRole::Ui,
                ClientRequest::AttachUi(AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: HostPaneId::new("cancel-detach"),
                    pending_launch: Some(token),
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events,
            )
            .await
            .expect("gated attach waits")
        else {
            panic!("expected a gated attachment waiter");
        };
        let session = pending.session().clone();
        broker
            .commit(token, HostPaneId::new("cancel-detach"))
            .await
            .expect("commit publishes gated ownership");
        // Invoke a cancellable detachable execution on the session so detach
        // has a `cancel` entry and awaits `request_execution_stop` (which
        // calls the adapter's blocking `cancel`) after the unlink.
        let invoked = broker
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
                mpsc::channel(1).0,
            )
            .await
            .expect("detachable invocation is accepted");
        assert!(
            matches!(
                invoked,
                RequestResult::Immediate(BrokerResponse::InvocationAccepted { .. })
            ),
            "invocation is accepted before the detach race"
        );
        // The execution must still own the session with cancel-on-control, or
        // detach has no `cancel` entry and never awaits `request_execution_stop`.
        {
            let state = broker.state.lock().await;
            let attached = state
                .executions
                .values()
                .filter(|record| {
                    record.session.as_ref() == Some(&session)
                        && record.cancellable
                        && record.on_menu_control == muxe_core::MenuControlAction::Cancel
                })
                .count();
            assert_eq!(
                attached, 1,
                "one cancellable cancel-on-control execution owns the session"
            );
        }
        // Block inside `request_execution_stop`: detach has released the
        // state lock (unlink + atomic inserts done post-fix) but has not yet
        // reached the provenance enqueues (pre-fix) or the readiness send.
        // Block the supervised adapter awaits too: otherwise the tasks
        // spawned by the atomic insert consume the entries before the
        // post-cancel assert runs.
        adapter.block_close.store(true, Ordering::SeqCst);
        adapter.block_end.store(true, Ordering::SeqCst);
        adapter.block_cancel.store(true, Ordering::SeqCst);
        let detach_broker = Arc::clone(&broker);
        let session_clone = session.clone();
        let detach = tokio::spawn(async move {
            detach_broker
                .detach(&session_clone, CaptureReleaseReason::UiDismissed)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), adapter.cancel_entered.notified())
            .await
            .expect("detach parks inside request_execution_stop");
        // Cancel exactly in the window: unlink done, stop-await pending.
        detach.abort();
        let _ = detach.await;
        // Provenance must already exist: atomic with the unlink, not after
        // the stop await. Pre-fix this assertion fails (zero entries).
        {
            let state = broker.state.lock().await;
            assert_eq!(
                state.cleanup.pending_panes.len(),
                1,
                "cancelled detach keeps the pane entry"
            );
            assert_eq!(
                state.cleanup.captures.len(),
                1,
                "cancelled detach keeps the capture entry"
            );
        }
        adapter.block_cancel.store(false, Ordering::SeqCst);
        adapter.cancel_release.notify_waiters();
        adapter.block_close.store(false, Ordering::SeqCst);
        adapter.block_end.store(false, Ordering::SeqCst);
        adapter.close_release.notify_waiters();
        adapter.end_release.notify_waiters();
        wait_for_closed_pane(&adapter, "cancel-detach").await;
        wait_for_ended_capture(&adapter, session.as_str()).await;
        crate::cleanup_task_hooks::clear();
    }

    #[tokio::test]
    async fn aborted_request_cannot_strand_inflight_pane_cleanup() {
        let (adapter, broker, _binding, _directory) = scoped_two_client_fixture();
        let token =
            prepared_registered_launch(&broker, "abort-cleanup", Some("temporary-tab")).await;
        adapter.block_close.store(true, Ordering::SeqCst);
        let hook = Arc::new(CleanupEnqueueHook::new());
        broker.set_cleanup_enqueue_hook(Some(Arc::clone(&hook)));
        let abort_broker = Arc::clone(&broker);
        let request = tokio::spawn(async move { abort_broker.abort(token).await });
        hook.entered.notified().await;
        adapter.close_entered.notified().await;
        assert!(
            !request.is_finished(),
            "originating abort remains pending at the post-enqueue barrier"
        );
        request.abort();
        hook.release.notify_one();
        adapter.close_release.notify_one();
        wait_for_closed_pane(&adapter, "abort-cleanup").await;
        assert!(
            broker.state.lock().await.cleanup.pending_panes.is_empty(),
            "supervised cleanup completes after requester abort"
        );
        let join_error = request
            .await
            .expect_err("aborted origin request must report cancellation");
        assert!(
            join_error.is_cancelled(),
            "origin request future was cancelled rather than completing cleanup inline"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while broker.cleanup.task_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("completed cleanup task is removed from the supervisor registry");
    }
    async fn prepare_pending_scope(
        broker: &Broker,
    ) -> (PendingLaunchToken, PendingAttachment, UiSessionId) {
        // A launcher flow in a third scope, still uncommitted when expiry hits.
        let pending = broker
            .prepare(muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new("client-c"),
                root: muxe_protocol::MenuId::named("main"),
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
                    root: muxe_protocol::MenuId::named("main"),
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
            cancellable: AtomicBool::new(true),
            block_cancel: AtomicBool::new(false),
            cancel_entered: Arc::new(Notify::new()),
            cancel_release: Arc::new(Notify::new()),
            capture_entered: Arc::new(Notify::new()),
            capture_release: Arc::new(Notify::new()),
            block_capture: AtomicBool::new(false),
            pending_releases: AtomicUsize::new(0),
            ended_captures: Mutex::new(Vec::new()),
            closed_panes: Mutex::new(Vec::new()),
            close_entered: Arc::new(Notify::new()),
            close_release: Arc::new(Notify::new()),
            block_close: AtomicBool::new(false),
            fail_close_once: AtomicBool::new(false),
            fail_close_always: AtomicBool::new(false),
            block_end: AtomicBool::new(false),
            end_entered: Arc::new(Notify::new()),
            end_release: Arc::new(Notify::new()),
            fail_end_once: AtomicBool::new(false),
            fail_end_always: AtomicBool::new(false),
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
        settings:
          execution:
            on-menu-control: cancel
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("test configuration compiles");
        let root = named("main");
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
    async fn wait_for_closed_pane(adapter: &ScopedTestAdapter, pane: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if adapter
                    .closed_panes
                    .lock()
                    .await
                    .iter()
                    .any(|(closed, _)| closed == pane)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("supervised pending-pane cleanup completes");
    }

    async fn wait_for_ended_capture(adapter: &ScopedTestAdapter, session: &str) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if adapter
                    .ended_captures
                    .lock()
                    .await
                    .iter()
                    .any(|(ended, _)| ended == session)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("supervised capture cleanup completes");
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
                    root: muxe_protocol::MenuId::named("main"),
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
        wait_for_ended_capture(adapter, expired.as_str()).await;
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
                    root: muxe_protocol::MenuId::named("main"),
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
        let adapter = counting_adapter(false);
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
                if broker
                    .generic
                    .processes
                    .lock()
                    .expect("generic supervisor registry is not poisoned")
                    .is_empty()
                {
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
    #[expect(
        clippy::too_many_lines,
        reason = "the detach-policy process-group lifecycle remains one auditable scenario"
    )]
    async fn activation_drain_detach_policy_keeps_owned_generic_process_group() {
        let directory = tempfile::tempdir().expect("owned generic-process directory");
        let script = directory.path().join("drain-detach-group.sh");
        let pidfile = directory.path().join("child.pid");
        let fifo = directory.path().join("started");
        let exit_fifo = directory.path().join("exit_trigger");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::from_bits_truncate(0o600))
            .expect("owned fifo exists");
        nix::unistd::mkfifo(&exit_fifo, nix::sys::stat::Mode::from_bits_truncate(0o600))
            .expect("exit fifo exists");
        let startup = tokio::task::spawn_blocking({
            let fifo = fifo.clone();
            move || {
                let mut reader = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&fifo)
                    .map_err(|error| error.to_string())?;
                let mut started = [0u8; 7];
                std::io::Read::read_exact(&mut reader, &mut started)
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(started)
            }
        });
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nprintf started > '{}'\nread line < '{}'\nexit 0\n",
                pidfile.display(),
                fifo.display(),
                exit_fifo.display()
            ),
        )
        .expect("write owned generic-process script");
        let adapter = counting_adapter(false);
        let yaml = format!(
            r#"
version: 1
menus:
  main:
    bindings:
      d:
        label: detach command
        action:
          type: command:execute
          program: /bin/sh
          args:
            - {script:?}
          cwd: {cwd:?}
        settings:
          execution:
            mode: await
            on-menu-control: detach
"#,
            script = script.to_string_lossy(),
            cwd = directory.path().to_string_lossy(),
        );
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<generic activation-drain regression>"),
            yaml.as_str(),
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("generic drain configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root)
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("generic binding is visible");
        let broker = Broker::from_compiled(adapter, directory.path().join("config.yml"), config);
        let (session, mut events_rx) = attach_ready(&broker, "generic-drain").await;
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
                mpsc::channel(1).0,
            )
            .await
            .expect("awaited detach command is accepted");
        assert!(
            matches!(
                response,
                RequestResult::Immediate(BrokerResponse::InvocationAccepted {
                    disposition: InvocationDisposition::Awaited,
                    ..
                })
            ),
            "detach-policy invocation stays awaited while the UI is attached"
        );
        // Startup barrier proves the child process actually started and wrote its PID.
        let _ = startup
            .await
            .expect("startup task joins")
            .expect("child signals start");
        let raw_pid: i32 = std::fs::read_to_string(&pidfile)
            .expect("child publishes its pid")
            .trim()
            .parse()
            .expect("pid parses");
        let child_pid = nix::unistd::Pid::from_raw(raw_pid);
        assert!(
            nix::sys::signal::kill(child_pid, None).is_ok(),
            "child is alive"
        );
        let core = {
            let state = broker.state.lock().await;
            state
                .awaiting
                .get(&session)
                .copied()
                .expect("awaited generic execution is registered")
        };
        // Drain routes session-owned work through the dismissal transition instead
        // of cancelling it directly.
        broker
            .drain_for_activation()
            .await
            .expect("activation drain honors the detach dismissal policy");
        // Drain notifies the UI before teardown, detaches the session, and
        // retains the Detach-policy child under its existing supervisor.
        let Some(WireMessage::Event {
            event: BrokerEvent::BrokerRetiring,
            ..
        }) = events_rx.recv().await
        else {
            panic!("drain emits BrokerRetiring to the live UI session before teardown");
        };
        assert!(
            !broker.sessions.lock().await.contains_key(&session),
            "drain detaches the UI session"
        );
        assert!(
            broker.has_supervised_children().await,
            "an awaited generic command configured to detach survives the drain supervised"
        );
        assert!(
            nix::sys::signal::kill(child_pid, None).is_ok(),
            "the detached child is still alive after the drain"
        );
        {
            let state = broker.state.lock().await;
            let record = state
                .executions
                .get(&core)
                .expect("detached generic owner survives the drain");
            assert!(
                record.session.is_none(),
                "drain clears the session attachment"
            );
            assert!(!record.awaiting, "drain clears the awaiting index entry");
            assert!(
                !state.awaiting.contains_key(&session),
                "activation drain leaves no awaiting entry behind"
            );
        }
        // The surviving child still finishes naturally and is reaped.
        {
            let mut trigger = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&exit_fifo)
                .expect("open exit trigger fifo");
            std::io::Write::write_all(&mut trigger, b"exit\n").expect("write exit trigger");
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while broker.has_supervised_children().await {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the detached child is reaped after its natural completion");
        assert_eq!(
            nix::sys::signal::kill(child_pid, None),
            Err(nix::errno::Errno::ESRCH),
            "detached child process is reaped and no longer exists"
        );
        assert!(
            broker.state.lock().await.executions.is_empty(),
            "the reaped generic owner is removed from supervision"
        );
        broker.reopen_dispatch().await;
        assert!(
            !broker.state.lock().await.activation_sealed,
            "reopening after a completed generic drain restores admission"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the cancel-policy process-group lifecycle remains one auditable scenario"
    )]
    async fn activation_drain_cancel_policy_stops_owned_generic_process_group() {
        let directory = tempfile::tempdir().expect("owned generic-process directory");
        let script = directory.path().join("drain-cancel-group.sh");
        let pidfile = directory.path().join("child.pid");
        let fifo = directory.path().join("started");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::from_bits_truncate(0o600))
            .expect("owned fifo exists");
        let startup = tokio::task::spawn_blocking({
            let fifo = fifo.clone();
            move || {
                let mut reader = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&fifo)
                    .map_err(|error| error.to_string())?;
                let mut started = [0u8; 7];
                std::io::Read::read_exact(&mut reader, &mut started)
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(started)
            }
        });
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nprintf started > '{}'\nwhile :; do sleep 1; done\n",
                pidfile.display(),
                fifo.display()
            ),
        )
        .expect("write owned generic-process script");
        let adapter = counting_adapter(false);
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<generic activation-drain cancel regression>"),
            r"
version: 1
menus:
  main:
    bindings:
      c:
        label: cancel command
        action: config:reload
",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("cancel drain configuration compiles");
        let broker = Broker::from_compiled(adapter, directory.path().join("config.yml"), config);
        let (session, mut events_rx) = attach_ready(&broker, "generic-drain-cancel").await;
        // Cancel-policy work cannot come from `command:execute` YAML: the compiler
        // rejects `on-menu-control: cancel` for the counting adapter because its
        // portable validation reports commands as non-cancellable. Build the same
        // execution record the invoke path would reserve for an awaited,
        // Cancel-policy cancellable generic child, then supervise a real child for it.
        let wire = ExecutionId([3; 16]);
        let core = CoreExecutionId(3);
        broker
            .reserve_execution(
                session.clone(),
                wire,
                core,
                ExecutionOwner::GenericProcess,
                None,
                &muxe_core::ExecutionPolicy {
                    mode: muxe_core::ExecutionMode::Await,
                    timeout: None,
                    on_timeout: TimeoutAction::Detach,
                    on_menu_control: muxe_core::MenuControlAction::Cancel,
                },
            )
            .await
            .expect("cancel-policy generic execution reserves");
        let mut origin = CountingAdapter::origin_without_cwd();
        origin.pane_cwd = Some(directory.path().to_path_buf());
        let script_arg = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
            script.to_string_lossy().into_owned(),
        )));
        broker
            .execute_command(CommandLaunch {
                session: session.clone(),
                wire,
                core,
                command: CommandAction {
                    program: ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
                        "/bin/sh".to_owned(),
                    ))),
                    args: vec![script_arg],
                    cwd: None,
                    env: std::collections::BTreeMap::default(),
                },
                origin,
                cwd_from_context: false,
                policy: muxe_core::ExecutionPolicy {
                    mode: muxe_core::ExecutionMode::Await,
                    timeout: None,
                    on_timeout: TimeoutAction::Detach,
                    on_menu_control: muxe_core::MenuControlAction::Cancel,
                },
            })
            .await
            .expect("cancel-policy generic child starts");
        let _ = startup
            .await
            .expect("startup task joins")
            .expect("child signals start");
        let raw_pid: i32 = std::fs::read_to_string(&pidfile)
            .expect("child publishes its pid")
            .trim()
            .parse()
            .expect("pid parses");
        let child_pid = nix::unistd::Pid::from_raw(raw_pid);
        assert!(
            nix::sys::signal::kill(child_pid, None).is_ok(),
            "child is alive"
        );
        {
            let state = broker.state.lock().await;
            assert_eq!(
                state.awaiting.get(&session),
                Some(&core),
                "cancel-policy execution is awaited before the drain"
            );
            let record = state
                .executions
                .get(&core)
                .expect("cancel-policy execution is owned");
            assert_eq!(
                record.on_menu_control,
                muxe_core::MenuControlAction::Cancel,
                "cancel-policy precondition holds before the drain"
            );
        }
        broker
            .drain_for_activation()
            .await
            .expect("activation drain applies the cancel dismissal policy");
        let Some(WireMessage::Event {
            event: BrokerEvent::BrokerRetiring,
            ..
        }) = events_rx.recv().await
        else {
            panic!("drain emits BrokerRetiring to the live UI session before teardown");
        };
        assert!(
            !broker.sessions.lock().await.contains_key(&session),
            "drain detaches the UI session"
        );
        // The Cancel-policy child is stopped through the dismissal transition and reaped.
        tokio::time::timeout(Duration::from_secs(10), async {
            while broker.has_supervised_children().await
                || !broker.state.lock().await.executions.is_empty()
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("the cancelled child is reaped after the drain");
        assert_eq!(
            nix::sys::signal::kill(child_pid, None),
            Err(nix::errno::Errno::ESRCH),
            "cancelled child process is reaped and no longer exists"
        );
        broker.reopen_dispatch().await;
        assert!(
            !broker.state.lock().await.activation_sealed,
            "reopening after a completed generic drain restores admission"
        );
    }
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
    #[error("activation drain timed out waiting for broker-owned host cleanup: {0}")]
    ActivationCleanupUnconfirmed(String),
    #[error("a menu control is already pending for this execution")]
    PendingControlInFlight,
    #[error("the UI already has an awaited execution in flight")]
    PendingExecutionInFlight,
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
