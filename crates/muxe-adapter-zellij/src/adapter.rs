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

use async_trait::async_trait;
use muxe_adapter_api::{
    ActivationReadiness, AdapterCapabilities, AdapterError, AdapterErrorKind, AdapterHealthEvent,
    CaptureLease as ApiCaptureLease, CaptureLeaseId, CaptureReleaseReason, CaptureRequest,
    DispatchAccepted, DispatchCompletion, ExecutionCorrelationId, HostAdapter, HostIdentity,
    HostKind as ApiHostKind, KeyboardCapabilities, ModalScopeId, NativeDispatchRequest,
    OriginCaptureRequest, PendingPaneLease, PendingPaneLeaseId, PendingPaneRegistration,
    PortableDispatchRequest, PostDismissalPortableDispatchRequest, UiSessionId,
};
use muxe_core::{
    ActionValidation, ActionValidator, ConfigDiagnostic, ExecutionCapabilities, ExecutionId,
    NativeActionCandidate, OriginContext, PaneId, PortableAction, SourceSpan,
};
use muxe_protocol::{
    CaptureLeaseId as CommonCaptureLeaseId, ExecutionId as CommonExecutionId,
    UiSessionId as CommonUiSessionId,
};
use muxe_zellij_protocol::{
    BridgeEvent, BridgeRequest, BridgeResponse, CaptureEndReason, ChannelGeneration,
    EventSubscription, MAX_PIPE_LINE_LEN, PipeEventKind, PipeRequest, RegistrationId, RequestId,
    ZellijDispatchRequest, ZellijOrigin, ZellijOriginRequest, bridge_build_id,
    bridge_protocol_fingerprint, decode_event_line, encode_event_subscription, encode_request_line,
    generated::{RawNativeCommand, ValidatedNativeCommand},
    generated_action_fingerprint, pinned_source_revision,
};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, Notify, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    ZellijValidator,
    capture::CaptureTable,
    origin::{OriginError, build_origin_context},
    parse::candidate_to_raw,
    pipes::{PipeChannel, PipeTransportError, RELEASE_TIMEOUT, channel_names},
    portable::{
        PortableError, PortableMapping, creation_requires_post_dismissal, map_portable,
        map_post_dismissal_creation,
    },
    registry::ZellijRegistry,
};

/// How long bootstrap retries the claim fan-out before failing rather than guessing.
const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(15);
/// How long capture waits for the bridge to confirm Locked mode.
const CAPTURE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long origin fan-out waits per client attempt before trying the next one.
const ORIGIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long resume waits for fresh compatible registrations covering the
/// authoritative membership before failing closed and staying suspended.
const RESUME_READY_TIMEOUT: Duration = Duration::from_secs(5);
/// Event-loop sweep cadence for heartbeat-lease expiry: a quiet bridge is
/// noticed within a few seconds past its 15s lease even when no caller
/// touches the availability gates.
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
/// How long one authoritative `list-clients` membership query may take
/// before resume and readiness fail closed instead of pretending Healthy.
const MEMBERSHIP_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
/// Output cap for one `list-clients` query: the pipe protocol's own line
/// bound. A table larger than a single pipe line cannot be a genuine
/// client census, so the read fails closed instead of buffering it.
const MEMBERSHIP_OUTPUT_CAP: usize = MAX_PIPE_LINE_LEN;
type CaptureReady = Result<String, AdapterError>;

struct PendingReply<T> {
    request: Option<RequestProvenance>,
    sender: T,
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the fixed public adapter error returned across this module"
)]
fn subscription_payload(generation: ChannelGeneration) -> Result<String, AdapterError> {
    encode_event_subscription(EventSubscription::new(generation))
        .map_err(|error| AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string()))
}

/// Pending capture waiters keyed by lease, each tagged with the owning
/// client so lease expiry can release only that client's waiters.
type CaptureWaiters = BTreeMap<[u8; 16], (String, PendingReply<oneshot::Sender<CaptureReady>>)>;
type OriginWaiters =
    BTreeMap<String, PendingReply<oneshot::Sender<Result<ZellijOrigin, OriginError>>>>;

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
/// Authoritative host membership oracle: the currently attached Zellij
/// client IDs for one session, in deterministic order, IDs only.
///
/// Bridge registrations prove a bridge is alive and compatible, but silence
/// proves nothing: a detached client sends no signal and a late newcomer
/// may not have registered yet. Resume coverage is always measured against
/// a fresh snapshot from this oracle, never against pipe-observed silence.
#[async_trait]
pub trait MembershipSource: Send + Sync {
    /// Returns the current attached client IDs, sorted and deduplicated.
    async fn snapshot_members(&self) -> Result<Vec<String>, AdapterError>;
}

/// Production oracle: a bounded one-shot `zellij --session <session> action
/// list-clients` over the pinned CLI (`zellij-utils/src/cli.rs`:
/// `Action(ListClients)` maps to `Action::ListClients`, routed through
/// screen/pty `ListClientsMetadata` and rendered by
/// `ClientMetadata::render_many` as
/// `CLIENT_ID ZELLIJ_PANE_ID RUNNING_COMMAND` rows). The query carries the
/// exact scoped binary and an explicit session, never a default host; the
/// querying CLI client holds no focused pane, so it never appears in its
/// own output (route.rs prefers the CLI client precisely for this query).
/// Only the client-ID column is retained; running commands and every other
/// payload stay out of logs, errors, and readiness records.
pub struct CliMembershipSource {
    zellij_exe: PathBuf,
    session_name: String,
}

impl CliMembershipSource {
    /// Builds the oracle for one session over its scoped binary.
    #[must_use]
    pub fn new(zellij_exe: PathBuf, session_name: String) -> Self {
        Self {
            zellij_exe,
            session_name,
        }
    }
}

/// Parses pinned `list-clients` table output into sorted unique client IDs.
/// The header row is skipped when present; every other non-blank line
/// contributes its first whitespace column. A header-only table is an empty
/// session, not an error; empty output fails closed instead of pretending
/// an empty membership.
///
/// IDs are classified from source, not guessed: the pinned host renders one
/// row per attached client holding a focused pane (`screen.rs`
/// `get_layout_metadata` over `connected_clients` with
/// `get_active_pane_id`, rendered by `ClientMetadata::render_many`), and a
/// client ID is a `u16` (`zellij-utils/src/data.rs`). Transient CLI
/// callers — including this query and the broker's pipe children — never
/// enter `connected_clients` (no `AttachClient` on the `is_cli_client`
/// path), so the census cannot include its own processes. A row whose
/// first column is not a numeric client ID is an unsupported host shape
/// and fails closed explicitly: carrying it as a member would demand a
/// bridge registration no client can ever satisfy and deadlock coverage.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the public host-adapter error; boxing it would burden every caller"
)]
fn parse_list_clients_output(output: &str) -> Result<Vec<String>, AdapterError> {
    let mut lines = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let Some(first) = lines.next() else {
        return Err(AdapterError::new(
            AdapterErrorKind::Unavailable,
            "zellij list-clients returned no output",
        ));
    };
    let rest: Vec<&str> = if first.split_whitespace().next() == Some("CLIENT_ID") {
        lines.collect()
    } else {
        std::iter::once(first).chain(lines).collect()
    };
    let mut members = Vec::new();
    for line in rest {
        let Some(id) = line.split_whitespace().next() else {
            continue;
        };
        if id.parse::<u16>().is_err() {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "zellij list-clients returned an unsupported row shape",
            ));
        }
        members.push(id.to_owned());
    }
    members.sort();
    members.dedup();
    Ok(members)
}

#[async_trait]
impl MembershipSource for CliMembershipSource {
    async fn snapshot_members(&self) -> Result<Vec<String>, AdapterError> {
        use tokio::io::AsyncReadExt;
        let mut child = tokio::process::Command::new(&self.zellij_exe)
            .arg("--session")
            .arg(&self.session_name)
            .arg("action")
            .arg("list-clients")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!("could not query zellij list-clients: {error}"),
                )
            })?;
        let mut stdout = child.stdout.take().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Unavailable,
                "zellij list-clients child has no stdout",
            )
        })?;
        // Bounded drain: the cap is enforced DURING the read, chunk by
        // chunk, so an output flood is killed and reaped without ever
        // allocating past the bound. A post-hoc length check after
        // `read_to_end` would already have buffered the flood.
        let deadline = tokio::time::Instant::now() + MEMBERSHIP_QUERY_TIMEOUT;
        let mut raw = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let _ = child.start_kill();
                tokio::spawn(async move {
                    let _ = child.wait().await;
                });
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "zellij list-clients query timed out",
                ));
            }
            match timeout(remaining, stdout.read(&mut chunk)).await {
                Ok(Ok(0)) => break,
                Ok(Ok(count)) => {
                    if raw.len() + count > MEMBERSHIP_OUTPUT_CAP {
                        let _ = child.start_kill();
                        tokio::spawn(async move {
                            let _ = child.wait().await;
                        });
                        return Err(AdapterError::new(
                            AdapterErrorKind::Unavailable,
                            "zellij list-clients output exceeded its bound",
                        ));
                    }
                    raw.extend_from_slice(&chunk[..count]);
                }
                Ok(Err(error)) => {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Unavailable,
                        format!("zellij list-clients query failed: {error}"),
                    ));
                }
                Err(_) => {
                    let _ = child.start_kill();
                    tokio::spawn(async move {
                        let _ = child.wait().await;
                    });
                    return Err(AdapterError::new(
                        AdapterErrorKind::Unavailable,
                        "zellij list-clients query timed out",
                    ));
                }
            }
        }
        let status = timeout(MEMBERSHIP_QUERY_TIMEOUT, child.wait())
            .await
            .map_err(|_| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "zellij list-clients query timed out",
                )
            })?
            .map_err(|error| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!("zellij list-clients query failed: {error}"),
                )
            })?;
        if !status.success() {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "zellij list-clients query failed",
            ));
        }
        parse_list_clients_output(&String::from_utf8_lossy(&raw))
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
/// correlation; lifecycle lines correlate through their own waiters. Request
/// identity is allocated only after the active registration is re-resolved at
/// send time.
struct QueuedItem {
    execution: Option<ExecutionId>,
    /// Synthetic slot this item settles in its execution record. A fresh
    /// `INITIAL`-then-`next()` sequence per execution, reserved at acceptance
    /// and renamed to the minted transport request ID at send, so every
    /// queued request owns a distinct terminal slot even before the
    /// transport mints its live provenance.
    request_slot: Option<RequestId>,
    client_id: String,
    /// Taken for encoding; restored on retry so no clone is needed.
    payload: Option<BridgeRequest>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SendItemResult {
    Accepted,
    Continue,
    RestartWhole,
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct RequestProvenance {
    request_id: RequestId,
    registration: RegistrationId,
}

/// One accepted execution and the host requests that settle it.
///
/// Acceptance/completion contract: `dispatch_*` returns [`DispatchAccepted`]
/// once every host request in the batch is queued; the single terminal
/// [`DispatchCompletion`] is published only after **every** request in
/// `requests` has reached a terminal state (bridge completion, write
/// failure, retirement, purge, release timeout, pipe restart, or
/// suspend/shutdown). A one-request batch therefore behaves exactly like the
/// schema-v1 single-command path: its lone completion is the terminal.
/// `dispatch_to_client` rejects an empty batch with `InvalidRequest`, so an
/// awaited caller can never accept an execution that completes nothing.
///
/// Aggregate precedence (worst case wins, deterministic):
/// `OutcomeUnknown` > `Failed` > `Succeeded`. Any unknown poisons the
/// aggregate even when a failure is already recorded; a failure beats
/// success; the terminal carries the worst error with the execution.
/// Exactly one terminal is published per execution.
struct ExecutionRecord {
    execution: ExecutionId,
    /// Outstanding host requests that must still settle before the terminal
    /// publishes. `None` until the request reaches the transport and mints
    /// provenance. Settled slots are REMOVED (not marked), so termination
    /// compares `settled` against the immutable `total` below, never against
    /// this map's shrinking length.
    requests: BTreeMap<RequestId, Option<RequestProvenance>>,
    /// Batch size captured at construction; the terminal publishes when
    /// `settled == total`.
    total: u64,
    /// Worst aggregate outcome so far; `None` while every settled request
    /// succeeded (or nothing settled yet).
    worst: Option<(OutcomeRank, AdapterError)>,
    settled: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OutcomeRank {
    Failed,
    Unknown,
}

impl ExecutionRecord {
    fn new(execution: ExecutionId, request_ids: Vec<RequestId>) -> Self {
        Self {
            execution,
            total: request_ids.len() as u64,
            requests: request_ids.into_iter().map(|id| (id, None)).collect(),
            worst: None,
            settled: 0,
        }
    }

    fn note_success(&mut self) -> bool {
        self.settled += 1;
        self.settled == self.total
    }

    fn note_failure(&mut self, rank: OutcomeRank, error: AdapterError) -> bool {
        let worse = match &self.worst {
            None => true,
            Some((OutcomeRank::Failed, _)) => rank == OutcomeRank::Unknown,
            Some((OutcomeRank::Unknown, _)) => false,
        };
        if worse {
            self.worst = Some((rank, error));
        }
        self.settled += 1;
        self.settled == self.total
    }

    fn terminal(&self) -> DispatchCompletion {
        match &self.worst {
            None => DispatchCompletion::Succeeded {
                execution: self.execution,
            },
            Some((OutcomeRank::Failed, error)) => DispatchCompletion::Failed {
                execution: self.execution,
                error: error.clone(),
            },
            Some((OutcomeRank::Unknown, error)) => DispatchCompletion::OutcomeUnknown {
                execution: self.execution,
                error: error.clone(),
            },
        }
    }
}

struct InFlight {
    request: RequestProvenance,
    generation: ChannelGeneration,
    execution: Option<ExecutionId>,
    request_slot: Option<RequestId>,
    settled: bool,
}

struct AtomicChannelGeneration(AtomicU64);

impl AtomicChannelGeneration {
    fn new() -> Self {
        Self(AtomicU64::new(ChannelGeneration::INITIAL.wire_value()))
    }

    fn current(&self) -> ChannelGeneration {
        ChannelGeneration::try_from(self.0.load(Ordering::Acquire))
            .expect("stored channel generation is always nonzero")
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the fixed public adapter error returned across this module"
    )]
    fn advance(&self) -> Result<ChannelGeneration, AdapterError> {
        let previous = self
            .0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Zellij channel generation exhausted",
                )
            })?;
        ChannelGeneration::try_from(previous + 1)
            .map_err(|error| AdapterError::new(AdapterErrorKind::Unavailable, error.to_string()))
    }
}
/// Broker executions stay below this ceiling; dispatch admission rejects the
/// reserved range so adapter-owned local executions cannot collide with them.
const LOCAL_EXECUTION_CEILING: u64 = 1 << 63;
/// Bounds one bridge completion report for an adapter-owned pane close.
const CLOSE_COMPLETION_TIMEOUT: Duration = Duration::from_secs(10);

struct LocalTokenSource(AtomicU64);

impl LocalTokenSource {
    fn new() -> Self {
        Self(AtomicU64::new(1))
    }

    fn mint(&self) -> [u8; 16] {
        let counter = self.0.fetch_add(1, Ordering::Relaxed);
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&counter.to_le_bytes());
        id[8..].copy_from_slice(&counter.to_be_bytes());
        if id == [0; 16] {
            id[0] = 1;
        }
        id
    }
}
#[cfg(test)]
struct WaitHook {
    entered: Notify,
    release: Notify,
}

struct AdapterInner {
    config: ZellijAdapterConfig,
    validator: ZellijValidator,
    request: Arc<dyn PipeChannel>,
    event: Arc<dyn PipeChannel>,
    registry: Mutex<ZellijRegistry>,
    /// Serializes registration publication with request allocation and sends.
    registration_transition: Mutex<()>,
    scheduler_cursor: Mutex<Option<String>>,
    pending_origin: Mutex<OriginWaiters>,
    captures: Mutex<CaptureTable>,
    queues: Mutex<BTreeMap<String, VecDeque<QueuedItem>>>,
    in_flight: Mutex<Option<InFlight>>,
    live_executions: Mutex<BTreeMap<u64, ExecutionRecord>>,
    close_waiters: StdMutex<BTreeMap<u64, oneshot::Sender<DispatchCompletion>>>,
    pending_capture: Mutex<CaptureWaiters>,
    pane_claims: Mutex<BTreeMap<String, String>>,
    pending_leases: StdMutex<BTreeMap<String, (String, RegistrationId)>>,
    snapshots: Mutex<BTreeMap<String, ZellijOrigin>>,
    generation: AtomicChannelGeneration,
    next_correlation: AtomicU64,
    next_local_execution: AtomicU64,
    local_tokens: LocalTokenSource,
    events_tx: mpsc::Sender<AdapterHealthEvent>,
    events_rx: Mutex<mpsc::Receiver<AdapterHealthEvent>>,
    event_loop: StdMutex<Option<JoinHandle<()>>>,
    quiescing: AtomicBool,
    #[cfg(test)]
    health_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    #[cfg(test)]
    emit_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    quiesce_wake: Notify,
    health_wake: Notify,
    shutdown: AtomicBool,
    /// While set, host-bound operations fail closed with `Unavailable` and
    /// pipe recovery pauses until resume reinstalls the transport.
    suspended: AtomicBool,
    /// Monotonic resume-attempt generation. Incremented at the start of every
    /// resume attempt; each registration is stamped with the observing
    /// generation, so evidence never combines across attempts.
    resume_epoch: AtomicU64,
    /// Freshness stamp of the last registration per client: the observing
    /// resume generation plus the event-channel install epoch that delivered
    /// it. Coverage requires both to match the current attempt. Registration
    /// identities also rotate on every new event channel; transport freshness
    /// never permits reuse of a retired identity.
    register_epoch: Mutex<BTreeMap<String, (u64, u64)>>,
    /// Authoritative membership snapshot of the last successful resume
    /// attempt: the exact round coverage was proven against. Feeds the
    /// readiness hook; `None` until a real round completes, cleared back
    /// to `None` on suspend and failed resume so stale evidence — and in
    /// particular constructor defaults on a fresh adapter that never ran
    /// a census — can never serve a coordinator.
    success_snapshot: Mutex<Option<Vec<String>>>,
    /// Authoritative host membership oracle (production: bounded
    /// `list-clients` CLI query). Resume measures coverage against a fresh
    /// snapshot per attempt; the readiness hook reports the retained
    /// success round, never a separate live query that could skew the set.
    membership: Arc<dyn MembershipSource>,
    /// Wakes resume while it awaits fresh post-suspend registrations.
    registry_notify: Notify,
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
        let oracle = Arc::new(CliMembershipSource::new(
            config.zellij_exe.clone(),
            config.session_name.clone(),
        ));
        Self::new_with_membership(config, request, event, oracle)
    }

    /// Builds the adapter over injected channels with an injected membership
    /// oracle. The contract suite injects scripted channels with recorded
    /// lines and a scripted oracle; production callers use
    /// [`ZellijAdapter::connect`], which installs the live CLI oracle.
    ///
    /// # Panics
    ///
    /// Panics if the event-loop mutex is poisoned while storing the spawned
    /// event-loop task.
    pub fn new_with_membership(
        config: ZellijAdapterConfig,
        request: Arc<dyn PipeChannel>,
        event: Arc<dyn PipeChannel>,
        membership: Arc<dyn MembershipSource>,
    ) -> Self {
        let (events_tx, events_rx) = mpsc::channel(256);
        let adapter = Self {
            inner: Arc::new(AdapterInner {
                config,
                validator: ZellijValidator,
                request,
                event,
                registry: Mutex::new(ZellijRegistry::new()),
                registration_transition: Mutex::new(()),
                captures: Mutex::new(CaptureTable::new()),
                scheduler_cursor: Mutex::new(None),
                queues: Mutex::new(BTreeMap::new()),
                in_flight: Mutex::new(None),
                live_executions: Mutex::new(BTreeMap::new()),
                close_waiters: StdMutex::new(BTreeMap::new()),
                pending_origin: Mutex::new(BTreeMap::new()),
                snapshots: Mutex::new(BTreeMap::new()),
                pending_capture: Mutex::new(BTreeMap::new()),
                pane_claims: Mutex::new(BTreeMap::new()),
                pending_leases: StdMutex::new(BTreeMap::new()),
                generation: AtomicChannelGeneration::new(),
                next_correlation: AtomicU64::new(1),
                next_local_execution: AtomicU64::new(1),
                local_tokens: LocalTokenSource::new(),
                events_tx,
                events_rx: Mutex::new(events_rx),
                event_loop: StdMutex::new(None),
                quiescing: AtomicBool::new(false),
                #[cfg(test)]
                health_wait_hook: StdMutex::new(None),
                #[cfg(test)]
                emit_wait_hook: StdMutex::new(None),
                quiesce_wake: Notify::new(),
                health_wake: Notify::new(),
                shutdown: AtomicBool::new(false),
                suspended: AtomicBool::new(false),
                resume_epoch: AtomicU64::new(0),
                register_epoch: Mutex::new(BTreeMap::new()),
                success_snapshot: Mutex::new(None),
                membership,
                registry_notify: Notify::new(),
                started: tokio::time::Instant::now(),
            }),
        };
        let worker = adapter.clone();
        let event_loop = tokio::spawn(async move {
            worker.event_loop().await;
        });
        adapter
            .inner
            .event_loop
            .lock()
            .expect("Zellij event-loop slot is not poisoned")
            .replace(event_loop);
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
        .map_err(|error| transport_error(&error))?;
        let subscribe = subscription_payload(ChannelGeneration::INITIAL)?;
        let event = SubprocessChannel::launch(
            config.zellij_exe.clone(),
            config.session_name.clone(),
            event_name,
            Some(subscribe),
        )
        .await
        .map_err(|error| transport_error(&error))?;
        Ok(Self::new(config, request, event))
    }

    /// Injection point for the contract suite: the request channel under test.
    #[must_use]
    pub fn request_channel(&self) -> &Arc<dyn PipeChannel> {
        &self.inner.request
    }

    /// Injection point for the contract suite: the event channel under test.
    #[must_use]
    pub fn event_channel(&self) -> &Arc<dyn PipeChannel> {
        &self.inner.event
    }
    /// Establishes the initial census round on a fresh target that never
    /// went through suspend/resume: takes one authoritative membership
    /// snapshot and awaits fresh compatible registrations covering it on
    /// the current event-channel install, then retains the round and
    /// reports `Healthy`. The transport is left alone — no respawn, no
    /// generation bump, no park — so pre-swap control stays available and
    /// registrations already in flight count: with no transport
    /// replacement there is no stale-evidence ambiguity to disambiguate.
    /// Fails closed (leaving prior partial stamps for the next retry)
    /// when suspended — the abort path owns that state — when no channel
    /// is installed, when the query fails, or when coverage times out.
    /// Already-established adapters return `Ok` immediately. Never blocks
    /// target bootstrap itself: call once after control bind, then poll
    /// [`activation_readiness`](HostAdapter::activation_readiness) and
    /// retry on `Err`; no UI or commit may proceed until it reports `Some`.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when shut down (`Shutdown`, fail fast without
    /// touching the transport), when suspended, when no event channel is
    /// installed, when the membership query fails, or when coverage times
    /// out. Failures leave partial stamps for the next retry and never
    pub async fn establish_initial_round(&self) -> Result<(), AdapterError> {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Zellij adapter is shut down; initial census is unavailable",
            ));
        }
        if self.inner.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij adapter is suspended for activation; resume owns the census round",
            ));
        }
        if self.inner.success_snapshot.lock().await.is_some() {
            return Ok(());
        }
        let epoch = self.inner.resume_epoch.load(Ordering::SeqCst);
        let Some(channel) = self.inner.event.install_epoch().await else {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij initial census has no installed event channel",
            ));
        };
        let snapshot = match self.inner.membership.snapshot_members().await {
            Ok(mut members) => {
                members.sort();
                members.dedup();
                members
            }
            Err(_) => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Zellij initial census could not observe the current client membership",
                ));
            }
        };
        if !self
            .await_resume_membership(epoch, channel, &snapshot)
            .await
        {
            if self.inner.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "Zellij adapter shut down while awaiting the initial census",
                ));
            }
            let registered = self.fresh_census().await;
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!(
                    "Zellij initial census did not observe fresh registrations for the current membership \
                     (members={snapshot:?}, registered={registered:?})"
                ),
            ));
        }
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Zellij adapter shut down while awaiting the initial census",
            ));
        }
        *self.inner.success_snapshot.lock().await = Some(snapshot);
        self.emit(AdapterHealthEvent::Healthy {
            identity: self.host_identity(),
        })
        .await;
        Ok(())
    }

    /// Reinstalls the event subscription before retrying an unsuccessful
    /// initial census. A bridge loaded after the prior one-shot broadcast
    /// receives the subscription from the fresh child and re-registers.
    /// The event-channel install epoch changes, so registrations observed
    /// before this refresh cannot satisfy the next census.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the adapter is shut down or suspended,
    /// or when the event child cannot be replaced.
    pub async fn refresh_initial_subscription(&self) -> Result<(), AdapterError> {
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Zellij adapter is shut down; initial subscription cannot be refreshed",
            ));
        }
        if self.inner.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij adapter is suspended; activation resume owns the subscription",
            ));
        }
        let _transition = self.inner.registration_transition.lock().await;
        let generation = self.inner.generation.advance()?;
        self.invalidate_all_registrations(
            "Zellij event subscription was replaced before completion",
        )
        .await;
        self.inner.register_epoch.lock().await.clear();
        self.inner
            .event
            .respawn_with_payload(subscription_payload(generation)?)
            .await
            .map_err(|error| transport_error(&error))
    }

    /// Fresh compatible census for the readiness hook: client IDs holding a
    /// compatible record in the current evidence generation. Private; the
    /// typed `activation_readiness` hook is the only consumer surface.
    async fn fresh_census(&self) -> Vec<String> {
        let registry = self.inner.registry.lock().await;
        let stamps = self.inner.register_epoch.lock().await;
        let epoch = self.inner.resume_epoch.load(Ordering::SeqCst);
        let mut census: Vec<String> = stamps
            .iter()
            .filter(|(client, stamped)| {
                stamped.0 == epoch && registry.get(client).is_some_and(|record| record.compatible)
            })
            .map(|(client, _)| client.clone())
            .collect();
        census.sort();
        census
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
    async fn emit(&self, event: AdapterHealthEvent) {
        let event = match event {
            AdapterHealthEvent::DispatchCompleted(completion)
                if dispatch_completion_execution(&completion).0 >= LOCAL_EXECUTION_CEILING =>
            {
                let execution = dispatch_completion_execution(&completion);
                let waiter = self
                    .inner
                    .close_waiters
                    .lock()
                    .expect("Zellij close waiter registry is not poisoned")
                    .remove(&execution.0);
                if let Some(waiter) = waiter {
                    let _ = waiter.send(completion);
                }
                return;
            }
            event => event,
        };
        let wake = self.inner.quiesce_wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        #[cfg(test)]
        {
            let hook = self
                .inner
                .emit_wait_hook
                .lock()
                .expect("Zellij emit hook is not poisoned")
                .clone();
            if let Some(hook) = hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
        }
        if self.inner.quiescing.load(Ordering::Acquire) {
            return;
        }
        tokio::select! {
            result = self.inner.events_tx.send(event) => {
                let _ = result;
            }
            () = wake => {}
        };
    }

    fn mint_local_id(&self) -> [u8; 16] {
        self.inner.local_tokens.mint()
    }
    fn mint_local_execution(&self) -> ExecutionId {
        // Reserved-namespace proof: adapter ids stay at or above
        // `LOCAL_EXECUTION_CEILING` (`1 << 63` OR the counter), while broker
        // ids stay below it (a bare u64 counter), and adapter admission
        // rejects any broker execution in the reserved range.
        let counter = self
            .inner
            .next_local_execution
            .fetch_add(1, Ordering::Relaxed);
        debug_assert!(
            counter < LOCAL_EXECUTION_CEILING,
            "local execution counter must stay below the reserved ceiling"
        );
        ExecutionId(LOCAL_EXECUTION_CEILING | counter)
    }

    fn correlation(&self) -> ExecutionCorrelationId {
        ExecutionCorrelationId::new(format!(
            "zellij-{}",
            self.inner.next_correlation.fetch_add(1, Ordering::Relaxed)
        ))
    }

    async fn event_loop(&self) {
        // Consecutive failures back off so a dead channel can never busy-spin
        // the runtime and starve dispatch work on the same thread.
        let mut failures: u32 = 0;
        // Lease-expiry sweep runs on this loop so an idle captured client
        // cannot stay stale without a caller: the availability gates
        // re-check the same sweep for callers racing the timer. The first
        // tick fires immediately and is consumed below so expiry is judged
        // over full periods. Shutdown drops the loop and the timer with it;
        // suspend pauses the sweep inside the helper.
        let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
        sweep.tick().await;
        loop {
            if self.inner.shutdown.load(Ordering::Relaxed)
                || self.inner.quiescing.load(Ordering::Acquire)
            {
                return;
            }
            tokio::select! {
                result = self.inner.event.next_line_tagged() => {
                    if let Ok((channel, line)) = result {
                        failures = 0;
                        self.handle_event_line(channel, &line).await;
                    } else {
                        if self.inner.shutdown.load(Ordering::Relaxed)
                            || self.inner.quiescing.load(Ordering::Acquire)
                        {
                            return;
                        }
                        failures = failures.saturating_add(1);
                        self.restart_whole_pipe().await;
                        let backoff = Duration::from_millis(10).saturating_mul(failures.min(100));
                        tokio::time::sleep(backoff).await;
                    }
                }
                _ = sweep.tick() => {
                    self.sweep_expired_clients().await;
                }
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the exhaustive event/provenance dispatch stays co-located for protocol review"
    )]
    async fn handle_event_line(&self, channel: u64, line: &str) {
        let Ok(frame) = decode_event_line(line) else {
            self.restart_whole_pipe().await;
            return;
        };
        if frame.channel_generation != self.inner.generation.current() {
            return;
        }
        let request_id = frame.request_id;
        let registration = frame.registration;
        match frame.event {
            PipeEventKind::Event(BridgeEvent::Register {
                registration: details,
            }) => {
                self.on_register(
                    channel,
                    frame.channel_generation,
                    details.client_id,
                    details.current_pane,
                    registration,
                    details.identity,
                )
                .await;
            }
            event => {
                let client_id = self
                    .inner
                    .registry
                    .lock()
                    .await
                    .client_for_registration(registration)
                    .map(str::to_owned);
                let Some(client_id) = client_id else {
                    return;
                };
                match event {
                    PipeEventKind::Response(BridgeResponse::RequestReleased) => {
                        self.on_released(
                            request_id.expect("validated solicited event"),
                            frame.channel_generation,
                            registration,
                        )
                        .await;
                    }
                    PipeEventKind::Response(BridgeResponse::DispatchCompleted {
                        execution,
                        outcome,
                    }) => {
                        self.on_completed(
                            request_id.expect("validated solicited event"),
                            registration,
                            execution,
                            outcome,
                        )
                        .await;
                    }
                    PipeEventKind::Response(BridgeResponse::OriginSnapshot {
                        ui_session,
                        origin,
                    }) => {
                        let request = RequestProvenance {
                            request_id: request_id.expect("validated solicited event"),
                            registration,
                        };
                        let mut pending = self.inner.pending_origin.lock().await;
                        if pending
                            .get(ui_session.as_str())
                            .is_some_and(|reply| reply.request == Some(request))
                            && let Some(reply) = pending.remove(ui_session.as_str())
                        {
                            let _ = reply.sender.send(Ok(origin));
                        }
                    }
                    PipeEventKind::Response(BridgeResponse::OriginDeclined { ui_session }) => {
                        let request = RequestProvenance {
                            request_id: request_id.expect("validated solicited event"),
                            registration,
                        };
                        let mut pending = self.inner.pending_origin.lock().await;
                        if pending
                            .get(ui_session.as_str())
                            .is_some_and(|reply| reply.request == Some(request))
                            && let Some(reply) = pending.remove(ui_session.as_str())
                        {
                            let _ = reply.sender.send(Err(OriginError::InvalidId {
                                field: "ui-pane",
                                reason: "bridge declined ownership of the pane",
                            }));
                        }
                    }
                    PipeEventKind::Response(BridgeResponse::CaptureReady { lease, state }) => {
                        let request = RequestProvenance {
                            request_id: request_id.expect("validated solicited event"),
                            registration,
                        };
                        let mut pending = self.inner.pending_capture.lock().await;
                        if pending
                            .get(&lease.0)
                            .is_some_and(|(_, reply)| reply.request == Some(request))
                            && let Some((_, reply)) = pending.remove(&lease.0)
                        {
                            let _ = reply.sender.send(Ok(state.prior_mode));
                        }
                    }
                    PipeEventKind::Event(BridgeEvent::CaptureLost { lease, reason }) => {
                        self.inner.pending_capture.lock().await.remove(&lease.0);
                        let loss = match reason {
                            muxe_zellij_protocol::CaptureLostReason::UserModeChanged => {
                                muxe_adapter_api::CaptureLossReason::UserModeChanged
                            }
                            muxe_zellij_protocol::CaptureLostReason::BrokerLeaseExpired => {
                                muxe_adapter_api::CaptureLossReason::BrokerLeaseExpired
                            }
                            muxe_zellij_protocol::CaptureLostReason::BridgeUnloading
                            | muxe_zellij_protocol::CaptureLostReason::AdapterHealth => {
                                muxe_adapter_api::CaptureLossReason::AdapterHealth
                            }
                        };
                        self.emit(AdapterHealthEvent::CaptureLost {
                            lease: ApiCaptureLease {
                                id: CaptureLeaseId::new(hex_id(&lease.0)),
                                ui_session: UiSessionId::new("unknown"),
                                modal_scope: ModalScopeId::new("unknown"),
                            },
                            reason: loss,
                        })
                        .await;
                    }
                    PipeEventKind::Event(BridgeEvent::Heartbeat) => {
                        let now = self.clock_millis();
                        let _ = self.inner.registry.lock().await.heartbeat(
                            &client_id,
                            registration,
                            now,
                        );
                    }
                    PipeEventKind::Response(
                        BridgeResponse::DispatchAccepted { .. } | BridgeResponse::Host(_),
                    )
                    | PipeEventKind::Event(
                        BridgeEvent::Register { .. }
                        | BridgeEvent::Health { .. }
                        | BridgeEvent::Host(_),
                    ) => {}
                }
            }
        }
    }

    /// Records one fresh bridge registration, stamped with the observing
    /// resume generation and event-channel install epoch captured at receipt.
    ///
    /// Registrations retired by channel replacement are rejected even if
    /// replayed on the new delivery boundary: every event subscription requires
    /// the bridge to mint a new unpredictable identity.
    async fn on_register(
        &self,
        channel: u64,
        channel_generation: ChannelGeneration,
        client_id: String,
        current_pane: Option<String>,
        registration: RegistrationId,
        identity: muxe_zellij_protocol::BridgeIdentity,
    ) {
        let compatible = identity.bridge_build_id == Some(bridge_build_id())
            && identity.source_revision == pinned_source_revision()
            && identity.action_fingerprint == generated_action_fingerprint().0
            && identity.protocol_fingerprint == bridge_protocol_fingerprint().0
            && identity.muxe_version == env!("CARGO_PKG_VERSION");
        let transition = self.inner.registration_transition.lock().await;
        if channel_generation != self.inner.generation.current()
            || self.inner.event.install_epoch().await != Some(channel)
        {
            return;
        }
        let now = self.clock_millis();
        let registered = self.inner.registry.lock().await.register(
            &client_id,
            registration,
            current_pane,
            identity.muxe_version,
            compatible,
            now,
        );
        let Ok(displaced) = registered else {
            return;
        };
        if let Some(displaced) = displaced {
            self.retire_registration_state(
                &client_id,
                displaced,
                "Zellij bridge registration was replaced before completion",
            )
            .await;
        }
        // Stamp receipt, not execution: `channel` was fixed when the line
        // arrived, so coverage can require current-attempt evidence per
        // member without cross-attempt reuse.
        let epoch = self.inner.resume_epoch.load(Ordering::SeqCst);
        self.inner
            .register_epoch
            .lock()
            .await
            .insert(client_id.clone(), (epoch, channel));
        // Wake a resume awaiting fresh post-suspend registrations.
        self.inner.registry_notify.notify_waiters();
        drop(transition);
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
        request_id: RequestId,
        channel_generation: ChannelGeneration,
        registration: RegistrationId,
    ) {
        let matches = self
            .inner
            .in_flight
            .lock()
            .await
            .as_ref()
            .is_some_and(|pending| {
                pending.request.request_id == request_id
                    && pending.generation == channel_generation
                    && pending.request.registration == registration
            });
        if !matches {
            // Late acknowledgement from a restarted channel: ignore.
            return;
        }
        let mut in_flight = self.inner.in_flight.lock().await;
        if let Some(pending) = in_flight.as_mut()
            && pending.request.request_id == request_id
            && pending.generation == channel_generation
            && pending.request.registration == registration
        {
            pending.settled = true;
        }
        *in_flight = None;
        drop(in_flight);
        self.pump_all().await;
    }

    async fn on_completed(
        &self,
        request_id: RequestId,
        registration: RegistrationId,
        execution: CommonExecutionId,
        outcome: muxe_zellij_protocol::CommandOutcome,
    ) {
        let Some(execution) = common_to_core(&execution) else {
            // Completion for an encoding no broker execution can own.
            return;
        };
        let terminal = {
            let mut live = self.inner.live_executions.lock().await;
            let Some(record) = live.get_mut(&execution.0) else {
                // Completion for an unsent, forgotten, or displaced execution.
                return;
            };
            let provenance = match record.requests.get(&request_id) {
                Some(Some(provenance)) => *provenance,
                // Completion for a request outside this execution's set,
                // an unsent (still queued) request, or a duplicate of an
                // already-settled request (its key is gone): no-op.
                Some(None) | None => return,
            };
            if provenance.registration != registration {
                // Completion from a displaced registration.
                return;
            }
            record.requests.remove(&request_id);
            let done = match outcome.status {
                muxe_zellij_protocol::CommandStatus::Succeeded => record.note_success(),
                muxe_zellij_protocol::CommandStatus::Failed => record.note_failure(
                    OutcomeRank::Failed,
                    AdapterError::new(AdapterErrorKind::DispatchFailed, outcome.detail),
                ),
            };
            if !done {
                return;
            }
            let terminal = record.terminal();
            live.remove(&execution.0);
            terminal
        };
        self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
            .await;
    }

    /// Settles one outstanding request slot, folding `outcome` into its
    /// execution aggregate. When the slot is the last unsettled request the
    /// record's single terminal is removed and returned; otherwise `None`
    /// (still outstanding siblings). Unknown slots (never sent, already
    /// settled, or already removed) return `None` and change nothing, so
    /// every interruption path resolves each request exactly once.
    fn settle_slot(
        live: &mut BTreeMap<u64, ExecutionRecord>,
        execution: ExecutionId,
        slot: RequestId,
        outcome: Option<(OutcomeRank, AdapterError)>,
    ) -> Option<DispatchCompletion> {
        let record = live.get_mut(&execution.0)?;
        // The slot must still be outstanding (never-sent `None` and
        // sent `Some` both count). Settled slots were removed, so a second
        // settle of the same slot finds no key and changes nothing: each
        // request resolves exactly once.
        record.requests.remove(&slot)?;
        let done = match outcome {
            None => record.note_success(),
            Some((rank, error)) => record.note_failure(rank, error),
        };
        if !done {
            return None;
        }
        let terminal = record.terminal();
        live.remove(&execution.0);
        Some(terminal)
    }

    /// Settles one outstanding request of `execution` regardless of slot,
    /// for queued items whose slot never reached the record. Prefers the
    /// acceptance slot, then any outstanding slot; returns the terminal when
    /// the execution reaches all-terminal.
    fn settle_any_slot(
        live: &mut BTreeMap<u64, ExecutionRecord>,
        execution: ExecutionId,
        outcome: Option<(OutcomeRank, AdapterError)>,
    ) -> Option<DispatchCompletion> {
        let record = live.get_mut(&execution.0)?;
        let slot = record.requests.keys().copied().next()?;
        Self::settle_slot(live, execution, slot, outcome)
    }

    /// Settles every outstanding request matching `predicate` as `OutcomeUnknown`
    /// with `reason`, publishing the aggregate terminal for each execution that
    /// reaches all-terminal and removing it. Returns the terminal completions
    /// so callers that must route them elsewhere (the local close-waiter
    /// table) can do so; broker-owned executions are emitted inline.
    async fn settle_matching_unknown(
        &self,
        predicate: impl Fn(RequestProvenance) -> bool,
        reason: &str,
    ) -> Vec<DispatchCompletion> {
        let terminals: Vec<DispatchCompletion> = {
            let mut live = self.inner.live_executions.lock().await;
            let mut terminals = Vec::new();
            let mut remove = Vec::new();
            for record in live.values_mut() {
                // Only sent slots carry provenance to match; never-sent
                // (`None`) slots belong to the queue drain, which settles by
                // slot key instead.
                let matched: Vec<RequestId> = record
                    .requests
                    .iter()
                    .filter_map(|(slot, provenance)| {
                        provenance
                            .filter(|provenance| predicate(*provenance))
                            .map(|_| *slot)
                    })
                    .collect();
                let unknown = matched.len() as u64;
                for slot in matched {
                    record.requests.remove(&slot);
                }
                let mut done = false;
                for _ in 0..unknown {
                    done = record.note_failure(
                        OutcomeRank::Unknown,
                        AdapterError::new(AdapterErrorKind::OutcomeUnknown, reason),
                    );
                }
                if unknown > 0 && done {
                    terminals.push(record.terminal());
                    remove.push(record.execution.0);
                }
            }
            for execution in &remove {
                live.remove(execution);
            }
            terminals
        };
        let mut locals = Vec::new();
        for terminal in terminals {
            let execution = dispatch_completion_execution(&terminal);
            if execution.0 >= LOCAL_EXECUTION_CEILING {
                locals.push(terminal);
            } else {
                self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                    .await;
            }
        }
        locals
    }
    async fn retire_registration_state(
        &self,
        client_id: &str,
        registration: RegistrationId,
        reason: &str,
    ) {
        {
            let mut in_flight = self.inner.in_flight.lock().await;
            if in_flight
                .as_ref()
                .is_some_and(|pending| pending.request.registration == registration)
            {
                *in_flight = None;
            }
        }

        for terminal in self
            .settle_matching_unknown(|provenance| provenance.registration == registration, reason)
            .await
        {
            self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                .await;
        }

        self.inner.pending_origin.lock().await.retain(|_, reply| {
            reply
                .request
                .is_none_or(|request| request.registration != registration)
        });
        let beginning_lease = match self.inner.captures.lock().await.state(client_id) {
            crate::capture::CaptureState::Beginning { lease, .. } => Some(*lease),
            crate::capture::CaptureState::Idle | crate::capture::CaptureState::Captured(_) => None,
        };
        let preserve_capture = {
            let mut preserve = false;
            self.inner
                .pending_capture
                .lock()
                .await
                .retain(|lease, (owner, reply)| {
                    if reply
                        .request
                        .is_some_and(|request| request.registration == registration)
                    {
                        return false;
                    }
                    if owner != client_id {
                        return true;
                    }
                    let keep = reply.request.is_none() && beginning_lease == Some(*lease);
                    preserve |= keep;
                    keep
                });
            preserve
        };
        self.inner
            .pending_leases
            .lock()
            .expect("Zellij pending lease registry is not poisoned")
            .retain(|_, (_, owner)| *owner != registration);
        self.inner
            .pane_claims
            .lock()
            .await
            .retain(|_, owner| owner != client_id);

        let displaced = if preserve_capture {
            None
        } else {
            self.inner
                .captures
                .lock()
                .await
                .invalidate_client(client_id)
        };
        let capture = displaced.and_then(|state| match state {
            crate::capture::CaptureState::Beginning { ui_session, lease } => {
                Some((ui_session, lease))
            }
            crate::capture::CaptureState::Captured(record) => {
                Some((record.ui_session, record.lease))
            }
            crate::capture::CaptureState::Idle => None,
        });
        if let Some((ui_session, lease)) = capture {
            self.emit(AdapterHealthEvent::CaptureLost {
                lease: ApiCaptureLease {
                    id: CaptureLeaseId::new(hex_id(&lease)),
                    ui_session: UiSessionId::new(ui_session),
                    modal_scope: Self::scope_for_client(client_id),
                },
                reason: muxe_adapter_api::CaptureLossReason::AdapterHealth,
            })
            .await;
        }
    }

    async fn invalidate_all_registrations(&self, reason: &str) {
        let displaced = self.inner.registry.lock().await.invalidate_all();
        for (client_id, registration) in displaced {
            self.retire_registration_state(&client_id, registration, reason)
                .await;
        }
    }

    async fn enqueue(&self, item: QueuedItem) {
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
        let transition = self.inner.registration_transition.lock().await;
        if self.inner.shutdown.load(Ordering::Relaxed)
            || self.inner.suspended.load(Ordering::SeqCst)
            || self.inner.in_flight.lock().await.is_some()
        {
            return;
        }
        let mut order: Vec<String> = {
            let queues = self.inner.queues.lock().await;
            queues
                .iter()
                .filter(|(_, queue)| !queue.is_empty())
                .map(|(client, _)| client.clone())
                .collect()
        };
        order.sort();
        if let Some(last) = self.inner.scheduler_cursor.lock().await.as_ref()
            && let Some(index) = order.iter().position(|client| client == last)
            && !order.is_empty()
        {
            let start = (index + 1) % order.len();
            order.rotate_left(start);
        }
        for client in order {
            match self.pump_one(&client).await {
                SendItemResult::Accepted => {
                    *self.inner.scheduler_cursor.lock().await = Some(client);
                    return;
                }
                SendItemResult::RestartWhole => {
                    drop(transition);
                    self.restart_whole_pipe().await;
                    return;
                }
                SendItemResult::Continue => {}
            }
        }
    }

    async fn pump_one(&self, client_id: &str) -> SendItemResult {
        // One request-child respawn retry inline. Lifecycle payloads retry
        // only when their semantics are idempotent; dispatch never replays.
        for _ in 0..2 {
            let mut item = {
                let mut queues = self.inner.queues.lock().await;
                match queues.get_mut(client_id).and_then(VecDeque::pop_front) {
                    Some(item) => item,
                    None => return SendItemResult::Continue,
                }
            };
            let request = self.inner.registry.lock().await.allocate_request(client_id);
            let Ok((registration, request_id)) = request else {
                self.inner
                    .queues
                    .lock()
                    .await
                    .entry(client_id.to_owned())
                    .or_default()
                    .push_front(item);
                return SendItemResult::Continue;
            };
            let provenance = RequestProvenance {
                request_id,
                registration,
            };
            // Stamp the live slot if the item still holds its acceptance
            // slot: requeue retries keep the minted slot (re-minting would
            // orphan the record key), while a foreign item (tests) with no
            // slot borrows the minted id.
            if item.execution.is_some() && item.request_slot.is_none() {
                item.request_slot = Some(request_id);
            }
            match self.send_item(client_id, provenance, item).await {
                SendItemResult::Accepted => return SendItemResult::Accepted,
                SendItemResult::RestartWhole => return SendItemResult::RestartWhole,
                SendItemResult::Continue => {}
            }
        }
        SendItemResult::Continue
    }

    /// Sends one queued item and reports transport recovery needed by the scheduler.
    #[expect(
        clippy::too_many_lines,
        reason = "request framing, provenance publication, and ambiguous-write handling are one transaction"
    )]
    async fn send_item(
        &self,
        client_id: &str,
        request: RequestProvenance,
        mut item: QueuedItem,
    ) -> SendItemResult {
        item.client_id = client_id.to_owned();
        let generation = self.inner.generation.current();
        let Some(payload) = item.payload.take() else {
            self.emit(AdapterHealthEvent::Unhealthy {
                modal_scope: Some(Self::scope_for_client(client_id)),
                error: AdapterError::new(
                    AdapterErrorKind::InvalidRequest,
                    "queued item lost its payload",
                ),
            })
            .await;
            if let (Some(execution), Some(slot)) = (item.execution, item.request_slot) {
                let terminal = {
                    let mut live_executions = self.inner.live_executions.lock().await;
                    Self::settle_slot(
                        &mut live_executions,
                        execution,
                        slot,
                        Some((
                            OutcomeRank::Unknown,
                            AdapterError::new(
                                AdapterErrorKind::OutcomeUnknown,
                                "queued Zellij request lost its payload before send",
                            ),
                        )),
                    )
                };
                if let Some(terminal) = terminal {
                    self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                        .await;
                }
            }
            return SendItemResult::Continue;
        };
        let replay_safe = !matches!(&payload, BridgeRequest::Dispatch { .. });
        if let BridgeRequest::BeginCapture { lease, .. } = &payload
            && let Some((_, pending)) = self.inner.pending_capture.lock().await.get_mut(&lease.0)
        {
            pending.request = Some(request);
        }
        if let BridgeRequest::RequestOrigin { ui_session, .. } = &payload
            && let Some(pending) = self
                .inner
                .pending_origin
                .lock()
                .await
                .get_mut(ui_session.as_str())
        {
            pending.request = Some(request);
        }
        let frame = PipeRequest {
            protocol: muxe_zellij_protocol::BRIDGE_PROTOCOL_VERSION,
            request_id: request.request_id,
            registration: request.registration,
            channel_generation: generation,
            target: muxe_zellij_protocol::BridgeTarget {
                client_id: client_id.to_owned(),
            },
            payload,
        };
        let line = match encode_request_line(&frame) {
            Ok(line) => line,
            Err(error) => {
                self.emit(AdapterHealthEvent::Unhealthy {
                    modal_scope: Some(Self::scope_for_client(client_id)),
                    error: AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string()),
                })
                .await;
                if let (Some(execution), Some(slot)) = (item.execution, item.request_slot) {
                    // Encoding failed before the transport minted provenance:
                    // the never-sent slot resolves unknown against the
                    // acceptance key. A last-slot failure publishes the
                    // aggregate terminal once; siblings still queue behind
                    // their own slots.
                    let terminal = {
                        let mut live_executions = self.inner.live_executions.lock().await;
                        Self::settle_slot(
                            &mut live_executions,
                            execution,
                            slot,
                            Some((
                                OutcomeRank::Unknown,
                                AdapterError::new(
                                    AdapterErrorKind::OutcomeUnknown,
                                    "Zellij request encoding failed before send",
                                ),
                            )),
                        )
                    };
                    if let Some(terminal) = terminal {
                        self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                            .await;
                    }
                }
                return SendItemResult::Continue;
            }
        };
        let PipeRequest { payload, .. } = frame;
        item.payload = Some(payload);
        if let Some(execution) = item.execution {
            let mut live_executions = self.inner.live_executions.lock().await;
            if let Some(record) = live_executions.get_mut(&execution.0) {
                // Rename the acceptance slot to the minted request id on
                // first send; retries keep the minted key.
                if let Some(slot) = item.request_slot
                    && slot != request.request_id
                    && record.requests.remove(&slot).is_some()
                {
                    record.requests.insert(request.request_id, Some(request));
                    item.request_slot = Some(request.request_id);
                } else if let Some(slot) = item.request_slot {
                    record.requests.insert(slot, Some(request));
                } else {
                    record.requests.insert(request.request_id, Some(request));
                    item.request_slot = Some(request.request_id);
                }
            }
        }
        *self.inner.in_flight.lock().await = Some(InFlight {
            request,
            generation,
            execution: item.execution,
            request_slot: item.request_slot,
            settled: false,
        });
        if let Err(error) = self.inner.request.send_line(line).await {
            let matches = self
                .inner
                .in_flight
                .lock()
                .await
                .as_ref()
                .is_some_and(|pending| {
                    pending.request.request_id == request.request_id
                        && pending.request.registration == request.registration
                });
            if matches {
                *self.inner.in_flight.lock().await = None;
            }
            // A state-changing dispatch may have reached the host and is never
            // replayed. Lifecycle payloads retry only when their payload kind
            // is explicitly classified as idempotent.
            if let Some(execution) = item.execution {
                let slot = item.request_slot.unwrap_or(request.request_id);
                let terminal = {
                    let mut live_executions = self.inner.live_executions.lock().await;
                    Self::settle_slot(
                        &mut live_executions,
                        execution,
                        slot,
                        Some((OutcomeRank::Unknown, transport_error(&error))),
                    )
                };
                if let Some(terminal) = terminal {
                    self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                        .await;
                }
            } else if !replay_safe {
                self.emit(AdapterHealthEvent::Unhealthy {
                    modal_scope: Some(Self::scope_for_client(client_id)),
                    error: AdapterError::new(
                        AdapterErrorKind::OutcomeUnknown,
                        "Zellij dispatch write failed; action outcome is unknown",
                    ),
                })
                .await;
            }
            if replay_safe {
                self.inner
                    .queues
                    .lock()
                    .await
                    .entry(client_id.to_owned())
                    .or_default()
                    .push_front(item);
            }
            return if self.replace_request_child().await {
                SendItemResult::Continue
            } else {
                SendItemResult::RestartWhole
            };
        }
        self.watch_release(request, generation);
        SendItemResult::Accepted
    }

    fn watch_release(&self, request: RequestProvenance, generation: ChannelGeneration) {
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
                    pending.request.request_id == request.request_id
                        && pending.request.registration == request.registration
                        && pending.generation == generation
                });
            if !stuck {
                return;
            }
            let pending = inner.in_flight.lock().await.take();
            if let Some(pending) = pending
                && let Some(execution) = pending.execution
                && !pending.settled
            {
                let slot = pending.request_slot.unwrap_or(pending.request.request_id);
                let terminal = {
                    let mut live = inner.live_executions.lock().await;
                    ZellijAdapter::settle_slot(
                        &mut live,
                        execution,
                        slot,
                        Some((
                            OutcomeRank::Unknown,
                            AdapterError::new(
                                AdapterErrorKind::OutcomeUnknown,
                                "Zellij bridge did not release the request pipe in time",
                            ),
                        )),
                    )
                };
                if let Some(terminal) = terminal {
                    adapter
                        .emit(AdapterHealthEvent::DispatchCompleted(terminal))
                        .await;
                }
            }
            // Never recover the transport while suspended for activation:
            // resume reinstalls both children after revalidation.
            if inner.shutdown.load(Ordering::Relaxed)
                || inner.quiescing.load(Ordering::Acquire)
                || inner.suspended.load(Ordering::SeqCst)
            {
                return;
            }
            let _ = inner.request.respawn().await;
            adapter.pump_all().await;
        });
    }

    async fn replace_request_child(&self) -> bool {
        if self.inner.shutdown.load(Ordering::Relaxed)
            || self.inner.quiescing.load(Ordering::Acquire)
            || self.inner.suspended.load(Ordering::SeqCst)
        {
            return false;
        }
        // No pump here: the caller's retry loop re-attempts the head item, so
        // replacing never recurses back through pump_all.
        self.inner.request.respawn().await.is_ok()
    }

    async fn restart_whole_pipe(&self) {
        if self.inner.shutdown.load(Ordering::Relaxed)
            || self.inner.quiescing.load(Ordering::Acquire)
            || self.inner.suspended.load(Ordering::SeqCst)
        {
            return;
        }
        let _transition = self.inner.registration_transition.lock().await;
        let Ok(generation) = self.inner.generation.advance() else {
            self.emit(AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Zellij channel generation exhausted",
                ),
            })
            .await;
            return;
        };
        // The in-flight request may have reached the host: outcome unknown, never replayed.
        if let Some(pending) = self.inner.in_flight.lock().await.take()
            && let Some(execution) = pending.execution
            && !pending.settled
        {
            let slot = pending.request_slot.unwrap_or(pending.request.request_id);
            let terminal = {
                let mut live = self.inner.live_executions.lock().await;
                Self::settle_slot(
                    &mut live,
                    execution,
                    slot,
                    Some((
                        OutcomeRank::Unknown,
                        AdapterError::new(
                            AdapterErrorKind::OutcomeUnknown,
                            "event pipe failed while a request was in flight",
                        ),
                    )),
                )
            };
            if let Some(terminal) = terminal {
                self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                    .await;
            }
        }
        self.invalidate_all_registrations("Zellij event pipe failed before completion")
            .await;
        self.inner.captures.lock().await.invalidate_all_clients();
        self.inner.pane_claims.lock().await.clear();
        self.inner.snapshots.lock().await.clear();
        let request_ok = self.inner.request.respawn().await.is_ok();
        let event_ok = match subscription_payload(generation) {
            Ok(payload) => self.inner.event.respawn_with_payload(payload).await.is_ok(),
            Err(_) => false,
        };
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

    /// Fails closed while suspended for activation or shut down. Every
    /// host-bound operation (capture, origin, dispatch, cleanup) calls this
    /// first so no stale-bridge work proceeds on a subscription about to be
    /// released to an activation target.
    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the public host-adapter error; boxing it would burden every caller"
    )]
    fn require_active(&self) -> Result<(), AdapterError> {
        if self.inner.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij adapter is suspended for activation; host-bound operations are blocked until resume or commit",
            ));
        }
        if self.inner.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Zellij adapter is shut down",
            ));
        }
        Ok(())
    }
    /// Reports whether current-attempt fresh compatible registrations cover
    /// the authoritative membership snapshot for (`epoch`, `channel`). Every
    /// snapshot member must hold a compatible registry record stamped with
    /// exactly this attempt's receipt tag; records from earlier attempts or
    /// displaced channels never count. Additionally, every client stamped in
    /// this attempt must hold a compatible record on this channel, so a
    /// client that attaches after the snapshot and registers mid-attempt is
    /// gated by the normal compatibility handshake instead of slipping past.
    /// A member absent from the snapshot (detached before the query) is not
    /// required; a member present without a registration fails closed.
    async fn resume_covered(&self, epoch: u64, channel: u64, snapshot: &[String]) -> bool {
        // A bridge that registered then went quiet past its lease is dead
        // evidence: expire it before coverage so silence never counts.
        self.sweep_expired_clients().await;
        let registry = self.inner.registry.lock().await;
        let stamps = self.inner.register_epoch.lock().await;
        let fresh_compatible = |client: &str| {
            registry.get(client).is_some_and(|record| record.compatible)
                && stamps
                    .get(client)
                    .is_some_and(|stamped| *stamped == (epoch, channel))
        };
        if !snapshot.iter().all(|client| fresh_compatible(client)) {
            return false;
        }
        stamps
            .iter()
            .filter(|(_, stamped)| stamped.0 == epoch)
            .all(|(client, stamped)| {
                stamped.1 == channel && registry.get(client).is_some_and(|record| record.compatible)
            })
    }
    /// Waits bounded for [`Self::resume_covered`]. Evidence is a fresh
    /// all-required handshake in this attempt: registrations already present
    /// count immediately; later ones wake the wait without polling. Shutdown
    /// fails the wait promptly (`false`) so callers map it without stalling
    /// to the deadline; [`Self::shutdown`] notifies this wait first.
    async fn await_resume_membership(&self, epoch: u64, channel: u64, snapshot: &[String]) -> bool {
        let deadline = tokio::time::Instant::now() + RESUME_READY_TIMEOUT;
        loop {
            // Shutdown wins over a simultaneously completing coverage: no
            // success round may be retained after the transport is gone.
            if self.inner.shutdown.load(Ordering::Relaxed) {
                return false;
            }
            if self.resume_covered(epoch, channel, snapshot).await {
                return true;
            }
            // Pin the wakeup before re-checking so a registration racing
            // this check cannot slip between the check and the wait.
            let wake = self.inner.registry_notify.notified();
            tokio::pin!(wake);
            if self.resume_covered(epoch, channel, snapshot).await {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return self.resume_covered(epoch, channel, snapshot).await;
            }
            // A spurious wake re-loops; the deadline still bounds the wait.
            let _ = tokio::time::timeout(remaining, &mut wake).await;
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

    async fn active_registration(&self, client_id: &str) -> Result<RegistrationId, AdapterError> {
        self.sweep_expired_clients().await;
        let _transition = self.inner.registration_transition.lock().await;
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
    /// Invalidates heartbeat-expired clients: registration, capture, and
    /// pending capture waiters for exactly the expired clients. Queues
    /// pause by absence (no compatible record to send through) and resume
    /// on fresh registration; the shared event channel and healthy
    /// clients are untouched, and no whole-pipe restart follows. An
    /// expired active capture reports `CaptureLost` (adapter health) so
    /// its UI session fails closed, then the client reports `Unhealthy`
    /// with its modal scope. Skipped while suspended or shut down, where
    /// suspend/shutdown teardown already owns registration state. Driven
    /// by the event-loop timer and re-checked at the availability gates.
    ///
    /// Every sweep also renews the bridge-side capture lease of each live
    /// captured client (see [`Self::renew_active_captures`]): the renewal
    /// is an explicit periodic `RenewCapture` line per captured client, so
    /// a healthy-but-idle broker keeps capture alive and only a genuinely
    /// silent broker lets the bridge expire it.
    async fn sweep_expired_clients(&self) {
        if self.inner.shutdown.load(Ordering::Relaxed)
            || self.inner.suspended.load(Ordering::SeqCst)
        {
            return;
        }
        // Scope the transition guard to the expiry section: renewal below
        // enqueues (pump re-acquires the non-reentrant transition lock), so
        // holding the guard across it would deadlock the event loop on the
        // first sweep with a captured client. The renewal snapshots the
        // capture table without holding the guard, so lock order stays
        // transition-then-captures inside expiry and never inverts.
        {
            let _transition = self.inner.registration_transition.lock().await;
            let now = self.clock_millis();
            let expired = self.inner.registry.lock().await.expire_leases(now);
            for (client, registration) in expired {
                self.retire_registration_state(
                    &client,
                    registration,
                    "Zellij bridge heartbeat expired before completion",
                )
                .await;
                self.emit(AdapterHealthEvent::Unhealthy {
                    modal_scope: Some(Self::scope_for_client(&client)),
                    error: AdapterError::new(
                        AdapterErrorKind::Unavailable,
                        format!("Zellij client {client} heartbeat lease expired"),
                    ),
                })
                .await;
            }
        }
        self.renew_active_captures().await;
    }

    /// Renews the bridge-side capture lease of every live captured client.
    ///
    /// One explicit `RenewCapture` line per captured client, emitted on the
    /// sweep cadence (event-loop timer plus availability-gate re-checks, so
    /// renewal cannot stall while a capture is active). The renewal carries
    /// the owning lease; the bridge only extends that exact lease, so a
    /// renewal can never resurrect a displaced or already-ended capture.
    /// Renewal lines are best-effort liveness, not correctness: if they are
    /// lost, the bridge expires the lease and restores the prior mode
    /// itself, and the adapter drops its own record on the next heartbeat
    /// expiry or broker-driven release.
    async fn renew_active_captures(&self) {
        if self.inner.shutdown.load(Ordering::Relaxed)
            || self.inner.suspended.load(Ordering::SeqCst)
        {
            return;
        }
        let renewals: Vec<(String, CommonCaptureLeaseId)> = self
            .inner
            .captures
            .lock()
            .await
            .captured_leases()
            .into_iter()
            .collect();
        for (client_id, lease) in renewals {
            self.enqueue_lifecycle(client_id, BridgeRequest::RenewCapture { lease })
                .await;
        }
    }

    /// Parks both children, drops this attempt's partial evidence, and
    /// reports `Unhealthy` while staying suspended. Every resume failure
    /// funnels here so no partial adapter can overlap the next attempt.
    async fn fail_resume(&self, message: &str) -> AdapterError {
        let _transition = self.inner.registration_transition.lock().await;
        self.inner.request.park().await;
        self.inner.event.park().await;
        self.invalidate_all_registrations("Zellij activation resume failed before completion")
            .await;
        self.inner.register_epoch.lock().await.clear();
        *self.inner.success_snapshot.lock().await = None;
        self.emit(AdapterHealthEvent::Unhealthy {
            modal_scope: None,
            error: AdapterError::new(AdapterErrorKind::Unavailable, message),
        })
        .await;
        AdapterError::new(AdapterErrorKind::Unavailable, message)
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
            let clients: Vec<(String, RegistrationId)> = {
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
                if let Ok(snapshot) = self
                    .request_origin_from(&client_id, registration, ui_session, ui_pane)
                    .await
                {
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
        registration: RegistrationId,
        ui_session: &str,
        ui_pane: &str,
    ) -> Result<ZellijOrigin, OriginError> {
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
                reason: "bridge registration is not active",
            });
        }
        let (sender, receiver) = oneshot::channel();
        self.inner.pending_origin.lock().await.insert(
            ui_session.to_owned(),
            PendingReply {
                request: None,
                sender,
            },
        );
        self.enqueue_lifecycle(
            client_id.to_owned(),
            BridgeRequest::RequestOrigin {
                ui_session: CommonUiSessionId::new(ui_session),
                request: ZellijOriginRequest {
                    ui_pane: ui_pane.to_owned(),
                },
            },
        )
        .await;
        let result = timeout(ORIGIN_ATTEMPT_TIMEOUT, receiver).await;
        self.inner.pending_origin.lock().await.remove(ui_session);
        if let Some(queue) = self.inner.queues.lock().await.get_mut(client_id) {
            queue.retain(|item| {
                !matches!(
                    item.payload.as_ref(),
                    Some(BridgeRequest::RequestOrigin {
                        ui_session: pending,
                        ..
                    }) if pending.as_str() == ui_session
                )
            });
        }
        match result {
            Ok(Ok(snapshot)) => {
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
        if execution.0 >= LOCAL_EXECUTION_CEILING {
            return Err(invalid_request(
                "Zellij broker execution uses the adapter-reserved execution range",
            ));
        }
        // Batch contract: an empty batch is a typed invalid request (never
        // an acceptance with no completion), and a batch of N >= 1 queues one
        // host request per command under this execution. Schema-v1 mappings
        // still produce the single-request case: keyboard key bytes are
        // concatenated before this layer, so `SendKeys` arrives as one
        // command and behaves exactly as before.
        if commands.is_empty() {
            return Err(invalid_request(
                "Zellij execution must resolve to at least one host command",
            ));
        }
        // Gate acceptance on a live compatible registration. Each request
        // mints its own ID only after re-resolving that registration at send
        // time; the slots below are a fresh `INITIAL`-then-`next()` sequence
        // per execution (not the registry counter) so the record owns the
        // full request set before the first send, and the transport renames
        // each slot to its minted ID.
        self.active_registration(&client_id).await?;
        let mut raws = Vec::with_capacity(commands.len());
        for raw in commands {
            ValidatedNativeCommand::try_from(raw.clone()).map_err(|error| {
                AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string())
            })?;
            raws.push(raw);
        }
        let mut slot = RequestId::INITIAL;
        let mut slots = Vec::with_capacity(raws.len());
        for _ in &raws {
            slots.push(slot);
            slot = slot
                .next()
                .map_err(|_| invalid_request("Zellij execution exceeds its request ID space"))?;
        }
        self.inner
            .live_executions
            .lock()
            .await
            .insert(execution.0, ExecutionRecord::new(execution, slots.clone()));
        for (raw, slot) in raws.into_iter().zip(slots) {
            self.inner
                .queues
                .lock()
                .await
                .entry(client_id.clone())
                .or_default()
                .push_back(QueuedItem {
                    execution: Some(execution),
                    request_slot: Some(slot),
                    client_id: client_id.clone(),
                    payload: Some(BridgeRequest::Dispatch {
                        execution: execution_to_common(execution),
                        request: ZellijDispatchRequest::Command(raw),
                    }),
                });
        }
        self.pump_all().await;
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
            .map_err(super::launch::LaunchError::into_adapter_error)?;
        let client_id = match target {
            crate::launch::LaunchTarget::Client(client) => client,
            crate::launch::LaunchTarget::UiPane(pane) => {
                let probe = format!("launch-{}", hex_id(&self.mint_local_id()));
                self.resolve_client_for_pane(&probe, pane.as_str()).await?
            }
        };
        self.dispatch_to_client(execution, client_id, vec![raw])
            .await
    }

    /// Release path for a failed capture setup: drops the table record so no
    /// stale `Beginning` state blocks later attempts, without claiming any
    /// mode restoration (the bridge owns Locked mode and restores it through
    /// its own guarded path). Idempotent: releasing an already-released or
    /// never-held lease is a no-op.
    async fn release_on_setup_failure(&self, client_id: &str, lease: [u8; 16]) {
        let _ = self
            .inner
            .captures
            .lock()
            .await
            .release(client_id, lease, false);
    }

    async fn enqueue_lifecycle(&self, client_id: String, payload: BridgeRequest) {
        self.enqueue(QueuedItem {
            execution: None,
            request_slot: None,
            client_id,
            payload: Some(payload),
        })
        .await;
    }
}

fn execution_to_common(execution: ExecutionId) -> CommonExecutionId {
    let mut bytes = [0; 16];
    bytes[8..].copy_from_slice(&execution.0.to_be_bytes());
    CommonExecutionId(bytes)
}

fn common_to_core(execution: &CommonExecutionId) -> Option<ExecutionId> {
    if execution.0[..8] != [0; 8] {
        return None;
    }
    let bytes: [u8; 8] = execution.0[8..].try_into().ok()?;
    Some(ExecutionId(u64::from_be_bytes(bytes)))
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

fn transport_error(error: &PipeTransportError) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Unavailable, error.to_string())
}

fn invalid_request(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidRequest, message)
}
fn dispatch_completion_execution(completion: &DispatchCompletion) -> ExecutionId {
    match completion {
        DispatchCompletion::Succeeded { execution }
        | DispatchCompletion::Failed { execution, .. }
        | DispatchCompletion::OutcomeUnknown { execution, .. } => *execution,
    }
}
/// Maximum member IDs in one readiness report, mirroring the protocol wire
/// bound (`MAX_READINESS_CLIENTS`): evidence larger than the wire record
/// cannot be served, so an over-bound session reports no per-client
/// evidence and the coordinator gates on adapter health instead. This is
/// the wire's bound, not an adapter invention.
const MAX_READINESS_MEMBERS: usize = 1024;

/// Canonicalizes a retained snapshot round into the authoritative member
/// set: byte-lexicographic order, deduplicated, wire-bounded. An empty
/// snapshot is genuine evidence (`Some([])`): a header-only CLI table
/// proves an empty session, and the coordinator's exact-set predicate
/// admits it. Only an over-bound set reports no evidence (`None`).
/// Member IDs stay opaque here: numeric `u16` validation is the oracle
/// parse boundary's job (non-numeric CLI rows already fail closed there),
/// never a report-time reinterpretation. A count alone could let a
/// newcomer mask a missing member; the exact set cannot.
fn canonical_member_set(mut members: Vec<String>) -> Option<Vec<String>> {
    if members.len() > MAX_READINESS_MEMBERS {
        return None;
    }
    members.sort();
    members.dedup();
    Some(members)
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

impl ZellijAdapter {
    /// Drains every queued item, settling its execution slot unknown and
    /// routing local close terminals through the close-waiter table.
    /// Suspend calls this BEFORE registration retirement so each queued slot
    /// settles exactly once here; the retire pass then only sees sent slots.
    /// Shutdown reuses the same drain for the same reason: queued slots must
    /// settle before the transport parks.
    async fn drain_queued_for_suspend(&self) {
        enum PurgeRoute {
            Waiter(oneshot::Sender<DispatchCompletion>, DispatchCompletion),
            Broker(DispatchCompletion),
        }
        // Purged queued requests settle their execution slots as unknown;
        // executions that reach all-terminal publish their aggregate once.
        // Local close executions route through the close-waiter table (the
        // `close_pending_pane` receiver) instead of the broker event stream.
        // A queued item whose slot never reached its record (seeded or raced
        // fixtures) still settles: the first unknown marks the record's only
        // slot so the waiter can never hang.
        let purged: Vec<(ExecutionId, Option<RequestId>)> = {
            let mut queues = self.inner.queues.lock().await;
            let mut purged = Vec::new();
            for queue in queues.values_mut() {
                for item in queue.drain(..) {
                    if let Some(execution) = item.execution {
                        purged.push((execution, item.request_slot));
                    }
                }
            }
            purged
        };
        {
            let mut live = self.inner.live_executions.lock().await;
            let mut terminals = Vec::new();
            for (execution, slot) in &purged {
                let error = || {
                    (
                        OutcomeRank::Unknown,
                        AdapterError::new(
                            AdapterErrorKind::OutcomeUnknown,
                            "Zellij adapter suspended for activation with a queued request",
                        ),
                    )
                };
                let terminal = match slot {
                    Some(slot) => Self::settle_slot(&mut live, *execution, *slot, Some(error())),
                    None => Self::settle_any_slot(&mut live, *execution, Some(error())),
                };
                if let Some(terminal) = terminal {
                    terminals.push(terminal);
                }
            }
            let routed: Vec<PurgeRoute> = {
                let mut waiters = self
                    .inner
                    .close_waiters
                    .lock()
                    .expect("Zellij close waiter registry is not poisoned");
                let mut routed = Vec::new();
                for terminal in terminals {
                    let execution = dispatch_completion_execution(&terminal);
                    if let Some(waiter) = waiters.remove(&execution.0) {
                        routed.push(PurgeRoute::Waiter(waiter, terminal));
                    } else {
                        routed.push(PurgeRoute::Broker(terminal));
                    }
                }
                routed
            };
            for route in routed {
                match route {
                    PurgeRoute::Waiter(waiter, terminal) => {
                        let _ = waiter.send(terminal);
                    }
                    PurgeRoute::Broker(terminal) => {
                        self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                            .await;
                    }
                }
            }
        }
    }

    /// Suspend tail: park the transport and report `Unhealthy`. Separated so
    /// the queue drain above can own queued slots before retirement runs.
    async fn park_suspended_transport(&self) -> Result<(), AdapterError> {
        self.inner.request.park().await;
        self.inner.event.park().await;
        self.emit(AdapterHealthEvent::Unhealthy {
            modal_scope: None,
            error: AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij pipe transport suspended for activation",
            ),
        })
        .await;
        Ok(())
    }
}

#[async_trait]
#[expect(
    clippy::too_many_lines,
    reason = "adapter-contract trait impl: begin_capture's setup/timeout arms plus the capture and origin surface stay co-located for protocol review"
)]
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
        self.require_active()?;
        // A throwaway session namespace keeps the claim fan-out keyed without
        // allocating a broker session.
        let probe_session = format!("claim-{}", hex_id(&self.mint_local_id()));
        let client = self
            .resolve_client_for_pane(&probe_session, ui_pane.as_str())
            .await?;
        Ok(Self::scope_for_client(&client))
    }

    async fn begin_capture(
        &self,
        request: CaptureRequest,
    ) -> Result<ApiCaptureLease, AdapterError> {
        self.require_active()?;
        let client_id = Self::client_for_scope(&request.modal_scope)?;
        // Guard: a live compatible bridge must own the client before capture.
        let _registration = self.active_registration(&client_id).await?;
        let lease = self.mint_local_id();
        {
            let mut captures = self.inner.captures.lock().await;
            captures
                .begin(&client_id, request.ui_session.as_str(), lease)
                .map_err(|error| {
                    AdapterError::new(AdapterErrorKind::Unavailable, error.to_string())
                })?;
        }
        let (sender, receiver) = oneshot::channel();
        self.inner.pending_capture.lock().await.insert(
            lease,
            (
                client_id.clone(),
                PendingReply {
                    request: None,
                    sender,
                },
            ),
        );
        self.enqueue_lifecycle(
            client_id.clone(),
            BridgeRequest::BeginCapture {
                lease: CommonCaptureLeaseId(lease),
                ui_session: CommonUiSessionId::new(request.ui_session.as_str()),
            },
        )
        .await;
        // `CaptureReady` arrived: confirm, but re-check the table first. A
        // concurrent `CaptureLost` (user mode change, bridge unload, or the
        // bridge-side lease expiring the capture itself) may have released
        // the record while the ready line was in flight; confirming then
        // would resurrect a stale lease. On that race the setup fails and
        // the release attempt below still runs so a bridge that did enter
        // Locked mode is told to restore.
        if let Ok(Ok(Ok(prior_mode))) = timeout(CAPTURE_TIMEOUT, receiver).await {
            let confirmed = self
                .inner
                .captures
                .lock()
                .await
                .confirm(&client_id, lease, prior_mode)
                .is_ok();
            if confirmed {
                return Ok(ApiCaptureLease {
                    id: CaptureLeaseId::new(hex_id(&lease)),
                    ui_session: request.ui_session,
                    modal_scope: request.modal_scope,
                });
            }
            self.release_on_setup_failure(&client_id, lease).await;
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij Locked-mode capture was lost before setup completed",
            ));
        }
        let transition = self.inner.registration_transition.lock().await;
        let sent = self
            .inner
            .pending_capture
            .lock()
            .await
            .remove(&lease)
            .is_some_and(|(_, reply)| reply.request.is_some());
        if !sent && let Some(queue) = self.inner.queues.lock().await.get_mut(&client_id) {
            queue.retain(|item| {
                !matches!(
                    item.payload.as_ref(),
                    Some(BridgeRequest::BeginCapture { lease: pending, .. })
                        if pending.0 == lease
                )
            });
        }
        // Timeout (or dropped waiter): the bridge may still have entered
        // Locked mode, so attempt the release path rather than relying on
        // the best-effort `EndCapture` line for correctness. Clearing the
        // table record first is what unblocks later attempts; the line
        // itself only asks the bridge to restore.
        self.release_on_setup_failure(&client_id, lease).await;
        drop(transition);
        if sent {
            self.enqueue_lifecycle(
                client_id,
                BridgeRequest::EndCapture {
                    lease: CommonCaptureLeaseId(lease),
                    reason: CaptureEndReason::LeaseExpired,
                },
            )
            .await;
        }
        Err(AdapterError::new(
            AdapterErrorKind::Unavailable,
            "timed out waiting for Zellij Locked-mode capture",
        ))
    }

    async fn end_capture(
        &self,
        lease: ApiCaptureLease,
        reason: CaptureReleaseReason,
    ) -> Result<(), AdapterError> {
        self.require_active()?;
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
                // `Busy` only comes from `begin`; listed for exhaustiveness.
                Ok(_)
                | Err(
                    crate::capture::CaptureError::StaleLease
                    | crate::capture::CaptureError::NotCaptured
                    | crate::capture::CaptureError::Busy,
                ) => {}
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
                    lease: CommonCaptureLeaseId(lease_id),
                    reason: end_reason,
                },
            )
            .await;
        }
        Ok(())
    }

    async fn register_pending_pane(
        &self,
        registration: PendingPaneRegistration,
    ) -> Result<PendingPaneLease, AdapterError> {
        self.require_active()?;
        if registration.temporary_tab.is_some() {
            return Err(invalid_request(
                "Zellij pending panes cannot carry a temporary tab",
            ));
        }
        let pane = parse_pane_id(registration.pane.as_str())?;
        let client = self
            .inner
            .registry
            .lock()
            .await
            .client_for_pane(registration.pane.as_str())
            .map(|record| record.client_id.clone())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "registered Zellij pane has no live client",
                )
            })?;
        let bridge_registration = self.active_registration(&client).await?;
        let id = format!(
            "zellij:{client}:{bridge_registration}:{}",
            registration.pane
        );
        self.inner
            .pending_leases
            .lock()
            .expect("Zellij pending lease registry is not poisoned")
            .insert(id.clone(), (client, bridge_registration));
        let _ = pane;
        Ok(PendingPaneLease {
            id: PendingPaneLeaseId::new(id),
            ui_session: registration.ui_session,
        })
    }

    /// Dispatches the close action and waits for the bridge's terminal
    /// completion. `Succeeded` means the bridge dispatched the close action,
    /// which is the strongest confirmation available from this protocol.
    async fn close_pending_pane(
        &self,
        registration: PendingPaneRegistration,
        lease: PendingPaneLease,
    ) -> Result<(), AdapterError> {
        self.require_active()?;
        if lease.ui_session != registration.ui_session {
            return Err(invalid_request(
                "Zellij pending cleanup lease belongs to another UI session",
            ));
        }
        let (client, bridge_registration) = self
            .inner
            .pending_leases
            .lock()
            .expect("Zellij pending lease registry is not poisoned")
            .get(lease.id.as_str())
            .cloned()
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Zellij pending cleanup lease is stale",
                )
            })?;
        let current = self.active_registration(&client).await?;
        if current != bridge_registration {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij pending cleanup lease registration is stale",
            ));
        }
        let pane = parse_pane_id(registration.pane.as_str())?;
        let execution = self.mint_local_execution();
        let (sender, receiver) = oneshot::channel();
        self.inner
            .close_waiters
            .lock()
            .expect("Zellij close waiter registry is not poisoned")
            .insert(execution.0, sender);
        self.inner.live_executions.lock().await.insert(
            execution.0,
            ExecutionRecord::new(execution, vec![RequestId::INITIAL]),
        );
        self.enqueue(QueuedItem {
            execution: Some(execution),
            request_slot: Some(RequestId::INITIAL),
            client_id: client,
            payload: Some(BridgeRequest::Dispatch {
                execution: execution_to_common(execution),
                request: ZellijDispatchRequest::Command(RawNativeCommand::ClosePaneWithId {
                    pane_id: pane,
                }),
            }),
        })
        .await;
        let completion = tokio::time::timeout(CLOSE_COMPLETION_TIMEOUT, receiver).await;
        let queued_unsent = {
            let mut queues = self.inner.queues.lock().await;
            let mut removed = false;
            for queue in queues.values_mut() {
                let before = queue.len();
                queue.retain(|item| item.execution != Some(execution));
                removed |= queue.len() != before;
            }
            removed
        };
        if queued_unsent {
            self.inner.live_executions.lock().await.remove(&execution.0);
        }
        self.inner
            .close_waiters
            .lock()
            .expect("Zellij close waiter registry is not poisoned")
            .remove(&execution.0);
        if queued_unsent {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij pending pane close was never sent",
            ));
        }
        let completion = match completion {
            Ok(Ok(completion)) => completion,
            Ok(Err(_)) => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Zellij pending pane close completion channel closed",
                ));
            }
            Err(_) => {
                return Err(AdapterError::new(
                    AdapterErrorKind::OutcomeUnknown,
                    "timed out waiting for Zellij pending pane close completion",
                ));
            }
        };
        match completion {
            DispatchCompletion::Succeeded { .. } => {
                self.inner
                    .pending_leases
                    .lock()
                    .expect("Zellij pending lease registry is not poisoned")
                    .remove(lease.id.as_str());
                Ok(())
            }
            DispatchCompletion::Failed { .. } => Err(AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Zellij bridge reported pending pane close failure",
            )),
            DispatchCompletion::OutcomeUnknown { .. } => Err(AdapterError::new(
                AdapterErrorKind::OutcomeUnknown,
                "Zellij pending pane close outcome is unknown",
            )),
        }
    }

    fn release_pending_pane(&self, lease: PendingPaneLease) {
        self.inner
            .pending_leases
            .lock()
            .expect("Zellij pending lease registry is not poisoned")
            .remove(lease.id.as_str());
    }
    async fn capture_origin(
        &self,
        request: OriginCaptureRequest,
    ) -> Result<OriginContext, AdapterError> {
        self.require_active()?;
        // Zellij native Run carries no Herdr bootstrap tuples: the UI attaches
        // through its own pane ID and the target bridge snapshots the prior
        // non-Muxe pane as the authoritative origin (DESIGN 1189-1196,
        // 1959-1967). Hint/caller tuples are never required here.
        let ui_pane = request.ui_pane.as_str().to_owned();
        // The claim fan-out in modal_scope usually cached the snapshot already;
        // reuse it so attach costs no second host round-trip.
        let snapshot =
            if let Some(snapshot) = self.inner.snapshots.lock().await.get(&ui_pane).cloned() {
                snapshot
            } else {
                let ui_session = format!("origin-{}", hex_id(&self.mint_local_id()));
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
        // `build_origin_context` verifies the snapshot belongs to this UI pane
        // and stores the bridge PRIOR pane as the action origin, never the UI
        // pane itself.
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
        self.require_active()?;
        match creation_requires_post_dismissal(&request.action.action) {
            Ok(true) => {
                return Err(invalid_request(
                    "focused creation must use post-dismissal dispatch after the Muxe UI closes",
                ));
            }
            Ok(false) => {}
            Err(error) => return Err(invalid_request(error.to_string())),
        }
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
                self.inner.live_executions.lock().await.insert(
                    request.execution.0,
                    ExecutionRecord::new(request.execution, vec![RequestId::INITIAL]),
                );
                let payload = match focus {
                    crate::FocusRequest::ByIndex { index } => BridgeRequest::Dispatch {
                        execution: execution_to_common(request.execution),
                        request: ZellijDispatchRequest::FocusPaneByIndex { index },
                    },
                    crate::FocusRequest::Neighbor { direction } => BridgeRequest::Dispatch {
                        execution: execution_to_common(request.execution),
                        request: ZellijDispatchRequest::FocusPaneNeighbor {
                            direction: direction.into_neighbor(),
                        },
                    },
                };
                self.enqueue(QueuedItem {
                    execution: Some(request.execution),
                    request_slot: Some(RequestId::INITIAL),
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

    async fn dispatch_portable_after_ui_dismissal(
        &self,
        request: PostDismissalPortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_active()?;
        let raw = map_post_dismissal_creation(&request.action.action, &request.origin).map_err(
            |error| match error {
                PortableError::Incompatible { reason, .. } => {
                    AdapterError::new(AdapterErrorKind::Incompatible, reason)
                }
                error => invalid_request(error.to_string()),
            },
        )?;
        ValidatedNativeCommand::try_from(raw.clone()).map_err(|error| {
            AdapterError::new(AdapterErrorKind::InvalidRequest, error.to_string())
        })?;
        let client_id = request
            .origin
            .client_id
            .as_ref()
            .map(|id| id.as_str().to_owned())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Zellij post-dismissal dispatch requires the captured origin client",
                )
            })?;
        let origin_pane = request
            .origin
            .pane_id
            .as_ref()
            .map(|id| id.as_str().to_owned())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Zellij post-dismissal dispatch requires the captured origin pane",
                )
            })?;
        self.active_registration(&client_id).await?;
        self.inner.live_executions.lock().await.insert(
            request.execution.0,
            ExecutionRecord::new(request.execution, vec![RequestId::INITIAL]),
        );
        self.enqueue(QueuedItem {
            execution: Some(request.execution),
            request_slot: Some(RequestId::INITIAL),
            client_id,
            payload: Some(BridgeRequest::Dispatch {
                execution: execution_to_common(request.execution),
                request: ZellijDispatchRequest::PostDismissalCreation {
                    ui_pane: request.ui_pane.as_str().to_owned(),
                    origin_pane,
                    command: raw,
                },
            }),
        })
        .await;
        Ok(DispatchAccepted {
            correlation: self.correlation(),
            execution: request.execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    async fn dispatch_native(
        &self,
        request: NativeDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_active()?;
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
        let mut events = self.inner.events_rx.lock().await;
        loop {
            // This is the sole health consumer (the broker monitor). Register
            // the wake before observing the queue or shutdown flag so a
            // notify_waiters during that observation cannot be lost.
            let wake = self.inner.health_wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            #[cfg(test)]
            {
                let hook = self
                    .inner
                    .health_wait_hook
                    .lock()
                    .expect("Zellij health hook is not poisoned")
                    .clone();
                if let Some(hook) = hook {
                    hook.entered.notify_one();
                    hook.release.notified().await;
                }
            }
            if let Ok(event) = events.try_recv() {
                return Ok(event);
            }
            if self.inner.shutdown.load(Ordering::Acquire) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "adapter shut down",
                ));
            }
            tokio::select! {
                biased;
                event = events.recv() => {
                    return event.ok_or_else(|| {
                        AdapterError::new(AdapterErrorKind::Shutdown, "adapter shut down")
                    });
                }
                () = wake => {}
            }
        }
    }

    /// Stops the retained pipe transport after broker-owned UI state drains
    /// and before the activation coordinator releases this broker's endpoint.
    /// Parks both pipe children (terminated and reaped, channels left open
    /// for resume), invalidates bridge registrations so no stale origin or
    /// capture survives, and emits `Unhealthy`. The await proves the old
    /// children are gone before any target connects. Idempotent: a second
    /// suspend while suspended succeeds without touching the transport.
    async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
        if self.inner.suspended.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let _transition = self.inner.registration_transition.lock().await;
        // Fail closed while the old pipes drain: stale registrations,
        // captures, claims, snapshots, queues, and waiters must never serve
        // a target release. Dropping the waiter senders releases their
        // receivers immediately instead of hanging to timeout.
        if let Some(pending) = self.inner.in_flight.lock().await.take()
            && let Some(execution) = pending.execution
            && !pending.settled
        {
            let slot = pending.request_slot.unwrap_or(pending.request.request_id);
            let terminal = {
                let mut live = self.inner.live_executions.lock().await;
                Self::settle_slot(
                    &mut live,
                    execution,
                    slot,
                    Some((
                        OutcomeRank::Unknown,
                        AdapterError::new(
                            AdapterErrorKind::OutcomeUnknown,
                            "Zellij adapter suspended for activation with a request in flight",
                        ),
                    )),
                )
            };
            if let Some(terminal) = terminal {
                self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                    .await;
            }
        }
        // Fail closed while the old pipes drain: stale registrations,
        // captures, claims, snapshots, queues, stamps, and waiters must never
        // serve a target release. Dropping the waiter senders releases their
        // receivers immediately instead of hanging to timeout. Membership for
        // resume comes from a fresh authoritative query per attempt, never
        // from this pre-suspend registry, so nothing is snapshotted here.
        // Queued items drain BEFORE registration retirement: the purge owns
        // each queued slot's unknown, so the retire pass below only settles
        // sent (provenance-bearing) slots and can never double-settle. The
        // retirement here settles by registration rather than by slot, but a
        // slot the purge already settled is `None` and `settle_slot` skips
        // it, so each request still resolves exactly once.
        self.drain_queued_for_suspend().await;
        self.invalidate_all_registrations("Zellij adapter suspended before completion")
            .await;
        self.inner.register_epoch.lock().await.clear();
        *self.inner.success_snapshot.lock().await = None;
        self.inner.captures.lock().await.invalidate_all_clients();
        self.inner.pane_claims.lock().await.clear();
        self.inner.snapshots.lock().await.clear();
        return self.park_suspended_transport().await;
    }

    /// Re-establishes the retained pipe transport after an activation abort,
    /// before the old endpoint accepts dispatch again. Opens a fresh evidence
    /// generation, respawns both children over the pinned production argv,
    /// records the new event-channel install epoch as the attempt's delivery
    /// boundary, then takes one fresh authoritative membership snapshot and
    /// awaits fresh compatible registrations covering exactly that snapshot
    /// with live fingerprint checks. `Healthy` is emitted only after the
    /// snapshot is covered: child spawn alone never counts as bridge
    /// readiness, and old capture leases stay invalid. Every attempt demands
    /// all-required registrations stamped with its own generation and
    /// channel; a failed attempt parks both children, drops its partial
    /// evidence, and leaves the adapter suspended and unhealthy, so the next
    /// attempt cannot reuse a retained subset and no partial adapter
    /// overlaps it. A member absent from this attempt's snapshot (detached
    /// before the query) is not required; a snapshot member without a fresh
    /// registration, or a failed query, fails closed.
    async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
        if !self.inner.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Zellij adapter is not suspended for activation; refusing to fabricate a resumed subscription",
            ));
        }
        let transition = self.inner.registration_transition.lock().await;
        // Open a fresh evidence generation BEFORE touching the transport: only
        // registrations stamped with this generation count, so partial
        // evidence from an earlier attempt can never combine with this one.
        // The registry and stamps are cleared with the new generation; a line
        // already read from the old pipe keeps its older channel tag at
        // receipt and can never satisfy the new channel boundary below.
        let epoch = self.inner.resume_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.invalidate_all_registrations("Zellij adapter resumed before completion")
            .await;
        self.inner.register_epoch.lock().await.clear();
        let generation = self.inner.generation.advance()?;
        let request_ok = self.inner.request.respawn().await.is_ok();
        let event_ok = match subscription_payload(generation) {
            Ok(payload) => self.inner.event.respawn_with_payload(payload).await.is_ok(),
            Err(_) => false,
        };
        drop(transition);
        if !request_ok || !event_ok {
            return Err(self
                .fail_resume(
                    "Zellij activation resume could not revalidate the retained pipe transport",
                )
                .await);
        }
        // Linearized snapshot boundary: the channel that must deliver this
        // attempt's evidence, read after the respawn that installed it. A
        // missing install means the transport never came back: fail closed.
        let Some(channel) = self.inner.event.install_epoch().await else {
            return Err(self
                .fail_resume(
                    "Zellij activation resume could not revalidate the retained pipe transport",
                )
                .await);
        };
        // Fresh authoritative membership for this attempt only. A detached
        // baseline member absent here stops blocking; a newly attached
        // client present here must register fresh. Query or transport
        // failure parks both children and stays suspended, never Healthy.
        let snapshot = match self.inner.membership.snapshot_members().await {
            Ok(mut members) => {
                members.sort();
                members.dedup();
                members
            }
            Err(_) => {
                return Err(self
                    .fail_resume(
                        "Zellij activation resume could not observe the current client membership",
                    )
                    .await);
            }
        };
        if !self
            .await_resume_membership(epoch, channel, &snapshot)
            .await
        {
            return Err(self
                .fail_resume("Zellij activation resume did not observe fresh registrations for the current membership")
                .await);
        }
        // Retain the success round: the snapshot this attempt covered plus
        // the success-generation stamps. The readiness hook reports exactly
        // this round, and the next suspend clears it with the registry.
        *self.inner.success_snapshot.lock().await = Some(snapshot);
        self.inner.suspended.store(false, Ordering::SeqCst);
        self.emit(AdapterHealthEvent::Healthy {
            identity: self.host_identity(),
        })
        .await;
        Ok(())
    }

    /// Reports per-client commit-gate evidence from the retained success
    /// round: the authoritative member set of the snapshot coverage was
    /// proven against, plus the fresh-compatible subset of it in the current
    /// evidence generation. While suspended, or with no retained success,
    /// the adapter reports `None` so the broker serves no stale evidence;
    /// an over-bound member set also reports `None` so the broker gates on
    /// adapter health instead of pretending coverage. An empty snapshot is
    /// genuine `Some` evidence of an empty session. Only IDs leave this
    /// hook, never commands or payloads.
    async fn activation_readiness(&self) -> Result<Option<ActivationReadiness>, AdapterError> {
        if self.inner.suspended.load(Ordering::SeqCst) {
            return Ok(None);
        }
        self.sweep_expired_clients().await;
        let Some(snapshot) = self.inner.success_snapshot.lock().await.clone() else {
            return Ok(None);
        };
        let Some(members) = canonical_member_set(snapshot) else {
            return Ok(None);
        };
        let census = self.fresh_census().await;
        let registered: Vec<String> = members
            .iter()
            .filter(|member| census.iter().any(|id| id == *member))
            .cloned()
            .collect();
        Ok(Some(ActivationReadiness {
            registered_clients: registered,
            member_clients: members,
        }))
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        // Settle outstanding executions first, before quiescing: `emit`
        // drops broker events once quiescing is set, so terminals must be
        // published while producers still run. Mirrors the suspend path:
        // the in-flight slot settles by key, the queue drain owns queued
        // slots (routing local close terminals to their waiters), and the
        // catch-all settles every remaining sent slot; each request resolves
        // exactly once and no execution is left non-terminal.
        if let Some(pending) = self.inner.in_flight.lock().await.take()
            && let Some(execution) = pending.execution
            && !pending.settled
        {
            let slot = pending.request_slot.unwrap_or(pending.request.request_id);
            let terminal = {
                let mut live = self.inner.live_executions.lock().await;
                Self::settle_slot(
                    &mut live,
                    execution,
                    slot,
                    Some((
                        OutcomeRank::Unknown,
                        AdapterError::new(
                            AdapterErrorKind::OutcomeUnknown,
                            "Zellij adapter shut down before completion",
                        ),
                    )),
                )
            };
            if let Some(terminal) = terminal {
                self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                    .await;
            }
        }
        self.drain_queued_for_suspend().await;
        for terminal in self
            .settle_matching_unknown(|_| true, "Zellij adapter shut down before completion")
            .await
        {
            self.emit(AdapterHealthEvent::DispatchCompleted(terminal))
                .await;
        }
        // Quiesce every producer before publishing the final shutdown wake.
        self.inner.quiescing.store(true, Ordering::Release);
        self.inner.quiesce_wake.notify_waiters();
        self.inner.event.close().await;
        let event_loop = self
            .inner
            .event_loop
            .lock()
            .expect("Zellij event-loop slot is not poisoned")
            .take();
        if let Some(event_loop) = event_loop {
            let _ = event_loop.await;
        }
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.health_wake.notify_waiters();
        self.inner.registry_notify.notify_waiters();
        self.inner.pending_origin.lock().await.clear();
        self.inner.pending_capture.lock().await.clear();
        self.inner.request.close().await;
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
    use crate::pipes::SubprocessChannel;
    use crate::pipes::testing::ScriptedChannel;
    use muxe_zellij_protocol::{
        BridgeIdentity, CommandOutcome, CommandStatus, PipeEvent, PipeEventKind,
        ZellijRegistration, bridge_build_id, decode_event_subscription, decode_request_line,
        encode_event_line,
    };
    fn registration_id(seed: u8) -> RegistrationId {
        RegistrationId::from_random_bytes([seed; 16]).expect("test registration")
    }

    fn registration_from_bytes(bytes: [u8; 16]) -> RegistrationId {
        registration_id(bytes[0])
    }

    #[test]
    fn common_execution_encoding_round_trips_and_rejects_foreign_space() {
        let execution = ExecutionId(42);
        assert_eq!(
            common_to_core(&execution_to_common(execution)),
            Some(execution)
        );
        assert_eq!(common_to_core(&CommonExecutionId([1; 16])), None);
    }

    fn register_event(
        registration: [u8; 16],
        bridge_build_id: Option<muxe_protocol::SchemaFingerprint>,
    ) -> PipeEvent {
        register_event_for(
            "client-1",
            registration,
            bridge_build_id,
            env!("CARGO_PKG_VERSION"),
        )
    }

    fn register_event_for(
        client_id: &str,
        registration: [u8; 16],
        bridge_build_id: Option<muxe_protocol::SchemaFingerprint>,
        muxe_version: &str,
    ) -> PipeEvent {
        let registration = registration_from_bytes(registration);
        PipeEvent {
            protocol: muxe_zellij_protocol::BRIDGE_PROTOCOL_VERSION,
            request_id: None,
            channel_generation: ChannelGeneration::INITIAL,
            registration,
            event: PipeEventKind::Event(BridgeEvent::Register {
                registration: ZellijRegistration {
                    client_id: client_id.to_owned(),
                    current_pane: Some("terminal_2".to_owned()),
                    plugin_id: Some(3),
                    identity: BridgeIdentity {
                        muxe_version: muxe_version.to_owned(),
                        source_revision: pinned_source_revision().to_owned(),
                        action_fingerprint: generated_action_fingerprint().0,
                        protocol_fingerprint: bridge_protocol_fingerprint().0,
                        bridge_build_id,
                    },
                },
            }),
        }
    }

    fn pipe_event(
        registration: [u8; 16],
        request_id: Option<RequestId>,
        event: PipeEventKind,
    ) -> PipeEvent {
        PipeEvent {
            protocol: muxe_zellij_protocol::BRIDGE_PROTOCOL_VERSION,
            request_id,
            channel_generation: ChannelGeneration::INITIAL,
            registration: registration_from_bytes(registration),
            event,
        }
    }
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

    /// Scripted membership oracle: replays a staged snapshot per resume
    /// query and counts queries, so tests prove a fresh authoritative
    /// snapshot per attempt instead of snapshot reuse. A set failure makes
    /// every query fail closed like a broken CLI transport.
    struct ScriptedMembership {
        snapshot: Mutex<Vec<String>>,
        fail: AtomicBool,
        queries: AtomicU64,
    }

    impl ScriptedMembership {
        fn fresh(members: Vec<String>) -> Arc<Self> {
            Arc::new(Self {
                snapshot: Mutex::new(members),
                fail: AtomicBool::new(false),
                queries: AtomicU64::new(0),
            })
        }

        /// Stages the snapshot for the next query.
        async fn stage(&self, members: Vec<String>) {
            *self.snapshot.lock().await = members;
        }

        /// Makes every further query fail closed.
        fn fail_closed(&self) {
            self.fail.store(true, Ordering::SeqCst);
        }

        fn query_count(&self) -> u64 {
            self.queries.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MembershipSource for ScriptedMembership {
        async fn snapshot_members(&self) -> Result<Vec<String>, AdapterError> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "scripted membership query failed",
                ));
            }
            Ok(self.snapshot.lock().await.clone())
        }
    }

    fn test_adapter(request: &Arc<ScriptedChannel>, event: &Arc<ScriptedChannel>) -> ZellijAdapter {
        test_adapter_with(request, event, ScriptedMembership::fresh(Vec::new()))
    }

    fn test_adapter_with(
        request: &Arc<ScriptedChannel>,
        event: &Arc<ScriptedChannel>,
        membership: Arc<ScriptedMembership>,
    ) -> ZellijAdapter {
        ZellijAdapter::new_with_membership(
            ZellijAdapterConfig {
                session_name: "session-alpha".to_owned(),
                zellij_exe: PathBuf::from("/nonexistent/zellij"),
            },
            Arc::clone(request) as Arc<dyn PipeChannel>,
            Arc::clone(event) as Arc<dyn PipeChannel>,
            membership,
        )
    }
    fn test_adapter_with_channels(
        request: Arc<dyn PipeChannel>,
        event: Arc<dyn PipeChannel>,
        membership: Arc<ScriptedMembership>,
    ) -> ZellijAdapter {
        ZellijAdapter::new_with_membership(
            ZellijAdapterConfig {
                session_name: "session-alpha".to_owned(),
                zellij_exe: PathBuf::from("/nonexistent/zellij"),
            },
            request,
            event,
            membership,
        )
    }
    /// Writes an owned fake `zellij` executable asserting its argv.
    fn write_fake_exe(body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::with_prefix("muxe-oracle-").expect("unique temp dir");
        let exe = dir.path().join("zellij");
        std::fs::write(&exe, body).expect("fake exe");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        (dir, exe)
    }

    async fn poll_outbound(channel: &ScriptedChannel) -> String {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let outbound = channel.take_outbound();
            if let Some(line) = outbound.into_iter().next() {
                return line;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for outbound request line"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
    #[tokio::test]
    async fn release_pending_pane_is_idempotent_for_stale_or_repeated_leases() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let lease = PendingPaneLease {
            id: PendingPaneLeaseId::new("lease-repeat"),
            ui_session: UiSessionId::new("session-repeat"),
        };
        adapter
            .inner
            .pending_leases
            .lock()
            .expect("Zellij pending lease registry is not poisoned")
            .insert(
                lease.id.as_str().to_owned(),
                ("client-1".to_owned(), registration_id(1)),
            );
        adapter.release_pending_pane(lease.clone());
        adapter.release_pending_pane(lease);
        assert!(
            adapter
                .inner
                .pending_leases
                .lock()
                .expect("Zellij pending lease registry is not poisoned")
                .is_empty(),
            "releasing a stale or repeated lease is a no-op"
        );
    }
    #[tokio::test]
    async fn close_pending_pane_waits_for_bridge_success_before_releasing_lease() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let (registration, lease) = registered_pending_pane(&adapter, &event).await;
        let lease_id = lease.id.clone();
        let close = tokio::spawn({
            let adapter = adapter.clone();
            async move {
                adapter
                    .close_pending_pane(registration, lease.clone())
                    .await
            }
        });
        let frame =
            decode_request_line(&poll_outbound(&request).await).expect("close request frame");
        let execution = match &frame.payload {
            BridgeRequest::Dispatch { execution, .. } => {
                common_to_core(execution).expect("local close execution")
            }
            _ => panic!("pending close uses a dispatch request"),
        };
        assert!(execution.0 >= LOCAL_EXECUTION_CEILING);
        push_dispatch_completion(
            &event,
            frame.request_id,
            execution,
            CommandOutcome::succeeded(),
        );
        close
            .await
            .expect("close task joins")
            .expect("bridge dispatch succeeds");
        assert!(
            !adapter
                .inner
                .pending_leases
                .lock()
                .expect("Zellij pending lease registry is not poisoned")
                .contains_key(lease_id.as_str())
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn close_pending_pane_retains_lease_after_bridge_failure_and_hides_local_event() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let (registration, lease) = registered_pending_pane(&adapter, &event).await;
        let lease_id = lease.id.clone();
        let close = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.close_pending_pane(registration, lease).await }
        });
        let frame =
            decode_request_line(&poll_outbound(&request).await).expect("close request frame");
        let execution = match &frame.payload {
            BridgeRequest::Dispatch { execution, .. } => {
                common_to_core(execution).expect("local close execution")
            }
            _ => panic!("pending close uses a dispatch request"),
        };
        push_dispatch_completion(
            &event,
            frame.request_id,
            execution,
            CommandOutcome {
                status: CommandStatus::Failed,
                detail: "bridge refused close".to_owned(),
            },
        );
        let error = close
            .await
            .expect("close task joins")
            .expect_err("bridge failure is not close success");
        assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
        assert!(
            adapter
                .inner
                .pending_leases
                .lock()
                .expect("Zellij pending lease registry is not poisoned")
                .contains_key(lease_id.as_str())
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "local completion never reaches the broker event stream"
        );
        adapter.shutdown().await.expect("shutdown");
    }
    #[tokio::test]
    async fn close_pending_pane_reports_unknown_after_registration_retirement_without_broker_event()
    {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let (registration, lease) = registered_pending_pane(&adapter, &event).await;
        let close = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.close_pending_pane(registration, lease).await }
        });
        let frame =
            decode_request_line(&poll_outbound(&request).await).expect("close request frame");
        let execution = match &frame.payload {
            BridgeRequest::Dispatch { execution, .. } => {
                common_to_core(execution).expect("local close execution")
            }
            _ => panic!("pending close uses a dispatch request"),
        };
        push_register(&event, "client-1", [8; 16], env!("CARGO_PKG_VERSION"));
        await_turnover_registration(&adapter).await;
        let error = close
            .await
            .expect("close task joins")
            .expect_err("retired registration has unknown close outcome");
        assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
        for _ in 0..3 {
            let Ok(Ok(event)) =
                tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event()).await
            else {
                break;
            };
            assert!(
                !matches!(
                    &event,
                    AdapterHealthEvent::DispatchCompleted(completion)
                        if dispatch_completion_execution(completion) == execution
                ),
                "local completion never reaches the broker event stream"
            );
        }
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn suspend_fails_queued_close_with_unknown_and_drops_live_execution() {
        // O1: suspending with a queued-but-unsent local close must fail the
        // close waiter with OutcomeUnknown — not hang to timeout, not
        // Unavailable — and remove the live_executions entry, so nothing waits
        // for a bridge completion that can no longer arrive. The seeded queue
        // entry exercises the `queues`-purge path (not the pre-existing
        // in-flight arm): pre-O1 the sender is merely dropped and
        // `close_pending_pane` reports Unavailable. Shutdown does NOT do
        // this: it only quiesces producers and clears capture queues,
        // leaving close waiters to their timeout path.
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        // Seed a queued-but-unsent close execution directly: live entry,
        // queued item, and a waiter sender for the same id.
        #[expect(
            clippy::decimal_bitwise_operands,
            reason = "the test uses decimal 777 as a recognizable reserved execution sentinel"
        )]
        let execution = ExecutionId(LOCAL_EXECUTION_CEILING | 777);
        adapter.inner.live_executions.lock().await.insert(
            execution.0,
            ExecutionRecord::new(execution, vec![RequestId::INITIAL]),
        );
        adapter
            .inner
            .queues
            .lock()
            .await
            .entry("client-1".to_owned())
            .or_default()
            .push_back(QueuedItem {
                execution: Some(execution),
                request_slot: Some(RequestId::INITIAL),
                client_id: "client-1".to_owned(),
                payload: None,
            });
        let (sender, receiver) = tokio::sync::oneshot::channel();
        adapter
            .inner
            .close_waiters
            .lock()
            .expect("close waiter registry is writable")
            .insert(execution.0, sender);
        adapter.suspend_for_activation().await.expect("suspend");
        let completion = tokio::time::timeout(Duration::from_secs(2), receiver)
            .await
            .expect("purged waiter answers instead of hanging")
            .expect("waiter sender sends, not drops");
        match completion {
            DispatchCompletion::OutcomeUnknown { execution: got, .. } => {
                assert_eq!(got, execution, "purge fails the exact queued execution");
            }
            other => panic!("purged waiter must fail OutcomeUnknown, got {other:?}"),
        }
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "purged execution is removed from live_executions"
        );
        assert!(
            request.take_outbound().is_empty(),
            "purged queue never reaches the transport"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn close_pending_pane_retains_lease_after_transport_write_failure() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let failing_request = WriteFailChannel::new(Arc::clone(&request));
        let adapter = test_adapter_with_channels(
            failing_request as Arc<dyn PipeChannel>,
            Arc::clone(&event) as Arc<dyn PipeChannel>,
            ScriptedMembership::fresh(Vec::new()),
        );
        let (registration, lease) = registered_pending_pane(&adapter, &event).await;
        let lease_id = lease.id.clone();
        let error = adapter
            .close_pending_pane(registration, lease)
            .await
            .expect_err("transport write failure is not close success");
        assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
        assert!(
            adapter
                .inner
                .pending_leases
                .lock()
                .expect("Zellij pending lease registry is not poisoned")
                .contains_key(lease_id.as_str())
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "local completion never reaches the broker event stream"
        );
        adapter.shutdown().await.expect("shutdown");
    }
    /// Log barrier, not a sleep: returns once the owned fake's invocation
    /// log shows both initial pipe children plus exactly one scoped
    /// `list-clients` query, bounded by an explicit deadline.
    async fn await_fake_invocations(log: &std::path::Path) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let invocations = std::fs::read_to_string(log).unwrap_or_default();
            let pipes = invocations
                .lines()
                .filter(|line| line.contains("--session session-alpha pipe --name"))
                .count();
            let queries = invocations
                .lines()
                .filter(|line| line.contains("--session session-alpha action list-clients"))
                .count();
            if pipes >= 2 && queries == 1 {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "fake invocations missing pipes+query: {invocations:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn next_event(adapter: &ZellijAdapter) -> AdapterHealthEvent {
        tokio::time::timeout(Duration::from_secs(2), adapter.next_health_event())
            .await
            .expect("health arrives")
            .expect("event ok")
    }
    /// Readiness barrier, not a sleep: returns once every named client holds
    /// a live compatible registration, bounded by an explicit deadline.
    async fn await_registered(adapter: &ZellijAdapter, clients: &[&str]) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut ready = true;
                for client in clients {
                    if adapter.active_registration(client).await.is_err() {
                        ready = false;
                        break;
                    }
                }
                if ready {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("registrations land bounded");
    }
    async fn registered_pending_pane(
        adapter: &ZellijAdapter,
        event: &ScriptedChannel,
    ) -> (PendingPaneRegistration, PendingPaneLease) {
        push_register(event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        let registration = PendingPaneRegistration {
            ui_session: UiSessionId::new("pending-close-session"),
            pane: PaneId::new("terminal_2"),
            temporary_tab: None,
        };
        let lease = adapter
            .register_pending_pane(registration.clone())
            .await
            .expect("pending pane lease registers");
        (registration, lease)
    }

    fn push_dispatch_completion(
        event: &ScriptedChannel,
        request_id: RequestId,
        execution: ExecutionId,
        outcome: CommandOutcome,
    ) {
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(request_id),
                PipeEventKind::Response(BridgeResponse::DispatchCompleted {
                    execution: execution_to_common(execution),
                    outcome,
                }),
            ))
            .expect("dispatch completion encodes"),
        );
    }
    fn push_request_released(event: &ScriptedChannel, request_id: RequestId) {
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(request_id),
                PipeEventKind::Response(BridgeResponse::RequestReleased),
            ))
            .expect("request release encodes"),
        );
    }

    fn two_request_batch() -> Vec<RawNativeCommand> {
        vec![RawNativeCommand::CloseFocus, RawNativeCommand::CloseSelf]
    }

    struct WriteFailChannel {
        inner: Arc<ScriptedChannel>,
        fail_writes: std::sync::atomic::AtomicBool,
    }

    impl WriteFailChannel {
        fn new(inner: Arc<ScriptedChannel>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                fail_writes: std::sync::atomic::AtomicBool::new(true),
            })
        }
    }

    #[async_trait]
    impl PipeChannel for WriteFailChannel {
        async fn send_line(&self, line: String) -> Result<(), PipeTransportError> {
            if self.fail_writes.load(std::sync::atomic::Ordering::SeqCst) {
                Err(PipeTransportError::Write {
                    reason: "scripted write failure".to_owned(),
                })
            } else {
                self.inner.send_line(line).await
            }
        }

        async fn next_line(&self) -> Result<String, PipeTransportError> {
            self.inner.next_line().await
        }

        async fn close(&self) {
            self.inner.close().await;
        }
    }
    /// Resume barrier, not a sleep: returns once the event channel installs
    /// an epoch newer than `prev`, proving the attempt respawned the
    /// transport and fixed its delivery boundary. Pushing lines only after
    /// this barrier models bridges re-registering on the new child after
    /// respawn; lines pushed before it carry the displaced epoch.
    async fn await_channel_bump(event: &ScriptedChannel, prev: u64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if event
                    .install_epoch()
                    .await
                    .is_some_and(|epoch| epoch > prev)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("resume respawns the transport bounded");
    }
    /// Turnover barrier, not a sleep: returns once the re-registered bridge
    /// lands in the registry under its new ID, bounded by an explicit deadline.
    async fn await_turnover_registration(adapter: &ZellijAdapter) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let refreshed = adapter
                .inner
                .registry
                .lock()
                .await
                .get("client-1")
                .is_some_and(|record| record.registration == registration_id(8));
            if refreshed {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "turnover registration did not land"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Dispatch-blocked probe: the observable suspended signal. Host-bound
    /// work fails fast with `Unavailable` while suspended, before touching
    /// bridge state.
    async fn assert_suspended(adapter: &ZellijAdapter) {
        let blocked = adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(900),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate(),
                },
                origin: test_origin(),
            })
            .await
            .expect_err("dispatch blocked while suspended");
        assert_eq!(blocked.kind, AdapterErrorKind::Unavailable);
        assert!(
            adapter
                .activation_readiness()
                .await
                .expect("readiness query")
                .is_none(),
            "suspended adapter reports no readiness evidence"
        );
    }

    /// Pushes one registration line for `client` on the event pipe.
    /// `muxe_version` is explicit so tests can prove a stale bridge version
    /// never counts; production-correct callers pass the native compiled version.
    fn push_register(
        event: &ScriptedChannel,
        client: &str,
        registration: [u8; 16],
        muxe_version: &str,
    ) {
        let mut frame =
            register_event_for(client, registration, Some(bridge_build_id()), muxe_version);
        if let Some(payload) = event.initial_payload() {
            frame.channel_generation = decode_event_subscription(&payload)
                .expect("subscription payload")
                .channel_generation();
        }
        event.push_line(encode_event_line(&frame).expect("register encodes"));
    }

    /// Starts one resume attempt: respawns the transport and waits for the
    /// new delivery boundary. The caller replays registration lines, then
    /// settles the attempt with [`finish_resume_attempt`].
    async fn begin_resume_attempt(
        adapter: &ZellijAdapter,
        event: &ScriptedChannel,
    ) -> tokio::task::JoinHandle<Result<(), AdapterError>> {
        let channel = event.install_epoch().await.unwrap_or(0);
        let pending = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.resume_after_activation_abort().await }
        });
        await_channel_bump(event, channel).await;
        pending
    }

    /// Settles a started resume attempt with a bounded wait for its outcome.
    async fn finish_resume_attempt(
        pending: tokio::task::JoinHandle<Result<(), AdapterError>>,
    ) -> Result<(), AdapterError> {
        tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .expect("resume finishes bounded")
            .expect("resume task")
    }

    /// Runs one resume attempt: respawns the transport, waits for the new
    /// delivery boundary, optionally replays one fresh registration on it,
    /// and returns the attempt outcome bounded.
    async fn resume_once(
        adapter: &ZellijAdapter,
        event: &ScriptedChannel,
        fresh: Option<(&str, [u8; 16])>,
    ) -> Result<(), AdapterError> {
        let pending = begin_resume_attempt(adapter, event).await;
        if let Some((client, registration)) = fresh {
            push_register(event, client, registration, env!("CARGO_PKG_VERSION"));
        }
        finish_resume_attempt(pending).await
    }
    /// Pushes a fingerprint-dishonest registration for the newcomer:
    /// observed by the attempt, never compatible.
    fn push_incompatible_newcomer(event: &ScriptedChannel) {
        let mut frame = register_event_for(
            "new-client",
            [20; 16],
            Some(muxe_protocol::SchemaFingerprint([1; 32])),
            env!("CARGO_PKG_VERSION"),
        );
        if let Some(payload) = event.initial_payload() {
            frame.channel_generation = decode_event_subscription(&payload)
                .expect("subscription payload")
                .channel_generation();
        }
        event.push_line(encode_event_line(&frame).expect("incompatible newcomer encodes"));
    }
    /// Seeds two registered clients, plants a stale capture lease, suspends,
    /// and replays one stale pre-suspend registration: the first resume
    /// attempt starts with displaced evidence it must ignore. Returns the
    /// adapter, both channels, the membership oracle, and the stale lease.
    async fn suspend_with_stale_evidence() -> (
        ZellijAdapter,
        Arc<ScriptedChannel>,
        Arc<ScriptedChannel>,
        Arc<ScriptedMembership>,
        [u8; 16],
    ) {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership =
            ScriptedMembership::fresh(vec!["client-1".to_owned(), "client-2".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        for (client, registration) in [("client-1", [7; 16]), ("client-2", [8; 16])] {
            push_register(&event, client, registration, env!("CARGO_PKG_VERSION"));
        }
        await_registered(&adapter, &["client-1", "client-2"]).await;
        // Drain the two registration health reports so later assertions
        // observe exactly the suspend/resume transitions below.
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        // Seed an old capture lease: suspend must invalidate it and resume
        // must never restore it.
        let stale_lease = adapter.mint_local_id();
        adapter
            .inner
            .captures
            .lock()
            .await
            .begin("client-1", "session-9", stale_lease)
            .expect("old lease begins");
        let old_channel = event
            .install_epoch()
            .await
            .expect("event channel installed");
        adapter
            .suspend_for_activation()
            .await
            .expect("suspend succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::CaptureLost { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_suspended(&adapter).await;
        // A late old-pipe Register carrying the pre-suspend ID is rejected:
        // its install epoch no longer names the active event channel, and its
        // registration identity was retired during suspend.
        let stale = encode_event_line(&register_event([7; 16], Some(bridge_build_id())))
            .expect("stale register encodes");
        adapter.handle_event_line(old_channel, &stale).await;
        assert!(adapter.active_registration("client-1").await.is_err());
        (adapter, request, event, membership, stale_lease)
    }

    /// Recorded `Register` → request → `RequestReleased` → `DispatchCompleted` flow:
    /// the exact script the common adapter-contract suite replays.
    #[tokio::test]
    async fn recorded_dispatch_flow_targets_active_registration() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);

        event.push_line(
            encode_event_line(&register_event([7; 16], Some(bridge_build_id())))
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
        assert_eq!(frame.registration, registration_id(7));
        let request_id = frame.request_id;
        assert!(matches!(frame.payload, BridgeRequest::Dispatch { .. }));

        // Transport release unblocks the pipe; completion follows on events.
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(request_id),
                PipeEventKind::Response(BridgeResponse::RequestReleased),
            ))
            .expect("release encodes"),
        );
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(request_id),
                PipeEventKind::Response(BridgeResponse::DispatchCompleted {
                    execution: execution_to_common(ExecutionId(7)),
                    outcome: CommandOutcome::succeeded(),
                }),
            ))
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
            AdapterHealthEvent::HostLost { error, .. } => panic!("host lost: {error}"),
            AdapterHealthEvent::CaptureReady { .. } => panic!("capture ready"),
            AdapterHealthEvent::CaptureLost { .. } => panic!("capture lost"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "single-request execution publishes exactly one terminal"
        );
        assert!(
            !adapter.inner.live_executions.lock().await.contains_key(&7),
            "single-request execution is removed after its terminal"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn multi_request_batch_publishes_one_aggregate_failure_after_all_requests() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        let execution = ExecutionId(101);
        adapter
            .dispatch_to_client(execution, "client-1".to_owned(), two_request_batch())
            .await
            .expect("two-request batch is accepted");
        let first = decode_request_line(&poll_outbound(&request).await).expect("first request");
        push_request_released(&event, first.request_id);
        let second = decode_request_line(&poll_outbound(&request).await).expect("second request");

        push_dispatch_completion(
            &event,
            first.request_id,
            execution,
            CommandOutcome::succeeded(),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "the first success does not complete the aggregate"
        );

        push_request_released(&event, second.request_id);
        push_dispatch_completion(
            &event,
            second.request_id,
            execution,
            CommandOutcome {
                status: CommandStatus::Failed,
                detail: "second request failed".to_owned(),
            },
        );
        match next_event(&adapter).await {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Failed {
                execution: got,
                error,
            }) => {
                assert_eq!(got, execution);
                assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
                assert_eq!(error.message, "second request failed");
            }
            other => {
                let _ = other;
                panic!("expected one aggregate failure")
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "aggregate after-action notification is emitted exactly once"
        );
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "aggregate record is removed after the final request"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn mixed_batch_with_write_failure_publishes_one_unknown_terminal() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let failing_request = WriteFailChannel::new(Arc::clone(&request));
        let adapter = test_adapter_with_channels(
            failing_request as Arc<dyn PipeChannel>,
            Arc::clone(&event) as Arc<dyn PipeChannel>,
            ScriptedMembership::fresh(Vec::new()),
        );
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        let execution = ExecutionId(202);
        adapter
            .dispatch_to_client(execution, "client-1".to_owned(), two_request_batch())
            .await
            .expect("mixed batch remains accepted before transport outcome");
        match next_event(&adapter).await {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution: got,
                error,
            }) => {
                assert_eq!(got, execution);
                assert_eq!(error.kind, AdapterErrorKind::Unavailable);
            }
            _ => panic!("expected one unknown terminal"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "write-failure batch publishes no duplicate terminal"
        );
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "write-failure batch leaves no live execution"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn registration_retirement_resolves_batch_once() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        let execution = ExecutionId(303);
        adapter
            .dispatch_to_client(execution, "client-1".to_owned(), two_request_batch())
            .await
            .expect("two-request batch is accepted");
        let _first = decode_request_line(&poll_outbound(&request).await).expect("first request");

        push_register(&event, "client-1", [8; 16], env!("CARGO_PKG_VERSION"));
        await_turnover_registration(&adapter).await;
        let second = decode_request_line(&poll_outbound(&request).await).expect("second request");
        assert_eq!(second.registration, registration_id(8));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        push_register(&event, "client-1", [9; 16], env!("CARGO_PKG_VERSION"));
        await_turnover_registration(&adapter).await;
        match next_event(&adapter).await {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution: got,
                error,
            }) => {
                assert_eq!(got, execution);
                assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
            }
            _ => panic!("expected one retirement terminal"),
        }
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "registration retirement emits no duplicate terminal"
        );
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "retired batch leaves no live execution"
        );
        assert!(
            adapter
                .inner
                .queues
                .lock()
                .await
                .values()
                .all(VecDeque::is_empty),
            "retired batch leaves no queued request"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn suspend_queue_purge_resolves_batch_once() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        let execution = ExecutionId(404);
        adapter
            .dispatch_to_client(execution, "client-1".to_owned(), two_request_batch())
            .await
            .expect("two-request batch is accepted");
        let _first = decode_request_line(&poll_outbound(&request).await).expect("first request");

        adapter
            .suspend_for_activation()
            .await
            .expect("suspend purges the batch");
        match next_event(&adapter).await {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution: got,
                error,
            }) => {
                assert_eq!(got, execution);
                assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
            }
            _ => panic!("expected one purge terminal"),
        }
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "queue purge emits no duplicate terminal"
        );
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "purged batch leaves no live execution"
        );
        assert!(
            adapter
                .inner
                .queues
                .lock()
                .await
                .values()
                .all(VecDeque::is_empty),
            "purged batch leaves no queued request"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    /// Success plus an outcome-unknown interruption aggregates unknown:
    /// worst case wins (`OutcomeUnknown` > `Failed` > `Succeeded`), and the
    /// terminal carries the interruption error, not the success. Fails
    /// pre-change at the old completion path, which published the first
    /// success as the terminal and orphaned the sibling.
    #[tokio::test]
    async fn batch_success_plus_unknown_publishes_one_unknown_terminal() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        let execution = ExecutionId(303);
        adapter
            .dispatch_to_client(execution, "client-1".to_owned(), two_request_batch())
            .await
            .expect("two-request batch is accepted");
        let first = decode_request_line(&poll_outbound(&request).await).expect("first request");
        push_request_released(&event, first.request_id);
        let second = decode_request_line(&poll_outbound(&request).await).expect("second request");

        push_dispatch_completion(
            &event,
            first.request_id,
            execution,
            CommandOutcome::succeeded(),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "the first success does not complete the aggregate"
        );

        // The second request's bridge dies mid-flight: retirement settles it
        // unknown, and the aggregate must publish exactly one unknown
        // terminal carrying the retirement error.
        push_register(&event, "client-1", [8; 16], env!("CARGO_PKG_VERSION"));
        await_turnover_registration(&adapter).await;
        match next_event(&adapter).await {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                execution: got,
                error,
            }) => {
                assert_eq!(got, execution);
                assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
            }
            _ => panic!("expected one unknown aggregate terminal"),
        }
        // The turnover registration itself reports `Healthy` on arrival; the
        // retirement terminal above was already consumed, so drain exactly
        // one `Healthy` and then require silence: no duplicate terminal.
        // (`push_request_released` is intentionally NOT called for
        // `second.request_id`: its slot settled unknown at retirement.)
        let _ = second;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "success+unknown batch publishes no duplicate terminal"
        );
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "aggregate record is removed after success+unknown"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    /// A duplicate bridge completion for an already-settled request is a
    /// no-op: it must not double-count, publish early, or orphan the real
    /// sibling. The terminal publishes exactly once, on the genuine second
    /// completion, and the record empties.
    #[tokio::test]
    async fn duplicate_completion_for_one_request_does_not_publish_early() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));

        let execution = ExecutionId(404);
        adapter
            .dispatch_to_client(execution, "client-1".to_owned(), two_request_batch())
            .await
            .expect("two-request batch is accepted");
        let first = decode_request_line(&poll_outbound(&request).await).expect("first request");
        push_request_released(&event, first.request_id);
        let second = decode_request_line(&poll_outbound(&request).await).expect("second request");
        push_request_released(&event, second.request_id);

        push_dispatch_completion(
            &event,
            first.request_id,
            execution,
            CommandOutcome::succeeded(),
        );
        // Duplicate of the FIRST request's completion: must be ignored, not
        // counted as the second request's terminal.
        push_dispatch_completion(
            &event,
            first.request_id,
            execution,
            CommandOutcome::succeeded(),
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "duplicate completion publishes no terminal"
        );

        push_dispatch_completion(
            &event,
            second.request_id,
            execution,
            CommandOutcome::succeeded(),
        );
        match next_event(&adapter).await {
            AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded {
                execution: got,
            }) => {
                assert_eq!(got, execution);
            }
            _ => panic!("expected one success terminal on the genuine second completion"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), adapter.next_health_event())
                .await
                .is_err(),
            "duplicate-tolerant batch publishes exactly one terminal"
        );
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0),
            "record empties after genuine completion"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn empty_batch_is_rejected_as_invalid_request() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);

        // The ExecutionRecord acceptance/completion contract documents this
        // deliberate bounded behavior: an empty batch is rejected rather than
        // accepted with no request capable of producing a terminal result.
        let error = adapter
            .dispatch_to_client(ExecutionId(505), "client-1".to_owned(), Vec::new())
            .await
            .expect_err("empty batch is invalid");
        assert_eq!(error.kind, AdapterErrorKind::InvalidRequest);
        assert_eq!(
            error.message,
            "Zellij execution must resolve to at least one host command"
        );
        assert!(
            adapter.inner.live_executions.lock().await.is_empty(),
            "rejected empty batch does not create an execution"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn scheduler_rotates_after_each_released_request() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        push_register(&event, "client-2", [8; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1", "client-2"]).await;

        adapter
            .enqueue_lifecycle("client-1".to_owned(), BridgeRequest::Retire)
            .await;
        adapter
            .enqueue_lifecycle("client-1".to_owned(), BridgeRequest::Retire)
            .await;
        adapter
            .enqueue_lifecycle("client-2".to_owned(), BridgeRequest::Retire)
            .await;
        let first =
            decode_request_line(&poll_outbound(&request).await).expect("first request frame");
        assert_eq!(first.target.client_id, "client-1");
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(first.request_id),
                PipeEventKind::Response(BridgeResponse::RequestReleased),
            ))
            .expect("release encodes"),
        );
        let second =
            decode_request_line(&poll_outbound(&request).await).expect("second request frame");
        assert_eq!(second.target.client_id, "client-2");
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn registration_turnover_finishes_sent_dispatch_as_unknown() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(77),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate(),
                },
                origin: test_origin(),
            })
            .await
            .expect("dispatch accepted");
        let sent = decode_request_line(&poll_outbound(&request).await).expect("request frame");
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(sent.request_id),
                PipeEventKind::Response(BridgeResponse::RequestReleased),
            ))
            .expect("release encodes"),
        );
        push_register(&event, "client-1", [8; 16], env!("CARGO_PKG_VERSION"));
        await_turnover_registration(&adapter).await;

        let mut saw_unknown = false;
        for _ in 0..3 {
            saw_unknown |= matches!(
                next_event(&adapter).await,
                AdapterHealthEvent::DispatchCompleted(DispatchCompletion::OutcomeUnknown {
                    execution: ExecutionId(77),
                    ..
                })
            );
        }
        assert!(saw_unknown);
        assert!(!adapter.inner.live_executions.lock().await.contains_key(&77));
        adapter.shutdown().await.expect("shutdown");
    }

    /// H12: setup failure after the bridge may have entered Locked mode
    /// (the `CaptureReady` waiter resolves only with `Err`, e.g. the waiter
    /// sender was dropped by a racing `CaptureLost`) attempts the release
    /// path and leaves the table clean so a subsequent capture succeeds.
    /// The test drops the waiter sender directly -- the same outcome the
    /// event loop produces when a `CaptureLost` removes the waiter -- then
    /// asserts the table is idle and a fresh capture round-trips.
    #[tokio::test]
    async fn capture_setup_failure_after_mode_entry_releases_and_unblocks_retry() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        // First attempt: let the BeginCapture line reach the transport, then
        // release the request pipe so the pump sends it (the event loop only
        // pumps after `RequestReleased`; without this the line sits queued).
        let first = tokio::spawn({
            let adapter = adapter.clone();
            async move {
                adapter
                    .begin_capture(CaptureRequest {
                        ui_session: UiSessionId::new("session-1"),
                        modal_scope: ZellijAdapter::scope_for_client("client-1"),
                    })
                    .await
            }
        });
        let begin_line = poll_outbound(&request).await;
        let begin_frame = decode_request_line(&begin_line).expect("begin frame");
        let BridgeRequest::BeginCapture { lease, .. } = begin_frame.payload else {
            panic!("first outbound line is BeginCapture");
        };
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(begin_frame.request_id),
                PipeEventKind::Response(BridgeResponse::RequestReleased),
            ))
            .expect("release encodes"),
        );
        // Simulate the racing loss: drop the waiter sender like
        // `CaptureLost` handling does, so the receiver errors even though
        // the bridge may have entered Locked mode.
        adapter.inner.pending_capture.lock().await.remove(&lease.0);
        let error = first
            .await
            .expect("capture task joins")
            .expect_err("failed setup errors");
        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
        // The table is clean: no stale Beginning record blocks a retry.
        assert!(
            adapter
                .inner
                .captures
                .lock()
                .await
                .state("client-1")
                .is_idle(),
            "failed setup leaves no stale capture record"
        );
        // A subsequent capture attempt succeeds end to end.
        let lease = drive_capture_to_ready(&adapter, &request, &event).await;
        assert_eq!(
            lease.modal_scope,
            ZellijAdapter::scope_for_client("client-1")
        );
        adapter.shutdown().await.expect("shutdown");
    }

    /// Drives one `begin_capture` round-trip against the scripted channels:
    /// spawns the capture, releases the request pipe, answers `CaptureReady`
    /// with a `Normal` prior, and returns the resulting lease.
    async fn drive_capture_to_ready(
        adapter: &ZellijAdapter,
        request: &ScriptedChannel,
        event: &ScriptedChannel,
    ) -> ApiCaptureLease {
        let retry = tokio::spawn({
            let adapter = adapter.clone();
            async move {
                adapter
                    .begin_capture(CaptureRequest {
                        ui_session: UiSessionId::new("session-1"),
                        modal_scope: ZellijAdapter::scope_for_client("client-1"),
                    })
                    .await
            }
        });
        let retry_line = poll_outbound(request).await;
        let retry_frame = decode_request_line(&retry_line).expect("retry frame");
        let BridgeRequest::BeginCapture {
            lease: retry_lease, ..
        } = retry_frame.payload
        else {
            panic!("retry outbound line is BeginCapture");
        };
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(retry_frame.request_id),
                PipeEventKind::Response(BridgeResponse::RequestReleased),
            ))
            .expect("release encodes"),
        );
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                Some(retry_frame.request_id),
                PipeEventKind::Response(BridgeResponse::CaptureReady {
                    lease: retry_lease,
                    state: muxe_zellij_protocol::ZellijCaptureState {
                        prior_mode: "Normal".to_owned(),
                    },
                }),
            ))
            .expect("ready encodes"),
        );
        retry.await.expect("retry joins").expect("retry succeeds")
    }

    /// H12: a renewal sweep emits one explicit `RenewCapture` per captured
    /// client on the sweep cadence, so an idle-but-live broker keeps the
    /// bridge-side lease alive. Seeded directly (confirmed capture, no
    /// waiter) so the assertion covers exactly the renewal path.
    #[tokio::test(start_paused = true)]
    async fn capture_sweep_renews_live_capture_on_cadence() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        let lease = [9; 16];
        {
            let mut captures = adapter.inner.captures.lock().await;
            captures
                .begin("client-1", "session-1", lease)
                .expect("capture begins");
            captures
                .confirm("client-1", lease, "Normal".to_owned())
                .expect("capture confirms");
        }
        // One sweep tick renews: a single RenewCapture line for the lease.
        adapter.sweep_expired_clients().await;
        let outbound = request.take_outbound();
        assert_eq!(outbound.len(), 1, "one renewal per captured client");
        let frame = decode_request_line(&outbound[0]).expect("renewal frame");
        assert_eq!(frame.target.client_id, "client-1");
        match frame.payload {
            BridgeRequest::RenewCapture { lease: renewed } => {
                assert_eq!(renewed.0, lease);
            }
            other => panic!("expected RenewCapture, got {other:?}"),
        }
        adapter.shutdown().await.expect("shutdown");
    }

    /// H12: bridge-side lease expiry surfaces as a typed
    /// `BrokerLeaseExpired` loss (not generic adapter-health), so the broker
    /// can distinguish a silent broker from other failures.
    #[tokio::test]
    async fn capture_lost_lease_expiry_maps_to_typed_reason() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        event.push_line(
            encode_event_line(&pipe_event(
                [7; 16],
                None,
                PipeEventKind::Event(BridgeEvent::CaptureLost {
                    lease: CommonCaptureLeaseId([4; 16]),
                    reason: muxe_zellij_protocol::CaptureLostReason::BrokerLeaseExpired,
                }),
            ))
            .expect("loss encodes"),
        );
        assert!(
            matches!(
                next_event(&adapter).await,
                AdapterHealthEvent::CaptureLost {
                    reason: muxe_adapter_api::CaptureLossReason::BrokerLeaseExpired,
                    ..
                }
            ),
            "lease expiry maps to the typed loss reason"
        );
        adapter.shutdown().await.expect("shutdown");
    }

    /// A mismatched `bridge_build_id` registration is contained: the bridge is
    /// recorded but flagged incompatible, so no dispatch line reaches the pipe.
    #[tokio::test]
    async fn mismatched_build_id_is_incompatible() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        event.push_line(
            encode_event_line(&register_event(
                [9; 16],
                Some(muxe_protocol::SchemaFingerprint([1; 32])),
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
                && error.kind == AdapterErrorKind::Incompatible
            {
                break;
            }
            // Before the registration lands the error is Unavailable; either
            // way no dispatch line may reach the pipe.
            assert!(
                tokio::time::Instant::now() < deadline,
                "mismatched build ID registration was not contained: {result:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(request.take_outbound().is_empty());
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn missing_build_id_does_not_invalidate_healthy_peer() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        event.push_line(
            encode_event_line(&register_event([9; 16], None)).expect("missing-ID register encodes"),
        );
        event.push_line(
            encode_event_line(&register_event_for(
                "client-2",
                [10; 16],
                Some(bridge_build_id()),
                env!("CARGO_PKG_VERSION"),
            ))
            .expect("healthy register encodes"),
        );

        let mut missing_origin = test_origin();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let result = adapter
                .dispatch_native(NativeDispatchRequest {
                    execution: ExecutionId(9),
                    action: muxe_adapter_api::ResolvedNativeAction {
                        candidate: candidate(),
                    },
                    origin: missing_origin.clone(),
                })
                .await;
            if let Err(error) = &result
                && error.kind == AdapterErrorKind::Incompatible
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "missing-ID registration was not rejected: {result:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(request.take_outbound().is_empty());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        missing_origin.client_id = Some(muxe_core::ClientId::new("client-2"));
        let accepted = loop {
            let result = adapter
                .dispatch_native(NativeDispatchRequest {
                    execution: ExecutionId(10),
                    action: muxe_adapter_api::ResolvedNativeAction {
                        candidate: candidate(),
                    },
                    origin: missing_origin.clone(),
                })
                .await;
            if let Ok(accepted) = result {
                break accepted;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "healthy peer was invalidated by missing-ID registration"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(accepted.execution, ExecutionId(10));
        let frame = decode_request_line(&poll_outbound(&request).await).expect("healthy request");
        assert_eq!(frame.target.client_id, "client-2");
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
            encode_event_line(&register_event([7; 16], Some(bridge_build_id())))
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
            BridgeRequest::Dispatch {
                execution,
                request: ZellijDispatchRequest::Command(command),
            } => {
                assert_eq!(execution, execution_to_common(ExecutionId(21)));
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
    async fn activation_suspend_blocks_host_work_and_resume_restores() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        // Resume without suspend fabricates nothing: fails closed.
        let premature = adapter
            .resume_after_activation_abort()
            .await
            .expect_err("resume without suspend fails");
        assert_eq!(premature.kind, AdapterErrorKind::Unavailable);
        // Suspend parks the transport (scripted no-op) and fails closed;
        // a second suspend while suspended stays idempotent.
        tokio::time::timeout(Duration::from_secs(10), adapter.suspend_for_activation())
            .await
            .expect("suspend finishes bounded")
            .expect("suspend succeeds");
        tokio::time::timeout(Duration::from_secs(10), adapter.suspend_for_activation())
            .await
            .expect("second suspend finishes bounded")
            .expect("suspend is idempotent");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        // Host-bound operations fail fast with Unavailable while suspended,
        // before touching bridge state (no 15s fan-out, no scope parsing).
        let suspended_scope = adapter
            .modal_scope(&PaneId::new("pane-9"))
            .await
            .expect_err("modal scope blocked while suspended");
        assert_eq!(suspended_scope.kind, AdapterErrorKind::Unavailable);
        let suspended_capture = adapter
            .begin_capture(CaptureRequest {
                ui_session: UiSessionId::new("session-1"),
                modal_scope: ModalScopeId::new("bad-scope"),
            })
            .await
            .expect_err("capture blocked while suspended");
        assert_eq!(suspended_capture.kind, AdapterErrorKind::Unavailable);
        let suspended_origin = adapter
            .capture_origin(origin_request("pane-9"))
            .await
            .expect_err("origin blocked while suspended");
        assert_eq!(suspended_origin.kind, AdapterErrorKind::Unavailable);
        // Resume revalidates (scripted no-op) and reports Healthy; the gate
        // clears so the same malformed scope now fails on scope parsing
        // instead of suspension.
        tokio::time::timeout(
            Duration::from_secs(10),
            adapter.resume_after_activation_abort(),
        )
        .await
        .expect("resume finishes bounded")
        .expect("resume succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        let cleared = adapter
            .begin_capture(CaptureRequest {
                ui_session: UiSessionId::new("session-1"),
                modal_scope: ModalScopeId::new("bad-scope"),
            })
            .await
            .expect_err("gate cleared after resume");
        assert_eq!(cleared.kind, AdapterErrorKind::InvalidRequest);
        let again = adapter
            .resume_after_activation_abort()
            .await
            .expect_err("second resume fails");
        assert_eq!(again.kind, AdapterErrorKind::Unavailable);
        adapter.shutdown().await.expect("shutdown");
    }
    /// Resume measures coverage against a fresh authoritative snapshot per
    /// attempt, with registrations stamped at receipt on that attempt's
    /// delivery channel. Two clients register before suspend; a late Register
    /// from the displaced channel is rejected. Attempt 1 sees no fresh lines
    /// and fails. Attempt 2 sees only client-1 and fails, proving partial
    /// evidence is not retained. Attempt 3 snapshots only client-1 after
    /// client-2 detaches and succeeds when client-1 mints another registration.
    /// Readiness, dispatch targeting, and old-lease invalidity prove both
    /// membership handling and retired-ID rejection.
    #[tokio::test]
    async fn activation_resume_waits_for_fresh_member_registrations() {
        let (adapter, request, event, membership, stale_lease) =
            suspend_with_stale_evidence().await;
        // Attempt 1: the snapshot covers both members but no fresh lines
        // arrive. Fails closed; the pre-attempt evidence above must not
        // count, and exactly one query ran for this attempt.
        let unregistered = resume_once(&adapter, &event, None)
            .await
            .expect_err("unregistered resume fails closed");
        assert_eq!(unregistered.kind, AdapterErrorKind::Unavailable);
        assert_suspended(&adapter).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_eq!(membership.query_count(), 1);
        // Attempt 2: only client-1 re-registers fresh in this generation.
        // The snapshot still needs client-2, so the attempt fails and its
        // evidence is dropped, never retained for the next attempt.
        membership
            .stage(vec!["client-1".to_owned(), "client-2".to_owned()])
            .await;
        let partial = resume_once(&adapter, &event, Some(("client-1", [9; 16])))
            .await
            .expect_err("partial resume fails closed");
        assert_eq!(partial.kind, AdapterErrorKind::Unavailable);
        assert_suspended(&adapter).await;
        // The fresh registration reported Healthy on arrival; the failed
        // attempt then reports Unhealthy and drops its evidence.
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_eq!(membership.query_count(), 2);
        // Attempt 3: client-2 detached before the query, so the authoritative
        // snapshot holds only client-1. Its bridge mints a new registration for
        // this event channel; the retired pre-suspend and failed-attempt IDs
        // remain invalid.
        membership.stage(vec!["client-1".to_owned()]).await;
        resume_once(&adapter, &event, Some(("client-1", [10; 16])))
            .await
            .expect("complete resume succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert_eq!(membership.query_count(), 3);
        // Readiness evidence: client-1 fresh against one authoritative member.
        let readiness = adapter
            .activation_readiness()
            .await
            .expect("readiness query")
            .expect("ready after complete resume");
        assert_eq!(readiness.registered_clients, vec!["client-1".to_owned()]);
        assert_eq!(readiness.member_clients, vec!["client-1".to_owned()]);
        assert_eq!(
            adapter
                .active_registration("client-1")
                .await
                .expect("client-1 live"),
            registration_id(10)
        );
        // Observable dispatch now targets the fresh registration, proving
        // restored bridge readiness beyond the health event.
        let accepted = adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(91),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate(),
                },
                origin: test_origin(),
            })
            .await
            .expect("dispatch succeeds after complete resume");
        assert_eq!(accepted.execution, ExecutionId(91));
        let line = poll_outbound(&request).await;
        let frame = decode_request_line(&line).expect("typed request frame");
        assert_eq!(frame.target.client_id, "client-1");
        assert_eq!(frame.registration, registration_id(10));
        // The pre-suspend lease was never restored: the cleared table
        // reports no active capture for the old lease.
        assert!(
            adapter
                .inner
                .captures
                .lock()
                .await
                .release("client-1", stale_lease, false)
                .is_err()
        );
        adapter.shutdown().await.expect("shutdown");
    }
    /// Readiness reports the covered snapshot exactly: a compatible
    /// registration from a client outside the snapshot never leaks into
    /// the registered set, so a count-based consumer cannot mistake the
    /// extra registration for coverage of a missing member.
    #[tokio::test]
    async fn activation_readiness_reports_covered_subset_only() {
        let (adapter, _request, event, membership, _lease) = suspend_with_stale_evidence().await;
        membership.stage(vec!["client-1".to_owned()]).await;
        let pending = begin_resume_attempt(&adapter, &event).await;
        push_register(&event, "client-1", [10; 16], env!("CARGO_PKG_VERSION"));
        // client-9 holds a compatible registration in this generation but
        // was never a snapshot member: the evidence must exclude it.
        push_register(&event, "client-9", [9; 16], env!("CARGO_PKG_VERSION"));
        finish_resume_attempt(pending)
            .await
            .expect("resume succeeds with extra non-member registration");
        let readiness = adapter
            .activation_readiness()
            .await
            .expect("readiness query")
            .expect("ready after covered resume");
        assert_eq!(readiness.member_clients, vec!["client-1".to_owned()]);
        assert_eq!(readiness.registered_clients, vec!["client-1".to_owned()]);
        adapter.shutdown().await.expect("shutdown");
    }
    /// An empty authoritative snapshot is genuine evidence: resume covers
    /// vacuously and the hook reports empty sets, never `None`. Only a
    /// missing success round (suspended, never resumed) reports `None`.
    #[tokio::test]
    async fn activation_readiness_reports_empty_session() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership = ScriptedMembership::fresh(Vec::new());
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        adapter
            .suspend_for_activation()
            .await
            .expect("suspend succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_suspended(&adapter).await;
        resume_once(&adapter, &event, None)
            .await
            .expect("empty snapshot resume succeeds");
        let readiness = adapter
            .activation_readiness()
            .await
            .expect("readiness query")
            .expect("empty session is evidence");
        assert!(readiness.member_clients.is_empty());
        assert!(readiness.registered_clients.is_empty());
        adapter.shutdown().await.expect("shutdown");
    }
    /// A stale bridge version never counts: identical source, action, and
    /// protocol fingerprints with an old `muxe_version` stay incompatible,
    /// so the member blocks coverage, the attempt fails, and no dispatch
    /// targets it. The same bridge at the native compiled version covers
    /// and reports exact readiness. A fresh adapter that never ran a round
    /// reports `None` throughout, never constructor-default evidence.
    #[tokio::test]
    async fn activation_resume_rejects_stale_bridge_version() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership =
            ScriptedMembership::fresh(vec!["client-1".to_owned(), "client-2".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        // Fresh adapter, nonempty session, no round ever ran: no evidence.
        assert!(
            adapter
                .activation_readiness()
                .await
                .expect("readiness query")
                .is_none()
        );
        adapter
            .suspend_for_activation()
            .await
            .expect("suspend succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_suspended(&adapter).await;
        // Attempt 1: client-1 registers at the native version but client-2
        // reports an old bridge. The member blocks coverage and the attempt
        // fails closed; nothing dispatches to the stale bridge.
        membership
            .stage(vec!["client-1".to_owned(), "client-2".to_owned()])
            .await;
        let pending = begin_resume_attempt(&adapter, &event).await;
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        push_register(&event, "client-2", [8; 16], "0.0.0-old");
        let stale = finish_resume_attempt(pending)
            .await
            .expect_err("stale-version member blocks coverage");
        assert_eq!(stale.kind, AdapterErrorKind::Unavailable);
        assert_suspended(&adapter).await;
        assert!(
            adapter
                .activation_readiness()
                .await
                .expect("readiness query")
                .is_none()
        );
        // Attempt 2 uses fresh registration IDs at the native compiled version.
        let pending = begin_resume_attempt(&adapter, &event).await;
        push_register(&event, "client-1", [9; 16], env!("CARGO_PKG_VERSION"));
        push_register(&event, "client-2", [10; 16], env!("CARGO_PKG_VERSION"));
        finish_resume_attempt(pending)
            .await
            .expect("native-version round succeeds");
        let readiness = adapter
            .activation_readiness()
            .await
            .expect("readiness query")
            .expect("covered round is evidence");
        assert_eq!(
            readiness.member_clients,
            vec!["client-1".to_owned(), "client-2".to_owned()]
        );
        assert_eq!(
            readiness.registered_clients,
            vec!["client-1".to_owned(), "client-2".to_owned()]
        );
        adapter.shutdown().await.expect("shutdown");
    }
    /// A fresh target that never went through suspend/resume still reaches
    /// readiness through its own initial round: no evidence before any
    /// census, then the production census function snapshots the live
    /// oracle and covers it with current-channel registrations — no abort
    /// cycle anywhere. Without this path fresh targets would stay
    /// permanently unready.
    #[tokio::test]
    async fn activation_initial_round_covers_fresh_target() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership =
            ScriptedMembership::fresh(vec!["client-1".to_owned(), "client-2".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        // Fresh target, nonempty session, no round ever ran: no evidence.
        assert!(
            adapter
                .activation_readiness()
                .await
                .expect("readiness query")
                .is_none()
        );
        // Bridges register on the live channel before the census starts.
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        push_register(&event, "client-2", [8; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1", "client-2"]).await;
        adapter
            .establish_initial_round()
            .await
            .expect("initial round covers the live membership");
        let readiness = adapter
            .activation_readiness()
            .await
            .expect("readiness query")
            .expect("covered fresh target is evidence");
        assert_eq!(
            readiness.member_clients,
            vec!["client-1".to_owned(), "client-2".to_owned()]
        );
        assert_eq!(
            readiness.registered_clients,
            vec!["client-1".to_owned(), "client-2".to_owned()]
        );
        assert_eq!(membership.query_count(), 1);
        adapter.shutdown().await.expect("shutdown");
    }
    /// A failed initial census retry reinstalls the event subscription.
    /// Registrations from the displaced event child cannot satisfy the new
    /// channel epoch; a bridge registration delivered after refresh can.
    #[tokio::test]
    async fn activation_initial_round_refreshes_missed_subscription() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership = ScriptedMembership::fresh(vec!["client-1".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        let displaced_channel = event.install_epoch().await.expect("initial event channel");

        adapter
            .refresh_initial_subscription()
            .await
            .expect("refresh event subscription");
        assert!(
            event
                .install_epoch()
                .await
                .is_some_and(|channel| channel > displaced_channel),
            "subscription refresh must install a new event-channel epoch"
        );

        let pending = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.establish_initial_round().await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while membership.query_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("refreshed census query runs bounded");
        assert!(
            !pending.is_finished(),
            "a registration from the displaced subscription satisfied the refreshed census"
        );
        push_register(&event, "client-1", [8; 16], env!("CARGO_PKG_VERSION"));
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("fresh registration completes the census")
            .expect("census task joins")
            .expect("refreshed census succeeds");
        adapter.shutdown().await.expect("shutdown");
    }

    /// Shutdown fails an in-flight initial census promptly instead of
    /// stalling to the census deadline: the waiter wakes on the shutdown
    /// notify and maps to `Shutdown`, retaining no success round.
    #[tokio::test]
    async fn activation_initial_round_shutdown_wakes_census_wait() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership =
            ScriptedMembership::fresh(vec!["client-1".to_owned(), "client-2".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        // No bridge ever registers, so the round must be awaiting coverage;
        // the completed membership query proves it passed the entry checks.
        let pending = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.establish_initial_round().await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while membership.query_count() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("census query runs bounded");
        adapter.shutdown().await.expect("shutdown");
        let outcome = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("shutdown wakes the census wait promptly")
            .expect("round task joins");
        let error = outcome.expect_err("shutdown fails the round");
        assert_eq!(error.kind, AdapterErrorKind::Shutdown);
        assert!(
            adapter
                .activation_readiness()
                .await
                .expect("readiness query")
                .is_none(),
            "shutdown retains no readiness evidence"
        );
    }
    /// Shutdown releases a pending capture waiter promptly: dropping the
    /// waiter sender fails the receiver through the existing timeout/else
    /// path instead of stalling to the capture deadline.
    #[tokio::test]
    async fn activation_shutdown_wakes_pending_capture() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership = ScriptedMembership::fresh(vec!["client-1".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        // No bridge answers the capture, so the waiter must be pending; the
        // outbound line proves the request reached the transport.
        let pending = tokio::spawn({
            let adapter = adapter.clone();
            async move {
                adapter
                    .begin_capture(CaptureRequest {
                        ui_session: UiSessionId::new("session-1"),
                        modal_scope: ZellijAdapter::scope_for_client("client-1"),
                    })
                    .await
            }
        });
        poll_outbound(&request).await;
        adapter.shutdown().await.expect("shutdown");
        let outcome = tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .expect("shutdown wakes the capture waiter promptly")
            .expect("capture task joins");
        let error = outcome.expect_err("shutdown fails the capture");
        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
    }
    /// A bridge that registers then goes quiet past its heartbeat lease is
    /// invalidated at the next availability gate: dispatch fails with
    /// `Unavailable` and the client reports `Unhealthy` once with its modal
    /// scope, while the queue pauses for lack of a compatible record.
    /// Paused tokio time proves the 15s lease without wall-clock waiting.
    #[tokio::test(start_paused = true)]
    async fn activation_heartbeat_expiry_invalidates_quiet_bridge() {
        use crate::registry::HEARTBEAT_LEASE;
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership = ScriptedMembership::fresh(vec!["client-1".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1"]).await;
        // Drain the registration health report so only the expiry report
        // remains observable below.
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        tokio::time::advance(HEARTBEAT_LEASE + Duration::from_secs(1)).await;
        let expired = adapter
            .active_registration("client-1")
            .await
            .expect_err("quiet bridge is unavailable");
        assert_eq!(expired.kind, AdapterErrorKind::Unavailable);
        assert!(
            matches!(
                next_event(&adapter).await,
                AdapterHealthEvent::Unhealthy {
                    modal_scope: Some(scope),
                    ..
                } if scope == ZellijAdapter::scope_for_client("client-1")
            ),
            "expiry reports the client scope unhealthy once"
        );
        adapter.shutdown().await.expect("shutdown");
    }
    /// Timer-driven sweep isolates one quiet client: while client-2
    /// heartbeats, client-1 expires with only its own registration,
    /// capture, waiter, and session health invalidated. Its queued
    /// dispatch pauses (kept, never sent); the shared event channel,
    /// healthy registrations, and ongoing dispatch acceptance continue
    /// with no whole-pipe restart. Paused tokio time proves lease timing
    /// without wall-clock waiting.
    #[tokio::test(start_paused = true)]
    #[expect(
        clippy::too_many_lines,
        reason = "one linear two-client isolation script: setup, timer travel with barriers, then per-client assertions in expiry order"
    )]
    async fn activation_sweep_expires_only_quiet_client() {
        use crate::registry::HEARTBEAT_LEASE;
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership =
            ScriptedMembership::fresh(vec!["client-1".to_owned(), "client-2".to_owned()]);
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        push_register(&event, "client-1", [7; 16], env!("CARGO_PKG_VERSION"));
        push_register(&event, "client-2", [8; 16], env!("CARGO_PKG_VERSION"));
        await_registered(&adapter, &["client-1", "client-2"]).await;
        // Seed the states an idle captured client holds: a confirmed
        // capture, one queued dispatch, and one pending capture waiter.
        // Seeded directly so no release-deadline timer starts before the
        // sweep under test.
        let lease = [9; 16];
        {
            let mut captures = adapter.inner.captures.lock().await;
            captures
                .begin("client-1", "session-1", lease)
                .expect("capture begins");
            captures
                .confirm("client-1", lease, "normal".to_owned())
                .expect("capture confirms");
        }
        adapter.inner.live_executions.lock().await.insert(
            77,
            ExecutionRecord::new(ExecutionId(77), vec![RequestId::INITIAL]),
        );
        // A real lifecycle payload: the pre-existing `payload: None` seed
        // could never survive any enqueue-triggered pump (the pump consumes
        // a payload-less head with an `Unhealthy` report), while production
        // queues only ever hold encodable payloads. `Retire` models one
        // queued lifecycle line without touching execution 77, which stays a
        // purely supervised entry that must never complete as unknown.
        adapter
            .inner
            .queues
            .lock()
            .await
            .entry("client-1".to_owned())
            .or_default()
            .push_back(QueuedItem {
                execution: None,
                request_slot: None,
                client_id: "client-1".to_owned(),
                payload: Some(BridgeRequest::Retire),
            });
        let (waiter_tx, mut waiter_rx) = oneshot::channel();
        adapter.inner.pending_capture.lock().await.insert(
            [10; 16],
            (
                "client-1".to_owned(),
                PendingReply {
                    request: None,
                    sender: waiter_tx,
                },
            ),
        );
        // Drain both registration health reports so only sweep reports
        // remain observable below.
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        let first_epoch = event.install_epoch().await;
        // Keep client-2 alive while client-1 goes quiet past its lease.
        let stamped = adapter
            .inner
            .registry
            .lock()
            .await
            .get("client-2")
            .map(|record| record.last_event_millis)
            .expect("client-2 registered");
        tokio::time::advance(
            HEARTBEAT_LEASE
                .checked_sub(Duration::from_secs(5))
                .expect("lease exceeds heartbeat margin"),
        )
        .await;
        event.push_line(
            encode_event_line(&pipe_event(
                [8; 16],
                None,
                PipeEventKind::Event(BridgeEvent::Heartbeat),
            ))
            .expect("heartbeat encodes"),
        );
        // Barrier, not a sleep: the heartbeat renewal must land before the
        // clock moves past client-1's lease.
        for _ in 0..1000 {
            let renewed = adapter
                .inner
                .registry
                .lock()
                .await
                .get("client-2")
                .is_some_and(|record| record.last_event_millis > stamped);
            if renewed {
                break;
            }
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(10)).await;
        // Barrier: the timer sweep must have reaped client-1.
        for _ in 0..1000 {
            if adapter
                .inner
                .registry
                .lock()
                .await
                .get("client-1")
                .is_none()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Only the quiet client expired: registration, capture, waiter.
        assert!(
            adapter
                .inner
                .registry
                .lock()
                .await
                .get("client-1")
                .is_none(),
            "quiet registration reaped"
        );
        assert!(
            adapter
                .inner
                .registry
                .lock()
                .await
                .get("client-2")
                .is_some_and(|record| record.registration == registration_id(8)),
            "heartbeating registration survives with its ID"
        );
        assert!(
            adapter
                .inner
                .captures
                .lock()
                .await
                .state("client-1")
                .is_idle(),
            "expired capture never stays valid"
        );
        assert!(
            adapter
                .inner
                .captures
                .lock()
                .await
                .confirm("client-1", lease, "normal".to_owned())
                .is_err(),
            "expired lease confirms nothing"
        );
        assert!(
            matches!(
                waiter_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "expired waiter sender dropped"
        );
        // Session health for exactly the expired client: capture loss with
        // its real session, then the scoped unavailable report.
        // Session health for exactly the expired client: capture loss with
        // its real session, then the scoped unavailable report.
        assert!(
            matches!(
                next_event(&adapter).await,
                AdapterHealthEvent::CaptureLost { lease, .. }
                    if lease.modal_scope == ZellijAdapter::scope_for_client("client-1")
            ),
            "expiry reports capture loss for the quiet session"
        );
        assert!(
            matches!(
                next_event(&adapter).await,
                AdapterHealthEvent::Unhealthy {
                    modal_scope: Some(scope),
                    ..
                } if scope == ZellijAdapter::scope_for_client("client-1")
            ),
            "expiry reports the quiet client unhealthy once"
        );
        // Pre-expiry pump evidence: the seeded Retire went out on the first
        // renewal-triggered pump (the renewal itself stays queued behind the
        // unacked Retire -- the release timer only re-arms the pipe at the
        // same instant the expiry sweep reaps the client, so the renewal is
        // never sent). Drained here so later assertions see only post-expiry
        // behavior.
        let pre_expiry = request.take_outbound();
        assert_eq!(pre_expiry.len(), 1, "only the seeded line goes out");
        let first_out = decode_request_line(&pre_expiry[0]).expect("seeded frame");
        assert_eq!(first_out.target.client_id, "client-1");
        assert!(matches!(first_out.payload, BridgeRequest::Retire));
        // The healthy client re-registers and pumps without touching the
        // expired client's paused queue; nothing new reaches the transport
        // and the shared event channel never restarted.
        push_register(&event, "client-2", [8; 16], env!("CARGO_PKG_VERSION"));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        adapter.pump_all().await;
        // Two renewal lines (one per live pre-expiry sweep at the 5s and
        // 10s ticks) pause instead of purging: without a registration the
        // pump re-queues instead of sending, and the shared event channel
        // never restarted. The 15s tick is the one that reaps (paused-clock
        // overhead makes now-last exceed the 15s lease there), so it renews
        // nothing after invalidating the capture.
        let paused: Vec<BridgeRequest> = adapter
            .inner
            .queues
            .lock()
            .await
            .get("client-1")
            .map(|queue| {
                queue
                    .iter()
                    .filter_map(|item| item.payload.clone())
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(paused.len(), 2, "renewals pause instead of purging");
        assert!(
            paused
                .iter()
                .all(|payload| matches!(payload, BridgeRequest::RenewCapture { .. })),
            "only renewals remain queued"
        );
        assert!(
            adapter.inner.live_executions.lock().await.contains_key(&77),
            "paused execution never completes as unknown"
        );
        assert!(
            request.take_outbound().is_empty(),
            "nothing dispatched for the expired client"
        );
        assert_eq!(
            event.install_epoch().await,
            first_epoch,
            "no whole-pipe restart around one expiry"
        );
        // Healthy dispatch acceptance continues; the expired client fails
        // at the gate before anything queues.
        let blocked = adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(78),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate(),
                },
                origin: test_origin(),
            })
            .await
            .expect_err("expired client dispatches nothing");
        assert_eq!(blocked.kind, AdapterErrorKind::Unavailable);
        let mut origin_2 = test_origin();
        origin_2.client_id = Some(muxe_core::ClientId::new("client-2"));
        let accepted = adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(79),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate(),
                },
                origin: origin_2,
            })
            .await
            .expect("healthy dispatch accepted");
        assert_eq!(accepted.execution, ExecutionId(79));
        let outbound = request.take_outbound();
        assert_eq!(outbound.len(), 1, "healthy dispatch reaches the transport");
        let frame = decode_request_line(&outbound[0]).expect("typed request frame");
        assert_eq!(frame.target.client_id, "client-2");
        adapter.shutdown().await.expect("shutdown");
    }

    /// A snapshot member without a registration never reports Healthy, and
    /// an incompatible registration never counts. Suspended with no prior
    /// clients, attempt 1 snapshots a newly attached client but sees no
    /// registration and fails: membership without evidence blocks, so
    /// silence is never proof. Attempt 2 sees an incompatible registration
    /// for the snapshotted newcomer and fails. Attempt 3 sees the honest
    /// compatible handshake and succeeds with the newcomer in the
    /// readiness evidence. Every assertion is consumer observable.
    #[tokio::test]
    async fn activation_resume_empty_membership_discovers_newcomers() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership = ScriptedMembership::fresh(Vec::new());
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        adapter
            .suspend_for_activation()
            .await
            .expect("suspend succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_suspended(&adapter).await;
        // Attempt 1: the authoritative snapshot reports a newly attached
        // client that never registers. Resume fails closed: the snapshot
        // member without evidence blocks, and silence never reads Healthy.
        membership.stage(vec!["new-client".to_owned()]).await;
        let silent = resume_once(&adapter, &event, None)
            .await
            .expect_err("membership without registration fails closed");
        assert_eq!(silent.kind, AdapterErrorKind::Unavailable);
        assert_suspended(&adapter).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_eq!(membership.query_count(), 1);
        // Attempt 2: the snapshotted newcomer registers with a dishonest
        // handshake. The registration is observed but incompatible, so the
        // attempt fails closed instead of covering the snapshot.
        membership.stage(vec!["new-client".to_owned()]).await;
        let pending = begin_resume_attempt(&adapter, &event).await;
        push_incompatible_newcomer(&event);
        let rejected = finish_resume_attempt(pending)
            .await
            .expect_err("incompatible newcomer fails closed");
        assert_eq!(rejected.kind, AdapterErrorKind::Unavailable);
        assert_suspended(&adapter).await;
        // The incompatible arrival reports Unhealthy; the failed attempt
        // reports Unhealthy again and drops its evidence.
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_eq!(membership.query_count(), 2);
        // Attempt 3: the newcomer returns with an honest compatible
        // handshake in this generation. Resume succeeds and the readiness
        // hook proves the current-attempt registration.
        membership.stage(vec!["new-client".to_owned()]).await;
        resume_once(&adapter, &event, Some(("new-client", [21; 16])))
            .await
            .expect("compatible newcomer succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Healthy { .. }
        ));
        assert_eq!(membership.query_count(), 3);
        let readiness = adapter
            .activation_readiness()
            .await
            .expect("readiness query")
            .expect("ready after newcomer resume");
        assert_eq!(readiness.registered_clients, vec!["new-client".to_owned()]);
        assert_eq!(readiness.member_clients, vec!["new-client".to_owned()]);
        adapter.shutdown().await.expect("shutdown");
    }
    /// A failed membership query parks both children and stays suspended:
    /// without an authoritative snapshot there is nothing to cover, so the
    /// attempt fails closed with `Unavailable` and the next attempt takes
    /// its own fresh snapshot.
    #[tokio::test]
    async fn activation_resume_membership_query_failure_parks_and_stays_suspended() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let membership = ScriptedMembership::fresh(Vec::new());
        membership.fail_closed();
        let adapter = test_adapter_with(&request, &event, Arc::clone(&membership));
        adapter
            .suspend_for_activation()
            .await
            .expect("suspend succeeds");
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        let channel = event.install_epoch().await.unwrap_or(0);
        let pending = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.resume_after_activation_abort().await }
        });
        await_channel_bump(&event, channel).await;
        let failed = tokio::time::timeout(Duration::from_secs(10), pending)
            .await
            .expect("failed-query resume finishes bounded")
            .expect("resume task")
            .expect_err("failed query fails closed");
        assert_eq!(failed.kind, AdapterErrorKind::Unavailable);
        assert_suspended(&adapter).await;
        assert!(matches!(
            next_event(&adapter).await,
            AdapterHealthEvent::Unhealthy { .. }
        ));
        assert_eq!(membership.query_count(), 1);
        adapter.shutdown().await.expect("shutdown");
    }

    /// Pinned `list-clients` table fixtures, derived from
    /// `ClientMetadata::render_many`: header plus space-padded rows, a
    /// header-only empty session, and empty output failing closed.
    #[test]
    fn list_clients_output_parses_pinned_table_format() {
        let table = "CLIENT_ID ZELLIJ_PANE_ID RUNNING_COMMAND\n\
                     1         terminal_2     /bin/zsh      \n\
                     2         terminal_5     N/A           \n";
        assert_eq!(
            super::parse_list_clients_output(table).expect("table parses"),
            vec!["1".to_owned(), "2".to_owned()]
        );
        let header_only = "CLIENT_ID ZELLIJ_PANE_ID RUNNING_COMMAND\n";
        assert!(
            super::parse_list_clients_output(header_only)
                .expect("empty session parses")
                .is_empty()
        );
        assert!(
            super::parse_list_clients_output("")
                .expect_err("empty output fails closed")
                .to_string()
                .contains("no output")
        );
        let bad_row =
            "CLIENT_ID ZELLIJ_PANE_ID RUNNING_COMMAND\n???       terminal_2     /bin/zsh\n";
        assert!(
            super::parse_list_clients_output(bad_row)
                .expect_err("non-numeric ID fails closed")
                .to_string()
                .contains("unsupported row shape")
        );
    }

    /// Production oracle against owned fake executables (no live host): the
    /// fake asserts the exact scoped argv and prints a recorded-format
    /// table, proving the query path end to end; a rejecting fake proves
    /// transport failure surfaces as `Unavailable` with no payload logged.
    #[tokio::test]
    async fn cli_membership_oracle_queries_scoped_session_binary() {
        let table = r#"#!/bin/sh
if [ "$1" != "--session" ] || [ "$2" != "session-alpha" ] || [ "$3" != "action" ] || [ "$4" != "list-clients" ]; then
  echo "bad argv: $*" >&2
  exit 3
fi
printf 'CLIENT_ID ZELLIJ_PANE_ID RUNNING_COMMAND\n3         terminal_2     /bin/zsh\n1         plugin_9       N/A\n'
"#;
        let (_dir, exe) = write_fake_exe(table);
        let members = super::CliMembershipSource::new(exe, "session-alpha".to_owned())
            .snapshot_members()
            .await
            .expect("table oracle succeeds");
        assert_eq!(members, vec!["1".to_owned(), "3".to_owned()]);
        let rejecting = "#!/bin/sh\nexit 3\n";
        let (_dir, exe) = write_fake_exe(rejecting);
        let failed = super::CliMembershipSource::new(exe, "session-alpha".to_owned())
            .snapshot_members()
            .await
            .expect_err("rejecting oracle fails closed");
        assert_eq!(failed.kind, AdapterErrorKind::Unavailable);
    }
    /// An output flood never buffers unboundedly: the cap is enforced
    /// during the read, so the child is killed and reaped past the bound
    /// and the query fails closed within its timeout.
    #[tokio::test]
    async fn cli_membership_oracle_rejects_output_flood_bounded() {
        let flood =
            "#!/bin/sh\npython3 -c 'import sys; sys.stdout.write(\"x\" * 70000 + \"\\n\")'\n";
        let (_dir, exe) = write_fake_exe(flood);
        let flooded = tokio::time::timeout(
            Duration::from_secs(10),
            super::CliMembershipSource::new(exe, "session-alpha".to_owned()).snapshot_members(),
        )
        .await
        .expect("flood terminates bounded")
        .expect_err("flood fails closed");
        assert_eq!(flooded.kind, AdapterErrorKind::Unavailable);
    }

    /// Suspend parks both production children and resume respawns them over
    /// the pinned argv, against owned fake executables (no live host).
    #[tokio::test]
    async fn activation_suspend_parks_and_resume_revalidates_transport() {
        let dir = tempfile::TempDir::with_prefix("muxe-suspend-").expect("unique temp dir");
        let log = dir.path().join("invocations.log");
        let script = format!(
            r#"#!/bin/sh
LOG="{}"
echo "start: $*" >> "$LOG"
if [ "$1" = "--session" ] && [ "$3" = "action" ] && [ "$4" = "list-clients" ]; then
  exit 3
fi
if [ "$1" != "--session" ] || [ "$3" != "pipe" ] || [ "$4" != "--name" ]; then
  echo "bad argv: $*" >&2
  exit 3
fi
while IFS= read -r line; do
  printf 'got:%s\n' "$line"
done
"#,
            log.display()
        );
        let exe = dir.path().join("zellij");
        std::fs::write(&exe, script).expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        let request = SubprocessChannel::launch(
            exe.clone(),
            "session-alpha".to_owned(),
            "req".to_owned(),
            None,
        )
        .await
        .expect("launches request fake");
        let event = SubprocessChannel::launch(
            exe.clone(),
            "session-alpha".to_owned(),
            "evt".to_owned(),
            Some("subscribe".to_owned()),
        )
        .await
        .expect("launches event fake");
        let adapter = ZellijAdapter::new(
            ZellijAdapterConfig {
                session_name: "session-alpha".to_owned(),
                zellij_exe: exe,
            },
            Arc::clone(&request) as Arc<dyn crate::pipes::PipeChannel>,
            Arc::clone(&event) as Arc<dyn crate::pipes::PipeChannel>,
        );
        // Functional pipe-argv proof while live: the request child speaks
        // the pinned `pipe --name` shape (the fake exits 3 otherwise).
        request
            .send_line("ping\n".to_owned())
            .await
            .expect("live request child accepts writes");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), request.next_line())
                .await
                .expect("live request child answers bounded")
                .expect("live line"),
            "got:ping"
        );
        tokio::time::timeout(Duration::from_secs(10), adapter.suspend_for_activation())
            .await
            .expect("suspend finishes bounded")
            .expect("suspend succeeds");
        // Parked channels accept nothing: the children are gone but the
        // channels stay open for resume.
        assert!(matches!(
            request.send_line("parked\n".to_owned()).await,
            Err(crate::pipes::PipeTransportError::Closed)
        ));
        let failed = tokio::time::timeout(
            Duration::from_secs(20),
            adapter.resume_after_activation_abort(),
        )
        .await
        .expect("resume finishes bounded")
        .expect_err("failed membership query fails closed");
        assert_eq!(failed.kind, AdapterErrorKind::Unavailable);
        // Production-boundary proof: both initial pipe children launched
        // over the pinned argv (the pre-suspend echo round-trip above
        // proves the request child speaks it), plus exactly one scoped
        // list-clients query. Resume ordering proves the respawn: the
        // query runs strictly after both respawns return `Ok` with a live
        // install, so its log line cannot precede them. Respawn echoes are
        // not asserted: the failing attempt parks — killing the
        // just-spawned children, which may die before their first echo
        // under load. Polled, not slept: spawn-to-log races the resume
        // return.
        await_fake_invocations(&log).await;
        assert!(matches!(
            request.send_line("parked\n".to_owned()).await,
            Err(crate::pipes::PipeTransportError::Closed)
        ));
        assert!(matches!(
            event.send_line("parked\n".to_owned()).await,
            Err(crate::pipes::PipeTransportError::Closed)
        ));
        let blocked = adapter
            .modal_scope(&PaneId::new("pane-9"))
            .await
            .expect_err("still suspended after failed resume");
        assert_eq!(blocked.kind, AdapterErrorKind::Unavailable);
        adapter.shutdown().await.expect("shutdown");
    }
    fn origin_request(ui_pane: &str) -> OriginCaptureRequest {
        // Zellij native Run supplies no Herdr bootstrap tuples; broker always
        // supplies ui_pane from attach.
        OriginCaptureRequest {
            ui_session: UiSessionId::new("session-1"),
            ui_pane: PaneId::new(ui_pane),
            origin_hint: None,
            caller_identity: None,
        }
    }

    /// Production-shaped snapshot: session, active tab, and prior-pane kind
    /// all come from the bridge frame, mirroring `request_origin` output.
    fn origin_snapshot(ui_pane: &str, prior: Option<&str>) -> muxe_zellij_protocol::ZellijOrigin {
        muxe_zellij_protocol::ZellijOrigin {
            client_id: "client-1".to_owned(),
            session_name: Some("session-alpha".to_owned()),
            active_tab_index: Some(3),
            active_tab_id: Some(11),
            prior_pane_id: prior.map(str::to_owned),
            ui_pane_id: ui_pane.to_owned(),
            prior_pane_cwd: Some("/work".to_owned()),
            prior_pane_is_plugin: Some(false),
        }
    }

    #[tokio::test]
    async fn capture_origin_resolves_from_ui_pane_without_hint_tuples() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        adapter.inner.snapshots.lock().await.insert(
            "plugin-9".to_owned(),
            origin_snapshot("plugin-9", Some("terminal_2")),
        );
        let origin = adapter
            .capture_origin(origin_request("plugin-9"))
            .await
            .expect("ui pane resolves without hint tuples");
        // The action origin is the bridge PRIOR pane: pane-scoped dispatch
        // must target terminal_2, never the Muxe UI pane plugin-9.
        assert_eq!(
            origin.pane_id.as_ref().map(muxe_core::PaneId::as_str),
            Some("terminal_2")
        );
        assert_eq!(origin.pane_cwd, Some(std::path::PathBuf::from("/work")));
        // Production-shaped capture: session, tab, and pane kind all arrive
        // in the bridge frame with no caller-supplied values.
        assert_eq!(
            origin.session_id.as_ref().map(muxe_core::SessionId::as_str),
            Some("session-alpha")
        );
        assert_eq!(origin.tab_index, Some(3));
        assert_eq!(
            origin.tab_id.as_ref().map(muxe_core::TabId::as_str),
            Some("11")
        );
        assert_eq!(origin.pane_type, Some(muxe_core::OriginPaneType::Terminal));
        adapter.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn capture_origin_rejects_snapshot_for_another_ui_pane() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        adapter.inner.snapshots.lock().await.insert(
            "plugin-9".to_owned(),
            origin_snapshot("plugin-8", Some("terminal_2")),
        );
        let mismatch = adapter
            .capture_origin(origin_request("plugin-9"))
            .await
            .expect_err("foreign snapshot fails");
        assert_eq!(mismatch.kind, AdapterErrorKind::InvalidRequest);
        adapter.shutdown().await.expect("shutdown");
    }

    /// Direct `Run` origin end to end over the injected channels: the adapter
    /// fans a typed `RequestOrigin` out on the request pipe, the bridge answer
    /// returns as a typed `OriginSnapshot` event, and capture resolves with no
    /// Herdr hint/caller tuples. A registration turnover between the request
    /// and the answer is contained: the stale answer fails the registration
    /// check, the fan-out re-queries the refreshed bridge, and the captured
    /// origin plus every pane-scoped consumer target the PRIOR pane, never
    /// the Muxe UI pane.
    #[tokio::test]
    async fn capture_origin_round_trip_targets_prior_pane_after_turnover() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);

        event.push_line(
            encode_event_line(&register_event([7; 16], Some(bridge_build_id())))
                .expect("register line"),
        );

        let worker = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.capture_origin(origin_request("plugin-9")).await }
        });
        let first = decode_request_line(&poll_outbound(&request).await).expect("origin frame");
        assert_eq!(first.target.client_id, "client-1");
        assert_eq!(first.registration, registration_id(7));
        let (session, pane) = match &first.payload {
            BridgeRequest::RequestOrigin {
                ui_session,
                request,
            } => (ui_session.clone(), request.ui_pane.clone()),
            _ => panic!("expected origin fan-out, got {:?}", first.payload),
        };
        assert_eq!(pane, "plugin-9");

        // The bridge re-registers before answering: the stale answer below
        // must fail the registration check so the fan-out re-queries.
        let mut turnover = register_event_for(
            "client-1",
            [8; 16],
            Some(bridge_build_id()),
            env!("CARGO_PKG_VERSION"),
        );
        if let PipeEventKind::Event(BridgeEvent::Register { registration }) = &mut turnover.event {
            registration.current_pane = Some("plugin-9".to_owned());
        }
        event.push_line(encode_event_line(&turnover).expect("turnover encodes"));
        await_turnover_registration(&adapter).await;

        let snapshot = |registration, request_id| {
            pipe_event(
                registration,
                Some(request_id),
                PipeEventKind::Response(BridgeResponse::OriginSnapshot {
                    ui_session: session.clone(),
                    origin: muxe_zellij_protocol::ZellijOrigin {
                        client_id: "client-1".to_owned(),
                        session_name: Some("session-alpha".to_owned()),
                        active_tab_index: Some(3),
                        active_tab_id: Some(11),
                        prior_pane_id: Some("terminal_2".to_owned()),
                        ui_pane_id: "plugin-9".to_owned(),
                        prior_pane_cwd: Some("/work".to_owned()),
                        prior_pane_is_plugin: Some(false),
                    },
                }),
            )
        };
        event.push_line(
            encode_event_line(&snapshot([7; 16], first.request_id)).expect("snapshot encodes"),
        );
        // The refreshed bridge is re-queried with its current registration.
        let second = decode_request_line(&poll_outbound(&request).await).expect("origin frame");
        assert_eq!(second.target.client_id, "client-1");
        assert_eq!(second.registration, registration_id(8));
        event.push_line(
            encode_event_line(&snapshot([8; 16], second.request_id)).expect("snapshot encodes"),
        );
        let origin = worker
            .await
            .expect("capture task")
            .expect("origin captures");
        // The captured action origin is the PRIOR pane, never the UI pane.
        assert_eq!(
            origin.pane_id.as_ref().map(muxe_core::PaneId::as_str),
            Some("terminal_2")
        );
        assert_eq!(origin.pane_cwd, Some(std::path::PathBuf::from("/work")));
        // The captured origin carries the bridge-reported session and tab.
        assert_eq!(
            origin.session_id.as_ref().map(muxe_core::SessionId::as_str),
            Some("session-alpha")
        );
        assert_eq!(origin.tab_index, Some(3));
        assert_eq!(
            origin.tab_id.as_ref().map(muxe_core::TabId::as_str),
            Some("11")
        );

        // Consumer proof: closing the origin pane closes the PRIOR pane, not
        // the Muxe UI that now holds focus.
        let PortableMapping::HostAction { commands } = map_portable(
            &muxe_core::PortableAction::Pane(muxe_core::PaneAction::Close),
            &origin,
        )
        .expect("pane close maps") else {
            panic!("expected host action");
        };
        assert_eq!(commands.len(), 1);
        let RawNativeCommand::RunAction { action, .. } = commands.into_iter().next().expect("one")
        else {
            panic!("expected run-action wrap");
        };
        assert!(matches!(
            action,
            muxe_zellij_protocol::generated::raw::Action::CloseFocusByPaneId {
                pane_id: muxe_zellij_protocol::generated::raw::PaneId::Terminal(2),
            }
        ));
        adapter.shutdown().await.expect("shutdown");
    }

    #[test]
    fn zellij_exe_search_uses_owned_dir_entries() {
        let dir = tempfile::TempDir::with_prefix("muxe-exe-").expect("unique temp dir");
        let exe = dir.path().join("zellij");
        std::fs::write(&exe, "#!/bin/sh\nexit 0\n").expect("fake exe");
        let found = super::find_zellij_in_dirs([dir.path().to_owned()].into_iter())
            .expect("finds owned fake");
        assert_eq!(found, exe);
        assert!(super::find_zellij_in_dirs([dir.path().join("missing-dir")].into_iter()).is_none());
    }

    #[tokio::test]
    async fn shutdown_wake_registered_before_shutdown_is_not_lost() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        let hook = Arc::new(WaitHook {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *adapter
            .inner
            .health_wait_hook
            .lock()
            .expect("health hook is writable") = Some(Arc::clone(&hook));
        let next = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.next_health_event().await }
        });
        hook.entered.notified().await;
        let shutdown = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.shutdown().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !adapter.inner.quiescing.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown enters producer quiescence");
        *adapter
            .inner
            .health_wait_hook
            .lock()
            .expect("health hook is writable") = None;
        hook.release.notify_one();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), next)
                .await
                .expect("health waiter wakes")
                .expect("health waiter task joins"),
            Err(AdapterError {
                kind: AdapterErrorKind::Shutdown,
                ..
            })
        ));
        shutdown
            .await
            .expect("shutdown task joins")
            .expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn shutdown_drains_one_queued_health_event_before_shutdown() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        adapter
            .inner
            .events_tx
            .send(AdapterHealthEvent::DispatchCompleted(
                DispatchCompletion::Succeeded {
                    execution: ExecutionId(91),
                },
            ))
            .await
            .expect("queue terminal health event");
        adapter.shutdown().await.expect("adapter shutdown");
        assert!(matches!(
            adapter.next_health_event().await,
            Ok(AdapterHealthEvent::DispatchCompleted(
                DispatchCompletion::Succeeded {
                    execution: ExecutionId(91)
                }
            ))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), adapter.next_health_event()).await,
            Ok(Err(AdapterError {
                kind: AdapterErrorKind::Shutdown,
                ..
            }))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn quiesce_wake_pre_registration_unblocks_full_health_producer() {
        let request = ScriptedChannel::new();
        let event = ScriptedChannel::new();
        let adapter = test_adapter(&request, &event);
        for _ in 0..256 {
            adapter
                .inner
                .events_tx
                .try_send(AdapterHealthEvent::Unhealthy {
                    modal_scope: None,
                    error: AdapterError::new(AdapterErrorKind::Unavailable, "fill health queue"),
                })
                .expect("fill bounded health queue");
        }
        let hook = Arc::new(WaitHook {
            entered: Notify::new(),
            release: Notify::new(),
        });
        *adapter
            .inner
            .emit_wait_hook
            .lock()
            .expect("emit hook is writable") = Some(Arc::clone(&hook));
        let execution = ExecutionId(42);
        let provenance = RequestProvenance {
            request_id: RequestId::INITIAL,
            registration: registration_id(1),
        };
        let generation = ChannelGeneration::INITIAL;
        *adapter.inner.in_flight.lock().await = Some(InFlight {
            request: provenance,
            generation,
            execution: Some(execution),
            request_slot: Some(RequestId::INITIAL),
            settled: false,
        });
        adapter.inner.live_executions.lock().await.insert(
            execution.0,
            ExecutionRecord {
                execution,
                total: 1,
                requests: BTreeMap::from([(RequestId::INITIAL, Some(provenance))]),
                worst: None,
                settled: 0,
            },
        );
        adapter.watch_release(provenance, generation);
        tokio::time::advance(RELEASE_TIMEOUT).await;
        hook.entered.notified().await;
        let shutdown = tokio::spawn({
            let adapter = adapter.clone();
            async move { adapter.shutdown().await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !adapter.inner.quiescing.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown enters producer quiescence");
        *adapter
            .inner
            .emit_wait_hook
            .lock()
            .expect("emit hook is writable") = None;
        hook.release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), shutdown)
            .await
            .expect("shutdown joins within bounded timeout")
            .expect("shutdown task joins")
            .expect("shutdown succeeds with full health queue");
        assert!(adapter.inner.in_flight.lock().await.is_none());
        assert!(
            !adapter
                .inner
                .live_executions
                .lock()
                .await
                .contains_key(&execution.0)
        );
    }
}
