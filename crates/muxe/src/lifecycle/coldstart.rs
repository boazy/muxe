//! On-demand broker startup for native consumers (launcher, UI attach).
//!
//! A launcher or UI first probes the deterministic normal endpoint. When no
//! live broker answers, the consumer cold-starts one ordinary broker through
//! the endpoint startup lock and verifies its identity before attaching.
//! Concurrent starters serialize on the lock with a post-acquire liveness
//! recheck, so exactly one startup attempt proceeds: a single endpoint
//! startup attempt, verified ready identity, and no overlapping host adapter
//! owners. The lock is dropped before awaiting readiness because the child
//! re-acquires the same lock in `BrokerServer::start_inner`; holding it
//! across the wait would deadlock parent against child.
//!
//! There is no retry framework here: one bounded readiness wait, fail closed
//! on expiry. A live broker with the wrong identity is never replaced by a
//! second broker; a live broker with a stale compiled record is reported for
//! the caller to drive through the same activation transaction as CLI
//! `activate` before attaching.

use std::{
    fs,
    num::NonZeroU32,
    os::unix::{
        ffi::OsStrExt,
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixStream,
    },
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use muxe_broker::{RuntimeEndpoint, RuntimeError, ServeHerdrSpawn, ServeZellijSpawn};
use muxe_protocol::{
    control::{ActivationPhase, ActivationStatus, CompatibilityRecord, LifecycleState},
    wire::{HostKind, ServerId},
};
use nix::{
    errno::Errno,
    sys::signal::kill,
    unistd::{Pid, Uid},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    activate::{BrokerSpawner, ControlPort, HostReloader, SpawnRequest, TargetHandle},
    control::{ControlError, VerifiedControlStatus},
    journal::{self, UnitKind, UnitLock, UnitLockAttempt},
    registry::{BridgeMemberId, BridgeUnitGuard, BrokerEntry, Registry, RegistryError},
};
use crate::{fsutil, integration, paths::BridgeIdentity};

/// Host provenance for one cold-started ordinary broker.
#[derive(Clone, Debug)]
pub enum ColdstartHost {
    /// Live Zellij session plus the pinned executable serving it.
    Zellij {
        session: String,
        zellij_exe: PathBuf,
    },
    /// Live Herdr discovery key plus the binaries and socket serving it.
    Herdr {
        discovery_key: String,
        live_server_id: ServerId,
        herdr_binary: PathBuf,
        herdr_socket: PathBuf,
    },
}

/// Inputs for [`ensure_broker`]. All paths are absolute; nothing is read from
/// ambient environment inside this module.
pub struct ColdstartInputs<'a, S, C, R> {
    /// Owner-only cache directory holding the broker registry.
    pub cache_dir: &'a Path,
    /// Absolute broker configuration file the child loads and derives the
    /// stable bridge path from.
    pub config_file: &'a Path,
    /// The running `muxe` executable re-spawned as the broker child.
    pub executable: &'a Path,
    /// Deterministic normal endpoint for this host.
    pub endpoint: RuntimeEndpoint,
    /// Host provenance for the spawned child.
    pub host: ColdstartHost,
    /// Compiled compatibility record of this executable: a live broker
    /// serving any other record is stale and must activate before attach.
    pub current_record: CompatibilityRecord,
    /// Process mechanics for the single spawned child.
    pub spawner: &'a S,
    /// Coordinator control port for identity verification.
    pub control: &'a C,
    /// Zellij bridge reloader; required for `Zellij`, unused for `Herdr`.
    pub reloader: Option<&'a R>,
    /// Outer bound for the single readiness wait.
    pub readiness_deadline: Duration,
    /// Poll interval while awaiting the spawned broker.
    pub poll_interval: Duration,
}

/// A live broker speaking for the expected host identity.
#[derive(Clone, Debug)]
pub struct LiveBroker {
    /// Registry entry of the live broker.
    pub entry: BrokerEntry,
    /// Control status proving identity (and record) before attach.
    pub status: ActivationStatus,
}

/// Outcome of [`ensure_broker`].
#[derive(Clone, Debug)]
pub enum ColdstartOutcome {
    /// A live broker serves this executable's compiled record: attach.
    Ready(LiveBroker),
    /// A live broker serves a stale record: drive it through the same
    /// activation transaction as CLI `activate` before attaching.
    StaleRecord(LiveBroker),
}

/// Coldstart failure. Every variant fails closed without spawning a second
/// broker or mutating coordinator state.
#[derive(Debug, Error)]
pub enum ColdstartError {
    /// Owner-only registry access failed.
    #[error("could not access the owner-only broker registry: {0}")]
    Registry(#[from] RegistryError),
    /// Startup-lock acquisition failed with no winner to await.
    #[error("could not serialize broker startup: {0}")]
    Startup(String),
    /// Child process mechanics failed.
    #[error("could not start the broker child: {0}")]
    Spawn(String),
    /// Stable bridge reload failed after spawning a Zellij broker.
    #[error("could not reload the stable bridge: {0}")]
    Reload(String),
    /// A live broker answers for a different host identity: never start a
    /// second broker over it.
    #[error("live broker serves {found} but the caller needs {expected}; refusing a second broker")]
    IdentityMismatch { expected: String, found: String },
    /// No verified broker answered before the readiness deadline.
    #[error("no verified broker answered before the readiness deadline")]
    StartupTimeout,
    #[error("verified endpoint conflicts with registry authority: {0}")]
    EndpointConflict(String),
    #[error("legacy control Status with a handoff is ambiguous and cannot admit UI")]
    LegacyActivationAmbiguous,
}

/// Unit ownership is held through endpoint observation and registry mutation.
enum HeldUnit {
    Herdr { _lock: UnitLock },
    Zellij(BridgeUnitGuard),
}

impl HeldUnit {
    fn bridge_guard(&self) -> Option<&BridgeUnitGuard> {
        match self {
            Self::Herdr { .. } => None,
            Self::Zellij(guard) => Some(guard),
        }
    }
}

/// Durable in-flight child ownership prevents another parent from spawning
/// while the first released unit/startup guards for child registration.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(transparent)]
struct ColdstartProcessId(NonZeroU32);

impl ColdstartProcessId {
    fn from_pid(pid: u32) -> Result<Self, ColdstartError> {
        NonZeroU32::new(pid)
            .map(Self)
            .ok_or_else(|| ColdstartError::Spawn("coldstart process has zero PID".to_owned()))
    }

    fn get(self) -> u32 {
        self.0.get()
    }
}

/// One caller's random startup claim, distinct even for concurrent requests
/// within the same process.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
struct ColdstartAttemptId([u8; 16]);

impl ColdstartAttemptId {
    fn generate() -> Result<Self, ColdstartError> {
        let mut bytes = [0; 16];
        getrandom::getrandom(&mut bytes).map_err(|error| {
            ColdstartError::Startup(format!("cannot mint startup claim: {error}"))
        })?;
        (bytes != [0; 16])
            .then_some(Self(bytes))
            .ok_or_else(|| ColdstartError::Startup("startup claim was zero".to_owned()))
    }
}

impl<'de> Deserialize<'de> for ColdstartAttemptId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <[u8; 16]>::deserialize(deserializer)?;
        (bytes != [0; 16])
            .then_some(Self(bytes))
            .ok_or_else(|| serde::de::Error::custom("zero startup claim"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct ColdstartSpawnIntent {
    schema_version: u32,
    unit: UnitKind,
    attempt: ColdstartAttemptId,
    endpoint: PathBuf,
    owner: ColdstartProcessId,
    child: Option<ColdstartProcessId>,
}

fn spawn_intent_path(cache_dir: &Path, endpoint: &RuntimeEndpoint) -> PathBuf {
    let digest = fsutil::sha256_hex(endpoint.socket().as_os_str().as_bytes());
    journal::activation_dir(cache_dir).join(format!(".muxe-coldstart-{}.pending", &digest[..32]))
}

fn read_spawn_intent(
    path: &Path,
    unit: &UnitKind,
    endpoint: &RuntimeEndpoint,
) -> Result<Option<ColdstartSpawnIntent>, ColdstartError> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(ColdstartError::EndpointConflict(format!(
                "cannot inspect coldstart ownership at {}: {error}",
                path.display()
            )));
        }
        Ok(_) => {}
    }
    let bytes = fsutil::read_owner_file(path)
        .map_err(|error| ColdstartError::EndpointConflict(error.to_string()))?;
    let intent: ColdstartSpawnIntent = serde_json::from_slice(&bytes).map_err(|error| {
        ColdstartError::EndpointConflict(format!("invalid coldstart intent: {error}"))
    })?;
    if intent.schema_version != 1 || &intent.unit != unit || intent.endpoint != endpoint.socket() {
        return Err(ColdstartError::EndpointConflict(
            "coldstart intent belongs to another unit or endpoint".to_owned(),
        ));
    }
    Ok(Some(intent))
}

fn publish_spawn_intent(path: &Path, intent: &ColdstartSpawnIntent) -> Result<(), ColdstartError> {
    let bytes = serde_json::to_vec(intent).map_err(|error| {
        ColdstartError::Spawn(format!("cannot encode coldstart intent: {error}"))
    })?;
    fsutil::write_atomic_new(path, &bytes, "coldstart")
        .map_err(|error| ColdstartError::Spawn(format!("cannot persist child ownership: {error}")))
}

fn update_spawn_intent(path: &Path, intent: &ColdstartSpawnIntent) -> Result<(), ColdstartError> {
    let bytes = serde_json::to_vec(intent)
        .map_err(|error| ColdstartError::Spawn(format!("cannot encode child PID: {error}")))?;
    fsutil::write_atomic(path, &bytes, "coldstart")
        .map_err(|error| ColdstartError::Spawn(format!("cannot persist child PID: {error}")))?;
    Ok(())
}

fn clear_spawn_intent(path: &Path) -> Result<(), ColdstartError> {
    fsutil::remove_file_durable(path, "removing coldstart child intent")
        .map(|_| ())
        .map_err(|error| ColdstartError::EndpointConflict(error.to_string()))
}

fn try_held_unit(
    cache_dir: &Path,
    unit: &UnitKind,
    bridge: Option<&BridgeIdentity>,
) -> Result<Option<HeldUnit>, ColdstartError> {
    if let Some(identity) = bridge {
        return BridgeUnitGuard::try_acquire(cache_dir, identity.clone())
            .map(|guard| guard.map(HeldUnit::Zellij))
            .map_err(ColdstartError::Registry);
    }
    match journal::try_acquire_unit_lock(cache_dir, unit)
        .map_err(|error| ColdstartError::Startup(error.to_string()))?
    {
        UnitLockAttempt::Acquired(lock) => Ok(Some(HeldUnit::Herdr { _lock: lock })),
        UnitLockAttempt::Active => Ok(None),
    }
}

/// Ensures one live broker for the expected host identity, cold-starting an
/// ordinary broker when none answers.
///
/// Decisions hold unit then deterministic endpoint ownership. An exact
/// journal check precedes every probe, registry mutation, and spawn.
/// A live endpoint's Status and process credentials come from one stream.
///
/// # Errors
///
/// Returns [`ColdstartError`] when registry access, serialization, spawning,
/// reload, identity verification, or the bounded wait fails.
#[expect(
    clippy::too_many_lines,
    reason = "coldstart keeps unit-lock, endpoint-lock, PID authority, spawn, reload, and readiness ordering in one auditable transaction"
)]
pub async fn ensure_broker<S, C, R>(
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<ColdstartOutcome, ColdstartError>
where
    S: BrokerSpawner,
    C: ControlPort,
    R: HostReloader,
{
    let deadline = Instant::now() + inputs.readiness_deadline;
    let bridge = expected_bridge_identity(&inputs.host, inputs.config_file)?;
    let unit = activation_unit(&inputs.host, bridge.as_ref())?;
    let pending_path = spawn_intent_path(inputs.cache_dir, &inputs.endpoint);
    let attempt = ColdstartAttemptId::generate()?;
    let mut ownership: Option<HeldUnit> = None;
    let mut spawned: Option<TargetHandle> = None;
    let mut prior_socket: Option<StaleEndpointObservation> = None;

    loop {
        if ownership.is_none() {
            ownership = try_held_unit(inputs.cache_dir, &unit, bridge.as_ref())?;
            if ownership.is_none() {
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            }
        }
        // A different caller's durable claim is observed before attempting
        // the endpoint lock, so it cannot steal the child startup window.
        ensure_no_activation_journal(inputs.cache_dir, &unit)?;
        if let Some(intent) = read_spawn_intent(&pending_path, &unit, &inputs.endpoint)?
            && intent.attempt != attempt
        {
            if intent.child.is_none() && recorded_process_is_dead(intent.owner.get())? {
                return Err(ColdstartError::EndpointConflict(
                    "prior starter died before recording its child".to_owned(),
                ));
            }
            let active_pid = intent.child.unwrap_or(intent.owner);
            if !recorded_process_is_dead(active_pid.get())? {
                drop(ownership.take());
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            }
        }
        // A nofollow readiness hint, not an authority probe: the child must
        // claim the endpoint before its parent can take the startup guard again.
        if let (Some(child), Some(previous)) = (spawned.as_mut(), prior_socket) {
            if child
                .child
                .try_wait()
                .map_err(|error| ColdstartError::Startup(error.to_string()))?
                .is_some()
            {
                return Err(ColdstartError::Startup(
                    "owned broker child exited before registration".to_owned(),
                ));
            }
            if !child_socket_replaced(previous, inputs.endpoint.socket())? {
                drop(ownership.take());
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            }
        }
        let startup_lock = match inputs.endpoint.acquire_startup_lock() {
            Ok(lock) => lock,
            Err(RuntimeError::StartupInProgress(_)) => {
                drop(ownership.take());
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            }
            Err(error) => {
                stop_spawned(inputs.spawner, spawned.take());
                return Err(ColdstartError::Startup(error.to_string()));
            }
        };
        if let Err(error) = ensure_no_activation_journal(inputs.cache_dir, &unit) {
            stop_spawned(inputs.spawner, spawned.take());
            return Err(error);
        }
        let verified = match probe_endpoint(inputs.control, inputs.endpoint.socket()).await {
            Ok(verified) => verified,
            Err(error) => {
                stop_spawned(inputs.spawner, spawned.take());
                return Err(error);
            }
        };
        let pending = read_spawn_intent(&pending_path, &unit, &inputs.endpoint)?;
        if let (Some(intent), Some(verified)) = (&pending, &verified)
            && intent
                .child
                .is_some_and(|child| child.get() != verified.authority.process().get())
        {
            stop_spawned(inputs.spawner, spawned.take());
            return Err(ColdstartError::EndpointConflict(
                "live endpoint differs from the pending owned child".to_owned(),
            ));
        }
        let registry = Registry::open(inputs.cache_dir)?;
        let entries = registry.entries()?;
        if let Some(verified) = verified {
            let entry = match reconcile_live_endpoint(
                &registry,
                &entries,
                &verified,
                inputs,
                ownership.as_ref().and_then(HeldUnit::bridge_guard),
                bridge.as_ref(),
            ) {
                Ok(entry) => entry,
                Err(error) => {
                    stop_spawned(inputs.spawner, spawned.take());
                    return Err(error);
                }
            };
            let outcome = classify(&entry, verified.status, inputs);
            if pending.is_some() {
                clear_spawn_intent(&pending_path)?;
            }
            if let Some(child) = spawned.as_mut() {
                child.surrender_to_live_broker();
            }
            drop(spawned.take());
            drop(startup_lock);
            drop(ownership.take());
            return Ok(outcome);
        }
        if let Some(intent) = pending {
            let Some(child) = intent.child else {
                if !recorded_process_is_dead(intent.owner.get())? {
                    drop(startup_lock);
                    drop(ownership.take());
                    wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                        .await?;
                    continue;
                }
                return Err(ColdstartError::EndpointConflict(
                    "coldstart parent died before recording its child; preserve ambiguous ownership"
                        .to_owned(),
                ));
            };
            if !recorded_process_is_dead(child.get())? {
                drop(startup_lock);
                drop(ownership.take());
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            }
            stop_spawned(inputs.spawner, spawned.take());
            clear_spawn_intent(&pending_path)?;
        }
        if spawned.is_some() {
            drop(startup_lock);
            drop(ownership.take());
            wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval).await?;
            continue;
        }
        let observed = reconcile_absent_endpoint(
            &registry,
            &entries,
            inputs,
            ownership.as_ref().and_then(HeldUnit::bridge_guard),
            bridge.as_ref(),
        )?;
        prior_socket = Some(observed);
        let request = coldstart_spawn_request(inputs).map_err(ColdstartError::Spawn)?;
        let mut intent = ColdstartSpawnIntent {
            schema_version: 1,
            attempt,
            unit: unit.clone(),
            endpoint: inputs.endpoint.socket().to_path_buf(),
            owner: ColdstartProcessId::from_pid(std::process::id())?,
            child: None,
        };
        publish_spawn_intent(&pending_path, &intent)?;
        // A durable owner intent prevents a sibling parent from spawning.
        // Release the endpoint guard before starting the child, which acquires
        // that guard before bind; retain the unit until its PID is recorded.
        drop(startup_lock);
        let mut child = match inputs.spawner.spawn_target(&request) {
            Ok(child) => child,
            Err(error) => {
                clear_spawn_intent(&pending_path)?;
                return Err(ColdstartError::Spawn(error.to_string()));
            }
        };
        intent.child = Some(ColdstartProcessId::from_pid(child.child.id())?);
        if let Err(error) = update_spawn_intent(&pending_path, &intent) {
            let _ = inputs.spawner.stop_target(&mut child);
            clear_spawn_intent(&pending_path)?;
            return Err(error);
        }
        spawned = Some(child);
        // The child needs the shared unit guard for registration. Do not hold
        // it while waiting for readiness; the durable PID intent bridges the gap.
        drop(ownership.take());
        if let ColdstartHost::Zellij { session, .. } = &inputs.host {
            let Some(identity) = bridge.as_ref() else {
                stop_spawned(inputs.spawner, spawned.take());
                return Err(ColdstartError::Startup(
                    "Zellij coldstart lost its canonical bridge identity".to_owned(),
                ));
            };
            let bridge_url = integration::kdl::bridge_url(
                &identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME)),
            );
            let reloader = inputs.reloader.ok_or_else(|| {
                ColdstartError::Reload("no Zellij bridge reloader for coldstart".to_owned())
            })?;
            if let Err(error) = reloader.reload_bridge(session, &bridge_url) {
                stop_spawned(inputs.spawner, spawned.take());
                return Err(ColdstartError::Reload(error.to_string()));
            }
        }
        wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval).await?;
    }
}

/// Waits before the next ownership/readiness check, stopping an owned child on timeout.
async fn wait_before_retry<S>(
    spawner: &S,
    child: &mut Option<TargetHandle>,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ColdstartError>
where
    S: BrokerSpawner,
{
    if Instant::now() >= deadline {
        stop_spawned(spawner, child.take());
        return Err(ColdstartError::StartupTimeout);
    }
    tokio::time::sleep(poll_interval).await;
    Ok(())
}

/// One retained control stream supplies Status and authenticated peer identity.
async fn probe_endpoint<C>(
    control: &C,
    socket: &Path,
) -> Result<Option<VerifiedControlStatus>, ColdstartError>
where
    C: ControlPort,
{
    match control.verified_status(socket).await {
        Ok(verified) => Ok(Some(verified)),
        Err(ControlError::Connect { source, .. })
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(ColdstartError::Startup(format!(
            "authenticated Status at {} failed: {error}",
            socket.display()
        ))),
    }
}

/// Classifies a previously authenticated and reconciled live broker.
fn classify<S, C, R>(
    entry: &BrokerEntry,
    status: ActivationStatus,
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> ColdstartOutcome {
    let live = LiveBroker {
        entry: entry.clone(),
        status,
    };
    if live.status.current == inputs.current_record {
        ColdstartOutcome::Ready(live)
    } else {
        ColdstartOutcome::StaleRecord(live)
    }
}

/// Attests host, incarnation, bridge, and attachable activation phase before
/// any registry reconciliation or UI attach.
fn validate_status_identity<S, C, R>(
    status: &ActivationStatus,
    inputs: &ColdstartInputs<'_, S, C, R>,
    bridge: Option<&BridgeIdentity>,
) -> Result<(), ColdstartError> {
    let expected_host = match inputs.host {
        ColdstartHost::Herdr { .. } => HostKind::Herdr,
        ColdstartHost::Zellij { .. } => HostKind::Zellij,
    };
    let expected_discovery = expected_identity(&inputs.host);
    let expected_incarnation = match &inputs.host {
        ColdstartHost::Herdr { live_server_id, .. } => Some(live_server_id),
        ColdstartHost::Zellij { .. } => None,
    };
    if status.live_server.host != expected_host
        || status.live_server.discovery_key != expected_discovery
        || expected_incarnation.is_some_and(|id| status.live_server.server_id != *id)
        || status.bridge_unit != bridge.map(BridgeIdentity::unit)
    {
        return Err(ColdstartError::IdentityMismatch {
            expected: format!(
                "{expected_host:?}/{expected_discovery}/{expected_incarnation:?}/{:?}",
                bridge.map(BridgeIdentity::unit)
            ),
            found: format!(
                "{:?}/{}/{}/{:?}",
                status.live_server.host,
                status.live_server.discovery_key,
                status.live_server.server_id.as_str(),
                status.bridge_unit
            ),
        });
    }
    let attachable = match status.phase {
        ActivationPhase::Ordinary => {
            status.lifecycle == LifecycleState::Running
                && status.handoff_id.is_none()
                && status.target.is_none()
        }
        ActivationPhase::TargetCommitted => {
            status.lifecycle == LifecycleState::Running
                && status.handoff_id.is_some()
                && status.target.is_none()
        }
        ActivationPhase::Legacy
            if status.lifecycle == LifecycleState::Running
                && status.handoff_id.is_none()
                && status.target.is_none() =>
        {
            true
        }
        ActivationPhase::Legacy
            if status.lifecycle == LifecycleState::Running && status.handoff_id.is_some() =>
        {
            return Err(ColdstartError::LegacyActivationAmbiguous);
        }
        ActivationPhase::Legacy
        | ActivationPhase::Preparing
        | ActivationPhase::Draining
        | ActivationPhase::SupervisorOnly
        | ActivationPhase::TargetGated
        | ActivationPhase::Retired => false,
    };
    if !attachable {
        return Err(ColdstartError::EndpointConflict(format!(
            "broker is not attachable: {:?}/{:?}/handoff={}/target={}",
            status.lifecycle,
            status.phase,
            status.handoff_id.is_some(),
            status.target.is_some()
        )));
    }
    Ok(())
}

/// Reuses, adopts, or atomically relocates the exact live owner. A peer
/// without an attested registration may only reuse its unchanged existing row.
fn reconcile_live_endpoint<S, C, R>(
    registry: &Registry,
    entries: &[BrokerEntry],
    verified: &VerifiedControlStatus,
    inputs: &ColdstartInputs<'_, S, C, R>,
    guard: Option<&BridgeUnitGuard>,
    bridge: Option<&BridgeIdentity>,
) -> Result<BrokerEntry, ColdstartError> {
    validate_status_identity(&verified.status, inputs, bridge)?;
    if verified.authority.endpoint() != inputs.endpoint.socket() {
        return Err(ColdstartError::EndpointConflict(
            "control stream is bound to a different endpoint".to_owned(),
        ));
    }
    let host_kind = match inputs.host {
        ColdstartHost::Herdr { .. } => "herdr",
        ColdstartHost::Zellij { .. } => "zellij",
    };
    let discovery = expected_identity(&inputs.host);
    let server_id = verified.status.live_server.server_id.as_str();
    let process = verified.authority.process().get();
    let expected_member = bridge
        .map(|_| BridgeMemberId::new(discovery.to_owned()))
        .transpose()?;
    if verified.status.registration.is_none() {
        let mut matching = entries
            .iter()
            .filter(|known| known.host_kind == host_kind && known.discovery_key == discovery);
        let Some(known) = matching.next().filter(|_| matching.next().is_none()) else {
            return Err(ColdstartError::EndpointConflict(
                "legacy peer lacks registration authority for adoption or relocation".to_owned(),
            ));
        };
        if known.socket != inputs.endpoint.socket()
            || known.server_pid != process
            || known.started_at == 0
            || known.live_server.as_deref() != Some(server_id)
            || known.bridge_identity.as_ref() != bridge
            || known.bridge_member != expected_member
            || known.handoff_id != bridge.and(verified.status.handoff_id)
        {
            return Err(ColdstartError::EndpointConflict(
                "legacy peer differs from its exact recorded endpoint".to_owned(),
            ));
        }
        registry.verify_existing(guard, entries, known, || {
            verified
                .authority
                .verify_path()
                .map_err(|error| RegistryError::Conflict(error.to_string()))
        })?;
        return Ok(known.clone());
    }
    let proof = verified
        .status
        .registration
        .expect("checked registration attestation");
    let candidate = BrokerEntry {
        host_kind: host_kind.to_owned(),
        discovery_key: discovery.to_owned(),
        socket: verified.authority.endpoint().to_path_buf(),
        server_pid: process,
        started_at: proof.started_at,
        registration_id: Some(proof.id),
        bridge_identity: bridge.cloned(),
        bridge_member: expected_member,
        handoff_id: bridge.and(verified.status.handoff_id),
        live_server: Some(server_id.to_owned()),
    };
    registry
        .reconcile_live(guard, entries, candidate, || {
            verified
                .authority
                .verify_path()
                .map_err(|error| RegistryError::Conflict(error.to_string()))
        })
        .map_err(ColdstartError::Registry)
}

/// Renders the exact ordinary serve request for one cold-started broker: the
/// current executable plus the broker-authored serve arguments with no
/// handoff pair, derived from caller-owned paths only.
fn coldstart_spawn_request<S, C, R>(
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<SpawnRequest, String> {
    let socket = inputs.endpoint.socket().to_path_buf();
    let program = inputs.executable.to_path_buf();
    let args = match &inputs.host {
        ColdstartHost::Zellij {
            session,
            zellij_exe,
        } => {
            let spawn = ServeZellijSpawn {
                binary: program.clone(),
                socket,
                zellij_exe: zellij_exe.clone(),
                session: session.clone(),
                config: inputs.config_file.to_path_buf(),
                cache_dir: inputs.cache_dir.to_path_buf(),
                handoff: None,
                activation_journal: None,
            };
            spawn.argv().map_err(|error| error.to_string())?
        }
        ColdstartHost::Herdr {
            herdr_binary,
            herdr_socket,
            ..
        } => {
            let spawn = ServeHerdrSpawn {
                binary: program.clone(),
                socket,
                herdr_binary: herdr_binary.clone(),
                herdr_socket: herdr_socket.clone(),
                config: inputs.config_file.to_path_buf(),
                cache_dir: inputs.cache_dir.to_path_buf(),
                handoff: None,
                activation_journal: None,
            };
            spawn.argv().map_err(|error| error.to_string())?
        }
    };
    Ok(SpawnRequest { program, args })
}

/// Canonical bridge authority is resolved once before acquiring the unit.
fn expected_bridge_identity(
    host: &ColdstartHost,
    config_file: &Path,
) -> Result<Option<BridgeIdentity>, ColdstartError> {
    match host {
        ColdstartHost::Herdr { .. } => Ok(None),
        ColdstartHost::Zellij { .. } => integration::bridge_identity(parent_of(config_file)?)
            .map(Some)
            .map_err(|error| ColdstartError::Startup(error.to_string())),
    }
}

fn activation_unit(
    host: &ColdstartHost,
    bridge: Option<&BridgeIdentity>,
) -> Result<UnitKind, ColdstartError> {
    match host {
        ColdstartHost::Herdr { discovery_key, .. } => Ok(UnitKind::Herdr {
            host_hash: journal::unit_hash(discovery_key),
        }),
        ColdstartHost::Zellij { .. } => Ok(UnitKind::Zellij {
            bridge_unit: bridge
                .ok_or_else(|| ColdstartError::Startup("missing canonical bridge".to_owned()))?
                .unit(),
        }),
    }
}

fn ensure_no_activation_journal(cache_dir: &Path, unit: &UnitKind) -> Result<(), ColdstartError> {
    let path = journal::activation_dir(cache_dir).join(unit.journal_name());
    match fs::symlink_metadata(&path) {
        Ok(_) => Err(ColdstartError::Startup(format!(
            "activation recovery is pending at {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ColdstartError::Startup(format!(
            "cannot inspect activation journal at {}: {error}",
            path.display()
        ))),
    }
}

/// Exact nofollow state of an endpoint that did not answer control Status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StaleEndpointObservation {
    Absent,
    RefusedSocket {
        device: u64,
        inode: u64,
        owner: u32,
        mode: u32,
    },
}

fn observe_stale_endpoint(path: &Path) -> Result<StaleEndpointObservation, ColdstartError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(StaleEndpointObservation::Absent);
        }
        Err(error) => {
            return Err(ColdstartError::EndpointConflict(format!(
                "cannot inspect {} without following links: {error}",
                path.display()
            )));
        }
    };
    let mode = metadata.permissions().mode() & 0o777;
    if !metadata.file_type().is_socket()
        || metadata.uid() != Uid::current().as_raw()
        || mode != 0o600
    {
        return Err(ColdstartError::EndpointConflict(format!(
            "stale endpoint {} is not an owner-only socket",
            path.display()
        )));
    }
    match UnixStream::connect(path) {
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
            let after = fs::symlink_metadata(path).map_err(|error| {
                ColdstartError::EndpointConflict(format!(
                    "stale endpoint {} changed after refusal: {error}",
                    path.display()
                ))
            })?;
            if !after.file_type().is_socket()
                || after.dev() != metadata.dev()
                || after.ino() != metadata.ino()
                || after.uid() != metadata.uid()
                || after.permissions().mode() & 0o777 != mode
            {
                return Err(ColdstartError::EndpointConflict(
                    "stale socket identity changed during observation".to_owned(),
                ));
            }
            Ok(StaleEndpointObservation::RefusedSocket {
                device: metadata.dev(),
                inode: metadata.ino(),
                owner: metadata.uid(),
                mode,
            })
        }
        Ok(_) => Err(ColdstartError::EndpointConflict(
            "endpoint became live after Status refusal".to_owned(),
        )),
        Err(error) => Err(ColdstartError::EndpointConflict(format!(
            "endpoint {} cannot be proven absent/refused: {error}",
            path.display()
        ))),
    }
}

fn child_socket_replaced(
    previous: StaleEndpointObservation,
    path: &Path,
) -> Result<bool, ColdstartError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(ColdstartError::EndpointConflict(format!(
                "cannot inspect starting child endpoint {}: {error}",
                path.display()
            )));
        }
    };
    if !metadata.file_type().is_socket()
        || metadata.uid() != Uid::current().as_raw()
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(ColdstartError::EndpointConflict(
            "starting child endpoint lost owner-only socket identity".to_owned(),
        ));
    }
    Ok(match previous {
        StaleEndpointObservation::Absent => true,
        StaleEndpointObservation::RefusedSocket { device, inode, .. } => {
            metadata.dev() != device || metadata.ino() != inode
        }
    })
}

fn reconcile_absent_endpoint<S, C, R>(
    registry: &Registry,
    entries: &[BrokerEntry],
    inputs: &ColdstartInputs<'_, S, C, R>,
    guard: Option<&BridgeUnitGuard>,
    bridge: Option<&BridgeIdentity>,
) -> Result<StaleEndpointObservation, ColdstartError> {
    let socket = inputs.endpoint.socket();
    let discovery = expected_identity(&inputs.host);
    let kind = match inputs.host {
        ColdstartHost::Herdr { .. } => "herdr",
        ColdstartHost::Zellij { .. } => "zellij",
    };
    if entries.iter().any(|entry| {
        entry.socket != socket && entry.host_kind == kind && entry.discovery_key == discovery
    }) {
        return Err(ColdstartError::EndpointConflict(
            "recorded logical broker owns another endpoint".to_owned(),
        ));
    }
    let mut at_endpoint = entries.iter().filter(|entry| entry.socket == socket);
    let stale = at_endpoint.next();
    if at_endpoint.next().is_some() {
        return Err(ColdstartError::EndpointConflict(
            "multiple registry rows occupy the deterministic endpoint".to_owned(),
        ));
    }
    let observation = observe_stale_endpoint(socket)?;
    let Some(stale) = stale else {
        return Ok(observation);
    };
    let expected_member = bridge
        .map(|_| BridgeMemberId::new(discovery.to_owned()))
        .transpose()?;
    if stale.host_kind != kind
        || stale.discovery_key != discovery
        || stale.bridge_identity.as_ref() != bridge
        || stale.bridge_member != expected_member
        || stale.started_at == 0
        || stale
            .registration_id
            .is_none_or(muxe_protocol::control::BrokerRegistrationId::is_zero)
        || stale.live_server.as_deref().is_none_or(str::is_empty)
        || matches!(
            &inputs.host,
            ColdstartHost::Herdr { live_server_id, .. }
                if stale.live_server.as_deref() != Some(live_server_id.as_str())
        )
    {
        return Err(ColdstartError::EndpointConflict(
            "stale row differs from the exact expected host, incarnation, or owner".to_owned(),
        ));
    }
    if !recorded_process_is_dead(stale.server_pid)? {
        return Err(ColdstartError::EndpointConflict(format!(
            "recorded broker PID {} is live or reused",
            stale.server_pid
        )));
    }
    registry.remove_exact_stale(guard, entries, stale, || {
        if !recorded_process_is_dead(stale.server_pid)
            .map_err(|error| RegistryError::Conflict(error.to_string()))?
            || observe_stale_endpoint(socket)
                .map_err(|error| RegistryError::Conflict(error.to_string()))?
                != observation
        {
            return Err(RegistryError::Conflict(
                "stale process or socket changed before removal".to_owned(),
            ));
        }
        Ok(())
    })?;
    Ok(observation)
}

/// Returns true only when the registry PID is valid and no process owns it.
fn recorded_process_is_dead(server_pid: u32) -> Result<bool, ColdstartError> {
    let pid = i32::try_from(server_pid)
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| {
            ColdstartError::Startup(format!(
                "registry carries invalid broker PID {server_pid}; refusing replacement"
            ))
        })?;
    match kill(Pid::from_raw(pid), None) {
        Err(Errno::ESRCH) => Ok(true),
        Ok(()) | Err(Errno::EPERM) => Ok(false),
        Err(error) => Err(ColdstartError::Startup(format!(
            "could not probe recorded broker PID {server_pid}: {error}"
        ))),
    }
}

/// Expected control identity for one coldstart host.
fn expected_identity(host: &ColdstartHost) -> &str {
    match host {
        ColdstartHost::Zellij { session, .. } => session,
        ColdstartHost::Herdr { discovery_key, .. } => discovery_key,
    }
}

/// Parent directory of an absolute config file, fail closed.
fn parent_of(config_file: &Path) -> Result<&Path, ColdstartError> {
    config_file.parent().ok_or_else(|| {
        ColdstartError::Spawn("broker configuration file has no parent directory".to_owned())
    })
}

/// Stops a spawned child after a failed coldstart so no orphaned host adapter
/// owner survives; best effort, the coldstart error stays authoritative.
fn stop_spawned<S>(spawner: &S, mut child: Option<TargetHandle>)
where
    S: BrokerSpawner,
{
    if let Some(handle) = child.as_mut() {
        let _ = spawner.stop_target(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::os::unix::net::UnixListener;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use muxe_protocol::control::{CompatibilityRecord, HandoffId, LifecycleState};
    use muxe_protocol::{HostKind, LiveServerIdentity, ServerId};

    use super::super::activate::{ActivateError, LiveControl};

    fn test_record(version: &str) -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: version.to_owned(),
            target_triple: "test-triple".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        }
    }

    fn owner_temp() -> tempfile::TempDir {
        let temp = tempfile::tempdir().expect("coldstart dir");
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .expect("owner-only test dir");
        temp
    }

    struct OkReloader;

    impl HostReloader for OkReloader {
        fn reload_bridge(&self, _session: &str, _bridge_url: &str) -> Result<(), ActivateError> {
            Ok(())
        }
    }

    struct GateAdapter {
        readiness: Mutex<Option<muxe_adapter_api::ActivationReadiness>>,
        kind: muxe_adapter_api::HostKind,
        discovery: String,
        server: String,
        shutdown: AtomicBool,
        wake: tokio::sync::Notify,
    }

    impl GateAdapter {
        fn new(kind: muxe_adapter_api::HostKind, discovery: &str, server: &str) -> Self {
            Self {
                readiness: Mutex::new(None),
                kind,
                discovery: discovery.to_owned(),
                server: server.to_owned(),
                shutdown: AtomicBool::new(false),
                wake: tokio::sync::Notify::new(),
            }
        }
    }

    impl muxe_core::ActionValidator for GateAdapter {
        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _action_span: &muxe_core::SourceSpan,
        ) -> Result<muxe_core::ActionValidation, muxe_core::ConfigDiagnostic> {
            Ok(muxe_core::ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }
        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<muxe_core::ActionValidation>, Vec<muxe_core::ConfigDiagnostic>> {
            Ok(vec![
                muxe_core::ActionValidation {
                    execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
                };
                candidates.len()
            ])
        }
    }

    #[async_trait::async_trait]
    impl muxe_adapter_api::HostAdapter for GateAdapter {
        async fn identity(
            &self,
        ) -> Result<muxe_adapter_api::HostIdentity, muxe_adapter_api::AdapterError> {
            Ok(muxe_adapter_api::HostIdentity {
                kind: self.kind,
                discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(self.discovery.clone())
                    .expect("valid test discovery key"),
                live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
                    self.server.clone(),
                )
                .expect("valid test incarnation"),
            })
        }
        async fn capabilities(
            &self,
        ) -> Result<muxe_adapter_api::AdapterCapabilities, muxe_adapter_api::AdapterError> {
            Ok(muxe_adapter_api::AdapterCapabilities {
                keyboard: muxe_adapter_api::KeyboardCapabilities {
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
        async fn modal_scope(
            &self,
            _ui_pane: &muxe_core::PaneId,
        ) -> Result<muxe_adapter_api::ModalScopeId, muxe_adapter_api::AdapterError> {
            Ok(muxe_adapter_api::ModalScopeId::new("gate-scope"))
        }
        async fn begin_capture(
            &self,
            _request: muxe_adapter_api::CaptureRequest,
        ) -> Result<muxe_adapter_api::CaptureLease, muxe_adapter_api::AdapterError> {
            Err(muxe_adapter_api::AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "gate fake declares no capture support",
            ))
        }
        async fn end_capture(
            &self,
            _lease: muxe_adapter_api::CaptureLease,
            _reason: muxe_adapter_api::CaptureReleaseReason,
        ) -> Result<(), muxe_adapter_api::AdapterError> {
            Ok(())
        }
        async fn register_pending_pane(
            &self,
            registration: muxe_adapter_api::PendingPaneRegistration,
        ) -> Result<muxe_adapter_api::PendingPaneLease, muxe_adapter_api::AdapterError> {
            Ok(muxe_adapter_api::PendingPaneLease {
                id: muxe_adapter_api::PendingPaneLeaseId::new(format!(
                    "gate:{}",
                    registration.ui_session
                )),
                ui_session: registration.ui_session,
            })
        }
        async fn close_pending_pane(
            &self,
            _registration: muxe_adapter_api::PendingPaneRegistration,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), muxe_adapter_api::AdapterError> {
            Ok(())
        }
        fn release_pending_pane(&self, _lease: muxe_adapter_api::PendingPaneLease) {}
        async fn capture_origin(
            &self,
            _request: muxe_adapter_api::OriginCaptureRequest,
        ) -> Result<muxe_core::OriginContext, muxe_adapter_api::AdapterError> {
            Ok(muxe_core::OriginContext {
                host_kind: match self.kind {
                    muxe_adapter_api::HostKind::Herdr => muxe_core::OriginHostKind::Herdr,
                    muxe_adapter_api::HostKind::Zellij => muxe_core::OriginHostKind::Zellij,
                },
                server_id: muxe_core::ServerId::new(&self.server),
                client_id: None,
                session_id: None,
                workspace_id: None,
                tab_id: None,
                tab_index: None,
                pane_id: Some(muxe_core::PaneId::new("owned-pane")),
                pane_type: None,
                pane_cwd: None,
                selection_text: None,
                invocation_source: muxe_core::OriginInvocationSource::RootBinding,
                worktree_id: None,
                worktree_path: None,
                agent_id: None,
                link_url: None,
                link_handler_id: None,
            })
        }
        async fn dispatch_portable(
            &self,
            _request: muxe_adapter_api::PortableDispatchRequest,
        ) -> Result<muxe_adapter_api::DispatchAccepted, muxe_adapter_api::AdapterError> {
            Err(muxe_adapter_api::AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "gate fake never dispatches",
            ))
        }
        async fn dispatch_native(
            &self,
            _request: muxe_adapter_api::NativeDispatchRequest,
        ) -> Result<muxe_adapter_api::DispatchAccepted, muxe_adapter_api::AdapterError> {
            Err(muxe_adapter_api::AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "gate fake never dispatches",
            ))
        }
        async fn cancel(
            &self,
            _execution: muxe_core::ExecutionId,
        ) -> Result<(), muxe_adapter_api::AdapterError> {
            Ok(())
        }
        async fn next_health_event(
            &self,
        ) -> Result<muxe_adapter_api::AdapterHealthEvent, muxe_adapter_api::AdapterError> {
            if !self.shutdown.load(Ordering::SeqCst) {
                self.wake.notified().await;
            }
            Err(muxe_adapter_api::AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Shutdown,
                "owned test adapter stopped",
            ))
        }
        async fn shutdown(&self) -> Result<(), muxe_adapter_api::AdapterError> {
            self.shutdown.store(true, Ordering::SeqCst);
            self.wake.notify_waiters();
            Ok(())
        }
        async fn activation_readiness(
            &self,
        ) -> Result<Option<muxe_adapter_api::ActivationReadiness>, muxe_adapter_api::AdapterError>
        {
            Ok(self.readiness.lock().clone())
        }
    }

    #[derive(Default)]
    struct RejectSpawn(AtomicUsize);

    impl BrokerSpawner for RejectSpawn {
        fn spawn_target(&self, _request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ActivateError::Spawn(
                "verified live endpoint must never spawn".to_owned(),
            ))
        }

        fn stop_target(&self, _handle: &mut TargetHandle) -> Result<(), ActivateError> {
            panic!("no child was admitted by this spawner")
        }
    }

    struct CommitAuthority(HandoffId);

    impl muxe_broker::RecoveryJournal for CommitAuthority {
        fn recovery_decision<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _status: &'a ActivationStatus,
        ) -> std::pin::Pin<Box<dyn Future<Output = muxe_broker::RecoveryDecision> + Send + 'a>>
        {
            Box::pin(async {
                muxe_broker::RecoveryDecision::Preserve {
                    reason: "owned test coordinator controls target commit".to_owned(),
                }
            })
        }

        fn authorize_target_commit<'a>(
            &'a self,
            handoff: &'a HandoffId,
            status: &'a ActivationStatus,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                if *handoff == self.0
                    && status.phase == ActivationPhase::TargetGated
                    && status.handoff_id == Some(self.0)
                {
                    Ok(())
                } else {
                    Err("target commit lacks exact gated handoff".to_owned())
                }
            })
        }

        fn authorize_old_commit<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _status: &'a ActivationStatus,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Err("test target never owns old Commit".to_owned()) })
        }

        fn publish_old_retirement<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _status: &'a ActivationStatus,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Err("test target never retires old broker".to_owned()) })
        }
    }

    struct OwnedBrokerFixture {
        root: tempfile::TempDir,
        config: PathBuf,
        endpoint: RuntimeEndpoint,
        registry: Registry,
        registration: crate::lifecycle::registry::Registration,
        bridge: Option<BridgeIdentity>,
        kind: HostKind,
        discovery: String,
        reloader: OkReloader,
        shutdown: tokio::sync::watch::Sender<bool>,
        task: Option<tokio::task::JoinHandle<Result<(), muxe_broker::ServerError>>>,
    }

    impl OwnedBrokerFixture {
        async fn start(kind: HostKind, target: bool) -> Self {
            let root = owner_temp();
            let config = root.path().join("config.yml");
            let yaml = "version: 1\nsettings:\n  reload:\n    watch: false\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n";
            std::fs::write(&config, yaml).unwrap();
            let discovery = match kind {
                HostKind::Herdr => "herdr-owned",
                HostKind::Zellij => "session-test",
            };
            let adapter = Arc::new(GateAdapter::new(
                match kind {
                    HostKind::Herdr => muxe_adapter_api::HostKind::Herdr,
                    HostKind::Zellij => muxe_adapter_api::HostKind::Zellij,
                },
                discovery,
                "server-test",
            ));
            let compiled = muxe_core::compile_yaml(
                muxe_core::CompiledGeneration(1),
                muxe_core::SourceId::new("<owned coldstart broker>"),
                yaml,
                muxe_core::KeyCapabilities::default(),
                Some(adapter.as_ref()),
            )
            .unwrap();
            let broker = muxe_broker::Broker::from_compiled(adapter.clone(), &config, compiled);
            let live = broker.live_identity().await.unwrap();
            let bridge = (kind == HostKind::Zellij)
                .then(|| integration::bridge_identity(root.path()).unwrap());
            let endpoint = RuntimeEndpoint::in_runtime_dir(root.path(), kind, discovery).unwrap();
            let handoff = HandoffId([7; 16]);
            let bootstrap = if target {
                muxe_broker::ActivationBootstrap::Target {
                    current: test_record("9.9.9"),
                    handoff,
                    live_server: live,
                    bridge_unit: bridge.as_ref().map(BridgeIdentity::unit),
                }
            } else {
                muxe_broker::ActivationBootstrap::Running {
                    current: test_record("9.9.9"),
                    bridge_unit: bridge.as_ref().map(BridgeIdentity::unit),
                }
            };
            let recovery = target.then(|| {
                Arc::new(CommitAuthority(handoff)) as Arc<dyn muxe_broker::RecoveryJournal>
            });
            let server = muxe_broker::BrokerServer::start_activation(
                broker,
                endpoint.clone(),
                bootstrap,
                recovery,
            )
            .await
            .unwrap();
            let registry = Registry::open(root.path()).unwrap();
            let registration = Self::register_endpoint(
                &registry,
                root.path(),
                &endpoint,
                bridge.as_ref(),
                kind,
                discovery,
                target.then_some(handoff),
            );
            server
                .attest_registration(
                    muxe_protocol::control::BrokerRegistrationProof::new(
                        registration.entry().registration_id.unwrap(),
                        registration.entry().started_at,
                    )
                    .unwrap(),
                )
                .unwrap();
            let (shutdown, receiver) = tokio::sync::watch::channel(false);
            let task = tokio::spawn(server.run(receiver));
            Self {
                root,
                config,
                endpoint,
                registry,
                registration,
                bridge,
                kind,
                discovery: discovery.to_owned(),
                reloader: OkReloader,
                shutdown,
                task: Some(task),
            }
        }

        fn register_endpoint(
            registry: &Registry,
            cache: &Path,
            endpoint: &RuntimeEndpoint,
            bridge: Option<&BridgeIdentity>,
            kind: HostKind,
            discovery: &str,
            handoff: Option<HandoffId>,
        ) -> crate::lifecycle::registry::Registration {
            let mut entry = BrokerEntry::now(
                if kind == HostKind::Herdr {
                    "herdr"
                } else {
                    "zellij"
                },
                discovery,
                endpoint.socket().to_path_buf(),
                std::process::id(),
            );
            entry.live_server = Some("server-test".to_owned());
            entry.registration_id =
                Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
            entry.bridge_identity = bridge.cloned();
            entry.bridge_member =
                bridge.map(|_| BridgeMemberId::new(discovery.to_owned()).unwrap());
            entry.handoff_id = bridge.and(handoff);
            let Some(identity) = bridge else {
                return registry.register_herdr(entry).unwrap();
            };
            let guard = BridgeUnitGuard::acquire(cache, identity.clone()).unwrap();
            let Some(handoff) = handoff else {
                return registry.register_zellij(&guard, entry).unwrap();
            };
            let mut old = entry.clone();
            old.handoff_id = None;
            old.registration_id =
                Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
            registry.register_zellij(&guard, old).unwrap();
            let capability = super::super::registry::TargetRegistrationCapability::new(
                identity.clone(),
                entry.bridge_member.clone().unwrap(),
                endpoint.socket().to_path_buf(),
                discovery.to_owned(),
                handoff,
            );
            registry.register_zellij_target(&capability, entry).unwrap()
        }

        fn inputs<'a>(
            &'a self,
            spawner: &'a RejectSpawn,
        ) -> ColdstartInputs<'a, RejectSpawn, LiveControl, OkReloader> {
            ColdstartInputs {
                cache_dir: self.root.path(),
                config_file: &self.config,
                executable: Path::new("/bin/false"),
                endpoint: self.endpoint.clone(),
                host: match self.kind {
                    HostKind::Herdr => ColdstartHost::Herdr {
                        discovery_key: self.discovery.clone(),
                        live_server_id: ServerId::new("server-test"),
                        herdr_binary: PathBuf::from("/bin/false"),
                        herdr_socket: self.root.path().join("herdr.sock"),
                    },
                    HostKind::Zellij => ColdstartHost::Zellij {
                        session: self.discovery.clone(),
                        zellij_exe: PathBuf::from("/bin/false"),
                    },
                },
                current_record: test_record("9.9.9"),
                spawner,
                control: &LiveControl,
                reloader: Some(&self.reloader),
                readiness_deadline: Duration::from_secs(2),
                poll_interval: Duration::from_millis(5),
            }
        }

        fn unregister_original(&self) -> bool {
            if let Some(identity) = self.bridge.as_ref() {
                let guard = BridgeUnitGuard::acquire(self.root.path(), identity.clone()).unwrap();
                self.registry
                    .unregister_zellij(&guard, &self.registration)
                    .unwrap()
            } else {
                self.registry.unregister_herdr(&self.registration).unwrap()
            }
        }

        async fn stop(mut self) {
            self.shutdown.send(true).unwrap();
            tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
                .await
                .expect("owned service exits")
                .expect("service task joins")
                .expect("service exits cleanly");
            assert!(!self.endpoint.socket().exists());
            let _ = self.unregister_original();
            assert!(self.registry.entries().unwrap().is_empty());
        }
    }

    impl Drop for OwnedBrokerFixture {
        fn drop(&mut self) {
            let _ = self.shutdown.send(true);
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }

    #[tokio::test]
    async fn live_both_hosts_adopt_or_relocate_without_regenerating_owner() {
        for kind in [HostKind::Herdr, HostKind::Zellij] {
            for scenario in ["registered", "missing", "relocated"] {
                let fixture = OwnedBrokerFixture::start(kind, false).await;
                let original = fixture.registration.entry().clone();
                if scenario != "registered" {
                    assert!(fixture.unregister_original());
                }
                if scenario == "relocated" {
                    let mut elsewhere = original.clone();
                    elsewhere.socket = fixture.root.path().join("old-registration.sock");
                    if let Some(identity) = fixture.bridge.as_ref() {
                        let guard = BridgeUnitGuard::acquire(fixture.root.path(), identity.clone())
                            .unwrap();
                        fixture.registry.register_zellij(&guard, elsewhere).unwrap();
                    } else {
                        fixture.registry.register_herdr(elsewhere).unwrap();
                    }
                }
                let spawner = RejectSpawn::default();
                let outcome = ensure_broker(&fixture.inputs(&spawner))
                    .await
                    .expect("authenticated endpoint is reused");
                let ColdstartOutcome::Ready(live) = outcome else {
                    panic!("same compiled broker must be attachable: {outcome:?}");
                };
                assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
                let mut expected = original.clone();
                expected.socket = fixture.endpoint.socket().to_path_buf();
                assert_eq!(live.entry, expected, "token and timestamp remain original");
                assert_eq!(live.status.phase, ActivationPhase::Ordinary);
                assert_eq!(
                    fixture.registry.entries().unwrap(),
                    vec![expected.clone()],
                    "reconciliation is one exact registry row"
                );
                assert!(
                    fixture.unregister_original(),
                    "broker token owns reconciled row"
                );
                assert!(fixture.registry.entries().unwrap().is_empty());

                if scenario == "relocated" {
                    let mut replacement = expected;
                    replacement.registration_id =
                        Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
                    let token = if let Some(identity) = fixture.bridge.as_ref() {
                        let guard = BridgeUnitGuard::acquire(fixture.root.path(), identity.clone())
                            .unwrap();
                        fixture
                            .registry
                            .register_zellij(&guard, replacement.clone())
                            .unwrap()
                    } else {
                        fixture
                            .registry
                            .register_herdr(replacement.clone())
                            .unwrap()
                    };
                    assert!(
                        !fixture.unregister_original(),
                        "old token must not erase replacement"
                    );
                    assert_eq!(fixture.registry.entries().unwrap(), vec![replacement]);
                    if let Some(identity) = fixture.bridge.as_ref() {
                        let guard = BridgeUnitGuard::acquire(fixture.root.path(), identity.clone())
                            .unwrap();
                        assert!(fixture.registry.unregister_zellij(&guard, &token).unwrap());
                    } else {
                        assert!(fixture.registry.unregister_herdr(&token).unwrap());
                    }
                }
                fixture.stop().await;
            }
        }
    }
    #[tokio::test]
    async fn canonical_alias_stale_record_and_wrong_host_never_spawn() {
        for kind in [HostKind::Herdr, HostKind::Zellij] {
            let fixture = OwnedBrokerFixture::start(kind, false).await;
            let original = fixture.registry.entries().unwrap();
            let spawner = RejectSpawn::default();
            let mut inputs = fixture.inputs(&spawner);
            inputs.current_record = test_record("older-compiled");
            assert!(matches!(
                ensure_broker(&inputs).await.unwrap(),
                ColdstartOutcome::StaleRecord(_)
            ));
            inputs.current_record = test_record("9.9.9");
            match kind {
                HostKind::Herdr => {
                    if let ColdstartHost::Herdr { live_server_id, .. } = &mut inputs.host {
                        *live_server_id = ServerId::new("foreign-incarnation");
                    }
                    assert!(matches!(
                        ensure_broker(&inputs).await,
                        Err(ColdstartError::IdentityMismatch { .. })
                    ));
                }
                HostKind::Zellij => {
                    let alias = fixture.root.path().join("alias");
                    std::os::unix::fs::symlink(fixture.root.path(), &alias).unwrap();
                    let aliased_config = alias.join("config.yml");
                    let canonical =
                        expected_bridge_identity(&inputs.host, &fixture.config).unwrap();
                    let through_alias =
                        expected_bridge_identity(&inputs.host, &aliased_config).unwrap();
                    assert_eq!(canonical, through_alias, "first-creation alias is one unit");
                    inputs.config_file = &aliased_config;
                    assert!(matches!(
                        ensure_broker(&inputs).await.unwrap(),
                        ColdstartOutcome::Ready(_)
                    ));
                    let foreign = fixture.root.path().join("foreign");
                    fsutil::ensure_owner_dir(&foreign).unwrap();
                    let foreign_config = foreign.join("config.yml");
                    inputs.config_file = &foreign_config;
                    assert!(matches!(
                        ensure_broker(&inputs).await,
                        Err(ColdstartError::IdentityMismatch { .. })
                    ));
                }
            }
            assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
            assert_eq!(fixture.registry.entries().unwrap(), original);
            fixture.stop().await;
        }
    }

    #[tokio::test]
    async fn legacy_ordinary_reuses_only_exact_row_and_handoff_is_ambiguous() {
        let fixture = OwnedBrokerFixture::start(HostKind::Zellij, false).await;
        let spawner = RejectSpawn::default();
        let inputs = fixture.inputs(&spawner);
        let mut verified = LiveControl
            .verified_status(fixture.endpoint.socket())
            .await
            .unwrap();
        verified.status.phase = ActivationPhase::Legacy;
        verified.status.registration = None;
        let before = fixture.registry.entries().unwrap();
        let guard =
            BridgeUnitGuard::acquire(fixture.root.path(), fixture.bridge.clone().unwrap()).unwrap();
        let reused = reconcile_live_endpoint(
            &fixture.registry,
            &before,
            &verified,
            &inputs,
            Some(&guard),
            fixture.bridge.as_ref(),
        )
        .expect("safe legacy Running without a handoff reuses its exact row");
        assert_eq!(reused, before[0]);
        verified.status.handoff_id = Some(HandoffId([9; 16]));
        assert!(matches!(
            validate_status_identity(&verified.status, &inputs, fixture.bridge.as_ref()),
            Err(ColdstartError::LegacyActivationAmbiguous)
        ));
        assert_eq!(fixture.registry.entries().unwrap(), before);
        assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
        drop(guard);
        fixture.stop().await;
    }

    #[tokio::test]
    async fn legacy_peer_without_registered_owner_cannot_be_adopted() {
        let fixture = OwnedBrokerFixture::start(HostKind::Herdr, false).await;
        let spawner = RejectSpawn::default();
        let mut verified = LiveControl
            .verified_status(fixture.endpoint.socket())
            .await
            .unwrap();
        verified.status.phase = ActivationPhase::Legacy;
        verified.status.registration = None;
        assert!(fixture.unregister_original());
        let inputs = fixture.inputs(&spawner);
        assert!(matches!(
            reconcile_live_endpoint(&fixture.registry, &[], &verified, &inputs, None, None,),
            Err(ColdstartError::EndpointConflict(_))
        ));
        assert!(fixture.registry.entries().unwrap().is_empty());
        assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
        fixture.stop().await;
    }

    struct StaleFixture {
        root: tempfile::TempDir,
        config: PathBuf,
        endpoint: RuntimeEndpoint,
        registry: Registry,
        recorded: BrokerEntry,
        bridge: Option<BridgeIdentity>,
        kind: HostKind,
        discovery: String,
        reloader: OkReloader,
    }

    impl StaleFixture {
        fn new(kind: HostKind) -> Self {
            let root = owner_temp();
            let config = root.path().join("config.yml");
            std::fs::write(&config, "version: 1\nmenus: {}\n").unwrap();
            let discovery = if kind == HostKind::Herdr {
                "herdr-stale"
            } else {
                "session-test"
            };
            let endpoint = RuntimeEndpoint::in_runtime_dir(root.path(), kind, discovery).unwrap();
            endpoint.ensure_owner_directory().unwrap();
            let bridge = (kind == HostKind::Zellij)
                .then(|| integration::bridge_identity(root.path()).unwrap());
            let mut child = std::process::Command::new("/usr/bin/true").spawn().unwrap();
            let dead_pid = child.id();
            assert!(
                child.wait().unwrap().success(),
                "owned PID is reaped before stale check"
            );
            let mut recorded = BrokerEntry::now(
                if kind == HostKind::Herdr {
                    "herdr"
                } else {
                    "zellij"
                },
                discovery,
                endpoint.socket().to_path_buf(),
                dead_pid,
            );
            recorded.live_server = Some("server-test".to_owned());
            recorded.registration_id =
                Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
            recorded.bridge_identity = bridge.clone();
            recorded.bridge_member = bridge
                .as_ref()
                .map(|_| BridgeMemberId::new(discovery.to_owned()).unwrap());
            let registry = Registry::open(root.path()).unwrap();
            if let Some(identity) = bridge.as_ref() {
                let guard = BridgeUnitGuard::acquire(root.path(), identity.clone()).unwrap();
                registry.register_zellij(&guard, recorded.clone()).unwrap();
            } else {
                registry.register_herdr(recorded.clone()).unwrap();
            }
            Self {
                root,
                config,
                endpoint,
                registry,
                recorded,
                bridge,
                kind,
                discovery: discovery.to_owned(),
                reloader: OkReloader,
            }
        }

        fn replace_record(&mut self, replacement: BrokerEntry) {
            if let Some(identity) = self.bridge.as_ref() {
                let guard = BridgeUnitGuard::acquire(self.root.path(), identity.clone()).unwrap();
                self.registry
                    .register_zellij(&guard, replacement.clone())
                    .unwrap();
            } else {
                self.registry.register_herdr(replacement.clone()).unwrap();
            }
            self.recorded = replacement;
        }

        fn inputs<'a>(
            &'a self,
            spawner: &'a RejectSpawn,
        ) -> ColdstartInputs<'a, RejectSpawn, LiveControl, OkReloader> {
            ColdstartInputs {
                cache_dir: self.root.path(),
                config_file: &self.config,
                executable: Path::new("/bin/false"),
                endpoint: self.endpoint.clone(),
                host: match self.kind {
                    HostKind::Herdr => ColdstartHost::Herdr {
                        discovery_key: self.discovery.clone(),
                        live_server_id: ServerId::new("server-test"),
                        herdr_binary: PathBuf::from("/bin/false"),
                        herdr_socket: self.root.path().join("herdr.sock"),
                    },
                    HostKind::Zellij => ColdstartHost::Zellij {
                        session: self.discovery.clone(),
                        zellij_exe: PathBuf::from("/bin/false"),
                    },
                },
                current_record: test_record("9.9.9"),
                spawner,
                control: &LiveControl,
                reloader: Some(&self.reloader),
                readiness_deadline: Duration::from_secs(1),
                poll_interval: Duration::from_millis(5),
            }
        }
    }

    #[tokio::test]
    async fn stale_exact_owner_can_spawn_once_but_foreign_or_live_owner_cannot() {
        for kind in [HostKind::Herdr, HostKind::Zellij] {
            for scenario in ["absent", "refused", "wrong-record", "live-pid", "symlink"] {
                let mut fixture = StaleFixture::new(kind);
                if scenario == "refused" {
                    let listener = UnixListener::bind(fixture.endpoint.socket()).unwrap();
                    std::fs::set_permissions(
                        fixture.endpoint.socket(),
                        std::fs::Permissions::from_mode(0o600),
                    )
                    .unwrap();
                    drop(listener);
                } else if scenario == "symlink" {
                    let target = fixture.root.path().join("foreign");
                    std::fs::write(&target, b"user-owned").unwrap();
                    std::os::unix::fs::symlink(&target, fixture.endpoint.socket()).unwrap();
                } else if scenario == "wrong-record" {
                    let mut wrong = fixture.recorded.clone();
                    wrong.registration_id = None;
                    fixture.replace_record(wrong);
                } else if scenario == "live-pid" {
                    let mut live = fixture.recorded.clone();
                    live.server_pid = std::process::id();
                    fixture.replace_record(live);
                }
                let before = fixture.registry.entries().unwrap();
                let spawner = RejectSpawn::default();
                let outcome = ensure_broker(&fixture.inputs(&spawner)).await;
                if matches!(scenario, "absent" | "refused") {
                    assert!(
                        matches!(outcome, Err(ColdstartError::Spawn(_))),
                        "{outcome:?}"
                    );
                    assert!(fixture.registry.entries().unwrap().is_empty());
                    assert_eq!(spawner.0.load(Ordering::SeqCst), 1);
                } else {
                    assert!(outcome.is_err(), "{scenario} cannot mutate or spawn");
                    assert_eq!(fixture.registry.entries().unwrap(), before);
                    assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
                }
            }
        }
    }

    #[test]
    fn injected_pid_symlink_and_rebind_races_preserve_exact_stale_row() {
        for race in ["pid-reuse", "symlink", "rebind"] {
            let fixture = StaleFixture::new(HostKind::Herdr);
            let socket = fixture.endpoint.socket();
            let listener = UnixListener::bind(socket).unwrap();
            std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600)).unwrap();
            drop(listener);
            let observed = observe_stale_endpoint(socket).unwrap();
            let entries = fixture.registry.entries().unwrap();
            let result =
                fixture
                    .registry
                    .remove_exact_stale(None, &entries, &fixture.recorded, || {
                        if race == "pid-reuse" {
                            assert!(
                                !recorded_process_is_dead(std::process::id()).unwrap(),
                                "a reused/live PID never grants removal"
                            );
                            return Err(RegistryError::Conflict("PID reused".to_owned()));
                        }
                        std::fs::remove_file(socket).unwrap();
                        if race == "symlink" {
                            let target = fixture.root.path().join("foreign");
                            std::fs::write(&target, b"unowned").unwrap();
                            std::os::unix::fs::symlink(target, socket).unwrap();
                        } else {
                            let rebound = UnixListener::bind(socket).unwrap();
                            std::fs::set_permissions(
                                socket,
                                std::fs::Permissions::from_mode(0o600),
                            )
                            .unwrap();
                            drop(rebound);
                        }
                        let current = observe_stale_endpoint(socket)
                            .map_err(|error| RegistryError::Conflict(error.to_string()))?;
                        if current != observed {
                            return Err(RegistryError::Conflict(
                                "socket identity changed".to_owned(),
                            ));
                        }
                        Ok(())
                    });
            assert!(result.is_err(), "{race} must fail inside the registry lock");
            assert_eq!(fixture.registry.entries().unwrap(), entries);
        }
    }

    #[tokio::test]
    async fn gated_and_committed_both_hosts_require_journal_clear_before_attach() {
        use muxe_protocol::{
            AttachUi, BrokerResponse, ClientRequest, HostPaneId, MenuId, PeerRole,
        };

        for kind in [HostKind::Herdr, HostKind::Zellij] {
            let fixture = OwnedBrokerFixture::start(kind, true).await;
            let spawner = RejectSpawn::default();
            let inputs = fixture.inputs(&spawner);
            let unit = activation_unit(&inputs.host, fixture.bridge.as_ref()).unwrap();
            let path = journal::activation_dir(fixture.root.path()).join(unit.journal_name());
            fsutil::write_atomic(&path, b"pending", "owned-journal").unwrap();
            assert!(matches!(
                ensure_broker(&inputs).await,
                Err(ColdstartError::Startup(message)) if message.contains("activation recovery")
            ));
            assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
            fsutil::remove_file_durable(&path, "clear owned journal").unwrap();
            assert!(matches!(
                ensure_broker(&inputs).await,
                Err(ColdstartError::EndpointConflict(_))
            ));
            assert_eq!(fixture.registry.entries().unwrap().len(), 1);
            let mut control =
                super::super::control::ControlClient::connect(fixture.endpoint.socket())
                    .await
                    .unwrap();
            let gated = control.status().await.unwrap();
            assert_eq!(gated.phase, ActivationPhase::TargetGated);
            let mut ui = muxe_broker::BrokerClient::connect(
                fixture.endpoint.socket(),
                PeerRole::Ui,
                "owned-gate-ui",
                gated.live_server.clone(),
            )
            .await
            .unwrap();
            let attach = ClientRequest::AttachUi(AttachUi {
                root: MenuId::named("main"),
                pane: HostPaneId::new("owned-pane"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            });
            assert!(matches!(
                ui.request(attach.clone()).await.unwrap(),
                BrokerResponse::Error(_)
            ));
            let committed = control.commit(HandoffId([7; 16])).await.unwrap();
            assert_eq!(committed.phase, ActivationPhase::TargetCommitted);
            let ColdstartOutcome::Ready(ready) = ensure_broker(&inputs).await.unwrap() else {
                panic!("committed target must be attachable after journal clear");
            };
            assert_eq!(ready.status.phase, ActivationPhase::TargetCommitted);
            assert_eq!(spawner.0.load(Ordering::SeqCst), 0);
            assert!(matches!(
                ui.request(attach).await.unwrap(),
                BrokerResponse::UiAttached { .. }
            ));
            drop(ui);
            drop(control);
            fixture.stop().await;
        }
    }

    /// Target-gate service behavior while server.run is live pre-swap: stale
    /// bridge phases keep gated control Status reachable with ready=None, UI
    /// attach refused, and no retirement; fresh exact coverage flips ready to
    /// the full set while the gate still holds UI until commit.
    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "service proof phases read linearly: stale waves, UI refusal, fresh coverage, post-coverage refusal, shutdown unlink; splitting would scatter the pre-swap ordering the test exists to pin"
    )]
    async fn target_gate_serves_none_under_stale_bridge_until_fresh_coverage() {
        use muxe_protocol::{BrokerResponse, ClientRequest, MenuId, PeerRole, WireMessage};

        let temp = owner_temp();
        let config_path = temp.path().join("config.yml");
        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
        )
        .expect("gate config");
        let adapter = Arc::new(GateAdapter::new(
            muxe_adapter_api::HostKind::Zellij,
            "session-test",
            "server-test",
        ));
        let source = std::fs::read_to_string(&config_path).expect("read gate config");
        let compiled = muxe_core::compile_yaml(
            muxe_core::CompiledGeneration(1),
            muxe_core::SourceId::new("<target-gate>"),
            source,
            muxe_core::KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("compile gate config");
        let broker = muxe_broker::Broker::from_compiled(
            adapter.clone() as Arc<dyn muxe_adapter_api::HostAdapter>,
            &config_path,
            compiled,
        );
        let endpoint_dir = tempfile::tempdir().expect("gate runtime dir");
        let endpoint =
            RuntimeEndpoint::in_runtime_dir(endpoint_dir.path(), HostKind::Zellij, "session-test")
                .expect("gate endpoint");
        let socket = endpoint.socket().to_path_buf();
        let live_server = LiveServerIdentity {
            host: HostKind::Zellij,
            discovery_key: "session-test".to_owned(),
            server_id: ServerId::new("server-test"),
        };
        let handoff = HandoffId([7; 16]);
        let server = muxe_broker::BrokerServer::start_activation(
            broker,
            endpoint,
            muxe_broker::ActivationBootstrap::Target {
                current: test_record("9.9.9"),
                handoff,
                live_server: live_server.clone(),
                bridge_unit: None,
            },
            None,
        )
        .await
        .expect("target binds before any round");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let service_task = tokio::spawn(async move { server.run(shutdown_rx).await });

        // Stale bridge waves: Status stays reachable, Running, ungated-None.
        for _ in 0..3 {
            let mut control = super::super::control::ControlClient::connect(&socket)
                .await
                .expect("gated control reachable pre-round");
            let status = control.status().await.expect("gated status reads");
            assert_eq!(status.lifecycle, LifecycleState::Running);
            assert_eq!(status.handoff_id, Some(handoff));
            assert!(status.ready.is_none(), "stale bridge never reads ready");
            assert_eq!(status.live_server.discovery_key, "session-test");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // UI admission refused while gated.
        let mut ui = muxe_broker::BrokerClient::connect(
            &socket,
            PeerRole::Ui,
            env!("CARGO_PKG_VERSION"),
            live_server.clone(),
        )
        .await
        .expect("ui handshake reaches the gated target");
        let frame = ui
            .request_frame(ClientRequest::AttachUi(muxe_protocol::AttachUi {
                root: MenuId::named("main"),
                pane: muxe_protocol::HostPaneId::new("pane-1"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }))
            .await
            .expect("gated attach answers");
        assert!(
            matches!(
                frame.deserialize().expect("typed refusal"),
                WireMessage::Response {
                    response: BrokerResponse::Error(_),
                    ..
                }
            ),
            "TargetGated refuses UI attach"
        );
        // Fresh exact coverage: the full compatible set reads ready, the UI
        // gate still holds until commit, and nothing retired mid-stream.
        *adapter.readiness.lock() = Some(muxe_adapter_api::ActivationReadiness {
            registered_clients: vec!["1".to_owned()],
            member_clients: vec!["1".to_owned()],
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let mut control = super::super::control::ControlClient::connect(&socket)
                .await
                .expect("control reachable during coverage");
            let status = control.status().await.expect("status during coverage");
            assert_eq!(status.lifecycle, LifecycleState::Running);
            if let Some(ready) = status.ready {
                assert_eq!(ready.member_ids, Some(vec!["1".to_owned()]));
                break;
            }
            assert!(std::time::Instant::now() < deadline, "coverage reads ready");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let frame = ui
            .request_frame(ClientRequest::AttachUi(muxe_protocol::AttachUi {
                root: MenuId::named("main"),
                pane: muxe_protocol::HostPaneId::new("pane-1"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }))
            .await
            .expect("post-coverage attach answers");
        assert!(
            matches!(
                frame.deserialize().expect("typed refusal"),
                WireMessage::Response {
                    response: BrokerResponse::Error(_),
                    ..
                }
            ),
            "coverage without commit still refuses UI attach"
        );
        shutdown_tx.send(true).expect("shutdown signals");
        service_task
            .await
            .expect("service task joins")
            .expect("clean shutdown unlinks");
        assert!(!socket.exists(), "shutdown unlinks the endpoint");
    }
}
