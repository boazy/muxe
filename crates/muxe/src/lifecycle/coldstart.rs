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
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use muxe_broker::{RuntimeEndpoint, RuntimeError, ServeHerdrSpawn, ServeZellijSpawn};
use muxe_protocol::control::{ActivationStatus, CompatibilityRecord};
use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
use thiserror::Error;

use super::{
    activate::{
        BrokerSpawner, ControlPort, ControlSession, HostReloader, SpawnRequest, TargetHandle,
    },
    journal::{self, UnitKind, UnitLockAttempt},
    registry::{BrokerEntry, Registry, RegistryError},
};
use crate::integration;

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
}

/// Registry liveness for the deterministic endpoint.
enum EndpointRegistration {
    Live(BrokerEntry),
    Stale(BrokerEntry),
    Absent,
}

/// Ensures one live broker for the expected host identity, cold-starting an
/// ordinary broker when none answers.
///
/// A pre-existing live broker is verified by control status before return: a
/// wrong identity fails closed (never a second broker), and a stale compiled
/// record reports [`ColdstartOutcome::StaleRecord`] for activation. A refused
/// or missing recorded endpoint is replaceable only while this caller owns
/// both its activation-unit and endpoint-startup locks and the recorded broker
/// PID is provably dead. Active activation or startup ownership enters the
/// bounded readiness wait instead of spawning a sibling adapter.
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
    let unit = activation_unit(&inputs.host, inputs.config_file)?;
    let mut unit_lock = None;
    let mut spawned: Option<TargetHandle> = None;

    loop {
        if unit_lock.is_none() {
            match journal::try_acquire_unit_lock(inputs.cache_dir, &unit)
                .map_err(|error| ColdstartError::Startup(error.to_string()))?
            {
                UnitLockAttempt::Acquired(lock) => unit_lock = Some(lock),
                UnitLockAttempt::Active => {
                    wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                        .await?;
                    continue;
                }
            }
        }

        let registration = match endpoint_registration(inputs.cache_dir, &inputs.endpoint) {
            Ok(registration) => registration,
            Err(error) => {
                stop_spawned(inputs.spawner, spawned.take());
                return Err(error);
            }
        };
        if let EndpointRegistration::Live(found) = registration {
            if spawned.is_none() {
                return verify_now(inputs.control, &found, inputs).await;
            }
            let Ok(status) = read_status(inputs.control, &found.socket).await else {
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            };
            match classify(&found, status, inputs) {
                Ok(outcome) => {
                    // The child is now the serving broker daemon; dropping
                    // its handle leaves it running under supervision.
                    drop(spawned.take());
                    drop(unit_lock.take());
                    return Ok(outcome);
                }
                Err(error) => {
                    stop_spawned(inputs.spawner, spawned.take());
                    return Err(error);
                }
            }
        }
        if spawned.is_some() {
            wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval).await?;
            continue;
        }

        let startup_lock = match inputs.endpoint.acquire_startup_lock() {
            Ok(lock) => lock,
            Err(RuntimeError::StartupInProgress(_)) => {
                drop(unit_lock.take());
                wait_before_retry(inputs.spawner, &mut spawned, deadline, inputs.poll_interval)
                    .await?;
                continue;
            }
            Err(error) => return Err(ColdstartError::Startup(error.to_string())),
        };
        let registration = endpoint_registration(inputs.cache_dir, &inputs.endpoint)?;
        match registration {
            EndpointRegistration::Live(_) => {
                drop(startup_lock);
                continue;
            }
            EndpointRegistration::Stale(found) => {
                if !recorded_process_is_dead(found.server_pid)? {
                    return Err(ColdstartError::Startup(format!(
                        "recorded broker PID {} is still alive while {} refuses connections",
                        found.server_pid,
                        found.socket.display()
                    )));
                }
            }
            EndpointRegistration::Absent => {}
        }

        let journal_path = journal::activation_dir(inputs.cache_dir).join(unit.journal_name());
        match std::fs::symlink_metadata(&journal_path) {
            Ok(_) => {
                return Err(ColdstartError::Startup(format!(
                    "activation recovery is pending at {}",
                    journal_path.display()
                )));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ColdstartError::Startup(format!(
                    "could not inspect activation recovery at {}: {error}",
                    journal_path.display()
                )));
            }
        }
        let request = coldstart_spawn_request(inputs).map_err(ColdstartError::Spawn)?;
        spawned = Some(
            inputs
                .spawner
                .spawn_target(&request)
                .map_err(|error| ColdstartError::Spawn(error.to_string()))?,
        );
        drop(startup_lock);
        if let ColdstartHost::Zellij { session, .. } = &inputs.host {
            let config_dir = parent_of(inputs.config_file)?;
            let bridge_url =
                integration::kdl::bridge_url(&integration::stable_bridge_path(config_dir));
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

/// Verifies a pre-existing entry without polling: a silent transport is a
/// caller-visible error because no child is starting to explain it.
async fn verify_now<S, C, R>(
    control: &C,
    entry: &BrokerEntry,
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<ColdstartOutcome, ColdstartError>
where
    C: ControlPort,
{
    let status = read_status(control, &entry.socket)
        .await
        .map_err(ColdstartError::Startup)?;
    classify(entry, status, inputs)
}

/// Reads one control status, reporting transport failure as text.
async fn read_status<C>(control: &C, socket: &Path) -> Result<ActivationStatus, String>
where
    C: ControlPort,
{
    let mut session = control
        .connect(socket)
        .await
        .map_err(|error| error.to_string())?;
    session.status().await.map_err(|error| error.to_string())
}

/// Classifies a verified status: wrong identity fails closed (never a second
/// broker), a stale compiled record reports for activation, otherwise ready.
fn classify<S, C, R>(
    entry: &BrokerEntry,
    status: ActivationStatus,
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<ColdstartOutcome, ColdstartError> {
    let expected = expected_identity(&inputs.host);
    if status.live_server.discovery_key != expected {
        return Err(ColdstartError::IdentityMismatch {
            expected,
            found: status.live_server.discovery_key,
        });
    }
    let live = LiveBroker {
        entry: entry.clone(),
        status,
    };
    if live.status.current == inputs.current_record {
        Ok(ColdstartOutcome::Ready(live))
    } else {
        Ok(ColdstartOutcome::StaleRecord(live))
    }
}

/// Renders the exact ordinary serve request for one cold-started broker: the
/// current executable plus the broker-authored serve arguments with no
/// handoff pair, derived from caller-owned paths only.
fn coldstart_spawn_request<S, C, R>(
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<SpawnRequest, String> {
    let socket = inputs.endpoint.socket().to_path_buf();
    let program = inputs.executable.to_path_buf();
    let (args, host_identity) = match &inputs.host {
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
            let args = spawn.argv().map_err(|error| error.to_string())?;
            (args, session.clone())
        }
        ColdstartHost::Herdr {
            discovery_key,
            herdr_binary,
            herdr_socket,
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
            let args = spawn.argv().map_err(|error| error.to_string())?;
            (args, discovery_key.clone())
        }
    };
    Ok(SpawnRequest {
        program,
        args,
        host_identity,
    })
}

/// Classifies the matching registry entry by owner-only socket liveness.
fn endpoint_registration(
    cache_dir: &Path,
    endpoint: &RuntimeEndpoint,
) -> Result<EndpointRegistration, ColdstartError> {
    let socket = endpoint.socket();
    let liveness = Registry::open(cache_dir)?.probe()?;
    if let Some(entry) = liveness
        .live
        .into_iter()
        .find(|entry| entry.socket == socket)
    {
        return Ok(EndpointRegistration::Live(entry));
    }
    if let Some(entry) = liveness
        .stale
        .into_iter()
        .find(|entry| entry.socket == socket)
    {
        return Ok(EndpointRegistration::Stale(entry));
    }
    Ok(EndpointRegistration::Absent)
}

/// Derives the activation-unit lock shared with replacement and recovery.
fn activation_unit(host: &ColdstartHost, config_file: &Path) -> Result<UnitKind, ColdstartError> {
    match host {
        ColdstartHost::Herdr { discovery_key, .. } => Ok(UnitKind::Herdr {
            host_hash: journal::unit_hash(discovery_key),
        }),
        ColdstartHost::Zellij { .. } => {
            let config_dir = parent_of(config_file)?;
            let bridge = integration::stable_bridge_path(config_dir);
            Ok(UnitKind::Zellij {
                bridge_path_hash: journal::unit_hash(&bridge.display().to_string()),
            })
        }
    }
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
fn expected_identity(host: &ColdstartHost) -> String {
    match host {
        ColdstartHost::Zellij { session, .. } => session.clone(),
        ColdstartHost::Herdr { discovery_key, .. } => discovery_key.clone(),
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
fn stop_spawned<S>(spawner: &S, child: Option<TargetHandle>)
where
    S: BrokerSpawner,
{
    if let Some(handle) = child {
        let _ = spawner.stop_target(handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use muxe_protocol::control::{CompatibilityRecord, HandoffId, LifecycleState};
    use muxe_protocol::{HostKind, LiveServerIdentity, ServerId};

    use super::super::activate::ActivateError;
    use crate::lifecycle::control::ControlError;

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

    fn test_status(discovery: &str, record: CompatibilityRecord) -> ActivationStatus {
        ActivationStatus {
            lifecycle: LifecycleState::Running,
            live_server: LiveServerIdentity {
                host: HostKind::Zellij,
                discovery_key: discovery.to_owned(),
                server_id: ServerId::new("server-test"),
            },
            current: record,
            target: None,
            handoff_id: Some(HandoffId([7; 16])),
            ready: None,
        }
    }

    struct FakeSession {
        status: Option<ActivationStatus>,
    }

    impl ControlSession for FakeSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            self.status.clone().ok_or(ControlError::Closed)
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn commit(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn abort(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
    }

    struct FakeControl {
        statuses: Mutex<HashMap<PathBuf, ActivationStatus>>,
    }

    impl ControlPort for FakeControl {
        type Session = FakeSession;
        async fn connect(&self, socket: &Path) -> Result<FakeSession, ControlError> {
            let status = self
                .statuses
                .lock()
                .expect("fake control is readable")
                .get(socket)
                .cloned();
            Ok(FakeSession { status })
        }
    }

    struct FakeSpawner {
        spawns: AtomicUsize,
        stops: AtomicUsize,
        auto_register: bool,
        host: HostKind,
        cache_dir: PathBuf,
        socket: PathBuf,
        session: String,
        control: Arc<FakeControl>,
        record: CompatibilityRecord,
        listener: Mutex<Option<UnixListener>>,
    }

    impl BrokerSpawner for FakeSpawner {
        fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
            self.spawns.fetch_add(1, Ordering::SeqCst);
            let expected_serve = match self.host {
                HostKind::Zellij => "serve-zellij",
                HostKind::Herdr => "serve-herdr",
            };
            assert!(
                request.args.iter().any(|arg| arg == expected_serve),
                "coldstart spawns the expected ordinary broker child"
            );
            assert!(
                !request.args.iter().any(|arg| arg == "--handoff"),
                "coldstart carries no activation handoff"
            );
            if self.auto_register {
                let _ = std::fs::remove_file(&self.socket);
                let listener = UnixListener::bind(&self.socket).expect("fake broker socket binds");
                *self.listener.lock().expect("fake listener is writable") = Some(listener);
                let host = match self.host {
                    HostKind::Zellij => "zellij",
                    HostKind::Herdr => "herdr",
                };
                let mut entry = BrokerEntry::now(host, &self.session, self.socket.clone(), 4242);
                entry.live_server = Some(self.session.clone());
                Registry::open(&self.cache_dir)
                    .expect("fake registry opens")
                    .register(entry)
                    .expect("fake child registers");
                let mut status = test_status(&self.session, self.record.clone());
                status.live_server.host = self.host;
                self.control
                    .statuses
                    .lock()
                    .expect("fake control is writable")
                    .insert(self.socket.clone(), status);
            }
            let child = std::process::Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .map_err(|error| ActivateError::Spawn(error.to_string()))?;
            Ok(TargetHandle { child })
        }
        fn stop_target(&self, mut handle: TargetHandle) -> Result<(), ActivateError> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            let _ = handle.child.kill();
            let _ = handle.child.wait();
            Ok(())
        }
    }

    struct OkReloader;

    impl HostReloader for OkReloader {
        fn reload_bridge(&self, _session: &str, _bridge_url: &str) -> Result<(), ActivateError> {
            Ok(())
        }
    }

    fn zellij_inputs<'a>(
        cache: &'a Path,
        config: &'a Path,
        endpoint: RuntimeEndpoint,
        spawner: &'a FakeSpawner,
        control: &'a FakeControl,
        reloader: Option<&'a OkReloader>,
        record: CompatibilityRecord,
    ) -> ColdstartInputs<'a, FakeSpawner, FakeControl, OkReloader> {
        ColdstartInputs {
            cache_dir: cache,
            config_file: config,
            executable: Path::new("/bin/false"),
            endpoint,
            host: ColdstartHost::Zellij {
                session: "session-test".to_owned(),
                zellij_exe: PathBuf::from("/bin/false"),
            },
            current_record: record,
            spawner,
            control,
            reloader,
            readiness_deadline: Duration::from_secs(10),
            poll_interval: Duration::from_millis(10),
        }
    }

    /// Concurrent starters serialize on the endpoint lock: exactly one child
    /// spawns and every caller verifies the same broker.
    #[tokio::test]
    async fn concurrent_starters_spawn_exactly_one_child() {
        let temp = owner_temp();
        let cache = temp.path().to_path_buf();
        let config = temp.path().join("config.yml");
        let control = Arc::new(FakeControl {
            statuses: Mutex::new(HashMap::new()),
        });
        let spawner = Arc::new(FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: true,
            host: HostKind::Zellij,
            listener: Mutex::new(None),
            cache_dir: cache.clone(),
            socket: endpoint_in(temp.path()).socket().to_path_buf(),
            session: "session-test".to_owned(),
            control: Arc::clone(&control),
            record: test_record("9.9.9"),
        });
        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let config = config.clone();
            let spawner = Arc::clone(&spawner);
            let control = Arc::clone(&control);
            handles.push(tokio::spawn(async move {
                let reloader = OkReloader;
                let endpoint = endpoint_in(&cache);
                let inputs = zellij_inputs(
                    &cache,
                    &config,
                    endpoint,
                    &spawner,
                    &control,
                    Some(&reloader),
                    test_record("9.9.9"),
                );
                ensure_broker(&inputs).await
            }));
        }
        let mut ready = 0;
        for handle in handles {
            match handle.await.expect("caller completes") {
                Ok(ColdstartOutcome::Ready(_)) => ready += 1,
                outcome => panic!("every caller verifies the same broker: {outcome:?}"),
            }
        }
        assert_eq!(ready, 8);
        assert_eq!(
            spawner.spawns.load(Ordering::SeqCst),
            1,
            "one endpoint startup attempt across concurrent starters"
        );
        assert_eq!(spawner.stops.load(Ordering::SeqCst), 0);
    }

    fn endpoint_in(cache: &Path) -> RuntimeEndpoint {
        RuntimeEndpoint::in_runtime_dir(cache, HostKind::Zellij, "session-test")
            .expect("test endpoint derives")
    }

    /// A dead Herdr broker registration whose socket refuses connections is
    /// replaced by one serialized `serve-herdr` coldstart.
    #[tokio::test]
    async fn refused_dead_herdr_socket_is_replaced_by_one_coldstart() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let discovery = "herdr-dead-stale";
        let endpoint = RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, discovery)
            .expect("Herdr endpoint derives");
        endpoint
            .ensure_owner_directory()
            .expect("runtime directory exists");
        let stale_listener = UnixListener::bind(endpoint.socket()).expect("stale socket binds");
        drop(stale_listener);
        let error = UnixStream::connect(endpoint.socket()).expect_err("stale socket refuses");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);
        let mut exited = std::process::Command::new("/usr/bin/true")
            .spawn()
            .expect("stale broker process starts");
        let stale_pid = exited.id();
        assert!(
            exited
                .wait()
                .expect("stale broker process is reaped")
                .success()
        );

        let mut stale = BrokerEntry::now(
            "herdr",
            discovery,
            endpoint.socket().to_path_buf(),
            stale_pid,
        );
        stale.live_server = Some("old-server".to_owned());
        Registry::open(temp.path())
            .expect("registry opens")
            .register(stale)
            .expect("stale broker registers");

        let control = Arc::new(FakeControl {
            statuses: Mutex::new(HashMap::new()),
        });
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: true,
            host: HostKind::Herdr,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: discovery.to_owned(),
            control: Arc::clone(&control),
            record: test_record("9.9.9"),
        };
        let inputs = ColdstartInputs {
            cache_dir: temp.path(),
            config_file: &config,
            executable: Path::new("/bin/false"),
            endpoint,
            host: ColdstartHost::Herdr {
                discovery_key: discovery.to_owned(),
                herdr_binary: PathBuf::from("/bin/false"),
                herdr_socket: temp.path().join("herdr.sock"),
            },
            current_record: test_record("9.9.9"),
            spawner: &spawner,
            control: control.as_ref(),
            reloader: None::<&OkReloader>,
            readiness_deadline: Duration::from_secs(10),
            poll_interval: Duration::from_millis(10),
        };

        let ColdstartOutcome::Ready(live) =
            ensure_broker(&inputs).await.expect("stale socket recovers")
        else {
            panic!("replacement broker uses the current record");
        };
        assert_eq!(live.entry.server_pid, 4242, "new registration wins");
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.stops.load(Ordering::SeqCst), 0);
    }

    /// A refused endpoint whose recorded broker PID is still live is
    /// ambiguous authority and must never be replaced.
    #[tokio::test]
    async fn refused_socket_with_live_recorded_pid_fails_closed() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let endpoint = endpoint_in(temp.path());
        endpoint
            .ensure_owner_directory()
            .expect("runtime directory exists");
        let stale_listener = UnixListener::bind(endpoint.socket()).expect("stale socket binds");
        drop(stale_listener);

        let mut entry = BrokerEntry::now(
            "zellij",
            "session-test",
            endpoint.socket().to_path_buf(),
            std::process::id(),
        );
        entry.live_server = Some("server-test".to_owned());
        Registry::open(temp.path())
            .expect("registry opens")
            .register(entry)
            .expect("live owner registers");
        let control = Arc::new(FakeControl {
            statuses: Mutex::new(HashMap::new()),
        });
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: false,
            host: HostKind::Zellij,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: "session-test".to_owned(),
            control: Arc::clone(&control),
            record: test_record("9.9.9"),
        };
        let reloader = OkReloader;
        let inputs = zellij_inputs(
            temp.path(),
            &config,
            endpoint,
            &spawner,
            &control,
            Some(&reloader),
            test_record("9.9.9"),
        );

        let error = ensure_broker(&inputs)
            .await
            .expect_err("live recorded process blocks replacement");
        let ColdstartError::Startup(message) = error else {
            panic!("live recorded process is a startup-serialization error: {error}");
        };
        assert!(message.contains("is still alive"), "{message}");
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 0);
    }

    /// Any directory entry at the activation-journal authority path blocks
    /// ordinary startup, including a dangling symlink.
    #[tokio::test]
    async fn dangling_activation_journal_fails_closed() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let endpoint = endpoint_in(temp.path());
        let unit = activation_unit(
            &ColdstartHost::Zellij {
                session: "session-test".to_owned(),
                zellij_exe: PathBuf::from("/bin/false"),
            },
            &config,
        )
        .expect("activation unit derives");
        let activation_dir = journal::activation_dir(temp.path());
        crate::fsutil::ensure_owner_dir(&activation_dir).expect("activation directory exists");
        let journal_path = activation_dir.join(unit.journal_name());
        std::os::unix::fs::symlink(temp.path().join("missing-journal"), &journal_path)
            .expect("dangling journal authority exists");

        let control = Arc::new(FakeControl {
            statuses: Mutex::new(HashMap::new()),
        });
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: false,
            host: HostKind::Zellij,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: "session-test".to_owned(),
            control: Arc::clone(&control),
            record: test_record("9.9.9"),
        };
        let reloader = OkReloader;
        let inputs = zellij_inputs(
            temp.path(),
            &config,
            endpoint,
            &spawner,
            &control,
            Some(&reloader),
            test_record("9.9.9"),
        );

        let error = ensure_broker(&inputs)
            .await
            .expect_err("journal authority blocks ordinary startup");
        let ColdstartError::Startup(message) = error else {
            panic!("journal authority is a startup-serialization error: {error}");
        };
        assert!(
            message.contains("activation recovery is pending"),
            "{message}"
        );
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 0);
    }

    /// A refused registration owned by the current startup-lock holder is not
    /// stale authority: another caller waits for that broker and never spawns.
    #[tokio::test]
    async fn refused_registered_herdr_startup_waits_for_lock_holder() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let discovery = "herdr-test";
        let endpoint = RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, discovery)
            .expect("Herdr endpoint derives");
        endpoint
            .ensure_owner_directory()
            .expect("runtime directory exists");
        let stale_listener = UnixListener::bind(endpoint.socket()).expect("stale socket binds");
        drop(stale_listener);
        let error = UnixStream::connect(endpoint.socket()).expect_err("stale socket refuses");
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionRefused);

        let startup_lock = endpoint
            .acquire_startup_lock()
            .expect("winner holds startup lock");
        let mut entry = BrokerEntry::now("herdr", discovery, endpoint.socket().to_path_buf(), 73);
        entry.live_server = Some("server-test".to_owned());
        Registry::open(temp.path())
            .expect("registry opens")
            .register(entry)
            .expect("starting broker registers");

        let record = test_record("9.9.9");
        let control = Arc::new(FakeControl {
            statuses: Mutex::new(HashMap::new()),
        });
        let winner_socket = endpoint.socket().to_path_buf();
        let winner_control = Arc::clone(&control);
        let winner_record = record.clone();
        let winner = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            std::fs::remove_file(&winner_socket).expect("winner removes stale socket");
            let listener = UnixListener::bind(&winner_socket).expect("winner binds endpoint");
            let mut status = test_status(discovery, winner_record);
            status.live_server.host = HostKind::Herdr;
            winner_control
                .statuses
                .lock()
                .expect("fake control is writable")
                .insert(winner_socket, status);
            drop(startup_lock);
            listener
        });
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: false,
            host: HostKind::Herdr,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: discovery.to_owned(),
            control: Arc::clone(&control),
            record: record.clone(),
        };
        let inputs = ColdstartInputs {
            cache_dir: temp.path(),
            config_file: &config,
            executable: Path::new("/bin/false"),
            endpoint,
            host: ColdstartHost::Herdr {
                discovery_key: discovery.to_owned(),
                herdr_binary: PathBuf::from("/bin/false"),
                herdr_socket: temp.path().join("herdr.sock"),
            },
            current_record: record,
            spawner: &spawner,
            control: control.as_ref(),
            reloader: None::<&OkReloader>,
            readiness_deadline: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
        };

        let ColdstartOutcome::Ready(live) =
            ensure_broker(&inputs).await.expect("loser awaits winner")
        else {
            panic!("winner uses the current record");
        };
        let _listener = winner.await.expect("winner completes");
        assert_eq!(live.entry.server_pid, 73, "winner registration remains");
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 0);
        assert_eq!(spawner.stops.load(Ordering::SeqCst), 0);
    }

    /// A bound activation target remains gated until the activation-unit
    /// owner releases its full transaction lock.
    #[tokio::test]
    async fn gated_herdr_target_waits_for_activation_unit_release() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let discovery = "herdr-gated";
        let endpoint = RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, discovery)
            .expect("Herdr endpoint derives");
        endpoint
            .ensure_owner_directory()
            .expect("runtime directory exists");
        let unit = UnitKind::Herdr {
            host_hash: journal::unit_hash(discovery),
        };
        let activation_lock =
            journal::acquire_unit_lock(temp.path(), &unit).expect("activation owns unit");
        let _listener = UnixListener::bind(endpoint.socket()).expect("target endpoint binds");

        let mut entry = BrokerEntry::now(
            "herdr",
            discovery,
            endpoint.socket().to_path_buf(),
            std::process::id(),
        );
        entry.live_server = Some("target-server".to_owned());
        Registry::open(temp.path())
            .expect("registry opens")
            .register(entry)
            .expect("target broker registers");
        let mut status = test_status(discovery, test_record("9.9.9"));
        status.live_server.host = HostKind::Herdr;
        let control = Arc::new(FakeControl {
            statuses: Mutex::new(HashMap::from([(endpoint.socket().to_path_buf(), status)])),
        });
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: false,
            host: HostKind::Herdr,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: discovery.to_owned(),
            control: Arc::clone(&control),
            record: test_record("9.9.9"),
        };
        let inputs = ColdstartInputs {
            cache_dir: temp.path(),
            config_file: &config,
            executable: Path::new("/bin/false"),
            endpoint,
            host: ColdstartHost::Herdr {
                discovery_key: discovery.to_owned(),
                herdr_binary: PathBuf::from("/bin/false"),
                herdr_socket: temp.path().join("herdr.sock"),
            },
            current_record: test_record("9.9.9"),
            spawner: &spawner,
            control: control.as_ref(),
            reloader: None::<&OkReloader>,
            readiness_deadline: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
        };

        let mut pending = Box::pin(ensure_broker(&inputs));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut pending)
                .await
                .is_err(),
            "gated target cannot become attachable while activation owns the unit"
        );
        assert_eq!(
            spawner.spawns.load(Ordering::SeqCst),
            0,
            "coldstart never creates an ordinary sibling"
        );
        drop(activation_lock);
        let ColdstartOutcome::Ready(live) =
            pending.await.expect("target verifies after activation")
        else {
            panic!("target carries the current record");
        };
        assert_eq!(live.entry.discovery_key, discovery);
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 0);
    }

    /// A live broker for another identity never gains a sibling: fail closed
    /// with no spawn.
    #[tokio::test]
    async fn wrong_identity_never_spawns_a_second_broker() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let endpoint = endpoint_in(temp.path());
        endpoint
            .ensure_owner_directory()
            .expect("runtime directory exists");
        let _listener = UnixListener::bind(endpoint.socket()).expect("live broker socket binds");
        let mut entry = BrokerEntry::now(
            "zellij",
            "other-session",
            endpoint.socket().to_path_buf(),
            4242,
        );
        entry.live_server = Some("other-session".to_owned());
        Registry::open(temp.path())
            .expect("registry opens")
            .register(entry)
            .expect("pre-existing registration");
        let control = FakeControl {
            statuses: Mutex::new(HashMap::from([(
                endpoint.socket().to_path_buf(),
                test_status("other-session", test_record("9.9.9")),
            )])),
        };
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: true,
            host: HostKind::Zellij,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: "session-test".to_owned(),
            control: Arc::new(FakeControl {
                statuses: Mutex::new(HashMap::new()),
            }),
            record: test_record("9.9.9"),
        };
        let reloader = OkReloader;
        let inputs = zellij_inputs(
            temp.path(),
            &config,
            endpoint,
            &spawner,
            &control,
            Some(&reloader),
            test_record("9.9.9"),
        );
        let error = ensure_broker(&inputs)
            .await
            .expect_err("wrong identity fails closed");
        assert!(
            matches!(error, ColdstartError::IdentityMismatch { .. }),
            "identity mismatch, not a transport error: {error}"
        );
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 0);
    }

    /// A live broker on a stale compiled record reports for activation,
    /// never a silent attach.
    #[tokio::test]
    async fn stale_record_reports_for_activation() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let endpoint = endpoint_in(temp.path());
        endpoint
            .ensure_owner_directory()
            .expect("runtime directory exists");
        let _listener = UnixListener::bind(endpoint.socket()).expect("live broker socket binds");
        let mut entry = BrokerEntry::now(
            "zellij",
            "session-test",
            endpoint.socket().to_path_buf(),
            4242,
        );
        entry.live_server = Some("session-test".to_owned());
        Registry::open(temp.path())
            .expect("registry opens")
            .register(entry)
            .expect("pre-existing registration");
        let control = FakeControl {
            statuses: Mutex::new(HashMap::from([(
                endpoint.socket().to_path_buf(),
                test_status("session-test", test_record("0.0.1")),
            )])),
        };
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: false,
            host: HostKind::Zellij,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: "session-test".to_owned(),
            control: Arc::new(FakeControl {
                statuses: Mutex::new(HashMap::new()),
            }),
            record: test_record("9.9.9"),
        };
        let reloader = OkReloader;
        let inputs = zellij_inputs(
            temp.path(),
            &config,
            endpoint,
            &spawner,
            &control,
            Some(&reloader),
            test_record("9.9.9"),
        );
        match ensure_broker(&inputs).await.expect("stale reports") {
            ColdstartOutcome::StaleRecord(live) => {
                assert_eq!(live.entry.discovery_key, "session-test");
            }
            outcome @ ColdstartOutcome::Ready(_) => {
                panic!("stale record must report for activation: {outcome:?}")
            }
        }
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 0);
    }

    /// A child that never answers is reaped on expiry: timeout, stopped.
    #[tokio::test]
    async fn silent_child_is_reaped_on_timeout() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let endpoint = endpoint_in(temp.path());
        let control = FakeControl {
            statuses: Mutex::new(HashMap::new()),
        };
        let spawner = FakeSpawner {
            spawns: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            auto_register: false,
            host: HostKind::Zellij,
            listener: Mutex::new(None),
            cache_dir: temp.path().to_path_buf(),
            socket: endpoint.socket().to_path_buf(),
            session: "session-test".to_owned(),
            control: Arc::new(FakeControl {
                statuses: Mutex::new(HashMap::new()),
            }),
            record: test_record("9.9.9"),
        };
        let reloader = OkReloader;
        let mut inputs = zellij_inputs(
            temp.path(),
            &config,
            endpoint,
            &spawner,
            &control,
            Some(&reloader),
            test_record("9.9.9"),
        );
        inputs.readiness_deadline = Duration::from_millis(150);
        let error = ensure_broker(&inputs).await.expect_err("silence times out");
        assert!(
            matches!(error, ColdstartError::StartupTimeout),
            "bounded wait, not a hang: {error}"
        );
        assert_eq!(spawner.spawns.load(Ordering::SeqCst), 1);
        assert_eq!(
            spawner.stops.load(Ordering::SeqCst),
            1,
            "the orphaned child is stopped"
        );
    }

    struct GateAdapter {
        readiness: Mutex<Option<muxe_adapter_api::ActivationReadiness>>,
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
                kind: muxe_adapter_api::HostKind::Zellij,
                discovery_key: "session-test".to_owned(),
                live_server_id: "server-test".to_owned(),
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
        async fn release_pending_pane(
            &self,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), muxe_adapter_api::AdapterError> {
            Ok(())
        }
        async fn capture_origin(
            &self,
            _request: muxe_adapter_api::OriginCaptureRequest,
        ) -> Result<muxe_core::OriginContext, muxe_adapter_api::AdapterError> {
            Err(muxe_adapter_api::AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "gate fake never attaches UI",
            ))
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
            std::future::pending().await
        }
        async fn shutdown(&self) -> Result<(), muxe_adapter_api::AdapterError> {
            Ok(())
        }
        async fn activation_readiness(
            &self,
        ) -> Result<Option<muxe_adapter_api::ActivationReadiness>, muxe_adapter_api::AdapterError>
        {
            Ok(self.readiness.lock().expect("readiness script").clone())
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
        let adapter = Arc::new(GateAdapter {
            readiness: Mutex::new(None),
        });
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
                root: MenuId::new("main"),
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
        *adapter.readiness.lock().expect("script coverage") =
            Some(muxe_adapter_api::ActivationReadiness {
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
                root: MenuId::new("main"),
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
