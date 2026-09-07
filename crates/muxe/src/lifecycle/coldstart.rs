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
use thiserror::Error;

use super::{
    activate::{
        BrokerSpawner, ControlPort, ControlSession, HostReloader, SpawnRequest, TargetHandle,
    },
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

/// Internal verification state for one candidate entry.
enum Verification {
    /// Control status verified: attach or activate (boxed: the outcome
    /// carries the full status while the other variants are tiny).
    Verified(Box<ColdstartOutcome>),
    /// Candidate not verifiable yet (no entry, or transport not up).
    NotReady,
    /// Candidate verified as the wrong broker: fail closed at once.
    Mismatched(ColdstartError),
}

/// Ensures one live broker for the expected host identity, cold-starting an
/// ordinary broker when none answers.
///
/// A pre-existing live broker is verified by control status before return: a
/// wrong identity fails closed (never a second broker), a stale compiled
/// record reports [`ColdstartOutcome::StaleRecord`] for activation. When no
/// broker is live, the endpoint startup lock serializes concurrent starters;
/// the winner rechecks liveness under the lock, spawns exactly one child,
/// drops the lock, reloads the stable Zellij bridge when it spawned one, and
/// awaits verified identity within `readiness_deadline`. A loser of the lock
/// race awaits the winner's broker on the same bound. Expiry after spawning
/// stops the spawned child before returning, so no orphaned host adapter
/// owner survives a failed coldstart.
///
/// # Errors
///
/// Returns [`ColdstartError`] when the registry, lock, spawn, reload,
/// identity, or the bounded wait fails.
pub async fn ensure_broker<S, C, R>(
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<ColdstartOutcome, ColdstartError>
where
    S: BrokerSpawner,
    C: ControlPort,
    R: HostReloader,
{
    if let Some(found) = live_entry_for(inputs.cache_dir, &inputs.endpoint)? {
        return verify_now(inputs.control, &found, inputs).await;
    }
    let lock = match inputs.endpoint.acquire_startup_lock() {
        Ok(lock) => Some(lock),
        Err(RuntimeError::StartupInProgress(_)) => None,
        Err(error) => return Err(ColdstartError::Startup(error.to_string())),
    };
    let mut spawned: Option<TargetHandle> = None;
    if lock.is_some() {
        if let Some(found) = live_entry_for(inputs.cache_dir, &inputs.endpoint)? {
            drop(lock);
            return verify_now(inputs.control, &found, inputs).await;
        }
        let request = coldstart_spawn_request(inputs).map_err(ColdstartError::Spawn)?;
        spawned = Some(
            inputs
                .spawner
                .spawn_target(&request)
                .map_err(|error| ColdstartError::Spawn(error.to_string()))?,
        );
        drop(lock);
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
    }
    let deadline = Instant::now() + inputs.readiness_deadline;
    loop {
        match poll_once(inputs).await? {
            Verification::Verified(outcome) => {
                // The spawned child (if any) is now the serving broker daemon;
                // dropping its handle leaves it running under supervision.
                drop(spawned.take());
                return Ok(*outcome);
            }
            Verification::Mismatched(error) => {
                stop_spawned(inputs.spawner, spawned.take());
                return Err(error);
            }
            Verification::NotReady => {
                if Instant::now() >= deadline {
                    stop_spawned(inputs.spawner, spawned.take());
                    return Err(ColdstartError::StartupTimeout);
                }
                tokio::time::sleep(inputs.poll_interval).await;
            }
        }
    }
}

/// One readiness poll: no registry entry or no control transport yet reads as
/// not ready; a wrong identity fails closed at once.
async fn poll_once<S, C, R>(
    inputs: &ColdstartInputs<'_, S, C, R>,
) -> Result<Verification, ColdstartError>
where
    C: ControlPort,
{
    let Some(found) = live_entry_for(inputs.cache_dir, &inputs.endpoint)? else {
        return Ok(Verification::NotReady);
    };
    let Ok(status) = read_status(inputs.control, &found.socket).await else {
        return Ok(Verification::NotReady);
    };
    match classify(&found, status, inputs) {
        Ok(outcome) => Ok(Verification::Verified(Box::new(outcome))),
        Err(error) => Ok(Verification::Mismatched(error)),
    }
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

/// Finds the registry entry for this endpoint socket, if any.
fn live_entry_for(
    cache_dir: &Path,
    endpoint: &RuntimeEndpoint,
) -> Result<Option<BrokerEntry>, ColdstartError> {
    let registry = Registry::open(cache_dir)?;
    let socket = endpoint.socket();
    for entry in registry.entries()? {
        if entry.socket == socket {
            return Ok(Some(entry));
        }
    }
    Ok(None)
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
        cache_dir: PathBuf,
        socket: PathBuf,
        session: String,
        control: Arc<FakeControl>,
        record: CompatibilityRecord,
    }

    impl BrokerSpawner for FakeSpawner {
        fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
            self.spawns.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.args.iter().any(|arg| arg == "serve-zellij"),
                "coldstart spawns the ordinary broker child"
            );
            assert!(
                !request.args.iter().any(|arg| arg == "--handoff"),
                "coldstart carries no activation handoff"
            );
            if self.auto_register {
                let mut entry =
                    BrokerEntry::now("zellij", &self.session, self.socket.clone(), 4242);
                entry.live_server = Some(self.session.clone());
                Registry::open(&self.cache_dir)
                    .expect("fake registry opens")
                    .register(entry)
                    .expect("fake child registers");
                self.control
                    .statuses
                    .lock()
                    .expect("fake control is writable")
                    .insert(
                        self.socket.clone(),
                        test_status(&self.session, self.record.clone()),
                    );
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

    /// A live broker for another identity never gains a sibling: fail closed
    /// with no spawn.
    #[tokio::test]
    async fn wrong_identity_never_spawns_a_second_broker() {
        let temp = owner_temp();
        let config = temp.path().join("config.yml");
        let endpoint = endpoint_in(temp.path());
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
