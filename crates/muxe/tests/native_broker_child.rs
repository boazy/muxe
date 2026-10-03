#[path = "support/scoped_env.rs"]
mod scoped_env;
use scoped_env::{apply_scoped_env, ensure_scoped_dirs, scoped_env_vec};

use std::{
    fmt::Write,
    io::Read,
    num::NonZeroI32,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use muxe::lifecycle::{
    BrokerSpawner, ColdstartHost, ColdstartInputs, ColdstartOutcome, LiveControl, ProcessSpawner,
    SpawnRequest, TargetHandle, ZellijCliReloader,
    control::ControlClient,
    ensure_broker,
    journal::{
        ActivationId, ActivationJournal, OldMemberProgress, ReadyMemberProof, ReadyProof,
        TargetMemberProgress, TransactionMember, UnitKind,
    },
};
use muxe_broker::RuntimeEndpoint;
use muxe_protocol::control::{HandoffId, LifecycleState};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    process::{Child, Command},
    task::JoinHandle,
    time::timeout,
};

struct FakeHerdr {
    _root: TempDir,
    socket: PathBuf,
    task: JoinHandle<()>,
}

impl FakeHerdr {
    fn start() -> Self {
        let root = tempfile::tempdir_in("/tmp").expect("fake Herdr root");
        let socket = root.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket).expect("fake Herdr socket");
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(handle_herdr_connection(stream));
            }
        });
        Self {
            _root: root,
            socket,
            task,
        }
    }

    fn socket(&self) -> &Path {
        &self.socket
    }
}

impl Drop for FakeHerdr {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_herdr_connection(stream: UnixStream) {
    let mut reader = BufReader::new(stream);
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line).await.unwrap_or(0) == 0 {
        return;
    }
    let request: Value = match serde_json::from_slice(&line) {
        Ok(request) => request,
        Err(_) => return,
    };
    let Some(id) = request.get("id").and_then(Value::as_str) else {
        return;
    };
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let result = match method {
        "ping" => json!({
            "type": "pong",
            "protocol": 20,
            "version": "0.8.2",
        }),
        "events.subscribe" => json!({"subscribed": true}),
        _ => json!({}),
    };
    let response = json!({"id": id, "result": result});
    let mut stream = reader.into_inner();
    if stream
        .write_all(format!("{response}\n").as_bytes())
        .await
        .is_err()
    {
        return;
    }
    if method == "events.subscribe" {
        // Real Herdr keeps a filtered lifecycle stream quiet until a matching
        // event; the client must not require fabricated heartbeat traffic.
        let mut discarded = Vec::new();
        let _ = stream.read_to_end(&mut discarded).await;
    }
}
fn write_schema_binary(root: &Path) -> PathBuf {
    let schema = root.join("schema.json");
    std::fs::write(
        &schema,
        include_str!("../../../fixtures/herdr/herdr-api.schema.json"),
    )
    .expect("write fake Herdr schema");
    let observed_env = root.join("child-env-proof");
    let binary = root.join("herdr");
    std::fs::write(
        &binary,
        format!(
            "#!/bin/sh\nprintf '%s|%s|%s|%s|%s\\n' \"$HOME\" \"$XDG_CONFIG_HOME\" \
\"$XDG_CACHE_HOME\" \"$XDG_RUNTIME_DIR\" \"$TMPDIR\" > '{}'\nexec cat \"$(dirname \"$0\")/schema.json\"\n",
            observed_env.display()
        ),
    )
    .expect("write fake Herdr executable");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("make fake Herdr executable");
    binary
}

async fn connect_when_ready(path: &Path, child: &mut Child) -> ControlClient {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(control) = ControlClient::connect(path).await {
                return control;
            }
            if let Ok(Some(status)) = child.try_wait() {
                let mut stderr = child.stderr.take().expect("child stderr pipe");
                let mut diagnostics = Vec::new();
                stderr
                    .read_to_end(&mut diagnostics)
                    .await
                    .expect("read child diagnostics");
                panic!(
                    "native broker child exited before control: {status}; stderr={}",
                    String::from_utf8_lossy(&diagnostics)
                );
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "native broker child control appears; child={:?}",
            child.id()
        )
    })
}

async fn wait_for_child(child: &mut Child) {
    let status = timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("native broker child exits after ACK")
        .expect("wait for native broker child");
    assert!(status.success(), "native broker child failed: {status}");
}

fn spawn_native_child(
    config: &Path,
    cache: &Path,
    herdr_binary: &Path,
    herdr_socket: &Path,
    endpoint: &Path,
    scoped_root: &Path,
    handoff: Option<(&str, &Path)>,
) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_muxe"));
    command
        .args(["broker", "serve-herdr"])
        .args(["--socket", endpoint.to_str().unwrap()])
        .args(["--herdr-binary", herdr_binary.to_str().unwrap()])
        .args(["--herdr-socket", herdr_socket.to_str().unwrap()])
        .args(["--config", config.to_str().unwrap()])
        .args(["--cache-dir", cache.to_str().unwrap()])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    apply_scoped_env(&mut command, scoped_root);
    command.current_dir(scoped_root);
    if let Some((handoff, journal)) = handoff {
        command
            .args(["--handoff", handoff])
            .args(["--activation-journal", journal.to_str().unwrap()]);
    }
    command.spawn().expect("spawn packaged native broker child")
}

fn handoff_hex(handoff: &HandoffId) -> String {
    let mut text = String::with_capacity(32);
    for byte in handoff.0 {
        write!(&mut text, "{byte:02x}").expect("write handoff hex");
    }
    text
}

fn target_journal(cache: &Path, endpoint: &Path, host: &str, handoff: HandoffId) -> PathBuf {
    let record = muxe::compatibility::embedded_record()
        .expect("native child embeds compatibility record")
        .handoff;
    let activation = ActivationId::generate().expect("generate target activation identity");
    let mut member = TransactionMember::new(
        activation,
        muxe::lifecycle::ActivationMemberId::new(host.to_owned()).unwrap(),
        muxe::lifecycle::MemberEndpoint::new(endpoint.to_path_buf()).unwrap(),
        handoff,
        record.clone(),
    )
    .expect("construct target journal member");
    member.old = OldMemberProgress::Drained;
    member.target = TargetMemberProgress::SpawnIntent;
    let mut journal = ActivationJournal::new(
        activation,
        UnitKind::Herdr {
            host_hash: muxe::lifecycle::HerdrUnitId::derive(host),
        },
        record,
        vec![member],
    )
    .expect("construct target authorization journal");
    journal.enter_activating();
    muxe::lifecycle::journal::write_journal(cache, &journal)
        .expect("write target authorization journal")
}

#[derive(Clone, Copy)]
struct OwnedChildPid(NonZeroI32);

impl OwnedChildPid {
    fn new(raw: u32) -> Self {
        Self(NonZeroI32::new(i32::try_from(raw).expect("owned child PID fits i32")).unwrap())
    }

    fn as_nix(self) -> nix::unistd::Pid {
        nix::unistd::Pid::from_raw(self.0.get())
    }
}

struct ScopedColdstartSpawner {
    root: PathBuf,
    spawns: AtomicUsize,
    owned: Mutex<Option<OwnedChildPid>>,
    diagnostics: Mutex<String>,
}

impl ScopedColdstartSpawner {
    fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            spawns: AtomicUsize::new(0),
            owned: Mutex::new(None),
            diagnostics: Mutex::new(String::new()),
        }
    }

    fn complete(&self, pid: OwnedChildPid) {
        let mut owned = self.owned.lock();
        assert_eq!(owned.map(|current| current.0), Some(pid.0));
        *owned = None;
    }
}

impl BrokerSpawner for ScopedColdstartSpawner {
    fn spawn_target(
        &self,
        request: &SpawnRequest,
    ) -> Result<TargetHandle, muxe::lifecycle::ActivateError> {
        self.spawns.fetch_add(1, Ordering::SeqCst);
        let mut command = std::process::Command::new(&request.program);
        command
            .args(&request.args)
            .env_clear()
            .envs(scoped_env_vec(&self.root))
            .current_dir(&self.root)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let child = command
            .spawn()
            .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
        *self.owned.lock() = Some(OwnedChildPid::new(child.id()));
        Ok(TargetHandle::new(child))
    }

    fn stop_target(&self, handle: &mut TargetHandle) -> Result<(), muxe::lifecycle::ActivateError> {
        let pid = OwnedChildPid::new(handle.child.id());
        let result = ProcessSpawner.stop_target(handle);
        if result.is_ok() {
            self.complete(pid);
        }
        if let Some(mut stderr) = handle.child.stderr.take() {
            let mut diagnostics = String::new();
            stderr.read_to_string(&mut diagnostics).unwrap();
            *self.diagnostics.lock() = diagnostics;
        }
        result
    }
}

impl Drop for ScopedColdstartSpawner {
    fn drop(&mut self) {
        if let Some(pid) = self.owned.lock().take() {
            let _ = nix::sys::signal::kill(pid.as_nix(), nix::sys::signal::Signal::SIGKILL);
            let _ = nix::sys::wait::waitpid(pid.as_nix(), None);
        }
    }
}
fn register_stale_owner(
    scenario: &str,
    endpoint: &RuntimeEndpoint,
    registry: &muxe::lifecycle::Registry,
    discovery: &str,
    current_server: &muxe_protocol::wire::ServerId,
) -> Option<muxe_protocol::control::BrokerRegistrationId> {
    if scenario == "concurrent" {
        return None;
    }
    endpoint.ensure_owner_directory().unwrap();
    if scenario == "refused" {
        let socket = std::os::unix::net::UnixListener::bind(endpoint.socket()).unwrap();
        std::fs::set_permissions(endpoint.socket(), std::fs::Permissions::from_mode(0o600))
            .unwrap();
        drop(socket);
    }
    let mut dead = std::process::Command::new("/usr/bin/true").spawn().unwrap();
    let dead_pid = dead.id();
    assert!(
        dead.wait().unwrap().success(),
        "owned prior child is reaped"
    );
    let mut entry = muxe::lifecycle::BrokerEntry::now(
        "herdr",
        discovery,
        endpoint.socket().to_path_buf(),
        dead_pid,
    );
    entry.live_server = Some(current_server.as_str().to_owned());
    entry.registration_id = Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
    let stale_id = entry.registration_id;
    registry.register_herdr(entry).unwrap();
    stale_id
}

async fn assert_ready_and_retire(
    first: ColdstartOutcome,
    second: ColdstartOutcome,
    stale_id: Option<muxe_protocol::control::BrokerRegistrationId>,
    endpoint: &RuntimeEndpoint,
    cache: &Path,
    spawner: &ScopedColdstartSpawner,
    registry: &muxe::lifecycle::Registry,
) {
    let ColdstartOutcome::Ready(first) = first else {
        panic!("new native broker serves current record");
    };
    let ColdstartOutcome::Ready(second) = second else {
        panic!("second native caller sees current record");
    };
    assert_eq!(spawner.spawns.load(Ordering::SeqCst), 1);
    assert_eq!(first.entry, second.entry);
    assert_ne!(first.entry.registration_id(), stale_id);
    assert_eq!(first.entry.socket(), endpoint.socket());
    let child_pid = spawner
        .owned
        .lock()
        .expect("spawn retained an owned child PID");
    assert_eq!(
        first.entry.server_pid().get(),
        u32::try_from(child_pid.0.get()).expect("owned child PID is positive")
    );
    assert!(
        !muxe::lifecycle::journal::activation_dir(cache)
            .read_dir()
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "pending"))
    );
    let mut control = ControlClient::connect(endpoint.socket()).await.unwrap();
    assert_eq!(
        control.status().await.unwrap().phase,
        muxe_protocol::control::ActivationPhase::Ordinary
    );
    control.retire().await.unwrap();
    let exit_code = timeout(Duration::from_secs(10), async {
        loop {
            match nix::sys::wait::waitpid(
                child_pid.as_nix(),
                Some(nix::sys::wait::WaitPidFlag::WNOHANG),
            ) {
                Ok(nix::sys::wait::WaitStatus::StillAlive) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(nix::sys::wait::WaitStatus::Exited(_, code)) => break code,
                result => panic!("owned broker exits and reaps: {result:?}"),
            }
        }
    })
    .await
    .expect("owned broker exits after Retire");
    assert_eq!(exit_code, 0);
    spawner.complete(child_pid);
    assert!(registry.entries().unwrap().is_empty());
}

async fn assert_coldstart_owned_child(scenario: &str) {
    let root = tempfile::Builder::new()
        .prefix("mx")
        .tempdir_in("/tmp")
        .expect("owned coldstart scope");
    ensure_scoped_dirs(root.path()).unwrap();
    let config = root.path().join("config.yml");
    std::fs::write(
        &config,
        "version: 1\nsettings:\n  reload:\n    watch: false\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
    )
    .unwrap();
    let herdr = FakeHerdr::start();
    let binary = write_schema_binary(root.path());
    let cache = root.path().join("cache");
    let runtime =
        muxe_adapter_herdr::HerdrRuntime::connect(muxe_adapter_herdr::HerdrAdapterConfig {
            socket_path: herdr.socket().to_path_buf(),
            herdr_binary: binary.clone(),
            cache_dir: cache.clone(),
        })
        .await
        .expect("retained Herdr runtime identifies current incarnation");
    let discovery = runtime.identity().discovery_key.clone();
    let current_server =
        muxe_protocol::wire::ServerId::new(runtime.identity().live_server_id.as_str());
    let endpoint = RuntimeEndpoint::in_runtime_dir(
        root.path().join("runtime"),
        muxe_protocol::wire::HostKind::Herdr,
        discovery.as_str(),
    )
    .unwrap();
    let registry = muxe::lifecycle::Registry::open(&cache).unwrap();
    let stale_id = register_stale_owner(
        scenario,
        &endpoint,
        &registry,
        discovery.as_str(),
        &current_server,
    );
    let spawner = ScopedColdstartSpawner::new(root.path());
    let executable = Path::new(env!("CARGO_BIN_EXE_muxe"));
    let inputs = ColdstartInputs {
        cache_dir: &cache,
        config_file: &config,
        executable,
        endpoint: endpoint.clone(),
        host: ColdstartHost::Herdr {
            discovery_key: discovery,
            live_server_id: current_server,
            herdr_binary: binary,
            herdr_socket: herdr.socket().to_path_buf(),
        },
        current_record: muxe::compatibility::embedded_record().unwrap().handoff,
        spawner: &spawner,
        control: &LiveControl,
        reloader: None::<&ZellijCliReloader>,
        readiness_deadline: Duration::from_secs(10),
        poll_interval: Duration::from_millis(10),
    };
    let (first, second) = if scenario == "concurrent" {
        let (first, second) = tokio::join!(ensure_broker(&inputs), ensure_broker(&inputs));
        (
            first.unwrap_or_else(|error| {
                panic!(
                    "first caller: {error}; stderr={}",
                    spawner.diagnostics.lock()
                )
            }),
            second.unwrap_or_else(|error| {
                panic!(
                    "second caller: {error}; stderr={}",
                    spawner.diagnostics.lock()
                )
            }),
        )
    } else {
        let first = ensure_broker(&inputs)
            .await
            .expect("stale row starts one broker");
        let second = ensure_broker(&inputs)
            .await
            .expect("later caller adopts broker");
        (first, second)
    };
    assert_ready_and_retire(
        first, second, stale_id, &endpoint, &cache, &spawner, &registry,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coldstart_replaces_exact_stale_and_concurrent_callers_share_one_child() {
    for scenario in ["absent", "refused", "concurrent"] {
        Box::pin(assert_coldstart_owned_child(scenario)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one subprocess lifecycle proof keeps refusal, rollback, target ACK, and exit ordering together"
)]
async fn native_child_rejects_uncertified_old_commit_and_handles_target_lifecycle() {
    let root = tempfile::Builder::new()
        .prefix("mx")
        .tempdir_in("/tmp")
        .expect("native smoke root");
    ensure_scoped_dirs(root.path()).expect("create native child scoped dirs");
    let config = root.path().join("config.yml");
    let cache = root.path().join("cache");
    std::fs::write(
        &config,
        "version: 1\nsettings:\n  reload:\n    watch: false\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
    )
    .expect("native smoke config");
    let herdr = FakeHerdr::start();
    let herdr_binary = write_schema_binary(root.path());
    let endpoint = RuntimeEndpoint::in_runtime_dir(
        root.path().join("runtime"),
        muxe_protocol::wire::HostKind::Herdr,
        &herdr.socket().to_string_lossy(),
    )
    .expect("native endpoint");

    let mut retire_child = spawn_native_child(
        &config,
        &cache,
        &herdr_binary,
        herdr.socket(),
        endpoint.socket(),
        root.path(),
        None,
    );
    let mut retire_control = connect_when_ready(endpoint.socket(), &mut retire_child).await;
    let observed = std::fs::read_to_string(root.path().join("child-env-proof"))
        .expect("fake Herdr observed native child environment");
    let expected = format!(
        "{}|{}|{}|{}|{}\n",
        root.path().join("home").display(),
        root.path().join("config").display(),
        root.path().join("cache").display(),
        root.path().join("runtime").display(),
        root.path().join("tmp").display(),
    );
    assert_eq!(
        observed, expected,
        "native child uses only the owned scoped HOME/XDG/TMPDIR roots"
    );
    let retired = retire_control.retire().await.expect("remote retire ACK");
    assert_eq!(retired.lifecycle, LifecycleState::Retired);
    wait_for_child(&mut retire_child).await;
    assert!(
        !endpoint.socket().exists(),
        "retire unlinks native endpoint"
    );

    // Without durable Ready and old CommitIntent, a prepared old child refuses
    // Commit rather than claiming a completed stop. Abort restores its endpoint.
    let mut old_child = spawn_native_child(
        &config,
        &cache,
        &herdr_binary,
        herdr.socket(),
        endpoint.socket(),
        root.path(),
        None,
    );
    let mut old_control = connect_when_ready(endpoint.socket(), &mut old_child).await;
    let old_target = muxe::compatibility::embedded_record()
        .expect("native child embeds compatibility record")
        .handoff;
    let requested_handoff = muxe_protocol::control::HandoffId([0x51; 16]);
    let prepared = old_control
        .prepare(old_target, requested_handoff)
        .await
        .expect("old broker Prepare ACK");
    assert_eq!(prepared.lifecycle, LifecycleState::Draining);
    let old_handoff = prepared.handoff_id.expect("old Prepare handoff");
    assert_eq!(old_handoff, requested_handoff);
    assert!(
        !endpoint.socket().exists(),
        "old Prepare drains the native endpoint before Commit"
    );
    assert!(
        matches!(
            old_control.commit(old_handoff).await,
            Err(muxe::lifecycle::control::ControlError::Rejected { .. })
        ),
        "old Commit requires durable Ready and exact old intent"
    );
    let restored = old_control
        .abort(old_handoff)
        .await
        .expect("old broker Abort ACK");
    assert_eq!(restored.lifecycle, LifecycleState::Running);
    let retired = old_control.retire().await.expect("old broker Retire ACK");
    assert_eq!(retired.lifecycle, LifecycleState::Retired);
    wait_for_child(&mut old_child).await;
    assert!(!endpoint.socket().exists(), "old child unlinks on Retire");

    let handoff = HandoffId([0x42; 16]);
    let journal = target_journal(
        &cache,
        endpoint.socket(),
        &herdr.socket().to_string_lossy(),
        handoff,
    );
    let handoff_text = handoff_hex(&handoff);
    let mut abort_child = spawn_native_child(
        &config,
        &cache,
        &herdr_binary,
        herdr.socket(),
        endpoint.socket(),
        root.path(),
        Some((&handoff_text, &journal)),
    );
    let mut abort_control = connect_when_ready(endpoint.socket(), &mut abort_child).await;
    let aborted = abort_control
        .abort(handoff)
        .await
        .expect("remote target abort ACK");
    assert_eq!(aborted.lifecycle, LifecycleState::Retired);
    wait_for_child(&mut abort_child).await;
    assert!(
        !endpoint.socket().exists(),
        "abort retires native target endpoint"
    );

    let journal = target_journal(
        &cache,
        endpoint.socket(),
        &herdr.socket().to_string_lossy(),
        handoff,
    );
    let mut commit_child = spawn_native_child(
        &config,
        &cache,
        &herdr_binary,
        herdr.socket(),
        endpoint.socket(),
        root.path(),
        Some((&handoff_text, &journal)),
    );
    let mut commit_control = connect_when_ready(endpoint.socket(), &mut commit_child).await;
    let status = commit_control.status().await.expect("gated target status");
    assert!(
        matches!(
            commit_control.commit(handoff).await,
            Err(muxe::lifecycle::control::ControlError::Rejected { .. })
        ),
        "pre-Ready target cannot Commit on handoff alone"
    );
    let mut ready = muxe::lifecycle::journal::read_journal(&journal)
        .expect("read native target activation journal");
    ready.members_mut()[0].target = TargetMemberProgress::Ready;
    let row = muxe::lifecycle::Registry::open(&cache)
        .expect("open native registry")
        .entries()
        .expect("read native registry")
        .into_iter()
        .find(|entry| entry.socket == endpoint.socket())
        .expect("target registered exact endpoint");
    assert_eq!(row.server_pid, commit_child.id().unwrap());
    let proof = ReadyProof::new(
        &ready,
        None,
        vec![
            ReadyMemberProof::new(&ready.members()[0], &row, &status.live_server.server_id)
                .expect("target has process registration identity"),
        ],
    )
    .expect("seal exact native target incarnation");
    let mut wrong = proof.clone();
    wrong.incarnations[0].entry.registration_id =
        Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
    ready.enter_ready(Some(wrong));
    muxe::lifecycle::journal::write_journal(&cache, &ready)
        .expect("persist incorrect target proof for refusal");
    assert!(
        matches!(
            commit_control.commit(handoff).await,
            Err(muxe::lifecycle::control::ControlError::Rejected { .. })
        ),
        "foreign registration proof cannot Commit"
    );
    ready.enter_ready(Some(proof));
    muxe::lifecycle::journal::write_journal(&cache, &ready)
        .expect("persist exact native Ready proof");
    let registry = muxe::lifecycle::Registry::open(&cache).expect("open native registry");
    let mut replacement = row.clone();
    replacement.server_pid += 1;
    replacement.registration_id =
        Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
    registry
        .register_herdr(replacement)
        .expect("publish a replacement at the same endpoint");
    assert!(
        matches!(
            commit_control.commit(handoff).await,
            Err(muxe::lifecycle::control::ControlError::Rejected { .. })
        ),
        "replacement registry incarnation cannot inherit sealed Ready"
    );
    registry
        .register_herdr(row)
        .expect("restore original registered incarnation");
    let committed = commit_control
        .commit(handoff)
        .await
        .expect("remote target commit ACK");
    assert_eq!(committed.lifecycle, LifecycleState::Running);
    let repeated = commit_control
        .commit(handoff)
        .await
        .expect("same native target can repeat Commit");
    assert_eq!(repeated.lifecycle, LifecycleState::Running);
    assert!(
        commit_child
            .try_wait()
            .expect("inspect commit child")
            .is_none(),
        "target Commit ACK must arrive before native child exits"
    );
    // A committed target is the active broker after handoff. Retirement must
    // accept that committed state, ACK, unlink the endpoint, and only then let
    // the native child exit.
    let retired = commit_control
        .retire()
        .await
        .expect("committed target retire ACK");
    assert_eq!(retired.lifecycle, LifecycleState::Retired);
    wait_for_child(&mut commit_child).await;
    assert!(
        !endpoint.socket().exists(),
        "committed target retire unlinks native endpoint"
    );
}
