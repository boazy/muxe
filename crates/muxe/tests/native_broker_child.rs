#[path = "support/scoped_env.rs"]
mod scoped_env;
use scoped_env::{apply_scoped_env, ensure_scoped_dirs};

use std::{
    fmt::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use muxe::lifecycle::{
    control::ControlClient,
    journal::{ActivationJournal, JournalState, MemberState, MemberTransition, UnitKind},
};
use muxe_broker::RuntimeEndpoint;
use muxe_protocol::control::{HandoffId, LifecycleState};
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
    let handoff_hex = handoff_hex(&handoff);
    let mut journal = ActivationJournal::new(
        UnitKind::Herdr {
            host_hash: "native-child".to_owned(),
        },
        record.clone(),
        record,
        vec![MemberState {
            host_identity: host.to_owned(),
            old_socket: endpoint.to_path_buf(),
            target_socket: None,
            handoff_id: Some(handoff_hex),
            state: MemberTransition::Prepared,
        }],
    );
    journal.state = JournalState::Prepared;
    muxe::lifecycle::journal::write_journal(cache, &journal)
        .expect("write target authorization journal")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one subprocess lifecycle proof keeps old and target role ACK/exit ordering together"
)]
async fn native_child_acknowledges_remote_retire_abort_and_commit_before_exit() {
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

    // Exercise the old prepared-broker role separately from the target role:
    // Prepare drains the old listener, then Commit must ACK as SupervisorOnly
    // before the supervisor-only child exits.
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
    let prepared = old_control
        .prepare(old_target)
        .await
        .expect("old broker Prepare ACK");
    assert_eq!(prepared.lifecycle, LifecycleState::Draining);
    let old_handoff = prepared.handoff_id.expect("old Prepare handoff");
    assert!(
        !endpoint.socket().exists(),
        "old Prepare drains the native endpoint before Commit"
    );
    let old_committed = old_control
        .commit(old_handoff)
        .await
        .expect("old broker Commit ACK");
    assert_eq!(old_committed.lifecycle, LifecycleState::SupervisorOnly);
    wait_for_child(&mut old_child).await;
    assert!(
        !endpoint.socket().exists(),
        "old supervisor-only child exits after Commit ACK"
    );

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
    let committed = commit_control
        .commit(handoff)
        .await
        .expect("remote target commit ACK");
    assert_eq!(committed.lifecycle, LifecycleState::Running);
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
