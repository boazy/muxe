//! The bootstrap must keep its initial client until the runner authorizes
//! detach. A rendered session alone cannot release that client.

use interprocess::local_socket::{prelude::*, ListenerNonblockingMode};
use std::io::Write as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use zellij_utils::consts::ipc_bind;
use zellij_utils::ipc::{ClientToServerMsg, IpcSenderWithContext, ServerToClientMsg};

fn serve_bootstrap_peer(
    listener: &interprocess::local_socket::Listener,
    drained: &std::sync::mpsc::Sender<Result<(), String>>,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    let stream = loop {
        match listener.accept() {
            Ok(stream) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "bootstrap never connected");
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("accept failed: {error}"),
        }
    };
    stream
        .set_recv_timeout(Some(Duration::from_secs(10)))
        .expect("bounded protocol receive");
    stream
        .set_send_timeout(Some(Duration::from_secs(10)))
        .expect("bounded protocol send");
    let mut sender: IpcSenderWithContext<ServerToClientMsg> = IpcSenderWithContext::new(stream);
    let mut receiver = sender.get_receiver::<ClientToServerMsg>();
    let initial = receiver.try_recv_client_msg().expect("initial message");
    assert!(matches!(
        initial.0,
        ClientToServerMsg::FirstClientConnected { .. }
    ));
    sender
        .send_server_msg(ServerToClientMsg::Render {
            content: "initialized owned session".to_owned(),
        })
        .expect("render evidence");
    // Larger than a Unix socket's usual send buffer: the retained
    // bootstrap must drain subsequent renders before detach is allowed.
    let sent = sender
        .send_server_msg(ServerToClientMsg::Render {
            content: "x".repeat(512 * 1024),
        })
        .map_err(|error| error.to_string());
    drained.send(sent).expect("render drain evidence");
    receiver
        .try_recv_client_msg()
        .is_ok_and(|(message, _)| matches!(message, ClientToServerMsg::ClientExited))
}

fn exercise_handoff(command: &[u8], expected_success: bool) {
    let root = tempfile::tempdir().expect("fresh owned root");
    let socket = root.path().join("session.sock");
    let ready = root.path().join("ready");
    let listener = ipc_bind(&socket).expect("explicit owned socket");
    listener
        .set_nonblocking(ListenerNonblockingMode::Accept)
        .expect("bounded accept");
    let (drained_tx, drained_rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || serve_bootstrap_peer(&listener, &drained_tx));
    let mut child = Command::new(env!("CARGO_BIN_EXE_muxe-zellij-bootstrap"))
        .arg("--socket")
        .arg(&socket)
        .arg("--config")
        .arg(root.path().join("config.kdl"))
        .arg("--config-dir")
        .arg(root.path())
        .arg("--data-dir")
        .arg(root.path())
        .arg("--cwd")
        .arg(root.path())
        .arg("--ready-file")
        .arg(&ready)
        .arg("--timeout-secs")
        .arg("10")
        .env_clear()
        .env("HOME", root.path())
        .current_dir(root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("retained bootstrap child");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready.is_file() {
        if child.try_wait().expect("poll child").is_some() || Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output().expect("reap failed bootstrap");
            panic!(
                "no render evidence: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !matches!(
        drained_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())),
        Ok(Ok(()))
    ) {
        let _ = child.kill();
        let output = child.wait_with_output().expect("reap blocked bootstrap");
        panic!(
            "retained bootstrap blocked host renders: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    child
        .stdin
        .take()
        .expect("owned handoff pipe")
        .write_all(command)
        .expect("explicit handoff");
    while child.try_wait().expect("poll detach").is_none() {
        if Instant::now() >= deadline {
            child.kill().expect("kill exact hung child");
            let output = child.wait_with_output().expect("reap hung bootstrap");
            panic!("handoff hung: {}", String::from_utf8_lossy(&output.stderr));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().expect("reap bootstrap");
    assert_eq!(output.status.success(), expected_success);
    assert_eq!(
        server.join().expect("owned protocol peer"),
        expected_success
    );
}

#[test]
fn rendered_session_detaches_only_after_valid_handoff() {
    exercise_handoff(b"detach\n", true);
}

#[test]
fn invalid_handoff_never_detaches_a_rendered_session() {
    exercise_handoff(b"not-detach\n", false);
}
