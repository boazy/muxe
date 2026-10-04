//! The bootstrap must keep its initial client until the runner authorizes
//! detach. A rendered session alone cannot release that client.

use interprocess::local_socket::{prelude::*, ListenerNonblockingMode};
use std::io::Write as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use zellij_utils::consts::ipc_bind;
use zellij_utils::ipc::{
    ClientToServerMsg, IpcReceiveError, IpcSenderWithContext, ServerToClientMsg,
};

fn serve_bootstrap_peer(
    listener: &interprocess::local_socket::Listener,
    drained: &std::sync::mpsc::Sender<Result<(), String>>,
) -> Result<bool, String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let stream = loop {
        match listener.accept() {
            Ok(stream) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err("bootstrap never connected".to_owned());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(format!("accept failed: {error}")),
        }
    };
    // Darwin inherits the listener's O_NONBLOCK on accept; interprocess only
    // sets that flag when requested, so explicitly clear it before timeouts.
    stream
        .set_nonblocking(false)
        .map_err(|error| format!("blocking accepted stream: {error}"))?;
    stream
        .set_recv_timeout(Some(Duration::from_secs(10)))
        .map_err(|error| format!("bounded protocol receive: {error}"))?;
    stream
        .set_send_timeout(Some(Duration::from_secs(10)))
        .map_err(|error| format!("bounded protocol send: {error}"))?;
    let mut sender: IpcSenderWithContext<ServerToClientMsg> = IpcSenderWithContext::new(stream);
    let mut receiver = sender.get_receiver::<ClientToServerMsg>();
    let initial = receiver
        .try_recv_client_msg()
        .map_err(|error| format!("initial message: {error}"))?;
    if !matches!(initial.0, ClientToServerMsg::FirstClientConnected { .. }) {
        return Err(format!("unexpected initial message: {:?}", initial.0));
    }
    sender
        .send_server_msg(ServerToClientMsg::Render {
            content: "initialized owned session".to_owned(),
        })
        .map_err(|error| format!("render evidence: {error}"))?;
    // Larger than a Unix socket's usual send buffer: the retained
    // bootstrap must drain subsequent renders before detach is allowed.
    let sent = sender
        .send_server_msg(ServerToClientMsg::Render {
            content: "x".repeat(512 * 1024),
        })
        .map_err(|error| format!("512 KiB render send failed: {error}"));
    let send_failed = sent.is_err();
    drained
        .send(sent)
        .map_err(|error| format!("render drain evidence disconnected: {error}"))?;
    if send_failed {
        return Ok(false);
    }
    match receiver.try_recv_client_msg() {
        Ok((ClientToServerMsg::ClientExited, _)) => Ok(true),
        Ok((message, _)) => Err(format!("unexpected handoff message: {message:?}")),
        // An invalid handoff closes the child without sending ClientExited.
        Err(IpcReceiveError::Disconnected) => Ok(false),
        Err(error) => Err(format!("handoff receive failed: {error}")),
    }
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
    let server = std::thread::spawn(move || serve_bootstrap_peer(&listener, &drained_tx));
    let deadline = Instant::now() + Duration::from_secs(10);
    let outcome = (|| -> Result<(), String> {
        while !ready.is_file() {
            if child
                .try_wait()
                .map_err(|error| format!("poll child: {error}"))?
                .is_some()
                || Instant::now() >= deadline
            {
                return Err("no render evidence before child exit/deadline".to_owned());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        drained_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| format!("render drain observation: {error}"))?
            .map_err(|error| format!("retained bootstrap blocked host renders: {error}"))?;
        child
            .stdin
            .take()
            .ok_or_else(|| "missing owned handoff pipe".to_owned())?
            .write_all(command)
            .map_err(|error| format!("explicit handoff: {error}"))?;
        while child
            .try_wait()
            .map_err(|error| format!("poll detach: {error}"))?
            .is_none()
        {
            if Instant::now() >= deadline {
                return Err("handoff hung".to_owned());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    })();
    // No assertion may escape before the exact child is reaped and the owned
    // protocol worker is joined, including receive/send/timeout failure paths.
    let kill_error = outcome.as_ref().err().and_then(|_| child.kill().err());
    let output = child.wait_with_output();
    let server_result = server.join();
    let output = output.expect("reap owned bootstrap");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        outcome.is_ok(),
        "bootstrap handoff failed: {outcome:?}; kill error: {kill_error:?}; \
         server: {server_result:?}; child status: {}; stderr: {stderr}",
        output.status,
    );
    assert_eq!(
        output.status.success(),
        expected_success,
        "child status: {}; stderr: {stderr}; server: {server_result:?}",
        output.status,
    );
    assert_eq!(
        server_result
            .expect("owned protocol peer")
            .expect("bootstrap protocol"),
        expected_success,
        "stderr: {stderr}",
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
