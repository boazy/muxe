use super::*;
use std::{
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    process::{Child, Command, Stdio},
    thread,
    time::Instant,
};

const DEADLINE: Duration = Duration::from_secs(3);
const CHILD_MODE: &str = "MUXE_PROCESS_OWNED_CHILD_MODE";
const CHILD_SOCKET: &str = "MUXE_PROCESS_OWNED_CHILD_SOCKET";

struct OwnedChild(Child);

impl OwnedChild {
    fn process(&self) -> ProcessId {
        ProcessId::new(self.0.id()).expect("owned child has a positive pid_t")
    }

    fn assert_alive(&mut self) {
        assert!(self.0.try_wait().expect("query owned child").is_none());
    }

    fn wait(&mut self) {
        let deadline = Instant::now() + DEADLINE;
        loop {
            if self.0.try_wait().expect("reap owned child").is_some() {
                return;
            }
            assert!(Instant::now() < deadline, "owned child did not exit");
            thread::sleep(Duration::from_millis(2));
        }
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        // This is only the retained direct child created by this fixture. Never
        // search for a process, signal a process group, or clean up by name.
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

struct OwnedPeer {
    child: OwnedChild,
    stream: UnixStream,
    _directory: tempfile::TempDir,
}

impl OwnedPeer {
    fn start(mode: &str) -> Self {
        let directory = tempfile::tempdir().expect("fresh owned fixture directory");
        let path = directory.path().join("control.sock");
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--ignored",
                "--exact",
                "tests::owned_child_helper",
                "--nocapture",
            ])
            .env(CHILD_MODE, mode)
            .env(CHILD_SOCKET, &path)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn retained owned child");
        let mut child = OwnedChild(child);
        let deadline = Instant::now() + DEADLINE;
        let mut stream = loop {
            match UnixStream::connect(&path) {
                Ok(stream) => break stream,
                Err(error) => {
                    child.assert_alive();
                    assert!(
                        Instant::now() < deadline,
                        "owned socket was not ready: {error}"
                    );
                    thread::sleep(Duration::from_millis(2));
                }
            }
        };
        if mode == "accepted" {
            stream.set_read_timeout(Some(DEADLINE)).unwrap();
            let mut ready = [0];
            stream
                .read_exact(&mut ready)
                .expect("owned child accepts this stream");
            assert_eq!(ready, *b"R");
        }
        Self {
            child,
            stream,
            _directory: directory,
        }
    }

    fn capture(&self) -> BrokerStopHandle {
        BrokerStopHandle::capture(&self.stream, self.child.process())
            .expect("capture only the known owned peer")
    }
}

#[test]
#[ignore = "owned fixture helper invoked explicitly by safety tests"]
fn owned_child_helper() {
    let mode = std::env::var(CHILD_MODE).expect("explicit owned child mode");
    if mode == "replacement" {
        let mut byte = [0];
        let _ = std::io::stdin().read(&mut byte);
        return;
    }
    let path = std::env::var_os(CHILD_SOCKET).expect("owned fixture socket");
    let listener = UnixListener::bind(path).expect("bind only the fresh owned fixture socket");
    let mut accepted = if mode == "accepted" {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"R").unwrap();
        Some(stream)
    } else {
        assert_eq!(mode, "pending");
        None
    };
    let mut byte = [0];
    if std::io::stdin().read(&mut byte).unwrap() == 1 && byte == *b"E" {
        use std::os::unix::process::CommandExt;
        // Exec the same owned fixture, not a shell or another child. The retained
        // parent Child handle still owns this PID; the broker socket is CLOEXEC.
        let error = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "tests::owned_child_helper",
                "--nocapture",
            ])
            .env(CHILD_MODE, "replacement")
            .exec();
        panic!("owned exec failed: {error}");
    }
    // Keep both listener and accepted socket alive until the parent stops us.
    drop(accepted.take());
    drop(listener);
}

#[test]
fn process_id_constructor_and_deserializer_enforce_pid_t_range() {
    for value in [0, i32::MAX as u32 + 1, u32::MAX] {
        assert!(ProcessId::new(value).is_err());
        assert!(serde_json::from_str::<ProcessId>(&value.to_string()).is_err());
    }
    assert!(serde_json::from_str::<ProcessId>("-1").is_err());
    for value in [1, i32::MAX as u32] {
        let process = ProcessId::new(value).unwrap();
        assert_eq!(process.get(), value);
        assert_eq!(
            serde_json::from_str::<ProcessId>(&value.to_string()).unwrap(),
            process
        );
        assert_eq!(serde_json::to_string(&process).unwrap(), value.to_string());
    }
}

#[test]
fn namespace_identity_rejects_missing_or_different_context() {
    let namespace = LinuxPidNamespace::new(4, 4_026_531_836).unwrap();
    assert!(namespace.verify(namespace).is_ok());
    assert!(matches!(
        namespace.verify(LinuxPidNamespace::new(5, 4_026_531_836).unwrap()),
        Err(ProcessError::ObservationContext)
    ));
    assert!(matches!(
        namespace.verify(LinuxPidNamespace::new(4, 4_026_531_837).unwrap()),
        Err(ProcessError::ObservationContext)
    ));
    assert!(LinuxPidNamespace::new(4, 0).is_err());
    assert!(serde_json::from_str::<LinuxPidNamespace>("[4,0]").is_err());
}

#[test]
fn procfs_view_requires_one_current_namespace_tgid() {
    let process = ProcessId::new(42).unwrap();
    assert!(validate_procfs_view("Name:\tfixture\nNStgid:\t42\n", process).is_ok());
    for status in [
        "Name:\tfixture\n",
        "NStgid:\n",
        "NStgid:\t41\n",
        "NStgid:\t9000\t42\n",
        "NStgid:\t42\t42\n",
        "NStgid:\tinvalid\n",
    ] {
        assert!(matches!(
            validate_procfs_view(status, process),
            Err(ProcessError::ObservationContext)
        ));
    }
}

#[test]
fn completed_observations_preserve_query_errors() {
    assert!(completed_observation(1).unwrap());
    assert!(matches!(
        completed_observation(0),
        Err(ProcessError::AmbiguousIncarnation)
    ));
    for errno in [nix::libc::EPERM, nix::libc::ENOTSUP, nix::libc::EINVAL] {
        match completed_observation(-errno) {
            Err(ProcessError::Io(error)) => assert_eq!(error.raw_os_error(), Some(errno)),
            result => panic!("lost query error: {result:?}"),
        }
    }
    assert!(matches!(
        completed_observation(2),
        Err(ProcessError::Identity)
    ));
    assert!(matches!(
        completed_observation(i32::MIN),
        Err(ProcessError::Identity)
    ));
}

#[test]
fn boot_id_deserializer_preserves_strict_shape_and_canonical_case() {
    let lower = "abcdef12-3456-7890-abcd-0123456789ab";
    let upper = "ABCDEF12-3456-7890-ABCD-0123456789AB";
    for value in [lower, upper] {
        let boot: BootId = serde_json::from_value(serde_json::json!(value)).unwrap();
        assert_eq!(boot.0, lower);
    }
    for value in [
        "",
        "not-a-boot-uuid",
        " abcdef12-3456-7890-abcd-0123456789ab",
        "abcdef12-3456-7890-abcd-0123456789ab ",
        "abcdef1234567890abcd0123456789ab",
    ] {
        assert!(serde_json::from_value::<BootId>(serde_json::json!(value)).is_err());
    }
}

#[test]
fn persisted_identity_normalizes_boot_case_and_rejects_invalid_pid() {
    let identity = ProcessIdentity::current().expect("own OS identity");
    let mut value = serde_json::to_value(&identity).unwrap();
    value["boot"] = value["boot"].as_str().unwrap().to_ascii_uppercase().into();
    assert_eq!(
        serde_json::from_value::<ProcessIdentity>(value.clone()).unwrap(),
        identity
    );
    value["boot"] = "not-a-boot-uuid".into();
    assert!(serde_json::from_value::<ProcessIdentity>(value).is_err());
    for invalid in [0, i32::MAX as u32 + 1] {
        let mut value = serde_json::to_value(&identity).unwrap();
        #[cfg(target_os = "macos")]
        {
            value["token"][5] = invalid.into();
        }
        #[cfg(target_os = "linux")]
        {
            value["process"] = invalid.into();
        }
        assert!(serde_json::from_value::<ProcessIdentity>(value).is_err());
    }
}

#[test]
fn owned_peer_stop_and_persisted_exit_proof() {
    let mut peer = OwnedPeer::start("accepted");
    let handle = peer.capture();
    assert_eq!(handle.identity().process(), peer.child.process());
    assert!(!handle.identity().is_gone().unwrap());
    handle
        .prove_socket_peer(&peer.stream)
        .expect("same accepted owned socket");
    let identity = handle.identity().clone();
    handle
        .kill_and_wait(DEADLINE)
        .expect("atomic owned stop and exact exit observer");
    let deadline = Instant::now() + DEADLINE;
    loop {
        if matches!(identity.is_gone(), Ok(true)) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "completed cleanup was not observable before reap"
        );
        thread::sleep(Duration::from_millis(2));
    }
    peer.child.wait();
    assert!(
        identity.is_gone().unwrap(),
        "completed crash-before-proof is recoverable after reap"
    );
}

#[test]
fn unrelated_socket_cannot_be_certified_as_captured_peer() {
    let mut original = OwnedPeer::start("accepted");
    let mut unrelated = OwnedPeer::start("accepted");
    let handle = original.capture();
    assert!(handle.prove_socket_peer(&unrelated.stream).is_err());
    original.child.assert_alive();
    unrelated.child.assert_alive();
}

#[cfg(target_os = "macos")]
#[test]
fn pending_darwin_connection_has_no_accepted_descriptor_proof() {
    let mut peer = OwnedPeer::start("pending");
    let handle = peer.capture();
    assert!(matches!(
        handle.prove_socket_peer(&peer.stream),
        Err(ProcessError::SocketOwnership)
    ));
    peer.child.assert_alive();
}

#[cfg(target_os = "macos")]
#[test]
fn wrong_darwin_version_cannot_signal_owned_peer() {
    let mut peer = OwnedPeer::start("accepted");
    let handle = peer.capture();
    let RecordedIdentity::Darwin { token, .. } = &handle.identity.0;
    let mut wrong = token.0;
    wrong[7] ^= 0x4000_0000;
    // SAFETY: fixed token words refer only to this retained owned child; the
    // intentionally wrong version must be rejected by the exact kernel API.
    assert_eq!(
        unsafe { muxe_process_signal_token(wrong.as_ptr()) },
        nix::libc::ESRCH
    );
    peer.child.assert_alive();
    handle.kill_and_wait(DEADLINE).unwrap();
    peer.child.wait();
}

#[cfg(target_os = "macos")]
#[test]
fn exec_version_change_is_not_an_exit_certificate() {
    let mut peer = OwnedPeer::start("accepted");
    let handle = peer.capture();
    let RecordedIdentity::Darwin { token, .. } = &handle.identity.0;
    peer.child
        .0
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"E")
        .unwrap();
    let deadline = Instant::now() + DEADLINE;
    while validate_token(token).is_ok() {
        peer.child.assert_alive();
        assert!(
            Instant::now() < deadline,
            "owned exec did not change audit version"
        );
        thread::sleep(Duration::from_millis(2));
    }
    peer.child.assert_alive();
    assert!(matches!(
        handle.identity().is_gone(),
        Err(ProcessError::AmbiguousIncarnation)
    ));
    assert!(handle.kill_and_wait(Duration::from_millis(20)).is_err());
    peer.child.assert_alive();
}

#[cfg(target_os = "macos")]
#[test]
fn c_completed_query_rejects_invalid_pid_as_errno() {
    // SAFETY: this observation-only query rejects invalid IDs before proc_info.
    let code = unsafe { muxe_process_pid_completed(0) };
    assert_eq!(code, -nix::libc::EINVAL);
    assert!(
        matches!(completed_observation(code), Err(ProcessError::Io(error)) if error.raw_os_error() == Some(nix::libc::EINVAL))
    );
}

#[cfg(target_os = "linux")]
#[test]
fn live_pid_never_becomes_dead_from_namespace_or_tick_mismatch() {
    let identity = ProcessIdentity::current().unwrap();
    let mut changed_ticks = serde_json::to_value(&identity).unwrap();
    changed_ticks["start"] = changed_ticks["start"]
        .as_u64()
        .unwrap()
        .saturating_add(1)
        .into();
    let changed_ticks: ProcessIdentity = serde_json::from_value(changed_ticks).unwrap();
    assert!(!changed_ticks.is_gone().unwrap());
    let mut other_namespace = serde_json::to_value(&identity).unwrap();
    other_namespace["namespace"][1] = other_namespace["namespace"][1]
        .as_u64()
        .unwrap()
        .saturating_add(1)
        .into();
    let other_namespace: ProcessIdentity = serde_json::from_value(other_namespace).unwrap();
    assert!(matches!(
        other_namespace.is_gone(),
        Err(ProcessError::ObservationContext)
    ));
    assert!(!identity.authorizes_stored_peer());
    assert!(BrokerStopHandle::resume_recorded(&identity).is_err());
}
