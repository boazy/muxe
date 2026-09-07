//! Host-free fixture smoke for the one-shot fault injector: transparent
//! forwarding, exact match, one-shot consumption, barrier hold/release,
//! real child status propagation, and bounded timeout cleanup.
//!
//! The "pinned CLI" is an owned fake executable (shell script) that
//! records its argv and exits a configured code. No hosts, no sockets,
//! no approval: these are plain tests over real processes.
//!
//! Requires the `muxe-zellij-fault-injector` bin target (core manifest
//! side); the binary path arrives via `CARGO_BIN_EXE`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const CASE_TIMEOUT: Duration = Duration::from_mins(1);

fn injector_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_muxe-zellij-fault-injector"))
}

fn case_root(name: &str) -> CaseDir {
    CaseDir::named(name)
}

/// Owned fake CLI: appends its argv line to `$ARGV_LOG` and exits
/// `$FAKE_EXIT`. Absolute path, owner-executable.
fn fake_cli(root: &Path) -> PathBuf {
    let path = root.join("fake-zellij");
    std::fs::write(
        &path,
        "#!/bin/sh\necho \"$@\" >> \"$ARGV_LOG\"\nexit \"$FAKE_EXIT\"\n",
    )
    .expect("write fake cli");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
            .expect("chmod fake cli");
    }
    path
}

/// Owned unique case directory: atomically created (no fixed-pattern
/// removal, no PID-name reuse), removed on drop. Only the exact created
/// path is ever removed.
struct CaseDir {
    path: PathBuf,
}

impl CaseDir {
    fn named(name: &str) -> Self {
        let base = std::env::temp_dir().join("muxe-fault-injector-smoke");
        let _ = std::fs::create_dir(&base);
        let mut counter = 0u32;
        loop {
            let path = base.join(format!(
                "{name}-{}-{}-{}",
                std::process::id(),
                counter,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(u128::from(counter), |elapsed| elapsed.as_nanos()),
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self { path },
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    counter += 1;
                }
                Err(error) => panic!("case dir creation failed: {error}"),
            }
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for CaseDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

impl std::ops::Deref for CaseDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        self.path()
    }
}

fn argv_lines(root: &Path) -> Vec<String> {
    let log = root.join("argv.log");
    if !log.is_file() {
        return Vec::new();
    }
    std::fs::read_to_string(&log)
        .expect("read argv log")
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Runs the injector with a hard test-side bound: a hung child fails the
/// case instead of hanging the suite.
fn run_injector(env: &HashMap<String, String>, argv: &[&str]) -> (Output, Duration) {
    let mut command = Command::new(injector_bin());
    command.args(argv);
    command.env_clear();
    command.env("PATH", std::env::var_os("PATH").unwrap_or_default());
    for (name, value) in env {
        command.env(name, value);
    }
    let started = Instant::now();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(command.output());
    });
    match receiver.recv_timeout(CASE_TIMEOUT) {
        Ok(Ok(output)) => (output, started.elapsed()),
        Ok(Err(error)) => panic!("injector spawn failed: {error}"),
        Err(error) => panic!("injector hung past the case bound: {error}"),
    }
}

fn base_env(root: &Path, fake: &Path) -> HashMap<String, String> {
    HashMap::from([
        ("MUXE_FI_REAL_ZELLIJ".to_owned(), fake.display().to_string()),
        ("MUXE_FI_FINAL_SESSION".to_owned(), "final".to_owned()),
        (
            "MUXE_FI_STABLE_URL".to_owned(),
            "file:/owned/stable.wasm".to_owned(),
        ),
        (
            "MUXE_FI_BARRIER_DIR".to_owned(),
            root.join("barrier").display().to_string(),
        ),
        (
            "MUXE_FI_FAIL_URL".to_owned(),
            "file:/owned/does-not-exist.wasm".to_owned(),
        ),
        (
            "ARGV_LOG".to_owned(),
            root.join("argv.log").display().to_string(),
        ),
        ("FAKE_EXIT".to_owned(), "0".to_owned()),
    ])
}

#[test]
fn transparent_forward_propagates_status() {
    let root = case_root("forward");
    let fake = fake_cli(&root);
    let mut env = base_env(&root, &fake);
    env.insert("FAKE_EXIT".to_owned(), "3".to_owned());
    // `--version` cannot match the reload shape: pure passthrough.
    let (output, _) = run_injector(&env, &["--version"]);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(argv_lines(&root), vec!["--version".to_owned()]);
}

#[test]
fn non_final_session_passes_through() {
    let root = case_root("other-session");
    let fake = fake_cli(&root);
    let env = base_env(&root, &fake);
    let argv = [
        "--session",
        "other",
        "action",
        "start-or-reload-plugin",
        "file:/owned/stable.wasm",
    ];
    let (output, _) = run_injector(&env, &argv);
    assert!(output.status.success());
    assert!(!root.join("barrier").join("request-final").exists());
    assert_eq!(
        argv_lines(&root),
        vec![argv.join(" ")],
        "non-matching argv forwards verbatim"
    );
}

#[test]
fn matched_reload_holds_releases_and_fails_real() {
    let root = case_root("match");
    let fake = fake_cli(&root);
    let barrier = root.join("barrier");
    std::fs::create_dir_all(&barrier).expect("barrier dir");
    let mut env = base_env(&root, &fake);
    env.insert("MUXE_FI_BARRIER_TIMEOUT_SECS".to_owned(), "30".to_owned());
    let argv = [
        "--config",
        "/owned/config.kdl",
        "--session",
        "final",
        "action",
        "start-or-reload-plugin",
        "file:/owned/stable.wasm",
    ];
    let (sender, receiver) = mpsc::channel();
    let env_clone = env.clone();
    let argv_owned: Vec<String> = argv.iter().map(ToString::to_string).collect();
    thread::spawn(move || {
        let refs: Vec<&str> = argv_owned.iter().map(String::as_str).collect();
        let mut command = Command::new(injector_bin());
        command.args(&refs);
        command.env_clear();
        command.env("PATH", std::env::var_os("PATH").unwrap_or_default());
        for (name, value) in &env_clone {
            command.env(name, value);
        }
        let _ = sender.send(command.output());
    });
    // The wrapper must hold at the barrier: request appears, no argv
    // reaches the fake CLI yet.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if barrier.join("request-final").exists() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "injector never held at the barrier"
        );
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        argv_lines(&root).is_empty(),
        "held reload forwards nothing before release"
    );
    std::fs::write(barrier.join("release-final"), b"release").expect("release");
    let output = receiver
        .recv_timeout(CASE_TIMEOUT)
        .expect("injector hung past release")
        .expect("injector spawn failed");
    assert!(output.status.success());
    assert!(
        barrier.join("consumed").is_file(),
        "one-shot consumed exactly once"
    );
    assert_eq!(
        argv_lines(&root),
        vec![[
            "--config",
            "/owned/config.kdl",
            "--session",
            "final",
            "action",
            "start-or-reload-plugin",
            "file:/owned/does-not-exist.wasm",
        ]
        .join(" ")],
        "released reload runs the real CLI with the fail URL swapped"
    );
}

#[test]
fn consumed_marker_passes_rollback_through() {
    let root = case_root("oneshot");
    let fake = fake_cli(&root);
    let barrier = root.join("barrier");
    std::fs::create_dir_all(&barrier).expect("barrier dir");
    std::fs::write(barrier.join("consumed"), b"already-fired").expect("seed consumed");
    let env = base_env(&root, &fake);
    let argv = [
        "--session",
        "final",
        "action",
        "start-or-reload-plugin",
        "file:/owned/stable.wasm",
    ];
    let (output, _) = run_injector(&env, &argv);
    assert!(output.status.success());
    assert_eq!(
        argv_lines(&root),
        vec![argv.join(" ")],
        "post-consumption reloads forward the original URL unchanged"
    );
}

#[test]
fn barrier_timeout_exits_bounded() {
    let root = case_root("timeout");
    let fake = fake_cli(&root);
    let barrier = root.join("barrier");
    std::fs::create_dir_all(&barrier).expect("barrier dir");
    let mut env = base_env(&root, &fake);
    env.insert("MUXE_FI_BARRIER_TIMEOUT_SECS".to_owned(), "1".to_owned());
    let argv = [
        "--session",
        "final",
        "action",
        "start-or-reload-plugin",
        "file:/owned/stable.wasm",
    ];
    let (output, elapsed) = run_injector(&env, &argv);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        elapsed < Duration::from_secs(20),
        "timeout path stays bounded"
    );
    assert!(
        argv_lines(&root).is_empty(),
        "unreleased reload never reaches the host"
    );
}
