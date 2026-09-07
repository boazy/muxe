//! Host-free smoke for the permission seeder: exact key plus exact
//! permission names land in the pinned-default cache file, merges
//! preserve other keys, and unknown inputs fail closed. No hosts, no
//! sockets, no approval: the seeder child runs under a TempDir-scoped
//! environment and the pinned code computes its own cache path, which
//! the test only locates from the binary's own report.

use std::path::PathBuf;
use std::process::Output;
use std::time::{Duration, Instant};

const CASE_TIMEOUT: Duration = Duration::from_mins(1);
const PLUGIN_KEY: &str = "/tmp/owned-fixture/muxe-zellij.wasm";
const PERMISSIONS: [&str; 3] = [
    "ReadApplicationState",
    "ChangeApplicationState",
    "ReadCliPipes",
];

fn seeder_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_muxe-zellij-permit"))
}

fn case_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "muxe-permit-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("owned case root");
    root
}

fn scoped_env(root: &std::path::Path) -> Vec<(String, String)> {
    vec![
        ("HOME".to_owned(), root.join("home").display().to_string()),
        (
            "XDG_CACHE_HOME".to_owned(),
            root.join("cache").display().to_string(),
        ),
        ("PATH".to_owned(), std::env::var("PATH").unwrap_or_default()),
    ]
}

fn run_seeder(root: &std::path::Path, argv: &[&str]) -> Output {
    use std::process::Stdio;
    let mut command = std::process::Command::new(seeder_bin());
    command.env_clear();
    for (name, value) in scoped_env(root) {
        command.env(name, value);
    }
    command
        .args(argv)
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn seeder");
    let deadline = Instant::now() + CASE_TIMEOUT;
    loop {
        if child.try_wait().expect("poll seeder").is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "seeder hung");
        std::thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().expect("reap seeder")
}

#[test]
fn seeds_exact_key_and_permissions_in_scoped_cache() {
    let root = case_root("seed");
    let mut argv = vec!["--plugin", PLUGIN_KEY];
    for permission in PERMISSIONS {
        argv.push("--permission");
        argv.push(permission);
    }
    let output = run_seeder(&root, &argv);
    assert!(
        output.status.success(),
        "seeder failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let report = String::from_utf8_lossy(&output.stdout).into_owned();
    let path = report
        .strip_prefix(&format!("permitted {PLUGIN_KEY} (3 permissions) at "))
        .unwrap_or_else(|| panic!("unexpected seeder report: {report:?}"))
        .trim()
        .to_owned();
    assert!(
        PathBuf::from(&path).starts_with(&root),
        "seeder cache escaped the owned root: {path:?}",
    );
    let body = std::fs::read_to_string(&path).expect("pinned cache file exists");
    assert!(body.contains(PLUGIN_KEY), "cache names no plugin key");
    for permission in PERMISSIONS {
        assert!(
            body.contains(permission),
            "cache names no {permission} grant"
        );
    }
    // Idempotent merge: a second seed for another plugin preserves ours.
    let output = run_seeder(
        &root,
        &[
            "--plugin",
            "/other/plugin.wasm",
            "--permission",
            "OpenFiles",
        ],
    );
    assert!(output.status.success());
    let body = std::fs::read_to_string(&path).expect("cache survives merge");
    assert!(body.contains(PLUGIN_KEY));
    assert!(body.contains("OpenFiles"));
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn rejects_unknown_permission_and_missing_inputs() {
    let root = case_root("reject");
    let denied = run_seeder(&root, &["--plugin", PLUGIN_KEY, "--permission", "Bogus"]);
    assert_eq!(denied.status.code(), Some(2));
    let missing = run_seeder(&root, &["--permission", PERMISSIONS[0]]);
    assert_eq!(missing.status.code(), Some(2));
    let bare = run_seeder(&root, &[]);
    assert_eq!(bare.status.code(), Some(2));
    std::fs::remove_dir_all(&root).ok();
}
