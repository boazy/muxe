use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

use tempfile::TempDir;

const SENSITIVE_CONFIG_PATH_LITERAL: &str = "sensitive-config-literal";

fn run_init(root: &Path) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_muxe"));
    command
        .arg("init")
        .env_clear()
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join(SENSITIVE_CONFIG_PATH_LITERAL))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("XDG_RUNTIME_DIR", root.join("runtime"))
        .env("TMPDIR", root.join("tmp"))
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .current_dir(root);
    command.output().expect("owned native CLI starts")
}

#[test]
fn native_command_failures_are_persisted_without_copying_stderr() {
    let root = TempDir::new().expect("fresh owned CLI root");
    let log = root
        .path()
        .join("cache")
        .join("muxe")
        .join("logs")
        .join("muxe.jsonl");

    let success = run_init(root.path());
    assert!(
        success.status.success(),
        "init must succeed under explicit owned XDG paths: {}",
        String::from_utf8_lossy(&success.stderr)
    );
    assert!(
        !log.exists(),
        "a successful command must not create a command-failure event"
    );

    let failure = run_init(root.path());
    assert!(!failure.status.success(), "repeat init must exit nonzero");
    let stderr = String::from_utf8_lossy(&failure.stderr);
    assert!(
        stderr.contains(SENSITIVE_CONFIG_PATH_LITERAL),
        "the primary stderr diagnostic retains its configuration context"
    );

    let records = std::fs::read_to_string(&log).expect("failure event is persisted before exit");
    let record: serde_json::Value =
        serde_json::from_str(records.trim()).expect("failure event is JSON");
    assert_eq!(record["host"], "local");
    assert_eq!(record["operation"], "init");
    assert_eq!(record["code"], "command-failed");
    assert!(
        !records.contains(SENSITIVE_CONFIG_PATH_LITERAL),
        "persistent events must not copy configuration-bearing diagnostics"
    );

    let lock = root
        .path()
        .join("cache")
        .join("muxe")
        .join("logs")
        .join("muxe.jsonl.lock");
    std::fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644))
        .expect("corrupt only the owned log lock mode");
    let logging_failure = run_init(root.path());
    assert!(
        !logging_failure.status.success(),
        "the command remains nonzero when failure logging fails"
    );
    let stderr = String::from_utf8_lossy(&logging_failure.stderr);
    assert!(
        stderr.contains(SENSITIVE_CONFIG_PATH_LITERAL),
        "the primary diagnostic remains visible when the sink fails"
    );
    assert!(
        stderr.contains(lock.to_string_lossy().as_ref()),
        "the sink diagnostic identifies the owned log lock path"
    );
    let records = std::fs::read_to_string(&log).expect("initial failure event remains persisted");
    assert!(
        !records.contains(SENSITIVE_CONFIG_PATH_LITERAL),
        "persistent events must not copy configuration-bearing diagnostics"
    );
}
