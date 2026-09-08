//! Host-free smoke for the permission seeder: exact per-key grants land in
//! the pinned-default cache file, merges preserve other keys without
//! overgrant, and unknown inputs fail closed. No hosts, no sockets, no
//! approval: the seeder child runs under a TempDir-scoped environment and
//! the test discovers the resulting cache file beneath that owned root.

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant};

use zellij_utils::data::PermissionType;

const CASE_TIMEOUT: Duration = Duration::from_mins(1);
/// Deliberately small generic input sets, distinct from the production
/// bridge contract: this test proves seeder/cache semantics (exact grants,
/// merge preservation, no overgrant), never the production list. Production
/// coverage lives in the `crates/muxe` runner, which passes the shared
/// `muxe_zellij_protocol::BRIDGE_PERMISSIONS` constant itself; this fixture
/// crate keeps its separate lockfile and stays decoupled.
const FIRST_KEY: &str = "/tmp/owned-fixture/plugin-a.wasm";
const FIRST_PERMISSIONS: [&str; 2] = ["OpenFiles", "WriteToStdin"];
const SECOND_KEY: &str = "/other/plugin.wasm";
const SECOND_PERMISSIONS: [&str; 1] = ["RunCommands"];

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

fn run_seeder(root: &Path, argv: &[&str]) -> Output {
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
        if Instant::now() >= deadline {
            let kill_result = child.kill();
            match child.wait_with_output() {
                Ok(output) => panic!(
                    "seeder hung; kill result: {kill_result:?}; stdout: {:?}; stderr: {:?}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ),
                Err(wait_error) => panic!(
                    "seeder hung; kill result: {kill_result:?}; \
                     wait_with_output failed: {wait_error}",
                ),
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().expect("reap seeder")
}

/// Finds the pinned cache file beneath the owned root without depending on
/// platform-specific directory rules or seeder output wording.
fn cache_path(root: &Path) -> PathBuf {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("read owned cache root") {
            let entry = entry.expect("read owned cache entry");
            let path = entry.path();
            if path.file_name().is_some_and(|name| name == "permissions.kdl") {
                return path;
            }
            if entry.file_type().expect("inspect owned cache entry").is_dir() {
                pending.push(path);
            }
        }
    }
    panic!("seeder did not create permissions.kdl under owned root");
}

/// Canonical grant names in a stable order: the pinned cache round-trip
/// does not preserve insertion order, and only membership is contractual.
fn grant_names(grants: &[PermissionType]) -> Vec<String> {
    let mut names: Vec<String> = grants.iter().map(ToString::to_string).collect();
    names.sort_unstable();
    names
}

#[test]
fn seeds_exact_per_key_grants_in_scoped_cache() {
    use zellij_utils::input::permission::PermissionCache;

    let root = case_root("seed");
    let mut argv = vec!["--plugin", FIRST_KEY];
    for permission in FIRST_PERMISSIONS {
        argv.push("--permission");
        argv.push(permission);
    }
    let output = run_seeder(&root, &argv);
    assert!(
        output.status.success(),
        "seeder failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let path = cache_path(&root);
    assert!(
        path.starts_with(&root),
        "seeder cache escaped the owned root: {path:?}",
    );
    let cache = PermissionCache::from_path_or_default(Some(path.clone()));
    assert_eq!(
        grant_names(cache.get_permissions(FIRST_KEY.to_owned()).expect("first grant stored")),
        vec!["OpenFiles", "WriteToStdin"],
        "first key holds no exact grant",
    );
    // Merge: a second seed for another plugin preserves ours exactly.
    let mut argv = vec!["--plugin", SECOND_KEY];
    for permission in SECOND_PERMISSIONS {
        argv.push("--permission");
        argv.push(permission);
    }
    let output = run_seeder(&root, &argv);
    assert!(
        output.status.success(),
        "merge seed failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    let cache = PermissionCache::from_path_or_default(Some(path));
    assert_eq!(
        grant_names(cache.get_permissions(SECOND_KEY.to_owned()).expect("other grant stored")),
        vec!["RunCommands"],
        "merge dropped the other grant",
    );
    assert_eq!(
        grant_names(cache.get_permissions(FIRST_KEY.to_owned()).expect("first grant kept")),
        vec!["OpenFiles", "WriteToStdin"],
        "merge altered our grant",
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn rejects_unknown_permission_and_missing_inputs() {
    let root = case_root("reject");
    let denied = run_seeder(&root, &["--plugin", FIRST_KEY, "--permission", "Bogus"]);
    assert_eq!(
        denied.status.code(),
        Some(2),
        "unknown permission seeder failure:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&denied.stdout),
        String::from_utf8_lossy(&denied.stderr),
    );
    let missing = run_seeder(&root, &["--permission", FIRST_PERMISSIONS[0]]);
    assert_eq!(
        missing.status.code(),
        Some(2),
        "missing plugin seeder failure:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&missing.stdout),
        String::from_utf8_lossy(&missing.stderr),
    );
    let bare = run_seeder(&root, &[]);
    assert_eq!(
        bare.status.code(),
        Some(2),
        "empty invocation seeder failure:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&bare.stdout),
        String::from_utf8_lossy(&bare.stderr),
    );
    std::fs::remove_dir_all(&root).ok();
}
