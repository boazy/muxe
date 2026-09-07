//! One-shot Zellij CLI fault injector for the release-owned live-host
//! fault case. Test-only fixture: never shipped, never referenced by
//! production code.
//!
//! The wrapper is transparent: every invocation execs the exact pinned
//! `zellij` binary with the received argv, except a single matched
//! final-session target reload. The match is the coordinator's exact
//! reload shape (`--session <final> action start-or-reload-plugin
//! <stable-url>`); the first match atomically consumes a one-shot marker,
//! notifies the runner through an owned barrier file, waits for the
//! runner's release file inside a bound, then execs the real CLI with the
//! stable URL swapped for a guaranteed-nonexistent fixture URL for that
//! one invocation. Rollback reloads arrive after consumption and pass
//! through unchanged. Exit statuses always come from the real host
//! process; this binary never synthesizes one.
//!
//! Fixture configuration travels via environment (the coordinator never
//! reads these variables): `MUXE_FI_REAL_ZELLIJ`, `MUXE_FI_FINAL_SESSION`,
//! `MUXE_FI_STABLE_URL`, `MUXE_FI_BARRIER_DIR`, `MUXE_FI_FAIL_URL`. The
//! wrapper enters through scoped PATH as an executable named `zellij`
//! (symlinked to this binary inside the owned root).

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

const BARRIER_POLL: Duration = Duration::from_millis(100);
const DEFAULT_BARRIER_TIMEOUT: Duration = Duration::from_mins(5);

/// Bound for the runner's release. Overridable for host-free fixture
/// tests via `MUXE_FI_BARRIER_TIMEOUT_SECS`; production-shaped runs use
/// the default. Invalid values fail closed.
fn barrier_timeout() -> Duration {
    match std::env::var("MUXE_FI_BARRIER_TIMEOUT_SECS") {
        Err(_) => DEFAULT_BARRIER_TIMEOUT,
        Ok(text) => {
            if let Ok(secs) = text.parse::<u64>() {
                Duration::from_secs(secs)
            } else {
                eprintln!("fault-injector: invalid MUXE_FI_BARRIER_TIMEOUT_SECS={text:?}");
                std::process::exit(2);
            }
        }
    }
}

/// Runs the real binary with `argv`, inheriting stdio, and exits with the
/// host's own status. Only the harness's own launch failures exit 2.
fn exec_real(real: &Path, argv: &[OsString]) -> ! {
    match Command::new(real)
        .args(argv)
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
    {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            eprintln!("fault-injector: cannot run {}: {error}", real.display());
            std::process::exit(2);
        }
    }
}

/// Matches exactly the coordinator reload shape for the final session:
/// `--session <final> action start-or-reload-plugin <stable-url>`,
/// anchored on the command name so leading config flags pass through.
fn is_final_reload(argv: &[OsString], final_session: &str, stable_url: &str) -> bool {
    let texts: Vec<String> = argv
        .iter()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let Some(reload_at) = texts
        .iter()
        .position(|item| item == "start-or-reload-plugin")
    else {
        return false;
    };
    if reload_at < 3 {
        return false;
    }
    texts[reload_at - 3] == "--session"
        && texts[reload_at - 2] == final_session
        && texts[reload_at - 1] == "action"
        && texts
            .get(reload_at + 1)
            .is_some_and(|url| url == stable_url)
}

fn required_path(name: &str) -> PathBuf {
    let value = std::env::var_os(name).unwrap_or_else(|| {
        eprintln!("fault-injector: {name} must be set");
        std::process::exit(2);
    });
    PathBuf::from(value)
}

fn required_str(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        eprintln!("fault-injector: {name} must be set");
        std::process::exit(2);
    })
}

fn main() {
    let argv: Vec<OsString> = std::env::args_os().skip(1).collect();
    // Forwarding needs the real binary. Matching needs the full fault
    // configuration: with only the binary present the wrapper is fully
    // transparent, and a partial fault configuration fails closed instead
    // of faulting the wrong session.
    let real = required_path("MUXE_FI_REAL_ZELLIJ");
    let configured = [
        "MUXE_FI_FINAL_SESSION",
        "MUXE_FI_STABLE_URL",
        "MUXE_FI_BARRIER_DIR",
        "MUXE_FI_FAIL_URL",
    ];
    let present = configured
        .iter()
        .filter(|name| std::env::var_os(name).is_some())
        .count();
    if present == 0 {
        exec_real(&real, &argv);
    }
    if present != configured.len() {
        eprintln!("fault-injector: partial fault configuration is refused");
        std::process::exit(2);
    }
    let final_session = required_str("MUXE_FI_FINAL_SESSION");
    let stable_url = required_str("MUXE_FI_STABLE_URL");
    let barrier_dir = required_path("MUXE_FI_BARRIER_DIR");
    let fail_url = required_str("MUXE_FI_FAIL_URL");

    if !is_final_reload(&argv, &final_session, &stable_url) {
        exec_real(&real, &argv);
    }

    // Atomic one-shot: only the first matched reload takes the fault path.
    // Rollback reloads find the marker and pass through unchanged.
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(barrier_dir.join("consumed"))
    {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            exec_real(&real, &argv);
        }
        Err(error) => {
            eprintln!("fault-injector: cannot claim the one-shot marker: {error}");
            std::process::exit(2);
        }
    }

    // Notify the runner, then hold for its release inside a bound. The
    // runner proves the earlier sessions healthy before releasing.
    if let Err(error) = std::fs::write(
        barrier_dir.join(format!("request-{final_session}")),
        b"reload-requested",
    ) {
        eprintln!("fault-injector: cannot write the barrier request: {error}");
        std::process::exit(2);
    }
    let release = barrier_dir.join(format!("release-{final_session}"));
    let deadline = Instant::now() + barrier_timeout();
    loop {
        if release.exists() {
            break;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "fault-injector: barrier release never arrived; failing loudly instead of hanging activation"
            );
            std::process::exit(2);
        }
        std::thread::sleep(BARRIER_POLL);
    }

    // Swap exactly the stable URL element for the nonexistent fixture URL
    // and run the real host. Its rejection is the fault.
    let mut fail_argv = argv;
    let mut swapped = 0;
    for element in &mut fail_argv {
        if element.to_string_lossy() == stable_url {
            *element = OsString::from(&fail_url);
            swapped += 1;
        }
    }
    if swapped != 1 {
        eprintln!("fault-injector: expected exactly one stable URL in argv, found {swapped}");
        std::process::exit(2);
    }
    exec_real(&real, &fail_argv);
}
