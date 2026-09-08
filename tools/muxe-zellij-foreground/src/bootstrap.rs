//! Test-only first-client bootstrap for the release-owned host runners.
//!
//! A bare foreground server starts with `session_data = None`; only
//! `ClientToServerMsg::FirstClientConnected` initializes the session
//! (pinned `zellij-server/src/lib.rs`, `FirstClientConnected` arm), while
//! ordinary `attach` sends `AttachClient` and unwraps `None`. A listening
//! socket is therefore NOT session readiness.
//!
//! This peer connects to the owned server socket, sends a real
//! `FirstClientConnected` built from the runner's actual config layout
//! inputs (explicit config file/dir, data dir, owned cwd, explicit
//! terminal size — the same `CliAssets` shape the pinned
//! `ClientInfo::New` path sends, minus the daemon spawn), and waits for
//! genuine initialized-session evidence: a non-empty
//! `ServerToClientMsg::Render` from the screen thread. It then closes
//! cleanly with `ClientExited` (the `RemoveClient` path, which keeps the
//! initialized session alive for the real PTY clients) and exits 0.
//! Mere send success or socket existence is never treated as proof.
//!
//! Typed inputs only: absolute socket/config/config-dir/data-dir/cwd
//! paths plus explicit geometry. There is no command hook and no shell.
//! The runner owns this process (bounded timeout, preserved diagnostics).
//!
//! This binary is a test fixture only. It is never shipped in a Muxe
//! release archive; core resolves it through an independent lockfile.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use zellij_utils::consts::ipc_connect;
use zellij_utils::input::cli_assets::{host_terminal_env, CliAssets};
use zellij_utils::input::options::Options;
use zellij_utils::ipc::{
    ClientToServerMsg, IpcReceiverWithContext, IpcSenderWithContext, ServerToClientMsg,
};
use zellij_utils::pane_size::Size;

const CONNECT_POLL: Duration = Duration::from_millis(50);

fn usage() -> ! {
    eprintln!(
        "usage: muxe-zellij-bootstrap --socket <abs-session-socket> \
         --config <abs-config-file> --config-dir <abs-config-dir> \
         --data-dir <abs-data-dir> --cwd <abs-dir> \
         [--rows <n>] [--cols <n>] [--timeout-secs <n>]"
    );
    std::process::exit(2);
}

fn required_abs(name: &str, value: Option<std::ffi::OsString>) -> PathBuf {
    let value = value.unwrap_or_else(|| usage());
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        eprintln!(
            "muxe-zellij-bootstrap requires an absolute {name} path, got {}",
            path.display()
        );
        std::process::exit(2);
    }
    path
}

fn optional_uint(name: &str, value: Option<std::ffi::OsString>) -> usize {
    let raw = value.unwrap_or_else(|| usage());
    let text = raw.to_string_lossy();
    text.parse::<usize>().unwrap_or_else(|_| {
        eprintln!("muxe-zellij-bootstrap requires a positive integer for {name}, got {text:?}");
        std::process::exit(2);
    })
}

struct BootstrapArgs {
    socket: PathBuf,
    config: PathBuf,
    config_dir: PathBuf,
    data_dir: PathBuf,
    cwd: PathBuf,
    rows: usize,
    cols: usize,
    timeout_secs: u64,
}

fn parse_args() -> BootstrapArgs {
    let mut socket: Option<std::ffi::OsString> = None;
    let mut config: Option<std::ffi::OsString> = None;
    let mut config_dir: Option<std::ffi::OsString> = None;
    let mut data_dir: Option<std::ffi::OsString> = None;
    let mut cwd: Option<std::ffi::OsString> = None;
    let mut rows: usize = 30;
    let mut cols: usize = 120;
    let mut timeout_secs: u64 = 60;

    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--socket" {
            if socket.is_some() {
                usage();
            }
            socket = Some(args.next().unwrap_or_else(|| usage()));
        } else if arg == "--config" {
            if config.is_some() {
                usage();
            }
            config = Some(args.next().unwrap_or_else(|| usage()));
        } else if arg == "--config-dir" {
            if config_dir.is_some() {
                usage();
            }
            config_dir = Some(args.next().unwrap_or_else(|| usage()));
        } else if arg == "--data-dir" {
            if data_dir.is_some() {
                usage();
            }
            data_dir = Some(args.next().unwrap_or_else(|| usage()));
        } else if arg == "--cwd" {
            if cwd.is_some() {
                usage();
            }
            cwd = Some(args.next().unwrap_or_else(|| usage()));
        } else if arg == "--rows" {
            rows = optional_uint("rows", Some(args.next().unwrap_or_else(|| usage())));
        } else if arg == "--cols" {
            cols = optional_uint("cols", Some(args.next().unwrap_or_else(|| usage())));
        } else if arg == "--timeout-secs" {
            timeout_secs = optional_timeout(args.next());
        } else {
            usage();
        }
    }
    if rows == 0 || cols == 0 || timeout_secs == 0 {
        eprintln!("muxe-zellij-bootstrap requires nonzero rows, cols, and timeout-secs");
        std::process::exit(2);
    }
    BootstrapArgs {
        socket: required_abs("--socket", socket),
        config: required_abs("--config", config),
        config_dir: required_abs("--config-dir", config_dir),
        data_dir: required_abs("--data-dir", data_dir),
        cwd: required_abs("--cwd", cwd),
        rows,
        cols,
        timeout_secs,
    }
}

fn optional_timeout(value: Option<std::ffi::OsString>) -> u64 {
    let raw = value.unwrap_or_else(|| usage());
    let text = raw.to_string_lossy();
    text.parse::<u64>().unwrap_or_else(|_| {
        eprintln!(
            "muxe-zellij-bootstrap requires a positive integer for timeout-secs, got {text:?}"
        );
        std::process::exit(2);
    })
}

fn main() {
    let args = parse_args();
    match run(args) {
        Ok(render_bytes) => {
            println!("bootstrapped session: first render evidence ({render_bytes} bytes)");
        }
        Err(error) => {
            eprintln!("muxe-zellij-bootstrap: {error}");
            std::process::exit(1);
        }
    }
}

/// Connects, sends the real first-client init, and awaits render evidence
/// inside the budget. Returns the render byte count as proof.
fn run(args: BootstrapArgs) -> Result<usize, String> {
    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);

    // Bounded connect: the server binds its socket on its listener thread
    // shortly after spawn; anything else fails closed inside the budget.
    let stream = loop {
        match ipc_connect(&args.socket) {
            Ok(stream) => break stream,
            Err(error) => {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "could not connect to {}: {error}",
                        args.socket.display()
                    ));
                }
                std::thread::sleep(CONNECT_POLL);
            }
        }
    };

    // The exact first-client shape the pinned `ClientInfo::New` path
    // sends, minus the daemon spawn: real config/layout resolution
    // happens server-side in `load_config_and_layout`.
    let host_env: BTreeMap<String, String> = host_terminal_env();
    let assets = CliAssets {
        config_file_path: Some(args.config),
        config_dir: Some(args.config_dir),
        should_ignore_config: false,
        configuration_options: Some(Options::default()),
        layout: None,
        terminal_window_size: Size {
            rows: args.rows,
            cols: args.cols,
        },
        data_dir: Some(args.data_dir),
        is_debug: false,
        max_panes: None,
        force_run_layout_commands: false,
        cwd: Some(args.cwd),
        host_terminal_env: host_env,
        initial_panes: None,
    };
    let mut sender: IpcSenderWithContext<ClientToServerMsg> = IpcSenderWithContext::new(stream);
    let receiver: IpcReceiverWithContext<ServerToClientMsg> = sender.get_receiver();
    sender
        .send_client_msg(ClientToServerMsg::FirstClientConnected {
            cli_assets: assets,
            is_web_client: false,
        })
        .map_err(|_| "could not send FirstClientConnected".to_owned())?;

    // Await genuine initialized-session evidence on a worker thread: the
    // screen thread renders only after tabs spawn. Send success alone
    // proves nothing. An empty render is not evidence; keep waiting.
    let (evidence_tx, evidence_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut receiver = receiver;
        loop {
            match receiver.try_recv_server_msg() {
                Ok((ServerToClientMsg::Render { content }, _)) => {
                    if content.is_empty() {
                        continue;
                    }
                    let _ = evidence_tx.send(Ok(content.len()));
                    return;
                }
                // The route thread unblocks the client input thread after
                // every handled instruction, including
                // `FirstClientConnected`, so this routinely precedes the
                // screen thread's first render: keep waiting for it.
                Ok((ServerToClientMsg::UnblockInputThread, _)) => continue,
                Ok((other, _)) => {
                    let _ =
                        evidence_tx.send(Err(format!("got {other:?} before any session render")));
                    return;
                }
                Err(_) => {
                    let _ = evidence_tx.send(Err(
                        "lost the server connection before any session render".to_owned(),
                    ));
                    return;
                }
            }
        }
    });
    let remaining = deadline.saturating_duration_since(Instant::now());
    match evidence_rx.recv_timeout(remaining) {
        Ok(Ok(render_bytes)) => {
            // Detach-style close: `ClientExited` routes to `RemoveClient`,
            // which keeps the initialized session alive for the real PTY
            // clients. (`ClientExit` on the last client would tear the
            // server loop down.)
            let _ = sender.send_client_msg(ClientToServerMsg::ClientExited);
            Ok(render_bytes)
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err("no session render inside the timeout".to_owned()),
    }
}

