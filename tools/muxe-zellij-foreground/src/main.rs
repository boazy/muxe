//! Test-only foreground Zellij server for the release-owned host runners.
//!
//! The pinned server unconditionally daemonizes on Unix
//! (`zellij-server::start_server`), so no distributed-binary argv yields a
//! retained host child. This entrypoint reproduces the daemon launch path
//! exactly — logger, config/cache folders, debug mode, real server OS
//! input — minus the daemonize call, then runs the exported
//! [`zellij_server::start_server_impl`] in the foreground with the same
//! panic-hook setting the daemon path uses. No host behavior is mocked,
//! the pin source is unpatched, and no flag is invented.
//!
//! Typed inputs only: an absolute session socket path plus an optional
//! debug switch. There is no command hook and no shell. The runner owns
//! this process as its foreground host child (SIGTERM-then-kill teardown)
//! and discovers sessions through the isolated socket directory.
//!
//! This binary is a test fixture only. It is never shipped in a Muxe
//! release archive; core resolves it through an independent lockfile.

use std::path::PathBuf;

use zellij_server::os_input_output::get_server_os_input;
use zellij_utils::consts::{create_config_and_cache_folders, DEBUG_MODE};
use zellij_utils::logging::configure_logger;

fn usage() -> ! {
    eprintln!("usage: muxe-zellij-foreground --socket <absolute-session-socket> [--debug]");
    std::process::exit(2);
}

fn main() {
    let mut socket: Option<PathBuf> = None;
    let mut debug = false;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--socket" {
            let value = args.next().unwrap_or_else(|| usage());
            if socket.is_some() {
                usage();
            }
            socket = Some(PathBuf::from(value));
        } else if arg == "--debug" {
            debug = true;
        } else {
            usage();
        }
    }
    let socket = socket.unwrap_or_else(|| usage());
    if !socket.is_absolute() {
        eprintln!(
            "muxe-zellij-foreground requires an absolute --socket path, got {}",
            socket.display()
        );
        std::process::exit(2);
    }

    // Identical setup to the daemon launch path in the pinned binary
    // (src/main.rs + src/commands.rs::start_server), minus daemonize.
    configure_logger();
    create_config_and_cache_folders();
    DEBUG_MODE.set(debug).unwrap();
    let os_input = match get_server_os_input() {
        Ok(os_input) => os_input,
        Err(error) => {
            eprintln!("failed to open terminal:\n{error}");
            std::process::exit(1);
        }
    };
    if let Some(parent) = socket.parent() {
        if let Err(error) = std::fs::create_dir_all(parent) {
            eprintln!(
                "muxe-zellij-foreground cannot create socket parent {}: {error}",
                parent.display()
            );
            std::process::exit(1);
        }
    }
    zellij_server::start_server_impl(Box::new(os_input), socket, true);
}
