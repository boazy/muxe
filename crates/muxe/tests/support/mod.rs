//! Owned host-child primitives for the release-owned host runners.
//!
//! Every host server (Herdr, Zellij) and broker child in the smoke and
//! upgrade runners is spawned through [`OwnedChild`]: an absolute binary
//! path, a TempDir-scoped environment, piped diagnostics with a bounded
//! tail, and an explicit endpoint. Teardown is reverse-order with
//! SIGTERM-then-kill escalation inside bounded timeouts, and diagnostics
//! are preserved on every failure path. Nothing here discovers or signals
//! arbitrary processes, touches a default user socket, or passes green
//! without a live handshake.

mod scoped_env;
pub use scoped_env::{apply_scoped_env, ensure_scoped_dirs, scoped_env_vec};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt as _;

use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const FORCEFUL_REAP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const MAX_UNIX_SOCKET_PATH: usize = 103;
/// Bound on one owned Zellij CLI child (`kill-session`, `list-clients`):
/// a hung CLI must never strand the retained server children it precedes
/// in teardown.
pub const CLI_TIMEOUT: Duration = Duration::from_secs(30);

/// Short owned scratch root under `/tmp` (never ambient `TMPDIR`): keeps
/// derived unix-socket paths inside `MAX_UNIX_SOCKET_PATH` with a stable
/// prefix per caller.
pub fn short_tempdir(prefix: &str) -> io::Result<tempfile::TempDir> {
    tempfile::Builder::new().prefix(prefix).tempdir_in("/tmp")
}

/// Runs one owned CLI command to completion inside `CLI_TIMEOUT` and
/// returns its output. The child is a retained `OwnedChild`, so expiry
/// kills with escalation in `terminate_and_reap` and fails closed with
/// preserved diagnostics; drop-time `start_kill` covers the narrow abort
/// window between spawn and the timeout arm. Pipe tails are bounded, so
/// only small CLI outputs (status tables, not transcripts) belong here.
pub async fn run_cli_bounded(tag: &str, command: &mut Command) -> io::Result<std::process::Output> {
    let mut child = OwnedChild::spawn(tag, command)?;
    let outcome = tokio::time::timeout(CLI_TIMEOUT, child.wait()).await;
    match outcome {
        Err(_) => {
            let diagnostics = child.terminate_and_reap().await?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "owned CLI '{tag}' exceeded CLI_TIMEOUT:\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    diagnostics.stdout_tail.lossy(),
                    diagnostics.stderr_tail.lossy(),
                ),
            ));
        }
        Ok(Err(error)) => {
            let diagnostics = child.terminate_and_reap().await?;
            return Err(io::Error::other(format!(
                "owned CLI '{tag}' wait failed: {error}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                diagnostics.stdout_tail.lossy(),
                diagnostics.stderr_tail.lossy(),
            )));
        }
        Ok(Ok(_)) => {}
    }
    let diagnostics = child.terminate_and_reap().await?;
    let status = diagnostics.exit_status.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("owned CLI '{tag}' reaped with no exit status"),
        )
    })?;
    Ok(std::process::Output {
        status,
        stdout: diagnostics.stdout_tail.bytes,
        stderr: diagnostics.stderr_tail.bytes,
    })
}

/// Runs the real public `muxe integration install zellij` against the
/// owned host config under the scoped environment, so foreground startup
/// reads managed autoload nodes and activation preflight finds
/// receipt-owned stable bytes. Runs BEFORE the first foreground server
/// starts and BEFORE the permission grant (the bridge loads at startup).
/// Fixed production argv (`--always-configure` with the explicit owned
/// config); the real stdout report returns as evidence. Nothing is
/// fabricated: receipt, stable bytes, and KDL nodes are all written by
/// the installed binary itself.
pub async fn install_zellij_integration(
    tag: &str,
    muxe_binary: &Path,
    host: &OwnedZellijHost,
    scoped_root: &Path,
) -> io::Result<String> {
    let mut command = Command::new(muxe_binary);
    command
        .arg("integration")
        .arg("install")
        .arg("zellij")
        .arg("--always-configure")
        .arg("--zellij-config")
        .arg(host.config_file());
    host.apply_host_scoped_env(&mut command, scoped_root);
    command.current_dir(scoped_root);
    let output = run_cli_bounded(&format!("{tag}-integration-install"), &mut command).await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "{tag}: public integration install failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )));
    }
    let report = String::from_utf8_lossy(&output.stdout).into_owned();
    eprintln!("[{tag}] integration install report:\n{report}");
    Ok(report)
}

/// Startup handshake budget for one owned host server.
pub const STARTUP_TIMEOUT: Duration = Duration::from_mins(1);
/// Bound on one bootstrap peer lifetime: connect, first-client init, and
/// first render evidence, then a detach-style close.
pub const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(90);
/// Explicit PTY geometry for bootstrap and interactive clients. `script(1)`
/// can inherit a zero/nonterminal size; every owned terminal sets its own
/// size inside its own PTY and never mutates the parent terminal.
pub const BOOTSTRAP_ROWS: usize = 30;
pub const BOOTSTRAP_COLS: usize = 120;
// Authoritative bridge permission contract: the seeder grant must match
// exactly what the WASM bridge requests. The runner passes each entry's
// canonical `ToString` variant name as one `--permission` argv to the
// fixture seeder, which parses it with the pinned `PermissionType`;
// permission names are never retyped here.
use muxe_zellij_protocol::BRIDGE_PERMISSIONS;

/// Pinned session-socket contract directory: `<socket-dir>/contract_version_1/<session>`.
/// Derived from `CLIENT_SERVER_CONTRACT_DIR` in the pinned zellij-utils
/// consts at the workspace-pinned revision; the socket scanner stays
/// version-agnostic, but the bind path for a new server must name it.
pub const ZELLIJ_CONTRACT_DIR: &str = "contract_version_1";

/// Runs `<binary> init` under the scoped environment and returns the
/// shared application paths every serve child and every activate must
/// use: the init-written config file plus the resolved cache directory.
/// One shared config/cache keeps broker registrations visible to
/// activation; hosts separate by registry identity only. Matches
/// `paths::resolve` (`$XDG_*/muxe`) under the fixture environment.
pub async fn init_shared_dirs(binary: &Path, scoped_root: &Path) -> io::Result<(PathBuf, PathBuf)> {
    ensure_scoped_dirs(scoped_root)?;
    let config_file = scoped_root.join("config").join("muxe").join("config.yml");
    let cache_dir = scoped_root.join("cache").join("muxe");
    let mut command = Command::new(binary);
    command.arg("init");
    apply_scoped_env(&mut command, scoped_root);
    command.current_dir(scoped_root);
    let output = command.output().await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "muxe init failed under the scoped environment:\n--- stdout ---\n{}\n--- stderr ---\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )));
    }
    if !config_file.is_file() {
        return Err(io::Error::other(format!(
            "muxe init reported success but wrote no config at {}",
            config_file.display()
        )));
    }
    std::fs::create_dir_all(&cache_dir)?;
    Ok((config_file, cache_dir))
}

/// Bounded tail of one captured diagnostics pipe.
#[derive(Clone, Debug, Default)]
pub struct PipeTail {
    pub bytes: Vec<u8>,
}

impl PipeTail {
    fn drain<R>(pipe: R) -> JoinHandle<Vec<u8>>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            use tokio::io::AsyncReadExt as _;
            let mut bytes = Vec::new();
            let mut pipe = pipe;
            let _ = pipe.read_to_end(&mut bytes).await;
            if bytes.len() > MAX_DIAGNOSTIC_BYTES {
                bytes = bytes[bytes.len() - MAX_DIAGNOSTIC_BYTES..].to_vec();
            }
            bytes
        })
    }

    /// Best-effort lossy rendering for failure reports.
    #[must_use]
    pub fn lossy(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

/// Emits bounded tails from the owned native audit log and pinned Zellij host
/// log before the caller drops its `TempDir`. Missing logs are normal; symlinks,
/// non-regular files, and paths resolving outside the supplied owned roots are
/// ignored.
pub fn emit_owned_host_log_tails(context: &str, cache_dir: &Path, scoped_tmp: &Path) {
    let mut emitted = false;
    let native_log = cache_dir.join("logs").join("muxe.jsonl");
    if let Some(tail) = read_owned_log_tail(&native_log, cache_dir) {
        emitted = true;
        eprintln!(
            "[{context}] --- native audit log tail ({}) ---\n{tail}",
            native_log.display()
        );
    }

    if let Ok(entries) = std::fs::read_dir(scoped_tmp) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() || file_type.is_symlink() {
                continue;
            }
            let name = entry.file_name();
            if !name.to_string_lossy().starts_with("zellij-") {
                continue;
            }
            let zellij_log = path.join("zellij-log").join("zellij.log");
            if let Some(tail) = read_owned_log_tail(&zellij_log, scoped_tmp) {
                emitted = true;
                eprintln!(
                    "[{context}] --- Zellij host log tail ({}) ---\n{tail}",
                    zellij_log.display()
                );
            }
        }
    }

    if !emitted {
        eprintln!(
            "[{context}] owned native/Zellij log tails: no readable logs under {} and {}",
            cache_dir.display(),
            scoped_tmp.display()
        );
    }
}

fn read_owned_log_tail(path: &Path, root: &Path) -> Option<String> {
    let root_metadata = std::fs::symlink_metadata(root).ok()?;
    if root_metadata.file_type().is_symlink() {
        return None;
    }
    let relative = path.strip_prefix(root).ok()?;
    let mut current = root.to_owned();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = std::fs::symlink_metadata(&current).ok()?;
        if metadata.file_type().is_symlink() {
            return None;
        }
    }
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if !metadata.file_type().is_file() {
        return None;
    }
    let canonical_root = std::fs::canonicalize(root).ok()?;
    let canonical_path = std::fs::canonicalize(path).ok()?;
    if !canonical_path.starts_with(&canonical_root) {
        return None;
    }
    let mut file = std::fs::File::open(path).ok()?;
    let max_bytes = u64::try_from(MAX_DIAGNOSTIC_BYTES).ok()?;
    let max_offset = i64::try_from(MAX_DIAGNOSTIC_BYTES).ok()?;
    let length = file.metadata().ok()?.len();
    if length > max_bytes {
        file.seek(SeekFrom::End(-max_offset)).ok()?;
    }
    let mut bytes = Vec::new();
    file.take(max_bytes).read_to_end(&mut bytes).ok()?;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// Post-reap evidence for one owned child.
#[derive(Debug)]
pub struct ChildDiagnostics {
    pub tag: String,
    pub stdout_tail: PipeTail,
    pub stderr_tail: PipeTail,
    pub exit_status: Option<std::process::ExitStatus>,
}

/// One retained owned child: piped diagnostics, explicit teardown, no global
/// cleanup. Only the handle created by [`OwnedChild::spawn`] is ever
/// signalled or reaped.
pub struct OwnedChild {
    tag: String,
    child: Option<Child>,
    stdout: Option<JoinHandle<Vec<u8>>>,
    stderr: Option<JoinHandle<Vec<u8>>>,
}

impl OwnedChild {
    /// Spawns `command` with piped diagnostics and retains the handle.
    /// The caller configures argv, env, and stdio before handing over;
    /// stdout/stderr are always piped here regardless of prior settings.
    pub fn spawn(tag: &str, command: &mut Command) -> io::Result<Self> {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().map(PipeTail::drain);
        let stderr = child.stderr.take().map(PipeTail::drain);
        Ok(Self {
            tag: tag.to_owned(),
            child: Some(child),
            stdout,
            stderr,
        })
    }

    /// Whether the child handle is still retained.
    #[must_use]
    pub fn is_retained(&self) -> bool {
        self.child.is_some()
    }

    /// Non-reaping liveness probe: `Ok(None)` while running. The handle
    /// stays retained; use [`OwnedChild::terminate_and_reap`] to reap.
    pub fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        match self.child.as_mut() {
            Some(child) => Ok(child.try_wait()?),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("owned child '{}' is already reaped", self.tag),
            )),
        }
    }

    /// Awaits exit without reaping diagnostics: use
    /// [`OwnedChild::terminate_and_reap`] afterwards to collect them.
    pub async fn wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        match self.child.as_mut() {
            Some(child) => Ok(child.wait().await.map(Some)?),
            None => Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("owned child '{}' is already reaped", self.tag),
            )),
        }
    }

    /// Graceful SIGTERM (unix) then forceful kill, bounded waits, awaited
    /// reap, and preserved diagnostics. Consumes the handle so a child is
    /// never reaped twice.
    pub async fn terminate_and_reap(&mut self) -> io::Result<ChildDiagnostics> {
        let child = self.child.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("owned child '{}' is already reaped", self.tag),
            )
        })?;
        if child.try_wait()?.is_none() {
            #[cfg(unix)]
            {
                let pid = child.id().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("owned child '{}' no longer has a process id", self.tag),
                    )
                })?;
                let pid = i32::try_from(pid).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("owned child '{}' process id exceeds i32", self.tag),
                    )
                })?;
                match nix::sys::signal::kill(
                    nix::unistd::Pid::from_raw(pid),
                    nix::sys::signal::Signal::SIGTERM,
                ) {
                    Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
                    Err(error) => {
                        return Err(io::Error::other(format!(
                            "could not send SIGTERM to owned child '{}': {error}",
                            self.tag
                        )));
                    }
                }
            }
            #[cfg(not(unix))]
            child.start_kill()?;
            if let Ok(result) = tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, child.wait()).await
            {
                result?;
            } else {
                child.start_kill()?;
                tokio::time::timeout(FORCEFUL_REAP_TIMEOUT, child.wait())
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "owned child '{}' survived SIGKILL past the reap budget",
                                self.tag
                            ),
                        )
                    })??;
            }
        }
        let exit_status = child.try_wait()?;
        self.child = None;
        let stdout_tail = PipeTail {
            bytes: collect_tail(self.stdout.take()).await,
        };
        let stderr_tail = PipeTail {
            bytes: collect_tail(self.stderr.take()).await,
        };
        Ok(ChildDiagnostics {
            tag: std::mem::take(&mut self.tag),
            stdout_tail,
            stderr_tail,
            exit_status,
        })
    }
}

impl Drop for OwnedChild {
    /// Best-effort signal only: [`OwnedChild::terminate_and_reap`] must run
    /// first so exits are observed and diagnostics preserved. A Drop that
    /// fired is a caller bug the message names; it never blocks.
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.start_kill();
        }
    }
}

/// Awaits one diagnostics drain with a bounded wait: a daemonized
/// grandchild inheriting the pipe can never stall the reap.
async fn collect_tail(task: Option<JoinHandle<Vec<u8>>>) -> Vec<u8> {
    match task {
        Some(task) => tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .ok()
            .and_then(std::result::Result::ok)
            .unwrap_or_default(),
        None => Vec::new(),
    }
}

/// Reads a required typed absolute-path input. Missing or relative inputs
/// fail closed naming the variable. There is no command hook anywhere in
/// these runners: only installation directories, host binaries, and sockets
/// cross this boundary.
pub fn input_path(name: &str) -> PathBuf {
    let value = std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"));
    let path = PathBuf::from(&value);
    assert!(
        path.is_absolute(),
        "{name} must be an absolute path, got {value:?}"
    );
    path
}

/// Locates the `muxe` executable inside an installation directory:
/// canonical root layout only (`<dir>/muxe` with `<dir>/lib/...`
/// alongside, DESIGN 1826). Native resolves the packaged bridge from the
/// executable's own parent, so a `bin/`-nested binary would misresolve;
/// there is no fallback. Anything else fails closed naming the layout.
pub fn installation_binary(dir: &Path) -> PathBuf {
    assert!(
        dir.is_dir(),
        "installation is not a directory: {}",
        dir.display()
    );
    let candidate = dir.join("muxe");
    assert!(
        candidate.is_file(),
        "installation has no canonical root muxe executable at {} (DESIGN 1826: <root>/muxe with <root>/lib/... alongside; no bin/ fallback)",
        candidate.display()
    );
    candidate
}

/// Validates one installed executable: regular file, owner-executable, and
/// answers `compatibility --json` with a versioned record. Returns the
/// binary path for direct execution with fixed argv.
pub async fn validate_installation(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt as _;
    let binary = installation_binary(dir);
    let metadata = std::fs::symlink_metadata(&binary).expect("stat installed muxe");
    assert!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "installed muxe is not a regular file: {}",
        binary.display()
    );
    assert!(
        metadata.permissions().mode() & 0o100 != 0,
        "installed muxe is not executable: {}",
        binary.display()
    );
    // Probes run before any case TempDir exists: hold an owned TempDir
    // for cwd so the child never inherits repo/user cwd.
    let probe_root = short_tempdir("muxe-live-probe-").expect("owned probe TempDir");
    let output = Command::new(&binary)
        .arg("compatibility")
        .arg("--json")
        .current_dir(probe_root.path())
        .output()
        .await
        .expect("run installed muxe compatibility");
    assert!(
        output.status.success(),
        "installed muxe compatibility failed: {}",
        binary.display()
    );
    let record: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("installed muxe prints JSON compatibility");
    assert!(
        record
            .get("muxe_version")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|version| !version.is_empty()),
        "installed muxe reports no muxe_version: {}",
        binary.display()
    );
    binary
}

/// One owned Herdr server child: `herdr server` under a TempDir-scoped
/// environment with an explicit socket, proved live by a ping handshake.
/// Mirrors the reviewed adapter fixture pattern; the server is never shared
/// and never outlives its owner.
pub struct OwnedHerdrServer {
    child: OwnedChild,
    socket: PathBuf,
    discovery_key: String,
}

impl OwnedHerdrServer {
    /// Starts exactly one Herdr server from an absolute binary path. The
    /// server lives under `root/{name}` and nowhere else.
    pub async fn start(herdr_binary: &Path, root: &Path, name: &str) -> io::Result<Self> {
        if !herdr_binary.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "owned Herdr server requires an absolute binary path, got {}",
                    herdr_binary.display()
                ),
            ));
        }
        let home = root.join(format!("{name}-herdr-home"));
        let config_home = root.join(format!("{name}-herdr-config"));
        let cache_home = root.join(format!("{name}-herdr-cache"));
        let tmp = root.join(format!("{name}-herdr-tmp"));
        std::fs::create_dir_all(&home)?;
        std::fs::create_dir_all(&config_home)?;
        std::fs::create_dir_all(&cache_home)?;
        std::fs::create_dir_all(&tmp)?;
        let socket = root.join(format!("{name}-herdr.sock"));
        #[cfg(unix)]
        if socket.as_os_str().len() > MAX_UNIX_SOCKET_PATH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("owned Herdr socket path is too long: {}", socket.display()),
            ));
        }
        let mut command = Command::new(herdr_binary);
        command
            .arg("server")
            .env_clear()
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_CACHE_HOME", &cache_home)
            .env("TMPDIR", &tmp)
            .env("HERDR_SOCKET_PATH", &socket);
        command.current_dir(root);
        let mut child = OwnedChild::spawn(&format!("{name}-herdr-server"), &mut command)?;
        let outcome = wait_for_herdr_handshake(&socket).await;
        if let Err(error) = outcome {
            let diagnostics = child.terminate_and_reap().await?;
            return Err(io::Error::other(format!(
                "owned Herdr server '{name}' never proved live at {}: {error}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                socket.display(),
                diagnostics.stdout_tail.lossy(),
                diagnostics.stderr_tail.lossy(),
            )));
        }
        let discovery_key = outcome.expect("handshake succeeded");
        Ok(Self {
            child,
            socket,
            discovery_key,
        })
    }

    /// Explicit owned socket path.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Discovery key proved by the readiness handshake, never caller-supplied.
    #[must_use]
    pub fn discovery_key(&self) -> &str {
        &self.discovery_key
    }

    /// Non-reaping liveness probe of the retained server child: an exited
    /// child (including a zombie) reports here, never as alive.
    pub fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    /// Terminates and reaps only the server child created by [`Self::start`].
    pub async fn shutdown(&mut self) -> io::Result<ChildDiagnostics> {
        self.child.terminate_and_reap().await
    }
}

/// Polls for the socket file, then proves liveness with a ping handshake
/// inside the startup budget. Returns the live discovery key.
async fn wait_for_herdr_handshake(socket: &Path) -> io::Result<String> {
    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    loop {
        if socket.exists() {
            let client = muxe_adapter_herdr::HerdrSocketClient::new(socket);
            if let Ok(identity) = muxe_adapter_herdr::probe_live_identity(&client).await {
                if identity.discovery_key.is_empty() || identity.live_server_id.is_empty() {
                    // Socket answers but the server is not fully up yet.
                } else {
                    return Ok(identity.discovery_key);
                }
                // Socket file exists before the server accepts; keep polling.
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "no live Herdr handshake at {} inside the startup budget",
                    socket.display()
                ),
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// One owned Zellij host: isolated environment plus named sessions.
///
/// Isolation (scoped `HOME`/`XDG`/`TMPDIR`/`ZELLIJ_SOCKET_DIR`, explicit config and
/// data dirs, session lifecycle commands, socket-directory discovery) is
/// verified from the owned contract probe
/// (`.local/zellij-contract-probe/run-host.sh`); only ambient-user-state
/// avoidance comes from there. The probe script itself is NOT copied for
/// server ownership: it also runs `attach --create-background`, whose
/// launcher daemonizes the server.
///
/// Server-process ownership is deliberately NOT claimed here. The pinned
/// server unconditionally daemonizes on Unix, so no distributed-binary argv
/// (including `--server` alone) yields a retained host child, and no
/// foreground flag is invented. The feasible fixture is a test-only
/// foreground entrypoint linked against the exact pinned zellij-server
/// calling its exported `start_server_impl` with the same real OS
/// input/debug setup (core coordinates build and pin; no pin-source patch,
/// no shipped helper). Until that entrypoint exists this type owns the
/// environment, session bookkeeping, and teardown sequencing, and every
/// runner fails closed via [`OwnedZellijHost::has_server_child`] rather
/// than pretending a launcher owns the server.
pub struct OwnedZellijHost {
    zellij_binary: PathBuf,
    root: PathBuf,
    workdir: PathBuf,
    socket_dir: PathBuf,
    config_file: PathBuf,
    config_dir: PathBuf,
    data_dir: PathBuf,
    servers: Vec<OwnedChild>,
    sessions: Vec<String>,
}

impl OwnedZellijHost {
    /// Prepares the isolated environment under `root/{name}` without
    /// starting any process. Pair with the core-coordinated foreground
    /// server entrypoint once it lands; see the type-level docs.
    pub fn prepare(zellij_binary: &Path, root: &Path, name: &str) -> io::Result<Self> {
        if !zellij_binary.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "owned Zellij host requires an absolute binary path, got {}",
                    zellij_binary.display()
                ),
            ));
        }
        // Canonicalize once: TempDirs may live under symlinked parents
        // (`/var` vs `/private/var` on macOS) while child `pwd` reports
        // the physical path. Physical roots keep every derived socket,
        // env, and cwd comparison exact.
        let root = std::fs::canonicalize(root)?;
        let base = root.join(format!("{name}-zellij"));
        let home = base.join("home");
        let cache = base.join("cache");
        let tmp = base.join("tmp");
        let data = base.join("data");
        let config_dir = base.join("config");
        let socket_dir = base.join("sockets");
        let workdir = base.join("work");
        for dir in [
            &home,
            &cache,
            &tmp,
            &data,
            &config_dir,
            &socket_dir,
            &workdir,
        ] {
            std::fs::create_dir_all(dir)?;
        }
        // Intentionally empty config: the runner supplies an explicit
        // one-pane layout and uses no user configuration. The managed
        // bridge is NOT smuggled through this config: it enters live
        // sessions through the real public path, the coordinator's
        // per-session `start-or-reload-plugin <stable-url>` reload during
        // `muxe activate` (see the fault-injector reload shape). An empty
        // config plus digest metadata alone would never count as bridge
        // proof; only live bridge registrations do.
        let config_file = config_dir.join("config.kdl");
        std::fs::write(
            &config_file,
            "// Owned runner config: explicit layout only, no user config.\n",
        )?;
        Ok(Self {
            zellij_binary: zellij_binary.to_owned(),
            root: base,
            workdir,
            socket_dir,
            config_file,
            config_dir,
            data_dir: data,
            servers: Vec::new(),
            sessions: Vec::new(),
        })
    }

    /// Spawns the core-coordinated test-only foreground entrypoint against
    /// this host's session socket path, initializes the session with a
    /// real first-client bootstrap, and tracks the session. The entrypoint
    /// runs the exact pinned server (`start_server_impl`, no mocks, no
    /// pin-source patch); the returned child is the retained foreground
    /// server process. A bare server starts with `session_data = None`,
    /// so the socket alone is never readiness: the owned bootstrap peer
    /// sends `FirstClientConnected` and only a real server render counts
    /// as initialized-session evidence, observed before any PTY client
    /// attaches. Any failure reaps the server child and reports both
    /// diagnostics.
    pub async fn serve_foreground(
        &mut self,
        helper: &Path,
        bootstrap: &Path,
        session: &str,
        scoped_root: &Path,
    ) -> io::Result<PathBuf> {
        for (name, path) in [
            ("foreground entrypoint", helper),
            ("bootstrap peer", bootstrap),
        ] {
            if !path.is_absolute() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{name} requires an absolute path, got {}", path.display()),
                ));
            }
            if !path.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "{name} is not a file (core checkpoint pending?): {}",
                        path.display()
                    ),
                ));
            }
        }
        let mut child = self.spawn_foreground_server(helper, session, scoped_root)?;
        let outcome: io::Result<PathBuf> = async {
            // The socket proves the listener bound; only the bootstrap
            // render proves the session initialized.
            self.wait_for_session(session).await?;
            self.run_bootstrap(bootstrap, session, scoped_root).await?;
            // The bootstrap closes detach-style, so the server must still
            // be alive with the session retained for the real clients.
            // (`child` is not yet attached; the caller asserts the full
            // set through `has_server_child`/`check_servers_alive`.)
            if child.try_wait()?.is_some() {
                return Err(io::Error::other(format!(
                    "owned Zellij server for session '{session}' exited during bootstrap"
                )));
            }
            self.sessions.push(session.to_owned());
            Ok(self.session_socket_path(session))
        }
        .await;
        match outcome {
            Ok(socket) => {
                self.attach_server_child(child);
                Ok(socket)
            }
            Err(error) => {
                let diagnostics = child.terminate_and_reap().await?;
                Err(io::Error::other(format!(
                    "foreground session '{session}' failed to initialize: {error}\n--- server stdout ---\n{}\n--- server stderr ---\n{}",
                    diagnostics.stdout_tail.lossy(),
                    diagnostics.stderr_tail.lossy(),
                )))
            }
        }
    }

    /// Builds the foreground server command: the helper plus the session
    /// socket under the complete scoped environment (muxe scoping overlaid
    /// with the host session identity) and the owned workdir as cwd. The
    /// helper's `configure_logger`/`create_config_and_cache_folders` calls
    /// therefore land in owned `TempDir` paths, never default user dirs, and
    /// the child never inherits repo/user cwd.
    fn foreground_command(&self, helper: &Path, session: &str, scoped_root: &Path) -> Command {
        let socket = self.session_socket_path(session);
        let mut command = Command::new(helper);
        command.arg("--socket").arg(&socket);
        self.apply_host_scoped_env(&mut command, scoped_root);
        command.current_dir(&self.workdir);
        command
    }

    /// Spawns the foreground server child without waiting: the exact
    /// production spawn path, factored so host-free regression spawns a
    /// real child and observes its real environment.
    fn spawn_foreground_server(
        &self,
        helper: &Path,
        session: &str,
        scoped_root: &Path,
    ) -> io::Result<OwnedChild> {
        let mut command = self.foreground_command(helper, session, scoped_root);
        OwnedChild::spawn(&format!("zellij-server-{session}"), &mut command)
    }

    /// Builds the bootstrap peer command: typed absolute inputs plus
    /// explicit geometry, under the same scoped environment and owned
    /// cwd as the server it initializes.
    fn bootstrap_command(&self, bootstrap: &Path, session: &str, scoped_root: &Path) -> Command {
        let mut command = Command::new(bootstrap);
        command
            .arg("--socket")
            .arg(self.session_socket_path(session))
            .arg("--config")
            .arg(&self.config_file)
            .arg("--config-dir")
            .arg(&self.config_dir)
            .arg("--data-dir")
            .arg(&self.data_dir)
            .arg("--cwd")
            .arg(&self.workdir)
            .arg("--rows")
            .arg(BOOTSTRAP_ROWS.to_string())
            .arg("--cols")
            .arg(BOOTSTRAP_COLS.to_string());
        self.apply_host_scoped_env(&mut command, scoped_root);
        command.env("TERM", "xterm-256color");
        command.current_dir(&self.workdir);
        command
    }

    /// Runs the owned bootstrap peer to completion inside a bound and
    /// asserts initialized-session evidence: a clean exit with a nonempty
    /// render report on stdout. Expiry kills with escalation and fails
    /// closed with both captured streams; a live server with no session
    /// can never pass.
    async fn run_bootstrap(
        &self,
        bootstrap: &Path,
        session: &str,
        scoped_root: &Path,
    ) -> io::Result<()> {
        let mut command = self.bootstrap_command(bootstrap, session, scoped_root);
        let mut child = OwnedChild::spawn(&format!("{session}-bootstrap"), &mut command)?;
        let outcome = tokio::time::timeout(BOOTSTRAP_TIMEOUT, child.wait()).await;
        match outcome {
            Err(_) => {
                let diagnostics = child.terminate_and_reap().await?;
                return Err(io::Error::other(format!(
                    "bootstrap peer for session '{session}' exceeded the bounded lifetime:\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    diagnostics.stdout_tail.lossy(),
                    diagnostics.stderr_tail.lossy(),
                )));
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(_)) => {}
        }
        let diagnostics = child.terminate_and_reap().await?;
        let clean = diagnostics
            .exit_status
            .is_some_and(|status| status.success());
        let report = diagnostics.stdout_tail.lossy();
        if !clean || report.trim().is_empty() {
            return Err(io::Error::other(format!(
                "bootstrap peer for session '{session}' proved no initialized session (status {:?}):\n--- stdout ---\n{report}\n--- stderr ---\n{}",
                diagnostics.exit_status,
                diagnostics.stderr_tail.lossy(),
            )));
        }
        eprintln!("[bootstrap] session '{session}': {report}");
        Ok(())
    }

    /// Builds the permission seeder command: the fixture peer plus the
    /// exact managed-bridge location string and permission names, under
    /// the merged scoped environment with the owned scoped root as cwd.
    /// The seeder resolves the pinned-default cache path itself, so the
    /// grant lands exactly where the owned server reads it.
    fn permission_seed_command(
        seeder: &Path,
        plugin_location: &str,
        scoped_root: &Path,
    ) -> Command {
        let mut command = Command::new(seeder);
        command.arg("--plugin").arg(plugin_location);
        for permission in BRIDGE_PERMISSIONS {
            command.arg("--permission").arg(permission.to_string());
        }
        apply_scoped_env(&mut command, scoped_root);
        command.current_dir(scoped_root);
        command
    }

    /// Runs the owned permission seeder to completion inside a bound and
    /// asserts the grant report names the plugin location. The seeder
    /// merges into the pinned cache (never ambient state) with the
    /// pinned code's own path and format; any other outcome fails
    /// closed with both captured streams.
    pub async fn run_permission_seed(
        &self,
        seeder: &Path,
        plugin_location: &str,
        scoped_root: &Path,
    ) -> io::Result<()> {
        let mut command = Self::permission_seed_command(seeder, plugin_location, scoped_root);
        let mut child = OwnedChild::spawn("bridge-permission-seed", &mut command)?;
        let outcome = tokio::time::timeout(BOOTSTRAP_TIMEOUT, child.wait()).await;
        match outcome {
            Err(_) => {
                let diagnostics = child.terminate_and_reap().await?;
                return Err(io::Error::other(format!(
                    "permission seeder exceeded the bounded lifetime:\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    diagnostics.stdout_tail.lossy(),
                    diagnostics.stderr_tail.lossy(),
                )));
            }
            Ok(Err(error)) => return Err(error),
            Ok(Ok(_)) => {}
        }
        let diagnostics = child.terminate_and_reap().await?;
        let clean = diagnostics
            .exit_status
            .is_some_and(|status| status.success());
        let report = diagnostics.stdout_tail.lossy();
        if !clean || !report.contains(plugin_location) {
            return Err(io::Error::other(format!(
                "permission seeder granted no scoped bridge permission (status {:?}):\n--- stdout ---\n{report}\n--- stderr ---\n{}",
                diagnostics.exit_status,
                diagnostics.stderr_tail.lossy(),
            )));
        }
        eprintln!("[permit] {report}");
        Ok(())
    }
    /// Session socket bind path for a new foreground server:
    /// `<socket-dir>/contract_version_1/<session>`.
    #[must_use]
    pub fn session_socket_path(&self, session: &str) -> PathBuf {
        self.socket_dir.join(ZELLIJ_CONTRACT_DIR).join(session)
    }

    /// Owned workdir: cwd for every server, bootstrap, PTY, and CLI child
    /// of this host. Never the repo or user cwd.
    #[must_use]
    pub fn workdir(&self) -> &Path {
        &self.workdir
    }

    /// Owned Zellij config file, passed explicitly to every CLI child
    /// and to the public integration install.
    #[must_use]
    pub fn config_file(&self) -> &Path {
        &self.config_file
    }

    /// Installs one more retained foreground server child spawned against
    /// [`OwnedZellijHost::session_socket_path`]. [`OwnedZellijHost::shutdown`]
    /// reaps every server in install order.
    pub fn attach_server_child(&mut self, child: OwnedChild) {
        self.servers.push(child);
    }

    /// Complete scoped environment for children that are both muxe-scoped
    /// and host-addressed (foreground servers, bootstrap peers, brokers,
    /// activate): one `env_clear` with the muxe `TempDir` base scope, then
    /// the additive Zellij-only overlay. The overlay never clears, so the
    /// shared muxe config/cache/runtime roots survive; calling the
    /// host-base [`OwnedZellijHost::apply_host_env`] here instead would
    /// wipe them (and the reverse order would drop the socket identity).
    /// PATH passes through for system tool lookup.
    pub fn apply_host_scoped_env(&self, command: &mut Command, scoped_root: &Path) {
        apply_scoped_env(command, scoped_root);
        self.apply_host_env_overlay(command);
    }

    /// Additive Zellij session identity for muxe-scoped children: socket,
    /// config file, config dir, and data dir only. No `env_clear`, no
    /// HOME/XDG/TMPDIR/PATH changes: the shared muxe registry roots stay
    /// exactly the scoped ones, so brokers and `activate` resolve the
    /// same registry `init` wrote.
    pub fn apply_host_env_overlay(&self, command: &mut Command) {
        command.envs(self.host_env_overlay_vec());
    }

    /// The single source for the Zellij overlay: the exact pairs
    /// [`OwnedZellijHost::apply_host_env_overlay`] installs.
    #[must_use]
    pub fn host_env_overlay_vec(&self) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        vec![
            (
                std::ffi::OsString::from("ZELLIJ_SOCKET_DIR"),
                self.socket_dir.clone().into_os_string(),
            ),
            (
                std::ffi::OsString::from("ZELLIJ_CONFIG_FILE"),
                self.config_file.clone().into_os_string(),
            ),
            (
                std::ffi::OsString::from("ZELLIJ_CONFIG_DIR"),
                self.config_dir.clone().into_os_string(),
            ),
            (
                std::ffi::OsString::from("ZELLIJ_DATA_DIR"),
                self.data_dir.clone().into_os_string(),
            ),
        ]
    }

    /// Isolated environment shared by every Zellij CLI child of this host.
    /// Host-base scope only: muxe children must use
    /// [`OwnedZellijHost::apply_host_scoped_env`] instead, or the shared
    /// registry roots are lost.
    pub fn apply_host_env(&self, command: &mut Command) {
        command.env_clear();
        command.envs(self.host_env_vec());
    }

    /// The single source for the host-base scope (Zellij CLI children).
    /// Kept separate so the trap test can prove this scope resolves a
    /// different muxe registry than the shared one.
    #[must_use]
    pub fn host_env_vec(&self) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
        use std::ffi::OsString;
        let path = std::env::var_os("PATH").unwrap_or_default();
        vec![
            (
                OsString::from("HOME"),
                self.root.join("home").into_os_string(),
            ),
            (
                OsString::from("XDG_CACHE_HOME"),
                self.root.join("cache").into_os_string(),
            ),
            (
                OsString::from("TMPDIR"),
                self.root.join("tmp").into_os_string(),
            ),
            (OsString::from("PATH"), path),
            (
                OsString::from("ZELLIJ_SOCKET_DIR"),
                self.socket_dir.clone().into_os_string(),
            ),
            (
                OsString::from("ZELLIJ_CONFIG_DIR"),
                self.config_dir.clone().into_os_string(),
            ),
        ]
    }

    /// Scoped base command for every Zellij CLI child of this host: the
    /// pinned binary plus the isolated environment and config flags, with
    /// the owned workdir as cwd. Only explicitly named sessions are ever
    /// addressed.
    fn base_command(&self) -> Command {
        let mut command = Command::new(&self.zellij_binary);
        self.apply_host_env(&mut command);
        command
            .arg("--config")
            .arg(&self.config_file)
            .arg("--config-dir")
            .arg(&self.config_dir)
            .arg("--data-dir")
            .arg(&self.data_dir);
        command.current_dir(&self.workdir);
        command
    }

    /// Pinned client argv (without program): config flags plus
    /// `attach <session>`, mirroring the probe client.
    fn zellij_client_argv(&self, session: &str) -> Vec<std::ffi::OsString> {
        use std::ffi::OsString;
        vec![
            OsString::from("--config"),
            self.config_file.as_os_str().to_owned(),
            OsString::from("--config-dir"),
            self.config_dir.as_os_str().to_owned(),
            OsString::from("--data-dir"),
            self.data_dir.as_os_str().to_owned(),
            OsString::from("attach"),
            OsString::from(session),
        ]
    }

    /// Polls for the isolated session socket inside the startup budget.
    /// The versioned socket subdirectory is discovered, never hardcoded.
    pub async fn wait_for_session(&self, session: &str) -> io::Result<PathBuf> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(socket) = self.session_socket(session) {
                return Ok(socket);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "no isolated socket for Zellij session '{session}' under {}",
                        self.socket_dir.display()
                    ),
                ));
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// Scans the isolated socket directory for this session's socket file.
    fn session_socket(&self, session: &str) -> Option<PathBuf> {
        let entries = std::fs::read_dir(&self.socket_dir).ok()?;
        for version_dir in entries.flatten() {
            let candidate = version_dir.path().join(session);
            if let Ok(kind) = std::fs::symlink_metadata(&candidate).map(|meta| meta.file_type()) {
                #[cfg(unix)]
                if kind.is_socket() {
                    return Some(candidate);
                }
                #[cfg(not(unix))]
                if kind.is_file() {
                    return Some(candidate);
                }
            }
        }
        None
    }

    /// Best-effort session removal; teardown continues past a missing session.
    pub async fn kill_session(&self, session: &str) {
        let mut command = self.base_command();
        command.arg("kill-session").arg(session);
        let _ = run_cli_bounded(&format!("kill-session-{session}"), &mut command).await;
    }

    /// Lists current client IDs for one session via the pinned CLI
    /// (`zellij --session <s> action list-clients`), returning the
    /// sorted unique `CLIENT_ID` column. A missing header fails closed
    /// (unknown table shape) instead of passing an empty set; an
    /// authoritatively empty session legitimately yields an empty set.
    pub async fn list_clients(&self, session: &str) -> io::Result<Vec<String>> {
        let mut command = self.base_command();
        command
            .arg("--session")
            .arg(session)
            .arg("action")
            .arg("list-clients");
        let output = run_cli_bounded(&format!("list-clients-{session}"), &mut command).await?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "list-clients on session '{session}' failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let mut header_seen = false;
        let mut ids = Vec::new();
        for line in text.lines() {
            match line.split_whitespace().next() {
                None => {}
                Some("CLIENT_ID") => {
                    header_seen = true;
                }
                Some(id) => ids.push(id.to_owned()),
            }
        }
        if !header_seen {
            return Err(io::Error::other(format!(
                "list-clients on session '{session}' has no CLIENT_ID header: {text:?}"
            )));
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    /// Spawns one retained interactive client: a blocking `attach <session>`
    /// under a real PTY allocated by the platform `script(1)` utility, with
    /// this host's isolated environment plus an explicit TERM. The `script`
    /// child is the retained handle: killing it hangs up the client, and a
    /// client that exits during the grace window fails closed with the
    /// typescript tail. No new dependencies: `script` ships by default on
    /// macOS and Linux (argv differs per OS, selected at runtime).
    pub async fn spawn_client(
        &self,
        tag: &str,
        session: &str,
        typescript: &Path,
    ) -> io::Result<OwnedChild> {
        let argv = self.zellij_client_argv(session);
        // Explicit geometry inside the owned PTY: `script(1)` can inherit
        // a zero/nonterminal size, which the client would then report as
        // its terminal size. `stty` runs inside the owned PTY only and
        // never touches the parent terminal.
        let size_prefix = format!("stty rows {BOOTSTRAP_ROWS} cols {BOOTSTRAP_COLS}; ");
        let mut command = if cfg!(target_os = "macos") {
            let mut shell = std::ffi::OsString::from(size_prefix);
            shell.push("exec ");
            shell.push(shell_quote(self.zellij_binary.as_os_str()));
            for arg in &argv {
                shell.push(" ");
                shell.push(shell_quote(arg));
            }
            let mut command = Command::new("script");
            command
                .arg("-q")
                .arg(typescript)
                .arg("sh")
                .arg("-c")
                .arg(shell);
            command
        } else {
            let mut shell = std::ffi::OsString::from(size_prefix);
            shell.push("exec ");
            shell.push(shell_quote(self.zellij_binary.as_os_str()));
            for arg in &argv {
                shell.push(" ");
                shell.push(shell_quote(arg));
            }
            let mut command = Command::new("script");
            command.arg("-qec").arg(shell).arg(typescript);
            command
        };
        self.apply_host_env(&mut command);
        command.env("TERM", "xterm-256color");
        command.current_dir(&self.workdir);
        let mut child = OwnedChild::spawn(&format!("{tag}-pty-client"), &mut command)?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        if child.try_wait()?.is_some() {
            let diagnostics = child.terminate_and_reap().await?;
            let typescript_tail = std::fs::read(typescript).map_or_else(
                |error| format!("(typescript unreadable: {error})"),
                |bytes| {
                    let tail = bytes
                        .len()
                        .saturating_sub(MAX_DIAGNOSTIC_BYTES)
                        .min(bytes.len());
                    String::from_utf8_lossy(&bytes[tail..]).into_owned()
                },
            );
            return Err(io::Error::other(format!(
                "{tag} PTY client for session '{session}' exited during the grace window:\n--- child stdout ---\n{}\n--- child stderr ---\n{}\n--- typescript ---\n{typescript_tail}",
                diagnostics.stdout_tail.lossy(),
                diagnostics.stderr_tail.lossy(),
            )));
        }
        Ok(child)
    }

    /// Sessions created on this host, in creation order.
    #[must_use]
    pub fn sessions(&self) -> &[String] {
        &self.sessions
    }

    /// Whether a retained foreground server child exists. Runners fail
    /// closed while this is false rather than running serverless.
    #[must_use]
    pub fn has_server_child(&self) -> bool {
        !self.servers.is_empty() && self.servers.iter().all(OwnedChild::is_retained)
    }

    /// Fails closed unless every retained server child is still running.
    /// Exited children (including zombies) report here via their handle,
    /// never via PID signal, which misreports zombies as alive.
    pub fn check_servers_alive(&mut self) -> io::Result<()> {
        if self.servers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "owned Zellij host retains no server child",
            ));
        }
        for (index, server) in self.servers.iter_mut().enumerate() {
            if server.try_wait()?.is_some() {
                return Err(io::Error::other(format!(
                    "owned Zellij server child {index} exited"
                )));
            }
        }
        Ok(())
    }

    /// Tears down every session, then terminates and reaps the retained
    /// server child. Session teardown is best-effort; the server reap is
    /// awaited and its diagnostics returned. Without a server child this
    /// fails closed naming the missing ownership.
    pub async fn shutdown(&mut self) -> io::Result<Vec<ChildDiagnostics>> {
        for session in std::mem::take(&mut self.sessions) {
            self.kill_session(&session).await;
        }
        if self.servers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "owned Zellij host retains no server child to reap",
            ));
        }
        let mut diagnostics = Vec::new();
        for server in &mut self.servers {
            diagnostics.push(server.terminate_and_reap().await?);
        }
        Ok(diagnostics)
    }
}
/// Single-quotes one argv element for the util-linux `script -c` shell
/// string. Lossy conversion is documented: runner paths are UTF-8 by
/// construction (`TempDir` + session names).
fn shell_quote(arg: &std::ffi::OsStr) -> std::ffi::OsString {
    let mut quoted = std::ffi::OsString::from("'");
    quoted.push(arg.to_string_lossy().replace('\'', "'\\''"));
    quoted.push("'");
    quoted
}

/// Retained single-subscription continuity witness for one owned Herdr
/// server.
///
/// One `events.subscribe` stream opens before the first broker spawns and
/// stays open until after the final commit. A worker drains it
/// continuously: every event counts, and the first EOF, malformed line,
/// transport error, or reconnect demand ends the stream as continuity loss.
/// A healthy filtered subscription may remain quiet indefinitely; bounded
/// evidence is the event count plus the first failure, if any.
/// Combined with the retained foreground host child (checked live via its
/// handle, never via PID signal, which misreports zombies) plus a final
/// independent ping identity match, this proves the same server process
/// served throughout. Socket device/inode comparison is rejected (inode
/// reuse).
pub struct ContinuityGuard {
    tag: String,
    expected: String,
    socket: PathBuf,
    events: std::sync::Arc<tokio::sync::Mutex<(u64, Option<String>)>>,
    task: JoinHandle<()>,
}

/// Continuity verdict: the retained stream never broke.
#[derive(Debug)]
pub struct ContinuityReport {
    pub tag: String,
    pub events: u64,
}

/// Initial subscription handshake deadline, mirrored for the witness stream.
const WITNESS_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

impl ContinuityGuard {
    /// Opens the retained subscription (initial handshake fails fast) and
    /// starts the drain worker before returning.
    pub async fn watch_herdr(tag: &str, socket: PathBuf, expected: String) -> io::Result<Self> {
        let client = muxe_adapter_herdr::HerdrSocketClient::new(&socket);
        let config = muxe_adapter_herdr::SubscriptionConfig {
            params: serde_json::json!({ "subscriptions": [{ "type": "tab.focused" }] }),
            subscribe_timeout: WITNESS_SUBSCRIBE_TIMEOUT,
        };
        let (subscription, _) = muxe_adapter_herdr::EventSubscription::connect(&client, config)
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "{tag}: witness subscribe handshake failed at {}: {error}",
                    socket.display()
                ))
            })?;
        let events = std::sync::Arc::new(tokio::sync::Mutex::new((0u64, None)));
        let worker_events = events.clone();
        let task = tokio::spawn(async move {
            let mut subscription = subscription;
            loop {
                match subscription.next_event().await {
                    Ok(_) => {
                        worker_events.lock().await.0 += 1;
                    }
                    Err(error) => {
                        worker_events.lock().await.1 = Some(error.to_string());
                        break;
                    }
                }
            }
        });
        Ok(Self {
            tag: tag.to_owned(),
            expected,
            socket,
            events,
            task,
        })
    }

    /// Stops the drain and verifies continuity: no stream failure, then a
    /// final independent ping identity match against the expected key.
    pub async fn finish(self) -> io::Result<ContinuityReport> {
        self.task.abort();
        let (events, error) = self.events.lock().await.clone();
        if let Some(error) = error {
            return Err(io::Error::other(format!(
                "continuity witness '{}': stream failed after {events} events: {error}",
                self.tag
            )));
        }
        let client = muxe_adapter_herdr::HerdrSocketClient::new(&self.socket);
        let identity = muxe_adapter_herdr::probe_live_identity(&client)
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "continuity witness '{}': final ping failed at {}: {error}",
                    self.tag,
                    self.socket.display()
                ))
            })?;
        if identity.discovery_key != self.expected {
            return Err(io::Error::other(format!(
                "continuity witness '{}': live identity changed mid-run",
                self.tag
            )));
        }
        Ok(ContinuityReport {
            tag: self.tag,
            events,
        })
    }
}

/// Readiness budget for one broker control handshake.
pub const BROKER_TIMEOUT: Duration = Duration::from_mins(1);
/// Release budget for a retired broker endpoint.
pub const RETIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Polls a fallible condition until success or the deadline. Every attempt
/// failure is preserved in the final timeout report.
pub async fn poll_until<T>(
    what: &str,
    timeout: Duration,
    mut attempt: impl AsyncFnMut() -> Result<T, String>,
) -> io::Result<T> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("timed out waiting for {what}: {error}"),
                    ));
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }
}

/// One old broker serving an owned Herdr server: the retained
/// `broker serve-herdr` child plus its normal endpoint. Spawned only
/// through the installed binary with the broker-authored fixed argv.
pub struct ServedBroker {
    pub tag: String,
    pub child: OwnedChild,
    pub endpoint: PathBuf,
}

/// Spawns `<muxe_binary> broker serve-herdr` against the owned Herdr
/// server and waits for Running status over the control endpoint. The
/// child runs under the scoped environment so registries and journals
/// stay inside the owned root. Any failure reaps the child and reports
/// its diagnostics.
#[expect(
    clippy::too_many_arguments,
    reason = "serve-herdr spawn threads every explicit typed input (tag, three binaries/paths, discovery, roots, dirs) with absolute paths; bundling would hide the typed-input surface"
)]
pub async fn spawn_serve_herdr(
    tag: &str,
    muxe_binary: &Path,
    herdr_binary: &Path,
    herdr_socket: &Path,
    discovery_key: &str,
    scoped_root: &Path,
    config_file: &Path,
    cache_dir: &Path,
) -> io::Result<ServedBroker> {
    let endpoint = muxe_broker::RuntimeEndpoint::in_runtime_dir(
        scoped_root.join("runtime"),
        muxe_protocol::HostKind::Herdr,
        discovery_key,
    )
    .map_err(|error| {
        io::Error::other(format!(
            "{tag}: cannot derive the normal Herdr endpoint: {error}"
        ))
    })?
    .socket()
    .to_path_buf();
    let mut command = serve_herdr_command(
        tag,
        muxe_binary,
        herdr_binary,
        herdr_socket,
        &endpoint,
        scoped_root,
        config_file,
        cache_dir,
    )?;
    let mut child = OwnedChild::spawn(&format!("{tag}-serve-herdr"), &mut command)?;
    await_running(&mut child, &endpoint, tag, "serve-herdr").await?;
    Ok(ServedBroker {
        tag: tag.to_owned(),
        child,
        endpoint,
    })
}

/// Builds the `broker serve-herdr` command from the broker-authored argv
/// under the scoped environment with the owned scoped root as cwd.
#[expect(
    clippy::too_many_arguments,
    reason = "serve argv is the exact eight-part broker-authored shape"
)]
pub fn serve_herdr_command(
    tag: &str,
    muxe_binary: &Path,
    herdr_binary: &Path,
    herdr_socket: &Path,
    endpoint: &Path,
    scoped_root: &Path,
    config_file: &Path,
    cache_dir: &Path,
) -> io::Result<Command> {
    let spawn = muxe_broker::ServeHerdrSpawn {
        binary: muxe_binary.to_path_buf(),
        socket: endpoint.to_path_buf(),
        herdr_binary: herdr_binary.to_path_buf(),
        herdr_socket: herdr_socket.to_path_buf(),
        config: config_file.to_path_buf(),
        cache_dir: cache_dir.to_path_buf(),
        handoff: None,
        activation_journal: None,
    };
    let argv = spawn.argv().map_err(|error| {
        io::Error::other(format!("{tag}: cannot render serve-herdr argv: {error}"))
    })?;
    let mut command = Command::new(muxe_binary);
    command.args(argv);
    apply_scoped_env(&mut command, scoped_root);
    command.current_dir(scoped_root);
    Ok(command)
}

/// Waits for Running status over a broker control endpoint. Any failure
/// reaps the child and reports its diagnostics with the failure.
async fn await_running(
    child: &mut OwnedChild,
    endpoint: &Path,
    tag: &str,
    mode: &str,
) -> io::Result<()> {
    let outcome: io::Result<()> = async {
        let mut control = poll_until(
            &format!("{tag} control endpoint"),
            BROKER_TIMEOUT,
            async || {
                muxe::lifecycle::control::ControlClient::connect(endpoint)
                    .await
                    .map_err(|error| error.to_string())
            },
        )
        .await?;
        let status = poll_until(
            &format!("{tag} running status"),
            BROKER_TIMEOUT,
            async || control.status().await.map_err(|error| error.to_string()),
        )
        .await?;
        if status.lifecycle != muxe_protocol::control::LifecycleState::Running {
            return Err(io::Error::other(format!(
                "{tag} broker is not running: {:?}",
                status.lifecycle
            )));
        }
        Ok(())
    }
    .await;
    if let Err(error) = outcome {
        let diagnostics = child.terminate_and_reap().await?;
        return Err(io::Error::other(format!(
            "{tag} {mode} never reached Running: {error}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            diagnostics.stdout_tail.lossy(),
            diagnostics.stderr_tail.lossy(),
        )));
    }
    Ok(())
}

/// Spawns `<muxe_binary> broker serve-zellij` for one session against the
/// pinned Zellij host and waits for Running status. Fixed argv mirroring
/// serve-herdr (socket, zellij-exe, session, config, cache-dir; handoff
/// pair for targets); the server enforces the normal endpoint derived
/// from the session identity and self-registers, so any mismatch fails
/// naming it. Same scoped environment and diagnostics discipline as Herdr.
#[expect(
    clippy::too_many_arguments,
    reason = "serve-zellij spawn threads every explicit typed input (tag, binary, host, exe, session, roots, dirs) with absolute paths; bundling would hide the typed-input surface"
)]
pub async fn spawn_serve_zellij(
    tag: &str,
    muxe_binary: &Path,
    host: &OwnedZellijHost,
    zellij_exe: &Path,
    session: &str,
    scoped_root: &Path,
    config_file: &Path,
    cache_dir: &Path,
) -> io::Result<ServedBroker> {
    let endpoint = muxe_broker::RuntimeEndpoint::in_runtime_dir(
        scoped_root.join("runtime"),
        muxe_protocol::HostKind::Zellij,
        session,
    )
    .map_err(|error| {
        io::Error::other(format!(
            "{tag}: cannot derive the normal Zellij endpoint: {error}"
        ))
    })?
    .socket()
    .to_path_buf();
    let mut command = serve_zellij_command(
        muxe_binary,
        host,
        &endpoint,
        zellij_exe,
        session,
        scoped_root,
        config_file,
        cache_dir,
    );
    let mut child = OwnedChild::spawn(&format!("{tag}-serve-zellij"), &mut command)?;
    await_running(&mut child, &endpoint, tag, "serve-zellij").await?;
    Ok(ServedBroker {
        tag: tag.to_owned(),
        child,
        endpoint,
    })
}

/// Builds the `broker serve-zellij` command: broker-authored fixed argv
/// under the merged scoped+host environment with the owned scoped root
/// as cwd. The single merged env call preserves both the muxe `TempDir`
/// scoping and the host session identity; layering `apply_host_env`
/// after `apply_scoped_env` would `env_clear` the muxe scoping away.
#[must_use]
#[expect(
    clippy::too_many_arguments,
    reason = "serve argv is the exact broker-authored fixed shape"
)]
pub fn serve_zellij_command(
    muxe_binary: &Path,
    host: &OwnedZellijHost,
    endpoint: &Path,
    zellij_exe: &Path,
    session: &str,
    scoped_root: &Path,
    config_file: &Path,
    cache_dir: &Path,
) -> Command {
    let mut command = Command::new(muxe_binary);
    command
        .arg("broker")
        .arg("serve-zellij")
        .arg("--socket")
        .arg(endpoint)
        .arg("--zellij-exe")
        .arg(zellij_exe)
        .arg("--session")
        .arg(session)
        .arg("--config")
        .arg(config_file)
        .arg("--cache-dir")
        .arg(cache_dir);
    host.apply_host_scoped_env(&mut command, scoped_root);
    command.current_dir(scoped_root);
    command
}

/// Retires one broker over its control endpoint, then waits for the
/// endpoint release. The retired broker unlinks its endpoint on exit.
pub async fn retire_broker(endpoint: &Path, tag: &str) -> io::Result<()> {
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| io::Error::other(format!("connect {tag} for retire: {error}")))?;
    control
        .retire()
        .await
        .map_err(|error| io::Error::other(format!("retire {tag}: {error}")))?;
    poll_until(&format!("{tag} endpoint release"), RETIRE_TIMEOUT, || {
        let released = !endpoint.exists();
        async move {
            if released {
                Ok(())
            } else {
                Err("endpoint still bound".to_owned())
            }
        }
    })
    .await
}

/// Asserts that production journal discovery finds no preserved activation
/// journals under `cache_dir`: a missing directory is clean, while every
/// returned JSON journal path fails the assertion. Corrupt journals remain
/// visible in the production listing and therefore fail too. Persistent
/// unit-lock inodes are not journals and are ignored by `list_journals`.
/// Journals are removed on commit, so leftovers mean a preserved or aborted
/// unit the next transfer must not inherit. Call before and after a transfer
/// to verify group hygiene.
pub fn assert_no_preserved_journals(cache_dir: &Path, tag: &str) -> io::Result<()> {
    let journals = muxe::lifecycle::journal::list_journals(cache_dir).map_err(|error| {
        io::Error::other(format!("{tag}: cannot list activation journals: {error}"))
    })?;
    if journals.is_empty() {
        Ok(())
    } else {
        let leftovers = journals
            .into_iter()
            .map(|(path, result)| match result {
                Ok(_) => path,
                Err(error) => PathBuf::from(format!("{} ({error})", path.display())),
            })
            .collect::<Vec<_>>();
        Err(io::Error::other(format!(
            "{tag}: preserved activation journals remain: {leftovers:?}; recovery state would contaminate the next transfer"
        )))
    }
}

/// Asserts the broker at `endpoint` serves Running with the expected
/// installed version, and returns its live discovery key for the
/// subscription-transfer proof.
pub async fn assert_broker_serving(
    endpoint: &Path,
    tag: &str,
    want_version: &str,
) -> io::Result<String> {
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| io::Error::other(format!("connect {tag} for status: {error}")))?;
    let status = control
        .status()
        .await
        .map_err(|error| io::Error::other(format!("{tag} status failed: {error}")))?;
    if status.lifecycle != muxe_protocol::control::LifecycleState::Running {
        return Err(io::Error::other(format!(
            "{tag} broker is not Running after activation: {:?}",
            status.lifecycle
        )));
    }
    if status.current.muxe_version != want_version {
        return Err(io::Error::other(format!(
            "{tag} broker serves version {}, want installed {want_version}",
            status.current.muxe_version
        )));
    }
    Ok(status.live_server.discovery_key)
}

/// Reads one broker's full reported compatibility record over control.
/// Used to state exact expectations (probed from a real broker) without
/// inventing records.
pub async fn read_broker_record(
    endpoint: &Path,
    tag: &str,
) -> io::Result<muxe_protocol::control::CompatibilityRecord> {
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| io::Error::other(format!("connect {tag} for record: {error}")))?;
    control
        .status()
        .await
        .map(|status| status.current)
        .map_err(|error| io::Error::other(format!("{tag} record status failed: {error}")))
}

/// Bounded readiness observation for a Herdr transfer target: the endpoint
/// must report the exact expected record with a handoff attached, the
/// matching discovery key, and Running lifecycle. Herdr has no bridge
/// concept, so no coverage applies here; Zellij sessions use
/// [`await_session_ready`] with snapshot-gated coverage instead.
pub async fn await_target_ready(
    endpoint: &Path,
    tag: &str,
    expected: &muxe_protocol::control::CompatibilityRecord,
    discovery: &str,
    timeout: Duration,
) -> io::Result<()> {
    poll_until(&format!("{tag} target readiness"), timeout, || async {
        let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
            .await
            .map_err(|error| error.to_string())?;
        let status = control.status().await.map_err(|error| error.to_string())?;
        let running = matches!(
            status.lifecycle,
            muxe_protocol::control::LifecycleState::Running
        );
        if status.current == *expected
            && status.handoff_id.is_some()
            && status.live_server.discovery_key == discovery
            && running
        {
            Ok(())
        } else {
            Err(format!(
                "not ready: lifecycle {:?}, handoff {}, version {}",
                status.lifecycle,
                status.handoff_id.is_some(),
                status.current.muxe_version,
            ))
        }
    })
    .await
}

/// Bounded readiness observation for one Zellij session target: the
/// endpoint must report the exact expected record with a handoff attached
/// and the matching discovery key, AND the same round's readiness
/// coverage must hold every client of the runner's own fresh
/// `list-clients` snapshot: `member_clients` equals the snapshot length
/// and every snapshot ID is registered. Count-only gating has a hole (a
/// post-snapshot newcomer can mask a missing member at equal counts);
/// registration IDs are never compared across rounds (the bridge
/// re-mints them per event-channel lifetime) and sets are never unioned
/// across polls. Retake the snapshot every round; a ready observed
/// pre-swap means nothing post-boundary.
pub async fn await_session_ready(
    endpoint: &Path,
    tag: &str,
    expected: &muxe_protocol::control::CompatibilityRecord,
    discovery: &str,
    host: &OwnedZellijHost,
    session: &str,
    timeout: Duration,
) -> io::Result<()> {
    poll_until(&format!("{tag} session readiness"), timeout, || async {
        let snapshot = host
            .list_clients(session)
            .await
            .map_err(|error| error.to_string())?;
        let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
            .await
            .map_err(|error| error.to_string())?;
        let status = control
            .status()
            .await
            .map_err(|error| error.to_string())?;
        let covered = status.ready.as_ref().is_some_and(|ready| {
            usize::try_from(ready.member_clients).unwrap_or(usize::MAX) == snapshot.len()
                && {
                    let registered: std::collections::BTreeSet<&str> = ready
                        .registered_clients
                        .iter()
                        .map(String::as_str)
                        .collect();
                    snapshot
                        .iter()
                        .all(|id| registered.contains(id.as_str()))
                }
        });
        if status.current == *expected
            && status.handoff_id.is_some()
            && status.live_server.discovery_key == discovery
            && covered
        {
            Ok(())
        } else {
            Err(format!(
                "not ready: lifecycle {:?}, handoff {}, version {}, snapshot {snapshot:?}, coverage {:?}",
                status.lifecycle,
                status.handoff_id.is_some(),
                status.current.muxe_version,
                status.ready.as_ref().map(|ready| (
                    ready.registered_clients.len(),
                    ready.member_clients
                )),
            ))
        }
    })
    .await
}

/// Reads the installed version string from `<binary> compatibility --json`.
/// Used to state post-activation expectations without inventing records.
pub async fn installed_version(binary: &Path) -> io::Result<String> {
    // Probes run before any case TempDir exists: hold an owned TempDir
    // for cwd so the child never inherits repo/user cwd.
    let probe_root = short_tempdir("muxe-live-probe-")?;
    let output = Command::new(binary)
        .arg("compatibility")
        .arg("--json")
        .current_dir(probe_root.path())
        .output()
        .await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "compatibility --json failed for {}",
            binary.display()
        )));
    }
    let record: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        io::Error::other(format!(
            "compatibility --json of {} is not JSON: {error}",
            binary.display()
        ))
    })?;
    record
        .get("muxe_version")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            io::Error::other(format!(
                "compatibility --json of {} names no muxe_version",
                binary.display()
            ))
        })
}

/// Reads the packaged bridge digest from `<binary> compatibility --json`.
/// `Ok(None)` when this build carries no packaged bridge (development
/// builds fail closed elsewhere); the hex string is logged as evidence,
/// never trusted from a sidecar.
pub async fn installed_wasm_digest(binary: &Path) -> io::Result<Option<String>> {
    let probe_root = short_tempdir("muxe-live-probe-")?;
    let output = Command::new(binary)
        .arg("compatibility")
        .arg("--json")
        .current_dir(probe_root.path())
        .output()
        .await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "compatibility --json failed for {}",
            binary.display()
        )));
    }
    let record: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|error| {
        io::Error::other(format!(
            "compatibility --json of {} is not JSON: {error}",
            binary.display()
        ))
    })?;
    Ok(record
        .get("packaged_wasm")
        .and_then(|packaged| packaged.get("sha256"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned))
}

/// Bound on one activate child lifetime.
pub const ACTIVATE_TIMEOUT: Duration = Duration::from_mins(10);

/// Drives `<binary> activate` (public CLI) under the scoped environment and
/// asserts a clean exit. The child runs under an owned handle with a
/// bounded lifetime: expiry kills with escalation and fails closed with
/// the preserved diagnostics. Post-conditions are asserted by the caller
/// against the serving brokers, never by parsing report text.
pub async fn drive_activate(
    binary: &Path,
    scoped_root: &Path,
    zellij_host: Option<&OwnedZellijHost>,
    tag: &str,
) -> io::Result<String> {
    let child = spawn_activate(binary, scoped_root, zellij_host, None, &[], tag)?;
    await_activate(child, tag).await
}

/// Spawns `<binary> activate` without waiting, so a test can interact
/// (barriers, observations) while the coordinator runs. `zellij_host`
/// propagates the owned Zellij context (socket/config/data dirs) so the
/// coordinator and every broker it spawns address the owned sessions,
/// never a default-user session. `path_prepend` optionally fronts one
/// owned directory on PATH (fault-injector boundary); nothing else about
/// the environment changes.
pub fn spawn_activate(
    binary: &Path,
    scoped_root: &Path,
    zellij_host: Option<&OwnedZellijHost>,
    path_prepend: Option<&Path>,
    extra_env: &[(&str, &str)],
    tag: &str,
) -> io::Result<OwnedChild> {
    let mut command = activate_command(binary, scoped_root, zellij_host, path_prepend, extra_env);
    OwnedChild::spawn(&format!("{tag}-activate"), &mut command)
}

/// Builds the `<binary> activate` command: the public CLI argv under the
/// merged scoped+host environment with the owned scoped root as cwd.
/// `path_prepend` optionally fronts one owned directory on PATH
/// (fault-injector boundary); nothing else about the environment changes.
pub fn activate_command(
    binary: &Path,
    scoped_root: &Path,
    zellij_host: Option<&OwnedZellijHost>,
    path_prepend: Option<&Path>,
    extra_env: &[(&str, &str)],
) -> Command {
    let mut command = Command::new(binary);
    command.arg("activate");
    match zellij_host {
        Some(host) => host.apply_host_scoped_env(&mut command, scoped_root),
        None => apply_scoped_env(&mut command, scoped_root),
    }
    if let Some(prepend) = path_prepend {
        let mut path = std::ffi::OsString::from(prepend.as_os_str());
        path.push(":");
        path.push(std::env::var_os("PATH").unwrap_or_default());
        command.env("PATH", path);
    }
    for (name, value) in extra_env {
        command.env(name, value);
    }
    command.current_dir(scoped_root);
    command
}

/// Awaits an activate child inside the lifetime bound and returns its
/// stdout on a clean exit. Any other outcome fails closed with both
/// captured streams.
pub async fn await_activate(mut child: OwnedChild, tag: &str) -> io::Result<String> {
    match tokio::time::timeout(ACTIVATE_TIMEOUT, child.wait()).await {
        Err(_) => {
            let diagnostics = child.terminate_and_reap().await?;
            return Err(io::Error::other(format!(
                "{tag}: muxe activate exceeded the bounded lifetime:\n--- stdout ---\n{}\n--- stderr ---\n{}",
                diagnostics.stdout_tail.lossy(),
                diagnostics.stderr_tail.lossy(),
            )));
        }
        Ok(Err(error)) => return Err(error),
        Ok(Ok(_)) => {}
    }
    let diagnostics = child.terminate_and_reap().await?;
    if !diagnostics
        .exit_status
        .is_some_and(|status| status.success())
    {
        return Err(io::Error::other(format!(
            "{tag}: muxe activate failed:\n--- stdout ---\n{}\n--- stderr ---\n{}",
            diagnostics.stdout_tail.lossy(),
            diagnostics.stderr_tail.lossy(),
        )));
    }
    Ok(diagnostics.stdout_tail.lossy())
}

/// Host-free regression for the owned-spawn boundary: every foreground,
/// bootstrap, PTY, CLI, and muxe child is spawned through the exact
/// production command constructors, and a real child process observes its
/// real environment. Nothing here asserts `Command` field copies: each
/// fake binary validates its own runtime env and cwd from inside the
/// child, fails closed (exit 3, no marker) when any root escapes the
/// expected `TempDir`, and only then leaves the owned marker the test
/// observes. No hosts, no sockets, no approval.
#[cfg(all(test, unix))]
mod scoped_spawn_tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    const MERGED_VARS: &str = "HOME XDG_CONFIG_HOME XDG_CACHE_HOME XDG_RUNTIME_DIR TMPDIR \
         ZELLIJ_SOCKET_DIR ZELLIJ_CONFIG_FILE ZELLIJ_CONFIG_DIR ZELLIJ_DATA_DIR";
    const CLI_VARS: &str = "HOME XDG_CACHE_HOME TMPDIR ZELLIJ_SOCKET_DIR ZELLIJ_CONFIG_DIR";
    const MUXE_VARS: &str = "HOME XDG_CONFIG_HOME XDG_CACHE_HOME XDG_RUNTIME_DIR TMPDIR";

    fn shell_escape(value: &str) -> String {
        value.replace('\'', "'\\''")
    }

    /// Writes an owner-executable fake binary with the expected root,
    /// marker, and behavior embedded: it checks every variable in
    /// `check_vars` (plus `TERM` when `check_term`) against the embedded
    /// root from inside the child, checks its own cwd, and only then
    /// acts. Any violation exits 3 before any write, so even a pre-fix
    /// over-scoped spawn can never touch ambient user dirs through this
    /// fake. Modes: `linger` (marker, then sleep for reap), `pty`
    /// (marker plus the owned PTY's real `stty size`, then sleep),
    /// `clients` (marker plus a `list-clients` table), `evidence`
    /// (marker plus a bootstrap render report), `permit` (marker plus a
    /// fixed grant report naming the plugin location), `ok` (marker,
    /// exit 0).
    fn write_fake(
        dir: &Path,
        name: &str,
        expected_root: &Path,
        check_vars: &str,
        check_term: bool,
        marker: &Path,
        mode: &str,
    ) -> PathBuf {
        // Canonicalize once: on macOS the TempDir lives under a
        // symlinked `/var` while a child `pwd` reports the physical
        // `/private/var`. Containment checks must compare physical
        // paths, or the fake fails safe on an artifact instead of a
        // real escape. Markers still work through the symlink.
        let expected_root = std::fs::canonicalize(expected_root).expect("canonical expected root");
        let script = format!(
            "#!/bin/sh\n\
             EXPECTED_ROOT='{root}'\n\
             MARKER='{marker}'\n\
             fail() {{ echo \"scope violation: $1\" >&2; exit 3; }}\n\
             [ -n \"$EXPECTED_ROOT\" ] || {{ echo \"scope test has no EXPECTED_ROOT\" >&2; exit 3; }}\n\
             for var in {vars}; do\n\
               eval \"value=\\$$var\"\n\
               case \"${{value:-}}\" in\n\
                 \"\") fail \"$var is empty\" ;;\n\
                 \"$EXPECTED_ROOT\"/*) ;;\n\
                 *) fail \"$var=$value\" ;;\n\
               esac\n\
             done\n\
             {term_check}\
             case \"$(pwd)\" in\n\
               \"$EXPECTED_ROOT\"/*) ;;\n\
               *) fail \"cwd=$(pwd)\" ;;\n\
             esac\n\
            {mode_body}\n",
            root = shell_escape(&expected_root.to_string_lossy()),
            marker = shell_escape(&marker.to_string_lossy()),
            vars = check_vars,
            term_check = if check_term {
                "[ \"${TERM:-}\" = \"xterm-256color\" ] || fail \"TERM=${TERM:-}\";\n"
            } else {
                ""
            },
            mode_body = match mode {
                "linger" => ": > \"$MARKER\" || exit 3;\nexec sleep 30",
                // PTY mode additionally records the owned PTY's real
                // geometry (`stty size` prints `rows cols`): the spawn
                // under test must set it explicitly inside its own PTY.
                "pty" =>
                    ": > \"$MARKER\" || exit 3;\n\
                     (stty size > \"$MARKER.size\" 2>/dev/null || echo stty-failed > \"$MARKER.size\");\n\
                     exec sleep 30",
                "clients" => ": > \"$MARKER\" || exit 3;\nprintf 'CLIENT_ID\\ntest-client-1\\n'",
                "evidence" =>
                    ": > \"$MARKER\" || exit 3;\n\
                     echo \"bootstrapped session: first render evidence (7 bytes)\"",
                // Permit mode prints a fixed grant report naming the
                // plugin location the test passes to the seed runner.
                "permit" =>
                    ": > \"$MARKER\" || exit 3;\n\
                     echo \"permitted /owned/fixture/bridge.wasm (3 permissions) at $MARKER.cache\"",
                _ => ": > \"$MARKER\" || exit 3",
            },
        );
        let path = dir.join(name);
        std::fs::write(&path, script).expect("write fake binary");
        let mut permissions = std::fs::metadata(&path).expect("stat fake").permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&path, permissions).expect("chmod fake");
        path
    }

    fn case_dir(name: &str) -> tempfile::TempDir {
        // Resolver tests temporarily redirect the process-wide TMPDIR. Keep
        // case roots outside that mutable variable so a concurrent resolver
        // cannot create another test's TempDir beneath a root it will drop.
        tempfile::Builder::new()
            .prefix(&format!("muxe-scope-{name}-"))
            .tempdir_in("/tmp")
            .expect("owned scope TempDir")
    }

    fn scoped_root(case: &Path) -> PathBuf {
        let case = std::fs::canonicalize(case).expect("canonical case root");
        let root = case.join("scoped");
        ensure_scoped_dirs(&root).expect("owned scoped dirs");
        root
    }

    fn wait_for_marker(marker: &Path) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !marker.is_file() {
            assert!(
                std::time::Instant::now() < deadline,
                "scoped child never left its owned marker"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[tokio::test]
    async fn foreground_spawn_scopes_real_child() {
        let case = case_dir("foreground");
        let scoped = scoped_root(case.path());
        let marker = case.path().join("foreground.marker");
        let fake = write_fake(
            case.path(),
            "fake-foreground",
            case.path(),
            MERGED_VARS,
            false,
            &marker,
            "linger",
        );
        let host = OwnedZellijHost::prepare(&fake, case.path(), "scope").expect("prepare host");
        let mut child = host
            .spawn_foreground_server(&fake, "scope-sess", &scoped)
            .expect("spawn foreground");
        wait_for_marker(&marker);
        child
            .terminate_and_reap()
            .await
            .expect("reap foreground fake");
    }

    #[tokio::test]
    async fn bootstrap_spawn_scopes_real_child_and_reports_evidence() {
        let case = case_dir("bootstrap");
        let scoped = scoped_root(case.path());
        let marker = case.path().join("bootstrap.marker");
        let fake = write_fake(
            case.path(),
            "fake-bootstrap",
            case.path(),
            MERGED_VARS,
            true,
            &marker,
            "evidence",
        );
        let host = OwnedZellijHost::prepare(&fake, case.path(), "scope").expect("prepare host");
        host.run_bootstrap(&fake, "scope-sess", &scoped)
            .await
            .expect("bootstrap evidence");
        assert!(marker.is_file(), "bootstrap child left no owned marker");
    }

    #[tokio::test]
    async fn permission_seed_scopes_real_child_and_reports_grant() {
        let case = case_dir("permit");
        let scoped = scoped_root(case.path());
        let marker = case.path().join("permit.marker");
        let fake = write_fake(
            case.path(),
            "fake-seeder",
            case.path(),
            MUXE_VARS,
            false,
            &marker,
            "permit",
        );
        let host = OwnedZellijHost::prepare(&fake, case.path(), "scope").expect("prepare host");
        host.run_permission_seed(&fake, "/owned/fixture/bridge.wasm", &scoped)
            .await
            .expect("permission grant");
        assert!(marker.is_file(), "seeder child left no owned marker");
    }

    #[tokio::test]
    async fn cli_spawn_scopes_real_child() {
        let case = case_dir("cli");
        let marker = case.path().join("cli.marker");
        let fake = write_fake(
            case.path(),
            "fake-zellij",
            case.path(),
            CLI_VARS,
            false,
            &marker,
            "clients",
        );
        let host = OwnedZellijHost::prepare(&fake, case.path(), "scope").expect("prepare host");
        let clients = host
            .list_clients("scope-sess")
            .await
            .expect("list-clients through scoped env");
        assert_eq!(clients, vec!["test-client-1".to_owned()]);
        assert!(marker.is_file(), "CLI child left no owned marker");
    }

    #[tokio::test]
    async fn pty_spawn_scopes_real_child_with_explicit_size() {
        let case = case_dir("pty");
        let marker = case.path().join("pty.marker");
        let fake = write_fake(
            case.path(),
            "fake-zellij",
            case.path(),
            CLI_VARS,
            true,
            &marker,
            "pty",
        );
        let host = OwnedZellijHost::prepare(&fake, case.path(), "scope").expect("prepare host");
        let typescript = host.workdir().join("client-scope.log");
        let mut child = host
            .spawn_client("scope-tag", "scope-sess", &typescript)
            .await
            .expect("spawn PTY client");
        wait_for_marker(&marker);
        // Real geometry evidence: the fake records `stty size` from
        // inside the owned PTY. The spawn under test must set rows/cols
        // explicitly there; an inherited zero/nonterminal size fails here.
        let size_file = marker.with_extension("marker.size");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !size_file.is_file() {
            assert!(
                std::time::Instant::now() < deadline,
                "owned PTY left no geometry evidence"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        let size = std::fs::read_to_string(&size_file)
            .expect("owned PTY geometry evidence")
            .trim()
            .to_owned();
        assert_eq!(
            size,
            format!("{BOOTSTRAP_ROWS} {BOOTSTRAP_COLS}"),
            "owned PTY has no explicit geometry",
        );
        assert!(typescript.is_file(), "PTY typescript was never created");
        child.terminate_and_reap().await.expect("reap PTY fake");
    }

    #[tokio::test]
    async fn muxe_spawns_scope_real_children() {
        let case = case_dir("muxe");
        let scoped = scoped_root(case.path());
        let config_file = scoped.join("config").join("muxe").join("config.yml");
        let cache_dir = scoped.join("cache").join("muxe");
        std::fs::create_dir_all(config_file.parent().expect("config parent")).expect("config dir");
        std::fs::create_dir_all(&cache_dir).expect("cache dir");
        let marker_herdr = case.path().join("herdr.marker");
        let marker_zellij = case.path().join("zellij.marker");
        let marker_activate = case.path().join("activate.marker");
        let fake_herdr = write_fake(
            case.path(),
            "fake-muxe-herdr",
            case.path(),
            MUXE_VARS,
            false,
            &marker_herdr,
            "ok",
        );
        let fake_zellij = write_fake(
            case.path(),
            "fake-muxe-zellij",
            case.path(),
            &format!(
                "{MUXE_VARS} ZELLIJ_SOCKET_DIR ZELLIJ_CONFIG_FILE ZELLIJ_CONFIG_DIR ZELLIJ_DATA_DIR"
            ),
            false,
            &marker_zellij,
            "ok",
        );
        let fake_activate = write_fake(
            case.path(),
            "fake-muxe-activate",
            case.path(),
            &format!(
                "{MUXE_VARS} ZELLIJ_SOCKET_DIR ZELLIJ_CONFIG_FILE ZELLIJ_CONFIG_DIR ZELLIJ_DATA_DIR"
            ),
            false,
            &marker_activate,
            "ok",
        );
        let host =
            OwnedZellijHost::prepare(&fake_zellij, case.path(), "scope").expect("prepare host");
        let endpoint = scoped.join("runtime").join("test.sock");
        let mut herdr_command = serve_herdr_command(
            "scope",
            &fake_herdr,
            &fake_herdr,
            &endpoint,
            &endpoint,
            &scoped,
            &config_file,
            &cache_dir,
        )
        .expect("render serve-herdr argv");
        let mut zellij_command = serve_zellij_command(
            &fake_zellij,
            &host,
            &endpoint,
            &fake_zellij,
            "scope-sess",
            &scoped,
            &config_file,
            &cache_dir,
        );
        let mut activate_command =
            activate_command(&fake_activate, &scoped, Some(&host), None, &[]);
        for (tag, command, marker) in [
            ("herdr", &mut herdr_command, &marker_herdr),
            ("zellij", &mut zellij_command, &marker_zellij),
            ("activate", &mut activate_command, &marker_activate),
        ] {
            let mut child =
                OwnedChild::spawn(&format!("scope-{tag}"), command).expect("spawn muxe fake");
            // The marker proves the body ran; then poll for natural
            // exit. Reaping immediately would race a fast-exiting
            // child with SIGTERM and misreport cooperation as failure.
            wait_for_marker(marker);
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while child.try_wait().expect("poll scoped child").is_none() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{tag} scoped child never exited after its marker"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let diagnostics = child.terminate_and_reap().await.expect("reap muxe fake");
            let exit_status = diagnostics.exit_status;
            assert!(
                exit_status.is_some_and(|status| status.success()),
                "{tag} scoped child failed (status {exit_status:?}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
                diagnostics.stdout_tail.lossy(),
                diagnostics.stderr_tail.lossy(),
            );
            assert!(marker.is_file(), "{tag} scoped child left no owned marker");
        }
    }

    #[tokio::test]
    async fn fake_fails_safe_outside_expected_root() {
        // The fake itself is the safety net: with a root outside its
        // embedded expectation it must exit 3 before any write, so even a
        // pre-fix over-scoped spawn could never launder an ambient write
        // through it.
        let case = case_dir("unsafe");
        let marker = case.path().join("never.marker");
        let fake = write_fake(
            case.path(),
            "fake-strict",
            case.path(),
            MERGED_VARS,
            false,
            &marker,
            "ok",
        );
        let mut command = Command::new(&fake);
        command
            .env_clear()
            .env("HOME", "/")
            .env("PATH", std::env::var_os("PATH").unwrap_or_default());
        let output = command.output().await.expect("run strict fake");
        assert_eq!(output.status.code(), Some(3));
        assert!(
            !marker.is_file(),
            "strict fake wrote outside its expected root"
        );
    }

    #[tokio::test]
    async fn muxe_child_filesystem_matches_shared_registry() {
        // A real child carrying the production merged scope writes into
        // the shared registry locations on the real filesystem: config
        // proof under `$XDG_CONFIG_HOME/muxe`, cache proof under
        // `$XDG_CACHE_HOME/muxe`. Exact-path equality (not prefix
        // containment) proves the SAME registry `init` wrote.
        let case = case_dir("childfs");
        let scoped = scoped_root(case.path());
        let host = OwnedZellijHost::prepare(&case.path().join("unused"), case.path(), "scope")
            .expect("prepare host");
        for dir in [
            scoped.join("config").join("muxe"),
            scoped.join("cache").join("muxe"),
        ] {
            std::fs::create_dir_all(&dir).expect("registry dir");
        }
        let script = case.path().join("registry-proof.sh");
        std::fs::write(
            &script,
            "#!/bin/sh\n\
             set -u\n\
             : > \"$XDG_CONFIG_HOME/muxe/scope-proof-config\" || exit 3\n\
             : > \"$XDG_CACHE_HOME/muxe/scope-proof-cache\" || exit 3\n\
             [ -d \"$XDG_RUNTIME_DIR\" ] || exit 3\n",
        )
        .expect("write proof script");
        let mut permissions = std::fs::metadata(&script)
            .expect("stat proof")
            .permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).expect("chmod proof");
        let mut pairs = scoped_env_vec(&scoped);
        pairs.extend(host.host_env_overlay_vec());
        let mut command = Command::new(&script);
        command.env_clear();
        for (name, value) in &pairs {
            command.env(name, value);
        }
        command.current_dir(&scoped);
        let output = command.output().await.expect("run proof child");
        assert!(
            output.status.success(),
            "registry proof child failed (status {:?}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        for proof in [
            scoped
                .join("config")
                .join("muxe")
                .join("scope-proof-config"),
            scoped.join("cache").join("muxe").join("scope-proof-cache"),
        ] {
            assert!(
                proof.is_file(),
                "merged scope did not reach the shared registry at {}",
                proof.display(),
            );
        }
    }

    #[test]
    fn journal_hygiene_ignores_persistent_unit_lock_but_rejects_corrupt_journal() {
        let temp = tempfile::tempdir().expect("journal hygiene tempdir");
        let activation = muxe::lifecycle::journal::activation_dir(temp.path());
        std::fs::create_dir_all(&activation).expect("activation directory");
        std::fs::write(activation.join("herdr-test.lock"), b"").expect("persistent unit lock");
        assert_no_preserved_journals(temp.path(), "lock-only")
            .expect("persistent lock is not a preserved journal");

        std::fs::write(activation.join("herdr-test.json"), b"{").expect("corrupt journal");
        assert!(
            assert_no_preserved_journals(temp.path(), "corrupt").is_err(),
            "corrupt production journal must remain a hygiene failure"
        );
    }
}
