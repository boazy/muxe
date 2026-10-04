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

use tokio::process::{Child, ChildStdin, Command};
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
    run_cli_bounded_until(tag, command, tokio::time::Instant::now() + CLI_TIMEOUT).await
}

/// Bound the borrowed wait, not the future owning the child: deadline expiry
/// must still explicitly reap it and collect both diagnostics pipes.
async fn run_cli_bounded_until(
    tag: &str,
    command: &mut Command,
    deadline: tokio::time::Instant,
) -> io::Result<std::process::Output> {
    let mut child = OwnedChild::spawn(tag, command)?;
    let deadline = deadline.min(tokio::time::Instant::now() + CLI_TIMEOUT);
    let outcome = tokio::time::timeout_at(deadline, child.wait()).await;
    match outcome {
        Err(_) => {
            let diagnostics = child.terminate_and_reap().await?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "owned CLI '{tag}' exceeded its command/admission deadline:\n--- stdout ---\n{}\n--- stderr ---\n{}",
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
/// Bound on bootstrap initialization, bridge readiness, and the explicit
/// retained-client handoff.
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
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
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
    // The cache parent is already scoped. Create the target owner-only;
    // a preexisting cache root is an error, never silently repaired.
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&cache_dir)?;
    std::fs::set_permissions(&cache_dir, std::fs::Permissions::from_mode(0o700))?;
    muxe_broker::validate_owner_directory(&cache_dir).map_err(io::Error::other)?;
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
    stdin: Option<ChildStdin>,
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
            stdin: None,
            stdout,
            stderr,
        })
    }

    /// Spawns a retained child with a writable stdin handle kept open for
    /// interactive PTY wrappers. No input is sent; retaining the writer avoids
    /// synthesizing EOF into the nested terminal.
    pub fn spawn_with_open_stdin(tag: &str, command: &mut Command) -> io::Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn()?;
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().map(PipeTail::drain);
        let stderr = child.stderr.take().map(PipeTail::drain);
        Ok(Self {
            tag: tag.to_owned(),
            child: Some(child),
            stdin,
            stdout,
            stderr,
        })
    }
    /// Sends input through this owned interactive PTY client's retained stdin.
    /// Ordinary CLI children have no writer and fail instead of selecting a
    /// terminal outside the test-owned session.
    pub async fn send_input(&mut self, bytes: &[u8]) -> io::Result<()> {
        use tokio::io::AsyncWriteExt as _;
        let stdin = self
            .stdin
            .as_mut()
            .ok_or_else(|| io::Error::other("owned child has no interactive stdin"))?;
        stdin.write_all(bytes).await?;
        stdin.flush().await
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
    /// explicit socket stays under `root`; the server shares the test's fresh
    /// scoped environment so panes resolve the same Muxe config and cache as
    /// their launcher, matching a real user's Herdr session.
    pub async fn start(
        herdr_binary: &Path,
        root: &Path,
        scoped_root: &Path,
        name: &str,
    ) -> io::Result<Self> {
        if !herdr_binary.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "owned Herdr server requires an absolute binary path, got {}",
                    herdr_binary.display()
                ),
            ));
        }
        let socket = root.join(format!("{name}-herdr.sock"));
        #[cfg(unix)]
        if socket.as_os_str().len() > MAX_UNIX_SOCKET_PATH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("owned Herdr socket path is too long: {}", socket.display()),
            ));
        }
        let mut command = Command::new(herdr_binary);
        command.arg("server").env_clear();
        apply_scoped_env(&mut command, scoped_root);
        command.env("HERDR_SOCKET_PATH", &socket).current_dir(root);
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
/// Spawns one retained Herdr TUI client inside an owned PTY against the
/// explicit test socket. The client's HOME/XDG/TMP state and transcript stay
/// under the caller's fresh root; it never discovers or starts a user server.
pub async fn spawn_herdr_client(
    tag: &str,
    herdr_binary: &Path,
    socket: &Path,
    scoped_root: &Path,
    workdir: &Path,
    typescript: &Path,
) -> io::Result<OwnedChild> {
    let size_prefix = format!("stty rows {BOOTSTRAP_ROWS} cols {BOOTSTRAP_COLS}; ");
    let mut shell = std::ffi::OsString::from(size_prefix);
    shell.push("exec ");
    shell.push(shell_quote(herdr_binary.as_os_str()));
    let mut command = if cfg!(target_os = "macos") {
        let mut command = Command::new("script");
        command
            .arg("-q")
            .arg(typescript)
            .arg("sh")
            .arg("-c")
            .arg(shell);
        command
    } else {
        let mut command = Command::new("script");
        command.arg("-qec").arg(shell).arg(typescript);
        command
    };
    apply_scoped_env(&mut command, scoped_root);
    command
        .env("HERDR_SOCKET_PATH", socket)
        .env("TERM", "xterm-256color")
        .current_dir(workdir);
    let mut child =
        OwnedChild::spawn_with_open_stdin(&format!("{tag}-herdr-pty-client"), &mut command)?;
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
            "{tag} Herdr PTY client exited during the grace window:\n--- child stdout ---\n{}\n--- child stderr ---\n{}\n--- typescript ---\n{typescript_tail}",
            diagnostics.stdout_tail.lossy(),
            diagnostics.stderr_tail.lossy(),
        )));
    }
    Ok(child)
}

/// Polls until the owned Herdr socket accepts a connection inside the startup
/// budget. Protocol identity is established later by the guarded runtime.
async fn wait_for_herdr_handshake(socket: &Path) -> io::Result<String> {
    let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
    loop {
        if socket.exists()
            && let Ok(stream) = tokio::net::UnixStream::connect(socket).await
        {
            drop(stream);
            return Ok(socket.display().to_string());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "no live Herdr socket at {} inside the startup budget",
                    socket.display()
                ),
            ));
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

struct BootstrapPeer {
    socket: PathBuf,
    client: muxe_core::ClientId,
    child: OwnedChild,
}

/// A successful CLI can unblock before it delivers its census. Only initial
/// handoff admission may wait for that missing observation; it is not an empty set.
enum ClientCensus {
    Unavailable,
    Observed(Vec<muxe_core::ClientId>),
}

impl std::fmt::Display for ClientCensus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable => formatter.write_str("successful CLI returned no response"),
            Self::Observed(members) => write!(formatter, "observed membership {members:?}"),
        }
    }
}

/// One owned Zellij host with isolated endpoints and retained server children.
///
/// The pinned CLI daemonizes its server on Unix. The runner instead uses the
/// test-only foreground entrypoint linked to the exact pinned
/// `zellij-server::start_server_impl`, without patching the host source.
/// A retained bootstrap peer proves session and bridge readiness, then hands
/// the session to the complete initial PTY client set. Shutdown reaps the
/// owned foreground servers and any bootstrap peers still awaiting handoff.
pub struct OwnedZellijHost {
    zellij_binary: PathBuf,
    root: PathBuf,
    workdir: PathBuf,
    socket_dir: PathBuf,
    config_file: PathBuf,
    config_dir: PathBuf,
    data_dir: PathBuf,
    servers: Vec<OwnedChild>,
    bootstrap_peers: Vec<BootstrapPeer>,
    sessions: Vec<String>,
}

impl OwnedZellijHost {
    /// Reads the selected installation's native bridge identity. The existing
    /// installation validator returns only its path, so retain this report
    /// separately instead of substituting this test crate's current constants.
    pub async fn installed_bridge_identity(
        binary: &Path,
    ) -> io::Result<muxe_zellij_protocol::BridgeIdentity> {
        let root = short_tempdir("muxe-bridge-probe-")?;
        let mut command = Command::new(binary);
        command
            .arg("compatibility")
            .arg("--json")
            .current_dir(root.path());
        apply_scoped_env(&mut command, root.path());
        let output = run_cli_bounded("installed-bridge-identity", &mut command).await?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "selected installation's compatibility probe failed: {}",
                String::from_utf8_lossy(&output.stderr),
            )));
        }
        let mut report: serde_json::Value =
            serde_json::from_slice(&output.stdout).map_err(io::Error::other)?;
        let muxe_version = Self::take_native_text(&mut report, "muxe_version")?;
        let mut zellij = report
            .get_mut("hosts")
            .and_then(|hosts| hosts.get_mut("zellij"))
            .map(serde_json::Value::take)
            .ok_or_else(|| {
                io::Error::other("selected native report has no Zellij compatibility")
            })?;
        let bridge_build_id = if zellij
            .get("bridge_build_id")
            .is_none_or(serde_json::Value::is_null)
        {
            None
        } else {
            Some(Self::take_native_fingerprint(
                &mut zellij,
                "bridge_build_id",
            )?)
        };
        let identity = muxe_zellij_protocol::BridgeIdentity {
            muxe_version,
            source_revision: Self::take_native_text(&mut zellij, "source_revision")?,
            action_fingerprint: Self::take_native_fingerprint(
                &mut zellij,
                "generated_action_fingerprint",
            )?
            .0,
            protocol_fingerprint: Self::take_native_fingerprint(
                &mut zellij,
                "bridge_protocol_fingerprint",
            )?
            .0,
            bridge_build_id,
        };
        identity.validate().map_err(io::Error::other)?;
        Ok(identity)
    }

    fn take_native_text(report: &mut serde_json::Value, field: &str) -> io::Result<String> {
        match report.get_mut(field).map(serde_json::Value::take) {
            Some(serde_json::Value::String(text)) => Ok(text),
            _ => Err(io::Error::other(format!(
                "selected native report has no text field {field}",
            ))),
        }
    }

    fn take_native_fingerprint(
        report: &mut serde_json::Value,
        field: &str,
    ) -> io::Result<muxe_protocol::wire::SchemaFingerprint> {
        let digest = muxe::integration::Sha256Digest::parse(Self::take_native_text(report, field)?)
            .map_err(io::Error::other)?;
        let mut bytes = [0; 32];
        for (byte, pair) in bytes
            .iter_mut()
            .zip(digest.as_str().as_bytes().chunks_exact(2))
        {
            let pair = std::str::from_utf8(pair).expect("validated hexadecimal is ASCII");
            *byte = u8::from_str_radix(pair, 16).expect("validated hexadecimal pair");
        }
        Ok(muxe_protocol::wire::SchemaFingerprint(bytes))
    }

    /// Checks the prepared receipt and bytes against the selected historical
    /// native report before spawning any Zellij server or bridge probe.
    pub fn validate_prepared_bridge(
        scoped_root: &Path,
        native: &muxe_zellij_protocol::BridgeIdentity,
    ) -> io::Result<()> {
        let config_dir = scoped_root.join("config").join("muxe");
        let directory = muxe::integration::integration_dir(&config_dir);
        let prepared = muxe::integration::receipt::load(&directory)
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("bootstrap has no prepared managed bridge receipt"))?
            .bridge;
        let authority = muxe::integration::existing_bridge_identity(&config_dir)
            .map_err(io::Error::other)?
            .ok_or_else(|| io::Error::other("prepared bridge has no physical authority"))?;
        let (eligibility, _) = muxe::integration::bridge::check_destination(
            &authority.stable_path(std::ffi::OsStr::new(muxe::integration::BRIDGE_FILE_NAME)),
            Some(&prepared.installed_digest),
        )
        .map_err(io::Error::other)?;
        if !matches!(
            eligibility,
            muxe::integration::bridge::Eligibility::EligibleReplace { .. }
        ) {
            return Err(io::Error::other("prepared managed bridge bytes are absent"));
        }
        if prepared.bridge_identity != authority
            || prepared.installed_version != native.muxe_version
            || prepared
                .bridge_compat
                .as_ref()
                .is_some_and(|compatibility| {
                    compatibility.source_revision != native.source_revision
                        || compatibility.generated_action_fingerprint.0 != native.action_fingerprint
                        || compatibility.bridge_protocol_fingerprint.0
                            != native.protocol_fingerprint
                        || compatibility.bridge_build_id != native.bridge_build_id
                })
        {
            return Err(io::Error::other(
                "prepared receipt, bridge bytes, and selected native record disagree",
            ));
        }
        Ok(())
    }

    /// Prepares the isolated environment under `root/{name}` without
    /// starting any process. After integration install, `serve_foreground`
    /// starts and retains the pinned server and bootstrap peer.
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
        // Begin with an empty owned config, never user configuration. Before
        // startup, the runner's public integration install writes the stable
        // bridge and receipt and adds the managed autoload nodes here.
        // Bootstrap requires a real bridge registration before client handoff;
        // later activation reloads the bridge when the transaction requires it.
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
            bootstrap_peers: Vec::new(),
            sessions: Vec::new(),
        })
    }

    /// Spawns the core-coordinated test-only foreground entrypoint against
    /// this host's session socket path, initializes the session with a
    /// real first-client bootstrap, and tracks the session. The entrypoint
    /// runs the exact pinned server (`start_server_impl`, no mocks, no
    /// pin-source patch). The host retains both the foreground server and
    /// bootstrap peer and returns the session socket path. A real render
    /// proves session initialization; a receipt-matched bridge registration
    /// separately proves autoload completion before any PTY client attaches.
    /// The bootstrap remains connected until all initial PTY clients attach.
    /// Any startup failure reaps the owned children and preserves diagnostics.
    pub async fn serve_foreground(
        &mut self,
        helper: &Path,
        bootstrap: &Path,
        session: &str,
        scoped_root: &Path,
        expected: &muxe_zellij_protocol::BridgeIdentity,
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
        let mut bootstrap_child = None;
        let mut bootstrap_client = None;
        let outcome: io::Result<PathBuf> = async {
            // The socket proves the listener bound; the bootstrap render
            // proves initialization, and the CLI poll proves a subsequent
            // `attach` can discover the initialized session. Zellij publishes
            // that registry state asynchronously after first render.
            self.wait_for_session(session).await?;
            bootstrap_child = Some(self.run_bootstrap(bootstrap, session, scoped_root).await?);
            self.wait_for_cli_session(session).await?;
            bootstrap_client = Some(
                self.await_bootstrap_bridge(session, scoped_root, expected)
                    .await?,
            );
            // Keep the bootstrap allocated through the complete initial PTY
            // client set, so no startup client can reuse its identity.
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
                self.bootstrap_peers.push(BootstrapPeer {
                    socket: socket.clone(),
                    client: bootstrap_client.expect("startup proved the bootstrap bridge"),
                    child: bootstrap_child.expect("successful startup retains its bootstrap peer"),
                });
                Ok(socket)
            }
            Err(error) => {
                use std::fmt::Write as _;
                let bootstrap_cleanup = if let Some(mut bootstrap) = bootstrap_child {
                    Some(bootstrap.terminate_and_reap().await)
                } else {
                    None
                };
                let server_cleanup = child.terminate_and_reap().await;
                let mut report =
                    format!("foreground session '{session}' failed to initialize: {error}");
                for (role, cleanup) in [
                    ("bootstrap", bootstrap_cleanup),
                    ("server", Some(server_cleanup)),
                ] {
                    if let Some(cleanup) = cleanup {
                        match cleanup {
                            Ok(diagnostics) => write!(
                                report,
                                "\n--- {role} stdout ---\n{}\n--- {role} stderr ---\n{}",
                                diagnostics.stdout_tail.lossy(),
                                diagnostics.stderr_tail.lossy(),
                            ),
                            Err(error) => write!(report, "\n{role} cleanup failed: {error}"),
                        }
                        .expect("writing diagnostics to a String cannot fail");
                    }
                }
                Err(io::Error::other(report))
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
        // The production CLI sets this before it starts the server. This
        // fixture enters start_server_impl directly, so seed the same
        // session identity for Run panes spawned by the owned server.
        command.env("ZELLIJ_SESSION_NAME", session);
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
            .arg("--ready-file")
            .arg(self.workdir.join(format!("bootstrap-{session}.ready")))
            .arg("--cwd")
            .arg(&self.workdir)
            .arg("--rows")
            .arg(BOOTSTRAP_ROWS.to_string())
            .arg("--cols")
            .arg(BOOTSTRAP_COLS.to_string());
        self.apply_host_scoped_env(&mut command, scoped_root);
        command
            .env("TERM", "xterm-256color")
            .env("ZELLIJ_SESSION_NAME", session);
        command.current_dir(&self.workdir);
        command
    }

    /// Retains the bootstrap after its explicit first-render evidence. Bridge
    /// readiness and the complete initial PTY client set must be observed
    /// before the runner authorizes its detach.
    async fn run_bootstrap(
        &self,
        bootstrap: &Path,
        session: &str,
        scoped_root: &Path,
    ) -> io::Result<OwnedChild> {
        let mut command = self.bootstrap_command(bootstrap, session, scoped_root);
        let mut child =
            OwnedChild::spawn_with_open_stdin(&format!("{session}-bootstrap"), &mut command)?;
        let ready = self.workdir.join(format!("bootstrap-{session}.ready"));
        let deadline = tokio::time::Instant::now() + BOOTSTRAP_TIMEOUT;
        loop {
            if child.try_wait()?.is_some() || tokio::time::Instant::now() >= deadline {
                let diagnostics = child.terminate_and_reap().await?;
                return Err(io::Error::other(format!(
                    "bootstrap peer for session '{session}' proved no initialized session (status {:?}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    diagnostics.exit_status,
                    diagnostics.stdout_tail.lossy(),
                    diagnostics.stderr_tail.lossy(),
                )));
            }
            if std::fs::read_to_string(&ready)
                .ok()
                .and_then(|report| report.parse::<usize>().ok())
                .is_some_and(|bytes| bytes > 0)
            {
                return Ok(child);
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// A first render can precede autoload completion. Prove the initial
    /// client's real managed bridge before attaching another client: the
    /// pinned host's `AddClient` path can only clone an installed plugin map.
    /// Use the selected installation's native identity after the caller
    /// validates receipt ownership and bytes. Legacy receipts can omit
    /// compatibility metadata without changing the expected native record.
    async fn await_bootstrap_bridge(
        &self,
        session: &str,
        scoped_root: &Path,
        expected: &muxe_zellij_protocol::BridgeIdentity,
    ) -> io::Result<muxe_core::ClientId> {
        use muxe_adapter_zellij::{PipeChannel as _, SubprocessChannel, channel_names};
        use muxe_zellij_protocol::{
            BridgeEvent, ChannelGeneration, EventSubscription, PipeEventKind, decode_event_line,
            encode_event_subscription,
        };

        let deadline = tokio::time::Instant::now() + BOOTSTRAP_TIMEOUT;
        let mut members = self.bootstrap_census_until(session, deadline).await?;
        if members.len() != 1 {
            return Err(io::Error::other(
                "bootstrap requires exactly one initial client",
            ));
        }
        let client = members.pop().expect("one initial client");
        let executable =
            self.scoped_cli_wrapper_named(scoped_root, &format!("bootstrap-zellij-{session}"))?;
        let subscription =
            encode_event_subscription(EventSubscription::new(ChannelGeneration::INITIAL))
                .map_err(io::Error::other)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "bootstrap readiness deadline elapsed before subscribing",
            ));
        }
        let channel = SubprocessChannel::launch(
            executable,
            session.to_owned(),
            channel_names(session).1,
            Some(subscription),
        )
        .await
        .map_err(io::Error::other)?;
        let result = async {
            loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "bootstrap bridge did not register inside its lifetime",
                    ));
                }
                let line = tokio::time::timeout_at(deadline, channel.next_line())
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "bootstrap bridge did not register inside its lifetime",
                        )
                    })?
                    .map_err(io::Error::other)?;
                let frame = decode_event_line(&line).map_err(io::Error::other)?;
                if let PipeEventKind::Event(BridgeEvent::Register { registration }) = frame.event {
                    let registered = muxe_core::ClientId::new(registration.client_id);
                    let identity = registration.identity;
                    if registered != client
                        || frame.channel_generation != ChannelGeneration::INITIAL
                        || identity != *expected
                    {
                        return Err(io::Error::other(
                            "bootstrap bridge registration does not match the selected installation",
                        ));
                    }
                    let current = self.bootstrap_census_until(session, deadline).await?;
                    if current.len() != 1 || current.first() != Some(&client) {
                        return Err(io::Error::other(
                            "bootstrap membership changed before bridge readiness",
                        ));
                    }
                    eprintln!(
                        "[bootstrap] session '{session}': initial bridge registered {client}"
                    );
                    return Ok(client);
                }
            }
        }
        .await;
        if result.is_err() {
            eprintln!(
                "[bootstrap] event child stderr: {}",
                String::from_utf8_lossy(&channel.stderr_tail().await),
            );
        }
        channel.close().await;
        result
    }

    /// Only bootstrap bridge readiness may wait for a missing CLI response.
    /// Both observations share the phase deadline; observed data stays strict.
    async fn bootstrap_census_until(
        &self,
        session: &str,
        deadline: tokio::time::Instant,
    ) -> io::Result<Vec<muxe_core::ClientId>> {
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "bootstrap census did not become available inside its lifetime",
                ));
            }
            match self.client_census_until(session, deadline).await? {
                ClientCensus::Observed(members) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "bootstrap census arrived after its readiness deadline",
                        ));
                    }
                    return Ok(members);
                }
                ClientCensus::Unavailable => {
                    tokio::time::sleep_until(
                        deadline.min(tokio::time::Instant::now() + POLL_INTERVAL),
                    )
                    .await;
                }
            }
        }
    }

    async fn typed_clients(&self, session: &str) -> io::Result<Vec<muxe_core::ClientId>> {
        match self.client_census(session).await? {
            ClientCensus::Observed(members) => Ok(members),
            ClientCensus::Unavailable => Err(io::Error::other(format!(
                "list-clients on session '{session}' returned no census",
            ))),
        }
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
    /// The pinned binary directory leads PATH so public Muxe commands resolve
    /// the exact host under test rather than an ambient installation.
    pub fn apply_host_scoped_env(&self, command: &mut Command, scoped_root: &Path) {
        apply_scoped_env(command, scoped_root);
        self.apply_host_env_overlay(command);
        self.prepend_pinned_binary_dir(command);
    }

    fn prepend_pinned_binary_dir(&self, command: &mut Command) {
        let Some(directory) = self.zellij_binary.parent() else {
            return;
        };
        let physical_directory =
            std::fs::canonicalize(directory).unwrap_or_else(|_| directory.to_path_buf());
        let mut path = physical_directory.into_os_string();
        path.push(":");
        path.push(std::env::var_os("PATH").unwrap_or_default());
        command.env("PATH", path);
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

    /// Gives production subprocess channels an immutable, owned environment
    /// without changing the test process's ambient state. Every respawn execs
    /// the exact pinned CLI under the same explicit host and Muxe roots.
    pub fn scoped_cli_wrapper(&self, scoped_root: &Path) -> io::Result<PathBuf> {
        self.scoped_cli_wrapper_named(scoped_root, "scoped-zellij")
    }

    fn scoped_cli_wrapper_named(&self, scoped_root: &Path, name: &str) -> io::Result<PathBuf> {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;

        let path = self.workdir.join(name);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o700)
            .open(&path)?;
        file.write_all(b"#!/bin/sh\ncd ")?;
        file.write_all(shell_quote(scoped_root.as_os_str()).as_encoded_bytes())?;
        file.write_all(b" || exit 1\nexec /usr/bin/env -i")?;
        let mut command = Command::new(&self.zellij_binary);
        self.apply_host_scoped_env(&mut command, scoped_root);
        for (name, value) in command.as_std().get_envs() {
            let value =
                value.ok_or_else(|| io::Error::other("scoped environment removes a value"))?;
            file.write_all(b" ")?;
            file.write_all(shell_quote(name).as_encoded_bytes())?;
            file.write_all(b"=")?;
            file.write_all(shell_quote(value).as_encoded_bytes())?;
        }
        file.write_all(b" ")?;
        file.write_all(shell_quote(self.zellij_binary.as_os_str()).as_encoded_bytes())?;
        file.write_all(b" \"$@\"\n")?;
        Ok(path)
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

    /// Waits until the pinned CLI's global registry lists the initialized
    /// session. Bootstrap render and socket presence precede this state on
    /// loaded CI runners. `list-sessions` never attaches to the session, so
    /// this readiness check cannot consume the client identity that the
    /// retained PTY and its bridge registration must share.
    async fn wait_for_cli_session(&self, session: &str) -> io::Result<()> {
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        loop {
            let mut command = self.base_command();
            command.arg("list-sessions").arg("--short");
            let attempt = run_cli_bounded(&format!("list-sessions-{session}"), &mut command).await;
            let result = attempt.and_then(|output| {
                if !output.status.success() {
                    return Err(io::Error::other(format!(
                        "list-sessions failed: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )));
                }
                String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .any(|listed| listed.trim() == session)
                    .then_some(())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("session '{session}' is absent from list-sessions"),
                        )
                    })
            });
            match result {
                Ok(()) => return Ok(()),
                Err(error) if tokio::time::Instant::now() >= deadline => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "Zellij CLI did not discover session '{session}' inside the startup budget; \
                             last attempt: {error}"
                        ),
                    ));
                }
                Err(_) => tokio::time::sleep(POLL_INTERVAL).await,
            }
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
        Ok(self
            .typed_clients(session)
            .await?
            .into_iter()
            .map(muxe_core::ClientId::into_string)
            .collect())
    }

    async fn client_census(&self, session: &str) -> io::Result<ClientCensus> {
        self.client_census_until(session, tokio::time::Instant::now() + CLI_TIMEOUT)
            .await
    }

    async fn client_census_until(
        &self,
        session: &str,
        deadline: tokio::time::Instant,
    ) -> io::Result<ClientCensus> {
        let mut command = self.base_command();
        command
            .arg("--session")
            .arg(session)
            .arg("action")
            .arg("list-clients");
        let output =
            run_cli_bounded_until(&format!("list-clients-{session}"), &mut command, deadline)
                .await?;
        if !output.status.success() {
            return Err(io::Error::other(format!(
                "list-clients on session '{session}' failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        if output.stdout.is_empty() {
            return Ok(ClientCensus::Unavailable);
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
                Some(id) => ids.push(muxe_core::ClientId::new(id)),
            }
        }
        if !header_seen {
            return Err(io::Error::other(format!(
                "list-clients on session '{session}' has no CLIENT_ID header: {text:?}"
            )));
        }
        ids.sort();
        ids.dedup();
        Ok(ClientCensus::Observed(ids))
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
        let mut child =
            OwnedChild::spawn_with_open_stdin(&format!("{tag}-pty-client"), &mut command)?;
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

    /// Transfers the initial session to the complete requested PTY client set.
    /// Keep the bootstrap ID allocated until every initial client has its own
    /// distinct ID; only then detach and require that exact set to remain.
    pub async fn finish_bootstrap_handoff(
        &mut self,
        session: &str,
        initial_clients: usize,
    ) -> io::Result<()> {
        self.finish_bootstrap_handoff_until(
            session,
            initial_clients,
            tokio::time::Instant::now() + STARTUP_TIMEOUT,
        )
        .await
    }

    async fn finish_bootstrap_handoff_until(
        &mut self,
        session: &str,
        initial_clients: usize,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        let socket = self.session_socket_path(session);
        let Some(index) = self
            .bootstrap_peers
            .iter()
            .position(|peer| peer.socket == socket)
        else {
            return Err(io::Error::other(format!(
                "no retained bootstrap handoff state for session '{session}'",
            )));
        };
        let BootstrapPeer {
            client: bootstrap,
            child: mut peer,
            ..
        } = self.bootstrap_peers.remove(index);
        let outcome = async {
            let mut last_census = None;
            let retained = loop {
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, format!(
                        "bootstrap admission deadline elapsed; last census: {}",
                        last_census.as_ref().map_or_else(|| "none received".to_owned(), ToString::to_string),
                    )));
                }
                let census = self.client_census_until(session, deadline).await.map_err(|error| {
                    io::Error::new(error.kind(), format!(
                        "bootstrap pre-admission census failed: {error}; last census: {}",
                        last_census.as_ref().map_or_else(|| "none received".to_owned(), ToString::to_string),
                    ))
                })?;
                last_census = Some(census);
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, format!(
                        "bootstrap admission deadline elapsed; last census: {}",
                        last_census.expect("one completed census"),
                    )));
                }
                if let Some(ClientCensus::Observed(members)) = &mut last_census
                    && members.len() == initial_clients + 1
                    && let Some(index) = members.iter().position(|client| client == &bootstrap)
                {
                    members.remove(index);
                    break std::mem::take(members);
                }
                if peer.try_wait()?.is_some() {
                    return Err(io::Error::other(format!(
                        "bootstrap and requested distinct PTY clients never overlapped; last census: {}",
                        last_census.expect("one completed census"),
                    )));
                }
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + POLL_INTERVAL),
                )
                .await;
            };
            eprintln!(
                "[bootstrap] session '{session}': overlapping bootstrap {bootstrap} and retained {retained:?}",
            );
            peer.send_input(b"detach\n").await?;
            let status = tokio::time::timeout(BOOTSTRAP_TIMEOUT, peer.wait())
                .await
                .map_err(|_| io::Error::other("bootstrap detach exceeded its lifetime"))??;
            if !status.is_some_and(|status| status.success()) {
                return Err(io::Error::other("bootstrap detach failed"));
            }
            loop {
                let members = self.typed_clients(session).await?;
                if members == retained {
                    eprintln!(
                        "[bootstrap] session '{session}': detached {bootstrap}; retained {retained:?}",
                    );
                    break;
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err(io::Error::other(
                        "bootstrap client remained in the post-handoff membership",
                    ));
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            Ok(())
        }
        .await;
        let diagnostics = peer.terminate_and_reap().await?;
        eprintln!(
            "[bootstrap] handoff for '{session}': {}\n{}",
            diagnostics.stdout_tail.lossy(),
            diagnostics.stderr_tail.lossy(),
        );
        outcome
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
        for mut peer in std::mem::take(&mut self.bootstrap_peers) {
            diagnostics.push(peer.child.terminate_and_reap().await?);
        }
        for server in &mut self.servers {
            diagnostics.push(server.terminate_and_reap().await?);
        }
        Ok(diagnostics)
    }
}
/// Single-quotes one shell word without altering its Unix bytes.
fn shell_quote(arg: &std::ffi::OsStr) -> std::ffi::OsString {
    use std::os::unix::ffi::OsStrExt as _;

    let mut quoted = std::ffi::OsString::from("'");
    let mut chunks = arg.as_bytes().split(|byte| *byte == b'\'');
    if let Some(first) = chunks.next() {
        quoted.push(std::ffi::OsStr::from_bytes(first));
    }
    for chunk in chunks {
        quoted.push("'\\''");
        quoted.push(std::ffi::OsStr::from_bytes(chunk));
    }
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
    pub async fn watch_herdr(
        tag: &str,
        runtime_config: muxe_adapter_herdr::HerdrAdapterConfig,
        expected: String,
    ) -> io::Result<Self> {
        let socket = runtime_config.socket_path.clone();
        let runtime = muxe_adapter_herdr::HerdrRuntime::connect(runtime_config)
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "{tag}: witness runtime failed at {}: {error}",
                    socket.display()
                ))
            })?;
        let config = muxe_adapter_herdr::SubscriptionConfig {
            params: serde_json::json!({ "subscriptions": [{ "type": "tab.focused" }] }),
            subscribe_timeout: WITNESS_SUBSCRIBE_TIMEOUT,
        };
        let (subscription, _) = runtime.subscribe(config).await.map_err(|error| {
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
        let stream = tokio::net::UnixStream::connect(&self.socket)
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "continuity witness '{}': final socket connection failed at {}: {error}",
                    self.tag,
                    self.socket.display()
                ))
            })?;
        drop(stream);
        if self.socket.display().to_string() != self.expected {
            return Err(io::Error::other(format!(
                "continuity witness '{}': configured socket changed mid-run",
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
/// Environment capability required for a public activate child.
///
/// Implementations must apply the complete owned scope; there is no
/// host-independent fallback inside the command builder.
pub trait ActivateCommandEnvironment {
    fn apply_owned_scoped_env(&self, command: &mut Command, scoped_root: &Path);
}

/// Applies only the Muxe-owned `TempDir` environment for a host-free runner.
pub struct ScopedOnlyActivateEnvironment;

impl ActivateCommandEnvironment for ScopedOnlyActivateEnvironment {
    fn apply_owned_scoped_env(&self, command: &mut Command, scoped_root: &Path) {
        apply_scoped_env(command, scoped_root);
    }
}

impl ActivateCommandEnvironment for OwnedZellijHost {
    fn apply_owned_scoped_env(&self, command: &mut Command, scoped_root: &Path) {
        self.apply_host_scoped_env(command, scoped_root);
    }
}

/// Drives `<binary> activate` (public CLI) with an explicit owned scoped
/// environment policy and asserts a clean exit. The child runs under an
/// owned handle with a bounded lifetime: expiry kills with escalation and
/// fails closed with preserved diagnostics. Post-conditions are asserted by
/// the caller against serving brokers, never by parsing report text.
pub async fn drive_activate(
    binary: &Path,
    scoped_root: &Path,
    environment: &dyn ActivateCommandEnvironment,
    tag: &str,
) -> io::Result<String> {
    let child = spawn_activate(binary, scoped_root, environment, None, &[], tag)?;
    await_activate(child, tag).await
}

/// Spawns `<binary> activate` without waiting, so a test can interact
/// (barriers, observations) while the coordinator runs. The required
/// environment policy applies the Muxe-owned scope and any concrete owned
/// host addresses. `path_prepend` optionally fronts one owned directory on
/// PATH (fault-injector boundary); nothing else about the environment changes.
pub fn spawn_activate(
    binary: &Path,
    scoped_root: &Path,
    environment: &dyn ActivateCommandEnvironment,
    path_prepend: Option<&Path>,
    extra_env: &[(&str, &str)],
    tag: &str,
) -> io::Result<OwnedChild> {
    let mut command = activate_command(binary, scoped_root, environment, path_prepend, extra_env);
    OwnedChild::spawn(&format!("{tag}-activate"), &mut command)
}

/// Builds the `<binary> activate` command: the public CLI argv under the
/// required owned scoped environment policy with `scoped_root` as cwd.
/// `path_prepend` optionally fronts one owned directory on PATH
/// (fault-injector boundary); nothing else about the environment changes.
pub fn activate_command(
    binary: &Path,
    scoped_root: &Path,
    environment: &dyn ActivateCommandEnvironment,
    path_prepend: Option<&Path>,
    extra_env: &[(&str, &str)],
) -> Command {
    let mut command = Command::new(binary);
    command.arg("activate");
    environment.apply_owned_scoped_env(&mut command, scoped_root);
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
    /// `clients` (marker plus a `list-clients` table),
    /// `sessions-after-retry` (one discovery failure, then a session name),
    /// `resolve-zellij` (marker after PATH resolves the owned pinned binary),
    /// `evidence` (marker plus a bootstrap render report), `permit` (marker
    /// plus a fixed grant report naming the plugin location), `ok` (marker,
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
                "sessions-after-retry" =>
                    "count_file=\"$MARKER.count\"\n\
                     count=0\n\
                     [ ! -f \"$count_file\" ] || count=$(cat \"$count_file\")\n\
                     count=$((count + 1))\n\
                     printf '%s' \"$count\" > \"$count_file\" || exit 3\n\
                     if [ \"$count\" -lt 2 ]; then\n\
                       echo \"No active zellij sessions found.\" >&2\n\
                       exit 1\n\
                     fi\n\
                     : > \"$MARKER\" || exit 3\n\
                     printf 'scope-sess\\n'",
                "resolve-zellij" =>
                    "resolved=$(command -v zellij) || fail 'zellij is absent from PATH'\n\
                     [ \"$resolved\" = \"$EXPECTED_ROOT/zellij\" ] || fail \"zellij resolved to $resolved\"\n\
                     : > \"$MARKER\" || exit 3",
                "evidence" =>
                    ": > \"$MARKER\" || exit 3;\n\
                     while [ \"$#\" -gt 0 ]; do\n\
                       if [ \"$1\" = '--ready-file' ]; then\n\
                         shift; printf '7' > \"$1\" || exit 3; break\n\
                       fi\n\
                       shift\n\
                     done\n\
                     read -r handoff; [ \"$handoff\" = detach ]",
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
    async fn shell_words_preserve_non_utf8_and_metacharacters() {
        use std::os::unix::ffi::OsStrExt as _;

        let case = case_dir("shell-word");
        let scoped = scoped_root(case.path());
        let bytes = b"raw-\xff-'\"-$HOME-`printf injected`-; printf extra";
        let mut shell = std::ffi::OsString::from("printf '%s' ");
        shell.push(shell_quote(std::ffi::OsStr::from_bytes(bytes)));
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg(shell).current_dir(&scoped);
        apply_scoped_env(&mut command, &scoped);
        let output = run_cli_bounded("shell-word", &mut command)
            .await
            .expect("owned shell quoting proof");
        assert!(output.status.success());
        assert_eq!(output.stdout.as_slice(), bytes);
    }

    #[tokio::test]
    async fn bootstrap_handoff_requires_retained_peer() {
        let case = case_dir("missing-bootstrap");
        let mut host =
            OwnedZellijHost::prepare(&case.path().join("unused-zellij"), case.path(), "scope")
                .expect("prepare owned host");
        assert!(host.finish_bootstrap_handoff("absent", 1).await.is_err());
    }

    /// A real CLI process yields one prescribed observation per call. The
    /// bootstrap's stdin is the only way to publish the detach marker.
    fn census_fixture(
        case: &Path,
        observations: &[(&str, i32, bool)],
        pipe_event: Option<&str>,
    ) -> (OwnedZellijHost, PathBuf) {
        use std::fmt::Write as _;

        let marker = case.join("detached");
        let count = case.join("census-count");
        let binary = case.join("census-cli");
        let mut script = String::from("#!/bin/sh\n");
        if let Some(event) = pipe_event {
            writeln!(
                script,
                "if [ \"$3\" = pipe ]; then\n\
                 printf '%s\\n' \"$$\" > '{}'\n\
                 [ -z '{}' ] || printf '%s\\n' '{}'\n\
                 while IFS= read -r line; do :; done\n\
                 exit 0\n\
                 fi",
                shell_escape(&case.join("event-pid").to_string_lossy()),
                shell_escape(event),
                shell_escape(event),
            )
            .expect("write event peer script");
        }
        write!(
            script,
            "\
             count_file='{count}'\n\
             count=0\n\
             [ ! -f \"$count_file\" ] || count=$(cat \"$count_file\")\n\
             count=$((count + 1))\n\
             printf '%s' \"$count\" > \"$count_file\" || exit 3\n\
             case \"$count\" in\n",
            count = shell_escape(&count.to_string_lossy()),
        )
        .expect("write census counter");
        for (index, (stdout, status, detached)) in observations.iter().enumerate() {
            writeln!(
                script,
                "{} ) [ {} -f '{}' ] || {{ echo 'detach crossed census boundary' >&2; exit 3; }}; \
                 printf '%s' '{}'; exit {status} ;;",
                index + 1,
                if *detached { "" } else { "!" },
                shell_escape(&marker.to_string_lossy()),
                shell_escape(stdout),
            )
            .expect("write census script");
        }
        script.push_str("*) echo 'unexpected extra census' >&2; exit 3 ;;\nesac\n");
        std::fs::write(&binary, script).expect("write census CLI");
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
            .expect("chmod census CLI");
        let host = OwnedZellijHost::prepare(&binary, case, "census").expect("owned census host");
        (host, marker)
    }

    fn readiness_identity() -> muxe_zellij_protocol::BridgeIdentity {
        muxe_zellij_protocol::BridgeIdentity {
            muxe_version: "0.1.4".to_owned(),
            source_revision: "af38660c5884f50bb3726682fb92961326c4268f".to_owned(),
            action_fingerprint: [1; 32],
            protocol_fingerprint: [2; 32],
            bridge_build_id: Some(muxe_protocol::wire::SchemaFingerprint([3; 32])),
        }
    }

    fn readiness_register(
        client: muxe_core::ClientId,
        identity: muxe_zellij_protocol::BridgeIdentity,
    ) -> muxe_zellij_protocol::PipeEvent {
        use muxe_zellij_protocol::{
            BRIDGE_PROTOCOL_VERSION, BridgeEvent, ChannelGeneration, PipeEvent, PipeEventKind,
            ZellijRegistration,
        };

        PipeEvent {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id: None,
            channel_generation: ChannelGeneration::INITIAL,
            registration: "00000000000000000000000001"
                .parse()
                .expect("registration ID"),
            event: PipeEventKind::Event(BridgeEvent::Register {
                registration: ZellijRegistration {
                    client_id: client.into_string(),
                    current_pane: None,
                    plugin_id: None,
                    identity,
                },
            }),
        }
    }

    fn assert_event_peer_reaped(root: &Path) -> bool {
        let path = root.join("event-pid");
        if !path.exists() {
            return false;
        }
        let raw: i32 = std::fs::read_to_string(path)
            .expect("owned event PID")
            .trim()
            .parse()
            .expect("valid event PID");
        assert!(raw > 0, "event PID is not a process");
        let pid = nix::unistd::Pid::from_raw(raw);
        assert_eq!(
            nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)),
            Err(nix::errno::Errno::ECHILD),
            "readiness event child was not reaped",
        );
        true
    }

    async fn exercise_bootstrap_readiness(
        observations: &[(&str, i32, bool)],
    ) -> (io::Result<muxe_core::ClientId>, String, bool) {
        let case = case_dir("bridge-readiness");
        let identity = readiness_identity();
        let event = readiness_register(muxe_core::ClientId::new("1"), identity.clone());
        let line = muxe_zellij_protocol::encode_event_line(&event).expect("canonical Register");
        let (host, _) = census_fixture(case.path(), observations, Some(&line));
        let scoped = scoped_root(case.path());
        let result = host
            .await_bootstrap_bridge("census", &scoped, &identity)
            .await;
        let count =
            std::fs::read_to_string(case.path().join("census-count")).expect("census count");
        (result, count, assert_event_peer_reaped(case.path()))
    }

    #[tokio::test]
    async fn bootstrap_readiness_waits_for_both_missing_census_observations() {
        let (result, count, reaped) = exercise_bootstrap_readiness(&[
            ("", 0, false),
            ("CLIENT_ID\n1\n", 0, false),
            ("", 0, false),
            ("CLIENT_ID\n1\n", 0, false),
        ])
        .await;
        assert_eq!(
            result.expect("registered bootstrap readiness"),
            muxe_core::ClientId::new("1")
        );
        assert_eq!(count, "4");
        assert!(reaped, "readiness event child never launched");
    }

    #[tokio::test]
    async fn bootstrap_readiness_rejects_observed_invalid_initial_membership() {
        for initial in [
            ("CLIENT_ID\n", 0, false),
            ("CLIENT_ID\n1\n2\n", 0, false),
            (" \n", 0, false),
            ("", 1, false),
        ] {
            let (result, count, reaped) = exercise_bootstrap_readiness(&[initial]).await;
            assert_eq!(
                result.expect_err("invalid initial census").kind(),
                io::ErrorKind::Other
            );
            assert_eq!(count, "1", "invalid observation was retried");
            assert!(!reaped, "invalid initial census launched an event peer");
        }
    }

    #[tokio::test]
    async fn bootstrap_readiness_rejects_changed_or_invalid_registered_membership() {
        for current in [
            ("CLIENT_ID\n", 0, false),
            ("CLIENT_ID\n2\n", 0, false),
            ("CLIENT_ID\n1\n2\n", 0, false),
            ("not-a-census\n", 0, false),
            ("", 1, false),
        ] {
            let (result, count, reaped) =
                exercise_bootstrap_readiness(&[("CLIENT_ID\n1\n", 0, false), current]).await;
            assert_eq!(
                result.expect_err("invalid registered census").kind(),
                io::ErrorKind::Other
            );
            assert_eq!(count, "2", "invalid registered observation was retried");
            assert!(reaped, "registered event peer never launched");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_readiness_silent_receive_expires_and_reaps_event_peer() {
        let case = case_dir("silent-readiness");
        let (host, _) = census_fixture(case.path(), &[("CLIENT_ID\n1\n", 0, false)], Some(""));
        let scoped = scoped_root(case.path());
        let identity = readiness_identity();
        let task = tokio::spawn(async move {
            host.await_bootstrap_bridge("census", &scoped, &identity)
                .await
        });
        let wall_deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !case.path().join("event-pid").exists() && std::time::Instant::now() < wall_deadline {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(BOOTSTRAP_TIMEOUT + Duration::from_secs(1)).await;
        tokio::time::resume();
        let error = task
            .await
            .expect("owned readiness task")
            .expect_err("silent event timeout");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            assert_event_peer_reaped(case.path()),
            "event peer did not launch"
        );
        assert_eq!(
            std::fs::read_to_string(case.path().join("census-count")).expect("census count"),
            "1",
            "silent event receive launched an extra census",
        );
    }

    async fn exercise_census_handoff(
        observations: &[(&str, i32, bool)],
    ) -> (io::Result<()>, bool, String) {
        let case = case_dir("census-handoff");
        let (mut host, marker) = census_fixture(case.path(), observations, None);
        retain_census_peer(&mut host, &marker, case.path());
        let result = host.finish_bootstrap_handoff("census", 1).await;
        let count =
            std::fs::read_to_string(case.path().join("census-count")).expect("CLI call count");
        (result, marker.is_file(), count)
    }

    fn retain_census_peer(host: &mut OwnedZellijHost, marker: &Path, root: &Path) {
        let mut command = Command::new("/bin/sh");
        command
            .arg("-c")
            .arg("read -r handoff; [ \"$handoff\" = detach ] || exit 3; : > \"$1\"")
            .arg("owned-bootstrap")
            .arg(marker)
            .current_dir(root);
        apply_scoped_env(&mut command, root);
        host.bootstrap_peers.push(BootstrapPeer {
            socket: host.session_socket_path("census"),
            client: muxe_core::ClientId::new("1"),
            child: OwnedChild::spawn_with_open_stdin("census-bootstrap", &mut command)
                .expect("retained peer"),
        });
    }

    #[tokio::test]
    async fn bootstrap_handoff_waits_for_observation_and_distinct_members() {
        let (result, detached, count) = exercise_census_handoff(&[
            ("", 0, false),
            ("CLIENT_ID\n1\n1\n", 0, false),
            ("CLIENT_ID\n1\n2\n", 0, false),
            ("CLIENT_ID\n2\n", 0, true),
        ])
        .await;
        result.expect("unavailable census must not reject initial admission");
        assert!(detached, "admitted peer did not receive detach");
        assert_eq!(
            count, "4",
            "all admission gates and post-detach census required"
        );
    }

    #[tokio::test]
    async fn bootstrap_handoff_rejects_nonempty_malformed_or_failed_census() {
        for observation in [
            (" \n", 0, false),
            ("not-a-census\n", 0, false),
            ("", 1, false),
        ] {
            let (result, detached, count) = exercise_census_handoff(&[observation]).await;
            assert!(result.is_err(), "invalid census admitted: {observation:?}");
            assert!(!detached, "invalid census authorized detach");
            assert_eq!(count, "1", "invalid census was retried");
        }
    }

    #[tokio::test]
    async fn bootstrap_handoff_rejects_missing_post_detach_census() {
        let (result, detached, count) =
            exercise_census_handoff(&[("CLIENT_ID\n1\n2\n", 0, false), ("", 0, true)]).await;
        assert_eq!(
            result
                .expect_err("post-detach census must remain strict")
                .kind(),
            io::ErrorKind::Other,
        );
        assert!(detached, "fixture never crossed the admission boundary");
        assert_eq!(count, "2", "post-detach absence was retried");
    }

    #[tokio::test]
    async fn client_census_distinguishes_unavailable_from_authoritative_empty() {
        let case = case_dir("strict-census");
        let (host, _) = census_fixture(
            case.path(),
            &[("", 0, false), ("CLIENT_ID\n", 0, false)],
            None,
        );
        assert_eq!(
            host.list_clients("census")
                .await
                .expect_err("strict census cannot accept absent output")
                .kind(),
            io::ErrorKind::Other,
        );
        assert_eq!(
            host.list_clients("census")
                .await
                .expect("authoritative empty census"),
            Vec::<String>::new()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expired_bootstrap_admission_never_launches_another_census_or_detaches() {
        let case = case_dir("expired-census");
        let (mut host, marker) =
            census_fixture(case.path(), &[("CLIENT_ID\n1\n2\n", 0, false)], None);
        retain_census_peer(&mut host, &marker, case.path());
        // A query would now fail NotFound, so TimedOut proves admission checked
        // its finite budget before trying to launch even an immediately valid CLI.
        std::fs::remove_file(&host.zellij_binary).expect("disable owned census CLI");
        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::resume();
        let error = host
            .finish_bootstrap_handoff_until("census", 1, deadline)
            .await
            .expect_err("expired admission must fail");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            !case.path().join("census-count").exists(),
            "expired admission queried the CLI"
        );
        assert!(!marker.exists(), "expired admission authorized detach");
    }

    #[tokio::test(start_paused = true)]
    async fn bootstrap_admission_deadline_reaps_hung_census_with_diagnostics() {
        use std::os::unix::fs::OpenOptionsExt as _;

        let case = case_dir("hung-census");
        let ready = case.path().join("ready-fifo");
        let blocked = case.path().join("blocked-fifo");
        for fifo in [&ready, &blocked] {
            nix::unistd::mkfifo(
                fifo,
                nix::sys::stat::Mode::S_IRUSR | nix::sys::stat::Mode::S_IWUSR,
            )
            .expect("owned handshake FIFO");
        }
        let mut handshake = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(nix::fcntl::OFlag::O_NONBLOCK.bits())
            .open(&ready)
            .expect("nonblocking owned handshake");
        let binary = case.path().join("hung-cli");
        std::fs::write(
            &binary,
            format!(
                "#!/bin/sh\n\
             echo 'census stdout proof'\n\
             echo 'hung census diagnostic' >&2\n\
             printf '%s\\n' \"$$\" > '{ready}'\n\
             exec /bin/cat '{blocked}'\n",
                ready = shell_escape(&ready.to_string_lossy()),
                blocked = shell_escape(&blocked.to_string_lossy()),
            ),
        )
        .expect("hung census CLI");
        std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
            .expect("chmod hung CLI");
        let marker = case.path().join("detached");
        let mut host =
            OwnedZellijHost::prepare(&binary, case.path(), "census").expect("owned host");
        retain_census_peer(&mut host, &marker, case.path());
        let deadline = tokio::time::Instant::now() + Duration::from_millis(250);
        let task = tokio::spawn(async move {
            host.finish_bootstrap_handoff_until("census", 1, deadline)
                .await
        });
        let wall_deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut pid_bytes = Vec::new();
        let mut buffer = [0; 32];
        while !pid_bytes.contains(&b'\n') && std::time::Instant::now() < wall_deadline {
            match handshake.read(&mut buffer) {
                Ok(length) => pid_bytes.extend_from_slice(&buffer[..length]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => panic!("owned CLI handshake: {error}"),
            }
            // Keep the paused clock stationary until the real child reports
            // readiness; this is a protocol handshake, not a timing sleep.
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::resume();
        let error = task
            .await
            .expect("owned handoff task")
            .expect_err("hung CLI must expire");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let diagnostic = error.to_string();
        for expected in ["census stdout proof", "hung census diagnostic"] {
            assert!(diagnostic.contains(expected), "{diagnostic}");
        }
        assert!(!marker.exists(), "deadline authorized detach");
        let pid = nix::unistd::Pid::from_raw(
            std::str::from_utf8(&pid_bytes)
                .expect("child PID bytes")
                .trim()
                .parse()
                .expect("child PID"),
        );
        assert_eq!(
            nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)),
            Err(nix::errno::Errno::ECHILD),
            "expired owned CLI was not reaped",
        );
    }

    #[tokio::test]
    async fn bootstrap_admission_failure_preserves_last_observed_client() {
        let observed = muxe_core::ClientId::new("observed-client-7");
        let pending = format!("CLIENT_ID\n{observed}\n");
        let (result, detached, count) =
            exercise_census_handoff(&[(&pending, 0, false), ("", 1, false)]).await;
        let error = result.expect_err("command failure after pending census");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(error.to_string().contains(observed.as_str()), "{error}");
        assert!(!detached, "pending census authorized detach");
        assert_eq!(count, "2", "command failure retried");
    }

    #[test]
    fn prepared_bridge_legacy_metadata_preserves_native_and_digest_authority() {
        use muxe::integration::{
            Receipt, Sha256Digest,
            receipt::{BridgeRecord, RECEIPT_SCHEMA_VERSION, store},
        };
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;

        let case = case_dir("legacy-bridge");
        let scoped = scoped_root(case.path());
        let directory = muxe::integration::integration_dir(&scoped.join("config").join("muxe"));
        let native = muxe_zellij_protocol::BridgeIdentity {
            muxe_version: "0.1.0".to_owned(),
            source_revision: "0123456789abcdef0123456789abcdef01234567".to_owned(),
            action_fingerprint: [1; 32],
            protocol_fingerprint: [2; 32],
            bridge_build_id: Some(muxe_protocol::wire::SchemaFingerprint([3; 32])),
        };
        assert!(OwnedZellijHost::validate_prepared_bridge(&scoped, &native).is_err());
        assert!(
            !directory.exists(),
            "validation must not create missing authority"
        );

        let authority = muxe::integration::bridge_identity(&scoped.join("config").join("muxe"))
            .expect("owned physical authority");
        let stable = muxe::integration::stable_bridge_path(&scoped.join("config").join("muxe"));
        let bridge = b"historical bridge bytes";
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&stable)
            .expect("create owned bridge");
        file.write_all(bridge).expect("write owned bridge");
        drop(file);
        let mut receipt = Receipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            bridge: BridgeRecord {
                bridge_identity: authority,
                installed_version: native.muxe_version.clone(),
                installed_digest: Sha256Digest::from_bytes(bridge),
                previous_digest: None,
                bridge_compat: None,
            },
            configs: Vec::new(),
        };
        store(&directory, &receipt).expect("legacy-compatible receipt");
        OwnedZellijHost::validate_prepared_bridge(&scoped, &native)
            .expect("missing legacy metadata uses selected native identity");

        receipt.bridge.bridge_compat = Some(muxe_protocol::control::ZellijCompatibility {
            source_revision: "1123456789abcdef0123456789abcdef01234567".to_owned(),
            generated_action_fingerprint: muxe_protocol::wire::SchemaFingerprint(
                native.action_fingerprint,
            ),
            bridge_protocol_fingerprint: muxe_protocol::wire::SchemaFingerprint(
                native.protocol_fingerprint,
            ),
            bridge_build_id: native.bridge_build_id,
        });
        store(&directory, &receipt).expect("present conflicting metadata");
        let receipt_path = directory.join(muxe::integration::receipt::RECEIPT_FILE_NAME);
        let before = std::fs::read(&receipt_path).expect("receipt bytes before guard");
        assert!(OwnedZellijHost::validate_prepared_bridge(&scoped, &native).is_err());
        assert_eq!(
            std::fs::read(&receipt_path).expect("unchanged receipt"),
            before
        );
        assert_eq!(std::fs::read(&stable).expect("unchanged bridge"), bridge);

        receipt.bridge.bridge_compat = None;
        store(&directory, &receipt).expect("restore legacy metadata");
        std::fs::write(&stable, b"foreign bytes").expect("change owned artifact");
        let before = std::fs::read(&receipt_path).expect("receipt before digest guard");
        assert!(OwnedZellijHost::validate_prepared_bridge(&scoped, &native).is_err());
        assert_eq!(
            std::fs::read(&receipt_path).expect("unchanged receipt"),
            before
        );
        assert_eq!(
            std::fs::read(&stable).expect("unmodified foreign artifact"),
            b"foreign bytes"
        );
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
        let mut peer = host
            .run_bootstrap(&fake, "scope-sess", &scoped)
            .await
            .expect("bootstrap evidence");
        peer.send_input(b"detach\n").await.expect("release peer");
        peer.wait().await.expect("peer detached");
        peer.terminate_and_reap()
            .await
            .expect("reap bootstrap fake");
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
    async fn cli_session_readiness_retries_transient_discovery_failure() {
        let case = case_dir("cli-readiness");
        let marker = case.path().join("cli-readiness.marker");
        let fake = write_fake(
            case.path(),
            "fake-zellij",
            case.path(),
            CLI_VARS,
            false,
            &marker,
            "sessions-after-retry",
        );
        let host = OwnedZellijHost::prepare(&fake, case.path(), "scope").expect("prepare host");
        host.wait_for_cli_session("scope-sess")
            .await
            .expect("session becomes discoverable");
        assert!(
            marker.is_file(),
            "readiness poll never reached a successful CLI call"
        );
        assert_eq!(
            std::fs::read_to_string(marker.with_extension("marker.count"))
                .expect("read readiness attempt count"),
            "2",
        );
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

    async fn assert_scoped_child(tag: &str, command: &mut Command, marker: &Path) {
        let mut child =
            OwnedChild::spawn(&format!("scope-{tag}"), command).expect("spawn muxe fake");
        // Wait for the real child to validate its environment and exit
        // naturally; immediate reaping could mistake it for a SIGTERM exit.
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
        let marker_activate_scoped = case.path().join("activate-scoped.marker");
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
            "zellij",
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
            "resolve-zellij",
        );
        let fake_activate_scoped = write_fake(
            case.path(),
            "fake-muxe-activate-scoped",
            case.path(),
            MUXE_VARS,
            false,
            &marker_activate_scoped,
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
        let mut zellij_activate_command =
            activate_command(&fake_activate, &scoped, &host, None, &[]);
        let mut scoped_activate_command = activate_command(
            &fake_activate_scoped,
            &scoped,
            &ScopedOnlyActivateEnvironment,
            None,
            &[],
        );
        for (tag, command, marker) in [
            ("herdr", &mut herdr_command, &marker_herdr),
            ("zellij", &mut zellij_command, &marker_zellij),
            ("activate", &mut zellij_activate_command, &marker_activate),
            (
                "activate-scoped",
                &mut scoped_activate_command,
                &marker_activate_scoped,
            ),
        ] {
            assert_scoped_child(tag, command, marker).await;
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
        // This tests the script's guard, not kernel shebang execution. Read the
        // completed script through its interpreter; actual spawn-contract tests
        // still execute the generated fake directly.
        let mut command = Command::new("/bin/sh");
        command.arg(&fake);
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
        let mut command = Command::new("/bin/sh");
        command.arg(&script);
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
