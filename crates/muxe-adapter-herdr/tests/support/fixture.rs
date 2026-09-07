use std::{
    cmp::min,
    fmt, io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::signal::{Signal, kill},
    unistd::Pid,
};

use muxe_adapter_api::HostIdentity;
use muxe_adapter_herdr::{HerdrSocketClient, probe_live_identity};
use tempfile::TempDir;
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_DELAY: Duration = Duration::from_millis(25);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const FORCEFUL_REAP_TIMEOUT: Duration = Duration::from_secs(2);
const DIAGNOSTIC_JOIN_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const MAX_UNIX_SOCKET_PATH: usize = 103;

/// A reviewed-only fixture for live adapter/CLI smoke tests. It starts exactly one child from an
/// absolute caller-supplied path and owns its temp directory, socket, diagnostics pipes, child
/// handle, and handshake identity. It never discovers or signals arbitrary processes.
pub struct OwnedHerdrFixture {
    _temp: TempDir,
    child: Option<Child>,
    socket: PathBuf,
    start_identity: Option<HostIdentity>,
    stdout: Option<JoinHandle<io::Result<PipeCapture>>>,
    stderr: Option<JoinHandle<io::Result<PipeCapture>>>,
}

struct PipeCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupDisposition {
    AlreadyExited,
    GracefulTermination,
    ForcedTermination,
}

#[derive(Debug)]
pub struct FixtureCleanup {
    pub disposition: CleanupDisposition,
    pub diagnostics: String,
}

pub enum FixtureStartupError {
    Setup(io::Error),
    Startup {
        error: io::Error,
        diagnostics: String,
    },
    /// The fixture remains owned by the caller when exact-child cleanup itself fails, so callers
    /// can retry `terminate_and_reap` rather than losing the only valid process handle.
    Cleanup {
        error: io::Error,
        cleanup: io::Error,
        diagnostics: String,
        fixture: Box<OwnedHerdrFixture>,
    },
}

impl fmt::Display for FixtureStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(error) => write!(formatter, "fixture setup failed: {error}"),
            Self::Startup { error, diagnostics } => {
                write!(formatter, "fixture startup failed: {error}; {diagnostics}")
            }
            Self::Cleanup {
                error,
                cleanup,
                diagnostics,
                fixture,
            } => write!(
                formatter,
                "fixture startup failed: {error}; cleanup failed: {cleanup}; retained fixture socket {}; {diagnostics}",
                fixture.socket.display()
            ),
        }
    }
}

impl OwnedHerdrFixture {
    pub async fn start(binary: PathBuf) -> Result<Self, FixtureStartupError> {
        if !binary.is_absolute() {
            return Err(FixtureStartupError::Setup(io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned Herdr fixture requires an absolute binary path",
            )));
        }
        if !binary.is_file() {
            return Err(FixtureStartupError::Setup(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "owned Herdr fixture binary is not a regular file: {}",
                    binary.display()
                ),
            )));
        }
        let temp = tempfile::tempdir().map_err(FixtureStartupError::Setup)?;
        // Keep the macOS Unix-domain path comfortably below its limit even when tempfile uses a
        // long system directory.
        let socket = temp.path().join("s");
        #[cfg(unix)]
        if socket.as_os_str().as_encoded_bytes().len() > MAX_UNIX_SOCKET_PATH {
            return Err(FixtureStartupError::Setup(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "owned Herdr fixture socket path is too long: {}",
                    socket.display()
                ),
            )));
        }
        let config_home = temp.path().join("config");
        let cache_home = temp.path().join("cache");
        let home = temp.path().join("home");
        std::fs::create_dir_all(&config_home).map_err(FixtureStartupError::Setup)?;
        std::fs::create_dir_all(&cache_home).map_err(FixtureStartupError::Setup)?;
        std::fs::create_dir_all(&home).map_err(FixtureStartupError::Setup)?;

        let mut child = Command::new(&binary)
            .arg("server")
            .env_clear()
            .env("HOME", &home)
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_CACHE_HOME", &cache_home)
            .env("HERDR_SOCKET_PATH", &socket)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(FixtureStartupError::Setup)?;
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let mut fixture = Self {
            _temp: temp,
            child: Some(child),
            socket,
            start_identity: None,
            stdout: stdout.map(|pipe| tokio::spawn(drain_pipe(pipe))),
            stderr: stderr.map(|pipe| tokio::spawn(drain_pipe(pipe))),
        };
        if fixture.stdout.is_none() || fixture.stderr.is_none() {
            return Err(fixture
                .fail_startup(io::Error::other(
                    "owned Herdr child did not retain both diagnostics pipes",
                ))
                .await);
        }
        match fixture.wait_for_handshake().await {
            Ok(identity) => {
                fixture.start_identity = Some(identity);
                Ok(fixture)
            }
            Err(error) => Err(fixture.fail_startup(error).await),
        }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Identity proved by a ping over this fixture's exact socket while its retained child was
    /// still running. It is never supplied by a caller.
    pub fn start_identity(&self) -> &HostIdentity {
        self.start_identity
            .as_ref()
            .expect("only a completed owned-child readiness handshake exposes fixture identity")
    }

    pub fn owned_child_pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    pub fn has_owned_child(&self) -> bool {
        self.child.is_some()
    }

    /// Terminates and reaps only the child created by `start`, preserving its handle if a cleanup
    /// operation itself fails. Returned diagnostics are collected before the `TempDir` can vanish.
    pub async fn terminate_and_reap(&mut self) -> io::Result<FixtureCleanup> {
        let child = self.child.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "owned Herdr child is already reaped",
            )
        })?;
        let disposition = if child.try_wait()?.is_none() {
            #[cfg(unix)]
            {
                let pid = child.id().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "owned Herdr child no longer has a process identifier",
                    )
                })?;
                let pid = i32::try_from(pid).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "owned Herdr child process identifier exceeds i32",
                    )
                })?;
                match kill(Pid::from_raw(pid), Signal::SIGTERM) {
                    Ok(()) | Err(Errno::ESRCH) => {}
                    Err(error) => {
                        return Err(io::Error::other(format!(
                            "could not send SIGTERM to the retained Herdr child: {error}"
                        )));
                    }
                }
            }
            #[cfg(not(unix))]
            child.start_kill()?;

            if let Ok(result) = timeout(GRACEFUL_SHUTDOWN_TIMEOUT, child.wait()).await {
                result?;
                CleanupDisposition::GracefulTermination
            } else {
                child.start_kill()?;
                timeout(FORCEFUL_REAP_TIMEOUT, child.wait())
                    .await
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "owned Herdr child did not reap after exact-child kill",
                        )
                    })??;
                CleanupDisposition::ForcedTermination
            }
        } else {
            CleanupDisposition::AlreadyExited
        };
        self.child = None;
        Ok(FixtureCleanup {
            disposition,
            diagnostics: self.collect_diagnostics().await,
        })
    }

    async fn wait_for_handshake(&mut self) -> io::Result<HostIdentity> {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        let client = HerdrSocketClient::new(&self.socket);
        loop {
            let child = self
                .child
                .as_mut()
                .expect("fixture retains its exact child until explicit reap");
            if let Some(status) = child.try_wait()? {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("owned Herdr child exited before ping readiness: {status}"),
                ));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned Herdr child did not complete ping readiness before startup deadline",
                ));
            }
            match timeout(remaining, probe_live_identity(&client)).await {
                Ok(Ok(identity)) => {
                    if identity.discovery_key != self.socket.display().to_string() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Herdr readiness identity is not bound to the fixture socket",
                        ));
                    }
                    return Ok(identity);
                }
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "owned Herdr child ping readiness handshake exceeded startup deadline",
                    ));
                }
                Ok(Err(error)) if error.kind == muxe_adapter_api::AdapterErrorKind::Unavailable => {
                    let retry = deadline
                        .saturating_duration_since(Instant::now())
                        .min(RETRY_DELAY);
                    if retry.is_zero() {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("owned Herdr child did not complete ping readiness: {error}"),
                        ));
                    }
                    tokio::select! {
                        () = sleep(retry) => {}
                        status = child.wait() => {
                            let status = status?;
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionAborted,
                                format!("owned Herdr child exited before ping readiness: {status}"),
                            ));
                        }
                    }
                }
                Ok(Err(error)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("owned Herdr child did not complete ping readiness: {error}"),
                    ));
                }
            }
        }
    }

    async fn fail_startup(mut self, error: io::Error) -> FixtureStartupError {
        match self.terminate_and_reap().await {
            Ok(cleanup) => FixtureStartupError::Startup {
                error,
                diagnostics: cleanup.diagnostics,
            },
            Err(cleanup) => {
                let diagnostics = self.collect_diagnostics().await;
                FixtureStartupError::Cleanup {
                    error,
                    cleanup,
                    diagnostics,
                    fixture: Box::new(self),
                }
            }
        }
    }

    async fn collect_diagnostics(&mut self) -> String {
        let stdout = collect_pipe(self.stdout.take()).await;
        let stderr = collect_pipe(self.stderr.take()).await;
        format!("stdout:\n{stdout}\nstderr:\n{stderr}")
    }
}

impl Drop for OwnedHerdrFixture {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            // Emergency unwind containment only. Startup and normal teardown explicitly await the
            // exact retained child; this never discovers a PID, searches a process name, or walks
            // a socket directory.
            let _ = child.start_kill();
        }
        if let Some(stdout) = self.stdout.take() {
            stdout.abort();
        }
        if let Some(stderr) = self.stderr.take() {
            stderr.abort();
        }
    }
}

async fn drain_pipe<R>(mut pipe: R) -> io::Result<PipeCapture>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::with_capacity(MAX_DIAGNOSTIC_BYTES);
    let mut truncated = false;
    let mut buffer = [0; 4096];
    loop {
        let read = pipe.read(&mut buffer).await?;
        if read == 0 {
            return Ok(PipeCapture { bytes, truncated });
        }
        let take = min(MAX_DIAGNOSTIC_BYTES.saturating_sub(bytes.len()), read);
        bytes.extend_from_slice(&buffer[..take]);
        truncated |= take != read;
    }
}

async fn collect_pipe(task: Option<JoinHandle<io::Result<PipeCapture>>>) -> String {
    let Some(mut task) = task else {
        return "<pipe unavailable>".to_owned();
    };
    match timeout(DIAGNOSTIC_JOIN_TIMEOUT, &mut task).await {
        Ok(Ok(Ok(capture))) => {
            let output = String::from_utf8_lossy(&capture.bytes);
            if capture.truncated {
                format!("{output}\n<diagnostic output truncated>")
            } else {
                output.into_owned()
            }
        }
        Ok(Ok(Err(error))) => format!("<could not read pipe: {error}>"),
        Ok(Err(error)) => format!("<pipe task failed: {error}>"),
        Err(_) => {
            task.abort();
            let _ = task.await;
            "<diagnostic capture timed out and was aborted>".to_owned()
        }
    }
}
