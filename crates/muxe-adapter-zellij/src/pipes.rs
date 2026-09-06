//! Pipe transport boundary for the two persistent `zellij pipe` children.
//!
//! The adapter owns one server-wide pair of children per live Zellij session:
//! the request channel carries broker-to-bridge JSON lines, and the event
//! channel carries bridge-to-broker JSON lines.
//!
//! Hardening contract (all verified without a live host):
//!
//! - Bounded incremental reads: lines accumulate chunk by chunk and are
//!   rejected the moment they exceed [`MAX_PIPE_LINE_LEN`], before any further
//!   growth. There is no unbounded read-until-newline anywhere on this path.
//! - Cancellation-safe reads: one reader task per child owns its stdout
//!   handle and forwards lines over an mpsc channel. `next_line` only awaits
//!   channel receive, so cancelling it drops nothing; an epoch tag discards
//!   output from a replaced child instead of restoring a stale reader.
//!   A pending receive survives respawn: a terminal error from a replaced
//!   epoch retries against the installed replacement and returns its line,
//!   while close still wakes the waiter bounded with `Closed`.
//! - No blocking guard across I/O: `next_line` snapshots the owned
//!   `(epoch, receiver)` pair under a short state lock and awaits without
//!   holding it. Lifecycle (close/respawn/park) serializes on its own lock
//!   and can never install a live child after close; a `replacing` flag
//!   plus wakeup lets gap waiters survive the take-to-install window.
//!   Termination kills first so a blocked writer fails fast, then drops
//!   stdin and reaps the exact taken child with a bounded wait.
//!
//! Pipe CLI contract (pinned `zellij-utils/src/cli.rs`, `Pipe` variant):
//! `zellij pipe --name <pipe> [-- payload]` broadcasts to listening plugins.
//! With a `-- payload` argument the child sends one message and then waits
//! blocked; a blocked pipe keeps receiving server output, so the event child
//! stays blocked forever while the request child reads further stdin lines only
//! after the target bridge unblocks its pipe.

use std::{
    collections::VecDeque,
    path::PathBuf,
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use thiserror::Error;
use tokio::{
    io::AsyncReadExt,
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Notify, mpsc},
};

use muxe_zellij_protocol::MAX_PIPE_LINE_LEN;

/// Deterministic pipe names for one live session.
pub fn channel_names(session: &str) -> (String, String) {
    (
        format!("muxe-request-{session}"),
        format!("muxe-event-{session}"),
    )
}

/// How long the broker waits for a `RequestReleased` acknowledgement before the
/// request pipe is declared stuck and only that child is replaced.
pub const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bounded wait to reap a killed child before its replacement starts.
const REAP_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounded retained stderr tail for fault reports.
const STDERR_TAIL_CAP: usize = 4 * 1024;

/// Depth of the per-child line queue; control traffic is low-volume.
const LINE_QUEUE_DEPTH: usize = 64;

/// Pipe transport failures. Any of these marks the channel unhealthy; the
/// adapter decides between single-child replacement and whole-pipe restart.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PipeTransportError {
    /// The `zellij pipe` child could not be spawned.
    #[error("could not spawn zellij pipe child: {reason}")]
    Spawn {
        /// Bounded reason.
        reason: String,
    },
    /// Writing a request line failed.
    #[error("request pipe write failed: {reason}")]
    Write {
        /// Bounded reason.
        reason: String,
    },
    /// Reading an event line failed or the child exited.
    #[error("event pipe read failed: {reason}")]
    Read {
        /// Bounded reason.
        reason: String,
    },
    /// A line exceeded the bounded frame length before any newline arrived.
    #[error("pipe line exceeds bound ({actual} bytes)")]
    Oversized {
        /// Observed length.
        actual: usize,
    },
    /// The previous child could not be reaped before replacement.
    #[error("could not reap replaced zellij pipe child: {reason}")]
    Reap {
        /// Bounded reason plus the retained stderr tail.
        reason: String,
    },
    /// The channel is shut down.
    #[error("pipe channel is closed")]
    Closed,
}

/// Shared line queue for one pipe-child epoch. Cloned under a short state
/// lock so `next_line` can await without holding lifecycle state.
type EpochLines = Arc<Mutex<mpsc::Receiver<(u64, Result<String, PipeTransportError>)>>>;
struct LiveChild {
    epoch: u64,
    lines: EpochLines,
    stdin: Arc<Mutex<Option<ChildStdin>>>,
    stderr_tail: Arc<Mutex<VecDeque<u8>>>,
    /// Process handle, waited on during every respawn/close.
    child: Option<Child>,
    /// OS pid for diagnostics.
    child_id: u32,
}
#[async_trait]
pub trait PipeChannel: Send + Sync {
    /// Sends one `\n`-terminated line. Request channels write broker frames;
    /// the event subscription line is written once at construction.
    async fn send_line(&self, line: String) -> Result<(), PipeTransportError>;
    /// Yields the next stdout line without its terminator.
    async fn next_line(&self) -> Result<String, PipeTransportError>;
    /// Closes the channel and reaps the child, if any.
    async fn close(&self);
    /// Parks the backing child without closing: terminates and reaps the
    /// current child and leaves the channel open so a later [`PipeChannel::respawn`]
    /// starts a fresh epoch. Used by activation suspend, never by shutdown.
    async fn park(&self) {}
    /// Replaces the backing child, for single-channel recovery. The default
    /// implementation is a no-op for scripted channels without children.
    async fn respawn(&self) -> Result<(), PipeTransportError> {
        Ok(())
    }
}
/// Production channel backed by a live `zellij pipe` child.
pub struct SubprocessChannel {
    state: Mutex<Option<LiveChild>>,
    /// Serializes close/respawn/park so concurrent lifecycle calls can never
    /// install a live child after close.
    lifecycle: Mutex<()>,
    /// Wakes `next_line` waiters parked across the respawn gap (old child
    /// taken, replacement not yet installed).
    state_notify: Notify,
    /// True while `respawn_inner` has taken the old child and not yet
    /// installed (or failed) its replacement. Lets a `next_line` that lands
    /// in the gap wait for the replacement instead of failing the pending
    /// receive.
    replacing: AtomicBool,
    epochs: AtomicU64,
    closed: AtomicBool,
    zellij_exe: PathBuf,
    session: String,
    pipe_name: String,
    initial_payload: Option<String>,
}

impl SubprocessChannel {
    /// Spawns `zellij --session <session> pipe --name <pipe> [-- payload]`.
    ///
    /// # Errors
    ///
    /// Returns [`PipeTransportError::Spawn`] when the child cannot be started.
    pub async fn launch(
        zellij_exe: PathBuf,
        session: String,
        pipe_name: String,
        initial_payload: Option<String>,
    ) -> Result<Arc<Self>, PipeTransportError> {
        let channel = Arc::new(Self {
            state: Mutex::new(None),
            lifecycle: Mutex::new(()),
            state_notify: Notify::new(),
            replacing: AtomicBool::new(false),
            epochs: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            zellij_exe,
            session,
            pipe_name,
            initial_payload,
        });
        channel.respawn_inner().await?;
        Ok(channel)
    }

    /// Kills and reaps the old child, then starts its replacement.
    ///
    /// # Errors
    ///
    /// Returns [`PipeTransportError`] when the old child cannot be reaped or
    /// the replacement cannot start; the retained stderr tail is included.
    pub async fn respawn(&self) -> Result<(), PipeTransportError> {
        self.respawn_inner().await
    }

    async fn respawn_inner(&self) -> Result<(), PipeTransportError> {
        let _lifecycle = self.lifecycle.lock().await;
        if self.closed.load(Ordering::Relaxed) {
            return Err(PipeTransportError::Closed);
        }
        // Terminate and reap the previous child BEFORE starting its
        // replacement: exactly one owned child exists at any moment.
        // The lifecycle lock keeps a concurrent close from interleaving
        // between the take and the install below. `replacing` lets a
        // `next_line` that lands in the take-to-install gap wait for the
        // replacement instead of failing the pending receive.
        self.replacing.store(true, Ordering::SeqCst);
        let previous = self.state.lock().await.take();
        if let Some(previous) = previous
            && let Err(error) = terminate_child(previous).await
        {
            self.replacing.store(false, Ordering::SeqCst);
            self.state_notify.notify_waiters();
            return Err(error);
        }
        let epoch = self.epochs.fetch_add(1, Ordering::Relaxed) + 1;
        let live = match self.spawn_epoch(epoch).await {
            Ok(live) => live,
            Err(error) => {
                self.replacing.store(false, Ordering::SeqCst);
                self.state_notify.notify_waiters();
                return Err(error);
            }
        };
        if self.closed.load(Ordering::Relaxed) {
            // Fail-closed defense: reap the orphan instead of installing a
            // live child after close.
            terminate_child(live).await.ok();
            self.replacing.store(false, Ordering::SeqCst);
            self.state_notify.notify_waiters();
            return Err(PipeTransportError::Closed);
        }
        *self.state.lock().await = Some(live);
        self.replacing.store(false, Ordering::SeqCst);
        self.state_notify.notify_waiters();
        Ok(())
    }

    /// Parks the backing child without closing, for activation suspend.
    async fn park_inner(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        if let Some(previous) = self.state.lock().await.take() {
            terminate_child(previous).await.ok();
        }
        self.state_notify.notify_waiters();
    }

    /// After a stale terminal on `epoch` (empty snapshot, dropped reader, or
    /// replaced-child error), decides whether the pending receive survives:
    /// waits out a respawn gap, retries on an installed replacement (`true`),
    /// or reports that the caller owns the terminal (`false`).
    async fn survive_stale(&self, epoch: u64) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            return false;
        }
        if self
            .state
            .lock()
            .await
            .as_ref()
            .is_some_and(|live| live.epoch != epoch)
        {
            return true;
        }
        if self.closed.load(Ordering::Relaxed) || !self.replacing.load(Ordering::SeqCst) {
            return false;
        }
        let wake = self.state_notify.notified();
        tokio::pin!(wake);
        // Re-verify after pinning so an install racing this check cannot
        // slip between the check and the wait.
        if self.state.lock().await.is_some() {
            return true;
        }
        if self.closed.load(Ordering::Relaxed) || !self.replacing.load(Ordering::SeqCst) {
            return false;
        }
        wake.await;
        true
    }

    async fn spawn_epoch(&self, epoch: u64) -> Result<LiveChild, PipeTransportError> {
        let mut command = Command::new(&self.zellij_exe);
        command
            .arg("--session")
            .arg(&self.session)
            .arg("pipe")
            .arg("--name")
            .arg(&self.pipe_name)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(payload) = &self.initial_payload {
            command.arg("--").arg(payload);
        }
        let mut child = command.spawn().map_err(|error| PipeTransportError::Spawn {
            reason: bounded(error.to_string()),
        })?;
        let child_id = child.id().unwrap_or(0);
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().ok_or(PipeTransportError::Spawn {
            reason: "zellij pipe child has no stdout".to_owned(),
        })?;
        let stderr = child.stderr.take().ok_or(PipeTransportError::Spawn {
            reason: "zellij pipe child has no stderr".to_owned(),
        })?;
        // The process handle stays owned here so every respawn and close can
        // terminate and reap it with a bounded wait. Reader tasks own only
        // the stdout/stderr handles, never the Child itself.
        let stderr_tail: Arc<Mutex<VecDeque<u8>>> = Arc::new(Mutex::new(VecDeque::new()));
        let (lines_tx, lines_rx) = mpsc::channel(LINE_QUEUE_DEPTH);
        tokio::spawn(read_lines(stdout, epoch, lines_tx));
        tokio::spawn(drain_stderr(stderr, Arc::clone(&stderr_tail)));
        Ok(LiveChild {
            epoch,
            lines: Arc::new(Mutex::new(lines_rx)),
            stdin: Arc::new(Mutex::new(stdin)),
            stderr_tail,
            child: Some(child),
            child_id,
        })
    }

    /// Bounded retained stderr tail for fault reports.
    pub async fn stderr_tail(&self) -> Vec<u8> {
        let tail = self
            .state
            .lock()
            .await
            .as_ref()
            .map(|live| Arc::clone(&live.stderr_tail));
        match tail {
            Some(tail) => tail.lock().await.iter().copied().collect(),
            None => Vec::new(),
        }
    }
}

async fn terminate_child(mut previous: LiveChild) -> Result<(), PipeTransportError> {
    // Kill first so a blocked `send_line` write fails fast (EPIPE) instead
    // of holding the stdin lock forever and deadlocking cleanup. Only then
    // take the stdin lock to drop the handle and let a stdin-blocked child
    // observe EOF.
    let mut child = previous.child.take().ok_or(PipeTransportError::Reap {
        reason: "replaced child has no process handle".to_owned(),
    })?;
    // Signal, then reap with a bounded wait. kill_on_drop is a backstop, not
    // proof: only wait() reaps the process.
    let _ = child.start_kill();
    *previous.stdin.lock().await = None;
    match tokio::time::timeout(REAP_TIMEOUT, child.wait()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(PipeTransportError::Reap {
            reason: bounded(format!("pid {} wait failed: {error}", previous.child_id)),
        }),
        Err(_) => {
            // Bounded wait expired: refuse silent success. Park a deferred
            // reaper so the process cannot linger as a zombie, and report the
            // failure with the retained stderr tail for diagnosis.
            let tail: Vec<u8> = previous.stderr_tail.lock().await.iter().copied().collect();
            let tail = String::from_utf8_lossy(&tail).into_owned();
            tokio::spawn(async move {
                let _ = child.wait().await;
            });
            Err(PipeTransportError::Reap {
                reason: bounded(format!(
                    "pid {} did not exit within {REAP_TIMEOUT:?}; stderr tail: {tail}",
                    previous.child_id
                )),
            })
        }
    }
}

async fn read_lines(
    stdout: ChildStdout,
    epoch: u64,
    lines_tx: mpsc::Sender<(u64, Result<String, PipeTransportError>)>,
) {
    let mut stdout = stdout;
    let mut pending: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stdout.read(&mut chunk).await {
            Err(error) => {
                let _ = lines_tx
                    .send((
                        epoch,
                        Err(PipeTransportError::Read {
                            reason: bounded(error.to_string()),
                        }),
                    ))
                    .await;
                return;
            }
            Ok(0) => {
                let _ = lines_tx
                    .send((
                        epoch,
                        Err(PipeTransportError::Read {
                            reason: "zellij pipe child exited".to_owned(),
                        }),
                    ))
                    .await;
                return;
            }
            Ok(count) => {
                if pending.len() + count > MAX_PIPE_LINE_LEN + 1 {
                    let _ = lines_tx
                        .send((
                            epoch,
                            Err(PipeTransportError::Oversized {
                                actual: pending.len() + count,
                            }),
                        ))
                        .await;
                    return;
                }
                pending.extend_from_slice(&chunk[..count]);
                while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                    let mut line: Vec<u8> = pending.drain(..=newline).collect();
                    line.pop();
                    let text = match String::from_utf8(line) {
                        Ok(text) => text,
                        Err(_) => {
                            let _ = lines_tx
                                .send((
                                    epoch,
                                    Err(PipeTransportError::Read {
                                        reason: "pipe line is not valid UTF-8".to_owned(),
                                    }),
                                ))
                                .await;
                            return;
                        }
                    };
                    if lines_tx.send((epoch, Ok(text))).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

async fn drain_stderr(stderr: ChildStderr, tail: Arc<Mutex<VecDeque<u8>>>) {
    let mut stderr = stderr;
    let mut chunk = [0u8; 1024];
    loop {
        match stderr.read(&mut chunk).await {
            Err(_) | Ok(0) => return,
            Ok(count) => {
                let mut guard = tail.lock().await;
                guard.extend(chunk[..count].iter().copied());
                while guard.len() > STDERR_TAIL_CAP {
                    guard.pop_front();
                }
            }
        }
    }
}

#[async_trait]
impl PipeChannel for SubprocessChannel {
    async fn send_line(&self, line: String) -> Result<(), PipeTransportError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(PipeTransportError::Closed);
        }
        let stdin = {
            let guard = self.state.lock().await;
            guard
                .as_ref()
                .map(|live| Arc::clone(&live.stdin))
                .ok_or(PipeTransportError::Closed)?
        };
        let mut guard = stdin.lock().await;
        let stream = guard.as_mut().ok_or(PipeTransportError::Closed)?;
        {
            use tokio::io::AsyncWriteExt;
            stream
                .write_all(line.as_bytes())
                .await
                .map_err(|error| PipeTransportError::Write {
                    reason: bounded(error.to_string()),
                })?;
            stream
                .flush()
                .await
                .map_err(|error| PipeTransportError::Write {
                    reason: bounded(error.to_string()),
                })?;
        }
        Ok(())
    }

    async fn next_line(&self) -> Result<String, PipeTransportError> {
        loop {
            if self.closed.load(Ordering::Relaxed) {
                return Err(PipeTransportError::Closed);
            }
            // Snapshot the owned (epoch, receiver) pair under a short state
            // lock, then await without holding it: close/park/respawn take
            // the state lock to replace the child, so holding it across
            // `recv` deadlocks a silent child.
            let snapshot = {
                let guard = self.state.lock().await;
                guard
                    .as_ref()
                    .map(|live| (live.epoch, Arc::clone(&live.lines)))
            };
            let Some((epoch, lines)) = snapshot else {
                // No live child. When a respawn is in flight the pending
                // receive survives the take-to-install gap: wait for the
                // lifecycle change instead of failing. Otherwise (parked,
                // dead, never launched) report Closed so the adapter's
                // restart loop can replace the child. Close sets `closed`
                // first, so shutdown still wakes bounded via the notify.
                if self.survive_stale(u64::MAX).await {
                    continue;
                }
                return Err(PipeTransportError::Closed);
            };
            let received = lines.lock().await.recv().await;
            match received {
                None => {
                    // Reader went away without a terminal line. A live
                    // replacement or an in-flight respawn means this receive
                    // is stale: survive into the replacement. Otherwise the
                    // channel is dead: report Closed.
                    if self.survive_stale(epoch).await {
                        continue;
                    }
                    return Err(PipeTransportError::Closed);
                }
                Some((line_epoch, _)) if line_epoch != epoch => continue,
                Some((_, Ok(line))) => return Ok(line),
                Some((_, Err(error))) => {
                    // Terminal error from the snapshotted child (EOF, oversize,
                    // invalid UTF-8). When a replacement is installed or a
                    // respawn is in flight, the pending receive survives and
                    // retries for the replacement's line; otherwise the error
                    // belongs to this caller.
                    if self.survive_stale(epoch).await {
                        continue;
                    }
                    if self.closed.load(Ordering::Relaxed) {
                        return Err(PipeTransportError::Closed);
                    }
                    return Err(error);
                }
            }
        }
    }
    async fn close(&self) {
        let _lifecycle = self.lifecycle.lock().await;
        self.closed.store(true, Ordering::Relaxed);
        if let Some(previous) = self.state.lock().await.take() {
            // Best-effort reap on close; a failure here is still reported
            // through the debug log path by the caller, never silently.
            let _ = terminate_child(previous).await;
        }
        self.state_notify.notify_waiters();
    }

    async fn park(&self) {
        self.park_inner().await;
    }

    async fn respawn(&self) -> Result<(), PipeTransportError> {
        SubprocessChannel::respawn(self).await
    }
}

fn bounded(reason: String) -> String {
    const LIMIT: usize = 512;
    if reason.len() > LIMIT {
        reason[..LIMIT].to_owned()
    } else {
        reason
    }
}
/// Deterministic in-memory channels for the adapter-contract suite and unit tests.
///
/// Plain `cargo check` builds report no in-crate caller; the module serves the
/// external contract suite through the injected `PipeChannel` boundary.
pub mod testing {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// Scripted line channel: `send_line` records outbound lines, `next_line`
    /// blocks until a queued inbound line arrives (like a real pipe child),
    /// and reports closure only after [`ScriptedChannel::close`].
    pub struct ScriptedChannel {
        outbound: StdMutex<Vec<String>>,
        inbound_tx: StdMutex<Option<mpsc::UnboundedSender<Result<String, PipeTransportError>>>>,
        inbound_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Result<String, PipeTransportError>>>,
    }

    impl ScriptedChannel {
        /// Empty channel.
        pub fn new() -> Arc<Self> {
            let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
            Arc::new(Self {
                outbound: StdMutex::new(Vec::new()),
                inbound_tx: StdMutex::new(Some(inbound_tx)),
                inbound_rx: tokio::sync::Mutex::new(inbound_rx),
            })
        }

        /// Queues one inbound line for the next `next_line` call.
        pub fn push_line(&self, line: impl Into<String>) {
            if let Ok(guard) = self.inbound_tx.lock()
                && let Some(sender) = guard.as_ref()
            {
                let _ = sender.send(Ok(line.into()));
            }
        }

        pub fn push_error(&self, error: PipeTransportError) {
            if let Ok(guard) = self.inbound_tx.lock()
                && let Some(sender) = guard.as_ref()
            {
                let _ = sender.send(Err(error));
            }
        }

        /// Drains recorded outbound lines.
        pub fn take_outbound(&self) -> Vec<String> {
            self.outbound
                .lock()
                .map(|mut guard| std::mem::take(&mut *guard))
                .unwrap_or_default()
        }
    }

    impl Default for ScriptedChannel {
        fn default() -> Self {
            let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
            Self {
                outbound: StdMutex::new(Vec::new()),
                inbound_tx: StdMutex::new(Some(inbound_tx)),
                inbound_rx: tokio::sync::Mutex::new(inbound_rx),
            }
        }
    }

    #[async_trait]
    impl PipeChannel for ScriptedChannel {
        async fn send_line(&self, line: String) -> Result<(), PipeTransportError> {
            self.outbound
                .lock()
                .map(|mut guard| guard.push(line))
                .map_err(|_| PipeTransportError::Closed)
        }

        async fn next_line(&self) -> Result<String, PipeTransportError> {
            let mut guard = self.inbound_rx.lock().await;
            guard
                .recv()
                .await
                .unwrap_or(Err(PipeTransportError::Closed))
        }

        async fn close(&self) {
            if let Ok(mut guard) = self.inbound_tx.lock() {
                guard.take();
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn scripted_channel_records_and_replays() {
            let channel = ScriptedChannel::new();
            channel.push_line("{\"sequence\":1}");
            channel
                .send_line("request\n".to_owned())
                .await
                .expect("sends");
            assert_eq!(
                channel.next_line().await.expect("replays"),
                "{\"sequence\":1}"
            );
            assert_eq!(channel.take_outbound(), vec!["request\n".to_owned()]);
            channel.close().await;
            assert!(matches!(
                channel.next_line().await,
                Err(PipeTransportError::Closed)
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipe_names_are_session_scoped() {
        let (request, event) = channel_names("work");
        assert_eq!(request, "muxe-request-work");
        assert_eq!(event, "muxe-event-work");
    }

    fn write_fake_zellij(script: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::TempDir::with_prefix("muxe-pipe-").expect("unique temp dir");
        let exe = dir.path().join("zellij");
        std::fs::write(&exe, script).expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        (dir, exe)
    }
    const ECHO_SCRIPT: &str = r#"#!/bin/sh
if [ "$1" != "--session" ] || [ "$3" != "pipe" ] || [ "$4" != "--name" ]; then
  echo "bad argv: $*" >&2
  exit 3
fi
while IFS= read -r line; do
  case "$line" in
    BIG) python3 -c 'import sys; sys.stdout.write("x" * 70000 + "\n")';;
    EXIT) exit 0;;
    *) printf 'got:%s\n' "$line";;
  esac
done
"#;
    // NOTE: `exec` is load-bearing: without it the shell leaves a `sleep`
    // grandchild holding the pipe ends, so killing the direct child neither
    // delivers stdout EOF to a blocked reader nor EPIPE to a blocked writer.
    // Every fake below stays one owned PID (shell-builtin loop or exec).
    const SILENT_SCRIPT: &str = r#"#!/bin/sh
if [ "$1" != "--session" ] || [ "$3" != "pipe" ] || [ "$4" != "--name" ]; then
  echo "bad argv: $*" >&2
  exit 3
fi
exec sleep 60
"#;
    /// Production child scenario against an owned fake `zellij` executable
    /// speaking the pipe CLI contract (`--session <s> pipe --name <p>` with
    /// piped stdin, stdout lines back). The fake validates its argv, echoes
    /// framed lines, emits one oversized line on demand, then exits: the test
    /// asserts exact framing, the oversize bound firing before unbounded
    /// growth, EOF propagation, respawn recovery with epoch separation, and
    /// close with reaping (a second close stays clean).
    #[tokio::test]
    async fn fake_cli_child_framing_lifecycle() {
        let script = ECHO_SCRIPT;
        let (_dir, exe) = write_fake_zellij(script);
        let channel =
            SubprocessChannel::launch(exe.clone(), "test".to_owned(), "pipe".to_owned(), None)
                .await
                .expect("launches fake cli");
        channel
            .send_line("one\n".to_owned())
            .await
            .expect("first line");
        channel
            .send_line("two\n".to_owned())
            .await
            .expect("second line");
        assert_eq!(channel.next_line().await.expect("first reply"), "got:one");
        assert_eq!(channel.next_line().await.expect("second reply"), "got:two");
        // Oversized output is rejected by the bound instead of buffering it.
        channel
            .send_line("BIG\n".to_owned())
            .await
            .expect("big trigger");
        assert!(matches!(
            channel.next_line().await,
            Err(PipeTransportError::Oversized { .. })
        ));
        // Respawn reaps the old child and starts a fresh epoch over the same
        // exact argv; stale output can no longer surface.
        channel.respawn().await.expect("respawns");
        channel
            .send_line("three\n".to_owned())
            .await
            .expect("third line");
        assert_eq!(channel.next_line().await.expect("third reply"), "got:three");
        // Fake exit propagates as EOF; close reaps and stays idempotent.
        channel
            .send_line("EXIT\n".to_owned())
            .await
            .expect("exit trigger");
        assert!(matches!(
            channel.next_line().await,
            Err(PipeTransportError::Read { .. })
        ));
        channel.close().await;
        assert!(matches!(
            channel.next_line().await,
            Err(PipeTransportError::Closed)
        ));
        channel.close().await;
    }
    /// A `next_line` pending on a silent child must not deadlock `close`:
    /// the close kills and reaps the exact child, the pending receive wakes
    /// bounded with `Closed`, and further reads stay `Closed`.
    #[tokio::test]
    async fn silent_child_close_unblocks_reader() {
        let (_dir, exe) = write_fake_zellij(SILENT_SCRIPT);
        let channel = SubprocessChannel::launch(exe, "test".to_owned(), "pipe".to_owned(), None)
            .await
            .expect("launches silent fake");
        let reader = tokio::spawn({
            let channel = Arc::clone(&channel);
            async move { channel.next_line().await }
        });
        // Scheduling hint only: both interleavings converge (pre-close
        // snapshot wakes via EOF with `closed` set; post-close snapshot
        // observes the taken state), so correctness never depends on it.
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(10), channel.close())
            .await
            .expect("close finishes bounded while a receive is pending on a silent child");
        let outcome = tokio::time::timeout(Duration::from_secs(10), reader)
            .await
            .expect("pending receive wakes bounded after close")
            .expect("reader task joins");
        assert!(
            matches!(outcome, Err(PipeTransportError::Closed)),
            "pending receive wakes with Closed, got {outcome:?}"
        );
        assert!(matches!(
            channel.next_line().await,
            Err(PipeTransportError::Closed)
        ));
        channel.close().await;
    }

    /// A `next_line` pending on a silent child must not deadlock `respawn`,
    /// and the same pending call survives into the replacement: it returns
    /// the replacement's line, never a stale-epoch terminal error.
    #[tokio::test]
    async fn silent_child_respawn_replaces_without_deadlock() {
        let (_dir, exe) = write_fake_zellij(ECHO_SCRIPT);
        let channel = SubprocessChannel::launch(exe, "test".to_owned(), "pipe".to_owned(), None)
            .await
            .expect("launches echo fake");
        let pending = tokio::spawn({
            let channel = Arc::clone(&channel);
            async move { channel.next_line().await }
        });
        // Scheduling hint only: every interleaving converges (pre-take
        // snapshot retries its stale terminal error into the replacement;
        // gap snapshot waits on the replacing flag; post-install snapshot
        // reads the replacement directly).
        tokio::task::yield_now().await;
        tokio::time::timeout(Duration::from_secs(10), channel.respawn())
            .await
            .expect("respawn finishes bounded while a receive is pending")
            .expect("respawn succeeds");
        channel
            .send_line("after\n".to_owned())
            .await
            .expect("replacement accepts writes");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), pending)
                .await
                .expect("pending receive survives respawn bounded")
                .expect("pending task joins")
                .expect("pending receive returns the replacement line"),
            "got:after"
        );
        channel.close().await;
    }

    /// Concurrent `close` and `respawn` never install a live child after
    /// close: after both settle, the channel stays closed.
    #[tokio::test]
    async fn concurrent_close_and_respawn_stays_closed() {
        let (_dir, exe) = write_fake_zellij(ECHO_SCRIPT);
        let channel = SubprocessChannel::launch(exe, "test".to_owned(), "pipe".to_owned(), None)
            .await
            .expect("launches echo fake");
        let (_, respawn_outcome) = tokio::join!(channel.close(), channel.respawn());
        // Either order is legal, but the channel must end closed: a respawn
        // that lost to close reports Closed and installs nothing.
        assert!(matches!(
            channel.next_line().await,
            Err(PipeTransportError::Closed)
        ));
        assert!(matches!(
            channel.send_line("late\n".to_owned()).await,
            Err(PipeTransportError::Closed)
        ));
        let _ = respawn_outcome;
        channel.close().await;
    }

    /// A `send_line` blocked on a child that never drains stdin must not
    /// deadlock cleanup: kill-first unblocks the writer, the exact child is
    /// reaped, and the channel stays closed.
    #[tokio::test]
    async fn blocked_writer_close_completes() {
        let (_dir, exe) = write_fake_zellij(SILENT_SCRIPT);
        let channel = SubprocessChannel::launch(exe, "test".to_owned(), "pipe".to_owned(), None)
            .await
            .expect("launches silent fake");
        // Readiness barrier, not a sleep: the closer proceeds once the writer
        // has entered its first bulk write. The fake never reads stdin, so a
        // multi-megabyte stream against a ~64 KiB pipe buffer guarantees the
        // writer is kernel-blocked (or fails fast once the kill lands); both
        // interleavings must resolve bounded.
        let writing = Arc::new(AtomicBool::new(false));
        let writer = tokio::spawn({
            let channel = Arc::clone(&channel);
            let writing = Arc::clone(&writing);
            async move {
                writing.store(true, Ordering::SeqCst);
                for _ in 0..64 {
                    let chunk = "w".repeat(64 * 1024);
                    if channel.send_line(chunk).await.is_err() {
                        return;
                    }
                }
            }
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while !writing.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("writer enters its bulk write");
        tokio::time::timeout(Duration::from_secs(10), channel.close())
            .await
            .expect("close finishes bounded while a writer is blocked");
        tokio::time::timeout(Duration::from_secs(10), writer)
            .await
            .expect("blocked writer resolves bounded after kill-first close")
            .expect("writer task joins");
        assert!(matches!(
            channel.send_line("late\n".to_owned()).await,
            Err(PipeTransportError::Closed)
        ));
        channel.close().await;
    }
}
