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
//! - No blocking guard across I/O: all production state uses
//!   `tokio::sync::Mutex`; writes serialize on the channel's own async lock
//!   and never block the reader task.
//! - Owned lifetime: respawn terminates and reaps the old child (bounded
//!   SIGKILL wait) before starting its replacement, and surfaces termination
//!   failures with the bounded stderr tail instead of succeeding silently.
//! - Both stdout (framed lines) and stderr (bounded diagnostic tail, never
//!   user payloads) are drained continuously.
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
    sync::{Mutex, mpsc},
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
struct LiveChild {
    lines: mpsc::Receiver<(u64, Result<String, PipeTransportError>)>,
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

    /// Replaces the backing child, for single-channel recovery. The default
    /// implementation is a no-op for scripted channels without children.
    async fn respawn(&self) -> Result<(), PipeTransportError> {
        Ok(())
    }
}

/// Production channel backed by a live `zellij pipe` child.
pub struct SubprocessChannel {
    state: Mutex<Option<LiveChild>>,
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
        if self.closed.load(Ordering::Relaxed) {
            return Err(PipeTransportError::Closed);
        }
        // Terminate and reap the previous child BEFORE starting its
        // replacement: exactly one owned child exists at any moment.
        let previous = self.state.lock().await.take();
        if let Some(previous) = previous {
            terminate_child(previous).await?;
        }
        let epoch = self.epochs.fetch_add(1, Ordering::Relaxed) + 1;
        let live = self.spawn_epoch(epoch).await?;
        *self.state.lock().await = Some(live);
        Ok(())
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
            lines: lines_rx,
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
    // Close stdin first so a child blocked reading stdin can observe EOF.
    *previous.stdin.lock().await = None;
    let mut child = previous.child.take().ok_or(PipeTransportError::Reap {
        reason: "replaced child has no process handle".to_owned(),
    })?;
    // Signal, then reap with a bounded wait. kill_on_drop is a backstop, not
    // proof: only wait() reaps the process.
    let _ = child.start_kill();
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
            let current_epoch = self.epochs.load(Ordering::Relaxed);
            let received = {
                let mut guard = self.state.lock().await;
                let live = guard.as_mut().ok_or(PipeTransportError::Closed)?;
                live.lines.recv().await
            };
            match received {
                None => return Err(PipeTransportError::Closed),
                Some((epoch, _)) if epoch != current_epoch => continue,
                Some((_, result)) => return result,
            }
        }
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        if let Some(previous) = self.state.lock().await.take() {
            // Best-effort reap on close; a failure here is still reported
            // through the debug log path by the caller, never silently.
            let _ = terminate_child(previous).await;
        }
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

    /// Production child scenario against an owned fake `zellij` executable
    /// speaking the pipe CLI contract (`--session <s> pipe --name <p>` with
    /// piped stdin, stdout lines back). The fake validates its argv, echoes
    /// framed lines, emits one oversized line on demand, then exits: the test
    /// asserts exact framing, the oversize bound firing before unbounded
    /// growth, EOF propagation, respawn recovery with epoch separation, and
    /// close with reaping (a second close stays clean).
    #[tokio::test]
    async fn fake_cli_child_framing_lifecycle() {
        let script = r#"#!/bin/sh
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
        let dir = std::env::temp_dir().join(format!("muxe-pipe-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let exe = dir.join("zellij");
        std::fs::write(&exe, script).expect("script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).expect("chmod");
        }
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
        std::fs::remove_dir_all(&dir).ok();
    }
}
