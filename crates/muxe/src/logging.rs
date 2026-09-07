//! Persistent owner-only JSON Lines diagnostics.
//!
//! Every native Muxe process appends to `$CACHE_DIR/logs/muxe.jsonl`. The logs
//! directory is owner-only (`0700`) and every file is `0600`. Writers
//! coordinate rotation through an adjacent owner-only lock; rotation happens
//! before an append would exceed 1 MiB, retaining `muxe.jsonl` plus
//! `muxe.jsonl.1` through `muxe.jsonl.4`.
//!
//! Short-lived launchers append and flush synchronously before exit. Rotation
//! or sink failures fall back to stderr and fail closed for operations that
//! require an auditable failure path.
//!
//! Records carry identifiers, versions, fingerprints, bounded diagnostics, and
//! state transitions only. They never carry resolved command arguments,
//! injected environment values, terminal input, configuration scalar values,
//! or native-action payloads; the [`LogEvent`] shape has no fields for them.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::json;
#[cfg(test)]
use std::os::unix::fs::PermissionsExt;
use thiserror::Error;

use crate::fsutil::{self, FsError};

/// Maximum size of the active log file before rotation.
pub const MAX_LOG_BYTES: u64 = 1024 * 1024;
/// Number of rotated generations retained (`muxe.jsonl.1` .. `muxe.jsonl.4`).
pub const RETAINED_GENERATIONS: u8 = 4;
/// Upper bound for any single diagnostic message.
pub const MAX_MESSAGE_LEN: usize = 4 * 1024;

const LOG_FILE_NAME: &str = "muxe.jsonl";
const LOCK_FILE_NAME: &str = "muxe.jsonl.lock";

#[derive(Debug, Error)]
pub enum LogError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error("log message exceeds {MAX_MESSAGE_LEN} bytes")]
    MessageTooLong,
    #[error("log sink failed; record also written to stderr")]
    SinkFailed,
}

/// One auditable record.
///
/// `message` is bounded and must already exclude payloads listed in the
/// module documentation. `operation`, `host`, `version`, `request_id`, and
/// `code` are the only structured carriers besides the message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogEvent {
    pub version: String,
    pub host: String,
    pub operation: String,
    pub request_id: Option<String>,
    pub code: Option<String>,
    pub message: String,
}

impl LogEvent {
    /// Builds an event, rejecting oversized messages before any IO.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::MessageTooLong`] when `message` exceeds [`MAX_MESSAGE_LEN`] bytes.
    pub fn new(
        version: impl Into<String>,
        host: impl Into<String>,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Result<Self, LogError> {
        let message = message.into();
        if message.len() > MAX_MESSAGE_LEN {
            return Err(LogError::MessageTooLong);
        }
        Ok(Self {
            version: version.into(),
            host: host.into(),
            operation: operation.into(),
            request_id: None,
            code: None,
            message,
        })
    }

    #[must_use]
    pub fn with_request(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }
}

/// Synchronous owner-only JSON Lines writer for one cache directory.
#[derive(Debug)]
pub struct Logger {
    directory: PathBuf,
    file: PathBuf,
    lock: PathBuf,
    version: String,
}

impl Logger {
    /// Opens the logger for `cache_dir`, creating `$CACHE_DIR/logs/` owner-only.
    ///
    /// # Errors
    ///
    /// Returns [`LogError`] when the owner-only log directory cannot be created or validated.
    pub fn open(cache_dir: &Path, version: impl Into<String>) -> Result<Self, LogError> {
        let directory = cache_dir.join("logs");
        fsutil::ensure_owner_dir(&directory)?;
        Ok(Self {
            file: directory.join(LOG_FILE_NAME),
            lock: directory.join(LOCK_FILE_NAME),
            directory,
            version: version.into(),
        })
    }

    /// Returns the logger version stamped on every record.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Appends one record and flushes synchronously before returning.
    ///
    /// Rotation runs first when the append would exceed [`MAX_LOG_BYTES`].
    /// Any rotation or sink failure is also written to stderr and returned as
    /// an error so auditable operations fail closed.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::SinkFailed`] when rotation or the log sink fails.
    pub fn append(&self, event: &LogEvent) -> Result<(), LogError> {
        let record = json!({
            "timestamp": unix_timestamp(),
            "version": event.version,
            "host": event.host,
            "operation": event.operation,
            "request_id": event.request_id,
            "code": event.code,
            "message": event.message,
        });
        let mut line = serde_json::to_string(&record)
            .unwrap_or_else(|_| r#"{"message":"log encoding failed"}"#.to_owned());
        line.push('\n');
        if let Err(error) = self.append_bytes(line.as_bytes()) {
            eprintln!("muxe log failure: {error}");
            return Err(LogError::SinkFailed);
        }
        Ok(())
    }

    fn append_bytes(&self, line: &[u8]) -> Result<(), FsError> {
        let _lock = RotationLock::acquire(&self.lock)?;
        self.rotate_for(line.len() as u64)?;
        let mut file = fsutil::open_owner_file(&self.file, true)?;
        file.write_all(line)
            .map_err(|source| fsutil::io_error("writing log file", &self.file, source))?;
        file.sync_all()
            .map_err(|source| fsutil::io_error("synchronizing log file", &self.file, source))?;
        fsutil::sync_dir(&self.directory)?;
        Ok(())
    }

    fn rotate_for(&self, incoming: u64) -> Result<(), FsError> {
        let current = fsutil::owner_file_metadata(&self.file)?.map_or(0, |metadata| metadata.len());
        if current + incoming <= MAX_LOG_BYTES {
            return Ok(());
        }
        self.rotate()
    }

    fn rotate(&self) -> Result<(), FsError> {
        // Drop the oldest generation, then shift each generation up by one.
        let oldest = self.generation(RETAINED_GENERATIONS);
        if fsutil::owner_file_metadata(&oldest)?.is_some() {
            fs::remove_file(&oldest)
                .map_err(|source| fsutil::io_error("rotating log file", &oldest, source))?;
        }
        for generation in (1..=RETAINED_GENERATIONS).rev() {
            let source = if generation == 1 {
                self.file.clone()
            } else {
                self.generation(generation - 1)
            };
            if fsutil::owner_file_metadata(&source)?.is_none() {
                continue;
            }
            let target = self.generation(generation);
            if fsutil::owner_file_metadata(&target)?.is_some() {
                return Err(FsError::PathChanged { path: target });
            }
            fs::rename(&source, &target)
                .map_err(|source| fsutil::io_error("rotating log file", &target, source))?;
        }
        fsutil::sync_dir(&self.directory)
    }

    fn generation(&self, generation: u8) -> PathBuf {
        self.directory.join(format!("{LOG_FILE_NAME}.{generation}"))
    }
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

/// Process-wide advisory lock held across one complete rotation and append.
///
/// The lock inode is persistent: deleting a lock file on release lets a
/// concurrent writer acquire a different inode and defeats cross-process
/// exclusion. A malformed or unavailable lock fails the append closed.
#[derive(Debug)]
struct RotationLock {
    _file: fs::File,
}

impl RotationLock {
    fn acquire(path: &Path) -> Result<Self, FsError> {
        let file = fsutil::open_owner_file(path, false)?;
        file.lock()
            .map_err(|source| fsutil::io_error("locking log rotation", path, source))?;
        fsutil::verify_owner_file_descriptor(path, &file)?;
        Ok(Self { _file: file })
    }
}

/// Verifies that a file mode is owner-only `0600`; used by tests.
#[cfg(test)]
pub(crate) fn assert_owner_only(path: &Path) {
    let mode = fs::metadata(path)
        .unwrap_or_else(|_| panic!("missing {}", path.display()))
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "{}", path.display());
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        os::unix::fs::{PermissionsExt, symlink},
        process::Command,
    };

    use super::*;

    const CHILD_DIRECTORY: &str = "MUXE_LOG_TEST_DIRECTORY";
    const CHILD_ID: &str = "MUXE_LOG_TEST_CHILD_ID";
    const CHILD_RECORDS: usize = 400;

    fn test_logger(dir: &Path) -> Logger {
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).unwrap();
        Logger::open(dir, "0.1.0").unwrap()
    }

    fn event(message: &str) -> LogEvent {
        LogEvent::new("0.1.0", "zellij", "install", message).unwrap()
    }

    #[test]
    fn appends_json_lines_with_owner_only_mode() {
        let temp = tempfile::TempDir::new().unwrap();
        let logger = test_logger(temp.path());
        logger.append(&event("bridge installed")).unwrap();
        let log = temp.path().join("logs").join("muxe.jsonl");
        assert_owner_only(&log);
        let text = fs::read_to_string(&log).unwrap();
        let record: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(record["operation"], "install");
        assert_eq!(record["message"], "bridge installed");
        assert!(record["timestamp"].is_u64());
    }

    #[test]
    fn rotates_before_exceeding_one_mebibyte_and_retains_four() {
        let temp = tempfile::TempDir::new().unwrap();
        let logger = test_logger(temp.path());
        let message = "x".repeat(MAX_MESSAGE_LEN);
        for _ in 0..(usize::from(RETAINED_GENERATIONS) + 2) * 256 {
            logger.append(&event(&message)).unwrap();
        }
        let dir = temp.path().join("logs");
        for generation in 1..=RETAINED_GENERATIONS {
            let path = dir.join(format!("muxe.jsonl.{generation}"));
            assert!(path.exists(), "missing generation {generation}");
            assert_owner_only(&path);
        }
        let current = fs::metadata(dir.join("muxe.jsonl")).unwrap().len();
        assert!(current <= MAX_LOG_BYTES);
    }

    #[test]
    fn oversized_messages_are_rejected_before_io() {
        let long = "y".repeat(MAX_MESSAGE_LEN + 1);
        assert!(matches!(
            LogEvent::new("0.1.0", "herdr", "activate", long),
            Err(LogError::MessageTooLong)
        ));
    }

    #[test]
    fn zero_length_wrong_mode_log_fails_closed_before_append() {
        let temp = tempfile::TempDir::new().unwrap();
        let logger = test_logger(temp.path());
        let log = temp.path().join("logs").join(LOG_FILE_NAME);
        fs::write(&log, []).unwrap();
        fs::set_permissions(&log, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            logger.append(&event("must not append")),
            Err(LogError::SinkFailed)
        ));
        assert_eq!(fs::metadata(&log).unwrap().len(), 0);
    }

    #[test]
    fn symlinked_log_fails_without_touching_its_target() {
        let temp = tempfile::TempDir::new().unwrap();
        let logger = test_logger(temp.path());
        let target = temp.path().join("target.jsonl");
        fs::write(&target, "unrelated\n").unwrap();
        let log = temp.path().join("logs").join(LOG_FILE_NAME);
        symlink(&target, &log).unwrap();
        assert!(matches!(
            logger.append(&event("must not follow")),
            Err(LogError::SinkFailed)
        ));
        assert_eq!(fs::read_to_string(&target).unwrap(), "unrelated\n");
    }

    #[test]
    fn child_process_appends() {
        let Some(directory) = env::var_os(CHILD_DIRECTORY) else {
            return;
        };
        let child_id = env::var(CHILD_ID).unwrap();
        let logger = test_logger(Path::new(&directory));
        let message = format!(
            "{child_id}:{}",
            "x".repeat(MAX_MESSAGE_LEN - child_id.len() - 1)
        );
        for _ in 0..CHILD_RECORDS {
            logger.append(&event(&message)).unwrap();
        }
    }

    #[test]
    fn multiple_processes_rotate_without_corrupting_json_lines() {
        let temp = tempfile::TempDir::new().unwrap();
        let executable = env::current_exe().unwrap();
        let mut children = Vec::new();
        for child_id in 0..3 {
            children.push(
                Command::new(&executable)
                    .args([
                        "--exact",
                        "logging::tests::child_process_appends",
                        "--nocapture",
                    ])
                    .env(CHILD_DIRECTORY, temp.path())
                    .env(CHILD_ID, child_id.to_string())
                    .spawn()
                    .unwrap(),
            );
        }
        for mut child in children {
            assert!(child.wait().unwrap().success());
        }

        let directory = temp.path().join("logs");
        let paths = std::iter::once(directory.join(LOG_FILE_NAME)).chain(
            (1..=RETAINED_GENERATIONS)
                .map(|generation| directory.join(format!("{LOG_FILE_NAME}.{generation}"))),
        );
        let mut records = 0;
        for path in paths {
            assert_owner_only(&path);
            let text = fs::read_to_string(&path).unwrap();
            for line in text.lines() {
                let record: serde_json::Value = serde_json::from_str(line).unwrap();
                assert_eq!(record["operation"], "install");
                records += 1;
            }
        }
        assert!(records > 0);
    }
}
