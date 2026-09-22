//! Cross-process lock for the broker-observed Zellij registration epoch.
//!
//! The host can attach or detach clients independently. This gate serializes
//! Muxe's published registration and coverage state while the coordinator
//! seals one historical as-of proof; it does not lease physical membership.
//!
//! Lock order: coordinator unit lock → shared readiness gate → endpoint and
//! registry reads; broker exclusive gate → adapter registration transition →
//! client registry. Host CLI membership queries and pipe respawns never run
//! while the readiness gate is held.

use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::Arc;

use muxe_protocol::{AsOfTick, BridgeUnitId};
use nix::fcntl::{Flock, FlockArg};
use thiserror::Error;

const POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Error)]
pub enum ReadinessGateError {
    #[error("readiness gate path is unsafe: {0}")]
    UnsafePath(PathBuf),
    #[error("cannot access readiness gate at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("readiness gate lock at {path} failed: {source}")]
    Lock {
        path: PathBuf,
        source: nix::errno::Errno,
    },
    #[error("readiness gate at {0} remained contended past its deadline")]
    Contended(PathBuf),
    #[error("OS-wide monotonic clock is unavailable: {0}")]
    Clock(String),
}

/// One cache root and canonical bridge unit. The lock file is a sibling of the
/// cache directory so cache purge cannot replace its inode while brokers live.
#[derive(Clone, Debug)]
pub struct ReadinessGate {
    cache_dir: PathBuf,
    path: Option<PathBuf>,
    #[cfg(test)]
    _test_root: Option<Arc<tempfile::TempDir>>,
}

/// Kernel-owned shared or exclusive readiness publication guard.
#[derive(Debug)]
pub struct ReadinessGateGuard {
    _file: Flock<File>,
}

impl ReadinessGate {
    #[must_use]
    pub fn new(cache_dir: PathBuf, unit: BridgeUnitId) -> Self {
        let path = cache_dir.file_name().map(|file_name| {
            let mut name = file_name.to_os_string();
            name.push(".zellij-readiness-");
            name.push(unit.to_hex());
            name.push(".lock");
            cache_dir.with_file_name(name)
        });
        Self {
            cache_dir,
            path,
            #[cfg(test)]
            _test_root: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn temporary() -> Self {
        use std::os::unix::fs::PermissionsExt;
        let root = Arc::new(tempfile::tempdir().expect("temporary readiness gate"));
        let cache_dir = root.path().join("cache");
        fs::create_dir(&cache_dir).expect("temporary cache directory");
        fs::set_permissions(&cache_dir, fs::Permissions::from_mode(0o700))
            .expect("owner-only temporary cache");
        Self {
            _test_root: Some(root),
            ..Self::new(
                cache_dir,
                BridgeUnitId::from_canonical_bytes(b"adapter-test-bridge"),
            )
        }
    }

    fn path(&self) -> Result<&Path, ReadinessGateError> {
        self.path
            .as_deref()
            .ok_or_else(|| ReadinessGateError::UnsafePath(self.cache_dir.clone()))
    }

    fn open(&self, path: &Path) -> Result<File, ReadinessGateError> {
        let owner = nix::unistd::geteuid().as_raw();
        for (index, directory) in [
            self.cache_dir.as_path(),
            path.parent().expect("sibling has parent"),
        ]
        .into_iter()
        .enumerate()
        {
            let metadata =
                fs::symlink_metadata(directory).map_err(|source| ReadinessGateError::Io {
                    path: directory.to_path_buf(),
                    source,
                })?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != owner
                || (index == 0 && metadata.mode() & 0o777 != 0o700)
                || (index == 1 && metadata.mode() & 0o022 != 0)
            {
                return Err(ReadinessGateError::UnsafePath(directory.to_path_buf()));
            }
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
        let file = options
            .open(path)
            .map_err(|source| ReadinessGateError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        let opened = file.metadata().map_err(|source| ReadinessGateError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let named = fs::symlink_metadata(path).map_err(|source| ReadinessGateError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        if !opened.is_file()
            || named.file_type().is_symlink()
            || opened.uid() != owner
            || opened.mode() & 0o777 != 0o600
            || opened.nlink() != 1
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(ReadinessGateError::UnsafePath(path.to_path_buf()));
        }
        Ok(file)
    }

    async fn acquire(
        &self,
        mode: FlockArg,
        wait: Duration,
    ) -> Result<ReadinessGateGuard, ReadinessGateError> {
        let path = self.path()?;
        let mut file = self.open(path)?;
        let deadline = Instant::now() + wait;
        loop {
            match Flock::lock(file, mode) {
                Ok(locked) => return Ok(ReadinessGateGuard { _file: locked }),
                Err((returned, nix::errno::Errno::EWOULDBLOCK)) => {
                    if Instant::now() >= deadline {
                        return Err(ReadinessGateError::Contended(path.to_path_buf()));
                    }
                    file = returned;
                    tokio::time::sleep(
                        POLL.min(deadline.saturating_duration_since(Instant::now())),
                    )
                    .await;
                }
                Err((_, source)) => {
                    return Err(ReadinessGateError::Lock {
                        path: path.to_path_buf(),
                        source,
                    });
                }
            }
        }
    }

    /// Captures one timestamp comparable across all local broker processes.
    ///
    /// # Errors
    ///
    /// Refuses clock or tick conversion failures.
    pub fn as_of_now() -> Result<AsOfTick, ReadinessGateError> {
        let tick = nix::time::clock_gettime(nix::time::ClockId::CLOCK_MONOTONIC)
            .map_err(|error| ReadinessGateError::Clock(error.to_string()))?;
        let seconds = u64::try_from(tick.tv_sec())
            .map_err(|error| ReadinessGateError::Clock(error.to_string()))?;
        let nanos = u64::try_from(tick.tv_nsec())
            .map_err(|error| ReadinessGateError::Clock(error.to_string()))?;
        let millis = seconds
            .checked_mul(1000)
            .and_then(|value| value.checked_add(nanos / 1_000_000))
            .ok_or_else(|| ReadinessGateError::Clock("monotonic clock overflow".to_owned()))?;
        AsOfTick::from_millis(millis).map_err(|error| ReadinessGateError::Clock(error.to_string()))
    }

    /// Holds the published broker epoch stable while the coordinator gathers
    /// and seals its as-of proof. It releases before the blocking Ready fsync.
    ///
    /// # Errors
    ///
    /// Refuses unsafe owner paths, lock errors, and deadline contention.
    pub async fn shared(&self, wait: Duration) -> Result<ReadinessGateGuard, ReadinessGateError> {
        self.acquire(FlockArg::LockSharedNonblock, wait).await
    }

    /// Serializes a broker publication against the coordinator's shared proof.
    ///
    /// # Errors
    ///
    /// Refuses unsafe owner paths, lock errors, and deadline contention.
    pub async fn exclusive(
        &self,
        wait: Duration,
    ) -> Result<ReadinessGateGuard, ReadinessGateError> {
        self.acquire(FlockArg::LockExclusiveNonblock, wait).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_proof_blocks_exclusive_publication_until_release() {
        let gate = ReadinessGate::temporary();
        let proof = gate.shared(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(
            gate.exclusive(Duration::from_millis(30)).await,
            Err(ReadinessGateError::Contended(_))
        ));
        drop(proof);
        let publication = gate.exclusive(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(
            gate.shared(Duration::from_millis(30)).await,
            Err(ReadinessGateError::Contended(_))
        ));
        drop(publication);
        gate.shared(Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn refuses_symlinked_and_group_readable_lock_paths() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let gate = ReadinessGate::temporary();
        let path = gate.path().unwrap().to_path_buf();
        let foreign = gate.cache_dir.join("foreign");
        fs::write(&foreign, b"do not follow").unwrap();
        symlink(&foreign, &path).unwrap();
        assert!(gate.shared(Duration::from_millis(30)).await.is_err());
        assert_eq!(fs::read(&foreign).unwrap(), b"do not follow");
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            gate.shared(Duration::from_millis(30)).await,
            Err(ReadinessGateError::UnsafePath(_))
        ));
    }

    #[test]
    fn os_monotonic_tick_is_nonzero_and_ordered() {
        let first = ReadinessGate::as_of_now().unwrap();
        let second = ReadinessGate::as_of_now().unwrap();
        assert!(first.millis() > 0);
        assert!(second >= first);
    }
}
