use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::{
        fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
        net::UnixStream,
    },
    path::{Path, PathBuf},
};

use data_encoding::HEXLOWER;
use muxe_protocol::HostKind;
use nix::{
    errno::Errno,
    fcntl::{Flock, FlockArg},
    unistd::Uid,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::net::UnixListener;

const OWNER_DIRECTORY_MODE: u32 = 0o700;
const OWNER_FILE_MODE: u32 = 0o600;
const STARTUP_LOCK_MARKER: &[u8] = b"muxe-startup-lock-v2\n";
const MAX_SOCKET_PATH_BYTES: usize = 103;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeEndpoint {
    runtime_dir: PathBuf,
    socket: PathBuf,
    startup_lock: PathBuf,
}

impl RuntimeEndpoint {
    /// Derives the normal endpoint from ambient runtime state (production only;
    /// tests use [`RuntimeEndpoint::in_runtime_dir`]).
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError::SocketPathTooLong` when the derived socket path
    /// exceeds the platform limit.
    pub fn for_host(host: HostKind, discovery_key: &str) -> Result<Self, RuntimeError> {
        let uid = Uid::current().as_raw();
        let base = env::var_os("XDG_RUNTIME_DIR").map_or_else(
            || env::temp_dir().join(format!("muxe-{uid}")),
            PathBuf::from,
        );
        Self::in_runtime_dir(base, host, discovery_key)
    }

    /// Derives the normal endpoint under an explicit base directory.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError::SocketPathTooLong` when the derived socket path
    /// exceeds the platform limit.
    pub fn in_runtime_dir(
        runtime_base: impl Into<PathBuf>,
        host: HostKind,
        discovery_key: &str,
    ) -> Result<Self, RuntimeError> {
        let digest = HEXLOWER.encode(&Sha256::digest(discovery_key.as_bytes()));
        let host = match host {
            HostKind::Zellij => "z",
            HostKind::Herdr => "h",
        };
        let runtime_dir = runtime_base.into().join("muxe");
        let stem = format!("b-{host}-{}", &digest[..20]);
        let socket = runtime_dir.join(format!("{stem}.sock"));
        let startup_lock = runtime_dir.join(format!("{stem}.lock"));
        if socket.as_os_str().as_encoded_bytes().len() > MAX_SOCKET_PATH_BYTES {
            return Err(RuntimeError::SocketPathTooLong(socket));
        }
        Ok(Self {
            runtime_dir,
            socket,
            startup_lock,
        })
    }

    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Creates the owner-only runtime directory when absent.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError` when the directory cannot be created or validated.
    pub fn ensure_owner_directory(&self) -> Result<(), RuntimeError> {
        match fs::symlink_metadata(&self.runtime_dir) {
            Ok(metadata) => validate_owner_directory_metadata(&self.runtime_dir, &metadata),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                builder.recursive(true).mode(OWNER_DIRECTORY_MODE);
                builder
                    .create(&self.runtime_dir)
                    .map_err(|source| RuntimeError::Io {
                        path: self.runtime_dir.clone(),
                        source,
                    })?;
                let metadata =
                    fs::symlink_metadata(&self.runtime_dir).map_err(|source| RuntimeError::Io {
                        path: self.runtime_dir.clone(),
                        source,
                    })?;
                validate_owner_directory_metadata(&self.runtime_dir, &metadata)
            }
            Err(source) => Err(RuntimeError::Io {
                path: self.runtime_dir.clone(),
                source,
            }),
        }
    }

    /// Acquires the persistent owner-only advisory lock that serializes broker startups.
    ///
    /// A fully initialized, already-locked inode is hard-linked into place
    /// atomically. Old PID-file locks (including an empty file before its PID
    /// write) are never adopted: they cannot prove exclusive ownership.
    /// Neither this guard nor later callers unlink the published inode.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError` on contention or invalid path/ownership.
    pub fn acquire_startup_lock(&self) -> Result<StartupLock, RuntimeError> {
        self.ensure_owner_directory()?;
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(&self.startup_lock)
        {
            Ok(file) => return self.lock_existing(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(RuntimeError::Io {
                    path: self.startup_lock.clone(),
                    source,
                });
            }
        }
        let mut staged = tempfile::Builder::new()
            .prefix(".muxe-startup-")
            .tempfile_in(&self.runtime_dir)
            .map_err(|source| RuntimeError::Io {
                path: self.runtime_dir.clone(),
                source,
            })?;
        staged
            .write_all(STARTUP_LOCK_MARKER)
            .map_err(|source| RuntimeError::Io {
                path: self.startup_lock.clone(),
                source,
            })?;
        staged
            .as_file()
            .sync_all()
            .map_err(|source| RuntimeError::Io {
                path: self.startup_lock.clone(),
                source,
            })?;
        let file = staged
            .as_file()
            .try_clone()
            .map_err(|source| RuntimeError::Io {
                path: self.startup_lock.clone(),
                source,
            })?;
        let locked =
            Flock::lock(file, FlockArg::LockExclusiveNonblock).map_err(|(_, source)| {
                RuntimeError::Io {
                    path: self.startup_lock.clone(),
                    source: source.into(),
                }
            })?;
        match staged.persist_noclobber(&self.startup_lock) {
            Ok(_) => {
                self.validate_lock_identity(&locked)?;
                Ok(StartupLock { _file: locked })
            }
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                drop(locked);
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
                    .open(&self.startup_lock)
                    .map_err(|source| RuntimeError::Io {
                        path: self.startup_lock.clone(),
                        source,
                    })?;
                self.lock_existing(file)
            }
            Err(error) => Err(RuntimeError::Io {
                path: self.startup_lock.clone(),
                source: error.error,
            }),
        }
    }

    fn lock_existing(&self, mut file: File) -> Result<StartupLock, RuntimeError> {
        self.validate_lock_identity(&file)?;
        if file
            .metadata()
            .map_err(|source| RuntimeError::Io {
                path: self.startup_lock.clone(),
                source,
            })?
            .len()
            != STARTUP_LOCK_MARKER.len() as u64
        {
            return Err(RuntimeError::LegacyStartupLock(self.startup_lock.clone()));
        }
        let mut marker = [0; STARTUP_LOCK_MARKER.len()];
        file.read_exact(&mut marker)
            .map_err(|source| RuntimeError::Io {
                path: self.startup_lock.clone(),
                source,
            })?;
        if marker != STARTUP_LOCK_MARKER {
            return Err(RuntimeError::LegacyStartupLock(self.startup_lock.clone()));
        }
        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(locked) => {
                self.validate_lock_identity(&locked)?;
                Ok(StartupLock { _file: locked })
            }
            Err((_, error)) if error == Errno::EWOULDBLOCK => {
                Err(RuntimeError::StartupInProgress(self.startup_lock.clone()))
            }
            Err((_, source)) => Err(RuntimeError::Io {
                path: self.startup_lock.clone(),
                source: source.into(),
            }),
        }
    }

    fn validate_lock_identity(&self, file: &File) -> Result<(), RuntimeError> {
        let descriptor = file.metadata().map_err(|source| RuntimeError::Io {
            path: self.startup_lock.clone(),
            source,
        })?;
        let pathname =
            fs::symlink_metadata(&self.startup_lock).map_err(|source| RuntimeError::Io {
                path: self.startup_lock.clone(),
                source,
            })?;
        if !descriptor.file_type().is_file()
            || !pathname.file_type().is_file()
            || descriptor.dev() != pathname.dev()
            || descriptor.ino() != pathname.ino()
        {
            return Err(RuntimeError::ReplacedStartupLock(self.startup_lock.clone()));
        }
        validate_owner_file_metadata(&self.startup_lock, &descriptor)?;
        validate_owner_file_metadata(&self.startup_lock, &pathname)?;
        if descriptor.permissions().mode() & 0o777 != OWNER_FILE_MODE
            || pathname.permissions().mode() & 0o777 != OWNER_FILE_MODE
        {
            return Err(RuntimeError::InsecurePermissions {
                path: self.startup_lock.clone(),
                mode: pathname.permissions().mode() & 0o777,
                expected: OWNER_FILE_MODE,
            });
        }
        Ok(())
    }

    /// Binds only after a startup lock holder has proved any existing socket is stale.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError` when binding fails or a live peer owns the path.
    pub fn bind_listener(&self) -> Result<UnixListener, RuntimeError> {
        self.ensure_owner_directory()?;
        if self.socket.exists() {
            return Err(RuntimeError::SocketExists(self.socket.clone()));
        }
        let listener = UnixListener::bind(&self.socket).map_err(|source| RuntimeError::Io {
            path: self.socket.clone(),
            source,
        })?;
        fs::set_permissions(&self.socket, fs::Permissions::from_mode(OWNER_FILE_MODE)).map_err(
            |source| RuntimeError::Io {
                path: self.socket.clone(),
                source,
            },
        )?;
        Ok(listener)
    }

    /// Removes a socket only after same-user ownership and a refused local connect prove it stale.
    ///
    /// # Errors
    ///
    /// Returns `RuntimeError` when validation or removal fails.
    pub fn remove_validated_stale_socket(&self) -> Result<(), RuntimeError> {
        let metadata = match fs::symlink_metadata(&self.socket) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(RuntimeError::Io {
                    path: self.socket.clone(),
                    source,
                });
            }
        };
        validate_owner_file_metadata(&self.socket, &metadata)?;
        if !metadata.file_type().is_socket() {
            return Err(RuntimeError::UnexpectedEndpointFile(self.socket.clone()));
        }
        match UnixStream::connect(&self.socket) {
            Ok(_) => Err(RuntimeError::LiveSocket(self.socket.clone())),
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
                ) =>
            {
                fs::remove_file(&self.socket).map_err(|source| RuntimeError::Io {
                    path: self.socket.clone(),
                    source,
                })
            }
            Err(source) => Err(RuntimeError::Io {
                path: self.socket.clone(),
                source,
            }),
        }
    }
}

#[derive(Debug)]
pub struct StartupLock {
    _file: Flock<File>,
}

/// Validates an owner-only directory without following symlinks.
///
/// # Errors
///
/// Returns `RuntimeError` on missing paths, wrong ownership, or loose modes.
pub fn validate_owner_directory(path: &Path) -> Result<(), RuntimeError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| RuntimeError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    validate_owner_directory_metadata(path, &metadata)
}

/// Validates an owner-only file without following symlinks.
///
/// # Errors
///
/// Returns `RuntimeError` on missing paths, wrong ownership, or loose modes.
pub fn validate_owner_file(path: &Path) -> Result<(), RuntimeError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| RuntimeError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_file() {
        return Err(RuntimeError::NotRegularFile(path.to_path_buf()));
    }
    validate_owner_file_metadata(path, &metadata)
}

fn validate_owner_directory_metadata(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), RuntimeError> {
    if !metadata.is_dir() {
        return Err(RuntimeError::NotDirectory(path.to_path_buf()));
    }
    validate_owner_mode(path, metadata, OWNER_DIRECTORY_MODE)
}

fn validate_owner_file_metadata(path: &Path, metadata: &fs::Metadata) -> Result<(), RuntimeError> {
    validate_owner_mode(path, metadata, OWNER_FILE_MODE)
}

fn validate_owner_mode(
    path: &Path,
    metadata: &fs::Metadata,
    expected: u32,
) -> Result<(), RuntimeError> {
    let uid = Uid::current().as_raw();
    if metadata.uid() != uid {
        return Err(RuntimeError::WrongOwner {
            path: path.to_path_buf(),
            expected: uid,
            actual: metadata.uid(),
        });
    }
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(RuntimeError::InsecurePermissions {
            path: path.to_path_buf(),
            mode: metadata.permissions().mode() & 0o777,
            expected,
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("runtime socket path exceeds the Unix-domain limit: {0}")]
    SocketPathTooLong(PathBuf),
    #[error("I/O at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("runtime endpoint is not a directory: {0}")]
    NotDirectory(PathBuf),
    #[error("runtime path is not a regular file: {0}")]
    NotRegularFile(PathBuf),
    #[error("runtime path has the wrong owner: {path} (expected {expected}, got {actual})")]
    WrongOwner {
        path: PathBuf,
        expected: u32,
        actual: u32,
    },
    #[error("runtime path has insecure mode {mode:o}, expected owner-only {expected:o}: {path}")]
    InsecurePermissions {
        path: PathBuf,
        mode: u32,
        expected: u32,
    },
    #[error("broker socket already exists: {0}")]
    SocketExists(PathBuf),
    #[error("broker socket is live: {0}")]
    LiveSocket(PathBuf),
    #[error("endpoint is not a Unix socket: {0}")]
    UnexpectedEndpointFile(PathBuf),
    #[error("legacy or incomplete startup lock cannot prove exclusive ownership: {0}")]
    LegacyStartupLock(PathBuf),
    #[error("startup lock pathname no longer names its opened inode: {0}")]
    ReplacedStartupLock(PathBuf),
    #[error("broker startup is already in progress at {0}")]
    StartupInProgress(PathBuf),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_stable_distinct_owner_only_endpoints() {
        let temp = tempfile::tempdir().unwrap();
        let first =
            RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, "/tmp/herdr.sock")
                .unwrap();
        let second =
            RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, "/tmp/herdr.sock")
                .unwrap();
        let different =
            RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Zellij, "session").unwrap();
        assert_eq!(first.socket(), second.socket());
        assert_ne!(first.socket(), different.socket());
        first.ensure_owner_directory().unwrap();
        let mode = fs::metadata(first.runtime_dir)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode & 0o077, 0);
    }

    #[test]
    fn stale_socket_is_removed_only_after_refused_connect() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint =
            RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, "server").unwrap();
        endpoint.ensure_owner_directory().unwrap();
        let listener = std::os::unix::net::UnixListener::bind(endpoint.socket()).unwrap();
        fs::set_permissions(
            endpoint.socket(),
            fs::Permissions::from_mode(OWNER_FILE_MODE),
        )
        .unwrap();
        drop(listener);
        endpoint.remove_validated_stale_socket().unwrap();
        assert!(!endpoint.socket().exists());
    }

    #[test]
    fn validates_owner_only_files_and_directories() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("runtime");
        fs::create_dir(&directory).unwrap();
        fs::set_permissions(&directory, fs::Permissions::from_mode(OWNER_DIRECTORY_MODE)).unwrap();
        validate_owner_directory(&directory).unwrap();

        let file = temp.path().join("state");
        fs::write(&file, b"state").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(OWNER_FILE_MODE)).unwrap();
        validate_owner_file(&file).unwrap();
        assert!(matches!(
            validate_owner_directory(&file),
            Err(RuntimeError::NotDirectory(path)) if path == file
        ));
        assert!(matches!(
            validate_owner_file(&directory),
            Err(RuntimeError::NotRegularFile(path)) if path == directory
        ));
    }

    #[test]
    fn contender_opened_before_guard_release_claims_the_same_persistent_inode() {
        use std::sync::mpsc;

        let temp = tempfile::tempdir().unwrap();
        let endpoint =
            RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, "same").unwrap();
        let first = endpoint.acquire_startup_lock().unwrap();
        let original = fs::symlink_metadata(&endpoint.startup_lock).unwrap();
        let (opened_tx, opened_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let contender = endpoint.clone();
        let waiter = std::thread::spawn(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
                .open(&contender.startup_lock)
                .unwrap();
            opened_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            contender.lock_existing(file).unwrap()
        });
        opened_rx.recv().unwrap();
        assert!(matches!(
            endpoint.acquire_startup_lock(),
            Err(RuntimeError::StartupInProgress(_))
        ));
        drop(first);
        resume_tx.send(()).unwrap();
        let second = waiter.join().unwrap();
        let current = fs::symlink_metadata(&endpoint.startup_lock).unwrap();
        assert_eq!(
            (original.dev(), original.ino()),
            (current.dev(), current.ino())
        );
        assert!(matches!(
            endpoint.acquire_startup_lock(),
            Err(RuntimeError::StartupInProgress(_))
        ));
        drop(second);
        let third = endpoint.acquire_startup_lock().unwrap();
        assert_eq!(
            original.ino(),
            fs::symlink_metadata(&endpoint.startup_lock).unwrap().ino()
        );
        drop(third);
    }

    #[test]
    fn unpublished_stage_and_legacy_empty_or_pid_files_never_claim_startup() {
        let temp = tempfile::tempdir().unwrap();
        let endpoint =
            RuntimeEndpoint::in_runtime_dir(temp.path(), HostKind::Herdr, "staged").unwrap();
        endpoint.ensure_owner_directory().unwrap();
        let mut legacy = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(OWNER_FILE_MODE)
            .open(&endpoint.startup_lock)
            .unwrap();
        assert!(matches!(
            endpoint.acquire_startup_lock(),
            Err(RuntimeError::LegacyStartupLock(_))
        ));
        writeln!(legacy, "{}", std::process::id()).unwrap();
        assert!(matches!(
            endpoint.acquire_startup_lock(),
            Err(RuntimeError::LegacyStartupLock(_))
        ));
        drop(legacy);
        fs::remove_file(&endpoint.startup_lock).unwrap();

        let mut unpublished = tempfile::Builder::new()
            .prefix(".muxe-startup-")
            .tempfile_in(&endpoint.runtime_dir)
            .unwrap();
        unpublished.write_all(STARTUP_LOCK_MARKER).unwrap();
        let staged = Flock::lock(
            unpublished.as_file().try_clone().unwrap(),
            FlockArg::LockExclusiveNonblock,
        )
        .unwrap();
        assert!(
            !endpoint.startup_lock.exists(),
            "incomplete stage is not published"
        );
        let owner = endpoint.acquire_startup_lock().unwrap();
        assert_eq!(
            fs::read(&endpoint.startup_lock).unwrap(),
            STARTUP_LOCK_MARKER
        );
        let error = unpublished
            .persist_noclobber(&endpoint.startup_lock)
            .unwrap_err();
        assert_eq!(error.error.kind(), io::ErrorKind::AlreadyExists);
        drop(staged);
        assert!(matches!(
            endpoint.acquire_startup_lock(),
            Err(RuntimeError::StartupInProgress(_))
        ));
        drop(owner);
        endpoint.acquire_startup_lock().unwrap();
    }
}
