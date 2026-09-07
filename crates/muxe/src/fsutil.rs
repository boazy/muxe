//! Owner-only, descriptor-checked filesystem primitives shared by lifecycle operations.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use muxe_broker::{RuntimeError, validate_owner_directory, validate_owner_file};
use thiserror::Error;

/// Owner-only file mode for receipts, journals, logs, and staged artifacts.
pub const OWNER_FILE_MODE: u32 = 0o600;
/// Owner-only directory mode for integration, activation, and log directories.
pub const OWNER_DIR_MODE: u32 = 0o700;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("could not {operation} {}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    OwnerValidation(#[from] RuntimeError),
    #[error("refusing ancestor directory {} with unexpected owner {actual}: expected {expected}", path.display())]
    WrongOwner {
        path: PathBuf,
        expected: u32,
        actual: u32,
    },
    #[error("refusing to use non-regular file {}", path.display())]
    NotRegularFile { path: PathBuf },
    #[error("refusing to use {} with mode {:o}: expected owner-only {:o}", path.display(), actual, expected)]
    BadMode {
        path: PathBuf,
        actual: u32,
        expected: u32,
    },
    #[error("refusing to use {} because it changed while opening", path.display())]
    PathChanged { path: PathBuf },
}

pub fn io_error(operation: &'static str, path: &Path, source: io::Error) -> FsError {
    FsError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

/// Creates `directory` owner-only, creating missing ancestors with OS-default
/// modes. The ownership boundary starts at the target itself: every ancestor
/// above it keeps OS-default creation (XDG roots belong to the session, not
/// to Muxe), while ownership and symlink-freedom are still validated on the
/// way down so an attacker-controlled path component fails closed.
///
/// Existing directories are never repaired: owner/type/mode drift on the
/// target itself remains a hard error. A newly created target is chmodded
/// before validation so the caller's umask cannot weaken the documented mode.
pub fn ensure_owner_dir(directory: &Path) -> Result<(), FsError> {
    match fs::symlink_metadata(directory) {
        Ok(_) => check_owner_directory(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = directory
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                ensure_ancestor_dir(parent)?;
            }
            let mut builder = fs::DirBuilder::new();
            builder.mode(OWNER_DIR_MODE);
            match builder.create(directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    return check_owner_directory(directory);
                }
                Err(source) => return Err(io_error("creating directory", directory, source)),
            }
            fs::set_permissions(directory, fs::Permissions::from_mode(OWNER_DIR_MODE))
                .map_err(|source| io_error("locking directory mode", directory, source))?;
            check_owner_directory(directory)
        }
        Err(source) => Err(io_error("checking directory", directory, source)),
    }
}

fn ensure_ancestor_dir(directory: &Path) -> Result<(), FsError> {
    match fs::symlink_metadata(directory) {
        Ok(_) => Ok(validate_ancestor_owned(directory)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = directory
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                ensure_ancestor_dir(parent)?;
            }
            match fs::create_dir(directory) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(io_error("creating ancestor directory", directory, source));
                }
            }
            Ok(validate_ancestor_owned(directory)?)
        }
        Err(source) => Err(io_error("checking ancestor directory", directory, source)),
    }
}

/// Validates one ancestor directory without imposing a mode: it must be a
/// real directory (never a symlink) owned by the current user. Modes above
/// the owned roots keep session defaults; only the owned target itself is
/// mode-locked.
fn validate_ancestor_owned(directory: &Path) -> Result<(), FsError> {
    let metadata = fs::symlink_metadata(directory)
        .map_err(|source| io_error("checking ancestor directory", directory, source))?;
    if !metadata.file_type().is_dir() {
        return Err(FsError::NotRegularFile {
            path: directory.to_path_buf(),
        });
    }
    let expected = nix::unistd::Uid::current().as_raw();
    let actual = metadata.uid();
    if actual != expected {
        return Err(FsError::WrongOwner {
            path: directory.to_path_buf(),
            expected,
            actual,
        });
    }
    Ok(())
}
fn check_owner_directory(path: &Path) -> Result<(), FsError> {
    validate_owner_directory(path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|source| io_error("checking directory", path, source))?;
    check_exact_mode(path, &metadata, OWNER_DIR_MODE)
}

fn check_exact_mode(path: &Path, metadata: &fs::Metadata, expected: u32) -> Result<(), FsError> {
    let actual = metadata.permissions().mode() & 0o777;
    if actual != expected {
        return Err(FsError::BadMode {
            path: path.to_path_buf(),
            actual,
            expected,
        });
    }
    Ok(())
}

/// Validates that an existing file is a regular owner-only file.
pub fn check_owner_file(path: &Path) -> Result<(), FsError> {
    drop(open_owner_file(path, false)?);
    Ok(())
}

/// Returns trusted metadata for an owner-only regular file, or `None` when it
/// does not exist. The metadata comes from the verified open descriptor.
pub fn owner_file_metadata(path: &Path) -> Result<Option<fs::Metadata>, FsError> {
    open_existing_owner_file(path, false)?
        .map(|file| {
            file.metadata()
                .map_err(|source| io_error("checking opened file", path, source))
        })
        .transpose()
}

/// Opens an existing owner-only file or creates one at `0600` without ever
/// following a symlink. The returned descriptor is verified against the named
/// path's inode after broker-provided owner validation, so a replacement race
/// fails rather than being used.
pub fn open_owner_file(path: &Path, append: bool) -> Result<File, FsError> {
    if let Some(file) = open_existing_owner_file(path, append)? {
        return Ok(file);
    }
    match create_new_owner_file(path, append)? {
        Some(file) => Ok(file),
        None => open_existing_owner_file(path, append)?.ok_or_else(|| FsError::PathChanged {
            path: path.to_path_buf(),
        }),
    }
}

/// Revalidates a held descriptor against its named owner-only file. Use this
/// after waiting on an advisory lock so lock-file replacement cannot split the
/// writer population across two inodes.
pub fn verify_owner_file_descriptor(path: &Path, file: &File) -> Result<(), FsError> {
    verify_opened_owner_file(path, file)
}

fn open_existing_owner_file(path: &Path, append: bool) -> Result<Option<File>, FsError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .append(append)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    match options.open(path) {
        Ok(file) => {
            verify_opened_owner_file(path, &file)?;
            Ok(Some(file))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(io_error("opening owner file", path, source)),
    }
}

fn create_new_owner_file(path: &Path, append: bool) -> Result<Option<File>, FsError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .append(append)
        .create_new(true)
        .mode(OWNER_FILE_MODE)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    match options.open(path) {
        Ok(file) => {
            file.set_permissions(fs::Permissions::from_mode(OWNER_FILE_MODE))
                .map_err(|source| io_error("locking file mode", path, source))?;
            verify_opened_owner_file(path, &file)?;
            Ok(Some(file))
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(None),
        Err(source) => Err(io_error("creating owner file", path, source)),
    }
}

fn verify_opened_owner_file(path: &Path, file: &File) -> Result<(), FsError> {
    let opened = file
        .metadata()
        .map_err(|source| io_error("checking opened file", path, source))?;
    if !opened.is_file() {
        return Err(FsError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    check_exact_mode(path, &opened, OWNER_FILE_MODE)?;
    validate_owner_file(path)?;
    let named = fs::symlink_metadata(path)
        .map_err(|source| io_error("checking owner file", path, source))?;
    if !named.is_file() || named.file_type().is_symlink() {
        return Err(FsError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    check_exact_mode(path, &named, OWNER_FILE_MODE)?;
    if opened.dev() != named.dev() || opened.ino() != named.ino() {
        return Err(FsError::PathChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

/// Creates a uniquely named owner-only staging file beside `final_name`.
pub fn create_staging_file(
    directory: &Path,
    final_name: &str,
    tag: &str,
) -> Result<(PathBuf, File), FsError> {
    for sequence in 0..64 {
        let path = directory.join(format!(
            ".{final_name}.{tag}-{}-{sequence}.tmp",
            std::process::id()
        ));
        if let Some(file) = create_new_owner_file(&path, false)? {
            return Ok((path, file));
        }
    }
    Err(FsError::Io {
        operation: "creating unique staging file",
        path: directory.to_path_buf(),
        source: io::Error::new(io::ErrorKind::AlreadyExists, "staging filename collision"),
    })
}

/// Durably replaces `target` with fully written `staging` contents.
pub fn commit_staging(staging: &Path, target: &Path) -> Result<(), FsError> {
    check_owner_file(staging)?;
    fs::rename(staging, target).map_err(|source| io_error("installing file", target, source))?;
    sync_dir_of(target)
}

/// Syncs the parent directory of `path` so a rename is crash-durable.
pub fn sync_dir_of(path: &Path) -> Result<(), FsError> {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    match directory {
        Some(directory) => sync_dir(directory),
        None => Ok(()),
    }
}

/// Syncs a directory entry itself through a descriptor that did not follow a symlink.
pub fn sync_dir(directory: &Path) -> Result<(), FsError> {
    check_owner_directory(directory)?;
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let file = options
        .open(directory)
        .map_err(|source| io_error("opening directory", directory, source))?;
    let opened = file
        .metadata()
        .map_err(|source| io_error("checking opened directory", directory, source))?;
    let named = fs::symlink_metadata(directory)
        .map_err(|source| io_error("checking directory", directory, source))?;
    if !opened.is_dir() || opened.dev() != named.dev() || opened.ino() != named.ino() {
        return Err(FsError::PathChanged {
            path: directory.to_path_buf(),
        });
    }
    file.sync_all()
        .map_err(|source| io_error("synchronizing directory", directory, source))
}

/// Syncs an external (non-Muxe-owned) directory entry itself without enforcing
/// owner-only modes. Host configuration directories keep whatever permissions
/// the user chose: this opens `O_DIRECTORY | O_NOFOLLOW`, verifies the opened
/// descriptor still names the same directory, and syncs it. It never creates,
/// chmods, or mode-gates the directory. Muxe internal state must keep using
/// [`sync_dir`], which enforces `0700`.
pub fn sync_external_dir(directory: &Path) -> Result<(), FsError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let file = options
        .open(directory)
        .map_err(|source| io_error("opening directory", directory, source))?;
    let opened = file
        .metadata()
        .map_err(|source| io_error("checking opened directory", directory, source))?;
    let named = fs::symlink_metadata(directory)
        .map_err(|source| io_error("checking directory", directory, source))?;
    if !opened.is_dir() || opened.dev() != named.dev() || opened.ino() != named.ino() {
        return Err(FsError::PathChanged {
            path: directory.to_path_buf(),
        });
    }
    file.sync_all()
        .map_err(|source| io_error("synchronizing directory", directory, source))
}

/// Writes `bytes` to a fresh owner-only staging file, syncs it, and atomically commits it over
/// `target`.
pub fn write_atomic(target: &Path, bytes: &[u8], tag: &str) -> Result<(), FsError> {
    let directory = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| FsError::Io {
            operation: "resolving parent directory",
            path: target.to_path_buf(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"),
        })?;
    ensure_owner_dir(directory)?;
    let name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    let (staging, mut file) = create_staging_file(directory, name, tag)?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|source| io_error("writing staging file", &staging, source))?;
        file.sync_all()
            .map_err(|source| io_error("synchronizing staging file", &staging, source))?;
        drop(file);
        commit_staging(&staging, target)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    result
}

/// Computes the lowercase hex SHA-256 digest of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes))
}

/// Reads a file that must already be an owner-only regular file without
/// creating it or following a symlink.
pub fn read_owner_file(path: &Path) -> Result<Vec<u8>, FsError> {
    let mut file = open_existing_owner_file(path, false)?.ok_or_else(|| {
        io_error(
            "opening existing file",
            path,
            io::Error::from(io::ErrorKind::NotFound),
        )
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error("reading file", path, source))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &Path) -> u32 {
        std::fs::symlink_metadata(path)
            .expect("test path exists")
            .permissions()
            .mode()
            & 0o777
    }

    /// Ancestors above the owned target keep session modes (755 allowed)
    /// while the target itself is created owner-only; target drift still
    /// fails closed.
    #[test]
    fn ancestors_keep_session_modes_while_target_is_owner_only() {
        let temp = tempfile::tempdir().expect("fs roots");
        let ancestor = temp.path().join("xdg");
        std::fs::create_dir(&ancestor).expect("ancestor created");
        std::fs::set_permissions(&ancestor, std::fs::Permissions::from_mode(0o755))
            .expect("session-default ancestor");
        let target = ancestor.join("muxe");
        ensure_owner_dir(&target).expect("ancestor modes tolerated");
        assert_eq!(mode(&ancestor), 0o755, "ancestors are never chmodded");
        assert_eq!(mode(&target), 0o700, "target is owner-only");
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
            .expect("drift the target");
        assert!(
            ensure_owner_dir(&target).is_err(),
            "target mode drift still fails closed"
        );
    }
    #[test]
    fn read_owner_file_does_not_create_absent_path() {
        let temp = tempfile::tempdir().expect("fs root");
        let path = temp.path().join("absent");
        let error = read_owner_file(&path).expect_err("absent file must fail");
        assert!(
            matches!(
                error,
                FsError::Io { ref source, .. } if source.kind() == io::ErrorKind::NotFound
            ),
            "missing read should preserve NotFound, got {error:?}"
        );
        assert!(
            !path.exists(),
            "read-only missing check must not create a file"
        );
    }
}
