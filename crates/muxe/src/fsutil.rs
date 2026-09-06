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

/// Creates `directory` and every missing ancestor with owner-only modes.
///
/// Existing directories are never repaired: owner/type/mode drift remains a
/// hard error. Newly created directories are chmodded before validation so
/// the caller's umask cannot weaken the documented mode.
pub fn ensure_owner_dir(directory: &Path) -> Result<(), FsError> {
    match fs::symlink_metadata(directory) {
        Ok(_) => check_owner_directory(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = directory.parent().filter(|parent| !parent.as_os_str().is_empty()) {
                ensure_owner_dir(parent)?;
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
        match create_new_owner_file(&path, false)? {
            Some(file) => return Ok((path, file)),
            None => continue,
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
    let directory = path.parent().filter(|parent| !parent.as_os_str().is_empty());
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

/// Reads a file that must already be an owner-only regular file without reopening its path.
pub fn read_owner_file(path: &Path) -> Result<Vec<u8>, FsError> {
    let mut file = open_owner_file(path, false)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| io_error("reading file", path, source))?;
    Ok(bytes)
}
