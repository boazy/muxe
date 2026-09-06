//! Stable bridge file transactions.
//!
//! The canonical stable WASM path is the transaction boundary. Safety
//! invariants, enforced jointly by these primitives and their callers:
//!
//! - The active stable bytes stay in place until the atomic staged-to-stable
//!   swap. Backups are owner-verified copies (hardlink preferred, byte copy
//!   otherwise), never a rename that leaves the stable path absent.
//! - One byte-for-byte backup is retained through the group transaction. An
//!   existing `.previous` rollback copy is never destroyed without
//!   receipt-and-journal authority: replacement requires the recorded previous
//!   digest to match the copy on disk.
//! - Destination eligibility is re-validated at the actual mutation, not just
//!   at preflight: the stable file must be a regular owner-owned file (never
//!   a symlink) whose digest still matches the receipt, read through
//!   no-follow metadata checks immediately before the swap.
//! - There is no unguarded commit: [`commit`] requires the expected current
//!   digest (`None` only when the destination must be absent).
//!
//! Unrecognized bytes or a receipt mismatch are never overwritten: the command
//! reports both digests and requires the user to resolve the file.

use std::{
    fs,
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use thiserror::Error;

use crate::fsutil::{self, FsError};

/// Stable bridge file name inside the integration directory.
pub const BRIDGE_FILE_NAME: &str = "muxe-zellij.wasm";
/// Rollback copy suffix: `<bridge>.previous`.
pub const PREVIOUS_SUFFIX: &str = ".previous";
/// Staging tag for bridge files.
const STAGING_TAG: &str = "bridge";
/// Owner-only mode enforced for bridge artifacts.
const BRIDGE_FILE_MODE: u32 = 0o600;

#[derive(Debug, Error)]
pub enum BridgeError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(
        "refusing to overwrite unrecognized bridge bytes at {path}: found digest {found}, receipt records {expected}"
    )]
    ForeignBytes {
        path: PathBuf,
        found: String,
        expected: String,
    },
    #[error(
        "bridge destination at {path} holds unrecognized bytes with digest {found} and no receipt exists; resolve the file before retrying"
    )]
    UntrackedBytes { path: PathBuf, found: String },
    #[error("staged bridge digest changed after write: expected {expected}, found {found}")]
    StagedDigestChanged { expected: String, found: String },
    #[error(
        "rollback copy at {path} is protected: recorded digest {recorded} does not match {found}; resolve it before retrying"
    )]
    PreviousProtected {
        path: PathBuf,
        recorded: String,
        found: String,
    },
    #[error("bridge destination at {} is not a regular owner-only file", path.display())]
    UnsafeDestination { path: PathBuf },
    #[error(
        "bridge destination at {} changed concurrently: expected {expected}, found {found}", path.display()
    )]
    ConcurrentChange {
        path: PathBuf,
        expected: String,
        found: String,
    },
    #[error(
        "bridge destination at {} appeared concurrently; refusing to overwrite", path.display()
    )]
    UnexpectedBytes { path: PathBuf },
}

/// Destination eligibility for replacement, checked before staging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Eligibility {
    /// Nothing is installed; the staged bridge commits directly.
    Absent,
    /// Current bytes match the receipt digest; replacement may proceed.
    EligibleReplace { current_digest: String },
}

/// Reads the current destination state without mutating anything.
///
/// Symlinks are never followed: eligibility requires a regular file or an
/// absent path. Ownership and digest are re-validated at commit time.
pub fn check_destination(
    stable: &Path,
    receipt_digest: Option<&str>,
) -> Result<(Eligibility, Option<String>), BridgeError> {
    match fs::symlink_metadata(stable) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(BridgeError::UnsafeDestination {
                    path: stable.to_path_buf(),
                });
            }
            let current = fs::read(stable).map_err(|source| {
                fsutil::io_error("reading installed bridge", stable, source)
            })?;
            let found = fsutil::sha256_hex(&current);
            match receipt_digest {
                Some(expected) if found == expected => Ok((
                    Eligibility::EligibleReplace {
                        current_digest: found.clone(),
                    },
                    Some(found),
                )),
                Some(expected) => Err(BridgeError::ForeignBytes {
                    path: stable.to_path_buf(),
                    found,
                    expected: expected.to_owned(),
                }),
                None => Err(BridgeError::UntrackedBytes {
                    path: stable.to_path_buf(),
                    found,
                }),
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok((Eligibility::Absent, None))
        }
        Err(source) => Err(BridgeError::Fs(fsutil::io_error(
            "reading installed bridge",
            stable,
            source,
        ))),
    }
}

/// Stages `bytes` beside `stable` through owner-only creation and verifies the
/// staged digest before returning the staging path.
pub fn stage(stable: &Path, bytes: &[u8]) -> Result<PathBuf, BridgeError> {
    let directory = stable.parent().filter(|p| !p.as_os_str().is_empty());
    let directory = directory.ok_or_else(|| {
        BridgeError::Fs(fsutil::io_error(
            "resolving bridge directory",
            stable,
            io::Error::new(io::ErrorKind::InvalidInput, "bridge path has no parent"),
        ))
    })?;
    fsutil::ensure_owner_dir(directory)?;
    let (staging, mut file) =
        fsutil::create_staging_file(directory, BRIDGE_FILE_NAME, STAGING_TAG)?;
    let result = (|| {
        use std::io::Write;
        file.write_all(bytes)
            .map_err(|source| fsutil::io_error("writing staged bridge", &staging, source))?;
        file.sync_all()
            .map_err(|source| fsutil::io_error("synchronizing staged bridge", &staging, source))?;
        drop(file);
        let staged_bytes = fs::read(&staging)
            .map_err(|source| fsutil::io_error("verifying staged bridge", &staging, source))?;
        let expected = fsutil::sha256_hex(bytes);
        let found = fsutil::sha256_hex(&staged_bytes);
        if found != expected {
            return Err(BridgeError::StagedDigestChanged { expected, found });
        }
        fs::set_permissions(&staging, fs::Permissions::from_mode(BRIDGE_FILE_MODE)).map_err(
            |source| fsutil::io_error("locking staged bridge mode", &staging, source),
        )?;
        fsutil::sync_dir(directory)?;
        Ok(staging.clone())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&staging);
    }
    Ok(result?)
}

/// Owner-verified backup record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Backup {
    pub path: PathBuf,
    pub digest: String,
}

/// Preserves the current stable bridge as the `.previous` rollback copy.
///
/// The stable path keeps serving until the atomic swap: the backup is a
/// hardlink where the filesystem allows it, falling back to a verified byte
/// copy, and its digest is verified before returning. An existing rollback
/// copy is replaced only under receipt-and-journal authority: `authority`
/// must be `Some` recorded digest matching the copy on disk, otherwise the
/// prior rollback copy survives untouched and this call fails.
pub fn backup(stable: &Path, authority: Option<&str>) -> Result<Backup, BridgeError> {

    let previous = previous_path(stable);
    if let Ok(metadata) = fs::symlink_metadata(&previous) {
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(BridgeError::UnsafeDestination { path: previous });
        }
        let existing = fs::read(&previous)
            .map_err(|source| fsutil::io_error("reading rollback copy", &previous, source))?;
        let found = fsutil::sha256_hex(&existing);
        match authority {
            Some(recorded) if recorded == found => {
                fs::remove_file(&previous).map_err(|source| {
                    fsutil::io_error("replacing rollback copy", &previous, source)
                })?;
            }
            Some(recorded) => {
                return Err(BridgeError::PreviousProtected {
                    path: previous,
                    recorded: recorded.to_owned(),
                    found,
                });
            }
            None => {
                return Err(BridgeError::PreviousProtected {
                    path: previous.clone(),
                    recorded: "<no recorded digest>".to_owned(),
                    found,
                });
            }
        }
    }
    let stable_bytes = fs::read(stable)
        .map_err(|source| fsutil::io_error("reading old bridge", stable, source))?;
    let digest = fsutil::sha256_hex(&stable_bytes);
    match fs::hard_link(stable, &previous) {
        Ok(()) => {}
        Err(_) => {
            // Cross-device or unsupported: fall back to a synced byte copy.
            let (staging, mut file) =
                fsutil::create_staging_file(previous_parent(&previous), "muxe-zellij.wasm.previous", "backup")?;
            let result = (|| {
                use std::io::Write;
                file.write_all(&stable_bytes).map_err(|source| {
                    fsutil::io_error("writing rollback copy", &staging, source)
                })?;
                file.sync_all().map_err(|source| {
                    fsutil::io_error("synchronizing rollback copy", &staging, source)
                })?;
                drop(file);
                fs::rename(&staging, &previous).map_err(|source| {
                    fsutil::io_error("installing rollback copy", &previous, source)
                })?;
                fsutil::sync_dir_of(&previous)
            })();
            if result.is_err() {
                let _ = fs::remove_file(&staging);
            }
            result?;
        }
    }
    // Owner-verified: the backup must digest identically, however it was made.
    let backed = fs::read(&previous)
        .map_err(|source| fsutil::io_error("verifying rollback copy", &previous, source))?;
    if fsutil::sha256_hex(&backed) != digest {
        let _ = fs::remove_file(&previous);
        return Err(BridgeError::StagedDigestChanged {
            expected: digest,
            found: fsutil::sha256_hex(&backed),
        });
    }
    fsutil::sync_dir_of(&previous)?;
    Ok(Backup {
        path: previous,
        digest,
    })
}

/// Idempotent backup: reuses the existing rollback copy when it already
/// preserves the current stable bytes (a retried transaction), otherwise
/// creates one under receipt-and-journal authority.
pub fn ensure_backup(stable: &Path, authority: Option<&str>) -> Result<Backup, BridgeError> {
    let stable_bytes =
        fs::read(stable).map_err(|source| fsutil::io_error("reading old bridge", stable, source))?;
    let digest = fsutil::sha256_hex(&stable_bytes);
    let previous = previous_path(stable);
    if let Ok(metadata) = fs::symlink_metadata(&previous) {
        if metadata.is_file() && !metadata.file_type().is_symlink() {
            if let Ok(existing) = fs::read(&previous) {
                if fsutil::sha256_hex(&existing) == digest {
                    return Ok(Backup { path: previous, digest });
                }
            }
        }
    }
    backup(stable, authority)
}

fn previous_parent(previous: &Path) -> &Path {
    previous.parent().unwrap_or_else(|| Path::new("."))
}

/// Atomically commits the staged bridge over the stable path.
///
/// Re-validates ownership at the actual mutation: the destination must be a
/// regular owner-owned file (never a symlink) whose current digest still
/// matches `expected_current`, or be absent when `expected_current` is `None`.
/// Any deviation fails closed and leaves the destination untouched.
pub fn commit(
    staged: &Path,
    stable: &Path,
    expected_current: Option<&str>,
) -> Result<(), BridgeError> {
    match fs::symlink_metadata(stable) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(BridgeError::UnsafeDestination {
                    path: stable.to_path_buf(),
                });
            }
            check_owner_only(stable, &metadata)?;
            let current = fs::read(stable)
                .map_err(|source| fsutil::io_error("reading installed bridge", stable, source))?;
            let found = fsutil::sha256_hex(&current);
            match expected_current {
                Some(expected) if found == expected => {}
                Some(expected) => {
                    return Err(BridgeError::ConcurrentChange {
                        path: stable.to_path_buf(),
                        expected: expected.to_owned(),
                        found,
                    });
                }
                None => {
                    return Err(BridgeError::UnexpectedBytes {
                        path: stable.to_path_buf(),
                    });
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if expected_current.is_some() {
                return Err(BridgeError::ConcurrentChange {
                    path: stable.to_path_buf(),
                    expected: expected_current.unwrap_or("").to_owned(),
                    found: "<absent>".to_owned(),
                });
            }
        }
        Err(source) => {
            return Err(BridgeError::Fs(fsutil::io_error(
                "reading installed bridge",
                stable,
                source,
            )));
        }
    }
    fsutil::commit_staging(staged, stable)?;
    Ok(())
}

fn check_owner_only(path: &Path, metadata: &fs::Metadata) -> Result<(), BridgeError> {
    let mode = metadata.permissions().mode() & 0o777;
    if mode != BRIDGE_FILE_MODE {
        return Err(BridgeError::UnsafeDestination {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

/// Removes a staging file that never committed. Never fails: cleanup must not
/// mask the triggering error.
pub fn discard_staging(staging: &Path) {
    let _ = fs::remove_file(staging);
}

/// Returns the `.previous` rollback path for a stable bridge.
#[must_use]
pub fn previous_path(stable: &Path) -> PathBuf {
    let mut name = stable
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(BRIDGE_FILE_NAME)
        .to_owned();
    name.push_str(PREVIOUS_SUFFIX);
    match stable.parent() {
        Some(parent) => parent.join(&name),
        None => PathBuf::from(name),
    }
}

/// Removes a file only when its current digest matches `expected`.
///
/// Returns `true` when the file was removed, `false` when it was already
/// absent. A digest mismatch is a hard error: user-modified bytes stay put.
/// Symlinks and non-regular files are never removed.
pub fn remove_if_matching(path: &Path, expected: &str) -> Result<bool, BridgeError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(BridgeError::Fs(fsutil::io_error(
                "reading bridge artifact",
                path,
                source,
            )));
        }
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(BridgeError::UnsafeDestination {
            path: path.to_path_buf(),
        });
    }
    let current = fs::read(path).map_err(|source| {
        fsutil::io_error("reading bridge artifact", path, source)
    })?;
    let found = fsutil::sha256_hex(&current);
    if found != expected {
        return Err(BridgeError::ForeignBytes {
            path: path.to_path_buf(),
            found,
            expected: expected.to_owned(),
        });
    }
    fs::remove_file(path)
        .map_err(|source| fsutil::io_error("removing bridge artifact", path, source))?;
    fsutil::sync_dir_of(path)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stable(dir: &Path) -> PathBuf {
        dir.join(BRIDGE_FILE_NAME)
    }

    fn install_bytes(dir: &Path, bytes: &[u8]) {
        let staged = stage(&stable(dir), bytes).unwrap();
        commit(&staged, &stable(dir), None).unwrap();
    }

    #[test]
    fn absent_destination_stages_and_commits() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        assert_eq!(
            check_destination(&stable, None).unwrap().0,
            Eligibility::Absent
        );
        let staged = stage(&stable, b"wasm-v1").unwrap();
        crate::logging::assert_owner_only(&staged);
        commit(&staged, &stable, None).unwrap();
        assert_eq!(fs::read(&stable).unwrap(), b"wasm-v1");
    }

    #[test]
    fn eligible_bridge_is_backed_up_before_commit() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        install_bytes(temp.path(), b"wasm-v1");
        let digest_v1 = fsutil::sha256_hex(b"wasm-v1");
        let (eligibility, _) = check_destination(&stable, Some(&digest_v1)).unwrap();
        assert!(matches!(eligibility, Eligibility::EligibleReplace { .. }));
        // Stable bytes stay in place until the swap: backup copies, not moves.
        let backup_record = backup(&stable, None).unwrap();
        assert_eq!(backup_record.digest, digest_v1);
        assert_eq!(fs::read(&stable).unwrap(), b"wasm-v1");
        let staged = stage(&stable, b"wasm-v2").unwrap();
        commit(&staged, &stable, Some(&digest_v1)).unwrap();
        assert_eq!(fs::read(&previous_path(&stable)).unwrap(), b"wasm-v1");
        assert_eq!(fs::read(&stable).unwrap(), b"wasm-v2");
    }

    #[test]
    fn existing_rollback_copy_is_protected_without_authority() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        install_bytes(temp.path(), b"wasm-v1");
        backup(&stable, None).unwrap();
        // A second backup without authority refuses to destroy the copy,
        // even when the caller names a wrong recorded digest.
        assert!(matches!(
            backup(&stable, Some(&"0".repeat(64))),
            Err(BridgeError::PreviousProtected { .. })
        ));
        assert!(matches!(
            backup(&stable, None),
            Err(BridgeError::PreviousProtected { .. })
        ));
        // With matching authority the copy rotates.
        let digest_v1 = fsutil::sha256_hex(b"wasm-v1");
        backup(&stable, Some(&digest_v1)).unwrap();
    }

    #[test]
    fn commit_revalidates_concurrent_change() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        install_bytes(temp.path(), b"wasm-v1");
        let digest_v1 = fsutil::sha256_hex(b"wasm-v1");
        let staged = stage(&stable, b"wasm-v2").unwrap();
        fs::write(&stable, b"intruder").unwrap();
        assert!(matches!(
            commit(&staged, &stable, Some(&digest_v1)),
            Err(BridgeError::ConcurrentChange { .. })
        ));
        assert_eq!(fs::read(&stable).unwrap(), b"intruder");
    }

    #[test]
    fn commit_refuses_symlink_destination() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        let target = temp.path().join("real.wasm");
        fs::write(&target, b"wasm").unwrap();
        std::os::unix::fs::symlink(&target, &stable).unwrap();
        assert!(matches!(
            check_destination(&stable, None),
            Err(BridgeError::UnsafeDestination { .. })
        ));
        let staged = stage(&stable, b"wasm-v2").unwrap();
        assert!(matches!(
            commit(&staged, &stable, None),
            Err(BridgeError::UnsafeDestination { .. })
        ));
    }

    #[test]
    fn foreign_bytes_are_never_overwritten() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        fs::write(&stable, b"someone-elses-wasm").unwrap();
        let error = check_destination(&stable, Some(&"a".repeat(64))).unwrap_err();
        assert!(matches!(error, BridgeError::ForeignBytes { .. }));
        assert!(matches!(
            check_destination(&stable, None),
            Err(BridgeError::UntrackedBytes { .. })
        ));
    }

    #[test]
    fn remove_requires_matching_digest() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let stable = stable(temp.path());
        fs::write(&stable, b"wasm-v1").unwrap();
        let digest = fsutil::sha256_hex(b"wasm-v1");
        assert!(remove_if_matching(&stable, &digest).unwrap());
        assert!(!stable.exists());
        fs::write(&stable, b"changed").unwrap();
        assert!(remove_if_matching(&stable, &digest).is_err());
        assert_eq!(fs::read(&stable).unwrap(), b"changed");
    }
}
