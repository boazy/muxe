//! Single native path resolver for configuration and cache directories.
//!
//! DESIGN 1570-1584 is authoritative: `$CONFIG_DIR` is `$XDG_CONFIG_HOME/muxe`
//! falling back to `~/.config/muxe`, and `$CACHE_DIR` is `$XDG_CACHE_HOME/muxe`
//! falling back to `~/.cache/muxe`. Linux and macOS both use these XDG
//! locations; on macOS Muxe forces XDG instead of `~/Library` application
//! support and caches. Resolution goes through `platform-dirs` with
//! `use_xdg_on_macos = true`, which additionally requires any `XDG_*`
//! override to be absolute.
//!
//! There is exactly one resolver. Launcher, init, integration, purge,
//! logging, and registry code all receive these injected paths; nothing
//! re-derives them from the environment. When no validated base exists (no
//! home directory), resolution fails instead of falling back to a relative
//! ambient directory.

use std::{
    ffi::{OsStr, OsString},
    fmt,
    fs::{self, OpenOptions},
    io,
    os::unix::{
        ffi::{OsStrExt, OsStringExt},
        fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
};

use muxe_protocol::BridgeUnitId;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error(
        "cannot locate the configuration and cache directories; set HOME to your home directory, or set XDG_CONFIG_HOME and XDG_CACHE_HOME to absolute paths"
    )]
    NoValidatedBase,
    #[error("could not resolve the current directory while normalizing {path}: {source}")]
    CurrentDirectory {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("Zellij configuration path {path} must be absolute")]
    RelativePath { path: PathBuf },
    #[error("the saved Zellij configuration path {path} must not contain a `.` path component")]
    DotComponent { path: PathBuf },
    #[error("Zellij configuration path {path} must not contain `..`")]
    ParentTraversal { path: PathBuf },
    #[error("could not {operation} the bridge installation path {path}: {source}")]
    BridgeIo {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("refusing non-directory or symlinked bridge integration directory {path}")]
    UnsafeBridgeDirectory { path: PathBuf },
    #[error(
        "the installed bridge at {path} must be a regular file, not a directory or symbolic link"
    )]
    UnsafeBridgeLeaf { path: PathBuf },
    #[error("bridge integration directory {path} changed while resolving")]
    BridgeDirectoryChanged { path: PathBuf },
    #[error(
        "the bridge installation directory {path} must have permissions 700, accessible only to its owner"
    )]
    BridgeDirectoryNotOwnerOnly { path: PathBuf },
    #[error(
        "cannot use bridge parent directory {path}: it must be a real directory owned by the current user and not writable by other users"
    )]
    UnsafeBridgeAncestor { path: PathBuf },
}

/// An absolute Zellij configuration path whose raw normalized spelling is its
/// ownership identity.
///
/// The stored [`OsString`] keeps repeated and trailing separators distinct.
/// Borrow [`Self::as_path`] only for file-system operations; identity
/// comparisons and receipt serialization use this type.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConfigPath(OsString);

impl ConfigPath {
    /// Makes command input absolute, removes only `.` path segments, and
    /// rejects `..`.
    ///
    /// # Errors
    ///
    /// Returns [`PathError::CurrentDirectory`] when a relative path cannot be
    /// made absolute, or [`PathError::ParentTraversal`] when `path` contains
    /// an actual `..` segment.
    pub fn from_input(path: &Path) -> Result<Self, PathError> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|source| PathError::CurrentDirectory {
                    path: path.to_path_buf(),
                    source,
                })?
                .join(path)
        };
        #[cfg(unix)]
        let normalized = normalize_unix_input_path(absolute.as_os_str(), &absolute)?;
        Ok(Self(normalized))
    }

    fn from_persisted(path: PathBuf) -> Result<Self, PathError> {
        if !path.is_absolute() {
            return Err(PathError::RelativePath { path });
        }
        #[cfg(unix)]
        validate_unix_persisted_path(&path)?;
        Ok(Self(path.into_os_string()))
    }

    /// Owns the exact stored spelling for an output boundary.
    #[must_use]
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }

    /// Returns whether this configuration is lexically below `directory`.
    #[must_use]
    pub fn is_within(&self, directory: &Path) -> bool {
        Path::new(&self.0).starts_with(directory)
    }

    /// Borrows the exact spelling only for file-system operations.
    #[must_use]
    pub(crate) fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl fmt::Display for ConfigPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Path::new(&self.0).display().fmt(formatter)
    }
}

#[cfg(unix)]
fn normalize_unix_input_path(
    raw: &std::ffi::OsStr,
    original: &Path,
) -> Result<OsString, PathError> {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let bytes = raw.as_bytes();
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        let start = cursor;
        while cursor < bytes.len() && bytes[cursor] != b'/' {
            cursor += 1;
        }
        let segment = &bytes[start..cursor];
        let has_separator = cursor < bytes.len();
        if segment == b".." {
            return Err(PathError::ParentTraversal {
                path: original.to_path_buf(),
            });
        }
        if segment != b"." {
            normalized.extend_from_slice(segment);
            if has_separator {
                normalized.push(b'/');
            }
        }
        if has_separator {
            cursor += 1;
        }
    }
    Ok(OsString::from_vec(normalized))
}

#[cfg(unix)]
fn validate_unix_persisted_path(path: &Path) -> Result<(), PathError> {
    use std::os::unix::ffi::OsStrExt;

    for segment in path.as_os_str().as_bytes().split(|byte| *byte == b'/') {
        if segment == b"." {
            return Err(PathError::DotComponent {
                path: path.to_path_buf(),
            });
        }
        if segment == b".." {
            return Err(PathError::ParentTraversal {
                path: path.to_path_buf(),
            });
        }
    }
    Ok(())
}

impl serde::Serialize for ConfigPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(Path::new(&self.0), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for ConfigPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::from_persisted(<PathBuf as serde::Deserialize>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
/// The descriptor-validated physical authority for one Zellij bridge.
///
/// Symlinked ancestors are collapsed into `canonical_directory`; the
/// integration directory itself and the stable bridge leaf are never followed.
/// The stable path is derived, so no second raw path can become authoritative.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BridgeIdentity {
    canonical_directory: PathBuf,
    unit: BridgeUnitId,
}
fn resolve_bridge_parent(parent: &Path, authority: &Path) -> Result<PathBuf, PathError> {
    let mut cursor = parent.to_path_buf();
    let mut missing = Vec::new();
    let existing = loop {
        match fs::symlink_metadata(&cursor) {
            Ok(_) => {
                break fs::canonicalize(&cursor).map_err(|source| PathError::BridgeIo {
                    operation: "canonicalize existing ancestor of",
                    path: authority.to_path_buf(),
                    source,
                })?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let name = cursor.file_name().ok_or_else(|| PathError::BridgeIo {
                    operation: "resolve existing ancestor of",
                    path: authority.to_path_buf(),
                    source: io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "no existing owned ancestor",
                    ),
                })?;
                missing.push(name.to_os_string());
                cursor = cursor
                    .parent()
                    .ok_or_else(|| PathError::BridgeIo {
                        operation: "resolve existing ancestor of",
                        path: authority.to_path_buf(),
                        source: io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "path has no existing ancestor",
                        ),
                    })?
                    .to_path_buf();
            }
            Err(source) => {
                return Err(PathError::BridgeIo {
                    operation: "inspect ancestor of",
                    path: authority.to_path_buf(),
                    source,
                });
            }
        }
    };
    let mut physical = verify_bridge_ancestor(&existing, false)?;
    for name in missing.iter().rev() {
        let next = physical.join(name);
        let created = match fs::symlink_metadata(&next) {
            Ok(_) => false,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(&next) {
                    Ok(()) => {
                        fs::set_permissions(&next, fs::Permissions::from_mode(0o700)).map_err(
                            |source| PathError::BridgeIo {
                                operation: "lock mode of created ancestor",
                                path: next.clone(),
                                source,
                            },
                        )?;
                        true
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
                    Err(source) => {
                        return Err(PathError::BridgeIo {
                            operation: "create bridge ancestor",
                            path: next,
                            source,
                        });
                    }
                }
            }
            Err(source) => {
                return Err(PathError::BridgeIo {
                    operation: "inspect bridge ancestor",
                    path: next,
                    source,
                });
            }
        };
        physical = verify_bridge_ancestor(&next, created)?;
    }
    Ok(physical)
}

fn verify_bridge_ancestor(path: &Path, require_owner_only: bool) -> Result<PathBuf, PathError> {
    let named = fs::symlink_metadata(path).map_err(|source| PathError::BridgeIo {
        operation: "inspect bridge ancestor",
        path: path.to_path_buf(),
        source,
    })?;
    let mode = named.permissions().mode() & 0o777;
    if !named.file_type().is_dir()
        || named.file_type().is_symlink()
        || named.uid() != nix::unistd::Uid::current().as_raw()
        || mode & 0o022 != 0
        || (require_owner_only && mode != 0o700)
    {
        return Err(PathError::UnsafeBridgeAncestor {
            path: path.to_path_buf(),
        });
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let descriptor = options.open(path).map_err(|source| PathError::BridgeIo {
        operation: "open bridge ancestor",
        path: path.to_path_buf(),
        source,
    })?;
    let opened = descriptor
        .metadata()
        .map_err(|source| PathError::BridgeIo {
            operation: "inspect opened bridge ancestor",
            path: path.to_path_buf(),
            source,
        })?;
    let named_after = fs::symlink_metadata(path).map_err(|source| PathError::BridgeIo {
        operation: "reinspect bridge ancestor",
        path: path.to_path_buf(),
        source,
    })?;
    if opened.dev() != named_after.dev() || opened.ino() != named_after.ino() {
        return Err(PathError::BridgeDirectoryChanged {
            path: path.to_path_buf(),
        });
    }
    let canonical = fs::canonicalize(path).map_err(|source| PathError::BridgeIo {
        operation: "canonicalize bridge ancestor",
        path: path.to_path_buf(),
        source,
    })?;
    let canonical_named =
        fs::symlink_metadata(&canonical).map_err(|source| PathError::BridgeIo {
            operation: "inspect canonical bridge ancestor",
            path: canonical.clone(),
            source,
        })?;
    if opened.dev() != canonical_named.dev() || opened.ino() != canonical_named.ino() {
        return Err(PathError::BridgeDirectoryChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(canonical)
}

impl BridgeIdentity {
    /// Resolves or safely creates the physical integration directory.
    ///
    /// `integration_directory` may have symlinked ancestors. Its final
    /// component must be a real owner-only directory, and `stable_leaf` must
    /// be absent or a regular file.
    ///
    /// # Errors
    ///
    /// Returns [`PathError`] when the directory cannot be safely created,
    /// opened, or verified.
    pub fn resolve(integration_directory: &Path, stable_leaf: &OsStr) -> Result<Self, PathError> {
        Self::resolve_with_hook(integration_directory, stable_leaf, || {})
    }

    /// Resolves an existing physical integration directory without creating
    /// any path component.
    ///
    /// # Errors
    ///
    /// Returns [`PathError`] when the existing directory or stable leaf cannot
    /// be safely opened and verified.
    pub fn resolve_existing(
        integration_directory: &Path,
        stable_leaf: &OsStr,
    ) -> Result<Self, PathError> {
        Self::resolve_existing_with_hook(integration_directory, stable_leaf, || {})
    }

    fn resolve_with_hook(
        integration_directory: &Path,
        stable_leaf: &OsStr,
        after_open: impl FnOnce(),
    ) -> Result<Self, PathError> {
        let parent = integration_directory
            .parent()
            .ok_or_else(|| PathError::BridgeIo {
                operation: "resolve parent of",
                path: integration_directory.to_path_buf(),
                source: io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"),
            })?;
        let canonical_parent = resolve_bridge_parent(parent, integration_directory)?;
        let name = integration_directory
            .file_name()
            .ok_or_else(|| PathError::BridgeIo {
                operation: "resolve final component of",
                path: integration_directory.to_path_buf(),
                source: io::Error::new(io::ErrorKind::InvalidInput, "path has no final component"),
            })?;
        let physical = canonical_parent.join(name);
        match fs::symlink_metadata(&physical) {
            Ok(metadata) if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() => {
            }
            Ok(_) => {
                return Err(PathError::UnsafeBridgeDirectory { path: physical });
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(&physical) {
                    Ok(()) => {
                        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).map_err(
                            |source| PathError::BridgeIo {
                                operation: "lock mode of",
                                path: physical.clone(),
                                source,
                            },
                        )?;
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(source) => {
                        return Err(PathError::BridgeIo {
                            operation: "create",
                            path: physical,
                            source,
                        });
                    }
                }
            }
            Err(source) => {
                return Err(PathError::BridgeIo {
                    operation: "inspect",
                    path: physical,
                    source,
                });
            }
        }
        Self::open_verified(physical, stable_leaf, after_open)
    }

    fn resolve_existing_with_hook(
        integration_directory: &Path,
        stable_leaf: &OsStr,
        after_open: impl FnOnce(),
    ) -> Result<Self, PathError> {
        let parent = integration_directory
            .parent()
            .ok_or_else(|| PathError::BridgeIo {
                operation: "resolve parent of",
                path: integration_directory.to_path_buf(),
                source: io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"),
            })?;
        let canonical_parent = fs::canonicalize(parent).map_err(|source| PathError::BridgeIo {
            operation: "canonicalize parent of",
            path: integration_directory.to_path_buf(),
            source,
        })?;
        let canonical_parent = verify_bridge_ancestor(&canonical_parent, false)?;
        let name = integration_directory
            .file_name()
            .ok_or_else(|| PathError::BridgeIo {
                operation: "resolve final component of",
                path: integration_directory.to_path_buf(),
                source: io::Error::new(io::ErrorKind::InvalidInput, "path has no final component"),
            })?;
        Self::open_verified(canonical_parent.join(name), stable_leaf, after_open)
    }

    fn open_verified(
        physical: PathBuf,
        stable_leaf: &OsStr,
        after_open: impl FnOnce(),
    ) -> Result<Self, PathError> {
        let named = fs::symlink_metadata(&physical).map_err(|source| PathError::BridgeIo {
            operation: "inspect",
            path: physical.clone(),
            source,
        })?;
        if !named.file_type().is_dir() || named.file_type().is_symlink() {
            return Err(PathError::UnsafeBridgeDirectory { path: physical });
        }
        let expected_uid = nix::unistd::Uid::current().as_raw();
        if named.uid() != expected_uid || named.permissions().mode() & 0o777 != 0o700 {
            return Err(PathError::BridgeDirectoryNotOwnerOnly { path: physical });
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
        let descriptor = options
            .open(&physical)
            .map_err(|source| PathError::BridgeIo {
                operation: "open",
                path: physical.clone(),
                source,
            })?;
        let opened = descriptor
            .metadata()
            .map_err(|source| PathError::BridgeIo {
                operation: "inspect opened",
                path: physical.clone(),
                source,
            })?;
        after_open();
        let named_after =
            fs::symlink_metadata(&physical).map_err(|source| PathError::BridgeIo {
                operation: "reinspect",
                path: physical.clone(),
                source,
            })?;
        if !named_after.file_type().is_dir()
            || named_after.file_type().is_symlink()
            || opened.dev() != named_after.dev()
            || opened.ino() != named_after.ino()
        {
            return Err(PathError::BridgeDirectoryChanged { path: physical });
        }
        let canonical = fs::canonicalize(&physical).map_err(|source| PathError::BridgeIo {
            operation: "canonicalize",
            path: physical.clone(),
            source,
        })?;
        let canonical_named =
            fs::symlink_metadata(&canonical).map_err(|source| PathError::BridgeIo {
                operation: "inspect canonical",
                path: canonical.clone(),
                source,
            })?;
        if opened.dev() != canonical_named.dev() || opened.ino() != canonical_named.ino() {
            return Err(PathError::BridgeDirectoryChanged { path: physical });
        }
        let stable = canonical.join(stable_leaf);
        match fs::symlink_metadata(&stable) {
            Ok(metadata)
                if metadata.file_type().is_file() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(PathError::UnsafeBridgeLeaf { path: stable }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(PathError::BridgeIo {
                    operation: "inspect stable leaf in",
                    path: stable,
                    source,
                });
            }
        }
        let unit = BridgeUnitId::from_canonical_bytes(canonical.as_os_str().as_bytes());
        Ok(Self {
            canonical_directory: canonical,
            unit,
        })
    }

    /// Returns the canonical integration directory.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.canonical_directory
    }

    /// Derives the canonical stable bridge path.
    #[must_use]
    pub fn stable_path(&self, stable_leaf: &OsStr) -> PathBuf {
        self.canonical_directory.join(stable_leaf)
    }

    /// Returns the lossless unit key and control attestation.
    #[must_use]
    pub const fn unit(&self) -> BridgeUnitId {
        self.unit
    }
}

impl fmt::Display for BridgeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.canonical_directory.display().fmt(formatter)
    }
}

impl serde::Serialize for BridgeIdentity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.canonical_directory
            .as_os_str()
            .as_bytes()
            .serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for BridgeIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let directory = PathBuf::from(OsString::from_vec(bytes));
        Self::resolve_existing(&directory, OsStr::new("muxe-zellij.wasm"))
            .map_err(serde::de::Error::custom)
    }
}

/// Validated absolute application directories.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppPaths {
    pub config_dir: PathBuf,
    pub cache_dir: PathBuf,
}

impl AppPaths {
    /// Builds paths directly (tests and explicit overrides).
    #[must_use]
    pub const fn new(config_dir: PathBuf, cache_dir: PathBuf) -> Self {
        Self {
            config_dir,
            cache_dir,
        }
    }

    /// Returns the starter configuration file path.
    #[must_use]
    pub fn config_file(&self) -> PathBuf {
        self.config_dir.join("config.yml")
    }
}

/// Resolves the single native path set from the process environment.
///
/// # Errors
///
/// Returns [`PathError::NoValidatedBase`] when no validated home directory exists.
pub fn resolve() -> Result<AppPaths, PathError> {
    let dirs = platform_dirs::AppDirs::new(Some("muxe"), true).ok_or(PathError::NoValidatedBase)?;
    Ok(AppPaths {
        config_dir: dirs.config_dir,
        cache_dir: dirs.cache_dir,
    })
}

/// Resolves which Zellij configuration to inspect or edit.
///
/// Uses `--zellij-config` when supplied, otherwise `$ZELLIJ_CONFIG_DIR/config.kdl`,
/// then the standard Zellij config path under the same XDG base. The result is
/// absolute with only `.` components removed before it crosses the receipt
/// boundary. It deliberately preserves every other component and rejects `..`:
/// collapsing parent traversal would change the target through a symlinked
/// ancestor. KDL reads and writes reject symlink targets through their
/// descriptor-safe checks.
///
/// # Errors
///
/// Returns [`PathError::NoValidatedBase`] when no validated base exists and no override is supplied.
pub fn zellij_config_path(override_path: Option<&Path>) -> Result<ConfigPath, PathError> {
    let path = if let Some(path) = override_path {
        path.to_path_buf()
    } else if let Some(dir) = std::env::var_os("ZELLIJ_CONFIG_DIR")
        && !dir.is_empty()
    {
        PathBuf::from(dir).join("config.kdl")
    } else {
        let dirs =
            platform_dirs::AppDirs::new(Some("zellij"), true).ok_or(PathError::NoValidatedBase)?;
        // platform-dirs appends the application name; Zellij's own file is
        // `config.kdl` directly under its configuration directory.
        dirs.config_dir.join("config.kdl")
    };
    ConfigPath::from_input(&path)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owner_temp() -> tempfile::TempDir {
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        temp
    }

    #[test]
    fn explicit_override_wins() {
        let next = zellij_config_path(Some(Path::new("/tmp/custom.kdl"))).unwrap();
        assert_eq!(
            serde_json::to_string(&next).unwrap(),
            r#""/tmp/custom.kdl""#
        );
    }

    #[test]
    fn normalization_preserves_repeated_and_trailing_separators() {
        let path = ConfigPath::from_input(Path::new("/tmp//muxe/./config.kdl/")).unwrap();
        assert_eq!(
            serde_json::to_string(&path).unwrap(),
            r#""/tmp//muxe/config.kdl/""#
        );
    }

    #[test]
    fn input_paths_round_trip_through_strict_receipt_spelling() {
        for (input, expected) in [("/.", "/"), ("/tmp/.", "/tmp/"), ("/tmp/./", "/tmp/")] {
            let path = ConfigPath::from_input(Path::new(input)).unwrap();
            let encoded = serde_json::to_string(&path).unwrap();
            assert_eq!(encoded, serde_json::to_string(expected).unwrap());
            let decoded: ConfigPath = serde_json::from_str(&encoded).unwrap();
            assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);
        }

        let dot_only = ConfigPath::from_input(Path::new(".")).unwrap();
        let encoded = serde_json::to_string(&dot_only).unwrap();
        assert!(encoded.ends_with("/\""));
        let decoded: ConfigPath = serde_json::from_str(&encoded).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);
    }

    #[test]
    fn normalization_rejects_parent_traversal() {
        assert!(matches!(
            ConfigPath::from_input(Path::new("/ancestor-symlink/../config.kdl")),
            Err(PathError::ParentTraversal { .. })
        ));
    }

    #[test]
    fn live_resolution_matches_xdg_contract() {
        // Reads the ambient environment without mutating it: either a resolved
        // XDG-conformant pair or a strict no-base failure.
        match resolve() {
            Ok(paths) => {
                assert!(paths.config_dir.is_absolute());
                assert!(paths.cache_dir.is_absolute());
                assert!(paths.config_dir.ends_with("muxe"));
            }
            Err(PathError::NoValidatedBase) => {}
            Err(error) => panic!("unexpected app path resolution error: {error}"),
        }
    }
    #[test]
    fn bridge_identity_collapses_symlinked_ancestors() {
        let temp = owner_temp();
        let physical = temp.path().join("physical");
        fs::create_dir(&physical).unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&physical, &alias).unwrap();
        let leaf = OsStr::new("muxe-zellij.wasm");

        let physical_identity =
            BridgeIdentity::resolve(&physical.join("integrations/zellij"), leaf).unwrap();
        let alias_identity =
            BridgeIdentity::resolve(&alias.join("integrations/zellij"), leaf).unwrap();

        assert_eq!(alias_identity, physical_identity);
        assert_eq!(alias_identity.unit(), physical_identity.unit());
    }

    #[test]
    fn bridge_identity_safely_creates_first_integration_directory() {
        let temp = owner_temp();
        let directory = temp.path().join("config/integrations/zellij");
        let identity = BridgeIdentity::resolve(&directory, OsStr::new("muxe-zellij.wasm")).unwrap();

        assert_eq!(identity.directory(), fs::canonicalize(&directory).unwrap());
        let metadata = fs::symlink_metadata(identity.directory()).unwrap();
        assert!(metadata.file_type().is_dir());
        assert!(!metadata.file_type().is_symlink());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
    }

    #[test]
    fn bridge_identity_first_creation_follows_only_existing_symlinked_ancestors() {
        let temp = owner_temp();
        let physical = temp.path().join("physical");
        fs::create_dir(&physical).unwrap();
        fs::set_permissions(&physical, fs::Permissions::from_mode(0o700)).unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&physical, &alias).unwrap();
        let requested = alias.join("config/integrations/zellij");

        let identity = BridgeIdentity::resolve(&requested, OsStr::new("muxe-zellij.wasm")).unwrap();

        assert_eq!(
            identity.directory(),
            fs::canonicalize(&physical)
                .unwrap()
                .join("config/integrations/zellij")
        );
        for path in [
            physical.join("config"),
            physical.join("config/integrations"),
            physical.join("config/integrations/zellij"),
        ] {
            let metadata = fs::symlink_metadata(path).unwrap();
            assert!(metadata.file_type().is_dir());
            assert!(!metadata.file_type().is_symlink());
            assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        }
    }

    #[test]
    fn bridge_identity_accepts_existing_owner_owned_readable_parent() {
        let temp = owner_temp();
        let parent = temp.path().join("config/integrations");
        fs::create_dir_all(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();
        let requested = parent.join("zellij");

        let identity = BridgeIdentity::resolve(&requested, OsStr::new("muxe-zellij.wasm")).unwrap();

        assert_eq!(identity.directory(), fs::canonicalize(&requested).unwrap());
        assert_eq!(
            fs::symlink_metadata(&parent).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert_eq!(
            fs::symlink_metadata(identity.directory())
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
    #[test]
    fn bridge_identity_first_creation_secures_owned_parent_and_final_directory() {
        let temp = owner_temp();
        let owned_parent = temp.path().join("config/integrations");
        let directory = owned_parent.join("zellij");
        let identity = BridgeIdentity::resolve(&directory, OsStr::new("muxe-zellij.wasm")).unwrap();

        for path in [&owned_parent, identity.directory()] {
            let metadata = fs::symlink_metadata(path).unwrap();
            assert!(metadata.file_type().is_dir());
            assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
        }
    }

    #[test]
    fn bridge_identity_serialization_preserves_non_utf8_unit_bytes() {
        let temp = owner_temp();
        let valid =
            BridgeIdentity::resolve(&temp.path().join("valid"), OsStr::new("muxe-zellij.wasm"))
                .unwrap();
        let valid_encoded = serde_json::to_vec(&valid).unwrap();
        let valid_decoded: BridgeIdentity = serde_json::from_slice(&valid_encoded).unwrap();
        assert_eq!(valid_decoded, valid);
        assert_eq!(valid_decoded.unit(), valid.unit());

        let directory = temp.path().join(OsString::from_vec(vec![b'i', b'd', 0x80]));
        let raw = directory.as_os_str().as_bytes().to_vec();
        let identity = BridgeIdentity {
            canonical_directory: directory,
            unit: BridgeUnitId::from_canonical_bytes(&raw),
        };
        let encoded = serde_json::to_vec(&identity).unwrap();
        let decoded_bytes: Vec<u8> = serde_json::from_slice(&encoded).unwrap();
        let decoded = BridgeIdentity {
            canonical_directory: PathBuf::from(OsString::from_vec(decoded_bytes.clone())),
            unit: BridgeUnitId::from_canonical_bytes(&decoded_bytes),
        };

        assert_eq!(decoded, identity);
        assert_eq!(decoded.unit(), identity.unit());
    }

    #[test]
    fn bridge_identity_detects_descriptor_toctou_replacement() {
        let temp = owner_temp();
        let directory = temp.path().join("zellij");
        BridgeIdentity::resolve(&directory, OsStr::new("muxe-zellij.wasm")).unwrap();
        let displaced = temp.path().join("displaced");

        let error = BridgeIdentity::resolve_existing_with_hook(
            &directory,
            OsStr::new("muxe-zellij.wasm"),
            || {
                fs::rename(&directory, &displaced).unwrap();
                fs::create_dir(&directory).unwrap();
                fs::set_permissions(&directory, fs::Permissions::from_mode(0o700)).unwrap();
            },
        )
        .unwrap_err();

        assert!(matches!(error, PathError::BridgeDirectoryChanged { .. }));
    }

    #[test]
    fn bridge_identity_rejects_symlinked_authority_and_leaf() {
        let temp = owner_temp();
        let real = temp.path().join("real");
        fs::create_dir(&real).unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o700)).unwrap();
        let linked = temp.path().join("linked");
        std::os::unix::fs::symlink(&real, &linked).unwrap();
        assert!(matches!(
            BridgeIdentity::resolve(&linked, OsStr::new("muxe-zellij.wasm")),
            Err(PathError::UnsafeBridgeDirectory { .. })
        ));

        let directory = temp.path().join("owned");
        BridgeIdentity::resolve(&directory, OsStr::new("muxe-zellij.wasm")).unwrap();
        let target = temp.path().join("target.wasm");
        fs::write(&target, b"target").unwrap();
        std::os::unix::fs::symlink(&target, directory.join("muxe-zellij.wasm")).unwrap();
        assert!(matches!(
            BridgeIdentity::resolve(&directory, OsStr::new("muxe-zellij.wasm")),
            Err(PathError::UnsafeBridgeLeaf { .. })
        ));
    }
    #[test]
    fn bridge_identity_rejects_stable_leaf_replaced_after_resolution() {
        let temp = owner_temp();
        let directory = temp.path().join("owned");
        let identity = BridgeIdentity::resolve(&directory, OsStr::new("muxe-zellij.wasm")).unwrap();
        let stable = identity.stable_path(OsStr::new("muxe-zellij.wasm"));
        fs::write(&stable, b"owned").unwrap();
        fs::remove_file(&stable).unwrap();
        let target = temp.path().join("target.wasm");
        fs::write(&target, b"foreign").unwrap();
        std::os::unix::fs::symlink(&target, &stable).unwrap();

        assert!(matches!(
            BridgeIdentity::resolve_existing(&directory, OsStr::new("muxe-zellij.wasm")),
            Err(PathError::UnsafeBridgeLeaf { .. })
        ));
    }
}
