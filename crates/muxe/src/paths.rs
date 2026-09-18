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
    ffi::OsString,
    fmt, io,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error(
        "no validated configuration base: set XDG_CONFIG_HOME/XDG_CACHE_HOME to absolute paths or provide a home directory"
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
    #[error("Zellij configuration path {path} must not contain `.` in persisted ownership")]
    DotComponent { path: PathBuf },
    #[error("Zellij configuration path {path} must not contain `..`")]
    ParentTraversal { path: PathBuf },
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
}
