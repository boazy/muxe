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
    io,
    path::{Component, Path, PathBuf},
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
    #[error("Zellij configuration path {path} must not contain `..`")]
    ParentTraversal { path: PathBuf },
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
pub fn zellij_config_path(override_path: Option<&Path>) -> Result<PathBuf, PathError> {
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
    normalize_config_path(&path)
}
/// Makes a Zellij configuration path absolute and removes only `.` components.
///
/// Receipt persistence and uninstall selection both use this one boundary.
/// Parent traversal is rejected instead of collapsed, so a symlink alias can
/// never silently become a receipt-owned path.
pub fn normalize_config_path(path: &Path) -> Result<PathBuf, PathError> {
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
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(PathError::ParentTraversal { path: absolute });
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_override_wins() {
        let next = zellij_config_path(Some(Path::new("/tmp/custom.kdl"))).unwrap();
        assert_eq!(next, PathBuf::from("/tmp/custom.kdl"));
    }

    #[test]
    fn normalization_rejects_parent_traversal() {
        assert!(matches!(
            normalize_config_path(Path::new("/ancestor-symlink/../config.kdl")),
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
