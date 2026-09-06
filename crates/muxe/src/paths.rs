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

use std::path::{Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error(
        "no validated configuration base: set XDG_CONFIG_HOME/XDG_CACHE_HOME to absolute paths or provide a home directory"
    )]
    NoValidatedBase,
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
/// then the standard Zellij config path under the same XDG base. Fails without
/// a validated base instead of guessing a relative path.
pub fn zellij_config_path(override_path: Option<&Path>) -> Result<PathBuf, PathError> {
    if let Some(path) = override_path {
        return Ok(path.to_path_buf());
    }
    if let Some(dir) = std::env::var_os("ZELLIJ_CONFIG_DIR") {
        if !dir.is_empty() {
            return Ok(PathBuf::from(dir).join("config.kdl"));
        }
    }
    let dirs = platform_dirs::AppDirs::new(Some("zellij"), true).ok_or(PathError::NoValidatedBase)?;
    // platform-dirs appends the application name; Zellij's own file is
    // `config.kdl` directly under its configuration directory.
    Ok(dirs.config_dir.join("config.kdl"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_carry_config_file() {
        let paths = AppPaths::new(
            PathBuf::from("/tmp/cfg/muxe"),
            PathBuf::from("/tmp/cache/muxe"),
        );
        assert_eq!(
            paths.config_file(),
            PathBuf::from("/tmp/cfg/muxe/config.yml")
        );
    }

    #[test]
    fn explicit_override_wins() {
        let next = zellij_config_path(Some(Path::new("/tmp/custom.kdl"))).unwrap();
        assert_eq!(next, PathBuf::from("/tmp/custom.kdl"));
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
        }
    }
}
