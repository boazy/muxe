use std::io;
use std::path::Path;
use tokio::process::Command;

/// Scopes ambient user state out of a muxe child process: HOME, XDG, the
/// runtime dir, and TMPDIR relocate under the owned `root`, so broker
/// endpoints, registries, caches, and journals never touch default user
/// paths. PATH passes through for system tool lookup; every muxe-relevant
/// path travels via explicit argv.
pub fn apply_scoped_env(command: &mut Command, root: &Path) {
    command.env_clear();
    command.envs(scoped_env_vec(root));
}

/// The single source for the muxe `TempDir` scope: the exact pairs
/// [`apply_scoped_env`] installs. Tests feed this map to the real resolver and
/// real children; no second copy of these roots exists.
#[must_use]
pub fn scoped_env_vec(root: &Path) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    use std::ffi::OsString;
    let path = std::env::var_os("PATH").unwrap_or_default();
    vec![
        (OsString::from("HOME"), root.join("home").into_os_string()),
        (
            OsString::from("XDG_CONFIG_HOME"),
            root.join("config").into_os_string(),
        ),
        (
            OsString::from("XDG_CACHE_HOME"),
            root.join("cache").into_os_string(),
        ),
        (
            OsString::from("XDG_RUNTIME_DIR"),
            root.join("runtime").into_os_string(),
        ),
        (OsString::from("TMPDIR"), root.join("tmp").into_os_string()),
        (OsString::from("PATH"), path),
    ]
}

/// Creates the owned scoped tree (home, config, cache, runtime, tmp),
/// owner-only on Unix. Broker children validate the same modes on use;
/// pre-creating keeps `TMPDIR` and `XDG_RUNTIME_DIR` inside the owned root
/// from the first spawn.
pub fn ensure_scoped_dirs(root: &Path) -> io::Result<()> {
    for name in ["home", "config", "cache", "runtime", "tmp"] {
        let dir = root.join(name);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&dir)?;
        }
        #[cfg(not(unix))]
        {
            std::fs::create_dir_all(&dir)?;
        }
    }
    Ok(())
}
