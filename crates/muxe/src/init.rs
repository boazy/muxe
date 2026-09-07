use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use thiserror::Error;

const STARTER_CONFIG: &str = include_str!("../assets/starter.yml");
const OWNER_FILE_MODE: u32 = 0o600;
/// Owner-only directory mode for Muxe internal state, matching `fsutil::OWNER_DIR_MODE`.
/// The boundary starts at the Muxe configuration directory itself: ancestors
/// above it keep whatever modes the OS or user gave them and are never
/// chmodded or enforced here. Preexisting directories are preserved untouched.
const OWNER_DIR_MODE: u32 = 0o700;

/// The paths created or retained by a successful initialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitResult {
    pub config_path: PathBuf,
    pub themes_directory: PathBuf,
    pub color_schemes_directory: PathBuf,
}

/// A failure to initialize Muxe's configuration tree.
#[derive(Debug, Error)]
pub enum InitError {
    #[error("configuration already exists: {0}")]
    ExistingConfig(PathBuf),
    #[error("configuration path has no parent directory: {0}")]
    MissingParent(PathBuf),
    #[error("expected a directory at {0}")]
    NotDirectory(PathBuf),
    #[error("I/O while {operation} {path}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

/// Returns the exact starter configuration embedded in the native executable.
pub const fn starter_config() -> &'static str {
    STARTER_CONFIG
}

/// Installs the starter configuration and its companion directories.
///
/// Newly created Muxe-owned directories (`config_directory`, `themes`,
/// `color-schemes`) are locked to owner-only `0700`, umask-proof, and the
/// starter file to `0600`, so the integration and broker pipelines accept the
/// tree without permission repairs. Ancestors above the configuration
/// directory keep OS-default modes; preexisting entries are never chmodded.
///
/// An existing `config.yml`, including a dangling symlink, always fails without mutation.
pub fn install(config_path: &Path) -> Result<InitResult, InitError> {
    let config_directory = config_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| InitError::MissingParent(config_path.to_path_buf()))?;
    let themes_directory = config_directory.join("themes");
    let color_schemes_directory = config_directory.join("color-schemes");

    ensure_missing(config_path)?;
    ensure_owned_directory(config_directory)?;
    ensure_owned_directory(&themes_directory)?;
    ensure_owned_directory(&color_schemes_directory)?;
    install_starter(config_path, config_directory)?;
    Ok(InitResult {
        config_path: config_path.to_path_buf(),
        themes_directory,
        color_schemes_directory,
    })
}

fn ensure_missing(path: &Path) -> Result<(), InitError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(InitError::ExistingConfig(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(io_error("checking", path, source)),
    }
}

fn ensure_directory(path: &Path) -> Result<(), InitError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(InitError::NotDirectory(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .ok_or_else(|| InitError::MissingParent(path.to_path_buf()))?;
            ensure_directory(parent)?;
            match fs::create_dir(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    ensure_directory(path)
                }
                Err(source) => Err(io_error("creating directory", path, source)),
            }
        }
        Err(source) => Err(io_error("checking", path, source)),
    }
}

/// Creates a Muxe-owned directory at `OWNER_DIR_MODE`, umask-proof.
///
/// Missing ancestors above `path` use [`ensure_directory`] (OS-default modes,
/// never chmodded); only `path` itself is locked owner-only, matching
/// `fsutil::ensure_owner_dir` for freshly created entries. A preexisting
/// directory is preserved untouched and never repaired: wrong modes fail
/// closed downstream instead of being chmodded here.
fn ensure_owned_directory(path: &Path) -> Result<(), InitError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(InitError::NotDirectory(path.to_path_buf())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .ok_or_else(|| InitError::MissingParent(path.to_path_buf()))?;
            ensure_directory(parent)?;
            let mut builder = fs::DirBuilder::new();
            builder.mode(OWNER_DIR_MODE);
            match builder.create(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    return ensure_owned_directory(path);
                }
                Err(source) => return Err(io_error("creating directory", path, source)),
            }
            fs::set_permissions(path, fs::Permissions::from_mode(OWNER_DIR_MODE))
                .map_err(|source| io_error("locking directory mode", path, source))?;
            let actual = fs::symlink_metadata(path)
                .map_err(|source| io_error("checking directory", path, source))?
                .permissions()
                .mode()
                & 0o777;
            if actual != OWNER_DIR_MODE {
                return Err(io_error(
                    "locking directory mode",
                    path,
                    io::Error::other(format!(
                        "directory mode {actual:o} is not owner-only {OWNER_DIR_MODE:o}"
                    )),
                ));
            }
            Ok(())
        }
        Err(source) => Err(io_error("checking", path, source)),
    }
}

fn install_starter(config_path: &Path, directory: &Path) -> Result<(), InitError> {
    let (temporary, mut file) = create_staging_file(directory, config_path)?;
    let write_result = write_starter(&mut file, &temporary);
    drop(file);
    if let Err(error) = write_result {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }

    let result = (|| {
        match fs::hard_link(&temporary, config_path) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                return Err(InitError::ExistingConfig(config_path.to_path_buf()));
            }
            Err(source) => {
                return Err(io_error(
                    "installing starter configuration",
                    config_path,
                    source,
                ));
            }
        }
        // The starter link is committed. Do not remove `config_path` if this sync fails: another
        // process could have replaced it after the link operation, and preserving content is safer
        // than attempting a conditional cleanup without a transaction owner.
        sync_directory(directory)
            .map_err(|source| io_error("synchronizing configuration directory", directory, source))
    })();
    let _ = fs::remove_file(&temporary);
    result
}

fn create_staging_file(directory: &Path, config_path: &Path) -> Result<(PathBuf, File), InitError> {
    let name = config_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.yml");
    for sequence in 0..32 {
        let path = directory.join(format!(
            ".{name}.muxe-init-{}-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(OWNER_FILE_MODE);
        match options.open(&path) {
            Ok(file) => {
                // The creation mode can only remove bits under an odd umask;
                // lock the exact owner-only mode like `fsutil` staging does.
                file.set_permissions(fs::Permissions::from_mode(OWNER_FILE_MODE))
                    .map_err(|source| io_error("locking staging file mode", &path, source))?;
                return Ok((path, file));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(source) => return Err(io_error("creating staging file", &path, source)),
        }
    }
    Err(InitError::Io {
        operation: "creating unique staging file",
        path: directory.to_path_buf(),
        source: io::Error::new(io::ErrorKind::AlreadyExists, "staging filename collision"),
    })
}

fn write_starter(file: &mut File, path: &Path) -> Result<(), InitError> {
    file.write_all(starter_config().as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| io_error("writing starter configuration", path, source))
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> InitError {
    InitError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{Arc, Barrier},
        thread,
    };

    use muxe_core::{
        ActionSpec, CompiledGeneration, KeyCapabilities, MenuAction, MenuId, PortableAction,
        SourceId, compile_yaml,
    };
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn creates_missing_config_ancestors_then_installs_starter_and_companion_directories() {
        let temporary = TempDir::new().expect("fresh temporary directory");
        let config_path = temporary.path().join("missing-xdg/config/muxe/config.yml");

        let result = install(&config_path).expect("initialization succeeds");

        assert_eq!(result.config_path, config_path);
        assert_eq!(
            fs::read_to_string(&config_path).expect("starter is written"),
            starter_config()
        );
        assert!(result.themes_directory.is_dir());
        assert!(result.color_schemes_directory.is_dir());
        assert!(
            fs::read_dir(config_path.parent().expect("config parent"))
                .expect("config parent is readable")
                .all(|entry| !entry
                    .expect("directory entry")
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp"))
        );
    }

    #[test]
    fn fresh_install_locks_owner_only_modes_and_stays_consumable() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = TempDir::new().expect("unique temporary directory");
        let config_path = temporary.path().join("fresh-xdg/config/muxe/config.yml");

        let result = install(&config_path).expect("initialization succeeds");

        // The source bug: fresh directories shipped OS-default 0755, which the
        // integration and broker pipelines refuse as insecure internal state.
        for directory in [
            config_path.parent().expect("config parent"),
            &result.themes_directory,
            &result.color_schemes_directory,
        ] {
            let mode = fs::symlink_metadata(directory)
                .expect("owned directory exists")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700, "fresh {}", directory.display());
        }
        let file_mode = fs::symlink_metadata(&result.config_path)
            .expect("starter exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600);

        // Immediate consumer use, not field copies: the committed starter reads
        // back byte-identical and compiles, and an owned companion directory
        // accepts a fresh staging file.
        let bytes = fs::read(&result.config_path).expect("starter is readable");
        assert_eq!(bytes, starter_config().as_bytes());
        compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<fresh starter config>"),
            starter_config(),
            KeyCapabilities::default(),
            None,
        )
        .expect("fresh starter compiles");
        let probe = result
            .themes_directory
            .join(".probe.muxe-init-regression.tmp");
        fs::write(&probe, b"probe").expect("owned directory is writable");
        assert_eq!(fs::read(&probe).expect("probe reads back"), b"probe");
        fs::remove_file(&probe).expect("probe cleans up");
    }

    #[test]
    fn starter_compiles_with_the_builtin_escape_exit() {
        let compiled = compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<starter config>"),
            starter_config(),
            KeyCapabilities::default(),
            None,
        )
        .expect("starter config compiles with default built-in bindings");
        let main = compiled
            .menu(&MenuId::new("main"))
            .expect("starter main menu exists");

        assert!(main.bindings.iter().any(|binding| {
            binding.key.canonical_string() == "esc"
                && matches!(
                    binding.action,
                    ActionSpec::Portable(PortableAction::Menu(MenuAction::Quit))
                )
        }));
    }

    #[test]
    fn existing_configuration_and_host_files_are_preserved() {
        let temporary = TempDir::new().expect("fresh temporary directory");
        let config_directory = temporary.path().join("muxe");
        fs::create_dir(&config_directory).expect("config directory exists");
        let config_path = config_directory.join("config.yml");
        fs::write(&config_path, "user-authored configuration\n").expect("user config exists");
        let host_file = config_directory.join("herdr.yml");
        fs::write(&host_file, "user-authored host override\n").expect("user host file exists");

        let error = install(&config_path).expect_err("existing config must fail");

        assert!(matches!(error, InitError::ExistingConfig(path) if path == config_path));
        assert_eq!(
            fs::read_to_string(&config_path).expect("config remains"),
            "user-authored configuration\n"
        );
        assert_eq!(
            fs::read_to_string(&host_file).expect("host file remains"),
            "user-authored host override\n"
        );
        assert!(!config_directory.join("themes").exists());
        assert!(!config_directory.join("color-schemes").exists());
    }

    #[test]
    fn dangling_configuration_symlink_is_never_replaced() {
        use std::os::unix::fs::symlink;

        let temporary = TempDir::new().expect("fresh temporary directory");
        let config_directory = temporary.path().join("muxe");
        fs::create_dir(&config_directory).expect("config directory exists");
        let config_path = config_directory.join("config.yml");
        symlink(config_directory.join("missing.yml"), &config_path)
            .expect("dangling configuration symlink exists");

        let error = install(&config_path).expect_err("dangling config link must fail");

        assert!(matches!(error, InitError::ExistingConfig(path) if path == config_path));
        assert!(
            fs::symlink_metadata(&config_path)
                .expect("link remains")
                .file_type()
                .is_symlink()
        );
        assert!(!config_directory.join("themes").exists());
        assert!(!config_directory.join("color-schemes").exists());
    }

    #[test]
    fn concurrent_initializers_leave_one_starter_configuration() {
        let temporary = TempDir::new().expect("fresh temporary directory");
        let config_path = temporary.path().join("muxe/config.yml");
        let barrier = Arc::new(Barrier::new(2));
        let attempts = (0..2)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let config_path = config_path.clone();
                thread::spawn(move || {
                    barrier.wait();
                    install(&config_path)
                })
            })
            .collect::<Vec<_>>();
        let results = attempts
            .into_iter()
            .map(|attempt| attempt.join().expect("initializer thread completes"))
            .collect::<Vec<_>>();

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(InitError::ExistingConfig(_))))
                .count(),
            1
        );
        assert_eq!(
            fs::read_to_string(&config_path).expect("one starter remains"),
            starter_config()
        );
        assert!(
            config_path
                .parent()
                .expect("config parent")
                .join("themes")
                .is_dir()
        );
        assert!(
            config_path
                .parent()
                .expect("config parent")
                .join("color-schemes")
                .is_dir()
        );
    }
}
