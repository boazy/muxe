//! Owner-only broker registry for activation host selection.
//!
//! `muxe activate` selects every live host recorded in the current user's
//! owner-only broker registry by default. The broker server registers on
//! startup and unregisters on retirement; the coordinator lists live entries
//! with connect-probe liveness. Stale entries are reported, never silently
//! auto-deleted, except through the explicit [`Registry::prune_stale`] call
//! the coordinator makes after probing.

use std::{
    fs::File,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fsutil::{self, FsError};

/// Registry directory name under `$CACHE_DIR`.
pub const REGISTRY_DIR_NAME: &str = "brokers";
/// Registry file name.
pub const REGISTRY_FILE_NAME: &str = "registry.json";
/// Registry schema version.
pub const REGISTRY_SCHEMA_VERSION: u32 = 1;
/// Advisory-lock file name beside the registry. The lock serializes every
/// read-modify-write so concurrent broker startups cannot overwrite each
/// other's entries. The inode is persistent: it is never deleted.
const REGISTRY_LOCK_FILE_NAME: &str = "registry.json.lock";

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error("registry at {} is corrupt: {source}", path.display())]
    Corrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("registry at {} uses unsupported schema version {version}", path.display())]
    UnsupportedVersion { path: PathBuf, version: u32 },
}

/// One registered broker endpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrokerEntry {
    /// `zellij` or `herdr`.
    pub host_kind: String,
    /// Broker discovery key (Herdr socket identity or Zellij session identity).
    pub discovery_key: String,
    /// Owner-only broker socket path.
    pub socket: PathBuf,
    /// Server process ID at registration time.
    pub server_pid: u32,
    /// Registration time as Unix epoch seconds.
    pub started_at: u64,
    /// Canonical stable bridge path (Zellij entries only; drives group selection).
    pub bridge_path: Option<PathBuf>,
    /// Live-server identity string (Zellij session name or Herdr server ID).
    pub live_server: Option<String>,
}

impl BrokerEntry {
    /// Builds an entry stamped with the current time.
    #[must_use]
    pub fn now(
        host_kind: impl Into<String>,
        discovery_key: impl Into<String>,
        socket: PathBuf,
        server_pid: u32,
    ) -> Self {
        Self {
            host_kind: host_kind.into(),
            discovery_key: discovery_key.into(),
            socket,
            server_pid,
            started_at: unix_now(),
            bridge_path: None,
            live_server: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RegistryFile {
    schema_version: u32,
    brokers: Vec<BrokerEntry>,
}

/// Liveness split from a connect probe.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Liveness {
    pub live: Vec<BrokerEntry>,
    pub stale: Vec<BrokerEntry>,
}

/// Ownership-scoped cleanup token for one registered endpoint.
///
/// `register` returns the exact entry it stored. [`Registry::unregister`]
/// removes only that exact entry, so an old broker shutting down after its
/// target replaced the same normal socket can never erase the target's record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Registration {
    entry: BrokerEntry,
}

impl Registration {
    /// The exact entry this token owns.
    #[must_use]
    pub fn entry(&self) -> &BrokerEntry {
        &self.entry
    }
}

/// Process-wide advisory lock held across one complete registry
/// read-modify-write. Mirrors the logging rotation lock: the lock inode is
/// persistent and never deleted, and a replacement race fails closed.
#[derive(Debug)]
struct RegistryLock {
    _file: File,
}

impl RegistryLock {
    fn acquire(lock_path: &Path) -> Result<Self, RegistryError> {
        let file = fsutil::open_owner_file(lock_path, false)?;
        file.lock().map_err(|source| {
            RegistryError::Fs(fsutil::io_error(
                "locking broker registry",
                lock_path,
                source,
            ))
        })?;
        fsutil::verify_owner_file_descriptor(lock_path, &file)?;
        Ok(Self { _file: file })
    }
}
/// Owner-only registry handle for one cache directory.
#[derive(Clone, Debug)]
pub struct Registry {
    path: PathBuf,
    lock_path: PathBuf,
}

impl Registry {
    /// Opens the registry, creating the owner-only directory.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the owner-only directory cannot be created or validated.
    pub fn open(cache_dir: &Path) -> Result<Self, RegistryError> {
        let directory = cache_dir.join(REGISTRY_DIR_NAME);
        fsutil::ensure_owner_dir(&directory)?;
        Ok(Self {
            lock_path: directory.join(REGISTRY_LOCK_FILE_NAME),
            path: directory.join(REGISTRY_FILE_NAME),
        })
    }

    /// Registers an endpoint, replacing any record for the same socket.
    ///
    /// The whole read-modify-write holds the registry advisory lock, so
    /// concurrent broker startups cannot overwrite each other's entries.
    /// Returns an ownership-scoped token; only [`Registry::unregister`] with
    /// that token removes this record.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be locked, read, or written.
    pub fn register(&self, entry: BrokerEntry) -> Result<Registration, RegistryError> {
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        file.brokers.retain(|known| known.socket != entry.socket);
        file.brokers.push(entry.clone());
        self.write(&file)?;
        Ok(Registration { entry })
    }

    /// Removes only the exact entry owned by `registration`. Returns true
    /// when it was present. An old broker shutting down after its target
    /// replaced the same normal socket keeps the target's record.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be locked, read, or written.
    pub fn unregister(&self, registration: &Registration) -> Result<bool, RegistryError> {
        self.unregister_entry(&registration.entry)
    }

    /// Removes only the exact observed `entry`. Coordinator form of
    /// [`Registry::unregister`] for observations (probes, planned units)
    /// rather than owned registration tokens. Returns true when present.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be locked, read, or written.
    pub fn unregister_entry(&self, entry: &BrokerEntry) -> Result<bool, RegistryError> {
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        let before = file.brokers.len();
        file.brokers.retain(|known| known != entry);
        let removed = file.brokers.len() != before;
        if removed {
            self.write(&file)?;
        }
        Ok(removed)
    }
}

impl Registry {
    /// Returns every recorded entry. A missing registry reads as empty.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be read or is corrupt.
    pub fn entries(&self) -> Result<Vec<BrokerEntry>, RegistryError> {
        Ok(self.read()?.brokers)
    }

    /// Probes every entry: a refused or missing socket is stale, an accepted
    /// connection is live. Any other socket error fails closed instead of
    /// guessing.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be read or a socket probe fails unexpectedly.
    pub fn probe(&self) -> Result<Liveness, RegistryError> {
        let mut liveness = Liveness::default();
        for entry in self.entries()? {
            if socket_is_stale(&entry.socket)? {
                liveness.stale.push(entry);
            } else {
                liveness.live.push(entry);
            }
        }
        Ok(liveness)
    }

    /// Removes every observed-stale entry that still refuses connections.
    ///
    /// Each observed entry is re-probed under the registry lock and only the
    /// exact observed entry is removed, so a replacement that rebound the
    /// same socket after the probe is never deleted.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be locked, read, written, or probed.
    pub fn prune_stale(&self) -> Result<usize, RegistryError> {
        let stale: Vec<BrokerEntry> = self.probe()?.stale;
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        let mut removed = 0;
        for observed in stale {
            if !file.brokers.contains(&observed) {
                continue;
            }
            if !socket_is_stale(&observed.socket)? {
                continue;
            }
            file.brokers.retain(|known| *known != observed);
            removed += 1;
        }
        if removed > 0 {
            self.write(&file)?;
        }
        Ok(removed)
    }

    fn read(&self) -> Result<RegistryFile, RegistryError> {
        match std::fs::read(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(RegistryFile {
                schema_version: REGISTRY_SCHEMA_VERSION,
                brokers: Vec::new(),
            }),
            Err(source) => Err(RegistryError::Fs(fsutil::io_error(
                "reading broker registry",
                &self.path,
                source,
            ))),
            Ok(bytes) => {
                fsutil::check_owner_file(&self.path)?;
                let file: RegistryFile =
                    serde_json::from_slice(&bytes).map_err(|source| RegistryError::Corrupt {
                        path: self.path.clone(),
                        source,
                    })?;
                if file.schema_version != REGISTRY_SCHEMA_VERSION {
                    return Err(RegistryError::UnsupportedVersion {
                        path: self.path.clone(),
                        version: file.schema_version,
                    });
                }
                Ok(file)
            }
        }
    }

    fn write(&self, file: &RegistryFile) -> Result<(), RegistryError> {
        let bytes = serde_json::to_vec_pretty(file).map_err(|source| RegistryError::Corrupt {
            path: self.path.clone(),
            source,
        })?;
        if let Some(parent) = self.path.parent() {
            fsutil::ensure_owner_dir(parent)?;
        }
        fsutil::write_atomic(&self.path, &bytes, "registry")?;
        Ok(())
    }
}

/// Connect-probes one broker socket: refused or missing is stale, accepted is
/// live. Any other socket error fails closed instead of guessing.
fn socket_is_stale(socket: &Path) -> Result<bool, RegistryError> {
    match UnixStream::connect(socket) {
        Ok(_) => Ok(false),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(true)
        }
        Err(source) => Err(RegistryError::Fs(fsutil::io_error(
            "probing broker socket",
            socket,
            source,
        ))),
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_registry(dir: &Path) -> Registry {
        std::fs::set_permissions(dir, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        Registry::open(dir).unwrap()
    }

    fn entry(socket: PathBuf) -> BrokerEntry {
        BrokerEntry::now("herdr", "server", socket, 1)
    }

    #[test]
    fn register_and_unregister_round_trip() {
        let temp = tempfile::TempDir::new().unwrap();
        let registry = test_registry(temp.path());
        let socket = temp.path().join("broker.sock");
        let registration = registry.register(entry(socket)).unwrap();
        crate::logging::assert_owner_only(&temp.path().join("brokers").join("registry.json"));
        assert_eq!(registry.entries().unwrap().len(), 1);
        assert!(registry.unregister(&registration).unwrap());
        assert!(!registry.unregister(&registration).unwrap());
        assert!(registry.entries().unwrap().is_empty());
    }

    #[test]
    fn old_unregister_after_target_replacement_retains_target() {
        let temp = tempfile::TempDir::new().unwrap();
        let registry = test_registry(temp.path());
        let socket = temp.path().join("normal.sock");
        let mut old = entry(socket.clone());
        old.server_pid = 100;
        old.started_at = 1;
        let mut target = entry(socket);
        target.server_pid = 200;
        target.started_at = 2;
        let old_registration = registry.register(old).unwrap();
        let target_registration = registry.register(target.clone()).unwrap();
        assert!(!registry.unregister(&old_registration).unwrap());
        assert_eq!(registry.entries().unwrap(), vec![target]);
        assert!(registry.unregister(&target_registration).unwrap());
        assert!(registry.entries().unwrap().is_empty());
    }

    #[test]
    fn missing_socket_probes_stale() {
        let temp = tempfile::TempDir::new().unwrap();
        let registry = test_registry(temp.path());
        registry
            .register(entry(temp.path().join("gone.sock")))
            .unwrap();
        let liveness = registry.probe().unwrap();
        assert!(liveness.live.is_empty());
        assert_eq!(liveness.stale.len(), 1);
        assert_eq!(registry.prune_stale().unwrap(), 1);
    }

    #[test]
    fn live_socket_probes_live() {
        let temp = tempfile::TempDir::new().unwrap();
        let socket = temp.path().join("live.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let registry = test_registry(temp.path());
        registry.register(entry(socket)).unwrap();
        let liveness = registry.probe().unwrap();
        assert_eq!(liveness.live.len(), 1);
        drop(listener);
    }

    #[test]
    fn prune_keeps_entry_whose_socket_rebound_live() {
        let temp = tempfile::TempDir::new().unwrap();
        let registry = test_registry(temp.path());
        let socket = temp.path().join("rebound.sock");
        registry.register(entry(socket.clone())).unwrap();
        assert!(
            registry
                .probe()
                .unwrap()
                .stale
                .iter()
                .any(|stale| stale.socket == socket)
        );
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        assert_eq!(registry.prune_stale().unwrap(), 0);
        assert_eq!(registry.entries().unwrap().len(), 1);
    }

    #[test]
    fn prune_keeps_live_replacement_over_stale_observation() {
        let temp = tempfile::TempDir::new().unwrap();
        let registry = test_registry(temp.path());
        let socket = temp.path().join("normal.sock");
        let mut old = entry(socket.clone());
        old.server_pid = 100;
        old.started_at = 1;
        registry.register(old).unwrap();
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let mut target = entry(socket);
        target.server_pid = 200;
        target.started_at = 2;
        registry.register(target.clone()).unwrap();
        assert_eq!(registry.prune_stale().unwrap(), 0);
        assert_eq!(registry.entries().unwrap(), vec![target]);
    }

    #[test]
    fn registry_concurrent_child_registers() {
        let Some(directory) = std::env::var_os("MUXE_REGISTRY_TEST_DIRECTORY") else {
            return;
        };
        let child_id = std::env::var("MUXE_REGISTRY_TEST_CHILD_ID").unwrap();
        let registry = Registry::open(Path::new(&directory)).unwrap();
        let mut child_entry = entry(Path::new(&directory).join(format!("child-{child_id}.sock")));
        child_entry.server_pid = std::process::id();
        child_entry.started_at = 1 + child_id.parse::<u64>().unwrap();
        registry.register(child_entry).unwrap();
    }

    #[test]
    fn concurrent_processes_keep_distinct_registrations() {
        const CHILDREN: usize = 8;
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let executable = std::env::current_exe().unwrap();
        let mut children = Vec::new();
        for child_id in 0..CHILDREN {
            children.push(
                std::process::Command::new(&executable)
                    .args([
                        "--exact",
                        "lifecycle::registry::tests::registry_concurrent_child_registers",
                        "--nocapture",
                    ])
                    .env("MUXE_REGISTRY_TEST_DIRECTORY", temp.path())
                    .env("MUXE_REGISTRY_TEST_CHILD_ID", child_id.to_string())
                    .spawn()
                    .unwrap(),
            );
        }
        for mut child in children {
            assert!(child.wait().unwrap().success());
        }
        let registry = Registry::open(temp.path()).unwrap();
        let mut sockets: Vec<PathBuf> = registry
            .entries()
            .unwrap()
            .iter()
            .map(|known| known.socket.clone())
            .collect();
        sockets.sort();
        let mut expected: Vec<PathBuf> = (0..CHILDREN)
            .map(|child_id| temp.path().join(format!("child-{child_id}.sock")))
            .collect();
        expected.sort();
        assert_eq!(sockets, expected);
    }
}
