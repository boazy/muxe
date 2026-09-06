//! Owner-only broker registry for activation host selection.
//!
//! `muxe activate` selects every live host recorded in the current user's
//! owner-only broker registry by default. The broker server registers on
//! startup and unregisters on retirement; the coordinator lists live entries
//! with connect-probe liveness. Stale entries are reported, never silently
//! auto-deleted, except through the explicit [`Registry::prune_stale`] call
//! the coordinator makes after probing.

use std::{
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

/// Owner-only registry handle for one cache directory.
#[derive(Clone, Debug)]
pub struct Registry {
    path: PathBuf,
}

impl Registry {
    /// Opens the registry, creating the owner-only directory.
    pub fn open(cache_dir: &Path) -> Result<Self, RegistryError> {
        let directory = cache_dir.join(REGISTRY_DIR_NAME);
        fsutil::ensure_owner_dir(&directory)?;
        Ok(Self {
            path: directory.join(REGISTRY_FILE_NAME),
        })
    }

    /// Registers an endpoint, replacing any record for the same socket.
    pub fn register(&self, entry: BrokerEntry) -> Result<(), RegistryError> {
        let mut file = self.read()?;
        file.brokers.retain(|known| known.socket != entry.socket);
        file.brokers.push(entry);
        self.write(&file)
    }

    /// Removes the record for `socket`. Returns true when one was removed.
    pub fn unregister_socket(&self, socket: &Path) -> Result<bool, RegistryError> {
        let mut file = self.read()?;
        let before = file.brokers.len();
        file.brokers.retain(|known| known.socket != socket);
        let removed = file.brokers.len() != before;
        if removed {
            self.write(&file)?;
        }
        Ok(removed)
    }

    /// Removes every record for a discovery key. Returns the removal count.
    pub fn unregister_discovery(
        &self,
        host_kind: &str,
        discovery_key: &str,
    ) -> Result<usize, RegistryError> {
        let mut file = self.read()?;
        let before = file.brokers.len();
        file.brokers.retain(|known| {
            !(known.host_kind == host_kind && known.discovery_key == discovery_key)
        });
        let removed = before - file.brokers.len();
        if removed > 0 {
            self.write(&file)?;
        }
        Ok(removed)
    }

    /// Returns every recorded entry. A missing registry reads as empty.
    pub fn entries(&self) -> Result<Vec<BrokerEntry>, RegistryError> {
        Ok(self.read()?.brokers)
    }

    /// Probes every entry: a refused or missing socket is stale, an accepted
    /// connection is live. Any other socket error fails closed instead of
    /// guessing.
    pub fn probe(&self) -> Result<Liveness, RegistryError> {
        let mut liveness = Liveness::default();
        for entry in self.entries()? {
            match UnixStream::connect(&entry.socket) {
                Ok(_) => liveness.live.push(entry),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused
                            | std::io::ErrorKind::NotFound
                    ) =>
                {
                    liveness.stale.push(entry);
                }
                Err(source) => {
                    return Err(RegistryError::Fs(fsutil::io_error(
                        "probing broker socket",
                        &entry.socket,
                        source,
                    )));
                }
            }
        }
        Ok(liveness)
    }

    /// Removes every entry that no longer accepts connections.
    pub fn prune_stale(&self) -> Result<usize, RegistryError> {
        let stale_sockets: Vec<PathBuf> = self
            .probe()?
            .stale
            .iter()
            .map(|entry| entry.socket.clone())
            .collect();
        let mut removed = 0;
        for socket in stale_sockets {
            if self.unregister_socket(&socket)? {
                removed += 1;
            }
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
        let bytes =
            serde_json::to_vec_pretty(file).map_err(|source| RegistryError::Corrupt {
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

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(socket: PathBuf) -> BrokerEntry {
        BrokerEntry::now("herdr", "server", socket, 1)
    }

    #[test]
    fn register_and_unregister_round_trip() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let registry = Registry::open(temp.path()).unwrap();
        let socket = temp.path().join("broker.sock");
        registry.register(entry(socket.clone())).unwrap();
        crate::logging::assert_owner_only(&temp.path().join("brokers").join("registry.json"));
        assert_eq!(registry.entries().unwrap().len(), 1);
        assert!(registry.unregister_socket(&socket).unwrap());
        assert!(registry.entries().unwrap().is_empty());
    }

    #[test]
    fn missing_socket_probes_stale() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let registry = Registry::open(temp.path()).unwrap();
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
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let socket = temp.path().join("live.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let registry = Registry::open(temp.path()).unwrap();
        registry.register(entry(socket)).unwrap();
        let liveness = registry.probe().unwrap();
        assert_eq!(liveness.live.len(), 1);
        drop(listener);
    }
}
