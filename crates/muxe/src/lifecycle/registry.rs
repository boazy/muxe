//! Owner-only broker registry for activation host selection.
//!
//! `muxe activate` selects every live host recorded in the current user's
//! owner-only broker registry by default. The broker server registers on
//! startup and unregisters on retirement. Storage reads and connect probes return
//! untrusted [`BrokerEntry`] DTOs; concrete selected validators produce
//! [`RegisteredBroker`] lifecycle state. Neither parsing nor connectivity is live
//! authorization. Exact snapshot, endpoint, and selected-authority checks remain
//! required before reuse or mutation.

use std::{
    fs::File,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use muxe_protocol::{
    control::{BrokerRegistrationId, HandoffId},
    wire::HostKind,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    fsutil::{self, FsError},
    lifecycle::journal::{self, UnitKind, UnitLock, UnitLockAttempt},
    paths::BridgeIdentity,
};

/// Registry directory name under `$CACHE_DIR`.
pub const REGISTRY_DIR_NAME: &str = "brokers";
/// Registry file name.
pub const REGISTRY_FILE_NAME: &str = "registry.json";
/// Registry schema version.
pub const REGISTRY_SCHEMA_VERSION: u32 = 2;
/// Advisory-lock file name beside the registry. The lock serializes every
/// read-modify-write so concurrent broker startups cannot overwrite each
/// other's entries. The inode is persistent: it is never deleted.
const REGISTRY_LOCK_FILE_NAME: &str = "registry.json.lock";

/// Closed registry schema spelling, used only at the persistence boundary.
pub(crate) fn persisted_host_label(host: HostKind) -> &'static str {
    match host {
        HostKind::Herdr => "herdr",
        HostKind::Zellij => "zellij",
    }
}

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
    #[error("Zellij registry mutation is not authorized: {0}")]
    Unauthorized(String),
    #[error("broker registry ownership conflict: {0}")]
    Conflict(String),
    #[error("cannot mint broker registration identity: {0}")]
    Entropy(String),
}

/// Untrusted persistence DTO; loading or connect-probing it proves no host authority.
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
    /// Fresh process-scoped registration identity; absent on legacy rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_id: Option<BrokerRegistrationId>,
    /// Descriptor-validated physical bridge authority (Zellij only).
    pub bridge_identity: Option<BridgeIdentity>,
    /// Logical bridge-group member (Zellij only).
    pub bridge_member: Option<BridgeMemberId>,
    /// Journal-authorized target handoff, absent for ordinary registration.
    pub handoff_id: Option<HandoffId>,
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
            registration_id: None,
            started_at: unix_now(),
            bridge_identity: None,
            bridge_member: None,
            handoff_id: None,
            live_server: None,
        }
    }

    /// Wraps the persisted host label before lifecycle code compares it.
    ///
    /// # Errors
    ///
    /// Rejects an unknown registry host label.
    pub(crate) fn parsed_host_kind(&self) -> Result<HostKind, RegistryError> {
        match self.host_kind.as_str() {
            "herdr" => Ok(HostKind::Herdr),
            "zellij" => Ok(HostKind::Zellij),
            _ => Err(RegistryError::Unauthorized(
                "registry row carries an unknown host kind".to_owned(),
            )),
        }
    }
}

/// Discovery identity recorded on disk, not an authenticated live host identity.
///
/// Unlike `HostDiscoveryKey`, historical registry values may be empty. Validation
/// here preserves those bytes; operation-specific live attestation remains required.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedDiscoveryKey(String);

impl RecordedDiscoveryKey {
    /// Wraps a value at the persistence boundary without strengthening its contract.
    #[must_use]
    pub fn from_recorded(value: String) -> Self {
        Self(value)
    }

    /// Returns the spelling at an IPC, journal, or persistence boundary.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RecordedDiscoveryKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Persisted process identifier; zero is admissible and is not live proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordedServerPid(u32);

impl RecordedServerPid {
    /// Wraps a process value at the persistence boundary.
    #[must_use]
    pub fn from_recorded(value: u32) -> Self {
        Self(value)
    }

    /// Returns the process value at an OS or persistence boundary.
    #[must_use]
    pub fn get(self) -> u32 {
        self.0
    }
}

/// Host-format-validated lifecycle state, distinct from a live authorization proof.
///
/// Concrete selected validators construct this state. The retained host enum exists
/// only for persistence/exact-snapshot conversion, never to reselect lifecycle policy.
/// Registry mutations still independently require the caller's sealed authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredBroker {
    persisted_host: HostKind,
    discovery_key: RecordedDiscoveryKey,
    socket: PathBuf,
    server_pid: RecordedServerPid,
    started_at: u64,
    registration_id: Option<BrokerRegistrationId>,
    bridge_identity: Option<BridgeIdentity>,
    bridge_member: Option<BridgeMemberId>,
    handoff_id: Option<HandoffId>,
    live_server: Option<muxe_protocol::wire::ServerId>,
}

impl RegisteredBroker {
    pub(crate) fn herdr(entry: BrokerEntry) -> Result<Self, RegistryError> {
        validate_herdr_entry(&entry)?;
        Ok(Self::from_validated(entry, HostKind::Herdr))
    }

    pub(crate) fn zellij(
        entry: BrokerEntry,
        identity: &BridgeIdentity,
    ) -> Result<Self, RegistryError> {
        validate_zellij_entry(&entry, identity, entry.handoff_id)?;
        Ok(Self::from_validated(entry, HostKind::Zellij))
    }

    #[must_use]
    pub fn discovery_key(&self) -> &RecordedDiscoveryKey {
        &self.discovery_key
    }

    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Moves the endpoint path out when crossing to an external client API.
    #[must_use]
    pub fn into_socket(self) -> PathBuf {
        self.socket
    }

    #[must_use]
    pub fn server_pid(&self) -> RecordedServerPid {
        self.server_pid
    }

    #[must_use]
    pub fn registration_id(&self) -> Option<BrokerRegistrationId> {
        self.registration_id
    }

    #[must_use]
    pub fn bridge_identity(&self) -> Option<&BridgeIdentity> {
        self.bridge_identity.as_ref()
    }

    #[must_use]
    pub fn bridge_member(&self) -> Option<&BridgeMemberId> {
        self.bridge_member.as_ref()
    }

    #[must_use]
    pub fn handoff_id(&self) -> Option<HandoffId> {
        self.handoff_id
    }

    #[must_use]
    pub fn live_server(&self) -> Option<&muxe_protocol::wire::ServerId> {
        self.live_server.as_ref()
    }

    /// Validates a concretely selected Zellij row while consuming its own identity.
    pub(crate) fn recorded_zellij(entry: BrokerEntry) -> Result<Self, RegistryError> {
        let identity = entry.bridge_identity.as_ref().ok_or_else(|| {
            RegistryError::Unauthorized("Zellij entry lacks canonical bridge authority".to_owned())
        })?;
        validate_zellij_entry(&entry, identity, entry.handoff_id)?;
        Ok(Self::from_validated(entry, HostKind::Zellij))
    }

    fn from_validated(entry: BrokerEntry, persisted_host: HostKind) -> Self {
        Self {
            persisted_host,
            discovery_key: RecordedDiscoveryKey::from_recorded(entry.discovery_key),
            socket: entry.socket,
            server_pid: RecordedServerPid::from_recorded(entry.server_pid),
            started_at: entry.started_at,
            registration_id: entry.registration_id,
            bridge_identity: entry.bridge_identity,
            bridge_member: entry.bridge_member,
            handoff_id: entry.handoff_id,
            live_server: entry.live_server.map(muxe_protocol::wire::ServerId::new),
        }
    }

    /// Rebuilds the exact DTO at a journal, persistence, or snapshot boundary.
    #[must_use]
    pub(crate) fn recorded_entry(&self) -> BrokerEntry {
        BrokerEntry {
            host_kind: persisted_host_label(self.persisted_host).to_owned(),
            discovery_key: self.discovery_key.as_str().to_owned(),
            socket: self.socket.clone(),
            server_pid: self.server_pid.get(),
            started_at: self.started_at,
            registration_id: self.registration_id,
            bridge_identity: self.bridge_identity.clone(),
            bridge_member: self.bridge_member.clone(),
            handoff_id: self.handoff_id,
            live_server: self.live_server.as_ref().map(|id| id.as_str().to_owned()),
        }
    }

    /// Compares an exact persisted snapshot without rebuilding or copying it.
    #[must_use]
    pub fn matches_recorded(&self, entry: &BrokerEntry) -> bool {
        entry.host_kind == persisted_host_label(self.persisted_host)
            && entry.discovery_key == self.discovery_key.as_str()
            && entry.socket == self.socket
            && entry.server_pid == self.server_pid.get()
            && entry.started_at == self.started_at
            && entry.registration_id == self.registration_id
            && entry.bridge_identity == self.bridge_identity
            && entry.bridge_member == self.bridge_member
            && entry.handoff_id == self.handoff_id
            && entry.live_server.as_deref()
                == self
                    .live_server
                    .as_ref()
                    .map(muxe_protocol::ServerId::as_str)
    }
}
/// Stable logical member of a bridge-sharing group.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct BridgeMemberId(String);

impl BridgeMemberId {
    /// Constructs a member from the validated discovery key.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the member is empty or contains NUL.
    pub fn new(value: String) -> Result<Self, RegistryError> {
        if value.is_empty() || value.contains('\0') {
            return Err(RegistryError::Unauthorized(
                "bridge member is empty or contains NUL".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the discovery spelling at a process/wire boundary.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Sorted, duplicate-free logical bridge membership.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct MemberCensus(Vec<BridgeMemberId>);

impl MemberCensus {
    /// Normalizes an exact census and rejects duplicate logical members.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when an entry carries another identity,
    /// lacks a typed member, or duplicates a logical member.
    pub fn from_entries<'a>(
        identity: &BridgeIdentity,
        entries: impl IntoIterator<Item = &'a BrokerEntry>,
    ) -> Result<Self, RegistryError> {
        let mut members = Vec::new();
        for entry in entries {
            if entry.bridge_identity.as_ref() != Some(identity) {
                return Err(RegistryError::Unauthorized(
                    "registry entry carries another bridge identity".to_owned(),
                ));
            }
            let member = entry.bridge_member.clone().ok_or_else(|| {
                RegistryError::Unauthorized("Zellij entry lacks a typed bridge member".to_owned())
            })?;
            if member.as_str() != entry.discovery_key {
                return Err(RegistryError::Unauthorized(
                    "typed bridge member disagrees with discovery key".to_owned(),
                ));
            }
            members.push(member);
        }
        members.sort_unstable();
        let original_len = members.len();
        members.dedup();
        if members.len() != original_len {
            return Err(RegistryError::Unauthorized(
                "bridge census contains duplicate logical members".to_owned(),
            ));
        }
        Ok(Self(members))
    }
    /// Normalizes an explicit logical member list.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the member list contains duplicates.
    pub fn from_members(mut members: Vec<BridgeMemberId>) -> Result<Self, RegistryError> {
        members.sort_unstable();
        let original_len = members.len();
        members.dedup();
        if members.len() != original_len {
            return Err(RegistryError::Unauthorized(
                "bridge census contains duplicate logical members".to_owned(),
            ));
        }
        Ok(Self(members))
    }

    /// Returns the normalized members.
    #[must_use]
    pub fn members(&self) -> &[BridgeMemberId] {
        &self.0
    }
}

/// Cache lease plus exclusive bridge-unit ownership.
#[derive(Debug)]
pub struct BridgeUnitGuard {
    identity: BridgeIdentity,
    _lock: UnitLock,
}

impl BridgeUnitGuard {
    /// Acquires the bridge-unit guard in the required cache -> unit order.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the cache lease or bridge-unit lock
    /// cannot be acquired.
    pub fn acquire(cache_dir: &Path, identity: BridgeIdentity) -> Result<Self, RegistryError> {
        let unit = UnitKind::Zellij {
            bridge_unit: identity.unit(),
        };
        let lock = journal::acquire_unit_lock(cache_dir, &unit)
            .map_err(|error| RegistryError::Unauthorized(error.to_string()))?;
        Ok(Self {
            identity,
            _lock: lock,
        })
    }
    /// Tries the same bridge-unit lock without blocking a live coordinator.
    ///
    /// # Errors
    ///
    /// Returns a registry error for invalid lock/cache ownership.
    pub fn try_acquire(
        cache_dir: &Path,
        identity: BridgeIdentity,
    ) -> Result<Option<Self>, RegistryError> {
        let unit = UnitKind::Zellij {
            bridge_unit: identity.unit(),
        };
        match journal::try_acquire_unit_lock(cache_dir, &unit)
            .map_err(|error| RegistryError::Unauthorized(error.to_string()))?
        {
            UnitLockAttempt::Acquired(lock) => Ok(Some(Self {
                identity,
                _lock: lock,
            })),
            UnitLockAttempt::Active => Ok(None),
        }
    }

    /// Acquires the same guard with a blocking flock for serialized cleanup.
    ///
    /// Call only from a blocking thread; activation targets must not use this
    /// while their parent coordinator owns the unit.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the cache lease or bridge-unit lock
    /// cannot be acquired.
    pub fn acquire_blocking(
        cache_dir: &Path,
        identity: BridgeIdentity,
    ) -> Result<Self, RegistryError> {
        let unit = UnitKind::Zellij {
            bridge_unit: identity.unit(),
        };
        let lock = journal::acquire_unit_lock_blocking(cache_dir, &unit)
            .map_err(|error| RegistryError::Unauthorized(error.to_string()))?;
        Ok(Self {
            identity,
            _lock: lock,
        })
    }

    /// The exact physical bridge authority protected by this guard.
    #[must_use]
    pub fn identity(&self) -> &BridgeIdentity {
        &self.identity
    }
}

mod authority_seal {
    pub trait Sealed {}
}

/// The caller's already-selected host authority, never inferred from an
/// untrusted persisted `host_kind` label. Implementations validate that label
/// and every host-specific field before a registry mutation or reuse.
pub(crate) trait RegistryAuthority: authority_seal::Sealed {
    fn validate_entry(&self, entry: &BrokerEntry) -> Result<(), RegistryError>;
    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError>;
}

/// Herdr coldstart retains its unit lock while reconciling registry ownership.
#[derive(Debug)]
pub(crate) struct HerdrUnitGuard {
    _lock: UnitLock,
}

impl HerdrUnitGuard {
    pub(crate) fn new(lock: UnitLock) -> Self {
        Self { _lock: lock }
    }
}

/// Retirement has exact-entry ownership but no Herdr unit lock, as before.
pub(crate) struct HerdrRegistryAuthority;

impl authority_seal::Sealed for HerdrUnitGuard {}
impl authority_seal::Sealed for HerdrRegistryAuthority {}
impl authority_seal::Sealed for BridgeUnitGuard {}

impl RegistryAuthority for HerdrUnitGuard {
    fn validate_entry(&self, entry: &BrokerEntry) -> Result<(), RegistryError> {
        validate_herdr_entry(entry)
    }

    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError> {
        RegisteredBroker::herdr(entry)
    }
}

impl RegistryAuthority for HerdrRegistryAuthority {
    fn validate_entry(&self, entry: &BrokerEntry) -> Result<(), RegistryError> {
        validate_herdr_entry(entry)
    }

    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError> {
        RegisteredBroker::herdr(entry)
    }
}

impl RegistryAuthority for BridgeUnitGuard {
    fn validate_entry(&self, entry: &BrokerEntry) -> Result<(), RegistryError> {
        validate_zellij_entry(entry, &self.identity, entry.handoff_id)
    }

    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError> {
        RegisteredBroker::zellij(entry, &self.identity)
    }
}

/// Exact journal-derived permission for one same-member target replacement.
#[derive(Clone, Debug)]
pub struct TargetRegistrationCapability {
    bridge_identity: BridgeIdentity,
    member: BridgeMemberId,
    endpoint: PathBuf,
    discovery_key: String,
    handoff_id: HandoffId,
}

impl TargetRegistrationCapability {
    pub(crate) fn new(
        bridge_identity: BridgeIdentity,
        member: BridgeMemberId,
        endpoint: PathBuf,
        discovery_key: String,
        handoff_id: HandoffId,
    ) -> Self {
        Self {
            bridge_identity,
            member,
            endpoint,
            discovery_key,
            handoff_id,
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
    pub fn register_herdr(&self, entry: BrokerEntry) -> Result<Registration, RegistryError> {
        validate_herdr_entry(&entry)?;
        self.register_inner(entry)
    }

    /// Registers an ordinary Zellij member while holding its bridge-unit guard.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the guard or entry authority is invalid
    /// or the registry cannot be updated.
    pub fn register_zellij(
        &self,
        guard: &BridgeUnitGuard,
        entry: BrokerEntry,
    ) -> Result<Registration, RegistryError> {
        validate_zellij_entry(&entry, guard.identity(), None)?;
        self.register_inner(entry)
    }

    /// Replaces exactly one existing logical member under journal authority.
    ///
    /// This capability cannot add a member to the census.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the capability does not authorize an
    /// exact replacement or the registry cannot be updated.
    pub fn register_zellij_target(
        &self,
        capability: &TargetRegistrationCapability,
        entry: BrokerEntry,
    ) -> Result<Registration, RegistryError> {
        validate_zellij_entry(
            &entry,
            &capability.bridge_identity,
            Some(capability.handoff_id),
        )?;
        if entry.bridge_member.as_ref() != Some(&capability.member)
            || entry.socket != capability.endpoint
            || entry.discovery_key != capability.discovery_key
        {
            return Err(RegistryError::Unauthorized(
                "target registration exceeds its exact member capability".to_owned(),
            ));
        }
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        let replaceable = file.brokers.iter().any(|known| {
            known.host_kind == "zellij"
                && known.bridge_identity.as_ref() == Some(&capability.bridge_identity)
                && known.bridge_member.as_ref() == Some(&capability.member)
                && known.socket == capability.endpoint
        });
        if !replaceable {
            return Err(RegistryError::Unauthorized(
                "target capability cannot add a logical bridge member".to_owned(),
            ));
        }
        file.brokers.retain(|known| known.socket != entry.socket);
        file.brokers.push(entry.clone());
        self.write(&file)?;
        Ok(Registration { entry })
    }

    /// Atomically replaces the exact journal-authorized target incarnation
    /// with the exact old row while the coordinator owns the bridge unit.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the old row or current registry
    /// incarnation is not authorized by the capability.
    pub fn restore_zellij_target(
        &self,
        capability: &TargetRegistrationCapability,
        old_entry: &BrokerEntry,
    ) -> Result<bool, RegistryError> {
        validate_zellij_entry(old_entry, &capability.bridge_identity, old_entry.handoff_id)?;
        if old_entry.bridge_member.as_ref() != Some(&capability.member)
            || old_entry.socket != capability.endpoint
            || old_entry.discovery_key != capability.discovery_key
        {
            return Err(RegistryError::Unauthorized(
                "old row exceeds the target capability's exact member".to_owned(),
            ));
        }
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        if file.brokers.contains(old_entry) {
            return Ok(false);
        }
        let target_index = file.brokers.iter().position(|known| {
            known.host_kind == "zellij"
                && known.bridge_identity.as_ref() == Some(&capability.bridge_identity)
                && known.bridge_member.as_ref() == Some(&capability.member)
                && known.socket == capability.endpoint
                && known.discovery_key == capability.discovery_key
                && known.handoff_id == Some(capability.handoff_id)
        });
        if let Some(target_index) = target_index {
            file.brokers[target_index] = old_entry.clone();
        } else {
            let conflicting = file.brokers.iter().any(|known| {
                known.socket == capability.endpoint
                    || (known.bridge_identity.as_ref() == Some(&capability.bridge_identity)
                        && known.bridge_member.as_ref() == Some(&capability.member))
            });
            if conflicting {
                return Err(RegistryError::Unauthorized(
                    "another registry incarnation occupies the journal member".to_owned(),
                ));
            }
            file.brokers.push(old_entry.clone());
        }
        self.write(&file)?;
        Ok(true)
    }

    fn register_inner(&self, entry: BrokerEntry) -> Result<Registration, RegistryError> {
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        file.brokers.retain(|known| known.socket != entry.socket);
        file.brokers.push(entry.clone());
        self.write(&file)?;
        Ok(Registration { entry })
    }
    #[cfg(test)]
    pub(crate) fn register(&self, entry: BrokerEntry) -> Result<Registration, RegistryError> {
        self.register_inner(entry)
    }

    /// Removes only the exact entry owned by `registration`. Returns true
    /// when it was present. An old broker shutting down after its target
    /// replaced the same normal socket keeps the target's record.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be locked, read, or written.
    pub fn unregister_herdr(&self, registration: &Registration) -> Result<bool, RegistryError> {
        if registration.entry.host_kind != "herdr" {
            return Err(RegistryError::Unauthorized(
                "non-Herdr cleanup requires a bridge-unit guard".to_owned(),
            ));
        }
        self.unregister_owned_inner(&registration.entry)
    }

    /// Removes an exact Zellij registration under the same unit guard.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registration does not match the
    /// guard or the registry cannot be updated.
    pub fn unregister_zellij(
        &self,
        guard: &BridgeUnitGuard,
        registration: &Registration,
    ) -> Result<bool, RegistryError> {
        validate_zellij_entry(
            &registration.entry,
            guard.identity(),
            registration.entry.handoff_id,
        )?;
        self.unregister_owned_inner(&registration.entry)
    }
    #[cfg(test)]
    fn unregister(&self, registration: &Registration) -> Result<bool, RegistryError> {
        self.unregister_owned_inner(&registration.entry)
    }

    /// Removes this exact process registration even after atomic relocation.
    /// A successor's fresh token cannot match the old broker's cleanup token.
    fn unregister_owned_inner(&self, entry: &BrokerEntry) -> Result<bool, RegistryError> {
        let id = entry.registration_id.ok_or_else(|| {
            RegistryError::Conflict("owned cleanup lacks registration identity".to_owned())
        })?;
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        let before = file.brokers.len();
        file.brokers.retain(|known| {
            known.registration_id != Some(id)
                || known.server_pid != entry.server_pid
                || known.started_at != entry.started_at
                || known.host_kind != entry.host_kind
                || known.discovery_key != entry.discovery_key
                || known.live_server != entry.live_server
                || known.bridge_identity != entry.bridge_identity
                || known.bridge_member != entry.bridge_member
                || known.handoff_id != entry.handoff_id
        });
        let removed = file.brokers.len() != before;
        if removed {
            self.write(&file)?;
        }
        Ok(removed)
    }

    /// Reconciles a same-stream authenticated broker in one guarded write.
    /// The caller owns unit then endpoint locks. `revalidate` inspects the
    /// retained stream's socket identity while this registry lock is held.
    ///
    /// # Errors
    ///
    /// Any snapshot, path, owner, or peer drift refuses mutation.
    pub(crate) fn reconcile_live(
        &self,
        authority: &dyn RegistryAuthority,
        observed: &[BrokerEntry],
        candidate: BrokerEntry,
        revalidate: impl FnOnce() -> Result<(), RegistryError>,
    ) -> Result<BrokerEntry, RegistryError> {
        authority.validate_entry(&candidate)?;
        if candidate
            .registration_id
            .is_none_or(BrokerRegistrationId::is_zero)
            || candidate.server_pid == 0
            || candidate.started_at == 0
            || candidate.live_server.as_deref().is_none_or(str::is_empty)
        {
            return Err(RegistryError::Conflict(
                "live candidate lacks exact process registration".to_owned(),
            ));
        }
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        if file.brokers != observed {
            return Err(RegistryError::Conflict(
                "registry changed since endpoint observation".to_owned(),
            ));
        }
        let mut owners = file.brokers.iter().enumerate().filter(|(_, known)| {
            known.host_kind == candidate.host_kind && known.discovery_key == candidate.discovery_key
        });
        let owner = owners.next().map(|(index, _)| index);
        if owners.next().is_some() {
            return Err(RegistryError::Conflict(
                "ambiguous logical broker owner".to_owned(),
            ));
        }
        if let Some(index) = owner {
            let mut expected = file.brokers[index].clone();
            expected.socket.clone_from(&candidate.socket);
            if expected != candidate {
                return Err(RegistryError::Conflict(
                    "live endpoint differs from the exact recorded owner".to_owned(),
                ));
            }
        }
        if file
            .brokers
            .iter()
            .enumerate()
            .any(|(index, known)| known.socket == candidate.socket && Some(index) != owner)
        {
            return Err(RegistryError::Conflict(
                "another registry owner occupies the live endpoint".to_owned(),
            ));
        }
        revalidate()?;
        match owner {
            Some(index) if file.brokers[index] == candidate => return Ok(candidate),
            Some(index) => file.brokers[index] = candidate.clone(),
            None => file.brokers.push(candidate.clone()),
        }
        self.write(&file)?;
        Ok(candidate)
    }

    /// Verifies an already-recorded legacy peer without adopting or rewriting
    /// its absent process registration token.
    ///
    /// # Errors
    ///
    /// Refuses snapshot, bridge, or endpoint drift under the registry lock.
    pub(crate) fn verify_existing(
        &self,
        authority: &dyn RegistryAuthority,
        observed: &[BrokerEntry],
        entry: &BrokerEntry,
        revalidate: impl FnOnce() -> Result<(), RegistryError>,
    ) -> Result<(), RegistryError> {
        authority.validate_entry(entry)?;
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let file = self.read()?;
        if file.brokers != observed || !file.brokers.contains(entry) {
            return Err(RegistryError::Conflict(
                "legacy registry owner changed before reuse".to_owned(),
            ));
        }
        revalidate()
    }

    /// Removes one exact dead row only after an endpoint/PID recheck executed
    /// under the registry lock. No bulk prune bypasses this authority.
    ///
    /// # Errors
    ///
    /// A changed snapshot, foreign row, or failed recheck refuses removal.
    pub(crate) fn remove_exact_stale(
        &self,
        authority: &dyn RegistryAuthority,
        observed: &[BrokerEntry],
        stale: &BrokerEntry,
        revalidate: impl FnOnce() -> Result<(), RegistryError>,
    ) -> Result<(), RegistryError> {
        authority.validate_entry(stale)?;
        let _lock = RegistryLock::acquire(&self.lock_path)?;
        let mut file = self.read()?;
        if file.brokers != observed || !file.brokers.contains(stale) {
            return Err(RegistryError::Conflict(
                "stale registry observation changed before removal".to_owned(),
            ));
        }
        revalidate()?;
        file.brokers.retain(|known| known != stale);
        self.write(&file)
    }

    /// Removes only the exact observed `entry`. Coordinator form of
    /// [`Registry::unregister`] for observations (probes, planned units)
    /// rather than owned registration tokens. Returns true when present.
    ///
    /// # Errors
    ///
    /// Returns [`RegistryError`] when the registry cannot be locked, read, or written.
    pub(crate) fn unregister_entry(
        &self,
        entry: &BrokerEntry,
        authority: &dyn RegistryAuthority,
    ) -> Result<bool, RegistryError> {
        authority.validate_entry(entry)?;
        self.unregister_entry_inner(entry)
    }

    fn unregister_entry_inner(&self, entry: &BrokerEntry) -> Result<bool, RegistryError> {
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

    /// Selects the first recorded Zellij row for the current session, then validates it.
    ///
    /// Unrelated rows do not participate in current-session admission.
    ///
    /// # Errors
    ///
    /// Returns an error for storage or a malformed selected row.
    pub fn zellij_entry_for_session(
        &self,
        session: &muxe_adapter_api::HostDiscoveryKey,
    ) -> Result<Option<RegisteredBroker>, RegistryError> {
        self.entries()?
            .into_iter()
            .find(|row| {
                row.parsed_host_kind().ok() == Some(HostKind::Zellij)
                    && (row.live_server.as_deref() == Some(session.as_str())
                        || row.discovery_key == session.as_str())
            })
            .map(RegisteredBroker::recorded_zellij)
            .transpose()
    }

    /// Probes all raw endpoints before selecting and validating this Herdr host.
    ///
    /// This preserves probe errors and excludes stale or unrelated malformed rows.
    ///
    /// # Errors
    ///
    /// Returns an error for storage, an unexpected probe failure, or a selected malformed row.
    pub fn live_herdr_entries_for(
        &self,
        discovery: &muxe_adapter_api::HostDiscoveryKey,
    ) -> Result<Vec<RegisteredBroker>, RegistryError> {
        self.probe()?
            .live
            .into_iter()
            .filter(|row| {
                row.parsed_host_kind().ok() == Some(HostKind::Herdr)
                    && row.discovery_key == discovery.as_str()
            })
            .map(RegisteredBroker::herdr)
            .collect()
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

/// Checks an untrusted Herdr row against the unguarded registration format.
fn validate_herdr_entry(entry: &BrokerEntry) -> Result<(), RegistryError> {
    if entry.host_kind != "herdr"
        || entry.bridge_identity.is_some()
        || entry.bridge_member.is_some()
        || entry.handoff_id.is_some()
    {
        return Err(RegistryError::Unauthorized(
            "Herdr registration carries Zellij bridge authority".to_owned(),
        ));
    }
    Ok(())
}

/// Checks an untrusted Zellij row against the retained bridge-unit guard.
fn validate_zellij_entry(
    entry: &BrokerEntry,
    identity: &BridgeIdentity,
    handoff: Option<HandoffId>,
) -> Result<(), RegistryError> {
    if entry.host_kind != "zellij"
        || entry.bridge_identity.as_ref() != Some(identity)
        || entry.handoff_id != handoff
    {
        return Err(RegistryError::Unauthorized(
            "Zellij entry does not match its bridge authority".to_owned(),
        ));
    }
    let member = entry.bridge_member.as_ref().ok_or_else(|| {
        RegistryError::Unauthorized("Zellij entry lacks a typed bridge member".to_owned())
    })?;
    if member.as_str() != entry.discovery_key {
        return Err(RegistryError::Unauthorized(
            "typed bridge member disagrees with discovery key".to_owned(),
        ));
    }
    Ok(())
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
        let mut entry = BrokerEntry::now("herdr", "server", socket, 1);
        entry.registration_id = Some(BrokerRegistrationId::generate().unwrap());
        entry.live_server = Some("server-test".to_owned());
        entry
    }

    #[test]
    fn broker_registration_identity_rejects_zero_on_disk() {
        let zero = serde_json::to_value([0_u8; 16]).unwrap();
        assert!(serde_json::from_value::<BrokerRegistrationId>(zero).is_err());
        let id = BrokerRegistrationId::generate().unwrap();
        assert_eq!(
            serde_json::from_value::<BrokerRegistrationId>(serde_json::to_value(id).unwrap())
                .unwrap(),
            id
        );
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
    fn guarded_live_reconciliation_preserves_owner_and_rejects_changed_snapshot() {
        let temp = tempfile::TempDir::new().unwrap();
        let registry = test_registry(temp.path());
        let endpoint = temp.path().join("endpoint.sock");
        let mut candidate = entry(endpoint);
        candidate.server_pid = std::process::id();
        assert_eq!(
            registry
                .reconcile_live(&HerdrRegistryAuthority, &[], candidate.clone(), || Ok(()))
                .unwrap(),
            candidate,
            "no-row adoption retains attested token and timestamp"
        );
        let original = candidate.clone();
        assert!(
            registry
                .unregister(&Registration {
                    entry: original.clone()
                })
                .unwrap()
        );
        let mut elsewhere = original;
        elsewhere.socket = temp.path().join("elsewhere.sock");
        let token = registry.register(elsewhere).unwrap();
        let observed = registry.entries().unwrap();
        assert_eq!(
            registry
                .reconcile_live(
                    &HerdrRegistryAuthority,
                    &observed,
                    candidate.clone(),
                    || Ok(())
                )
                .unwrap(),
            candidate
        );
        assert_eq!(registry.entries().unwrap(), vec![candidate.clone()]);
        assert!(
            registry.unregister(&token).unwrap(),
            "original token removes relocated row"
        );
        let stale_snapshot = observed;
        registry.register(candidate.clone()).unwrap();
        assert!(
            registry
                .reconcile_live(
                    &HerdrRegistryAuthority,
                    &stale_snapshot,
                    candidate.clone(),
                    || Ok(())
                )
                .is_err()
        );
        assert_eq!(registry.entries().unwrap(), vec![candidate.clone()]);
        let observed = registry.entries().unwrap();
        let mut changed = candidate.clone();
        changed.socket = temp.path().join("rebound.sock");
        assert!(
            registry
                .reconcile_live(&HerdrRegistryAuthority, &observed, changed, || {
                    Err(RegistryError::Conflict("injected socket rebind".to_owned()))
                })
                .is_err()
        );
        assert_eq!(registry.entries().unwrap(), vec![candidate]);
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
    }

    #[test]
    fn live_socket_probes_live() {
        let temp = tempfile::TempDir::new().unwrap();
        let socket = temp.path().join("live.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let registry = test_registry(temp.path());
        registry.register(entry(socket)).unwrap();
        let liveness = registry.probe().unwrap();
        assert_eq!(liveness.live.len(), 1);
    }

    #[test]
    fn registry_concurrent_child_registers() {
        let Some(directory) = std::env::var_os("MUXE_REGISTRY_TEST_DIRECTORY") else {
            return;
        };
        let child_id = std::env::var("MUXE_REGISTRY_TEST_CHILD_ID").unwrap();
        let registry = Registry::open(Path::new(&directory)).unwrap();
        let mut child_entry = entry(Path::new(&directory).join(format!("child-{child_id}.sock")));
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

    fn zellij_entry(
        identity: &BridgeIdentity,
        member: &str,
        socket: PathBuf,
        handoff_id: Option<HandoffId>,
    ) -> BrokerEntry {
        let mut entry = BrokerEntry::now("zellij", member, socket, 1);
        entry.bridge_identity = Some(identity.clone());
        entry.bridge_member = Some(BridgeMemberId::new(member.to_owned()).unwrap());
        entry.handoff_id = handoff_id;
        entry.live_server = Some(format!("{member}-server"));
        entry.registration_id = Some(BrokerRegistrationId::generate().unwrap());
        entry
    }

    #[test]
    fn exact_herdr_live_selection_ignores_unrelated_and_stale_malformed_rows() {
        let temp = tempfile::tempdir().unwrap();
        let registry = test_registry(temp.path());
        let socket = temp.path().join("current.sock");
        let _current = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let valid = entry(socket);
        registry.register(valid.clone()).unwrap();
        let unrelated_socket = temp.path().join("unrelated.sock");
        let _unrelated = std::os::unix::net::UnixListener::bind(&unrelated_socket).unwrap();
        let mut unrelated = entry(unrelated_socket);
        unrelated.discovery_key = "unrelated".to_owned();
        unrelated.bridge_member = Some(BridgeMemberId::new("foreign".to_owned()).unwrap());
        registry.register(unrelated.clone()).unwrap();
        let mut stale = unrelated.clone();
        stale.discovery_key = "server".to_owned();
        stale.socket = temp.path().join("absent.sock");
        registry.register(stale).unwrap();
        let discovery = muxe_adapter_api::HostDiscoveryKey::parse("server").unwrap();
        let selected = registry.live_herdr_entries_for(&discovery).unwrap();
        assert_eq!(selected.len(), 1);
        assert!(selected[0].matches_recorded(&valid));
        assert!(
            super::super::activate::select_units(
                &registry.probe().unwrap().live,
                crate::cli::HostScope::All,
                None,
            )
            .is_err()
        );
        let mut malformed_selected = valid;
        malformed_selected.bridge_member = unrelated.bridge_member;
        registry.register(malformed_selected).unwrap();
        let before = std::fs::read(&registry.path).unwrap();
        assert!(registry.live_herdr_entries_for(&discovery).is_err());
        assert_eq!(std::fs::read(&registry.path).unwrap(), before);
    }

    #[test]
    fn exact_zellij_session_selection_validates_only_first_matching_record() {
        let temp = tempfile::tempdir().unwrap();
        let registry = test_registry(temp.path());
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(crate::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let unrelated =
            BrokerEntry::now("zellij", "unrelated", temp.path().join("unrelated.sock"), 0);
        registry.register(unrelated).unwrap();
        let mut selected = zellij_entry(
            &identity,
            "logical-member",
            temp.path().join("selected.sock"),
            None,
        );
        selected.live_server = Some("current-session".to_owned());
        registry.register(selected.clone()).unwrap();
        let session = muxe_adapter_api::HostDiscoveryKey::parse("current-session").unwrap();
        let validated = registry
            .zellij_entry_for_session(&session)
            .unwrap()
            .unwrap();
        assert!(validated.matches_recorded(&selected));
        assert!(
            super::super::activate::select_units(
                &registry.entries().unwrap(),
                crate::cli::HostScope::All,
                None,
            )
            .is_err()
        );
        selected.bridge_member = None;
        registry.register(selected).unwrap();
        let before = std::fs::read(&registry.path).unwrap();
        assert!(registry.zellij_entry_for_session(&session).is_err());
        assert_eq!(std::fs::read(&registry.path).unwrap(), before);
    }

    #[test]
    fn selected_authority_rejects_foreign_rows_before_registry_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let registry = test_registry(temp.path());
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(crate::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let guard = BridgeUnitGuard::acquire(temp.path(), identity.clone()).unwrap();
        let herdr = entry(temp.path().join("herdr.sock"));
        let zellij = zellij_entry(
            &identity,
            "session-a",
            temp.path().join("zellij.sock"),
            None,
        );
        assert!(matches!(
            registry.reconcile_live(&HerdrRegistryAuthority, &[], zellij, || {
                panic!("foreign row must fail before endpoint revalidation")
            }),
            Err(RegistryError::Unauthorized(_))
        ));
        assert!(matches!(
            registry.reconcile_live(&guard, &[], herdr.clone(), || {
                panic!("foreign row must fail before endpoint revalidation")
            }),
            Err(RegistryError::Unauthorized(_))
        ));
        assert!(registry.entries().unwrap().is_empty());

        registry.register_herdr(herdr.clone()).unwrap();
        let observed = registry.entries().unwrap();
        assert!(matches!(
            registry.verify_existing(&guard, &observed, &herdr, || {
                panic!("foreign legacy row must fail before reuse")
            }),
            Err(RegistryError::Unauthorized(_))
        ));
        assert!(matches!(
            registry.remove_exact_stale(&guard, &observed, &herdr, || {
                panic!("foreign stale row must fail before removal")
            }),
            Err(RegistryError::Unauthorized(_))
        ));
        assert!(matches!(
            registry.unregister_entry(&herdr, &guard),
            Err(RegistryError::Unauthorized(_))
        ));
        assert_eq!(registry.entries().unwrap(), observed);
    }

    #[test]
    fn canonical_aliases_share_one_unit_lock() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let physical = temp.path().join("physical");
        std::fs::create_dir(&physical).unwrap();
        let alias = temp.path().join("alias");
        std::os::unix::fs::symlink(&physical, &alias).unwrap();
        let leaf = std::ffi::OsStr::new(crate::integration::BRIDGE_FILE_NAME);
        let physical_identity =
            BridgeIdentity::resolve(&physical.join("integrations/zellij"), leaf).unwrap();
        let alias_identity =
            BridgeIdentity::resolve(&alias.join("integrations/zellij"), leaf).unwrap();

        let _guard = BridgeUnitGuard::acquire(temp.path(), physical_identity).unwrap();
        assert!(BridgeUnitGuard::acquire(temp.path(), alias_identity).is_err());
    }

    #[test]
    fn target_replacement_and_rollback_preserve_exact_registration_authority() {
        let temp = tempfile::tempdir().unwrap();
        let registry = test_registry(temp.path());
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(crate::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let guard = BridgeUnitGuard::acquire(temp.path(), identity.clone()).unwrap();
        let endpoint = temp.path().join("member.sock");
        let old = zellij_entry(&identity, "session-a", endpoint.clone(), None);
        let old_registration = registry.register_zellij(&guard, old.clone()).unwrap();
        let first_handoff = HandoffId([7; 16]);
        let first_capability = TargetRegistrationCapability::new(
            identity.clone(),
            BridgeMemberId::new("session-a".to_owned()).unwrap(),
            endpoint.clone(),
            "session-a".to_owned(),
            first_handoff,
        );
        let first_target = zellij_entry(
            &identity,
            "session-a",
            endpoint.clone(),
            Some(first_handoff),
        );
        let first_target_registration = registry
            .register_zellij_target(&first_capability, first_target.clone())
            .unwrap();
        assert!(
            !registry
                .unregister_zellij(&guard, &old_registration)
                .unwrap()
        );
        assert_eq!(registry.entries().unwrap(), vec![first_target.clone()]);

        assert!(
            registry
                .restore_zellij_target(&first_capability, &old)
                .unwrap()
        );
        assert_eq!(registry.entries().unwrap(), vec![old]);
        assert!(
            !registry
                .unregister_zellij(&guard, &first_target_registration)
                .unwrap()
        );

        let restored_first = registry
            .register_zellij_target(&first_capability, first_target.clone())
            .unwrap();
        let second_handoff = HandoffId([8; 16]);
        let second_capability = TargetRegistrationCapability::new(
            identity.clone(),
            BridgeMemberId::new("session-a".to_owned()).unwrap(),
            endpoint.clone(),
            "session-a".to_owned(),
            second_handoff,
        );
        let second_target = zellij_entry(&identity, "session-a", endpoint, Some(second_handoff));
        let second_registration = registry
            .register_zellij_target(&second_capability, second_target.clone())
            .unwrap();
        assert!(!registry.unregister_zellij(&guard, &restored_first).unwrap());
        assert_eq!(registry.entries().unwrap(), vec![second_target]);
        assert!(
            registry
                .restore_zellij_target(&second_capability, &first_target)
                .unwrap()
        );
        assert_eq!(registry.entries().unwrap(), vec![first_target]);
        assert!(
            !registry
                .unregister_zellij(&guard, &second_registration)
                .unwrap()
        );
        assert!(registry.unregister_zellij(&guard, &restored_first).unwrap());
        assert!(registry.entries().unwrap().is_empty());

        let other_endpoint = temp.path().join("other.sock");
        let other_capability = TargetRegistrationCapability::new(
            identity.clone(),
            BridgeMemberId::new("session-b".to_owned()).unwrap(),
            other_endpoint.clone(),
            "session-b".to_owned(),
            first_handoff,
        );
        let addition = zellij_entry(&identity, "session-b", other_endpoint, Some(first_handoff));
        assert!(
            registry
                .register_zellij_target(&other_capability, addition)
                .is_err()
        );
    }

    #[test]
    fn ordinary_registration_waits_for_held_unit_guard() {
        let temp = tempfile::tempdir().unwrap();
        let registry = test_registry(temp.path());
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(crate::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let activation_guard = BridgeUnitGuard::acquire(temp.path(), identity.clone()).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let cache = temp.path().to_path_buf();
        let worker_registry = registry.clone();
        let worker_identity = identity;
        let endpoint = temp.path().join("member.sock");
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let registration_guard =
                BridgeUnitGuard::acquire_blocking(&cache, worker_identity.clone()).unwrap();
            worker_registry
                .register_zellij(
                    &registration_guard,
                    zellij_entry(&worker_identity, "session-a", endpoint, None),
                )
                .unwrap();
            done_tx.send(()).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
        assert!(registry.entries().unwrap().is_empty());
        drop(activation_guard);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert_eq!(registry.entries().unwrap().len(), 1);
    }
    #[test]

    fn membership_cleanup_waits_for_unit_guard() {
        let temp = tempfile::tempdir().unwrap();
        let registry = test_registry(temp.path());
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(crate::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let guard = BridgeUnitGuard::acquire(temp.path(), identity.clone()).unwrap();
        let registration = registry
            .register_zellij(
                &guard,
                zellij_entry(
                    &identity,
                    "session-a",
                    temp.path().join("member.sock"),
                    None,
                ),
            )
            .unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let cache = temp.path().to_path_buf();
        let worker_registry = registry;
        let worker_identity = identity;
        std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let cleanup_guard = BridgeUnitGuard::acquire_blocking(&cache, worker_identity).unwrap();
            let removed = worker_registry
                .unregister_zellij(&cleanup_guard, &registration)
                .unwrap();
            done_tx.send(removed).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "cleanup published while activation owned the unit"
        );
        drop(guard);
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
        );
    }
}
