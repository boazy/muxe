//! Owner-only activation journals and crash recovery records.
//!
//! Before the first external mutation, the coordinator durably writes one
//! journal per activation unit: `$CACHE_DIR/activation/herdr-{host-hash}.json`
//! for a Herdr broker, `$CACHE_DIR/activation/zellij-{bridge-path-hash}.json`
//! for a Zellij bridge-sharing group. Each journal records a typed transaction
//! identity, preallocated handoffs, exact old and target compatibility records,
//! immutable bridge artifacts and receipt preimage, per-participant progress,
//! the single tagged transaction phase, and the recovery deadline. Journals
//! never contain configuration values, environment values,
//! or action payloads.
//!
//! Every journal transition and external mutation is idempotent, so the
//! coordinator, old brokers, target brokers, and the next Muxe invocation can
//! all resume recovery. Inconsistent host identities, handoff IDs, digests,
//! membership, or unrecognized journal states fail closed and preserve the
//! journal and exact transaction artifacts for diagnosis.

use std::{
    fmt, fs, io,
    os::unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use nix::fcntl::{Flock, FlockArg};

use muxe_protocol::{
    control::{
        AsOfTick, BridgeUnitId, BrokerRegistrationId, CompatibilityRecord, HandoffId,
        UnitReadinessEpochId,
    },
    wire::ServerId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    fsutil::{self, FsError},
    integration::receipt::{BridgeRecord, Sha256Digest},
    lifecycle::registry::{
        BridgeMemberId, BrokerEntry, MemberCensus, TargetRegistrationCapability,
    },
    paths::BridgeIdentity,
};

/// Activation journal directory name under `$CACHE_DIR`.
pub const ACTIVATION_DIR_NAME: &str = "activation";
const TARGET_RETIREMENT_RECEIPT_SCHEMA_VERSION: u32 = 1;
const OLD_RETIREMENT_RECEIPT_SCHEMA_VERSION: u32 = 1;
/// Activation journal schema version.
pub const ACTIVATION_JOURNAL_SCHEMA_VERSION: u32 = 5;
/// Recovery deadline: ten minutes after the journal is written.
pub const RECOVERY_DEADLINE_SECS: u64 = 600;

#[derive(Debug, Error)]
pub enum JournalError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error("activation journal at {} is corrupt: {source}", path.display())]
    Corrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("activation journal at {} uses unsupported schema version {version}", path.display())]
    UnsupportedVersion { path: PathBuf, version: u32 },
    #[error("activation journal state is inconsistent: {0}")]
    Inconsistent(String),
    #[error("Muxe cannot acquire the cache lock while another operation holds it: {}", path.display())]
    CacheActive { path: PathBuf },
    #[error("Muxe could not acquire the cache lock at {}: {source}", path.display())]
    CacheLock {
        path: PathBuf,
        #[source]
        source: nix::errno::Errno,
    },
    #[error(
        "The Zellij activation journal is missing the canonical bridge identity or the complete list of participating sessions"
    )]
    MissingZellijAuthority,
}

/// Typed identity of one Herdr activation unit.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct HerdrUnitId(String);

impl HerdrUnitId {
    /// Derives the stable unit identity from its discovery key.
    #[must_use]
    pub fn derive(discovery: &str) -> Self {
        Self(fsutil::sha256_hex(discovery.as_bytes())[..32].to_owned())
    }

    /// Parses a persisted lowercase hexadecimal unit identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is not exactly 32 lowercase hexadecimal
    /// characters.
    pub fn parse(value: String) -> Result<Self, JournalError> {
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(JournalError::Inconsistent(
                "Herdr unit identity is not 32 lowercase hexadecimal characters".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One activation unit: a single Herdr broker, or all live Zellij brokers
/// sharing one descriptor-validated physical bridge identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "unit", rename_all = "snake_case")]
pub enum UnitKind {
    Herdr {
        host_hash: HerdrUnitId,
    },
    Zellij {
        /// Serialized under the legacy key so schema-v1 files remain readable;
        /// canonical authority is separately required on the journal.
        #[serde(rename = "bridge_path_hash")]
        bridge_unit: BridgeUnitId,
    },
}

impl UnitKind {
    /// Returns the journal file name for this unit.
    #[must_use]
    pub fn journal_name(&self) -> String {
        match self {
            Self::Herdr { host_hash } => format!("herdr-{}.json", host_hash.as_str()),
            Self::Zellij { bridge_unit } => format!("zellij-{bridge_unit}.json"),
        }
    }
}
/// Cross-process cache lease held for every activation or recovery participant.
/// Its persistent lock file lives beside, rather than inside, `$CACHE_DIR`, so
/// cache purge cannot remove the inode while a participant still owns it.
#[derive(Debug)]
pub struct CacheLease {
    _file: Flock<fs::File>,
}

/// Cross-process unit lock held for the full activation or recovery decision.
/// The cache lease is acquired first and retained with the unit lock.
#[derive(Debug)]
pub struct UnitLock {
    _cache: CacheLease,
    _file: Flock<fs::File>,
}

/// Result of a nonblocking activation-unit lock attempt.
#[derive(Debug)]
pub enum UnitLockAttempt {
    /// The caller exclusively owns the activation unit.
    Acquired(UnitLock),
    /// Another activation or recovery participant owns the unit.
    Active,
}

/// Acquires the shared cache lifetime lease used by activation and recovery.
///
/// # Errors
///
/// Returns a journal error when the persistent lock cannot be prepared or is
/// exclusively owned by cache purge.
pub fn acquire_cache_lease(cache_dir: &Path) -> Result<CacheLease, JournalError> {
    acquire_cache_lock(cache_dir, FlockArg::LockSharedNonblock)
}

/// Acquires the exclusive cache lifetime lease used by cache purge.
///
/// # Errors
///
/// Returns a journal error when an activation/recovery participant owns the
/// shared lease or the persistent lock cannot be prepared.
pub fn acquire_cache_purge_lock(cache_dir: &Path) -> Result<CacheLease, JournalError> {
    acquire_cache_lock(cache_dir, FlockArg::LockExclusiveNonblock)
}

/// Acquires the one shared lock for an activation unit.
///
/// # Errors
///
/// Returns a journal error when the cache/unit lock cannot be created or acquired.
pub fn acquire_unit_lock(cache_dir: &Path, unit: &UnitKind) -> Result<UnitLock, JournalError> {
    match try_acquire_unit_lock(cache_dir, unit)? {
        UnitLockAttempt::Acquired(lock) => Ok(lock),
        UnitLockAttempt::Active => Err(JournalError::Inconsistent(format!(
            "activation unit is already owned by another process at {}",
            activation_dir(cache_dir)
                .join(unit.journal_name())
                .with_extension("lock")
                .display()
        ))),
    }
}

/// Tries to acquire one activation-unit lock without blocking.
///
/// # Errors
///
/// Returns a journal error for cache, directory, file, ownership, or lock
/// failures other than contention.
pub fn try_acquire_unit_lock(
    cache_dir: &Path,
    unit: &UnitKind,
) -> Result<UnitLockAttempt, JournalError> {
    let cache = acquire_cache_lease(cache_dir)?;
    let directory = activation_dir(cache_dir);
    fsutil::ensure_owner_dir(&directory)?;
    let path = directory.join(unit.journal_name()).with_extension("lock");
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let file = options.open(&path).map_err(|source| {
        JournalError::Inconsistent(format!(
            "cannot open activation unit lock at {}: {source}",
            path.display()
        ))
    })?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(file) => Ok(UnitLockAttempt::Acquired(UnitLock {
            _cache: cache,
            _file: file,
        })),
        Err((_, error)) if error == nix::errno::Errno::EWOULDBLOCK => Ok(UnitLockAttempt::Active),
        Err((_, error)) => Err(JournalError::Inconsistent(format!(
            "cannot acquire activation unit lock at {}: {error}",
            path.display()
        ))),
    }
}
/// Acquires one unit lock with a kernel-blocking flock.
///
/// Call only from a blocking thread. The cache lease remains first in the
/// ordering and is retained for the lifetime of the returned guard.
///
/// # Errors
///
/// Returns [`JournalError`] when the cache lease or unit lock cannot be
/// created, validated, or acquired.
pub fn acquire_unit_lock_blocking(
    cache_dir: &Path,
    unit: &UnitKind,
) -> Result<UnitLock, JournalError> {
    let cache = acquire_cache_lease(cache_dir)?;
    let directory = activation_dir(cache_dir);
    fsutil::ensure_owner_dir(&directory)?;
    let path = directory.join(unit.journal_name()).with_extension("lock");
    let file = acquire_lock_path_blocking(&path)?;
    Ok(UnitLock {
        _cache: cache,
        _file: file,
    })
}

/// Acquires an existing journal lock with a kernel-blocking flock. Callers
/// invoke this from `spawn_blocking` when the lifecycle participant must wait
/// for the current owner to finish its retirement barrier.
///
/// # Errors
///
/// Returns a journal error when the cache/unit lock cannot be prepared or acquired.
pub fn acquire_journal_lock_blocking(path: &Path) -> Result<UnitLock, JournalError> {
    let lock = path.with_extension("lock");
    let activation = lock
        .parent()
        .ok_or_else(|| JournalError::Inconsistent("journal lock path has no parent".to_owned()))?;
    let cache_dir = activation.parent().ok_or_else(|| {
        JournalError::Inconsistent("activation lock path has no cache parent".to_owned())
    })?;
    let cache = acquire_cache_lease(cache_dir)?;
    fsutil::ensure_owner_dir(activation)?;
    let file = acquire_lock_path_blocking(&lock)?;
    Ok(UnitLock {
        _cache: cache,
        _file: file,
    })
}

fn cache_lock_path(cache_dir: &Path) -> Result<PathBuf, JournalError> {
    let absolute = if cache_dir.is_absolute() {
        cache_dir.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| {
                JournalError::Inconsistent(format!("cannot resolve cache directory: {error}"))
            })?
            .join(cache_dir)
    };
    let identity = fs::canonicalize(&absolute)
        .or_else(|_| {
            let parent = absolute
                .parent()
                .ok_or_else(|| io::Error::other("cache directory has no parent"))?;
            let parent = fs::canonicalize(parent)?;
            Ok::<_, io::Error>(
                parent.join(
                    absolute
                        .file_name()
                        .ok_or_else(|| io::Error::other("cache directory has no name"))?,
                ),
            )
        })
        .map_err(|error| {
            JournalError::Inconsistent(format!("cannot canonicalize cache directory: {error}"))
        })?;
    let parent = identity.parent().ok_or_else(|| {
        JournalError::Inconsistent("cache directory has no lock parent".to_owned())
    })?;
    if !parent.exists() {
        fsutil::ensure_owner_dir(parent)?;
    }
    let digest = fsutil::sha256_hex(identity.as_os_str().as_bytes());
    Ok(parent.join(format!(".muxe-cache-{}.lock", &digest[..32])))
}

fn acquire_cache_lock(cache_dir: &Path, mode: FlockArg) -> Result<CacheLease, JournalError> {
    let path = cache_lock_path(cache_dir)?;
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let file = options.open(&path).map_err(|source| {
        JournalError::Inconsistent(format!(
            "cannot open cache lifetime lock at {}: {source}",
            path.display()
        ))
    })?;
    let file = Flock::lock(file, mode).map_err(|(_, error)| cache_lock_error(path, error))?;
    Ok(CacheLease { _file: file })
}

fn cache_lock_error(path: PathBuf, source: nix::errno::Errno) -> JournalError {
    if source == nix::errno::Errno::EWOULDBLOCK {
        JournalError::CacheActive { path }
    } else {
        JournalError::CacheLock { path, source }
    }
}

fn acquire_lock_path_blocking(path: &Path) -> Result<Flock<fs::File>, JournalError> {
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC);
    let file = options.open(path).map_err(|source| {
        JournalError::Inconsistent(format!(
            "cannot open activation unit lock at {}: {source}",
            path.display()
        ))
    })?;
    Flock::lock(file, FlockArg::LockExclusive).map_err(|(_, error)| {
        JournalError::Inconsistent(format!(
            "cannot acquire activation unit lock at {}: {error}",
            path.display()
        ))
    })
}

/// Hashes a textual external identity into the typed Herdr unit key.
#[must_use]
pub fn unit_hash(identity: &str) -> HerdrUnitId {
    HerdrUnitId::derive(identity)
}

/// Durable identity of one activation transaction.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActivationId([u8; 16]);

impl ActivationId {
    /// Generates one nonzero activation identity from the operating system.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Inconsistent`] when entropy is unavailable.
    pub fn generate() -> Result<Self, JournalError> {
        let mut bytes = [0_u8; 16];
        getrandom::getrandom(&mut bytes).map_err(|error| {
            JournalError::Inconsistent(format!("cannot generate activation identity: {error}"))
        })?;
        if bytes == [0; 16] {
            return Err(JournalError::Inconsistent(
                "generated zero activation identity".to_owned(),
            ));
        }
        Ok(Self(bytes))
    }

    /// Constructs a transaction identity from boundary bytes.
    ///
    /// # Errors
    ///
    /// Rejects the reserved all-zero identity.
    pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, JournalError> {
        if bytes == [0; 16] {
            return Err(JournalError::Inconsistent(
                "activation identity is zero".to_owned(),
            ));
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        hex_bytes(&self.0)
    }
}

impl fmt::Display for ActivationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl Serialize for ActivationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ActivationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        let bytes = parse_hex::<16>(&value).map_err(serde::de::Error::custom)?;
        if bytes == [0; 16] {
            return Err(serde::de::Error::custom("activation identity is zero"));
        }
        Ok(Self(bytes))
    }
}

/// Typed logical member identity within an activation unit.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ActivationMemberId(String);

impl ActivationMemberId {
    /// Validates a registry discovery key as an activation member identity.
    ///
    /// # Errors
    ///
    /// Rejects empty or control-bearing identities.
    pub fn new(value: String) -> Result<Self, JournalError> {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(JournalError::Inconsistent(
                "activation member identity is empty or contains a control character".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated runtime endpoint of one activation member.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MemberEndpoint(PathBuf);

impl MemberEndpoint {
    /// Validates and wraps an absolute normalized member endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, relative, or non-normalized paths.
    pub fn new(path: PathBuf) -> Result<Self, JournalError> {
        if !path.is_absolute()
            || path.as_os_str().is_empty()
            || path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(JournalError::Inconsistent(
                "activation member endpoint is not an absolute normalized path".to_owned(),
            ));
        }
        Ok(Self(path))
    }

    #[must_use]
    pub fn as_path(&self) -> &Path {
        &self.0
    }

    #[must_use]
    pub fn into_path(self) -> PathBuf {
        self.0
    }
}

/// Transaction-scoped identity of one exact activation member.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MemberTransactionId(String);

impl MemberTransactionId {
    fn derive(activation: ActivationId, member: &ActivationMemberId) -> Self {
        let mut bytes = Vec::with_capacity(16 + member.as_str().len());
        bytes.extend_from_slice(&activation.0);
        bytes.extend_from_slice(member.as_str().as_bytes());
        Self(fsutil::sha256_hex(&bytes))
    }

    fn is_valid(&self) -> bool {
        self.0.len() == 64
            && self
                .0
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }
}

/// Operating-system process identity retained with an owned target child.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TargetProcessId(u32);

impl TargetProcessId {
    /// Wraps a nonzero child-process identity.
    ///
    /// # Errors
    ///
    /// Rejects the reserved zero process identity.
    pub fn new(value: u32) -> Result<Self, JournalError> {
        if value == 0 {
            return Err(JournalError::Inconsistent(
                "target process identity is zero".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Exact process authority whose completed stop may publish retirement proof.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "authority", rename_all = "snake_case")]
pub enum TargetRetirementAuthority {
    OwnedProcess { process_id: TargetProcessId },
    RemoteServer { server_id: ServerId },
}

impl TargetRetirementAuthority {
    fn validate(&self) -> Result<(), JournalError> {
        match self {
            Self::OwnedProcess { process_id } => TargetProcessId::new(process_id.get()).map(|_| ()),
            Self::RemoteServer { server_id }
                if server_id.as_str().is_empty()
                    || server_id.as_str().chars().any(char::is_control) =>
            {
                Err(JournalError::Inconsistent(
                    "target server identity is empty or contains a control character".to_owned(),
                ))
            }
            Self::RemoteServer { .. } => Ok(()),
        }
    }
}

/// Transaction-private identity of one exact target-retirement receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TargetRetirementReceiptId(String);

impl TargetRetirementReceiptId {
    fn derive(
        activation_id: ActivationId,
        member_id: &MemberTransactionId,
        launch_authority: &MemberLaunchAuthority,
        target_record: &CompatibilityRecord,
        authority: &TargetRetirementAuthority,
    ) -> Result<Self, JournalError> {
        #[derive(Serialize)]
        struct Binding<'a> {
            activation_id: ActivationId,
            member_id: &'a MemberTransactionId,
            launch_authority: &'a MemberLaunchAuthority,
            target_record: &'a CompatibilityRecord,
            authority: &'a TargetRetirementAuthority,
        }

        let bytes = serde_json::to_vec(&Binding {
            activation_id,
            member_id,
            launch_authority,
            target_record,
            authority,
        })
        .map_err(|error| {
            JournalError::Inconsistent(format!(
                "cannot derive target retirement receipt identity: {error}"
            ))
        })?;
        Ok(Self(fsutil::sha256_hex(&bytes)))
    }

    fn is_valid(&self) -> bool {
        self.0.len() == 64
            && self
                .0
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Durable `RetireIntent` authority and its predeclared proof path identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TargetRetirementIntent {
    receipt_id: TargetRetirementReceiptId,
    authority: TargetRetirementAuthority,
}

impl TargetRetirementIntent {
    /// Derives the only retirement intent valid for one exact journal member.
    ///
    /// # Errors
    ///
    /// Returns an error when the process authority or derived receipt identity
    /// is invalid.
    pub fn new(
        journal: &ActivationJournal,
        member: &TransactionMember,
        authority: TargetRetirementAuthority,
    ) -> Result<Self, JournalError> {
        authority.validate()?;
        let receipt_id = TargetRetirementReceiptId::derive(
            journal.activation_id,
            &member.id,
            &member.authority,
            &journal.target_record,
            &authority,
        )?;
        Ok(Self {
            receipt_id,
            authority,
        })
    }

    fn validate(
        &self,
        journal: &ActivationJournal,
        member: &TransactionMember,
    ) -> Result<(), JournalError> {
        self.authority.validate()?;
        if !self.receipt_id.is_valid()
            || self.receipt_id
                != TargetRetirementReceiptId::derive(
                    journal.activation_id,
                    &member.id,
                    &member.authority,
                    &journal.target_record,
                    &self.authority,
                )?
        {
            return Err(JournalError::Inconsistent(
                "target retirement intent does not match exact journal authority".to_owned(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn authority(&self) -> &TargetRetirementAuthority {
        &self.authority
    }

    #[must_use]
    pub const fn receipt_id(&self) -> &TargetRetirementReceiptId {
        &self.receipt_id
    }
}

/// Role of one immutable transaction-owned bridge artifact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BridgeArtifactRole {
    Old,
    Target,
}

/// Typed identity of one immutable bridge artifact.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BridgeArtifactId {
    pub activation: ActivationId,
    pub role: BridgeArtifactRole,
}

impl BridgeArtifactId {
    #[must_use]
    pub const fn new(activation: ActivationId, role: BridgeArtifactRole) -> Self {
        Self { activation, role }
    }

    #[must_use]
    pub fn file_name(self) -> String {
        let role = match self.role {
            BridgeArtifactRole::Old => "old",
            BridgeArtifactRole::Target => "target",
        };
        format!(".muxe-activation-{}-{role}.wasm", self.activation)
    }
}

/// Progress of the old broker for one exact member.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OldMemberProgress {
    Pending,
    PrepareIntent,
    Drained,
    CommitIntent,
    Committed,
    ResumeIntent,
    Resumed,
}

/// Progress of the target broker for one exact member.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetMemberProgress {
    Absent,
    SpawnIntent,
    Gated,
    Ready,
    CommitIntent,
    Committed,
    /// Durable intent whose [`TargetRetirementIntent`] predeclares the only
    /// accepted post-stop receipt.
    RetireIntent,
    Retired,
}

/// Exact authority passed to one target launch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemberLaunchAuthority {
    pub member: ActivationMemberId,
    pub endpoint: MemberEndpoint,
    pub handoff_id: HandoffId,
}

/// One exact old/target member pair.

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionMember {
    pub id: MemberTransactionId,
    pub authority: MemberLaunchAuthority,
    pub old_record: CompatibilityRecord,
    pub old: OldMemberProgress,
    pub target: TargetMemberProgress,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_retirement: Option<TargetRetirementIntent>,
}

impl TransactionMember {
    /// Builds one member with a preallocated nonzero handoff.
    ///
    /// # Errors
    ///
    /// Rejects an invalid identity or zero handoff.
    pub fn new(
        activation: ActivationId,
        member: ActivationMemberId,
        endpoint: MemberEndpoint,
        handoff_id: HandoffId,
        old_record: CompatibilityRecord,
    ) -> Result<Self, JournalError> {
        if handoff_id.0 == [0; 16] {
            return Err(JournalError::Inconsistent(
                "activation member has a zero handoff".to_owned(),
            ));
        }
        let id = MemberTransactionId::derive(activation, &member);
        Ok(Self {
            id,
            authority: MemberLaunchAuthority {
                member,
                endpoint,
                handoff_id,
            },
            old_record,
            old: OldMemberProgress::Pending,
            target: TargetMemberProgress::Absent,
            target_retirement: None,
        })
    }

    #[must_use]
    pub fn member(&self) -> &ActivationMemberId {
        &self.authority.member
    }

    #[must_use]
    pub fn endpoint(&self) -> &MemberEndpoint {
        &self.authority.endpoint
    }

    #[must_use]
    pub const fn handoff_id(&self) -> HandoffId {
        self.authority.handoff_id
    }
}

/// Exact immutable bridge artifacts and receipt authority for one transaction.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeArtifacts {
    pub old: BridgeArtifactId,
    pub target: BridgeArtifactId,
    pub old_digest: Sha256Digest,
    pub target_digest: Sha256Digest,
    pub receipt_preimage: BridgeRecord,
    pub receipt_target: BridgeRecord,
    pub receipt_rollback: BridgeRecord,
}

/// Durable bridge transaction progress. Reload coverage is nested in the
/// phase that owns it, so a journal cannot combine a reload intent with an
/// unrelated artifact phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum BridgeProgress {
    ArtifactsPending,
    ArtifactsReady,
    TargetInstallIntent,
    TargetInstalled,
    TargetReloading {
        active: Option<MemberTransactionId>,
        completed: Vec<MemberTransactionId>,
    },
    TargetReloaded,
    PreviousPublishIntent,
    PreviousPublished,
    ReceiptIntent,
    ReceiptPublished,
    OldInstallIntent,
    OldInstalled,
    TargetPreviousIntent,
    TargetPreviousPublished,
    OldReceiptIntent,
    OldReceiptPublished,
    OldReloading {
        active: Option<MemberTransactionId>,
        completed: Vec<MemberTransactionId>,
    },
    Restored {
        reloaded: Vec<MemberTransactionId>,
    },
}

impl BridgeProgress {
    fn reload_coverage(&self) -> Option<(&Option<MemberTransactionId>, &[MemberTransactionId])> {
        match self {
            Self::TargetReloading { active, completed }
            | Self::OldReloading { active, completed } => Some((active, completed)),
            _ => None,
        }
    }

    fn restored_coverage(&self) -> Option<&[MemberTransactionId]> {
        match self {
            Self::Restored { reloaded } => Some(reloaded),
            _ => None,
        }
    }
}

/// Zellij-only artifact and reload progress.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeTransaction {
    pub artifacts: BridgeArtifacts,
    pub progress: BridgeProgress,
}

/// The exact target registry incarnation observed by the coordinator at
/// Ready. The enclosing journal binds compatibility and activation identity;
/// `member_id` derives from that activation and the member authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadyMemberProof {
    pub member_id: MemberTransactionId,
    pub entry: BrokerEntry,
    pub server_id: ServerId,
}

impl ReadyMemberProof {
    /// Captures only a fully identified registry incarnation.
    ///
    /// # Errors
    ///
    /// Rejects a missing or zero process-scoped registration identity.
    pub fn new(
        member: &TransactionMember,
        entry: &BrokerEntry,
        server_id: &ServerId,
    ) -> Result<Self, JournalError> {
        let proof = Self {
            member_id: member.id.clone(),
            entry: entry.clone(),
            server_id: server_id.clone(),
        };
        if entry
            .registration_id
            .is_none_or(muxe_protocol::control::BrokerRegistrationId::is_zero)
            || entry.server_pid == 0
        {
            return Err(JournalError::Inconsistent(
                "Ready target lacks a unique process registration identity".to_owned(),
            ));
        }
        Ok(proof)
    }

    fn validate(&self, journal: &ActivationJournal, member: &TransactionMember) -> bool {
        let zellij = matches!(journal.unit, UnitKind::Zellij { .. });
        self.member_id == member.id
            && self.member_id == MemberTransactionId::derive(journal.activation_id, member.member())
            && self.entry.registration_id.is_some_and(|id| !id.is_zero())
            && self.entry.server_pid != 0
            && self.entry.started_at != 0
            && self.entry.host_kind == if zellij { "zellij" } else { "herdr" }
            && self.entry.discovery_key == member.member().as_str()
            && self.entry.socket == member.endpoint().as_path()
            && self.entry.handoff_id == zellij.then_some(member.handoff_id())
            && self.entry.live_server.as_deref() == Some(self.server_id.as_str())
            && !self.server_id.as_str().is_empty()
            && self.entry.bridge_identity == journal.bridge_identity
            && self
                .entry
                .bridge_member
                .as_ref()
                .map(BridgeMemberId::as_str)
                == zellij.then_some(member.member().as_str())
            && (!zellij
                || journal
                    .bridge_identity
                    .as_ref()
                    .is_some_and(|bridge| self.entry.bridge_identity.as_ref() == Some(bridge)))
    }

    /// Compares the exact proof-era row and broker-reported server identity.
    #[must_use]
    pub fn matches(&self, row: &BrokerEntry, server_id: &ServerId) -> bool {
        &self.entry == row && &self.server_id == server_id
    }
}

/// Durable unit decision binding the complete observed target incarnations
/// to one transaction. Zellij also records a shared broker-observed as-of
/// epoch; Herdr has no client census epoch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadyProof {
    pub epoch: Option<UnitReadinessEpochId>,
    pub bridge_unit: Option<BridgeUnitId>,
    pub member_ids: Vec<MemberTransactionId>,
    pub as_of: Option<AsOfTick>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub incarnations: Vec<ReadyMemberProof>,
}

impl ReadyProof {
    /// Seals a complete exact member census.
    ///
    /// # Errors
    ///
    /// Rejects missing, duplicate, or foreign target incarnations.
    pub fn new(
        journal: &ActivationJournal,
        as_of: Option<(UnitReadinessEpochId, AsOfTick)>,
        incarnations: Vec<ReadyMemberProof>,
    ) -> Result<Self, JournalError> {
        let mut member_ids = journal
            .members()
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        member_ids.sort_unstable();
        let proof = Self {
            epoch: as_of.map(|(epoch, _)| epoch),
            bridge_unit: match journal.unit {
                UnitKind::Zellij { bridge_unit } => Some(bridge_unit),
                UnitKind::Herdr { .. } => None,
            },
            member_ids,
            as_of: as_of.map(|(_, tick)| tick),
            incarnations,
        };
        proof.validate(journal)?;
        Ok(proof)
    }

    fn validate_legacy(&self, journal: &ActivationJournal) -> bool {
        let UnitKind::Zellij { bridge_unit } = journal.unit else {
            return false;
        };
        let mut expected = journal
            .members()
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        expected.sort_unstable();
        self.epoch.is_some_and(|epoch| !epoch.is_zero())
            && self.as_of.is_some_and(|tick| tick.millis() != 0)
            && self.bridge_unit == Some(bridge_unit)
            && self.member_ids == expected
            && self.incarnations.is_empty()
    }

    fn validate(&self, journal: &ActivationJournal) -> Result<(), JournalError> {
        let zellij = matches!(journal.unit, UnitKind::Zellij { .. });
        let mut expected = journal
            .members()
            .iter()
            .map(|member| member.id.clone())
            .collect::<Vec<_>>();
        expected.sort_unstable();
        let mut actual = self
            .incarnations
            .iter()
            .map(|member| member.member_id.clone())
            .collect::<Vec<_>>();
        actual.sort_unstable();
        let unit_valid = match journal.unit {
            UnitKind::Zellij { bridge_unit } => {
                self.bridge_unit == Some(bridge_unit)
                    && self.epoch.is_some_and(|epoch| !epoch.is_zero())
                    && self.as_of.is_some_and(|tick| tick.millis() != 0)
                    && journal
                        .bridge_identity
                        .as_ref()
                        .is_some_and(|identity| identity.unit() == bridge_unit)
            }
            UnitKind::Herdr { .. } => {
                self.bridge_unit.is_none() && self.epoch.is_none() && self.as_of.is_none()
            }
        };
        if !unit_valid
            || self.member_ids != expected
            || actual != expected
            || self.incarnations.len() != expected.len()
            || self.incarnations.iter().any(|proof| {
                journal
                    .members()
                    .iter()
                    .find(|member| member.id == proof.member_id)
                    .is_none_or(|member| !proof.validate(journal, member))
            })
            || (zellij && journal.bridge_identity.is_none())
        {
            return Err(JournalError::Inconsistent(
                "Ready proof lacks the exact target incarnation census".to_owned(),
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn member(&self, member_id: &MemberTransactionId) -> Option<&ReadyMemberProof> {
        self.incarnations
            .iter()
            .find(|proof| &proof.member_id == member_id)
    }
}

/// Nested durable progress shared by every nonterminal and terminal phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TransactionProgress {
    pub members: Vec<TransactionMember>,
    pub bridge: Option<BridgeTransaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_proof: Option<ReadyProof>,
}

/// The single durable transaction state. No parallel state/decision booleans
/// exist beside this tagged phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum TransactionPhase {
    Preparing {
        progress: TransactionProgress,
    },
    Activating {
        progress: TransactionProgress,
    },
    Ready {
        progress: TransactionProgress,
    },
    Committing {
        progress: TransactionProgress,
    },
    RollingBack {
        reason: String,
        progress: TransactionProgress,
    },
    Committed {
        progress: TransactionProgress,
    },
    RolledBack {
        progress: TransactionProgress,
    },
}

/// The one state interpreter consumed by normal activation and both recovery
/// actors. Capabilities execute the directive but do not choose transaction fate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionDirective {
    Prepare,
    Activate,
    Commit,
    RollBack,
    CleanupCommitted,
    CleanupRolledBack,
}

/// Broker-local progress acknowledgement. Old and target commit proof remain
/// distinct so one process can never attest the other process's transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerRecoveryAck {
    Resumed,
    TargetRetired,
    OldCommitted,
    TargetCommitted,
}

impl TransactionPhase {
    #[must_use]
    pub const fn progress(&self) -> &TransactionProgress {
        match self {
            Self::Preparing { progress }
            | Self::Activating { progress }
            | Self::Ready { progress }
            | Self::Committing { progress }
            | Self::RollingBack { progress, .. }
            | Self::Committed { progress }
            | Self::RolledBack { progress } => progress,
        }
    }

    pub fn progress_mut(&mut self) -> &mut TransactionProgress {
        match self {
            Self::Preparing { progress }
            | Self::Activating { progress }
            | Self::Ready { progress }
            | Self::Committing { progress }
            | Self::RollingBack { progress, .. }
            | Self::Committed { progress }
            | Self::RolledBack { progress } => progress,
        }
    }

    #[must_use]
    pub const fn directive(&self) -> TransactionDirective {
        match self {
            Self::Preparing { .. } => TransactionDirective::Prepare,
            Self::Activating { .. } => TransactionDirective::Activate,
            Self::Ready { .. } | Self::Committing { .. } => TransactionDirective::Commit,
            Self::RollingBack { .. } => TransactionDirective::RollBack,
            Self::Committed { .. } => TransactionDirective::CleanupCommitted,
            Self::RolledBack { .. } => TransactionDirective::CleanupRolledBack,
        }
    }
}

/// Durable activation journal schema v5. Schemas v3/v4 remain readable for
/// pre-Ready rollback; older Ready phases lack exact target incarnation proof.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivationJournal {
    pub schema_version: u32,
    pub activation_id: ActivationId,
    pub unit: UnitKind,
    pub bridge_identity: Option<BridgeIdentity>,
    pub member_census: Option<MemberCensus>,
    pub target_record: CompatibilityRecord,
    pub old_registry: Vec<BrokerEntry>,
    pub transaction: TransactionPhase,
    pub recovery_deadline: u64,
}

impl ActivationJournal {
    /// Builds a fully identified Preparing transaction.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] when the member set is invalid.
    pub fn new(
        activation_id: ActivationId,
        unit: UnitKind,
        target_record: CompatibilityRecord,
        members: Vec<TransactionMember>,
    ) -> Result<Self, JournalError> {
        let journal = Self {
            schema_version: ACTIVATION_JOURNAL_SCHEMA_VERSION,
            activation_id,
            unit,
            bridge_identity: None,
            member_census: None,
            target_record,
            old_registry: Vec::new(),
            transaction: TransactionPhase::Preparing {
                progress: TransactionProgress {
                    members,
                    bridge: None,
                    ready_proof: None,
                },
            },
            recovery_deadline: unix_now() + RECOVERY_DEADLINE_SECS,
        };
        if journal.transaction.progress().members.is_empty() {
            return Err(JournalError::Inconsistent(
                "journal has no members".to_owned(),
            ));
        }
        Ok(journal)
    }

    /// Binds exact bridge identity, census, and artifact authority to a Zellij journal.
    ///
    /// # Errors
    ///
    /// Returns an error when the authority does not match the journal's unit,
    /// activation, artifacts, or receipt identities.
    pub fn bind_zellij_authority(
        &mut self,
        identity: BridgeIdentity,
        census: MemberCensus,
        artifacts: BridgeArtifacts,
    ) -> Result<(), JournalError> {
        match self.unit {
            UnitKind::Zellij { bridge_unit } if bridge_unit == identity.unit() => {
                if artifacts.old.activation != self.activation_id
                    || artifacts.target.activation != self.activation_id
                    || artifacts.old.role != BridgeArtifactRole::Old
                    || artifacts.target.role != BridgeArtifactRole::Target
                    || artifacts.receipt_preimage.bridge_identity != identity
                {
                    return Err(JournalError::Inconsistent(
                        "bridge artifacts disagree with transaction authority".to_owned(),
                    ));
                }
                self.bridge_identity = Some(identity);
                self.member_census = Some(census);
                self.transaction.progress_mut().bridge = Some(BridgeTransaction {
                    artifacts,
                    progress: BridgeProgress::ArtifactsPending,
                });
                Ok(())
            }
            UnitKind::Zellij { .. } => Err(JournalError::Inconsistent(
                "journal unit key disagrees with canonical bridge identity".to_owned(),
            )),
            UnitKind::Herdr { .. } => Err(JournalError::Inconsistent(
                "cannot attach Zellij bridge authority to Herdr journal".to_owned(),
            )),
        }
    }

    #[must_use]
    pub const fn directive(&self) -> TransactionDirective {
        self.transaction.directive()
    }

    /// Returns the exact transaction member authorized by a broker handoff.
    ///
    /// # Errors
    ///
    /// Rejects an unknown discovery key or a mismatched handoff.
    pub fn recovery_member(
        &self,
        discovery_key: &ActivationMemberId,
        handoff: HandoffId,
    ) -> Result<&TransactionMember, JournalError> {
        self.members()
            .iter()
            .find(|member| member.member() == discovery_key && member.handoff_id() == handoff)
            .ok_or_else(|| {
                JournalError::Inconsistent(
                    "broker identity and handoff do not name one transaction member".to_owned(),
                )
            })
    }

    /// Records broker-local progress under the transaction's already-durable
    /// fate. It never chooses commit versus rollback or enters terminal
    /// Committed: bridge and receipt publication must finish first.
    ///
    /// # Errors
    ///
    /// Rejects acknowledgements that contradict the durable directive.
    pub fn acknowledge_broker(
        &mut self,
        discovery_key: &ActivationMemberId,
        handoff: HandoffId,
        ack: BrokerRecoveryAck,
    ) -> Result<(), JournalError> {
        let directive = self.directive();
        if matches!(
            directive,
            TransactionDirective::CleanupCommitted | TransactionDirective::CleanupRolledBack
        ) {
            return Err(JournalError::Inconsistent(
                "terminal transactions accept cleanup only, not broker acknowledgements".to_owned(),
            ));
        }
        let member = self
            .members_mut()
            .iter_mut()
            .find(|member| member.member() == discovery_key && member.handoff_id() == handoff)
            .ok_or_else(|| {
                JournalError::Inconsistent(
                    "broker acknowledgement does not name one transaction member".to_owned(),
                )
            })?;
        match (directive, ack) {
            (TransactionDirective::RollBack, BrokerRecoveryAck::Resumed)
                if matches!(
                    member.old,
                    OldMemberProgress::ResumeIntent | OldMemberProgress::Resumed
                ) =>
            {
                member.old = OldMemberProgress::Resumed;
            }
            (TransactionDirective::Commit, BrokerRecoveryAck::OldCommitted) => {
                member.old = OldMemberProgress::Committed;
            }
            (TransactionDirective::Commit, BrokerRecoveryAck::TargetCommitted) => {
                member.target = TargetMemberProgress::Committed;
            }
            _ => {
                return Err(JournalError::Inconsistent(
                    "broker acknowledgement contradicts the durable transaction directive or intent"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn members(&self) -> &[TransactionMember] {
        &self.transaction.progress().members
    }

    pub fn members_mut(&mut self) -> &mut [TransactionMember] {
        &mut self.transaction.progress_mut().members
    }

    #[must_use]
    pub fn bridge(&self) -> Option<&BridgeTransaction> {
        self.transaction.progress().bridge.as_ref()
    }

    pub fn bridge_mut(&mut self) -> Option<&mut BridgeTransaction> {
        self.transaction.progress_mut().bridge.as_mut()
    }

    #[must_use]
    pub fn ready_proof(&self) -> Option<&ReadyProof> {
        self.transaction.progress().ready_proof.as_ref()
    }

    /// Only a v5 Ready decision with a complete target incarnation census
    /// authorizes Commit. Earlier Ready files remain readable but Preserved.
    #[must_use]
    pub fn has_commit_certificate(&self) -> bool {
        self.schema_version == ACTIVATION_JOURNAL_SCHEMA_VERSION
            && self
                .ready_proof()
                .is_some_and(|proof| proof.validate(self).is_ok())
    }

    pub fn enter_activating(&mut self) {
        let progress = self.transaction.progress().clone();
        self.transaction = TransactionPhase::Activating { progress };
    }

    pub fn enter_ready(&mut self, proof: Option<ReadyProof>) {
        let mut progress = self.transaction.progress().clone();
        progress.ready_proof = proof;
        self.transaction = TransactionPhase::Ready { progress };
    }

    pub fn enter_committing(&mut self) {
        let progress = self.transaction.progress().clone();
        self.transaction = TransactionPhase::Committing { progress };
    }

    pub fn enter_rollback(&mut self, reason: String) {
        let progress = self.transaction.progress().clone();
        self.transaction = TransactionPhase::RollingBack { reason, progress };
    }

    pub fn enter_committed(&mut self) {
        let progress = self.transaction.progress().clone();
        self.transaction = TransactionPhase::Committed { progress };
    }

    pub fn enter_rolled_back(&mut self) {
        let progress = self.transaction.progress().clone();
        self.transaction = TransactionPhase::RolledBack { progress };
    }

    /// Derives target registration authority for one exact journal member.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal is invalid, lacks Zellij authority, or
    /// does not contain the exact member endpoint and handoff.
    pub fn target_registration_capability(
        &self,
        discovery_key: &str,
        endpoint: &Path,
        handoff: HandoffId,
    ) -> Result<TargetRegistrationCapability, JournalError> {
        if !matches!(
            self.directive(),
            TransactionDirective::Prepare | TransactionDirective::Activate
        ) {
            return Err(JournalError::Inconsistent(
                "Ready has sealed its target; no new target registration is authorized".to_owned(),
            ));
        }
        self.target_restore_capability(discovery_key, endpoint, handoff)
    }

    /// Internal rollback authority for restoring an old row after Ready; it
    /// cannot be obtained by a broker child through the public startup API.
    pub(crate) fn target_restore_capability(
        &self,
        discovery_key: &str,
        endpoint: &Path,
        handoff: HandoffId,
    ) -> Result<TargetRegistrationCapability, JournalError> {
        self.validate()?;
        let identity = self
            .bridge_identity
            .clone()
            .ok_or(JournalError::MissingZellijAuthority)?;
        let member = BridgeMemberId::new(discovery_key.to_owned())
            .map_err(|error| JournalError::Inconsistent(error.to_string()))?;
        let census = self
            .member_census
            .as_ref()
            .ok_or(JournalError::MissingZellijAuthority)?;
        let authorized = census.members().contains(&member)
            && self.members().iter().any(|record| {
                record.member().as_str() == discovery_key
                    && record.endpoint().as_path() == endpoint
                    && record.handoff_id() == handoff
            });
        if !authorized {
            return Err(JournalError::Inconsistent(
                "journal does not authorize this target member endpoint and handoff".to_owned(),
            ));
        }
        Ok(TargetRegistrationCapability::new(
            identity,
            member,
            endpoint.to_path_buf(),
            discovery_key.to_owned(),
            handoff,
        ))
    }

    /// Validates every transaction, member, bridge, artifact, and phase invariant.
    ///
    /// # Errors
    ///
    /// Returns an error for unsupported schemas or any inconsistent authority
    /// and progress combination.
    #[expect(
        clippy::too_many_lines,
        reason = "semantic validation intentionally keeps phase/member/artifact cross-invariants together"
    )]
    pub fn validate(&self) -> Result<(), JournalError> {
        if !matches!(
            self.schema_version,
            3 | 4 | ACTIVATION_JOURNAL_SCHEMA_VERSION
        ) {
            return Err(JournalError::UnsupportedVersion {
                path: PathBuf::from("<memory>"),
                version: self.schema_version,
            });
        }
        let progress = self.transaction.progress();
        if progress.members.is_empty() {
            return Err(JournalError::Inconsistent(
                "journal has no members".to_owned(),
            ));
        }
        let mut member_ids = std::collections::HashSet::new();
        let mut handoffs = std::collections::HashSet::new();
        for member in &progress.members {
            if ActivationMemberId::new(member.member().as_str().to_owned()).is_err()
                || MemberEndpoint::new(member.endpoint().as_path().to_path_buf()).is_err()
                || !member.id.is_valid()
                || member.id != MemberTransactionId::derive(self.activation_id, member.member())
                || member.handoff_id().0 == [0; 16]
                || !member_ids.insert(member.member().clone())
                || !handoffs.insert(member.handoff_id())
            {
                return Err(JournalError::Inconsistent(
                    "journal has invalid or duplicate member authority".to_owned(),
                ));
            }
            match (member.target, member.target_retirement.as_ref()) {
                (
                    TargetMemberProgress::RetireIntent | TargetMemberProgress::Retired,
                    Some(intent),
                ) => intent.validate(self, member)?,
                (TargetMemberProgress::RetireIntent | TargetMemberProgress::Retired, None) => {
                    return Err(JournalError::Inconsistent(
                        "retiring target lacks exact durable retirement intent".to_owned(),
                    ));
                }
                (_, Some(_)) => {
                    return Err(JournalError::Inconsistent(
                        "target retirement intent exists outside retirement progress".to_owned(),
                    ));
                }
                (_, None) => {}
            }
        }
        match &self.unit {
            UnitKind::Zellij { bridge_unit } => {
                let identity = self
                    .bridge_identity
                    .as_ref()
                    .ok_or(JournalError::MissingZellijAuthority)?;
                let census = self
                    .member_census
                    .as_ref()
                    .ok_or(JournalError::MissingZellijAuthority)?;
                if identity.unit() != *bridge_unit {
                    return Err(JournalError::Inconsistent(
                        "journal bridge identity disagrees with unit".to_owned(),
                    ));
                }
                let members = progress
                    .members
                    .iter()
                    .map(|member| BridgeMemberId::new(member.member().as_str().to_owned()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| JournalError::Inconsistent(error.to_string()))?;
                if census.members() != members.as_slice()
                    || MemberCensus::from_entries(identity, self.old_registry.iter())
                        .map_err(|error| JournalError::Inconsistent(error.to_string()))?
                        != *census
                {
                    return Err(JournalError::Inconsistent(
                        "journal member census or exact old rows disagree".to_owned(),
                    ));
                }
                let bridge = progress.bridge.as_ref().ok_or_else(|| {
                    JournalError::Inconsistent(
                        "Zellij transaction lacks bridge authority".to_owned(),
                    )
                })?;
                let mut expected_rollback = bridge.artifacts.receipt_preimage.clone();
                expected_rollback.previous_digest = Some(bridge.artifacts.target_digest.clone());
                if bridge.artifacts.old.activation != self.activation_id
                    || bridge.artifacts.target.activation != self.activation_id
                    || bridge.artifacts.old.role != BridgeArtifactRole::Old
                    || bridge.artifacts.target.role != BridgeArtifactRole::Target
                    || bridge.artifacts.receipt_preimage.bridge_identity != *identity
                    || bridge.artifacts.receipt_preimage.installed_digest
                        != bridge.artifacts.old_digest
                    || bridge.artifacts.receipt_target.bridge_identity != *identity
                    || bridge.artifacts.receipt_target.installed_digest
                        != bridge.artifacts.target_digest
                    || bridge.artifacts.receipt_target.previous_digest.as_ref()
                        != Some(&bridge.artifacts.old_digest)
                    || bridge.artifacts.receipt_target.installed_version
                        != self.target_record.muxe_version
                    || bridge.artifacts.receipt_target.bridge_compat != self.target_record.zellij
                    || bridge.artifacts.receipt_rollback != expected_rollback
                {
                    return Err(JournalError::Inconsistent(
                        "journal bridge artifacts or exact receipt states disagree with authority"
                            .to_owned(),
                    ));
                }
                if let Some((active, completed)) = bridge.progress.reload_coverage() {
                    let foreign = active
                        .iter()
                        .chain(completed.iter())
                        .any(|id| !progress.members.iter().any(|member| &member.id == id));
                    let duplicate = active.as_ref().is_some_and(|id| completed.contains(id))
                        || completed
                            .iter()
                            .enumerate()
                            .any(|(index, id)| completed[..index].contains(id));
                    if foreign || duplicate {
                        return Err(JournalError::Inconsistent(
                            "journal reload progress is foreign or duplicated".to_owned(),
                        ));
                    }
                }
                if let Some(reloaded) = bridge.progress.restored_coverage() {
                    let unmutated = reloaded.is_empty()
                        && progress.members.iter().all(|member| {
                            member.old == OldMemberProgress::Pending
                                && member.target == TargetMemberProgress::Absent
                        });
                    let complete = reloaded.len() == progress.members.len()
                        && progress.members.iter().all(|member| {
                            reloaded.iter().filter(|id| *id == &member.id).count() == 1
                        });
                    if !unmutated && !complete {
                        return Err(JournalError::Inconsistent(
                            "The journal's bridge-reload records do not exactly match the full list of participating sessions".to_owned(),
                        ));
                    }
                }
            }
            UnitKind::Herdr { host_hash } => {
                if HerdrUnitId::parse(host_hash.as_str().to_owned()).is_err()
                    || self.bridge_identity.is_some()
                    || self.member_census.is_some()
                    || progress.bridge.is_some()
                {
                    return Err(JournalError::Inconsistent(
                        "Herdr transaction carries Zellij bridge authority".to_owned(),
                    ));
                }
            }
        }

        let valid = match &self.transaction {
            TransactionPhase::Preparing { progress } => progress.members.iter().all(|member| {
                matches!(
                    member.old,
                    OldMemberProgress::Pending
                        | OldMemberProgress::PrepareIntent
                        | OldMemberProgress::Drained
                ) && member.target == TargetMemberProgress::Absent
            }),
            TransactionPhase::Activating { progress } => progress.members.iter().all(|member| {
                member.old == OldMemberProgress::Drained
                    && matches!(
                        member.target,
                        TargetMemberProgress::Absent
                            | TargetMemberProgress::SpawnIntent
                            | TargetMemberProgress::Gated
                            | TargetMemberProgress::Ready
                    )
            }),
            TransactionPhase::Ready { progress } => progress.members.iter().all(|member| {
                member.old == OldMemberProgress::Drained
                    && member.target == TargetMemberProgress::Ready
            }),
            TransactionPhase::Committing { progress } => progress.members.iter().all(|member| {
                matches!(
                    member.old,
                    OldMemberProgress::Drained
                        | OldMemberProgress::CommitIntent
                        | OldMemberProgress::Committed
                ) && matches!(
                    member.target,
                    TargetMemberProgress::Ready
                        | TargetMemberProgress::CommitIntent
                        | TargetMemberProgress::Committed
                )
            }),
            TransactionPhase::RollingBack { progress, .. } => {
                progress.members.iter().all(|member| {
                    !matches!(
                        member.old,
                        OldMemberProgress::CommitIntent | OldMemberProgress::Committed
                    )
                })
            }
            TransactionPhase::Committed { progress } => progress.members.iter().all(|member| {
                member.old == OldMemberProgress::Committed
                    && member.target == TargetMemberProgress::Committed
            }),
            TransactionPhase::RolledBack { progress } => progress.members.iter().all(|member| {
                matches!(
                    member.old,
                    OldMemberProgress::Pending | OldMemberProgress::Resumed
                ) && matches!(
                    member.target,
                    TargetMemberProgress::Absent | TargetMemberProgress::Retired
                )
            }),
        };
        let bridge_valid = progress
            .bridge
            .as_ref()
            .is_none_or(|bridge| match &self.transaction {
                TransactionPhase::Preparing { .. } => matches!(
                    &bridge.progress,
                    BridgeProgress::ArtifactsPending | BridgeProgress::ArtifactsReady
                ),
                TransactionPhase::Activating { .. } => matches!(
                    &bridge.progress,
                    BridgeProgress::ArtifactsReady
                        | BridgeProgress::TargetInstallIntent
                        | BridgeProgress::TargetInstalled
                        | BridgeProgress::TargetReloading { .. }
                        | BridgeProgress::TargetReloaded
                ),
                TransactionPhase::Ready { .. } => bridge.progress == BridgeProgress::TargetReloaded,
                TransactionPhase::Committing { .. } => matches!(
                    &bridge.progress,
                    BridgeProgress::TargetReloaded
                        | BridgeProgress::PreviousPublishIntent
                        | BridgeProgress::PreviousPublished
                        | BridgeProgress::ReceiptIntent
                        | BridgeProgress::ReceiptPublished
                ),
                TransactionPhase::RollingBack { progress, .. } => {
                    let targets_retired = progress.members.iter().all(|member| {
                        matches!(
                            member.target,
                            TargetMemberProgress::Absent | TargetMemberProgress::Retired
                        )
                    });
                    let restore_started = matches!(
                        bridge.progress,
                        BridgeProgress::OldInstallIntent
                            | BridgeProgress::OldInstalled
                            | BridgeProgress::TargetPreviousIntent
                            | BridgeProgress::TargetPreviousPublished
                            | BridgeProgress::OldReceiptIntent
                            | BridgeProgress::OldReceiptPublished
                            | BridgeProgress::OldReloading { .. }
                            | BridgeProgress::Restored { .. }
                    );
                    (!restore_started || targets_retired)
                        && !progress.members.iter().any(|member| {
                            matches!(
                                member.old,
                                OldMemberProgress::ResumeIntent | OldMemberProgress::Resumed
                            ) && !matches!(bridge.progress, BridgeProgress::Restored { .. })
                        })
                }
                TransactionPhase::Committed { .. } => {
                    bridge.progress == BridgeProgress::ReceiptPublished
                }
                TransactionPhase::RolledBack { .. } => {
                    matches!(bridge.progress, BridgeProgress::Restored { .. })
                }
            });
        let rollback_target_barrier_valid =
            if let TransactionPhase::RollingBack { progress, .. } = &self.transaction {
                let targets_retired = progress.members.iter().all(|member| {
                    matches!(
                        member.target,
                        TargetMemberProgress::Absent | TargetMemberProgress::Retired
                    )
                });
                targets_retired
                    || !progress.members.iter().any(|member| {
                        matches!(
                            member.old,
                            OldMemberProgress::ResumeIntent | OldMemberProgress::Resumed
                        )
                    })
            } else {
                true
            };
        let proof_phase = matches!(
            self.transaction,
            TransactionPhase::Ready { .. }
                | TransactionPhase::Committing { .. }
                | TransactionPhase::Committed { .. }
        );
        let proof_valid = match (self.schema_version, progress.ready_proof.as_ref()) {
            (5, Some(proof)) if proof_phase => proof.validate(self).is_ok(),
            (5, None) if !proof_phase => true,
            (4, Some(proof)) if proof_phase => proof.validate_legacy(self),
            (4, None) if !proof_phase || matches!(self.unit, UnitKind::Herdr { .. }) => true,
            (3, None) => true,
            _ => false,
        };
        if !proof_valid {
            return Err(JournalError::Inconsistent(
                "transaction phase lacks exact versioned Ready proof".to_owned(),
            ));
        }
        let valid = valid && bridge_valid && rollback_target_barrier_valid;
        if !valid {
            return Err(JournalError::Inconsistent(
                "transaction phase contains impossible member progress".to_owned(),
            ));
        }
        Ok(())
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], &'static str> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("identifier is not canonical lowercase hexadecimal");
    }
    let mut bytes = [0_u8; N];
    for (slot, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => unreachable!("validated hexadecimal byte"),
        };
        *slot = (digit(pair[0]) << 4) | digit(pair[1]);
    }
    Ok(bytes)
}

/// Returns the activation directory for a cache directory.
#[must_use]
pub fn activation_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join(ACTIVATION_DIR_NAME)
}

/// Transaction- and incarnation-bound identity of one old-broker stop proof.
#[derive(Clone, Debug, Eq, PartialEq)]
struct OldRetirementReceiptId(String);

impl OldRetirementReceiptId {
    fn for_member(
        journal: &ActivationJournal,
        member: &TransactionMember,
    ) -> Result<(Self, OldRetirementReceipt), JournalError> {
        let mut entries = journal.old_registry.iter().filter(|entry| {
            entry.socket == member.endpoint().as_path()
                && entry.discovery_key == member.member().as_str()
        });
        let entry = entries.next().ok_or_else(|| {
            JournalError::Inconsistent(
                "The journal does not contain the old broker's recorded identity. Muxe cannot verify that it stopped for this handoff".to_owned(),
            )
        })?;
        if entries.next().is_some()
            || entry
                .registration_id
                .is_none_or(BrokerRegistrationId::is_zero)
            || entry.server_pid == 0
            || entry.live_server.as_deref().is_none_or(str::is_empty)
        {
            return Err(JournalError::Inconsistent(
                "The journal's recorded identity for the old broker is incomplete or matches more than one entry. Muxe cannot verify that it stopped for this handoff".to_owned(),
            ));
        }
        let receipt = OldRetirementReceipt {
            schema_version: OLD_RETIREMENT_RECEIPT_SCHEMA_VERSION,
            activation_id: journal.activation_id,
            member_id: member.id.clone(),
            authority: member.authority.clone(),
            old_record: member.old_record.clone(),
            target_record: journal.target_record.clone(),
            old_entry: entry.clone(),
        };
        let bytes = serde_json::to_vec(&receipt).map_err(|source| JournalError::Corrupt {
            path: PathBuf::from("<old retirement identity>"),
            source,
        })?;
        Ok((Self(fsutil::sha256_hex(&bytes)), receipt))
    }

    fn path(&self, directory: &Path) -> PathBuf {
        directory.join(format!(".muxe-old-retired-{}.receipt", self.0))
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OldRetirementReceipt {
    schema_version: u32,
    activation_id: ActivationId,
    member_id: MemberTransactionId,
    authority: MemberLaunchAuthority,
    old_record: CompatibilityRecord,
    target_record: CompatibilityRecord,
    old_entry: BrokerEntry,
}

/// Checks one exact old-broker post-stop receipt. Absence is not retirement proof.
///
/// # Errors
///
/// A foreign, corrupt, symlinked, or non-owner entry fails closed.
pub fn has_old_retirement_receipt(
    directory: &Path,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<bool, JournalError> {
    let (id, expected) = OldRetirementReceiptId::for_member(journal, member)?;
    let path = id.path(directory);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(JournalError::Fs(fsutil::io_error(
                "checking old retirement receipt",
                &path,
                source,
            )));
        }
        Ok(_) => {}
    }
    let bytes = fsutil::read_owner_file(&path)?;
    let actual = serde_json::from_slice::<OldRetirementReceipt>(&bytes).map_err(|source| {
        JournalError::Corrupt {
            path: path.clone(),
            source,
        }
    })?;
    if actual != expected {
        return Err(JournalError::Inconsistent(
            "old retirement receipt does not attest the exact journal incarnation".to_owned(),
        ));
    }
    Ok(true)
}

/// Durably publishes old-broker retirement only after the stop ticket exists.
///
/// # Errors
///
/// Requires a durable Commit decision, the exact old server identity and a
/// vacant receipt path; never replaces preexisting evidence.
pub fn write_old_retirement_receipt(
    cache_dir: &Path,
    journal: &ActivationJournal,
    member: &TransactionMember,
    server_id: &ServerId,
) -> Result<(), JournalError> {
    if journal.directive() != TransactionDirective::Commit
        || !journal.has_commit_certificate()
        || !matches!(member.old, OldMemberProgress::CommitIntent)
    {
        return Err(JournalError::Inconsistent(
            "old retirement requires durable exact commit intent".to_owned(),
        ));
    }
    let (id, receipt) = OldRetirementReceiptId::for_member(journal, member)?;
    if receipt.old_entry.live_server.as_deref() != Some(server_id.as_str()) {
        return Err(JournalError::Inconsistent(
            "old retirement server differs from recorded old incarnation".to_owned(),
        ));
    }
    let path = id.path(&activation_dir(cache_dir));
    let bytes = serde_json::to_vec_pretty(&receipt).map_err(|source| JournalError::Corrupt {
        path: path.clone(),
        source,
    })?;
    fsutil::write_atomic_new(&path, &bytes, "old-retirement")?;
    Ok(())
}

/// Removes a validated old-retirement proof during terminal cleanup.
///
/// # Errors
///
/// Refuses malformed or foreign entries and incomplete unlink durability.
pub fn remove_old_retirement_receipt(
    directory: &Path,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<(), JournalError> {
    let (id, _) = OldRetirementReceiptId::for_member(journal, member)?;
    let path = id.path(directory);
    has_old_retirement_receipt(directory, journal, member)?;
    fsutil::remove_file_durable(&path, "removing old retirement receipt")?;
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct TargetRetirementReceipt {
    schema_version: u32,
    activation_id: ActivationId,
    member_id: MemberTransactionId,
    launch_authority: MemberLaunchAuthority,
    target_record: CompatibilityRecord,
    retirement: TargetRetirementIntent,
}

fn retirement_intent(member: &TransactionMember) -> Result<&TargetRetirementIntent, JournalError> {
    if !matches!(
        member.target,
        TargetMemberProgress::RetireIntent | TargetMemberProgress::Retired
    ) {
        return Err(JournalError::Inconsistent(
            "target retirement receipt requested outside retirement progress".to_owned(),
        ));
    }
    member.target_retirement.as_ref().ok_or_else(|| {
        JournalError::Inconsistent(
            "target retirement receipt lacks durable intent authority".to_owned(),
        )
    })
}

/// Returns the predeclared receipt path for one durable target retirement.
#[must_use]
pub(crate) fn target_retirement_receipt_path(
    directory: &Path,
    intent: &TargetRetirementIntent,
) -> PathBuf {
    directory.join(format!(
        ".muxe-target-retired-{}.receipt",
        intent.receipt_id().as_str()
    ))
}

fn validate_target_retirement_receipt(
    receipt: &TargetRetirementReceipt,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<(), JournalError> {
    let expected = retirement_intent(member)?;
    expected.validate(journal, member)?;
    if receipt.schema_version != TARGET_RETIREMENT_RECEIPT_SCHEMA_VERSION
        || receipt.activation_id != journal.activation_id
        || receipt.member_id != member.id
        || receipt.launch_authority != member.authority
        || receipt.target_record != journal.target_record
        || receipt.retirement != *expected
    {
        return Err(JournalError::Inconsistent(
            "target retirement receipt does not attest exact journal authority".to_owned(),
        ));
    }
    Ok(())
}

/// Loads and validates the exact durable post-stop receipt for one target.
///
/// # Errors
///
/// Foreign, stale, malformed, mismatched, non-owner, and symlinked entries all
/// fail closed. Absence alone returns `false`.
pub(crate) fn has_target_retirement_receipt(
    directory: &Path,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<bool, JournalError> {
    let intent = retirement_intent(member)?;
    let path = target_retirement_receipt_path(directory, intent);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(JournalError::Fs(fsutil::io_error(
                "checking target retirement receipt",
                &path,
                source,
            )));
        }
        Ok(_) => {}
    }
    let bytes = fsutil::read_owner_file(&path)?;
    let receipt = serde_json::from_slice::<TargetRetirementReceipt>(&bytes).map_err(|source| {
        JournalError::Corrupt {
            path: path.clone(),
            source,
        }
    })?;
    validate_target_retirement_receipt(&receipt, journal, member)?;
    Ok(true)
}

/// Reports whether any entry occupies a predeclared receipt path.
///
/// This is used before persisting a fresh `RetireIntent` so a pre-stop file can
/// never be reinterpreted as post-stop evidence on replay.
pub(crate) fn target_retirement_receipt_entry_exists(
    directory: &Path,
    intent: &TargetRetirementIntent,
) -> Result<bool, JournalError> {
    let path = target_retirement_receipt_path(directory, intent);
    match fs::symlink_metadata(&path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(JournalError::Fs(fsutil::io_error(
            "checking target retirement receipt path",
            &path,
            source,
        ))),
    }
}

/// Publishes exact post-stop proof without replacing any existing entry.
///
/// # Errors
///
/// Returns an error for invalid journal authority, serialization failures, an
/// existing entry of any kind, or incomplete file/directory durability.
pub(crate) fn write_target_retirement_receipt(
    directory: &Path,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<(), JournalError> {
    let intent = retirement_intent(member)?;
    intent.validate(journal, member)?;
    let receipt = TargetRetirementReceipt {
        schema_version: TARGET_RETIREMENT_RECEIPT_SCHEMA_VERSION,
        activation_id: journal.activation_id,
        member_id: member.id.clone(),
        launch_authority: member.authority.clone(),
        target_record: journal.target_record.clone(),
        retirement: intent.clone(),
    };
    let path = target_retirement_receipt_path(directory, intent);
    let bytes = serde_json::to_vec_pretty(&receipt).map_err(|source| JournalError::Corrupt {
        path: path.clone(),
        source,
    })?;
    fsutil::write_atomic_new(&path, &bytes, "target-retirement")?;
    Ok(())
}

/// Persists exact broker-local post-stop proof before advancing to `Retired`.
///
/// The caller must hold the retirement ticket returned after listener shutdown.
/// Receipt replay is accepted only when its full journal/process authority
/// matches; the journal outcome is always written after receipt durability.
///
/// # Errors
///
/// Returns an error for a mismatched member, handoff, server incarnation,
/// retirement intent, receipt, or journal write.
pub fn acknowledge_remote_target_retirement(
    cache_dir: &Path,
    journal: &mut ActivationJournal,
    discovery_key: &ActivationMemberId,
    handoff: HandoffId,
    server_id: &ServerId,
) -> Result<PathBuf, JournalError> {
    if journal.directive() != TransactionDirective::RollBack {
        return Err(JournalError::Inconsistent(
            "target retirement acknowledgement requires rollback".to_owned(),
        ));
    }
    let index = journal
        .members()
        .iter()
        .position(|member| member.member() == discovery_key && member.handoff_id() == handoff)
        .ok_or_else(|| {
            JournalError::Inconsistent(
                "target retirement acknowledgement does not name one member".to_owned(),
            )
        })?;
    let member = journal.members()[index].clone();
    if member.target != TargetMemberProgress::RetireIntent {
        return Err(JournalError::Inconsistent(
            "target retirement acknowledgement requires durable RetireIntent".to_owned(),
        ));
    }
    let intent = retirement_intent(&member)?;
    if !matches!(
        intent.authority(),
        TargetRetirementAuthority::RemoteServer {
            server_id: expected
        } if expected == server_id
    ) {
        return Err(JournalError::Inconsistent(
            "target retirement acknowledgement has mismatched server authority".to_owned(),
        ));
    }
    let directory = activation_dir(cache_dir);
    if !has_target_retirement_receipt(&directory, journal, &member)? {
        write_target_retirement_receipt(&directory, journal, &member)?;
    }
    journal.members_mut()[index].target = TargetMemberProgress::Retired;
    write_journal(cache_dir, journal)
}

/// Durably removes an exact receipt during terminal transaction cleanup.
///
/// Already-absent receipts are accepted so unlink-before-journal-removal replay
/// remains idempotent. Any present entry must validate exactly before removal.
pub(crate) fn remove_target_retirement_receipt(
    directory: &Path,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<(), JournalError> {
    let intent = retirement_intent(member)?;
    let path = target_retirement_receipt_path(directory, intent);
    has_target_retirement_receipt(directory, journal, member)?;
    fsutil::remove_file_durable(&path, "removing target retirement receipt")?;
    Ok(())
}

/// Durably writes a journal by atomic replacement plus directory sync.
///
/// Must be called before the first external mutation of the unit.
///
/// # Errors
///
/// Returns [`JournalError`] when validation, directory creation, serialization,
/// or the atomic write fails.
pub fn write_journal(
    cache_dir: &Path,
    journal: &ActivationJournal,
) -> Result<PathBuf, JournalError> {
    journal.validate()?;
    let directory = activation_dir(cache_dir);
    fsutil::ensure_owner_dir(&directory)?;
    let path = directory.join(journal.unit.journal_name());
    let bytes = serde_json::to_vec_pretty(journal).map_err(|source| JournalError::Corrupt {
        path: path.clone(),
        source,
    })?;
    fsutil::write_atomic(&path, &bytes, "activation")?;
    Ok(path)
}

/// Reads and validates a journal. Unrecognized states fail closed.
///
/// # Errors
///
/// Returns [`JournalError`] when the file cannot be read, deserialized, or validated.
pub fn read_journal(path: &Path) -> Result<ActivationJournal, JournalError> {
    let bytes = fsutil::read_owner_file(path)?;
    let raw: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|source| JournalError::Corrupt {
            path: path.to_path_buf(),
            source,
        })?;
    let Some(version) = raw
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
    else {
        return Err(JournalError::UnsupportedVersion {
            path: path.to_path_buf(),
            version: 0,
        });
    };
    if !matches!(version, 3..=5) {
        return Err(JournalError::UnsupportedVersion {
            path: path.to_path_buf(),
            version: u32::try_from(version).unwrap_or(u32::MAX),
        });
    }
    let journal: ActivationJournal =
        serde_json::from_value(raw).map_err(|source| JournalError::Corrupt {
            path: path.to_path_buf(),
            source,
        })?;
    journal.validate()?;
    Ok(journal)
}

/// Removes a journal after its unit commits. Only called once the complete
/// target stack is durable.
///
/// # Errors
///
/// Returns [`JournalError`] when removal or directory sync fails.
pub fn remove_journal(path: &Path) -> Result<(), JournalError> {
    fsutil::remove_file_durable(path, "removing activation journal")?;
    Ok(())
}

/// One unit's journal entries: paths paired with their decode outcome.
pub type JournalEntries = Vec<(PathBuf, Result<ActivationJournal, JournalError>)>;

/// Lists every journal file present. Corrupt or unrecognized journals are
/// returned inline so recovery preserves them instead of deleting what it
/// cannot understand.
///
/// # Errors
///
/// Returns [`JournalError`] when the activation directory cannot be scanned.
/// Per-file corruptions are returned inline, never as an outer error.
pub fn list_journals(cache_dir: &Path) -> Result<JournalEntries, JournalError> {
    let directory = activation_dir(cache_dir);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(JournalError::Fs(fsutil::io_error(
                "scanning activation journals",
                &directory,
                source,
            )));
        }
    };
    let mut journals = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| {
            fsutil::io_error("scanning activation journals", &directory, source)
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        journals.push((path.clone(), read_journal(&path)));
    }
    journals.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(journals)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn fixture_record(version: &str) -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: version.to_owned(),
            target_triple: "aarch64-apple-darwin".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([7; 32]),
            zellij: None,
            herdr: None,
        }
    }

    fn fixture_journal() -> ActivationJournal {
        let activation = ActivationId::from_bytes([1; 16]).unwrap();
        ActivationJournal::new(
            activation,
            UnitKind::Herdr {
                host_hash: unit_hash("server"),
            },
            fixture_record("0.2.0"),
            vec![
                TransactionMember::new(
                    activation,
                    ActivationMemberId::new("server".to_owned()).unwrap(),
                    MemberEndpoint::new(PathBuf::from("/tmp/old.sock")).unwrap(),
                    HandoffId([0xab; 16]),
                    fixture_record("0.1.0"),
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }
    fn fixture_ready_proof(journal: &ActivationJournal) -> ReadyProof {
        let member = &journal.members()[0];
        let mut row = BrokerEntry::now(
            "herdr",
            member.member().as_str(),
            member.endpoint().as_path().to_path_buf(),
            77,
        );
        row.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::from_bytes([7; 16]).unwrap());
        row.live_server = Some("server-id".to_owned());
        ReadyProof::new(
            journal,
            None,
            vec![ReadyMemberProof::new(member, &row, &ServerId::new("server-id")).unwrap()],
        )
        .unwrap()
    }

    #[test]
    fn write_read_round_trip_is_owner_only() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let journal = fixture_journal();
        let path = write_journal(temp.path(), &journal).unwrap();
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("herdr-"))
        );
        crate::logging::assert_owner_only(&path);
        assert_eq!(read_journal(&path).unwrap(), journal);
    }
    #[test]
    fn legacy_hash_only_zellij_journal_is_preserved_as_unsupported() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let directory = activation_dir(temp.path());
        fsutil::ensure_owner_dir(&directory).unwrap();
        let path = directory.join("zellij-legacy.json");
        fsutil::write_atomic(
            &path,
            br#"{
                "schema_version": 1,
                "unit": "zellij",
                "bridge_path_hash": "0123456789abcdef0123456789abcdef"
            }"#,
            "legacy",
        )
        .unwrap();

        assert!(matches!(
            read_journal(&path),
            Err(JournalError::UnsupportedVersion { version: 1, .. })
        ));
        assert!(
            path.exists(),
            "unsupported legacy journal remains untouched"
        );
    }

    #[test]
    fn prior_schema_v2_is_preserved_as_unsupported_before_shape_decode() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let journal = fixture_journal();
        let directory = activation_dir(temp.path());
        fsutil::ensure_owner_dir(&directory).unwrap();
        let path = directory.join(journal.unit.journal_name());
        let mut value = serde_json::to_value(journal).unwrap();
        value["schema_version"] = serde_json::Value::from(2);
        let bytes = serde_json::to_vec_pretty(&value).unwrap();
        fsutil::write_atomic(&path, &bytes, "schema-v2").unwrap();

        assert!(matches!(
            read_journal(&path),
            Err(JournalError::UnsupportedVersion { version: 2, .. })
        ));
        assert_eq!(
            fsutil::read_owner_file(&path).unwrap(),
            bytes,
            "unsupported schema-v2 journal remains byte-for-byte untouched"
        );
    }

    #[test]
    fn unit_lock_is_released_when_owner_process_dies_and_inode_persists() {
        const CHILD_DIRECTORY: &str = "MUXE_UNIT_LOCK_CHILD_DIRECTORY";
        if let Some(directory) = std::env::var_os(CHILD_DIRECTORY) {
            let directory = Path::new(&directory);
            let unit = UnitKind::Herdr {
                host_hash: unit_hash("server"),
            };
            let _lock = acquire_unit_lock(directory, &unit).expect("child acquires unit lock");
            println!("UNIT_LOCK_READY");
            std::io::Write::flush(&mut std::io::stdout()).expect("flush lock-ready marker");
            loop {
                std::thread::park();
            }
        }

        let temp = tempfile::TempDir::new().unwrap();
        let unit = UnitKind::Herdr {
            host_hash: unit_hash("server"),
        };
        let executable = std::env::current_exe().unwrap();
        let mut child = std::process::Command::new(executable)
            .args([
                "--exact",
                "lifecycle::journal::tests::unit_lock_is_released_when_owner_process_dies_and_inode_persists",
                "--nocapture",
            ])
            .env(CHILD_DIRECTORY, temp.path())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut reader = std::io::BufReader::new(stdout);

        let mut line = String::new();
        loop {
            line.clear();
            assert!(std::io::BufRead::read_line(&mut reader, &mut line).unwrap() > 0);
            if line.trim_end() == "UNIT_LOCK_READY" {
                break;
            }
        }

        assert!(acquire_unit_lock(temp.path(), &unit).is_err());
        let lock_path = activation_dir(temp.path()).join(format!(
            "{}.lock",
            unit.journal_name().trim_end_matches(".json")
        ));
        let before = std::fs::metadata(&lock_path).unwrap();
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(
            !status.success(),
            "lock owner must be killed, not exit cleanly"
        );
        let reacquired = acquire_unit_lock(temp.path(), &unit).unwrap();
        let after = std::fs::metadata(&lock_path).unwrap();
        assert_eq!(
            std::os::unix::fs::MetadataExt::ino(&before),
            std::os::unix::fs::MetadataExt::ino(&after),
            "reacquisition keeps the persistent lock inode"
        );
        drop(reacquired);
    }

    #[test]
    fn zero_handoff_fails_closed() {
        let mut journal = fixture_journal();
        journal.members_mut()[0].authority.handoff_id = HandoffId([0; 16]);
        assert!(matches!(
            journal.validate(),
            Err(JournalError::Inconsistent(_))
        ));
    }

    #[test]
    fn old_stop_receipt_reconciles_exact_commit_after_ack_write_failure() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut journal = fixture_journal();
        let member_id = journal.members()[0].member().clone();
        let handoff = journal.members()[0].handoff_id();
        let old_server = ServerId::new("old-live-server");
        let mut old = BrokerEntry::now(
            "herdr",
            member_id.as_str(),
            journal.members()[0].endpoint().as_path().to_path_buf(),
            std::process::id(),
        );
        old.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::from_bytes([9; 16]).unwrap());
        old.live_server = Some(old_server.as_str().to_owned());
        journal.old_registry.push(old);
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Ready;
        let proof = fixture_ready_proof(&journal);
        journal.enter_ready(Some(proof));
        journal.enter_committing();
        journal.members_mut()[0].old = OldMemberProgress::CommitIntent;
        let path = write_journal(temp.path(), &journal).unwrap();
        let directory = activation_dir(temp.path());
        assert!(!has_old_retirement_receipt(&directory, &journal, &journal.members()[0]).unwrap());
        assert!(
            write_old_retirement_receipt(
                temp.path(),
                &journal,
                &journal.members()[0],
                &ServerId::new("foreign-server")
            )
            .is_err()
        );
        write_old_retirement_receipt(temp.path(), &journal, &journal.members()[0], &old_server)
            .unwrap();
        assert!(has_old_retirement_receipt(&directory, &journal, &journal.members()[0]).unwrap());
        assert!(
            write_old_retirement_receipt(temp.path(), &journal, &journal.members()[0], &old_server)
                .is_err(),
            "a retry cannot replace the exact post-stop proof"
        );

        journal
            .acknowledge_broker(&member_id, handoff, BrokerRecoveryAck::OldCommitted)
            .unwrap();
        fsutil::inject_tagged_durability_fault("activation", fsutil::DurabilityFault::BeforeRename);
        assert!(write_journal(temp.path(), &journal).is_err());
        let mut recovered = read_journal(&path).unwrap();
        assert_eq!(recovered.members()[0].old, OldMemberProgress::CommitIntent);
        assert!(
            has_old_retirement_receipt(&directory, &recovered, &recovered.members()[0]).unwrap()
        );
        recovered
            .acknowledge_broker(&member_id, handoff, BrokerRecoveryAck::OldCommitted)
            .unwrap();
        write_journal(temp.path(), &recovered).unwrap();
        assert_eq!(
            read_journal(&path).unwrap().members()[0].old,
            OldMemberProgress::Committed
        );
    }

    #[test]
    fn herdr_resume_intent_requires_complete_target_retirement_barrier() {
        let mut journal = fixture_journal();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        journal.enter_rollback("test rollback".to_owned());
        journal.validate().unwrap();

        journal.members_mut()[0].old = OldMemberProgress::ResumeIntent;
        assert!(journal.validate().is_err());
        let member = journal.members()[0].clone();
        let intent = TargetRetirementIntent::new(
            &journal,
            &member,
            TargetRetirementAuthority::OwnedProcess {
                process_id: TargetProcessId::new(7).unwrap(),
            },
        )
        .unwrap();
        let member = &mut journal.members_mut()[0];
        member.target = TargetMemberProgress::Retired;
        member.target_retirement = Some(intent);
        journal.validate().unwrap();
    }

    #[test]
    fn tagged_phase_is_the_only_fate_driver() {
        let mut journal = fixture_journal();
        assert_eq!(journal.directive(), TransactionDirective::Prepare);
        journal.enter_activating();
        assert_eq!(journal.directive(), TransactionDirective::Activate);
        journal.enter_ready(None);
        assert_eq!(journal.directive(), TransactionDirective::Commit);
        journal.enter_rollback("refused".to_owned());
        assert_eq!(journal.directive(), TransactionDirective::RollBack);
        journal.enter_rolled_back();
        assert_eq!(journal.directive(), TransactionDirective::CleanupRolledBack);
    }
    #[test]
    fn broker_acknowledgements_advance_only_the_durable_fate() {
        let member_id = ActivationMemberId::new("server".to_owned()).unwrap();
        let mut rollback = fixture_journal();
        let handoff = rollback.members()[0].handoff_id();
        rollback.members_mut()[0].old = OldMemberProgress::Drained;
        rollback.enter_rollback("test rollback".to_owned());
        assert!(
            rollback
                .acknowledge_broker(&member_id, handoff, BrokerRecoveryAck::Resumed)
                .is_err(),
            "broker resume acknowledgement requires durable ResumeIntent"
        );
        rollback.members_mut()[0].old = OldMemberProgress::ResumeIntent;
        rollback
            .acknowledge_broker(&member_id, handoff, BrokerRecoveryAck::Resumed)
            .unwrap();
        assert_eq!(rollback.directive(), TransactionDirective::RollBack);
        assert_eq!(rollback.members()[0].old, OldMemberProgress::Resumed);
        rollback.validate().unwrap();

        let mut commit = fixture_journal();
        let handoff = commit.members()[0].handoff_id();
        commit.members_mut()[0].old = OldMemberProgress::Drained;
        commit.members_mut()[0].target = TargetMemberProgress::Ready;
        let proof = fixture_ready_proof(&commit);
        commit.enter_ready(Some(proof));
        commit.enter_committing();
        commit
            .acknowledge_broker(&member_id, handoff, BrokerRecoveryAck::OldCommitted)
            .unwrap();
        assert_eq!(commit.directive(), TransactionDirective::Commit);
        commit.validate().unwrap();
        commit
            .acknowledge_broker(&member_id, handoff, BrokerRecoveryAck::TargetCommitted)
            .unwrap();
        assert_eq!(commit.directive(), TransactionDirective::Commit);
        commit.validate().unwrap();
    }

    #[test]
    fn remote_target_retirement_acknowledgement_persists_exact_receipt_first() {
        let temp = tempfile::TempDir::new().unwrap();
        let mut journal = fixture_journal();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.enter_rollback("test rollback".to_owned());
        let member = journal.members()[0].clone();
        let server_id = ServerId::new("exact-target-server");
        let intent = TargetRetirementIntent::new(
            &journal,
            &member,
            TargetRetirementAuthority::RemoteServer {
                server_id: server_id.clone(),
            },
        )
        .unwrap();
        let member = &mut journal.members_mut()[0];
        member.target = TargetMemberProgress::RetireIntent;
        member.target_retirement = Some(intent);
        write_journal(temp.path(), &journal).unwrap();
        let handoff = journal.members()[0].handoff_id();
        let member_id = journal.members()[0].member().clone();

        assert!(
            acknowledge_remote_target_retirement(
                temp.path(),
                &mut journal,
                &member_id,
                handoff,
                &ServerId::new("foreign-target-server"),
            )
            .is_err()
        );
        assert_eq!(
            journal.members()[0].target,
            TargetMemberProgress::RetireIntent
        );
        acknowledge_remote_target_retirement(
            temp.path(),
            &mut journal,
            &member_id,
            handoff,
            &server_id,
        )
        .unwrap();
        assert_eq!(journal.members()[0].target, TargetMemberProgress::Retired);
        assert!(
            has_target_retirement_receipt(
                &activation_dir(temp.path()),
                &journal,
                &journal.members()[0],
            )
            .unwrap()
        );
    }

    #[test]
    fn schema_v5_has_one_nested_transaction_state() {
        let value = serde_json::to_value(fixture_journal()).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(
            object
                .get("schema_version")
                .and_then(serde_json::Value::as_u64),
            Some(5)
        );
        for obsolete in [
            "state",
            "members",
            "target_members",
            "recovery",
            "bridge_restored",
            "backup_path",
            "staged_bridge_digest",
            "old_bridge_digest",
        ] {
            assert!(
                !object.contains_key(obsolete),
                "schema v5 must not retain parallel `{obsolete}` state"
            );
        }
        let transaction = object
            .get("transaction")
            .and_then(serde_json::Value::as_object)
            .unwrap();
        assert_eq!(
            transaction.get("phase").and_then(serde_json::Value::as_str),
            Some("preparing")
        );
        assert!(transaction.get("progress").is_some());
    }
    #[test]
    fn empty_membership_fails_closed() {
        let mut journal = fixture_journal();
        journal.transaction.progress_mut().members.clear();
        assert!(matches!(
            journal.validate(),
            Err(JournalError::Inconsistent(_))
        ));
    }

    #[test]
    fn corrupt_journal_is_reported_not_deleted() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let directory = activation_dir(temp.path());
        fsutil::ensure_owner_dir(&directory).unwrap();
        fsutil::write_atomic(&directory.join("herdr-x.json"), b"{corrupt", "activation").unwrap();
        let listed = list_journals(temp.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert!(listed[0].1.is_err());
        assert!(listed[0].0.exists());
    }
    #[test]
    fn cache_lease_keys_distinguish_non_utf8_paths() {
        use std::os::unix::ffi::OsStringExt as _;

        let temp = tempfile::tempdir().unwrap();
        let first = temp
            .path()
            .join(std::ffi::OsString::from_vec(vec![b'c', 0x80]));
        let second = temp
            .path()
            .join(std::ffi::OsString::from_vec(vec![b'c', 0x81]));
        assert_eq!(first.to_string_lossy(), second.to_string_lossy());
        assert_ne!(
            cache_lock_path(&first).unwrap(),
            cache_lock_path(&second).unwrap()
        );
    }

    #[test]
    fn cache_lock_contention_blocks_purge_and_shared_participants() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache = temp.path().join("cache");
        let shared = acquire_cache_lease(&cache).unwrap();
        let another_shared = acquire_cache_lease(&cache).unwrap();
        let expected_path = cache_lock_path(&cache).unwrap();
        assert!(matches!(
            acquire_cache_purge_lock(&cache),
            Err(JournalError::CacheActive { path }) if path == expected_path
        ));
        drop(shared);
        drop(another_shared);
        let exclusive = acquire_cache_purge_lock(&cache).unwrap();
        assert!(matches!(
            acquire_cache_lease(&cache),
            Err(JournalError::CacheActive { path }) if path == expected_path
        ));
        drop(exclusive);
        assert!(acquire_cache_lease(&cache).is_ok());
    }

    #[test]
    fn cache_lock_noncontention_errors_retain_the_os_cause() {
        let path = PathBuf::from("/cache-lock");
        for cause in [
            nix::errno::Errno::EBADF,
            nix::errno::Errno::ENOLCK,
            nix::errno::Errno::EINTR,
        ] {
            let error = cache_lock_error(path.clone(), cause);
            assert!(matches!(
                &error,
                JournalError::CacheLock { path: actual, source }
                    if actual == &path && *source == cause
            ));
            assert!(std::error::Error::source(&error).is_some());
        }
        assert!(matches!(
            cache_lock_error(path.clone(), nix::errno::Errno::EWOULDBLOCK),
            JournalError::CacheActive { path: actual } if actual == path
        ));
    }

    #[test]
    fn journal_unlink_replay_resyncs_an_already_absent_entry() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let path = write_journal(temp.path(), &fixture_journal()).unwrap();
        crate::fsutil::inject_durability_fault(
            crate::fsutil::DurabilityFault::AfterUnlinkBeforeDirectorySync,
        );
        assert!(remove_journal(&path).is_err());
        assert!(!path.exists());
        remove_journal(&path).expect("replay re-syncs the absent journal parent");
    }
}
