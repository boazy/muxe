//! Owner-only activation journals and crash recovery records.
//!
//! Before the first external mutation, the coordinator durably writes one
//! journal per activation unit: `$CACHE_DIR/activation/herdr-{host-hash}.json`
//! for a Herdr broker, `$CACHE_DIR/activation/zellij-{bridge-path-hash}.json`
//! for a Zellij bridge-sharing group. Each journal records handoff IDs, host
//! identities, old and target compatibility records, old and staged bridge
//! digests, the backup path, per-member transition state, and the recovery
//! deadline. Journals never contain configuration values, environment values,
//! or action payloads.
//!
//! Every journal transition and external mutation is idempotent, so the
//! coordinator, old brokers, target brokers, and the next Muxe invocation can
//! all resume recovery. Inconsistent host identities, handoff IDs, digests,
//! membership, or unrecognized journal states fail closed and preserve the
//! journal, staging file, and backup for diagnosis.

use std::{
    fs, io,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use nix::fcntl::{Flock, FlockArg};

use muxe_protocol::control::CompatibilityRecord;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fsutil::{self, FsError};

/// Activation journal directory name under `$CACHE_DIR`.
pub const ACTIVATION_DIR_NAME: &str = "activation";
/// Activation journal schema version.
pub const ACTIVATION_JOURNAL_SCHEMA_VERSION: u32 = 1;
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
    #[error("cache lifetime is active at {path}")]
    CacheActive { path: PathBuf },
}

/// One activation unit: a single Herdr broker, or all live Zellij brokers
/// sharing one canonical stable WASM path as a single atomic group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "unit", rename_all = "snake_case")]
pub enum UnitKind {
    Herdr { host_hash: String },
    Zellij { bridge_path_hash: String },
}

impl UnitKind {
    /// Returns the journal file name for this unit.
    #[must_use]
    pub fn journal_name(&self) -> String {
        match self {
            Self::Herdr { host_hash } => format!("herdr-{host_hash}.json"),
            Self::Zellij { bridge_path_hash } => format!("zellij-{bridge_path_hash}.json"),
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
    let digest = fsutil::sha256_hex(identity.to_string_lossy().as_bytes());
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
    let file = Flock::lock(file, mode).map_err(|(_, error)| match mode {
        FlockArg::LockExclusiveNonblock | FlockArg::LockExclusive => {
            JournalError::CacheActive { path: path.clone() }
        }
        _ => JournalError::Inconsistent(format!(
            "cannot acquire cache lifetime lock at {}: {error}",
            path.display()
        )),
    })?;
    Ok(CacheLease { _file: file })
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

/// Hashes an identity string into the 32-hex-character journal key.
#[must_use]
pub fn unit_hash(identity: &str) -> String {
    fsutil::sha256_hex(identity.as_bytes())[..32].to_owned()
}

/// Per-member old-broker transition state inside a unit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberTransition {
    Prepared,
    Ready,
    Committed,
    Aborted,
    Resumed,
}

/// Per-target transition state. Target retirement is never represented by the
/// old-broker state, because one member has two distinct lifecycle participants.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetTransition {
    #[default]
    Pending,
    Retired,
    Committed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TargetMemberState {
    pub host_identity: String,
    pub handoff_id: String,
    pub state: TargetTransition,
}

/// Durable unit-level recovery decision.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPhase {
    #[default]
    Pending,
    RestoringOld,
    TargetOwns,
    Committed,
}

/// One group member: a prepared old broker and its replacement target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemberState {
    pub host_identity: String,
    pub old_socket: PathBuf,
    pub target_socket: Option<PathBuf>,
    pub handoff_id: Option<String>,
    pub state: MemberTransition,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalState {
    Announced,
    Prepared,
    Ready,
}

/// Durable activation journal for one unit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivationJournal {
    pub schema_version: u32,
    pub unit: UnitKind,
    pub old_record: CompatibilityRecord,
    pub target_record: CompatibilityRecord,
    #[serde(default)]
    pub bridge_restored: bool,
    pub old_bridge_digest: Option<String>,
    pub staged_bridge_digest: Option<String>,
    pub backup_path: Option<PathBuf>,
    pub members: Vec<MemberState>,
    #[serde(default)]
    pub target_members: Vec<TargetMemberState>,
    #[serde(default)]
    pub recovery: RecoveryPhase,
    pub state: JournalState,
    pub recovery_deadline: u64,
}

impl ActivationJournal {
    /// Builds a journal stamped with a fresh recovery deadline.
    #[must_use]
    pub fn new(
        unit: UnitKind,
        old_record: CompatibilityRecord,
        target_record: CompatibilityRecord,
        members: Vec<MemberState>,
    ) -> Self {
        let target_members = members
            .iter()
            .filter_map(|member| {
                Some(TargetMemberState {
                    host_identity: member.host_identity.clone(),
                    handoff_id: member.handoff_id.clone()?,
                    state: TargetTransition::Pending,
                })
            })
            .collect();
        Self {
            schema_version: ACTIVATION_JOURNAL_SCHEMA_VERSION,
            unit,
            old_record,
            target_record,
            bridge_restored: false,
            old_bridge_digest: None,
            staged_bridge_digest: None,
            backup_path: None,
            members,
            target_members,
            recovery: RecoveryPhase::Pending,
            state: JournalState::Prepared,
            recovery_deadline: unix_now() + RECOVERY_DEADLINE_SECS,
        }
    }
    /// Rebuilds role-separated target progress from current member handoffs,
    /// preserving already durable target acknowledgements.
    pub fn refresh_target_members(&mut self) {
        let prior = self
            .target_members
            .iter()
            .map(|target| (target.handoff_id.clone(), target.state))
            .collect::<std::collections::HashMap<_, _>>();
        self.target_members = self
            .members
            .iter()
            .filter_map(|member| {
                let handoff = member.handoff_id.clone()?;
                Some(TargetMemberState {
                    host_identity: member.host_identity.clone(),
                    state: prior.get(&handoff).copied().unwrap_or_default(),
                    handoff_id: handoff,
                })
            })
            .collect();
    }

    /// Validates internal consistency: schema version, non-empty membership,
    /// well-formed handoff IDs, and digest shapes.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] when the schema version, membership, handoff IDs,
    /// or bridge digests are inconsistent.
    pub fn validate(&self) -> Result<(), JournalError> {
        if self.schema_version != ACTIVATION_JOURNAL_SCHEMA_VERSION {
            return Err(JournalError::UnsupportedVersion {
                path: PathBuf::from("<memory>"),
                version: self.schema_version,
            });
        }
        if self.members.is_empty() {
            return Err(JournalError::Inconsistent(
                "journal has no members".to_owned(),
            ));
        }
        for member in &self.members {
            if member.host_identity.is_empty() || member.host_identity.chars().any(char::is_control)
            {
                return Err(JournalError::Inconsistent(
                    "journal has an invalid host identity".to_owned(),
                ));
            }
            match member.handoff_id.as_ref() {
                None if self.state == JournalState::Announced => {}
                None => {
                    return Err(JournalError::Inconsistent(
                        "journal member lacks a handoff ID after announcement".to_owned(),
                    ));
                }
                Some(handoff)
                    if handoff.len() == 32
                        && handoff.bytes().all(|byte| byte.is_ascii_hexdigit()) => {}
                Some(_) => {
                    return Err(JournalError::Inconsistent(
                        "journal has a malformed handoff ID".to_owned(),
                    ));
                }
            }
        }
        if self.state != JournalState::Announced {
            if self.target_members.len() != self.members.len() {
                return Err(JournalError::Inconsistent(
                    "journal target acknowledgement membership is incomplete".to_owned(),
                ));
            }
            let mut seen = std::collections::HashSet::new();
            for target in &self.target_members {
                if !seen.insert(target.handoff_id.clone())
                    || !self.members.iter().any(|member| {
                        member.host_identity == target.host_identity
                            && member.handoff_id.as_deref() == Some(target.handoff_id.as_str())
                    })
                {
                    return Err(JournalError::Inconsistent(
                        "journal target acknowledgement membership is not bijective".to_owned(),
                    ));
                }
            }
        }
        for target in &self.target_members {
            if target.host_identity.is_empty()
                || target.handoff_id.len() != 32
                || !target
                    .handoff_id
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(JournalError::Inconsistent(
                    "journal has malformed target acknowledgement".to_owned(),
                ));
            }
        }
        for digest in self
            .old_bridge_digest
            .iter()
            .chain(self.staged_bridge_digest.iter())
        {
            if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(JournalError::Inconsistent(
                    "journal has a malformed bridge digest".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// Returns the activation directory for a cache directory.
#[must_use]
pub fn activation_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join(ACTIVATION_DIR_NAME)
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
    let journal: ActivationJournal =
        serde_json::from_slice(&bytes).map_err(|source| JournalError::Corrupt {
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
    match fs::remove_file(path) {
        Ok(()) => {
            fsutil::sync_dir_of(path)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(JournalError::Fs(fsutil::io_error(
            "removing activation journal",
            path,
            source,
        ))),
    }
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
        ActivationJournal::new(
            UnitKind::Herdr {
                host_hash: unit_hash("server"),
            },
            fixture_record("0.1.0"),
            fixture_record("0.2.0"),
            vec![MemberState {
                host_identity: "server".to_owned(),
                old_socket: PathBuf::from("/tmp/old.sock"),
                target_socket: None,
                handoff_id: Some("ab".repeat(16)),
                state: MemberTransition::Prepared,
            }],
        )
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
    fn malformed_handoff_fails_closed() {
        let mut journal = fixture_journal();
        journal.members[0].handoff_id = Some("not-a-handoff".to_owned());
        assert!(matches!(
            journal.validate(),
            Err(JournalError::Inconsistent(_))
        ));
    }

    #[test]
    fn undecided_handoff_only_validates_while_announced() {
        let mut journal = fixture_journal();
        journal.members[0].handoff_id = None;
        assert!(matches!(
            journal.validate(),
            Err(JournalError::Inconsistent(_))
        ));
        journal.state = JournalState::Announced;
        journal.validate().unwrap();
    }

    #[test]
    fn empty_membership_fails_closed() {
        let mut journal = fixture_journal();
        journal.members.clear();
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
}
