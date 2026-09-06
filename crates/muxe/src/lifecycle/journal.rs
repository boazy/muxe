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
    fs,
    io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

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

/// Hashes an identity string into the 32-hex-character journal key.
#[must_use]
pub fn unit_hash(identity: &str) -> String {
    fsutil::sha256_hex(identity.as_bytes())[..32].to_owned()
}

/// Per-member transition state inside a unit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberTransition {
    Prepared,
    Ready,
    Committed,
    Aborted,
}

/// One group member: a prepared old broker and its replacement target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MemberState {
    /// Human-scope identity (Herdr discovery key or Zellij session name).
    pub host_identity: String,
    /// Control socket of the prepared old broker.
    pub old_socket: PathBuf,
    /// Control socket of the started target broker (once spawned).
    pub target_socket: Option<PathBuf>,
    /// 128-bit handoff ID as lowercase hex; None until prepare assigns one.
    /// Recovery adopts observed handoffs into undecided members, never
    /// placeholders: None means undecided, not zero.
    pub handoff_id: Option<String>,
    /// Current transition state.
    pub state: MemberTransition,
}

/// Journal lifecycle state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalState {
    /// Written before the first external mutation; handoffs still unknown.
    Announced,
    /// Old brokers drained and prepared; targets may be starting.
    Prepared,
    /// Every target broker (and every required bridge registration for a
    /// Zellij group) reports ready; commit may proceed.
    Ready,
}

/// The durable activation journal for one unit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivationJournal {
    pub schema_version: u32,
    pub unit: UnitKind,
    pub old_record: CompatibilityRecord,
    pub target_record: CompatibilityRecord,
    /// Digest of the bridge being replaced (Zellij units only).
    pub old_bridge_digest: Option<String>,
    /// Digest of the staged replacement bridge (Zellij units only).
    pub staged_bridge_digest: Option<String>,
    /// Byte-for-byte backup of the old bridge (Zellij units only).
    pub backup_path: Option<PathBuf>,
    pub members: Vec<MemberState>,
    pub state: JournalState,
    /// Unix epoch seconds after which recovery requires operator diagnosis.
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
        Self {
            schema_version: ACTIVATION_JOURNAL_SCHEMA_VERSION,
            unit,
            old_record,
            target_record,
            old_bridge_digest: None,
            staged_bridge_digest: None,
            backup_path: None,
            members,
            state: JournalState::Prepared,
            recovery_deadline: unix_now() + RECOVERY_DEADLINE_SECS,
        }
    }

    /// Validates internal consistency: schema version, non-empty membership,
    /// well-formed handoff IDs, and digest shapes.
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
            if member.host_identity.is_empty()
                || member.host_identity.chars().any(char::is_control)
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
pub fn write_journal(cache_dir: &Path, journal: &ActivationJournal) -> Result<PathBuf, JournalError> {
    journal.validate()?;
    let directory = activation_dir(cache_dir);
    fsutil::ensure_owner_dir(&directory)?;
    let path = directory.join(journal.unit.journal_name());
    let bytes =
        serde_json::to_vec_pretty(journal).map_err(|source| JournalError::Corrupt {
            path: path.clone(),
            source,
        })?;
    fsutil::write_atomic(&path, &bytes, "activation")?;
    Ok(path)
}

/// Reads and validates a journal. Unrecognized states fail closed.
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

/// Lists every journal file present. Corrupt or unrecognized journals are
/// returned as paths with a `None` record so recovery preserves them instead
/// of deleting what it cannot understand.
pub fn list_journals(
    cache_dir: &Path,
) -> Result<Vec<(PathBuf, Result<ActivationJournal, JournalError>)>, JournalError> {
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
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
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
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
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
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let directory = activation_dir(temp.path());
        fsutil::ensure_owner_dir(&directory).unwrap();
        fsutil::write_atomic(&directory.join("herdr-x.json"), b"{corrupt", "activation").unwrap();
        let listed = list_journals(temp.path()).unwrap();
        assert_eq!(listed.len(), 1);
        assert!(listed[0].1.is_err());
        assert!(listed[0].0.exists());
    }
}
