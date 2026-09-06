//! Receipt-owned Zellij bridge installation and uninstallation.
//!
//! `muxe integration install zellij` materializes the packaged bridge at the
//! canonical stable path and optionally edits the Zellij KDL configuration,
//! all recorded in the owner-only integration receipt. A destination matching
//! the receipt digest is eligible for replacement; unrecognized bytes or a
//! receipt mismatch are never overwritten.
//!
//! Every installation writes an owner-only transaction journal before changing
//! the bridge, KDL document, or receipt. A retry completes or rolls back the
//! interrupted transaction before starting another; the receipt commits only
//! after the bridge and accepted KDL edits are durable.

pub mod bridge;
pub mod kdl;
pub mod receipt;

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use ::kdl::KdlDocument;
use thiserror::Error;

use crate::{
    cli::ConfigurationPolicy,
    compatibility,
    fsutil::{self, FsError},
    logging::Logger,
};

pub use bridge::{BRIDGE_FILE_NAME, PREVIOUS_SUFFIX};
pub use kdl::{LOAD_PLUGINS_NODE, MUXE_NODE, PLUGINS_NODE};
pub use receipt::{Disposition, ManagedNode, NodeRecord, Receipt};

/// Integration directory name under `$CONFIG_DIR/integrations/`.
pub const ZELLIJ_INTEGRATION_NAME: &str = "zellij";
/// Install transaction journal file name.
pub const INSTALL_JOURNAL_FILE_NAME: &str = ".install-journal.json";
/// Install journal schema version.
pub const INSTALL_JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum IntegrationError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(transparent)]
    Receipt(#[from] receipt::ReceiptError),
    #[error(transparent)]
    Bridge(#[from] bridge::BridgeError),
    #[error(transparent)]
    Asset(#[from] compatibility::AssetVerificationError),
    #[error("cannot resolve Zellij configuration: {0}")]
    ConfigDiscovery(String),
    #[error(
        "activation journal for this bridge is still live; resolve activation before uninstalling"
    )]
    ActivationJournalLive { journal: PathBuf },
    #[error("fault injected after {step:?} (test hook)")]
    FaultInjected { step: InstallStep },
    #[error("interrupted install journal is inconsistent: {0}")]
    InconsistentJournal(String),
    #[error("compatibility record unavailable: {0}")]
    Compat(String),
    #[error("auditable operation cannot proceed without its log record")]
    Audit(#[from] crate::logging::LogError),
}

/// Install transaction boundaries for failure injection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallStep {
    JournalWritten,
    BridgeStaged,
    BeforeKdlApply,
    ConfigEdited,
    BeforeBridgeSwap,
    BridgeCommitted,
    BeforeReceiptCommit,
}

/// Test and recovery hooks. Production passes `Hooks::default()`.
#[derive(Clone, Debug, Default)]
pub struct Hooks {
    /// When set, the install fails with `FaultInjected` right after the step.
    pub fail_after: Option<InstallStep>,
}

impl Hooks {
    fn check(&self, step: InstallStep) -> Result<(), IntegrationError> {
        if self.fail_after == Some(step) {
            return Err(IntegrationError::FaultInjected { step });
        }
        Ok(())
    }
}

/// Configuration policy after resolving flags and interactivity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolvedPolicy {
    Always,
    Never,
    Ask,
}

/// Resolves the KDL configuration policy from the invocation shape.
///
/// An explicit flag always wins (quiet included). Without one, quiet and
/// non-interactive invocations never edit; only an interactive terminal
/// without a policy flag prompts. Absence of a prompt is never consent.
#[must_use]
pub const fn resolve_policy(
    explicit: Option<ConfigurationPolicy>,
    quiet: bool,
    interactive: bool,
) -> ResolvedPolicy {
    match explicit {
        Some(ConfigurationPolicy::Always) => ResolvedPolicy::Always,
        Some(ConfigurationPolicy::Never) => ResolvedPolicy::Never,
        None if quiet || !interactive => ResolvedPolicy::Never,
        None => ResolvedPolicy::Ask,
    }
}

/// Inputs for `muxe integration install zellij`.
pub struct InstallInputs<'a> {
    /// Validated absolute Muxe configuration directory.
    pub config_dir: &'a Path,
    /// Packaged `lib/muxe/muxe-zellij.wasm` bytes.
    pub packaged_wasm: &'a [u8],
    /// Muxe version being installed.
    pub version: &'a str,
    /// Explicit `--zellij-config` override, if any.
    pub zellij_config: Option<PathBuf>,
    /// Explicit `--always-configure` / `--never-configure` flag, if any.
    pub explicit_policy: Option<ConfigurationPolicy>,
    /// Whether `-q/--quiet` was passed.
    pub quiet: bool,
    /// Whether standard input is an interactive terminal.
    pub interactive: bool,
    /// Prompt callback used only under the `Ask` policy. Returns true to edit.
    pub asker: Option<&'a dyn Fn(&str) -> bool>,
    /// Persistent logger, if the sink initialized.
    pub logger: Option<&'a Logger>,
    /// Failure-injection hooks.
    pub hooks: Hooks,
}

/// Outcome of a bridge installation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallOutcome {
    pub bridge_digest: String,
    pub receipt_path: PathBuf,
    pub config_path: Option<PathBuf>,
    pub config_edited: bool,
    pub created_config: bool,
    pub node_dispositions: Vec<(ManagedNode, Disposition)>,
    /// Set when the KDL edit was skipped or aborted; the bridge is installed.
    pub manual_snippet: Option<String>,
    /// Set when a previous interrupted transaction was resumed first.
    pub resumed: Option<ResumeOutcome>,
}

/// Outcome of resuming an interrupted install transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResumeOutcome {
    /// The interrupted transaction committed; carries its install outcome.
    Completed(Box<InstallOutcome>),
    /// Staged artifacts were discarded; nothing was committed.
    RolledBack,
}

/// Inputs for `muxe integration uninstall zellij`.
pub struct UninstallInputs<'a> {
    /// Validated absolute Muxe configuration directory.
    pub config_dir: &'a Path,
    /// Validated absolute Muxe cache directory (activation journal scan).
    pub cache_dir: &'a Path,
    /// Explicit `--zellij-config` override, if any.
    pub zellij_config: Option<PathBuf>,
    pub explicit_policy: Option<ConfigurationPolicy>,
    pub quiet: bool,
    pub interactive: bool,
    pub asker: Option<&'a dyn Fn(&str) -> bool>,
    pub logger: Option<&'a Logger>,
}

/// One managed record left untouched by uninstall.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedRecord {
    pub node: Option<ManagedNode>,
    pub config_path: Option<PathBuf>,
    pub reason: String,
}

/// Outcome of a bridge uninstallation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UninstallOutcome {
    pub bridge_removed: bool,
    pub previous_removed: bool,
    pub staging_removed: usize,
    pub restored_nodes: Vec<ManagedNode>,
    pub removed_nodes: Vec<ManagedNode>,
    pub unresolved: Vec<UnresolvedRecord>,
    pub receipt_removed: bool,
}

/// Returns the integration directory for Zellij.
#[must_use]
pub fn integration_dir(config_dir: &Path) -> PathBuf {
    config_dir
        .join("integrations")
        .join(ZELLIJ_INTEGRATION_NAME)
}

/// Returns the canonical stable bridge path.
#[must_use]
pub fn stable_bridge_path(config_dir: &Path) -> PathBuf {
    integration_dir(config_dir).join(BRIDGE_FILE_NAME)
}

/// Resolves which Zellij configuration to inspect or edit.
///
/// Delegates to the single native [`crate::paths`] resolver: an explicit
/// override wins, otherwise `$ZELLIJ_CONFIG_DIR` and the standard Zellij
/// configuration path apply. Never falls back to a relative directory.
///
/// # Errors
///
/// Returns `IntegrationError::ConfigDiscovery` when neither `XDG_CONFIG_HOME`
/// nor `HOME` is set and no explicit override was passed.
pub fn zellij_config_path(override_path: Option<&Path>) -> Result<PathBuf, IntegrationError> {
    crate::paths::zellij_config_path(override_path).map_err(|_| {
        IntegrationError::ConfigDiscovery(
            "neither XDG_CONFIG_HOME nor HOME is set; pass --zellij-config explicitly".to_owned(),
        )
    })
}

/// Installs the Zellij bridge transactionally.
///
/// A pending install journal is resumed first: a completed resume is returned
/// directly, a rolled-back one proceeds with the fresh install.
///
/// # Errors
///
/// Returns an error when the packaged asset fails verification, the journal,
/// bridge, configuration, or receipt cannot be read or written, or a test
/// hook injects a fault.
#[expect(
    clippy::needless_pass_by_value,
    reason = "public install shape: by-value inputs match public uninstall and keep CLI call-site moves simple; the transaction only borrows"
)]
pub fn install(inputs: InstallInputs<'_>) -> Result<InstallOutcome, IntegrationError> {
    let verification = compatibility::verify_packaged_asset(inputs.packaged_wasm)?;
    install_verified(&inputs, verification)
}

fn install_verified(
    inputs: &InstallInputs<'_>,
    verification: compatibility::NativeAssetVerification,
) -> Result<InstallOutcome, IntegrationError> {
    let directory = integration_dir(inputs.config_dir);
    if journal_path(&directory).exists() {
        let resumed = resume_with(&directory, &inputs.hooks, inputs.logger)?;
        match resumed {
            ResumeOutcome::Completed(outcome) => return Ok(*outcome),
            ResumeOutcome::RolledBack => {}
        }
        let mut outcome = install_fresh(inputs, &directory, verification)?;
        outcome.resumed = Some(ResumeOutcome::RolledBack);
        return Ok(outcome);
    }
    install_fresh(inputs, &directory, verification)
}

#[expect(
    clippy::too_many_lines,
    reason = "the install transaction is intentionally linear: journal, stage, KDL edit, swap, receipt. Splitting it would scatter the ordering guarantees."
)]
fn install_fresh(
    inputs: &InstallInputs<'_>,
    directory: &Path,
    verification: compatibility::NativeAssetVerification,
) -> Result<InstallOutcome, IntegrationError> {
    let stable = directory.join(BRIDGE_FILE_NAME);
    let receipt = receipt::load(directory)?;
    let receipt_digest = receipt
        .as_ref()
        .map(|receipt| receipt.bridge.installed_digest.as_str());
    let (eligibility, _) = bridge::check_destination(&stable, receipt_digest)?;

    // Journal before the first external mutation. The prior receipt identity
    // is recorded now so recovery can detect an out-of-band receipt change
    // before mutating.
    let mut journal = InstallJournal {
        schema_version: INSTALL_JOURNAL_SCHEMA_VERSION,
        phase: InstallPhase::Started,
        version: inputs.version.to_owned(),
        packaged_digest: verification.packaged_digest.clone(),
        prior_digest: receipt
            .as_ref()
            .map(|receipt| receipt.bridge.installed_digest.clone()),
        staged_name: None,
        config_path: None,
        bridge_url: String::new(),
        create_config: false,
        apply_config: false,
        pending: Vec::new(),
    };
    write_journal(directory, &journal)?;
    inputs.hooks.check(InstallStep::JournalWritten)?;

    let staged = bridge::stage(&stable, inputs.packaged_wasm)?;
    journal.staged_name = Some(staged_name(&staged)?);
    journal.phase = InstallPhase::Staged;
    write_journal(directory, &journal)?;
    inputs.hooks.check(InstallStep::BridgeStaged)?;
    // Configuration edit under the consent policy. The accepted plan --
    // node dispositions, previous bytes, consent -- is journaled BEFORE the
    // first KDL byte changes, so recovery preserves the original provenance
    // instead of recomputing it from an already-applied document.
    let bridge_url = kdl::bridge_url(&stable);
    journal.bridge_url.clone_from(&bridge_url);
    let policy = resolve_policy(inputs.explicit_policy, inputs.quiet, inputs.interactive);
    let config_path = zellij_config_path(inputs.zellij_config.as_deref())?;
    journal.config_path = Some(config_path.clone());
    let (planned, decision, plan_snippet) =
        plan_config(&config_path, &bridge_url, policy, inputs.asker)?;
    let mut manual_snippet = plan_snippet;
    if decision == EditDecision::Apply
        && let Some(planned) = planned
    {
        journal.pending = kdl::plan_records(&config_path, &planned.plan.nodes);
        journal.create_config = !planned.existed;
        journal.apply_config = true;
        write_journal(directory, &journal)?;
        inputs.hooks.check(InstallStep::BeforeKdlApply)?;
        match kdl::commit_planned(&config_path, &bridge_url, &planned) {
            Ok(result) => {
                journal.create_config = matches!(result, kdl::ConfigApplied::CreatedMinimal);
            }
            Err(error) => {
                // The configuration edit aborts and prints the required
                // snippet; the staged bridge is discarded and the
                // install continues without KDL ownership.
                journal.pending.clear();
                journal.apply_config = false;
                manual_snippet = Some(kdl_abort_snippet(&config_path, &bridge_url, error)?);
            }
        }
    }
    journal.phase = InstallPhase::ConfigEdited;
    write_journal(directory, &journal)?;
    inputs.hooks.check(InstallStep::ConfigEdited)?;

    // Commit the bridge, preserving the eligible old bytes first. The stable
    // bytes stay in place until the atomic swap; the existing rollback copy
    // rotates only under the prior receipt's authority.
    inputs.hooks.check(InstallStep::BeforeBridgeSwap)?;
    let authority = receipt
        .as_ref()
        .and_then(|receipt| receipt.bridge.previous_digest.as_deref());
    let (expected_current, previous_digest) = match eligibility {
        bridge::Eligibility::Absent => (None, None),
        bridge::Eligibility::EligibleReplace { current_digest } => {
            let record = bridge::ensure_backup(&stable, authority)?;
            debug_assert_eq!(record.digest, current_digest);
            (Some(current_digest), Some(record.digest))
        }
    };
    bridge::commit(&staged, &stable, expected_current.as_deref())?;
    journal.phase = InstallPhase::BridgeCommitted;
    write_journal(directory, &journal)?;
    inputs.hooks.check(InstallStep::BridgeCommitted)?;

    // Commit the receipt last, preserving untouched ownership records.
    let merged = merge_records(receipt.as_ref(), &config_path, &journal.pending);
    let bridge_compat = compatibility::embedded_record()
        .map_err(|error| IntegrationError::Compat(error.to_string()))?
        .handoff
        .zellij;
    if bridge_compat.is_none() {
        return Err(IntegrationError::Compat(
            "embedded record lacks Zellij compatibility".to_owned(),
        ));
    }
    inputs.hooks.check(InstallStep::BeforeReceiptCommit)?;
    let next = Receipt {
        schema_version: receipt::RECEIPT_SCHEMA_VERSION,
        bridge: receipt::BridgeRecord {
            canonical_path: stable,
            installed_version: inputs.version.to_owned(),
            installed_digest: verification.packaged_digest.clone(),
            previous_digest: previous_digest.or_else(|| {
                receipt
                    .as_ref()
                    .and_then(|receipt| receipt.bridge.previous_digest.clone())
            }),
            bridge_compat,
        },
        configs: merged,
    };
    receipt::store(directory, &next)?;

    remove_journal(directory)?;
    log(
        inputs.logger,
        "install",
        &format!("installed bridge {}", verification.packaged_digest),
    )?;
    Ok(InstallOutcome {
        bridge_digest: verification.packaged_digest,
        receipt_path: directory.join(receipt::RECEIPT_FILE_NAME),
        config_path: journal.apply_config.then(|| config_path.clone()),
        config_edited: journal.apply_config
            && journal
                .pending
                .iter()
                .any(|record| record.disposition != Disposition::Observed),
        created_config: journal.create_config,
        node_dispositions: journal
            .pending
            .iter()
            .map(|record| (record.node, record.disposition))
            .collect(),
        manual_snippet,
        resumed: None,
    })
}
/// Whether the configuration edit was accepted under the consent policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EditDecision {
    Apply,
    Skip,
}

/// Reads the configuration and plans both managed nodes without mutating.
///
/// Returns the plan with the consent decision and an optional manual snippet.
/// A malformed or ambiguous document aborts only the configuration edit (the
/// snippet path): the install still materializes the bridge. The caller
/// journals the accepted plan before committing it.
fn plan_config(
    config_path: &Path,
    bridge_url: &str,
    policy: ResolvedPolicy,
    asker: Option<&dyn Fn(&str) -> bool>,
) -> Result<(Option<kdl::PlannedFile>, EditDecision, Option<String>), IntegrationError> {
    let decision = match policy {
        ResolvedPolicy::Never => EditDecision::Skip,
        ResolvedPolicy::Always => EditDecision::Apply,
        ResolvedPolicy::Ask => {
            let prompt = format!(
                "Add or correct the Muxe nodes in {}?\n{}",
                config_path.display(),
                kdl::required_snippet(bridge_url)
            );
            if asker.is_some_and(|ask| ask(&prompt)) {
                EditDecision::Apply
            } else {
                EditDecision::Skip
            }
        }
    };
    if decision == EditDecision::Skip {
        return Ok((None, decision, None));
    }
    match kdl::read_and_plan(config_path, bridge_url) {
        Ok(planned) => Ok((Some(planned), decision, None)),
        Err(
            error @ (kdl::KdlError::Unparseable { .. }
            | kdl::KdlError::Ambiguous { .. }
            | kdl::KdlError::ConcurrentChange { .. }
            | kdl::KdlError::CandidateRejected { .. }),
        ) => Ok((
            None,
            decision,
            Some(format!(
                "Could not edit {}: {error}\nAdd these nodes manually:\n{}",
                config_path.display(),
                kdl::required_snippet(bridge_url)
            )),
        )),
        Err(kdl::KdlError::Fs(source)) => Err(IntegrationError::Fs(source)),
    }
}

/// Renders the manual snippet after a refused KDL edit. Only called for
/// plannable failures; IO failures propagate as errors instead.
fn kdl_abort_snippet(
    config_path: &Path,
    bridge_url: &str,
    error: kdl::KdlError,
) -> Result<String, IntegrationError> {
    match error {
        kdl::KdlError::Unparseable { .. }
        | kdl::KdlError::Ambiguous { .. }
        | kdl::KdlError::ConcurrentChange { .. }
        | kdl::KdlError::CandidateRejected { .. } => Ok(format!(
            "Could not edit {error} in {}.\nAdd these nodes manually:\n{}",
            config_path.display(),
            kdl::required_snippet(bridge_url)
        )),
        kdl::KdlError::Fs(source) => Err(IntegrationError::Fs(source)),
    }
}

fn merge_records(
    previous: Option<&Receipt>,
    config_path: &Path,
    pending: &[NodeRecord],
) -> Vec<NodeRecord> {
    let mut merged: Vec<NodeRecord> = previous
        .map(|receipt| {
            receipt
                .configs
                .iter()
                .filter(|record| record.config_path != config_path)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    merged.extend(pending.iter().cloned());
    merged
}

/// Records an auditable event. Messages carry identifiers, versions, and
/// digests only -- never configuration values, environment values, or action
/// payloads (the call sites below pass digests and state flags exclusively).
/// Failures propagate: operations that require an auditable failure path fail
/// closed instead of swallowing the sink error.
fn log(logger: Option<&Logger>, operation: &str, message: &str) -> Result<(), IntegrationError> {
    if let Some(logger) = logger {
        let event = crate::logging::LogEvent::new(
            logger.version().to_owned(),
            "zellij",
            operation,
            message,
        )?;
        logger.append(&event)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Install journal and crash recovery.
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum InstallPhase {
    Started,
    Staged,
    ConfigEdited,
    BridgeCommitted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InstallJournal {
    schema_version: u32,
    phase: InstallPhase,
    version: String,
    packaged_digest: String,
    /// Installed bridge digest of the receipt observed before installing.
    /// Recovery refuses to mutate when the live receipt no longer matches:
    /// an out-of-band receipt change is ambiguity, not authority.
    prior_digest: Option<String>,
    staged_name: Option<String>,
    config_path: Option<PathBuf>,
    bridge_url: String,
    create_config: bool,
    apply_config: bool,
    pending: Vec<NodeRecord>,
}

fn journal_path(directory: &Path) -> PathBuf {
    directory.join(INSTALL_JOURNAL_FILE_NAME)
}

fn write_journal(directory: &Path, journal: &InstallJournal) -> Result<(), IntegrationError> {
    let bytes = serde_json::to_vec_pretty(journal).map_err(|source| {
        IntegrationError::InconsistentJournal(format!("cannot encode install journal: {source}"))
    })?;
    fsutil::ensure_owner_dir(directory)?;
    fsutil::write_atomic(&journal_path(directory), &bytes, "install-journal")?;
    Ok(())
}

fn read_journal(directory: &Path) -> Result<InstallJournal, IntegrationError> {
    let path = journal_path(directory);
    let bytes = fsutil::read_owner_file(&path)?;
    let journal: InstallJournal = serde_json::from_slice(&bytes).map_err(|source| {
        IntegrationError::InconsistentJournal(format!(
            "interrupted install journal is corrupt: {source}"
        ))
    })?;
    if journal.schema_version != INSTALL_JOURNAL_SCHEMA_VERSION {
        return Err(IntegrationError::InconsistentJournal(format!(
            "unsupported install journal schema {}",
            journal.schema_version
        )));
    }
    Ok(journal)
}

fn remove_journal(directory: &Path) -> Result<(), IntegrationError> {
    let path = journal_path(directory);
    match fs::remove_file(&path) {
        Ok(()) => {
            fsutil::sync_dir_of(&path)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(IntegrationError::Fs(fsutil::io_error(
            "removing install journal",
            &path,
            source,
        ))),
    }
}

fn staged_name(staged: &Path) -> Result<String, IntegrationError> {
    staged
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            IntegrationError::InconsistentJournal("staged bridge has no file name".to_owned())
        })
}

/// Completes or rolls back an interrupted install transaction.
///
/// Recovery is idempotent and converges to either the fully installed state
/// or the untouched prior state; inconsistent journals fail closed with the
/// staging file, backup, and journal preserved for diagnosis.
///
/// # Errors
///
/// Returns an error when the journal cannot be read or validated, the prior
/// receipt or bridge state contradicts it, or any recovery write fails.
pub fn resume_install(
    config_dir: &Path,
    hooks: &Hooks,
    logger: Option<&Logger>,
) -> Result<Option<ResumeOutcome>, IntegrationError> {
    let directory = integration_dir(config_dir);
    if !journal_path(&directory).exists() {
        return Ok(None);
    }
    resume_with(&directory, hooks, logger).map(Some)
}

// Full resume needs no fresh packaged bytes: the staged file carries them.
fn resume_with(
    directory: &Path,
    hooks: &Hooks,
    logger: Option<&Logger>,
) -> Result<ResumeOutcome, IntegrationError> {
    let journal = read_journal(directory)?;
    let stable = directory.join(BRIDGE_FILE_NAME);
    match journal.phase {
        InstallPhase::Started => {
            // The journal predates staging. Adopt a matching orphan staging
            // file by digest, or clean up and roll back when none matches.
            if let Some(staged) = adopt_or_clean_staging(directory, &journal)? {
                let mut journal = journal;
                journal.staged_name = Some(staged_name(&staged)?);
                journal.phase = InstallPhase::Staged;
                write_journal(directory, &journal)?;
                resume_with_staged(directory, &journal, Some(&staged), hooks, logger)
            } else {
                verify_pre_swap_bridge_state(directory, &journal)?;
                remove_journal(directory)?;
                Ok(ResumeOutcome::RolledBack)
            }
        }
        InstallPhase::Staged | InstallPhase::ConfigEdited => {
            let staged_path = validated_staging(directory, &journal)?;
            let staged = if staged_path.exists() {
                let bytes = fs::read(&staged_path).map_err(|source| {
                    fsutil::io_error("reading staged bridge", &staged_path, source)
                })?;
                if fsutil::sha256_hex(&bytes) != journal.packaged_digest {
                    return Err(IntegrationError::InconsistentJournal(
                        "staged bridge digest does not match the journal".to_owned(),
                    ));
                }
                Some(staged_path)
            } else {
                None
            };
            resume_with_staged(directory, &journal, staged.as_deref(), hooks, logger)
        }
        InstallPhase::BridgeCommitted => {
            let staged_path = validated_staging(directory, &journal)?;
            let staged = staged_path.exists().then_some(staged_path);
            commit_resumed(
                directory,
                &stable,
                staged.as_deref(),
                &journal,
                None,
                hooks,
                logger,
            )
        }
    }
}

/// Validates the journaled staging name as a safe plain filename beneath the
/// integration directory. A journal carrying separators or parent references
/// never resolves to a mutation target: fail closed.
fn validated_staging(
    directory: &Path,
    journal: &InstallJournal,
) -> Result<PathBuf, IntegrationError> {
    let name = journal.staged_name.clone().ok_or_else(|| {
        IntegrationError::InconsistentJournal("journal lacks the staged file name".to_owned())
    })?;
    if name.is_empty()
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
        || Path::new(&name).file_name().and_then(|base| base.to_str()) != Some(name.as_str())
    {
        return Err(IntegrationError::InconsistentJournal(
            "journal staging name is not a plain filename".to_owned(),
        ));
    }
    Ok(directory.join(name))
}

/// Scans for orphan staging files from a crash before the staged name was
/// journaled. A candidate whose digest matches the journaled packaged digest
/// is adopted by deterministic identity. A mismatching candidate makes the
/// transaction inconsistent, so recovery preserves it for diagnosis.
fn adopt_or_clean_staging(
    directory: &Path,
    journal: &InstallJournal,
) -> Result<Option<PathBuf>, IntegrationError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(IntegrationError::Fs(fsutil::io_error(
                "scanning integration directory",
                directory,
                source,
            )));
        }
    };
    let mut adopted = None;
    for entry in entries {
        let entry = entry.map_err(|source| {
            fsutil::io_error("scanning integration directory", directory, source)
        })?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".tmp") || !name.contains(BRIDGE_FILE_NAME) {
            continue;
        }
        let path = entry.path();
        let bytes = fs::read(&path)
            .map_err(|source| fsutil::io_error("reading staging file", &path, source))?;
        if fsutil::sha256_hex(&bytes) == journal.packaged_digest {
            adopted = Some(path);
        } else {
            return Err(IntegrationError::InconsistentJournal(format!(
                "staging file {} does not match the journaled bridge digest",
                path.display()
            )));
        }
    }
    if adopted.is_some() {
        fsutil::sync_dir_of(directory)?;
    }
    Ok(adopted)
}

/// Continues recovery once staging presence is known.
fn resume_with_staged(
    directory: &Path,
    journal: &InstallJournal,
    staged: Option<&Path>,
    hooks: &Hooks,
    logger: Option<&Logger>,
) -> Result<ResumeOutcome, IntegrationError> {
    let stable = directory.join(BRIDGE_FILE_NAME);
    check_prior_receipt(directory, journal)?;
    let mut journal = journal.clone();
    // Converge the accepted KDL edit without recomputing provenance: the
    // journaled dispositions stand. Re-application only converges bytes.
    let mut manual_snippet = None;
    if journal.apply_config {
        let config_path = journal.config_path.clone().ok_or_else(|| {
            IntegrationError::InconsistentJournal("journal lacks the config path".to_owned())
        })?;
        match kdl::verify_records(&config_path, &journal.pending) {
            Ok(()) => {}
            Err(_) if journal.phase == InstallPhase::Staged => {
                // The edit never committed (atomic rename): apply fresh under
                // the recorded consent and adopt the resulting provenance,
                // which reflects the document as it actually is.
                match reapply_kdl(&config_path, &journal) {
                    Ok(records) => {
                        journal.pending = records;
                        journal.phase = InstallPhase::ConfigEdited;
                        write_journal(directory, &journal)?;
                    }
                    Err(Reapply::AbortSnippet(snippet)) => {
                        if staged.is_none() {
                            // No bridge bytes to install and no KDL claim to
                            // keep: nothing of ours exists, roll back.
                            remove_journal(directory)?;
                            return Ok(ResumeOutcome::RolledBack);
                        }
                        journal.pending.clear();
                        journal.apply_config = false;
                        manual_snippet = Some(snippet);
                    }
                    Err(Reapply::Fatal(error)) => return Err(error),
                }
            }
            Err(reason) => {
                // The journal claims an edit that the document contradicts.
                // With no staged bytes forward progress is impossible: restore
                // proven nodes when they verify, otherwise fail closed.
                if staged.is_none() {
                    return rollback_kdl_and_journal(directory, &journal, logger, &reason);
                }
                return Err(IntegrationError::InconsistentJournal(format!(
                    "journaled KDL nodes do not match the configuration: {reason}"
                )));
            }
        }
        // Re-verify after any fresh apply before the receipt may commit.
        if let Err(reason) = kdl::verify_records(&config_path, &journal.pending) {
            if staged.is_none() {
                return rollback_kdl_and_journal(directory, &journal, logger, &reason);
            }
            return Err(IntegrationError::InconsistentJournal(format!(
                "journaled KDL nodes do not match the configuration: {reason}"
            )));
        }
    }
    if staged.is_none() && journal.phase != InstallPhase::BridgeCommitted {
        // No bridge bytes exist to install. When the swap never happened the
        // only honest outcomes are restoring proven nodes or, when they no
        // longer verify, failing closed with artifacts preserved.
        return rollback_kdl_and_journal(
            directory,
            &journal,
            logger,
            "staged bridge missing; cannot complete swap",
        );
    }
    commit_resumed(
        directory,
        &stable,
        staged,
        &journal,
        manual_snippet,
        hooks,
        logger,
    )
}

/// Verifies the live receipt still matches the journal's recorded prior
/// identity, including the canonical bridge path. Any out-of-band receipt
/// change is ambiguity, never authority to mutate.
fn check_prior_receipt(
    directory: &Path,
    journal: &InstallJournal,
) -> Result<Option<Receipt>, IntegrationError> {
    let receipt = receipt::load(directory)?;
    let current = receipt
        .as_ref()
        .map(|receipt| receipt.bridge.installed_digest.clone());
    if current != journal.prior_digest {
        return Err(IntegrationError::InconsistentJournal(
            "receipt changed outside the transaction".to_owned(),
        ));
    }
    if let Some(receipt) = receipt.as_ref()
        && receipt.bridge.canonical_path != directory.join(BRIDGE_FILE_NAME)
    {
        return Err(IntegrationError::InconsistentJournal(
            "receipt canonical path mismatch".to_owned(),
        ));
    }
    Ok(receipt)
}

/// Verifies that the bridge still has exactly its pre-swap authority before
/// a recovery can discard a journal whose staged bytes are missing.
fn verify_pre_swap_bridge_state(
    directory: &Path,
    journal: &InstallJournal,
) -> Result<(), IntegrationError> {
    let receipt = check_prior_receipt(directory, journal)?;
    let stable = directory.join(BRIDGE_FILE_NAME);
    match (receipt, fs::read(&stable)) {
        (None, Err(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        (None, Ok(_)) => Err(IntegrationError::InconsistentJournal(
            "unreceipted stable bridge exists while staged bridge is missing".to_owned(),
        )),
        (Some(receipt), Ok(bytes))
            if fsutil::sha256_hex(&bytes) == receipt.bridge.installed_digest =>
        {
            Ok(())
        }
        (Some(_), Ok(_)) => Err(IntegrationError::InconsistentJournal(
            "stable bridge changed while staged bridge is missing".to_owned(),
        )),
        (Some(_), Err(error)) if error.kind() == io::ErrorKind::NotFound => {
            Err(IntegrationError::InconsistentJournal(
                "receipted stable bridge is missing while staged bridge is missing".to_owned(),
            ))
        }
        (_, Err(source)) => Err(IntegrationError::Fs(fsutil::io_error(
            "reading stable bridge",
            &stable,
            source,
        ))),
    }
}

/// Fresh KDL re-application during recovery, under recorded consent.
enum Reapply {
    /// The edit aborts with a manual snippet (mirrors the fresh flow).
    AbortSnippet(String),
    /// Recovery cannot continue honestly.
    Fatal(IntegrationError),
}

fn reapply_kdl(config_path: &Path, journal: &InstallJournal) -> Result<Vec<NodeRecord>, Reapply> {
    let planned =
        kdl::read_and_plan(config_path, &journal.bridge_url).map_err(|error| match error {
            kdl::KdlError::Fs(source) => Reapply::Fatal(IntegrationError::Fs(source)),
            other => Reapply::Fatal(IntegrationError::InconsistentJournal(format!(
                "cannot re-plan KDL edit: {other}"
            ))),
        })?;
    match kdl::commit_planned(config_path, &journal.bridge_url, &planned) {
        Ok(applied) => Ok(applied.node_records(config_path)),
        Err(
            error @ (kdl::KdlError::Unparseable { .. }
            | kdl::KdlError::Ambiguous { .. }
            | kdl::KdlError::ConcurrentChange { .. }
            | kdl::KdlError::CandidateRejected { .. }),
        ) => Err(Reapply::AbortSnippet(format!(
            "Could not edit {}: {error}\nAdd these nodes manually:\n{}",
            config_path.display(),
            kdl::required_snippet(&journal.bridge_url)
        ))),
        Err(kdl::KdlError::Fs(source)) => Err(Reapply::Fatal(IntegrationError::Fs(source))),
    }
}

/// Restores proven journaled nodes and removes the journal. Only called when
/// forward progress is impossible (no staged bytes) and the nodes verify;
/// anything else fails closed with artifacts preserved.
fn rollback_kdl_and_journal(
    directory: &Path,
    journal: &InstallJournal,
    logger: Option<&Logger>,
    context: &str,
) -> Result<ResumeOutcome, IntegrationError> {
    verify_pre_swap_bridge_state(directory, journal)?;
    if journal.apply_config {
        let config_path = journal.config_path.clone().ok_or_else(|| {
            IntegrationError::InconsistentJournal("journal lacks the config path".to_owned())
        })?;
        kdl::verify_records(&config_path, &journal.pending).map_err(|reason| {
            IntegrationError::InconsistentJournal(format!(
                "cannot restore KDL ({context}): {reason}"
            ))
        })?;
        rollback_pending_nodes(&config_path, &journal.pending).map_err(|reason| {
            IntegrationError::InconsistentJournal(format!(
                "cannot restore KDL ({context}): {reason}"
            ))
        })?;
    }
    remove_journal(directory)?;
    log(logger, "install-resume", &format!("rolled back: {context}"))?;
    Ok(ResumeOutcome::RolledBack)
}

/// Removes Created nodes and restores Updated ones from journaled provenance.
/// Every node must verify before any edit is planned; the combined candidate
/// is re-parsed and checked for concurrent change before the atomic write.
fn rollback_pending_nodes(config_path: &Path, pending: &[NodeRecord]) -> Result<(), String> {
    let bytes = std::fs::read(config_path)
        .map_err(|error| format!("cannot read {}: {error}", config_path.display()))?;
    let original = String::from_utf8(bytes)
        .map_err(|_| format!("{} is not valid UTF-8", config_path.display()))?;
    let document = KdlDocument::parse_v1(&original)
        .map_err(|error| format!("cannot parse {}: {error}", config_path.display()))?;
    let mut edits = Vec::new();
    for record in pending {
        if record.disposition == Disposition::Observed {
            continue;
        }
        if let Some(edit) = plan_uninstall_node(&document, &original, record)? {
            edits.push(edit);
        }
    }
    if edits.is_empty() {
        return Ok(());
    }
    let candidate = kdl::apply_edits(&original, &edits);
    KdlDocument::parse_v1(&candidate)
        .map_err(|error| format!("rollback candidate rejected: {error}"))?;
    let current = std::fs::read(config_path)
        .map_err(|error| format!("cannot re-read {}: {error}", config_path.display()))?;
    if current != original.as_bytes() {
        return Err("configuration changed concurrently".to_owned());
    }
    kdl::write_raw_config(config_path, &candidate)
        .map_err(|error| format!("cannot write rollback: {error}"))?;
    Ok(())
}
#[expect(
    clippy::too_many_lines,
    reason = "recovery re-validation is intentionally sequential: prior receipt, stable bytes, swap, receipt. Splitting it would hide the fail-closed ordering."
)]
fn commit_resumed(
    directory: &Path,
    stable: &Path,
    staged: Option<&Path>,
    journal: &InstallJournal,
    manual_snippet: Option<String>,
    hooks: &Hooks,
    logger: Option<&Logger>,
) -> Result<ResumeOutcome, IntegrationError> {
    let receipt = check_prior_receipt(directory, journal)?;
    // Mutation-time re-validation: the stable destination must still hold the
    // vouched old bytes (or be absent for a fresh install). Anything else
    // fails closed with artifacts preserved.
    let previous_digest = match fs::read(stable) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A missing stable bridge with a staged replacement and no prior
            // receipt is the normal fresh-install resume: proceed to the swap,
            // which re-validates absence at the actual mutation. A missing
            // stable bridge with a prior receipt means the old bytes are gone:
            // never silently proceed without them.
            if receipt.is_some() {
                return Err(IntegrationError::InconsistentJournal(
                    "stable bridge vanished outside the transaction".to_owned(),
                ));
            }
            if staged.is_none() {
                if journal.phase == InstallPhase::BridgeCommitted {
                    return Err(IntegrationError::InconsistentJournal(
                        "bridge-committed journal has no stable or staged bridge".to_owned(),
                    ));
                }
                remove_journal(directory)?;
                return Ok(ResumeOutcome::RolledBack);
            }
            None
        }
        Err(source) => {
            return Err(IntegrationError::Fs(fsutil::io_error(
                "reading installed bridge",
                stable,
                source,
            )));
        }
        Ok(current) => {
            let current_digest = fsutil::sha256_hex(&current);
            if current_digest == journal.packaged_digest {
                // A previous attempt already swapped; the staged file (if any)
                // is an orphan of the completed swap.
                if let Some(staged) = staged {
                    bridge::discard_staging(staged);
                }
                receipt
                    .as_ref()
                    .and_then(|receipt| receipt.bridge.previous_digest.clone())
            } else {
                match receipt.as_ref() {
                    Some(receipt) if receipt.bridge.installed_digest == current_digest => {
                        let authority = receipt.bridge.previous_digest.as_deref();
                        let record = bridge::ensure_backup(stable, authority)?;
                        Some(record.digest)
                    }
                    _ => {
                        return Err(IntegrationError::InconsistentJournal(
                            "stable bridge changed outside the transaction".to_owned(),
                        ));
                    }
                }
            }
        }
    };
    if let Some(staged) = staged {
        let expected = match fs::read(stable) {
            Ok(current) => Some(fsutil::sha256_hex(&current)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(IntegrationError::Fs(fsutil::io_error(
                    "reading installed bridge",
                    stable,
                    source,
                )));
            }
        };
        // The backup above guarantees the stable bytes are the vouched old
        // ones; commit re-validates them at the actual mutation.
        let vouched = receipt
            .as_ref()
            .map(|receipt| receipt.bridge.installed_digest.clone());
        if expected != vouched && receipt.is_some() {
            return Err(IntegrationError::InconsistentJournal(
                "stable bridge changed outside the transaction".to_owned(),
            ));
        }
        bridge::commit(staged, stable, expected.as_deref())?;
    }
    hooks.check(InstallStep::BridgeCommitted)?;
    let config_path = journal
        .config_path
        .clone()
        .unwrap_or_else(|| stable.to_path_buf());
    let merged = merge_records(receipt.as_ref(), &config_path, &journal.pending);
    let bridge_compat = compatibility::embedded_record()
        .map_err(|error| IntegrationError::Compat(error.to_string()))?
        .handoff
        .zellij;
    if bridge_compat.is_none() {
        return Err(IntegrationError::Compat(
            "embedded record lacks Zellij compatibility".to_owned(),
        ));
    }
    receipt::store(
        directory,
        &Receipt {
            schema_version: receipt::RECEIPT_SCHEMA_VERSION,
            bridge: receipt::BridgeRecord {
                canonical_path: stable.to_path_buf(),
                installed_version: journal.version.clone(),
                installed_digest: journal.packaged_digest.clone(),
                previous_digest: previous_digest.or_else(|| {
                    receipt
                        .as_ref()
                        .and_then(|receipt| receipt.bridge.previous_digest.clone())
                }),
                bridge_compat,
            },
            configs: merged,
        },
    )?;
    if let Some(staged) = staged {
        bridge::discard_staging(staged);
    }
    remove_journal(directory)?;
    log(
        logger,
        "install-resume",
        &format!("completed install {}", journal.packaged_digest),
    )?;
    let outcome = InstallOutcome {
        bridge_digest: journal.packaged_digest.clone(),
        receipt_path: directory.join(receipt::RECEIPT_FILE_NAME),
        config_path: journal.apply_config.then(|| config_path.clone()),
        config_edited: journal
            .pending
            .iter()
            .any(|record| record.disposition != Disposition::Observed),
        created_config: journal.create_config
            && journal
                .pending
                .iter()
                .any(|record| record.disposition == Disposition::Created),
        node_dispositions: journal
            .pending
            .iter()
            .map(|record| (record.node, record.disposition))
            .collect(),
        manual_snippet,
        resumed: None,
    };
    Ok(ResumeOutcome::Completed(Box::new(outcome)))
}

// ---------------------------------------------------------------------------
// Uninstallation.
// ---------------------------------------------------------------------------

/// Removes receipt-owned Zellij integration artifacts.
///
/// Nodes recorded as observed are never removed; created or updated nodes are
/// removed or restored only when their current semantic representation and
/// exact text digest still match the receipt. Root keybindings are user-owned
/// and never touched. The receipt is removed only after every managed artifact
/// is gone and no unresolved record remains.
///
/// # Errors
///
/// Returns an error when the receipt cannot be read, an activation journal
/// for this bridge is still live, or bridge, staging, or receipt cleanup fails.
/// KDL deviations are reported as unresolved records, not errors.
#[expect(
    clippy::needless_pass_by_value,
    reason = "public API takes `UninstallInputs` by value for call-site ergonomics; changing it would break external callers."
)]
pub fn uninstall(inputs: UninstallInputs<'_>) -> Result<UninstallOutcome, IntegrationError> {
    let directory = integration_dir(inputs.config_dir);
    let stable = directory.join(BRIDGE_FILE_NAME);
    let Some(receipt) = receipt::load(&directory)? else {
        return Ok(UninstallOutcome {
            bridge_removed: false,
            previous_removed: false,
            staging_removed: remove_staging_leftovers(&directory)?,
            restored_nodes: Vec::new(),
            removed_nodes: Vec::new(),
            unresolved: Vec::new(),
            receipt_removed: false,
        });
    };
    refuse_when_activation_live(inputs.cache_dir, &receipt.bridge.installed_digest)?;

    let mut outcome = UninstallOutcome {
        bridge_removed: false,
        previous_removed: false,
        staging_removed: 0,
        restored_nodes: Vec::new(),
        removed_nodes: Vec::new(),
        unresolved: Vec::new(),
        receipt_removed: false,
    };

    // KDL nodes first so a later bridge refusal never orphans ownership.
    let policy = resolve_policy(inputs.explicit_policy, inputs.quiet, inputs.interactive);
    let remove_configs = match policy {
        ResolvedPolicy::Never => false,
        ResolvedPolicy::Always => true,
        ResolvedPolicy::Ask => inputs
            .asker
            .is_some_and(|ask| ask("Remove or restore Muxe-owned Zellij KDL nodes?")),
    };
    if remove_configs {
        apply_uninstall_edits(&receipt, inputs.zellij_config.as_deref(), &mut outcome);
    } else {
        for record in &receipt.configs {
            if record.disposition != Disposition::Observed {
                outcome.unresolved.push(UnresolvedRecord {
                    node: Some(record.node),
                    config_path: Some(record.config_path.clone()),
                    reason: "configuration edit declined; node left in place".to_owned(),
                });
            }
        }
    }

    let previous = bridge::previous_path(&stable);
    match receipt.bridge.previous_digest.as_deref() {
        Some(expected) => {
            outcome.previous_removed = bridge::remove_if_matching(&previous, expected)?;
        }
        None if previous.exists() => {
            // A rollback copy exists that no receipt vouches for: never delete
            // unknown bytes, and keep the receipt until a human resolves it.
            outcome.unresolved.push(UnresolvedRecord {
                node: None,
                config_path: None,
                reason: format!(
                    "rollback copy {} has no recorded digest; left in place",
                    previous.display()
                ),
            });
        }
        None => {}
    }
    outcome.bridge_removed = bridge::remove_if_matching(&stable, &receipt.bridge.installed_digest)?;
    outcome.staging_removed = remove_staging_leftovers(&directory)?;

    if outcome.unresolved.is_empty() && !stable.exists() {
        receipt::remove(&directory)?;
        outcome.receipt_removed = true;
    }
    log(
        inputs.logger,
        "uninstall",
        &format!(
            "bridge_removed={} receipt_removed={}",
            outcome.bridge_removed, outcome.receipt_removed
        ),
    )?;
    Ok(outcome)
}

/// Refuses uninstallation while an activation journal references the bridge digest.
fn refuse_when_activation_live(
    cache_dir: &Path,
    installed_digest: &str,
) -> Result<(), IntegrationError> {
    let activation = cache_dir.join("activation");
    let entries = match fs::read_dir(&activation) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(IntegrationError::Fs(fsutil::io_error(
                "scanning activation journals",
                &activation,
                source,
            )));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| {
            fsutil::io_error("scanning activation journals", &activation, source)
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let bytes = fs::read(&path)
            .map_err(|source| fsutil::io_error("reading activation journal", &path, source))?;
        if bytes
            .windows(installed_digest.len())
            .any(|window| window == installed_digest.as_bytes())
        {
            return Err(IntegrationError::ActivationJournalLive { journal: path });
        }
    }
    Ok(())
}

fn remove_staging_leftovers(directory: &Path) -> Result<usize, IntegrationError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(source) => {
            return Err(IntegrationError::Fs(fsutil::io_error(
                "scanning integration directory",
                directory,
                source,
            )));
        }
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry.map_err(|source| {
            fsutil::io_error("scanning integration directory", directory, source)
        })?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".tmp") && name.contains(BRIDGE_FILE_NAME) {
            fs::remove_file(entry.path()).map_err(|source| {
                fsutil::io_error("removing staging file", &entry.path(), source)
            })?;
            removed += 1;
        }
    }
    if removed > 0 {
        fsutil::sync_dir_of(directory)?;
    }
    Ok(removed)
}

/// Applies owned-node removals and restorations grouped per configuration file.
fn apply_uninstall_edits(
    receipt: &Receipt,
    config_override: Option<&Path>,
    outcome: &mut UninstallOutcome,
) {
    use std::collections::BTreeMap;
    let mut by_file: BTreeMap<PathBuf, Vec<&NodeRecord>> = BTreeMap::new();
    for record in &receipt.configs {
        by_file
            .entry(record.config_path.clone())
            .or_default()
            .push(record);
    }
    for (config_path, records) in by_file {
        let target = config_override.map_or(config_path.clone(), PathBuf::from);
        match uninstall_one_file(&target, &records) {
            Ok(edits) => {
                outcome.removed_nodes.extend(edits.removed);
                outcome.restored_nodes.extend(edits.restored);
                outcome.unresolved.extend(edits.unresolved);
            }
            Err(error) => {
                for record in records {
                    outcome.unresolved.push(UnresolvedRecord {
                        node: Some(record.node),
                        config_path: Some(target.clone()),
                        reason: error.to_string(),
                    });
                }
            }
        }
    }
}

#[derive(Default)]
struct FileUninstall {
    removed: Vec<ManagedNode>,
    restored: Vec<ManagedNode>,
    unresolved: Vec<UnresolvedRecord>,
}

fn uninstall_one_file(
    config_path: &Path,
    records: &[&NodeRecord],
) -> Result<FileUninstall, kdl::KdlError> {
    let mut result = FileUninstall::default();
    let bytes = match fs::read(config_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            for record in records {
                result.unresolved.push(UnresolvedRecord {
                    node: Some(record.node),
                    config_path: Some(config_path.to_path_buf()),
                    reason: "configuration file no longer exists".to_owned(),
                });
            }
            return Ok(result);
        }
        Err(source) => {
            return Err(kdl::KdlError::Fs(fsutil::io_error(
                "reading Zellij configuration",
                config_path,
                source,
            )));
        }
    };
    let original = String::from_utf8(bytes.clone()).map_err(|_| kdl::KdlError::Unparseable {
        path: config_path.to_path_buf(),
        detail: "configuration is not valid UTF-8".to_owned(),
    })?;
    let document =
        KdlDocument::parse_v1(&original).map_err(|error| kdl::KdlError::Unparseable {
            path: config_path.to_path_buf(),
            detail: error.to_string(),
        })?;
    let mut edits = Vec::new();
    for record in records {
        if record.disposition == Disposition::Observed {
            continue;
        }
        match plan_uninstall_node(&document, &original, record) {
            Ok(Some(edit)) => {
                edits.push((record.node, record.disposition, edit));
            }
            Ok(None) => {}
            Err(reason) => result.unresolved.push(UnresolvedRecord {
                node: Some(record.node),
                config_path: Some(config_path.to_path_buf()),
                reason,
            }),
        }
    }
    if edits.is_empty() {
        return Ok(result);
    }
    let text_edits: Vec<kdl::TextEdit> = edits.iter().map(|(_, _, edit)| edit.clone()).collect();
    let candidate = kdl::apply_edits(&original, &text_edits);
    KdlDocument::parse_v1(&candidate).map_err(|error| kdl::KdlError::CandidateRejected {
        detail: error.to_string(),
    })?;
    let current = fs::read(config_path).map_err(|source| {
        fsutil::io_error("re-reading Zellij configuration", config_path, source)
    })?;
    if current != bytes {
        return Err(kdl::KdlError::ConcurrentChange {
            path: config_path.to_path_buf(),
        });
    }
    // Commit the already-validated candidate, preserving file permissions.
    kdl::write_raw_config(config_path, &candidate)?;
    for (node, disposition, _) in edits {
        match disposition {
            Disposition::Created => result.removed.push(node),
            Disposition::Updated => result.restored.push(node),
            Disposition::Observed => {}
        }
    }
    Ok(result)
}

/// Plans the removal or restoration of one owned node.
///
/// Returns `Ok(None)` when the node is already gone (idempotent). Any
/// semantic or exact-text deviation from the receipt leaves the node to the
/// user with a reason.
fn plan_uninstall_node(
    document: &KdlDocument,
    original: &str,
    record: &NodeRecord,
) -> Result<Option<kdl::TextEdit>, String> {
    // Locate the parent block and the managed child through the public KDL API.
    let parent_name = match record.node {
        ManagedNode::PluginsAlias => PLUGINS_NODE,
        ManagedNode::LoadPluginsEntry => LOAD_PLUGINS_NODE,
    };
    let blocks: Vec<_> = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == parent_name)
        .collect();
    if blocks.len() > 1 {
        return Err(format!(
            "ambiguous duplicate `{parent_name}` blocks; left untouched"
        ));
    }
    let Some(block) = blocks.first() else {
        // A created node whose parent block is gone is already gone too.
        // An updated node cannot be restored without its parent.
        if record.disposition == Disposition::Created {
            return Ok(None);
        }
        return Err(format!(
            "`{parent_name}` block is gone; `{}` cannot be restored",
            record.node.as_str()
        ));
    };
    let children: Vec<_> = block
        .children()
        .map(|doc| {
            doc.nodes()
                .iter()
                .filter(|node| node.name().value() == MUXE_NODE)
                .collect()
        })
        .unwrap_or_default();
    if children.len() > 1 {
        return Err(format!(
            "ambiguous duplicate `{}` nodes; left untouched",
            record.node.as_str()
        ));
    }
    let Some(current) = children.first() else {
        // Already gone: idempotent success with no edit.
        return Ok(None);
    };
    let span = current.span();
    let current_text = original
        .get(span.offset()..span.offset() + span.len())
        .unwrap_or("")
        .to_owned();
    if fsutil::sha256_hex(current_text.as_bytes()) != record.text_digest {
        return Err(format!(
            "`{}` changed since installation; left untouched",
            record.node.as_str()
        ));
    }
    let mut probe = (*current).clone();
    probe.autoformat();
    if probe.to_string() != record.semantic {
        return Err(format!(
            "`{}` changed since installation; left untouched",
            record.node.as_str()
        ));
    }
    match record.disposition {
        Disposition::Observed => Ok(None),
        Disposition::Created => {
            // Extend the removal past one following newline when present so no
            // blank line is left behind.
            let mut end = span.offset() + span.len();
            if original.as_bytes().get(end) == Some(&b'\n') {
                end += 1;
            }
            Ok(Some(kdl::TextEdit {
                start: span.offset(),
                end,
                replacement: String::new(),
            }))
        }
        Disposition::Updated => {
            let previous = record.previous_text.clone().ok_or_else(|| {
                format!(
                    "`{}` lacks previous text; left untouched",
                    record.node.as_str()
                )
            })?;
            Ok(Some(kdl::TextEdit {
                start: span.offset(),
                end: span.offset() + span.len(),
                replacement: previous,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_inputs<'a>(config_dir: &'a Path, wasm: &'a [u8]) -> InstallInputs<'a> {
        InstallInputs {
            config_dir,
            packaged_wasm: wasm,
            version: "0.1.0",
            zellij_config: None,
            explicit_policy: Some(ConfigurationPolicy::Never),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
            hooks: Hooks::default(),
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "fixture shim mirrors the public by-value install shape so transaction tests exercise identical move semantics"
    )]
    fn install(inputs: InstallInputs<'_>) -> Result<InstallOutcome, IntegrationError> {
        let verification = compatibility::NativeAssetVerification {
            packaged_digest: fsutil::sha256_hex(inputs.packaged_wasm),
            registration: compatibility::BridgeRegistrationDigest::current(),
        };
        install_verified(&inputs, verification)
    }

    #[test]
    fn policy_matrix_matches_spec() {
        use ResolvedPolicy::{Always, Ask, Never};
        assert_eq!(
            resolve_policy(Some(ConfigurationPolicy::Always), true, false),
            Always
        );
        assert_eq!(
            resolve_policy(Some(ConfigurationPolicy::Never), false, true),
            Never
        );
        assert_eq!(resolve_policy(None, true, true), Never);
        assert_eq!(resolve_policy(None, false, false), Never);
        assert_eq!(resolve_policy(None, false, true), Ask);
        // Explicit configure wins over quiet.
        assert_eq!(
            resolve_policy(Some(ConfigurationPolicy::Always), true, true),
            Always
        );
    }

    #[test]
    fn install_materializes_bridge_and_receipt_without_config() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let wasm = b"wasm-bytes-v1";
        let digest = fsutil::sha256_hex(wasm);
        let outcome = install(install_inputs(temp.path(), wasm)).unwrap();
        assert_eq!(outcome.bridge_digest, digest);
        assert!(stable_bridge_path(temp.path()).exists());
        assert!(outcome.receipt_path.exists());
        assert!(outcome.manual_snippet.is_none());
    }

    #[test]
    fn install_refuses_foreign_bytes_with_both_digests() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let wasm = b"wasm-v1";
        let digest = fsutil::sha256_hex(wasm);
        install(install_inputs(temp.path(), wasm)).unwrap();
        // Corrupt the bridge outside the transaction.
        fs::write(stable_bridge_path(temp.path()), b"intruder").unwrap();
        let wasm2 = b"wasm-v2";
        let error = install(install_inputs(temp.path(), wasm2)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains(&fsutil::sha256_hex(b"intruder")));
        assert!(message.contains(&digest));
    }

    #[test]
    fn install_edits_config_and_records_ownership() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let conf_dir = temp.path().join("zellij-conf");
        std::fs::create_dir_all(&conf_dir).unwrap();
        std::fs::set_permissions(
            &conf_dir,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = conf_dir.join("config.kdl");
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        let outcome = install(inputs).unwrap();
        assert!(outcome.config_edited);
        let text = fs::read_to_string(&config).unwrap();
        assert!(text.contains("muxe-zellij.wasm"));
        let receipt = receipt::load(&integration_dir(temp.path()))
            .unwrap()
            .unwrap();
        assert_eq!(receipt.configs.len(), 2);
    }

    #[test]
    fn fault_after_every_step_converges() {
        for step in [
            InstallStep::JournalWritten,
            InstallStep::BridgeStaged,
            InstallStep::BeforeKdlApply,
            InstallStep::ConfigEdited,
            InstallStep::BeforeBridgeSwap,
            InstallStep::BridgeCommitted,
            InstallStep::BeforeReceiptCommit,
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let config = temp.path().join("config.kdl");
            let wasm = b"wasm-v1";
            let digest = fsutil::sha256_hex(wasm);
            let mut inputs = install_inputs(temp.path(), wasm);
            inputs.explicit_policy = Some(ConfigurationPolicy::Always);
            inputs.zellij_config = Some(config.clone());
            inputs.hooks.fail_after = Some(step);
            assert!(
                matches!(install(inputs), Err(IntegrationError::FaultInjected { .. })),
                "step {step:?} did not inject"
            );
            // Retry completes or rolls back, then converges to installed.
            let outcome = install({
                let mut retry = install_inputs(temp.path(), wasm);
                retry.explicit_policy = Some(ConfigurationPolicy::Always);
                retry.zellij_config = Some(config.clone());
                retry
            })
            .unwrap_or_else(|error| panic!("step {step:?} retry failed: {error}"));
            assert!(outcome.resumed.is_some() || outcome.bridge_digest == digest);
            assert_eq!(fs::read(stable_bridge_path(temp.path())).unwrap(), wasm);
            let receipt = receipt::load(&integration_dir(temp.path()))
                .unwrap()
                .unwrap();
            assert!(receipt.bridge.bridge_compat.is_some());
        }
    }
    #[test]
    fn kdl_interrupt_preserves_original_provenance() {
        // A crash after the KDL replace but before the bridge swap must not
        // lose Created/Updated provenance: recovery keeps the journaled
        // dispositions instead of recomputing Observed from the applied file.
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        let original =
            "plugins {\n    muxe location=\"file:/old.wasm\"\n}\nload_plugins {\n    muxe\n}\n";
        fs::write(&config, original).unwrap();
        let wasm = b"wasm-v1";
        let digest = fsutil::sha256_hex(wasm);
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        inputs.hooks.fail_after = Some(InstallStep::BeforeBridgeSwap);
        assert!(matches!(
            install(inputs),
            Err(IntegrationError::FaultInjected { .. })
        ));
        let outcome = install({
            let mut retry = install_inputs(temp.path(), wasm);
            retry.explicit_policy = Some(ConfigurationPolicy::Always);
            retry.zellij_config = Some(config);
            retry
        })
        .unwrap();
        assert_eq!(outcome.bridge_digest, digest);
        let receipt = receipt::load(&integration_dir(temp.path()))
            .unwrap()
            .unwrap();
        let plugins = receipt
            .configs
            .iter()
            .find(|record| record.node == ManagedNode::PluginsAlias)
            .unwrap();
        assert_eq!(plugins.disposition, Disposition::Updated);
        assert!(
            plugins
                .previous_text
                .as_ref()
                .unwrap()
                .contains("/old.wasm")
        );
    }

    #[test]
    fn staged_missing_after_kdl_edit_restores_nodes() {
        // Crash after the KDL edit with the staging file deleted externally:
        // forward progress is impossible, so proven nodes are restored and
        // the journal is removed -- a truthful rollback, not false success.
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        let original = "// keep\n";
        fs::write(&config, original).unwrap();
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        inputs.hooks.fail_after = Some(InstallStep::BeforeBridgeSwap);
        assert!(matches!(
            install(inputs),
            Err(IntegrationError::FaultInjected { .. })
        ));
        // KDL was edited; now delete every staging file out-of-band.
        assert_ne!(fs::read_to_string(&config).unwrap(), original);
        for entry in fs::read_dir(integration_dir(temp.path())).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("tmp") {
                fs::remove_file(&path).unwrap();
            }
        }
        let outcome = resume_install(temp.path(), &Hooks::default(), None)
            .unwrap()
            .expect("journal present");
        assert!(matches!(outcome, ResumeOutcome::RolledBack));
        // Rollback removes owned nodes and restores replaced ones; blocks
        // Muxe itself appended stay behind empty, never user bytes.
        let rolled = fs::read_to_string(&config).unwrap();
        assert!(rolled.starts_with(original));
        let document = ::kdl::KdlDocument::parse_v1(&rolled).expect("rolled-back file parses");
        for block in ["plugins", "load_plugins"] {
            let count = document
                .nodes()
                .iter()
                .filter(|node| node.name().value() == block)
                .flat_map(|node| node.children().into_iter())
                .flat_map(|children| children.nodes().iter())
                .filter(|node| node.name().value() == "muxe")
                .count();
            assert_eq!(count, 0, "{block} still holds a muxe node");
        }
        assert!(!stable_bridge_path(temp.path()).exists());
        assert!(
            receipt::load(&integration_dir(temp.path()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn missing_staging_with_changed_prior_bridge_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let first = b"wasm-v1";
        install(install_inputs(temp.path(), first)).unwrap();

        let replacement = b"wasm-v2";
        let mut interrupted = install_inputs(temp.path(), replacement);
        interrupted.hooks.fail_after = Some(InstallStep::BeforeBridgeSwap);
        assert!(matches!(
            install(interrupted),
            Err(IntegrationError::FaultInjected { .. })
        ));
        for entry in fs::read_dir(integration_dir(temp.path())).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("tmp") {
                fs::remove_file(path).unwrap();
            }
        }
        fs::write(stable_bridge_path(temp.path()), b"out-of-band-bridge").unwrap();

        let error = resume_install(temp.path(), &Hooks::default(), None).unwrap_err();
        assert!(matches!(error, IntegrationError::InconsistentJournal(_)));
        assert!(journal_path(&integration_dir(temp.path())).exists());
    }
    #[test]
    fn staged_missing_with_modified_kdl_fails_closed() {
        // Same crash, but the user modified the KDL afterwards: the journaled
        // nodes no longer verify, so recovery preserves everything instead
        // of restoring over user bytes or claiming false success.
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        fs::write(&config, "// keep\n").unwrap();
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        inputs.hooks.fail_after = Some(InstallStep::BeforeBridgeSwap);
        assert!(matches!(
            install(inputs),
            Err(IntegrationError::FaultInjected { .. })
        ));
        for entry in fs::read_dir(integration_dir(temp.path())).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("tmp") {
                fs::remove_file(&path).unwrap();
            }
        }
        fs::write(
            &config,
            "# user rewrite\nplugins {\n    other location=\"x\"\n}\n",
        )
        .unwrap();
        let error = install({
            let mut retry = install_inputs(temp.path(), wasm);
            retry.explicit_policy = Some(ConfigurationPolicy::Always);
            retry.zellij_config = Some(config);
            retry
        })
        .unwrap_err();
        assert!(
            matches!(error, IntegrationError::InconsistentJournal(_)),
            "{error}"
        );
        assert!(journal_path(&integration_dir(temp.path())).exists());
    }

    #[test]
    fn out_of_band_receipt_change_fails_closed() {
        // A receipt that changed outside the transaction is ambiguity, never
        // authority: recovery refuses to mutate.
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.hooks.fail_after = Some(InstallStep::BeforeBridgeSwap);
        assert!(matches!(
            install(inputs),
            Err(IntegrationError::FaultInjected { .. })
        ));
        let directory = integration_dir(temp.path());
        let mut receipt = receipt::load(&directory)
            .unwrap()
            .unwrap_or_else(|| Receipt {
                schema_version: receipt::RECEIPT_SCHEMA_VERSION,
                bridge: receipt::BridgeRecord {
                    canonical_path: stable_bridge_path(temp.path()),
                    installed_version: "9.9.9".to_owned(),
                    installed_digest: "f".repeat(64),
                    previous_digest: None,
                    bridge_compat: None,
                },
                configs: Vec::new(),
            });
        receipt.bridge.installed_digest = "e".repeat(64);
        receipt::store(&directory, &receipt).unwrap();
        let error = install(install_inputs(temp.path(), wasm)).unwrap_err();
        assert!(
            matches!(error, IntegrationError::InconsistentJournal(_)),
            "{error}"
        );
    }

    #[test]
    fn hostile_staging_name_never_resolves() {
        // A journal carrying a non-plain staging name never becomes a
        // mutation target.
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let directory = integration_dir(temp.path());
        fsutil::ensure_owner_dir(&directory).unwrap();
        let journal = InstallJournal {
            schema_version: INSTALL_JOURNAL_SCHEMA_VERSION,
            phase: InstallPhase::Staged,
            version: "0.1.0".to_owned(),
            packaged_digest: "a".repeat(64),
            prior_digest: None,
            staged_name: Some("../evil".to_owned()),
            config_path: None,
            bridge_url: String::new(),
            create_config: false,
            apply_config: false,
            pending: Vec::new(),
        };
        write_journal(&directory, &journal).unwrap();
        let error = resume_install(temp.path(), &Hooks::default(), None).unwrap_err();
        assert!(
            matches!(error, IntegrationError::InconsistentJournal(_)),
            "{error}"
        );
    }

    #[test]
    fn uninstall_restores_updated_nodes_and_removes_receipt() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        fs::write(
            &config,
            "plugins {\n    muxe location=\"file:/old.wasm\"\n}\nload_plugins {\n    muxe\n}\n",
        )
        .unwrap();
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();

        let cache = temp.path().join("cache");
        let outcome = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &cache,
            zellij_config: Some(config.clone()),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap();
        assert!(outcome.bridge_removed);
        assert!(outcome.restored_nodes.contains(&ManagedNode::PluginsAlias));
        assert!(outcome.receipt_removed);
        let text = fs::read_to_string(&config).unwrap();
        assert!(text.contains("file:/old.wasm"));
    }

    #[test]
    fn uninstall_leaves_user_modified_nodes() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();
        // User modifies the node after installation.
        let text = fs::read_to_string(&config).unwrap();
        fs::write(&config, text.replace("muxe-zellij.wasm", "custom.wasm")).unwrap();

        let cache = temp.path().join("cache");
        let outcome = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &cache,
            zellij_config: Some(config),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap();
        assert!(!outcome.unresolved.is_empty());
        assert!(!outcome.receipt_removed);
        assert!(outcome.bridge_removed);
    }
}
