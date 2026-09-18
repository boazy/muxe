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
use nix::fcntl::{Flock, FlockArg};
use thiserror::Error;

use crate::{
    cli::ConfigurationPolicy,
    compatibility,
    fsutil::{self, FsError},
    lifecycle::journal::{self, JournalError, UnitKind, UnitLock},
    logging::Logger,
    paths::ConfigPath,
};

pub use bridge::{BRIDGE_FILE_NAME, PREVIOUS_SUFFIX};
pub use kdl::{LOAD_PLUGINS_NODE, MUXE_NODE, PLUGINS_NODE};
pub use receipt::{Disposition, ManagedNode, NodeRecord, Receipt, Sha256Digest};

/// Integration directory name under `$CONFIG_DIR/integrations/`.
pub const ZELLIJ_INTEGRATION_NAME: &str = "zellij";
/// Install transaction journal file name.
pub const INSTALL_JOURNAL_FILE_NAME: &str = ".install-journal.json";
/// Persistent lock file for the canonical integration directory.
const INTEGRATION_LOCK_FILE_NAME: &str = ".integration.lock";
/// Install journal schema version.
pub const INSTALL_JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum IntegrationLockError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(
        "cannot resolve canonical integration directory {}: {source}",
        directory.display()
    )]
    Canonical {
        directory: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("cannot acquire integration lock at {}: {source}", path.display())]
    Acquire {
        path: PathBuf,
        #[source]
        source: nix::errno::Errno,
    },
    #[error("integration lock at {} is already held", path.display())]
    Active { path: PathBuf },
}

#[derive(Debug)]
struct IntegrationLock {
    _file: Flock<fs::File>,
}

fn integration_lock_path(directory: &Path) -> Result<PathBuf, IntegrationLockError> {
    fs::canonicalize(directory)
        .map(|directory| directory.join(INTEGRATION_LOCK_FILE_NAME))
        .map_err(|source| IntegrationLockError::Canonical {
            directory: directory.to_path_buf(),
            source,
        })
}

fn open_integration_lock(directory: &Path) -> Result<(PathBuf, fs::File), IntegrationLockError> {
    fsutil::ensure_owner_dir(directory)?;
    let path = integration_lock_path(directory)?;
    let file = fsutil::open_owner_file(&path, false)?;
    Ok((path, file))
}

fn lock_integration_file(
    path: PathBuf,
    file: fs::File,
) -> Result<IntegrationLock, IntegrationLockError> {
    let file = Flock::lock(file, FlockArg::LockExclusive).map_err(|(_, source)| {
        IntegrationLockError::Acquire {
            path: path.clone(),
            source,
        }
    })?;
    fsutil::verify_owner_file_descriptor(&path, &file)?;
    Ok(IntegrationLock { _file: file })
}

fn acquire_integration_lock(directory: &Path) -> Result<IntegrationLock, IntegrationLockError> {
    let (path, file) = open_integration_lock(directory)?;
    lock_integration_file(path, file)
}

#[cfg(test)]
enum IntegrationLockAttempt {
    Acquired(IntegrationLock),
    Active,
}

#[cfg(test)]
fn try_lock_integration_file(
    path: PathBuf,
    file: fs::File,
) -> Result<IntegrationLockAttempt, IntegrationLockError> {
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(file) => {
            fsutil::verify_owner_file_descriptor(&path, &file)?;
            Ok(IntegrationLockAttempt::Acquired(IntegrationLock {
                _file: file,
            }))
        }
        Err((_, source)) if source == nix::errno::Errno::EWOULDBLOCK => {
            Ok(IntegrationLockAttempt::Active)
        }
        Err((_, source)) => Err(IntegrationLockError::Acquire { path, source }),
    }
}

#[cfg(test)]
fn try_acquire_integration_lock(
    directory: &Path,
) -> Result<IntegrationLockAttempt, IntegrationLockError> {
    let (path, file) = open_integration_lock(directory)?;
    try_lock_integration_file(path, file)
}

#[derive(Debug, Error)]
pub enum IntegrationError {
    #[error(transparent)]
    IntegrationLock(#[from] IntegrationLockError),
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(transparent)]
    Receipt(#[from] receipt::ReceiptError),
    #[error(transparent)]
    Bridge(#[from] bridge::BridgeError),
    #[error(transparent)]
    Kdl(#[from] kdl::KdlError),
    #[error(transparent)]
    Asset(#[from] compatibility::AssetVerificationError),
    #[error("cannot resolve Zellij configuration: {0}")]
    ConfigDiscovery(String),
    #[error(
        "--zellij-config {} is not a receipt-owned Zellij configuration path; refusing uninstall without mutation",
        path.display()
    )]
    ConfigOverrideUnowned { path: PathBuf },
    #[error(
        "activation journal for this bridge is still live; resolve activation before uninstalling"
    )]
    ActivationJournalLive { journal: PathBuf },
    #[error(transparent)]
    ActivationJournal(#[from] JournalError),
    #[error(
        "interrupted install journal at {} must be resolved before uninstalling",
        journal.display()
    )]
    InstallJournalLive { journal: PathBuf },
    #[error("fault injected after {step:?} (test hook)")]
    FaultInjected { step: InstallStep },
    #[cfg(test)]
    #[error("fault injected after bridge removal (test hook)")]
    UninstallFaultInjected,
    #[error("interrupted install journal is inconsistent: {0}")]
    InconsistentJournal(String),
    #[error("compatibility record unavailable: {0}")]
    Compat(String),
    #[error("auditable operation cannot proceed without its log record")]
    Audit(#[from] crate::logging::LogError),
}

#[cfg(test)]
std::thread_local! {
    static FAIL_UNINSTALL_AFTER_BRIDGE_REMOVAL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
fn set_fail_uninstall_after_bridge_removal(value: bool) {
    FAIL_UNINSTALL_AFTER_BRIDGE_REMOVAL.with(|failure| failure.set(value));
}

#[cfg(test)]
fn fail_uninstall_after_bridge_removal() -> bool {
    FAIL_UNINSTALL_AFTER_BRIDGE_REMOVAL.with(std::cell::Cell::get)
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

#[cfg(test)]
#[derive(Debug)]
struct InstallStepGate {
    step: InstallStep,
    reached: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
}

#[cfg(test)]
#[derive(Debug)]
struct UninstallGate {
    directory: PathBuf,
    reached: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
}

#[cfg(test)]
static UNINSTALL_GATE: std::sync::OnceLock<
    std::sync::Mutex<Option<std::sync::Arc<UninstallGate>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
fn set_uninstall_gate(gate: Option<std::sync::Arc<UninstallGate>>) {
    *UNINSTALL_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap() = gate;
}

#[cfg(test)]
fn wait_uninstall_gate(directory: &Path) {
    let gate = UNINSTALL_GATE
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .unwrap()
        .clone();
    if let Some(gate) = gate.filter(|gate| gate.directory == directory) {
        gate.reached.wait();
        gate.release.wait();
    }
}

#[cfg(test)]
fn assert_integration_lock_active(directory: &Path) {
    match try_acquire_integration_lock(directory).unwrap() {
        IntegrationLockAttempt::Active => {}
        IntegrationLockAttempt::Acquired(lock) => {
            drop(lock);
            panic!("integration lock unexpectedly available");
        }
    }
}

#[cfg(test)]
fn install_gate(
    step: InstallStep,
    reached: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
) -> std::sync::Arc<InstallStepGate> {
    std::sync::Arc::new(InstallStepGate {
        step,
        reached,
        release,
    })
}

#[cfg(test)]
fn uninstall_gate(
    directory: PathBuf,
    reached: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
) -> std::sync::Arc<UninstallGate> {
    std::sync::Arc::new(UninstallGate {
        directory,
        reached,
        release,
    })
}

/// Test and recovery hooks. Production passes `Hooks::default()`.
#[derive(Clone, Debug, Default)]
pub struct Hooks {
    /// When set, the install fails with `FaultInjected` right after the step.
    pub fail_after: Option<InstallStep>,
    #[cfg(test)]
    gate: Option<std::sync::Arc<InstallStepGate>>,
}

impl Hooks {
    fn check(&self, step: InstallStep) -> Result<(), IntegrationError> {
        #[cfg(test)]
        if let Some(gate) = self.gate.as_ref().filter(|gate| gate.step == step) {
            gate.reached.wait();
            gate.release.wait();
        }
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
/// configuration path apply. Its normalized, non-link-resolving result is also
/// the receipt ownership boundary. Never falls back to a relative directory.
///
/// # Errors
///
/// Returns `IntegrationError::ConfigDiscovery` when no usable Zellij path can
/// be resolved.
pub fn zellij_config_path(override_path: Option<&Path>) -> Result<ConfigPath, IntegrationError> {
    crate::paths::zellij_config_path(override_path)
        .map_err(|error| IntegrationError::ConfigDiscovery(error.to_string()))
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
    let directory = integration_dir(inputs.config_dir);
    let _integration_lock = acquire_integration_lock(&directory)?;
    install_verified(&inputs, verification)
}
fn validated_receipt_digest(value: String) -> Result<Sha256Digest, IntegrationError> {
    Sha256Digest::parse(value).map_err(|error| {
        IntegrationError::InconsistentJournal(format!(
            "journal carried an invalid SHA-256 digest: {error}"
        ))
    })
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
            .map(|receipt| receipt.bridge.installed_digest.as_str().to_owned()),
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
        plan_config(config_path.as_path(), &bridge_url, policy, inputs.asker)?;
    let mut manual_snippet = plan_snippet;
    if decision == EditDecision::Apply
        && let Some(planned) = planned
    {
        journal.pending = kdl::plan_records(&config_path, &planned.plan().nodes);
        journal.create_config = planned.is_absent();
        journal.apply_config = true;
        write_journal(directory, &journal)?;
        inputs.hooks.check(InstallStep::BeforeKdlApply)?;
        match kdl::commit_planned(config_path.as_path(), &bridge_url, &planned) {
            Ok(result) => {
                journal.create_config = matches!(result, kdl::ConfigApplied::CreatedMinimal);
            }
            Err(error) => {
                // The configuration edit aborts and prints the required
                // snippet; the staged bridge is discarded and the
                // install continues without KDL ownership.
                journal.pending.clear();
                journal.apply_config = false;
                manual_snippet = Some(kdl_abort_snippet(
                    config_path.as_path(),
                    &bridge_url,
                    error,
                )?);
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
        .and_then(|receipt| receipt.bridge.previous_digest.as_ref())
        .map(Sha256Digest::as_str);
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
            installed_digest: Sha256Digest::from_bytes(inputs.packaged_wasm),
            previous_digest: previous_digest
                .map(validated_receipt_digest)
                .transpose()?
                .or_else(|| {
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
        config_path: journal.apply_config.then(|| config_path.to_path_buf()),
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
            | kdl::KdlError::UnsafePath { .. }
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
        | kdl::KdlError::UnsafePath { .. }
        | kdl::KdlError::ConcurrentChange { .. }
        | kdl::KdlError::CandidateRejected { .. } => Ok(format!(
            "Could not edit {error} in {}.\nAdd these nodes manually:\n{}",
            config_path.display(),
            kdl::required_snippet(bridge_url)
        )),
        kdl::KdlError::Fs(source) => Err(IntegrationError::Fs(source)),
    }
}

/// Merges the journaled KDL plans with receipt provenance by the exact
/// configuration ownership identity and managed node.
///
/// A skipped edit has no pending records, so every prior record remains. A
/// no-op observation only retains ownership when its exact installed semantic
/// and text digest still match the prior receipt. Replacing an already-owned
/// node keeps the prior disposition and provenance, so repeated updates still
/// restore the user's original text on uninstall.
fn merge_records(
    previous: Option<&Receipt>,
    config_path: &ConfigPath,
    pending: &[NodeRecord],
) -> Vec<NodeRecord> {
    let mut merged = previous
        .map(|receipt| receipt.configs.clone())
        .unwrap_or_default();
    for pending_record in pending {
        debug_assert_eq!(&pending_record.config_path, config_path);
        if let Some(index) = merged.iter().position(|previous_record| {
            previous_record.config_path == pending_record.config_path
                && previous_record.node == pending_record.node
        }) {
            merged[index] = merge_record(&merged[index], pending_record);
        } else {
            merged.push(pending_record.clone());
        }
    }
    merged
}

fn merge_record(previous: &NodeRecord, pending: &NodeRecord) -> NodeRecord {
    match pending.disposition {
        Disposition::Observed
            if previous.disposition != Disposition::Observed
                && matches_installed_record(previous, pending) =>
        {
            previous.clone()
        }
        Disposition::Updated
            if previous.disposition != Disposition::Observed
                && pending_replaces_installed_record(previous, pending) =>
        {
            NodeRecord {
                disposition: previous.disposition,
                previous_text: previous.previous_text.clone(),
                previous_semantic: previous.previous_semantic.clone(),
                ..pending.clone()
            }
        }
        _ => pending.clone(),
    }
}

fn matches_installed_record(previous: &NodeRecord, pending: &NodeRecord) -> bool {
    previous.semantic == pending.semantic && previous.text_digest == pending.text_digest
}

fn pending_replaces_installed_record(previous: &NodeRecord, pending: &NodeRecord) -> bool {
    pending.previous_semantic.as_deref() == Some(&previous.semantic)
        && pending
            .previous_text
            .as_ref()
            .is_some_and(|text| Sha256Digest::from_bytes(text.as_bytes()) == previous.text_digest)
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
    config_path: Option<ConfigPath>,
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
    let _integration_lock = acquire_integration_lock(&directory)?;
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
        match kdl::verify_records(config_path.as_path(), &journal.pending) {
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
        if let Err(reason) = kdl::verify_records(config_path.as_path(), &journal.pending) {
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
        .map(|receipt| receipt.bridge.installed_digest.as_str().to_owned());
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
            if Sha256Digest::from_bytes(&bytes) == receipt.bridge.installed_digest =>
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

fn reapply_kdl(
    config_path: &ConfigPath,
    journal: &InstallJournal,
) -> Result<Vec<NodeRecord>, Reapply> {
    let planned = kdl::read_and_plan(config_path.as_path(), &journal.bridge_url).map_err(
        |error| match error {
            kdl::KdlError::Fs(source) => Reapply::Fatal(IntegrationError::Fs(source)),
            other => Reapply::Fatal(IntegrationError::InconsistentJournal(format!(
                "cannot re-plan KDL edit: {other}"
            ))),
        },
    )?;
    match kdl::commit_planned(config_path.as_path(), &journal.bridge_url, &planned) {
        Ok(applied) => Ok(applied.node_records(config_path)),
        Err(
            error @ (kdl::KdlError::Unparseable { .. }
            | kdl::KdlError::Ambiguous { .. }
            | kdl::KdlError::UnsafePath { .. }
            | kdl::KdlError::ConcurrentChange { .. }
            | kdl::KdlError::CandidateRejected { .. }),
        ) => Err(Reapply::AbortSnippet(format!(
            "Could not edit {config_path}: {error}\nAdd these nodes manually:\n{}",
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
        kdl::verify_records(config_path.as_path(), &journal.pending).map_err(|reason| {
            IntegrationError::InconsistentJournal(format!(
                "cannot restore KDL ({context}): {reason}"
            ))
        })?;
        rollback_pending_nodes(config_path.as_path(), &journal.pending).map_err(|reason| {
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
                    Some(receipt) if receipt.bridge.installed_digest.as_str() == current_digest => {
                        let authority = receipt
                            .bridge
                            .previous_digest
                            .as_ref()
                            .map(Sha256Digest::as_str);
                        let record = bridge::ensure_backup(stable, authority)?;
                        Some(validated_receipt_digest(record.digest)?)
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
            .map(|receipt| receipt.bridge.installed_digest.as_str().to_owned());
        if expected != vouched && receipt.is_some() {
            return Err(IntegrationError::InconsistentJournal(
                "stable bridge changed outside the transaction".to_owned(),
            ));
        }
        bridge::commit(staged, stable, expected.as_deref())?;
    }
    hooks.check(InstallStep::BridgeCommitted)?;
    let config_path = match journal.config_path.clone() {
        Some(config_path) => config_path,
        None => ConfigPath::from_input(stable).map_err(|error| {
            IntegrationError::InconsistentJournal(format!(
                "cannot recover the default configuration identity: {error}"
            ))
        })?,
    };
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
                installed_digest: validated_receipt_digest(journal.packaged_digest.clone())?,
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
        config_path: journal.apply_config.then(|| config_path.to_path_buf()),
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
    let _integration_lock = acquire_integration_lock(&directory)?;
    let stable = directory.join(BRIDGE_FILE_NAME);
    refuse_interrupted_install(&directory)?;
    let receipt = receipt::load(&directory)?;
    let _unit_lock = refuse_when_activation_live(inputs.cache_dir, &stable)?;
    let Some(receipt) = receipt else {
        return Ok(UninstallOutcome {
            bridge_removed: false,
            previous_removed: false,
            staging_removed: 0,
            restored_nodes: Vec::new(),
            removed_nodes: Vec::new(),
            unresolved: Vec::new(),
            receipt_removed: false,
        });
    };
    let (selected_records, unselected_records) =
        select_uninstall_records(&receipt, inputs.zellij_config.as_deref())?;
    // The exact bridge-sharing unit stays locked across receipt authority
    // preflight and mutation. A valid Announced journal has no bridge digest,
    // so unit identity — never a raw-byte digest search — is the boundary.

    let policy = resolve_policy(inputs.explicit_policy, inputs.quiet, inputs.interactive);
    let remove_configs = match policy {
        ResolvedPolicy::Never => false,
        ResolvedPolicy::Always => true,
        ResolvedPolicy::Ask => inputs
            .asker
            .is_some_and(|ask| ask("Remove or restore Muxe-owned Zellij KDL nodes?")),
    };
    let plans = if remove_configs {
        preflight_uninstall_edits(&selected_records)?
    } else {
        Vec::new()
    };
    // Nothing destructively changes before every KDL candidate and both bridge
    // artifacts have receipt authority. Mutation-time operations re-validate
    // their captured bytes to reject concurrent replacement.
    preflight_bridge_artifacts(&stable, &receipt.bridge)?;

    #[cfg(test)]
    wait_uninstall_gate(&directory);

    let mut outcome = UninstallOutcome {
        bridge_removed: false,
        previous_removed: false,
        staging_removed: 0,
        restored_nodes: Vec::new(),
        removed_nodes: Vec::new(),
        unresolved: Vec::new(),
        receipt_removed: false,
    };
    for record in unselected_records {
        if record.disposition != Disposition::Observed {
            outcome.unresolved.push(UnresolvedRecord {
                node: Some(record.node),
                config_path: Some(record.config_path.to_path_buf()),
                reason: "configuration was not selected by --zellij-config; node left in place"
                    .to_owned(),
            });
        }
    }
    if remove_configs {
        for plan in plans {
            let edits = commit_uninstall_plan(plan)?;
            outcome.removed_nodes.extend(edits.removed);
            outcome.restored_nodes.extend(edits.restored);
            outcome.unresolved.extend(edits.unresolved);
        }
    } else {
        for record in selected_records {
            if record.disposition != Disposition::Observed {
                outcome.unresolved.push(UnresolvedRecord {
                    node: Some(record.node),
                    config_path: Some(record.config_path.to_path_buf()),
                    reason: "configuration edit declined; node left in place".to_owned(),
                });
            }
        }
    }

    let previous = bridge::previous_path(&stable);
    if let Some(expected) = receipt.bridge.previous_digest.as_ref() {
        outcome.previous_removed = bridge::remove_if_matching(&previous, expected.as_str())?;
    }
    outcome.bridge_removed =
        bridge::remove_if_matching(&stable, receipt.bridge.installed_digest.as_str())?;
    #[cfg(test)]
    if fail_uninstall_after_bridge_removal() {
        return Err(IntegrationError::UninstallFaultInjected);
    }

    if outcome.unresolved.is_empty() && bridge_absent(&stable)? {
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

/// Refuses cleanup while an interrupted install journal remains authoritative.
///
/// Uninstall deliberately does not resume it: recovery can legitimately edit
/// KDL or commit bridge bytes, whereas this operation has not yet resolved
/// whether it owns any artifact.
fn refuse_interrupted_install(directory: &Path) -> Result<(), IntegrationError> {
    let path = journal_path(directory);
    match fs::symlink_metadata(&path) {
        Ok(_) => Err(IntegrationError::InstallJournalLive { journal: path }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(IntegrationError::Fs(fsutil::io_error(
            "checking interrupted install journal",
            &path,
            source,
        ))),
    }
}

/// Refuses uninstallation while the exact bridge-sharing activation unit is live.
///
/// A corrupt journal at that unit path is surfaced through the typed lifecycle
/// reader and is never mistaken for an absent journal.
fn refuse_when_activation_live(
    cache_dir: &Path,
    stable: &Path,
) -> Result<UnitLock, IntegrationError> {
    let unit = UnitKind::Zellij {
        bridge_path_hash: journal::unit_hash(&stable.display().to_string()),
    };
    let lock = journal::acquire_unit_lock(cache_dir, &unit)?;
    for (path, entry) in journal::list_journals(cache_dir)? {
        let journal = entry?;
        if path.file_name().and_then(|name| name.to_str())
            != Some(journal.unit.journal_name().as_str())
        {
            return Err(IntegrationError::ActivationJournal(
                JournalError::Inconsistent(format!(
                    "activation journal at {} does not match its typed unit name",
                    path.display()
                )),
            ));
        }
        if journal.unit == unit {
            return Err(IntegrationError::ActivationJournalLive { journal: path });
        }
    }
    Ok(lock)
}

/// Verifies receipt authority for both bridge artifacts without changing either.
fn preflight_bridge_artifacts(
    stable: &Path,
    bridge_record: &receipt::BridgeRecord,
) -> Result<(), IntegrationError> {
    let _ = bridge::check_destination(stable, Some(bridge_record.installed_digest.as_str()))?;
    bridge::check_previous(
        stable,
        bridge_record
            .previous_digest
            .as_ref()
            .map(Sha256Digest::as_str),
    )?;
    Ok(())
}

/// Returns whether the stable bridge path is exactly absent.
///
/// A dangling symlink is still an artifact. Receipt provenance may be removed
/// only when no directory entry exists at the stable path.
fn bridge_absent(stable: &Path) -> Result<bool, IntegrationError> {
    match fs::symlink_metadata(stable) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Ok(_) => Ok(false),
        Err(source) => Err(IntegrationError::Fs(fsutil::io_error(
            "checking stable bridge before receipt removal",
            stable,
            source,
        ))),
    }
}

/// Selects receipt records for uninstallation without ever transferring
/// ownership between configuration paths.
fn select_uninstall_records<'a>(
    receipt: &'a Receipt,
    config_override: Option<&Path>,
) -> Result<(Vec<&'a NodeRecord>, Vec<&'a NodeRecord>), IntegrationError> {
    let Some(config_override) = config_override else {
        return Ok((receipt.configs.iter().collect(), Vec::new()));
    };
    let selected_path = zellij_config_path(Some(config_override))?;
    let (selected, unselected): (Vec<_>, Vec<_>) = receipt
        .configs
        .iter()
        .partition(|record| record.config_path == selected_path);
    if selected.is_empty() {
        return Err(IntegrationError::ConfigOverrideUnowned {
            path: selected_path.to_path_buf(),
        });
    }
    Ok((selected, unselected))
}

/// Preflights every selected configuration before any bridge or KDL mutation.
fn preflight_uninstall_edits(
    records: &[&NodeRecord],
) -> Result<Vec<FileUninstallPlan>, IntegrationError> {
    use std::collections::BTreeMap;
    let mut by_file: BTreeMap<ConfigPath, Vec<&NodeRecord>> = BTreeMap::new();
    for record in records {
        by_file
            .entry(record.config_path.clone())
            .or_default()
            .push(*record);
    }
    by_file
        .into_iter()
        .map(|(config_path, records)| preflight_uninstall_file(config_path.as_path(), &records))
        .collect()
}

#[derive(Default)]
struct FileUninstall {
    removed: Vec<ManagedNode>,
    restored: Vec<ManagedNode>,
    unresolved: Vec<UnresolvedRecord>,
}

struct FileUninstallPlan {
    config_path: PathBuf,
    original: Option<kdl::ExistingConfig>,
    candidate: Option<String>,
    applied: Vec<(ManagedNode, Disposition)>,
    result: FileUninstall,
}

fn unresolved_plan(
    config_path: &Path,
    records: &[&NodeRecord],
    reason: String,
) -> FileUninstallPlan {
    FileUninstallPlan {
        config_path: config_path.to_path_buf(),
        original: None,
        candidate: None,
        applied: Vec::new(),
        result: FileUninstall {
            removed: Vec::new(),
            restored: Vec::new(),
            unresolved: records
                .iter()
                .filter(|record| record.disposition != Disposition::Observed)
                .map(|record| UnresolvedRecord {
                    node: Some(record.node),
                    config_path: Some(config_path.to_path_buf()),
                    reason: reason.clone(),
                })
                .collect(),
        },
    }
}

fn preflight_uninstall_file(
    config_path: &Path,
    records: &[&NodeRecord],
) -> Result<FileUninstallPlan, IntegrationError> {
    match fs::symlink_metadata(config_path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(unresolved_plan(
                config_path,
                records,
                "configuration file no longer exists".to_owned(),
            ));
        }
        Err(source) => {
            return Err(IntegrationError::Fs(fsutil::io_error(
                "checking Zellij configuration",
                config_path,
                source,
            )));
        }
    }
    let snapshot = kdl::read_existing_config(config_path)?;
    let bytes = snapshot.bytes().to_vec();
    let original = match String::from_utf8(bytes.clone()) {
        Ok(original) => original,
        Err(_) => {
            return Ok(unresolved_plan(
                config_path,
                records,
                "configuration is not valid UTF-8".to_owned(),
            ));
        }
    };
    let document = match KdlDocument::parse_v1(&original) {
        Ok(document) => document,
        Err(error) => return Ok(unresolved_plan(config_path, records, error.to_string())),
    };
    let mut result = FileUninstall::default();
    let mut edits = Vec::new();
    for record in records {
        if record.disposition == Disposition::Observed {
            continue;
        }
        match plan_uninstall_node(&document, &original, record) {
            Ok(Some(edit)) => edits.push((record.node, record.disposition, edit)),
            Ok(None) if record.disposition == Disposition::Created => {
                result.removed.push(record.node);
            }
            Ok(None) => result.unresolved.push(UnresolvedRecord {
                node: Some(record.node),
                config_path: Some(config_path.to_path_buf()),
                reason: format!(
                    "`{}` is absent without proven restored provenance; left untouched",
                    record.node.as_str()
                ),
            }),
            Err(_) if restored_exactly(&document, &original, record) => {
                result.restored.push(record.node);
            }
            Err(reason) => result.unresolved.push(UnresolvedRecord {
                node: Some(record.node),
                config_path: Some(config_path.to_path_buf()),
                reason,
            }),
        }
    }
    let candidate = if edits.is_empty() {
        None
    } else {
        let text_edits: Vec<kdl::TextEdit> =
            edits.iter().map(|(_, _, edit)| edit.clone()).collect();
        let candidate = kdl::apply_edits(&original, &text_edits);
        KdlDocument::parse_v1(&candidate).map_err(|error| {
            IntegrationError::Kdl(kdl::KdlError::CandidateRejected {
                detail: error.to_string(),
            })
        })?;
        Some(candidate)
    };
    Ok(FileUninstallPlan {
        config_path: config_path.to_path_buf(),
        original: Some(snapshot),
        candidate,
        applied: edits
            .into_iter()
            .map(|(node, disposition, _)| (node, disposition))
            .collect(),
        result,
    })
}

fn commit_uninstall_plan(mut plan: FileUninstallPlan) -> Result<FileUninstall, IntegrationError> {
    if let (Some(snapshot), Some(candidate)) = (&plan.original, &plan.candidate) {
        kdl::write_existing_config(&plan.config_path, snapshot, candidate)?;
        for (node, disposition) in plan.applied {
            match disposition {
                Disposition::Created => plan.result.removed.push(node),
                Disposition::Updated => plan.result.restored.push(node),
                Disposition::Observed => {}
            }
        }
    }
    Ok(plan.result)
}

/// Recognizes an exact restored `Updated` node on retry without rewriting it.
fn restored_exactly(document: &KdlDocument, original: &str, record: &NodeRecord) -> bool {
    if record.disposition != Disposition::Updated {
        return false;
    }
    let parent_name = match record.node {
        ManagedNode::PluginsAlias => PLUGINS_NODE,
        ManagedNode::LoadPluginsEntry => LOAD_PLUGINS_NODE,
    };
    let blocks: Vec<_> = document
        .nodes()
        .iter()
        .filter(|node| node.name().value() == parent_name)
        .collect();
    let Some(block) = (blocks.len() == 1).then(|| blocks[0]) else {
        return false;
    };
    let children: Vec<_> = block
        .children()
        .map(|children| {
            children
                .nodes()
                .iter()
                .filter(|node| node.name().value() == MUXE_NODE)
                .collect()
        })
        .unwrap_or_default();
    let Some(current) = (children.len() == 1).then(|| children[0]) else {
        return false;
    };
    let span = current.span();
    let Some(text) = original.get(span.offset()..span.offset() + span.len()) else {
        return false;
    };
    let mut semantic = (*current).clone();
    semantic.autoformat();
    let semantic = semantic.to_string();
    record.previous_text.as_deref() == Some(text)
        && record.previous_semantic.as_deref() == Some(semantic.as_str())
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
    if Sha256Digest::from_bytes(current_text.as_bytes()) != record.text_digest {
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
        };
        let directory = integration_dir(inputs.config_dir);
        let _integration_lock = acquire_integration_lock(&directory)?;
        install_verified(&inputs, verification)
    }

    fn uninstall_inputs<'a>(
        config_dir: &'a Path,
        cache_dir: &'a Path,
        zellij_config: PathBuf,
    ) -> UninstallInputs<'a> {
        UninstallInputs {
            config_dir,
            cache_dir,
            zellij_config: Some(zellij_config),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        }
    }

    fn owner_temp() -> tempfile::TempDir {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        temp
    }

    fn lifecycle_record() -> muxe_protocol::control::CompatibilityRecord {
        muxe_protocol::control::CompatibilityRecord {
            muxe_version: "0.1.0".to_owned(),
            target_triple: "aarch64-apple-darwin".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([7; 32]),
            zellij: None,
            herdr: None,
        }
    }

    fn write_announced_journal(cache: &Path, stable: &Path) -> PathBuf {
        let unit = UnitKind::Zellij {
            bridge_path_hash: journal::unit_hash(&stable.display().to_string()),
        };
        let mut journal = journal::ActivationJournal::new(
            unit,
            lifecycle_record(),
            lifecycle_record(),
            vec![journal::MemberState {
                host_identity: "zellij-session".to_owned(),
                old_socket: PathBuf::from("/tmp/zellij-old.sock"),
                target_socket: None,
                handoff_id: None,
                state: journal::MemberTransition::Prepared,
            }],
        );
        journal.state = journal::JournalState::Announced;
        journal::write_journal(cache, &journal).unwrap()
    }

    fn assert_managed_node_absent(document: &KdlDocument, node: ManagedNode) {
        let parent_name = match node {
            ManagedNode::PluginsAlias => PLUGINS_NODE,
            ManagedNode::LoadPluginsEntry => LOAD_PLUGINS_NODE,
        };
        let parents: Vec<_> = document
            .nodes()
            .iter()
            .filter(|candidate| candidate.name().value() == parent_name)
            .collect();
        assert_eq!(parents.len(), 1, "expected one `{parent_name}` block");
        assert!(
            !parents[0]
                .children()
                .unwrap()
                .nodes()
                .iter()
                .any(|candidate| candidate.name().value() == MUXE_NODE),
            "owned `{}` node remained",
            node.as_str()
        );
    }

    fn assert_created_nodes_removed_with_user_text(config: &Path) {
        let text = fs::read_to_string(config).unwrap();
        assert!(text.contains("    other location=\"file:/other.wasm\"\n"));
        assert!(text.contains("    other\n"));
        let document = KdlDocument::parse_v1(&text).unwrap();
        assert_managed_node_absent(&document, ManagedNode::PluginsAlias);
        assert_managed_node_absent(&document, ManagedNode::LoadPluginsEntry);
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
    fn skipped_or_declined_reinstall_preserves_created_records_for_uninstall() {
        for (case, policy, interactive) in [
            ("skipped", Some(ConfigurationPolicy::Never), false),
            ("declined", None, true),
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let config = temp.path().join("config.kdl");
            let original = "plugins {\n    other location=\"file:/other.wasm\"\n}\nload_plugins {\n    other\n}\n";
            fs::write(&config, original).unwrap();
            let wasm = b"wasm-v1";

            let mut initial = install_inputs(temp.path(), wasm);
            initial.explicit_policy = Some(ConfigurationPolicy::Always);
            initial.zellij_config = Some(config.clone());
            install(initial).unwrap();

            let declined = |_prompt: &str| false;
            let mut reinstall = install_inputs(temp.path(), wasm);
            reinstall.explicit_policy = policy;
            reinstall.quiet = false;
            reinstall.interactive = interactive;
            reinstall.asker = Some(&declined);
            reinstall.zellij_config = Some(config.clone());
            let outcome = install(reinstall).unwrap();
            assert!(
                !outcome.config_edited,
                "{case} reinstall edited configuration"
            );

            let config_path = ConfigPath::from_input(&config).unwrap();
            let receipt = receipt::load(&integration_dir(temp.path()))
                .unwrap()
                .unwrap();
            assert!(receipt.configs.iter().all(|record| {
                record.config_path == config_path && record.disposition == Disposition::Created
            }));

            let cache = temp.path().join("cache");
            let outcome = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap();
            assert_eq!(
                outcome.removed_nodes,
                vec![ManagedNode::PluginsAlias, ManagedNode::LoadPluginsEntry],
                "{case} reinstall lost created ownership"
            );
            assert!(outcome.receipt_removed);
            assert_created_nodes_removed_with_user_text(&config);
        }
    }

    #[test]
    fn no_op_reinstall_retains_created_records_for_uninstall() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        let original =
            "plugins {\n    other location=\"file:/other.wasm\"\n}\nload_plugins {\n    other\n}\n";
        fs::write(&config, original).unwrap();
        let wasm = b"wasm-v1";

        let mut initial = install_inputs(temp.path(), wasm);
        initial.explicit_policy = Some(ConfigurationPolicy::Always);
        initial.zellij_config = Some(config.clone());
        install(initial).unwrap();

        let mut reinstall = install_inputs(temp.path(), wasm);
        reinstall.explicit_policy = Some(ConfigurationPolicy::Always);
        reinstall.zellij_config = Some(config.clone());
        let outcome = install(reinstall).unwrap();
        assert!(!outcome.config_edited);

        let config_path = ConfigPath::from_input(&config).unwrap();
        let receipt = receipt::load(&integration_dir(temp.path()))
            .unwrap()
            .unwrap();
        assert!(receipt.configs.iter().all(|record| {
            record.config_path == config_path && record.disposition == Disposition::Created
        }));

        let cache = temp.path().join("cache");
        let outcome = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap();
        assert_eq!(
            outcome.removed_nodes,
            vec![ManagedNode::PluginsAlias, ManagedNode::LoadPluginsEntry]
        );
        assert!(outcome.receipt_removed);
        assert_created_nodes_removed_with_user_text(&config);
    }

    #[test]
    fn no_op_reinstall_keeps_updated_node_original_restoration_text() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        let original_alias = "muxe    location=\"file:/original.wasm\"";
        let original =
            format!("plugins {{\n    {original_alias}\n}}\nload_plugins {{\n    muxe\n}}\n");
        fs::write(&config, &original).unwrap();
        let wasm = b"wasm-v1";

        let mut initial = install_inputs(temp.path(), wasm);
        initial.explicit_policy = Some(ConfigurationPolicy::Always);
        initial.zellij_config = Some(config.clone());
        install(initial).unwrap();

        let mut reinstall = install_inputs(temp.path(), wasm);
        reinstall.explicit_policy = Some(ConfigurationPolicy::Always);
        reinstall.zellij_config = Some(config.clone());
        assert!(!install(reinstall).unwrap().config_edited);

        let config_path = ConfigPath::from_input(&config).unwrap();
        let receipt = receipt::load(&integration_dir(temp.path()))
            .unwrap()
            .unwrap();
        let alias = receipt
            .configs
            .iter()
            .find(|record| {
                record.config_path == config_path && record.node == ManagedNode::PluginsAlias
            })
            .unwrap_or_else(|| panic!("missing owned alias record: {:?}", receipt.configs));
        assert_eq!(alias.disposition, Disposition::Updated);
        assert_eq!(alias.previous_text.as_deref(), Some(original_alias));

        let cache = temp.path().join("cache");
        let outcome = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap();
        assert_eq!(outcome.restored_nodes, vec![ManagedNode::PluginsAlias]);
        assert!(outcome.receipt_removed);
        assert_eq!(fs::read(&config).unwrap(), original.as_bytes());
    }

    #[test]
    fn user_edited_node_is_observed_without_reclaiming_ownership() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        fs::write(
            &config,
            "plugins {\n    other location=\"file:/other.wasm\"\n}\nload_plugins {\n    other\n}\n",
        )
        .unwrap();
        let wasm = b"wasm-v1";

        let mut initial = install_inputs(temp.path(), wasm);
        initial.explicit_policy = Some(ConfigurationPolicy::Always);
        initial.zellij_config = Some(config.clone());
        install(initial).unwrap();
        let user_edited =
            fs::read_to_string(&config)
                .unwrap()
                .replacen("muxe location=", "muxe    location=", 1);
        fs::write(&config, &user_edited).unwrap();

        let mut reinstall = install_inputs(temp.path(), wasm);
        reinstall.explicit_policy = Some(ConfigurationPolicy::Always);
        reinstall.zellij_config = Some(config.clone());
        assert!(!install(reinstall).unwrap().config_edited);
        let config_path = ConfigPath::from_input(&config).unwrap();
        let receipt = receipt::load(&integration_dir(temp.path()))
            .unwrap()
            .unwrap();
        let alias = receipt
            .configs
            .iter()
            .find(|record| {
                record.config_path == config_path && record.node == ManagedNode::PluginsAlias
            })
            .unwrap();
        assert_eq!(alias.disposition, Disposition::Observed);

        let cache = temp.path().join("cache");
        let outcome = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap();
        assert!(!outcome.removed_nodes.contains(&ManagedNode::PluginsAlias));
        assert!(
            outcome
                .removed_nodes
                .contains(&ManagedNode::LoadPluginsEntry)
        );
        assert!(outcome.receipt_removed);
        let text = fs::read_to_string(&config).unwrap();
        assert!(text.contains("    other location=\"file:/other.wasm\"\n"));
        assert!(text.contains("    other\n"));
        assert!(text.contains("muxe    location=\""));
        let document = KdlDocument::parse_v1(&text).unwrap();
        assert_managed_node_absent(&document, ManagedNode::LoadPluginsEntry);
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
                    installed_digest: Sha256Digest::parse("f".repeat(64)).unwrap(),
                    previous_digest: None,
                    bridge_compat: None,
                },
                configs: Vec::new(),
            });
        receipt.bridge.installed_digest = Sha256Digest::parse("e".repeat(64)).unwrap();
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
    fn uninstall_rejects_invalid_receipt_provenance_before_mutation() {
        for (case, installed_digest, disposition, previous_text, previous_semantic) in [
            ("empty digest", "", "Created", None, None),
            ("malformed digest", "not-a-sha256", "Created", None, None),
            (
                "created claims prior state",
                &"a".repeat(64),
                "Created",
                Some("old node"),
                Some("old semantic"),
            ),
            (
                "updated lacks prior state",
                &"a".repeat(64),
                "Updated",
                None,
                None,
            ),
            (
                "updated has empty prior text",
                &"a".repeat(64),
                "Updated",
                Some(""),
                Some("muxe location=\"file:/old.wasm\""),
            ),
            (
                "updated prior text names wrong node",
                &"a".repeat(64),
                "Updated",
                Some("other location=\"file:/old.wasm\""),
                Some("other location=\"file:/old.wasm\""),
            ),
            (
                "updated prior semantic mismatches text",
                &"a".repeat(64),
                "Updated",
                Some("muxe location=\"file:/old.wasm\""),
                Some("muxe location=\"file:/different.wasm\""),
            ),
            (
                "updated prior text injects sibling node",
                &"a".repeat(64),
                "Updated",
                Some("muxe location=\"file:/old.wasm\"\nother"),
                Some("muxe location=\"file:/old.wasm\""),
            ),
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let config = temp.path().join("config.kdl");
            let stable = stable_bridge_path(temp.path());
            let directory = integration_dir(temp.path());
            let cache = temp.path().join("cache");
            let receipt_path = directory.join(receipt::RECEIPT_FILE_NAME);
            fs::write(&config, "// user configuration\n").unwrap();
            fsutil::ensure_owner_dir(&directory).unwrap();
            fs::write(&stable, b"bridge bytes").unwrap();
            fsutil::ensure_owner_dir(&cache.join("activation")).unwrap();
            fs::write(cache.join("activation/live.json"), "a".repeat(64)).unwrap();
            let receipt = serde_json::json!({
                "schema_version": receipt::RECEIPT_SCHEMA_VERSION,
                "bridge": {
                    "canonical_path": stable,
                    "installed_version": "0.1.0",
                    "installed_digest": installed_digest,
                    "previous_digest": null,
                    "bridge_compat": null,
                },
                "configs": [{
                    "config_path": config,
                    "node": "plugins_alias",
                    "disposition": disposition,
                    "semantic": "muxe",
                    "text_digest": "b".repeat(64),
                    "previous_text": previous_text,
                    "previous_semantic": previous_semantic,
                }],
            });
            fsutil::write_atomic(
                &receipt_path,
                &serde_json::to_vec(&receipt).unwrap(),
                "receipt",
            )
            .unwrap();

            let error = uninstall(UninstallInputs {
                config_dir: temp.path(),
                cache_dir: &cache,
                zellij_config: Some(config.clone()),

                explicit_policy: Some(ConfigurationPolicy::Always),
                quiet: true,
                interactive: false,
                asker: None,
                logger: None,
            })
            .unwrap_err();

            assert!(
                matches!(
                    error,
                    IntegrationError::Receipt(receipt::ReceiptError::Invalid { .. })
                ),
                "{case}: {error}"
            );
            assert_eq!(fs::read(&stable).unwrap(), b"bridge bytes", "{case}");
            assert_eq!(
                fs::read_to_string(&config).unwrap(),
                "// user configuration\n",
                "{case}"
            );
            assert!(receipt_path.exists(), "{case}");
        }
    }

    #[test]
    fn uninstall_rejects_legacy_parent_traversal_receipt_before_mutation() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("config.kdl");
        let stable = stable_bridge_path(temp.path());
        let directory = integration_dir(temp.path());
        let receipt_path = directory.join(receipt::RECEIPT_FILE_NAME);
        fs::write(&config, "// user configuration\n").unwrap();
        fsutil::ensure_owner_dir(&directory).unwrap();
        fs::write(&stable, b"bridge bytes").unwrap();
        let alias = temp.path().join("ancestor-symlink");
        std::os::unix::fs::symlink(temp.path(), &alias).unwrap();
        let receipt = serde_json::json!({
            "schema_version": receipt::RECEIPT_SCHEMA_VERSION,
            "bridge": {
                "canonical_path": stable,
                "installed_version": "0.1.0",
                "installed_digest": fsutil::sha256_hex(b"bridge bytes"),
                "previous_digest": null,
                "bridge_compat": null,
            },
            "configs": [{
                "config_path": alias.join("../config.kdl"),
                "node": "plugins_alias",
                "disposition": "Created",
                "semantic": "muxe",
                "text_digest": "b".repeat(64),
                "previous_text": null,
                "previous_semantic": null,
            }],
        });
        fsutil::write_atomic(
            &receipt_path,
            &serde_json::to_vec(&receipt).unwrap(),
            "receipt",
        )
        .unwrap();
        let config_before = fs::read(&config).unwrap();
        let bridge_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: None,
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::Receipt(receipt::ReceiptError::Corrupt { .. })
        ));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), bridge_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    }

    #[test]
    fn uninstall_rejects_relative_or_dot_receipt_paths_before_mutation() {
        for case in ["relative", "dot segment"] {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let config = temp.path().join("owned/config.kdl");
            let stable = stable_bridge_path(temp.path());
            let directory = integration_dir(temp.path());
            let receipt_path = directory.join(receipt::RECEIPT_FILE_NAME);
            fs::create_dir_all(config.parent().unwrap()).unwrap();
            fs::write(&config, "// user configuration\n").unwrap();
            fsutil::ensure_owner_dir(&directory).unwrap();
            fs::write(&stable, b"bridge bytes").unwrap();
            let persisted_path = match case {
                "relative" => PathBuf::from("owned/config.kdl"),
                "dot segment" => temp.path().join("owned/./config.kdl"),
                _ => unreachable!(),
            };
            let receipt = serde_json::json!({
                "schema_version": receipt::RECEIPT_SCHEMA_VERSION,
                "bridge": {
                    "canonical_path": stable,
                    "installed_version": "0.1.0",
                    "installed_digest": fsutil::sha256_hex(b"bridge bytes"),
                    "previous_digest": null,
                    "bridge_compat": null,
                },
                "configs": [{
                    "config_path": persisted_path,
                    "node": "plugins_alias",
                    "disposition": "Created",
                    "semantic": "muxe",
                    "text_digest": "b".repeat(64),
                    "previous_text": null,
                    "previous_semantic": null,
                }],
            });
            fsutil::write_atomic(
                &receipt_path,
                &serde_json::to_vec(&receipt).unwrap(),
                "receipt",
            )
            .unwrap();
            let config_before = fs::read(&config).unwrap();
            let bridge_before = fs::read(&stable).unwrap();
            let receipt_before = fs::read(&receipt_path).unwrap();

            let error = uninstall(UninstallInputs {
                config_dir: temp.path(),
                cache_dir: &temp.path().join("cache"),
                zellij_config: None,
                explicit_policy: Some(ConfigurationPolicy::Always),
                quiet: true,
                interactive: false,
                asker: None,
                logger: None,
            })
            .unwrap_err();

            assert!(
                matches!(
                    error,
                    IntegrationError::Receipt(receipt::ReceiptError::Corrupt { .. })
                ),
                "{case}: {error}"
            );
            assert_eq!(fs::read(&config).unwrap(), config_before, "{case}");
            assert_eq!(fs::read(&stable).unwrap(), bridge_before, "{case}");
            assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before, "{case}");
        }
    }
    #[test]
    fn uninstall_override_cannot_transfer_identical_nodes_between_paths() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config_a = temp.path().join("a/config.kdl");
        let config_b = temp.path().join("b/config.kdl");
        fs::create_dir_all(config_a.parent().unwrap()).unwrap();
        fs::create_dir_all(config_b.parent().unwrap()).unwrap();
        let wasm = b"wasm-v1";
        let mut inputs = install_inputs(temp.path(), wasm);
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config_a.clone());
        install(inputs).unwrap();
        fs::write(&config_b, fs::read(&config_a).unwrap()).unwrap();
        let stable = stable_bridge_path(temp.path());
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        let a_before = fs::read(&config_a).unwrap();
        let b_before = fs::read(&config_b).unwrap();
        let bridge_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: Some(config_b.clone()),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::ConfigOverrideUnowned { .. }
        ));
        assert_eq!(fs::read(&config_a).unwrap(), a_before);
        assert_eq!(fs::read(&config_b).unwrap(), b_before);
        assert_eq!(fs::read(&stable).unwrap(), bridge_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    }

    #[test]
    fn uninstall_exact_receipt_spelling_removes_owned_nodes() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("owned//config.kdl");
        fs::create_dir_all(config.parent().unwrap()).unwrap();
        fs::write(&config, "// unrelated configuration\n").unwrap();
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();

        let outcome = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: Some(config.clone()),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap();

        assert!(outcome.bridge_removed);
        assert_eq!(outcome.removed_nodes.len(), 2);
        assert!(outcome.receipt_removed);
        assert!(config.exists());
        let text = fs::read_to_string(&config).unwrap();
        assert!(text.starts_with("// unrelated configuration\n"));
        assert!(
            !text.lines().any(|line| line.trim() == "muxe"),
            "receipt-owned KDL nodes remain:\n{text}"
        );
    }

    #[test]
    fn uninstall_override_keeps_repeated_separator_spellings_distinct() {
        for case in ["repeated owned spelling", "plain owned spelling"] {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let plain = temp.path().join("a/config.kdl");
            let repeated = temp.path().join("a//config.kdl");
            assert_ne!(plain.as_os_str(), repeated.as_os_str());
            fs::create_dir_all(plain.parent().unwrap()).unwrap();
            let (owned, override_path) = match case {
                "repeated owned spelling" => (repeated, plain),
                "plain owned spelling" => (plain, repeated),
                _ => unreachable!(),
            };
            let mut inputs = install_inputs(temp.path(), b"wasm-v1");
            inputs.explicit_policy = Some(ConfigurationPolicy::Always);
            inputs.zellij_config = Some(owned.clone());
            install(inputs).unwrap();

            let stable = stable_bridge_path(temp.path());
            let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
            let config_before = fs::read(&owned).unwrap();
            let bridge_before = fs::read(&stable).unwrap();
            let receipt_before = fs::read(&receipt_path).unwrap();

            let error = uninstall(UninstallInputs {
                config_dir: temp.path(),
                cache_dir: &temp.path().join("cache"),
                zellij_config: Some(override_path),
                explicit_policy: Some(ConfigurationPolicy::Always),
                quiet: true,
                interactive: false,
                asker: None,
                logger: None,
            })
            .unwrap_err();

            assert!(
                matches!(error, IntegrationError::ConfigOverrideUnowned { .. }),
                "{case}: {error}"
            );
            assert_eq!(fs::read(&owned).unwrap(), config_before, "{case}");
            assert_eq!(fs::read(&stable).unwrap(), bridge_before, "{case}");
            assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before, "{case}");
        }
    }

    #[test]
    fn uninstall_override_with_trailing_separator_cannot_acquire_file_ownership() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("a/config.kdl");
        let trailing = temp.path().join("a/config.kdl/");
        assert_ne!(config.as_os_str(), trailing.as_os_str());
        fs::create_dir_all(config.parent().unwrap()).unwrap();
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();

        let stable = stable_bridge_path(temp.path());
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        let config_before = fs::read(&config).unwrap();
        let bridge_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();
        let error = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: Some(trailing),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::ConfigOverrideUnowned { .. }
        ));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), bridge_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    }

    #[test]
    fn uninstall_rejects_parent_traversal_without_touching_artifacts() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("owned/config.kdl");
        fs::create_dir_all(config.parent().unwrap()).unwrap();
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();
        let alias = temp.path().join("ancestor-symlink");
        std::os::unix::fs::symlink(config.parent().unwrap(), &alias).unwrap();
        let stable = stable_bridge_path(temp.path());
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        let config_before = fs::read(&config).unwrap();
        let bridge_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: Some(alias.join("../config.kdl")),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap_err();

        assert!(matches!(error, IntegrationError::ConfigDiscovery(_)));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), bridge_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    }

    #[test]
    fn uninstall_rejects_symlinked_ancestor_alias_without_touching_artifacts() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let config = temp.path().join("owned/config.kdl");
        fs::create_dir_all(config.parent().unwrap()).unwrap();
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();
        let alias = temp.path().join("ancestor-symlink");
        std::os::unix::fs::symlink(config.parent().unwrap(), &alias).unwrap();
        let stable = stable_bridge_path(temp.path());
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        let config_before = fs::read(&config).unwrap();
        let bridge_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: Some(alias.join("config.kdl")),
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::ConfigOverrideUnowned { .. }
        ));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), bridge_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
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

    #[test]
    fn uninstall_refuses_announced_exact_bridge_unit_before_mutation() {
        let temp = owner_temp();
        let config = temp.path().join("config.kdl");
        let cache = temp.path().join("cache");
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();
        let stable = stable_bridge_path(temp.path());
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        let journal_path = write_announced_journal(&cache, &stable);
        let config_before = fs::read(&config).unwrap();
        let stable_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::ActivationJournalLive { .. }
        ));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), stable_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
        assert!(journal_path.exists());
    }

    #[test]
    fn uninstall_refuses_bridge_committed_install_without_receipt_before_mutation() {
        let temp = owner_temp();
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.hooks.fail_after = Some(InstallStep::BridgeCommitted);
        assert!(matches!(
            install(inputs),
            Err(IntegrationError::FaultInjected {
                step: InstallStep::BridgeCommitted
            })
        ));
        let directory = integration_dir(temp.path());
        let stable = stable_bridge_path(temp.path());
        let journal = journal_path(&directory);
        let stable_before = fs::read(&stable).unwrap();
        let journal_before = fs::read(&journal).unwrap();

        let error = uninstall(UninstallInputs {
            config_dir: temp.path(),
            cache_dir: &temp.path().join("cache"),
            zellij_config: None,
            explicit_policy: Some(ConfigurationPolicy::Always),
            quiet: true,
            interactive: false,
            asker: None,
            logger: None,
        })
        .unwrap_err();

        assert!(matches!(error, IntegrationError::InstallJournalLive { .. }));
        assert_eq!(fs::read(&stable).unwrap(), stable_before);
        assert_eq!(fs::read(&journal).unwrap(), journal_before);
        assert!(receipt::load(&directory).unwrap().is_none());
    }

    #[test]
    fn uninstall_refuses_corrupt_activation_journal_before_mutation() {
        let temp = owner_temp();
        let config = temp.path().join("config.kdl");
        let cache = temp.path().join("cache");
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();
        let stable = stable_bridge_path(temp.path());
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        let unit = UnitKind::Zellij {
            bridge_path_hash: journal::unit_hash(&stable.display().to_string()),
        };
        let activation = journal::activation_dir(&cache);
        fsutil::ensure_owner_dir(&activation).unwrap();
        let corrupt = activation.join(unit.journal_name());
        fsutil::write_atomic(&corrupt, b"{not json", "activation").unwrap();
        let config_before = fs::read(&config).unwrap();
        let stable_before = fs::read(&stable).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::ActivationJournal(JournalError::Corrupt { .. })
        ));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), stable_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
        assert_eq!(fs::read(&corrupt).unwrap(), b"{not json");
    }

    #[test]
    fn mismatched_rollback_copy_refuses_before_kdl_mutation() {
        let temp = owner_temp();
        let config = temp.path().join("config.kdl");
        for wasm in [b"wasm-v1".as_slice(), b"wasm-v2".as_slice()] {
            let mut inputs = install_inputs(temp.path(), wasm);
            inputs.explicit_policy = Some(ConfigurationPolicy::Always);
            inputs.zellij_config = Some(config.clone());
            install(inputs).unwrap();
        }
        let stable = stable_bridge_path(temp.path());
        let previous = bridge::previous_path(&stable);
        let receipt_path = integration_dir(temp.path()).join(receipt::RECEIPT_FILE_NAME);
        fs::write(&previous, b"user backup").unwrap();
        let config_before = fs::read(&config).unwrap();
        let stable_before = fs::read(&stable).unwrap();
        let previous_before = fs::read(&previous).unwrap();
        let receipt_before = fs::read(&receipt_path).unwrap();

        let error = uninstall(uninstall_inputs(
            temp.path(),
            &temp.path().join("cache"),
            config.clone(),
        ))
        .unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::Bridge(bridge::BridgeError::PreviousProtected { .. })
        ));
        assert_eq!(fs::read(&config).unwrap(), config_before);
        assert_eq!(fs::read(&stable).unwrap(), stable_before);
        assert_eq!(fs::read(&previous).unwrap(), previous_before);
        assert_eq!(fs::read(&receipt_path).unwrap(), receipt_before);
    }

    #[test]
    fn retry_converges_after_bridge_removal_before_receipt_commit() {
        for (case, reinstall) in [
            ("stable absent", false),
            ("stable and previous absent", true),
        ] {
            let temp = owner_temp();
            let config = temp.path().join("config.kdl");
            let cache = temp.path().join("cache");
            fs::write(
                &config,
                "plugins {\n    other location=\"file:/other.wasm\"\n}\nload_plugins {\n    other\n}\n",
            )
            .unwrap();
            let mut inputs = install_inputs(temp.path(), b"wasm-v1");
            inputs.explicit_policy = Some(ConfigurationPolicy::Always);
            inputs.zellij_config = Some(config.clone());
            install(inputs).unwrap();
            if reinstall {
                let mut inputs = install_inputs(temp.path(), b"wasm-v2");
                inputs.explicit_policy = Some(ConfigurationPolicy::Always);
                inputs.zellij_config = Some(config.clone());
                install(inputs).unwrap();
            }
            let directory = integration_dir(temp.path());
            let stable = stable_bridge_path(temp.path());
            let previous = bridge::previous_path(&stable);
            assert_eq!(previous.exists(), reinstall, "{case}");

            set_fail_uninstall_after_bridge_removal(true);
            let error =
                uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap_err();
            set_fail_uninstall_after_bridge_removal(false);
            assert!(matches!(error, IntegrationError::UninstallFaultInjected));
            assert!(!stable.exists(), "{case}");
            assert!(!previous.exists(), "{case}");
            assert_created_nodes_removed_with_user_text(&config);
            assert!(receipt::load(&directory).unwrap().is_some());

            let outcome = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap();

            assert!(outcome.receipt_removed, "{case}");
            assert_created_nodes_removed_with_user_text(&config);
        }
    }

    #[test]
    fn dangling_stable_replacement_keeps_receipt_provenance() {
        let temp = owner_temp();
        let config = temp.path().join("config.kdl");
        let cache = temp.path().join("cache");
        let mut inputs = install_inputs(temp.path(), b"wasm-v1");
        inputs.explicit_policy = Some(ConfigurationPolicy::Always);
        inputs.zellij_config = Some(config.clone());
        install(inputs).unwrap();
        let directory = integration_dir(temp.path());
        let stable = stable_bridge_path(temp.path());

        set_fail_uninstall_after_bridge_removal(true);
        let error = uninstall(uninstall_inputs(temp.path(), &cache, config.clone())).unwrap_err();
        set_fail_uninstall_after_bridge_removal(false);
        assert!(matches!(error, IntegrationError::UninstallFaultInjected));
        assert!(bridge_absent(&stable).unwrap());

        let replacement = temp.path().join("user-bridge.wasm");
        std::os::unix::fs::symlink(&replacement, &stable).unwrap();
        let error = uninstall(uninstall_inputs(temp.path(), &cache, config)).unwrap_err();

        assert!(matches!(
            error,
            IntegrationError::Bridge(bridge::BridgeError::UnsafeDestination { .. })
        ));
        assert!(!bridge_absent(&stable).unwrap());
        assert!(receipt::load(&directory).unwrap().is_some());
        assert!(
            fs::symlink_metadata(&stable)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
    #[test]
    fn replacing_blocked_integration_lock_fails_closed_without_split_authority() {
        let temp = owner_temp();
        let directory = integration_dir(temp.path());
        let holder = acquire_integration_lock(&directory).unwrap();
        let path = integration_lock_path(&directory).unwrap();
        let inode_a = std::os::unix::fs::MetadataExt::ino(&fs::metadata(&path).unwrap());
        let (opened_tx, opened_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let waiter_directory = directory.clone();
            let waiter = scope.spawn(move || {
                let (path, file) = open_integration_lock(&waiter_directory).unwrap();
                opened_tx.send(()).unwrap();
                let result = lock_integration_file(path, file).map(|_| ());
                result_tx.send(result).unwrap();
            });
            opened_rx.recv().unwrap();

            let replacement = directory.join(".replacement-lock");
            drop(fsutil::open_owner_file(&replacement, false).unwrap());
            let inode_b = std::os::unix::fs::MetadataExt::ino(&fs::metadata(&replacement).unwrap());
            assert_ne!(inode_a, inode_b);
            fs::rename(&replacement, &path).unwrap();
            drop(holder);

            let result = result_rx.recv().unwrap();
            assert!(matches!(
                result,
                Err(IntegrationLockError::Fs(FsError::PathChanged { .. }))
            ));
            waiter.join().unwrap();

            match try_acquire_integration_lock(&directory).unwrap() {
                IntegrationLockAttempt::Acquired(lock) => drop(lock),
                IntegrationLockAttempt::Active => {
                    panic!("fresh contender unexpectedly saw the replaced inode as active")
                }
            }
        });
    }

    #[test]
    fn integration_lock_serializes_install_and_uninstall_authority() {
        let temp = owner_temp();
        let config = temp.path().join("config.kdl");
        fs::write(&config, "// user configuration\n").unwrap();
        let directory = integration_dir(temp.path());
        let stable = stable_bridge_path(temp.path());
        let journal = journal_path(&directory);
        let reached = std::sync::Arc::new(std::sync::Barrier::new(2));
        let release = std::sync::Arc::new(std::sync::Barrier::new(2));
        let gate = install_gate(
            InstallStep::JournalWritten,
            reached.clone(),
            release.clone(),
        );
        let cache = temp.path().join("cache");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        std::thread::scope(|scope| {
            let install_root = temp.path();
            let install_config = config.clone();
            let install_gate = gate.clone();
            let install_thread = scope.spawn(move || {
                let mut inputs = install_inputs(install_root, b"wasm-v1");
                inputs.explicit_policy = Some(ConfigurationPolicy::Always);
                inputs.zellij_config = Some(install_config);
                inputs.hooks.gate = Some(install_gate);
                install(inputs)
            });
            reached.wait();
            assert_integration_lock_active(&directory);
            assert!(journal.exists());
            assert!(!stable.exists());

            let uninstall_config = config.clone();
            let uninstall_root = temp.path();
            let uninstall_cache = cache.clone();
            let uninstall_thread = scope.spawn(move || {
                started_tx.send(()).unwrap();
                let result = uninstall(uninstall_inputs(
                    uninstall_root,
                    &uninstall_cache,
                    uninstall_config,
                ))
                .map_err(|error| error.to_string());
                result_tx.send(result).unwrap();
            });
            started_rx.recv().unwrap();
            assert!(matches!(
                result_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            assert!(!stable.exists());
            assert!(receipt::load(&directory).unwrap().is_none());

            release.wait();
            let install_result = install_thread.join().unwrap().unwrap();
            let uninstall_result = result_rx.recv().unwrap().unwrap();
            uninstall_thread.join().unwrap();
            assert_eq!(install_result.bridge_digest, fsutil::sha256_hex(b"wasm-v1"));
            assert!(uninstall_result.bridge_removed);
            assert!(uninstall_result.receipt_removed);
        });

        assert!(!stable.exists());
        assert!(receipt::load(&directory).unwrap().is_none());
        assert!(integration_lock_path(&directory).unwrap().exists());
    }

    #[test]
    fn uninstall_lock_blocks_install_until_fresh_receipt_recheck() {
        let temp = owner_temp();
        let config = temp.path().join("config.kdl");
        fs::write(&config, "// user configuration\n").unwrap();
        let cache = temp.path().join("cache");
        let mut initial = install_inputs(temp.path(), b"wasm-v1");
        initial.explicit_policy = Some(ConfigurationPolicy::Always);
        initial.zellij_config = Some(config.clone());
        install(initial).unwrap();
        let directory = integration_dir(temp.path());
        let stable = stable_bridge_path(temp.path());
        let original_receipt = receipt::load(&directory).unwrap().unwrap();
        let reached = std::sync::Arc::new(std::sync::Barrier::new(2));
        let release = std::sync::Arc::new(std::sync::Barrier::new(2));
        set_uninstall_gate(Some(uninstall_gate(
            directory.clone(),
            reached.clone(),
            release.clone(),
        )));

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (install_started_tx, install_started_rx) = std::sync::mpsc::channel();
        let (install_result_tx, install_result_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let uninstall_config = config.clone();
            let uninstall_root = temp.path();
            let uninstall_cache = cache.clone();
            let uninstall_thread = scope.spawn(move || {
                started_tx.send(()).unwrap();
                uninstall(uninstall_inputs(
                    uninstall_root,
                    &uninstall_cache,
                    uninstall_config,
                ))
            });
            started_rx.recv().unwrap();
            reached.wait();
            assert_integration_lock_active(&directory);
            assert_eq!(fs::read(&stable).unwrap(), b"wasm-v1");
            assert_eq!(
                receipt::load(&directory)
                    .unwrap()
                    .unwrap()
                    .bridge
                    .installed_digest,
                original_receipt.bridge.installed_digest
            );

            let install_config = config.clone();
            let install_root = temp.path();
            let install_thread = scope.spawn(move || {
                let blocked = matches!(
                    try_acquire_integration_lock(&integration_dir(install_root)).unwrap(),
                    IntegrationLockAttempt::Active
                );
                install_started_tx.send(blocked).unwrap();
                let mut inputs = install_inputs(install_root, b"wasm-v2");
                inputs.explicit_policy = Some(ConfigurationPolicy::Always);
                inputs.zellij_config = Some(install_config);
                let result = install(inputs).map_err(|error| error.to_string());
                install_result_tx.send(result).unwrap();
            });
            assert!(install_started_rx.recv().unwrap());
            assert!(matches!(
                install_result_rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            release.wait();
            let uninstall_result = uninstall_thread.join().unwrap().unwrap();
            let install_result = install_result_rx.recv().unwrap().unwrap();
            install_thread.join().unwrap();
            assert!(uninstall_result.receipt_removed);
            assert_eq!(install_result.bridge_digest, fsutil::sha256_hex(b"wasm-v2"));
        });
        set_uninstall_gate(None);

        assert_eq!(fs::read(&stable).unwrap(), b"wasm-v2");
        let receipt = receipt::load(&directory).unwrap().unwrap();
        assert_eq!(
            receipt.bridge.installed_digest,
            Sha256Digest::from_bytes(b"wasm-v2")
        );
    }
}
