//! Activation, upgrade, and rollback transactions.
//!
//! `muxe activate` treats its own executable and packaged assets as the target
//! version. Host selection defaults to every live host in the owner-only
//! broker registry; `--host current` requires invocation from a managed host,
//! while `zellij` and `herdr` limit activation to that host kind.
//!
//! Every selected unit is preflighted before any unit mutates. Units commit
//! independently after global preflight: one Herdr broker is one unit, and all
//! live Zellij brokers sharing one canonical stable WASM path form one atomic
//! group. A failure in any group member aborts the complete Zellij group and
//! restores the one old bridge backup across every switched session.
//!
//! # Retained control sessions
//!
//! The old broker closes and unlinks its listener at prepare while retaining
//! the accepted coordinator stream. The coordinator therefore holds one
//! connected session per member across status, prepare, commit, and abort:
//! commit and abort never connect fresh (a fresh connection would reach the
//! target or fail). Target brokers claim the normal per-host endpoint under
//! the startup lock — never an invented separate socket — so post-spawn
//! connections to the recorded path reach the target.
//!
//! # Recovery
//!
//! Recovery never runs both host adapters concurrently and never picks a
//! stack by version ordering. A ready unit commits (commit is idempotent, so
//! targets that already self-committed simply acknowledge); anything else
//! restores the complete recorded old unit: targets are shut down over the
//! wire, the verified bridge backup is restored and reloaded in every
//! recorded session, and then old brokers resume. Rollback reports the
//! original failure plus every rollback failure; the journal is preserved on
//! any ambiguity.
//!
//! Split of responsibilities: the broker owns drain, supervision, and
//! control-protocol serving (including the target-shutdown and old-reacquire
//! interpretation of abort, and target readiness reporting). This module owns
//! preflight orchestration, the journals, the bridge swap and reload fan-out,
//! commit/abort sequencing, and crash recovery.

use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use muxe_protocol::control::{
    ActivationStatus, AsOfTick, CompatibilityRecord, HandoffId, LifecycleState,
    PrepareHandoffProtocol, TargetReadiness, UnitReadinessEpochId,
};
use muxe_protocol::wire::HostKind;
use thiserror::Error;

use crate::{
    cli::HostScope,
    compatibility,
    fsutil::{self, FsError},
    integration::{self, receipt::Sha256Digest},
    logging::Logger,
    paths::BridgeIdentity,
};

use super::{
    control::{ControlClient, ControlError},
    journal::{
        self, ActivationId, ActivationJournal, ActivationMemberId, BridgeArtifactId,
        BridgeArtifactRole, BridgeArtifacts, BridgeProgress, JournalError, MemberEndpoint,
        MemberLaunchAuthority, MemberTransactionId, OldMemberProgress, TargetMemberProgress,
        TargetProcessId, TargetRetirementAuthority, TargetRetirementIntent, TransactionDirective,
        TransactionMember, UnitKind, unit_hash,
    },
    registry::{
        BridgeMemberId, BrokerEntry, MemberCensus, RegisteredBroker, Registry, RegistryError,
    },
};

/// Activation transaction boundaries for failure injection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivateStep {
    PreflightDone,
    JournalWritten,
    ArtifactsReady,
    OldPrepared,
    TargetSpawned,
    BridgeSwapped,
    OldInstallAppliedBeforeOutcome,
    ReloadIssued,
    ReadinessRecorded,
    ReceiptUpdated,
    TerminalWritten,
    TerminalCleaned,
    Committed,
}

/// Failure-injection hooks. Production passes `ActivateHooks::default()`.
#[derive(Clone, Debug, Default)]
pub struct ActivateHooks {
    /// When set, activation fails with `FaultInjected` right after the step.
    pub fail_after: Option<ActivateStep>,
}

impl ActivateHooks {
    fn check(&self, step: ActivateStep) -> Result<(), ActivateError> {
        if self.fail_after == Some(step) {
            return Err(ActivateError::FaultInjected { step });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ActivateError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(transparent)]
    Path(#[from] crate::paths::PathError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Receipt(#[from] crate::integration::receipt::ReceiptError),
    #[error(transparent)]
    Asset(#[from] compatibility::AssetVerificationError),
    #[error(transparent)]
    Bridge(#[from] crate::integration::bridge::BridgeError),
    #[error("activation preflight failed: {0}")]
    Preflight(String),
    #[error("no live brokers match the selected host scope")]
    NoLiveUnits,
    #[error("--host current requires invocation from a managed host")]
    CurrentHostRequired,
    #[error("fault injected after {step:?} (test hook)")]
    FaultInjected { step: ActivateStep },
    #[error("target broker spawn failed: {0}")]
    Spawn(String),
    #[error("target process state inspection failed: {0}")]
    TargetStopInspect(String),
    #[error("target process termination failed: {0}")]
    TargetStopKill(String),
    #[error("target process reap failed: {0}")]
    TargetStopWait(String),
    #[error("bridge reload failed for session {session}: {detail}")]
    Reload { session: String, detail: String },
    #[error("readiness wait timed out for {identity}")]
    ReadinessTimeout { identity: String },
    #[error("unit failed: {reason}")]
    UnitFailed { reason: String },
    #[error("auditable operation cannot proceed without its log record")]
    Audit(#[from] crate::logging::LogError),
}

/// The invoking host for `--host current` scoping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DetectedHost {
    Zellij {
        session: String,
        bridge_identity: BridgeIdentity,
    },
    Herdr {
        discovery_key: String,
    },
}

/// Candidate replacement bridge bytes.
///
/// Activation verifies these against the executable's embedded producer digest
/// before preparing any selected Zellij unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedBridge {
    pub bytes: Vec<u8>,
}

/// One retained coordinator control session.
///
/// Sessions are held across status, prepare, commit, and abort for a single
/// member because the old broker unlinks its listener at prepare: only the
/// retained stream still reaches the old broker afterwards.
#[expect(
    async_fn_in_trait,
    reason = "coordinator traits use static dispatch with one implementation per process; no Send bound is required"
)]
pub trait ControlSession {
    async fn status(&mut self) -> Result<ActivationStatus, ControlError>;
    async fn status_at(
        &mut self,
        _handoff: &HandoffId,
        _epoch: UnitReadinessEpochId,
        _as_of: AsOfTick,
    ) -> Result<ActivationStatus, ControlError> {
        Err(ControlError::Rejected {
            diagnostic: "peer does not support an as-of readiness proof".to_owned(),
        })
    }
    async fn prepare(
        &mut self,
        target: &CompatibilityRecord,
        handoff: &HandoffId,
    ) -> Result<ActivationStatus, ControlError>;
    async fn commit(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError>;
    async fn abort(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError>;
    async fn retire(&mut self) -> Result<ActivationStatus, ControlError>;
}

impl ControlSession for ControlClient {
    async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
        ControlClient::status(self).await
    }
    async fn status_at(
        &mut self,
        handoff: &HandoffId,
        epoch: UnitReadinessEpochId,
        as_of: AsOfTick,
    ) -> Result<ActivationStatus, ControlError> {
        ControlClient::status_at(self, *handoff, epoch, as_of).await
    }
    async fn prepare(
        &mut self,
        target: &CompatibilityRecord,
        handoff: &HandoffId,
    ) -> Result<ActivationStatus, ControlError> {
        ControlClient::prepare(self, target.clone(), *handoff).await
    }
    async fn commit(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
        ControlClient::commit(self, *handoff).await
    }
    async fn abort(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
        ControlClient::abort(self, *handoff).await
    }
    async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
        ControlClient::retire(self).await
    }
}

/// Factory for retained coordinator control sessions.
#[expect(
    async_fn_in_trait,
    reason = "coordinator traits use static dispatch with one implementation per process; no Send bound is required"
)]
pub trait ControlPort {
    type Session: ControlSession;
    async fn connect(&self, socket: &Path) -> Result<Self::Session, ControlError>;
    /// A retained stream's Status plus authenticated peer and socket identity.
    /// Non-live fixtures must supply their own owned stream authority explicitly.
    async fn verified_status(
        &self,
        _socket: &Path,
    ) -> Result<super::control::VerifiedControlStatus, ControlError> {
        Err(ControlError::PeerIdentity)
    }
}

/// Production control port: real framing over owner-only sockets.
#[derive(Clone, Copy, Debug)]
pub struct LiveControl;

impl ControlPort for LiveControl {
    type Session = ControlClient;
    async fn connect(&self, socket: &Path) -> Result<ControlClient, ControlError> {
        ControlClient::connect(socket).await
    }
    async fn verified_status(
        &self,
        socket: &Path,
    ) -> Result<super::control::VerifiedControlStatus, ControlError> {
        let mut client = ControlClient::connect(socket).await?;
        client.verified_status().await
    }
}

/// Exact selected unit and typed observed row authority for target spawning.
/// A persisted host label is wrapped and checked before reaching the renderer.
#[derive(Clone, Debug)]
pub struct SpawnMember<'a> {
    pub unit: &'a UnitKind,
    pub authority: MemberLaunchAuthority,
    pub observed_host: HostKind,
    pub observed_bridge_identity: Option<&'a BridgeIdentity>,
    pub observed_bridge_member: Option<&'a BridgeMemberId>,
    pub observed_handoff_id: Option<HandoffId>,
    /// Exact journal path resolved by the coordinator under unit authority.
    pub journal_path: PathBuf,
}

/// Owned request to start one target broker: the exact executable plus the
/// broker-authored argument vector. Constructed by the caller (the current
/// executable plus the broker's internal serve mode), never from ambient
/// environment fallbacks.
#[derive(Clone, Debug)]
pub struct SpawnRequest {
    pub program: PathBuf,
    pub args: Vec<OsString>,
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetStopFault {
    Inspect,
    Kill,
    Wait,
}

/// Handle to a running target broker: the retained owned child.
#[derive(Debug)]
pub struct TargetHandle {
    pub child: std::process::Child,
    cleanup_on_drop: bool,
    #[cfg(test)]
    stop_fault: Option<TargetStopFault>,
}

impl TargetHandle {
    #[must_use]
    pub fn new(child: std::process::Child) -> Self {
        Self {
            child,
            cleanup_on_drop: true,
            #[cfg(test)]
            stop_fault: None,
        }
    }

    pub(crate) fn surrender_to_live_broker(&mut self) {
        self.cleanup_on_drop = false;
    }
}

impl Drop for TargetHandle {
    fn drop(&mut self) {
        if self.cleanup_on_drop && !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Exact transaction member paired with the coordinator-owned target child.
#[derive(Debug)]
struct OwnedTarget {
    member: MemberTransactionId,
    handle: TargetHandle,
}

/// Bounded owner for one activation unit. It never outlives the coordinator
/// invocation: unresolved children are killed and reaped by `TargetHandle`
/// when this owner drops. No process-global owner can orphan them at exit.
#[derive(Debug)]
struct ActivationSupervisor {
    cache_dir: PathBuf,
    unit: UnitKind,
    targets: Vec<OwnedTarget>,
}

impl ActivationSupervisor {
    fn new(
        cache_dir: &Path,
        unit: &UnitKind,
        targets: Vec<OwnedTarget>,
    ) -> Result<Self, (ActivateError, Vec<OwnedTarget>)> {
        let duplicate = {
            let mut members = std::collections::HashSet::with_capacity(targets.len());
            targets.iter().any(|target| !members.insert(&target.member))
        };
        if duplicate {
            return Err((
                ActivateError::UnitFailed {
                    reason: "duplicate owned target for one activation member".to_owned(),
                },
                targets,
            ));
        }
        Ok(Self {
            cache_dir: cache_dir.to_path_buf(),
            unit: unit.clone(),
            targets,
        })
    }

    fn check_scope(
        &self,
        cache_dir: &Path,
        journal: &ActivationJournal,
    ) -> Result<(), ActivateError> {
        if self.cache_dir != cache_dir || self.unit != journal.unit {
            return Err(ActivateError::UnitFailed {
                reason: "target supervisor does not own this cache and activation unit".to_owned(),
            });
        }
        Ok(())
    }

    fn process_id(
        &self,
        member: &MemberTransactionId,
    ) -> Result<Option<TargetProcessId>, ActivateError> {
        self.targets
            .iter()
            .find(|target| &target.member == member)
            .map(|target| {
                TargetProcessId::new(target.handle.child.id()).map_err(ActivateError::from)
            })
            .transpose()
    }

    fn stop(
        &mut self,
        member: &MemberTransactionId,
        process_id: TargetProcessId,
        spawner: &impl BrokerSpawner,
    ) -> Result<(), ActivateError> {
        let target = self
            .targets
            .iter_mut()
            .find(|target| &target.member == member)
            .ok_or_else(|| ActivateError::UnitFailed {
                reason: "owned target process authority is unavailable".to_owned(),
            })?;
        if target.handle.child.id() != process_id.get() {
            return Err(ActivateError::UnitFailed {
                reason: "owned target process authority changed".to_owned(),
            });
        }
        spawner.stop_target(&mut target.handle)
    }

    fn release(&mut self, member: &MemberTransactionId) -> Result<(), ActivateError> {
        let index = self
            .targets
            .iter()
            .position(|target| &target.member == member)
            .ok_or_else(|| ActivateError::UnitFailed {
                reason: "owned target disappeared before receipt durability".to_owned(),
            })?;
        self.targets.remove(index);
        Ok(())
    }

    /// Stops every still-owned child before returning a terminal rollback
    /// outcome. Drop is only a last-resort fallback if a task is cancelled.
    fn shutdown(self, spawner: &impl BrokerSpawner) -> Vec<String> {
        Self::shutdown_targets(self.targets, spawner)
    }

    fn shutdown_targets(targets: Vec<OwnedTarget>, spawner: &impl BrokerSpawner) -> Vec<String> {
        let mut diagnostics = Vec::new();
        for mut target in targets {
            let pid = target.handle.child.id();
            diagnostics.push(format!(
                "shutdown unresolved target member {:?} pid {pid} without retirement receipt",
                target.member
            ));
            if let Err(error) = spawner.stop_target(&mut target.handle) {
                diagnostics.push(format!(
                    "shutdown owned target member {:?} pid {pid}: {error}",
                    target.member
                ));
            }
            // A successful stop is already reaped. On error the handle's
            // Drop retries best-effort, without suppressing this diagnostic.
        }
        diagnostics
    }

    #[cfg(test)]
    fn is_live(&mut self, member: &MemberTransactionId) -> bool {
        self.targets
            .iter_mut()
            .find(|target| &target.member == member)
            .is_some_and(|target| matches!(target.handle.child.try_wait(), Ok(None)))
    }
}

/// Starts and stops target brokers. Only owns process mechanics; the argv it
/// executes comes from the caller-owned [`SpawnRequest`].
pub trait BrokerSpawner {
    /// Spawns one target broker child from a caller-owned argv.
    ///
    /// # Errors
    ///
    /// Returns [`ActivateError`] when the child process cannot be spawned.
    fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError>;
    /// Stops and reaps a spawner-owned target child without surrendering the
    /// handle until the wait barrier succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`ActivateError`] when process-state inspection, signalling, or
    /// waiting is ambiguous. The caller retains the handle for retry.
    fn stop_target(&self, handle: &mut TargetHandle) -> Result<(), ActivateError>;
}

/// Production spawner: real process spawn with a retained owned child and an
/// explicit endpoint. No process-name or global cleanup, ever.
#[derive(Clone, Copy, Debug)]
pub struct ProcessSpawner;

impl BrokerSpawner for ProcessSpawner {
    fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
        use std::os::unix::process::CommandExt as _;
        let child = std::process::Command::new(&request.program)
            .args(&request.args)
            // Broker children must outlive the launching UI pane. Zellij
            // closes that pane's foreground process group on dismissal; a
            // distinct group keeps the registered ordinary broker alive.
            .process_group(0)
            .spawn()
            .map_err(|source| ActivateError::Spawn(source.to_string()))?;
        Ok(TargetHandle::new(child))
    }

    fn stop_target(&self, handle: &mut TargetHandle) -> Result<(), ActivateError> {
        #[cfg(test)]
        if handle.stop_fault == Some(TargetStopFault::Inspect) {
            return Err(ActivateError::TargetStopInspect(
                "injected try_wait failure".to_owned(),
            ));
        }
        if handle
            .child
            .try_wait()
            .map_err(|source| ActivateError::TargetStopInspect(source.to_string()))?
            .is_none()
        {
            #[cfg(test)]
            if handle.stop_fault == Some(TargetStopFault::Kill) {
                return Err(ActivateError::TargetStopKill(
                    "injected kill failure".to_owned(),
                ));
            }
            handle
                .child
                .kill()
                .map_err(|source| ActivateError::TargetStopKill(source.to_string()))?;
        }
        #[cfg(test)]
        if handle.stop_fault == Some(TargetStopFault::Wait) {
            return Err(ActivateError::TargetStopWait(
                "injected wait failure".to_owned(),
            ));
        }
        handle
            .child
            .wait()
            .map(|_| ())
            .map_err(|source| ActivateError::TargetStopWait(source.to_string()))
    }
}

/// Reloads the stable bridge inside live Zellij sessions.
pub trait HostReloader {
    /// Runs the per-session reload command once for every participating
    /// session. Any session failure aborts the complete Zellij group.
    ///
    /// # Errors
    ///
    /// Returns [`ActivateError`] when any session reload fails.
    fn reload_bridge(&self, session: &str, bridge_url: &str) -> Result<(), ActivateError>;
    /// Reports whether the session already hosts the managed bridge plugin.
    /// Coldstart can reuse a compatible autoloaded bridge without destroying
    /// its prior focus history; broker readiness still verifies its handshake.
    ///
    /// # Errors
    ///
    /// Returns [`ActivateError`] when the loaded-plugin inventory or its
    /// managed location cannot be verified.
    fn bridge_loaded(&self, session: &str, bridge_url: &str) -> Result<bool, ActivateError>;
}

/// Production reloader: runs the real Zellij CLI once per session.
///
/// ```text
/// zellij --session {session} action start-or-reload-plugin {bridge_url}
/// ```
/// Production use awaits the Zellij runtime probe permission grant; the
/// command itself touches only the named session.
#[derive(Clone, Debug)]
pub struct ZellijCliReloader {
    pub program: Option<PathBuf>,
}

#[derive(serde::Deserialize)]
struct LoadedZellijPane {
    is_plugin: bool,
    plugin_url: Option<String>,
    exited: bool,
}

/// Zellij reports an autoloaded plugin by alias, not its in-memory URL.
/// The owner-only receipt, stable digest and current KDL mapping constrain
/// that alias to the canonical bridge; the broker's fresh handshake remains
/// the authority for the code actually running in the plugin.
fn verify_managed_zellij_alias(bridge_url: &str) -> Result<(), String> {
    let stable = Path::new(
        bridge_url
            .strip_prefix("file:")
            .ok_or_else(|| "managed bridge URL is not file-based".to_owned())?,
    );
    let directory = stable
        .parent()
        .ok_or_else(|| "managed bridge URL has no directory".to_owned())?;
    let identity = BridgeIdentity::resolve_existing(
        directory,
        std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
    )
    .map_err(|error| format!("cannot resolve managed bridge location: {error}"))?;
    if identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME)) != stable {
        return Err("loaded alias points outside the canonical bridge location".to_owned());
    }
    let receipt = integration::receipt::load(identity.directory())
        .map_err(|error| format!("cannot read integration receipt: {error}"))?
        .ok_or_else(|| "loaded alias has no managed integration receipt".to_owned())?;
    if receipt.bridge.bridge_identity != identity {
        return Err("loaded alias disagrees with the receipt bridge identity".to_owned());
    }
    let installed = fsutil::read_owner_file(stable)
        .map_err(|error| format!("cannot read owner-only loaded bridge: {error}"))?;
    if integration::receipt::Sha256Digest::from_bytes(&installed) != receipt.bridge.installed_digest
    {
        return Err("loaded alias bridge bytes differ from the receipt".to_owned());
    }
    let selected_config = std::env::var_os("ZELLIJ_CONFIG_FILE")
        .map(|value| {
            let path = PathBuf::from(value);
            if !path.is_absolute() {
                return Err("ZELLIJ_CONFIG_FILE is not absolute".to_owned());
            }
            crate::paths::ConfigPath::from_input(&path)
                .map_err(|error| format!("invalid ZELLIJ_CONFIG_FILE: {error}"))
        })
        .transpose()?;
    let mut verified = 0_usize;
    for alias in receipt
        .configs
        .iter()
        .filter(|record| record.node == integration::receipt::ManagedNode::PluginsAlias)
        .filter(|record| {
            selected_config
                .as_ref()
                .is_none_or(|path| &record.config_path == path)
        })
    {
        if !receipt.configs.iter().any(|record| {
            record.node == integration::receipt::ManagedNode::LoadPluginsEntry
                && record.config_path == alias.config_path
        }) {
            return Err(format!(
                "loaded alias has no paired autoload record in {}",
                alias.config_path
            ));
        }
        let config_path = alias.config_path.to_path_buf();
        let planned = integration::kdl::read_and_plan(&config_path, bridge_url)
            .map_err(|error| format!("cannot inspect recorded Zellij config: {error}"))?;
        if !planned.plan().already_correct {
            return Err(format!(
                "loaded alias in {} no longer maps to the canonical bridge URL",
                alias.config_path
            ));
        }
        verified += 1;
    }
    if verified == 0 {
        return Err("loaded alias has no receipt-backed config for this host".to_owned());
    }
    Ok(())
}

impl HostReloader for ZellijCliReloader {
    fn reload_bridge(&self, session: &str, bridge_url: &str) -> Result<(), ActivateError> {
        let program = self.program.as_ref().ok_or_else(|| ActivateError::Reload {
            session: session.to_owned(),
            detail: "no Zellij executable is installed; cannot reload the bridge".to_owned(),
        })?;
        let output = std::process::Command::new(program)
            .args([
                "--session",
                session,
                "action",
                "start-or-reload-plugin",
                bridge_url,
            ])
            .output()
            .map_err(|source| ActivateError::Reload {
                session: session.to_owned(),
                detail: source.to_string(),
            })?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr);
            let detail = detail.trim();
            let detail = if detail.is_empty() {
                format!("exit {}", output.status)
            } else {
                detail.chars().take(512).collect()
            };
            return Err(ActivateError::Reload {
                session: session.to_owned(),
                detail,
            });
        }
        Ok(())
    }
    fn bridge_loaded(&self, session: &str, bridge_url: &str) -> Result<bool, ActivateError> {
        let program = self.program.as_ref().ok_or_else(|| ActivateError::Reload {
            session: session.to_owned(),
            detail: "no Zellij executable is installed; cannot inspect the bridge".to_owned(),
        })?;
        let output = std::process::Command::new(program)
            .args(["--session", session, "action", "list-panes", "--json"])
            .output()
            .map_err(|source| ActivateError::Reload {
                session: session.to_owned(),
                detail: source.to_string(),
            })?;
        if !output.status.success() {
            return Err(ActivateError::Reload {
                session: session.to_owned(),
                detail: format!(
                    "cannot inspect loaded plugins: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }
        let panes: Vec<LoadedZellijPane> =
            serde_json::from_slice(&output.stdout).map_err(|source| ActivateError::Reload {
                session: session.to_owned(),
                detail: format!("invalid loaded-plugin inventory: {source}"),
            })?;
        let mut active = panes.iter().filter(|pane| pane.is_plugin && !pane.exited);
        if active
            .clone()
            .any(|pane| pane.plugin_url.as_deref() == Some(bridge_url))
        {
            return Ok(true);
        }
        if active.any(|pane| pane.plugin_url.as_deref() == Some("muxe")) {
            verify_managed_zellij_alias(bridge_url).map_err(|detail| ActivateError::Reload {
                session: session.to_owned(),
                detail,
            })?;
            return Ok(true);
        }
        Ok(false)
    }
}

/// A selected, fixed-identity host owns its live version and action probes.
#[expect(
    async_fn_in_trait,
    reason = "the selected host and production preflight use static dispatch"
)]
pub trait HostPreflight {
    async fn validate_live_host(&self, live: &LivePreflight<'_>) -> Result<(), String>;
}

/// Global configuration check plus the selected host's concrete admission probe.
#[expect(
    async_fn_in_trait,
    reason = "coordinator traits use static dispatch with one implementation per process"
)]
pub trait Preflight {
    async fn validate_config(&self) -> Result<(), String>;
    async fn validate_host<H: HostPreflight>(&self, host: &H) -> Result<(), String>;
}
/// Production preflight: fail-fast configuration and host checks before any
/// unit mutates.
///
/// Full per-adapter validation still happens at target startup with rollback;
/// these gates reject an unreadable or invalid configuration, an unreachable
/// or incompatible Herdr host, and an out-of-policy host version early.
pub struct LivePreflight<'a> {
    /// Absolute Muxe configuration file the target brokers will serve.
    pub config_path: PathBuf,
    /// Cache base for Herdr schema records.
    pub cache_dir: PathBuf,
    /// Absolute pinned Herdr executable for live host probes. Required only
    /// when a Herdr unit is selected.
    pub herdr_binary: Option<PathBuf>,
    /// Absolute pinned Zellij executable for version probes. Required only
    /// when a Zellij unit is selected.
    pub zellij_exe: Option<PathBuf>,
    /// Persistent logger for policy warnings; failures always error.
    pub logger: Option<&'a Logger>,
}

impl LivePreflight<'_> {
    fn compile_config(&self) -> Result<muxe_core::CompiledConfig, String> {
        let yaml = std::fs::read_to_string(&self.config_path)
            .map_err(|error| format!("cannot read {}: {error}", self.config_path.display()))?;
        // Permissive key capabilities: preflight must never reject a form the
        // target accepts. Host-specific action validation happens at target
        // startup with rollback.
        let capabilities = muxe_core::KeyCapabilities {
            event_types: true,
            alternate_keys: true,
            all_keys_as_escape_codes: true,
        };
        muxe_core::compile_yaml(
            muxe_core::CompiledGeneration(1),
            muxe_core::SourceId::new(self.config_path.display().to_string()),
            yaml,
            capabilities,
            None,
        )
        .map_err(|diagnostics| {
            diagnostics
                .iter()
                .map(|diagnostic| diagnostic.message.clone())
                .collect::<Vec<_>>()
                .join("; ")
        })
    }

    fn herdr_runtime(
        &self,
        discovery_key: &str,
    ) -> impl Future<Output = Result<muxe_adapter_herdr::HerdrRuntime, String>> {
        let binary = self
            .herdr_binary
            .clone()
            .ok_or_else(|| "no Herdr executable is installed; cannot probe Herdr hosts".to_owned());
        let config = binary.map(|herdr_binary| muxe_adapter_herdr::HerdrAdapterConfig {
            socket_path: PathBuf::from(discovery_key),
            herdr_binary,
            cache_dir: self.cache_dir.clone(),
        });
        async move {
            let config = config?;
            muxe_adapter_herdr::HerdrRuntime::connect(config)
                .await
                .map_err(|error| {
                    if error.kind == muxe_adapter_api::AdapterErrorKind::Incompatible {
                        format!("Herdr host {discovery_key} is incompatible: {error}")
                    } else {
                        format!("Herdr host {discovery_key} is unreachable: {error}")
                    }
                })
        }
    }

    /// Splits `major.minor.patch` into comparable numbers.
    fn version_numbers(version: &str) -> Option<(u64, u64, u64)> {
        let mut parts = version.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some((major, minor, patch))
    }

    fn warn(&self, host: &str, message: String) {
        if let Some(logger) = &self.logger
            && let Ok(event) = crate::logging::LogEvent::new(
                env!("CARGO_PKG_VERSION"),
                host,
                "activate-preflight",
                message,
            )
        {
            let _ = logger.append(&event);
        }
    }

    fn version_policy(&self) -> Result<muxe_core::HostVersionCheck, String> {
        self.compile_config()
            .map_err(|error| format!("cannot read version policy: {error}"))
            .map(|config| config.host.version_check)
    }

    fn check_version(
        &self,
        host: &str,
        discovery_key: &str,
        live: &str,
        minimum: &str,
        latest: &str,
        policy: muxe_core::HostVersionCheck,
    ) -> Result<(), String> {
        let live_numbers = Self::version_numbers(live).ok_or_else(|| {
            format!("host {discovery_key} reports an unparsable version {live:?}")
        })?;
        let minimum_numbers = Self::version_numbers(minimum).expect("embedded minimum is semver");
        let latest_numbers = Self::version_numbers(latest).expect("embedded latest is semver");
        if live_numbers < minimum_numbers {
            return Err(match policy {
                muxe_core::HostVersionCheck::Off => format!(
                    "host {discovery_key} runs {live}, below minimum {minimum}; refusing even with version gating off because protocol checks cannot pass"
                ),
                _ => format!("host {discovery_key} runs {live}, below minimum {minimum}"),
            });
        }
        if live_numbers > latest_numbers {
            let message =
                format!("host {discovery_key} runs {live}, newer than latest verified {latest}");
            match policy {
                muxe_core::HostVersionCheck::Strict => return Err(message),
                muxe_core::HostVersionCheck::Min => self.warn(host, message),
                muxe_core::HostVersionCheck::Off => {}
            }
        }
        Ok(())
    }
}
impl Preflight for LivePreflight<'_> {
    async fn validate_config(&self) -> Result<(), String> {
        self.compile_config().map(|_| ())
    }

    async fn validate_host<H: HostPreflight>(&self, host: &H) -> Result<(), String> {
        host.validate_live_host(self).await
    }
}

/// One planned activation unit.
#[derive(Clone, Debug)]
pub(crate) enum PlannedUnit {
    Herdr {
        entry: RegisteredBroker,
    },
    Zellij {
        bridge_identity: BridgeIdentity,
        entries: Vec<RegisteredBroker>,
        census: MemberCensus,
    },
}

impl PlannedUnit {
    fn unit_kind(&self) -> UnitKind {
        match self {
            Self::Herdr { entry } => UnitKind::Herdr {
                host_hash: unit_hash(entry.discovery_key().as_str()),
            },
            Self::Zellij {
                bridge_identity, ..
            } => UnitKind::Zellij {
                bridge_unit: bridge_identity.unit(),
            },
        }
    }

    pub(super) fn retirement_guard(
        &self,
        cache_dir: &Path,
    ) -> Result<Option<super::registry::BridgeUnitGuard>, RegistryError> {
        match self {
            Self::Herdr { .. } => Ok(None),
            Self::Zellij {
                bridge_identity,
                entries,
                census,
            } => ZellijActivation {
                identity: bridge_identity,
                entries,
                census,
            }
            .retirement_guard(cache_dir)
            .map(Some),
        }
    }
}

/// Host policy is selected from the planned unit at the composition boundary.
/// The transaction interpreter only sees this contract and durable capabilities.
trait ActivationHost: HostPreflight {
    type ReadinessGuard;
    type ProofSnapshot: Send + 'static;

    fn entries(&self) -> &[RegisteredBroker];
    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError>;
    fn attests_entry(&self, status: &ActivationStatus, entry: &RegisteredBroker) -> bool;
    fn wire_host(&self) -> muxe_protocol::wire::HostKind;
    fn bridge(&self) -> Option<&dyn BridgeActivation>;
    async fn preflight<P: Preflight>(&self, preflight: &P, config_dir: &Path)
    -> Result<(), String>;
    fn attests_host(&self, status: &ActivationStatus) -> bool;
    async fn readiness_guard(
        &self,
        cache_dir: &Path,
        deadline: Instant,
    ) -> Result<Self::ReadinessGuard, ActivateError>;
    fn readiness_proof(
        &self,
        guard: &Self::ReadinessGuard,
    ) -> Result<Option<(UnitReadinessEpochId, AsOfTick)>, ActivateError>;
    fn proof_snapshot(&self) -> Self::ProofSnapshot;
    fn prove_ready_authority(
        snapshot: Self::ProofSnapshot,
        config_dir: &Path,
        cache_dir: &Path,
        journal: &ActivationJournal,
        prepared: &[PreparedAuthority],
    ) -> Result<Vec<RegisteredBroker>, ActivateError>;
}

/// Bridge transaction operations exist only for a unit with bridge authority.
/// Durable `journal.bridge()` progress remains the recovery interpreter's input.
trait BridgeActivation {
    fn preflight_global(
        &self,
        config_dir: &Path,
        staged_bridge: Option<&StagedBridge>,
        preparation: &mut GlobalPreflight,
    ) -> Result<(), String>;
    fn revalidate_locked(&self, cache_dir: &Path) -> Result<(), String>;
    fn unchanged_bridge(&self, preparation: &GlobalPreflight) -> bool;
    fn target_coverage(&self, status: &ActivationStatus, target: &CompatibilityRecord) -> bool;
    fn retirement_guard(
        &self,
        cache_dir: &Path,
    ) -> Result<super::registry::BridgeUnitGuard, RegistryError>;
    fn bind_authority(
        &self,
        journal: &mut ActivationJournal,
        preparation: &GlobalPreflight,
        target: &CompatibilityRecord,
    ) -> Result<(), ActivateError>;
    fn install_and_reload(
        &self,
        cache_dir: &Path,
        journal: &mut ActivationJournal,
        prepared: &[PreparedAuthority],
        reloader: &dyn HostReloader,
        hooks: &ActivateHooks,
    ) -> Result<(), ActivateError>;
}

struct HerdrActivation<'a>(&'a RegisteredBroker);

struct ZellijActivation<'a> {
    identity: &'a BridgeIdentity,
    entries: &'a [RegisteredBroker],
    census: &'a MemberCensus,
}

impl HostPreflight for HerdrActivation<'_> {
    async fn validate_live_host(&self, live: &LivePreflight<'_>) -> Result<(), String> {
        // The runtime refuses a server below the adapter's minimum release.
        // Herdr has no upper bound, so `settings.host.version.check` does not apply.
        live.herdr_runtime(self.0.discovery_key().as_str())
            .await
            .map(drop)
    }
}

impl HostPreflight for ZellijActivation<'_> {
    async fn validate_live_host(&self, live: &LivePreflight<'_>) -> Result<(), String> {
        for entry in self.entries {
            let policy = live.version_policy()?;
            let program = live.zellij_exe.as_ref().ok_or_else(|| {
                "no Zellij executable is installed; cannot probe Zellij hosts".to_owned()
            })?;
            let output = std::process::Command::new(program)
                .arg("--version")
                .output()
                .map_err(|error| format!("cannot probe the Zellij executable: {error}"))?;
            if !output.status.success() {
                return Err("the Zellij executable refuses its version probe".to_owned());
            }
            let text = String::from_utf8_lossy(&output.stdout);
            let version = text.split_whitespace().nth(1).ok_or_else(|| {
                "the Zellij executable reports an unrecognized version line".to_owned()
            })?;
            live.check_version(
                "zellij",
                entry.discovery_key().as_str(),
                version,
                crate::compatibility::ZELLIJ_MINIMUM,
                crate::compatibility::ZELLIJ_LATEST_VERIFIED,
                policy,
            )?;
        }
        Ok(())
    }
}

impl ActivationHost for HerdrActivation<'_> {
    type ReadinessGuard = ();
    type ProofSnapshot = ();

    fn entries(&self) -> &[RegisteredBroker] {
        std::slice::from_ref(self.0)
    }

    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError> {
        RegisteredBroker::herdr(entry)
    }

    fn attests_entry(&self, status: &ActivationStatus, _entry: &RegisteredBroker) -> bool {
        status.bridge_unit.is_none()
    }

    fn wire_host(&self) -> muxe_protocol::wire::HostKind {
        muxe_protocol::wire::HostKind::Herdr
    }

    fn bridge(&self) -> Option<&dyn BridgeActivation> {
        None
    }

    async fn preflight<P: Preflight>(
        &self,
        preflight: &P,
        _config_dir: &Path,
    ) -> Result<(), String> {
        preflight.validate_host(self).await
    }

    fn attests_host(&self, status: &ActivationStatus) -> bool {
        status.live_server.host == muxe_protocol::wire::HostKind::Herdr
    }

    async fn readiness_guard(
        &self,
        _cache_dir: &Path,
        _deadline: Instant,
    ) -> Result<Self::ReadinessGuard, ActivateError> {
        Ok(())
    }

    fn readiness_proof(
        &self,
        _guard: &Self::ReadinessGuard,
    ) -> Result<Option<(UnitReadinessEpochId, AsOfTick)>, ActivateError> {
        Ok(None)
    }

    fn proof_snapshot(&self) -> Self::ProofSnapshot {}

    fn prove_ready_authority(
        (): Self::ProofSnapshot,
        _config_dir: &Path,
        cache_dir: &Path,
        journal: &ActivationJournal,
        _prepared: &[PreparedAuthority],
    ) -> Result<Vec<RegisteredBroker>, ActivateError> {
        Registry::open(cache_dir)?
            .entries()?
            .into_iter()
            // Select exact target endpoints before parsing host format. Keep
            // every row at those endpoints so foreign duplicates cannot vanish.
            .filter(|entry| {
                journal
                    .members()
                    .iter()
                    .any(|member| member.endpoint().as_path() == entry.socket)
            })
            .map(|entry| RegisteredBroker::herdr(entry).map_err(ActivateError::from))
            .collect()
    }
}

impl ActivationHost for ZellijActivation<'_> {
    type ReadinessGuard = muxe_adapter_zellij::ReadinessGateGuard;
    type ProofSnapshot = (BridgeIdentity, Vec<RegisteredBroker>, MemberCensus);

    fn entries(&self) -> &[RegisteredBroker] {
        self.entries
    }

    fn validate_recorded(&self, entry: BrokerEntry) -> Result<RegisteredBroker, RegistryError> {
        RegisteredBroker::zellij(entry, self.identity)
    }

    fn attests_entry(&self, status: &ActivationStatus, entry: &RegisteredBroker) -> bool {
        entry.bridge_identity() == Some(self.identity)
            && status.bridge_unit == Some(self.identity.unit())
    }

    fn wire_host(&self) -> muxe_protocol::wire::HostKind {
        muxe_protocol::wire::HostKind::Zellij
    }

    fn bridge(&self) -> Option<&dyn BridgeActivation> {
        Some(self)
    }

    async fn preflight<P: Preflight>(
        &self,
        preflight: &P,
        config_dir: &Path,
    ) -> Result<(), String> {
        let expected = integration::bridge_identity(config_dir)
            .map_err(|error| format!("cannot resolve canonical bridge authority: {error}"))?;
        if *self.identity != expected {
            return Err(format!(
                "Zellij unit uses unmanaged bridge identity {}",
                self.identity
            ));
        }
        preflight.validate_host(self).await
    }

    fn attests_host(&self, status: &ActivationStatus) -> bool {
        status.live_server.host == muxe_protocol::wire::HostKind::Zellij
    }

    async fn readiness_guard(
        &self,
        cache_dir: &Path,
        deadline: Instant,
    ) -> Result<Self::ReadinessGuard, ActivateError> {
        muxe_adapter_zellij::ReadinessGate::new(cache_dir.to_path_buf(), self.identity.unit())
            .shared(
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_secs(2)),
            )
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "cannot hold broker-observed readiness while sealing as-of proof: {error}"
                ),
            })
    }

    fn readiness_proof(
        &self,
        _guard: &Self::ReadinessGuard,
    ) -> Result<Option<(UnitReadinessEpochId, AsOfTick)>, ActivateError> {
        let mut bytes = [0_u8; 16];
        getrandom::getrandom(&mut bytes).map_err(|error| ActivateError::UnitFailed {
            reason: format!("cannot mint unit readiness epoch: {error}"),
        })?;
        let epoch =
            UnitReadinessEpochId::from_bytes(bytes).map_err(|error| ActivateError::UnitFailed {
                reason: format!("invalid unit readiness epoch: {error}"),
            })?;
        let as_of = muxe_adapter_zellij::ReadinessGate::as_of_now().map_err(|error| {
            ActivateError::UnitFailed {
                reason: format!("cannot capture common monotonic readiness tick: {error}"),
            }
        })?;
        Ok(Some((epoch, as_of)))
    }

    fn proof_snapshot(&self) -> Self::ProofSnapshot {
        (
            self.identity.clone(),
            self.entries.to_vec(),
            self.census.clone(),
        )
    }

    fn prove_ready_authority(
        (identity, entries, census): Self::ProofSnapshot,
        config_dir: &Path,
        cache_dir: &Path,
        journal: &ActivationJournal,
        prepared: &[PreparedAuthority],
    ) -> Result<Vec<RegisteredBroker>, ActivateError> {
        prove_ready_bridge(
            config_dir, cache_dir, &identity, &entries, &census, journal, prepared,
        )?;
        let raw = Registry::open(cache_dir)?.entries()?;
        // Bridge membership completeness is independent of exact endpoint
        // admission: foreign rows at a selected socket must remain observable.
        if raw
            .iter()
            .filter(|row| {
                row.parsed_host_kind().ok() == Some(muxe_protocol::wire::HostKind::Zellij)
                    && row.bridge_identity.as_ref() == Some(&identity)
            })
            .count()
            != prepared.len()
        {
            return Err(ActivateError::UnitFailed {
                reason: "final Ready proof has incomplete target registry membership".to_owned(),
            });
        }
        raw.into_iter()
            .filter(|row| {
                journal
                    .members()
                    .iter()
                    .any(|member| member.endpoint().as_path() == row.socket)
            })
            .map(|row| RegisteredBroker::zellij(row, &identity).map_err(ActivateError::from))
            .collect()
    }
}

impl BridgeActivation for ZellijActivation<'_> {
    fn revalidate_locked(&self, cache_dir: &Path) -> Result<(), String> {
        let live = Registry::open(cache_dir)
            .map_err(|error| error.to_string())?
            .probe()
            .map_err(|error| error.to_string())?
            .live;
        let mut current_entries = live
            .into_iter()
            .filter(|entry| {
                entry.parsed_host_kind().ok() == Some(muxe_protocol::wire::HostKind::Zellij)
                    && entry.bridge_identity.as_ref() == Some(self.identity)
            })
            .map(|entry| self.validate_recorded(entry))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        current_entries.sort_by(|left, right| left.bridge_member().cmp(&right.bridge_member()));
        let current_census = MemberCensus::from_members(
            current_entries
                .iter()
                .map(|entry| {
                    entry
                        .bridge_member()
                        .cloned()
                        .expect("validated Zellij member")
                })
                .collect(),
        )
        .map_err(|error| error.to_string())?;
        if &current_census != self.census || current_entries != self.entries {
            return Err(
                "Zellij bridge membership changed after preflight; activation aborted before journal or drain"
                    .to_owned(),
            );
        }
        Ok(())
    }

    fn unchanged_bridge(&self, preparation: &GlobalPreflight) -> bool {
        matches!(
            (&preparation.expected_current, &preparation.verified_bridge),
            (Some(current), Some(target)) if current == &target.packaged_digest
        )
    }

    fn target_coverage(&self, status: &ActivationStatus, target: &CompatibilityRecord) -> bool {
        target
            .zellij
            .as_ref()
            .and_then(|zellij| zellij.bridge_build_id)
            .is_some_and(|build_id| !build_id.is_zero())
            && status.ready.as_ref().is_some_and(zellij_census_covered)
    }

    fn retirement_guard(
        &self,
        cache_dir: &Path,
    ) -> Result<super::registry::BridgeUnitGuard, RegistryError> {
        super::registry::BridgeUnitGuard::acquire(cache_dir, self.identity.clone())
    }

    fn preflight_global(
        &self,
        config_dir: &Path,
        staged_bridge: Option<&StagedBridge>,
        preparation: &mut GlobalPreflight,
    ) -> Result<(), String> {
        let staged = staged_bridge.ok_or_else(|| {
            "a Zellij unit is selected but no staged replacement bridge was provided".to_owned()
        })?;
        let verification =
            compatibility::verify_packaged_asset(&staged.bytes).map_err(|error| {
                format!("staged bridge rejected by native package identity: {error}")
            })?;
        let identity = integration::bridge_identity(config_dir)
            .map_err(|error| format!("cannot resolve canonical bridge authority: {error}"))?;
        let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        let receipt = integration::receipt::load(identity.directory())
            .map_err(|error| format!("cannot read integration receipt: {error}"))?
            .ok_or_else(|| {
                "activation requires an existing receipt-owned Zellij bridge; install it first"
                    .to_owned()
            })?;
        if receipt.bridge.bridge_identity != identity {
            return Err(format!(
                "integration receipt bridge identity {} does not match {}",
                receipt.bridge.bridge_identity, identity
            ));
        }
        integration::bridge::check_previous(&stable, receipt.bridge.previous_digest.as_ref())
            .map_err(|error| format!("rollback copy preflight failed: {error}"))?;
        let (eligibility, _) =
            integration::bridge::check_destination(&stable, Some(&receipt.bridge.installed_digest))
                .map_err(|error| format!("bridge preflight failed: {error}"))?;
        let expected_current = match eligibility {
            integration::bridge::Eligibility::Absent => {
                return Err(
                    "receipt-owned stable Zellij bridge is absent; refusing activation".to_owned(),
                );
            }
            integration::bridge::Eligibility::EligibleReplace { current_digest } => {
                Some(current_digest)
            }
        };
        if expected_current.is_some() {
            fsutil::read_owner_file(&stable)
                .map_err(|error| format!("bridge destination is not owner-only: {error}"))?;
        }
        preparation.verified_bridge = Some(verification);
        preparation.expected_current = expected_current;
        preparation.bridge_receipt = Some(receipt.bridge);
        Ok(())
    }

    fn bind_authority(
        &self,
        journal: &mut ActivationJournal,
        preparation: &GlobalPreflight,
        target: &CompatibilityRecord,
    ) -> Result<(), ActivateError> {
        let verification =
            preparation
                .verified_bridge
                .as_ref()
                .ok_or_else(|| ActivateError::UnitFailed {
                    reason: "Zellij bridge package was not verified".to_owned(),
                })?;
        let receipt =
            preparation
                .bridge_receipt
                .clone()
                .ok_or_else(|| ActivateError::UnitFailed {
                    reason: "Zellij receipt authority is absent".to_owned(),
                })?;
        let old_digest =
            preparation
                .expected_current
                .clone()
                .ok_or_else(|| ActivateError::UnitFailed {
                    reason: "receipt-owned stable bridge is absent".to_owned(),
                })?;
        let target_digest = verification.packaged_digest.clone();
        let receipt_target = integration::receipt::BridgeRecord {
            bridge_identity: self.identity.clone(),
            installed_version: target.muxe_version.clone(),
            installed_digest: target_digest.clone(),
            previous_digest: Some(old_digest.clone()),
            bridge_compat: target.zellij.clone(),
        };
        let mut receipt_rollback = receipt.clone();
        receipt_rollback.previous_digest = Some(target_digest.clone());
        journal.bind_zellij_authority(
            self.identity.clone(),
            self.census.clone(),
            BridgeArtifacts {
                old: BridgeArtifactId::new(journal.activation_id, BridgeArtifactRole::Old),
                target: BridgeArtifactId::new(journal.activation_id, BridgeArtifactRole::Target),
                old_digest,
                target_digest,
                receipt_preimage: receipt,
                receipt_target,
                receipt_rollback,
            },
        )?;
        Ok(())
    }

    fn install_and_reload(
        &self,
        cache_dir: &Path,
        journal: &mut ActivationJournal,
        prepared: &[PreparedAuthority],
        reloader: &dyn HostReloader,
        hooks: &ActivateHooks,
    ) -> Result<(), ActivateError> {
        revalidate_transaction_membership(
            cache_dir,
            self.identity,
            self.census,
            self.entries,
            prepared,
        )
        .map_err(|reason| ActivateError::UnitFailed { reason })?;
        let stable = self
            .identity
            .stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        let artifacts = journal
            .bridge()
            .expect("validated Zellij journal has artifacts")
            .artifacts
            .clone();
        journal
            .bridge_mut()
            .expect("validated Zellij journal has artifacts")
            .progress = BridgeProgress::TargetInstallIntent;
        journal::write_journal(cache_dir, journal)?;
        integration::bridge::install_artifact(
            self.identity,
            artifacts.target,
            &artifacts.target_digest,
            &stable,
            &[&artifacts.old_digest, &artifacts.target_digest],
            false,
        )?;
        journal
            .bridge_mut()
            .expect("validated Zellij journal has artifacts")
            .progress = BridgeProgress::TargetInstalled;
        journal::write_journal(cache_dir, journal)?;
        hooks.check(ActivateStep::BridgeSwapped)?;
        let url = integration::kdl::bridge_url(&stable);
        journal
            .bridge_mut()
            .expect("validated Zellij journal has bridge")
            .progress = BridgeProgress::TargetReloading {
            active: None,
            completed: Vec::new(),
        };
        journal::write_journal(cache_dir, journal)?;
        for member in journal.members().to_vec() {
            if let BridgeProgress::TargetReloading { active, .. } = &mut journal
                .bridge_mut()
                .expect("validated Zellij journal has bridge")
                .progress
            {
                *active = Some(member.id.clone());
            }
            journal::write_journal(cache_dir, journal)?;
            reloader.reload_bridge(member.member().as_str(), &url)?;
            if let BridgeProgress::TargetReloading { active, completed } = &mut journal
                .bridge_mut()
                .expect("validated Zellij journal has bridge")
                .progress
            {
                *active = None;
                if !completed.contains(&member.id) {
                    completed.push(member.id);
                }
            }
            journal::write_journal(cache_dir, journal)?;
        }
        journal
            .bridge_mut()
            .expect("validated Zellij journal has bridge")
            .progress = BridgeProgress::TargetReloaded;
        journal::write_journal(cache_dir, journal)?;
        hooks.check(ActivateStep::ReloadIssued)?;
        Ok(())
    }
}

/// Outcome of one activation unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnitOutcome {
    Committed {
        unit: String,
    },
    /// Every broker already reports the target; Zellij also has a
    /// receipt-authorized stable bridge whose bytes match the verified package.
    Unchanged {
        unit: String,
    },
    RolledBack {
        unit: String,
        reason: String,
    },
    Failed {
        unit: String,
        reason: String,
    },
}

/// Final activation report naming every unit outcome.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ActivateReport {
    pub units: Vec<UnitOutcome>,
}

/// One concrete target renderer selected before iterating a unit's members.
/// Its implementation owns host-specific serve argv and exact row admission.
pub trait TargetSpawnPolicy {
    /// Builds the exact owned child request under the selected unit.
    ///
    /// # Errors
    ///
    /// Refuses absent executable authority or a mismatched observed member.
    fn render(&self, member: &SpawnMember<'_>) -> Result<(PathBuf, Vec<OsString>), ActivateError>;
}

/// One composition-boundary selection for an entire typed activation unit.
pub trait TargetSpawnSelector {
    /// Returns the concrete policy chosen for the complete unit transaction.
    fn select(&self, unit: &UnitKind) -> &dyn TargetSpawnPolicy;
}

/// Borrowed selector installed at the executable composition boundary.
pub type SpawnPolicySelector<'a> = &'a dyn TargetSpawnSelector;

pub struct ActivateInputs<'a, C, S, R, P> {
    pub config_dir: &'a Path,
    pub cache_dir: &'a Path,
    pub target: CompatibilityRecord,
    /// Verified replacement bridge bytes. Required when a Zellij unit is selected.
    pub staged_bridge: Option<StagedBridge>,
    pub scope: HostScope,
    pub current: Option<DetectedHost>,
    pub control: &'a C,
    pub spawner: &'a S,
    pub reloader: &'a R,
    pub preflight: &'a P,
    /// Selects one concrete target renderer before the unit's member loop.
    pub spawn_policy: SpawnPolicySelector<'a>,
    pub readiness_deadline: Duration,
    pub poll_interval: Duration,
    pub hooks: ActivateHooks,
    pub logger: Option<&'a Logger>,
}

/// Bridge package and receipt authority captured before any broker is drained.
#[derive(Debug)]
struct GlobalPreflight {
    verified_bridge: Option<compatibility::NativeAssetVerification>,
    expected_current: Option<Sha256Digest>,
    bridge_receipt: Option<integration::receipt::BridgeRecord>,
}

/// Runs one activation across every selected unit, committing or rolling back
/// each independently.
///
/// # Errors
///
/// Fails when recovery finds an unresolved journal, when no live units match
/// the scope, or when global preflight fails. Per-unit failures surface in
/// the returned report, never as an early return.
pub async fn activate<C, S, R, P>(
    inputs: ActivateInputs<'_, C, S, R, P>,
) -> Result<ActivateReport, ActivateError>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    let recovered = recover(
        inputs.cache_dir,
        inputs.control,
        inputs.reloader,
        inputs.logger,
    )
    .await?;
    if let Some(RecoveryOutcome::Preserved { unit, reason }) = recovered
        .iter()
        .find(|outcome| matches!(outcome, RecoveryOutcome::Preserved { .. }))
    {
        return Err(ActivateError::UnitFailed {
            reason: format!("activation recovery remains unresolved for {unit}: {reason}"),
        });
    }

    inputs.hooks.check(ActivateStep::PreflightDone)?;

    let registry = Registry::open(inputs.cache_dir)?;
    let live = registry.probe()?.live;
    let units = select_units(&live, inputs.scope, inputs.current.as_ref())?;
    if units.is_empty() {
        return Err(ActivateError::NoLiveUnits);
    }

    // Global preflight before any unit mutates. Transaction artifacts are
    // created only after the complete v2 intent journal is durable.
    let mut preparation = global_preflight(&inputs, &units)
        .await
        .map_err(ActivateError::Preflight)?;
    inputs.hooks.check(ActivateStep::PreflightDone)?;

    let mut report = ActivateReport::default();
    for unit in units {
        report
            .units
            .push(activate_unit(&inputs, &unit, &mut preparation).await);
    }
    Ok(report)
}

pub(crate) fn select_units(
    live: &[BrokerEntry],
    scope: HostScope,
    current: Option<&DetectedHost>,
) -> Result<Vec<PlannedUnit>, ActivateError> {
    match scope {
        HostScope::All => {
            let mut units = herdr_units(live, None)?;
            units.extend(group_zellij(live)?);
            Ok(units)
        }
        HostScope::Zellij => group_zellij(live),
        HostScope::Herdr => herdr_units(live, None),
        HostScope::Current => {
            let Some(current) = current else {
                return Err(ActivateError::CurrentHostRequired);
            };
            match current {
                DetectedHost::Herdr { discovery_key } => {
                    let units = herdr_units(live, Some(discovery_key))?;
                    units
                        .into_iter()
                        .next()
                        .map(|unit| vec![unit])
                        .ok_or(ActivateError::NoLiveUnits)
                }
                DetectedHost::Zellij {
                    bridge_identity, ..
                } => {
                    let group = recorded_zellij_group(live, bridge_identity)?;
                    if group.is_empty() {
                        return Err(ActivateError::NoLiveUnits);
                    }
                    Ok(vec![zellij_unit(bridge_identity.clone(), group)?])
                }
            }
        }
    }
}

fn herdr_units(
    rows: &[BrokerEntry],
    discovery: Option<&str>,
) -> Result<Vec<PlannedUnit>, ActivateError> {
    rows.iter()
        .filter(|entry| {
            entry.parsed_host_kind().ok() == Some(muxe_protocol::wire::HostKind::Herdr)
                && discovery.is_none_or(|key| entry.discovery_key == key)
        })
        .take(discovery.map_or(usize::MAX, |_| 1))
        .cloned()
        .map(|entry| {
            RegisteredBroker::herdr(entry)
                .map(|entry| PlannedUnit::Herdr { entry })
                .map_err(ActivateError::from)
        })
        .collect()
}

fn recorded_zellij_group(
    rows: &[BrokerEntry],
    identity: &BridgeIdentity,
) -> Result<Vec<RegisteredBroker>, RegistryError> {
    rows.iter()
        .filter(|entry| {
            entry.parsed_host_kind().ok() == Some(muxe_protocol::wire::HostKind::Zellij)
                && entry.bridge_identity.as_ref() == Some(identity)
        })
        .cloned()
        .map(|entry| RegisteredBroker::zellij(entry, identity))
        .collect()
}

/// Concrete host prerequisites for native activation composition.
#[derive(Clone, Copy, Debug)]
pub struct SelectedHostRequirements {
    pub herdr: bool,
    pub zellij: bool,
}

/// Validates selected lifecycle records before choosing native executables.
///
/// # Errors
///
/// Returns errors for an absent current host or a malformed selected record.
pub fn selected_host_requirements(
    rows: Vec<BrokerEntry>,
    scope: HostScope,
    current: Option<&DetectedHost>,
) -> Result<SelectedHostRequirements, ActivateError> {
    let mut requirements = SelectedHostRequirements {
        herdr: false,
        zellij: false,
    };
    if matches!(scope, HostScope::Current) && current.is_none() {
        return Err(ActivateError::CurrentHostRequired);
    }
    // Native executable composition consumes the probe snapshot, avoiding
    // copies of planned records that activation immediately rereads.
    for row in rows {
        match row.parsed_host_kind().ok() {
            Some(muxe_protocol::wire::HostKind::Herdr)
                if !matches!(scope, HostScope::Zellij)
                    && (!matches!(scope, HostScope::Current)
                        || matches!(current, Some(DetectedHost::Herdr { discovery_key })
                            if row.discovery_key == *discovery_key && !requirements.herdr)) =>
            {
                RegisteredBroker::herdr(row)?;
                requirements.herdr = true;
            }
            Some(muxe_protocol::wire::HostKind::Zellij)
                if !matches!(scope, HostScope::Herdr)
                    && (!matches!(scope, HostScope::Current)
                        || matches!(current, Some(DetectedHost::Zellij { bridge_identity, .. })
                            if row.bridge_identity.as_ref() == Some(bridge_identity))) =>
            {
                RegisteredBroker::recorded_zellij(row)?;
                requirements.zellij = true;
            }
            _ => {}
        }
    }
    if matches!(scope, HostScope::Current) && !requirements.herdr && !requirements.zellij {
        return Err(ActivateError::NoLiveUnits);
    }
    Ok(requirements)
}

fn group_zellij(live: &[BrokerEntry]) -> Result<Vec<PlannedUnit>, ActivateError> {
    let mut groups: std::collections::BTreeMap<BridgeIdentity, Vec<RegisteredBroker>> =
        std::collections::BTreeMap::new();
    for entry in live.iter().filter(|entry| {
        entry.parsed_host_kind().ok() == Some(muxe_protocol::wire::HostKind::Zellij)
    }) {
        let identity = entry
            .bridge_identity
            .clone()
            .ok_or_else(|| ActivateError::UnitFailed {
                reason: format!(
                    "Zellij registry entry {} lacks canonical bridge authority",
                    entry.discovery_key
                ),
            })?;
        let registered = RegisteredBroker::zellij(entry.clone(), &identity)?;
        groups.entry(identity).or_default().push(registered);
    }
    groups
        .into_iter()
        .map(|(identity, entries)| zellij_unit(identity, entries))
        .collect()
}

fn zellij_unit(
    bridge_identity: BridgeIdentity,
    mut entries: Vec<RegisteredBroker>,
) -> Result<PlannedUnit, ActivateError> {
    entries.sort_by(|left, right| left.bridge_member().cmp(&right.bridge_member()));
    let census = MemberCensus::from_members(
        entries
            .iter()
            .map(|entry| {
                entry
                    .bridge_member()
                    .cloned()
                    .expect("validated Zellij member")
            })
            .collect(),
    )
    .map_err(|error| ActivateError::UnitFailed {
        reason: error.to_string(),
    })?;
    Ok(PlannedUnit::Zellij {
        bridge_identity,
        entries,
        census,
    })
}

async fn global_preflight<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    units: &[PlannedUnit],
) -> Result<GlobalPreflight, String>
where
    P: Preflight,
{
    let mut preparation = GlobalPreflight {
        verified_bridge: None,
        expected_current: None,
        bridge_receipt: None,
    };
    // Select the concrete bridge validator once before any unit mutates.
    if let Some(bridge) = units.iter().find_map(|unit| match unit {
        PlannedUnit::Herdr { .. } => None,
        PlannedUnit::Zellij {
            bridge_identity,
            entries,
            census,
        } => Some(ZellijActivation {
            identity: bridge_identity,
            entries,
            census,
        }),
    }) {
        bridge.preflight_global(
            inputs.config_dir,
            inputs.staged_bridge.as_ref(),
            &mut preparation,
        )?;
    }
    inputs.preflight.validate_config().await?;
    for unit in units {
        match unit {
            PlannedUnit::Herdr { entry } => {
                HerdrActivation(entry)
                    .preflight(inputs.preflight, inputs.config_dir)
                    .await?;
            }
            PlannedUnit::Zellij {
                bridge_identity,
                entries,
                census,
            } => {
                ZellijActivation {
                    identity: bridge_identity,
                    entries,
                    census,
                }
                .preflight(inputs.preflight, inputs.config_dir)
                .await?;
            }
        }
    }
    fsutil::ensure_owner_dir(&journal::activation_dir(inputs.cache_dir))
        .map_err(|error| format!("activation journal directory is not writable: {error}"))?;
    Ok(preparation)
}

async fn activate_unit<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    preparation: &mut GlobalPreflight,
) -> UnitOutcome
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    match unit {
        PlannedUnit::Herdr { entry } => {
            activate_unit_for(inputs, unit, &HerdrActivation(entry), preparation).await
        }
        PlannedUnit::Zellij {
            bridge_identity,
            entries,
            census,
        } => {
            activate_unit_for(
                inputs,
                unit,
                &ZellijActivation {
                    identity: bridge_identity,
                    entries,
                    census,
                },
                preparation,
            )
            .await
        }
    }
}

async fn activate_unit_for<C, S, R, P, H>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    host: &H,
    preparation: &mut GlobalPreflight,
) -> UnitOutcome
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
    H: ActivationHost,
{
    let label = unit_label(unit);
    let unit_kind = unit.unit_kind();
    let _unit_lock = match journal::acquire_unit_lock(inputs.cache_dir, &unit_kind) {
        Ok(lock) => lock,
        Err(error) => {
            return UnitOutcome::Failed {
                unit: label,
                reason: format!("activation unit is already in progress: {error}"),
            };
        }
    };
    if let Some(bridge) = host.bridge()
        && let Err(reason) = bridge.revalidate_locked(inputs.cache_dir)
    {
        return UnitOutcome::Failed {
            unit: label,
            reason,
        };
    }
    match activate_unit_inner_prepared(inputs, unit, host, unit_kind, preparation).await {
        Ok(outcome) => outcome,
        Err(ActivateError::FaultInjected { step }) => UnitOutcome::Failed {
            unit: label,
            reason: format!("fault injected after {step:?}"),
        },
        Err(error) => UnitOutcome::Failed {
            unit: label,
            reason: error.to_string(),
        },
    }
}

pub(crate) fn unit_label(unit: &PlannedUnit) -> String {
    match unit {
        PlannedUnit::Herdr { entry } => format!("herdr:{}", entry.discovery_key()),
        PlannedUnit::Zellij {
            bridge_identity, ..
        } => format!("zellij:{bridge_identity}"),
    }
}

fn revalidate_transaction_membership(
    cache_dir: &Path,
    bridge_identity: &BridgeIdentity,
    census: &MemberCensus,
    old_entries: &[RegisteredBroker],
    prepared: &[PreparedAuthority],
) -> Result<(), String> {
    let raw = Registry::open(cache_dir)
        .map_err(|error| error.to_string())?
        .entries()
        .map_err(|error| error.to_string())?;
    let registry_entries =
        recorded_zellij_group(&raw, bridge_identity).map_err(|error| error.to_string())?;
    let current_census = MemberCensus::from_members(
        registry_entries
            .iter()
            .map(|entry| {
                entry
                    .bridge_member()
                    .cloned()
                    .expect("validated Zellij member")
            })
            .collect(),
    )
    .map_err(|error| error.to_string())?;
    if &current_census != census {
        return Err("logical Zellij bridge membership changed during activation".to_owned());
    }
    for current in &registry_entries {
        if old_entries.contains(current) {
            continue;
        }
        let authorized = current.bridge_identity() == Some(bridge_identity)
            && prepared.iter().any(|member| {
                current.bridge_member() == member.entry.bridge_member()
                    && current.discovery_key() == member.entry.discovery_key()
                    && current.socket() == member.entry.socket()
                    && current.handoff_id() == Some(member.handoff)
            });
        if !authorized {
            return Err("registry contains a non-journal-authorized bridge incarnation".to_owned());
        }
    }
    Ok(())
}

/// One prepared member with its retained old-broker session.
struct PreparedMember<C: ControlPort> {
    entry: RegisteredBroker,
    handoff: HandoffId,
    old_session: C::Session,
}

/// Session-free authority copied into a bounded, read-only filesystem probe.
#[derive(Clone)]
struct PreparedAuthority {
    entry: RegisteredBroker,
    handoff: HandoffId,
}

enum PrepareFailure {
    Refused(String),
    Ambiguous(String),
}

#[cfg(test)]
async fn activate_unit_with_global_preflight<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
) -> Result<UnitOutcome, ActivateError>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    let mut preparation = global_preflight(inputs, std::slice::from_ref(unit))
        .await
        .map_err(ActivateError::Preflight)?;
    Ok(activate_unit(inputs, unit, &mut preparation).await)
}

#[expect(
    clippy::too_many_lines,
    reason = "the normal actor executes the single durable transaction interpreter and persists every intent/result boundary"
)]
async fn activate_unit_inner_prepared<C, S, R, P, H>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    host: &H,
    unit_kind: UnitKind,
    preparation: &mut GlobalPreflight,
) -> Result<UnitOutcome, ActivateError>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
    H: ActivationHost,
{
    let label = unit_label(unit);
    let entries = host.entries();

    // Observe exact old records before constructing the complete journal. No
    // Prepare can occur until every handoff and member intent is durable.
    let mut observed = Vec::with_capacity(entries.len());
    let mut all_current = true;
    for entry in entries {
        let mut session = inputs
            .control
            .connect(entry.socket())
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!("connect {} before journal: {error}", entry.discovery_key()),
            })?;
        let status = session
            .status()
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!("status {} before journal: {error}", entry.discovery_key()),
            })?;
        if status.prepare_handoff != Some(PrepareHandoffProtocol::CoordinatorSuppliedV1)
            || !host.attests_entry(&status, entry)
            || status.lifecycle != LifecycleState::Running
            || status.handoff_id.is_some()
        {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "old broker {} does not attest a clean running incarnation",
                    entry.discovery_key()
                ),
            });
        }
        all_current &= status.current == inputs.target;
        observed.push((entry.clone(), status.current));
    }
    if all_current
        && host
            .bridge()
            .is_none_or(|bridge| bridge.unchanged_bridge(preparation))
    {
        return Ok(UnitOutcome::Unchanged { unit: label });
    }
    let spawn_policy = inputs.spawn_policy.select(&unit_kind);

    let activation_id = ActivationId::generate()?;
    let members = observed
        .iter()
        .map(|(entry, old_record)| {
            TransactionMember::new(
                activation_id,
                ActivationMemberId::new(entry.discovery_key().as_str().to_owned())?,
                MemberEndpoint::new(entry.socket().to_path_buf())?,
                fresh_handoff()?,
                old_record.clone(),
            )
        })
        .collect::<Result<Vec<_>, JournalError>>()?;
    let mut journal =
        ActivationJournal::new(activation_id, unit_kind, inputs.target.clone(), members)?;
    journal
        .old_registry
        .extend(entries.iter().map(RegisteredBroker::recorded_entry));
    if let Some(bridge) = host.bridge() {
        bridge.bind_authority(&mut journal, preparation, &inputs.target)?;
    }
    let journal_path = journal::write_journal(inputs.cache_dir, &journal)?;
    // Private artifacts are created only after their exact typed identities,
    // digests, and receipt preimage are durable.
    let artifact_result = (|| -> Result<(), ActivateError> {
        inputs.hooks.check(ActivateStep::JournalWritten)?;
        if let Some(bridge) = journal.bridge() {
            let identity = journal
                .bridge_identity
                .as_ref()
                .expect("validated Zellij journal has identity");
            let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
            integration::bridge::ensure_artifact_from_file(
                identity,
                bridge.artifacts.old,
                &stable,
                &bridge.artifacts.old_digest,
            )?;
            let target_bytes = &inputs
                .staged_bridge
                .as_ref()
                .ok_or_else(|| ActivateError::UnitFailed {
                    reason: "target bridge bytes are absent".to_owned(),
                })?
                .bytes;
            integration::bridge::ensure_artifact(
                identity,
                bridge.artifacts.target,
                target_bytes,
                &bridge.artifacts.target_digest,
            )?;
            journal
                .bridge_mut()
                .expect("validated Zellij journal has bridge")
                .progress = BridgeProgress::ArtifactsReady;
            journal::write_journal(inputs.cache_dir, &journal)?;
            inputs.hooks.check(ActivateStep::ArtifactsReady)?;
        }
        Ok(())
    })();
    if let Err(error) = artifact_result {
        let reason = error.to_string();
        let diagnostics = rollback_transaction(
            inputs,
            unit,
            &mut journal,
            &journal_path,
            Vec::new(),
            Vec::new(),
            reason.clone(),
        )
        .await;
        return Ok(rollback_outcome(label, reason, &diagnostics));
    }

    let mut prepared = Vec::with_capacity(entries.len());
    for entry in entries {
        let member_index = journal
            .members()
            .iter()
            .position(|member| member.endpoint().as_path() == entry.socket())
            .expect("journal member covers every planned entry");
        journal.members_mut()[member_index].old = OldMemberProgress::PrepareIntent;
        if let Err(error) = journal::write_journal(inputs.cache_dir, &journal) {
            let reason = error.to_string();
            let diagnostics = rollback_transaction(
                inputs,
                unit,
                &mut journal,
                &journal_path,
                prepared,
                Vec::new(),
                reason.clone(),
            )
            .await;
            return Ok(rollback_outcome(label, reason, &diagnostics));
        }
        let handoff = journal.members()[member_index].handoff_id();
        let old_record = journal.members()[member_index].old_record.clone();
        match drain_one(inputs, host, entry, handoff, &old_record).await {
            Ok(member) => {
                journal.members_mut()[member_index].old = OldMemberProgress::Drained;
                prepared.push(member);
                if let Err(error) = journal::write_journal(inputs.cache_dir, &journal) {
                    let reason = error.to_string();
                    let diagnostics = rollback_transaction(
                        inputs,
                        unit,
                        &mut journal,
                        &journal_path,
                        prepared,
                        Vec::new(),
                        reason.clone(),
                    )
                    .await;
                    return Ok(rollback_outcome(label, reason, &diagnostics));
                }
            }
            Err(PrepareFailure::Refused(reason)) => {
                let diagnostics = rollback_transaction(
                    inputs,
                    unit,
                    &mut journal,
                    &journal_path,
                    prepared,
                    Vec::new(),
                    reason.clone(),
                )
                .await;
                return Ok(if diagnostics.is_empty() {
                    UnitOutcome::RolledBack {
                        unit: label,
                        reason,
                    }
                } else {
                    UnitOutcome::Failed {
                        unit: label,
                        reason: with_rollback(reason, &diagnostics),
                    }
                });
            }
            Err(PrepareFailure::Ambiguous(reason)) => {
                return Ok(UnitOutcome::Failed {
                    unit: label,
                    reason: format!("{reason}; journal preserved at {}", journal_path.display()),
                });
            }
        }
    }
    let activation_result = (|| -> Result<(), ActivateError> {
        inputs.hooks.check(ActivateStep::OldPrepared)?;
        journal.enter_activating();
        journal::write_journal(inputs.cache_dir, &journal)?;
        Ok(())
    })();
    if let Err(error) = activation_result {
        let reason = error.to_string();
        let diagnostics = rollback_transaction(
            inputs,
            unit,
            &mut journal,
            &journal_path,
            prepared,
            Vec::new(),
            reason.clone(),
        )
        .await;
        return Ok(rollback_outcome(label, reason, &diagnostics));
    }

    let mut targets = Vec::with_capacity(prepared.len());
    for member in &prepared {
        let index = journal
            .members()
            .iter()
            .position(|record| record.endpoint().as_path() == member.entry.socket())
            .expect("prepared member remains journaled");
        journal.members_mut()[index].target = TargetMemberProgress::SpawnIntent;
        if let Err(error) = journal::write_journal(inputs.cache_dir, &journal) {
            let reason = error.to_string();
            let diagnostics = rollback_transaction(
                inputs,
                unit,
                &mut journal,
                &journal_path,
                prepared,
                targets,
                reason.clone(),
            )
            .await;
            return Ok(rollback_outcome(label, reason, &diagnostics));
        }
        let record = &journal.members()[index];
        if member.entry.discovery_key().as_str() != record.member().as_str()
            || member.entry.socket() != record.endpoint().as_path()
        {
            return Err(ActivateError::UnitFailed {
                reason: "prepared registry member differs from journal authority".to_owned(),
            });
        }
        let spawn_member = SpawnMember {
            unit: &journal.unit,
            authority: record.authority.clone(),
            observed_host: host.wire_host(),
            observed_bridge_identity: member.entry.bridge_identity(),
            observed_bridge_member: member.entry.bridge_member(),
            observed_handoff_id: member.entry.handoff_id(),
            journal_path: journal_path.clone(),
        };
        let (program, args) = match spawn_policy.render(&spawn_member) {
            Ok(request) => request,
            Err(error) => {
                let reason = error.to_string();
                let diagnostics = rollback_transaction(
                    inputs,
                    unit,
                    &mut journal,
                    &journal_path,
                    prepared,
                    targets,
                    reason.clone(),
                )
                .await;
                return Ok(rollback_outcome(label, reason, &diagnostics));
            }
        };
        match inputs.spawner.spawn_target(&SpawnRequest { program, args }) {
            Ok(handle) => {
                journal.members_mut()[index].target = TargetMemberProgress::Gated;
                targets.push(OwnedTarget {
                    member: journal.members()[index].id.clone(),
                    handle,
                });
                if let Err(error) = journal::write_journal(inputs.cache_dir, &journal) {
                    let reason = format!("persist exact spawned target: {error}");
                    let diagnostics = rollback_transaction(
                        inputs,
                        unit,
                        &mut journal,
                        &journal_path,
                        prepared,
                        targets,
                        reason.clone(),
                    )
                    .await;
                    return Ok(rollback_outcome(label, reason, &diagnostics));
                }
            }
            Err(error) => {
                let reason = error.to_string();
                let diagnostics = rollback_transaction(
                    inputs,
                    unit,
                    &mut journal,
                    &journal_path,
                    prepared,
                    targets,
                    reason.clone(),
                )
                .await;
                return Ok(rollback_outcome(label, reason, &diagnostics));
            }
        }
    }
    let authorities = prepared
        .iter()
        .map(|member| PreparedAuthority {
            entry: member.entry.clone(),
            handoff: member.handoff,
        })
        .collect::<Vec<_>>();
    let pre_ready = async {
        inputs.hooks.check(ActivateStep::TargetSpawned)?;
        if let Some(bridge) = host.bridge() {
            bridge.install_and_reload(
                inputs.cache_dir,
                &mut journal,
                &authorities,
                inputs.reloader,
                &inputs.hooks,
            )?;
        }

        let deadline = Instant::now() + inputs.readiness_deadline;
        for member in &prepared {
            wait_ready(
                inputs.control,
                &member.entry,
                &member.handoff,
                &inputs.target,
                host,
                deadline,
                inputs.poll_interval,
            )
            .await?;
            let index = journal
                .members()
                .iter()
                .position(|record| record.endpoint().as_path() == member.entry.socket())
                .expect("ready member remains journaled");
            journal.members_mut()[index].target = TargetMemberProgress::Ready;
            journal::write_journal(inputs.cache_dir, &journal)?;
        }
        Ok::<(), ActivateError>(())
    }
    .await;
    if let Err(error) = pre_ready {
        let reason = error.to_string();
        let diagnostics = rollback_transaction(
            inputs,
            unit,
            &mut journal,
            &journal_path,
            prepared,
            targets,
            reason.clone(),
        )
        .await;
        return Ok(rollback_outcome(label, reason, &diagnostics));
    }
    let proof_deadline = Instant::now() + inputs.readiness_deadline;
    let readiness_guard = match host.readiness_guard(inputs.cache_dir, proof_deadline).await {
        Ok(guard) => guard,
        Err(error) => {
            let reason = error.to_string();
            let diagnostics = rollback_transaction(
                inputs,
                unit,
                &mut journal,
                &journal_path,
                prepared,
                targets,
                reason.clone(),
            )
            .await;
            return Ok(rollback_outcome(label, reason, &diagnostics));
        }
    };
    let proof_result = async {
        let epoch = host.readiness_proof(&readiness_guard)?;
        let incarnations = prove_ready_unit(
            inputs,
            host,
            &journal,
            ReadyUnitMembers {
                prepared: &prepared,
                authorities: &authorities,
                targets: &mut targets,
            },
            ReadyWindow {
                proof: epoch,
                deadline: proof_deadline,
            },
        )
        .await?;
        journal::ReadyProof::new(&journal, epoch, incarnations).map_err(ActivateError::from)
    }
    .await;
    let sealed_proof = match proof_result {
        Ok(proof) => proof,
        Err(error) => {
            drop(readiness_guard);
            let reason = error.to_string();
            let diagnostics = rollback_transaction(
                inputs,
                unit,
                &mut journal,
                &journal_path,
                prepared,
                targets,
                reason.clone(),
            )
            .await;
            return Ok(rollback_outcome(label, reason, &diagnostics));
        }
    };
    // The certificate states a historical broker epoch. The gate may release
    // before the blocking Ready fsync; late writers cannot change that as-of fact.
    drop(readiness_guard);
    let ready_outcome = persist_and_transfer_ready(
        inputs.cache_dir,
        &journal_path,
        &mut journal,
        sealed_proof,
        &mut targets,
    );
    if let ReadyWriteOutcome::NotWritten(reason) = ready_outcome? {
        let diagnostics = rollback_transaction(
            inputs,
            unit,
            &mut journal,
            &journal_path,
            prepared,
            targets,
            reason.clone(),
        )
        .await;
        return Ok(rollback_outcome(label, reason, &diagnostics));
    }
    inputs.hooks.check(ActivateStep::ReadinessRecorded)?;

    journal.enter_committing();
    journal::write_journal(inputs.cache_dir, &journal)?;
    let mut commit_failures = Vec::new();
    for member in &mut prepared {
        let index = journal
            .members()
            .iter()
            .position(|record| record.endpoint().as_path() == member.entry.socket())
            .expect("commit member remains journaled");
        journal.members_mut()[index].old = OldMemberProgress::CommitIntent;
        journal::write_journal(inputs.cache_dir, &journal)?;
        match member.old_session.commit(&member.handoff).await {
            Ok(_) => {
                if !journal::has_old_retirement_receipt(
                    &journal::activation_dir(inputs.cache_dir),
                    &journal,
                    &journal.members()[index],
                )? {
                    commit_failures.push(format!(
                        "old {} acknowledged without a durable retirement receipt",
                        member.entry.discovery_key()
                    ));
                    continue;
                }
                journal.members_mut()[index].old = OldMemberProgress::Committed;
                journal::write_journal(inputs.cache_dir, &journal)?;
            }
            Err(error) => commit_failures.push(format!(
                "commit old {}: {error}",
                member.entry.discovery_key()
            )),
        }
    }
    if !commit_failures.is_empty() {
        return Ok(UnitOutcome::Failed {
            unit: label,
            reason: commit_failures.join("; "),
        });
    }
    for member in &prepared {
        let index = journal
            .members()
            .iter()
            .position(|record| record.endpoint().as_path() == member.entry.socket())
            .expect("target commit member remains journaled");
        let mut target_session = match certified_target_session(
            inputs.cache_dir,
            inputs.control,
            &journal,
            &journal.members()[index],
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                commit_failures.push(format!(
                    "verify target {}: {error}",
                    member.entry.discovery_key()
                ));
                continue;
            }
        };
        journal.members_mut()[index].target = TargetMemberProgress::CommitIntent;
        journal::write_journal(inputs.cache_dir, &journal)?;
        match target_session.commit(&member.handoff).await {
            Ok(_) => {
                journal.members_mut()[index].target = TargetMemberProgress::Committed;
                journal::write_journal(inputs.cache_dir, &journal)?;
            }
            Err(error) => commit_failures.push(format!(
                "commit target {}: {error}",
                member.entry.discovery_key()
            )),
        }
    }
    if !commit_failures.is_empty() {
        return Ok(UnitOutcome::Failed {
            unit: label,
            reason: commit_failures.join("; "),
        });
    }

    publish_commit_artifacts(inputs.cache_dir, &mut journal)?;
    if host.bridge().is_some() {
        inputs.hooks.check(ActivateStep::ReceiptUpdated)?;
    }

    journal.enter_committed();
    journal::write_journal(inputs.cache_dir, &journal)?;
    inputs.hooks.check(ActivateStep::TerminalWritten)?;
    cleanup_terminal_transaction(&journal, &journal_path)?;
    inputs.hooks.check(ActivateStep::TerminalCleaned)?;
    inputs.hooks.check(ActivateStep::Committed)?;
    log(inputs.logger, &label, "activation committed")?;
    Ok(UnitOutcome::Committed { unit: label })
}

/// Drains one old broker with a handoff that was durable before this call.
async fn drain_one<C, H: ActivationHost>(
    inputs: &ActivateInputs<'_, C, impl BrokerSpawner, impl HostReloader, impl Preflight>,
    host: &H,
    entry: &RegisteredBroker,
    handoff: HandoffId,
    old_record: &CompatibilityRecord,
) -> Result<PreparedMember<C>, PrepareFailure>
where
    C: ControlPort,
{
    let mut session = inputs
        .control
        .connect(entry.socket())
        .await
        .map_err(|error| {
            PrepareFailure::Ambiguous(format!("connect {}: {error}", entry.discovery_key()))
        })?;
    let status = session.status().await.map_err(|error| {
        PrepareFailure::Ambiguous(format!(
            "status failed for {}: {error}",
            entry.discovery_key()
        ))
    })?;
    if !host.attests_entry(&status, entry)
        || status.current != *old_record
        || status.lifecycle != LifecycleState::Running
        || status.handoff_id.is_some()
    {
        return Err(PrepareFailure::Ambiguous(format!(
            "pre-Prepare status mismatched exact old authority for {}",
            entry.discovery_key()
        )));
    }
    let prepared = match session.prepare(&inputs.target, &handoff).await {
        Ok(status) => status,
        Err(error) => match session.status().await {
            Ok(status)
                if host.attests_entry(&status, entry)
                    && status.current == *old_record
                    && status.lifecycle == LifecycleState::Running
                    && status.handoff_id.is_none() =>
            {
                return Err(PrepareFailure::Refused(format!(
                    "prepare refused for {} without mutation: {error}",
                    entry.discovery_key()
                )));
            }
            Ok(status)
                if host.attests_entry(&status, entry)
                    && status.lifecycle == LifecycleState::Draining
                    && status.handoff_id == Some(handoff)
                    && status.target.as_ref() == Some(&inputs.target) =>
            {
                status
            }
            Ok(_) | Err(_) => {
                return Err(PrepareFailure::Ambiguous(format!(
                    "prepare outcome is ambiguous for {}: {error}",
                    entry.discovery_key()
                )));
            }
        },
    };
    if !host.attests_entry(&prepared, entry)
        || prepared.lifecycle != LifecycleState::Draining
        || prepared.handoff_id != Some(handoff)
        || prepared.target.as_ref() != Some(&inputs.target)
    {
        return Err(PrepareFailure::Ambiguous(format!(
            "prepared status mismatches exact journaled intent for {}",
            entry.discovery_key()
        )));
    }
    Ok(PreparedMember {
        entry: entry.clone(),
        handoff,
        old_session: session,
    })
}

fn fresh_handoff() -> Result<HandoffId, JournalError> {
    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes).map_err(|error| {
        JournalError::Inconsistent(format!("cannot generate handoff identity: {error}"))
    })?;
    if bytes == [0; 16] {
        return Err(JournalError::Inconsistent(
            "generated zero handoff identity".to_owned(),
        ));
    }
    Ok(HandoffId(bytes))
}

/// Verifies that one recovery status is bound to the journal's canonical unit.
///
/// Zellij requires a canonical identity whose unit equals both the journal key
/// and the broker attestation. Herdr must not carry bridge authority.
#[must_use]
pub fn status_attests_journal(status: &ActivationStatus, journal: &ActivationJournal) -> bool {
    match (&journal.unit, journal.bridge_identity.as_ref()) {
        (UnitKind::Zellij { bridge_unit }, Some(identity)) => {
            identity.unit() == *bridge_unit && status.bridge_unit == Some(*bridge_unit)
        }
        (UnitKind::Herdr { .. }, None) => status.bridge_unit.is_none(),
        _ => false,
    }
}

/// A target is ready only when it reports the exact expected handoff, the
/// matching host identity, and the complete target compatibility record.
/// Zellij members additionally require a complete, nonempty, duplicate-free
/// census from one snapshot round: every snapshot member holds a fresh
/// compatible registration, so a partially or spuriously registered target
/// can never commit. Herdr keeps its subscription/health gate and never
/// requires this census.
fn target_ready<H: ActivationHost>(
    status: &ActivationStatus,
    member: &RegisteredBroker,
    expected_handoff: &HandoffId,
    target: &CompatibilityRecord,
    host: &H,
) -> bool {
    status.current == *target
        && status.handoff_id == Some(*expected_handoff)
        && status.live_server.discovery_key == member.discovery_key().as_str()
        && host.attests_host(status)
        && status.target.is_none()
        && host.attests_entry(status, member)
        && status.lifecycle == LifecycleState::Running
        && host
            .bridge()
            .is_none_or(|bridge| bridge.target_coverage(status, target))
}

/// Checks one snapshot round of Zellij commit-gate evidence: the registered
/// set must equal the authoritative member set exactly — no gaps, extras, or
/// duplicates — with every ID nonempty. The count is consistency-checked,
/// never proof. A missing report is never ready; a present-but-empty
/// snapshot with no registrations is a legitimate zero-client unit.
fn zellij_census_covered(ready: &TargetReadiness) -> bool {
    let Some(original) = ready.member_ids.as_ref() else {
        return false;
    };
    if original.iter().map(String::as_str).any(str::is_empty)
        || ready
            .registered_clients
            .iter()
            .map(String::as_str)
            .any(str::is_empty)
    {
        return false;
    }
    let mut members = original.clone();
    members.sort();
    members.dedup();
    if members.len() != original.len() || members.len() as u64 != ready.member_clients {
        return false;
    }
    let mut registered = ready.registered_clients.clone();
    registered.sort();
    registered.dedup();
    registered == members
}

async fn wait_ready<C, H>(
    control: &C,
    member: &RegisteredBroker,
    expected_handoff: &HandoffId,
    target: &CompatibilityRecord,
    host: &H,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ActivateError>
where
    C: ControlPort,
    H: ActivationHost,
{
    loop {
        if let Ok(mut session) = control.connect(member.socket()).await
            && let Ok(status) = session.status().await
            && target_ready(&status, member, expected_handoff, target, host)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ActivateError::ReadinessTimeout {
                identity: member.discovery_key().as_str().to_owned(),
            });
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Exact retained sessions, journal authorities, and owned target handles
/// participating in one final Ready proof.
struct ReadyUnitMembers<'a, C: ControlPort> {
    prepared: &'a [PreparedMember<C>],
    authorities: &'a [PreparedAuthority],
    targets: &'a mut [OwnedTarget],
}

/// One bounded as-of proof attempt; Herdr carries no epoch.
#[derive(Clone, Copy)]
struct ReadyWindow {
    proof: Option<(UnitReadinessEpochId, AsOfTick)>,
    deadline: Instant,
}
/// The final, unit-locked Ready proof. Earlier polling may have observed each
/// member in a different round; only this fresh pass authorizes the durable
/// Ready transition. Bridge bytes and receipt prove installed artifacts, not
/// which WASM a host has loaded; the live per-client snapshot is separate.
async fn prove_ready_unit<C, S, R, P, H>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    host: &H,
    journal: &ActivationJournal,
    members: ReadyUnitMembers<'_, C>,
    window: ReadyWindow,
) -> Result<Vec<journal::ReadyMemberProof>, ActivateError>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
    H: ActivationHost,
{
    let ReadyUnitMembers {
        prepared,
        authorities,
        targets,
    } = members;
    journal.validate()?;
    if journal.directive() != TransactionDirective::Activate
        || prepared.len() != journal.members().len()
        || targets.len() != prepared.len()
        || journal.members().iter().any(|member| {
            member.old != OldMemberProgress::Drained || member.target != TargetMemberProgress::Ready
        })
    {
        return Err(ActivateError::UnitFailed {
            reason: "final Ready proof lacks complete durable member progress".to_owned(),
        });
    }
    let config_dir = inputs.config_dir.to_path_buf();
    let cache_dir = inputs.cache_dir.to_path_buf();
    let proof_snapshot = host.proof_snapshot();
    let journal_snapshot = journal.clone();
    let authorities = authorities.to_vec();
    let rows = tokio::time::timeout_at(
        window.deadline.into(),
        tokio::task::spawn_blocking(move || {
            H::prove_ready_authority(
                proof_snapshot,
                &config_dir,
                &cache_dir,
                &journal_snapshot,
                &authorities,
            )
        }),
    )
    .await
    .map_err(|_| ActivateError::UnitFailed {
        reason: "final Ready proof filesystem and registry read timed out".to_owned(),
    })?
    .map_err(|error| ActivateError::UnitFailed {
        reason: format!("final Ready proof reader task failed: {error}"),
    })??;
    let mut incarnations = Vec::with_capacity(prepared.len());
    for prepared_member in prepared {
        incarnations.push(
            prove_ready_member(
                inputs.control,
                host,
                journal,
                prepared_member,
                targets,
                &rows,
                window,
            )
            .await?,
        );
    }
    Ok(incarnations)
}

fn prove_ready_bridge(
    config_dir: &Path,
    cache_dir: &Path,
    bridge_identity: &BridgeIdentity,
    old_entries: &[RegisteredBroker],
    census: &MemberCensus,
    journal: &ActivationJournal,
    prepared: &[PreparedAuthority],
) -> Result<(), ActivateError> {
    if journal.bridge_identity.as_ref() != Some(bridge_identity)
        || journal.member_census.as_ref() != Some(census)
        || journal
            .bridge()
            .is_none_or(|bridge| bridge.progress != BridgeProgress::TargetReloaded)
    {
        return Err(ActivateError::UnitFailed {
            reason: "final Ready proof lacks exact reloaded bridge authority".to_owned(),
        });
    }
    revalidate_transaction_membership(cache_dir, bridge_identity, census, old_entries, prepared)
        .map_err(|reason| ActivateError::UnitFailed { reason })?;
    if integration::bridge_identity(config_dir)? != *bridge_identity {
        return Err(ActivateError::UnitFailed {
            reason: "final Ready proof observed a changed canonical bridge identity".to_owned(),
        });
    }
    let artifacts = &journal
        .bridge()
        .expect("checked bridge authority")
        .artifacts;
    let stable = bridge_identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
    for (path, digest) in [
        (stable, &artifacts.target_digest),
        (
            integration::bridge::artifact_path(bridge_identity, artifacts.old),
            &artifacts.old_digest,
        ),
        (
            integration::bridge::artifact_path(bridge_identity, artifacts.target),
            &artifacts.target_digest,
        ),
    ] {
        let bytes = fsutil::read_owner_file(&path)?;
        if Sha256Digest::from_bytes(&bytes) != *digest {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "final Ready proof found foreign bridge bytes at {}",
                    path.display()
                ),
            });
        }
    }
    let receipt = integration::receipt::load(bridge_identity.directory())?.ok_or_else(|| {
        ActivateError::UnitFailed {
            reason: "final Ready proof lost the old bridge receipt".to_owned(),
        }
    })?;
    if receipt.bridge != artifacts.receipt_preimage {
        return Err(ActivateError::UnitFailed {
            reason: "final Ready proof found changed old bridge receipt authority".to_owned(),
        });
    }
    Ok(())
}

async fn prove_ready_member<C: ControlPort, H: ActivationHost>(
    control: &C,
    host: &H,
    journal: &ActivationJournal,
    prepared: &PreparedMember<C>,
    targets: &mut [OwnedTarget],
    rows: &[RegisteredBroker],
    window: ReadyWindow,
) -> Result<journal::ReadyMemberProof, ActivateError> {
    let member = journal
        .members()
        .iter()
        .find(|member| {
            member.member().as_str() == prepared.entry.discovery_key().as_str()
                && member.endpoint().as_path() == prepared.entry.socket()
                && member.handoff_id() == prepared.handoff
        })
        .ok_or_else(|| ActivateError::UnitFailed {
            reason: "final Ready proof lost exact journal member authority".to_owned(),
        })?;
    let target = targets
        .iter_mut()
        .find(|target| target.member == member.id)
        .ok_or_else(|| ActivateError::UnitFailed {
            reason: "final Ready proof lost its owned target child".to_owned(),
        })?;
    if target
        .handle
        .child
        .try_wait()
        .map_err(|error| ActivateError::TargetStopInspect(error.to_string()))?
        .is_some()
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "final Ready proof target {} already exited",
                member.member().as_str()
            ),
        });
    }
    let mut matching = rows
        .iter()
        .filter(|row| row.socket() == prepared.entry.socket());
    let row = matching.next().ok_or_else(|| ActivateError::UnitFailed {
        reason: format!(
            "final Ready proof target {} has no registry row",
            member.member().as_str()
        ),
    })?;
    if matching.next().is_some()
        || row.discovery_key() != prepared.entry.discovery_key()
        || row.server_pid().get() != target.handle.child.id()
        || row.bridge_identity() != prepared.entry.bridge_identity()
        || row.bridge_member() != prepared.entry.bridge_member()
        || row.handoff_id() != journal.bridge().map(|_| prepared.handoff)
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "final Ready proof target {} registry incarnation is not journal-authorized",
                member.member().as_str()
            ),
        });
    }
    let status =
        fetch_final_ready_status(control, prepared, row, window.proof, window.deadline).await?;
    if !target_ready(
        &status,
        row,
        &prepared.handoff,
        &journal.target_record,
        host,
    ) || !status_attests_journal(&status, journal)
        || row.live_server() != Some(&status.live_server.server_id)
        || window.proof.is_some_and(|(epoch, _)| {
            status
                .ready
                .as_ref()
                .is_none_or(|ready| ready.proof_epoch != Some(epoch))
        })
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "final Ready proof target {} lost exact live readiness",
                member.member().as_str()
            ),
        });
    }
    journal::ReadyMemberProof::new(member, &row.recorded_entry(), &status.live_server.server_id)
        .map_err(ActivateError::from)
}

/// Reads one certified target on a single bounded control connection. The
/// caller separately compares the result with the sealed journal and registry.
async fn fetch_final_ready_status<C: ControlPort>(
    control: &C,
    prepared: &PreparedMember<C>,
    row: &RegisteredBroker,
    proof: Option<(UnitReadinessEpochId, AsOfTick)>,
    deadline: Instant,
) -> Result<ActivationStatus, ActivateError> {
    let discovery = prepared.entry.discovery_key().as_str();
    let mut session = tokio::time::timeout_at(deadline.into(), control.connect(row.socket()))
        .await
        .map_err(|_| ActivateError::UnitFailed {
            reason: format!("final Ready proof connection to {discovery} timed out"),
        })?
        .map_err(|error| ActivateError::UnitFailed {
            reason: format!("final Ready proof cannot connect {discovery}: {error}"),
        })?;
    tokio::time::timeout_at(deadline.into(), async {
        if let Some((epoch, as_of)) = proof {
            session.status_at(&prepared.handoff, epoch, as_of).await
        } else {
            session.status().await
        }
    })
    .await
    .map_err(|_| ActivateError::UnitFailed {
        reason: format!("final Ready proof status for {discovery} timed out"),
    })?
    .map_err(|error| ActivateError::UnitFailed {
        reason: format!("final Ready proof cannot inspect {discovery}: {error}"),
    })
}

#[derive(Debug, Eq, PartialEq)]
enum ReadyWriteOutcome {
    Durable,
    NotWritten(String),
}

/// Immediately transfers certified child liveness to broker recovery after
/// durable or uncertain Ready; only an exactly observed pre-Ready phase keeps
/// rollback's owned-child kill authority.
fn persist_and_transfer_ready(
    cache_dir: &Path,
    journal_path: &Path,
    journal: &mut ActivationJournal,
    proof: journal::ReadyProof,
    targets: &mut [OwnedTarget],
) -> Result<ReadyWriteOutcome, ActivateError> {
    let outcome = persist_ready_decision(cache_dir, journal_path, journal, Some(proof));
    if !matches!(outcome, Ok(ReadyWriteOutcome::NotWritten(_))) {
        for target in targets {
            target.handle.surrender_to_live_broker();
        }
    }
    outcome
}

/// On an ambiguous write error, the disk phase decides fate. An installed
/// Ready file is synced before Commit; an unchanged Activating file rolls back.
fn persist_ready_decision(
    cache_dir: &Path,
    journal_path: &Path,
    journal: &mut ActivationJournal,
    proof: Option<journal::ReadyProof>,
) -> Result<ReadyWriteOutcome, ActivateError> {
    let before_ready = journal.clone();
    journal.enter_ready(proof);
    match journal::write_journal(cache_dir, journal) {
        Ok(_) => Ok(ReadyWriteOutcome::Durable),
        Err(error) => {
            let on_disk = journal::read_journal(journal_path).map_err(|read_error| {
                ActivateError::UnitFailed {
                    reason: format!(
                        "Ready persistence failed ({error}); cannot establish durable phase: {read_error}"
                    ),
                }
            })?;
            if on_disk == *journal {
                fsutil::sync_file_and_parent(journal_path).map_err(|sync_error| {
                    ActivateError::UnitFailed {
                        reason: format!(
                            "Ready persistence failed ({error}); cannot complete durability: {sync_error}"
                        ),
                    }
                })?;
                Ok(ReadyWriteOutcome::Durable)
            } else if on_disk == before_ready {
                *journal = on_disk;
                Ok(ReadyWriteOutcome::NotWritten(format!(
                    "Ready persistence failed before durable decision: {error}"
                )))
            } else {
                Err(ActivateError::UnitFailed {
                    reason: format!(
                        "Ready persistence failed ({error}); journal changed outside exact transaction authority"
                    ),
                })
            }
        }
    }
}

/// Combines the triggering failure with every rollback diagnostic. Rollback
/// failures never replace the original error; they extend it.
fn with_rollback(reason: String, diagnostics: &[String]) -> String {
    if diagnostics.is_empty() {
        reason
    } else {
        format!("{reason}; rollback: {}", diagnostics.join("; "))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RollbackAction {
    ResolvePrepare(usize),
    ResolveTargetSpawn(usize),
    RetireTarget(usize),
    MarkBridgeRestored,
    InstallOldBridge,
    PublishTargetPrevious,
    RestoreOldReceipt,
    BeginOldReload,
    ReloadOldBridge(usize),
    CompleteBridgeRestore,
    ResumeOld(usize),
    AwaitingPeer,
    Finish,
}

fn next_rollback_action<A: RollbackActor>(
    journal: &ActivationJournal,
    actor: &A,
) -> RollbackAction {
    for (index, member) in journal.members().iter().enumerate() {
        if member.old == OldMemberProgress::PrepareIntent {
            return RollbackAction::ResolvePrepare(index);
        }
        if member.target == TargetMemberProgress::SpawnIntent {
            return RollbackAction::ResolveTargetSpawn(index);
        }
        if matches!(
            member.target,
            TargetMemberProgress::Gated
                | TargetMemberProgress::Ready
                | TargetMemberProgress::CommitIntent
                | TargetMemberProgress::Committed
                | TargetMemberProgress::RetireIntent
        ) {
            return RollbackAction::RetireTarget(index);
        }
    }
    if let Some(bridge) = journal.bridge() {
        return match &bridge.progress {
            BridgeProgress::ArtifactsPending
                if journal.members().iter().all(|member| {
                    member.old == OldMemberProgress::Pending
                        && member.target == TargetMemberProgress::Absent
                }) =>
            {
                RollbackAction::MarkBridgeRestored
            }
            BridgeProgress::Restored { .. } => {
                if let Some(index) = journal.members().iter().position(|member| {
                    !matches!(
                        member.old,
                        OldMemberProgress::Pending | OldMemberProgress::Resumed
                    ) && actor.can_resume(member)
                }) {
                    RollbackAction::ResumeOld(index)
                } else if journal.members().iter().any(|member| {
                    !matches!(
                        member.old,
                        OldMemberProgress::Pending | OldMemberProgress::Resumed
                    )
                }) {
                    RollbackAction::AwaitingPeer
                } else {
                    RollbackAction::Finish
                }
            }
            BridgeProgress::OldInstalled | BridgeProgress::TargetPreviousIntent => {
                RollbackAction::PublishTargetPrevious
            }
            BridgeProgress::TargetPreviousPublished | BridgeProgress::OldReceiptIntent => {
                RollbackAction::RestoreOldReceipt
            }
            BridgeProgress::OldReceiptPublished => RollbackAction::BeginOldReload,
            BridgeProgress::OldReloading { active, completed } => {
                let index = active
                    .as_ref()
                    .and_then(|id| journal.members().iter().position(|member| &member.id == id))
                    .or_else(|| {
                        journal
                            .members()
                            .iter()
                            .position(|member| !completed.contains(&member.id))
                    });
                index.map_or(
                    RollbackAction::CompleteBridgeRestore,
                    RollbackAction::ReloadOldBridge,
                )
            }
            _ => RollbackAction::InstallOldBridge,
        };
    }
    if let Some(index) = journal.members().iter().position(|member| {
        !matches!(
            member.old,
            OldMemberProgress::Pending | OldMemberProgress::Resumed
        ) && actor.can_resume(member)
    }) {
        RollbackAction::ResumeOld(index)
    } else if journal.members().iter().any(|member| {
        !matches!(
            member.old,
            OldMemberProgress::Pending | OldMemberProgress::Resumed
        )
    }) {
        RollbackAction::AwaitingPeer
    } else {
        RollbackAction::Finish
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "bridge rollback is one ordered intent/action/progress interpreter whose durable sequence is clearest in one match"
)]
fn apply_bridge_rollback_action<R: HostReloader + ?Sized>(
    cache_dir: &Path,
    reloader: &R,
    hooks: Option<&ActivateHooks>,
    journal: &mut ActivationJournal,
    action: &RollbackAction,
) -> Result<(), ActivateError> {
    let identity = journal
        .bridge_identity
        .clone()
        .ok_or_else(|| ActivateError::UnitFailed {
            reason: "bridge rollback action lacks canonical identity".to_owned(),
        })?;
    let artifacts = journal
        .bridge()
        .ok_or_else(|| ActivateError::UnitFailed {
            reason: "bridge rollback action lacks artifacts".to_owned(),
        })?
        .artifacts
        .clone();
    let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
    match action {
        RollbackAction::MarkBridgeRestored => {
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::Restored {
                reloaded: Vec::new(),
            };
        }
        RollbackAction::InstallOldBridge => {
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::OldInstallIntent;
            journal::write_journal(cache_dir, journal)?;
            integration::bridge::install_artifact(
                &identity,
                artifacts.old,
                &artifacts.old_digest,
                &stable,
                &[&artifacts.target_digest, &artifacts.old_digest],
                false,
            )?;
            if let Some(hooks) = hooks {
                hooks.check(ActivateStep::OldInstallAppliedBeforeOutcome)?;
            }
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::OldInstalled;
        }
        RollbackAction::PublishTargetPrevious => {
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::TargetPreviousIntent;
            journal::write_journal(cache_dir, journal)?;
            integration::bridge::publish_previous(
                &identity,
                artifacts.target,
                &artifacts.target_digest,
                &stable,
                artifacts.receipt_preimage.previous_digest.as_ref(),
            )?;
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::TargetPreviousPublished;
        }
        RollbackAction::RestoreOldReceipt => {
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::OldReceiptIntent;
            journal::write_journal(cache_dir, journal)?;
            restore_bridge_receipt(&identity, &artifacts)?;
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::OldReceiptPublished;
        }
        RollbackAction::BeginOldReload => {
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::OldReloading {
                active: None,
                completed: Vec::new(),
            };
        }
        RollbackAction::ReloadOldBridge(index) => {
            let member = journal.members()[*index].clone();
            if let BridgeProgress::OldReloading { active, .. } = &mut journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress
            {
                *active = Some(member.id.clone());
            }
            journal::write_journal(cache_dir, journal)?;
            reloader.reload_bridge(
                member.member().as_str(),
                &integration::kdl::bridge_url(&stable),
            )?;
            if let BridgeProgress::OldReloading { active, completed } = &mut journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress
            {
                *active = None;
                if !completed.contains(&member.id) {
                    completed.push(member.id);
                }
            }
        }
        RollbackAction::CompleteBridgeRestore => {
            let completed_members = match &journal
                .bridge()
                .expect("bridge authority was validated")
                .progress
            {
                BridgeProgress::OldReloading { completed, .. } => completed.clone(),
                _ => {
                    return Err(ActivateError::UnitFailed {
                        reason: "bridge restoration completed outside old reload phase".to_owned(),
                    });
                }
            };
            journal
                .bridge_mut()
                .expect("bridge authority was validated")
                .progress = BridgeProgress::Restored {
                reloaded: completed_members,
            };
        }
        _ => {
            return Err(ActivateError::UnitFailed {
                reason: "non-bridge action reached bridge rollback executor".to_owned(),
            });
        }
    }
    journal::write_journal(cache_dir, journal)?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrepareResolution {
    Pending,
    Drained,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TargetSpawnResolution {
    Absent,
    NeedsRetirement(TargetRetirementAuthority),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResumeDisposition {
    Completed,
    DeferredToLocalBroker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RollbackDriveOutcome {
    Complete,
    ResumeRequired,
    AwaitingPeer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResumeEvidence {
    AlreadyResumed,
    NeedsResume,
}

fn classify_prepare_status(
    status: &ActivationStatus,
    member: &TransactionMember,
    journal: &ActivationJournal,
) -> Result<PrepareResolution, ActivateError> {
    let exact_member = status.live_server.discovery_key == member.member().as_str();
    if !exact_member || !status_attests_journal(status, journal) {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "{} Prepare intent status does not attest exact journal authority",
                member.member().as_str()
            ),
        });
    }
    if status.lifecycle == LifecycleState::Running
        && status.current == member.old_record
        && status.target.is_none()
        && status.handoff_id.is_none()
    {
        Ok(PrepareResolution::Pending)
    } else if status.lifecycle == LifecycleState::Draining
        && status.current == member.old_record
        && status.target.as_ref() == Some(&journal.target_record)
        && status.handoff_id == Some(member.handoff_id())
    {
        Ok(PrepareResolution::Drained)
    } else {
        Err(ActivateError::UnitFailed {
            reason: format!(
                "{} Prepare intent has mismatched evidence",
                member.member().as_str()
            ),
        })
    }
}

fn classify_target_status(
    status: &ActivationStatus,
    member: &TransactionMember,
    journal: &ActivationJournal,
) -> Result<(), ActivateError> {
    if status.live_server.discovery_key == member.member().as_str()
        && status_attests_journal(status, journal)
        && status.current == journal.target_record
        && status.lifecycle == LifecycleState::Running
        && status.target.is_none()
        && status.handoff_id == Some(member.handoff_id())
    {
        Ok(())
    } else {
        Err(ActivateError::UnitFailed {
            reason: format!(
                "target {} does not attest exact rollback authority",
                member.member().as_str()
            ),
        })
    }
}

fn classify_resume_status(
    status: &ActivationStatus,
    member: &TransactionMember,
    journal: &ActivationJournal,
) -> Result<ResumeEvidence, ActivateError> {
    if status.live_server.discovery_key != member.member().as_str()
        || !status_attests_journal(status, journal)
        || status.current != member.old_record
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "{} resume status does not attest exact journal authority",
                member.member().as_str()
            ),
        });
    }
    if status.lifecycle == LifecycleState::Running
        && status.target.is_none()
        && status.handoff_id.is_none()
    {
        Ok(ResumeEvidence::AlreadyResumed)
    } else if status.lifecycle == LifecycleState::Draining
        && status.target.as_ref() == Some(&journal.target_record)
        && status.handoff_id == Some(member.handoff_id())
    {
        Ok(ResumeEvidence::NeedsResume)
    } else {
        Err(ActivateError::UnitFailed {
            reason: format!(
                "{} resume intent has mismatched evidence",
                member.member().as_str()
            ),
        })
    }
}

trait RollbackActor {
    fn cache_dir(&self) -> &Path;
    fn reloader(&self) -> &dyn HostReloader;
    fn hooks(&self) -> Option<&ActivateHooks> {
        None
    }
    fn can_resume(&self, _member: &TransactionMember) -> bool {
        true
    }
    fn observe(
        &self,
        _stage: &'static str,
        _action: &RollbackAction,
        _journal: &ActivationJournal,
    ) {
    }
    async fn resolve_prepare(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<PrepareResolution, ActivateError>;
    async fn resolve_target_spawn(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<TargetSpawnResolution, ActivateError>;
    async fn target_retirement_authority(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<TargetRetirementAuthority, ActivateError>;
    async fn retire_target(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
        authority: &TargetRetirementAuthority,
    ) -> Result<(), ActivateError>;
    fn release_retired_target(&mut self, _member: &TransactionMember) -> Result<(), ActivateError> {
        Ok(())
    }
    async fn resume_old(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<ResumeDisposition, ActivateError>;
}

async fn complete_target_retirement<A: RollbackActor>(
    actor: &mut A,
    journal: &mut ActivationJournal,
    index: usize,
    action: &RollbackAction,
    prepared_authority: Option<TargetRetirementAuthority>,
) -> Result<(), ActivateError> {
    let replaying_intent = journal.members()[index].target == TargetMemberProgress::RetireIntent;
    let directory = journal::activation_dir(actor.cache_dir());
    if !replaying_intent {
        let member = journal.members()[index].clone();
        let authority = match prepared_authority {
            Some(authority) => authority,
            None => actor.target_retirement_authority(&member, journal).await?,
        };
        let intent = TargetRetirementIntent::new(journal, &member, authority)?;
        if journal::target_retirement_receipt_entry_exists(&directory, &intent)? {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "target {} retirement receipt path was occupied before RetireIntent",
                    member.member().as_str()
                ),
            });
        }
        let prior_target = member.target;
        let member = &mut journal.members_mut()[index];
        member.target = TargetMemberProgress::RetireIntent;
        member.target_retirement = Some(intent);
        if let Err(error) = journal::write_journal(actor.cache_dir(), journal) {
            let member = &mut journal.members_mut()[index];
            member.target = prior_target;
            member.target_retirement = None;
            return Err(error.into());
        }
    }
    actor.observe("retire_intent", action, journal);
    let member = journal.members()[index].clone();
    if journal::has_target_retirement_receipt(&directory, journal, &member)? {
        if !replaying_intent {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "target {} retirement receipt existed before its stop barrier",
                    member.member().as_str()
                ),
            });
        }
    } else {
        let authority = member
            .target_retirement
            .as_ref()
            .expect("validated RetireIntent has exact retirement authority")
            .authority()
            .clone();
        actor.retire_target(&member, journal, &authority).await?;
        journal::write_target_retirement_receipt(&directory, journal, &member)?;
        actor.release_retired_target(&member)?;
    }
    let member = &mut journal.members_mut()[index];
    member.target = TargetMemberProgress::Retired;
    journal::write_journal(actor.cache_dir(), journal)?;
    actor.observe("retired", action, journal);
    Ok(())
}

async fn drive_rollback<A: RollbackActor>(
    actor: &mut A,
    journal: &mut ActivationJournal,
    journal_path: &Path,
) -> Result<RollbackDriveOutcome, ActivateError> {
    let mut registry_restored = false;
    loop {
        let action = next_rollback_action(journal, actor);
        if matches!(
            action,
            RollbackAction::ResumeOld(_) | RollbackAction::Finish | RollbackAction::AwaitingPeer
        ) && !registry_restored
        {
            restore_old_registry_rows(actor.cache_dir(), journal).map_err(|reason| {
                ActivateError::UnitFailed {
                    reason: format!("restore old registry rows: {reason}"),
                }
            })?;
            registry_restored = true;
        }
        match action {
            RollbackAction::ResolvePrepare(index) => {
                actor.observe("prepare_evidence", &action, journal);
                let member = journal.members()[index].clone();
                journal.members_mut()[index].old =
                    match actor.resolve_prepare(&member, journal).await? {
                        PrepareResolution::Pending => OldMemberProgress::Pending,
                        PrepareResolution::Drained => OldMemberProgress::Drained,
                    };
                journal::write_journal(actor.cache_dir(), journal)?;
                actor.observe("prepare_resolved", &action, journal);
            }
            RollbackAction::ResolveTargetSpawn(index) => {
                let member = journal.members()[index].clone();
                match actor.resolve_target_spawn(&member, journal).await? {
                    TargetSpawnResolution::Absent => {
                        let member = &mut journal.members_mut()[index];
                        member.target = TargetMemberProgress::Absent;
                        member.target_retirement = None;
                        journal::write_journal(actor.cache_dir(), journal)?;
                    }
                    TargetSpawnResolution::NeedsRetirement(authority) => {
                        complete_target_retirement(actor, journal, index, &action, Some(authority))
                            .await?;
                    }
                }
            }
            RollbackAction::RetireTarget(index) => {
                complete_target_retirement(actor, journal, index, &action, None).await?;
            }
            action @ (RollbackAction::MarkBridgeRestored
            | RollbackAction::InstallOldBridge
            | RollbackAction::PublishTargetPrevious
            | RollbackAction::RestoreOldReceipt
            | RollbackAction::BeginOldReload
            | RollbackAction::ReloadOldBridge(_)
            | RollbackAction::CompleteBridgeRestore) => {
                apply_bridge_rollback_action(
                    actor.cache_dir(),
                    actor.reloader(),
                    actor.hooks(),
                    journal,
                    &action,
                )?;
            }
            RollbackAction::ResumeOld(index) => {
                if journal.members()[index].old != OldMemberProgress::ResumeIntent {
                    journal.members_mut()[index].old = OldMemberProgress::ResumeIntent;
                    journal::write_journal(actor.cache_dir(), journal)?;
                }
                actor.observe("resume_intent", &action, journal);
                let member = journal.members()[index].clone();
                match actor.resume_old(&member, journal).await? {
                    ResumeDisposition::Completed => {
                        journal.members_mut()[index].old = OldMemberProgress::Resumed;
                        journal::write_journal(actor.cache_dir(), journal)?;
                        actor.observe("resumed", &action, journal);
                    }
                    ResumeDisposition::DeferredToLocalBroker => {
                        return Ok(RollbackDriveOutcome::ResumeRequired);
                    }
                }
            }
            RollbackAction::AwaitingPeer => return Ok(RollbackDriveOutcome::AwaitingPeer),
            RollbackAction::Finish => {
                journal.enter_rolled_back();
                journal::write_journal(actor.cache_dir(), journal)?;
                actor.observe("terminal_written", &action, journal);
                if let Some(hooks) = actor.hooks() {
                    hooks.check(ActivateStep::TerminalWritten)?;
                }
                cleanup_terminal_transaction(journal, journal_path)?;
                actor.observe("cleanup_complete", &action, journal);
                if let Some(hooks) = actor.hooks() {
                    hooks.check(ActivateStep::TerminalCleaned)?;
                }
                return Ok(RollbackDriveOutcome::Complete);
            }
        }
    }
}

#[cfg(test)]
fn record_rollback_trace(
    trace: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    stage: &'static str,
    action: &RollbackAction,
    journal: &ActivationJournal,
) {
    let progress = journal
        .members()
        .iter()
        .map(|member| format!("{:?}", member.old))
        .collect::<Vec<_>>()
        .join(",");
    trace
        .lock()
        .expect("rollback trace is not poisoned")
        .push(format!(
            "{stage}:{action:?}:{progress}:{:?}",
            journal.directive()
        ));
}

struct NormalRollbackActor<'a, 'inputs, C, S, R, P>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    inputs: &'a ActivateInputs<'inputs, C, S, R, P>,
    prepared: Vec<PreparedMember<C>>,
    supervisor: &'a mut ActivationSupervisor,
    #[cfg(test)]
    trace: Option<std::sync::Arc<std::sync::Mutex<Vec<String>>>>,
}

impl<C, S, R, P> RollbackActor for NormalRollbackActor<'_, '_, C, S, R, P>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    fn cache_dir(&self) -> &Path {
        self.inputs.cache_dir
    }

    fn reloader(&self) -> &dyn HostReloader {
        self.inputs.reloader
    }

    fn hooks(&self) -> Option<&ActivateHooks> {
        Some(&self.inputs.hooks)
    }
    fn observe(&self, stage: &'static str, action: &RollbackAction, journal: &ActivationJournal) {
        #[cfg(test)]
        if let Some(trace) = &self.trace {
            record_rollback_trace(trace, stage, action, journal);
        }
        #[cfg(not(test))]
        let _ = (stage, action, journal);
    }

    async fn resolve_prepare(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<PrepareResolution, ActivateError> {
        let status = if let Some(prepared) = self
            .prepared
            .iter_mut()
            .find(|prepared| prepared.entry.socket() == member.endpoint().as_path())
        {
            prepared
                .old_session
                .status()
                .await
                .map_err(|error| ActivateError::UnitFailed {
                    reason: format!(
                        "{} retained Prepare intent status is unavailable: {error}",
                        member.member().as_str()
                    ),
                })?
        } else {
            let mut session = self
                .inputs
                .control
                .connect(member.endpoint().as_path())
                .await
                .map_err(|error| ActivateError::UnitFailed {
                    reason: format!(
                        "{} Prepare intent is silent and remains ambiguous: {error}",
                        member.member().as_str()
                    ),
                })?;
            session
                .status()
                .await
                .map_err(|error| ActivateError::UnitFailed {
                    reason: format!(
                        "{} Prepare intent status is unavailable: {error}",
                        member.member().as_str()
                    ),
                })?
        };
        classify_prepare_status(&status, member, journal)
    }

    async fn resolve_target_spawn(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<TargetSpawnResolution, ActivateError> {
        self.supervisor
            .check_scope(self.inputs.cache_dir, journal)?;
        Ok(match self.supervisor.process_id(&member.id)? {
            Some(process_id) => {
                TargetSpawnResolution::NeedsRetirement(TargetRetirementAuthority::OwnedProcess {
                    process_id,
                })
            }
            None => TargetSpawnResolution::Absent,
        })
    }

    async fn target_retirement_authority(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<TargetRetirementAuthority, ActivateError> {
        self.supervisor
            .check_scope(self.inputs.cache_dir, journal)?;
        let process_id =
            self.supervisor
                .process_id(&member.id)?
                .ok_or_else(|| ActivateError::UnitFailed {
                    reason: format!(
                        "target {} retirement lacks exact owned-process proof",
                        member.member().as_str()
                    ),
                })?;
        Ok(TargetRetirementAuthority::OwnedProcess { process_id })
    }

    async fn retire_target(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
        authority: &TargetRetirementAuthority,
    ) -> Result<(), ActivateError> {
        self.supervisor
            .check_scope(self.inputs.cache_dir, journal)?;
        let TargetRetirementAuthority::OwnedProcess { process_id } = authority else {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "target {} retirement has non-owned process authority",
                    member.member().as_str()
                ),
            });
        };
        self.supervisor
            .stop(&member.id, *process_id, self.inputs.spawner)
    }

    fn release_retired_target(&mut self, member: &TransactionMember) -> Result<(), ActivateError> {
        self.supervisor.release(&member.id)
    }

    async fn resume_old(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<ResumeDisposition, ActivateError> {
        let prepared = self
            .prepared
            .iter_mut()
            .find(|prepared| prepared.entry.socket() == member.endpoint().as_path())
            .ok_or_else(|| ActivateError::UnitFailed {
                reason: format!(
                    "old member {} lacks retained control authority",
                    member.member().as_str()
                ),
            })?;
        let before =
            prepared
                .old_session
                .status()
                .await
                .map_err(|error| ActivateError::UnitFailed {
                    reason: format!(
                        "query old {} before resume: {error}",
                        member.member().as_str()
                    ),
                })?;
        if classify_resume_status(&before, member, journal)? == ResumeEvidence::AlreadyResumed {
            return Ok(ResumeDisposition::Completed);
        }
        let after = prepared
            .old_session
            .abort(&prepared.handoff)
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!("resume old {}: {error}", member.member().as_str()),
            })?;
        if classify_resume_status(&after, member, journal)? != ResumeEvidence::AlreadyResumed {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "old member {} did not attest Running after resume",
                    member.member().as_str()
                ),
            });
        }
        Ok(ResumeDisposition::Completed)
    }
}

/// Drives the single durable rollback interpreter. Every intent is persisted
/// before its external action; any failed barrier leaves `RollingBack` durable.
async fn rollback_transaction<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    journal: &mut ActivationJournal,
    journal_path: &Path,
    prepared: Vec<PreparedMember<C>>,
    targets: Vec<OwnedTarget>,
    reason: String,
) -> Vec<String>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    // This owner is scoped to the coordinator's unit attempt. It keeps exact
    // children across immediate durable-write retries; a terminal failure
    // drops and reaps them instead of leaving a process-global orphan.
    let mut supervisor =
        match ActivationSupervisor::new(inputs.cache_dir, &unit.unit_kind(), targets) {
            Ok(supervisor) => supervisor,
            Err((error, targets)) => {
                let mut diagnostics = vec![error.to_string()];
                diagnostics.extend(ActivationSupervisor::shutdown_targets(
                    targets,
                    inputs.spawner,
                ));
                return diagnostics;
            }
        };
    journal.enter_rollback(reason.clone());
    let mut diagnostics = Vec::new();
    if let Err(error) = journal::write_journal(inputs.cache_dir, journal) {
        let diagnostic = format!("persist rollback decision: {error}");
        journal.enter_rollback(format!("{reason}; {diagnostic}"));
        diagnostics.push(diagnostic);
        if let Err(retry) = journal::write_journal(inputs.cache_dir, journal) {
            diagnostics.push(format!(
                "retry rollback decision while targets remain owned: {retry}"
            ));
            diagnostics.extend(supervisor.shutdown(inputs.spawner));
            return diagnostics;
        }
    }
    let mut actor = NormalRollbackActor {
        inputs,
        prepared,
        supervisor: &mut supervisor,
        #[cfg(test)]
        trace: None,
    };
    let first = drive_rollback(&mut actor, journal, journal_path).await;
    let outcome = if matches!(first, Err(ActivateError::Journal(_))) {
        diagnostics.push(format!(
            "persist rollback barrier: {}",
            first.as_ref().unwrap_err()
        ));
        drive_rollback(&mut actor, journal, journal_path).await
    } else {
        first
    };
    match outcome {
        Ok(RollbackDriveOutcome::Complete) => {}
        Ok(RollbackDriveOutcome::ResumeRequired | RollbackDriveOutcome::AwaitingPeer) => {
            diagnostics.push("normal rollback stopped before durable completion".to_owned());
        }
        Err(error) => diagnostics.push(error.to_string()),
    }
    diagnostics.extend(supervisor.shutdown(inputs.spawner));
    diagnostics
}

fn rollback_outcome(unit: String, reason: String, diagnostics: &[String]) -> UnitOutcome {
    if diagnostics.is_empty() {
        UnitOutcome::RolledBack { unit, reason }
    } else {
        UnitOutcome::Failed {
            unit,
            reason: with_rollback(reason, diagnostics),
        }
    }
}
/// Restores exact prior Zellij registry rows under journal authority.
///
/// # Errors
///
/// Returns an error when journal capability validation or the atomic registry
/// replacement fails.
pub fn restore_old_registry_rows(
    cache_dir: &Path,
    journal: &ActivationJournal,
) -> Result<(), String> {
    if !matches!(journal.unit, UnitKind::Zellij { .. }) {
        return Ok(());
    }
    let registry = Registry::open(cache_dir).map_err(|error| error.to_string())?;
    for member in journal.members() {
        let capability = journal
            .target_restore_capability(
                member.member().as_str(),
                member.endpoint().as_path(),
                member.handoff_id(),
            )
            .map_err(|error| error.to_string())?;
        let old_entry = journal
            .old_registry
            .iter()
            .find(|entry| {
                entry.discovery_key == member.member().as_str()
                    && entry.socket == member.endpoint().as_path()
            })
            .ok_or_else(|| {
                format!(
                    "journal lacks exact old registry row for {}",
                    member.member().as_str()
                )
            })?;
        registry
            .restore_zellij_target(&capability, old_entry)
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Replays the durable Zellij commit intents under the caller's unit lock.
/// Each action accepts its exact retained artifact or receipt as already
/// published, so interruption before the following journal write is safe.
fn publish_commit_artifacts(
    cache_dir: &Path,
    journal: &mut ActivationJournal,
) -> Result<(), ActivateError> {
    let Some(bridge) = journal.bridge() else {
        return Ok(());
    };
    let artifacts = bridge.artifacts.clone();
    let published_before = bridge.progress == BridgeProgress::ReceiptPublished;
    let identity = journal
        .bridge_identity
        .clone()
        .ok_or_else(|| ActivateError::UnitFailed {
            reason: "Zellij commit lacks canonical bridge identity".to_owned(),
        })?;
    let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
    if bridge.progress == BridgeProgress::TargetReloaded {
        journal.bridge_mut().expect("bridge exists").progress =
            BridgeProgress::PreviousPublishIntent;
        journal::write_journal(cache_dir, journal)?;
    }
    if journal.bridge().expect("bridge exists").progress == BridgeProgress::PreviousPublishIntent {
        integration::bridge::publish_previous(
            &identity,
            artifacts.old,
            &artifacts.old_digest,
            &stable,
            artifacts.receipt_preimage.previous_digest.as_ref(),
        )?;
        journal.bridge_mut().expect("bridge exists").progress = BridgeProgress::PreviousPublished;
        journal::write_journal(cache_dir, journal)?;
    }
    if journal.bridge().expect("bridge exists").progress == BridgeProgress::PreviousPublished {
        journal.bridge_mut().expect("bridge exists").progress = BridgeProgress::ReceiptIntent;
        journal::write_journal(cache_dir, journal)?;
    }
    if journal.bridge().expect("bridge exists").progress == BridgeProgress::ReceiptIntent {
        publish_bridge_receipt(
            &identity,
            &artifacts.receipt_preimage,
            &artifacts.receipt_target,
        )?;
        journal.bridge_mut().expect("bridge exists").progress = BridgeProgress::ReceiptPublished;
        journal::write_journal(cache_dir, journal)?;
    }
    if journal.bridge().expect("bridge exists").progress != BridgeProgress::ReceiptPublished {
        return Err(ActivateError::UnitFailed {
            reason: "Zellij commit bridge publication is incomplete".to_owned(),
        });
    }
    // A replay of a previously terminal-looking progress record still checks
    // the exact owned artifacts before the terminal journal is written.
    if published_before {
        integration::bridge::publish_previous(
            &identity,
            artifacts.old,
            &artifacts.old_digest,
            &stable,
            artifacts.receipt_preimage.previous_digest.as_ref(),
        )?;
        publish_bridge_receipt(
            &identity,
            &artifacts.receipt_preimage,
            &artifacts.receipt_target,
        )?;
    }
    Ok(())
}

/// Completes an acknowledged Commit under the caller's activation-unit lock.
/// Member acknowledgements cannot make the transaction terminal until exact
/// old retirement proofs and bridge publication have been durably replayed.
///
/// # Errors
///
/// Returns an error for missing certification, retirement proof, foreign
/// bridge/receipt authority, or any durability failure.
pub fn finish_acknowledged_commit(
    cache_dir: &Path,
    journal: &mut ActivationJournal,
    path: &Path,
) -> Result<bool, ActivateError> {
    if !matches!(
        journal.transaction,
        journal::TransactionPhase::Committing { .. }
    ) || !journal.has_commit_certificate()
    {
        return Err(ActivateError::UnitFailed {
            reason: "broker commit completion lacks a durable Committing certificate".to_owned(),
        });
    }
    if journal.members().iter().any(|member| {
        member.old != OldMemberProgress::Committed
            || member.target != TargetMemberProgress::Committed
    }) {
        return Ok(false);
    }
    let directory = journal::activation_dir(cache_dir);
    for member in journal.members() {
        if !journal::has_old_retirement_receipt(&directory, journal, member)? {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "old member {} lacks exact durable post-stop retirement proof",
                    member.member().as_str()
                ),
            });
        }
    }
    publish_commit_artifacts(cache_dir, journal)?;
    journal.enter_committed();
    journal::write_journal(cache_dir, journal)?;
    cleanup_terminal_transaction(journal, path)?;
    Ok(true)
}

fn publish_bridge_receipt(
    identity: &BridgeIdentity,
    preimage: &integration::receipt::BridgeRecord,
    target: &integration::receipt::BridgeRecord,
) -> Result<(), ActivateError> {
    let directory = identity.directory();
    let mut receipt =
        integration::receipt::load(directory)?.ok_or_else(|| ActivateError::UnitFailed {
            reason: "Zellij bridge commit has no integration receipt".to_owned(),
        })?;
    if receipt.bridge != *preimage && receipt.bridge != *target {
        return Err(ActivateError::UnitFailed {
            reason: "Zellij commit receipt metadata changed outside transaction authority"
                .to_owned(),
        });
    }
    receipt.bridge.clone_from(target);
    integration::receipt::store(directory, &receipt)?;
    Ok(())
}

fn restore_bridge_receipt(
    identity: &BridgeIdentity,
    artifacts: &BridgeArtifacts,
) -> Result<(), ActivateError> {
    let directory = identity.directory();
    let mut receipt =
        integration::receipt::load(directory)?.ok_or_else(|| ActivateError::UnitFailed {
            reason: "Zellij rollback has no integration receipt".to_owned(),
        })?;
    if receipt.bridge != artifacts.receipt_preimage
        && receipt.bridge != artifacts.receipt_target
        && receipt.bridge != artifacts.receipt_rollback
    {
        return Err(ActivateError::UnitFailed {
            reason: "Zellij rollback receipt metadata changed outside transaction authority"
                .to_owned(),
        });
    }
    receipt.bridge.clone_from(&artifacts.receipt_rollback);
    integration::receipt::store(directory, &receipt)?;
    Ok(())
}

/// Removes exact transaction-owned artifacts and then the durable terminal journal.
///
/// # Errors
///
/// Refuses nonterminal state, foreign artifacts, and every unlink or directory-sync failure.
pub fn cleanup_terminal_transaction(
    journal: &ActivationJournal,
    journal_path: &Path,
) -> Result<(), ActivateError> {
    if !matches!(
        journal.directive(),
        TransactionDirective::CleanupCommitted | TransactionDirective::CleanupRolledBack
    ) {
        return Err(ActivateError::UnitFailed {
            reason: "refusing cleanup of nonterminal activation journal".to_owned(),
        });
    }
    let activation_directory = journal_path
        .parent()
        .ok_or_else(|| ActivateError::UnitFailed {
            reason: format!(
                "activation journal {} has no receipt directory",
                journal_path.display()
            ),
        })?;
    for member in journal
        .members()
        .iter()
        .filter(|member| member.target == TargetMemberProgress::Retired)
    {
        journal::remove_target_retirement_receipt(activation_directory, journal, member)?;
    }
    for member in journal
        .members()
        .iter()
        .filter(|member| member.old == OldMemberProgress::Committed)
    {
        journal::remove_old_retirement_receipt(activation_directory, journal, member)?;
    }
    if let (Some(identity), Some(bridge)) = (&journal.bridge_identity, journal.bridge()) {
        integration::bridge::remove_artifact(
            identity,
            bridge.artifacts.old,
            &bridge.artifacts.old_digest,
        )?;
        integration::bridge::remove_artifact(
            identity,
            bridge.artifacts.target,
            &bridge.artifacts.target_digest,
        )?;
    }
    journal::remove_journal(journal_path)?;
    Ok(())
}

/// Records an auditable event carrying identifiers, versions, digests, and
/// state only -- never configuration values, environment values, terminal
/// input, or action payloads. Failures propagate so auditable operations fail
/// closed instead of swallowing the sink error.
fn log(logger: Option<&Logger>, unit: &str, message: &str) -> Result<(), ActivateError> {
    if let Some(logger) = logger {
        let event =
            crate::logging::LogEvent::new(logger.version().to_owned(), unit, "activate", message)?;
        logger.append(&event)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Crash recovery.
// ---------------------------------------------------------------------------

/// Outcome of recovering one journaled unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryOutcome {
    Committed { unit: String },
    RolledBack { unit: String, reason: String },
    Preserved { unit: String, reason: String },
}

/// Replays each activation journal under its unit lock. The durable phase,
/// rather than a target-looking endpoint, determines the transaction's fate.
///
/// `Preparing` and `Activating` persist `RollBack` before restoring the old unit.
/// `Ready` and `Committing` only advance `Commit`; if the exact target is missing,
/// recovery preserves the journal and drained old brokers. It never invents a
/// replacement target. Invalid identity, authority, or journal state also
/// preserves the transaction for diagnosis.
///
/// # Errors
///
/// Returns [`ActivateError`] when the journal directory cannot be scanned.
/// Per-journal outcomes are returned inline, never as an outer error.
pub async fn recover<C, R>(
    cache_dir: &Path,
    control: &C,
    reloader: &R,
    logger: Option<&Logger>,
) -> Result<Vec<RecoveryOutcome>, ActivateError>
where
    C: ControlPort,
    R: HostReloader,
{
    let mut outcomes = Vec::new();
    for (path, journal) in journal::list_journals(cache_dir)? {
        let journal = match journal {
            Ok(journal) => journal,
            Err(error) => {
                outcomes.push(RecoveryOutcome::Preserved {
                    unit: path.display().to_string(),
                    reason: format!("unrecognized journal: {error}"),
                });
                continue;
            }
        };
        let lock = match journal::acquire_unit_lock(cache_dir, &journal.unit) {
            Ok(lock) => lock,
            Err(error) => {
                outcomes.push(RecoveryOutcome::Preserved {
                    unit: path.display().to_string(),
                    reason: format!("recovery unit lock unavailable: {error}"),
                });
                continue;
            }
        };
        outcomes.push(recover_one(cache_dir, control, reloader, journal, &path, logger).await);
        drop(lock);
    }
    Ok(outcomes)
}

async fn recover_one<C, R>(
    cache_dir: &Path,
    control: &C,
    reloader: &R,
    mut journal: ActivationJournal,
    path: &Path,
    logger: Option<&Logger>,
) -> RecoveryOutcome
where
    C: ControlPort,
    R: HostReloader,
{
    let unit = format!("{:?}", journal.unit);
    let result = match journal.directive() {
        TransactionDirective::CleanupCommitted => cleanup_terminal_transaction(&journal, path)
            .map(|()| RecoveryOutcome::Committed { unit: unit.clone() }),
        TransactionDirective::CleanupRolledBack => cleanup_terminal_transaction(&journal, path)
            .map(|()| RecoveryOutcome::RolledBack {
                unit: unit.clone(),
                reason: "terminal rollback cleanup completed".to_owned(),
            }),
        TransactionDirective::Prepare | TransactionDirective::Activate => {
            journal.enter_rollback("recovery selected rollback before durable Ready".to_owned());
            if let Err(error) = journal::write_journal(cache_dir, &journal) {
                Err(ActivateError::UnitFailed {
                    reason: format!("cannot persist rollback decision: {error}"),
                })
            } else {
                recover_rollback(cache_dir, control, reloader, &mut journal, path)
                    .await
                    .map(|()| RecoveryOutcome::RolledBack {
                        unit: unit.clone(),
                        reason: "pre-Ready transaction rolled back".to_owned(),
                    })
            }
        }
        TransactionDirective::RollBack => {
            recover_rollback(cache_dir, control, reloader, &mut journal, path)
                .await
                .map(|()| RecoveryOutcome::RolledBack {
                    unit: unit.clone(),
                    reason: "durable rollback completed".to_owned(),
                })
        }
        TransactionDirective::Commit if !journal.has_commit_certificate() => {
            Err(ActivateError::UnitFailed {
                reason: "Ready journal lacks an exact target incarnation certificate".to_owned(),
            })
        }
        TransactionDirective::Commit => recover_commit(cache_dir, control, &mut journal, path)
            .await
            .map(|()| RecoveryOutcome::Committed { unit: unit.clone() }),
    };
    match result {
        Ok(outcome) => {
            let _ = log(logger, &unit, "activation recovery converged");
            outcome
        }
        Err(error) => RecoveryOutcome::Preserved {
            unit,
            reason: error.to_string(),
        },
    }
}

/// The endpoint and compatibility alone are not a Commit permit: a restarted
/// broker can reuse both. Compare its live reply and the complete persisted
/// registry incarnation against the proof sealed before Ready.
async fn certified_target_session<C: ControlPort>(
    cache_dir: &Path,
    control: &C,
    journal: &ActivationJournal,
    member: &TransactionMember,
) -> Result<C::Session, ActivateError> {
    if !journal.has_commit_certificate() {
        return Err(ActivateError::UnitFailed {
            reason: "Ready lacks an exact target incarnation certificate".to_owned(),
        });
    }
    let mut target = control
        .connect(member.endpoint().as_path())
        .await
        .map_err(|error| ActivateError::UnitFailed {
            reason: format!(
                "Ready target {} is unavailable: {error}",
                member.member().as_str()
            ),
        })?;
    let status = target
        .status()
        .await
        .map_err(|error| ActivateError::UnitFailed {
            reason: format!(
                "cannot inspect Ready target {}: {error}",
                member.member().as_str()
            ),
        })?;
    let rows = Registry::open(cache_dir)?.entries()?;
    let mut matching = rows
        .iter()
        .filter(|row| row.socket == member.endpoint().as_path());
    let proof = journal
        .ready_proof()
        .and_then(|proof| proof.member(&member.id));
    if !status_attests_journal(&status, journal)
        || status.handoff_id != Some(member.handoff_id())
        || status.current != journal.target_record
        || status.live_server.discovery_key != member.member().as_str()
        || status.live_server.host
            != if matches!(journal.unit, UnitKind::Zellij { .. }) {
                muxe_protocol::wire::HostKind::Zellij
            } else {
                muxe_protocol::wire::HostKind::Herdr
            }
        || status.lifecycle != LifecycleState::Running
        || status.target.is_some()
        || matching
            .next()
            .zip(proof)
            .is_none_or(|(row, proof)| !proof.matches(row, &status.live_server.server_id))
        || matching.next().is_some()
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "Ready target {} is missing or differs from its sealed broker incarnation",
                member.member().as_str()
            ),
        });
    }
    Ok(target)
}

async fn recover_commit<C: ControlPort>(
    cache_dir: &Path,
    control: &C,
    journal: &mut ActivationJournal,
    path: &Path,
) -> Result<(), ActivateError> {
    if matches!(journal.directive(), TransactionDirective::Commit)
        && !matches!(
            journal.transaction,
            journal::TransactionPhase::Committing { .. }
        )
    {
        journal.enter_committing();
        journal::write_journal(cache_dir, journal)?;
    }
    let directory = journal::activation_dir(cache_dir);
    for index in 0..journal.members().len() {
        let member = &journal.members()[index];
        if !matches!(
            member.old,
            OldMemberProgress::CommitIntent | OldMemberProgress::Committed
        ) || !journal::has_old_retirement_receipt(&directory, journal, member)?
        {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "old member {} lacks exact durable post-stop retirement proof",
                    member.member().as_str()
                ),
            });
        }
        if member.old == OldMemberProgress::CommitIntent {
            journal.members_mut()[index].old = OldMemberProgress::Committed;
            journal::write_journal(cache_dir, journal)?;
        }
    }
    let snapshots = journal.members().to_vec();
    for (index, member) in snapshots.iter().enumerate() {
        let mut target = certified_target_session(cache_dir, control, journal, member).await?;
        if member.target == TargetMemberProgress::Committed {
            continue;
        }
        journal.members_mut()[index].target = TargetMemberProgress::CommitIntent;
        journal::write_journal(cache_dir, journal)?;
        target
            .commit(&member.handoff_id())
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!("commit target {}: {error}", member.member().as_str()),
            })?;
        journal.members_mut()[index].target = TargetMemberProgress::Committed;
        journal::write_journal(cache_dir, journal)?;
    }
    if finish_acknowledged_commit(cache_dir, journal, path)? {
        Ok(())
    } else {
        Err(ActivateError::UnitFailed {
            reason: "commit remains incomplete".to_owned(),
        })
    }
}

/// Coordinator/broker recovery capabilities consumed by the shared rollback driver.
struct RecoveryRollbackActor<'a, C, R> {
    cache_dir: &'a Path,
    control: &'a C,
    reloader: &'a R,
    local_member: Option<&'a ActivationMemberId>,
    local_status: Option<&'a ActivationStatus>,
    local_can_resume: bool,
    #[cfg(test)]
    trace: Option<std::sync::Arc<std::sync::Mutex<Vec<String>>>>,
}

impl<C, R> RecoveryRollbackActor<'_, C, R>
where
    C: ControlPort,
{
    async fn target_status(
        &self,
        member: &TransactionMember,
    ) -> Result<ActivationStatus, ActivateError> {
        let mut target = self
            .control
            .connect(member.endpoint().as_path())
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "target {} launch remains ambiguous: {error}",
                    member.member().as_str()
                ),
            })?;
        target
            .status()
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "target {} launch status is ambiguous: {error}",
                    member.member().as_str()
                ),
            })
    }
}

impl<C, R> RollbackActor for RecoveryRollbackActor<'_, C, R>
where
    C: ControlPort,
    R: HostReloader,
{
    fn cache_dir(&self) -> &Path {
        self.cache_dir
    }

    fn reloader(&self) -> &dyn HostReloader {
        self.reloader
    }

    fn can_resume(&self, member: &TransactionMember) -> bool {
        self.local_member
            .is_none_or(|local| self.local_can_resume && member.member() == local)
    }
    fn observe(&self, stage: &'static str, action: &RollbackAction, journal: &ActivationJournal) {
        #[cfg(test)]
        if let Some(trace) = &self.trace {
            record_rollback_trace(trace, stage, action, journal);
        }
        #[cfg(not(test))]
        let _ = (stage, action, journal);
    }

    async fn resolve_prepare(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<PrepareResolution, ActivateError> {
        if self.local_member == Some(member.member()) {
            let status = self.local_status.ok_or_else(|| ActivateError::UnitFailed {
                reason: format!(
                    "{} local Prepare intent lacks exact broker status",
                    member.member().as_str()
                ),
            })?;
            return classify_prepare_status(status, member, journal);
        }
        let mut session = self
            .control
            .connect(member.endpoint().as_path())
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "{} Prepare intent is silent and remains ambiguous: {error}",
                    member.member().as_str()
                ),
            })?;
        let status = session
            .status()
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "{} Prepare intent status is unavailable: {error}",
                    member.member().as_str()
                ),
            })?;
        classify_prepare_status(&status, member, journal)
    }

    async fn resolve_target_spawn(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<TargetSpawnResolution, ActivateError> {
        self.target_retirement_authority(member, journal)
            .await
            .map(TargetSpawnResolution::NeedsRetirement)
    }

    async fn target_retirement_authority(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<TargetRetirementAuthority, ActivateError> {
        let status = self.target_status(member).await?;
        classify_target_status(&status, member, journal)?;
        Ok(TargetRetirementAuthority::RemoteServer {
            server_id: status.live_server.server_id,
        })
    }

    async fn retire_target(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
        authority: &TargetRetirementAuthority,
    ) -> Result<(), ActivateError> {
        let TargetRetirementAuthority::RemoteServer { server_id } = authority else {
            return Err(ActivateError::UnitFailed {
                reason: "owned target authority cannot outlive its activation supervisor"
                    .to_owned(),
            });
        };
        let mut target = self
            .control
            .connect(member.endpoint().as_path())
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "target {} retirement is ambiguous: {error}",
                    member.member().as_str()
                ),
            })?;
        let status = target
            .status()
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "target {} status is ambiguous: {error}",
                    member.member().as_str()
                ),
            })?;
        classify_target_status(&status, member, journal)?;
        if status.live_server.server_id != *server_id {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "target {} process authority changed before retirement",
                    member.member().as_str()
                ),
            });
        }
        let retired = target.abort(&member.handoff_id()).await.map_err(|error| {
            ActivateError::UnitFailed {
                reason: format!("retire target {}: {error}", member.member().as_str()),
            }
        })?;
        if retired.live_server.discovery_key != member.member().as_str()
            || retired.live_server.server_id != *server_id
            || !status_attests_journal(&retired, journal)
            || retired.current != journal.target_record
            || retired.lifecycle != LifecycleState::Retired
            || retired.target.is_some()
            || retired.handoff_id.is_some()
        {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "target {} stop barrier returned mismatched evidence",
                    member.member().as_str()
                ),
            });
        }
        Ok(())
    }

    async fn resume_old(
        &mut self,
        member: &TransactionMember,
        journal: &ActivationJournal,
    ) -> Result<ResumeDisposition, ActivateError> {
        if self.local_member == Some(member.member()) {
            let status = self.local_status.ok_or_else(|| ActivateError::UnitFailed {
                reason: format!(
                    "{} local Resume intent lacks exact broker status",
                    member.member().as_str()
                ),
            })?;
            return match classify_resume_status(status, member, journal)? {
                ResumeEvidence::AlreadyResumed => Ok(ResumeDisposition::Completed),
                ResumeEvidence::NeedsResume => Ok(ResumeDisposition::DeferredToLocalBroker),
            };
        }
        let mut old = self
            .control
            .connect(member.endpoint().as_path())
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "old member {} resume remains unproven: {error}",
                    member.member().as_str()
                ),
            })?;
        let before = old
            .status()
            .await
            .map_err(|error| ActivateError::UnitFailed {
                reason: format!(
                    "old member {} status failed: {error}",
                    member.member().as_str()
                ),
            })?;
        if classify_resume_status(&before, member, journal)? == ResumeEvidence::AlreadyResumed {
            return Ok(ResumeDisposition::Completed);
        }
        let after =
            old.abort(&member.handoff_id())
                .await
                .map_err(|error| ActivateError::UnitFailed {
                    reason: format!("resume old {}: {error}", member.member().as_str()),
                })?;
        if classify_resume_status(&after, member, journal)? != ResumeEvidence::AlreadyResumed {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "old member {} did not attest Running after resume",
                    member.member().as_str()
                ),
            });
        }
        Ok(ResumeDisposition::Completed)
    }
}

/// Result of advancing broker-local rollback under the unit/journal lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrokerRollbackOutcome {
    /// The exact local member has durable `ResumeIntent`; the service may resume it.
    ResumeLocal,
    /// Other members must advance through their own exact authority.
    AwaitingPeers,
    /// The terminal state and cleanup completed.
    Complete,
}

/// Advances broker-disconnect rollback through the shared durable driver.
///
/// # Errors
///
/// Preserves the journal on any silent, foreign, or mismatched participant state.
pub async fn prepare_broker_rollback<C, R>(
    cache_dir: &Path,
    control: &C,
    reloader: &R,
    journal: &mut ActivationJournal,
    path: &Path,
    local_member: &ActivationMemberId,
    local_status: &ActivationStatus,
) -> Result<BrokerRollbackOutcome, ActivateError>
where
    C: ControlPort,
    R: HostReloader,
{
    let local_can_resume = journal
        .members()
        .iter()
        .find(|member| member.member() == local_member)
        .is_some_and(|member| {
            local_status.current == member.old_record
                && matches!(
                    local_status.lifecycle,
                    LifecycleState::Running | LifecycleState::Draining
                )
        });
    let mut actor = RecoveryRollbackActor {
        cache_dir,
        control,
        reloader,
        local_member: Some(local_member),
        local_status: Some(local_status),
        local_can_resume,
        #[cfg(test)]
        trace: None,
    };
    drive_rollback(&mut actor, journal, path)
        .await
        .map(|outcome| match outcome {
            RollbackDriveOutcome::ResumeRequired => BrokerRollbackOutcome::ResumeLocal,
            RollbackDriveOutcome::AwaitingPeer => BrokerRollbackOutcome::AwaitingPeers,
            RollbackDriveOutcome::Complete => BrokerRollbackOutcome::Complete,
        })
}

/// Continues the same broker rollback after a durable local acknowledgement.
///
/// # Errors
///
/// Returns an error when journal persistence, exact participant evidence, or
/// terminal cleanup cannot be completed.
pub async fn continue_broker_rollback<C, R>(
    cache_dir: &Path,
    control: &C,
    reloader: &R,
    journal: &mut ActivationJournal,
    path: &Path,
    local_member: &ActivationMemberId,
) -> Result<BrokerRollbackOutcome, ActivateError>
where
    C: ControlPort,
    R: HostReloader,
{
    let mut actor = RecoveryRollbackActor {
        cache_dir,
        control,
        reloader,
        local_member: Some(local_member),
        local_status: None,
        local_can_resume: true,
        #[cfg(test)]
        trace: None,
    };
    drive_rollback(&mut actor, journal, path)
        .await
        .map(|outcome| match outcome {
            RollbackDriveOutcome::ResumeRequired => BrokerRollbackOutcome::ResumeLocal,
            RollbackDriveOutcome::AwaitingPeer => BrokerRollbackOutcome::AwaitingPeers,
            RollbackDriveOutcome::Complete => BrokerRollbackOutcome::Complete,
        })
}

async fn recover_rollback<C, R>(
    cache_dir: &Path,
    control: &C,
    reloader: &R,
    journal: &mut ActivationJournal,
    path: &Path,
) -> Result<(), ActivateError>
where
    C: ControlPort,
    R: HostReloader,
{
    let mut actor = RecoveryRollbackActor {
        cache_dir,
        control,
        reloader,
        local_member: None,
        local_status: None,
        local_can_resume: false,
        #[cfg(test)]
        trace: None,
    };
    match drive_rollback(&mut actor, journal, path).await? {
        RollbackDriveOutcome::Complete => Ok(()),
        RollbackDriveOutcome::ResumeRequired | RollbackDriveOutcome::AwaitingPeer => {
            Err(ActivateError::UnitFailed {
                reason: "coordinator rollback stopped before durable completion".to_owned(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_protocol::control::{ControlDecoder, ControlPolicy};
    use muxe_protocol::wire::{HostKind, LiveServerIdentity, ServerId};
    use std::sync::{Arc, Mutex};
    use tokio::{net::UnixListener, task::JoinHandle};

    /// Fixed process mechanics for transaction tests, without host selection.
    struct FixedSpawnSelection {
        program: &'static str,
        args: &'static [&'static str],
    }

    impl TargetSpawnPolicy for FixedSpawnSelection {
        fn render(
            &self,
            _member: &SpawnMember<'_>,
        ) -> Result<(PathBuf, Vec<OsString>), ActivateError> {
            Ok((
                PathBuf::from(self.program),
                self.args.iter().map(OsString::from).collect(),
            ))
        }
    }

    impl TargetSpawnSelector for FixedSpawnSelection {
        fn select(&self, _unit: &UnitKind) -> &dyn TargetSpawnPolicy {
            self
        }
    }

    static SLEEP_SPAWN: FixedSpawnSelection = FixedSpawnSelection {
        program: "/bin/sleep",
        args: &["30"],
    };
    static TRUE_SPAWN: FixedSpawnSelection = FixedSpawnSelection {
        program: "/bin/true",
        args: &[],
    };

    fn old_record() -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: "0.1.0".to_owned(),
            target_triple: "test-triple".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        }
    }

    fn target_record() -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: "0.2.0".to_owned(),
            zellij: Some(muxe_protocol::control::ZellijCompatibility {
                source_revision: muxe_zellij_protocol::compat::pinned_source_revision().to_owned(),
                generated_action_fingerprint:
                    muxe_zellij_protocol::compat::generated_action_fingerprint(),
                bridge_protocol_fingerprint:
                    muxe_zellij_protocol::compat::bridge_protocol_fingerprint(),
                bridge_build_id: Some(muxe_zellij_protocol::compat::bridge_build_id()),
            }),
            ..old_record()
        }
    }

    fn handoff(n: u8) -> HandoffId {
        HandoffId([n; 16])
    }

    fn identity(discovery: &str) -> LiveServerIdentity {
        LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: discovery.to_owned(),
            server_id: ServerId::new("id"),
        }
    }

    /// Script for one framed fixture broker.
    struct BrokerScript {
        current: CompatibilityRecord,
        prepare_refusals: usize,
        supports_supplied_handoff: bool,
        host: HostKind,
        bridge_unit: Option<muxe_protocol::BridgeUnitId>,
    }

    impl BrokerScript {
        fn status(
            &self,
            current: &CompatibilityRecord,
            handoff: Option<HandoffId>,
            discovery: &str,
            lifecycle: LifecycleState,
        ) -> ActivationStatus {
            let mut status = status_of(current, handoff, discovery, lifecycle);
            status.bridge_unit = self.bridge_unit;
            status.live_server.host = self.host;
            status
        }
    }

    /// Sends the broker prelude on accept, exactly like production: the peer
    /// role in the prelude drives the coordinator decoder before any frame.
    async fn send_broker_prelude(stream: &mut tokio::net::UnixStream) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let prelude =
            muxe_protocol::frame::Prelude::control(muxe_protocol::wire::PeerRole::Broker).encode();
        stream.write_all(&prelude).await?;
        stream.flush().await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "framed fixture wire state machine mirroring production (prelude, decode, Status, Prepare, Commit, Abort, Retire) where the drain-while-stream-open ordering is the behavior under test; splitting request handling from the accept loop would hide it"
    )]
    async fn serve_old(
        socket: PathBuf,
        mut script: BrokerScript,
        discovery: String,
        events: Arc<Mutex<Vec<String>>>,
    ) {
        use tokio::io::AsyncWriteExt;
        let mut listener_slot = Some(UnixListener::bind(&socket).expect("bind old listener"));
        std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .expect("owner-only old listener");
        let mut prepared_handoff: Option<HandoffId> = None;
        // Accept connections one at a time: short-lived probes and fast-path
        // checks are each served to EOF, so they never steal the retained
        // drain stream. Draining drops the listener and unlinks the path;
        // later connections on the path belong to the claimed target.
        while let Some(listener) = listener_slot.as_ref() {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            if send_broker_prelude(&mut stream).await.is_err() {
                continue;
            }
            let mut decoder = ControlDecoder::new(ControlPolicy::broker());
            let mut buffer = [0u8; 8192];
            loop {
                let read = match tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                let mut requests = Vec::new();
                decoder
                    .push(&buffer[..read], |message| {
                        if let ControlMessage::Request(request) = message {
                            requests.push(request);
                        }
                    })
                    .expect("decode coordinator frame");
                for request in requests {
                    let result = match request.operation {
                        ControlOperation::StatusAt { .. } => ControlResult::Error {
                            diagnostic: "old fixture broker cannot attest target readiness"
                                .to_owned(),
                        },
                        ControlOperation::Status => {
                            let mut status = script.status(
                                &script.current,
                                prepared_handoff,
                                &discovery,
                                if prepared_handoff.is_some() {
                                    LifecycleState::Draining
                                } else {
                                    LifecycleState::Running
                                },
                            );
                            if prepared_handoff.is_some() {
                                status.target = Some(target_record());
                            }
                            if !script.supports_supplied_handoff {
                                status.prepare_handoff = None;
                            }
                            ControlResult::Status(status)
                        }
                        ControlOperation::Prepare { target, handoff_id } => {
                            assert_eq!(target.muxe_version, "0.2.0");
                            if script.prepare_refusals != 0 {
                                script.prepare_refusals -= 1;
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("prepare-refused".to_owned());
                                ControlResult::Error {
                                    diagnostic: "prepare refused: non-cancellable work".to_owned(),
                                }
                            } else {
                                let handoff = handoff_id;
                                prepared_handoff = Some(handoff);
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("prepared".to_owned());
                                // Drain: close and unlink the listener while
                                // keeping this accepted stream open.
                                drop(listener_slot.take());
                                let _ = std::fs::remove_file(&socket);
                                let mut status = script.status(
                                    &script.current,
                                    Some(handoff),
                                    &discovery,
                                    LifecycleState::Draining,
                                );
                                status.target = Some((*target).clone());
                                ControlResult::Prepared(status)
                            }
                        }
                        ControlOperation::Commit { handoff_id } => {
                            if Some(handoff_id) == prepared_handoff {
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("old-committed".to_owned());
                                ControlResult::Committed(script.status(
                                    &target_record(),
                                    Some(handoff_id),
                                    &discovery,
                                    LifecycleState::SupervisorOnly,
                                ))
                            } else {
                                ControlResult::Error {
                                    diagnostic: "handoff mismatch".to_owned(),
                                }
                            }
                        }
                        ControlOperation::Abort { handoff_id } => {
                            if Some(handoff_id) == prepared_handoff {
                                prepared_handoff = None;
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("old-aborted".to_owned());
                                ControlResult::Aborted(script.status(
                                    &script.current,
                                    None,
                                    &discovery,
                                    LifecycleState::Running,
                                ))
                            } else {
                                ControlResult::Error {
                                    diagnostic: "handoff mismatch".to_owned(),
                                }
                            }
                        }
                        ControlOperation::Retire => ControlResult::Retired(script.status(
                            &script.current,
                            None,
                            &discovery,
                            LifecycleState::Retired,
                        )),
                    };
                    let response = ControlMessage::Response(ControlResponse {
                        request_id: request.request_id,
                        result,
                    });
                    let payload = serde_json::to_vec(&response).unwrap();
                    // Polling coordinators may drop between status and read; a
                    // dead stream ends this connection, never the task.
                    if stream
                        .write_all(
                            &u32::try_from(payload.len())
                                .expect("fixture frame fits u32")
                                .to_be_bytes(),
                        )
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if stream.write_all(&payload).await.is_err() {
                        break;
                    }
                    if stream.flush().await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    fn status_of(
        current: &CompatibilityRecord,
        handoff: Option<HandoffId>,
        discovery: &str,
        lifecycle: LifecycleState,
    ) -> ActivationStatus {
        ActivationStatus {
            lifecycle,
            phase: muxe_protocol::control::ActivationPhase::Legacy,
            registration: None,
            live_server: identity(discovery),
            current: current.clone(),
            target: None,
            handoff_id: handoff,
            prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
            bridge_unit: None,
            ready: None,
        }
    }

    use muxe_protocol::control::{
        ControlMessage, ControlOperation, ControlResponse, ControlResult,
    };

    struct FixtureHerdrSpawner {
        cache: PathBuf,
    }

    impl BrokerSpawner for FixtureHerdrSpawner {
        fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
            let handle = ProcessSpawner.spawn_target(request)?;
            let journal = journal::list_journals(&self.cache)?
                .into_iter()
                .next()
                .expect("fixture owns one activation journal")
                .1?;
            let member = &journal.members()[0];
            let mut entry = BrokerEntry::now(
                "herdr",
                member.member().as_str(),
                member.endpoint().as_path().to_path_buf(),
                handle.child.id(),
            );
            entry.live_server = Some("id".to_owned());
            entry.registration_id = Some(
                muxe_protocol::control::BrokerRegistrationId::generate()
                    .map_err(|error| RegistryError::Entropy(error.to_string()))?,
            );
            Registry::open(&self.cache)?.register_herdr(entry)?;
            Ok(handle)
        }

        fn stop_target(&self, handle: &mut TargetHandle) -> Result<(), ActivateError> {
            ProcessSpawner.stop_target(handle)
        }
    }

    struct RejectingTargetSpawner;

    impl BrokerSpawner for RejectingTargetSpawner {
        fn spawn_target(&self, _request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
            Err(ActivateError::Spawn(
                "target child refused startup".to_owned(),
            ))
        }

        fn stop_target(&self, handle: &mut TargetHandle) -> Result<(), ActivateError> {
            ProcessSpawner.stop_target(handle)
        }
    }

    struct Fixture {
        _temp: tempfile::TempDir,
        cache: PathBuf,
        config: PathBuf,
        control: LiveControl,
        spawner: ProcessSpawner,
        herdr_spawner: FixtureHerdrSpawner,
        reloader: FixtureReloader,
        preflight: FixturePreflight,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone, Default)]
    struct FixtureReloader {
        fail_sessions: Vec<String>,
        reloaded: Arc<Mutex<Vec<(String, String)>>>,
        reloaded_bytes: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl HostReloader for FixtureReloader {
        fn reload_bridge(&self, session: &str, bridge_url: &str) -> Result<(), ActivateError> {
            let path =
                bridge_url
                    .strip_prefix("file:")
                    .ok_or_else(|| ActivateError::UnitFailed {
                        reason: format!("fixture bridge URL is not file-based: {bridge_url}"),
                    })?;
            let bytes = std::fs::read(path).map_err(|error| ActivateError::UnitFailed {
                reason: format!("fixture cannot observe bridge bytes: {error}"),
            })?;
            self.reloaded
                .lock()
                .expect("fixture reloads are not poisoned")
                .push((session.to_owned(), bridge_url.to_owned()));
            self.reloaded_bytes
                .lock()
                .expect("fixture bridge bytes are not poisoned")
                .push(bytes);
            if self.fail_sessions.iter().any(|entry| entry == session) {
                return Err(ActivateError::Reload {
                    session: session.to_owned(),
                    detail: "reload refused".to_owned(),
                });
            }
            Ok(())
        }
        fn bridge_loaded(&self, _session: &str, _bridge_url: &str) -> Result<bool, ActivateError> {
            Ok(false)
        }
    }

    #[derive(Clone, Default)]
    struct FixturePreflight {
        fail_config: Option<String>,
    }

    impl Preflight for FixturePreflight {
        async fn validate_config(&self) -> Result<(), String> {
            self.fail_config.clone().map_or(Ok(()), Err)
        }
        async fn validate_host<H: HostPreflight>(&self, _host: &H) -> Result<(), String> {
            Ok(())
        }
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::TempDir::new().unwrap();
            std::fs::set_permissions(
                temp.path(),
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
            let cache = temp.path().join("cache");
            let config = temp.path().join("config");
            for directory in [&cache, &config] {
                std::fs::create_dir_all(directory).unwrap();
                std::fs::set_permissions(
                    directory,
                    std::os::unix::fs::PermissionsExt::from_mode(0o700),
                )
                .unwrap();
            }
            let herdr_spawner = FixtureHerdrSpawner {
                cache: cache.clone(),
            };
            Self {
                cache,
                config,
                _temp: temp,
                control: LiveControl,
                spawner: ProcessSpawner,
                herdr_spawner,
                reloader: FixtureReloader::default(),
                preflight: FixturePreflight::default(),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Registers a framed old broker and returns its socket. The listener
        /// stays bound (liveness probe connects) until prepare unlinks it.
        async fn old_broker(
            &self,
            discovery_key: &str,
            script: BrokerScript,
        ) -> (PathBuf, JoinHandle<()>) {
            let socket = self.cache.join(format!("{discovery_key}.sock"));
            std::fs::create_dir_all(&self.cache).unwrap();
            let registry = Registry::open(&self.cache).unwrap();
            registry
                .register(BrokerEntry {
                    host_kind: "herdr".to_owned(),
                    discovery_key: discovery_key.to_owned(),
                    socket: socket.clone(),
                    server_pid: std::process::id(),
                    started_at: 1,
                    registration_id: None,
                    bridge_identity: None,
                    bridge_member: None,
                    handoff_id: None,
                    live_server: Some(discovery_key.to_owned()),
                })
                .unwrap();
            let events = self.events.clone();
            let key = discovery_key.to_owned();
            let handle = tokio::spawn(serve_old(socket.clone(), script, key, events));
            // Wait until the listener binds; a blocking sleep would starve a
            // single-threaded test runtime before the server task runs.
            for _ in 0..400 {
                if socket.exists() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            (socket, handle)
        }

        fn herdr_inputs(
            &self,
        ) -> ActivateInputs<'_, LiveControl, FixtureHerdrSpawner, FixtureReloader, FixturePreflight>
        {
            ActivateInputs {
                config_dir: &self.config,
                cache_dir: &self.cache,
                target: target_record(),
                staged_bridge: None,
                spawn_policy: &SLEEP_SPAWN,
                scope: HostScope::Herdr,
                current: None,
                control: &self.control,
                spawner: &self.herdr_spawner,
                reloader: &self.reloader,
                preflight: &self.preflight,
                readiness_deadline: Duration::from_secs(5),
                poll_interval: Duration::from_millis(5),
                hooks: ActivateHooks::default(),
                logger: None,
            }
        }
    }

    fn herdr_script() -> BrokerScript {
        BrokerScript {
            current: old_record(),
            prepare_refusals: 0,
            supports_supplied_handoff: true,
            host: HostKind::Herdr,
            bridge_unit: None,
        }
    }
    #[expect(
        clippy::needless_pass_by_value,
        reason = "test fixtures pass owned paths directly from one-shot setup expressions"
    )]
    fn zellij_entry(socket: PathBuf, bridge_path: PathBuf) -> BrokerEntry {
        let identity = BridgeIdentity::resolve(
            bridge_path.parent().unwrap(),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        BrokerEntry {
            host_kind: "zellij".to_owned(),
            discovery_key: "session".to_owned(),
            socket,
            server_pid: std::process::id(),
            started_at: 1,
            registration_id: None,
            bridge_identity: Some(identity),
            bridge_member: Some(
                super::super::registry::BridgeMemberId::new("session".to_owned()).unwrap(),
            ),
            handoff_id: None,
            live_server: Some("session".to_owned()),
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "test fixtures consume the stable path while constructing one planned unit"
    )]
    fn test_zellij_unit(stable: PathBuf, entries: Vec<BrokerEntry>) -> PlannedUnit {
        let identity = BridgeIdentity::resolve(
            stable.parent().unwrap(),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let registered = entries
            .into_iter()
            .map(|entry| RegisteredBroker::zellij(entry, &identity).unwrap())
            .collect();
        zellij_unit(identity, registered).unwrap()
    }

    async fn zellij_member(
        fixture: &Fixture,
        bridge_path: &Path,
        current: CompatibilityRecord,
    ) -> (BrokerEntry, JoinHandle<()>) {
        let identity = BridgeIdentity::resolve(
            bridge_path.parent().unwrap(),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let (socket, old) = fixture
            .old_broker(
                "session",
                BrokerScript {
                    current,
                    host: HostKind::Zellij,
                    bridge_unit: Some(identity.unit()),
                    ..herdr_script()
                },
            )
            .await;
        let entry = zellij_entry(socket, bridge_path.to_path_buf());
        Registry::open(&fixture.cache)
            .expect("registry opens")
            .register(entry.clone())
            .expect("Zellij registration replaces the fixture host entry");
        (entry, old)
    }

    fn zellij_inputs<'a>(
        fixture: &'a Fixture,
        staged_bridge: &[u8],
    ) -> ActivateInputs<'a, LiveControl, ProcessSpawner, FixtureReloader, FixturePreflight> {
        ActivateInputs {
            config_dir: &fixture.config,
            cache_dir: &fixture.cache,
            target: target_record(),
            staged_bridge: Some(StagedBridge {
                bytes: staged_bridge.to_vec(),
            }),
            scope: HostScope::Zellij,
            current: None,
            control: &fixture.control,
            spawner: &fixture.spawner,
            reloader: &fixture.reloader,
            preflight: &fixture.preflight,
            spawn_policy: &SLEEP_SPAWN,
            readiness_deadline: Duration::from_millis(100),
            poll_interval: Duration::from_millis(5),
            hooks: ActivateHooks::default(),
            logger: None,
        }
    }

    fn store_bridge_receipt(fixture: &Fixture, _stable: &Path, installed_digest: Sha256Digest) {
        let identity = integration::bridge_identity(&fixture.config).unwrap();
        integration::receipt::store(
            identity.directory(),
            &integration::receipt::Receipt {
                schema_version: integration::receipt::RECEIPT_SCHEMA_VERSION,
                bridge: integration::receipt::BridgeRecord {
                    bridge_identity: identity.clone(),
                    installed_version: "0.1.0".to_owned(),
                    installed_digest,
                    previous_digest: None,
                    bridge_compat: None,
                },
                configs: Vec::new(),
            },
        )
        .expect("bridge receipt stores");
    }
    fn producer_wasm_bytes() -> Vec<u8> {
        let path = std::env::var_os("MUXE_TEST_PACKAGED_WASM")
            .expect("packaged activation tests require MUXE_TEST_PACKAGED_WASM");
        std::fs::read(path).expect("read pinned producer WASM")
    }
    fn prepare_bridge_directory(fixture: &Fixture) -> PathBuf {
        let directory = integration::integration_dir(&fixture.config);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::set_permissions(
            &directory,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        directory
    }
    fn previous_producer_wasm_bytes() -> Vec<u8> {
        std::env::var_os("MUXE_TEST_PREVIOUS_WASM").map_or_else(
            || b"old-bridge-bytes".to_vec(),
            |path| std::fs::read(path).expect("read preserved previous producer WASM"),
        )
    }

    #[tokio::test]
    #[ignore = "requires pinned producer WASM; run mise run verify-activation-preflight"]
    async fn zellij_fast_path_requires_verified_stable_bridge_bytes() {
        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        let target_bytes = producer_wasm_bytes();
        std::fs::write(&stable, &target_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        store_bridge_receipt(&fixture, &stable, Sha256Digest::from_bytes(&target_bytes));
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let outcome = activate_unit_with_global_preflight(
            &zellij_inputs(&fixture, &target_bytes),
            &test_zellij_unit(stable.clone(), vec![entry]),
        )
        .await
        .expect("matching broker and bridge take the fast path");
        assert!(
            matches!(outcome, UnitOutcome::Unchanged { .. }),
            "matching broker and bridge should be unchanged: {outcome:?}"
        );
        assert_eq!(std::fs::read(&stable).unwrap(), target_bytes);
        let mut names = std::fs::read_dir(integration::integration_dir(&fixture.config))
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        let mut expected = vec![
            std::ffi::OsString::from(integration::bridge::BRIDGE_FILE_NAME),
            std::ffi::OsString::from(integration::receipt::RECEIPT_FILE_NAME),
        ];
        expected.sort_unstable();
        assert_eq!(
            names, expected,
            "second Unchanged activation leaves no bridge staging artifact"
        );
        old.abort();
    }

    #[tokio::test]
    #[ignore = "requires pinned producer WASM; run mise run verify-activation-preflight"]
    async fn zellij_stale_receipt_owned_bridge_transacts_despite_target_record() {
        {
            let fixture = Fixture::new();
            let stable = integration::stable_bridge_path(&fixture.config);
            prepare_bridge_directory(&fixture);
            let target_bytes = producer_wasm_bytes();
            let foreign_previous = b"foreign-rollback-artifact";
            std::fs::write(&stable, &target_bytes).unwrap();
            std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
                .unwrap();
            store_bridge_receipt(&fixture, &stable, Sha256Digest::from_bytes(&target_bytes));
            let directory = integration::integration_dir(&fixture.config);
            let mut receipt = integration::receipt::load(&directory).unwrap().unwrap();
            receipt.bridge.previous_digest = Some(integration::receipt::Sha256Digest::from_bytes(
                b"recorded-rollback",
            ));
            integration::receipt::store(&directory, &receipt).unwrap();
            let previous = integration::bridge::previous_path(&stable);
            std::fs::write(&previous, foreign_previous).unwrap();
            std::fs::set_permissions(
                &previous,
                std::os::unix::fs::PermissionsExt::from_mode(0o600),
            )
            .unwrap();
            let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
            let result = activate_unit_with_global_preflight(
                &zellij_inputs(&fixture, &target_bytes),
                &test_zellij_unit(stable, vec![entry]),
            )
            .await;
            assert!(matches!(
                result,
                Err(ActivateError::Preflight(message))
                    if message.contains("rollback copy preflight failed")
            ));
            assert!(
                fixture
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|event| { !event.contains("prepare") && !event.contains("spawn") }),
                "foreign .previous rejection precedes Prepare and target spawn"
            );
            old.abort();
        }
        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        let old_bytes = previous_producer_wasm_bytes();
        let target_bytes = producer_wasm_bytes();
        assert_ne!(
            old_bytes, target_bytes,
            "packaged smoke needs distinct previous and current producer artifacts"
        );
        std::fs::write(&stable, &old_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        store_bridge_receipt(&fixture, &stable, Sha256Digest::from_bytes(&old_bytes));
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let outcome = activate_unit_with_global_preflight(
            &zellij_inputs(&fixture, &target_bytes),
            &test_zellij_unit(stable.clone(), vec![entry]),
        )
        .await
        .expect("transaction returns a unit outcome");
        assert!(matches!(outcome, UnitOutcome::RolledBack { .. }));
        let reloaded_bytes = fixture.reloader.reloaded_bytes.lock().unwrap();
        assert_eq!(
            &*reloaded_bytes,
            &vec![target_bytes.clone(), old_bytes.clone()],
            "reload observes target bytes before rollback and old bytes after rollback"
        );
        assert_eq!(
            std::fs::read(&stable).unwrap(),
            old_bytes,
            "rollback preserves the receipt-owned old bytes"
        );
        old.abort();
    }

    #[tokio::test]
    #[ignore = "requires pinned producer WASM; run with packaged activation fixtures"]
    async fn zellij_target_spawn_failure_keeps_receipt_owned_predecessor() {
        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        let old_bytes = previous_producer_wasm_bytes();
        let target_bytes = producer_wasm_bytes();
        assert_ne!(old_bytes, target_bytes);
        std::fs::write(&stable, &old_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        store_bridge_receipt(&fixture, &stable, Sha256Digest::from_bytes(&old_bytes));
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let reject = RejectingTargetSpawner;
        let inputs = ActivateInputs {
            config_dir: &fixture.config,
            cache_dir: &fixture.cache,
            target: target_record(),
            staged_bridge: Some(StagedBridge {
                bytes: target_bytes,
            }),
            scope: HostScope::Zellij,
            current: None,
            control: &fixture.control,
            spawner: &reject,
            reloader: &fixture.reloader,
            preflight: &fixture.preflight,
            spawn_policy: &SLEEP_SPAWN,
            readiness_deadline: Duration::from_millis(100),
            poll_interval: Duration::from_millis(5),
            hooks: ActivateHooks::default(),
            logger: None,
        };
        let outcome = activate_unit_with_global_preflight(
            &inputs,
            &test_zellij_unit(stable.clone(), vec![entry.clone()]),
        )
        .await
        .expect("spawn failure returns one unit outcome");
        assert!(matches!(outcome, UnitOutcome::RolledBack { .. }));
        assert_eq!(std::fs::read(&stable).unwrap(), old_bytes);
        let receipt = integration::receipt::load(&integration::integration_dir(&fixture.config))
            .unwrap()
            .unwrap();
        assert_eq!(
            receipt.bridge.installed_digest,
            Sha256Digest::from_bytes(&old_bytes)
        );
        assert_eq!(
            Registry::open(&fixture.cache).unwrap().entries().unwrap(),
            vec![entry]
        );
        assert!(journal::list_journals(&fixture.cache).unwrap().is_empty());
        old.abort();
    }

    #[tokio::test]
    #[ignore = "requires pinned producer WASM; run mise run verify-activation-preflight"]
    async fn zellij_missing_or_unrecognized_bridge_never_reports_unchanged() {
        for corrupt in [false, true] {
            let fixture = Fixture::new();
            let stable = integration::stable_bridge_path(&fixture.config);
            prepare_bridge_directory(&fixture);
            let target_bytes = producer_wasm_bytes();
            let original = if corrupt {
                std::fs::write(&stable, &target_bytes).unwrap();
                std::fs::set_permissions(
                    &stable,
                    std::os::unix::fs::PermissionsExt::from_mode(0o600),
                )
                .unwrap();
                store_bridge_receipt(
                    &fixture,
                    &stable,
                    Sha256Digest::from_bytes(b"wrong-receipt"),
                );
                Some(target_bytes.as_slice())
            } else {
                None
            };
            if !corrupt {
                assert!(!stable.exists(), "absent read must not create the bridge");
            }
            let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
            let result = activate_unit_with_global_preflight(
                &zellij_inputs(&fixture, &target_bytes),
                &test_zellij_unit(stable.clone(), vec![entry]),
            )
            .await;
            match result {
                Err(ActivateError::Preflight(message)) => {
                    if corrupt {
                        assert!(message.contains("unrecognized bridge bytes"), "{message}");
                    } else {
                        assert!(message.contains("receipt-owned Zellij bridge"), "{message}");
                    }
                    if let Some(bytes) = original {
                        assert_eq!(std::fs::read(&stable).unwrap(), bytes);
                    }
                }
                Ok(outcome) => {
                    panic!("unrecognized bridge unexpectedly reached activation: {outcome:?}");
                }
                Err(error) => panic!("unexpected activation error: {error}"),
            }
            old.abort();
        }

        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        let target_bytes = producer_wasm_bytes();
        std::fs::write(&stable, &target_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let result = activate_unit_with_global_preflight(
            &zellij_inputs(&fixture, &target_bytes),
            &test_zellij_unit(stable.clone(), vec![entry]),
        )
        .await;
        assert!(matches!(
            result,
            Err(ActivateError::Preflight(message))
                if message.contains("receipt-owned Zellij bridge")
        ));
        assert_eq!(std::fs::read(&stable).unwrap(), target_bytes);
        old.abort();
    }
    #[tokio::test]
    #[ignore = "requires pinned producer WASM; run mise run verify-activation-preflight"]
    async fn zellij_symlink_with_matching_receipt_fails_closed() {
        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        let target = fixture.config.join("target-bridge.wasm");
        let target_bytes = producer_wasm_bytes();
        std::fs::write(&target, &target_bytes).unwrap();
        std::fs::set_permissions(&target, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        std::fs::write(&stable, &target_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        store_bridge_receipt(&fixture, &stable, Sha256Digest::from_bytes(&target_bytes));
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let unit = test_zellij_unit(stable.clone(), vec![entry]);
        std::fs::remove_file(&stable).unwrap();
        std::os::unix::fs::symlink(&target, &stable).unwrap();
        let result =
            activate_unit_with_global_preflight(&zellij_inputs(&fixture, &target_bytes), &unit)
                .await;
        assert!(matches!(
            result,
            Err(ActivateError::Preflight(message))
                if message.contains("symlinked or non-regular stable bridge leaf")
        ));
        assert!(
            std::fs::symlink_metadata(&stable)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(&target).unwrap(), target_bytes);
        old.abort();
    }

    #[tokio::test]
    #[ignore = "requires pinned producer WASM; run mise run verify-activation-preflight"]
    async fn zellij_receipt_path_and_mode_fail_closed() {
        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        let target_bytes = producer_wasm_bytes();
        std::fs::write(&stable, &target_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o600))
            .unwrap();
        let wrong_path = fixture.config.join("wrong-bridge.wasm");
        let directory = integration::integration_dir(&fixture.config);
        fsutil::write_atomic(
            &directory.join(integration::receipt::RECEIPT_FILE_NAME),
            &serde_json::to_vec(&serde_json::json!({
                "schema_version": integration::receipt::RECEIPT_SCHEMA_VERSION,
                "bridge": {
                    "canonical_path": wrong_path,
                    "installed_version": "0.1.0",
                    "installed_digest": integration::receipt::Sha256Digest::from_bytes(&target_bytes),
                    "previous_digest": null,
                    "bridge_compat": null,
                },
                "configs": [],
            }))
            .unwrap(),
            "receipt",
        )
        .unwrap();
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let result = activate_unit_with_global_preflight(
            &zellij_inputs(&fixture, &target_bytes),
            &test_zellij_unit(stable.clone(), vec![entry]),
        )
        .await;
        assert!(matches!(
            result,
            Err(ActivateError::Preflight(message))
                if message.contains("semantically corrupt")
        ));
        assert_eq!(std::fs::read(&stable).unwrap(), target_bytes);
        assert!(fixture.reloader.reloaded.lock().unwrap().is_empty());
        old.abort();

        let fixture = Fixture::new();
        let stable = integration::stable_bridge_path(&fixture.config);
        prepare_bridge_directory(&fixture);
        std::fs::write(&stable, &target_bytes).unwrap();
        std::fs::set_permissions(&stable, std::os::unix::fs::PermissionsExt::from_mode(0o644))
            .unwrap();
        store_bridge_receipt(&fixture, &stable, Sha256Digest::from_bytes(&target_bytes));
        let (entry, old) = zellij_member(&fixture, &stable, target_record()).await;
        let result = activate_unit_with_global_preflight(
            &zellij_inputs(&fixture, &target_bytes),
            &test_zellij_unit(stable.clone(), vec![entry]),
        )
        .await;
        assert!(matches!(
            result,
            Err(ActivateError::Preflight(message))
                if message.contains("not owner-only")
        ));
        assert_eq!(std::fs::read(&stable).unwrap(), target_bytes);
        assert_eq!(
            std::os::unix::fs::MetadataExt::mode(&std::fs::metadata(&stable).unwrap()) & 0o777,
            0o644,
            "preflight must not chmod an unowned-mode bridge"
        );
        old.abort();
    }

    #[tokio::test]
    async fn prepare_refusal_cleans_up_rollback_journal() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture
            .old_broker(
                "server",
                BrokerScript {
                    prepare_refusals: 1,
                    ..herdr_script()
                },
            )
            .await;
        let first = Box::pin(activate(fixture.herdr_inputs())).await.unwrap();
        assert!(matches!(first.units[0], UnitOutcome::RolledBack { .. }));
        assert!(
            journal::list_journals(&fixture.cache).unwrap().is_empty(),
            "definite refusal reaches durable rollback cleanup"
        );
        let events = fixture
            .events
            .lock()
            .expect("fixture events are not poisoned");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.as_str() == "prepare-refused")
                .count(),
            1
        );
        old.abort();
    }

    #[tokio::test]
    async fn legacy_prepare_peer_is_refused_before_journal_or_drain() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture
            .old_broker(
                "server",
                BrokerScript {
                    supports_supplied_handoff: false,
                    ..herdr_script()
                },
            )
            .await;
        let report = Box::pin(activate(fixture.herdr_inputs())).await.unwrap();
        assert!(matches!(report.units[0], UnitOutcome::Failed { .. }));
        assert!(journal::list_journals(&fixture.cache).unwrap().is_empty());
        let events = fixture
            .events
            .lock()
            .expect("fixture events are not poisoned");
        assert!(
            !events.iter().any(|event| event == "prepared"),
            "legacy peer is rejected by Status capability before Prepare"
        );
        old.abort();
    }
    #[tokio::test]
    async fn absent_target_restores_old_stack() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture.old_broker("server", herdr_script()).await;
        // No target task is spawned by the test, but the spawner still runs
        // real process mechanics (/bin/sleep child, killed on abort).
        let report = Box::pin(activate(fixture.herdr_inputs())).await.unwrap();
        assert!(matches!(report.units[0], UnitOutcome::RolledBack { .. }));
        let events = fixture
            .events
            .lock()
            .expect("fixture events are not poisoned");
        assert!(events.contains(&"old-aborted".to_owned()));
        old.abort();
    }

    #[tokio::test]
    async fn unknown_journal_is_preserved() {
        let fixture = Fixture::new();
        crate::fsutil::ensure_owner_dir(&journal::activation_dir(&fixture.cache)).unwrap();
        let path = journal::activation_dir(&fixture.cache).join("herdr-x.json");
        crate::fsutil::write_atomic(&path, b"{corrupt", "activation").unwrap();
        let outcomes = recover(&fixture.cache, &fixture.control, &fixture.reloader, None)
            .await
            .unwrap();
        assert!(matches!(outcomes[0], RecoveryOutcome::Preserved { .. }));
        assert!(path.exists());
    }

    #[tokio::test]
    async fn zellij_cli_reloader_runs_the_real_command_shape() {
        // A TempDir fixture executable stands in for the Zellij CLI: the real
        // Command path, argument vector, and failure detection run without
        // touching any live host.
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let recorded = temp.path().join("args");
        let program = temp.path().join("zellij");
        crate::generated_executable::write_executable_script(&program, |writer| {
            std::io::Write::write_all(
                writer,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nif [ \"$2\" = \"bad\" ]; then exit 3; fi\nexit 0\n",
                    recorded.display()
                )
                .as_bytes(),
            )
        })
        .unwrap();
        let reloader = ZellijCliReloader {
            program: Some(program),
        };
        reloader
            .reload_bridge("session-a", "file:/bridge.wasm")
            .unwrap();
        let args = std::fs::read_to_string(&recorded).unwrap();
        assert!(
            args.contains(
                "--session\nsession-a\naction\nstart-or-reload-plugin\nfile:/bridge.wasm"
            ) || args.contains("start-or-reload-plugin")
        );
        let error = reloader
            .reload_bridge("bad", "file:/bridge.wasm")
            .unwrap_err();
        assert!(matches!(error, ActivateError::Reload { .. }));
    }

    #[test]
    fn zellij_coldstart_refuses_foreign_loaded_alias() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let program = temp.path().join("zellij");
        crate::generated_executable::write_executable_script(&program, |writer| {
            std::io::Write::write_all(
                writer,
                b"#!/bin/sh\nprintf '%s\\n' '[{\"is_plugin\":true,\"plugin_url\":\"muxe\",\"exited\":false}]'\n",
            )
        })
        .unwrap();
        let reloader = ZellijCliReloader {
            program: Some(program),
        };
        let foreign = format!("file:{}/muxe-zellij.wasm", temp.path().display());
        assert!(matches!(
            reloader.bridge_loaded("owned", &foreign),
            Err(ActivateError::Reload { .. })
        ));
    }

    #[tokio::test]
    async fn preflight_failure_changes_nothing() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture.old_broker("server", herdr_script()).await;
        let mut inputs = fixture.herdr_inputs();
        let preflight = FixturePreflight {
            fail_config: Some("configuration does not parse".to_owned()),
        };
        inputs.preflight = &preflight;
        let result = Box::pin(activate(inputs)).await;
        assert!(matches!(result, Err(ActivateError::Preflight(_))));
        assert!(journal::list_journals(&fixture.cache).unwrap().is_empty());
        old.abort();
    }

    fn ready_census(registered: &[&str], members: Option<&[&str]>) -> TargetReadiness {
        TargetReadiness {
            registered_clients: registered.iter().map(ToString::to_string).collect(),
            member_clients: members.map_or(0, <[_]>::len) as u64,
            member_ids: members.map(|set| set.iter().map(ToString::to_string).collect()),
            proof_epoch: None,
        }
    }

    fn census_member(socket: PathBuf, host_kind: &str, discovery: &str) -> BrokerEntry {
        BrokerEntry {
            host_kind: host_kind.to_owned(),
            discovery_key: discovery.to_owned(),
            socket,
            server_pid: 1,
            started_at: 1,
            registration_id: None,
            bridge_identity: None,
            bridge_member: None,
            handoff_id: None,
            live_server: None,
        }
    }

    fn census_status(
        handoff: HandoffId,
        discovery: &str,
        ready: Option<TargetReadiness>,
    ) -> ActivationStatus {
        ActivationStatus {
            lifecycle: LifecycleState::Running,
            phase: muxe_protocol::control::ActivationPhase::TargetGated,
            registration: None,
            live_server: identity(discovery),
            current: target_record(),
            target: None,
            handoff_id: Some(handoff),
            prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
            bridge_unit: None,
            ready,
        }
    }
    fn attested_zellij_member(root: &Path, socket: PathBuf, discovery: &str) -> RegisteredBroker {
        std::fs::set_permissions(root, std::os::unix::fs::PermissionsExt::from_mode(0o700))
            .unwrap();
        let identity = BridgeIdentity::resolve(
            &root.join(discovery),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let mut member = census_member(socket, "zellij", discovery);
        member.bridge_identity = Some(identity.clone());
        member.bridge_member =
            Some(super::super::registry::BridgeMemberId::new(discovery.to_owned()).unwrap());
        RegisteredBroker::zellij(member, &identity).unwrap()
    }

    fn zellij_readiness_host<'a>(
        member: &'a RegisteredBroker,
        census: &'a MemberCensus,
    ) -> ZellijActivation<'a> {
        ZellijActivation {
            identity: member
                .bridge_identity()
                .expect("fixed Zellij fixture has bridge authority"),
            entries: std::slice::from_ref(member),
            census,
        }
    }

    fn attested_zellij_status(
        mut status: ActivationStatus,
        member: &RegisteredBroker,
    ) -> ActivationStatus {
        status.bridge_unit = Some(
            member
                .bridge_identity()
                .expect("fixed Zellij fixture has bridge authority")
                .unit(),
        );
        status.live_server.host = HostKind::Zellij;
        status
    }

    #[test]
    fn status_bridge_attestation_mismatch_and_absence_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let mut entry = census_member(PathBuf::from("/tmp/status.sock"), "zellij", "session-a");
        entry.bridge_identity = Some(identity.clone());
        entry.bridge_member =
            Some(super::super::registry::BridgeMemberId::new("session-a".to_owned()).unwrap());
        let activation = ActivationId::from_bytes([3; 16]).unwrap();
        let member = TransactionMember::new(
            activation,
            ActivationMemberId::new("session-a".to_owned()).unwrap(),
            MemberEndpoint::new(entry.socket.clone()).unwrap(),
            handoff(3),
            old_record(),
        )
        .unwrap();
        let mut journal = ActivationJournal::new(
            activation,
            UnitKind::Zellij {
                bridge_unit: identity.unit(),
            },
            target_record(),
            vec![member],
        )
        .unwrap();
        let bridge_digest = crate::integration::receipt::Sha256Digest::from_bytes(b"test-bridge");
        let receipt_preimage = crate::integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: "0.1.0".to_owned(),
            installed_digest: bridge_digest.clone(),
            previous_digest: None,
            bridge_compat: old_record().zellij,
        };
        let receipt_target = crate::integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: target_record().muxe_version,
            installed_digest: bridge_digest.clone(),
            previous_digest: Some(bridge_digest.clone()),
            bridge_compat: target_record().zellij,
        };
        let receipt_rollback = crate::integration::receipt::BridgeRecord {
            previous_digest: Some(bridge_digest.clone()),
            ..receipt_preimage.clone()
        };
        journal
            .bind_zellij_authority(
                identity.clone(),
                MemberCensus::from_members(vec![
                    super::super::registry::BridgeMemberId::new("session-a".to_owned()).unwrap(),
                ])
                .unwrap(),
                BridgeArtifacts {
                    old: BridgeArtifactId::new(activation, BridgeArtifactRole::Old),
                    target: BridgeArtifactId::new(activation, BridgeArtifactRole::Target),
                    old_digest: bridge_digest.clone(),
                    target_digest: bridge_digest,
                    receipt_preimage,
                    receipt_target,
                    receipt_rollback,
                },
            )
            .unwrap();
        let mut status = census_status(handoff(3), "session-a", None);
        let entry = RegisteredBroker::zellij(entry, &identity).unwrap();
        let census = MemberCensus::default();
        let host = zellij_readiness_host(&entry, &census);

        assert!(!status_attests_journal(&status, &journal));
        assert!(!host.attests_entry(&status, &entry));
        status.bridge_unit = Some(muxe_protocol::BridgeUnitId::from_canonical_bytes(b"wrong"));
        assert!(!host.attests_entry(&status, &entry));
        assert!(!status_attests_journal(&status, &journal));
        status.bridge_unit = Some(identity.unit());
        assert!(host.attests_entry(&status, &entry));
        assert!(status_attests_journal(&status, &journal));
    }

    #[test]
    fn zellij_census_requires_exact_set_coverage() {
        assert!(zellij_census_covered(&ready_census(
            &["b", "a"],
            Some(&["a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(
            &["a"],
            Some(&["a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(
            &["a", "newcomer"],
            Some(&["a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(
            &["a", "b", "c"],
            Some(&["a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(
            &["a", "a"],
            Some(&["a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(
            &["a", "b"],
            Some(&["a", "a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(&["a", "b"], None)));
        assert!(zellij_census_covered(&ready_census(&[], Some(&[]))));
        assert!(!zellij_census_covered(&ready_census(
            &[],
            Some(&["a", "b"])
        )));
        assert!(!zellij_census_covered(&ready_census(&[""], Some(&[""]))));
        assert!(!zellij_census_covered(&ready_census(
            &["a"],
            Some(&["a", ""])
        )));
    }

    #[test]
    fn target_ready_gates_zellij_census_but_not_herdr_health() {
        let expected = handoff(9);
        let socket = PathBuf::from("/run/census.sock");
        let herdr =
            RegisteredBroker::herdr(census_member(socket.clone(), "herdr", "herdr.sock")).unwrap();
        assert!(target_ready(
            &census_status(expected, "herdr.sock", None),
            &herdr,
            &expected,
            &target_record(),
            &HerdrActivation(&herdr),
        ));
        let temp = tempfile::tempdir().unwrap();
        let zellij = attested_zellij_member(temp.path(), socket, "session-a");
        let census = MemberCensus::default();
        let host = zellij_readiness_host(&zellij, &census);
        assert!(!target_ready(
            &attested_zellij_status(census_status(expected, "session-a", None), &zellij),
            &zellij,
            &expected,
            &target_record(),
            &host,
        ));
        assert!(!target_ready(
            &attested_zellij_status(
                census_status(
                    expected,
                    "session-a",
                    Some(ready_census(&["a"], Some(&["a", "b"]))),
                ),
                &zellij,
            ),
            &zellij,
            &expected,
            &target_record(),
            &host,
        ));
        assert!(target_ready(
            &attested_zellij_status(
                census_status(
                    expected,
                    "session-a",
                    Some(ready_census(&["a", "b"], Some(&["a", "b"]))),
                ),
                &zellij,
            ),
            &zellij,
            &expected,
            &target_record(),
            &host,
        ));
        assert!(!target_ready(
            &attested_zellij_status(
                census_status(
                    expected,
                    "session-a",
                    Some(ready_census(&["a", "b"], Some(&["a", "b"]))),
                ),
                &zellij,
            ),
            &zellij,
            &handoff(3),
            &target_record(),
            &host,
        ));
    }

    #[test]
    fn target_ready_rejects_missing_or_zero_bridge_build_id() {
        let expected = handoff(9);
        let socket = PathBuf::from("/run/census.sock");
        let temp = tempfile::tempdir().unwrap();
        let member = attested_zellij_member(temp.path(), socket, "session-a");
        let census = MemberCensus::default();
        let host = zellij_readiness_host(&member, &census);
        let ready = Some(ready_census(&["a", "b"], Some(&["a", "b"])));

        let mut missing = target_record();
        missing
            .zellij
            .as_mut()
            .expect("test target has Zellij compatibility")
            .bridge_build_id = None;
        let mut missing_status =
            attested_zellij_status(census_status(expected, "session-a", ready.clone()), &member);
        missing_status.current = missing.clone();
        assert!(!target_ready(
            &missing_status,
            &member,
            &expected,
            &missing,
            &host
        ));

        let mut zero = target_record();
        zero.zellij
            .as_mut()
            .expect("test target has Zellij compatibility")
            .bridge_build_id = Some(muxe_protocol::SchemaFingerprint([0; 32]));
        let mut zero_status =
            attested_zellij_status(census_status(expected, "session-a", ready), &member);
        zero_status.current = zero.clone();
        assert!(!target_ready(
            &zero_status,
            &member,
            &expected,
            &zero,
            &host
        ));
    }

    /// Serves one fixed readiness status over the real control framing so
    /// `wait_ready` is proven through the production client, not a scripted
    /// session type. Accepts every connection in a loop because the
    /// coordinator reconnects on each poll.
    fn serve_readiness_status(socket: PathBuf, status: ActivationStatus) -> JoinHandle<()> {
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = UnixListener::bind(&socket).expect("bind readiness peer");
            std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))
                .expect("owner-only readiness peer");
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let status = status.clone();
                tokio::spawn(async move {
                    if send_broker_prelude(&mut stream).await.is_err() {
                        return;
                    }
                    let mut decoder = ControlDecoder::new(ControlPolicy::broker());
                    let mut buffer = [0u8; 8192];
                    loop {
                        let read = match AsyncReadExt::read(&mut stream, &mut buffer).await {
                            Ok(0) | Err(_) => break,
                            Ok(read) => read,
                        };
                        let mut pending = Vec::new();
                        decoder
                            .push(&buffer[..read], |message| {
                                if let ControlMessage::Request(request) = message {
                                    let result = match request.operation {
                                        ControlOperation::Status => {
                                            ControlResult::Status(status.clone())
                                        }
                                        ControlOperation::Commit { .. } => {
                                            ControlResult::Committed(status.clone())
                                        }
                                        ControlOperation::Abort { .. } => {
                                            ControlResult::Aborted(status.clone())
                                        }
                                        _ => ControlResult::Error {
                                            diagnostic: "unsupported fixture operation".to_owned(),
                                        },
                                    };
                                    pending.push((request.request_id, result));
                                }
                            })
                            .expect("decode readiness frame");
                        for (request_id, result) in pending {
                            let response =
                                ControlMessage::Response(ControlResponse { request_id, result });
                            let payload = serde_json::to_vec(&response).unwrap();
                            let length =
                                u32::try_from(payload.len()).expect("status frame fits u32");
                            if stream.write_all(&length.to_be_bytes()).await.is_err() {
                                return;
                            }
                            if stream.write_all(&payload).await.is_err() {
                                return;
                            }
                            if stream.flush().await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        })
    }

    /// Serves an evolving round sequence over the real control framing: each
    /// status poll consumes the next round, saturating at the last. Proves a
    /// fresh target with a nonempty session stays unready through empty early
    /// rounds and opens only when real registrations arrive.
    fn serve_readiness_rounds(socket: PathBuf, rounds: Vec<ActivationStatus>) -> JoinHandle<()> {
        use std::sync::{Arc, Mutex};
        let rounds = Arc::new(Mutex::new(rounds));
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = UnixListener::bind(&socket).expect("bind rounds peer");
            std::fs::set_permissions(&socket, std::os::unix::fs::PermissionsExt::from_mode(0o600))
                .expect("owner-only rounds peer");
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let rounds = Arc::clone(&rounds);
                tokio::spawn(async move {
                    if send_broker_prelude(&mut stream).await.is_err() {
                        return;
                    }
                    let mut decoder = ControlDecoder::new(ControlPolicy::broker());
                    let mut buffer = [0u8; 8192];
                    loop {
                        let read = match AsyncReadExt::read(&mut stream, &mut buffer).await {
                            Ok(0) | Err(_) => break,
                            Ok(read) => read,
                        };
                        let mut pending = Vec::new();
                        decoder
                            .push(&buffer[..read], |message| {
                                if let ControlMessage::Request(request) = message {
                                    pending.push(request.request_id);
                                }
                            })
                            .expect("decode rounds frame");
                        for request_id in pending {
                            let status = {
                                let mut rounds = rounds.lock().expect("rounds are readable");
                                if rounds.len() > 1 {
                                    rounds.remove(0)
                                } else {
                                    rounds.first().cloned().expect("rounds never empty")
                                }
                            };
                            let response = ControlMessage::Response(ControlResponse {
                                request_id,
                                result: ControlResult::Status(status),
                            });
                            let payload = serde_json::to_vec(&response).unwrap();
                            let length =
                                u32::try_from(payload.len()).expect("status frame fits u32");
                            if stream.write_all(&length.to_be_bytes()).await.is_err() {
                                return;
                            }
                            if stream.write_all(&payload).await.is_err() {
                                return;
                            }
                            if stream.flush().await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        })
    }

    #[tokio::test]
    async fn wait_ready_blocks_partial_census_and_passes_full() {
        let temp = tempfile::tempdir().expect("readiness sockets");
        let handoff = handoff(7);
        let partial = temp.path().join("partial.sock");
        let member = attested_zellij_member(temp.path(), partial.clone(), "session-a");
        let census = MemberCensus::default();
        let host = zellij_readiness_host(&member, &census);
        serve_readiness_status(
            partial,
            attested_zellij_status(
                census_status(
                    handoff,
                    "session-a",
                    Some(ready_census(&["a"], Some(&["a", "b"]))),
                ),
                &member,
            ),
        );
        let blocked = wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            &host,
            Instant::now() + Duration::from_millis(150),
            Duration::from_millis(10),
        )
        .await
        .expect_err("a partial census never reads ready");
        assert!(
            matches!(blocked, ActivateError::ReadinessTimeout { .. }),
            "a partial census fails closed by timeout, never by commit"
        );
        let full = temp.path().join("full.sock");
        let member = attested_zellij_member(temp.path(), full.clone(), "session-a-full");
        let host = zellij_readiness_host(&member, &census);
        serve_readiness_status(
            full,
            attested_zellij_status(
                census_status(
                    handoff,
                    "session-a-full",
                    Some(ready_census(&["a", "b"], Some(&["a", "b"]))),
                ),
                &member,
            ),
        );
        wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            &host,
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await
        .expect("a full fresh census reads ready");
    }

    #[test]
    fn barriered_preflight_race_detects_late_registration_before_drain() {
        use std::os::unix::net::UnixListener;

        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::os::unix::fs::PermissionsExt::from_mode(0o700))
            .unwrap();
        let identity = BridgeIdentity::resolve(
            &temp.path().join("config/integrations/zellij"),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let registry = Registry::open(&cache).unwrap();
        let first_socket = temp.path().join("first.sock");
        let _first_listener = UnixListener::bind(&first_socket).unwrap();
        {
            let guard =
                super::super::registry::BridgeUnitGuard::acquire(&cache, identity.clone()).unwrap();
            registry
                .register_zellij(
                    &guard,
                    zellij_entry(
                        first_socket,
                        identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME)),
                    ),
                )
                .unwrap();
        }
        let snapshot = select_units(&registry.probe().unwrap().live, HostScope::Zellij, None)
            .unwrap()
            .pop()
            .unwrap();

        let second_socket = temp.path().join("second.sock");
        let _second_listener = UnixListener::bind(&second_socket).unwrap();
        let preflight_release = std::sync::Arc::new(std::sync::Barrier::new(2));
        let (registered_tx, registered_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let worker_cache = cache.clone();
            let worker_identity = identity.clone();
            let worker_registry = registry.clone();
            let worker_release = std::sync::Arc::clone(&preflight_release);
            scope.spawn(move || {
                worker_release.wait();
                let guard = super::super::registry::BridgeUnitGuard::acquire(
                    &worker_cache,
                    worker_identity.clone(),
                )
                .unwrap();
                let mut late = zellij_entry(
                    second_socket,
                    worker_identity
                        .stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME)),
                );
                late.discovery_key = "session-late".to_owned();
                late.bridge_member = Some(
                    super::super::registry::BridgeMemberId::new("session-late".to_owned()).unwrap(),
                );
                worker_registry.register_zellij(&guard, late).unwrap();
                // The preflight side acquires this lock after the completion signal.
                drop(guard);
                registered_tx.send(()).unwrap();
            });

            // The snapshot above is the async preflight boundary. Release an
            // ordinary registrar while preflight is still outside the unit
            // lock, then take the lock and re-probe before any drain.
            preflight_release.wait();
            registered_rx.recv().unwrap();
            let _activation_lock =
                journal::acquire_unit_lock(&cache, &snapshot.unit_kind()).unwrap();
            let PlannedUnit::Zellij {
                bridge_identity,
                entries,
                census,
            } = &snapshot
            else {
                panic!("fixed Zellij fixture produced another unit");
            };
            assert!(
                ZellijActivation {
                    identity: bridge_identity,
                    entries,
                    census,
                }
                .revalidate_locked(&cache)
                .is_err(),
                "locked revalidation refuses the newly registered member"
            );
            let rows = registry.entries().unwrap();
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().any(|row| row.discovery_key == "session-late"));
            assert!(journal::list_journals(&cache).unwrap().is_empty());
        });
    }

    #[test]
    fn selection_preserves_legacy_rows_and_rejects_only_selected_malformed_hosts() {
        let temp = tempfile::tempdir().unwrap();
        let registry = Registry::open(temp.path()).unwrap();
        let mut legacy = BrokerEntry::now("herdr", "", temp.path().join("legacy.sock"), 0);
        legacy.started_at = 0;
        legacy.live_server = Some(String::new());
        registry.register(legacy.clone()).unwrap();
        let unknown = BrokerEntry::now(
            "future-host",
            "unknown",
            temp.path().join("unknown.sock"),
            0,
        );
        registry.register(unknown).unwrap();
        let malformed =
            BrokerEntry::now("zellij", "session", temp.path().join("malformed.sock"), 0);
        registry.register(malformed).unwrap();
        let rows = registry.entries().unwrap();
        let units = select_units(&rows, HostScope::Herdr, None).unwrap();
        let PlannedUnit::Herdr { entry } = &units[0] else {
            panic!("selected Herdr row must retain its concrete lifecycle policy");
        };
        assert!(entry.matches_recorded(&legacy));
        assert_eq!(entry.server_pid().get(), 0);
        assert_eq!(entry.registration_id(), None);
        assert!(select_units(&rows, HostScope::Zellij, None).is_err());
        assert!(select_units(&rows, HostScope::All, None).is_err());
        assert_eq!(
            registry.entries().unwrap(),
            rows,
            "selection never rewrites untrusted storage"
        );
    }

    /// Two actual service registrations sharing one stable bridge form a
    /// single atomic group: real bound listener sockets registered through
    /// the file registry prove live, and selection yields one Zellij group
    /// of two. A bridgeless registration (the old serve default) is dropped
    /// from selection instead of splitting the unit.
    #[test]
    fn bridge_sharing_registrations_form_one_atomic_group() {
        use std::os::unix::net::UnixListener;
        let temp = tempfile::tempdir().expect("registry dir");
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .expect("owner-only test dir");
        let registry = Registry::open(temp.path()).expect("owner-only registry");
        let integration = temp.path().join("zellij");
        let identity = BridgeIdentity::resolve(
            &integration,
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let mut bound = Vec::new();
        for (session, socket) in [("session-b", "b.sock"), ("session-a", "a.sock")] {
            let path = temp.path().join(socket);
            bound.push(UnixListener::bind(&path).expect("service listener"));
            let mut entry = BrokerEntry::now("zellij", session, path, std::process::id());
            entry.bridge_identity = Some(identity.clone());
            entry.bridge_member =
                Some(super::super::registry::BridgeMemberId::new(session.to_owned()).unwrap());
            entry.live_server = Some(session.to_owned());
            registry.register(entry).expect("service registration");
        }
        registry
            .register(BrokerEntry::now(
                "zellij",
                "session-c",
                temp.path().join("c.sock"),
                std::process::id(),
            ))
            .expect("bridgeless registration");
        let live = registry.probe().expect("liveness probe").live;
        assert_eq!(live.len(), 2, "only the bound listeners prove live");
        let units = select_units(&live, HostScope::All, None).expect("unit selection");
        assert_eq!(
            units.len(),
            1,
            "bridge-sharing brokers commit as one atomic unit, bridgeless dropped"
        );
        match &units[0] {
            PlannedUnit::Zellij {
                bridge_identity,
                entries,
                census,
            } => {
                assert_eq!(
                    entries
                        .iter()
                        .map(|entry| entry.discovery_key().as_str())
                        .collect::<Vec<_>>(),
                    vec!["session-a", "session-b"],
                    "selection normalizes registration order"
                );
                assert_eq!(census.members().len(), 2);
                let _lock = journal::acquire_unit_lock(temp.path(), &units[0].unit_kind()).unwrap();
                ZellijActivation {
                    identity: bridge_identity,
                    entries,
                    census,
                }
                .revalidate_locked(temp.path())
                .expect("the same members in registry insertion order remain admissible");
            }
            unit @ PlannedUnit::Herdr { .. } => panic!("expected one Zellij group, found {unit:?}"),
        }
        drop(bound);
    }

    #[tokio::test]
    async fn wait_ready_opens_only_on_real_regs_with_empty_valid() {
        let temp = tempfile::tempdir().expect("rounds sockets");
        let handoff = handoff(11);
        let evolving = temp.path().join("evolving.sock");
        let member = attested_zellij_member(temp.path(), evolving.clone(), "session-a");
        let census = MemberCensus::default();
        let host = zellij_readiness_host(&member, &census);
        let empty_round = attested_zellij_status(
            census_status(handoff, "session-a", Some(ready_census(&[], Some(&["a"])))),
            &member,
        );
        let full_round = attested_zellij_status(
            census_status(
                handoff,
                "session-a",
                Some(ready_census(&["a"], Some(&["a"]))),
            ),
            &member,
        );
        serve_readiness_rounds(evolving, vec![empty_round, full_round]);
        wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            &host,
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await
        .expect("real registrations open a fresh target");
        let vacant = temp.path().join("vacant.sock");
        let member = attested_zellij_member(temp.path(), vacant.clone(), "session-vacant");
        let host = zellij_readiness_host(&member, &census);
        serve_readiness_status(
            vacant,
            attested_zellij_status(
                census_status(
                    handoff,
                    "session-vacant",
                    Some(ready_census(&[], Some(&[]))),
                ),
                &member,
            ),
        );
        wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            &host,
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await
        .expect("a genuinely queried empty snapshot reads ready");
    }

    #[derive(Clone, Default)]
    struct BarrierReloader {
        attempts: Arc<Mutex<Vec<String>>>,
        fail_session: Arc<Mutex<Option<String>>>,
    }

    impl HostReloader for BarrierReloader {
        fn reload_bridge(&self, session: &str, _bridge_url: &str) -> Result<(), ActivateError> {
            self.attempts.lock().unwrap().push(session.to_owned());
            if self.fail_session.lock().unwrap().as_deref() == Some(session) {
                return Err(ActivateError::Reload {
                    session: session.to_owned(),
                    detail: "injected old reload failure".to_owned(),
                });
            }
            Ok(())
        }
        fn bridge_loaded(&self, _session: &str, _bridge_url: &str) -> Result<bool, ActivateError> {
            Ok(false)
        }
    }

    struct BarrierRollbackActor<'a> {
        cache_dir: &'a Path,
        reloader: BarrierReloader,
        hooks: ActivateHooks,
        targets: std::collections::VecDeque<TargetHandle>,
        retired_targets: usize,
        stop_failure_after: Option<usize>,
        fail_retired_journal_write: bool,
        resume_failure: Option<String>,
        resumes: Vec<String>,
    }

    impl RollbackActor for BarrierRollbackActor<'_> {
        fn cache_dir(&self) -> &Path {
            self.cache_dir
        }

        fn reloader(&self) -> &dyn HostReloader {
            &self.reloader
        }

        fn hooks(&self) -> Option<&ActivateHooks> {
            Some(&self.hooks)
        }

        async fn resolve_prepare(
            &mut self,
            _member: &TransactionMember,
            _journal: &ActivationJournal,
        ) -> Result<PrepareResolution, ActivateError> {
            Err(ActivateError::UnitFailed {
                reason: "barrier fixture has no unresolved Prepare intent".to_owned(),
            })
        }

        async fn resolve_target_spawn(
            &mut self,
            member: &TransactionMember,
            journal: &ActivationJournal,
        ) -> Result<TargetSpawnResolution, ActivateError> {
            if self.targets.is_empty() {
                Ok(TargetSpawnResolution::Absent)
            } else {
                self.target_retirement_authority(member, journal)
                    .await
                    .map(TargetSpawnResolution::NeedsRetirement)
            }
        }

        async fn target_retirement_authority(
            &mut self,
            _member: &TransactionMember,
            _journal: &ActivationJournal,
        ) -> Result<TargetRetirementAuthority, ActivateError> {
            let handle = self
                .targets
                .front()
                .expect("fixture retains one owned child per unretired target");
            Ok(TargetRetirementAuthority::OwnedProcess {
                process_id: TargetProcessId::new(handle.child.id())?,
            })
        }

        async fn retire_target(
            &mut self,
            _member: &TransactionMember,
            _journal: &ActivationJournal,
            authority: &TargetRetirementAuthority,
        ) -> Result<(), ActivateError> {
            if self.stop_failure_after == Some(self.retired_targets) {
                return Err(ActivateError::UnitFailed {
                    reason: "injected target stop barrier failure".to_owned(),
                });
            }
            let handle = self
                .targets
                .front_mut()
                .expect("fixture retains one owned child per unretired target");
            let TargetRetirementAuthority::OwnedProcess { process_id } = authority else {
                return Err(ActivateError::UnitFailed {
                    reason: "fixture target retirement has foreign process authority".to_owned(),
                });
            };
            if handle.child.id() != process_id.get() {
                return Err(ActivateError::UnitFailed {
                    reason: "fixture target retirement process authority changed".to_owned(),
                });
            }
            ProcessSpawner.stop_target(handle)?;
            if self.fail_retired_journal_write {
                self.fail_retired_journal_write = false;
                crate::fsutil::inject_tagged_durability_fault(
                    "activation",
                    crate::fsutil::DurabilityFault::BeforeRename,
                );
            }
            Ok(())
        }

        fn release_retired_target(
            &mut self,
            _member: &TransactionMember,
        ) -> Result<(), ActivateError> {
            self.targets
                .pop_front()
                .expect("receipt durability releases one exact owned target");
            self.retired_targets += 1;
            Ok(())
        }

        async fn resume_old(
            &mut self,
            member: &TransactionMember,
            journal: &ActivationJournal,
        ) -> Result<ResumeDisposition, ActivateError> {
            assert!(
                journal.members().iter().all(|member| matches!(
                    member.target,
                    TargetMemberProgress::Absent | TargetMemberProgress::Retired
                )),
                "old resume cannot cross an incomplete target-stop barrier"
            );
            if let Some(bridge) = journal.bridge() {
                assert!(
                    matches!(bridge.progress, BridgeProgress::Restored { .. }),
                    "old resume cannot cross an incomplete bridge/reload barrier"
                );
            }
            if self.resume_failure.as_deref() == Some(member.member().as_str()) {
                return Err(ActivateError::UnitFailed {
                    reason: "injected old resume failure".to_owned(),
                });
            }
            self.resumes.push(member.member().as_str().to_owned());
            Ok(ResumeDisposition::Completed)
        }
    }

    struct H21BridgeCase {
        _temp: tempfile::TempDir,
        cache: PathBuf,
        journal: ActivationJournal,
        journal_path: PathBuf,
        identity: BridgeIdentity,
        stable: PathBuf,
        old_bytes: Vec<u8>,
        target_bytes: Vec<u8>,
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the H21 fixture constructs one complete two-member bridge journal with immutable artifacts, receipt authority, and exact old registry rows"
    )]
    fn h21_bridge_case() -> H21BridgeCase {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        let integration_dir = temp.path().join("integration");
        for directory in [&cache, &integration_dir] {
            std::fs::create_dir(directory).unwrap();
            std::fs::set_permissions(
                directory,
                std::os::unix::fs::PermissionsExt::from_mode(0o700),
            )
            .unwrap();
        }
        let identity = BridgeIdentity::resolve(
            &integration_dir,
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        let old_bytes = b"h21-old-bridge".to_vec();
        let target_bytes = b"h21-target-bridge".to_vec();
        let old_digest = integration::receipt::Sha256Digest::from_bytes(&old_bytes);
        let target_digest = integration::receipt::Sha256Digest::from_bytes(&target_bytes);
        crate::fsutil::write_atomic(&stable, &target_bytes, "h21-stable").unwrap();

        let activation = ActivationId::from_bytes([0x81; 16]).unwrap();
        let mut members = Vec::new();
        let mut bridge_members = Vec::new();
        let mut old_registry = Vec::new();
        for (index, name) in ["session-a", "session-b"].into_iter().enumerate() {
            let endpoint = cache.join(format!("{name}.sock"));
            let member_id = ActivationMemberId::new(name.to_owned()).unwrap();
            let mut member = TransactionMember::new(
                activation,
                member_id,
                MemberEndpoint::new(endpoint.clone()).unwrap(),
                handoff(u8::try_from(0x82 + index).unwrap()),
                old_record(),
            )
            .unwrap();
            member.old = OldMemberProgress::Drained;
            member.target = if index == 0 {
                TargetMemberProgress::Gated
            } else {
                TargetMemberProgress::Committed
            };
            members.push(member);

            let bridge_member =
                super::super::registry::BridgeMemberId::new(name.to_owned()).unwrap();
            bridge_members.push(bridge_member.clone());
            let mut old = BrokerEntry::now("zellij", name, endpoint, std::process::id());
            old.bridge_identity = Some(identity.clone());
            old.bridge_member = Some(bridge_member);
            old.live_server = Some(name.to_owned());
            old_registry.push(old);
        }
        let receipt_preimage = integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: "0.1.0".to_owned(),
            installed_digest: old_digest.clone(),
            previous_digest: None,
            bridge_compat: old_record().zellij,
        };
        let receipt_target = integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: target_record().muxe_version,
            installed_digest: target_digest.clone(),
            previous_digest: Some(old_digest.clone()),
            bridge_compat: target_record().zellij,
        };
        let receipt_rollback = integration::receipt::BridgeRecord {
            previous_digest: Some(target_digest.clone()),
            ..receipt_preimage.clone()
        };
        integration::receipt::store(
            identity.directory(),
            &integration::receipt::Receipt {
                schema_version: integration::receipt::RECEIPT_SCHEMA_VERSION,
                bridge: receipt_target.clone(),
                configs: Vec::new(),
            },
        )
        .unwrap();
        let artifacts = BridgeArtifacts {
            old: BridgeArtifactId::new(activation, BridgeArtifactRole::Old),
            target: BridgeArtifactId::new(activation, BridgeArtifactRole::Target),
            old_digest: old_digest.clone(),
            target_digest: target_digest.clone(),
            receipt_preimage,
            receipt_target,
            receipt_rollback,
        };
        integration::bridge::ensure_artifact(&identity, artifacts.old, &old_bytes, &old_digest)
            .unwrap();
        integration::bridge::ensure_artifact(
            &identity,
            artifacts.target,
            &target_bytes,
            &target_digest,
        )
        .unwrap();
        let mut journal = ActivationJournal::new(
            activation,
            UnitKind::Zellij {
                bridge_unit: identity.unit(),
            },
            target_record(),
            members,
        )
        .unwrap();
        journal
            .bind_zellij_authority(
                identity.clone(),
                MemberCensus::from_members(bridge_members).unwrap(),
                artifacts,
            )
            .unwrap();
        journal.old_registry = old_registry;
        journal
            .bridge_mut()
            .expect("fixture has bridge authority")
            .progress = BridgeProgress::TargetReloaded;
        journal.enter_rollback("H21 barrier fixture".to_owned());
        let journal_path = journal::write_journal(&cache, &journal).unwrap();
        H21BridgeCase {
            _temp: temp,
            cache,
            journal,
            journal_path,
            identity,
            stable,
            old_bytes,
            target_bytes,
        }
    }

    fn h21_owned_targets(count: usize) -> std::collections::VecDeque<TargetHandle> {
        (0..count)
            .map(|_| {
                TargetHandle::new(
                    std::process::Command::new("/bin/sleep")
                        .arg("30")
                        .spawn()
                        .unwrap(),
                )
            })
            .collect()
    }

    fn h21_actor_with_target_count(
        cache: &Path,
        reloader: BarrierReloader,
        target_count: usize,
    ) -> BarrierRollbackActor<'_> {
        BarrierRollbackActor {
            cache_dir: cache,
            reloader,
            hooks: ActivateHooks::default(),
            targets: h21_owned_targets(target_count),
            retired_targets: 0,
            stop_failure_after: None,
            fail_retired_journal_write: false,
            resume_failure: None,
            resumes: Vec::new(),
        }
    }

    fn h21_actor(cache: &Path, reloader: BarrierReloader) -> BarrierRollbackActor<'_> {
        h21_actor_with_target_count(cache, reloader, 2)
    }

    #[tokio::test]
    async fn target_stop_failure_blocks_bridge_reload_and_old_resume_until_retry() {
        let mut case = h21_bridge_case();
        let reloader = BarrierReloader::default();
        let mut actor = h21_actor(&case.cache, reloader.clone());
        actor.stop_failure_after = Some(1);

        assert!(
            drive_rollback(&mut actor, &mut case.journal, &case.journal_path)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&case.stable).unwrap(), case.target_bytes);
        assert!(reloader.attempts.lock().unwrap().is_empty());
        assert!(actor.resumes.is_empty());
        let persisted = journal::read_journal(&case.journal_path).unwrap();
        assert_eq!(
            persisted
                .members()
                .iter()
                .map(|member| member.target)
                .collect::<Vec<_>>(),
            [
                TargetMemberProgress::Retired,
                TargetMemberProgress::RetireIntent,
            ]
        );
        assert_eq!(actor.retired_targets, 1);
        assert_eq!(actor.targets.len(), 1);
        assert!(
            actor
                .targets
                .front_mut()
                .unwrap()
                .child
                .try_wait()
                .unwrap()
                .is_none(),
            "ordinary stop failure retains supervision of the still-live child"
        );
        assert!(
            journal::has_target_retirement_receipt(
                &journal::activation_dir(&case.cache),
                &persisted,
                &persisted.members()[0],
            )
            .unwrap(),
            "completed stop is durable before the Retired journal outcome"
        );

        actor.stop_failure_after = None;
        assert_eq!(
            drive_rollback(&mut actor, &mut case.journal, &case.journal_path)
                .await
                .unwrap(),
            RollbackDriveOutcome::Complete
        );
        assert_eq!(std::fs::read(&case.stable).unwrap(), case.old_bytes);
        assert_eq!(
            std::fs::read(integration::bridge::previous_path(&case.stable)).unwrap(),
            case.target_bytes
        );
        assert_eq!(actor.resumes, ["session-a", "session-b"]);
        assert_eq!(
            Registry::open(&case.cache).unwrap().entries().unwrap(),
            case.journal.old_registry.clone(),
            "terminal rollback retains the exact old registry rows"
        );
        assert_eq!(
            integration::receipt::load(case.identity.directory())
                .unwrap()
                .unwrap()
                .bridge,
            case.journal.bridge().unwrap().artifacts.receipt_rollback
        );
        assert!(!case.journal_path.exists());
        let activation_directory = journal::activation_dir(&case.cache);
        for member in case.journal.members() {
            assert!(
                !journal::target_retirement_receipt_entry_exists(
                    &activation_directory,
                    member
                        .target_retirement
                        .as_ref()
                        .expect("retired target retains its receipt authority"),
                )
                .unwrap(),
                "terminal cleanup durably removes every retirement receipt"
            );
        }
    }

    #[tokio::test]
    async fn durable_retirement_receipt_replays_after_retired_journal_write_failure() {
        let (_temp, cache, mut journal, path, mut control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        journal::write_journal(&cache, &journal).unwrap();
        let endpoint = journal.members()[0].endpoint().as_path().to_path_buf();
        let reloader = BarrierReloader::default();
        let mut actor = h21_actor_with_target_count(&cache, reloader.clone(), 1);
        actor.fail_retired_journal_write = true;

        assert!(
            drive_rollback(&mut actor, &mut journal, &path)
                .await
                .is_err(),
            "the injected Retired journal write must fail after receipt durability"
        );
        assert!(
            actor.targets.is_empty(),
            "the reaped handle is released only after the receipt is durable"
        );
        assert!(!endpoint.exists(), "the target endpoint is absent");
        drop(actor);

        let persisted = journal::read_journal(&path).unwrap();
        assert_eq!(
            persisted.members()[0].target,
            TargetMemberProgress::RetireIntent
        );
        assert!(
            journal::has_target_retirement_receipt(
                &journal::activation_dir(&cache),
                &persisted,
                &persisted.members()[0],
            )
            .unwrap()
        );

        control.silent = true;
        let local_member = persisted.members()[0].member().clone();
        let local_status = control.session.status.lock().unwrap().clone();
        let mut recovered = persisted;
        let mut recovery_actor = RecoveryRollbackActor {
            cache_dir: &cache,
            control: &control,
            reloader: &reloader,
            local_member: Some(&local_member),
            local_status: Some(&local_status),
            local_can_resume: true,
            trace: None,
        };
        assert_eq!(
            drive_rollback(&mut recovery_actor, &mut recovered, &path)
                .await
                .unwrap(),
            RollbackDriveOutcome::ResumeRequired
        );
        assert_eq!(recovered.members()[0].target, TargetMemberProgress::Retired);
        assert_eq!(recovered.members()[0].old, OldMemberProgress::ResumeIntent);
    }

    #[tokio::test]
    async fn pre_stop_retirement_receipt_never_proves_retirement() {
        let (_temp, cache, mut journal, path, _control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        let reloader = BarrierReloader::default();
        let mut actor = h21_actor_with_target_count(&cache, reloader, 1);
        let member = journal.members()[0].clone();
        let authority = actor
            .target_retirement_authority(&member, &journal)
            .await
            .unwrap();
        let intent = TargetRetirementIntent::new(&journal, &member, authority).unwrap();
        {
            let member = &mut journal.members_mut()[0];
            member.target = TargetMemberProgress::RetireIntent;
            member.target_retirement = Some(intent);
        }
        journal::write_target_retirement_receipt(
            &journal::activation_dir(&cache),
            &journal,
            &journal.members()[0],
        )
        .unwrap();
        {
            let member = &mut journal.members_mut()[0];
            member.target = TargetMemberProgress::Gated;
            member.target_retirement = None;
        }
        journal::write_journal(&cache, &journal).unwrap();

        assert!(
            drive_rollback(&mut actor, &mut journal, &path)
                .await
                .is_err()
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().members()[0].target,
            TargetMemberProgress::Gated
        );
        assert_eq!(actor.targets.len(), 1);
        assert!(
            actor
                .targets
                .front_mut()
                .unwrap()
                .child
                .try_wait()
                .unwrap()
                .is_none(),
            "pre-stop receipt rejection retains the live child"
        );
    }

    #[tokio::test]
    async fn mismatched_and_symlinked_retirement_receipts_preserve_intent() {
        let (_temp, cache, mut journal, path, mut control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        let reloader = BarrierReloader::default();
        let mut actor = h21_actor_with_target_count(&cache, reloader.clone(), 1);
        let member = journal.members()[0].clone();
        let authority = actor
            .target_retirement_authority(&member, &journal)
            .await
            .unwrap();
        let intent = TargetRetirementIntent::new(&journal, &member, authority).unwrap();
        {
            let member = &mut journal.members_mut()[0];
            member.target = TargetMemberProgress::RetireIntent;
            member.target_retirement = Some(intent.clone());
        }
        journal::write_journal(&cache, &journal).unwrap();
        let directory = journal::activation_dir(&cache);
        journal::write_target_retirement_receipt(&directory, &journal, &journal.members()[0])
            .unwrap();
        drop(actor);

        let receipt_path = journal::target_retirement_receipt_path(&directory, &intent);
        let mut foreign: serde_json::Value =
            serde_json::from_slice(&fsutil::read_owner_file(&receipt_path).unwrap()).unwrap();
        foreign["activation_id"] =
            serde_json::Value::String(ActivationId::from_bytes([0x77; 16]).unwrap().to_hex());
        fsutil::write_atomic(
            &receipt_path,
            &serde_json::to_vec_pretty(&foreign).unwrap(),
            "foreign-retirement",
        )
        .unwrap();
        control.silent = true;
        let mut recovery_actor = RecoveryRollbackActor {
            cache_dir: &cache,
            control: &control,
            reloader: &reloader,
            local_member: None,
            local_status: None,
            local_can_resume: false,
            trace: None,
        };
        assert!(
            drive_rollback(&mut recovery_actor, &mut journal, &path)
                .await
                .is_err()
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().members()[0].target,
            TargetMemberProgress::RetireIntent
        );

        std::fs::remove_file(&receipt_path).unwrap();
        let foreign_target = directory.join("foreign-retirement");
        fsutil::write_atomic(&foreign_target, b"foreign", "foreign-retirement").unwrap();
        std::os::unix::fs::symlink(&foreign_target, &receipt_path).unwrap();
        assert!(
            drive_rollback(&mut recovery_actor, &mut journal, &path)
                .await
                .is_err()
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().members()[0].target,
            TargetMemberProgress::RetireIntent
        );
    }

    #[tokio::test]
    async fn final_reload_failure_blocks_all_old_resumes_and_retry_converges() {
        let mut case = h21_bridge_case();
        let reloader = BarrierReloader::default();
        *reloader.fail_session.lock().unwrap() = Some("session-b".to_owned());
        let mut actor = h21_actor(&case.cache, reloader.clone());

        assert!(
            drive_rollback(&mut actor, &mut case.journal, &case.journal_path)
                .await
                .is_err()
        );
        assert!(actor.resumes.is_empty());
        assert_eq!(std::fs::read(&case.stable).unwrap(), case.old_bytes);
        assert_eq!(
            std::fs::read(integration::bridge::previous_path(&case.stable)).unwrap(),
            case.target_bytes
        );
        assert_eq!(
            reloader.attempts.lock().unwrap().as_slice(),
            ["session-a", "session-b"]
        );
        assert!(matches!(
            journal::read_journal(&case.journal_path)
                .unwrap()
                .bridge()
                .unwrap()
                .progress,
            BridgeProgress::OldReloading { .. }
        ));

        *reloader.fail_session.lock().unwrap() = None;
        assert_eq!(
            drive_rollback(&mut actor, &mut case.journal, &case.journal_path)
                .await
                .unwrap(),
            RollbackDriveOutcome::Complete
        );
        assert_eq!(
            reloader.attempts.lock().unwrap().as_slice(),
            ["session-a", "session-b", "session-b"]
        );
        assert_eq!(actor.resumes, ["session-a", "session-b"]);
    }

    #[tokio::test]
    async fn bridge_and_receipt_failures_stop_before_reload_or_resume() {
        let mut bridge_case = h21_bridge_case();
        let bridge_reloader = BarrierReloader::default();
        let mut bridge_actor = h21_actor(&bridge_case.cache, bridge_reloader.clone());
        bridge_actor.hooks.fail_after = Some(ActivateStep::OldInstallAppliedBeforeOutcome);
        assert!(
            drive_rollback(
                &mut bridge_actor,
                &mut bridge_case.journal,
                &bridge_case.journal_path,
            )
            .await
            .is_err()
        );
        assert_eq!(
            std::fs::read(&bridge_case.stable).unwrap(),
            bridge_case.old_bytes
        );
        assert!(bridge_reloader.attempts.lock().unwrap().is_empty());
        assert!(bridge_actor.resumes.is_empty());
        assert!(matches!(
            journal::read_journal(&bridge_case.journal_path)
                .unwrap()
                .bridge()
                .unwrap()
                .progress,
            BridgeProgress::OldInstallIntent
        ));
        bridge_actor.hooks.fail_after = None;
        assert_eq!(
            drive_rollback(
                &mut bridge_actor,
                &mut bridge_case.journal,
                &bridge_case.journal_path,
            )
            .await
            .unwrap(),
            RollbackDriveOutcome::Complete
        );

        let mut receipt_case = h21_bridge_case();
        let receipt_reloader = BarrierReloader::default();
        let mut foreign = integration::receipt::load(receipt_case.identity.directory())
            .unwrap()
            .unwrap();
        foreign.bridge.installed_version = "foreign".to_owned();
        integration::receipt::store(receipt_case.identity.directory(), &foreign).unwrap();
        let mut receipt_actor = h21_actor(&receipt_case.cache, receipt_reloader.clone());
        assert!(
            drive_rollback(
                &mut receipt_actor,
                &mut receipt_case.journal,
                &receipt_case.journal_path,
            )
            .await
            .is_err()
        );
        assert!(receipt_reloader.attempts.lock().unwrap().is_empty());
        assert!(receipt_actor.resumes.is_empty());
        assert!(matches!(
            journal::read_journal(&receipt_case.journal_path)
                .unwrap()
                .bridge()
                .unwrap()
                .progress,
            BridgeProgress::OldReceiptIntent
        ));
        let exact_target = receipt_case
            .journal
            .bridge()
            .unwrap()
            .artifacts
            .receipt_target
            .clone();
        foreign.bridge = exact_target;
        integration::receipt::store(receipt_case.identity.directory(), &foreign).unwrap();
        assert_eq!(
            drive_rollback(
                &mut receipt_actor,
                &mut receipt_case.journal,
                &receipt_case.journal_path,
            )
            .await
            .unwrap(),
            RollbackDriveOutcome::Complete
        );
    }

    #[tokio::test]
    async fn persistent_old_resume_failure_never_reports_rolled_back() {
        let mut case = h21_bridge_case();
        let reloader = BarrierReloader::default();
        let mut actor = h21_actor(&case.cache, reloader);
        actor.resume_failure = Some("session-a".to_owned());
        let error = drive_rollback(&mut actor, &mut case.journal, &case.journal_path)
            .await
            .unwrap_err();
        assert!(actor.resumes.is_empty());
        let persisted = journal::read_journal(&case.journal_path).unwrap();
        assert_eq!(persisted.directive(), TransactionDirective::RollBack);
        assert_eq!(persisted.members()[0].old, OldMemberProgress::ResumeIntent);
        assert!(matches!(
            rollback_outcome(
                "zellij:test".to_owned(),
                "activation failed".to_owned(),
                &[error.to_string()],
            ),
            UnitOutcome::Failed { .. }
        ));

        actor.resume_failure = None;
        assert_eq!(
            drive_rollback(&mut actor, &mut case.journal, &case.journal_path)
                .await
                .unwrap(),
            RollbackDriveOutcome::Complete
        );
        assert_eq!(actor.resumes, ["session-a", "session-b"]);
    }

    #[tokio::test]
    async fn broker_local_recovery_cannot_jump_target_retirement_barrier() {
        let (_temp, cache, mut journal, path, control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        journal::write_journal(&cache, &journal).unwrap();
        let member = journal.members()[0].member().clone();
        let local_status = control.session.status.lock().unwrap().clone();
        let result = prepare_broker_rollback(
            &cache,
            &control,
            &FixtureReloader::default(),
            &mut journal,
            &path,
            &member,
            &local_status,
        )
        .await;
        assert!(result.is_err());
        let persisted = journal::read_journal(&path).unwrap();
        assert_eq!(persisted.members()[0].old, OldMemberProgress::Drained);
        assert_eq!(
            persisted.members()[0].target,
            TargetMemberProgress::Gated,
            "broker-local recovery cannot invent target process authority"
        );
        assert!(persisted.members()[0].target_retirement.is_none());
    }

    #[tokio::test]
    async fn uncertain_spawn_intent_is_never_normalized_from_endpoint_absence() {
        let (_temp, cache, mut journal, path, mut control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::SpawnIntent;
        journal::write_journal(&cache, &journal).unwrap();
        control.silent = true;
        let reloader = FixtureReloader::default();
        let mut actor = RecoveryRollbackActor {
            cache_dir: &cache,
            control: &control,
            reloader: &reloader,
            local_member: None,
            local_status: None,
            local_can_resume: false,
            trace: None,
        };
        assert!(
            drive_rollback(&mut actor, &mut journal, &path)
                .await
                .is_err()
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().members()[0].target,
            TargetMemberProgress::SpawnIntent
        );
    }

    #[derive(Clone)]
    struct TraceSession {
        status: Arc<Mutex<ActivationStatus>>,
        journal_path: PathBuf,
        resumes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ControlSession for TraceSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.lock().unwrap().clone())
        }

        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
            _handoff: &HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }

        async fn commit(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }

        async fn abort(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            let journal = journal::read_journal(&self.journal_path).unwrap();
            let member = &journal.members()[0];
            assert_eq!(*handoff, member.handoff_id());
            assert_eq!(member.old, OldMemberProgress::ResumeIntent);
            self.resumes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut status = self.status.lock().unwrap();
            status.lifecycle = LifecycleState::Running;
            status.target = None;
            status.handoff_id = None;
            Ok(status.clone())
        }

        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
    }

    #[derive(Clone)]
    struct TraceControl {
        session: TraceSession,
        silent: bool,
    }

    impl ControlPort for TraceControl {
        type Session = TraceSession;

        async fn connect(&self, _socket: &Path) -> Result<Self::Session, ControlError> {
            if self.silent {
                Err(ControlError::Closed)
            } else {
                Ok(self.session.clone())
            }
        }
    }

    fn rollback_trace_case() -> (
        tempfile::TempDir,
        PathBuf,
        ActivationJournal,
        PathBuf,
        TraceControl,
    ) {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::os::unix::fs::PermissionsExt::from_mode(0o700))
            .unwrap();
        let activation = ActivationId::from_bytes([0x71; 16]).unwrap();
        let member_id = ActivationMemberId::new("server".to_owned()).unwrap();
        let endpoint = MemberEndpoint::new(cache.join("server.sock")).unwrap();
        let handoff = handoff(0x72);
        let member =
            TransactionMember::new(activation, member_id, endpoint, handoff, old_record()).unwrap();
        let mut journal = ActivationJournal::new(
            activation,
            UnitKind::Herdr {
                host_hash: unit_hash("server"),
            },
            target_record(),
            vec![member],
        )
        .unwrap();
        journal.members_mut()[0].old = OldMemberProgress::PrepareIntent;
        journal.enter_rollback("trace rollback".to_owned());
        let journal_path = journal::write_journal(&cache, &journal).unwrap();
        let mut draining = status_of(
            &old_record(),
            Some(handoff),
            "server",
            LifecycleState::Draining,
        );
        draining.target = Some(target_record());
        let session = TraceSession {
            status: Arc::new(Mutex::new(draining)),
            journal_path: journal_path.clone(),
            resumes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        (
            temp,
            cache,
            journal,
            journal_path,
            TraceControl {
                session,
                silent: false,
            },
        )
    }

    fn assert_child_reaped(pid: u32) {
        let pid = nix::unistd::Pid::from_raw(i32::try_from(pid).unwrap());
        assert!(matches!(
            nix::sys::wait::waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)),
            Err(nix::errno::Errno::ECHILD)
        ));
        assert!(matches!(
            nix::sys::signal::kill(pid, None),
            Err(nix::errno::Errno::ESRCH)
        ));
    }

    /// Reaps only the exact test-owned children after their disarmed handles
    /// are deliberately dropped; a panic still cannot leave a live sleeper.
    struct CertifiedChildCleanup(Vec<nix::unistd::Pid>);

    impl CertifiedChildCleanup {
        fn for_targets(targets: &[OwnedTarget]) -> Self {
            Self(
                targets
                    .iter()
                    .map(|target| {
                        nix::unistd::Pid::from_raw(i32::try_from(target.handle.child.id()).unwrap())
                    })
                    .collect(),
            )
        }

        fn assert_live(&self) {
            for pid in &self.0 {
                assert!(
                    nix::sys::signal::kill(*pid, None).is_ok(),
                    "{pid} was killed"
                );
            }
        }
    }

    impl Drop for CertifiedChildCleanup {
        fn drop(&mut self) {
            for pid in &self.0 {
                let _ = nix::sys::signal::kill(*pid, Some(nix::sys::signal::Signal::SIGKILL));
                let _ = nix::sys::wait::waitpid(*pid, None);
            }
        }
    }

    fn trace_prepared_member(
        control: &TraceControl,
        journal: &ActivationJournal,
    ) -> PreparedMember<TraceControl> {
        let member = &journal.members()[0];
        PreparedMember {
            entry: RegisteredBroker::herdr(census_member(
                member.endpoint().as_path().to_path_buf(),
                "herdr",
                member.member().as_str(),
            ))
            .unwrap(),
            handoff: member.handoff_id(),
            old_session: control.session.clone(),
        }
    }

    fn fixture_herdr_ready_proof(journal: &ActivationJournal) -> journal::ReadyProof {
        let member = &journal.members()[0];
        let mut entry = BrokerEntry::now(
            "herdr",
            member.member().as_str(),
            member.endpoint().as_path().to_path_buf(),
            77,
        );
        entry.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::from_bytes([9; 16]).unwrap());
        entry.live_server = Some("id".to_owned());
        journal::ReadyProof::new(
            journal,
            None,
            vec![
                journal::ReadyMemberProof::new(
                    member,
                    &entry,
                    &muxe_protocol::wire::ServerId::new("id"),
                )
                .unwrap(),
            ],
        )
        .unwrap()
    }

    #[test]
    fn ready_write_fault_follows_exact_durable_disk_phase() {
        for fault in [
            crate::fsutil::DurabilityFault::BeforeRename,
            crate::fsutil::DurabilityFault::AfterRenameBeforeDirectorySync,
        ] {
            let (_temp, cache, mut journal, path, _control) = rollback_trace_case();
            journal.members_mut()[0].old = OldMemberProgress::Drained;
            journal.members_mut()[0].target = TargetMemberProgress::Ready;
            journal.enter_activating();
            journal::write_journal(&cache, &journal).unwrap();
            if fault == crate::fsutil::DurabilityFault::BeforeRename {
                crate::fsutil::inject_tagged_durability_fault("activation", fault);
            } else {
                crate::fsutil::inject_durability_fault(fault);
            }
            let proof = fixture_herdr_ready_proof(&journal);
            let result = persist_ready_decision(&cache, &path, &mut journal, Some(proof)).unwrap();
            let persisted = journal::read_journal(&path).unwrap();
            match fault {
                crate::fsutil::DurabilityFault::BeforeRename => {
                    assert!(matches!(result, ReadyWriteOutcome::NotWritten(_)));
                    assert_eq!(journal.directive(), TransactionDirective::Activate);
                    assert_eq!(persisted.directive(), TransactionDirective::Activate);
                }
                crate::fsutil::DurabilityFault::AfterRenameBeforeDirectorySync => {
                    assert_eq!(result, ReadyWriteOutcome::Durable);
                    assert_eq!(journal.directive(), TransactionDirective::Commit);
                    assert_eq!(persisted.directive(), TransactionDirective::Commit);
                }
                crate::fsutil::DurabilityFault::AfterUnlinkBeforeDirectorySync => unreachable!(),
            }
        }
    }

    #[derive(Clone)]
    struct FateControl {
        status: ActivationStatus,
        commits: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl ControlSession for FateControl {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.clone())
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
            _handoff: &HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn commit(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(ControlError::Closed)
        }
        async fn abort(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
    }

    impl ControlPort for FateControl {
        type Session = Self;
        async fn connect(&self, _socket: &Path) -> Result<Self::Session, ControlError> {
            Ok(self.clone())
        }
    }

    #[tokio::test]
    async fn recovery_fate_comes_only_from_durable_ready_phase() {
        let (_temp, cache, mut journal, path, _control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Ready;
        journal.enter_activating();
        journal::write_journal(&cache, &journal).unwrap();
        let commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let target_looking = FateControl {
            status: status_of(
                &target_record(),
                Some(journal.members()[0].handoff_id()),
                journal.members()[0].member().as_str(),
                LifecycleState::Running,
            ),
            commits: Arc::clone(&commits),
        };
        let result = recover(&cache, &target_looking, &BarrierReloader::default(), None)
            .await
            .unwrap();
        assert!(matches!(
            result.as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::RollBack
        );
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(path.exists());

        let (_temp, cache, mut journal, path, mut control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Ready;
        let proof = fixture_herdr_ready_proof(&journal);
        journal.enter_ready(Some(proof));
        journal::write_journal(&cache, &journal).unwrap();
        control.silent = true;
        let result = recover(&cache, &control, &BarrierReloader::default(), None)
            .await
            .unwrap();
        assert!(matches!(
            result.as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::Commit
        );
        assert!(path.exists());
    }

    struct ReadinessLossHook {
        second: PathBuf,
        first: PathBuf,
        stale: ActivationStatus,
        gate: muxe_adapter_zellij::ReadinessGate,
        started: tokio::sync::oneshot::Sender<()>,
        completed: tokio::sync::oneshot::Sender<()>,
    }
    struct AsOfExpiryHook {
        first: PathBuf,
        second: PathBuf,
        first_heartbeat: AsOfTick,
        lease_millis: u64,
        second_delay: Duration,
        now: Arc<std::sync::atomic::AtomicU64>,
    }

    #[derive(Clone, Default)]
    struct ReadyRoundControl {
        statuses: Arc<
            Mutex<
                std::collections::BTreeMap<PathBuf, std::collections::VecDeque<ActivationStatus>>,
            >,
        >,
        commits: Arc<std::sync::atomic::AtomicUsize>,
        commit_ok: Arc<std::sync::atomic::AtomicBool>,
        loss: Arc<Mutex<Option<ReadinessLossHook>>>,
        as_of_expiry: Arc<Mutex<Option<AsOfExpiryHook>>>,
        hung_status_at: Arc<Mutex<Option<PathBuf>>>,
    }

    impl ReadyRoundControl {
        fn set(&self, socket: &Path, statuses: Vec<ActivationStatus>) {
            self.statuses
                .lock()
                .unwrap()
                .insert(socket.to_path_buf(), statuses.into());
        }
    }

    struct ReadyRoundSession {
        socket: PathBuf,
        control: ReadyRoundControl,
    }

    impl ControlPort for ReadyRoundControl {
        type Session = ReadyRoundSession;
        async fn connect(&self, socket: &Path) -> Result<Self::Session, ControlError> {
            if !self.statuses.lock().unwrap().contains_key(socket) {
                return Err(ControlError::Closed);
            }
            Ok(ReadyRoundSession {
                socket: socket.to_path_buf(),
                control: self.clone(),
            })
        }
    }

    impl ReadyRoundSession {
        fn scripted_status(&mut self) -> Result<ActivationStatus, ControlError> {
            let status = {
                let mut statuses = self.control.statuses.lock().unwrap();
                let queue = statuses.get_mut(&self.socket).ok_or(ControlError::Closed)?;
                if queue.len() > 1 {
                    queue.pop_front().expect("scripted status exists")
                } else {
                    queue.front().cloned().ok_or(ControlError::Closed)?
                }
            };
            let hook = self
                .control
                .loss
                .lock()
                .unwrap()
                .take_if(|hook| hook.second == self.socket);
            if let Some(hook) = hook {
                let control = self.control.clone();
                tokio::spawn(async move {
                    let _ = hook.started.send(());
                    let _guard = hook
                        .gate
                        .exclusive(Duration::from_secs(2))
                        .await
                        .expect("loss publication acquires gate after Ready");
                    control.set(&hook.first, vec![hook.stale]);
                    let _ = hook.completed.send(());
                });
            }
            Ok(status)
        }
    }

    impl ControlSession for ReadyRoundSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            let mut status = self.scripted_status()?;
            let timing = self.control.as_of_expiry.lock().unwrap();
            if let Some(timing) = timing.as_ref()
                && self.socket == timing.first
                && AsOfTick::from_millis(timing.now.load(std::sync::atomic::Ordering::SeqCst))
                    .unwrap()
                    .age_since(timing.first_heartbeat)
                    .is_none_or(|age| age > timing.lease_millis)
            {
                status.ready = None;
            }
            Ok(status)
        }
        async fn status_at(
            &mut self,
            handoff: &HandoffId,
            epoch: UnitReadinessEpochId,
            as_of: AsOfTick,
        ) -> Result<ActivationStatus, ControlError> {
            if self.control.hung_status_at.lock().unwrap().as_deref() == Some(&self.socket) {
                std::future::pending::<()>().await;
            }
            let delay = {
                let timing = self.control.as_of_expiry.lock().unwrap();
                timing.as_ref().and_then(|timing| {
                    (self.socket == timing.second)
                        .then(|| (timing.second_delay, Arc::clone(&timing.now)))
                })
            };
            if let Some((delay, now)) = delay {
                tokio::time::sleep(delay).await;
                now.fetch_add(
                    u64::try_from(delay.as_millis()).unwrap(),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
            let mut status = self.scripted_status()?;
            if status.handoff_id != Some(*handoff) {
                return Err(ControlError::Rejected {
                    diagnostic: "scripted as-of handoff differs".to_owned(),
                });
            }
            let timing = self.control.as_of_expiry.lock().unwrap();
            if let Some(timing) = timing.as_ref()
                && self.socket == timing.first
                && as_of
                    .age_since(timing.first_heartbeat)
                    .is_none_or(|age| age > timing.lease_millis)
            {
                status.ready = None;
            }
            drop(timing);
            let ready = status
                .ready
                .as_mut()
                .ok_or_else(|| ControlError::Rejected {
                    diagnostic: "scripted as-of readiness is absent".to_owned(),
                })?;
            ready.proof_epoch = Some(epoch);
            Ok(status)
        }
        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
            _handoff: &HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn commit(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            self.control
                .commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self
                .control
                .commit_ok
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                self.status().await
            } else {
                Err(ControlError::Closed)
            }
        }
        async fn abort(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Err(ControlError::Closed)
        }
    }

    struct ReadyProofCase {
        _temp: tempfile::TempDir,
        cache: PathBuf,
        config: PathBuf,
        identity: BridgeIdentity,
        control: ReadyRoundControl,
        journal: ActivationJournal,
        unit: PlannedUnit,
        prepared: Vec<PreparedMember<ReadyRoundControl>>,
        targets: Vec<OwnedTarget>,
        reloader: FixtureReloader,
        preflight: FixturePreflight,
        spawner: ProcessSpawner,
    }

    impl ReadyProofCase {
        #[expect(
            clippy::too_many_lines,
            reason = "the two-member fixture constructs matching journal, artifact, receipt, registry, and live target authorities"
        )]
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            for directory in [
                temp.path().to_path_buf(),
                temp.path().join("cache"),
                temp.path().join("config"),
                temp.path().join("config/integrations/zellij"),
            ] {
                std::fs::create_dir_all(&directory).unwrap();
                std::fs::set_permissions(
                    &directory,
                    std::os::unix::fs::PermissionsExt::from_mode(0o700),
                )
                .unwrap();
            }
            let cache = temp.path().join("cache");
            let config = temp.path().join("config");
            let identity = integration::bridge_identity(&config).unwrap();
            let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
            let old_bytes = b"old-bridge";
            let target_bytes = b"target-bridge";
            let old_digest = integration::receipt::Sha256Digest::from_bytes(old_bytes);
            let target_digest = integration::receipt::Sha256Digest::from_bytes(target_bytes);
            fsutil::write_atomic(&stable, target_bytes, "ready-fixture").unwrap();
            let activation = ActivationId::from_bytes([0x88; 16]).unwrap();
            let artifacts = BridgeArtifacts {
                old: BridgeArtifactId::new(activation, BridgeArtifactRole::Old),
                target: BridgeArtifactId::new(activation, BridgeArtifactRole::Target),
                old_digest: old_digest.clone(),
                target_digest: target_digest.clone(),
                receipt_preimage: integration::receipt::BridgeRecord {
                    bridge_identity: identity.clone(),
                    installed_version: old_record().muxe_version,
                    installed_digest: old_digest.clone(),
                    previous_digest: None,
                    bridge_compat: old_record().zellij,
                },
                receipt_target: integration::receipt::BridgeRecord {
                    bridge_identity: identity.clone(),
                    installed_version: target_record().muxe_version,
                    installed_digest: target_digest.clone(),
                    previous_digest: Some(old_digest.clone()),
                    bridge_compat: target_record().zellij,
                },
                receipt_rollback: integration::receipt::BridgeRecord {
                    bridge_identity: identity.clone(),
                    installed_version: old_record().muxe_version,
                    installed_digest: old_digest.clone(),
                    previous_digest: Some(target_digest.clone()),
                    bridge_compat: old_record().zellij,
                },
            };
            integration::bridge::ensure_artifact(&identity, artifacts.old, old_bytes, &old_digest)
                .unwrap();
            integration::bridge::ensure_artifact(
                &identity,
                artifacts.target,
                target_bytes,
                &target_digest,
            )
            .unwrap();
            integration::receipt::store(
                identity.directory(),
                &integration::receipt::Receipt {
                    schema_version: integration::receipt::RECEIPT_SCHEMA_VERSION,
                    bridge: artifacts.receipt_preimage.clone(),
                    configs: Vec::new(),
                },
            )
            .unwrap();
            let mut entries = Vec::new();
            let mut members = Vec::new();
            for (index, discovery) in ["session-a", "session-b"].into_iter().enumerate() {
                let socket = cache.join(format!("{discovery}.sock"));
                let mut entry = census_member(socket.clone(), "zellij", discovery);
                entry.bridge_identity = Some(identity.clone());
                entry.bridge_member = Some(
                    super::super::registry::BridgeMemberId::new(discovery.to_owned()).unwrap(),
                );
                entries.push(entry);
                members.push(
                    TransactionMember::new(
                        activation,
                        ActivationMemberId::new(discovery.to_owned()).unwrap(),
                        MemberEndpoint::new(socket).unwrap(),
                        handoff(u8::try_from(index + 1).unwrap()),
                        old_record(),
                    )
                    .unwrap(),
                );
            }
            let census = MemberCensus::from_entries(&identity, &entries).unwrap();
            let mut journal = ActivationJournal::new(
                activation,
                UnitKind::Zellij {
                    bridge_unit: identity.unit(),
                },
                target_record(),
                members,
            )
            .unwrap();
            journal.old_registry = entries.clone();
            journal
                .bind_zellij_authority(identity.clone(), census.clone(), artifacts)
                .unwrap();
            journal.enter_activating();
            for member in journal.members_mut() {
                member.old = OldMemberProgress::Drained;
                member.target = TargetMemberProgress::Ready;
            }
            journal.bridge_mut().unwrap().progress = BridgeProgress::TargetReloaded;
            journal::write_journal(&cache, &journal).unwrap();
            let unit = PlannedUnit::Zellij {
                bridge_identity: identity.clone(),
                entries: entries
                    .iter()
                    .cloned()
                    .map(|entry| RegisteredBroker::zellij(entry, &identity).unwrap())
                    .collect(),
                census,
            };
            let control = ReadyRoundControl::default();
            let registry = Registry::open(&cache).unwrap();
            let mut prepared = Vec::new();
            let mut targets = Vec::new();
            for (entry, member) in entries.iter().zip(journal.members()) {
                let handle = h21_owned_targets(1).pop_front().unwrap();
                let mut target_row = entry.clone();
                target_row.server_pid = handle.child.id();
                target_row.handoff_id = Some(member.handoff_id());
                target_row.live_server = Some("id".to_owned());
                target_row.registration_id =
                    Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
                registry.register(target_row).unwrap();
                let mut status = status_of(
                    &journal.target_record,
                    Some(member.handoff_id()),
                    entry.discovery_key.as_str(),
                    LifecycleState::Running,
                );
                status.live_server.host = HostKind::Zellij;
                status.bridge_unit = Some(identity.unit());
                status.ready = Some(ready_census(&["client"], Some(&["client"])));
                control.set(&entry.socket, vec![status]);
                prepared.push(PreparedMember {
                    entry: RegisteredBroker::zellij(entry.clone(), &identity).unwrap(),
                    handoff: member.handoff_id(),
                    old_session: ReadyRoundSession {
                        socket: entry.socket.clone(),
                        control: control.clone(),
                    },
                });
                targets.push(OwnedTarget {
                    member: member.id.clone(),
                    handle,
                });
            }
            Self {
                _temp: temp,
                cache,
                config,
                identity,
                control,
                journal,
                unit,
                prepared,
                targets,
                reloader: FixtureReloader::default(),
                preflight: FixturePreflight::default(),
                spawner: ProcessSpawner,
            }
        }

        fn status(&self, index: usize) -> ActivationStatus {
            let member = &self.journal.members()[index];
            let mut status = status_of(
                &self.journal.target_record,
                Some(member.handoff_id()),
                member.member().as_str(),
                LifecycleState::Running,
            );
            status.live_server.host = HostKind::Zellij;
            status.bridge_unit = Some(self.identity.unit());
            status.ready = Some(ready_census(&["client"], Some(&["client"])));
            status
        }

        fn set(&self, index: usize, statuses: Vec<ActivationStatus>) {
            self.control
                .set(self.prepared[index].entry.socket(), statuses);
        }

        fn proof(&self) -> journal::ReadyProof {
            self.proof_at(AsOfTick::from_millis(1).unwrap())
        }

        fn proof_at(&self, as_of: AsOfTick) -> journal::ReadyProof {
            let rows = Registry::open(&self.cache).unwrap().entries().unwrap();
            let incarnations = self
                .journal
                .members()
                .iter()
                .map(|member| {
                    let row = rows
                        .iter()
                        .find(|row| row.socket == member.endpoint().as_path())
                        .unwrap();
                    journal::ReadyMemberProof::new(
                        member,
                        row,
                        &muxe_protocol::wire::ServerId::new(
                            row.live_server.as_ref().unwrap().clone(),
                        ),
                    )
                    .unwrap()
                })
                .collect();
            journal::ReadyProof::new(
                &self.journal,
                Some((UnitReadinessEpochId::from_bytes([0x55; 16]).unwrap(), as_of)),
                incarnations,
            )
            .unwrap()
        }

        async fn prove(&mut self) -> Result<(), ActivateError> {
            self.prove_at(AsOfTick::from_millis(1).unwrap(), Duration::from_secs(1))
                .await
        }

        async fn prove_at(
            &mut self,
            as_of: AsOfTick,
            readiness_deadline: Duration,
        ) -> Result<(), ActivateError> {
            let inputs = ActivateInputs {
                config_dir: &self.config,
                cache_dir: &self.cache,
                target: self.journal.target_record.clone(),
                staged_bridge: None,
                scope: HostScope::Zellij,
                current: None,
                control: &self.control,
                spawner: &self.spawner,
                reloader: &self.reloader,
                preflight: &self.preflight,
                spawn_policy: &TRUE_SPAWN,
                readiness_deadline,
                poll_interval: Duration::from_millis(1),
                hooks: ActivateHooks::default(),
                logger: None,
            };
            let PlannedUnit::Zellij {
                bridge_identity,
                entries,
                census,
            } = &self.unit
            else {
                panic!("Ready proof fixture is a fixed Zellij unit");
            };
            let host = ZellijActivation {
                identity: bridge_identity,
                entries,
                census,
            };
            let authorities = self
                .prepared
                .iter()
                .map(|member| PreparedAuthority {
                    entry: member.entry.clone(),
                    handoff: member.handoff,
                })
                .collect::<Vec<_>>();
            prove_ready_unit(
                &inputs,
                &host,
                &self.journal,
                ReadyUnitMembers {
                    prepared: &self.prepared,
                    authorities: &authorities,
                    targets: &mut self.targets,
                },
                ReadyWindow {
                    proof: Some((UnitReadinessEpochId::from_bytes([0x55; 16]).unwrap(), as_of)),
                    deadline: Instant::now() + readiness_deadline,
                },
            )
            .await
            .map(|_| ())
        }

        async fn rejects_status(&mut self, status: ActivationStatus) {
            self.set(0, vec![status]);
            assert!(self.prove().await.is_err());
            assert_eq!(self.journal.directive(), TransactionDirective::Activate);
            assert_eq!(
                self.control
                    .commits
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
            self.set(0, vec![self.status(0)]);
        }

        async fn rollback_after_failed_proof(&mut self) -> Vec<String> {
            let inputs = ActivateInputs {
                config_dir: &self.config,
                cache_dir: &self.cache,
                target: self.journal.target_record.clone(),
                staged_bridge: None,
                scope: HostScope::Zellij,
                current: None,
                control: &self.control,
                spawner: &self.spawner,
                reloader: &self.reloader,
                preflight: &self.preflight,
                spawn_policy: &TRUE_SPAWN,
                readiness_deadline: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                hooks: ActivateHooks::default(),
                logger: None,
            };
            let path = journal::activation_dir(&self.cache).join(self.journal.unit.journal_name());
            rollback_transaction(
                &inputs,
                &self.unit,
                &mut self.journal,
                &path,
                std::mem::take(&mut self.prepared),
                std::mem::take(&mut self.targets),
                "final same-round readiness failed".to_owned(),
            )
            .await
        }
        /// Starts a genuine incomplete Commit with durable old retirement
        /// receipts, leaving target acknowledgements and bridge publication
        /// for the recovery path under test.
        async fn start_committing_with_retired_old(&mut self) -> PathBuf {
            self.prove().await.unwrap();
            self.journal.enter_ready(Some(self.proof()));
            self.journal.enter_committing();
            for entry in &mut self.journal.old_registry {
                entry.registration_id =
                    Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
                entry.live_server = Some(format!("old-{}", entry.discovery_key));
            }
            for index in 0..self.journal.members().len() {
                self.journal.members_mut()[index].old = OldMemberProgress::CommitIntent;
                let server_id = muxe_protocol::wire::ServerId::new(
                    self.journal.old_registry[index]
                        .live_server
                        .as_deref()
                        .unwrap(),
                );
                journal::write_old_retirement_receipt(
                    &self.cache,
                    &self.journal,
                    &self.journal.members()[index],
                    &server_id,
                )
                .unwrap();
                self.journal.members_mut()[index].old = OldMemberProgress::Committed;
            }
            journal::write_journal(&self.cache, &self.journal).unwrap()
        }
    }

    #[tokio::test]
    async fn two_member_ready_proof_rechecks_stale_first_member_after_second_readiness() {
        let mut case = ReadyProofCase::new();
        case.prove()
            .await
            .expect("complete final round authorizes Ready");
        let mut stale_a = case.status(0);
        stale_a.ready = None;
        case.set(0, vec![case.status(0), stale_a]);
        let mut pending_b = case.status(1);
        pending_b.ready = None;
        case.set(1, vec![pending_b, case.status(1)]);
        let census = MemberCensus::default();
        for member in &case.prepared {
            wait_ready(
                &case.control,
                &member.entry,
                &member.handoff,
                &case.journal.target_record,
                &zellij_readiness_host(&member.entry, &census),
                Instant::now() + Duration::from_secs(1),
                Duration::from_millis(1),
            )
            .await
            .expect("each sequential readiness poll succeeds");
        }
        let gate =
            muxe_adapter_zellij::ReadinessGate::new(case.cache.clone(), case.identity.unit());
        let proof = gate.shared(Duration::from_secs(1)).await.unwrap();
        assert!(
            case.prove().await.is_err(),
            "A's final same-round status is stale"
        );
        drop(proof);
        let publication = gate.exclusive(Duration::from_secs(1)).await.unwrap();
        drop(publication);
        let _diagnostics = case.rollback_after_failed_proof().await;
        assert_eq!(
            journal::read_journal(
                &journal::activation_dir(&case.cache).join(case.journal.unit.journal_name())
            )
            .unwrap()
            .directive(),
            TransactionDirective::RollBack
        );
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn two_member_as_of_proof_survives_expiry_after_a_status_but_rejects_expiry_at_tick() {
        let tick = AsOfTick::from_millis(100_000).unwrap();
        let mut case = ReadyProofCase::new();
        let now = Arc::new(std::sync::atomic::AtomicU64::new(100_000));
        *case.control.as_of_expiry.lock().unwrap() = Some(AsOfExpiryHook {
            first: case.prepared[0].entry.socket().to_path_buf(),
            second: case.prepared[1].entry.socket().to_path_buf(),
            lease_millis: 15_000,
            first_heartbeat: AsOfTick::from_millis(85_001).unwrap(),
            second_delay: Duration::from_millis(2),
            now: Arc::clone(&now),
        });
        let gate =
            muxe_adapter_zellij::ReadinessGate::new(case.cache.clone(), case.identity.unit());
        let read = gate.shared(Duration::from_secs(1)).await.unwrap();
        case.prove_at(tick, Duration::from_secs(1))
            .await
            .expect("A remains covered at common tick even though B completes after A expires");
        assert_eq!(now.load(std::sync::atomic::Ordering::SeqCst), 100_002);
        assert!(now.load(std::sync::atomic::Ordering::SeqCst) - 85_001 > 15_000);
        let mut a = case
            .control
            .connect(case.prepared[0].entry.socket())
            .await
            .unwrap();
        assert!(
            a.status().await.unwrap().ready.is_none(),
            "ordinary status at B's later clock must reject A's expired lease"
        );
        drop(read);
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        let sealed = case.proof_at(tick);
        assert_eq!(
            persist_ready_decision(&case.cache, &path, &mut case.journal, Some(sealed.clone()))
                .unwrap(),
            ReadyWriteOutcome::Durable
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().ready_proof(),
            Some(&sealed)
        );

        let mut expired = ReadyProofCase::new();
        *expired.control.as_of_expiry.lock().unwrap() = Some(AsOfExpiryHook {
            first: expired.prepared[0].entry.socket().to_path_buf(),
            second: expired.prepared[1].entry.socket().to_path_buf(),
            lease_millis: 15_000,
            first_heartbeat: AsOfTick::from_millis(84_999).unwrap(),
            second_delay: Duration::from_millis(2),
            now: Arc::new(std::sync::atomic::AtomicU64::new(100_000)),
        });
        assert!(
            expired
                .prove_at(tick, Duration::from_secs(1))
                .await
                .is_err(),
            "A was already expired at the captured tick"
        );
        assert_eq!(expired.journal.directive(), TransactionDirective::Activate);
        let _ = expired.rollback_after_failed_proof().await;
        let path =
            journal::activation_dir(&expired.cache).join(expired.journal.unit.journal_name());
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::RollBack
        );
        assert_eq!(
            expired
                .control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn hung_status_at_uses_one_deadline_and_releases_gate_for_rollback() {
        let mut case = ReadyProofCase::new();
        *case.control.hung_status_at.lock().unwrap() =
            Some(case.prepared[1].entry.socket().to_path_buf());
        let gate =
            muxe_adapter_zellij::ReadinessGate::new(case.cache.clone(), case.identity.unit());
        let read = gate.shared(Duration::from_secs(1)).await.unwrap();
        let error = case
            .prove_at(
                AsOfTick::from_millis(100).unwrap(),
                Duration::from_millis(150),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"), "{error}");
        drop(read);
        let publication = gate.exclusive(Duration::from_secs(1)).await.unwrap();
        drop(publication);
        let _ = case.rollback_after_failed_proof().await;
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::RollBack
        );
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn readiness_gate_seals_as_of_proof_before_later_loss_and_ready_fsync() {
        let mut case = ReadyProofCase::new();
        let gate =
            muxe_adapter_zellij::ReadinessGate::new(case.cache.clone(), case.identity.unit());
        let mut stale = case.status(0);
        stale.ready = None;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, mut completed_rx) = tokio::sync::oneshot::channel();
        *case.control.loss.lock().unwrap() = Some(ReadinessLossHook {
            second: case.prepared[1].entry.socket().to_path_buf(),
            first: case.prepared[0].entry.socket().to_path_buf(),
            stale,
            gate: gate.clone(),
            started: started_tx,
            completed: completed_tx,
        });
        let read = gate.shared(Duration::from_secs(1)).await.unwrap();
        case.prove()
            .await
            .expect("published A and B remain covered under shared gate");
        started_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), &mut completed_rx)
                .await
                .is_err(),
            "A's loss cannot publish between A's status and B's status"
        );
        drop(read);
        completed_rx.await.unwrap();
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        let proof = case.proof();
        assert_eq!(
            persist_ready_decision(&case.cache, &path, &mut case.journal, Some(proof.clone()))
                .unwrap(),
            ReadyWriteOutcome::Durable
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::Commit
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().ready_proof(),
            Some(&proof)
        );
        let mut a = case
            .control
            .connect(case.prepared[0].entry.socket())
            .await
            .unwrap();
        assert!(a.status().await.unwrap().ready.is_none());
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn final_ready_rejects_wrong_target_identity_and_client_census() {
        let mut case = ReadyProofCase::new();
        case.prove().await.unwrap();

        let mut wrong = case.status(0);
        wrong.live_server.host = HostKind::Herdr;
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.bridge_unit = None;
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.handoff_id = Some(handoff(99));
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.current = old_record();
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.live_server.server_id = ServerId::new("foreign");
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.ready = None;
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.ready = Some(ready_census(&["client"], Some(&["client", "client"])));
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.ready = Some(ready_census(&["client", "extra"], Some(&["client"])));
        case.rejects_status(wrong).await;
        let mut wrong = case.status(0);
        wrong.ready = Some(ready_census(&[], Some(&["client"])));
        case.rejects_status(wrong).await;

        case.journal
            .target_record
            .zellij
            .as_mut()
            .unwrap()
            .bridge_build_id = Some(muxe_protocol::SchemaFingerprint([0; 32]));
        let compat = case.journal.target_record.zellij.clone();
        case.journal
            .bridge_mut()
            .unwrap()
            .artifacts
            .receipt_target
            .bridge_compat = compat;
        for index in 0..2 {
            case.set(index, vec![case.status(index)]);
        }
        assert!(
            case.prove().await.is_err(),
            "zero target build ID never proves Ready"
        );
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one fixed Herdr fixture constructs the complete journal, owned child, registry incarnation and live status before exercising exact-endpoint failures"
    )]
    #[tokio::test]
    async fn herdr_final_ready_selects_exact_endpoints_without_hiding_duplicates() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let config = temp.path().join("config");
        Registry::open(&cache).unwrap();
        let endpoint = cache.join("selected.sock");
        let old = BrokerEntry::now("herdr", "server", endpoint.clone(), 1);
        let activation = ActivationId::from_bytes([0x42; 16]).unwrap();
        let handoff = handoff(0x43);
        let member = TransactionMember::new(
            activation,
            ActivationMemberId::new("server".to_owned()).unwrap(),
            MemberEndpoint::new(endpoint.clone()).unwrap(),
            handoff,
            old_record(),
        )
        .unwrap();
        let mut journal = ActivationJournal::new(
            activation,
            UnitKind::Herdr {
                host_hash: unit_hash("server"),
            },
            target_record(),
            vec![member],
        )
        .unwrap();
        journal.old_registry = vec![old.clone()];
        journal.enter_activating();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Ready;
        let handle = h21_owned_targets(1).pop_front().unwrap();
        let mut row = old.clone();
        row.server_pid = handle.child.id();
        row.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        row.live_server = Some("id".to_owned());
        let control = ReadyRoundControl::default();
        control.set(
            &endpoint,
            vec![status_of(
                &journal.target_record,
                Some(handoff),
                "server",
                LifecycleState::Running,
            )],
        );
        let prepared = vec![PreparedMember {
            entry: RegisteredBroker::herdr(old).unwrap(),
            handoff,
            old_session: control.connect(&endpoint).await.unwrap(),
        }];
        let authorities = vec![PreparedAuthority {
            entry: prepared[0].entry.clone(),
            handoff,
        }];
        let mut targets = vec![OwnedTarget {
            member: journal.members()[0].id.clone(),
            handle,
        }];
        let inputs = ActivateInputs {
            config_dir: &config,
            cache_dir: &cache,
            target: journal.target_record.clone(),
            staged_bridge: None,
            scope: HostScope::Herdr,
            current: None,
            control: &control,
            spawner: &ProcessSpawner,
            reloader: &FixtureReloader::default(),
            preflight: &FixturePreflight::default(),
            spawn_policy: &TRUE_SPAWN,
            readiness_deadline: Duration::from_secs(1),
            poll_interval: Duration::from_millis(1),
            hooks: ActivateHooks::default(),
            logger: None,
        };
        let host = HerdrActivation(&prepared[0].entry);
        let prove = async |targets: &mut [OwnedTarget]| {
            prove_ready_unit(
                &inputs,
                &host,
                &journal,
                ReadyUnitMembers {
                    prepared: &prepared,
                    authorities: &authorities,
                    targets,
                },
                ReadyWindow {
                    proof: None,
                    deadline: Instant::now() + Duration::from_secs(1),
                },
            )
            .await
        };
        let registry_path = cache
            .join(super::super::registry::REGISTRY_DIR_NAME)
            .join(super::super::registry::REGISTRY_FILE_NAME);
        let store = |rows: &[BrokerEntry]| {
            fsutil::write_atomic(
                &registry_path,
                &serde_json::to_vec(&serde_json::json!({
                    "schema_version": super::super::registry::REGISTRY_SCHEMA_VERSION,
                    "brokers": rows,
                }))
                .unwrap(),
                "ready-selection-fixture",
            )
            .unwrap();
        };
        let mut unrelated = BrokerEntry::now("herdr", "unrelated", cache.join("stale.sock"), 0);
        unrelated.bridge_member = Some(BridgeMemberId::new("malformed".to_owned()).unwrap());
        store(&[row.clone(), unrelated.clone()]);
        let proof = prove(&mut targets)
            .await
            .expect("unrelated stale malformed row cannot poison selected Ready proof");
        assert_eq!(proof[0].entry.registration_id, row.registration_id);
        for label in ["herdr", "zellij", "unknown-host"] {
            let mut duplicate = row.clone();
            duplicate.host_kind = label.to_owned();
            store(&[row.clone(), unrelated.clone(), duplicate]);
            assert!(
                prove(&mut targets).await.is_err(),
                "duplicate endpoint with {label} label must not certify Ready"
            );
        }
        let mut wrong_host = row.clone();
        wrong_host.host_kind = "zellij".to_owned();
        store(&[wrong_host, unrelated]);
        assert!(
            prove(&mut targets).await.is_err(),
            "selected wrong host must not certify Ready"
        );
        store(&[row]);
        prove(&mut targets)
            .await
            .expect("exact selected incarnation remains certifiable");
    }

    #[tokio::test]
    async fn zellij_final_ready_rejects_foreign_and_unknown_endpoint_duplicates() {
        let mut case = ReadyProofCase::new();
        let registry = Registry::open(&case.cache).unwrap();
        let original = registry.entries().unwrap();
        let mut unrelated =
            BrokerEntry::now("herdr", "unrelated", case.cache.join("stale.sock"), 0);
        unrelated.bridge_member = Some(BridgeMemberId::new("malformed".to_owned()).unwrap());
        registry.register(unrelated.clone()).unwrap();
        case.prove()
            .await
            .expect("unrelated stale malformed row is outside the selected bridge endpoints");
        let registry_path = case
            .cache
            .join(super::super::registry::REGISTRY_DIR_NAME)
            .join(super::super::registry::REGISTRY_FILE_NAME);
        for label in ["herdr", "unknown-host"] {
            let mut rows = original.clone();
            rows.push(unrelated.clone());
            let mut duplicate = rows[0].clone();
            duplicate.host_kind = label.to_owned();
            rows.push(duplicate);
            fsutil::write_atomic(
                &registry_path,
                &serde_json::to_vec(&serde_json::json!({
                    "schema_version": super::super::registry::REGISTRY_SCHEMA_VERSION,
                    "brokers": rows,
                }))
                .unwrap(),
                "ready-selection-fixture",
            )
            .unwrap();
            assert!(
                case.prove().await.is_err(),
                "foreign or unknown duplicate endpoint must not certify Ready"
            );
        }
        let mut rows = original;
        rows[0].host_kind = "herdr".to_owned();
        fsutil::write_atomic(
            &registry_path,
            &serde_json::to_vec(&serde_json::json!({
                "schema_version": super::super::registry::REGISTRY_SCHEMA_VERSION,
                "brokers": rows,
            }))
            .unwrap(),
            "ready-selection-fixture",
        )
        .unwrap();
        assert!(
            case.prove().await.is_err(),
            "selected wrong host must not certify Ready"
        );
    }

    #[tokio::test]
    async fn final_ready_rejects_registry_bridge_artifact_and_receipt_drift() {
        let mut case = ReadyProofCase::new();
        case.prove().await.unwrap();
        let registry = Registry::open(&case.cache).unwrap();
        let row = registry
            .entries()
            .unwrap()
            .into_iter()
            .find(|row| row.socket == case.prepared[0].entry.socket())
            .unwrap();
        let mut foreign = row.clone();
        foreign.server_pid = 42;
        registry.register(foreign).unwrap();
        assert!(
            case.prove().await.is_err(),
            "foreign target process is not authorized"
        );
        registry.register(row).unwrap();

        case.journal.bridge_mut().unwrap().progress = BridgeProgress::TargetInstalled;
        assert!(
            case.prove().await.is_err(),
            "incomplete bridge reload cannot Ready"
        );
        case.journal.bridge_mut().unwrap().progress = BridgeProgress::TargetReloaded;

        let stable = case
            .identity
            .stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        fsutil::write_atomic(&stable, b"foreign", "ready-drift").unwrap();
        assert!(
            case.prove().await.is_err(),
            "stable target bytes must still match"
        );
        fsutil::write_atomic(&stable, b"target-bridge", "ready-drift").unwrap();

        let target_artifact = integration::bridge::artifact_path(
            &case.identity,
            case.journal.bridge().unwrap().artifacts.target,
        );
        fsutil::write_atomic(&target_artifact, b"foreign", "ready-drift").unwrap();
        assert!(
            case.prove().await.is_err(),
            "immutable target artifact must still match"
        );
        fsutil::write_atomic(&target_artifact, b"target-bridge", "ready-drift").unwrap();

        let mut receipt = integration::receipt::load(case.identity.directory())
            .unwrap()
            .unwrap();
        let exact = receipt.clone();
        receipt.bridge.installed_digest =
            integration::receipt::Sha256Digest::from_bytes(b"foreign");
        integration::receipt::store(case.identity.directory(), &receipt).unwrap();
        assert!(
            case.prove().await.is_err(),
            "Ready still requires exact OLD receipt"
        );
        integration::receipt::store(case.identity.directory(), &exact).unwrap();

        let mut extra = case.prepared[0].entry.recorded_entry();
        extra.discovery_key = "session-extra".to_owned();
        extra.bridge_member =
            Some(super::super::registry::BridgeMemberId::new("session-extra".to_owned()).unwrap());
        extra.socket = case.cache.join("extra.sock");
        extra.handoff_id = Some(handoff(3));
        registry.register(extra).unwrap();
        assert!(
            case.prove().await.is_err(),
            "extra logical member cannot enter Ready"
        );
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(case.journal.directive(), TransactionDirective::Activate);
    }

    #[tokio::test]
    async fn complete_group_ready_write_failure_respects_disk_phase() {
        let mut case = ReadyProofCase::new();
        case.prove().await.unwrap();
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        let pre_ready_pids = case
            .targets
            .iter()
            .map(|target| target.handle.child.id())
            .collect::<Vec<_>>();
        let proof = case.proof();
        crate::fsutil::inject_tagged_durability_fault(
            "activation",
            crate::fsutil::DurabilityFault::BeforeRename,
        );
        assert!(matches!(
            persist_and_transfer_ready(
                &case.cache,
                &path,
                &mut case.journal,
                proof,
                &mut case.targets
            )
            .unwrap(),
            ReadyWriteOutcome::NotWritten(_)
        ));
        let _diagnostics = case.rollback_after_failed_proof().await;
        for pid in pre_ready_pids {
            assert_child_reaped(pid);
        }
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::RollBack
        );
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        let mut case = ReadyProofCase::new();
        case.prove().await.unwrap();
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        let proof = case.proof();
        let cleanup = CertifiedChildCleanup::for_targets(&case.targets);
        crate::fsutil::inject_durability_fault(
            crate::fsutil::DurabilityFault::AfterRenameBeforeDirectorySync,
        );
        assert_eq!(
            persist_and_transfer_ready(
                &case.cache,
                &path,
                &mut case.journal,
                proof,
                &mut case.targets
            )
            .unwrap(),
            ReadyWriteOutcome::Durable
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::Commit
        );
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(std::mem::take(&mut case.targets));
        cleanup.assert_live();
        drop(cleanup);
    }

    #[tokio::test]
    async fn persistent_ready_fsync_failure_preserves_exact_disk_certificate() {
        let mut case = ReadyProofCase::new();
        case.prove().await.unwrap();
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        let proof = case.proof();
        let cleanup = CertifiedChildCleanup::for_targets(&case.targets);
        crate::fsutil::inject_durability_fault(
            crate::fsutil::DurabilityFault::AfterRenameBeforeDirectorySync,
        );
        crate::fsutil::inject_persistent_durability_replay_failure(true);
        let failed = persist_and_transfer_ready(
            &case.cache,
            &path,
            &mut case.journal,
            proof.clone(),
            &mut case.targets,
        );
        crate::fsutil::inject_persistent_durability_replay_failure(false);
        assert!(
            failed
                .unwrap_err()
                .to_string()
                .contains("cannot complete durability")
        );
        let on_disk = journal::read_journal(&path).unwrap();
        assert_eq!(on_disk.directive(), TransactionDirective::Commit);
        assert_eq!(on_disk.ready_proof(), Some(&proof));
        assert!(on_disk.has_commit_certificate());
        let outcomes = recover(&case.cache, &case.control, &case.reloader, None)
            .await
            .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::Commit,
            "ambiguous Ready must never be reclassified as pre-Ready rollback"
        );
        drop(std::mem::take(&mut case.targets));
        cleanup.assert_live();
        drop(cleanup);
    }

    #[tokio::test]
    async fn certified_owned_targets_survive_hook_commit_error_and_cancellation() {
        for scenario in ["hook", "commit", "cancel"] {
            let mut case = ReadyProofCase::new();
            case.prove().await.unwrap();
            let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
            let proof = case.proof();
            let cleanup = CertifiedChildCleanup::for_targets(&case.targets);
            assert_eq!(
                persist_and_transfer_ready(
                    &case.cache,
                    &path,
                    &mut case.journal,
                    proof,
                    &mut case.targets,
                )
                .unwrap(),
                ReadyWriteOutcome::Durable
            );
            match scenario {
                "hook" => {
                    let hook = ActivateHooks {
                        fail_after: Some(ActivateStep::ReadinessRecorded),
                    };
                    assert!(matches!(
                        hook.check(ActivateStep::ReadinessRecorded),
                        Err(ActivateError::FaultInjected { .. })
                    ));
                    drop(std::mem::take(&mut case.targets));
                }
                "commit" => {
                    case.journal.enter_committing();
                    for entry in &mut case.journal.old_registry {
                        entry.registration_id =
                            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
                        entry.live_server = Some(format!("old-{}", entry.discovery_key));
                    }
                    for index in 0..case.journal.members().len() {
                        case.journal.members_mut()[index].old = OldMemberProgress::CommitIntent;
                        let server_id = muxe_protocol::wire::ServerId::new(
                            case.journal.old_registry[index]
                                .live_server
                                .as_deref()
                                .unwrap(),
                        );
                        journal::write_old_retirement_receipt(
                            &case.cache,
                            &case.journal,
                            &case.journal.members()[index],
                            &server_id,
                        )
                        .unwrap();
                    }
                    journal::write_journal(&case.cache, &case.journal).unwrap();
                    assert!(
                        recover_commit(&case.cache, &case.control, &mut case.journal, &path)
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("commit target")
                    );
                    drop(std::mem::take(&mut case.targets));
                }
                "cancel" => {
                    let targets = std::mem::take(&mut case.targets);
                    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
                    let task = tokio::spawn(async move {
                        let _targets = targets;
                        entered_tx.send(()).unwrap();
                        std::future::pending::<()>().await;
                    });
                    entered_rx.await.unwrap();
                    task.abort();
                    assert!(task.await.unwrap_err().is_cancelled());
                }
                _ => unreachable!(),
            }
            assert_eq!(
                journal::read_journal(&path).unwrap().directive(),
                TransactionDirective::Commit,
                "{scenario} must leave the original Ready certificate recoverable"
            );
            cleanup.assert_live();
            for member in case.journal.members() {
                certified_target_session(&case.cache, &case.control, &case.journal, member)
                    .await
                    .unwrap();
            }
            cleanup.assert_live();
            drop(cleanup);
        }
    }

    #[test]
    fn ready_journal_write_requires_proof_members_and_bridge_reload() {
        let case = ReadyProofCase::new();
        let path = journal::activation_dir(&case.cache).join(case.journal.unit.journal_name());
        let mut missing_proof = case.journal.clone();
        missing_proof.enter_ready(None);
        assert!(journal::write_journal(&case.cache, &missing_proof).is_err());
        let mut wrong_members = case.journal.clone();
        let mut proof = case.proof();
        proof.member_ids.reverse();
        wrong_members.enter_ready(Some(proof));
        assert!(journal::write_journal(&case.cache, &wrong_members).is_err());
        let mut missing_member = case.journal.clone();
        missing_member.members_mut()[0].target = TargetMemberProgress::Gated;
        missing_member.enter_ready(Some(case.proof()));
        assert!(journal::write_journal(&case.cache, &missing_member).is_err());
        let mut incomplete_bridge = case.journal.clone();
        incomplete_bridge.bridge_mut().unwrap().progress = BridgeProgress::TargetInstalled;
        incomplete_bridge.enter_ready(Some(case.proof()));
        assert!(journal::write_journal(&case.cache, &incomplete_bridge).is_err());
        let mut sealed = case.journal.clone();
        sealed.enter_ready(Some(case.proof()));
        assert!(sealed.validate().is_ok());
        assert!(
            sealed
                .target_registration_capability(
                    case.journal.members()[0].member().as_str(),
                    case.journal.members()[0].endpoint().as_path(),
                    case.journal.members()[0].handoff_id(),
                )
                .is_err(),
            "a new broker cannot mint registration authority after Ready"
        );
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::Activate
        );
    }

    #[tokio::test]
    async fn post_ready_recovery_rejects_registry_and_certificate_drift() {
        let mut case = ReadyProofCase::new();
        let path = case.start_committing_with_retired_old().await;
        let registry = Registry::open(&case.cache).unwrap();
        let original = registry
            .entries()
            .unwrap()
            .into_iter()
            .find(|row| row.socket == case.prepared[0].entry.socket())
            .unwrap();
        let commits = &case.control.commits;
        let mut partial = case.journal.clone();
        partial.members_mut()[0].target = TargetMemberProgress::Committed;
        journal::write_journal(&case.cache, &partial).unwrap();
        let mut replaced_after_commit = original.clone();
        replaced_after_commit.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        registry.register(replaced_after_commit).unwrap();
        assert!(matches!(
            recover(&case.cache, &case.control, &case.reloader, None)
                .await
                .unwrap()
                .as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        registry.register(original.clone()).unwrap();
        journal::write_journal(&case.cache, &case.journal).unwrap();
        let mut wrong_journal = case.journal.clone();
        let mut wrong_proof = case.proof();
        wrong_proof.incarnations[0].entry.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        wrong_journal.transaction.progress_mut().ready_proof = Some(wrong_proof);
        journal::write_journal(&case.cache, &wrong_journal).unwrap();
        assert!(matches!(
            recover(&case.cache, &case.control, &case.reloader, None)
                .await
                .unwrap()
                .as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        journal::write_journal(&case.cache, &case.journal).unwrap();
        for changed in 0..4 {
            let mut replacement = original.clone();
            match changed {
                0 => {
                    replacement.registration_id =
                        Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
                }
                1 => replacement.server_pid += 1,
                2 => replacement.live_server = Some("new-id".to_owned()),
                _ => replacement.started_at += 1,
            }
            registry.register(replacement).unwrap();
            let outcome = recover(&case.cache, &case.control, &case.reloader, None)
                .await
                .unwrap();
            assert!(
                matches!(outcome.as_slice(), [RecoveryOutcome::Preserved { .. }]),
                "replacement dimension {changed} must preserve"
            );
            assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
            assert_eq!(
                journal::read_journal(&path).unwrap().directive(),
                TransactionDirective::Commit
            );
        }
        registry.register(original).unwrap();
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            journal::read_journal(&path).unwrap().directive(),
            TransactionDirective::Commit
        );
    }

    #[tokio::test]
    async fn post_ready_recovery_rejects_live_identity_then_commits_original() {
        let mut case = ReadyProofCase::new();
        let path = case.start_committing_with_retired_old().await;
        let registry = Registry::open(&case.cache).unwrap();
        let original = registry
            .entries()
            .unwrap()
            .into_iter()
            .find(|row| row.socket == case.prepared[0].entry.socket())
            .unwrap();
        let commits = &case.control.commits;
        let mut wrong = case.status(0);
        wrong.live_server.server_id = muxe_protocol::wire::ServerId::new("new-id");
        case.set(0, vec![wrong]);
        assert!(matches!(
            recover(&case.cache, &case.control, &case.reloader, None)
                .await
                .unwrap()
                .as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        case.set(0, vec![case.status(0)]);
        let guard =
            super::super::registry::BridgeUnitGuard::acquire(&case.cache, case.identity.clone())
                .unwrap();
        assert!(registry.unregister_entry(&original, &guard).unwrap());
        drop(guard);
        assert!(matches!(
            recover(&case.cache, &case.control, &case.reloader, None)
                .await
                .unwrap()
                .as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 0);
        registry.register(original).unwrap();
        case.control
            .commit_ok
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let outcome = recover(&case.cache, &case.control, &case.reloader, None)
            .await
            .unwrap();
        assert!(
            matches!(outcome.as_slice(), [RecoveryOutcome::Committed { .. }]),
            "{outcome:?}"
        );
        assert_eq!(commits.load(std::sync::atomic::Ordering::SeqCst), 2);
        let stable = case
            .identity
            .stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        assert_eq!(
            std::fs::read(integration::bridge::previous_path(&stable)).unwrap(),
            b"old-bridge"
        );
        assert_eq!(
            integration::receipt::load(case.identity.directory())
                .unwrap()
                .unwrap()
                .bridge,
            case.journal.bridge().unwrap().artifacts.receipt_target
        );
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn committing_replays_every_bridge_intent_and_action_boundary() {
        for (progress, previous_done, receipt_done) in [
            (BridgeProgress::TargetReloaded, false, false),
            (BridgeProgress::PreviousPublishIntent, false, false),
            (BridgeProgress::PreviousPublishIntent, true, false),
            (BridgeProgress::PreviousPublished, true, false),
            (BridgeProgress::ReceiptIntent, true, false),
            (BridgeProgress::ReceiptIntent, true, true),
            (BridgeProgress::ReceiptPublished, true, false),
            (BridgeProgress::ReceiptPublished, true, true),
        ] {
            let mut case = ReadyProofCase::new();
            case.prove().await.unwrap();
            case.journal.enter_ready(Some(case.proof()));
            case.journal.enter_committing();
            for entry in &mut case.journal.old_registry {
                entry.registration_id =
                    Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
                entry.live_server = Some(format!("old-{}", entry.discovery_key));
            }
            for index in 0..case.journal.members().len() {
                case.journal.members_mut()[index].old = OldMemberProgress::CommitIntent;
                let server_id = muxe_protocol::wire::ServerId::new(
                    case.journal.old_registry[index]
                        .live_server
                        .as_deref()
                        .unwrap(),
                );
                journal::write_old_retirement_receipt(
                    &case.cache,
                    &case.journal,
                    &case.journal.members()[index],
                    &server_id,
                )
                .unwrap();
                case.journal.members_mut()[index].old = OldMemberProgress::Committed;
                case.journal.members_mut()[index].target = TargetMemberProgress::Committed;
            }
            let artifacts = case.journal.bridge().unwrap().artifacts.clone();
            let stable = case
                .identity
                .stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
            if previous_done {
                integration::bridge::publish_previous(
                    &case.identity,
                    artifacts.old,
                    &artifacts.old_digest,
                    &stable,
                    artifacts.receipt_preimage.previous_digest.as_ref(),
                )
                .unwrap();
            }
            if receipt_done {
                publish_bridge_receipt(
                    &case.identity,
                    &artifacts.receipt_preimage,
                    &artifacts.receipt_target,
                )
                .unwrap();
            }
            case.journal.bridge_mut().unwrap().progress = progress.clone();
            let path = journal::write_journal(&case.cache, &case.journal).unwrap();
            let outcomes = recover(&case.cache, &case.control, &case.reloader, None)
                .await
                .unwrap();
            assert!(
                matches!(outcomes.as_slice(), [RecoveryOutcome::Committed { .. }]),
                "{progress:?}: {outcomes:?}"
            );
            assert!(!path.exists(), "{progress:?} left a nonterminal journal");
            assert_eq!(std::fs::read(&stable).unwrap(), b"target-bridge");
            assert_eq!(
                std::fs::read(integration::bridge::previous_path(&stable)).unwrap(),
                b"old-bridge"
            );
            assert_eq!(
                integration::receipt::load(case.identity.directory())
                    .unwrap()
                    .unwrap()
                    .bridge,
                artifacts.receipt_target
            );
            assert_eq!(
                case.control
                    .commits
                    .load(std::sync::atomic::Ordering::SeqCst),
                0,
                "already-committed targets must not receive another Commit RPC"
            );
        }
    }

    #[tokio::test]
    async fn broker_acknowledgements_publish_bridge_before_terminal_commit() {
        let mut case = ReadyProofCase::new();
        case.prove().await.unwrap();
        case.journal.enter_ready(Some(case.proof()));
        case.journal.enter_committing();
        for entry in &mut case.journal.old_registry {
            entry.registration_id =
                Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
            entry.live_server = Some(format!("old-{}", entry.discovery_key));
        }
        let path = journal::write_journal(&case.cache, &case.journal).unwrap();
        let _guard =
            super::super::registry::BridgeUnitGuard::acquire(&case.cache, case.identity.clone())
                .unwrap();
        for index in 0..case.journal.members().len() {
            case.journal.members_mut()[index].old = OldMemberProgress::CommitIntent;
            journal::write_journal(&case.cache, &case.journal).unwrap();
            let member = case.journal.members()[index].clone();
            let server_id = muxe_protocol::wire::ServerId::new(
                case.journal.old_registry[index]
                    .live_server
                    .as_deref()
                    .unwrap(),
            );
            journal::write_old_retirement_receipt(&case.cache, &case.journal, &member, &server_id)
                .unwrap();
            case.journal
                .acknowledge_broker(
                    member.member(),
                    member.handoff_id(),
                    journal::BrokerRecoveryAck::OldCommitted,
                )
                .unwrap();
            journal::write_journal(&case.cache, &case.journal).unwrap();
            assert!(!finish_acknowledged_commit(&case.cache, &mut case.journal, &path).unwrap());
            case.journal
                .acknowledge_broker(
                    member.member(),
                    member.handoff_id(),
                    journal::BrokerRecoveryAck::TargetCommitted,
                )
                .unwrap();
            journal::write_journal(&case.cache, &case.journal).unwrap();
        }
        assert_eq!(case.journal.directive(), TransactionDirective::Commit);
        assert_eq!(
            case.journal.bridge().unwrap().progress,
            BridgeProgress::TargetReloaded
        );
        assert!(finish_acknowledged_commit(&case.cache, &mut case.journal, &path).unwrap());
        assert!(!path.exists());
        let stable = case
            .identity
            .stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        assert_eq!(
            std::fs::read(integration::bridge::previous_path(&stable)).unwrap(),
            b"old-bridge"
        );
        assert_eq!(
            integration::receipt::load(case.identity.directory())
                .unwrap()
                .unwrap()
                .bridge,
            case.journal.bridge().unwrap().artifacts.receipt_target
        );
    }

    #[tokio::test]
    async fn legacy_zellij_ready_without_as_of_proof_is_preserved() {
        let case = ReadyProofCase::new();
        let mut legacy = case.journal.clone();
        legacy.schema_version = 3;
        legacy.enter_ready(None);
        let path = journal::write_journal(&case.cache, &legacy).unwrap();
        let outcomes = recover(&case.cache, &case.control, &case.reloader, None)
            .await
            .unwrap();
        assert!(matches!(

            outcomes.as_slice(),
            [RecoveryOutcome::Preserved { reason, .. }]
                if reason.contains("lacks an exact target incarnation certificate")
        ));
        assert_eq!(journal::read_journal(&path).unwrap(), legacy);
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }
    #[tokio::test]
    async fn legacy_as_of_ready_without_target_incarnations_is_preserved() {
        let case = ReadyProofCase::new();
        let mut legacy = case.journal.clone();
        legacy.schema_version = 4;
        let mut proof = case.proof();
        proof.incarnations.clear();
        legacy.enter_ready(Some(proof));
        let path = journal::write_journal(&case.cache, &legacy).unwrap();
        let outcomes = recover(&case.cache, &case.control, &case.reloader, None)
            .await
            .unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [RecoveryOutcome::Preserved { reason, .. }]
                if reason.contains("lacks an exact target incarnation certificate")
        ));
        assert_eq!(journal::read_journal(&path).unwrap(), legacy);
        assert_eq!(
            case.control
                .commits
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "real children prove both persistence boundaries retain exact ownership through immediate retry"
    )]
    async fn pre_stop_persistence_failures_retain_production_owner_until_retry() {
        {
            let (temp, cache, mut journal, path, control) = rollback_trace_case();
            journal.members_mut()[0].old = OldMemberProgress::Drained;
            journal.members_mut()[0].target = TargetMemberProgress::Gated;
            journal::write_journal(&cache, &journal).unwrap();
            let member_id = journal.members()[0].id.clone();
            let prepared = trace_prepared_member(&control, &journal);
            let endpoint = journal.members()[0].endpoint().as_path().to_path_buf();
            let config = temp.path().join("config");
            std::fs::create_dir(&config).unwrap();
            let reloader = BarrierReloader::default();
            let preflight = FixturePreflight::default();
            let spawner = ProcessSpawner;
            let inputs = ActivateInputs {
                config_dir: &config,
                cache_dir: &cache,
                target: target_record(),
                staged_bridge: None,
                scope: HostScope::Herdr,
                current: None,
                control: &control,
                spawner: &spawner,
                reloader: &reloader,
                preflight: &preflight,
                spawn_policy: &TRUE_SPAWN,
                readiness_deadline: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                hooks: ActivateHooks::default(),
                logger: None,
            };
            let handle = h21_owned_targets(1).pop_front().unwrap();
            let pid = handle.child.id();
            let mut supervisor = ActivationSupervisor::new(
                &cache,
                &journal.unit,
                vec![OwnedTarget {
                    member: member_id.clone(),
                    handle,
                }],
            )
            .unwrap();
            {
                let mut actor = NormalRollbackActor {
                    inputs: &inputs,
                    prepared: vec![prepared],
                    supervisor: &mut supervisor,
                    trace: None,
                };
                crate::fsutil::inject_tagged_durability_fault(
                    "activation",
                    crate::fsutil::DurabilityFault::BeforeRename,
                );
                assert!(matches!(
                    drive_rollback(&mut actor, &mut journal, &path).await,
                    Err(ActivateError::Journal(_))
                ));
            }
            assert!(supervisor.is_live(&member_id));
            assert!(!endpoint.exists());
            let persisted = journal::read_journal(&path).unwrap();
            assert_eq!(persisted.members()[0].target, TargetMemberProgress::Gated);
            assert_eq!(journal.members()[0].target, TargetMemberProgress::Gated);
            let mut actor = NormalRollbackActor {
                inputs: &inputs,
                prepared: vec![trace_prepared_member(&control, &journal)],
                supervisor: &mut supervisor,
                trace: None,
            };
            assert_eq!(
                drive_rollback(&mut actor, &mut journal, &path)
                    .await
                    .unwrap(),
                RollbackDriveOutcome::Complete
            );
            assert_eq!(journal.members()[0].target, TargetMemberProgress::Retired);
            assert!(supervisor.process_id(&member_id).unwrap().is_none());
            assert_child_reaped(pid);
        }

        {
            let (temp, cache, mut journal, path, control) = rollback_trace_case();
            journal.members_mut()[0].old = OldMemberProgress::Drained;
            journal.members_mut()[0].target = TargetMemberProgress::Gated;
            journal.enter_activating();
            journal::write_journal(&cache, &journal).unwrap();
            let member_id = journal.members()[0].id.clone();
            let prepared = trace_prepared_member(&control, &journal);
            let config = temp.path().join("config");
            std::fs::create_dir(&config).unwrap();
            let reloader = BarrierReloader::default();
            let preflight = FixturePreflight::default();
            let spawner = ProcessSpawner;
            let inputs = ActivateInputs {
                config_dir: &config,
                cache_dir: &cache,
                target: target_record(),
                staged_bridge: None,
                scope: HostScope::Herdr,
                current: None,
                control: &control,
                spawner: &spawner,
                reloader: &reloader,
                preflight: &preflight,
                spawn_policy: &TRUE_SPAWN,
                readiness_deadline: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                hooks: ActivateHooks::default(),
                logger: None,
            };
            let handle = h21_owned_targets(1).pop_front().unwrap();
            let pid = handle.child.id();
            let unit = PlannedUnit::Herdr {
                entry: RegisteredBroker::herdr(census_member(
                    journal.members()[0].endpoint().as_path().to_path_buf(),
                    "herdr",
                    journal.members()[0].member().as_str(),
                ))
                .unwrap(),
            };
            crate::fsutil::inject_tagged_durability_fault(
                "activation",
                crate::fsutil::DurabilityFault::BeforeRename,
            );
            let diagnostics = rollback_transaction(
                &inputs,
                &unit,
                &mut journal,
                &path,
                vec![prepared],
                vec![OwnedTarget {
                    member: member_id.clone(),
                    handle,
                }],
                "injected rollback decision failure".to_owned(),
            )
            .await;
            assert!(
                diagnostics
                    .iter()
                    .any(|diagnostic| diagnostic.contains("persist rollback decision"))
            );
            assert_eq!(journal.members()[0].target, TargetMemberProgress::Retired);
            assert!(!path.exists(), "immediate retry completed the rollback");
            assert_child_reaped(pid);
        }
    }

    #[tokio::test]
    async fn pre_stop_failure_without_followup_reaps_child_on_supervisor_shutdown() {
        let (temp, cache, mut journal, path, control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::SpawnIntent;
        journal::write_journal(&cache, &journal).unwrap();
        let member_id = journal.members()[0].id.clone();
        let endpoint = journal.members()[0].endpoint().as_path().to_path_buf();
        let config = temp.path().join("config");
        std::fs::create_dir(&config).unwrap();
        let reloader = BarrierReloader::default();
        let preflight = FixturePreflight::default();
        let spawner = ProcessSpawner;
        let inputs = ActivateInputs {
            config_dir: &config,
            cache_dir: &cache,
            target: target_record(),
            staged_bridge: None,
            scope: HostScope::Herdr,
            current: None,
            control: &control,
            spawner: &spawner,
            reloader: &reloader,
            preflight: &preflight,
            spawn_policy: &TRUE_SPAWN,
            readiness_deadline: Duration::from_secs(1),
            poll_interval: Duration::from_millis(1),
            hooks: ActivateHooks::default(),
            logger: None,
        };
        let handle = h21_owned_targets(1).pop_front().unwrap();
        let pid = handle.child.id();
        let mut supervisor = ActivationSupervisor::new(
            &cache,
            &journal.unit,
            vec![OwnedTarget {
                member: member_id.clone(),
                handle,
            }],
        )
        .unwrap();
        {
            let mut actor = NormalRollbackActor {
                inputs: &inputs,
                prepared: Vec::new(),
                supervisor: &mut supervisor,
                trace: None,
            };
            crate::fsutil::inject_tagged_durability_fault(
                "activation",
                crate::fsutil::DurabilityFault::BeforeRename,
            );
            assert!(matches!(
                drive_rollback(&mut actor, &mut journal, &path).await,
                Err(ActivateError::Journal(_))
            ));
        }
        assert!(supervisor.is_live(&member_id));
        assert!(!endpoint.exists());
        assert_eq!(
            journal::read_journal(&path).unwrap().members()[0].target,
            TargetMemberProgress::SpawnIntent
        );
        assert_eq!(
            journal.members()[0].target,
            TargetMemberProgress::SpawnIntent
        );
        drop(supervisor);
        assert_child_reaped(pid);
        assert!(
            path.exists(),
            "unresolved journal remains for next-process diagnostics"
        );
    }

    #[tokio::test]
    async fn unreceipted_owned_retire_intent_preserves_after_supervisor_shutdown() {
        let (_temp, cache, mut journal, path, mut control) = rollback_trace_case();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        let member = journal.members()[0].clone();
        let handle = h21_owned_targets(1).pop_front().unwrap();
        let pid = handle.child.id();
        let intent = TargetRetirementIntent::new(
            &journal,
            &member,
            TargetRetirementAuthority::OwnedProcess {
                process_id: TargetProcessId::new(pid).unwrap(),
            },
        )
        .unwrap();
        journal.members_mut()[0].target = TargetMemberProgress::RetireIntent;
        journal.members_mut()[0].target_retirement = Some(intent);
        journal::write_journal(&cache, &journal).unwrap();
        let supervisor = ActivationSupervisor::new(
            &cache,
            &journal.unit,
            vec![OwnedTarget {
                member: member.id.clone(),
                handle,
            }],
        )
        .unwrap();
        let diagnostics = supervisor.shutdown(&ProcessSpawner);
        assert!(
            diagnostics
                .iter()
                .any(|line| line.contains("without retirement receipt"))
        );
        assert_child_reaped(pid);
        control.silent = true;
        let recovered = recover(&cache, &control, &BarrierReloader::default(), None)
            .await
            .unwrap();
        assert!(matches!(
            recovered.as_slice(),
            [RecoveryOutcome::Preserved { reason, .. }]
                if reason.contains("owned target authority cannot outlive its activation supervisor")
        ));
        assert!(path.exists());
        assert!(
            !journal::has_target_retirement_receipt(
                &journal::activation_dir(&cache),
                &journal,
                &journal.members()[0],
            )
            .unwrap()
        );
    }

    #[test]
    fn supervisor_rejects_duplicate_member_and_foreign_cache_or_unit() {
        let (_temp, cache, journal, _path, _control) = rollback_trace_case();
        let member_id = journal.members()[0].id.clone();
        let mut handles = h21_owned_targets(2);
        let first = handles.pop_front().unwrap();
        let second = handles.pop_front().unwrap();
        let pids = [first.child.id(), second.child.id()];
        let (error, targets) = ActivationSupervisor::new(
            &cache,
            &journal.unit,
            vec![
                OwnedTarget {
                    member: member_id.clone(),
                    handle: first,
                },
                OwnedTarget {
                    member: member_id,
                    handle: second,
                },
            ],
        )
        .unwrap_err();
        assert!(matches!(error, ActivateError::UnitFailed { .. }));
        let diagnostics = ActivationSupervisor::shutdown_targets(targets, &ProcessSpawner);
        for pid in pids {
            assert_child_reaped(pid);
            assert!(diagnostics.iter().any(|diagnostic| {
                diagnostic.contains(&format!("pid {pid} without retirement receipt"))
            }));
        }
        let supervisor = ActivationSupervisor::new(&cache, &journal.unit, Vec::new()).unwrap();
        assert!(supervisor.check_scope(&cache, &journal).is_ok());
        assert!(
            supervisor
                .check_scope(cache.join("foreign").as_path(), &journal)
                .is_err()
        );
        let mut foreign = journal;
        foreign.unit = UnitKind::Herdr {
            host_hash: unit_hash("foreign"),
        };
        assert!(supervisor.check_scope(&cache, &foreign).is_err());
    }

    #[tokio::test]
    async fn terminal_stop_failures_surface_exact_member_pid_and_operation() {
        for (fault, operation) in [
            (
                TargetStopFault::Inspect,
                "target process state inspection failed",
            ),
            (TargetStopFault::Kill, "target process termination failed"),
            (TargetStopFault::Wait, "target process reap failed"),
        ] {
            let (temp, cache, mut journal, path, mut control) = rollback_trace_case();
            control.silent = true;
            let member_id = journal.members()[0].id.clone();
            let unit = PlannedUnit::Herdr {
                entry: RegisteredBroker::herdr(census_member(
                    journal.members()[0].endpoint().as_path().to_path_buf(),
                    "herdr",
                    journal.members()[0].member().as_str(),
                ))
                .unwrap(),
            };
            let config = temp.path().join("config");
            std::fs::create_dir(&config).unwrap();
            let reloader = BarrierReloader::default();
            let preflight = FixturePreflight::default();
            let spawner = ProcessSpawner;
            let inputs = ActivateInputs {
                config_dir: &config,
                cache_dir: &cache,
                target: target_record(),
                staged_bridge: None,
                scope: HostScope::Herdr,
                current: None,
                control: &control,
                spawner: &spawner,
                reloader: &reloader,
                preflight: &preflight,
                spawn_policy: &TRUE_SPAWN,
                readiness_deadline: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                hooks: ActivateHooks::default(),
                logger: None,
            };
            let mut handle = h21_owned_targets(1).pop_front().unwrap();
            let pid = handle.child.id();
            handle.stop_fault = Some(fault);
            let diagnostics = rollback_transaction(
                &inputs,
                &unit,
                &mut journal,
                &path,
                Vec::new(),
                vec![OwnedTarget {
                    member: member_id.clone(),
                    handle,
                }],
                "old status unavailable".to_owned(),
            )
            .await;
            let UnitOutcome::Failed { reason, .. } = rollback_outcome(
                "herdr".to_owned(),
                "old status unavailable".to_owned(),
                &diagnostics,
            ) else {
                panic!("shutdown process failure must fail the unit");
            };
            assert!(reason.contains(operation), "{reason}");
            assert!(reason.contains(&format!("{member_id:?}")), "{reason}");
            assert!(reason.contains(&format!("pid {pid}")), "{reason}");
            assert_child_reaped(pid);
            assert_eq!(
                journal::read_journal(&path).unwrap().directive(),
                TransactionDirective::RollBack
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "one end-to-end comparison keeps the three real rollback actors and their ordered durable traces visibly identical"
    )]
    async fn rollback_actors_share_prepare_resume_ack_terminal_trace() {
        let normal_trace = Arc::new(Mutex::new(Vec::new()));
        {
            let (temp, cache, mut journal, path, control) = rollback_trace_case();
            let config = temp.path().join("config");
            std::fs::create_dir(&config).unwrap();
            let reloader = FixtureReloader::default();
            let preflight = FixturePreflight::default();
            let spawner = ProcessSpawner;
            let inputs = ActivateInputs {
                config_dir: &config,
                cache_dir: &cache,
                target: target_record(),
                staged_bridge: None,
                scope: HostScope::Herdr,
                current: None,
                control: &control,
                spawner: &spawner,
                reloader: &reloader,
                preflight: &preflight,
                spawn_policy: &TRUE_SPAWN,
                readiness_deadline: Duration::from_secs(1),
                poll_interval: Duration::from_millis(1),
                hooks: ActivateHooks::default(),
                logger: None,
            };
            let member = journal.members()[0].clone();
            let prepared = PreparedMember {
                entry: RegisteredBroker::herdr(census_member(
                    member.endpoint().as_path().to_path_buf(),
                    "herdr",
                    member.member().as_str(),
                ))
                .unwrap(),
                handoff: member.handoff_id(),
                old_session: control.session.clone(),
            };
            let mut supervisor =
                ActivationSupervisor::new(&cache, &journal.unit, Vec::new()).unwrap();
            let mut actor = NormalRollbackActor {
                inputs: &inputs,
                prepared: vec![prepared],
                supervisor: &mut supervisor,
                trace: Some(Arc::clone(&normal_trace)),
            };
            assert_eq!(
                drive_rollback(&mut actor, &mut journal, &path)
                    .await
                    .unwrap(),
                RollbackDriveOutcome::Complete
            );
            assert_eq!(
                control
                    .session
                    .resumes
                    .load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            assert!(!path.exists());
        }

        let coordinator_trace = Arc::new(Mutex::new(Vec::new()));
        {
            let (_temp, cache, mut journal, path, control) = rollback_trace_case();
            let reloader = FixtureReloader::default();
            let mut actor = RecoveryRollbackActor {
                cache_dir: &cache,
                control: &control,
                reloader: &reloader,
                local_member: None,
                local_status: None,
                local_can_resume: false,
                trace: Some(Arc::clone(&coordinator_trace)),
            };
            assert_eq!(
                drive_rollback(&mut actor, &mut journal, &path)
                    .await
                    .unwrap(),
                RollbackDriveOutcome::Complete
            );
            assert_eq!(
                control
                    .session
                    .resumes
                    .load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            assert!(!path.exists());
        }

        let broker_trace = Arc::new(Mutex::new(Vec::new()));
        {
            let (_temp, cache, mut journal, path, control) = rollback_trace_case();
            let reloader = FixtureReloader::default();
            let member = journal.members()[0].member().clone();
            let status = control.session.status.lock().unwrap().clone();
            let mut actor = RecoveryRollbackActor {
                cache_dir: &cache,
                control: &control,
                reloader: &reloader,
                local_member: Some(&member),
                local_status: Some(&status),
                local_can_resume: true,
                trace: Some(Arc::clone(&broker_trace)),
            };
            assert_eq!(
                drive_rollback(&mut actor, &mut journal, &path)
                    .await
                    .unwrap(),
                RollbackDriveOutcome::ResumeRequired
            );
            assert_eq!(
                journal::read_journal(&path).unwrap().members()[0].old,
                OldMemberProgress::ResumeIntent
            );
            let mut service_session = control.session.clone();
            service_session
                .abort(&journal.members()[0].handoff_id())
                .await
                .unwrap();
            journal
                .acknowledge_broker(
                    &member,
                    journal.members()[0].handoff_id(),
                    journal::BrokerRecoveryAck::Resumed,
                )
                .unwrap();
            journal::write_journal(&cache, &journal).unwrap();
            actor.observe("resumed", &RollbackAction::ResumeOld(0), &journal);
            assert_eq!(
                drive_rollback(&mut actor, &mut journal, &path)
                    .await
                    .unwrap(),
                RollbackDriveOutcome::Complete
            );
            assert_eq!(
                control
                    .session
                    .resumes
                    .load(std::sync::atomic::Ordering::SeqCst),
                1
            );
            assert!(!path.exists());
        }

        let normal = normal_trace.lock().unwrap().clone();
        let coordinator = coordinator_trace.lock().unwrap().clone();
        let broker = broker_trace.lock().unwrap().clone();
        assert_eq!(normal, coordinator);
        assert_eq!(coordinator, broker);
        assert_eq!(
            normal
                .iter()
                .map(|entry| entry.split(':').next().unwrap())
                .collect::<Vec<_>>(),
            [
                "prepare_evidence",
                "prepare_resolved",
                "resume_intent",
                "resumed",
                "terminal_written",
                "cleanup_complete",
            ]
        );
    }

    #[tokio::test]
    async fn broker_resume_crash_before_ack_does_not_resume_twice() {
        let (_temp, cache, mut journal, path, control) = rollback_trace_case();
        let reloader = FixtureReloader::default();
        let member = journal.members()[0].member().clone();
        let draining = control.session.status.lock().unwrap().clone();
        let mut actor = RecoveryRollbackActor {
            cache_dir: &cache,
            control: &control,
            reloader: &reloader,
            local_member: Some(&member),
            local_status: Some(&draining),
            local_can_resume: true,
            trace: None,
        };
        assert_eq!(
            drive_rollback(&mut actor, &mut journal, &path)
                .await
                .unwrap(),
            RollbackDriveOutcome::ResumeRequired
        );
        let mut service_session = control.session.clone();
        service_session
            .abort(&journal.members()[0].handoff_id())
            .await
            .unwrap();
        drop(actor);

        let mut recovered = journal::read_journal(&path).unwrap();
        assert_eq!(recovered.members()[0].old, OldMemberProgress::ResumeIntent);
        let running = control.session.status.lock().unwrap().clone();
        let mut replay = RecoveryRollbackActor {
            cache_dir: &cache,
            control: &control,
            reloader: &reloader,
            local_member: Some(&member),
            local_status: Some(&running),
            local_can_resume: true,
            trace: None,
        };
        assert_eq!(
            drive_rollback(&mut replay, &mut recovered, &path)
                .await
                .unwrap(),
            RollbackDriveOutcome::Complete
        );
        assert_eq!(
            control
                .session
                .resumes
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn prepare_resolution_accepts_exact_running_and_preserves_ambiguity() {
        for ambiguous in ["silence", "mismatch"] {
            let (_temp, cache, mut journal, path, mut control) = rollback_trace_case();
            if ambiguous == "silence" {
                control.silent = true;
            } else {
                control.session.status.lock().unwrap().handoff_id = Some(handoff(0x73));
            }
            let reloader = FixtureReloader::default();
            let mut actor = RecoveryRollbackActor {
                cache_dir: &cache,
                control: &control,
                reloader: &reloader,
                local_member: None,
                local_status: None,
                local_can_resume: false,
                trace: None,
            };
            assert!(
                drive_rollback(&mut actor, &mut journal, &path)
                    .await
                    .is_err()
            );
            let preserved = journal::read_journal(&path).unwrap();
            assert_eq!(
                preserved.members()[0].old,
                OldMemberProgress::PrepareIntent,
                "{ambiguous} evidence must not mutate PrepareIntent"
            );
        }

        let (_temp, cache, mut journal, path, control) = rollback_trace_case();
        {
            let mut status = control.session.status.lock().unwrap();
            status.lifecycle = LifecycleState::Running;
            status.target = None;
            status.handoff_id = None;
        }
        let trace = Arc::new(Mutex::new(Vec::new()));
        let reloader = FixtureReloader::default();
        let mut actor = RecoveryRollbackActor {
            cache_dir: &cache,
            control: &control,
            reloader: &reloader,
            local_member: None,
            local_status: None,
            local_can_resume: false,
            trace: Some(Arc::clone(&trace)),
        };
        assert_eq!(
            drive_rollback(&mut actor, &mut journal, &path)
                .await
                .unwrap(),
            RollbackDriveOutcome::Complete
        );
        assert!(
            trace.lock().unwrap()[1].contains(":Pending:"),
            "only exact Running-old/no-handoff resolves PrepareIntent to Pending"
        );
        assert_eq!(
            control
                .session
                .resumes
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[derive(Clone)]
    struct StaticSession {
        status: ActivationStatus,
    }

    impl ControlSession for StaticSession {
        async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.clone())
        }

        async fn prepare(
            &mut self,
            _target: &CompatibilityRecord,
            _handoff: &HandoffId,
        ) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.clone())
        }

        async fn commit(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.clone())
        }

        async fn abort(&mut self, _handoff: &HandoffId) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.clone())
        }

        async fn retire(&mut self) -> Result<ActivationStatus, ControlError> {
            Ok(self.status.clone())
        }
    }

    #[derive(Clone)]
    struct StaticControl {
        status: ActivationStatus,
    }

    impl ControlPort for StaticControl {
        type Session = StaticSession;

        async fn connect(&self, _socket: &Path) -> Result<Self::Session, ControlError> {
            Ok(StaticSession {
                status: self.status.clone(),
            })
        }
    }

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        reason = "the crash-replay scenario keeps artifact, receipt, journal, and durability-fault evidence in one end-to-end test"
    )]
    async fn recovery_replays_old_install_crash_and_terminal_cleanup_idempotently() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let cache = temp.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::os::unix::fs::PermissionsExt::from_mode(0o700))
            .unwrap();
        let identity = BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let stable = identity.stable_path(std::ffi::OsStr::new(integration::BRIDGE_FILE_NAME));
        let old_bytes = b"old-bridge";
        let target_bytes = b"target-bridge";
        let old_digest = integration::receipt::Sha256Digest::from_bytes(old_bytes);
        let target_digest = integration::receipt::Sha256Digest::from_bytes(target_bytes);
        let activation = ActivationId::from_bytes([0x44; 16]).unwrap();
        let old_artifact = BridgeArtifactId::new(activation, BridgeArtifactRole::Old);
        let target_artifact = BridgeArtifactId::new(activation, BridgeArtifactRole::Target);
        integration::bridge::ensure_artifact(&identity, old_artifact, old_bytes, &old_digest)
            .unwrap();
        integration::bridge::ensure_artifact(
            &identity,
            target_artifact,
            target_bytes,
            &target_digest,
        )
        .unwrap();
        // Crash witness: stable already contains old bytes, while the journal
        // still records OldInstallIntent.
        crate::fsutil::inject_durability_fault(
            crate::fsutil::DurabilityFault::AfterRenameBeforeDirectorySync,
        );
        assert!(
            crate::fsutil::write_atomic(&stable, old_bytes, "recovery-test").is_err(),
            "rename completes before the injected directory-sync failure"
        );
        assert_eq!(std::fs::read(&stable).unwrap(), old_bytes);
        let receipt_preimage = integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: "0.1.0".to_owned(),
            installed_digest: old_digest.clone(),
            previous_digest: None,
            bridge_compat: old_record().zellij,
        };
        let receipt_target = integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: "0.2.0".to_owned(),
            installed_digest: target_digest.clone(),
            previous_digest: Some(old_digest.clone()),
            bridge_compat: target_record().zellij,
        };
        let receipt_rollback = integration::receipt::BridgeRecord {
            previous_digest: Some(target_digest.clone()),
            ..receipt_preimage.clone()
        };
        integration::receipt::store(
            identity.directory(),
            &integration::receipt::Receipt {
                schema_version: integration::receipt::RECEIPT_SCHEMA_VERSION,
                bridge: receipt_target.clone(),
                configs: Vec::new(),
            },
        )
        .unwrap();

        let endpoint = cache.join("session-a.sock");
        let handoff = handoff(0x55);
        let member = TransactionMember::new(
            activation,
            ActivationMemberId::new("session-a".to_owned()).unwrap(),
            MemberEndpoint::new(endpoint.clone()).unwrap(),
            handoff,
            old_record(),
        )
        .unwrap();
        let mut journal = ActivationJournal::new(
            activation,
            UnitKind::Zellij {
                bridge_unit: identity.unit(),
            },
            target_record(),
            vec![member],
        )
        .unwrap();
        let bridge_member =
            super::super::registry::BridgeMemberId::new("session-a".to_owned()).unwrap();
        journal
            .bind_zellij_authority(
                identity.clone(),
                MemberCensus::from_members(vec![bridge_member.clone()]).unwrap(),
                BridgeArtifacts {
                    old: old_artifact,
                    target: target_artifact,
                    old_digest: old_digest.clone(),
                    target_digest: target_digest.clone(),
                    receipt_preimage,
                    receipt_target,
                    receipt_rollback,
                },
            )
            .unwrap();
        let mut old_entry = BrokerEntry::now("zellij", "session-a", endpoint, std::process::id());
        old_entry.bridge_identity = Some(identity.clone());
        old_entry.bridge_member = Some(bridge_member);
        old_entry.live_server = Some("session-a".to_owned());
        journal.old_registry = vec![old_entry];
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        let member = journal.members()[0].clone();
        let retirement = TargetRetirementIntent::new(
            &journal,
            &member,
            TargetRetirementAuthority::RemoteServer {
                server_id: ServerId::new("retired-target"),
            },
        )
        .unwrap();
        let member = &mut journal.members_mut()[0];
        member.target = TargetMemberProgress::Retired;
        member.target_retirement = Some(retirement);
        journal.enter_rollback("injected crash".to_owned());
        journal
            .bridge_mut()
            .expect("Zellij bridge progress")
            .progress = BridgeProgress::OldInstallIntent;
        let journal_path = journal::write_journal(&cache, &journal).unwrap();

        let control = StaticControl {
            status: ActivationStatus {
                lifecycle: LifecycleState::Running,
                phase: muxe_protocol::control::ActivationPhase::Ordinary,
                registration: None,
                live_server: LiveServerIdentity {
                    host: HostKind::Zellij,
                    discovery_key: "session-a".to_owned(),
                    server_id: ServerId::new("old-server"),
                },
                current: old_record(),
                target: None,
                handoff_id: None,
                prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
                bridge_unit: Some(identity.unit()),
                ready: None,
            },
        };
        let reloader = FixtureReloader::default();
        let exact_target_receipt = journal
            .bridge()
            .expect("journal retains exact receipt states")
            .artifacts
            .receipt_target
            .clone();
        let mut foreign_receipt = integration::receipt::load(identity.directory())
            .unwrap()
            .unwrap();
        foreign_receipt.bridge.installed_version = "foreign-same-digest".to_owned();
        integration::receipt::store(identity.directory(), &foreign_receipt).unwrap();
        let preserved = recover(&cache, &control, &reloader, None).await.unwrap();
        assert!(matches!(
            preserved.as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert_eq!(
            integration::receipt::load(identity.directory())
                .unwrap()
                .unwrap()
                .bridge
                .installed_version,
            "foreign-same-digest",
            "same bridge digest never authorizes foreign receipt metadata overwrite"
        );
        foreign_receipt.bridge = exact_target_receipt;
        integration::receipt::store(identity.directory(), &foreign_receipt).unwrap();
        crate::fsutil::inject_durability_fault(
            crate::fsutil::DurabilityFault::AfterUnlinkBeforeDirectorySync,
        );
        let interrupted = recover(&cache, &control, &reloader, None).await.unwrap();
        assert!(matches!(
            interrupted.as_slice(),
            [RecoveryOutcome::Preserved { .. }]
        ));
        assert!(
            journal_path.exists(),
            "terminal journal survives an artifact unlink sync failure"
        );
        assert!(
            !integration::bridge::artifact_path(&identity, old_artifact).exists(),
            "unlink completed before the injected directory-sync failure"
        );
        let outcomes = recover(&cache, &control, &reloader, None).await.unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [RecoveryOutcome::RolledBack { .. }]
        ));
        assert_eq!(std::fs::read(&stable).unwrap(), old_bytes);
        assert_eq!(
            std::fs::read(integration::bridge::previous_path(&stable)).unwrap(),
            target_bytes
        );
        let restored = integration::receipt::load(identity.directory())
            .unwrap()
            .unwrap();
        assert_eq!(restored.bridge.installed_digest, old_digest);
        assert_eq!(restored.bridge.previous_digest, Some(target_digest));
        assert!(!journal_path.exists());
        assert!(
            !integration::bridge::artifact_path(&identity, old_artifact).exists()
                && !integration::bridge::artifact_path(&identity, target_artifact).exists()
        );
        assert!(
            recover(&cache, &control, &reloader, None)
                .await
                .unwrap()
                .is_empty(),
            "repeated recovery after terminal cleanup is a no-op"
        );
    }

    /// Serves one fixed `ping` result on every connection to `socket`.
    fn serve_herdr_pong(socket: &Path, version: &str, protocol: u64) -> JoinHandle<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = UnixListener::bind(socket).unwrap();
        let version = version.to_owned();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let mut reader = BufReader::new(stream);
                let mut line = Vec::new();
                if reader.read_until(b'\n', &mut line).await.is_err() {
                    continue;
                }
                let request: serde_json::Value = serde_json::from_slice(&line).unwrap();
                let response = serde_json::json!({
                    "id": request["id"],
                    "result": { "type": "pong", "protocol": protocol, "version": version },
                });
                let _ = reader
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await;
            }
        })
    }

    #[tokio::test]
    async fn herdr_preflight_enforces_only_the_minimum_release_under_every_version_policy() {
        let temp = tempfile::tempdir().unwrap();
        let schema = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/herdr/herdr-api.schema.json");
        let herdr = temp.path().join("herdr");
        crate::generated_executable::write_executable_script(&herdr, |writer| {
            std::io::Write::write_all(
                writer,
                format!("#!/bin/sh\nexec cat '{}'\n", schema.display()).as_bytes(),
            )
        })
        .unwrap();
        let cases = [
            ("0.9.4", 22, "strict", true),
            ("1.0.0", 99, "strict", true),
            ("0.9.4", 22, "min", true),
            ("0.8.2", 20, "strict", true),
            ("0.8.1", 20, "off", false),
            ("0.8.1", 22, "strict", false),
        ];
        for (case, (version, protocol, policy, accepted)) in cases.into_iter().enumerate() {
            let socket = temp.path().join(format!("h{case}.sock"));
            let server = serve_herdr_pong(&socket, version, protocol);
            let config_path = temp.path().join(format!("config{case}.yml"));
            std::fs::write(
                &config_path,
                format!(
                    "version: 1\nsettings:\n  host:\n    version: {{ check: {policy} }}\nmenus:\n  main:\n    bindings:\n      q: {{ label: quit, action: menu:quit }}\n"
                ),
            )
            .unwrap();
            let entry = RegisteredBroker::herdr(census_member(
                socket.clone(),
                "herdr",
                &socket.display().to_string(),
            ))
            .unwrap();
            let live = LivePreflight {
                config_path,
                cache_dir: temp.path().join(format!("cache{case}")),
                herdr_binary: Some(herdr.clone()),
                zellij_exe: None,
                logger: None,
            };
            let outcome = HerdrActivation(&entry).validate_live_host(&live).await;
            server.abort();
            assert_eq!(
                outcome.is_ok(),
                accepted,
                "Herdr {version} (protocol {protocol}) under {policy}: {outcome:?}"
            );
        }
    }
}
