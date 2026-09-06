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
    ActivationStatus, CompatibilityRecord, HandoffId, LifecycleState, TargetReadiness,
};
use thiserror::Error;

use crate::{
    cli::HostScope,
    compatibility,
    fsutil::{self, FsError},
    integration,
    logging::Logger,
};

use super::{
    control::{ControlClient, ControlError, handoff_from_hex},
    journal::{
        self, ActivationJournal, JournalError, JournalState, MemberState, MemberTransition,
        UnitKind, unit_hash,
    },
    registry::{BrokerEntry, Registry, RegistryError},
};

/// Activation transaction boundaries for failure injection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActivateStep {
    PreflightDone,
    JournalWritten,
    OldPrepared,
    TargetSpawned,
    BridgeSwapped,
    ReloadIssued,
    ReadinessRecorded,
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
    Registry(#[from] RegistryError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Control(#[from] ControlError),
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
        bridge_path: PathBuf,
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
    async fn prepare(
        &mut self,
        target: &CompatibilityRecord,
    ) -> Result<ActivationStatus, ControlError>;
    async fn commit(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError>;
    async fn abort(&mut self, handoff: &HandoffId) -> Result<ActivationStatus, ControlError>;
    async fn retire(&mut self) -> Result<ActivationStatus, ControlError>;
}

impl ControlSession for ControlClient {
    async fn status(&mut self) -> Result<ActivationStatus, ControlError> {
        ControlClient::status(self).await
    }
    async fn prepare(
        &mut self,
        target: &CompatibilityRecord,
    ) -> Result<ActivationStatus, ControlError> {
        ControlClient::prepare(self, target.clone()).await
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
}

/// Production control port: real framing over owner-only sockets.
#[derive(Clone, Copy, Debug)]
pub struct LiveControl;

impl ControlPort for LiveControl {
    type Session = ControlClient;
    async fn connect(&self, socket: &Path) -> Result<ControlClient, ControlError> {
        ControlClient::connect(socket).await
    }
}

/// Identity of one prepared member for target spawning.
#[derive(Clone, Debug)]
pub struct SpawnMember {
    pub host_identity: String,
    pub handoff_hex: String,
    /// Normal per-host endpoint the target claims under the startup lock.
    pub endpoint: PathBuf,
}

/// Owned request to start one target broker: the exact executable plus the
/// broker-authored argument vector. Constructed by the caller (the current
/// executable plus the broker's internal serve mode), never from ambient
/// environment fallbacks.
#[derive(Clone, Debug)]
pub struct SpawnRequest {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub host_identity: String,
}

/// Handle to a running target broker: the retained owned child.
pub struct TargetHandle {
    pub child: std::process::Child,
}

/// Starts and stops target brokers. Only owns process mechanics; the argv it
/// executes comes from the caller-owned [`SpawnRequest`].
pub trait BrokerSpawner {
    fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError>;
    fn stop_target(&self, handle: TargetHandle) -> Result<(), ActivateError>;
}

/// Production spawner: real process spawn with a retained owned child and an
/// explicit endpoint. No process-name or global cleanup, ever.
#[derive(Clone, Copy, Debug)]
pub struct ProcessSpawner;

impl BrokerSpawner for ProcessSpawner {
    fn spawn_target(&self, request: &SpawnRequest) -> Result<TargetHandle, ActivateError> {
        let child = std::process::Command::new(&request.program)
            .args(&request.args)
            .spawn()
            .map_err(|source| ActivateError::Spawn(source.to_string()))?;
        Ok(TargetHandle { child })
    }

    fn stop_target(&self, mut handle: TargetHandle) -> Result<(), ActivateError> {
        handle
            .child
            .kill()
            .map_err(|source| ActivateError::Spawn(source.to_string()))?;
        let _ = handle.child.wait();
        Ok(())
    }
}

/// Reloads the stable bridge inside live Zellij sessions.
pub trait HostReloader {
    /// Runs the per-session reload command once for every participating
    /// session. Any session failure aborts the complete Zellij group.
    fn reload_bridge(&self, session: &str, bridge_url: &str) -> Result<(), ActivateError>;
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
}

/// Preflight checks owned by configuration and host-adapter modules.
#[expect(
    async_fn_in_trait,
    reason = "coordinator traits use static dispatch with one implementation per process; no Send bound is required"
)]
pub trait Preflight {
    async fn validate_config(&self) -> Result<(), String>;
    async fn revalidate_herdr_actions(&self, discovery_key: &str) -> Result<(), String>;
    async fn check_host_version(&self, host_kind: &str, discovery_key: &str) -> Result<(), String>;
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
                .map_err(|error| format!("Herdr host {discovery_key} is unreachable: {error}"))
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
}
impl Preflight for LivePreflight<'_> {
    async fn validate_config(&self) -> Result<(), String> {
        self.compile_config().map(|_| ())
    }

    async fn revalidate_herdr_actions(&self, discovery_key: &str) -> Result<(), String> {
        self.herdr_runtime(discovery_key).await.map(|_| ())
    }

    async fn check_host_version(&self, host_kind: &str, discovery_key: &str) -> Result<(), String> {
        let policy = self
            .compile_config()
            .map_err(|error| format!("cannot read version policy: {error}"))?
            .host
            .version_check;
        let (minimum, latest, live) = match host_kind {
            "herdr" => {
                let identity = self.herdr_runtime(discovery_key).await?.identity().clone();
                let version = identity
                    .live_server_id
                    .split("/ver:")
                    .nth(1)
                    .and_then(|tail| tail.split('/').next())
                    .ok_or_else(|| {
                        format!("Herdr host {discovery_key} reports an unrecognized identity shape")
                    })?;
                (
                    crate::compatibility::HERDR_MINIMUM,
                    crate::compatibility::HERDR_LATEST_VERIFIED,
                    version.to_owned(),
                )
            }
            "zellij" => {
                let program = self.zellij_exe.as_ref().ok_or_else(|| {
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
                (
                    crate::compatibility::ZELLIJ_MINIMUM,
                    crate::compatibility::ZELLIJ_LATEST_VERIFIED,
                    version.to_owned(),
                )
            }
            other => return Err(format!("unknown host kind {other}")),
        };
        let live_numbers = Self::version_numbers(&live).ok_or_else(|| {
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
                muxe_core::HostVersionCheck::Min => self.warn(host_kind, message),
                muxe_core::HostVersionCheck::Off => {}
            }
        }
        Ok(())
    }
}

/// One planned activation unit.
#[derive(Clone, Debug)]
pub(crate) enum PlannedUnit {
    Herdr {
        entry: BrokerEntry,
    },
    Zellij {
        bridge_path: PathBuf,
        entries: Vec<BrokerEntry>,
    },
}

impl PlannedUnit {
    fn unit_kind(&self) -> UnitKind {
        match self {
            Self::Herdr { entry } => UnitKind::Herdr {
                host_hash: unit_hash(&entry.discovery_key),
            },
            Self::Zellij { bridge_path, .. } => UnitKind::Zellij {
                bridge_path_hash: unit_hash(&bridge_path.display().to_string()),
            },
        }
    }
}

/// Outcome of one activation unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UnitOutcome {
    Committed { unit: String },
    Unchanged { unit: String },
    RolledBack { unit: String, reason: String },
    Failed { unit: String, reason: String },
}

/// Final activation report naming every unit outcome.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ActivateReport {
    pub units: Vec<UnitOutcome>,
}

/// Inputs for `muxe activate`.
/// Renders the exact owned spawn request for one prepared member: the current
/// executable plus the broker-authored serve arguments.
pub type SpawnArgv<'a> =
    &'a dyn Fn(&SpawnMember) -> Result<(PathBuf, Vec<OsString>), ActivateError>;

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
    /// Builds the exact owned spawn request per member.
    pub spawn_argv: SpawnArgv<'a>,
    pub readiness_deadline: Duration,
    pub poll_interval: Duration,
    pub hooks: ActivateHooks,
    pub logger: Option<&'a Logger>,
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

    // Global preflight before any unit mutates.
    let verified_bridge = global_preflight(&inputs, &units)
        .await
        .map_err(ActivateError::Preflight)?;
    inputs.hooks.check(ActivateStep::PreflightDone)?;

    let mut report = ActivateReport::default();
    for unit in units {
        report
            .units
            .push(activate_unit(&inputs, &unit, verified_bridge.as_ref()).await);
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
            let mut units = Vec::new();
            let mut zellij_groups: std::collections::BTreeMap<PathBuf, Vec<BrokerEntry>> =
                std::collections::BTreeMap::new();
            for entry in live {
                if entry.host_kind == "zellij" {
                    if let Some(bridge) = entry.bridge_path.clone() {
                        zellij_groups.entry(bridge).or_default().push(entry.clone());
                    }
                } else if entry.host_kind == "herdr" {
                    units.push(PlannedUnit::Herdr {
                        entry: entry.clone(),
                    });
                }
            }
            for (bridge_path, entries) in zellij_groups {
                units.push(PlannedUnit::Zellij {
                    bridge_path,
                    entries,
                });
            }
            Ok(units)
        }
        HostScope::Zellij => Ok(group_zellij(live)),
        HostScope::Herdr => Ok(live
            .iter()
            .filter(|entry| entry.host_kind == "herdr")
            .map(|entry| PlannedUnit::Herdr {
                entry: entry.clone(),
            })
            .collect()),
        HostScope::Current => {
            let Some(current) = current else {
                return Err(ActivateError::CurrentHostRequired);
            };
            match current {
                DetectedHost::Herdr { discovery_key } => live
                    .iter()
                    .find(|entry| {
                        entry.host_kind == "herdr" && &entry.discovery_key == discovery_key
                    })
                    .map(|entry| {
                        vec![PlannedUnit::Herdr {
                            entry: entry.clone(),
                        }]
                    })
                    .ok_or(ActivateError::NoLiveUnits),
                DetectedHost::Zellij { bridge_path, .. } => {
                    // A current Zellij host expands to the complete
                    // bridge-sharing group: the stable bridge is one atomic unit.
                    let group: Vec<BrokerEntry> = live
                        .iter()
                        .filter(|entry| {
                            entry.host_kind == "zellij"
                                && entry.bridge_path.as_ref() == Some(bridge_path)
                        })
                        .cloned()
                        .collect();
                    if group.is_empty() {
                        return Err(ActivateError::NoLiveUnits);
                    }
                    Ok(vec![PlannedUnit::Zellij {
                        bridge_path: bridge_path.clone(),
                        entries: group,
                    }])
                }
            }
        }
    }
}

fn group_zellij(live: &[BrokerEntry]) -> Vec<PlannedUnit> {
    let mut groups: std::collections::BTreeMap<PathBuf, Vec<BrokerEntry>> =
        std::collections::BTreeMap::new();
    for entry in live.iter().filter(|entry| entry.host_kind == "zellij") {
        if let Some(bridge) = entry.bridge_path.clone() {
            groups.entry(bridge).or_default().push(entry.clone());
        }
    }
    groups
        .into_iter()
        .map(|(bridge_path, entries)| PlannedUnit::Zellij {
            bridge_path,
            entries,
        })
        .collect()
}

async fn global_preflight<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    units: &[PlannedUnit],
) -> Result<Option<compatibility::NativeAssetVerification>, String>
where
    P: Preflight,
{
    let zellij_selected = units
        .iter()
        .any(|unit| matches!(unit, PlannedUnit::Zellij { .. }));
    let verified_bridge = if zellij_selected {
        let staged = inputs.staged_bridge.as_ref().ok_or_else(|| {
            "a Zellij unit is selected but no staged replacement bridge was provided".to_owned()
        })?;
        Some(
            compatibility::verify_packaged_asset(&staged.bytes).map_err(|error| {
                format!("staged bridge rejected by native package identity: {error}")
            })?,
        )
    } else {
        None
    };
    inputs.preflight.validate_config().await?;
    for unit in units {
        match unit {
            PlannedUnit::Herdr { entry } => {
                inputs
                    .preflight
                    .check_host_version("herdr", &entry.discovery_key)
                    .await?;
                inputs
                    .preflight
                    .revalidate_herdr_actions(&entry.discovery_key)
                    .await?;
            }
            PlannedUnit::Zellij {
                bridge_path,
                entries,
            } => {
                let expected = integration::stable_bridge_path(inputs.config_dir);
                if *bridge_path != expected {
                    return Err(format!(
                        "Zellij unit uses an unmanaged bridge path: {}",
                        bridge_path.display()
                    ));
                }
                for entry in entries {
                    inputs
                        .preflight
                        .check_host_version("zellij", &entry.discovery_key)
                        .await?;
                }
            }
        }
    }
    fsutil::ensure_owner_dir(&journal::activation_dir(inputs.cache_dir))
        .map_err(|error| format!("activation journal directory is not writable: {error}"))?;
    Ok(verified_bridge)
}
async fn activate_unit<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    verified_bridge: Option<&compatibility::NativeAssetVerification>,
) -> UnitOutcome
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    let label = unit_label(unit);
    match activate_unit_inner(inputs, unit, verified_bridge).await {
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
        PlannedUnit::Herdr { entry } => format!("herdr:{}", entry.discovery_key),
        PlannedUnit::Zellij { bridge_path, .. } => {
            format!("zellij:{}", bridge_path.display())
        }
    }
}

/// One prepared member with its retained old-broker session.
struct PreparedMember<C: ControlPort> {
    entry: BrokerEntry,
    old_record: CompatibilityRecord,
    handoff: HandoffId,
    handoff_hex: String,
    old_session: C::Session,
}

async fn activate_unit_inner<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    verified_bridge: Option<&compatibility::NativeAssetVerification>,
) -> Result<UnitOutcome, ActivateError>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    let label = unit_label(unit);
    let entries: Vec<BrokerEntry> = match unit {
        PlannedUnit::Herdr { entry } => vec![entry.clone()],
        PlannedUnit::Zellij { entries, .. } => entries.clone(),
    };

    // Fast path: every old broker already runs the target record.
    let mut all_current = true;
    for entry in &entries {
        match inputs.control.connect(&entry.socket).await {
            Ok(mut session) => match session.status().await {
                Ok(status) if status.current == inputs.target => {}
                _ => {
                    all_current = false;
                    break;
                }
            },
            Err(_) => {
                all_current = false;
                break;
            }
        }
    }
    if all_current {
        return Ok(UnitOutcome::Unchanged { unit: label });
    }

    // Journal before the first external mutation. Handoffs are still unknown,
    // so the journal starts Announced; recovery of an Announced journal
    // probes live state and adopts observed handoffs, never placeholders.
    let mut journal = ActivationJournal::new(
        unit.unit_kind(),
        inputs.target.clone(),
        inputs.target.clone(),
        entries
            .iter()
            .map(|entry| MemberState {
                host_identity: entry.discovery_key.clone(),
                old_socket: entry.socket.clone(),
                target_socket: None,
                handoff_id: None,
                state: MemberTransition::Prepared,
            })
            .collect(),
    );
    journal.state = JournalState::Announced;
    let journal_path = journal::write_journal(inputs.cache_dir, &journal)?;
    inputs.hooks.check(ActivateStep::JournalWritten)?;

    // Drain every old broker over retained sessions.
    let mut prepared: Vec<PreparedMember<C>> = Vec::new();
    let mut drain_failure: Option<String> = None;
    for entry in &entries {
        match drain_one(inputs, entry).await {
            Ok(member) => prepared.push(member),
            Err(reason) => {
                drain_failure = Some(reason);
                break;
            }
        }
    }
    if let Some(reason) = drain_failure {
        let rollback = abort_prepared(
            inputs,
            unit,
            &journal,
            &journal_path,
            prepared,
            Vec::new(),
            None,
        )
        .await;
        return Ok(UnitOutcome::RolledBack {
            unit: label,
            reason: with_rollback(reason, rollback),
        });
    }
    // Rewrite the journal with real old records and handoff IDs.
    journal.state = JournalState::Prepared;
    journal.old_record = prepared
        .first()
        .map(|member| member.old_record.clone())
        .unwrap_or_else(|| inputs.target.clone());
    for (record, member) in journal.members.iter_mut().zip(prepared.iter()) {
        record.handoff_id = Some(member.handoff_hex.clone());
    }
    if matches!(unit, PlannedUnit::Zellij { .. }) {
        let verification = verified_bridge.ok_or_else(|| ActivateError::UnitFailed {
            reason: "Zellij bridge was not verified during global preflight".to_owned(),
        })?;
        journal.staged_bridge_digest = Some(verification.packaged_digest.clone());
    }
    journal::write_journal(inputs.cache_dir, &journal)?;
    inputs.hooks.check(ActivateStep::OldPrepared)?;

    // Start one target broker per prepared old broker.
    let mut targets: Vec<TargetHandle> = Vec::new();
    let mut spawn_failure: Option<String> = None;
    for member in &prepared {
        let spawn_member = SpawnMember {
            host_identity: member.entry.discovery_key.clone(),
            handoff_hex: member.handoff_hex.clone(),
            endpoint: member.entry.socket.clone(),
        };
        let (program, args) = match (inputs.spawn_argv)(&spawn_member) {
            Ok(pair) => pair,
            Err(error) => {
                spawn_failure = Some(error.to_string());
                break;
            }
        };
        match inputs.spawner.spawn_target(&SpawnRequest {
            program,
            args,
            host_identity: spawn_member.host_identity.clone(),
        }) {
            Ok(handle) => {
                if let Some(record) = journal
                    .members
                    .iter_mut()
                    .find(|record| record.old_socket == member.entry.socket)
                {
                    // Targets claim the normal endpoint; the recorded path
                    // keeps naming the member across recovery.
                    record.target_socket = Some(member.entry.socket.clone());
                }
                targets.push(handle);
            }
            Err(error) => {
                spawn_failure = Some(error.to_string());
                break;
            }
        }
    }
    if let Some(reason) = spawn_failure {
        let rollback = abort_prepared(
            inputs,
            unit,
            &journal,
            &journal_path,
            prepared,
            targets,
            None,
        )
        .await;
        return Ok(UnitOutcome::RolledBack {
            unit: label,
            reason: with_rollback(reason, rollback),
        });
    }
    journal::write_journal(inputs.cache_dir, &journal)?;
    inputs.hooks.check(ActivateStep::TargetSpawned)?;

    // Zellij bridge transaction: swap once, reload every session.
    if let PlannedUnit::Zellij { .. } = unit {
        let staged = inputs
            .staged_bridge
            .as_ref()
            .ok_or_else(|| ActivateError::UnitFailed {
                reason: "missing staged bridge for Zellij unit".to_owned(),
            })?;
        let stable = integration::stable_bridge_path(inputs.config_dir);
        let directory = integration::integration_dir(inputs.config_dir);
        fsutil::ensure_owner_dir(&directory)?;
        let (expected_current, authority) = match activation_authority(&directory, &stable) {
            Ok(authority) => authority,
            Err(reason) => {
                let rollback = abort_prepared(
                    inputs,
                    unit,
                    &journal,
                    &journal_path,
                    prepared,
                    targets,
                    None,
                )
                .await;
                return Ok(UnitOutcome::RolledBack {
                    unit: label,
                    reason: with_rollback(reason, rollback),
                });
            }
        };
        let disk_staged = integration::bridge::stage(&stable, &staged.bytes)?;
        if stable.exists() {
            let record = integration::bridge::ensure_backup(&stable, authority.as_deref())?;
            journal.old_bridge_digest = Some(record.digest.clone());
            journal.backup_path = Some(record.path);
            journal::write_journal(inputs.cache_dir, &journal)?;
        }
        if let Err(error) =
            integration::bridge::commit(&disk_staged, &stable, expected_current.as_deref())
        {
            let reason = error.to_string();
            integration::bridge::discard_staging(&disk_staged);
            let rollback = abort_prepared(
                inputs,
                unit,
                &journal,
                &journal_path,
                prepared,
                targets,
                None,
            )
            .await;
            return Ok(UnitOutcome::RolledBack {
                unit: label,
                reason: with_rollback(reason, rollback),
            });
        }
        inputs.hooks.check(ActivateStep::BridgeSwapped)?;
        let bridge_url = integration::kdl::bridge_url(&stable);
        for member in &prepared {
            let session = session_name(&member.entry);
            if let Err(error) = inputs.reloader.reload_bridge(&session, &bridge_url) {
                let reason = error.to_string();
                let rollback = abort_prepared(
                    inputs,
                    unit,
                    &journal,
                    &journal_path,
                    prepared,
                    targets,
                    Some(&bridge_url),
                )
                .await;
                return Ok(UnitOutcome::RolledBack {
                    unit: label,
                    reason: with_rollback(reason, rollback),
                });
            }
        }
        inputs.hooks.check(ActivateStep::ReloadIssued)?;
    }

    // Wait for every target to report ready: exact expected handoff, matching
    // host identity, and the full target compatibility record. Zellij members
    // additionally prove a complete census from one snapshot round; Herdr
    // members keep the subscription/health gate. Any member that never
    // reports ready rolls the whole unit back.
    let deadline = Instant::now() + inputs.readiness_deadline;
    for member in &prepared {
        if let Err(error) = wait_ready(
            inputs.control,
            &member.entry,
            &member.handoff,
            &inputs.target,
            deadline,
            inputs.poll_interval,
        )
        .await
        {
            let reason = error.to_string();
            let bridge_url = match unit {
                PlannedUnit::Zellij { .. } => Some(integration::kdl::bridge_url(
                    &integration::stable_bridge_path(inputs.config_dir),
                )),
                PlannedUnit::Herdr { .. } => None,
            };
            let rollback = abort_prepared(
                inputs,
                unit,
                &journal,
                &journal_path,
                prepared,
                targets,
                bridge_url.as_deref(),
            )
            .await;
            return Ok(UnitOutcome::RolledBack {
                unit: label,
                reason: with_rollback(reason, rollback),
            });
        }
    }
    journal.state = JournalState::Ready;
    for record in journal.members.iter_mut() {
        record.state = MemberTransition::Ready;
    }
    journal::write_journal(inputs.cache_dir, &journal)?;
    inputs.hooks.check(ActivateStep::ReadinessRecorded)?;

    // Commit: old brokers over their retained sessions first, then targets
    // over fresh connections to the claimed endpoint.
    let mut commit_failures = Vec::new();
    for member in prepared.iter_mut() {
        if let Err(error) = member.old_session.commit(&member.handoff).await {
            commit_failures.push(format!(
                "commit old {}: {error}",
                member.entry.discovery_key
            ));
        }
    }
    for member in &prepared {
        match inputs.control.connect(&member.entry.socket).await {
            Ok(mut target_session) => {
                if let Err(error) = target_session.commit(&member.handoff).await {
                    commit_failures.push(format!(
                        "commit target {}: {error}",
                        member.entry.discovery_key
                    ));
                }
            }
            Err(error) => commit_failures.push(format!(
                "connect target {}: {error}",
                member.entry.discovery_key
            )),
        }
    }
    inputs.hooks.check(ActivateStep::Committed)?;
    if commit_failures.is_empty() {
        for record in journal.members.iter_mut() {
            record.state = MemberTransition::Committed;
        }
        journal::remove_journal(&journal_path)?;
        log(inputs.logger, &label, "activation committed")?;
        Ok(UnitOutcome::Committed { unit: label })
    } else {
        Ok(UnitOutcome::Failed {
            unit: label,
            reason: commit_failures.join("; "),
        })
    }
}

/// Drains one old broker over a retained session.
async fn drain_one<C>(
    inputs: &ActivateInputs<'_, C, impl BrokerSpawner, impl HostReloader, impl Preflight>,
    entry: &BrokerEntry,
) -> Result<PreparedMember<C>, String>
where
    C: ControlPort,
{
    let mut session = inputs
        .control
        .connect(&entry.socket)
        .await
        .map_err(|error| format!("connect {}: {error}", entry.discovery_key))?;
    let status = session
        .status()
        .await
        .map_err(|error| format!("status failed for {}: {error}", entry.discovery_key))?;
    let prepared = session
        .prepare(&inputs.target)
        .await
        .map_err(|error| format!("prepare failed for {}: {error}", entry.discovery_key))?;
    let handoff = prepared
        .handoff_id
        .ok_or_else(|| format!("prepare gave no handoff ID for {}", entry.discovery_key))?;
    Ok(PreparedMember {
        old_record: status.current,
        entry: entry.clone(),
        handoff,
        handoff_hex: hex_lower(&handoff.0),
        old_session: session,
    })
}

fn session_name(entry: &BrokerEntry) -> String {
    entry
        .live_server
        .clone()
        .unwrap_or_else(|| entry.discovery_key.clone())
}

/// Determines the swap authority for a Zellij group: the expected current
/// digest plus the receipt authority for rotating the rollback copy.
///
/// Returns the human reason when the existing bridge is untrusted, in which
/// case the caller rolls back before staging anything.
fn activation_authority(
    directory: &Path,
    stable: &Path,
) -> Result<(Option<String>, Option<String>), String> {
    match integration::receipt::load(directory).map_err(|error| error.to_string())? {
        Some(receipt) => {
            if receipt.bridge.canonical_path != stable {
                return Err(format!(
                    "receipt canonical path {} does not match {}",
                    receipt.bridge.canonical_path.display(),
                    stable.display()
                ));
            }
            match std::fs::read(stable) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    Ok((None, receipt.bridge.previous_digest.clone()))
                }
                Err(error) => Err(format!("cannot read existing bridge: {error}")),
                Ok(current) => {
                    let found = fsutil::sha256_hex(&current);
                    if found != receipt.bridge.installed_digest {
                        return Err(format!(
                            "existing bridge digest {found} does not match receipt {}; resolve the file before retrying",
                            receipt.bridge.installed_digest
                        ));
                    }
                    Ok((Some(found), receipt.bridge.previous_digest.clone()))
                }
            }
        }
        None => {
            if stable.exists() {
                return Err(
                    "existing bridge has no receipt; resolve the file before retrying".to_owned(),
                );
            }
            Ok((None, None))
        }
    }
}

/// A target is ready only when it reports the exact expected handoff, the
/// matching host identity, and the complete target compatibility record.
/// Zellij members additionally require a complete, nonempty, duplicate-free
/// census from one snapshot round: every snapshot member holds a fresh
/// compatible registration, so a partially or spuriously registered target
/// can never commit. Herdr keeps its subscription/health gate and never
/// requires this census.
fn target_ready(
    status: &ActivationStatus,
    member: &BrokerEntry,
    expected_handoff: &HandoffId,
    target: &CompatibilityRecord,
) -> bool {
    if status.current != *target
        || status.handoff_id != Some(*expected_handoff)
        || status.live_server.discovery_key != member.discovery_key
        || !matches!(status.lifecycle, LifecycleState::Running)
    {
        return false;
    }
    if member.host_kind == "zellij" {
        return status.ready.as_ref().is_some_and(zellij_census_covered);
    }
    true
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

async fn wait_ready<C>(
    control: &C,
    member: &BrokerEntry,
    expected_handoff: &HandoffId,
    target: &CompatibilityRecord,
    deadline: Instant,
    poll_interval: Duration,
) -> Result<(), ActivateError>
where
    C: ControlPort,
{
    loop {
        match control.connect(&member.socket).await {
            Ok(mut session) => match session.status().await {
                Ok(status) => {
                    if target_ready(&status, member, expected_handoff, target) {
                        return Ok(());
                    }
                }
                Err(_) => {}
            },
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            return Err(ActivateError::ReadinessTimeout {
                identity: member.discovery_key.clone(),
            });
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Combines the triggering failure with every rollback diagnostic. Rollback
/// failures never replace the original error; they extend it.
fn with_rollback(reason: String, diagnostics: Vec<String>) -> String {
    if diagnostics.is_empty() {
        reason
    } else {
        format!("{reason}; rollback: {}", diagnostics.join("; "))
    }
}

/// Aborts a unit across every prepared member: stops all spawned targets,
/// restores the verified backup and reloads switched sessions for a Zellij
/// group, then resumes all old brokers over their retained sessions.
/// Reports the triggering reason plus every rollback failure.
async fn abort_prepared<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
    unit: &PlannedUnit,
    journal: &ActivationJournal,
    journal_path: &Path,
    mut prepared: Vec<PreparedMember<C>>,
    targets: Vec<TargetHandle>,
    bridge_url: Option<&str>,
) -> Vec<String>
where
    C: ControlPort,
    S: BrokerSpawner,
    R: HostReloader,
    P: Preflight,
{
    let _ = journal_path;
    let mut rollback_errors = Vec::new();
    for handle in targets {
        if let Err(error) = inputs.spawner.stop_target(handle) {
            rollback_errors.push(format!("stop target: {error}"));
        }
    }
    if let PlannedUnit::Zellij { .. } = unit {
        if let Some(backup) = journal.backup_path.as_ref() {
            let stable = integration::stable_bridge_path(inputs.config_dir);
            match std::fs::rename(backup, &stable) {
                Ok(()) => {
                    let _ = fsutil::sync_dir_of(&stable);
                    let restored_ok = std::fs::read(&stable).map_or(false, |bytes| {
                        journal
                            .old_bridge_digest
                            .as_ref()
                            .is_some_and(|expected| fsutil::sha256_hex(&bytes) == *expected)
                    });
                    if !restored_ok {
                        rollback_errors.push("restored bridge digest mismatch".to_owned());
                    } else if let Some(url) = bridge_url {
                        for record in &journal.members {
                            let session = record.host_identity.clone();
                            if let Err(error) = inputs.reloader.reload_bridge(&session, url) {
                                rollback_errors.push(format!("rollback reload {session}: {error}"));
                            }
                        }
                    }
                }
                Err(error) => rollback_errors.push(format!(
                    "restore old bridge from {}: {error}",
                    backup.display()
                )),
            }
        }
    }
    for member in prepared.iter_mut() {
        if let Err(error) = member.old_session.abort(&member.handoff).await {
            rollback_errors.push(format!("abort old {}: {error}", member.entry.discovery_key));
        }
    }
    if !rollback_errors.is_empty() {
        let message = format!("rollback diagnostics: {}", rollback_errors.join("; "));
        // An audit failure joins the diagnostics rather than replacing the
        // triggering error; the caller reports the combined reason.
        if let Err(error) = log(inputs.logger, &unit_label(unit), &message) {
            rollback_errors.push(format!("audit log failed: {error}"));
        }
    }
    rollback_errors
}

fn hex_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(ALPHABET[(byte >> 4) as usize] as char);
        out.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    out
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

/// Resumes every activation journal without running both host adapters
/// concurrently and without picking a stack by version ordering.
///
/// Per member the coordinator connects to the recorded endpoint and compares
/// exact state: a unit whose every member reports the complete target record
/// with the recorded handoff commits idempotently; anything else restores the
/// complete recorded old unit (targets shut down over the wire, the verified
/// bridge backup restored and reloaded in every recorded session, then old
/// brokers resumed). Inconsistent identities, handoffs, digests, membership,
/// or unrecognized journal states fail closed with artifacts preserved.
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
        outcomes.push(recover_one(cache_dir, control, reloader, journal, &path, logger).await);
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
    let _ = logger;
    let unit = format!("{:?}", journal.unit);
    // Probe every member endpoint and adopt observed handoffs into Announced
    // journals. Whoever answers — drained old, live old, or claimed target —
    // reports exact lifecycle, record, and handoff for comparison.
    enum Probe {
        Answer { status: ActivationStatus },
        Silent,
    }
    let mut probes = Vec::new();
    for record in &journal.members {
        match control.connect(&record.old_socket).await {
            Ok(mut session) => match session.status().await {
                Ok(status) => probes.push(Probe::Answer { status }),
                Err(_) => probes.push(Probe::Silent),
            },
            Err(_) => probes.push(Probe::Silent),
        }
    }
    // Adopt handoffs for members that lack them.
    let mut adopted = false;
    for (record, probe) in journal.members.iter_mut().zip(probes.iter()) {
        if record.handoff_id.is_none() {
            if let Probe::Answer { status } = probe {
                if let Some(handoff) = status.handoff_id {
                    record.handoff_id = Some(hex_lower(&handoff.0));
                    adopted = true;
                }
            }
        }
    }
    if adopted {
        if journal::write_journal(cache_dir, &journal).is_err() {
            return RecoveryOutcome::Preserved {
                unit,
                reason: "cannot persist adopted handoffs".to_owned(),
            };
        }
    }
    // Commit path: every member answers with the complete target record and
    // the recorded handoff.
    let mut all_committed = true;
    for (record, probe) in journal.members.iter().zip(probes.iter()) {
        match (record.handoff_id.as_ref(), probe) {
            (Some(hex), Probe::Answer { status }) => {
                let expected = match handoff_from_hex(hex) {
                    Ok(handoff) => handoff,
                    Err(_) => {
                        return RecoveryOutcome::Preserved {
                            unit,
                            reason: "journal handoff ID malformed".to_owned(),
                        };
                    }
                };
                if status.current != journal.target_record
                    || status.handoff_id != Some(expected)
                    || status.live_server.discovery_key != record.host_identity
                {
                    all_committed = false;
                    break;
                }
            }
            _ => {
                all_committed = false;
                break;
            }
        }
    }
    if all_committed {
        let mut failures = Vec::new();
        for record in journal.members.iter() {
            let hex = record.handoff_id.clone().unwrap_or_default();
            let Ok(handoff) = handoff_from_hex(&hex) else {
                return RecoveryOutcome::Preserved {
                    unit,
                    reason: "journal handoff ID malformed".to_owned(),
                };
            };
            // Commit is idempotent: targets that self-committed acknowledge.
            match control.connect(&record.old_socket).await {
                Ok(mut session) => {
                    if let Err(error) = session.commit(&handoff).await {
                        failures.push(format!("{}: {error}", record.host_identity));
                    }
                }
                Err(error) => failures.push(format!("{}: {error}", record.host_identity)),
            }
        }
        if failures.is_empty() {
            if let Err(error) = journal::remove_journal(path) {
                return RecoveryOutcome::Preserved {
                    unit,
                    reason: format!(
                        "target state is committed but the journal could not be removed: {error}"
                    ),
                };
            }
            return RecoveryOutcome::Committed { unit };
        }
        return RecoveryOutcome::Preserved {
            unit,
            reason: failures.join("; "),
        };
    }
    // A journal with no durable handoff cannot prove that prepare never began:
    // an interrupted write must remain diagnosable rather than claiming a
    // zero-handoff rollback or deleting the sole transaction record.
    let mut failures = Vec::new();
    let mut contacted_any = false;
    for (record, probe) in journal.members.iter().zip(probes.iter()) {
        if matches!(probe, Probe::Silent) {
            return RecoveryOutcome::Preserved {
                unit,
                reason: format!(
                    "{} is unreachable during recovery; the complete old unit cannot be restored",
                    record.host_identity
                ),
            };
        }
        let Some(hex) = record.handoff_id.as_ref() else {
            // An answered member with no recorded or observed handoff: the
            // unit drained outside any known transaction. Ambiguous.
            return RecoveryOutcome::Preserved {
                unit,
                reason: format!(
                    "{} answers without a known handoff; diagnosis required",
                    record.host_identity
                ),
            };
        };
        let Ok(handoff) = handoff_from_hex(hex) else {
            return RecoveryOutcome::Preserved {
                unit,
                reason: "journal handoff ID malformed".to_owned(),
            };
        };
        // Abort doubles as old-reacquire and target-shutdown: the broker
        // interprets it by role against the recorded handoff.
        match control.connect(&record.old_socket).await {
            Ok(mut session) => {
                contacted_any = true;
                if let Err(error) = session.abort(&handoff).await {
                    failures.push(format!("{}: {error}", record.host_identity));
                }
            }
            Err(error) => failures.push(format!("{}: {error}", record.host_identity)),
        }
    }
    if matches!(journal.unit, UnitKind::Zellij { .. }) {
        // Restore the one old bridge across all switched sessions when the
        // stable bytes are the staged ones; anything else is ambiguity.
        // The stable path is derived from the backup location inside
        // `restore_recorded_bridge`, never from the cache directory.
        if let Err(error) = restore_recorded_bridge(cache_dir, &journal, reloader).await {
            failures.push(error);
        }
    }
    if failures.is_empty() && contacted_any {
        if let Err(error) = journal::remove_journal(path) {
            return RecoveryOutcome::Preserved {
                unit,
                reason: format!(
                    "old unit was restored but the journal could not be removed: {error}"
                ),
            };
        }
        RecoveryOutcome::RolledBack {
            unit,
            reason: "incomplete targets; complete old unit restored".to_owned(),
        }
    } else if failures.is_empty() {
        // No member could even be contacted: nothing was restored and nothing
        // was verified. Preserve the journal for diagnosis or restart.
        RecoveryOutcome::Preserved {
            unit,
            reason: "no member reachable and no target recorded; operator restart required"
                .to_owned(),
        }
    } else {
        RecoveryOutcome::Preserved {
            unit,
            reason: failures.join("; "),
        }
    }
}

/// Restores the recorded old bridge when the stable bytes are exactly the
/// staged ones, then reloads every recorded session. Any other on-disk state
/// is ambiguity, reported as an error with artifacts preserved.
async fn restore_recorded_bridge<R>(
    cache_dir: &Path,
    journal: &ActivationJournal,
    reloader: &R,
) -> Result<(), String>
where
    R: HostReloader,
{
    let (stable, backup, staged_digest, old_digest) = match journal.unit.clone() {
        UnitKind::Zellij { .. } => {
            let backup = journal
                .backup_path
                .clone()
                .ok_or_else(|| "no recorded bridge backup; diagnosis required".to_owned())?;
            let staged = journal
                .staged_bridge_digest
                .clone()
                .ok_or_else(|| "no recorded staged digest; diagnosis required".to_owned())?;
            let old = journal
                .old_bridge_digest
                .clone()
                .ok_or_else(|| "no recorded old digest; diagnosis required".to_owned())?;
            // The stable path lives under the integration directory, not the
            // cache directory; recover it from the backup's parent.
            let stable = backup
                .parent()
                .map(|parent| parent.join(integration::BRIDGE_FILE_NAME))
                .ok_or_else(|| "recorded backup has no parent".to_owned())?;
            (stable, backup, staged, old)
        }
        UnitKind::Herdr { .. } => return Ok(()),
    };
    let _ = cache_dir;
    let current =
        std::fs::read(&stable).map_err(|error| format!("cannot read stable bridge: {error}"))?;
    if fsutil::sha256_hex(&current) != staged_digest {
        return Err("stable bridge is neither staged nor old; diagnosis required".to_owned());
    }
    std::fs::rename(&backup, &stable)
        .map_err(|error| format!("cannot restore old bridge: {error}"))?;
    let _ = fsutil::sync_dir_of(&stable);
    let restored = std::fs::read(&stable)
        .map_err(|error| format!("cannot verify restored bridge: {error}"))?;
    if fsutil::sha256_hex(&restored) != old_digest {
        return Err("restored bridge digest mismatch; diagnosis required".to_owned());
    }
    let bridge_url = integration::kdl::bridge_url(&stable);
    for record in &journal.members {
        reloader
            .reload_bridge(&record.host_identity, &bridge_url)
            .map_err(|error| format!("rollback reload {}: {error}", record.host_identity))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_protocol::control::{ControlDecoder, ControlPolicy};
    use muxe_protocol::wire::{HostKind, LiveServerIdentity, ServerId};
    use std::sync::{Arc, Mutex};
    use tokio::{net::UnixListener, task::JoinHandle};

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
        handoff_byte: u8,
        fail_prepare: bool,
    }

    struct TargetScript {
        target: CompatibilityRecord,
        handoff_byte: u8,
        /// When true the target never binds: it stays absent.
        absent: bool,
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

    async fn serve_old(
        socket: PathBuf,
        script: BrokerScript,
        discovery: String,
        events: Arc<Mutex<Vec<String>>>,
    ) {
        let mut listener_slot = Some(UnixListener::bind(&socket).expect("bind old listener"));
        let mut prepared_handoff: Option<HandoffId> = None;
        // Accept connections one at a time: short-lived probes and fast-path
        // checks are each served to EOF, so they never steal the retained
        // drain stream. Draining drops the listener and unlinks the path;
        // later connections on the path belong to the claimed target.
        loop {
            let mut stream = match listener_slot.as_ref() {
                Some(listener) => match listener.accept().await {
                    Ok((stream, _)) => stream,
                    Err(_) => break,
                },
                None => break,
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
                        ControlOperation::Status => ControlResult::Status(status_of(
                            &script.current,
                            None,
                            &discovery,
                            LifecycleState::Running,
                        )),
                        ControlOperation::Prepare { target } => {
                            assert_eq!(target.muxe_version, "0.2.0");
                            if script.fail_prepare {
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("prepare-refused".to_owned());
                                ControlResult::Error {
                                    diagnostic: "prepare refused: non-cancellable work".to_owned(),
                                }
                            } else {
                                let handoff = handoff(script.handoff_byte);
                                prepared_handoff = Some(handoff);
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("prepared".to_owned());
                                // Drain: close and unlink the listener while
                                // keeping this accepted stream open.
                                drop(listener_slot.take());
                                let _ = std::fs::remove_file(&socket);
                                ControlResult::Prepared(status_of(
                                    &script.current,
                                    Some(handoff),
                                    &discovery,
                                    LifecycleState::Draining,
                                ))
                            }
                        }
                        ControlOperation::Commit { handoff_id } => {
                            if Some(handoff_id) == prepared_handoff {
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("old-committed".to_owned());
                                ControlResult::Committed(status_of(
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
                                events
                                    .lock()
                                    .expect("fixture events are not poisoned")
                                    .push("old-aborted".to_owned());
                                ControlResult::Aborted(status_of(
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
                        ControlOperation::Retire => ControlResult::Retired(status_of(
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
                    use tokio::io::AsyncWriteExt;
                    // Polling coordinators may drop between status and read; a
                    // dead stream ends this connection, never the task.
                    if stream
                        .write_all(&(payload.len() as u32).to_be_bytes())
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
            live_server: identity(discovery),
            current: current.clone(),
            target: None,
            handoff_id: handoff,
            ready: None,
        }
    }

    async fn serve_target(path: PathBuf, script: TargetScript, discovery: String) {
        if script.absent {
            return;
        }
        // Wait for the old broker to unlink, then claim the normal endpoint.
        for _ in 0..200 {
            if !path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let listener = UnixListener::bind(&path).expect("target claims endpoint");
        loop {
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
                    .expect("decode target frame");
                for request in requests {
                    let handoff = handoff(script.handoff_byte);
                    let result = match request.operation {
                        ControlOperation::Status => ControlResult::Status(status_of(
                            &script.target,
                            Some(handoff),
                            &discovery,
                            LifecycleState::Running,
                        )),
                        ControlOperation::Commit { handoff_id } if handoff_id == handoff => {
                            ControlResult::Committed(status_of(
                                &script.target,
                                Some(handoff),
                                &discovery,
                                LifecycleState::Running,
                            ))
                        }
                        ControlOperation::Abort { .. } => {
                            // Target shutdown: acknowledge then exit the loop.
                            let response = ControlMessage::Response(ControlResponse {
                                request_id: request.request_id,
                                result: ControlResult::Aborted(status_of(
                                    &script.target,
                                    None,
                                    &discovery,
                                    LifecycleState::Retired,
                                )),
                            });
                            let payload = serde_json::to_vec(&response).unwrap();
                            use tokio::io::AsyncWriteExt;
                            let _ = stream
                                .write_all(&(payload.len() as u32).to_be_bytes())
                                .await;
                            let _ = stream.write_all(&payload).await;
                            let _ = stream.flush().await;
                            return;
                        }
                        _ => ControlResult::Error {
                            diagnostic: "unexpected target op".to_owned(),
                        },
                    };
                    let response = ControlMessage::Response(ControlResponse {
                        request_id: request.request_id,
                        result,
                    });
                    let payload = serde_json::to_vec(&response).unwrap();
                    use tokio::io::AsyncWriteExt;
                    if stream
                        .write_all(&(payload.len() as u32).to_be_bytes())
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

    use muxe_protocol::control::{
        ControlMessage, ControlOperation, ControlResponse, ControlResult,
    };

    struct Fixture {
        _temp: tempfile::TempDir,
        cache: PathBuf,
        config: PathBuf,
        control: LiveControl,
        spawner: ProcessSpawner,
        reloader: FixtureReloader,
        preflight: FixturePreflight,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone, Default)]
    struct FixtureReloader {
        fail_sessions: Vec<String>,
        reloaded: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl HostReloader for FixtureReloader {
        fn reload_bridge(&self, session: &str, bridge_url: &str) -> Result<(), ActivateError> {
            self.reloaded
                .lock()
                .expect("fixture reloads are not poisoned")
                .push((session.to_owned(), bridge_url.to_owned()));
            if self.fail_sessions.iter().any(|entry| entry == session) {
                return Err(ActivateError::Reload {
                    session: session.to_owned(),
                    detail: "reload refused".to_owned(),
                });
            }
            Ok(())
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
        async fn revalidate_herdr_actions(&self, _discovery_key: &str) -> Result<(), String> {
            Ok(())
        }
        async fn check_host_version(
            &self,
            _host_kind: &str,
            _discovery_key: &str,
        ) -> Result<(), String> {
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
            Self {
                cache,
                config,
                _temp: temp,
                control: LiveControl,
                spawner: ProcessSpawner,
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
                    bridge_path: None,
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

        fn spawn_target_task(
            &self,
            socket: PathBuf,
            script: TargetScript,
            discovery: &str,
        ) -> JoinHandle<()> {
            tokio::spawn(serve_target(socket, script, discovery.to_owned()))
        }

        fn herdr_inputs(
            &self,
        ) -> ActivateInputs<'_, LiveControl, ProcessSpawner, FixtureReloader, FixturePreflight>
        {
            ActivateInputs {
                config_dir: &self.config,
                cache_dir: &self.cache,
                target: target_record(),
                staged_bridge: None,
                spawn_argv: &|_member| {
                    Ok((PathBuf::from("/bin/sleep"), vec![OsString::from("30")]))
                },
                scope: HostScope::Herdr,
                current: None,
                control: &self.control,
                spawner: &self.spawner,
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
            handoff_byte: 0x11,
            fail_prepare: false,
        }
    }

    #[tokio::test]
    async fn herdr_commit_through_retained_sessions() {
        let fixture = Fixture::new();
        let (socket, old) = fixture.old_broker("server", herdr_script()).await;
        // The target claims the normal endpoint after prepare unlinks it.
        let target = fixture.spawn_target_task(
            socket.clone(),
            TargetScript {
                target: target_record(),
                handoff_byte: 0x11,
                absent: false,
            },
            "server",
        );
        let report = activate(fixture.herdr_inputs()).await.unwrap();
        assert_eq!(
            report.units,
            vec![UnitOutcome::Committed {
                unit: "herdr:server".to_owned()
            }]
        );
        assert!(journal::list_journals(&fixture.cache).unwrap().is_empty());
        let events = fixture
            .events
            .lock()
            .expect("fixture events are not poisoned");
        assert!(events.contains(&"prepared".to_owned()));
        assert!(events.contains(&"old-committed".to_owned()));
        old.abort();
        target.abort();
    }

    #[tokio::test]
    async fn prepare_refusal_rolls_back_without_mutation() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture
            .old_broker(
                "server",
                BrokerScript {
                    fail_prepare: true,
                    ..herdr_script()
                },
            )
            .await;
        let report = activate(fixture.herdr_inputs()).await.unwrap();
        assert!(matches!(report.units[0], UnitOutcome::RolledBack { .. }));
        // The old broker refused prepare and was never drained, so there is
        // nothing to abort and no target was spawned.
        let events = fixture
            .events
            .lock()
            .expect("fixture events are not poisoned");
        assert!(events.contains(&"prepare-refused".to_owned()));
        assert!(!events.contains(&"old-aborted".to_owned()));
        old.abort();
    }

    #[tokio::test]
    async fn absent_target_restores_old_stack() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture.old_broker("server", herdr_script()).await;
        // No target task is spawned by the test, but the spawner still runs
        // real process mechanics (/bin/sleep child, killed on abort).
        let report = activate(fixture.herdr_inputs()).await.unwrap();
        assert!(matches!(report.units[0], UnitOutcome::RolledBack { .. }));
        let events = fixture
            .events
            .lock()
            .expect("fixture events are not poisoned");
        assert!(events.contains(&"old-aborted".to_owned()));
        old.abort();
    }

    #[tokio::test]
    async fn coordinator_death_preserves_unverifiable_journal() {
        let fixture = Fixture::new();
        let (socket, old) = fixture.old_broker("server", herdr_script()).await;
        // Drive prepare manually, then drop every coordinator session: the
        // coordinator died after drain with no target ever claiming.
        let handoff = {
            let mut session = fixture.control.connect(&socket).await.unwrap();
            session.status().await.unwrap();
            let target = target_record();
            let prepared = session.prepare(target).await.unwrap();
            prepared.handoff_id.unwrap()
        };
        let _ = handoff;
        // Journal written as the coordinator would have: Announced is adopted
        // on probe; write the Prepared form directly here.
        let mut journal = ActivationJournal::new(
            UnitKind::Herdr {
                host_hash: unit_hash("server"),
            },
            old_record(),
            target_record(),
            vec![MemberState {
                host_identity: "server".to_owned(),
                old_socket: socket.clone(),
                target_socket: None,
                handoff_id: Some(hex_lower(&handoff.0)),
                state: MemberTransition::Prepared,
            }],
        );
        journal.state = JournalState::Prepared;
        journal::write_journal(&fixture.cache, &journal).unwrap();
        let outcomes = recover(&fixture.cache, &fixture.control, &fixture.reloader, None)
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        // Nothing is reachable: the old unlinked its listener at prepare and
        // no target ever claimed the endpoint. Recovery preserves the journal
        // for diagnosis or restart instead of claiming a rollback it cannot
        // verify; the drained old restores itself on coordinator-stream loss.
        assert!(
            matches!(outcomes[0], RecoveryOutcome::Preserved { .. }),
            "{:?}",
            outcomes[0]
        );
        old.abort();
    }

    #[tokio::test]
    async fn announced_journal_without_handoff_is_preserved_for_diagnosis() {
        let fixture = Fixture::new();
        let (socket, old) = fixture.old_broker("server", herdr_script()).await;
        // Crash between the Announced write and prepare: the old still runs.
        let journal = ActivationJournal::new(
            UnitKind::Herdr {
                host_hash: unit_hash("server"),
            },
            target_record(),
            target_record(),
            vec![MemberState {
                host_identity: "server".to_owned(),
                old_socket: socket,
                target_socket: None,
                handoff_id: None,
                state: MemberTransition::Prepared,
            }],
        );
        let mut journal = journal;
        journal.state = JournalState::Announced;
        journal::write_journal(&fixture.cache, &journal).unwrap();
        let outcomes = recover(&fixture.cache, &fixture.control, &fixture.reloader, None)
            .await
            .unwrap();
        assert!(
            matches!(outcomes[0], RecoveryOutcome::Preserved { .. }),
            "{:?}",
            outcomes[0]
        );
        assert_eq!(journal::list_journals(&fixture.cache).unwrap().len(), 1);
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
        std::fs::write(
            &program,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nif [ \"$2\" = \"bad\" ]; then exit 3; fi\nexit 0\n",
                recorded.display()
            ),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        let reloader = ZellijCliReloader {
            program: Some(program.clone()),
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

    #[tokio::test]
    async fn preflight_failure_changes_nothing() {
        let fixture = Fixture::new();
        let (_socket, old) = fixture.old_broker("server", herdr_script()).await;
        let mut inputs = fixture.herdr_inputs();
        let preflight = FixturePreflight {
            fail_config: Some("configuration does not parse".to_owned()),
        };
        inputs.preflight = &preflight;
        let result = activate(inputs).await;
        assert!(matches!(result, Err(ActivateError::Preflight(_))));
        assert!(journal::list_journals(&fixture.cache).unwrap().is_empty());
        old.abort();
    }

    fn ready_census(registered: &[&str], members: Option<&[&str]>) -> TargetReadiness {
        TargetReadiness {
            registered_clients: registered.iter().map(ToString::to_string).collect(),
            member_clients: members.map_or(0, <[_]>::len) as u64,
            member_ids: members.map(|set| set.iter().map(ToString::to_string).collect()),
        }
    }

    fn census_member(socket: PathBuf, host_kind: &str, discovery: &str) -> BrokerEntry {
        BrokerEntry {
            host_kind: host_kind.to_owned(),
            discovery_key: discovery.to_owned(),
            socket,
            server_pid: 1,
            started_at: 1,
            bridge_path: None,
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
            live_server: identity(discovery),
            current: target_record(),
            target: None,
            handoff_id: Some(handoff),
            ready,
        }
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
        let herdr = census_member(socket.clone(), "herdr", "herdr.sock");
        assert!(target_ready(
            &census_status(expected, "herdr.sock", None),
            &herdr,
            &expected,
            &target_record(),
        ));
        let zellij = census_member(socket, "zellij", "session-a");
        assert!(!target_ready(
            &census_status(expected, "session-a", None),
            &zellij,
            &expected,
            &target_record(),
        ));
        assert!(!target_ready(
            &census_status(
                expected,
                "session-a",
                Some(ready_census(&["a"], Some(&["a", "b"])))
            ),
            &zellij,
            &expected,
            &target_record(),
        ));
        assert!(target_ready(
            &census_status(
                expected,
                "session-a",
                Some(ready_census(&["a", "b"], Some(&["a", "b"])))
            ),
            &zellij,
            &expected,
            &target_record(),
        ));
        assert!(!target_ready(
            &census_status(
                expected,
                "session-a",
                Some(ready_census(&["a", "b"], Some(&["a", "b"])))
            ),
            &zellij,
            &handoff(3),
            &target_record(),
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
                                    pending.push(request.request_id);
                                }
                            })
                            .expect("decode readiness frame");
                        for request_id in pending {
                            let response = ControlMessage::Response(ControlResponse {
                                request_id,
                                result: ControlResult::Status(status.clone()),
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
        serve_readiness_status(
            partial.clone(),
            census_status(
                handoff,
                "session-a",
                Some(ready_census(&["a"], Some(&["a", "b"]))),
            ),
        );
        let member = census_member(partial, "zellij", "session-a");
        let blocked = wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
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
        serve_readiness_status(
            full.clone(),
            census_status(
                handoff,
                "session-a",
                Some(ready_census(&["a", "b"], Some(&["a", "b"]))),
            ),
        );
        let member = census_member(full, "zellij", "session-a");
        wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await
        .expect("a full fresh census reads ready");
    }

    #[tokio::test]
    async fn wait_ready_opens_only_on_real_regs_with_empty_valid() {
        let temp = tempfile::tempdir().expect("rounds sockets");
        let handoff = handoff(11);
        let evolving = temp.path().join("evolving.sock");
        let empty_round =
            census_status(handoff, "session-a", Some(ready_census(&[], Some(&["a"]))));
        let full_round = census_status(
            handoff,
            "session-a",
            Some(ready_census(&["a"], Some(&["a"]))),
        );
        serve_readiness_rounds(evolving.clone(), vec![empty_round, full_round]);
        let member = census_member(evolving, "zellij", "session-a");
        wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await
        .expect("real registrations open a fresh target");
        let vacant = temp.path().join("vacant.sock");
        serve_readiness_status(
            vacant.clone(),
            census_status(handoff, "session-a", Some(ready_census(&[], Some(&[])))),
        );
        let member = census_member(vacant, "zellij", "session-a");
        wait_ready(
            &LiveControl,
            &member,
            &handoff,
            &target_record(),
            Instant::now() + Duration::from_secs(5),
            Duration::from_millis(10),
        )
        .await
        .expect("a genuinely queried empty snapshot reads ready");
    }
}
