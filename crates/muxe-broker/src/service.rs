use std::{
    collections::HashSet,
    fmt::Write as _,
    fs,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};

use crate::{
    Broker, BrokerError, PendingAttachment, RequestResult, RuntimeEndpoint, RuntimeError,
    StartupLock,
};
use muxe_adapter_api::AdapterErrorKind;
use muxe_protocol::{
    ArchivedFrame, BrokerResponse, ClientRequest, ConnectionDecoder, ConnectionPolicy, DecodeError,
    PeerRole, PendingLaunchToken, Prelude, RequestId, SchemaFingerprint, UiSessionId, WireMessage,
    control::{
        ActivationPhase, ActivationStatus, BridgeUnitId, BrokerRegistrationProof,
        CompatibilityRecord, ControlDecoder, ControlMessage, ControlOperation, ControlPolicy,
        ControlRequestId, ControlResponse, ControlResult, HandoffId, LifecycleState,
        PrepareHandoffProtocol,
    },
    encode_frame,
};
use nix::unistd::Uid;
use notify::{RecursiveMode, Watcher};
#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

fn control_error(diagnostic: String) -> ControlResult {
    ControlResult::Error {
        diagnostic: bounded_control_diagnostic(diagnostic),
        prepare_refusal: None,
    }
}

fn bounded_control_diagnostic(mut diagnostic: String) -> String {
    if diagnostic.chars().any(char::is_control) {
        diagnostic = diagnostic
            .chars()
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect();
    }
    muxe_protocol::truncate_utf8(&mut diagnostic, muxe_protocol::MAX_DIAGNOSTIC_LEN);
    diagnostic
}

/// Activation state supplied by the executable that owns this broker process.
///
/// An old broker starts [`ActivationBootstrap::Running`]. A target broker cannot bind until the
/// executable has read the durable journal and supplied its exact host identity and nonzero
/// handoff. Targets remain attachment-gated until the coordinator commits that handoff.
#[derive(Clone, Debug)]
pub enum ActivationBootstrap {
    Running {
        current: CompatibilityRecord,
        bridge_unit: Option<BridgeUnitId>,
    },
    Target {
        current: CompatibilityRecord,
        handoff: HandoffId,
        live_server: muxe_protocol::LiveServerIdentity,
        bridge_unit: Option<BridgeUnitId>,
    },
}

#[derive(Clone)]
struct ActivationController {
    current: CompatibilityRecord,
    expected_live_server: muxe_protocol::LiveServerIdentity,
    bridge_unit: Option<BridgeUnitId>,
    registration: Arc<OnceLock<BrokerRegistrationProof>>,
    state: Arc<Mutex<ActivationState>>,
    commands: Arc<Mutex<Option<mpsc::Sender<ServerCommand>>>>,
    // Serializes whole coordinator transitions (Prepare/Commit/Abort/Retire) so two
    // control connections cannot interleave drain/suspend/listener steps. The server
    // command loop never takes it; control handlers never send commands while holding it
    // beyond the channel send, so it cannot deadlock the listener loop.
    transition: Arc<Mutex<()>>,
    // Live coordinator control connections. Disconnect recovery runs only when the
    // last one closes; per-operation reconnects therefore never trigger recovery
    // while the coordinator is still driving.
    connections: Arc<Mutex<usize>>,
    // Owner-side journal mapping for disconnect recovery. None until the executable
    // installs one after startup; without it an absent journal is assumed.
    recovery: Arc<Mutex<Option<Arc<dyn RecoveryJournal>>>>,
    // Exact in-process Stop result. A retry may reuse proof but never sends a
    // second Stop to the exited listener loop. Durable replay uses the journal
    // receipt instead of this memory.
    old_retirement: Arc<Mutex<Option<OldRetirementProof>>>,
}

#[derive(Clone)]
struct OldRetirementProof {
    handoff: HandoffId,
    outcome: Result<(), String>,
}

/// Completion acknowledgement emitted only after the broker has completed its
/// local retirement or resume barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryAck {
    Resumed,
    TargetRetired,
    OldCommitted,
    TargetCommitted,
}

pub trait RecoveryPermit: Send + Sync {
    fn acknowledge<'a>(
        &'a self,
        handoff: &'a HandoffId,
        ack: RecoveryAck,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

#[derive(Clone)]
pub enum RecoveryDecision {
    NoJournal,
    Preserve {
        reason: String,
    },
    TargetOwns {
        recover_after: Duration,
        permit: Option<Arc<dyn RecoveryPermit>>,
    },
    RestoreOld {
        recover_after: Duration,
        permit: Option<Arc<dyn RecoveryPermit>>,
    },
    Committed {
        permit: Option<Arc<dyn RecoveryPermit>>,
    },
    /// Terminal transaction authority was validated and cleanup converged.
    /// The broker must perform no lifecycle transition or acknowledgement.
    CleanupComplete,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum RecoveryJournalStatus {
    Absent,
    Present,
    Inconsistent,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
enum RecoveryMemberStatus {
    Pending,
    Ready,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct RecoveryView {
    journal: RecoveryJournalStatus,
    member: RecoveryMemberStatus,
    target_live: bool,
    recover_after: Duration,
}

#[cfg(test)]
impl RecoveryView {
    fn absent() -> Self {
        Self {
            journal: RecoveryJournalStatus::Absent,
            member: RecoveryMemberStatus::Pending,
            target_live: false,
            recover_after: Duration::ZERO,
        }
    }
}

pub trait RecoveryJournal: Send + Sync {
    /// Performs owner-side journal validation, per-member probing, and any
    /// required artifact decision without holding broker transition locks.
    fn recovery_decision<'a>(
        &'a self,
        handoff: &'a HandoffId,
        local_status: &'a ActivationStatus,
    ) -> Pin<Box<dyn Future<Output = RecoveryDecision> + Send + 'a>>;
    /// Read-only exact-incarnation check for a normal target Commit. Unlike
    /// disconnect recovery, this must not acquire the coordinator's unit lock:
    /// the coordinator holds it while sending Commit.
    fn authorize_target_commit<'a>(
        &'a self,
        handoff: &'a HandoffId,
        local_status: &'a ActivationStatus,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
    /// Read-only exact old-incarnation and durable `CommitIntent` preflight.
    /// The coordinator can hold the unit lock while issuing normal Commit.
    fn authorize_old_commit<'a>(
        &'a self,
        handoff: &'a HandoffId,
        local_status: &'a ActivationStatus,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
    /// Persists exact post-stop old-broker proof before a wire or journal ACK.
    /// Called only while the completed stop barrier's ticket is held.
    fn publish_old_retirement<'a>(
        &'a self,
        handoff: &'a HandoffId,
        local_status: &'a ActivationStatus,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

#[derive(Clone, Debug)]
enum ActivationState {
    Running,
    Draining {
        target: Box<CompatibilityRecord>,
        handoff: HandoffId,
        // True once the old host subscription is suspended (and false again once a
        // failed Prepare restored it). Abort resumes the adapter only when set; the
        // listener rebind always runs. Without this Abort would demand a resume from
        // a live adapter, which correctly refuses to fabricate one.
        host_suspended: bool,
    },
    SupervisorOnly {
        handoff: HandoffId,
    },
    TargetGated {
        handoff: HandoffId,
    },
    TargetCommitted {
        handoff: HandoffId,
    },
    Retired,
}
enum PrepareAdmission {
    Begin,
    Idempotent,
    Reject(String),
}

enum ServerCommand {
    Drain {
        complete: oneshot::Sender<Result<(), String>>,
    },
    Resume {
        complete: oneshot::Sender<Result<(), String>>,
    },
    Stop {
        complete: oneshot::Sender<Result<RetirementTicket, String>>,
    },
}

/// Two-phase retirement ownership. The run loop finishes resource shutdown and
/// unlinks its endpoint before handing this ticket to the RPC/recovery owner.
/// Dropping the ticket is the explicit permission for the supervisor loop to
/// finish and let the process exit.
struct RetirementTicket {
    release: Option<oneshot::Sender<()>>,
}

impl RetirementTicket {
    fn pair() -> (Self, oneshot::Receiver<()>) {
        let (release, released) = oneshot::channel();
        (
            Self {
                release: Some(release),
            },
            released,
        )
    }
}

impl Drop for RetirementTicket {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
    }
}

impl ActivationController {
    async fn start(
        broker: &Broker,
        bootstrap: ActivationBootstrap,
    ) -> Result<Arc<Self>, ServerError> {
        let (current, expected_live_server, bridge_unit, state) = match bootstrap {
            ActivationBootstrap::Running {
                current,
                bridge_unit,
            } => (
                current,
                broker.live_identity().await?,
                bridge_unit,
                ActivationState::Running,
            ),
            ActivationBootstrap::Target {
                current,
                handoff,
                live_server,
                bridge_unit,
            } => {
                // Before the bridge swap, the target has no covered identity.
                // Its caller reserved this candidate on the adapter; Status
                // stays gated until the adapter proves that exact identity.
                match broker.live_identity().await {
                    Ok(actual) if actual != live_server => {
                        return Err(ServerError::Activation(format!(
                            "replacement broker cannot start because the host identity changed: expected {:?} at {} with server ID {}, received {:?} at {} with server ID {}",
                            live_server.host,
                            live_server.discovery_key,
                            live_server.server_id.as_str(),
                            actual.host,
                            actual.discovery_key,
                            actual.server_id.as_str()
                        )));
                    }
                    Err(BrokerError::Adapter(error))
                        if error.kind == AdapterErrorKind::Unavailable => {}
                    Err(error) => return Err(error.into()),
                    Ok(_) => {}
                }
                (
                    current,
                    live_server,
                    bridge_unit,
                    ActivationState::TargetGated { handoff },
                )
            }
        };
        Ok(Arc::new(Self {
            current,
            expected_live_server,
            bridge_unit,
            registration: Arc::new(OnceLock::new()),
            state: Arc::new(Mutex::new(state)),
            commands: Arc::new(Mutex::new(None)),
            transition: Arc::new(Mutex::new(())),
            connections: Arc::new(Mutex::new(0)),
            recovery: Arc::new(Mutex::new(None)),
            old_retirement: Arc::new(Mutex::new(None)),
        }))
    }

    /// Installs the owner-side journal mapping. Called once by the executable after
    /// startup; disconnect recovery assumes an absent journal until then.
    async fn set_recovery(&self, recovery: Arc<dyn RecoveryJournal>) {
        *self.recovery.lock().await = Some(recovery);
    }

    async fn install_commands(&self, commands: mpsc::Sender<ServerCommand>) {
        *self.commands.lock().await = Some(commands);
    }

    async fn allows_ui(&self) -> bool {
        matches!(
            *self.state.lock().await,
            ActivationState::Running | ActivationState::TargetCommitted { .. }
        )
    }
    async fn status(&self, broker: &Broker) -> Result<ActivationStatus, String> {
        self.status_with(broker, None).await
    }

    async fn status_with(
        &self,
        broker: &Broker,
        proof: Option<(muxe_protocol::AsOfTick, muxe_protocol::UnitReadinessEpochId)>,
    ) -> Result<ActivationStatus, String> {
        let state = self.state.lock().await.clone();
        // Draining retains the previously proven identity while the adapter
        // is suspended. A gated target may report only its reserved candidate
        // before coverage; readiness remains absent until the host proves it.
        // All other live states compare the current covered identity.
        let (live_server, uncovered_target) = match state {
            ActivationState::Draining { .. } | ActivationState::SupervisorOnly { .. } => {
                (self.expected_live_server.clone(), false)
            }
            ActivationState::TargetGated { .. } => match broker.live_identity().await {
                Err(BrokerError::Adapter(error)) if error.kind == AdapterErrorKind::Unavailable => {
                    (self.expected_live_server.clone(), true)
                }
                Err(error) => return Err(error.to_string()),
                Ok(actual) if actual == self.expected_live_server => (actual, false),
                Ok(actual) => {
                    return Err(self.identity_mismatch_diagnostic(&actual));
                }
            },
            _ => {
                let actual = broker
                    .live_identity()
                    .await
                    .map_err(|error| error.to_string())?;
                if actual != self.expected_live_server {
                    return Err(self.identity_mismatch_diagnostic(&actual));
                }
                (actual, false)
            }
        };
        let (lifecycle, phase, target, handoff_id, suspended) = match state {
            ActivationState::Running => (
                LifecycleState::Running,
                ActivationPhase::Ordinary,
                None,
                None,
                false,
            ),
            ActivationState::Draining {
                target, handoff, ..
            } => (
                LifecycleState::Draining,
                ActivationPhase::Draining,
                Some(*target),
                Some(handoff),
                true,
            ),
            ActivationState::SupervisorOnly { handoff } => (
                LifecycleState::SupervisorOnly,
                ActivationPhase::SupervisorOnly,
                None,
                Some(handoff),
                true,
            ),
            ActivationState::TargetGated { handoff } => (
                LifecycleState::Running,
                ActivationPhase::TargetGated,
                None,
                Some(handoff),
                false,
            ),
            ActivationState::TargetCommitted { handoff } => (
                LifecycleState::Running,
                ActivationPhase::TargetCommitted,
                None,
                Some(handoff),
                false,
            ),
            ActivationState::Retired => (
                LifecycleState::Retired,
                ActivationPhase::Retired,
                None,
                None,
                false,
            ),
        };
        Ok(ActivationStatus {
            lifecycle,
            phase,
            live_server,
            current: self.current.clone(),
            target,
            handoff_id,
            prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
            registration: self.registration.get().copied(),
            bridge_unit: self.bridge_unit,
            // Per-client bridge evidence comes from the live adapter only.
            // Suspended brokers and uncovered targets never report Ready.
            // A failed query also serves None so Status stays observable.
            ready: if suspended || uncovered_target {
                None
            } else if let Some((as_of, epoch)) = proof {
                broker.activation_readiness_at(as_of, epoch).await
            } else {
                broker.activation_readiness().await
            },
        })
    }

    fn identity_mismatch_diagnostic(&self, actual: &muxe_protocol::LiveServerIdentity) -> String {
        format!(
            "cannot confirm activation status because the host identity changed: expected {:?} at {} with server ID {}, received {:?} at {} with server ID {}",
            self.expected_live_server.host,
            self.expected_live_server.discovery_key,
            self.expected_live_server.server_id.as_str(),
            actual.host,
            actual.discovery_key,
            actual.server_id.as_str()
        )
    }

    async fn drain_listener(&self) -> Result<(), String> {
        let (complete, result) = oneshot::channel();
        let sender = self.command_sender().await?;
        sender
            .send(ServerCommand::Drain { complete })
            .await
            .map_err(|_| {
                "broker activation service stopped before accepting the request to stop listening"
                    .to_owned()
            })?;
        result
            .await
            .map_err(|_| "broker activation service stopped before confirming that it stopped listening; the listener state is unconfirmed".to_owned())?
    }

    async fn resume_listener(&self) -> Result<(), String> {
        let (complete, result) = oneshot::channel();
        let sender = self.command_sender().await?;
        sender
            .send(ServerCommand::Resume { complete })
            .await
            .map_err(|_| {
                "broker activation service stopped before accepting the request to resume listening"
                    .to_owned()
            })?;
        result
            .await
            .map_err(|_| "broker activation service stopped before confirming that it resumed listening; the listener state is unconfirmed".to_owned())?
    }

    async fn stop_listener(&self) -> Result<RetirementTicket, String> {
        let (complete, result) = oneshot::channel();
        self.command_sender()
            .await?
            .send(ServerCommand::Stop { complete })
            .await
            .map_err(|_| "broker activation service stopped before accepting the request to shut down the host connection and listener".to_owned())?;
        result
            .await
            .map_err(|_| "broker activation service stopped before confirming shutdown of the host connection and listener; shutdown is unconfirmed".to_owned())?
    }

    async fn command_sender(&self) -> Result<mpsc::Sender<ServerCommand>, String> {
        self.commands.lock().await.clone().ok_or_else(|| {
            "broker activation service is not running and cannot accept listener changes".to_owned()
        })
    }

    async fn handle(&self, broker: &Broker, operation: ControlOperation) -> (ControlResult, bool) {
        // One coordinator transition at a time: concurrent control connections share this
        // controller, and Prepare/Abort interleave drain, suspend, and listener steps.
        let _transition = self.transition.lock().await;
        match operation {
            ControlOperation::Status => (
                self.status_result(broker, ControlResult::Status).await,
                false,
            ),
            ControlOperation::StatusAt {
                handoff_id,
                epoch,
                as_of,
            } => {
                if !matches!(
                    *self.state.lock().await,
                    ActivationState::TargetGated { handoff } if handoff == handoff_id
                ) || self.bridge_unit.is_none()
                {
                    return (
                        control_error(
                            "cannot check client registrations at the requested time: this broker must be the uncommitted Zellij replacement for the requested handoff ID"
                                .to_owned(),
                        ),
                        false,
                    );
                }
                (
                    match self.status_with(broker, Some((as_of, epoch))).await {
                        Ok(status) => ControlResult::StatusAt(status),
                        Err(diagnostic) => control_error(diagnostic),
                    },
                    false,
                )
            }
            ControlOperation::Prepare { target, handoff_id } => {
                self.handle_prepare(broker, *target, handoff_id).await
            }
            ControlOperation::Commit { handoff_id } => self.handle_commit(broker, handoff_id).await,
            ControlOperation::Abort { handoff_id } => self.handle_abort(broker, handoff_id).await,
            ControlOperation::Retire => {
                {
                    let mut state = self.state.lock().await;
                    if !matches!(
                        *state,
                        ActivationState::Running
                            | ActivationState::TargetCommitted { .. }
                            | ActivationState::Retired
                    ) {
                        return (
                            control_error(
                                "cannot stop the broker while its replacement activation is in progress".to_owned(),
                            ),
                            false,
                        );
                    }
                    *state = ActivationState::Retired;
                }
                if let Err(error) = broker.drain_for_activation().await {
                    return (control_error(error.to_string()), false);
                }
                (
                    self.status_result(broker, ControlResult::Retired).await,
                    true,
                )
            }
        }
    }

    async fn handle_commit(&self, broker: &Broker, handoff_id: HandoffId) -> (ControlResult, bool) {
        let target_role = matches!(
            *self.state.lock().await,
            ActivationState::TargetGated { handoff }
                | ActivationState::TargetCommitted { handoff }
                if handoff == handoff_id
        );
        if target_role {
            let status = match self.status(broker).await {
                Ok(status) => status,
                Err(diagnostic) => {
                    return (control_error(diagnostic), false);
                }
            };
            let authorization = match self.recovery.lock().await.clone() {
                Some(recovery) => recovery.authorize_target_commit(&handoff_id, &status).await,
                None => Err("cannot commit the replacement broker: no saved activation record is available to authorize it".to_owned()),
            };
            if let Err(diagnostic) = authorization {
                return (control_error(diagnostic), false);
            }
        }
        let state = self.state.lock().await.clone();
        match state {
            ActivationState::Draining { handoff, .. } if handoff == handoff_id => {
                let status = match self.status(broker).await {
                    Ok(status) => status,
                    Err(diagnostic) => {
                        return (control_error(diagnostic), false);
                    }
                };
                let authorization = match self.recovery.lock().await.clone() {
                    Some(recovery) => recovery.authorize_old_commit(&handoff_id, &status).await,
                    None => Err("cannot stop the existing broker for commit: no saved activation record is available to authorize it".to_owned()),
                };
                if let Err(diagnostic) = authorization {
                    return (control_error(diagnostic), false);
                }
                (ControlResult::Committed(status), true)
            }
            ActivationState::SupervisorOnly { handoff } if handoff == handoff_id => {
                let proof = self.old_retirement.lock().await.clone();
                match proof {
                    Some(OldRetirementProof {
                        handoff: current,
                        outcome: Ok(()),
                    }) if current == handoff => (
                        self.status_result(broker, ControlResult::Committed).await,
                        true,
                    ),
                    Some(OldRetirementProof {
                        handoff: current,
                        outcome: Err(diagnostic),
                    }) if current == handoff => (control_error(diagnostic), false),
                    _ => (
                        control_error("Muxe has no recorded confirmation that the old broker finished stopping for this handoff".to_owned()),
                        false,
                    ),
                }
            }
            ActivationState::TargetGated { handoff } if handoff == handoff_id => {
                *self.state.lock().await = ActivationState::TargetCommitted { handoff };
                (
                    self.status_result(broker, ControlResult::Committed).await,
                    false,
                )
            }
            ActivationState::TargetCommitted { handoff } if handoff == handoff_id => (
                self.status_result(broker, ControlResult::Committed).await,
                false,
            ),
            _ => (
                control_error("cannot commit activation: the broker is not prepared for the requested handoff ID".to_owned()),
                false,
            ),
        }
    }

    /// The sole old-role retirement barrier for RPC Commit and both recovery watches.
    /// The transition mutex serializes entry, but the state mutex is never held over
    /// adapter shutdown, monitor join, socket unlink, or receipt durability.
    async fn retire_old(
        &self,
        broker: &Broker,
        handoff: HandoffId,
    ) -> Result<(Option<RetirementTicket>, Result<(), String>), String> {
        let _transition = self.transition.lock().await;
        let state = self.state.lock().await.clone();
        if let ActivationState::SupervisorOnly { handoff: current } = state {
            if current != handoff {
                return Err("The requested handoff ID does not match the handoff for which the old broker confirmed it stopped".to_owned());
            }
            let proof = self
                .old_retirement
                .lock()
                .await
                .clone()
                .filter(|recorded| recorded.handoff == handoff)
                .ok_or_else(|| "Muxe has no recorded confirmation that the old broker finished stopping for this handoff".to_owned())?;
            return Ok((None, proof.outcome));
        }
        if !matches!(
            state,
            ActivationState::Draining { handoff: current, .. } if current == handoff
        ) {
            return Err("cannot stop the existing broker: it must first stop accepting new work for the requested handoff ID".to_owned());
        }
        let status = self.status(broker).await?;
        let ticket = self.stop_listener().await?;
        // Stop is irreversible even if proof persistence fails. Never reopen this
        // adapter or claim a second Stop on the now-exited run loop.
        *self.state.lock().await = ActivationState::SupervisorOnly { handoff };
        let proof = match self.recovery.lock().await.clone() {
            Some(recovery) => recovery.publish_old_retirement(&handoff, &status).await,
            None => Ok(()),
        };
        *self.old_retirement.lock().await = Some(OldRetirementProof {
            handoff,
            outcome: proof.clone(),
        });
        Ok((Some(ticket), proof))
    }

    async fn admit_prepare(
        &self,
        target: &CompatibilityRecord,
        handoff: HandoffId,
    ) -> PrepareAdmission {
        let mut state = self.state.lock().await;
        match &*state {
            ActivationState::Draining {
                target: current,
                handoff: current_handoff,
                ..
            } if **current == *target && *current_handoff == handoff => {
                PrepareAdmission::Idempotent
            }
            ActivationState::Draining { .. } => PrepareAdmission::Reject(
                "cannot prepare the existing broker: it is already preparing a different activation target or handoff ID".to_owned(),
            ),
            ActivationState::Running | ActivationState::TargetCommitted { .. } => {
                *state = ActivationState::Draining {
                    target: Box::new(target.clone()),
                    handoff,
                    host_suspended: false,
                };
                PrepareAdmission::Begin
            }
            _ => PrepareAdmission::Reject(
                "cannot prepare replacement: the existing broker must be running".to_owned(),
            ),
        }
    }

    async fn handle_suspend_failure(
        &self,
        broker: &Broker,
        target: &CompatibilityRecord,
        handoff: HandoffId,
        error: &BrokerError,
    ) -> (ControlResult, bool) {
        if matches!(
            error,
            BrokerError::Adapter(adapter) if adapter.kind == AdapterErrorKind::Unsupported
        ) {
            *self.state.lock().await = ActivationState::Running;
            broker.reopen_dispatch().await;
            return (
                control_error(format!(
                    "host adapter cannot suspend for activation: {error}"
                )),
                false,
            );
        }
        let restore = broker.resume_host_after_abort().await;
        let mut diagnostic = format!("could not suspend host subscription for activation: {error}");
        match restore {
            Ok(()) => {
                *self.state.lock().await = ActivationState::Running;
                broker.reopen_dispatch().await;
            }
            Err(restore) => {
                *self.state.lock().await = ActivationState::Draining {
                    target: Box::new(target.clone()),
                    handoff,
                    host_suspended: true,
                };
                let _ = write!(
                    diagnostic,
                    "; could not restore the host subscription either; the existing broker is still not accepting new work: {restore}"
                );
            }
        }
        (control_error(diagnostic), false)
    }

    async fn handle_listener_drain_failure(
        &self,
        broker: &Broker,
        target: &CompatibilityRecord,
        handoff: HandoffId,
        error: &str,
    ) -> (ControlResult, bool) {
        let restore = broker.resume_host_after_abort().await;
        *self.state.lock().await = ActivationState::Draining {
            target: Box::new(target.clone()),
            handoff,
            host_suspended: restore.is_ok(),
        };
        let mut diagnostic = format!(
            "could not stop the existing broker listener for activation: {error}; the broker is still not accepting new work"
        );
        if let Err(restore) = restore {
            let _ = write!(
                diagnostic,
                "; could not restore the host subscription either: {restore}"
            );
        }
        (control_error(diagnostic), false)
    }

    /// Runs the old-broker Prepare transition: UI drain, host suspend, listener
    /// release. Every failure restores a consistent state without ever claiming a
    /// healthy Running broker it has not re-verified.
    async fn handle_prepare(
        &self,
        broker: &Broker,
        target: CompatibilityRecord,
        handoff: HandoffId,
    ) -> (ControlResult, bool) {
        match self.admit_prepare(&target, handoff).await {
            PrepareAdmission::Idempotent => {
                return (
                    self.status_result(broker, ControlResult::Prepared).await,
                    false,
                );
            }
            PrepareAdmission::Reject(diagnostic) => {
                return (control_error(diagnostic), false);
            }
            PrepareAdmission::Begin => {}
        }
        if let Err(error) = broker.drain_for_activation().await {
            let prepare_refusal = matches!(&error, BrokerError::ActivationDrainRefused(_))
                .then_some(muxe_protocol::control::PrepareRefusalEvidence::NoHostMutation);
            *self.state.lock().await = ActivationState::Running;
            return (
                ControlResult::Error {
                    diagnostic: bounded_control_diagnostic(error.to_string()),
                    prepare_refusal,
                },
                false,
            );
        }
        if let Err(error) = broker.suspend_host_for_activation().await {
            return self
                .handle_suspend_failure(broker, &target, handoff, &error)
                .await;
        }
        if let Err(error) = self.drain_listener().await {
            return self
                .handle_listener_drain_failure(broker, &target, handoff, &error)
                .await;
        }
        if let ActivationState::Draining { host_suspended, .. } = &mut *self.state.lock().await {
            *host_suspended = true;
        }
        (
            self.status_result(broker, ControlResult::Prepared).await,
            false,
        )
    }

    /// Runs the old-broker Abort transition: host resume, listener rebind, and only
    /// then Running. Any failure stays Draining so the coordinator can retry.
    async fn handle_abort(&self, broker: &Broker, handoff_id: HandoffId) -> (ControlResult, bool) {
        enum AbortPlan {
            ResumeOld {
                target: Box<CompatibilityRecord>,
                host_suspended: bool,
            },
            Stop,
        }
        // Decide under the state lock, then act after it is released: awaiting
        // `status_result` while holding the state mutex deadlocks on itself.
        let plan = {
            let mut state = self.state.lock().await;
            match state.clone() {
                ActivationState::Draining {
                    target,
                    handoff,
                    host_suspended,
                } if handoff == handoff_id => AbortPlan::ResumeOld {
                    target,
                    host_suspended,
                },
                ActivationState::TargetGated { handoff } if handoff == handoff_id => {
                    *state = ActivationState::Retired;
                    AbortPlan::Stop
                }
                ActivationState::Retired => AbortPlan::Stop,
                _ => {
                    return (
                        control_error(
                            "cannot abort activation: the broker is not prepared for the requested handoff ID".to_owned(),
                        ),
                        false,
                    );
                }
            }
        };
        match plan {
            AbortPlan::ResumeOld {
                target,
                host_suspended,
            } => {
                // Resume and revalidate the old host subscription before
                // the old endpoint accepts dispatch again, then rebind the
                // listener. Running is entered only after both are
                // healthy; any failure stays Draining so the coordinator
                // can retry Abort. Never reopen as healthy and never
                // pretend rollback succeeded. A Prepare that restored the
                // adapter after its own listener failure leaves the host
                // live, so the adapter resume is skipped then.
                if host_suspended && let Err(error) = broker.resume_host_after_abort().await {
                    return (
                        control_error(format!(
                            "could not restore the existing broker's host subscription after activation abort: {error}; the broker is still not accepting new work"
                        )),
                        false,
                    );
                }
                if let Err(error) = self.resume_listener().await {
                    *self.state.lock().await = ActivationState::Draining {
                        target,
                        handoff: handoff_id,
                        host_suspended: false,
                    };
                    return (
                        control_error(format!(
                            "could not reopen the existing broker listener after activation abort: {error}; the broker is still not accepting new work"
                        )),
                        false,
                    );
                }
                *self.state.lock().await = ActivationState::Running;
                (
                    self.status_result(broker, ControlResult::Aborted).await,
                    false,
                )
            }
            AbortPlan::Stop => (
                self.status_result(broker, ControlResult::Aborted).await,
                true,
            ),
        }
    }

    async fn status_result(
        &self,
        broker: &Broker,
        result: fn(ActivationStatus) -> ControlResult,
    ) -> ControlResult {
        match self.status(broker).await {
            Ok(status) => result(status),
            Err(diagnostic) => control_error(diagnostic),
        }
    }

    async fn control_connected(&self) {
        *self.connections.lock().await += 1;
    }

    async fn control_disconnected(self: Arc<Self>, broker: Arc<Broker>) {
        let remaining = {
            let mut connections = self.connections.lock().await;
            *connections = connections.saturating_sub(1);
            *connections
        };
        if remaining != 0 {
            return;
        }
        self.spawn_recovery_watch(broker).await;
    }

    /// Starts the disconnect watch when the last coordinator connection closes while
    /// this broker is mid-activation. The watch re-verifies everything at fire time,
    /// so a coordinator that reconnects (including per-operation reconnects) owns
    /// recovery instead.
    #[expect(
        clippy::too_many_lines,
        reason = "disconnect recovery keeps target-retire, target-own, and old-restore decisions in one ordered state machine"
    )]
    async fn spawn_recovery_watch(self: Arc<Self>, broker: Arc<Broker>) {
        enum Watch {
            Old { handoff: HandoffId },
            Target { handoff: HandoffId },
            Nothing,
        }
        let watch = {
            let _transition = self.transition.lock().await;
            match self.state.lock().await.clone() {
                ActivationState::Draining { handoff, .. }
                | ActivationState::SupervisorOnly { handoff } => Watch::Old { handoff },
                ActivationState::TargetGated { handoff } => Watch::Target { handoff },
                _ => Watch::Nothing,
            }
        };
        let (handoff, is_target) = match watch {
            Watch::Nothing => return,
            Watch::Old { handoff } => (handoff, false),
            Watch::Target { handoff } => (handoff, true),
        };
        let decision = self.recovery_decision(&broker, &handoff).await;
        match decision {
            RecoveryDecision::Preserve { reason } => {
                tracing::warn!(%reason, "activation recovery preserves owner decision");
                return;
            }
            RecoveryDecision::CleanupComplete | RecoveryDecision::Committed { permit: None } => {
                return;
            }
            RecoveryDecision::Committed {
                permit: Some(permit),
            } if !is_target => {
                self.finish_old_recovery(&broker, handoff, Some(permit))
                    .await;
                return;
            }
            RecoveryDecision::Committed {
                permit: Some(permit),
            } => {
                Self::acknowledge_target_commit(permit, &handoff).await;
                return;
            }
            RecoveryDecision::TargetOwns { permit, .. } if !is_target => {
                self.finish_old_recovery(&broker, handoff, permit).await;
                return;
            }
            RecoveryDecision::TargetOwns { permit, .. } => {
                let gated = matches!(
                    self.state.lock().await.clone(),
                    ActivationState::TargetGated { handoff: current } if current == handoff
                );
                if gated {
                    *self.state.lock().await = ActivationState::TargetCommitted { handoff };
                    if let Some(permit) = permit {
                        let _ = permit
                            .acknowledge(&handoff, RecoveryAck::TargetCommitted)
                            .await;
                    }
                    tracing::warn!("target completed its unit commit after coordinator disconnect");
                }
                return;
            }
            RecoveryDecision::NoJournal if is_target => {
                let retire = matches!(
                    self.state.lock().await.clone(),
                    ActivationState::TargetGated { handoff: current } if current == handoff
                );
                if !retire {
                    return;
                }
                *self.state.lock().await = ActivationState::Retired;
                if let Err(error) = self.stop_listener().await {
                    tracing::warn!(%error, "incomplete-unit target could not stop listener");
                }
                return;
            }
            RecoveryDecision::RestoreOld { permit, .. } if is_target => {
                let retire = matches!(
                    self.state.lock().await.clone(),
                    ActivationState::TargetGated { handoff: current } if current == handoff
                );
                if !retire {
                    return;
                }
                *self.state.lock().await = ActivationState::Retired;
                match self.stop_listener().await {
                    Err(error) => {
                        tracing::warn!(%error, "incomplete-unit target could not stop listener");
                    }
                    Ok(ticket) => {
                        if let Some(permit) = permit
                            && let Err(error) = permit
                                .acknowledge(&handoff, RecoveryAck::TargetRetired)
                                .await
                        {
                            tracing::warn!(%error, "target retirement acknowledgement failed");
                        }
                        drop(ticket);
                    }
                }
                return;
            }
            RecoveryDecision::NoJournal => {}
            RecoveryDecision::RestoreOld {
                recover_after,
                permit,
            } => {
                drop(permit);
                if !recover_after.is_zero() {
                    tokio::time::sleep(recover_after).await;
                }
            }
        }
        self.run_recovery_watch(&broker, handoff, is_target).await;
    }
    async fn acknowledge_target_commit(permit: Arc<dyn RecoveryPermit>, handoff: &HandoffId) {
        if let Err(error) = permit
            .acknowledge(handoff, RecoveryAck::TargetCommitted)
            .await
        {
            tracing::warn!(%error, "target commit acknowledgement failed");
        }
    }

    async fn finish_old_recovery(
        &self,
        broker: &Broker,
        handoff: HandoffId,
        permit: Option<Arc<dyn RecoveryPermit>>,
    ) {
        let (ticket, proof) = match self.retire_old(broker, handoff).await {
            Ok(retirement) => retirement,
            Err(error) => {
                tracing::warn!(%error, "old broker recovery retirement barrier failed");
                return;
            }
        };
        if let Err(error) = proof {
            tracing::warn!(%error, "old broker post-stop proof failed");
            return;
        }
        if let Some(permit) = permit
            && let Err(error) = permit
                .acknowledge(&handoff, RecoveryAck::OldCommitted)
                .await
        {
            tracing::warn!(%error, "old broker retirement acknowledgement failed");
        }
        drop(ticket);
    }

    async fn run_recovery_watch(&self, broker: &Broker, handoff: HandoffId, is_target: bool) {
        let transition = self.transition.lock().await;
        if *self.connections.lock().await != 0 {
            return; // the coordinator returned; it owns recovery now.
        }
        drop(transition);
        // Re-read the journal at fire time without holding the transition lock:
        // owner-side probes and restoration may await control sockets.
        let decision = self.recovery_decision(broker, &handoff).await;
        let mut resume_permit: Option<Arc<dyn RecoveryPermit>> = None;
        match decision {
            RecoveryDecision::Preserve { reason } => {
                tracing::warn!(%reason, "activation recovery preserves owner decision");
                return;
            }
            RecoveryDecision::CleanupComplete | RecoveryDecision::Committed { permit: None } => {
                return;
            }
            RecoveryDecision::TargetOwns { permit, .. } if is_target => {
                let gated = matches!(
                    self.state.lock().await.clone(),
                    ActivationState::TargetGated { handoff: current } if current == handoff
                );
                if gated {
                    *self.state.lock().await = ActivationState::TargetCommitted { handoff };
                    if let Some(permit) = permit {
                        Self::acknowledge_target_commit(permit, &handoff).await;
                    }
                }
                return;
            }
            RecoveryDecision::Committed {
                permit: Some(permit),
            } if !is_target => {
                self.finish_old_recovery(broker, handoff, Some(permit))
                    .await;
                return;
            }
            RecoveryDecision::Committed {
                permit: Some(permit),
            } => {
                Self::acknowledge_target_commit(permit, &handoff).await;
                return;
            }
            RecoveryDecision::TargetOwns { permit, .. } => {
                self.finish_old_recovery(broker, handoff, permit).await;
                return;
            }
            RecoveryDecision::NoJournal if is_target => {
                let gated = matches!(
                    self.state.lock().await.clone(),
                    ActivationState::TargetGated { handoff: current } if current == handoff
                );
                if gated {
                    *self.state.lock().await = ActivationState::Retired;
                    if let Err(error) = self.stop_listener().await {
                        tracing::warn!(%error, "incomplete-unit target could not stop listener");
                    }
                }
                return;
            }
            RecoveryDecision::RestoreOld { permit, .. } if is_target => {
                let gated = matches!(
                    self.state.lock().await.clone(),
                    ActivationState::TargetGated { handoff: current } if current == handoff
                );
                if !gated {
                    return;
                }
                *self.state.lock().await = ActivationState::Retired;
                match self.stop_listener().await {
                    Err(error) => {
                        tracing::warn!(%error, "incomplete-unit target could not stop listener");
                    }
                    Ok(ticket) => {
                        if let Some(permit) = permit
                            && let Err(error) = permit
                                .acknowledge(&handoff, RecoveryAck::TargetRetired)
                                .await
                        {
                            tracing::warn!(%error, "target retirement acknowledgement failed");
                        }
                        drop(ticket);
                    }
                }
                return;
            }
            RecoveryDecision::NoJournal => {}
            RecoveryDecision::RestoreOld {
                recover_after,
                permit,
            } => {
                resume_permit = permit;
                if !recover_after.is_zero() {
                    tokio::time::sleep(recover_after).await;
                }
            }
        }
        self.resume_old_after_disconnect(broker, &handoff, resume_permit)
            .await;
    }

    async fn resume_old_after_disconnect(
        &self,
        broker: &Broker,
        handoff: &HandoffId,
        resume_permit: Option<Arc<dyn RecoveryPermit>>,
    ) {
        // Old unit restoration: only from the same Draining handoff, never after commit.
        let host_suspended = match self.state.lock().await.clone() {
            ActivationState::Draining {
                handoff: current,
                host_suspended,
                ..
            } if current == *handoff => host_suspended,
            _ => return,
        };
        if host_suspended && let Err(error) = broker.resume_host_after_abort().await {
            tracing::warn!(%error, "disconnect recovery could not resume host; endpoint stays drained");
            return;
        }
        if let Err(error) = self.resume_listener().await {
            tracing::warn!(%error, "disconnect recovery could not rebind listener; endpoint stays drained");
            return;
        }
        *self.state.lock().await = ActivationState::Running;
        broker.reopen_dispatch().await;
        if let Some(permit) = resume_permit
            && let Err(error) = permit.acknowledge(handoff, RecoveryAck::Resumed).await
        {
            tracing::warn!(%error, "disconnect recovery resume acknowledgment failed");
            return;
        }
        tracing::warn!("old broker restored its endpoint after coordinator disconnect");
    }

    async fn recovery_decision(&self, broker: &Broker, handoff: &HandoffId) -> RecoveryDecision {
        let local_status = match self.status(broker).await {
            Ok(status) => status,
            Err(reason) => return RecoveryDecision::Preserve { reason },
        };
        let recovery = self.recovery.lock().await.clone();
        match recovery {
            Some(recovery) => recovery.recovery_decision(handoff, &local_status).await,
            None => RecoveryDecision::Preserve {
                reason: "owner recovery operation is not installed".to_owned(),
            },
        }
    }
}

pub struct BrokerServer {
    broker: Arc<Broker>,
    endpoint: RuntimeEndpoint,
    listener: Option<UnixListener>,
    socket_device: u64,
    socket_inode: u64,
    // Paused while an activation transition drains this broker: a drained broker must
    // not reload or revalidate configuration against a suspended adapter. The run loop
    // drops it on Drain and restarts it on Resume; Stop and shutdown drop it for good.
    config_watch: Option<ConfigWatch>,
    activation: Option<Arc<ActivationController>>,
}

impl BrokerServer {
    /// Binds the owner-only endpoint and starts configuration watching.
    ///
    /// # Errors
    ///
    /// Returns `ServerError` when the endpoint cannot be bound or watched.
    pub async fn start(
        broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
    ) -> Result<Self, ServerError> {
        Self::start_inner(broker, endpoint, None, None).await
    }

    /// Binds the owner-only endpoint while holding a startup lock the executable
    /// acquired before its own pre-bind work (adapter connect). Consuming the
    /// held lock closes the coldstart overlap where two children connect at
    /// once and only the second bind fails: the loser fails fast at acquire
    /// time instead. The lock still serializes only the startup transaction,
    /// never the server lifetime.
    ///
    /// # Errors
    ///
    /// Returns `ServerError` when the endpoint cannot be bound or watched.
    pub async fn start_with_lock(
        broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
        lock: StartupLock,
    ) -> Result<Self, ServerError> {
        Self::start_inner(broker, endpoint, None, Some(lock)).await
    }

    /// Starts a broker that serves the activation coordinator protocol on the same owner-only
    /// endpoint as ordinary IPC. Targets verify their recorded host identity before binding and
    /// remain UI-gated until the matching commit.
    ///
    /// # Errors
    ///
    /// Returns `ServerError` when host identity, journal authorization, endpoint
    /// binding, or configuration watching fails.
    pub async fn start_activation(
        broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
        bootstrap: ActivationBootstrap,
        recovery: Option<Arc<dyn RecoveryJournal>>,
    ) -> Result<Self, ServerError> {
        let activation = ActivationController::start(&broker, bootstrap).await?;
        if let Some(recovery) = recovery {
            activation.set_recovery(recovery).await;
        }
        Self::start_inner(broker, endpoint, Some(activation), None).await
    }

    /// Binds this server's original registry token to every control status.
    /// Must run after registration and before `run` accepts connections.
    ///
    /// # Errors
    ///
    /// Refuses duplicate attestation or a non-activation server.
    pub fn attest_registration(&self, proof: BrokerRegistrationProof) -> Result<(), ServerError> {
        self.activation
            .as_ref()
            .ok_or_else(|| ServerError::Activation("cannot record broker registration identity: this server has no activation control service".to_owned()))?
            .registration
            .set(proof)
            .map_err(|_| ServerError::Activation("cannot record broker registration identity a second time".to_owned()))
    }

    /// Activation form of [`BrokerServer::start_with_lock`]: the executable
    /// acquires the startup lock, connects its adapter, then hands the held
    /// lock here so the target bind cannot overlap a sibling child.
    ///
    /// # Errors
    ///
    /// Returns `ServerError` when host identity, journal authorization, endpoint
    /// binding, or configuration watching fails.
    pub async fn start_activation_with_lock(
        broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
        bootstrap: ActivationBootstrap,
        recovery: Option<Arc<dyn RecoveryJournal>>,
        lock: StartupLock,
    ) -> Result<Self, ServerError> {
        let activation = ActivationController::start(&broker, bootstrap).await?;
        if let Some(recovery) = recovery {
            activation.set_recovery(recovery).await;
        }
        Self::start_inner(broker, endpoint, Some(activation), Some(lock)).await
    }

    async fn start_inner(
        broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
        activation: Option<Arc<ActivationController>>,
        lock: Option<StartupLock>,
    ) -> Result<Self, ServerError> {
        // A child-held lock extends the same startup transaction across the
        // executable's pre-bind work; otherwise the lock is acquired here.
        // Either way it is dropped after bind plus watcher install below.
        let startup_lock = match lock {
            Some(lock) => lock,
            None => endpoint.acquire_startup_lock()?,
        };
        endpoint.remove_validated_stale_socket()?;
        let listener = endpoint.bind_listener()?;
        let socket_metadata = fs::symlink_metadata(endpoint.socket())?;
        if !socket_metadata.file_type().is_socket() {
            return Err(io::Error::other("broker listener path is not a Unix socket").into());
        }
        let config_watch = ConfigWatch::start(Arc::clone(&broker)).await?;
        // Binding the owner-only listener and installing the watcher make this server ready for
        // the handshake. The lock serializes only that startup transaction, never its lifetime.
        drop(startup_lock);
        Ok(Self {
            broker,
            endpoint,
            listener: Some(listener),
            socket_device: socket_metadata.dev(),
            socket_inode: socket_metadata.ino(),
            config_watch: Some(config_watch),
            activation,
        })
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    /// Serves the owner-only socket until the supplied shutdown signal changes to true.
    /// A control Stop unlinks the endpoint and then lingers as a detached-child
    /// supervisor until every remaining generic child is reaped.
    ///
    /// # Panics
    ///
    /// Panics if the listener vanishes while bound; the command loop prevents this
    /// by enabling accepts only while the listener is present.
    ///
    /// # Errors
    ///
    /// Returns `ServerError` when binding, watching, identity, or IO fails.
    #[expect(
        clippy::too_many_lines,
        reason = "broker run owns the select loop and retirement barrier so command ordering remains auditable"
    )]
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<(), ServerError> {
        let broker = Arc::clone(&self.broker);
        let endpoint = self.endpoint.clone();
        let activation = self.activation.clone();
        let mut listener = self.listener.take();
        let mut socket_device = self.socket_device;
        let mut socket_inode = self.socket_inode;
        let mut config_watch = self.config_watch.take();
        let mut health = Some(tokio::spawn(Arc::clone(&broker).monitor(shutdown.clone())));
        let (commands, mut command_rx) = mpsc::channel(8);
        if let Some(activation) = &activation {
            activation.install_commands(commands).await;
        }
        let mut expiry = tokio::time::interval(Duration::from_millis(100));
        let mut supervisor_only = false;
        let result = loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break Ok(());
                    }
                }
                _ = expiry.tick() => {
                    broker.expire_pending().await;
                    // Poll the broker-owned flag here too: `notify_waiters`
                    // stores no permit, so a notification that fired before
                    // this select was entered would otherwise be missed.
                    if !supervisor_only && broker.host_loss_notified() {
                        if let Err(error) = retire_for_host_loss(
                            &broker,
                            &endpoint,
                            &mut listener,
                            socket_device,
                            socket_inode,
                            &mut config_watch,
                            &mut health,
                        )
                        .await
                        {
                            tracing::warn!(%error, "host-loss retirement teardown failed");
                        }
                        supervisor_only = true;
                        break Ok(());
                    }
                }
                () = broker.host_loss_retired() => {
                    // Re-check after wake: the notification carries no permit,
                    // so only the broker-owned flag authorizes retirement.
                    if supervisor_only || !broker.host_loss_notified() {
                        continue;
                    }
                    // Terminal host loss: the monitor failed every Adapter-owned
                    // execution and signalled; the helper below drains UI state
                    // (drain's BrokerRetiring emits + detaches), shuts the host
                    // adapter down, joins the monitor, and unlinks the owned
                    // endpoint — then the loop lingers supervisor-only for
                    // detached generic children. The loop owns
                    // listener/config-watch; the broker only signals.
                    if let Err(error) = retire_for_host_loss(
                        &broker,
                        &endpoint,
                        &mut listener,
                        socket_device,
                        socket_inode,
                        &mut config_watch,
                        &mut health,
                    )
                    .await
                    {
                        tracing::warn!(%error, "host-loss retirement teardown failed");
                    }
                    supervisor_only = true;
                    break Ok(());
                }
                command = command_rx.recv(), if activation.is_some() => {
                    match command {
                        Some(ServerCommand::Drain { complete }) => {
                            // A drained broker stops watching configuration until Resume:
                            // reloads would revalidate against a suspended adapter.
                            config_watch.take();
                            let result = if listener.take().is_some() {
                                remove_owned_socket(&endpoint, socket_device, socket_inode)
                                    .map_err(|error| error.to_string())
                            } else {
                                Ok(())
                            };
                            let _ = complete.send(result);
                        }
                        Some(ServerCommand::Resume { complete }) => {
                            resume_run_endpoint(
                                &broker,
                                &endpoint,
                                &mut listener,
                                &mut socket_device,
                                &mut socket_inode,
                                &mut config_watch,
                                complete,
                            )
                            .await;
                        }
                        Some(ServerCommand::Stop { complete }) => {
                            // Terminal dispatch outcomes remain observable through the health
                            // monitor until the adapter seals admission, joins every retained
                            // host task, and closes its completion queue.
                            // Retire drains UI here through its control transition;
                            // old Commit already drained UI at Prepare. Stop must
                            // not repeat either transition.
                            drop(config_watch.take());
                            let outcome = match broker.shutdown_host_adapter().await {
                                Ok(()) => {
                                    let monitor_result = match health.take() {
                                        Some(monitor) => monitor
                                            .await
                                            .map_err(|error| format!("could not wait for the host monitor to stop: {error}")),
                                        None => Ok(()),
                                    };
                                    monitor_result.and_then(|()| {
                                        if listener.take().is_some() {
                                            remove_owned_socket(
                                                &endpoint,
                                                socket_device,
                                                socket_inode,
                                            )
                                            .map_err(|error| error.to_string())
                                        } else {
                                            Ok(())
                                        }
                                    })
                                }
                                Err(error) => Err(error.to_string()),
                            };
                            match outcome {
                                Ok(()) => {
                                    supervisor_only = true;
                                    let (ticket, released) = RetirementTicket::pair();
                                    if complete.send(Ok(ticket)).is_ok() {
                                        let _ = released.await;
                                    }
                                    break Ok(());
                                }
                                Err(error) => {
                                    let _ = complete.send(Err(error));
                                    supervisor_only = true;
                                    break Ok(());
                                }
                            }
                        }
                        None => break Ok(()),
                    }
                }
                accepted = async {
                    listener
                        .as_ref()
                        .expect("listener branch is enabled only while bound")
                        .accept()
                        .await
                }, if listener.is_some() => {
                    let (stream, _) = accepted.map_err(ServerError::Io)?;
                    let broker = Arc::clone(&broker);
                    let activation = activation.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_connection(broker, activation, stream).await {
                            tracing::debug!(%error, "broker client disconnected");
                        }
                    });
                }
            }
        };
        if let Some(health) = health {
            health.abort();
        }
        if listener.is_some() {
            let _ = remove_owned_socket(&endpoint, socket_device, socket_inode);
        }
        if supervisor_only {
            await_supervised_drain(&broker, &mut shutdown).await;
        }
        result
    }
}

/// Terminal host-loss teardown (not the coordinator Stop path, which owns
/// its UI drain through the Retire branch): drains broker-owned UI state
/// (`BrokerRetiring` emits + detaches; a dead host cannot confirm
/// pending-pane/capture closes, so the cleanup error is warned and ignored),
/// then drops the config watch (probes stop: reloads revalidate against the
/// adapter), shuts the host adapter down, joins the health monitor, and
/// unlinks the owned endpoint. The caller then lingers supervisor-only until
/// detached generic children are reaped.
/// Listener/config-watch ownership stays with the run loop; the broker only
/// signals via its host-loss flag. The monitor already failed every
/// Adapter-owned execution before signalling, so the drain finds an empty
/// host-owner set and cannot refuse on non-cancellable Herdr work.
async fn retire_for_host_loss(
    broker: &Arc<Broker>,
    endpoint: &RuntimeEndpoint,
    listener: &mut Option<UnixListener>,
    socket_device: u64,
    socket_inode: u64,
    config_watch: &mut Option<ConfigWatch>,
    health: &mut Option<JoinHandle<()>>,
) -> Result<(), String> {
    if let Err(error) = broker.drain_for_activation().await {
        tracing::warn!(
            %error,
            "host-loss drain could not confirm host cleanup; retiring anyway"
        );
    }
    drop(config_watch.take());
    match broker.shutdown_host_adapter().await {
        Ok(()) => {
            if let Some(monitor) = health.take() {
                let _ = monitor.await;
            }
            if listener.take().is_some() {
                remove_owned_socket(endpoint, socket_device, socket_inode)
                    .map_err(|error| error.to_string())
            } else {
                Ok(())
            }
        }
        Err(error) => Err(error.to_string()),
    }
}

/// Waits until no detached generic child remains supervised. Shutdown interrupts
/// the wait; the caller already dropped the endpoint, watcher, and health monitor.
async fn await_supervised_drain(broker: &Arc<Broker>, shutdown: &mut watch::Receiver<bool>) {
    // DES commit path: the old broker exits only after its remaining detached
    // children are reaped. Shutdown still interrupts the wait.
    while broker.has_supervised_children() {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

/// Rebinds a drained endpoint and restarts configuration watching after abort or
/// disconnect recovery. Idempotent: an endpoint that never drained succeeds at once.
async fn resume_run_endpoint(
    broker: &Arc<Broker>,
    endpoint: &RuntimeEndpoint,
    listener: &mut Option<UnixListener>,
    socket_device: &mut u64,
    socket_inode: &mut u64,
    config_watch: &mut Option<ConfigWatch>,
    complete: oneshot::Sender<Result<(), String>>,
) {
    // Idempotent ensure: a Prepare that restored the adapter after its own
    // listener failure leaves the endpoint bound, so Abort must not fail here.
    let result = if listener.is_some() {
        Ok(())
    } else {
        match ConfigWatch::start(Arc::clone(broker)).await {
            Err(error) => Err(error.to_string()),
            Ok(watch) => {
                *config_watch = Some(watch);
                (|| -> Result<(), ServerError> {
                    let startup_lock = endpoint.acquire_startup_lock()?;
                    endpoint.remove_validated_stale_socket()?;
                    let rebound = endpoint.bind_listener()?;
                    let metadata = fs::symlink_metadata(endpoint.socket())?;
                    if !metadata.file_type().is_socket() {
                        return Err(
                            io::Error::other("broker listener path is not a Unix socket").into(),
                        );
                    }
                    *socket_device = metadata.dev();
                    *socket_inode = metadata.ino();
                    *listener = Some(rebound);
                    drop(startup_lock);
                    Ok(())
                })()
                .map_err(|error| {
                    config_watch.take();
                    error.to_string()
                })
            }
        }
    };
    let _ = complete.send(result);
}

fn remove_owned_socket(
    endpoint: &RuntimeEndpoint,
    socket_device: u64,
    socket_inode: u64,
) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(endpoint.socket()) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket()
        || metadata.dev() != socket_device
        || metadata.ino() != socket_inode
    {
        return Ok(());
    }
    fs::remove_file(endpoint.socket())
}
impl Drop for BrokerServer {
    fn drop(&mut self) {
        let Ok(metadata) = fs::symlink_metadata(self.endpoint.socket()) else {
            return;
        };
        if metadata.file_type().is_socket()
            && metadata.dev() == self.socket_device
            && metadata.ino() == self.socket_inode
        {
            let _ = fs::remove_file(self.endpoint.socket());
        }
    }
}

struct ConnectionResources {
    role: PeerRole,
    handshaken: bool,
    request_ids: HashSet<RequestId>,
    launcher_tokens: HashSet<PendingLaunchToken>,
    /// The one phase of this connection's UI attachment. A single enum owns
    /// the pending-vs-attached distinction: `Pending` carries both the launch
    /// token and the pre-published session, `Attached` carries only the live
    /// session, and the spawned readiness waiter moves `Pending` forward
    /// before enqueueing its response so disconnect can never observe a stale
    /// token for a consumed launch.
    attachment: Arc<std::sync::Mutex<UiAttachment>>,
    /// The one gated `AttachUi` readiness waiter. Keeping the real handle lets
    /// disconnect abort and join it before taking attachment ownership.
    waiter: Option<JoinHandle<()>>,
    /// Read-only placement wait; disconnect cancels it without claiming the launch token.
    placement_waiter: Option<JoinHandle<()>>,
    #[cfg(test)]
    publication_hook: Option<Arc<crate::broker::WaitHook>>,
}

/// Phaseful UI attachment ownership for one connection. Exactly one variant
/// is live at a time; disconnect takes the state to `Closed` atomically.
#[derive(Clone, Debug)]
enum UiAttachment {
    Idle,
    WaitingPlacement,
    Pending {
        token: PendingLaunchToken,
        session: UiSessionId,
    },
    Attached {
        session: UiSessionId,
    },
    Closed,
}

impl ConnectionResources {
    fn new(role: PeerRole) -> Self {
        Self {
            role,
            handshaken: false,
            request_ids: HashSet::new(),
            launcher_tokens: HashSet::new(),
            attachment: Arc::new(std::sync::Mutex::new(UiAttachment::Idle)),
            waiter: None,
            placement_waiter: None,
            #[cfg(test)]
            publication_hook: None,
        }
    }

    fn attachment(&self) -> UiAttachment {
        self.attachment
            .lock()
            .expect("connection attachment is not poisoned")
            .clone()
    }

    fn attached_session(&self) -> Option<UiSessionId> {
        match self.attachment() {
            UiAttachment::Attached { session } => Some(session),
            UiAttachment::Pending { .. }
            | UiAttachment::WaitingPlacement
            | UiAttachment::Idle
            | UiAttachment::Closed => None,
        }
    }

    fn validate_request(&self, request: &ClientRequest) -> Result<(), &'static str> {
        match request {
            ClientRequest::WaitUiPlacement(_) if self.role == PeerRole::Ui => {
                if matches!(self.attachment(), UiAttachment::Idle)
                    && self.placement_waiter.is_none()
                {
                    Ok(())
                } else {
                    Err("one UI connection may wait for placement only before attachment")
                }
            }
            ClientRequest::AttachUi(_) if self.role == PeerRole::Ui => match self.attachment() {
                UiAttachment::Idle => Ok(()),
                _ => Err("one UI connection may attach at most one session"),
            },
            ClientRequest::InvokeBinding(request) if self.role == PeerRole::Ui => self
                .attached_session()
                .is_some_and(|session| session == request.session)
                .then_some(())
                .ok_or("UI connection does not own the invoked session"),
            ClientRequest::MenuControl(request) if self.role == PeerRole::Ui => self
                .attached_session()
                .is_some_and(|session| session == request.session)
                .then_some(())
                .ok_or("UI connection does not own the controlled session"),
            ClientRequest::DetachUi(request) if self.role == PeerRole::Ui => self
                .attached_session()
                .is_some_and(|session| session == request.session)
                .then_some(())
                .ok_or("UI connection does not own the detached session"),
            ClientRequest::RegisterPendingPane(request) if self.role == PeerRole::Launcher => self
                .launcher_tokens
                .contains(&request.token)
                .then_some(())
                .ok_or("launcher connection does not own the pending launch token"),
            ClientRequest::CommitUiLaunch(request) if self.role == PeerRole::Launcher => self
                .launcher_tokens
                .contains(&request.token)
                .then_some(())
                .ok_or("launcher connection does not own the pending launch token"),
            ClientRequest::AbortUiLaunch(request) if self.role == PeerRole::Launcher => self
                .launcher_tokens
                .contains(&request.token)
                .then_some(())
                .ok_or("launcher connection does not own the pending launch token"),
            _ => Ok(()),
        }
    }

    fn record_response(&mut self, request: &ClientRequest, response: &BrokerResponse) {
        match (request, response) {
            (ClientRequest::PrepareUiLaunch(_), BrokerResponse::LaunchPrepared { token, .. }) => {
                self.launcher_tokens.insert(*token);
            }
            (ClientRequest::AttachUi(_), BrokerResponse::UiAttached { session, .. }) => {
                *self
                    .attachment
                    .lock()
                    .expect("connection attachment is not poisoned") = UiAttachment::Attached {
                    session: session.clone(),
                };
            }
            (ClientRequest::DetachUi(request), BrokerResponse::Detached) => {
                let mut attachment = self
                    .attachment
                    .lock()
                    .expect("connection attachment is not poisoned");
                if matches!(
                    &*attachment,
                    UiAttachment::Attached { session } if session == &request.session
                ) {
                    *attachment = UiAttachment::Closed;
                }
            }
            (ClientRequest::AbortUiLaunch(request), BrokerResponse::Acknowledged) => {
                self.launcher_tokens.remove(&request.token);
            }
            _ => {}
        }
    }

    fn record_pending_attachment(
        &mut self,
        request: &ClientRequest,
        session: UiSessionId,
    ) -> Result<Arc<std::sync::Mutex<UiAttachment>>, &'static str> {
        let ClientRequest::AttachUi(request) = request else {
            return Err("only AttachUi may wait for a launch commit");
        };
        let token = request
            .pending_launch
            .ok_or("only a gated AttachUi may wait for a launch commit")?;
        let mut attachment = self
            .attachment
            .lock()
            .expect("connection attachment is not poisoned");
        if !matches!(*attachment, UiAttachment::Idle) {
            return Err("one UI connection may attach at most one session");
        }
        *attachment = UiAttachment::Pending { token, session };
        Ok(Arc::clone(&self.attachment))
    }
}

async fn serve_connection(
    broker: Arc<Broker>,
    activation: Option<Arc<ActivationController>>,
    mut stream: UnixStream,
) -> Result<(), ServerError> {
    verify_same_user_peer(&stream)?;
    let mut prelude = [0; muxe_protocol::PRELUDE_LEN];
    stream.read_exact(&mut prelude).await?;
    let received = Prelude::decode(prelude)?;
    let role = received.role;
    if role == PeerRole::ActivationCoordinator {
        let activation = activation.ok_or(ServerError::UnsupportedRole(role))?;
        return serve_activation_connection(activation, broker, stream, prelude).await;
    }
    if !matches!(role, PeerRole::Launcher | PeerRole::Ui) {
        return Err(ServerError::UnsupportedRole(role));
    }
    let mut decoder = ConnectionDecoder::new(ConnectionPolicy::broker(
        role,
        SchemaFingerprint::application(),
    ));
    let mut initial = Vec::new();
    decoder.push(&prelude, |frame| initial.push(frame))?;

    let (mut reader, mut writer) = stream.into_split();
    writer
        .write_all(&Prelude::rkyv(PeerRole::Broker, SchemaFingerprint::application()).encode())
        .await?;
    let (outbox, mut outbound) = mpsc::channel::<WireMessage>(32);
    let writer_task = tokio::spawn(async move {
        while let Some(message) = outbound.recv().await {
            write_message(&mut writer, &message).await?;
        }
        Ok::<(), io::Error>(())
    });

    let mut resources = ConnectionResources::new(role);
    let result = async {
        process_frames(
            &broker,
            activation.as_ref(),
            &outbox,
            &mut resources,
            initial,
        )
        .await?;
        let mut bytes = Box::new([0; 16 * 1024]);
        loop {
            let read = reader.read(bytes.as_mut()).await?;
            if read == 0 {
                return Ok(());
            }
            let mut frames = Vec::new();
            decoder.push(&bytes[..read], |frame| frames.push(frame))?;
            process_frames(
                &broker,
                activation.as_ref(),
                &outbox,
                &mut resources,
                frames,
            )
            .await?;
        }
    }
    .await;
    disconnect_resources(&broker, &mut resources).await;
    drop(outbox);
    writer_task.abort();
    result
}

async fn serve_activation_connection(
    activation: Arc<ActivationController>,
    broker: Arc<Broker>,
    stream: UnixStream,
    prelude: [u8; muxe_protocol::PRELUDE_LEN],
) -> Result<(), ServerError> {
    activation.control_connected().await;
    let result = serve_activation_connection_inner(
        Arc::clone(&activation),
        Arc::clone(&broker),
        stream,
        prelude,
    )
    .await;
    activation.control_disconnected(broker).await;
    result
}

async fn serve_activation_connection_inner(
    activation: Arc<ActivationController>,
    broker: Arc<Broker>,
    mut stream: UnixStream,
    prelude: [u8; muxe_protocol::PRELUDE_LEN],
) -> Result<(), ServerError> {
    let mut decoder = ControlDecoder::new(ControlPolicy::broker());
    let mut initial = Vec::new();
    decoder
        .push(&prelude, |message| initial.push(message))
        .map_err(|error| ServerError::Activation(error.to_string()))?;
    stream
        .write_all(&Prelude::control(PeerRole::Broker).encode())
        .await?;
    let mut request_ids = HashSet::<ControlRequestId>::new();
    let mut messages = initial;
    let mut bytes = Box::new([0; 16 * 1024]);
    loop {
        while let Some(message) = messages.pop() {
            let ControlMessage::Request(request) = message else {
                return Err(ServerError::UnexpectedMessage);
            };
            if !request_ids.insert(request.request_id) {
                return Err(ServerError::DuplicateControlRequestId);
            }
            let old_handoff = match &request.operation {
                ControlOperation::Commit { handoff_id } => Some(*handoff_id),
                _ => None,
            };
            let (mut result, stop_after_response) =
                activation.handle(&broker, request.operation).await;
            let retirement_ticket = if stop_after_response {
                if let Some(handoff) = old_handoff {
                    match activation.retire_old(&broker, handoff).await {
                        Ok((ticket, Ok(()))) => {
                            result = activation
                                .status_result(&broker, ControlResult::Committed)
                                .await;
                            ticket
                        }
                        Ok((ticket, Err(diagnostic))) => {
                            tracing::warn!(%diagnostic, "old broker post-stop proof failed");
                            result = control_error(diagnostic);
                            ticket
                        }
                        Err(diagnostic) => {
                            tracing::warn!(%diagnostic, "old broker retirement barrier failed");
                            result = control_error(diagnostic);
                            None
                        }
                    }
                } else {
                    Some(
                        activation
                            .stop_listener()
                            .await
                            .map_err(ServerError::Activation)?,
                    )
                }
            } else {
                None
            };
            let response = ControlResponse {
                request_id: request.request_id,
                result,
            };
            // Stop, proof durability, and owned unlink precede this ACK. The
            // ticket keeps detached children supervised through the wire flush.
            write_control_response(&mut stream, &response).await?;
            if retirement_ticket.is_some()
                || (stop_after_response
                    && old_handoff.is_some()
                    && matches!(response.result, ControlResult::Committed(_)))
            {
                drop(retirement_ticket);
                return Ok(());
            }
        }
        let read = stream.read(bytes.as_mut()).await?;
        if read == 0 {
            return Ok(());
        }
        decoder
            .push(&bytes[..read], |message| messages.push(message))
            .map_err(|error| ServerError::Activation(error.to_string()))?;
        messages.reverse();
    }
}

async fn write_control_response(
    stream: &mut UnixStream,
    response: &ControlResponse,
) -> Result<(), ServerError> {
    let frame = muxe_protocol::control::encode_broker_control_response(response)
        .map_err(|error| ServerError::Activation(error.to_string()))?;
    stream.write_all(&frame).await?;
    stream.flush().await?;
    Ok(())
}

async fn process_frames(
    broker: &Arc<Broker>,
    activation: Option<&Arc<ActivationController>>,
    outbox: &mpsc::Sender<WireMessage>,
    resources: &mut ConnectionResources,
    frames: Vec<ArchivedFrame>,
) -> Result<(), ServerError> {
    for frame in frames {
        let message = frame.deserialize()?;
        match message {
            WireMessage::Hello { request_id, hello } => {
                if resources.handshaken || !broker.serves_identity(&hello.live_server).await? {
                    return Err(ServerError::IdentityMismatch);
                }
                resources.handshaken = true;
                outbox
                    .send(WireMessage::Welcome {
                        request_id,
                        welcome: muxe_protocol::Welcome {
                            broker_version: env!("CARGO_PKG_VERSION").to_owned(),
                            live_server: broker.live_identity().await?,
                            accepted_frame_len: muxe_protocol::MAX_FRAME_LEN,
                        },
                    })
                    .await
                    .map_err(|_| ServerError::WriterClosed)?;
            }
            WireMessage::Request {
                request_id,
                request,
            } => {
                serve_request_frame(broker, activation, outbox, resources, request_id, request)
                    .await?;
            }
            WireMessage::Welcome { .. }
            | WireMessage::Response { .. }
            | WireMessage::Event { .. } => return Err(ServerError::UnexpectedMessage),
        }
    }
    Ok(())
}

fn serve_pending_attachment(
    broker: &Arc<Broker>,
    outbox: &mpsc::Sender<WireMessage>,
    resources: &mut ConnectionResources,
    tracking: &ClientRequest,
    request_id: RequestId,
    pending: Box<PendingAttachment>,
) -> Result<(), ServerError> {
    let pending_token = pending.token();
    let attachment = resources
        .record_pending_attachment(tracking, pending.session().clone())
        .map_err(|_| ServerError::UnexpectedMessage)?;
    let outbox = outbox.clone();
    let broker = Arc::clone(broker);
    #[cfg(test)]
    let publication_hook = resources.publication_hook.clone();
    resources.waiter = Some(tokio::spawn(async move {
        // Centralize the pending-result transition on the shared
        // attachment phase: the waiter owns Pending→Attached/Closed,
        // disconnect owns *→Closed, and both are atomic under the
        // attachment mutex. A commit that publishes the session before
        // disconnect runs still converges through `disconnect_gated`,
        // which classifies pending-vs-attached under the broker lock.
        let response = (*pending).wait().await;
        // A gated waiter always answers its connection: success
        // publishes UiAttached, while any other readiness outcome
        // (abort, expiry, detach) still delivers its Error frame so
        // the connection never hangs in `request_frame`. Only the
        // success path transitions Pending->Attached and confirms
        // the gated attachment; disconnect wins by taking the phase
        // first, in which case this waiter owns nothing and sends
        // nothing.
        let (published_session, send_response) = {
            let mut attachment = attachment
                .lock()
                .expect("connection attachment is not poisoned");
            // Only the waiter that still observes Pending transitions
            // the phase. When disconnect already took Closed, this
            // waiter owns nothing and must not enqueue a stale
            // UiAttached for a session it no longer owns.
            if let UiAttachment::Pending { session, .. } = &*attachment {
                let session = session.clone();
                if let BrokerResponse::UiAttached { .. } = &response {
                    *attachment = UiAttachment::Attached {
                        session: session.clone(),
                    };
                    (Some(session), true)
                } else {
                    *attachment = UiAttachment::Closed;
                    (None, true)
                }
            } else {
                (None, false)
            }
        };
        if !send_response {
            return;
        }
        if let Some(session) = published_session {
            #[cfg(test)]
            if let Some(hook) = publication_hook {
                hook.entered.notify_one();
                hook.release.notified().await;
            }
            if outbox
                .send(WireMessage::Response {
                    request_id,
                    response,
                })
                .await
                .is_ok()
            {
                broker
                    .confirm_gated_attachment(pending_token, &session)
                    .await;
            }
        } else if outbox
            .send(WireMessage::Response {
                request_id,
                response,
            })
            .await
            .is_err()
        {
            // The connection is gone; the phase is already Closed and
            // the gated token's cleanup already ran when readiness
            // failed, so there is nothing left to confirm.
        }
    }));
    Ok(())
}
fn serve_placement_wait(
    broker: &Arc<Broker>,
    outbox: &mpsc::Sender<WireMessage>,
    resources: &mut ConnectionResources,
    request_id: RequestId,
    token: PendingLaunchToken,
) {
    let attachment = Arc::clone(&resources.attachment);
    *attachment
        .lock()
        .expect("connection attachment is not poisoned") = UiAttachment::WaitingPlacement;
    let broker = Arc::clone(broker);
    let outbox = outbox.clone();
    resources.placement_waiter = Some(tokio::spawn(async move {
        let response = match broker.wait_ui_placement(token).await {
            Ok(()) => BrokerResponse::Acknowledged,
            Err(error) => BrokerResponse::Error(error_diagnostic(&error)),
        };
        {
            let mut attachment = attachment
                .lock()
                .expect("connection attachment is not poisoned");
            if !matches!(*attachment, UiAttachment::WaitingPlacement) {
                return;
            }
            *attachment = if matches!(response, BrokerResponse::Acknowledged) {
                UiAttachment::Idle
            } else {
                UiAttachment::Closed
            };
        }
        let _ = outbox
            .send(WireMessage::Response {
                request_id,
                response,
            })
            .await;
    }));
}

/// Serves one validated client request frame: handshake gate, attachment gate,
/// ownership check, broker dispatch, and the immediate or pending response.
async fn serve_request_frame(
    broker: &Arc<Broker>,
    activation: Option<&Arc<ActivationController>>,
    outbox: &mpsc::Sender<WireMessage>,
    resources: &mut ConnectionResources,
    request_id: RequestId,
    request: ClientRequest,
) -> Result<(), ServerError> {
    if !resources.handshaken {
        return Err(ServerError::UnexpectedMessage);
    }
    if !resources.request_ids.insert(request_id) {
        return Err(ServerError::DuplicateRequestId);
    }
    let attachment_allowed = match activation {
        Some(activation) if matches!(request, ClientRequest::AttachUi(_)) => {
            activation.allows_ui().await
        }
        _ => true,
    };
    if !attachment_allowed {
        outbox
            .send(WireMessage::Response {
                request_id,
                response: activation_in_progress(),
            })
            .await
            .map_err(|_| ServerError::WriterClosed)?;
        return Ok(());
    }
    if let Err(message) = resources.validate_request(&request) {
        outbox
            .send(WireMessage::Response {
                request_id,
                response: ownership_error(message),
            })
            .await
            .map_err(|_| ServerError::WriterClosed)?;
        return Ok(());
    }
    let tracking = request.clone();
    match broker.handle(resources.role, request, outbox.clone()).await {
        Ok(RequestResult::Immediate(response)) => {
            let gated_confirmation = match (&tracking, &response) {
                (ClientRequest::AttachUi(request), BrokerResponse::UiAttached { session, .. }) => {
                    request.pending_launch.map(|token| (token, session.clone()))
                }
                _ => None,
            };
            resources.record_response(&tracking, &response);
            outbox
                .send(WireMessage::Response {
                    request_id,
                    response,
                })
                .await
                .map_err(|_| ServerError::WriterClosed)?;
            if let Some((token, session)) = gated_confirmation {
                broker.confirm_gated_attachment(token, &session).await;
            }
        }
        Ok(RequestResult::WaitForAttachment(pending)) => {
            serve_pending_attachment(broker, outbox, resources, &tracking, request_id, pending)?;
        }
        Ok(RequestResult::WaitForPlacement(token)) => {
            serve_placement_wait(broker, outbox, resources, request_id, token);
        }

        Err(error) => {
            outbox
                .send(WireMessage::Response {
                    request_id,
                    response: BrokerResponse::Error(error_diagnostic(&error)),
                })
                .await
                .map_err(|_| ServerError::WriterClosed)?;
        }
    }
    Ok(())
}

async fn disconnect_resources(broker: &Arc<Broker>, resources: &mut ConnectionResources) {
    if let Some(waiter) = resources.placement_waiter.take() {
        waiter.abort();
        let _ = waiter.await;
    }
    if let Some(waiter) = resources.waiter.take() {
        waiter.abort();
        let _ = waiter.await;
    }
    // Take the attachment phase to Closed atomically: the waiter transitions
    // Pending forward exactly once, disconnect transitions anything to Closed
    // exactly once, and `disconnect_gated` classifies the taken
    // (token, session) pair under the broker lock so a commit racing
    // disconnect converges on exactly one cleanup branch.
    let attachment = {
        let mut attachment = resources
            .attachment
            .lock()
            .expect("connection attachment is not poisoned");
        std::mem::replace(&mut *attachment, UiAttachment::Closed)
    };
    match attachment {
        UiAttachment::Pending { token, session } => {
            broker.disconnect_gated(token, session).await;
        }
        UiAttachment::Attached { session } => {
            broker.disconnect(Some(&session)).await;
        }
        UiAttachment::WaitingPlacement | UiAttachment::Idle | UiAttachment::Closed => {}
    }
    for token in resources.launcher_tokens.drain() {
        let _ = broker.abort_on_launcher_disconnect(token).await;
    }
}

fn verify_same_user_peer(stream: &UnixStream) -> Result<(), ServerError> {
    let peer = stream.peer_cred()?;
    (peer.uid() == Uid::current().as_raw())
        .then_some(())
        .ok_or(ServerError::PeerCredential)
}

fn ownership_error(message: &str) -> BrokerResponse {
    BrokerResponse::Error(muxe_protocol::ProtocolDiagnostic {
        code: muxe_protocol::DiagnosticCode::ProtocolViolation,
        message: message.to_owned(),
    })
}

fn activation_in_progress() -> BrokerResponse {
    BrokerResponse::Error(muxe_protocol::ProtocolDiagnostic {
        code: muxe_protocol::DiagnosticCode::ActivationInProgress,
        message: "activation is in progress; this broker is not accepting UI attachments"
            .to_owned(),
    })
}

async fn write_message(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    message: &WireMessage,
) -> Result<(), io::Error> {
    let frame = encode_frame(message).map_err(io::Error::other)?;
    writer.write_all(frame.prefix()).await?;
    writer.write_all(frame.payload()).await
}

fn error_diagnostic(error: &BrokerError) -> muxe_protocol::ProtocolDiagnostic {
    let mut message = match error {
        BrokerError::Configuration(config) => match config.as_ref() {
            crate::ConfigError::Diagnostic(report) | crate::ConfigError::Diagnostics(report) => {
                let mut summary = String::new();
                for (index, diagnostic) in report.diagnostics.iter().enumerate() {
                    if index > 0 {
                        summary.push_str("; ");
                    }
                    write!(
                        summary,
                        "[{}] {}",
                        diagnostic.code.as_str(),
                        diagnostic.message
                    )
                    .expect("writing to a String cannot fail");
                }
                summary
            }
            _ => error.to_string(),
        },
        _ => error.to_string(),
    };
    // Rich local reports contain source excerpts and line breaks. Wire
    // diagnostics are single-line text, including when an external cause
    // supplies a control character.
    if message.chars().any(char::is_control) {
        message = message
            .chars()
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect();
    }
    muxe_protocol::truncate_utf8(&mut message, muxe_protocol::MAX_DIAGNOSTIC_LEN);
    muxe_protocol::ProtocolDiagnostic {
        code: match error {
            BrokerError::ActivationInProgress => {
                muxe_protocol::DiagnosticCode::ActivationInProgress
            }
            BrokerError::NativeCompatibility(_) => muxe_protocol::DiagnosticCode::ActionBlocked,
            _ => muxe_protocol::DiagnosticCode::InvalidRequest,
        },
        message,
    }
}

pub struct ConfigWatch {
    _watcher: Option<notify::RecommendedWatcher>,
    task: Option<JoinHandle<()>>,
}

impl ConfigWatch {
    async fn start(broker: Arc<Broker>) -> Result<Self, ServerError> {
        let mut spec = broker.config_watch_spec().await;
        if !spec.settings.watch {
            return Ok(Self {
                _watcher: None,
                task: None,
            });
        }
        let (change_tx, mut changes) = mpsc::channel(8);
        let watched_inputs = spec
            .inputs
            .iter()
            .map(|path| canonical_watch_path(path))
            .collect::<Vec<_>>();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
                Ok(event) => {
                    if event.paths.iter().any(|changed_path| {
                        let changed_path = canonical_watch_path(changed_path);
                        watched_inputs.iter().any(|input| {
                            input == &changed_path
                                || changed_path.starts_with(input)
                                || input.starts_with(&changed_path)
                        })
                    }) {
                        let _ = change_tx.try_send(());
                    }
                }
                Err(error) => tracing::warn!(%error, "configuration watcher failed"),
            })
            .map_err(ServerError::Watch)?;
        watcher
            .watch(&spec.root, RecursiveMode::Recursive)
            .map_err(ServerError::Watch)?;
        let task = tokio::spawn(async move {
            while changes.recv().await.is_some() {
                tokio::time::sleep(spec.settings.debounce).await;
                while changes.try_recv().is_ok() {}
                match broker.reload().await {
                    Ok(_) => {
                        spec = broker.config_watch_spec().await;
                        if !spec.settings.watch {
                            return;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "configuration reload rejected; keeping active generation");
                    }
                }
            }
        });
        Ok(Self {
            _watcher: Some(watcher),
            task: Some(task),
        })
    }
}

impl Drop for ConfigWatch {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn canonical_watch_path(path: &std::path::Path) -> std::path::PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| {
        path.parent()
            .and_then(|parent| fs::canonicalize(parent).ok())
            .zip(path.file_name())
            .map_or_else(|| path.to_path_buf(), |(parent, name)| parent.join(name))
    })
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("runtime endpoint error: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("Unix socket I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid IPC frame: {0}")]
    Decode(#[from] DecodeError),
    #[error("broker operation failed: {0}")]
    Broker(#[from] BrokerError),
    #[error("client role {0:?} cannot connect to the broker socket")]
    UnsupportedRole(PeerRole),
    #[error("client live-host identity does not match the active broker adapter")]
    IdentityMismatch,
    #[error("client sent a broker-to-peer message")]
    UnexpectedMessage,
    #[error("broker socket peer credentials do not belong to the current user")]
    PeerCredential,
    #[error("client reused a request ID on one connection")]
    DuplicateRequestId,
    #[error("activation coordinator reused a control request ID on one connection")]
    DuplicateControlRequestId,
    #[error("client connection closed while response was queued")]
    WriterClosed,
    #[error("cannot watch configuration path {0}")]
    WatchPath(std::path::PathBuf),

    #[error("configuration watcher failed: {0}")]
    Watch(#[source] notify::Error),

    #[error("activation control cannot proceed: {0}")]
    Activation(String),
}
#[cfg(test)]
mod tests {
    use crate::{BrokerClient, ClientError, broker::WaitHook};
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use async_trait::async_trait;
    use muxe_adapter_api::{
        AdapterCapabilities, AdapterError, AdapterHealthEvent, CaptureLease, CaptureReleaseReason,
        CaptureRequest, DispatchAccepted, DispatchCompletion, ExecutionCorrelationId, HostAdapter,
        HostIdentity, KeyboardCapabilities, ModalScopeId, NativeDispatchRequest,
        OriginCaptureRequest, PendingPaneRegistration, PortableDispatchRequest,
    };
    use muxe_core::{
        ActionValidation, ActionValidator, CompiledGeneration, ConfigDiagnostic, KeyCapabilities,
        OriginContext, OriginHostKind, OriginInvocationSource, PaneId, ServerId, SourceId,
    };
    use muxe_protocol::{
        AttachUi, BrokerEvent, BrokerResponse, ClientRequest, ControlRequest, HostKind, HostPaneId,
        InvocationDisposition, LiveServerIdentity, PeerRole, Prelude, SchemaFingerprint,
        ServerId as WireServerId,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::UnixStream,
        sync::watch,
    };

    use super::*;

    fn decode_error_response(
        diagnostic: muxe_protocol::ProtocolDiagnostic,
    ) -> muxe_protocol::ProtocolDiagnostic {
        let welcome = WireMessage::Welcome {
            request_id: RequestId([1; 16]),
            welcome: muxe_protocol::Welcome {
                broker_version: "test".to_owned(),
                live_server: LiveServerIdentity {
                    host: HostKind::Herdr,
                    discovery_key: "owned-frame".to_owned(),
                    server_id: WireServerId::new("owned-server"),
                },
                accepted_frame_len: muxe_protocol::MAX_FRAME_LEN,
            },
        };
        let response = WireMessage::Response {
            request_id: RequestId([2; 16]),
            response: BrokerResponse::Error(diagnostic),
        };
        let mut bytes = Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application())
            .encode()
            .to_vec();
        for message in [welcome, response] {
            let frame = encode_frame(&message).unwrap();
            bytes.extend_from_slice(frame.prefix());
            bytes.extend_from_slice(frame.payload());
        }
        let mut decoder = ConnectionDecoder::new(ConnectionPolicy::client(
            PeerRole::Ui,
            SchemaFingerprint::application(),
        ));
        let mut responses = Vec::new();
        decoder
            .push(&bytes, |frame| responses.push(frame.deserialize().unwrap()))
            .unwrap();
        decoder.finish().unwrap();
        let WireMessage::Response {
            response: BrokerResponse::Error(diagnostic),
            ..
        } = responses.pop().unwrap()
        else {
            panic!("decoder must deliver the broker error");
        };
        diagnostic
    }

    #[test]
    fn control_failures_with_external_text_survive_encoding_and_decoding() {
        let raw = format!(
            "configuration.yml:7\ninvalid action\u{0}{}",
            "é".repeat(muxe_protocol::MAX_DIAGNOSTIC_LEN)
        );
        let response = ControlResponse {
            request_id: ControlRequestId([1; 16]),
            result: control_error(raw),
        };
        let mut bytes = Prelude::control(PeerRole::Broker).encode().to_vec();
        bytes.extend(muxe_protocol::control::encode_broker_control_response(&response).unwrap());
        let mut decoder = ControlDecoder::new(ControlPolicy::coordinator());
        let mut received = Vec::new();
        decoder
            .push(&bytes, |message| received.push(message))
            .unwrap();
        decoder.finish().unwrap();
        let ControlMessage::Response(ControlResponse {
            result:
                ControlResult::Error {
                    diagnostic,
                    prepare_refusal,
                },
            ..
        }) = received.pop().unwrap()
        else {
            panic!("control failure must remain an error response");
        };
        assert!(diagnostic.contains("configuration.yml:7"));
        assert!(!diagnostic.chars().any(char::is_control));
        assert!(diagnostic.len() <= muxe_protocol::MAX_DIAGNOSTIC_LEN);
        assert_eq!(prepare_refusal, None);
    }

    #[test]
    fn configuration_errors_survive_the_public_wire_decoder() {
        let directory = tempfile::tempdir().unwrap();
        let config_path = directory.path().join("config.yml");
        let long_action = format!("unknown:{}", "é".repeat(muxe_protocol::MAX_DIAGNOSTIC_LEN));
        for yaml in [
            "menus: [unterminated\n".to_owned(),
            "version: 1\ntheme: missing-theme\nmenus: {main: {bindings: {}}}\n".to_owned(),
            format!(
                "version: 1\nmenus:\n  main:\n    bindings:\n      a: {{action: '{long_action}'}}\n"
            ),
        ] {
            std::fs::write(&config_path, yaml).unwrap();
            let error = crate::load_effective_config(
                &config_path,
                "herdr.yml",
                KeyCapabilities::default(),
                &SmokeAdapter::new(),
            )
            .unwrap_err();
            let (crate::ConfigError::Diagnostic(report) | crate::ConfigError::Diagnostics(report)) =
                &error
            else {
                panic!("fixture must produce structured configuration diagnostics");
            };
            assert!(error.to_string().contains('\n'));
            let code = report.diagnostics[0].code.as_str();
            let diagnostic = decode_error_response(error_diagnostic(&BrokerError::Configuration(
                Box::new(error),
            )));
            assert_eq!(
                diagnostic.code,
                muxe_protocol::DiagnosticCode::InvalidRequest
            );
            assert!(diagnostic.message.contains(code));
            assert!(!diagnostic.message.chars().any(char::is_control));
            assert!(diagnostic.message.len() <= muxe_protocol::MAX_DIAGNOSTIC_LEN);
        }
    }

    #[test]
    fn detached_response_closes_attachment_ownership() {
        let mut resources = ConnectionResources::new(PeerRole::Ui);
        let session = muxe_protocol::UiSessionId::new("ui-detached");
        *resources
            .attachment
            .lock()
            .expect("attachment mutex is not poisoned") = UiAttachment::Attached {
            session: session.clone(),
        };
        let request = ClientRequest::DetachUi(muxe_protocol::DetachUi {
            session: session.clone(),
        });
        resources.record_response(&request, &BrokerResponse::Detached);
        assert!(matches!(resources.attachment(), UiAttachment::Closed));
        assert!(
            resources
                .validate_request(&ClientRequest::InvokeBinding(
                    muxe_protocol::InvokeBinding {
                        session,
                        generation: 1,
                        binding: muxe_protocol::BindingId {
                            generation: 1,
                            ordinal: 0,
                        },
                    }
                ))
                .is_err(),
            "detached UI cannot invoke through the old connection owner"
        );
        assert!(
            resources.validate_request(&request).is_err(),
            "detached UI cannot detach or be cleaned up a second time"
        );
    }

    #[tokio::test]
    async fn placement_wait_keeps_connection_responsive_and_disconnect_preserves_launch() {
        let (broker, _adapter, _directory) = ordering_broker(false, false);
        let (events, _events_rx) = mpsc::channel(8);
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::PrepareUiLaunch(launch_request()),
                events.clone(),
            )
            .await
            .expect("launcher prepares")
        else {
            panic!("expected launch token");
        };
        broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::RegisterPendingPane(muxe_protocol::RegisterPendingPane {
                    token,
                    pane: HostPaneId::new("owned-ui-pane"),
                    temporary_tab: None,
                }),
                events.clone(),
            )
            .await
            .expect("launcher registers its pane");
        let mut resources = ConnectionResources::new(PeerRole::Ui);
        resources.handshaken = true;
        let (outbox, mut responses) = mpsc::channel(8);
        tokio::time::timeout(
            Duration::from_secs(5),
            serve_request_frame(
                &broker,
                None,
                &outbox,
                &mut resources,
                RequestId([34; 16]),
                ClientRequest::WaitUiPlacement(muxe_protocol::WaitUiPlacement { token }),
            ),
        )
        .await
        .expect("waiting for placement must not block the connection reader")
        .expect("UI begins its placement wait");
        serve_request_frame(
            &broker,
            None,
            &outbox,
            &mut resources,
            RequestId([35; 16]),
            ClientRequest::Heartbeat,
        )
        .await
        .expect("same connection remains responsive before placement");
        assert!(matches!(
            responses.recv().await,
            Some(WireMessage::Response {
                request_id,
                response: BrokerResponse::Acknowledged,
            }) if request_id == RequestId([35; 16])
        ));
        tokio::time::timeout(
            Duration::from_secs(5),
            disconnect_resources(&broker, &mut resources),
        )
        .await
        .expect("disconnect cancels the placement wait without awaiting the lease");
        assert!(matches!(
            broker
                .handle(
                    PeerRole::Launcher,
                    ClientRequest::CommitUiLaunch(muxe_protocol::CommitUiLaunch {
                        token,
                        pane: HostPaneId::new("owned-ui-pane"),
                    }),
                    events,
                )
                .await
                .expect("read-only UI disconnect does not abort the launcher's token"),
            RequestResult::Immediate(BrokerResponse::Acknowledged)
        ));
    }

    #[tokio::test]
    async fn production_pending_waiter_disconnect_race_joins_waiter_and_cleans_once() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let commit_hook = Arc::new(WaitHook::new());
        broker.set_commit_ui_launch_hook(Some(Arc::clone(&commit_hook)));
        let (events, _events_rx) = mpsc::channel(8);
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::PrepareUiLaunch(launch_request()),
                events.clone(),
            )
            .await
            .expect("prepare launch token")
        else {
            panic!("prepare launch returns an immediate token");
        };
        broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::RegisterPendingPane(muxe_protocol::RegisterPendingPane {
                    token,
                    pane: HostPaneId::new("owned-ui-pane"),
                    temporary_tab: None,
                }),
                events,
            )
            .await
            .expect("register pending pane");

        let mut resources = ConnectionResources::new(PeerRole::Ui);
        resources.handshaken = true;
        let (outbox, mut responses) = mpsc::channel(8);
        serve_request_frame(
            &broker,
            None,
            &outbox,
            &mut resources,
            RequestId([1; 16]),
            ClientRequest::AttachUi(AttachUi {
                root: muxe_protocol::MenuId::named("main"),
                pane: HostPaneId::new("owned-ui-pane"),
                pending_launch: Some(token),
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }),
        )
        .await
        .expect("production AttachUi request enters readiness wait");
        assert!(
            resources.waiter.is_some(),
            "the production request retains its readiness waiter"
        );

        let commit = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move {
                broker
                    .handle(
                        PeerRole::Launcher,
                        ClientRequest::CommitUiLaunch(muxe_protocol::CommitUiLaunch {
                            token,
                            pane: HostPaneId::new("owned-ui-pane"),
                        }),
                        mpsc::channel(1).0,
                    )
                    .await
            }
        });
        commit_hook.entered.notified().await;
        disconnect_resources(&broker, &mut resources).await;
        commit_hook.release.notify_waiters();
        let _commit_result = commit.await.expect("commit task joins");

        assert!(
            resources.waiter.is_none(),
            "disconnect joins and clears the production readiness waiter"
        );
        assert!(
            matches!(
                responses.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "the losing waiter emits no stale UiAttached response"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.close_calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending-pane cleanup reaches the adapter");
        assert_eq!(
            adapter.close_calls.load(Ordering::SeqCst),
            1,
            "the commit/disconnect race cleans the pane exactly once"
        );
    }
    #[tokio::test]
    async fn gated_waiter_delivers_abort_error_when_connection_stays_alive() {
        // B1-a: a gated AttachUi waits for commit; aborting the token with
        // the connection alive must deliver exactly one Error(LaunchAborted)
        // response on the outbox (pre-fix the waiter swallowed it and the
        // connection hung in `request_frame` forever).
        let (broker, _adapter, _directory) = ordering_broker(false, false);
        let (events, _events_rx) = mpsc::channel(8);
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::PrepareUiLaunch(launch_request()),
                events.clone(),
            )
            .await
            .expect("prepare launch token")
        else {
            panic!("prepare launch returns an immediate token");
        };
        broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::RegisterPendingPane(muxe_protocol::RegisterPendingPane {
                    token,
                    pane: HostPaneId::new("owned-ui-pane"),
                    temporary_tab: None,
                }),
                events,
            )
            .await
            .expect("register pending pane");

        let mut resources = ConnectionResources::new(PeerRole::Ui);
        resources.handshaken = true;
        let (outbox, mut responses) = mpsc::channel(8);
        serve_request_frame(
            &broker,
            None,
            &outbox,
            &mut resources,
            RequestId([3; 16]),
            ClientRequest::AttachUi(AttachUi {
                root: muxe_protocol::MenuId::named("main"),
                pane: HostPaneId::new("owned-ui-pane"),
                pending_launch: Some(token),
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }),
        )
        .await
        .expect("gated AttachUi enters readiness wait");
        assert!(
            resources.waiter.is_some(),
            "gated request retains its readiness waiter"
        );
        // Abort the pending launch while the connection is alive: readiness
        // fails, and the waiter must answer with the Error frame.
        broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::AbortUiLaunch(muxe_protocol::AbortUiLaunch { token }),
                mpsc::channel(1).0,
            )
            .await
            .expect("abort owns the pending launch");
        let message = tokio::time::timeout(Duration::from_secs(1), responses.recv())
            .await
            .expect("waiter answers instead of hanging")
            .expect("outbox stays open");
        let WireMessage::Response {
            request_id,
            response,
        } = message
        else {
            panic!("waiter delivers a response frame");
        };
        assert_eq!(
            request_id,
            RequestId([3; 16]),
            "the error answers the gated request id"
        );
        match response {
            BrokerResponse::Error(diagnostic) => assert_eq!(
                diagnostic.code,
                muxe_protocol::DiagnosticCode::LaunchAborted,
                "aborted waiter fails with LaunchAborted"
            ),
            other => panic!("aborted waiter must deliver Error, got {other:?}"),
        }
        assert!(
            responses.try_recv().is_err(),
            "the gated request gets exactly one response"
        );
    }

    #[tokio::test]
    async fn late_attachment_publication_disconnect_claims_unconfirmed_pane_once() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let publication_hook = Arc::new(WaitHook::new());
        let (events, _events_rx) = mpsc::channel(8);
        let RequestResult::Immediate(BrokerResponse::LaunchPrepared { token, .. }) = broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::PrepareUiLaunch(launch_request()),
                events.clone(),
            )
            .await
            .expect("prepare launch token")
        else {
            panic!("prepare launch returns an immediate token");
        };
        broker
            .handle(
                PeerRole::Launcher,
                ClientRequest::RegisterPendingPane(muxe_protocol::RegisterPendingPane {
                    token,
                    pane: HostPaneId::new("owned-ui-pane"),
                    temporary_tab: None,
                }),
                events,
            )
            .await
            .expect("register pending pane");

        let mut resources = ConnectionResources::new(PeerRole::Ui);
        resources.handshaken = true;
        resources.publication_hook = Some(Arc::clone(&publication_hook));
        let (outbox, mut responses) = mpsc::channel(8);
        serve_request_frame(
            &broker,
            None,
            &outbox,
            &mut resources,
            RequestId([2; 16]),
            ClientRequest::AttachUi(AttachUi {
                root: muxe_protocol::MenuId::named("main"),
                pane: HostPaneId::new("owned-ui-pane"),
                pending_launch: Some(token),
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }),
        )
        .await
        .expect("production AttachUi request enters readiness wait");

        let commit = tokio::spawn({
            let broker = Arc::clone(&broker);
            async move {
                broker
                    .handle(
                        PeerRole::Launcher,
                        ClientRequest::CommitUiLaunch(muxe_protocol::CommitUiLaunch {
                            token,
                            pane: HostPaneId::new("owned-ui-pane"),
                        }),
                        mpsc::channel(1).0,
                    )
                    .await
            }
        });
        publication_hook.entered.notified().await;
        assert!(
            matches!(resources.attachment(), UiAttachment::Attached { .. }),
            "readiness waiter transitions to Attached before publication confirmation"
        );
        disconnect_resources(&broker, &mut resources).await;
        publication_hook.release.notify_waiters();
        let _commit_result = commit.await.expect("commit task joins");

        assert!(resources.waiter.is_none());
        assert!(
            matches!(
                responses.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "disconnect wins before publication and emits no stale UiAttached response"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while adapter.close_calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unconfirmed pending-pane cleanup reaches the adapter");
        assert_eq!(adapter.close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            adapter.release_calls.load(Ordering::SeqCst),
            0,
            "disconnect claims unconfirmed provenance instead of releasing it"
        );
    }

    struct SmokeAdapter {
        shutdown: AtomicBool,
        shutdown_wake: tokio::sync::Notify,
    }

    impl SmokeAdapter {
        fn new() -> Self {
            Self {
                shutdown: AtomicBool::new(false),
                shutdown_wake: tokio::sync::Notify::new(),
            }
        }
    }
    impl ActionValidator for SmokeAdapter {
        fn matches_host(&self, host: OriginHostKind) -> bool {
            host == OriginHostKind::Herdr
        }

        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _action_span: &muxe_core::SourceSpan,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
            Ok(vec![
                ActionValidation {
                    execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
                };
                candidates.len()
            ])
        }
    }

    #[async_trait]
    impl HostAdapter for SmokeAdapter {
        async fn identity(&self) -> Result<HostIdentity, AdapterError> {
            Ok(HostIdentity {
                kind: muxe_adapter_api::HostKind::Herdr,
                discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(
                    "owned-fake-host".to_owned(),
                )
                .expect("validated host discovery key"),
                live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
                    "owned-fake-server".to_owned(),
                )
                .expect("validated live server incarnation"),
            })
        }

        fn config_override_filename(&self) -> &'static str {
            "herdr.yml"
        }

        async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
            Ok(AdapterCapabilities {
                keyboard: KeyboardCapabilities {
                    kitty_baseline: false,
                    kitty_event_types: false,
                    kitty_alternate_keys: false,
                    kitty_all_keys_as_escape_codes: false,
                },
                supports_capture: false,
                supports_notifications: false,
                supports_native_cancellation: false,
            })
        }

        async fn modal_scope(&self, _ui_pane: &PaneId) -> Result<ModalScopeId, AdapterError> {
            Ok(ModalScopeId::new("owned-fake-scope"))
        }

        async fn begin_capture(
            &self,
            _request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            unreachable!("smoke adapter declares no capture support")
        }

        async fn end_capture(
            &self,
            _lease: CaptureLease,
            _reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn register_pending_pane(
            &self,
            registration: PendingPaneRegistration,
        ) -> Result<muxe_adapter_api::PendingPaneLease, AdapterError> {
            Ok(muxe_adapter_api::PendingPaneLease {
                id: muxe_adapter_api::PendingPaneLeaseId::new(format!(
                    "smoke:{}",
                    registration.ui_session
                )),
                ui_session: registration.ui_session,
            })
        }

        async fn close_pending_pane(
            &self,
            _registration: PendingPaneRegistration,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        fn release_pending_pane(&self, _lease: muxe_adapter_api::PendingPaneLease) {}

        async fn capture_origin(
            &self,
            _request: OriginCaptureRequest,
        ) -> Result<OriginContext, AdapterError> {
            Ok(OriginContext {
                host_kind: OriginHostKind::Herdr,

                server_id: ServerId::new("owned-fake-server"),
                client_id: None,
                session_id: None,
                workspace_id: None,
                tab_id: None,
                tab_index: None,
                pane_id: Some(PaneId::new("owned-ui-pane")),
                pane_type: None,
                pane_cwd: None,
                selection_text: None,
                invocation_source: OriginInvocationSource::RootBinding,
                worktree_id: None,
                worktree_path: None,
                agent_id: None,
                link_url: None,
                link_handler_id: None,
            })
        }

        async fn dispatch_portable(
            &self,
            request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("owned-fake-portable"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn dispatch_native(
            &self,
            request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("owned-fake-native"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn cancel(&self, _execution: muxe_core::ExecutionId) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Shutdown,
                    "smoke fake host is shut down",
                ));
            }
            self.shutdown_wake.notified().await;
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Shutdown,
                "smoke fake host is shut down",
            ))
        }

        async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            self.shutdown.store(true, Ordering::Relaxed);
            self.shutdown_wake.notify_waiters();
            Ok(())
        }
    }
    struct OrderingAdapter {
        calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        // Counts real adapter cleanup calls in the production race regression.
        close_calls: AtomicUsize,
        release_calls: AtomicUsize,
        suspend_unsupported: bool,
        resume_fails: bool,
        readiness: std::sync::Mutex<Option<muxe_adapter_api::ActivationReadiness>>,
        current_identity: tokio::sync::RwLock<Option<HostIdentity>>,
        config_override_filename: &'static str,
        shutdown: AtomicBool,
        shutdown_wake: tokio::sync::Notify,
        block_shutdown: AtomicBool,
        shutdown_error: AtomicBool,
        shutdown_entered: tokio::sync::Notify,
        shutdown_release: tokio::sync::Notify,
        queued_events: std::sync::Mutex<VecDeque<AdapterHealthEvent>>,
        observed_events: AtomicUsize,
    }

    impl OrderingAdapter {
        fn record(&self, call: &str) {
            self.calls
                .lock()
                .expect("activation call log is readable")
                .push(call.to_owned());
        }

        fn calls(&self) -> Vec<String> {
            self.calls
                .lock()
                .expect("activation call log is readable")
                .clone()
        }

        fn set_readiness(&self, readiness: Option<muxe_adapter_api::ActivationReadiness>) {
            *self.readiness.lock().expect("readiness script is writable") = readiness;
        }

        fn queue_event(&self, event: AdapterHealthEvent) {
            self.queued_events
                .lock()
                .expect("health queue is writable")
                .push_back(event);
        }

        fn observed_events(&self) -> usize {
            self.observed_events.load(Ordering::SeqCst)
        }
    }
    impl ActionValidator for OrderingAdapter {
        fn matches_host(&self, host: OriginHostKind) -> bool {
            host == OriginHostKind::Herdr
        }

        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _action_span: &muxe_core::SourceSpan,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
            Ok(vec![
                ActionValidation {
                    execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
                };
                candidates.len()
            ])
        }
    }

    #[async_trait]
    impl HostAdapter for OrderingAdapter {
        async fn identity(&self) -> Result<HostIdentity, AdapterError> {
            self.current_identity.read().await.clone().ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "fresh membership coverage is pending",
                )
            })
        }

        fn config_override_filename(&self) -> &'static str {
            self.config_override_filename
        }

        async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
            Ok(AdapterCapabilities {
                keyboard: KeyboardCapabilities {
                    kitty_baseline: false,
                    kitty_event_types: false,
                    kitty_all_keys_as_escape_codes: false,
                    kitty_alternate_keys: false,
                },
                supports_capture: false,
                supports_notifications: false,
                supports_native_cancellation: false,
            })
        }

        async fn modal_scope(&self, _ui_pane: &PaneId) -> Result<ModalScopeId, AdapterError> {
            Ok(ModalScopeId::new("owned-fake-scope"))
        }

        async fn begin_capture(
            &self,
            _request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            unreachable!("ordering adapter declares no capture support")
        }

        async fn end_capture(
            &self,
            _lease: CaptureLease,
            _reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn register_pending_pane(
            &self,
            registration: PendingPaneRegistration,
        ) -> Result<muxe_adapter_api::PendingPaneLease, AdapterError> {
            Ok(muxe_adapter_api::PendingPaneLease {
                id: muxe_adapter_api::PendingPaneLeaseId::new(format!(
                    "ordering:{}",
                    registration.ui_session
                )),
                ui_session: registration.ui_session,
            })
        }

        async fn close_pending_pane(
            &self,
            _registration: PendingPaneRegistration,
            _lease: muxe_adapter_api::PendingPaneLease,
        ) -> Result<(), AdapterError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn release_pending_pane(&self, _lease: muxe_adapter_api::PendingPaneLease) {
            self.release_calls.fetch_add(1, Ordering::SeqCst);
        }

        async fn capture_origin(
            &self,
            _request: OriginCaptureRequest,
        ) -> Result<OriginContext, AdapterError> {
            Ok(OriginContext {
                host_kind: OriginHostKind::Herdr,
                server_id: ServerId::new("owned-fake-server"),
                client_id: None,
                session_id: None,
                workspace_id: None,
                tab_id: None,
                tab_index: None,
                pane_id: Some(PaneId::new("owned-ui-pane")),
                pane_type: None,
                pane_cwd: None,
                selection_text: None,
                invocation_source: OriginInvocationSource::RootBinding,
                worktree_id: None,
                worktree_path: None,
                agent_id: None,
                link_url: None,
                link_handler_id: None,
            })
        }

        async fn dispatch_portable(
            &self,
            request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("owned-fake-portable"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn dispatch_native(
            &self,
            request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Ok(DispatchAccepted {
                correlation: ExecutionCorrelationId::new("owned-fake-native"),
                execution: request.execution,
                capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        async fn cancel(&self, _execution: muxe_core::ExecutionId) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
            let queued = self
                .queued_events
                .lock()
                .expect("health queue is readable")
                .pop_front();
            if let Some(event) = queued {
                self.observed_events.fetch_add(1, Ordering::SeqCst);
                return Ok(event);
            }
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Shutdown,
                    "ordering fake host is shut down",
                ));
            }
            self.shutdown_wake.notified().await;
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Shutdown,
                "ordering fake host is shut down",
            ))
        }

        async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
            self.record("suspend");
            if self.suspend_unsupported {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unsupported,
                    "ordering fake host cannot suspend for activation",
                ));
            }
            Ok(())
        }

        async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
            self.record("resume");
            if self.resume_fails {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "ordering fake host lost its retained subscription",
                ));
            }
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            self.record("shutdown_begin");
            self.shutdown_entered.notify_one();
            if self.block_shutdown.load(Ordering::SeqCst) {
                self.shutdown_release.notified().await;
            }
            if self.shutdown_error.load(Ordering::SeqCst) {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "scripted old adapter shutdown failed",
                ));
            }
            self.shutdown.store(true, Ordering::Relaxed);
            self.shutdown_wake.notify_waiters();
            self.record("shutdown_complete");
            Ok(())
        }

        async fn activation_readiness(
            &self,
        ) -> Result<Option<muxe_adapter_api::ActivationReadiness>, AdapterError> {
            Ok(self
                .readiness
                .lock()
                .expect("readiness script is readable")
                .clone())
        }
    }

    fn test_record() -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: "test-0.0.0".to_owned(),
            target_triple: "test-triple".to_owned(),
            application_schema_fingerprint: SchemaFingerprint::application(),
            zellij: None,
            herdr: None,
        }
    }

    fn test_handoff() -> HandoffId {
        static NEXT: AtomicUsize = AtomicUsize::new(1);
        let value = NEXT.fetch_add(1, Ordering::Relaxed);
        HandoffId([u8::try_from(value % 255 + 1).expect("test handoff byte"); 16])
    }

    fn ordering_broker(
        suspend_unsupported: bool,
        resume_fails: bool,
    ) -> (Arc<Broker>, Arc<OrderingAdapter>, tempfile::TempDir) {
        ordering_broker_with_identity(
            suspend_unsupported,
            resume_fails,
            HostIdentity {
                kind: muxe_adapter_api::HostKind::Herdr,
                discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(
                    "owned-fake-host".to_owned(),
                )
                .expect("validated host discovery key"),
                live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
                    "owned-fake-server".to_owned(),
                )
                .expect("validated live server incarnation"),
            },
            "herdr.yml",
        )
    }

    fn ordering_broker_with_identity(
        suspend_unsupported: bool,
        resume_fails: bool,
        identity: HostIdentity,
        config_override_filename: &'static str,
    ) -> (Arc<Broker>, Arc<OrderingAdapter>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("owned activation runtime directory");
        let config_path = directory.path().join("config.yml");
        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
        )
        .expect("write owned activation config");
        let adapter = Arc::new(OrderingAdapter {
            calls: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            close_calls: AtomicUsize::new(0),
            release_calls: AtomicUsize::new(0),
            suspend_unsupported,
            resume_fails,
            readiness: std::sync::Mutex::new(None),
            current_identity: tokio::sync::RwLock::new(Some(identity)),
            config_override_filename,
            shutdown: AtomicBool::new(false),
            shutdown_wake: tokio::sync::Notify::new(),
            block_shutdown: AtomicBool::new(false),
            shutdown_error: AtomicBool::new(false),
            shutdown_entered: tokio::sync::Notify::new(),
            shutdown_release: tokio::sync::Notify::new(),
            queued_events: std::sync::Mutex::new(VecDeque::new()),
            observed_events: AtomicUsize::new(0),
        });
        let config_source =
            std::fs::read_to_string(&config_path).expect("read owned activation config");
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<owned activation>"),
            config_source,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("compile owned activation config");
        let broker = Broker::from_compiled(adapter.clone(), &config_path, config);
        (broker, adapter, directory)
    }

    async fn with_listener_commands(
        controller: &Arc<ActivationController>,
        calls: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) -> tokio::task::JoinHandle<()> {
        let (commands, mut commands_rx) = tokio::sync::mpsc::channel(8);
        controller.install_commands(commands).await;
        let calls = std::sync::Arc::clone(calls);
        tokio::spawn(async move {
            while let Some(command) = commands_rx.recv().await {
                match command {
                    ServerCommand::Drain { complete } => {
                        calls
                            .lock()
                            .expect("activation call log is writable")
                            .push("drain_listener".to_owned());
                        let _ = complete.send(Ok(()));
                    }
                    ServerCommand::Resume { complete } => {
                        calls
                            .lock()
                            .expect("activation call log is writable")
                            .push("resume_listener".to_owned());
                        let _ = complete.send(Ok(()));
                    }
                    ServerCommand::Stop { complete } => {
                        let (ticket, _released) = RetirementTicket::pair();
                        let _ = complete.send(Ok(ticket));
                        break;
                    }
                }
            }
        })
    }
    async fn running_unix_server() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        Arc<Broker>,
        RuntimeEndpoint,
        watch::Sender<bool>,
        tokio::task::JoinHandle<Result<(), ServerError>>,
    ) {
        let directory = tempfile::tempdir().expect("owned broker runtime directory");
        let config_path = directory.path().join("config.yml");
        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
        )
        .expect("write owned broker config");
        let adapter = Arc::new(SmokeAdapter::new());
        let config_source =
            std::fs::read_to_string(&config_path).expect("read owned broker config");
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<owned broker smoke>"),
            config_source,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("compile owned broker config");
        let broker = Broker::from_compiled(adapter, &config_path, config);
        // The endpoint lives outside the watched config tree: a recursive
        // notify watch over a directory containing a live Unix socket fails
        // on macOS, so the runtime directory is never the config directory.
        let runtime = tempfile::tempdir().expect("owned broker endpoint directory");
        let endpoint =
            RuntimeEndpoint::in_runtime_dir(runtime.path(), HostKind::Herdr, "owned-fake-host")
                .expect("derive owned endpoint");
        let server = BrokerServer::start(Arc::clone(&broker), endpoint.clone())
            .await
            .expect("start owned broker listener");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server_task = tokio::spawn(server.run(shutdown_rx));
        (
            directory,
            runtime,
            broker,
            endpoint,
            shutdown_tx,
            server_task,
        )
    }
    #[tokio::test]
    async fn activation_stop_joins_health_monitor_after_empty_or_queued_event() {
        for queued in [false, true] {
            let (broker, adapter, _config_dir) = ordering_broker(false, false);
            let runtime = tempfile::tempdir().expect("owned activation stop endpoint directory");
            let endpoint =
                RuntimeEndpoint::in_runtime_dir(runtime.path(), HostKind::Herdr, "owned-fake-host")
                    .expect("derive activation stop endpoint");
            if queued {
                adapter.queue_event(AdapterHealthEvent::DispatchCompleted(
                    DispatchCompletion::Succeeded {
                        execution: muxe_core::ExecutionId(999),
                    },
                ));
            }
            let server = BrokerServer::start_activation(
                Arc::clone(&broker),
                endpoint.clone(),
                ActivationBootstrap::Running {
                    current: test_record(),
                    bridge_unit: None,
                },
                None,
            )
            .await
            .expect("start activation stop server");
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let run_task = tokio::spawn(server.run(shutdown_rx));
            let mut control = ProductionControl::connect(endpoint.socket())
                .await
                .expect("connect activation coordinator");
            let retired = control
                .round_trip(ControlOperation::Retire)
                .await
                .expect("retire round trip");
            assert!(
                matches!(retired, ControlResult::Retired(_)),
                "retire returns a terminal status"
            );
            tokio::time::timeout(Duration::from_secs(2), run_task)
                .await
                .expect("activation stop returns")
                .expect("activation stop task joins")
                .expect("activation stop succeeds");
            assert_eq!(
                adapter.observed_events(),
                usize::from(queued),
                "queued terminal event is observed exactly once before stop"
            );
        }
    }

    #[tokio::test]
    async fn child_held_lock_binds_and_sibling_acquire_fails_fast() {
        let directory = tempfile::tempdir().expect("owned broker runtime directory");
        let config_path = directory.path().join("config.yml");
        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n",
        )
        .expect("write owned broker config");
        let adapter = Arc::new(SmokeAdapter::new());
        let config_source =
            std::fs::read_to_string(&config_path).expect("read owned broker config");
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<owned broker smoke>"),
            config_source,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("compile owned broker config");
        let broker = Broker::from_compiled(adapter, &config_path, config);
        // Isolated runtime directory, as above: the watched config tree must
        // not contain the live endpoint socket.
        let runtime = tempfile::tempdir().expect("owned broker endpoint directory");
        let endpoint =
            RuntimeEndpoint::in_runtime_dir(runtime.path(), HostKind::Herdr, "owned-fake-host")
                .expect("derive owned endpoint");
        // The child acquires before its pre-bind work: a sibling racing the
        // adapter-connect window fails at acquire time instead of overlapping.
        let lock = endpoint
            .acquire_startup_lock()
            .expect("child pre-acquires startup lock");
        assert!(
            endpoint.acquire_startup_lock().is_err(),
            "a sibling child fails fast while the lock is held"
        );
        let server = BrokerServer::start_with_lock(Arc::clone(&broker), endpoint.clone(), lock)
            .await
            .expect("start with the child-held lock binds");
        UnixStream::connect(endpoint.socket())
            .await
            .expect("held-lock server bound the endpoint");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let server_task = tokio::spawn(server.run(shutdown_rx));
        shutdown_tx.send(true).expect("stop held-lock server");
        tokio::time::timeout(Duration::from_secs(2), server_task)
            .await
            .expect("held-lock server stops")
            .expect("held-lock server task does not panic")
            .expect("held-lock server exits cleanly");
    }

    async fn malformed_peer_is_closed(endpoint: &RuntimeEndpoint) {
        let mut malformed = UnixStream::connect(endpoint.socket())
            .await
            .expect("connect isolated malformed peer");
        malformed
            .write_all(&Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application()).encode())
            .await
            .expect("write malformed peer prelude");
        let mut broker_prelude = [0; muxe_protocol::PRELUDE_LEN];
        malformed
            .read_exact(&mut broker_prelude)
            .await
            .expect("read broker prelude before malformed frame");
        malformed
            .write_all(&0_u32.to_be_bytes())
            .await
            .expect("write invalid zero-length frame");
        let mut eof = [0; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), malformed.read(&mut eof))
                .await
                .expect("malformed peer is closed")
                .expect("read malformed peer closure"),
            0,
            "malformed connection closes only itself"
        );
    }

    async fn await_generation(broker: &Arc<Broker>, generation: CompiledGeneration) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if broker.generation().await == generation {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("recursive watcher publishes a new immutable generation");
    }

    async fn assert_retired_incarnation_rejected(
        endpoint: &RuntimeEndpoint,
        current: &LiveServerIdentity,
    ) {
        let retired = LiveServerIdentity {
            host: current.host,
            discovery_key: current.discovery_key.clone(),
            server_id: WireServerId::new("retired-incarnation"),
        };
        let Err(error) =
            BrokerClient::connect(endpoint.socket(), PeerRole::Ui, "owned-stale-ui", retired).await
        else {
            panic!("same discovery key with a retired incarnation must be rejected");
        };
        assert!(matches!(
            error,
            ClientError::ConnectionClosed | ClientError::IdentityMismatch
        ));
    }

    async fn attach_ui_and_reject_cross_session_detach(
        endpoint: &RuntimeEndpoint,
        identity: LiveServerIdentity,
    ) -> (BrokerClient, UiSessionId) {
        let mut client = BrokerClient::connect(
            endpoint.socket(),
            PeerRole::Ui,
            "owned-smoke-ui",
            identity.clone(),
        )
        .await
        .expect("connect over the owned broker socket");
        let attached = client
            .request(ClientRequest::AttachUi(AttachUi {
                root: muxe_protocol::MenuId::named("main"),
                pane: HostPaneId::new("owned-ui-pane"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }))
            .await
            .expect("attach fake-host UI over broker IPC");
        let BrokerResponse::UiAttached { session, .. } = attached else {
            panic!("owned fake-host UI must attach before terminal setup");
        };
        let mut intruder = BrokerClient::connect(
            endpoint.socket(),
            PeerRole::Ui,
            "owned-intruder-ui",
            identity,
        )
        .await
        .expect("connect second owned UI peer");
        assert!(matches!(
            intruder
                .request(ClientRequest::DetachUi(muxe_protocol::DetachUi {
                    session: session.clone(),
                }))
                .await
                .expect("cross-session request receives a local protocol error"),
            BrokerResponse::Error(muxe_protocol::ProtocolDiagnostic {
                code: muxe_protocol::DiagnosticCode::ProtocolViolation,
                ..
            })
        ));
        assert!(matches!(
            client
                .request(ClientRequest::Heartbeat)
                .await
                .expect("valid attached peer survives cross-session rejection"),
            BrokerResponse::Acknowledged
        ));
        drop(intruder);
        (client, session)
    }

    #[tokio::test]
    async fn unix_server_rejects_retired_identity_and_accepts_status_identity() {
        let (directory, _runtime, broker, endpoint, shutdown_tx, server_task) =
            running_unix_server().await;
        let config_path = directory.path().join("config.yml");
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("running controller captures broker identity");
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        let ControlResult::Status(status) = result else {
            panic!("running broker reports status, got {result:?}");
        };
        let client_identity = status.live_server;
        assert_retired_incarnation_rejected(&endpoint, &client_identity).await;
        let (mut client, session) =
            attach_ui_and_reject_cross_session_detach(&endpoint, client_identity).await;

        malformed_peer_is_closed(&endpoint).await;
        assert!(matches!(
            client
                .request(ClientRequest::Heartbeat)
                .await
                .expect("valid peer survives malformed peer closure"),
            BrokerResponse::Acknowledged
        ));
        assert!(matches!(
            client
                .request(ClientRequest::DetachUi(muxe_protocol::DetachUi { session }))
                .await
                .expect("detach over broker IPC"),
            BrokerResponse::Detached
        ));

        std::fs::write(
            &config_path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: updated\n        action: menu:quit\n",
        )
        .expect("replace watched owned broker config");
        await_generation(&broker, CompiledGeneration(2)).await;

        shutdown_tx.send(true).expect("stop owned broker listener");
        tokio::time::timeout(Duration::from_secs(1), server_task)
            .await
            .expect("owned broker server exits")
            .expect("broker task joins")
            .expect("broker server has no error");
        assert!(
            !endpoint.socket().exists(),
            "owned broker listener unlinks only its own endpoint on shutdown"
        );
    }
    #[tokio::test]
    async fn prepare_suspends_after_drain_and_abort_resumes_before_rebind() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        let responder = with_listener_commands(&controller, &adapter.calls).await;

        let (result, stop) = controller
            .handle(
                &broker,
                ControlOperation::Prepare {
                    target: Box::new(test_record()),
                    handoff_id: test_handoff(),
                },
            )
            .await;
        assert!(!stop, "prepare never stops the old broker service");
        let ControlResult::Prepared(status) = result else {
            panic!("prepare with a suspendable host must succeed, got {result:?}");
        };
        assert_eq!(
            adapter.calls(),
            vec!["suspend".to_owned(), "drain_listener".to_owned()],
            "the host subscription suspends after UI drain and before the endpoint release"
        );
        let handoff = status
            .handoff_id
            .expect("prepare reports the fresh nonzero handoff");

        let (retry, retry_stop) = controller
            .handle(
                &broker,
                ControlOperation::Prepare {
                    target: Box::new(test_record()),
                    handoff_id: handoff,
                },
            )
            .await;
        assert!(!retry_stop);
        assert!(
            matches!(
                retry,
                ControlResult::Prepared(ActivationStatus {
                    handoff_id: Some(reported),
                    ..
                }) if reported == handoff
            ),
            "an exact supplied Prepare retry is idempotent"
        );
        assert_eq!(
            adapter.calls(),
            vec!["suspend".to_owned(), "drain_listener".to_owned()],
            "an idempotent retry does not drain twice"
        );
        let (conflict, _) = controller
            .handle(
                &broker,
                ControlOperation::Prepare {
                    target: Box::new(test_record()),
                    handoff_id: test_handoff(),
                },
            )
            .await;
        assert!(
            matches!(conflict, ControlResult::Error { .. }),
            "a different handoff cannot adopt the active drain"
        );

        let (result, stop) = controller
            .handle(
                &broker,
                ControlOperation::Abort {
                    handoff_id: handoff,
                },
            )
            .await;
        assert!(
            !stop,
            "abort of a draining old broker never stops its service"
        );
        assert!(
            matches!(result, ControlResult::Aborted(_)),
            "abort with a resumable host must succeed, got {result:?}"
        );
        assert_eq!(
            adapter.calls(),
            vec![
                "suspend".to_owned(),
                "drain_listener".to_owned(),
                "resume".to_owned(),
                "resume_listener".to_owned(),
            ],
            "the old host subscription resumes and revalidates before the endpoint rebinds"
        );
        responder.abort();
    }

    #[tokio::test]
    async fn prepare_fails_closed_when_suspend_is_unsupported() {
        let (broker, adapter, _directory) = ordering_broker(true, false);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        let responder = with_listener_commands(&controller, &adapter.calls).await;

        let (result, stop) = controller
            .handle(
                &broker,
                ControlOperation::Prepare {
                    target: Box::new(test_record()),
                    handoff_id: test_handoff(),
                },
            )
            .await;
        assert!(!stop, "a failed prepare never stops the old broker service");
        let ControlResult::Error {
            prepare_refusal: None,
            ..
        } = result
        else {
            panic!("prepare with an unsuspendable host must fail closed, got {result:?}");
        };
        assert_eq!(
            adapter.calls(),
            vec!["suspend".to_owned()],
            "the endpoint is never released when suspend fails"
        );

        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        assert!(
            matches!(
                result,
                ControlResult::Status(ActivationStatus {
                    lifecycle: LifecycleState::Running,
                    ..
                })
            ),
            "the old broker stays running after a refused prepare, got {result:?}"
        );
        responder.abort();
    }

    #[tokio::test]
    async fn abort_keeps_the_endpoint_drained_when_resume_fails() {
        let (broker, adapter, _directory) = ordering_broker(false, true);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        let responder = with_listener_commands(&controller, &adapter.calls).await;

        let (result, _) = controller
            .handle(
                &broker,
                ControlOperation::Prepare {
                    target: Box::new(test_record()),
                    handoff_id: test_handoff(),
                },
            )
            .await;
        let ControlResult::Prepared(status) = result else {
            panic!("prepare with a suspendable host must succeed, got {result:?}");
        };
        let handoff = status
            .handoff_id
            .expect("prepare reports the fresh nonzero handoff");

        let (result, stop) = controller
            .handle(
                &broker,
                ControlOperation::Abort {
                    handoff_id: handoff,
                },
            )
            .await;
        assert!(!stop, "a failed abort never stops the old broker service");
        let ControlResult::Error { .. } = result else {
            panic!("abort with a lost host must not claim rollback, got {result:?}");
        };
        assert_eq!(
            adapter.calls(),
            vec![
                "suspend".to_owned(),
                "drain_listener".to_owned(),
                "resume".to_owned(),
            ],
            "the endpoint stays drained when resume fails; no rebind, no healthy claim"
        );

        // The state returns to Draining so the coordinator can retry Abort after
        // restoring the host instead of stranding the old broker.
        let (retry, _) = controller
            .handle(
                &broker,
                ControlOperation::Abort {
                    handoff_id: handoff,
                },
            )
            .await;
        assert!(
            matches!(retry, ControlResult::Error { .. }),
            "a retried abort still reports the unrestored host, got {retry:?}"
        );
        responder.abort();
    }

    struct ScriptedRecovery(std::sync::Mutex<RecoveryView>);

    impl RecoveryJournal for ScriptedRecovery {
        fn recovery_decision<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = RecoveryDecision> + Send + 'a>> {
            let view = *self.0.lock().expect("recovery script is readable");
            Box::pin(async move {
                if matches!(view.journal, RecoveryJournalStatus::Inconsistent) {
                    return RecoveryDecision::Preserve {
                        reason: "scripted inconsistent journal".to_owned(),
                    };
                }
                if matches!(view.journal, RecoveryJournalStatus::Absent) {
                    return RecoveryDecision::NoJournal;
                }
                if matches!(view.member, RecoveryMemberStatus::Ready) && view.target_live {
                    RecoveryDecision::TargetOwns {
                        recover_after: view.recover_after,
                        permit: None,
                    }
                } else {
                    RecoveryDecision::RestoreOld {
                        recover_after: view.recover_after,
                        permit: None,
                    }
                }
            })
        }
        fn authorize_target_commit<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            let view = *self.0.lock().expect("recovery script is readable");
            Box::pin(async move {
                if matches!(view.journal, RecoveryJournalStatus::Present)
                    && matches!(view.member, RecoveryMemberStatus::Ready)
                    && view.target_live
                {
                    Ok(())
                } else {
                    Err("scripted target Commit lacks original Ready authority".to_owned())
                }
            })
        }
        fn authorize_old_commit<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            let view = *self.0.lock().expect("recovery script is readable");
            Box::pin(async move {
                if matches!(view.journal, RecoveryJournalStatus::Present)
                    && matches!(view.member, RecoveryMemberStatus::Ready)
                    && view.target_live
                    && local_status.lifecycle == LifecycleState::Draining
                {
                    Ok(())
                } else {
                    Err("scripted old Commit lacks Ready authority".to_owned())
                }
            })
        }

        fn publish_old_retirement<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn absent_recovery() -> Arc<ScriptedRecovery> {
        Arc::new(ScriptedRecovery(std::sync::Mutex::new(
            RecoveryView::absent(),
        )))
    }

    fn launch_request() -> muxe_protocol::PrepareUiLaunch {
        muxe_protocol::PrepareUiLaunch {
            modal_scope: muxe_protocol::ModalScopeId::new("owned-fake-scope"),
            root: muxe_protocol::MenuId::named("main"),
            lease_millis: 60_000,
        }
    }

    async fn prepared_old(
        broker: &Broker,
        controller: &Arc<ActivationController>,
        adapter_calls: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) -> (HandoffId, tokio::task::JoinHandle<()>) {
        let responder = with_listener_commands(controller, adapter_calls).await;
        let (result, stop) = controller
            .handle(
                broker,
                ControlOperation::Prepare {
                    target: Box::new(test_record()),
                    handoff_id: test_handoff(),
                },
            )
            .await;
        assert!(!stop, "prepare never stops the old broker service");
        let ControlResult::Prepared(status) = result else {
            panic!("prepare with a suspendable host must succeed, got {result:?}");
        };
        let handoff = status.handoff_id.expect("prepare reports a handoff");
        // Admission is sealed while draining: no launch token crosses the boundary.
        assert!(
            matches!(
                broker
                    .handle(
                        PeerRole::Launcher,
                        ClientRequest::PrepareUiLaunch(launch_request()),
                        tokio::sync::mpsc::channel(1).0,
                    )
                    .await,
                Err(BrokerError::ActivationInProgress)
            ),
            "drained broker refuses new launches"
        );
        // The responder stays alive: recovery and abort rebind through it.
        (handoff, responder)
    }

    #[tokio::test]
    async fn disconnect_restores_old_broker_when_journal_absent() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        controller.set_recovery(absent_recovery()).await;
        let (handoff, responder) = prepared_old(&broker, &controller, &adapter.calls).await;

        // The last coordinator connection closes with no journal authorizing any
        // target: the old unit restores itself instead of stranding drained.
        Arc::clone(&controller)
            .control_disconnected(Arc::clone(&broker))
            .await;
        assert_eq!(
            adapter.calls(),
            vec![
                "suspend".to_owned(),
                "drain_listener".to_owned(),
                "resume".to_owned(),
                "resume_listener".to_owned(),
            ],
            "disconnect resumes the host before rebinding the endpoint"
        );
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        let ControlResult::Status(status) = result else {
            panic!("recovered broker reports status, got {result:?}");
        };
        assert_eq!(status.lifecycle, LifecycleState::Running);
        assert_eq!(status.handoff_id, None);
        assert_eq!(status.live_server.discovery_key, "owned-fake-host");
        // Admission reopens with Running: the handoff is over.
        let _ = handoff;
        assert!(
            broker
                .handle(
                    PeerRole::Launcher,
                    ClientRequest::PrepareUiLaunch(launch_request()),
                    tokio::sync::mpsc::channel(1).0,
                )
                .await
                .is_ok(),
            "recovered broker admits launches again"
        );
        responder.abort();
    }

    #[tokio::test]
    async fn disconnect_preserves_inconsistent_journal() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        controller
            .set_recovery(Arc::new(ScriptedRecovery(std::sync::Mutex::new(
                RecoveryView {
                    journal: RecoveryJournalStatus::Inconsistent,
                    member: RecoveryMemberStatus::Pending,
                    target_live: false,
                    recover_after: Duration::ZERO,
                },
            ))))
            .await;
        let (_, responder) = prepared_old(&broker, &controller, &adapter.calls).await;

        Arc::clone(&controller)
            .control_disconnected(Arc::clone(&broker))
            .await;
        assert_eq!(
            adapter.calls(),
            vec!["suspend".to_owned(), "drain_listener".to_owned()],
            "inconsistent journals are preserved, never auto-restored"
        );
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        assert!(
            matches!(
                result,
                ControlResult::Status(ActivationStatus {
                    lifecycle: LifecycleState::Draining,
                    ..
                })
            ),
            "inconsistent journal keeps the endpoint drained, got {result:?}"
        );
        responder.abort();
    }

    struct OldRetirementPermit {
        adapter: Arc<OrderingAdapter>,
        endpoint: RuntimeEndpoint,
        events: Arc<std::sync::Mutex<Vec<&'static str>>>,
        acknowledged: tokio::sync::Notify,
        fail_ack: AtomicBool,
    }

    impl RecoveryPermit for OldRetirementPermit {
        fn acknowledge<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            ack: RecoveryAck,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                assert_eq!(ack, RecoveryAck::OldCommitted);
                assert!(self.adapter.shutdown.load(Ordering::SeqCst));
                assert!(!self.endpoint.socket().exists());
                if self.fail_ack.load(Ordering::SeqCst) {
                    return Err("scripted journal ACK persistence failed".to_owned());
                }
                self.events.lock().expect("event trace").push("ack");
                self.acknowledged.notify_one();
                Ok(())
            })
        }
    }

    struct OldCommitRecovery {
        permit: Arc<OldRetirementPermit>,
        decisions: AtomicUsize,
        delayed: bool,
        fail_proof: AtomicBool,
    }

    impl RecoveryJournal for OldCommitRecovery {
        fn recovery_decision<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = RecoveryDecision> + Send + 'a>> {
            let first = self.decisions.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move {
                if self.delayed && first {
                    RecoveryDecision::RestoreOld {
                        recover_after: Duration::from_millis(1),
                        permit: None,
                    }
                } else {
                    RecoveryDecision::TargetOwns {
                        recover_after: Duration::ZERO,
                        permit: Some(self.permit.clone()),
                    }
                }
            })
        }

        fn authorize_target_commit<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Err("old broker cannot authorize target Commit".to_owned()) })
        }

        fn authorize_old_commit<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Err("recovery-only fixture has no normal Commit permit".to_owned()) })
        }

        fn publish_old_retirement<'a>(
            &'a self,
            _handoff: &'a HandoffId,
            _local_status: &'a ActivationStatus,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async move {
                assert!(self.permit.adapter.shutdown.load(Ordering::SeqCst));
                assert!(!self.permit.endpoint.socket().exists());
                if self.fail_proof.load(Ordering::SeqCst) {
                    return Err("scripted retirement proof persistence failed".to_owned());
                }
                self.permit
                    .events
                    .lock()
                    .expect("event trace")
                    .push("proof");
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn disconnected_old_stops_before_ack_in_both_recovery_watches() {
        for delayed in [false, true] {
            let (broker, adapter, _config) = ordering_broker(false, false);
            adapter.block_shutdown.store(true, Ordering::SeqCst);
            let runtime = tempfile::tempdir().expect("owned runtime");
            let endpoint =
                RuntimeEndpoint::in_runtime_dir(runtime.path(), HostKind::Herdr, "owned-fake-host")
                    .expect("owned endpoint");
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            let permit = Arc::new(OldRetirementPermit {
                adapter: adapter.clone(),
                endpoint: endpoint.clone(),
                events: events.clone(),
                acknowledged: tokio::sync::Notify::new(),
                fail_ack: AtomicBool::new(false),
            });
            let recovery = Arc::new(OldCommitRecovery {
                permit: permit.clone(),
                decisions: AtomicUsize::new(0),
                delayed,
                fail_proof: AtomicBool::new(false),
            });
            let server = BrokerServer::start_activation(
                broker.clone(),
                endpoint.clone(),
                ActivationBootstrap::Running {
                    current: test_record(),
                    bridge_unit: None,
                },
                Some(recovery.clone()),
            )
            .await
            .expect("start old broker");
            let controller = server.activation.clone().expect("controller");
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(server.run(shutdown_rx));
            let mut control = ProductionControl::connect(endpoint.socket())
                .await
                .expect("connect coordinator");
            let handoff = prepare_draining(&mut control, "prepare").await;
            let (unauthorized, stop) = controller
                .handle(
                    &broker,
                    ControlOperation::Commit {
                        handoff_id: handoff,
                    },
                )
                .await;
            assert!(matches!(unauthorized, ControlResult::Error { .. }));
            assert!(!stop, "an unauthorized old Commit cannot reach Stop");
            assert!(
                controller
                    .retire_old(&broker, test_handoff())
                    .await
                    .is_err()
            );
            drop(control);
            tokio::time::timeout(Duration::from_secs(5), adapter.shutdown_entered.notified())
                .await
                .expect("recovery reaches adapter shutdown");
            assert!(!adapter.shutdown.load(Ordering::SeqCst));
            assert!(events.lock().expect("event trace").is_empty());
            assert!(!endpoint.socket().exists());
            assert!(matches!(
                *controller.state.lock().await,
                ActivationState::Draining { handoff: current, .. } if current == handoff
            ));
            adapter.shutdown_release.notify_one();
            tokio::time::timeout(Duration::from_secs(5), permit.acknowledged.notified())
                .await
                .expect("old retirement ACK follows stop");
            assert_eq!(*events.lock().expect("event trace"), ["proof", "ack"]);
            let (repeat_ticket, proof) = controller
                .retire_old(&broker, handoff)
                .await
                .expect("exact repeated handoff reuses completed stop");
            assert!(repeat_ticket.is_none());
            assert!(proof.is_ok());
            assert!(
                controller
                    .retire_old(&broker, test_handoff())
                    .await
                    .is_err()
            );
            assert_eq!(
                adapter
                    .calls()
                    .iter()
                    .filter(|call| call.as_str() == "shutdown_begin")
                    .count(),
                1,
                "replay never sends Stop twice"
            );
            assert!(adapter.calls().iter().all(|call| call != "resume"));
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("old service exits after ACK")
                .expect("service joins")
                .expect("service succeeds");
        }
    }

    #[tokio::test]
    async fn old_recovery_never_acks_or_resumes_after_stop_or_persistence_failure() {
        for failure in ["stop", "proof", "ack"] {
            let (broker, adapter, _config) = ordering_broker(false, false);
            adapter
                .shutdown_error
                .store(failure == "stop", Ordering::SeqCst);
            let runtime = tempfile::tempdir().expect("owned runtime");
            let endpoint =
                RuntimeEndpoint::in_runtime_dir(runtime.path(), HostKind::Herdr, "owned-fake-host")
                    .expect("owned endpoint");
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            let permit = Arc::new(OldRetirementPermit {
                adapter: adapter.clone(),
                endpoint: endpoint.clone(),
                events: events.clone(),
                acknowledged: tokio::sync::Notify::new(),
                fail_ack: AtomicBool::new(failure == "ack"),
            });
            let recovery = Arc::new(OldCommitRecovery {
                permit,
                decisions: AtomicUsize::new(0),
                delayed: false,
                fail_proof: AtomicBool::new(failure == "proof"),
            });
            let server = BrokerServer::start_activation(
                broker.clone(),
                endpoint.clone(),
                ActivationBootstrap::Running {
                    current: test_record(),
                    bridge_unit: None,
                },
                Some(recovery),
            )
            .await
            .expect("start old broker");
            let controller = server.activation.clone().expect("controller");
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let task = tokio::spawn(server.run(shutdown_rx));
            let mut control = ProductionControl::connect(endpoint.socket())
                .await
                .expect("connect coordinator");
            let handoff = prepare_draining(&mut control, "prepare").await;
            drop(control);
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .expect("old service exits after failed barrier")
                .expect("service joins")
                .expect("service exits without rebind");
            assert!(!endpoint.socket().exists());
            assert!(adapter.calls().iter().all(|call| call != "resume"));
            assert!(
                events
                    .lock()
                    .expect("event trace")
                    .iter()
                    .all(|entry| *entry != "ack")
            );
            if failure == "stop" {
                assert!(matches!(
                    *controller.state.lock().await,
                    ActivationState::Draining { handoff: current, .. } if current == handoff
                ));
            } else {
                assert!(matches!(
                    *controller.state.lock().await,
                    ActivationState::SupervisorOnly { handoff: current } if current == handoff
                ));
            }
        }
    }
    #[tokio::test]
    async fn disconnect_restores_when_pre_ready_target_is_dead() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        controller
            .set_recovery(Arc::new(ScriptedRecovery(std::sync::Mutex::new(
                RecoveryView {
                    journal: RecoveryJournalStatus::Present,
                    member: RecoveryMemberStatus::Pending,
                    target_live: false,
                    recover_after: Duration::ZERO,
                },
            ))))
            .await;
        let (_, responder) = prepared_old(&broker, &controller, &adapter.calls).await;

        // Before a durable Ready decision, a failed target can still roll back.
        Arc::clone(&controller)
            .control_disconnected(Arc::clone(&broker))
            .await;
        assert_eq!(
            adapter.calls(),
            vec![
                "suspend".to_owned(),
                "drain_listener".to_owned(),
                "resume".to_owned(),
                "resume_listener".to_owned(),
            ],
            "a pre-Ready dead target cannot own the handoff; old broker restores"
        );
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        let ControlResult::Status(status) = result else {
            panic!("restored broker reports status, got {result:?}");
        };
        assert_eq!(status.lifecycle, LifecycleState::Running);
        responder.abort();
    }
    /// Before durable Ready, a unit-incomplete target stands down before
    /// either old broker restores its suspended adapter.
    #[tokio::test]
    async fn disconnect_restores_both_olds_before_ready_when_target_is_dead() {
        fn incomplete() -> Arc<ScriptedRecovery> {
            Arc::new(ScriptedRecovery(std::sync::Mutex::new(RecoveryView {
                journal: RecoveryJournalStatus::Present,
                member: RecoveryMemberStatus::Pending,
                target_live: false,
                recover_after: Duration::ZERO,
            })))
        }
        let (broker_a, adapter_a, _dir_a) = ordering_broker(false, false);
        let controller_a = ActivationController::start(
            &broker_a,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("old A starts");
        controller_a.set_recovery(incomplete()).await;
        let (_, responder_a) = prepared_old(&broker_a, &controller_a, &adapter_a.calls).await;
        let (broker_b, adapter_b, _dir_b) = ordering_broker(false, false);
        let controller_b = ActivationController::start(
            &broker_b,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("old B starts");
        controller_b.set_recovery(incomplete()).await;
        let (_, responder_b) = prepared_old(&broker_b, &controller_b, &adapter_b.calls).await;
        let (target_broker, target_adapter, _dir_t) = ordering_broker(false, false);
        let target = ActivationController::start(
            &target_broker,
            ActivationBootstrap::Target {
                current: test_record(),
                handoff: HandoffId([7; 16]),
                live_server: LiveServerIdentity {
                    host: HostKind::Herdr,
                    discovery_key: "owned-fake-host".to_owned(),
                    server_id: WireServerId::new("owned-fake-server"),
                },
                bridge_unit: None,
            },
        )
        .await
        .expect("live target B starts gated");
        target.set_recovery(incomplete()).await;
        // Live target stands down first; olds restore after without overlap.
        Arc::clone(&target)
            .control_disconnected(Arc::clone(&target_broker))
            .await;
        Arc::clone(&controller_a)
            .control_disconnected(Arc::clone(&broker_a))
            .await;
        Arc::clone(&controller_b)
            .control_disconnected(Arc::clone(&broker_b))
            .await;
        for (adapter, name) in [(&adapter_a, "A"), (&adapter_b, "B")] {
            assert_eq!(
                adapter.calls(),
                vec![
                    "suspend".to_owned(),
                    "drain_listener".to_owned(),
                    "resume".to_owned(),
                    "resume_listener".to_owned(),
                ],
                "old {name} restores on unit-incomplete, never stands down alone"
            );
        }
        assert!(
            target_adapter.calls().is_empty(),
            "retiring target touches no host adapter"
        );
        let (result, _) = target
            .handle(&target_broker, ControlOperation::Status)
            .await;
        assert!(
            matches!(
                result,
                ControlResult::Status(ActivationStatus {
                    lifecycle: LifecycleState::Retired,
                    ..
                })
            ),
            "live target retires for incomplete unit, got {result:?}"
        );
        responder_a.abort();
        responder_b.abort();
    }

    #[tokio::test]
    async fn disconnect_fails_closed_when_resume_fails() {
        let (broker, adapter, _directory) = ordering_broker(false, true);
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a running broker");
        controller.set_recovery(absent_recovery()).await;
        let (_, responder) = prepared_old(&broker, &controller, &adapter.calls).await;

        Arc::clone(&controller)
            .control_disconnected(Arc::clone(&broker))
            .await;
        assert_eq!(
            adapter.calls(),
            vec![
                "suspend".to_owned(),
                "drain_listener".to_owned(),
                "resume".to_owned(),
            ],
            "failed resume never rebinds or claims Running"
        );
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        assert!(
            matches!(
                result,
                ControlResult::Status(ActivationStatus {
                    lifecycle: LifecycleState::Draining,
                    ..
                })
            ),
            "failed resume keeps the endpoint drained, got {result:?}"
        );
        responder.abort();
    }

    #[tokio::test]
    async fn gated_target_reports_reserved_identity_only_until_coverage() {
        let identity = HostIdentity {
            kind: muxe_adapter_api::HostKind::Zellij,
            discovery_key: muxe_adapter_api::HostDiscoveryKey::parse("owned-fake-host".to_owned())
                .unwrap(),
            live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
                "owned-fake-server".to_owned(),
            )
            .unwrap(),
        };
        let (broker, adapter, _directory) =
            ordering_broker_with_identity(false, false, identity.clone(), "zellij.yml");
        *adapter.current_identity.write().await = None;
        adapter.set_readiness(Some(muxe_adapter_api::ActivationReadiness {
            registered_clients: Vec::new(),
            member_clients: Vec::new(),
        }));
        let reserved = LiveServerIdentity {
            host: HostKind::Zellij,
            discovery_key: "owned-fake-host".to_owned(),
            server_id: WireServerId::new("owned-fake-server"),
        };
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Target {
                current: test_record(),
                handoff: HandoffId([7; 16]),
                live_server: reserved.clone(),
                bridge_unit: None,
            },
        )
        .await
        .expect("uncovered target binds gated");
        let gated = controller.status(&broker).await.expect("gated status");
        assert_eq!(gated.phase, ActivationPhase::TargetGated);
        assert_eq!(gated.live_server, reserved);
        assert!(gated.ready.is_none());

        *adapter.current_identity.write().await = Some(identity.clone());
        let covered = controller.status(&broker).await.unwrap();
        assert_eq!(
            covered.live_server, reserved,
            "covered identity matches the reserved target"
        );
        assert!(covered.ready.is_some(), "coverage permits Ready evidence");
        let mut replaced = identity;
        replaced.live_server_id =
            muxe_adapter_api::LiveServerIncarnationId::parse("replaced-fake-server".to_owned())
                .unwrap();
        *adapter.current_identity.write().await = Some(replaced);
        assert!(
            controller.status(&broker).await.is_err(),
            "a different covered incarnation cannot claim the gated target"
        );
    }

    #[tokio::test]
    async fn disconnect_completes_ready_target() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let live_server = LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: "owned-fake-host".to_owned(),
            server_id: WireServerId::new("owned-fake-server"),
        };
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Target {
                current: test_record(),
                handoff: HandoffId([7; 16]),
                live_server,
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a gated target");
        controller
            .set_recovery(Arc::new(ScriptedRecovery(std::sync::Mutex::new(
                RecoveryView {
                    journal: RecoveryJournalStatus::Present,
                    member: RecoveryMemberStatus::Ready,
                    target_live: true,
                    recover_after: Duration::from_millis(50),
                },
            ))))
            .await;

        Arc::clone(&controller)
            .control_disconnected(Arc::clone(&broker))
            .await;
        assert!(
            adapter.calls().is_empty(),
            "target completion touches no host adapter"
        );
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        let ControlResult::Status(status) = result else {
            panic!("completed target reports status, got {result:?}");
        };
        assert_eq!(status.lifecycle, LifecycleState::Running);
        assert_eq!(status.handoff_id, Some(HandoffId([7; 16])));
    }

    fn drain_pipe<T>(pipe: T) -> tokio::task::JoinHandle<()>
    where
        T: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            let mut pipe = pipe;
            let mut buffer = [0u8; 1024];
            while tokio::io::AsyncReadExt::read(&mut pipe, &mut buffer)
                .await
                .is_ok_and(|read| read > 0)
            {}
        })
    }

    /// Owned live Herdr server for production proofs. Retained and reaped by the
    /// test; only this child is ever signalled. Approval-gated: without
    /// `MUXE_HERDR_TEST_BINARY` the tests below report skip and pass.
    struct OwnedHerdrServer {
        _temp: tempfile::TempDir,
        child: tokio::process::Child,
        socket: std::path::PathBuf,
        drains: Vec<tokio::task::JoinHandle<()>>,
    }

    impl OwnedHerdrServer {
        fn spawn(binary: &std::path::Path) -> Result<Self, String> {
            let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
            let socket = temp.path().join("s");
            for directory in ["config", "cache", "home"] {
                std::fs::create_dir_all(temp.path().join(directory))
                    .map_err(|error| error.to_string())?;
            }
            let mut child = tokio::process::Command::new(binary)
                .arg("server")
                .env_clear()
                .env("HOME", temp.path().join("home"))
                .env("XDG_CONFIG_HOME", temp.path().join("config"))
                .env("XDG_CACHE_HOME", temp.path().join("cache"))
                .env("HERDR_SOCKET_PATH", &socket)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(|error| format!("could not spawn owned Herdr server: {error}"))?;
            // Drain diagnostics so the child never blocks on a full pipe; the
            // drains die with the test and only this child is ever reaped.
            let mut drains = Vec::new();
            if let Some(pipe) = child.stdout.take() {
                drains.push(drain_pipe(pipe));
            }
            if let Some(pipe) = child.stderr.take() {
                drains.push(drain_pipe(pipe));
            }
            Ok(Self {
                _temp: temp,
                child,
                socket,
                drains,
            })
        }

        async fn shutdown(mut self) {
            for drain in &self.drains {
                drain.abort();
            }
            let _ = self.child.kill().await;
            let _ = self.child.wait().await;
        }
    }

    /// Minimal hand-rolled coordinator speaking the real control framing, so the
    /// proof exercises production encode/decode rather than controller calls.
    struct ProductionControl {
        stream: UnixStream,
        decoder: ControlDecoder,
        next_id: u64,
    }

    impl ProductionControl {
        async fn connect(socket: &std::path::Path) -> Result<Self, String> {
            let mut stream = UnixStream::connect(socket)
                .await
                .map_err(|error| format!("could not connect control socket: {error}"))?;
            stream
                .write_all(&Prelude::control(PeerRole::ActivationCoordinator).encode())
                .await
                .map_err(|error| error.to_string())?;
            let mut prelude = [0; muxe_protocol::PRELUDE_LEN];
            tokio::io::AsyncReadExt::read_exact(&mut stream, &mut prelude)
                .await
                .map_err(|error| error.to_string())?;
            Prelude::decode(prelude).map_err(|error| error.to_string())?;
            let mut decoder = ControlDecoder::new(ControlPolicy::coordinator());
            // Advance the decoder past its prelude state exactly like the broker
            // side does; subsequent traffic is length-prefixed only.
            decoder
                .push(&prelude, |_| {})
                .map_err(|error| error.to_string())?;
            Ok(Self {
                stream,
                decoder,
                next_id: 1,
            })
        }

        async fn round_trip(
            &mut self,
            operation: ControlOperation,
        ) -> Result<ControlResult, String> {
            self.next_id += 1;
            let mut id = [0; 16];
            id[..8].copy_from_slice(&self.next_id.to_be_bytes());
            let payload = serde_json::to_vec(&ControlMessage::Request(ControlRequest {
                request_id: ControlRequestId(id),
                operation,
            }))
            .map_err(|error| error.to_string())?;
            self.stream
                .write_all(
                    &u32::try_from(payload.len())
                        .expect("control payload fits the frame cap")
                        .to_be_bytes(),
                )
                .await
                .map_err(|error| error.to_string())?;
            self.stream
                .write_all(&payload)
                .await
                .map_err(|error| error.to_string())?;
            self.stream
                .flush()
                .await
                .map_err(|error| error.to_string())?;
            loop {
                let mut chunk = [0u8; 4096];
                let read = self
                    .stream
                    .read(&mut chunk)
                    .await
                    .map_err(|error| error.to_string())?;
                if read == 0 {
                    return Err("control stream closed mid-operation".to_owned());
                }
                let mut result = None;
                self.decoder
                    .push(&chunk[..read], |message| {
                        if let ControlMessage::Response(response) = message
                            && response.request_id == ControlRequestId(id)
                        {
                            result = Some(response.result);
                        }
                    })
                    .map_err(|error| error.to_string())?;
                if let Some(result) = result {
                    return Ok(result);
                }
            }
        }
    }

    fn production_binary() -> Option<std::path::PathBuf> {
        std::env::var_os("MUXE_HERDR_TEST_BINARY")
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from)
            .filter(|path| path.is_absolute())
    }

    fn production_config(
        directory: &tempfile::TempDir,
        adapter: &muxe_adapter_herdr::HerdrAdapter,
    ) -> (std::path::PathBuf, muxe_core::CompiledConfig) {
        let config_path = directory.path().join("config.yml");
        let source = "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: menu:quit\n";
        std::fs::write(&config_path, source).expect("write production proof config");
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<production proof>"),
            std::fs::read_to_string(&config_path).expect("read production proof config"),
            KeyCapabilities::default(),
            Some(adapter),
        )
        .expect("compile production proof config");
        (config_path, config)
    }

    /// Shared production stack for the approval-gated proofs: owned server,
    /// adapter, broker, listener, runner, and coordinator connection.
    struct ProductionStack {
        server: OwnedHerdrServer,
        _cache: tempfile::TempDir,
        _directory: tempfile::TempDir,
        _runtime: tempfile::TempDir,
        _broker: Arc<Broker>,
        endpoint: RuntimeEndpoint,
        control: ProductionControl,
        run_task: tokio::task::JoinHandle<Result<(), ServerError>>,
        shutdown_tx: watch::Sender<bool>,
        live: LiveServerIdentity,
    }

    impl ProductionStack {
        async fn start(authorize_commit: bool) -> Option<Self> {
            let Some(binary) = production_binary() else {
                eprintln!("skipping: MUXE_HERDR_TEST_BINARY names no absolute Herdr binary");
                return None;
            };
            let server = OwnedHerdrServer::spawn(&binary).expect("owned Herdr server starts");
            let cache = tempfile::tempdir().expect("owned adapter cache directory");
            let adapter = tokio::time::timeout(
                Duration::from_secs(15),
                muxe_adapter_herdr::HerdrAdapter::connect(muxe_adapter_herdr::HerdrAdapterConfig {
                    socket_path: server.socket.clone(),
                    herdr_binary: binary,
                    cache_dir: cache.path().to_path_buf(),
                }),
            )
            .await
            .expect("production adapter connects before the deadline")
            .expect("production adapter connects");
            let directory = tempfile::tempdir().expect("owned broker directory");
            let (config_path, config) = production_config(&directory, &adapter);
            let broker = Broker::from_compiled(adapter, &config_path, config);
            let live = broker.live_identity().await.expect("live identity");
            let runtime = tempfile::tempdir().expect("owned runtime directory");
            let endpoint = RuntimeEndpoint::in_runtime_dir(
                runtime.path(),
                HostKind::Herdr,
                &live.discovery_key,
            )
            .expect("derive owned listener endpoint");
            let server_handle = BrokerServer::start_activation(
                Arc::clone(&broker),
                endpoint.clone(),
                ActivationBootstrap::Running {
                    current: test_record(),
                    bridge_unit: None,
                },
                authorize_commit.then(|| {
                    Arc::new(ScriptedRecovery(std::sync::Mutex::new(RecoveryView {
                        journal: RecoveryJournalStatus::Present,
                        member: RecoveryMemberStatus::Ready,
                        target_live: true,
                        recover_after: Duration::ZERO,
                    }))) as Arc<dyn RecoveryJournal>
                }),
            )
            .await
            .expect("start production activation server");
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let run_task = tokio::spawn(server_handle.run(shutdown_rx));
            let control = ProductionControl::connect(endpoint.socket())
                .await
                .expect("coordinator connects");
            Some(Self {
                server,
                _cache: cache,
                _directory: directory,
                _runtime: runtime,
                _broker: broker,
                endpoint,
                control,
                run_task,
                shutdown_tx,
                live,
            })
        }
    }

    /// Production Prepare/Status/Abort/Commit through a real Herdr adapter, a real
    /// broker listener, and real control framing. In particular Status after Prepare
    /// must report Draining with the retained identity even though the suspended
    /// production adapter cannot answer host dispatch.
    #[tokio::test]
    #[ignore = "requires an owned live Herdr server via MUXE_HERDR_TEST_BINARY; never runs by default"]
    async fn production_herdr_prepare_status_abort_commit() {
        let Some(mut stack) = ProductionStack::start(true).await else {
            return;
        };
        let live_before = stack.live.clone();
        let status = match stack
            .control
            .round_trip(ControlOperation::Status)
            .await
            .expect("status round trip")
        {
            ControlResult::Status(status) => status,
            result => panic!("status must report, got {result:?}"),
        };
        assert_eq!(status.lifecycle, LifecycleState::Running);

        let prepared = match stack
            .control
            .round_trip(ControlOperation::Prepare {
                target: Box::new(test_record()),
                handoff_id: test_handoff(),
            })
            .await
            .expect("prepare round trip")
        {
            ControlResult::Prepared(status) => status,
            result => panic!("prepare must succeed, got {result:?}"),
        };
        let handoff = prepared.handoff_id.expect("prepare reports a handoff");
        assert_eq!(prepared.lifecycle, LifecycleState::Draining);
        assert_eq!(
            prepared.live_server, live_before,
            "suspended production adapter still reports the retained identity"
        );

        let aborted = match stack
            .control
            .round_trip(ControlOperation::Abort {
                handoff_id: handoff,
            })
            .await
            .expect("abort round trip")
        {
            ControlResult::Aborted(status) => status,
            result => panic!("abort must succeed, got {result:?}"),
        };
        assert_eq!(aborted.lifecycle, LifecycleState::Running);

        let prepared = match stack
            .control
            .round_trip(ControlOperation::Prepare {
                target: Box::new(test_record()),
                handoff_id: test_handoff(),
            })
            .await
            .expect("second prepare round trip")
        {
            ControlResult::Prepared(status) => status,
            result => panic!("second prepare must succeed, got {result:?}"),
        };
        let handoff = prepared.handoff_id.expect("second prepare reports");
        let committed = match stack
            .control
            .round_trip(ControlOperation::Commit {
                handoff_id: handoff,
            })
            .await
            .expect("commit round trip")
        {
            ControlResult::Committed(status) => status,
            result => panic!("commit must succeed, got {result:?}"),
        };
        assert_eq!(committed.lifecycle, LifecycleState::SupervisorOnly);
        drop(stack.control);
        let _ = stack.shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), stack.run_task)
            .await
            .expect("broker service stops")
            .expect("service joins")
            .expect("service has no error");
        stack.server.shutdown().await;
    }

    /// Coordinator disconnect mid-activation against the production adapter: with no
    /// journal authorizing a target, the old broker restores itself instead of
    /// stranding the endpoint drained.
    #[tokio::test]
    #[ignore = "requires an owned live Herdr server via MUXE_HERDR_TEST_BINARY; never runs by default"]
    async fn production_herdr_disconnect_restores_old_broker() {
        let Some(mut stack) = ProductionStack::start(false).await else {
            return;
        };
        let prepared = match stack
            .control
            .round_trip(ControlOperation::Prepare {
                target: Box::new(test_record()),
                handoff_id: test_handoff(),
            })
            .await
            .expect("prepare round trip")
        {
            ControlResult::Prepared(status) => status,
            result => panic!("prepare must succeed, got {result:?}"),
        };
        assert_eq!(prepared.lifecycle, LifecycleState::Draining);
        // The coordinator dies here: dropping the only control connection must
        // restore the old broker without any journal authorizing a target.
        drop(stack.control);
        let mut recovered = None;
        for _ in 0..100 {
            let mut probe = ProductionControl::connect(stack.endpoint.socket())
                .await
                .expect("reconnect for status probe");
            if let ControlResult::Status(status) = probe
                .round_trip(ControlOperation::Status)
                .await
                .expect("probe")
                && status.lifecycle == LifecycleState::Running
            {
                recovered = Some(status);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let recovered = recovered.expect("old broker restores itself after disconnect");
        assert_eq!(recovered.live_server, stack.live);
        drop(stack.run_task);
        stack.server.shutdown().await;
    }
    /// Scripted schema child answering `herdr api schema --json` from the pinned
    /// fixture, so the recorded proof exercises the real runtime schema path with
    /// no live binary. Owned home `TempDir`, retained by the caller.
    fn recorded_schema_binary() -> (std::path::PathBuf, tempfile::TempDir) {
        let schema = include_str!("../../../fixtures/herdr/herdr-api.schema.json");
        let home = tempfile::tempdir().expect("owned schema home directory");
        let schema_path = home.path().join("herdr-api.schema.json");
        std::fs::write(&schema_path, schema).expect("stage pinned schema fixture");
        let script = home.path().join("herdr");
        crate::generated_executable::write_executable_script(&script, |writer| {
            std::io::Write::write_all(
                writer,
                format!("#!/bin/sh\ncat '{}'\n", schema_path.display()).as_bytes(),
            )
        })
        .expect("stage schema script");
        (script, home)
    }

    /// Scripted Herdr socket server: exact ping/subscribe exchanges with retained
    /// subscription streams and a live-stream counter, so suspend/resume stream
    /// release is observable without any Herdr process.
    fn serve_scripted(
        path: &std::path::Path,
        live_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::UnixListener::bind(path).expect("bind scripted socket");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let live_streams = std::sync::Arc::clone(&live_streams);
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut request = Vec::new();
                    let read = reader.read_until(b'\n', &mut request).await.unwrap_or(0);
                    if read == 0 {
                        // Probe connection without a request line.
                        return;
                    }
                    let payload: serde_json::Value =
                        serde_json::from_slice(&request).unwrap_or_default();
                    let method = payload
                        .get("method")
                        .and_then(|method| method.as_str())
                        .unwrap_or_default()
                        .to_owned();
                    let id = payload
                        .get("id")
                        .and_then(|id| id.as_str())
                        .unwrap_or_default()
                        .to_owned();
                    if method == "events.subscribe" {
                        live_streams.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let response = serde_json::json!({
                            "id": id,
                            "result": {"subscribed": true},
                        });
                        let _ = reader.write_all(format!("{response}\n").as_bytes()).await;
                        // Retained stream: the adapter dropping its side reads as
                        // EOF here, which releases the count before any target
                        // could connect.
                        loop {
                            let mut probe = [0u8; 1];
                            match reader.read(&mut probe).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                        live_streams.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        return;
                    }
                    let result = match method.as_str() {
                        "session.snapshot" => serde_json::json!({
                            "type": "session_snapshot",
                            "snapshot": {},
                        }),
                        _ => serde_json::json!({
                            "type": "pong",
                            "protocol": muxe_adapter_herdr::generated::BUNDLED_PROTOCOL,
                            "version": "0.8.2",
                        }),
                    };
                    let response = serde_json::json!({"id": id, "result": result});
                    let _ = reader.write_all(format!("{response}\n").as_bytes()).await;
                });
            }
        })
    }

    async fn assert_recorded_prepared_status(
        control: &mut ProductionControl,
        live_before: &LiveServerIdentity,
        live_streams: &Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let prepared = match control
            .round_trip(ControlOperation::Status)
            .await
            .expect("status round trip")
        {
            ControlResult::Status(status) => status,
            result => panic!("status must report, got {result:?}"),
        };
        assert_eq!(
            &prepared.live_server, live_before,
            "suspended adapter still reports the retained identity"
        );
        await_stream_count(
            live_streams,
            0,
            Duration::from_secs(5),
            "suspend releases the retained stream before any target connects",
        )
        .await;
    }

    /// Recorded-host production round trip: a real Herdr adapter against a scripted
    /// socket plus schema child, a real broker listener, and real control framing
    /// prove Prepare/Status/Abort/Commit with retained identity through suspend.
    /// The retained-subscription counter proves the old stream is released before
    /// any target could connect and re-established on abort.
    #[tokio::test]
    async fn recorded_herdr_prepare_abort_commit_round_trip() {
        let directory = tempfile::tempdir().expect("owned recorded directory");
        let (schema_binary, _schema_home) = recorded_schema_binary();
        let socket = directory.path().join("herdr.sock");
        let live_streams = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let server = serve_scripted(&socket, Arc::clone(&live_streams));
        let cache = tempfile::tempdir().expect("owned adapter cache directory");
        let adapter = tokio::time::timeout(
            Duration::from_secs(15),
            muxe_adapter_herdr::HerdrAdapter::connect(muxe_adapter_herdr::HerdrAdapterConfig {
                socket_path: socket,
                herdr_binary: schema_binary,
                cache_dir: cache.path().to_path_buf(),
            }),
        )
        .await
        .expect("recorded adapter connects before the deadline")
        .expect("recorded adapter connects");
        assert_eq!(
            live_streams.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "connect retains exactly one subscription stream"
        );
        let config_directory = tempfile::tempdir().expect("owned broker directory");
        let (config_path, config) = production_config(&config_directory, &adapter);
        let broker = Broker::from_compiled(adapter.clone(), &config_path, config);
        let live_before = broker
            .live_identity()
            .await
            .expect("live identity before prepare");
        let runtime = tempfile::tempdir().expect("owned runtime directory");
        let endpoint = RuntimeEndpoint::in_runtime_dir(
            runtime.path(),
            HostKind::Herdr,
            &live_before.discovery_key,
        )
        .expect("derive owned listener endpoint");
        let broker_server = BrokerServer::start_activation(
            Arc::clone(&broker),
            endpoint.clone(),
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
            Some(Arc::new(ScriptedRecovery(std::sync::Mutex::new(
                RecoveryView {
                    journal: RecoveryJournalStatus::Present,
                    member: RecoveryMemberStatus::Ready,
                    target_live: true,
                    recover_after: Duration::ZERO,
                },
            )))),
        )
        .await
        .expect("start recorded activation server");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run_task = tokio::spawn(broker_server.run(shutdown_rx));
        let mut control = ProductionControl::connect(endpoint.socket())
            .await
            .expect("coordinator connects");
        let handoff = prepare_draining(&mut control, "prepare").await;
        assert_recorded_prepared_status(&mut control, &live_before, &live_streams).await;
        abort_and_assert_running(&mut control, handoff).await;
        await_stream_count(
            &live_streams,
            1,
            Duration::from_secs(10),
            "abort re-establishes exactly one subscription stream",
        )
        .await;
        let handoff = prepare_draining(&mut control, "second prepare").await;
        commit_and_assert_supervisor(&mut control, handoff).await;
        drop(control);
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("broker service stops")
            .expect("service joins")
            .expect("service has no error");
        adapter
            .shutdown()
            .await
            .expect("recorded adapter shuts down");
        server.abort();
    }

    /// Aborts a prepared handoff and asserts the old broker runs again.
    async fn abort_and_assert_running(control: &mut ProductionControl, handoff: HandoffId) {
        let aborted = match control
            .round_trip(ControlOperation::Abort {
                handoff_id: handoff,
            })
            .await
            .expect("abort round trip")
        {
            ControlResult::Aborted(status) => status,
            result => panic!("abort must succeed, got {result:?}"),
        };
        assert_eq!(aborted.lifecycle, LifecycleState::Running);
    }

    /// Prepares and asserts the draining state, returning the fresh handoff.
    async fn prepare_draining(control: &mut ProductionControl, what: &str) -> HandoffId {
        let prepared = match control
            .round_trip(ControlOperation::Prepare {
                target: Box::new(test_record()),
                handoff_id: test_handoff(),
            })
            .await
            .unwrap_or_else(|_| panic!("{what} round trip"))
        {
            ControlResult::Prepared(status) => status,
            result => panic!("{what} must succeed, got {result:?}"),
        };
        assert_eq!(prepared.lifecycle, LifecycleState::Draining);
        prepared.handoff_id.expect("prepare reports a handoff")
    }

    /// Commits a prepared handoff and asserts supervisor-only retirement.
    async fn commit_and_assert_supervisor(control: &mut ProductionControl, handoff: HandoffId) {
        let committed = match control
            .round_trip(ControlOperation::Commit {
                handoff_id: handoff,
            })
            .await
            .expect("commit round trip")
        {
            ControlResult::Committed(status) => status,
            result => panic!("commit must succeed, got {result:?}"),
        };
        assert_eq!(committed.lifecycle, LifecycleState::SupervisorOnly);
    }

    /// Polls a scripted-server stream counter until it reaches the expected count.
    async fn await_stream_count(
        live_streams: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
        expected: usize,
        timeout: Duration,
        what: &'static str,
    ) {
        tokio::time::timeout(timeout, async {
            while live_streams.load(std::sync::atomic::Ordering::SeqCst) != expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect(what);
    }

    async fn assert_target_commit_authority(
        controller: &Arc<ActivationController>,
        broker: &Arc<Broker>,
    ) {
        // A handoff without owner proof cannot authorize target Commit.
        let (result, stop) = controller
            .handle(
                broker,
                ControlOperation::Commit {
                    handoff_id: HandoffId([7; 16]),
                },
            )
            .await;
        assert!(!stop);
        assert!(matches!(result, ControlResult::Error { .. }));
        assert!(!controller.allows_ui().await);
        controller.set_recovery(absent_recovery()).await;
        let (result, _) = controller
            .handle(
                broker,
                ControlOperation::Commit {
                    handoff_id: HandoffId([7; 16]),
                },
            )
            .await;
        assert!(matches!(result, ControlResult::Error { .. }));
        assert!(!controller.allows_ui().await);
        controller
            .set_recovery(Arc::new(ScriptedRecovery(std::sync::Mutex::new(
                RecoveryView {
                    journal: RecoveryJournalStatus::Present,
                    member: RecoveryMemberStatus::Ready,
                    target_live: true,
                    recover_after: Duration::ZERO,
                },
            ))))
            .await;
        let (result, stop) = controller
            .handle(
                broker,
                ControlOperation::Commit {
                    handoff_id: HandoffId([7; 16]),
                },
            )
            .await;
        assert!(!stop, "target commit never stops its own service");
        assert!(matches!(result, ControlResult::Committed(_)));
        assert!(controller.allows_ui().await);
        let (again, _) = controller
            .handle(
                broker,
                ControlOperation::Commit {
                    handoff_id: HandoffId([7; 16]),
                },
            )
            .await;
        assert!(
            matches!(again, ControlResult::Committed(_)),
            "the original may replay Commit"
        );
    }

    /// A fresh gated target reports no readiness until its adapter completes a
    /// real census round: the broker exposes gated control (status works, UI
    /// refused) without waiting for evidence, serves evidence verbatim once the
    /// round lands (even an authoritative empty set), and reflects a lost round
    /// as None again instead of caching stale coverage.
    #[tokio::test]
    async fn gated_target_reports_no_readiness_before_a_real_round() {
        let (broker, adapter, _directory) = ordering_broker(false, false);
        let live_server = LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: "owned-fake-host".to_owned(),
            server_id: WireServerId::new("owned-fake-server"),
        };
        let controller = ActivationController::start(
            &broker,
            ActivationBootstrap::Target {
                current: test_record(),
                handoff: HandoffId([7; 16]),
                live_server,
                bridge_unit: None,
            },
        )
        .await
        .expect("activation controller starts for a gated target");
        // No round has completed: gated control is exposed, UI refused, no evidence.
        let (result, stop) = controller.handle(&broker, ControlOperation::Status).await;
        assert!(!stop, "status never stops the target service");
        let ControlResult::Status(status) = result else {
            panic!("gated target reports status, got {result:?}");
        };
        assert_eq!(status.handoff_id, Some(HandoffId([7; 16])));
        assert_eq!(status.ready, None);
        assert!(
            !controller.allows_ui().await,
            "a gated target refuses UI before commit"
        );
        // The adapter completes a real round for a zero-client unit: the broker
        // serves the authoritative empty set verbatim instead of coercing it.
        adapter.set_readiness(Some(muxe_adapter_api::ActivationReadiness {
            registered_clients: Vec::new(),
            member_clients: Vec::new(),
        }));
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        let ControlResult::Status(status) = result else {
            panic!("gated target reports status, got {result:?}");
        };
        assert_eq!(
            status.ready,
            Some(muxe_protocol::TargetReadiness {
                registered_clients: Vec::new(),
                member_clients: 0,
                member_ids: Some(Vec::new()),
                proof_epoch: None,
            }),
            "authoritative empty evidence is served verbatim, still UI-gated"
        );
        assert!(
            !controller.allows_ui().await,
            "evidence alone never opens UI before commit"
        );
        // The round is lost again: the broker reflects None rather than caching.
        adapter.set_readiness(None);
        let (result, _) = controller.handle(&broker, ControlOperation::Status).await;
        let ControlResult::Status(status) = result else {
            panic!("gated target reports status, got {result:?}");
        };
        assert_eq!(status.ready, None);
        assert!(
            controller
                .retire_old(&broker, HandoffId([7; 16]))
                .await
                .is_err(),
            "target role cannot use old retirement"
        );
        assert_target_commit_authority(&controller, &broker).await;
    }

    /// Stages a lingering `/bin/sh` child (pid file + FIFO announcement) and returns
    /// its script, the startup barrier, and its pid file.
    fn stage_lingering_child(
        directory: &tempfile::TempDir,
    ) -> (
        std::path::PathBuf,
        tokio::task::JoinHandle<[u8; 7]>,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let fifo = directory.path().join("started");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::from_bits_truncate(0o600))
            .expect("owned fifo exists");
        let startup = tokio::task::spawn_blocking({
            let fifo = fifo.clone();
            move || {
                let mut reader = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&fifo)
                    .map_err(|error| error.to_string())?;
                let mut started = [0u8; 7];
                std::io::Read::read_exact(&mut reader, &mut started)
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>(started)
            }
        });
        let startup = tokio::spawn(async move {
            startup
                .await
                .expect("reader thread joins")
                .expect("child signals start")
        });
        let exit_fifo = directory.path().join("exit_trigger");
        nix::unistd::mkfifo(&exit_fifo, nix::sys::stat::Mode::from_bits_truncate(0o600))
            .expect("exit fifo exists");
        let pidfile = directory.path().join("child.pid");
        let script = directory.path().join("linger.sh");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\nprintf '%s' \"$$\" > '{}'\nprintf started > '{}'\nread line < '{}'\nexit 0\n",
                pidfile.display(),
                fifo.display(),
                exit_fifo.display()
            ),
        )
        .expect("stage lingering child script");
        (script, startup, pidfile, exit_fifo)
    }

    /// The response/recovery owner must explicitly release the retirement ticket:
    /// resource cleanup is complete before the ticket is issued, but supervisor
    /// completion remains blocked until its owner has finished the ACK barrier.
    #[tokio::test]
    async fn retirement_ticket_blocks_supervisor_until_owner_releases() {
        let (ticket, released) = RetirementTicket::pair();
        let supervisor = tokio::spawn(async move {
            released.await.expect("retirement owner releases ticket");
        });
        tokio::task::yield_now().await;
        assert!(
            !supervisor.is_finished(),
            "retired resources do not authorize supervisor exit by themselves"
        );
        drop(ticket);
        tokio::time::timeout(Duration::from_secs(1), supervisor)
            .await
            .expect("ticket release wakes supervisor")
            .expect("supervisor barrier task joins");
    }

    #[derive(Clone, Copy)]
    enum RetirementScenario {
        Explicit,
        RecoveredOld,
        CancelledStop,
    }

    struct LingerService {
        broker: Arc<Broker>,
        adapter: Arc<SmokeAdapter>,
        activation: Arc<ActivationController>,
        endpoint: RuntimeEndpoint,
        run_task: JoinHandle<Result<(), ServerError>>,
        _config_directory: tempfile::TempDir,
        _runtime_directory: tempfile::TempDir,
        _shutdown_tx: tokio::sync::watch::Sender<bool>,
    }

    struct SupervisedChildFixture {
        service: LingerService,
        ui_client: BrokerClient,
        child: nix::unistd::Pid,
        exit_fifo: std::path::PathBuf,
        _staging: tempfile::TempDir,
    }

    async fn start_linger_service(
        staging: &tempfile::TempDir,
        script: &std::path::Path,
        scenario: RetirementScenario,
    ) -> LingerService {
        let config_directory = tempfile::tempdir().expect("owned activation runtime directory");
        let config_path = config_directory.path().join("config.yml");
        let yaml = format!(
            r"
version: 1
menus:
  main:
    bindings:
      l:
        label: linger
        action:
          type: command:execute
          program: /bin/sh
          args:
            - {script:?}
          cwd: {cwd:?}
        settings:
          execution:
            mode: detach
",
            script = script.to_string_lossy(),
            cwd = staging.path().to_string_lossy(),
        );
        std::fs::write(&config_path, &yaml).expect("write linger config");
        let adapter = Arc::new(SmokeAdapter::new());
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<owned activation linger>"),
            yaml,
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("compile linger config");
        let broker = Broker::from_compiled(adapter.clone(), &config_path, config);
        let runtime_directory = tempfile::tempdir().expect("owned runtime directory");
        let endpoint = RuntimeEndpoint::in_runtime_dir(
            runtime_directory.path(),
            HostKind::Herdr,
            "owned-fake-host",
        )
        .expect("derive owned endpoint");
        let broker_server = BrokerServer::start_activation(
            Arc::clone(&broker),
            endpoint.clone(),
            ActivationBootstrap::Running {
                current: test_record(),
                bridge_unit: None,
            },
            matches!(scenario, RetirementScenario::RecoveredOld).then(|| {
                Arc::new(ScriptedRecovery(std::sync::Mutex::new(RecoveryView {
                    journal: RecoveryJournalStatus::Present,
                    member: RecoveryMemberStatus::Ready,
                    target_live: true,
                    recover_after: Duration::ZERO,
                }))) as Arc<dyn RecoveryJournal>
            }),
        )
        .await
        .expect("start owned activation server");
        let activation = broker_server
            .activation
            .clone()
            .expect("activation controller");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run_task = tokio::spawn(broker_server.run(shutdown_rx));
        LingerService {
            broker,
            adapter,
            activation,
            endpoint,
            run_task,
            _config_directory: config_directory,
            _runtime_directory: runtime_directory,
            _shutdown_tx: shutdown_tx,
        }
    }

    async fn start_supervised_child_fixture(
        scenario: RetirementScenario,
    ) -> SupervisedChildFixture {
        let staging = tempfile::tempdir().expect("owned child staging directory");
        let (script, startup, pidfile, exit_fifo) = stage_lingering_child(&staging);
        let service = start_linger_service(&staging, &script, scenario).await;
        let live_server = service.broker.live_identity().await.expect("host identity");
        let client_identity = LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: "owned-fake-host".to_owned(),
            server_id: WireServerId::new(live_server.server_id.as_str()),
        };
        let mut ui_client = BrokerClient::connect(
            service.endpoint.socket(),
            PeerRole::Ui,
            "owned-retire-ui",
            client_identity,
        )
        .await
        .expect("connect UI client");
        let attached = ui_client
            .request(ClientRequest::AttachUi(AttachUi {
                root: muxe_protocol::MenuId::named("main"),
                pane: HostPaneId::new("owned-ui-pane"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }))
            .await
            .expect("attach UI");
        let BrokerResponse::UiAttached { session, snapshot } = attached else {
            panic!("expected UI attached");
        };
        let binding = snapshot
            .menu
            .menus
            .iter()
            .find(|menu| menu.id == muxe_protocol::MenuId::named("main"))
            .and_then(|menu| menu.bindings.first())
            .expect("linger binding visible");
        let invoked = ui_client
            .request(ClientRequest::InvokeBinding(muxe_protocol::InvokeBinding {
                session,
                generation: 1,
                binding: binding.id,
            }))
            .await
            .expect("invoke linger binding");
        assert!(matches!(
            invoked,
            BrokerResponse::InvocationAccepted {
                disposition: InvocationDisposition::Detached,
                ..
            }
        ));
        let started = startup.await.expect("child signals start");
        assert_eq!(&started, b"started");
        let child_pid: i32 = std::fs::read_to_string(&pidfile)
            .expect("child publishes its pid")
            .trim()
            .parse()
            .expect("pid parses");
        let child = nix::unistd::Pid::from_raw(child_pid);
        assert!(
            nix::sys::signal::kill(child, None).is_ok(),
            "child alive while running"
        );
        SupervisedChildFixture {
            service,
            ui_client,
            child,
            exit_fifo,
            _staging: staging,
        }
    }

    async fn assert_supervised_child_exits_after_release(fixture: &mut SupervisedChildFixture) {
        assert!(
            !fixture.service.run_task.is_finished(),
            "the service stays supervisor-only while the child runs"
        );
        assert!(fixture.service.broker.has_supervised_children());
        assert!(nix::sys::signal::kill(fixture.child, None).is_ok());
        {
            let mut trigger = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&fixture.exit_fifo)
                .expect("open exit trigger fifo");
            std::io::Write::write_all(&mut trigger, b"exit\n").expect("write exit trigger");
        }
        tokio::time::timeout(Duration::from_secs(10), &mut fixture.service.run_task)
            .await
            .expect("service terminates after child exits and is reaped")
            .expect("service joins")
            .expect("service has no error");
        assert_eq!(
            nix::sys::signal::kill(fixture.child, None),
            Err(nix::errno::Errno::ESRCH),
            "child process is reaped and no longer exists"
        );
        assert!(!fixture.service.broker.has_supervised_children());
    }

    /// Retire lifetime smoke over a production server: a real detached generic
    /// child keeps running; the service stays supervisor-only until the
    /// child exits and is reaped, then terminates. Barriers throughout: FIFO
    /// readiness, task completion, and liveness probes — no sleep assumptions.
    #[tokio::test]
    async fn retire_supervises_detached_child_until_reaped() {
        assert_detached_child_supervision(RetirementScenario::Explicit).await;
    }

    #[tokio::test]
    async fn recovered_old_supervises_detached_child_until_reaped() {
        assert_detached_child_supervision(RetirementScenario::RecoveredOld).await;
    }

    #[tokio::test]
    async fn cancelled_stop_waiter_keeps_detached_child_supervised() {
        assert_detached_child_supervision(RetirementScenario::CancelledStop).await;
    }

    async fn assert_detached_child_supervision(scenario: RetirementScenario) {
        let mut fixture = start_supervised_child_fixture(scenario).await;
        match scenario {
            RetirementScenario::RecoveredOld => {
                let mut control = ProductionControl::connect(fixture.service.endpoint.socket())
                    .await
                    .expect("coordinator connects");
                prepare_draining(&mut control, "prepare").await;
                drop(control);
            }
            RetirementScenario::Explicit => {
                let mut control = ProductionControl::connect(fixture.service.endpoint.socket())
                    .await
                    .expect("coordinator connects");
                let retired = match control
                    .round_trip(ControlOperation::Retire)
                    .await
                    .expect("retire round trip")
                {
                    ControlResult::Retired(status) => status,
                    result => panic!("retire must succeed, got {result:?}"),
                };
                assert_eq!(retired.lifecycle, LifecycleState::Retired);
            }
            RetirementScenario::CancelledStop => {
                fixture
                    .service
                    .broker
                    .drain_for_activation()
                    .await
                    .expect("drain UI before Stop");
                let (complete, waiter) = oneshot::channel();
                drop(waiter);
                fixture
                    .service
                    .activation
                    .command_sender()
                    .await
                    .expect("service installed commands")
                    .send(ServerCommand::Stop { complete })
                    .await
                    .expect("deliver Stop with cancelled waiter");
            }
        }
        let event = tokio::time::timeout(Duration::from_secs(5), fixture.ui_client.next_event())
            .await
            .expect("retire emits a UI event within bound")
            .expect("ready UI receives the retirement event");
        assert!(
            matches!(event, BrokerEvent::BrokerRetiring),
            "the ready UI observes BrokerRetiring before the connection closes"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.service.endpoint.socket().exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("retire unlinks the endpoint");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !fixture.service.adapter.shutdown.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovered old adapter completes shutdown");
        assert!(
            fixture.service.adapter.shutdown.load(Ordering::SeqCst),
            "host adapter stops before detached-child supervision"
        );
        if matches!(scenario, RetirementScenario::CancelledStop) {
            assert!(
                fixture
                    .service
                    .activation
                    .old_retirement
                    .lock()
                    .await
                    .is_none(),
                "cancelled command has no Commit proof or acknowledgement"
            );
        }
        assert_supervised_child_exits_after_release(&mut fixture).await;
    }

    /// H08 acceptance (service layer, owned fixtures only): a queued terminal
    /// `HostLost` drives the exact Stop teardown — config watch dropped (probes
    /// stop), owned endpoint unlinked, awaiting host execution failed with
    /// `HostUnavailable` — while the detached generic child stays supervised.
    /// Cloned from `retire_supervises_detached_child_until_reaped`; the only
    /// difference is the retirement trigger (health signal, not Retire RPC).
    #[tokio::test]
    async fn host_loss_retires_owned_endpoint_and_keeps_generic_supervision() {
        // Host loss, without a coordinator or Retire RPC, must drive Stop.
        let mut fixture = start_supervised_child_fixture(RetirementScenario::Explicit).await;
        fixture
            .service
            .broker
            .handle_health_event(AdapterHealthEvent::HostLost {
                identity: HostIdentity {
                    kind: muxe_adapter_api::HostKind::Herdr,
                    discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(
                        "owned-fake-host".to_owned(),
                    )
                    .expect("validated host discovery key"),
                    live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
                        "owned-fake-server".to_owned(),
                    )
                    .expect("validated live server incarnation"),
                },
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Herdr host did not recover within the bounded reconnect grace",
                ),
            })
            .await;

        // Health events may precede the terminal BrokerRetiring notification.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match fixture.ui_client.next_event().await {
                    Ok(BrokerEvent::BrokerRetiring) => return,
                    Ok(_) => {}
                    Err(error) => panic!("ready UI receives the retirement event: {error}"),
                }
            }
        })
        .await
        .expect("host loss emits a UI event within bound");
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.service.endpoint.socket().exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("host loss unlinks the endpoint");
        assert_supervised_child_exits_after_release(&mut fixture).await;
    }
}
