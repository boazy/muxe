//! Forced recovery keeps the original decision readable and journals every
//! irreversible participant stop separately before superseding that decision.

use super::{
    ActivateError, ActivateInputs, ActivationId, ActivationJournal, BridgeProgress, BrokerEntry,
    CompatibilityRecord, ControlError, ControlPort, ControlSession, Duration, HostReloader,
    HostScope, JournalError, LifecycleState, MemberTransactionId, NativeBrokerAuthority,
    OldMemberProgress, Path, PathBuf, RecoveryOutcome, RegisteredBroker, Registry,
    TargetMemberProgress, TransactionDirective, UnitKind, fsutil, journal,
    publish_commit_artifacts, recover_one, select_units,
};
use crate::lifecycle::control;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub(super) struct RecoveredStop {
    pub entry: BrokerEntry,
    pub old_record: Option<CompatibilityRecord>,
    pub authority: journal::ForcedStop,
}

#[derive(Debug)]
pub(super) struct ForcedRecovery {
    pub journal: ActivationJournal,
    pub _lock: journal::UnitLock,
    pub rows: Vec<BrokerEntry>,
    pub stopped: Vec<RecoveredStop>,
}

impl ForcedRecovery {
    fn pending(journal: ActivationJournal, lock: journal::UnitLock) -> Self {
        let stopped = journal
            .members()
            .iter()
            .filter_map(|member| {
                let authority = member
                    .forced_stop
                    .as_ref()
                    .filter(|stop| stop.proof.is_some())?;
                let entry = journal.old_registry.iter().find(|entry| {
                    entry.socket == member.endpoint().as_path()
                        && entry.discovery_key == member.member().as_str()
                })?;
                Some(RecoveredStop {
                    entry: entry.clone(),
                    old_record: member.old_record.clone(),
                    authority: authority.clone(),
                })
            })
            .collect();
        Self {
            rows: journal.old_registry.clone(),
            journal,
            _lock: lock,
            stopped,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ParticipantRole {
    Old,
    Target,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ParticipantStop {
    member: MemberTransactionId,
    role: ParticipantRole,
    entry: BrokerEntry,
    old_record: Option<CompatibilityRecord>,
    authority: journal::ForcedStop,
}

#[derive(Debug, Deserialize, Serialize)]
struct CommitStops {
    schema_version: u32,
    activation: ActivationId,
    unit: UnitKind,
    participants: Vec<ParticipantStop>,
}

fn ledger_path(cache: &Path, journal: &ActivationJournal) -> PathBuf {
    journal::activation_dir(cache).join(format!("force-{}.ledger", journal.activation_id))
}

fn write_ledger(
    cache: &Path,
    saved: &ActivationJournal,
    ledger: &CommitStops,
) -> Result<(), ActivateError> {
    let path = ledger_path(cache, saved);
    let bytes = serde_json::to_vec_pretty(ledger).map_err(|source| JournalError::Corrupt {
        path: path.clone(),
        source,
    })?;
    fsutil::write_atomic(&path, &bytes, "saving broker stops for forced replacement")?;
    Ok(())
}

fn read_ledger(
    cache: &Path,
    saved: &ActivationJournal,
) -> Result<Option<CommitStops>, ActivateError> {
    let path = ledger_path(cache, saved);
    if matches!(std::fs::symlink_metadata(&path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(None);
    }
    let bytes = fsutil::read_owner_file(&path)?;
    let ledger: CommitStops =
        serde_json::from_slice(&bytes).map_err(|source| JournalError::Corrupt {
            path: path.clone(),
            source,
        })?;
    if ledger.schema_version != journal::FORCED_ACTIVATION_JOURNAL_SCHEMA_VERSION
        || ledger.activation != saved.activation_id
        || ledger.unit != saved.unit
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "Muxe cannot use the saved stop record at {}: it belongs to a different broker group or activation decision. Muxe made no new stop request while checking this record; leave it unchanged",
                path.display()
            ),
        });
    }
    let mut unique = std::collections::HashSet::new();
    for participant in &ledger.participants {
        let member = saved
            .members()
            .iter()
            .find(|member| member.id == participant.member)
            .ok_or_else(|| ActivateError::UnitFailed {
                reason: format!("Muxe cannot use the saved stop record at {}: it names a broker missing from the activation record. Muxe made no new stop request while checking this record; leave it unchanged", path.display()),
            })?;
        let expected = match participant.role {
            ParticipantRole::Old => saved.old_registry.iter().find(|row| {
                row.socket == member.endpoint().as_path()
                    && row.discovery_key == member.member().as_str()
            }),
            ParticipantRole::Target => saved
                .ready_proof()
                .and_then(|proof| proof.member(&member.id))
                .map(|proof| &proof.entry),
        };
        if expected != Some(&participant.entry)
            || !unique.insert((participant.member.clone(), participant.role))
            || participant.authority.process.process().get() != participant.entry.server_pid
            || participant
                .entry
                .process_identity
                .as_ref()
                .is_some_and(|identity| identity != &participant.authority.process)
            || participant
                .authority
                .endpoint
                .as_ref()
                .is_some_and(|endpoint| {
                    !endpoint.valid_recorded() || endpoint.path() != participant.entry.socket
                })
            || (participant.authority.endpoint.is_none()
                && participant.entry.process_identity.as_ref()
                    != Some(&participant.authority.process))
        {
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "Muxe cannot use the saved stop record at {} for broker {} (process {}): its process identity or socket differs from the activation record. Muxe made no new stop request while checking this record; leave it unchanged",
                    path.display(),
                    participant.entry.discovery_key,
                    participant.entry.server_pid
                ),
            });
        }
    }
    if saved
        .members()
        .iter()
        .any(|member| !unique.contains(&(member.id.clone(), ParticipantRole::Target)))
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "Muxe cannot use the saved stop record at {}: it does not identify every replacement broker. Muxe made no new stop request while checking this record; leave it unchanged",
                path.display()
            ),
        });
    }
    Ok(Some(ledger))
}

fn typed_entry(row: &BrokerEntry) -> Result<RegisteredBroker, ActivateError> {
    let mut units = select_units(std::slice::from_ref(row), HostScope::All, None)?;
    let unit = units.pop().ok_or_else(|| ActivateError::UnitFailed {
        reason: "Muxe cannot authorize forced recovery: the saved broker registration has an unknown host kind".to_owned(),
    })?;
    // This is a persistence-to-domain boundary, not another host policy selector.
    Ok(unit.entries()[0].clone())
}

// Role admission comes only from private native registry/journal loaders.
// An OS cookie proves an instance, not broker role; arbitrary config/IPC DTOs
// cannot construct the non-serializable admission capability.
pub(super) async fn capture_participant<C: ControlPort>(
    control: &C,
    admitted: &NativeBrokerAuthority<'_>,
    prior: Option<&journal::ForcedStop>,
    require_original: bool,
    known_target: Option<&control::ForcedPeer>,
) -> Result<
    (
        Option<C::Session>,
        Option<control::ForcedPeer>,
        journal::ForcedStop,
    ),
    ActivateError,
> {
    let entry = admitted.entry();
    let failure = |detail: &str| participant_failure(entry, detail);
    if entry.server_pid().get() == std::process::id() {
        return Err(failure(
            "the saved broker identity names this activation coordinator",
        ));
    }
    if let Some(stop) = prior {
        if stop.proof.is_some() {
            return Ok((None, None, stop.clone()));
        }
        if stop
            .process
            .is_gone()
            .map_err(|error| failure(&error.to_string()))?
        {
            let mut ended = stop.clone();
            ended.proof = Some(journal::ForcedStopProof::ProcessExited);
            return Ok((None, None, ended));
        }
    } else if let Some(identity) = entry.process_identity()
        && identity
            .is_gone()
            .map_err(|error| failure(&error.to_string()))?
    {
        return Ok((
            None,
            None,
            journal::ForcedStop {
                process: identity.clone(),
                endpoint: None,
                hard_stop_intent: false,
                proof: Some(journal::ForcedStopProof::ProcessExited),
            },
        ));
    }
    if require_original
        && entry.registration_id().is_none()
        && prior.is_none()
        && !entry
            .process_identity()
            .is_some_and(muxe_process::ProcessIdentity::authorizes_stored_peer)
    {
        return Err(failure(
            "the previous activation did not save a registration token or a usable OS process identity",
        ));
    }
    // An old listener may be absent while this exact shared path belongs to a
    // separately authenticated original target. Never record its inode as old.
    let target_owns_path = if let Some(target) = known_target {
        target
            .endpoint
            .as_ref()
            .is_some_and(|endpoint| endpoint.path() == entry.socket() && endpoint.is_current())
            && !target
                .process
                .identity()
                .is_gone()
                .map_err(|error| failure(&error.to_string()))?
    } else {
        false
    };
    if !target_owns_path {
        match control.connect(entry.socket()).await {
            Ok(mut session) => {
                let peer = session
                    .forced_peer(admitted)
                    .await
                    .map_err(|error| failure(&error.to_string()))?;
                if prior.is_some_and(|stop| stop.process != *peer.process.identity())
                    || (require_original
                        && peer.status.is_none()
                        && (!peer.process.identity().authorizes_stored_peer()
                            || entry.process_identity() != Some(peer.process.identity())))
                {
                    return Err(failure(
                        "the connected process does not match the original saved broker",
                    ));
                }
                let authority = journal::ForcedStop {
                    process: peer.process.identity().clone(),
                    endpoint: peer.endpoint.clone(),
                    hard_stop_intent: false,
                    proof: None,
                };
                return Ok((Some(session), Some(peer), authority));
            }
            Err(ControlError::Connect { source, .. })
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) => {}
            Err(error) => return Err(failure(&error.to_string())),
        }
    }
    let (peer, authority) = stored_participant_authority(entry, prior, target_owns_path)?;
    Ok((None, Some(peer), authority))
}

fn participant_failure(entry: &RegisteredBroker, detail: &str) -> ActivateError {
    ActivateError::UnitFailed {
        reason: format!(
            "Muxe cannot safely identify broker {} (process {}): {detail}. Muxe did not stop this broker; leave its saved activation record unchanged",
            entry.discovery_key(),
            entry.server_pid().get()
        ),
    }
}

fn stored_participant_authority(
    entry: &RegisteredBroker,
    prior: Option<&journal::ForcedStop>,
    target_owns_path: bool,
) -> Result<(control::ForcedPeer, journal::ForcedStop), ActivateError> {
    let failure = |detail: &str| participant_failure(entry, detail);
    let identity = prior.map(|stop| &stop.process).or_else(|| entry.process_identity())
        .filter(|identity| identity.authorizes_stored_peer())
        .ok_or_else(|| failure("its control socket is unavailable and the OS cannot identify the process from the saved record; a process ID alone is insufficient to authorize a stop"))?;
    let process = muxe_process::BrokerStopHandle::resume_recorded(identity)
        .map_err(|error| failure(&error.to_string()))?;
    let endpoint = if target_owns_path {
        None
    } else {
        prior.and_then(|stop| stop.endpoint.clone())
    };
    let authority = journal::ForcedStop {
        process: identity.clone(),
        endpoint: endpoint.clone(),
        hard_stop_intent: false,
        proof: None,
    };
    Ok((
        control::ForcedPeer {
            process,
            endpoint,
            status: None,
        },
        authority,
    ))
}

async fn stop_participant<C: ControlPort>(
    session: &mut Option<C::Session>,
    peer: control::ForcedPeer,
    entry: &BrokerEntry,
    authorize_hard_stop: impl FnOnce() -> Result<(), ActivateError>,
) -> Result<journal::ForcedStopProof, ActivateError> {
    if let Some(session) = session {
        let retired = tokio::time::timeout(Duration::from_secs(5), session.retire()).await;
        if matches!(&retired, Ok(Ok(status)) if status.lifecycle == LifecycleState::Retired
            && status.target.is_none() && status.handoff_id.is_none()
            && status.live_server.discovery_key == entry.discovery_key)
        {
            return Ok(journal::ForcedStopProof::CooperativeRetirement);
        }
    }
    authorize_hard_stop()?;
    peer.process
        .kill_and_wait(Duration::from_secs(5))
        .map_err(|error| ActivateError::UnitFailed {
            reason: format!("Muxe cannot confirm that broker {} (process {}) stopped: {error}. A stop may have been requested, but no replacement was started for this broker. Its saved stop intent is retained; retry with muxe activate --force to resolve this exact process", entry.discovery_key, entry.server_pid),
        })?;
    Ok(journal::ForcedStopProof::ProcessExited)
}

#[expect(
    clippy::too_many_lines,
    reason = "durable commit supersession authenticates the complete participant set before its ordered journal/stop/proof boundaries"
)]
pub(super) async fn resolve_commit<C: ControlPort, R: HostReloader>(
    cache: &Path,
    control: &C,
    _reloader: &R,
    saved: &mut ActivationJournal,
) -> Result<Vec<RecoveredStop>, ActivateError> {
    let existing = read_ledger(cache, saved)?;
    let mut ledger = existing.unwrap_or(CommitStops {
        schema_version: journal::FORCED_ACTIVATION_JOURNAL_SCHEMA_VERSION,
        activation: saved.activation_id,
        unit: saved.unit.clone(),
        participants: Vec::new(),
    });
    let mut captures: Vec<(usize, Option<C::Session>, Option<control::ForcedPeer>)> = Vec::new();
    for member in saved.members() {
        let old_ended = member.old == OldMemberProgress::ForceStopped
            || (matches!(
                member.old,
                OldMemberProgress::CommitIntent | OldMemberProgress::Committed
            ) && journal::has_old_retirement_receipt(
                &journal::activation_dir(cache),
                saved,
                member,
            )?);
        for role in [ParticipantRole::Target, ParticipantRole::Old] {
            if role == ParticipantRole::Old && old_ended {
                continue;
            }
            let row = match role {
                ParticipantRole::Old => saved
                    .old_registry
                    .iter()
                    .find(|row| {
                        row.socket == member.endpoint().as_path()
                            && row.discovery_key == member.member().as_str()
                    })
                    .ok_or_else(|| ActivateError::UnitFailed {
                        reason: "the saved activation record is missing the existing broker's registration".to_owned(),
                    })?,
                ParticipantRole::Target => {
                    &saved
                        .ready_proof()
                        .and_then(|proof| proof.member(&member.id))
                        .expect("commit certificate")
                        .entry
                }
            };
            let entry = typed_entry(row)?;
            let admitted = NativeBrokerAuthority::from_journal(cache, saved, &entry)?;
            if role == ParticipantRole::Old
                && (row.registration_id.is_none() || row.live_server.is_none())
            {
                return Err(ActivateError::UnitFailed {
                    reason: format!(
                        "Muxe cannot finish the saved stop record for broker {} (process {}): its original registration token or server identity is missing. Leave the record unchanged",
                        row.discovery_key, row.server_pid
                    ),
                });
            }
            let index = ledger.participants.iter().position(|participant| {
                participant.member == member.id && participant.role == role
            });
            let prior = index.map(|index| &ledger.participants[index].authority);
            let known_target = if role == ParticipantRole::Old {
                captures.iter().find_map(|(index, _, peer)| {
                    let participant = &ledger.participants[*index];
                    (participant.member == member.id && participant.role == ParticipantRole::Target)
                        .then_some(peer.as_ref())
                        .flatten()
                })
            } else {
                None
            };
            let (session, peer, authority) =
                capture_participant(control, &admitted, prior, true, known_target).await?;
            let old_record = match role {
                ParticipantRole::Old => member.old_record.clone(),
                ParticipantRole::Target => Some(saved.target_record.clone()),
            };
            let index = if let Some(index) = index {
                ledger.participants[index].authority = authority;
                index
            } else {
                ledger.participants.push(ParticipantStop {
                    member: member.id.clone(),
                    role,
                    entry: row.clone(),
                    old_record,
                    authority,
                });
                ledger.participants.len() - 1
            };
            captures.push((index, session, peer));
        }
    }
    // Every participant authority is durable before the first irreversible stop.
    write_ledger(cache, saved, &ledger)?;
    for (index, mut session, peer) in captures {
        if let Some(peer) = peer {
            let member = saved
                .members()
                .iter()
                .find(|member| member.id == ledger.participants[index].member)
                .expect("validated stop member");
            let row = match ledger.participants[index].role {
                ParticipantRole::Old => saved
                    .old_registry
                    .iter()
                    .find(|row| {
                        row.socket == member.endpoint().as_path()
                            && row.discovery_key == member.member().as_str()
                    })
                    .expect("validated old stop row"),
                ParticipantRole::Target => {
                    &saved
                        .ready_proof()
                        .and_then(|proof| proof.member(&member.id))
                        .expect("validated target stop row")
                        .entry
                }
            };
            let proof = stop_participant::<C>(&mut session, peer, row, || {
                ledger.participants[index].authority.hard_stop_intent = true;
                write_ledger(cache, saved, &ledger)
            })
            .await?;
            ledger.participants[index].authority.proof = Some(proof);
            write_ledger(cache, saved, &ledger)?;
            if let Some(endpoint) = &ledger.participants[index].authority.endpoint {
                endpoint.remove_stopped().map_err(|error| ActivateError::UnitFailed {
                    reason: format!("Broker {} stopped serving requests. Muxe could not safely remove its original socket: {error}. No termination signal was sent during this cleanup check. Recovery details were kept at {}. Retry with muxe activate --force", ledger.participants[index].entry.discovery_key, ledger_path(cache, saved).display()),
                })?;
            }
        }
        if ledger.participants[index].role == ParticipantRole::Old {
            let member_index = saved
                .members()
                .iter()
                .position(|member| member.id == ledger.participants[index].member)
                .expect("validated old member");
            if saved.members()[member_index].old != OldMemberProgress::ForceStopped {
                saved.enter_committing();
                saved.members_mut()[member_index].old = OldMemberProgress::CommitIntent;
                journal::write_journal(cache, saved)?;
                let server = ledger.participants[index]
                    .entry
                    .live_server
                    .as_ref()
                    .ok_or_else(|| ActivateError::UnitFailed {
                        reason: "the existing broker's stop-confirmation record is missing its saved server ID".to_owned(),
                    })?;
                if !journal::has_old_retirement_receipt(
                    &journal::activation_dir(cache),
                    saved,
                    &saved.members()[member_index],
                )? {
                    journal::write_old_retirement_receipt(
                        cache,
                        saved,
                        &saved.members()[member_index],
                        &muxe_protocol::wire::ServerId::new(server.clone()),
                    )?;
                }
                saved.members_mut()[member_index].old = OldMemberProgress::Committed;
                journal::write_journal(cache, saved)?;
            }
        }
    }
    saved.enter_committing();
    journal::write_journal(cache, saved)?;
    publish_commit_artifacts(cache, saved)?;
    let stopped = ledger
        .participants
        .into_iter()
        .filter(|participant| participant.role == ParticipantRole::Target)
        .map(|participant| RecoveredStop {
            entry: participant.entry,
            old_record: participant.old_record,
            authority: participant.authority,
        })
        .collect();
    Ok(stopped)
}

pub(super) fn validate_admission_scope(saved: &ActivationJournal) -> Result<(), ActivateError> {
    let units = select_units(&saved.old_registry, HostScope::All, None)?;
    if units.len() != 1
        || units[0].unit_kind() != saved.unit
        || units[0].entries().len() != saved.old_registry.len()
        || units[0].entries().len() != saved.members().len()
        || units[0].entries().iter().any(|entry| {
            !saved.members().iter().any(|member| {
                member.member().as_str() == entry.discovery_key().as_str()
                    && member.endpoint().as_path() == entry.socket()
            })
        })
    {
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "Muxe cannot continue forced recovery using {}: the saved records disagree about which brokers belong to this activation. Leave the record unchanged. Muxe will not use it to stop brokers",
                saved.unit.journal_name()
            ),
        });
    }
    Ok(())
}

fn release_retired_target_rows(
    cache: &Path,
    saved: &ActivationJournal,
) -> Result<(), ActivateError> {
    let registry = Registry::open(cache)?;
    for member in saved.members() {
        if member.target != TargetMemberProgress::Retired {
            continue;
        }
        let intent = member
            .target_retirement
            .as_ref()
            .expect("validated retired target");
        let Some(proof) = intent.stopped_registration_proof(cache, saved, member)? else {
            continue;
        };
        if let Some(endpoint) = proof.endpoint() {
            endpoint.remove_stopped().map_err(|error| ActivateError::UnitFailed {
                reason: format!("Muxe cannot remove the stopped replacement broker's socket for {}: {error}. Its matching stop-confirmation record was kept; no unrelated socket was removed while checking this socket", member.member().as_str()),
            })?;
        }
        registry.release_stopped_target(&proof)?;
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "forced recovery retains locked decisions and dispatches only their authenticated participant state"
)]
pub(super) async fn recover_for_force<C, S, R, P>(
    inputs: &ActivateInputs<'_, C, S, R, P>,
) -> Result<Vec<ForcedRecovery>, ActivateError>
where
    C: ControlPort,
    R: HostReloader,
{
    let mut pending = Vec::new();
    for (path, loaded) in journal::list_journals(inputs.cache_dir)? {
        let snapshot = loaded?;
        validate_admission_scope(&snapshot)?;
        let selected = match select_units(
            &snapshot.old_registry,
            inputs.scope,
            inputs.current.as_ref(),
        ) {
            Ok(units) => !units.is_empty(),
            Err(ActivateError::NoLiveUnits) => false,
            Err(error) => return Err(error),
        };
        if !selected {
            continue;
        }
        let lock = journal::acquire_unit_lock(inputs.cache_dir, &snapshot.unit)?;
        if matches!(std::fs::symlink_metadata(&path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
        {
            continue;
        }
        let original = journal::read_journal(&path)?;
        if original.unit != snapshot.unit {
            return Err(ActivateError::UnitFailed {
                reason: "the broker group in the saved activation record changed while Muxe acquired its forced-recovery lock".to_owned(),
            });
        }
        validate_admission_scope(&original)?;
        let skips_ordinary = original.directive() == TransactionDirective::Commit
            || (original.is_forced()
                && (original.directive() == TransactionDirective::RetryForce
                    || (original.directive() == TransactionDirective::Prepare
                        && original
                            .members()
                            .iter()
                            .all(|member| member.target == TargetMemberProgress::Absent))));
        if !skips_ordinary {
            let outcome = recover_one(
                inputs.cache_dir,
                inputs.control,
                inputs.reloader,
                original,
                &path,
                inputs.logger,
            )
            .await;
            if !matches!(outcome, RecoveryOutcome::Preserved { .. }) {
                continue;
            }
        }
        let mut saved = journal::read_journal(&path)?;
        if saved.is_forced()
            && matches!(
                saved.directive(),
                TransactionDirective::RetryForce | TransactionDirective::Prepare
            )
        {
            for index in 0..saved.members().len() {
                if saved.members()[index].old == OldMemberProgress::ForceStopIntent {
                    let stop = saved.members()[index]
                        .forced_stop
                        .as_ref()
                        .expect("validated forced intent");
                    if stop
                        .process
                        .is_gone()
                        .map_err(|error| ActivateError::UnitFailed {
                            reason: error.to_string(),
                        })?
                    {
                        saved.members_mut()[index]
                            .forced_stop
                            .as_mut()
                            .expect("forced intent")
                            .proof = Some(journal::ForcedStopProof::ProcessExited);
                        saved.members_mut()[index].old = OldMemberProgress::ForceStopped;
                        journal::write_journal(inputs.cache_dir, &saved)?;
                    }
                }
            }
            release_retired_target_rows(inputs.cache_dir, &saved)?;
            pending.push(ForcedRecovery::pending(saved, lock));
            continue;
        }
        if saved.directive() == TransactionDirective::RollBack {
            let targets_ended = saved.members().iter().all(|member| {
                member.target == TargetMemberProgress::Absent
                    || (member.target == TargetMemberProgress::Retired
                        && journal::has_target_retirement_receipt(
                            &journal::activation_dir(inputs.cache_dir),
                            &saved,
                            member,
                        )
                        .is_ok_and(|proof| proof))
            });
            let bridge_safe = saved.bridge().is_none_or(|bridge| {
                matches!(
                    bridge.progress,
                    BridgeProgress::ArtifactsPending
                        | BridgeProgress::ArtifactsReady
                        | BridgeProgress::Restored { .. }
                )
            });
            let rows = Registry::open(inputs.cache_dir)?.entries()?;
            let old_rows_known = saved.members().iter().all(|member| {
                let old = saved.old_registry.iter().find(|row| {
                    row.socket == member.endpoint().as_path()
                        && row.discovery_key == member.member().as_str()
                });
                let mut current = rows
                    .iter()
                    .filter(|row| row.socket == member.endpoint().as_path());
                current.next() == old && current.next().is_none()
            });
            if targets_ended && bridge_safe && old_rows_known {
                pending.push(ForcedRecovery::pending(saved, lock));
                continue;
            }
            return Err(ActivateError::UnitFailed {
                reason: format!(
                    "Muxe cannot continue forced recovery at {}: it could not verify every existing broker registration, confirm every replacement broker stopped, or confirm the bridge was restored. The saved activation decision was kept",
                    path.display()
                ),
            });
        }
        if saved.directive() == TransactionDirective::Commit && saved.has_commit_certificate() {
            let rows = saved
                .ready_proof()
                .expect("validated commit certificate")
                .incarnations
                .iter()
                .map(|proof| proof.entry.clone())
                .collect();
            let stopped = read_ledger(inputs.cache_dir, &saved)?.map_or_else(Vec::new, |ledger| {
                ledger
                    .participants
                    .into_iter()
                    .filter(|participant| {
                        participant.role == ParticipantRole::Target
                            && participant.authority.proof.is_some()
                    })
                    .map(|participant| RecoveredStop {
                        entry: participant.entry,
                        old_record: participant.old_record,
                        authority: participant.authority,
                    })
                    .collect()
            });
            pending.push(ForcedRecovery {
                journal: saved,
                _lock: lock,
                rows,
                stopped,
            });
            continue;
        }
        return Err(ActivateError::UnitFailed {
            reason: format!(
                "Muxe cannot continue forced recovery at {}: the saved activation record lacks the process identities or confirmations needed to determine what happened to its brokers. The saved activation decision was kept",
                path.display()
            ),
        });
    }
    Ok(pending)
}
