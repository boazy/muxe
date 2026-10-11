//! Real repeated and irreversible activation rehearsals on owned pinned hosts.
use super::*;
use muxe::lifecycle::{
    self, ActivateHooks, ActivateInputs, ActivateStep, BrokerSpawner, LiveControl, LivePreflight,
    ProcessSpawner, SpawnMember, SpawnRequest, StagedBridge, TargetHandle, TargetSpawnPolicy,
    TargetSpawnSelector, UnitKind, UnitOutcome, ZellijCliReloader, control::ControlClient,
};
use muxe_protocol::control::{ActivationPhase, LifecycleState};

struct ScopedZellijSpawn<'a> {
    rig: &'a Rig,
    binary: &'a Path,
    zellij: &'a Path,
    delayed: bool,
}
impl TargetSpawnSelector for ScopedZellijSpawn<'_> {
    fn select(&self, unit: &UnitKind) -> &dyn TargetSpawnPolicy {
        assert!(matches!(unit, UnitKind::Zellij { .. }));
        self
    }
}
impl TargetSpawnPolicy for ScopedZellijSpawn<'_> {
    fn render(
        &self,
        member: &SpawnMember<'_>,
    ) -> Result<(PathBuf, Vec<std::ffi::OsString>), lifecycle::ActivateError> {
        assert!(matches!(member.unit, UnitKind::Zellij { .. }));
        assert_eq!(
            member
                .observed_bridge_member
                .map(lifecycle::BridgeMemberId::as_str),
            Some(member.authority.member.as_str())
        );
        let program = self.binary.to_path_buf();
        let args = muxe_broker::ServeZellijSpawn {
            binary: program.clone(),
            socket: member.authority.endpoint.as_path().to_path_buf(),
            zellij_exe: self.zellij.to_path_buf(),
            session: member.authority.member.as_str().to_owned(),
            config: self.rig.config_file.clone(),
            cache_dir: self.rig.cache_dir.clone(),
            handoff: Some(member.authority.handoff_id),
            activation_journal: Some(member.journal_path.clone()),
        }
        .argv()
        .map_err(|error| lifecycle::ActivateError::Spawn(error.to_string()))?;
        Ok((program, args))
    }
}
impl BrokerSpawner for ScopedZellijSpawn<'_> {
    fn spawn_target(
        &self,
        request: &SpawnRequest,
    ) -> Result<TargetHandle, lifecycle::ActivateError> {
        use std::os::unix::process::CommandExt as _;

        let mut command = if self.delayed {
            let mut command = tokio::process::Command::new("/bin/sh");
            command.args(["-c", "sleep 0.5; exec \"$@\"", "owned-delayed-target"]);
            command.arg(&request.program);
            command
        } else {
            tokio::process::Command::new(&request.program)
        };
        command.args(&request.args);
        self.rig
            .zellij
            .as_ref()
            .expect("owned Zellij host")
            .apply_host_scoped_env(&mut command, &self.rig.scoped_root);
        command.current_dir(&self.rig.scoped_root);
        command
            .as_std_mut()
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        command.as_std_mut().process_group(0);
        let child = command
            .as_std_mut()
            .spawn()
            .map_err(|error| lifecycle::ActivateError::Spawn(error.to_string()))?;
        Ok(TargetHandle::new(child))
    }
    fn stop_target(&self, handle: &mut TargetHandle) -> Result<(), lifecycle::ActivateError> {
        ProcessSpawner.stop_target(handle)
    }
}

async fn library_activation(
    rig: &Rig,
    binary: &Path,
    zellij: &Path,
    force: bool,
    step: Option<ActivateStep>,
    delayed: bool,
) -> io::Result<lifecycle::ActivateReport> {
    let policy = ScopedZellijSpawn {
        rig,
        binary,
        zellij,
        delayed,
    };
    let preflight = LivePreflight {
        config_path: rig.config_file.clone(),
        cache_dir: rig.cache_dir.clone(),
        herdr_binary: None,
        zellij_exe: Some(zellij.to_path_buf()),
        logger: None,
    };
    let package = binary
        .parent()
        .expect("validated installation root")
        .join("lib/muxe/muxe-zellij.wasm");
    let target = muxe::compatibility::embedded_record()
        .map_err(io::Error::other)?
        .handoff;
    Box::pin(lifecycle::activate(ActivateInputs {
        force,
        config_dir: rig.config_file.parent().expect("owned config directory"),
        cache_dir: &rig.cache_dir,
        target,
        staged_bridge: Some(StagedBridge {
            bytes: std::fs::read(package)?,
        }),
        scope: muxe::cli::HostScope::Zellij,
        current: None,
        control: &LiveControl,
        spawner: &policy,
        reloader: &ZellijCliReloader {
            program: Some(zellij.to_path_buf()),
        },
        preflight: &preflight,
        spawn_policy: &policy,
        readiness_deadline: std::time::Duration::from_mins(1),
        poll_interval: std::time::Duration::from_millis(50),
        hooks: ActivateHooks { fail_after: step },
        logger: None,
    }))
    .await
    .map_err(io::Error::other)
}

async fn cli_activation(rig: &Rig, binary: &Path, force: bool) -> io::Result<()> {
    let mut command = tokio::process::Command::new(binary);
    command.args(["activate", "--host", "zellij"]);
    if force {
        command.arg("--force");
    }
    rig.activate_environment()
        .apply_owned_scoped_env(&mut command, &rig.scoped_root);
    let output = run_cli_bounded("repeat-activation", &mut command).await?;
    eprintln!(
        "[repeat] activation stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err(io::Error::other("actual scoped activation failed"));
    }
    Ok(())
}

fn stale_owned_bridge(rig: &Rig, tag: u8) -> io::Result<()> {
    let identity = muxe::integration::bridge_identity(
        rig.config_file.parent().expect("owned config directory"),
    )
    .map_err(io::Error::other)?;
    let stable = identity.stable_path(std::ffi::OsStr::new(muxe::integration::BRIDGE_FILE_NAME));
    let mut bytes = std::fs::read(&stable)?;
    // A valid, semantically ignored WASM custom section gives a real compatible
    // old bridge distinct bytes without inventing a native compatibility record.
    bytes.extend_from_slice(&[0, 4, 2, b'x', b'y', tag]);
    std::fs::write(&stable, &bytes)?;
    std::fs::File::open(&stable)?.sync_all()?;
    let mut receipt = muxe::integration::receipt::load(identity.directory())
        .map_err(io::Error::other)?
        .expect("owned installed receipt");
    receipt.bridge.installed_digest = muxe::integration::receipt::Sha256Digest::from_bytes(&bytes);
    muxe::integration::receipt::store(identity.directory(), &receipt).map_err(io::Error::other)
}

fn current_zellij_row(rig: &Rig, endpoint: &Path) -> io::Result<lifecycle::BrokerEntry> {
    let rows = lifecycle::Registry::open(&rig.cache_dir)
        .map_err(io::Error::other)?
        .entries()
        .map_err(io::Error::other)?;
    let mut selected = rows.into_iter().filter(|row| row.socket == endpoint);
    let row = selected
        .next()
        .ok_or_else(|| io::Error::other("owned broker has no registry row"))?;
    assert!(selected.next().is_none());
    Ok(row)
}

async fn attach_menu_after_rollback(
    rig: &mut Rig,
    binary: &Path,
    zellij: &Path,
    session: &str,
) -> io::Result<()> {
    let host = rig.zellij.as_ref().expect("owned Zellij host");
    let mut focus = tokio::process::Command::new(zellij);
    focus.args([
        "--session",
        session,
        "action",
        "focus-pane-id",
        "terminal_0",
    ]);
    host.apply_host_scoped_env(&mut focus, &rig.scoped_root);
    let output = run_cli_bounded("owned origin focus", &mut focus).await?;
    if !output.status.success() {
        return Err(io::Error::other(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    rig.pty_clients
        .first_mut()
        .expect("retained real client")
        .send_input(b"\x1bm")
        .await?;
    poll_until("menu attachment after rollback", READY_TIMEOUT, || async {
        assert_live_zellij_menu_pane(
            host,
            zellij,
            &rig.scoped_root,
            &rig.workdir,
            session,
            binary,
        )
        .await
        .map_err(|error| error.to_string())
    })
    .await
}

async fn owned_rig(case: &str) -> io::Result<(Rig, PathBuf, PathBuf, PathBuf)> {
    let binary = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let herdr = input_path("MUXE_HERDR_BINARY");
    let zellij = input_path("MUXE_ZELLIJ_BINARY");
    let mut rig = bring_hosts(
        case,
        &binary,
        &binary,
        &herdr,
        &zellij,
        &input_path("MUXE_ZELLIJ_FOREGROUND_BINARY"),
        &input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY"),
        &input_path("MUXE_ZELLIJ_PERMISSION_SEEDER"),
        &[("lifecycle", 1)],
        Some(&binary),
    )
    .await?;
    let (_, endpoints) =
        serve_old_brokers(&mut rig, &binary, &herdr, &zellij, case, &["lifecycle"]).await?;
    Ok((rig, binary, zellij, endpoints[0].clone()))
}

fn require(condition: bool, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message.to_owned()))
    }
}

async fn finish_preserving(mut rig: Rig, case: &str, result: io::Result<()>) -> io::Result<()> {
    let mut prior_cleanup = Ok(());
    for mut broker in std::mem::take(&mut rig.brokers) {
        let child_exited = broker.child.try_wait().is_ok_and(|status| status.is_some());
        let endpoint_gone = matches!(tokio::net::UnixStream::connect(&broker.endpoint).await,
            Err(error) if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused));
        if child_exited && endpoint_gone {
            match broker.child.terminate_and_reap().await {
                Ok(diagnostics) => eprintln!(
                    "[{case}] already stopped owned child {} reaped: {:?}\n{}",
                    diagnostics.tag,
                    diagnostics.exit_status,
                    diagnostics.stderr_tail.lossy()
                ),
                Err(error) => prior_cleanup = Err(error),
            }
        } else {
            rig.brokers.push(broker);
        }
    }
    let result = rig
        .finish(case, combine_body_and_cleanup(result, prior_cleanup))
        .await;
    if result.is_err() {
        eprintln!(
            "[{case}] failure evidence retained at {}",
            rig.root.keep().display()
        );
    }
    result
}

#[tokio::test]
#[ignore = "owned pinned hosts: explicit MUXE_LIVE_HOSTS_APPROVED=true and fresh matching installation/host helpers required"]
async fn normal_repeated_replacement_then_rollback_keeps_menu_attachable() {
    require_live_approval();
    let (mut rig, binary, zellij, endpoint) = owned_rig("normal-repeat").await.expect("owned rig");
    let result = Box::pin(async {
        let first = current_zellij_row(&rig, &endpoint)?;
        stale_owned_bridge(&rig, 1)?;
        cli_activation(&rig, &binary, false).await?;
        let second = current_zellij_row(&rig, &endpoint)?;
        require(first.server_pid != second.server_pid, "first ordinary replacement did not change broker PID")?;
        let mut client = ControlClient::connect(&endpoint).await.map_err(io::Error::other)?;
        let status = client.status().await.map_err(io::Error::other)?;
        require(status.phase == ActivationPhase::TargetCommitted, "first replacement did not commit target")?;
        drop(client);
        stale_owned_bridge(&rig, 2)?;
        cli_activation(&rig, &binary, false).await?;
        let third = current_zellij_row(&rig, &endpoint)?;
        require(second.server_pid != third.server_pid, "ordinary activation must replace a real committed target again")?;
        stale_owned_bridge(&rig, 3)?;
        let wrapper = rig.zellij.as_ref().expect("owned host").scoped_cli_wrapper(&rig.scoped_root)?;
        let report = Box::pin(library_activation(&rig, &binary, &wrapper, false, Some(ActivateStep::BridgeSwapped), false)).await?;
        eprintln!("[normal-repeat] failed third replacement report: {report:?}");
        require(matches!(report.units.as_slice(), [UnitOutcome::RolledBack { .. }]), "third replacement did not roll back; see report")?;
        require(current_zellij_row(&rig, &endpoint)? == third, "rollback did not preserve exact old ownership row")?;
        let mut client = ControlClient::connect(&endpoint).await.map_err(io::Error::other)?;
        let restored = client.status().await.map_err(io::Error::other)?;
        require(restored.phase == ActivationPhase::TargetCommitted
            && restored.handoff_id == third.handoff_id
            && restored.lifecycle == LifecycleState::Running, "rollback did not restore committed admission")?;
        require(third.live_server.as_deref() != Some(restored.live_server.server_id.as_str()),
            "authorized resume must advertise a fresh admission epoch, not the immutable ownership epoch")?;
        drop(client);
        attach_menu_after_rollback(&mut rig, &binary, &zellij, "lifecycle").await?;
        cli_activation(&rig, &binary, false).await?;
        let fourth = current_zellij_row(&rig, &endpoint)?;
        require(fourth.server_pid != third.server_pid && fourth.registration_id != third.registration_id,
            "fourth ordinary replacement after rollback must complete with fresh ownership")?;
        rig.zellij.as_mut().expect("retained owned host").check_servers_alive()?;
        Ok(())
    }).await;
    finish_preserving(rig, "normal-repeat", result)
        .await
        .expect("normal repeat/rollback/menu proof");
}

#[tokio::test]
#[ignore = "owned pinned hosts: explicit MUXE_LIVE_HOSTS_APPROVED=true and fresh matching installation/host helpers required"]
async fn force_no_live_retry_releases_exact_retired_row_and_waits_for_delayed_registration() {
    Box::pin(run_force_no_live(false)).await;
}

#[tokio::test]
#[ignore = "owned pinned hosts: actual CLI --force retry begins with no live registry brokers and a saved Zellij journal"]
async fn cli_force_retries_genuine_saved_zellij_no_live_state() {
    Box::pin(run_force_no_live(true)).await;
}

async fn retire_unrelated_owned_herdr(rig: &mut Rig) -> io::Result<()> {
    let rows = lifecycle::Registry::open(&rig.cache_dir)
        .map_err(io::Error::other)?
        .entries()
        .map_err(io::Error::other)?;
    for row in rows {
        if row.host_kind == "herdr" {
            retire_broker(&row.socket, "owned unrelated Herdr fixture").await?;
            if let Some(broker) = rig
                .brokers
                .iter_mut()
                .find(|broker| broker.endpoint == row.socket)
            {
                tokio::time::timeout(std::time::Duration::from_secs(5), broker.child.wait())
                    .await
                    .map_err(|_| io::Error::other("owned Herdr fixture retirement timed out"))??;
            }
        }
    }
    Ok(())
}

fn require_saved_force_retry(rig: &Rig) -> io::Result<()> {
    let saved = lifecycle::journal::list_journals(&rig.cache_dir).map_err(io::Error::other)?;
    require(
        saved.len() == 1,
        "force failure must retain exactly one saved retry",
    )?;
    require(
        saved[0]
            .1
            .as_ref()
            .map_err(|error| io::Error::other(error.to_string()))?
            .directive()
            == lifecycle::journal::TransactionDirective::RetryForce,
        "force failure must retain RetryForce direction",
    )
}

async fn run_force_no_live(cli_retry: bool) {
    require_live_approval();
    let case = if cli_retry {
        "cli-nolive"
    } else {
        "force-no-live"
    };
    let (mut rig, binary, _zellij, endpoint) = owned_rig(case).await.expect("owned rig");
    let result = Box::pin(async {
        if cli_retry {
            retire_unrelated_owned_herdr(&mut rig).await?;
        }
        let wrapper = rig
            .zellij
            .as_ref()
            .expect("owned host")
            .scoped_cli_wrapper(&rig.scoped_root)?;
        let before = current_zellij_row(&rig, &endpoint)?;
        let report = Box::pin(library_activation(
            &rig,
            &binary,
            &wrapper,
            true,
            Some(ActivateStep::BridgeSwapped),
            false,
        ))
        .await?;
        eprintln!("[force-no-live] failed replacement report: {report:?}");
        require(
            matches!(report.units.as_slice(), [UnitOutcome::Failed { .. }]),
            "irreversible stop must not claim rollback; see report",
        )?;
        let stale_target = current_zellij_row(&rig, &endpoint)?;
        require(
            stale_target.server_pid != before.server_pid,
            "fault must occur after real target registered",
        )?;
        require(
            ControlClient::connect(&endpoint).await.is_err(),
            "retired target listener remained live",
        )?;
        require_saved_force_retry(&rig)?;
        if cli_retry {
            require(
                lifecycle::Registry::open(&rig.cache_dir)
                    .map_err(io::Error::other)?
                    .probe()
                    .map_err(io::Error::other)?
                    .live
                    .is_empty(),
                "CLI NoLive test must have no live registry brokers before retry",
            )?;
            cli_activation(&rig, &binary, true).await?;
        } else {
            let report = Box::pin(library_activation(
                &rig, &binary, &wrapper, true, None, true,
            ))
            .await?;
            eprintln!("[force-no-live] delayed library retry report: {report:?}");
            require(
                matches!(report.units.as_slice(), [UnitOutcome::Committed { .. }]),
                "delayed no-live retry did not commit",
            )?;
        }
        let after = current_zellij_row(&rig, &endpoint)?;
        require(
            stale_target.server_pid != after.server_pid
                && stale_target.registration_id != after.registration_id,
            "no-live retry did not replace stale target identity",
        )?;
        let mut client = ControlClient::connect(&endpoint)
            .await
            .map_err(io::Error::other)?;
        require(
            client.status().await.map_err(io::Error::other)?.phase
                == ActivationPhase::TargetCommitted,
            "no-live target is not committed",
        )?;
        drop(client);
        cli_activation(&rig, &binary, true).await?;
        let current = current_zellij_row(&rig, &endpoint)?;
        require(
            current.server_pid != after.server_pid
                && current.registration_id != after.registration_id,
            "force must replace already-current broker",
        )?;
        rig.zellij
            .as_mut()
            .expect("retained owned host")
            .check_servers_alive()?;
        Ok(())
    })
    .await;
    finish_preserving(rig, case, result)
        .await
        .expect("forced no-live delayed registration proof");
}
