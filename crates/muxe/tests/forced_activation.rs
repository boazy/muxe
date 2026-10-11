//! Real native commands against retained, explicitly scoped owned hosts.
#[path = "support/retire_inode.rs"]
mod retire_inode;
#[expect(
    dead_code,
    reason = "shared owned-host harness includes Zellij and upgrade helpers unused by this Herdr-only test target"
)]
mod support;

use muxe::lifecycle::{Registry, control::ControlClient};
use muxe_protocol::control::{ActivationPhase, LifecycleState};
use std::{io, path::Path, time::Duration};
use support::{
    OwnedHerdrServer, apply_scoped_env, init_shared_dirs, run_cli_bounded, short_tempdir,
    spawn_serve_herdr,
};

#[tokio::test]
#[ignore = "requires MUXE_LIVE_HOSTS_APPROVED=true and explicit MUXE_HERDR_BINARY; starts only owned TempDir hosts"]
async fn force_replaces_healthy_current_herdr_and_reuses_new_registration() {
    Box::pin(replace_current_herdr(ReplacementCase::Healthy)).await;
}

#[tokio::test]
#[ignore = "requires MUXE_LIVE_HOSTS_APPROVED=true and explicit MUXE_HERDR_BINARY; stops only the retained owned broker child"]
async fn force_replaces_unresponsive_owned_herdr_using_exact_process_stop() {
    Box::pin(replace_current_herdr(ReplacementCase::Unresponsive)).await;
}

#[tokio::test]
#[ignore = "requires MUXE_LIVE_HOSTS_APPROVED=true and explicit MUXE_HERDR_BINARY; injects failure only after the owned old broker stops"]
async fn force_retries_retained_stop_intent_without_live_registry_rows() {
    Box::pin(replace_current_herdr(ReplacementCase::RetryAfterFailure)).await;
}

#[tokio::test]
#[ignore = "requires MUXE_LIVE_HOSTS_APPROVED=true and explicit MUXE_HERDR_BINARY; fails only after exact target identity is verified before Ready"]
async fn force_herdr_retries_after_registered_target_retirement() {
    Box::pin(replace_current_herdr(
        ReplacementCase::RetryAfterRegisteredTarget,
    ))
    .await;
}

#[tokio::test]
#[ignore = "requires explicit owned Herdr approval; replaces only the test-owned socket after a real Retire ACK"]
async fn authenticated_retire_ack_survives_replaced_inode_without_hard_stop() {
    Box::pin(replace_current_herdr(ReplacementCase::AckInodeReplacement)).await;
}

#[tokio::test]
#[ignore = "requires explicit owned Herdr approval; preserves a real durable Ready decision before a new force attempt"]
async fn force_supersedes_owned_ready_decision_without_fake_rollback() {
    Box::pin(replace_current_herdr(ReplacementCase::ReadySupersession)).await;
}

#[derive(Clone, Copy)]
enum ReplacementCase {
    Healthy,
    Unresponsive,
    RetryAfterFailure,
    RetryAfterRegisteredTarget,
    AckInodeReplacement,
    ReadySupersession,
}

fn require(condition: bool, detail: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(detail.to_owned()))
    }
}

async fn replace_current_herdr(case: ReplacementCase) {
    assert_eq!(
        std::env::var("MUXE_LIVE_HOSTS_APPROVED").as_deref(),
        Ok("true")
    );
    let binary = Path::new(env!("CARGO_BIN_EXE_muxe"));
    let herdr = std::path::PathBuf::from(
        std::env::var_os("MUXE_HERDR_BINARY").expect("explicit owned Herdr binary"),
    );
    assert!(herdr.is_absolute());
    let root = short_tempdir("muxe-force-").expect("owned short root");
    let scoped = root.path().join("scope");
    let (config, cache) = init_shared_dirs(binary, &scoped)
        .await
        .expect("owned config");
    let mut server = OwnedHerdrServer::start(&herdr, root.path(), &scoped, "force")
        .await
        .expect("owned Herdr server");
    let mut original = spawn_serve_herdr(
        "force-current",
        binary,
        &herdr,
        server.socket(),
        server.discovery_key(),
        &scoped,
        &config,
        &cache,
    )
    .await
    .expect("owned ordinary broker");
    let endpoint = original.endpoint.clone();
    let result: io::Result<()> = Box::pin(async {
        let policy = OwnedHerdrPolicy {
            binary,
            herdr: &herdr,
            config: &config,
            cache: &cache,
            scoped: &scoped,
        };
        Box::pin(run_owned_replacement(
            case,
            &policy,
            &endpoint,
            &mut original.child,
            &mut server,
        ))
        .await
    })
    .await;
    let old_cleanup = original.child.terminate_and_reap().await;
    let host_cleanup = server.shutdown().await;
    if let Err(error) = result {
        let preserved = root.keep();
        panic!(
            "owned forced activation failed: {error}; retained {}\nold cleanup: {old_cleanup:?}\nhost cleanup: {host_cleanup:?}",
            preserved.display()
        );
    }
    old_cleanup.expect("owned old child reaped");
    host_cleanup.expect("owned host reaped");
}

async fn run_owned_replacement(
    case: ReplacementCase,
    policy: &OwnedHerdrPolicy<'_>,
    endpoint: &Path,
    original: &mut support::OwnedChild,
    server: &mut OwnedHerdrServer,
) -> io::Result<()> {
    let before = Registry::open(policy.cache)
        .map_err(io::Error::other)?
        .entries()
        .map_err(io::Error::other)?;
    require(
        before.len() == 1,
        "owned old broker must have one registry row",
    )?;
    let mut client = ControlClient::connect(endpoint)
        .await
        .map_err(io::Error::other)?;
    let status = client.status().await.map_err(io::Error::other)?;
    require(
        status.lifecycle == LifecycleState::Running && status.phase == ActivationPhase::Ordinary,
        "owned initial broker must be ordinary and running",
    )?;
    drop(client);
    if matches!(case, ReplacementCase::AckInodeReplacement) {
        return Box::pin(acknowledge_then_replace_inode(
            policy,
            endpoint,
            status.current,
        ))
        .await;
    }
    Box::pin(prepare_owned_replacement_fault(
        case,
        policy,
        endpoint,
        original,
        &before[0],
        &status.current,
    ))
    .await?;
    let mut command = tokio::process::Command::new(policy.binary);
    command.args(["activate", "--force", "--host", "herdr"]);
    apply_scoped_env(&mut command, policy.scoped);
    command.env("HERDR_SOCKET_PATH", server.socket());
    let output = run_cli_bounded("force-healthy-current", &mut command).await?;
    eprintln!(
        "force stdout:\n{}\nforce stderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        return Err(io::Error::other("actual forced activation failed"));
    }
    let after = Registry::open(policy.cache)
        .map_err(io::Error::other)?
        .entries()
        .map_err(io::Error::other)?;
    require(after.len() == 1, "force must leave one broker registration")?;
    require(
        before[0].server_pid != after[0].server_pid
            && before[0].registration_id != after[0].registration_id,
        "force must replace current broker PID and nonce, not return Unchanged",
    )?;
    let mut client = ControlClient::connect(endpoint)
        .await
        .map_err(io::Error::other)?;
    let status = client.status().await.map_err(io::Error::other)?;
    require(
        status.phase == ActivationPhase::TargetCommitted
            && status.lifecycle == LifecycleState::Running,
        "forced replacement must be committed and running",
    )?;
    drop(client);
    require(
        server.try_wait()?.is_none(),
        "owned multiplexer server must remain alive",
    )?;
    let exit = tokio::time::timeout(Duration::from_secs(5), original.wait())
        .await
        .map_err(|_| io::Error::other("ordinary old broker did not finish retirement"))??;
    require(
        exit.is_some_and(|status| {
            if matches!(case, ReplacementCase::Unresponsive) {
                std::os::unix::process::ExitStatusExt::signal(&status) == Some(nix::libc::SIGKILL)
            } else {
                status.success()
            }
        }),
        "old host authority must end before new activation",
    )?;
    verify_ordinary_activation_reuses_registration(policy, server, endpoint, &after[0]).await
}

async fn prepare_owned_replacement_fault(
    case: ReplacementCase,
    policy: &OwnedHerdrPolicy<'_>,
    endpoint: &Path,
    original: &mut support::OwnedChild,
    before: &muxe::lifecycle::registry::BrokerEntry,
    target: &muxe_protocol::control::CompatibilityRecord,
) -> io::Result<()> {
    if matches!(case, ReplacementCase::Unresponsive) {
        // The retained Child proves this PID is the fresh test-owned
        // broker; SIGSTOP makes both Status and Retire unresponsive.
        require(
            original.suspend()? == before.server_pid,
            "registry PID differs from retained owned child",
        )?;
    }
    if matches!(
        case,
        ReplacementCase::RetryAfterFailure
            | ReplacementCase::RetryAfterRegisteredTarget
            | ReplacementCase::ReadySupersession
    ) {
        let registered = !matches!(case, ReplacementCase::RetryAfterFailure);
        let step = match case {
            ReplacementCase::ReadySupersession => muxe::lifecycle::ActivateStep::ReadinessRecorded,
            ReplacementCase::RetryAfterRegisteredTarget => {
                muxe::lifecycle::ActivateStep::ReadyVerified
            }
            _ => muxe::lifecycle::ActivateStep::OldPrepared,
        };
        Box::pin(fail_after_owned_stop(policy, target.clone(), step)).await?;
        let rows = Registry::open(policy.cache)
            .map_err(io::Error::other)?
            .entries()
            .map_err(io::Error::other)?;
        if registered {
            require(
                rows.len() == 1 && rows[0].server_pid != before.server_pid,
                "fault must retain the exact stopped registered target, not an old placeholder",
            )?;
        } else {
            require(
                rows.is_empty(),
                "pre-spawn failure must leave no live registry row",
            )?;
        }
        if !matches!(case, ReplacementCase::ReadySupersession) {
            require(
                ControlClient::connect(endpoint).await.is_err(),
                "stopped broker endpoint must not remain live",
            )?;
        }
    }
    Ok(())
}

async fn verify_ordinary_activation_reuses_registration(
    policy: &OwnedHerdrPolicy<'_>,
    server: &OwnedHerdrServer,
    endpoint: &Path,
    after: &muxe::lifecycle::registry::BrokerEntry,
) -> io::Result<()> {
    let mut normal = tokio::process::Command::new(policy.binary);
    normal.args(["activate", "--host", "herdr"]);
    apply_scoped_env(&mut normal, policy.scoped);
    normal.env("HERDR_SOCKET_PATH", server.socket());
    let output = run_cli_bounded("normal-after-force", &mut normal).await?;
    eprintln!(
        "normal activation stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    require(
        output.status.success(),
        "committed handoff must not block subsequent ordinary activation",
    )?;
    let normal_rows = Registry::open(policy.cache)
        .map_err(io::Error::other)?
        .entries()
        .map_err(io::Error::other)?;
    require(
        normal_rows.len() == 1 && normal_rows[0].registration_id == after.registration_id,
        "ordinary already-current activation must reuse the new registration",
    )?;
    support::retire_broker(endpoint, "force-target").await?;
    Ok(())
}

struct OwnedHerdrPolicy<'a> {
    binary: &'a Path,
    herdr: &'a Path,
    config: &'a Path,
    cache: &'a Path,
    scoped: &'a Path,
}

impl muxe::lifecycle::TargetSpawnSelector for OwnedHerdrPolicy<'_> {
    fn select(&self, _unit: &muxe::lifecycle::UnitKind) -> &dyn muxe::lifecycle::TargetSpawnPolicy {
        self
    }
}

impl muxe::lifecycle::TargetSpawnPolicy for OwnedHerdrPolicy<'_> {
    fn render(
        &self,
        member: &muxe::lifecycle::SpawnMember<'_>,
    ) -> Result<(std::path::PathBuf, Vec<std::ffi::OsString>), muxe::lifecycle::ActivateError> {
        assert!(matches!(
            member.unit,
            muxe::lifecycle::UnitKind::Herdr { .. }
        ));
        let program = self.binary.to_path_buf();
        let args = muxe_broker::ServeHerdrSpawn {
            binary: program.clone(),
            socket: member.authority.endpoint.as_path().to_path_buf(),
            herdr_binary: self.herdr.to_path_buf(),
            herdr_socket: std::path::PathBuf::from(member.authority.member.as_str()),
            config: self.config.to_path_buf(),
            cache_dir: self.cache.to_path_buf(),
            handoff: Some(member.authority.handoff_id),
            activation_journal: Some(member.journal_path.clone()),
        }
        .argv()
        .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
        Ok((program, args))
    }
}

impl muxe::lifecycle::BrokerSpawner for OwnedHerdrPolicy<'_> {
    fn spawn_target(
        &self,
        request: &muxe::lifecycle::SpawnRequest,
    ) -> Result<muxe::lifecycle::TargetHandle, muxe::lifecycle::ActivateError> {
        use std::os::unix::process::CommandExt as _;

        let mut command = tokio::process::Command::new(&request.program);
        command.args(&request.args);
        apply_scoped_env(&mut command, self.scoped);
        command.current_dir(self.scoped);
        command
            .as_std_mut()
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit());
        command.as_std_mut().process_group(0);
        command
            .as_std_mut()
            .spawn()
            .map(muxe::lifecycle::TargetHandle::new)
            .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))
    }
    fn stop_target(
        &self,
        handle: &mut muxe::lifecycle::TargetHandle,
    ) -> Result<(), muxe::lifecycle::ActivateError> {
        muxe::lifecycle::BrokerSpawner::stop_target(&muxe::lifecycle::ProcessSpawner, handle)
    }
}

async fn fail_after_owned_stop(
    policy: &OwnedHerdrPolicy<'_>,
    target: muxe_protocol::control::CompatibilityRecord,
    step: muxe::lifecycle::ActivateStep,
) -> io::Result<()> {
    use muxe::lifecycle::{
        ActivateHooks, ActivateInputs, LiveControl, LivePreflight, UnitOutcome, ZellijCliReloader,
        activate, journal,
    };
    let preflight = LivePreflight {
        config_path: policy.config.to_path_buf(),
        cache_dir: policy.cache.to_path_buf(),
        herdr_binary: Some(policy.herdr.to_path_buf()),
        zellij_exe: None,
        logger: None,
    };
    let report = Box::pin(activate(ActivateInputs {
        force: true,
        config_dir: policy.config.parent().expect("owned config directory"),
        cache_dir: policy.cache,
        target,
        staged_bridge: None,
        scope: muxe::cli::HostScope::Herdr,
        current: None,
        control: &LiveControl,
        spawner: policy,
        reloader: &ZellijCliReloader { program: None },
        preflight: &preflight,
        spawn_policy: policy,
        readiness_deadline: Duration::from_secs(10),
        poll_interval: Duration::from_millis(50),
        hooks: ActivateHooks {
            fail_after: Some(step),
        },
        logger: None,
    }))
    .await
    .map_err(io::Error::other)?;
    eprintln!("injected forced failure report: {report:?}");
    require(
        matches!(report.units.as_slice(), [UnitOutcome::Failed { .. }]),
        "a stopped old broker cannot be reported as rolled back",
    )?;
    let saved = journal::list_journals(policy.cache).map_err(io::Error::other)?;
    require(saved.len() == 1, "force failure must retain one journal")?;
    let expected = if step == muxe::lifecycle::ActivateStep::ReadinessRecorded {
        journal::TransactionDirective::Commit
    } else {
        journal::TransactionDirective::RetryForce
    };
    require(
        saved[0]
            .1
            .as_ref()
            .map_err(|error| io::Error::other(error.to_string()))?
            .directive()
            == expected,
        "failure must retain its actual durable decision direction",
    )?;
    Ok(())
}

async fn acknowledge_then_replace_inode(
    policy: &OwnedHerdrPolicy<'_>,
    endpoint: &Path,
    target: muxe_protocol::control::CompatibilityRecord,
) -> io::Result<()> {
    use muxe::lifecycle::{
        ActivateHooks, ActivateInputs, LivePreflight, UnitOutcome, ZellijCliReloader, activate,
        journal,
    };
    let control = retire_inode::ReplacingControl {
        endpoint: endpoint.to_path_buf(),
        listener: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
    };
    let preflight = LivePreflight {
        config_path: policy.config.to_path_buf(),
        cache_dir: policy.cache.to_path_buf(),
        herdr_binary: Some(policy.herdr.to_path_buf()),
        zellij_exe: None,
        logger: None,
    };
    let report = Box::pin(activate(ActivateInputs {
        force: true,
        config_dir: policy.config.parent().expect("owned config directory"),
        cache_dir: policy.cache,
        target,
        staged_bridge: None,
        scope: muxe::cli::HostScope::Herdr,
        current: None,
        control: &control,
        spawner: policy,
        reloader: &ZellijCliReloader { program: None },
        preflight: &preflight,
        spawn_policy: policy,
        readiness_deadline: Duration::from_secs(10),
        poll_interval: Duration::from_millis(50),
        hooks: ActivateHooks::default(),
        logger: None,
    }))
    .await
    .map_err(io::Error::other)?;
    eprintln!("ACK/inode fault report: {report:?}");
    require(
        matches!(report.units.as_slice(), [UnitOutcome::Failed { .. }]),
        "replaced inode must refuse new activation",
    )?;
    let saved = journal::list_journals(policy.cache).map_err(io::Error::other)?;
    require(
        saved.len() == 1,
        "retirement proof must be durable despite cleanup failure",
    )?;
    let saved = saved[0]
        .1
        .as_ref()
        .map_err(|error| io::Error::other(error.to_string()))?;
    let stop = saved.members()[0]
        .forced_stop
        .as_ref()
        .expect("forced stop record");
    require(
        stop.proof == Some(journal::ForcedStopProof::CooperativeRetirement)
            && !stop.hard_stop_intent,
        "authenticated retirement must not fall back to termination when the inode changes",
    )?;
    require(
        control.listener.lock().await.is_some(),
        "unknown replacement listener must not be removed",
    )?;
    control.listener.lock().await.take();
    Ok(())
}
