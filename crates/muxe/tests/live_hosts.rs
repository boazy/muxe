//! Live-host integration tests: target-only smoke, upgrade/rollback matrix,
//! and final-session reload failure.
//!
//! Every host is an owned foreground child under one fresh `TempDir` per
//! test; every muxe child runs under the scoped environment, so no default
//! user socket, configuration, or process is touched. One shared
//! `muxe init` config and cache keeps broker registrations visible to
//! activation; hosts separate by registry identity only. Zellij servers
//! run through the test-only foreground entrypoint (exact pinned server,
//! no mocks); interactive clients are retained PTY `attach` children. The
//! Herdr no-restart proof is the retained server child plus the unbroken
//! subscription stream, never socket device/inode comparison. Teardown is
//! reverse-order with awaited bounded cleanup on every failure path;
//! diagnostics are preserved.
//!
//! Each test is `#[ignore]`d by default AND requires explicit runtime
//! approval: `MUXE_LIVE_HOSTS_APPROVED` must be exactly `true`, checked
//! before any spawn. `--ignored` alone launches nothing, and env that is
//! accidentally present without approval launches nothing either.
//!
//! Typed inputs only (absolute paths, no command hook). Smoke needs no
//! predecessor; matrix and fault need both installations:
//!
//! ```text
//! MUXE_LIVE_HOSTS_APPROVED=true \
//! MUXE_TARGET_INSTALLATION=/path/to/target-install \
//! MUXE_HERDR_BINARY=/path/to/herdr \
//! MUXE_ZELLIJ_BINARY=/path/to/zellij \
//! MUXE_ZELLIJ_FOREGROUND_BINARY=/path/to/muxe-zellij-foreground \
//! MUXE_ZELLIJ_BOOTSTRAP_BINARY=/path/to/muxe-zellij-bootstrap \
//! MUXE_ZELLIJ_PERMISSION_SEEDER=/path/to/muxe-zellij-permit \
//! cargo test --locked -p muxe --test live_hosts -- --ignored --exact target_only_smoke
//! ```
//!
//! ```text
//! MUXE_LIVE_HOSTS_APPROVED=true \
//! MUXE_OLD_INSTALLATION=/path/to/old-install \
//! MUXE_TARGET_INSTALLATION=/path/to/target-install \
//! MUXE_ZELLIJ_FAULT_INJECTOR=/path/to/muxe-zellij-fault-injector \
//! [same host binaries: MUXE_HERDR_BINARY, MUXE_ZELLIJ_BINARY, \
//! MUXE_ZELLIJ_FOREGROUND_BINARY, MUXE_ZELLIJ_BOOTSTRAP_BINARY, \
//! MUXE_ZELLIJ_PERMISSION_SEEDER] \
//! cargo test --locked -p muxe --test live_hosts -- --ignored --exact upgrade_and_rollback
//! cargo test --locked -p muxe --test live_hosts -- --ignored --exact final_session_reload_failure
//! ```
//!
//! The fault case runs a real two-phase transaction. Phase 1 upgrades
//! old to target through the installed `muxe activate` CLI and records
//! the installed bridge digest. Phase 2 arms a nonshipped one-shot CLI
//! wrapper (scoped PATH, exact reload-shape match) and runs the downgrade
//! activation in the background: the wrapper holds the final-session
//! reload at an owned file barrier, the runner proves the earlier
//! session's target healthy through its control status plus live clients
//! and only then releases the failure, and the wrapper runs the real host
//! CLI once with a guaranteed-nonexistent URL. Rollback reloads pass
//! through unchanged after the one-shot consumes. Recovery asserts the
//! pre-fault group is complete: old-version brokers serving exact probed
//! records, the installed bridge digest restored, live sessions, and
//! unbroken host witnesses.

#[path = "support/mod.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use muxe_adapter_api::{AdapterHealthEvent, HostAdapter as _};
use muxe_adapter_zellij::{
    CliMembershipSource, MembershipSource, PipeChannel, PipeEpoch, PipeTransportError,
    ReadinessGate, SubprocessChannel, ZellijAdapter, ZellijAdapterConfig, channel_names,
};
use muxe_protocol::control::CompatibilityRecord;
use muxe_zellij_protocol::{
    BRIDGE_PROTOCOL_VERSION, BridgeEvent, BridgeRequest, BridgeResponse, BridgeTarget,
    ChannelGeneration, EventSubscription, PipeEvent, PipeEventKind, PipeRequest, RegistrationId,
    RequestId, ZellijOriginRequest, bridge_build_id, bridge_protocol_fingerprint,
    decode_event_line, encode_event_subscription, encode_request_line,
    generated_action_fingerprint, pinned_source_revision,
};
use sha2::{Digest, Sha256};
use support::{
    ActivateCommandEnvironment, ContinuityGuard, OwnedChild, OwnedHerdrServer, OwnedZellijHost,
    ScopedOnlyActivateEnvironment, ServedBroker, apply_scoped_env, assert_broker_serving,
    assert_no_preserved_journals, await_activate, await_session_ready, await_target_ready,
    drive_activate, emit_owned_host_log_tails, init_shared_dirs, input_path,
    install_zellij_integration, installed_version, installed_wasm_digest, poll_until,
    read_broker_record, retire_broker, run_cli_bounded, short_tempdir, spawn_activate,
    spawn_herdr_client, spawn_serve_herdr, spawn_serve_zellij, validate_installation,
};

/// Bounded wait for one barrier file to appear.
const BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(3);
/// Bounded wait for one transfer target to report ready.
const READY_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(3);
/// Bounded wait for one addressed duo origin round (release plus snapshot).
const DUO_ROUTE_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(1);

/// Explicit human approval for live hosts. Checked before any spawn,
/// installation probe, or host launch: without exactly `true` the test
/// fails naming this variable and launches nothing.
fn require_live_approval() {
    match std::env::var("MUXE_LIVE_HOSTS_APPROVED") {
        Ok(value) if value == "true" => {}
        _ => panic!(
            "live-host run requires MUXE_LIVE_HOSTS_APPROVED=true (explicit human approval); \
             refusing to spawn any host or probe any installation"
        ),
    }
}

struct Rig {
    root: tempfile::TempDir,
    scoped_root: PathBuf,
    workdir: PathBuf,
    config_file: PathBuf,
    cache_dir: PathBuf,
    herdr: Option<OwnedHerdrServer>,
    discovery: String,
    zellij: Option<OwnedZellijHost>,
    pty_clients: Vec<OwnedChild>,
    brokers: Vec<ServedBroker>,
    continuity: Option<ContinuityGuard>,
}

impl Rig {
    fn activate_environment(&self) -> &dyn ActivateCommandEnvironment {
        static SCOPED_ONLY: ScopedOnlyActivateEnvironment = ScopedOnlyActivateEnvironment;
        match self.zellij.as_ref() {
            Some(host) => host,
            None => &SCOPED_ONLY,
        }
    }

    /// Reverse-order teardown, preserving every failure.
    /// Brokers retire first so they exit orderly; witnesses close while
    /// hosts still run; hosts reap last with diagnostics logged.
    async fn close(&mut self, context: &str) -> io::Result<()> {
        eprintln!("[{context}] teardown under {}", self.root.path().display());
        let mut errors = Vec::new();
        let mut note = |error: io::Error| {
            eprintln!("[{context}] teardown error: {error}");
            errors.push(error);
        };
        for broker in std::mem::take(&mut self.brokers) {
            if let Err(error) = retire_broker(&broker.endpoint, &broker.tag).await {
                note(error);
            }
            let mut child = broker.child;
            match child.terminate_and_reap().await {
                Ok(diagnostics) => eprintln!(
                    "[{context}] reaped {} (status {:?}):\n--- stderr ---\n{}",
                    diagnostics.tag,
                    diagnostics.exit_status,
                    diagnostics.stderr_tail.lossy(),
                ),
                Err(error) => note(error),
            }
        }
        if let Some(server) = self.herdr.as_mut()
            && let Err(error) = server.try_wait().and_then(|exited| {
                exited.map_or(Ok(()), |_| {
                    Err(io::Error::other(format!(
                        "[{context}] herdr server child exited mid-run"
                    )))
                })
            })
        {
            note(error);
        }
        if let Some(host) = self.zellij.as_mut()
            && let Err(error) = host.check_servers_alive()
        {
            note(error);
        }
        if let Some(guard) = self.continuity.take() {
            match guard.finish().await {
                Ok(report) => eprintln!(
                    "[{context}] continuity {}: {} unbroken events",
                    report.tag, report.events
                ),
                Err(error) => note(error),
            }
        }
        if let Some(host) = self.zellij.as_mut() {
            for session in host.sessions().to_owned() {
                host.kill_session(&session).await;
            }
        }
        for mut client in std::mem::take(&mut self.pty_clients) {
            match client.terminate_and_reap().await {
                Ok(diagnostics) => eprintln!(
                    "[{context}] reaped {} (status {:?})",
                    diagnostics.tag, diagnostics.exit_status
                ),
                Err(error) => note(error),
            }
        }
        if let Some(host) = self.zellij.as_mut() {
            match host.shutdown().await {
                Ok(all) => {
                    for diagnostics in all {
                        eprintln!(
                            "[{context}] reaped {} (status {:?}):\n--- stderr ---\n{}",
                            diagnostics.tag,
                            diagnostics.exit_status,
                            diagnostics.stderr_tail.lossy(),
                        );
                    }
                }
                Err(error) => note(error),
            }
        }
        self.zellij = None;
        if let Some(server) = self.herdr.as_mut() {
            match server.shutdown().await {
                Ok(diagnostics) => eprintln!(
                    "[{context}] reaped {} (status {:?})",
                    diagnostics.tag, diagnostics.exit_status
                ),
                Err(error) => note(error),
            }
        }
        emit_owned_host_log_tails(context, &self.cache_dir, &self.scoped_root.join("tmp"));
        if errors.is_empty() {
            Ok(())
        } else {
            let details = errors
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            Err(io::Error::other(format!(
                "[{context}] teardown failures: {details}"
            )))
        }
    }
    async fn finish(&mut self, context: &str, body: io::Result<()>) -> io::Result<()> {
        let cleanup = self.close(context).await;
        combine_body_and_cleanup(body, cleanup)
    }
}

fn combine_body_and_cleanup(body: io::Result<()>, cleanup: io::Result<()>) -> io::Result<()> {
    match (body, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(body), Ok(())) => Err(body),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(body), Err(cleanup)) => Err(io::Error::other(format!(
            "operation failed: {body}; teardown failed: {cleanup}"
        ))),
    }
}

/// Brings up every owned host for one case: shared init config/cache,
/// Herdr server, Zellij servers plus sessions, retained PTY clients, and
/// the continuity witness. Each foreground server is initialized by the
/// owned bootstrap peer (real `FirstClientConnected` plus render
/// evidence) before any PTY client attaches; only then do the retained
/// `attach` clients count toward the session. The witness starts before
/// the first broker spawns. `sessions` maps each session name to its
/// client count.
#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one owned host rig keeps explicit binaries, optional scoped binding, startup, and reverse-order teardown together"
)]
async fn bring_hosts(
    case: &str,
    init_bin: &Path,
    install_bin: &Path,
    herdr_binary: &Path,
    zellij_binary: &Path,
    foreground: &Path,
    bootstrap: &Path,
    seeder: &Path,
    sessions: &[(&str, usize)],
    ui_hotkey: Option<&Path>,
) -> io::Result<Rig> {
    let root = short_tempdir(&format!("muxe-live-{case}-"))?;
    let scoped_root = root.path().join("scoped");
    let (config_file, cache_dir) = init_shared_dirs(init_bin, &scoped_root).await?;
    let expected_bridge = OwnedZellijHost::installed_bridge_identity(install_bin).await?;

    let host = OwnedZellijHost::prepare(zellij_binary, root.path(), case)?;
    // Receipt-owned pre-state before any startup: the real public
    // install writes stable bytes, receipt, and autoload KDL nodes, so
    // foreground startup loads the managed bridge and activation
    // preflight finds receipt-owned bytes. `install_bin` selects the
    // pre-state generation (old for upgrade rehearsal, target for smoke).
    install_zellij_integration(case, install_bin, &host, &scoped_root).await?;
    OwnedZellijHost::validate_prepared_bridge(&scoped_root, &expected_bridge)?;

    if let Some(binary) = ui_hotkey {
        use std::io::Write as _;
        let path = binary
            .to_str()
            .ok_or_else(|| io::Error::other("owned UI binary path is not UTF-8"))?;
        let program = serde_json::to_string(path).map_err(io::Error::other)?;
        let normal_marker =
            serde_json::to_string(&scoped_root.join("normal-mode.marker").to_string_lossy())
                .map_err(io::Error::other)?;
        let locked_marker =
            serde_json::to_string(&scoped_root.join("locked-mode.marker").to_string_lossy())
                .map_err(io::Error::other)?;
        let binding = format!(
            r#"
keybinds {{
    normal {{
        bind "Alt m" {{
            SwitchToMode "Normal"
            Run {program} "ui" "menu" "main" {{
                floating true
                x "0"
                y "70%"
                width "100%"
                height "30%"
                borderless true
                close_on_exit true
                start_suspended false
            }}
        }}
        bind "Alt x" {{
            Run "/usr/bin/touch" {normal_marker} {{
                close_on_exit true
                start_suspended false
            }}
        }}
    }}
    locked {{
        bind "Alt m" {{
            SwitchToMode "Locked"
            Run {program} "ui" "menu" "main" {{
                floating true
                x "0"
                y "70%"
                width "100%"
                height "30%"
                borderless true
                close_on_exit true
                start_suspended false
            }}
        }}
        bind "Alt x" {{
            Run "/usr/bin/touch" {locked_marker} {{
                close_on_exit true
                start_suspended false
            }}
        }}
    }}
}}
"#
        );
        let mut config = std::fs::OpenOptions::new()
            .append(true)
            .open(host.config_file())?;
        config.write_all(binding.as_bytes())?;
    }
    // Scoped permission grant for the managed bridge location: the exact
    // stable path the coordinator will load (bare path, matching the
    // pinned `Display for RunPluginLocation::File`), seeded with the
    // pinned cache code before the bridge loads at startup. The test
    // guard approval alone never implies this grant; both are explicit.
    let stable = muxe::integration::stable_bridge_path(
        config_file
            .parent()
            .ok_or_else(|| io::Error::other(format!("{case}: config file has no parent")))?,
    );
    host.run_permission_seed(seeder, &stable.display().to_string(), &scoped_root)
        .await?;
    let herdr = OwnedHerdrServer::start(herdr_binary, root.path(), &scoped_root, case).await?;
    let discovery = herdr.discovery_key().to_owned();
    let workdir = host.workdir().to_path_buf();
    let mut rig = Rig {
        root,
        scoped_root,
        workdir,
        config_file,
        cache_dir,
        herdr: Some(herdr),
        discovery,
        zellij: Some(host),
        pty_clients: Vec::new(),
        brokers: Vec::new(),
        continuity: None,
    };
    let startup = async {
        let herdr = rig
            .herdr
            .as_mut()
            .expect("startup retains its Herdr server");
        if herdr.try_wait()?.is_some() {
            return Err(io::Error::other(format!(
                "{case}: herdr server exited on startup"
            )));
        }
        let host = rig
            .zellij
            .as_mut()
            .expect("startup retains its Zellij host");
        for (session, _) in sessions {
            host.serve_foreground(
                foreground,
                bootstrap,
                session,
                &rig.scoped_root,
                &expected_bridge,
            )
            .await?;
        }
        if !host.has_server_child() {
            return Err(io::Error::other(format!(
                "{case}: zellij server children missing right after spawn"
            )));
        }
        host.check_servers_alive()?;
        for (session, count) in sessions {
            for index in 0..*count {
                let typescript = rig.workdir.join(format!("client-{session}-{index}.log"));
                rig.pty_clients.push(
                    host.spawn_client(&format!("{case}-{session}-{index}"), session, &typescript)
                        .await?,
                );
            }
        }
        for (session, count) in sessions {
            host.finish_bootstrap_handoff(session, *count).await?;
        }
        rig.continuity = Some(
            ContinuityGuard::watch_herdr(
                &format!("{case}-herdr"),
                muxe_adapter_herdr::HerdrAdapterConfig {
                    socket_path: herdr.socket().to_path_buf(),
                    herdr_binary: herdr_binary.to_path_buf(),
                    cache_dir: rig.cache_dir.clone(),
                },
                rig.discovery.clone(),
            )
            .await?,
        );
        Ok(())
    }
    .await;
    match startup {
        Ok(()) => Ok(rig),
        Err(error) => {
            let cleanup = rig.close(case).await;
            Err(combine_body_and_cleanup(Err(error), cleanup)
                .expect_err("startup failed before cleanup"))
        }
    }
}

/// Starts old brokers through the installed executable and returns the Herdr
/// endpoint plus one endpoint per Zellij session, in order.
/// This sets up an already activated installation; it does not test cold start.
/// The test exercises the public `muxe activate` path
/// (`transfer_to`/`drive_activate`), including its on-demand broker startup.
async fn serve_old_brokers(
    rig: &mut Rig,
    muxe_bin: &Path,
    herdr_binary: &Path,
    zellij_exe: &Path,
    tag: &str,
    sessions: &[&str],
) -> io::Result<(PathBuf, Vec<PathBuf>)> {
    let Some(server) = rig.herdr.as_ref() else {
        return Err(io::Error::other(format!("{tag}: herdr server is gone")));
    };
    let herdr_broker = spawn_serve_herdr(
        &format!("{tag}-herdr"),
        muxe_bin,
        herdr_binary,
        server.socket(),
        &rig.discovery.clone(),
        &rig.scoped_root.clone(),
        &rig.config_file.clone(),
        &rig.cache_dir.clone(),
    )
    .await?;
    let herdr_endpoint = herdr_broker.endpoint.clone();
    rig.brokers.push(herdr_broker);
    let mut zellij_endpoints = Vec::new();
    for session in sessions {
        let Some(host) = rig.zellij.as_ref() else {
            return Err(io::Error::other(format!("{tag}: zellij host is gone")));
        };
        let broker = spawn_serve_zellij(
            &format!("{tag}-{session}"),
            muxe_bin,
            host,
            zellij_exe,
            session,
            &rig.scoped_root.clone(),
            &rig.config_file.clone(),
            &rig.cache_dir.clone(),
        )
        .await?;
        zellij_endpoints.push(broker.endpoint.clone());
        rig.brokers.push(broker);
    }
    Ok((herdr_endpoint, zellij_endpoints))
}

/// Probes one installation's full reported record on every host by serving
/// it briefly as Running (no handoff) and retiring it. Returns the Herdr
/// record plus one record per session, in order. Expectations come from a
/// real broker report, never invented.
async fn probe_records(
    rig: &Rig,
    binary: &Path,
    herdr_binary: &Path,
    zellij_exe: &Path,
    tag: &str,
    sessions: &[&str],
) -> io::Result<(CompatibilityRecord, Vec<CompatibilityRecord>)> {
    let Some(server) = rig.herdr.as_ref() else {
        return Err(io::Error::other(format!("{tag}: herdr server is gone")));
    };
    let probe = spawn_serve_herdr(
        &format!("{tag}-probe-herdr"),
        binary,
        herdr_binary,
        server.socket(),
        &rig.discovery,
        &rig.scoped_root,
        &rig.config_file,
        &rig.cache_dir,
    )
    .await?;
    let herdr_record = read_broker_record(&probe.endpoint, tag).await?;
    retire_broker(&probe.endpoint, &format!("{tag}-probe-herdr")).await?;
    let Some(host) = rig.zellij.as_ref() else {
        return Err(io::Error::other(format!("{tag}: zellij host is gone")));
    };
    let mut session_records = Vec::new();
    for session in sessions {
        let probe = spawn_serve_zellij(
            &format!("{tag}-probe-{session}"),
            binary,
            host,
            zellij_exe,
            session,
            &rig.scoped_root,
            &rig.config_file,
            &rig.cache_dir,
        )
        .await?;
        session_records.push(read_broker_record(&probe.endpoint, tag).await?);
        retire_broker(&probe.endpoint, &format!("{tag}-probe-{session}")).await?;
    }
    Ok((herdr_record, session_records))
}

/// Asserts one endpoint serves exactly the expected record for the
/// expected discovery key after a transfer committed.
async fn assert_serving_record(
    endpoint: &Path,
    tag: &str,
    expected: &CompatibilityRecord,
    discovery: &str,
) -> io::Result<()> {
    let record = read_broker_record(endpoint, tag).await?;
    if record != *expected {
        return Err(io::Error::other(format!(
            "{tag}: serving record is version {}, want version {}",
            record.muxe_version, expected.muxe_version
        )));
    }
    let live = read_live_discovery(endpoint, tag).await?;
    if live != discovery {
        return Err(io::Error::other(format!(
            "{tag}: serving discovery is {live:?}, want {discovery:?}"
        )));
    }
    Ok(())
}

/// Reads one endpoint's live discovery key over control.
async fn read_live_discovery(endpoint: &Path, tag: &str) -> io::Result<String> {
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| io::Error::other(format!("connect {tag} for discovery: {error}")))?;
    control
        .status()
        .await
        .map(|status| status.live_server.discovery_key)
        .map_err(|error| io::Error::other(format!("{tag} discovery status failed: {error}")))
}

/// SHA-256 hex of the stable bridge file under the shared config.
fn stable_digest(config_file: &Path) -> io::Result<String> {
    let config_dir = config_file.parent().ok_or_else(|| {
        io::Error::other(format!(
            "config file has no parent: {}",
            config_file.display()
        ))
    })?;
    let stable = muxe::integration::stable_bridge_path(config_dir);
    let bytes = std::fs::read(&stable).map_err(|error| {
        io::Error::other(format!(
            "cannot read stable bridge at {}: {error}",
            stable.display()
        ))
    })?;
    Ok(format!("{:x}", Sha256::digest(&bytes)))
}

/// The fence combines a child epoch with its separate wire generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DuoFence {
    epoch: PipeEpoch,
    generation: ChannelGeneration,
}

struct DuoObservation {
    epoch: Option<PipeEpoch>,
    result: Result<String, PipeTransportError>,
}

/// Snapshots both real 4 KiB transport tails before either child is discarded.
/// Recovery replaces/parks request before event; shutdown closes event first.
struct DuoDiagnostics {
    request: Arc<SubprocessChannel>,
    event: Arc<SubprocessChannel>,
}

impl DuoDiagnostics {
    async fn log(&self, reason: &str) {
        for (label, channel) in [("request", &self.request), ("event", &self.event)] {
            duo_log_stderr_tail(label, channel, reason).await;
        }
    }
}

async fn duo_log_stderr_tail(label: &str, channel: &SubprocessChannel, reason: &str) {
    let tail = channel.stderr_tail().await;
    eprintln!(
        "[duo] {reason}: {label} epoch {:?} stderr tail ({} bytes):\n{}",
        channel.install_epoch().await,
        tail.len(),
        String::from_utf8_lossy(&tail),
    );
}

/// Fixture-only diagnostics; transport semantics remain production-owned.
struct DuoDiagnosticChannel {
    channel: Arc<SubprocessChannel>,
    label: &'static str,
    diagnostics: Arc<DuoDiagnostics>,
}

#[async_trait::async_trait]
impl PipeChannel for DuoDiagnosticChannel {
    async fn send_line(&self, line: String) -> Result<(), PipeTransportError> {
        let result = self.channel.send_line(line).await;
        if let Err(error) = &result {
            self.diagnostics
                .log(&format!("{} write error: {error}", self.label))
                .await;
        }
        result
    }

    async fn next_line(&self) -> Result<String, PipeTransportError> {
        self.next_line_tagged().await.map(|(_, line)| line)
    }

    async fn next_line_tagged(&self) -> Result<(PipeEpoch, String), PipeTransportError> {
        let result = self.channel.next_line_tagged().await;
        if let Err(error) = &result {
            self.diagnostics
                .log(&format!("{} read error: {error}", self.label))
                .await;
        }
        result
    }

    async fn install_epoch(&self) -> Option<PipeEpoch> {
        self.channel.install_epoch().await
    }

    async fn close(&self) {
        self.diagnostics
            .log(&format!("before {} close", self.label))
            .await;
        self.channel.close().await;
    }

    async fn park(&self) {
        self.diagnostics
            .log(&format!("before {} park", self.label))
            .await;
        self.channel.park().await;
    }

    async fn respawn(&self) -> Result<(), PipeTransportError> {
        self.diagnostics
            .log(&format!("before {} respawn", self.label))
            .await;
        let result = self.channel.respawn().await;
        if let Err(error) = &result {
            self.diagnostics
                .log(&format!("{} respawn error: {error}", self.label))
                .await;
        }
        result
    }

    async fn respawn_with_payload(&self, payload: String) -> Result<(), PipeTransportError> {
        self.diagnostics
            .log(&format!("before {} subscription respawn", self.label))
            .await;
        let result = self.channel.respawn_with_payload(payload).await;
        if let Err(error) = &result {
            self.diagnostics
                .log(&format!(
                    "{} subscription respawn error: {error}",
                    self.label
                ))
                .await;
        }
        result
    }
}

/// Observes real transport results while the production adapter owns recovery.
/// The queue is bounded; it neither substitutes responses nor replays requests.
struct DuoEventChannel {
    channel: Arc<DuoDiagnosticChannel>,
    overflowed: Arc<AtomicBool>,
    observations: tokio::sync::mpsc::Sender<DuoObservation>,
}

#[async_trait::async_trait]
impl PipeChannel for DuoEventChannel {
    async fn send_line(&self, line: String) -> Result<(), PipeTransportError> {
        self.channel.send_line(line).await
    }

    async fn next_line(&self) -> Result<String, PipeTransportError> {
        self.next_line_tagged().await.map(|(_, line)| line)
    }

    async fn next_line_tagged(&self) -> Result<(PipeEpoch, String), PipeTransportError> {
        let result = self.channel.next_line_tagged().await;
        let observation = DuoObservation {
            epoch: result.as_ref().ok().map(|(epoch, _)| *epoch),
            result: result
                .as_ref()
                .map(|(_, line)| line.clone())
                .map_err(Clone::clone),
        };
        if self.observations.try_send(observation).is_err() {
            self.overflowed.store(true, Ordering::Release);
            return Err(PipeTransportError::Closed);
        }
        result
    }

    async fn install_epoch(&self) -> Option<PipeEpoch> {
        self.channel.install_epoch().await
    }

    async fn close(&self) {
        self.channel.close().await;
    }

    async fn park(&self) {
        self.channel.park().await;
    }

    async fn respawn(&self) -> Result<(), PipeTransportError> {
        self.channel.respawn().await
    }

    async fn respawn_with_payload(&self, payload: String) -> Result<(), PipeTransportError> {
        self.channel.respawn_with_payload(payload).await
    }
}

struct DuoEvents {
    channel: Arc<SubprocessChannel>,
    observations: tokio::sync::mpsc::Receiver<DuoObservation>,
    fence: Option<DuoFence>,
    overflowed: Arc<AtomicBool>,
}

impl DuoEvents {
    async fn observe(&mut self, timeout: std::time::Duration) -> io::Result<DuoObservation> {
        if self.overflowed.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "duo: bounded event observation queue failed",
            ));
        }
        tokio::time::timeout(timeout, self.observations.recv())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "duo: event observation timed out")
            })?
            .ok_or_else(|| io::Error::other("duo: event observation ended"))
    }
}

async fn duo_send_request_line(pipe: &dyn PipeChannel, line: &str) -> io::Result<()> {
    pipe.send_line(line.to_owned())
        .await
        .map_err(|error| io::Error::other(format!("duo: request pipe write failed: {error}")))
}

/// Once admitted, any transport or generation change fails without replay.
async fn duo_next_event(
    event: &mut DuoEvents,
    timeout: std::time::Duration,
) -> io::Result<PipeEvent> {
    let fence = event
        .fence
        .ok_or_else(|| io::Error::other("duo: routing began without current census coverage"))?;
    let observation = event.observe(timeout).await?;
    let line = observation.result.map_err(|error| {
        io::Error::other(format!("duo: admitted event transport failed: {error}"))
    })?;
    if observation.epoch != Some(fence.epoch)
        || event.channel.install_epoch().await != Some(fence.epoch)
    {
        return Err(io::Error::other("duo: admitted event channel changed"));
    }
    let frame = decode_event_line(&line)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if frame.channel_generation != fence.generation {
        return Err(io::Error::other("duo: admitted event generation changed"));
    }
    Ok(frame)
}

/// Deterministic registration-scoped request IDs for waiter routing.
fn duo_request_id(counter: u64) -> RequestId {
    RequestId::try_from(counter).expect("duo request IDs start at one")
}

/// Produces the typed initial-generation payload consumed by the bridge's
/// event-channel subscription decoder.
fn duo_subscription_payload() -> io::Result<String> {
    encode_event_subscription(EventSubscription::new(ChannelGeneration::INITIAL))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

/// Simultaneous two-client regression on a second fresh Rig: two real PTY
/// clients on one session, exact-two authoritative census before admission,
/// two anchor-bound bridge Registers, then sequential typed read-only
/// origin queries each addressed to one (`client_id`, registration). No
/// broker serves this session, so this scenario owns the only event-pipe
/// subscription and every answer is bridge-direct. Read-only throughout:
/// no capture, dispatch, or bridge retirement is ever sent.
async fn run_simultaneous_two_clients() -> io::Result<()> {
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let zellij_binary = input_path("MUXE_ZELLIJ_BINARY");
    let foreground = input_path("MUXE_ZELLIJ_FOREGROUND_BINARY");
    let bootstrap = input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY");
    let seeder = input_path("MUXE_ZELLIJ_PERMISSION_SEEDER");

    let mut rig = bring_hosts(
        "duo",
        &target_bin,
        &target_bin,
        &herdr_binary,
        &zellij_binary,
        &foreground,
        &bootstrap,
        &seeder,
        &[("duo", 2)],
        None,
    )
    .await?;
    let result = async {
        let Some(host) = rig.zellij.as_ref() else {
            return Err(io::Error::other("duo: zellij host is gone"));
        };
        let executable = host.scoped_cli_wrapper(&rig.scoped_root)?;
        let membership = CliMembershipSource::new(executable.clone(), "duo".to_owned());
        // Authoritative anchor first: exactly two live clients, never one.
        // A one-client snapshot is a poll retry, never admission.
        let census_deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        let anchor = loop {
            let census = match duo_snapshot_census(&membership, census_deadline).await? {
                Some(census) if census.len() == 2 => break census,
                census => census,
            };
            if tokio::time::Instant::now() >= census_deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "duo: timed out waiting for exact two-client census, last saw {census:?}"
                    ),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        eprintln!("[duo] authoritative census: {anchor:?}");

        duo_connect_and_probe(&rig, &anchor, executable).await
    }
    .await;
    rig.finish("duo", result).await
}

async fn duo_connect_and_probe(
    rig: &Rig,
    anchor: &[muxe_core::ClientId],
    executable: PathBuf,
) -> io::Result<()> {
    let (request_name, event_name) = channel_names("duo");
    let bridge = muxe::integration::bridge_identity(
        rig.config_file.parent().expect("owned config has a parent"),
    )
    .map_err(io::Error::other)?;
    let subscription = duo_subscription_payload()?;
    let request =
        SubprocessChannel::launch(executable.clone(), "duo".to_owned(), request_name, None)
            .await
            .map_err(|error| {
                io::Error::other(format!("duo: request pipe launch failed: {error}"))
            })?;
    let channel = match SubprocessChannel::launch(
        executable.clone(),
        "duo".to_owned(),
        event_name,
        Some(subscription),
    )
    .await
    {
        Ok(channel) => channel,
        Err(error) => {
            duo_log_stderr_tail("request", &request, "event launch failed").await;
            request.close().await;
            return Err(io::Error::other(format!(
                "duo: event pipe launch failed: {error}",
            )));
        }
    };
    let diagnostics = Arc::new(DuoDiagnostics {
        request: Arc::clone(&request),
        event: Arc::clone(&channel),
    });
    let request = Arc::new(DuoDiagnosticChannel {
        channel: request,
        label: "request",
        diagnostics: Arc::clone(&diagnostics),
    });
    let observed_channel = Arc::new(DuoDiagnosticChannel {
        channel: Arc::clone(&channel),
        label: "event",
        diagnostics: Arc::clone(&diagnostics),
    });
    let (observations, receiver) = tokio::sync::mpsc::channel(64);
    let overflowed = Arc::new(AtomicBool::new(false));
    let event_channel = Arc::new(DuoEventChannel {
        channel: observed_channel,
        observations,
        overflowed: Arc::clone(&overflowed),
    });
    let adapter = ZellijAdapter::new(
        ZellijAdapterConfig {
            session_name: "duo".to_owned(),
            zellij_exe: executable.clone(),
            readiness_gate: ReadinessGate::new(rig.cache_dir.clone(), bridge.unit()),
        },
        request.clone(),
        event_channel,
    );
    let monitor = adapter.clone();
    let health = tokio::spawn(async move {
        while let Ok(event) = monitor.next_health_event().await {
            if let AdapterHealthEvent::Unhealthy { error, .. } = event {
                eprintln!("[duo] adapter unavailable: {error}");
            }
        }
    });
    let membership = CliMembershipSource::new(executable, "duo".to_owned());
    let mut event = DuoEvents {
        channel,
        observations: receiver,
        fence: None,
        overflowed,
    };
    let body = duo_probe_rounds(anchor, request.as_ref(), &mut event, &adapter, &membership).await;
    diagnostics.log("before adapter shutdown").await;
    let cleanup = adapter.shutdown().await.map_err(io::Error::other);
    health.abort();
    let _ = health.await;
    combine_body_and_cleanup(body, cleanup)
}

/// Census coverage plus addressed routing for the duo scenario. First awaits
/// fresh compatible Registers covering exactly the anchor members (foreign
/// IDs are stale native coverage and ignored); then sends one sequential
/// read-only origin query per member addressed to its (`client_id`, registration)
/// and requires both the transport release and the origin snapshot to agree
/// on the owner. Release and snapshot may arrive in either order; duplicates,
/// wrong owners, wrong requests, and UI-session mismatches all fail closed.
async fn duo_probe_rounds(
    anchor: &[muxe_core::ClientId],
    request: &dyn PipeChannel,
    event: &mut DuoEvents,
    adapter: &ZellijAdapter,
    membership: &dyn MembershipSource,
) -> io::Result<()> {
    let registrations = duo_collect_registrations(anchor, event, adapter, membership).await?;
    let targets = duo_route_targets(anchor, &registrations, request, event).await?;
    duo_drain_route_heartbeats(anchor, &registrations, &targets, event).await
}

async fn duo_collect_registrations(
    anchor: &[muxe_core::ClientId],
    event: &mut DuoEvents,
    adapter: &ZellijAdapter,
    membership: &dyn MembershipSource,
) -> io::Result<BTreeMap<muxe_core::ClientId, (RegistrationId, String)>> {
    let coverage_deadline = tokio::time::Instant::now() + READY_TIMEOUT;
    let mut registrations: BTreeMap<muxe_core::ClientId, (RegistrationId, String)> =
        BTreeMap::new();
    let mut candidate = None;
    loop {
        let remaining = coverage_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "duo: timed out awaiting census coverage, anchor is {anchor:?}, registered {:?}",
                    registrations.keys().collect::<Vec<_>>(),
                ),
            ));
        }
        let current = event.channel.install_epoch().await;
        if candidate.is_some_and(|fence: DuoFence| Some(fence.epoch) != current) {
            registrations.clear();
            candidate = None;
        }
        if duo_admit_coverage(
            anchor,
            registrations.len(),
            event,
            adapter,
            membership,
            candidate,
            coverage_deadline,
        )
        .await?
        {
            return Ok(registrations);
        }
        let observation = if registrations.len() == anchor.len() {
            tokio::select! {
                observation = event.observe(remaining) => observation?,
                () = tokio::time::sleep(std::time::Duration::from_millis(10)) => continue,
            }
        } else {
            event.observe(remaining).await?
        };
        let line = match observation.result {
            Ok(line) => line,
            Err(error) => {
                eprintln!("[duo] pre-admission transport loss: {error}; requiring fresh coverage");
                registrations.clear();
                candidate = None;
                continue;
            }
        };
        let epoch = observation
            .epoch
            .ok_or_else(|| io::Error::other("duo: untagged event"))?;
        if Some(epoch) != event.channel.install_epoch().await {
            registrations.clear();
            candidate = None;
            continue;
        }
        let frame = decode_event_line(&line).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("duo: cannot decode event line: {error}"),
            )
        })?;
        let fence = DuoFence {
            epoch,
            generation: frame.channel_generation,
        };
        if let Some(candidate) = candidate {
            if candidate != fence {
                return Err(io::Error::other(
                    "duo: mixed subscription generations in one epoch",
                ));
            }
        } else {
            candidate = Some(fence);
        }
        let registration = frame.registration;
        match frame.event {
            PipeEventKind::Event(BridgeEvent::Register {
                registration: details,
            }) if !anchor
                .iter()
                .any(|client| client.as_str() == details.client_id) => {}
            PipeEventKind::Event(BridgeEvent::Register {
                registration: details,
            }) => {
                duo_add_registration(&mut registrations, registration, details)?;
            }
            PipeEventKind::Event(BridgeEvent::Heartbeat) => {}
            other => {
                return Err(io::Error::other(format!(
                    "duo: unexpected event while awaiting census coverage: {other:?}"
                )));
            }
        }
    }
}

async fn duo_admit_coverage(
    anchor: &[muxe_core::ClientId],
    registered: usize,
    event: &mut DuoEvents,
    adapter: &ZellijAdapter,
    membership: &dyn MembershipSource,
    candidate: Option<DuoFence>,
    deadline: tokio::time::Instant,
) -> io::Result<bool> {
    let Some(fence) = candidate else {
        return Ok(false);
    };
    if registered != anchor.len()
        || event.channel.install_epoch().await != Some(fence.epoch)
        || adapter.identity().await.is_err()
    {
        return Ok(false);
    }
    if !duo_recheck_census(anchor, membership, deadline).await? {
        eprintln!(
            "[duo] retrying admission census: candidate {fence:?}, current epoch {:?}, ready {}",
            event.channel.install_epoch().await,
            adapter.identity().await.is_ok(),
        );
        return Ok(false);
    }
    if event.overflowed.load(Ordering::Acquire) {
        return Err(io::Error::other(
            "duo: bounded event observation queue failed",
        ));
    }
    if event.channel.install_epoch().await != Some(fence.epoch) || adapter.identity().await.is_err()
    {
        return Ok(false);
    }
    event.fence = Some(fence);
    Ok(true)
}

async fn duo_recheck_census(
    anchor: &[muxe_core::ClientId],
    membership: &dyn MembershipSource,
    deadline: tokio::time::Instant,
) -> io::Result<bool> {
    let Some(members) = duo_snapshot_census(membership, deadline).await? else {
        return Ok(false);
    };
    if members != anchor {
        return Err(io::Error::other(format!(
            "duo: membership changed before admission: {members:?}",
        )));
    }
    Ok(true)
}

async fn duo_snapshot_census(
    membership: &dyn MembershipSource,
    deadline: tokio::time::Instant,
) -> io::Result<Option<Vec<muxe_core::ClientId>>> {
    let result = tokio::time::timeout_at(deadline, membership.snapshot_members())
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "duo: pre-admission census timed out",
            )
        })?;
    match result {
        Ok(members) => Ok(Some(members)),
        Err(error) if error.kind == muxe_adapter_api::AdapterErrorKind::Unavailable => {
            eprintln!("[duo] pre-admission census unavailable: {error}");
            Ok(None)
        }
        Err(error) => Err(io::Error::other(error)),
    }
}

fn duo_add_registration(
    registrations: &mut BTreeMap<muxe_core::ClientId, (RegistrationId, String)>,
    registration: RegistrationId,
    details: muxe_zellij_protocol::ZellijRegistration,
) -> io::Result<()> {
    let identity = &details.identity;
    if !(identity.bridge_build_id == Some(bridge_build_id())
        && identity.source_revision == pinned_source_revision()
        && identity.action_fingerprint == generated_action_fingerprint().0
        && identity.protocol_fingerprint == bridge_protocol_fingerprint().0)
    {
        return Err(io::Error::other(format!(
            "duo: incompatible bridge handshake for client {:?}",
            details.client_id,
        )));
    }
    if identity.muxe_version != env!("CARGO_PKG_VERSION") {
        return Err(io::Error::other(format!(
            "duo: bridge version {:?} does not match target {:?} for client {:?}",
            identity.muxe_version,
            env!("CARGO_PKG_VERSION"),
            details.client_id,
        )));
    }
    let client_id = muxe_core::ClientId::new(details.client_id);
    let Some(current_pane) = details.current_pane else {
        return Err(io::Error::other(format!(
            "duo: no focused-pane anchor for client {client_id:?}, cannot address an origin query",
        )));
    };
    if let Some((previous, _)) = registrations.get(&client_id) {
        eprintln!(
            "[duo] superseding registration for client {client_id:?} (previous {previous:?})",
        );
    }
    registrations.insert(client_id, (registration, current_pane));
    Ok(())
}
async fn duo_route_targets(
    anchor: &[muxe_core::ClientId],
    registrations: &BTreeMap<muxe_core::ClientId, (RegistrationId, String)>,
    request: &dyn PipeChannel,
    event: &mut DuoEvents,
) -> io::Result<Vec<(muxe_core::ClientId, RegistrationId)>> {
    let targets: Vec<(muxe_core::ClientId, RegistrationId)> = anchor
        .iter()
        .map(|client| {
            let (registration, _) = registrations.get(client).ok_or_else(|| {
                io::Error::other(format!("duo: anchor member {client:?} never registered"))
            })?;
            Ok((client.clone(), *registration))
        })
        .collect::<io::Result<Vec<_>>>()?;
    if targets[0].1 == targets[1].1 {
        return Err(io::Error::other(format!(
            "duo: both clients share registration {:?}",
            targets[0].1
        )));
    }
    eprintln!(
        "[duo] census covered: {:?}",
        targets
            .iter()
            .map(|(client, registration)| format!("{client}={registration:?}"))
            .collect::<Vec<_>>(),
    );

    for (index, (client_id, registration)) in targets.iter().enumerate() {
        duo_route_one(
            anchor,
            registrations,
            request,
            event,
            index,
            client_id,
            *registration,
        )
        .await?;
    }
    Ok(targets)
}
async fn duo_route_one(
    anchor: &[muxe_core::ClientId],
    registrations: &BTreeMap<muxe_core::ClientId, (RegistrationId, String)>,
    request: &dyn PipeChannel,
    event: &mut DuoEvents,
    index: usize,
    client_id: &muxe_core::ClientId,
    registration: RegistrationId,
) -> io::Result<()> {
    let (_, current_pane) = registrations.get(client_id).ok_or_else(|| {
        io::Error::other(format!(
            "duo: anchor member {client_id:?} lost its pane anchor"
        ))
    })?;
    let request_id = duo_request_id(index as u64 + 1);
    let ui_session = format!("duo-route-{client_id}");
    let generation = event
        .fence
        .ok_or_else(|| io::Error::other("duo: origin route has no current coverage"))?
        .generation;
    let outbound = PipeRequest {
        protocol: BRIDGE_PROTOCOL_VERSION,
        request_id,
        registration,
        channel_generation: generation,
        target: BridgeTarget {
            client_id: client_id.as_str().to_owned(),
        },
        payload: BridgeRequest::RequestOrigin {
            ui_session: muxe_protocol::UiSessionId::new(ui_session.clone()),
            request: ZellijOriginRequest {
                ui_pane: current_pane.clone(),
            },
        },
    };
    let line = encode_request_line(&outbound)
        .map_err(|error| io::Error::other(format!("duo: cannot encode origin request: {error}")))?;
    let round_deadline = tokio::time::Instant::now() + DUO_ROUTE_TIMEOUT;
    let send_remaining = round_deadline.saturating_duration_since(tokio::time::Instant::now());
    let send_result = tokio::time::timeout(send_remaining, duo_send_request_line(request, &line))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("duo: timed out sending routed origin for client {client_id:?}"),
            )
        })?;
    send_result?;
    let mut state = DuoRouteState {
        anchor,
        request_id,
        registration,
        generation,
        ui_session: &ui_session,
        current_pane,
        client_id,
        released: false,
        round_deadline,
        snapshot: false,
    };
    duo_await_route_response(event, &mut state).await?;
    eprintln!("[duo] routed origin for client {client_id:?}: release plus snapshot agree");
    Ok(())
}
struct DuoRouteState<'a> {
    anchor: &'a [muxe_core::ClientId],
    request_id: RequestId,
    registration: RegistrationId,
    generation: ChannelGeneration,
    ui_session: &'a str,
    current_pane: &'a str,
    client_id: &'a muxe_core::ClientId,
    round_deadline: tokio::time::Instant,
    released: bool,
    snapshot: bool,
}

impl DuoRouteState<'_> {
    fn apply(&mut self, frame: PipeEvent) -> io::Result<()> {
        let solicited = matches!(
            &frame.event,
            PipeEventKind::Response(
                BridgeResponse::RequestReleased
                    | BridgeResponse::OriginSnapshot { .. }
                    | BridgeResponse::OriginDeclined { .. }
            )
        );
        if solicited {
            if frame.request_id != Some(self.request_id) {
                return Err(io::Error::other(format!(
                    "duo: event for wrong request {:?} while routing client {:?}",
                    frame.request_id, self.client_id
                )));
            }
            if frame.registration != self.registration {
                return Err(io::Error::other(format!(
                    "duo: event from wrong owner {:?} for client {:?}",
                    frame.registration, self.client_id
                )));
            }
            if frame.channel_generation != self.generation {
                return Err(io::Error::other(format!(
                    "duo: event on wrong generation {} for client {:?}",
                    frame.channel_generation, self.client_id
                )));
            }
        }
        match frame.event {
            PipeEventKind::Response(BridgeResponse::RequestReleased) => {
                if self.released {
                    return Err(io::Error::other(format!(
                        "duo: duplicate release for client {:?}",
                        self.client_id
                    )));
                }
                self.released = true;
            }
            PipeEventKind::Response(BridgeResponse::OriginSnapshot { ui_session, origin }) => {
                if ui_session.as_str() != self.ui_session {
                    return Err(io::Error::other(format!(
                        "duo: snapshot for wrong UI session {ui_session:?} while routing client {:?}",
                        self.client_id
                    )));
                }
                let client_id = muxe_core::ClientId::new(origin.client_id);
                if client_id != *self.client_id {
                    return Err(io::Error::other(format!(
                        "duo: snapshot for wrong client {:?} while routing client {:?}",
                        client_id, self.client_id
                    )));
                }
                if origin.ui_pane_id != self.current_pane {
                    return Err(io::Error::other(format!(
                        "duo: snapshot echoes wrong UI pane {:?} for client {:?}",
                        origin.ui_pane_id, self.client_id
                    )));
                }
                if self.snapshot {
                    return Err(io::Error::other(format!(
                        "duo: duplicate snapshot for client {:?}",
                        self.client_id
                    )));
                }
                self.snapshot = true;
            }
            PipeEventKind::Response(BridgeResponse::OriginDeclined { ui_session }) => {
                return Err(io::Error::other(format!(
                    "duo: owner declined its own pane (session {ui_session:?}, request {:?}, owner {:?}) for client {:?}",
                    frame.request_id, frame.registration, self.client_id
                )));
            }
            PipeEventKind::Event(BridgeEvent::Heartbeat) => {}
            PipeEventKind::Event(BridgeEvent::Register {
                registration: details,
            }) => {
                let client_id = muxe_core::ClientId::new(details.client_id);
                if self.anchor.contains(&client_id) {
                    return Err(io::Error::other(format!(
                        "duo: unexpected re-registration for client {:?} while routing client {:?}",
                        client_id, self.client_id
                    )));
                }
            }
            other => {
                return Err(io::Error::other(format!(
                    "duo: unexpected event while routing client {:?}: {other:?}",
                    self.client_id
                )));
            }
        }
        Ok(())
    }
}

async fn duo_await_route_response(
    event: &mut DuoEvents,
    state: &mut DuoRouteState<'_>,
) -> io::Result<()> {
    while !(state.released && state.snapshot) {
        let remaining = state
            .round_deadline
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "duo: timed out awaiting routed origin for client {:?} (released={}, snapshot={})",
                    state.client_id, state.released, state.snapshot
                ),
            ));
        }
        let frame = duo_next_event(event, remaining).await?;
        state.apply(frame)?;
    }
    Ok(())
}
async fn duo_drain_route_heartbeats(
    anchor: &[muxe_core::ClientId],
    registrations: &BTreeMap<muxe_core::ClientId, (RegistrationId, String)>,
    targets: &[(muxe_core::ClientId, RegistrationId)],
    event: &mut DuoEvents,
) -> io::Result<()> {
    // Let the protocol drain through the next heartbeat from both active
    // anchors. This catches queued duplicate replies after the final route
    // while keeping the boundary bounded by the existing route timeout.
    let heartbeat_deadline = tokio::time::Instant::now() + DUO_ROUTE_TIMEOUT;
    let mut heartbeats = BTreeSet::new();
    while heartbeats.len() < targets.len() {
        let remaining = heartbeat_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("duo: timed out awaiting post-route heartbeats, observed {heartbeats:?}"),
            ));
        }
        let frame = duo_next_event(event, remaining).await?;
        let registration = frame.registration;
        match frame.event {
            PipeEventKind::Event(BridgeEvent::Heartbeat) => {
                if let Some((client_id, _)) =
                    registrations.iter().find(|(client_id, (active, _))| {
                        anchor.contains(client_id) && *active == registration
                    })
                {
                    heartbeats.insert(client_id.clone());
                }
            }
            PipeEventKind::Event(BridgeEvent::Register {
                registration: details,
            }) => {
                let client_id = muxe_core::ClientId::new(details.client_id);
                if anchor.contains(&client_id) {
                    return Err(io::Error::other(format!(
                        "duo: unexpected re-registration for active client {client_id:?} after routing"
                    )));
                }
            }
            PipeEventKind::Response(
                BridgeResponse::OriginSnapshot { .. }
                | BridgeResponse::RequestReleased
                | BridgeResponse::OriginDeclined { .. },
            ) => {
                return Err(io::Error::other(
                    "duo: unexpected duplicate routed response after final origin",
                ));
            }
            other => {
                return Err(io::Error::other(format!(
                    "duo: unexpected event while awaiting post-route heartbeats: {other:?}"
                )));
            }
        }
    }
    eprintln!("[duo] post-route heartbeats observed for both active anchors");
    Ok(())
}
async fn prepare_herdr_menu_origin(
    rig: &mut Rig,
    herdr_binary: &Path,
) -> io::Result<(
    PathBuf,
    muxe_adapter_herdr::HerdrRuntime,
    muxe_adapter_herdr::FocusedPane,
    PathBuf,
)> {
    let socket = rig
        .herdr
        .as_ref()
        .ok_or_else(|| io::Error::other("smoke: Herdr server is gone"))?
        .socket()
        .to_path_buf();
    let runtime =
        muxe_adapter_herdr::HerdrRuntime::connect(muxe_adapter_herdr::HerdrAdapterConfig {
            socket_path: socket.clone(),
            herdr_binary: herdr_binary.to_path_buf(),
            cache_dir: rig.cache_dir.clone(),
        })
        .await
        .map_err(|error| io::Error::other(format!("smoke: connect Herdr runtime: {error}")))?;
    match runtime
        .invoke_response(
            "workspace.create",
            serde_json::json!({
                "cwd": rig.workdir,
                "env": {},
                "focus": true,
                "label": "muxe-live-smoke",
            }),
        )
        .await
        .map_err(|error| io::Error::other(format!("smoke: create Herdr workspace: {error}")))?
    {
        muxe_adapter_herdr::HerdrResponse::Success(_) => {}
        muxe_adapter_herdr::HerdrResponse::Error { code, message } => {
            return Err(io::Error::other(format!(
                "smoke: Herdr rejected origin workspace with {code}: {message}"
            )));
        }
    }
    let typescript = rig.root.path().join("smoke-herdr.typescript");
    let client = spawn_herdr_client(
        "smoke-menu",
        herdr_binary,
        &socket,
        &rig.scoped_root,
        &rig.workdir,
        &typescript,
    )
    .await?;
    rig.pty_clients.push(client);
    let origin = poll_until(
        "focused Herdr origin pane",
        std::time::Duration::from_secs(5),
        async || {
            muxe_adapter_herdr::focused_pane(&runtime)
                .await
                .map_err(|error| error.to_string())
        },
    )
    .await?;
    Ok((socket, runtime, origin, typescript))
}

async fn launch_herdr_smoke_menu(
    rig: &Rig,
    target_bin: &Path,
    socket: &Path,
    origin: &muxe_adapter_herdr::FocusedPane,
) -> io::Result<muxe_core::PaneId> {
    let mut command = tokio::process::Command::new(target_bin);
    command.args([
        "menu",
        "open",
        "--pane-type",
        "split",
        "--direction",
        "down",
        "--height",
        "30%",
        "main",
    ]);
    apply_scoped_env(&mut command, &rig.scoped_root);
    command
        .env("HERDR_SOCKET_PATH", socket)
        .env("HERDR_ACTIVE_WORKSPACE_ID", origin.workspace.as_str())
        .env("HERDR_ACTIVE_TAB_ID", origin.tab.as_str())
        .env("HERDR_ACTIVE_PANE_ID", origin.pane.as_str())
        .current_dir(&origin.cwd);
    let output = run_cli_bounded("herdr-menu-launcher", &mut command).await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "smoke: Herdr menu launcher failed (status {:?}):\n--- stdout ---\n{}\n--- stderr ---\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("opened "))
        .filter(|pane| !pane.is_empty())
        .map(muxe_core::PaneId::new)
        .ok_or_else(|| io::Error::other(format!("smoke: launcher reported no pane: {stdout}")))
}

async fn assert_herdr_menu_stays_open(
    rig: &mut Rig,
    target_bin: &Path,
    herdr_binary: &Path,
) -> io::Result<()> {
    let (socket, runtime, origin, typescript) =
        prepare_herdr_menu_origin(rig, herdr_binary).await?;
    let pane = launch_herdr_smoke_menu(rig, target_bin, &socket, &origin).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let menu_pane = muxe_adapter_herdr::pane_by_id(&runtime, pane.as_str())
        .await
        .map_err(|error| {
            let transcript = std::fs::read(&typescript).map_or_else(
                |read_error| format!("(transcript unreadable: {read_error})"),
                |bytes| {
                    let start = bytes.len().saturating_sub(16 * 1024);
                    String::from_utf8_lossy(&bytes[start..]).into_owned()
                },
            );
            io::Error::other(format!(
                "smoke: valid Herdr menu pane {pane} exited immediately: {error}\n\
                 --- attached Herdr transcript ---\n{transcript}"
            ))
        })?;
    let expected_rows = u16::try_from((u32::from(origin.rows) * 3 + 5) / 10)
        .expect("30% of a u16 row count fits in u16");
    let actual_rows = menu_pane.rows;
    if actual_rows.abs_diff(expected_rows) > 1 {
        return Err(io::Error::other(format!(
            "smoke: Herdr menu pane {pane} has {actual_rows} rows; expected 30% of the {origin_rows}-row origin (within one cell)",
            origin_rows = origin.rows,
        )));
    }
    eprintln!(
        "[smoke] Herdr menu pane {pane} remained live after launcher exit at {actual_rows}/{origin_rows} rows",
        origin_rows = origin.rows,
    );
    Ok(())
}

async fn herdr_smoke_request(
    runtime: &muxe_adapter_herdr::HerdrRuntime,
    method: &str,
    params: serde_json::Value,
) -> io::Result<serde_json::Value> {
    match runtime
        .invoke_response(method, params)
        .await
        .map_err(|error| io::Error::other(format!("herdr-tab-create: {method}: {error}")))?
    {
        muxe_adapter_herdr::HerdrResponse::Success(value) => Ok(value),
        muxe_adapter_herdr::HerdrResponse::Error { code, message } => Err(io::Error::other(
            format!("herdr-tab-create: {method} rejected with {code}: {message}"),
        )),
    }
}

async fn herdr_smoke_snapshot(
    runtime: &muxe_adapter_herdr::HerdrRuntime,
) -> io::Result<serde_json::Value> {
    let response = herdr_smoke_request(runtime, "session.snapshot", serde_json::json!({})).await?;
    if response.get("type").and_then(serde_json::Value::as_str) != Some("session_snapshot") {
        return Err(io::Error::other(format!(
            "unexpected snapshot response: {response}"
        )));
    }
    response
        .get("snapshot")
        .filter(|snapshot| snapshot.is_object())
        .cloned()
        .ok_or_else(|| io::Error::other(format!("missing snapshot object: {response}")))
}

fn herdr_created_tab_pane(
    snapshot: &serde_json::Value,
    origin: &muxe_adapter_herdr::FocusedPane,
    initial_tabs: &BTreeSet<muxe_core::TabId>,
    menu_pane: &muxe_core::PaneId,
) -> Result<muxe_core::PaneId, String> {
    let panes = snapshot
        .get("panes")
        .and_then(serde_json::Value::as_array)
        .ok_or("snapshot lacks panes")?;
    if panes.iter().any(|pane| {
        pane.get("pane_id").and_then(serde_json::Value::as_str) == Some(menu_pane.as_str())
    }) {
        return Err("menu UI pane is still live".into());
    }
    let tabs = snapshot
        .get("tabs")
        .and_then(serde_json::Value::as_array)
        .ok_or("snapshot lacks tabs")?;
    let new_tabs: Vec<_> = tabs
        .iter()
        .filter(|tab| {
            tab.get("tab_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| !initial_tabs.contains(&muxe_core::TabId::new(id)))
        })
        .collect();
    if new_tabs.len() != 1 {
        return Err(format!(
            "expected exactly one new tab, found {}",
            new_tabs.len()
        ));
    }
    let tab = new_tabs[0];
    let new_tab = tab
        .get("tab_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(muxe_core::TabId::new)
        .ok_or("new tab lacks identity")?;
    if tab.get("workspace_id").and_then(serde_json::Value::as_str)
        != Some(origin.workspace.as_str())
        || snapshot
            .get("focused_workspace_id")
            .and_then(serde_json::Value::as_str)
            != Some(origin.workspace.as_str())
        || snapshot
            .get("focused_tab_id")
            .and_then(serde_json::Value::as_str)
            != Some(new_tab.as_str())
    {
        return Err("new tab is not focused in the origin workspace".into());
    }
    let pane = panes
        .iter()
        .find(|pane| {
            pane.get("workspace_id").and_then(serde_json::Value::as_str)
                == Some(origin.workspace.as_str())
                && pane.get("tab_id").and_then(serde_json::Value::as_str) == Some(new_tab.as_str())
                && pane.get("pane_id").and_then(serde_json::Value::as_str)
                    == snapshot
                        .get("focused_pane_id")
                        .and_then(serde_json::Value::as_str)
        })
        .ok_or("new tab has no focused shell pane")?;
    let pane_id = pane
        .get("pane_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
        .map(muxe_core::PaneId::new)
        .ok_or("new pane lacks identity")?;
    eprintln!("[herdr-tab-create] UI gone; focused new tab {new_tab}, pane {pane_id}");
    Ok(pane_id)
}

async fn assert_herdr_menu_creates_tab(
    rig: &mut Rig,
    target_bin: &Path,
    herdr_binary: &Path,
) -> io::Result<()> {
    let (socket, runtime, origin, typescript) =
        prepare_herdr_menu_origin(rig, herdr_binary).await?;
    let result = async {
        let before = herdr_smoke_snapshot(&runtime).await?;
        let initial_tabs: BTreeSet<muxe_core::TabId> = before
            .get("tabs")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| io::Error::other(format!("snapshot lacks tabs: {before}")))?
            .iter()
            .map(|tab| {
                tab.get("tab_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|id| !id.is_empty())
                    .map(muxe_core::TabId::new)
                    .ok_or_else(|| io::Error::other(format!("tab lacks identity: {tab}")))
            })
            .collect::<io::Result<_>>()?;
        let menu_pane = launch_herdr_smoke_menu(rig, target_bin, &socket, &origin).await?;
        // Allow the real terminal UI to attach and enter its input loop before
        // sending the same two keys as the user's main -> tabs -> tab:create.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        muxe_adapter_herdr::pane_by_id(&runtime, menu_pane.as_str())
            .await
            .map_err(|error| io::Error::other(format!("menu exited before input: {error}")))?;
        for _ in 0..2 {
            herdr_smoke_request(
                &runtime,
                "pane.send_keys",
                serde_json::json!({ "pane_id": menu_pane.as_str(), "keys": ["t"] }),
            )
            .await?;
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
        let new_pane = poll_until(
            "bare tab:create to dismiss its UI and focus a new origin-workspace tab",
            std::time::Duration::from_secs(10),
            async || {
                let snapshot = herdr_smoke_snapshot(&runtime)
                    .await
                    .map_err(|error| error.to_string())?;
                herdr_created_tab_pane(&snapshot, &origin, &initial_tabs, &menu_pane)
                    .map_err(|error| format!("{error}\n--- authoritative snapshot ---\n{snapshot}"))
            },
        )
        .await?;
        // A real shell must execute the probe, not merely leave an empty layout.
        let marker = rig.workdir.join("new-tab-shell-proof");
        let quoted_marker = marker.to_string_lossy().replace('\'', "'\\''");
        herdr_smoke_request(
            &runtime,
            "pane.send_text",
            serde_json::json!({
                "pane_id": new_pane.as_str(),
                "text": format!("printf '%s' muxe-live-tab-shell > '{quoted_marker}'"),
            }),
        )
        .await?;
        herdr_smoke_request(
            &runtime,
            "pane.send_keys",
            serde_json::json!({ "pane_id": new_pane.as_str(), "keys": ["enter"] }),
        )
        .await?;
        poll_until(
            "new tab shell to execute its proof command",
            std::time::Duration::from_secs(5),
            async || match std::fs::read_to_string(&marker) {
                Ok(value) if value == "muxe-live-tab-shell" => Ok(()),
                other => Err(format!("shell proof not present: {other:?}")),
            },
        )
        .await
    }
    .await;
    result.map_err(|error| {
        let transcript = std::fs::read(&typescript).map_or_else(
            |read_error| format!("(transcript unreadable: {read_error})"),
            |bytes| {
                String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(16 * 1024)..])
                    .into_owned()
            },
        );
        io::Error::other(format!(
            "{error}\n--- attached Herdr transcript ---\n{transcript}"
        ))
    })
}

#[derive(Clone, Copy)]
enum HerdrMenuSmoke {
    StaysOpen,
    BareTabCreate,
}

async fn run_herdr_menu_smoke(scenario: HerdrMenuSmoke) -> io::Result<()> {
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let target_version = installed_version(&target_bin).await?;
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let root = short_tempdir("muxe-live-herdr-menu-")?;
    let scoped_root = root.path().join("scoped");
    let (config_file, cache_dir) = init_shared_dirs(&target_bin, &scoped_root).await?;
    if matches!(scenario, HerdrMenuSmoke::BareTabCreate) {
        std::fs::write(
            &config_file,
            "version: 1\nsettings:\n  timeout: off\nmenus:\n  main:\n    bindings:\n      t: { label: tabs, action: 'menu:open tabs' }\n  tabs:\n    bindings:\n      t: { label: new tab, action: 'tab:create' }\n",
        )?;
    }
    let workdir = root.path().join("work");
    std::fs::create_dir_all(&workdir)?;
    let herdr =
        OwnedHerdrServer::start(&herdr_binary, root.path(), &scoped_root, "herdr-menu").await?;
    let discovery = herdr.discovery_key().to_owned();
    let mut rig = Rig {
        root,
        scoped_root,
        workdir,
        config_file,
        cache_dir,
        herdr: Some(herdr),
        discovery: discovery.clone(),
        zellij: None,
        pty_clients: Vec::new(),
        brokers: Vec::new(),
        continuity: None,
    };
    let result = async {
        if rig
            .herdr
            .as_mut()
            .expect("owned server is retained")
            .try_wait()?
            .is_some()
        {
            return Err(io::Error::other(
                "herdr-menu: Herdr server exited on startup",
            ));
        }
        rig.continuity = Some(
            ContinuityGuard::watch_herdr(
                "herdr-menu",
                muxe_adapter_herdr::HerdrAdapterConfig {
                    socket_path: rig
                        .herdr
                        .as_ref()
                        .expect("owned server is retained")
                        .socket()
                        .to_path_buf(),
                    herdr_binary: herdr_binary.clone(),
                    cache_dir: rig.cache_dir.clone(),
                },
                discovery.clone(),
            )
            .await?,
        );
        let server = rig
            .herdr
            .as_ref()
            .ok_or_else(|| io::Error::other("herdr-menu: Herdr server is gone"))?;
        let broker = spawn_serve_herdr(
            "herdr-menu",
            &target_bin,
            &herdr_binary,
            server.socket(),
            &discovery,
            &rig.scoped_root,
            &rig.config_file,
            &rig.cache_dir,
        )
        .await?;
        let endpoint = broker.endpoint.clone();
        rig.brokers.push(broker);
        assert_broker_serving(&endpoint, "herdr-menu", &target_version).await?;
        match scenario {
            HerdrMenuSmoke::StaysOpen => {
                assert_herdr_menu_stays_open(&mut rig, &target_bin, &herdr_binary).await
            }
            HerdrMenuSmoke::BareTabCreate => {
                assert_herdr_menu_creates_tab(&mut rig, &target_bin, &herdr_binary).await
            }
        }
    }
    .await;
    rig.finish("herdr-menu", result).await
}
async fn run_target_only_smoke() -> io::Result<()> {
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let target_version = installed_version(&target_bin).await?;

    let target_digest = installed_wasm_digest(&target_bin).await?;
    eprintln!("[smoke] target {target_version} packaged_wasm.sha256={target_digest:?}");
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let zellij_binary = input_path("MUXE_ZELLIJ_BINARY");
    let foreground = input_path("MUXE_ZELLIJ_FOREGROUND_BINARY");
    let bootstrap = input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY");
    let seeder = input_path("MUXE_ZELLIJ_PERMISSION_SEEDER");

    let mut rig = bring_hosts(
        "smoke",
        &target_bin,
        &target_bin,
        &herdr_binary,
        &zellij_binary,
        &foreground,
        &bootstrap,
        &seeder,
        &[("smoke", 1)],
        None,
    )
    .await?;
    let result = async {
        let (herdr_endpoint, zellij_endpoints) = serve_old_brokers(
            &mut rig,
            &target_bin,
            &herdr_binary,
            &zellij_binary,
            "smoke-old",
            &["smoke"],
        )
        .await?;
        // Pre-transfer sanity: the old broker serves its installed version.
        assert_broker_serving(&herdr_endpoint, "smoke", &target_version).await?;
        let report = drive_activate(
            &target_bin,
            &rig.scoped_root,
            rig.activate_environment(),
            "smoke",
        )
        .await?;
        eprintln!("[smoke] activate report:\n{report}");
        let live = assert_broker_serving(&herdr_endpoint, "smoke", &target_version).await?;
        if live != rig.discovery {
            return Err(io::Error::other(format!(
                "smoke: serving discovery changed (want {})",
                rig.discovery
            )));
        }
        let endpoint = zellij_endpoints
            .first()
            .ok_or_else(|| io::Error::other("smoke: no zellij endpoint"))?
            .clone();
        let session_live = assert_broker_serving(&endpoint, "smoke", &target_version).await?;
        if session_live != "smoke" {
            return Err(io::Error::other(format!(
                "smoke: zellij serving identity is {session_live:?}, want session \"smoke\""
            )));
        }
        assert_herdr_menu_stays_open(&mut rig, &target_bin, &herdr_binary).await?;
        Ok(())
    }
    .await;
    // The one-client activation scenario above is unchanged; the same exact
    // test then runs the simultaneous two-client regression on a second
    // fresh Rig under its own case label.
    rig.finish("smoke", result).await?;
    run_simultaneous_two_clients().await
}

/// Drives one public-`activate` transfer and asserts every endpoint serves
/// its exact probed record with handoff and discovery match (plus
/// snapshot-gated Zellij coverage in the fault case). Callers bracket
/// each transfer with `assert_no_preserved_journals`: clean before
/// (no stale recovery state enters group selection) and clean after
/// (commit left nothing preserved). Together with the full-member
/// record transitions this proves the complete group moved. Registration-level
/// bridge grouping is recorded (`serve-zellij` registers the canonical stable
/// `bridge_path`) and the grouping predicate itself is proved host-free by
/// `bridge_sharing_registrations_form_one_atomic_group`; what this witness
/// does not yet read back is per-member registry `bridge_path` equality
/// during the live run (success removes the journals, so only Status
/// records are observed here).
#[expect(
    clippy::too_many_arguments,
    reason = "transfer witness threads the complete expected group (binary, herdr record, session records, endpoints, sessions, case) so one call proves the whole unit moved; bundling would hide the per-member evidence"
)]
async fn transfer_to(
    rig: &Rig,
    to_bin: &Path,
    expected_herdr: &CompatibilityRecord,
    expected_sessions: &[CompatibilityRecord],
    herdr_endpoint: &Path,
    zellij_endpoints: &[PathBuf],
    sessions: &[&str],
    case: &str,
) -> io::Result<()> {
    let report = drive_activate(to_bin, &rig.scoped_root, rig.activate_environment(), case).await?;
    eprintln!("[matrix] {case} activate report:\n{report}");
    assert_serving_record(herdr_endpoint, case, expected_herdr, &rig.discovery).await?;
    for (endpoint, expected, session) in zellij_endpoints
        .iter()
        .zip(expected_sessions)
        .zip(sessions)
        .map(|((endpoint, expected), session)| (endpoint, expected, session))
    {
        assert_serving_record(endpoint, case, expected, session).await?;
    }
    Ok(())
}
async fn assert_group_serving(
    endpoints: &[PathBuf],
    expected_records: &[CompatibilityRecord],
    sessions: &[&str],
) -> io::Result<()> {
    for (endpoint, expected, session) in endpoints
        .iter()
        .zip(expected_records)
        .zip(sessions)
        .map(|((endpoint, expected), session)| (endpoint, expected, session))
    {
        assert_serving_record(endpoint, "matrix", expected, session).await?;
    }
    Ok(())
}

async fn run_upgrade_and_rollback() -> io::Result<()> {
    let old_bin = validate_installation(&input_path("MUXE_OLD_INSTALLATION")).await;
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let old_version = installed_version(&old_bin).await?;
    let target_version = installed_version(&target_bin).await?;
    if old_version == target_version {
        return Err(io::Error::other(format!(
            "old and target installations report the same version {old_version}; \
             no upgrade to rehearse and no waiver to grant"
        )));
    }
    for (label, binary) in [("old", &old_bin), ("target", &target_bin)] {
        let digest = installed_wasm_digest(binary).await?;
        eprintln!("[matrix] {label} packaged_wasm.sha256={digest:?}");
    }
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let zellij_binary = input_path("MUXE_ZELLIJ_BINARY");
    let foreground = input_path("MUXE_ZELLIJ_FOREGROUND_BINARY");
    let bootstrap = input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY");
    let seeder = input_path("MUXE_ZELLIJ_PERMISSION_SEEDER");
    let sessions = ["matrix-alpha", "matrix-beta"];

    let mut rig = bring_hosts(
        "matrix",
        &target_bin,
        &old_bin,
        &herdr_binary,
        &zellij_binary,
        &foreground,
        &bootstrap,
        &seeder,
        &[("matrix-alpha", 2), ("matrix-beta", 1)],
        None,
    )
    .await?;
    let result = async {
        let (old_herdr, old_sessions) = probe_records(
            &rig,
            &old_bin,
            &herdr_binary,
            &zellij_binary,
            "matrix-old",
            &sessions,
        )
        .await?;
        let (target_herdr, target_sessions) = probe_records(
            &rig,
            &target_bin,
            &herdr_binary,
            &zellij_binary,
            "matrix-target",
            &sessions,
        )
        .await?;
        let (herdr_endpoint, zellij_endpoints) = serve_old_brokers(
            &mut rig,
            &old_bin,
            &herdr_binary,
            &zellij_binary,
            "matrix-old",
            &sessions,
        )
        .await?;
        assert_serving_record(&herdr_endpoint, "matrix", &old_herdr, &rig.discovery).await?;
        assert_group_serving(&zellij_endpoints, &old_sessions, &sessions).await?;
        assert_no_preserved_journals(&rig.cache_dir, "matrix-pre")?;
        transfer_to(
            &rig,
            &target_bin,
            &target_herdr,
            &target_sessions,
            &herdr_endpoint,
            &zellij_endpoints,
            &sessions,
            "upgrade",
        )
        .await?;
        // Post-commit group hygiene: the commit cleaned up before rollback.
        assert_no_preserved_journals(&rig.cache_dir, "matrix-upgraded")?;
        transfer_to(
            &rig,
            &old_bin,
            &old_herdr,
            &old_sessions,
            &herdr_endpoint,
            &zellij_endpoints,
            &sessions,
            "rollback",
        )
        .await?;
        assert_no_preserved_journals(&rig.cache_dir, "matrix-rolled-back")?;
        Ok(())
    }
    .await;
    rig.finish("matrix", result).await
}

#[expect(
    clippy::too_many_lines,
    reason = "two-phase fault transaction (upgrade, barrier-armed downgrade, recovery asserts) reads linearly; splitting would scatter the phase coupling the ignored live gate exists to rehearse"
)]
async fn run_final_session_reload_failure() -> io::Result<()> {
    let old_bin = validate_installation(&input_path("MUXE_OLD_INSTALLATION")).await;
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let old_version = installed_version(&old_bin).await?;
    let target_version = installed_version(&target_bin).await?;
    if old_version == target_version {
        return Err(io::Error::other(format!(
            "old and target installations report the same version {old_version}; \
             no upgrade to rehearse and no waiver to grant"
        )));
    }
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let zellij_binary = input_path("MUXE_ZELLIJ_BINARY");
    let foreground = input_path("MUXE_ZELLIJ_FOREGROUND_BINARY");
    let bootstrap = input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY");
    let seeder = input_path("MUXE_ZELLIJ_PERMISSION_SEEDER");
    let injector_bin = input_path("MUXE_ZELLIJ_FAULT_INJECTOR");
    if !injector_bin.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "fault injector is not a file (second bin of the excluded fixture workspace?): {}",
                injector_bin.display()
            ),
        ));
    }
    let sessions = ["fault-alpha", "fault-beta"];
    let final_session = "fault-beta";

    let mut rig = bring_hosts(
        "fault",
        &target_bin,
        &old_bin,
        &herdr_binary,
        &zellij_binary,
        &foreground,
        &bootstrap,
        &seeder,
        &[("fault-alpha", 1), ("fault-beta", 1)],
        None,
    )
    .await?;
    let result = async {
        // Phase 1: a real successful upgrade, establishing the installed
        // bridge whose digest the faulted run must restore.
        // Probe both records up front while every endpoint is free:
        // probing later would contend with serving brokers.
        let (old_herdr, old_sessions) = probe_records(
            &rig,
            &old_bin,
            &herdr_binary,
            &zellij_binary,
            "fault-old",
            &sessions,
        )
        .await?;
        let (target_herdr, target_sessions) = probe_records(
            &rig,
            &target_bin,
            &herdr_binary,
            &zellij_binary,
            "fault-target",
            &sessions,
        )
        .await?;
        let (herdr_endpoint, zellij_endpoints) = serve_old_brokers(
            &mut rig,
            &old_bin,
            &herdr_binary,
            &zellij_binary,
            "fault-old",
            &sessions,
        )
        .await?;
        transfer_to(
            &rig,
            &target_bin,
            &target_herdr,
            &target_sessions,
            &herdr_endpoint,
            &zellij_endpoints,
            &sessions,
            "fault-setup",
        )
        .await?;
        let restored_digest = stable_digest(&rig.config_file)?;
        eprintln!("[fault] installed bridge digest after setup: {restored_digest}");

        // Phase 2: arm the one-shot injector for the final session, then
        // run the downgrade activation in the background.
        let injector_dir = rig.workdir.join("injector-bin");
        std::fs::create_dir_all(&injector_dir)?;
        symlink(&injector_bin, injector_dir.join("zellij"))?;
        let barrier_dir = rig.workdir.join("fault-barrier");
        std::fs::create_dir_all(&barrier_dir)?;
        let stable = muxe::integration::stable_bridge_path(
            rig.config_file
                .parent()
                .ok_or_else(|| io::Error::other("fault: config file has no parent"))?,
        );
        let stable_url = muxe::integration::kdl::bridge_url(&stable);
        let fail_url = format!("file:{}", rig.workdir.join("does-not-exist.wasm").display());
        let injector_env = [
            (
                "MUXE_FI_REAL_ZELLIJ",
                zellij_binary.to_string_lossy().into_owned(),
            ),
            ("MUXE_FI_FINAL_SESSION", final_session.to_owned()),
            ("MUXE_FI_STABLE_URL", stable_url.clone()),
            (
                "MUXE_FI_BARRIER_DIR",
                barrier_dir.to_string_lossy().into_owned(),
            ),
            ("MUXE_FI_FAIL_URL", fail_url),
        ];
        let injector_refs: Vec<(&str, &str)> = injector_env
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let child = spawn_activate(
            &old_bin,
            &rig.scoped_root,
            rig.activate_environment(),
            Some(&injector_dir),
            &injector_refs,
            "fault-downgrade",
        )?;

        // Barrier: wait for the injector's hold on the final reload, then
        // prove every earlier member healthy — the Herdr target on its
        // probed record plus the first session on snapshot-gated coverage
        // with live clients — and only then release the failure.
        // Recovery boundary: the held final reload means the final member
        // can never be Ready, so no durable all-members-Ready journal
        // exists when the failure fires; the coordinator must roll back,
        // never converge a commit on the volatile census.
        let request = barrier_dir.join(format!("request-{final_session}"));
        poll_until("fault barrier request", BARRIER_TIMEOUT, || async {
            if request.exists() {
                Ok(())
            } else {
                Err("injector has not held the final reload yet".to_owned())
            }
        })
        .await?;
        let Some(host) = rig.zellij.as_ref() else {
            return Err(io::Error::other(
                "fault: zellij host is gone for the barrier",
            ));
        };
        await_session_ready(
            &zellij_endpoints[0],
            "fault-alpha",
            &old_sessions[0],
            "fault-alpha",
            host,
            "fault-alpha",
            READY_TIMEOUT,
        )
        .await?;
        await_target_ready(
            &herdr_endpoint,
            "fault-herdr",
            &old_herdr,
            &rig.discovery,
            READY_TIMEOUT,
        )
        .await?;
        for client in &mut rig.pty_clients {
            if client.try_wait()?.is_some() {
                return Err(io::Error::other(
                    "fault: a PTY client exited before the barrier released",
                ));
            }
        }
        std::fs::write(
            barrier_dir.join(format!("release-{final_session}")),
            b"release",
        )?;
        let outcome = await_activate(child, "fault-downgrade").await;
        match &outcome {
            Ok(output) => eprintln!("[fault] downgrade activate exited clean:\n{output}"),
            Err(error) => {
                eprintln!("[fault] downgrade activate ended nonzero (abort surfacing): {error}");
            }
        }

        // The fault must have fired exactly once: without the trigger the
        // run proves nothing.
        if !barrier_dir.join("consumed").is_file() {
            return Err(io::Error::other(
                "fault: injector never triggered (coordinator bypassed scoped PATH?); \
                 no failure was exercised",
            ));
        }
        // Recovery: the pre-fault group is complete again. The old-version
        // brokers serve their exact probed records, the stable bridge
        // digest is the installed one, both sessions stay live, and the
        // host servers never restarted (witnesses close over the run).
        assert_serving_record(&herdr_endpoint, "fault", &target_herdr, &rig.discovery).await?;
        for (endpoint, expected, session) in zellij_endpoints
            .iter()
            .zip(&target_sessions)
            .zip(sessions)
            .map(|((endpoint, expected), session)| (endpoint, expected, session))
        {
            assert_serving_record(endpoint, "fault", expected, session).await?;
        }
        let after = stable_digest(&rig.config_file)?;
        if after != restored_digest {
            return Err(io::Error::other(format!(
                "fault: stable bridge digest changed ({restored_digest} -> {after}); \
                 the old bridge was not restored"
            )));
        }
        let Some(host) = rig.zellij.as_ref() else {
            return Err(io::Error::other("fault: zellij host is gone"));
        };
        host.wait_for_session("fault-alpha").await?;
        host.wait_for_session("fault-beta").await?;
        // Rollback cleanup: no aborted unit may linger preserved.
        assert_no_preserved_journals(&rig.cache_dir, "fault-rolled-back")?;
        Ok(())
    }
    .await;
    rig.finish("fault", result).await
}

#[tokio::test]
#[ignore = "live Herdr: needs a staged install, Herdr binary, and MUXE_LIVE_HOSTS_APPROVED=true"]
async fn herdr_menu_stays_open() {
    require_live_approval();
    if let Err(error) = run_herdr_menu_smoke(HerdrMenuSmoke::StaysOpen).await {
        panic!("Herdr menu smoke failed: {error}");
    }
}

/// Run against either an installed release or a staged fixed installation:
///
/// ```sh
/// MUXE_LIVE_HOSTS_APPROVED=true MUXE_TARGET_INSTALLATION=/absolute/install \
/// MUXE_HERDR_BINARY=/absolute/herdr cargo test --locked -p muxe --test live_hosts \
/// -- --ignored --exact herdr_menu_bare_tab_create --nocapture
/// ```
#[tokio::test]
#[ignore = "live Herdr: needs a staged install, Herdr binary, and MUXE_LIVE_HOSTS_APPROVED=true"]
async fn herdr_menu_bare_tab_create() {
    require_live_approval();
    if let Err(error) = run_herdr_menu_smoke(HerdrMenuSmoke::BareTabCreate).await {
        panic!("Herdr bare tab:create regression failed: {error}");
    }
}

#[tokio::test]
#[ignore = "live hosts: needs staged installs, pinned host binaries, and MUXE_LIVE_HOSTS_APPROVED=true"]
async fn target_only_smoke() {
    require_live_approval();
    if let Err(error) = run_target_only_smoke().await {
        panic!("target-only smoke failed: {error}");
    }
}

#[tokio::test]
#[ignore = "live hosts: needs staged installs, pinned host binaries, and MUXE_LIVE_HOSTS_APPROVED=true"]
async fn upgrade_and_rollback() {
    require_live_approval();
    if let Err(error) = run_upgrade_and_rollback().await {
        panic!("upgrade and rollback matrix failed: {error}");
    }
}

#[tokio::test]
#[ignore = "live hosts: needs staged installs, pinned host binaries, and MUXE_LIVE_HOSTS_APPROVED=true"]
async fn final_session_reload_failure() {
    require_live_approval();
    if let Err(error) = run_final_session_reload_failure().await {
        panic!("final-session reload failure case failed: {error}");
    }
}
/// A previously loaded same-source bridge may cover the target briefly even
/// when the target subscribed to its predecessor. The post-commit lease
/// witness must still see the exact retained client after old retirement.
async fn prove_zellij_coverage_survives_lease(
    endpoint: &Path,
    host: &OwnedZellijHost,
    session: &str,
) -> io::Result<()> {
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    let initial = control
        .status()
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    drop(control);
    if initial.handoff_id.is_none() {
        return Err(io::Error::other("target has no committed handoff"));
    }
    await_session_ready(
        endpoint,
        "same-source-initial",
        &initial.current,
        session,
        host,
        session,
        READY_TIMEOUT,
    )
    .await?;
    tokio::time::sleep(muxe_adapter_zellij::HEARTBEAT_LEASE + std::time::Duration::from_secs(8))
        .await;
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    let current = control
        .status()
        .await
        .map_err(|error| io::Error::other(error.to_string()))?;
    let snapshot = host.list_clients(session).await?;
    let ready = current
        .ready
        .as_ref()
        .ok_or_else(|| io::Error::other("target lost readiness after heartbeat lease"))?;
    if current.lifecycle != muxe_protocol::control::LifecycleState::Running
        || current.handoff_id != initial.handoff_id
        || current.live_server != initial.live_server
        || current.current != initial.current
        || snapshot.len() != 1
        || ready.member_clients != 1
        || ready.member_ids.as_ref() != Some(&snapshot)
        || ready.registered_clients != snapshot
    {
        return Err(io::Error::other(format!(
            "target lost exact one-client coverage after lease: snapshot={snapshot:?}, status={current:?}"
        )));
    }
    Ok(())
}

async fn run_same_source_bridge_replacement() -> io::Result<()> {
    let old_bin = validate_installation(&input_path("MUXE_OLD_INSTALLATION")).await;
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let old_digest = installed_wasm_digest(&old_bin)
        .await?
        .ok_or_else(|| io::Error::other("old same-source installation lacks a packaged bridge"))?;
    let target_digest = installed_wasm_digest(&target_bin).await?.ok_or_else(|| {
        io::Error::other("target same-source installation lacks a packaged bridge")
    })?;
    if old_digest == target_digest
        || installed_version(&old_bin).await? != installed_version(&target_bin).await?
    {
        return Err(io::Error::other(
            "same-source probe requires one version with distinct producer WASM bytes",
        ));
    }
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let zellij_binary = input_path("MUXE_ZELLIJ_BINARY");
    let foreground = input_path("MUXE_ZELLIJ_FOREGROUND_BINARY");
    let bootstrap = input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY");
    let seeder = input_path("MUXE_ZELLIJ_PERMISSION_SEEDER");
    let mut rig = bring_hosts(
        "same-source",
        &target_bin,
        &old_bin,
        &herdr_binary,
        &zellij_binary,
        &foreground,
        &bootstrap,
        &seeder,
        &[("same-source", 1)],
        None,
    )
    .await?;
    let result = async {
        let (herdr_endpoint, zellij_endpoints) = serve_old_brokers(
            &mut rig,
            &old_bin,
            &herdr_binary,
            &zellij_binary,
            "same-source-old",
            &["same-source"],
        )
        .await?;
        if stable_digest(&rig.config_file)? != old_digest {
            return Err(io::Error::other("old bridge does not match its receipt"));
        }
        assert_no_preserved_journals(&rig.cache_dir, "same-source-before")?;
        drive_activate(
            &target_bin,
            &rig.scoped_root,
            rig.activate_environment(),
            "same-source",
        )
        .await?;
        if stable_digest(&rig.config_file)? != target_digest {
            return Err(io::Error::other("target bridge bytes were not installed"));
        }
        assert_no_preserved_journals(&rig.cache_dir, "same-source-after")?;
        let version = installed_version(&target_bin).await?;
        assert_broker_serving(&herdr_endpoint, "same-source-herdr", &version).await?;
        let endpoint = zellij_endpoints
            .first()
            .ok_or_else(|| io::Error::other("same-source host has no broker endpoint"))?;
        assert_broker_serving(endpoint, "same-source-zellij", &version).await?;
        let host = rig
            .zellij
            .as_ref()
            .ok_or_else(|| io::Error::other("owned Zellij host disappeared"))?;
        prove_zellij_coverage_survives_lease(endpoint, host, "same-source").await
    }
    .await;
    rig.finish("same-source", result).await
}

#[tokio::test]
#[ignore = "live same-source debug/release installations and explicit host approval required"]
async fn same_source_bridge_replacement_retains_live_client_past_lease() {
    require_live_approval();
    if let Err(error) = run_same_source_bridge_replacement().await {
        panic!("same-source bridge replacement smoke failed: {error}");
    }
}

async fn ordinary_zellij_client_coverage(
    endpoint: &Path,
    host: &OwnedZellijHost,
    session: &str,
    expected: &CompatibilityRecord,
) -> Result<(), String> {
    let census = host
        .list_clients(session)
        .await
        .map_err(|error| error.to_string())?;
    let mut control = muxe::lifecycle::control::ControlClient::connect(endpoint)
        .await
        .map_err(|error| error.to_string())?;
    let status = control.status().await.map_err(|error| error.to_string())?;
    let covered = status.ready.as_ref().is_some_and(|ready| {
        ready.member_clients == 1
            && ready.member_ids.as_ref() == Some(&census)
            && ready.registered_clients == census
    });
    if status.handoff_id.is_none()
        && status.lifecycle == muxe_protocol::control::LifecycleState::Running
        && status.current == *expected
        && status.live_server.discovery_key == session
        && census.len() == 1
        && covered
    {
        Ok(())
    } else {
        Err(format!(
            "ordinary menu coldstart lost exact client coverage: status={status:?}, census={census:?}"
        ))
    }
}

async fn assert_live_zellij_menu_pane(
    host: &OwnedZellijHost,
    zellij_binary: &Path,
    scoped_root: &Path,
    workdir: &Path,
    session: &str,
    target_bin: &Path,
) -> io::Result<()> {
    let mut command = tokio::process::Command::new(zellij_binary);
    command.args([
        "--session",
        session,
        "action",
        "list-panes",
        "--all",
        "--json",
    ]);
    host.apply_host_scoped_env(&mut command, scoped_root);
    command.current_dir(workdir);
    let output = run_cli_bounded("menu-live-pane", &mut command).await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "cannot inspect menu panes: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let panes: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
    let target = target_bin
        .to_str()
        .ok_or_else(|| io::Error::other("target installation is not UTF-8"))?;
    let mut menus = panes.iter().filter(|pane| {
        pane.get("terminal_command")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|command| command.contains(target) && command.contains(" ui menu main"))
    });
    let menu = menus.next().ok_or_else(|| {
        io::Error::other(format!(
            "no live UI command pane: {}",
            String::from_utf8_lossy(&output.stdout)
        ))
    })?;
    if menus.next().is_some()
        || menu.get("is_plugin").and_then(serde_json::Value::as_bool) != Some(false)
        || menu.get("is_focused").and_then(serde_json::Value::as_bool) != Some(true)
        || menu.get("exited").and_then(serde_json::Value::as_bool) != Some(false)
        || menu.get("is_held").and_then(serde_json::Value::as_bool) != Some(false)
    {
        return Err(io::Error::other(format!(
            "UI pane exited, lost focus, or is ambiguous: {}",
            String::from_utf8_lossy(&output.stdout)
        )));
    }
    let id = menu
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| io::Error::other("UI pane lacks numeric host ID"))?;
    let pane = muxe_protocol::HostPaneId::new(format!("terminal_{id}"));
    let mut inventory = tokio::process::Command::new(zellij_binary);
    inventory.args(["--session", session, "action", "list-clients"]);
    host.apply_host_scoped_env(&mut inventory, scoped_root);
    inventory.current_dir(workdir);
    let clients = run_cli_bounded("menu-live-client", &mut inventory).await?;
    let text = String::from_utf8_lossy(&clients.stdout);
    let mut rows = text.lines().skip(1);
    if !clients.status.success()
        || rows.next().and_then(|row| row.split_whitespace().nth(1)) != Some(pane.as_str())
        || rows.next().is_some()
    {
        return Err(io::Error::other(format!(
            "UI pane is not owned by the sole live client: {text}"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum OwnedMenuMode {
    Normal,
    Locked,
}

impl OwnedMenuMode {
    const fn case(self) -> &'static str {
        match self {
            Self::Normal => "mn",
            Self::Locked => "ml",
        }
    }

    fn markers(self, root: &Path) -> (PathBuf, PathBuf) {
        let normal = root.join("normal-mode.marker");
        let locked = root.join("locked-mode.marker");
        match self {
            Self::Normal => (normal, locked),
            Self::Locked => (locked, normal),
        }
    }
}

/// A public menu on a fresh session must coldstart one broker without a
/// manually pre-served endpoint, and retain client coverage beyond the lease.
#[expect(
    clippy::too_many_lines,
    reason = "the ignored live scenario brackets one fresh Rig and its full public hotkey, lease, dismissal, mode-restoration and teardown proof"
)]
async fn run_zellij_menu_coldstart(mode: OwnedMenuMode) -> io::Result<()> {
    let target_bin = validate_installation(&input_path("MUXE_TARGET_INSTALLATION")).await;
    let herdr_binary = input_path("MUXE_HERDR_BINARY");
    let zellij_binary = input_path("MUXE_ZELLIJ_BINARY");
    let foreground = input_path("MUXE_ZELLIJ_FOREGROUND_BINARY");
    let bootstrap = input_path("MUXE_ZELLIJ_BOOTSTRAP_BINARY");
    let seeder = input_path("MUXE_ZELLIJ_PERMISSION_SEEDER");
    let session = "cold";
    let mut rig = bring_hosts(
        mode.case(),
        &target_bin,
        &target_bin,
        &herdr_binary,
        &zellij_binary,
        &foreground,
        &bootstrap,
        &seeder,
        &[(session, 1)],
        Some(&target_bin),
    )
    .await?;
    {
        use std::io::Write as _;
        let mut config = std::fs::OpenOptions::new()
            .append(true)
            .open(&rig.config_file)?;
        config.write_all(b"settings: { timeout: off }\n")?;
    }
    let endpoint = muxe_broker::RuntimeEndpoint::in_runtime_dir(
        rig.scoped_root.join("runtime"),
        muxe_protocol::HostKind::Zellij,
        session,
    )
    .map_err(|error| io::Error::other(error.to_string()))?
    .socket()
    .to_path_buf();
    let result = async {
        if endpoint.exists() {
            return Err(io::Error::other(
                "fresh menu host unexpectedly has a broker",
            ));
        }
        let host = rig
            .zellij
            .as_ref()
            .ok_or_else(|| io::Error::other("owned host is gone"))?;
        let mut inventory = tokio::process::Command::new(&zellij_binary);
        inventory.args(["--session", session, "action", "list-clients"]);
        host.apply_host_scoped_env(&mut inventory, &rig.scoped_root);
        inventory.current_dir(&rig.workdir);
        let observed = run_cli_bounded("menu-origin-inventory", &mut inventory).await?;
        eprintln!(
            "[cold] attached client inventory before menu input: status={:?}, {}",
            observed.status,
            String::from_utf8_lossy(&observed.stdout)
        );
        if String::from_utf8_lossy(&observed.stdout).contains("zellij:about") {
            // The pinned first-client bootstrap opens the host-owned About
            // overlay. Its own Esc binding closes it; only a subsequently
            // observed terminal pane may act as the public menu origin.
            rig.pty_clients
                .first_mut()
                .ok_or_else(|| io::Error::other("owned interactive client is gone"))?
                .send_input(b"\x1b")
                .await?;
        }
        let origin = poll_until(
            "attached shell pane after host overlay",
            std::time::Duration::from_secs(5),
            || async {
                let mut inventory = tokio::process::Command::new(&zellij_binary);
                inventory.args(["--session", session, "action", "list-clients"]);
                host.apply_host_scoped_env(&mut inventory, &rig.scoped_root);
                inventory.current_dir(&rig.workdir);
                let output = run_cli_bounded("menu-shell-inventory", &mut inventory)
                    .await
                    .map_err(|error| error.to_string())?;
                let text = String::from_utf8_lossy(&output.stdout);
                let mut rows = text.lines().skip(1);
                let row = rows
                    .next()
                    .ok_or_else(|| format!("no attached client: {text}"))?;
                let pane = row.split_whitespace().nth(1).unwrap_or("");
                if !output.status.success()
                    || rows.next().is_some()
                    || !pane.starts_with("terminal_")
                {
                    return Err(format!("client has no unique focused shell pane: {text}"));
                }
                Ok(text.into_owned())
            },
        )
        .await?;
        eprintln!("[cold] real menu origin: {origin}");
        // Let the bridge see another host census with focus on the actual
        // terminal. The just-dismissed About pane is not a valid menu origin.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        if matches!(mode, OwnedMenuMode::Locked) {
            // The real client enters Locked through the pinned Ctrl-g binding.
            rig.pty_clients
                .first_mut()
                .ok_or_else(|| io::Error::other("owned interactive client is gone"))?
                .send_input(b"\x07")
                .await?;
        }
        // The real attached client triggers the documented native Run
        // keybinding. This is not an external CLI pretending to own a client.
        rig.pty_clients
            .first_mut()
            .ok_or_else(|| io::Error::other("owned interactive client is gone"))?
            .send_input(b"\x1bm")
            .await?;
        if let Err(error) = poll_until(
            "menu-created ordinary Zellij endpoint",
            std::time::Duration::from_secs(15),
            || {
                let bound = endpoint.exists();
                async move {
                    if bound {
                        Ok(())
                    } else {
                        Err("no broker endpoint yet".to_owned())
                    }
                }
            },
        )
        .await
        {
            let mut dump = tokio::process::Command::new(&zellij_binary);
            dump.args([
                "--session",
                session,
                "action",
                "dump-screen",
                "--pane-id",
                "terminal_1",
            ]);
            host.apply_host_scoped_env(&mut dump, &rig.scoped_root);
            dump.current_dir(&rig.workdir);
            let output = run_cli_bounded("menu-ui-screen", &mut dump).await?;
            return Err(io::Error::other(format!(
                "{error}; UI pane screen: {}",
                String::from_utf8_lossy(&output.stdout)
            )));
        }
        assert_broker_serving(&endpoint, session, &installed_version(&target_bin).await?).await?;
        let record = muxe::compatibility::embedded_record().map_err(io::Error::other)?;
        poll_until("ordinary menu initial coverage", READY_TIMEOUT, || {
            ordinary_zellij_client_coverage(&endpoint, host, session, &record.handoff)
        })
        .await?;
        poll_until("attached public menu pane", READY_TIMEOUT, || async {
            assert_live_zellij_menu_pane(
                host,
                &zellij_binary,
                &rig.scoped_root,
                &rig.workdir,
                session,
                &target_bin,
            )
            .await
            .map_err(|error| error.to_string())
        })
        .await?;
        tokio::time::sleep(
            muxe_adapter_zellij::HEARTBEAT_LEASE + std::time::Duration::from_secs(8),
        )
        .await;
        ordinary_zellij_client_coverage(&endpoint, host, session, &record.handoff)
            .await
            .map_err(io::Error::other)?;
        assert_live_zellij_menu_pane(
            host,
            &zellij_binary,
            &rig.scoped_root,
            &rig.workdir,
            session,
            &target_bin,
        )
        .await?;
        // Dismiss through the real client, then prove the ordinary broker
        // survives the UI pane process and still serves its exact record.
        rig.pty_clients
            .first_mut()
            .ok_or_else(|| io::Error::other("owned interactive client is gone"))?
            .send_input(b"\x1b")
            .await?;
        poll_until(
            "owned menu dismissal",
            std::time::Duration::from_secs(8),
            || async {
                let mut panes = tokio::process::Command::new(&zellij_binary);
                panes.args([
                    "--session",
                    session,
                    "action",
                    "list-panes",
                    "--all",
                    "--json",
                ]);
                host.apply_host_scoped_env(&mut panes, &rig.scoped_root);
                panes.current_dir(&rig.workdir);
                let output = run_cli_bounded("menu-dismissed-panes", &mut panes)
                    .await
                    .map_err(|error| error.to_string())?;
                let panes: Vec<serde_json::Value> =
                    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())?;
                if panes.iter().any(|pane| {
                    pane.get("terminal_command")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|command| command.contains(" ui menu main"))
                        && pane.get("exited").and_then(serde_json::Value::as_bool) == Some(false)
                }) {
                    Err("the menu UI process still owns its pane".to_owned())
                } else {
                    Ok(())
                }
            },
        )
        .await?;
        assert_broker_serving(&endpoint, session, &installed_version(&target_bin).await?).await?;
        // Closing the UI pane precedes the broker's async EndCapture host
        // action; let that action settle before probing the restored mode.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        rig.pty_clients
            .first_mut()
            .ok_or_else(|| io::Error::other("owned interactive client is gone"))?
            .send_input(b"\x1bx")
            .await?;
        let (expected_marker, foreign_marker) = mode.markers(&rig.scoped_root);
        poll_until(
            "restored host input mode",
            std::time::Duration::from_secs(5),
            || {
                let expected = expected_marker.exists();
                let foreign = foreign_marker.exists();
                async move {
                    if expected && !foreign {
                        Ok(())
                    } else {
                        Err(format!(
                            "expected marker={expected}, foreign marker={foreign}"
                        ))
                    }
                }
            },
        )
        .await?;
        Ok(())
    }
    .await;
    let retire = if endpoint.exists() {
        retire_broker(&endpoint, session).await
    } else {
        Ok(())
    };
    rig.finish(session, combine_body_and_cleanup(result, retire))
        .await
}

#[tokio::test]
#[ignore = "live pinned hosts, staged installation, and explicit host approval required"]
async fn zellij_public_menu_coldstart_retains_live_client_past_lease() {
    require_live_approval();
    if let Err(error) = run_zellij_menu_coldstart(OwnedMenuMode::Normal).await {
        panic!("fresh Zellij menu coldstart failed: {error}");
    }
}

#[tokio::test]
#[ignore = "live pinned hosts, staged installation, and explicit host approval required"]
async fn zellij_locked_menu_coldstart_restores_locked_mode() {
    require_live_approval();
    if let Err(error) = run_zellij_menu_coldstart(OwnedMenuMode::Locked).await {
        panic!("fresh Locked-mode Zellij menu coldstart failed: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CensusSequence {
        results: tokio::sync::Mutex<
            std::collections::VecDeque<
                Result<Vec<muxe_core::ClientId>, muxe_adapter_api::AdapterError>,
            >,
        >,
    }

    #[async_trait::async_trait]
    impl MembershipSource for CensusSequence {
        async fn snapshot_members(
            &self,
        ) -> Result<Vec<muxe_core::ClientId>, muxe_adapter_api::AdapterError> {
            self.results.lock().await.pop_front().expect("census query")
        }
    }
    struct PendingCensus;
    #[async_trait::async_trait]
    impl MembershipSource for PendingCensus {
        async fn snapshot_members(
            &self,
        ) -> Result<Vec<muxe_core::ClientId>, muxe_adapter_api::AdapterError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn admission_retries_empty_census_but_rejects_changed_membership() {
        let anchor = vec![muxe_core::ClientId::new("1"), muxe_core::ClientId::new("2")];
        let membership = CensusSequence {
            results: tokio::sync::Mutex::new(std::collections::VecDeque::from([
                Err(muxe_adapter_api::AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Unavailable,
                    "temporary discovery failure",
                )),
                Ok(anchor.clone()),
                Ok(vec![
                    muxe_core::ClientId::new("1"),
                    muxe_core::ClientId::new("3"),
                ]),
            ])),
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        assert!(
            !duo_recheck_census(&anchor, &membership, deadline)
                .await
                .unwrap()
        );
        assert!(
            duo_recheck_census(&anchor, &membership, deadline)
                .await
                .unwrap()
        );
        assert!(
            duo_recheck_census(&anchor, &membership, deadline)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn admission_rejects_permanent_errors_and_keeps_original_deadline() {
        let anchor = vec![muxe_core::ClientId::new("1"), muxe_core::ClientId::new("2")];
        let permanent_kinds = [
            muxe_adapter_api::AdapterErrorKind::InvalidRequest,
            muxe_adapter_api::AdapterErrorKind::ContextUnavailable,
        ];
        let membership = CensusSequence {
            results: tokio::sync::Mutex::new(std::collections::VecDeque::from([
                Err(muxe_adapter_api::AdapterError::new(
                    permanent_kinds[0],
                    "permanent failure",
                )),
                Err(muxe_adapter_api::AdapterError::new(
                    permanent_kinds[1],
                    "permanent failure",
                )),
            ])),
        };
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        for kind in permanent_kinds {
            let error = duo_recheck_census(&anchor, &membership, deadline)
                .await
                .unwrap_err();
            assert_eq!(
                error
                    .get_ref()
                    .and_then(|source| source.downcast_ref::<muxe_adapter_api::AdapterError>())
                    .expect("preserved typed oracle error")
                    .kind,
                kind,
            );
        }
        assert_eq!(
            duo_recheck_census(&anchor, &PendingCensus, tokio::time::Instant::now())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut,
        );
    }

    #[test]
    fn finish_preserves_body_and_cleanup_errors() {
        let error = combine_body_and_cleanup(
            Err(io::Error::other("body failure")),
            Err(io::Error::other("teardown failure")),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("body failure"));
        assert!(error.contains("teardown failure"));
    }

    #[test]
    fn routed_release_rejects_pre_recovery_generation() {
        let registration = RegistrationId::from_random_bytes([1; 16]).expect("registration");
        let generation = ChannelGeneration::try_from(2_u64).expect("recovered generation");
        let anchors = [muxe_core::ClientId::new("1"), muxe_core::ClientId::new("2")];
        let mut state = DuoRouteState {
            anchor: &anchors,
            request_id: RequestId::INITIAL,
            registration,
            generation,
            ui_session: "duo-route-1",
            current_pane: "plugin_3",
            client_id: &anchors[0],
            round_deadline: tokio::time::Instant::now() + DUO_ROUTE_TIMEOUT,
            released: false,
            snapshot: false,
        };
        let mut release = PipeEvent {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id: Some(RequestId::INITIAL),
            registration,
            channel_generation: ChannelGeneration::INITIAL,
            event: PipeEventKind::Response(BridgeResponse::RequestReleased),
        };
        assert!(state.apply(release.clone()).is_err());
        assert!(!state.released);
        release.channel_generation = generation;
        state.apply(release).expect("current-generation release");
        assert!(state.released);
    }
}
