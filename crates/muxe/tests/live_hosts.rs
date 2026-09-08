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

use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use muxe_protocol::control::CompatibilityRecord;
use sha2::{Digest, Sha256};
use support::{
    ContinuityGuard, OwnedChild, OwnedHerdrServer, OwnedZellijHost, ServedBroker,
    assert_broker_serving, assert_no_preserved_journals, await_activate, await_session_ready,
    await_target_ready, drive_activate, init_shared_dirs, input_path, install_zellij_integration,
    installed_version, installed_wasm_digest, poll_until, read_broker_record, retire_broker,
    short_tempdir, spawn_activate, spawn_serve_herdr, spawn_serve_zellij, validate_installation,
};

/// Bounded wait for one barrier file to appear.
const BARRIER_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(3);
/// Bounded wait for one transfer target to report ready.
const READY_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(3);

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

fn combine_body_and_cleanup(
    body: io::Result<()>,
    cleanup: io::Result<()>,
) -> io::Result<()> {
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
    reason = "rig builder threads every explicit typed input (case label, init plus install binaries, five pinned binaries/dirs, session table) with absolute paths and no command hook; bundling would hide the typed-input surface the live gate documents"
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
) -> io::Result<Rig> {
    let root = short_tempdir(&format!("muxe-live-{case}-"))?;
    let scoped_root = root.path().join("scoped");
    let (config_file, cache_dir) = init_shared_dirs(init_bin, &scoped_root).await?;

    let mut herdr = OwnedHerdrServer::start(herdr_binary, root.path(), case).await?;
    let discovery = herdr.discovery_key().to_owned();
    if herdr.try_wait()?.is_some() {
        return Err(io::Error::other(format!(
            "{case}: herdr server exited on startup"
        )));
    }

    let mut host = OwnedZellijHost::prepare(zellij_binary, root.path(), case)?;
    // Receipt-owned pre-state before any startup: the real public
    // install writes stable bytes, receipt, and autoload KDL nodes, so
    // foreground startup loads the managed bridge and activation
    // preflight finds receipt-owned bytes. `install_bin` selects the
    // pre-state generation (old for upgrade rehearsal, target for smoke).
    install_zellij_integration(case, install_bin, &host, &scoped_root).await?;
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
    for (session, _) in sessions {
        host.serve_foreground(foreground, bootstrap, session, &scoped_root)
            .await?;
    }
    if !host.has_server_child() {
        return Err(io::Error::other(format!(
            "{case}: zellij server children missing right after spawn"
        )));
    }
    host.check_servers_alive()?;

    let workdir = host.workdir().to_path_buf();
    let mut clients = Vec::new();
    for (session, count) in sessions {
        for index in 0..*count {
            let typescript = workdir.join(format!("client-{session}-{index}.log"));
            clients.push(
                host.spawn_client(&format!("{case}-{session}-{index}"), session, &typescript)
                    .await?,
            );
        }
    }

    let continuity = ContinuityGuard::watch_herdr(
        &format!("{case}-herdr"),
        herdr.socket().to_path_buf(),
        discovery.clone(),
    )
    .await?;
    Ok(Rig {
        root,
        scoped_root,
        workdir,
        config_file,
        cache_dir,
        herdr: Some(herdr),
        discovery,
        zellij: Some(host),
        pty_clients: clients,
        brokers: Vec::new(),
        continuity: Some(continuity),
    })
}

/// Serves old brokers on every host through the installed binary and
/// returns the Herdr endpoint plus one endpoint per session, in order.
/// Pre-state fixture only: these direct serves stand in for a previously
/// activated old stack (no time travel available). They prove nothing
/// about cold start. The lifecycle under proof is always the public
/// `muxe activate` path (`transfer_to`/`drive_activate`); on-demand
/// broker startup inside activation is core-owned consumer work the
/// runner never bypasses or simulates.
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
        let report =
            drive_activate(&target_bin, &rig.scoped_root, rig.zellij.as_ref(), "smoke").await?;
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
        Ok(())
    }
    .await;
    rig.finish("smoke", result).await
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
    let report = drive_activate(to_bin, &rig.scoped_root, rig.zellij.as_ref(), case).await?;
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
            rig.zellij.as_ref(),
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
#[cfg(test)]
mod tests {
    use super::*;

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
}
