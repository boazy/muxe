//! Real persistent-plugin command panes on a retained, isolated Herdr server.
//! Requires `MUXE_LIVE_HOSTS_APPROVED=true`, `MUXE_HERDR_BINARY` and a complete
//! `MUXE_TARGET_INSTALLATION`. Put that installation first on `PATH` for the host.
use super::support;

use muxe_adapter_api::{
    AdapterHealthEvent, DispatchCompletion, HostAdapter, OriginCaptureRequest, OriginHintSource,
    PostDismissalPortableDispatchRequest, UiSessionId, UntrustedOriginHint,
};
use muxe_adapter_herdr::{HerdrAdapter, HerdrAdapterConfig, HerdrResponse, HerdrRuntime};
use muxe_core::{
    ActionSpec, CompiledGeneration, ExecutionId, KeyCapabilities, MenuId, MenuName, SourceId,
};
use serde_json::{Value, json};
use std::{
    io,
    path::{Path, PathBuf},
    time::Duration,
};

async fn invoke(runtime: &HerdrRuntime, method: &str, params: Value) -> io::Result<Value> {
    match runtime
        .invoke_response(method, params)
        .await
        .map_err(io::Error::other)?
    {
        HerdrResponse::Success(value) => Ok(value),
        HerdrResponse::Error { code, message } => {
            Err(io::Error::other(format!("{method}: {code}: {message}")))
        }
    }
}

async fn marker(path: &Path) -> io::Result<String> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match tokio::fs::read_to_string(path).await {
                Ok(value) => return Ok(value),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|_| io::Error::other(format!("command marker never appeared: {}", path.display())))?
}

const PLUGIN_ID: &str = "muxe.command-panes";
const UNRELATED_PLUGIN_ID: &str = "owned.muxe-isolation-proof";
const SHIPPED_MANIFEST: &str = include_str!("../../../../lib/muxe/herdr-plugin.toml");

#[derive(Debug, PartialEq, Eq)]
struct FileInode(u64);

#[derive(Debug, PartialEq, Eq)]
pub(super) struct OwnedFileSnapshot {
    contents: Vec<u8>,
    modified: std::time::SystemTime,
    inode: FileInode,
    mode: u32,
    owner: nix::unistd::Uid,
    group: nix::unistd::Gid,
}

fn file_snapshot(path: &Path) -> io::Result<OwnedFileSnapshot> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::other(
            "owned plugin fixture is not a regular file",
        ));
    }
    Ok(OwnedFileSnapshot {
        contents: std::fs::read(path)?,
        modified: metadata.modified()?,
        inode: FileInode(metadata.ino()),
        mode: metadata.permissions().mode(),
        owner: nix::unistd::Uid::from_raw(metadata.uid()),
        group: nix::unistd::Gid::from_raw(metadata.gid()),
    })
}

pub(super) fn registry_snapshot(scoped: &Path) -> io::Result<OwnedFileSnapshot> {
    file_snapshot(&scoped.join("config/herdr/plugins.json"))
}

async fn plugin_entry(runtime: &HerdrRuntime, plugin_id: &str) -> io::Result<Option<Value>> {
    let reply = invoke(runtime, "plugin.list", json!({"plugin_id":plugin_id})).await?;
    let plugins = reply["plugins"]
        .as_array()
        .ok_or_else(|| io::Error::other(format!("plugin.list has no plugins: {reply}")))?;
    let mut matches = plugins
        .iter()
        .filter(|plugin| plugin["plugin_id"] == plugin_id);
    let entry = matches.next().cloned();
    if matches.next().is_some() {
        return Err(io::Error::other("duplicate owned plugin registry entry"));
    }
    Ok(entry)
}

pub(super) async fn assert_plugin_missing(runtime: &HerdrRuntime) -> io::Result<()> {
    if plugin_entry(runtime, PLUGIN_ID).await?.is_some() {
        return Err(io::Error::other(
            "fresh owned host already has the Muxe plugin",
        ));
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct ManagedPluginSnapshot {
    entry: Value,
    manifest: OwnedFileSnapshot,
}

pub(super) async fn managed_plugin_snapshot(
    runtime: &HerdrRuntime,
    cache_dir: &Path,
) -> io::Result<ManagedPluginSnapshot> {
    let path = cache_dir.join("herdr/herdr-plugin.toml");
    let manifest = file_snapshot(&path)?;
    let canonical = std::fs::canonicalize(&path)?;
    let entry = plugin_entry(runtime, PLUGIN_ID).await?.ok_or_else(|| {
        io::Error::other("broker did not install the missing command pane plugin")
    })?;
    let expected_version = SHIPPED_MANIFEST
        .lines()
        .find_map(|line| {
            line.strip_prefix("version = \"")
                .and_then(|value| value.strip_suffix('"'))
        })
        .ok_or_else(|| io::Error::other("shipped plugin version absent"))?;
    if manifest.contents != SHIPPED_MANIFEST.as_bytes()
        || manifest.mode & 0o777 != 0o600
        || entry["enabled"] != true
        || entry["manifest_path"] != json!(canonical)
        || entry["plugin_root"] != json!(canonical.parent())
        || entry["version"] != expected_version
        || entry["panes"]
            != json!([{
                "id":"command", "title":"Muxe command", "placement":"popup",
                "command":["muxe","pane","exec"]
            }])
        || entry
            .get("warnings")
            .is_some_and(|warnings| warnings != &json!([]))
    {
        return Err(io::Error::other(format!(
            "managed command pane plugin is not repaired: {entry}"
        )));
    }
    Ok(ManagedPluginSnapshot { entry, manifest })
}

pub(super) struct UnrelatedPluginSnapshot {
    path: PathBuf,
    entry: Value,
    manifest: OwnedFileSnapshot,
}

impl UnrelatedPluginSnapshot {
    pub(super) async fn install(runtime: &HerdrRuntime, root: &Path) -> io::Result<Self> {
        let path = root.join("unrelated-plugin/herdr-plugin.toml");
        std::fs::create_dir_all(path.parent().unwrap())?;
        let contents = SHIPPED_MANIFEST
            .replace(PLUGIN_ID, UNRELATED_PLUGIN_ID)
            .replace("[\"muxe\", \"pane\", \"exec\"]", "[\"/usr/bin/false\"]");
        std::fs::write(&path, contents)?;
        invoke(runtime, "plugin.link", json!({"path":path,"enabled":true})).await?;
        let entry = plugin_entry(runtime, UNRELATED_PLUGIN_ID)
            .await?
            .ok_or_else(|| io::Error::other("unrelated owned fixture was not registered"))?;
        let manifest = file_snapshot(&path)?;
        Ok(Self {
            path,
            entry,
            manifest,
        })
    }

    pub(super) async fn assert_unchanged(&self, runtime: &HerdrRuntime) -> io::Result<()> {
        if plugin_entry(runtime, UNRELATED_PLUGIN_ID).await?.as_ref() != Some(&self.entry)
            || file_snapshot(&self.path)? != self.manifest
        {
            return Err(io::Error::other(
                "Muxe repair changed the unrelated owned plugin",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum PluginDamage {
    Missing,
    DeletedOldManifest,
    Disabled,
    WrongCommand,
    WrongEntrypoint,
    WrongVersion,
    WrongManagedContents,
}

pub(super) const PLUGIN_DAMAGE_CASES: [PluginDamage; 7] = [
    PluginDamage::Missing,
    PluginDamage::DeletedOldManifest,
    PluginDamage::Disabled,
    PluginDamage::WrongCommand,
    PluginDamage::WrongEntrypoint,
    PluginDamage::WrongVersion,
    PluginDamage::WrongManagedContents,
];

pub(super) async fn damage_plugin(
    runtime: &HerdrRuntime,
    root: &Path,
    cache_dir: &Path,
    damage: PluginDamage,
) -> io::Result<()> {
    if matches!(damage, PluginDamage::Missing) {
        invoke(runtime, "plugin.unlink", json!({"plugin_id":PLUGIN_ID})).await?;
        return Ok(());
    }
    if matches!(damage, PluginDamage::Disabled) {
        invoke(runtime, "plugin.disable", json!({"plugin_id":PLUGIN_ID})).await?;
        return Ok(());
    }
    if matches!(damage, PluginDamage::WrongManagedContents) {
        std::fs::write(
            cache_dir.join("herdr/herdr-plugin.toml"),
            "invalid managed manifest fixture\n",
        )?;
        return Ok(());
    }
    let directory = root.join(format!("bad-muxe-plugin-{damage:?}"));
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("herdr-plugin.toml");
    let contents = match damage {
        PluginDamage::WrongCommand => {
            SHIPPED_MANIFEST.replace("[\"muxe\", \"pane\", \"exec\"]", "[\"/usr/bin/false\"]")
        }
        PluginDamage::WrongEntrypoint => {
            SHIPPED_MANIFEST.replace("id = \"command\"", "id = \"wrong-command\"")
        }
        PluginDamage::WrongVersion => {
            SHIPPED_MANIFEST.replace("version = \"0.1.0\"", "version = \"0.0.0\"")
        }
        PluginDamage::DeletedOldManifest => SHIPPED_MANIFEST.to_owned(),
        PluginDamage::Missing | PluginDamage::Disabled | PluginDamage::WrongManagedContents => {
            unreachable!()
        }
    };
    std::fs::write(&path, contents)?;
    // These are deliberate corruptions of ONLY the exact Muxe ID, never prerequisites.
    invoke(runtime, "plugin.link", json!({"path":path,"enabled":true})).await?;
    if matches!(damage, PluginDamage::DeletedOldManifest) {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

fn resolved_executable_action(
    adapter: &HerdrAdapter,
    pane_type: &str,
    path: &Path,
    origin: &muxe_core::OriginContext,
) -> io::Result<muxe_core::ResolvedPortableAction> {
    let script = "test -z \"${MUXE_COMMAND_PANE_ARGV+x}\" || exit 91; printf '%s|%s|%s|%s|%s|%s' \"$PWD\" \"$2\" \"$3\" \"$4\" \"$5\" \"$HERDR_PLUGIN_ID\" > \"$1\"; printf 'real-command-output'; sleep 120";
    let yaml = serde_json::to_string(&json!({"version":1,"menus":{"main":{"bindings":{"p":{
        "label":"Executable", "action":{"type":"pane:open","pane-type":pane_type,
        "program":"/bin/sh","args":["-c",script,"proof",path.to_str().unwrap(),"","a b","$HOME","; touch nope"]}
    }}}}})).map_err(io::Error::other)?;
    let compiled = muxe_core::compile_yaml(
        CompiledGeneration(1),
        SourceId::new("owned-pane-action"),
        yaml,
        KeyCapabilities::default(),
        Some(adapter),
    )
    .map_err(|errors| io::Error::other(format!("{errors:?}")))?;
    let menu = compiled
        .menu(&MenuId::named(MenuName::parse("main").unwrap()))
        .unwrap();
    let ActionSpec::Portable(action) = &menu.bindings[0].action else {
        panic!("portable action expected")
    };
    action
        .resolve_context(origin)
        .map_err(|error| io::Error::other(format!("{error:?}")))
}

async fn wait_for_dispatch_completion(
    adapter: &HerdrAdapter,
    execution: ExecutionId,
) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let AdapterHealthEvent::DispatchCompleted(completion) = adapter
                .next_health_event()
                .await
                .map_err(io::Error::other)?
            {
                match completion {
                    DispatchCompletion::Succeeded { execution: id } if id == execution => {
                        return Ok(());
                    }
                    DispatchCompletion::Failed { error, .. }
                    | DispatchCompletion::OutcomeUnknown { error, .. } => {
                        return Err(io::Error::other(format!(
                            "command creation failed: {error}"
                        )));
                    }
                    DispatchCompletion::Succeeded { .. } => {}
                }
            }
        }
    })
    .await
    .map_err(|_| io::Error::other("action completion timed out"))?
}

async fn wait_for_overlay_output(runtime: &HerdrRuntime, pane_id: &str) -> io::Result<()> {
    let mut last_reply = None;
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            // Check the viewport; Herdr 0.8.2's recent tail can omit visible output.
            let reply = invoke(
                runtime,
                "pane.read",
                json!({"pane_id":pane_id,"source":"visible","lines":20}),
            )
            .await?;
            let text = reply
                .pointer("/read/text")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    io::Error::other(format!(
                        "pane.read response has no string /read/text: {reply}"
                    ))
                })?;
            if text.contains("real-command-output") {
                return Ok(());
            }
            last_reply = Some(reply);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(_) => Err(io::Error::other(format!(
            "missing real overlay terminal output: {}",
            last_reply.map_or_else(
                || "<no pane.read reply>".to_owned(),
                |reply| reply.to_string()
            )
        ))),
    }
}

async fn close_executable_pane(
    runtime: &HerdrRuntime,
    origin: &muxe_adapter_herdr::FocusedPane,
    pane_type: &str,
) -> io::Result<()> {
    if pane_type == "popup" {
        invoke(runtime, "popup.close", json!({})).await?;
        return Ok(());
    }
    let snapshot = invoke(runtime, "session.snapshot", json!({})).await?;
    let panes = snapshot
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("panes"))
        .and_then(Value::as_array)
        .ok_or_else(|| io::Error::other(format!("snapshot has no panes: {snapshot}")))?;
    let overlay = panes
        .iter()
        .filter_map(|pane| pane.get("id").or_else(|| pane.get("pane_id")))
        .filter_map(Value::as_str)
        .find(|id| *id != origin.pane.as_str())
        .ok_or_else(|| io::Error::other(format!("no overlay pane: {snapshot}")))?;
    wait_for_overlay_output(runtime, overlay).await?;
    invoke(runtime, "pane.close", json!({"pane_id":overlay})).await?;
    Ok(())
}

async fn exercise_generic_pane(
    runtime: &HerdrRuntime,
    adapter: &HerdrAdapter,
    origin: &muxe_adapter_herdr::FocusedPane,
    root: &Path,
    pane_type: &str,
    execution: ExecutionId,
) -> io::Result<()> {
    let path = root.join(format!("{pane_type}.marker"));
    // A real host terminal stands in for the menu terminal; broker ordering is
    // covered separately. No host request may execute before this pane closes.
    let ui = muxe_adapter_herdr::open_command_pane(
        runtime,
        muxe_adapter_herdr::CommandPaneLaunch {
            origin: origin.clone(),
            destination: origin.clone(),
            cwd: root.into(),
            argv: vec!["/bin/sleep".into(), "120".into()],
            direction: muxe_adapter_herdr::UiSplitDirection::Down,
            ratio: 0.5,
            focus: true,
        },
    )
    .await
    .map_err(io::Error::other)?;
    let captured = adapter
        .capture_origin(OriginCaptureRequest {
            ui_session: UiSessionId::new(format!("owned-{pane_type}")),
            ui_pane: ui.pane.clone(),
            origin_hint: Some(UntrustedOriginHint {
                workspace_id: origin.workspace.clone(),
                tab_id: origin.tab.clone(),
                pane_id: origin.pane.clone(),
                cwd: Some(origin.cwd.clone()),
                source: OriginHintSource::LauncherBootstrap,
            }),
            caller_identity: Some(muxe_adapter_api::HostCallerIdentity {
                workspace_id: origin.workspace.clone(),
                tab_id: origin.tab.clone(),
                pane_id: ui.pane.clone(),
                cwd: Some(root.into()),
            }),
        })
        .await
        .map_err(io::Error::other)?;
    let action = resolved_executable_action(adapter, pane_type, &path, &captured)?;
    adapter
        .dispatch_portable_after_ui_dismissal(PostDismissalPortableDispatchRequest {
            execution,
            action,
            origin: captured,
            ui_pane: ui.pane.clone(),
        })
        .await
        .map_err(io::Error::other)?;
    if path.exists() {
        return Err(io::Error::other(
            "command ran before menu terminal dismissal",
        ));
    }
    invoke(runtime, "pane.close", json!({"pane_id":ui.pane.as_str()})).await?;
    wait_for_dispatch_completion(adapter, execution).await?;
    let actual = marker(&path).await?;
    let expected = format!(
        "{}||a b|$HOME|; touch nope|muxe.command-panes",
        origin.cwd.display()
    );
    if actual != expected {
        return Err(io::Error::other(format!(
            "exact argv/cwd/transport proof differs: expected {expected:?}, actual {actual:?}"
        )));
    }
    eprintln!("{pane_type}: {actual}");
    close_executable_pane(runtime, origin, pane_type).await?;
    std::fs::remove_file(path)
}

async fn exercise_installed_cli(
    installation: &Path,
    scoped: &Path,
    herdr_binary: &Path,
    socket: &Path,
    origin: &muxe_adapter_herdr::FocusedPane,
    root: &Path,
    runtime: &HerdrRuntime,
) -> io::Result<()> {
    // The packaged executable and public CLI reuse the same real plugin path.
    let path = root.join("cli-popup.marker");
    let mut cli = tokio::process::Command::new(installation.join("muxe"));
    support::apply_scoped_env(&mut cli, scoped);
    cli.env("HERDR_SOCKET_PATH", socket)
        .env("HERDR_BIN_PATH", herdr_binary)
        .env("HERDR_ACTIVE_WORKSPACE_ID", origin.workspace.as_str())
        .env("HERDR_ACTIVE_TAB_ID", origin.tab.as_str())
        .env("HERDR_ACTIVE_PANE_ID", origin.pane.as_str())
        .args([
            "pane",
            "open",
            "--host",
            "herdr",
            "--pane-type",
            "popup",
            "--width",
            "80%",
            "--height",
            "12",
            "--",
            "/bin/sh",
            "-c",
            "pwd > \"$1\"; sleep 120",
            "owned-cli-proof",
        ])
        .arg(&path);
    let output = support::run_cli_bounded("owned-cli-popup", &mut cli).await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "CLI popup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    support::poll_until(
        "CLI executable retains the saved cwd",
        Duration::from_secs(15),
        async || {
            let actual = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
            if actual.ends_with('\n') && Path::new(actual.trim()) == origin.cwd {
                Ok(())
            } else {
                Err("waiting for exact CLI cwd proof".to_owned())
            }
        },
    )
    .await?;
    invoke(runtime, "popup.close", json!({})).await?;
    std::fs::remove_file(path)
}

async fn exercise_noop_generic_panes(
    runtime: &HerdrRuntime,
    adapter: &HerdrAdapter,
    origin: &muxe_adapter_herdr::FocusedPane,
    root: &Path,
    scoped: &Path,
    cache_dir: &Path,
) -> io::Result<(ManagedPluginSnapshot, OwnedFileSnapshot)> {
    let correct = managed_plugin_snapshot(runtime, cache_dir).await?;
    let registry = registry_snapshot(scoped)?;
    for (ordinal, pane_type) in ["popup", "overlay"].into_iter().enumerate() {
        exercise_generic_pane(
            runtime,
            adapter,
            origin,
            root,
            pane_type,
            ExecutionId(10 + ordinal as u64),
        )
        .await?;
        if managed_plugin_snapshot(runtime, cache_dir).await? != correct
            || registry_snapshot(scoped)? != registry
        {
            return Err(io::Error::other(
                "already-correct plugin was rewritten or relinked",
            ));
        }
    }
    Ok((correct, registry))
}

async fn exercise_installed_plugin(
    herdr_binary: &Path,
    installation: &Path,
    root: &Path,
    scoped: &Path,
    socket: &Path,
) -> io::Result<()> {
    let config = HerdrAdapterConfig {
        socket_path: socket.to_path_buf(),
        herdr_binary: herdr_binary.to_path_buf(),
        cache_dir: scoped.join("cache/muxe"),
    };
    let runtime = HerdrRuntime::connect(config.clone())
        .await
        .map_err(io::Error::other)?;
    invoke(
        &runtime,
        "workspace.create",
        json!({"cwd": root, "env": {}, "focus": true, "label": "owned-executable-panes"}),
    )
    .await?;
    let origin = muxe_adapter_herdr::focused_pane(&runtime)
        .await
        .map_err(io::Error::other)?;
    assert_plugin_missing(&runtime).await?;
    let unrelated = UnrelatedPluginSnapshot::install(&runtime, root).await?;
    let cache_dir = config.cache_dir.clone();
    let adapter = HerdrAdapter::connect(config)
        .await
        .map_err(io::Error::other)?;
    let body = async {
        // Connecting must not register anything: startup authorization is a separate boundary.
        assert_plugin_missing(&runtime).await?;
        adapter
            .ensure_command_pane_plugin()
            .await
            .map_err(io::Error::other)?;
        unrelated.assert_unchanged(&runtime).await?;
        let (correct, registry) = exercise_noop_generic_panes(
            &runtime,
            adapter.as_ref(),
            &origin,
            root,
            scoped,
            &cache_dir,
        )
        .await?;
        exercise_installed_cli(
            installation,
            scoped,
            herdr_binary,
            socket,
            &origin,
            root,
            &runtime,
        )
        .await?;
        if managed_plugin_snapshot(&runtime, &cache_dir).await? != correct
            || registry_snapshot(scoped)? != registry
        {
            return Err(io::Error::other(
                "already-correct CLI plugin was rewritten or relinked",
            ));
        }
        unrelated.assert_unchanged(&runtime).await?;
        for (ordinal, damage) in PLUGIN_DAMAGE_CASES.into_iter().enumerate() {
            damage_plugin(&runtime, root, &cache_dir, damage).await?;
            exercise_generic_pane(
                &runtime,
                adapter.as_ref(),
                &origin,
                root,
                if ordinal % 2 == 0 { "popup" } else { "overlay" },
                ExecutionId(20 + ordinal as u64),
            )
            .await?;
            managed_plugin_snapshot(&runtime, &cache_dir).await?;
            unrelated.assert_unchanged(&runtime).await?;
            // Corrupt again: the standalone public CLI must repair independently of dispatch.
            damage_plugin(&runtime, root, &cache_dir, damage).await?;
            exercise_installed_cli(
                installation,
                scoped,
                herdr_binary,
                socket,
                &origin,
                root,
                &runtime,
            )
            .await?;
            managed_plugin_snapshot(&runtime, &cache_dir).await?;
            unrelated.assert_unchanged(&runtime).await?;
        }
        Ok(())
    }
    .await;
    let cleanup = adapter.shutdown().await.map_err(io::Error::other);
    super::combine_body_and_cleanup(body, cleanup)
}

#[tokio::test]
#[ignore = "requires explicitly approved owned Herdr and an installed Muxe layout"]
async fn installed_plugin_executes_generic_popup_overlay_and_cli() -> io::Result<()> {
    if std::env::var("MUXE_LIVE_HOSTS_APPROVED").as_deref() != Ok("true") {
        return Err(io::Error::other(
            "MUXE_LIVE_HOSTS_APPROVED=true is required",
        ));
    }
    let herdr_binary = support::input_path("MUXE_HERDR_BINARY");
    let installation = support::input_path("MUXE_TARGET_INSTALLATION");
    let root = support::short_tempdir("muxe-pane-exec-")?;
    let scoped = root.path().join("scoped");
    support::ensure_scoped_dirs(&scoped)?;
    let mut server =
        support::OwnedHerdrServer::start(&herdr_binary, root.path(), &scoped, "panes").await?;
    // Keep the owned server in this outer scope so every exercise error still
    // reaches shutdown and preserves diagnostics.
    let body = exercise_installed_plugin(
        &herdr_binary,
        &installation,
        root.path(),
        &scoped,
        server.socket(),
    )
    .await;
    let cleanup = server.shutdown().await.map(|diagnostics| {
        eprintln!(
            "owned Herdr reaped: {:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
            diagnostics.exit_status,
            diagnostics.stdout_tail.lossy(),
            diagnostics.stderr_tail.lossy()
        );
    });
    if body.is_err() || cleanup.is_err() {
        eprintln!("preserved owned diagnostics: {}", root.keep().display());
    }
    super::combine_body_and_cleanup(body, cleanup)
}
