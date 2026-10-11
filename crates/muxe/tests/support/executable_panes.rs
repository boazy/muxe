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
use std::{io, path::Path, time::Duration};

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

async fn ensure_command_pane_plugin(
    runtime: &HerdrRuntime,
    origin: &muxe_adapter_herdr::FocusedPane,
    herdr_binary: &Path,
    socket: &Path,
    scoped: &Path,
    manifest: &Path,
) -> io::Result<()> {
    let negative_launch = || muxe_adapter_herdr::PluginCommandPaneLaunch {
        origin: origin.clone(),
        cwd: origin.cwd.clone(),
        argv: vec!["/usr/bin/false".into()],
        pane_type: muxe_adapter_herdr::PluginCommandPaneType::Popup,
        width: None,
        height: None,
    };
    let error = muxe_adapter_herdr::open_plugin_command_pane(runtime, negative_launch())
        .await
        .err()
        .ok_or_else(|| io::Error::other("unlinked plugin unexpectedly opened a terminal"))?;
    if !error.to_string().contains("herdr plugin link") {
        return Err(io::Error::other(format!(
            "missing actionable install diagnostic: {error}"
        )));
    }
    // Native link targets only this retained child's explicit socket.
    let mut link = tokio::process::Command::new(herdr_binary);
    support::apply_scoped_env(&mut link, scoped);
    link.env("HERDR_SOCKET_PATH", socket)
        .args(["plugin", "link"])
        .arg(manifest);
    let output = support::run_cli_bounded("owned-plugin-link", &mut link).await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "plugin link failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    invoke(
        runtime,
        "plugin.disable",
        json!({"plugin_id":"muxe.command-panes"}),
    )
    .await?;
    let error = muxe_adapter_herdr::open_plugin_command_pane(runtime, negative_launch())
        .await
        .err()
        .ok_or_else(|| io::Error::other("disabled plugin unexpectedly opened a terminal"))?;
    if !error.to_string().contains("plugin_disabled")
        || !error.to_string().contains("herdr plugin enable")
    {
        return Err(io::Error::other(format!(
            "missing actionable disabled-plugin diagnostic: {error}"
        )));
    }
    invoke(
        runtime,
        "plugin.enable",
        json!({"plugin_id":"muxe.command-panes"}),
    )
    .await?;
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
            let reply = invoke(
                runtime,
                "pane.read",
                json!({"pane_id":pane_id,"source":"recent","lines":20}),
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
    close_executable_pane(runtime, origin, pane_type).await
}

async fn exercise_installed_cli(
    installation: &Path,
    scoped: &Path,
    herdr_binary: &Path,
    socket: &Path,
    origin: &muxe_adapter_herdr::FocusedPane,
    root: &Path,
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
            "/usr/bin/touch",
        ])
        .arg(&path);
    let output = support::run_cli_bounded("owned-cli-popup", &mut cli).await?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "CLI popup failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("CLI executable did not run"))?;
    Ok(())
}

async fn exercise_installed_plugin(
    herdr_binary: &Path,
    installation: &Path,
    root: &Path,
    scoped: &Path,
    socket: &Path,
    manifest: &Path,
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
    ensure_command_pane_plugin(&runtime, &origin, herdr_binary, socket, scoped, manifest).await?;
    let adapter = HerdrAdapter::connect(config)
        .await
        .map_err(io::Error::other)?;
    for (ordinal, pane_type) in ["popup", "overlay"].into_iter().enumerate() {
        exercise_generic_pane(
            &runtime,
            adapter.as_ref(),
            &origin,
            root,
            pane_type,
            ExecutionId(10 + ordinal as u64),
        )
        .await?;
    }
    exercise_installed_cli(installation, scoped, herdr_binary, socket, &origin, root).await?;
    adapter.shutdown().await.map_err(io::Error::other)
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
    let manifest = installation.join("lib/muxe/herdr-plugin.toml");
    let shipped = include_str!("../../../../lib/muxe/herdr-plugin.toml");
    if std::fs::read_to_string(&manifest)? != shipped {
        return Err(io::Error::other(
            "installed persistent plugin differs from shipped manifest",
        ));
    }
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
        &manifest,
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
