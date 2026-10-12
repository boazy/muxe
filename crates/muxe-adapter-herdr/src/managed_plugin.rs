//! The sole registration mutation owned by Muxe; never runs hooks or opens a pane.

use std::path::Path;

use muxe_adapter_api::{AdapterError, AdapterErrorKind};
use serde_json::{Value, json};

use crate::{
    HerdrCache, HerdrResponse,
    launch::{COMMAND_PANE_ENTRYPOINT, COMMAND_PANE_PLUGIN_ID},
    runtime::HerdrRequestAuthority,
};

pub(crate) const METHODS: &[&str] = &["plugin.list", "plugin.link"];

/// Must be called inside the exact runtime's ordered transaction. In particular,
/// a command open cannot interleave another Muxe mutation between repair and use.
pub(crate) async fn ensure_with(
    authority: &dyn HerdrRequestAuthority,
    cache: &HerdrCache,
) -> Result<(), AdapterError> {
    // Probe authority first: an obsolete endpoint cannot authorize local setup.
    let before = list(authority).await?;
    let path = cache.command_pane_manifest().map_err(|_| failure(
        AdapterErrorKind::Unavailable,
        "Muxe could not read or update its Herdr command-pane manifest. Check cache ownership, permissions, and available disk space. Muxe will not use symlinks, hard-linked files, or files owned by another user.",
    ))?;
    if registration_correct(&before, &path)? {
        return Ok(());
    }
    let path_text = path.to_str().ok_or_else(|| failure(
        AdapterErrorKind::InvalidRequest,
        "The managed Herdr command-pane manifest path must be UTF-8. Choose a UTF-8 cache directory.",
    ))?;
    let response = request(
        authority,
        "plugin.link",
        json!({"path": path_text, "enabled": true}),
    )
    .await?;
    match response {
        HerdrResponse::Success(result)
            if result.get("type").and_then(Value::as_str) == Some("plugin_linked")
                && result
                    .get("plugin")
                    .is_some_and(|entry| entry_correct(entry, &path)) => {}
        HerdrResponse::Success(_) => {
            return Err(failure(
                AdapterErrorKind::OutcomeUnknown,
                "Herdr accepted the request to link the Muxe command-pane plugin, but returned an invalid plugin description. The link may have changed. No executable was launched. Inspect `herdr plugin list --plugin muxe.command-panes --json` before trying again.",
            ));
        }
        HerdrResponse::Error { .. } => {
            return Err(failure(
                AdapterErrorKind::DispatchFailed,
                "Herdr refused to link the Muxe command-pane plugin. Check that its plugin registry is writable. No executable was launched.",
            ));
        }
    }
    // A successful link response alone is not durable registry evidence.
    let after = list(authority).await?;
    if !registration_correct(&after, &path)? {
        return Err(failure(
            AdapterErrorKind::DispatchFailed,
            "Herdr accepted the link request, but the Muxe command-pane plugin is still missing, disabled, or incorrectly configured. No executable was launched. Inspect `herdr plugin list --plugin muxe.command-panes --json` and the registry permissions.",
        ));
    }
    Ok(())
}

async fn request(
    authority: &dyn HerdrRequestAuthority,
    method: &str,
    params: Value,
) -> Result<HerdrResponse, AdapterError> {
    authority.request(method, params).await.map_err(|error| {
        // Preserve transport/outcome classification and continuity notifications,
        // but never expose host payloads, argv or environment through diagnostics.
        failure(error.kind, "Muxe could not finish checking or repairing its Herdr command-pane plugin. No executable was launched. Check the Herdr connection and inspect `herdr plugin list --plugin muxe.command-panes --json` before trying again.")
    })
}

async fn list(authority: &dyn HerdrRequestAuthority) -> Result<Value, AdapterError> {
    match request(
        authority,
        "plugin.list",
        json!({"plugin_id": COMMAND_PANE_PLUGIN_ID}),
    )
    .await?
    {
        HerdrResponse::Success(result)
            if result.get("type").and_then(Value::as_str) == Some("plugin_list")
                && result.get("plugins").is_some_and(Value::is_array) =>
        {
            Ok(result)
        }
        HerdrResponse::Success(_) => Err(failure(
            AdapterErrorKind::InvalidRequest,
            "Herdr returned an invalid response while listing its plugins. Muxe could not verify the command-pane plugin. No executable was launched.",
        )),
        HerdrResponse::Error { .. } => Err(failure(
            AdapterErrorKind::DispatchFailed,
            "Herdr refused the request to list its plugins. Muxe could not verify the command-pane plugin. Check the Herdr version and plugin registry permissions. No executable was launched.",
        )),
    }
}

#[expect(clippy::result_large_err, reason = "shared adapter boundary error")]
fn registration_correct(result: &Value, path: &Path) -> Result<bool, AdapterError> {
    let entries = result["plugins"].as_array().ok_or_else(|| {
        failure(
            AdapterErrorKind::InvalidRequest,
            "Herdr returned an invalid plugin list. Muxe could not verify the command-pane plugin. No executable was launched.",
        )
    })?;
    let mut selected = None;
    for entry in entries {
        let id = entry.get("plugin_id").and_then(Value::as_str).ok_or_else(|| failure(AdapterErrorKind::InvalidRequest,
            "Herdr returned a plugin entry without a plugin ID. Muxe could not verify the command-pane plugin. No executable was launched."))?;
        if id == COMMAND_PANE_PLUGIN_ID && selected.replace(entry).is_some() {
            return Err(failure(
                AdapterErrorKind::InvalidRequest,
                "Herdr listed muxe.command-panes more than once. Muxe cannot choose which entry to repair. No executable was launched. Inspect `herdr plugin list --plugin muxe.command-panes --json` before trying again.",
            ));
        }
    }
    Ok(selected.is_some_and(|entry| entry_correct(entry, path)))
}

fn empty_array_or_absent(entry: &Value, field: &str) -> bool {
    entry
        .get(field)
        .is_none_or(|value| value.as_array().is_some_and(Vec::is_empty))
}

fn absent_or_null(entry: &Value, field: &str) -> bool {
    entry.get(field).is_none_or(Value::is_null)
}

fn entry_correct(entry: &Value, path: &Path) -> bool {
    let Some(path_text) = path.to_str() else {
        return false;
    };
    let Some(root_text) = path.parent().and_then(Path::to_str) else {
        return false;
    };
    if entry.get("plugin_id").and_then(Value::as_str) != Some(COMMAND_PANE_PLUGIN_ID)
        || entry.get("enabled").and_then(Value::as_bool) != Some(true)
        || entry.get("manifest_path").and_then(Value::as_str) != Some(path_text)
        || entry.get("plugin_root").and_then(Value::as_str) != Some(root_text)
        || entry.get("name").and_then(Value::as_str) != Some("Muxe command panes")
        || entry.get("version").and_then(Value::as_str) != Some("0.1.0")
        || entry.get("min_herdr_version").and_then(Value::as_str) != Some("0.8.2")
        || !string_array_matches(entry.get("platforms"), &["linux", "macos"])
        || entry.get("description").and_then(Value::as_str)
            != Some("Run exact executable commands in Herdr overlays and popups through Muxe")
        || [
            "warnings",
            "startup",
            "actions",
            "events",
            "build",
            "link_handlers",
        ]
        .iter()
        .any(|field| !empty_array_or_absent(entry, field))
    {
        return false;
    }
    if let Some(source) = entry.get("source")
        && (source.get("kind").and_then(Value::as_str) != Some("local")
            || [
                "managed_path",
                "owner",
                "repo",
                "requested_ref",
                "resolved_commit",
                "subdir",
            ]
            .iter()
            .any(|field| !absent_or_null(source, field)))
    {
        return false;
    }
    let Some(panes) = entry.get("panes").and_then(Value::as_array) else {
        return false;
    };
    if panes.len() != 1 {
        return false;
    }
    let pane = &panes[0];
    pane.get("id").and_then(Value::as_str) == Some(COMMAND_PANE_ENTRYPOINT)
        && pane.get("title").and_then(Value::as_str) == Some("Muxe command")
        && pane.get("placement").and_then(Value::as_str) == Some("popup")
        && string_array_matches(pane.get("command"), &["muxe", "pane", "exec"])
        && ["width", "height", "platforms", "description"]
            .iter()
            .all(|field| absent_or_null(pane, field))
}

fn string_array_matches(value: Option<&Value>, expected: &[&str]) -> bool {
    value.and_then(Value::as_array).is_some_and(|values| {
        values.len() == expected.len()
            && values
                .iter()
                .zip(expected)
                .all(|(value, expected)| value.as_str() == Some(*expected))
    })
}

fn failure(kind: AdapterErrorKind, message: &'static str) -> AdapterError {
    AdapterError::new(kind, message)
        .with_safe_diagnostic(muxe_adapter_api::SafeAdapterDiagnostic::new(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::{collections::VecDeque, path::PathBuf};

    struct Script {
        replies: Mutex<VecDeque<(&'static str, Result<HerdrResponse, AdapterError>)>>,
        calls: Mutex<Vec<(String, Value)>>,
    }

    impl Script {
        fn new(replies: Vec<(&'static str, Result<HerdrResponse, AdapterError>)>) -> Self {
            Self {
                replies: Mutex::new(replies.into()),
                calls: Mutex::new(Vec::new()),
            }
        }
        fn methods(&self) -> Vec<String> {
            self.calls
                .lock()
                .iter()
                .map(|(method, _)| method.clone())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl HerdrRequestAuthority for Script {
        async fn request(
            &self,
            method: &str,
            params: Value,
        ) -> Result<HerdrResponse, AdapterError> {
            self.calls.lock().push((method.to_owned(), params));
            let (expected, response) = self.replies.lock().pop_front().expect("unexpected request");
            assert_eq!(method, expected);
            response
        }
    }

    fn entry(path: &Path) -> Value {
        json!({
            "plugin_id": COMMAND_PANE_PLUGIN_ID, "name": "Muxe command panes",
            "version": "0.1.0", "min_herdr_version": "0.8.2",
            "description": "Run exact executable commands in Herdr overlays and popups through Muxe",
            "platforms": ["linux", "macos"], "enabled": true,
            "manifest_path": path, "plugin_root": path.parent().unwrap(),
            "panes": [{"id": "command", "title": "Muxe command", "placement": "popup",
                "command": ["muxe", "pane", "exec"]}],
            "source": {"kind": "local"}
        })
    }

    fn listed(entries: Vec<Value>) -> HerdrResponse {
        HerdrResponse::Success(Value::Object(serde_json::Map::from_iter([
            ("type".to_owned(), Value::String("plugin_list".to_owned())),
            ("plugins".to_owned(), Value::Array(entries)),
        ])))
    }

    fn linked(entry: Value) -> HerdrResponse {
        HerdrResponse::Success(Value::Object(serde_json::Map::from_iter([
            ("type".to_owned(), Value::String("plugin_linked".to_owned())),
            ("plugin".to_owned(), entry),
        ])))
    }

    #[tokio::test]
    async fn correct_registration_is_no_op_and_exact_identity_is_selected() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        let path = cache.command_pane_manifest().unwrap();
        let before = std::fs::metadata(&path).unwrap();
        let script = Script::new(vec![(
            "plugin.list",
            Ok(listed(vec![
                json!({"plugin_id": "unrelated", "enabled": false, "warnings": ["secret"]}),
                entry(&path),
            ])),
        )]);
        ensure_with(&script, &cache).await.unwrap();
        assert_eq!(script.methods(), ["plugin.list"]);
        assert_eq!(
            script.calls.lock()[0].1,
            json!({"plugin_id": COMMAND_PANE_PLUGIN_ID})
        );
        let after = std::fs::metadata(&path).unwrap();
        assert_eq!(before.modified().unwrap(), after.modified().unwrap());
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(
                before.ino(),
                after.ino(),
                "correct manifest must not be replaced"
            );
        }
    }

    #[tokio::test]
    async fn absent_stale_disabled_and_incorrect_contracts_are_repaired_once() {
        for defect in [
            "absent",
            "deleted-install",
            "disabled",
            "version",
            "root",
            "runner",
            "entrypoint",
            "extra-pane",
            "hooks",
            "warnings",
            "malformed",
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let cache = HerdrCache::new(temp.path());
            let path = cache.command_pane_manifest().unwrap();
            let good = entry(&path);
            let mut broken = good.clone();
            match defect {
                "absent" => {}
                "deleted-install" => {
                    broken["manifest_path"] =
                        json!("/deleted/mise/0.4.0/lib/muxe/herdr-plugin.toml");
                    broken["warnings"] = json!(["manifest unavailable: secret path"]);
                }
                "disabled" => broken["enabled"] = json!(false),
                "version" => broken["version"] = json!("9.9.9"),
                "root" => broken["plugin_root"] = json!("/other/root"),
                "runner" => broken["panes"][0]["command"] = json!(["unrelated", "command"]),
                "entrypoint" => broken["panes"][0]["id"] = json!("unrelated"),
                "extra-pane" => broken["panes"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"id": "other"})),
                "hooks" => broken["startup"] = json!([{"command": ["unrelated"]}]),
                "warnings" => broken["warnings"] = json!(["stale metadata"]),
                "malformed" => broken = json!({"plugin_id": COMMAND_PANE_PLUGIN_ID}),
                _ => unreachable!(),
            }
            let unrelated = json!({"plugin_id": "other", "enabled": false});
            let entries = if defect == "absent" {
                vec![unrelated.clone()]
            } else {
                vec![unrelated.clone(), broken]
            };
            let script = Script::new(vec![
                ("plugin.list", Ok(listed(entries))),
                ("plugin.link", Ok(linked(good.clone()))),
                ("plugin.list", Ok(listed(vec![good, unrelated]))),
            ]);
            ensure_with(&script, &cache).await.unwrap();
            assert_eq!(
                script.methods(),
                ["plugin.list", "plugin.link", "plugin.list"],
                "{defect}"
            );
            assert_eq!(
                script.calls.lock()[1].1,
                json!({"path": path, "enabled": true})
            );
        }
    }

    #[tokio::test]
    async fn ambiguous_or_unidentified_registry_never_mutates() {
        for entries in [
            vec![json!({})],
            vec![
                json!({"plugin_id": COMMAND_PANE_PLUGIN_ID}),
                json!({"plugin_id": COMMAND_PANE_PLUGIN_ID}),
            ],
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let script = Script::new(vec![("plugin.list", Ok(listed(entries)))]);
            let error = ensure_with(&script, &HerdrCache::new(temp.path()))
                .await
                .unwrap_err();
            assert_eq!(error.kind, AdapterErrorKind::InvalidRequest);
            assert_eq!(script.methods(), ["plugin.list"]);
        }
        let temp = tempfile::TempDir::new().unwrap();
        let script = Script::new(vec![(
            "plugin.list",
            Ok(HerdrResponse::Success(
                json!({"type": "secret", "plugins": []}),
            )),
        )]);
        assert_eq!(
            ensure_with(&script, &HerdrCache::new(temp.path()))
                .await
                .unwrap_err()
                .kind,
            AdapterErrorKind::InvalidRequest
        );
        assert!(!temp.path().join("herdr/herdr-plugin.toml").exists());
    }

    #[tokio::test]
    async fn transport_outcomes_and_link_rejections_are_preserved_without_payloads_or_replay() {
        for kind in [
            AdapterErrorKind::OutcomeUnknown,
            AdapterErrorKind::Unavailable,
            AdapterErrorKind::Shutdown,
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let script = Script::new(vec![
                ("plugin.list", Ok(listed(vec![]))),
                (
                    "plugin.link",
                    Err(AdapterError::new(kind, "secret argv env host payload")),
                ),
            ]);
            let error = ensure_with(&script, &HerdrCache::new(temp.path()))
                .await
                .unwrap_err();
            assert_eq!(error.kind, kind);
            assert!(!error.message.contains("secret"));
            assert!(!error.safe_message().contains("secret"));
            assert_eq!(script.methods(), ["plugin.list", "plugin.link"]);
        }
        for reply in [
            Ok(HerdrResponse::Error {
                code: "secret".into(),
                message: "secret host".into(),
            }),
            Ok(HerdrResponse::Success(json!({"type": "secret"}))),
        ] {
            let expected = if matches!(reply, Ok(HerdrResponse::Error { .. })) {
                AdapterErrorKind::DispatchFailed
            } else {
                AdapterErrorKind::OutcomeUnknown
            };
            let temp = tempfile::TempDir::new().unwrap();
            let script = Script::new(vec![
                ("plugin.list", Ok(listed(vec![]))),
                ("plugin.link", reply),
            ]);
            let error = ensure_with(&script, &HerdrCache::new(temp.path()))
                .await
                .unwrap_err();
            assert_eq!(error.kind, expected);
            assert!(!error.message.contains("secret"));
            assert_eq!(script.methods(), ["plugin.list", "plugin.link"]);
        }
    }

    #[tokio::test]
    async fn link_success_requires_correct_readback_before_any_open() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        let path = cache.command_pane_manifest().unwrap();
        let script = Script::new(vec![
            ("plugin.list", Ok(listed(vec![]))),
            ("plugin.link", Ok(linked(entry(&path)))),
            ("plugin.list", Ok(listed(vec![]))),
        ]);
        assert_eq!(
            ensure_with(&script, &cache).await.unwrap_err().kind,
            AdapterErrorKind::DispatchFailed
        );
        assert_eq!(
            script.methods(),
            ["plugin.list", "plugin.link", "plugin.list"]
        );
    }

    #[tokio::test]
    async fn unsafe_manifest_is_refused_before_registration() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        let path = cache.command_pane_manifest().unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        let script = Script::new(vec![("plugin.list", Ok(listed(vec![])))]);
        let error = ensure_with(&script, &cache).await.unwrap_err();
        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
        assert_eq!(script.methods(), ["plugin.list"]);
        assert!(path.is_dir());
    }

    fn snapshot(active: &str) -> HerdrResponse {
        HerdrResponse::Success(json!({"type": "session_snapshot", "snapshot": {
            "focused_workspace_id": "w", "focused_tab_id": "t", "focused_pane_id": active,
            "panes": [{"workspace_id": "w", "tab_id": "t", "pane_id": active, "cwd": "/owned"}],
            "layouts": [{"workspace_id": "w", "tab_id": "t", "panes": [
                {"pane_id": active, "rect": {"width": 80, "height": 24}}
            ]}]
        }}))
    }

    fn launch(pane_type: crate::PluginCommandPaneType) -> crate::PluginCommandPaneLaunch {
        crate::PluginCommandPaneLaunch {
            origin: crate::FocusedPane {
                workspace: muxe_core::WorkspaceId::new("w"),
                tab: muxe_core::TabId::new("t"),
                pane: muxe_core::PaneId::new("p"),
                cwd: PathBuf::from("/owned"),
                columns: 80,
                rows: 24,
            },
            cwd: PathBuf::from("/owned"),
            argv: vec!["exact-program".into(), "arg with spaces".into()],
            pane_type,
            width: None,
            height: None,
        }
    }

    #[tokio::test]
    async fn both_executable_placements_repair_then_open_exactly_once_and_never_replay_unknown() {
        for pane_type in [
            crate::PluginCommandPaneType::Popup,
            crate::PluginCommandPaneType::Overlay,
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let cache = HerdrCache::new(temp.path());
            let path = cache.command_pane_manifest().unwrap();
            let script = Script::new(vec![
                ("session.snapshot", Ok(snapshot("p"))),
                ("plugin.list", Ok(listed(vec![]))),
                ("plugin.link", Ok(linked(entry(&path)))),
                ("plugin.list", Ok(listed(vec![entry(&path)]))),
                ("session.snapshot", Ok(snapshot("p"))),
                (
                    "plugin.pane.open",
                    Err(AdapterError::new(
                        AdapterErrorKind::OutcomeUnknown,
                        "unknown command delivery",
                    )),
                ),
            ]);
            let error =
                crate::launch::open_plugin_command_pane_with(&script, &cache, launch(pane_type))
                    .await
                    .unwrap_err();
            assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
            assert_eq!(
                script.methods(),
                [
                    "session.snapshot",
                    "plugin.list",
                    "plugin.link",
                    "plugin.list",
                    "session.snapshot",
                    "plugin.pane.open"
                ]
            );
            let calls = script.calls.lock();
            let params = &calls.last().unwrap().1;
            assert_eq!(params["plugin_id"], COMMAND_PANE_PLUGIN_ID);
            assert_eq!(params["entrypoint"], COMMAND_PANE_ENTRYPOINT);
            assert_eq!(
                params["env"][crate::COMMAND_PANE_ARGV_ENV],
                "[\"exact-program\",\"arg with spaces\"]"
            );
            assert_eq!(params["cwd"], "/owned");
        }
    }

    #[tokio::test]
    async fn focus_changed_during_repair_does_not_open_a_terminal_in_the_wrong_origin() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        let path = cache.command_pane_manifest().unwrap();
        let script = Script::new(vec![
            ("session.snapshot", Ok(snapshot("p"))),
            ("plugin.list", Ok(listed(vec![]))),
            ("plugin.link", Ok(linked(entry(&path)))),
            ("plugin.list", Ok(listed(vec![entry(&path)]))),
            ("session.snapshot", Ok(snapshot("other"))),
        ]);
        let error = crate::launch::open_plugin_command_pane_with(
            &script,
            &cache,
            launch(crate::PluginCommandPaneType::Popup),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, AdapterErrorKind::InvalidRequest);
        assert_eq!(
            error.safe_message(),
            crate::HerdrExecutionDiagnostic::OriginNotActive
                .safe()
                .message()
        );
        assert!(
            !script
                .methods()
                .iter()
                .any(|method| method == "plugin.pane.open")
        );
    }

    #[tokio::test]
    async fn inactive_or_invalid_command_never_creates_manifest_or_repairs_registration() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        let script = Script::new(vec![("session.snapshot", Ok(snapshot("other")))]);
        crate::launch::open_plugin_command_pane_with(
            &script,
            &cache,
            launch(crate::PluginCommandPaneType::Popup),
        )
        .await
        .unwrap_err();
        assert_eq!(script.methods(), ["session.snapshot"]);
        assert!(!temp.path().join("herdr/herdr-plugin.toml").exists());
        let script = Script::new(vec![]);
        let mut invalid = launch(crate::PluginCommandPaneType::Popup);
        invalid.argv.clear();
        crate::launch::open_plugin_command_pane_with(&script, &cache, invalid)
            .await
            .unwrap_err();
        assert!(script.methods().is_empty());
    }

    #[tokio::test]
    async fn verification_failure_blocks_the_user_command_without_retrying() {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        let script = Script::new(vec![
            ("session.snapshot", Ok(snapshot("p"))),
            ("plugin.list", Ok(listed(vec![]))),
            (
                "plugin.link",
                Err(AdapterError::new(
                    AdapterErrorKind::OutcomeUnknown,
                    "secret registration transport",
                )),
            ),
        ]);
        let error = crate::launch::open_plugin_command_pane_with(
            &script,
            &cache,
            launch(crate::PluginCommandPaneType::Popup),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, AdapterErrorKind::OutcomeUnknown);
        assert_eq!(
            script.methods(),
            ["session.snapshot", "plugin.list", "plugin.link"]
        );
        assert!(!error.message.contains("secret"));
    }
}
