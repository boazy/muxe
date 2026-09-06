use muxe_core::{
    CompileInput, CompiledGeneration, Compiler, ConfigDocument, KeyCapabilities, SourceId,
    ThemeAssets,
};
use std::sync::Mutex;

fn document(name: &str, yaml: &str) -> ConfigDocument {
    ConfigDocument::parse(SourceId::new(name), yaml).unwrap()
}

fn compile(
    base: &str,
    host: Option<&str>,
    capabilities: KeyCapabilities,
) -> Result<muxe_core::CompiledConfig, Vec<muxe_core::ConfigDiagnostic>> {
    Compiler.compile(
        CompileInput {
            generation: CompiledGeneration(9),
            base: document("config.yml", base),
            host_override: host.map(|yaml| document("host.yml", yaml)),
            key_capabilities: capabilities,
            theme_assets: ThemeAssets::default(),
        },
        None,
    )
}

#[test]
fn ordered_host_merge_removes_injected_binding_and_replaces_settings() {
    let config = compile(
        r"
version: 1
settings:
  after_action: stay
menus:
  main:
    settings:
      after_action: return
    bindings:
      a:
        label: alpha
        settings:
          after_action: quit
        action: config:reload
  remove-me:
    bindings:
      x: { label: gone, action: config:reload }
",
        Some(
            r"
menus:
  remove-me: { _remove: true }
  main:
    settings:
      _replace: true
      after_action: return
inject:
  Builtin.escape: { _remove: true }
  Test.remove-backspace:
    select: { type: id:exact, value: main }
    action:
      type: override
      bindings:
        backspace: { _remove: true }
",
        ),
        KeyCapabilities::default(),
    )
    .expect("merged configuration should compile");

    assert!(config.menu(&muxe_core::MenuId::new("remove-me")).is_none());
    let main = config.menu(&muxe_core::MenuId::new("main")).unwrap();
    assert!(
        main.bindings
            .iter()
            .all(|binding| binding.key.canonical_string() != "esc")
    );
    assert!(
        main.bindings
            .iter()
            .all(|binding| binding.key.canonical_string() != "backspace")
    );
    let action = main
        .bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "a")
        .unwrap();
    assert_eq!(action.settings.after_action, muxe_core::AfterAction::Quit);
}

#[test]
fn cycles_and_invalid_context_types_are_rejected_with_semantic_codes() {
    let cycle = compile(
        r"
version: 1
menus:
  a:
    bindings: { a: { label: to-b, action: menu:open b } }
  b:
    bindings: { b: { label: to-a, action: menu:open a } }
",
        None,
        KeyCapabilities::default(),
    )
    .unwrap_err();
    assert!(
        cycle
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::MenuCycle)
    );

    let mismatch = compile(
        r"
version: 1
menus:
  main:
    bindings:
      c:
        label: command
        action:
          type: command:execute
          program: cargo
          cwd: { $context: origin.pane.id }
",
        None,
        KeyCapabilities::default(),
    )
    .unwrap_err();
    assert!(
        mismatch
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::ContextTypeMismatch)
    );
}

#[test]
fn kitty_capability_validation_keeps_documented_ctrl_l_repeat_path() {
    let host = KeyCapabilities {
        event_types: true,
        alternate_keys: true,
        all_keys_as_escape_codes: false,
    };
    let valid = compile(
        r"
version: 1
keyboard:
  mode: kitty
  kitty: { event-types: true, alternate-keys: true, all-keys-as-escape-codes: false }
menus:
  main:
    bindings:
      ctrl+l:
        label: resize
        settings: { repeat: true }
        action: pane:resize direction=right amount=0.1
",
        None,
        host,
    );
    assert!(
        valid.is_ok(),
        "the documented Herdr ctrl+l repeat case is valid"
    );

    let invalid = compile(
        r"
version: 1
keyboard:
  mode: kitty
  kitty: { event-types: true, alternate-keys: true, all-keys-as-escape-codes: false }
menus:
  main:
    bindings:
      a:
        label: text repeat
        settings: { repeat: true }
        action: config:reload
",
        None,
        host,
    )
    .unwrap_err();
    assert!(
        invalid
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::KeyCapability)
    );
}

#[test]
fn command_execution_defaults_to_detach_unless_an_inherited_mode_overrides_it() {
    let detached = compile(
        r"
version: 1
menus:
  main:
    bindings:
      c:
        label: command
        action: command:execute program=echo
",
        None,
        KeyCapabilities::default(),
    )
    .expect("command action with no mode should compile");
    assert_eq!(
        detached
            .menu(&muxe_core::MenuId::new("main"))
            .unwrap()
            .bindings
            .iter()
            .find(|binding| binding.key.canonical_string() == "c")
            .unwrap()
            .settings
            .execution
            .mode,
        muxe_core::ExecutionMode::Detach,
    );

    let inherited = compile(
        r"
version: 1
settings:
  execution: { mode: await }
menus:
  main:
    bindings:
      c:
        label: command
        action: command:execute program=echo
",
        None,
        KeyCapabilities::default(),
    )
    .expect("an explicit inherited mode should compile");
    assert_eq!(
        inherited
            .menu(&muxe_core::MenuId::new("main"))
            .unwrap()
            .bindings
            .iter()
            .find(|binding| binding.key.canonical_string() == "c")
            .unwrap()
            .settings
            .execution
            .mode,
        muxe_core::ExecutionMode::Await,
    );
}

#[test]
fn portable_multiplexer_families_remain_available_to_host_capability_mapping() {
    let config = compile(
        r#"
version: 1
menus:
  main:
    bindings:
      a: { label: tab-swap, action: "tab:swap index=5" }
      b: { label: pane-create, action: pane:create }
      c: { label: pane-fullscreen, action: "pane:fullscreen enabled=true" }
      d: { label: pane-floating, action: "pane:floating enabled=false" }
      e: { label: pane-frame, action: "pane:frame visible=true" }
      f: { label: session-create, action: session:create }
      g: { label: session-attach, action: "session:attach work" }
      h: { label: session-switch, action: "session:switch work" }
      i: { label: session-rename, action: "session:rename work" }
      j: { label: session-detach, action: session:detach }
      k: { label: session-quit, action: session:quit }
      l: { label: session-kill, action: session:kill }
"#,
        None,
        KeyCapabilities::default(),
    )
    .expect("the Design portable families should compile before host capability checks");

    let kinds = config
        .menu(&muxe_core::MenuId::new("main"))
        .unwrap()
        .bindings
        .iter()
        .map(|binding| binding.action.kind())
        .collect::<std::collections::BTreeSet<_>>();
    for kind in [
        muxe_core::PortableActionKind::TabSwap,
        muxe_core::PortableActionKind::PaneCreate,
        muxe_core::PortableActionKind::PaneFullscreen,
        muxe_core::PortableActionKind::PaneFloating,
        muxe_core::PortableActionKind::PaneFrame,
        muxe_core::PortableActionKind::SessionCreate,
        muxe_core::PortableActionKind::SessionAttach,
        muxe_core::PortableActionKind::SessionSwitch,
        muxe_core::PortableActionKind::SessionRename,
        muxe_core::PortableActionKind::SessionDetach,
        muxe_core::PortableActionKind::SessionQuit,
        muxe_core::PortableActionKind::SessionKill,
    ] {
        assert!(kinds.contains(&muxe_core::ActionKind::Portable(kind)));
    }
}

#[test]
fn source_aware_yaml_and_unknown_field_errors_keep_the_candidate_inactive() {
    let error = ConfigDocument::parse(SourceId::new("bad.yml"), "version: 1\nmenus: [\n")
        .expect_err("malformed YAML must not produce a candidate");
    assert_eq!(error.code, muxe_core::DiagnosticCode::YamlSyntax);
    assert_eq!(error.labels[0].span.source.as_str(), "bad.yml");
}

#[test]
fn reload_and_host_version_settings_are_typed_retained_and_strictly_validated() {
    let config = compile(
        r"
version: 1
settings:
  reload: { watch: false, debounce: 350ms }
  host:
    version: { check: strict }
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
        None,
        KeyCapabilities::default(),
    )
    .expect("typed global settings should compile");
    assert_eq!(
        config.reload,
        muxe_core::ReloadSettings {
            watch: false,
            debounce: std::time::Duration::from_millis(350),
        }
    );
    assert_eq!(
        config.host.version_check,
        muxe_core::HostVersionCheck::Strict
    );

    let invalid = compile(
        r"
version: 1
settings:
  reload: { watch: true, unexpected: value }
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
        None,
        KeyCapabilities::default(),
    )
    .unwrap_err();
    assert!(
        invalid
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::UnknownField)
    );

    let nested = compile(
        r"
version: 1
menus:
  main:
    settings:
      reload: { watch: false }
    bindings:
      r: { label: reload, action: config:reload }
",
        None,
        KeyCapabilities::default(),
    )
    .unwrap_err();
    assert!(
        nested
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::UnknownField)
    );
}

#[test]
fn compact_actions_preserve_quotes_empty_values_equals_and_yaml_core_scalars() {
    let config = compile(
        r#"
version: 1
menus:
  main:
    bindings:
      a: { label: attach literal, action: 'session:attach "a=b"' }
      b: { label: empty command, action: 'command:execute program=""' }
      c: { label: fullscreen, action: 'pane:fullscreen enabled=true' }
      d: { label: focus hexadecimal, action: 'tab:focus index=0x10' }
      e: { label: literal context syntax, action: 'session:attach "$origin.workspace.id"' }
"#,
        None,
        KeyCapabilities::default(),
    )
    .expect("quoted tokens and YAML core scalars should keep their distinct meanings");
    let bindings = &config
        .menu(&muxe_core::MenuId::new("main"))
        .unwrap()
        .bindings;
    let attach = bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "a")
        .unwrap();
    let muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Session(
        muxe_core::SessionAction::Attach { name },
    )) = &attach.action
    else {
        panic!("expected session attach");
    };
    assert_eq!(name.value.as_str(), Some("a=b"));

    let command = bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "b")
        .unwrap();
    let muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Command(command)) =
        &command.action
    else {
        panic!("expected command");
    };
    assert_eq!(command.program.value.as_str(), Some(""));

    let fullscreen = bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "c")
        .unwrap();
    let muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Pane(
        muxe_core::PaneAction::Fullscreen {
            enabled: Some(enabled),
        },
    )) = &fullscreen.action
    else {
        panic!("expected fullscreen");
    };
    assert_eq!(enabled.value.as_bool(), Some(true));

    let focus = bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "d")
        .unwrap();
    let muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Tab(
        muxe_core::TabAction::Focus(muxe_core::IndexOrDirection::Index(index)),
    )) = &focus.action
    else {
        panic!("expected tab focus");
    };
    assert!(matches!(
        index.value.kind,
        muxe_core::ConfigValueKind::Integer(16)
    ));

    let literal_context = bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "e")
        .unwrap();
    let muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Session(
        muxe_core::SessionAction::Attach { name },
    )) = &literal_context.action
    else {
        panic!("expected literal session attach");
    };
    assert_eq!(name.value.as_str(), Some("$origin.workspace.id"));

    for action in [
        "pane:fullscreen enabled=\"true\"",
        "tab:focus index=\"16\"",
        "pane:resize direction=left direction=right",
        "session:attach name=work later",
        "session:attach work name=again",
        "command:execute program=[echo]",
    ] {
        let yaml = format!(
            "version: 1\nmenus:\n  main:\n    bindings:\n      a: {{ label: compact, action: '{action}' }}\n"
        );
        assert!(
            compile(&yaml, None, KeyCapabilities::default()).is_err(),
            "compact action must reject {action:?}",
        );
    }
}

#[test]
fn selected_theme_and_color_scheme_are_carried_to_attachment() {
    let assets = ThemeAssets {
        themes: std::collections::BTreeMap::from([(
            "custom".to_owned(),
            document(
                "themes/custom.yml",
                r#"
common:
  styles:
    default: { foreground: base.text }
menu:
  styles:
    title: { foreground: menu.hotkey, bold: true }
  templates:
    cell: "{{ title }}"
"#,
            ),
        )]),
        color_schemes: std::collections::BTreeMap::from([(
            "ink".to_owned(),
            document(
                "color-schemes/ink.yml",
                r##"
title: Ink
palette:
  foreground: "#ddeeff"
  accent: "#789abc"
colors:
  base: { text: foreground }
  menu: { hotkey: accent }
"##,
            ),
        )]),
    };
    let config = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(10),
                base: document(
                    "config.yml",
                    r"
version: 1
theme: custom
color-scheme: ink
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: assets,
            },
            None,
        )
        .expect("selected source-tracked assets should compile and pair");
    assert_eq!(config.theme_selection.theme, "custom");
    assert_eq!(config.theme_selection.color_scheme, "ink");
    let attachment = config
        .attachment_view(&muxe_core::MenuId::new("main"))
        .expect("compiled main menu has an attachment");
    assert_eq!(attachment.theme_selection, config.theme_selection);
    assert_eq!(attachment.theme, config.theme);
}

#[test]
fn unknown_or_invalid_theme_assets_are_rejected() {
    let unknown = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(11),
                base: document(
                    "config.yml",
                    r"
version: 1
theme: absent
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets::default(),
            },
            None,
        )
        .expect_err("an absent selected theme must not compile");
    assert!(
        unknown
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::InvalidTheme)
    );

    let invalid_pair = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(12),
                base: document(
                    "config.yml",
                    r"
version: 1
theme: broken
color-scheme: ink
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets {
                    themes: std::collections::BTreeMap::from([(
                        "broken".to_owned(),
                        document(
                            "themes/broken.yml",
                            r"
common:
  styles:
    default: { foreground: absent.color }
",
                        ),
                    )]),
                    color_schemes: std::collections::BTreeMap::from([(
                        "ink".to_owned(),
                        document(
                            "color-schemes/ink.yml",
                            r##"
title: Ink
palette: { foreground: "#ddeeff" }
colors: { base: { text: foreground } }
"##,
                        ),
                    )]),
                },
            },
            None,
        )
        .expect_err("an invalid selected theme pair must not compile");
    assert!(
        invalid_pair
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::InvalidTheme)
    );
}

struct DirectionalHostValidator;

impl muxe_core::ActionValidator for DirectionalHostValidator {
    fn validate_portable(
        &self,
        action: &muxe_core::PortableAction,
        action_span: &muxe_core::SourceSpan,
    ) -> Result<muxe_core::ActionValidation, muxe_core::ConfigDiagnostic> {
        if let muxe_core::PortableAction::Pane(muxe_core::PaneAction::Split {
            direction: Some(direction),
        }) = action
            && direction.value.as_str() == Some("right")
        {
            return Err(muxe_core::ConfigDiagnostic::error(
                muxe_core::DiagnosticCode::InvalidActionArguments,
                "the active host does not support rightward splits",
                action_span.clone(),
            ));
        }
        Ok(muxe_core::ActionValidation {
            execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    fn validate_native_batch(
        &self,
        _candidates: &[&muxe_core::NativeActionCandidate],
    ) -> Result<Vec<muxe_core::ActionValidation>, Vec<muxe_core::ConfigDiagnostic>> {
        unreachable!("the test config contains no native action")
    }
}

#[derive(Default)]
struct BatchHostValidator {
    batches: Mutex<Vec<Vec<String>>>,
    omit_last_validation: bool,
}

impl muxe_core::ActionValidator for BatchHostValidator {
    fn validate_portable(
        &self,
        _action: &muxe_core::PortableAction,
        _action_span: &muxe_core::SourceSpan,
    ) -> Result<muxe_core::ActionValidation, muxe_core::ConfigDiagnostic> {
        Ok(muxe_core::ActionValidation {
            execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    fn validate_native_batch(
        &self,
        candidates: &[&muxe_core::NativeActionCandidate],
    ) -> Result<Vec<muxe_core::ActionValidation>, Vec<muxe_core::ConfigDiagnostic>> {
        self.batches
            .lock()
            .expect("batch observations are not poisoned")
            .push(
                candidates
                    .iter()
                    .map(|candidate| candidate.type_name.clone())
                    .collect(),
            );
        let validation_count = candidates
            .len()
            .saturating_sub(usize::from(self.omit_last_validation));
        Ok(vec![
            muxe_core::ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            };
            validation_count
        ])
    }
}

#[test]
fn native_actions_are_validated_once_as_an_ordered_effective_batch() {
    let validator = BatchHostValidator::default();
    Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(12),
                base: document(
                    "config.yml",
                    r"
version: 1
menus:
  first:
    bindings:
      a: { label: first, action: native.test:first }
  second:
    bindings:
      b: { label: second, action: native.test:second }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets::default(),
            },
            Some(&validator),
        )
        .expect("native actions validate as one batch");
    assert_eq!(
        *validator
            .batches
            .lock()
            .expect("batch observations are not poisoned"),
        vec![vec![
            "native.test:first".to_owned(),
            "native.test:second".to_owned(),
        ]]
    );
}

#[test]
fn native_validation_batch_must_cover_every_effective_candidate() {
    let validator = BatchHostValidator {
        batches: Mutex::default(),
        omit_last_validation: true,
    };
    let diagnostics = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(12),
                base: document(
                    "config.yml",
                    r"
version: 1
menus:
  first:
    bindings:
      a: { label: first, action: native.test:first }
  second:
    bindings:
      b: { label: second, action: native.test:second }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets::default(),
            },
            Some(&validator),
        )
        .expect_err("incomplete native validation results must reject the config");
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::NativeActionRejected)
    );
}

#[test]
fn active_host_portable_validation_rejects_an_incompatible_candidate_before_activation() {
    let validator = DirectionalHostValidator;
    let rejected = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(12),
                base: document(
                    "config.yml",
                    r"
version: 1
menus:
  main:
    bindings:
      s: { label: split, action: pane:split right }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets::default(),
            },
            Some(&validator),
        )
        .unwrap_err();
    assert!(rejected.iter().any(|diagnostic| {
        diagnostic.code == muxe_core::DiagnosticCode::InvalidActionArguments
            && diagnostic
                .labels
                .iter()
                .any(|label| label.span.source.as_str() == "config.yml")
    }));
}

fn origin_with_context_values() -> muxe_core::OriginContext {
    muxe_core::OriginContext {
        host_kind: muxe_core::OriginHostKind::Herdr,
        server_id: muxe_core::ServerId::new("server"),
        client_id: None,
        session_id: None,
        workspace_id: Some(muxe_core::WorkspaceId::new("workspace")),
        tab_id: Some(muxe_core::TabId::new("tab")),
        tab_index: Some(4),
        pane_id: Some(muxe_core::PaneId::new("pane")),
        pane_type: Some(muxe_core::OriginPaneType::Terminal),
        pane_cwd: Some(std::path::PathBuf::from("/tmp/project")),
        selection_text: Some("selected text".to_owned()),
        invocation_source: muxe_core::OriginInvocationSource::CommandLine,
        worktree_id: None,
        worktree_path: None,
        agent_id: None,
        link_url: None,
        link_handler_id: None,
    }
}

fn portable_context_config() -> muxe_core::CompiledConfig {
    compile(
        r"
version: 1
menus:
  main:
    bindings:
      c:
        label: command
        action:
          type: command:execute
          program: { $context: origin.selection.text }
          args: [{ $context: origin.selection.text }]
          cwd: { $context: origin.pane.cwd }
          env: { SELECTED: { $context: origin.selection.text } }
      k:
        label: keyboard text
        action:
          type: keyboard:send
          text: { $context: origin.selection.text }
      q:
        label: keyboard keys
        action:
          type: keyboard:send
          keys: [{ $context: origin.selection.text }]
      p:
        label: resize
        action:
          type: pane:resize
          direction: right
          amount: { $context: origin.tab.index }
      s:
        label: session
        action:
          type: session:attach
          name: { $context: origin.selection.text }
      t:
        label: focus origin tab
        action:
          type: tab:focus
          index: { $context: origin.tab.index }
      w:
        label: create workspace tab
        action: tab:create workspace-id=$origin.workspace.id
",
        None,
        KeyCapabilities::default(),
    )
    .expect("typed portable context values should compile")
}

fn portable_main_action<'a>(
    config: &'a muxe_core::CompiledConfig,
    key: &str,
) -> &'a muxe_core::PortableAction {
    let binding = config
        .menu(&muxe_core::MenuId::new("main"))
        .and_then(|menu| {
            menu.bindings
                .iter()
                .find(|binding| binding.key.canonical_string() == key)
        })
        .expect("compiled main menu has the requested binding");
    let muxe_core::ActionSpec::Portable(action) = &binding.action else {
        panic!("expected portable action");
    };
    action
}

#[test]
fn available_portable_context_values_resolve_for_each_action_kind() {
    let config = portable_context_config();
    let origin = origin_with_context_values();
    for key in ["c", "k", "p", "s", "t", "w"] {
        portable_main_action(&config, key)
            .resolve_context(&origin)
            .expect("available context must resolve and pass concrete validation");
    }
}

#[test]
fn command_context_values_preserve_source_and_revalidate_cwd() {
    let config = portable_context_config();
    let action = portable_main_action(&config, "c");
    let muxe_core::PortableAction::Command(unresolved) = action else {
        panic!("expected command");
    };
    assert!(matches!(
        unresolved.program.value.kind,
        muxe_core::ConfigValueKind::Context(_)
    ));
    assert_eq!(unresolved.program.value.span.source.as_str(), "config.yml");

    let origin = origin_with_context_values();
    let muxe_core::PortableAction::Command(command) = action
        .resolve_context(&origin)
        .expect("command context should resolve")
    else {
        panic!("expected resolved command");
    };
    assert_eq!(command.program.value.as_str(), Some("selected text"));
    assert_eq!(command.args[0].value.as_str(), Some("selected text"));
    assert_eq!(
        command.cwd.expect("configured cwd").value.as_str(),
        Some("/tmp/project")
    );
    assert_eq!(
        command.env["SELECTED"].value.as_str(),
        Some("selected text")
    );

    let relative_cwd_origin = muxe_core::OriginContext {
        pane_cwd: Some(std::path::PathBuf::from("relative/project")),
        ..origin
    };
    assert!(matches!(
        action.resolve_context(&relative_cwd_origin),
        Err(muxe_core::PortableActionResolutionError::InvalidValue {
            parameter: "command.cwd",
            ..
        })
    ));
}

#[test]
fn tab_and_keyboard_context_values_revalidate_to_their_declared_types() {
    let config = portable_context_config();
    let origin = origin_with_context_values();
    let tab_action = portable_main_action(&config, "t");
    let muxe_core::PortableAction::Tab(muxe_core::TabAction::Focus(
        muxe_core::IndexOrDirection::Index(index),
    )) = tab_action
        .resolve_context(&origin)
        .expect("tab index context resolves")
    else {
        panic!("expected resolved tab index");
    };
    assert!(matches!(
        index.value.kind,
        muxe_core::ConfigValueKind::Integer(4)
    ));

    let create_action = portable_main_action(&config, "w");
    let muxe_core::PortableAction::Tab(muxe_core::TabAction::Create {
        workspace_id: Some(workspace_id),
    }) = create_action
        .resolve_context(&origin)
        .expect("workspace context resolves")
    else {
        panic!("expected resolved workspace-targeted tab create");
    };
    assert_eq!(workspace_id.value.as_str(), Some("workspace"));

    let key_action = portable_main_action(&config, "q");
    let key_origin = muxe_core::OriginContext {
        selection_text: Some("ctrl+c".to_owned()),
        ..origin.clone()
    };
    key_action
        .resolve_context(&key_origin)
        .expect("resolved keyboard key must revalidate");
    let invalid_key_origin = muxe_core::OriginContext {
        selection_text: Some("not-a-canonical-key".to_owned()),
        ..origin
    };
    assert!(matches!(
        key_action.resolve_context(&invalid_key_origin),
        Err(muxe_core::PortableActionResolutionError::InvalidValue { .. })
    ));
}

#[test]
fn missing_context_value_fails_resolution() {
    let config = portable_context_config();
    let unavailable = muxe_core::OriginContext {
        selection_text: None,
        ..origin_with_context_values()
    };
    assert!(matches!(
        portable_main_action(&config, "c").resolve_context(&unavailable),
        Err(muxe_core::PortableActionResolutionError::Context(_))
    ));
}

#[expect(clippy::too_many_lines, reason = "single exhaustive alias-collision table; splitting would scatter the collision matrix")]
#[test]
fn vt100_aliases_collide_at_compile_time_and_match_their_single_legacy_event() {
    let aliases = compile(
        r#"
version: 1
keyboard: { mode: vt100 }
menus:
  main:
    bindings:
      tab: { label: tab, action: config:reload }
      "ctrl+i": { label: ctrl i, action: config:reload }
      enter: { label: enter, action: config:reload }
      "ctrl+m": { label: ctrl m, action: config:reload }
      backspace: { label: backspace, action: config:reload }
      "ctrl+h": { label: ctrl h, action: config:reload }
      esc: { label: escape, action: config:reload }
      "ctrl+[": { label: ctrl bracket, action: config:reload }
      "ctrl+a": { label: ctrl a, action: config:reload }
      "ctrl+shift+a": { label: ctrl shift a, action: config:reload }
"#,
        None,
        KeyCapabilities::default(),
    )
    .expect_err("VT100 aliases must not create ambiguous bindings");
    let collisions = aliases
        .iter()
        .filter(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::KeyCollision)
        .collect::<Vec<_>>();
    assert_eq!(collisions.len(), 5);
    assert!(
        collisions
            .iter()
            .all(|diagnostic| diagnostic.labels.len() == 2)
    );

    let profile = muxe_core::KeyboardProfile::Vt100 {
        escape_timeout: std::time::Duration::from_millis(25),
    };
    let named_event = |named| muxe_core::KeyEvent {
        primary: Some(muxe_core::KeyIdentity::Named(named)),
        alternate: None,
        base_layout: None,
        modifiers: muxe_core::Modifiers::empty(),
        kind: muxe_core::EventKind::Press,
        locks: muxe_core::LockModifiers::default(),
        keypad: false,
    };
    for (binding, event) in [
        ("tab", named_event(muxe_core::NamedKey::Tab)),
        ("ctrl+i", named_event(muxe_core::NamedKey::Tab)),
        ("enter", named_event(muxe_core::NamedKey::Enter)),
        ("ctrl+m", named_event(muxe_core::NamedKey::Enter)),
        ("backspace", named_event(muxe_core::NamedKey::Backspace)),
        ("ctrl+h", named_event(muxe_core::NamedKey::Backspace)),
        ("esc", named_event(muxe_core::NamedKey::Escape)),
        ("ctrl+[", named_event(muxe_core::NamedKey::Escape)),
    ] {
        assert!(profile.matches_binding(&muxe_core::CanonicalKey::parse(binding).unwrap(), &event));
    }
    let control_a = muxe_core::KeyEvent {
        primary: Some(muxe_core::KeyIdentity::Text('\u{1}')),
        alternate: None,
        base_layout: None,
        modifiers: muxe_core::Modifiers::empty(),
        kind: muxe_core::EventKind::Press,
        locks: muxe_core::LockModifiers::default(),
        keypad: false,
    };
    assert!(profile.matches_binding(
        &muxe_core::CanonicalKey::parse("ctrl+a").unwrap(),
        &control_a
    ));
    assert!(profile.matches_binding(
        &muxe_core::CanonicalKey::parse("ctrl+shift+a").unwrap(),
        &control_a,
    ));

    let kitty = muxe_core::KeyboardProfile::Kitty(KeyCapabilities::default());
    assert!(kitty.matches_binding(
        &muxe_core::CanonicalKey::parse("tab").unwrap(),
        &named_event(muxe_core::NamedKey::Tab),
    ));
    assert!(!kitty.matches_binding(
        &muxe_core::CanonicalKey::parse("ctrl+i").unwrap(),
        &named_event(muxe_core::NamedKey::Tab),
    ));

    compile(
        r#"
version: 1
keyboard: { mode: kitty }
menus:
  main:
    bindings:
      tab: { label: tab, action: config:reload }
      "ctrl+i": { label: ctrl i, action: config:reload }
      enter: { label: enter, action: config:reload }
      "ctrl+m": { label: ctrl m, action: config:reload }
      backspace: { label: backspace, action: config:reload }
      "ctrl+h": { label: ctrl h, action: config:reload }
      esc: { label: escape, action: config:reload }
      "ctrl+[": { label: ctrl bracket, action: config:reload }
      "ctrl+a": { label: ctrl a, action: config:reload }
      "ctrl+shift+a": { label: ctrl shift a, action: config:reload }
"#,
        None,
        KeyCapabilities::default(),
    )
    .expect("Kitty preserves the supplied key identities and modifiers");
}

#[test]
fn command_cwd_allows_literal_relative_paths() {
    let config = compile(
        r"
version: 1
menus:
  main:
    bindings:
      c:
        label: command
        action:
          type: command:execute
          program: echo
          cwd: relative/project
",
        None,
        KeyCapabilities::default(),
    )
    .expect("literal relative command cwd is supported");
    let binding = config
        .menu(&muxe_core::MenuId::new("main"))
        .unwrap()
        .bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "c")
        .unwrap();
    let muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Command(command)) =
        &binding.action
    else {
        panic!("expected command action");
    };
    assert_eq!(
        command.cwd.as_ref().unwrap().value.as_str(),
        Some("relative/project")
    );
}

#[test]
fn binding_lookup_indexes_the_authoritative_menu_payload_without_duplication() {
    let config = compile(
        r"
version: 1
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
        None,
        KeyCapabilities::default(),
    )
    .unwrap();
    let menu_binding = &config
        .menu(&muxe_core::MenuId::new("main"))
        .unwrap()
        .bindings[0];
    let indexed = config.binding(config.generation, menu_binding.id).unwrap();
    assert!(std::ptr::eq(menu_binding, indexed));
    assert!(
        config
            .binding(
                muxe_core::CompiledGeneration(config.generation.0 + 1),
                menu_binding.id
            )
            .is_none()
    );
}
