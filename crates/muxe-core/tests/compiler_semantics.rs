use muxe_core::{
    CompileInput, CompiledGeneration, Compiler, ConfigDocument, DiagnosticCode, KeyCapabilities,
    MenuName, SourceId, ThemeAssets,
};
use std::sync::Mutex;

fn id(name: &str) -> muxe_core::MenuId {
    muxe_core::MenuId::named(MenuName::parse(name).expect("valid test menu name"))
}

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

    assert!(config.menu(&id("remove-me")).is_none());
    let main = config.menu(&id("main")).unwrap();
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
            .menu(&id("main"))
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
            .menu(&id("main"))
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
        .menu(&id("main"))
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
fn unknown_underscore_fields_report_source_aware_errors_after_merge() {
    let host = r"
menus:
  main:
    settings:
      _typo: true
";
    let diagnostics = compile(
        r"
version: 1
menus:
  main:
    bindings:
      r: { label: reload, action: config:reload }
",
        Some(host),
        KeyCapabilities::default(),
    )
    .expect_err("unknown underscore-prefixed fields must be rejected");

    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.code == DiagnosticCode::UnknownField)
        .expect("unknown underscore-prefixed field diagnostic");
    let span = &diagnostic.labels[0].span;
    assert_eq!(span.source.as_str(), "host.yml");
    let start = host.find("_typo").expect("typo key in host source");
    assert_eq!(span.start, start);
    assert_eq!(span.end, start + "_typo".len());
}

#[test]
fn user_authored_inline_marker_is_rejected_as_reserved() {
    let diagnostics = compile(
        r"
version: 1
menus:
  main:
    bindings:
      x:
        label: nested
        action:
          type: menu:open
          submenu:
            _muxe_inline_id: main#0
            bindings:
              q:
                label: quit
                action: menu:quit
",
        None,
        KeyCapabilities::default(),
    )
    .expect_err("user-authored inline marker must be rejected");

    let diagnostic = diagnostics
        .iter()
        .find(|diagnostic| diagnostic.message.contains("_muxe_inline_id"))
        .expect("reserved marker diagnostic");
    assert_eq!(diagnostic.code, DiagnosticCode::UnknownField);
    assert!(diagnostic.message.contains("reserved"));
    assert_eq!(diagnostic.labels[0].span.source.as_str(), "config.yml");
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
    let bindings = &config.menu(&id("main")).unwrap().bindings;
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
fn portable_action_schema_preserves_failure_diagnostics() {
    struct Case {
        name: &'static str,
        yaml: &'static str,
        code: DiagnosticCode,
        message: &'static str,
        span_text: &'static str,
    }

    let cases = [
        Case {
            name: "unknown mapping field",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: bad\n        action:\n          type: pane:move\n          direction: left\n          bogus: true\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "unknown action argument `bogus`",
            span_text: "bogus",
        },
        Case {
            name: "missing required mapping field",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: bad\n        action:\n          type: session:attach\n",
            code: DiagnosticCode::MissingField,
            message: "action requires `name`",
            span_text: "session:attach",
        },
        Case {
            name: "wrong mapping value type",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: bad\n        action:\n          type: pane:fullscreen\n          enabled: yes\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "enabled must be boolean",
            span_text: "yes",
        },
        Case {
            name: "wrong context type",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: bad\n        action:\n          type: pane:resize\n          direction: { $context: origin.pane.id }\n",
            code: DiagnosticCode::ContextTypeMismatch,
            message: "pane direction has an incompatible context reference type",
            span_text: "",
        },
        Case {
            name: "missing exactly-one value",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a: { label: bad, action: pane:move }\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "action requires exactly one of `index` or `direction`",
            span_text: "pane:move",
        },
        Case {
            name: "conflicting exactly-one values",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: bad\n        action:\n          type: pane:move\n          index: 1\n          direction: left\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "action requires exactly one of `index` or `direction`",
            span_text: "1",
        },
        Case {
            name: "missing required companion",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: bad\n        action:\n          type: tab:create\n          args: [one]\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "tab args requires `program`",
            span_text: "",
        },
        Case {
            name: "positional argument after named argument",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a: { label: bad, action: pane:move direction=left 1 }\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "positional action arguments must precede named arguments",
            span_text: "pane:move direction=left 1",
        },
        Case {
            name: "too many positional arguments",
            yaml: "version: 1\nmenus:\n  main:\n    bindings:\n      a: { label: bad, action: pane:move left right }\n",
            code: DiagnosticCode::InvalidActionArguments,
            message: "too many positional action arguments",
            span_text: "pane:move left right",
        },
    ];

    for case in cases {
        let diagnostics =
            compile(case.yaml, None, KeyCapabilities::default()).expect_err(case.name);
        assert_eq!(diagnostics.len(), 1, "{}", case.name);
        let diagnostic = &diagnostics[0];
        assert_eq!(diagnostic.code, case.code, "{}", case.name);
        assert_eq!(diagnostic.message, case.message, "{}", case.name);
        assert_eq!(diagnostic.labels.len(), 1, "{}", case.name);
        let span = &diagnostic.labels[0].span;
        assert_eq!(span.source.as_str(), "config.yml", "{}", case.name);
        if case.span_text.is_empty() {
            assert_eq!((span.start, span.end), (0, 0), "{}", case.name);
        }
        assert_eq!(
            &case.yaml[span.start..span.end],
            case.span_text,
            "{} span={span:?}",
            case.name
        );
    }
}

const COMPLETE_CUSTOM_THEME_YAML: &str = r#"
common:
  styles:
    default: { foreground: base.text }
menu:
  styles:
    title: { foreground: menu.hotkey, bold: true }
  templates:
    cell: "{{ title }}"
    breadcrumbs: "{{ crumbs }}"
    pagination:
      full: "{{ pages.current }}/{{ pages.count }}"
      short: "{{ pages.current }}/{{ pages.count }}"
    status: "{{ message }}"
"#;

fn custom_theme_assets(theme_yaml: &str) -> ThemeAssets {
    ThemeAssets {
        themes: std::collections::BTreeMap::from([(
            "custom".to_owned(),
            document("themes/custom.yml", theme_yaml),
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
    }
}

fn custom_theme_config(theme_yaml: &str) -> muxe_core::CompiledConfig {
    Compiler
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
                theme_assets: custom_theme_assets(theme_yaml),
            },
            None,
        )
        .expect("selected source-tracked assets should compile and pair")
}

#[test]
fn incomplete_theme_is_rejected_before_attachment() {
    let diagnostics = Compiler
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
                theme_assets: custom_theme_assets(
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
            },
            None,
        )
        .expect_err("a theme with only `cell` must not compile");
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("breadcrumbs")),
        "missing `breadcrumbs` must be named, got {diagnostics:?}"
    );
}

#[test]
fn whitespace_controlled_loader_tags_are_rejected_at_compile() {
    for (name, cell) in [
        ("dash include", "{%- include 'status' %}"),
        ("plus include", "{%+ include 'status' %}"),
        ("dash import", "{%- import 'status' as registered %}"),
        ("dash from-import", "{%- from 'status' import registered %}"),
    ] {
        let theme_yaml = format!(
            r#"
common:
  styles:
    default: {{ foreground: base.text }}
menu:
  styles:
    title: {{ foreground: menu.hotkey, bold: true }}
  templates:
    cell: "{cell}"
    breadcrumbs: "{{{{ crumbs }}}}"
    pagination:
      full: "{{{{ pages.current }}}}{{{{ pages.count }}}}"
      short: "{{{{ pages.current }}}}{{{{ pages.count }}}}"
    status: "ok"
"#,
        );
        let diagnostics = Compiler
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
                    theme_assets: custom_theme_assets(&theme_yaml),
                },
                None,
            )
            .expect_err("a loader-backed cell must not compile");
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("loader-backed")),
            "{name} must be rejected as loader-backed, got {diagnostics:?}"
        );
    }
}

#[test]
fn selected_theme_and_color_scheme_are_carried_to_attachment() {
    let config = custom_theme_config(COMPLETE_CUSTOM_THEME_YAML);
    assert_eq!(config.theme_selection.theme, "custom");
    assert_eq!(config.theme_selection.color_scheme, "ink");
    let attachment = config
        .attachment_view(&id("main"), &config.theme_selection)
        .expect("compiled main menu has an attachment");
    assert_eq!(attachment.theme_selection, config.theme_selection);
    assert_eq!(attachment.theme.as_ref(), &config.theme);
}

#[test]
fn unused_malformed_asset_does_not_block_generation_but_preserves_attach_diagnostic() {
    let mut assets = custom_theme_assets(COMPLETE_CUSTOM_THEME_YAML);
    assets.themes.insert(
        "unused-malformed".to_owned(),
        document("themes/unused-malformed.yml", "common:\n  unknown: true\n"),
    );
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
      q: { label: quit, action: menu:quit }
",
                ),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: assets,
            },
            None,
        )
        .expect("unused malformed assets do not block the selected generation pair");
    let error = config
        .resolve_theme(&muxe_core::ThemeSelection {
            theme: "unused-malformed".to_owned(),
            color_scheme: "ink".to_owned(),
        })
        .expect_err("selecting malformed asset reports its pinned diagnostics");
    let muxe_core::ThemeSelectionError::InvalidTheme { diagnostics, .. } = error else {
        panic!("expected exact malformed-theme diagnostics");
    };
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, DiagnosticCode::UnknownField);
    assert_eq!(
        diagnostics[0].labels[0].span.source.as_str(),
        "themes/unused-malformed.yml"
    );
}

#[test]
fn attachment_view_distinguishes_missing_menu_from_theme_resolution() {
    let config = custom_theme_config(COMPLETE_CUSTOM_THEME_YAML);
    assert!(matches!(
        config.attachment_view(&id("missing"), &config.theme_selection),
        Err(muxe_core::AttachmentViewError::MissingMenu(_))
    ));
    let unknown = muxe_core::ThemeSelection {
        theme: "missing".to_owned(),
        color_scheme: config.theme_selection.color_scheme.clone(),
    };
    assert!(matches!(
        config.attachment_view(&id("main"), &unknown),
        Err(muxe_core::AttachmentViewError::Theme(
            muxe_core::ThemeSelectionError::UnknownTheme { .. }
        ))
    ));
    let resolved = config
        .resolve_theme(&config.theme_selection)
        .expect("selected pair resolves");
    let other = custom_theme_config(COMPLETE_CUSTOM_THEME_YAML);
    assert!(matches!(
        other.attachment_view_resolved(&id("main"), &resolved),
        Err(muxe_core::AttachmentViewError::Theme(
            muxe_core::ThemeSelectionError::StaleResolution
        ))
    ));
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
            ..
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

#[test]
fn keyboard_send_rejects_an_empty_key_sequence() {
    let diagnostics = compile(
        r"
version: 1
menus:
  main:
    bindings:
      x:
        label: empty
        action:
          type: keyboard:send
          keys: []
",
        None,
        KeyCapabilities::default(),
    )
    .expect_err("empty key sequence must be rejected");
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::InvalidActionArguments)
    );
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
        .menu(&id("main"))
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
        ..
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

#[expect(
    clippy::too_many_lines,
    reason = "single exhaustive alias-collision table; splitting would scatter the collision matrix"
)]
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
fn creation_actions_preserve_named_focus_and_exact_command_vectors() {
    let config = compile(
        r"
version: 1
menus:
  main:
    bindings:
      t:
        label: logs
        action:
          type: tab:create
          name: logs
          focus: false
          program: tail
          args: [-f, app.log]
          cwd: /srv/app
      p:
        label: monitor
        action:
          type: pane:split
          direction: right
          focus: true
          program: htop
          args: []
          cwd: /srv/app
",
        None,
        KeyCapabilities::default(),
    )
    .expect("creation actions accept their declared fields");
    let tab = portable_main_action(&config, "t");
    let muxe_core::PortableAction::Tab(muxe_core::TabAction::Create {
        name,
        focus,
        command,
        ..
    }) = tab
    else {
        panic!("expected tab creation action");
    };
    assert_eq!(
        name.as_ref().and_then(|value| value.value.as_str()),
        Some("logs")
    );
    assert_eq!(
        focus.as_ref().and_then(|value| value.value.as_bool()),
        Some(false)
    );
    assert_eq!(
        command
            .program
            .as_ref()
            .and_then(|value| value.value.as_str()),
        Some("tail")
    );
    assert_eq!(command.args.len(), 2);
    assert_eq!(
        command.cwd.as_ref().and_then(|value| value.value.as_str()),
        Some("/srv/app")
    );
    let pane = portable_main_action(&config, "p");
    let muxe_core::PortableAction::Pane(muxe_core::PaneAction::Split {
        direction,
        focus,
        command,
    }) = pane
    else {
        panic!("expected pane split action");
    };
    assert_eq!(
        direction.as_ref().and_then(|value| value.value.as_str()),
        Some("right")
    );
    assert_eq!(
        focus.as_ref().and_then(|value| value.value.as_bool()),
        Some(true)
    );
    assert_eq!(
        command
            .program
            .as_ref()
            .and_then(|value| value.value.as_str()),
        Some("htop")
    );
    assert!(command.args.is_empty());
    assert_eq!(
        command.cwd.as_ref().and_then(|value| value.value.as_str()),
        Some("/srv/app")
    );
}

#[test]
fn creation_args_require_a_program() {
    let diagnostics = compile(
        r"
version: 1
menus:
  main:
    bindings:
      p:
        label: bad
        action:
          type: pane:split
          args: [--watch]
",
        None,
        KeyCapabilities::default(),
    )
    .expect_err("args without a creation program must fail configuration");
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic.code == muxe_core::DiagnosticCode::InvalidActionArguments
            && diagnostic.message.contains("args requires `program`")
    }));
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
        .menu(&id("main"))
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
    let menu_binding = &config.menu(&id("main")).unwrap().bindings[0];
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

#[test]
fn non_boolean_conditions_are_rejected_at_compile_time_without_publication() {
    for source in ["true && 1", "1", "1 < true", "!1"] {
        let yaml = format!(
            "version: 1\nmenus:\n  main:\n    bindings:\n      a:\n        label: gated\n        action: menu:quit\n        conditions:\n          include: '{source}'\n"
        );
        let diagnostics = compile(&yaml, None, KeyCapabilities::default()).unwrap_err();
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::InvalidCondition),
            "{source}: {diagnostics:?}"
        );
    }
}

#[test]
fn cel_arithmetic_and_conditional_conditions_compile_for_pager_bindings() {
    let yaml = "version: 1\nmenus:\n  main:\n    bindings:\n      left:\n        label: prev\n        action: menu.page:prev\n        conditions:\n          include: 'pages.count + 1 > 2'\n";
    let config = compile(yaml, None, KeyCapabilities::default())
        .expect("arithmetic conditions should compile");
    let binding = config
        .menu(&id("main"))
        .unwrap()
        .bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "left")
        .unwrap();
    assert!(binding.conditions.include.is_some());
    assert!(binding.conditions.include.as_ref().unwrap().uses_pages());
}

const MINIMAL_BINDING: &str = "      a:\n        label: alpha\n        action: menu:quit\n";

fn empty_name_document() -> muxe_core::ConfigDocument {
    use muxe_core::{
        ConfigDocument, ConfigField, ConfigValue, ConfigValueKind, SourceId, SourceSpan,
    };
    let source = SourceId::new("empty.yml");
    ConfigDocument {
        source: source.clone(),
        text: "".into(),
        root: ConfigValue {
            span: SourceSpan::new(source.clone(), 0, 0),
            kind: ConfigValueKind::Mapping(vec![
                ConfigField {
                    name: "version".to_owned(),
                    name_span: SourceSpan::new(source.clone(), 0, 0),
                    value: ConfigValue {
                        span: SourceSpan::new(source.clone(), 0, 0),
                        kind: ConfigValueKind::Integer(1),
                    },
                },
                ConfigField {
                    name: "menus".to_owned(),
                    name_span: SourceSpan::new(source.clone(), 0, 0),
                    value: ConfigValue {
                        span: SourceSpan::new(source.clone(), 0, 0),
                        kind: ConfigValueKind::Mapping(vec![ConfigField {
                            name: String::new(),
                            name_span: SourceSpan::new(source.clone(), 10, 10),
                            value: ConfigValue {
                                span: SourceSpan::new(source, 10, 10),
                                kind: ConfigValueKind::Mapping(vec![]),
                            },
                        }]),
                    },
                },
            ]),
        },
    }
}

#[test]
fn quoted_whitespace_menu_name_compiles_and_round_trips_through_wire() {
    let yaml = "version: 1\nmenus:\n  \"my menu\":\n    bindings:\n      a:\n        label: alpha\n        action: menu:quit\n";
    let config = compile(yaml, None, KeyCapabilities::default()).expect("whitespace name compiles");
    let root = id("my menu");
    let view = config
        .attachment_view(&root, &config.theme_selection)
        .expect("whitespace root view");
    assert_eq!(view.menu.root, root);
    // The wire half (validation + lossless variant round-trip of this exact
    // compiled view) is covered by
    // `whitespace_compiled_view_passes_wire_validation_and_round_trips` in
    // `muxe-broker`, which owns the core+protocol dependency edge.
}

#[test]
fn empty_and_control_menu_names_are_rejected_at_compile_time_naming_the_menu() {
    // Control characters survive YAML parsing as string keys, so they reach
    // the named-menu domain check (empty keys do not parse as strings and are
    // rejected earlier with "mapping keys must be strings").
    for name in ["bad\x07name", "bad\u{7f}name"] {
        let yaml = format!("version: 1\nmenus:\n  \"{name}\":\n    bindings:\n{MINIMAL_BINDING}");
        let diagnostics = compile(&yaml, None, KeyCapabilities::default()).unwrap_err();
        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.code == muxe_core::DiagnosticCode::InvalidValue
                    && format!("{diagnostic:?}").contains("(control)")
            }),
            "name {name:?}: {diagnostics:?}"
        );
    }
    // The empty name cannot survive YAML key parsing, so hand-build the
    // document and assert the compiler's own empty-name path fires with a
    // diagnostic naming it.
    let diagnostics = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(9),
                base: empty_name_document(),
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets::default(),
            },
            None,
        )
        .unwrap_err();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, muxe_core::DiagnosticCode::InvalidValue);
    assert!(
        format!("{:?}", diagnostics[0]).contains("(empty)"),
        "empty diagnostic: {diagnostics:?}"
    );
}

#[test]
fn named_main_at_zero_coexists_with_inline_submenu_as_distinct_identities() {
    let yaml = "version: 1\nmenus:\n  main:\n    bindings:\n      x:\n        label: tools\n        action:\n          type: menu:open\n          submenu:\n            bindings:\n              q:\n                label: quit\n                action: menu:quit\n  \"main@0\":\n    bindings:\n      a:\n        label: alpha\n        action: menu:quit\n";
    let config = compile(yaml, None, KeyCapabilities::default()).expect("collision-free compile");
    let inline_targets: Vec<muxe_core::MenuId> = config
        .menu(&id("main"))
        .expect("main")
        .bindings
        .iter()
        .filter_map(|binding| match &binding.action {
            muxe_core::ActionSpec::Portable(muxe_core::PortableAction::Menu(
                muxe_core::MenuAction::Open(muxe_core::MenuTarget::Inline(target)),
            )) => Some(muxe_core::MenuId::inline(target.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(inline_targets.len(), 1);
    assert_ne!(inline_targets[0], id("main@0"));
    assert!(config.menu(&id("main@0")).is_some());
    let view = config
        .attachment_view(&id("main"), &config.theme_selection)
        .expect("root view");
    assert_eq!(view.menu.menus.len(), 3);
}

#[test]
fn duplicate_named_menu_is_rejected_with_both_spans_not_first_wins() {
    use muxe_core::{ConfigField, ConfigValue, ConfigValueKind, SourceId, SourceSpan};
    let source = SourceId::new("dup.yml");
    let span = |start: usize, end: usize| SourceSpan::new(source.clone(), start, end);
    let binding = || ConfigValue {
        span: span(0, 0),
        kind: ConfigValueKind::Mapping(vec![ConfigField {
            name: "a".to_owned(),
            name_span: span(0, 0),
            value: ConfigValue {
                span: span(0, 0),
                kind: ConfigValueKind::Mapping(vec![]),
            },
        }]),
    };
    let menu = |at: usize| ConfigField {
        name: "main".to_owned(),
        name_span: span(at, at + 4),
        value: ConfigValue {
            span: span(at, at + 4),
            kind: ConfigValueKind::Mapping(vec![ConfigField {
                name: "bindings".to_owned(),
                name_span: span(at, at + 4),
                value: binding(),
            }]),
        },
    };
    let base = ConfigDocument {
        source: source.clone(),
        text: "".into(),
        root: ConfigValue {
            span: span(0, 0),
            kind: ConfigValueKind::Mapping(vec![
                ConfigField {
                    name: "version".to_owned(),
                    name_span: span(0, 0),
                    value: ConfigValue {
                        span: span(0, 0),
                        kind: ConfigValueKind::Integer(1),
                    },
                },
                ConfigField {
                    name: "menus".to_owned(),
                    name_span: span(0, 0),
                    value: ConfigValue {
                        span: span(0, 0),
                        kind: ConfigValueKind::Mapping(vec![menu(10), menu(20)]),
                    },
                },
            ]),
        },
    };
    let diagnostics = Compiler
        .compile(
            CompileInput {
                generation: CompiledGeneration(9),
                base,
                host_override: None,
                key_capabilities: KeyCapabilities::default(),
                theme_assets: ThemeAssets::default(),
            },
            None,
        )
        .unwrap_err();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(
        diagnostics[0].code,
        muxe_core::DiagnosticCode::DuplicateYamlKey
    );
    let rendered = format!("{:?}", diagnostics[0]);
    assert!(rendered.contains("main"), "{rendered}");
}
