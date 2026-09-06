use muxe_core::{
    CompileInput, CompiledGeneration, Compiler, ConfigDocument, KeyCapabilities, SourceId,
};

fn document(name: &str, yaml: &str) -> ConfigDocument {
    ConfigDocument::parse(SourceId::new(name), yaml).unwrap()
}

fn compile(base: &str, host: Option<&str>, capabilities: KeyCapabilities) -> Result<muxe_core::CompiledConfig, Vec<muxe_core::ConfigDiagnostic>> {
    Compiler.compile(
        CompileInput {
            generation: CompiledGeneration(9),
            base: document("config.yml", base),
            host_override: host.map(|yaml| document("host.yml", yaml)),
            key_capabilities: capabilities,
        },
        None,
    )
}

#[test]
fn ordered_host_merge_removes_injected_binding_and_replaces_settings() {
    let config = compile(
        r#"
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
"#,
        Some(
            r#"
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
"#,
        ),
        KeyCapabilities::default(),
    )
    .expect("merged configuration should compile");

    assert!(config.menu(&muxe_core::MenuId::new("remove-me")).is_none());
    let main = config.menu(&muxe_core::MenuId::new("main")).unwrap();
    assert!(main.bindings.iter().all(|binding| binding.key.canonical_string() != "esc"));
    assert!(main.bindings.iter().all(|binding| binding.key.canonical_string() != "backspace"));
    let action = main.bindings.iter().find(|binding| binding.key.canonical_string() == "a").unwrap();
    assert_eq!(action.settings.after_action, muxe_core::AfterAction::Quit);
}

#[test]
fn cycles_and_invalid_context_types_are_rejected_with_semantic_codes() {
    let cycle = compile(
        r#"
version: 1
menus:
  a:
    bindings: { a: { label: to-b, action: menu:open b } }
  b:
    bindings: { b: { label: to-a, action: menu:open a } }
"#,
        None,
        KeyCapabilities::default(),
    )
    .unwrap_err();
    assert!(cycle.iter().any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::MenuCycle));

    let mismatch = compile(
        r#"
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
"#,
        None,
        KeyCapabilities::default(),
    )
    .unwrap_err();
    assert!(mismatch.iter().any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::ContextTypeMismatch));
}

#[test]
fn kitty_capability_validation_keeps_documented_ctrl_l_repeat_path() {
    let host = KeyCapabilities { event_types: true, alternate_keys: true, all_keys_as_escape_codes: false };
    let valid = compile(
        r#"
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
"#,
        None,
        host,
    );
    assert!(valid.is_ok(), "the documented Herdr ctrl+l repeat case is valid");

    let invalid = compile(
        r#"
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
"#,
        None,
        host,
    )
    .unwrap_err();
    assert!(invalid.iter().any(|diagnostic| diagnostic.code == muxe_core::DiagnosticCode::KeyCapability));
}

#[test]
fn source_aware_yaml_and_unknown_field_errors_keep_the_candidate_inactive() {
    let error = ConfigDocument::parse(SourceId::new("bad.yml"), "version: 1\nmenus: [\n")
        .expect_err("malformed YAML must not produce a candidate");
    assert_eq!(error.code, muxe_core::DiagnosticCode::YamlSyntax);
    assert_eq!(error.labels[0].span.source.as_str(), "bad.yml");
}
