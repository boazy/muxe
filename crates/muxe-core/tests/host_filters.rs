use muxe_core::{
    ActionValidation, ActionValidator, CompileInput, CompiledConfig, CompiledGeneration, Compiler,
    ConfigDiagnostic, ConfigDocument, DiagnosticCode, ExecutionCapabilities, KeyCapabilities,
    MenuId, MenuName, NativeActionCandidate, OriginHostKind, PaneAction, PortableAction, SourceId,
    SourceSpan, ThemeAssets,
};

struct HerdrValidator;
struct ZellijValidator;

fn accepted_action() -> ActionValidation {
    ActionValidation {
        execution: ExecutionCapabilities::ASYNCHRONOUS,
    }
}

fn unsupported_native(
    candidates: &[&NativeActionCandidate],
) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    Err(candidates
        .iter()
        .map(|candidate| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidAction,
                "this fixture does not support native actions",
                candidate.type_span.clone(),
            )
        })
        .collect())
}

impl ActionValidator for HerdrValidator {
    fn matches_host(&self, host: OriginHostKind) -> bool {
        host == OriginHostKind::Herdr
    }

    fn validate_portable(
        &self,
        action: &PortableAction,
        span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        if matches!(action, PortableAction::Pane(PaneAction::Create)) {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::InvalidAction,
                "this fixture does not support pane:create",
                span.clone(),
            ));
        }
        Ok(accepted_action())
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        unsupported_native(candidates)
    }
}

impl ActionValidator for ZellijValidator {
    fn matches_host(&self, host: OriginHostKind) -> bool {
        host == OriginHostKind::Zellij
    }

    fn validate_portable(
        &self,
        _action: &PortableAction,
        _span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        Ok(accepted_action())
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        unsupported_native(candidates)
    }
}

fn compile(
    base: &str,
    overlay: Option<&str>,
    validator: Option<&dyn ActionValidator>,
) -> Result<CompiledConfig, Vec<ConfigDiagnostic>> {
    Compiler.compile(
        CompileInput {
            generation: CompiledGeneration(21),
            base: ConfigDocument::parse(SourceId::new("filters.yml"), base).unwrap(),
            host_override: overlay
                .map(|yaml| ConfigDocument::parse(SourceId::new("override.yml"), yaml).unwrap()),
            key_capabilities: KeyCapabilities::default(),
            theme_assets: ThemeAssets::default(),
        },
        validator,
    )
}

fn menu_id(name: &str) -> MenuId {
    MenuId::named(MenuName::parse(name).unwrap())
}

fn labels<'a>(config: &'a CompiledConfig, menu: &str) -> Vec<&'a str> {
    config
        .menu(&menu_id(menu))
        .unwrap()
        .bindings
        .iter()
        .filter_map(|binding| binding.label.as_deref())
        .collect()
}

#[test]
fn binding_filters_select_the_host_and_exclusion_wins() {
    let yaml = r"
version: 1
menus:
  main:
    bindings:
      a: { label: only zellij, only-hosts: [zellij], action: 'pane:create' }
      b: { label: only herdr, only-hosts: [herdr], action: 'config:reload' }
      c: { label: not herdr, skip-hosts: [herdr], action: 'pane:create' }
      d: { label: not zellij, skip-hosts: [zellij], action: 'config:reload' }
      e:
        label: skip wins
        only-hosts: [herdr, zellij]
        skip-hosts: [herdr]
        action: pane:create
      f: { only-hosts: [], action: invalid-action }
      g: { label: unrestricted, skip-hosts: [], action: 'config:reload' }
";
    let herdr = compile(yaml, None, Some(&HerdrValidator)).unwrap();
    assert_eq!(
        labels(&herdr, "main"),
        ["only herdr", "not zellij", "unrestricted"]
    );
    let zellij = compile(yaml, None, Some(&ZellijValidator)).unwrap();
    assert_eq!(
        labels(&zellij, "main"),
        ["only zellij", "not herdr", "skip wins", "unrestricted"]
    );
}

#[test]
fn excluded_menus_remove_openers_and_do_not_compile_inline_descendants() {
    let yaml = r"
version: 1
menus:
  main:
    bindings:
      a: { label: named only, action: 'menu:open zellij-only' }
      b: { label: named skip, action: 'menu:open not-herdr' }
      c:
        label: inline only
        action:
          type: menu:open
          submenu:
            only-hosts: [zellij]
            bindings:
              x: { label: unsupported, action: 'native.test:unsupported' }
      d:
        skip-hosts: [herdr]
        action:
          type: menu:open
          submenu:
            bindings:
              x:
                action:
                  type: menu:open
                  submenu:
                    bindings:
                      bad key!: { action: invalid-action }
      e:
        label: available submenu
        action:
          type: menu:open
          submenu:
            only-hosts: [herdr]
            bindings:
              r: { label: reload, action: 'config:reload' }
  zellij-only:
    only-hosts: [zellij]
    bindings:
      x:
        action:
          type: menu:open
          submenu:
            bindings:
              x: { label: unsupported, action: 'native.test:unsupported' }
  not-herdr:
    skip-hosts: [herdr]
    bindings:
      bad key!: { action: invalid-action }
";
    let config = compile(yaml, None, Some(&HerdrValidator)).unwrap();
    assert_eq!(labels(&config, "main"), ["available submenu"]);
    assert!(config.menu(&menu_id("zellij-only")).is_none());
    assert!(config.menu(&menu_id("not-herdr")).is_none());
    let inline = config
        .menus
        .iter()
        .filter(|menu| menu.id.inline_id().is_some())
        .collect::<Vec<_>>();
    assert_eq!(inline.len(), 1);
    assert_eq!(
        inline[0]
            .bindings
            .iter()
            .filter_map(|binding| binding.label.as_deref())
            .collect::<Vec<_>>(),
        ["reload"]
    );
}

#[test]
fn menu_filters_support_empty_lists_and_combined_precedence() {
    let yaml = r"
version: 1
menus:
  main: { bindings: {} }
  denied:
    only-hosts: [herdr, zellij]
    skip-hosts: [herdr]
    bindings:
      x: { label: unsupported, action: 'pane:create' }
  nobody:
    only-hosts: []
    bindings:
      x: { action: invalid-action }
  everyone:
    skip-hosts: []
    bindings:
      r: { label: reload, action: 'config:reload' }
";
    let herdr = compile(yaml, None, Some(&HerdrValidator)).unwrap();
    assert!(herdr.menu(&menu_id("denied")).is_none());
    assert!(herdr.menu(&menu_id("nobody")).is_none());
    assert_eq!(labels(&herdr, "everyone"), ["reload"]);
    let zellij = compile(yaml, None, Some(&ZellijValidator)).unwrap();
    assert_eq!(labels(&zellij, "denied"), ["unsupported"]);
    assert!(zellij.menu(&menu_id("nobody")).is_none());
}

#[test]
fn filters_apply_to_effective_injections_and_host_overrides() {
    let yaml = r"
version: 1
inject:
  local-only:
    select: { type: id:exact, value: main }
    action:
      type: override
      bindings:
        x: { label: injected, skip-hosts: [herdr], action: 'pane:create' }
menus:
  main:
    only-hosts: [zellij]
    bindings:
      a: { label: overridden, only-hosts: [zellij], action: 'pane:create' }
";
    let overlay = r"
menus:
  main:
    only-hosts: [herdr]
    bindings:
      a:
        only-hosts: [herdr]
        action: config:reload
";
    let config = compile(yaml, Some(overlay), Some(&HerdrValidator)).unwrap();
    assert_eq!(labels(&config, "main"), ["overridden"]);
}

#[test]
fn navigation_defaults_preserve_whole_host_filtered_bindings() {
    let yaml = r"
version: 1
menus:
  main:
    bindings:
      left: { label: left, action: 'pane:focus direction=left' }
      right: { label: right, action: 'pane:focus direction=right' }
  split:
    bindings:
      left: { label: left, skip-hosts: [herdr], action: 'pane:split direction=left' }
      right: { label: right, action: 'pane:split direction=right' }
";
    let herdr = compile(yaml, None, Some(&HerdrValidator)).unwrap();
    let zellij = compile(yaml, None, Some(&ZellijValidator)).unwrap();
    for config in [&herdr, &zellij] {
        for (menu, keys) in [("main", &["left", "right"][..]), ("split", &["right"][..])] {
            for key in keys {
                let binding = config
                    .menu(&menu_id(menu))
                    .unwrap()
                    .bindings
                    .iter()
                    .find(|binding| binding.key.canonical_string() == *key)
                    .unwrap();
                assert!(!binding.hidden);
                assert_eq!(binding.conditions, muxe_core::BindingConditions::default());
                assert!(matches!(
                    binding.action,
                    muxe_core::ActionSpec::Portable(PortableAction::Pane(_))
                ));
            }
        }
        let pgdn = config
            .menu(&menu_id("main"))
            .unwrap()
            .bindings
            .iter()
            .find(|binding| binding.key.canonical_string() == "pgdn")
            .unwrap();
        assert!(pgdn.hidden);
        assert!(pgdn.conditions.include.is_some());
        assert!(matches!(
            pgdn.action,
            muxe_core::ActionSpec::Portable(PortableAction::Menu(muxe_core::MenuAction::PageNext))
        ));
    }
    assert!(
        !herdr
            .menu(&menu_id("split"))
            .unwrap()
            .bindings
            .iter()
            .any(|binding| binding.key.canonical_string() == "left")
    );
    let left = zellij
        .menu(&menu_id("split"))
        .unwrap()
        .bindings
        .iter()
        .find(|binding| binding.key.canonical_string() == "left")
        .unwrap();
    assert!(!left.hidden);
    assert_eq!(left.conditions, muxe_core::BindingConditions::default());
    assert!(matches!(
        left.action,
        muxe_core::ActionSpec::Portable(PortableAction::Pane(PaneAction::Split { .. }))
    ));
}

#[test]
fn included_actions_and_truly_unknown_menu_targets_still_fail() {
    let unsupported = "version: 1\nmenus:\n  main:\n    bindings:\n      x: { label: active, only-hosts: [herdr], action: 'pane:create' }\n";
    let errors = compile(unsupported, None, Some(&HerdrValidator)).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == DiagnosticCode::InvalidAction)
    );

    let unknown = "version: 1\nmenus:\n  main:\n    bindings:\n      x: { label: missing, action: 'menu:open missing' }\n  excluded:\n    skip-hosts: [herdr]\n    bindings: {}\n";
    let errors = compile(unknown, None, Some(&HerdrValidator)).unwrap_err();
    assert!(
        errors
            .iter()
            .any(|error| error.code == DiagnosticCode::InvalidMenuReference)
    );
}

#[test]
fn invalid_filter_values_report_the_offending_source() {
    for (field, value, offending) in [
        ("only-hosts", "herdr", "herdr"),
        ("skip-hosts", "[7]", "7"),
        ("only-hosts", "[tmux]", "tmux"),
        ("skip-hosts", "[Herdr]", "Herdr"),
    ] {
        let yaml =
            format!("version: 1\nmenus:\n  main:\n    {field}: {value}\n    bindings: {{}}\n");
        let errors = compile(&yaml, None, Some(&HerdrValidator)).unwrap_err();
        let diagnostic = errors
            .iter()
            .find(|error| error.code == DiagnosticCode::InvalidValue)
            .unwrap();
        assert!(diagnostic.labels.iter().any(|label| {
            label.span.source.as_str() == "filters.yml"
                && yaml[label.span.start..label.span.end].contains(offending)
        }));
    }
    let both = "version: 1\nmenus:\n  main:\n    only-hosts: [zellij]\n    skip-hosts: herdr\n    bindings: {}\n";
    assert!(
        compile(both, None, Some(&HerdrValidator))
            .unwrap_err()
            .iter()
            .any(|error| error.code == DiagnosticCode::InvalidValue)
    );
}

#[test]
fn host_filters_require_a_host_and_can_exclude_every_root() {
    let yaml = "version: 1\nmenus:\n  main:\n    only-hosts: []\n    bindings:\n      x: { action: invalid-action }\n";
    assert!(
        compile(yaml, None, None)
            .unwrap_err()
            .iter()
            .any(|error| error.code == DiagnosticCode::InvalidValue)
    );
    let config = compile(yaml, None, Some(&HerdrValidator)).unwrap();
    assert!(config.menu(&menu_id("main")).is_none());
    assert!(config.menus.is_empty());
}
