use muxe_core::{
    compile_yaml, CompiledGeneration, KeyCapabilities, MenuId, SourceId,
};

const REPRESENTATIVE: &str = r#"
version: 1
keyboard:
  mode: vt100
settings:
  timeout: 10s
  after_action: quit
menus:
  main:
    title: Muxe
    tags: [root]
    bindings:
      t:
        label: tabs
        action: menu:open tabs
      p:
        label: panes
        action:
          type: menu:open
          submenu:
            title: Pane actions
            tags: [pane-menu]
            settings:
              after_action: stay
            bindings:
              h:
                label: focus left
                action: pane:focus direction=left
              v:
                label: split right
                action: pane:split direction=right
      c:
        label: run tests
        action:
          type: command:execute
          program: cargo
          args: [test]
          cwd:
            $context: origin.pane.cwd
          env:
            CARGO_TERM_COLOR: always
        settings:
          execution:
            mode: await
            timeout: 2m
            on-timeout: cancel
            on-menu-control: detach
      s:
        label: send interrupt
        action:
          type: keyboard:send
          keys: [ctrl+c]
      i:
        label: insert greeting
        action:
          type: keyboard:send
          text: "hello\n"
      r:
        label: reload configuration
        settings:
          after_action: stay
        action: config:reload
  tabs:
    title: Tabs
    tags: [tabs]
    bindings:
      t:
        label: new tab
        action: tab:create
      r:
        label: rename tab
        action: tab:rename
      "1":
        label: focus tab 1
        action: tab:focus index=1
"#;

#[test]
fn compiles_the_design_representative_with_injections_and_inline_menu() {
    let config = compile_yaml(
        CompiledGeneration(7),
        SourceId::new("representative.yml"),
        REPRESENTATIVE,
        KeyCapabilities::default(),
        None,
    )
    .expect("design representative should compile");

    assert_eq!(config.generation, CompiledGeneration(7));
    assert!(config.menu(&MenuId::new("main")).is_some());
    assert!(config.menu(&MenuId::new("tabs")).is_some());
    assert_eq!(config.menus.len(), 3, "the inline submenu is compiled as a graph node");
    assert!(config
        .menu(&MenuId::new("main"))
        .expect("main menu")
        .bindings
        .iter()
        .any(|binding| binding.key.canonical_string() == "esc"));
}
