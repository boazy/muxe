use std::hint::black_box;

use muxe_core::{
    CompiledGeneration, ConditionProgram, ConfigDocument, KeyCapabilities, PagesContext, SourceId,
    SourceSpan, compile_yaml,
};

const COMPLETE_BASE: &str = r#"
version: 1

keyboard:
  mode: vt100

settings:
  timeout: 10s
  after_action: quit

  reload:
    watch: true
    debounce: 200ms

  execution:
    timeout: off
    on-timeout: detach
    on-menu-control: detach

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

const PAGER_CONDITION: &str = "pages.current < pages.count && pages.count > 1";

fn main() {
    divan::main();
}

#[divan::bench]
fn parse_config_document() {
    black_box(
        ConfigDocument::parse(SourceId::new("complete-base.yml"), black_box(COMPLETE_BASE))
            .expect("representative config parses"),
    );
}

#[divan::bench]
fn compile_config_yaml() {
    black_box(
        compile_yaml(
            CompiledGeneration(1),
            SourceId::new("complete-base.yml"),
            black_box(COMPLETE_BASE),
            KeyCapabilities::default(),
            None,
        )
        .expect("representative config compiles"),
    );
}

#[divan::bench]
fn compile_condition() {
    let span = SourceSpan::new(SourceId::new("condition"), 0, PAGER_CONDITION.len());
    black_box(
        ConditionProgram::compile(black_box(PAGER_CONDITION), span)
            .expect("pager condition compiles"),
    );
}

#[divan::bench]
fn evaluate_condition(bencher: divan::Bencher<'_, '_>) {
    let span = SourceSpan::new(SourceId::new("condition"), 0, PAGER_CONDITION.len());
    let program =
        ConditionProgram::compile(PAGER_CONDITION, span).expect("pager condition compiles");
    bencher.bench(|| {
        black_box(
            program
                .evaluate(black_box(PagesContext {
                    count: 4,
                    current: 2,
                }))
                .expect("pager condition evaluates"),
        )
    });
}
