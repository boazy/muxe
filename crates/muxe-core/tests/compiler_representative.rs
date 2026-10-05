use muxe_core::{CompiledGeneration, KeyCapabilities, MenuId, MenuName, SourceId, compile_yaml};

fn id(name: &str) -> MenuId {
    MenuId::named(MenuName::parse(name).expect("valid test menu name"))
}

const COMPLETE_BASE: &str = include_str!("fixtures/representative.yml");

#[test]
fn compiles_representative_config_with_injections_and_inline_menu() {
    let config = compile_yaml(
        CompiledGeneration(7),
        SourceId::new("complete-base.yml"),
        COMPLETE_BASE,
        KeyCapabilities::default(),
        None,
    )
    .expect("representative configuration compiles");

    assert_eq!(config.generation, CompiledGeneration(7));
    assert!(config.menu(&id("main")).is_some());
    assert!(config.menu(&id("tabs")).is_some());
    assert_eq!(
        config.menus.len(),
        3,
        "the inline submenu is compiled as a graph node"
    );
    assert!(
        config
            .menu(&id("main"))
            .expect("main menu")
            .bindings
            .iter()
            .any(|binding| binding.key.canonical_string() == "esc")
    );
}
