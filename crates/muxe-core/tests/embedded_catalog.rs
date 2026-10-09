use std::fmt::Write as _;

use muxe_core::{
    Color, ColorAliasName, ColorSchemeName, ConfigDocument, SourceId, ThemeAssets, ThemeName,
};

const SEMANTICS: [&str; 10] = [
    "base.text",
    "base.background",
    "base.muted",
    "menu.hotkey",
    "menu.separator",
    "status.error",
    "status.pending",
    "status.blocked",
    "status.reload",
    "status.notice",
];

#[test]
fn embedded_catalog_is_complete_and_every_pair_resolves() {
    let catalog = ThemeAssets::default().compile_catalog();
    assert_eq!(catalog.theme_names().count(), 5);
    assert_eq!(catalog.color_scheme_names().count(), 31);
    for theme in catalog.theme_names() {
        for scheme in catalog.color_scheme_names() {
            let pair = catalog
                .resolve(theme, scheme)
                .expect("every embedded pair is valid");
            for name in SEMANTICS {
                pair.scheme
                    .resolve(&ColorAliasName::new(name))
                    .expect("semantic slot exists");
            }
        }
    }
}

#[test]
fn terminal_inheriting_default_resolves_semantic_roles_to_terminal_defaults() {
    let catalog = ThemeAssets::default().compile_catalog();
    let pair = catalog
        .resolve(&ThemeName::new("default"), &ColorSchemeName::new("default"))
        .expect("default pair resolves");
    for name in SEMANTICS {
        assert_eq!(
            pair.scheme
                .resolve(&ColorAliasName::new(name))
                .expect("default semantic role"),
            Color::Inherit,
            "{name} must retain terminal inheritance rather than force RGB",
        );
    }
}

#[test]
fn user_replacements_are_whole_assets_including_default() {
    let mut assets = ThemeAssets::default();
    assets.themes.insert(ThemeName::new("brackets"), ConfigDocument::parse(
        SourceId::new("user brackets.yml"), "menu:\n  templates:\n    cell: user\n    breadcrumbs: user\n    pagination.full: user\n    pagination.short: user\n    status: user\n",
    ).expect("user YAML parses"));
    let mut yaml = "title: User default\npalette:\n  ink: '#123456'\ncolors:\n".to_owned();
    for name in SEMANTICS {
        writeln!(yaml, "  {name}: ink").expect("write to String");
    }
    assets.color_schemes.insert(
        ColorSchemeName::new("default"),
        ConfigDocument::parse(SourceId::new("user default.yml"), yaml).expect("user YAML parses"),
    );
    let catalog = assets.compile_catalog();
    let pair = catalog
        .resolve(
            &ThemeName::new("brackets"),
            &ColorSchemeName::new("default"),
        )
        .expect("user replacements resolve");
    assert_eq!(pair.theme.menu.templates["cell"], "user");
    assert_eq!(pair.scheme.title, "User default");
    assert_eq!(pair.scheme.palette.len(), 1);
    assert_eq!(catalog.theme_names().count(), 5);
    assert_eq!(catalog.color_scheme_names().count(), 31);
}

#[test]
fn malformed_user_replacement_never_falls_back_to_embedded_asset() {
    let mut assets = ThemeAssets::default();
    assets.themes.insert(
        ThemeName::new("brackets"),
        ConfigDocument::parse(SourceId::new("user brackets.yml"), "unknown-field: true\n")
            .expect("well-formed YAML"),
    );
    let catalog = assets.compile_catalog();
    assert!(
        catalog
            .resolve(&ThemeName::new("brackets"), &ColorSchemeName::new("nord"))
            .is_err()
    );
    assert!(
        catalog
            .resolve(&ThemeName::new("dots"), &ColorSchemeName::new("nord"))
            .is_ok()
    );
}
