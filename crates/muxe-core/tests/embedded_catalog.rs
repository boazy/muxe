use std::collections::BTreeMap;
use std::fmt::Write as _;

use muxe_core::{
    Color, ColorAliasName, ColorSchemeName, ConfigDocument, SourceId, ThemeAssets, ThemeName,
};
use serde::Deserialize;

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

fn rgb(color: Color) -> [u8; 3] {
    match color {
        Color::Rgb { red, green, blue } => [red, green, blue],
        Color::Inherit => panic!("an explicit scheme must resolve to RGB"),
    }
}

fn luminance(color: Color) -> f64 {
    let [red, green, blue] = rgb(color).map(|channel| {
        let value = f64::from(channel) / 255.0;
        if value <= 0.04045 {
            value / 12.92
        } else {
            ((value + 0.055) / 1.055).powf(2.4)
        }
    });
    0.2126 * red + 0.7152 * green + 0.0722 * blue
}

fn contrast_ratio(foreground: Color, background: Color) -> f64 {
    let foreground = luminance(foreground);
    let background = luminance(background);
    (foreground.max(background) + 0.05) / (foreground.min(background) + 0.05)
}

#[test]
fn explicit_schemes_preserve_secondary_hierarchy_and_status_accents() {
    let catalog = ThemeAssets::default().compile_catalog();
    let default_scheme = ColorSchemeName::new("default");
    for name in catalog.color_scheme_names() {
        if name == &default_scheme {
            continue;
        }
        let pair = catalog.resolve(&ThemeName::new("default"), name).unwrap();
        let role = |name| pair.scheme.resolve(&ColorAliasName::new(name)).unwrap();
        let text = role("base.text");
        let background = role("base.background");
        let muted = role("base.muted");
        let hotkey = role("menu.hotkey");
        let separator = role("menu.separator");
        assert_ne!(
            muted, background,
            "{name}: secondary text must remain visible"
        );
        assert!(
            contrast_ratio(muted, background) < contrast_ratio(text, background),
            "{name}: secondary text must be subdued, not primary or emphasized text"
        );
        assert_ne!(hotkey, text, "{name}: keys need their own accent");
        for other in [text, hotkey, role("status.error")] {
            assert_ne!(
                separator, other,
                "{name}: separators must not collapse into text, keys or errors"
            );
        }

        // Distinct accents prevent the reported neutral-text substitution.
        // Hue choices belong to each canonical palette, including olive greens
        // and green-biased yellows; channel ordering cannot classify them reliably.
        for status in [
            "status.error",
            "status.pending",
            "status.blocked",
            "status.reload",
            "status.notice",
        ] {
            for neutral in [text, background, muted] {
                assert_ne!(
                    role(status),
                    neutral,
                    "{name}: {status} must retain an accent instead of neutral text"
                );
            }
        }
        for warning in ["status.pending", "status.blocked"] {
            assert_ne!(
                role(warning),
                role("status.error"),
                "{name}: warnings must remain distinct from errors"
            );
        }
    }
}

// Raw strings are confined to the catalog's serialization boundary. Resolve them
// through domain names before comparing metadata with the consumer's scheme.
#[derive(Deserialize)]
struct RawCatalog {
    variants: Vec<RawVariant>,
}

#[derive(Deserialize)]
struct RawVariant {
    id: String,
    palette: BTreeMap<String, String>,
    semantic: BTreeMap<String, String>,
    upstream_roles: RawUpstreamRoles,
    palette_usage: BTreeMap<String, Vec<String>>,
    contrast: RawContrast,
}

#[derive(Deserialize)]
struct RawUpstreamRoles {
    foreground: String,
    background: String,
}

#[derive(Deserialize)]
struct RawContrast {
    normal_text_reference: f64,
    background: String,
    slots: BTreeMap<String, RawContrastSlot>,
    chevron_badge: RawBadgeContrast,
}

#[derive(Deserialize)]
struct RawContrastSlot {
    palette_name: String,
    hex: String,
    ratio: f64,
    below_normal_text_reference: bool,
}

#[derive(Deserialize)]
struct RawBadgeContrast {
    foreground_name: String,
    foreground_hex: String,
    background_name: String,
    background_hex: String,
    ratio: f64,
    below_normal_text_reference: bool,
}

#[test]
fn catalog_metadata_matches_consumed_palettes_roles_and_measured_contrast() {
    // JSON is a YAML flow document; use the existing parser without adding a
    // second serialization dependency solely for embedded metadata.
    let raw: RawCatalog = serde_saphyr::from_str(include_str!("../assets/catalog.json")).unwrap();
    let catalog = ThemeAssets::default().compile_catalog();
    assert_eq!(raw.variants.len(), catalog.color_scheme_names().count() - 1);
    for variant in raw.variants {
        let name = ColorSchemeName::new(variant.id);
        let pair = catalog.resolve(&ThemeName::new("default"), &name).unwrap();
        let palette: BTreeMap<_, _> = variant
            .palette
            .into_iter()
            .map(|(alias, hex)| (ColorAliasName::new(alias), Color::parse(&hex).unwrap()))
            .collect();
        assert_eq!(
            pair.scheme.palette, palette,
            "{name}: complete source palette"
        );
        let semantic: BTreeMap<_, _> = variant
            .semantic
            .into_iter()
            .map(|(role, alias)| (ColorAliasName::new(role), ColorAliasName::new(alias)))
            .collect();
        let role = |name| pair.scheme.resolve(&ColorAliasName::new(name)).unwrap();
        assert_eq!(
            role("base.text"),
            palette[&ColorAliasName::new(variant.upstream_roles.foreground)],
            "{name}: keep the canonical foreground, even below the contrast reference"
        );
        let background = role("base.background");
        assert_eq!(
            background,
            palette[&ColorAliasName::new(variant.upstream_roles.background)],
            "{name}: keep the canonical background"
        );
        assert_eq!(pair.scheme.colors.len(), semantic.len());
        for (role, alias) in &semantic {
            assert_eq!(
                pair.scheme.resolve(role).unwrap(),
                palette[alias],
                "{name}: {role}"
            );
        }
        assert_eq!(variant.palette_usage.len(), palette.len());
        for (alias, roles) in variant.palette_usage {
            let alias = ColorAliasName::new(alias);
            let mut documented: Vec<_> = roles.into_iter().map(ColorAliasName::new).collect();
            documented.sort();
            let actual: Vec<_> = semantic
                .iter()
                .filter(|(_, chosen)| *chosen == &alias)
                .map(|(role, _)| role.clone())
                .collect();
            assert_eq!(documented, actual, "{name}: {alias} usage");
        }
        assert_eq!(
            Color::parse(&variant.contrast.background).unwrap(),
            background
        );
        assert_eq!(variant.contrast.slots.len(), semantic.len() - 1);
        for (role, slot) in variant.contrast.slots {
            let role = ColorAliasName::new(role);
            let color = pair.scheme.resolve(&role).unwrap();
            assert_eq!(semantic[&role], ColorAliasName::new(slot.palette_name));
            assert_eq!(color, Color::parse(&slot.hex).unwrap());
            let measured = contrast_ratio(color, background);
            assert!(
                (measured - slot.ratio).abs() < 0.000_001,
                "{name}: {role} contrast must describe the actual RGB pair"
            );
            assert_eq!(
                slot.below_normal_text_reference,
                measured < variant.contrast.normal_text_reference,
                "{name}: {role} must disclose contrast below the reference"
            );
        }
        let badge = variant.contrast.chevron_badge;
        let hotkey = role("menu.hotkey");
        assert_eq!(
            palette[&ColorAliasName::new(badge.foreground_name)],
            background
        );
        assert_eq!(palette[&ColorAliasName::new(badge.background_name)], hotkey);
        assert_eq!(Color::parse(&badge.foreground_hex).unwrap(), background);
        assert_eq!(Color::parse(&badge.background_hex).unwrap(), hotkey);
        let measured = contrast_ratio(background, hotkey);
        assert!(
            (measured - badge.ratio).abs() < 0.000_001,
            "{name}: badge contrast"
        );
        assert_eq!(
            badge.below_normal_text_reference,
            measured < variant.contrast.normal_text_reference,
            "{name}: inverted badges must disclose contrast below the reference"
        );
    }
}
