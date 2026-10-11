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

fn hue_degrees(color: Color) -> f64 {
    let [red_channel, green_channel, blue_channel] = rgb(color);
    let maximum_channel = red_channel.max(green_channel).max(blue_channel);
    let minimum_channel = red_channel.min(green_channel).min(blue_channel);
    let chroma = f64::from(maximum_channel - minimum_channel);
    assert!(chroma > 0.0, "status colors must retain a hue");
    let red = f64::from(red_channel);
    let green = f64::from(green_channel);
    let blue = f64::from(blue_channel);
    let sector = if maximum_channel == red_channel {
        (green - blue) / chroma
    } else if maximum_channel == green_channel {
        (blue - red) / chroma + 2.0
    } else {
        (red - green) / chroma + 4.0
    };
    (sector * 60.0).rem_euclid(360.0)
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
        for other in [text, muted, hotkey, role("status.error")] {
            assert_ne!(
                separator, other,
                "{name}: separators must not collapse into text, secondary labels, keys or errors"
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
        let states = [
            role("status.error"),
            role("status.pending"),
            role("status.blocked"),
            role("status.reload"),
            role("status.notice"),
        ];
        for (index, state) in states.iter().enumerate() {
            for other in &states[index + 1..] {
                // A small guard against identical-hue ochres, even when their
                // brightness differs. This is not a visual acceptance metric.
                let distance = (hue_degrees(*state) - hue_degrees(*other)).abs();
                assert!(
                    distance.min(360.0 - distance) >= 5.0,
                    "{name}: states must not differ only in brightness"
                );
            }
        }
    }
}
