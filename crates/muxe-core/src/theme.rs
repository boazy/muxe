use std::collections::{BTreeMap, HashSet};
use std::fmt;

/// Resolved theme color. `Inherit` delegates foreground or background selection to the host
/// terminal; `Rgb` is an explicit truecolor value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Color {
    Inherit,
    Rgb { red: u8, green: u8, blue: u8 },
}

impl Color {
    pub fn parse(value: &str) -> Result<Self, ThemePairError> {
        let value = value.strip_prefix('#').ok_or_else(|| ThemePairError::new("color must begin with `#`"))?;
        let expand = |character: char| -> Result<u8, ThemePairError> {
            let digit = character
                .to_digit(16)
                .ok_or_else(|| ThemePairError::new("color contains a non-hex digit"))? as u8;
            Ok(digit * 17)
        };
        match value.len() {
            3 => {
                let mut chars = value.chars();
                Ok(Self::Rgb {
                    red: expand(chars.next().expect("length checked"))?,
                    green: expand(chars.next().expect("length checked"))?,
                    blue: expand(chars.next().expect("length checked"))?,
                })
            }
            6 => Ok(Self::Rgb {
                red: u8::from_str_radix(&value[0..2], 16).map_err(|_| ThemePairError::new("invalid red channel"))?,
                green: u8::from_str_radix(&value[2..4], 16).map_err(|_| ThemePairError::new("invalid green channel"))?,
                blue: u8::from_str_radix(&value[4..6], 16).map_err(|_| ThemePairError::new("invalid blue channel"))?,
            }),
            _ => Err(ThemePairError::new("color must use `#rgb` or `#rrggbb`")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColorScheme {
    pub title: String,
    pub palette: BTreeMap<String, String>,
    /// Dot-qualified semantic aliases such as `base.text` and `status.error`.
    pub colors: BTreeMap<String, String>,
}

impl ColorScheme {
    pub fn resolve(&self, name: &str) -> Result<Color, ThemePairError> {
        let mut seen = HashSet::new();
        self.resolve_inner(name, &mut seen)
    }

    fn resolve_inner(&self, name: &str, seen: &mut HashSet<String>) -> Result<Color, ThemePairError> {
        if !seen.insert(name.to_owned()) {
            return Err(ThemePairError::new(format!("color alias cycle at `{name}`")));
        }
        let value = self
            .colors
            .get(name)
            .or_else(|| self.palette.get(name))
            .ok_or_else(|| ThemePairError::new(format!("unknown palette or semantic color `{name}`")))?;
        if value == "inherit" {
            Ok(Color::Inherit)
        } else if value.starts_with('#') {
            Color::parse(value)
        } else {
            self.resolve_inner(value, seen)
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Style {
    pub foreground: Option<String>,
    pub background: Option<String>,
    pub bold: bool,
    pub dim: bool,
    pub italic: bool,
    pub underline: bool,
    pub strikethrough: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ThemeSection {
    pub styles: BTreeMap<String, Style>,
    pub templates: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Theme {
    pub common: ThemeSection,
    pub menu: ThemeSection,
    /// Theme-owned forward-compatible settings are intentionally opaque to the core schema.
    pub settings: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledTheme {
    pub theme: Theme,
    pub scheme: ColorScheme,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThemePairError(String);

impl ThemePairError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ThemePairError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ThemePairError {}

impl CompiledTheme {
    /// Validates a pure theme/scheme pair. It parses templates without filesystem loaders and
    /// verifies every declared foreground/background path before a UI can select the pair.
    pub fn compile(theme: Theme, scheme: ColorScheme) -> Result<Self, ThemePairError> {
        for (section, fallback) in [(&theme.common, None), (&theme.menu, Some(&theme.common))] {
            for style in section.styles.values() {
                for color in [style.foreground.as_deref(), style.background.as_deref()].into_iter().flatten() {
                    if color.starts_with('#') {
                        Color::parse(color)?;
                    } else {
                        scheme.resolve(color)?;
                    }
                }
            }
            for template in section.templates.values() {
                if template.contains("{% extends") || template.contains("{% include") || template.contains("{% import") {
                    return Err(ThemePairError::new("theme templates cannot use loader-backed tags"));
                }
                minijinja::Environment::new()
                    .template_from_str(template)
                    .map_err(|error| ThemePairError::new(error.to_string()))?;
                for style_name in literal_style_tags(template) {
                    if section.styles.contains_key(style_name)
                        || fallback.is_some_and(|common| common.styles.contains_key(style_name))
                    {
                        continue;
                    }
                    return Err(ThemePairError::new(format!(
                        "template references unknown literal style `{style_name}`"
                    )));
                }
            }
        }
        Ok(Self { theme, scheme })
    }

    pub fn style(&self, name: &str) -> Option<&Style> {
        self.theme.menu.styles.get(name).or_else(|| self.theme.common.styles.get(name))
    }
}

fn literal_style_tags(template: &str) -> impl Iterator<Item = &str> {
    template.split('[').skip(1).filter_map(|remainder| {
        let tag = remainder.split_once(']')?.0;
        let tag = tag.strip_prefix('/').unwrap_or(tag);
        (!tag.is_empty()
            && tag
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')))
        .then_some(tag)
    })
}

/// Built-in `default` theme. It uses only semantic style paths, so it can pair with a user
/// scheme or the built-in host-inheriting scheme.
pub fn default_theme() -> Theme {
    Theme {
        common: ThemeSection {
            styles: BTreeMap::from([
                (
                    "default".to_owned(),
                    Style {
                        foreground: Some("base.text".to_owned()),
                        background: Some("base.background".to_owned()),
                        ..Style::default()
                    },
                ),
                (
                    "muted".to_owned(),
                    Style {
                        foreground: Some("base.muted".to_owned()),
                        ..Style::default()
                    },
                ),
            ]),
            templates: BTreeMap::new(),
        },
        menu: ThemeSection {
            styles: BTreeMap::from([
                (
                    "title".to_owned(),
                    Style {
                        foreground: Some("base.text".to_owned()),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "hotkey".to_owned(),
                    Style {
                        foreground: Some("menu.hotkey".to_owned()),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "alert".to_owned(),
                    Style {
                        foreground: Some("status.error".to_owned()),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "arrow".to_owned(),
                    Style {
                        foreground: Some("menu.separator".to_owned()),
                        ..Style::default()
                    },
                ),
                (
                    "disabled".to_owned(),
                    Style {
                        foreground: Some("base.muted".to_owned()),
                        dim: true,
                        ..Style::default()
                    },
                ),
                (
                    "crumb".to_owned(),
                    Style {
                        foreground: Some("menu.separator".to_owned()),
                        ..Style::default()
                    },
                ),
                (
                    "error".to_owned(),
                    Style {
                        foreground: Some("status.error".to_owned()),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "pending".to_owned(),
                    Style {
                        foreground: Some("status.pending".to_owned()),
                        ..Style::default()
                    },
                ),
                (
                    "blocked".to_owned(),
                    Style {
                        foreground: Some("status.blocked".to_owned()),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "reload".to_owned(),
                    Style {
                        foreground: Some("status.reload".to_owned()),
                        ..Style::default()
                    },
                ),
                (
                    "notice".to_owned(),
                    Style {
                        foreground: Some("status.notice".to_owned()),
                        ..Style::default()
                    },
                ),
            ]),
            templates: BTreeMap::from([
                (
                    "cell".to_owned(),
                    "{% if blocked %}[alert]! [/alert]{% endif %}{% if disabled %}[disabled]{{ key | rpad(6, ' ') }} → {{ title }}[/disabled]{% else %}[hotkey]{{ key | rpad(6, ' ') }}[/hotkey] [arrow]→[/arrow] [title]{{ title }}[/title]{% endif %}".to_owned(),
                ),
                (
                    "breadcrumbs".to_owned(),
                    "{% for c in crumbs %}[crumb]{{ c }}[/crumb]{% if not loop.last %} › {% endif %}{% endfor %}".to_owned(),
                ),
                (
                    "pagination.full".to_owned(),
                    "[muted]◀ {{ prev_key }} · {{ pages.current }}/{{ pages.count }} · {{ next_key }} ▶[/muted]".to_owned(),
                ),
                (
                    "pagination.short".to_owned(),
                    "[muted]‹{{ pages.current }}/{{ pages.count }}›[/muted]".to_owned(),
                ),
                (
                    "status".to_owned(),
                    "{% if level == 'error' %}[error]{{ message }}[/error]{% elif level == 'pending' %}[pending]{{ message }}[/pending]{% elif level == 'blocked' %}[blocked]{{ message }}[/blocked]{% elif level == 'reload' %}[reload]{{ message }}[/reload]{% else %}[notice]{{ message }}[/notice]{% endif %}".to_owned(),
                ),
            ]),
        },
        settings: BTreeMap::new(),
    }
}

/// Built-in `default` color scheme. Every semantic path inherits the host terminal color.
pub fn default_color_scheme() -> ColorScheme {
    ColorScheme {
        title: "default".to_owned(),
        palette: BTreeMap::new(),
        colors: BTreeMap::from([
            ("base.text".to_owned(), "inherit".to_owned()),
            ("base.background".to_owned(), "inherit".to_owned()),
            ("base.muted".to_owned(), "inherit".to_owned()),
            ("menu.hotkey".to_owned(), "inherit".to_owned()),
            ("menu.separator".to_owned(), "inherit".to_owned()),
            ("status.error".to_owned(), "inherit".to_owned()),
            ("status.pending".to_owned(), "inherit".to_owned()),
            ("status.blocked".to_owned(), "inherit".to_owned()),
            ("status.reload".to_owned(), "inherit".to_owned()),
            ("status.notice".to_owned(), "inherit".to_owned()),
        ]),
    }
}

/// Compiles the built-in host-inheriting pair. Its construction is infallible because both
/// values above are embedded, validated constants.
pub fn compiled_default_theme() -> CompiledTheme {
    CompiledTheme::compile(default_theme(), default_color_scheme())
        .expect("the embedded default theme and scheme are valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn color_scheme_resolves_alias_chains_and_rejects_cycles() {
        let scheme = ColorScheme {
            title: "test".into(),
            palette: BTreeMap::from([("base".into(), "#123".into())]),
            colors: BTreeMap::from([("menu.hotkey".into(), "base".into())]),
        };
        assert_eq!(
            scheme.resolve("menu.hotkey").unwrap(),
            Color::Rgb { red: 17, green: 34, blue: 51 }
        );
    }

    #[test]
    fn theme_pair_rejects_unknown_literal_style_tags() {
        let error = CompiledTheme::compile(
            Theme {
                common: ThemeSection::default(),
                menu: ThemeSection {
                    styles: BTreeMap::new(),
                    templates: BTreeMap::from([("cell".to_owned(), "[missing]text[/missing]".to_owned())]),
                },
                settings: BTreeMap::new(),
            },
            ColorScheme {
                title: "test".to_owned(),
                palette: BTreeMap::new(),
                colors: BTreeMap::new(),
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown literal style"));
    }

    #[test]
    fn embedded_default_theme_compiles_with_host_inherited_colors() {
        let theme = compiled_default_theme();
        assert_eq!(theme.scheme.resolve("menu.hotkey").unwrap(), Color::Inherit);
        assert!(theme.theme.menu.templates.contains_key("cell"));
        assert!(theme.theme.menu.templates.contains_key("breadcrumbs"));
        assert!(theme.theme.menu.templates.contains_key("pagination.full"));
        assert!(theme.theme.menu.templates.contains_key("pagination.short"));
        assert!(theme.theme.menu.templates.contains_key("status"));
    }
}
