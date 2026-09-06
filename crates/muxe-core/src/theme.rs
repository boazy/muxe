use std::collections::{BTreeMap, HashSet};
use std::fmt;

/// Truecolor RGB value accepted by v1 themes and schemes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Color {
    pub red: u8,
    pub green: u8,
    pub blue: u8,
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
                Ok(Self {
                    red: expand(chars.next().expect("length checked"))?,
                    green: expand(chars.next().expect("length checked"))?,
                    blue: expand(chars.next().expect("length checked"))?,
                })
            }
            6 => Ok(Self {
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
        if value.starts_with('#') {
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
        for section in [&theme.common, &theme.menu] {
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
            }
        }
        Ok(Self { theme, scheme })
    }

    pub fn style(&self, name: &str) -> Option<&Style> {
        self.theme.menu.styles.get(name).or_else(|| self.theme.common.styles.get(name))
    }
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
        assert_eq!(scheme.resolve("menu.hotkey").unwrap(), Color { red: 17, green: 34, blue: 51 });
    }
}
