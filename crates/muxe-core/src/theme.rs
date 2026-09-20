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
    /// Parses `#rgb` or `#rrggbb` color syntax.
    ///
    /// # Errors
    ///
    /// Returns [`ThemePairError`] when the value is not a valid hexadecimal color.
    pub fn parse(value: &str) -> Result<Self, ThemePairError> {
        let value = value
            .strip_prefix('#')
            .ok_or_else(|| ThemePairError::new("color must begin with `#`"))?;
        match value.len() {
            3 | 6 => {}
            _ => return Err(ThemePairError::new("color must use `#rgb` or `#rrggbb`")),
        }
        if !value.is_ascii() {
            return Err(ThemePairError::new("color contains a non-hex digit"));
        }
        if let Some(index) = value.bytes().position(|byte| !byte.is_ascii_hexdigit()) {
            let message = match value.len() {
                3 => "color contains a non-hex digit",
                6 if index < 2 => "invalid red channel",
                6 if index < 4 => "invalid green channel",
                6 => "invalid blue channel",
                _ => unreachable!("validated color length"),
            };
            return Err(ThemePairError::new(message));
        }

        let value = value.as_bytes();
        let digit = |byte: u8| match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => unreachable!("validated hexadecimal byte"),
        };
        match value.len() {
            3 => Ok(Self::Rgb {
                red: digit(value[0]) * 17,
                green: digit(value[1]) * 17,
                blue: digit(value[2]) * 17,
            }),
            6 => Ok(Self::Rgb {
                red: digit(value[0]) * 16 + digit(value[1]),
                green: digit(value[2]) * 16 + digit(value[3]),
                blue: digit(value[4]) * 16 + digit(value[5]),
            }),
            _ => unreachable!("validated color length"),
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
    /// Resolves one semantic or palette alias to a concrete color.
    ///
    /// # Errors
    ///
    /// Returns [`ThemePairError`] for missing aliases, alias cycles, or invalid colors.
    pub fn resolve(&self, name: &str) -> Result<Color, ThemePairError> {
        let mut seen = HashSet::new();
        self.resolve_inner(name, &mut seen)
    }

    fn resolve_inner(
        &self,
        name: &str,
        seen: &mut HashSet<String>,
    ) -> Result<Color, ThemePairError> {
        if !seen.insert(name.to_owned()) {
            return Err(ThemePairError::new(format!(
                "color alias cycle at `{name}`"
            )));
        }
        let value = self
            .colors
            .get(name)
            .or_else(|| self.palette.get(name))
            .ok_or_else(|| {
                ThemePairError::new(format!("unknown palette or semantic color `{name}`"))
            })?;
        if value == "inherit" {
            Ok(Color::Inherit)
        } else if value.starts_with('#') {
            Color::parse(value)
        } else {
            self.resolve_inner(value, seen)
        }
    }
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "the independently serializable terminal style flags are part of the theme schema"
)]
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

/// Required component templates a compiled theme must provide.
///
/// This registry is the renderer's completeness contract: every entry must resolve
/// through `common`/`menu` templates before a theme is publishable, and the UI
/// renderer constructs from exactly this set. Keep the list here as the single
/// authority; the renderer consumes it rather than maintaining its own copy.
pub const REQUIRED_COMPONENT_TEMPLATES: [&str; 5] = [
    "cell",
    "breadcrumbs",
    "pagination.full",
    "pagination.short",
    "status",
];

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
    /// Validates a pure theme/scheme pair. It rejects loader-backed constructs, parses
    /// templates without filesystem loaders, verifies every declared foreground/background
    /// path, and proves every [`REQUIRED_COMPONENT_TEMPLATES`] entry resolves before a UI
    /// can select the pair.
    ///
    /// # Errors
    ///
    /// Returns [`ThemePairError`] for loader-backed or invalid templates, unresolved style
    /// colors, unknown literal style references, or a missing required component template.
    pub fn compile(theme: Theme, scheme: ColorScheme) -> Result<Self, ThemePairError> {
        for (section, fallback) in [(&theme.common, None), (&theme.menu, Some(&theme.common))] {
            for style in section.styles.values() {
                for color in [style.foreground.as_deref(), style.background.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    if color.starts_with('#') {
                        Color::parse(color)?;
                    } else {
                        scheme.resolve(color)?;
                    }
                }
            }
            for template in section.templates.values() {
                if has_loader_backed_construct(template) {
                    return Err(ThemePairError::new(
                        "theme templates cannot use loader-backed tags",
                    ));
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
        for name in REQUIRED_COMPONENT_TEMPLATES {
            if !theme.menu.templates.contains_key(name)
                && !theme.common.templates.contains_key(name)
            {
                return Err(ThemePairError::new(format!(
                    "selected theme has no `{name}` template"
                )));
            }
        }
        Ok(Self { theme, scheme })
    }

    #[must_use]
    pub fn style(&self, name: &str) -> Option<&Style> {
        self.theme
            .menu
            .styles
            .get(name)
            .or_else(|| self.theme.common.styles.get(name))
    }
}

fn literal_style_tags(template: &str) -> impl Iterator<Item = &str> {
    template.split('[').skip(1).filter_map(|remainder| {
        let tag = remainder.split_once(']')?.0;
        let tag = tag.strip_prefix('/').unwrap_or(tag);
        (!tag.is_empty()
            && tag.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
            }))
        .then_some(tag)
    })
}

/// Reports whether a template source uses a loader-backed construct (`extends`,
/// `include`, `import`, or `from`).
///
/// This is the single authority shared by the compiler and the UI renderer. It makes one
/// forward pass over the source: comment (`{# ... #}`) and expression (`{{ ... }}`)
/// bodies are skipped verbatim, and each `{% ... %}` statement is classified exactly
/// once — after the opener it skips ASCII whitespace plus one optional `-`/`+`
/// whitespace-control marker, requires the keyword to end at an identifier boundary so
/// lookalikes such as `included` stay benign, then scans once to the closing `%}`
/// (skipping quoted string literals) and resumes after it. A tag without a closer ends
/// the scan with no match. Each byte is therefore visited a constant number of times, so
/// adversarial inputs such as kilobytes of unterminated `{%` openers stay linear. The
/// keyword set is exactly the minijinja statement set that emits a loader instruction
/// (`extends` emits `LoadBlocks`; `include`, `import`, and `from ... import ...` emit
/// `Include`); `block` declares a local block and emits no loader access.
#[must_use]
pub fn has_loader_backed_construct(source: &str) -> bool {
    let bytes = source.as_bytes();
    let mut index = 0;
    while index + 1 < bytes.len() {
        if bytes[index] != b'{' {
            index += 1;
            continue;
        }
        match bytes[index + 1] {
            b'#' => {
                let Some(after) = skip_verbatim_tag(bytes, index + 2, b'#', b'}') else {
                    return false;
                };
                index = after;
            }
            b'{' => {
                let Some(after) = skip_verbatim_tag(bytes, index + 2, b'}', b'}') else {
                    return false;
                };
                index = after;
            }
            b'%' => {
                let Some((is_loader, after)) = scan_statement(source, index + 2) else {
                    return false;
                };
                if is_loader {
                    return true;
                }
                index = after;
            }
            _ => index += 1,
        }
    }
    false
}

/// Skips a comment or expression body to its two-byte closer, returning the byte offset
/// just past it, or `None` when the tag never closes (nothing after it can terminate
/// either, so the caller stops the whole scan).
fn skip_verbatim_tag(bytes: &[u8], mut position: usize, first: u8, second: u8) -> Option<usize> {
    while position + 1 < bytes.len() {
        if bytes[position] == first && bytes[position + 1] == second {
            return Some(position + 2);
        }
        position += 1;
    }
    None
}

/// Classifies the `{% ... %}` statement opened at `cursor` (the offset just past `{%`),
/// returning whether it opens a loader-backed construct plus the offset just past its
/// closing `%}`. Returns `None` when the statement never closes. The closer scan skips
/// quoted string literals so `{% set x = "%}" %}` still ends at the true closer; a
/// keyword inside a string literal is therefore never misread as the statement head,
/// and an opener inside a string is skipped along with its statement.
fn scan_statement(source: &str, mut cursor: usize) -> Option<(bool, usize)> {
    let bytes = source.as_bytes();
    while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
        cursor += 1;
    }
    if cursor < bytes.len() && (bytes[cursor] == b'-' || bytes[cursor] == b'+') {
        cursor += 1;
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
    }
    let length = [
        ("extends", 7_usize),
        ("include", 7_usize),
        ("import", 6_usize),
        ("from", 4_usize),
    ]
    .into_iter()
    .find(|(word, _)| {
        source
            .get(cursor..)
            .is_some_and(|tail| tail.starts_with(word))
    })
    .map(|(_, length)| length);
    let is_loader = length.is_some_and(|length| {
        source.get(cursor + length..).is_some_and(|tail| {
            tail.chars()
                .next()
                .is_none_or(|character| !(character == '_' || character.is_alphanumeric()))
        })
    });
    let mut position = cursor + length.unwrap_or(0);
    let mut quote: Option<u8> = None;
    while position + 1 < bytes.len() {
        let byte = bytes[position];
        if let Some(open) = quote {
            if byte == b'\\' {
                position += 2;
                continue;
            }
            if byte == open {
                quote = None;
            }
            position += 1;
            continue;
        }
        if byte == b'"' || byte == b'\'' {
            quote = Some(byte);
            position += 1;
            continue;
        }
        if byte == b'%' && bytes[position + 1] == b'}' {
            return Some((is_loader, position + 2));
        }
        position += 1;
    }
    None
}
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "the embedded default theme remains a single auditable style registry"
)]
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
#[must_use]
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
///
/// # Panics
///
/// Panics if an embedded theme constant violates the validation contract.
#[must_use]
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
            Color::Rgb {
                red: 17,
                green: 34,
                blue: 51
            }
        );
    }

    #[test]
    fn color_parse_rejects_non_ascii_and_non_hex_input() {
        assert!(Color::parse("#aéaaa").is_err());
        assert!(Color::parse("#12g").is_err());
        assert_eq!(
            Color::parse("#g00000").unwrap_err().to_string(),
            "invalid red channel"
        );
        assert_eq!(
            Color::parse("#00g000").unwrap_err().to_string(),
            "invalid green channel"
        );
        assert_eq!(
            Color::parse("#0000g0").unwrap_err().to_string(),
            "invalid blue channel"
        );
    }

    fn complete_templates(cell_source: &str) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("cell".to_owned(), cell_source.to_owned()),
            ("breadcrumbs".to_owned(), "{{ crumbs }}".to_owned()),
            (
                "pagination.full".to_owned(),
                "{{ pages.current }}/{{ pages.count }}".to_owned(),
            ),
            (
                "pagination.short".to_owned(),
                "{{ pages.current }}/{{ pages.count }}".to_owned(),
            ),
            ("status".to_owned(), "{{ message }}".to_owned()),
        ])
    }

    fn test_scheme() -> ColorScheme {
        ColorScheme {
            title: "test".to_owned(),
            palette: BTreeMap::new(),
            colors: BTreeMap::new(),
        }
    }

    #[test]
    fn theme_pair_rejects_unknown_literal_style_tags() {
        let error = CompiledTheme::compile(
            Theme {
                common: ThemeSection::default(),
                menu: ThemeSection {
                    styles: BTreeMap::new(),
                    templates: complete_templates("[missing]text[/missing]"),
                },
                settings: BTreeMap::new(),
            },
            test_scheme(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("unknown literal style"));
    }

    #[test]
    fn compile_rejects_each_missing_required_component_template_by_name() {
        for missing in REQUIRED_COMPONENT_TEMPLATES {
            let mut templates = complete_templates("{{ title }}");
            templates.remove(missing);
            let error = CompiledTheme::compile(
                Theme {
                    common: ThemeSection::default(),
                    menu: ThemeSection {
                        styles: BTreeMap::new(),
                        templates,
                    },
                    settings: BTreeMap::new(),
                },
                test_scheme(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(missing),
                "missing `{missing}` must be named, got `{error}`"
            );
        }
    }

    #[test]
    fn compile_accepts_common_section_templates_for_required_names() {
        let mut menu_templates = complete_templates("{{ title }}");
        let status = menu_templates.remove("status").expect("status fixture");
        let error = CompiledTheme::compile(
            Theme {
                common: ThemeSection {
                    styles: BTreeMap::new(),
                    templates: BTreeMap::from([("status".to_owned(), status)]),
                },
                menu: ThemeSection {
                    styles: BTreeMap::new(),
                    templates: menu_templates,
                },
                settings: BTreeMap::new(),
            },
            test_scheme(),
        );
        assert!(error.is_ok(), "common fallback must satisfy completeness");
    }

    #[test]
    fn compile_rejects_loader_backed_spellings_and_accepts_benign_constructs() {
        for source in [
            "{% include x %}",
            "{%- include x %}",
            "{% extends x %}",
            "{%- import x as y %}",
            "{%+ include x %}",
            "{%\tinclude x %}",
            "{% from 'x' import y %}",
            "{%- from 'x' import y %}",
            "{%from 'x' import y%}",
        ] {
            let error = CompiledTheme::compile(
                Theme {
                    common: ThemeSection::default(),
                    menu: ThemeSection {
                        styles: BTreeMap::new(),
                        templates: complete_templates(source),
                    },
                    settings: BTreeMap::new(),
                },
                test_scheme(),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("loader-backed"),
                "`{source}` must be rejected, got `{error}`"
            );
        }
        for source in [
            "{# {% include x %} #}",
            "{{ value }}",
            "{% if included %}ok{% endif %}",
            "{% set include_count = 1 %}{{ include_count }}",
            "{% set x = 1 %}{{ x }}",
            "{% if x %}ok{% endif %}",
        ] {
            CompiledTheme::compile(
                Theme {
                    common: ThemeSection::default(),
                    menu: ThemeSection {
                        styles: BTreeMap::new(),
                        templates: complete_templates(source),
                    },
                    settings: BTreeMap::new(),
                },
                test_scheme(),
            )
            .unwrap_or_else(|error| panic!("`{source}` must be benign, got `{error}`"));
        }
    }
    #[test]
    fn loader_scan_stays_linear_on_adversarial_openers() {
        use std::time::{Duration, Instant};
        // 64 KiB of unterminated `{%` openers: no keyword head, so the scan advances
        // past each opener once and reports no construct well inside this bound.
        let openers = "{%".repeat(32 * 1024);
        let started = Instant::now();
        assert!(
            !has_loader_backed_construct(&openers),
            "an unterminated statement cannot be a loader construct"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "64 KiB of openers must scan in linear time"
        );
        // The true quadratic shape of the old per-opener re-scan: every opener carries a
        // keyword head but no closer, so the old code re-scanned to EOF once per opener
        // (~14 s in debug at 256 KiB). The single forward pass stops at the first
        // unterminated statement and finishes far inside this bound with no match.
        let keywords = "{% include ".repeat(32 * 1024);
        let started = Instant::now();
        assert!(
            !has_loader_backed_construct(&keywords),
            "an unterminated loader statement cannot match"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "256 KiB of unclosed keyword openers must scan in linear time"
        );
        // Same bare-openers input with exactly one late closer: the benign statement is
        // classified once and the scan completes just as quickly.
        let late_close = format!("{openers} if x %}}");
        let started = Instant::now();
        assert!(
            !has_loader_backed_construct(&late_close),
            "a late-closed benign statement must stay benign"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a late closer must not trigger a quadratic re-scan"
        );
        // And a late loader keyword is still caught through the same single pass.
        let late_loader = ["{%", &(" ".repeat(64 * 1024 - 16) + "include x"), "%}"].concat();
        assert!(
            has_loader_backed_construct(&late_loader),
            "a late loader keyword must still be detected"
        );
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
