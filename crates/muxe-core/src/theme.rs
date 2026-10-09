use crate::diagnostic::{ConfigDiagnostic, SourceSpan};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

macro_rules! theme_name {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);
        impl $name {
            #[must_use]
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
            #[must_use]
            pub fn into_string(self) -> String {
                self.0
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}
theme_name!(
    ThemeName,
    "Opaque theme catalog identity, distinct from color-scheme identity and display titles."
);
theme_name!(
    ColorSchemeName,
    "Opaque color-scheme catalog identity, distinct from theme identity and display titles."
);
theme_name!(
    ColorAliasName,
    "A name in the shared semantic-color and palette lookup namespace."
);

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

impl fmt::Display for Color {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Inherit => formatter.write_str("inherit"),
            Self::Rgb { red, green, blue } => write!(formatter, "#{red:02x}{green:02x}{blue:02x}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColorExpr {
    Literal(Color),
    Alias(ColorAliasName),
    Inherit,
}

impl ColorExpr {
    /// Parses config style syntax: only hexadecimal text is literal; `inherit` is an alias.
    ///
    /// # Errors
    ///
    /// Returns a color syntax error for malformed hexadecimal literals.
    pub fn parse_style(value: &str) -> Result<Self, ThemePairError> {
        if value.starts_with('#') {
            Color::parse(value).map(Self::Literal)
        } else {
            Ok(Self::Alias(ColorAliasName::new(value)))
        }
    }

    /// Parses external wire color syntax, whose `inherit` keyword precedes name lookup.
    ///
    /// # Errors
    ///
    /// Returns a color syntax error for malformed hexadecimal literals.
    pub fn parse_wire(value: &str) -> Result<Self, ThemePairError> {
        if value == "inherit" {
            Ok(Self::Inherit)
        } else {
            Self::parse_style(value)
        }
    }
}

/// Final palette and semantic colors, after asset aliases have been resolved once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColorScheme {
    pub title: String,
    pub palette: BTreeMap<ColorAliasName, Color>,
    pub colors: BTreeMap<ColorAliasName, Color>,
}

impl ColorScheme {
    /// Looks up a resolved name, retaining semantic-before-palette precedence.
    ///
    /// # Errors
    ///
    /// Returns an error when the name does not exist.
    pub fn resolve(&self, name: &ColorAliasName) -> Result<Color, ThemePairError> {
        self.colors
            .get(name)
            .or_else(|| self.palette.get(name))
            .copied()
            .ok_or_else(|| {
                ThemePairError::new(format!("unknown palette or semantic color `{name}`"))
            })
    }
}

/// A boundary-parsed color graph. Deferred entry errors preserve legacy archive reachability.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColorExpressions {
    palette: BTreeMap<ColorAliasName, Result<ColorExpr, ThemePairError>>,
    colors: BTreeMap<ColorAliasName, Result<ColorExpr, ThemePairError>>,
}

impl ColorExpressions {
    pub fn insert_palette_first(
        &mut self,
        name: ColorAliasName,
        value: Result<ColorExpr, ThemePairError>,
    ) {
        self.palette.entry(name).or_insert(value);
    }
    /// Inserts a config palette field, preserving its last-wins duplicate policy.
    pub(crate) fn insert_palette_last(
        &mut self,
        name: ColorAliasName,
        value: Result<ColorExpr, ThemePairError>,
    ) {
        self.palette.insert(name, value);
    }
    pub fn insert_semantic_first(
        &mut self,
        name: ColorAliasName,
        value: Result<ColorExpr, ThemePairError>,
    ) {
        self.colors.entry(name).or_insert(value);
    }

    /// Validates every asset entry and consumes expressions into resolved colors.
    ///
    /// # Errors
    ///
    /// Returns the first malformed expression, unknown alias, or cycle, including unused entries.
    pub fn resolve_all(self, title: String) -> Result<ColorScheme, ThemePairError> {
        let mut resolver = ColorResolver::from_expressions(&self);
        let palette_values = self
            .palette
            .values()
            .map(|value| resolver.resolve(value.as_ref().map_err(Clone::clone)?))
            .collect::<Result<Vec<_>, ThemePairError>>()?;
        let color_values = self
            .colors
            .keys()
            .map(|name| resolver.resolve_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        drop(resolver);
        Ok(ColorScheme {
            title,
            palette: self.palette.into_keys().zip(palette_values).collect(),
            colors: self.colors.into_keys().zip(color_values).collect(),
        })
    }
}

#[derive(Clone, Copy)]
enum ColorSource<'a> {
    Scheme(&'a ColorScheme),
    Expressions(&'a ColorExpressions),
}

/// One construction-round resolver, shared by assets, compiled styles, and external archives.
pub struct ColorResolver<'a> {
    source: ColorSource<'a>,
    active: HashSet<&'a ColorAliasName>,
    resolved: BTreeMap<&'a ColorAliasName, Color>,
}

impl<'a> ColorResolver<'a> {
    #[must_use]
    pub fn from_scheme(scheme: &'a ColorScheme) -> Self {
        Self {
            source: ColorSource::Scheme(scheme),
            active: HashSet::new(),
            resolved: BTreeMap::new(),
        }
    }
    #[must_use]
    pub fn from_expressions(expressions: &'a ColorExpressions) -> Self {
        Self {
            source: ColorSource::Expressions(expressions),
            active: HashSet::new(),
            resolved: BTreeMap::new(),
        }
    }

    /// Resolves a typed root; successful named graph results are memoized for this round.
    ///
    /// # Errors
    ///
    /// Returns stored parsing errors, unknown names, or alias cycles.
    pub fn resolve(&mut self, expression: &ColorExpr) -> Result<Color, ThemePairError> {
        match expression {
            ColorExpr::Literal(color) => Ok(*color),
            ColorExpr::Inherit => Ok(Color::Inherit),
            ColorExpr::Alias(name) => self.resolve_name(name),
        }
    }

    fn resolve_name(&mut self, name: &ColorAliasName) -> Result<Color, ThemePairError> {
        let graph = match self.source {
            ColorSource::Scheme(scheme) => return scheme.resolve(name),
            ColorSource::Expressions(graph) => graph,
        };
        if let Some(color) = self.resolved.get(name) {
            return Ok(*color);
        }
        let (key, expression) = graph
            .colors
            .get_key_value(name)
            .or_else(|| graph.palette.get_key_value(name))
            .ok_or_else(|| {
                ThemePairError::new(format!("unknown palette or semantic color `{name}`"))
            })?;
        if !self.active.insert(key) {
            return Err(ThemePairError::new(format!(
                "color alias cycle at `{name}`"
            )));
        }
        let result = match expression {
            Ok(expression) => self.resolve(expression),
            Err(error) => Err(error.clone()),
        };
        self.active.remove(key);
        if let Ok(color) = result {
            self.resolved.insert(key, color);
        }
        result
    }
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "the independently serializable terminal style flags are part of the theme schema"
)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Style {
    pub foreground: Option<Result<ColorExpr, ThemePairError>>,
    pub background: Option<Result<ColorExpr, ThemePairError>>,
    pub bold: bool,
    pub dim: bool,
    pub italic: bool,
    pub underline: bool,
    pub strikethrough: bool,
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "the independent terminal style flags preserve the existing schema"
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResolvedStyle {
    pub foreground: Option<Color>,
    pub background: Option<Color>,
    pub bold: bool,
    pub dim: bool,
    pub italic: bool,
    pub underline: bool,
    pub strikethrough: bool,
}

impl Style {
    /// Resolves style colors while preserving absent values and independent attributes.
    ///
    /// # Errors
    ///
    /// Returns the resolver's color-expression error.
    pub fn resolve(
        self,
        resolver: &mut ColorResolver<'_>,
    ) -> Result<ResolvedStyle, ThemePairError> {
        Ok(ResolvedStyle {
            foreground: self
                .foreground
                .map(|expression| expression.and_then(|expression| resolver.resolve(&expression)))
                .transpose()?,
            background: self
                .background
                .map(|expression| expression.and_then(|expression| resolver.resolve(&expression)))
                .transpose()?,
            bold: self.bold,
            dim: self.dim,
            italic: self.italic,
            underline: self.underline,
            strikethrough: self.strikethrough,
        })
    }
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedThemeSection {
    pub styles: BTreeMap<String, ResolvedStyle>,
    pub templates: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedTheme {
    pub common: ResolvedThemeSection,
    pub menu: ResolvedThemeSection,
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
    pub theme: ResolvedTheme,
    pub scheme: ColorScheme,
}
/// Immutable parsed theme and color-scheme assets retained by one compiled generation.
///
/// Each entry retains either its parsed value or the exact source diagnostics produced while
/// parsing it. The broker can therefore reject a malformed attach override without filesystem I/O.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledThemeCatalog {
    themes: Arc<BTreeMap<ThemeName, Arc<ThemeAsset<Theme>>>>,
    color_schemes: Arc<BTreeMap<ColorSchemeName, Arc<ThemeAsset<ColorScheme>>>>,
    origin: Arc<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ThemeAsset<T> {
    parsed: Result<T, Vec<ConfigDiagnostic>>,
    span: SourceSpan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ThemeSelectionError {
    UnknownTheme {
        name: ThemeName,
    },
    UnknownColorScheme {
        name: ColorSchemeName,
    },
    InvalidTheme {
        name: ThemeName,
        diagnostics: Vec<ConfigDiagnostic>,
    },
    InvalidColorScheme {
        name: ColorSchemeName,
        diagnostics: Vec<ConfigDiagnostic>,
    },
    InvalidPair {
        theme: ThemeName,
        color_scheme: ColorSchemeName,
        error: ThemePairError,
        span: SourceSpan,
    },
    StaleResolution,
}

impl fmt::Display for ThemeSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTheme { name } => write!(formatter, "unknown theme `{name}`"),
            Self::UnknownColorScheme { name } => {
                write!(formatter, "unknown color scheme `{name}`")
            }
            Self::InvalidTheme { name, diagnostics } => write!(
                formatter,
                "theme `{name}` is invalid: {}",
                diagnostics
                    .iter()
                    .map(|diagnostic| diagnostic.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Self::InvalidColorScheme { name, diagnostics } => write!(
                formatter,
                "color scheme `{name}` is invalid: {}",
                diagnostics
                    .iter()
                    .map(|diagnostic| diagnostic.message.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Self::InvalidPair {
                theme,
                color_scheme,
                error,
                ..
            } => write!(
                formatter,
                "theme `{theme}` cannot pair with color scheme `{color_scheme}`: {error}"
            ),
            Self::StaleResolution => formatter.write_str(
                "cannot apply the resolved theme: it belongs to a different configuration",
            ),
        }
    }
}

impl std::error::Error for ThemeSelectionError {}

impl CompiledThemeCatalog {
    pub(crate) fn new(
        themes: BTreeMap<ThemeName, (Result<Theme, Vec<ConfigDiagnostic>>, SourceSpan)>,
        color_schemes: BTreeMap<
            ColorSchemeName,
            (Result<ColorScheme, Vec<ConfigDiagnostic>>, SourceSpan),
        >,
    ) -> Self {
        Self {
            themes: Arc::new(
                themes
                    .into_iter()
                    .map(|(name, (parsed, span))| (name, Arc::new(ThemeAsset { parsed, span })))
                    .collect(),
            ),
            color_schemes: Arc::new(
                color_schemes
                    .into_iter()
                    .map(|(name, (parsed, span))| (name, Arc::new(ThemeAsset { parsed, span })))
                    .collect(),
            ),
            origin: Arc::new(()),
        }
    }
    /// Lists all selectable display-theme names, including user replacements.
    pub fn theme_names(&self) -> impl Iterator<Item = &ThemeName> {
        self.themes.keys()
    }

    /// Lists all selectable color-scheme names, including user replacements.
    pub fn color_scheme_names(&self) -> impl Iterator<Item = &ColorSchemeName> {
        self.color_schemes.keys()
    }

    pub(crate) fn with_overrides(&self, overrides: Self) -> Self {
        let mut catalog = self.clone();
        if !overrides.themes.is_empty() {
            Arc::make_mut(&mut catalog.themes).extend(Arc::unwrap_or_clone(overrides.themes));
        }
        if !overrides.color_schemes.is_empty() {
            Arc::make_mut(&mut catalog.color_schemes)
                .extend(Arc::unwrap_or_clone(overrides.color_schemes));
        }
        // Resolution proofs belong to this generation, even when all assets are embedded.
        catalog.origin = overrides.origin;
        catalog
    }

    pub(crate) fn origin_token(&self) -> Arc<()> {
        Arc::clone(&self.origin)
    }

    /// Resolves and validates one exact theme/color-scheme pair.
    ///
    /// # Errors
    ///
    /// Returns [`ThemeSelectionError`] when either asset is absent or malformed, or when
    /// the mixed pair violates the shared [`CompiledTheme`] validation contract.
    pub fn resolve(
        &self,
        theme_name: &ThemeName,
        color_scheme_name: &ColorSchemeName,
    ) -> Result<CompiledTheme, ThemeSelectionError> {
        let theme_asset =
            self.themes
                .get(theme_name)
                .ok_or_else(|| ThemeSelectionError::UnknownTheme {
                    name: theme_name.clone(),
                })?;
        let theme = theme_asset.parsed.clone().map_err(|diagnostics| {
            ThemeSelectionError::InvalidTheme {
                name: theme_name.clone(),
                diagnostics,
            }
        })?;
        let scheme_asset = self.color_schemes.get(color_scheme_name).ok_or_else(|| {
            ThemeSelectionError::UnknownColorScheme {
                name: color_scheme_name.clone(),
            }
        })?;
        let scheme = scheme_asset.parsed.clone().map_err(|diagnostics| {
            ThemeSelectionError::InvalidColorScheme {
                name: color_scheme_name.clone(),
                diagnostics,
            }
        })?;
        CompiledTheme::compile(theme, scheme).map_err(|error| ThemeSelectionError::InvalidPair {
            theme: theme_name.clone(),
            color_scheme: color_scheme_name.clone(),
            error,
            span: theme_asset.span.clone(),
        })
    }
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
        let mut resolver = ColorResolver::from_scheme(&scheme);
        let common = compile_theme_section(theme.common, None, &mut resolver)?;
        let menu = compile_theme_section(theme.menu, Some(&common), &mut resolver)?;
        for name in REQUIRED_COMPONENT_TEMPLATES {
            if !menu.templates.contains_key(name) && !common.templates.contains_key(name) {
                return Err(ThemePairError::new(format!(
                    "selected theme has no `{name}` template"
                )));
            }
        }
        Ok(Self {
            theme: ResolvedTheme {
                common,
                menu,
                settings: theme.settings,
            },
            scheme,
        })
    }

    #[must_use]
    pub fn style(&self, name: &str) -> Option<&ResolvedStyle> {
        self.theme
            .menu
            .styles
            .get(name)
            .or_else(|| self.theme.common.styles.get(name))
    }
}

fn compile_theme_section(
    section: ThemeSection,
    fallback: Option<&ResolvedThemeSection>,
    resolver: &mut ColorResolver<'_>,
) -> Result<ResolvedThemeSection, ThemePairError> {
    let styles = section
        .styles
        .into_iter()
        .map(|(name, style)| style.resolve(resolver).map(|style| (name, style)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    for template in section.templates.values() {
        if has_loader_backed_construct(template) {
            return Err(ThemePairError::new(
                "theme templates cannot load other templates; `include`, `import`, `from`, and `extends` are not supported",
            ));
        }
        minijinja::Environment::new()
            .template_from_str(template)
            .map_err(|error| ThemePairError::new(error.to_string()))?;
        for style_name in literal_style_tags(template) {
            if styles.contains_key(style_name)
                || fallback.is_some_and(|common| common.styles.contains_key(style_name))
            {
                continue;
            }
            return Err(ThemePairError::new(format!(
                "template references unknown literal style `{style_name}`"
            )));
        }
    }
    Ok(ResolvedThemeSection {
        styles,
        templates: section.templates,
    })
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
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("base.text")))),
                        background: Some(Ok(ColorExpr::Alias(ColorAliasName::new("base.background")))),
                        ..Style::default()
                    },
                ),
                (
                    "muted".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("base.muted")))),
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
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("base.text")))),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "hotkey".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("menu.hotkey")))),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "alert".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("status.error")))),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "arrow".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("menu.separator")))),
                        ..Style::default()
                    },
                ),
                (
                    "disabled".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("base.muted")))),
                        dim: true,
                        ..Style::default()
                    },
                ),
                (
                    "crumb".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("menu.separator")))),
                        ..Style::default()
                    },
                ),
                (
                    "error".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("status.error")))),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "pending".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("status.pending")))),
                        ..Style::default()
                    },
                ),
                (
                    "blocked".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("status.blocked")))),
                        bold: true,
                        ..Style::default()
                    },
                ),
                (
                    "reload".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("status.reload")))),
                        ..Style::default()
                    },
                ),
                (
                    "notice".to_owned(),
                    Style {
                        foreground: Some(Ok(ColorExpr::Alias(ColorAliasName::new("status.notice")))),
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
            (ColorAliasName::new("base.text"), Color::Inherit),
            (ColorAliasName::new("base.background"), Color::Inherit),
            (ColorAliasName::new("base.muted"), Color::Inherit),
            (ColorAliasName::new("menu.hotkey"), Color::Inherit),
            (ColorAliasName::new("menu.separator"), Color::Inherit),
            (ColorAliasName::new("status.error"), Color::Inherit),
            (ColorAliasName::new("status.pending"), Color::Inherit),
            (ColorAliasName::new("status.blocked"), Color::Inherit),
            (ColorAliasName::new("status.reload"), Color::Inherit),
            (ColorAliasName::new("status.notice"), Color::Inherit),
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
        let mut expressions = ColorExpressions::default();
        expressions
            .insert_palette_first(ColorAliasName::new("base"), ColorExpr::parse_wire("#123"));
        expressions
            .insert_semantic_first(ColorAliasName::new("mid"), ColorExpr::parse_style("base"));
        expressions.insert_semantic_first(
            ColorAliasName::new("menu.hotkey"),
            ColorExpr::parse_style("mid"),
        );
        let scheme = expressions
            .resolve_all("test".to_owned())
            .expect("alias chain resolves");
        assert_eq!(
            scheme.resolve(&ColorAliasName::new("menu.hotkey")).unwrap(),
            Color::Rgb {
                red: 17,
                green: 34,
                blue: 51
            }
        );
        let mut cycle = ColorExpressions::default();
        cycle.insert_semantic_first(ColorAliasName::new("a"), ColorExpr::parse_style("b"));
        cycle.insert_semantic_first(ColorAliasName::new("b"), ColorExpr::parse_style("a"));
        assert!(cycle.resolve_all("cycle".to_owned()).is_err());
    }

    #[test]
    fn color_parse_rejects_non_ascii_and_non_hex_input() {
        assert!(Color::parse("#aéaaa").is_err());
        assert!(Color::parse("#12g").is_err());
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
        let mut theme = Theme {
            common: ThemeSection::default(),
            menu: ThemeSection {
                styles: BTreeMap::new(),
                templates: complete_templates("[missing]text[/missing]"),
            },
            settings: BTreeMap::new(),
        };
        assert!(CompiledTheme::compile(theme.clone(), test_scheme()).is_err());
        theme
            .menu
            .styles
            .insert("missing".to_owned(), Style::default());
        CompiledTheme::compile(theme, test_scheme())
            .expect("declaring the referenced style remedies the failure");
    }

    #[test]
    fn missing_component_is_rejected_until_supplied_by_the_common_fallback() {
        for missing in REQUIRED_COMPONENT_TEMPLATES {
            let mut theme = Theme {
                common: ThemeSection::default(),
                menu: ThemeSection {
                    styles: BTreeMap::new(),
                    templates: complete_templates("{{ title }}"),
                },
                settings: BTreeMap::new(),
            };
            let removed = theme
                .menu
                .templates
                .remove(missing)
                .expect("required fixture template");
            assert!(CompiledTheme::compile(theme.clone(), test_scheme()).is_err());
            theme.common.templates.insert(missing.to_owned(), removed);
            CompiledTheme::compile(theme, test_scheme())
                .expect("common fallback restores the missing component");
        }
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
            let result = CompiledTheme::compile(
                Theme {
                    common: ThemeSection::default(),
                    menu: ThemeSection {
                        styles: BTreeMap::new(),
                        templates: complete_templates(source),
                    },
                    settings: BTreeMap::new(),
                },
                test_scheme(),
            );
            assert!(result.is_err(), "loader-backed input must reject");
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
    fn embedded_catalog_storage_is_shared_but_resolution_origins_are_generation_scoped() {
        let first = crate::ThemeAssets::default().compile_catalog();
        let second = crate::ThemeAssets::default().compile_catalog();
        assert!(Arc::ptr_eq(&first.themes, &second.themes));
        assert!(Arc::ptr_eq(&first.color_schemes, &second.color_schemes));
        assert!(!Arc::ptr_eq(&first.origin, &second.origin));
    }

    #[test]
    fn embedded_default_theme_compiles_with_host_inherited_colors() {
        let theme = compiled_default_theme();
        assert_eq!(
            theme
                .scheme
                .resolve(&ColorAliasName::new("menu.hotkey"))
                .unwrap(),
            Color::Inherit
        );
    }
}
