use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};

use minijinja::{context, Environment, Error as MiniError, ErrorKind, UndefinedBehavior};
use muxe_core::{Color, CompiledTheme, Style};
use muxe_protocol::{ArchivedCompiledThemeWire, ArchivedStyleWire};
use ratatui::style::{Color as RatatuiColor, Modifier, Style as RatatuiStyle};
use thiserror::Error;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::layout::{ellipsize, sanitize_single_line};

const MAX_COMPONENT_BYTES: usize = 64 * 1024;

/// Plain values available to the menu cell template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellTemplate<'a> {
    pub key: &'a str,
    pub title: &'a str,
    pub disabled: bool,
    pub blocked: bool,
    pub max_title_width: usize,
}

/// Values available to the breadcrumbs template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BreadcrumbTemplate<'a> {
    pub crumbs: &'a [&'a str],
}

/// Values available to either pagination template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaginationTemplate<'a> {
    pub current: u64,
    pub count: u64,
    pub prev_key: &'a str,
    pub next_key: &'a str,
    pub prev_keys: &'a [&'a str],
    pub next_keys: &'a [&'a str],
}

/// Values available to the status template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatusTemplate<'a> {
    pub level: &'a str,
    pub message: &'a str,
}

/// One visible styled segment after template markup is resolved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedSpan {
    pub text: String,
    pub style: RatatuiStyle,
}

/// A rendered component with its visible text and resolved ratatui spans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RenderedText {
    pub plain: String,
    pub spans: Vec<RenderedSpan>,
}

/// A template compilation or render failure.
#[derive(Debug, Error)]
pub enum TemplateError {
    #[error("template `{0}` uses a forbidden loader-backed construct")]
    ForbiddenLoaderBackedConstruct(&'static str),
    #[error("selected theme has no `{0}` template")]
    MissingTemplate(&'static str),
    #[error("menu template error: {0}")]
    Render(String),
    #[error("rendered component exceeds the {MAX_COMPONENT_BYTES}-byte limit")]
    OutputTooLarge,
}

/// A selected theme's reusable strict template environment and resolved styles.
pub struct TemplateRenderer {
    environment: Environment<'static>,
    styles: BTreeMap<String, RatatuiStyle>,
}

impl TemplateRenderer {
    /// Compiles a core theme once for the lifetime of its UI attachment.
    pub fn new(theme: &CompiledTheme) -> Result<Self, TemplateError> {
        Self::from_sources(|name| template_source(theme, name), resolve_styles(theme)?)
    }

    /// Compiles the resolved, checked theme embedded in a broker attachment without
    /// deserializing the attachment's menu graph.
    pub fn from_archived(theme: &ArchivedCompiledThemeWire) -> Result<Self, TemplateError> {
        Self::from_sources(
            |name| archived_template_source(theme, name),
            resolve_archived_styles(theme)?,
        )
    }

    fn from_sources<'a>(
        template_source: impl Fn(&str) -> Option<&'a str>,
        styles: BTreeMap<String, RatatuiStyle>,
    ) -> Result<Self, TemplateError> {
        let mut environment = Environment::new();
        environment.set_undefined_behavior(UndefinedBehavior::Strict);
        environment.add_filter("rpad", rpad_filter);
        environment.add_filter("lpad", lpad_filter);
        environment.add_filter("ellipsis", ellipsis_filter);
        environment.add_filter("style", style_filter);

        for name in [
            "cell",
            "breadcrumbs",
            "pagination.full",
            "pagination.short",
            "status",
        ] {
            let source = template_source(name).ok_or(TemplateError::MissingTemplate(name))?;
            if has_loader_backed_construct(source) {
                return Err(TemplateError::ForbiddenLoaderBackedConstruct(name));
            }
            environment
                .add_template_owned(name.to_owned(), source.to_owned())
                .map_err(|error| TemplateError::Render(error.to_string()))?;
        }

        Ok(Self {
            environment,
            styles,
        })
    }

    /// Renders a sanitized, title-limited cell before layout measures its visible text.
    pub fn render_cell(&self, cell: CellTemplate<'_>) -> Result<RenderedText, TemplateError> {
        self.render(
            "cell",
            context! {
                key => escape_markup_text(&sanitize_single_line(cell.key))?,
                title => escape_markup_text(&ellipsize(cell.title, cell.max_title_width))?,
                disabled => cell.disabled,
                blocked => cell.blocked,
            },
        )
    }

    pub fn render_breadcrumbs(
        &self,
        breadcrumbs: BreadcrumbTemplate<'_>,
    ) -> Result<RenderedText, TemplateError> {
        let crumbs = breadcrumbs
            .crumbs
            .iter()
            .map(|crumb| escape_markup_text(&sanitize_single_line(crumb)))
            .collect::<Result<Vec<_>, _>>()?;
        self.render("breadcrumbs", context! { crumbs => crumbs })
    }

    pub fn render_pagination_full(
        &self,
        pagination: PaginationTemplate<'_>,
    ) -> Result<RenderedText, TemplateError> {
        self.render_pagination("pagination.full", pagination)
    }

    pub fn render_pagination_short(
        &self,
        pagination: PaginationTemplate<'_>,
    ) -> Result<RenderedText, TemplateError> {
        self.render_pagination("pagination.short", pagination)
    }

    pub fn render_status(&self, status: StatusTemplate<'_>) -> Result<RenderedText, TemplateError> {
        self.render(
            "status",
            context! {
                level => escape_markup_text(status.level)?,
                message => escape_markup_text(&sanitize_single_line(status.message))?,
            },
        )
    }

    fn render_pagination(
        &self,
        template: &'static str,
        pagination: PaginationTemplate<'_>,
    ) -> Result<RenderedText, TemplateError> {
        let prev_keys = pagination
            .prev_keys
            .iter()
            .map(|key| escape_markup_text(&sanitize_single_line(key)))
            .collect::<Result<Vec<_>, _>>()?;
        let next_keys = pagination
            .next_keys
            .iter()
            .map(|key| escape_markup_text(&sanitize_single_line(key)))
            .collect::<Result<Vec<_>, _>>()?;
        self.render(
            template,
            context! {
                pages => context! { current => pagination.current, count => pagination.count },
                prev_key => escape_markup_text(&sanitize_single_line(pagination.prev_key))?,
                next_key => escape_markup_text(&sanitize_single_line(pagination.next_key))?,
                prev_keys => prev_keys,
                next_keys => next_keys,
            },
        )
    }

    fn render<S: serde::Serialize>(
        &self,
        name: &'static str,
        context: S,
    ) -> Result<RenderedText, TemplateError> {
        let template = self
            .environment
            .get_template(name)
            .map_err(|error| TemplateError::Render(error.to_string()))?;
        let mut writer = BoundedWriter::new();
        let render_result = template.render_captured_to(context, &mut writer);
        if writer.exceeded {
            return Err(TemplateError::OutputTooLarge);
        }
        render_result.map_err(|error| TemplateError::Render(error.to_string()))?;
        let rendered = String::from_utf8(writer.bytes)
            .map_err(|error| TemplateError::Render(error.to_string()))?;
        parse_style_markup(&rendered, &self.styles)
    }
}

/// Escapes text that could otherwise be interpreted as style markup or a control character.
pub fn escape_markup_text(value: &str) -> Result<String, TemplateError> {
    let mut output = BoundedWriter::new();

    for character in value.chars() {
        match character {
            '[' | ']' => {
                output
                    .write_all(b"\\")
                    .map_err(|_| TemplateError::OutputTooLarge)?;
                output
                    .write_all(character.encode_utf8(&mut [0; 4]).as_bytes())
                    .map_err(|_| TemplateError::OutputTooLarge)?;
            }
            character if character.is_control() => {
                let escaped = format!("\\u{{{:04x}}}", character as u32);
                output
                    .write_all(escaped.as_bytes())
                    .map_err(|_| TemplateError::OutputTooLarge)?;
            }
            character => output
                .write_all(character.encode_utf8(&mut [0; 4]).as_bytes())
                .map_err(|_| TemplateError::OutputTooLarge)?,
        }
    }
    String::from_utf8(output.bytes).map_err(|error| TemplateError::Render(error.to_string()))
}

fn has_loader_backed_construct(source: &str) -> bool {
    source.split("{%").skip(1).any(|tag| {
        let statement = tag
            .trim_start()
            .strip_prefix('-')
            .unwrap_or(tag.trim_start())
            .trim_start();
        ["extends", "include", "import"].into_iter().any(|keyword| {
            statement.strip_prefix(keyword).is_some_and(|remainder| {
                remainder
                    .chars()
                    .next()
                    .is_none_or(|character| !(character == '_' || character.is_alphanumeric()))
            })
        })
    })
}

fn template_source<'a>(theme: &'a CompiledTheme, name: &str) -> Option<&'a str> {
    theme
        .theme
        .menu
        .templates
        .get(name)
        .or_else(|| theme.theme.common.templates.get(name))
        .map(String::as_str)
}

fn archived_template_source<'a>(
    theme: &'a ArchivedCompiledThemeWire,
    name: &str,
) -> Option<&'a str> {
    theme
        .menu
        .templates
        .iter()
        .find(|template| template.name.as_str() == name)
        .or_else(|| {
            theme
                .common
                .templates
                .iter()
                .find(|template| template.name.as_str() == name)
        })
        .map(|template| template.value.as_str())
}

fn resolve_styles(theme: &CompiledTheme) -> Result<BTreeMap<String, RatatuiStyle>, TemplateError> {
    let mut output = BTreeMap::new();
    for section in [&theme.theme.common, &theme.theme.menu] {
        for (name, style) in &section.styles {
            output.insert(name.clone(), resolve_style(theme, style)?);
        }
    }
    Ok(output)
}

fn resolve_archived_styles(
    theme: &ArchivedCompiledThemeWire,
) -> Result<BTreeMap<String, RatatuiStyle>, TemplateError> {
    let mut output = BTreeMap::new();
    for section in [&theme.common, &theme.menu] {
        for named_style in section.styles.iter() {
            output.insert(
                named_style.name.as_str().to_owned(),
                resolve_archived_style(theme, &named_style.style)?,
            );
        }
    }
    Ok(output)
}

fn resolve_style(theme: &CompiledTheme, style: &Style) -> Result<RatatuiStyle, TemplateError> {
    let mut resolved = RatatuiStyle::default();
    if let Some(color) = &style.foreground {
        resolved = resolved.fg(resolve_color(theme, color)?);
    }
    if let Some(color) = &style.background {
        resolved = resolved.bg(resolve_color(theme, color)?);
    }
    for (enabled, modifier) in [
        (style.bold, Modifier::BOLD),
        (style.dim, Modifier::DIM),
        (style.italic, Modifier::ITALIC),
        (style.underline, Modifier::UNDERLINED),
        (style.strikethrough, Modifier::CROSSED_OUT),
    ] {
        if enabled {
            resolved = resolved.add_modifier(modifier);
        }
    }
    Ok(resolved)
}

fn resolve_archived_style(
    theme: &ArchivedCompiledThemeWire,
    style: &ArchivedStyleWire,
) -> Result<RatatuiStyle, TemplateError> {
    let mut resolved = RatatuiStyle::default();
    if let Some(color) = style.foreground.as_ref() {
        resolved = resolved.fg(resolve_archived_color(theme, color.as_str())?);
    }
    if let Some(color) = style.background.as_ref() {
        resolved = resolved.bg(resolve_archived_color(theme, color.as_str())?);
    }
    for (enabled, modifier) in [
        (style.bold, Modifier::BOLD),
        (style.dim, Modifier::DIM),
        (style.italic, Modifier::ITALIC),
        (style.underline, Modifier::UNDERLINED),
        (style.strikethrough, Modifier::CROSSED_OUT),
    ] {
        if enabled {
            resolved = resolved.add_modifier(modifier);
        }
    }
    Ok(resolved)
}

fn resolve_color(theme: &CompiledTheme, color: &str) -> Result<RatatuiColor, TemplateError> {
    let color = if color.starts_with('#') {
        Color::parse(color)
    } else {
        theme.scheme.resolve(color)
    }
    .map_err(|error| TemplateError::Render(error.to_string()))?;
    Ok(match color {
        Color::Inherit => RatatuiColor::Reset,
        Color::Rgb { red, green, blue } => RatatuiColor::Rgb(red, green, blue),
    })
}

fn resolve_archived_color(
    theme: &ArchivedCompiledThemeWire,
    color: &str,
) -> Result<RatatuiColor, TemplateError> {
    let mut seen = BTreeSet::new();
    let color = resolve_archived_color_inner(theme, color, &mut seen)?;
    Ok(match color {
        Color::Inherit => RatatuiColor::Reset,
        Color::Rgb { red, green, blue } => RatatuiColor::Rgb(red, green, blue),
    })
}

fn resolve_archived_color_inner(
    theme: &ArchivedCompiledThemeWire,
    color: &str,
    seen: &mut BTreeSet<String>,
) -> Result<Color, TemplateError> {
    if color == "inherit" {
        return Ok(Color::Inherit);
    }
    if color.starts_with('#') {
        return Color::parse(color).map_err(|error| TemplateError::Render(error.to_string()));
    }
    if !seen.insert(color.to_owned()) {
        return Err(TemplateError::Render(format!(
            "color alias cycle at `{color}`"
        )));
    }
    let value = theme
        .scheme
        .colors
        .iter()
        .find(|entry| entry.name.as_str() == color)
        .or_else(|| {
            theme
                .scheme
                .palette
                .iter()
                .find(|entry| entry.name.as_str() == color)
        })
        .ok_or_else(|| {
            TemplateError::Render(format!("unknown palette or semantic color `{color}`"))
        })?;
    resolve_archived_color_inner(theme, value.value.as_str(), seen)
}

fn parse_style_markup(
    rendered: &str,
    styles: &BTreeMap<String, RatatuiStyle>,
) -> Result<RenderedText, TemplateError> {
    let mut spans = Vec::new();
    let mut plain = String::new();
    let mut stack: Vec<(String, RatatuiStyle)> = Vec::new();
    let mut text = String::new();
    let mut characters = rendered.chars().peekable();

    while let Some(character) = characters.next() {
        if character == '\\' && matches!(characters.peek(), Some('[' | ']')) {
            text.push(characters.next().expect("peeked character"));
            continue;
        }
        if character != '[' {
            text.push(character);
            continue;
        }

        let mut tag = String::new();
        let mut closed = false;
        for tag_character in characters.by_ref() {
            if tag_character == ']' {
                closed = true;
                break;
            }
            tag.push(tag_character);
        }
        if !closed {
            text.push('[');
            text.push_str(&tag);
            break;
        }
        flush_span(
            &mut spans,
            &mut plain,
            &mut text,
            stack.last().map(|(_, style)| *style),
        );
        if let Some(name) = tag.strip_prefix('/') {
            if stack.last().is_some_and(|(open, _)| open == name) {
                stack.pop();
            }
        } else if let Some(style) = styles.get(&tag) {
            stack.push((tag, *style));
        }
    }
    flush_span(
        &mut spans,
        &mut plain,
        &mut text,
        stack.last().map(|(_, style)| *style),
    );
    Ok(RenderedText { plain, spans })
}

fn flush_span(
    spans: &mut Vec<RenderedSpan>,
    plain: &mut String,
    text: &mut String,
    style: Option<RatatuiStyle>,
) {
    if text.is_empty() {
        return;
    }
    plain.push_str(text);
    let style = style.unwrap_or_default();
    if let Some(last) = spans.last_mut().filter(|last| last.style == style) {
        last.text.push_str(text);
    } else {
        spans.push(RenderedSpan {
            text: core::mem::take(text),
            style,
        });
        return;
    }
    text.clear();
}

fn rpad_filter(value: String, width: usize, pad: Option<String>) -> Result<String, MiniError> {
    pad_filter(value, width, pad, false)
}

fn lpad_filter(value: String, width: usize, pad: Option<String>) -> Result<String, MiniError> {
    pad_filter(value, width, pad, true)
}

fn pad_filter(
    value: String,
    width: usize,
    pad: Option<String>,
    left: bool,
) -> Result<String, MiniError> {
    if width > MAX_COMPONENT_BYTES {
        return Err(filter_error("padding width exceeds the component limit"));
    }
    let pad = pad.unwrap_or_else(|| " ".into());
    let pad_width = UnicodeWidthStr::width(pad.as_str());
    if pad_width == 0 {
        return Err(filter_error("padding string has zero display width"));
    }
    let value = truncate_to_width(&value, width);
    let missing = width.saturating_sub(UnicodeWidthStr::width(value.as_str()));
    let padding = repeat_to_width(&pad, missing)?;
    let mut output = String::with_capacity(value.len() + padding.len());
    if left {
        output.push_str(&padding);
    }
    output.push_str(&value);
    if !left {
        output.push_str(&padding);
    }
    if output.len() > MAX_COMPONENT_BYTES {
        return Err(filter_error("padding output exceeds the component limit"));
    }
    Ok(output)
}

fn ellipsis_filter(value: String, width: usize) -> Result<String, MiniError> {
    if width > MAX_COMPONENT_BYTES {
        return Err(filter_error("ellipsis width exceeds the component limit"));
    }
    let output = ellipsize(&value, width);
    if output.len() > MAX_COMPONENT_BYTES {
        return Err(filter_error("ellipsis output exceeds the component limit"));
    }
    Ok(output)
}

fn style_filter(value: String, name: String) -> Result<String, MiniError> {
    let output = format!("[{name}]{value}[/{name}]");
    if output.len() > MAX_COMPONENT_BYTES {
        return Err(filter_error("style output exceeds the component limit"));
    }
    Ok(output)
}

fn truncate_to_width(value: &str, width: usize) -> String {
    let mut output = String::new();
    let mut used = 0;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used + character_width > width {
            break;
        }
        output.push(character);
        used += character_width;
    }
    output
}

fn repeat_to_width(pad: &str, width: usize) -> Result<String, MiniError> {
    let mut output = String::new();
    while UnicodeWidthStr::width(output.as_str()) < width {
        output.push_str(pad);
        if output.len() > MAX_COMPONENT_BYTES {
            return Err(filter_error("padding output exceeds the component limit"));
        }
    }
    Ok(truncate_to_width(&output, width))
}

fn filter_error(message: &'static str) -> MiniError {
    MiniError::new(ErrorKind::InvalidOperation, message)
}

struct BoundedWriter {
    bytes: Vec<u8>,
    exceeded: bool,
}

impl BoundedWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(1024),
            exceeded: false,
        }
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let remaining = MAX_COMPONENT_BYTES.saturating_sub(self.bytes.len());
        if bytes.len() > remaining {
            self.bytes.extend_from_slice(&bytes[..remaining]);
            self.exceeded = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "component output limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use muxe_core::{ColorScheme, Theme, ThemeSection};

    use super::*;

    fn theme_with_templates(cell: &str) -> CompiledTheme {
        CompiledTheme::compile(
            Theme {
                common: ThemeSection::default(),
                menu: ThemeSection {
                    styles: BTreeMap::from([(
                        "hotkey".into(),
                        Style {
                            bold: true,
                            ..Style::default()
                        },
                    )]),
                    templates: BTreeMap::from([
                        ("cell".into(), cell.into()),
                        ("breadcrumbs".into(), "{{ crumbs | join(' › ') }}".into()),
                        (
                            "pagination.full".into(),
                            "{{ prev_key }} {{ pages.current }}/{{ pages.count }} {{ next_key }}"
                                .into(),
                        ),
                        (
                            "pagination.short".into(),
                            "{{ pages.current }}/{{ pages.count }}".into(),
                        ),
                        ("status".into(), "{{ level }} {{ message }}".into()),
                    ]),
                },
                settings: BTreeMap::new(),
            },
            ColorScheme {
                title: "test".into(),
                palette: BTreeMap::new(),
                colors: BTreeMap::new(),
            },
        )
        .expect("fixture uses a valid core theme")
    }

    #[test]
    fn environment_is_reused_and_cell_markup_becomes_styled_text() {
        let renderer = TemplateRenderer::new(&theme_with_templates(
            "{% if blocked %}[hotkey]! [/hotkey]{% endif %}{{ key | rpad(6, ' ') }} → {{ title }}",
        ))
        .expect("theme compiles once");
        let first = renderer
            .render_cell(CellTemplate {
                key: "a",
                title: "open",
                disabled: false,
                blocked: true,
                max_title_width: 24,
            })
            .expect("cell renders");
        let second = renderer
            .render_cell(CellTemplate {
                key: "b",
                title: "build",
                disabled: false,
                blocked: false,
                max_title_width: 24,
            })
            .expect("same environment renders again");
        assert_eq!(first.plain, "! a      → open");
        assert_eq!(second.plain, "b      → build");
        assert_eq!(first.spans[0].text, "! ");
        assert!(first.spans[0].style.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn cell_values_cannot_create_style_markup() {
        let renderer = TemplateRenderer::new(&theme_with_templates("{{ key }} {{ title }}"))
            .expect("theme compiles");
        let rendered = renderer
            .render_cell(CellTemplate {
                key: "[hotkey]x[/hotkey]",
                title: "line\u{0007} one",
                disabled: false,
                blocked: false,
                max_title_width: 24,
            })
            .expect("cell renders");
        assert_eq!(rendered.plain, "[hotkey]x[/hotkey] line one");
    }

    #[test]
    fn loader_backed_constructs_cannot_reference_registered_component_templates() {
        for source in [
            "{% extends 'cell' %}",
            "{% include 'status' %}",
            "{% import 'status' as registered %}",
        ] {
            let templates = BTreeMap::from([
                ("cell", source),
                ("breadcrumbs", "{{ crumbs | join(' › ') }}"),
                ("pagination.full", "{{ pages.current }}/{{ pages.count }}"),
                ("pagination.short", "{{ pages.current }}/{{ pages.count }}"),
                ("status", "registered component"),
            ]);
            let error = match TemplateRenderer::from_sources(
                |name| templates.get(name).copied(),
                BTreeMap::new(),
            ) {
                Ok(_) => panic!("loader-backed `{source}` must be rejected"),
                Err(error) => error,
            };
            assert!(matches!(
                error,
                TemplateError::ForbiddenLoaderBackedConstruct("cell")
            ));
        }
    }

    #[test]
    fn output_limit_stops_rendering_before_an_unbounded_string_exists() {
        let renderer =
            TemplateRenderer::new(&theme_with_templates("{{ title }}")).expect("theme compiles");
        let title = "x".repeat(MAX_COMPONENT_BYTES + 1);
        let error = renderer
            .render_cell(CellTemplate {
                key: "k",
                title: &title,
                disabled: false,
                blocked: false,
                max_title_width: MAX_COMPONENT_BYTES + 1,
            })
            .expect_err("oversized component must fail");
        assert!(matches!(error, TemplateError::OutputTooLarge));
    }
}
