use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};

use minijinja::{Environment, Error as MiniError, ErrorKind, UndefinedBehavior, context};
use muxe_core::{
    Color, CompiledTheme, REQUIRED_COMPONENT_TEMPLATES, Style, has_loader_backed_construct,
};
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
    ///
    /// [`CompiledTheme::compile`] already proves every
    /// [`REQUIRED_COMPONENT_TEMPLATES`](muxe_core::REQUIRED_COMPONENT_TEMPLATES) entry
    /// resolves and rejects loader-backed constructs, so the [`TemplateError::MissingTemplate`]
    /// and [`TemplateError::ForbiddenLoaderBackedConstruct`] arms below are a defensive
    /// backstop for themes built outside that path (notably [`Self::from_archived`], which
    /// reads archived wire bytes that were not re-proven at compile time).
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::MissingTemplate`] when a required component template is absent,
    /// [`TemplateError::ForbiddenLoaderBackedConstruct`] when a template uses a loader-backed
    /// construct, or [`TemplateError::Render`] when `MiniJinja` rejects a template.
    pub fn new(theme: &CompiledTheme) -> Result<Self, TemplateError> {
        Self::from_sources(|name| template_source(theme, name), resolve_styles(theme)?)
    }

    /// Compiles the resolved, checked theme embedded in a broker attachment without
    /// deserializing the attachment's menu graph.
    ///
    /// This is the one constructor that can still observe a missing or loader-backed
    /// template first: the archived bytes travel outside [`CompiledTheme::compile`]'s proof,
    /// so the same two error arms remain the enforcement point here rather than a backstop.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::MissingTemplate`] when a required component template is absent,
    /// [`TemplateError::ForbiddenLoaderBackedConstruct`] when a template uses a loader-backed
    /// construct, or [`TemplateError::Render`] when `MiniJinja` rejects a template.
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

        for name in REQUIRED_COMPONENT_TEMPLATES {
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
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::OutputTooLarge`] when the escaped input or rendered component
    /// exceeds the component byte limit, or [`TemplateError::Render`] when `MiniJinja` rendering
    /// fails.
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

    /// Renders the breadcrumb trail for the current menu stack.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::OutputTooLarge`] when the escaped crumbs or rendered component
    /// exceeds the component byte limit, or [`TemplateError::Render`] when `MiniJinja` rendering
    /// fails.
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

    /// Renders the full pagination component for a multi-page menu.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::OutputTooLarge`] when the escaped keys or rendered component
    /// exceeds the component byte limit, or [`TemplateError::Render`] when `MiniJinja` rendering
    /// fails.
    pub fn render_pagination_full(
        &self,
        pagination: PaginationTemplate<'_>,
    ) -> Result<RenderedText, TemplateError> {
        self.render_pagination("pagination.full", pagination)
    }

    /// Renders the short pagination component when the full one does not fit.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::OutputTooLarge`] when the escaped keys or rendered component
    /// exceeds the component byte limit, or [`TemplateError::Render`] when `MiniJinja` rendering
    /// fails.
    pub fn render_pagination_short(
        &self,
        pagination: PaginationTemplate<'_>,
    ) -> Result<RenderedText, TemplateError> {
        self.render_pagination("pagination.short", pagination)
    }

    /// Renders the status line for a recoverable broker diagnostic.
    ///
    /// # Errors
    ///
    /// Returns [`TemplateError::OutputTooLarge`] when the escaped status or rendered component
    /// exceeds the component byte limit, or [`TemplateError::Render`] when `MiniJinja` rendering
    /// fails.
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
        Ok(parse_style_markup(&rendered, &self.styles))
    }
}

/// Escapes text that could otherwise be interpreted as style markup or a control character.
///
/// # Errors
///
/// Returns [`TemplateError::OutputTooLarge`] when the escaped text exceeds the component byte
/// limit, or [`TemplateError::Render`] when the escaped bytes are not valid UTF-8.
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

fn parse_style_markup(rendered: &str, styles: &BTreeMap<String, RatatuiStyle>) -> RenderedText {
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
    RenderedText { plain, spans }
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

fn rpad_filter(value: &str, width: usize, pad: Option<String>) -> Result<String, MiniError> {
    pad_filter(value, width, pad, false)
}

fn lpad_filter(value: &str, width: usize, pad: Option<String>) -> Result<String, MiniError> {
    pad_filter(value, width, pad, true)
}

fn pad_filter(
    value: &str,
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
    let value = truncate_to_width(value, width);
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

fn ellipsis_filter(value: &str, width: usize) -> Result<String, MiniError> {
    if width > MAX_COMPONENT_BYTES {
        return Err(filter_error("ellipsis width exceeds the component limit"));
    }
    let output = ellipsize(value, width);
    if output.len() > MAX_COMPONENT_BYTES {
        return Err(filter_error("ellipsis output exceeds the component limit"));
    }
    Ok(output)
}

fn style_filter(value: &str, name: &str) -> Result<String, MiniError> {
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

/// Measures display width for the padding fill path.
///
/// Every `UnicodeWidthStr::width` call in the fill path goes through this
/// helper so the linearity guard cannot be bypassed by re-measuring the
/// growing buffer with a raw call: test builds count invocations and assert
/// a constant bound independent of the output size. Production builds pay
/// nothing for it.
fn pad_str_width(value: &str) -> usize {
    #[cfg(test)]
    PAD_WIDTH_PROBES.with(|probes| probes.set(probes.get() + 1));
    UnicodeWidthStr::width(value)
}

#[cfg(test)]
thread_local! {
    static PAD_WIDTH_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_pad_width_probes() {
    PAD_WIDTH_PROBES.with(|probes| probes.set(0));
}

#[cfg(test)]
fn pad_width_probes() -> usize {
    PAD_WIDTH_PROBES.with(std::cell::Cell::get)
}

/// Reports whether repeating `pad` grows display width additively.
///
/// `UnicodeWidthStr::width` is context-sensitive, so a fill's measured width
/// need not equal the sum of its copies' widths: a ZWJ-terminated scalar
/// measures 2 for any repeat count, and VS16 + heart measures 1, 3, 5 for
/// one, two, three copies. A single scalar cannot join across copies, so its
/// width is additive and the sized path is safe without measuring. A
/// multi-scalar fill takes the sized path only when
/// `width(pad.repeat(2)) == 2 * pad_width` and
/// `width(pad.repeat(3)) == 3 * pad_width`; the two-copy probe alone is not
/// enough (1, 3, 5 passes it at n = 2 but fails at n = 3). Every probe read
/// goes through the counting helper; the counter itself is `#[cfg(test)]`-only
/// so production pays only the width computations (at most three reads).
fn fill_is_additive(pad: &str, pad_width: usize) -> bool {
    if pad.chars().count() == 1 {
        return true;
    }
    let double = pad.repeat(2);
    if pad_str_width(double.as_str()) != 2 * pad_width {
        return false;
    }
    let triple = pad.repeat(3);
    pad_str_width(triple.as_str()) == 3 * pad_width
}

/// The pre-change greedy loop, verbatim: append one copy, re-measure the
/// accumulated fill, enforce the byte limit on the accumulated untruncated
/// size after each append, stop as soon as the measured width reaches the
/// target, then `truncate_to_width`.
///
/// The non-additive fallback below calls this so exotic fills return
/// byte-identical results and errors (same bytes, same
/// `"padding output exceeds the component limit"` message, same
/// boundaries). Its cost is bounded by `MAX_COMPONENT_BYTES` pad bytes
/// rather than linear in produced output: each append re-measures the
/// accumulated fill. Making it linear would change observable results for
/// context-sensitive fills (the sized fill overshoots where the old loop
/// stopped early, or undershoots where boundary joins add width), so the
/// fallback keeps the loop deliberately. This residual is a known,
/// deliberate limitation, accepted to preserve exact equivalence.
fn greedy_repeat_to_width(pad: &str, width: usize) -> Result<String, MiniError> {
    let mut output = String::new();
    while pad_str_width(output.as_str()) < width {
        output.push_str(pad);
        if output.len() > MAX_COMPONENT_BYTES {
            return Err(filter_error("padding output exceeds the component limit"));
        }
    }
    Ok(truncate_to_width(&output, width))
}

fn repeat_to_width(pad: &str, width: usize) -> Result<String, MiniError> {
    let pad_width = pad_str_width(pad);
    if pad_width == 0 {
        return Err(filter_error("padding string has zero display width"));
    }
    if !fill_is_additive(pad, pad_width) {
        return greedy_repeat_to_width(pad, width);
    }
    // Size the fill up front: the smallest copy count whose summed per-copy
    // widths reach (or pass) `width`. A width-2 fill at an odd target still
    // overshoots and `truncate_to_width` still trims the overhang, preserving
    // the old undershoot. `str::repeat` builds the fill in one shot, and the
    // byte check stays over the pre-truncation size, as the old loop did.
    let copies = width.div_ceil(pad_width);
    if copies.saturating_mul(pad.len()) > MAX_COMPONENT_BYTES {
        return Err(filter_error("padding output exceeds the component limit"));
    }
    let fill = pad.repeat(copies);
    Ok(truncate_to_width(&fill, width))
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
        let cell = renderer
            .render_cell(CellTemplate {
                key: "[hotkey]x[/hotkey]",
                title: "line\u{0007} one",
                disabled: false,
                blocked: false,
                max_title_width: 24,
            })
            .expect("cell renders");
        assert_eq!(cell.plain, "[hotkey]x[/hotkey] line one");
    }

    #[test]
    fn renderer_requires_exactly_the_core_template_registry() {
        let mut missing = BTreeMap::from([
            ("cell", "{{ title }}"),
            ("breadcrumbs", "{{ crumbs | join(' › ') }}"),
            ("pagination.full", "{{ pages.current }}/{{ pages.count }}"),
            ("pagination.short", "{{ pages.current }}/{{ pages.count }}"),
            ("status", "registered component"),
        ]);
        for name in REQUIRED_COMPONENT_TEMPLATES {
            let removed = missing.remove(name).expect("registry entry has a fixture");
            let Err(error) =
                TemplateRenderer::from_sources(|key| missing.get(key).copied(), BTreeMap::new())
            else {
                panic!("renderer must require `{name}`")
            };
            assert!(
                matches!(error, TemplateError::MissingTemplate(got) if got == name),
                "renderer must name missing `{name}`, got `{error:?}`"
            );
            missing.insert(name, removed);
        }
        TemplateRenderer::from_sources(|key| missing.get(key).copied(), BTreeMap::new())
            .expect("the full core registry must construct the renderer");
    }

    #[test]
    fn renderer_rejects_the_same_loader_spellings_as_core() {
        for source in [
            "{% extends 'cell' %}",
            "{% include 'status' %}",
            "{% import 'status' as registered %}",
            "{%- include 'status' %}",
            "{%- import 'status' as registered %}",
            "{% from 'status' import registered %}",
            "{%- from 'status' import registered %}",
            "{%from 'status' import registered%}",
        ] {
            assert!(
                has_loader_backed_construct(source),
                "core must reject `{source}` exactly as the renderer does"
            );
            let templates = BTreeMap::from([
                ("cell", source),
                ("breadcrumbs", "{{ crumbs | join(' › ') }}"),
                ("pagination.full", "{{ pages.current }}/{{ pages.count }}"),
                ("pagination.short", "{{ pages.current }}/{{ pages.count }}"),
                ("status", "registered component"),
            ]);
            let Err(error) = TemplateRenderer::from_sources(
                |name| templates.get(name).copied(),
                BTreeMap::new(),
            ) else {
                panic!("loader-backed `{source}` must be rejected")
            };
            assert!(matches!(
                error,
                TemplateError::ForbiddenLoaderBackedConstruct("cell")
            ));
        }
    }

    #[test]
    fn compiled_theme_from_core_registry_always_constructs_the_renderer() {
        let theme = theme_with_templates("{{ title }}");
        TemplateRenderer::new(&theme).expect("a compiled theme must be renderable");
        for missing in REQUIRED_COMPONENT_TEMPLATES {
            let mut templates = BTreeMap::from([
                ("cell".to_owned(), "{{ title }}".to_owned()),
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
            ]);
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
                ColorScheme {
                    title: "test".into(),
                    palette: BTreeMap::new(),
                    colors: BTreeMap::new(),
                },
            )
            .unwrap_err();
            assert!(
                error.to_string().contains(missing),
                "removing `{missing}` must fail compilation, got `{error}`"
            );
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

    fn pad_error_message(error: &MiniError) -> &str {
        error
            .detail()
            .expect("padding errors carry a detail message")
    }

    #[test]
    fn padding_fills_to_exact_width_with_single_width_fill() {
        let padded = rpad_filter("ab", 5, Some(" ".into())).expect("padding fits");
        assert_eq!(padded, "ab   ");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 5);
        let padded = lpad_filter("ab", 5, Some("-".into())).expect("padding fits");
        assert_eq!(padded, "---ab");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 5);
    }

    #[test]
    fn padding_with_wide_fill_keeps_the_old_undershoot() {
        // The greedy loop overshot an odd target, then `truncate_to_width`
        // dropped the overhanging wide char instead of exceeding the width.
        let padded = rpad_filter("", 3, Some("日".into())).expect("padding fits");
        assert_eq!(padded, "日");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 2);
        let padded = rpad_filter("", 4, Some("日".into())).expect("padding fits");
        assert_eq!(padded, "日日");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 4);
    }

    #[test]
    fn padding_handles_empty_at_width_and_over_width_values() {
        let padded = rpad_filter("", 3, Some(" ".into())).expect("padding fits");
        assert_eq!(padded, "   ");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 3);
        let padded = rpad_filter("abc", 3, Some(" ".into())).expect("padding fits");
        assert_eq!(padded, "abc");
        let padded = rpad_filter("abcdef", 3, Some(" ".into())).expect("padding fits");
        assert_eq!(padded, "abc");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 3);
    }

    #[test]
    fn padding_rejects_a_zero_width_fill_before_any_fill_work() {
        for pad in ["", "\u{0301}"] {
            let error = rpad_filter("ab", 5, Some(pad.into())).expect_err("no fill width");
            assert_eq!(error.kind(), ErrorKind::InvalidOperation);
            assert_eq!(
                pad_error_message(&error),
                "padding string has zero display width"
            );
            let error = repeat_to_width(pad, 5).expect_err("no fill width");
            assert_eq!(error.kind(), ErrorKind::InvalidOperation);
            assert_eq!(
                pad_error_message(&error),
                "padding string has zero display width"
            );
        }
    }

    #[test]
    fn padding_keeps_an_additive_multi_character_fill_on_the_sized_path() {
        // "ab" is additive (2, 4, 6 for one, two, three copies), so the
        // probe passes and the sized path must return the loop outcome at
        // several widths across the range, including the odd-target
        // overshoot-then-truncate and the limit-adjacent edges.
        assert!(fill_is_additive("ab", UnicodeWidthStr::width("ab")));
        for (target, expected) in [
            (2, "ab".to_string()),
            (3, "aba".to_string()),
            (5, "ababa".to_string()),
            (6, "ababab".to_string()),
            (1024, "ab".repeat(512)),
            (8192, "ab".repeat(4096)),
            (MAX_COMPONENT_BYTES - 1, "ab".repeat(32_767) + "a"),
            (MAX_COMPONENT_BYTES, "ab".repeat(32_768)),
        ] {
            let padded = repeat_to_width("ab", target)
                .expect("pre-change loop succeeded within the byte limit");
            assert_eq!(
                padded, expected,
                "pre-change loop built `{expected}` for an additive fill at width {target}"
            );
            assert_eq!(
                UnicodeWidthStr::width(padded.as_str()),
                target.min(expected.len())
            );
            reset_pad_width_probes();
            let padded =
                repeat_to_width("ab", target).expect("additive fill stays on the sized path");
            assert_eq!(
                padded, expected,
                "sized path must match the pre-change loop at width {target}"
            );
            assert_eq!(
                pad_width_probes(),
                3,
                "an additive multi-character fill costs exactly 3 width reads \
                 (pad + 2-copy probe + 3-copy probe) at width {target}"
            );
            let padded = rpad_filter("", target, Some("ab".into()))
                .expect("pre-change filter succeeded within the byte limit");
            assert_eq!(
                padded, expected,
                "pre-change filter built `{expected}` for an additive fill at width {target}"
            );
        }
        let error = repeat_to_width("ab", MAX_COMPONENT_BYTES + 1)
            .expect_err("pre-change loop failed past the byte limit");
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
    }

    #[test]
    fn padding_matches_the_old_loop_for_a_never_growing_fill() {
        // "👩‍" is U+1F469 U+200D: width 2 alone and width 2 for any repeat,
        // so the pre-change loop never reached its target and failed on
        // bytes at every target below.
        let pad = "👩‍";
        assert_eq!(UnicodeWidthStr::width(pad), 2);
        assert_eq!(
            UnicodeWidthStr::width(pad.repeat(2).as_str()),
            2,
            "the fill is not additive, which is what makes this case special"
        );
        assert!(!fill_is_additive(pad, 2));
        for target in [3, 4, 15_000, 21_843, 21_844] {
            let error = repeat_to_width(pad, target)
                .expect_err("pre-change loop failed on bytes for a never-growing fill");
            assert_eq!(error.kind(), ErrorKind::InvalidOperation);
            assert_eq!(
                pad_error_message(&error),
                "padding output exceeds the component limit",
                "pre-change loop failed on bytes at target {target}"
            );
            let error = rpad_filter("", target, Some(pad.into()))
                .expect_err("pre-change filter failed on bytes for a never-growing fill");
            assert_eq!(error.kind(), ErrorKind::InvalidOperation);
            assert_eq!(
                pad_error_message(&error),
                "padding output exceeds the component limit",
                "pre-change filter failed on bytes at target {target}"
            );
        }
        let error = lpad_filter("", 4, Some(pad.into()))
            .expect_err("pre-change left filter failed on bytes for a never-growing fill");
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
    }

    #[test]
    fn padding_matches_the_old_loop_for_a_super_additive_fill() {
        // "\u{FE0F}❤" is VS16 + heart: width 1 alone but 3 for two copies and
        // 5 for three, so the sized path would overshoot where the pre-change
        // loop stopped early. The probe must fail and the fallback must rerun
        // the loop byte-for-byte, succeeding at 15000 and 21843 (10922
        // copies, 65,532 bytes) and failing only at 21844.
        let pad = "\u{FE0F}❤";
        assert_eq!(UnicodeWidthStr::width(pad), 1);
        assert_eq!(UnicodeWidthStr::width(pad.repeat(2).as_str()), 3);
        assert_eq!(UnicodeWidthStr::width(pad.repeat(3).as_str()), 5);
        assert!(!fill_is_additive(pad, 1));
        for (target, copies, bytes, width) in [
            (3, 2, 12, 3),
            (4, 3, 18, 5),
            (15_000, 7501, 45_006, 15_001),
            (21_843, 10_922, 65_532, 21_843),
        ] {
            let expected = pad.repeat(copies);
            assert_eq!(expected.len(), bytes);
            let padded = repeat_to_width(pad, target).expect(
                "pre-change loop stopped early at the first measured width reaching the target",
            );
            assert_eq!(
                padded, expected,
                "pre-change loop built {copies} copies ({bytes} bytes) at target {target}"
            );
            assert_eq!(UnicodeWidthStr::width(padded.as_str()), width);
            let padded = rpad_filter("", target, Some(pad.into()))
                .expect("pre-change filter agreed with the loop");
            assert_eq!(
                padded, expected,
                "pre-change filter built {copies} copies ({bytes} bytes) at target {target}"
            );
        }
        let error = repeat_to_width(pad, 21_844)
            .expect_err("pre-change loop failed on bytes past 10922 copies");
        assert_eq!(error.kind(), ErrorKind::InvalidOperation);
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit",
            "pre-change loop failed on bytes at target 21844"
        );
        let error = rpad_filter("", 21_844, Some(pad.into()))
            .expect_err("pre-change filter failed on bytes at target 21844");
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
    }

    #[test]
    fn padding_keeps_single_character_fills_on_the_sized_path() {
        // A single scalar's width is additive across copies, so both fills
        // below skip the two-copy/three-copy probe and cost exactly one
        // measurement at any size, preserving the width-2 truncation
        // undershoot (target 3 keeps one wide char at width 2).
        assert!(fill_is_additive(" ", 1));
        assert!(fill_is_additive("日", 2));
        for target in [
            1,
            2,
            3,
            4,
            1023,
            1024,
            MAX_COMPONENT_BYTES - 1,
            MAX_COMPONENT_BYTES,
        ] {
            let padded =
                repeat_to_width(" ", target).expect("pre-change loop succeeded for spaces");
            assert_eq!(padded.len(), target);
            reset_pad_width_probes();
            let padded = repeat_to_width(" ", target).expect("space stays sized");
            assert_eq!(
                padded,
                " ".repeat(target),
                "pre-change loop built {target} spaces"
            );
            assert_eq!(
                pad_width_probes(),
                1,
                "a single-character fill costs exactly 1 width read at width {target}"
            );
        }
        let padded = repeat_to_width("日", 3).expect("pre-change loop overshot, then truncated");
        assert_eq!(padded, "日");
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 2);
        let padded = repeat_to_width("日", 4).expect("pre-change loop fit evenly");
        assert_eq!(padded, "日日");
        // The byte check stays over the pre-truncation fill: 32,768 copies of
        // the 3-byte `日` are 98,304 bytes, so the widest fill that fits is
        // 21,845 copies (65,535 bytes) at width 43,690; the pre-change loop
        // agreed, failing only on bytes past that.
        let padded = repeat_to_width("日", 43_690)
            .expect("pre-change loop succeeded just under the byte limit");
        assert_eq!(padded, "日".repeat(21_845));
        assert_eq!(UnicodeWidthStr::width(padded.as_str()), 43_690);
        let error = repeat_to_width("日", MAX_COMPONENT_BYTES + 1)
            .expect_err("pre-change loop failed on bytes past the limit");
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
    }

    #[test]
    fn padding_fails_fast_for_a_never_growing_fill() {
        // A longer non-additive pad ("👩‍".repeat(100), still width 2 for any
        // repeat) hits the byte limit in ~90 appends with the same error as
        // the pre-change loop, keeping this boundedness test fast.
        let pad = "👩‍".repeat(100);
        assert_eq!(UnicodeWidthStr::width(pad.as_str()), 2);
        assert_eq!(UnicodeWidthStr::width(pad.repeat(2).as_str()), 2);
        assert!(!fill_is_additive(&pad, 2));
        let error = repeat_to_width(&pad, 5)
            .expect_err("pre-change loop failed on bytes for a never-growing fill");
        assert_eq!(error.kind(), ErrorKind::InvalidOperation);
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
        let error = rpad_filter("", 5, Some(pad.clone()))
            .expect_err("pre-change filter failed on bytes for a never-growing fill");
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
        reset_pad_width_probes();
        let error = repeat_to_width(&pad, 5).expect_err("fallback still fails on bytes");
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
        assert!(
            pad_width_probes() < 200,
            "the non-additive fallback must stay bounded by the byte limit, \
             not grow with the target"
        );
    }

    #[test]
    fn padding_keeps_the_width_and_byte_limits() {
        let first = rpad_filter("ab", MAX_COMPONENT_BYTES + 1, Some(" ".into()))
            .expect_err("width limit must hold");
        assert_eq!(first.kind(), ErrorKind::InvalidOperation);
        assert_eq!(
            pad_error_message(&first),
            "padding width exceeds the component limit"
        );
        let ok = rpad_filter("ab", 8, Some(" ".into())).expect("limit edge fits");
        assert_eq!(ok, "ab      ");
        // `é` is one column but two bytes, so the byte limit binds first.
        let ok =
            rpad_filter(&"é".repeat(32_768), 32_768, Some(" ".into())).expect("byte edge fits");
        assert_eq!(ok.len(), MAX_COMPONENT_BYTES);
        let error = rpad_filter(&"é".repeat(32_768), 32_769, Some(" ".into()))
            .expect_err("bytes past the limit must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidOperation);
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
        let ok = repeat_to_width(" ", MAX_COMPONENT_BYTES).expect("fill edge fits");
        assert_eq!(ok.len(), MAX_COMPONENT_BYTES);
        let error = repeat_to_width(" ", MAX_COMPONENT_BYTES + 1)
            .expect_err("fill past the limit must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidOperation);
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
        // The byte check stays over the pre-truncation fill: 21,846 copies of
        // the 3-byte `日` are 65,538 bytes, so this fails even though the
        // truncated result would fit in 65,535 bytes.
        let error = rpad_filter("", 43_691, Some("日".into()))
            .expect_err("untruncated wide fill must fail");
        assert_eq!(error.kind(), ErrorKind::InvalidOperation);
        assert_eq!(
            pad_error_message(&error),
            "padding output exceeds the component limit"
        );
    }

    #[test]
    fn padding_scales_linearly_without_remeasuring_the_fill() {
        // The old loop re-measured the whole growing fill on every append,
        // so probes grew with the target (~65k probes for a 64 KiB fill).
        // A single-character fill measures the pad once and never probes,
        // so it costs exactly 1 measurement at every size: any per-append
        // re-measurement on the fast path would break this constant. Every
        // width read in the fill path goes through `pad_str_width`, so a
        // quadratic re-measurement cannot bypass this counter.
        for target in [1024, 8192, MAX_COMPONENT_BYTES] {
            reset_pad_width_probes();
            let padded = repeat_to_width(" ", target).expect("valid padding fits");
            assert_eq!(padded.len(), target);
            assert_eq!(UnicodeWidthStr::width(padded.as_str()), target);
            assert_eq!(
                pad_width_probes(),
                1,
                "a {target}-column fill must cost 1 width read, not one per append"
            );
        }
        // An additive multi-character fill adds the two probe reads.
        for target in [1024, 8192, MAX_COMPONENT_BYTES] {
            reset_pad_width_probes();
            let padded = repeat_to_width("ab", target).expect("valid padding fits");
            assert_eq!(UnicodeWidthStr::width(padded.as_str()), target);
            assert_eq!(
                pad_width_probes(),
                3,
                "a {target}-column additive fill must cost 3 width reads, not one per append"
            );
        }
    }
}
