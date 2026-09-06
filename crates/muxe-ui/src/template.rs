use minijinja::{Environment, UndefinedBehavior, context};
use muxe_core::CompiledTheme;
use thiserror::Error;

const MAX_COMPONENT_BYTES: usize = 64 * 1024;

/// The plain values available to a menu cell template.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CellTemplate<'a> {
    pub key: &'a str,
    pub title: &'a str,
    pub disabled: bool,
    pub blocked: bool,
}

/// A template compilation or render failure.
#[derive(Debug, Error)]
pub enum TemplateError {
    #[error("selected theme has no menu cell template")]
    MissingCellTemplate,
    #[error("menu template error: {0}")]
    Render(String),
    #[error("rendered component exceeds the {MAX_COMPONENT_BYTES}-byte limit")]
    OutputTooLarge,
}

/// Renders the selected theme's menu cell template with strict values and no loader access.
pub fn render_cell_template(
    theme: &CompiledTheme,
    cell: CellTemplate<'_>,
) -> Result<String, TemplateError> {
    let source = theme
        .theme
        .menu
        .templates
        .get("cell")
        .ok_or(TemplateError::MissingCellTemplate)?;
    let mut environment = Environment::new();
    environment.set_undefined_behavior(UndefinedBehavior::Strict);
    environment
        .add_template("cell", source)
        .map_err(|error| TemplateError::Render(error.to_string()))?;
    let rendered = environment
        .get_template("cell")
        .expect("template was inserted")
        .render(context! {
            key => escape_markup_text(cell.key),
            title => escape_markup_text(cell.title),
            disabled => cell.disabled,
            blocked => cell.blocked,
        })
        .map_err(|error| TemplateError::Render(error.to_string()))?;
    if rendered.len() > MAX_COMPONENT_BYTES {
        return Err(TemplateError::OutputTooLarge);
    }
    Ok(rendered)
}

/// Escapes text that could otherwise be interpreted as style markup or a control character.
pub fn escape_markup_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '[' | ']' => {
                escaped.push('\\');
                escaped.push(character);
            }
            character if character.is_control() => {
                use core::fmt::Write as _;
                let _ = write!(escaped, "\\u{{{:04x}}}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use muxe_core::{ColorScheme, Theme, ThemeSection};

    use super::*;

    fn theme_with_cell(template: &str) -> CompiledTheme {
        CompiledTheme::compile(
            Theme {
                common: ThemeSection::default(),
                menu: ThemeSection {
                    styles: BTreeMap::new(),
                    templates: BTreeMap::from([("cell".into(), template.into())]),
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
    fn cell_values_cannot_create_style_markup() {
        let rendered = render_cell_template(
            &theme_with_cell("{{ key }} {{ title }}"),
            CellTemplate {
                key: "[hotkey]x[/hotkey]",
                title: "line\u{0007} one",
                disabled: false,
                blocked: false,
            },
        )
        .expect("cell renders");
        assert_eq!(rendered, "\\[hotkey\\]x\\[/hotkey\\] line\\u{0007} one");
    }

    #[test]
    fn undefined_template_values_fail() {
        let error = render_cell_template(
            &theme_with_cell("{{ unknown }}"),
            CellTemplate {
                key: "k",
                title: "title",
                disabled: false,
                blocked: false,
            },
        )
        .expect_err("strict undefined values must fail");
        assert!(matches!(error, TemplateError::Render(_)));
    }

    #[test]
    fn output_limit_is_enforced_after_rendering() {
        let title = "x".repeat(MAX_COMPONENT_BYTES + 1);
        let error = render_cell_template(
            &theme_with_cell("{{ title }}"),
            CellTemplate {
                key: "k",
                title: &title,
                disabled: false,
                blocked: false,
            },
        )
        .expect_err("oversized component must fail");
        assert!(matches!(error, TemplateError::OutputTooLarge));
    }
}
