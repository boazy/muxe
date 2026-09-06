//!
//! The compiler retains native candidates as ordered kebab-case fields with
//! source spans. This module converts them into the generated raw mirror types
//! (`RawNativeCommand` and `raw::Action`), which then validate through the
//! generated `TryFrom` conversions. The intermediate JSON map is a private
//! in-crate parsing aid exactly like the Herdr adapter's field conversion; no
//! JSON value crosses a crate boundary. Everything leaving this module is a
//! generated typed mirror with the candidate's spans retained for diagnostics.

use muxe_core::{ConfigDiagnostic, ConfigField, ConfigValue, ConfigValueKind, DiagnosticCode};
use muxe_zellij_protocol::generated::{RawNativeCommand, raw};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::names::{NativeType, action_kebab_to_variant, field_to_snake, parse_native_type};

/// Candidate parsing failure with the source span that produced it.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum ParseError {
    /// The candidate type belongs to another adapter.
    #[error("not a Zellij native action: {type_name}")]
    NotZellij {
        /// Candidate discriminator.
        type_name: String,
    },
    /// Unknown or non-exposed command or action name.
    #[error("unknown Zellij native {kind} '{name}'")]
    UnknownName {
        /// `action` or `command`.
        kind: &'static str,
        /// Kebab-case name from the candidate.
        name: String,
    },
    /// A field value or shape the generated mirror rejects.
    #[error("invalid {kind} '{name}': {message}")]
    InvalidArguments {
        /// `action` or `command`.
        kind: &'static str,
        /// Kebab-case member name.
        name: String,
        /// Bounded deserialization error from the generated mirror.
        message: String,
    },
    /// A context reference survived to a point where only concrete values are valid.
    #[error("unresolved context reference in field '{field}'")]
    UnresolvedContext {
        /// Field carrying the marker.
        field: String,
    },
}

impl ParseError {
    /// Renders the failure as a span-anchored configuration diagnostic.
    pub fn to_diagnostic(&self, type_span: &muxe_core::SourceSpan) -> ConfigDiagnostic {
        let (code, message) = match self {
            Self::NotZellij { .. } => (DiagnosticCode::InvalidAction, self.to_string()),
            Self::UnknownName { .. } => (DiagnosticCode::InvalidAction, self.to_string()),
            Self::InvalidArguments { .. } => {
                (DiagnosticCode::InvalidActionArguments, self.to_string())
            }
            Self::UnresolvedContext { .. } => {
                (DiagnosticCode::InvalidContextReference, self.to_string())
            }
        };
        ConfigDiagnostic::error(code, message, type_span.clone())
    }
}

/// Converts ordered candidate fields into a JSON object for mirror parsing.
///
/// Key casing follows the generated serde shapes: high-level command arguments
/// use kebab-case (`rename_all_fields = "kebab-case"` on `RawNativeCommand`),
/// while low-level action fields and every nested mirror struct use
/// snake_case. `top_snake` selects the top level; nested mappings are always
/// snake_case.
///
/// Context markers have no concrete value: load-time validation substitutes a
/// neutral placeholder per context type, exactly like the Herdr boundary, while
/// post-resolution dispatch rejects a surviving marker instead of guessing.
pub fn fields_to_json_map(
    fields: &[ConfigField],
    allow_context: bool,
    top_snake: bool,
) -> Result<Map<String, Value>, ParseError> {
    let mut map = Map::with_capacity(fields.len());
    for field in fields {
        let key = if top_snake {
            field_to_snake(&field.name)
        } else {
            field.name.clone()
        };
        let value = value_to_json(&field.value, &key, allow_context)?;
        map.insert(key, value);
    }
    Ok(map)
}

/// Normalizes a field key for leaf conversion (`pane-id` and `pane_id` alike).
fn leaf_key(key: &str) -> String {
    key.to_ascii_lowercase().replace('-', "_")
}

/// Converts a string leaf holding a pinned text identity (`terminal_<n>`).
fn pane_id_json(text: &str, field: &str) -> Result<Value, ParseError> {
    if let Some(number) = text.strip_prefix("terminal_") {
        return number
            .parse::<u32>()
            .map(|id| Value::Object(Map::from_iter([("Terminal".to_owned(), Value::from(id))])))
            .map_err(|_| ParseError::InvalidArguments {
                kind: "value",
                name: field.to_owned(),
                message: format!("invalid terminal pane ID '{text}'"),
            });
    }
    if let Some(number) = text.strip_prefix("plugin_") {
        return number
            .parse::<u32>()
            .map(|id| Value::Object(Map::from_iter([("Plugin".to_owned(), Value::from(id))])))
            .map_err(|_| ParseError::InvalidArguments {
                kind: "value",
                name: field.to_owned(),
                message: format!("invalid plugin pane ID '{text}'"),
            });
    }
    text.parse::<u32>()
        .map(|id| Value::Object(Map::from_iter([("Terminal".to_owned(), Value::from(id))])))
        .map_err(|_| ParseError::InvalidArguments {
            kind: "value",
            name: field.to_owned(),
            message: format!("pane ID '{text}' is not terminal_<n>, plugin_<n>, or a bare number"),
        })
}

/// Maps a closed kebab literal set to its PascalCase mirror variant.
fn closed_literal(
    text: &str,
    field: &str,
    cases: &[(&str, &str)],
    what: &str,
) -> Result<Value, ParseError> {
    cases
        .iter()
        .find_map(|(kebab, variant)| (*kebab == text).then(|| Value::String((*variant).to_owned())))
        .ok_or_else(|| ParseError::InvalidArguments {
            kind: "value",
            name: field.to_owned(),
            message: format!("invalid {what} '{text}'"),
        })
}

/// Key-directed string-leaf conversion for closed mirror types. Returns `None`
/// when the key carries free text that passes through untouched.
fn typed_leaf(key: &str, text: &str, field: &str) -> Result<Option<Value>, ParseError> {
    match leaf_key(key).as_str() {
        "pane_id" => pane_id_json(text, field).map(Some),
        "direction" => closed_literal(
            text,
            field,
            &[
                ("left", "Left"),
                ("right", "Right"),
                ("up", "Up"),
                ("down", "Down"),
            ],
            "direction",
        )
        .map(Some),
        "resize" => closed_literal(
            text,
            field,
            &[("increase", "Increase"), ("decrease", "Decrease")],
            "resize",
        )
        .map(Some),
        "input_mode" => closed_literal(
            text,
            field,
            &[
                ("normal", "Normal"),
                ("locked", "Locked"),
                ("resize", "Resize"),
                ("pane", "Pane"),
                ("tab", "Tab"),
                ("scroll", "Scroll"),
                ("entersearch", "EnterSearch"),
                ("search", "Search"),
                ("rename-tab", "RenameTab"),
                ("rename-pane", "RenamePane"),
                ("session", "Session"),
                ("move", "Move"),
                ("prompt", "Prompt"),
                ("tmux", "Tmux"),
            ],
            "input mode",
        )
        .map(Some),
        _ => Ok(None),
    }
}

fn value_to_json(value: &ConfigValue, key: &str, allow_context: bool) -> Result<Value, ParseError> {
    match &value.kind {
        ConfigValueKind::Null => Ok(Value::Null),
        ConfigValueKind::Boolean(flag) => Ok(Value::Bool(*flag)),
        ConfigValueKind::Integer(number) => Ok(Value::Number((*number).into())),
        ConfigValueKind::Float(number) => serde_json::Number::from_f64(*number)
            .map(Value::Number)
            .ok_or_else(|| ParseError::InvalidArguments {
                kind: "value",
                name: key.to_owned(),
                message: "non-finite number".to_owned(),
            }),
        ConfigValueKind::String(text) => {
            if let Some(converted) = typed_leaf(key, text, key)? {
                return Ok(converted);
            }
            Ok(Value::String(text.clone()))
        }
        ConfigValueKind::Context(_) if allow_context => {
            // Pane-typed markers need a fitting placeholder: Null cannot
            // validate as a PaneId, while {"Terminal": 0} proves the shape.
            if leaf_key(key) == "pane_id" {
                Ok(Value::Object(Map::from_iter([(
                    "Terminal".to_owned(),
                    Value::from(0),
                )])))
            } else {
                Ok(Value::Null)
            }
        }
        ConfigValueKind::Context(_) => Err(ParseError::UnresolvedContext {
            field: key.to_owned(),
        }),
        ConfigValueKind::Sequence(items) => items
            .iter()
            .map(|item| value_to_json(item, key, allow_context))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        ConfigValueKind::Mapping(entries) => {
            let mut map = Map::with_capacity(entries.len());
            for entry in entries {
                let nested = field_to_snake(&entry.name);
                map.insert(
                    nested.clone(),
                    value_to_json(&entry.value, &nested, allow_context)?,
                );
            }
            Ok(Value::Object(map))
        }
    }
}

/// Parses a candidate into its generated raw mirror.
///
/// The `action` namespace wraps the parsed [`raw::Action`] as
/// [`RawNativeCommand::RunAction`] with an empty plugin context, so the bridge
/// dispatches every low-level action through the typed `run_action` path in
/// generated code. Returns [`ParseError::NotZellij`] for other adapters' types.
pub fn candidate_to_raw(
    type_name: &str,
    fields: &[ConfigField],
    allow_context: bool,
) -> Result<RawNativeCommand, ParseError> {
    match parse_native_type(type_name) {
        None => Err(ParseError::NotZellij {
            type_name: type_name.to_owned(),
        }),
        Some(NativeType::Command(kebab)) => {
            if !crate::names::is_exposed_command(&kebab) {
                return Err(ParseError::UnknownName {
                    kind: "command",
                    name: kebab,
                });
            }
            let mut envelope = Map::with_capacity(2);
            envelope.insert("command".to_owned(), Value::String(kebab.clone()));
            // Unit commands carry no arguments key; struct commands carry
            // their kebab-case argument map.
            if fields.is_empty()
                && let Ok(raw) =
                    serde_json::from_value::<RawNativeCommand>(Value::Object(envelope.clone()))
            {
                return Ok(raw);
            }
            envelope.insert(
                "arguments".to_owned(),
                Value::Object(fields_to_json_map(fields, allow_context, false)?),
            );
            serde_json::from_value(Value::Object(envelope)).map_err(|error| {
                ParseError::InvalidArguments {
                    kind: "command",
                    name: kebab,
                    message: bounded(error.to_string()),
                }
            })
        }
        Some(NativeType::Action(kebab)) => {
            let variant =
                action_kebab_to_variant(&kebab).ok_or_else(|| ParseError::UnknownName {
                    kind: "action",
                    name: kebab.clone(),
                })?;
            // Unit action variants serialize as bare strings; struct variants
            // carry their snake-case field map.
            if fields.is_empty()
                && let Ok(action) =
                    serde_json::from_value::<raw::Action>(Value::String(variant.to_owned()))
            {
                return Ok(RawNativeCommand::RunAction {
                    action,
                    context: Vec::new(),
                });
            }
            let mut envelope = Map::with_capacity(1);
            envelope.insert(
                variant.to_owned(),
                Value::Object(fields_to_json_map(fields, allow_context, true)?),
            );
            let action: raw::Action =
                serde_json::from_value(Value::Object(envelope)).map_err(|error| {
                    ParseError::InvalidArguments {
                        kind: "action",
                        name: kebab,
                        message: bounded(error.to_string()),
                    }
                })?;
            Ok(RawNativeCommand::RunAction {
                action,
                context: Vec::new(),
            })
        }
    }
}

fn bounded(message: String) -> String {
    const LIMIT: usize = 1024;
    if message.len() > LIMIT {
        message[..LIMIT].to_owned()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_core::{ConfigValueKind, SourceId, SourceSpan};

    fn span() -> SourceSpan {
        SourceSpan::new(SourceId::new("<test>"), 0, 1)
    }

    fn value(kind: ConfigValueKind) -> ConfigValue {
        ConfigValue { span: span(), kind }
    }

    fn field(name: &str, kind: ConfigValueKind) -> ConfigField {
        ConfigField {
            name: name.to_owned(),
            name_span: span(),
            value: value(kind),
        }
    }

    #[test]
    fn parses_exposed_command_with_kebab_fields() {
        let fields = [field(
            "should-float-if-hidden",
            ConfigValueKind::Boolean(true),
        )];
        let raw = candidate_to_raw("native.zellij.command:show-self", &fields, false)
            .expect("show-self parses");
        assert!(matches!(raw, RawNativeCommand::ShowSelf { .. }));
    }

    #[test]
    fn unknown_command_fails_closed() {
        let error = candidate_to_raw("native.zellij.command:exec-cmd", &[], false)
            .expect_err("background command is not exposed");
        assert!(matches!(error, ParseError::UnknownName { .. }));
    }

    #[test]
    fn unknown_action_fails_closed() {
        let error = candidate_to_raw("native.zellij.action:fly-to-the-moon", &[], false)
            .expect_err("unknown action is rejected");
        assert!(matches!(error, ParseError::UnknownName { .. }));
    }

    #[test]
    fn foreign_namespace_is_not_zellij() {
        let error = candidate_to_raw("native.herdr.pane:resize", &[], false)
            .expect_err("herdr type is not zellij");
        assert!(matches!(error, ParseError::NotZellij { .. }));
    }

    #[test]
    fn wrong_field_type_reports_arguments() {
        let fields = [field(
            "tab-index",
            ConfigValueKind::String("nope".to_owned()),
        )];
        let error = candidate_to_raw("native.zellij.command:close-tab-with-index", &fields, false)
            .expect_err("string index is rejected");
        assert!(matches!(error, ParseError::InvalidArguments { .. }));
    }

    #[test]
    fn action_wraps_as_run_action() {
        let raw = candidate_to_raw("native.zellij.action:close-focus", &[], false)
            .expect("close-focus parses");
        assert!(matches!(raw, RawNativeCommand::RunAction { .. }));
    }

    #[test]
    fn surviving_context_marker_is_rejected_after_resolution() {
        let reference =
            muxe_core::ContextReference::parse("origin.pane.id", span()).expect("valid path");
        let fields = [field("pane-id", ConfigValueKind::Context(reference))];
        assert!(
            candidate_to_raw("native.zellij.command:close-pane-with-id", &fields, true).is_ok()
        );
        let error = candidate_to_raw("native.zellij.command:close-pane-with-id", &fields, false)
            .expect_err("surviving marker is rejected");
        assert!(matches!(error, ParseError::UnresolvedContext { .. }));
    }
}
