//!
//! The compiler retains native candidates as ordered kebab-case fields with
//! source spans. This module converts them into the generated raw mirror types
//! (`RawNativeCommand` and `raw::Action`), which then validate through the
//! generated `TryFrom` conversions. The intermediate JSON map is a private
//! in-crate parsing aid exactly like the Herdr adapter's field conversion; no
//! JSON value crosses a crate boundary. Everything leaving this module is a
//! generated typed mirror with the candidate's spans retained for diagnostics.

use muxe_core::{
    ConfigDiagnostic, ConfigField, ConfigValue, ConfigValueKind, ContextReference, ContextType,
    DiagnosticCode,
};
use muxe_zellij_protocol::generated::{NATIVE_ZELLIJ_COMMAND_ARGUMENTS, RawNativeCommand, raw};
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
    /// A context reference cannot supply the parameter's declared generated type.
    #[error("context type mismatch: {0}")]
    ContextTypeMismatch(Box<ContextMismatch>),
}

/// A context reference whose declared domain cannot supply the parameter's
/// generated type. Boxed to keep [`ParseError`] small by value.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextMismatch {
    /// Kebab-case member name (`rename-session`, `close-tab-with-index`).
    pub name: String,
    /// Kebab-case parameter name as written (`tab-index`, `name`).
    pub parameter: String,
    /// Closed registry path (`origin.tab.index`).
    pub reference: String,
    /// Declared domain of the reference (`UnsignedInteger`, `PaneId`, ...).
    pub context_type: String,
    /// Generated rust type of the parameter (`usize`, `String`, ...).
    pub expected: String,
    /// `command` or `action`.
    pub kind: &'static str,
    /// `command` phrase or `action` variant detail for the message tail.
    pub member: String,
}

impl std::fmt::Display for ContextMismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "context reference `{}` ({}) cannot supply parameter '{}' of {} '{}': expected {}",
            self.reference,
            self.context_type,
            self.parameter,
            self.member,
            self.name,
            self.expected,
        )
    }
}

impl ParseError {
    /// Renders the failure as a span-anchored configuration diagnostic.
    pub fn to_diagnostic(&self, type_span: &muxe_core::SourceSpan) -> ConfigDiagnostic {
        let (code, message) = match self {
            Self::NotZellij { .. } | Self::UnknownName { .. } => {
                (DiagnosticCode::InvalidAction, self.to_string())
            }
            Self::InvalidArguments { .. } => {
                (DiagnosticCode::InvalidActionArguments, self.to_string())
            }
            Self::UnresolvedContext { .. } => {
                (DiagnosticCode::InvalidContextReference, self.to_string())
            }
            Self::ContextTypeMismatch { .. } => {
                (DiagnosticCode::ContextTypeMismatch, self.to_string())
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
/// `snake_case`. `top_snake` selects the top level; nested mappings are always
/// `snake_case`.
///
/// Context markers have no concrete value: load-time validation substitutes a
/// typed placeholder derived from the parameter's generated rust type and the
/// reference's declared domain, while post-resolution dispatch rejects a
/// surviving marker instead of guessing.
///
/// `member` carries the lookup identity for that derivation: the kebab-case
/// command name (`rename-session`) or the `PascalCase` action variant
/// (`GoToTab`).
fn fields_to_json_map_for_member(
    fields: &[ConfigField],
    allow_context: bool,
    top_snake: bool,
    member: Option<(&str, bool)>,
) -> Result<Map<String, Value>, ParseError> {
    let mut map = Map::with_capacity(fields.len());
    for field in fields {
        let key = if top_snake {
            field_to_snake(&field.name)
        } else {
            field.name.clone()
        };
        let value = value_to_json(&field.value, &key, allow_context, member)?;
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

/// Maps a closed kebab literal set to its `PascalCase` mirror variant.
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

fn value_to_json(
    value: &ConfigValue,
    key: &str,
    allow_context: bool,
    member: Option<(&str, bool)>,
) -> Result<Value, ParseError> {
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
        ConfigValueKind::Context(reference) if allow_context => {
            context_placeholder(reference, key, member)
        }
        ConfigValueKind::Context(_) => Err(ParseError::UnresolvedContext {
            field: key.to_owned(),
        }),
        ConfigValueKind::Sequence(items) => items
            .iter()
            .map(|item| value_to_json(item, key, allow_context, member))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        ConfigValueKind::Mapping(entries) => {
            let mut map = Map::with_capacity(entries.len());
            for entry in entries {
                let nested = field_to_snake(&entry.name);
                map.insert(
                    nested.clone(),
                    value_to_json(&entry.value, &nested, allow_context, member)?,
                );
            }
            Ok(Value::Object(map))
        }
    }
}

/// Derives the load-time placeholder for one unresolved context marker from
/// the parameter's generated rust type and the reference's declared domain.
///
/// The generated type selects the JSON shape the raw mirror accepts; the
/// context domain must be able to supply it. Unsigned integers take `0`,
/// text parameters take `"muxe-context"`, path parameters take `"/"`,
/// Zellij pane identities (`raw::PaneId` and the `(u32, bool)` pane tuple)
/// take `{"Terminal": 0}`, `Option<T>` carries its inner placeholder (a
/// missing optional may equally be omitted), and `Vec<raw::PaneId>` takes
/// the empty array (elements validate only when supplied). Booleans have no
/// v1 context source and any other generated shape has no typed placeholder:
/// the reference is rejected here with the reference path, the parameter,
/// and both types named.
fn context_placeholder(
    reference: &ContextReference,
    key: &str,
    member: Option<(&str, bool)>,
) -> Result<Value, ParseError> {
    let Some((member_name, is_action)) = member else {
        return Err(ParseError::UnresolvedContext {
            field: key.to_owned(),
        });
    };
    let Some(generated) = generated_parameter_type(member_name, is_action, key) else {
        // Not in the pinned surface: keep the unknown-parameter diagnostic
        // stable with the literal path by rejecting the argument itself.
        return Err(ParseError::InvalidArguments {
            kind: if is_action { "action" } else { "command" },
            name: display_member(member_name, is_action),
            message: format!(
                "unknown parameter '{}' for {} '{}'",
                display_key(key, is_action),
                member_kind(member_name, is_action),
                display_member(member_name, is_action),
            ),
        });
    };
    let context_type = reference.expected_type();
    if let Some(placeholder) = placeholder_for(generated, context_type) {
        return Ok(placeholder);
    }
    Err(ParseError::ContextTypeMismatch(Box::new(ContextMismatch {
        name: display_member(member_name, is_action),
        parameter: display_key(key, is_action),
        reference: reference.path.as_str().to_owned(),
        context_type: format!("{context_type:?}"),
        expected: generated.to_owned(),
        kind: if is_action { "action" } else { "command" },
        member: member_kind(member_name, is_action),
    })))
}

/// Looks up the generated rust type for one parameter: `(command, arg)` rows
/// of `NATIVE_ZELLIJ_COMMAND_ARGUMENTS` for commands, `ACTION_VARIANTS` field
/// rows (`"name: Type"`) for actions. Returns `None` when the parameter is
/// not in the pinned surface; the caller then fails closed.
fn generated_parameter_type(member: &str, is_action: bool, key: &str) -> Option<&'static str> {
    if is_action {
        let (_, fields) = muxe_zellij_protocol::generated::ACTION_VARIANTS
            .iter()
            .find(|(variant, _)| *variant == member)?;
        let wanted = leaf_key(key);
        fields.iter().find_map(|entry| {
            let (name, ty) = entry.split_once(':')?;
            (name.trim() == wanted).then(|| ty.trim())
        })
    } else {
        NATIVE_ZELLIJ_COMMAND_ARGUMENTS
            .iter()
            .find(|(command, arg, _, _)| *command == member && leaf_key(arg) == leaf_key(key))
            .map(|(_, _, ty, _)| *ty)
    }
}

/// Selects the placeholder JSON for a generated rust type when the context
/// domain can supply it; `None` means domain and type are incompatible (or
/// the type has no scalar placeholder shape).
fn placeholder_for(generated: &str, context_type: ContextType) -> Option<Value> {
    let normalized: String = generated.chars().filter(|c| !c.is_whitespace()).collect();
    if let Some(inner) = strip_option(&normalized) {
        // An omitted optional is valid, but a supplied marker must still be
        // type-correct: carry the inner placeholder so the generated
        // conversion proves the shape at load time.
        return placeholder_for(inner, context_type);
    }
    if strip_vec(&normalized).is_some() {
        return (context_type == ContextType::PaneId).then(|| Value::Array(Vec::new()));
    }
    if normalized == "(u32,bool)" {
        return (context_type == ContextType::PaneId).then(pane_placeholder);
    }
    if normalized == "raw::PaneId" || normalized == "PaneId" {
        return (context_type == ContextType::PaneId).then(pane_placeholder);
    }
    if normalized == "bool" {
        return None;
    }
    if matches!(normalized.as_str(), "usize" | "u32" | "u64") {
        return (context_type == ContextType::UnsignedInteger).then(|| Value::from(0));
    }
    if normalized == "std::path::PathBuf" || normalized == "PathBuf" {
        return (context_type == ContextType::AbsolutePath).then(|| Value::from("/"));
    }
    if normalized == "String" {
        return context_supplies_text(context_type).then(|| Value::from("muxe-context"));
    }
    None
}

/// Whether a context domain resolves to a concrete string leaf at dispatch
/// (opaque host identities, host/pane/session text, URLs, and selection
/// text all become `ConfigValueKind::String`; see `context_value_kind`).
fn context_supplies_text(context_type: ContextType) -> bool {
    !matches!(
        context_type,
        ContextType::UnsignedInteger | ContextType::PaneId
    )
}

/// Pane-shaped placeholder the `PaneId` mirror accepts.
fn pane_placeholder() -> Value {
    Value::Object(Map::from_iter([("Terminal".to_owned(), Value::from(0))]))
}

/// Strips one `Option<T>` layer; `None` when the type is not optional.
fn strip_option(normalized: &str) -> Option<&str> {
    normalized
        .strip_prefix("Option<")
        .and_then(|inner| inner.strip_suffix('>'))
}

/// Strips one `Vec<T>` layer; `None` when the type is not a vector.
fn strip_vec(normalized: &str) -> Option<&str> {
    normalized
        .strip_prefix("Vec<")
        .and_then(|inner| inner.strip_suffix('>'))
}

/// Renders the member name for diagnostics (kebab commands as-is, actions by
/// variant).
fn display_member(member: &str, is_action: bool) -> String {
    if is_action {
        crate::names::to_kebab(member)
    } else {
        member.to_owned()
    }
}

/// Renders the parameter name for diagnostics (kebab on the wire in both
/// namespaces).
fn display_key(key: &str, is_action: bool) -> String {
    if is_action {
        crate::names::to_kebab(&leaf_key(key))
    } else {
        leaf_key(key).replace('_', "-")
    }
}

/// Renders the member kind phrase for the mismatch message.
fn member_kind(member: &str, is_action: bool) -> String {
    if is_action {
        format!("action variant '{member}'")
    } else {
        "command".to_owned()
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
                Value::Object(fields_to_json_map_for_member(
                    fields,
                    allow_context,
                    false,
                    Some((kebab.as_str(), false)),
                )?),
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
                Value::Object(fields_to_json_map_for_member(
                    fields,
                    allow_context,
                    true,
                    Some((variant, true)),
                )?),
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

fn bounded(mut message: String) -> String {
    const LIMIT: usize = 1024;
    muxe_protocol::truncate_utf8(&mut message, LIMIT);
    message
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
    fn bounded_diagnostic_preserves_utf8_boundaries() {
        assert_eq!(bounded(format!("{}é", "x".repeat(1023))), "x".repeat(1023));
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
        // The pane-id placeholder ({"Terminal": 0}) is typed, not null: the
        // allow_context=true arm must still accept a pane reference at load
        // time, while allow_context=false (post-resolution dispatch) rejects
        // the surviving marker instead of guessing a concrete pane.
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

    #[test]
    fn mismatched_context_is_rejected_with_reference_and_parameter() {
        let reference =
            muxe_core::ContextReference::parse("origin.pane.id", span()).expect("valid path");
        let fields = [field("tab-index", ConfigValueKind::Context(reference))];
        let error = candidate_to_raw("native.zellij.command:close-tab-with-index", &fields, true)
            .expect_err("pane reference cannot supply usize");
        let mismatch = assert_matches_context_mismatch(error);
        assert_eq!(mismatch.0, "close-tab-with-index");
        assert_eq!(mismatch.1, "tab-index");
        assert!(mismatch.2.contains("origin.pane.id"), "names the reference");
        assert!(mismatch.3.contains("usize"), "names the generated type");
    }

    #[test]
    fn concrete_wrong_type_is_rejected_without_context() {
        // Dispatch-time behavior is unchanged: a resolved concrete value of
        // the wrong type fails the generated mirror before any host call.
        let fields = [field(
            "tab-index",
            ConfigValueKind::String("terminal_1".to_owned()),
        )];
        let error = candidate_to_raw("native.zellij.command:close-tab-with-index", &fields, false)
            .expect_err("concrete string index is rejected");
        assert!(matches!(error, ParseError::InvalidArguments { .. }));
    }

    #[test]
    fn missing_origin_value_fails_resolution_before_dispatch() {
        use muxe_core::{
            ClientId, OriginContext, OriginHostKind, OriginInvocationSource, ServerId,
        };
        let origin = OriginContext {
            host_kind: OriginHostKind::Zellij,
            server_id: ServerId::new("session-alpha"),
            client_id: Some(ClientId::new("client-1")),
            session_id: None,
            workspace_id: None,
            tab_id: None,
            tab_index: None,
            pane_id: None,
            pane_type: None,
            pane_cwd: None,
            selection_text: None,
            invocation_source: OriginInvocationSource::RootBinding,
            worktree_id: None,
            worktree_path: None,
            agent_id: None,
            link_url: None,
            link_handler_id: None,
        };
        let reference =
            muxe_core::ContextReference::parse("origin.tab.index", span()).expect("valid path");
        let candidate = muxe_core::NativeActionCandidate {
            type_name: "native.zellij.command:close-tab-with-index".to_owned(),
            type_span: span(),
            fields: vec![field("tab-index", ConfigValueKind::Context(reference))],
        };
        // Core resolution fails closed on the missing concrete value, so the
        // broker maps it to ContextUnavailable before the adapter is called.
        assert!(candidate.resolve_context(&origin).is_err());
        // And a surviving marker is still rejected at the adapter boundary.
        let error = candidate_to_raw(&candidate.type_name, &candidate.fields, false)
            .expect_err("surviving marker is rejected");
        assert!(matches!(error, ParseError::UnresolvedContext { .. }));
    }

    #[test]
    fn dispatch_rejects_surviving_marker_as_invalid_request() {
        // candidate_to_raw with allow_context=false is the exact call the
        // dispatch path makes (adapter.rs dispatch_native): a surviving
        // marker becomes AdapterErrorKind::InvalidRequest, never a host call.
        let reference =
            muxe_core::ContextReference::parse("origin.tab.index", span()).expect("valid path");
        let fields = [field("tab-index", ConfigValueKind::Context(reference))];
        let error = candidate_to_raw("native.zellij.command:close-tab-with-index", &fields, false)
            .expect_err("surviving marker is rejected");
        assert!(matches!(error, ParseError::UnresolvedContext { .. }));
        assert_eq!(
            error.to_diagnostic(&span()).code,
            muxe_core::DiagnosticCode::InvalidContextReference
        );
    }

    fn assert_matches_context_mismatch(error: ParseError) -> (String, String, String, String) {
        match error {
            ParseError::ContextTypeMismatch(mismatch) => (
                mismatch.name.clone(),
                mismatch.parameter.clone(),
                mismatch.reference.clone(),
                mismatch.expected.clone(),
            ),
            other => panic!("expected ContextTypeMismatch, got {other:?}"),
        }
    }
}
