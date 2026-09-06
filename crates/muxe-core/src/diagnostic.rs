use std::fmt;
use std::sync::Arc;

/// A configuration source is identified by caller-provided, displayable name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceId(Arc<str>);

impl SourceId {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A byte range in one UTF-8 YAML source. Ranges are half-open.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SourceSpan {
    pub source: SourceId,
    pub start: usize,
    pub end: usize,
}

impl SourceSpan {
    pub fn new(source: SourceId, start: usize, end: usize) -> Self {
        Self { source, start, end }
    }

    pub fn whole(source: SourceId, source_text: &str) -> Self {
        Self::new(source, 0, source_text.len())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum DiagnosticCode {
    YamlSyntax,
    DuplicateYamlKey,
    UnknownField,
    UnsupportedFeature,
    InvalidVersion,
    InvalidValue,
    MissingField,
    InvalidKey,
    KeyCapability,
    KeyCollision,
    InvalidAction,
    InvalidActionArguments,
    InvalidContextReference,
    ContextTypeMismatch,
    InvalidCondition,
    InvalidMenuReference,
    MenuCycle,
    InvalidInjection,
    InvalidTheme,
    InvalidColorScheme,
    NativeActionRejected,
}

impl DiagnosticCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::YamlSyntax => "yaml_syntax",
            Self::DuplicateYamlKey => "duplicate_yaml_key",
            Self::UnknownField => "unknown_field",
            Self::UnsupportedFeature => "unsupported_feature",
            Self::InvalidVersion => "invalid_version",
            Self::InvalidValue => "invalid_value",
            Self::MissingField => "missing_field",
            Self::InvalidKey => "invalid_key",
            Self::KeyCapability => "key_capability",
            Self::KeyCollision => "key_collision",
            Self::InvalidAction => "invalid_action",
            Self::InvalidActionArguments => "invalid_action_arguments",
            Self::InvalidContextReference => "invalid_context_reference",
            Self::ContextTypeMismatch => "context_type_mismatch",
            Self::InvalidCondition => "invalid_condition",
            Self::InvalidMenuReference => "invalid_menu_reference",
            Self::MenuCycle => "menu_cycle",
            Self::InvalidInjection => "invalid_injection",
            Self::InvalidTheme => "invalid_theme",
            Self::InvalidColorScheme => "invalid_color_scheme",
            Self::NativeActionRejected => "native_action_rejected",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiagnosticLabel {
    pub span: SourceSpan,
    pub message: String,
}

/// Structured source-aware diagnostic suitable for an Ariadne presentation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigDiagnostic {
    pub severity: DiagnosticSeverity,
    pub code: DiagnosticCode,
    pub message: String,
    pub labels: Vec<DiagnosticLabel>,
    pub notes: Vec<String>,
    pub help: Option<String>,
}

impl ConfigDiagnostic {
    pub fn error(code: DiagnosticCode, message: impl Into<String>, span: SourceSpan) -> Self {
        Self {
            severity: DiagnosticSeverity::Error,
            code,
            message: message.into(),
            labels: vec![DiagnosticLabel { span, message: String::new() }],
            notes: Vec::new(),
            help: None,
        }
    }

    pub fn warning(code: DiagnosticCode, message: impl Into<String>, span: SourceSpan) -> Self {
        Self {
            severity: DiagnosticSeverity::Warning,
            code,
            message: message.into(),
            labels: vec![DiagnosticLabel { span, message: String::new() }],
            notes: Vec::new(),
            help: None,
        }
    }

    pub fn with_label(mut self, span: SourceSpan, message: impl Into<String>) -> Self {
        self.labels.push(DiagnosticLabel { span, message: message.into() });
        self
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }
}
