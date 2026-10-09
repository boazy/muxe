use std::collections::BTreeMap;
use std::io::{self, Write};

use ariadne::{Config, IndexType, Label, Report, ReportKind, sources};
use std::fmt;
use std::sync::Arc;

/// A configuration source is identified by caller-provided, displayable name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SourceId(Arc<str>);

impl SourceId {
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    #[must_use]
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
    #[must_use]
    pub fn new(source: SourceId, start: usize, end: usize) -> Self {
        Self { source, start, end }
    }

    #[must_use]
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
    #[must_use]
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
            labels: vec![DiagnosticLabel {
                span,
                message: String::new(),
            }],
            notes: Vec::new(),
            help: None,
        }
    }

    pub fn warning(code: DiagnosticCode, message: impl Into<String>, span: SourceSpan) -> Self {
        Self {
            severity: DiagnosticSeverity::Warning,
            code,
            message: message.into(),
            labels: vec![DiagnosticLabel {
                span,
                message: String::new(),
            }],
            notes: Vec::new(),
            help: None,
        }
    }

    #[must_use]
    pub fn with_label(mut self, span: SourceSpan, message: impl Into<String>) -> Self {
        self.labels.push(DiagnosticLabel {
            span,
            message: message.into(),
        });
        self
    }

    #[must_use]
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    #[must_use]
    pub fn with_help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }
}

impl fmt::Display for ConfigDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} [{}]", self.message, self.code.as_str())?;
        for label in &self.labels {
            write!(
                formatter,
                "\n  --> {} (bytes {}..{})",
                label.span.source, label.span.start, label.span.end
            )?;
            if !label.message.is_empty() {
                write!(formatter, ": {}", label.message)?;
            }
        }
        for note in &self.notes {
            write!(formatter, "\n  note: {note}")?;
        }
        if let Some(help) = &self.help {
            write!(formatter, "\n  help: {help}")?;
        }
        Ok(())
    }
}

/// Diagnostics and the immutable source text used to produce them.
///
/// Source IDs are labels, not filesystem paths. Rendering never reads files.
#[derive(Clone, Debug, Default)]
pub struct ConfigDiagnosticReport {
    pub diagnostics: Vec<ConfigDiagnostic>,
    pub sources: BTreeMap<SourceId, Arc<str>>,
}

impl From<ConfigDiagnostic> for ConfigDiagnosticReport {
    fn from(diagnostic: ConfigDiagnostic) -> Self {
        vec![diagnostic].into()
    }
}

impl From<Vec<ConfigDiagnostic>> for ConfigDiagnosticReport {
    fn from(diagnostics: Vec<ConfigDiagnostic>) -> Self {
        Self {
            diagnostics,
            sources: BTreeMap::new(),
        }
    }
}

impl ConfigDiagnosticReport {
    /// Renders captured excerpts, or source locations when text is unavailable.
    ///
    /// # Errors
    ///
    /// Returns an output error without discarding the original diagnostics.
    pub fn write(&self, context: Option<&str>, output: &mut dyn Write) -> io::Result<()> {
        for diagnostic in &self.diagnostics {
            let usable = |label: &DiagnosticLabel| {
                self.sources.get(&label.span.source).is_some_and(|text| {
                    label.span.start <= label.span.end && label.span.end <= text.len()
                })
            };
            let Some((primary_index, primary)) = diagnostic
                .labels
                .iter()
                .enumerate()
                .find(|(_, label)| usable(label))
            else {
                if let Some(context) = context {
                    write!(output, "{context} configuration: ")?;
                }
                writeln!(output, "{diagnostic}")?;
                continue;
            };
            let kind = match diagnostic.severity {
                DiagnosticSeverity::Error => ReportKind::Error,
                DiagnosticSeverity::Warning => ReportKind::Warning,
            };
            let code = context.map_or_else(
                || diagnostic.code.as_str().to_owned(),
                |context| format!("{context}/{}", diagnostic.code.as_str()),
            );
            let mut report = Report::build(
                kind,
                (
                    primary.span.source.clone(),
                    primary.span.start..primary.span.end,
                ),
            )
            .with_config(
                Config::default()
                    .with_color(false)
                    .with_index_type(IndexType::Byte),
            )
            .with_code(code)
            .with_message(&diagnostic.message);
            for (index, label) in diagnostic.labels.iter().enumerate() {
                if usable(label) {
                    let rendered =
                        Label::new((label.span.source.clone(), label.span.start..label.span.end));
                    report = if label.message.is_empty() && index == primary_index {
                        report.with_label(rendered.with_message("here"))
                    } else {
                        report.with_label(rendered.with_message(&label.message))
                    };
                } else {
                    report = report.with_note(format!(
                        "{} (bytes {}..{}): {}",
                        label.span.source, label.span.start, label.span.end, label.message,
                    ));
                }
            }
            for note in &diagnostic.notes {
                report = report.with_note(note);
            }
            if let Some(help) = &diagnostic.help {
                report = report.with_help(help);
            }
            report.finish().write(
                sources(
                    self.sources
                        .iter()
                        .map(|(id, text)| (id.clone(), text.as_ref())),
                ),
                &mut *output,
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for ConfigDiagnosticReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Output<'a, 'b>(&'a mut fmt::Formatter<'b>);
        impl Write for Output<'_, '_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                let text = std::str::from_utf8(bytes).map_err(io::Error::other)?;
                self.0
                    .write_str(text)
                    .map_err(|_| io::Error::other("diagnostic output failed"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        self.write(None, &mut Output(formatter))
            .map_err(|_| fmt::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_less_display_preserves_all_structured_details() {
        let diagnostic = ConfigDiagnostic::error(
            DiagnosticCode::InvalidAction,
            "unknown action",
            SourceSpan::new(SourceId::new("<synthetic>"), 4, 9),
        )
        .with_label(
            SourceSpan::new(SourceId::new("config.yml"), 12, 20),
            "related binding",
        )
        .with_note("the binding cannot be compiled")
        .with_help("choose a supported action");
        let rendered = diagnostic.to_string();
        for value in [
            diagnostic.message.as_str(),
            diagnostic.code.as_str(),
            "<synthetic>",
            "4..9",
            "config.yml",
            "12..20",
            diagnostic.labels[1].message.as_str(),
            diagnostic.notes[0].as_str(),
            diagnostic.help.as_deref().unwrap(),
        ] {
            assert!(rendered.contains(value));
        }
    }

    #[test]
    fn captured_multi_source_reports_use_unicode_byte_offsets() {
        let primary = SourceId::new("<primary>");
        let secondary = SourceId::new("<secondary>");
        let diagnostic = ConfigDiagnostic::error(
            DiagnosticCode::InvalidValue,
            "invalid value",
            SourceSpan::new(primary.clone(), 4, 5),
        )
        .with_label(SourceSpan::new(secondary.clone(), 0, 1), "related value")
        .with_label(
            SourceSpan::new(SourceId::new("<unavailable>"), 9, 10),
            "missing excerpt",
        )
        .with_note("retained note")
        .with_help("retained help");
        let report = ConfigDiagnosticReport {
            diagnostics: vec![diagnostic],
            sources: BTreeMap::from([
                (primary, Arc::from("é: x\n")),
                (secondary, Arc::from("y: z\n")),
            ]),
        };
        let rendered = report.to_string();
        for evidence in [
            "<primary>:1:4",
            "<secondary>:1:1",
            "related value",
            "<unavailable>",
            "9..10",
            "missing excerpt",
            "retained note",
            "retained help",
        ] {
            assert!(rendered.contains(evidence), "{rendered}");
        }
    }
}
