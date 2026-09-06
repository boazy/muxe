//! Load-time action validation for the Zellij adapter.
//!
//! The broker calls [`ActionValidator`] while compiling a candidate
//! configuration. Portable actions validate structurally through
//! [`validate_portable_structure`](crate::portable::validate_portable_structure);
//! native candidates parse into generated raw mirrors (with context placeholders)
//! and run the full generated `TryFrom` conversion so no invalid payload can
//! become active. Execution capabilities mirror the host contract: broker-owned
//! actions are synchronous, host actions are asynchronous without host-side
//! cancellation.

use muxe_core::{
    ActionValidation, ActionValidator, ConfigDiagnostic, DiagnosticCode, ExecutionCapabilities,
    NativeActionCandidate, PortableAction, SourceSpan,
};

use crate::{
    names::parse_native_type, parse::candidate_to_raw, portable::validate_portable_structure,
};

/// Stateless load-time validator shared by the adapter and tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct ZellijValidator;

impl ZellijValidator {
    /// Validates one portable action structurally.
    pub fn validate_portable(
        &self,
        action: &PortableAction,
        span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        validate_portable_structure(action).map_err(|error| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidAction,
                error.to_string(),
                span.clone(),
            )
        })?;
        Ok(ActionValidation {
            execution: host_capabilities(action),
        })
    }

    /// Validates one native candidate through parsing plus generated conversion.
    pub fn validate_native_candidate(
        &self,
        candidate: &NativeActionCandidate,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        let namespace = parse_native_type(&candidate.type_name).ok_or_else(|| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidAction,
                format!("not a Zellij native action: {}", candidate.type_name),
                candidate.type_span.clone(),
            )
        })?;
        let _ = namespace;
        let raw = candidate_to_raw(&candidate.type_name, &candidate.fields, true)
            .map_err(|error| error.to_diagnostic(&candidate.type_span))?;
        // Full generated conversion with placeholders: structural error here is a
        // configuration error, never deferred to dispatch.
        validated_from_raw(&raw).map_err(|error| {
            ConfigDiagnostic::error(
                DiagnosticCode::InvalidActionArguments,
                error.to_string(),
                candidate.type_span.clone(),
            )
        })?;
        Ok(ActionValidation {
            execution: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    /// Validates an effective-config batch without copying its candidates.
    ///
    /// Successful output preserves the input cardinality and order. When one or
    /// more candidates are invalid, every diagnostic remains attached to that
    /// candidate's source span.
    pub fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        let mut validations = Vec::with_capacity(candidates.len());
        let mut diagnostics = Vec::new();

        for &candidate in candidates {
            match self.validate_native_candidate(candidate) {
                Ok(validation) => validations.push(validation),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }

        if diagnostics.is_empty() {
            Ok(validations)
        } else {
            Err(diagnostics)
        }
    }
}

/// Execution capabilities for a structurally valid portable action.
fn host_capabilities(action: &PortableAction) -> ExecutionCapabilities {
    match action {
        PortableAction::Menu(_) | PortableAction::Config(_) => ExecutionCapabilities::SYNCHRONOUS,
        // Supervised broker-side processes support full await/detach/cancel.
        PortableAction::Command(_) => ExecutionCapabilities {
            awaitable: true,
            detachable: true,
            cancellable: true,
        },
        PortableAction::Keyboard(_)
        | PortableAction::Tab(_)
        | PortableAction::Pane(_)
        | PortableAction::Session(_) => ExecutionCapabilities::ASYNCHRONOUS,
    }
}

fn validated_from_raw(
    raw: &muxe_zellij_protocol::generated::RawNativeCommand,
) -> Result<muxe_zellij_protocol::generated::ValidatedNativeCommand, String> {
    use muxe_zellij_protocol::generated::ValidatedNativeCommand;
    ValidatedNativeCommand::try_from(raw.clone()).map_err(|error| error.to_string())
}

impl ActionValidator for ZellijValidator {
    fn validate_portable(
        &self,
        action: &PortableAction,
        action_span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        Self::validate_portable(self, action, action_span)
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        Self::validate_native_batch(self, candidates)
    }
}

/// Rejects executable keyboard profiles the Zellij input path cannot honor.
///
/// Stock Zellij supports only the Kitty baseline (`CSI > 1 u`); the adapter
/// rejects an effective configuration that enables the three optional
/// enhancements. Sending a larger flag set cannot upgrade the host path.
pub fn check_keyboard_profile(
    event_types: bool,
    alternate_keys: bool,
    all_keys_as_escape_codes: bool,
    span: &SourceSpan,
) -> Result<(), ConfigDiagnostic> {
    if event_types || alternate_keys || all_keys_as_escape_codes {
        return Err(ConfigDiagnostic::error(
            DiagnosticCode::UnsupportedFeature,
            "Zellij supports only the Kitty baseline; event-types, alternate-keys, and \
             all-keys-as-escape-codes are unavailable on this host",
            span.clone(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_core::{ConfigField, ConfigValue, ConfigValueKind, SourceId};

    fn span() -> SourceSpan {
        SourceSpan::new(SourceId::new("<test>"), 0, 1)
    }

    fn candidate(type_name: &str, fields: Vec<ConfigField>) -> NativeActionCandidate {
        NativeActionCandidate {
            type_name: type_name.to_owned(),
            type_span: span(),
            fields,
        }
    }

    fn field(name: &str, kind: ConfigValueKind) -> ConfigField {
        ConfigField {
            name: name.to_owned(),
            name_span: span(),
            value: ConfigValue { span: span(), kind },
        }
    }

    #[test]
    fn exposed_command_validates() {
        let validator = ZellijValidator;
        let result = validator
            .validate_native_candidate(&candidate("native.zellij.command:close-focus", Vec::new()));
        assert!(result.is_ok());
    }

    #[test]
    fn background_command_is_rejected_at_load() {
        let validator = ZellijValidator;
        let error = validator
            .validate_native_candidate(&candidate("native.zellij.command:exec-cmd", Vec::new()))
            .expect_err("unsupported commands fail closed");
        assert_eq!(error.code, DiagnosticCode::InvalidAction);
    }

    #[test]
    fn query_command_is_rejected_at_load() {
        let validator = ZellijValidator;
        let error = validator
            .validate_native_candidate(&candidate(
                "native.zellij.command:get-pane-info",
                Vec::new(),
            ))
            .expect_err("queries are excluded from the surface");
        assert_eq!(error.code, DiagnosticCode::InvalidAction);
    }

    #[test]
    fn action_namespace_validates_through_run_action() {
        let validator = ZellijValidator;
        let result = validator
            .validate_native_candidate(&candidate("native.zellij.action:close-focus", Vec::new()));
        assert!(result.is_ok());
    }

    #[test]
    fn foreign_namespace_is_rejected() {
        let validator = ZellijValidator;
        let error = validator
            .validate_native_candidate(&candidate("native.herdr.pane:resize", Vec::new()))
            .expect_err("herdr types do not validate here");
        assert_eq!(error.code, DiagnosticCode::InvalidAction);
    }

    #[test]
    fn typed_field_mismatch_is_rejected() {
        let validator = ZellijValidator;
        let error = validator
            .validate_native_candidate(&candidate(
                "native.zellij.command:close-tab-with-index",
                vec![field("tab-index", ConfigValueKind::String("x".to_owned()))],
            ))
            .expect_err("wrong field type fails");
        assert_eq!(error.code, DiagnosticCode::InvalidActionArguments);
    }

    #[test]
    fn native_batch_preserves_success_cardinality() {
        let validator = ZellijValidator;
        let command = candidate("native.zellij.command:close-focus", Vec::new());
        let action = candidate("native.zellij.action:close-focus", Vec::new());

        let validations = validator
            .validate_native_batch(&[&command, &action])
            .expect("each valid candidate produces a validation");

        assert_eq!(
            validations,
            vec![
                ActionValidation {
                    execution: ExecutionCapabilities::ASYNCHRONOUS
                },
                ActionValidation {
                    execution: ExecutionCapabilities::ASYNCHRONOUS
                },
            ]
        );
    }

    #[test]
    fn native_batch_collects_source_bound_diagnostics_in_input_order() {
        let validator = ZellijValidator;
        let mut foreign = candidate("native.herdr.pane:resize", Vec::new());
        foreign.type_span = SourceSpan::new(SourceId::new("<test>"), 11, 12);
        let mut mismatched = candidate(
            "native.zellij.command:close-tab-with-index",
            vec![field("tab-index", ConfigValueKind::String("x".to_owned()))],
        );
        mismatched.type_span = SourceSpan::new(SourceId::new("<test>"), 29, 30);

        let diagnostics = validator
            .validate_native_batch(&[&foreign, &mismatched])
            .expect_err("each invalid candidate reports a source-bound diagnostic");

        assert_eq!(
            diagnostics
                .iter()
                .map(|diagnostic| diagnostic.code)
                .collect::<Vec<_>>(),
            vec![
                DiagnosticCode::InvalidAction,
                DiagnosticCode::InvalidActionArguments,
            ]
        );
        assert_eq!(diagnostics[0].labels[0].span, foreign.type_span);
        assert_eq!(diagnostics[1].labels[0].span, mismatched.type_span);
    }

    #[test]
    fn portable_incompatible_is_rejected() {
        let validator = ZellijValidator;
        let error = validator
            .validate_portable(
                &PortableAction::Session(muxe_core::SessionAction::Quit),
                &span(),
            )
            .expect_err("session quit has no mapping");
        assert_eq!(error.code, DiagnosticCode::InvalidAction);
    }

    #[test]
    fn keyboard_enhancements_are_rejected() {
        assert!(check_keyboard_profile(false, false, false, &span()).is_ok());
        let error = check_keyboard_profile(true, false, false, &span())
            .expect_err("event types unavailable");
        assert_eq!(error.code, DiagnosticCode::UnsupportedFeature);
    }
}
