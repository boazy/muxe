//! Typed dispatch of broker requests through generated conversion code.
//!
//! Every dispatch revalidates its raw mirror (`Raw → Validated`) and converts
//! directly into upstream Zellij types with no JSON value bridge. Direct shim
//! commands complete synchronously from their return values; `RunAction`
//! completes asynchronously through the `ActionComplete` event correlated by a
//! `muxe_execution` context entry, because `run_action` only queues host
//! dispatch on another thread.

use std::collections::BTreeMap;

use muxe_zellij_protocol::{
    CommandOutcome,
    generated::{
        NativeCommandDispatch, RawNativeCommand, ValidatedNativeCommand, dispatch_native_command,
    },
};

use crate::outcome::outcome_of;

/// Key joining a `run_action` dispatch with its later `ActionComplete` echo.
pub const EXECUTION_CONTEXT_KEY: &str = "muxe_execution";

/// A validated dispatch ready for the host, plus how it completes.
pub enum ReadyDispatch {
    /// Synchronous: the shim return value decides the outcome now.
    Sync(NativeCommandDispatch),
    /// Asynchronous: completion arrives via `ActionComplete`.
    Async {
        /// Dispatch to execute.
        dispatch: NativeCommandDispatch,
        /// Broker execution ID for correlation.
        execution: String,
    },
}

/// Revalidates a raw command and sorts it into its completion mode.
///
/// # Errors
///
/// Returns the generated validation error text when the raw mirror is invalid.
pub fn prepare(command: RawNativeCommand, execution: &str) -> Result<ReadyDispatch, String> {
    let validated = ValidatedNativeCommand::try_from(command).map_err(|error| error.to_string())?;
    match validated {
        ValidatedNativeCommand::RunAction {
            action,
            mut context,
        } => {
            context.insert(EXECUTION_CONTEXT_KEY.to_owned(), execution.to_owned());
            Ok(ReadyDispatch::Async {
                dispatch: NativeCommandDispatch::RunAction { action, context },
                execution: execution.to_owned(),
            })
        }
        other => Ok(ReadyDispatch::Sync(NativeCommandDispatch::from(other))),
    }
}

/// Executes a synchronous dispatch and maps its return to a typed outcome.
pub fn execute_sync(dispatch: NativeCommandDispatch) -> CommandOutcome {
    outcome_of(&dispatch_native_command(dispatch))
}

/// Revalidates a raw low-level action (used only in tests of the conversion
/// path; production actions arrive inside `RunAction` commands).
#[cfg(test)]
pub fn prepare_action(
    action: muxe_zellij_protocol::generated::raw::Action,
) -> Result<muxe_zellij_protocol::generated::validated::Action, String> {
    muxe_zellij_protocol::generated::validated::Action::try_from(action)
        .map_err(|error| error.to_string())
}
/// Extracts the correlation execution ID from an `ActionComplete` context.
pub fn completion_execution(context: &BTreeMap<String, String>) -> Option<&str> {
    context.get(EXECUTION_CONTEXT_KEY).map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_zellij_protocol::generated::raw;

    #[test]
    fn run_action_completes_async_with_correlation() {
        let command = RawNativeCommand::RunAction {
            action: raw::Action::CloseFocus,
            context: Vec::new(),
        };
        match prepare(command, "exec-1").expect("prepares") {
            ReadyDispatch::Async {
                execution,
                dispatch,
            } => {
                assert_eq!(execution, "exec-1");
                assert!(matches!(dispatch, NativeCommandDispatch::RunAction { .. }));
                if let NativeCommandDispatch::RunAction { context, .. } = dispatch {
                    assert_eq!(completion_execution(&context), Some("exec-1"));
                }
            }
            ReadyDispatch::Sync(_) => panic!("run_action must complete async"),
        }
    }

    #[test]
    fn direct_command_completes_sync() {
        let command = RawNativeCommand::CloseFocus;
        match prepare(command, "exec-2").expect("prepares") {
            ReadyDispatch::Sync(dispatch) => {
                assert!(matches!(dispatch, NativeCommandDispatch::CloseFocus));
            }
            ReadyDispatch::Async { .. } => panic!("direct command must complete sync"),
        }
    }

    #[test]
    fn invalid_raw_is_rejected_before_dispatch() {
        // Duplicate map keys are rejected by the generated conversion.
        let command = RawNativeCommand::EditLayout {
            layout_name: "dev".to_owned(),
            context: vec![
                raw::MapEntry {
                    key: "a".to_owned(),
                    value: "1".to_owned(),
                },
                raw::MapEntry {
                    key: "a".to_owned(),
                    value: "2".to_owned(),
                },
            ],
        };
        assert!(prepare(command, "exec-3").is_err());
    }

    #[test]
    fn raw_action_converts_to_upstream_shape() {
        let validated = prepare_action(raw::Action::CloseFocus).expect("converts");
        assert_eq!(
            validated,
            muxe_zellij_protocol::generated::validated::Action::CloseFocus
        );
    }
}
