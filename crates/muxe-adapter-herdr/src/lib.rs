//! Direct Herdr socket adapter and schema compatibility boundary.
//!
//! The adapter keeps the Herdr JSON Schema dynamic at its boundary: configuration is checked
//! against the bundled request schema, runtime drift is checked against the exact installed
//! Herdr binary's schema, and the resolved request is checked again immediately before dispatch.

#![forbid(unsafe_code)]

mod adapter;
mod cache;
pub mod generated;
mod launch;
mod origin;
mod runtime;
mod schema;
mod subscription;
mod transport;
mod validation;
pub use adapter::{HerdrAdapter, HerdrConfigValidator};
pub use cache::{
    ComparisonKey, HerdrCache, hash_configured_request_refs, hash_configured_requests,
};
pub use launch::{
    CommandPaneLaunch, CommandPanePlacement, CommandTabLaunch, FocusedPane, PreparedUiPane,
    UiPaneLaunch, UiPanePlacement, UiSplitDirection, close_transient_tab, focused_pane,
    move_prepared_ui_pane, open_command_pane, open_command_tab, pane_by_id, pane_by_identity,
    prepare_ui_pane,
};
pub use runtime::{HerdrAdapterConfig, HerdrRuntime, probe_endpoint_identity, probe_live_identity};
pub use schema::{
    ApiSchema, MethodSchema, VALIDATOR_FORMAT_VERSION, ValidationCode, ValidationError,
};
pub use subscription::{EventSubscription, SubscriptionConfig, SubscriptionEvent};
pub use transport::{
    DeliveryState, EndpointIdentity, HerdrResponse, HerdrSocketClient, PeerIdentity, SocketError,
};
pub use validation::{CandidateValidationError, fields_to_json, validate_candidate};

/// Extracts the typed `pane.get` payload from Herdr's success envelope.
///
/// Protocol 20 returns `{ "type": "pane_info", "pane": { ... } }`; keeping
/// this unwrap at the transport boundary prevents callers from accidentally
/// treating the envelope as pane metadata.
pub(crate) fn pane_info(
    result: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    let object = result.as_object()?;
    (object.get("type").and_then(serde_json::Value::as_str) == Some("pane_info"))
        .then(|| object.get("pane").and_then(serde_json::Value::as_object))
        .flatten()
}

/// Authoritative verified Herdr API feature set: every JSON-RPC method below is exercised
/// by a production path in this crate (not tests, not the full generated method table).
///
/// Provenance per method:
/// - `ping`: runtime liveness probe before dispatch;
/// - `events.subscribe`: retained event subscription;
/// - `session.snapshot`: origin capture and launcher pane resolution;
/// - `pane.get`: capture and pending-pane validation;
/// - `pane.send_keys`, `pane.send_text`, `tab.create`, `tab.close`, `tab.rename`,
///   `tab.move`, `pane.split`, `pane.close`, `pane.focus_direction`, `pane.swap`,
///   `pane.resize`, `pane.zoom`: portable dispatch invocations;
/// - `layout.apply`, `pane.move`: UI trampoline create and move phases;
/// - `tab.close`: transient-tab failure cleanup;
/// - `notification.show`: capability-gated launcher failure reporting.
///
/// The compatibility record carries this set so cross-version handoffs compare the
/// exercised surface, not the generated table. Sorted for stable rendering.
pub const VERIFIED_HERDR_METHODS: &[&str] = &[
    "events.subscribe",
    "layout.apply",
    "notification.show",
    "pane.close",
    "pane.focus_direction",
    "pane.get",
    "pane.move",
    "pane.resize",
    "pane.send_keys",
    "pane.send_text",
    "pane.split",
    "pane.swap",
    "pane.zoom",
    "ping",
    "session.snapshot",
    "tab.close",
    "tab.create",
    "tab.move",
    "tab.rename",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_methods_are_sorted_unique_and_bundled() {
        assert!(!VERIFIED_HERDR_METHODS.is_empty());
        let mut sorted = VERIFIED_HERDR_METHODS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted, VERIFIED_HERDR_METHODS,
            "verified set stays sorted and unique"
        );
        for method in VERIFIED_HERDR_METHODS {
            assert!(
                generated::method_metadata(method).is_some(),
                "verified method {method} must exist in the bundled schema metadata"
            );
        }
        for required in [
            "ping",
            "events.subscribe",
            "session.snapshot",
            "layout.apply",
            "pane.move",
        ] {
            assert!(
                VERIFIED_HERDR_METHODS.contains(&required),
                "verified set must keep the trampoline-critical {required}"
            );
        }
    }

    #[test]
    fn pane_info_unwraps_protocol_twenty_success_envelope() {
        let response = serde_json::json!({
            "type": "pane_info",
            "pane": {
                "workspace_id": "w1",
                "tab_id": "w1:t1",
                "pane_id": "w1:p1",
                "terminal_id": "term-1",
                "focused": true,
                "agent_status": "unknown",
                "revision": 0
            }
        });
        let pane = pane_info(&response).expect("pane_info envelope");
        assert_eq!(
            pane.get("workspace_id").and_then(serde_json::Value::as_str),
            Some("w1")
        );
        assert!(pane_info(&serde_json::json!({"workspace_id":"w1"})).is_none());
    }
}
