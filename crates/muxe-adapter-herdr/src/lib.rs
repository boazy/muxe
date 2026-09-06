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
mod transport;
mod subscription;
mod validation;

pub use adapter::HerdrAdapter;
pub use cache::{ComparisonKey, HerdrCache, hash_configured_request_refs, hash_configured_requests};
pub use launch::{
    CommandPaneLaunch, CommandPanePlacement, FocusedPane, PreparedUiPane, UiPaneLaunch,
    UiPanePlacement, UiSplitDirection, close_transient_tab, focused_pane, move_prepared_ui_pane,
    open_command_pane, open_ui_pane, pane_by_id, pane_by_identity, prepare_ui_pane,
};
pub use runtime::{
    HerdrAdapterConfig, HerdrRuntime, probe_endpoint_identity, probe_live_identity,
};
pub use schema::{
    ApiSchema, MethodSchema, VALIDATOR_FORMAT_VERSION, ValidationCode, ValidationError,
};
pub use subscription::{EventSubscription, SubscriptionConfig, SubscriptionEvent};
pub use transport::{
    DeliveryState, EndpointIdentity, HerdrResponse, HerdrSocketClient, PeerIdentity, SocketError,
};
pub use validation::{CandidateValidationError, fields_to_json, validate_candidate};
