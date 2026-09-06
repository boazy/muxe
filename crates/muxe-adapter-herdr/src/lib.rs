//! Direct Herdr socket adapter and schema compatibility boundary.
//!
//! The adapter keeps the Herdr JSON Schema dynamic at its boundary: configuration is checked
//! against the bundled request schema, runtime drift is checked against the exact installed
//! Herdr binary's schema, and the resolved request is checked again immediately before dispatch.

#![forbid(unsafe_code)]

pub mod generated;
mod origin;
mod schema;
mod transport;

pub use origin::capture_origin;
pub use schema::{ApiSchema, MethodSchema, ValidationCode, ValidationError, VALIDATOR_FORMAT_VERSION};
pub use transport::{DeliveryState, HerdrResponse, HerdrSocketClient, SocketError};
