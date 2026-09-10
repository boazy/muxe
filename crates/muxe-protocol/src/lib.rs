//! Versioned broker IPC framing and typed wire contracts.
//!
//! This crate deliberately owns only transport representations and validation. It has no
//! dependency on `muxe-core`, broker runtime, UI implementation, or concrete host adapter.

#![forbid(unsafe_code)]

pub mod bridge;
pub mod control;
pub mod frame;
pub mod wire;
pub use bridge::{
    BridgeCaptureEndReason, BridgeCaptureLostReason, BridgeEvent, BridgeEventEnvelope,
    BridgeOutput, BridgeProtocolScalarError, BridgeProtocolVersion, BridgeRegistrationId,
    BridgeRequest, BridgeRequestEnvelope, BridgeRequestId, BridgeResponse, BridgeResponseEnvelope,
};

pub use control::{
    ActivationStatus, CompatibilityRecord, ControlDecoder, ControlDirection, ControlMessage,
    ControlOperation, ControlPolicy, ControlRequest, ControlRequestId, ControlResponse, HandoffId,
    TargetReadiness,
};
pub use frame::{
    ArchivedFrame, ConnectionDecoder, ConnectionPolicy, DecodeError, EncodedFrame, PRELUDE_LEN,
    Prelude, encode_frame,
};
pub use wire::*;
