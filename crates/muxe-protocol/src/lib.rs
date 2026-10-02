//! Versioned broker IPC framing and typed wire contracts.
//!
//! This crate deliberately owns only transport representations and validation. It has no
//! dependency on `muxe-core`, broker runtime, UI implementation, or concrete host adapter.

#![forbid(unsafe_code)]

pub mod bridge;
pub mod control;
pub mod frame;
pub mod text;
pub mod wire;
pub use bridge::{
    BridgeCaptureEndReason, BridgeCaptureLostReason, BridgeEvent, BridgeEventEnvelope,
    BridgeOutput, BridgeProtocolScalarError, BridgeProtocolVersion, BridgeRegistrationId,
    BridgeRequest, BridgeRequestEnvelope, BridgeRequestId, BridgeResponse, BridgeResponseEnvelope,
};

pub use control::{
    ActivationStatus, AsOfTick, BridgeUnitId, CompatibilityRecord, ControlDecoder,
    ControlDirection, ControlMessage, ControlOperation, ControlPolicy, ControlRequest,
    ControlRequestId, ControlResponse, HandoffId, PrepareHandoffProtocol, TargetReadiness,
    UnitReadinessEpochId,
};
pub use frame::{
    ArchivedFrame, ConnectionDecoder, ConnectionPolicy, DecodeError, EncodedFrame, PRELUDE_LEN,
    Prelude, encode_frame,
};
pub use text::truncate_utf8;
pub use wire::*;

// Non-wire traits live here because wire.rs source participates in the protocol fingerprint.
impl Ord for UiSessionId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for UiSessionId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CaptureLeaseId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for CaptureLeaseId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
