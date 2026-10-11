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

impl BrokerResponse {
    /// Names the response without exposing its payload in a user diagnostic.
    #[must_use]
    pub const fn kind_name(&self) -> &'static str {
        match self {
            Self::LaunchPrepared { .. } => "launch reservation",
            Self::PendingPaneRegistered => "pane registration confirmation",
            Self::AttachPending => "attachment waiting for launch completion",
            Self::UiAttached { .. } => "UI attachment",
            Self::InvocationAccepted { .. } => "execution acceptance",
            Self::PendingControlCompleted { .. } => "menu-control completion",
            Self::Detached => "UI detachment confirmation",
            Self::Acknowledged => "acknowledgement",
            Self::Error(_) => "error",
        }
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::fmt::Display for ExecutionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

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

#[cfg(test)]
mod tests {
    use super::ExecutionId;

    #[test]
    fn execution_id_display_preserves_all_bits_without_changing_serialization() {
        let id = ExecutionId([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0,
            0xe0, 0xff,
        ]);
        assert_eq!(id.to_string(), "00010203040506078090a0b0c0d0e0ff");
        let json = serde_json::to_value(id).expect("execution ID serializes");
        assert_eq!(
            json,
            serde_json::json!([
                0, 1, 2, 3, 4, 5, 6, 7, 128, 144, 160, 176, 192, 208, 224, 255,
            ])
        );
        assert_eq!(serde_json::from_value::<ExecutionId>(json).unwrap(), id);
        assert_eq!(
            ExecutionId([0; 16]).to_string(),
            "00000000000000000000000000000000"
        );
    }
}
