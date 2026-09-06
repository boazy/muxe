use std::mem;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    frame::{DecodeError, PRELUDE_LEN, Prelude},
    wire::{
        Codec, HostKind, LiveServerIdentity, MAX_CONTROL_FRAME_LEN, PeerRole, SchemaFingerprint,
    },
};

const LENGTH_PREFIX_LEN: usize = 4;
const MAX_CONTROL_DIAGNOSTIC_LEN: usize = 4 * 1024;

macro_rules! control_nonce {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 16]);

        impl $name {
            fn validate(self, name: &'static str) -> Result<(), ControlSemanticError> {
                (self.0 != [0; 16])
                    .then_some(())
                    .ok_or(ControlSemanticError::ZeroNonce(name))
            }
        }
    };
}

control_nonce!(ControlRequestId);
control_nonce!(HandoffId);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZellijCompatibility {
    pub source_revision: String,
    pub generated_action_fingerprint: SchemaFingerprint,
    pub bridge_protocol_fingerprint: SchemaFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HerdrCompatibility {
    pub protocol_version: u32,
    pub schema_version: u32,
    pub schema_fingerprint: SchemaFingerprint,
}

/// Embedded compatibility material needed to decide a cross-version handoff. It intentionally
/// has no loaded-artifact digest: the pinned host API cannot attest the artifact it loaded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilityRecord {
    pub muxe_version: String,
    pub target_triple: String,
    pub application_schema_fingerprint: SchemaFingerprint,
    pub zellij: Option<ZellijCompatibility>,
    pub herdr: Option<HerdrCompatibility>,
}

impl CompatibilityRecord {
    fn validate(&self) -> Result<(), ControlSemanticError> {
        validate_control_text("Muxe version", &self.muxe_version)?;
        validate_control_text("target triple", &self.target_triple)?;
        validate_fingerprint(self.application_schema_fingerprint)?;
        if let Some(zellij) = &self.zellij {
            validate_control_text("Zellij source revision", &zellij.source_revision)?;
            validate_fingerprint(zellij.generated_action_fingerprint)?;
            validate_fingerprint(zellij.bridge_protocol_fingerprint)?;
        }
        if let Some(herdr) = &self.herdr {
            if herdr.protocol_version == 0 || herdr.schema_version == 0 {
                return Err(ControlSemanticError::InvalidCompatibility);
            }
            validate_fingerprint(herdr.schema_fingerprint)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivationStatus {
    pub lifecycle: LifecycleState,
    pub live_server: LiveServerIdentity,
    pub current: CompatibilityRecord,
    pub target: Option<CompatibilityRecord>,
    pub handoff_id: Option<HandoffId>,
}

impl ActivationStatus {
    fn validate(&self) -> Result<(), ControlSemanticError> {
        validate_live_server(&self.live_server)?;
        self.current.validate()?;
        if let Some(target) = &self.target {
            target.validate()?;
        }
        if let Some(handoff) = self.handoff_id {
            handoff.validate("handoff ID")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case")]
pub enum ControlMessage {
    Request(ControlRequest),
    Response(ControlResponse),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlRequest {
    pub request_id: ControlRequestId,
    pub operation: ControlOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ControlOperation {
    Status,
    Prepare { target: CompatibilityRecord },
    Commit { handoff_id: HandoffId },
    Abort { handoff_id: HandoffId },
    Retire,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlResponse {
    pub request_id: ControlRequestId,
    pub result: ControlResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ControlResult {
    Status(ActivationStatus),
    Prepared(ActivationStatus),
    Committed(ActivationStatus),
    Aborted(ActivationStatus),
    Retired(ActivationStatus),
    Error { diagnostic: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Running,
    Preparing,
    Draining,
    SupervisorOnly,
    Retired,
}

impl ControlMessage {
    fn validate(&self, direction: ControlDirection) -> Result<(), ControlSemanticError> {
        match (direction, self) {
            (ControlDirection::CoordinatorToBroker, Self::Request(request)) => request.validate(),
            (ControlDirection::BrokerToCoordinator, Self::Response(response)) => {
                response.validate()
            }
            _ => Err(ControlSemanticError::WrongDirection),
        }
    }
}

impl ControlRequest {
    fn validate(&self) -> Result<(), ControlSemanticError> {
        self.request_id.validate("control request ID")?;
        match &self.operation {
            ControlOperation::Status | ControlOperation::Retire => Ok(()),
            ControlOperation::Prepare { target } => target.validate(),
            ControlOperation::Commit { handoff_id } | ControlOperation::Abort { handoff_id } => {
                handoff_id.validate("handoff ID")
            }
        }
    }
}

impl ControlResponse {
    fn validate(&self) -> Result<(), ControlSemanticError> {
        self.request_id.validate("control request ID")?;
        match &self.result {
            ControlResult::Status(status)
            | ControlResult::Prepared(status)
            | ControlResult::Committed(status)
            | ControlResult::Aborted(status)
            | ControlResult::Retired(status) => status.validate(),
            ControlResult::Error { diagnostic } => {
                if diagnostic.len() > MAX_CONTROL_DIAGNOSTIC_LEN {
                    return Err(ControlSemanticError::DiagnosticTooLong(diagnostic.len()));
                }
                validate_control_text("control diagnostic", diagnostic)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlDirection {
    CoordinatorToBroker,
    BrokerToCoordinator,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlPolicy {
    pub direction: ControlDirection,
}

impl ControlPolicy {
    pub const fn broker() -> Self {
        Self {
            direction: ControlDirection::CoordinatorToBroker,
        }
    }

    pub const fn coordinator() -> Self {
        Self {
            direction: ControlDirection::BrokerToCoordinator,
        }
    }

    const fn expected_peer_role(self) -> PeerRole {
        match self.direction {
            ControlDirection::CoordinatorToBroker => PeerRole::ActivationCoordinator,
            ControlDirection::BrokerToCoordinator => PeerRole::Broker,
        }
    }
}

#[derive(Debug)]
pub struct ControlDecoder {
    policy: ControlPolicy,
    state: ControlDecoderState,
    failed: bool,
}

#[derive(Debug)]
enum ControlDecoderState {
    Prelude {
        bytes: [u8; PRELUDE_LEN],
        filled: usize,
    },
    LengthPrefix {
        bytes: [u8; LENGTH_PREFIX_LEN],
        filled: usize,
    },
    Payload {
        declared_len: u32,
        bytes: Vec<u8>,
    },
    Failed,
}

impl ControlDecoder {
    pub fn new(policy: ControlPolicy) -> Self {
        Self {
            policy,
            state: ControlDecoderState::Prelude {
                bytes: [0; PRELUDE_LEN],
                filled: 0,
            },
            failed: false,
        }
    }

    pub fn push<F>(&mut self, mut input: &[u8], mut on_message: F) -> Result<(), ControlDecodeError>
    where
        F: FnMut(ControlMessage),
    {
        if self.failed {
            return Err(ControlDecodeError::DecoderClosed);
        }
        while !input.is_empty() {
            let state = mem::replace(&mut self.state, ControlDecoderState::Failed);
            match state {
                ControlDecoderState::Prelude {
                    mut bytes,
                    mut filled,
                } => {
                    let copied = copy_from_input(&mut bytes, &mut filled, input);
                    input = &input[copied..];
                    if filled != PRELUDE_LEN {
                        self.state = ControlDecoderState::Prelude { bytes, filled };
                        continue;
                    }
                    let prelude = match Prelude::decode(bytes) {
                        Ok(prelude) => prelude,
                        Err(error) => return self.fail(ControlDecodeError::Prelude(error)),
                    };
                    if let Err(error) = prelude.validate(
                        Codec::ControlJsonV1,
                        self.policy.expected_peer_role(),
                        SchemaFingerprint::ZERO,
                        MAX_CONTROL_FRAME_LEN,
                    ) {
                        return self.fail(ControlDecodeError::Prelude(error));
                    }
                    self.state = ControlDecoderState::LengthPrefix {
                        bytes: [0; LENGTH_PREFIX_LEN],
                        filled: 0,
                    };
                }
                ControlDecoderState::LengthPrefix {
                    mut bytes,
                    mut filled,
                } => {
                    let copied = copy_from_input(&mut bytes, &mut filled, input);
                    input = &input[copied..];
                    if filled != LENGTH_PREFIX_LEN {
                        self.state = ControlDecoderState::LengthPrefix { bytes, filled };
                        continue;
                    }
                    let declared_len = u32::from_be_bytes(bytes);
                    if declared_len == 0 {
                        return self.fail(ControlDecodeError::EmptyFrame);
                    }
                    if declared_len > MAX_CONTROL_FRAME_LEN {
                        return self.fail(ControlDecodeError::FrameTooLarge(declared_len));
                    }
                    self.state = ControlDecoderState::Payload {
                        declared_len,
                        bytes: Vec::with_capacity(declared_len as usize),
                    };
                }
                ControlDecoderState::Payload {
                    declared_len,
                    mut bytes,
                } => {
                    let remaining = declared_len as usize - bytes.len();
                    let copied = remaining.min(input.len());
                    bytes.extend_from_slice(&input[..copied]);
                    input = &input[copied..];
                    if bytes.len() != declared_len as usize {
                        self.state = ControlDecoderState::Payload {
                            declared_len,
                            bytes,
                        };
                        continue;
                    }
                    let message: ControlMessage = match serde_json::from_slice(&bytes) {
                        Ok(message) => message,
                        Err(error) => {
                            return self.fail(ControlDecodeError::InvalidJson(error.to_string()));
                        }
                    };
                    if let Err(error) = message.validate(self.policy.direction) {
                        return self.fail(ControlDecodeError::Semantic(error));
                    }
                    self.state = ControlDecoderState::LengthPrefix {
                        bytes: [0; LENGTH_PREFIX_LEN],
                        filled: 0,
                    };
                    on_message(message);
                }
                ControlDecoderState::Failed => return self.fail(ControlDecodeError::DecoderClosed),
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), ControlDecodeError> {
        if self.failed {
            return Err(ControlDecodeError::DecoderClosed);
        }
        let truncated = match &self.state {
            ControlDecoderState::Prelude { filled, .. } => Some(*filled),
            ControlDecoderState::LengthPrefix { filled: 0, .. } => None,
            ControlDecoderState::LengthPrefix { filled, .. } => Some(*filled),
            ControlDecoderState::Payload {
                declared_len,
                bytes,
                ..
            } => Some((declared_len - bytes.len() as u32) as usize),
            ControlDecoderState::Failed => return Err(ControlDecodeError::DecoderClosed),
        };
        match truncated {
            Some(value) => self.fail(ControlDecodeError::Truncated(value)),
            None => Ok(()),
        }
    }

    fn fail<T>(&mut self, error: ControlDecodeError) -> Result<T, ControlDecodeError> {
        self.state = ControlDecoderState::Failed;
        self.failed = true;
        Err(error)
    }
}

fn validate_fingerprint(value: SchemaFingerprint) -> Result<(), ControlSemanticError> {
    (!value.is_zero())
        .then_some(())
        .ok_or(ControlSemanticError::ZeroFingerprint)
}

fn validate_live_server(value: &LiveServerIdentity) -> Result<(), ControlSemanticError> {
    if value.discovery_key.is_empty()
        || value.discovery_key.chars().any(char::is_control)
        || value.server_id.as_str().is_empty()
        || value.server_id.as_str().chars().any(char::is_control)
    {
        return Err(ControlSemanticError::InvalidLiveServerIdentity);
    }
    match value.host {
        HostKind::Zellij | HostKind::Herdr => Ok(()),
    }
}

fn validate_control_text(field: &'static str, value: &str) -> Result<(), ControlSemanticError> {
    if value.is_empty() || value.contains('\0') || value.chars().any(char::is_control) {
        Err(ControlSemanticError::InvalidText(field))
    } else {
        Ok(())
    }
}

fn copy_from_input<const N: usize>(
    destination: &mut [u8; N],
    filled: &mut usize,
    input: &[u8],
) -> usize {
    let copied = (N - *filled).min(input.len());
    destination[*filled..*filled + copied].copy_from_slice(&input[..copied]);
    *filled += copied;
    copied
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlSemanticError {
    #[error("{0} must be nonzero")]
    ZeroNonce(&'static str),
    #[error("schema fingerprint must be nonzero")]
    ZeroFingerprint,
    #[error("invalid live server identity")]
    InvalidLiveServerIdentity,
    #[error("invalid compatibility record")]
    InvalidCompatibility,
    #[error("invalid {0}")]
    InvalidText(&'static str),
    #[error("control diagnostic exceeds bound: {0}")]
    DiagnosticTooLong(usize),
    #[error("control message is illegal for this connection direction")]
    WrongDirection,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlDecodeError {
    #[error("invalid control prelude: {0}")]
    Prelude(#[source] DecodeError),
    #[error("zero-length control frames are invalid")]
    EmptyFrame,
    #[error("control frame exceeds 64 KiB: {0}")]
    FrameTooLarge(u32),
    #[error("truncated control stream after {0} bytes")]
    Truncated(usize),
    #[error("invalid control JSON: {0}")]
    InvalidJson(String),
    #[error("invalid control message: {0}")]
    Semantic(#[from] ControlSemanticError),
    #[error("decoder is closed after a protocol error")]
    DecoderClosed,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> LiveServerIdentity {
        LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: "socket-key".into(),
            server_id: crate::wire::ServerId::new("server"),
        }
    }

    fn record() -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: "0.1.0".into(),
            target_triple: "aarch64-apple-darwin".into(),
            application_schema_fingerprint: SchemaFingerprint::application(),
            zellij: None,
            herdr: Some(HerdrCompatibility {
                protocol_version: 20,
                schema_version: 1,
                schema_fingerprint: SchemaFingerprint::application(),
            }),
        }
    }

    fn request() -> ControlMessage {
        ControlMessage::Request(ControlRequest {
            request_id: ControlRequestId([1; 16]),
            operation: ControlOperation::Prepare { target: record() },
        })
    }

    fn frame(role: PeerRole, message: &ControlMessage) -> Vec<u8> {
        let payload = serde_json::to_vec(message).unwrap();
        let mut bytes = Prelude::control(role).encode().to_vec();
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend(payload);
        bytes
    }

    #[test]
    fn broker_accepts_only_coordinator_requests_and_unknown_additions() {
        let mut bytes = frame(PeerRole::ActivationCoordinator, &request());
        let payload_start = PRELUDE_LEN + LENGTH_PREFIX_LEN;
        let mut payload: serde_json::Value =
            serde_json::from_slice(&bytes[payload_start..]).unwrap();
        payload["future_field"] = serde_json::json!(true);
        let payload = serde_json::to_vec(&payload).unwrap();
        bytes.truncate(PRELUDE_LEN);
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend(payload);
        let mut messages = Vec::new();
        ControlDecoder::new(ControlPolicy::broker())
            .push(&bytes, |message| messages.push(message))
            .unwrap();
        assert_eq!(messages, vec![request()]);
        assert!(payload_start < bytes.len());
    }

    #[test]
    fn rejects_wrong_role_direction_and_empty_frame() {
        let wrong_role = frame(PeerRole::Ui, &request());
        assert!(matches!(
            ControlDecoder::new(ControlPolicy::broker()).push(&wrong_role, |_| {}),
            Err(ControlDecodeError::Prelude(_))
        ));

        let response = ControlMessage::Response(ControlResponse {
            request_id: ControlRequestId([1; 16]),
            result: ControlResult::Error {
                diagnostic: "no".into(),
            },
        });
        let response_for_broker = frame(PeerRole::ActivationCoordinator, &response);
        assert!(matches!(
            ControlDecoder::new(ControlPolicy::broker()).push(&response_for_broker, |_| {}),
            Err(ControlDecodeError::Semantic(
                ControlSemanticError::WrongDirection
            ))
        ));

        let mut empty = Prelude::control(PeerRole::ActivationCoordinator)
            .encode()
            .to_vec();
        empty.extend_from_slice(&0_u32.to_be_bytes());
        assert!(matches!(
            ControlDecoder::new(ControlPolicy::broker()).push(&empty, |_| {}),
            Err(ControlDecodeError::EmptyFrame)
        ));
    }

    #[test]
    fn coordinator_accepts_only_broker_responses() {
        let status = ActivationStatus {
            lifecycle: LifecycleState::Running,
            live_server: identity(),
            current: record(),
            target: None,
            handoff_id: None,
        };
        let response = ControlMessage::Response(ControlResponse {
            request_id: ControlRequestId([2; 16]),
            result: ControlResult::Status(status),
        });
        let mut messages = Vec::new();
        ControlDecoder::new(ControlPolicy::coordinator())
            .push(&frame(PeerRole::Broker, &response), |message| {
                messages.push(message)
            })
            .unwrap();
        assert_eq!(messages, vec![response]);
    }
}
