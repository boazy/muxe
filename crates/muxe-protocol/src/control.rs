use std::mem;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    frame::{DecodeError, Prelude, PRELUDE_LEN},
    wire::{LiveServerIdentity, PeerRole, SchemaFingerprint, MAX_CONTROL_FRAME_LEN},
};

const LENGTH_PREFIX_LEN: usize = 4;
const MAX_CONTROL_DIAGNOSTIC_LEN: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HandoffId(pub [u8; 16]);

impl HandoffId {
    pub fn is_zero(self) -> bool {
        self.0 == [0; 16]
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
    pub request_id: HandoffId,
    pub operation: ControlOperation,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum ControlOperation {
    Status,
    Prepare {
        target_version: String,
        target_schema_fingerprint: SchemaFingerprint,
        live_server: LiveServerIdentity,
    },
    Commit {
        handoff_id: HandoffId,
    },
    Abort {
        handoff_id: HandoffId,
    },
    Retire,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlResponse {
    pub request_id: HandoffId,
    pub result: ControlResult,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ControlResult {
    Status {
        broker_version: String,
        application_fingerprint: SchemaFingerprint,
        live_server: LiveServerIdentity,
        lifecycle: LifecycleState,
        active_ui_sessions: u32,
        foreground_executions: u32,
    },
    Prepared { handoff_id: HandoffId },
    Committed,
    Aborted,
    Retired,
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
    pub fn validate(&self) -> Result<(), ControlSemanticError> {
        match self {
            Self::Request(request) => request.validate(),
            Self::Response(response) => response.validate(),
        }
    }
}

impl ControlRequest {
    fn validate(&self) -> Result<(), ControlSemanticError> {
        validate_handoff_id(self.request_id)?;
        match &self.operation {
            ControlOperation::Status | ControlOperation::Retire => Ok(()),
            ControlOperation::Prepare {
                target_version,
                target_schema_fingerprint,
                live_server,
            } => {
                validate_control_text("target version", target_version)?;
                if target_schema_fingerprint.is_zero() {
                    return Err(ControlSemanticError::ZeroFingerprint);
                }
                validate_live_server(live_server)
            }
            ControlOperation::Commit { handoff_id } | ControlOperation::Abort { handoff_id } => {
                validate_handoff_id(*handoff_id)
            }
        }
    }
}

impl ControlResponse {
    fn validate(&self) -> Result<(), ControlSemanticError> {
        validate_handoff_id(self.request_id)?;
        match &self.result {
            ControlResult::Status {
                broker_version,
                application_fingerprint,
                live_server,
                ..
            } => {
                validate_control_text("broker version", broker_version)?;
                if application_fingerprint.is_zero() {
                    return Err(ControlSemanticError::ZeroFingerprint);
                }
                validate_live_server(live_server)
            }
            ControlResult::Prepared { handoff_id } => validate_handoff_id(*handoff_id),
            ControlResult::Error { diagnostic } => {
                if diagnostic.len() > MAX_CONTROL_DIAGNOSTIC_LEN {
                    return Err(ControlSemanticError::DiagnosticTooLong(diagnostic.len()));
                }
                validate_control_text("control diagnostic", diagnostic)
            }
            ControlResult::Committed | ControlResult::Aborted | ControlResult::Retired => Ok(()),
        }
    }
}

fn validate_handoff_id(value: HandoffId) -> Result<(), ControlSemanticError> {
    if value.is_zero() {
        Err(ControlSemanticError::ZeroHandoffId)
    } else {
        Ok(())
    }
}

fn validate_live_server(value: &LiveServerIdentity) -> Result<(), ControlSemanticError> {
    if value.discovery_key.is_empty() || value.discovery_key.chars().any(char::is_control) {
        return Err(ControlSemanticError::InvalidLiveServerIdentity);
    }
    if value.server_id.as_str().is_empty() || value.server_id.as_str().chars().any(char::is_control)
    {
        return Err(ControlSemanticError::InvalidLiveServerIdentity);
    }
    Ok(())
}

fn validate_control_text(field: &'static str, value: &str) -> Result<(), ControlSemanticError> {
    if value.is_empty() || value.contains('\0') || value.chars().any(char::is_control) {
        Err(ControlSemanticError::InvalidText(field))
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ControlDecoder {
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

impl Default for ControlDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlDecoder {
    pub fn new() -> Self {
        Self {
            state: ControlDecoderState::Prelude {
                bytes: [0; PRELUDE_LEN],
                filled: 0,
            },
            failed: false,
        }
    }

    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<ControlMessage>, ControlDecodeError> {
        if self.failed {
            return Err(ControlDecodeError::DecoderClosed);
        }

        let mut messages = Vec::new();
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
                    let prelude = Prelude::decode(bytes).map_err(ControlDecodeError::Prelude)?;
                    if let Err(error) = prelude.validate(
                        crate::wire::Codec::ControlJsonV1,
                        PeerRole::ActivationCoordinator,
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
                        Err(error) => return self.fail(ControlDecodeError::InvalidJson(error.to_string())),
                    };
                    if let Err(error) = message.validate() {
                        return self.fail(ControlDecodeError::Semantic(error));
                    }
                    self.state = ControlDecoderState::LengthPrefix {
                        bytes: [0; LENGTH_PREFIX_LEN],
                        filled: 0,
                    };
                    messages.push(message);
                }
                ControlDecoderState::Failed => return self.fail(ControlDecodeError::DecoderClosed),
            }
        }
        Ok(messages)
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
                declared_len, bytes, ..
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
    #[error("handoff ID must be nonzero")]
    ZeroHandoffId,
    #[error("schema fingerprint must be nonzero")]
    ZeroFingerprint,
    #[error("invalid live server identity")]
    InvalidLiveServerIdentity,
    #[error("invalid {0}")]
    InvalidText(&'static str),
    #[error("control diagnostic exceeds bound: {0}")]
    DiagnosticTooLong(usize),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ControlDecodeError {
    #[error("invalid control prelude: {0}")]
    Prelude(#[source] DecodeError),
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
            host: crate::wire::HostKind::Herdr,
            discovery_key: "socket-key".into(),
            server_id: crate::wire::ServerId::new("server"),
        }
    }

    fn request() -> ControlMessage {
        ControlMessage::Request(ControlRequest {
            request_id: HandoffId([1; 16]),
            operation: ControlOperation::Prepare {
                target_version: "0.1.0".into(),
                target_schema_fingerprint: SchemaFingerprint::application(),
                live_server: identity(),
            },
        })
    }

    fn frame(message: &ControlMessage) -> Vec<u8> {
        let payload = serde_json::to_vec(message).unwrap();
        let mut bytes = Prelude::control(PeerRole::ActivationCoordinator).encode().to_vec();
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend(payload);
        bytes
    }

    #[test]
    fn accepts_control_json_with_unknown_additive_fields() {
        let mut bytes = frame(&request());
        let payload_start = PRELUDE_LEN + LENGTH_PREFIX_LEN;
        let mut payload: serde_json::Value = serde_json::from_slice(&bytes[payload_start..]).unwrap();
        payload["future_field"] = serde_json::json!(true);
        let payload = serde_json::to_vec(&payload).unwrap();
        bytes.truncate(PRELUDE_LEN);
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend(payload);
        assert_eq!(ControlDecoder::new().push(&bytes).unwrap(), vec![request()]);
    }

    #[test]
    fn rejects_normal_ui_prelude_and_oversized_frame() {
        let mut wrong = Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application())
            .encode()
            .to_vec();
        wrong.extend_from_slice(&0_u32.to_be_bytes());
        assert!(matches!(
            ControlDecoder::new().push(&wrong),
            Err(ControlDecodeError::Prelude(_))
        ));

        let mut oversized = Prelude::control(PeerRole::ActivationCoordinator)
            .encode()
            .to_vec();
        oversized.extend_from_slice(&(MAX_CONTROL_FRAME_LEN + 1).to_be_bytes());
        assert!(matches!(
            ControlDecoder::new().push(&oversized),
            Err(ControlDecodeError::FrameTooLarge(_))
        ));
    }
}
