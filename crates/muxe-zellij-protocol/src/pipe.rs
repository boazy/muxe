//! Typed JSON-lines frames for the two persistent `zellij pipe` channels.
//!
//! The broker owns one server-wide pair of `zellij pipe` children per live Zellij
//! session: the request pipe carries [`PipeRequest`] lines from broker to bridge,
//! and the event pipe carries [`PipeEvent`] lines from bridge to broker. Every
//! line is one complete JSON document terminated by `\n`; decoding uses bounded
//! single-line frames so non-protocol or oversized output marks the channel
//! unhealthy instead of growing an unbounded buffer.
//!
//! The common envelopes in `muxe-protocol` wrap concrete typed host payloads.
//! This module supplies the Zellij-specific payloads: they are plain typed
//! structs with no `serde_json::Value` anywhere on the path between crates.

use muxe_protocol::{BridgeEventEnvelope, BridgeRequestEnvelope, SchemaFingerprint};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::generated::RawNativeCommand;

use crate::{ChannelGeneration, ProtocolVersion};
#[cfg(test)]
use crate::{RegistrationId, RequestId};

/// Pipe protocol version implemented by this build.
pub const BRIDGE_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::CURRENT;
/// Maximum accepted JSON line length on either pipe, in bytes.
pub const MAX_PIPE_LINE_LEN: usize = 64 * 1024;
/// Maximum length of a human-facing detail or diagnostic string.
pub const MAX_DETAIL_LEN: usize = 4 * 1024;
/// Typed initial payload that installs one event-pipe generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventSubscription {
    muxe: SubscriptionMarker,
    protocol: ProtocolVersion,
    channel_generation: ChannelGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum SubscriptionMarker {
    #[serde(rename = "subscribe")]
    Subscribe,
}

impl EventSubscription {
    /// Creates a subscription for the current protocol and supplied generation.
    #[must_use]
    pub const fn new(channel_generation: ChannelGeneration) -> Self {
        Self {
            muxe: SubscriptionMarker::Subscribe,
            protocol: BRIDGE_PROTOCOL_VERSION,
            channel_generation,
        }
    }

    /// Installed generation carried by this subscription.
    #[must_use]
    pub const fn channel_generation(self) -> ChannelGeneration {
        self.channel_generation
    }

    /// Whether the subscription uses the protocol implemented by this build.
    #[must_use]
    pub const fn is_current(self) -> bool {
        self.protocol.is_current()
    }
}

/// Encodes an event subscription as the exact JSON payload passed to `zellij pipe`.
///
/// # Errors
///
/// Returns [`PipeError::Encode`] when JSON serialization fails.
pub fn encode_event_subscription(subscription: EventSubscription) -> Result<String, PipeError> {
    serde_json::to_string(&subscription).map_err(|error| PipeError::Encode {
        reason: bounded_reason(error.to_string()),
    })
}

/// Decodes and validates an event subscription payload.
///
/// # Errors
///
/// Returns [`PipeError::InvalidFrame`] for malformed JSON and
/// [`PipeError::Validation`] for an unsupported protocol.
pub fn decode_event_subscription(payload: &str) -> Result<EventSubscription, PipeError> {
    let subscription: EventSubscription =
        serde_json::from_str(payload).map_err(|error| PipeError::InvalidFrame {
            reason: bounded_reason(error.to_string()),
        })?;
    if !subscription.is_current() {
        return Err(PipeError::Validation {
            reason: format!(
                "unsupported bridge protocol {}; expected {BRIDGE_PROTOCOL_VERSION}",
                subscription.protocol
            ),
        });
    }
    Ok(subscription)
}

/// Zellij broadcast target for one common bridge request envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeTarget {
    /// Zellij client ID owning the target bridge, as reported by `list_clients`.
    pub client_id: String,
}

/// Zellij-specific dispatch operation carried by the common dispatch lifecycle.
#[expect(
    clippy::large_enum_variant,
    reason = "native commands stay inline to avoid a heap allocation on every request"
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ZellijDispatchRequest {
    Command(RawNativeCommand),
    FocusPaneByIndex { index: u32 },
    FocusPaneNeighbor { direction: NeighborDirection },
}

/// Zellij data required to resolve a generic origin request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ZellijOriginRequest {
    pub ui_pane: String,
}

/// No additional Zellij request lifecycle exists outside the common contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ZellijRequestExtension {}

/// Typed broker-to-bridge lifecycle request.
pub type BridgeRequest = muxe_protocol::BridgeRequest<
    ZellijDispatchRequest,
    ZellijOriginRequest,
    ZellijRequestExtension,
>;

/// One broker-to-bridge frame. Channel generation and broadcast targeting use
/// Zellij-owned types while the common envelope owns protocol correlation.
pub type PipeRequest = BridgeRequestEnvelope<ChannelGeneration, BridgeTarget, BridgeRequest>;

/// Why broker-side capture ends.
pub type CaptureEndReason = muxe_protocol::BridgeCaptureEndReason;

/// Cardinal direction for bridge-resolved neighbor focus.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NeighborDirection {
    Left,
    Right,
    Up,
    Down,
}

/// Zellij registration data advertised through the common registration event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ZellijRegistration {
    pub client_id: String,
    pub current_pane: Option<String>,
    pub plugin_id: Option<u32>,
    pub identity: BridgeIdentity,
}

/// Zellij state returned when Locked-mode capture becomes active.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ZellijCaptureState {
    pub prior_mode: String,
}

/// No additional Zellij response lifecycle exists outside the common contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ZellijResponseExtension {}

/// Typed solicited bridge response.
pub type BridgeResponse = muxe_protocol::BridgeResponse<
    CommandOutcome,
    ZellijOrigin,
    ZellijCaptureState,
    ZellijResponseExtension,
>;

/// No additional Zellij unsolicited lifecycle exists outside the common contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ZellijEventExtension {}

/// Typed unsolicited bridge event.
pub type BridgeEvent = muxe_protocol::BridgeEvent<ZellijRegistration, ZellijEventExtension>;

/// Why the bridge stopped owning capture outside the broker-driven path.
pub type CaptureLostReason = muxe_protocol::BridgeCaptureLostReason;

/// Response or unsolicited lifecycle payload on the Zellij event pipe.
pub type PipeEventKind = muxe_protocol::BridgeOutput<BridgeResponse, BridgeEvent>;

/// Every event-pipe frame carries the full provenance envelope. Unsolicited
/// payloads encode `request_id` as JSON `null`.
pub type PipeEvent = BridgeEventEnvelope<ChannelGeneration, PipeEventKind>;

/// Typed synchronous outcome for one dispatched native command.
///
/// `ActionComplete` semantics apply: success means Zellij finished dispatching
/// the action, not that an arbitrary resulting operation succeeded. There is no
/// general failure value on this path; fallible returns surface as [`CommandStatus::Failed`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandOutcome {
    /// Success or failure of host dispatch.
    pub status: CommandStatus,
    /// Bounded detail: fallible return text on failure, empty on success.
    pub detail: String,
}

/// Dispatch result discriminator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandStatus {
    /// Zellij accepted and synchronously dispatched the command.
    Succeeded,
    /// A fallible shim return carried `Err`; `detail` holds the bounded message.
    Failed,
}

impl CommandOutcome {
    /// Successful host dispatch with no detail.
    #[must_use]
    pub fn succeeded() -> Self {
        Self {
            status: CommandStatus::Succeeded,
            detail: String::new(),
        }
    }

    /// Failed host dispatch with a bounded message.
    pub fn failed(detail: impl Into<String>) -> Self {
        let mut text = detail.into();
        if text.len() > MAX_DETAIL_LEN {
            text.truncate(MAX_DETAIL_LEN);
        }
        Self {
            status: CommandStatus::Failed,
            detail: text,
        }
    }
}

/// Origin snapshot supplied by the target bridge for one attaching Muxe UI.
///
/// The bridge continuously tracks the last focused non-Muxe pane for its client;
/// when the Muxe UI attaches through its own pane ID, the bridge snapshots that
/// prior pane. Fields the host does not expose stay `None`; the adapter turns a
/// missing required value into `context_unavailable` instead of guessing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZellijOrigin {
    /// Zellij client ID that owns this snapshot.
    pub client_id: String,
    /// Live session name, when the bridge could read it.
    pub session_name: Option<String>,
    /// Last focused non-Muxe pane before the Muxe UI pane took focus.
    pub prior_pane_id: Option<String>,
    /// The Muxe UI's own pane ID, echoed for cleanup correlation.
    pub ui_pane_id: String,
    /// Working directory of the prior pane, when exposed.
    pub prior_pane_cwd: Option<String>,
}

/// Handshake material the WASM bridge embeds and reports at registration.
///
/// Semantic versions alone are insufficient: runtime handshakes compare the
/// pinned source revision, the generated-action fingerprint, and the bridge
/// protocol fingerprint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeIdentity {
    /// Muxe version compiled into the bridge.
    pub muxe_version: String,
    /// Pinned Zellij source revision compiled into the bridge.
    pub source_revision: String,
    /// Fingerprint of the generated action/command surface.
    pub action_fingerprint: [u8; 32],
    /// Fingerprint of this pipe protocol schema.
    pub protocol_fingerprint: [u8; 32],
    /// Shared deterministic pre-link bridge/protocol build identity.
    ///
    /// `None` decodes legacy control/pipe data but never qualifies a
    /// registration as compatible.
    #[serde(default)]
    pub bridge_build_id: Option<SchemaFingerprint>,
}

/// Typed pipe framing and validation errors. Oversized or non-protocol output
/// makes the channel unhealthy; it never grows an unbounded buffer.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum PipeError {
    /// A JSON line exceeded [`MAX_PIPE_LINE_LEN`] bytes.
    #[error("pipe line exceeds {MAX_PIPE_LINE_LEN} bytes ({actual} bytes)")]
    LineTooLong {
        /// Observed byte length.
        actual: usize,
    },
    /// A line is not a valid typed frame.
    #[error("invalid pipe frame: {reason}")]
    InvalidFrame {
        /// Bounded reason; never echoes unbounded line content.
        reason: String,
    },
    /// A frame failed semantic validation.
    #[error("pipe frame failed validation: {reason}")]
    Validation {
        /// Bounded reason.
        reason: String,
    },
    /// Serialization of an outbound frame failed.
    #[error("could not encode pipe frame: {reason}")]
    Encode {
        /// Bounded reason.
        reason: String,
    },
}

fn bounded_reason(message: impl Into<String>) -> String {
    let mut text = message.into();
    if text.len() > MAX_DETAIL_LEN {
        text.truncate(MAX_DETAIL_LEN);
    }
    text
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), PipeError> {
    if value.is_empty() {
        return Err(PipeError::Validation {
            reason: format!("{field} must not be empty"),
        });
    }
    if value.len() > MAX_DETAIL_LEN {
        return Err(PipeError::Validation {
            reason: format!("{field} exceeds {MAX_DETAIL_LEN} bytes"),
        });
    }
    Ok(())
}

fn validate_lease(lease: &[u8; 16]) -> Result<(), PipeError> {
    if lease == &[0; 16] {
        Err(PipeError::Validation {
            reason: "capture lease must not be zero".to_owned(),
        })
    } else {
        Ok(())
    }
}

trait ValidatePipeRequest {
    fn validate(&self) -> Result<(), PipeError>;
}

impl ValidatePipeRequest for PipeRequest {
    fn validate(&self) -> Result<(), PipeError> {
        if self.protocol != BRIDGE_PROTOCOL_VERSION {
            return Err(PipeError::Validation {
                reason: format!(
                    "unsupported bridge protocol {}; expected {BRIDGE_PROTOCOL_VERSION}",
                    self.protocol
                ),
            });
        }
        require_non_empty("target client ID", &self.target.client_id)?;
        self.payload.validate()
    }
}

trait ValidateBridgeRequest {
    fn validate(&self) -> Result<(), PipeError>;
}

impl ValidateBridgeRequest for BridgeRequest {
    fn validate(&self) -> Result<(), PipeError> {
        match self {
            Self::Dispatch { execution, .. } => require_non_empty("execution ID", execution),
            Self::BeginCapture { lease, ui_session } => {
                validate_lease(lease)?;
                require_non_empty("UI session", ui_session)
            }
            Self::EndCapture { lease, .. } => validate_lease(lease),
            Self::RequestOrigin {
                ui_session,
                request,
            } => {
                require_non_empty("UI session", ui_session)?;
                require_non_empty("UI pane", &request.ui_pane)
            }
            Self::Retire | Self::Shutdown => Ok(()),
            Self::Host(_) => Err(PipeError::Validation {
                reason: "unsupported Zellij request extension".to_owned(),
            }),
        }
    }
}

trait ValidatePipeEvent {
    fn validate(&self) -> Result<(), PipeError>;
}

impl ValidatePipeEvent for PipeEvent {
    fn validate(&self) -> Result<(), PipeError> {
        if self.protocol != BRIDGE_PROTOCOL_VERSION {
            return Err(PipeError::Validation {
                reason: format!(
                    "unsupported bridge protocol {}; expected {BRIDGE_PROTOCOL_VERSION}",
                    self.protocol
                ),
            });
        }
        match &self.event {
            PipeEventKind::Response(response) => {
                if self.request_id.is_none() {
                    return Err(PipeError::Validation {
                        reason: "solicited response requires a request ID".to_owned(),
                    });
                }
                response.validate()
            }
            PipeEventKind::Event(event) => {
                if self.request_id.is_some() {
                    return Err(PipeError::Validation {
                        reason: "unsolicited event must not carry a request ID".to_owned(),
                    });
                }
                event.validate()
            }
        }
    }
}

trait ValidateBridgeResponse {
    fn validate(&self) -> Result<(), PipeError>;
}

impl ValidateBridgeResponse for BridgeResponse {
    fn validate(&self) -> Result<(), PipeError> {
        match self {
            Self::RequestReleased => Ok(()),
            Self::DispatchAccepted { execution } => require_non_empty("execution ID", execution),
            Self::DispatchCompleted { execution, outcome } => {
                require_non_empty("execution ID", execution)?;
                if outcome.detail.len() > MAX_DETAIL_LEN {
                    return Err(PipeError::Validation {
                        reason: format!("outcome detail exceeds {MAX_DETAIL_LEN} bytes"),
                    });
                }
                Ok(())
            }
            Self::OriginSnapshot { ui_session, origin } => {
                require_non_empty("UI session", ui_session)?;
                origin.validate()
            }
            Self::OriginDeclined { ui_session } => require_non_empty("UI session", ui_session),
            Self::CaptureReady { lease, state } => {
                validate_lease(lease)?;
                require_non_empty("prior input mode", &state.prior_mode)
            }
            Self::Host(_) => Err(PipeError::Validation {
                reason: "unsupported Zellij response extension".to_owned(),
            }),
        }
    }
}

trait ValidateBridgeEvent {
    fn validate(&self) -> Result<(), PipeError>;
}

impl ValidateBridgeEvent for BridgeEvent {
    fn validate(&self) -> Result<(), PipeError> {
        match self {
            Self::Register { registration } => {
                require_non_empty("client ID", &registration.client_id)?;
                registration.identity.validate()
            }
            Self::CaptureLost { lease, .. } => validate_lease(lease),
            Self::Heartbeat | Self::Retire | Self::Shutdown => Ok(()),
            Self::Health { detail } => detail
                .as_deref()
                .map(|detail| require_non_empty("health detail", detail))
                .transpose()
                .map(|_| ()),
            Self::Host(_) => Err(PipeError::Validation {
                reason: "unsupported Zellij event extension".to_owned(),
            }),
        }
    }
}

impl BridgeIdentity {
    /// Handshake validation: required text fields are non-empty and required
    /// fingerprints are non-zero. The optional build ID may be absent for
    /// legacy control/pipe decoding; registration compatibility rejects that
    /// legacy form, while a supplied zero build ID is invalid here.
    ///
    /// # Errors
    ///
    /// Returns [`PipeError::Validation`] when required fields are missing or a
    /// supplied fingerprint/build ID is zero.
    pub fn validate(&self) -> Result<(), PipeError> {
        require_non_empty("Muxe version", &self.muxe_version)?;
        require_non_empty("Zellij source revision", &self.source_revision)?;
        if self.action_fingerprint == [0; 32] || self.protocol_fingerprint == [0; 32] {
            return Err(PipeError::Validation {
                reason: "bridge fingerprints must not be zero".to_owned(),
            });
        }
        if self.bridge_build_id.is_some_and(SchemaFingerprint::is_zero) {
            return Err(PipeError::Validation {
                reason: "bridge build ID must not be zero".to_owned(),
            });
        }
        Ok(())
    }
}

impl ZellijOrigin {
    /// Snapshot validation: IDs are non-empty and any supplied working
    /// directory is absolute.
    ///
    /// # Errors
    ///
    /// Returns [`PipeError::Validation`] when IDs are missing or cwd is relative.
    pub fn validate(&self) -> Result<(), PipeError> {
        require_non_empty("origin client ID", &self.client_id)?;
        require_non_empty("UI pane ID", &self.ui_pane_id)?;
        if let Some(cwd) = &self.prior_pane_cwd
            && !cwd.starts_with('/')
        {
            return Err(PipeError::Validation {
                reason: "prior pane working directory must be absolute".to_owned(),
            });
        }
        Ok(())
    }
}

/// Encodes a request as one bounded `\n`-terminated JSON line for the request pipe.
///
/// # Errors
///
/// Returns [`PipeError::Encode`] when serialization fails or the line is oversized.
pub fn encode_request_line(request: &PipeRequest) -> Result<String, PipeError> {
    encode_line(request)
}

/// Encodes an event as one bounded `\n`-terminated JSON line for the event pipe.
///
/// # Errors
///
/// Returns [`PipeError::Encode`] when serialization fails or the line is oversized.
pub fn encode_event_line(event: &PipeEvent) -> Result<String, PipeError> {
    encode_line(event)
}

fn encode_line<T: Serialize>(frame: &T) -> Result<String, PipeError> {
    let mut line = serde_json::to_string(frame).map_err(|error| PipeError::Encode {
        reason: bounded_reason(error.to_string()),
    })?;
    line.push('\n');
    if line.len() > MAX_PIPE_LINE_LEN {
        return Err(PipeError::Encode {
            reason: format!("encoded line exceeds {MAX_PIPE_LINE_LEN} bytes"),
        });
    }
    Ok(line)
}

fn check_line_bound(line: &str) -> Result<(), PipeError> {
    if line.len() > MAX_PIPE_LINE_LEN {
        return Err(PipeError::LineTooLong { actual: line.len() });
    }
    Ok(())
}

/// Decodes and validates one request-pipe line into its typed frame.
///
/// # Errors
///
/// Returns [`PipeError`] when the line is oversized, unparsable, or invalid.
pub fn decode_request_line(line: &str) -> Result<PipeRequest, PipeError> {
    check_line_bound(line)?;
    let request: PipeRequest =
        serde_json::from_str(line).map_err(|error| PipeError::InvalidFrame {
            reason: bounded_reason(error.to_string()),
        })?;
    request.validate()?;
    Ok(request)
}

/// Decodes and validates one event-pipe line into its typed frame.
///
/// # Errors
///
/// Returns [`PipeError`] when the line is oversized, unparsable, or invalid.
pub fn decode_event_line(line: &str) -> Result<PipeEvent, PipeError> {
    check_line_bound(line)?;
    let event: PipeEvent = serde_json::from_str(line).map_err(|error| PipeError::InvalidFrame {
        reason: bounded_reason(error.to_string()),
    })?;
    event.validate()?;
    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn registration(seed: u8) -> RegistrationId {
        RegistrationId::from_random_bytes([seed; 16]).expect("test registration")
    }

    fn sample_target() -> BridgeTarget {
        BridgeTarget {
            client_id: "client-1".to_owned(),
        }
    }

    #[test]
    fn request_round_trip_preserves_typed_payload() {
        let request = PipeRequest {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id: RequestId::INITIAL,
            registration: registration(7),
            channel_generation: ChannelGeneration::try_from(3).expect("generation"),
            target: sample_target(),
            payload: BridgeRequest::EndCapture {
                lease: [9; 16],
                reason: CaptureEndReason::UiDismissed,
            },
        };
        let line = encode_request_line(&request).expect("encodes");
        assert!(line.ends_with('\n'));
        assert_eq!(decode_request_line(&line).expect("decodes"), request);
    }

    #[test]
    fn wrong_protocol_version_and_zero_request_are_rejected() {
        let mut request = PipeRequest {
            protocol: ProtocolVersion::try_from(2).expect("version two"),
            request_id: RequestId::INITIAL,
            registration: registration(7),
            channel_generation: ChannelGeneration::INITIAL,
            target: sample_target(),
            payload: BridgeRequest::Retire,
        };
        assert!(request.validate().is_err());
        request.protocol = BRIDGE_PROTOCOL_VERSION;
        let line = serde_json::to_string(&request)
            .expect("request serializes")
            .replace("\"request_id\":1", "\"request_id\":0");
        assert!(decode_request_line(&line).is_err());
    }

    #[test]
    fn oversized_lines_make_channels_unhealthy() {
        let big = "x".repeat(MAX_PIPE_LINE_LEN + 1);
        assert!(matches!(
            decode_event_line(&big),
            Err(PipeError::LineTooLong { .. })
        ));
        assert!(matches!(
            decode_request_line("not json"),
            Err(PipeError::InvalidFrame { .. })
        ));
    }

    #[test]
    fn event_envelope_enforces_request_correlation_and_capture_lease() {
        let unsolicited = PipeEvent {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id: None,
            channel_generation: ChannelGeneration::INITIAL,
            registration: registration(2),
            event: PipeEventKind::Event(BridgeEvent::Heartbeat),
        };
        assert!(unsolicited.validate().is_ok());
        let missing_request = PipeEvent {
            event: PipeEventKind::Response(BridgeResponse::RequestReleased),
            ..unsolicited
        };
        assert!(missing_request.validate().is_err());
        let unexpected_request = PipeEvent {
            request_id: Some(RequestId::INITIAL),
            event: PipeEventKind::Event(BridgeEvent::Heartbeat),
            ..missing_request
        };
        assert!(unexpected_request.validate().is_err());
        let zero_lease = PipeEvent {
            request_id: None,
            event: PipeEventKind::Event(BridgeEvent::CaptureLost {
                lease: [0; 16],
                reason: CaptureLostReason::UserModeChanged,
            }),
            ..unexpected_request
        };
        assert!(zero_lease.validate().is_err());
    }

    #[test]
    fn outcome_detail_is_bounded() {
        let outcome = CommandOutcome::failed("e".repeat(MAX_DETAIL_LEN + 100));
        assert_eq!(outcome.detail.len(), MAX_DETAIL_LEN);
        assert_eq!(outcome.status, CommandStatus::Failed);
        assert_eq!(CommandOutcome::succeeded().status, CommandStatus::Succeeded);
    }

    #[test]
    fn relative_origin_cwd_is_rejected() {
        let origin = ZellijOrigin {
            client_id: "c".to_owned(),
            session_name: None,
            prior_pane_id: None,
            ui_pane_id: "pane-1".to_owned(),
            prior_pane_cwd: Some("relative/path".to_owned()),
        };
        assert!(origin.validate().is_err());
    }

    #[test]
    fn bridge_identity_requires_nonzero_fingerprints() {
        let identity = BridgeIdentity {
            muxe_version: "0.1.0".to_owned(),
            source_revision: "rev".to_owned(),
            action_fingerprint: [0; 32],
            protocol_fingerprint: [0; 32],
            bridge_build_id: None,
        };
        assert!(identity.validate().is_err());
    }

    #[test]
    fn bridge_identity_rejects_zero_build_id() {
        let identity = BridgeIdentity {
            muxe_version: "0.1.0".to_owned(),
            source_revision: "rev".to_owned(),
            action_fingerprint: [1; 32],
            protocol_fingerprint: [1; 32],
            bridge_build_id: Some(SchemaFingerprint([0; 32])),
        };
        assert!(identity.validate().is_err());
    }
}
