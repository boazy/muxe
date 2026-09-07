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

use muxe_protocol::SchemaFingerprint;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::generated::RawNativeCommand;

/// Pipe protocol version. Bridges reject requests with any other version.
pub const BRIDGE_PROTOCOL_VERSION: u16 = 1;
/// Maximum accepted JSON line length on either pipe, in bytes.
pub const MAX_PIPE_LINE_LEN: usize = 64 * 1024;
/// Maximum length of a human-facing detail or diagnostic string.
pub const MAX_DETAIL_LEN: usize = 4 * 1024;

/// Targeted delivery: Zellij broadcasts pipe messages to every bridge instance,
/// so only the active registration named here may act on a request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BridgeTarget {
    /// Zellij client ID owning the target bridge, as reported by `list_clients`.
    pub client_id: String,
    /// Active bridge registration ID for that client.
    pub registration: [u8; 16],
}

/// One broker-to-bridge frame on the request pipe.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PipeRequest {
    /// Must equal [`BRIDGE_PROTOCOL_VERSION`].
    pub protocol: u16,
    /// Unpredictable broker-issued correlation ID, echoed by every event for it.
    pub request_id: [u8; 16],
    /// Request-channel generation; stale generations are ignored after restart.
    pub channel_generation: u64,
    /// Only the named registration acts; every other instance drops the frame.
    pub target: BridgeTarget,
    /// Typed host payload. Raw mirrors are revalidated by the bridge before dispatch.
    pub payload: BridgeRequest,
}

/// Typed broker-to-bridge payloads. Dispatch payloads carry generated raw mirrors
/// so the bridge performs the same `Raw -> Validated -> upstream` conversion the
/// native adapter used at configuration load.
#[expect(
    clippy::large_enum_variant,
    reason = "dispatch carries its typed raw command inline to avoid a heap allocation on every broker-to-bridge request"
)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum BridgeRequest {
    /// Dispatch one validated-at-load native command through the target bridge.
    Dispatch {
        /// Broker execution ID, echoed in acceptance and completion events.
        execution: String,
        /// Raw command; the bridge revalidates before dispatching.
        command: RawNativeCommand,
    },
    /// Snapshot the client's current input mode and enter Zellij Locked mode.
    BeginCapture {
        /// Capture lease minted by the broker for one modal scope.
        lease: [u8; 16],
        /// Broker UI session the capture serves.
        ui_session: String,
    },
    /// Release a capture lease with guarded restoration of the prior mode.
    EndCapture {
        /// Lease that must still own capture for restoration to happen.
        lease: [u8; 16],
        /// Why capture ends; user-driven mode changes are never restored over.
        reason: CaptureEndReason,
    },
    /// Snapshot the last focused non-Muxe pane for origin-context capture.
    RequestOrigin {
        /// Broker UI session the snapshot serves.
        ui_session: String,
        /// The Muxe UI's own pane ID, used to exclude it from focus history.
        ui_pane: String,
    },
    /// Focus the indexed pane of the active tab through the bridge's tracked
    /// pane inventory (`PaneUpdate` manifest order). No pinned primitive takes
    /// a positional pane target, so the bridge resolves the index against its
    /// live inventory and focuses by ID; an out-of-range index completes Failed.
    FocusPaneByIndex {
        /// Broker execution ID, echoed in acceptance and completion events.
        execution: String,
        /// Position in the active tab's manifest pane order.
        index: u32,
    },
    /// Focus the nearest pane from the origin pane in one cardinal direction
    /// through the bridge's tracked geometry. Directional `MoveFocus` acts
    /// from the currently focused (menu) pane, so it cannot honor the
    /// immutable origin; the bridge computes the neighbor from the origin
    /// pane's tracked geometry and focuses by ID. No neighbor completes Failed.
    FocusPaneNeighbor {
        /// Broker execution ID, echoed in acceptance and completion events.
        execution: String,
        /// Cardinal direction from the origin pane.
        direction: NeighborDirection,
    },
    /// Best-effort retirement notice; correctness never depends on its delivery.
    RetireBridge,
}

/// Why broker-side capture ends. Mirrors the adapter's release reasons without
/// importing async adapter types into this transport crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CaptureEndReason {
    /// Menu dismissed normally; restore the captured mode while it is still ours.
    UiDismissed,
    /// A replacement menu takes the modal scope; ordered release then recapture.
    Replaced,
    /// The broker lease expired; the bridge keeps user-owned modes untouched.
    LeaseExpired,
    /// The user changed modes; the newer mode is authoritative, never restored over.
    UserModeChanged,
    /// Broker shutdown; restore only Muxe-owned Locked mode.
    AdapterShutdown,
}

/// Cardinal direction for bridge-resolved neighbor focus.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NeighborDirection {
    /// Pane to the left of the origin pane.
    Left,
    /// Pane to the right of the origin pane.
    Right,
    /// Pane above the origin pane.
    Up,
    /// Pane below the origin pane.
    Down,
}
/// One bridge-to-broker frame on the event pipe.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipeEvent {
    /// Monotonic per-registration sequence starting at 1; gaps mark lost events.
    pub sequence: u64,
    /// Typed event payload.
    pub event: PipeEventKind,
}

/// Typed bridge-to-broker payloads.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipeEventKind {
    /// Fresh registration on a new event channel, including after plugin reload
    /// or event-pipe replacement. Supersedes any previous registration for the
    /// client; the broker retires the displaced ID.
    Register {
        /// Zellij client ID selected from the entry marked `is_current_client`.
        client_id: String,
        /// Currently focused pane from the bridge's perspective, if known.
        current_pane: Option<String>,
        /// Fresh unpredictable registration ID for this channel lifetime.
        registration: [u8; 16],
        /// Zellij plugin ID for diagnostics only; never a Muxe identity.
        plugin_id: Option<u32>,
        /// Version and fingerprint handshake material.
        identity: BridgeIdentity,
    },
    /// Transport acknowledgement: the target bridge validated the request and
    /// asked Zellij to unblock the request pipe. This is not action success;
    /// dispatch acceptance and completion are separate events.
    RequestReleased {
        /// Request being released.
        request_id: [u8; 16],
        /// Channel generation the request was sent on; stale generations are ignored.
        channel_generation: u64,
        /// Registration that acted on it.
        registration: [u8; 16],
    },
    /// The bridge accepted a dispatch request after revalidation.
    DispatchAccepted {
        /// Request that was accepted.
        request_id: [u8; 16],
        /// Broker execution ID from the request.
        execution: String,
    },
    /// Final outcome for one accepted dispatch.
    DispatchCompleted {
        /// Request that completed.
        request_id: [u8; 16],
        /// Broker execution ID from the request.
        execution: String,
        /// Typed outcome; see [`CommandOutcome`].
        outcome: CommandOutcome,
    },
    /// Origin snapshot answering a [`BridgeRequest::RequestOrigin`].
    OriginSnapshot {
        /// Broker UI session from the request.
        ui_session: String,
        /// Snapshot of the last focused non-Muxe pane and its context.
        origin: ZellijOrigin,
    },
    /// The UI pane in a [`BridgeRequest::RequestOrigin`] does not belong to
    /// this bridge's client. The adapter moves on to the next client; no unique
    /// match fails the bootstrap rather than guessing.
    OriginDeclined {
        /// Broker UI session from the request, for waiter routing.
        ui_session: String,
        /// Request being declined.
        request_id: [u8; 16],
        /// Registration that declined it.
        registration: [u8; 16],
    },
    /// Locked-mode capture confirmed with the snapshotted prior mode.
    CaptureReady {
        /// Lease that now owns capture.
        lease: [u8; 16],
        /// Input mode to restore on guarded release, as a Zellij mode name.
        prior_mode: String,
    },
    /// Capture ended without broker request, or a release was refused.
    CaptureLost {
        /// Lease that lost capture.
        lease: [u8; 16],
        /// Why capture was lost.
        reason: CaptureLostReason,
    },
    /// Periodic liveness for the heartbeat lease owned by one registration.
    Heartbeat {
        /// Registration renewing its lease.
        registration: [u8; 16],
        /// Client the registration serves.
        client_id: String,
    },
}

/// Why the bridge stopped owning capture outside the broker-driven path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CaptureLostReason {
    /// The user (or another plugin) changed modes; the new mode is authoritative.
    UserModeChanged,
    /// The bridge is unloading; the broker must invalidate the registration.
    BridgeUnloading,
}

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

impl PipeRequest {
    /// Semantic validation before a bridge acts on a request.
    ///
    /// # Errors
    ///
    /// Returns [`PipeError::Validation`] when the version, IDs, or payload are invalid.
    pub fn validate(&self) -> Result<(), PipeError> {
        if self.protocol != BRIDGE_PROTOCOL_VERSION {
            return Err(PipeError::Validation {
                reason: format!(
                    "unsupported bridge protocol {}; expected {BRIDGE_PROTOCOL_VERSION}",
                    self.protocol
                ),
            });
        }
        if self.request_id == [0; 16] {
            return Err(PipeError::Validation {
                reason: "request ID must not be zero".to_owned(),
            });
        }
        if self.target.registration == [0; 16] {
            return Err(PipeError::Validation {
                reason: "target registration must not be zero".to_owned(),
            });
        }
        require_non_empty("target client ID", &self.target.client_id)?;
        self.payload.validate()
    }
}

impl BridgeRequest {
    /// Semantic validation for a typed request payload.
    ///
    /// # Errors
    ///
    /// Returns [`PipeError::Validation`] when an ID is empty or overlong.
    pub fn validate(&self) -> Result<(), PipeError> {
        match self {
            Self::Dispatch { execution, .. } => require_non_empty("execution ID", execution),
            Self::BeginCapture { lease, ui_session } => {
                if lease == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "capture lease must not be zero".to_owned(),
                    });
                }
                require_non_empty("UI session", ui_session)
            }
            Self::EndCapture { lease, .. } => {
                if lease == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "capture lease must not be zero".to_owned(),
                    });
                }
                Ok(())
            }
            Self::RequestOrigin {
                ui_session,
                ui_pane,
            } => {
                require_non_empty("UI session", ui_session)?;
                require_non_empty("UI pane", ui_pane)
            }
            Self::FocusPaneByIndex { execution, .. }
            | Self::FocusPaneNeighbor { execution, .. } => {
                require_non_empty("execution ID", execution)
            }
            Self::RetireBridge => Ok(()),
        }
    }
}

impl PipeEvent {
    /// Semantic validation before the broker routes an event.
    ///
    /// # Errors
    ///
    /// Returns [`PipeError::Validation`] when the sequence is zero or payload invalid.
    pub fn validate(&self) -> Result<(), PipeError> {
        if self.sequence == 0 {
            return Err(PipeError::Validation {
                reason: "event sequence must not be zero".to_owned(),
            });
        }
        self.event.validate()
    }
}

impl PipeEventKind {
    /// Semantic validation for a typed event payload.
    ///
    /// # Errors
    ///
    /// Returns [`PipeError::Validation`] when IDs, fingerprints, or details are invalid.
    #[expect(
        clippy::too_many_lines,
        reason = "the exhaustive event wire contract remains co-located so every variant is reviewed together"
    )]
    pub fn validate(&self) -> Result<(), PipeError> {
        match self {
            Self::Register {
                client_id,
                registration,
                identity,
                ..
            } => {
                require_non_empty("client ID", client_id)?;
                if registration == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "registration ID must not be zero".to_owned(),
                    });
                }
                identity.validate()
            }
            Self::RequestReleased {
                request_id,
                registration,
                ..
            } => {
                if request_id == &[0; 16] || registration == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "release acknowledgement IDs must not be zero".to_owned(),
                    });
                }
                Ok(())
            }
            Self::DispatchAccepted {
                request_id,
                execution,
            } => {
                if request_id == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "request ID must not be zero".to_owned(),
                    });
                }
                require_non_empty("execution ID", execution)
            }
            Self::DispatchCompleted {
                request_id,
                execution,
                outcome,
            } => {
                if request_id == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "request ID must not be zero".to_owned(),
                    });
                }
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
            Self::OriginDeclined {
                ui_session,
                request_id,
                registration,
            } => {
                require_non_empty("UI session", ui_session)?;
                if request_id == &[0; 16] || registration == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "decline IDs must not be zero".to_owned(),
                    });
                }
                Ok(())
            }
            Self::CaptureReady { lease, prior_mode } => {
                if lease == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "capture lease must not be zero".to_owned(),
                    });
                }
                require_non_empty("prior input mode", prior_mode)
            }
            Self::CaptureLost { lease, .. } => {
                if lease == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "capture lease must not be zero".to_owned(),
                    });
                }
                Ok(())
            }
            Self::Heartbeat {
                registration,
                client_id,
            } => {
                if registration == &[0; 16] {
                    return Err(PipeError::Validation {
                        reason: "registration ID must not be zero".to_owned(),
                    });
                }
                require_non_empty("client ID", client_id)
            }
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

    fn sample_target() -> BridgeTarget {
        BridgeTarget {
            client_id: "client-1".to_owned(),
            registration: [7; 16],
        }
    }

    #[test]
    fn request_round_trip_preserves_typed_payload() {
        let request = PipeRequest {
            protocol: BRIDGE_PROTOCOL_VERSION,
            request_id: [1; 16],
            channel_generation: 3,
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
    fn wrong_protocol_version_is_rejected() {
        let mut request = PipeRequest {
            protocol: BRIDGE_PROTOCOL_VERSION + 1,
            request_id: [1; 16],
            channel_generation: 0,
            target: sample_target(),
            payload: BridgeRequest::RetireBridge,
        };
        assert!(request.validate().is_err());
        request.protocol = BRIDGE_PROTOCOL_VERSION;
        request.request_id = [0; 16];
        assert!(request.validate().is_err());
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
    fn zero_sequence_and_zero_lease_are_rejected() {
        let event = PipeEvent {
            sequence: 0,
            event: PipeEventKind::Heartbeat {
                registration: [2; 16],
                client_id: "c".to_owned(),
            },
        };
        assert!(event.validate().is_err());
        let lost = PipeEventKind::CaptureLost {
            lease: [0; 16],
            reason: CaptureLostReason::UserModeChanged,
        };
        assert!(lost.validate().is_err());
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
