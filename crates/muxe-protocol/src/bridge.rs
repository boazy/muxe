//! Host-independent bridge envelopes and correlation scalars.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU64};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;
use ulid::Ulid;

use crate::wire::{CaptureLeaseId, ExecutionId, SemanticError, UiSessionId, Validate};

/// Errors constructing or advancing a bridge protocol scalar.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum BridgeProtocolScalarError {
    /// Zero is reserved for the absence of a request identity.
    #[error("request ID must not be zero")]
    ZeroRequestId,
    /// Zero is not an installed channel generation.
    #[error("channel generation must not be zero")]
    ZeroChannelGeneration,
    /// A counter cannot advance without reusing an earlier value.
    #[error("{field} exhausted its value space")]
    Exhausted {
        /// Counter whose value space was exhausted.
        field: &'static str,
    },
    /// The nil ULID is reserved and never identifies a registration.
    #[error("registration ID must not be the nil ULID")]
    NilRegistration,
    /// The text is not a canonical ULID.
    #[error("invalid registration ULID: {0}")]
    InvalidRegistration(String),
}

/// Version of a host bridge protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BridgeProtocolVersion(NonZeroU16);

impl BridgeProtocolVersion {
    /// Initial protocol version.
    pub const INITIAL: Self = Self(NonZeroU16::MIN);
    /// Current v1 protocol version.
    pub const CURRENT: Self = Self::INITIAL;
    /// Whether this value names the protocol implemented by this build.
    #[must_use]
    pub const fn is_current(self) -> bool {
        self.0.get() == Self::CURRENT.0.get()
    }

    /// Raw wire value.
    #[must_use]
    pub const fn wire_value(self) -> u16 {
        self.0.get()
    }
}

impl TryFrom<u16> for BridgeProtocolVersion {
    type Error = std::num::TryFromIntError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        NonZeroU16::try_from(value).map(Self)
    }
}

impl fmt::Display for BridgeProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Request identity scoped to one bridge registration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BridgeRequestId(NonZeroU64);

impl BridgeRequestId {
    /// First request in a fresh registration.
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    /// Returns the next request identity without mutating this value.
    ///
    /// # Errors
    ///
    /// Returns [`BridgeProtocolScalarError::Exhausted`] at `u64::MAX`.
    pub fn next(self) -> Result<Self, BridgeProtocolScalarError> {
        self.0
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(BridgeProtocolScalarError::Exhausted {
                field: "request ID",
            })
    }

    /// Raw wire value.
    #[must_use]
    pub const fn wire_value(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for BridgeRequestId {
    type Error = BridgeProtocolScalarError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(BridgeProtocolScalarError::ZeroRequestId)
    }
}

impl fmt::Display for BridgeRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Fresh non-monotonic ULID identifying one bridge registration epoch.
///
/// All 128 value bits come directly from a CSPRNG. The ULID encoding supplies
/// canonical text without imposing timestamp or monotonic-generation semantics.
#[derive(
    rkyv::Archive,
    rkyv::Deserialize,
    rkyv::Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub struct BridgeRegistrationId([u8; 16]);

impl BridgeRegistrationId {
    /// Constructs a registration ULID from 128 independent random bits.
    ///
    /// # Errors
    ///
    /// Returns an error when the random value is the reserved nil ULID.
    pub fn from_random_bytes(bytes: [u8; 16]) -> Result<Self, BridgeProtocolScalarError> {
        if bytes == [0; 16] {
            Err(BridgeProtocolScalarError::NilRegistration)
        } else {
            Ok(Self(bytes))
        }
    }

    /// Whether this is the reserved nil registration.
    #[must_use]
    pub fn is_zero(self) -> bool {
        self.0 == [0; 16]
    }
}

impl FromStr for BridgeRegistrationId {
    type Err = BridgeProtocolScalarError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = Ulid::from_string(value)
            .map_err(|error| BridgeProtocolScalarError::InvalidRegistration(error.to_string()))?;
        Self::from_random_bytes(parsed.to_bytes())
    }
}

impl fmt::Display for BridgeRegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ulid::from_bytes(self.0).fmt(formatter)
    }
}

impl Serialize for BridgeRegistrationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for BridgeRegistrationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(D::Error::custom)
    }
}

impl Validate for BridgeRegistrationId {
    fn validate(&self) -> Result<(), SemanticError> {
        if self.is_zero() {
            Err(SemanticError::ZeroNonce("BridgeRegistrationId"))
        } else {
            Ok(())
        }
    }
}

/// Why broker-owned input capture ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BridgeCaptureEndReason {
    UiDismissed,
    Replaced,
    LeaseExpired,
    UserModeChanged,
    AdapterShutdown,
}

/// Why a bridge stopped owning input capture without a broker request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BridgeCaptureLostReason {
    UserModeChanged,
    BridgeUnloading,
    AdapterHealth,
}

/// Host-independent broker-to-bridge lifecycle with typed host extensions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BridgeRequest<D, O, H> {
    Dispatch {
        execution: ExecutionId,
        request: D,
    },
    BeginCapture {
        lease: CaptureLeaseId,
        ui_session: UiSessionId,
    },
    EndCapture {
        lease: CaptureLeaseId,
        reason: BridgeCaptureEndReason,
    },
    RequestOrigin {
        ui_session: UiSessionId,
        request: O,
    },
    Retire,
    Shutdown,
    Host(H),
}

/// Host-independent solicited bridge result with typed host result payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BridgeResponse<D, O, C, H> {
    RequestReleased,
    DispatchAccepted { execution: ExecutionId },
    DispatchCompleted { execution: ExecutionId, outcome: D },
    OriginSnapshot { ui_session: UiSessionId, origin: O },
    OriginDeclined { ui_session: UiSessionId },
    CaptureReady { lease: CaptureLeaseId, state: C },
    Host(H),
}

/// Host-independent unsolicited bridge lifecycle with typed registration data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BridgeEvent<R, H> {
    Register {
        registration: R,
    },
    CaptureLost {
        lease: CaptureLeaseId,
        reason: BridgeCaptureLostReason,
    },
    Heartbeat,
    Health {
        detail: Option<String>,
    },
    Host(H),
}

/// One host-adapter request with generic transport provenance and typed payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeRequestEnvelope<G, T, P> {
    pub protocol: BridgeProtocolVersion,
    pub request_id: BridgeRequestId,
    pub registration: BridgeRegistrationId,
    pub channel_generation: G,
    pub target: T,
    pub payload: P,
}

/// One solicited bridge response for transports with a dedicated response path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeResponseEnvelope<P> {
    pub protocol: BridgeProtocolVersion,
    pub registration: BridgeRegistrationId,
    pub request_id: BridgeRequestId,
    pub response: P,
}

/// One event-channel frame. `request_id` is `None` only for unsolicited payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BridgeEventEnvelope<G, P> {
    pub protocol: BridgeProtocolVersion,
    pub request_id: Option<BridgeRequestId>,
    pub registration: BridgeRegistrationId,
    pub channel_generation: G,
    pub event: P,
}

/// Direction of one payload on a combined response/event transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BridgeOutput<R, E> {
    Response(R),
    Event(E),
}
