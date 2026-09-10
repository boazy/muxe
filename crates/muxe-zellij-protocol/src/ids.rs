//! Strong identifier and version types for the Zellij pipe protocol.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU64};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;
use ulid::Ulid;

/// Errors constructing or advancing a Zellij protocol scalar.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ProtocolScalarError {
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

/// Version of the Zellij JSON pipe protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtocolVersion(NonZeroU16);

impl ProtocolVersion {
    /// Protocol version implemented by this build.
    pub const CURRENT: Self = Self(NonZeroU16::MIN);

    /// Whether this value names the protocol implemented by this build.
    #[must_use]
    pub const fn is_current(self) -> bool {
        self.0.get() == Self::CURRENT.0.get()
    }

    /// Raw wire value for formatting an external control payload.
    #[must_use]
    pub const fn wire_value(self) -> u16 {
        self.0.get()
    }
}

impl TryFrom<u16> for ProtocolVersion {
    type Error = std::num::TryFromIntError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        NonZeroU16::try_from(value).map(Self)
    }
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// One installed generation of the Zellij pipe pair.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChannelGeneration(NonZeroU64);

impl ChannelGeneration {
    /// Initial installed generation.
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    /// Advances to a fresh generation without wraparound.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolScalarError::Exhausted`] at `u64::MAX`.
    pub fn advance(&mut self) -> Result<Self, ProtocolScalarError> {
        let next = self
            .0
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(ProtocolScalarError::Exhausted {
                field: "channel generation",
            })?;
        self.0 = next;
        Ok(*self)
    }

    /// Raw wire value for the subprocess subscription boundary.
    #[must_use]
    pub const fn wire_value(self) -> u64 {
        self.0.get()
    }
}

impl Default for ChannelGeneration {
    fn default() -> Self {
        Self::INITIAL
    }
}

impl TryFrom<u64> for ChannelGeneration {
    type Error = ProtocolScalarError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProtocolScalarError::ZeroChannelGeneration)
    }
}

impl fmt::Display for ChannelGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Request identity scoped to one bridge registration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(NonZeroU64);

impl RequestId {
    /// First request in a fresh registration.
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    /// Returns the next request identity without mutating this value.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolScalarError::Exhausted`] at `u64::MAX`.
    pub fn next(self) -> Result<Self, ProtocolScalarError> {
        self.0
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .map(Self)
            .ok_or(ProtocolScalarError::Exhausted {
                field: "request ID",
            })
    }

    /// Raw value for host action-completion context.
    #[must_use]
    pub const fn wire_value(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for RequestId {
    type Error = ProtocolScalarError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProtocolScalarError::ZeroRequestId)
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Fresh non-monotonic ULID identifying one bridge registration epoch.
///
/// All 128 value bits come directly from a CSPRNG. The ULID wrapper supplies
/// canonical 26-character Crockford Base32 text without imposing timestamp or
/// monotonic-generation semantics on registration identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RegistrationId(Ulid);

impl RegistrationId {
    /// Constructs a registration ULID from 128 independent random bits.
    ///
    /// # Errors
    ///
    /// Returns an error when the random value is the reserved nil ULID.
    pub fn from_random_bytes(bytes: [u8; 16]) -> Result<Self, ProtocolScalarError> {
        Self::try_from(Ulid::from_bytes(bytes))
    }
}

impl TryFrom<Ulid> for RegistrationId {
    type Error = ProtocolScalarError;

    fn try_from(value: Ulid) -> Result<Self, Self::Error> {
        if value.is_nil() {
            Err(ProtocolScalarError::NilRegistration)
        } else {
            Ok(Self(value))
        }
    }
}

impl FromStr for RegistrationId {
    type Err = ProtocolScalarError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = Ulid::from_string(value)
            .map_err(|error| ProtocolScalarError::InvalidRegistration(error.to_string()))?;
        Self::try_from(parsed)
    }
}

impl fmt::Display for RegistrationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for RegistrationId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RegistrationId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Ulid::deserialize(deserializer)?;
        Self::try_from(value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_uses_canonical_ulid_text_with_all_random_bits() {
        let registration =
            RegistrationId::from_random_bytes([0xA5; 16]).expect("registration");
        assert_eq!(registration.to_string().len(), 26);
        let encoded = serde_json::to_string(&registration).expect("registration serializes");
        assert_eq!(encoded.len(), 28);
        assert_eq!(
            serde_json::from_str::<RegistrationId>(&encoded).expect("registration deserializes"),
            registration
        );
        assert!(RegistrationId::from_random_bytes([0; 16]).is_err());
    }

    #[test]
    fn generations_only_progress_and_request_ids_never_wrap() {
        let mut generation = ChannelGeneration::default();
        assert_eq!(generation.advance(), Ok(ChannelGeneration::try_from(2).expect("two")));
        let maximum = RequestId::try_from(u64::MAX).expect("maximum request ID");
        assert!(maximum.next().is_err());
    }
}
