//! Shared bridge identifiers specialized by the Zellij protocol.

use std::fmt;
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

pub use muxe_protocol::{
    BridgeProtocolScalarError as ProtocolScalarError, BridgeProtocolVersion as ProtocolVersion,
    BridgeRegistrationId as RegistrationId, BridgeRequestId as RequestId,
};

/// One installed generation of the Zellij CLI-pipe pair.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_uses_canonical_ulid_text_with_all_random_bits() {
        let registration = RegistrationId::from_random_bytes([0xA5; 16]).expect("registration");
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
        assert_eq!(
            generation.advance(),
            Ok(ChannelGeneration::try_from(2).expect("two"))
        );
        let maximum = RequestId::try_from(u64::MAX).expect("maximum request ID");
        assert!(maximum.next().is_err());
    }
}
