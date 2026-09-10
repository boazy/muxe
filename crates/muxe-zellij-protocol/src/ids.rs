//! Shared bridge identifiers specialized by the Zellij protocol.

pub use muxe_protocol::{
    BridgeChannelGeneration as ChannelGeneration, BridgeProtocolScalarError as ProtocolScalarError,
    BridgeProtocolVersion as ProtocolVersion, BridgeRegistrationId as RegistrationId,
    BridgeRequestId as RequestId,
};

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
