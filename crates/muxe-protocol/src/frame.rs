use std::mem;

use rkyv::{rancor::Error as RkyvError, util::AlignedVec};
use thiserror::Error;

use crate::wire::{
    ArchivedWireMessage, Codec, ConnectionPhase, MAX_FRAME_LEN, MessageDirection, PROTOCOL_VERSION,
    PeerRole, SchemaFingerprint, SemanticError, WireMessage, validate_archived_wire_message,
};

pub const PRELUDE_LEN: usize = 44;
const MAGIC: [u8; 4] = *b"MUXE";
const LENGTH_PREFIX_LEN: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Prelude {
    pub protocol_version: u16,
    pub codec: Codec,
    pub role: PeerRole,
    pub schema_fingerprint: SchemaFingerprint,
    pub frame_limit: u32,
}

impl Prelude {
    pub const fn rkyv(role: PeerRole, schema_fingerprint: SchemaFingerprint) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            codec: Codec::Rkyv,
            role,
            schema_fingerprint,
            frame_limit: MAX_FRAME_LEN,
        }
    }

    pub const fn control(role: PeerRole) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            codec: Codec::ControlJsonV1,
            role,
            schema_fingerprint: SchemaFingerprint::ZERO,
            frame_limit: crate::wire::MAX_CONTROL_FRAME_LEN,
        }
    }

    pub fn encode(self) -> [u8; PRELUDE_LEN] {
        let mut bytes = [0; PRELUDE_LEN];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&self.protocol_version.to_be_bytes());
        bytes[6] = self.codec.as_byte();
        bytes[7] = self.role.as_byte();
        bytes[8..40].copy_from_slice(&self.schema_fingerprint.0);
        bytes[40..44].copy_from_slice(&self.frame_limit.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: [u8; PRELUDE_LEN]) -> Result<Self, DecodeError> {
        if bytes[..4] != MAGIC {
            return Err(DecodeError::BadMagic);
        }
        let protocol_version = u16::from_be_bytes([bytes[4], bytes[5]]);
        let codec = Codec::from_byte(bytes[6]).ok_or(DecodeError::UnknownCodec(bytes[6]))?;
        let role = PeerRole::from_byte(bytes[7]).ok_or(DecodeError::UnknownPeerRole(bytes[7]))?;
        let mut fingerprint = [0; 32];
        fingerprint.copy_from_slice(&bytes[8..40]);
        let frame_limit = u32::from_be_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]);
        Ok(Self {
            protocol_version,
            codec,
            role,
            schema_fingerprint: SchemaFingerprint(fingerprint),
            frame_limit,
        })
    }

    pub fn validate(
        &self,
        expected_codec: Codec,
        expected_role: PeerRole,
        expected_fingerprint: SchemaFingerprint,
        expected_frame_limit: u32,
    ) -> Result<(), DecodeError> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(DecodeError::ProtocolVersionMismatch {
                expected: PROTOCOL_VERSION,
                received: self.protocol_version,
            });
        }
        if self.codec != expected_codec {
            return Err(DecodeError::CodecMismatch {
                expected: expected_codec,
                received: self.codec,
            });
        }
        if self.role != expected_role {
            return Err(DecodeError::PeerRoleMismatch {
                expected: expected_role,
                received: self.role,
            });
        }
        if self.schema_fingerprint != expected_fingerprint {
            return Err(DecodeError::SchemaFingerprintMismatch);
        }
        if self.frame_limit != expected_frame_limit {
            return Err(DecodeError::FrameLimitMismatch {
                expected: expected_frame_limit,
                received: self.frame_limit,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionPolicy {
    pub role: PeerRole,
    pub direction: MessageDirection,
    pub schema_fingerprint: SchemaFingerprint,
}

impl ConnectionPolicy {
    pub const fn broker(role: PeerRole, schema_fingerprint: SchemaFingerprint) -> Self {
        Self {
            role,
            direction: MessageDirection::PeerToBroker,
            schema_fingerprint,
        }
    }

    pub const fn client(role: PeerRole, schema_fingerprint: SchemaFingerprint) -> Self {
        Self {
            role,
            direction: MessageDirection::BrokerToPeer,
            schema_fingerprint,
        }
    }

    fn initial_phase(self) -> ConnectionPhase {
        match self.direction {
            MessageDirection::PeerToBroker => ConnectionPhase::AwaitingHello,
            MessageDirection::BrokerToPeer => ConnectionPhase::AwaitingWelcome,
        }
    }
}

#[derive(Debug)]
pub struct ArchivedFrame {
    bytes: AlignedVec,
}

impl ArchivedFrame {
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub fn archived(&self) -> Result<&ArchivedWireMessage, DecodeError> {
        rkyv::access::<ArchivedWireMessage, RkyvError>(self.bytes.as_slice()).map_err(archive_error)
    }

    pub fn deserialize(&self) -> Result<WireMessage, DecodeError> {
        let archived = self.archived()?;
        rkyv::deserialize::<WireMessage, RkyvError>(archived).map_err(archive_error)
    }
}

/// A frame split into its endian-stable prefix and aligned archive payload.
///
/// Writers must write the two slices in order instead of joining them into another allocation.
#[derive(Debug)]
pub struct EncodedFrame {
    prefix: [u8; LENGTH_PREFIX_LEN],
    payload: AlignedVec,
}

impl EncodedFrame {
    pub fn prefix(&self) -> &[u8; LENGTH_PREFIX_LEN] {
        &self.prefix
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn into_parts(self) -> ([u8; LENGTH_PREFIX_LEN], AlignedVec) {
        (self.prefix, self.payload)
    }
}

pub fn encode_frame(message: &WireMessage) -> Result<EncodedFrame, DecodeError> {
    let payload = rkyv::to_bytes::<RkyvError>(message).map_err(archive_error)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| DecodeError::FrameTooLarge {
        declared: u32::MAX,
        maximum: MAX_FRAME_LEN,
    })?;
    if payload_len == 0 {
        return Err(DecodeError::EmptyFrame);
    }
    if payload_len > MAX_FRAME_LEN {
        return Err(DecodeError::FrameTooLarge {
            declared: payload_len,
            maximum: MAX_FRAME_LEN,
        });
    }
    Ok(EncodedFrame {
        prefix: payload_len.to_be_bytes(),
        payload,
    })
}

#[derive(Debug)]
pub struct ConnectionDecoder {
    policy: ConnectionPolicy,
    phase: ConnectionPhase,
    state: DecoderState,
    prelude: Option<Prelude>,
    failed: bool,
}

#[derive(Debug)]
enum DecoderState {
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
        bytes: AlignedVec,
    },
    Failed,
}

impl ConnectionDecoder {
    pub fn new(policy: ConnectionPolicy) -> Self {
        Self {
            phase: policy.initial_phase(),
            policy,
            state: DecoderState::Prelude {
                bytes: [0; PRELUDE_LEN],
                filled: 0,
            },
            prelude: None,
            failed: false,
        }
    }

    pub fn prelude(&self) -> Option<Prelude> {
        self.prelude
    }

    /// Decodes arbitrary stream chunks and calls `on_frame` for each valid frame before any later
    /// coalesced frame can close the connection. Earlier valid messages therefore never disappear
    /// merely because a subsequent frame is malformed.
    pub fn push<F>(&mut self, mut input: &[u8], mut on_frame: F) -> Result<(), DecodeError>
    where
        F: FnMut(ArchivedFrame),
    {
        if self.failed {
            return Err(DecodeError::DecoderClosed);
        }

        while !input.is_empty() {
            let state = mem::replace(&mut self.state, DecoderState::Failed);
            match state {
                DecoderState::Prelude {
                    mut bytes,
                    mut filled,
                } => {
                    let copied = copy_from_input(&mut bytes, &mut filled, input);
                    input = &input[copied..];
                    if filled != PRELUDE_LEN {
                        self.state = DecoderState::Prelude { bytes, filled };
                        continue;
                    }
                    let prelude = match Prelude::decode(bytes).and_then(|prelude| {
                        prelude.validate(
                            Codec::Rkyv,
                            self.policy.role,
                            self.policy.schema_fingerprint,
                            MAX_FRAME_LEN,
                        )?;
                        Ok(prelude)
                    }) {
                        Ok(prelude) => prelude,
                        Err(error) => return self.fail(error),
                    };
                    self.prelude = Some(prelude);
                    self.state = DecoderState::LengthPrefix {
                        bytes: [0; LENGTH_PREFIX_LEN],
                        filled: 0,
                    };
                }
                DecoderState::LengthPrefix {
                    mut bytes,
                    mut filled,
                } => {
                    let copied = copy_from_input(&mut bytes, &mut filled, input);
                    input = &input[copied..];
                    if filled != LENGTH_PREFIX_LEN {
                        self.state = DecoderState::LengthPrefix { bytes, filled };
                        continue;
                    }
                    let declared_len = u32::from_be_bytes(bytes);
                    if declared_len == 0 {
                        return self.fail(DecodeError::EmptyFrame);
                    }
                    if declared_len > MAX_FRAME_LEN {
                        return self.fail(DecodeError::FrameTooLarge {
                            declared: declared_len,
                            maximum: MAX_FRAME_LEN,
                        });
                    }
                    // The bound above executes before this is allowed to allocate.
                    self.state = DecoderState::Payload {
                        declared_len,
                        bytes: AlignedVec::with_capacity(declared_len as usize),
                    };
                }
                DecoderState::Payload {
                    declared_len,
                    mut bytes,
                } => {
                    let remaining = declared_len as usize - bytes.len();
                    let copied = remaining.min(input.len());
                    bytes.extend_from_slice(&input[..copied]);
                    input = &input[copied..];
                    if bytes.len() != declared_len as usize {
                        self.state = DecoderState::Payload {
                            declared_len,
                            bytes,
                        };
                        continue;
                    }

                    let frame = ArchivedFrame { bytes };
                    let archived = match frame.archived() {
                        Ok(archived) => archived,
                        Err(error) => return self.fail(error),
                    };
                    let next_phase = match validate_archived_wire_message(
                        archived,
                        self.policy.role,
                        self.policy.direction,
                        self.phase,
                    ) {
                        Ok(phase) => phase,
                        Err(error) => return self.fail(DecodeError::Semantic(error)),
                    };
                    self.phase = next_phase;
                    self.state = DecoderState::LengthPrefix {
                        bytes: [0; LENGTH_PREFIX_LEN],
                        filled: 0,
                    };
                    on_frame(frame);
                }
                DecoderState::Failed => return self.fail(DecodeError::DecoderClosed),
            }
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), DecodeError> {
        if self.failed {
            return Err(DecodeError::DecoderClosed);
        }
        let truncated = match &self.state {
            DecoderState::Prelude { filled, .. } => Some(TruncationStage::Prelude(*filled)),
            DecoderState::LengthPrefix { filled: 0, .. } if self.prelude.is_some() => None,
            DecoderState::LengthPrefix { filled, .. } => {
                Some(TruncationStage::LengthPrefix(*filled))
            }
            DecoderState::Payload {
                declared_len,
                bytes,
                ..
            } => Some(TruncationStage::Payload {
                expected: *declared_len,
                received: bytes.len() as u32,
            }),
            DecoderState::Failed => return Err(DecodeError::DecoderClosed),
        };
        match truncated {
            Some(stage) => self.fail(DecodeError::Truncated(stage)),
            None => Ok(()),
        }
    }

    fn fail<T>(&mut self, error: DecodeError) -> Result<T, DecodeError> {
        self.state = DecoderState::Failed;
        self.failed = true;
        Err(error)
    }

    #[cfg(test)]
    fn buffered_payload_len(&self) -> usize {
        match &self.state {
            DecoderState::Payload { bytes, .. } => bytes.len(),
            _ => 0,
        }
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

fn archive_error(error: RkyvError) -> DecodeError {
    DecodeError::InvalidArchive(format!("{error:?}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TruncationStage {
    Prelude(usize),
    LengthPrefix(usize),
    Payload { expected: u32, received: u32 },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("invalid protocol magic")]
    BadMagic,
    #[error("unknown codec byte {0}")]
    UnknownCodec(u8),
    #[error("unknown peer role byte {0}")]
    UnknownPeerRole(u8),
    #[error("protocol version mismatch: expected {expected}, received {received}")]
    ProtocolVersionMismatch { expected: u16, received: u16 },
    #[error("codec mismatch: expected {expected:?}, received {received:?}")]
    CodecMismatch { expected: Codec, received: Codec },
    #[error("peer role mismatch: expected {expected:?}, received {received:?}")]
    PeerRoleMismatch {
        expected: PeerRole,
        received: PeerRole,
    },
    #[error("schema fingerprint mismatch")]
    SchemaFingerprintMismatch,
    #[error("frame limit mismatch: expected {expected}, received {received}")]
    FrameLimitMismatch { expected: u32, received: u32 },
    #[error("zero-length frames are not valid rkyv archives")]
    EmptyFrame,
    #[error("frame length {declared} exceeds maximum {maximum}")]
    FrameTooLarge { declared: u32, maximum: u32 },
    #[error("truncated {0:?}")]
    Truncated(TruncationStage),
    #[error("invalid checked rkyv archive: {0}")]
    InvalidArchive(String),
    #[error("semantic protocol violation: {0}")]
    Semantic(#[from] SemanticError),
    #[error("decoder is closed after a protocol error")]
    DecoderClosed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Hello, LiveServerIdentity, RequestId, ServerId};
    use proptest::prelude::*;

    fn request_id(value: u8) -> RequestId {
        RequestId([value; 16])
    }

    fn hello() -> WireMessage {
        WireMessage::Hello {
            request_id: request_id(1),
            hello: Hello {
                process_version: "0.1.0".into(),
                live_server: LiveServerIdentity {
                    host: crate::wire::HostKind::Zellij,
                    discovery_key: "session-a".into(),
                    server_id: ServerId::new("server-a"),
                },
            },
        }
    }

    fn decoder() -> ConnectionDecoder {
        ConnectionDecoder::new(ConnectionPolicy::broker(
            PeerRole::Ui,
            SchemaFingerprint::application(),
        ))
    }

    fn prelude() -> Vec<u8> {
        Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application())
            .encode()
            .to_vec()
    }

    fn append_frame(output: &mut Vec<u8>, frame: EncodedFrame) {
        output.extend_from_slice(frame.prefix());
        output.extend_from_slice(frame.payload());
    }

    #[test]
    fn delivers_valid_coalesced_frames_before_later_processing() {
        let mut bytes = prelude();
        append_frame(&mut bytes, encode_frame(&hello()).unwrap());
        append_frame(
            &mut bytes,
            encode_frame(&WireMessage::Request {
                request_id: request_id(2),
                request: crate::wire::ClientRequest::Heartbeat,
            })
            .unwrap(),
        );

        let mut frames = Vec::new();
        decoder().push(&bytes, |frame| frames.push(frame)).unwrap();
        assert_eq!(frames.len(), 2);
        assert!(matches!(
            frames[0].archived().unwrap(),
            ArchivedWireMessage::Hello { .. }
        ));
    }

    #[test]
    fn delivers_a_valid_frame_before_a_later_coalesced_malformed_frame() {
        let mut bytes = prelude();
        append_frame(&mut bytes, encode_frame(&hello()).unwrap());
        bytes.extend_from_slice(&4_u32.to_be_bytes());
        bytes.extend_from_slice(&[0, 0, 0, 0]);

        let mut delivered = 0;
        assert!(matches!(
            decoder().push(&bytes, |_| delivered += 1),
            Err(DecodeError::InvalidArchive(_))
        ));
        assert_eq!(delivered, 1);
    }

    #[test]
    fn accepts_every_source_alignment_after_copying_to_aligned_storage() {
        let mut stream = prelude();
        append_frame(&mut stream, encode_frame(&hello()).unwrap());
        for offset in 0..32 {
            let mut source = vec![0; offset];
            source.extend_from_slice(&stream);
            let mut aligned = false;
            decoder()
                .push(&source[offset..], |frame| {
                    aligned = frame.as_bytes().as_ptr() as usize % AlignedVec::<16>::ALIGNMENT == 0;
                })
                .unwrap();
            assert!(aligned);
        }
    }

    #[test]
    fn rejects_empty_and_oversized_length_before_collecting_payload() {
        let mut empty = prelude();
        empty.extend_from_slice(&0_u32.to_be_bytes());
        assert!(matches!(
            decoder().push(&empty, |_| {}),
            Err(DecodeError::EmptyFrame)
        ));

        let mut oversized = prelude();
        oversized.extend_from_slice(&(MAX_FRAME_LEN + 1).to_be_bytes());
        let mut decoder = decoder();
        assert!(matches!(
            decoder.push(&oversized, |_| {}),
            Err(DecodeError::FrameTooLarge { .. })
        ));
        assert_eq!(decoder.buffered_payload_len(), 0);
    }

    #[test]
    fn detects_every_prelude_and_prefix_truncation_boundary() {
        let prelude = prelude();
        for cutoff in 0..PRELUDE_LEN {
            let mut decoder = decoder();
            decoder.push(&prelude[..cutoff], |_| {}).unwrap();
            assert!(matches!(decoder.finish(), Err(DecodeError::Truncated(_))));
        }
        let mut stream = prelude;
        stream.extend_from_slice(&[0, 0, 0, 4]);
        for cutoff in PRELUDE_LEN + 1..PRELUDE_LEN + LENGTH_PREFIX_LEN {
            let mut decoder = decoder();
            decoder.push(&stream[..cutoff], |_| {}).unwrap();
            assert!(matches!(decoder.finish(), Err(DecodeError::Truncated(_))));
        }
    }

    #[test]
    fn rejects_invalid_archives_and_payload_length_mismatches() {
        let mut malformed = prelude();
        malformed.extend_from_slice(&4_u32.to_be_bytes());
        malformed.extend_from_slice(&[0, 0, 0, 0]);
        assert!(matches!(
            decoder().push(&malformed, |_| {}),
            Err(DecodeError::InvalidArchive(_))
        ));

        let valid = encode_frame(&hello()).unwrap();
        let declared = u32::from_be_bytes(*valid.prefix());
        let mut short = prelude();
        short.extend_from_slice(&(declared - 1).to_be_bytes());
        short.extend_from_slice(valid.payload());
        assert!(decoder().push(&short, |_| {}).is_err());

        let mut long = prelude();
        long.extend_from_slice(&(declared + 1).to_be_bytes());
        long.extend_from_slice(valid.payload());
        let mut decoder = decoder();
        decoder.push(&long, |_| {}).unwrap();
        assert!(matches!(decoder.finish(), Err(DecodeError::Truncated(_))));
    }

    #[test]
    fn rejects_wrong_role_schema_and_role_specific_messages() {
        let mut wrong_role = Prelude::rkyv(PeerRole::Launcher, SchemaFingerprint::application())
            .encode()
            .to_vec();
        append_frame(&mut wrong_role, encode_frame(&hello()).unwrap());
        assert!(matches!(
            decoder().push(&wrong_role, |_| {}),
            Err(DecodeError::PeerRoleMismatch { .. })
        ));

        let mut wrong_schema = Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::ZERO)
            .encode()
            .to_vec();
        append_frame(&mut wrong_schema, encode_frame(&hello()).unwrap());
        assert!(matches!(
            decoder().push(&wrong_schema, |_| {}),
            Err(DecodeError::SchemaFingerprintMismatch)
        ));

        let mut illegal = prelude();
        append_frame(&mut illegal, encode_frame(&hello()).unwrap());
        append_frame(
            &mut illegal,
            encode_frame(&WireMessage::Request {
                request_id: request_id(2),
                request: crate::wire::ClientRequest::PrepareUiLaunch(
                    crate::wire::PrepareUiLaunch {
                        modal_scope: crate::wire::ModalScopeId::new("scope"),
                        root: crate::wire::MenuId::new("root"),
                        lease_millis: 1,
                    },
                ),
            })
            .unwrap(),
        );
        assert!(matches!(
            decoder().push(&illegal, |_| {}),
            Err(DecodeError::Semantic(_))
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_input_never_panics_or_waits_after_finish(input in prop::collection::vec(any::<u8>(), 0..8192)) {
            let mut decoder = decoder();
            let _ = decoder.push(&input, |_| {});
            let _ = decoder.finish();
        }
    }
}
