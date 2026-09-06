use std::mem;

use rkyv::{rancor::Error as RkyvError, util::AlignedVec};
use thiserror::Error;

use crate::wire::{
    Codec, ConnectionPhase, MessageDirection, PeerRole, SchemaFingerprint, SemanticError,
    WireMessage, ArchivedWireMessage, MAX_FRAME_LEN, PROTOCOL_VERSION,
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
        rkyv::access::<ArchivedWireMessage, RkyvError>(self.bytes.as_slice())
            .map_err(archive_error)
    }

    pub fn deserialize(&self) -> Result<WireMessage, DecodeError> {
        let archived = self.archived()?;
        rkyv::deserialize::<WireMessage, RkyvError>(archived).map_err(archive_error)
    }
}

pub fn encode_frame(message: &WireMessage) -> Result<Vec<u8>, DecodeError> {
    let payload = rkyv::to_bytes::<RkyvError>(message).map_err(archive_error)?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| DecodeError::FrameTooLarge {
        declared: u32::MAX,
        maximum: MAX_FRAME_LEN,
    })?;
    if payload_len > MAX_FRAME_LEN {
        return Err(DecodeError::FrameTooLarge {
            declared: payload_len,
            maximum: MAX_FRAME_LEN,
        });
    }
    let mut frame = Vec::with_capacity(LENGTH_PREFIX_LEN + payload.len());
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(payload.as_slice());
    Ok(frame)
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

    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<ArchivedFrame>, DecodeError> {
        if self.failed {
            return Err(DecodeError::DecoderClosed);
        }

        let mut frames = Vec::new();
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
                    let message = match frame.deserialize() {
                        Ok(message) => message,
                        Err(error) => return self.fail(error),
                    };
                    let next_phase = match message.validate_for_peer(
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
                    frames.push(frame);
                }
                DecoderState::Failed => return self.fail(DecodeError::DecoderClosed),
            }
        }
        Ok(frames)
    }

    pub fn finish(&mut self) -> Result<(), DecodeError> {
        if self.failed {
            return Err(DecodeError::DecoderClosed);
        }
        let truncated = match &self.state {
            DecoderState::Prelude { filled, .. } => Some(TruncationStage::Prelude(*filled)),
            DecoderState::LengthPrefix { filled: 0, .. } if self.prelude.is_some() => None,
            DecoderState::LengthPrefix { filled, .. } => Some(TruncationStage::LengthPrefix(*filled)),
            DecoderState::Payload {
                declared_len, bytes, ..
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

    #[test]
    fn accepts_coalesced_hello_and_request() {
        let mut bytes = prelude();
        bytes.extend(encode_frame(&hello()).unwrap());
        bytes.extend(
            encode_frame(&WireMessage::Request {
                request_id: request_id(2),
                request: crate::wire::ClientRequest::Heartbeat,
            })
            .unwrap(),
        );

        let frames = decoder().push(&bytes).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].deserialize().unwrap(), hello());
    }

    #[test]
    fn accepts_every_source_alignment_after_copying_to_aligned_storage() {
        let mut stream = prelude();
        stream.extend(encode_frame(&hello()).unwrap());
        for offset in 0..32 {
            let mut source = vec![0; offset];
            source.extend_from_slice(&stream);
            let frames = decoder().push(&source[offset..]).unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].as_bytes().as_ptr() as usize % AlignedVec::<16>::ALIGNMENT, 0);
        }
    }

    #[test]
    fn rejects_oversized_length_before_collecting_payload() {
        let mut stream = prelude();
        stream.extend_from_slice(&(MAX_FRAME_LEN + 1).to_be_bytes());
        let mut decoder = decoder();
        assert!(matches!(decoder.push(&stream), Err(DecodeError::FrameTooLarge { .. })));
        assert_eq!(decoder.buffered_payload_len(), 0);
    }

    #[test]
    fn detects_every_prelude_and_prefix_truncation_boundary() {
        let prelude = prelude();
        for cutoff in 0..PRELUDE_LEN {
            let mut decoder = decoder();
            decoder.push(&prelude[..cutoff]).unwrap();
            assert!(matches!(decoder.finish(), Err(DecodeError::Truncated(_))));
        }
        let mut stream = prelude;
        stream.extend_from_slice(&[0, 0, 0, 4]);
        for cutoff in PRELUDE_LEN + 1..PRELUDE_LEN + LENGTH_PREFIX_LEN {
            let mut decoder = decoder();
            decoder.push(&stream[..cutoff]).unwrap();
            assert!(matches!(decoder.finish(), Err(DecodeError::Truncated(_))));
        }
    }

    #[test]
    fn rejects_invalid_archives_and_payload_length_mismatches() {
        let mut malformed = prelude();
        malformed.extend_from_slice(&4_u32.to_be_bytes());
        malformed.extend_from_slice(&[0, 0, 0, 0]);
        assert!(matches!(decoder().push(&malformed), Err(DecodeError::InvalidArchive(_))));

        let valid = encode_frame(&hello()).unwrap();
        let declared = u32::from_be_bytes(valid[..4].try_into().unwrap());
        let mut short = prelude();
        short.extend_from_slice(&(declared - 1).to_be_bytes());
        short.extend_from_slice(&valid[4..]);
        assert!(decoder().push(&short).is_err());

        let mut long = prelude();
        long.extend_from_slice(&(declared + 1).to_be_bytes());
        long.extend_from_slice(&valid[4..]);
        let mut decoder = decoder();
        decoder.push(&long).unwrap();
        assert!(matches!(decoder.finish(), Err(DecodeError::Truncated(_))));
    }

    #[test]
    fn rejects_wrong_role_and_schema_without_delivering_a_frame() {
        let mut wrong_role = Prelude::rkyv(PeerRole::Launcher, SchemaFingerprint::application())
            .encode()
            .to_vec();
        wrong_role.extend(encode_frame(&hello()).unwrap());
        assert!(matches!(
            decoder().push(&wrong_role),
            Err(DecodeError::PeerRoleMismatch { .. })
        ));

        let mut wrong_schema = Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::ZERO)
            .encode()
            .to_vec();
        wrong_schema.extend(encode_frame(&hello()).unwrap());
        assert!(matches!(
            decoder().push(&wrong_schema),
            Err(DecodeError::SchemaFingerprintMismatch)
        ));
    }

    #[test]
    fn rejects_illegal_role_specific_messages() {
        let mut stream = prelude();
        stream.extend(encode_frame(&hello()).unwrap());
        stream.extend(
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
        assert!(matches!(decoder().push(&stream), Err(DecodeError::Semantic(_))));
    }

    proptest! {
        #[test]
        fn arbitrary_input_never_panics_or_waits_after_finish(input in prop::collection::vec(any::<u8>(), 0..8192)) {
            let mut decoder = decoder();
            let _ = decoder.push(&input);
            let _ = decoder.finish();
        }
    }
}
