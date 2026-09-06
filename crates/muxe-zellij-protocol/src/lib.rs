//! Typed Zellij bridge payloads shared by the native adapter and the WASM bridge.
//!
//! This crate owns the JSON-lines pipe frames exchanged over the two persistent
//! `zellij pipe` children plus the generated Zellij API mirrors. It contains no
//! process spawning, no async runtime, and no transport state machines: those live
//! in `muxe-adapter-zellij`. The WASM bridge links the same generated conversion
//! and dispatch code so native and bridge can never disagree on shapes.
//!
//! Pipe traffic is versioned JSON control data, never `serde_json::Value` across
//! crate boundaries: every frame decodes into a typed [`pipe::PipeRequest`] or
//! [`pipe::PipeEvent`].

#![forbid(unsafe_code)]

pub mod compat;
pub mod generated;
pub mod pipe;
pub use compat::{
    bridge_protocol_fingerprint, generated_action_fingerprint, native_verified_artifact_sha256,
    pinned_source_revision, pinned_zellij_version,
};
pub use pipe::{
    BRIDGE_PROTOCOL_VERSION, BridgeArtifact, BridgeIdentity, BridgeRequest, BridgeTarget,
    CaptureEndReason, CaptureLostReason, CommandOutcome, CommandStatus, MAX_DETAIL_LEN,
    MAX_PIPE_LINE_LEN, NeighborDirection, PipeError, PipeEvent, PipeEventKind, PipeRequest,
    ZellijOrigin, decode_event_line, decode_request_line, encode_event_line, encode_request_line,
};
