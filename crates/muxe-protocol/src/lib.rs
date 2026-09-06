//! Versioned broker IPC framing and typed wire contracts.
//!
//! This crate deliberately owns only transport representations and validation. It has no
//! dependency on `muxe-core`, broker runtime, UI implementation, or concrete host adapter.

#![forbid(unsafe_code)]

pub mod control;
pub mod frame;
pub mod wire;

pub use control::{
    ControlDecoder, ControlMessage, ControlOperation, ControlRequest, ControlResponse,
    HandoffId,
};
pub use frame::{
    encode_frame, ArchivedFrame, ConnectionDecoder, ConnectionPolicy, DecodeError, Prelude,
    PRELUDE_LEN,
};
pub use wire::*;
