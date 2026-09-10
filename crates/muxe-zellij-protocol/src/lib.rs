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
pub mod ids;
pub mod pipe;
pub use compat::{
    bridge_build_id, bridge_build_id_hex, bridge_protocol_fingerprint,
    generated_action_fingerprint, pinned_source_revision, pinned_zellij_version,
};
pub use ids::{ChannelGeneration, ProtocolScalarError, ProtocolVersion, RegistrationId, RequestId};
pub use pipe::{
    BRIDGE_PROTOCOL_VERSION, BridgeIdentity, BridgeRequest, BridgeTarget, CaptureEndReason,
    CaptureLostReason, CommandOutcome, CommandStatus, EventSubscription, MAX_DETAIL_LEN,
    MAX_PIPE_LINE_LEN, NeighborDirection, PipeError, PipeEvent, PipeEventKind, PipeRequest,
    ZellijOrigin, decode_event_line, decode_event_subscription, decode_request_line,
    encode_event_line, encode_event_subscription, encode_request_line,
};

/// Authoritative Zellij permission contract for the managed WASM bridge.
///
/// Every permission required by the exposed command set plus the bridge
/// lifecycle operations, from the pinned permission map
/// (`zellij-server/src/plugins/zellij_exports.rs`, `check_command_permission`).
/// DESIGN requires the plugin to request the whole exposed set even when the
/// effective configuration references only some of its commands, so this is a
/// fixed upfront superset: never trim it to a basic-menu minimum and never
/// extend it with functional scope. Only membership is contractual; the
/// listing order is preserved unchanged from the original bridge request.
/// Writing this list grants nothing by itself; the host prompts the user.
///
/// The canonical wire name of each entry is its variant name via [`ToString`]
/// (the pinned `PermissionType` derives `Display` plus `EnumString`), not the
/// human-label `display_name`.
pub const BRIDGE_PERMISSIONS: [zellij_utils::data::PermissionType; 11] = [
    zellij_utils::data::PermissionType::ReadApplicationState,
    zellij_utils::data::PermissionType::ChangeApplicationState,
    zellij_utils::data::PermissionType::RunActionsAsUser,
    zellij_utils::data::PermissionType::OpenFiles,
    zellij_utils::data::PermissionType::OpenTerminalsOrPlugins,
    zellij_utils::data::PermissionType::RunCommands,
    zellij_utils::data::PermissionType::WriteToStdin,
    zellij_utils::data::PermissionType::WriteToClipboard,
    zellij_utils::data::PermissionType::Reconfigure,
    zellij_utils::data::PermissionType::FullHdAccess,
    zellij_utils::data::PermissionType::ReadCliPipes,
];
