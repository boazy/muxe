//! Native broker-side Zellij adapter.
//!
//! The adapter owns the two persistent `zellij pipe` children for one live
//! Zellij session, per-client bridge registrations, Locked-mode capture leases,
//! origin-context capture, and typed native-action correlation. It implements
//! the async [`HostAdapter`](muxe_adapter_api::HostAdapter) contract; the WASM
//! bridge on each client performs the actual host calls.
//!
//! Configuration input arrives as [`NativeActionCandidate`](muxe_core::NativeActionCandidate)
//! values in two namespaces: `native.zellij.action:{kebab-action}` for the
//! low-level action surface dispatched through `run_action`, and
//! `native.zellij.command:{kebab-command}` for the 153 exposed high-level plugin
//! commands. Both parse into generated raw mirrors and validate through
//! generated `TryFrom` conversions; pipe payloads are typed throughout with no
//! JSON value bridge between crates.

#![forbid(unsafe_code)]

mod adapter;
mod capture;
mod keyboard;
mod launch;
mod names;
mod origin;
mod parse;
mod pipes;
mod portable;
mod registry;
mod validation;

pub use adapter::{ZellijAdapter, ZellijAdapterConfig};
pub use keyboard::{KeyboardError, MappedKey, map_canonical_key};
pub use launch::{
    LaunchError, LaunchKind, LaunchTarget, SizeSpec, ZellijPaneLaunch, ZellijPlacement,
    ZellijSplitDirection, build_launch_command, parse_position, parse_size,
};
pub use names::{
    ACTION_NAMESPACE, COMMAND_NAMESPACE, NativeType, action_kebab_to_variant,
    is_exposed_command, is_zellij_native_type, parse_native_type,
};
pub use origin::{OriginError, build_origin_context, prior_pane_id};
pub use parse::{ParseError, candidate_to_raw, fields_to_json_map};
pub use pipes::{
    PipeChannel, PipeTransportError, RELEASE_TIMEOUT, SubprocessChannel, channel_names,
};
pub use portable::{
    Cardinal, FocusRequest, PortableError, PortableMapping, map_portable, to_validated_action,
    validate_portable_structure,
};
pub use registry::{BridgeRecord, HEARTBEAT_LEASE, RegistryError, ZellijRegistry};
pub use validation::{ZellijValidator, check_keyboard_profile};
