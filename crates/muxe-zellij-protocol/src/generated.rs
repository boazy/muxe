//! Checked-in generated Zellij API mirrors for the exact pin.
//!
//! The inventory is produced deterministically by `tools/muxe-zellij-gen` from the
//! minimized source corpus under `fixtures/zellij/0.46.0/source/` and checked in at
//! `fixtures/zellij/0.46.0/action-inventory.rs`. This module reuses that output
//! verbatim: raw mirrors deserialize pipe/config input, `TryFrom` validates raw
//! into typed models, and `dispatch_native_command` converts validated models
//! directly into upstream Zellij types without any JSON value bridge.
//!
//! Native code links this module for parsing and validation. Only the WASM bridge
//! calls `dispatch_native_command`; the native adapter never invokes host shims.

#![expect(
    clippy::allow_attributes,
    clippy::allow_attributes_without_reason,
    clippy::large_enum_variant,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::needless_question_mark,
    clippy::pub_underscore_fields,
    clippy::redundant_closure,
    clippy::redundant_field_names,
    clippy::redundant_locals,
    clippy::struct_excessive_bools,
    clippy::too_many_lines,
    clippy::unit_arg,
    clippy::used_underscore_binding,
    reason = "the pinned Zellij inventory is mechanically generated and included verbatim; regenerate it rather than hand-editing syntax"
)]

include!("../../../fixtures/zellij/0.46.0/action-inventory.rs");

/// Compile-time-adjacent pin guard: the checked-in inventory must name the exact
/// recorded revision. Evaluated by tests and by [`assert_pinned_revision`].
pub const EXPECTED_ZELLIJ_REVISION: &str = "af38660c5884f50bb3726682fb92961326c4268f";
/// Pinned host version matching `pins/zellij.toml`.
pub const EXPECTED_ZELLIJ_VERSION: &str = "0.46.0";
/// Exposed (user-dispatchable) shim commands in the v1 surface.
pub const EXPECTED_EXPOSED_COMMANDS: usize = 153;

/// Fails the build's test gate (and documents the contract here) when the
/// checked-in inventory drifts from the recorded pin.
///
/// # Panics
///
/// Panics when the recorded revision, version, exposed command count, or
/// generated conversion coverage no longer matches the checked-in inventory.
pub fn assert_pinned_revision() {
    assert_eq!(
        PINNED_ZELLIJ_REVISION, EXPECTED_ZELLIJ_REVISION,
        "generated Zellij inventory revision drift"
    );
    assert_eq!(
        PINNED_ZELLIJ_VERSION, EXPECTED_ZELLIJ_VERSION,
        "generated Zellij inventory version drift"
    );
    assert_eq!(
        NATIVE_ZELLIJ_COMMANDS.len(),
        EXPECTED_EXPOSED_COMMANDS,
        "exposed Zellij command surface changed; regenerate and reclassify"
    );
    assert!(
        NATIVE_ZELLIJ_COMMAND_CONVERTER_HOLES.is_empty(),
        "unconverted exposed Zellij commands must fail closed, never ship"
    );
    assert!(
        ACTION_CONVERTER_HOLES.is_empty(),
        "unconverted Zellij action types must fail closed, never ship"
    );
}
