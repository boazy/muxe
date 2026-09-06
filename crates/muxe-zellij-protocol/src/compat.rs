//! Compatibility fingerprints for the Zellij integration record.
//!
//! The typed compatibility record compiled into the native binary carries the
//! pinned Zellij source revision, the generated-action fingerprint, and the
//! bridge protocol fingerprint. Semantic versions alone are insufficient for the
//! runtime handshake. There is intentionally no loaded-artifact digest here: the
//! pinned plugin SDK exposes no digest of its own loaded bytes, so none is
//! fabricated. See [`crate::pipe::BridgeArtifact`] for the honest attestation type.

use muxe_protocol::wire::SchemaFingerprint;
use sha2::{Digest, Sha256};

use crate::generated::{
    ACTION_CONVERTER_HOLES, ACTION_CONVERTERS, ACTION_VARIANTS, EXPECTED_ZELLIJ_REVISION,
    EXPECTED_ZELLIJ_VERSION, NATIVE_ZELLIJ_COMMAND_ARGUMENTS,
    NATIVE_ZELLIJ_COMMAND_CONVERTER_HOLES, NATIVE_ZELLIJ_COMMANDS, SOURCE_INPUT_SHA256,
    assert_pinned_revision,
};

/// Pinned Zellij source revision from `pins/zellij.toml`.
#[must_use]
pub fn pinned_source_revision() -> &'static str {
    assert_pinned_revision();
    EXPECTED_ZELLIJ_REVISION
}

/// Pinned Zellij host version.
#[must_use]
pub fn pinned_zellij_version() -> &'static str {
    assert_pinned_revision();
    EXPECTED_ZELLIJ_VERSION
}

fn fingerprint_for(profile: &[u8], parts: &[&[u8]]) -> SchemaFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(profile);
    for part in parts {
        hasher.update(*part);
    }
    SchemaFingerprint(hasher.finalize().into())
}

/// Fingerprint of the generated action/command surface.
///
/// Any generator, policy, or pinned-source change alters the hashed tables, so a
/// bridge built from different generated code fails the handshake instead of
/// misinterpreting payloads.
#[must_use]
pub fn generated_action_fingerprint() -> SchemaFingerprint {
    assert_pinned_revision();
    let mut parts: Vec<&[u8]> = Vec::new();
    for (path, digest) in SOURCE_INPUT_SHA256 {
        parts.push(path.as_bytes());
        parts.push(digest.as_bytes());
    }
    for (name, fields) in ACTION_VARIANTS {
        parts.push(name.as_bytes());
        for field in *fields {
            parts.push(field.as_bytes());
        }
    }
    for (name, class) in ACTION_CONVERTERS {
        parts.push(name.as_bytes());
        parts.push(class.as_bytes());
    }
    for hole in ACTION_CONVERTER_HOLES {
        parts.push(hole.as_bytes());
    }
    for (kebab, snake, returns, stage) in NATIVE_ZELLIJ_COMMANDS {
        parts.push(kebab.as_bytes());
        parts.push(snake.as_bytes());
        parts.push(returns.as_bytes());
        parts.push(stage.as_bytes());
    }
    for (command, field, ty, stage) in NATIVE_ZELLIJ_COMMAND_ARGUMENTS {
        parts.push(command.as_bytes());
        parts.push(field.as_bytes());
        parts.push(ty.as_bytes());
        parts.push(stage.as_bytes());
    }
    for (command, field, reason) in NATIVE_ZELLIJ_COMMAND_CONVERTER_HOLES {
        parts.push(command.as_bytes());
        parts.push(field.as_bytes());
        parts.push(reason.as_bytes());
    }
    fingerprint_for(b"muxe-zellij-actions/v1;", &parts)
}

/// Fingerprint of the pipe protocol schema: hashing the transport source makes
/// any pipe-shape edit change the handshake automatically.
#[must_use]
pub fn bridge_protocol_fingerprint() -> SchemaFingerprint {
    const PIPE_SCHEMA_SOURCE: &[u8] = include_bytes!("pipe.rs");
    const LIB_SCHEMA_SOURCE: &[u8] = include_bytes!("lib.rs");
    fingerprint_for(
        b"muxe-zellij-pipe/v1;",
        &[PIPE_SCHEMA_SOURCE, LIB_SCHEMA_SOURCE],
    )
}

/// SHA-256 of staged native bridge bytes, labeled exactly as what it is.
///
/// This helper measures bytes the native side staged for installation. It must
/// never be presented as a bridge-attested full-bytes digest: the bridge SDK
/// cannot attest its own loaded bytes.
#[must_use]
pub fn native_verified_artifact_sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_are_stable_and_nonzero() {
        let action = generated_action_fingerprint();
        assert_eq!(action, generated_action_fingerprint());
        assert_ne!(action.0, [0; 32]);
        let protocol = bridge_protocol_fingerprint();
        assert_eq!(protocol, bridge_protocol_fingerprint());
        assert_ne!(protocol.0, [0; 32]);
        assert_ne!(action.0, protocol.0);
    }

    #[test]
    fn pin_guards_hold() {
        assert_pinned_revision();
        assert_eq!(pinned_zellij_version(), "0.46.0");
    }

    #[test]
    fn native_artifact_hash_measures_bytes() {
        let digest = native_verified_artifact_sha256(b"wasm-bytes");
        assert_ne!(digest, [0; 32]);
        assert_ne!(digest, native_verified_artifact_sha256(b"other-bytes"));
    }
}
