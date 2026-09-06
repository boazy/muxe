//! Embedded compatibility record and native asset verification.
//!
//! Takes over the partial `compatibility` behavior previously inline in
//! `main.rs`: the target triple now comes from the build environment
//! (`MUXE_TARGET_TRIPLE`, derived from `TARGET` by `crates/muxe/build.rs`),
//! and the record is the typed `muxe_protocol::control::CompatibilityRecord`
//! shared with the activation handshake.
//!
//! # Blocked bridge digest
//!
//! The typed record intentionally carries no loaded-artifact digest: the host
//! API cannot attest the bytes it loaded, so a bridge self-reported artifact
//! hash is unavailable. That design decision is still awaiting the user
//! (DESIGN 2074 conflict) and this module finalizes no alternative schema.
//!
//! What *is* implemented here is one-sided native verification: the SHA-256
//! digest of the packaged `lib/muxe/muxe-zellij.wasm` bytes is checked at
//! install/activation time and recorded in the integration receipt and the
//! activation journal. The missing piece — a bridge-registration digest
//! reported back through the host channel — is represented explicitly as
//! [`BridgeRegistrationDigest::Blocked`], never as a fabricated value, and
//! release metadata must not be claimed complete while it is blocked.

use muxe_protocol::control::{CompatibilityRecord, HerdrCompatibility, ZellijCompatibility};
use serde_json::{Value, json};
use thiserror::Error;

/// Minimum-supported and latest-verified Zellij version for V1.
pub const ZELLIJ_MINIMUM: &str = "0.46.0";
/// Latest-verified Zellij version for V1.
pub const ZELLIJ_LATEST_VERIFIED: &str = "0.46.0";
/// Minimum-supported Herdr version for V1.
pub const HERDR_MINIMUM: &str = "0.8.2";
/// Latest-verified Herdr version for V1.
pub const HERDR_LATEST_VERIFIED: &str = "0.8.2";


#[derive(Debug, Error)]
pub enum CompatibilityError {
    #[error("Herdr bundled protocol {0} does not fit the compatibility record")]
    HerdrProtocolOutOfRange(u64),
    #[error("Herdr bundled schema version {0} does not fit the compatibility record")]
    HerdrSchemaOutOfRange(u64),
    #[error("Herdr bundled request-schema digest is not a valid non-zero 32-byte hash")]
    HerdrDigestMalformed,
}
/// Decodes the generator-produced Herdr request-schema digest into a typed fingerprint.
///
/// The digest is source-derived from `muxe-adapter-herdr`'s generated schema constants,
/// never a hardcoded fingerprint: any schema regeneration changes the value automatically.
fn herdr_schema_fingerprint() -> Result<muxe_protocol::SchemaFingerprint, CompatibilityError> {
    const DIGEST: &str = muxe_adapter_herdr::generated::BUNDLED_REQUEST_SCHEMA_SHA256;
    if DIGEST.len() != 64 || !DIGEST.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CompatibilityError::HerdrDigestMalformed);
    }
    let mut raw = [0u8; 32];
    for (index, chunk) in DIGEST.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| CompatibilityError::HerdrDigestMalformed)?;
        raw[index] = u8::from_str_radix(text, 16).map_err(|_| CompatibilityError::HerdrDigestMalformed)?;
    }
    let fingerprint = muxe_protocol::SchemaFingerprint(raw);
    if fingerprint.is_zero() {
        return Err(CompatibilityError::HerdrDigestMalformed);
    }
    Ok(fingerprint)
}

/// Builds the embedded compatibility record for this executable.
///
/// All fingerprints are computed from versioned sources, never hardcoded: the
/// Zellij action and bridge-protocol fingerprints come from
/// `muxe-zellij-protocol`'s generator-derived tables, and the Herdr schema
/// fingerprint decodes the generator-produced digest. The coordinator never
/// bypasses a production mismatch: broker-side comparison treats any
/// divergence as unverifiable, and any future extension of the central
/// protocol record is additive and coordinated with the broker owner.
pub fn embedded_record() -> Result<CompatibilityRecord, CompatibilityError> {
    let protocol = muxe_adapter_herdr::generated::BUNDLED_PROTOCOL;
    let schema_version = muxe_adapter_herdr::generated::BUNDLED_SCHEMA_VERSION;
    let schema_fingerprint = herdr_schema_fingerprint()?;
    Ok(CompatibilityRecord {
        muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
        target_triple: env!("MUXE_TARGET_TRIPLE").to_owned(),
        application_schema_fingerprint: muxe_protocol::SchemaFingerprint::application(),
        zellij: Some(ZellijCompatibility {
            source_revision: muxe_zellij_protocol::compat::pinned_source_revision().to_owned(),
            generated_action_fingerprint: muxe_zellij_protocol::compat::generated_action_fingerprint(
            ),
            bridge_protocol_fingerprint: muxe_zellij_protocol::compat::bridge_protocol_fingerprint(
            ),
        }),
        herdr: Some(HerdrCompatibility {
            protocol_version: u32::try_from(protocol)
                .map_err(|_| CompatibilityError::HerdrProtocolOutOfRange(protocol))?,
            schema_version: u32::try_from(schema_version)
                .map_err(|_| CompatibilityError::HerdrSchemaOutOfRange(schema_version))?,
            schema_fingerprint,
        }),
    })
}

/// The state of the bridge-registration digest.
///
/// `Blocked` names the missing prerequisite: the host channel has no way for
/// the running bridge to attest its own bytes, so no complete release
/// metadata can be claimed until the DESIGN 2074 decision lands.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BridgeRegistrationDigest {
    Blocked { reason: &'static str },
}

impl BridgeRegistrationDigest {
    /// The current state: blocked on the user-approved digest design.
    #[must_use]
    pub const fn current() -> Self {
        Self::Blocked {
            reason: "bridge cannot self-report its artifact hash through the host API; awaiting digest design decision",
        }
    }
}

/// Native-side verification of the packaged bridge bytes.
///
/// This is one half of the digest story: it proves the bytes being installed
/// match the release's recorded digest. It says nothing about which bytes a
/// live host actually loaded; that half remains [`BridgeRegistrationDigest::Blocked`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeAssetVerification {
    /// SHA-256 of the packaged `lib/muxe/muxe-zellij.wasm` bytes.
    pub packaged_digest: String,
    /// Whether the packaged bytes matched the expected release digest.
    pub matches_expected: bool,
    /// Registration half: always blocked, never fabricated.
    pub registration: BridgeRegistrationDigest,
}

#[derive(Debug, Error)]
pub enum AssetVerificationError {
    #[error("packaged bridge digest mismatch: expected {expected}, found {found}")]
    DigestMismatch { expected: String, found: String },
}

/// Encodes bytes as lowercase hexadecimal without an extra dependency.
fn hex_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(ALPHABET[(byte >> 4) as usize] as char);
        out.push(ALPHABET[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn verify_packaged_asset(
    packaged_bytes: &[u8],
    expected_digest: &str,
) -> Result<NativeAssetVerification, AssetVerificationError> {
    let found = crate::fsutil::sha256_hex(packaged_bytes);
    if found != expected_digest {
        return Err(AssetVerificationError::DigestMismatch {
            expected: expected_digest.to_owned(),
            found,
        });
    }
    Ok(NativeAssetVerification {
        packaged_digest: found,
        matches_expected: true,
        registration: BridgeRegistrationDigest::current(),
    })
}

/// Renders the human-readable compatibility report.
#[must_use]
pub fn render_human(record: &CompatibilityRecord) -> String {
    let mut lines = vec![
        format!("Muxe {} ({})", record.muxe_version, record.target_triple),
        format!("protocol {}", muxe_protocol::PROTOCOL_VERSION),
        format!("Zellij: {ZELLIJ_MINIMUM} minimum / {ZELLIJ_LATEST_VERIFIED} latest verified",),
        format!("Herdr: {HERDR_MINIMUM} minimum / {HERDR_LATEST_VERIFIED} latest verified",),
    ];
    match &record.zellij {
        Some(zellij) => lines.push(format!(
            "Zellij source revision {} (fingerprints embedded)",
            zellij.source_revision
        )),
        None => lines.push(
            "Zellij action/bridge fingerprints: unavailable (no verified adapter source in this build)"
                .to_owned(),
        ),
    }
    lines.join("\n") + "\n"
}

/// Renders the stable snake_case JSON compatibility report.
///
/// Field names are stable: `muxe_version`, `target_triple`,
/// `application_schema_fingerprint`, `zellij`, `herdr`, plus the
/// `bridge_registration_digest` marker which is `null` with a `blocked_reason`
/// while the digest design decision is pending.
#[must_use]
pub fn render_json(record: &CompatibilityRecord) -> Value {
    let fingerprint_hex = hex_lower(&record.application_schema_fingerprint.0);
    let zellij = record.zellij.as_ref().map(|zellij| {
        json!({
            "minimum": ZELLIJ_MINIMUM,
            "latest_verified": ZELLIJ_LATEST_VERIFIED,
            "source_revision": zellij.source_revision,
            "generated_action_fingerprint": hex_lower(&zellij.generated_action_fingerprint.0),
            "bridge_protocol_fingerprint": hex_lower(&zellij.bridge_protocol_fingerprint.0),
        })
    });
    json!({
        "muxe_version": record.muxe_version,
        "target_triple": record.target_triple,
        "protocol_version": muxe_protocol::PROTOCOL_VERSION,
        "application_schema_fingerprint": fingerprint_hex,
        "hosts": {
            "zellij": zellij,
            "herdr": {
                "minimum": HERDR_MINIMUM,
                "latest_verified": HERDR_LATEST_VERIFIED,
                "protocol": muxe_adapter_herdr::generated::BUNDLED_PROTOCOL,
                "schema_version": muxe_adapter_herdr::generated::BUNDLED_SCHEMA_VERSION,
                "raw_schema_sha256": muxe_adapter_herdr::generated::BUNDLED_RAW_SCHEMA_SHA256,
                "request_schema_sha256": muxe_adapter_herdr::generated::BUNDLED_REQUEST_SCHEMA_SHA256,
            }
        },
        "bridge_registration_digest": null,
        "bridge_registration_blocked_reason": match BridgeRegistrationDigest::current() {
            BridgeRegistrationDigest::Blocked { reason } => reason,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn packaged_verification_fails_closed_on_mismatch() {
        let error = verify_packaged_asset(b"wasm-bytes", &"0".repeat(64)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("digest mismatch"));
    }

}
