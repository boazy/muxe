//! Embedded compatibility record and native WASM asset verification.
//!
//! The native record carries the shared pre-link bridge build identity for
//! registration compatibility. The separately producer-provided
//! `wasm_sha256` remains the trusted local package identity; the pinned host
//! API cannot attest the bytes it loaded.

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
    #[error("build supplied an invalid native packaged-WASM SHA-256")]
    PackagedWasmDigestMalformed,
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
        let text =
            std::str::from_utf8(chunk).map_err(|_| CompatibilityError::HerdrDigestMalformed)?;
        raw[index] =
            u8::from_str_radix(text, 16).map_err(|_| CompatibilityError::HerdrDigestMalformed)?;
    }
    let fingerprint = muxe_protocol::SchemaFingerprint(raw);
    if fingerprint.is_zero() {
        return Err(CompatibilityError::HerdrDigestMalformed);
    }
    Ok(fingerprint)
}

/// Native package identity for the bridge bytes distributed with this binary.
///
/// This proves only the local package-to-install relationship. The separate
/// `bridge_build_id` identifies expected bridge registration compatibility.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PackagedWasmArtifact {
    Verified { sha256: [u8; 32] },
    Unavailable { reason: &'static str },
}

/// Complete compatibility material embedded in the native executable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCompatibilityRecord {
    /// Cross-process compatibility used by broker handoffs.
    pub handoff: CompatibilityRecord,
    /// Native-report-only exercised Herdr surface, borrowed from the
    /// adapter's authoritative constant. Never part of the handoff schema,
    /// so omission stays valid for old v1 records.
    pub herdr_verified_methods: &'static [&'static str],
    /// Producer-attested identity of the bridge package distributed beside us.
    pub packaged_wasm: PackagedWasmArtifact,
}

fn packaged_wasm_artifact() -> Result<PackagedWasmArtifact, CompatibilityError> {
    const DIGEST: &str = env!("MUXE_WASM_SHA256");
    if DIGEST == "unavailable" {
        return Ok(PackagedWasmArtifact::Unavailable {
            reason: "no packaged bridge digest was supplied for this development build",
        });
    }
    if DIGEST.len() != 64
        || !DIGEST
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CompatibilityError::PackagedWasmDigestMalformed);
    }
    let mut sha256 = [0_u8; 32];
    for (index, chunk) in DIGEST.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk)
            .map_err(|_| CompatibilityError::PackagedWasmDigestMalformed)?;
        sha256[index] = u8::from_str_radix(text, 16)
            .map_err(|_| CompatibilityError::PackagedWasmDigestMalformed)?;
    }
    if sha256.iter().all(|byte| *byte == 0) {
        return Err(CompatibilityError::PackagedWasmDigestMalformed);
    }
    Ok(PackagedWasmArtifact::Verified { sha256 })
}

/// Builds the complete native compatibility record for this executable.
///
/// The handoff fields come from versioned sources. The packaged bridge digest
/// is producer-provided at build time, never derived from install input.
///
/// # Errors
///
/// Fails when the embedded producer digest is malformed, a bundled protocol
/// or schema version overflows its handoff field, or the schema fingerprint
/// cannot be derived from the generated constants.
pub fn embedded_record() -> Result<NativeCompatibilityRecord, CompatibilityError> {
    let protocol = muxe_adapter_herdr::generated::BUNDLED_PROTOCOL;
    let schema_version = muxe_adapter_herdr::generated::BUNDLED_SCHEMA_VERSION;
    let schema_fingerprint = herdr_schema_fingerprint()?;
    Ok(NativeCompatibilityRecord {
        handoff: CompatibilityRecord {
            muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
            target_triple: env!("MUXE_TARGET_TRIPLE").to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint::application(),
            zellij: Some(ZellijCompatibility {
                source_revision: muxe_zellij_protocol::compat::pinned_source_revision().to_owned(),
                generated_action_fingerprint:
                    muxe_zellij_protocol::compat::generated_action_fingerprint(),
                bridge_protocol_fingerprint:
                    muxe_zellij_protocol::compat::bridge_protocol_fingerprint(),
                bridge_build_id: Some(muxe_zellij_protocol::compat::bridge_build_id()),
            }),
            herdr: Some(HerdrCompatibility {
                protocol_version: u32::try_from(protocol)
                    .map_err(|_| CompatibilityError::HerdrProtocolOutOfRange(protocol))?,
                schema_version: u32::try_from(schema_version)
                    .map_err(|_| CompatibilityError::HerdrSchemaOutOfRange(schema_version))?,
                schema_fingerprint,
            }),
        },
        herdr_verified_methods: muxe_adapter_herdr::VERIFIED_HERDR_METHODS,
        packaged_wasm: packaged_wasm_artifact()?,
    })
}

/// Native-side verification of the packaged WASM bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeAssetVerification {
    /// SHA-256 of the packaged `lib/muxe/muxe-zellij.wasm` bytes.
    pub packaged_digest: String,
}

#[derive(Debug, Error)]
pub enum AssetVerificationError {
    #[error("native packaged bridge is unavailable: {reason}")]
    Unavailable { reason: &'static str },
    #[error("embedded native compatibility is invalid: {0}")]
    Embedded(#[from] CompatibilityError),
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

/// Verifies candidate packaged bytes against this binary's embedded producer digest.
///
/// # Errors
///
/// Returns an error when this development build has no packaged bridge or the
/// candidate bytes do not match the producer-provided embedded digest.
pub fn verify_packaged_asset(
    packaged_bytes: &[u8],
) -> Result<NativeAssetVerification, AssetVerificationError> {
    let expected = match packaged_wasm_artifact()? {
        PackagedWasmArtifact::Verified { sha256 } => hex_lower(&sha256),
        PackagedWasmArtifact::Unavailable { reason } => {
            return Err(AssetVerificationError::Unavailable { reason });
        }
    };
    let found = crate::fsutil::sha256_hex(packaged_bytes);
    if found != expected {
        return Err(AssetVerificationError::DigestMismatch { expected, found });
    }
    Ok(NativeAssetVerification {
        packaged_digest: found,
    })
}

/// Renders the human-readable compatibility report.
#[must_use]
pub fn render_human(record: &NativeCompatibilityRecord) -> String {
    let handoff = &record.handoff;
    let mut lines = vec![
        format!("Muxe {} ({})", handoff.muxe_version, handoff.target_triple),
        format!("protocol {}", muxe_protocol::PROTOCOL_VERSION),
        format!("Zellij: {ZELLIJ_MINIMUM} minimum / {ZELLIJ_LATEST_VERIFIED} latest verified",),
        format!("Herdr: {HERDR_MINIMUM} minimum / {HERDR_LATEST_VERIFIED} latest verified",),
    ];
    match &handoff.herdr {
        Some(herdr) => {
            lines.push(format!(
                "Herdr protocol {} schema {} (fingerprint {})",
                herdr.protocol_version,
                herdr.schema_version,
                hex_lower(&herdr.schema_fingerprint.0)
            ));
            lines.push(format!(
                "Herdr verified methods ({}): {}",
                record.herdr_verified_methods.len(),
                record.herdr_verified_methods.join(", ")
            ));
        }
        None => lines.push(
            "Herdr fingerprints: unavailable (no verified adapter source in this build)".to_owned(),
        ),
    }
    match &handoff.zellij {
        Some(zellij) => lines.push(format!(
            "Zellij source revision {} (fingerprints embedded)",
            zellij.source_revision
        )),
        None => lines.push(
            "Zellij action/bridge fingerprints: unavailable (no verified adapter source in this build)"
                .to_owned(),
        ),
    }
    match &record.packaged_wasm {
        PackagedWasmArtifact::Verified { sha256 } => {
            lines.push(format!(
                "Packaged Zellij bridge SHA-256 {}",
                hex_lower(sha256)
            ));
        }
        PackagedWasmArtifact::Unavailable { reason } => {
            lines.push(format!("Packaged Zellij bridge unavailable: {reason}"));
        }
    }
    lines.join("\n") + "\n"
}

/// Renders the stable `snake_case` JSON compatibility report.
///
/// `packaged_wasm.sha256` is the producer-provided identity of local bridge
/// bytes. `hosts.zellij.bridge_build_id` is the shared pre-link compatibility
/// identity; it is not a loaded-byte attestation.
#[must_use]
pub fn render_json(record: &NativeCompatibilityRecord) -> Value {
    let handoff = &record.handoff;
    let fingerprint_hex = hex_lower(&handoff.application_schema_fingerprint.0);
    let zellij = handoff.zellij.as_ref().map(|zellij| {
        json!({
            "minimum": ZELLIJ_MINIMUM,
            "latest_verified": ZELLIJ_LATEST_VERIFIED,
            "source_revision": zellij.source_revision,
            "generated_action_fingerprint": hex_lower(&zellij.generated_action_fingerprint.0),
            "bridge_protocol_fingerprint": hex_lower(&zellij.bridge_protocol_fingerprint.0),
            "bridge_build_id": zellij.bridge_build_id.map(|id| hex_lower(&id.0)),
        })
    });
    let herdr = handoff.herdr.as_ref().map(|herdr| {
        json!({
            "minimum": HERDR_MINIMUM,
            "latest_verified": HERDR_LATEST_VERIFIED,
            "protocol": herdr.protocol_version,
            "schema_version": herdr.schema_version,
            "schema_fingerprint": hex_lower(&herdr.schema_fingerprint.0),
            "raw_schema_sha256": muxe_adapter_herdr::generated::BUNDLED_RAW_SCHEMA_SHA256,
            "request_schema_sha256": muxe_adapter_herdr::generated::BUNDLED_REQUEST_SCHEMA_SHA256,
            "verified_methods": record.herdr_verified_methods,
        })
    });
    let packaged_wasm = match &record.packaged_wasm {
        PackagedWasmArtifact::Verified { sha256 } => json!({
            "sha256": hex_lower(sha256),
            "unavailable_reason": Value::Null,
        }),
        PackagedWasmArtifact::Unavailable { reason } => json!({
            "sha256": Value::Null,
            "unavailable_reason": reason,
        }),
    };
    json!({
        "muxe_version": handoff.muxe_version,
        "target_triple": handoff.target_triple,
        "protocol_version": muxe_protocol::PROTOCOL_VERSION,
        "application_schema_fingerprint": fingerprint_hex,
        "hosts": {
            "zellij": zellij,
            "herdr": herdr,
        },
        "packaged_wasm": packaged_wasm,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn development_build_explicitly_names_the_missing_packaged_bridge() {
        // Only meaningful without a producer digest; the ignored artifact
        // proof below covers the digest-embedded build.
        if env!("MUXE_WASM_SHA256") != "unavailable" {
            return;
        }
        let record = embedded_record().expect("development compatibility record is valid");
        assert!(matches!(
            record.packaged_wasm,
            PackagedWasmArtifact::Unavailable { .. }
        ));
    }

    /// Real assembled-binary artifact proof against the producer WASM.
    ///
    /// Ignored in ordinary runs so Herdr-only development keeps the typed
    /// `Unavailable` behavior. Run with the verified producer digest:
    /// `mise run verify-packaged-wasm`. The expected identity comes only
    /// from this binary's embedded producer digest, never from the bytes.
    #[test]
    #[ignore = "requires MUXE_WASM_SHA256 from the verified staged bridge at compile time"]
    fn packaged_wasm_matches_producer_bytes_and_rejects_tampering() {
        let record = embedded_record().expect("digest-embedded compatibility record is valid");
        let PackagedWasmArtifact::Verified { sha256 } = record.packaged_wasm else {
            panic!(
                "artifact proof requires MUXE_WASM_SHA256 at compile time; use mise run verify-packaged-wasm"
            );
        };
        let wasm_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../dist-wasm/muxe-zellij.wasm");
        let bytes = std::fs::read(&wasm_path)
            .unwrap_or_else(|_| panic!("missing producer bridge at {}", wasm_path.display()));
        let verified = verify_packaged_asset(&bytes).expect("producer bytes verify");
        assert_eq!(verified.packaged_digest, hex_lower(&sha256));
        let rendered = render_json(&record);
        assert_eq!(
            rendered["packaged_wasm"]["sha256"],
            json!(verified.packaged_digest)
        );
        assert_eq!(
            rendered["hosts"]["zellij"]["bridge_build_id"],
            muxe_zellij_protocol::compat::bridge_build_id_hex(),
        );
        let mut tampered = bytes;
        tampered[64] ^= 0x01;
        assert!(matches!(
            verify_packaged_asset(&tampered),
            Err(AssetVerificationError::DigestMismatch { .. })
        ));
    }
}
