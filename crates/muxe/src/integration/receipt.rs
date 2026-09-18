//! Owner-only Zellij integration receipt.
//!
//! The receipt records exactly what Muxe owns: the installed bridge digest and
//! version, plus per-node KDL ownership (created, updated with previous text,
//! or merely observed). It never contains a complete Zellij configuration,
//! Muxe configuration values, environment values, or action payloads.

use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{BRIDGE_FILE_NAME, kdl};
use crate::fsutil::{self, FsError};

/// Current receipt schema version.
pub const RECEIPT_SCHEMA_VERSION: u32 = 1;
/// Receipt file name inside the integration directory.
pub const RECEIPT_FILE_NAME: &str = "receipt.json";

/// A validated SHA-256 digest in its canonical lowercase hexadecimal form.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    /// Calculates the digest for trusted bytes.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(fsutil::sha256_hex(bytes))
    }

    /// Parses a persisted canonical SHA-256 digest.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidSha256Digest`] when `value` is not exactly 64 lowercase
    /// hexadecimal characters.
    pub fn parse(value: String) -> Result<Self, InvalidSha256Digest> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(InvalidSha256Digest);
        }
        Ok(Self(value))
    }

    /// Returns the digest at a raw comparison or serialization boundary.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for Sha256Digest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A non-canonical or malformed SHA-256 digest.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("must be 64 lowercase hexadecimal characters")]
pub struct InvalidSha256Digest;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedNode {
    /// `plugins { muxe location="..." }`.
    PluginsAlias,
    /// `load_plugins { muxe }`.
    LoadPluginsEntry,
}

impl ManagedNode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PluginsAlias => "plugins.muxe",
            Self::LoadPluginsEntry => "load_plugins.muxe",
        }
    }
}

/// How the installer treated a node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Disposition {
    /// Muxe created the node; uninstall may remove it under unchanged checks.
    Created,
    /// Muxe replaced a pre-existing node; uninstall may restore the previous
    /// text under unchanged checks. Previous text is retained only for nodes
    /// the user explicitly allowed Muxe to replace.
    Updated,
    /// A correct pre-existing node Muxe never claimed; uninstall never touches it.
    Observed,
}

/// The installed bridge record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BridgeRecord {
    /// Canonical stable bridge path.
    pub canonical_path: PathBuf,
    /// Installed Muxe version.
    pub installed_version: String,
    /// SHA-256 digest of the installed bytes.
    pub installed_digest: Sha256Digest,
    /// Digest of the replaced bridge, retained as the rollback copy.
    pub previous_digest: Option<Sha256Digest>,
    /// Bridge compatibility at install time (Zellij source revision and
    /// generated-action/bridge-protocol fingerprints, central types).
    pub bridge_compat: Option<muxe_protocol::control::ZellijCompatibility>,
}

/// Ownership record for one managed KDL node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeRecord {
    /// Which Zellij configuration file holds the node.
    pub config_path: PathBuf,
    /// Which managed node this record covers.
    pub node: ManagedNode,
    /// How the installer treated the node.
    pub disposition: Disposition,
    /// Canonical semantic representation of the installed node.
    pub semantic: String,
    /// SHA-256 digest of the exact installed node text.
    pub text_digest: Sha256Digest,
    /// Exact previous node text (only for `Updated` nodes).
    pub previous_text: Option<String>,
    /// Semantic representation of the previous node (only for `Updated` nodes).
    pub previous_semantic: Option<String>,
}

/// The versioned integration receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Receipt {
    pub schema_version: u32,
    pub bridge: BridgeRecord,
    pub configs: Vec<NodeRecord>,
}

impl Receipt {
    fn from_persisted(raw: RawReceipt, directory: &Path) -> Result<Self, String> {
        let receipt = Self {
            schema_version: raw.schema_version,
            bridge: BridgeRecord {
                canonical_path: raw.bridge.canonical_path,
                installed_version: raw.bridge.installed_version,
                installed_digest: parse_digest(
                    "bridge.installed_digest",
                    raw.bridge.installed_digest,
                )?,
                previous_digest: raw
                    .bridge
                    .previous_digest
                    .map(|digest| parse_digest("bridge.previous_digest", digest))
                    .transpose()?,
                bridge_compat: raw.bridge.bridge_compat,
            },
            configs: raw
                .configs
                .into_iter()
                .enumerate()
                .map(|(index, record)| {
                    Ok(NodeRecord {
                        config_path: record.config_path,
                        node: record.node,
                        disposition: record.disposition,
                        semantic: record.semantic,
                        text_digest: parse_digest(
                            &format!("configs[{index}].text_digest"),
                            record.text_digest,
                        )?,
                        previous_text: record.previous_text,
                        previous_semantic: record.previous_semantic,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
        };
        receipt.validate(directory)?;
        Ok(receipt)
    }

    fn validate(&self, directory: &Path) -> Result<(), String> {
        let expected_bridge = directory.join(BRIDGE_FILE_NAME);
        if self.bridge.canonical_path != expected_bridge {
            return Err(format!(
                "bridge.canonical_path {} does not name the receipt-owned bridge {}",
                self.bridge.canonical_path.display(),
                expected_bridge.display()
            ));
        }

        let mut records = HashSet::new();
        for record in &self.configs {
            if !records.insert((&record.config_path, record.node)) {
                return Err(format!(
                    "duplicate ownership record for {} in {}",
                    record.node.as_str(),
                    record.config_path.display()
                ));
            }
            match (
                record.disposition,
                &record.previous_text,
                &record.previous_semantic,
            ) {
                (Disposition::Updated, Some(previous_text), Some(previous_semantic)) => {
                    if previous_semantic.trim().is_empty() {
                        return Err(format!(
                            "{} updated provenance has empty previous semantic state",
                            record.node.as_str()
                        ));
                    }
                    let canonical = kdl::validate_previous_node(record.node, previous_text)
                        .map_err(|detail| {
                            format!(
                                "{} updated provenance has invalid previous text: {detail}",
                                record.node.as_str()
                            )
                        })?;
                    if canonical != *previous_semantic {
                        return Err(format!(
                            "{} updated provenance previous semantic state does not match previous text",
                            record.node.as_str()
                        ));
                    }
                }
                (Disposition::Updated, _, _) => {
                    return Err(format!(
                        "{} updated provenance lacks complete previous text and semantic state",
                        record.node.as_str()
                    ));
                }
                (Disposition::Created | Disposition::Observed, None, None) => {}
                (Disposition::Created | Disposition::Observed, _, _) => {
                    return Err(format!(
                        "{} {:?} provenance claims previous updated state",
                        record.node.as_str(),
                        record.disposition
                    ));
                }
            }
        }
        Ok(())
    }
}

fn parse_digest(field: &str, value: String) -> Result<Sha256Digest, String> {
    Sha256Digest::parse(value).map_err(|error| format!("{field} {error}"))
}

#[derive(Deserialize)]
struct RawReceipt {
    schema_version: u32,
    bridge: RawBridgeRecord,
    configs: Vec<RawNodeRecord>,
}

#[derive(Deserialize)]
struct RawBridgeRecord {
    canonical_path: PathBuf,
    installed_version: String,
    installed_digest: String,
    previous_digest: Option<String>,
    bridge_compat: Option<muxe_protocol::control::ZellijCompatibility>,
}

#[derive(Deserialize)]
struct RawNodeRecord {
    config_path: PathBuf,
    node: ManagedNode,
    disposition: Disposition,
    semantic: String,
    text_digest: String,
    previous_text: Option<String>,
    previous_semantic: Option<String>,
}

#[derive(Debug, Error)]
pub enum ReceiptError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error("receipt at {} is not valid JSON: {source}", path.display())]
    Corrupt {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("receipt at {} is semantically corrupt: {detail}", path.display())]
    Invalid { path: PathBuf, detail: String },
    #[error("receipt at {} uses unsupported schema version {version}", path.display())]
    UnsupportedVersion { path: PathBuf, version: u32 },
}

fn receipt_path(directory: &Path) -> PathBuf {
    directory.join(RECEIPT_FILE_NAME)
}

/// Loads the receipt, returning `None` when no receipt exists.
///
/// A corrupt receipt or an unsupported schema version is a hard error: the
/// installer must never guess ownership.
///
/// # Errors
///
/// Returns an error when the receipt cannot be read, is not owner-only,
/// fails to parse as JSON, uses an unsupported schema version, or fails
/// ownership validation.
pub fn load(directory: &Path) -> Result<Option<Receipt>, ReceiptError> {
    let path = receipt_path(directory);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ReceiptError::Fs(fsutil::io_error(
                "reading receipt",
                &path,
                source,
            )));
        }
    };
    // Ownership decisions require the receipt to be owner-only; a
    // world-readable receipt could have been planted by another user.
    fsutil::check_owner_file(&path)?;
    let raw: RawReceipt =
        serde_json::from_slice(&bytes).map_err(|source| ReceiptError::Corrupt {
            path: path.clone(),
            source,
        })?;
    if raw.schema_version != RECEIPT_SCHEMA_VERSION {
        return Err(ReceiptError::UnsupportedVersion {
            path,
            version: raw.schema_version,
        });
    }
    Receipt::from_persisted(raw, directory)
        .map(Some)
        .map_err(|detail| ReceiptError::Invalid { path, detail })
}

/// Stores the receipt atomically with owner-only mode.
///
/// # Errors
///
/// Returns an error when the receipt cannot be serialized or the
/// directory setup and atomic write fail.
pub fn store(directory: &Path, receipt: &Receipt) -> Result<(), ReceiptError> {
    receipt
        .validate(directory)
        .map_err(|detail| ReceiptError::Invalid {
            path: receipt_path(directory),
            detail,
        })?;
    let bytes = serde_json::to_vec_pretty(receipt).map_err(|source| ReceiptError::Corrupt {
        path: receipt_path(directory),
        source,
    })?;
    fsutil::ensure_owner_dir(directory)?;
    fsutil::write_atomic(&receipt_path(directory), &bytes, "receipt")?;
    Ok(())
}

/// Removes the receipt file. Only called after every managed artifact is gone
/// or every unresolved record was explicitly left to the user.
///
/// # Errors
///
/// Returns an error when the receipt cannot be removed or the parent
/// directory cannot be synchronized.
pub fn remove(directory: &Path) -> Result<(), ReceiptError> {
    let path = receipt_path(directory);
    match std::fs::remove_file(&path) {
        Ok(()) => {
            fsutil::sync_dir_of(&path)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(ReceiptError::Fs(fsutil::io_error(
            "removing receipt",
            &path,
            source,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_receipt(directory: &Path) -> Receipt {
        Receipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            bridge: BridgeRecord {
                canonical_path: directory.join(BRIDGE_FILE_NAME),
                installed_version: "0.1.0".to_owned(),
                installed_digest: Sha256Digest::parse("a".repeat(64)).unwrap(),
                previous_digest: None,
                bridge_compat: None,
            },
            configs: vec![NodeRecord {
                config_path: PathBuf::from("/cfg/config.kdl"),
                node: ManagedNode::PluginsAlias,
                disposition: Disposition::Created,
                semantic: "muxe".to_owned(),
                text_digest: Sha256Digest::parse("b".repeat(64)).unwrap(),
                previous_text: None,
                previous_semantic: None,
            }],
        }
    }

    #[test]
    fn round_trip_preserves_owner_only_mode() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        let dir = temp.path().join("zellij");
        store(&dir, &sample_receipt(&dir)).unwrap();
        crate::logging::assert_owner_only(&dir.join("receipt.json"));
        assert_eq!(load(&dir).unwrap(), Some(sample_receipt(&dir)));
    }

    #[test]
    fn missing_receipt_loads_as_none() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        assert_eq!(load(temp.path()).unwrap(), None);
    }

    #[test]
    fn corrupt_receipt_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(
            temp.path(),
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .unwrap();
        fsutil::ensure_owner_dir(temp.path()).unwrap();
        fsutil::write_atomic(&temp.path().join("receipt.json"), b"{nope", "receipt").unwrap();
        assert!(matches!(
            load(temp.path()),
            Err(ReceiptError::Corrupt { .. })
        ));
    }
}
