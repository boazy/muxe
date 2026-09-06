//! Owner-only Zellij integration receipt.
//!
//! The receipt records exactly what Muxe owns: the installed bridge digest and
//! version, plus per-node KDL ownership (created, updated with previous text,
//! or merely observed). It never contains a complete Zellij configuration,
//! Muxe configuration values, environment values, or action payloads.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::fsutil::{self, FsError};

/// Current receipt schema version.
pub const RECEIPT_SCHEMA_VERSION: u32 = 1;
/// Receipt file name inside the integration directory.
pub const RECEIPT_FILE_NAME: &str = "receipt.json";

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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BridgeRecord {
    /// Canonical stable bridge path.
    pub canonical_path: PathBuf,
    /// Installed Muxe version.
    pub installed_version: String,
    /// SHA-256 digest of the installed bytes.
    pub installed_digest: String,
    /// Digest of the replaced bridge, retained as the rollback copy.
    pub previous_digest: Option<String>,
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
    pub text_digest: String,
    /// Exact previous node text (only for `Updated` nodes).
    pub previous_text: Option<String>,
    /// Semantic representation of the previous node (only for `Updated` nodes).
    pub previous_semantic: Option<String>,
}

/// The versioned integration receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Receipt {
    pub schema_version: u32,
    pub bridge: BridgeRecord,
    pub configs: Vec<NodeRecord>,
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
    let receipt: Receipt = serde_json::from_slice(&bytes)
        .map_err(|source| ReceiptError::Corrupt { path: path.clone(), source })?;
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION {
        return Err(ReceiptError::UnsupportedVersion {
            path,
            version: receipt.schema_version,
        });
    }
    Ok(Some(receipt))
}

/// Stores the receipt atomically with owner-only mode.
pub fn store(directory: &Path, receipt: &Receipt) -> Result<(), ReceiptError> {
    let bytes = serde_json::to_vec_pretty(receipt)
        .map_err(|source| ReceiptError::Corrupt {
            path: receipt_path(directory),
            source,
        })?;
    fsutil::ensure_owner_dir(directory)?;
    fsutil::write_atomic(&receipt_path(directory), &bytes, "receipt")?;
    Ok(())
}

/// Removes the receipt file. Only called after every managed artifact is gone
/// or every unresolved record was explicitly left to the user.
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

use std::fs;

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_receipt() -> Receipt {
        Receipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            bridge: BridgeRecord {
                canonical_path: PathBuf::from("/cfg/integrations/zellij/muxe-zellij.wasm"),
                installed_version: "0.1.0".to_owned(),
                installed_digest: "a".repeat(64),
                previous_digest: None,
                bridge_compat: None,
            },
            configs: vec![NodeRecord {
                config_path: PathBuf::from("/cfg/config.kdl"),
                node: ManagedNode::PluginsAlias,
                disposition: Disposition::Created,
                semantic: "muxe".to_owned(),
                text_digest: "b".repeat(64),
                previous_text: None,
                previous_semantic: None,
            }],
        }
    }

    #[test]
    fn round_trip_preserves_owner_only_mode() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        let dir = temp.path().join("zellij");
        store(&dir, &sample_receipt()).unwrap();
        crate::logging::assert_owner_only(&dir.join("receipt.json"));
        assert_eq!(load(&dir).unwrap(), Some(sample_receipt()));
    }

    #[test]
    fn missing_receipt_loads_as_none() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        assert_eq!(load(temp.path()).unwrap(), None);
    }

    #[test]
    fn corrupt_receipt_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        std::fs::set_permissions(temp.path(), std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
        fsutil::ensure_owner_dir(temp.path()).unwrap();
        fsutil::write_atomic(&temp.path().join("receipt.json"), b"{nope", "receipt").unwrap();
        assert!(matches!(load(temp.path()), Err(ReceiptError::Corrupt { .. })));
    }
}
