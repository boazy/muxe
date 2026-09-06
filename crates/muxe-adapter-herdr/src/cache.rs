//! Atomic file caches for Herdr runtime schema compatibility.
//!
//! Implements [DESIGN 1742-1777][1]'s two caches under `$CACHE_DIR/herdr`:
//!
//! * the normalized runtime schema cache, keyed by protocol, schema version,
//!   SHA-256 of the canonical request schema, and validator format version;
//! * the configured-request comparison cache, keyed by bundled schema hash,
//!   runtime schema hash, normalized configured native-request hash, context-type
//!   registry version, and validator format version.
//!
//! Entries are written atomically (temporary file plus rename); a truncated,
//! corrupt, or key-mismatched entry is treated as a miss and recomputed.
//! Protocol numbers alone never decide a hit: the content hashes do.
//!
//! [1]: ../../../../../muxe-design/DESIGN.md

use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use muxe_core::{ConfigValue, ConfigValueKind, NativeActionCandidate};
use nix::unistd::Uid;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::VALIDATOR_FORMAT_VERSION;

/// Version of the closed `ContextPath`/`ContextType` registry understood by the
/// configured-request hash. Bump whenever `muxe-core`'s context registry gains,
/// loses, or retypes a path.
pub const CONTEXT_TYPE_REGISTRY_VERSION: u32 = 1;

const NORMALIZED_PREFIX: &str = "normalized-schema-";
const COMPARISON_PREFIX: &str = "configured-requests-";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Owner-only cache root (`$CACHE_DIR/herdr`).
#[derive(Clone, Debug)]
pub struct HerdrCache {
    root: PathBuf,
}

impl HerdrCache {
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            root: cache_dir.join("herdr"),
        }
    }

    fn ensure_root(&self) -> std::io::Result<()> {
        // The configured cache directory itself must not be a symlink: otherwise the
        // owner-only root below it would be created inside an attacker-chosen
        // directory while validation only inspects the resolved path.
        if let Ok(parent) = fs::symlink_metadata(self.root.parent().unwrap_or(&self.root)) {
            if parent.file_type().is_symlink() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!(
                        "Herdr cache parent is a symlink: {}",
                        self.root.display()
                    ),
                ));
            }
        }
        let mut builder = fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&self.root)?;
        validate_owner_directory(&self.root)
    }

    fn entry_path(&self, prefix: &str, key_hash: &str) -> PathBuf {
        self.root.join(format!("{prefix}{key_hash}.json"))
    }

    /// Loads the normalized runtime schema entry, or stores and returns the
    /// freshly normalized document on any miss (absent, corrupt, key drift,
    /// untrusted ownership, or symlink). Returns the canonical request-schema
    /// bytes and whether the entry was a hit.
    ///
    /// On a hit the stored normalized representation is hash-verified and returned
    /// directly, so readers reuse validated bytes instead of trusting recomputed
    /// output. Entries written before the representation was stored have no
    /// `normalized_request` field and are treated as misses.
    pub fn normalized_schema(
        &self,
        protocol: u64,
        schema_version: u64,
        raw_document: &Value,
    ) -> std::io::Result<(Vec<u8>, bool)> {
        let normalized = canonical_request_schema(raw_document);
        let canonical = canonical_bytes(normalized.clone());
        let request_hash = sha256_hex(&canonical);
        let key_hash = sha256_hex(
            format!("{protocol}\n{schema_version}\n{request_hash}\n{VALIDATOR_FORMAT_VERSION}\n")
                .as_bytes(),
        );
        self.ensure_root()?;
        let path = self.entry_path(NORMALIZED_PREFIX, &key_hash);
        if let Some(hit) = read_entry(&path) {
            let matches = hit.get("protocol").and_then(Value::as_u64) == Some(protocol)
                && hit.get("schema_version").and_then(Value::as_u64) == Some(schema_version)
                && hit.get("request_sha256").and_then(Value::as_str) == Some(&request_hash)
                && hit.get("validator_format_version").and_then(Value::as_u64)
                    == Some(u64::from(VALIDATOR_FORMAT_VERSION));
            if matches {
                if let Some(stored) = hit.get("normalized_request") {
                    let stored_bytes = canonical_bytes(stored.clone());
                    if sha256_hex(&stored_bytes) == request_hash {
                        return Ok((stored_bytes, true));
                    }
                }
            }
        }
        let entry = serde_json::json!({
            "protocol": protocol,
            "schema_version": schema_version,
            "request_sha256": request_hash,
            "validator_format_version": VALIDATOR_FORMAT_VERSION,
            "normalized_request": canonicalize(normalized),
        });
        write_atomic(&path, entry.to_string().as_bytes())?;
        sync_parent(&path)?;
        Ok((canonical, false))
    }

    /// Returns the cached per-candidate compatibility outcomes when every key
    /// component matches, else `None`.
    pub fn comparison_lookup(&self, key: &ComparisonKey) -> Option<Vec<bool>> {
        let path = self.entry_path(COMPARISON_PREFIX, &key.hash());
        let entry = read_entry(&path)?;
        if !key.matches(&entry) {
            return None;
        }
        entry
            .get("outcomes")?
            .as_array()?
            .iter()
            .map(Value::as_bool)
            .collect()
    }

    /// Stores whole-set compatibility outcomes atomically.
    pub fn comparison_store(&self, key: &ComparisonKey, outcomes: &[bool]) -> std::io::Result<()> {
        self.ensure_root()?;
        let path = self.entry_path(COMPARISON_PREFIX, &key.hash());
        let mut entry = key.fields();
        entry.insert("outcomes".to_owned(), Value::from(outcomes.to_vec()));
        write_atomic(&path, Value::Object(entry).to_string().as_bytes())?;
        sync_parent(&path)
    }
}

/// Cache key for one configured native-request set. The normalized request hash
/// covers methods, supplied wire fields, constraint-relevant literal values,
/// and typed context references in canonical order.
#[derive(Clone, Debug)]
pub struct ComparisonKey {
    pub bundled_schema_hash: String,
    pub runtime_schema_hash: String,
    pub configured_requests_hash: String,
}

impl ComparisonKey {
    pub fn hash(&self) -> String {
        sha256_hex(
            format!(
                "{}\n{}\n{}\n{}\n{}\n",
                self.bundled_schema_hash,
                self.runtime_schema_hash,
                self.configured_requests_hash,
                CONTEXT_TYPE_REGISTRY_VERSION,
                VALIDATOR_FORMAT_VERSION,
            )
            .as_bytes(),
        )
    }

    fn fields(&self) -> serde_json::Map<String, Value> {
        let mut fields = serde_json::Map::new();
        fields.insert(
            "bundled_schema_hash".to_owned(),
            Value::String(self.bundled_schema_hash.clone()),
        );
        fields.insert(
            "runtime_schema_hash".to_owned(),
            Value::String(self.runtime_schema_hash.clone()),
        );
        fields.insert(
            "configured_requests_hash".to_owned(),
            Value::String(self.configured_requests_hash.clone()),
        );
        fields.insert(
            "context_type_registry_version".to_owned(),
            Value::Number(CONTEXT_TYPE_REGISTRY_VERSION.into()),
        );
        fields.insert(
            "validator_format_version".to_owned(),
            Value::Number(VALIDATOR_FORMAT_VERSION.into()),
        );
        fields
    }

    fn matches(&self, entry: &Value) -> bool {
        let object = entry.as_object();
        self.fields()
            .iter()
            .all(|(field, expected)| object.and_then(|object| object.get(field)) == Some(expected))
    }
}

/// Canonical hash of the effective native-request sequence: method names, wire field names
/// (kebab-case YAML becomes snake_case wire names), literal JSON values, and typed context
/// references. The compiler supplies deterministic binding order, which is retained because
/// cached outcomes are positional and must never be applied to a reordered configuration.
pub fn hash_configured_requests(candidates: &[NativeActionCandidate]) -> String {
    hash_configured_candidate_iter(candidates.iter())
}

/// Equivalent whole-effective-set hash without cloning borrowed compiler candidates.
pub fn hash_configured_request_refs(candidates: &[&NativeActionCandidate]) -> String {
    hash_configured_candidate_iter(candidates.iter().copied())
}

fn hash_configured_candidate_iter<'a>(
    candidates: impl Iterator<Item = &'a NativeActionCandidate>,
) -> String {
    let normalized: Vec<Value> = candidates
        .map(|candidate| {
            let mut fields = BTreeMap::new();
            for field in &candidate.fields {
                fields.insert(
                    field.name.replace('-', "_"),
                    canonical_config_value(&field.value),
                );
            }
            serde_json::json!({
                "type": candidate.type_name,
                "fields": fields,
            })
        })
        .collect();
    sha256_hex(&canonical_bytes(Value::Array(normalized)))
}

fn canonical_config_value(value: &ConfigValue) -> Value {
    match &value.kind {
        ConfigValueKind::Null => Value::Null,
        ConfigValueKind::Boolean(value) => Value::Bool(*value),
        ConfigValueKind::Integer(value) => Value::from(*value),
        ConfigValueKind::Float(value) => serde_json::Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ConfigValueKind::String(value) => Value::String(value.clone()),
        ConfigValueKind::Context(reference) => serde_json::json!({
            "$context": reference.path.as_str(),
            "type": format!("{:?}", reference.expected_type()),
        }),
        ConfigValueKind::Sequence(values) => {
            Value::Array(values.iter().map(canonical_config_value).collect())
        }
        ConfigValueKind::Mapping(fields) => {
            let mut object = BTreeMap::new();
            for field in fields {
                object.insert(
                    field.name.replace('-', "_"),
                    canonical_config_value(&field.value),
                );
            }
            Value::Object(object.into_iter().collect())
        }
    }
}

fn canonical_request_schema(raw_document: &Value) -> Value {
    raw_document
        .pointer("/schemas/request")
        .cloned()
        .unwrap_or(Value::Null)
}

/// Recursively sorts object keys so hashes are independent of JSON key order.
fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        scalar => scalar,
    }
}

fn canonical_bytes(value: Value) -> Vec<u8> {
    serde_json::to_vec(&canonicalize(value)).expect("canonical JSON serialization is infallible")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Reads one cache entry, trusting nothing on disk: symlinks, foreign owners, and
/// group/other-readable files are all treated as absent so the caller recomputes.
fn read_entry(path: &Path) -> Option<Value> {
    let file_type = fs::symlink_metadata(path).ok()?.file_type();
    if file_type.is_symlink() || !file_type.is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::metadata(path).ok()?;
        if metadata.uid() != Uid::current().as_raw() {
            return None;
        }
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return None;
        }
    }
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = path.with_extension(format!("{}.{}.tmp", std::process::id(), sequence));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    let write_result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    write_result
}

fn validate_owner_directory(path: &Path) -> std::io::Result<()> {
    // symlink_metadata first: a symlinked cache root must never be followed, even
    // when it points at an owner-only directory.
    let root_type = fs::symlink_metadata(path)?.file_type();
    if root_type.is_symlink() || !root_type.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("Herdr cache root is not a directory: {}", path.display()),
        ));
    }
    let metadata = fs::metadata(path)?;
    if !metadata.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!("Herdr cache root is not a directory: {}", path.display()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != Uid::current().as_raw() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("Herdr cache root is not owned by the current user: {}", path.display()),
            ));
        }
        // Exact owner-only enforcement: the root must be 0700, not merely closed to
        // group/other. Anything else fails closed before any entry is trusted.
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("Herdr cache root is not owner-only (0700): {}", path.display()),
            ));
        }
    }
    Ok(())
}

/// Syncs the entry's parent directory so an atomic rename survives a crash, following
/// the same staging-commit durability pattern as the broker runtime and native fsutil.
fn sync_parent(path: &Path) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    fs::File::open(parent)?.sync_all()
}

#[cfg(test)]
mod tests {
    use muxe_core::{ConfigField, ConfigValue};

    use super::*;

    fn raw_document(protocol: u64, schema_version: u64, marker: &str) -> Value {
        serde_json::json!({
            "protocol": protocol,
            "schema_version": schema_version,
            "schemas": {"request": {"marker": marker}},
        })
    }

    fn cache() -> (tempfile::TempDir, HerdrCache) {
        let temp = tempfile::TempDir::new().unwrap();
        let cache = HerdrCache::new(temp.path());
        (temp, cache)
    }

    #[test]
    fn normalized_schema_hits_on_identical_document() {
        let (_temp, cache) = cache();
        let document = raw_document(20, 1, "same");
        let (first, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(!hit);
        let (second, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(hit);
        assert_eq!(first, second);
    }

    #[test]
    fn normalized_schema_misses_when_any_key_component_changes() {
        let (_temp, cache) = cache();
        let (_, _) = cache
            .normalized_schema(20, 1, &raw_document(20, 1, "same"))
            .unwrap();
        // Same schema_version but changed content must miss: version numbers alone
        // are insufficient.
        let (_, hit) = cache
            .normalized_schema(20, 1, &raw_document(20, 1, "changed"))
            .unwrap();
        assert!(!hit);
        // Identical content under a different protocol must miss.
        let (_, hit) = cache
            .normalized_schema(21, 1, &raw_document(20, 1, "same"))
            .unwrap();
        assert!(!hit);
    }

    #[test]
    fn normalized_schema_recovers_from_corrupt_entries() {
        let (_temp, cache) = cache();
        let document = raw_document(20, 1, "same");
        let (_, _) = cache.normalized_schema(20, 1, &document).unwrap();
        let entry = fs::read_dir(&cache.root)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(NORMALIZED_PREFIX) && name.ends_with(".json")
                    })
            })
            .unwrap();
        fs::write(&entry, b"{truncated").unwrap();
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(!hit, "corrupt entries must recompute");
        assert!(!bytes.is_empty());
        let (_, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(hit, "recomputed entries must be readable again");
        assert!(
            fs::read_dir(&cache.root)
                .unwrap()
                .map(Result::unwrap)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp")),
            "no temporary files may remain after atomic replacement"
        );
    }

    fn normalized_entry_path(cache: &HerdrCache) -> std::path::PathBuf {
        fs::read_dir(&cache.root)
            .unwrap()
            .map(Result::unwrap)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(NORMALIZED_PREFIX) && name.ends_with(".json")
                    })
            })
            .unwrap()
    }

    #[test]
    fn normalized_schema_verifies_stored_representation_before_reuse() {
        let (_temp, cache) = cache();
        let document = raw_document(20, 1, "same");
        let (first, _) = cache.normalized_schema(20, 1, &document).unwrap();
        let path = normalized_entry_path(&cache);
        let mut entry: Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        // Keep every key field but swap the stored representation: the next load
        // must detect the hash mismatch, recompute, and heal the entry.
        entry["normalized_request"] = serde_json::json!({"marker": "forged"});
        fs::write(&path, entry.to_string().as_bytes()).unwrap();
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(!hit, "hash-mismatched stored bytes must recompute");
        assert_eq!(bytes, first);
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(hit, "healed entries must be reusable");
        assert_eq!(bytes, first);
    }

    #[test]
    fn normalized_schema_rejects_legacy_entries_without_representation() {
        let (_temp, cache) = cache();
        let document = raw_document(20, 1, "same");
        let (first, _) = cache.normalized_schema(20, 1, &document).unwrap();
        let path = normalized_entry_path(&cache);
        let mut entry: Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        entry.as_object_mut().unwrap().remove("normalized_request");
        fs::write(&path, entry.to_string().as_bytes()).unwrap();
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(!hit, "entries without a stored representation must recompute");
        assert_eq!(bytes, first);
        let (_, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(hit);
    }

    #[cfg(unix)]
    #[test]
    fn normalized_schema_distrusts_symlinked_entries() {
        let (_temp, cache) = cache();
        let document = raw_document(20, 1, "same");
        let (first, _) = cache.normalized_schema(20, 1, &document).unwrap();
        let path = normalized_entry_path(&cache);
        let target = cache.root.join("external.json");
        fs::rename(&path, &target).unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(!hit, "symlinked entries must recompute, never be trusted");
        assert_eq!(bytes, first);
        assert!(
            !fs::symlink_metadata(&normalized_entry_path(&cache))
                .unwrap()
                .file_type()
                .is_symlink(),
            "atomic replacement must heal the symlink itself, not its target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn normalized_schema_distrusts_group_readable_entries() {
        use std::os::unix::fs::PermissionsExt;
        let (_temp, cache) = cache();
        let document = raw_document(20, 1, "same");
        let (first, _) = cache.normalized_schema(20, 1, &document).unwrap();
        let path = normalized_entry_path(&cache);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(!hit, "group-readable entries must recompute");
        assert_eq!(bytes, first);
    }

    #[cfg(unix)]
    #[test]
    fn cache_root_rejects_symlinks_and_loose_modes() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::TempDir::new().unwrap();
        let link = temp.path().join("linked-root");
        std::os::unix::fs::symlink(temp.path(), &link).unwrap();
        assert!(
            HerdrCache::new(&link).normalized_schema(20, 1, &raw_document(20, 1, "x")).is_err(),
            "a symlinked cache root must fail closed"
        );
        let (_held, cache) = cache();
        let _ = cache.normalized_schema(20, 1, &raw_document(20, 1, "x")).unwrap();
        fs::set_permissions(&cache.root, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            cache.normalized_schema(20, 1, &raw_document(20, 1, "x")).is_err(),
            "a non-0700 cache root must fail closed"
        );
    }

    #[test]
    fn comparison_cache_round_trips_and_rejects_key_drift() {
        let (_temp, cache) = cache();
        let key = ComparisonKey {
            bundled_schema_hash: "bundled".to_owned(),
            runtime_schema_hash: "runtime".to_owned(),
            configured_requests_hash: "configured".to_owned(),
        };
        assert_eq!(cache.comparison_lookup(&key), None);
        cache.comparison_store(&key, &[true, false]).unwrap();
        assert_eq!(cache.comparison_lookup(&key), Some(vec![true, false]));
        let drifted = ComparisonKey {
            runtime_schema_hash: "runtime-changed".to_owned(),
            ..key.clone()
        };
        assert_eq!(cache.comparison_lookup(&drifted), None);
        let drifted = ComparisonKey {
            configured_requests_hash: "configured-changed".to_owned(),
            ..key
        };
        assert_eq!(cache.comparison_lookup(&drifted), None);
    }

    #[test]
    fn configured_request_hash_is_case_stable_and_position_sensitive() {
        let candidate = |type_name: &str, field: &str| NativeActionCandidate {
            type_name: type_name.to_owned(),
            type_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("test"), 0, 1),
            fields: vec![ConfigField {
                name: field.to_owned(),
                name_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("test"), 0, 1),
                value: ConfigValue::string("w1:p3"),
            }],
        };
        let kebab = vec![candidate("native.herdr.pane:resize", "pane-id")];
        let snake_direct = vec![NativeActionCandidate {
            type_name: "native.herdr.pane:resize".to_owned(),
            type_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("test"), 0, 1),
            fields: vec![ConfigField {
                name: "pane_id".to_owned(),
                name_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("test"), 0, 1),
                value: ConfigValue::string("w1:p3"),
            }],
        }];
        assert_eq!(
            hash_configured_requests(&kebab),
            hash_configured_requests(&snake_direct)
        );
        let pair = vec![
            candidate("native.herdr.pane:resize", "pane-id"),
            candidate("native.herdr.server:reload-config", "pane-id"),
        ];
        let swapped = vec![pair[1].clone(), pair[0].clone()];
        assert_ne!(
            hash_configured_requests(&pair),
            hash_configured_requests(&swapped),
            "whole-set outcomes are positional in compiler binding order"
        );
    }
}
