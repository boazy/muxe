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
    #[must_use]
    pub fn new(cache_dir: &Path) -> Self {
        Self {
            root: cache_dir.join("herdr"),
        }
    }

    fn ensure_root(&self) -> std::io::Result<()> {
        // The configured cache directory itself must not be a symlink: otherwise the
        // owner-only root below it would be created inside an attacker-chosen
        // directory while validation only inspects the resolved path.
        if let Ok(parent) = fs::symlink_metadata(self.root.parent().unwrap_or(&self.root))
            && parent.file_type().is_symlink()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("Herdr cache parent is a symlink: {}", self.root.display()),
            ));
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
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the cache root cannot be created or validated,
    /// or when the fresh entry cannot be written and synced.
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
            if matches && let Some(stored) = hit.get("normalized_request") {
                let stored_bytes = canonical_bytes(stored.clone());
                if sha256_hex(&stored_bytes) == request_hash {
                    return Ok((stored_bytes, true));
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
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the cache root cannot be created or validated,
    /// or when the entry cannot be written and synced.
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
/// covers native discriminators, exact supplied YAML spellings in source order,
/// derived wire names, and lossless literal/context values.
#[derive(Clone, Debug)]
pub struct ComparisonKey {
    pub bundled_schema_hash: String,
    pub runtime_schema_hash: String,
    pub configured_requests_hash: String,
}

impl ComparisonKey {
    #[must_use]
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

/// Fallible whole-set hash over structurally validated native requests. The
/// structural representation rejects the same spellings and duplicate fields as
/// the uncached validator; canonical source values retain distinctions such as
/// context paths that are not visible in the wire placeholder.
///
/// # Errors
///
/// Returns [`crate::ValidationError`] when any candidate is structurally invalid.
pub fn validated_requests_hash(
    candidates: &[&NativeActionCandidate],
) -> Result<String, crate::ValidationError> {
    let validated: Vec<Value> = candidates
        .iter()
        .map(|candidate| {
            let structural = crate::validation::structural_cache_key(candidate)?;
            let canonical_fields = candidate
                .fields
                .iter()
                .map(|field| {
                    serde_json::json!({
                        "name": field.name,
                        "value": canonical_config_value(&field.value),
                    })
                })
                .collect::<Vec<_>>();
            Ok(serde_json::json!({
                "structural": structural,
                "canonical_fields": canonical_fields,
            }))
        })
        .collect::<Result<_, crate::ValidationError>>()?;
    Ok(sha256_hex(&canonical_bytes(Value::Array(validated))))
}

fn canonical_config_value(value: &ConfigValue) -> Value {
    match &value.kind {
        ConfigValueKind::Null => Value::Null,
        ConfigValueKind::Boolean(value) => Value::Bool(*value),
        ConfigValueKind::Integer(value) => Value::from(*value),
        ConfigValueKind::Float(value) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
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

/// Canonical hash of the effective native-request sequence. This compatibility
/// API remains for callers that need a best-effort hash before validation; the
/// adapter cache uses [`validated_requests_hash`] so invalid candidates never
/// reach a lookup.
#[must_use]
pub fn hash_configured_requests(candidates: &[NativeActionCandidate]) -> String {
    hash_configured_candidate_iter(candidates.iter())
}

/// Equivalent whole-effective-set hash without cloning borrowed compiler candidates.
#[must_use]
pub fn hash_configured_request_refs(candidates: &[&NativeActionCandidate]) -> String {
    hash_configured_candidate_iter(candidates.iter().copied())
}

fn hash_configured_candidate_iter<'a>(
    candidates: impl Iterator<Item = &'a NativeActionCandidate>,
) -> String {
    let normalized: Vec<Value> = candidates
        .map(|candidate| {
            crate::validation::structural_cache_key(candidate).unwrap_or_else(|_| {
                // Unvalidated spellings must still hash without colliding with any
                // validated key: keep the exact source spelling and losslessly
                // encode values structurally, never through the wire derivation.
                serde_json::json!({
                    "type": candidate.type_name,
                    "unvalidated_fields": candidate.fields.iter().map(|field| {
                        serde_json::json!({
                            "name": field.name,
                            "value": structural_config_value(&field.value),
                        })
                    }).collect::<Vec<_>>(),
                })
            })
        })
        .collect();
    sha256_hex(&canonical_bytes(Value::Array(normalized)))
}

/// Lossless structural encoding of a config value for unvalidated candidates.
fn structural_config_value(value: &ConfigValue) -> Value {
    match &value.kind {
        ConfigValueKind::Null => Value::Null,
        ConfigValueKind::Boolean(value) => Value::Bool(*value),
        ConfigValueKind::Integer(value) => Value::from(*value),
        ConfigValueKind::Float(value) => {
            serde_json::Number::from_f64(*value).map_or(Value::Null, Value::Number)
        }
        ConfigValueKind::String(value) => Value::String(value.clone()),
        ConfigValueKind::Context(reference) => serde_json::json!({
            "$context": reference.path.as_str(),
            "type": format!("{:?}", reference.expected_type()),
        }),
        ConfigValueKind::Sequence(values) => {
            Value::Array(values.iter().map(structural_config_value).collect())
        }
        ConfigValueKind::Mapping(fields) => Value::Array(
            fields
                .iter()
                .map(|field| {
                    serde_json::json!({
                        "name": field.name,
                        "value": structural_config_value(&field.value),
                    })
                })
                .collect(),
        ),
    }
}

fn canonical_request_schema(raw_document: &Value) -> Value {
    raw_document
        .pointer("/schemas/request")
        .cloned()
        .unwrap_or(Value::Null)
}

/// Canonical bytes of one checked-in fixture's `/schemas/request` subtree,
/// computed with the cache's own key-sorting policy. The agreement test uses
/// this to prove the adapter cache and the schema validator hash the same bytes.
#[doc(hidden)]
#[must_use]
pub fn canonical_request_bytes_for_test(raw_document: &Value) -> Vec<u8> {
    canonical_bytes(canonical_request_schema(raw_document))
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
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::metadata(path).ok()?;
        if metadata.uid() != Uid::current().as_raw() {
            return None;
        }
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
                format!(
                    "Herdr cache root is not owned by the current user: {}",
                    path.display()
                ),
            ));
        }
        // Exact owner-only enforcement: the root must be 0700, not merely closed to
        // group/other. Anything else fails closed before any entry is trusted.
        if metadata.permissions().mode() & 0o777 != 0o700 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "Herdr cache root is not owner-only (0700): {}",
                    path.display()
                ),
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

    fn candidate(type_name: &str, fields: &[&str]) -> NativeActionCandidate {
        NativeActionCandidate {
            type_name: type_name.to_owned(),
            type_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("test"), 0, 1),
            fields: fields
                .iter()
                .map(|field| ConfigField {
                    name: (*field).to_owned(),
                    name_span: muxe_core::SourceSpan::new(muxe_core::SourceId::new("test"), 0, 1),
                    value: ConfigValue::string("w1:p3"),
                })
                .collect(),
        }
    }

    fn comparison_key(configured_requests_hash: String) -> ComparisonKey {
        ComparisonKey {
            bundled_schema_hash: "bundled".to_owned(),
            runtime_schema_hash: "runtime".to_owned(),
            configured_requests_hash,
        }
    }

    fn lookup_after_structural_validation(
        cache: &HerdrCache,
        candidate: &NativeActionCandidate,
    ) -> Result<Option<Vec<bool>>, crate::ValidationError> {
        let configured_requests_hash = validated_requests_hash(&[candidate])?;
        Ok(cache.comparison_lookup(&comparison_key(configured_requests_hash)))
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
                        name.starts_with(NORMALIZED_PREFIX)
                            && std::path::Path::new(name)
                                .extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
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
                        name.starts_with(NORMALIZED_PREFIX)
                            && std::path::Path::new(name)
                                .extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
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
        let mut entry: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
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
        let mut entry: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        entry.as_object_mut().unwrap().remove("normalized_request");
        fs::write(&path, entry.to_string().as_bytes()).unwrap();
        let (bytes, hit) = cache.normalized_schema(20, 1, &document).unwrap();
        assert!(
            !hit,
            "entries without a stored representation must recompute"
        );
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
            !fs::symlink_metadata(normalized_entry_path(&cache))
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
            HerdrCache::new(&link)
                .normalized_schema(20, 1, &raw_document(20, 1, "x"))
                .is_err(),
            "a symlinked cache root must fail closed"
        );
        let (_held, cache) = cache();
        let _ = cache
            .normalized_schema(20, 1, &raw_document(20, 1, "x"))
            .unwrap();
        fs::set_permissions(&cache.root, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            cache
                .normalized_schema(20, 1, &raw_document(20, 1, "x"))
                .is_err(),
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
    fn configured_request_hash_rejects_invalid_spelling_and_preserves_wire_normalization() {
        let kebab = candidate("native.herdr.pane:resize", &["pane-id"]);
        let snake = candidate("native.herdr.pane:resize", &["pane_id"]);

        let snake_error = validated_requests_hash(&[&snake])
            .expect_err("snake_case spelling must fail the structural pre-check");
        assert_eq!(snake_error.code, crate::ValidationCode::AdditionalProperty);

        let structural = crate::validation::structural_cache_key(&kebab).unwrap();
        assert_eq!(
            structural["fields"][0]["name"],
            serde_json::json!("pane-id")
        );
        assert_eq!(
            structural["fields"][0]["wire"],
            serde_json::json!("pane_id")
        );

        let pair = vec![
            candidate("native.herdr.pane:resize", &["pane-id"]),
            candidate("native.herdr.server:reload-config", &["pane-id"]),
        ];
        let swapped = vec![pair[1].clone(), pair[0].clone()];
        let pair_refs = pair.iter().collect::<Vec<_>>();
        let swapped_refs = swapped.iter().collect::<Vec<_>>();
        assert_ne!(
            validated_requests_hash(&pair_refs).unwrap(),
            validated_requests_hash(&swapped_refs).unwrap(),
            "whole-set outcomes are positional in compiler binding order"
        );
    }

    #[test]
    fn duplicate_supplied_fields_reject_identically_warm_and_cold() {
        let valid = candidate("native.herdr.pane:resize", &["pane-id"]);
        let duplicate = candidate("native.herdr.pane:resize", &["pane-id", "pane-id"]);
        let (_warm_temp, warm) = cache();
        let valid_key = comparison_key(validated_requests_hash(&[&valid]).unwrap());
        warm.comparison_store(&valid_key, &[true]).unwrap();

        let warm_result = lookup_after_structural_validation(&warm, &duplicate);
        let (_cold_temp, cold) = cache();
        let cold_result = lookup_after_structural_validation(&cold, &duplicate);
        assert_eq!(
            warm_result, cold_result,
            "duplicate fields must fail before cache state can affect validation"
        );
        let error = warm_result.expect_err("duplicate supplied fields must be rejected");
        assert_eq!(error.code, crate::ValidationCode::AdditionalProperty);
    }

    #[test]
    fn comparison_cache_reuses_a_genuinely_identical_valid_candidate() {
        let first = candidate("native.herdr.pane:resize", &["pane-id"]);
        let identical = first.clone();
        let first_hash = validated_requests_hash(&[&first]).unwrap();
        let identical_hash = validated_requests_hash(&[&identical]).unwrap();
        assert_eq!(first_hash, identical_hash);

        let (_temp, cache) = cache();
        let key = comparison_key(first_hash);
        assert_eq!(cache.comparison_lookup(&key), None);
        cache.comparison_store(&key, &[false]).unwrap();
        assert_eq!(
            lookup_after_structural_validation(&cache, &identical).unwrap(),
            Some(vec![false]),
            "an identical valid candidate must reuse the stored outcome"
        );
    }

    #[test]
    fn cached_valid_request_does_not_authorize_invalid_spelling() {
        let valid = candidate("native.herdr.pane:resize", &["pane-id"]);
        let snake = candidate("native.herdr.pane:resize", &["pane_id"]);
        let (_warm_temp, warm) = cache();
        let valid_key = comparison_key(validated_requests_hash(&[&valid]).unwrap());
        warm.comparison_store(&valid_key, &[true]).unwrap();
        assert_eq!(warm.comparison_lookup(&valid_key), Some(vec![true]));

        let cached_result = lookup_after_structural_validation(&warm, &snake);
        let (_cold_temp, cold) = cache();
        let uncached_result = lookup_after_structural_validation(&cold, &snake);
        assert_eq!(
            cached_result, uncached_result,
            "cached and uncached invalid candidates must reject identically"
        );
        let error = cached_result.expect_err("snake_case spelling must be rejected");
        assert_eq!(error.code, crate::ValidationCode::AdditionalProperty);
        assert_eq!(
            warm.comparison_lookup(&valid_key),
            Some(vec![true]),
            "the warm valid result must not be trusted for the invalid candidate"
        );
    }
}
