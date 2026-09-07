//! Explicit retained-data removal.
//!
//! Takes over the partial `purge` behavior previously inline in `main.rs` with
//! the missing guards: `--cache` refuses while an activation journal is live
//! or needs recovery, and `--config` names the dangling Zellij bridge
//! reference without editing it.
//!
//! `muxe purge` requires at least one of `--config` or `--cache`, prints every
//! resolved path, and requires an interactive confirmation; `--yes` is the
//! explicit non-interactive authorization. `--config` removes the complete
//! Muxe configuration tree, including user-authored themes and the
//! materialized Zellij bridge if it remains there. `--cache` removes compiled
//! schemas and logs. The command never edits Zellij or Herdr keybindings.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use kdl::KdlDocument;
use thiserror::Error;

use crate::{
    fsutil::{self, FsError},
    integration::{
        self,
        receipt::{ReceiptError, load as load_receipt},
    },
    lifecycle::journal::{self, JournalError},
    logging::{LogError, LogEvent, Logger},
};

#[derive(Debug, Error)]
pub enum PurgeError {
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error(transparent)]
    Receipt(#[from] ReceiptError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error("could not scan receipt-listed Zellij reference in {path}: {detail}")]
    ReferenceScan { path: PathBuf, detail: String },
    #[error("muxe purge requires at least one of --config or --cache")]
    NoTarget,
    #[error("muxe purge requires --yes when standard input is not interactive")]
    NonInteractiveWithoutYes,
    #[error("purge declined; nothing was removed")]
    Declined,
    #[error("refusing --cache while activation journal {journal} is live or needs recovery")]
    ActivationJournalLive { journal: PathBuf },
    #[error("refusing --cache while activation owns cache lifetime lock {path}")]
    CacheLeaseActive { path: PathBuf },
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("refusing to purge non-directory target {}", path.display())]
    NotDirectory { path: PathBuf },
}

/// Inputs for `muxe purge`. Paths are injected absolute directories.
#[expect(
    clippy::struct_excessive_bools,
    reason = "the four flags are the CLI-selected purge targets confirmed as one destructive scope; splitting them would change the public constructor shape without any safety gain"
)]
pub struct PurgeInputs<'a> {
    pub config_dir: &'a Path,
    pub cache_dir: &'a Path,
    pub config: bool,
    pub cache: bool,
    pub yes: bool,
    /// Whether standard input is an interactive terminal.
    pub interactive: bool,
    /// Receives every resolved target and real dangling Zellij reference before
    /// confirmation. Call this even for `--yes` so non-interactive users see
    /// the destructive scope.
    pub presenter: Option<&'a dyn Fn(&PurgePreview)>,
    /// Confirmation callback used for interactive runs. It receives exactly
    /// the preview already presented to the user.
    pub confirmer: Option<&'a dyn Fn(&PurgePreview) -> bool>,
    pub logger: Option<&'a Logger>,
}

/// A target that will be removed if authorization and the pre-removal audit succeed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeTarget {
    pub path: PathBuf,
    pub kind: &'static str,
}

/// The destructive scope displayed before confirmation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PurgePreview {
    pub targets: Vec<PurgeTarget>,
    /// Receipt-backed Zellij configuration references that will survive Muxe
    /// config deletion and therefore need manual removal.
    pub warnings: Vec<String>,
}

/// Outcome of a purge run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PurgeReport {
    pub removed: Vec<PurgeTarget>,
    /// Dangling references left for the user (never edited).
    pub warnings: Vec<String>,
}

/// Removes the selected retained data after authorization.
///
/// Refusals happen before any removal: a missing target flag, a
/// non-interactive run without `--yes`, and a live activation journal under
/// `--cache` all fail with nothing removed.
///
/// # Errors
///
/// Returns [`PurgeError`] when authorization fails, a live journal blocks the run, or removal IO fails.
#[expect(
    clippy::needless_pass_by_value,
    reason = "by-value keeps the single CLI-dispatch call ergonomic for this cheap borrowed bundle; a reference would ripple into the retained dispatch signature without benefit"
)]
pub fn purge(inputs: PurgeInputs<'_>) -> Result<PurgeReport, PurgeError> {
    if !inputs.config && !inputs.cache {
        return Err(PurgeError::NoTarget);
    }
    let mut targets = Vec::new();
    if inputs.config {
        targets.push(PurgeTarget {
            path: inputs.config_dir.to_path_buf(),
            kind: "config",
        });
    }
    if inputs.cache {
        let journal = live_activation_journal(inputs.cache_dir)?;
        if let Some(journal) = journal {
            return Err(PurgeError::ActivationJournalLive { journal });
        }
        targets.push(PurgeTarget {
            path: inputs.cache_dir.to_path_buf(),
            kind: "cache",
        });
    }
    let preview = PurgePreview {
        warnings: if inputs.config {
            dangling_bridge_warnings(inputs.config_dir)?
        } else {
            Vec::new()
        },
        targets,
    };
    if let Some(present) = inputs.presenter {
        present(&preview);
    }
    if !inputs.yes {
        if !inputs.interactive {
            return Err(PurgeError::NonInteractiveWithoutYes);
        }
        let confirmed = inputs.confirmer.is_some_and(|confirm| confirm(&preview));
        if !confirmed {
            return Err(PurgeError::Declined);
        }
    }
    let _cache_lease = if inputs.cache {
        let lease = match journal::acquire_cache_purge_lock(inputs.cache_dir) {
            Ok(lease) => lease,
            Err(JournalError::CacheActive { path }) => {
                return Err(PurgeError::CacheLeaseActive { path });
            }
            Err(error) => return Err(PurgeError::Journal(error)),
        };
        if let Some(journal) = live_activation_journal(inputs.cache_dir)? {
            return Err(PurgeError::ActivationJournalLive { journal });
        }
        Some(lease)
    } else {
        None
    };

    log_intent(inputs.logger, &preview)?;
    let mut report = PurgeReport {
        removed: Vec::new(),
        warnings: preview.warnings,
    };
    for target in preview.targets {
        match fs::symlink_metadata(&target.path) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                fs::remove_dir_all(&target.path).map_err(|source| {
                    fsutil::io_error("removing retained data", &target.path, source)
                })?;
                report.removed.push(target);
            }
            Ok(_) => return Err(PurgeError::NotDirectory { path: target.path }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(PurgeError::Fs(fsutil::io_error(
                    "inspecting retained data",
                    &target.path,
                    source,
                )));
            }
        }
    }
    Ok(report)
}

/// Returns the first live activation journal, if any.
///
/// Any journal file present — readable or not — blocks `--cache`: an
/// unreadable journal needs diagnosis, not deletion.
fn live_activation_journal(cache_dir: &Path) -> Result<Option<PathBuf>, PurgeError> {
    let directory = cache_dir.join(crate::lifecycle::journal::ACTIVATION_DIR_NAME);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(PurgeError::Fs(fsutil::io_error(
                "scanning activation journals",
                &directory,
                source,
            )));
        }
    };
    for entry in entries {
        let entry = entry.map_err(|source| {
            fsutil::io_error("scanning activation journals", &directory, source)
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// Returns receipt-backed, still-present Zellij nodes outside the Muxe configuration tree.
fn dangling_bridge_warnings(config_dir: &Path) -> Result<Vec<String>, PurgeError> {
    let Some(receipt) = load_receipt(&integration::integration_dir(config_dir))? else {
        return Ok(Vec::new());
    };
    let mut warnings = Vec::new();
    for record in receipt.configs {
        if record.config_path.starts_with(config_dir)
            || !receipt_listed_node_exists(&record.config_path, record.node)?
        {
            continue;
        }
        warnings.push(format!(
            "Zellij configuration {} still contains receipt-listed {}; remove that node manually because purge never edits host configuration",
            record.config_path.display(),
            record.node.as_str(),
        ));
    }
    Ok(warnings)
}

fn receipt_listed_node_exists(
    path: &Path,
    node: integration::ManagedNode,
) -> Result<bool, PurgeError> {
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(source) => {
            return Err(PurgeError::Fs(fsutil::io_error(
                "reading Zellij configuration",
                path,
                source,
            )));
        }
    };
    let document = KdlDocument::parse_v1(&source).map_err(|error| PurgeError::ReferenceScan {
        path: path.to_path_buf(),
        detail: error.to_string(),
    })?;
    let parent = match node {
        integration::ManagedNode::PluginsAlias => integration::PLUGINS_NODE,
        integration::ManagedNode::LoadPluginsEntry => integration::LOAD_PLUGINS_NODE,
    };
    Ok(document
        .nodes()
        .iter()
        .filter(|candidate| candidate.name().value() == parent)
        .flat_map(|candidate| {
            candidate
                .children()
                .into_iter()
                .flat_map(|children| children.nodes().iter())
        })
        .any(|candidate| candidate.name().value() == integration::MUXE_NODE))
}

fn log_intent(logger: Option<&Logger>, preview: &PurgePreview) -> Result<(), PurgeError> {
    let Some(logger) = logger else {
        return Ok(());
    };
    let kinds = preview
        .targets
        .iter()
        .map(|target| target.kind)
        .collect::<Vec<_>>()
        .join(", ");
    let event = LogEvent::new(
        logger.version().to_owned(),
        "local",
        "purge",
        format!("authorized removal of {kinds}"),
    )?;
    logger.append(&event)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::integration::receipt::{
        BridgeRecord, Disposition, NodeRecord, RECEIPT_SCHEMA_VERSION, Receipt, store,
    };
    fn secure_test_root(path: &Path) {
        let root = path.parent().expect("test path has TempDir parent");
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))
            .expect("test TempDir becomes owner-only");
    }

    fn inputs<'a>(
        config: &'a Path,
        cache: &'a Path,
        purge_config: bool,
        purge_cache: bool,
    ) -> PurgeInputs<'a> {
        secure_test_root(config);
        PurgeInputs {
            config_dir: config,
            cache_dir: cache,
            config: purge_config,
            cache: purge_cache,
            yes: true,
            interactive: false,
            presenter: None,
            confirmer: None,
            logger: None,
        }
    }

    #[test]
    fn requires_a_target() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        assert!(matches!(
            purge(inputs(&config, &cache, false, false)),
            Err(PurgeError::NoTarget)
        ));
    }

    #[test]
    fn cache_lock_owner_blocks_purge_even_without_a_journal() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        fs::create_dir_all(&cache).unwrap();
        secure_test_root(&config);
        let unit = crate::lifecycle::journal::UnitKind::Herdr {
            host_hash: "purge-lock".to_owned(),
        };
        let lock = crate::lifecycle::journal::acquire_unit_lock(&cache, &unit).unwrap();
        let error = purge(inputs(&config, &cache, false, true)).unwrap_err();
        assert!(matches!(error, PurgeError::CacheLeaseActive { .. }));
        assert!(cache.exists());
        drop(lock);
        let report = purge(inputs(&config, &cache, false, true)).unwrap();
        assert_eq!(report.removed.len(), 1);
        assert!(!cache.exists());
    }

    #[test]
    fn cache_purge_rechecks_journal_after_confirmation() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        fs::create_dir_all(&cache).unwrap();
        let confirm = |_: &PurgePreview| {
            let activation = cache.join(crate::lifecycle::journal::ACTIVATION_DIR_NAME);
            fs::create_dir_all(&activation).unwrap();
            fs::write(activation.join("herdr-race.json"), b"{}").unwrap();
            true
        };
        let error = purge(PurgeInputs {
            yes: false,
            interactive: true,
            confirmer: Some(&confirm),
            ..inputs(&config, &cache, false, true)
        })
        .unwrap_err();
        assert!(matches!(error, PurgeError::ActivationJournalLive { .. }));
        assert!(cache.exists());
    }

    #[test]
    fn refuses_cache_while_journal_live() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        let activation = cache.join("activation");
        fs::create_dir_all(&activation).unwrap();
        fs::write(activation.join("herdr-x.json"), b"{}").unwrap();
        fs::create_dir_all(cache.join("logs")).unwrap();
        let error = purge(inputs(&config, &cache, false, true)).unwrap_err();
        assert!(matches!(error, PurgeError::ActivationJournalLive { .. }));
        assert!(cache.exists());
    }

    #[test]
    fn config_purge_warns_about_actual_receipt_listed_zellij_reference() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        let zellij_config = temp.path().join("zellij.kdl");
        fs::write(
            &zellij_config,
            "plugins {\n    muxe location=\"file:/owned/muxe-zellij.wasm\"\n}\n",
        )
        .unwrap();
        store_receipt(&config, &zellij_config);

        let report = purge(inputs(&config, &cache, true, false)).unwrap();
        assert_eq!(report.removed.len(), 1);
        assert_eq!(report.removed[0].kind, "config");
        assert_eq!(report.warnings.len(), 1);
        assert!(report.warnings[0].contains("plugins.muxe"));
        assert!(report.warnings[0].contains(&zellij_config.display().to_string()));
        assert!(!config.exists());
    }

    #[test]
    fn non_interactive_without_yes_fails_closed() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        fs::create_dir_all(&config).unwrap();
        let result = purge(PurgeInputs {
            yes: false,
            interactive: false,
            ..inputs(&config, &cache, true, false)
        });
        assert!(matches!(result, Err(PurgeError::NonInteractiveWithoutYes)));
        assert!(config.exists());
    }

    #[test]
    fn declined_confirmation_shows_actual_scope_and_removes_nothing() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        let zellij_config = temp.path().join("zellij.kdl");
        fs::write(&zellij_config, "load_plugins {\n    muxe\n}\n").unwrap();
        store_receipt(&config, &zellij_config);
        let presented = std::cell::RefCell::new(None);
        let result = purge(PurgeInputs {
            yes: false,
            interactive: true,
            presenter: Some(&|preview| {
                *presented.borrow_mut() = Some(preview.clone());
            }),
            confirmer: Some(&|preview| {
                assert!(
                    preview
                        .warnings
                        .iter()
                        .any(|warning| warning.contains("load_plugins.muxe"))
                );
                false
            }),
            ..inputs(&config, &cache, true, false)
        });
        assert!(matches!(result, Err(PurgeError::Declined)));
        let preview = presented
            .into_inner()
            .expect("scope is presented before confirmation");
        assert_eq!(preview.targets.len(), 1);
        assert_eq!(preview.targets[0].path, config);
        assert!(config.exists());
        assert!(zellij_config.exists());
    }

    #[test]
    fn yes_presents_resolved_targets_before_removal() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        fs::create_dir_all(&cache).unwrap();
        let presented = std::cell::RefCell::new(None);
        let report = purge(PurgeInputs {
            presenter: Some(&|preview| {
                *presented.borrow_mut() = Some(preview.clone());
            }),
            ..inputs(&config, &cache, false, true)
        })
        .unwrap();
        let preview = presented
            .into_inner()
            .expect("--yes still receives a visible scope");
        assert_eq!(
            preview.targets,
            vec![PurgeTarget {
                path: cache.clone(),
                kind: "cache",
            }]
        );
        assert_eq!(report.removed, preview.targets);
        assert!(!cache.exists());
    }

    #[test]
    fn writes_auditable_intent_before_removing_config() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        fs::create_dir_all(&config).unwrap();
        secure_test_root(&cache);
        let logger = Logger::open(&cache, "0.1.0").unwrap();

        purge(PurgeInputs {
            logger: Some(&logger),
            ..inputs(&config, &cache, true, false)
        })
        .unwrap();

        assert!(!config.exists());
        let log = fs::read_to_string(cache.join("logs").join("muxe.jsonl")).unwrap();
        assert!(log.contains("authorized removal of config"));
    }

    #[test]
    fn audit_failure_refuses_removal() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        fs::create_dir_all(&config).unwrap();
        secure_test_root(&cache);
        let logger = Logger::open(&cache, "0.1.0").unwrap();
        let lock = cache.join("logs").join("muxe.jsonl.lock");
        fs::write(&lock, []).unwrap();
        fs::set_permissions(&lock, std::fs::Permissions::from_mode(0o644)).unwrap();

        let error = purge(PurgeInputs {
            logger: Some(&logger),
            ..inputs(&config, &cache, true, false)
        })
        .unwrap_err();

        assert!(matches!(error, PurgeError::Log(LogError::SinkFailed)));
        assert!(config.exists());
    }

    #[test]
    fn cache_purge_does_not_recreate_its_audit_directory() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = temp.path().join("config");
        let cache = temp.path().join("cache");
        secure_test_root(&cache);
        let logger = Logger::open(&cache, "0.1.0").unwrap();

        purge(PurgeInputs {
            logger: Some(&logger),
            ..inputs(&config, &cache, false, true)
        })
        .unwrap();

        assert!(!cache.exists());
    }

    fn store_receipt(config: &Path, zellij_config: &Path) {
        secure_test_root(config);

        let receipt = Receipt {
            schema_version: RECEIPT_SCHEMA_VERSION,
            bridge: BridgeRecord {
                canonical_path: crate::integration::stable_bridge_path(config),
                installed_version: "test".to_owned(),
                installed_digest: "a".repeat(64),
                previous_digest: None,
                bridge_compat: None,
            },
            configs: vec![NodeRecord {
                config_path: zellij_config.to_path_buf(),
                node: if fs::read_to_string(zellij_config)
                    .unwrap()
                    .contains("load_plugins")
                {
                    crate::integration::ManagedNode::LoadPluginsEntry
                } else {
                    crate::integration::ManagedNode::PluginsAlias
                },
                disposition: Disposition::Created,
                semantic: "muxe".to_owned(),
                text_digest: "b".repeat(64),
                previous_text: None,
                previous_semantic: None,
            }],
        };
        store(&crate::integration::integration_dir(config), &receipt).unwrap();
    }
}
