use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use muxe_adapter_api::{AdapterCapabilities, HostAdapter, HostKind as AdapterHostKind};
use muxe_core::{
    ActionValidation, ActionValidator, CompileInput, CompiledConfig, CompiledGeneration, Compiler,
    ConfigDiagnostic, ConfigDocument, KeyCapabilities, ReloadSettings, SourceId, ThemeAssets,
};
use thiserror::Error;
use tokio::sync::Mutex;

/// All source inputs that define one effective configuration generation.
///
/// The broker discovers the host override once from the adapter kind, then uses the same source
/// set for every reload. The watch service uses the accessors below rather than reconstructing
/// paths independently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigInputs {
    base: PathBuf,
    host_override: Option<PathBuf>,
    directory: PathBuf,
}

impl ConfigInputs {
    pub fn for_host(
        base: impl Into<PathBuf>,
        host: AdapterHostKind,
    ) -> Result<Self, ConfigError> {
        let base = base.into();
        let directory = base
            .parent()
            .ok_or_else(|| ConfigError::AssetDirectory(base.clone()))?
            .to_path_buf();
        let override_name = match host {
            AdapterHostKind::Zellij => "zellij.yml",
            AdapterHostKind::Herdr => "herdr.yml",
        };
        Ok(Self {
            base,
            host_override: Some(directory.join(override_name)),
            directory,
        })
    }

    fn base_only(base: impl Into<PathBuf>) -> Self {
        let base = base.into();
        let directory = base.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
        Self {
            base,
            host_override: None,
            directory,
        }
    }

    pub fn base(&self) -> &Path {
        &self.base
    }

    /// The selected host's optional override location. It may be absent on disk; a reload checks
    /// the path again so creating or removing it is atomic with the next candidate compilation.
    pub fn host_override(&self) -> Option<&Path> {
        self.host_override.as_deref()
    }

    pub fn watch_root(&self) -> &Path {
        &self.directory
    }

    /// Returns whether a recursively watched path can affect this generation.
    pub fn tracks_change(&self, changed: &Path) -> bool {
        changed == self.base
            || self.host_override.as_deref() == Some(changed)
            || ["themes", "color-schemes"].iter().map(|name| self.directory.join(name)).any(
                |directory| changed == directory || changed.starts_with(directory),
            )
    }
}

#[derive(Clone)]
pub struct ConfigSnapshot {
    pub config: Arc<CompiledConfig>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigWatchSpec {
    pub root: PathBuf,
    pub inputs: Vec<PathBuf>,
    pub settings: ReloadSettings,
}

pub struct ConfigStore {
    inputs: ConfigInputs,
    state: Mutex<ConfigSnapshot>,
}

impl ConfigStore {
    pub async fn load(
        path: impl Into<PathBuf>,
        adapter: &dyn HostAdapter,
    ) -> Result<Self, ConfigError> {
        let identity = adapter.identity().await.map_err(ConfigError::Adapter)?;
        let inputs = ConfigInputs::for_host(path, identity.kind)?;
        Self::load_inputs(inputs, adapter).await
    }

    pub async fn load_inputs(
        inputs: ConfigInputs,
        adapter: &dyn HostAdapter,
    ) -> Result<Self, ConfigError> {
        let config = compile_inputs(&inputs, CompiledGeneration(1), adapter).await?;
        Ok(Self {
            inputs,
            state: Mutex::new(ConfigSnapshot {
                config: Arc::new(config),
            }),
        })
    }

    pub fn from_compiled(path: impl Into<PathBuf>, config: CompiledConfig) -> Self {
        Self {
            inputs: ConfigInputs::base_only(path),
            state: Mutex::new(ConfigSnapshot {
                config: Arc::new(config),
            }),
        }
    }

    pub fn path(&self) -> &Path {
        self.inputs.base()
    }

    pub fn inputs(&self) -> &ConfigInputs {
        &self.inputs
    }

    pub async fn snapshot(&self) -> ConfigSnapshot {
        self.state.lock().await.clone()
    }

    /// Returns one immutable watch plan without retaining the configuration mutex.
    pub async fn watch_spec(&self) -> ConfigWatchSpec {
        let settings = self.state.lock().await.config.reload;
        self.inputs.watch_spec(settings)
    }

    /// Compiles the next generation completely before replacing the active immutable snapshot.
    /// Existing UI sessions retain their own Arc and therefore cannot observe a partial reload.
    pub async fn reload(
        &self,
        adapter: &dyn HostAdapter,
    ) -> Result<CompiledGeneration, ConfigError> {
        let next = {
            let state = self.state.lock().await;
            CompiledGeneration(
                state
                    .config
                    .generation
                    .0
                    .checked_add(1)
                    .ok_or(ConfigError::GenerationExhausted)?,
            )
        };
        let candidate = compile_inputs(&self.inputs, next, adapter).await?;

        let mut state = self.state.lock().await;
        if state.config.generation >= next {
            return Ok(state.config.generation);
        }
        state.config = Arc::new(candidate);
        Ok(next)
    }
}

impl ConfigInputs {
    fn watch_spec(&self, settings: ReloadSettings) -> ConfigWatchSpec {
        let mut inputs = vec![
            self.base.clone(),
            self.directory.join("themes"),
            self.directory.join("color-schemes"),
        ];
        if let Some(host_override) = &self.host_override {
            inputs.push(host_override.clone());
        }
        ConfigWatchSpec {
            root: self.directory.clone(),
            inputs,
            settings,
        }
    }
}

async fn compile_inputs(
    inputs: &ConfigInputs,
    generation: CompiledGeneration,
    adapter: &dyn HostAdapter,
) -> Result<CompiledConfig, ConfigError> {
    let path = inputs.base();
    let source = SourceId::new(path.display().to_string());
    let yaml = fs::read_to_string(path).map_err(|source_error| ConfigError::Read {
        path: path.to_path_buf(),
        source: source_error,
    })?;
    let base = ConfigDocument::parse(source, yaml).map_err(ConfigError::Diagnostic)?;
    let host_override = match inputs.host_override() {
        Some(path) => read_optional_document(path)?,
        None => None,
    };
    let theme_assets = load_theme_assets(path)?;
    let capabilities = adapter.capabilities().await.map_err(ConfigError::Adapter)?;
    let validator = AdapterValidator(adapter);
    Compiler
        .compile(
            CompileInput {
                generation,
                base,
                host_override,
                key_capabilities: key_capabilities(capabilities),
                theme_assets,
            },
            Some(&validator),
        )
        .map_err(ConfigError::Diagnostics)
}

fn read_optional_document(path: &Path) -> Result<Option<ConfigDocument>, ConfigError> {
    let yaml = match fs::read_to_string(path) {
        Ok(yaml) => yaml,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(ConfigError::Read {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    ConfigDocument::parse(SourceId::new(path.display().to_string()), yaml)
        .map(Some)
        .map_err(ConfigError::Diagnostic)
}

fn load_theme_assets(config_path: &Path) -> Result<ThemeAssets, ConfigError> {
    let config_dir = config_path
        .parent()
        .ok_or_else(|| ConfigError::AssetDirectory(config_path.to_path_buf()))?;
    Ok(ThemeAssets {
        themes: load_asset_catalog(&config_dir.join("themes"))?,
        color_schemes: load_asset_catalog(&config_dir.join("color-schemes"))?,
    })
}

fn load_asset_catalog(directory: &Path) -> Result<BTreeMap<String, ConfigDocument>, ConfigError> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(source) => {
            return Err(ConfigError::ReadDirectory {
                path: directory.to_path_buf(),
                source,
            });
        }
    };
    let mut documents = BTreeMap::new();
    for entry in entries {
        let entry = entry.map_err(|source| ConfigError::ReadDirectory {
            path: directory.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if !entry
            .file_type()
            .map_err(|source| ConfigError::ReadDirectory {
                path: path.clone(),
                source,
            })?
            .is_file()
            || path.extension().is_none_or(|extension| extension != "yml")
        {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            return Err(ConfigError::AssetName(path));
        };
        let text = fs::read_to_string(&path).map_err(|source| ConfigError::Read {
            path: path.clone(),
            source,
        })?;
        let source = SourceId::new(path.display().to_string());
        let document = ConfigDocument::parse(source, text).map_err(ConfigError::Diagnostic)?;
        if documents.insert(stem.to_owned(), document).is_some() {
            return Err(ConfigError::DuplicateAsset(stem.to_owned()));
        }
    }
    Ok(documents)
}

fn key_capabilities(capabilities: AdapterCapabilities) -> KeyCapabilities {
    KeyCapabilities {
        event_types: capabilities.keyboard.kitty_event_types,
        alternate_keys: capabilities.keyboard.kitty_alternate_keys,
        all_keys_as_escape_codes: capabilities.keyboard.kitty_all_keys_as_escape_codes,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use muxe_adapter_api::{
        AdapterError, AdapterErrorKind, AdapterHealthEvent, CaptureLease, CaptureReleaseReason,
        CaptureRequest, DispatchAccepted, HostIdentity, KeyboardCapabilities, ModalScopeId,
        NativeDispatchRequest, OriginCaptureRequest, PendingPaneRegistration,
        PortableDispatchRequest,
    };
    use tokio::{sync::Notify, time::timeout};

    use super::*;

    struct ReloadAdapter {
        pause_capabilities: AtomicBool,
        capabilities_started: Notify,
        capabilities_release: Notify,
    }

    impl ReloadAdapter {
        fn new() -> Self {
            Self {
                pause_capabilities: AtomicBool::new(false),
                capabilities_started: Notify::new(),
                capabilities_release: Notify::new(),
            }
        }
    }

    impl ActionValidator for ReloadAdapter {
        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _action_span: &muxe_core::SourceSpan,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        fn validate_native(
            &self,
            _candidate: &muxe_core::NativeActionCandidate,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }
    }

    #[async_trait]
    impl HostAdapter for ReloadAdapter {
        async fn identity(&self) -> Result<HostIdentity, AdapterError> {
            Ok(HostIdentity {
                kind: AdapterHostKind::Herdr,
                discovery_key: "config-test-host".to_owned(),
                live_server_id: "config-test-server".to_owned(),
            })
        }

        async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
            if self.pause_capabilities.load(Ordering::Acquire) {
                self.capabilities_started.notify_one();
                self.capabilities_release.notified().await;
            }
            Ok(AdapterCapabilities {
                keyboard: KeyboardCapabilities {
                    kitty_baseline: false,
                    kitty_event_types: false,
                    kitty_alternate_keys: false,
                    kitty_all_keys_as_escape_codes: false,
                },
                supports_capture: false,
                supports_notifications: false,
                supports_native_cancellation: false,
            })
        }

        async fn modal_scope(
            &self,
            _ui_pane: &muxe_core::PaneId,
        ) -> Result<ModalScopeId, AdapterError> {
            Err(unavailable())
        }

        async fn begin_capture(
            &self,
            _request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            Err(unavailable())
        }

        async fn end_capture(
            &self,
            _lease: CaptureLease,
            _reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn close_pending_pane(
            &self,
            _registration: PendingPaneRegistration,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn capture_origin(
            &self,
            _request: OriginCaptureRequest,
        ) -> Result<muxe_core::OriginContext, AdapterError> {
            Err(unavailable())
        }

        async fn dispatch_portable(
            &self,
            _request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Err(unavailable())
        }

        async fn dispatch_native(
            &self,
            _request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Err(unavailable())
        }

        async fn cancel(&self, _execution: muxe_core::ExecutionId) -> Result<(), AdapterError> {
            Err(unavailable())
        }

        async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
            Err(unavailable())
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            Ok(())
        }
    }

    fn unavailable() -> AdapterError {
        AdapterError::new(AdapterErrorKind::Unavailable, "not used by config reload")
    }

    #[test]
    fn host_inputs_track_optional_override_and_asset_changes() {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().join("config.yml");
        let inputs = ConfigInputs::for_host(&base, AdapterHostKind::Herdr).unwrap();
        let override_path = directory.path().join("herdr.yml");
        assert_eq!(inputs.host_override(), Some(override_path.as_path()));
        assert!(inputs.tracks_change(&base));
        assert!(inputs.tracks_change(&override_path));
        assert!(inputs.tracks_change(&directory.path().join("themes/default.yml")));
        assert!(!inputs.tracks_change(&directory.path().join("unrelated.yml")));

        let settings = ReloadSettings {
            watch: true,
            debounce: Duration::from_millis(200),
        };
        let before = inputs.watch_spec(settings);
        assert!(before.inputs.contains(&override_path));
        fs::write(&override_path, "version: 1\nmenus: {}\n").unwrap();
        let after = inputs.watch_spec(settings);
        assert_eq!(after.inputs, before.inputs);
        assert_eq!(after.root, directory.path());
        assert_eq!(after.settings, settings);
    }

    #[tokio::test]
    async fn reload_keeps_snapshots_available_and_rejects_failed_candidates_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.yml");
        fs::write(
            &path,
            "version: 1\nmenus:\n  main:\n    bindings:\n      q:\n        label: Quit\n        action: menu:quit\n",
        )
        .unwrap();
        let adapter = Arc::new(ReloadAdapter::new());
        let store = Arc::new(ConfigStore::load(&path, adapter.as_ref()).await.unwrap());
        assert_eq!(store.snapshot().await.config.generation, CompiledGeneration(1));

        adapter.pause_capabilities.store(true, Ordering::Release);
        let reloading_store = Arc::clone(&store);
        let reloading_adapter = Arc::clone(&adapter);
        let reload =
            tokio::spawn(async move { reloading_store.reload(reloading_adapter.as_ref()).await });
        adapter.capabilities_started.notified().await;

        let during_reload = timeout(Duration::from_secs(1), store.snapshot())
            .await
            .expect("reload validation must not retain the snapshot mutex");
        assert_eq!(during_reload.config.generation, CompiledGeneration(1));

        adapter.capabilities_release.notify_one();
        assert_eq!(reload.await.unwrap().unwrap(), CompiledGeneration(2));
        assert_eq!(store.snapshot().await.config.generation, CompiledGeneration(2));

        fs::write(&path, "version: [").unwrap();
        assert!(store.reload(adapter.as_ref()).await.is_err());
        assert_eq!(store.snapshot().await.config.generation, CompiledGeneration(2));
    }
}


struct AdapterValidator<'a>(&'a dyn HostAdapter);

impl ActionValidator for AdapterValidator<'_> {
    fn validate_portable(
        &self,
        action: &muxe_core::PortableAction,
        action_span: &muxe_core::SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        self.0.validate_portable(action, action_span)
    }

    fn validate_native(
        &self,
        candidate: &muxe_core::NativeActionCandidate,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        self.0.validate_native(candidate)
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read configuration {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("could not enumerate configuration assets in {path}: {source}")]
    ReadDirectory {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("configuration asset path has no valid UTF-8 .yml filename stem: {0}")]
    AssetName(PathBuf),
    #[error("configuration assets contain duplicate stem {0:?}")]
    DuplicateAsset(String),
    #[error("configuration path has no parent directory: {0}")]
    AssetDirectory(PathBuf),
    #[error("configuration generation overflowed")]
    GenerationExhausted,
    #[error("host capability query failed: {0}")]
    Adapter(#[from] muxe_adapter_api::AdapterError),
    #[error("configuration parse failed: {0:?}")]
    Diagnostic(ConfigDiagnostic),
    #[error("configuration compilation failed: {0:?}")]
    Diagnostics(Vec<ConfigDiagnostic>),
}
