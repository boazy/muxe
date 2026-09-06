use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use saphyr::{LoadableYamlNode, MarkedYamlOwned, ScalarOwned, ScanError, YamlDataOwned};

use crate::action::{NativeActionCandidate, PortableAction};
use crate::context::{ContextReference, ContextValue, OriginContext};
use crate::diagnostic::{ConfigDiagnostic, DiagnosticCode, SourceId, SourceSpan};
use crate::execution::ExecutionCapabilities;
use crate::key::{CanonicalKey, KeyCapabilities, KeyEvent};
use crate::menu::{
    menu_view, BindingId, BindingLocation, CompiledBinding, CompiledGeneration, CompiledMenu,
    MenuId, UiAttachmentView,
};
use crate::theme::CompiledTheme;

/// One configuration value with the exact source range that produced it.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigValue {
    pub span: SourceSpan,
    pub kind: ConfigValueKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigValueKind {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    /// A closed, type-checked context reference retained until broker dispatch resolution.
    Context(ContextReference),
    Sequence(Vec<ConfigValue>),
    Mapping(Vec<ConfigField>),
}

/// An ordered mapping entry. Both the key and value retain independent spans.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigField {
    pub name: String,
    pub name_span: SourceSpan,
    pub value: ConfigValue,
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextResolutionError {
    pub reference: ContextReference,
}
impl ConfigValue {
    pub fn synthetic(kind: ConfigValueKind) -> Self {
        let source = SourceId::new("<muxe built-in>");
        Self {
            span: SourceSpan::new(source, 0, 0),
            kind,
        }
    }

    pub fn string(value: impl Into<String>) -> Self {
        Self::synthetic(ConfigValueKind::String(value.into()))
    }

    pub fn boolean(value: bool) -> Self {
        Self::synthetic(ConfigValueKind::Boolean(value))
    }

    pub fn mapping(fields: impl IntoIterator<Item = (impl Into<String>, ConfigValue)>) -> Self {
        let mut mapping = Vec::new();
        for (name, value) in fields {
            let name = name.into();
            mapping.push(ConfigField {
                name,
                name_span: value.span.clone(),
                value,
            });
        }
        Self::synthetic(ConfigValueKind::Mapping(mapping))
    }

    pub fn as_mapping(&self) -> Option<&[ConfigField]> {
        match &self.kind {
            ConfigValueKind::Mapping(mapping) => Some(mapping),
            _ => None,
        }
    }

    pub fn as_mapping_mut(&mut self) -> Option<&mut Vec<ConfigField>> {
        match &mut self.kind {
            ConfigValueKind::Mapping(mapping) => Some(mapping),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match &self.kind {
            ConfigValueKind::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self.kind {
            ConfigValueKind::Boolean(value) => Some(value),
            _ => None,
        }
    }

    pub fn field(&self, name: &str) -> Option<&ConfigField> {
        self.as_mapping()?.iter().find(|field| field.name == name)
    }

    pub fn field_mut(&mut self, name: &str) -> Option<&mut ConfigField> {
        self.as_mapping_mut()?.iter_mut().find(|field| field.name == name)
    }

    pub fn remove_field(&mut self, name: &str) -> Option<ConfigField> {
        let mapping = self.as_mapping_mut()?;
        let position = mapping.iter().position(|field| field.name == name)?;
        Some(mapping.remove(position))
    }

    pub fn contains_marker(&self, marker: &str) -> bool {
        self.field(marker).is_some_and(|field| field.value.as_bool() == Some(true))
    }

    /// Resolves every typed context marker from the attach-time immutable origin. A missing known
    /// path is an error; Muxe never drops the parameter or substitutes current focus.
    pub fn resolve_context(&self, origin: &OriginContext) -> Result<Self, ContextResolutionError> {
        let kind = match &self.kind {
            ConfigValueKind::Context(reference) => {
                let value = reference
                    .resolve(origin)
                    .ok_or_else(|| ContextResolutionError { reference: reference.clone() })?;
                return Ok(Self { span: self.span.clone(), kind: context_value_kind(value) });
            }
            ConfigValueKind::Sequence(values) => ConfigValueKind::Sequence(
                values.iter().map(|value| value.resolve_context(origin)).collect::<Result<_, _>>()?,
            ),
            ConfigValueKind::Mapping(fields) => ConfigValueKind::Mapping(
                fields
                    .iter()
                    .map(|field| {
                        Ok(ConfigField {
                            name: field.name.clone(),
                            name_span: field.name_span.clone(),
                            value: field.value.resolve_context(origin)?,
                        })
                    })
                    .collect::<Result<_, ContextResolutionError>>()?,
            ),
            _ => return Ok(self.clone()),
        };
        Ok(Self { span: self.span.clone(), kind })
    }
}

fn context_value_kind(value: ContextValue) -> ConfigValueKind {
    match value {
        ContextValue::UnsignedInteger(value) => ConfigValueKind::Integer(i64::try_from(value).unwrap_or(i64::MAX)),
        ContextValue::String(value) | ContextValue::Url(value) => ConfigValueKind::String(value),
        ContextValue::AbsolutePath(value) => ConfigValueKind::String(value.to_string_lossy().into_owned()),
        ContextValue::HostKind(value) => ConfigValueKind::String(format!("{value:?}").to_ascii_lowercase()),
        ContextValue::PaneType(value) => ConfigValueKind::String(format!("{value:?}").to_ascii_lowercase()),
        ContextValue::InvocationSource(value) => ConfigValueKind::String(format!("{value:?}").to_ascii_lowercase()),
        ContextValue::ServerId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::ClientId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::SessionId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::WorkspaceId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::TabId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::PaneId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::WorktreeId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::AgentId(value) => ConfigValueKind::String(value.to_string()),
        ContextValue::LinkHandlerId(value) => ConfigValueKind::String(value.to_string()),
    }
}

/// One parsed YAML document. Parsing is I/O-free and retains all source spans.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigDocument {
    pub source: SourceId,
    pub text: Arc<str>,
    pub root: ConfigValue,
}

/// Parsed configuration before host merge and semantic compilation.
pub type RawConfig = ConfigDocument;

impl ConfigDocument {
    pub fn parse(source: SourceId, text: impl Into<Arc<str>>) -> Result<Self, ConfigDiagnostic> {
        let text = text.into();
        let documents = MarkedYamlOwned::load_from_str(&text).map_err(|error| yaml_error(source.clone(), &text, error))?;
        if documents.len() != 1 {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::YamlSyntax,
                "a Muxe configuration must contain exactly one YAML document",
                SourceSpan::whole(source, &text),
            ));
        }
        let root = config_value_from_yaml(documents.into_iter().next().expect("length checked"), &source)?;
        Ok(Self { source, text, root })
    }
}

fn yaml_error(source: SourceId, text: &str, error: ScanError) -> ConfigDiagnostic {
    let offset = error.marker().index().min(text.len());
    ConfigDiagnostic::error(
        DiagnosticCode::YamlSyntax,
        error.to_string(),
        SourceSpan::new(source, offset, offset),
    )
}

fn config_value_from_yaml(
    value: MarkedYamlOwned,
    source: &SourceId,
) -> Result<ConfigValue, ConfigDiagnostic> {
    let span = SourceSpan::new(source.clone(), value.span.start.index(), value.span.end.index());
    let kind = match value.data {
        YamlDataOwned::Value(scalar) => match scalar {
            ScalarOwned::Null => ConfigValueKind::Null,
            ScalarOwned::Boolean(value) => ConfigValueKind::Boolean(value),
            ScalarOwned::Integer(value) => ConfigValueKind::Integer(value),
            ScalarOwned::FloatingPoint(value) => ConfigValueKind::Float(value.into_inner()),
            ScalarOwned::String(value) => ConfigValueKind::String(value),
        },
        YamlDataOwned::Sequence(sequence) => ConfigValueKind::Sequence(
            sequence
                .into_iter()
                .map(|value| config_value_from_yaml(value, source))
                .collect::<Result<_, _>>()?,
        ),
        YamlDataOwned::Mapping(mapping) => {
            let mut names = HashSet::new();
            let mut fields = Vec::with_capacity(mapping.len());
            for (key, value) in mapping {
                let key_span = SourceSpan::new(source.clone(), key.span.start.index(), key.span.end.index());
                let name = match key.data {
                    YamlDataOwned::Value(ScalarOwned::String(value)) => value,
                    _ => {
                        return Err(ConfigDiagnostic::error(
                            DiagnosticCode::InvalidValue,
                            "mapping keys must be strings",
                            key_span,
                        ));
                    }
                };
                if !names.insert(name.clone()) {
                    return Err(ConfigDiagnostic::error(
                        DiagnosticCode::DuplicateYamlKey,
                        format!("duplicate mapping key `{name}`"),
                        key_span,
                    ));
                }
                fields.push(ConfigField {
                    name,
                    name_span: key_span,
                    value: config_value_from_yaml(value, source)?,
                });
            }
            ConfigValueKind::Mapping(fields)
        }
        YamlDataOwned::Alias(_) => {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::UnsupportedFeature,
                "YAML aliases are not supported in Muxe configuration",
                span,
            ));
        }
        YamlDataOwned::BadValue => {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::YamlSyntax,
                "YAML contains an invalid scalar",
                span,
            ));
        }
        YamlDataOwned::Representation(_, _, _) | YamlDataOwned::Tagged(_, _) => {
            return Err(ConfigDiagnostic::error(
                DiagnosticCode::UnsupportedFeature,
                "YAML tags are not supported in Muxe configuration",
                span,
            ));
        }
    };
    Ok(ConfigValue { span, kind })
}

/// Deep merge with Muxe's ordering, removal, and wholesale-replacement semantics.
pub fn merge_values(base: &mut ConfigValue, overlay: ConfigValue) {
    let (Some(base_mapping), Some(overlay_mapping)) = (base.as_mapping_mut(), overlay.as_mapping()) else {
        *base = clean_directives(overlay);
        return;
    };

    for overlay_field in overlay_mapping {
        if overlay_field.value.contains_marker("_remove") {
            if let Some(position) = base_mapping.iter().position(|field| field.name == overlay_field.name) {
                base_mapping.remove(position);
            }
            continue;
        }
        let overlay_value = clean_directives(overlay_field.value.clone());
        if let Some(existing) = base_mapping.iter_mut().find(|field| field.name == overlay_field.name) {
            if overlay_field.value.contains_marker("_replace") {
                existing.value = overlay_value;
                existing.name_span = overlay_field.name_span.clone();
            } else {
                merge_values(&mut existing.value, overlay_value);
            }
        } else {
            base_mapping.push(ConfigField {
                name: overlay_field.name.clone(),
                name_span: overlay_field.name_span.clone(),
                value: overlay_value,
            });
        }
    }
}

fn clean_directives(mut value: ConfigValue) -> ConfigValue {
    if let Some(mapping) = value.as_mapping_mut() {
        mapping.retain(|field| field.name != "_remove" && field.name != "_replace");
    }
    value
}

/// The top-level effective input is merged in the prescribed seed, base, host-override order.
#[derive(Clone, Debug)]
pub struct CompileInput {
    pub generation: CompiledGeneration,
    pub base: ConfigDocument,
    pub host_override: Option<ConfigDocument>,
    pub key_capabilities: KeyCapabilities,
    pub theme_assets: ThemeAssets,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyboardProfile {
    Vt100 { escape_timeout: Duration },
    Kitty(KeyCapabilities),
}

impl KeyboardProfile {
    /// Matches a configured binding against one event according to the effective input profile.
    ///
    /// VT100 matching recognizes only the legacy control-byte aliases that the terminal cannot
    /// distinguish. Kitty matching preserves the supplied identities and modifiers.
    pub fn matches_binding(&self, binding: &CanonicalKey, event: &KeyEvent) -> bool {
        match self {
            Self::Vt100 { .. } => binding.matches_vt100(event),
            Self::Kitty(_) => binding.matches(event),
        }
    }
}

/// Effective reload behavior. The broker owns filesystem watching and applies this immutable
/// policy to the compiled generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReloadSettings {
    pub watch: bool,
    pub debounce: Duration,
}

impl Default for ReloadSettings {
    fn default() -> Self {
        Self { watch: true, debounce: Duration::from_millis(200) }
    }
}

/// Host version gate independent from schema and action-capability validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostVersionCheck {
    Min,
    Strict,
    Off,
}

impl Default for HostVersionCheck {
    fn default() -> Self {
        Self::Min
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HostSettings {
    pub version_check: HostVersionCheck,
}

/// Pure external-asset inputs. The broker reads the configured theme and color-scheme files and
/// supplies their already source-tracked YAML documents here; this crate performs all parsing and
/// pairing validation without filesystem access.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ThemeAssets {
    pub themes: BTreeMap<String, ConfigDocument>,
    pub color_schemes: BTreeMap<String, ConfigDocument>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThemeSelection {
    pub theme: String,
    pub color_scheme: String,
}

impl Default for ThemeSelection {
    fn default() -> Self {
        Self { theme: "default".to_owned(), color_scheme: "default".to_owned() }
    }
}

/// Active-host action validation lives behind this source-aware compiler boundary.
///
/// Concrete adapters receive fully parsed portable actions and structured native candidates,
/// report action-specific execution capabilities, and attach incompatibility diagnostics to the
/// exact action discriminator. They must validate again immediately before dispatch; load-time
/// acceptance never replaces that check.
pub trait ActionValidator: Send + Sync {
    fn validate_portable(
        &self,
        action: &PortableAction,
        action_span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic>;

    fn validate_native(
        &self,
        candidate: &NativeActionCandidate,
    ) -> Result<ActionValidation, ConfigDiagnostic>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionValidation {
    pub execution: ExecutionCapabilities,
}

/// An accepted, immutable effective generation. It retains action payloads only for broker use.
#[derive(Clone, Debug, PartialEq)]
pub struct CompiledConfig {
    pub generation: CompiledGeneration,
    pub keyboard: KeyboardProfile,
    pub inactivity_timeout: Option<Duration>,
    pub reload: ReloadSettings,
    pub host: HostSettings,
    pub theme_selection: ThemeSelection,
    pub theme: CompiledTheme,
    /// The sole owner of compiled action payloads.
    pub menus: Vec<CompiledMenu>,
    /// Compact generation-scoped locations into `menus`, never cloned payloads.
    pub bindings: BTreeMap<BindingId, BindingLocation>,
}

impl CompiledConfig {
    pub fn menu(&self, id: &MenuId) -> Option<&CompiledMenu> {
        self.menus.iter().find(|menu| &menu.id == id)
    }

    pub fn attachment_view(&self, root: &MenuId) -> Option<UiAttachmentView> {
        Some(UiAttachmentView {
            menu: menu_view(self.generation, root, &self.menus)?,
            keyboard: self.keyboard.clone(),
            inactivity_timeout: self.inactivity_timeout,
            theme_selection: self.theme_selection.clone(),
            theme: self.theme.clone(),
        })
    }

    pub fn binding(&self, generation: CompiledGeneration, id: BindingId) -> Option<&CompiledBinding> {
        (generation == self.generation && id.generation() == generation)
            .then(|| self.bindings.get(&id))
            .flatten()
            .and_then(|location| self.menus.get(location.menu)?.bindings.get(location.binding))
    }
}

/// Pure compiler. It has no filesystem, host, terminal, IPC, or async-runtime dependency.
#[derive(Default)]
pub struct Compiler;

impl Compiler {
    pub fn compile(
        &self,
        input: CompileInput,
        action_validator: Option<&dyn ActionValidator>,
    ) -> Result<CompiledConfig, Vec<ConfigDiagnostic>> {
        crate::compiler::compile_effective(input, action_validator)
    }
}

/// Convenience compiler entrypoint for a base YAML document without a host override.
pub fn compile_yaml(
    generation: CompiledGeneration,
    source: SourceId,
    yaml: impl Into<Arc<str>>,
    key_capabilities: KeyCapabilities,
    action_validator: Option<&dyn ActionValidator>,
) -> Result<CompiledConfig, Vec<ConfigDiagnostic>> {
    let base = ConfigDocument::parse(source, yaml).map_err(|diagnostic| vec![diagnostic])?;
    Compiler.compile(
        CompileInput {
            generation,
            base,
            host_override: None,
            key_capabilities,
            theme_assets: ThemeAssets::default(),
        },
        action_validator,
    )
}

