use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;

use crate::action::{
    ActionKind, ActionScalar, ActionSpec, CommandAction, ConfigAction, IndexOrDirection,
    KeyboardAction, MenuAction, MenuTarget, NativeActionCandidate, PaneAction, PortableAction,
    PortableActionKind, SessionAction, TabAction,
};
use crate::condition::ConditionProgram;
use crate::config::{
    merge_values, ActionValidator, CompileInput, CompiledConfig, ConfigDocument, ConfigField,
    ConfigValue, ConfigValueKind, HostSettings, HostVersionCheck, KeyboardProfile, ReloadSettings,
    ThemeAssets, ThemeSelection,
};
use crate::context::{ContextReference, ContextType};
use crate::diagnostic::{ConfigDiagnostic, DiagnosticCode, SourceId, SourceSpan};
use crate::execution::{
    AfterAction, ExecutionCapabilities, ExecutionMode, ExecutionPolicy, MenuControlAction,
    TimeoutAction,
};
use crate::key::{CanonicalKey, KeyCapabilities, Vt100BindingKey};
use crate::menu::{
    binding_index, BindingConditions, BindingId, BindingSettings, CompiledBinding, CompiledGeneration,
    CompiledMenu, LayoutSettings, MenuId,
};
use crate::theme::{Color, ColorScheme, CompiledTheme, Style, Theme, ThemeSection, default_color_scheme, default_theme};

const BUILTINS: &str = r#"
settings:
  timeout: 10s
  after_action: quit
  execution:
    timeout: off
    on-timeout: detach
    on-menu-control: detach
inject:
  Builtin.escape:
    select: { type: all }
    action:
      type: override
      bindings:
        esc: { hidden: true, action: "menu:quit" }
  Builtin.backspace:
    select: { type: all }
    action:
      type: override
      bindings:
        backspace: { hidden: true, action: "menu:return" }
  Builtin.pagination:
    select: { type: all }
    action:
      type: override
      bindings:
        left:
          hidden: true
          action: "menu.page:prev"
          conditions: { include: "pages.count > 1", enable: "pages.current > 1" }
        pgup:
          hidden: true
          action: "menu.page:prev"
          conditions: { include: "pages.count > 1", enable: "pages.current > 1" }
        right:
          hidden: true
          action: "menu.page:next"
          conditions: { include: "pages.count > 1", enable: "pages.current < pages.count" }
        pgdn:
          hidden: true
          action: "menu.page:next"
          conditions: { include: "pages.count > 1", enable: "pages.current < pages.count" }
"#;

pub(crate) fn compile_effective(
    input: CompileInput,
    action_validator: Option<&dyn ActionValidator>,
) -> Result<CompiledConfig, Vec<ConfigDiagnostic>> {
    let builtin = ConfigDocument::parse(SourceId::new("<muxe built-in>"), Arc::<str>::from(BUILTINS))
        .map_err(|diagnostic| vec![diagnostic])?;
    let mut root = builtin.root;
    merge_values(&mut root, input.base.root.clone());
    if let Some(override_document) = &input.host_override {
        if let Some(version) = override_document.root.field("version") {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidVersion,
                "host override files inherit the base version and may not set `version`",
                version.name_span.clone(),
            )]);
        }
        merge_values(&mut root, override_document.root.clone());
    }

    validate_fields(
        &root,
        &["version", "keyboard", "settings", "layout", "menus", "inject", "theme", "color-scheme"],
    )?;
    validate_version(&root)?;
    apply_injections(&mut root)?;
    let keyboard = compile_keyboard(root.field("keyboard").map(|field| &field.value), input.key_capabilities)?;
    let global_settings = compile_settings(
        root.field("settings").map(|field| &field.value),
        EffectiveSettings::default(),
        SettingsScope::Global,
    )?;
    let inactivity_timeout = global_settings.timeout;
    let reload = global_settings.reload;
    let host = global_settings.host;
    let (theme_selection, theme) = compile_theme_pair(&root, &input.theme_assets)?;
    let global_layout = compile_layout(root.field("layout").map(|field| &field.value), LayoutSettings::default())?;

    let menus_value = required_field(&root, "menus")?;
    let mut raw_menus = mapping_fields(&menus_value.value, "`menus` must be an ordered mapping")?.to_vec();
    if raw_menus.is_empty() {
        return Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidValue,
            "`menus` must define at least one root menu",
            menus_value.value.span.clone(),
        )]);
    }
    let mut inline_counter = 0_u64;
    let mut inline_menus = Vec::new();
    for menu in &mut raw_menus {
        collect_inline_menus(&mut menu.value, &menu.name, &mut inline_counter, &mut inline_menus);
    }
    raw_menus.extend(inline_menus);
    let known_menus = raw_menus.iter().map(|field| field.name.clone()).collect::<BTreeSet<_>>();

    let mut compiler = MenuCompiler {
        generation: input.generation,
        keyboard: &keyboard,
        global_settings,
        global_layout,
        known_menus: &known_menus,
        action_validator,
        next_binding: 0,
        diagnostics: Vec::new(),
    };
    let mut menus = Vec::with_capacity(raw_menus.len());
    for menu in &raw_menus {
        if let Some(menu) = compiler.compile_menu(menu) {
            menus.push(menu);
        }
    }
    if !compiler.diagnostics.is_empty() {
        return Err(compiler.diagnostics);
    }
    validate_menu_cycles(&menus)?;
    let bindings = binding_index(&menus);
    Ok(CompiledConfig {
        generation: input.generation,
        keyboard,
        inactivity_timeout,
        reload,
        host,
        theme_selection,
        theme,
        menus,
        bindings,
    })
}

fn validate_version(root: &ConfigValue) -> Result<(), Vec<ConfigDiagnostic>> {
    let version = required_field(root, "version")?;
    match version.value.kind {
        ConfigValueKind::Integer(1) => Ok(()),
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidVersion,
            "Muxe configuration requires `version: 1`",
            version.value.span.clone(),
        )]),
    }
}

fn compile_keyboard(
    value: Option<&ConfigValue>,
    host_capabilities: KeyCapabilities,
) -> Result<KeyboardProfile, Vec<ConfigDiagnostic>> {
    let Some(value) = value else {
        return Ok(KeyboardProfile::Vt100 { escape_timeout: Duration::from_millis(25) });
    };
    validate_fields(value, &["mode", "vt100", "kitty"])?;
    let mode = optional_string(value, "mode")?.unwrap_or("vt100");
    match mode {
        "vt100" => {
            let timeout = if let Some(field) = value.field("vt100") {
                validate_fields(&field.value, &["escape-timeout"])?;
                optional_duration(&field.value, "escape-timeout", false)?
                    .unwrap_or(Duration::from_millis(25))
            } else {
                Duration::from_millis(25)
            };
            Ok(KeyboardProfile::Vt100 { escape_timeout: timeout })
        }
        "kitty" => {
            let mut effective = host_capabilities;
            if let Some(kitty) = value.field("kitty") {
                validate_fields(
                    &kitty.value,
                    &["event-types", "alternate-keys", "all-keys-as-escape-codes"],
                )?;
                effective.event_types = keyboard_flag(
                    &kitty.value,
                    "event-types",
                    host_capabilities.event_types,
                    host_capabilities.event_types,
                )?;
                effective.alternate_keys = keyboard_flag(
                    &kitty.value,
                    "alternate-keys",
                    host_capabilities.alternate_keys,
                    host_capabilities.alternate_keys,
                )?;
                effective.all_keys_as_escape_codes = keyboard_flag(
                    &kitty.value,
                    "all-keys-as-escape-codes",
                    host_capabilities.all_keys_as_escape_codes,
                    host_capabilities.all_keys_as_escape_codes,
                )?;
            }
            Ok(KeyboardProfile::Kitty(effective))
        }
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidValue,
            "`keyboard.mode` must be `vt100` or `kitty`",
            value.field("mode").map_or_else(|| value.span.clone(), |field| field.value.span.clone()),
        )]),
    }
}

fn keyboard_flag(
    value: &ConfigValue,
    name: &str,
    default: bool,
    supported: bool,
) -> Result<bool, Vec<ConfigDiagnostic>> {
    let Some(field) = value.field(name) else { return Ok(default) };
    let enabled = expect_bool(&field.value, format!("`keyboard.kitty.{name}` must be boolean"))?;
    if enabled && !supported {
        return Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::KeyCapability,
            format!("`keyboard.kitty.{name}` is not supported by the active adapter"),
            field.value.span.clone(),
        )]);
    }
    Ok(enabled)
}

#[derive(Clone)]
struct EffectiveSettings {
    timeout: Option<Duration>,
    after_action: AfterAction,
    execution: ExecutionPolicy,
    /// An explicit inherited `execution.mode` overrides the action-family default.
    execution_mode_explicit: bool,
    repeat: Option<bool>,
    reload: ReloadSettings,
    host: HostSettings,
}

impl Default for EffectiveSettings {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(10)),
            after_action: AfterAction::Quit,
            execution: ExecutionPolicy::default(),
            execution_mode_explicit: false,
            repeat: None,
            reload: ReloadSettings::default(),
            host: HostSettings::default(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SettingsScope {
    Global,
    Menu,
    Binding,
}

fn compile_settings(
    value: Option<&ConfigValue>,
    mut base: EffectiveSettings,
    scope: SettingsScope,
) -> Result<EffectiveSettings, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(base) };
    let allowed = match scope {
        SettingsScope::Global => &["timeout", "after_action", "execution", "reload", "host"][..],
        SettingsScope::Menu => &["timeout", "after_action", "execution"][..],
        SettingsScope::Binding => &["timeout", "after_action", "execution", "repeat"][..],
    };
    validate_fields(value, allowed)?;
    if let Some(timeout) = value.field("timeout") {
        base.timeout = parse_duration_value(&timeout.value, true)?;
    }
    if let Some(after) = value.field("after_action") {
        base.after_action = parse_after_action(&after.value)?;
    }
    if let Some(repeat) = value.field("repeat") {
        base.repeat = Some(expect_bool(&repeat.value, "`repeat` must be boolean")?);
    }
    if let Some(execution) = value.field("execution") {
        validate_fields(&execution.value, &["mode", "timeout", "on-timeout", "on-menu-control"])?;
        if let Some(mode) = execution.value.field("mode") {
            base.execution.mode = match expect_string(&mode.value, "execution mode must be a string")? {
                "await" => ExecutionMode::Await,
                "detach" => ExecutionMode::Detach,
                _ => return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "execution mode must be `await` or `detach`", mode.value.span.clone())]),
            };
            base.execution_mode_explicit = true;
        }
        if let Some(timeout) = execution.value.field("timeout") {
            base.execution.timeout = parse_duration_value(&timeout.value, true)?;
        }
        if let Some(action) = execution.value.field("on-timeout") {
            base.execution.on_timeout = match expect_string(&action.value, "on-timeout must be a string")? {
                "detach" => TimeoutAction::Detach,
                "cancel" => TimeoutAction::Cancel,
                _ => return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "on-timeout must be `detach` or `cancel`", action.value.span.clone())]),
            };
        }
        if let Some(action) = execution.value.field("on-menu-control") {
            base.execution.on_menu_control = match expect_string(&action.value, "on-menu-control must be a string")? {
                "detach" => MenuControlAction::Detach,
                "cancel" => MenuControlAction::Cancel,
                _ => return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "on-menu-control must be `detach` or `cancel`", action.value.span.clone())]),
            };
        }
    }
    if scope == SettingsScope::Global {
        if let Some(reload) = value.field("reload") {
            validate_fields(&reload.value, &["watch", "debounce"])?;
            if let Some(watch) = reload.value.field("watch") {
                base.reload.watch = expect_bool(&watch.value, "`settings.reload.watch` must be boolean")?;
            }
            if let Some(debounce) = reload.value.field("debounce") {
                base.reload.debounce = parse_duration_value(&debounce.value, false)?.expect("`off` was rejected");
            }
        }
        if let Some(host) = value.field("host") {
            validate_fields(&host.value, &["version"])?;
            if let Some(version) = host.value.field("version") {
                validate_fields(&version.value, &["check"])?;
                if let Some(check) = version.value.field("check") {
                    base.host.version_check = match expect_string(&check.value, "`settings.host.version.check` must be a string")? {
                        "min" => HostVersionCheck::Min,
                        "strict" => HostVersionCheck::Strict,
                        "off" => HostVersionCheck::Off,
                        _ => return Err(vec![ConfigDiagnostic::error(
                            DiagnosticCode::InvalidValue,
                            "`settings.host.version.check` must be `min`, `strict`, or `off`",
                            check.value.span.clone(),
                        )]),
                    };
                }
            }
        }
    }
    Ok(base)
}

fn parse_after_action(value: &ConfigValue) -> Result<AfterAction, Vec<ConfigDiagnostic>> {
    match expect_string(value, "after_action must be a string")? {
        "quit" => Ok(AfterAction::Quit),
        "return" => Ok(AfterAction::Return),
        "stay" => Ok(AfterAction::Stay),
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidValue,
            "after_action must be `quit`, `return`, or `stay`",
            value.span.clone(),
        )]),
    }
}

fn compile_layout(
    value: Option<&ConfigValue>,
    mut layout: LayoutSettings,
) -> Result<LayoutSettings, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(layout) };
    validate_fields(value, &["padding", "max-item-title-length"])?;
    if let Some(limit) = value.field("max-item-title-length") {
        layout.max_item_title_length = nonnegative_u16(&limit.value, "max-item-title-length")?;
    }
    if let Some(padding) = value.field("padding") {
        validate_fields(
            &padding.value,
            &["left", "right", "top", "bottom", "between-rows", "between-columns"],
        )?;
        let mut resolved = layout.padding;
        for (name, slot) in [
            ("left", &mut resolved.left),
            ("right", &mut resolved.right),
            ("top", &mut resolved.top),
            ("bottom", &mut resolved.bottom),
            ("between-rows", &mut resolved.between_rows),
            ("between-columns", &mut resolved.between_columns),
        ] {
            if let Some(value) = padding.value.field(name) {
                *slot = nonnegative_u16(&value.value, name)?;
            }
        }
        layout.padding = resolved;
    }
    Ok(layout)
}

fn compile_theme_pair(
    root: &ConfigValue,
    assets: &ThemeAssets,
) -> Result<(ThemeSelection, CompiledTheme), Vec<ConfigDiagnostic>> {
    let (theme_name, theme_span) = selected_asset_name(root, "theme")?;
    let (color_scheme_name, scheme_span) = selected_asset_name(root, "color-scheme")?;
    let theme = match assets.themes.get(&theme_name) {
        Some(document) => parse_theme_document(document)?,
        None if theme_name == "default" => default_theme(),
        None => {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidTheme,
                format!("unknown theme `{theme_name}`"),
                theme_span,
            )]);
        }
    };
    let scheme = match assets.color_schemes.get(&color_scheme_name) {
        Some(document) => parse_color_scheme_document(document)?,
        None if color_scheme_name == "default" => default_color_scheme(),
        None => {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidColorScheme,
                format!("unknown color scheme `{color_scheme_name}`"),
                scheme_span,
            )]);
        }
    };
    let compiled = CompiledTheme::compile(theme, scheme).map_err(|error| {
        vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidTheme,
            format!("theme `{theme_name}` cannot pair with color scheme `{color_scheme_name}`: {error}"),
            theme_span,
        )]
    })?;
    Ok((
        ThemeSelection {
            theme: theme_name,
            color_scheme: color_scheme_name,
        },
        compiled,
    ))
}

fn selected_asset_name(root: &ConfigValue, field_name: &str) -> Result<(String, SourceSpan), Vec<ConfigDiagnostic>> {
    let Some(field) = root.field(field_name) else {
        return Ok(("default".to_owned(), root.span.clone()));
    };
    Ok((
        expect_string(&field.value, format!("`{field_name}` must be a string"))?.to_owned(),
        field.value.span.clone(),
    ))
}

fn parse_theme_document(document: &ConfigDocument) -> Result<Theme, Vec<ConfigDiagnostic>> {
    validate_fields(&document.root, &["common", "menu", "settings"])?;
    let common = parse_theme_section(document.root.field("common").map(|field| &field.value))?;
    let menu = parse_theme_section(document.root.field("menu").map(|field| &field.value))?;
    let settings = match document.root.field("settings") {
        Some(field) => {
            let fields = mapping_fields(&field.value, "theme `settings` must be a mapping")?;
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name.clone(),
                        scalar_text(&field.value).map_err(|_| {
                            vec![ConfigDiagnostic::error(
                                DiagnosticCode::InvalidTheme,
                                "theme `settings` values must be scalars",
                                field.value.span.clone(),
                            )]
                        })?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Vec<ConfigDiagnostic>>>()?
        }
        None => BTreeMap::new(),
    };
    Ok(Theme { common, menu, settings })
}

fn parse_theme_section(value: Option<&ConfigValue>) -> Result<ThemeSection, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(ThemeSection::default()) };
    validate_fields(value, &["styles", "templates"])?;
    let mut styles = BTreeMap::new();
    if let Some(style_map) = value.field("styles") {
        for style in mapping_fields(&style_map.value, "theme `styles` must be a mapping")? {
            validate_fields(
                &style.value,
                &["foreground", "background", "bold", "dim", "italic", "underline", "strikethrough"],
            )?;
            styles.insert(
                style.name.clone(),
                Style {
                    foreground: optional_string(&style.value, "foreground")?.map(str::to_owned),
                    background: optional_string(&style.value, "background")?.map(str::to_owned),
                    bold: style.value.field("bold").map(|field| expect_bool(&field.value, "style `bold` must be boolean")).transpose()?.unwrap_or(false),
                    dim: style.value.field("dim").map(|field| expect_bool(&field.value, "style `dim` must be boolean")).transpose()?.unwrap_or(false),
                    italic: style.value.field("italic").map(|field| expect_bool(&field.value, "style `italic` must be boolean")).transpose()?.unwrap_or(false),
                    underline: style.value.field("underline").map(|field| expect_bool(&field.value, "style `underline` must be boolean")).transpose()?.unwrap_or(false),
                    strikethrough: style.value.field("strikethrough").map(|field| expect_bool(&field.value, "style `strikethrough` must be boolean")).transpose()?.unwrap_or(false),
                },
            );
        }
    }
    let mut templates = BTreeMap::new();
    if let Some(template_map) = value.field("templates") {
        flatten_scalar_mapping(&template_map.value, "", &mut templates, "theme template")?;
    }
    Ok(ThemeSection { styles, templates })
}

fn parse_color_scheme_document(document: &ConfigDocument) -> Result<ColorScheme, Vec<ConfigDiagnostic>> {
    validate_fields(&document.root, &["title", "palette", "colors"])?;
    let title = expect_string(&required_field(&document.root, "title")?.value, "color-scheme `title` must be a string")?.to_owned();
    let mut palette = BTreeMap::new();
    if let Some(palette_map) = document.root.field("palette") {
        for field in mapping_fields(&palette_map.value, "color-scheme `palette` must be a mapping")? {
            let value = expect_string(&field.value, "palette values must be `#rgb` or `#rrggbb` strings")?.to_owned();
            Color::parse(&value).map_err(|error| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidColorScheme, error.to_string(), field.value.span.clone())])?;
            palette.insert(field.name.clone(), value);
        }
    }
    let mut colors = BTreeMap::new();
    if let Some(color_map) = document.root.field("colors") {
        flatten_scalar_mapping(&color_map.value, "", &mut colors, "color-scheme color")?;
    }
    if colors.values().any(|value| value == "inherit") {
        return Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidColorScheme,
            "`inherit` is reserved for the embedded default color scheme; user color values must be palette aliases or `#hex`",
            document.root.span.clone(),
        )]);
    }
    let scheme = ColorScheme { title, palette, colors };
    for name in scheme.colors.keys() {
        scheme.resolve(name).map_err(|error| {
            vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidColorScheme,
                error.to_string(),
                document.root.span.clone(),
            )]
        })?;
    }
    Ok(scheme)
}

fn flatten_scalar_mapping(
    value: &ConfigValue,
    prefix: &str,
    output: &mut BTreeMap<String, String>,
    subject: &str,
) -> Result<(), Vec<ConfigDiagnostic>> {
    for field in mapping_fields(value, &format!("{subject} must be a mapping"))? {
        let name = if prefix.is_empty() {
            field.name.clone()
        } else {
            format!("{prefix}.{}", field.name)
        };
        match &field.value.kind {
            ConfigValueKind::Mapping(_) => flatten_scalar_mapping(&field.value, &name, output, subject)?,
            _ => {
                let text = expect_string(&field.value, format!("{subject} values must be strings"))?;
                output.insert(name, text.to_owned());
            }
        }
    }
    Ok(())
}

fn nonnegative_u16(value: &ConfigValue, name: &str) -> Result<u16, Vec<ConfigDiagnostic>> {
    match value.kind {
        ConfigValueKind::Integer(value) if (0..=u16::MAX as i64).contains(&value) => Ok(value as u16),
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidValue,
            format!("`{name}` must be a non-negative integer no greater than {}", u16::MAX),
            value.span.clone(),
        )]),
    }
}
struct MenuCompiler<'a> {
    generation: CompiledGeneration,
    keyboard: &'a KeyboardProfile,
    global_settings: EffectiveSettings,
    global_layout: LayoutSettings,
    known_menus: &'a BTreeSet<String>,
    action_validator: Option<&'a dyn ActionValidator>,
    next_binding: u64,
    diagnostics: Vec<ConfigDiagnostic>,
}

impl<'a> MenuCompiler<'a> {
    fn compile_menu(&mut self, field: &ConfigField) -> Option<CompiledMenu> {
        let mapping = match mapping_fields(&field.value, "a menu must be a mapping") {
            Ok(mapping) => mapping,
            Err(mut errors) => {
                self.diagnostics.append(&mut errors);
                return None;
            }
        };
        if let Err(mut errors) = validate_fields(&field.value, &["title", "tags", "settings", "layout", "bindings", "_muxe_inline_id"]) {
            self.diagnostics.append(&mut errors);
            return None;
        }
        let title = match optional_string(&field.value, "title") {
            Ok(title) => title.map(str::to_owned),
            Err(mut errors) => {
                self.diagnostics.append(&mut errors);
                return None;
            }
        };
        let tags = match string_sequence(field.value.field("tags").map(|field| &field.value), "tags") {
            Ok(tags) => tags,
            Err(mut errors) => {
                self.diagnostics.append(&mut errors);
                return None;
            }
        };
        let settings = match compile_settings(
            field.value.field("settings").map(|field| &field.value),
            self.global_settings.clone(),
            SettingsScope::Menu,
        ) {
            Ok(settings) => settings,
            Err(mut errors) => {
                self.diagnostics.append(&mut errors);
                return None;
            }
        };
        let layout = match compile_layout(field.value.field("layout").map(|field| &field.value), self.global_layout) {
            Ok(layout) => layout,
            Err(mut errors) => {
                self.diagnostics.append(&mut errors);
                return None;
            }
        };
        let bindings_value = match field.value.field("bindings") {
            Some(binding) => &binding.value,
            None => {
                self.diagnostics.push(ConfigDiagnostic::error(DiagnosticCode::MissingField, "menu requires `bindings`", field.value.span.clone()));
                return None;
            }
        };
        let bindings = match mapping_fields(bindings_value, "`bindings` must be an ordered mapping") {
            Ok(bindings) => bindings,
            Err(mut errors) => {
                self.diagnostics.append(&mut errors);
                return None;
            }
        };
        let mut compiled = Vec::with_capacity(bindings.len());
        let mut vt100_bindings = BTreeMap::<Vt100BindingKey, SourceSpan>::new();
        for binding_field in bindings {
            match self.compile_binding(binding_field, &settings) {
                Ok(binding) => {
                    if matches!(self.keyboard, KeyboardProfile::Vt100 { .. }) {
                        let key = binding.key.vt100_binding_key();
                        if let Some(first) = vt100_bindings.get(&key) {
                            self.diagnostics.push(
                                ConfigDiagnostic::error(
                                    DiagnosticCode::KeyCollision,
                                    format!(
                                        "binding `{}` is indistinguishable from another binding in the vt100 profile",
                                        binding.key.canonical_string()
                                    ),
                                    binding_field.name_span.clone(),
                                )
                                .with_label(
                                    first.clone(),
                                    "first indistinguishable binding is here",
                                ),
                            );
                            continue;
                        }
                        vt100_bindings.insert(key, binding_field.name_span.clone());
                    }
                    compiled.push(binding);
                }
                Err(mut errors) => self.diagnostics.append(&mut errors),
            }
        }
        let _ = mapping;
        Some(CompiledMenu {
            id: MenuId::new(field.name.clone()),
            title,
            tags,
            bindings: compiled,
            layout,
        })
    }

    fn compile_binding(
        &mut self,
        field: &ConfigField,
        menu_settings: &EffectiveSettings,
    ) -> Result<CompiledBinding, Vec<ConfigDiagnostic>> {
        validate_fields(&field.value, &["label", "hidden", "action", "settings", "conditions"])?;
        let key = CanonicalKey::parse_diagnostic(&field.name, field.name_span.clone()).map_err(|error| vec![error])?;
        let hidden = field.value.field("hidden").map(|field| expect_bool(&field.value, "`hidden` must be boolean")).transpose()?.unwrap_or(false);
        let label = optional_string(&field.value, "label")?.map(str::to_owned);
        if !hidden && label.as_deref().is_none_or(str::is_empty) {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::MissingField,
                "every visible binding requires a non-empty `label`",
                field.value.span.clone(),
            )]);
        }
        let mut settings = compile_settings(
            field.value.field("settings").map(|field| &field.value),
            menu_settings.clone(),
            SettingsScope::Binding,
        )?;
        let action_value = required_field(&field.value, "action")?;
        let (action, execution) = self.parse_action(&action_value.value)?;
        if !settings.execution_mode_explicit
            && matches!(action.kind(), ActionKind::Portable(PortableActionKind::CommandExecute))
        {
            settings.execution.mode = ExecutionMode::Detach;
        }
        validate_execution(&settings.execution, execution, action_value.value.span.clone())?;
        let profile = match self.keyboard {
            KeyboardProfile::Vt100 { .. } => KeyCapabilities::default(),
            KeyboardProfile::Kitty(capabilities) => *capabilities,
        };
        let required = key.required_capabilities(settings.repeat);
        validate_key_capabilities(required, profile, &field.name_span)?;
        let conditions = compile_conditions(
            field.value.field("conditions").map(|field| &field.value),
            matches!(action.kind(), ActionKind::Portable(PortableActionKind::MenuPagePrev | PortableActionKind::MenuPageNext)),
        )?;
        let id = BindingId::new(self.generation, self.next_binding);
        self.next_binding += 1;
        Ok(CompiledBinding {
            id,
            key,
            label,
            hidden,
            action,
            settings: BindingSettings {
                after_action: settings.after_action,
                execution: settings.execution,
                repeat: settings.repeat,
            },
            conditions,
        })
    }

    fn parse_action(
        &self,
        value: &ConfigValue,
    ) -> Result<(ActionSpec, ExecutionCapabilities), Vec<ConfigDiagnostic>> {
        let (type_name, type_span, fields) = action_fields(value)?;
        let kind = ActionKind::parse(&type_name).ok_or_else(|| {
            vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidAction,
                format!("unknown action `{type_name}`"),
                type_span.clone(),
            )]
        })?;
        match kind {
            ActionKind::Native(_) => {
                for field in &fields {
                    validate_context_references(&field.value)?;
                }
                let candidate = NativeActionCandidate { type_name, type_span, fields };
                let Some(validator) = self.action_validator else {
                    return Err(vec![ConfigDiagnostic::error(
                        DiagnosticCode::NativeActionRejected,
                        "native actions require an active adapter validator",
                        value.span.clone(),
                    )]);
                };
                let validated = validator.validate_native(&candidate).map_err(|diagnostic| vec![diagnostic])?;
                Ok((ActionSpec::Native(candidate), validated.execution))
            }
            ActionKind::Portable(kind) => {
                let (spec, baseline) = self.parse_portable(kind, fields, type_span.clone())?;
                let ActionSpec::Portable(action) = &spec else {
                    unreachable!("portable parser returns a portable action");
                };
                let capabilities = self
                    .action_validator
                    .map(|validator| validator.validate_portable(action, &type_span))
                    .transpose()
                    .map_err(|diagnostic| vec![diagnostic])?
                    .map_or(baseline, |validated| validated.execution);
                Ok((spec, capabilities))
            }
        }
    }

    fn parse_portable(
        &self,
        kind: PortableActionKind,
        fields: Vec<ConfigField>,
        span: SourceSpan,
    ) -> Result<(ActionSpec, ExecutionCapabilities), Vec<ConfigDiagnostic>> {
        let action = match kind {
            PortableActionKind::MenuOpen => {
                let menu = field_named(&fields, "menu");
                let submenu = field_named(&fields, "submenu");
                if menu.is_some() == submenu.is_some() {
                    return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidActionArguments, "menu:open requires exactly one of `menu` or `submenu`", span)]);
                }
                ensure_action_fields(&fields, &["menu", "submenu"])?;
                let target = if let Some(menu) = menu {
                    let target = expect_string(&menu.value, "`menu` must be a menu ID")?.to_owned();
                    if !self.known_menus.contains(&target) {
                        return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidMenuReference, format!("unknown menu `{target}`"), menu.value.span.clone())]);
                    }
                    MenuTarget::Named(target)
                } else {
                    let submenu = submenu.expect("checked");
                    let inline_id = required_field(&submenu.value, "_muxe_inline_id")?;
                    let target = expect_string(&inline_id.value, "invalid compiler inline menu ID")?.to_owned();
                    MenuTarget::Inline(target)
                };
                PortableAction::Menu(MenuAction::Open(target))
            }
            PortableActionKind::MenuReturn => { ensure_action_fields(&fields, &[])?; PortableAction::Menu(MenuAction::Return) }
            PortableActionKind::MenuQuit => { ensure_action_fields(&fields, &[])?; PortableAction::Menu(MenuAction::Quit) }
            PortableActionKind::MenuPagePrev => { ensure_action_fields(&fields, &[])?; PortableAction::Menu(MenuAction::PagePrev) }
            PortableActionKind::MenuPageNext => { ensure_action_fields(&fields, &[])?; PortableAction::Menu(MenuAction::PageNext) }
            PortableActionKind::ConfigReload => { ensure_action_fields(&fields, &[])?; PortableAction::Config(ConfigAction::Reload) }
            PortableActionKind::KeyboardSend => {
                if field_named(&fields, "sequence").is_some() {
                    return Err(vec![ConfigDiagnostic::error(DiagnosticCode::UnsupportedFeature, "keyboard:send `sequence` is not supported in schema version 1", field_named(&fields, "sequence").expect("checked").value.span.clone())]);
                }
                ensure_action_fields(&fields, &["keys", "text", "sequence"])?;
                match (field_named(&fields, "keys"), field_named(&fields, "text")) {
                    (Some(keys), None) => PortableAction::Keyboard(KeyboardAction::SendKeys(action_key_sequence(&keys.value)?)),
                    (None, Some(text)) => PortableAction::Keyboard(KeyboardAction::SendText(action_string_scalar(&text.value, "keyboard text", ContextAllowance::StringOnly)?)),
                    _ => return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidActionArguments, "keyboard:send requires exactly one of `keys` or `text`", span)]),
                }
            }
            PortableActionKind::CommandExecute => {
                ensure_action_fields(&fields, &["program", "args", "cwd", "env"])?;
                let program = action_string_scalar(&required_action_field(&fields, "program")?.value, "command program", ContextAllowance::Textual)?;
                let args = action_string_sequence(field_named(&fields, "args").map(|field| &field.value), "command args", ContextAllowance::Textual)?;
                let cwd = field_named(&fields, "cwd").map(|field| action_string_scalar(&field.value, "command cwd", ContextAllowance::AbsolutePath)).transpose()?;
                let env = action_string_mapping(field_named(&fields, "env").map(|field| &field.value), "command env", ContextAllowance::Textual)?;
                PortableAction::Command(CommandAction { program, args, cwd, env })
            }
            PortableActionKind::TabCreate => {
                ensure_action_fields(&fields, &["workspace-id"])?;
                let workspace_id = field_named(&fields, "workspace-id")
                    .map(|field| action_string_scalar(&field.value, "tab workspace-id", ContextAllowance::WorkspaceId))
                    .transpose()?;
                PortableAction::Tab(TabAction::Create { workspace_id })
            }
            PortableActionKind::TabClose => { ensure_action_fields(&fields, &[])?; PortableAction::Tab(TabAction::Close) }
            PortableActionKind::TabRename => {
                ensure_action_fields(&fields, &["name"])?;
                PortableAction::Tab(TabAction::Rename {
                    name: field_named(&fields, "name")
                        .map(|field| action_string_scalar(&field.value, "tab name", ContextAllowance::Textual))
                        .transpose()?,
                })
            }
            PortableActionKind::TabFocus => PortableAction::Tab(TabAction::Focus(index_or_direction(&fields)?)),
            PortableActionKind::TabMove => PortableAction::Tab(TabAction::Move(index_or_direction(&fields)?)),
            PortableActionKind::TabSwap => PortableAction::Tab(TabAction::Swap(index_or_direction(&fields)?)),
            PortableActionKind::PaneCreate => { ensure_action_fields(&fields, &[])?; PortableAction::Pane(PaneAction::Create) }
            PortableActionKind::PaneSplit => {
                ensure_action_fields(&fields, &["direction"])?;
                PortableAction::Pane(PaneAction::Split {
                    direction: field_named(&fields, "direction")
                        .map(|field| action_direction_scalar(&field.value, "pane direction"))
                        .transpose()?,
                })
            }
            PortableActionKind::PaneClose => { ensure_action_fields(&fields, &[])?; PortableAction::Pane(PaneAction::Close) }
            PortableActionKind::PaneFocus => PortableAction::Pane(PaneAction::Focus(index_or_direction(&fields)?)),
            PortableActionKind::PaneMove => PortableAction::Pane(PaneAction::Move(index_or_direction(&fields)?)),
            PortableActionKind::PaneSwap => PortableAction::Pane(PaneAction::Swap(index_or_direction(&fields)?)),
            PortableActionKind::PaneResize => {
                ensure_action_fields(&fields, &["direction", "amount"])?;
                let direction = action_direction_scalar(&required_action_field(&fields, "direction")?.value, "pane direction")?;
                let amount = field_named(&fields, "amount").map(|field| action_scalar(&field.value, "pane amount")).transpose()?;
                PortableAction::Pane(PaneAction::Resize { direction, amount })
            }
            PortableActionKind::PaneZoom => PortableAction::Pane(PaneAction::Zoom { enabled: optional_action_bool_field(&fields, "enabled")? }),
            PortableActionKind::PaneFullscreen => PortableAction::Pane(PaneAction::Fullscreen { enabled: optional_action_bool_field(&fields, "enabled")? }),
            PortableActionKind::PaneFloating => PortableAction::Pane(PaneAction::Floating { enabled: optional_action_bool_field(&fields, "enabled")? }),
            PortableActionKind::PaneFrame => {
                ensure_action_fields(&fields, &["visible"])?;
                PortableAction::Pane(PaneAction::Frame {
                    visible: field_named(&fields, "visible").map(|field| action_bool_scalar(&field.value, "pane visible")).transpose()?,
                })
            }
            PortableActionKind::SessionCreate => { ensure_action_fields(&fields, &[])?; PortableAction::Session(SessionAction::Create) }
            PortableActionKind::SessionAttach => PortableAction::Session(SessionAction::Attach { name: required_string_action_field(&fields, "name", "session name")? }),
            PortableActionKind::SessionSwitch => PortableAction::Session(SessionAction::Switch { name: required_string_action_field(&fields, "name", "session name")? }),
            PortableActionKind::SessionRename => PortableAction::Session(SessionAction::Rename { name: required_string_action_field(&fields, "name", "session name")? }),
            PortableActionKind::SessionDetach => { ensure_action_fields(&fields, &[])?; PortableAction::Session(SessionAction::Detach) }
            PortableActionKind::SessionQuit => { ensure_action_fields(&fields, &[])?; PortableAction::Session(SessionAction::Quit) }
            PortableActionKind::SessionKill => { ensure_action_fields(&fields, &[])?; PortableAction::Session(SessionAction::Kill) }
        };
        let capabilities = kind.descriptor().capabilities;
        Ok((ActionSpec::Portable(action), capabilities))
    }
}
fn positional_fields(kind: &ActionKind) -> &'static [&'static str] {
    match kind {
        ActionKind::Portable(PortableActionKind::MenuOpen) => &["menu"],
        ActionKind::Portable(PortableActionKind::PaneSplit) => &["direction"],
        ActionKind::Portable(
            PortableActionKind::TabFocus
            | PortableActionKind::TabSwap
            | PortableActionKind::PaneFocus
            | PortableActionKind::PaneSwap,
        ) => &["index"],
        ActionKind::Portable(PortableActionKind::TabMove | PortableActionKind::PaneMove) => &["direction"],
        ActionKind::Portable(
            PortableActionKind::SessionAttach
            | PortableActionKind::SessionSwitch
            | PortableActionKind::SessionRename,
        ) => &["name"],
        _ => &[],
    }
}

fn validate_execution(
    policy: &ExecutionPolicy,
    capabilities: ExecutionCapabilities,
    span: SourceSpan,
) -> Result<(), Vec<ConfigDiagnostic>> {
    if policy.mode == ExecutionMode::Await && !capabilities.awaitable && capabilities.detachable {
        return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "action cannot be awaited", span)]);
    }
    if policy.mode == ExecutionMode::Detach && !capabilities.detachable && capabilities.awaitable {
        return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "action cannot be detached", span)]);
    }
    if policy.execution_timeout_is_cancel() && !capabilities.cancellable {
        return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "on-timeout: cancel requires a cancellable action", span)]);
    }
    if policy.on_menu_control == MenuControlAction::Cancel && !capabilities.cancellable {
        return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "on-menu-control: cancel requires a cancellable action", span)]);
    }
    Ok(())
}

trait ExecutionPolicyExt {
    fn execution_timeout_is_cancel(&self) -> bool;
}

impl ExecutionPolicyExt for ExecutionPolicy {
    fn execution_timeout_is_cancel(&self) -> bool {
        self.timeout.is_some() && self.on_timeout == TimeoutAction::Cancel
    }
}

fn validate_key_capabilities(
    required: KeyCapabilities,
    active: KeyCapabilities,
    span: &SourceSpan,
) -> Result<(), Vec<ConfigDiagnostic>> {
    let unavailable = (required.event_types && !active.event_types)
        || (required.alternate_keys && !active.alternate_keys)
        || (required.all_keys_as_escape_codes && !active.all_keys_as_escape_codes);
    unavailable
        .then(|| {
            vec![ConfigDiagnostic::error(
                DiagnosticCode::KeyCapability,
                "binding requires a disabled keyboard capability",
                span.clone(),
            )]
        })
        .map_or(Ok(()), Err)
}

fn action_fields(value: &ConfigValue) -> Result<(String, SourceSpan, Vec<ConfigField>), Vec<ConfigDiagnostic>> {
    match &value.kind {
        ConfigValueKind::String(compact) => compact_action_fields(compact, value.span.clone()),
        ConfigValueKind::Mapping(fields) => {
            let type_field = fields.iter().find(|field| field.name == "type").ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::MissingField, "action mapping requires `type`", value.span.clone())])?;
            let type_name = expect_string(&type_field.value, "action `type` must be a string")?.to_owned();
            let fields = fields
                .iter()
                .filter(|field| field.name != "type")
                .map(|field| {
                    Ok(ConfigField {
                        name: field.name.clone(),
                        name_span: field.name_span.clone(),
                        value: normalize_action_value(&field.value)?,
                    })
                })
                .collect::<Result<Vec<_>, Vec<ConfigDiagnostic>>>()?;
            Ok((type_name, type_field.value.span.clone(), fields))
        }
        _ => Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidAction, "action must be a compact string or tagged mapping", value.span.clone())]),
    }
}

fn normalize_action_value(value: &ConfigValue) -> Result<ConfigValue, Vec<ConfigDiagnostic>> {
    if let Some(reference) = context_reference(value)? {
        return Ok(ConfigValue { span: value.span.clone(), kind: ConfigValueKind::Context(reference) });
    }
    let kind = match &value.kind {
        ConfigValueKind::Sequence(values) => ConfigValueKind::Sequence(
            values.iter().map(normalize_action_value).collect::<Result<_, _>>()?,
        ),
        ConfigValueKind::Mapping(fields) => ConfigValueKind::Mapping(
            fields
                .iter()
                .map(|field| {
                    Ok(ConfigField {
                        name: field.name.clone(),
                        name_span: field.name_span.clone(),
                        value: normalize_action_value(&field.value)?,
                    })
                })
                .collect::<Result<_, Vec<ConfigDiagnostic>>>()?,
        ),
        _ => return Ok(value.clone()),
    };
    Ok(ConfigValue { span: value.span.clone(), kind })
}

fn compact_action_fields(value: &str, span: SourceSpan) -> Result<(String, SourceSpan, Vec<ConfigField>), Vec<ConfigDiagnostic>> {
    let tokens = compact_tokens(value).map_err(|message| {
        vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            message,
            span.clone(),
        )]
    })?;
    let Some((type_token, arguments)) = tokens.split_first() else {
        return Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidAction,
            "action is empty",
            span,
        )]);
    };
    let type_name = compact_string(type_token, span.clone())?;
    let kind = ActionKind::parse(&type_name).ok_or_else(|| {
        vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidAction,
            format!("unknown action `{type_name}`"),
            span.clone(),
        )]
    })?;
    let positional = positional_fields(&kind);
    let mut fields = Vec::new();
    let mut named = false;
    let mut position = 0;
    let mut seen = HashSet::new();
    for argument in arguments {
        let (name, value) = if let Some((name, raw_value)) = compact_assignment(argument) {
            if name.is_empty() {
                return Err(vec![ConfigDiagnostic::error(
                    DiagnosticCode::InvalidActionArguments,
                    "named action argument requires a field name before `=`",
                    span.clone(),
                )]);
            }
            named = true;
            (name.to_owned(), compact_action_scalar(raw_value, span.clone())?)
        } else {
            if named {
                return Err(vec![ConfigDiagnostic::error(
                    DiagnosticCode::InvalidActionArguments,
                    "positional action arguments must precede named arguments",
                    span.clone(),
                )]);
            }
            let Some(name) = positional.get(position) else {
                return Err(vec![ConfigDiagnostic::error(
                    DiagnosticCode::InvalidActionArguments,
                    "too many positional action arguments",
                    span.clone(),
                )]);
            };
            position += 1;
            ((*name).to_owned(), compact_action_scalar(argument, span.clone())?)
        };
        if !seen.insert(name.clone()) {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidActionArguments,
                format!("duplicate action argument `{name}`"),
                span.clone(),
            )]);
        }
        fields.push(ConfigField { name, name_span: span.clone(), value });
    }
    Ok((type_name, span, fields))
}

fn compact_tokens(value: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for character in value.chars() {
        if escaped {
            current.push(character);
            escaped = false;
            continue;
        }
        if character == '\\' && quote == Some('"') {
            current.push(character);
            escaped = true;
            started = true;
            continue;
        }
        if matches!(character, '\'' | '"') {
            match quote {
                Some(active) if active == character => quote = None,
                Some(_) => {}
                None => quote = Some(character),
            }
            current.push(character);
            started = true;
            continue;
        }
        if character.is_whitespace() && quote.is_none() {
            if started {
                tokens.push(std::mem::take(&mut current));
                started = false;
            }
            continue;
        }
        current.push(character);
        started = true;
    }
    if quote.is_some() || escaped {
        return Err("unterminated quoted compact action argument".to_owned());
    }
    if started {
        tokens.push(current);
    }
    Ok(tokens)
}

fn compact_assignment(token: &str) -> Option<(&str, &str)> {
    let mut quote = None;
    let mut escaped = false;
    for (offset, character) in token.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if character == '\\' && quote == Some('"') {
            escaped = true;
        } else if matches!(character, '\'' | '"') {
            match quote {
                Some(active) if active == character => quote = None,
                Some(_) => {}
                None => quote = Some(character),
            }
        } else if character == '=' && quote.is_none() {
            return Some((&token[..offset], &token[offset + character.len_utf8()..]));
        }
    }
    None
}

fn compact_string(token: &str, span: SourceSpan) -> Result<String, Vec<ConfigDiagnostic>> {
    let value = compact_scalar(token, span.clone())?;
    expect_string(&value, "action type must be a string")
        .map(str::to_owned)
        .map_err(|_| {
            vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidAction,
                "action type must be a string",
                span,
            )]
        })
}

fn compact_scalar(token: &str, span: SourceSpan) -> Result<ConfigValue, Vec<ConfigDiagnostic>> {
    let yaml = format!("value: {token}\n");
    let document = ConfigDocument::parse(SourceId::new("<compact action scalar>"), Arc::<str>::from(yaml))
        .map_err(|_| {
            vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidActionArguments,
                "compact scalar is not a valid YAML 1.2 core scalar",
                span.clone(),
            )]
        })?;
    let mut value = document
        .root
        .field("value")
        .expect("the synthetic compact-scalar mapping has its value field")
        .value
        .clone();
    if matches!(value.kind, ConfigValueKind::Sequence(_) | ConfigValueKind::Mapping(_)) {
        return Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            "compact actions accept scalar arguments only; use tagged mapping form for lists and mappings",
            span,
        )]);
    }
    value.span = span;
    Ok(value)
}

fn compact_action_scalar(token: &str, span: SourceSpan) -> Result<ConfigValue, Vec<ConfigDiagnostic>> {
    let mut value = compact_scalar(token, span.clone())?;
    if let Some(path) = token.strip_prefix('$') {
        value.kind = ConfigValueKind::Context(
            ContextReference::parse(path, span).map_err(|error| vec![error])?,
        );
    }
    Ok(value)
}

fn compile_conditions(value: Option<&ConfigValue>, pager: bool) -> Result<BindingConditions, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(BindingConditions::default()) };
    validate_fields(value, &["include", "enable", "show"])?;
    let compile = |name: &str| -> Result<Option<ConditionProgram>, Vec<ConfigDiagnostic>> {
        let Some(field) = value.field(name) else { return Ok(None) };
        let source = expect_string(&field.value, "condition must be a CEL string")?;
        let program = ConditionProgram::compile(source, field.value.span.clone()).map_err(|error| vec![error])?;
        if program.uses_pages() && !pager {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidCondition,
                "`pages.*` is available only to pager bindings",
                field.value.span.clone(),
            )]);
        }
        Ok(Some(program))
    };
    Ok(BindingConditions {
        include: compile("include")?,
        enable: compile("enable")?,
        show: compile("show")?,
    })
}

fn apply_injections(root: &mut ConfigValue) -> Result<(), Vec<ConfigDiagnostic>> {
    let injections = root.field("inject").map(|field| field.value.clone());
    let Some(injections) = injections else { return Ok(()) };
    let injections = mapping_fields(&injections, "`inject` must be an ordered mapping")?.to_vec();
    let menus = required_field_mut(root, "menus")?;
    for injection in injections {
        let spec = Injection::parse(&injection)?;
        apply_injection_to_menus(&mut menus.value, &spec)?;
    }
    Ok(())
}

#[derive(Clone)]
struct Injection {
    selector: Selector,
    defaults: bool,
    patch: ConfigValue,
}

impl Injection {
    fn parse(field: &ConfigField) -> Result<Self, Vec<ConfigDiagnostic>> {
        validate_fields(&field.value, &["select", "action"])?;
        let select = required_field(&field.value, "select")?;
        validate_fields(&select.value, &["type", "value"])?;
        let selector_type = expect_string(&required_field(&select.value, "type")?.value, "selector type must be string")?;
        let selector_value = select.value.field("value").map(|field| expect_string(&field.value, "selector value must be string").map(str::to_owned)).transpose()?;
        let selector = Selector::parse(selector_type, selector_value, select.value.span.clone())?;
        let action = required_field(&field.value, "action")?;
        validate_fields(&action.value, &["type", "bindings", "title", "tags", "settings"])?;
        let defaults = match expect_string(&required_field(&action.value, "type")?.value, "injection action type must be string")? {
            "override" => false,
            "defaults" => true,
            _ => return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidInjection, "injection action type must be `override` or `defaults`", action.value.span.clone())]),
        };
        let mut patch = action.value.clone();
        patch.remove_field("type");
        Ok(Self { selector, defaults, patch })
    }
}

#[derive(Clone)]
enum Selector {
    All,
    IdExact(String),
    IdRegex(Regex),
    TitleExact(String),
    TitleRegex(Regex),
    TagsContain(String),
}

impl Selector {
    fn parse(kind: &str, value: Option<String>, span: SourceSpan) -> Result<Self, Vec<ConfigDiagnostic>> {
        let require = |value: Option<String>| value.ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::MissingField, "selector requires `value`", span.clone())]);
        match kind {
            "all" => Ok(Self::All),
            "id:exact" => Ok(Self::IdExact(require(value)?)),
            "id:regex" => Regex::new(&require(value)?).map(Self::IdRegex).map_err(|error| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidInjection, error.to_string(), span)]),
            "title:exact" => Ok(Self::TitleExact(require(value)?)),
            "title:regex" => Regex::new(&require(value)?).map(Self::TitleRegex).map_err(|error| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidInjection, error.to_string(), span)]),
            "tags:contain" => Ok(Self::TagsContain(require(value)?)),
            _ => Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidInjection, "unknown injection selector", span)]),
        }
    }

    fn matches(&self, menu: &ConfigValue, id: Option<&str>) -> bool {
        let title = menu.field("title").and_then(|field| field.value.as_str());
        let tags = menu.field("tags").and_then(|field| match &field.value.kind { ConfigValueKind::Sequence(values) => Some(values.iter().filter_map(ConfigValue::as_str)), _ => None });
        match self {
            Self::All => true,
            Self::IdExact(expected) => id == Some(expected),
            Self::IdRegex(regex) => id.is_some_and(|id| regex.is_match(id)),
            Self::TitleExact(expected) => title == Some(expected),
            Self::TitleRegex(regex) => title.is_some_and(|title| regex.is_match(title)),
            Self::TagsContain(expected) => tags.is_some_and(|mut tags| tags.any(|tag| tag == expected)),
        }
    }
}

fn apply_injection_to_menus(menus: &mut ConfigValue, injection: &Injection) -> Result<(), Vec<ConfigDiagnostic>> {
    let menu_fields = mapping_fields_mut(menus, "`menus` must be an ordered mapping")?;
    for menu in menu_fields {
        apply_injection_to_menu(&mut menu.value, Some(&menu.name), injection)?;
    }
    Ok(())
}

fn apply_injection_to_menu(menu: &mut ConfigValue, id: Option<&str>, injection: &Injection) -> Result<(), Vec<ConfigDiagnostic>> {
    if injection.selector.matches(menu, id) {
        if injection.defaults {
            merge_defaults(menu, &injection.patch);
        } else {
            merge_values(menu, injection.patch.clone());
        }
    }
    if let Some(bindings) = menu.field_mut("bindings") {
        if let Some(bindings) = bindings.value.as_mapping_mut() {
            for binding in bindings {
                if let Some(action) = binding.value.field_mut("action") {
                    if let Some(submenu) = action.value.field_mut("submenu") {
                        apply_injection_to_menu(&mut submenu.value, None, injection)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn merge_defaults(target: &mut ConfigValue, defaults: &ConfigValue) {
    let (Some(target), Some(defaults)) = (target.as_mapping_mut(), defaults.as_mapping()) else { return };
    for default in defaults {
        if let Some(existing) = target.iter_mut().find(|field| field.name == default.name) {
            merge_defaults(&mut existing.value, &default.value);
        } else {
            target.push(default.clone());
        }
    }
}

fn collect_inline_menus(menu: &mut ConfigValue, parent: &str, next: &mut u64, output: &mut Vec<ConfigField>) {
    let mut discovered = Vec::new();
    if let Some(bindings) = menu.field_mut("bindings").and_then(|field| field.value.as_mapping_mut()) {
        for binding in bindings {
            let Some(action) = binding.value.field_mut("action") else { continue };
            let Some(submenu) = action.value.field_mut("submenu") else { continue };
            if submenu.value.as_mapping().is_none() { continue }
            let id = format!("{parent}@{}", *next);
            *next += 1;
            let marker = ConfigField { name: "_muxe_inline_id".to_owned(), name_span: submenu.value.span.clone(), value: ConfigValue::string(id.clone()) };
            submenu.value.as_mapping_mut().expect("checked").push(marker);
            let mut inline = ConfigField { name: id, name_span: submenu.name_span.clone(), value: submenu.value.clone() };
            collect_inline_menus(&mut inline.value, &inline.name, next, output);
            discovered.push(inline);
        }
    }
    output.extend(discovered);
}

fn validate_menu_cycles(menus: &[CompiledMenu]) -> Result<(), Vec<ConfigDiagnostic>> {
    let graph = menus
        .iter()
        .map(|menu| {
            let edges = menu.bindings.iter().filter_map(|binding| match &binding.action {
                ActionSpec::Portable(PortableAction::Menu(MenuAction::Open(MenuTarget::Named(target) | MenuTarget::Inline(target)))) => Some(target.clone()),
                _ => None,
            }).collect::<Vec<_>>();
            (menu.id.as_str().to_owned(), edges)
        })
        .collect::<BTreeMap<_, _>>();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for node in graph.keys() {
        if detect_cycle(node, &graph, &mut visiting, &mut visited) {
            return Err(vec![ConfigDiagnostic::error(DiagnosticCode::MenuCycle, format!("menu reference cycle includes `{node}`"), SourceSpan::new(SourceId::new("<compiled configuration>"), 0, 0))]);
        }
    }
    Ok(())
}

fn detect_cycle(
    node: &str,
    graph: &BTreeMap<String, Vec<String>>,
    visiting: &mut HashSet<String>,
    visited: &mut HashSet<String>,
) -> bool {
    if visited.contains(node) { return false }
    if !visiting.insert(node.to_owned()) { return true }
    let cycle = graph.get(node).is_some_and(|edges| edges.iter().any(|edge| detect_cycle(edge, graph, visiting, visited)));
    visiting.remove(node);
    visited.insert(node.to_owned());
    cycle
}

fn validate_context_references(value: &ConfigValue) -> Result<(), Vec<ConfigDiagnostic>> {
    if let Some(reference) = context_reference(value)? {
        let _ = reference;
        return Ok(());
    }
    match &value.kind {
        ConfigValueKind::Sequence(values) => {
            for value in values { validate_context_references(value)?; }
        }
        ConfigValueKind::Mapping(fields) => {
            for field in fields { validate_context_references(&field.value)?; }
        }
        _ => {}
    }
    Ok(())
}

fn context_reference(value: &ConfigValue) -> Result<Option<ContextReference>, Vec<ConfigDiagnostic>> {
    if let ConfigValueKind::Context(reference) = &value.kind {
        return Ok(Some(reference.clone()));
    }
    let Some(mapping) = value.as_mapping() else { return Ok(None) };
    if mapping.len() != 1 || mapping[0].name != "$context" {
        return Ok(None);
    }
    let path = expect_string(&mapping[0].value, "`$context` must be a string")?;
    ContextReference::parse(path, mapping[0].value.span.clone()).map(Some).map_err(|error| vec![error])
}

#[derive(Clone, Copy)]
enum ContextAllowance {
    StringOnly,
    Textual,
    AbsolutePath,
    WorkspaceId,
}

impl ContextAllowance {
    fn accepts(self, context_type: ContextType) -> bool {
        match self {
            Self::StringOnly => context_type == ContextType::String,
            Self::Textual => context_type != ContextType::UnsignedInteger,
            Self::AbsolutePath => context_type == ContextType::AbsolutePath,
            Self::WorkspaceId => context_type == ContextType::WorkspaceId,
        }
    }
}

fn action_scalar(value: &ConfigValue, parameter: &str) -> Result<ActionScalar, Vec<ConfigDiagnostic>> {
    match &value.kind {
        ConfigValueKind::Null
        | ConfigValueKind::Boolean(_)
        | ConfigValueKind::Integer(_)
        | ConfigValueKind::Float(_)
        | ConfigValueKind::String(_)
        | ConfigValueKind::Context(_) => Ok(ActionScalar::new(value.clone())),
        ConfigValueKind::Sequence(_) | ConfigValueKind::Mapping(_) => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            format!("{parameter} must be a scalar or typed context reference"),
            value.span.clone(),
        )]),
    }
}

fn action_string_scalar(
    value: &ConfigValue,
    parameter: &str,
    allowance: ContextAllowance,
) -> Result<ActionScalar, Vec<ConfigDiagnostic>> {
    let scalar = action_scalar(value, parameter)?;
    match &scalar.value.kind {
        ConfigValueKind::String(_) => Ok(scalar),
        ConfigValueKind::Context(reference) if allowance.accepts(reference.expected_type()) => Ok(scalar),
        ConfigValueKind::Context(_) => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::ContextTypeMismatch,
            format!("{parameter} has an incompatible context reference type"),
            scalar.value.span.clone(),
        )]),
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            format!("{parameter} must be a string"),
            scalar.value.span.clone(),
        )]),
    }
}


fn action_direction_scalar(value: &ConfigValue, parameter: &str) -> Result<ActionScalar, Vec<ConfigDiagnostic>> {
    let scalar = action_string_scalar(value, parameter, ContextAllowance::StringOnly)?;
    if let ConfigValueKind::String(value) = &scalar.value.kind {
        if !matches!(value.as_str(), "left" | "right" | "up" | "down" | "next" | "previous") {
            return Err(vec![ConfigDiagnostic::error(
                DiagnosticCode::InvalidActionArguments,
                "direction must be left, right, up, down, next, or previous",
                scalar.value.span.clone(),
            )]);
        }
    }
    Ok(scalar)
}

fn action_bool_scalar(value: &ConfigValue, parameter: &str) -> Result<ActionScalar, Vec<ConfigDiagnostic>> {
    let scalar = action_scalar(value, parameter)?;
    match scalar.value.kind {
        ConfigValueKind::Boolean(_) => Ok(scalar),
        ConfigValueKind::Context(_) => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::ContextTypeMismatch,
            format!("{parameter} has no boolean context reference in v1"),
            scalar.value.span.clone(),
        )]),
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            format!("{parameter} must be boolean"),
            scalar.value.span.clone(),
        )]),
    }
}

fn action_index_scalar(value: &ConfigValue) -> Result<ActionScalar, Vec<ConfigDiagnostic>> {
    let scalar = action_scalar(value, "index")?;
    match &scalar.value.kind {
        ConfigValueKind::Integer(value) if *value >= 0 => Ok(scalar),
        ConfigValueKind::Context(reference) if reference.expected_type() == ContextType::UnsignedInteger => Ok(scalar),
        ConfigValueKind::Context(_) => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::ContextTypeMismatch,
            "index requires an unsigned-integer context reference",
            scalar.value.span.clone(),
        )]),
        _ => Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            "index must be a non-negative integer",
            scalar.value.span.clone(),
        )]),
    }
}
fn index_or_direction(fields: &[ConfigField]) -> Result<IndexOrDirection, Vec<ConfigDiagnostic>> {
    ensure_action_fields(fields, &["index", "direction"])?;
    match (field_named(fields, "index"), field_named(fields, "direction")) {
        (Some(index), None) => Ok(IndexOrDirection::Index(action_index_scalar(&index.value)?)),
        (None, Some(direction)) => Ok(IndexOrDirection::Direction(action_direction_scalar(&direction.value, "direction")?)),
        _ => Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidActionArguments, "action requires exactly one of `index` or `direction`", fields.first().map_or_else(|| SourceSpan::new(SourceId::new("<compact action>"), 0, 0), |field| field.value.span.clone()))]),
    }
}

fn optional_action_bool_field(fields: &[ConfigField], name: &str) -> Result<Option<ActionScalar>, Vec<ConfigDiagnostic>> {
    ensure_action_fields(fields, &[name])?;
    field_named(fields, name).map(|field| action_bool_scalar(&field.value, name)).transpose()
}

fn action_key_sequence(value: &ConfigValue) -> Result<Vec<ActionScalar>, Vec<ConfigDiagnostic>> {
    let ConfigValueKind::Sequence(values) = &value.kind else { return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidActionArguments, "keys must be an array", value.span.clone())]) };
    values
        .iter()
        .map(|value| {
            let scalar = action_string_scalar(value, "keyboard key", ContextAllowance::StringOnly)?;
            if let ConfigValueKind::String(key) = &scalar.value.kind {
                CanonicalKey::parse_diagnostic(key, scalar.value.span.clone()).map_err(|error| vec![error])?;
            }
            Ok(scalar)
        })
        .collect()
}

fn action_string_sequence(
    value: Option<&ConfigValue>,
    parameter: &str,
    allowance: ContextAllowance,
) -> Result<Vec<ActionScalar>, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(Vec::new()) };
    let ConfigValueKind::Sequence(values) = &value.kind else {
        return Err(vec![ConfigDiagnostic::error(
            DiagnosticCode::InvalidActionArguments,
            format!("{parameter} must be an array"),
            value.span.clone(),
        )]);
    };
    values
        .iter()
        .map(|value| action_string_scalar(value, parameter, allowance))
        .collect()
}

fn action_string_mapping(
    value: Option<&ConfigValue>,
    parameter: &str,
    allowance: ContextAllowance,
) -> Result<BTreeMap<String, ActionScalar>, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(BTreeMap::new()) };
    mapping_fields(value, &format!("{parameter} must be a mapping"))?
        .iter()
        .map(|field| {
            Ok((
                field.name.clone(),
                action_string_scalar(&field.value, parameter, allowance)?,
            ))
        })
        .collect()
}

fn string_sequence(value: Option<&ConfigValue>, name: &str) -> Result<Vec<String>, Vec<ConfigDiagnostic>> {
    let Some(value) = value else { return Ok(Vec::new()) };
    let ConfigValueKind::Sequence(values) = &value.kind else { return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, format!("{name} must be an array"), value.span.clone())]) };
    values.iter().map(|value| expect_string(value, format!("{name} values must be strings")).map(str::to_owned)).collect()
}

fn scalar_text(value: &ConfigValue) -> Result<String, Vec<ConfigDiagnostic>> {
    Ok(match &value.kind {
        ConfigValueKind::String(value) => value.clone(),
        ConfigValueKind::Integer(value) => value.to_string(),
        ConfigValueKind::Float(value) => value.to_string(),
        ConfigValueKind::Boolean(value) => value.to_string(),
        _ => return Err(vec![ConfigDiagnostic::error(DiagnosticCode::InvalidActionArguments, "expected scalar", value.span.clone())]),
    })
}

fn optional_duration(value: &ConfigValue, name: &str, off_allowed: bool) -> Result<Option<Duration>, Vec<ConfigDiagnostic>> {
    value
        .field(name)
        .map(|field| parse_duration_value(&field.value, off_allowed))
        .transpose()
        .map(Option::flatten)
}

fn parse_duration_value(value: &ConfigValue, off_allowed: bool) -> Result<Option<Duration>, Vec<ConfigDiagnostic>> {
    let text = expect_string(value, "duration must be a string")?;
    if text == "off" {
        return off_allowed.then_some(None).ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "`off` is not valid here", value.span.clone())]);
    }
    let parse = |suffix: &str, multiplier: u64| text.strip_suffix(suffix).and_then(|number| number.parse::<u64>().ok()).and_then(|number| number.checked_mul(multiplier)).map(Duration::from_millis);
    parse("ms", 1).or_else(|| parse("s", 1_000)).or_else(|| parse("m", 60_000)).map(Some).ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, "duration must use ms, s, m, or off", value.span.clone())])
}

fn required_field<'a>(value: &'a ConfigValue, name: &str) -> Result<&'a ConfigField, Vec<ConfigDiagnostic>> {
    value.field(name).ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::MissingField, format!("missing required `{name}`"), value.span.clone())])
}

fn required_field_mut<'a>(value: &'a mut ConfigValue, name: &str) -> Result<&'a mut ConfigField, Vec<ConfigDiagnostic>> {
    let span = value.span.clone();
    value.field_mut(name).ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::MissingField, format!("missing required `{name}`"), span)])
}

fn required_action_field<'a>(fields: &'a [ConfigField], name: &str) -> Result<&'a ConfigField, Vec<ConfigDiagnostic>> {
    field_named(fields, name).ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::MissingField, format!("action requires `{name}`"), fields.first().map_or_else(|| SourceSpan::new(SourceId::new("<compact action>"), 0, 0), |field| field.value.span.clone()))])
}

fn required_string_action_field(
    fields: &[ConfigField],
    name: &str,
    parameter: &str,
) -> Result<ActionScalar, Vec<ConfigDiagnostic>> {
    ensure_action_fields(fields, &[name])?;
    action_string_scalar(
        &required_action_field(fields, name)?.value,
        parameter,
        ContextAllowance::Textual,
    )
}

fn field_named<'a>(fields: &'a [ConfigField], name: &str) -> Option<&'a ConfigField> {
    fields.iter().find(|field| field.name == name)
}

fn expect_string<'a>(value: &'a ConfigValue, message: impl Into<String>) -> Result<&'a str, Vec<ConfigDiagnostic>> {
    value.as_str().ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, message.into(), value.span.clone())])
}

fn optional_string<'a>(value: &'a ConfigValue, name: &str) -> Result<Option<&'a str>, Vec<ConfigDiagnostic>> {
    value.field(name).map(|field| expect_string(&field.value, format!("`{name}` must be a string"))).transpose()
}

fn expect_bool(value: &ConfigValue, message: impl Into<String>) -> Result<bool, Vec<ConfigDiagnostic>> {
    value.as_bool().ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, message.into(), value.span.clone())])
}

fn mapping_fields<'a>(value: &'a ConfigValue, message: &str) -> Result<&'a [ConfigField], Vec<ConfigDiagnostic>> {
    value.as_mapping().ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, message, value.span.clone())])
}

fn mapping_fields_mut<'a>(value: &'a mut ConfigValue, message: &str) -> Result<&'a mut Vec<ConfigField>, Vec<ConfigDiagnostic>> {
    let span = value.span.clone();
    value.as_mapping_mut().ok_or_else(|| vec![ConfigDiagnostic::error(DiagnosticCode::InvalidValue, message, span)])
}

fn validate_fields(value: &ConfigValue, allowed: &[&str]) -> Result<(), Vec<ConfigDiagnostic>> {
    let fields = mapping_fields(value, "expected a mapping")?;
    let mut errors = Vec::new();
    for field in fields {
        if !allowed.contains(&field.name.as_str()) && !field.name.starts_with('_') {
            let suggestion = nearest(&field.name, allowed);
            let diagnostic = ConfigDiagnostic::error(DiagnosticCode::UnknownField, format!("unknown field `{}`", field.name), field.name_span.clone());
            errors.push(if let Some(suggestion) = suggestion { diagnostic.with_help(format!("did you mean `{suggestion}`?")) } else { diagnostic });
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

fn ensure_action_fields(fields: &[ConfigField], allowed: &[&str]) -> Result<(), Vec<ConfigDiagnostic>> {
    let mut errors = Vec::new();
    for field in fields {
        if !allowed.contains(&field.name.as_str()) {
            errors.push(ConfigDiagnostic::error(DiagnosticCode::InvalidActionArguments, format!("unknown action argument `{}`", field.name), field.name_span.clone()));
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

fn nearest<'a>(value: &str, options: &'a [&str]) -> Option<&'a str> {
    options.iter().copied().min_by_key(|option| edit_distance(value, option)).filter(|option| edit_distance(value, option) <= 3)
}

fn edit_distance(left: &str, right: &str) -> usize {
    let mut row = (0..=right.chars().count()).collect::<Vec<_>>();
    for (left_index, left_character) in left.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = left_index + 1;
        for (right_index, right_character) in right.chars().enumerate() {
            let previous = row[right_index + 1];
            row[right_index + 1] = (row[right_index + 1] + 1)
                .min(row[right_index] + 1)
                .min(diagonal + usize::from(left_character != right_character));
            diagonal = previous;
        }
    }
    row[right.chars().count()]
}
