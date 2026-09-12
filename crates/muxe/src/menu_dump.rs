//! Stable JSON rendering for fully compiled menu configuration.

use std::{borrow::Cow, collections::BTreeSet, time::Duration};

use muxe_core::{
    ActionScalar, ActionSpec, AfterAction, BindingConditions, CommandAction, CompiledBinding,
    CompiledConfig, CompiledMenu, ConfigValue, ConfigValueKind, CreateCommand, ExecutionMode,
    IndexOrDirection, KeyboardAction, MenuAction, MenuControlAction, MenuId, MenuTarget,
    PaneAction, PortableAction, SessionAction, TabAction, TimeoutAction,
};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use thiserror::Error;

/// Selects either one named menu or every named menu in an effective configuration.
#[derive(Clone, Copy, Debug)]
pub enum MenuDumpSelection<'a> {
    Menu(&'a MenuId),
    All,
}

/// A failure to render a compiled menu graph.
#[derive(Debug, Error)]
pub enum MenuDumpError {
    #[error("unknown menu `{0}`")]
    UnknownMenu(String),
    #[error("compiled inline menu target `{0}` is missing")]
    MissingInlineMenu(String),
    #[error("could not encode the effective menu as JSON: {0}")]
    Encode(#[from] serde_json::Error),
}

/// Renders effective menu contents as pretty JSON.
///
/// A single-menu dump is the menu object itself. An all-menu dump is an object
/// keyed by named-menu ID. Compiler-generated inline menu IDs never appear as
/// top-level keys; inline contents remain nested under their `menu:open` action.
/// Ordered menu and binding mappings retain compilation order.
///
/// # Errors
///
/// Returns [`MenuDumpError::UnknownMenu`] for an absent requested menu, or an
/// encoding/invariant error when the compiled graph cannot be represented.
pub fn render(
    config: &CompiledConfig,
    selection: MenuDumpSelection<'_>,
) -> Result<String, MenuDumpError> {
    let node = match selection {
        MenuDumpSelection::Menu(id) => {
            let menu = config
                .menu(id)
                .ok_or_else(|| MenuDumpError::UnknownMenu(id.as_str().to_owned()))?;
            menu_node(config, menu)?
        }
        MenuDumpSelection::All => {
            let inline = inline_menu_ids(config);
            let mut menus = Vec::with_capacity(config.menus.len().saturating_sub(inline.len()));
            for menu in &config.menus {
                if !inline.contains(menu.id.as_str()) {
                    menus.push((Cow::Borrowed(menu.id.as_str()), menu_node(config, menu)?));
                }
            }
            JsonNode::Object(menus)
        }
    };
    serde_json::to_string_pretty(&node).map_err(MenuDumpError::from)
}

fn inline_menu_ids(config: &CompiledConfig) -> BTreeSet<&str> {
    config
        .menus
        .iter()
        .flat_map(|menu| &menu.bindings)
        .filter_map(|binding| match &binding.action {
            ActionSpec::Portable(PortableAction::Menu(MenuAction::Open(MenuTarget::Inline(
                target,
            )))) => Some(target.as_str()),
            _ => None,
        })
        .collect()
}

fn find_menu<'a>(config: &'a CompiledConfig, id: &str) -> Option<&'a CompiledMenu> {
    config.menus.iter().find(|menu| menu.id.as_str() == id)
}

fn menu_node<'a>(
    config: &'a CompiledConfig,
    menu: &'a CompiledMenu,
) -> Result<JsonNode<'a>, MenuDumpError> {
    let mut bindings = Vec::with_capacity(menu.bindings.len());
    for binding in &menu.bindings {
        bindings.push((
            Cow::Owned(binding.key.canonical_string()),
            binding_node(config, binding)?,
        ));
    }
    Ok(JsonNode::Object(vec![
        field(
            "title",
            menu.title.as_deref().map_or(JsonNode::Null, string),
        ),
        field(
            "tags",
            JsonNode::Array(menu.tags.iter().map(|tag| string(tag)).collect()),
        ),
        field(
            "settings",
            JsonNode::Object(vec![field(
                "timeout",
                duration_node(menu.inactivity_timeout),
            )]),
        ),
        field("layout", layout_node(menu)),
        field("bindings", JsonNode::Object(bindings)),
    ]))
}

fn layout_node(menu: &CompiledMenu) -> JsonNode<'_> {
    let padding = menu.layout.padding;
    JsonNode::Object(vec![
        field(
            "padding",
            JsonNode::Object(vec![
                field("left", JsonNode::Unsigned(u64::from(padding.left))),
                field("right", JsonNode::Unsigned(u64::from(padding.right))),
                field("top", JsonNode::Unsigned(u64::from(padding.top))),
                field("bottom", JsonNode::Unsigned(u64::from(padding.bottom))),
                field(
                    "between-rows",
                    JsonNode::Unsigned(u64::from(padding.between_rows)),
                ),
                field(
                    "between-columns",
                    JsonNode::Unsigned(u64::from(padding.between_columns)),
                ),
            ]),
        ),
        field(
            "max-item-title-length",
            JsonNode::Unsigned(u64::from(menu.layout.max_item_title_length)),
        ),
    ])
}

fn binding_node<'a>(
    config: &'a CompiledConfig,
    binding: &'a CompiledBinding,
) -> Result<JsonNode<'a>, MenuDumpError> {
    Ok(JsonNode::Object(vec![
        field(
            "label",
            binding.label.as_deref().map_or(JsonNode::Null, string),
        ),
        field("hidden", JsonNode::Bool(binding.hidden)),
        field("action", action_node(config, &binding.action)?),
        field("settings", settings_node(binding)),
        field("conditions", conditions_node(&binding.conditions)),
    ]))
}

fn settings_node(binding: &CompiledBinding) -> JsonNode<'_> {
    let settings = &binding.settings;
    JsonNode::Object(vec![
        field(
            "after_action",
            string(match settings.after_action {
                AfterAction::Quit => "quit",
                AfterAction::Return => "return",
                AfterAction::Stay => "stay",
            }),
        ),
        field(
            "execution",
            JsonNode::Object(vec![
                field(
                    "mode",
                    string(match settings.execution.mode {
                        ExecutionMode::Await => "await",
                        ExecutionMode::Detach => "detach",
                    }),
                ),
                field("timeout", duration_node(settings.execution.timeout)),
                field(
                    "on-timeout",
                    string(match settings.execution.on_timeout {
                        TimeoutAction::Detach => "detach",
                        TimeoutAction::Cancel => "cancel",
                    }),
                ),
                field(
                    "on-menu-control",
                    string(match settings.execution.on_menu_control {
                        MenuControlAction::Detach => "detach",
                        MenuControlAction::Cancel => "cancel",
                    }),
                ),
            ]),
        ),
        field(
            "repeat",
            settings.repeat.map_or(JsonNode::Null, JsonNode::Bool),
        ),
    ])
}

fn duration_node(duration: Option<Duration>) -> JsonNode<'static> {
    let Some(duration) = duration else {
        return string("off");
    };
    let millis = duration.as_millis();
    let value = if millis != 0 && millis % 60_000 == 0 {
        format!("{}m", millis / 60_000)
    } else if millis != 0 && millis % 1_000 == 0 {
        format!("{}s", millis / 1_000)
    } else {
        format!("{millis}ms")
    };
    JsonNode::String(Cow::Owned(value))
}

fn conditions_node(conditions: &BindingConditions) -> JsonNode<'_> {
    JsonNode::Object(vec![
        field(
            "include",
            string(
                conditions
                    .include
                    .as_ref()
                    .map_or("true", |condition| condition.source()),
            ),
        ),
        field(
            "enable",
            string(
                conditions
                    .enable
                    .as_ref()
                    .map_or("true", |condition| condition.source()),
            ),
        ),
        field(
            "show",
            string(
                conditions
                    .show
                    .as_ref()
                    .map_or("true", |condition| condition.source()),
            ),
        ),
    ])
}

fn action_node<'a>(
    config: &'a CompiledConfig,
    action: &'a ActionSpec,
) -> Result<JsonNode<'a>, MenuDumpError> {
    match action {
        ActionSpec::Native(action) => {
            let mut fields = Vec::with_capacity(action.fields.len() + 1);
            fields.push(field("type", string(&action.type_name)));
            fields.extend(action.fields.iter().map(|field_value| {
                (
                    Cow::Borrowed(field_value.name.as_str()),
                    config_value_node(&field_value.value),
                )
            }));
            Ok(JsonNode::Object(fields))
        }
        ActionSpec::Portable(action) => portable_action_node(config, action),
    }
}

fn portable_action_node<'a>(
    config: &'a CompiledConfig,
    action: &'a PortableAction,
) -> Result<JsonNode<'a>, MenuDumpError> {
    let mut fields = vec![field("type", string(action.kind().as_str()))];
    match action {
        PortableAction::Menu(action) => {
            push_menu_action_fields(config, &mut fields, action)?;
        }
        PortableAction::Config(_) => {}
        PortableAction::Keyboard(action) => push_keyboard_action_fields(&mut fields, action),
        PortableAction::Command(action) => push_command_action_fields(&mut fields, action),
        PortableAction::Tab(action) => push_tab_action_fields(&mut fields, action),
        PortableAction::Pane(action) => push_pane_action_fields(&mut fields, action),
        PortableAction::Session(action) => push_session_action_fields(&mut fields, action),
    }
    Ok(JsonNode::Object(fields))
}

fn push_menu_action_fields<'a>(
    config: &'a CompiledConfig,
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    action: &'a MenuAction,
) -> Result<(), MenuDumpError> {
    match action {
        MenuAction::Open(MenuTarget::Named(target)) => {
            fields.push(field("menu", string(target)));
        }
        MenuAction::Open(MenuTarget::Inline(target)) => {
            let submenu = find_menu(config, target)
                .ok_or_else(|| MenuDumpError::MissingInlineMenu(target.clone()))?;
            fields.push(field("submenu", menu_node(config, submenu)?));
        }
        MenuAction::Return | MenuAction::Quit | MenuAction::PagePrev | MenuAction::PageNext => {}
    }
    Ok(())
}

fn push_keyboard_action_fields<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    action: &'a KeyboardAction,
) {
    match action {
        KeyboardAction::SendKeys(keys) => {
            fields.push(field(
                "keys",
                JsonNode::Array(keys.iter().map(scalar_node).collect()),
            ));
        }
        KeyboardAction::SendText(text) => fields.push(field("text", scalar_node(text))),
    }
}

fn push_command_action_fields<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    action: &'a CommandAction,
) {
    fields.push(field("program", scalar_node(&action.program)));
    fields.push(field(
        "args",
        JsonNode::Array(action.args.iter().map(scalar_node).collect()),
    ));
    fields.push(field("cwd", optional_scalar_node(action.cwd.as_ref())));
    fields.push(field(
        "env",
        JsonNode::Object(
            action
                .env
                .iter()
                .map(|(name, value)| (Cow::Borrowed(name.as_str()), scalar_node(value)))
                .collect(),
        ),
    ));
}

fn push_tab_action_fields<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    action: &'a TabAction,
) {
    match action {
        TabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        } => {
            fields.push(field(
                "workspace-id",
                optional_scalar_node(workspace_id.as_ref()),
            ));
            fields.push(field("name", optional_scalar_node(name.as_ref())));
            fields.push(field(
                "focus",
                focus.as_ref().map_or(JsonNode::Bool(true), scalar_node),
            ));
            push_create_command_fields(fields, command);
        }
        TabAction::Close => {}
        TabAction::Rename { name } => {
            fields.push(field("name", optional_scalar_node(name.as_ref())));
        }
        TabAction::Focus(target) | TabAction::Move(target) | TabAction::Swap(target) => {
            push_target_field(fields, target);
        }
    }
}

fn push_pane_action_fields<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    action: &'a PaneAction,
) {
    match action {
        PaneAction::Create | PaneAction::Close => {}
        PaneAction::Split {
            direction,
            focus,
            command,
        } => {
            fields.push(field("direction", optional_scalar_node(direction.as_ref())));
            fields.push(field(
                "focus",
                focus.as_ref().map_or(JsonNode::Bool(true), scalar_node),
            ));
            push_create_command_fields(fields, command);
        }
        PaneAction::Focus(target) | PaneAction::Move(target) | PaneAction::Swap(target) => {
            push_target_field(fields, target);
        }
        PaneAction::Resize { direction, amount } => {
            fields.push(field("direction", scalar_node(direction)));
            fields.push(field("amount", optional_scalar_node(amount.as_ref())));
        }
        PaneAction::Zoom { enabled }
        | PaneAction::Fullscreen { enabled }
        | PaneAction::Floating { enabled } => {
            fields.push(field("enabled", optional_scalar_node(enabled.as_ref())));
        }
        PaneAction::Frame { visible } => {
            fields.push(field("visible", optional_scalar_node(visible.as_ref())));
        }
    }
}

fn push_session_action_fields<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    action: &'a SessionAction,
) {
    match action {
        SessionAction::Attach { name }
        | SessionAction::Switch { name }
        | SessionAction::Rename { name } => fields.push(field("name", scalar_node(name))),
        SessionAction::Create
        | SessionAction::Detach
        | SessionAction::Quit
        | SessionAction::Kill => {}
    }
}

fn push_create_command_fields<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    command: &'a CreateCommand,
) {
    fields.push(field(
        "program",
        optional_scalar_node(command.program.as_ref()),
    ));
    fields.push(field(
        "args",
        JsonNode::Array(command.args.iter().map(scalar_node).collect()),
    ));
    fields.push(field("cwd", optional_scalar_node(command.cwd.as_ref())));
}

fn push_target_field<'a>(
    fields: &mut Vec<(Cow<'a, str>, JsonNode<'a>)>,
    target: &'a IndexOrDirection,
) {
    match target {
        IndexOrDirection::Index(index) => fields.push(field("index", scalar_node(index))),
        IndexOrDirection::Direction(direction) => {
            fields.push(field("direction", scalar_node(direction)));
        }
    }
}

fn optional_scalar_node(value: Option<&ActionScalar>) -> JsonNode<'_> {
    value.map_or(JsonNode::Null, scalar_node)
}

fn scalar_node(value: &ActionScalar) -> JsonNode<'_> {
    config_value_node(&value.value)
}

fn config_value_node(value: &ConfigValue) -> JsonNode<'_> {
    match &value.kind {
        ConfigValueKind::Null => JsonNode::Null,
        ConfigValueKind::Boolean(value) => JsonNode::Bool(*value),
        ConfigValueKind::Integer(value) => JsonNode::Integer(*value),
        ConfigValueKind::Float(value) => JsonNode::Float(*value),
        ConfigValueKind::String(value) => string(value),
        ConfigValueKind::Context(reference) => {
            JsonNode::Object(vec![field("$context", string(reference.path.as_str()))])
        }
        ConfigValueKind::Sequence(values) => {
            JsonNode::Array(values.iter().map(config_value_node).collect())
        }
        ConfigValueKind::Mapping(fields) => JsonNode::Object(
            fields
                .iter()
                .map(|field_value| {
                    (
                        Cow::Borrowed(field_value.name.as_str()),
                        config_value_node(&field_value.value),
                    )
                })
                .collect(),
        ),
    }
}

fn field<'a>(name: &'static str, value: JsonNode<'a>) -> (Cow<'a, str>, JsonNode<'a>) {
    (Cow::Borrowed(name), value)
}

fn string(value: &str) -> JsonNode<'_> {
    JsonNode::String(Cow::Borrowed(value))
}

enum JsonNode<'a> {
    Null,
    Bool(bool),
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    String(Cow<'a, str>),
    Array(Vec<Self>),
    Object(Vec<(Cow<'a, str>, Self)>),
}

impl Serialize for JsonNode<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Integer(value) => serializer.serialize_i64(*value),
            Self::Unsigned(value) => serializer.serialize_u64(*value),
            Self::Float(value) => serializer.serialize_f64(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(value)?;
                }
                sequence.end()
            }
            Self::Object(fields) => {
                let mut mapping = serializer.serialize_map(Some(fields.len()))?;
                for (name, value) in fields {
                    mapping.serialize_entry(name, value)?;
                }
                mapping.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use muxe_core::{CompiledGeneration, KeyCapabilities, SourceId, compile_yaml};
    use serde_json::Value;

    use super::*;

    fn compiled() -> CompiledConfig {
        compile_yaml(
            CompiledGeneration(1),
            SourceId::new("<menu dump test>"),
            r"
version: 1
settings:
  timeout: 30s
menus:
  main:
    title: Main
    tags: [root]
    bindings:
      x:
        label: tools
        action:
          type: menu:open
          submenu:
            title: Tools
            settings:
              timeout: 2m
            bindings:
              n:
                label: nested
                action:
                  type: menu:open
                  submenu:
                    bindings:
                      q:
                        label: quit
                        action: menu:quit
      c:
        label: command
        action:
          type: command:execute
          program: printf
          args: [hello]
  named:
    bindings:
      t:
        label: new tab
        action: tab:create
inject:
  Tools.tag:
    select:
      type: title:exact
      value: Tools
    action:
      type: defaults
      tags: [injected]
",
            KeyCapabilities::default(),
            None,
        )
        .expect("representative menu config compiles")
    }

    #[test]
    fn single_dump_nests_fully_compiled_inline_menus() {
        let config = compiled();
        let output =
            render(&config, MenuDumpSelection::Menu(&MenuId::new("main"))).expect("menu renders");
        let value: Value = serde_json::from_str(&output).expect("valid JSON");

        let inline = &value["bindings"]["x"]["action"]["submenu"];
        assert_eq!(inline["title"], "Tools");
        assert_eq!(inline["tags"], serde_json::json!(["injected"]));
        assert_eq!(value["settings"]["timeout"], "30s");
        assert_eq!(inline["settings"]["timeout"], "2m");
        assert_eq!(inline["bindings"]["esc"]["action"]["type"], "menu:quit");
        assert_eq!(
            inline["bindings"]["n"]["action"]["submenu"]["bindings"]["backspace"]["action"]["type"],
            "menu:return"
        );
        assert_eq!(
            value["bindings"]["c"]["settings"]["execution"]["mode"],
            "detach"
        );

        let x = output.find("\n    \"x\": {").expect("x binding");
        let c = output.find("\n    \"c\": {").expect("c binding");
        assert!(x < c, "binding order must remain effective-config order");
    }

    #[test]
    fn all_dump_keys_only_named_menus_and_materializes_action_defaults() {
        let config = compiled();
        let output = render(&config, MenuDumpSelection::All).expect("menus render");
        let value: Value = serde_json::from_str(&output).expect("valid JSON");
        let menus = value.as_object().expect("all dump is an object");

        assert_eq!(menus.len(), 2);
        assert!(menus.contains_key("main"));
        assert!(menus.contains_key("named"));
        assert!(menus.keys().all(|name| !name.contains('@')));
        assert_eq!(menus["named"]["bindings"]["t"]["action"]["focus"], true);
    }

    #[test]
    fn single_dump_rejects_an_unknown_menu() {
        let error = render(
            &compiled(),
            MenuDumpSelection::Menu(&MenuId::new("missing")),
        )
        .expect_err("unknown menu must fail");
        assert!(matches!(error, MenuDumpError::UnknownMenu(name) if name == "missing"));
    }
}
