use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use muxe_core::{
    ActionCategory, ActionConstraint, ActionParameterName, ActionParameterType, CanonicalKey,
    KeyCapabilities, KeyIdentity, KeyIdentitySource, Modifiers, NamedKey, OriginHostKind,
    OriginInvocationSource, OriginPaneType, PortableActionKind,
};
use serde::Deserialize;
use strum::{Display, EnumString, IntoEnumIterator};

struct ActionCatalog {
    actions: BTreeMap<PortableActionKind, ActionDescription>,
}

struct ActionDescription {
    summary: String,
    parameters: BTreeMap<ActionParameterName, String>,
}

#[derive(Debug, Deserialize)]
struct RawActionCatalog {
    actions: BTreeMap<String, RawActionDescription>,
}

#[derive(Debug, Deserialize)]
struct RawActionDescription {
    summary: String,
    #[serde(default)]
    parameters: BTreeMap<String, String>,
}

struct HostSupport {
    rows: BTreeMap<PortableActionKind, HostSupportRow>,
}

struct HostSupportRow {
    alert: HostAlert,
    zellij: Option<String>,
    herdr: Option<String>,
}

#[derive(Clone, Copy, Display, EnumString)]
enum HostAlert {
    #[strum(serialize = "none")]
    None,
    #[strum(serialize = "note")]
    Note,
    #[strum(serialize = "warning")]
    Warning,
    #[strum(serialize = "caution")]
    Caution,
}

impl HostAlert {
    const fn markdown_name(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Note => Some("NOTE"),
            Self::Warning => Some("WARNING"),
            Self::Caution => Some("CAUTION"),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let Some(output) = arguments.next() else {
        return Err(
            "usage: generate_reference <output-path> <host-support.tsv> [action-descriptions.yaml]"
                .into(),
        );
    };
    let Some(host_support_path) = arguments.next() else {
        return Err(
            "usage: generate_reference <output-path> <host-support.tsv> [action-descriptions.yaml]"
                .into(),
        );
    };
    let catalog_path = arguments.next().map_or_else(
        || PathBuf::from("reference/action-descriptions.yaml"),
        PathBuf::from,
    );
    if arguments.next().is_some() {
        return Err(
            "usage: generate_reference <output-path> <host-support.tsv> [action-descriptions.yaml]"
                .into(),
        );
    }

    let host_support = load_host_support(&PathBuf::from(host_support_path))?;
    let catalog = load_action_catalog(&catalog_path)?;
    fs::write(PathBuf::from(output), render(&host_support, &catalog))?;
    Ok(())
}

fn load_action_catalog(path: &PathBuf) -> Result<ActionCatalog, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let raw: RawActionCatalog = serde_saphyr::from_str(&contents)?;
    let mut actions = BTreeMap::new();
    for (action_name, raw_description) in raw.actions {
        let action = action_name.parse::<PortableActionKind>().map_err(|_| {
            format!(
                "{}: unknown action `{action_name}` in action descriptions",
                path.display()
            )
        })?;
        let mut parameters = BTreeMap::new();
        for (parameter_name, description) in raw_description.parameters {
            let parameter = parameter_name.parse::<ActionParameterName>().map_err(|_| {
                format!(
                    "{}: unknown parameter `{parameter_name}` for `{action_name}`",
                    path.display()
                )
            })?;
            if parameters.insert(parameter, description).is_some() {
                return Err(format!(
                    "{}: duplicate parameter `{parameter_name}` for `{action_name}`",
                    path.display()
                )
                .into());
            }
        }
        let expected = action
            .schema()
            .parameters
            .iter()
            .filter(|parameter| parameter.value_type != ActionParameterType::Unsupported)
            .map(|parameter| parameter.name)
            .collect::<Vec<_>>();
        for parameter in &expected {
            if !parameters.contains_key(parameter) {
                return Err(format!(
                    "{}: missing description for `{}.{}`",
                    path.display(),
                    action,
                    parameter
                )
                .into());
            }
        }
        for parameter in parameters.keys() {
            if !expected.contains(parameter) {
                return Err(format!(
                    "{}: `{}` has no parameter `{}`",
                    path.display(),
                    action,
                    parameter
                )
                .into());
            }
        }
        if actions
            .insert(
                action,
                ActionDescription {
                    summary: raw_description.summary,
                    parameters,
                },
            )
            .is_some()
        {
            return Err(format!("{}: duplicate action `{action}`", path.display()).into());
        }
    }
    for action in PortableActionKind::iter() {
        if !actions.contains_key(&action) {
            return Err(format!(
                "{}: missing action description for `{action}`",
                path.display()
            )
            .into());
        }
    }
    Ok(ActionCatalog { actions })
}

#[expect(
    clippy::too_many_lines,
    reason = "the generated reference keeps its ordered document sections together"
)]
fn render(host_support: &HostSupport, catalog: &ActionCatalog) -> String {
    let mut output = String::new();
    writeln!(output, "# Muxe action reference\n").unwrap();
    writeln!(
        output,
        "> Generated by `cargo run --locked -p muxe-core --bin generate_reference -- REFERENCE.md reference/host-support.tsv reference/action-descriptions.yaml`. Do not edit this file by hand.\n"
    )
    .unwrap();
    writeln!(
        output,
        "Use actions in menu bindings to tell Muxe what to do when you press a key. Actions can open another menu, run a command, or control a tab, pane, or session."
    )
    .unwrap();

    writeln!(output, "\n## How Muxe handles actions\n").unwrap();
    writeln!(
        output,
        "When Muxe loads its configuration, it checks each action name, parameter, and value. It also checks whether Zellij or Herdr supports the requested operation. If an action is invalid or unsupported, Muxe reports an error and does not load that configuration."
    )
    .unwrap();
    writeln!(
        output,
        "\nMuxe remembers which session, tab, and pane were active when you opened the menu. In compact actions, references such as `$origin.pane.cwd` read values from that saved location. The mapping form writes the same reference as `{{ $context: origin.pane.cwd }}`."
    )
    .unwrap();
    writeln!(
        output,
        "\nMuxe rejects the configuration if a reference has a type that the parameter cannot use. Some saved values are optional and may be unavailable. If an optional value is unavailable when the binding runs, Muxe reports an error for that action and leaves the configuration loaded."
    )
    .unwrap();

    writeln!(output, "\n## Writing an action\n").unwrap();
    writeln!(
        output,
        "Write an action as a YAML mapping or as a compact string. The mapping form supports strings, lists, mappings, and `$context` references. The compact form accepts an action name followed by positional or `name=value` arguments."
    )
    .unwrap();
    writeln!(
        output,
        "\nIn compact form, an unquoted value that begins with `$origin.` is a reference. Quote the value when you need to pass the `$` text unchanged.\n"
    )
    .unwrap();
    writeln!(
        output,
        "```yaml\n# Mapping form\naction:\n  type: command:execute\n  program: cargo\n  cwd:\n    $context: origin.pane.cwd\n\n# Compact form\naction: command:execute program=cargo cwd=$origin.pane.cwd\n```"
    )
    .unwrap();
    writeln!(
        output,
        "\n`await` waits for an action to finish. `detach` starts it without waiting. Cancellation stops a running action when an execution timeout or menu-control rule requests it. Each action below lists the supported choices."
    )
    .unwrap();

    render_action_inventory(&mut output);
    render_action_catalog(&mut output, host_support, catalog);

    writeln!(output, "\n## Command working directories\n").unwrap();
    writeln!(
        output,
        "By default, `command:execute` runs in the working directory of the pane where you opened the menu."
    )
    .unwrap();
    writeln!(
        output,
        "\n- An absolute `cwd` uses that exact directory.\n- A relative `cwd` starts from the pane's working directory.\n- A `$origin.` reference used for `cwd` must resolve to an absolute path.\n- If Muxe cannot determine an absolute working directory, it reports an error without starting the command."
    )
    .unwrap();

    render_context_references(&mut output);

    writeln!(output, "\n## Key names\n").unwrap();
    writeln!(
        output,
        "Keys use the form `[selector:]modifier+…+key`. Most keys omit the selector. Use `alternate:` or `base:` only when you need to select one of those terminal key representations."
    )
    .unwrap();
    writeln!(
        output,
        "\nWrite modifiers in this order: `ctrl`, `alt`, `shift`, `super`, `hyper`, `meta`, `caps-lock`, `num-lock`."
    )
    .unwrap();
    writeln!(
        output,
        "\nFor a printable character, write the character itself. Use `unicode+<hex-scalar>` for other Unicode characters. Examples: `ctrl+c`, `alternate:alt+unicode+e9`, and `ctrl+f12`.\n"
    )
    .unwrap();

    writeln!(output, "### Keys that share a VT100 code\n").unwrap();
    writeln!(
        output,
        "Older VT100-style terminal input cannot distinguish these pairs:"
    )
    .unwrap();
    writeln!(
        output,
        "\n- `tab` and `ctrl+i`\n- `enter` and `ctrl+m`\n- `backspace` and `ctrl+h`\n- `esc` and `ctrl+[`"
    )
    .unwrap();
    writeln!(
        output,
        "\nVT100 input also cannot distinguish uppercase and lowercase letters while Ctrl is held. Muxe rejects bindings that would produce the same VT100 input. Terminals using the Kitty keyboard protocol can distinguish these keys."
    )
    .unwrap();
    writeln!(output, "\n### Terminal requirements for named keys\n").unwrap();
    writeln!(
        output,
        "Some named keys need optional Kitty keyboard features. The table shows which features must be enabled. `none` means that the key needs no optional feature."
    )
    .unwrap();
    writeln!(
        output,
        "\nUsing `alternate:` or `base:` requires alternate-key reporting. Setting `repeat` to either `true` or `false` requires key-event reporting. Lock modifiers on printable characters require all-keys-as-escape-codes mode."
    )
    .unwrap();
    writeln!(
        output,
        "\n| Named key | Without explicit `repeat` | With explicit `repeat` |\n|---|---|---|"
    )
    .unwrap();
    for key in NamedKey::FINITE {
        let base = canonical_named(*key).required_capabilities(None);
        let repeated = canonical_named(*key).required_capabilities(Some(true));
        assert_eq!(
            repeated,
            canonical_named(*key).required_capabilities(Some(false)),
            "explicit repeat values must require the same keyboard features"
        );
        writeln!(
            output,
            "| `{}` | {} | {} |",
            key.as_str(),
            key_capabilities(base),
            key_capabilities(repeated),
        )
        .unwrap();
    }

    writeln!(output, "\n## Native actions\n").unwrap();
    writeln!(
        output,
        "Native actions call operations specific to Zellij or Herdr. Their `type` begins with `native.`. When Muxe loads the configuration, it checks the action name and parameters for the terminal multiplexer where you opened the menu. Example: `native.zellij.command:close-focus`."
    )
    .unwrap();

    output
}

fn render_action_inventory(output: &mut String) {
    writeln!(output, "\n## Action index\n").unwrap();
    writeln!(output, "| Category | Actions |\n|---|---|").unwrap();
    for category in ActionCategory::iter() {
        let links = PortableActionKind::iter()
            .filter(|action| action.schema().category == category)
            .map(|action| {
                let link_anchor = action.as_str().replace([':', '.'], "");
                format!("[`{action}`](#{link_anchor})")
            })
            .collect::<Vec<_>>()
            .join(", ");
        writeln!(output, "| {category} | {links} |").unwrap();
    }
}

fn render_action_catalog(output: &mut String, host_support: &HostSupport, catalog: &ActionCatalog) {
    writeln!(output, "\n## Actions\n").unwrap();

    for action in PortableActionKind::iter() {
        let schema = action.schema();
        let desc = catalog
            .actions
            .get(&action)
            .expect("action exists in catalog");
        let host_row = host_support
            .rows
            .get(&action)
            .expect("action exists in host support");

        writeln!(output, "\n### `{action}`\n").unwrap();
        writeln!(output, "{}\n", desc.summary).unwrap();

        let execution = match (
            schema.default_execution,
            schema.capabilities.awaitable,
            schema.capabilities.detachable,
            schema.capabilities.cancellable,
        ) {
            (None, false, false, false) => "immediate.".to_owned(),
            (Some(muxe_core::ExecutionMode::Await), true, true, false) => {
                "`await` by default; `detach` supported.".to_owned()
            }
            (Some(muxe_core::ExecutionMode::Detach), true, true, false) => {
                "`detach` by default; `await` supported.".to_owned()
            }
            (Some(muxe_core::ExecutionMode::Await), true, true, true) => {
                "`await` by default; `detach` and cancellation supported.".to_owned()
            }
            (Some(muxe_core::ExecutionMode::Detach), true, true, true) => {
                "`detach` by default; `await` and cancellation supported.".to_owned()
            }
            _ => "see the action's execution settings.".to_owned(),
        };
        writeln!(output, "- **Execution:** {execution}").unwrap();
        if let Some(parameter) = schema
            .parameters
            .iter()
            .filter_map(|parameter| parameter.positional.map(|position| (position, parameter)))
            .min_by_key(|(position, _)| *position)
            .map(|(_, parameter)| parameter)
        {
            writeln!(
                output,
                "- **Compact form:** `{}` may be the first unnamed argument.",
                parameter.name
            )
            .unwrap();
        }

        writeln!(output, "\n#### Parameters\n").unwrap();
        let parameters = schema
            .parameters
            .iter()
            .filter(|parameter| parameter.value_type != ActionParameterType::Unsupported)
            .collect::<Vec<_>>();
        if parameters.is_empty() {
            writeln!(output, "This action has no parameters.").unwrap();
        } else {
            writeln!(
                output,
                "| Parameter | Type | Requirement | Default | Description |\n|---|---|---|---|---|"
            )
            .unwrap();
            for parameter in parameters {
                let default = parameter
                    .omitted
                    .map_or_else(|| "—".to_owned(), |value| value.to_string());
                let description = desc
                    .parameters
                    .get(&parameter.name)
                    .expect("parameter exists in catalog");
                writeln!(
                    output,
                    "| `{}` | {} | {} | {} | {} |",
                    parameter.name,
                    parameter.value_type,
                    parameter_requirement(schema, parameter.name),
                    default,
                    description
                )
                .unwrap();
            }
        }

        if let Some(alert_name) = host_row.alert.markdown_name() {
            writeln!(output, "\n> [!{alert_name}]").unwrap();
            if let Some(zellij) = &host_row.zellij {
                writeln!(output, "> **Zellij:** {zellij}").unwrap();
                if host_row.herdr.is_some() {
                    writeln!(output, ">").unwrap();
                }
            }
            if let Some(herdr) = &host_row.herdr {
                writeln!(output, "> **Herdr:** {herdr}").unwrap();
            }
        }
    }
}
fn parameter_requirement(
    schema: muxe_core::PortableActionSchema,
    parameter: ActionParameterName,
) -> String {
    if schema
        .parameters
        .iter()
        .any(|candidate| candidate.name == parameter && candidate.required)
    {
        return "required".to_owned();
    }
    for constraint in schema.constraints {
        match constraint {
            ActionConstraint::ExactlyOne { parameters, .. } if parameters.contains(&parameter) => {
                let names = parameters
                    .iter()
                    .map(|name| format!("`{name}`"))
                    .collect::<Vec<_>>();
                return format!("exactly one of {}", names.join(" or "));
            }
            ActionConstraint::Requires {
                parameter: constrained,
                required_parameter,
                ..
            } if *constrained == parameter => {
                return format!("optional; requires `{required_parameter}`");
            }
            _ => {}
        }
    }
    "optional".to_owned()
}

fn enum_values<T>() -> String
where
    T: IntoEnumIterator + std::fmt::Display,
{
    let mut values = T::iter()
        .map(|value| format!("`{value}`"))
        .collect::<Vec<_>>();
    let last = values.pop().expect("documented enum is non-empty");
    match values.as_slice() {
        [] => last,
        [only] => format!("{only} or {last}"),
        _ => format!("{}, or {last}", values.join(", ")),
    }
}

fn render_context_references(output: &mut String) {
    writeln!(
        output,
        "\n## Using values from the pane that opened the menu\n"
    )
    .unwrap();
    writeln!(
        output,
        "Use these `$origin.` references to reuse information from the pane where you opened the menu. Some optional references may be unavailable.\n"
    )
    .unwrap();
    writeln!(output, "| Reference | Value | Description |\n|---|---|---|").unwrap();
    let host_kinds = enum_values::<OriginHostKind>();
    let pane_types = enum_values::<OriginPaneType>();
    let invocation_sources = enum_values::<OriginInvocationSource>();
    let contexts = [
        (
            "$origin.host.kind",
            host_kinds.as_str(),
            "Terminal multiplexer where the menu was opened.",
        ),
        (
            "$origin.server.id",
            "server ID",
            "Zellij or Herdr server where the menu was opened.",
        ),
        (
            "$origin.client.id",
            "client ID",
            "Terminal connection that opened the menu.",
        ),
        (
            "$origin.session.id",
            "session ID",
            "Session where the menu was opened.",
        ),
        (
            "$origin.workspace.id",
            "workspace ID",
            "Workspace containing the pane.",
        ),
        ("$origin.tab.id", "tab ID", "Tab where the menu was opened."),
        (
            "$origin.tab.index",
            "non-negative integer",
            "Index of that tab.",
        ),
        (
            "$origin.pane.id",
            "pane ID",
            "Pane where the menu was opened.",
        ),
        (
            "$origin.pane.type",
            pane_types.as_str(),
            "Type of that pane.",
        ),
        (
            "$origin.pane.cwd",
            "absolute path",
            "Working directory of that pane.",
        ),
        (
            "$origin.selection.text",
            "string",
            "Text selected in that pane.",
        ),
        (
            "$origin.invocation.source",
            invocation_sources.as_str(),
            "How the menu was opened.",
        ),
        (
            "$origin.worktree.id",
            "worktree ID",
            "Git worktree associated with that pane.",
        ),
        (
            "$origin.worktree.path",
            "absolute path",
            "Path to that Git worktree.",
        ),
        (
            "$origin.agent.id",
            "agent ID",
            "Coding agent associated with that pane.",
        ),
        ("$origin.link.url", "URL", "URL under the cursor."),
        (
            "$origin.link.handler.id",
            "link handler ID",
            "Muxe link handler selected for that URL.",
        ),
    ];
    for (token, value, description) in contexts {
        writeln!(output, "| `{token}` | {value} | {description} |").unwrap();
    }
}

fn load_host_support(path: &PathBuf) -> Result<HostSupport, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let mut rows = BTreeMap::new();
    for (line_number, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let columns = line.split('\t').collect::<Vec<_>>();
        if columns.len() != 4 {
            return Err(format!(
                "{}:{}: expected action, alert level, Zellij note, and Herdr note separated by tabs",
                path.display(),
                line_number + 1,
            )
            .into());
        }
        let action = columns[0].trim();
        let kind = PortableActionKind::parse(action).ok_or_else(|| {
            format!(
                "{}:{}: unknown portable action `{action}`",
                path.display(),
                line_number + 1
            )
        })?;
        let alert = columns[1].trim().parse::<HostAlert>().map_err(|_| {
            format!(
                "{}:{}: alert level must be `none`, `note`, `warning`, or `caution`",
                path.display(),
                line_number + 1
            )
        })?;
        let note = |value: &str| match value.trim() {
            "-" => None,
            value => Some(value.to_owned()),
        };
        let row = HostSupportRow {
            alert,
            zellij: note(columns[2]),
            herdr: note(columns[3]),
        };
        if matches!(alert, HostAlert::None) && (row.zellij.is_some() || row.herdr.is_some()) {
            return Err(format!(
                "{}:{}: alert level `none` cannot have host notes",
                path.display(),
                line_number + 1,
            )
            .into());
        }
        if !matches!(alert, HostAlert::None) && row.zellij.is_none() && row.herdr.is_none() {
            return Err(format!(
                "{}:{}: alert level requires at least one host note",
                path.display(),
                line_number + 1,
            )
            .into());
        }
        if rows.insert(kind, row).is_some() {
            return Err(format!(
                "{}:{}: duplicate portable action `{action}`",
                path.display(),
                line_number + 1,
            )
            .into());
        }
    }
    for action in PortableActionKind::iter() {
        if !rows.contains_key(&action) {
            return Err(format!(
                "{}: missing host support for `{}`",
                path.display(),
                action.as_str()
            )
            .into());
        }
    }
    Ok(HostSupport { rows })
}

fn canonical_named(named: NamedKey) -> CanonicalKey {
    CanonicalKey {
        source: KeyIdentitySource::Primary,
        modifiers: Modifiers::empty(),
        identity: KeyIdentity::Named(named),
    }
}

fn key_capabilities(capabilities: KeyCapabilities) -> &'static str {
    match (
        capabilities.event_types,
        capabilities.alternate_keys,
        capabilities.all_keys_as_escape_codes,
    ) {
        (false, false, false) => "none",
        (true, false, false) => "event types",
        (false, true, false) => "alternate keys",
        (false, false, true) => "all keys as escape codes",
        (true, true, false) => "event types, alternate keys",
        (true, false, true) => "event types, all keys as escape codes",
        (false, true, true) => "alternate keys, all keys as escape codes",
        (true, true, true) => "event types, alternate keys, all keys as escape codes",
    }
}
