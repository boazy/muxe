use std::{ffi::OsString, path::PathBuf, str::FromStr};

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

/// The native Muxe command line.
#[derive(Debug, Parser)]
#[command(name = "muxe", version, propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// The public Muxe command tree.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create the starter configuration without installing host integration.
    Init,
    /// Open a configured root menu through a host launcher.
    Menu(MenuCommand),
    /// Open a generic command pane.
    Pane(PaneCommand),
    /// Install or remove managed host integration.
    Integration(IntegrationCommand),
    /// Activate this version for selected live hosts.
    Activate(ActivateCommand),
    /// Manage broker lifecycle.
    Broker(BrokerCommand),
    /// Print the embedded compatibility record.
    Compatibility(CompatibilityCommand),
    /// Remove explicitly selected retained Muxe data.
    Purge(PurgeCommand),
    /// Run the native terminal UI.
    Ui(UiCommand),
}

/// Commands that start a configured menu.
#[derive(Debug, Args)]
pub struct MenuCommand {
    #[command(subcommand)]
    pub command: MenuSubcommand,
}

/// Menu subcommands.
#[derive(Debug, Subcommand)]
pub enum MenuSubcommand {
    /// Open a root menu as a focused modal UI.
    Open(MenuOpen),
}

/// Arguments for `muxe menu open`.
#[derive(Debug, Args)]
pub struct MenuOpen {
    #[command(flatten)]
    pub placement: PlacementOptions,
    /// Override the configured theme for this invocation.
    #[arg(long)]
    pub theme: Option<String>,
    /// Override the configured color scheme for this invocation.
    #[arg(long = "color-scheme")]
    pub color_scheme: Option<String>,
    /// The configured root menu ID.
    pub root: String,
}

/// Commands that launch a generic command pane.
#[derive(Debug, Args)]
pub struct PaneCommand {
    #[command(subcommand)]
    pub command: PaneSubcommand,
}

/// Pane subcommands.
#[derive(Debug, Subcommand)]
pub enum PaneSubcommand {
    /// Open one generic command pane.
    Open(PaneOpen),
}

/// Arguments for `muxe pane open`.
#[derive(Debug, Args)]
pub struct PaneOpen {
    #[command(flatten)]
    pub placement: PlacementOptions,
    /// Leave focus on the current pane after opening the generic child.
    #[arg(long)]
    pub no_focus: bool,
    /// Override the generic child working directory.
    #[arg(long)]
    pub cwd: Option<PathBuf>,
    /// The exact program and argument vector after `--`.
    #[arg(last = true, required = true, num_args = 1.., allow_hyphen_values = true)]
    pub argv: Vec<OsString>,
}

/// Host-independent pane placement options.
#[derive(Clone, Debug, Args, Eq, PartialEq)]
pub struct PlacementOptions {
    /// Select a host or inherit it from the current environment.
    #[arg(long, value_enum, default_value = "auto")]
    pub host: HostSelector,
    /// Select the host pane representation.
    #[arg(long = "pane-type", value_enum, default_value = "split")]
    pub pane_type: PaneType,
    /// Select an explicit parent pane or the captured origin pane.
    #[arg(long = "parent-pane", default_value = "current")]
    pub parent_pane: ParentPane,
    /// Choose the split direction where the host supports it.
    #[arg(long, value_enum, default_value = "down")]
    pub direction: SplitDirection,
    /// Set a pane width in terminal cells or as a percentage.
    #[arg(long)]
    pub width: Option<Dimension>,
    /// Set a pane height in terminal cells or as a percentage.
    #[arg(long)]
    pub height: Option<Dimension>,
    /// Set an overlay or popup position as `x,y` terminal-cell coordinates.
    #[arg(long)]
    pub position: Option<Position>,
}

/// A host selected by a launcher command.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HostSelector {
    Auto,
    Zellij,
    Herdr,
}

/// A host pane type requested by a launcher command.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum PaneType {
    Split,
    Overlay,
    Popup,
}

/// A split direction requested by a launcher command.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SplitDirection {
    Down,
    Up,
    Left,
    Right,
}

/// A launcher parent-pane selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParentPane {
    Current,
    Id(String),
}

impl FromStr for ParentPane {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err("parent pane cannot be empty".into());
        }
        Ok(if value == "current" {
            Self::Current
        } else {
            Self::Id(value.into())
        })
    }
}

/// A terminal-cell count or a percentage of the containing terminal area.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Dimension {
    Cells(u16),
    Percent(u16),
}

impl FromStr for Dimension {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (number, percent) = value
            .strip_suffix('%')
            .map_or((value, false), |number| (number, true));
        let number = number
            .parse()
            .map_err(|_| "dimension must be terminal cells or a percentage".to_owned())?;
        Ok(if percent {
            Self::Percent(number)
        } else {
            Self::Cells(number)
        })
    }
}

/// A terminal-cell coordinate for floating or overlay placement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub x: u16,
    pub y: u16,
}

impl FromStr for Position {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (x, y) = value
            .split_once(',')
            .ok_or_else(|| "position must use x,y terminal-cell coordinates".to_owned())?;
        if x.is_empty() || y.is_empty() || y.contains(',') {
            return Err("position must use one x,y coordinate pair".into());
        }
        Ok(Self {
            x: x.parse().map_err(|_| {
                "position x must be an unsigned terminal-cell coordinate".to_owned()
            })?,
            y: y.parse().map_err(|_| {
                "position y must be an unsigned terminal-cell coordinate".to_owned()
            })?,
        })
    }
}

/// Commands that install or remove a host integration.
#[derive(Debug, Args)]
pub struct IntegrationCommand {
    #[command(subcommand)]
    pub command: IntegrationSubcommand,
}

/// Integration subcommands.
#[derive(Debug, Subcommand)]
pub enum IntegrationSubcommand {
    /// Install the bundled Zellij bridge and optionally configure Zellij KDL.
    Install(InstallIntegrationCommand),
    /// Remove receipt-owned Zellij integration artifacts.
    Uninstall(UninstallIntegrationCommand),
}

/// Installation target selection.
#[derive(Debug, Args)]
pub struct InstallIntegrationCommand {
    #[command(subcommand)]
    pub target: IntegrationTarget,
}

/// Uninstallation target selection.
#[derive(Debug, Args)]
pub struct UninstallIntegrationCommand {
    #[command(subcommand)]
    pub target: IntegrationTarget,
}

/// The host integration targets implemented in v1.
#[derive(Debug, Subcommand)]
pub enum IntegrationTarget {
    Zellij(ZellijIntegrationOptions),
}

/// Shared install or uninstall options for the Zellij integration.
#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("configuration-policy")
        .args(["always_configure", "never_configure"])
        .multiple(false)
))]
pub struct ZellijIntegrationOptions {
    /// Suppress normal output and prompts.
    #[arg(short, long)]
    pub quiet: bool,
    /// Apply safe Muxe KDL edits without prompting.
    #[arg(long)]
    pub always_configure: bool,
    /// Do not edit KDL and do not prompt.
    #[arg(long)]
    pub never_configure: bool,
    /// Inspect or edit this Zellij configuration instead of the discovered default.
    #[arg(long)]
    pub zellij_config: Option<PathBuf>,
}

impl ZellijIntegrationOptions {
    /// Returns the policy requested explicitly by the command line, if any.
    pub const fn configuration_policy(&self) -> Option<ConfigurationPolicy> {
        if self.always_configure {
            Some(ConfigurationPolicy::Always)
        } else if self.never_configure {
            Some(ConfigurationPolicy::Never)
        } else {
            None
        }
    }
}

/// An explicit KDL configuration policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationPolicy {
    Always,
    Never,
}

/// Arguments for `muxe activate`.
#[derive(Debug, Args)]
pub struct ActivateCommand {
    /// Select live hosts for activation.
    #[arg(long, value_enum, default_value = "all")]
    pub host: HostScope,
}

/// Commands under `muxe broker`.
#[derive(Debug, Args)]
pub struct BrokerCommand {
    #[command(subcommand)]
    pub command: BrokerSubcommand,
}

/// Public broker subcommands.
#[derive(Debug, Subcommand)]
pub enum BrokerSubcommand {
    /// Drain and retire brokers without starting replacements.
    Retire(BrokerRetireCommand),
}

/// Arguments for `muxe broker retire`.
#[derive(Debug, Args)]
pub struct BrokerRetireCommand {
    /// Select live hosts to retire. The public contract intentionally has no default.
    #[arg(long, value_enum)]
    pub host: Option<HostScope>,
}

/// A lifecycle host scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum HostScope {
    All,
    Current,
    Zellij,
    Herdr,
}

/// Arguments for `muxe compatibility`.
#[derive(Debug, Args)]
pub struct CompatibilityCommand {
    /// Emit the stable snake_case JSON record.
    #[arg(long)]
    pub json: bool,
}

/// Arguments for `muxe purge`.
#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("purge-target")
        .args(["config", "cache"])
        .required(true)
        .multiple(true)
))]
pub struct PurgeCommand {
    /// Remove the complete Muxe configuration tree.
    #[arg(long)]
    pub config: bool,
    /// Remove cached schemas and logs.
    #[arg(long)]
    pub cache: bool,
    /// Authorize deletion without an interactive confirmation.
    #[arg(long)]
    pub yes: bool,
}

/// Commands that run the native terminal UI.
#[derive(Debug, Args)]
pub struct UiCommand {
    #[command(subcommand)]
    pub command: UiSubcommand,
}

/// UI subcommands.
#[derive(Debug, Subcommand)]
pub enum UiSubcommand {
    /// Run the UI for one root menu.
    Menu(UiMenuCommand),
}

/// Arguments for `muxe ui menu`.
#[derive(Debug, Args)]
pub struct UiMenuCommand {
    /// Override the configured theme for this invocation.
    #[arg(long)]
    pub theme: Option<String>,
    /// Override the configured color scheme for this invocation.
    #[arg(long = "color-scheme")]
    pub color_scheme: Option<String>,
    /// The configured root menu ID.
    pub root: String,
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn pane_command_retains_an_exact_argv_after_double_dash() {
        let cli = Cli::try_parse_from([
            "muxe",
            "pane",
            "open",
            "--host",
            "herdr",
            "--no-focus",
            "--",
            "tool",
            "--literal-flag",
            "two words",
        ])
        .expect("generic pane command parses");
        let Command::Pane(PaneCommand {
            command: PaneSubcommand::Open(command),
        }) = cli.command
        else {
            panic!("expected pane open");
        };
        assert_eq!(command.placement.host, HostSelector::Herdr);
        assert!(command.no_focus);
        assert_eq!(
            command.argv,
            ["tool", "--literal-flag", "two words"].map(OsString::from)
        );
    }

    #[test]
    fn pane_command_requires_double_dash_before_the_program() {
        assert!(Cli::try_parse_from(["muxe", "pane", "open", "tool"]).is_err());
    }

    #[test]
    fn menu_command_excludes_generic_cwd_and_focus_options() {
        assert!(Cli::try_parse_from(["muxe", "menu", "open", "--no-focus", "main"]).is_err());
        assert!(Cli::try_parse_from(["muxe", "menu", "open", "--cwd", "/tmp", "main"]).is_err());
    }

    #[test]
    fn menu_placement_uses_canonical_defaults_and_types() {
        let cli = Cli::try_parse_from([
            "muxe",
            "menu",
            "open",
            "--pane-type",
            "popup",
            "--parent-pane",
            "pane-7",
            "--direction",
            "right",
            "--width",
            "80%",
            "--height",
            "12",
            "--position",
            "3,4",
            "main",
        ])
        .expect("menu placement parses");
        let Command::Menu(MenuCommand {
            command: MenuSubcommand::Open(command),
        }) = cli.command
        else {
            panic!("expected menu open");
        };
        assert_eq!(command.placement.host, HostSelector::Auto);
        assert_eq!(command.placement.pane_type, PaneType::Popup);
        assert_eq!(
            command.placement.parent_pane,
            ParentPane::Id("pane-7".into())
        );
        assert_eq!(command.placement.direction, SplitDirection::Right);
        assert_eq!(command.placement.width, Some(Dimension::Percent(80)));
        assert_eq!(command.placement.height, Some(Dimension::Cells(12)));
        assert_eq!(command.placement.position, Some(Position { x: 3, y: 4 }));
    }

    #[test]
    fn integration_policy_flags_are_exclusive() {
        assert!(Cli::try_parse_from([
            "muxe",
            "integration",
            "install",
            "zellij",
            "--always-configure",
            "--never-configure",
        ])
        .is_err());
        let cli = Cli::try_parse_from([
            "muxe",
            "integration",
            "uninstall",
            "zellij",
            "--quiet",
            "--always-configure",
        ])
        .expect("exclusive policy accepts one flag");
        let Command::Integration(IntegrationCommand {
            command:
                IntegrationSubcommand::Uninstall(UninstallIntegrationCommand {
                    target: IntegrationTarget::Zellij(options),
                }),
        }) = cli.command
        else {
            panic!("expected zellij uninstall");
        };
        assert!(options.always_configure);
        assert!(!options.never_configure);
    }

    #[test]
    fn lifecycle_defaults_and_purge_target_requirement_match_the_contract() {
        let activate = Cli::try_parse_from(["muxe", "activate"]).expect("activate parses");
        let Command::Activate(command) = activate.command else {
            panic!("expected activate");
        };
        assert_eq!(command.host, HostScope::All);
        let retire = Cli::try_parse_from(["muxe", "broker", "retire"]).expect("retire parses");
        let Command::Broker(BrokerCommand {
            command: BrokerSubcommand::Retire(command),
        }) = retire.command
        else {
            panic!("expected broker retire");
        };
        assert_eq!(command.host, None);
        assert!(Cli::try_parse_from(["muxe", "purge", "--yes"]).is_err());
        assert!(Cli::try_parse_from(["muxe", "purge", "--config", "--yes"]).is_ok());
    }

    #[test]
    fn simple_public_commands_keep_their_documented_arguments() {
        assert!(matches!(
            Cli::try_parse_from(["muxe", "init"])
                .expect("init parses")
                .command,
            Command::Init
        ));
        let compatibility =
            Cli::try_parse_from(["muxe", "compatibility", "--json"]).expect("compatibility parses");
        let Command::Compatibility(command) = compatibility.command else {
            panic!("expected compatibility");
        };
        assert!(command.json);
        let ui = Cli::try_parse_from([
            "muxe",
            "ui",
            "menu",
            "--theme",
            "night",
            "--color-scheme",
            "ink",
            "main",
        ])
        .expect("UI menu parses");
        let Command::Ui(UiCommand {
            command: UiSubcommand::Menu(command),
        }) = ui.command
        else {
            panic!("expected UI menu");
        };
        assert_eq!(command.theme.as_deref(), Some("night"));
        assert_eq!(command.color_scheme.as_deref(), Some("ink"));
        assert_eq!(command.root, "main");
    }
}
