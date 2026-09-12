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
    /// Validate configuration for every supported host.
    Config(ConfigCommand),
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

/// Commands that validate Muxe configuration.
#[derive(Debug, Args)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub command: ConfigSubcommand,
}

/// Configuration validation subcommands.
#[derive(Debug, Subcommand)]
pub enum ConfigSubcommand {
    /// Check the base configuration with each host override.
    Check,
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
    /// Print the effective contents of one or every configured menu as JSON.
    Dump(MenuDump),
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

/// Arguments for `muxe menu dump`.
#[derive(Debug, Args)]
pub struct MenuDump {
    /// Dump every named menu in an object keyed by menu ID.
    #[arg(long)]
    pub all: bool,
    /// The configured menu ID. Ignored when `--all` is present.
    #[arg(required_unless_present = "all")]
    pub menu: Option<String>,
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
    #[must_use]
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

/// Broker subcommands.
#[derive(Debug, Subcommand)]
pub enum BrokerSubcommand {
    /// Drain and retire brokers without starting replacements.
    Retire(BrokerRetireCommand),
    /// Start the target Herdr broker in the repository-owned upgrade runner.
    #[command(name = "serve-herdr", hide = true)]
    ServeHerdr(BrokerServeHerdrCommand),
    /// Start the target Zellij broker in the repository-owned upgrade runner.
    #[command(name = "serve-zellij", hide = true)]
    ServeZellij(BrokerServeZellijCommand),
}
/// Arguments for `muxe broker retire`.
#[derive(Debug, Args)]
pub struct BrokerRetireCommand {
    /// Select live hosts to retire. The public contract intentionally has no default.
    #[arg(long, value_enum)]
    pub host: Option<HostScope>,
}

/// Fixed inputs for the hidden target-Herdr serving command.
///
/// The command accepts only the runner's pinned endpoint and installation
/// inputs; it deliberately provides no arbitrary command or argument hook.
#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("activation-handoff")
        .args(["handoff", "activation_journal"])
        .multiple(true)
))]
pub struct BrokerServeHerdrCommand {
    /// Absolute Muxe broker endpoint socket path.
    #[arg(long, value_parser = parse_absolute_path)]
    pub socket: PathBuf,
    /// Absolute path to the pinned Herdr binary.
    #[arg(long, value_parser = parse_absolute_path)]
    pub herdr_binary: PathBuf,
    /// Absolute Herdr server socket path.
    #[arg(long, value_parser = parse_absolute_path)]
    pub herdr_socket: PathBuf,
    /// Absolute configuration path for the target broker.
    #[arg(long, value_parser = parse_absolute_path)]
    pub config: PathBuf,
    /// Absolute cache directory for the target broker.
    #[arg(long, value_parser = parse_absolute_path)]
    pub cache_dir: PathBuf,
    /// Exact 32-hex-character target activation handoff ID.
    #[arg(
        long,
        requires = "activation_journal",
        value_parser = parse_handoff_hex
    )]
    pub handoff: Option<String>,
    /// Durable activation journal read before the target broker binds.
    #[arg(long, requires = "handoff", value_parser = parse_absolute_path)]
    pub activation_journal: Option<PathBuf>,
}

/// Fixed inputs for the hidden target-Zellij serving command.
///
/// Mirrors [`BrokerServeHerdrCommand`]: only the runner's pinned endpoint and
/// installation inputs, with the Zellij session name and executable in place
/// of the Herdr socket pair. No arbitrary command or argument hook.
#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("activation-handoff")
        .args(["handoff", "activation_journal"])
        .multiple(true)
))]
pub struct BrokerServeZellijCommand {
    /// Absolute Muxe broker endpoint socket path.
    #[arg(long, value_parser = parse_absolute_path)]
    pub socket: PathBuf,
    /// Absolute path to the pinned Zellij binary.
    #[arg(long, value_parser = parse_absolute_path)]
    pub zellij_exe: PathBuf,
    /// Live Zellij session name the target broker serves.
    #[arg(long)]
    pub session: String,
    /// Absolute configuration path for the target broker.
    #[arg(long, value_parser = parse_absolute_path)]
    pub config: PathBuf,
    /// Absolute cache directory for the target broker.
    #[arg(long, value_parser = parse_absolute_path)]
    pub cache_dir: PathBuf,
    /// Exact 32-hex-character target activation handoff ID.
    #[arg(
        long,
        requires = "activation_journal",
        value_parser = parse_handoff_hex
    )]
    pub handoff: Option<String>,
    /// Durable activation journal read before the target broker binds.
    #[arg(long, requires = "handoff", value_parser = parse_absolute_path)]
    pub activation_journal: Option<PathBuf>,
}
fn parse_absolute_path(value: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err("path must be absolute".to_owned())
    }
}

fn parse_handoff_hex(value: &str) -> Result<String, String> {
    if value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(value.to_owned())
    } else {
        Err("handoff must be exactly 32 hexadecimal characters".to_owned())
    }
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
    /// Emit the stable `snake_case` JSON record.
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
    use clap::{CommandFactory, Parser};

    use super::*;

    #[test]
    fn config_check_command_parses() {
        let cli = Cli::try_parse_from(["muxe", "config", "check"]).expect("config check parses");
        assert!(matches!(
            cli.command,
            Command::Config(ConfigCommand {
                command: ConfigSubcommand::Check,
            })
        ));
    }

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
    fn menu_dump_requires_one_selector_and_all_ignores_a_supplied_menu() {
        let cli =
            Cli::try_parse_from(["muxe", "menu", "dump", "main"]).expect("one menu dump parses");
        let Command::Menu(MenuCommand {
            command: MenuSubcommand::Dump(command),
        }) = cli.command
        else {
            panic!("expected menu dump");
        };
        assert!(!command.all);
        assert_eq!(command.menu.as_deref(), Some("main"));

        let cli = Cli::try_parse_from(["muxe", "menu", "dump", "--all"])
            .expect("all-menu dump needs no menu");
        let Command::Menu(MenuCommand {
            command: MenuSubcommand::Dump(command),
        }) = cli.command
        else {
            panic!("expected all-menu dump");
        };
        assert!(command.all);
        assert_eq!(command.menu, None);

        let cli = Cli::try_parse_from(["muxe", "menu", "dump", "--all", "ignored"])
            .expect("all-menu dump accepts and ignores a menu argument");
        let Command::Menu(MenuCommand {
            command: MenuSubcommand::Dump(command),
        }) = cli.command
        else {
            panic!("expected all-menu dump");
        };
        assert!(command.all);
        assert_eq!(command.menu.as_deref(), Some("ignored"));

        assert!(Cli::try_parse_from(["muxe", "menu", "dump"]).is_err());
    }

    #[test]
    fn integration_policy_flags_are_exclusive() {
        assert!(
            Cli::try_parse_from([
                "muxe",
                "integration",
                "install",
                "zellij",
                "--always-configure",
                "--never-configure",
            ])
            .is_err()
        );
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
    fn hidden_herdr_serve_command_accepts_only_typed_inputs() {
        let base_serve_args = [
            "muxe",
            "broker",
            "serve-herdr",
            "--socket",
            "/tmp/muxe-target.sock",
            "--herdr-binary",
            "/opt/herdr/herdr",
            "--herdr-socket",
            "/tmp/herdr.sock",
            "--config",
            "/tmp/config.yml",
            "--cache-dir",
            "/tmp/muxe-cache",
        ];
        let serve = Cli::try_parse_from(base_serve_args.iter().copied())
            .expect("old broker inputs without handoff parse");
        let Command::Broker(BrokerCommand {
            command: BrokerSubcommand::ServeHerdr(serve),
        }) = serve.command
        else {
            panic!("expected hidden Herdr serve command");
        };
        assert_eq!(serve.socket, PathBuf::from("/tmp/muxe-target.sock"));
        assert_eq!(serve.herdr_socket, PathBuf::from("/tmp/herdr.sock"));
        assert_eq!(serve.handoff, None);
        assert_eq!(serve.activation_journal, None);

        let mut relative_socket_args = base_serve_args;
        relative_socket_args[4] = "relative.sock";
        assert!(Cli::try_parse_from(relative_socket_args).is_err());
        assert!(
            Cli::try_parse_from(
                base_serve_args
                    .iter()
                    .copied()
                    .chain(["--command", "arbitrary"])
            )
            .is_err()
        );

        assert!(
            Cli::try_parse_from(
                base_serve_args
                    .iter()
                    .copied()
                    .chain(["--handoff", "0123456789abcdef0123456789ABCDEF"])
            )
            .is_err()
        );
        assert!(
            Cli::try_parse_from(
                base_serve_args
                    .iter()
                    .copied()
                    .chain(["--activation-journal", "/tmp/activation-journal.json"])
            )
            .is_err()
        );
        assert!(
            Cli::try_parse_from(base_serve_args.iter().copied().chain([
                "--handoff",
                "not-a-handoff",
                "--activation-journal",
                "/tmp/activation-journal.json",
            ]))
            .is_err()
        );

        let target = Cli::try_parse_from(base_serve_args.iter().copied().chain([
            "--handoff",
            "0123456789abcdef0123456789ABCDEF",
            "--activation-journal",
            "/tmp/activation-journal.json",
        ]))
        .expect("target inputs with handoff record parse");
        let Command::Broker(BrokerCommand {
            command: BrokerSubcommand::ServeHerdr(target),
        }) = target.command
        else {
            panic!("expected hidden target Herdr serve command");
        };
        assert_eq!(
            target.handoff.as_deref(),
            Some("0123456789abcdef0123456789ABCDEF")
        );
        assert_eq!(
            target.activation_journal.as_deref(),
            Some(std::path::Path::new("/tmp/activation-journal.json"))
        );

        let mut command = Cli::command();
        let broker = command
            .find_subcommand_mut("broker")
            .expect("broker command is present");
        let help = broker.render_long_help().to_string();
        assert!(!help.contains("serve-herdr"));
    }
    #[test]
    fn hidden_zellij_serve_command_accepts_only_typed_inputs() {
        let base_serve_args = [
            "muxe",
            "broker",
            "serve-zellij",
            "--socket",
            "/tmp/muxe-target.sock",
            "--zellij-exe",
            "/opt/zellij/zellij",
            "--session",
            "work",
            "--config",
            "/tmp/config.yml",
            "--cache-dir",
            "/tmp/muxe-cache",
        ];
        let serve = Cli::try_parse_from(base_serve_args.iter().copied())
            .expect("old broker inputs without handoff parse");
        let Command::Broker(BrokerCommand {
            command: BrokerSubcommand::ServeZellij(serve),
        }) = serve.command
        else {
            panic!("expected hidden Zellij serve command");
        };
        assert_eq!(serve.socket, PathBuf::from("/tmp/muxe-target.sock"));
        assert_eq!(serve.zellij_exe, PathBuf::from("/opt/zellij/zellij"));
        assert_eq!(serve.session, "work");
        assert_eq!(serve.handoff, None);
        assert_eq!(serve.activation_journal, None);

        let mut relative_socket_args = base_serve_args;
        relative_socket_args[4] = "relative.sock";
        assert!(Cli::try_parse_from(relative_socket_args).is_err());
        assert!(
            Cli::try_parse_from(
                base_serve_args
                    .iter()
                    .copied()
                    .chain(["--command", "arbitrary"])
            )
            .is_err()
        );

        assert!(
            Cli::try_parse_from(
                base_serve_args
                    .iter()
                    .copied()
                    .chain(["--handoff", "0123456789abcdef0123456789ABCDEF"])
            )
            .is_err()
        );
        assert!(
            Cli::try_parse_from(
                base_serve_args
                    .iter()
                    .copied()
                    .chain(["--activation-journal", "/tmp/activation-journal.json"])
            )
            .is_err()
        );
        assert!(
            Cli::try_parse_from(base_serve_args.iter().copied().chain([
                "--handoff",
                "not-a-handoff",
                "--activation-journal",
                "/tmp/activation-journal.json",
            ]))
            .is_err()
        );

        let target = Cli::try_parse_from(base_serve_args.iter().copied().chain([
            "--handoff",
            "0123456789abcdef0123456789ABCDEF",
            "--activation-journal",
            "/tmp/activation-journal.json",
        ]))
        .expect("target inputs with handoff record parse");
        let Command::Broker(BrokerCommand {
            command: BrokerSubcommand::ServeZellij(target),
        }) = target.command
        else {
            panic!("expected hidden target Zellij serve command");
        };
        assert_eq!(
            target.handoff.as_deref(),
            Some("0123456789abcdef0123456789ABCDEF")
        );
        assert_eq!(
            target.activation_journal.as_deref(),
            Some(std::path::Path::new("/tmp/activation-journal.json"))
        );

        let mut command = Cli::command();
        let broker = command
            .find_subcommand_mut("broker")
            .expect("broker command is present");
        let help = broker.render_long_help().to_string();
        assert!(!help.contains("serve-zellij"));
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
