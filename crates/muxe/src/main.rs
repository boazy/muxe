#![forbid(unsafe_code)]

#[cfg(test)]
#[path = "../tests/support/generated_executable.rs"]
mod generated_executable;

mod config_check;
mod init;

use std::{
    env,
    ffi::OsString,
    future::Future,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use muxe_adapter_api::HostAdapter;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use clap::Parser;
use color_eyre::eyre::{Context, Result, bail};
use muxe_broker::{BrokerClient, ClientError, RuntimeEndpoint};
use muxe_protocol::{
    AttachUi, BindingId, BrokerResponse, ClientRequest, HostKind as ProtocolHostKind, HostPaneId,
    HostTabId, LiveServerIdentity, MenuControl, MenuId, PeerRole, PendingLaunchToken,
    UiCallerIdentityWire, UiMenuControl, UiOriginBootstrap, UiSessionId, WorkspaceId,
};

use muxe::cli::{
    BrokerServeHerdrCommand, BrokerServeZellijCommand, Cli, Command, CompatibilityCommand,
    ConfigSubcommand, HostScope, HostSelector, IntegrationSubcommand, MenuSubcommand, PaneOpen,
    PaneSubcommand, PaneType, ParentPane, PurgeCommand, SplitDirection, UiMenuCommand,
    UiSubcommand,
};

#[derive(Clone, Copy)]
enum NativeCommandOperation {
    Init,
    ConfigCheck,
    Compatibility,
    Purge,
    MenuOpen,
    MenuDump,
    PaneOpen,
    UiMenu,
    IntegrationInstall,
    IntegrationUninstall,
    Activate,
    BrokerRetire,
}

impl NativeCommandOperation {
    const fn from_command(command: &Command) -> Option<Self> {
        match command {
            Command::Init => Some(Self::Init),
            Command::Config(_) => Some(Self::ConfigCheck),
            Command::Compatibility(_) => Some(Self::Compatibility),
            Command::Purge(_) => Some(Self::Purge),
            Command::Menu(menu) => match menu.command {
                MenuSubcommand::Open(_) => Some(Self::MenuOpen),
                MenuSubcommand::Dump(_) => Some(Self::MenuDump),
            },
            Command::Pane(_) => Some(Self::PaneOpen),
            Command::Ui(_) => Some(Self::UiMenu),
            Command::Integration(integration) => match integration.command {
                IntegrationSubcommand::Install(_) => Some(Self::IntegrationInstall),
                IntegrationSubcommand::Uninstall(_) => Some(Self::IntegrationUninstall),
            },
            Command::Activate(_) => Some(Self::Activate),
            Command::Broker(broker) => match broker.command {
                muxe::cli::BrokerSubcommand::Retire(_) => Some(Self::BrokerRetire),
                // Broker services already append their own lifecycle records.
                muxe::cli::BrokerSubcommand::ServeHerdr(_)
                | muxe::cli::BrokerSubcommand::ServeZellij(_) => None,
            },
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::ConfigCheck => "config-check",
            Self::Compatibility => "compatibility",
            Self::Purge => "purge",
            Self::MenuOpen => "menu-open",
            Self::MenuDump => "menu-dump",
            Self::PaneOpen => "pane-open",
            Self::UiMenu => "ui-menu",
            Self::IntegrationInstall => "integration-install",
            Self::IntegrationUninstall => "integration-uninstall",
            Self::Activate => "activate",
            Self::BrokerRetire => "broker-retire",
        }
    }
}

/// Records one payload-safe failure event without replacing the command's
/// stderr diagnostics. Broker services keep their existing lifecycle records.
fn record_command_failure(
    operation: NativeCommandOperation,
    error: color_eyre::Report,
) -> Result<()> {
    let paths = match muxe::paths::resolve() {
        Ok(paths) => paths,
        Err(log_error) => {
            return Err(error.wrap_err(format!(
                "could not resolve the cache directory to record the native command failure: {log_error}"
            )));
        }
    };
    let logger = match muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION")) {
        Ok(logger) => logger,
        Err(log_error) => {
            return Err(error.wrap_err(format!(
                "could not open the native command failure audit log: {log_error}"
            )));
        }
    };
    let event = muxe::logging::LogEvent::new(
        env!("CARGO_PKG_VERSION"),
        "local",
        operation.as_str(),
        "native command failed; inspect stderr for diagnostics",
    )
    .expect("fixed native command failure event fits the log message bound")
    .with_code("command-failed");
    if let Err(log_error) = logger.append(&event) {
        return Err(error.wrap_err(format!(
            "could not persist the native command failure audit record: {log_error}"
        )));
    }
    Err(error)
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let cli = Cli::parse();
    let operation = NativeCommandOperation::from_command(&cli.command);
    match Box::pin(dispatch(cli)).await {
        Ok(()) => Ok(()),
        Err(error) => match operation {
            Some(operation) => record_command_failure(operation, error),
            None => Err(error),
        },
    }
}

async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Init => {
            let paths = muxe::paths::resolve()?;
            let result = init::install(&paths.config_file())?;
            println!("created {}", result.config_path.display());
            println!("created {}", result.themes_directory.display());
            println!("created {}", result.color_schemes_directory.display());
            Ok(())
        }
        Command::Config(command) => match command.command {
            ConfigSubcommand::Check => Box::pin(check_configuration()).await,
        },
        Command::Compatibility(command) => compatibility(&command),
        Command::Purge(command) => purge(&command),
        Command::Menu(menu) => match menu.command {
            MenuSubcommand::Open(open) => Box::pin(launch_menu(open)).await,
            MenuSubcommand::Dump(dump) => Box::pin(dump_menu(dump)).await,
        },
        Command::Pane(pane) => match pane.command {
            PaneSubcommand::Open(open) => Box::pin(launch_pane(open)).await,
        },
        Command::Ui(ui) => match ui.command {
            UiSubcommand::Menu(menu) => Box::pin(run_ui(menu)).await,
        },
        Command::Integration(integration) => match integration.command {
            IntegrationSubcommand::Install(command) => install_zellij(command),
            IntegrationSubcommand::Uninstall(command) => uninstall_zellij(command),
        },
        Command::Activate(command) => Box::pin(activate_brokers(command)).await,
        Command::Broker(command) => match command.command {
            muxe::cli::BrokerSubcommand::Retire(retire) => {
                let scope = retire.host.ok_or_else(|| {
                    color_eyre::eyre::eyre!("muxe broker retire requires an explicit --host scope")
                })?;
                retire_brokers(scope).await
            }
            muxe::cli::BrokerSubcommand::ServeHerdr(command) => serve_herdr_broker(command).await,
            muxe::cli::BrokerSubcommand::ServeZellij(command) => serve_zellij_broker(command).await,
        },
    }
}

async fn check_configuration() -> Result<()> {
    let paths = muxe::paths::resolve()?;
    let herdr_binary = herdr_binary_from_path()?;
    let mut output = io::stderr().lock();
    if config_check::check(
        &paths.config_file(),
        &herdr_binary,
        &paths.cache_dir,
        &mut output,
    )
    .await?
    {
        println!("configuration is valid for zellij and herdr");
        Ok(())
    } else {
        bail!("configuration check failed")
    }
}

fn compatibility(command: &CompatibilityCommand) -> Result<()> {
    let record = muxe::compatibility::embedded_record()?;
    if command.json {
        println!("{}", muxe::compatibility::render_json(&record));
    } else {
        print!("{}", muxe::compatibility::render_human(&record));
    }
    Ok(())
}

async fn dump_menu(command: muxe::cli::MenuDump) -> Result<()> {
    let paths = muxe::paths::resolve()?;
    let host = autodetected_host().ok_or_else(|| {
        color_eyre::eyre::eyre!("could not detect a supported host for menu dump")
    })?;
    let config_file = paths.config_file();
    let config = match host {
        HostSelector::Zellij => muxe_broker::load_effective_config(
            &config_file,
            muxe_adapter_zellij::CONFIG_OVERRIDE_FILENAME,
            muxe_core::KeyCapabilities::default(),
            &muxe_adapter_zellij::ZellijValidator,
        )
        .wrap_err("could not compile the effective Zellij configuration for dump")?,
        HostSelector::Herdr => {
            let herdr_binary = herdr_binary_from_path()?;
            let validator =
                muxe_adapter_herdr::HerdrConfigValidator::load(&herdr_binary, &paths.cache_dir)
                    .await
                    .wrap_err("could not load the installed Herdr schema for menu dump")?;
            muxe_broker::load_effective_config(
                &config_file,
                muxe_adapter_herdr::CONFIG_OVERRIDE_FILENAME,
                muxe_core::KeyCapabilities::default(),
                &validator,
            )
            .wrap_err("could not compile the effective Herdr configuration for dump")?
        }
        HostSelector::Auto => unreachable!("automatic host selection is resolved"),
    };
    let requested = if command.all {
        None
    } else {
        let name = command
            .menu
            .expect("clap requires a menu unless --all is present");
        let parsed = muxe_core::MenuName::parse(name.as_str()).map_err(|error| {
            color_eyre::eyre::eyre!(
                "invalid menu name `{name}` ({}): names must be non-empty with no NUL/control characters",
                error.reason
            )
        })?;
        Some(muxe_core::MenuId::named(parsed))
    };
    let selection = requested.as_ref().map_or(
        muxe::menu_dump::MenuDumpSelection::All,
        muxe::menu_dump::MenuDumpSelection::Menu,
    );
    println!("{}", muxe::menu_dump::render(&config, selection)?);
    Ok(())
}

fn purge(command: &PurgeCommand) -> Result<()> {
    let paths = muxe::paths::resolve()?;
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))?;
    let presenter = |preview: &muxe::purge::PurgePreview| {
        eprintln!("Muxe will permanently remove:");
        for target in &preview.targets {
            eprintln!("  {} ({})", target.path.display(), target.kind);
        }
        for warning in &preview.warnings {
            eprintln!("warning: {warning}");
        }
    };

    let confirmer = |_preview: &muxe::purge::PurgePreview| {
        eprint!("Continue? [y/N] ");
        if io::stderr().flush().is_err() {
            return false;
        }
        let mut answer = String::new();
        io::stdin()
            .read_line(&mut answer)
            .is_ok_and(|_| matches!(answer.trim(), "y" | "Y" | "yes" | "YES"))
    };
    muxe::purge::purge(muxe::purge::PurgeInputs {
        config_dir: &paths.config_dir,
        cache_dir: &paths.cache_dir,
        config: command.config,
        cache: command.cache,
        yes: command.yes,
        interactive: io::stdin().is_terminal(),
        presenter: Some(&presenter),
        confirmer: Some(&confirmer),
        logger: Some(&logger),
    })?;
    Ok(())
}

async fn retire_brokers(scope: HostScope) -> Result<()> {
    let paths = muxe::paths::resolve()?;
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))?;
    let current = match scope {
        HostScope::Current => Some(detect_current_host(&paths.cache_dir)?),
        _ => None,
    };
    let report = muxe::lifecycle::retire(muxe::lifecycle::RetireInputs {
        cache_dir: &paths.cache_dir,
        scope,
        current,
        control: &muxe::lifecycle::LiveControl,
        logger: Some(&logger),
    })
    .await?;
    for outcome in report.units {
        match outcome {
            muxe::lifecycle::RetireOutcome::Retired { unit } => println!("retired {unit}"),
            muxe::lifecycle::RetireOutcome::AlreadyGone { unit } => {
                println!("already retired {unit}");
            }
        }
    }
    Ok(())
}

/// Locates the packaged bridge inside the release layout: the archive keeps
/// `lib/muxe/muxe-zellij.wasm` beside the installation root holding this
/// executable. A bare binary without its layout fails closed here instead of
/// inventing bridge bytes.
fn packaged_asset_path(exe_dir: &Path) -> PathBuf {
    exe_dir.join("lib").join("muxe").join("muxe-zellij.wasm")
}

fn packaged_bridge_bytes() -> Result<Vec<u8>> {
    let executable = env::current_exe().wrap_err("could not locate the running muxe executable")?;
    let directory = executable.parent().ok_or_else(|| {
        color_eyre::eyre::eyre!("the muxe executable path has no parent directory")
    })?;
    let path = packaged_asset_path(directory);
    std::fs::read(&path).wrap_err(format!(
        "could not read the packaged bridge at {}; install requires the complete release layout",
        path.display()
    ))
}

fn ask_for_consent(prompt: &str) -> bool {
    eprint!("{prompt} [y/N] ");
    if io::stderr().flush().is_err() {
        return false;
    }
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .is_ok_and(|_| matches!(answer.trim(), "y" | "Y" | "yes" | "YES"))
}

fn install_zellij(command: muxe::cli::InstallIntegrationCommand) -> Result<()> {
    let muxe::cli::IntegrationTarget::Zellij(options) = command.target;
    let paths = muxe::paths::resolve()?;
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the install audit log")?;
    let packaged = packaged_bridge_bytes()?;
    let explicit_policy = options.configuration_policy();
    let outcome = muxe::integration::install(muxe::integration::InstallInputs {
        config_dir: &paths.config_dir,
        cache_dir: &paths.cache_dir,
        packaged_wasm: &packaged,
        version: env!("CARGO_PKG_VERSION"),
        zellij_config: options.zellij_config,
        explicit_policy,
        quiet: options.quiet,
        interactive: io::stdin().is_terminal(),
        asker: Some(&ask_for_consent),
        logger: Some(&logger),
        hooks: muxe::integration::Hooks::default(),
    })
    .wrap_err("could not install the Zellij bridge")?;
    // Quiet is an explicit CLI contract: no status lines on stdout, success
    // reported by exit status and the persistent audit log only.
    if !options.quiet {
        println!(
            "installed bridge {} (sha256 {})",
            outcome.receipt_path.display(),
            outcome.bridge_digest
        );
        match outcome.config_path {
            Some(path) if outcome.config_edited => {
                println!("edited {}", path.display());
            }
            _ => {}
        }
        if let Some(snippet) = outcome.manual_snippet {
            println!("{snippet}");
        }
    }
    Ok(())
}

fn uninstall_zellij(command: muxe::cli::UninstallIntegrationCommand) -> Result<()> {
    let muxe::cli::IntegrationTarget::Zellij(options) = command.target;
    let paths = muxe::paths::resolve()?;
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the uninstall audit log")?;
    let explicit_policy = options.configuration_policy();
    let outcome = muxe::integration::uninstall(muxe::integration::UninstallInputs {
        config_dir: &paths.config_dir,
        cache_dir: &paths.cache_dir,
        explicit_policy,
        quiet: options.quiet,
        interactive: io::stdin().is_terminal(),
        zellij_config: options.zellij_config,
        asker: Some(&ask_for_consent),
        logger: Some(&logger),
    })
    .wrap_err("could not remove the Zellij bridge")?;
    if options.quiet {
        // Quiet keeps normal stdout empty, but unresolved artifacts that keep
        // the receipt (user-modified nodes, restore failures) still warn on
        // stderr. Nodes left purely by an explicit never-configure policy are
        // expected, not surprising, so they stay silent.
        let never_only = matches!(explicit_policy, Some(muxe::cli::ConfigurationPolicy::Never));
        if !never_only {
            for record in &outcome.unresolved {
                eprintln!("left: {}", record.reason);
            }
        }
    } else {
        println!(
            "removed bridge={} receipt={} restored={} removed_nodes={} unresolved={}",
            outcome.bridge_removed,
            outcome.receipt_removed,
            outcome.restored_nodes.len(),
            outcome.removed_nodes.len(),
            outcome.unresolved.len()
        );
        for record in &outcome.unresolved {
            println!("left: {}", record.reason);
        }
    }
    Ok(())
}
/// live registry; anything else fails closed.
fn detect_current_host(cache_dir: &Path) -> Result<muxe::lifecycle::DetectedHost> {
    if let Some(socket) = env::var_os("HERDR_SOCKET_PATH").filter(|value| !value.is_empty()) {
        return Ok(muxe::lifecycle::DetectedHost::Herdr {
            discovery_key: PathBuf::from(socket).to_string_lossy().into_owned(),
        });
    }
    if let Some(session) = env::var_os("ZELLIJ_SESSION_NAME").filter(|value| !value.is_empty()) {
        let session = session.to_string_lossy().into_owned();
        let session_key = muxe_adapter_api::HostDiscoveryKey::parse(session.as_str())?;
        let registry = muxe::lifecycle::Registry::open(cache_dir)
            .wrap_err("could not open the owner-only broker registry")?;
        let bridge_identity = registry
            .zellij_entry_for_session(&session_key)
            .wrap_err("could not read the owner-only broker registry")?
            .and_then(|entry| entry.bridge_identity().cloned())
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "no live Zellij broker serves session {session}; --host current requires invocation from a managed host"
                )
            })?;
        return Ok(muxe::lifecycle::DetectedHost::Zellij {
            session,
            bridge_identity,
        });
    }
    bail!("--host current requires invocation from a managed host")
}

/// Runs one activation across every live unit in scope: registry liveness
/// selects units, staged bridge bytes verify against the embedded producer
/// digest when Zellij can be selected, and every unit commits or rolls back
/// independently with a printed per-unit outcome.
async fn activate_brokers(command: muxe::cli::ActivateCommand) -> Result<()> {
    let paths = muxe::paths::resolve()?;
    let current = match command.host {
        HostScope::Current => Some(detect_current_host(&paths.cache_dir)?),
        _ => None,
    };
    let report = Box::pin(run_activation(command.host, current)).await?;
    for unit in &report.units {
        match unit {
            muxe::lifecycle::UnitOutcome::Committed { unit } => println!("committed {unit}"),
            muxe::lifecycle::UnitOutcome::Unchanged { unit } => println!("unchanged {unit}"),
            muxe::lifecycle::UnitOutcome::RolledBack { unit, reason } => {
                println!("rolled back {unit}: {reason}");
            }
            muxe::lifecycle::UnitOutcome::Failed { unit, reason } => {
                println!("failed {unit}: {reason}");
            }
        }
    }
    // A rolled-back unit restored the old broker, but the requested activation
    // still failed: report every outcome, then exit nonzero like any error so
    // CLI callers observe the original failure instead of a quiet success.
    if activation_incomplete(&report.units) {
        bail!("activation did not complete every selected unit");
    }
    Ok(())
}

/// Drives the activation transaction for one scope. CLI `activate` and
/// coldstart stale-record recovery share this exact path: a stale broker is
/// never replaced by a parallel transaction.
async fn run_activation(
    scope: HostScope,
    current: Option<muxe::lifecycle::DetectedHost>,
) -> Result<muxe::lifecycle::ActivateReport> {
    let paths = muxe::paths::resolve()?;
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the activation audit log")?;
    let record = muxe::compatibility::embedded_record()
        .wrap_err("could not load the embedded compatibility record")?;
    let registry = muxe::lifecycle::Registry::open(&paths.cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    let live = registry
        .probe()
        .wrap_err("could not probe the owner-only broker registry")?
        .live;
    let selected =
        muxe::lifecycle::activate::selected_host_requirements(live, scope, current.as_ref())
            .wrap_err("could not validate selected broker registry records")?;
    let herdr_selected = selected.herdr;
    let zellij_selected = selected.zellij;
    let herdr_binary = herdr_selected.then(herdr_binary_from_path).transpose()?;
    let zellij_exe = zellij_selected
        .then(|| {
            muxe_adapter_zellij::resolve_zellij_exe().map_err(|error| {
                color_eyre::eyre::eyre!("could not resolve the pinned Zellij executable: {error}")
            })
        })
        .transpose()?;
    let staged_bridge = if zellij_selected {
        Some(muxe::lifecycle::StagedBridge {
            bytes: packaged_bridge_bytes().wrap_err(
                "a Zellij unit is selected but no packaged bridge is installed alongside this binary",
            )?,
        })
    } else {
        None
    };
    let executable = env::current_exe().wrap_err("could not locate the running muxe executable")?;
    let config_file = paths.config_file();
    let cache_dir = paths.cache_dir.clone();
    let spawn_context = TargetSpawnContext {
        executable: &executable,
        config_file: &config_file,
        cache_dir: &cache_dir,
    };
    let herdr_spawn = HerdrTargetSpawn {
        context: &spawn_context,
        binary: herdr_binary.as_ref(),
    };
    let zellij_spawn = ZellijTargetSpawn {
        context: &spawn_context,
        binary: zellij_exe.as_ref(),
    };
    let select_spawn = SelectedTargetSpawns {
        herdr: &herdr_spawn,
        zellij: &zellij_spawn,
    };
    let preflight = muxe::lifecycle::LivePreflight {
        config_path: paths.config_file(),
        cache_dir: paths.cache_dir.clone(),
        herdr_binary: herdr_binary.clone(),
        zellij_exe: zellij_exe.clone(),
        logger: Some(&logger),
    };
    Box::pin(muxe::lifecycle::activate(muxe::lifecycle::ActivateInputs {
        config_dir: &paths.config_dir,
        cache_dir: &paths.cache_dir,
        target: record.handoff,
        staged_bridge,
        scope,
        current,
        control: &muxe::lifecycle::LiveControl,
        spawner: &muxe::lifecycle::ProcessSpawner,
        reloader: &muxe::lifecycle::ZellijCliReloader {
            program: preflight.zellij_exe.clone(),
        },
        preflight: &preflight,
        spawn_policy: &select_spawn,
        readiness_deadline: Duration::from_mins(2),
        poll_interval: Duration::from_millis(200),
        hooks: muxe::lifecycle::ActivateHooks::default(),
        logger: Some(&logger),
    }))
    .await
    .map_err(|error| color_eyre::eyre::eyre!("activation failed: {error}"))
}

/// Ensures a live Herdr broker for this runtime, cold-starting an ordinary
/// broker when none answers. A stale compiled record drives the same
/// activation transaction as CLI `activate` before return; a wrong identity
/// fails closed without a second broker. Returns the verified broker socket.
async fn ensure_herdr_broker(
    cache_dir: &Path,
    config_file: &Path,
    runtime: &muxe_adapter_herdr::HerdrRuntime,
) -> Result<PathBuf> {
    let record = muxe::compatibility::embedded_record()
        .wrap_err("could not load the embedded compatibility record")?;
    let discovery = runtime.identity().discovery_key.clone();
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Herdr, discovery.as_str())
        .wrap_err("could not derive the normal Herdr broker endpoint")?;
    let executable = env::current_exe().wrap_err("could not locate the running muxe executable")?;
    let inputs = muxe::lifecycle::ColdstartInputs {
        cache_dir,
        config_file,
        executable: &executable,
        endpoint,
        host: muxe::lifecycle::ColdstartHost::Herdr {
            discovery_key: discovery,
            live_server_id: muxe_protocol::wire::ServerId::new(
                runtime.identity().live_server_id.as_str(),
            ),
            herdr_binary: herdr_binary_from_path()?,
            herdr_socket: required_absolute_environment_path("HERDR_SOCKET_PATH")?,
        },
        current_record: record.handoff,
        spawner: &muxe::lifecycle::ProcessSpawner,
        control: &muxe::lifecycle::LiveControl,
        reloader: None::<&muxe::lifecycle::ZellijCliReloader>,
        readiness_deadline: Duration::from_mins(2),
        poll_interval: Duration::from_millis(200),
    };
    match muxe::lifecycle::ensure_broker(&inputs)
        .await
        .map_err(|error| color_eyre::eyre::eyre!("could not ensure the Herdr broker: {error}"))?
    {
        muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live.entry.into_socket()),
        muxe::lifecycle::ColdstartOutcome::StaleRecord(_) => {
            Box::pin(run_activation(HostScope::Herdr, None)).await?;
            match muxe::lifecycle::ensure_broker(&inputs)
                .await
                .map_err(|error| {
                    color_eyre::eyre::eyre!("could not re-verify the Herdr broker: {error}")
                })? {
                muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live.entry.into_socket()),
                muxe::lifecycle::ColdstartOutcome::StaleRecord(_) => {
                    bail!("activation did not converge the Herdr broker record")
                }
            }
        }
    }
}

/// Ensures a live Zellij broker for this session: a brokerless session
/// cold-starts one ordinary broker, reloads the stable bridge, and awaits
/// the fresh compatible round before return, never a second incompatible
/// broker. A stale record activates the invoking bridge group first.
async fn ensure_zellij_broker(
    cache_dir: &Path,
    config_file: &Path,
    session: &str,
    zellij_exe: &Path,
) -> Result<muxe::lifecycle::LiveBroker> {
    let record = muxe::compatibility::embedded_record()
        .wrap_err("could not load the embedded compatibility record")?;
    let session = muxe_adapter_api::HostDiscoveryKey::parse(session)?;
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Zellij, session.as_str())
        .wrap_err("could not derive the normal Zellij broker endpoint")?;
    let executable = env::current_exe().wrap_err("could not locate the running muxe executable")?;
    let reloader = muxe::lifecycle::ZellijCliReloader {
        program: Some(zellij_exe.to_path_buf()),
    };
    let inputs = muxe::lifecycle::ColdstartInputs {
        cache_dir,
        config_file,
        executable: &executable,
        endpoint,
        host: muxe::lifecycle::ColdstartHost::Zellij {
            session,
            zellij_exe: zellij_exe.to_path_buf(),
        },
        current_record: record.handoff,
        spawner: &muxe::lifecycle::ProcessSpawner,
        control: &muxe::lifecycle::LiveControl,
        reloader: Some(&reloader),
        readiness_deadline: Duration::from_mins(2),
        poll_interval: Duration::from_millis(200),
    };
    match muxe::lifecycle::ensure_broker(&inputs)
        .await
        .map_err(|error| color_eyre::eyre::eyre!("could not ensure the Zellij broker: {error}"))?
    {
        muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live),
        muxe::lifecycle::ColdstartOutcome::StaleRecord(_) => {
            let current = Some(detect_current_host(cache_dir)?);
            Box::pin(run_activation(HostScope::Current, current)).await?;
            match muxe::lifecycle::ensure_broker(&inputs)
                .await
                .map_err(|error| {
                    color_eyre::eyre::eyre!("could not re-verify the Zellij broker: {error}")
                })? {
                muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live),
                muxe::lifecycle::ColdstartOutcome::StaleRecord(_) => {
                    bail!("activation did not converge the Zellij broker record")
                }
            }
        }
    }
}

/// Activation exit boundary: any failed or rolled-back unit is a nonzero
/// exit, even when the rollback itself succeeded. Only all-committed or
/// unchanged reports succeed.
fn activation_incomplete(units: &[muxe::lifecycle::UnitOutcome]) -> bool {
    units.iter().any(|unit| {
        matches!(
            unit,
            muxe::lifecycle::UnitOutcome::Failed { .. }
                | muxe::lifecycle::UnitOutcome::RolledBack { .. }
        )
    })
}
/// Owned path inputs borrowed by one selected target renderer.
struct TargetSpawnContext<'a> {
    executable: &'a Path,
    config_file: &'a Path,
    cache_dir: &'a Path,
}

struct HerdrTargetSpawn<'a> {
    context: &'a TargetSpawnContext<'a>,
    binary: Option<&'a PathBuf>,
}

impl muxe::lifecycle::TargetSpawnPolicy for HerdrTargetSpawn<'_> {
    fn render(
        &self,
        member: &muxe::lifecycle::SpawnMember<'_>,
    ) -> Result<(PathBuf, Vec<OsString>), muxe::lifecycle::ActivateError> {
        let muxe::lifecycle::UnitKind::Herdr { host_hash } = member.unit else {
            return Err(muxe::lifecycle::ActivateError::Spawn(
                "Herdr renderer received a foreign journal unit".to_owned(),
            ));
        };
        if member.observed_host != ProtocolHostKind::Herdr
            || member.observed_bridge_identity.is_some()
            || member.observed_bridge_member.is_some()
            || member.observed_handoff_id.is_some()
            || muxe::lifecycle::journal::unit_hash(member.authority.member.as_str()) != *host_hash
        {
            return Err(muxe::lifecycle::ActivateError::Spawn(
                "observed Herdr member disagrees with journal unit".to_owned(),
            ));
        }
        let program = self.context.executable.to_path_buf();
        let args = muxe_broker::ServeHerdrSpawn {
            binary: program.clone(),
            socket: member.authority.endpoint.as_path().to_path_buf(),
            herdr_binary: self.binary.cloned().ok_or_else(|| {
                muxe::lifecycle::ActivateError::Spawn(
                    "no Herdr executable is installed for a Herdr target".to_owned(),
                )
            })?,
            herdr_socket: PathBuf::from(member.authority.member.as_str()),
            config: self.context.config_file.to_path_buf(),
            cache_dir: self.context.cache_dir.to_path_buf(),
            handoff: Some(member.authority.handoff_id),
            activation_journal: Some(member.journal_path.clone()),
        }
        .argv()
        .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
        Ok((program, args))
    }
}

struct ZellijTargetSpawn<'a> {
    context: &'a TargetSpawnContext<'a>,
    binary: Option<&'a PathBuf>,
}

impl muxe::lifecycle::TargetSpawnPolicy for ZellijTargetSpawn<'_> {
    fn render(
        &self,
        member: &muxe::lifecycle::SpawnMember<'_>,
    ) -> Result<(PathBuf, Vec<OsString>), muxe::lifecycle::ActivateError> {
        let muxe::lifecycle::UnitKind::Zellij { bridge_unit } = member.unit else {
            return Err(muxe::lifecycle::ActivateError::Spawn(
                "Zellij renderer received a foreign journal unit".to_owned(),
            ));
        };
        if member.observed_host != ProtocolHostKind::Zellij
            || member
                .observed_bridge_identity
                .is_none_or(|identity| identity.unit() != *bridge_unit)
            || member
                .observed_bridge_member
                .is_none_or(|id| id.as_str() != member.authority.member.as_str())
        {
            return Err(muxe::lifecycle::ActivateError::Spawn(
                "observed bridge member disagrees with journal unit".to_owned(),
            ));
        }
        let program = self.context.executable.to_path_buf();
        let args = muxe_broker::ServeZellijSpawn {
            binary: program.clone(),
            socket: member.authority.endpoint.as_path().to_path_buf(),
            zellij_exe: self.binary.cloned().ok_or_else(|| {
                muxe::lifecycle::ActivateError::Spawn(
                    "no Zellij executable is installed for a Zellij target".to_owned(),
                )
            })?,
            session: member.authority.member.as_str().to_owned(),
            config: self.context.config_file.to_path_buf(),
            cache_dir: self.context.cache_dir.to_path_buf(),
            handoff: Some(member.authority.handoff_id),
            activation_journal: Some(member.journal_path.clone()),
        }
        .argv()
        .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
        Ok((program, args))
    }
}

/// Selects a borrowed concrete renderer once per journal unit.
struct SelectedTargetSpawns<'a> {
    herdr: &'a HerdrTargetSpawn<'a>,
    zellij: &'a ZellijTargetSpawn<'a>,
}

impl muxe::lifecycle::TargetSpawnSelector for SelectedTargetSpawns<'_> {
    fn select(&self, unit: &muxe::lifecycle::UnitKind) -> &dyn muxe::lifecycle::TargetSpawnPolicy {
        match unit {
            muxe::lifecycle::UnitKind::Herdr { .. } => self.herdr,
            muxe::lifecycle::UnitKind::Zellij { .. } => self.zellij,
        }
    }
}

/// Appends one broker-service audit record. Failures to write the log never
/// change the service outcome; the caller still returns its own result.
fn serve_event(logger: &muxe::logging::Logger, host: &str, operation: &str, message: &str) {
    if let Ok(event) = muxe::logging::LogEvent::new(
        env!("CARGO_PKG_VERSION"),
        host,
        operation,
        message.chars().take(512).collect::<String>(),
    ) {
        let _ = logger.append(&event);
    }
}

fn broker_diagnostic_message(outcome: muxe_protocol::ExecutionOutcome) -> &'static str {
    match outcome {
        muxe_protocol::ExecutionOutcome::Failed => "detached execution failed",
        muxe_protocol::ExecutionOutcome::OutcomeUnknown => "detached execution outcome is unknown",
        muxe_protocol::ExecutionOutcome::Cancelled => "detached execution was cancelled",
        muxe_protocol::ExecutionOutcome::TimedOut => "detached execution timed out",
        muxe_protocol::ExecutionOutcome::Succeeded | muxe_protocol::ExecutionOutcome::Detached => {
            "detached execution completed"
        }
    }
}

struct DiagnosticConsumer {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl DiagnosticConsumer {
    async fn stop_and_join(self) {
        let _ = self.stop.send(());
        let _ = self.task.await;
    }
}

fn persist_broker_diagnostic(
    logger: &muxe::logging::Logger,
    host: &str,
    diagnostic: &muxe_broker::BrokerDiagnostic,
) {
    let event = muxe::logging::LogEvent::new(
        logger.version().to_owned(),
        host,
        "detached-execution",
        broker_diagnostic_message(diagnostic.outcome),
    )
    .map(|event| {
        event
            .with_request(format!("{:?}", diagnostic.execution))
            .with_code(format!("{:?}: {:?}", diagnostic.outcome, diagnostic.code))
    });
    if let Ok(event) = event {
        let _ = logger.append(&event);
    }
}

fn retain_broker_diagnostics(
    logger: Arc<muxe::logging::Logger>,
    host: &'static str,
    mut diagnostics: tokio::sync::mpsc::UnboundedReceiver<muxe_broker::BrokerDiagnostic>,
) -> DiagnosticConsumer {
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                diagnostic = diagnostics.recv() => match diagnostic {
                    Some(diagnostic) => persist_broker_diagnostic(&logger, host, &diagnostic),
                    None => return,
                },
                _ = &mut stopped => {
                    diagnostics.close();
                    while let Some(diagnostic) = diagnostics.recv().await {
                        persist_broker_diagnostic(&logger, host, &diagnostic);
                    }
                    return;
                }
            }
        }
    });
    DiagnosticConsumer { stop, task }
}

fn attest_broker_registration(
    server: &muxe_broker::BrokerServer,
    registration: &muxe::lifecycle::registry::Registration,
) -> Result<()> {
    let entry = registration.entry();
    let id = entry
        .registration_id
        .ok_or_else(|| color_eyre::eyre::eyre!("broker registration lacks its original token"))?;
    let proof = muxe_protocol::control::BrokerRegistrationProof::new(id, entry.started_at)
        .wrap_err("broker registration lacks its original timestamp")?;
    server
        .attest_registration(proof)
        .wrap_err("broker could not attest its registration")
}

/// Private broker child mode used only by the repository-owned cross-version fixture.
///
/// It accepts concrete paths from the coordinator, never an ambient command hook. A target reads
/// the durable journal and proves its own compatibility record, live Herdr identity, normal
/// endpoint, and nonzero handoff before it is allowed to bind.
#[expect(
    clippy::too_many_lines,
    reason = "broker serve transaction: endpoint lock, adapter connect, identity match, journal authorization, registry token, bind, and run form one ordered startup that must stay together to keep the construction/bind gap closed"
)]
async fn serve_herdr_broker(command: BrokerServeHerdrCommand) -> Result<()> {
    let logger = Arc::new(
        muxe::logging::Logger::open(&command.cache_dir, env!("CARGO_PKG_VERSION"))
            .wrap_err("could not open the broker service audit log")?,
    );
    // Earliest lock ownership, mirroring the Zellij path: the Herdr discovery
    // key is the server socket string, so the normal endpoint derives from
    // argv before any host contact. A live endpoint exits before adapter
    // construction; the held guard is consumed by bind below.
    let pre_endpoint = RuntimeEndpoint::for_host(
        ProtocolHostKind::Herdr,
        &command.herdr_socket.to_string_lossy(),
    )
    .wrap_err("could not derive the normal Herdr broker endpoint")?;
    if pre_endpoint.socket() != command.socket {
        bail!(
            "broker endpoint {} does not match the recorded normal Herdr endpoint {}",
            pre_endpoint.socket().display(),
            command.socket.display()
        );
    }
    let pre_lock = pre_endpoint.acquire_startup_lock().map_err(|error| {
        color_eyre::eyre::eyre!("another broker starter holds the Herdr endpoint: {error}")
    })?;
    if let Err(error) = pre_endpoint.remove_validated_stale_socket() {
        drop(pre_lock);
        return Err(color_eyre::eyre::eyre!(
            "could not claim the Herdr broker endpoint: {error}"
        ));
    }
    let adapter =
        muxe_adapter_herdr::HerdrAdapter::connect(muxe_adapter_herdr::HerdrAdapterConfig {
            socket_path: command.herdr_socket.clone(),
            herdr_binary: command.herdr_binary,
            cache_dir: command.cache_dir.clone(),
        })
        .await
        .wrap_err("could not connect the pinned Herdr server for broker startup")?;
    let broker = muxe_broker::Broker::load(adapter, &command.config)
        .await
        .wrap_err("could not load the broker configuration")?;
    let diagnostics = broker
        .take_diagnostics()
        .await
        .expect("new broker owns its diagnostic sink");
    let diagnostics_task = retain_broker_diagnostics(Arc::clone(&logger), "herdr", diagnostics);
    let live_server = broker
        .live_identity()
        .await
        .wrap_err("could not capture the pinned Herdr live identity")?;
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Herdr, &live_server.discovery_key)
        .wrap_err("could not derive the normal Herdr broker endpoint")?;
    if endpoint.socket() != command.socket {
        bail!(
            "broker endpoint {} does not match the recorded normal Herdr endpoint {}",
            endpoint.socket().display(),
            command.socket.display()
        );
    }
    let current = muxe::compatibility::embedded_record()?.handoff;
    let bootstrap = match (command.handoff, command.activation_journal) {
        (None, None) => muxe_broker::ActivationBootstrap::Running {
            current,
            bridge_unit: None,
        },
        (Some(handoff), Some(journal_path)) => {
            let journal = muxe::lifecycle::journal::read_journal(&journal_path)
                .wrap_err("could not read the durable activation journal")?;
            if !matches!(journal.unit, muxe::lifecycle::UnitKind::Herdr { .. })
                || journal.target_record != current
                || journal.directive() != muxe::lifecycle::TransactionDirective::Activate
            {
                bail!("activation journal does not authorize this Herdr target record");
            }
            let member_id =
                muxe::lifecycle::ActivationMemberId::new(live_server.discovery_key.clone())
                    .wrap_err("Herdr discovery key is not a valid activation member identity")?;
            let member = journal.recovery_member(&member_id, handoff).wrap_err(
                "activation journal does not authorize this Herdr host identity and handoff",
            )?;
            if member.endpoint().as_path() != command.socket {
                bail!("activation journal does not authorize this Herdr endpoint");
            }
            muxe_broker::ActivationBootstrap::Target {
                current,
                handoff,
                live_server: live_server.clone(),
                bridge_unit: None,
            }
        }
        _ => bail!("broker target startup requires both --handoff and --activation-journal"),
    };
    let registry = muxe::lifecycle::Registry::open(&command.cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    let mut entry = muxe::lifecycle::BrokerEntry::now(
        "herdr",
        live_server.discovery_key.clone(),
        command.socket.clone(),
        std::process::id(),
    );
    entry.registration_id = Some(
        muxe_protocol::control::BrokerRegistrationId::generate()
            .wrap_err("could not mint Herdr broker registration identity")?,
    );
    entry.live_server = Some(live_server.server_id.as_str().to_owned());
    // The Registration token scopes cleanup to this process's exact entry: an old
    // broker exiting after handoff must never erase the target's entry at the same
    // normal socket. Startup failure removes only the owned entry, never a peer's.
    let registration = registry
        .register_herdr(entry)
        .wrap_err("could not register the Herdr broker endpoint")?;
    let recovery = Arc::new(JournalRecovery {
        cache_dir: command.cache_dir.clone(),
        unit: muxe::lifecycle::UnitKind::Herdr {
            host_hash: muxe::lifecycle::journal::unit_hash(&live_server.discovery_key),
        },
        bridge_identity: None,
        bridge_member: None,
        discovery_key: muxe::lifecycle::ActivationMemberId::new(live_server.discovery_key.clone())
            .wrap_err("Herdr discovery key is not a valid activation member identity")?,
        zellij_exe: None,
        registration_id: registration.entry().registration_id,
    });
    let endpoint_path = endpoint.socket().display().to_string();
    // The held startup guard is consumed here, exactly like the Zellij path:
    // bind reuses the pre-connect claim, closing the construction/bind gap.
    let server = match muxe_broker::BrokerServer::start_activation_with_lock(
        Arc::clone(&broker),
        endpoint,
        bootstrap,
        Some(recovery),
        pre_lock,
    )
    .await
    {
        Ok(server) => {
            serve_event(
                &logger,
                "herdr",
                "broker-serve",
                &format!("serving {endpoint_path}"),
            );
            server
        }
        Err(error) => {
            serve_event(
                &logger,
                "herdr",
                "broker-serve",
                &format!("startup failed: {error}"),
            );
            let _ = registry.unregister_herdr(&registration);
            return Err(error).wrap_err("could not start the Herdr broker endpoint");
        }
    };
    if let Err(error) = attest_broker_registration(&server, &registration) {
        let _ = registry.unregister_herdr(&registration);
        return Err(error);
    }
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = server.run(shutdown_rx).await;
    diagnostics_task.stop_and_join().await;
    let _ = registry.unregister_herdr(&registration);
    serve_event(&logger, "herdr", "broker-serve", "stopped");
    result.wrap_err("Herdr broker service stopped unexpectedly")
}

/// Hidden Zellij broker child mode, mirroring `serve_herdr_broker`: fixed typed
/// inputs, normal-endpoint enforcement from the live session identity,
/// journal/handoff target authorization, and owner-token registry cleanup.
#[expect(
    clippy::too_many_lines,
    reason = "broker serve transaction: endpoint lock, adapter connect, identity match, journal and bridge authorization, registry token, bind, initial round, and run form one ordered startup that must stay together to keep the construction/bind gap closed"
)]
async fn serve_zellij_broker(command: BrokerServeZellijCommand) -> Result<()> {
    let logger = Arc::new(
        muxe::logging::Logger::open(&command.cache_dir, env!("CARGO_PKG_VERSION"))
            .wrap_err("could not open the broker service audit log")?,
    );
    let serve_config_dir = command.config.parent().ok_or_else(|| {
        color_eyre::eyre::eyre!("broker configuration file has no parent directory")
    })?;
    let bridge_identity = muxe::integration::bridge_identity(serve_config_dir)
        .wrap_err("could not resolve canonical Zellij bridge authority")?;
    let mut unit_guard = if command.handoff.is_none() {
        let cache_dir = command.cache_dir.clone();
        let identity = bridge_identity.clone();
        Some(
            tokio::task::spawn_blocking(move || {
                muxe::lifecycle::BridgeUnitGuard::acquire_blocking(&cache_dir, identity)
            })
            .await
            .wrap_err("Zellij bridge-unit guard task failed")?
            .wrap_err("could not acquire Zellij bridge-unit guard before endpoint startup")?,
        )
    } else {
        None
    };
    // Earliest lock ownership: serialize with concurrent starters before
    // touching the host, so two children never hold overlapping adapters. A
    // live endpoint means another broker won: exit before adapter
    // construction. The guard drops before start_inner re-acquires; passing
    // the held guard through bind awaits the broker-owned lock API.
    let pre_endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Zellij, &command.session)
        .wrap_err("could not derive the normal Zellij broker endpoint")?;
    if pre_endpoint.socket() != command.socket {
        bail!(
            "broker endpoint {} does not match the recorded normal Zellij endpoint {}",
            pre_endpoint.socket().display(),
            command.socket.display()
        );
    }
    let pre_lock = pre_endpoint.acquire_startup_lock().map_err(|error| {
        color_eyre::eyre::eyre!("another broker starter holds the Zellij endpoint: {error}")
    })?;
    if let Err(error) = pre_endpoint.remove_validated_stale_socket() {
        drop(pre_lock);
        return Err(color_eyre::eyre::eyre!(
            "could not claim the Zellij broker endpoint: {error}"
        ));
    }
    // Ordinary brokers subscribe immediately. A target binds TargetGated
    // control with dormant pipes, then subscribes only after its exact
    // journal records the replacement bridge reloaded in every session.
    let is_target = command.handoff.is_some();
    let adapter_config = muxe_adapter_zellij::ZellijAdapterConfig {
        session_name: command.session.clone(),
        zellij_exe: command.zellij_exe.clone(),
        readiness_gate: muxe_adapter_zellij::ReadinessGate::new(
            command.cache_dir.clone(),
            bridge_identity.unit(),
        ),
    };
    let (adapter, deferred_pipes) = if is_target {
        let (adapter, pipes) = muxe_adapter_zellij::ZellijAdapter::connect_deferred(adapter_config)
            .wrap_err("could not reserve dormant Zellij target pipes")?;
        (std::sync::Arc::new(adapter), Some(pipes))
    } else {
        (
            std::sync::Arc::new(
                muxe_adapter_zellij::ZellijAdapter::connect(adapter_config)
                    .await
                    .wrap_err("could not connect the pinned Zellij session for broker startup")?,
            ),
            None,
        )
    };
    // The adapter reserves a gated identity without any bridge registration.
    let reserved = if is_target {
        let identity = adapter
            .reserve_startup_identity()
            .wrap_err("could not reserve the gated Zellij target identity")?;
        Some(muxe_protocol::LiveServerIdentity {
            host: ProtocolHostKind::Zellij,
            discovery_key: identity.discovery_key.as_str().to_owned(),
            server_id: muxe_protocol::ServerId::new(identity.live_server_id.as_str()),
        })
    } else {
        let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
        establish_initial_round_until(&adapter, deadline, &logger)
            .await
            .wrap_err("Zellij initial census round never established")?;
        None
    };
    let adapter_object: std::sync::Arc<dyn muxe_adapter_api::HostAdapter> = adapter.clone();
    let broker = muxe_broker::Broker::load(adapter_object, &command.config)
        .await
        .wrap_err("could not load the broker configuration")?;
    let diagnostics = broker
        .take_diagnostics()
        .await
        .expect("new broker owns its diagnostic sink");
    let diagnostics_task = retain_broker_diagnostics(Arc::clone(&logger), "zellij", diagnostics);
    let live_server = if let Some(reserved) = reserved {
        reserved
    } else {
        broker
            .live_identity()
            .await
            .wrap_err("could not capture the pinned Zellij live identity")?
    };
    if live_server.discovery_key != command.session {
        bail!(
            "broker live session {} does not match the requested Zellij session {}",
            live_server.discovery_key,
            command.session
        );
    }
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Zellij, &live_server.discovery_key)
        .wrap_err("could not derive the normal Zellij broker endpoint")?;
    if endpoint.socket() != command.socket {
        bail!(
            "broker endpoint {} does not match the recorded normal Zellij endpoint {}",
            endpoint.socket().display(),
            command.socket.display()
        );
    }
    let current = muxe::compatibility::embedded_record()?.handoff;
    let expected_target = is_target.then(|| current.clone());
    // A half pair bails in the bootstrap match below.
    let (bootstrap, target_registration, registration_handoff, target_wait) = match (
        command.handoff,
        command.activation_journal,
    ) {
        (None, None) => (
            muxe_broker::ActivationBootstrap::Running {
                current,
                bridge_unit: Some(bridge_identity.unit()),
            },
            None,
            None,
            None,
        ),
        (Some(handoff), Some(journal_path)) => {
            let journal = muxe::lifecycle::journal::read_journal(&journal_path)
                .wrap_err("could not read the durable activation journal")?;
            if !matches!(journal.unit, muxe::lifecycle::UnitKind::Zellij { .. })
                || journal.target_record != current
                || journal.bridge_identity.as_ref() != Some(&bridge_identity)
                || journal.directive() != muxe::lifecycle::TransactionDirective::Activate
            {
                bail!(
                    "activation journal does not authorize this Zellij target record and bridge identity"
                );
            }
            let capability = journal
                .target_registration_capability(
                    &live_server.discovery_key,
                    &command.socket,
                    handoff,
                )
                .wrap_err("activation journal does not authorize target registration")?;
            (
                muxe_broker::ActivationBootstrap::Target {
                    current,
                    handoff,
                    live_server: live_server.clone(),
                    bridge_unit: Some(bridge_identity.unit()),
                },
                Some(capability),
                Some(handoff),
                Some((journal_path, journal.activation_id)),
            )
        }
        _ => bail!("broker target startup requires both --handoff and --activation-journal"),
    };

    let registry = muxe::lifecycle::Registry::open(&command.cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    if let Some(receipt) = muxe::integration::receipt::load(bridge_identity.directory())
        .wrap_err("could not read the Zellij integration receipt")?
        && receipt.bridge.bridge_identity != bridge_identity
    {
        bail!(
            "integration receipt names bridge identity {} but this broker serves {}; refusing a divergent registration",
            receipt.bridge.bridge_identity,
            bridge_identity
        );
    }
    let mut entry = muxe::lifecycle::BrokerEntry::now(
        "zellij",
        live_server.discovery_key.clone(),
        command.socket.clone(),
        std::process::id(),
    );
    entry.bridge_identity = Some(bridge_identity.clone());
    entry.bridge_member = Some(
        muxe::lifecycle::BridgeMemberId::new(live_server.discovery_key.clone())
            .wrap_err("invalid Zellij logical member")?,
    );
    entry.handoff_id = registration_handoff;
    entry.live_server = Some(live_server.server_id.as_str().to_owned());
    entry.registration_id = Some(
        muxe_protocol::control::BrokerRegistrationId::generate()
            .wrap_err("could not mint Zellij broker registration identity")?,
    );
    let recovery = Arc::new(JournalRecovery {
        cache_dir: command.cache_dir.clone(),
        unit: muxe::lifecycle::UnitKind::Zellij {
            bridge_unit: bridge_identity.unit(),
        },
        bridge_identity: Some(bridge_identity.clone()),
        bridge_member: entry.bridge_member.clone(),
        discovery_key: muxe::lifecycle::ActivationMemberId::new(live_server.discovery_key.clone())
            .wrap_err("Zellij discovery key is not a valid activation member identity")?,
        zellij_exe: Some(command.zellij_exe.clone()),
        registration_id: entry.registration_id,
    });
    // The endpoint was claimed before adapter construction and the guard is
    // consumed here: bind reuses the held lock instead of re-acquiring, so no
    // gap admits a second child between construction and bind.
    let recovery_port: Arc<dyn muxe_broker::RecoveryJournal> = recovery.clone();
    let server = match muxe_broker::BrokerServer::start_activation_with_lock(
        Arc::clone(&broker),
        endpoint,
        bootstrap,
        Some(recovery_port),
        pre_lock,
    )
    .await
    {
        Ok(server) => server,
        Err(error) => {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                &format!("startup failed: {error}"),
            );
            return Err(error).wrap_err("could not start the Zellij broker endpoint");
        }
    };
    let registration_result: Result<muxe::lifecycle::registry::Registration> = async {
        let gate = muxe_adapter_zellij::ReadinessGate::new(
            command.cache_dir.clone(),
            bridge_identity.unit(),
        )
        .exclusive(std::time::Duration::from_secs(2))
        .await
        .wrap_err("could not serialize Zellij target registry publication")?;
        let registration = if let Some(capability) = target_registration.as_ref() {
            registry.register_zellij_target(capability, entry)
        } else {
            registry.register_zellij(
                unit_guard
                    .as_ref()
                    .expect("ordinary Zellij startup owns the unit guard"),
                entry,
            )
        }
        .wrap_err("could not register the Zellij broker endpoint")?;
        drop(gate);
        Ok(registration)
    }
    .await;
    let registration = match registration_result {
        Ok(registration) => registration,
        Err(error) => {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                &format!("registration failed after endpoint bind: {error}"),
            );
            drop(server);
            let _ = adapter.shutdown().await;
            diagnostics_task.stop_and_join().await;
            return Err(error);
        }
    };
    if let Err(error) = attest_broker_registration(&server, &registration) {
        if let Some(guard) = unit_guard.as_ref() {
            let _ = registry.unregister_zellij(guard, &registration);
        }
        drop(server);
        let _ = adapter.shutdown().await;
        diagnostics_task.stop_and_join().await;
        return Err(error);
    }
    serve_event(
        &logger,
        "zellij",
        "broker-serve",
        &format!("serving {}", command.socket.display()),
    );
    drop(unit_guard.take());
    if is_target {
        // TargetGated control is bound before the swap, but neither pipe
        // subscribes to the predecessor bridge. The exact durable reload
        // barrier opens both children; only that subscription can prove Ready.
        // The same deadline bounds journal wait, child launch, and census.
        let (journal_path, activation_id) =
            target_wait.expect("validated target owns an activation journal");
        let handoff = registration_handoff.expect("validated target owns a handoff");
        let deferred_pipes = deferred_pipes.expect("validated target has dormant pipes");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server_handle = tokio::spawn(async move { server.run(shutdown_rx).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
        let covered = async {
            wait_for_target_bridge_reload(
                TargetBridgeWait {
                    journal_path: &journal_path,
                    activation_id,
                    bridge_identity: &bridge_identity,
                    member: &recovery.discovery_key,
                    endpoint: &command.socket,
                    handoff,
                    target: expected_target.as_ref().expect("validated target record"),
                },
                deadline,
            )
            .await?;
            tokio::time::timeout(
                deadline.saturating_duration_since(std::time::Instant::now()),
                deferred_pipes.start(),
            )
            .await
            .map_err(|_| color_eyre::eyre::eyre!("Zellij target pipe startup exceeded its deadline"))?
            .wrap_err("could not start Zellij target pipes after bridge reload")?;
            establish_initial_round_until(&adapter, deadline, &logger).await?;
            let identity = broker
                .live_identity()
                .await
                .wrap_err("could not read the covered Zellij target identity")?;
            if identity != live_server {
                bail!(
                    "covered Zellij target identity differs from the gated registration: expected {live_server:?}, found {identity:?}"
                );
            }
            Ok::<(), color_eyre::Report>(())
        }
        .await;
        if let Err(error) = covered {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                &format!("target census or identity verification failed: {error}"),
            );
            let _ = shutdown_tx.send(true);
            let _ = server_handle.await;
            diagnostics_task.stop_and_join().await;
            cleanup_zellij_registration(
                &command.cache_dir,
                &registry,
                &bridge_identity,
                None,
                &registration,
            )
            .wrap_err("could not remove failed Zellij target registration")?;
            return Err(error).wrap_err("Zellij target census or identity verification failed");
        }
        let joined = server_handle.await;
        diagnostics_task.stop_and_join().await;
        cleanup_zellij_registration(
            &command.cache_dir,
            &registry,
            &bridge_identity,
            None,
            &registration,
        )
        .wrap_err("could not remove stopped Zellij target registration")?;
        serve_event(&logger, "zellij", "broker-serve", "stopped");
        return joined
            .wrap_err("Zellij broker service task ended unexpectedly")?
            .wrap_err("Zellij broker service stopped unexpectedly");
    }
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = server.run(shutdown_rx).await;
    diagnostics_task.stop_and_join().await;
    cleanup_zellij_registration(
        &command.cache_dir,
        &registry,
        &bridge_identity,
        None,
        &registration,
    )
    .wrap_err("could not remove stopped Zellij registration")?;
    serve_event(&logger, "zellij", "broker-serve", "stopped");
    result.wrap_err("Zellij broker service stopped unexpectedly")
}
fn cleanup_zellij_registration(
    cache_dir: &Path,
    registry: &muxe::lifecycle::Registry,
    identity: &muxe::paths::BridgeIdentity,
    held_guard: Option<&muxe::lifecycle::BridgeUnitGuard>,
    registration: &muxe::lifecycle::registry::Registration,
) -> Result<(), muxe::lifecycle::registry::RegistryError> {
    if let Some(guard) = held_guard {
        registry.unregister_zellij(guard, registration)?;
        return Ok(());
    }
    let guard = muxe::lifecycle::BridgeUnitGuard::acquire_blocking(cache_dir, identity.clone())?;
    registry.unregister_zellij(&guard, registration)?;
    Ok(())
}

/// Exact durable authority that permits one gated target to subscribe after
/// the bridge replacement was reloaded in every recorded session.
struct TargetBridgeWait<'a> {
    journal_path: &'a Path,
    activation_id: muxe::lifecycle::ActivationId,
    bridge_identity: &'a muxe::paths::BridgeIdentity,
    member: &'a muxe::lifecycle::ActivationMemberId,
    endpoint: &'a Path,
    handoff: muxe_protocol::HandoffId,
    target: &'a muxe_protocol::control::CompatibilityRecord,
}

async fn wait_for_target_bridge_reload(
    expected: TargetBridgeWait<'_>,
    deadline: std::time::Instant,
) -> Result<()> {
    loop {
        let journal = muxe::lifecycle::journal::read_journal(expected.journal_path)
            .wrap_err("could not inspect gated target's activation journal")?;
        if journal.activation_id != expected.activation_id
            || journal.unit
                != (muxe::lifecycle::UnitKind::Zellij {
                    bridge_unit: expected.bridge_identity.unit(),
                })
            || journal.bridge_identity.as_ref() != Some(expected.bridge_identity)
            || journal.target_record != *expected.target
            || journal.directive() != muxe::lifecycle::TransactionDirective::Activate
            || !journal
                .recovery_member(expected.member, expected.handoff)
                .is_ok_and(|member| member.endpoint().as_path() == expected.endpoint)
        {
            bail!("gated Zellij target lost its exact journal authority before bridge reload");
        }
        if journal.bridge().is_some_and(|bridge| {
            bridge.progress == muxe::lifecycle::BridgeProgress::TargetReloaded
        }) {
            return Ok(());
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            bail!("gated Zellij target waited past its deadline for durable bridge reload");
        }
        tokio::time::sleep(remaining.min(std::time::Duration::from_millis(100))).await;
    }
}

/// Retries the inherent initial census round until success or the outer
/// deadline. After an unsuccessful round, the next attempt first replaces
/// the event child and reissues its one-shot subscription. This recovers when
/// a bridge loaded after the prior broadcast while preserving freshness:
/// registrations from the displaced child carry an older install epoch and
/// cannot cover the retried census. Every establish or refresh call remains
/// bounded by the outer deadline.
async fn establish_initial_round_until(
    adapter: &std::sync::Arc<muxe_adapter_zellij::ZellijAdapter>,
    deadline: std::time::Instant,
    logger: &muxe::logging::Logger,
) -> Result<()> {
    let mut last_error = String::from("startup budget elapsed before the first attempt");
    let mut retrying = false;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        if retrying {
            match tokio::time::timeout(remaining, adapter.refresh_initial_subscription()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let error = error.to_string();
                    if last_error != error {
                        serve_event(
                            logger,
                            "zellij",
                            "broker-serve",
                            &format!("initial subscription refresh failed: {error}"),
                        );
                    }
                    last_error = error;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    continue;
                }
                Err(_) => {
                    last_error = String::from(
                        "initial subscription refresh stalled past the startup budget",
                    );
                    serve_event(logger, "zellij", "broker-serve", &last_error);
                    break;
                }
            }
        }
        retrying = true;
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, adapter.establish_initial_round()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => {
                let error = error.to_string();
                if last_error != error {
                    serve_event(
                        logger,
                        "zellij",
                        "broker-serve",
                        &format!("initial census attempt failed: {error}"),
                    );
                }
                last_error = error;
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(_) => {
                last_error = String::from("initial census round stalled past the startup budget");
                serve_event(
                    logger,
                    "zellij",
                    "broker-serve",
                    &format!("initial census attempt failed: {last_error}"),
                );
                break;
            }
        }
    }
    Err(color_eyre::eyre::eyre!("{last_error}"))
}

struct JournalRecoveryPermit {
    path: PathBuf,
    discovery_key: muxe::lifecycle::ActivationMemberId,
    server_id: muxe_protocol::wire::ServerId,
    zellij_exe: Option<PathBuf>,
    lock: Mutex<Option<muxe::lifecycle::journal::UnitLock>>,
}

impl JournalRecoveryPermit {
    async fn continue_after_ack(
        &self,
        cache_dir: &Path,
        journal: &mut muxe::lifecycle::journal::ActivationJournal,
    ) -> std::result::Result<(), String> {
        if journal.directive() == muxe::lifecycle::TransactionDirective::RollBack {
            let reloader = muxe::lifecycle::ZellijCliReloader {
                program: self.zellij_exe.clone(),
            };
            match muxe::lifecycle::activate::continue_broker_rollback(
                cache_dir,
                &muxe::lifecycle::LiveControl,
                &reloader,
                journal,
                &self.path,
                &self.discovery_key,
            )
            .await
            .map_err(|error| format!("cannot continue rollback after ack: {error}"))?
            {
                muxe::lifecycle::BrokerRollbackOutcome::ResumeLocal => {
                    return Err("rollback requested a duplicate local resume after ack".to_owned());
                }
                muxe::lifecycle::BrokerRollbackOutcome::AwaitingPeers
                | muxe::lifecycle::BrokerRollbackOutcome::Complete => {}
            }
        } else if journal.directive() == muxe::lifecycle::TransactionDirective::Commit {
            muxe::lifecycle::activate::finish_acknowledged_commit(cache_dir, journal, &self.path)
                .map_err(|error| format!("cannot finish broker-acknowledged commit: {error}"))?;
        }
        Ok(())
    }
}

impl muxe_broker::RecoveryPermit for JournalRecoveryPermit {
    fn acknowledge<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
        ack: muxe_broker::RecoveryAck,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let unit_lock = self
                .lock
                .lock()
                .map_err(|_| "recovery permit lock poisoned".to_owned())?
                .take()
                .ok_or_else(|| "recovery permit was already consumed".to_owned())?;
            let mut journal = muxe::lifecycle::journal::read_journal(&self.path)
                .map_err(|error| format!("cannot read recovery journal for ack: {error}"))?;
            if ack == muxe_broker::RecoveryAck::OldCommitted {
                let member = journal
                    .recovery_member(&self.discovery_key, *handoff)
                    .map_err(|error| format!("old retirement member unauthorized: {error}"))?;
                let directory = self
                    .path
                    .parent()
                    .ok_or_else(|| "recovery receipt directory is missing".to_owned())?;
                if !muxe::lifecycle::journal::has_old_retirement_receipt(
                    directory, &journal, member,
                )
                .map_err(|error| format!("old retirement proof invalid: {error}"))?
                {
                    return Err("old retirement proof is absent after stop barrier".to_owned());
                }
            }
            let cache_dir = self
                .path
                .parent()
                .and_then(Path::parent)
                .ok_or_else(|| "recovery journal cache directory is missing".to_owned())?;
            match ack {
                muxe_broker::RecoveryAck::TargetRetired => {
                    muxe::lifecycle::journal::acknowledge_remote_target_retirement(
                        cache_dir,
                        &mut journal,
                        &self.discovery_key,
                        *handoff,
                        &self.server_id,
                    )
                    .map_err(|error| {
                        format!("cannot persist target retirement acknowledgement: {error}")
                    })?;
                }
                ack => {
                    let ack = match ack {
                        muxe_broker::RecoveryAck::Resumed => {
                            muxe::lifecycle::journal::BrokerRecoveryAck::Resumed
                        }
                        muxe_broker::RecoveryAck::OldCommitted => {
                            muxe::lifecycle::journal::BrokerRecoveryAck::OldCommitted
                        }
                        muxe_broker::RecoveryAck::TargetCommitted => {
                            muxe::lifecycle::journal::BrokerRecoveryAck::TargetCommitted
                        }
                        muxe_broker::RecoveryAck::TargetRetired => unreachable!(),
                    };
                    journal
                        .acknowledge_broker(&self.discovery_key, *handoff, ack)
                        .map_err(|error| {
                            format!("cannot apply recovery acknowledgement: {error}")
                        })?;
                    muxe::lifecycle::journal::write_journal(cache_dir, &journal).map_err(
                        |error| format!("cannot persist recovery acknowledgement: {error}"),
                    )?;
                }
            }
            self.continue_after_ack(cache_dir, &mut journal).await?;
            drop(unit_lock);
            Ok(())
        })
    }
}

/// Owner-side broker adapter for the transaction journal's single directive.
struct JournalRecovery {
    cache_dir: PathBuf,
    unit: muxe::lifecycle::UnitKind,
    bridge_identity: Option<muxe::paths::BridgeIdentity>,
    bridge_member: Option<muxe::lifecycle::BridgeMemberId>,
    discovery_key: muxe::lifecycle::ActivationMemberId,
    registration_id: Option<muxe_protocol::control::BrokerRegistrationId>,
    zellij_exe: Option<PathBuf>,
}

impl JournalRecovery {
    fn validate_target_incarnation(
        &self,
        journal: &muxe::lifecycle::journal::ActivationJournal,
        member: &muxe::lifecycle::journal::TransactionMember,
        status: &muxe_protocol::control::ActivationStatus,
    ) -> std::result::Result<(), String> {
        let zellij = matches!(self.unit, muxe::lifecycle::UnitKind::Zellij { .. });
        if journal.unit != self.unit
            || journal.directive() != muxe::lifecycle::TransactionDirective::Commit
            || !journal.has_commit_certificate()
            || status.current != journal.target_record
            || status.handoff_id != Some(member.handoff_id())
            || status.lifecycle != muxe_protocol::control::LifecycleState::Running
            || status.target.is_some()
            || status.live_server.discovery_key != member.member().as_str()
            || status.live_server.host
                != if zellij {
                    ProtocolHostKind::Zellij
                } else {
                    ProtocolHostKind::Herdr
                }
            || !muxe::lifecycle::activate::status_attests_journal(status, journal)
        {
            return Err("target Commit lacks exact proof-era journal and broker status".to_owned());
        }
        let rows = muxe::lifecycle::Registry::open(&self.cache_dir)
            .and_then(|registry| registry.entries())
            .map_err(|error| format!("target Commit registry unavailable: {error}"))?;
        let mut matching = rows
            .iter()
            .filter(|row| row.socket == member.endpoint().as_path());
        let row = matching
            .next()
            .ok_or_else(|| "target Commit lacks its original registry incarnation".to_owned())?;
        if matching.next().is_some()
            || row.host_kind != if zellij { "zellij" } else { "herdr" }
            || row.discovery_key != self.discovery_key.as_str()
            || row.server_pid != std::process::id()
            || row.bridge_identity != self.bridge_identity
            || row.bridge_member != self.bridge_member
            || row.handoff_id != zellij.then_some(member.handoff_id())
            || row.registration_id != self.registration_id
            || journal
                .ready_proof()
                .and_then(|proof| proof.member(&member.id))
                .is_none_or(|proof| !proof.matches(row, &status.live_server.server_id))
        {
            return Err("target Commit registry identity differs from sealed Ready".to_owned());
        }
        Ok(())
    }

    fn validate_old_incarnation(
        &self,
        journal: &muxe::lifecycle::journal::ActivationJournal,
        member: &muxe::lifecycle::journal::TransactionMember,
        status: &muxe_protocol::control::ActivationStatus,
    ) -> std::result::Result<(), String> {
        if journal.directive() != muxe::lifecycle::TransactionDirective::Commit
            || !journal.has_commit_certificate()
            || status.current != member.old_record
            || status.handoff_id != Some(member.handoff_id())
            || status.live_server.discovery_key != member.member().as_str()
            || !muxe::lifecycle::activate::status_attests_journal(status, journal)
            || !matches!(
                status.lifecycle,
                muxe_protocol::control::LifecycleState::Draining
                    | muxe_protocol::control::LifecycleState::SupervisorOnly
            )
            || (status.lifecycle == muxe_protocol::control::LifecycleState::Draining
                && status.target.as_ref() != Some(&journal.target_record))
            || (status.lifecycle == muxe_protocol::control::LifecycleState::SupervisorOnly
                && status.target.is_some())
        {
            return Err("old retirement lacks exact Ready and draining handoff".to_owned());
        }
        let mut entries = journal.old_registry.iter().filter(|entry| {
            entry.socket == member.endpoint().as_path()
                && entry.discovery_key == member.member().as_str()
        });
        let row = entries
            .next()
            .ok_or_else(|| "old retirement lacks recorded broker incarnation".to_owned())?;
        if entries.next().is_some()
            || row.server_pid != std::process::id()
            || row.registration_id != self.registration_id
            || row
                .registration_id
                .is_none_or(muxe_protocol::control::BrokerRegistrationId::is_zero)
            || row.live_server.as_deref() != Some(status.live_server.server_id.as_str())
            || row.bridge_identity != self.bridge_identity
            || row.bridge_member != self.bridge_member
        {
            return Err("old retirement differs from recorded broker incarnation".to_owned());
        }
        Ok(())
    }
}

impl muxe_broker::RecoveryJournal for JournalRecovery {
    #[expect(
        clippy::too_many_lines,
        reason = "recovery keeps lock acquisition, exact authority validation, durable directive selection, and permit construction in one auditable critical section"
    )]
    fn recovery_decision<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
        local_status: &'a muxe_protocol::control::ActivationStatus,
    ) -> std::pin::Pin<Box<dyn Future<Output = muxe_broker::RecoveryDecision> + Send + 'a>> {
        Box::pin(async move {
            let path = muxe::lifecycle::journal::activation_dir(&self.cache_dir)
                .join(self.unit.journal_name());
            match std::fs::symlink_metadata(&path) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return muxe_broker::RecoveryDecision::NoJournal;
                }
                Err(error) => {
                    return muxe_broker::RecoveryDecision::Preserve {
                        reason: format!(
                            "cannot establish activation journal state at {}: {error}",
                            path.display()
                        ),
                    };
                }
            }
            let lock_path = path.clone();
            let lock = match tokio::task::spawn_blocking(move || {
                muxe::lifecycle::journal::acquire_journal_lock_blocking(&lock_path)
            })
            .await
            {
                Ok(Ok(lock)) => lock,
                Ok(Err(error)) => {
                    return muxe_broker::RecoveryDecision::Preserve {
                        reason: format!("recovery unit lock acquisition failed: {error}"),
                    };
                }
                Err(error) => {
                    return muxe_broker::RecoveryDecision::Preserve {
                        reason: format!("recovery unit lock task failed: {error}"),
                    };
                }
            };
            let preserve = |reason: &str| muxe_broker::RecoveryDecision::Preserve {
                reason: reason.to_owned(),
            };
            let Ok(mut journal) = muxe::lifecycle::journal::read_journal(&path) else {
                return preserve("activation journal is corrupt, unsupported, or inconsistent");
            };
            if journal.unit != self.unit {
                return preserve("activation journal does not match this typed unit");
            }
            match &self.unit {
                muxe::lifecycle::UnitKind::Zellij { bridge_unit } => {
                    let Some(identity) = self.bridge_identity.as_ref() else {
                        return preserve("Zellij recovery lacks canonical bridge identity");
                    };
                    let Some(member) = self.bridge_member.as_ref() else {
                        return preserve("Zellij recovery lacks typed bridge member");
                    };
                    if identity.unit() != *bridge_unit
                        || journal.bridge_identity.as_ref() != Some(identity)
                        || !journal
                            .member_census
                            .as_ref()
                            .is_some_and(|census| census.members().contains(member))
                    {
                        return preserve(
                            "activation journal canonical bridge authority does not match this broker",
                        );
                    }
                }
                muxe::lifecycle::UnitKind::Herdr { .. } => {
                    if self.bridge_identity.is_some() || self.bridge_member.is_some() {
                        return preserve("Herdr recovery carries Zellij bridge authority");
                    }
                }
            }
            let Ok(member) = journal.recovery_member(&self.discovery_key, *handoff) else {
                return preserve("broker identity and handoff do not match the transaction member");
            };
            let expected_host = if matches!(self.unit, muxe::lifecycle::UnitKind::Zellij { .. }) {
                ProtocolHostKind::Zellij
            } else {
                ProtocolHostKind::Herdr
            };
            if local_status.live_server.host != expected_host
                || local_status.live_server.discovery_key != self.discovery_key.as_str()
                || !muxe::lifecycle::activate::status_attests_journal(local_status, &journal)
            {
                return preserve(
                    "broker status does not attest the exact journal host and bridge unit",
                );
            }
            if journal.directive() == muxe::lifecycle::TransactionDirective::Commit
                && !journal.has_commit_certificate()
            {
                return preserve("Ready journal lacks an exact target incarnation certificate");
            }
            if journal.directive() == muxe::lifecycle::TransactionDirective::Commit {
                let old_role = local_status.current == member.old_record
                    && local_status.handoff_id == Some(*handoff)
                    && match local_status.lifecycle {
                        muxe_protocol::control::LifecycleState::Draining => {
                            local_status.target.as_ref() == Some(&journal.target_record)
                        }
                        muxe_protocol::control::LifecycleState::SupervisorOnly => {
                            local_status.target.is_none()
                        }
                        _ => false,
                    };
                let target_role = local_status.current == journal.target_record
                    && local_status.handoff_id == Some(*handoff)
                    && local_status.lifecycle == muxe_protocol::control::LifecycleState::Running
                    && local_status.target.is_none();
                if !old_role && !target_role {
                    return preserve(
                        "commit permit lacks exact old or target journal-authorized live status",
                    );
                }
                if old_role
                    && let Err(reason) =
                        self.validate_old_incarnation(&journal, member, local_status)
                {
                    return preserve(&reason);
                }
                if target_role
                    && let Err(reason) =
                        self.validate_target_incarnation(&journal, member, local_status)
                {
                    return preserve(&reason);
                }
            }
            let old_commit_index = (journal.directive()
                == muxe::lifecycle::TransactionDirective::Commit
                && local_status.lifecycle == muxe_protocol::control::LifecycleState::Draining
                && local_status.current == member.old_record
                && member.old == muxe::lifecycle::journal::OldMemberProgress::Drained)
                .then(|| {
                    journal
                        .members()
                        .iter()
                        .position(|candidate| candidate.id == member.id)
                        .expect("validated recovery member remains in journal")
                });
            if matches!(
                journal.directive(),
                muxe::lifecycle::TransactionDirective::CleanupCommitted
                    | muxe::lifecycle::TransactionDirective::CleanupRolledBack
            ) {
                return match muxe::lifecycle::activate::cleanup_terminal_transaction(
                    &journal, &path,
                ) {
                    Ok(()) => muxe_broker::RecoveryDecision::CleanupComplete,
                    Err(error) => preserve(&format!(
                        "terminal transaction cleanup could not converge: {error}"
                    )),
                };
            }
            if matches!(
                journal.directive(),
                muxe::lifecycle::TransactionDirective::Prepare
                    | muxe::lifecycle::TransactionDirective::Activate
            ) {
                journal.enter_rollback("broker disconnect selected rollback".to_owned());
                if let Err(error) =
                    muxe::lifecycle::journal::write_journal(&self.cache_dir, &journal)
                {
                    return preserve(&format!(
                        "rollback decision could not be persisted: {error}"
                    ));
                }
            }
            if journal.directive() == muxe::lifecycle::TransactionDirective::RollBack {
                let reloader = muxe::lifecycle::ZellijCliReloader {
                    program: self.zellij_exe.clone(),
                };
                match muxe::lifecycle::activate::prepare_broker_rollback(
                    &self.cache_dir,
                    &muxe::lifecycle::LiveControl,
                    &reloader,
                    &mut journal,
                    &path,
                    &self.discovery_key,
                    local_status,
                )
                .await
                {
                    Ok(muxe::lifecycle::BrokerRollbackOutcome::ResumeLocal) => {}
                    Ok(muxe::lifecycle::BrokerRollbackOutcome::AwaitingPeers) => {
                        return preserve("rollback awaits exact progress from another member");
                    }
                    Ok(muxe::lifecycle::BrokerRollbackOutcome::Complete) => {
                        return muxe_broker::RecoveryDecision::CleanupComplete;
                    }
                    Err(error) => {
                        return preserve(&format!(
                            "shared rollback driver could not converge: {error}"
                        ));
                    }
                }
            } else if journal.directive() == muxe::lifecycle::TransactionDirective::Commit
                && matches!(
                    journal.transaction,
                    muxe::lifecycle::journal::TransactionPhase::Ready { .. }
                )
            {
                journal.enter_committing();
                if let Err(error) =
                    muxe::lifecycle::journal::write_journal(&self.cache_dir, &journal)
                {
                    return preserve(&format!("commit decision could not be persisted: {error}"));
                }
            }
            if let Some(member) = old_commit_index {
                journal.members_mut()[member].old =
                    muxe::lifecycle::journal::OldMemberProgress::CommitIntent;
                if let Err(error) =
                    muxe::lifecycle::journal::write_journal(&self.cache_dir, &journal)
                {
                    return preserve(&format!(
                        "old commit intent could not be persisted: {error}"
                    ));
                }
            }
            if journal.directive() == muxe::lifecycle::TransactionDirective::Commit
                && local_status.lifecycle == muxe_protocol::control::LifecycleState::Draining
            {
                let member = journal
                    .recovery_member(&self.discovery_key, *handoff)
                    .expect("validated recovery member remains in journal");
                match muxe::lifecycle::journal::has_old_retirement_receipt(
                    &muxe::lifecycle::journal::activation_dir(&self.cache_dir),
                    &journal,
                    member,
                ) {
                    Ok(false) => {}
                    Ok(true) => {
                        return preserve("old retirement proof already exists before Stop");
                    }
                    Err(error) => {
                        return preserve(&format!("old retirement proof path is unsafe: {error}"));
                    }
                }
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_secs());
            let recover_after =
                std::time::Duration::from_secs(journal.recovery_deadline.saturating_sub(now));
            let permit = Arc::new(JournalRecoveryPermit {
                path,
                discovery_key: self.discovery_key.clone(),
                server_id: local_status.live_server.server_id.clone(),
                zellij_exe: self.zellij_exe.clone(),
                lock: Mutex::new(Some(lock)),
            });
            match journal.directive() {
                muxe::lifecycle::TransactionDirective::Prepare
                | muxe::lifecycle::TransactionDirective::Activate
                | muxe::lifecycle::TransactionDirective::RollBack => {
                    muxe_broker::RecoveryDecision::RestoreOld {
                        recover_after,
                        permit: Some(permit),
                    }
                }
                muxe::lifecycle::TransactionDirective::Commit => {
                    muxe_broker::RecoveryDecision::TargetOwns {
                        recover_after,
                        permit: Some(permit),
                    }
                }
                muxe::lifecycle::TransactionDirective::CleanupCommitted
                | muxe::lifecycle::TransactionDirective::CleanupRolledBack => {
                    muxe_broker::RecoveryDecision::Preserve {
                        reason: "terminal cleanup was not handled under the unit lock".to_owned(),
                    }
                }
            }
        })
    }
    fn authorize_target_commit<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
        local_status: &'a muxe_protocol::control::ActivationStatus,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let path = muxe::lifecycle::journal::activation_dir(&self.cache_dir)
                .join(self.unit.journal_name());
            let journal = muxe::lifecycle::journal::read_journal(&path)
                .map_err(|error| format!("target Commit journal unavailable: {error}"))?;
            let member = journal
                .recovery_member(&self.discovery_key, *handoff)
                .map_err(|error| format!("target Commit member unauthorized: {error}"))?;
            self.validate_target_incarnation(&journal, member, local_status)
        })
    }
    fn authorize_old_commit<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
        local_status: &'a muxe_protocol::control::ActivationStatus,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let path = muxe::lifecycle::journal::activation_dir(&self.cache_dir)
                .join(self.unit.journal_name());
            let journal = muxe::lifecycle::journal::read_journal(&path)
                .map_err(|error| format!("old Commit journal unavailable: {error}"))?;
            let member = journal
                .recovery_member(&self.discovery_key, *handoff)
                .map_err(|error| format!("old Commit member unauthorized: {error}"))?;
            self.validate_old_incarnation(&journal, member, local_status)?;
            if member.old != muxe::lifecycle::journal::OldMemberProgress::CommitIntent {
                return Err("old Commit lacks durable member CommitIntent".to_owned());
            }
            match muxe::lifecycle::journal::has_old_retirement_receipt(
                &muxe::lifecycle::journal::activation_dir(&self.cache_dir),
                &journal,
                member,
            ) {
                Ok(false) => Ok(()),
                Ok(true) => Err("old Commit found preexisting retirement proof".to_owned()),
                Err(error) => Err(format!("old Commit proof path is unsafe: {error}")),
            }
        })
    }
    fn publish_old_retirement<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
        local_status: &'a muxe_protocol::control::ActivationStatus,
    ) -> std::pin::Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let path = muxe::lifecycle::journal::activation_dir(&self.cache_dir)
                .join(self.unit.journal_name());
            let journal = muxe::lifecycle::journal::read_journal(&path)
                .map_err(|error| format!("old retirement journal unavailable: {error}"))?;
            let member = journal
                .recovery_member(&self.discovery_key, *handoff)
                .map_err(|error| format!("old retirement member unauthorized: {error}"))?;
            self.validate_old_incarnation(&journal, member, local_status)?;
            muxe::lifecycle::journal::write_old_retirement_receipt(
                &self.cache_dir,
                &journal,
                member,
                &local_status.live_server.server_id,
            )
            .map_err(|error| format!("cannot persist old retirement proof: {error}"))
        })
    }
}

/// Lowercase hex for one nonce-sized byte string without per-byte `format!`.
fn hex_bytes(bytes: &[u8]) -> String {
    const HEXDIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEXDIGITS[(byte >> 4) as usize] as char);
        out.push(HEXDIGITS[(byte & 0xF) as usize] as char);
    }
    out
}

async fn launch_menu(open: muxe::cli::MenuOpen) -> Result<()> {
    // The thin launcher delegates to pane open with the canonical UI argv,
    // then exits. Focus and cwd are always derived, never overridden.
    let executable = env::current_exe().wrap_err("could not locate the running muxe executable")?;
    let mut argv = vec![
        executable.into_os_string(),
        OsString::from("ui"),
        OsString::from("menu"),
    ];
    for (flag, value) in [
        ("--theme", open.theme.as_deref()),
        ("--color-scheme", open.color_scheme.as_deref()),
    ] {
        if let Some(value) = value {
            argv.push(OsString::from(flag));
            argv.push(OsString::from(value));
        }
    }
    argv.push(OsString::from(&open.root));
    Box::pin(open_pane(&PaneOpen {
        placement: open.placement,
        no_focus: false,
        cwd: None,
        argv,
    }))
    .await
}

async fn launch_pane(open: PaneOpen) -> Result<()> {
    Box::pin(open_pane(&open)).await
}

async fn open_pane(open: &PaneOpen) -> Result<()> {
    let paths = muxe::paths::resolve()?;
    // A detached launcher has no visible stderr, so its audit log is required:
    // failure to open it fails the launch closed before touching the host.
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the launcher audit log")?;
    let host = match selected_host(open.placement.host) {
        Ok(host) => host,
        Err(error) => {
            notify_launcher_failure(None, "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    match host {
        HostSelector::Zellij => {
            Box::pin(zellij_open_pane(
                &logger,
                &paths.cache_dir,
                &paths.config_file(),
                open,
            ))
            .await
        }
        HostSelector::Herdr => {
            Box::pin(herdr_open_pane(
                &logger,
                &paths.cache_dir,
                &paths.config_file(),
                open,
            ))
            .await
        }
        HostSelector::Auto => unreachable!("automatic host selection is resolved"),
    }
}

async fn herdr_open_pane(
    logger: &muxe::logging::Logger,
    cache_dir: &Path,
    config_file: &Path,
    open: &PaneOpen,
) -> Result<()> {
    let runtime = match muxe_adapter_herdr::HerdrRuntime::connect(herdr_launch_config(
        cache_dir.to_path_buf(),
    )?)
    .await
    {
        Ok(runtime) => runtime,
        Err(error) => {
            let error = color_eyre::eyre::eyre!(
                "could not establish the exact configured Herdr runtime: {error}"
            );
            notify_launcher_failure(None, "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    let origin = match launcher_origin(&runtime).await {
        Ok(origin) => origin,
        Err(error) => {
            notify_launcher_failure(Some(&runtime), "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    let destination = match &open.placement.parent_pane {
        ParentPane::Current => origin.clone(),
        ParentPane::Id(pane) => match muxe_adapter_herdr::pane_by_id(&runtime, pane).await {
            Ok(destination) => destination,
            Err(error) => {
                let error = color_eyre::eyre::eyre!(
                    "the explicit Herdr parent pane is not a valid live destination: {error}"
                );
                notify_launcher_failure(Some(&runtime), "pane-open", &error.to_string()).await;
                return Err(error);
            }
        },
    };
    let argv = match open
        .argv
        .iter()
        .map(|argument| {
            argument.to_str().map(ToOwned::to_owned).ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "Herdr panes require UTF-8 argv because its JSON socket API has string arguments"
                )
            })
        })
        .collect::<Result<Vec<_>>>() {
        Ok(argv) => argv,
        Err(error) => {
            notify_launcher_failure(Some(&runtime), "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    // Canonical UI argv launches through the broker-gated UI transaction so
    // the placed pane carries a minted launch token; the adapter revalidates
    // the canonical shape before creating anything.
    if muxe_adapter_api::launch::is_ui_argv(&argv) {
        return Box::pin(herdr_open_ui_pane(
            logger,
            cache_dir,
            config_file,
            &runtime,
            origin,
            destination,
            open,
            argv,
        ))
        .await;
    }
    let launch = match command_pane_launch(open, origin, destination) {
        Ok(launch) => launch,
        Err(error) => {
            notify_launcher_failure(Some(&runtime), "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    match muxe_adapter_herdr::open_command_pane(&runtime, launch).await {
        Ok(placement) => {
            if let Ok(event) = muxe::logging::LogEvent::new(
                env!("CARGO_PKG_VERSION"),
                "herdr",
                "pane-open",
                format!("opened {}", placement.pane.as_str()),
            ) {
                let _ = logger.append(&event);
            }
            println!("opened {}", placement.pane.as_str());
            Ok(())
        }
        Err(error) => {
            let error =
                color_eyre::eyre::eyre!("could not open the requested Herdr command pane: {error}");
            notify_launcher_failure(Some(&runtime), "pane-open", &error.to_string()).await;
            Err(error)
        }
    }
}

/// Lease covering pane creation after a minted launch token: layout apply and
/// move complete in seconds; the broker expires the token afterwards.
const LAUNCH_TOKEN_LEASE_MILLIS: u32 = 60_000;

async fn commit_herdr_ui_pane(
    client: &mut muxe_broker::BrokerClient,
    runtime: &muxe_adapter_herdr::HerdrRuntime,
    launch: muxe_adapter_herdr::UiPaneLaunch,
    token: muxe_protocol::PendingLaunchToken,
) -> Result<String> {
    let prepared = muxe_adapter_herdr::prepare_ui_pane(runtime, &launch)
        .await
        .map_err(|error| {
            color_eyre::eyre::eyre!("could not prepare the requested Herdr UI pane: {error}")
        })?;
    let pane = muxe_protocol::HostPaneId::new(prepared.ui_pane.as_str());
    let registration = client
        .request(muxe_protocol::ClientRequest::RegisterPendingPane(
            muxe_protocol::RegisterPendingPane {
                token,
                pane: pane.clone(),
                temporary_tab: Some(muxe_protocol::HostTabId::new(
                    prepared.temporary_tab.as_str(),
                )),
            },
        ))
        .await;
    let registration_error = match registration {
        Ok(muxe_protocol::BrokerResponse::PendingPaneRegistered) => None,
        Ok(muxe_protocol::BrokerResponse::Error(diagnostic)) => {
            Some(format!("broker rejected pane registration: {diagnostic:?}"))
        }
        Ok(response) => Some(format!(
            "broker returned unexpected pane-registration response: {response:?}"
        )),
        Err(error) => Some(format!("could not register the placed UI pane: {error}")),
    };
    if let Some(error) = registration_error {
        let cleanup =
            muxe_adapter_herdr::close_transient_tab(runtime, &prepared.temporary_tab).await;
        return Err(match cleanup {
            Ok(()) => color_eyre::eyre::eyre!("{error}"),
            Err(cleanup_error) => color_eyre::eyre::eyre!(
                "{error}; closing the temporary tab also failed: {cleanup_error}"
            ),
        });
    }
    let placement = muxe_adapter_herdr::move_prepared_ui_pane(runtime, &launch, prepared)
        .await
        .map_err(|error| {
            color_eyre::eyre::eyre!("could not move the placed Herdr UI pane: {error}")
        })?;
    match client
        .request(muxe_protocol::ClientRequest::CommitUiLaunch(
            muxe_protocol::CommitUiLaunch {
                token,
                pane: muxe_protocol::HostPaneId::new(placement.ui_pane.as_str()),
            },
        ))
        .await
        .map_err(|error| color_eyre::eyre::eyre!("could not commit the placed UI pane: {error}"))?
    {
        muxe_protocol::BrokerResponse::Acknowledged => {}
        muxe_protocol::BrokerResponse::Error(diagnostic) => {
            return Err(color_eyre::eyre::eyre!(
                "broker rejected the placed UI pane commit: {diagnostic:?}"
            ));
        }
        response => {
            return Err(color_eyre::eyre::eyre!(
                "broker returned unexpected UI pane commit response: {response:?}"
            ));
        }
    }
    Ok(placement.ui_pane.as_str().to_owned())
}

/// Opens a canonical UI pane through the broker-gated launch transaction:
/// prepare a token with the live broker, create the pane carrying it, then
/// register and commit. Any failure after prepare aborts best-effort so no
/// minted token lingers.
#[expect(
    clippy::too_many_arguments,
    reason = "gated launch threads every participant handle through one prepare/create/register/commit chain; bundling would hide the coupling the transaction exists to pin"
)]
async fn herdr_open_ui_pane(
    logger: &muxe::logging::Logger,
    cache_dir: &Path,
    config_file: &Path,
    runtime: &muxe_adapter_herdr::HerdrRuntime,
    origin: muxe_adapter_herdr::FocusedPane,
    destination: muxe_adapter_herdr::FocusedPane,
    open: &PaneOpen,
    argv: Vec<String>,
) -> Result<()> {
    if open.no_focus {
        bail!("Herdr UI panes always take focus because a modal menu must receive keys");
    }
    if open.placement.pane_type != PaneType::Split {
        bail!("Herdr UI panes support only --pane-type split");
    }
    if open.placement.position.is_some() {
        bail!("Herdr split UI panes do not support --position");
    }
    let direction = match open.placement.direction {
        SplitDirection::Down => muxe_adapter_herdr::UiSplitDirection::Down,
        SplitDirection::Right => muxe_adapter_herdr::UiSplitDirection::Right,
        SplitDirection::Up | SplitDirection::Left => {
            bail!("Herdr UI panes support only --direction down or right")
        }
    };
    let ratio = command_pane_ratio(&open.placement, &destination, direction)?;
    let cwd = resolve_pane_cwd(&origin.cwd, open.cwd.as_deref())?;
    let root = argv
        .last()
        .cloned()
        .ok_or_else(|| color_eyre::eyre::eyre!("Herdr UI launch requires a root argument"))?;
    let mut client = Box::pin(launcher_client(cache_dir, config_file, runtime)).await?;
    let token = prepare_ui_launch(&mut client, cache_dir, &origin, &root).await?;
    let launch = muxe_adapter_herdr::UiPaneLaunch {
        origin_workspace: origin.workspace.clone(),
        origin_tab: origin.tab.clone(),
        origin_pane: origin.pane.clone(),
        cwd,
        argv,
        bootstrap_env: bootstrap_env(&origin, token),
        direction,
        ratio,
        focus: true,
    };
    match commit_herdr_ui_pane(&mut client, runtime, launch, token).await {
        Ok(pane) => {
            if let Ok(event) = muxe::logging::LogEvent::new(
                env!("CARGO_PKG_VERSION"),
                "herdr",
                "menu-open",
                format!("opened {pane}"),
            ) {
                let _ = logger.append(&event);
            }
            println!("opened {pane}");
            Ok(())
        }
        Err(error) => {
            let _ = client
                .request(muxe_protocol::ClientRequest::AbortUiLaunch(
                    muxe_protocol::AbortUiLaunch { token },
                ))
                .await;
            notify_launcher_failure(Some(runtime), "menu-open", &error.to_string()).await;
            Err(error)
        }
    }
}
async fn prepare_ui_launch(
    client: &mut muxe_broker::BrokerClient,
    cache_dir: &Path,
    origin: &muxe_adapter_herdr::FocusedPane,
    root: &str,
) -> Result<muxe_protocol::PendingLaunchToken> {
    let adapter =
        muxe_adapter_herdr::HerdrAdapter::connect(muxe_adapter_herdr::HerdrAdapterConfig {
            socket_path: required_absolute_environment_path("HERDR_SOCKET_PATH")?,
            herdr_binary: herdr_binary_from_path()?,
            cache_dir: cache_dir.to_path_buf(),
        })
        .await
        .wrap_err("could not connect the Herdr adapter for modal scope")?;
    let scope = adapter.modal_scope(&origin.pane).await.map_err(|error| {
        color_eyre::eyre::eyre!("could not resolve the menu modal scope: {error}")
    })?;
    match client
        .request(muxe_protocol::ClientRequest::PrepareUiLaunch(
            muxe_protocol::PrepareUiLaunch {
                modal_scope: muxe_protocol::ModalScopeId::new(scope.as_str()),
                root: muxe_protocol::MenuId::named(root),
                lease_millis: LAUNCH_TOKEN_LEASE_MILLIS,
            },
        ))
        .await
        .wrap_err("could not prepare the UI launch with the broker")?
    {
        muxe_protocol::BrokerResponse::LaunchPrepared { token, .. } => Ok(token),
        response => bail!("broker refused the UI launch preparation: {response:?}"),
    }
}
/// Connects to the live Herdr broker serving the launcher's own server as a
/// launcher-role client. The registry entry is matched by the runtime's exact
/// discovery key; zero or several live matches fail closed.
async fn launcher_client(
    cache_dir: &Path,
    config_file: &Path,
    runtime: &muxe_adapter_herdr::HerdrRuntime,
) -> Result<muxe_broker::BrokerClient> {
    // Coldstart first: no live broker means one is started (or the stale one
    // is activated) before the exactly-one selection below. A wrong identity
    // fails closed here, never with a second broker.
    Box::pin(ensure_herdr_broker(cache_dir, config_file, runtime)).await?;
    let registry = muxe::lifecycle::Registry::open(cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    let mut matches = registry
        .live_herdr_entries_for(&runtime.identity().discovery_key)
        .wrap_err("could not probe validated Herdr broker records")?;
    if matches.len() != 1 {
        bail!(
            "launching a UI pane requires exactly one live Herdr broker for this server; start one before launching UI panes"
        );
    }
    let entry = matches.pop().expect("exactly one live broker entry");
    let identity = runtime.identity();
    muxe_broker::BrokerClient::connect(
        entry.socket(),
        muxe_protocol::PeerRole::Launcher,
        env!("CARGO_PKG_VERSION"),
        muxe_protocol::LiveServerIdentity {
            host: muxe_protocol::HostKind::Herdr,
            discovery_key: identity.discovery_key.as_str().to_owned(),
            server_id: muxe_protocol::ServerId::new(identity.live_server_id.as_str()),
        },
    )
    .await
    .wrap_err("could not establish the Herdr launcher broker connection")
}

/// Builds the exact bootstrap environment the trampoline validates: origin
/// tuple plus the minted token as lowercase hex, nothing else.
fn bootstrap_env(
    origin: &muxe_adapter_herdr::FocusedPane,
    token: muxe_protocol::PendingLaunchToken,
) -> std::collections::BTreeMap<String, String> {
    let mut env = std::collections::BTreeMap::new();
    env.insert(
        "MUXE_HERDR_ORIGIN_WORKSPACE_ID".to_owned(),
        origin.workspace.as_str().to_owned(),
    );
    env.insert(
        "MUXE_HERDR_ORIGIN_TAB_ID".to_owned(),
        origin.tab.as_str().to_owned(),
    );
    env.insert(
        "MUXE_HERDR_ORIGIN_PANE_ID".to_owned(),
        origin.pane.as_str().to_owned(),
    );
    env.insert(
        "MUXE_HERDR_ORIGIN_PANE_CWD".to_owned(),
        origin.cwd.to_string_lossy().into_owned(),
    );
    env.insert("MUXE_PENDING_LAUNCH_TOKEN".to_owned(), hex_bytes(&token.0));
    env
}

/// Opens a pane through the pinned Zellij CLI: `zellij --session <name> run`.
/// Placement maps onto Run flags; semantics the CLI cannot express fail
/// closed instead of silently degrading.
async fn zellij_open_pane(
    logger: &muxe::logging::Logger,
    cache_dir: &Path,
    config_file: &Path,
    open: &PaneOpen,
) -> Result<()> {
    let session = required_environment("ZELLIJ_SESSION_NAME")?;
    let program = muxe_adapter_zellij::resolve_zellij_exe().map_err(|error| {
        color_eyre::eyre::eyre!("could not resolve the pinned Zellij executable: {error}")
    })?;
    let argv = zellij_run_argv(&session, open)?;
    if muxe_adapter_api::launch::is_ui_argv(&open.argv) {
        // The selected Zellij UI launcher ensures a fresh bridge subscription
        // while the invoking pane still owns focus. The UI child reuses that
        // broker; reloading only after the UI pane is focused would erase the
        // confirmed non-UI origin. Generic command panes never coldstart here.
        Box::pin(ensure_zellij_broker(
            cache_dir,
            config_file,
            &session,
            &program,
        ))
        .await?;
    }
    let output = std::process::Command::new(&program)
        .args(&argv)
        .output()
        .map_err(|error| {
            color_eyre::eyre::eyre!("could not execute the Zellij run command: {error}")
        })?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr);
        let detail = detail.trim();
        let error = if detail.is_empty() {
            color_eyre::eyre::eyre!("Zellij run exited {}", output.status)
        } else {
            color_eyre::eyre::eyre!(
                "Zellij run failed: {}",
                detail.chars().take(512).collect::<String>()
            )
        };
        return Err(error);
    }
    let pane = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if let Ok(event) = muxe::logging::LogEvent::new(
        env!("CARGO_PKG_VERSION"),
        "zellij",
        "pane-open",
        format!("opened {pane}"),
    ) {
        let _ = logger.append(&event);
    }
    println!("opened {pane}");
    Ok(())
}
/// Renders the exact pinned `zellij run` argv for a placement. Pure for unit
/// coverage: no process spawns here.
fn zellij_run_argv(session: &str, open: &PaneOpen) -> Result<Vec<OsString>> {
    if open.no_focus {
        bail!("Zellij run always focuses the new pane; --no-focus cannot be honored");
    }
    if !matches!(open.placement.parent_pane, muxe::cli::ParentPane::Current) {
        bail!(
            "Zellij run opens relative to the focused pane; explicit parent panes are unsupported"
        );
    }
    let mut argv = vec![
        OsString::from("--session"),
        OsString::from(session),
        OsString::from("run"),
    ];
    match open.placement.pane_type {
        muxe::cli::PaneType::Split => {
            if open.placement.width.is_some() || open.placement.height.is_some() {
                bail!(
                    "Zellij tiled splits take no explicit size; use overlay or popup placement for sized panes"
                );
            }
            argv.push(OsString::from("--direction"));
            argv.push(OsString::from(match open.placement.direction {
                muxe::cli::SplitDirection::Down => "down",
                muxe::cli::SplitDirection::Up => "up",
                muxe::cli::SplitDirection::Left => "left",
                muxe::cli::SplitDirection::Right => "right",
            }));
        }
        muxe::cli::PaneType::Overlay | muxe::cli::PaneType::Popup => {
            argv.push(OsString::from("--floating"));
            if let Some(position) = open.placement.position {
                argv.push(OsString::from("--x"));
                argv.push(OsString::from(position.x.to_string()));
                argv.push(OsString::from("--y"));
                argv.push(OsString::from(position.y.to_string()));
            }
            for (flag, dimension) in [
                ("--width", open.placement.width),
                ("--height", open.placement.height),
            ] {
                if let Some(dimension) = dimension {
                    argv.push(OsString::from(flag));
                    argv.push(OsString::from(match dimension {
                        muxe::cli::Dimension::Cells(cells) => cells.to_string(),
                        muxe::cli::Dimension::Percent(percent) => format!("{percent}%"),
                    }));
                }
            }
        }
    }
    match &open.cwd {
        None => {}
        Some(path) if path.is_absolute() => {
            argv.push(OsString::from("--cwd"));
            argv.push(path.clone().into_os_string());
        }
        Some(path) => {
            let base =
                env::current_dir().wrap_err("could not resolve the launcher working directory")?;
            argv.push(OsString::from("--cwd"));
            argv.push(base.join(path).into_os_string());
        }
    }
    let command = open
        .argv
        .iter()
        .map(|argument| {
            argument.to_str().map(ToOwned::to_owned).ok_or_else(|| {
                color_eyre::eyre::eyre!("Zellij run requires UTF-8 argv for its command line")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if command.is_empty() || command[0].is_empty() {
        bail!("Zellij run requires a nonempty program");
    }
    argv.push(OsString::from("--"));
    argv.extend(command.into_iter().map(OsString::from));
    Ok(argv)
}

/// Runs the UI inside a directly-Run Zellij pane. The server injects
/// `ZELLIJ_PANE_ID` into every pane process, so the caller pane is exact;
/// the adapter captures the origin from that live pane without hint tuples.
async fn run_zellij_ui(menu: UiMenuCommand) -> Result<()> {
    let pane = required_environment("ZELLIJ_PANE_ID")?;
    let session = required_environment("ZELLIJ_SESSION_NAME")?;
    let paths = muxe::paths::resolve()?;
    let zellij_exe = muxe_adapter_zellij::resolve_zellij_exe().map_err(|error| {
        color_eyre::eyre::eyre!("could not resolve the pinned Zellij executable: {error}")
    })?;
    // Coldstart reuses a receipt-owned loaded bridge (or loads an absent
    // one) before awaiting a fresh compatible round, so attach never races
    // initial readiness. A stale record activates the invoking bridge group;
    // a wrong identity fails closed without a second broker.
    let live = Box::pin(ensure_zellij_broker(
        &paths.cache_dir,
        &paths.config_file(),
        &session,
        &zellij_exe,
    ))
    .await?;
    let mut client = muxe_broker::BrokerClient::connect(
        live.entry.socket(),
        muxe_protocol::PeerRole::Ui,
        env!("CARGO_PKG_VERSION"),
        live.status.live_server,
    )
    .await
    .wrap_err("could not establish the Zellij UI broker connection")?;
    let frame = client
        .request_frame(muxe_protocol::ClientRequest::AttachUi(
            muxe_protocol::AttachUi {
                root: validated_wire_menu_root(&menu.root)?,
                pane: muxe_protocol::HostPaneId::new(&pane),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: menu.theme.clone(),
                color_scheme: menu.color_scheme.clone(),
            },
        ))
        .await
        .wrap_err("could not attach the Zellij UI session")?;
    let response = frame.deserialize()?;
    let muxe_protocol::WireMessage::Response {
        response: muxe_protocol::BrokerResponse::UiAttached { session, .. },
        ..
    } = response
    else {
        bail!("broker did not return a UI attachment snapshot: {response:?}");
    };
    let mut control = BrokerUiControl { client, session };
    let _ = muxe_ui::run_attached(frame, &mut control)
        .await
        .wrap_err("Zellij terminal UI terminated unexpectedly")?;
    Ok(())
}

/// Makes a best-effort Herdr notification after a launcher failure. Persistent
/// logging occurs at the CLI composition root, so notification failure never
/// replaces or hides the original error returned by the caller.
async fn notify_launcher_failure(
    runtime: Option<&muxe_adapter_herdr::HerdrRuntime>,
    operation: &str,
    message: &str,
) {
    if let Some(runtime) = runtime {
        best_effort_notify(runtime, &format!("{operation} failed: {message}")).await;
    }
}

/// Best-effort `notification.show` capped at Herdr's 240-character limit. The
/// caller's persistent audit event remains authoritative.
async fn best_effort_notify(runtime: &muxe_adapter_herdr::HerdrRuntime, text: &str) {
    let body: String = text.chars().take(240).collect();
    let _ = runtime
        .invoke_response(
            "notification.show",
            serde_json::json!({"title": "Muxe", "body": body}),
        )
        .await;
}

fn autodetected_host() -> Option<HostSelector> {
    if env::var_os("HERDR_SOCKET_PATH").is_some() {
        Some(HostSelector::Herdr)
    } else if env::var_os("ZELLIJ_SESSION_NAME").is_some_and(|value| !value.is_empty()) {
        Some(HostSelector::Zellij)
    } else {
        None
    }
}

fn selected_host(requested: HostSelector) -> Result<HostSelector> {
    match requested {
        HostSelector::Herdr | HostSelector::Zellij => Ok(requested),
        HostSelector::Auto => autodetected_host().ok_or_else(|| {
            color_eyre::eyre::eyre!("could not detect a supported host for muxe pane open")
        }),
    }
}

fn herdr_launch_config(cache_dir: PathBuf) -> Result<muxe_adapter_herdr::HerdrAdapterConfig> {
    Ok(muxe_adapter_herdr::HerdrAdapterConfig {
        socket_path: required_absolute_environment_path("HERDR_SOCKET_PATH")?,
        herdr_binary: herdr_binary_from_path()?,
        cache_dir,
    })
}

fn herdr_binary_from_path() -> Result<PathBuf> {
    let path = env::var_os("PATH").ok_or_else(|| {
        color_eyre::eyre::eyre!("PATH is required to resolve the installed Herdr executable")
    })?;
    for directory in env::split_paths(&path) {
        let candidate = directory.join("herdr");
        let Ok(metadata) = std::fs::metadata(&candidate) else {
            continue;
        };
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o111 == 0 {
            continue;
        }
        if metadata.is_file() {
            return candidate
                .canonicalize()
                .wrap_err("could not resolve the installed Herdr executable");
        }
    }
    bail!("could not resolve an executable `herdr` from PATH")
}

fn required_absolute_environment_path(name: &str) -> Result<PathBuf> {
    let path = env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| color_eyre::eyre::eyre!("{name} must name an absolute path"))?;
    if !path.is_absolute() {
        bail!("{name} must name an absolute path");
    }
    Ok(path)
}

async fn launcher_origin(
    runtime: &muxe_adapter_herdr::HerdrRuntime,
) -> Result<muxe_adapter_herdr::FocusedPane> {
    launcher_origin_from(runtime, &env_lookup).await
}

/// Resolves the launcher origin against one fresh snapshot. The environment lookup is
/// injected so the selection boundary is unit-testable without mutating process state.
async fn launcher_origin_from(
    runtime: &muxe_adapter_herdr::HerdrRuntime,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<muxe_adapter_herdr::FocusedPane> {
    let selected = select_launcher_origin(get)?;
    let mut origin = muxe_adapter_herdr::pane_by_identity(
        runtime,
        selected.workspace,
        selected.tab,
        selected.pane,
    )
    .await
    .wrap_err(format!(
        "the {} Herdr launcher origin tuple is not live",
        selected.source
    ))?;
    // The immutable trampoline origin keeps its captured cwd when the bootstrap
    // supplied one; an absent cwd keeps the live snapshot enrichment instead of a
    // fallback. Inherited tuples always use the live snapshot cwd.
    if let Some(cwd) = selected.cwd_override {
        origin.cwd = cwd;
    }
    Ok(origin)
}

#[derive(Debug)]
struct SelectedOrigin {
    workspace: muxe_core::WorkspaceId,
    tab: muxe_core::TabId,
    pane: muxe_core::PaneId,
    cwd_override: Option<PathBuf>,
    source: &'static str,
}
fn env_lookup(name: &str) -> Option<String> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.into_string().ok())
}

/// Selects the launcher origin identifiers without touching the host. Precedence is
/// fixed: the immutable trampoline tuple, then the detached-keybinding ACTIVE tuple,
/// then the managed-pane tuple. A missing inherited origin fails closed: DES1993
/// never permits silently recapturing whatever pane later acquired focus.
fn select_launcher_origin(get: &dyn Fn(&str) -> Option<String>) -> Result<SelectedOrigin> {
    if let Some((workspace, tab, pane, cwd)) = saved_origin_tuple(get)? {
        return Ok(SelectedOrigin {
            workspace,
            tab,
            pane,
            cwd_override: cwd,
            source: "saved",
        });
    }
    for (source, workspace_name, tab_name, pane_name) in [
        (
            "HERDR_ACTIVE_*",
            "HERDR_ACTIVE_WORKSPACE_ID",
            "HERDR_ACTIVE_TAB_ID",
            "HERDR_ACTIVE_PANE_ID",
        ),
        (
            "HERDR_*",
            "HERDR_WORKSPACE_ID",
            "HERDR_TAB_ID",
            "HERDR_PANE_ID",
        ),
    ] {
        if let Some((workspace, tab, pane)) =
            launcher_tuple(get, workspace_name, tab_name, pane_name)?
        {
            return Ok(SelectedOrigin {
                workspace,
                tab,
                pane,
                cwd_override: None,
                source,
            });
        }
    }
    bail!(
        "Herdr origin context is required: set HERDR_ACTIVE_WORKSPACE_ID, HERDR_ACTIVE_TAB_ID, and HERDR_ACTIVE_PANE_ID, or their HERDR_* managed-pane equivalents"
    )
}

/// Saved origin triple plus optional cwd from the launcher environment.
type SavedOriginTuple = (
    muxe_core::WorkspaceId,
    muxe_core::TabId,
    muxe_core::PaneId,
    Option<PathBuf>,
);

fn saved_origin_tuple(get: &dyn Fn(&str) -> Option<String>) -> Result<Option<SavedOriginTuple>> {
    const NAMES: [&str; 3] = [
        "MUXE_HERDR_ORIGIN_WORKSPACE_ID",
        "MUXE_HERDR_ORIGIN_TAB_ID",
        "MUXE_HERDR_ORIGIN_PANE_ID",
    ];
    let values = NAMES.map(get);
    let cwd = optional_absolute_environment_path(get, "MUXE_HERDR_ORIGIN_PANE_CWD")?;
    match values {
        [None, None, None] if cwd.is_none() => Ok(None),
        [Some(workspace), Some(tab), Some(pane)] => Ok(Some((
            muxe_core::WorkspaceId::new(workspace),
            muxe_core::TabId::new(tab),
            muxe_core::PaneId::new(pane),
            cwd,
        ))),
        _ => bail!("MUXE_HERDR_ORIGIN_* must supply one complete immutable origin tuple"),
    }
}

fn launcher_tuple(
    get: &dyn Fn(&str) -> Option<String>,
    workspace_name: &str,
    tab_name: &str,
    pane_name: &str,
) -> Result<Option<(muxe_core::WorkspaceId, muxe_core::TabId, muxe_core::PaneId)>> {
    let values = [workspace_name, tab_name, pane_name].map(get);
    match values {
        [None, None, None] => Ok(None),
        [Some(workspace), Some(tab), Some(pane)] => Ok(Some((
            muxe_core::WorkspaceId::new(workspace),
            muxe_core::TabId::new(tab),
            muxe_core::PaneId::new(pane),
        ))),
        _ => bail!(
            "{workspace_name}, {tab_name}, and {pane_name} must be supplied together as one Herdr launcher tuple"
        ),
    }
}

fn optional_absolute_environment_path(
    get: &dyn Fn(&str) -> Option<String>,
    name: &str,
) -> Result<Option<PathBuf>> {
    let Some(path) = get(name).map(PathBuf::from) else {
        return Ok(None);
    };
    if !path.is_absolute() {
        bail!("{name} must name an absolute path when present");
    }
    Ok(Some(path))
}

fn command_pane_launch(
    open: &PaneOpen,
    origin: muxe_adapter_herdr::FocusedPane,
    destination: muxe_adapter_herdr::FocusedPane,
) -> Result<muxe_adapter_herdr::CommandPaneLaunch> {
    if open.placement.pane_type != PaneType::Split {
        bail!("Herdr command panes support only --pane-type split");
    }
    if open.placement.position.is_some() {
        bail!("Herdr split command panes do not support --position");
    }
    let direction = match open.placement.direction {
        SplitDirection::Down => muxe_adapter_herdr::UiSplitDirection::Down,
        SplitDirection::Right => muxe_adapter_herdr::UiSplitDirection::Right,
        SplitDirection::Up | SplitDirection::Left => {
            bail!("Herdr command panes support only --direction down or right")
        }
    };
    let ratio = command_pane_ratio(&open.placement, &destination, direction)?;
    let cwd = resolve_pane_cwd(&origin.cwd, open.cwd.as_deref())?;
    let argv = open
        .argv
        .iter()
        .map(|argument| {
            argument.to_str().map(ToOwned::to_owned).ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "Herdr command panes require UTF-8 argv because its JSON socket API has string arguments"
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(muxe_adapter_herdr::CommandPaneLaunch {
        origin,
        destination,
        cwd,
        argv,
        direction,
        ratio,
        focus: !open.no_focus,
    })
}

fn command_pane_ratio(
    placement: &muxe::cli::PlacementOptions,
    destination: &muxe_adapter_herdr::FocusedPane,
    direction: muxe_adapter_herdr::UiSplitDirection,
) -> Result<f64> {
    let (requested, unsupported, available) = match direction {
        muxe_adapter_herdr::UiSplitDirection::Down => {
            (placement.height, placement.width, destination.rows)
        }
        muxe_adapter_herdr::UiSplitDirection::Right => {
            (placement.width, placement.height, destination.columns)
        }
    };
    if unsupported.is_some() {
        bail!("the requested split dimension is perpendicular to the Herdr split direction");
    }
    let requested_ratio = match requested {
        None => return Ok(0.5),
        Some(muxe::cli::Dimension::Percent(percent)) if (10..=90).contains(&percent) => {
            f64::from(percent) / 100.0
        }
        Some(muxe::cli::Dimension::Percent(_)) => {
            bail!("Herdr split percentage must be between 10% and 90%")
        }
        Some(muxe::cli::Dimension::Cells(cells)) => {
            if cells == 0 || cells >= available {
                bail!(
                    "Herdr split cell dimension must be positive and smaller than the validated destination axis"
                );
            }
            let cells = u32::from(cells);
            let available = u32::from(available);
            if cells * 10 < available || cells * 10 > available * 9 {
                bail!(
                    "Herdr split cell dimension must reserve between 10% and 90% of the validated destination axis"
                );
            }
            f64::from(cells) / f64::from(available)
        }
    };
    // Herdr records the existing target as the first child and applies `ratio` to it.
    // The CLI dimension instead names the newly opened second child.
    Ok(1.0 - requested_ratio)
}

/// Resolves `muxe pane open --cwd` exactly once at the launcher boundary. The captured live
/// origin cwd is the only base: omitted uses it; a relative override is joined to it; no ambient
/// broker directory, home, or root fallback is permitted.
fn resolve_pane_cwd(captured_origin: &Path, override_cwd: Option<&Path>) -> Result<PathBuf> {
    if !captured_origin.is_absolute() {
        bail!("captured Herdr origin working directory must be absolute");
    }
    match override_cwd {
        None => Ok(captured_origin.to_path_buf()),
        Some(path) if path.is_absolute() => Ok(path.to_path_buf()),
        Some(path) => Ok(captured_origin.join(path)),
    }
}

async fn run_ui(menu: UiMenuCommand) -> Result<()> {
    match selected_host(HostSelector::Auto)? {
        HostSelector::Herdr => Box::pin(run_herdr_ui(menu)).await,
        HostSelector::Zellij => Box::pin(run_zellij_ui(menu)).await,
        HostSelector::Auto => unreachable!("automatic host selection is resolved"),
    }
}

async fn run_herdr_ui(menu: UiMenuCommand) -> Result<()> {
    let paths = muxe::paths::resolve()?;
    let runtime =
        muxe_adapter_herdr::HerdrRuntime::connect(herdr_launch_config(paths.cache_dir.clone())?)
            .await
            .wrap_err("could not establish the exact configured Herdr runtime")?;
    let attach = ui_attach_request(&menu, &runtime).await?;
    // Coldstart first: no live broker means one is started (or the stale one
    // is activated) before attach. The verified socket replaces the direct
    // endpoint connect so UI never races broker startup.
    let socket = Box::pin(ensure_herdr_broker(
        &paths.cache_dir,
        &paths.config_file(),
        &runtime,
    ))
    .await?;
    let live_server = LiveServerIdentity {
        host: ProtocolHostKind::Herdr,
        discovery_key: runtime.identity().discovery_key.as_str().to_owned(),
        server_id: muxe_protocol::ServerId::new(runtime.identity().live_server_id.as_str()),
    };
    let mut client = BrokerClient::connect(
        &socket,
        PeerRole::Ui,
        env!("CARGO_PKG_VERSION"),
        live_server,
    )
    .await
    .wrap_err("could not establish the Herdr UI broker connection")?;
    let frame = client
        .request_frame(ClientRequest::AttachUi(attach))
        .await
        .wrap_err("could not attach the Herdr UI session")?;
    let response = frame.deserialize()?;
    let muxe_protocol::WireMessage::Response {
        response: BrokerResponse::UiAttached { session, .. },
        ..
    } = response
    else {
        bail!("broker did not return a UI attachment snapshot: {response:?}");
    };
    let mut control = BrokerUiControl { client, session };
    let _ = muxe_ui::run_attached(frame, &mut control)
        .await
        .wrap_err("Herdr terminal UI terminated unexpectedly")?;
    Ok(())
}

struct BrokerUiControl {
    client: BrokerClient,
    session: UiSessionId,
}

#[async_trait::async_trait]
impl muxe_ui::UiControl for BrokerUiControl {
    type Error = ClientError;

    async fn invoke(
        &mut self,
        generation: u64,
        binding: BindingId,
    ) -> Result<BrokerResponse, Self::Error> {
        self.client
            .request(ClientRequest::InvokeBinding(muxe_protocol::InvokeBinding {
                session: self.session.clone(),
                generation,
                binding,
            }))
            .await
    }

    async fn menu_control(&mut self, control: MenuControl) -> Result<BrokerResponse, Self::Error> {
        self.client
            .request(ClientRequest::MenuControl(UiMenuControl {
                session: self.session.clone(),
                control,
            }))
            .await
    }

    async fn detach(&mut self) -> Result<BrokerResponse, Self::Error> {
        self.client
            .request(ClientRequest::DetachUi(muxe_protocol::DetachUi {
                session: self.session.clone(),
            }))
            .await
    }

    async fn next_event(&mut self) -> Result<muxe_protocol::BrokerEvent, Self::Error> {
        self.client.next_event().await
    }
}

/// Validates a CLI-supplied root menu name at the construction boundary,
/// returning a user-facing error for names outside the validated domain.
fn validated_wire_menu_root(root: &str) -> Result<MenuId> {
    if root.is_empty() || root.contains('\0') || root.chars().any(char::is_control) {
        bail!("invalid menu name `{root}`: names must be non-empty with no NUL/control characters");
    }
    Ok(MenuId::named(root))
}

async fn ui_attach_request(
    menu: &UiMenuCommand,
    runtime: &muxe_adapter_herdr::HerdrRuntime,
) -> Result<AttachUi> {
    let origin = UiOriginBootstrap {
        workspace: WorkspaceId::new(required_environment("MUXE_HERDR_ORIGIN_WORKSPACE_ID")?),
        tab: HostTabId::new(required_environment("MUXE_HERDR_ORIGIN_TAB_ID")?),
        pane: HostPaneId::new(required_environment("MUXE_HERDR_ORIGIN_PANE_ID")?),
        cwd: optional_absolute_environment_path(&env_lookup, "MUXE_HERDR_ORIGIN_PANE_CWD")?
            .map(|path| path.to_string_lossy().into_owned()),
    };
    let token = pending_launch_token(&required_environment("MUXE_PENDING_LAUNCH_TOKEN")?)?;
    // `pane.move` can close the temporary creation tab after this process inherited its
    // environment. `HERDR_PANE_ID` remains the UI process's stable host identity, while the
    // inherited workspace/tab tuple may therefore be stale. Resolve its current tuple by that
    // pane only; never substitute saved origin data or current focus.
    let pane = HostPaneId::new(required_environment("HERDR_PANE_ID")?);
    let caller = muxe_adapter_herdr::pane_by_id(runtime, pane.as_str())
        .await
        .wrap_err("the Herdr UI caller pane is not live after its launcher move")?;
    let workspace = WorkspaceId::new(caller.workspace.as_str());
    let tab = HostTabId::new(caller.tab.as_str());
    Ok(AttachUi {
        root: validated_wire_menu_root(&menu.root)?,
        pane: pane.clone(),
        pending_launch: Some(token),
        origin: Some(origin),
        caller_identity: Some(UiCallerIdentityWire {
            workspace,
            tab,
            pane,
            cwd: caller.cwd.to_string_lossy().into_owned(),
        }),
        theme: menu.theme.clone(),
        color_scheme: menu.color_scheme.clone(),
    })
}

fn required_environment(name: &str) -> Result<String> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| color_eyre::eyre::eyre!("{name} must be present, nonempty, and UTF-8"))
}

fn pending_launch_token(value: &str) -> Result<PendingLaunchToken> {
    if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("MUXE_PENDING_LAUNCH_TOKEN must be a 32-character hexadecimal nonce");
    }
    let mut bytes = [0_u8; 16];
    for (destination, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *destination = u8::from_str_radix(
            std::str::from_utf8(pair)
                .map_err(|_| color_eyre::eyre::eyre!("invalid pending launch token"))?,
            16,
        )
        .map_err(|_| color_eyre::eyre::eyre!("invalid pending launch token"))?;
    }
    if bytes == [0; 16] {
        bail!("MUXE_PENDING_LAUNCH_TOKEN must not be zero");
    }
    Ok(PendingLaunchToken(bytes))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pane_cwd_uses_the_captured_origin_as_its_only_base() {
        let captured = Path::new("/captured/origin");
        assert_eq!(
            resolve_pane_cwd(captured, None).unwrap(),
            PathBuf::from("/captured/origin")
        );
        assert_eq!(
            resolve_pane_cwd(captured, Some(Path::new("child"))).unwrap(),
            PathBuf::from("/captured/origin/child")
        );
        assert_eq!(
            resolve_pane_cwd(captured, Some(Path::new("/explicit"))).unwrap(),
            PathBuf::from("/explicit")
        );
        assert!(resolve_pane_cwd(Path::new("relative"), None).is_err());
    }

    /// Consumer boundary: a successful rollback still fails the command, so
    /// CLI callers observe the original activation failure. Pins the exit
    /// decision, never the printed wording.
    #[test]
    fn rolled_back_activation_still_fails_the_command() {
        use muxe::lifecycle::UnitOutcome as Outcome;
        assert!(!activation_incomplete(&[]));
        assert!(!activation_incomplete(&[
            Outcome::Committed {
                unit: "herdr:a".to_owned(),
            },
            Outcome::Unchanged {
                unit: "zellij:/bridge".to_owned(),
            },
        ]));
        assert!(activation_incomplete(&[Outcome::RolledBack {
            unit: "zellij:/bridge".to_owned(),
            reason: "target diverged".to_owned(),
        }]));
        assert!(activation_incomplete(&[
            Outcome::Committed {
                unit: "herdr:a".to_owned(),
            },
            Outcome::Failed {
                unit: "zellij:/bridge".to_owned(),
                reason: "spawn refused".to_owned(),
            },
        ]));
    }
    #[tokio::test]
    async fn journal_recovery_only_reports_no_journal_for_exact_not_found() {
        use muxe::lifecycle::journal::UnitKind;
        use muxe_broker::{RecoveryDecision, RecoveryJournal};
        use muxe_protocol::control::{
            ActivationStatus, CompatibilityRecord, HandoffId, LifecycleState,
            PrepareHandoffProtocol,
        };
        use muxe_protocol::wire::{HostKind, LiveServerIdentity, ServerId};
        use std::os::unix::fs::PermissionsExt;

        let cache = tempfile::tempdir().expect("owned recovery cache");
        std::fs::set_permissions(cache.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache_dir = cache.path().join("cache");
        let unit = UnitKind::Herdr {
            host_hash: muxe::lifecycle::HerdrUnitId::derive("exact-state"),
        };
        let journal_dir = muxe::lifecycle::journal::activation_dir(&cache_dir);
        std::fs::create_dir_all(&journal_dir).unwrap();
        std::fs::set_permissions(&cache_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let journal_path = journal_dir.join(unit.journal_name());
        let recovery = JournalRecovery {
            cache_dir,
            unit,
            bridge_identity: None,
            bridge_member: None,
            discovery_key: muxe::lifecycle::ActivationMemberId::new("server".to_owned()).unwrap(),
            zellij_exe: None,
            registration_id: None,
        };
        let handoff = HandoffId([31; 16]);
        let status = ActivationStatus {
            lifecycle: LifecycleState::Running,
            phase: muxe_protocol::control::ActivationPhase::Ordinary,
            registration: None,
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "server".to_owned(),
                server_id: ServerId::new("server"),
            },
            current: CompatibilityRecord {
                muxe_version: "old".to_owned(),
                target_triple: "test".to_owned(),
                application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
                zellij: None,
                herdr: None,
            },
            target: None,
            handoff_id: None,
            prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
            bridge_unit: None,
            ready: None,
        };

        assert!(matches!(
            recovery.recovery_decision(&handoff, &status).await,
            RecoveryDecision::NoJournal
        ));

        std::fs::write(&journal_path, b"{").unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, &status).await,
            RecoveryDecision::Preserve { .. }
        ));
        std::fs::write(&journal_path, br#"{"schema_version":1,"unit":"zellij"}"#).unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, &status).await,
            RecoveryDecision::Preserve { .. }
        ));

        let symlink_target = cache.path().join("symlink-journal");
        std::fs::write(&symlink_target, b"{}").unwrap();
        std::fs::remove_file(&journal_path).unwrap();
        std::os::unix::fs::symlink(&symlink_target, &journal_path).unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, &status).await,
            RecoveryDecision::Preserve { .. }
        ));

        std::fs::remove_file(&journal_path).unwrap();
        std::os::unix::fs::symlink(cache.path().join("missing-target"), &journal_path).unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, &status).await,
            RecoveryDecision::Preserve { .. }
        ));
    }

    fn rollback_peer_statuses(
        old: muxe_protocol::control::CompatibilityRecord,
        target: muxe_protocol::control::CompatibilityRecord,
        handoff: muxe_protocol::control::HandoffId,
    ) -> (
        muxe_protocol::control::ActivationStatus,
        muxe_protocol::control::ActivationStatus,
    ) {
        use muxe_protocol::control::{
            ActivationPhase, ActivationStatus, LifecycleState, PrepareHandoffProtocol,
        };
        use muxe_protocol::wire::{HostKind, LiveServerIdentity, ServerId};
        let target_status = ActivationStatus {
            lifecycle: LifecycleState::Running,
            phase: ActivationPhase::TargetGated,
            registration: None,
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "server".to_owned(),
                server_id: ServerId::new("target-server"),
            },
            current: target.clone(),
            target: None,
            handoff_id: Some(handoff),
            prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
            bridge_unit: None,
            ready: None,
        };
        let old_status = ActivationStatus {
            lifecycle: LifecycleState::Draining,
            phase: ActivationPhase::Draining,
            registration: None,
            live_server: LiveServerIdentity {
                host: HostKind::Herdr,
                discovery_key: "server".to_owned(),
                server_id: ServerId::new("old-server"),
            },
            current: old,
            target: Some(target),
            handoff_id: Some(handoff),
            prepare_handoff: Some(PrepareHandoffProtocol::CoordinatorSuppliedV1),
            bridge_unit: None,
            ready: None,
        };
        (target_status, old_status)
    }

    #[tokio::test]
    async fn broker_disconnect_and_coordinator_share_the_durable_rollback_directive() {
        use muxe::lifecycle::journal::{
            ActivationId, ActivationJournal, OldMemberProgress, TransactionDirective,
            TransactionMember, UnitKind,
        };
        use muxe_broker::{RecoveryDecision, RecoveryJournal};
        use muxe_protocol::control::{CompatibilityRecord, HandoffId};
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::set_permissions(&cache_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let unit = UnitKind::Herdr {
            host_hash: muxe::lifecycle::journal::unit_hash("server"),
        };
        let record = CompatibilityRecord {
            muxe_version: "old".to_owned(),
            target_triple: "test".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        };
        let target = CompatibilityRecord {
            muxe_version: "target".to_owned(),
            ..record.clone()
        };
        let activation = ActivationId::from_bytes([0x61; 16]).unwrap();
        let handoff = HandoffId([0x62; 16]);
        let mut member = TransactionMember::new(
            activation,
            muxe::lifecycle::ActivationMemberId::new("server".to_owned()).unwrap(),
            muxe::lifecycle::MemberEndpoint::new(cache_dir.join("server.sock")).unwrap(),
            handoff,
            record.clone(),
        )
        .unwrap();
        member.old = OldMemberProgress::Drained;
        let journal =
            ActivationJournal::new(activation, unit.clone(), target.clone(), vec![member]).unwrap();
        let path = muxe::lifecycle::journal::write_journal(&cache_dir, &journal).unwrap();
        let recovery = JournalRecovery {
            cache_dir,
            unit,
            bridge_identity: None,
            bridge_member: None,
            discovery_key: muxe::lifecycle::ActivationMemberId::new("server".to_owned()).unwrap(),
            zellij_exe: None,
            registration_id: None,
        };
        let (target_looking, local_status) = rollback_peer_statuses(record, target, handoff);
        assert!(matches!(
            recovery.recovery_decision(&handoff, &target_looking).await,
            RecoveryDecision::Preserve { .. }
        ));
        assert_eq!(
            muxe::lifecycle::journal::read_journal(&path)
                .unwrap()
                .directive(),
            TransactionDirective::RollBack,
            "broker-local target-looking status cannot promote pre-Ready fate"
        );

        let decision = recovery.recovery_decision(&handoff, &local_status).await;
        let RecoveryDecision::RestoreOld {
            permit: Some(permit),
            ..
        } = decision
        else {
            panic!("broker-local rollback must grant only a durable resume intent");
        };
        let journal = muxe::lifecycle::journal::read_journal(&path).unwrap();
        assert_eq!(journal.directive(), TransactionDirective::RollBack);
        assert_eq!(
            journal.members()[0].old,
            muxe::lifecycle::journal::OldMemberProgress::ResumeIntent
        );
        permit
            .acknowledge(&handoff, muxe_broker::RecoveryAck::Resumed)
            .await
            .unwrap();
        assert!(
            !path.exists(),
            "resume acknowledgement drives terminal write and cleanup"
        );
    }

    async fn assert_ready_initial_authority(
        recovery: &JournalRecovery,
        old_recovery: &JournalRecovery,
        handoff: muxe_protocol::control::HandoffId,
        status: &muxe_protocol::control::ActivationStatus,
        old: &mut muxe_protocol::control::ActivationStatus,
        row: &muxe::lifecycle::BrokerEntry,
        path: &Path,
    ) {
        use muxe_broker::{RecoveryDecision, RecoveryJournal};
        use muxe_protocol::control::LifecycleState;
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::Preserve { .. }
        ));
        assert!(
            recovery
                .authorize_target_commit(&handoff, status)
                .await
                .is_err()
        );
        assert_eq!(
            muxe::lifecycle::journal::read_journal(path)
                .unwrap()
                .directive(),
            muxe::lifecycle::TransactionDirective::Commit
        );
        assert!(matches!(
            old_recovery.recovery_decision(&handoff, old).await,
            RecoveryDecision::TargetOwns { .. }
        ));
        assert!(
            old_recovery
                .authorize_old_commit(&handoff, old)
                .await
                .is_ok()
        );
        old.lifecycle = LifecycleState::SupervisorOnly;
        old.target = None;
        assert!(matches!(
            old_recovery.recovery_decision(&handoff, old).await,
            RecoveryDecision::TargetOwns { .. }
        ));
        muxe::lifecycle::Registry::open(&recovery.cache_dir)
            .unwrap()
            .register_herdr(row.clone())
            .unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, old).await,
            RecoveryDecision::Preserve { .. }
        ));
        assert!(matches!(
            old_recovery.recovery_decision(&handoff, old).await,
            RecoveryDecision::TargetOwns { .. }
        ));
    }

    async fn assert_ready_replacement_and_stop_boundaries(
        recovery: &JournalRecovery,
        old_recovery: &JournalRecovery,
        handoff: muxe_protocol::control::HandoffId,
        status: &muxe_protocol::control::ActivationStatus,
        old: &mut muxe_protocol::control::ActivationStatus,
        row: &muxe::lifecycle::BrokerEntry,
        path: &Path,
    ) {
        use muxe_broker::{RecoveryDecision, RecoveryJournal};
        use muxe_protocol::control::{HandoffId, LifecycleState};
        let cache = &recovery.cache_dir;
        let mut wrong = status.clone();
        wrong.current.muxe_version = "foreign".to_owned();
        assert!(matches!(
            recovery.recovery_decision(&handoff, &wrong).await,
            RecoveryDecision::Preserve { .. }
        ));
        let mut wrong = status.clone();
        wrong.handoff_id = Some(HandoffId([0x73; 16]));
        assert!(matches!(
            recovery.recovery_decision(&handoff, &wrong).await,
            RecoveryDecision::Preserve { .. }
        ));
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::TargetOwns { .. }
        ));
        assert!(
            recovery
                .authorize_target_commit(&handoff, status)
                .await
                .is_ok()
        );
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::TargetOwns { .. }
        ));
        let registry = muxe::lifecycle::Registry::open(cache).unwrap();
        let mut replacement = row.clone();
        replacement.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        registry.register_herdr(replacement).unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::Preserve { .. }
        ));
        assert!(
            recovery
                .authorize_target_commit(&handoff, status)
                .await
                .is_err()
        );
        let mut replacement = row.clone();
        replacement.server_pid = row.server_pid + 1;
        registry.register_herdr(replacement).unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::Preserve { .. }
        ));
        let mut replacement = row.clone();
        replacement.live_server = Some("new-server".to_owned());
        registry.register_herdr(replacement).unwrap();
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::Preserve { .. }
        ));
        let original = registry.register_herdr(row.clone()).unwrap();
        assert!(registry.unregister_herdr(&original).unwrap());
        assert!(matches!(
            recovery.recovery_decision(&handoff, status).await,
            RecoveryDecision::Preserve { .. }
        ));
        assert!(
            recovery
                .authorize_target_commit(&handoff, status)
                .await
                .is_err()
        );
        assert_eq!(
            muxe::lifecycle::journal::read_journal(path)
                .unwrap()
                .directive(),
            muxe::lifecycle::TransactionDirective::Commit
        );
        let recorded = muxe::lifecycle::journal::read_journal(path).unwrap();
        assert_eq!(
            recorded.members()[0].old,
            muxe::lifecycle::journal::OldMemberProgress::CommitIntent
        );
        muxe::lifecycle::journal::write_old_retirement_receipt(
            cache,
            &recorded,
            &recorded.members()[0],
            &old.live_server.server_id,
        )
        .unwrap();
        old.lifecycle = LifecycleState::Draining;
        old.target = Some(status.current.clone());
        assert!(
            old_recovery
                .authorize_old_commit(&handoff, old)
                .await
                .is_err(),
            "a preexisting stop receipt cannot authorize another old Stop"
        );
    }

    fn ready_recovery_for_server(
        cache: &Path,
        unit: &muxe::lifecycle::UnitKind,
        registration_id: Option<muxe_protocol::control::BrokerRegistrationId>,
    ) -> JournalRecovery {
        JournalRecovery {
            cache_dir: cache.to_path_buf(),
            unit: unit.clone(),
            bridge_identity: None,
            bridge_member: None,
            discovery_key: muxe::lifecycle::ActivationMemberId::new("server".to_owned()).unwrap(),
            zellij_exe: None,
            registration_id,
        }
    }

    #[tokio::test]
    async fn broker_ready_permit_requires_exact_target_status_and_registry_incarnation() {
        use muxe::lifecycle::journal::{
            ActivationId, ActivationJournal, OldMemberProgress, TargetMemberProgress,
            TransactionMember, UnitKind,
        };
        use muxe_protocol::control::{CompatibilityRecord, HandoffId};
        use muxe_protocol::wire::ServerId;
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache = temp.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
        let unit = UnitKind::Herdr {
            host_hash: muxe::lifecycle::journal::unit_hash("server"),
        };
        let target = CompatibilityRecord {
            muxe_version: "target".to_owned(),
            target_triple: "test".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        };
        let handoff = HandoffId([0x72; 16]);
        let old_record = CompatibilityRecord {
            muxe_version: "old".to_owned(),
            ..target.clone()
        };
        let socket = cache.join("server.sock");
        let mut member = TransactionMember::new(
            ActivationId::from_bytes([0x71; 16]).unwrap(),
            muxe::lifecycle::ActivationMemberId::new("server".to_owned()).unwrap(),
            muxe::lifecycle::MemberEndpoint::new(socket.clone()).unwrap(),
            handoff,
            old_record.clone(),
        )
        .unwrap();
        member.old = OldMemberProgress::Drained;
        member.target = TargetMemberProgress::Ready;
        let mut journal = ActivationJournal::new(
            ActivationId::from_bytes([0x71; 16]).unwrap(),
            unit.clone(),
            target.clone(),
            vec![member],
        )
        .unwrap();
        let mut row =
            muxe::lifecycle::BrokerEntry::now("herdr", "server", socket, std::process::id());
        row.live_server = Some("target-server".to_owned());
        row.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        let proof = muxe::lifecycle::journal::ReadyProof::new(
            &journal,
            None,
            vec![
                muxe::lifecycle::journal::ReadyMemberProof::new(
                    &journal.members()[0],
                    &row,
                    &ServerId::new("target-server"),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        let mut old_row = muxe::lifecycle::BrokerEntry::now(
            "herdr",
            "server",
            cache.join("server.sock"),
            std::process::id(),
        );
        old_row.live_server = Some("old-server".to_owned());
        old_row.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        journal.old_registry.push(old_row.clone());
        journal.enter_ready(Some(proof));
        let path = muxe::lifecycle::journal::write_journal(&cache, &journal).unwrap();
        let recovery = ready_recovery_for_server(&cache, &unit, row.registration_id);
        let old_recovery = ready_recovery_for_server(&cache, &unit, old_row.registration_id);
        let (status, mut old) = rollback_peer_statuses(old_record, target, handoff);
        assert_ready_initial_authority(
            &recovery,
            &old_recovery,
            handoff,
            &status,
            &mut old,
            &row,
            &path,
        )
        .await;
        assert_ready_replacement_and_stop_boundaries(
            &recovery,
            &old_recovery,
            handoff,
            &status,
            &mut old,
            &row,
            &path,
        )
        .await;
    }

    fn zellij_test_entry(
        identity: &muxe::paths::BridgeIdentity,
        socket: PathBuf,
        handoff: Option<muxe_protocol::control::HandoffId>,
        pid: u32,
    ) -> muxe::lifecycle::BrokerEntry {
        let mut entry = muxe::lifecycle::BrokerEntry::now("zellij", "session-a", socket, pid);
        entry.bridge_identity = Some(identity.clone());
        entry.bridge_member =
            Some(muxe::lifecycle::BridgeMemberId::new("session-a".to_owned()).unwrap());
        entry.handoff_id = handoff;
        entry.live_server = Some("session-a".to_owned());
        entry.registration_id =
            Some(muxe_protocol::control::BrokerRegistrationId::generate().unwrap());
        entry
    }

    fn zellij_test_journal(
        identity: &muxe::paths::BridgeIdentity,
        old: muxe::lifecycle::BrokerEntry,
        handoff: muxe_protocol::control::HandoffId,
    ) -> muxe::lifecycle::journal::ActivationJournal {
        use muxe::lifecycle::journal::{
            ActivationId, ActivationJournal, BridgeArtifactId, BridgeArtifactRole, BridgeArtifacts,
            TransactionMember, UnitKind,
        };
        use muxe_protocol::control::CompatibilityRecord;

        let record = CompatibilityRecord {
            muxe_version: "9.9.9".to_owned(),
            target_triple: "test-triple".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        };
        let activation = ActivationId::from_bytes([9; 16]).unwrap();
        let member = TransactionMember::new(
            activation,
            muxe::lifecycle::ActivationMemberId::new("session-a".to_owned()).unwrap(),
            muxe::lifecycle::MemberEndpoint::new(old.socket.clone()).unwrap(),
            handoff,
            record.clone(),
        )
        .unwrap();
        let mut journal = ActivationJournal::new(
            activation,
            UnitKind::Zellij {
                bridge_unit: identity.unit(),
            },
            record,
            vec![member],
        )
        .unwrap();
        let bridge_digest = muxe::integration::receipt::Sha256Digest::from_bytes(b"test-bridge");
        let receipt_preimage = muxe::integration::receipt::BridgeRecord {
            bridge_identity: identity.clone(),
            installed_version: "9.9.9".to_owned(),
            installed_digest: bridge_digest.clone(),
            previous_digest: None,
            bridge_compat: None,
        };
        let receipt_target = muxe::integration::receipt::BridgeRecord {
            previous_digest: Some(bridge_digest.clone()),
            ..receipt_preimage.clone()
        };
        let receipt_rollback = receipt_target.clone();
        journal
            .bind_zellij_authority(
                identity.clone(),
                muxe::lifecycle::MemberCensus::from_members(vec![
                    muxe::lifecycle::BridgeMemberId::new("session-a".to_owned()).unwrap(),
                ])
                .unwrap(),
                BridgeArtifacts {
                    old: BridgeArtifactId::new(activation, BridgeArtifactRole::Old),
                    target: BridgeArtifactId::new(activation, BridgeArtifactRole::Target),
                    old_digest: bridge_digest.clone(),
                    target_digest: bridge_digest,
                    receipt_preimage,
                    receipt_target,
                    receipt_rollback,
                },
            )
            .unwrap();
        journal.old_registry = vec![old];
        journal
    }

    #[tokio::test]
    async fn gated_target_wait_rejects_replaced_and_rollback_journals_and_expires() {
        use muxe::lifecycle::journal::{
            BridgeProgress, OldMemberProgress, TargetMemberProgress, write_journal,
        };
        use std::{os::unix::fs::PermissionsExt, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache = temp.path().join("cache");
        std::fs::create_dir(&cache).unwrap();
        std::fs::set_permissions(&cache, std::fs::Permissions::from_mode(0o700)).unwrap();
        let identity = muxe::paths::BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(muxe::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let endpoint = temp.path().join("member.sock");
        let handoff = muxe_protocol::control::HandoffId([42; 16]);
        let old = zellij_test_entry(&identity, endpoint.clone(), None, 1);
        let mut journal = zellij_test_journal(&identity, old, handoff);
        journal.enter_activating();
        journal.members_mut()[0].old = OldMemberProgress::Drained;
        journal.members_mut()[0].target = TargetMemberProgress::Gated;
        journal.bridge_mut().unwrap().progress = BridgeProgress::ArtifactsReady;
        let original_record = journal.target_record.clone();
        let activation_id = journal.activation_id;
        let member = muxe::lifecycle::ActivationMemberId::new("session-a".to_owned()).unwrap();
        let path = write_journal(&cache, &journal).unwrap();
        let expected = || TargetBridgeWait {
            journal_path: &path,
            activation_id,
            bridge_identity: &identity,
            member: &member,
            endpoint: &endpoint,
            handoff,
            target: &original_record,
        };
        let wait = |duration| {
            wait_for_target_bridge_reload(expected(), std::time::Instant::now() + duration)
        };
        assert!(
            tokio::time::timeout(Duration::from_secs(1), wait(Duration::from_millis(20)))
                .await
                .expect("journal wait must stop at its deadline")
                .is_err()
        );

        journal.target_record.target_triple = "replacement".to_owned();
        journal.bridge_mut().unwrap().progress = BridgeProgress::TargetReloaded;
        write_journal(&cache, &journal).unwrap();
        assert!(wait(Duration::from_secs(1)).await.is_err());

        journal.target_record = original_record.clone();
        journal.enter_rollback("activation abandoned".to_owned());
        write_journal(&cache, &journal).unwrap();
        assert!(wait(Duration::from_secs(1)).await.is_err());
    }

    #[test]
    fn single_target_cleanup_waits_for_journal_owner_then_removes_exact_row() {
        use std::{os::unix::fs::PermissionsExt, sync::mpsc, time::Duration};

        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::set_permissions(&cache_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let identity = muxe::paths::BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(muxe::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let registry = muxe::lifecycle::Registry::open(&cache_dir).unwrap();
        let guard =
            muxe::lifecycle::BridgeUnitGuard::acquire(&cache_dir, identity.clone()).unwrap();
        let socket = temp.path().join("member.sock");
        let old = zellij_test_entry(&identity, socket.clone(), None, 1);
        registry.register_zellij(&guard, old.clone()).unwrap();
        let handoff = muxe_protocol::control::HandoffId([41; 16]);
        let journal = zellij_test_journal(&identity, old, handoff);
        let journal_path = muxe::lifecycle::journal::write_journal(&cache_dir, &journal).unwrap();
        let capability = journal
            .target_registration_capability("session-a", &socket, handoff)
            .unwrap();
        let target = zellij_test_entry(&identity, socket, Some(handoff), 2);
        let target_registration = registry
            .register_zellij_target(&capability, target.clone())
            .unwrap();

        let (finished_tx, finished_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let cleanup_cache = cache_dir;
        let cleanup_registry = registry.clone();
        let cleanup_identity = identity;
        let cleanup_registration = target_registration;
        let cleanup = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = cleanup_zellij_registration(
                &cleanup_cache,
                &cleanup_registry,
                &cleanup_identity,
                None,
                &cleanup_registration,
            );
            finished_tx.send(result).unwrap();
        });
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("cleanup invocation started");
        assert!(
            finished_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err()
        );
        assert_eq!(registry.entries().unwrap(), vec![target]);
        muxe::lifecycle::journal::remove_journal(&journal_path).unwrap();
        drop(guard);
        finished_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        cleanup.join().unwrap();
        assert!(registry.entries().unwrap().is_empty());
    }

    #[test]
    fn committed_target_is_exact_old_authority_for_the_next_activation() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let cache_dir = temp.path().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();
        std::fs::set_permissions(&cache_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        let identity = muxe::paths::BridgeIdentity::resolve(
            &temp.path().join("integration"),
            std::ffi::OsStr::new(muxe::integration::BRIDGE_FILE_NAME),
        )
        .unwrap();
        let registry = muxe::lifecycle::Registry::open(&cache_dir).unwrap();
        let socket = temp.path().join("member.sock");
        let first_guard =
            muxe::lifecycle::BridgeUnitGuard::acquire(&cache_dir, identity.clone()).unwrap();
        let ordinary = zellij_test_entry(&identity, socket.clone(), None, 1);
        registry
            .register_zellij(&first_guard, ordinary.clone())
            .unwrap();

        let first_handoff = muxe_protocol::control::HandoffId([51; 16]);
        let first_journal = zellij_test_journal(&identity, ordinary, first_handoff);
        let first_path =
            muxe::lifecycle::journal::write_journal(&cache_dir, &first_journal).unwrap();
        let first_capability = first_journal
            .target_registration_capability("session-a", &socket, first_handoff)
            .unwrap();
        let first_target = zellij_test_entry(&identity, socket.clone(), Some(first_handoff), 2);
        let first_registration = registry
            .register_zellij_target(&first_capability, first_target.clone())
            .unwrap();
        muxe::lifecycle::journal::remove_journal(&first_path).unwrap();
        drop(first_guard);

        let second_guard =
            muxe::lifecycle::BridgeUnitGuard::acquire(&cache_dir, identity.clone()).unwrap();
        let second_handoff = muxe_protocol::control::HandoffId([52; 16]);
        let second_journal = zellij_test_journal(&identity, first_target.clone(), second_handoff);
        let second_path =
            muxe::lifecycle::journal::write_journal(&cache_dir, &second_journal).unwrap();
        let second_capability = second_journal
            .target_registration_capability("session-a", &socket, second_handoff)
            .unwrap();
        let second_target = zellij_test_entry(&identity, socket, Some(second_handoff), 3);
        let second_registration = registry
            .register_zellij_target(&second_capability, second_target.clone())
            .unwrap();
        assert!(
            !registry
                .unregister_zellij(&second_guard, &first_registration)
                .unwrap()
        );
        assert_eq!(registry.entries().unwrap(), vec![second_target]);

        let cleanup_cache = cache_dir.clone();
        let cleanup_registry = registry.clone();
        let cleanup_identity = identity.clone();
        let cleanup = std::thread::spawn(move || {
            cleanup_zellij_registration(
                &cleanup_cache,
                &cleanup_registry,
                &cleanup_identity,
                None,
                &second_registration,
            )
        });
        muxe::lifecycle::activate::restore_old_registry_rows(&cache_dir, &second_journal).unwrap();
        assert_eq!(registry.entries().unwrap(), vec![first_target]);
        muxe::lifecycle::journal::remove_journal(&second_path).unwrap();
        drop(second_guard);
        cleanup.join().unwrap().unwrap();

        cleanup_zellij_registration(&cache_dir, &registry, &identity, None, &first_registration)
            .unwrap();
        assert!(registry.entries().unwrap().is_empty());
    }
}
#[cfg(test)]
mod launcher_tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup(map: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
        move |name| map.get(name).cloned()
    }

    fn active_map() -> HashMap<String, String> {
        HashMap::from([
            ("HERDR_ACTIVE_WORKSPACE_ID".to_owned(), "w1".to_owned()),
            ("HERDR_ACTIVE_TAB_ID".to_owned(), "w1:tA".to_owned()),
            ("HERDR_ACTIVE_PANE_ID".to_owned(), "w1:pA".to_owned()),
        ])
    }

    #[test]
    fn active_tuple_wins_over_managed_and_absent_fails_closed() {
        let mut map = active_map();
        map.insert("HERDR_WORKSPACE_ID".to_owned(), "w1".to_owned());
        map.insert("HERDR_TAB_ID".to_owned(), "w1:tB".to_owned());
        map.insert("HERDR_PANE_ID".to_owned(), "w1:pB".to_owned());
        let selected = select_launcher_origin(&lookup(&map)).expect("complete ACTIVE wins");
        assert_eq!(selected.workspace.as_str(), "w1");
        assert_eq!(selected.tab.as_str(), "w1:tA");
        assert_eq!(selected.pane.as_str(), "w1:pA");
        assert_eq!(selected.source, "HERDR_ACTIVE_*");

        let managed_only: HashMap<String, String> = HashMap::from([
            ("HERDR_WORKSPACE_ID".to_owned(), "w1".to_owned()),
            ("HERDR_TAB_ID".to_owned(), "w1:tB".to_owned()),
            ("HERDR_PANE_ID".to_owned(), "w1:pB".to_owned()),
        ]);
        let selected =
            select_launcher_origin(&lookup(&managed_only)).expect("managed tuple applies");
        assert_eq!(selected.workspace.as_str(), "w1");
        assert_eq!(selected.tab.as_str(), "w1:tB");
        assert_eq!(selected.pane.as_str(), "w1:pB");
        assert_eq!(selected.source, "HERDR_*");

        let mut partial = active_map();
        partial.remove("HERDR_ACTIVE_PANE_ID");
        assert!(select_launcher_origin(&lookup(&partial)).is_err());

        let empty: HashMap<String, String> = HashMap::new();
        let error = select_launcher_origin(&lookup(&empty)).expect_err("no focus recapture");
        assert!(error.to_string().contains("HERDR_ACTIVE_WORKSPACE_ID"));
    }

    #[test]
    fn saved_tuple_keeps_optional_cwd_and_rejects_partial() {
        let map: HashMap<String, String> = HashMap::from([
            ("MUXE_HERDR_ORIGIN_WORKSPACE_ID".to_owned(), "w1".to_owned()),
            ("MUXE_HERDR_ORIGIN_TAB_ID".to_owned(), "w1:tA".to_owned()),
            ("MUXE_HERDR_ORIGIN_PANE_ID".to_owned(), "w1:pA".to_owned()),
        ]);
        let selected = select_launcher_origin(&lookup(&map)).expect("saved tuple applies");
        assert_eq!(selected.source, "saved");
        assert_eq!(selected.workspace.as_str(), "w1");
        assert_eq!(selected.tab.as_str(), "w1:tA");
        assert_eq!(selected.pane.as_str(), "w1:pA");
        assert_eq!(selected.cwd_override, None);

        let mut with_cwd = map.clone();
        with_cwd.insert("MUXE_HERDR_ORIGIN_PANE_CWD".to_owned(), "/saved".to_owned());
        let selected =
            select_launcher_origin(&lookup(&with_cwd)).expect("saved cwd is kept when present");
        assert_eq!(selected.cwd_override, Some(PathBuf::from("/saved")));

        let mut relative = map.clone();
        relative.insert(
            "MUXE_HERDR_ORIGIN_PANE_CWD".to_owned(),
            "relative".to_owned(),
        );
        assert!(select_launcher_origin(&lookup(&relative)).is_err());

        let mut partial = map;
        partial.remove("MUXE_HERDR_ORIGIN_PANE_ID");
        assert!(select_launcher_origin(&lookup(&partial)).is_err());
    }

    /// launcher boundary must resolve paneA with live-enriched cwd, never focus.
    fn snapshot_body() -> serde_json::Value {
        serde_json::json!({
            "focused_workspace_id": "w1",
            "focused_tab_id": "w1:tB",
            "focused_pane_id": "w1:pB",
            "panes": [
                {"workspace_id": "w1", "tab_id": "w1:tA", "pane_id": "w1:pA", "cwd": "/a"},
                {"workspace_id": "w1", "tab_id": "w1:tB", "pane_id": "w1:pB", "cwd": "/b"},
            ],
            "layouts": [
                {"workspace_id": "w1", "tab_id": "w1:tA",
                 "panes": [{"pane_id": "w1:pA", "rect": {"width": 80, "height": 24}}]},
                {"workspace_id": "w1", "tab_id": "w1:tB",
                 "panes": [{"pane_id": "w1:pB", "rect": {"width": 100, "height": 30}}]},
            ],
        })
    }

    async fn recorded_runtime(
        directory: &tempfile::TempDir,
        socket: &std::path::Path,
    ) -> muxe_adapter_herdr::HerdrRuntime {
        let schema = directory.path().join("schema.json");
        std::fs::write(
            &schema,
            include_str!("../../../fixtures/herdr/herdr-api.schema.json"),
        )
        .expect("write recorded runtime schema");
        let binary = directory.path().join("herdr");
        crate::generated_executable::write_executable_script(&binary, |writer| {
            std::io::Write::write_all(
                writer,
                b"#!/bin/sh\nexec cat \"$(dirname \"$0\")/schema.json\"\n",
            )
        })
        .expect("write recorded schema executable");
        muxe_adapter_herdr::HerdrRuntime::connect(muxe_adapter_herdr::HerdrAdapterConfig {
            socket_path: socket.to_path_buf(),
            herdr_binary: binary,
            cache_dir: directory.path().join("cache"),
        })
        .await
        .expect("guarded recorded runtime connects")
    }

    fn serve_snapshot(
        path: &std::path::Path,
        body: serde_json::Value,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::UnixListener::bind(path).expect("bind owned snapshot socket");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut request = Vec::new();
                    if reader.read_until(b'\n', &mut request).await.is_err() {
                        return;
                    }
                    let payload =
                        serde_json::from_slice::<serde_json::Value>(&request).unwrap_or_default();
                    let id = payload
                        .get("id")
                        .and_then(|id| id.as_str())
                        .unwrap_or_default();
                    let result =
                        if payload.get("method").and_then(|value| value.as_str()) == Some("ping") {
                            serde_json::json!({
                                "type": "pong",
                                "protocol": 20,
                                "version": "0.8.2",
                            })
                        } else {
                            serde_json::json!({"type": "session_snapshot", "snapshot": body})
                        };
                    let response = serde_json::json!({"id": id, "result": result});
                    let _ = reader.write_all(format!("{response}\n").as_bytes()).await;
                });
            }
        })
    }

    #[tokio::test]
    async fn inherited_active_origin_resolves_against_snapshot_not_focus() {
        let directory = tempfile::tempdir().expect("owned launcher boundary directory");
        let path = directory.path().join("herdr.sock");
        let server = serve_snapshot(&path, snapshot_body());
        let runtime = recorded_runtime(&directory, &path).await;

        let origin = launcher_origin_from(&runtime, &lookup(&active_map()))
            .await
            .expect("inherited ACTIVE pane resolves");
        assert_eq!(origin.workspace.as_str(), "w1");
        assert_eq!(origin.pane.as_str(), "w1:pA");
        assert_eq!(origin.tab.as_str(), "w1:tA");
        assert_eq!(origin.cwd, PathBuf::from("/a"));

        let saved: HashMap<String, String> = HashMap::from([
            ("MUXE_HERDR_ORIGIN_WORKSPACE_ID".to_owned(), "w1".to_owned()),
            ("MUXE_HERDR_ORIGIN_TAB_ID".to_owned(), "w1:tA".to_owned()),
            ("MUXE_HERDR_ORIGIN_PANE_ID".to_owned(), "w1:pA".to_owned()),
        ]);
        let origin = launcher_origin_from(&runtime, &lookup(&saved))
            .await
            .expect("saved origin without cwd keeps live enrichment");
        assert_eq!(origin.workspace.as_str(), "w1");
        assert_eq!(origin.pane.as_str(), "w1:pA");
        assert_eq!(origin.cwd, PathBuf::from("/a"));

        let empty: HashMap<String, String> = HashMap::new();
        assert!(
            launcher_origin_from(&runtime, &lookup(&empty))
                .await
                .is_err(),
            "absent origin never recaptures changed focus"
        );
        server.abort();
    }
    /// Failure before UI creation (the ACTIVE pane is absent from the snapshot) must
    /// still reach the audit log and attempt notification; only the launcher's own
    /// snapshot and notification requests may exist — never layout or move calls.
    fn serve_recording(
        path: &std::path::Path,
        snapshot: serde_json::Value,
        seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        bodies: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::UnixListener::bind(path).expect("bind owned socket");
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let snapshot = snapshot.clone();
                let seen = std::sync::Arc::clone(&seen);
                let bodies = std::sync::Arc::clone(&bodies);
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut request = Vec::new();
                    if reader.read_until(b'\n', &mut request).await.is_err() {
                        return;
                    }
                    let payload: serde_json::Value =
                        serde_json::from_slice(&request).unwrap_or_default();
                    let method = payload
                        .get("method")
                        .and_then(|method| method.as_str())
                        .unwrap_or_default()
                        .to_owned();
                    let id = payload
                        .get("id")
                        .and_then(|id| id.as_str())
                        .unwrap_or_default()
                        .to_owned();
                    seen.lock()
                        .expect("method log is writable")
                        .push(method.clone());
                    let result = if method.as_str() == "ping" {
                        serde_json::json!({
                            "type": "pong",
                            "protocol": 20,
                            "version": "0.8.2",
                        })
                    } else if method.as_str() == "session.snapshot" {
                        serde_json::json!({
                            "type": "session_snapshot",
                            "snapshot": snapshot,
                        })
                    } else {
                        bodies
                            .lock()
                            .expect("body log is writable")
                            .push(payload.clone());
                        serde_json::json!({"shown": true})
                    };
                    let response = serde_json::json!({"id": id, "result": result});
                    let _ = reader.write_all(format!("{response}\n").as_bytes()).await;
                });
            }
        })
    }

    #[tokio::test]
    async fn launcher_failure_is_notified_before_ui_creation() {
        let directory = tempfile::tempdir().expect("owned launcher failure directory");
        // The snapshot knows only paneB; the inherited ACTIVE tuple names paneA.
        let snapshot = serde_json::json!({
            "focused_workspace_id": "w1",
            "focused_tab_id": "w1:tB",
            "focused_pane_id": "w1:pB",
            "panes": [
                {"workspace_id": "w1", "tab_id": "w1:tB", "pane_id": "w1:pB", "cwd": "/b"},
            ],
            "layouts": [
                {"workspace_id": "w1", "tab_id": "w1:tB", "panes": [{"pane_id": "w1:pB", "rect": {"width": 100, "height": 30}}]},
            ],
        });
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let bodies = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let path = directory.path().join("herdr.sock");
        let server = serve_recording(
            &path,
            snapshot,
            std::sync::Arc::clone(&seen),
            std::sync::Arc::clone(&bodies),
        );
        let runtime = recorded_runtime(&directory, &path).await;
        let error = launcher_origin_from(&runtime, &lookup(&active_map()))
            .await
            .expect_err("absent ACTIVE pane fails before UI creation");
        assert!(error.to_string().contains("not live"));
        notify_launcher_failure(Some(&runtime), "pane-open", &error.to_string()).await;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if seen.lock().expect("method log is readable").len() >= 3 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("notification attempt follows the failure");
        let seen = seen.lock().expect("method log is readable").clone();
        assert_eq!(
            seen,
            vec![
                "ping".to_owned(),
                "session.snapshot".to_owned(),
                "notification.show".to_owned()
            ],
            "only the launcher's own requests exist; no UI was created"
        );
        let bodies = bodies.lock().expect("body log is readable").clone();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0]["params"]["title"], "Muxe");
        let body = bodies[0]["params"]["body"].as_str().expect("capped body");
        assert!(body.chars().count() <= 240);
        server.abort();
    }
}

#[cfg(test)]
mod consumer_tests {
    use super::*;
    use muxe::cli::{Dimension, ParentPane};

    fn placement() -> muxe::cli::PlacementOptions {
        muxe::cli::PlacementOptions {
            host: HostSelector::Zellij,
            pane_type: PaneType::Split,
            parent_pane: ParentPane::Current,
            direction: SplitDirection::Down,
            width: None,
            height: None,
            position: None,
        }
    }

    fn pane_open() -> PaneOpen {
        PaneOpen {
            placement: placement(),
            no_focus: false,
            cwd: None,
            argv: vec![OsString::from("muxe"), OsString::from("ui")],
        }
    }

    #[test]
    fn packaged_asset_lives_beside_the_installation_root() {
        assert_eq!(
            packaged_asset_path(Path::new("/opt/muxenv")),
            PathBuf::from("/opt/muxenv/lib/muxe/muxe-zellij.wasm")
        );
    }

    #[test]
    fn zellij_split_maps_to_direction_run() {
        let argv = zellij_run_argv("alpha", &pane_open()).expect("split maps");
        let text = argv
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            text,
            vec![
                "--session",
                "alpha",
                "run",
                "--direction",
                "down",
                "--",
                "muxe",
                "ui"
            ]
        );
    }

    #[test]
    fn zellij_overlay_maps_to_floating_run() {
        let mut open = pane_open();
        open.placement.pane_type = PaneType::Popup;
        open.placement.position = Some(muxe::cli::Position { x: 1, y: 2 });
        open.placement.width = Some(Dimension::Percent(50));
        let argv = zellij_run_argv("alpha", &open).expect("popup maps");
        let text = argv
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            text,
            vec![
                "--session",
                "alpha",
                "run",
                "--floating",
                "--x",
                "1",
                "--y",
                "2",
                "--width",
                "50%",
                "--",
                "muxe",
                "ui"
            ]
        );
    }

    #[test]
    fn zellij_run_rejects_inexpressible_placement() {
        let mut open = pane_open();
        open.no_focus = true;
        assert!(zellij_run_argv("alpha", &open).is_err());
        let mut open = pane_open();
        open.placement.parent_pane = ParentPane::Id("other".to_owned());
        assert!(zellij_run_argv("alpha", &open).is_err());
        let mut open = pane_open();
        open.placement.height = Some(Dimension::Cells(10));
        assert!(zellij_run_argv("alpha", &open).is_err());
    }

    #[test]
    fn herdr_split_ratio_reserves_requested_space_for_moved_pane() {
        let destination = muxe_adapter_herdr::FocusedPane {
            workspace: muxe_core::WorkspaceId::new("workspace"),
            tab: muxe_core::TabId::new("tab"),
            pane: muxe_core::PaneId::new("pane"),
            cwd: PathBuf::from("/project"),
            columns: 80,
            rows: 50,
        };
        let mut open = placement();
        open.height = Some(Dimension::Percent(30));
        let ratio = command_pane_ratio(
            &open,
            &destination,
            muxe_adapter_herdr::UiSplitDirection::Down,
        )
        .expect("30% height converts");
        assert!((ratio - 0.7).abs() < f64::EPSILON);

        open.height = None;
        open.width = Some(Dimension::Cells(20));
        let ratio = command_pane_ratio(
            &open,
            &destination,
            muxe_adapter_herdr::UiSplitDirection::Right,
        )
        .expect("20-cell width converts");
        assert!((ratio - 0.75).abs() < f64::EPSILON);

        open.width = None;
        open.height = Some(Dimension::Percent(9));
        assert!(
            command_pane_ratio(
                &open,
                &destination,
                muxe_adapter_herdr::UiSplitDirection::Down
            )
            .expect_err("Herdr cannot honor a 9% split")
            .to_string()
            .contains("between 10% and 90%")
        );

        open.height = Some(Dimension::Cells(4));
        assert!(
            command_pane_ratio(
                &open,
                &destination,
                muxe_adapter_herdr::UiSplitDirection::Down
            )
            .expect_err("Herdr cannot honor a 4-cell height in 50 rows")
            .to_string()
            .contains("between 10% and 90%")
        );
    }

    #[test]
    fn activation_spawn_rejects_a_foreign_observed_host_before_rendering_child() {
        let temp = tempfile::tempdir().unwrap();
        let socket = PathBuf::from("/run/b-h-member.sock");
        let unit = muxe::lifecycle::UnitKind::Herdr {
            host_hash: muxe::lifecycle::journal::unit_hash("/herdr.sock"),
        };
        let member = muxe::lifecycle::SpawnMember {
            unit: &unit,
            authority: muxe::lifecycle::MemberLaunchAuthority {
                member: muxe::lifecycle::ActivationMemberId::new("/herdr.sock".to_owned()).unwrap(),
                endpoint: muxe::lifecycle::MemberEndpoint::new(socket).unwrap(),
                handoff_id: muxe_protocol::HandoffId([0xab; 16]),
            },
            observed_host: ProtocolHostKind::Zellij,
            observed_bridge_identity: None,
            observed_bridge_member: None,
            observed_handoff_id: None,
            journal_path: temp.path().join("activation.json"),
        };
        let executable = temp.path().join("muxe");
        let config = temp.path().join("config.yml");
        let context = TargetSpawnContext {
            executable: &executable,
            config_file: &config,
            cache_dir: temp.path(),
        };
        let binary = PathBuf::from("/bin/herdr");
        let renderer = HerdrTargetSpawn {
            context: &context,
            binary: Some(&binary),
        };
        assert!(matches!(
            muxe::lifecycle::TargetSpawnPolicy::render(&renderer, &member),
            Err(muxe::lifecycle::ActivateError::Spawn(_))
        ));
    }
}
#[cfg(test)]
mod mixed_recovery_production_tests {
    fn named(name: &str) -> muxe_core::MenuId {
        muxe_core::MenuId::named(muxe_core::MenuName::parse(name).expect("fixture menu name"))
    }

    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use muxe_adapter_api::{
        AdapterCapabilities, AdapterError, AdapterHealthEvent, CaptureLease, CaptureReleaseReason,
        CaptureRequest, DispatchAccepted, ExecutionCorrelationId, HostAdapter, HostIdentity,
        KeyboardCapabilities, ModalScopeId, NativeDispatchRequest, OriginCaptureRequest,
        PendingPaneLease, PendingPaneRegistration, PortableDispatchRequest,
    };
    use muxe_core::{
        ActionValidation, ActionValidator, CompiledGeneration, ConfigDiagnostic, KeyCapabilities,
        OriginContext, OriginHostKind, OriginInvocationSource, PaneId, SourceId,
    };
    use tokio::sync::watch;

    struct ShutdownGate {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    struct RecoveryAdapter {
        discovery_key: String,
        shutdown_gate: Option<Arc<ShutdownGate>>,
        resume_gate: Option<Arc<ShutdownGate>>,
        diagnostic_mode: bool,
        diagnostic_emitted: AtomicBool,
        shutdown: AtomicBool,
        shutdown_wake: tokio::sync::Notify,
    }

    impl ActionValidator for RecoveryAdapter {
        fn validate_portable(
            &self,
            _action: &muxe_core::PortableAction,
            _span: &muxe_core::SourceSpan,
        ) -> Result<ActionValidation, ConfigDiagnostic> {
            Ok(ActionValidation {
                execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
            })
        }

        fn validate_native_batch(
            &self,
            candidates: &[&muxe_core::NativeActionCandidate],
        ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
            Ok(vec![
                ActionValidation {
                    execution: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
                };
                candidates.len()
            ])
        }
    }

    #[async_trait]
    impl HostAdapter for RecoveryAdapter {
        async fn identity(&self) -> Result<HostIdentity, AdapterError> {
            Ok(HostIdentity {
                kind: muxe_adapter_api::HostKind::Zellij,
                discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(
                    self.discovery_key.clone(),
                )
                .expect("validated host discovery key"),
                live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(format!(
                    "server-{}",
                    self.discovery_key
                ))
                .expect("validated live server incarnation"),
            })
        }

        fn config_override_filename(&self) -> &'static str {
            "zellij.yml"
        }

        async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
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
            _pane: &muxe_core::PaneId,
        ) -> Result<ModalScopeId, AdapterError> {
            Ok(ModalScopeId::new("recovery-scope"))
        }

        async fn register_pending_pane(
            &self,
            registration: PendingPaneRegistration,
        ) -> Result<PendingPaneLease, AdapterError> {
            Ok(PendingPaneLease {
                id: muxe_adapter_api::PendingPaneLeaseId::new(registration.ui_session.as_str()),
                ui_session: registration.ui_session,
            })
        }

        async fn close_pending_pane(
            &self,
            _registration: PendingPaneRegistration,
            _lease: PendingPaneLease,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        fn release_pending_pane(&self, _lease: PendingPaneLease) {}

        async fn end_capture(
            &self,
            _lease: CaptureLease,
            _reason: CaptureReleaseReason,
        ) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn begin_capture(
            &self,
            _request: CaptureRequest,
        ) -> Result<CaptureLease, AdapterError> {
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "recovery test does not attach UI",
            ))
        }

        async fn capture_origin(
            &self,
            _request: OriginCaptureRequest,
        ) -> Result<OriginContext, AdapterError> {
            if self.diagnostic_mode {
                return Ok(OriginContext {
                    host_kind: OriginHostKind::Zellij,
                    server_id: muxe_core::ServerId::new("diagnostic-server"),
                    client_id: None,
                    session_id: None,
                    workspace_id: None,
                    tab_id: None,
                    tab_index: None,
                    pane_id: Some(PaneId::new("diagnostic-origin-pane")),
                    pane_type: None,
                    pane_cwd: None,
                    selection_text: None,
                    invocation_source: OriginInvocationSource::RootBinding,
                    worktree_id: None,
                    worktree_path: None,
                    agent_id: None,
                    link_url: None,
                    link_handler_id: None,
                });
            }
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "recovery test does not capture UI origin",
            ))
        }

        async fn dispatch_portable(
            &self,
            request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            if self.diagnostic_mode {
                return Ok(DispatchAccepted {
                    correlation: ExecutionCorrelationId::new("diagnostic-dispatch"),
                    execution: request.execution,
                    capabilities: muxe_core::ExecutionCapabilities::ASYNCHRONOUS,
                });
            }
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "recovery test does not dispatch",
            ))
        }

        async fn dispatch_native(
            &self,
            _request: NativeDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "recovery test does not dispatch",
            ))
        }

        async fn cancel(&self, _execution: muxe_core::ExecutionId) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
            if self.diagnostic_mode && !self.diagnostic_emitted.swap(true, Ordering::Relaxed) {
                return Ok(AdapterHealthEvent::DispatchCompleted(
                    muxe_adapter_api::DispatchCompletion::Failed {
                        execution: muxe_core::ExecutionId(1),
                        error: AdapterError::new(
                            muxe_adapter_api::AdapterErrorKind::DispatchFailed,
                            "host response carries secret-sentinel",
                        ),
                    },
                ));
            }
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    muxe_adapter_api::AdapterErrorKind::Shutdown,
                    "recovery fake host is shut down",
                ));
            }
            self.shutdown_wake.notified().await;
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Shutdown,
                "recovery fake host is shut down",
            ))
        }

        async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
            Ok(())
        }

        async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
            if let Some(gate) = &self.resume_gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), AdapterError> {
            if let Some(gate) = &self.shutdown_gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            self.shutdown.store(true, Ordering::Relaxed);
            self.shutdown_wake.notify_waiters();
            Ok(())
        }
    }
    async fn invoke_detached_then_close_ui(
        broker: &Arc<muxe_broker::Broker>,
        binding: muxe_protocol::BindingId,
    ) {
        let (events, _events_rx) = tokio::sync::mpsc::channel(1);
        let muxe_broker::RequestResult::Immediate(muxe_protocol::BrokerResponse::UiAttached {
            session,
            ..
        }) = broker
            .handle(
                muxe_protocol::PeerRole::Ui,
                muxe_protocol::ClientRequest::AttachUi(muxe_protocol::AttachUi {
                    root: muxe_protocol::MenuId::named("main"),
                    pane: muxe_protocol::HostPaneId::new("muxe-ui"),
                    pending_launch: None,
                    origin: None,
                    caller_identity: None,
                    theme: None,
                    color_scheme: None,
                }),
                events.clone(),
            )
            .await
            .expect("fake host attaches UI")
        else {
            panic!("expected immediate UI attachment");
        };
        let muxe_broker::RequestResult::Immediate(response) = broker
            .handle(
                muxe_protocol::PeerRole::Ui,
                muxe_protocol::ClientRequest::InvokeBinding(muxe_protocol::InvokeBinding {
                    session: session.clone(),
                    generation: 1,
                    binding,
                }),
                events.clone(),
            )
            .await
            .expect("fake host accepts detached dispatch")
        else {
            panic!("expected immediate detached acceptance");
        };
        assert!(matches!(
            response,
            muxe_protocol::BrokerResponse::InvocationAccepted {
                disposition: muxe_protocol::InvocationDisposition::Detached,
                ..
            }
        ));
        broker
            .handle(
                muxe_protocol::PeerRole::Ui,
                muxe_protocol::ClientRequest::DetachUi(muxe_protocol::DetachUi { session }),
                events,
            )
            .await
            .expect("UI closes after accepted dispatch");
    }

    async fn assert_native_diagnostic_written(
        cache: &std::path::Path,
        broker: Arc<muxe_broker::Broker>,
        consumer: super::DiagnosticConsumer,
    ) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let health = tokio::spawn(Arc::clone(&broker).monitor(shutdown_rx));
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.join("logs/muxe.jsonl").exists() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual broker failure reaches the native diagnostic consumer");
        let _ = shutdown_tx.send(true);
        health.await.expect("broker health monitor stops");
        consumer.stop_and_join().await;

        let record =
            fs::read_to_string(cache.join("logs/muxe.jsonl")).expect("native diagnostic record");
        assert!(record.contains("detached execution failed"));
        assert!(record.contains("Failed: ActionBlocked"));
        assert!(!record.contains("secret-sentinel"));
    }

    #[tokio::test]
    async fn detached_broker_failure_after_ui_close_reaches_native_json_log() {
        let cache = tempfile::tempdir().expect("owned cache directory");
        let cache_dir = cache.path().join("muxe");
        let config_path = cache.path().join("config.yml");
        let adapter = Arc::new(RecoveryAdapter {
            discovery_key: "diagnostic-host".to_owned(),
            shutdown_gate: None,
            resume_gate: None,
            diagnostic_mode: true,
            diagnostic_emitted: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            shutdown_wake: tokio::sync::Notify::new(),
        });
        let config = muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("diagnostic.yml"),
            "version: 1\nmenus:\n  main:\n    bindings:\n      f:\n        label: focus\n        action: tab:focus index=1\n        settings:\n          execution:\n            mode: detach\n",
            KeyCapabilities::default(),
            Some(adapter.as_ref()),
        )
        .expect("diagnostic configuration compiles");
        let root = named("main");
        let binding = config
            .attachment_view(&root, &config.theme_selection)
            .ok()
            .and_then(|view| {
                view.menu
                    .menu(&root)
                    .and_then(|menu| menu.bindings.first().map(|binding| binding.id))
            })
            .expect("diagnostic binding is visible");
        let broker = muxe_broker::Broker::from_compiled(adapter, &config_path, config);
        let diagnostics = broker
            .take_diagnostics()
            .await
            .expect("native composition root owns the broker diagnostic receiver");
        let logger = Arc::new(
            muxe::logging::Logger::open(&cache_dir, "test").expect("open owner-only logger"),
        );
        let consumer = super::retain_broker_diagnostics(Arc::clone(&logger), "zellij", diagnostics);
        invoke_detached_then_close_ui(
            &broker,
            muxe_protocol::BindingId {
                generation: binding.generation().0,
                ordinal: binding.ordinal(),
            },
        )
        .await;
        assert_native_diagnostic_written(&cache_dir, broker, consumer).await;
    }
}
