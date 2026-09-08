#![forbid(unsafe_code)]

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
    HostScope, HostSelector, IntegrationSubcommand, MenuSubcommand, PaneOpen, PaneSubcommand,
    PaneType, ParentPane, PurgeCommand, SplitDirection, UiMenuCommand, UiSubcommand,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    Box::pin(dispatch(Cli::parse())).await
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
        Command::Compatibility(command) => compatibility(&command),
        Command::Purge(command) => purge(&command),
        Command::Menu(menu) => match menu.command {
            MenuSubcommand::Open(open) => Box::pin(launch_menu(open)).await,
        },
        Command::Pane(pane) => match pane.command {
            PaneSubcommand::Open(open) => Box::pin(launch_pane(open)).await,
        },
        Command::Ui(ui) => match ui.command {
            UiSubcommand::Menu(menu) => run_ui(menu).await,
        },
        Command::Integration(integration) => match integration.command {
            IntegrationSubcommand::Install(command) => install_zellij(command),
            IntegrationSubcommand::Uninstall(command) => uninstall_zellij(command),
        },
        Command::Activate(command) => activate_brokers(command).await,
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

fn compatibility(command: &CompatibilityCommand) -> Result<()> {
    let record = muxe::compatibility::embedded_record()?;
    if command.json {
        println!("{}", muxe::compatibility::render_json(&record));
    } else {
        print!("{}", muxe::compatibility::render_human(&record));
    }
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
        let registry = muxe::lifecycle::Registry::open(cache_dir)
            .wrap_err("could not open the owner-only broker registry")?;
        let bridge_path = registry
            .entries()
            .wrap_err("could not read the owner-only broker registry")?
            .into_iter()
            .filter(|entry| entry.host_kind == "zellij")
            .find(|entry| {
                entry.live_server.as_deref() == Some(session.as_str())
                    || entry.discovery_key == session
            })
            .and_then(|entry| entry.bridge_path)
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "no live Zellij broker serves session {session}; --host current requires invocation from a managed host"
                )
            })?;
        return Ok(muxe::lifecycle::DetectedHost::Zellij {
            session,
            bridge_path,
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
    let report = run_activation(command.host, current).await?;
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
    let herdr_selected =
        !matches!(scope, HostScope::Zellij) && live.iter().any(|entry| entry.host_kind == "herdr");
    let zellij_selected =
        !matches!(scope, HostScope::Herdr) && live.iter().any(|entry| entry.host_kind == "zellij");
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
    let entries = live
        .iter()
        .map(|entry| (entry.socket.clone(), entry.host_kind.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    let spawn_herdr_binary = herdr_binary.clone();
    let spawn_zellij_exe = zellij_exe.clone();
    let spawn_argv = move |member: &muxe::lifecycle::SpawnMember| {
        activate_spawn_argv(
            &executable,
            &config_file,
            &cache_dir,
            spawn_herdr_binary.as_ref(),
            spawn_zellij_exe.as_ref(),
            &entries,
            member,
        )
    };
    let preflight = muxe::lifecycle::LivePreflight {
        config_path: paths.config_file(),
        cache_dir: paths.cache_dir.clone(),
        herdr_binary: herdr_binary.clone(),
        zellij_exe: zellij_exe.clone(),
        logger: Some(&logger),
    };
    muxe::lifecycle::activate(muxe::lifecycle::ActivateInputs {
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
        spawn_argv: &spawn_argv,
        readiness_deadline: Duration::from_mins(2),
        poll_interval: Duration::from_millis(200),
        hooks: muxe::lifecycle::ActivateHooks::default(),
        logger: Some(&logger),
    })
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
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Herdr, &discovery)
        .wrap_err("could not derive the normal Herdr broker endpoint")?;
    let executable = env::current_exe().wrap_err("could not locate the running muxe executable")?;
    let inputs = muxe::lifecycle::ColdstartInputs {
        cache_dir,
        config_file,
        executable: &executable,
        endpoint,
        host: muxe::lifecycle::ColdstartHost::Herdr {
            discovery_key: discovery,
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
        muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live.entry.socket),
        muxe::lifecycle::ColdstartOutcome::StaleRecord(_) => {
            run_activation(HostScope::Herdr, None).await?;
            match muxe::lifecycle::ensure_broker(&inputs)
                .await
                .map_err(|error| {
                    color_eyre::eyre::eyre!("could not re-verify the Herdr broker: {error}")
                })? {
                muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live.entry.socket),
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
) -> Result<PathBuf> {
    let record = muxe::compatibility::embedded_record()
        .wrap_err("could not load the embedded compatibility record")?;
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Zellij, session)
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
            session: session.to_owned(),
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
        muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live.entry.socket),
        muxe::lifecycle::ColdstartOutcome::StaleRecord(_) => {
            let current = Some(detect_current_host(cache_dir)?);
            run_activation(HostScope::Current, current).await?;
            match muxe::lifecycle::ensure_broker(&inputs)
                .await
                .map_err(|error| {
                    color_eyre::eyre::eyre!("could not re-verify the Zellij broker: {error}")
                })? {
                muxe::lifecycle::ColdstartOutcome::Ready(live) => Ok(live.entry.socket),
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
/// normal-endpoint stem (`b-z-`/`b-h-`) covers a member registered between
/// the coordinator's probe and this spawn.
fn activate_spawn_argv(
    executable: &Path,
    config_file: &Path,
    cache_dir: &Path,
    herdr_binary: Option<&PathBuf>,
    zellij_exe: Option<&PathBuf>,
    entries: &std::collections::HashMap<PathBuf, String>,
    member: &muxe::lifecycle::SpawnMember,
) -> Result<(PathBuf, Vec<OsString>), muxe::lifecycle::ActivateError> {
    let handoff = parse_handoff(&member.handoff_hex)
        .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
    let program = executable.to_path_buf();
    let kind = entries
        .get(&member.endpoint)
        .cloned()
        .or_else(|| {
            member
                .endpoint
                .file_name()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| {
                    stem.strip_prefix("b-")
                        .and_then(|rest| rest.split('-').next())
                        .and_then(|kind| match kind {
                            "z" => Some("zellij".to_owned()),
                            "h" => Some("herdr".to_owned()),
                            _ => None,
                        })
                })
        })
        .ok_or_else(|| {
            muxe::lifecycle::ActivateError::Spawn(format!(
                "cannot determine the host kind for broker endpoint {}",
                member.endpoint.display()
            ))
        })?;
    if kind == "zellij" {
        let config_dir = config_file.parent().ok_or_else(|| {
            muxe::lifecycle::ActivateError::Spawn("configuration file has no parent".to_owned())
        })?;
        let bridge = muxe::integration::stable_bridge_path(config_dir);
        let journal = muxe::lifecycle::journal::activation_dir(cache_dir).join(
            muxe::lifecycle::journal::UnitKind::Zellij {
                bridge_path_hash: muxe::lifecycle::journal::unit_hash(
                    &bridge.display().to_string(),
                ),
            }
            .journal_name(),
        );
        let spawn = muxe_broker::ServeZellijSpawn {
            binary: program.clone(),
            socket: member.endpoint.clone(),
            zellij_exe: zellij_exe.cloned().ok_or_else(|| {
                muxe::lifecycle::ActivateError::Spawn(
                    "no Zellij executable is installed for a Zellij target".to_owned(),
                )
            })?,
            session: member.host_identity.clone(),
            config: config_file.to_path_buf(),
            cache_dir: cache_dir.to_path_buf(),
            handoff: Some(handoff),
            activation_journal: Some(journal),
        };
        let args = spawn
            .argv()
            .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
        return Ok((program, args));
    }
    let journal = muxe::lifecycle::journal::activation_dir(cache_dir).join(
        muxe::lifecycle::journal::UnitKind::Herdr {
            host_hash: muxe::lifecycle::journal::unit_hash(&member.host_identity),
        }
        .journal_name(),
    );
    let spawn = muxe_broker::ServeHerdrSpawn {
        binary: program.clone(),
        socket: member.endpoint.clone(),
        herdr_binary: herdr_binary.cloned().ok_or_else(|| {
            muxe::lifecycle::ActivateError::Spawn(
                "no Herdr executable is installed for a Herdr target".to_owned(),
            )
        })?,
        herdr_socket: PathBuf::from(&member.host_identity),
        config: config_file.to_path_buf(),
        cache_dir: cache_dir.to_path_buf(),
        handoff: Some(handoff),
        activation_journal: Some(journal),
    };
    let args = spawn
        .argv()
        .map_err(|error| muxe::lifecycle::ActivateError::Spawn(error.to_string()))?;
    Ok((program, args))
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
    let logger = muxe::logging::Logger::open(&command.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the broker service audit log")?;
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
    let recovery_path: Option<PathBuf> = match (&command.handoff, &command.activation_journal) {
        (None, None) => Some(herdr_journal_path(
            &command.cache_dir,
            &live_server.discovery_key,
        )),
        (Some(_), Some(journal)) => Some(journal.clone()),
        // Unreachable: bootstrap construction above already failed a half pair closed.
        _ => None,
    };
    let bootstrap = match (command.handoff, command.activation_journal) {
        (None, None) => muxe_broker::ActivationBootstrap::Running { current },
        (Some(handoff), Some(journal_path)) => {
            let handoff = parse_handoff(&handoff)?;
            let journal = muxe::lifecycle::journal::read_journal(&journal_path)
                .wrap_err("could not read the durable activation journal")?;
            if !matches!(journal.unit, muxe::lifecycle::UnitKind::Herdr { .. })
                || journal.target_record != current
                || !matches!(
                    journal.state,
                    muxe::lifecycle::JournalState::Prepared | muxe::lifecycle::JournalState::Ready
                )
            {
                bail!("activation journal does not authorize this Herdr target record");
            }
            let handoff_hex = handoff_hex(&handoff);
            let authorized = journal.members.iter().any(|member| {
                member.host_identity == live_server.discovery_key
                    && member.old_socket == command.socket
                    && member
                        .handoff_id
                        .as_deref()
                        .is_some_and(|recorded| recorded.eq_ignore_ascii_case(&handoff_hex))
            });
            if !authorized {
                bail!("activation journal does not authorize this Herdr host identity and handoff");
            }
            muxe_broker::ActivationBootstrap::Target {
                current,
                handoff,
                live_server: live_server.clone(),
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
    entry.live_server = Some(live_server.server_id.as_str().to_owned());
    // The Registration token scopes cleanup to this process's exact entry: an old
    // broker exiting after handoff must never erase the target's entry at the same
    // normal socket. Startup failure removes only the owned entry, never a peer's.
    let registration = registry
        .register(entry)
        .wrap_err("could not register the Herdr broker endpoint")?;
    let recovery = Arc::new(JournalRecovery {
        journal_path: recovery_path,
        discovery_key: live_server.discovery_key.clone(),
        zellij_exe: None,
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
            let _ = registry.unregister(&registration);
            return Err(error).wrap_err("could not start the Herdr broker endpoint");
        }
    };
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = server.run(shutdown_rx).await;
    let _ = registry.unregister(&registration);
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
    let logger = muxe::logging::Logger::open(&command.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the broker service audit log")?;
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
    // inherent input API directly, while the broker shares the same Arc as a
    // trait object after load.
    let adapter = std::sync::Arc::new(
        muxe_adapter_zellij::ZellijAdapter::connect(muxe_adapter_zellij::ZellijAdapterConfig {
            session_name: command.session.clone(),
            zellij_exe: command.zellij_exe.clone(),
        })
        .await
        .wrap_err("could not connect the pinned Zellij session for broker startup")?,
    );
    // Load and authorize before binding. The initial census round splits by
    // bootstrap kind: ordinary Running has no broker-side UI latch (adapter
    // health is broadcast-only), so its round completes before the endpoint
    // binds and any UI observes readiness from the first byte. An activation
    // target must bind first and serve TargetGated status with ready=None
    // before the coordinator swaps the bridge; its round runs concurrently
    // with serving below.
    let adapter_object: std::sync::Arc<dyn muxe_adapter_api::HostAdapter> = adapter.clone();
    let broker = muxe_broker::Broker::load(adapter_object, &command.config)
        .await
        .wrap_err("could not load the broker configuration")?;
    let live_server = broker
        .live_identity()
        .await
        .wrap_err("could not capture the pinned Zellij live identity")?;
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
    // A half pair bails in the bootstrap match below; the flag only steers the
    // initial-round placement (pre-bind for ordinary Running, concurrent with
    // serving for an activation target).
    let is_target = command.handoff.is_some();
    let recovery_path: Option<PathBuf> = match (&command.handoff, &command.activation_journal) {
        (None, None) => zellij_journal_for(&command.cache_dir, &command.session, &command.socket),
        (Some(_), Some(journal)) => Some(journal.clone()),
        // Unreachable: bootstrap construction above already failed a half pair closed.
        _ => None,
    };
    let recovery = Arc::new(JournalRecovery {
        journal_path: recovery_path,
        discovery_key: live_server.discovery_key.clone(),
        zellij_exe: Some(command.zellij_exe.clone()),
    });
    let bootstrap = match (command.handoff, command.activation_journal) {
        (None, None) => muxe_broker::ActivationBootstrap::Running { current },
        (Some(handoff), Some(journal_path)) => {
            let handoff = parse_handoff(&handoff)?;
            let journal = muxe::lifecycle::journal::read_journal(&journal_path)
                .wrap_err("could not read the durable activation journal")?;
            if !matches!(journal.unit, muxe::lifecycle::UnitKind::Zellij { .. })
                || journal.target_record != current
                || !matches!(
                    journal.state,
                    muxe::lifecycle::JournalState::Prepared | muxe::lifecycle::JournalState::Ready
                )
            {
                bail!("activation journal does not authorize this Zellij target record");
            }
            // The registration below must carry the canonical stable managed
            // bridge path (receipt authority), and the journal must name that
            // same bridge: a target serving any other path would split the
            // bridge-sharing group the coordinator commits atomically.
            let config_dir = command.config.parent().ok_or_else(|| {
                color_eyre::eyre::eyre!("broker configuration file has no parent directory")
            })?;
            let journal_bridge = muxe::integration::stable_bridge_path(config_dir);
            if let muxe::lifecycle::UnitKind::Zellij { bridge_path_hash } = &journal.unit
                && *bridge_path_hash
                    != muxe::lifecycle::journal::unit_hash(&journal_bridge.display().to_string())
            {
                bail!("activation journal does not authorize this Zellij bridge path");
            }
            let handoff_hex = handoff_hex(&handoff);
            let authorized = journal.members.iter().any(|member| {
                member.host_identity == live_server.discovery_key
                    && member.old_socket == command.socket
                    && member
                        .handoff_id
                        .as_deref()
                        .is_some_and(|recorded| recorded.eq_ignore_ascii_case(&handoff_hex))
            });
            if !authorized {
                bail!(
                    "activation journal does not authorize this Zellij host identity and handoff"
                );
            }
            muxe_broker::ActivationBootstrap::Target {
                current,
                handoff,
                live_server: live_server.clone(),
            }
        }
        _ => bail!("broker target startup requires both --handoff and --activation-journal"),
    };
    if !is_target {
        // Ordinary Running admits UI the moment the endpoint binds (adapter
        // health is broadcast-only, never a broker-side latch), so the single
        // bounded round completes here: after bind every attach already
        // observes readiness. Exhausting the budget fails startup closed.
        let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
        if let Err(error) = establish_initial_round_until(&adapter, deadline).await {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                &format!("initial census round never established: {error}"),
            );
            return Err(error).wrap_err("Zellij initial census round never established");
        }
        serve_event(
            &logger,
            "zellij",
            "broker-serve",
            "initial census round established",
        );
    }
    let registry = muxe::lifecycle::Registry::open(&command.cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    // Every Zellij registration carries the canonical stable managed bridge
    // path: without it select_units/group_zellij drop the entry and the broker
    // is never selected or grouped for activation, and --host current cannot
    // resolve the invoking session. Derived from the served config file (the
    // receipt-canonical path the coordinator enforces in preflight), never a
    // packaged path or a guessed byte hash; the target arm above already
    // proved the journal names this same bridge.
    let serve_config_dir = command.config.parent().ok_or_else(|| {
        color_eyre::eyre::eyre!("broker configuration file has no parent directory")
    })?;
    let serve_bridge = muxe::integration::stable_bridge_path(serve_config_dir);
    // A receipt naming any other bridge path refuses rather than registering
    // a divergent bridge_path that would split the atomic bridge group.
    if let Some(receipt) =
        muxe::integration::receipt::load(&muxe::integration::integration_dir(serve_config_dir))
            .wrap_err("could not read the Zellij integration receipt")?
        && receipt.bridge.canonical_path != serve_bridge
    {
        bail!(
            "integration receipt names {} but this broker serves {}; refusing a divergent bridge registration",
            receipt.bridge.canonical_path.display(),
            serve_bridge.display()
        );
    }
    let mut entry = muxe::lifecycle::BrokerEntry::now(
        "zellij",
        live_server.discovery_key.clone(),
        command.socket.clone(),
        std::process::id(),
    );
    entry.bridge_path = Some(serve_bridge);
    entry.live_server = Some(live_server.server_id.as_str().to_owned());
    // Owner-token cleanup, exactly like the Herdr path: this broker removes only
    // its exact entry, never a target sharing the normal socket.
    let registration = registry
        .register(entry)
        .wrap_err("could not register the Zellij broker endpoint")?;
    // The endpoint was claimed before adapter construction and the guard is
    // consumed here: bind reuses the held lock instead of re-acquiring, so no
    // gap admits a second child between construction and bind.
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
                "zellij",
                "broker-serve",
                &format!("serving {}", command.socket.display()),
            );
            server
        }
        Err(error) => {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                &format!("startup failed: {error}"),
            );
            let _ = registry.unregister(&registration);
            return Err(error).wrap_err("could not start the Zellij broker endpoint");
        }
    };
    if is_target {
        // The endpoint is already bound and serving TargetGated status with
        // ready=None, so the coordinator observes the target before the
        // bridge swap. The round runs concurrently with serving: retry on
        // Err with the transport intact, while the outer deadline bounds even
        // a hanging call. Expiry requests shutdown, awaits the owned service
        // (whose run epilogue unlinks the endpoint), unregisters the owned
        // entry, and propagates the round failure.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server_handle = tokio::spawn(async move { server.run(shutdown_rx).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_mins(2);
        if let Err(error) = establish_initial_round_until(&adapter, deadline).await {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                &format!("initial census round never established: {error}"),
            );
            let _ = shutdown_tx.send(true);
            let _ = server_handle.await;
            let _ = registry.unregister(&registration);
            return Err(error).wrap_err("Zellij initial census round never established");
        }
        serve_event(
            &logger,
            "zellij",
            "broker-serve",
            "initial census round established",
        );
        let joined = server_handle.await;
        let _ = registry.unregister(&registration);
        serve_event(&logger, "zellij", "broker-serve", "stopped");
        return joined
            .wrap_err("Zellij broker service task ended unexpectedly")?
            .wrap_err("Zellij broker service stopped unexpectedly");
    }
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = server.run(shutdown_rx).await;
    let _ = registry.unregister(&registration);
    serve_event(&logger, "zellij", "broker-serve", "stopped");
    result.wrap_err("Zellij broker service stopped unexpectedly")
}

/// Retries the inherent initial census round until success or the outer
/// deadline. Each attempt is bounded by the remaining budget, so even a
/// hanging call cannot outlive `deadline`; `Err` retries after a short pause
/// with the transport intact (no churn, no park). Cancellation-safe: dropping
/// a timed-out attempt leaves partial stamps for the next retry, and an
/// established adapter returns immediately.
async fn establish_initial_round_until(
    adapter: &std::sync::Arc<muxe_adapter_zellij::ZellijAdapter>,
    deadline: std::time::Instant,
) -> Result<()> {
    let mut last_error = String::from("startup budget elapsed before the first attempt");
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, adapter.establish_initial_round()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => {
                last_error = error.to_string();
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(_) => {
                last_error = String::from("initial census round stalled past the startup budget");
                break;
            }
        }
    }
    Err(color_eyre::eyre::eyre!("{last_error}"))
}

struct JournalRecoveryPermit {
    path: PathBuf,
    discovery_key: String,
    lock: Mutex<Option<muxe::lifecycle::journal::UnitLock>>,
}

impl muxe_broker::RecoveryPermit for JournalRecoveryPermit {
    fn acknowledge<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
        ack: muxe_broker::RecoveryAck,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let mut lock = self
                .lock
                .lock()
                .map_err(|_| "recovery permit lock poisoned".to_owned())?;
            let mut journal = muxe::lifecycle::journal::read_journal(&self.path)
                .map_err(|error| format!("cannot read recovery journal for ack: {error}"))?;
            let wanted = handoff_hex(handoff);
            match ack {
                muxe_broker::RecoveryAck::TargetRetired => {
                    let target = journal
                        .target_members
                        .iter_mut()
                        .find(|target| target.handoff_id.eq_ignore_ascii_case(&wanted))
                        .ok_or_else(|| {
                            "target handoff disappeared before retirement ACK".to_owned()
                        })?;
                    target.state = muxe::lifecycle::journal::TargetTransition::Retired;
                }
                muxe_broker::RecoveryAck::Resumed => {
                    let member = journal
                        .members
                        .iter_mut()
                        .find(|member| {
                            member.host_identity == self.discovery_key
                                && member
                                    .handoff_id
                                    .as_deref()
                                    .is_some_and(|id| id.eq_ignore_ascii_case(&wanted))
                        })
                        .ok_or_else(|| "old handoff disappeared before resume ACK".to_owned())?;
                    member.state = muxe::lifecycle::journal::MemberTransition::Resumed;
                }
                muxe_broker::RecoveryAck::Committed => {
                    let member = journal
                        .members
                        .iter_mut()
                        .find(|member| {
                            member.host_identity == self.discovery_key
                                && member
                                    .handoff_id
                                    .as_deref()
                                    .is_some_and(|id| id.eq_ignore_ascii_case(&wanted))
                        })
                        .ok_or_else(|| "member disappeared before commit ACK".to_owned())?;
                    member.state = muxe::lifecycle::journal::MemberTransition::Committed;
                }
            }
            let complete = matches!(
                journal.recovery,
                muxe::lifecycle::journal::RecoveryPhase::RestoringOld
            ) && journal.bridge_restored
                && journal.members.iter().all(|member| {
                    matches!(
                        member.state,
                        muxe::lifecycle::journal::MemberTransition::Resumed
                    )
                })
                && journal.target_members.iter().all(|target| {
                    matches!(
                        target.state,
                        muxe::lifecycle::journal::TargetTransition::Retired
                    )
                });
            let cache_dir = self
                .path
                .parent()
                .and_then(Path::parent)
                .ok_or_else(|| "recovery journal cache directory is missing".to_owned())?;
            muxe::lifecycle::journal::write_journal(cache_dir, &journal)
                .map_err(|error| format!("cannot persist recovery ack: {error}"))?;
            if complete {
                muxe::lifecycle::journal::remove_journal(&self.path).map_err(|error| {
                    format!("cannot remove completed recovery journal: {error}")
                })?;
            }
            lock.take();
            Ok(())
        })
    }
}

/// Owner-side journal mapping for broker disconnect recovery.
struct JournalRecovery {
    journal_path: Option<PathBuf>,
    discovery_key: String,
    zellij_exe: Option<PathBuf>,
}

impl muxe_broker::RecoveryJournal for JournalRecovery {
    #[expect(
        clippy::too_many_lines,
        reason = "journal recovery keeps lock acquisition, member census, artifact restoration, and permit publication in one auditable decision"
    )]
    fn recovery_decision<'a>(
        &'a self,
        handoff: &'a muxe_protocol::control::HandoffId,
    ) -> std::pin::Pin<Box<dyn Future<Output = muxe_broker::RecoveryDecision> + Send + 'a>> {
        Box::pin(async move {
            let Some(path) = &self.journal_path else {
                return muxe_broker::RecoveryDecision::NoJournal;
            };
            if !path.exists() {
                return muxe_broker::RecoveryDecision::NoJournal;
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
            let Ok(mut journal) = muxe::lifecycle::journal::read_journal(path) else {
                return preserve("activation journal is corrupt or inconsistent");
            };
            journal.refresh_target_members();
            let Some(member) = journal
                .members
                .iter()
                .find(|member| member.host_identity == self.discovery_key)
            else {
                return preserve("activation journal does not name this member");
            };
            let wanted = handoff_hex(handoff);
            if member
                .handoff_id
                .as_deref()
                .is_none_or(|recorded| !recorded.eq_ignore_ascii_case(&wanted))
            {
                return preserve("activation handoff does not match the recorded member");
            }
            let member_committed = matches!(
                member.state,
                muxe::lifecycle::journal::MemberTransition::Committed
            );
            let target_live =
                matches!(journal.state, muxe::lifecycle::journal::JournalState::Ready)
                    && unit_ready_targets_live(&journal).await;
            if !target_live && !member_committed {
                let targets = journal
                    .members
                    .iter()
                    .filter_map(|member| {
                        matches!(
                            member.state,
                            muxe::lifecycle::journal::MemberTransition::Ready
                        )
                        .then(|| member.target_socket.clone().zip(member.handoff_id.clone()))
                        .flatten()
                    })
                    .collect::<Vec<_>>();
                for (target_socket, target_handoff_hex) in targets {
                    let Ok(target_handoff) =
                        muxe::lifecycle::control::handoff_from_hex(&target_handoff_hex)
                    else {
                        return preserve("target handoff is malformed");
                    };
                    let Ok(mut control) =
                        muxe::lifecycle::control::ControlClient::connect(&target_socket).await
                    else {
                        if target_socket.exists() {
                            return preserve("recorded live target rejected control connection");
                        }
                        if let Some(target) = journal.target_members.iter_mut().find(|target| {
                            target.handoff_id.eq_ignore_ascii_case(&target_handoff_hex)
                        }) {
                            target.state = muxe::lifecycle::journal::TargetTransition::Retired;
                        }
                        let cache_dir = path
                            .parent()
                            .and_then(Path::parent)
                            .unwrap_or_else(|| Path::new("."));
                        if let Err(error) =
                            muxe::lifecycle::journal::write_journal(cache_dir, &journal)
                        {
                            return preserve(&format!(
                                "absent target acknowledgement could not be persisted: {error}"
                            ));
                        }
                        continue;
                    };
                    if let Err(error) = control.abort(target_handoff).await {
                        return preserve(&format!("target retirement failed: {error}"));
                    }
                    if let Some(target) = journal
                        .target_members
                        .iter_mut()
                        .find(|target| target.handoff_id.eq_ignore_ascii_case(&target_handoff_hex))
                    {
                        target.state = muxe::lifecycle::journal::TargetTransition::Retired;
                    }
                    let cache_dir = path
                        .parent()
                        .and_then(Path::parent)
                        .unwrap_or_else(|| Path::new("."));
                    if let Err(error) = muxe::lifecycle::journal::write_journal(cache_dir, &journal)
                    {
                        return preserve(&format!(
                            "target retirement acknowledgement could not be persisted: {error}"
                        ));
                    }
                }
            }
            if !target_live
                && !member_committed
                && matches!(
                    journal.unit,
                    muxe::lifecycle::journal::UnitKind::Zellij { .. }
                )
                && journal.backup_path.is_some()
            {
                journal.recovery = muxe::lifecycle::journal::RecoveryPhase::RestoringOld;
                let restore = if let Some(program) = &self.zellij_exe {
                    let reloader = muxe::lifecycle::ZellijCliReloader {
                        program: Some(program.clone()),
                    };
                    muxe::lifecycle::activate::restore_recorded_bridge_and_reload(
                        &mut journal,
                        &reloader,
                    )
                } else {
                    muxe::lifecycle::activate::restore_recorded_bridge_artifact(&mut journal)
                };
                if let Err(error) = restore {
                    return preserve(&format!("bridge restore/reload failed: {error}"));
                }
                if let Err(error) =
                    muxe::lifecycle::activate::restore_recorded_rollback_receipt(&journal)
                {
                    return preserve(&format!("receipt restore failed: {error}"));
                }
                let cache_dir = path
                    .parent()
                    .and_then(Path::parent)
                    .unwrap_or_else(|| Path::new("."));
                if let Err(error) = muxe::lifecycle::journal::write_journal(cache_dir, &journal) {
                    return preserve(&format!(
                        "recovery artifact decision could not be persisted: {error}"
                    ));
                }
            }
            let permit = Arc::new(JournalRecoveryPermit {
                path: path.clone(),
                discovery_key: self.discovery_key.clone(),
                lock: Mutex::new(Some(lock)),
            });
            if member_committed {
                journal.recovery = muxe::lifecycle::journal::RecoveryPhase::Committed;
                let cache_dir = path
                    .parent()
                    .and_then(Path::parent)
                    .unwrap_or_else(|| Path::new("."));
                if let Err(error) = muxe::lifecycle::journal::write_journal(cache_dir, &journal) {
                    return preserve(&format!("commit decision could not be persisted: {error}"));
                }
                return muxe_broker::RecoveryDecision::Committed {
                    permit: Some(permit),
                };
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |elapsed| elapsed.as_secs());
            let recover_after =
                std::time::Duration::from_secs(journal.recovery_deadline.saturating_sub(now));
            let target_live =
                matches!(journal.state, muxe::lifecycle::journal::JournalState::Ready)
                    && unit_ready_targets_live(&journal).await;
            if target_live {
                journal.recovery = muxe::lifecycle::journal::RecoveryPhase::TargetOwns;
                let cache_dir = path
                    .parent()
                    .and_then(Path::parent)
                    .unwrap_or_else(|| Path::new("."));
                if let Err(error) = muxe::lifecycle::journal::write_journal(cache_dir, &journal) {
                    return preserve(&format!("target decision could not be persisted: {error}"));
                }
                muxe_broker::RecoveryDecision::TargetOwns {
                    recover_after,
                    permit: Some(permit),
                }
            } else {
                muxe_broker::RecoveryDecision::RestoreOld {
                    recover_after,
                    permit: Some(permit),
                }
            }
        })
    }
}

/// Probes every Ready member through its recorded target control socket. A
/// durable Ready unit is complete only when every member answers with the
/// recorded handoff, target record, live identity, and Running lifecycle.
async fn unit_ready_targets_live(journal: &muxe::lifecycle::journal::ActivationJournal) -> bool {
    let mut covered_any = false;
    for member in &journal.members {
        if !matches!(
            member.state,
            muxe::lifecycle::journal::MemberTransition::Ready
        ) {
            continue;
        }
        let (Some(socket), Some(handoff)) =
            (member.target_socket.as_ref(), member.handoff_id.as_deref())
        else {
            return false;
        };
        let Ok(expected_handoff) = muxe::lifecycle::control::handoff_from_hex(handoff) else {
            return false;
        };
        let Ok(mut control) = muxe::lifecycle::control::ControlClient::connect(socket).await else {
            return false;
        };
        let Ok(status) = control.status().await else {
            return false;
        };
        if status.lifecycle != muxe_protocol::control::LifecycleState::Running
            || status.current != journal.target_record
            || status.handoff_id != Some(expected_handoff)
            || status.live_server.discovery_key != member.host_identity
        {
            return false;
        }
        covered_any = true;
    }
    covered_any
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

fn handoff_hex(handoff: &muxe_protocol::control::HandoffId) -> String {
    hex_bytes(&handoff.0)
}

/// Deterministic old-unit journal location for one Herdr discovery key.
fn herdr_journal_path(cache_dir: &Path, discovery_key: &str) -> PathBuf {
    cache_dir.join("activation").join(
        muxe::lifecycle::UnitKind::Herdr {
            host_hash: muxe::lifecycle::journal::unit_hash(discovery_key),
        }
        .journal_name(),
    )
}

/// Finds the Zellij group journal recording this session and old endpoint, if any.
/// Old brokers resolve it by scan because the group bridge path lives in the journal.
fn zellij_journal_for(cache_dir: &Path, session: &str, socket: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(cache_dir.join("activation")).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("zellij-"))
        {
            continue;
        }
        let Ok(journal) = muxe::lifecycle::journal::read_journal(&path) else {
            continue;
        };
        if journal
            .members
            .iter()
            .any(|member| member.host_identity == session && member.old_socket == socket)
        {
            return Some(path);
        }
    }
    None
}

fn parse_handoff(value: &str) -> Result<muxe_protocol::control::HandoffId> {
    let mut bytes = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = u8::from_str_radix(
            std::str::from_utf8(pair).expect("CLI parser accepts ASCII hexadecimal"),
            16,
        )
        .map_err(|_| color_eyre::eyre::eyre!("handoff must be hexadecimal"))?;
    }
    if bytes == [0; 16] {
        bail!("handoff must be nonzero");
    }
    Ok(muxe_protocol::control::HandoffId(bytes))
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
    open_pane(&PaneOpen {
        placement: open.placement,
        no_focus: false,
        cwd: None,
        argv,
    })
    .await
}

async fn launch_pane(open: PaneOpen) -> Result<()> {
    open_pane(&open).await
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
            report_launcher_failure(&logger, None, "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    match host {
        HostSelector::Zellij => zellij_open_pane(&logger, open),
        HostSelector::Herdr => {
            herdr_open_pane(&logger, &paths.cache_dir, &paths.config_file(), open).await
        }
        HostSelector::Auto => unreachable!("automatic host selection is resolved"),
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "launcher transaction: runtime connect, origin precedence, pane open, and failure reporting form one ordered DES1993 precedence chain; splitting would scatter the saved-over-managed-over-absent ordering"
)]
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
            report_launcher_failure(logger, None, "pane-open", &error.to_string()).await;
            return Err(error);
        }
    };
    let origin = match launcher_origin(&runtime).await {
        Ok(origin) => origin,
        Err(error) => {
            report_launcher_failure(
                logger,
                Some(runtime.client()),
                "pane-open",
                &error.to_string(),
            )
            .await;
            return Err(error);
        }
    };
    let destination = match &open.placement.parent_pane {
        ParentPane::Current => origin.clone(),
        ParentPane::Id(pane) => {
            match muxe_adapter_herdr::pane_by_id(runtime.client(), runtime.schema(), pane).await {
                Ok(destination) => destination,
                Err(error) => {
                    let error = color_eyre::eyre::eyre!(
                        "the explicit Herdr parent pane is not a valid live destination: {error}"
                    );
                    report_launcher_failure(
                        logger,
                        Some(runtime.client()),
                        "pane-open",
                        &error.to_string(),
                    )
                    .await;
                    return Err(error);
                }
            }
        }
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
            report_launcher_failure(
                logger,
                Some(runtime.client()),
                "pane-open",
                &error.to_string(),
            )
            .await;
            return Err(error);
        }
    };
    // Canonical UI argv launches through the broker-gated UI transaction so
    // the placed pane carries a minted launch token; the adapter revalidates
    // the canonical shape before creating anything.
    if muxe_adapter_zellij::is_ui_argv(&argv) {
        return herdr_open_ui_pane(
            logger,
            cache_dir,
            config_file,
            &runtime,
            origin,
            destination,
            open,
            argv,
        )
        .await;
    }
    let launch = match command_pane_launch(open, origin, destination) {
        Ok(launch) => launch,
        Err(error) => {
            report_launcher_failure(
                logger,
                Some(runtime.client()),
                "pane-open",
                &error.to_string(),
            )
            .await;
            return Err(error);
        }
    };
    match muxe_adapter_herdr::open_command_pane(runtime.client(), runtime.schema(), launch).await {
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
            report_launcher_failure(
                logger,
                Some(runtime.client()),
                "pane-open",
                &error.to_string(),
            )
            .await;
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
    let prepared = muxe_adapter_herdr::prepare_ui_pane(runtime.client(), runtime.schema(), &launch)
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
        let cleanup = muxe_adapter_herdr::close_transient_tab(
            runtime.client(),
            runtime.schema(),
            &prepared.temporary_tab,
        )
        .await;
        return Err(match cleanup {
            Ok(()) => color_eyre::eyre::eyre!("{error}"),
            Err(cleanup_error) => color_eyre::eyre::eyre!(
                "{error}; closing the temporary tab also failed: {cleanup_error}"
            ),
        });
    }
    let placement = muxe_adapter_herdr::move_prepared_ui_pane(
        runtime.client(),
        runtime.schema(),
        &launch,
        prepared,
    )
    .await
    .map_err(|error| color_eyre::eyre::eyre!("could not move the placed Herdr UI pane: {error}"))?;
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
    let mut client = launcher_client(cache_dir, config_file, runtime).await?;
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
            report_launcher_failure(
                logger,
                Some(runtime.client()),
                "menu-open",
                &error.to_string(),
            )
            .await;
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
                root: muxe_protocol::MenuId::new(root),
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
    ensure_herdr_broker(cache_dir, config_file, runtime).await?;
    let discovery = runtime.identity().discovery_key.clone();
    let registry = muxe::lifecycle::Registry::open(cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    let mut matches = registry
        .probe()
        .wrap_err("could not probe the owner-only broker registry")?
        .live
        .into_iter()
        .filter(|entry| entry.host_kind == "herdr" && entry.discovery_key == discovery)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        bail!(
            "launching a UI pane requires exactly one live Herdr broker for this server; start one before launching UI panes"
        );
    }
    let entry = matches.pop().expect("exactly one live broker entry");
    let identity = runtime.identity();
    muxe_broker::BrokerClient::connect(
        &entry.socket,
        muxe_protocol::PeerRole::Launcher,
        env!("CARGO_PKG_VERSION"),
        muxe_protocol::LiveServerIdentity {
            host: muxe_protocol::HostKind::Herdr,
            discovery_key: identity.discovery_key.clone(),
            server_id: muxe_protocol::ServerId::new(identity.live_server_id.clone()),
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
fn zellij_open_pane(logger: &muxe::logging::Logger, open: &PaneOpen) -> Result<()> {
    let session = required_environment("ZELLIJ_SESSION_NAME")?;
    let program = muxe_adapter_zellij::resolve_zellij_exe().map_err(|error| {
        color_eyre::eyre::eyre!("could not resolve the pinned Zellij executable: {error}")
    })?;
    let argv = zellij_run_argv(&session, open)?;
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
        if let Ok(event) = muxe::logging::LogEvent::new(
            env!("CARGO_PKG_VERSION"),
            "zellij",
            "pane-open",
            error.to_string().chars().take(512).collect::<String>(),
        ) {
            let _ = logger.append(&event);
        }
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
    // Coldstart first: a brokerless session starts one ordinary broker,
    // reloads the stable bridge, and awaits the fresh compatible round here,
    // so attach below never races initial readiness. A stale record
    // activates the invoking bridge group; a wrong identity fails closed
    // without a second broker.
    let socket = ensure_zellij_broker(
        &paths.cache_dir,
        &paths.config_file(),
        &session,
        &zellij_exe,
    )
    .await?;
    let mut client = muxe_broker::BrokerClient::connect(
        &socket,
        muxe_protocol::PeerRole::Ui,
        env!("CARGO_PKG_VERSION"),
        muxe_protocol::LiveServerIdentity {
            host: muxe_protocol::HostKind::Zellij,
            discovery_key: session.clone(),
            server_id: muxe_protocol::ServerId::new(format!("zellij-session:{session}")),
        },
    )
    .await
    .wrap_err("could not establish the Zellij UI broker connection")?;
    let frame = client
        .request_frame(muxe_protocol::ClientRequest::AttachUi(
            muxe_protocol::AttachUi {
                root: muxe_protocol::MenuId::new(&menu.root),
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

/// Records a launcher failure to the persistent audit log and makes a best-effort
/// Herdr notification. The log remains authoritative: notification failure never
/// replaces or hides the original error, which the caller still returns.
async fn report_launcher_failure(
    logger: &muxe::logging::Logger,
    client: Option<&muxe_adapter_herdr::HerdrSocketClient>,
    operation: &str,
    message: &str,
) {
    let message = bounded_log_message(message);
    if let Ok(event) = muxe::logging::LogEvent::new(
        env!("CARGO_PKG_VERSION"),
        "herdr",
        operation,
        message.clone(),
    ) {
        let _ = logger.append(&event);
    }
    if let Some(client) = client {
        best_effort_notify(client, &format!("{operation} failed: {message}")).await;
    }
}

/// Truncates to the audit bound on a char boundary. Callers pass only semantic
/// diagnostics here, never resolved argv, environment values, terminal input,
/// configuration scalars, or native payloads.
fn bounded_log_message(message: &str) -> String {
    const MAX: usize = muxe::logging::MAX_MESSAGE_LEN;
    if message.len() <= MAX {
        return message.to_owned();
    }
    let mut end = MAX;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

/// Best-effort `notification.show` capped at Herdr's 240-character limit. Every
/// failure is swallowed: the persistent log above is authoritative.
async fn best_effort_notify(client: &muxe_adapter_herdr::HerdrSocketClient, text: &str) {
    let body: String = text.chars().take(240).collect();
    let Some(metadata) = muxe_adapter_herdr::generated::method_metadata("notification.show") else {
        return;
    };
    let _ = client
        .unary(metadata, serde_json::json!({"title": "Muxe", "body": body}))
        .await;
}

fn selected_host(requested: HostSelector) -> Result<HostSelector> {
    match requested {
        HostSelector::Herdr | HostSelector::Zellij => Ok(requested),
        HostSelector::Auto if env::var_os("HERDR_SOCKET_PATH").is_some() => Ok(HostSelector::Herdr),
        HostSelector::Auto
            if env::var_os("ZELLIJ_SESSION_NAME").is_some_and(|value| !value.is_empty()) =>
        {
            Ok(HostSelector::Zellij)
        }
        HostSelector::Auto => bail!("could not detect a supported host for muxe pane open"),
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
    launcher_origin_from(runtime.client(), runtime.schema(), &env_lookup).await
}

/// Resolves the launcher origin against one fresh snapshot. The environment lookup is
/// injected so the selection boundary is unit-testable without mutating process state.
async fn launcher_origin_from(
    client: &muxe_adapter_herdr::HerdrSocketClient,
    schema: &muxe_adapter_herdr::ApiSchema,
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<muxe_adapter_herdr::FocusedPane> {
    let selected = select_launcher_origin(get)?;
    let mut origin = muxe_adapter_herdr::pane_by_identity(
        client,
        schema,
        muxe_core::WorkspaceId::new(selected.workspace),
        muxe_core::TabId::new(selected.tab),
        muxe_core::PaneId::new(selected.pane),
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
    workspace: String,
    tab: String,
    pane: String,
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
type SavedOriginTuple = (String, String, String, Option<PathBuf>);

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
        [Some(workspace), Some(tab), Some(pane)] => Ok(Some((workspace, tab, pane, cwd))),
        _ => bail!("MUXE_HERDR_ORIGIN_* must supply one complete immutable origin tuple"),
    }
}

fn launcher_tuple(
    get: &dyn Fn(&str) -> Option<String>,
    workspace_name: &str,
    tab_name: &str,
    pane_name: &str,
) -> Result<Option<(String, String, String)>> {
    let values = [workspace_name, tab_name, pane_name].map(get);
    match values {
        [None, None, None] => Ok(None),
        [Some(workspace), Some(tab), Some(pane)] => Ok(Some((workspace, tab, pane))),
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
    match requested {
        None => Ok(0.5),
        Some(muxe::cli::Dimension::Percent(percent)) if (1..100).contains(&percent) => {
            Ok(f64::from(percent) / 100.0)
        }
        Some(muxe::cli::Dimension::Cells(cells)) if cells > 0 && cells < available => {
            Ok(f64::from(cells) / f64::from(available))
        }
        Some(muxe::cli::Dimension::Percent(_)) => {
            bail!("Herdr split percentage must be between 1% and 99%")
        }
        Some(muxe::cli::Dimension::Cells(_)) => bail!(
            "Herdr split cell dimension must be positive and smaller than the validated destination axis"
        ),
    }
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
        HostSelector::Herdr => run_herdr_ui(menu).await,
        HostSelector::Zellij => run_zellij_ui(menu).await,
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
    let socket = ensure_herdr_broker(&paths.cache_dir, &paths.config_file(), &runtime).await?;
    let live_server = LiveServerIdentity {
        host: ProtocolHostKind::Herdr,
        discovery_key: runtime.identity().discovery_key.clone(),
        server_id: muxe_protocol::ServerId::new(runtime.identity().live_server_id.clone()),
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
    let caller = muxe_adapter_herdr::pane_by_id(runtime.client(), runtime.schema(), pane.as_str())
        .await
        .wrap_err("the Herdr UI caller pane is not live after its launcher move")?;
    let workspace = WorkspaceId::new(caller.workspace.as_str());
    let tab = HostTabId::new(caller.tab.as_str());
    Ok(AttachUi {
        root: MenuId::new(&menu.root),
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
    /// Production disconnect mapping is unit-consistent (DESIGN 2215): one dead
    /// Ready target makes every member observe an incomplete unit, never a
    /// per-member split where oldA restores while oldB stands down.
    #[tokio::test]
    async fn journal_recovery_reports_unit_incomplete_when_one_target_is_dead() {
        use muxe::lifecycle::journal::{
            ActivationJournal, JournalState, MemberState, MemberTransition, UnitKind,
        };
        use muxe_broker::{RecoveryDecision, RecoveryJournal};
        use muxe_protocol::control::{CompatibilityRecord, HandoffId};
        let cache = tempfile::tempdir().expect("owned recovery cache");
        let cache_dir = cache.path().join("cache");
        std::fs::create_dir_all(&cache_dir).expect("cache exists");
        let handoff = HandoffId([23; 16]);
        let wanted = handoff_hex(&handoff);
        let handoff_b = HandoffId([24; 16]);
        let wanted_b = handoff_hex(&handoff_b);
        let record = CompatibilityRecord {
            muxe_version: "9.9.9".to_owned(),
            target_triple: "test-triple".to_owned(),
            application_schema_fingerprint: muxe_protocol::SchemaFingerprint([1; 32]),
            zellij: None,
            herdr: None,
        };
        let socket_a = cache.path().join("a.sock");
        let socket_b = cache.path().join("b.sock");
        std::fs::write(&socket_b, b"foreign socket").expect("write foreign B endpoint");
        let members = vec![
            MemberState {
                host_identity: "session-a".to_owned(),
                old_socket: socket_a.clone(),
                target_socket: Some(socket_a.clone()),
                handoff_id: Some(wanted.clone()),
                state: MemberTransition::Ready,
            },
            MemberState {
                host_identity: "session-b".to_owned(),
                old_socket: socket_b.clone(),
                target_socket: Some(socket_b),
                handoff_id: Some(wanted_b),
                state: MemberTransition::Ready,
            },
        ];
        let mut journal = ActivationJournal::new(
            UnitKind::Zellij {
                bridge_path_hash: "unit-test".to_owned(),
            },
            record.clone(),
            record,
            members,
        );
        journal.state = JournalState::Ready;
        let path = muxe::lifecycle::journal::write_journal(&cache_dir, &journal)
            .expect("write Ready journal");
        let decision = JournalRecovery {
            journal_path: Some(path.clone()),
            discovery_key: "session-a".to_owned(),
            zellij_exe: None,
        }
        .recovery_decision(&handoff)
        .await;
        assert!(matches!(decision, RecoveryDecision::Preserve { .. }));
        drop(decision);
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
        assert_eq!(selected.pane, "w1:pA");
        assert_eq!(selected.source, "HERDR_ACTIVE_*");

        let managed_only: HashMap<String, String> = HashMap::from([
            ("HERDR_WORKSPACE_ID".to_owned(), "w1".to_owned()),
            ("HERDR_TAB_ID".to_owned(), "w1:tB".to_owned()),
            ("HERDR_PANE_ID".to_owned(), "w1:pB".to_owned()),
        ]);
        let selected =
            select_launcher_origin(&lookup(&managed_only)).expect("managed tuple applies");
        assert_eq!(selected.pane, "w1:pB");
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
                    let id = serde_json::from_slice::<serde_json::Value>(&request)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("id")
                                .and_then(|id| id.as_str())
                                .map(str::to_owned)
                        })
                        .unwrap_or_default();
                    let response = serde_json::json!({
                        "id": id,
                        "result": {"type": "session_snapshot", "snapshot": body},
                    });
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
        let client = muxe_adapter_herdr::HerdrSocketClient::new(&path);
        let schema = muxe_adapter_herdr::ApiSchema::parse(
            serde_json::from_str(include_str!(
                "../../../fixtures/herdr/herdr-api.schema.json"
            ))
            .expect("bundled schema JSON"),
        )
        .expect("bundled schema parses");

        let origin = launcher_origin_from(&client, &schema, &lookup(&active_map()))
            .await
            .expect("inherited ACTIVE pane resolves");
        assert_eq!(origin.pane.as_str(), "w1:pA");
        assert_eq!(origin.tab.as_str(), "w1:tA");
        assert_eq!(origin.cwd, PathBuf::from("/a"));

        let saved: HashMap<String, String> = HashMap::from([
            ("MUXE_HERDR_ORIGIN_WORKSPACE_ID".to_owned(), "w1".to_owned()),
            ("MUXE_HERDR_ORIGIN_TAB_ID".to_owned(), "w1:tA".to_owned()),
            ("MUXE_HERDR_ORIGIN_PANE_ID".to_owned(), "w1:pA".to_owned()),
        ]);
        let origin = launcher_origin_from(&client, &schema, &lookup(&saved))
            .await
            .expect("saved origin without cwd keeps live enrichment");
        assert_eq!(origin.pane.as_str(), "w1:pA");
        assert_eq!(origin.cwd, PathBuf::from("/a"));

        let empty: HashMap<String, String> = HashMap::new();
        assert!(
            launcher_origin_from(&client, &schema, &lookup(&empty))
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
                    let result = if method.as_str() == "session.snapshot" {
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
    async fn launcher_failure_is_logged_and_notified_before_ui_creation() {
        let directory = tempfile::tempdir().expect("owned launcher failure directory");
        let cache_dir = directory.path().join("cache");
        std::fs::create_dir_all(&cache_dir).expect("owned cache directory exists");
        std::fs::set_permissions(
            &cache_dir,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .expect("owned cache directory is owner-only");
        let logger = muxe::logging::Logger::open(&cache_dir, env!("CARGO_PKG_VERSION"))
            .expect("owned audit log opens");
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
        let client = muxe_adapter_herdr::HerdrSocketClient::new(&path);
        let schema = muxe_adapter_herdr::ApiSchema::parse(
            serde_json::from_str(include_str!(
                "../../../fixtures/herdr/herdr-api.schema.json"
            ))
            .expect("bundled schema parses"),
        )
        .expect("bundled schema validates");
        let error = launcher_origin_from(&client, &schema, &lookup(&active_map()))
            .await
            .expect_err("absent ACTIVE pane fails before UI creation");
        assert!(error.to_string().contains("not live"));
        report_launcher_failure(&logger, Some(&client), "pane-open", &error.to_string()).await;
        let log = std::fs::read_to_string(cache_dir.join("logs").join("muxe.jsonl"))
            .expect("audit log written before exit");
        let record: serde_json::Value = serde_json::from_str(log.trim()).expect("audit log parses");
        assert_eq!(record["operation"], "pane-open");
        assert_eq!(record["host"], "herdr");
        let message = record["message"].as_str().expect("audit message recorded");
        assert!(message.contains("not live"));
        assert!(message.len() <= muxe::logging::MAX_MESSAGE_LEN);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if seen.lock().expect("method log is readable").len() >= 2 {
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
    fn activate_spawn_renders_both_host_shapes() {
        let exe = Path::new("/opt/muxenv/muxe");
        let config = Path::new("/cfg/config.yml");
        let cache = Path::new("/cache");
        let entries = std::collections::HashMap::from([
            (PathBuf::from("/run/b-h.sock"), "herdr".to_owned()),
            (PathBuf::from("/run/b-z.sock"), "zellij".to_owned()),
        ]);
        let herdr = activate_spawn_argv(
            exe,
            config,
            cache,
            Some(&PathBuf::from("/bin/herdr")),
            None,
            &entries,
            &muxe::lifecycle::SpawnMember {
                host_identity: "/herdr.sock".to_owned(),
                handoff_hex: "ab".repeat(16),
                endpoint: PathBuf::from("/run/b-h.sock"),
            },
        )
        .expect("herdr spawn renders");
        let herdr_text = herdr
            .1
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(herdr_text[0], "broker");
        assert_eq!(herdr_text[1], "serve-herdr");
        assert!(herdr_text.contains(&"--herdr-socket".to_owned()));
        let zellij = activate_spawn_argv(
            exe,
            config,
            cache,
            None,
            Some(&PathBuf::from("/bin/zellij")),
            &entries,
            &muxe::lifecycle::SpawnMember {
                host_identity: "session-a".to_owned(),
                handoff_hex: "cd".repeat(16),
                endpoint: PathBuf::from("/run/b-z.sock"),
            },
        )
        .expect("zellij spawn renders");
        let zellij_text = zellij
            .1
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(zellij_text[1], "serve-zellij");
        assert!(zellij_text.contains(&"--session".to_owned()));
        assert!(
            activate_spawn_argv(
                exe,
                config,
                cache,
                None,
                None,
                &std::collections::HashMap::new(),
                &muxe::lifecycle::SpawnMember {
                    host_identity: "x".to_owned(),
                    handoff_hex: "ab".repeat(16),
                    endpoint: PathBuf::from("/run/odd.sock"),
                },
            )
            .is_err()
        );
    }
}
#[cfg(test)]
mod mixed_recovery_production_tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::{fs, path::Path, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use muxe_adapter_api::{
        AdapterCapabilities, AdapterError, AdapterHealthEvent, CaptureLease, CaptureReleaseReason,
        CaptureRequest, DispatchAccepted, HostAdapter, HostIdentity, KeyboardCapabilities,
        ModalScopeId, NativeDispatchRequest, OriginCaptureRequest, PendingPaneLease,
        PendingPaneRegistration, PortableDispatchRequest,
    };
    use muxe_core::{
        ActionValidation, ActionValidator, CompiledConfig, CompiledGeneration, ConfigDiagnostic,
        KeyCapabilities, OriginContext, SourceId,
    };
    use muxe_protocol::{
        control::{CompatibilityRecord, ZellijCompatibility},
        wire::{HostKind, LiveServerIdentity, SchemaFingerprint, ServerId},
    };
    use sha2::{Digest, Sha256};
    use tokio::sync::watch;

    use super::{JournalRecovery, handoff_hex};

    struct ShutdownGate {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    struct RecoveryAdapter {
        discovery_key: String,
        shutdown_gate: Option<Arc<ShutdownGate>>,
        resume_gate: Option<Arc<ShutdownGate>>,
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
                discovery_key: self.discovery_key.clone(),
                live_server_id: format!("server-{}", self.discovery_key),
            })
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

        async fn release_pending_pane(&self, _lease: PendingPaneLease) -> Result<(), AdapterError> {
            Ok(())
        }

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
            Err(AdapterError::new(
                muxe_adapter_api::AdapterErrorKind::Unsupported,
                "recovery test does not capture UI origin",
            ))
        }

        async fn dispatch_portable(
            &self,
            _request: PortableDispatchRequest,
        ) -> Result<DispatchAccepted, AdapterError> {
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
            std::future::pending().await
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
            Ok(())
        }
    }

    fn compiled_config() -> CompiledConfig {
        muxe_core::compile_yaml(
            CompiledGeneration(1),
            SourceId::new("recovery-test.yml"),
            "version: 1\nsettings:\n  reload:\n    watch: false\nmenus:\n  main:\n    bindings:\n      q:\n        label: quit\n        action: config:reload\n",
            KeyCapabilities::default(),
            None,
        )
        .expect("minimal recovery config compiles")
    }

    fn record(version: &str) -> CompatibilityRecord {
        CompatibilityRecord {
            muxe_version: version.to_owned(),
            target_triple: "test-triple".to_owned(),
            application_schema_fingerprint: SchemaFingerprint([1; 32]),
            zellij: Some(ZellijCompatibility {
                source_revision: "fixture".to_owned(),
                generated_action_fingerprint: SchemaFingerprint([2; 32]),
                bridge_protocol_fingerprint: SchemaFingerprint([3; 32]),
                bridge_build_id: Some(SchemaFingerprint([4; 32])),
            }),
            herdr: None,
        }
    }
    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn identity(key: &str) -> LiveServerIdentity {
        LiveServerIdentity {
            host: HostKind::Zellij,
            discovery_key: key.to_owned(),
            server_id: ServerId::new(format!("server-{key}")),
        }
    }
    async fn wait_for_journal_removed(path: &Path) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if !path.exists() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "journal was not removed after all observable old resumes: {:?}",
                muxe::lifecycle::journal::read_journal(path)
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_socket(path: &Path, expected: bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if path.exists() == expected {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "socket {} did not reach expected presence={expected}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn wait_for_old_status(
        socket: &Path,
        discovery_key: &str,
        expected_record: &CompatibilityRecord,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut last: String;
        loop {
            match muxe::lifecycle::control::ControlClient::connect(socket).await {
                Ok(mut control) => match control.status().await {
                    Ok(status) => {
                        last = format!(
                            "lifecycle={:?} discovery={} current={:?}",
                            status.lifecycle, status.live_server.discovery_key, status.current
                        );
                        if status.lifecycle == muxe_protocol::control::LifecycleState::Running
                            && status.live_server.discovery_key == discovery_key
                            && status.current == *expected_record
                        {
                            return;
                        }
                    }
                    Err(error) => last = format!("status error: {error}"),
                },
                Err(error) => last = format!("connect error: {error}"),
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "old broker {discovery_key} did not report Running with its recorded old identity: {last}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Asserts the shared resume-barrier boundary: the unit lock stays held, the
    /// recorded bridge is restored, every target is durably retired, and exactly
    /// the expected winner (if any) has recorded its resume ACK. Synchronous: the
    /// loser's entry already causally follows the winner's ACK write and permit
    /// release, so no polling is needed.
    fn assert_resume_boundary(journal_path: &Path, cache_dir: &Path, expect_resumed: Option<&str>) {
        assert!(
            muxe::lifecycle::journal::acquire_unit_lock(
                cache_dir,
                &muxe::lifecycle::journal::UnitKind::Zellij {
                    bridge_path_hash: "mixed-production".to_owned(),
                },
            )
            .is_err(),
            "unit lock remains held through local resume ACK barrier"
        );
        let journal = muxe::lifecycle::journal::read_journal(journal_path)
            .expect("recovery journal remains readable at resume barrier");
        assert!(
            journal.bridge_restored,
            "recorded bridge restores before any old resume"
        );
        assert!(
            journal.target_members.iter().all(|target| matches!(
                target.state,
                muxe::lifecycle::journal::TargetTransition::Retired
            )),
            "every target retires before old resume ACKs: {:?}",
            journal.target_members
        );
        let resumed: Vec<&str> = journal
            .members
            .iter()
            .filter(|member| {
                matches!(
                    member.state,
                    muxe::lifecycle::journal::MemberTransition::Resumed
                )
            })
            .map(|member| member.host_identity.as_str())
            .collect();
        match expect_resumed {
            None => assert!(
                resumed.is_empty(),
                "no old resume ACKs before the first release: {resumed:?}"
            ),
            Some(winner) => assert_eq!(
                resumed,
                [winner],
                "only the released winner ACKs before the second release"
            ),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "host-free mixed recovery proof keeps child lifecycle, retained sessions, journal permits, and artifact witnesses in one auditable scenario"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn production_journal_recovery_restores_mixed_ready_unit_after_disconnect() {
        let root = tempfile::tempdir().expect("owned recovery root");
        let cache_dir = root.path().join("cache");
        let runtime_dir = root.path().join("runtime");
        let config_path = root.path().join("config.yml");
        fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700))
            .expect("secure recovery root");
        fs::create_dir_all(&cache_dir).expect("cache directory");
        fs::write(
            &config_path,
            "version: 1\nsettings:\n  reload:\n    watch: false\nmenus: {}\n",
        )
        .expect("config file");
        let fake_zellij = root.path().join("zellij-reloader");
        let reload_log = root.path().join("reload.log");
        fs::write(
            &fake_zellij,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n",
                reload_log.display()
            ),
        )
        .expect("write owned fake Zellij reloader");
        fs::set_permissions(&fake_zellij, std::fs::Permissions::from_mode(0o700))
            .expect("make owned fake reloader executable");

        let stable = root.path().join("muxe-zellij.wasm");
        let previous = root.path().join("muxe-zellij.wasm.previous");
        let old_bridge = b"old bridge bytes";
        let target_bridge = b"target bridge bytes";
        fs::write(&stable, target_bridge).expect("target bridge installed");
        fs::write(&previous, old_bridge).expect("old bridge backup");
        muxe::integration::receipt::store(
            root.path(),
            &muxe::integration::receipt::Receipt {
                schema_version: muxe::integration::receipt::RECEIPT_SCHEMA_VERSION,
                bridge: muxe::integration::receipt::BridgeRecord {
                    canonical_path: stable.clone(),
                    installed_version: "target".to_owned(),
                    installed_digest: digest(target_bridge),
                    previous_digest: Some(digest(old_bridge)),
                    bridge_compat: record("target").zellij,
                },
                configs: Vec::new(),
            },
        )
        .expect("target receipt");

        let old_record = record("old");
        let target_record = record("target");
        let endpoint_old_a =
            muxe_broker::RuntimeEndpoint::in_runtime_dir(&runtime_dir, HostKind::Zellij, "old-a")
                .expect("old A endpoint");
        let endpoint_old_b =
            muxe_broker::RuntimeEndpoint::in_runtime_dir(&runtime_dir, HostKind::Zellij, "old-b")
                .expect("old B endpoint");
        let endpoint_target_a = muxe_broker::RuntimeEndpoint::in_runtime_dir(
            &runtime_dir,
            HostKind::Zellij,
            "target-a",
        )
        .expect("target A endpoint");
        let endpoint_target_b = muxe_broker::RuntimeEndpoint::in_runtime_dir(
            &runtime_dir,
            HostKind::Zellij,
            "target-b",
        )
        .expect("target B endpoint");

        let resume_gate_a = Arc::new(ShutdownGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let resume_gate_b = Arc::new(ShutdownGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let adapter_a = Arc::new(RecoveryAdapter {
            discovery_key: "old-a".to_owned(),
            shutdown_gate: None,
            resume_gate: Some(Arc::clone(&resume_gate_a)),
        });
        let adapter_b = Arc::new(RecoveryAdapter {
            discovery_key: "old-b".to_owned(),
            shutdown_gate: None,
            resume_gate: Some(Arc::clone(&resume_gate_b)),
        });
        let old_a = muxe_broker::Broker::from_compiled(adapter_a, &config_path, compiled_config());
        let old_b = muxe_broker::Broker::from_compiled(adapter_b, &config_path, compiled_config());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let journal_path = muxe::lifecycle::journal::activation_dir(&cache_dir)
            .join("zellij-mixed-production.json");
        let server_a = muxe_broker::BrokerServer::start_activation(
            Arc::clone(&old_a),
            endpoint_old_a.clone(),
            muxe_broker::ActivationBootstrap::Running {
                current: old_record.clone(),
            },
            Some(Arc::new(JournalRecovery {
                journal_path: Some(journal_path.clone()),
                discovery_key: "old-a".to_owned(),
                zellij_exe: Some(fake_zellij.clone()),
            })),
        )
        .await
        .expect("old A production server");
        let server_b = muxe_broker::BrokerServer::start_activation(
            Arc::clone(&old_b),
            endpoint_old_b.clone(),
            muxe_broker::ActivationBootstrap::Running {
                current: old_record.clone(),
            },
            Some(Arc::new(JournalRecovery {
                journal_path: Some(journal_path.clone()),
                discovery_key: "old-b".to_owned(),
                zellij_exe: Some(fake_zellij.clone()),
            })),
        )
        .await
        .expect("old B production server");
        let task_a = tokio::spawn(server_a.run(shutdown_rx.clone()));
        let task_b = tokio::spawn(server_b.run(shutdown_rx.clone()));

        let mut control_a =
            muxe::lifecycle::control::ControlClient::connect(endpoint_old_a.socket())
                .await
                .expect("old A coordinator stream");
        let mut control_b =
            muxe::lifecycle::control::ControlClient::connect(endpoint_old_b.socket())
                .await
                .expect("old B coordinator stream");
        let status_a = control_a
            .prepare(target_record.clone())
            .await
            .expect("old A prepared");
        let status_b = control_b
            .prepare(target_record.clone())
            .await
            .expect("old B prepared");
        let handoff_a = status_a.handoff_id.expect("old A handoff");
        let handoff_b = status_b.handoff_id.expect("old B handoff");

        let members = vec![
            muxe::lifecycle::journal::MemberState {
                host_identity: "old-a".to_owned(),
                old_socket: endpoint_old_a.socket().to_path_buf(),
                target_socket: Some(endpoint_target_a.socket().to_path_buf()),
                handoff_id: Some(handoff_hex(&handoff_a)),
                state: muxe::lifecycle::journal::MemberTransition::Ready,
            },
            muxe::lifecycle::journal::MemberState {
                host_identity: "old-b".to_owned(),
                old_socket: endpoint_old_b.socket().to_path_buf(),
                target_socket: Some(endpoint_target_b.socket().to_path_buf()),
                handoff_id: Some(handoff_hex(&handoff_b)),
                state: muxe::lifecycle::journal::MemberTransition::Ready,
            },
        ];
        let mut journal = muxe::lifecycle::journal::ActivationJournal::new(
            muxe::lifecycle::journal::UnitKind::Zellij {
                bridge_path_hash: "mixed-production".to_owned(),
            },
            old_record.clone(),
            target_record.clone(),
            members,
        );
        journal.state = muxe::lifecycle::journal::JournalState::Ready;
        journal.old_bridge_digest = Some(digest(old_bridge));
        journal.staged_bridge_digest = Some(digest(target_bridge));
        journal.backup_path = Some(previous.clone());
        journal.recovery_deadline = 0;
        let written_path =
            muxe::lifecycle::journal::write_journal(&cache_dir, &journal).expect("Ready journal");
        assert_eq!(written_path, journal_path);

        let shutdown_gate = Arc::new(ShutdownGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let target_b = muxe_broker::Broker::from_compiled(
            Arc::new(RecoveryAdapter {
                discovery_key: "old-b".to_owned(),
                shutdown_gate: Some(Arc::clone(&shutdown_gate)),
                resume_gate: None,
            }),
            &config_path,
            compiled_config(),
        );
        let target_server_b = muxe_broker::BrokerServer::start_activation(
            Arc::clone(&target_b),
            endpoint_target_b.clone(),
            muxe_broker::ActivationBootstrap::Target {
                current: target_record,
                handoff: handoff_b,
                live_server: identity("old-b"),
            },
            Some(Arc::new(JournalRecovery {
                journal_path: Some(journal_path.clone()),
                discovery_key: "old-b".to_owned(),
                zellij_exe: Some(fake_zellij.clone()),
            })),
        )
        .await
        .expect("target B production server");
        let target_task = tokio::spawn(target_server_b.run(shutdown_rx.clone()));

        let mut target_control =
            muxe::lifecycle::control::ControlClient::connect(endpoint_target_b.socket())
                .await
                .expect("target B coordinator stream");
        target_control.status().await.expect("target B status");

        drop(control_a);
        drop(control_b);
        tokio::time::timeout(Duration::from_secs(5), shutdown_gate.entered.notified())
            .await
            .expect("owner must issue target B stop before old resume");
        assert!(
            !endpoint_old_a.socket().exists(),
            "old A remains drained at stop barrier"
        );
        assert!(
            !endpoint_old_b.socket().exists(),
            "old B remains drained at stop barrier"
        );
        assert!(
            muxe::lifecycle::journal::acquire_unit_lock(
                &cache_dir,
                &muxe::lifecycle::journal::UnitKind::Zellij {
                    bridge_path_hash: "mixed-production".to_owned(),
                },
            )
            .is_err(),
            "unit lock must remain held through target retirement barrier"
        );
        assert!(
            journal_path.exists(),
            "journal remains present while old resume ACKs are pending"
        );
        shutdown_gate.release.notify_one();
        wait_for_socket(endpoint_target_b.socket(), false).await;
        drop(target_control);
        // Either old member may win the unit-lock race and block on its resume gate
        // while holding the exclusive lock; the loser cannot resume until the winner
        // ACKs. `notify_one` retains a permit, so `select!` observes whichever barrier
        // enters first even if entry precedes the wait. Release that winner first so
        // the test never deadlocks itself against the other gate.
        let first = tokio::time::timeout(Duration::from_mins(1), async {
            tokio::select! {
                () = resume_gate_a.entered.notified() => true,
                () = resume_gate_b.entered.notified() => false,
            }
        })
        .await;
        let Ok(a_first) = first else {
            panic!(
                "old resume barrier entered: {:?}",
                muxe::lifecycle::journal::read_journal(&journal_path)
            );
        };
        assert_resume_boundary(&journal_path, &cache_dir, None);
        if a_first {
            resume_gate_a.release.notify_one();
            wait_for_old_status(endpoint_old_a.socket(), "old-a", &old_record).await;
            tokio::time::timeout(Duration::from_secs(5), resume_gate_b.entered.notified())
                .await
                .expect("old B resume barrier entered");
            assert_resume_boundary(&journal_path, &cache_dir, Some("old-a"));
            resume_gate_b.release.notify_one();
            wait_for_old_status(endpoint_old_b.socket(), "old-b", &old_record).await;
        } else {
            resume_gate_b.release.notify_one();
            wait_for_old_status(endpoint_old_b.socket(), "old-b", &old_record).await;
            tokio::time::timeout(Duration::from_secs(5), resume_gate_a.entered.notified())
                .await
                .expect("old A resume barrier entered");
            assert_resume_boundary(&journal_path, &cache_dir, Some("old-b"));
            resume_gate_a.release.notify_one();
            wait_for_old_status(endpoint_old_a.socket(), "old-a", &old_record).await;
        }
        wait_for_journal_removed(&journal_path).await;
        assert_eq!(
            fs::read(&stable).expect("restored bridge"),
            old_bridge,
            "recorded backup must restore the shared bridge"
        );
        let receipt = muxe::integration::receipt::load(root.path())
            .expect("restored receipt readable")
            .expect("restored receipt present");
        assert_eq!(receipt.bridge.canonical_path, stable);
        assert_eq!(receipt.bridge.installed_digest, digest(old_bridge));
        assert_eq!(
            receipt.bridge.previous_digest,
            Some(digest(target_bridge)),
            "receipt retains rotated target authority after rollback"
        );
        let reloads = fs::read_to_string(&reload_log).expect("owned reloader recorded sessions");
        assert_eq!(
            reloads.lines().count(),
            2,
            "every recorded session must be reloaded exactly once"
        );
        assert!(reloads.lines().any(|line| line.contains("--session old-a")));
        assert!(reloads.lines().any(|line| line.contains("--session old-b")));

        shutdown_tx.send(true).expect("shutdown servers");
        let _ = tokio::join!(task_a, task_b, target_task);
    }
}
