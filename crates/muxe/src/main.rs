#![forbid(unsafe_code)]

mod init;

use std::{
    env,
    ffi::OsString,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    sync::Arc,
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
    dispatch(Cli::parse()).await
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
        Command::Compatibility(command) => compatibility(command),
        Command::Purge(command) => purge(command),
        Command::Menu(menu) => match menu.command {
            MenuSubcommand::Open(open) => launch_menu(open).await,
        },
        Command::Pane(pane) => match pane.command {
            PaneSubcommand::Open(open) => launch_pane(open).await,
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

fn compatibility(command: CompatibilityCommand) -> Result<()> {
    let record = muxe::compatibility::embedded_record()?;
    if command.json {
        println!("{}", muxe::compatibility::render_json(&record));
    } else {
        print!("{}", muxe::compatibility::render_human(&record));
    }
    Ok(())
}

fn purge(command: PurgeCommand) -> Result<()> {
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
                println!("already retired {unit}")
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
    Ok(())
}
/// Detects the invoking host for `--host current` from the launcher
/// environment. A Zellij session resolves its canonical bridge through the
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
    let logger = muxe::logging::Logger::open(&paths.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the activation audit log")?;
    let record = muxe::compatibility::embedded_record()
        .wrap_err("could not load the embedded compatibility record")?;
    let current = match command.host {
        HostScope::Current => Some(detect_current_host(&paths.cache_dir)?),
        _ => None,
    };
    let registry = muxe::lifecycle::Registry::open(&paths.cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    let live = registry
        .probe()
        .wrap_err("could not probe the owner-only broker registry")?
        .live;
    let herdr_selected = !matches!(command.host, HostScope::Zellij)
        && live.iter().any(|entry| entry.host_kind == "herdr");
    let zellij_selected = !matches!(command.host, HostScope::Herdr)
        && live.iter().any(|entry| entry.host_kind == "zellij");
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
            &spawn_herdr_binary,
            &spawn_zellij_exe,
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
    let report = muxe::lifecycle::activate(muxe::lifecycle::ActivateInputs {
        config_dir: &paths.config_dir,
        cache_dir: &paths.cache_dir,
        target: record.handoff,
        staged_bridge,
        scope: command.host,
        current,
        control: &muxe::lifecycle::LiveControl,
        spawner: &muxe::lifecycle::ProcessSpawner,
        reloader: &muxe::lifecycle::ZellijCliReloader {
            program: preflight.zellij_exe.clone(),
        },
        preflight: &preflight,
        spawn_argv: &spawn_argv,
        readiness_deadline: Duration::from_secs(120),
        poll_interval: Duration::from_millis(200),
        hooks: muxe::lifecycle::ActivateHooks::default(),
        logger: Some(&logger),
    })
    .await
    .map_err(|error| color_eyre::eyre::eyre!("activation failed: {error}"))?;
    for unit in &report.units {
        match unit {
            muxe::lifecycle::UnitOutcome::Committed { unit } => println!("committed {unit}"),
            muxe::lifecycle::UnitOutcome::Unchanged { unit } => println!("unchanged {unit}"),
            muxe::lifecycle::UnitOutcome::RolledBack { unit, reason } => {
                println!("rolled back {unit}: {reason}")
            }
            muxe::lifecycle::UnitOutcome::Failed { unit, reason } => {
                println!("failed {unit}: {reason}")
            }
        }
    }
    if report
        .units
        .iter()
        .any(|unit| matches!(unit, muxe::lifecycle::UnitOutcome::Failed { .. }))
    {
        bail!("activation reported a failed unit");
    }
    Ok(())
}

/// Renders the exact owned spawn request for one prepared member. Herdr
/// members derive everything from the discovery key; Zellij members use the
/// canonical stable bridge enforced by preflight. The journal path recomputes
/// deterministically because selected units always hash these same inputs.
/// Registry entries resolve the member kind authoritatively; the broker's
/// normal-endpoint stem (`b-z-`/`b-h-`) covers a member registered between
/// the coordinator's probe and this spawn.
fn activate_spawn_argv(
    executable: &Path,
    config_file: &Path,
    cache_dir: &Path,
    herdr_binary: &Option<PathBuf>,
    zellij_exe: &Option<PathBuf>,
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
            zellij_exe: zellij_exe.clone().ok_or_else(|| {
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
        herdr_binary: herdr_binary.clone().ok_or_else(|| {
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
fn serve_event(logger: &muxe::logging::Logger, host: &str, operation: &str, message: String) {
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
async fn serve_herdr_broker(command: BrokerServeHerdrCommand) -> Result<()> {
    let logger = muxe::logging::Logger::open(&command.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the broker service audit log")?;
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
            let handoff_hex = handoff
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
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
    });
    let endpoint_path = endpoint.socket().display().to_string();
    let server = match muxe_broker::BrokerServer::start_activation(
        Arc::clone(&broker),
        endpoint,
        bootstrap,
        Some(recovery),
    )
    .await
    {
        Ok(server) => {
            serve_event(
                &logger,
                "herdr",
                "broker-serve",
                format!("serving {endpoint_path}"),
            );
            server
        }
        Err(error) => {
            serve_event(
                &logger,
                "herdr",
                "broker-serve",
                format!("startup failed: {error}"),
            );
            let _ = registry.unregister(&registration);
            return Err(error).wrap_err("could not start the Herdr broker endpoint");
        }
    };
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = server.run(shutdown_rx).await;
    let _ = registry.unregister(&registration);
    serve_event(&logger, "herdr", "broker-serve", "stopped".to_owned());
    result.wrap_err("Herdr broker service stopped unexpectedly")
}

/// Hidden Zellij broker child mode, mirroring `serve_herdr_broker`: fixed typed
/// inputs, normal-endpoint enforcement from the live session identity,
/// journal/handoff target authorization, and owner-token registry cleanup.
async fn serve_zellij_broker(command: BrokerServeZellijCommand) -> Result<()> {
    let logger = muxe::logging::Logger::open(&command.cache_dir, env!("CARGO_PKG_VERSION"))
        .wrap_err("could not open the broker service audit log")?;
    let adapter =
        muxe_adapter_zellij::ZellijAdapter::connect(muxe_adapter_zellij::ZellijAdapterConfig {
            session_name: command.session.clone(),
            zellij_exe: command.zellij_exe.clone(),
        })
        .await
        .wrap_err("could not connect the pinned Zellij session for broker startup")?;
    let broker = muxe_broker::Broker::load(Arc::new(adapter), &command.config)
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
    let recovery_path: Option<PathBuf> = match (&command.handoff, &command.activation_journal) {
        (None, None) => zellij_journal_for(&command.cache_dir, &command.session, &command.socket),
        (Some(_), Some(journal)) => Some(journal.clone()),
        // Unreachable: bootstrap construction above already failed a half pair closed.
        _ => None,
    };
    let recovery = Arc::new(JournalRecovery {
        journal_path: recovery_path,
        discovery_key: live_server.discovery_key.clone(),
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
            let handoff_hex = handoff
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
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
    let registry = muxe::lifecycle::Registry::open(&command.cache_dir)
        .wrap_err("could not open the owner-only broker registry")?;
    let mut entry = muxe::lifecycle::BrokerEntry::now(
        "zellij",
        live_server.discovery_key.clone(),
        command.socket.clone(),
        std::process::id(),
    );
    entry.live_server = Some(live_server.server_id.as_str().to_owned());
    // Owner-token cleanup, exactly like the Herdr path: this broker removes only
    // its exact entry, never a target sharing the normal socket.
    let registration = registry
        .register(entry)
        .wrap_err("could not register the Zellij broker endpoint")?;
    let server = match muxe_broker::BrokerServer::start_activation(
        Arc::clone(&broker),
        endpoint,
        bootstrap,
        Some(recovery),
    )
    .await
    {
        Ok(server) => {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                format!("serving {}", command.socket.display()),
            );
            server
        }
        Err(error) => {
            serve_event(
                &logger,
                "zellij",
                "broker-serve",
                format!("startup failed: {error}"),
            );
            let _ = registry.unregister(&registration);
            return Err(error).wrap_err("could not start the Zellij broker endpoint");
        }
    };
    let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let result = server.run(shutdown_rx).await;
    let _ = registry.unregister(&registration);
    serve_event(&logger, "zellij", "broker-serve", "stopped".to_owned());
    result.wrap_err("Zellij broker service stopped unexpectedly")
}

/// Owner-side journal mapping for broker disconnect recovery.
struct JournalRecovery {
    journal_path: Option<PathBuf>,
    discovery_key: String,
}

impl muxe_broker::RecoveryJournal for JournalRecovery {
    fn recovery_view(
        &self,
        handoff: &muxe_protocol::control::HandoffId,
    ) -> muxe_broker::RecoveryView {
        let Some(path) = &self.journal_path else {
            return muxe_broker::RecoveryView::absent();
        };
        if !path.exists() {
            return muxe_broker::RecoveryView::absent();
        }
        let inconsistent = || muxe_broker::RecoveryView {
            journal_present: true,
            inconsistent: true,
            member_ready: false,
            member_committed: false,
            recover_after: Duration::ZERO,
        };
        let journal = match muxe::lifecycle::journal::read_journal(path) {
            Ok(journal) => journal,
            Err(_) => return inconsistent(),
        };
        let member = journal
            .members
            .iter()
            .find(|member| member.host_identity == self.discovery_key);
        let Some(member) = member else {
            return inconsistent();
        };
        let wanted = handoff_hex(handoff);
        if member
            .handoff_id
            .as_deref()
            .is_none_or(|recorded| !recorded.eq_ignore_ascii_case(&wanted))
        {
            return inconsistent();
        }
        let (member_ready, member_committed) = match member.state {
            muxe::lifecycle::journal::MemberTransition::Ready => (true, false),
            muxe::lifecycle::journal::MemberTransition::Committed => (false, true),
            _ => (false, false),
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        muxe_broker::RecoveryView {
            journal_present: true,
            inconsistent: false,
            member_ready,
            member_committed,
            recover_after: Duration::from_secs(journal.recovery_deadline.saturating_sub(now)),
        }
    }
}

fn handoff_hex(handoff: &muxe_protocol::control::HandoffId) -> String {
    handoff.0.iter().map(|byte| format!("{byte:02x}")).collect()
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
        HostSelector::Zellij => zellij_open_pane(&logger, open).await,
        HostSelector::Herdr => herdr_open_pane(&logger, &paths.cache_dir, open).await,
        HostSelector::Auto => unreachable!("automatic host selection is resolved"),
    }
}

async fn herdr_open_pane(
    logger: &muxe::logging::Logger,
    cache_dir: &Path,
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
        return herdr_open_ui_pane(logger, cache_dir, &runtime, origin, destination, open, argv)
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

/// Opens a canonical UI pane through the broker-gated launch transaction:
/// prepare a token with the live broker, create the pane carrying it, then
/// register and commit. Any failure after prepare aborts best-effort so no
/// minted token lingers.
async fn herdr_open_ui_pane(
    logger: &muxe::logging::Logger,
    cache_dir: &Path,
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
    let commit_pane = async |client: &mut muxe_broker::BrokerClient,
                             token: muxe_protocol::PendingLaunchToken| {
        let placement = muxe_adapter_herdr::open_ui_pane(
            runtime.client(),
            runtime.schema(),
            muxe_adapter_herdr::UiPaneLaunch {
                origin_workspace: origin.workspace.clone(),
                origin_tab: origin.tab.clone(),
                origin_pane: origin.pane.clone(),
                cwd,
                argv,
                bootstrap_env: bootstrap_env(&origin, token),
                direction,
                ratio,
                focus: true,
            },
        )
        .await
        .map_err(|error| {
            color_eyre::eyre::eyre!("could not open the requested Herdr UI pane: {error}")
        })?;
        let pane = muxe_protocol::HostPaneId::new(placement.ui_pane.as_str());
        client
            .request(muxe_protocol::ClientRequest::RegisterPendingPane(
                muxe_protocol::RegisterPendingPane {
                    token,
                    pane: pane.clone(),
                    temporary_tab: Some(muxe_protocol::HostTabId::new(
                        placement.temporary_tab.as_str(),
                    )),
                },
            ))
            .await
            .map_err(|error| {
                color_eyre::eyre::eyre!("could not register the placed UI pane: {error}")
            })?;
        client
            .request(muxe_protocol::ClientRequest::CommitUiLaunch(
                muxe_protocol::CommitUiLaunch { token, pane },
            ))
            .await
            .map_err(|error| {
                color_eyre::eyre::eyre!("could not commit the placed UI pane: {error}")
            })?;
        Ok::<String, color_eyre::eyre::Error>(placement.ui_pane.as_str().to_owned())
    };
    let mut client = launcher_client(cache_dir, runtime).await?;
    let token = prepare_ui_launch(&mut client, cache_dir, &origin, &root).await?;
    match commit_pane(&mut client, token).await {
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
    runtime: &muxe_adapter_herdr::HerdrRuntime,
) -> Result<muxe_broker::BrokerClient> {
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
    env.insert(
        "MUXE_PENDING_LAUNCH_TOKEN".to_owned(),
        token.0.iter().map(|byte| format!("{byte:02x}")).collect(),
    );
    env
}

/// Opens a pane through the pinned Zellij CLI: `zellij --session <name> run`.
/// Placement maps onto Run flags; semantics the CLI cannot express fail
/// closed instead of silently degrading.
async fn zellij_open_pane(logger: &muxe::logging::Logger, open: &PaneOpen) -> Result<()> {
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
    let endpoint =
        muxe_broker::RuntimeEndpoint::for_host(muxe_protocol::HostKind::Zellij, &session)
            .wrap_err("could not derive the normal Zellij broker endpoint")?;
    let mut client = muxe_broker::BrokerClient::connect(
        endpoint.socket(),
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

fn saved_origin_tuple(
    get: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<SavedOriginTuple>> {
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
    let runtime = muxe_adapter_herdr::HerdrRuntime::connect(herdr_launch_config(paths.cache_dir)?)
        .await
        .wrap_err("could not establish the exact configured Herdr runtime")?;
    let attach = ui_attach_request(&menu, &runtime).await?;
    let live_server = LiveServerIdentity {
        host: ProtocolHostKind::Herdr,
        discovery_key: runtime.identity().discovery_key.clone(),
        server_id: muxe_protocol::ServerId::new(runtime.identity().live_server_id.clone()),
    };
    let endpoint = RuntimeEndpoint::for_host(ProtocolHostKind::Herdr, &live_server.discovery_key)?;
    let mut client = BrokerClient::connect(
        endpoint.socket(),
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

        let mut partial = map.clone();
        partial.remove("MUXE_HERDR_ORIGIN_PANE_ID");
        assert!(select_launcher_origin(&lookup(&partial)).is_err());
    }

    /// Snapshot focus is paneB while the inherited ACTIVE tuple names paneA: the
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

    async fn serve_snapshot(
        path: std::path::PathBuf,
        body: serde_json::Value,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::UnixListener::bind(&path).expect("bind owned snapshot socket");
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
        let server = serve_snapshot(path.clone(), snapshot_body()).await;
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
    async fn serve_recording(
        path: std::path::PathBuf,
        snapshot: serde_json::Value,
        seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        bodies: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::UnixListener::bind(&path).expect("bind owned socket");
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
                    let result = match method.as_str() {
                        "session.snapshot" => serde_json::json!({
                            "type": "session_snapshot",
                            "snapshot": snapshot,
                        }),
                        _ => {
                            bodies
                                .lock()
                                .expect("body log is writable")
                                .push(payload.clone());
                            serde_json::json!({"shown": true})
                        }
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
            path.clone(),
            snapshot,
            std::sync::Arc::clone(&seen),
            std::sync::Arc::clone(&bodies),
        )
        .await;
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
            &Some(PathBuf::from("/bin/herdr")),
            &None,
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
            &None,
            &Some(PathBuf::from("/bin/zellij")),
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
                &None,
                &None,
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
