# Print an optspec for argparse to handle cmd's options that are independent of any subcommand.
function __fish_muxe_global_optspecs
    string join \n h/help V/version
end

function __fish_muxe_needs_command
    # Figure out if the current invocation already has a command.
    set -l cmd (commandline -opc)
    set -e cmd[1]
    argparse -s (__fish_muxe_global_optspecs) -- $cmd 2>/dev/null
    or return
    if set -q argv[1]
        # Also print the command, so this can be used to figure out what it is.
        echo $argv[1]
        return 1
    end
    return 0
end

function __fish_muxe_using_subcommand
    set -l cmd (__fish_muxe_needs_command)
    test -z "$cmd"
    and return 1
    contains -- $cmd[1] $argv
end

complete -c muxe -n "__fish_muxe_needs_command" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_needs_command" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "init" -d 'Create the starter configuration without installing host integration'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "config" -d 'Validate configuration for every supported host'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "menu" -d 'Open a configured root menu through a host launcher'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "pane" -d 'Open a generic command pane'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "integration" -d 'Install or remove managed host integration'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "activate" -d 'Activate this version for selected live hosts'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "broker" -d 'Manage broker lifecycle'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "compatibility" -d 'Print the embedded compatibility record'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "purge" -d 'Remove explicitly selected retained Muxe data'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "ui" -d 'Run the native terminal UI'
complete -c muxe -n "__fish_muxe_needs_command" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand init" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand init" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand config; and not __fish_seen_subcommand_from check help" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand config; and not __fish_seen_subcommand_from check help" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand config; and not __fish_seen_subcommand_from check help" -f -a "check" -d 'Check the base configuration with each host override'
complete -c muxe -n "__fish_muxe_using_subcommand config; and not __fish_seen_subcommand_from check help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand config; and __fish_seen_subcommand_from check" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand config; and __fish_seen_subcommand_from check" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand config; and __fish_seen_subcommand_from help" -f -a "check" -d 'Check the base configuration with each host override'
complete -c muxe -n "__fish_muxe_using_subcommand config; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and not __fish_seen_subcommand_from open dump help" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and not __fish_seen_subcommand_from open dump help" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and not __fish_seen_subcommand_from open dump help" -f -a "open" -d 'Open a root menu as a focused modal UI'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and not __fish_seen_subcommand_from open dump help" -f -a "dump" -d 'Print the effective contents of one or every configured menu as JSON'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and not __fish_seen_subcommand_from open dump help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l host -d 'Select a host or inherit it from the current environment' -r -f -a "auto\t''
zellij\t''
herdr\t''"
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l pane-type -d 'Select the host pane representation' -r -f -a "split\t''
overlay\t''
popup\t''"
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l parent-pane -d 'Select an explicit parent pane or the captured origin pane' -r
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l direction -d 'Choose the split direction where the host supports it' -r -f -a "down\t''
up\t''
left\t''
right\t''"
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l width -d 'Set a pane width in terminal cells or as a percentage' -r
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l height -d 'Set a pane height in terminal cells or as a percentage' -r
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l position -d 'Set an overlay or popup position as `x,y` terminal-cell coordinates' -r
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l theme -d 'Override the configured theme for this invocation' -r
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -l color-scheme -d 'Override the configured color scheme for this invocation' -r
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from open" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from dump" -l all -d 'Dump every named menu in an object keyed by menu ID'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from dump" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from dump" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from help" -f -a "open" -d 'Open a root menu as a focused modal UI'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from help" -f -a "dump" -d 'Print the effective contents of one or every configured menu as JSON'
complete -c muxe -n "__fish_muxe_using_subcommand menu; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and not __fish_seen_subcommand_from open help" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and not __fish_seen_subcommand_from open help" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and not __fish_seen_subcommand_from open help" -f -a "open" -d 'Open one generic command pane'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and not __fish_seen_subcommand_from open help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l host -d 'Select a host or inherit it from the current environment' -r -f -a "auto\t''
zellij\t''
herdr\t''"
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l pane-type -d 'Select the host pane representation' -r -f -a "split\t''
overlay\t''
popup\t''"
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l parent-pane -d 'Select an explicit parent pane or the captured origin pane' -r
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l direction -d 'Choose the split direction where the host supports it' -r -f -a "down\t''
up\t''
left\t''
right\t''"
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l width -d 'Set a pane width in terminal cells or as a percentage' -r
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l height -d 'Set a pane height in terminal cells or as a percentage' -r
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l position -d 'Set an overlay or popup position as `x,y` terminal-cell coordinates' -r
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l cwd -d 'Override the generic child working directory' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -l no-focus -d 'Leave focus on the current pane after opening the generic child'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from open" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from help" -f -a "open" -d 'Open one generic command pane'
complete -c muxe -n "__fish_muxe_using_subcommand pane; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and not __fish_seen_subcommand_from install uninstall help" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and not __fish_seen_subcommand_from install uninstall help" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and not __fish_seen_subcommand_from install uninstall help" -f -a "install" -d 'Install the bundled Zellij bridge and optionally configure Zellij KDL'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and not __fish_seen_subcommand_from install uninstall help" -f -a "uninstall" -d 'Remove receipt-owned Zellij integration artifacts'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and not __fish_seen_subcommand_from install uninstall help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from install" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from install" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from install" -f -a "zellij" -d 'Shared install or uninstall options for the Zellij integration'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from install" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from uninstall" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from uninstall" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from uninstall" -f -a "zellij" -d 'Shared install or uninstall options for the Zellij integration'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from uninstall" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from help" -f -a "install" -d 'Install the bundled Zellij bridge and optionally configure Zellij KDL'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from help" -f -a "uninstall" -d 'Remove receipt-owned Zellij integration artifacts'
complete -c muxe -n "__fish_muxe_using_subcommand integration; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand activate" -l host -d 'Select live hosts for activation' -r -f -a "all\t''
current\t''
zellij\t''
herdr\t''"
complete -c muxe -n "__fish_muxe_using_subcommand activate" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand activate" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and not __fish_seen_subcommand_from retire serve-herdr serve-zellij help" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and not __fish_seen_subcommand_from retire serve-herdr serve-zellij help" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and not __fish_seen_subcommand_from retire serve-herdr serve-zellij help" -f -a "retire" -d 'Drain and retire brokers without starting replacements'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and not __fish_seen_subcommand_from retire serve-herdr serve-zellij help" -f -a "serve-herdr" -d 'Start the target Herdr broker in the repository-owned upgrade runner'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and not __fish_seen_subcommand_from retire serve-herdr serve-zellij help" -f -a "serve-zellij" -d 'Start the target Zellij broker in the repository-owned upgrade runner'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and not __fish_seen_subcommand_from retire serve-herdr serve-zellij help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from retire" -l host -d 'Select live hosts to retire. The public contract intentionally has no default' -r -f -a "all\t''
current\t''
zellij\t''
herdr\t''"
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from retire" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from retire" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l socket -d 'Absolute Muxe broker endpoint socket path' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l herdr-binary -d 'Absolute path to the pinned Herdr binary' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l herdr-socket -d 'Absolute Herdr server socket path' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l config -d 'Absolute configuration path for the target broker' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l cache-dir -d 'Absolute cache directory for the target broker' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l handoff -d 'Exact 32-hex-character target activation handoff ID' -r
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -l activation-journal -d 'Durable activation journal read before the target broker binds' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-herdr" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l socket -d 'Absolute Muxe broker endpoint socket path' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l zellij-exe -d 'Absolute path to the pinned Zellij binary' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l session -d 'Live Zellij session name the target broker serves' -r
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l config -d 'Absolute configuration path for the target broker' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l cache-dir -d 'Absolute cache directory for the target broker' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l handoff -d 'Exact 32-hex-character target activation handoff ID' -r
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -l activation-journal -d 'Durable activation journal read before the target broker binds' -r -F
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from serve-zellij" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from help" -f -a "retire" -d 'Drain and retire brokers without starting replacements'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from help" -f -a "serve-herdr" -d 'Start the target Herdr broker in the repository-owned upgrade runner'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from help" -f -a "serve-zellij" -d 'Start the target Zellij broker in the repository-owned upgrade runner'
complete -c muxe -n "__fish_muxe_using_subcommand broker; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand compatibility" -l json -d 'Emit the stable `snake_case` JSON record'
complete -c muxe -n "__fish_muxe_using_subcommand compatibility" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand compatibility" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand purge" -l config -d 'Remove the complete Muxe configuration tree'
complete -c muxe -n "__fish_muxe_using_subcommand purge" -l cache -d 'Remove cached schemas and logs'
complete -c muxe -n "__fish_muxe_using_subcommand purge" -l yes -d 'Authorize deletion without an interactive confirmation'
complete -c muxe -n "__fish_muxe_using_subcommand purge" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand purge" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and not __fish_seen_subcommand_from menu help" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and not __fish_seen_subcommand_from menu help" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and not __fish_seen_subcommand_from menu help" -f -a "menu" -d 'Run the UI for one root menu'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and not __fish_seen_subcommand_from menu help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and __fish_seen_subcommand_from menu" -l theme -d 'Override the configured theme for this invocation' -r
complete -c muxe -n "__fish_muxe_using_subcommand ui; and __fish_seen_subcommand_from menu" -l color-scheme -d 'Override the configured color scheme for this invocation' -r
complete -c muxe -n "__fish_muxe_using_subcommand ui; and __fish_seen_subcommand_from menu" -s h -l help -d 'Print help'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and __fish_seen_subcommand_from menu" -s V -l version -d 'Print version'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and __fish_seen_subcommand_from help" -f -a "menu" -d 'Run the UI for one root menu'
complete -c muxe -n "__fish_muxe_using_subcommand ui; and __fish_seen_subcommand_from help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "init" -d 'Create the starter configuration without installing host integration'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "config" -d 'Validate configuration for every supported host'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "menu" -d 'Open a configured root menu through a host launcher'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "pane" -d 'Open a generic command pane'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "integration" -d 'Install or remove managed host integration'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "activate" -d 'Activate this version for selected live hosts'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "broker" -d 'Manage broker lifecycle'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "compatibility" -d 'Print the embedded compatibility record'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "purge" -d 'Remove explicitly selected retained Muxe data'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "ui" -d 'Run the native terminal UI'
complete -c muxe -n "__fish_muxe_using_subcommand help; and not __fish_seen_subcommand_from init config menu pane integration activate broker compatibility purge ui help" -f -a "help" -d 'Print this message or the help of the given subcommand(s)'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from config" -f -a "check" -d 'Check the base configuration with each host override'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from menu" -f -a "open" -d 'Open a root menu as a focused modal UI'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from menu" -f -a "dump" -d 'Print the effective contents of one or every configured menu as JSON'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from pane" -f -a "open" -d 'Open one generic command pane'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from integration" -f -a "install" -d 'Install the bundled Zellij bridge and optionally configure Zellij KDL'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from integration" -f -a "uninstall" -d 'Remove receipt-owned Zellij integration artifacts'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from broker" -f -a "retire" -d 'Drain and retire brokers without starting replacements'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from broker" -f -a "serve-herdr" -d 'Start the target Herdr broker in the repository-owned upgrade runner'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from broker" -f -a "serve-zellij" -d 'Start the target Zellij broker in the repository-owned upgrade runner'
complete -c muxe -n "__fish_muxe_using_subcommand help; and __fish_seen_subcommand_from ui" -f -a "menu" -d 'Run the UI for one root menu'
