_muxe() {
    local i cur prev opts cmd
    COMPREPLY=()
    if [[ "${BASH_VERSINFO[0]}" -ge 4 ]]; then
        cur="$2"
    else
        cur="${COMP_WORDS[COMP_CWORD]}"
    fi
    prev="$3"
    cmd=""
    opts=""

    for i in "${COMP_WORDS[@]:0:COMP_CWORD}"
    do
        case "${cmd},${i}" in
            ",$1")
                cmd="muxe"
                ;;
            muxe,activate)
                cmd="muxe__subcmd__activate"
                ;;
            muxe,broker)
                cmd="muxe__subcmd__broker"
                ;;
            muxe,compatibility)
                cmd="muxe__subcmd__compatibility"
                ;;
            muxe,help)
                cmd="muxe__subcmd__help"
                ;;
            muxe,init)
                cmd="muxe__subcmd__init"
                ;;
            muxe,integration)
                cmd="muxe__subcmd__integration"
                ;;
            muxe,menu)
                cmd="muxe__subcmd__menu"
                ;;
            muxe,pane)
                cmd="muxe__subcmd__pane"
                ;;
            muxe,purge)
                cmd="muxe__subcmd__purge"
                ;;
            muxe,ui)
                cmd="muxe__subcmd__ui"
                ;;
            muxe__subcmd__broker,help)
                cmd="muxe__subcmd__broker__subcmd__help"
                ;;
            muxe__subcmd__broker,retire)
                cmd="muxe__subcmd__broker__subcmd__retire"
                ;;
            muxe__subcmd__broker,serve-herdr)
                cmd="muxe__subcmd__broker__subcmd__serve__subcmd__herdr"
                ;;
            muxe__subcmd__broker,serve-zellij)
                cmd="muxe__subcmd__broker__subcmd__serve__subcmd__zellij"
                ;;
            muxe__subcmd__broker__subcmd__help,help)
                cmd="muxe__subcmd__broker__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__broker__subcmd__help,retire)
                cmd="muxe__subcmd__broker__subcmd__help__subcmd__retire"
                ;;
            muxe__subcmd__broker__subcmd__help,serve-herdr)
                cmd="muxe__subcmd__broker__subcmd__help__subcmd__serve__subcmd__herdr"
                ;;
            muxe__subcmd__broker__subcmd__help,serve-zellij)
                cmd="muxe__subcmd__broker__subcmd__help__subcmd__serve__subcmd__zellij"
                ;;
            muxe__subcmd__help,activate)
                cmd="muxe__subcmd__help__subcmd__activate"
                ;;
            muxe__subcmd__help,broker)
                cmd="muxe__subcmd__help__subcmd__broker"
                ;;
            muxe__subcmd__help,compatibility)
                cmd="muxe__subcmd__help__subcmd__compatibility"
                ;;
            muxe__subcmd__help,help)
                cmd="muxe__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__help,init)
                cmd="muxe__subcmd__help__subcmd__init"
                ;;
            muxe__subcmd__help,integration)
                cmd="muxe__subcmd__help__subcmd__integration"
                ;;
            muxe__subcmd__help,menu)
                cmd="muxe__subcmd__help__subcmd__menu"
                ;;
            muxe__subcmd__help,pane)
                cmd="muxe__subcmd__help__subcmd__pane"
                ;;
            muxe__subcmd__help,purge)
                cmd="muxe__subcmd__help__subcmd__purge"
                ;;
            muxe__subcmd__help,ui)
                cmd="muxe__subcmd__help__subcmd__ui"
                ;;
            muxe__subcmd__help__subcmd__broker,retire)
                cmd="muxe__subcmd__help__subcmd__broker__subcmd__retire"
                ;;
            muxe__subcmd__help__subcmd__broker,serve-herdr)
                cmd="muxe__subcmd__help__subcmd__broker__subcmd__serve__subcmd__herdr"
                ;;
            muxe__subcmd__help__subcmd__broker,serve-zellij)
                cmd="muxe__subcmd__help__subcmd__broker__subcmd__serve__subcmd__zellij"
                ;;
            muxe__subcmd__help__subcmd__integration,install)
                cmd="muxe__subcmd__help__subcmd__integration__subcmd__install"
                ;;
            muxe__subcmd__help__subcmd__integration,uninstall)
                cmd="muxe__subcmd__help__subcmd__integration__subcmd__uninstall"
                ;;
            muxe__subcmd__help__subcmd__integration__subcmd__install,zellij)
                cmd="muxe__subcmd__help__subcmd__integration__subcmd__install__subcmd__zellij"
                ;;
            muxe__subcmd__help__subcmd__integration__subcmd__uninstall,zellij)
                cmd="muxe__subcmd__help__subcmd__integration__subcmd__uninstall__subcmd__zellij"
                ;;
            muxe__subcmd__help__subcmd__menu,dump)
                cmd="muxe__subcmd__help__subcmd__menu__subcmd__dump"
                ;;
            muxe__subcmd__help__subcmd__menu,open)
                cmd="muxe__subcmd__help__subcmd__menu__subcmd__open"
                ;;
            muxe__subcmd__help__subcmd__pane,open)
                cmd="muxe__subcmd__help__subcmd__pane__subcmd__open"
                ;;
            muxe__subcmd__help__subcmd__ui,menu)
                cmd="muxe__subcmd__help__subcmd__ui__subcmd__menu"
                ;;
            muxe__subcmd__integration,help)
                cmd="muxe__subcmd__integration__subcmd__help"
                ;;
            muxe__subcmd__integration,install)
                cmd="muxe__subcmd__integration__subcmd__install"
                ;;
            muxe__subcmd__integration,uninstall)
                cmd="muxe__subcmd__integration__subcmd__uninstall"
                ;;
            muxe__subcmd__integration__subcmd__help,help)
                cmd="muxe__subcmd__integration__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__integration__subcmd__help,install)
                cmd="muxe__subcmd__integration__subcmd__help__subcmd__install"
                ;;
            muxe__subcmd__integration__subcmd__help,uninstall)
                cmd="muxe__subcmd__integration__subcmd__help__subcmd__uninstall"
                ;;
            muxe__subcmd__integration__subcmd__help__subcmd__install,zellij)
                cmd="muxe__subcmd__integration__subcmd__help__subcmd__install__subcmd__zellij"
                ;;
            muxe__subcmd__integration__subcmd__help__subcmd__uninstall,zellij)
                cmd="muxe__subcmd__integration__subcmd__help__subcmd__uninstall__subcmd__zellij"
                ;;
            muxe__subcmd__integration__subcmd__install,help)
                cmd="muxe__subcmd__integration__subcmd__install__subcmd__help"
                ;;
            muxe__subcmd__integration__subcmd__install,zellij)
                cmd="muxe__subcmd__integration__subcmd__install__subcmd__zellij"
                ;;
            muxe__subcmd__integration__subcmd__install__subcmd__help,help)
                cmd="muxe__subcmd__integration__subcmd__install__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__integration__subcmd__install__subcmd__help,zellij)
                cmd="muxe__subcmd__integration__subcmd__install__subcmd__help__subcmd__zellij"
                ;;
            muxe__subcmd__integration__subcmd__uninstall,help)
                cmd="muxe__subcmd__integration__subcmd__uninstall__subcmd__help"
                ;;
            muxe__subcmd__integration__subcmd__uninstall,zellij)
                cmd="muxe__subcmd__integration__subcmd__uninstall__subcmd__zellij"
                ;;
            muxe__subcmd__integration__subcmd__uninstall__subcmd__help,help)
                cmd="muxe__subcmd__integration__subcmd__uninstall__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__integration__subcmd__uninstall__subcmd__help,zellij)
                cmd="muxe__subcmd__integration__subcmd__uninstall__subcmd__help__subcmd__zellij"
                ;;
            muxe__subcmd__menu,dump)
                cmd="muxe__subcmd__menu__subcmd__dump"
                ;;
            muxe__subcmd__menu,help)
                cmd="muxe__subcmd__menu__subcmd__help"
                ;;
            muxe__subcmd__menu,open)
                cmd="muxe__subcmd__menu__subcmd__open"
                ;;
            muxe__subcmd__menu__subcmd__help,dump)
                cmd="muxe__subcmd__menu__subcmd__help__subcmd__dump"
                ;;
            muxe__subcmd__menu__subcmd__help,help)
                cmd="muxe__subcmd__menu__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__menu__subcmd__help,open)
                cmd="muxe__subcmd__menu__subcmd__help__subcmd__open"
                ;;
            muxe__subcmd__pane,help)
                cmd="muxe__subcmd__pane__subcmd__help"
                ;;
            muxe__subcmd__pane,open)
                cmd="muxe__subcmd__pane__subcmd__open"
                ;;
            muxe__subcmd__pane__subcmd__help,help)
                cmd="muxe__subcmd__pane__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__pane__subcmd__help,open)
                cmd="muxe__subcmd__pane__subcmd__help__subcmd__open"
                ;;
            muxe__subcmd__ui,help)
                cmd="muxe__subcmd__ui__subcmd__help"
                ;;
            muxe__subcmd__ui,menu)
                cmd="muxe__subcmd__ui__subcmd__menu"
                ;;
            muxe__subcmd__ui__subcmd__help,help)
                cmd="muxe__subcmd__ui__subcmd__help__subcmd__help"
                ;;
            muxe__subcmd__ui__subcmd__help,menu)
                cmd="muxe__subcmd__ui__subcmd__help__subcmd__menu"
                ;;
            *)
                ;;
        esac
    done

    case "${cmd}" in
        muxe)
            opts="-h -V --help --version init menu pane integration activate broker compatibility purge ui help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 1 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__activate)
            opts="-h -V --host --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --host)
                    COMPREPLY=($(compgen -W "all current zellij herdr" -- "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker)
            opts="-h -V --help --version retire serve-herdr serve-zellij help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__help)
            opts="retire serve-herdr serve-zellij help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__help__subcmd__retire)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__help__subcmd__serve__subcmd__herdr)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__help__subcmd__serve__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__retire)
            opts="-h -V --host --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --host)
                    COMPREPLY=($(compgen -W "all current zellij herdr" -- "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__serve__subcmd__herdr)
            opts="-h -V --socket --herdr-binary --herdr-socket --config --cache-dir --handoff --activation-journal --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --socket)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --herdr-binary)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --herdr-socket)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --config)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --cache-dir)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --handoff)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --activation-journal)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__broker__subcmd__serve__subcmd__zellij)
            opts="-h -V --socket --zellij-exe --session --config --cache-dir --handoff --activation-journal --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --socket)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --zellij-exe)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --session)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --config)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --cache-dir)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --handoff)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --activation-journal)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__compatibility)
            opts="-h -V --json --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help)
            opts="init menu pane integration activate broker compatibility purge ui help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__activate)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__broker)
            opts="retire serve-herdr serve-zellij"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__broker__subcmd__retire)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__broker__subcmd__serve__subcmd__herdr)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__broker__subcmd__serve__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__compatibility)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__init)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__integration)
            opts="install uninstall"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__integration__subcmd__install)
            opts="zellij"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__integration__subcmd__install__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__integration__subcmd__uninstall)
            opts="zellij"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__integration__subcmd__uninstall__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__menu)
            opts="open dump"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__menu__subcmd__dump)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__menu__subcmd__open)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__pane)
            opts="open"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__pane__subcmd__open)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__purge)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__ui)
            opts="menu"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__help__subcmd__ui__subcmd__menu)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__init)
            opts="-h -V --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration)
            opts="-h -V --help --version install uninstall help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__help)
            opts="install uninstall help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__help__subcmd__install)
            opts="zellij"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__help__subcmd__install__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__help__subcmd__uninstall)
            opts="zellij"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__help__subcmd__uninstall__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__install)
            opts="-h -V --help --version zellij help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__install__subcmd__help)
            opts="zellij help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__install__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__install__subcmd__help__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__install__subcmd__zellij)
            opts="-q -h -V --quiet --always-configure --never-configure --zellij-config --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --zellij-config)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__uninstall)
            opts="-h -V --help --version zellij help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__uninstall__subcmd__help)
            opts="zellij help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__uninstall__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__uninstall__subcmd__help__subcmd__zellij)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 5 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__integration__subcmd__uninstall__subcmd__zellij)
            opts="-q -h -V --quiet --always-configure --never-configure --zellij-config --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --zellij-config)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu)
            opts="-h -V --help --version open dump help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu__subcmd__dump)
            opts="-h -V --all --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu__subcmd__help)
            opts="open dump help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu__subcmd__help__subcmd__dump)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu__subcmd__help__subcmd__open)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__menu__subcmd__open)
            opts="-h -V --host --pane-type --parent-pane --direction --width --height --position --theme --color-scheme --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --host)
                    COMPREPLY=($(compgen -W "auto zellij herdr" -- "${cur}"))
                    return 0
                    ;;
                --pane-type)
                    COMPREPLY=($(compgen -W "split overlay popup" -- "${cur}"))
                    return 0
                    ;;
                --parent-pane)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --direction)
                    COMPREPLY=($(compgen -W "down up left right" -- "${cur}"))
                    return 0
                    ;;
                --width)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --height)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --position)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --theme)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color-scheme)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__pane)
            opts="-h -V --help --version open help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__pane__subcmd__help)
            opts="open help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__pane__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__pane__subcmd__help__subcmd__open)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__pane__subcmd__open)
            opts="-h -V --host --pane-type --parent-pane --direction --width --height --position --no-focus --cwd --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --host)
                    COMPREPLY=($(compgen -W "auto zellij herdr" -- "${cur}"))
                    return 0
                    ;;
                --pane-type)
                    COMPREPLY=($(compgen -W "split overlay popup" -- "${cur}"))
                    return 0
                    ;;
                --parent-pane)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --direction)
                    COMPREPLY=($(compgen -W "down up left right" -- "${cur}"))
                    return 0
                    ;;
                --width)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --height)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --position)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --cwd)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__purge)
            opts="-h -V --config --cache --yes --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__ui)
            opts="-h -V --help --version menu help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 2 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__ui__subcmd__help)
            opts="menu help"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__ui__subcmd__help__subcmd__help)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__ui__subcmd__help__subcmd__menu)
            opts=""
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 4 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
        muxe__subcmd__ui__subcmd__menu)
            opts="-h -V --theme --color-scheme --help --version"
            if [[ ${cur} == -* || ${COMP_CWORD} -eq 3 ]] ; then
                COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
                return 0
            fi
            case "${prev}" in
                --theme)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                --color-scheme)
                    COMPREPLY=($(compgen -f "${cur}"))
                    return 0
                    ;;
                *)
                    COMPREPLY=()
                    ;;
            esac
            COMPREPLY=( $(compgen -W "${opts}" -- "${cur}") )
            return 0
            ;;
    esac
}

if [[ "${BASH_VERSINFO[0]}" -eq 4 && "${BASH_VERSINFO[1]}" -ge 4 || "${BASH_VERSINFO[0]}" -gt 4 ]]; then
    complete -F _muxe -o nosort -o bashdefault -o default muxe
else
    complete -F _muxe -o bashdefault -o default muxe
fi
