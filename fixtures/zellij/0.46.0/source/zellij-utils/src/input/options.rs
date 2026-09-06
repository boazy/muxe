// Minimized from the exact pinned source: zellij-utils/src/input/options.rs

#[derive(Copy, Clone, Debug, PartialEq, Deserialize, Serialize, ValueEnum)]
pub enum OnForceClose {
    #[serde(alias = "quit")]
    Quit,
    #[serde(alias = "detach")]
    Detach,
}
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
pub enum NestedSessionHandling {
    #[serde(alias = "ask")]
    Ask,
    #[serde(alias = "fullscreen")]
    Fullscreen,
    #[serde(alias = "descend")]
    Descend,
    #[serde(alias = "never")]
    Never,
}
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PaneFrameStyle {
    Full,
    Titles,
    None,
}
#[derive(Clone, Default, Debug, PartialEq, Deserialize, Serialize, Args)]
/// Options that can be set either through the config file,
/// or cli flags - cli flags should take precedence over the config file
/// TODO: In order to correctly parse boolean flags, this is currently split
/// into Options and CliOptions, this could be a good canditate for a macro
pub struct Options {
    /// Allow plugins to use a more simplified layout
    /// that is compatible with more fonts (true or false)
    #[clap(long, value_parser)]
    #[serde(default)]
    pub simplified_ui: Option<bool>,
    /// Set the default theme
    #[clap(long, value_parser)]
    pub theme: Option<String>,
    /// Theme name to apply when the host terminal reports a dark color palette
    /// (CSI 2031 / DSR 997). Requires `theme_light` to also be set; if either
    /// is missing the static `theme` remains authoritative.
    #[clap(long, value_parser)]
    pub theme_dark: Option<String>,
    /// Theme name to apply when the host terminal reports a light color palette
    /// (CSI 2031 / DSR 997). Requires `theme_dark` to also be set; if either
    /// is missing the static `theme` remains authoritative.
    #[clap(long, value_parser)]
    pub theme_light: Option<String>,
    /// Pin the session to a dark or light appearance ("dark" or "light"),
    /// resolved before the first render and kept authoritative over ambient
    /// host terminal reports (CSI 2031 / DSR 997). When unset, the session
    /// follows the host terminal.
    #[clap(long, value_enum, hide_possible_values = true, value_parser)]
    pub explicit_theme_hue: Option<ThemeHue>,
    /// Set the default mode
    #[clap(long, value_enum, hide_possible_values = true, value_parser)]
    pub default_mode: Option<InputMode>,
    /// Set the default shell
    #[clap(long, value_parser)]
    pub default_shell: Option<PathBuf>,
    /// Set the default cwd
    #[clap(long, value_parser)]
    pub default_cwd: Option<PathBuf>,
    /// Set the default layout
    #[clap(long, value_parser)]
    pub default_layout: Option<PathBuf>,
    /// Set the layout_dir, defaults to
    /// subdirectory of config dir
    #[clap(long, value_parser)]
    pub layout_dir: Option<PathBuf>,
    /// Set the theme_dir, defaults to
    /// subdirectory of config dir
    #[clap(long, value_parser)]
    pub theme_dir: Option<PathBuf>,
    #[clap(long, value_parser)]
    #[serde(default)]
    /// Set the handling of mouse events (true or false)
    /// Can be temporarily bypassed by the [SHIFT] key
    pub mouse_mode: Option<bool>,
    #[clap(long, value_parser)]
    #[serde(default)]
    /// Set display of the pane frames (true or false)
    pub pane_frames: Option<bool>,
    #[clap(long, value_enum, hide_possible_values = true, value_parser)]
    #[serde(default)]
    pub pane_frame_style: Option<PaneFrameStyle>,
    #[clap(long, value_parser)]
    #[serde(default)]
    /// Mirror session when multiple users are connected (true or false)
    pub mirror_session: Option<bool>,
    /// Set behaviour on force close (quit or detach)
    #[clap(long, value_enum, hide_possible_values = true, value_parser)]
    pub on_force_close: Option<OnForceClose>,
    #[clap(long, value_parser)]
    pub scroll_buffer_size: Option<usize>,
    /// Switch to using a user supplied command for clipboard instead of OSC52
    #[clap(long, value_parser)]
    #[serde(default)]
    pub copy_command: Option<String>,
    /// OSC52 destination clipboard
    #[clap(
        long,
        value_enum,
        ignore_case = true,
        conflicts_with = "copy_command",
        value_parser
    )]
    #[serde(default)]
    pub copy_clipboard: Option<Clipboard>,
    /// Automatically copy when selecting text (true or false)
    #[clap(long, value_parser)]
    #[serde(default)]
    pub copy_on_select: Option<bool>,
    /// Enable OSC8 hyperlink output (true or false)
    #[clap(long, value_parser)]
    #[serde(default)]
    pub osc8_hyperlinks: Option<bool>,
    /// Explicit full path to open the scrollback editor (default is $EDITOR or $VISUAL)
    #[clap(long, value_parser)]
    pub scrollback_editor: Option<PathBuf>,
    /// The name of the session to create when starting Zellij
    #[clap(long, value_parser)]
    #[serde(default)]
    pub session_name: Option<String>,
    /// Whether to attach to a session specified in "session-name" if it exists
    #[clap(long, value_parser)]
    #[serde(default)]
    pub attach_to_session: Option<bool>,
    /// Whether to lay out panes in a predefined set of layouts whenever possible
    #[clap(long, value_parser)]
    #[serde(default)]
    pub auto_layout: Option<bool>,
    /// Whether sessions should be serialized to the HD so that they can be later resurrected,
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub session_serialization: Option<bool>,
    /// Whether pane viewports are serialized along with the session, default is false
    #[clap(long, value_parser)]
    #[serde(default)]
    pub serialize_pane_viewport: Option<bool>,
    /// Scrollback lines to serialize along with the pane viewport when serializing sessions, 0
    /// defaults to the scrollback size. If this number is higher than the scrollback size, it will
    /// also default to the scrollback size
    #[clap(long, value_parser)]
    #[serde(default)]
    pub scrollback_lines_to_serialize: Option<usize>,
    /// Whether to use ANSI styled underlines
    #[clap(long, value_parser)]
    #[serde(default)]
    pub styled_underlines: Option<bool>,
    /// The interval at which to serialize sessions for resurrection (in seconds)
    #[clap(long, value_parser)]
    pub serialization_interval: Option<u64>,
    /// If true, will disable writing session metadata to disk
    #[clap(long, value_parser)]
    pub disable_session_metadata: Option<bool>,
    /// Whether to enable support for the Kitty keyboard protocol (must also be supported by the
    /// host terminal), defaults to true if the terminal supports it
    #[clap(long, value_parser)]
    #[serde(default)]
    pub support_kitty_keyboard_protocol: Option<bool>,
    /// Whether to enable support for the Kitty graphics (image) protocol (must also be supported
    /// by the host terminal), defaults to true if the terminal supports it
    #[clap(long, value_parser)]
    #[serde(default)]
    pub support_kitty_graphics_protocol: Option<bool>,
    /// Whether to make sure a local web server is running when a new Zellij session starts.
    /// This web server will allow creating new sessions and attaching to existing ones that have
    /// opted in to being shared in the browser.
    ///
    /// Note: a local web server can still be manually started from within a Zellij session or from the CLI.
    /// If this is not desired, one can use a version of Zellij compiled without
    /// web_server_capability
    ///
    /// Possible values:
    /// - true
    /// - false
    /// Default: false
    #[clap(long, value_parser)]
    #[serde(default)]
    pub web_server: Option<bool>,
    /// Whether to allow new sessions to be shared through a local web server, assuming one is
    /// running (see the `web_server` option for more details).
    ///
    /// Note: if Zellij was compiled without web_server_capability, this option will be locked to
    /// "disabled"
    ///
    /// Possible values:
    /// - "on" (new sessions will allow web sharing through the local web server if it
    /// is online)
    /// - "off" (new sessions will not allow web sharing unless they explicitly opt-in to it)
    /// - "disabled" (new sessions will not allow web sharing and will not be able to opt-in to it)
    /// Default: "off"
    #[clap(long, value_parser)]
    #[serde(default)]
    pub web_sharing: Option<WebSharing>,
    /// Whether to stack panes when resizing beyond a certain size
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub stacked_resize: Option<bool>,
    #[clap(long, value_parser)]
    #[serde(default)]
    pub stacked_pane_list: Option<bool>,
    /// Whether to show startup tips when starting a new session
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub show_startup_tips: Option<bool>,
    /// Whether to show release notes on first run of a new version
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub show_release_notes: Option<bool>,
    /// Whether to enable mouse hover effects and pane grouping functionality
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub advanced_mouse_actions: Option<bool>,
    /// Whether Ctrl+ScrollWheel resizes panes
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub mouse_scroll_resize: Option<bool>,
    /// Whether scrolling a pane implicitly enters (and leaving the scroll implicitly exits) Scroll mode
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub scroll_mode_sync: Option<bool>,
    /// Whether to enable mouse hover visual effects (frame highlight and help text)
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub mouse_hover_effects: Option<bool>,
    /// Whether to show mouse hover help-text tips (resize help and group shortcuts)
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub mouse_hover_tips: Option<bool>,
    /// Whether to show visual bell indicators (pane/tab frame flash and [!] suffix)
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub visual_bell: Option<bool>,
    /// Whether to focus panes on mouse hover (true or false)
    /// default is false
    #[clap(long, value_parser)]
    #[serde(default)]
    pub focus_follows_mouse: Option<bool>,
    /// Whether clicking a pane to focus it also sends the click into the pane (true or false)
    /// default is false
    #[clap(long, value_parser)]
    #[serde(default)]
    pub mouse_click_through: Option<bool>,
    /// Whether triple-clicking inside shell-marked (OSC 133) command output selects the command
    /// and its output rather than the logical line
    /// default is true
    #[clap(long, value_parser)]
    #[serde(default)]
    pub osc133_command_selection: Option<bool>,
    /// Characters that terminate a word when double-clicking to select it, in addition to
    /// whitespace (which is always a separator)
    /// default is "[]{}<>()"
    #[clap(long, value_parser)]
    #[serde(default)]
    pub word_separators: Option<String>,
    #[clap(long, value_parser)]
    #[serde(default)]
    pub host_notification_protocol: Option<HostNotificationProtocol>,
    pub web_server_ip: Option<IpAddr>,
    pub web_server_port: Option<u16>,
    pub web_server_cert: Option<PathBuf>,
    pub web_server_key: Option<PathBuf>,
    pub enforce_https_for_localhost: Option<bool>,
    /// A command to run after the discovery of running commands when serializing, for the purpose
    /// of manipulating the command (eg. with a regex) before it gets serialized
    #[clap(long, value_parser)]
    pub post_command_discovery_hook: Option<String>,
    /// Number of async worker tasks to spawn per active client.
    ///
    /// Allocating few tasks may result in resource contention and lags. Small values (around 4)
    /// should typically work best. Set to 0 to use the number of (physical) CPU cores.
    /// NOTE: This only applies to web clients at the moment.
    #[clap(long)]
    pub client_async_worker_tasks: Option<usize>,
    /// How to handle a nested Zellij session detected inside a pane
    /// (ask, fullscreen, descend, never)
    #[clap(long, value_enum, hide_possible_values = true, value_parser)]
    #[serde(default)]
    pub nested_session_handling: Option<NestedSessionHandling>,
    #[clap(long, value_parser)]
    #[serde(default)]
    pub dangerously_enable_paste_buffer_read: Option<bool>,
}
#[derive(ValueEnum, Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
pub enum Clipboard {
    #[serde(alias = "system")]
    System,
    #[serde(alias = "primary")]
    Primary,
}
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
pub enum HostNotificationProtocol {
    #[serde(alias = "auto")]
    Auto,
    #[serde(alias = "osc9")]
    Osc9,
    #[serde(alias = "osc99")]
    Osc99,
    #[serde(alias = "bell")]
    Bell,
    #[serde(alias = "off")]
    Off,
}
