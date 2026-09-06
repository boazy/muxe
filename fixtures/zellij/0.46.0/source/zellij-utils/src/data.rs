// Minimized from the exact pinned source: zellij-utils/src/data.rs

pub type ClientId = u16;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnblockCondition {
    /// Unblock only when exit status is 0 (success)
    OnExitSuccess,
    /// Unblock only when exit status is non-zero (failure)
    OnExitFailure,
    /// Unblock on any exit (success or failure)
    OnAnyExit,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandOrPlugin {
    Command(RunCommandAction),
    Plugin(RunPluginOrAlias),
    File(FileToOpen),
}
#[derive(Debug, Clone, Eq, Serialize, Deserialize, PartialOrd, Ord)]
pub struct KeyWithModifier {
    pub bare_key: BareKey,
    pub key_modifiers: BTreeSet<KeyModifier>,
}
#[derive(
    Eq,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord
)]
pub enum BareKey {
    PageDown,
    PageUp,
    Left,
    Down,
    Up,
    Right,
    Home,
    End,
    Backspace,
    Delete,
    Insert,
    F(u8),
    Char(char),
    Tab,
    Esc,
    Enter,
    CapsLock,
    ScrollLock,
    NumLock,
    PrintScreen,
    Pause,
    Menu,
}
#[derive(
    Eq,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord,
    Display,
)]
pub enum KeyModifier {
    Ctrl,
    Alt,
    Shift,
    Super,
}
#[derive(
    Eq,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord
)]
pub enum Direction {
    Left,
    Right,
    Up,
    Down,
}
/// Resize operation to perform.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub enum Resize {
    Increase,
    Decrease,
}
/// Container type that fully describes resize operations.
///
/// This is best thought of as follows:
///
/// - `resize` commands how the total *area* of the pane will change as part of this resize
///   operation.
/// - `direction` has two meanings:
///     - `None` means to resize all borders equally
///     - Anything else means to move the named border to achieve the change in area
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct ResizeStrategy {
    /// Whether to increase or resize total area
    pub resize: Resize,
    /// With which border, if any, to change area
    pub direction: Option<Direction>,
    /// If set to true (default), increasing resizes towards a viewport border will be inverted.
    /// I.e. a scenario like this ("increase right"):
    ///
    /// ```text
    /// +---+---+
    /// |   | X |->
    /// +---+---+
    /// ```
    ///
    /// turns into this ("decrease left"):
    ///
    /// ```text
    /// +---+---+
    /// |   |-> |
    /// +---+---+
    /// ```
    pub invert_on_boundaries: bool,
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Mouse {
    ScrollUp(usize),
    ScrollDown(usize),
    ScrollLeft(usize),
    ScrollRight(usize),
    LeftClick(isize, usize),
    RightClick(isize, usize),
    Hold(isize, usize),
    Release(isize, usize),
    Hover(isize, usize),
}
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileMetadata {
    pub is_dir: bool,
    pub is_file: bool,
    pub is_symlink: bool,
    pub len: u64,
}
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StyledText {
    pub text: String,
    pub indices: Vec<Vec<usize>>,
}
/// These events can be subscribed to with subscribe method exported by `zellij-tile`.
/// Once subscribed to, they will trigger the `update` method of the `ZellijPlugin` trait.
#[derive(Debug, Clone, PartialEq, EnumDiscriminants, Display, Serialize, Deserialize)]
#[strum_discriminants(derive(EnumString, Hash, Serialize, Deserialize))]
#[strum_discriminants(name(EventType))]
#[non_exhaustive]
pub enum Event {
    ModeUpdate(ModeInfo),
    TabUpdate(Vec<TabInfo>),
    PaneUpdate(PaneManifest),
    /// A key was pressed while the user is focused on this plugin's pane
    Key(KeyWithModifier),
    /// A mouse event happened while the user is focused on this plugin's pane
    Mouse(Mouse),
    /// A timer expired set by the `set_timeout` method exported by `zellij-tile`.
    Timer(f64),
    /// Text was copied to the clipboard anywhere in the app
    CopyToClipboard(CopyDestination),
    /// Failed to copy text to clipboard anywhere in the app
    SystemClipboardFailure,
    /// Input was received anywhere in the app
    InputReceived,
    /// This plugin became visible or invisible
    Visible(bool),
    /// A message from one of the plugin's workers
    CustomMessage(String, String),
    /// A file was created somewhere in the Zellij CWD folder
    FileSystemCreate(Vec<(PathBuf, Option<FileMetadata>)>),
    /// A file was accessed somewhere in the Zellij CWD folder
    FileSystemRead(Vec<(PathBuf, Option<FileMetadata>)>),
    /// A file was modified somewhere in the Zellij CWD folder
    FileSystemUpdate(Vec<(PathBuf, Option<FileMetadata>)>),
    /// A file was deleted somewhere in the Zellij CWD folder
    FileSystemDelete(Vec<(PathBuf, Option<FileMetadata>)>),
    /// A Result of plugin permission request
    PermissionRequestResult(PermissionStatus),
    SessionUpdate(Vec<SessionInfo>, Vec<(String, Duration)>),
    RunCommandResult(Option<i32>, Vec<u8>, Vec<u8>, BTreeMap<String, String>),
    WebRequestResult(u16, BTreeMap<String, String>, Vec<u8>, BTreeMap<String, String>),
    CommandPaneOpened(u32, Context),
    CommandPaneExited(u32, Option<i32>, Context),
    PaneClosed(PaneId),
    EditPaneOpened(u32, Context),
    EditPaneExited(u32, Option<i32>, Context),
    CommandPaneReRun(u32, Context),
    FailedToWriteConfigToDisk(Option<String>),
    ListClients(Vec<ClientInfo>),
    HostFolderChanged(PathBuf),
    FailedToChangeHostFolder(Option<String>),
    PastedText(String),
    ConfigWasWrittenToDisk,
    WebServerStatus(WebServerStatus),
    FailedToStartWebServer(String),
    BeforeClose,
    InterceptedKeyPress(KeyWithModifier),
    /// An action was performed by the user (requires InterceptInput permission)
    UserAction(Action, ClientId, Option<u32>, Option<ClientId>),
    PaneRenderReport(HashMap<PaneId, PaneContents>),
    ActionComplete(Action, Option<PaneId>, BTreeMap<String, String>),
    CwdChanged(PaneId, PathBuf, Vec<ClientId>),
    CommandChanged(PaneId, Vec<String>, bool, Vec<ClientId>),
    AvailableLayoutInfo(Vec<LayoutInfo>, Vec<LayoutWithError>),
    PluginConfigurationChanged(BTreeMap<String, String>),
    HighlightClicked {
        pane_id: PaneId,
        pattern: String,
        matched_string: String,
        context: BTreeMap<String, String>,
    },
    /// Initial keybindings sent once on plugin load and on reconfiguration.
    /// Plugins that subscribe to this event signal they cache keybindings
    /// and can handle lightweight ModeUpdate events without keybindings.
    InitialKeybinds(KeybindsVec),
    /// The host terminal indicated its color palette theme mode (CSI 2031 / DSR 997).
    HostTerminalThemeChanged(HostTerminalThemeMode),
    SoftKeyboardVisibilityChanged(bool),
    HintText(BTreeMap<usize, StyledText>),
    ActivePaneScroll(Option<(usize, usize)>),
}
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HostTerminalThemeMode {
    Dark,
    Light,
}
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    EnumDiscriminants,
    Display,
    Serialize,
    Deserialize
)]
pub enum WebServerStatus {
    Online(String),
    Offline,
    DifferentVersion(String),
}
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Clone,
    EnumDiscriminants,
    Display,
    Serialize,
    Deserialize,
    PartialOrd,
    Ord,
)]
#[strum_discriminants(
    derive(EnumString, Hash, Serialize, Deserialize, Display, PartialOrd, Ord)
)]
#[strum_discriminants(name(PermissionType))]
#[non_exhaustive]
pub enum Permission {
    ReadApplicationState,
    ChangeApplicationState,
    OpenFiles,
    RunCommands,
    OpenTerminalsOrPlugins,
    WriteToStdin,
    WebAccess,
    ReadCliPipes,
    MessageAndLaunchOtherPlugins,
    Reconfigure,
    FullHdAccess,
    StartWebServer,
    InterceptInput,
    ReadPaneContents,
    RunActionsAsUser,
    WriteToClipboard,
    ReadSessionEnvironmentVariables,
}
#[derive(Debug, Clone)]
pub struct PluginPermission {
    pub name: String,
    pub permissions: Vec<PermissionType>,
}
/// Describes the different input modes, which change the way that keystrokes will be interpreted.
#[derive(
    Debug,
    PartialEq,
    Eq,
    Hash,
    Copy,
    Clone,
    EnumIter,
    Serialize,
    Deserialize,
    ValueEnum,
    PartialOrd,
    Ord,
)]
pub enum InputMode {
    /// In `Normal` mode, input is always written to the terminal, except for the shortcuts leading
    /// to other modes
    #[serde(alias = "normal")]
    Normal,
    /// In `Locked` mode, input is always written to the terminal and all shortcuts are disabled
    /// except the one leading back to normal mode
    #[serde(alias = "locked")]
    Locked,
    /// `Resize` mode allows resizing the different existing panes.
    #[serde(alias = "resize")]
    Resize,
    /// `Pane` mode allows creating and closing panes, as well as moving between them.
    #[serde(alias = "pane")]
    Pane,
    /// `Tab` mode allows creating and closing tabs, as well as moving between them.
    #[serde(alias = "tab")]
    Tab,
    /// `Scroll` mode allows scrolling up and down within a pane.
    #[serde(alias = "scroll")]
    Scroll,
    /// `EnterSearch` mode allows for typing in the needle for a search in the scroll buffer of a pane.
    #[serde(alias = "entersearch")]
    EnterSearch,
    /// `Search` mode allows for searching a term in a pane (superset of `Scroll`).
    #[serde(alias = "search")]
    Search,
    /// `RenameTab` mode allows assigning a new name to a tab.
    #[serde(alias = "renametab")]
    RenameTab,
    /// `RenamePane` mode allows assigning a new name to a pane.
    #[serde(alias = "renamepane")]
    RenamePane,
    /// `Session` mode allows detaching sessions
    #[serde(alias = "session")]
    Session,
    /// `Move` mode allows moving the different existing panes within a tab
    #[serde(alias = "move")]
    Move,
    /// `Prompt` mode allows interacting with active prompts.
    #[serde(alias = "prompt")]
    Prompt,
    /// `Tmux` mode allows for basic tmux keybindings functionality
    #[serde(alias = "tmux")]
    Tmux,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, ValueEnum)]
pub enum ThemeHue {
    #[serde(alias = "light")]
    Light,
    #[serde(alias = "dark")]
    Dark,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PaletteColor {
    Rgb((u8, u8, u8)),
    EightBit(u8),
}
/// Priority layer for plugin-supplied regex highlights.
/// Higher-priority layers take visual precedence over lower ones
/// when highlights overlap.  Built-in highlights (mouse selection,
/// search results) always take precedence over all plugin layers.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize
)]
pub enum HighlightLayer {
    Hint,
    Tool,
    ActionFeedback,
}
/// Style for a plugin-supplied regex highlight.
/// Theme-based variants reference `style.colors.text_unselected.*`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighlightStyle {
    None,
    Emphasis0,
    Emphasis1,
    Emphasis2,
    Emphasis3,
    BackgroundEmphasis0,
    BackgroundEmphasis1,
    BackgroundEmphasis2,
    BackgroundEmphasis3,
    CustomRgb { fg: Option<(u8, u8, u8)>, bg: Option<(u8, u8, u8)> },
    CustomIndex { fg: Option<u8>, bg: Option<u8> },
}
/// One pattern + style pair sent by a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegexHighlight {
    pub pattern: String,
    pub style: HighlightStyle,
    pub layer: HighlightLayer,
    pub context: BTreeMap<String, String>,
    pub on_hover: bool,
    pub bold: bool,
    pub italic: bool,
    pub underline: bool,
    pub tooltip_text: Option<String>,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PaletteSource {
    Default,
    Xresources,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct Palette {
    pub source: PaletteSource,
    pub theme_hue: ThemeHue,
    pub fg: PaletteColor,
    pub bg: PaletteColor,
    pub black: PaletteColor,
    pub red: PaletteColor,
    pub green: PaletteColor,
    pub yellow: PaletteColor,
    pub blue: PaletteColor,
    pub magenta: PaletteColor,
    pub cyan: PaletteColor,
    pub white: PaletteColor,
    pub orange: PaletteColor,
    pub gray: PaletteColor,
    pub purple: PaletteColor,
    pub gold: PaletteColor,
    pub silver: PaletteColor,
    pub pink: PaletteColor,
    pub brown: PaletteColor,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Style {
    pub colors: Styling,
    pub rounded_corners: bool,
    pub hide_session_name: bool,
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub enum Coloration {
    NoStyling,
    Styled(StyleDeclaration),
}
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct Styling {
    pub text_unselected: StyleDeclaration,
    pub text_selected: StyleDeclaration,
    pub ribbon_unselected: StyleDeclaration,
    pub ribbon_selected: StyleDeclaration,
    pub table_title: StyleDeclaration,
    pub table_cell_unselected: StyleDeclaration,
    pub table_cell_selected: StyleDeclaration,
    pub list_unselected: StyleDeclaration,
    pub list_selected: StyleDeclaration,
    pub frame_unselected: Option<StyleDeclaration>,
    pub frame_selected: StyleDeclaration,
    pub frame_highlight: StyleDeclaration,
    pub exit_code_success: StyleDeclaration,
    pub exit_code_error: StyleDeclaration,
    pub multiplayer_user_colors: MultiplayerColors,
}
#[derive(Debug, Copy, Default, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct StyleDeclaration {
    pub base: PaletteColor,
    pub background: PaletteColor,
    pub emphasis_0: PaletteColor,
    pub emphasis_1: PaletteColor,
    pub emphasis_2: PaletteColor,
    pub emphasis_3: PaletteColor,
}
#[derive(Debug, Copy, Default, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MultiplayerColors {
    pub player_1: PaletteColor,
    pub player_2: PaletteColor,
    pub player_3: PaletteColor,
    pub player_4: PaletteColor,
    pub player_5: PaletteColor,
    pub player_6: PaletteColor,
    pub player_7: PaletteColor,
    pub player_8: PaletteColor,
    pub player_9: PaletteColor,
    pub player_10: PaletteColor,
}
pub type KeybindsVec = Vec<(InputMode, Vec<(KeyWithModifier, Vec<Action>)>)>;
/// Provides information helpful in rendering the Zellij controls for UI bars
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeInfo {
    pub mode: InputMode,
    pub base_mode: Option<InputMode>,
    pub keybinds: KeybindsVec,
    pub style: Style,
    pub capabilities: PluginCapabilities,
    pub session_name: Option<String>,
    pub editor: Option<PathBuf>,
    pub shell: Option<PathBuf>,
    pub web_clients_allowed: Option<bool>,
    pub web_sharing: Option<WebSharing>,
    pub currently_marking_pane_group: Option<bool>,
    pub is_web_client: Option<bool>,
    pub web_server_ip: Option<IpAddr>,
    pub web_server_port: Option<u16>,
    pub web_server_capability: Option<bool>,
    pub pane_frame_style: Option<PaneFrameStyle>,
    pub session_dimmed: Option<bool>,
    pub session_ancestry: Vec<String>,
    pub host_fullscreen: Option<bool>,
    pub nested_ascend_keys: Vec<KeyWithModifier>,
    pub session_ascended: Option<bool>,
    pub nested_descend_keys: Vec<KeyWithModifier>,
}
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct SessionInfo {
    pub name: String,
    pub tabs: Vec<TabInfo>,
    pub panes: PaneManifest,
    pub connected_clients: usize,
    pub is_current_session: bool,
    pub available_layouts: Vec<LayoutInfo>,
    pub plugins: BTreeMap<u32, PluginInfo>,
    pub web_clients_allowed: bool,
    pub web_client_count: usize,
    pub tab_history: BTreeMap<ClientId, Vec<usize>>,
    pub pane_history: BTreeMap<ClientId, Vec<PaneId>>,
    pub creation_time: Duration,
}
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PluginInfo {
    pub location: String,
    pub configuration: BTreeMap<String, String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum LayoutInfo {
    BuiltIn(String),
    File(String, LayoutMetadata),
    Url(String),
    Stringified(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LayoutWithError {
    pub layout_name: String,
    pub error: LayoutParsingError,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum LayoutParsingError {
    KdlError { kdl_error: KdlError, file_name: String, source_code: String },
    SyntaxError,
}
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct LayoutMetadata {
    pub tabs: Vec<TabMetadata>,
    pub creation_time: String,
    pub update_time: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct TabMetadata {
    pub panes: Vec<PaneMetadata>,
    pub name: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PaneMetadata {
    pub name: Option<String>,
    pub is_plugin: bool,
    pub is_builtin_plugin: bool,
}
/// Contains all the information for a currently opened tab.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct TabInfo {
    /// The Tab's 0 indexed position
    pub position: usize,
    /// The name of the tab as it appears in the UI (if there's enough room for it)
    pub name: String,
    /// Whether this tab is focused
    pub active: bool,
    /// The number of suppressed panes this tab has
    pub panes_to_hide: usize,
    /// Whether there's one pane taking up the whole display area on this tab
    pub is_fullscreen_active: bool,
    /// Whether input sent to this tab will be synced to all panes in it
    pub is_sync_panes_active: bool,
    pub are_floating_panes_visible: bool,
    pub other_focused_clients: Vec<ClientId>,
    pub active_swap_layout_name: Option<String>,
    /// Whether the user manually changed the layout, moving out of the swap layout scheme
    pub is_swap_layout_dirty: bool,
    /// Row count in the viewport (including all non-ui panes, eg. will exclude the status bar)
    pub viewport_rows: usize,
    /// Column count in the viewport (including all non-ui panes, eg. will exclude the status bar)
    pub viewport_columns: usize,
    /// Row count in the display area (including all panes, will typically be larger than the
    /// viewport)
    pub display_area_rows: usize,
    /// Column count in the display area (including all panes, will typically be larger than the
    /// viewport)
    pub display_area_columns: usize,
    /// The number of selectable (eg. not the UI bars) tiled panes currently in this tab
    pub selectable_tiled_panes_count: usize,
    /// The number of selectable (eg. not the UI bars) floating panes currently in this tab
    pub selectable_floating_panes_count: usize,
    /// The stable identifier for this tab
    pub tab_id: usize,
    /// Whether this tab has an active (persistent) bell notification
    pub has_bell_notification: bool,
    /// Whether this tab is currently flashing its bell (transient 400ms state)
    pub is_flashing_bell: bool,
}
/// The `PaneManifest` contains a dictionary of panes, indexed by the tab position (0 indexed).
/// Panes include all panes in the relevant tab, including `tiled` panes, `floating` panes and
/// `suppressed` panes.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PaneManifest {
    pub panes: HashMap<usize, Vec<PaneInfo>>,
}
/// Contains all the information for a currently open pane
///
/// # Difference between coordinates/size and content coordinates/size
///
/// The pane basic coordinates and size (eg. `pane_x` or `pane_columns`) are the entire space taken
/// up by this pane - including its frame and title if it has a border.
///
/// The pane content coordinates and size (eg. `pane_content_x` or `pane_content_columns`)
/// represent the area taken by the pane's content, excluding its frame and title if it has a
/// border.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct PaneInfo {
    /// The id of the pane, unique to all panes of this kind (eg. id in terminals or id in panes)
    pub id: u32,
    /// Whether this pane is a plugin (`true`) or a terminal (`false`), used along with `id` can represent a unique pane ID across
    /// the running session
    pub is_plugin: bool,
    /// Whether the pane is focused in its layer (tiled or floating)
    pub is_focused: bool,
    pub is_fullscreen: bool,
    /// Whether a pane is floating or tiled (embedded)
    pub is_floating: bool,
    /// Whether a pane is suppressed - suppressed panes are not visible to the user, but still run
    /// in the background
    pub is_suppressed: bool,
    /// The full title of the pane as it appears in the UI (if there is room for it)
    pub title: String,
    /// Whether a pane exited or not, note that most panes close themselves before setting this
    /// flag, so this is only relevant to command panes
    pub exited: bool,
    /// The exit status of a pane if it did exit and is still in the UI
    pub exit_status: Option<i32>,
    /// A "held" pane is a paused pane that is waiting for user input (eg. a command pane that
    /// exited and is waiting to be re-run or closed)
    pub is_held: bool,
    pub pane_x: usize,
    pub pane_content_x: usize,
    pub pane_y: usize,
    pub pane_content_y: usize,
    pub pane_rows: usize,
    pub pane_content_rows: usize,
    pub pane_columns: usize,
    pub pane_content_columns: usize,
    /// The coordinates of the cursor - if this pane is focused - relative to the pane's
    /// coordinates
    pub cursor_coordinates_in_pane: Option<(usize, usize)>,
    /// If this is a command pane, this will show the stringified version of the command and its
    /// arguments
    pub terminal_command: Option<String>,
    /// The URL from which this plugin was loaded (eg. `zellij:strider` for the built-in `strider`
    /// plugin or `file:/path/to/my/plugin.wasm` for a local plugin)
    pub plugin_url: Option<String>,
    /// Unselectable panes are often used for UI elements that do not have direct user interaction
    /// (eg. the default `status-bar` or `tab-bar`).
    pub is_selectable: bool,
    /// Grouped panes (usually through an explicit user action) that are staged for a bulk action
    /// the index is kept track of in order to preserve the pane group order
    pub index_in_pane_group: BTreeMap<ClientId, usize>,
    /// The default foreground color of this pane, if set (e.g. "#00e000")
    pub default_fg: Option<String>,
    /// The default background color of this pane, if set (e.g. "#001a3a")
    pub default_bg: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct PaneListEntry {
    #[serde(flatten)]
    pub pane_info: PaneInfo,
    pub tab_id: usize,
    pub tab_position: usize,
    pub tab_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pane_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pane_cwd: Option<String>,
}
pub type ListPanesResponse = Vec<PaneListEntry>;
pub type ListTabsResponse = Vec<TabInfo>;
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ClientInfo {
    pub client_id: ClientId,
    pub pane_id: PaneId,
    pub running_command: String,
    pub is_current_client: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PaneRenderReport {
    pub all_pane_contents: HashMap<ClientId, HashMap<PaneId, PaneContents>>,
    pub all_pane_contents_with_ansi: HashMap<ClientId, HashMap<PaneId, PaneContents>>,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PaneContents {
    pub lines_above_viewport: Vec<String>,
    pub lines_below_viewport: Vec<String>,
    pub viewport: Vec<String>,
    pub selected_text: Option<SelectedText>,
    pub cursor: Option<(usize, usize)>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaneScrollbackResponse {
    Ok(PaneContents),
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GetPanePidResponse {
    Ok(i32),
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GetPaneRunningCommandResponse {
    Ok(Vec<String>),
    Err(String),
}
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionListSnapshot {
    pub live_sessions: Vec<SessionInfo>,
    pub resurrectable_sessions: Vec<(String, Duration)>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GetSessionListResponse {
    Ok(SessionListSnapshot),
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KillSessionsResponse {
    Ok,
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeleteDeadSessionResponse {
    Ok,
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DeleteAllDeadSessionsResponse {
    Ok,
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GetPaneCwdResponse {
    Ok(PathBuf),
    Err(String),
}
#[derive(Debug, Clone, PartialEq)]
pub enum GetFocusedPaneInfoResponse {
    Ok { tab_index: usize, pane_id: PaneId },
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaveLayoutResponse {
    Ok(()),
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeleteLayoutResponse {
    Ok(()),
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RenameLayoutResponse {
    Ok(()),
    Err(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EditLayoutResponse {
    Ok(()),
    Err(String),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedText {
    pub start: Position,
    pub end: Position,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct PluginIds {
    pub plugin_id: u32,
    pub zellij_pid: u32,
    pub initial_cwd: PathBuf,
    pub client_id: ClientId,
}
/// Tag used to identify the plugin in layout and config kdl files
#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Deserialize,
    Serialize,
    PartialOrd,
    Ord
)]
pub struct PluginTag(String);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct PluginCapabilities {
    pub arrow_fonts: bool,
}
/// Represents a Clipboard type
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum CopyDestination {
    Command,
    Primary,
    System,
}
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum PermissionStatus {
    Granted,
    Denied,
}
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileToOpen {
    pub path: PathBuf,
    pub line_number: Option<usize>,
    pub cwd: Option<PathBuf>,
}
#[derive(Debug, Default, Clone)]
pub struct CommandToRun {
    pub path: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
}
#[derive(Debug, Default, Clone)]
pub struct MessageToPlugin {
    pub plugin_url: Option<String>,
    pub destination_plugin_id: Option<u32>,
    pub plugin_config: BTreeMap<String, String>,
    pub message_name: String,
    pub message_payload: Option<String>,
    pub message_args: BTreeMap<String, String>,
    /// these will only be used in case we need to launch a new plugin to send this message to,
    /// since none are running
    pub new_plugin_args: Option<NewPluginArgs>,
    pub floating_pane_coordinates: Option<FloatingPaneCoordinates>,
}
#[derive(Debug, Default, Clone)]
pub struct NewPluginArgs {
    pub should_float: Option<bool>,
    pub pane_id_to_replace: Option<PaneId>,
    pub pane_title: Option<String>,
    pub cwd: Option<PathBuf>,
    pub skip_cache: bool,
    pub should_focus: Option<bool>,
}
#[derive(
    Debug,
    Clone,
    Copy,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord
)]
pub enum PaneId {
    Terminal(u32),
    Plugin(u32),
}
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectToSession {
    pub name: Option<String>,
    pub tab_position: Option<usize>,
    pub pane_id: Option<(u32, bool)>,
    pub layout: Option<LayoutInfo>,
    pub cwd: Option<PathBuf>,
}
#[derive(Debug, Default, Clone)]
pub struct PluginMessage {
    pub name: String,
    pub payload: String,
    pub worker_name: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HttpVerb {
    Get,
    Post,
    Put,
    Delete,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PipeSource {
    Cli(String),
    Plugin(u32),
    Keybind,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PipeMessage {
    pub source: PipeSource,
    pub name: String,
    pub payload: Option<String>,
    pub args: BTreeMap<String, String>,
    pub is_private: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Default)]
pub struct FloatingPaneCoordinates {
    pub x: Option<PercentOrFixed>,
    pub y: Option<PercentOrFixed>,
    pub width: Option<PercentOrFixed>,
    pub height: Option<PercentOrFixed>,
    pub pinned: Option<bool>,
    pub borderless: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OriginatingPlugin {
    pub plugin_id: u32,
    pub client_id: ClientId,
    pub context: Context,
}
#[derive(ValueEnum, Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSharing {
    #[serde(alias = "on")]
    On,
    #[serde(alias = "off")]
    Off,
    #[serde(alias = "disabled")]
    Disabled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NewPanePlacement {
    NoPreference { borderless: Option<bool> },
    Tiled { direction: Option<Direction>, borderless: Option<bool> },
    Floating(Option<FloatingPaneCoordinates>),
    InPlace {
        pane_id_to_replace: Option<PaneId>,
        close_replaced_pane: bool,
        borderless: Option<bool>,
    },
    Stacked { pane_id_to_stack_under: Option<PaneId>, borderless: Option<bool> },
}
type Context = BTreeMap<String, String>;
#[derive(Debug, Clone, EnumDiscriminants, Display)]
#[strum_discriminants(derive(EnumString, Hash, Serialize, Deserialize))]
#[strum_discriminants(name(CommandType))]
pub enum PluginCommand {
    Subscribe(HashSet<EventType>),
    Unsubscribe(HashSet<EventType>),
    SetSelectable(bool),
    ShowCursor(Option<(usize, usize)>),
    GetPluginIds,
    GetZellijVersion,
    OpenFile(FileToOpen, Context),
    OpenFileFloating(FileToOpen, Option<FloatingPaneCoordinates>, Context),
    OpenTerminal(FileToOpen),
    OpenTerminalFloating(FileToOpen, Option<FloatingPaneCoordinates>),
    OpenCommandPane(CommandToRun, Context),
    OpenCommandPaneFloating(CommandToRun, Option<FloatingPaneCoordinates>, Context),
    SwitchTabTo(u32),
    SetTimeout(f64),
    ExecCmd(Vec<String>),
    PostMessageTo(PluginMessage),
    PostMessageToPlugin(PluginMessage),
    HideSelf,
    ShowSelf(bool),
    SwitchToMode(InputMode),
    NewTabsWithLayout(String),
    NewTab { name: Option<String>, cwd: Option<String> },
    NewTabUnfocused { name: Option<String>, cwd: Option<String> },
    NewTiledPaneInTab { tab_position: usize },
    ToggleFloatingPanes { tab_id: Option<u64> },
    NewPane,
    GoToNextTab,
    GoToPreviousTab,
    Resize(Resize),
    ResizeWithDirection(ResizeStrategy),
    FocusNextPane,
    FocusPreviousPane,
    FocusLastPane,
    MoveFocus(Direction),
    MoveFocusOrTab(Direction),
    Detach,
    EditScrollback,
    Write(Vec<u8>),
    WriteChars(String),
    ToggleTab,
    MovePane,
    MovePaneWithDirection(Direction),
    ClearScreen,
    ScrollUp,
    ScrollDown,
    ScrollToTop,
    ScrollToBottom,
    PageScrollUp,
    PageScrollDown,
    ToggleFocusFullscreen,
    ToggleFocusNoUiFullscreen,
    TogglePaneFrames,
    SetPaneFrameStyle(PaneFrameStyle),
    TogglePaneEmbedOrEject,
    UndoRenamePane,
    CloseFocus,
    ToggleActiveTabSync,
    CloseFocusedTab,
    UndoRenameTab,
    QuitZellij,
    PreviousSwapLayout,
    NextSwapLayout,
    GoToTabName(String),
    FocusOrCreateTab(String),
    GoToTab(u32),
    StartOrReloadPlugin(String),
    CloseTerminalPane(u32),
    ClosePluginPane(u32),
    FocusTerminalPane(u32, bool, bool),
    FocusPluginPane(u32, bool, bool),
    RenameTerminalPane(u32, String),
    RenamePluginPane(u32, String),
    RenameTab(u32, String),
    ReportPanic(String),
    RequestPluginPermissions(Vec<PermissionType>),
    SwitchSession(ConnectToSession),
    DeleteDeadSession(String),
    DeleteAllDeadSessions,
    OpenTerminalInPlace(FileToOpen),
    OpenFileInPlace(FileToOpen, Context),
    OpenCommandPaneInPlace(CommandToRun, Context),
    RunCommand(Vec<String>, BTreeMap<String, String>, PathBuf, BTreeMap<String, String>),
    WebRequest(
        String,
        HttpVerb,
        BTreeMap<String, String>,
        Vec<u8>,
        BTreeMap<String, String>,
    ),
    RenameSession(String),
    UnblockCliPipeInput(String),
    BlockCliPipeInput(String),
    CliPipeOutput(String, String),
    MessageToPlugin(MessageToPlugin),
    DisconnectOtherClients,
    KillSessions(Vec<String>),
    ScanHostFolder(PathBuf),
    WatchFilesystem,
    DumpSessionLayout { tab_index: Option<usize> },
    CloseSelf,
    NewTabsWithLayoutInfo(LayoutInfo),
    Reconfigure(String, bool),
    HidePaneWithId(PaneId),
    ShowPaneWithId(PaneId, bool, bool),
    OpenCommandPaneBackground(CommandToRun, Context),
    RerunCommandPane(u32),
    ResizePaneIdWithDirection(ResizeStrategy, PaneId),
    EditScrollbackForPaneWithId(PaneId),
    GetPaneScrollback { pane_id: PaneId, get_full_scrollback: bool },
    WriteToPaneId(Vec<u8>, PaneId),
    WriteCharsToPaneId(String, PaneId),
    SendSigintToPaneId(PaneId),
    SendSigkillToPaneId(PaneId),
    GetPanePid { pane_id: PaneId },
    GetPaneRunningCommand { pane_id: PaneId },
    GetPaneCwd { pane_id: PaneId },
    MovePaneWithPaneId(PaneId),
    MovePaneWithPaneIdInDirection(PaneId, Direction),
    ClearScreenForPaneId(PaneId),
    ScrollUpInPaneId(PaneId),
    ScrollDownInPaneId(PaneId),
    ScrollToTopInPaneId(PaneId),
    ScrollToBottomInPaneId(PaneId),
    PageScrollUpInPaneId(PaneId),
    PageScrollDownInPaneId(PaneId),
    TogglePaneIdFullscreen(PaneId),
    TogglePaneEmbedOrEjectForPaneId(PaneId),
    CloseTabWithIndex(usize),
    BreakPanesToNewTab(Vec<PaneId>, Option<String>, bool),
    BreakPanesToTabWithIndex(Vec<PaneId>, usize, bool),
    SwitchTabToId(u64),
    GoToTabWithId(u64),
    CloseTabWithId(u64),
    RenameTabWithId(u64, String),
    BreakPanesToTabWithId(Vec<PaneId>, u64, bool),
    ReloadPlugin(u32),
    LoadNewPlugin {
        url: String,
        config: BTreeMap<String, String>,
        load_in_background: bool,
        skip_plugin_cache: bool,
    },
    RebindKeys {
        keys_to_rebind: Vec<(InputMode, KeyWithModifier, Vec<Action>)>,
        keys_to_unbind: Vec<(InputMode, KeyWithModifier)>,
        write_config_to_disk: bool,
    },
    ListClients,
    ChangeHostFolder(PathBuf),
    SetFloatingPanePinned(PaneId, bool),
    StackPanes(Vec<PaneId>),
    ChangeFloatingPanesCoordinates(Vec<(PaneId, FloatingPaneCoordinates)>),
    TogglePaneBorderless(PaneId),
    SetPaneBorderless(PaneId, bool),
    OpenCommandPaneNearPlugin(CommandToRun, Context),
    OpenTerminalNearPlugin(FileToOpen),
    OpenTerminalFloatingNearPlugin(FileToOpen, Option<FloatingPaneCoordinates>),
    OpenTerminalInPlaceOfPlugin(FileToOpen, bool),
    OpenCommandPaneFloatingNearPlugin(
        CommandToRun,
        Option<FloatingPaneCoordinates>,
        Context,
    ),
    OpenCommandPaneInPlaceOfPlugin(CommandToRun, bool, Context),
    OpenFileNearPlugin(FileToOpen, Context),
    OpenFileFloatingNearPlugin(FileToOpen, Option<FloatingPaneCoordinates>, Context),
    StartWebServer,
    StopWebServer,
    ShareCurrentSession,
    StopSharingCurrentSession,
    OpenFileInPlaceOfPlugin(FileToOpen, bool, Context),
    GroupAndUngroupPanes(Vec<PaneId>, Vec<PaneId>, bool),
    HighlightAndUnhighlightPanes(Vec<PaneId>, Vec<PaneId>),
    CloseMultiplePanes(Vec<PaneId>),
    FloatMultiplePanes(Vec<PaneId>),
    EmbedMultiplePanes(Vec<PaneId>),
    QueryWebServerStatus,
    SetSelfMouseSelectionSupport(bool),
    GenerateWebLoginToken(Option<String>, bool),
    RevokeWebLoginToken(String),
    ListWebLoginTokens,
    RevokeAllWebLoginTokens,
    RenameWebLoginToken(String, String),
    InterceptKeyPresses,
    ClearKeyPressesIntercepts,
    ReplacePaneWithExistingPane(PaneId, PaneId, bool),
    RunAction(Action, BTreeMap<String, String>),
    CopyToClipboard(String),
    OverrideLayout(LayoutInfo, bool, bool, bool, BTreeMap<String, String>),
    SaveLayout { layout_name: String, layout_kdl: String, overwrite: bool },
    DeleteLayout { layout_name: String },
    RenameLayout { old_layout_name: String, new_layout_name: String },
    EditLayout { layout_name: String, context: Context },
    GenerateRandomName,
    DumpLayout(String),
    ParseLayout(String),
    GetLayoutDir,
    GetFocusedPaneInfo,
    SaveSession,
    CurrentSessionLastSavedTime,
    GetPaneInfo(PaneId),
    GetTabInfo(usize),
    GetSessionEnvironmentVariables,
    OpenCommandPaneInNewTab(CommandToRun, Context),
    OpenPluginPaneInNewTab {
        plugin_url: String,
        configuration: BTreeMap<String, String>,
        context: Context,
    },
    OpenEditorPaneInNewTab(FileToOpen, Context),
    OpenCommandPaneInPlaceOfPaneId(PaneId, CommandToRun, bool, Context),
    OpenTerminalPaneInPlaceOfPaneId(PaneId, FileToOpen, bool),
    OpenEditPaneInPlaceOfPaneId(PaneId, FileToOpen, bool, Context),
    HideFloatingPanes { tab_id: Option<usize> },
    ShowFloatingPanes { tab_id: Option<usize> },
    SetPaneColor(PaneId, Option<String>, Option<String>),
    SetPaneRegexHighlights(PaneId, Vec<RegexHighlight>),
    ClearPaneHighlights(PaneId),
    OpenPluginPaneFloating {
        plugin_url: String,
        configuration: BTreeMap<String, String>,
        floating_pane_coordinates: Option<FloatingPaneCoordinates>,
        context: BTreeMap<String, String>,
    },
    ListWindowsVolumes,
    GetSessionList,
    KillSessionsAndReply(Vec<String>),
    DeleteDeadSessionAndReply(String),
    DeleteAllDeadSessionsAndReply,
    SetSoftKeyboard(bool),
    FocusHostSession,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenPaneInNewTabResponse {
    pub tab_id: Option<usize>,
    pub pane_id: Option<PaneId>,
}
pub type NewTabResponse = Option<usize>;
pub type NewTabUnfocusedResponse = Option<usize>;
pub type NewTabsResponse = Vec<usize>;
pub type FocusOrCreateTabResponse = Option<usize>;
pub type BreakPanesToNewTabResponse = Option<usize>;
pub type BreakPanesToTabWithIndexResponse = Option<usize>;
pub type BreakPanesToTabWithIdResponse = Option<usize>;
pub type OpenFileResponse = Option<PaneId>;
pub type OpenFileFloatingResponse = Option<PaneId>;
pub type OpenFileInPlaceResponse = Option<PaneId>;
pub type OpenFileNearPluginResponse = Option<PaneId>;
pub type OpenFileFloatingNearPluginResponse = Option<PaneId>;
pub type OpenFileInPlaceOfPluginResponse = Option<PaneId>;
pub type OpenTerminalResponse = Option<PaneId>;
pub type OpenTerminalFloatingResponse = Option<PaneId>;
pub type OpenTerminalInPlaceResponse = Option<PaneId>;
pub type OpenTerminalNearPluginResponse = Option<PaneId>;
pub type OpenTerminalFloatingNearPluginResponse = Option<PaneId>;
pub type OpenTerminalInPlaceOfPluginResponse = Option<PaneId>;
pub type NewTiledPaneInTabResponse = Option<PaneId>;
pub type OpenCommandPaneResponse = Option<PaneId>;
pub type OpenCommandPaneFloatingResponse = Option<PaneId>;
pub type OpenCommandPaneInPlaceResponse = Option<PaneId>;
pub type OpenCommandPaneNearPluginResponse = Option<PaneId>;
pub type OpenCommandPaneFloatingNearPluginResponse = Option<PaneId>;
pub type OpenCommandPaneInPlaceOfPluginResponse = Option<PaneId>;
pub type OpenCommandPaneBackgroundResponse = Option<PaneId>;
pub type OpenCommandPaneInPlaceOfPaneIdResponse = Option<PaneId>;
pub type OpenTerminalPaneInPlaceOfPaneIdResponse = Option<PaneId>;
pub type OpenEditPaneInPlaceOfPaneIdResponse = Option<PaneId>;
pub type OpenPluginPaneFloatingResponse = Option<PaneId>;
