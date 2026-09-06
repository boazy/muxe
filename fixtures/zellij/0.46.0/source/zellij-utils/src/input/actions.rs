// Minimized from the exact pinned source: zellij-utils/src/input/actions.rs

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum ResizeDirection {
    Left,
    Right,
    Up,
    Down,
    Increase,
    Decrease,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum SearchDirection {
    Down,
    Up,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum SearchOption {
    CaseSensitivity,
    WholeWord,
    Wrap,
}
/// Actions that can be bound to keys.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    Deserialize,
    Serialize,
    strum_macros::Display,
    strum_macros::EnumString,
    strum_macros::EnumIter,
)]
#[strum(ascii_case_insensitive)]
pub enum Action {
    /// Quit Zellij.
    Quit,
    /// Write to the terminal.
    Write {
        key_with_modifier: Option<KeyWithModifier>,
        bytes: Vec<u8>,
        is_kitty_keyboard_protocol: bool,
    },
    /// Write Characters to the terminal.
    WriteChars { chars: String },
    /// Write to a specific pane by ID.
    WriteToPaneId { bytes: Vec<u8>, pane_id: PaneId },
    /// Write Characters to a specific pane by ID.
    WriteCharsToPaneId { chars: String, pane_id: PaneId },
    /// Paste text using bracketed paste mode, optionally to a specific pane.
    Paste { chars: String, pane_id: Option<PaneId> },
    /// Switch to the specified input mode.
    SwitchToMode { input_mode: InputMode },
    /// Switch all connected clients to the specified input mode.
    SwitchModeForAllClients { input_mode: InputMode },
    /// Shrink/enlarge focused pane at specified border
    Resize { resize: Resize, direction: Option<Direction> },
    /// Switch focus to next pane in specified direction.
    FocusNextPane,
    FocusPreviousPane,
    /// Switch focus to the last focused pane.
    FocusLastPane,
    /// Move the focus pane in specified direction.
    SwitchFocus,
    MoveFocus { direction: Direction },
    /// Tries to move the focus pane in specified direction.
    /// If there is no pane in the direction, move to previous/next Tab.
    MoveFocusOrTab { direction: Direction },
    MovePane { direction: Option<Direction> },
    MovePaneBackwards,
    /// Clear all buffers of a current screen
    ClearScreen,
    /// Dumps the screen to a file or STDOUT
    DumpScreen {
        file_path: Option<String>,
        include_scrollback: bool,
        pane_id: Option<PaneId>,
        ansi: bool,
    },
    /// Dumps
    DumpLayout,
    /// Save the current session state to disk
    SaveSession,
    EditScrollback { ansi: bool },
    /// Scroll up in focus pane.
    ScrollUp,
    /// Scroll up at point
    ScrollUpAt { position: Position },
    /// Scroll down in focus pane.
    ScrollDown,
    /// Scroll down at point
    ScrollDownAt { position: Position },
    ScrollToPreviousPrompt,
    ScrollToNextPrompt,
    SelectCommandAtScrollPosition,
    CopyLastCommandOutput,
    /// Scroll down to bottom in focus pane.
    ScrollToBottom,
    /// Scroll up to top in focus pane.
    ScrollToTop,
    /// Scroll up one page in focus pane.
    PageScrollUp,
    /// Scroll down one page in focus pane.
    PageScrollDown,
    /// Scroll up half page in focus pane.
    HalfPageScrollUp,
    /// Scroll down half page in focus pane.
    HalfPageScrollDown,
    /// Toggle between fullscreen focus pane and normal layout.
    ToggleFocusFullscreen,
    ToggleFocusNoUiFullscreen,
    /// Toggle frames around panes in the UI
    TogglePaneFrames,
    SetPaneFrameStyle(PaneFrameStyle),
    /// Toggle between sending text commands to all panes on the current tab and normal mode.
    ToggleActiveSyncTab,
    /// Open a new pane in the specified direction (relative to focus).
    /// If no direction is specified, will try to use the biggest available space.
    NewPane {
        direction: Option<Direction>,
        pane_name: Option<String>,
        start_suppressed: bool,
    },
    /// Returns: Created pane ID (format: terminal_<id>)
    NewBlockingPane {
        placement: NewPanePlacement,
        pane_name: Option<String>,
        command: Option<RunCommandAction>,
        unblock_condition: Option<UnblockCondition>,
        near_current_pane: bool,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    /// Open the file in a new pane using the default editor
    /// Returns: Created pane ID (format: terminal_<id>)
    EditFile {
        payload: OpenFilePayload,
        direction: Option<Direction>,
        floating: bool,
        in_place: bool,
        close_replaced_pane: bool,
        start_suppressed: bool,
        coordinates: Option<FloatingPaneCoordinates>,
        near_current_pane: bool,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    /// Open a new floating pane
    /// Returns: Created pane ID (format: terminal_<id> or plugin_<id>)
    NewFloatingPane {
        command: Option<RunCommandAction>,
        pane_name: Option<String>,
        coordinates: Option<FloatingPaneCoordinates>,
        near_current_pane: bool,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    /// Open a new tiled (embedded, non-floating) pane
    /// Returns: Created pane ID (format: terminal_<id> or plugin_<id>)
    NewTiledPane {
        direction: Option<Direction>,
        command: Option<RunCommandAction>,
        pane_name: Option<String>,
        near_current_pane: bool,
        no_focus: bool,
        borderless: Option<bool>,
        tab_id: Option<usize>,
    },
    /// Open a new pane in place of the focused one, suppressing it instead
    /// Returns: Created pane ID (format: terminal_<id> or plugin_<id>)
    NewInPlacePane {
        command: Option<RunCommandAction>,
        pane_name: Option<String>,
        near_current_pane: bool,
        no_focus: bool,
        pane_id_to_replace: Option<PaneId>,
        close_replaced_pane: bool,
        tab_id: Option<usize>,
    },
    /// Returns: Created pane ID (format: terminal_<id> or plugin_<id>)
    NewStackedPane {
        command: Option<RunCommandAction>,
        pane_name: Option<String>,
        near_current_pane: bool,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    /// Embed focused pane in tab if floating or float focused pane if embedded
    TogglePaneEmbedOrFloating,
    /// Toggle the visibility of all floating panes (if any) in the current Tab
    ToggleFloatingPanes,
    /// Show all floating panes in the specified tab (or active tab if tab_id is None)
    ShowFloatingPanes { tab_id: Option<usize> },
    /// Hide all floating panes in the specified tab (or active tab if tab_id is None)
    HideFloatingPanes { tab_id: Option<usize> },
    /// Check if floating panes are visible in the specified tab (or active tab if tab_id is None)
    AreFloatingPanesVisible { tab_id: Option<usize> },
    /// Close the focus pane.
    CloseFocus,
    PaneNameInput { input: Vec<u8> },
    UndoRenamePane,
    /// Create a new tab, optionally with a specified tab layout.
    NewTab {
        tiled_layout: Option<TiledPaneLayout>,
        floating_layouts: Vec<FloatingPaneLayout>,
        swap_tiled_layouts: Option<Vec<SwapTiledLayout>>,
        swap_floating_layouts: Option<Vec<SwapFloatingLayout>>,
        tab_name: Option<String>,
        should_change_focus_to_new_tab: bool,
        cwd: Option<PathBuf>,
        initial_panes: Option<Vec<CommandOrPlugin>>,
        first_pane_unblock_condition: Option<UnblockCondition>,
    },
    /// Do nothing.
    NoOp,
    /// Go to the next tab.
    GoToNextTab,
    /// Go to the previous tab.
    GoToPreviousTab,
    /// Close the current tab.
    CloseTab,
    GoToTab { index: u32 },
    GoToTabName { name: String, create: bool },
    ToggleTab,
    TabNameInput { input: Vec<u8> },
    UndoRenameTab,
    MoveTab { direction: Direction },
    /// Run specified command in new pane.
    Run { command: RunCommandAction, near_current_pane: bool, no_focus: bool },
    /// Set pane default foreground/background color
    SetPaneColor { pane_id: PaneId, fg: Option<String>, bg: Option<String> },
    /// Detach session and exit
    Detach,
    /// Switch the host-terminal theme mode to dark (uses configured `theme_dark`).
    SetDarkTheme,
    /// Switch the host-terminal theme mode to light (uses configured `theme_light`).
    SetLightTheme,
    /// Toggle between dark and light host-terminal theme modes.
    ToggleTheme,
    /// Switch to a different session
    SwitchSession {
        name: String,
        tab_position: Option<usize>,
        pane_id: Option<(u32, bool)>,
        layout: Option<LayoutInfo>,
        cwd: Option<PathBuf>,
    },
    /// Returns: Plugin pane ID (format: plugin_<id>) when creating or focusing plugin
    LaunchOrFocusPlugin {
        plugin: RunPluginOrAlias,
        should_float: bool,
        move_to_focused_tab: bool,
        should_open_in_place: bool,
        close_replaced_pane: bool,
        skip_cache: bool,
        tab_id: Option<usize>,
    },
    /// Returns: Plugin pane ID (format: plugin_<id>)
    LaunchPlugin {
        plugin: RunPluginOrAlias,
        should_float: bool,
        should_open_in_place: bool,
        close_replaced_pane: bool,
        skip_cache: bool,
        cwd: Option<PathBuf>,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    MouseEvent { event: MouseEvent },
    Copy,
    /// Confirm a prompt
    Confirm,
    /// Deny a prompt
    Deny,
    /// Confirm an action that invokes a prompt automatically
    SkipConfirm { action: Box<Action> },
    /// Search for String
    SearchInput { input: Vec<u8> },
    /// Search for something
    Search { direction: SearchDirection },
    /// Toggle case sensitivity of search
    SearchToggleOption { option: SearchOption },
    ToggleMouseMode,
    PreviousSwapLayout,
    NextSwapLayout,
    /// Override the layout of the active tab
    OverrideLayout {
        tabs: Vec<TabLayoutInfo>,
        retain_existing_terminal_panes: bool,
        retain_existing_plugin_panes: bool,
        apply_only_to_active_tab: bool,
    },
    /// Query all tab names
    QueryTabNames,
    /// Open a new tiled (embedded, non-floating) plugin pane
    /// Returns: Created pane ID (format: plugin_<id>)
    NewTiledPluginPane {
        plugin: RunPluginOrAlias,
        pane_name: Option<String>,
        skip_cache: bool,
        cwd: Option<PathBuf>,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    /// Returns: Created pane ID (format: plugin_<id>)
    NewFloatingPluginPane {
        plugin: RunPluginOrAlias,
        pane_name: Option<String>,
        skip_cache: bool,
        cwd: Option<PathBuf>,
        coordinates: Option<FloatingPaneCoordinates>,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    /// Returns: Created pane ID (format: plugin_<id>)
    NewInPlacePluginPane {
        plugin: RunPluginOrAlias,
        pane_name: Option<String>,
        skip_cache: bool,
        close_replaced_pane: bool,
        no_focus: bool,
        tab_id: Option<usize>,
    },
    StartOrReloadPlugin { plugin: RunPluginOrAlias },
    CloseTerminalPane { pane_id: u32 },
    ClosePluginPane { pane_id: u32 },
    FocusTerminalPaneWithId {
        pane_id: u32,
        should_float_if_hidden: bool,
        should_be_in_place_if_hidden: bool,
    },
    FocusPluginPaneWithId {
        pane_id: u32,
        should_float_if_hidden: bool,
        should_be_in_place_if_hidden: bool,
    },
    RenameTerminalPane { pane_id: u32, name: Vec<u8> },
    RenamePluginPane { pane_id: u32, name: Vec<u8> },
    RenameTab { tab_index: u32, name: Vec<u8> },
    GoToTabById { id: u64 },
    CloseTabById { id: u64 },
    RenameTabById { id: u64, name: String },
    BreakPane,
    BreakPaneRight,
    BreakPaneLeft,
    FocusHostSession,
    FocusGuestSession,
    ToggleHostFullscreen,
    RenameSession { name: String },
    CliPipe {
        pipe_id: String,
        name: Option<String>,
        payload: Option<String>,
        args: Option<BTreeMap<String, String>>,
        plugin: Option<String>,
        configuration: Option<BTreeMap<String, String>>,
        launch_new: bool,
        skip_cache: bool,
        floating: Option<bool>,
        in_place: Option<bool>,
        cwd: Option<PathBuf>,
        pane_title: Option<String>,
    },
    KeybindPipe {
        name: Option<String>,
        payload: Option<String>,
        args: Option<BTreeMap<String, String>>,
        plugin: Option<String>,
        plugin_id: Option<u32>,
        configuration: Option<BTreeMap<String, String>>,
        launch_new: bool,
        skip_cache: bool,
        floating: Option<bool>,
        in_place: Option<bool>,
        cwd: Option<PathBuf>,
        pane_title: Option<String>,
    },
    ListClients,
    ListPanes {
        show_tab: bool,
        show_command: bool,
        show_state: bool,
        show_geometry: bool,
        show_all: bool,
        output_json: bool,
    },
    ListTabs {
        show_state: bool,
        show_dimensions: bool,
        show_panes: bool,
        show_layout: bool,
        show_all: bool,
        output_json: bool,
    },
    CurrentTabInfo { output_json: bool },
    TogglePanePinned,
    StackPanes { pane_ids: Vec<PaneId> },
    ChangeFloatingPaneCoordinates {
        pane_id: PaneId,
        coordinates: FloatingPaneCoordinates,
    },
    TogglePaneBorderless { pane_id: PaneId },
    SetPaneBorderless { pane_id: PaneId, borderless: bool },
    TogglePaneInGroup,
    ToggleGroupMarking,
    ScrollUpByPaneId { pane_id: PaneId },
    ScrollDownByPaneId { pane_id: PaneId },
    ScrollToTopByPaneId { pane_id: PaneId },
    ScrollToBottomByPaneId { pane_id: PaneId },
    PageScrollUpByPaneId { pane_id: PaneId },
    PageScrollDownByPaneId { pane_id: PaneId },
    HalfPageScrollUpByPaneId { pane_id: PaneId },
    HalfPageScrollDownByPaneId { pane_id: PaneId },
    ResizeByPaneId { pane_id: PaneId, resize: Resize, direction: Option<Direction> },
    MovePaneByPaneId { pane_id: PaneId, direction: Option<Direction> },
    MovePaneBackwardsByPaneId { pane_id: PaneId },
    ClearScreenByPaneId { pane_id: PaneId },
    EditScrollbackByPaneId { pane_id: PaneId, ansi: bool },
    ToggleFocusFullscreenByPaneId { pane_id: PaneId },
    ToggleFocusNoUiFullscreenByPaneId { pane_id: PaneId },
    TogglePaneEmbedOrFloatingByPaneId { pane_id: PaneId },
    CloseFocusByPaneId { pane_id: PaneId },
    RenamePaneByPaneId { pane_id: Option<PaneId>, name: Vec<u8> },
    UndoRenamePaneByPaneId { pane_id: PaneId },
    TogglePanePinnedByPaneId { pane_id: PaneId },
    FocusPaneByPaneId { pane_id: PaneId },
    UndoRenameTabByTabId { id: u64 },
    ToggleActiveSyncTabByTabId { id: u64 },
    ToggleFloatingPanesByTabId { id: u64 },
    PreviousSwapLayoutByTabId { id: u64 },
    NextSwapLayoutByTabId { id: u64 },
    MoveTabByTabId { id: u64, direction: Direction },
}
