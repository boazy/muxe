// Minimized from the exact pinned source: zellij-tile/src/shim.rs

pub fn subscribe(event_types: &[EventType]) {}
pub fn unsubscribe(event_types: &[EventType]) {}
pub fn set_selectable(selectable: bool) {}
pub fn show_cursor(cursor_position: Option<(usize, usize)>) {}
pub fn request_permission(permissions: &[PermissionType]) {}
pub fn get_plugin_ids() -> PluginIds {}
pub fn get_zellij_version() -> String {}
pub fn generate_random_name() -> String {}
pub fn dump_layout(layout_name: &str) -> Result<String, String> {}
pub fn get_layout_dir() -> String {}
pub fn get_session_environment_variables() -> BTreeMap<String, String> {}
pub fn get_focused_pane_info() -> Result<(usize, PaneId), String> {}
pub fn get_pane_info(pane_id: PaneId) -> Option<PaneInfo> {}
pub fn get_tab_info(tab_id: usize) -> Option<TabInfo> {}
pub fn save_session() -> Result<(), String> {}
pub fn current_session_last_saved_time() -> Option<u64> {}
pub fn open_file(
    file_to_open: FileToOpen,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_file_floating(
    file_to_open: FileToOpen,
    coordinates: Option<FloatingPaneCoordinates>,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_file_in_place(
    file_to_open: FileToOpen,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_file_near_plugin(
    file_to_open: FileToOpen,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_file_floating_near_plugin(
    file_to_open: FileToOpen,
    coordinates: Option<FloatingPaneCoordinates>,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_file_in_place_of_plugin(
    file_to_open: FileToOpen,
    close_plugin_after_replace: bool,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_terminal<P: AsRef<Path>>(path: P) -> Option<PaneId> {}
pub fn open_terminal_near_plugin<P: AsRef<Path>>(path: P) -> Option<PaneId> {}
pub fn open_terminal_floating<P: AsRef<Path>>(
    path: P,
    coordinates: Option<FloatingPaneCoordinates>,
) -> Option<PaneId> {}
pub fn open_terminal_floating_near_plugin<P: AsRef<Path>>(
    path: P,
    coordinates: Option<FloatingPaneCoordinates>,
) -> Option<PaneId> {}
pub fn open_terminal_in_place<P: AsRef<Path>>(path: P) -> Option<PaneId> {}
pub fn open_terminal_in_place_of_plugin<P: AsRef<Path>>(
    path: P,
    close_plugin_after_replace: bool,
) -> Option<PaneId> {}
pub fn open_command_pane(
    command_to_run: CommandToRun,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_near_plugin(
    command_to_run: CommandToRun,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_floating(
    command_to_run: CommandToRun,
    coordinates: Option<FloatingPaneCoordinates>,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_floating_near_plugin(
    command_to_run: CommandToRun,
    coordinates: Option<FloatingPaneCoordinates>,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_in_place(
    command_to_run: CommandToRun,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_in_place_of_plugin(
    command_to_run: CommandToRun,
    close_plugin_after_replace: bool,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_in_place_of_pane_id(
    pane_id: PaneId,
    command_to_run: CommandToRun,
    close_replaced_pane: bool,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_terminal_pane_in_place_of_pane_id<P: AsRef<Path>>(
    pane_id: PaneId,
    cwd: P,
    close_replaced_pane: bool,
) -> Option<PaneId> {}
pub fn open_edit_pane_in_place_of_pane_id(
    pane_id: PaneId,
    file_to_open: FileToOpen,
    close_replaced_pane: bool,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_command_pane_background(
    command_to_run: CommandToRun,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn switch_tab_to(tab_idx: u32) {}
pub fn set_timeout(secs: f64) {}
pub fn exec_cmd(cmd: &[&str]) {}
pub fn run_command(cmd: &[&str], context: BTreeMap<String, String>) {}
pub fn run_command_with_env_variables_and_cwd(
    cmd: &[&str],
    env_variables: BTreeMap<String, String>,
    cwd: PathBuf,
    context: BTreeMap<String, String>,
) {}
pub fn web_request<S: AsRef<str>>(
    url: S,
    verb: HttpVerb,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    context: BTreeMap<String, String>,
)
where
    S: ToString,
{}
pub fn hide_self() {}
pub fn hide_pane_with_id(pane_id: PaneId) {}
pub fn show_self(should_float_if_hidden: bool) {}
pub fn show_pane_with_id(
    pane_id: PaneId,
    should_float_if_hidden: bool,
    should_focus_pane: bool,
) {}
pub fn close_self() {}
pub fn switch_to_input_mode(mode: &InputMode) {}
pub fn new_tabs_with_layout(layout: &str) -> Vec<usize> {}
pub fn new_tabs_with_layout_info<L: AsRef<LayoutInfo>>(layout_info: L) -> Vec<usize> {}
pub fn new_tab<S: AsRef<str>>(name: Option<S>, cwd: Option<S>) -> Option<usize>
where
    S: ToString,
{}
pub fn new_tab_unfocused<S: AsRef<str>>(name: Option<S>, cwd: Option<S>) -> Option<usize>
where
    S: ToString,
{}
pub fn new_tiled_pane_in_tab(tab_position: usize) -> Option<PaneId> {}
pub fn open_command_pane_in_new_tab(
    command_to_run: CommandToRun,
    context: BTreeMap<String, String>,
) -> (Option<usize>, Option<PaneId>) {}
pub fn open_plugin_pane_in_new_tab(
    plugin_url: impl ToString,
    configuration: BTreeMap<String, String>,
    context: BTreeMap<String, String>,
) -> (Option<usize>, Option<PaneId>) {}
pub fn open_plugin_pane_floating(
    plugin_url: &str,
    configuration: BTreeMap<String, String>,
    coordinates: Option<FloatingPaneCoordinates>,
    context: BTreeMap<String, String>,
) -> Option<PaneId> {}
pub fn open_editor_pane_in_new_tab(
    file_to_open: FileToOpen,
    context: BTreeMap<String, String>,
) -> (Option<usize>, Option<PaneId>) {}
pub fn go_to_next_tab() {}
pub fn go_to_previous_tab() {}
pub fn report_panic(info: &std::panic::PanicHookInfo) {}
pub fn resize_focused_pane(resize: Resize) {}
pub fn resize_focused_pane_with_direction(resize: Resize, direction: Direction) {}
pub fn focus_next_pane() {}
pub fn focus_previous_pane() {}
pub fn focus_last_pane() {}
pub fn move_focus(direction: Direction) {}
pub fn move_focus_or_tab(direction: Direction) {}
pub fn detach() {}
pub fn edit_scrollback() {}
pub fn write(bytes: Vec<u8>) {}
pub fn write_chars(chars: &str) {}
pub fn copy_to_clipboard(text: impl Into<String>) {}
pub fn toggle_tab() {}
pub fn move_pane() {}
pub fn move_pane_with_direction(direction: Direction) {}
pub fn clear_screen() {}
pub fn scroll_up() {}
pub fn scroll_down() {}
pub fn scroll_to_top() {}
pub fn scroll_to_bottom() {}
pub fn page_scroll_up() {}
pub fn page_scroll_down() {}
pub fn toggle_focus_fullscreen() {}
pub fn toggle_focus_no_ui_fullscreen() {}
pub fn focus_host_session() {}
pub fn toggle_pane_frames() {}
pub fn set_pane_frame_style(pane_frame_style: PaneFrameStyle) {}
pub fn toggle_pane_embed_or_eject() {}
pub fn undo_rename_pane() {}
pub fn close_focus() {}
pub fn new_pane() {}
pub fn toggle_floating_panes(tab_id: Option<u64>) {}
pub fn toggle_active_tab_sync() {}
pub fn close_focused_tab() {}
pub fn undo_rename_tab() {}
pub fn quit_zellij() {}
pub fn previous_swap_layout() {}
pub fn next_swap_layout() {}
pub fn go_to_tab_name(tab_name: &str) {}
pub fn focus_or_create_tab(tab_name: &str) -> Option<usize> {}
pub fn go_to_tab(tab_index: u32) {}
pub fn start_or_reload_plugin(url: &str) {}
pub fn close_terminal_pane(terminal_pane_id: u32) {}
pub fn close_plugin_pane(plugin_pane_id: u32) {}
pub fn focus_terminal_pane(
    terminal_pane_id: u32,
    should_float_if_hidden: bool,
    should_be_in_place_if_hidden: bool,
) {}
pub fn focus_plugin_pane(
    plugin_pane_id: u32,
    should_float_if_hidden: bool,
    should_be_in_place_if_hidden: bool,
) {}
pub fn rename_terminal_pane<S: AsRef<str>>(terminal_pane_id: u32, new_name: S)
where
    S: ToString,
{}
pub fn rename_plugin_pane<S: AsRef<str>>(plugin_pane_id: u32, new_name: S)
where
    S: ToString,
{}
pub fn rename_tab<S: AsRef<str>>(tab_position: u32, new_name: S)
where
    S: ToString,
{}
pub fn rename_tab_with_id<S: AsRef<str>>(tab_id: u64, new_name: S)
where
    S: ToString,
{}
pub fn switch_session(name: Option<&str>) {}
pub fn switch_session_with_layout(
    name: Option<&str>,
    layout: LayoutInfo,
    cwd: Option<PathBuf>,
) {}
pub fn switch_session_with_cwd(name: Option<&str>, cwd: Option<PathBuf>) {}
pub fn switch_session_with_focus(
    name: &str,
    tab_position: Option<usize>,
    pane_id: Option<(u32, bool)>,
) {}
pub fn delete_dead_session(name: &str) -> Result<(), String> {}
pub fn delete_all_dead_sessions() -> Result<(), String> {}
pub fn rename_session(name: &str) {}
pub fn unblock_cli_pipe_input(pipe_name: &str) {}
pub fn block_cli_pipe_input(pipe_name: &str) {}
pub fn cli_pipe_output(pipe_name: &str, output: &str) {}
pub fn pipe_message_to_plugin(message_to_plugin: MessageToPlugin) {}
pub fn disconnect_other_clients() {}
pub fn kill_sessions<S: AsRef<str>>(session_names: &[S]) -> Result<(), String>
where
    S: ToString,
{}
pub fn list_windows_volumes() {}
pub fn scan_host_folder<S: AsRef<Path>>(folder_to_scan: &S) {}
pub fn set_soft_keyboard(on: bool) {}
pub fn watch_filesystem() {}
pub fn dump_session_layout() -> Result<(String, Option<LayoutMetadata>), String> {}
pub fn dump_session_layout_for_tab(
    tab_index: usize,
) -> Result<(String, Option<LayoutMetadata>), String> {}
pub fn parse_layout(layout_string: &str) -> Result<LayoutMetadata, LayoutParsingError> {}
pub fn list_clients() {}
pub fn reconfigure(new_config: String, save_configuration_file: bool) {}
pub fn rerun_command_pane(terminal_pane_id: u32) {}
pub fn close_pane_with_id(pane_id: PaneId) {}
pub fn resize_pane_with_id(resize_strategy: ResizeStrategy, pane_id: PaneId) {}
pub fn focus_pane_with_id(
    pane_id: PaneId,
    should_float_if_hidden: bool,
    should_be_in_place_if_hidden: bool,
) {}
pub fn edit_scrollback_for_pane_with_id(pane_id: PaneId) {}
pub fn get_pane_scrollback(
    pane_id: PaneId,
    get_full_scrollback: bool,
) -> Result<PaneContents, String> {}
pub fn write_to_pane_id(bytes: Vec<u8>, pane_id: PaneId) {}
pub fn write_chars_to_pane_id(chars: &str, pane_id: PaneId) {}
pub fn send_sigint_to_pane_id(pane_id: PaneId) {}
pub fn send_sigkill_to_pane_id(pane_id: PaneId) {}
pub fn get_pane_pid(pane_id: PaneId) -> Result<i32, String> {}
pub fn get_pane_running_command(pane_id: PaneId) -> Result<Vec<String>, String> {}
pub fn get_session_list() -> Result<SessionListSnapshot, String> {}
pub fn get_pane_cwd(pane_id: PaneId) -> Result<PathBuf, String> {}
pub fn save_layout<S: AsRef<str>>(
    layout_name: S,
    layout_kdl: S,
    overwrite: bool,
) -> Result<(), String> {}
pub fn delete_layout<S: AsRef<str>>(layout_name: S) -> Result<(), String> {}
pub fn rename_layout(
    old_layout_name: impl Into<String>,
    new_layout_name: impl Into<String>,
) -> Result<(), String> {}
pub fn edit_layout<S: AsRef<str>>(
    layout_name: S,
    context: BTreeMap<String, String>,
) -> Result<(), String> {}
pub fn move_pane_with_pane_id(pane_id: PaneId) {}
pub fn move_pane_with_pane_id_in_direction(pane_id: PaneId, direction: Direction) {}
pub fn clear_screen_for_pane_id(pane_id: PaneId) {}
pub fn scroll_up_in_pane_id(pane_id: PaneId) {}
pub fn scroll_down_in_pane_id(pane_id: PaneId) {}
pub fn scroll_to_top_in_pane_id(pane_id: PaneId) {}
pub fn scroll_to_bottom_in_pane_id(pane_id: PaneId) {}
pub fn page_scroll_up_in_pane_id(pane_id: PaneId) {}
pub fn page_scroll_down_in_pane_id(pane_id: PaneId) {}
pub fn toggle_pane_id_fullscreen(pane_id: PaneId) {}
pub fn toggle_pane_embed_or_eject_for_pane_id(pane_id: PaneId) {}
pub fn close_tab_with_index(tab_index: usize) {}
pub fn close_tab_with_id(tab_id: u64) {}
pub fn rename_pane_with_id<S: AsRef<str>>(pane_id: PaneId, new_name: S)
where
    S: ToString,
{}
pub fn break_panes_to_new_tab(
    pane_ids: &[PaneId],
    new_tab_name: Option<String>,
    should_change_focus_to_new_tab: bool,
) -> Option<usize> {}
pub fn break_panes_to_tab_with_index(
    pane_ids: &[PaneId],
    tab_index: usize,
    should_change_focus_to_new_tab: bool,
) -> Option<usize> {}
pub fn break_panes_to_tab_with_id(
    pane_ids: &[PaneId],
    tab_id: usize,
    should_change_focus_to_target_tab: bool,
) -> Option<usize> {}
pub fn reload_plugin_with_id(plugin_id: u32) {}
pub fn load_new_plugin<S: AsRef<str>>(
    url: S,
    config: BTreeMap<String, String>,
    load_in_background: bool,
    skip_plugin_cache: bool,
)
where
    S: ToString,
{}
pub fn rebind_keys(
    keys_to_unbind: Vec<(InputMode, KeyWithModifier)>,
    keys_to_rebind: Vec<(InputMode, KeyWithModifier, Vec<Action>)>,
    write_config_to_disk: bool,
) {}
pub fn change_host_folder(new_host_folder: PathBuf) {}
pub fn set_floating_pane_pinned(pane_id: PaneId, should_be_pinned: bool) {}
pub fn stack_panes(pane_ids: Vec<PaneId>) {}
pub fn change_floating_panes_coordinates(
    pane_ids_and_coordinates: Vec<(PaneId, FloatingPaneCoordinates)>,
) {}
pub fn toggle_pane_borderless(pane_id: PaneId) {}
pub fn set_pane_borderless(pane_id: PaneId, borderless: bool) {}
pub fn set_pane_color(pane_id: PaneId, fg: Option<String>, bg: Option<String>) {}
pub fn start_web_server() {}
pub fn stop_web_server() {}
pub fn query_web_server_status() {}
pub fn share_current_session() {}
pub fn stop_sharing_current_session() {}
pub fn group_and_ungroup_panes(
    pane_ids_to_group: Vec<PaneId>,
    pane_ids_to_ungroup: Vec<PaneId>,
    for_all_clients: bool,
) {}
pub fn highlight_and_unhighlight_panes(
    pane_ids_to_highlight: Vec<PaneId>,
    pane_ids_to_unhighlight: Vec<PaneId>,
) {}
pub fn close_multiple_panes(pane_ids: Vec<PaneId>) {}
pub fn float_multiple_panes(pane_ids: Vec<PaneId>) {}
pub fn embed_multiple_panes(pane_ids: Vec<PaneId>) {}
pub fn set_self_mouse_selection_support(selection_support: bool) {}
pub fn generate_web_login_token(
    token_label: Option<String>,
    read_only: bool,
) -> Result<String, String> {}
pub fn revoke_web_login_token(token_label: &str) -> Result<(), String> {}
pub fn list_web_login_tokens() -> Result<Vec<(String, String, bool)>, String> {}
pub fn revoke_all_web_tokens() -> Result<(), String> {}
pub fn rename_web_token(old_name: &str, new_name: &str) -> Result<(), String> {}
pub fn intercept_key_presses() {}
pub fn clear_key_presses_intercepts() {}
pub fn replace_pane_with_existing_pane(
    pane_id_to_replace: PaneId,
    existing_pane_id: PaneId,
    suppress_replaced_pane: bool,
) {}
pub fn get_focused_tab(tab_infos: &Vec<TabInfo>) -> Option<TabInfo> {}
pub fn get_focused_pane(
    tab_position: usize,
    pane_manifest: &PaneManifest,
) -> Option<PaneInfo> {}
pub fn override_layout<L: AsRef<LayoutInfo>>(
    layout_info: L,
    retain_existing_terminal_panes: bool,
    retain_existing_plugin_panes: bool,
    apply_only_to_active_tab: bool,
    context: BTreeMap<String, String>,
) {}
pub fn object_from_stdin<T: DeserializeOwned>() -> Result<T> {}
pub fn bytes_from_stdin() -> Result<Vec<u8>> {}
pub fn object_to_stdout(object: &impl Serialize) {}
pub fn post_message_to(plugin_message: PluginMessage) {}
pub fn post_message_to_plugin(plugin_message: PluginMessage) {}
pub fn run_action(action: Action, context: BTreeMap<String, String>) {}
pub fn show_floating_panes(tab_id: Option<usize>) -> Result<bool, String> {}
pub fn hide_floating_panes(tab_id: Option<usize>) -> Result<bool, String> {}
pub fn set_pane_regex_highlights(pane_id: PaneId, highlights: Vec<RegexHighlight>) {}
pub fn clear_pane_highlights(pane_id: PaneId) {}
