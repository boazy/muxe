// Minimized from the exact pinned source: zellij-utils/src/input/layout.rs

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone, Copy)]
pub enum SplitDirection {
    Horizontal,
    Vertical,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum SplitSize {
    #[serde(alias = "percent")]
    Percent(usize),
    #[serde(alias = "fixed")]
    Fixed(usize),
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum RunPluginOrAlias {
    RunPlugin(RunPlugin),
    Alias(PluginAlias),
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Run {
    #[serde(rename = "plugin")]
    Plugin(RunPluginOrAlias),
    #[serde(rename = "command")]
    Command(RunCommand),
    EditFile(PathBuf, Option<usize>, Option<PathBuf>),
    Cwd(PathBuf),
}
#[allow(clippy::derive_hash_xor_eq)]
#[derive(Debug, Serialize, Deserialize, Clone, Hash, Default)]
pub struct RunPlugin {
    #[serde(default)]
    pub _allow_exec_host_cmd: bool,
    pub location: RunPluginLocation,
    pub configuration: PluginUserConfiguration,
    pub initial_cwd: Option<PathBuf>,
}
#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq)]
pub struct PluginAlias {
    pub name: String,
    pub configuration: Option<PluginUserConfiguration>,
    pub initial_cwd: Option<PathBuf>,
    pub run_plugin: Option<RunPlugin>,
}
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PluginUserConfiguration(BTreeMap<String, String>);
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum RunPluginLocation {
    File(PathBuf),
    Zellij(PluginTag),
    Remote(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub enum LayoutConstraint {
    MaxPanes(usize),
    MinPanes(usize),
    ExactPanes(usize),
    NoConstraint,
}
pub type SwapTiledLayout = (BTreeMap<LayoutConstraint, TiledPaneLayout>, Option<String>);
pub type SwapFloatingLayout = (
    BTreeMap<LayoutConstraint, Vec<FloatingPaneLayout>>,
    Option<String>,
);
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct Layout {
    pub tabs: Vec<(Option<String>, TiledPaneLayout, Vec<FloatingPaneLayout>)>,
    pub focused_tab_index: Option<usize>,
    pub template: Option<(TiledPaneLayout, Vec<FloatingPaneLayout>)>,
    pub swap_layouts: Vec<(TiledPaneLayout, Vec<FloatingPaneLayout>)>,
    pub swap_tiled_layouts: Vec<SwapTiledLayout>,
    pub swap_floating_layouts: Vec<SwapFloatingLayout>,
}
/// Layout configuration for a single tab in multi-tab override
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct TabLayoutInfo {
    pub tab_index: usize,
    pub tab_name: Option<String>,
    pub tiled_layout: TiledPaneLayout,
    pub floating_layouts: Vec<FloatingPaneLayout>,
    pub swap_tiled_layouts: Option<Vec<SwapTiledLayout>>,
    pub swap_floating_layouts: Option<Vec<SwapFloatingLayout>>,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum PercentOrFixed {
    Percent(usize),
    Fixed(usize),
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct FloatingPaneLayout {
    pub name: Option<String>,
    pub height: Option<PercentOrFixed>,
    pub width: Option<PercentOrFixed>,
    pub x: Option<PercentOrFixed>,
    pub y: Option<PercentOrFixed>,
    pub pinned: Option<bool>,
    pub borderless: Option<bool>,
    pub run: Option<Run>,
    pub focus: Option<bool>,
    pub already_running: bool,
    pub pane_initial_contents: Option<String>,
    pub logical_position: Option<usize>,
    pub default_fg: Option<String>,
    pub default_bg: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct TiledPaneLayout {
    pub children_split_direction: SplitDirection,
    pub name: Option<String>,
    pub children: Vec<TiledPaneLayout>,
    pub split_size: Option<SplitSize>,
    pub run: Option<Run>,
    pub borderless: Option<bool>,
    pub focus: Option<bool>,
    pub external_children_index: Option<usize>,
    pub children_are_stacked: bool,
    pub is_expanded_in_stack: bool,
    pub exclude_from_sync: Option<bool>,
    pub run_instructions_to_ignore: Vec<Option<Run>>,
    pub hide_floating_panes: bool,
    pub pane_initial_contents: Option<String>,
    pub default_fg: Option<String>,
    pub default_bg: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum LayoutParts {
    Tabs(Vec<(Option<String>, Layout)>),
    Panes(Vec<Layout>),
}
