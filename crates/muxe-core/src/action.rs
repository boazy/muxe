use std::collections::BTreeMap;

use crate::config::{ConfigField, ConfigValue, ContextResolutionError};
use crate::context::OriginContext;
use crate::key::CanonicalKey;
/// Flat action discriminator accepted by the v1 compiler.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ActionKind {
    Portable(PortableActionKind),
    Native(String),
}

impl ActionKind {
    pub fn parse(value: &str) -> Option<Self> {
        PortableActionKind::parse(value)
            .map(Self::Portable)
            .or_else(|| value.starts_with("native.").then(|| Self::Native(value.to_owned())))
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Portable(kind) => kind.as_str(),
            Self::Native(kind) => kind,
        }
    }
}

/// The closed v1 portable action registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum PortableActionKind {
    MenuOpen,
    MenuReturn,
    MenuQuit,
    MenuPagePrev,
    MenuPageNext,
    ConfigReload,
    KeyboardSend,
    CommandExecute,
    TabCreate,
    TabClose,
    TabRename,
    TabFocus,
    TabMove,
    PaneSplit,
    PaneClose,
    PaneFocus,
    PaneMove,
    PaneSwap,
    PaneResize,
    PaneZoom,
}

impl PortableActionKind {
    pub const ALL: &'static [Self] = &[
        Self::MenuOpen,
        Self::MenuReturn,
        Self::MenuQuit,
        Self::MenuPagePrev,
        Self::MenuPageNext,
        Self::ConfigReload,
        Self::KeyboardSend,
        Self::CommandExecute,
        Self::TabCreate,
        Self::TabClose,
        Self::TabRename,
        Self::TabFocus,
        Self::TabMove,
        Self::PaneSplit,
        Self::PaneClose,
        Self::PaneFocus,
        Self::PaneMove,
        Self::PaneSwap,
        Self::PaneResize,
        Self::PaneZoom,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MenuOpen => "menu:open",
            Self::MenuReturn => "menu:return",
            Self::MenuQuit => "menu:quit",
            Self::MenuPagePrev => "menu.page:prev",
            Self::MenuPageNext => "menu.page:next",
            Self::ConfigReload => "config:reload",
            Self::KeyboardSend => "keyboard:send",
            Self::CommandExecute => "command:execute",
            Self::TabCreate => "tab:create",
            Self::TabClose => "tab:close",
            Self::TabRename => "tab:rename",
            Self::TabFocus => "tab:focus",
            Self::TabMove => "tab:move",
            Self::PaneSplit => "pane:split",
            Self::PaneClose => "pane:close",
            Self::PaneFocus => "pane:focus",
            Self::PaneMove => "pane:move",
            Self::PaneSwap => "pane:swap",
            Self::PaneResize => "pane:resize",
            Self::PaneZoom => "pane:zoom",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|kind| kind.as_str() == value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
    Left,
    Right,
    Up,
    Down,
    Next,
    Previous,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IndexOrDirection {
    Index(u64),
    Direction(Direction),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MenuTarget {
    Named(String),
    /// Compiler-generated stable ID for one embedded submenu.
    Inline(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MenuAction {
    Open(MenuTarget),
    Return,
    Quit,
    PagePrev,
    PageNext,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigAction {
    Reload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyboardAction {
    SendKeys(Vec<CanonicalKey>),
    SendText(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandAction {
    pub program: String,
    pub args: Vec<String>,
    /// Scalar context references remain source-aware until broker dispatch resolves the origin.
    pub cwd: Option<ConfigValue>,
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TabAction {
    Create,
    Close,
    Rename { name: Option<String> },
    Focus(IndexOrDirection),
    Move(IndexOrDirection),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PaneAction {
    Split { direction: Option<Direction> },
    Close,
    Focus(IndexOrDirection),
    Move(IndexOrDirection),
    Swap(IndexOrDirection),
    Resize { direction: Direction, amount: Option<String> },
    Zoom { enabled: Option<bool> },
}


#[derive(Clone, Debug, PartialEq)]
pub enum PortableAction {
    Menu(MenuAction),
    Config(ConfigAction),
    Keyboard(KeyboardAction),
    Command(CommandAction),
    Tab(TabAction),
    Pane(PaneAction),
}

impl PortableAction {
    pub const fn kind(&self) -> PortableActionKind {
        match self {
            Self::Menu(MenuAction::Open(_)) => PortableActionKind::MenuOpen,
            Self::Menu(MenuAction::Return) => PortableActionKind::MenuReturn,
            Self::Menu(MenuAction::Quit) => PortableActionKind::MenuQuit,
            Self::Menu(MenuAction::PagePrev) => PortableActionKind::MenuPagePrev,
            Self::Menu(MenuAction::PageNext) => PortableActionKind::MenuPageNext,
            Self::Config(ConfigAction::Reload) => PortableActionKind::ConfigReload,
            Self::Keyboard(_) => PortableActionKind::KeyboardSend,
            Self::Command(_) => PortableActionKind::CommandExecute,
            Self::Tab(TabAction::Create) => PortableActionKind::TabCreate,
            Self::Tab(TabAction::Close) => PortableActionKind::TabClose,
            Self::Tab(TabAction::Rename { .. }) => PortableActionKind::TabRename,
            Self::Tab(TabAction::Focus(_)) => PortableActionKind::TabFocus,
            Self::Tab(TabAction::Move(_)) => PortableActionKind::TabMove,
            Self::Pane(PaneAction::Split { .. }) => PortableActionKind::PaneSplit,
            Self::Pane(PaneAction::Close) => PortableActionKind::PaneClose,
            Self::Pane(PaneAction::Focus(_)) => PortableActionKind::PaneFocus,
            Self::Pane(PaneAction::Move(_)) => PortableActionKind::PaneMove,
            Self::Pane(PaneAction::Swap(_)) => PortableActionKind::PaneSwap,
            Self::Pane(PaneAction::Resize { .. }) => PortableActionKind::PaneResize,
            Self::Pane(PaneAction::Zoom { .. }) => PortableActionKind::PaneZoom,
        }
    }
}

/// Source-aware native candidate retained by the core but interpreted only by an injected
/// adapter/schema validator. Ordered fields preserve their individual name and value spans.
#[derive(Clone, Debug, PartialEq)]
pub struct NativeActionCandidate {
    pub type_name: String,
    /// Source range of the native discriminator, including zero-field methods such as ping.
    pub type_span: crate::diagnostic::SourceSpan,
    pub fields: Vec<ConfigField>,
}
impl NativeActionCandidate {
    pub fn field(&self, name: &str) -> Option<&ConfigValue> {
        self.fields.iter().find_map(|field| (field.name == name).then_some(&field.value))
    }

    /// Resolves typed `$context` markers against the immutable attach-time origin immediately
    /// before native dispatch. The returned candidate contains no context marker.
    pub fn resolve_context(
        &self,
        origin: &OriginContext,
    ) -> Result<Self, ContextResolutionError> {
        Ok(Self {
            type_name: self.type_name.clone(),
            type_span: self.type_span.clone(),
            fields: self
                .fields
                .iter()
                .map(|field| {
                    Ok(ConfigField {
                        name: field.name.clone(),
                        name_span: field.name_span.clone(),
                        value: field.value.resolve_context(origin)?,
                    })
                })
                .collect::<Result<_, ContextResolutionError>>()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ActionSpec {
    Portable(PortableAction),
    Native(NativeActionCandidate),
}

impl ActionSpec {
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::Portable(action) => ActionKind::Portable(action.kind()),
            Self::Native(action) => ActionKind::Native(action.type_name.clone()),
        }
    }
}
