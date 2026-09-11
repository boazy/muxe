use std::collections::BTreeMap;

use crate::config::{ConfigField, ConfigValue, ConfigValueKind, ContextResolutionError};
use crate::context::OriginContext;
use crate::diagnostic::SourceSpan;
use crate::execution::{ExecutionCapabilities, ExecutionMode};
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
            .or_else(|| {
                value
                    .starts_with("native.")
                    .then(|| Self::Native(value.to_owned()))
            })
    }

    #[must_use]
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
    TabSwap,
    PaneCreate,
    PaneSplit,
    PaneClose,
    PaneFocus,
    PaneMove,
    PaneSwap,
    PaneResize,
    PaneZoom,
    PaneFullscreen,
    PaneFloating,
    PaneFrame,
    SessionCreate,
    SessionAttach,
    SessionSwitch,
    SessionRename,
    SessionDetach,
    SessionQuit,
    SessionKill,
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
        Self::TabSwap,
        Self::PaneCreate,
        Self::PaneSplit,
        Self::PaneClose,
        Self::PaneFocus,
        Self::PaneMove,
        Self::PaneSwap,
        Self::PaneResize,
        Self::PaneZoom,
        Self::PaneFullscreen,
        Self::PaneFloating,
        Self::PaneFrame,
        Self::SessionCreate,
        Self::SessionAttach,
        Self::SessionSwitch,
        Self::SessionRename,
        Self::SessionDetach,
        Self::SessionQuit,
        Self::SessionKill,
    ];

    #[must_use]
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
            Self::TabSwap => "tab:swap",
            Self::PaneCreate => "pane:create",
            Self::PaneSplit => "pane:split",
            Self::PaneClose => "pane:close",
            Self::PaneFocus => "pane:focus",
            Self::PaneMove => "pane:move",
            Self::PaneSwap => "pane:swap",
            Self::PaneResize => "pane:resize",
            Self::PaneZoom => "pane:zoom",
            Self::PaneFullscreen => "pane:fullscreen",
            Self::PaneFloating => "pane:floating",
            Self::PaneFrame => "pane:frame",
            Self::SessionCreate => "session:create",
            Self::SessionAttach => "session:attach",
            Self::SessionSwitch => "session:switch",
            Self::SessionRename => "session:rename",
            Self::SessionDetach => "session:detach",
            Self::SessionQuit => "session:quit",
            Self::SessionKill => "session:kill",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|kind| kind.as_str() == value)
    }
}

/// One entry in the closed portable-action registry. `parameter_syntax` is emitted into the
/// generated reference and intentionally describes the source schema, not host wire payloads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableActionDescriptor {
    pub kind: PortableActionKind,
    pub parameter_syntax: &'static str,
    pub default_execution: Option<ExecutionMode>,
    pub capabilities: ExecutionCapabilities,
}

impl PortableActionKind {
    #[must_use]
    pub const fn descriptor(self) -> PortableActionDescriptor {
        let (parameter_syntax, default_execution, capabilities) = match self {
            Self::MenuOpen => (
                "menu=<id> or submenu=<menu>",
                None,
                ExecutionCapabilities::SYNCHRONOUS,
            ),
            Self::MenuReturn
            | Self::MenuQuit
            | Self::MenuPagePrev
            | Self::MenuPageNext
            | Self::ConfigReload => ("none", None, ExecutionCapabilities::SYNCHRONOUS),
            Self::KeyboardSend => (
                "exactly one of keys=[<canonical-key or $context>, …] or text=<string or $context>",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::CommandExecute => (
                "program=<string or $context>; args=[<string or $context>, …]?; cwd=<path or $context>?; env={<string>: <string or $context>}?",
                Some(ExecutionMode::Detach),
                ExecutionCapabilities {
                    awaitable: true,
                    detachable: true,
                    cancellable: true,
                },
            ),
            Self::TabClose
            | Self::PaneCreate
            | Self::PaneClose
            | Self::SessionCreate
            | Self::SessionDetach
            | Self::SessionQuit
            | Self::SessionKill => (
                "none",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::TabCreate => (
                "workspace-id=<string or $context>?; name=<string or $context>?; focus=<boolean>? (default true); program=<string or $context>?; args=[<string or $context>, …]?; cwd=<path or $context>?",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::TabRename => (
                "name=<string or $context>?",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::TabFocus
            | Self::TabMove
            | Self::TabSwap
            | Self::PaneFocus
            | Self::PaneMove
            | Self::PaneSwap => (
                "exactly one of index=<non-negative integer or $context> or direction=<direction or $context>",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::PaneSplit => (
                "direction=<direction or $context>?; focus=<boolean>? (default true); program=<string or $context>?; args=[<string or $context>, …]?; cwd=<path or $context>?",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::PaneResize => (
                "direction=<direction or $context>; amount=<scalar or $context>?",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::PaneZoom | Self::PaneFullscreen | Self::PaneFloating => (
                "enabled=<boolean>?",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::PaneFrame => (
                "visible=<boolean>?",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
            Self::SessionAttach | Self::SessionSwitch | Self::SessionRename => (
                "name=<string or $context>",
                Some(ExecutionMode::Await),
                ExecutionCapabilities::ASYNCHRONOUS,
            ),
        };
        PortableActionDescriptor {
            kind: self,
            parameter_syntax,
            default_execution,
            capabilities,
        }
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

/// One scalar action parameter before origin resolution. It carries either a concrete YAML scalar
/// or a normalized typed context marker, always retaining the source span that produced it.
#[derive(Clone, Debug, PartialEq)]
pub struct ActionScalar {
    pub value: ConfigValue,
}

impl ActionScalar {
    #[must_use]
    pub fn new(value: ConfigValue) -> Self {
        Self { value }
    }

    /// Resolves a context scalar or reports the unavailable origin field.
    ///
    /// # Errors
    ///
    /// Returns an error when the referenced origin context is unavailable.
    pub fn resolve_context(&self, origin: &OriginContext) -> Result<Self, ContextResolutionError> {
        Ok(Self {
            value: self.value.resolve_context(origin)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum IndexOrDirection {
    Index(ActionScalar),
    Direction(ActionScalar),
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

#[derive(Clone, Debug, PartialEq)]
pub enum KeyboardAction {
    SendKeys(Vec<ActionScalar>),
    SendText(ActionScalar),
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandAction {
    pub program: ActionScalar,
    pub args: Vec<ActionScalar>,
    pub cwd: Option<ActionScalar>,
    pub env: BTreeMap<String, ActionScalar>,
}

/// Exact command-vector fields for a tab or split-pane creation action.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CreateCommand {
    pub program: Option<ActionScalar>,
    pub args: Vec<ActionScalar>,
    pub cwd: Option<ActionScalar>,
}

#[expect(
    clippy::large_enum_variant,
    reason = "the compiled action model carries creation payloads inline; `Create` is the common case, and boxing `command` would heap-allocate for it while churning every construction and match site in the compiler, both adapters, the broker, and fixtures"
)]
#[derive(Clone, Debug, PartialEq)]
pub enum TabAction {
    Create {
        workspace_id: Option<ActionScalar>,
        name: Option<ActionScalar>,
        focus: Option<ActionScalar>,
        command: CreateCommand,
    },
    Close,
    Rename {
        name: Option<ActionScalar>,
    },
    Focus(IndexOrDirection),
    Move(IndexOrDirection),
    Swap(IndexOrDirection),
}

#[derive(Clone, Debug, PartialEq)]
pub enum PaneAction {
    Create,
    Split {
        direction: Option<ActionScalar>,
        focus: Option<ActionScalar>,
        command: CreateCommand,
    },
    Close,
    Focus(IndexOrDirection),
    Move(IndexOrDirection),
    Swap(IndexOrDirection),
    Resize {
        direction: ActionScalar,
        amount: Option<ActionScalar>,
    },
    Zoom {
        enabled: Option<ActionScalar>,
    },
    Fullscreen {
        enabled: Option<ActionScalar>,
    },
    Floating {
        enabled: Option<ActionScalar>,
    },
    Frame {
        visible: Option<ActionScalar>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum SessionAction {
    Create,
    Attach { name: ActionScalar },
    Switch { name: ActionScalar },
    Rename { name: ActionScalar },
    Detach,
    Quit,
    Kill,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PortableAction {
    Menu(MenuAction),
    Config(ConfigAction),
    Keyboard(KeyboardAction),
    Command(CommandAction),
    Tab(TabAction),
    Pane(PaneAction),
    Session(SessionAction),
}

impl PortableAction {
    #[must_use]
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
            Self::Tab(TabAction::Create { .. }) => PortableActionKind::TabCreate,
            Self::Tab(TabAction::Close) => PortableActionKind::TabClose,
            Self::Tab(TabAction::Rename { .. }) => PortableActionKind::TabRename,
            Self::Tab(TabAction::Focus(_)) => PortableActionKind::TabFocus,
            Self::Tab(TabAction::Move(_)) => PortableActionKind::TabMove,
            Self::Tab(TabAction::Swap(_)) => PortableActionKind::TabSwap,
            Self::Pane(PaneAction::Create) => PortableActionKind::PaneCreate,
            Self::Pane(PaneAction::Split { .. }) => PortableActionKind::PaneSplit,
            Self::Pane(PaneAction::Close) => PortableActionKind::PaneClose,
            Self::Pane(PaneAction::Focus(_)) => PortableActionKind::PaneFocus,
            Self::Pane(PaneAction::Move(_)) => PortableActionKind::PaneMove,
            Self::Pane(PaneAction::Swap(_)) => PortableActionKind::PaneSwap,
            Self::Pane(PaneAction::Resize { .. }) => PortableActionKind::PaneResize,
            Self::Pane(PaneAction::Zoom { .. }) => PortableActionKind::PaneZoom,
            Self::Pane(PaneAction::Fullscreen { .. }) => PortableActionKind::PaneFullscreen,
            Self::Pane(PaneAction::Floating { .. }) => PortableActionKind::PaneFloating,
            Self::Pane(PaneAction::Frame { .. }) => PortableActionKind::PaneFrame,
            Self::Session(SessionAction::Create) => PortableActionKind::SessionCreate,
            Self::Session(SessionAction::Attach { .. }) => PortableActionKind::SessionAttach,
            Self::Session(SessionAction::Switch { .. }) => PortableActionKind::SessionSwitch,
            Self::Session(SessionAction::Rename { .. }) => PortableActionKind::SessionRename,
            Self::Session(SessionAction::Detach) => PortableActionKind::SessionDetach,
            Self::Session(SessionAction::Quit) => PortableActionKind::SessionQuit,
            Self::Session(SessionAction::Kill) => PortableActionKind::SessionKill,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PortableActionResolutionError {
    Context(ContextResolutionError),
    InvalidValue {
        parameter: &'static str,
        message: String,
        span: SourceSpan,
    },
}

impl PortableAction {
    /// Resolves every portable scalar against the immutable attach-time origin and validates the
    /// resulting concrete values before an adapter can observe or dispatch the action.
    ///
    /// # Errors
    ///
    /// Returns an error when a required origin field is unavailable or a resolved scalar is invalid.
    #[expect(
        clippy::too_many_lines,
        reason = "the closed portable action registry stays co-located with its context validation"
    )]
    pub fn resolve_context(
        &self,
        origin: &OriginContext,
    ) -> Result<Self, PortableActionResolutionError> {
        Ok(match self {
            Self::Menu(action) => Self::Menu(action.clone()),
            Self::Config(action) => Self::Config(*action),
            Self::Keyboard(KeyboardAction::SendKeys(keys)) => {
                let keys = keys
                    .iter()
                    .map(|key| {
                        let key = resolve_scalar(key, origin)?;
                        ensure_key(&key)?;
                        Ok(key)
                    })
                    .collect::<Result<_, PortableActionResolutionError>>()?;
                Self::Keyboard(KeyboardAction::SendKeys(keys))
            }
            Self::Keyboard(KeyboardAction::SendText(text)) => {
                let text = resolve_scalar(text, origin)?;
                ensure_string(&text, "keyboard.text")?;
                Self::Keyboard(KeyboardAction::SendText(text))
            }
            Self::Command(command) => {
                let program = resolve_scalar(&command.program, origin)?;
                ensure_string(&program, "command.program")?;
                let args = command
                    .args
                    .iter()
                    .map(|argument| {
                        let argument = resolve_scalar(argument, origin)?;
                        ensure_string(&argument, "command.args")?;
                        Ok(argument)
                    })
                    .collect::<Result<_, PortableActionResolutionError>>()?;
                let cwd = command
                    .cwd
                    .as_ref()
                    .map(|cwd| {
                        let context_path = matches!(cwd.value.kind, ConfigValueKind::Context(_));
                        let cwd = resolve_scalar(cwd, origin)?;
                        if context_path {
                            ensure_absolute_path(&cwd, "command.cwd")?;
                        } else {
                            ensure_string(&cwd, "command.cwd")?;
                        }
                        Ok(cwd)
                    })
                    .transpose()?;
                let env = command
                    .env
                    .iter()
                    .map(|(name, value)| {
                        let value = resolve_scalar(value, origin)?;
                        ensure_string(&value, "command.env")?;
                        Ok((name.clone(), value))
                    })
                    .collect::<Result<_, PortableActionResolutionError>>()?;
                Self::Command(CommandAction {
                    program,
                    args,
                    cwd,
                    env,
                })
            }
            Self::Tab(TabAction::Create {
                workspace_id,
                name,
                focus,
                command,
            }) => {
                let workspace_id = workspace_id
                    .as_ref()
                    .map(|workspace_id| {
                        let workspace_id = resolve_scalar(workspace_id, origin)?;
                        ensure_string(&workspace_id, "tab.workspace-id")?;
                        Ok(workspace_id)
                    })
                    .transpose()?;
                let name = resolve_optional_string(name.as_ref(), origin, "tab.name")?;
                let focus = resolve_optional_bool(focus.as_ref(), origin, "tab.focus")?;
                let command =
                    resolve_create_command(command, origin, "tab.program", "tab.args", "tab.cwd")?;
                Self::Tab(TabAction::Create {
                    workspace_id,
                    name,
                    focus,
                    command,
                })
            }
            Self::Tab(TabAction::Close) => Self::Tab(TabAction::Close),
            Self::Tab(TabAction::Rename { name }) => {
                let name = resolve_optional_string(name.as_ref(), origin, "tab.name")?;
                Self::Tab(TabAction::Rename { name })
            }
            Self::Tab(TabAction::Focus(target)) => {
                Self::Tab(TabAction::Focus(resolve_target(target, origin)?))
            }
            Self::Tab(TabAction::Move(target)) => {
                Self::Tab(TabAction::Move(resolve_target(target, origin)?))
            }
            Self::Tab(TabAction::Swap(target)) => {
                Self::Tab(TabAction::Swap(resolve_target(target, origin)?))
            }
            Self::Pane(PaneAction::Create) => Self::Pane(PaneAction::Create),
            Self::Pane(PaneAction::Split {
                direction,
                focus,
                command,
            }) => {
                let direction =
                    resolve_optional_direction(direction.as_ref(), origin, "pane.direction")?;
                let focus = resolve_optional_bool(focus.as_ref(), origin, "pane.focus")?;
                let command = resolve_create_command(
                    command,
                    origin,
                    "pane.program",
                    "pane.args",
                    "pane.cwd",
                )?;
                Self::Pane(PaneAction::Split {
                    direction,
                    focus,
                    command,
                })
            }
            Self::Pane(PaneAction::Close) => Self::Pane(PaneAction::Close),
            Self::Pane(PaneAction::Focus(target)) => {
                Self::Pane(PaneAction::Focus(resolve_target(target, origin)?))
            }
            Self::Pane(PaneAction::Move(target)) => {
                Self::Pane(PaneAction::Move(resolve_target(target, origin)?))
            }
            Self::Pane(PaneAction::Swap(target)) => {
                Self::Pane(PaneAction::Swap(resolve_target(target, origin)?))
            }
            Self::Pane(PaneAction::Resize { direction, amount }) => {
                let direction = resolve_scalar(direction, origin)?;
                ensure_direction(&direction, "pane.direction")?;
                let amount = amount
                    .as_ref()
                    .map(|amount| {
                        let amount = resolve_scalar(amount, origin)?;
                        ensure_non_null_scalar(&amount, "pane.amount")?;
                        Ok(amount)
                    })
                    .transpose()?;
                Self::Pane(PaneAction::Resize { direction, amount })
            }
            Self::Pane(PaneAction::Zoom { enabled }) => {
                let enabled = resolve_optional_bool(enabled.as_ref(), origin, "pane.enabled")?;
                Self::Pane(PaneAction::Zoom { enabled })
            }
            Self::Pane(PaneAction::Fullscreen { enabled }) => {
                let enabled = resolve_optional_bool(enabled.as_ref(), origin, "pane.enabled")?;
                Self::Pane(PaneAction::Fullscreen { enabled })
            }
            Self::Pane(PaneAction::Floating { enabled }) => {
                let enabled = resolve_optional_bool(enabled.as_ref(), origin, "pane.enabled")?;
                Self::Pane(PaneAction::Floating { enabled })
            }
            Self::Pane(PaneAction::Frame { visible }) => {
                let visible = resolve_optional_bool(visible.as_ref(), origin, "pane.visible")?;
                Self::Pane(PaneAction::Frame { visible })
            }
            Self::Session(SessionAction::Create) => Self::Session(SessionAction::Create),
            Self::Session(SessionAction::Attach { name }) => {
                let name = resolve_scalar(name, origin)?;
                ensure_string(&name, "session.name")?;
                Self::Session(SessionAction::Attach { name })
            }
            Self::Session(SessionAction::Switch { name }) => {
                let name = resolve_scalar(name, origin)?;
                ensure_string(&name, "session.name")?;
                Self::Session(SessionAction::Switch { name })
            }
            Self::Session(SessionAction::Rename { name }) => {
                let name = resolve_scalar(name, origin)?;
                ensure_string(&name, "session.name")?;
                Self::Session(SessionAction::Rename { name })
            }
            Self::Session(SessionAction::Detach) => Self::Session(SessionAction::Detach),
            Self::Session(SessionAction::Quit) => Self::Session(SessionAction::Quit),
            Self::Session(SessionAction::Kill) => Self::Session(SessionAction::Kill),
        })
    }
}

fn resolve_scalar(
    scalar: &ActionScalar,
    origin: &OriginContext,
) -> Result<ActionScalar, PortableActionResolutionError> {
    scalar
        .resolve_context(origin)
        .map_err(PortableActionResolutionError::Context)
}

fn resolve_target(
    target: &IndexOrDirection,
    origin: &OriginContext,
) -> Result<IndexOrDirection, PortableActionResolutionError> {
    match target {
        IndexOrDirection::Index(index) => {
            let index = resolve_scalar(index, origin)?;
            ensure_index(&index, "index")?;
            Ok(IndexOrDirection::Index(index))
        }
        IndexOrDirection::Direction(direction) => {
            let direction = resolve_scalar(direction, origin)?;
            ensure_direction(&direction, "direction")?;
            Ok(IndexOrDirection::Direction(direction))
        }
    }
}

fn resolve_optional_string(
    scalar: Option<&ActionScalar>,
    origin: &OriginContext,
    parameter: &'static str,
) -> Result<Option<ActionScalar>, PortableActionResolutionError> {
    scalar
        .map(|scalar| {
            let scalar = resolve_scalar(scalar, origin)?;
            ensure_string(&scalar, parameter)?;
            Ok(scalar)
        })
        .transpose()
}

fn resolve_optional_direction(
    scalar: Option<&ActionScalar>,
    origin: &OriginContext,
    parameter: &'static str,
) -> Result<Option<ActionScalar>, PortableActionResolutionError> {
    scalar
        .map(|scalar| {
            let scalar = resolve_scalar(scalar, origin)?;
            ensure_direction(&scalar, parameter)?;
            Ok(scalar)
        })
        .transpose()
}

fn resolve_create_command(
    command: &CreateCommand,
    origin: &OriginContext,
    program_parameter: &'static str,
    args_parameter: &'static str,
    cwd_parameter: &'static str,
) -> Result<CreateCommand, PortableActionResolutionError> {
    let program = command
        .program
        .as_ref()
        .map(|program| {
            let program = resolve_scalar(program, origin)?;
            ensure_string(&program, program_parameter)?;
            Ok(program)
        })
        .transpose()?;
    let args = command
        .args
        .iter()
        .map(|argument| {
            let argument = resolve_scalar(argument, origin)?;
            ensure_string(&argument, args_parameter)?;
            Ok(argument)
        })
        .collect::<Result<_, PortableActionResolutionError>>()?;
    let cwd = command
        .cwd
        .as_ref()
        .map(|cwd| {
            let context_path = matches!(cwd.value.kind, ConfigValueKind::Context(_));
            let cwd = resolve_scalar(cwd, origin)?;
            if context_path {
                ensure_absolute_path(&cwd, cwd_parameter)?;
            } else {
                ensure_string(&cwd, cwd_parameter)?;
            }
            Ok(cwd)
        })
        .transpose()?;
    Ok(CreateCommand { program, args, cwd })
}

fn resolve_optional_bool(
    scalar: Option<&ActionScalar>,
    origin: &OriginContext,
    parameter: &'static str,
) -> Result<Option<ActionScalar>, PortableActionResolutionError> {
    scalar
        .map(|scalar| {
            let scalar = resolve_scalar(scalar, origin)?;
            ensure_bool(&scalar, parameter)?;
            Ok(scalar)
        })
        .transpose()
}

fn ensure_key(scalar: &ActionScalar) -> Result<(), PortableActionResolutionError> {
    let value = ensure_string(scalar, "keyboard.keys")?;
    CanonicalKey::parse(value).map(|_| ()).map_err(|error| {
        invalid_value(
            scalar,
            "keyboard.keys",
            format!("invalid canonical key: {error}"),
        )
    })
}

fn ensure_direction(
    scalar: &ActionScalar,
    parameter: &'static str,
) -> Result<(), PortableActionResolutionError> {
    match ensure_string(scalar, parameter)? {
        "left" | "right" | "up" | "down" | "next" | "previous" => Ok(()),
        _ => Err(invalid_value(
            scalar,
            parameter,
            "direction must be left, right, up, down, next, or previous",
        )),
    }
}

fn ensure_index(
    scalar: &ActionScalar,
    parameter: &'static str,
) -> Result<(), PortableActionResolutionError> {
    matches!(scalar.value.kind, ConfigValueKind::Integer(value) if value >= 0)
        .then_some(())
        .ok_or_else(|| invalid_value(scalar, parameter, "index must be a non-negative integer"))
}

fn ensure_bool(
    scalar: &ActionScalar,
    parameter: &'static str,
) -> Result<(), PortableActionResolutionError> {
    matches!(scalar.value.kind, ConfigValueKind::Boolean(_))
        .then_some(())
        .ok_or_else(|| invalid_value(scalar, parameter, "value must be boolean"))
}

fn ensure_string<'a>(
    scalar: &'a ActionScalar,
    parameter: &'static str,
) -> Result<&'a str, PortableActionResolutionError> {
    scalar
        .value
        .as_str()
        .ok_or_else(|| invalid_value(scalar, parameter, "value must be a string"))
}

fn ensure_absolute_path(
    scalar: &ActionScalar,
    parameter: &'static str,
) -> Result<(), PortableActionResolutionError> {
    let value = ensure_string(scalar, parameter)?;
    std::path::Path::new(value)
        .is_absolute()
        .then_some(())
        .ok_or_else(|| invalid_value(scalar, parameter, "path must be absolute"))
}

fn ensure_non_null_scalar(
    scalar: &ActionScalar,
    parameter: &'static str,
) -> Result<(), PortableActionResolutionError> {
    (!matches!(
        scalar.value.kind,
        ConfigValueKind::Null
            | ConfigValueKind::Mapping(_)
            | ConfigValueKind::Sequence(_)
            | ConfigValueKind::Context(_)
    ))
    .then_some(())
    .ok_or_else(|| invalid_value(scalar, parameter, "value must be a non-null scalar"))
}

fn invalid_value(
    scalar: &ActionScalar,
    parameter: &'static str,
    message: impl Into<String>,
) -> PortableActionResolutionError {
    PortableActionResolutionError::InvalidValue {
        parameter,
        message: message.into(),
        span: scalar.value.span.clone(),
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
    #[must_use]
    pub fn field(&self, name: &str) -> Option<&ConfigValue> {
        self.fields
            .iter()
            .find_map(|field| (field.name == name).then_some(&field.value))
    }

    /// Resolves typed `$context` markers against the immutable attach-time origin immediately
    /// before native dispatch. The returned candidate contains no context marker.
    ///
    /// # Errors
    ///
    /// Returns an error when a referenced origin context field is unavailable.
    pub fn resolve_context(&self, origin: &OriginContext) -> Result<Self, ContextResolutionError> {
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

#[expect(
    clippy::large_enum_variant,
    reason = "portable is the dominant compiled form; boxing it would add indirection on every dispatch while churning each compiler, broker, and adapter site that builds or matches this enum for the smaller native alternative"
)]
#[derive(Clone, Debug, PartialEq)]
pub enum ActionSpec {
    Portable(PortableAction),
    Native(NativeActionCandidate),
}

impl ActionSpec {
    #[must_use]
    pub fn kind(&self) -> ActionKind {
        match self {
            Self::Portable(action) => ActionKind::Portable(action.kind()),
            Self::Native(action) => ActionKind::Native(action.type_name.clone()),
        }
    }
}
