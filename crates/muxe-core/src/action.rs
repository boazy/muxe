use std::collections::BTreeMap;
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

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

/// Group used to organize portable actions in generated documentation.
#[derive(Clone, Copy, Debug, Display, EnumIter, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ActionCategory {
    Menu,
    Configuration,
    Keyboard,
    Command,
    Tab,
    Pane,
    Session,
}

/// Closed registry of portable action parameter names.
#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    EnumIter,
    EnumString,
    Eq,
    Hash,
    IntoStaticStr,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum ActionParameterName {
    #[strum(serialize = "menu")]
    Menu,
    #[strum(serialize = "submenu")]
    Submenu,
    #[strum(serialize = "sequence")]
    Sequence,
    #[strum(serialize = "keys")]
    Keys,
    #[strum(serialize = "text")]
    Text,
    #[strum(serialize = "program")]
    Program,
    #[strum(serialize = "args")]
    Args,
    #[strum(serialize = "cwd")]
    Cwd,
    #[strum(serialize = "env")]
    Env,
    #[strum(serialize = "workspace-id")]
    WorkspaceId,
    #[strum(serialize = "name")]
    Name,
    #[strum(serialize = "focus")]
    Focus,
    #[strum(serialize = "index")]
    Index,
    #[strum(serialize = "direction")]
    Direction,
    #[strum(serialize = "amount")]
    Amount,
    #[strum(serialize = "enabled")]
    Enabled,
    #[strum(serialize = "visible")]
    Visible,
}

impl ActionParameterName {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// Value grammar accepted by one portable action parameter.
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum ActionParameterType {
    #[strum(serialize = "menu name")]
    MenuName,
    #[strum(serialize = "menu mapping")]
    MenuMapping,
    #[strum(serialize = "unsupported")]
    Unsupported,
    #[strum(serialize = "list of keys or `$origin.` references")]
    KeySequence,
    #[strum(serialize = "string or string-valued `$origin.` reference")]
    StringContextOnly,
    #[strum(serialize = "string or `$origin.` reference")]
    TextualContext,
    #[strum(serialize = "list of strings or `$origin.` references")]
    TextualContextSequence,
    #[strum(serialize = "path or `$origin.` reference")]
    ContextPath,
    #[strum(serialize = "mapping")]
    TextualContextMapping,
    #[strum(serialize = "workspace ID or `$origin.` reference")]
    ContextWorkspaceId,
    #[strum(serialize = "boolean")]
    Boolean,
    #[strum(serialize = "non-negative integer or `$origin.` reference")]
    Index,
    #[strum(serialize = "direction or `$origin.` reference")]
    Direction,
    #[strum(serialize = "number or `$origin.` reference")]
    Scalar,
}

/// Behavior used when an optional parameter is omitted.
///
/// This metadata describes downstream behavior. Parsing preserves the existing `None`, empty
/// sequence, or empty mapping representation instead of injecting a scalar value.
#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum OmittedParameterBehavior {
    #[strum(serialize = "empty list")]
    EmptySequence,
    #[strum(serialize = "empty mapping")]
    EmptyMapping,
    #[strum(serialize = "working directory of the pane where the menu was opened")]
    OriginPaneCwd,
    #[strum(serialize = "workspace containing the pane where the menu was opened")]
    OriginWorkspace,
    #[strum(serialize = "chosen by Zellij or Herdr")]
    HostGeneratedName,
    #[strum(serialize = "host-dependent; see host note")]
    HostDependent,
    #[strum(serialize = "default shell")]
    DefaultShell,
    #[strum(serialize = "true")]
    True,
    #[strum(serialize = "one resize step")]
    ResizeStep,
    #[strum(serialize = "toggle the current state")]
    Toggle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionParameterSchema {
    pub name: ActionParameterName,
    pub value_type: ActionParameterType,
    pub required: bool,
    pub positional: Option<usize>,
    pub omitted: Option<OmittedParameterBehavior>,
}

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum ConstraintValidationPhase {
    BeforeFields,
    AfterFields,
}

#[derive(Clone, Copy, Debug, Display, Eq, PartialEq)]
pub enum ConstraintErrorLocation {
    Action,
    FirstFieldValue,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionConstraint {
    ExactlyOne {
        parameters: &'static [ActionParameterName],
        message: &'static str,
        validation_phase: ConstraintValidationPhase,
        error_location: ConstraintErrorLocation,
    },
    Requires {
        parameter: ActionParameterName,
        required_parameter: ActionParameterName,
        message: &'static str,
    },
    Unsupported {
        parameter: ActionParameterName,
        message: &'static str,
    },
}

/// Generated syntax and execution metadata for one portable action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableActionSchema {
    pub kind: PortableActionKind,
    pub category: ActionCategory,
    pub parameters: &'static [ActionParameterSchema],
    pub constraints: &'static [ActionConstraint],
    pub default_execution: Option<ExecutionMode>,
    pub capabilities: ExecutionCapabilities,
}

macro_rules! parameter_required {
    () => {
        false
    };
    (true) => {
        true
    };
}

macro_rules! parameter_position {
    () => {
        None
    };
    ($position:literal) => {
        Some($position)
    };
}

macro_rules! parameter_omitted {
    () => {
        None
    };
    ($omitted:ident) => {
        Some(OmittedParameterBehavior::$omitted)
    };
}

macro_rules! define_portable_actions {
    (
        $(
            $variant:ident {
                name: $name:literal,
                category: $category:ident,
                execution: $execution:expr,
                capabilities: $capabilities:expr,
                parameters: [
                    $(
                        $field:ident as $parameter:ident : $value_type:ident
                        $([required = $required:tt])?
                        $([position = $position:literal])?
                        $([default = $omitted:ident])?;
                    )*
                ],
                constraints: [
                    $($constraint:expr;)*
                ],
            }
        )*
    ) => {
        /// The closed v1 portable action registry.
        #[derive(
            Clone,
            Copy,
            Debug,
            Display,
            EnumIter,
            EnumString,
            Eq,
            Hash,
            IntoStaticStr,
            Ord,
            PartialEq,
            PartialOrd,
        )]
        #[non_exhaustive]
        pub enum PortableActionKind {
            $(
                #[strum(serialize = $name)]
                $variant,
            )*
        }

        impl PortableActionKind {
            #[must_use]
            pub fn parse(value: &str) -> Option<Self> {
                value.parse().ok()
            }

            #[must_use]
            pub fn as_str(self) -> &'static str {
                self.into()
            }

            #[must_use]
            pub const fn schema(self) -> PortableActionSchema {
                match self {
                    $(
                        Self::$variant => PortableActionSchema {
                            kind: self,
                            category: ActionCategory::$category,
                            parameters: &[
                                $(
                                    ActionParameterSchema {
                                        name: ActionParameterName::$parameter,
                                        value_type: ActionParameterType::$value_type,
                                        required: parameter_required!($($required)?),
                                        positional: parameter_position!($($position)?),
                                        omitted: parameter_omitted!($($omitted)?),
                                    },
                                )*
                            ],
                            constraints: &[$($constraint,)*],
                            default_execution: $execution,
                            capabilities: $capabilities,
                        },
                    )*
                }
            }
        }
    };
}

macro_rules! portable_action_definitions {
    ($callback:ident) => {
        $callback! {
        MenuOpen {
            name: "menu:open",
            category: Menu,
            execution: None,
            capabilities: ExecutionCapabilities::SYNCHRONOUS,
            parameters: [
                menu as Menu: MenuName [position = 0];
                submenu as Submenu: MenuMapping;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Menu, ActionParameterName::Submenu],
                    validation_phase: ConstraintValidationPhase::BeforeFields,
                    message: "menu:open requires exactly one of `menu` or `submenu`",
                    error_location: ConstraintErrorLocation::Action,
                };
            ],
        }
        MenuReturn {
            name: "menu:return",
            category: Menu,
            execution: None,
            capabilities: ExecutionCapabilities::SYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        MenuQuit {
            name: "menu:quit",
            category: Menu,
            execution: None,
            capabilities: ExecutionCapabilities::SYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        MenuPagePrev {
            name: "menu.page:prev",
            category: Menu,
            execution: None,
            capabilities: ExecutionCapabilities::SYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        MenuPageNext {
            name: "menu.page:next",
            category: Menu,
            execution: None,
            capabilities: ExecutionCapabilities::SYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        ConfigReload {
            name: "config:reload",
            category: Configuration,
            execution: None,
            capabilities: ExecutionCapabilities::SYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        KeyboardSend {
            name: "keyboard:send",
            category: Keyboard,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                sequence as Sequence: Unsupported;
                keys as Keys: KeySequence;
                text as Text: StringContextOnly;
            ],
            constraints: [
                ActionConstraint::Unsupported {
                    parameter: ActionParameterName::Sequence,
                    message: "keyboard:send `sequence` is not supported in schema version 1",
                };
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Keys, ActionParameterName::Text],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "keyboard:send requires exactly one of `keys` or `text`",
                    error_location: ConstraintErrorLocation::Action,
                };
            ],
        }
        CommandExecute {
            name: "command:execute",
            category: Command,
            execution: Some(ExecutionMode::Detach),
            capabilities: ExecutionCapabilities {
                awaitable: true,
                detachable: true,
                cancellable: true,
            },
            parameters: [
                program as Program: TextualContext [required = true];
                args as Args: TextualContextSequence [default = EmptySequence];
                cwd as Cwd: ContextPath [default = OriginPaneCwd];
                env as Env: TextualContextMapping [default = EmptyMapping];
            ],
            constraints: [],
        }
        TabCreate {
            name: "tab:create",
            category: Tab,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                workspace_id as WorkspaceId: ContextWorkspaceId [default = OriginWorkspace];
                name as Name: TextualContext [default = HostGeneratedName];
                focus as Focus: Boolean [default = True];
                program as Program: TextualContext [default = DefaultShell];
                args as Args: TextualContextSequence [default = EmptySequence];
                cwd as Cwd: ContextPath [default = OriginPaneCwd];
            ],
            constraints: [
                ActionConstraint::Requires {
                    parameter: ActionParameterName::Args,
                    required_parameter: ActionParameterName::Program,
                    message: "tab args requires `program`",
                };
            ],
        }
        TabClose {
            name: "tab:close",
            category: Tab,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        TabRename {
            name: "tab:rename",
            category: Tab,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                name as Name: TextualContext;
            ],
            constraints: [],
        }
        TabFocus {
            name: "tab:focus",
            category: Tab,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                index as Index: Index [position = 0];
                direction as Direction: Direction;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Index, ActionParameterName::Direction],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "action requires exactly one of `index` or `direction`",
                    error_location: ConstraintErrorLocation::FirstFieldValue,
                };
            ],
        }
        TabMove {
            name: "tab:move",
            category: Tab,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                direction as Direction: Direction [position = 0];
                index as Index: Index;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Index, ActionParameterName::Direction],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "action requires exactly one of `index` or `direction`",
                    error_location: ConstraintErrorLocation::FirstFieldValue,
                };
            ],
        }
        TabSwap {
            name: "tab:swap",
            category: Tab,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                index as Index: Index [position = 0];
                direction as Direction: Direction;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Index, ActionParameterName::Direction],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "action requires exactly one of `index` or `direction`",
                    error_location: ConstraintErrorLocation::FirstFieldValue,
                };
            ],
        }
        PaneCreate {
            name: "pane:create",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        PaneSplit {
            name: "pane:split",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                direction as Direction: Direction [position = 0] [default = HostDependent];
                focus as Focus: Boolean [default = True];
                program as Program: TextualContext [default = DefaultShell];
                args as Args: TextualContextSequence [default = EmptySequence];
                cwd as Cwd: ContextPath [default = OriginPaneCwd];
            ],
            constraints: [
                ActionConstraint::Requires {
                    parameter: ActionParameterName::Args,
                    required_parameter: ActionParameterName::Program,
                    message: "pane args requires `program`",
                };
            ],
        }
        PaneClose {
            name: "pane:close",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        PaneFocus {
            name: "pane:focus",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                index as Index: Index [position = 0];
                direction as Direction: Direction;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Index, ActionParameterName::Direction],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "action requires exactly one of `index` or `direction`",
                    error_location: ConstraintErrorLocation::FirstFieldValue,
                };
            ],
        }
        PaneMove {
            name: "pane:move",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                direction as Direction: Direction [position = 0];
                index as Index: Index;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Index, ActionParameterName::Direction],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "action requires exactly one of `index` or `direction`",
                    error_location: ConstraintErrorLocation::FirstFieldValue,
                };
            ],
        }
        PaneSwap {
            name: "pane:swap",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                index as Index: Index [position = 0];
                direction as Direction: Direction;
            ],
            constraints: [
                ActionConstraint::ExactlyOne {
                    parameters: &[ActionParameterName::Index, ActionParameterName::Direction],
                    validation_phase: ConstraintValidationPhase::AfterFields,
                    message: "action requires exactly one of `index` or `direction`",
                    error_location: ConstraintErrorLocation::FirstFieldValue,
                };
            ],
        }
        PaneResize {
            name: "pane:resize",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                direction as Direction: Direction [required = true];
                amount as Amount: Scalar [default = ResizeStep];
            ],
            constraints: [],
        }
        PaneZoom {
            name: "pane:zoom",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                enabled as Enabled: Boolean [default = Toggle];
            ],
            constraints: [],
        }
        PaneFullscreen {
            name: "pane:fullscreen",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                enabled as Enabled: Boolean [default = Toggle];
            ],
            constraints: [],
        }
        PaneFloating {
            name: "pane:floating",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                enabled as Enabled: Boolean [default = Toggle];
            ],
            constraints: [],
        }
        PaneFrame {
            name: "pane:frame",
            category: Pane,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                visible as Visible: Boolean [default = Toggle];
            ],
            constraints: [],
        }
        SessionCreate {
            name: "session:create",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        SessionAttach {
            name: "session:attach",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                name as Name: TextualContext [required = true] [position = 0];
            ],
            constraints: [],
        }
        SessionSwitch {
            name: "session:switch",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                name as Name: TextualContext [required = true] [position = 0];
            ],
            constraints: [],
        }
        SessionRename {
            name: "session:rename",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [
                name as Name: TextualContext [required = true] [position = 0];
            ],
            constraints: [],
        }
        SessionDetach {
            name: "session:detach",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        SessionQuit {
            name: "session:quit",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
        SessionKill {
            name: "session:kill",
            category: Session,
            execution: Some(ExecutionMode::Await),
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
            parameters: [],
            constraints: [],
        }
            }
    };
}

pub(crate) use portable_action_definitions;

portable_action_definitions!(define_portable_actions);

#[derive(Clone, Copy, Debug, Display, EnumString, Eq, Hash, IntoStaticStr, PartialEq)]
#[strum(serialize_all = "lowercase")]
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
    Named(crate::menu::MenuName),
    /// Compiler-generated structural identity for one embedded submenu.
    Inline(crate::menu::InlineMenuId),
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
    ensure_string(scalar, parameter)?
        .parse::<Direction>()
        .map(|_| ())
        .map_err(|_| {
            invalid_value(
                scalar,
                parameter,
                "direction must be left, right, up, down, next, or previous",
            )
        })
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use strum::IntoEnumIterator;

    use super::{ActionConstraint, ActionParameterType, PortableActionKind};

    #[test]
    fn portable_action_schemas_are_complete_and_internally_consistent() {
        let mut action_names = BTreeSet::new();
        for action in PortableActionKind::iter() {
            assert!(action_names.insert(action.as_str()));
            assert_eq!(action.as_str().parse::<PortableActionKind>(), Ok(action));

            let schema = action.schema();
            assert_eq!(schema.kind, action);
            let mut parameter_names = BTreeSet::new();
            let mut positions = BTreeSet::new();
            for parameter in schema.parameters {
                assert!(
                    parameter_names.insert(parameter.name),
                    "{action} declares `{}` more than once",
                    parameter.name
                );
                if let Some(position) = parameter.positional {
                    assert!(
                        positions.insert(position),
                        "{action} declares positional index {position} more than once"
                    );
                }
                assert!(
                    !parameter.required || parameter.omitted.is_none(),
                    "{action}.{} is required but declares omitted-value behavior",
                    parameter.name
                );
            }
            assert_eq!(
                positions.iter().copied().collect::<Vec<_>>(),
                (0..positions.len()).collect::<Vec<_>>(),
                "{action} positional indices must be contiguous"
            );

            for constraint in schema.constraints {
                match constraint {
                    ActionConstraint::ExactlyOne { parameters, .. } => {
                        assert!(parameters.len() >= 2);
                        for parameter in *parameters {
                            assert!(
                                parameter_names.contains(parameter),
                                "{action} constraint names undeclared parameter `{parameter}`"
                            );
                        }
                    }
                    ActionConstraint::Requires {
                        parameter,
                        required_parameter,
                        ..
                    } => {
                        assert!(parameter_names.contains(parameter));
                        assert!(parameter_names.contains(required_parameter));
                    }
                    ActionConstraint::Unsupported { parameter, .. } => {
                        assert!(parameter_names.contains(parameter));
                        assert_eq!(
                            schema
                                .parameters
                                .iter()
                                .find(|candidate| candidate.name == *parameter)
                                .map(|candidate| candidate.value_type),
                            Some(ActionParameterType::Unsupported)
                        );
                    }
                }
            }
        }
    }
}
