//! Host-independent Muxe configuration, key, action, menu, and diagnostic contracts.
//!
//! This crate deliberately contains no host, terminal, async-runtime, IPC, or wire-codec
//! dependency.  Host adapters and protocols convert at their own boundaries.

#![forbid(unsafe_code)]

mod action;
mod compiler;
mod condition;
mod config;
mod context;
mod diagnostic;
mod execution;
mod key;
mod menu;
mod session;
mod theme;

pub use action::{
    ActionCategory, ActionConstraint, ActionKind, ActionParameterName, ActionParameterSchema,
    ActionParameterType, ActionScalar, ActionSpec, CommandAction, ConfigAction,
    ConstraintErrorLocation, ConstraintValidationPhase, CreateCommand, Direction, IndexOrDirection,
    KeyboardAction, MenuAction, MenuTarget, NativeActionCandidate, OmittedParameterBehavior,
    PaneAction, PortableAction, PortableActionKind, PortableActionResolutionError,
    PortableActionSchema, SessionAction, TabAction,
};
pub use condition::{
    ConditionEvaluationError, ConditionIr, ConditionProgram, PagesContext, evaluate_condition_ir,
};
pub use config::{
    ActionValidation, ActionValidator, CompileInput, CompiledConfig, Compiler, ConfigDocument,
    ConfigField, ConfigValue, ConfigValueKind, ContextResolutionError, HostSettings,
    HostVersionCheck, KeyboardProfile, RawConfig, ReloadSettings, ThemeAssets, ThemeSelection,
    compile_yaml, merge_values,
};
pub use context::{
    AgentId, ClientId, ContextPath, ContextReference, ContextType, ContextValue, LinkHandlerId,
    OriginContext, OriginHostKind, OriginInvocationSource, OriginPaneType, PaneId, ServerId,
    SessionId, TabId, WorkspaceId, WorktreeId,
};
pub use diagnostic::{
    ConfigDiagnostic, DiagnosticCode, DiagnosticLabel, DiagnosticSeverity, SourceId, SourceSpan,
};
pub use execution::{
    AfterAction, ExecutionCapabilities, ExecutionMode, ExecutionPolicy, MenuControl,
    MenuControlAction, TimeoutAction,
};
pub use key::{
    CanonicalKey, EventKind, KeyCapabilities, KeyEvent, KeyIdentity, KeyIdentitySource,
    KeyParseError, LockModifiers, Modifiers, NamedKey,
};
pub use menu::{
    BindingConditions, BindingId, BindingLocation, BindingSettings, BindingView, CompiledBinding,
    CompiledGeneration, CompiledMenu, InlineMenuId, InvalidMenuName, LayoutPadding, LayoutSettings,
    LocalMenuAction, MenuId, MenuName, MenuView, MenuViewMenu, UiAttachmentView,
    ViewBindingSettings, ViewBindingState, binding_index, menu_view,
};
pub use session::{
    ExecutionId, MenuSession, MenuSessionEvent, MenuSessionInput, MenuSessionOutput,
    MenuSessionState, SessionInstant,
};
pub use theme::{
    Color, ColorScheme, CompiledTheme, REQUIRED_COMPONENT_TEMPLATES, Style, Theme, ThemePairError,
    ThemeSection, compiled_default_theme, default_color_scheme, default_theme,
    has_loader_backed_construct,
};
