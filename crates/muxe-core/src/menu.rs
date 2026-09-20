use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::action::{ActionSpec, MenuAction, MenuTarget, PortableAction};
use crate::condition::{ConditionEvaluationError, ConditionProgram, PagesContext};
use crate::config::{KeyboardProfile, ThemeSelection};
use crate::diagnostic::SourceSpan;
use crate::execution::{AfterAction, ExecutionPolicy, MenuControl};
use crate::key::CanonicalKey;
use crate::theme::CompiledTheme;

/// Monotonic broker-owned configuration generation. Binding IDs never cross this boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompiledGeneration(pub u64);

/// Validated user-chosen menu name.
///
/// The domain is non-empty with no NUL/control characters. Whitespace IS allowed:
/// quoted whitespace names (e.g. `"my menu"`) are a documented launcher feature.
/// Construction enforces the domain; there is no unchecked constructor.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MenuName(Arc<str>);

/// Error for a rejected menu name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidMenuName {
    /// Machine-readable reason: `empty`, `nul`, or `control`.
    pub reason: &'static str,
}

impl std::fmt::Display for InvalidMenuName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid menu name ({})", self.reason)
    }
}

impl std::error::Error for InvalidMenuName {}

impl MenuName {
    /// Parses a candidate name, enforcing the named-menu domain.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidMenuName`] for empty names or names containing
    /// NUL/control characters.
    pub fn parse(value: impl Into<Arc<str>>) -> Result<Self, InvalidMenuName> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidMenuName { reason: "empty" });
        }
        if value.contains('\0') {
            return Err(InvalidMenuName { reason: "nul" });
        }
        if value.chars().any(char::is_control) {
            return Err(InvalidMenuName { reason: "control" });
        }
        Ok(Self(value))
    }

    /// Parses a candidate name, mapping the rejection into a config diagnostic.
    ///
    /// # Errors
    ///
    /// Returns [`crate::diagnostic::ConfigDiagnostic`] naming the offending menu.
    pub fn parse_diagnostic(
        value: &str,
        span: crate::diagnostic::SourceSpan,
    ) -> Result<Self, crate::diagnostic::ConfigDiagnostic> {
        Self::parse(value).map_err(|error| {
            crate::diagnostic::ConfigDiagnostic::error(
                crate::diagnostic::DiagnosticCode::InvalidValue,
                format!("invalid menu name `{value}` ({})", error.reason),
                span,
            )
        })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Compiler-generated identity for one embedded submenu.
///
/// Structural, never a free-form string: the owning top-level [`MenuName`] plus
/// a single monotonic ordinal across the compilation unit, assigned in document
/// order.
/// It cannot equal a user-chosen name, so a named menu literally called
/// `main#0` never collides with the inline submenu of `main`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InlineMenuId {
    owner: MenuName,
    ordinal: u64,
}

impl InlineMenuId {
    #[must_use]
    pub fn new(owner: MenuName, ordinal: u64) -> Self {
        Self { owner, ordinal }
    }

    #[must_use]
    pub fn owner(&self) -> &MenuName {
        &self.owner
    }

    #[must_use]
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
}

/// Validated menu identity: either a user-chosen name or a compiler-generated
/// inline identity. The variants are distinct types, so a named menu can never
/// equal an inline submenu.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MenuId {
    Named(MenuName),
    Inline(InlineMenuId),
}

impl MenuId {
    #[must_use]
    pub fn named(name: MenuName) -> Self {
        Self::Named(name)
    }

    #[must_use]
    pub fn inline(id: InlineMenuId) -> Self {
        Self::Inline(id)
    }

    /// Returns the name for named identities, or `None` for inline ones.
    #[must_use]
    pub fn name(&self) -> Option<&MenuName> {
        match self {
            Self::Named(name) => Some(name),
            Self::Inline(_) => None,
        }
    }

    /// Returns the inline identity for inline identities, or `None` for named ones.
    #[must_use]
    pub fn inline_id(&self) -> Option<&InlineMenuId> {
        match self {
            Self::Named(_) => None,
            Self::Inline(id) => Some(id),
        }
    }

    /// Lossless display form: named identities render bare; inline identities
    /// render as `owner#ordinal`, which can never be re-parsed as a named
    /// identity (the wire contract keeps the variant explicit).
    #[must_use]
    pub fn display(&self) -> std::borrow::Cow<'_, str> {
        match self {
            Self::Named(name) => std::borrow::Cow::Borrowed(name.as_str()),
            Self::Inline(id) => {
                std::borrow::Cow::Owned(format!("{}#{}", id.owner.as_str(), id.ordinal))
            }
        }
    }
}

/// Opaque binding identity. It is valid only in its embedded configuration generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BindingId {
    generation: CompiledGeneration,
    ordinal: u64,
}

impl BindingId {
    #[must_use]
    pub const fn new(generation: CompiledGeneration, ordinal: u64) -> Self {
        Self {
            generation,
            ordinal,
        }
    }

    #[must_use]
    pub const fn generation(self) -> CompiledGeneration {
        self.generation
    }

    #[must_use]
    pub const fn ordinal(self) -> u64 {
        self.ordinal
    }
}

/// Compact location of the one authoritative compiled binding payload in its owning menu.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BindingLocation {
    pub menu: usize,
    pub binding: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingSettings {
    pub after_action: AfterAction,
    pub execution: ExecutionPolicy,
    pub repeat: Option<bool>,
}

/// Broker-authoritative compiled binding. This type retains payloads; [`MenuView`] does not.
#[derive(Clone, Debug, PartialEq)]
pub struct CompiledBinding {
    pub id: BindingId,
    pub key: CanonicalKey,
    pub label: Option<String>,
    pub hidden: bool,
    pub action: ActionSpec,
    pub action_span: SourceSpan,
    pub settings: BindingSettings,
    pub conditions: BindingConditions,
}

/// Parsed-once condition programs that a UI evaluates against pager geometry without reparsing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BindingConditions {
    pub include: Option<ConditionProgram>,
    pub enable: Option<ConditionProgram>,
    pub show: Option<ConditionProgram>,
}

impl BindingConditions {
    /// Evaluates the configured conditions for one pager state.
    ///
    /// # Errors
    ///
    /// Returns an error when a compiled condition cannot evaluate.
    pub fn evaluate(
        &self,
        pages: PagesContext,
    ) -> Result<ViewBindingState, ConditionEvaluationError> {
        Ok(ViewBindingState {
            included: self
                .include
                .as_ref()
                .map(|program| program.evaluate(pages))
                .transpose()?
                .unwrap_or(true),
            enabled: self
                .enable
                .as_ref()
                .map(|program| program.evaluate(pages))
                .transpose()?
                .unwrap_or(true),
            shown: self
                .show
                .as_ref()
                .map(|program| program.evaluate(pages))
                .transpose()?
                .unwrap_or(true),
            blocked: false,
        })
    }
}

/// Layout resolved from global and menu settings before a UI attachment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LayoutPadding {
    pub left: u16,
    pub right: u16,
    pub top: u16,
    pub bottom: u16,
    pub between_rows: u16,
    pub between_columns: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LayoutSettings {
    pub padding: LayoutPadding,
    pub max_item_title_length: u16,
}

impl Default for LayoutSettings {
    fn default() -> Self {
        Self {
            padding: LayoutPadding {
                left: 1,
                right: 1,
                top: 0,
                bottom: 0,
                between_rows: 0,
                between_columns: 3,
            },
            max_item_title_length: 24,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompiledMenu {
    pub id: MenuId,
    pub title: Option<String>,
    pub tags: Vec<String>,
    /// Effective inactivity timeout after global and menu settings are applied.
    pub inactivity_timeout: Option<Duration>,
    pub bindings: Vec<CompiledBinding>,
    pub layout: LayoutSettings,
}

/// Evaluated binding conditions and adapter compatibility. `included` removes an item before
/// layout; `shown` controls the menu bar only; `enabled` controls executability.
#[expect(
    clippy::struct_excessive_bools,
    reason = "the four independently serialized UI visibility states are part of the attachment contract"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewBindingState {
    pub included: bool,
    pub enabled: bool,
    pub shown: bool,
    pub blocked: bool,
}

impl ViewBindingState {
    pub const ENABLED: Self = Self {
        included: true,
        enabled: true,
        shown: true,
        blocked: false,
    };
}

/// Effective settings the UI requires for matching, navigation, and pending input behavior.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ViewBindingSettings {
    pub after_action: AfterAction,
    pub execution: ExecutionPolicy,
    pub repeat: Option<bool>,
}

impl From<&BindingSettings> for ViewBindingSettings {
    fn from(settings: &BindingSettings) -> Self {
        Self {
            after_action: settings.after_action,
            execution: settings.execution.clone(),
            repeat: settings.repeat,
        }
    }
}

/// The only action semantics exposed in a UI snapshot. It is action-payload-free and lets the UI
/// preserve caller-stack, pager, and pending-control behavior locally.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalMenuAction {
    Open { target: MenuId },
    Control(MenuControl),
    PagePrevious,
    PageNext,
}

/// Action-free binding information suitable for an immutable UI snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingView {
    pub id: BindingId,
    pub key: CanonicalKey,
    pub label: Option<String>,
    pub hidden: bool,
    pub state: ViewBindingState,
    pub settings: ViewBindingSettings,
    pub conditions: BindingConditions,
    pub local_menu_action: Option<LocalMenuAction>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MenuViewMenu {
    pub id: MenuId,
    pub title: Option<String>,
    pub layout: LayoutSettings,
    pub bindings: Vec<BindingView>,
}

/// Immutable complete menu graph for one UI attachment. It contains no action arguments or native
/// payloads. The broker remains authoritative when an ordinary binding is selected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MenuView {
    pub generation: CompiledGeneration,
    pub root: MenuId,
    pub menus: Vec<MenuViewMenu>,
}

impl MenuView {
    #[must_use]
    pub fn menu(&self, id: &MenuId) -> Option<&MenuViewMenu> {
        self.menus.iter().find(|menu| &menu.id == id)
    }

    #[must_use]
    pub fn binding(&self, id: BindingId) -> Option<&BindingView> {
        (id.generation() == self.generation)
            .then(|| {
                self.menus
                    .iter()
                    .flat_map(|menu| &menu.bindings)
                    .find(|binding| binding.id == id)
            })
            .flatten()
    }
}

/// Complete immutable attachment metadata. The UI receives no filesystem/config fallback: keyboard
/// negotiation, inactivity, per-menu layout, parsed conditions, and resolved theme pairing are all
/// carried here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UiAttachmentView {
    pub menu: MenuView,
    pub keyboard: KeyboardProfile,
    pub inactivity_timeout: Option<Duration>,
    pub theme_selection: ThemeSelection,
    pub theme: CompiledTheme,
}

impl CompiledMenu {
    pub fn view(&self) -> MenuViewMenu {
        MenuViewMenu {
            id: self.id.clone(),
            title: self.title.clone(),
            layout: self.layout,
            bindings: self.bindings.iter().map(CompiledBinding::view).collect(),
        }
    }
}

impl CompiledBinding {
    fn view(&self) -> BindingView {
        BindingView {
            id: self.id,
            key: self.key.clone(),
            label: self.label.clone(),
            hidden: self.hidden,
            state: ViewBindingState::ENABLED,
            settings: ViewBindingSettings::from(&self.settings),
            conditions: self.conditions.clone(),
            local_menu_action: local_menu_action(&self.action),
        }
    }
}

fn local_menu_action(action: &ActionSpec) -> Option<LocalMenuAction> {
    let ActionSpec::Portable(PortableAction::Menu(action)) = action else {
        return None;
    };
    Some(match action {
        MenuAction::Open(MenuTarget::Named(target)) => LocalMenuAction::Open {
            target: MenuId::named(target.clone()),
        },
        MenuAction::Open(MenuTarget::Inline(target)) => LocalMenuAction::Open {
            target: MenuId::inline(target.clone()),
        },
        MenuAction::Return => LocalMenuAction::Control(MenuControl::Return),
        MenuAction::Quit => LocalMenuAction::Control(MenuControl::Quit),
        MenuAction::PagePrev => LocalMenuAction::PagePrevious,
        MenuAction::PageNext => LocalMenuAction::PageNext,
    })
}

/// Builds a root-specific immutable UI snapshot from broker-authoritative compiled configuration.
#[must_use]
pub fn menu_view(
    generation: CompiledGeneration,
    root: &MenuId,
    menus: &[CompiledMenu],
) -> Option<MenuView> {
    menus.iter().any(|menu| &menu.id == root).then(|| MenuView {
        generation,
        root: root.clone(),
        menus: menus.iter().map(CompiledMenu::view).collect(),
    })
}

/// Indexes broker-authoritative bindings without duplicating action payloads.
#[must_use]
pub fn binding_index(menus: &[CompiledMenu]) -> BTreeMap<BindingId, BindingLocation> {
    menus
        .iter()
        .enumerate()
        .flat_map(|(menu, compiled)| {
            compiled
                .bindings
                .iter()
                .enumerate()
                .map(move |(binding, compiled_binding)| {
                    (compiled_binding.id, BindingLocation { menu, binding })
                })
        })
        .collect()
}
