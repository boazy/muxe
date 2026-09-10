use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use sha2::{Digest, Sha256};
use std::sync::LazyLock;
use thiserror::Error;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME_LEN: u32 = 8 * 1024 * 1024;
pub const MAX_CONTROL_FRAME_LEN: u32 = 64 * 1024;
/// Diagnostics are deliberately bounded; every other collection/string is bounded by its
/// enclosing frame rather than an invented protocol depth or item limit.
pub const MAX_DIAGNOSTIC_LEN: usize = 4 * 1024;
const RKYV_SERIALIZATION_PROFILE: &[u8] =
    b"rkyv=0.8.10;endian=little;alignment=aligned;pointer_width=32";
// This is the complete source that defines the archived DTO schema. Hashing it makes any
// schema edit change the fingerprint automatically; the fixed rkyv profile is hashed too.
const WIRE_SCHEMA_SOURCE: &[u8] = include_bytes!("wire.rs");
static APPLICATION_SCHEMA_FINGERPRINT: LazyLock<SchemaFingerprint> =
    LazyLock::new(|| fingerprint_for(WIRE_SCHEMA_SOURCE, RKYV_SERIALIZATION_PROFILE));

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
pub struct SchemaFingerprint(pub [u8; 32]);

impl SchemaFingerprint {
    pub const ZERO: Self = Self([0; 32]);

    #[must_use]
    pub fn application() -> Self {
        *APPLICATION_SCHEMA_FINGERPRINT
    }

    #[must_use]
    pub fn is_zero(self) -> bool {
        self == Self::ZERO
    }
}

fn fingerprint_for(schema: &[u8], profile: &[u8]) -> SchemaFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(profile);
    hasher.update(schema);
    SchemaFingerprint(hasher.finalize().into())
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum Codec {
    Rkyv = 1,
    ControlJsonV1 = 2,
}

impl Codec {
    #[must_use]
    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Rkyv),
            2 => Some(Self::ControlJsonV1),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum PeerRole {
    Ui = 1,
    Launcher = 2,
    Bridge = 3,
    ActivationCoordinator = 4,
    Broker = 5,
}

impl PeerRole {
    #[must_use]
    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Ui),
            2 => Some(Self::Launcher),
            3 => Some(Self::Bridge),
            4 => Some(Self::ActivationCoordinator),
            5 => Some(Self::Broker),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum HostKind {
    Zellij = 1,
    Herdr = 2,
}

macro_rules! string_id {
    ($name:ident) => {
        #[derive(
            Archive,
            Deserialize,
            Serialize,
            SerdeSerialize,
            SerdeDeserialize,
            Clone,
            Debug,
            PartialEq,
            Eq,
            Hash,
        )]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Validate for $name {
            fn validate(&self) -> Result<(), SemanticError> {
                validate_identifier(stringify!($name), &self.0)
            }
        }
    };
}

string_id!(UiSessionId);
string_id!(ModalScopeId);
string_id!(MenuId);
string_id!(HostPaneId);
string_id!(HostTabId);
string_id!(HostClientId);
string_id!(HostSessionId);
string_id!(ServerId);
string_id!(WorkspaceId);
string_id!(WorktreeId);
string_id!(AgentId);
string_id!(LinkHandlerId);

macro_rules! nonce_id {
    ($name:ident) => {
        #[derive(
            Archive,
            Deserialize,
            Serialize,
            SerdeSerialize,
            SerdeDeserialize,
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
            Hash,
        )]
        pub struct $name(pub [u8; 16]);

        impl $name {
            pub fn is_zero(self) -> bool {
                self.0 == [0; 16]
            }
        }

        impl Validate for $name {
            fn validate(&self) -> Result<(), SemanticError> {
                if self.is_zero() {
                    Err(SemanticError::ZeroNonce(stringify!($name)))
                } else {
                    Ok(())
                }
            }
        }
    };
}
nonce_id!(RequestId);
nonce_id!(EventId);
nonce_id!(PendingLaunchToken);
nonce_id!(ExecutionId);
nonce_id!(CaptureLeaseId);

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
pub struct BindingId {
    pub generation: u64,
    pub ordinal: u64,
}

impl Validate for BindingId {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_generation(self.generation)
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct LiveServerIdentity {
    pub host: HostKind,
    pub discovery_key: String,
    pub server_id: ServerId,
}

impl Validate for LiveServerIdentity {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_text("live server discovery key", &self.discovery_key)?;
        self.server_id.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct Hello {
    pub process_version: String,
    pub live_server: LiveServerIdentity,
}

impl Validate for Hello {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_text("process version", &self.process_version)?;
        self.live_server.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct Welcome {
    pub broker_version: String,
    pub live_server: LiveServerIdentity,
    pub accepted_frame_len: u32,
}

impl Validate for Welcome {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_text("broker version", &self.broker_version)?;
        self.live_server.validate()?;
        if self.accepted_frame_len == 0 || self.accepted_frame_len > MAX_FRAME_LEN {
            return Err(SemanticError::InvalidFrameLimit(self.accepted_frame_len));
        }
        Ok(())
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum MenuControl {
    Quit = 1,
    Return = 2,
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum AfterAction {
    Quit = 1,
    Return = 2,
    Stay = 3,
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum ExecutionMode {
    Await = 1,
    Detach = 2,
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum TimeoutAction {
    Detach = 1,
    Cancel = 2,
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum MenuControlAction {
    Detach = 1,
    Cancel = 2,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct ExecutionPolicyWire {
    pub mode: ExecutionMode,
    pub timeout_millis: Option<u64>,
    pub on_timeout: TimeoutAction,
    pub on_menu_control: MenuControlAction,
}

impl Validate for ExecutionPolicyWire {
    fn validate(&self) -> Result<(), SemanticError> {
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct BindingSettingsWire {
    pub after_action: AfterAction,
    pub execution: ExecutionPolicyWire,
    pub repeat: Option<bool>,
}

impl Validate for BindingSettingsWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.execution.validate()
    }
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "rkyv wire stability: field layout is part of the archived schema fingerprint"
)]
#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct BindingStateWire {
    pub included: bool,
    pub enabled: bool,
    pub shown: bool,
    pub blocked: bool,
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct PagesContextWire {
    pub count: u64,
    pub current: u64,
}

/// Closed, parse-once CEL lowering shared with `muxe-core::ConditionIr`.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum ConditionIrWire {
    Bool(bool),
    Integer(i64),
    PagesCount,
    PagesCurrent,
    Not(#[rkyv(omit_bounds)] Box<ConditionIrWire>),
    And(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    Or(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    Equal(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    NotEqual(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    Less(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    LessEqual(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    Greater(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
    GreaterEqual(
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
        #[rkyv(omit_bounds)] Box<ConditionIrWire>,
    ),
}

impl Validate for ConditionIrWire {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::Bool(_) | Self::Integer(_) | Self::PagesCount | Self::PagesCurrent => Ok(()),
            Self::Not(value) => value.validate(),
            Self::And(left, right)
            | Self::Or(left, right)
            | Self::Equal(left, right)
            | Self::NotEqual(left, right)
            | Self::Less(left, right)
            | Self::LessEqual(left, right)
            | Self::Greater(left, right)
            | Self::GreaterEqual(left, right) => {
                left.validate()?;
                right.validate()
            }
        }
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Default,
)]
pub struct BindingConditionsWire {
    pub include: Option<ConditionIrWire>,
    pub enable: Option<ConditionIrWire>,
    pub show: Option<ConditionIrWire>,
}

impl Validate for BindingConditionsWire {
    fn validate(&self) -> Result<(), SemanticError> {
        for condition in [&self.include, &self.enable, &self.show]
            .into_iter()
            .flatten()
        {
            condition.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum ConditionEvaluationErrorWire {
    #[error("condition expected a boolean value")]
    ExpectedBoolean,
    #[error("condition expected an integer value")]
    ExpectedInteger,
    #[error("condition result must be boolean")]
    NonBooleanResult,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConditionValueWire {
    Bool(bool),
    Integer(i64),
}

impl ConditionValueWire {
    fn boolean(self) -> Result<bool, ConditionEvaluationErrorWire> {
        match self {
            Self::Bool(value) => Ok(value),
            Self::Integer(_) => Err(ConditionEvaluationErrorWire::ExpectedBoolean),
        }
    }

    fn integer(self) -> Result<i64, ConditionEvaluationErrorWire> {
        match self {
            Self::Bool(_) => Err(ConditionEvaluationErrorWire::ExpectedInteger),
            Self::Integer(value) => Ok(value),
        }
    }
}

/// Evaluates a checked archived condition directly; no CEL parser or deserialized menu graph is
/// involved when page geometry changes.
/// # Errors
///
/// Returns the evaluation error when the checked condition is not boolean.
pub fn evaluate_archived_condition(
    condition: &ArchivedConditionIrWire,
    pages: PagesContextWire,
) -> Result<bool, ConditionEvaluationErrorWire> {
    match evaluate_archived_condition_value(condition, pages)? {
        ConditionValueWire::Bool(value) => Ok(value),
        ConditionValueWire::Integer(_) => Err(ConditionEvaluationErrorWire::NonBooleanResult),
    }
}

/// # Errors
///
/// Returns the evaluation error when a binding condition is not boolean.
pub fn evaluate_archived_binding_state(
    binding: &ArchivedBindingViewWire,
    pages: PagesContextWire,
) -> Result<BindingStateWire, ConditionEvaluationErrorWire> {
    let evaluate = |condition: Option<&ArchivedConditionIrWire>| {
        condition
            .map(|condition| evaluate_archived_condition(condition, pages))
            .transpose()
            .map(|value| value.unwrap_or(true))
    };
    Ok(BindingStateWire {
        included: evaluate(binding.conditions.include.as_ref())?,
        enabled: evaluate(binding.conditions.enable.as_ref())?,
        shown: evaluate(binding.conditions.show.as_ref())?,
        blocked: binding.state.blocked,
    })
}

fn evaluate_archived_condition_value(
    condition: &ArchivedConditionIrWire,
    pages: PagesContextWire,
) -> Result<ConditionValueWire, ConditionEvaluationErrorWire> {
    use ArchivedConditionIrWire as Ir;
    match condition {
        Ir::Bool(value) => Ok(ConditionValueWire::Bool(*value)),
        Ir::Integer(value) => Ok(ConditionValueWire::Integer(value.to_native())),
        Ir::PagesCount => Ok(ConditionValueWire::Integer(
            i64::try_from(pages.count).unwrap_or(i64::MAX),
        )),
        Ir::PagesCurrent => Ok(ConditionValueWire::Integer(
            i64::try_from(pages.current).unwrap_or(i64::MAX),
        )),
        Ir::Not(value) => Ok(ConditionValueWire::Bool(
            !evaluate_archived_condition_value(value.get(), pages)?.boolean()?,
        )),
        Ir::And(left, right) => {
            let left = evaluate_archived_condition_value(left.get(), pages)?.boolean()?;
            Ok(ConditionValueWire::Bool(
                left && evaluate_archived_condition_value(right.get(), pages)?.boolean()?,
            ))
        }
        Ir::Or(left, right) => {
            let left = evaluate_archived_condition_value(left.get(), pages)?.boolean()?;
            Ok(ConditionValueWire::Bool(
                left || evaluate_archived_condition_value(right.get(), pages)?.boolean()?,
            ))
        }
        Ir::Equal(left, right) => Ok(ConditionValueWire::Bool(
            evaluate_archived_condition_value(left.get(), pages)?
                == evaluate_archived_condition_value(right.get(), pages)?,
        )),
        Ir::NotEqual(left, right) => Ok(ConditionValueWire::Bool(
            evaluate_archived_condition_value(left.get(), pages)?
                != evaluate_archived_condition_value(right.get(), pages)?,
        )),
        Ir::Less(left, right) => {
            compare_archived_condition(left.get(), right.get(), pages, |a, b| a < b)
        }
        Ir::LessEqual(left, right) => {
            compare_archived_condition(left.get(), right.get(), pages, |a, b| a <= b)
        }
        Ir::Greater(left, right) => {
            compare_archived_condition(left.get(), right.get(), pages, |a, b| a > b)
        }
        Ir::GreaterEqual(left, right) => {
            compare_archived_condition(left.get(), right.get(), pages, |a, b| a >= b)
        }
    }
}

fn compare_archived_condition(
    left: &ArchivedConditionIrWire,
    right: &ArchivedConditionIrWire,
    pages: PagesContextWire,
    predicate: impl FnOnce(i64, i64) -> bool,
) -> Result<ConditionValueWire, ConditionEvaluationErrorWire> {
    let left = evaluate_archived_condition_value(left, pages)?.integer()?;
    let right = evaluate_archived_condition_value(right, pages)?.integer()?;
    Ok(ConditionValueWire::Bool(predicate(left, right)))
}
#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum BindingAvailability {
    Enabled = 1,
    Blocked = 2,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum LocalMenuActionWire {
    Open { target: MenuId },
    Control(MenuControl),
    PagePrevious,
    PageNext,
}

impl Validate for LocalMenuActionWire {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::Open { target } => target.validate(),
            Self::Control(_) | Self::PagePrevious | Self::PageNext => Ok(()),
        }
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct BindingViewWire {
    pub id: BindingId,
    pub key: String,
    pub label: Option<String>,
    pub hidden: bool,
    pub state: BindingStateWire,
    pub settings: BindingSettingsWire,
    pub conditions: BindingConditionsWire,
    pub local_menu_action: Option<LocalMenuActionWire>,
    pub diagnostic: Option<ProtocolDiagnostic>,
}

impl Validate for BindingViewWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.id.validate()?;
        validate_text("binding key", &self.key)?;
        if let Some(label) = &self.label {
            validate_clean_text("binding label", label)?;
        }
        if self.state.included && self.state.shown && !self.hidden {
            let label = self
                .label
                .as_deref()
                .ok_or(SemanticError::MissingVisibleLabel)?;
            validate_text("visible binding label", label)?;
        }
        self.settings.validate()?;
        self.conditions.validate()?;
        if let Some(action) = &self.local_menu_action {
            action.validate()?;
        }
        match (self.state.blocked, &self.diagnostic) {
            (false, Some(_)) => Err(SemanticError::UnexpectedDiagnostic),
            (true, None) => Err(SemanticError::MissingDiagnostic),
            (_, Some(diagnostic)) => diagnostic.validate(),
            (_, None) => Ok(()),
        }
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct LayoutPaddingWire {
    pub left: u16,
    pub right: u16,
    pub top: u16,
    pub bottom: u16,
    pub between_rows: u16,
    pub between_columns: u16,
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct LayoutSettingsWire {
    pub padding: LayoutPaddingWire,
    pub max_item_title_length: u16,
}

impl Validate for LayoutSettingsWire {
    fn validate(&self) -> Result<(), SemanticError> {
        (self.max_item_title_length > 0)
            .then_some(())
            .ok_or(SemanticError::ZeroLayoutTitleLength)
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct MenuViewMenuWire {
    pub id: MenuId,
    /// `None` is semantically distinct from an empty title: selectors and the UI do not fall
    /// back to the menu ID for an absent title.
    pub title: Option<String>,
    pub layout: LayoutSettingsWire,
    pub bindings: Vec<BindingViewWire>,
}

impl Validate for MenuViewMenuWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.id.validate()?;
        self.layout.validate()?;
        if let Some(title) = &self.title {
            validate_clean_text("menu title", title)?;
        }
        for binding in &self.bindings {
            binding.validate()?;
        }
        Ok(())
    }
}

/// Immutable, action-payload-free graph retained by a UI as a checked archive.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct MenuViewWire {
    pub generation: u64,
    pub root: MenuId,
    pub menus: Vec<MenuViewMenuWire>,
}

impl Validate for MenuViewWire {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_generation(self.generation)?;
        self.root.validate()?;
        let mut root_found = false;
        for (menu_index, menu) in self.menus.iter().enumerate() {
            menu.validate()?;
            if menu.id == self.root {
                root_found = true;
            }
            if self.menus[..menu_index]
                .iter()
                .any(|prior| prior.id == menu.id)
            {
                return Err(SemanticError::DuplicateMenu);
            }
            for binding in &menu.bindings {
                if binding.id.generation != self.generation {
                    return Err(SemanticError::BindingGenerationMismatch);
                }
                if let Some(LocalMenuActionWire::Open { target }) = &binding.local_menu_action
                    && !self.menus.iter().any(|candidate| candidate.id == *target)
                {
                    return Err(SemanticError::UnknownMenuTarget);
                }
            }
        }
        root_found
            .then_some(())
            .ok_or(SemanticError::MissingRootMenu)
    }
}
#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct KeyCapabilitiesWire {
    pub event_types: bool,
    pub alternate_keys: bool,
    pub all_keys_as_escape_codes: bool,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum KeyboardProfileWire {
    Vt100 { escape_timeout_millis: u64 },
    Kitty(KeyCapabilitiesWire),
}

impl Validate for KeyboardProfileWire {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::Vt100 {
                escape_timeout_millis,
            } if *escape_timeout_millis == 0 => Err(SemanticError::ZeroEscapeTimeout),
            Self::Vt100 { .. } | Self::Kitty(_) => Ok(()),
        }
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct NamedStringWire {
    pub name: String,
    pub value: String,
}

impl Validate for NamedStringWire {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_identifier("render-model name", &self.name)?;
        validate_clean_text("render-model value", &self.value)
    }
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "rkyv wire stability: field layout is part of the archived schema fingerprint"
)]
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct StyleWire {
    pub foreground: Option<String>,
    pub background: Option<String>,
    pub bold: bool,
    pub dim: bool,
    pub italic: bool,
    pub underline: bool,
    pub strikethrough: bool,
}

impl Validate for StyleWire {
    fn validate(&self) -> Result<(), SemanticError> {
        for color in [&self.foreground, &self.background].into_iter().flatten() {
            validate_clean_text("style color", color)?;
        }
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct NamedStyleWire {
    pub name: String,
    pub style: StyleWire,
}

impl Validate for NamedStyleWire {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_identifier("style name", &self.name)?;
        self.style.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct ThemeSectionWire {
    pub styles: Vec<NamedStyleWire>,
    pub templates: Vec<NamedStringWire>,
}

impl Validate for ThemeSectionWire {
    fn validate(&self) -> Result<(), SemanticError> {
        for style in &self.styles {
            style.validate()?;
        }
        for template in &self.templates {
            template.validate()?;
        }
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct ColorSchemeWire {
    pub title: String,
    pub palette: Vec<NamedStringWire>,
    pub colors: Vec<NamedStringWire>,
}

impl Validate for ColorSchemeWire {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_clean_text("color-scheme title", &self.title)?;
        for entry in self.palette.iter().chain(&self.colors) {
            entry.validate()?;
        }
        Ok(())
    }
}

/// Fully supplied, prevalidated theme/scheme input. The UI reads no theme or color files.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct CompiledThemeWire {
    pub common: ThemeSectionWire,
    pub menu: ThemeSectionWire,
    pub settings: Vec<NamedStringWire>,
    pub scheme: ColorSchemeWire,
}

impl Validate for CompiledThemeWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.common.validate()?;
        self.menu.validate()?;
        for setting in &self.settings {
            setting.validate()?;
        }
        self.scheme.validate()
    }
}

/// Complete, action-payload-free attachment snapshot retained as an aligned archive by the UI.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct UiAttachmentWire {
    pub menu: MenuViewWire,
    pub keyboard: KeyboardProfileWire,
    pub inactivity_timeout_millis: Option<u64>,
    pub theme: CompiledThemeWire,
}

impl Validate for UiAttachmentWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.menu.validate()?;
        self.keyboard.validate()?;
        self.theme.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct PrepareUiLaunch {
    pub modal_scope: ModalScopeId,
    pub root: MenuId,
    pub lease_millis: u32,
}

impl Validate for PrepareUiLaunch {
    fn validate(&self) -> Result<(), SemanticError> {
        self.modal_scope.validate()?;
        self.root.validate()?;
        if self.lease_millis == 0 {
            return Err(SemanticError::ZeroLease);
        }
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct RegisterPendingPane {
    pub token: PendingLaunchToken,
    pub pane: HostPaneId,
    pub temporary_tab: Option<HostTabId>,
}

impl Validate for RegisterPendingPane {
    fn validate(&self) -> Result<(), SemanticError> {
        self.token.validate()?;
        self.pane.validate()?;
        if let Some(tab) = &self.temporary_tab {
            tab.validate()?;
        }
        Ok(())
    }
}

/// Immutable launcher-captured origin supplied by a UI process. The broker converts it only at
/// the concrete adapter boundary; it never substitutes current focus or process environment.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct UiOriginBootstrap {
    pub workspace: WorkspaceId,
    pub tab: HostTabId,
    pub pane: HostPaneId,
    pub cwd: Option<String>,
}

impl Validate for UiOriginBootstrap {
    fn validate(&self) -> Result<(), SemanticError> {
        self.workspace.validate()?;
        self.tab.validate()?;
        self.pane.validate()?;
        if let Some(cwd) = &self.cwd {
            validate_absolute_path("origin cwd", cwd)?;
        }
        Ok(())
    }
}

/// The attaching UI pane's independently reported identity. It is checked against the host's
/// fresh snapshot and is never used as a fallback origin.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct UiCallerIdentityWire {
    pub workspace: WorkspaceId,
    pub tab: HostTabId,
    pub pane: HostPaneId,
    pub cwd: String,
}

impl Validate for UiCallerIdentityWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.workspace.validate()?;
        self.tab.validate()?;
        self.pane.validate()?;
        validate_absolute_path("caller cwd", &self.cwd)
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct AttachUi {
    pub root: MenuId,
    pub pane: HostPaneId,
    pub pending_launch: Option<PendingLaunchToken>,
    pub origin: Option<UiOriginBootstrap>,
    pub caller_identity: Option<UiCallerIdentityWire>,
    pub theme: Option<String>,
    pub color_scheme: Option<String>,
}

impl Validate for AttachUi {
    fn validate(&self) -> Result<(), SemanticError> {
        self.root.validate()?;
        self.pane.validate()?;
        if let Some(token) = self.pending_launch {
            token.validate()?;
        }
        if let Some(origin) = &self.origin {
            origin.validate()?;
        }
        if let Some(caller_identity) = &self.caller_identity {
            caller_identity.validate()?;
        }
        if let Some(theme) = &self.theme {
            validate_identifier("theme override", theme)?;
        }
        if let Some(color_scheme) = &self.color_scheme {
            validate_identifier("color scheme override", color_scheme)?;
        }
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct CommitUiLaunch {
    pub token: PendingLaunchToken,
    pub pane: HostPaneId,
}

impl Validate for CommitUiLaunch {
    fn validate(&self) -> Result<(), SemanticError> {
        self.token.validate()?;
        self.pane.validate()
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
pub struct AbortUiLaunch {
    pub token: PendingLaunchToken,
}

impl Validate for AbortUiLaunch {
    fn validate(&self) -> Result<(), SemanticError> {
        self.token.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct InvokeBinding {
    pub session: UiSessionId,
    pub generation: u64,
    pub binding: BindingId,
}

impl Validate for InvokeBinding {
    fn validate(&self) -> Result<(), SemanticError> {
        self.session.validate()?;
        validate_generation(self.generation)?;
        self.binding.validate()?;
        if self.binding.generation != self.generation {
            return Err(SemanticError::BindingGenerationMismatch);
        }
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct UiMenuControl {
    pub session: UiSessionId,
    pub control: MenuControl,
}

impl Validate for UiMenuControl {
    fn validate(&self) -> Result<(), SemanticError> {
        self.session.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct DetachUi {
    pub session: UiSessionId,
}

impl Validate for DetachUi {
    fn validate(&self) -> Result<(), SemanticError> {
        self.session.validate()
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "rkyv wire stability: variant layout is part of the archived schema fingerprint"
)]
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum ClientRequest {
    PrepareUiLaunch(PrepareUiLaunch),
    RegisterPendingPane(RegisterPendingPane),
    AttachUi(AttachUi),
    CommitUiLaunch(CommitUiLaunch),
    AbortUiLaunch(AbortUiLaunch),
    InvokeBinding(InvokeBinding),
    MenuControl(UiMenuControl),
    DetachUi(DetachUi),
    Heartbeat,
}

impl ClientRequest {
    #[must_use]
    pub const fn allowed_for(&self, role: PeerRole) -> bool {
        match role {
            PeerRole::Launcher => matches!(
                self,
                Self::PrepareUiLaunch(_)
                    | Self::RegisterPendingPane(_)
                    | Self::CommitUiLaunch(_)
                    | Self::AbortUiLaunch(_)
                    | Self::Heartbeat
            ),
            PeerRole::Ui => matches!(
                self,
                Self::AttachUi(_)
                    | Self::InvokeBinding(_)
                    | Self::MenuControl(_)
                    | Self::DetachUi(_)
                    | Self::Heartbeat
            ),
            PeerRole::Bridge | PeerRole::ActivationCoordinator | PeerRole::Broker => false,
        }
    }
}

impl Validate for ClientRequest {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::PrepareUiLaunch(value) => value.validate(),
            Self::RegisterPendingPane(value) => value.validate(),
            Self::AttachUi(value) => value.validate(),
            Self::CommitUiLaunch(value) => value.validate(),
            Self::AbortUiLaunch(value) => value.validate(),
            Self::InvokeBinding(value) => value.validate(),
            Self::MenuControl(value) => value.validate(),
            Self::DetachUi(value) => value.validate(),
            Self::Heartbeat => Ok(()),
        }
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum DiagnosticCode {
    InvalidRequest = 1,
    LaunchAborted = 2,
    StaleGeneration = 3,
    ContextUnavailable = 4,
    ActionBlocked = 5,
    ActivationInProgress = 6,
    HostUnavailable = 7,
    OutcomeUnknown = 8,
    ProtocolViolation = 9,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct ProtocolDiagnostic {
    pub code: DiagnosticCode,
    pub message: String,
}

impl Validate for ProtocolDiagnostic {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_diagnostic("protocol diagnostic", &self.message)
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum InvocationDisposition {
    Awaited = 1,
    Detached = 2,
}

#[expect(
    clippy::large_enum_variant,
    reason = "rkyv wire stability: variant layout is part of the archived schema fingerprint"
)]
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum BrokerResponse {
    LaunchPrepared {
        token: PendingLaunchToken,
        lease_millis: u32,
    },
    PendingPaneRegistered,
    AttachPending,
    UiAttached {
        session: UiSessionId,
        snapshot: UiAttachmentWire,
    },
    InvocationAccepted {
        execution: ExecutionId,
        disposition: InvocationDisposition,
    },
    PendingControlCompleted {
        execution: ExecutionId,
        control: MenuControl,
    },
    Detached,
    Acknowledged,
    Error(ProtocolDiagnostic),
}

impl Validate for BrokerResponse {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::LaunchPrepared {
                token,
                lease_millis,
            } => {
                token.validate()?;
                if *lease_millis == 0 {
                    return Err(SemanticError::ZeroLease);
                }
                Ok(())
            }
            Self::UiAttached { session, snapshot } => {
                session.validate()?;
                snapshot.validate()
            }
            Self::InvocationAccepted { execution, .. }
            | Self::PendingControlCompleted { execution, .. } => execution.validate(),
            Self::Error(error) => error.validate(),
            Self::PendingPaneRegistered
            | Self::AttachPending
            | Self::Detached
            | Self::Acknowledged => Ok(()),
        }
    }
}

#[derive(
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
)]
#[repr(u8)]
pub enum ExecutionOutcome {
    Succeeded = 1,
    Failed = 2,
    Cancelled = 3,
    TimedOut = 4,
    Detached = 5,
    OutcomeUnknown = 6,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum BrokerEvent {
    ExecutionCompleted {
        session: UiSessionId,
        execution: ExecutionId,
        outcome: ExecutionOutcome,
        diagnostic: Option<ProtocolDiagnostic>,
    },
    BindingAvailabilityChanged {
        session: UiSessionId,
        generation: u64,
        binding: BindingId,
        availability: BindingAvailability,
        diagnostic: Option<ProtocolDiagnostic>,
    },
    AdapterHealthChanged {
        healthy: bool,
        diagnostic: Option<ProtocolDiagnostic>,
    },
    BrokerRetiring,
    Fatal(ProtocolDiagnostic),
}

impl Validate for BrokerEvent {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::ExecutionCompleted {
                session,
                execution,
                diagnostic,
                ..
            } => {
                session.validate()?;
                execution.validate()?;
                validate_optional(diagnostic.as_ref())
            }
            Self::BindingAvailabilityChanged {
                session,
                generation,
                binding,
                availability,
                diagnostic,
            } => {
                session.validate()?;
                validate_generation(*generation)?;
                binding.validate()?;
                match (availability, diagnostic) {
                    (BindingAvailability::Enabled, Some(_)) => {
                        Err(SemanticError::UnexpectedDiagnostic)
                    }
                    (BindingAvailability::Blocked, None) => Err(SemanticError::MissingDiagnostic),
                    (_, Some(value)) => value.validate(),
                    (_, None) => Ok(()),
                }
            }
            Self::AdapterHealthChanged { diagnostic, .. } => validate_optional(diagnostic.as_ref()),
            Self::Fatal(diagnostic) => diagnostic.validate(),
            Self::BrokerRetiring => Ok(()),
        }
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum WireMessage {
    Hello {
        request_id: RequestId,
        hello: Hello,
    },
    Welcome {
        request_id: RequestId,
        welcome: Welcome,
    },
    Request {
        request_id: RequestId,
        request: ClientRequest,
    },
    Response {
        request_id: RequestId,
        response: BrokerResponse,
    },
    Event {
        event_id: EventId,
        event: BrokerEvent,
    },
}

impl WireMessage {
    /// # Errors
    ///
    /// Returns `SemanticError` on illegal role, direction, phase, or request content.
    pub fn validate_for_peer(
        &self,
        role: PeerRole,
        direction: MessageDirection,
        phase: ConnectionPhase,
    ) -> Result<ConnectionPhase, SemanticError> {
        match (direction, phase, self) {
            (
                MessageDirection::PeerToBroker,
                ConnectionPhase::AwaitingHello,
                Self::Hello { request_id, hello },
            ) => {
                request_id.validate()?;
                hello.validate()?;
                Ok(ConnectionPhase::Ready)
            }
            (
                MessageDirection::BrokerToPeer,
                ConnectionPhase::AwaitingWelcome,
                Self::Welcome {
                    request_id,
                    welcome,
                },
            ) => {
                request_id.validate()?;
                welcome.validate()?;
                Ok(ConnectionPhase::Ready)
            }
            (
                MessageDirection::PeerToBroker,
                ConnectionPhase::Ready,
                Self::Request {
                    request_id,
                    request,
                },
            ) => {
                request_id.validate()?;
                request.validate()?;
                if !request.allowed_for(role) {
                    return Err(SemanticError::IllegalMessageForRole { role });
                }
                Ok(ConnectionPhase::Ready)
            }
            (
                MessageDirection::BrokerToPeer,
                ConnectionPhase::Ready,
                Self::Response {
                    request_id,
                    response,
                },
            ) => {
                request_id.validate()?;
                response.validate()?;
                Ok(ConnectionPhase::Ready)
            }
            (
                MessageDirection::BrokerToPeer,
                ConnectionPhase::Ready,
                Self::Event { event_id, event },
            ) => {
                event_id.validate()?;
                event.validate()?;
                Ok(ConnectionPhase::Ready)
            }
            _ => Err(SemanticError::IllegalSequence { role, phase }),
        }
    }
}

/// Validates message direction, handshake order, and role legality directly against an already
/// checked archive. This deliberately does not deserialize a retained UI view.
/// # Errors
///
/// Returns `SemanticError` on illegal role, direction, phase, or archived content.
pub fn validate_archived_wire_message(
    message: &ArchivedWireMessage,
    role: PeerRole,
    direction: MessageDirection,
    phase: ConnectionPhase,
) -> Result<ConnectionPhase, SemanticError> {
    match (direction, phase, message) {
        (
            MessageDirection::PeerToBroker,
            ConnectionPhase::AwaitingHello,
            ArchivedWireMessage::Hello { request_id, hello },
        ) => {
            validate_archived_nonce(&request_id.0, "RequestId")?;
            validate_archived_text("process version", hello.process_version.as_str(), true)?;
            validate_archived_text(
                "live server discovery key",
                hello.live_server.discovery_key.as_str(),
                true,
            )?;
            validate_archived_identifier("ServerId", hello.live_server.server_id.0.as_str())?;
            Ok(ConnectionPhase::Ready)
        }
        (
            MessageDirection::BrokerToPeer,
            ConnectionPhase::AwaitingWelcome,
            ArchivedWireMessage::Welcome {
                request_id,
                welcome,
            },
        ) => {
            validate_archived_nonce(&request_id.0, "RequestId")?;
            validate_archived_text("broker version", welcome.broker_version.as_str(), true)?;
            validate_archived_text(
                "live server discovery key",
                welcome.live_server.discovery_key.as_str(),
                true,
            )?;
            validate_archived_identifier("ServerId", welcome.live_server.server_id.0.as_str())?;
            (welcome.accepted_frame_len.to_native() > 0
                && welcome.accepted_frame_len.to_native() <= MAX_FRAME_LEN)
                .then_some(())
                .ok_or(SemanticError::InvalidFrameLimit(
                    welcome.accepted_frame_len.to_native(),
                ))?;
            Ok(ConnectionPhase::Ready)
        }
        (
            MessageDirection::PeerToBroker,
            ConnectionPhase::Ready,
            ArchivedWireMessage::Request {
                request_id,
                request,
            },
        ) if archived_request_allowed_for(request, role) => {
            validate_archived_nonce(&request_id.0, "RequestId")?;
            validate_archived_request(request)?;
            Ok(ConnectionPhase::Ready)
        }
        (
            MessageDirection::BrokerToPeer,
            ConnectionPhase::Ready,
            ArchivedWireMessage::Response {
                request_id,
                response,
            },
        ) => {
            validate_archived_nonce(&request_id.0, "RequestId")?;
            validate_archived_response(response)?;
            Ok(ConnectionPhase::Ready)
        }
        (
            MessageDirection::BrokerToPeer,
            ConnectionPhase::Ready,
            ArchivedWireMessage::Event { event_id, event },
        ) => {
            validate_archived_nonce(&event_id.0, "EventId")?;
            validate_archived_event(event)?;
            Ok(ConnectionPhase::Ready)
        }
        (
            MessageDirection::PeerToBroker,
            ConnectionPhase::Ready,
            ArchivedWireMessage::Request { .. },
        ) => Err(SemanticError::IllegalMessageForRole { role }),
        _ => Err(SemanticError::IllegalSequence { role, phase }),
    }
}

fn archived_request_allowed_for(request: &ArchivedClientRequest, role: PeerRole) -> bool {
    match role {
        PeerRole::Launcher => matches!(
            request,
            ArchivedClientRequest::PrepareUiLaunch(_)
                | ArchivedClientRequest::RegisterPendingPane(_)
                | ArchivedClientRequest::CommitUiLaunch(_)
                | ArchivedClientRequest::AbortUiLaunch(_)
                | ArchivedClientRequest::Heartbeat
        ),
        PeerRole::Ui => matches!(
            request,
            ArchivedClientRequest::AttachUi(_)
                | ArchivedClientRequest::InvokeBinding(_)
                | ArchivedClientRequest::MenuControl(_)
                | ArchivedClientRequest::DetachUi(_)
                | ArchivedClientRequest::Heartbeat
        ),
        PeerRole::Bridge | PeerRole::ActivationCoordinator | PeerRole::Broker => false,
    }
}

fn validate_archived_nonce(value: &[u8; 16], name: &'static str) -> Result<(), SemanticError> {
    (value != &[0; 16])
        .then_some(())
        .ok_or(SemanticError::ZeroNonce(name))
}

fn validate_archived_identifier(field: &'static str, value: &str) -> Result<(), SemanticError> {
    validate_archived_text(field, value, true)?;
    if value.chars().any(char::is_whitespace) {
        return Err(SemanticError::Whitespace { field });
    }
    Ok(())
}

fn validate_archived_text(
    field: &'static str,
    value: &str,
    required: bool,
) -> Result<(), SemanticError> {
    if required && value.is_empty() {
        return Err(SemanticError::Empty { field });
    }
    if value.contains('\0') {
        return Err(SemanticError::Nul { field });
    }
    if value.chars().any(char::is_control) {
        return Err(SemanticError::Control { field });
    }
    Ok(())
}

fn validate_archived_request(request: &ArchivedClientRequest) -> Result<(), SemanticError> {
    match request {
        ArchivedClientRequest::PrepareUiLaunch(value) => {
            validate_archived_identifier("ModalScopeId", value.modal_scope.0.as_str())?;
            validate_archived_identifier("MenuId", value.root.0.as_str())?;
            (value.lease_millis.to_native() > 0)
                .then_some(())
                .ok_or(SemanticError::ZeroLease)
        }
        ArchivedClientRequest::RegisterPendingPane(value) => {
            validate_archived_nonce(&value.token.0, "PendingLaunchToken")?;
            validate_archived_identifier("HostPaneId", value.pane.0.as_str())?;
            if let Some(tab) = value.temporary_tab.as_ref() {
                validate_archived_identifier("HostTabId", tab.0.as_str())?;
            }
            Ok(())
        }
        ArchivedClientRequest::AttachUi(value) => {
            validate_archived_identifier("MenuId", value.root.0.as_str())?;
            validate_archived_identifier("HostPaneId", value.pane.0.as_str())?;
            if let Some(token) = value.pending_launch.as_ref() {
                validate_archived_nonce(&token.0, "PendingLaunchToken")?;
            }
            if let Some(origin) = value.origin.as_ref() {
                validate_archived_origin_bootstrap(origin)?;
            }
            if let Some(caller_identity) = value.caller_identity.as_ref() {
                validate_archived_caller_identity(caller_identity)?;
            }
            if let Some(theme) = value.theme.as_ref() {
                validate_archived_identifier("theme override", theme.as_str())?;
            }
            if let Some(color_scheme) = value.color_scheme.as_ref() {
                validate_archived_identifier("color scheme override", color_scheme.as_str())?;
            }
            Ok(())
        }
        ArchivedClientRequest::CommitUiLaunch(value) => {
            validate_archived_nonce(&value.token.0, "PendingLaunchToken")?;
            validate_archived_identifier("HostPaneId", value.pane.0.as_str())
        }
        ArchivedClientRequest::AbortUiLaunch(value) => {
            validate_archived_nonce(&value.token.0, "PendingLaunchToken")
        }
        ArchivedClientRequest::InvokeBinding(value) => {
            validate_archived_identifier("UiSessionId", value.session.0.as_str())?;
            let generation = value.generation.to_native();
            (generation > 0)
                .then_some(())
                .ok_or(SemanticError::ZeroGeneration)?;
            let binding_generation = value.binding.generation.to_native();
            (binding_generation == generation)
                .then_some(())
                .ok_or(SemanticError::BindingGenerationMismatch)
        }
        ArchivedClientRequest::MenuControl(value) => {
            validate_archived_identifier("UiSessionId", value.session.0.as_str())
        }
        ArchivedClientRequest::DetachUi(value) => {
            validate_archived_identifier("UiSessionId", value.session.0.as_str())
        }
        ArchivedClientRequest::Heartbeat => Ok(()),
    }
}

fn validate_archived_response(response: &ArchivedBrokerResponse) -> Result<(), SemanticError> {
    match response {
        ArchivedBrokerResponse::LaunchPrepared {
            token,
            lease_millis,
        } => {
            validate_archived_nonce(&token.0, "PendingLaunchToken")?;
            (lease_millis.to_native() > 0)
                .then_some(())
                .ok_or(SemanticError::ZeroLease)
        }
        ArchivedBrokerResponse::UiAttached { session, snapshot } => {
            validate_archived_identifier("UiSessionId", session.0.as_str())?;
            validate_archived_attachment(snapshot)
        }
        ArchivedBrokerResponse::InvocationAccepted { execution, .. }
        | ArchivedBrokerResponse::PendingControlCompleted { execution, .. } => {
            validate_archived_nonce(&execution.0, "ExecutionId")
        }
        ArchivedBrokerResponse::Error(diagnostic) => validate_archived_diagnostic(diagnostic),
        ArchivedBrokerResponse::PendingPaneRegistered
        | ArchivedBrokerResponse::AttachPending
        | ArchivedBrokerResponse::Detached
        | ArchivedBrokerResponse::Acknowledged => Ok(()),
    }
}

fn validate_archived_event(event: &ArchivedBrokerEvent) -> Result<(), SemanticError> {
    match event {
        ArchivedBrokerEvent::ExecutionCompleted {
            session,
            execution,
            diagnostic,
            ..
        } => {
            validate_archived_identifier("UiSessionId", session.0.as_str())?;
            validate_archived_nonce(&execution.0, "ExecutionId")?;
            validate_archived_optional_diagnostic(diagnostic.as_ref())
        }
        ArchivedBrokerEvent::BindingAvailabilityChanged {
            session,
            generation,
            binding,
            availability,
            diagnostic,
        } => {
            validate_archived_identifier("UiSessionId", session.0.as_str())?;
            let generation = generation.to_native();
            validate_archived_generation(generation)?;
            validate_archived_generation(binding.generation.to_native())?;
            if binding.generation.to_native() != generation {
                return Err(SemanticError::BindingGenerationMismatch);
            }
            match (availability, diagnostic.as_ref()) {
                (ArchivedBindingAvailability::Enabled, Some(_)) => {
                    Err(SemanticError::UnexpectedDiagnostic)
                }
                (ArchivedBindingAvailability::Blocked, None) => {
                    Err(SemanticError::MissingDiagnostic)
                }
                (_, Some(diagnostic)) => validate_archived_diagnostic(diagnostic),
                (_, None) => Ok(()),
            }
        }
        ArchivedBrokerEvent::AdapterHealthChanged { diagnostic, .. } => {
            validate_archived_optional_diagnostic(diagnostic.as_ref())
        }
        ArchivedBrokerEvent::Fatal(diagnostic) => validate_archived_diagnostic(diagnostic),
        ArchivedBrokerEvent::BrokerRetiring => Ok(()),
    }
}

fn validate_archived_attachment(value: &ArchivedUiAttachmentWire) -> Result<(), SemanticError> {
    validate_archived_menu_view(&value.menu)?;
    validate_archived_keyboard(&value.keyboard)?;
    validate_archived_theme(&value.theme)
}

fn validate_archived_menu_view(value: &ArchivedMenuViewWire) -> Result<(), SemanticError> {
    let generation = value.generation.to_native();
    validate_archived_generation(generation)?;
    validate_archived_identifier("MenuId", value.root.0.as_str())?;
    let mut root_found = false;
    for (index, menu) in value.menus.iter().enumerate() {
        validate_archived_menu(menu)?;
        if menu.id.0.as_str() == value.root.0.as_str() {
            root_found = true;
        }
        if value
            .menus
            .iter()
            .take(index)
            .any(|prior| prior.id.0.as_str() == menu.id.0.as_str())
        {
            return Err(SemanticError::DuplicateMenu);
        }
        for binding in menu.bindings.iter() {
            if binding.id.generation.to_native() != generation {
                return Err(SemanticError::BindingGenerationMismatch);
            }
            if let Some(ArchivedLocalMenuActionWire::Open { target }) =
                binding.local_menu_action.as_ref()
                && !value
                    .menus
                    .iter()
                    .any(|candidate| candidate.id.0.as_str() == target.0.as_str())
            {
                return Err(SemanticError::UnknownMenuTarget);
            }
        }
    }
    root_found
        .then_some(())
        .ok_or(SemanticError::MissingRootMenu)
}

fn validate_archived_menu(value: &ArchivedMenuViewMenuWire) -> Result<(), SemanticError> {
    validate_archived_identifier("MenuId", value.id.0.as_str())?;
    if value.layout.max_item_title_length.to_native() == 0 {
        return Err(SemanticError::ZeroLayoutTitleLength);
    }
    if let Some(title) = value.title.as_ref() {
        validate_archived_clean_text("menu title", title.as_str())?;
    }
    for binding in value.bindings.iter() {
        validate_archived_binding(binding)?;
    }
    Ok(())
}

fn validate_archived_binding(value: &ArchivedBindingViewWire) -> Result<(), SemanticError> {
    validate_archived_generation(value.id.generation.to_native())?;
    validate_archived_text("binding key", value.key.as_str(), true)?;
    if let Some(label) = value.label.as_ref() {
        validate_archived_clean_text("binding label", label.as_str())?;
    }
    if value.state.included && value.state.shown && !value.hidden {
        let label = value
            .label
            .as_ref()
            .ok_or(SemanticError::MissingVisibleLabel)?;
        validate_archived_text("visible binding label", label.as_str(), true)?;
    }
    if let Some(action) = value.local_menu_action.as_ref() {
        validate_archived_local_action(action)?;
    }
    match (value.state.blocked, value.diagnostic.as_ref()) {
        (false, Some(_)) => Err(SemanticError::UnexpectedDiagnostic),
        (true, None) => Err(SemanticError::MissingDiagnostic),
        (_, Some(diagnostic)) => validate_archived_diagnostic(diagnostic),
        (_, None) => Ok(()),
    }
}

fn validate_archived_local_action(
    value: &ArchivedLocalMenuActionWire,
) -> Result<(), SemanticError> {
    match value {
        ArchivedLocalMenuActionWire::Open { target } => {
            validate_archived_identifier("MenuId", target.0.as_str())
        }
        ArchivedLocalMenuActionWire::Control(_)
        | ArchivedLocalMenuActionWire::PagePrevious
        | ArchivedLocalMenuActionWire::PageNext => Ok(()),
    }
}

fn validate_archived_keyboard(value: &ArchivedKeyboardProfileWire) -> Result<(), SemanticError> {
    match value {
        ArchivedKeyboardProfileWire::Vt100 {
            escape_timeout_millis,
        } if escape_timeout_millis.to_native() == 0 => Err(SemanticError::ZeroEscapeTimeout),
        ArchivedKeyboardProfileWire::Vt100 { .. } | ArchivedKeyboardProfileWire::Kitty(_) => Ok(()),
    }
}

fn validate_archived_theme(value: &ArchivedCompiledThemeWire) -> Result<(), SemanticError> {
    validate_archived_theme_section(&value.common)?;
    validate_archived_theme_section(&value.menu)?;
    for setting in value.settings.iter() {
        validate_archived_named_string(setting)?;
    }
    validate_archived_color_scheme(&value.scheme)
}

fn validate_archived_theme_section(value: &ArchivedThemeSectionWire) -> Result<(), SemanticError> {
    for style in value.styles.iter() {
        validate_archived_named_style(style)?;
    }
    for template in value.templates.iter() {
        validate_archived_named_string(template)?;
    }
    Ok(())
}

fn validate_archived_named_style(value: &ArchivedNamedStyleWire) -> Result<(), SemanticError> {
    validate_archived_identifier("style name", value.name.as_str())?;
    for color in [&value.style.foreground, &value.style.background] {
        if let Some(color) = color.as_ref() {
            validate_archived_clean_text("style color", color.as_str())?;
        }
    }
    Ok(())
}

fn validate_archived_named_string(value: &ArchivedNamedStringWire) -> Result<(), SemanticError> {
    validate_archived_identifier("render-model name", value.name.as_str())?;
    validate_archived_clean_text("render-model value", value.value.as_str())
}

fn validate_archived_color_scheme(value: &ArchivedColorSchemeWire) -> Result<(), SemanticError> {
    validate_archived_clean_text("color-scheme title", value.title.as_str())?;
    for entry in value.palette.iter().chain(value.colors.iter()) {
        validate_archived_named_string(entry)?;
    }
    Ok(())
}

fn validate_archived_origin_bootstrap(
    value: &ArchivedUiOriginBootstrap,
) -> Result<(), SemanticError> {
    validate_archived_identifier("WorkspaceId", value.workspace.0.as_str())?;
    validate_archived_identifier("HostTabId", value.tab.0.as_str())?;
    validate_archived_identifier("HostPaneId", value.pane.0.as_str())?;
    if let Some(cwd) = value.cwd.as_ref() {
        validate_archived_absolute_path("origin cwd", cwd.as_str())?;
    }
    Ok(())
}

fn validate_archived_caller_identity(
    value: &ArchivedUiCallerIdentityWire,
) -> Result<(), SemanticError> {
    validate_archived_identifier("WorkspaceId", value.workspace.0.as_str())?;
    validate_archived_identifier("HostTabId", value.tab.0.as_str())?;
    validate_archived_identifier("HostPaneId", value.pane.0.as_str())?;
    validate_archived_absolute_path("caller cwd", value.cwd.as_str())
}

fn validate_archived_diagnostic(value: &ArchivedProtocolDiagnostic) -> Result<(), SemanticError> {
    let message = value.message.as_str();
    if message.len() > MAX_DIAGNOSTIC_LEN {
        return Err(SemanticError::TooLong {
            field: "protocol diagnostic",
            maximum: MAX_DIAGNOSTIC_LEN,
            actual: message.len(),
        });
    }
    validate_archived_text("protocol diagnostic", message, true)
}

fn validate_archived_optional_diagnostic(
    value: Option<&ArchivedProtocolDiagnostic>,
) -> Result<(), SemanticError> {
    value
        .map(validate_archived_diagnostic)
        .transpose()
        .map(|_| ())
}

fn validate_archived_absolute_path(field: &'static str, value: &str) -> Result<(), SemanticError> {
    validate_archived_text(field, value, true)?;
    value
        .starts_with('/')
        .then_some(())
        .ok_or(SemanticError::RelativePath { field })
}

fn validate_archived_clean_text(field: &'static str, value: &str) -> Result<(), SemanticError> {
    validate_archived_text(field, value, false)
}

fn validate_archived_generation(generation: u64) -> Result<(), SemanticError> {
    (generation > 0)
        .then_some(())
        .ok_or(SemanticError::ZeroGeneration)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageDirection {
    PeerToBroker,
    BrokerToPeer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionPhase {
    AwaitingHello,
    AwaitingWelcome,
    Ready,
}

pub trait Validate {
    /// Validates structural and semantic invariants.
    ///
    /// # Errors
    ///
    /// Returns `SemanticError` describing the first violation.
    fn validate(&self) -> Result<(), SemanticError>;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SemanticError {
    #[error("{field} must not be empty")]
    Empty { field: &'static str },
    #[error("{field} exceeds maximum length {maximum}: {actual}")]
    TooLong {
        field: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("{field} contains a NUL byte")]
    Nul { field: &'static str },
    #[error("{field} contains a control character")]
    Control { field: &'static str },
    #[error("{field} contains invalid whitespace")]
    Whitespace { field: &'static str },
    #[error("{0} must not be the zero nonce")]
    ZeroNonce(&'static str),
    #[error("configuration generation must be nonzero")]
    ZeroGeneration,
    #[error("launch lease must be nonzero")]
    ZeroLease,
    #[error("invalid advertised frame limit: {0}")]
    InvalidFrameLimit(u32),
    #[error("layout max item title length must be nonzero")]
    ZeroLayoutTitleLength,
    #[error("vt100 escape timeout must be nonzero")]
    ZeroEscapeTimeout,
    #[error("{field} has too many items: {actual} > {maximum}")]
    TooManyItems {
        field: &'static str,
        maximum: usize,
        actual: usize,
    },
    #[error("enabled binding cannot include a diagnostic")]
    UnexpectedDiagnostic,
    #[error("blocked binding must include a diagnostic")]
    MissingDiagnostic,
    #[error("an included, shown non-hidden binding requires a non-empty label")]
    MissingVisibleLabel,
    #[error("menu graph contains the same menu ID more than once")]
    DuplicateMenu,
    #[error("menu graph does not contain its root menu")]
    MissingRootMenu,
    #[error("{field} must be an absolute path")]
    RelativePath { field: &'static str },
    #[error("binding ID generation does not match the containing menu graph")]
    BindingGenerationMismatch,
    #[error("local menu action targets a menu outside the menu graph")]
    UnknownMenuTarget,
    #[error("message is illegal for {role:?} peer role")]
    IllegalMessageForRole { role: PeerRole },
    #[error("message is illegal during {phase:?} for {role:?}")]
    IllegalSequence {
        role: PeerRole,
        phase: ConnectionPhase,
    },
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), SemanticError> {
    validate_text(field, value)?;
    if value.chars().any(char::is_whitespace) {
        return Err(SemanticError::Whitespace { field });
    }
    Ok(())
}

fn validate_text(field: &'static str, value: &str) -> Result<(), SemanticError> {
    if value.is_empty() {
        return Err(SemanticError::Empty { field });
    }
    if value.contains('\0') {
        return Err(SemanticError::Nul { field });
    }
    if value.chars().any(char::is_control) {
        return Err(SemanticError::Control { field });
    }
    Ok(())
}

fn validate_clean_text(field: &'static str, value: &str) -> Result<(), SemanticError> {
    if value.contains('\0') {
        return Err(SemanticError::Nul { field });
    }
    if value.chars().any(char::is_control) {
        return Err(SemanticError::Control { field });
    }
    Ok(())
}

fn validate_absolute_path(field: &'static str, value: &str) -> Result<(), SemanticError> {
    validate_text(field, value)?;
    value
        .starts_with('/')
        .then_some(())
        .ok_or(SemanticError::RelativePath { field })
}

fn validate_diagnostic(field: &'static str, value: &str) -> Result<(), SemanticError> {
    if value.len() > MAX_DIAGNOSTIC_LEN {
        return Err(SemanticError::TooLong {
            field,
            maximum: MAX_DIAGNOSTIC_LEN,
            actual: value.len(),
        });
    }
    validate_text(field, value)
}

fn validate_generation(generation: u64) -> Result<(), SemanticError> {
    if generation == 0 {
        Err(SemanticError::ZeroGeneration)
    } else {
        Ok(())
    }
}

fn validate_optional(value: Option<&ProtocolDiagnostic>) -> Result<(), SemanticError> {
    match value {
        Some(value) => value.validate(),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> BindingSettingsWire {
        BindingSettingsWire {
            after_action: AfterAction::Stay,
            execution: ExecutionPolicyWire {
                mode: ExecutionMode::Await,
                timeout_millis: None,
                on_timeout: TimeoutAction::Detach,
                on_menu_control: MenuControlAction::Detach,
            },
            repeat: None,
        }
    }

    fn layout() -> LayoutSettingsWire {
        LayoutSettingsWire {
            padding: LayoutPaddingWire {
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

    fn binding(
        generation: u64,
        ordinal: u64,
        action: Option<LocalMenuActionWire>,
    ) -> BindingViewWire {
        BindingViewWire {
            id: BindingId {
                generation,
                ordinal,
            },
            key: format!("key-{ordinal}"),
            label: Some(format!("Binding {ordinal}")),
            hidden: false,
            state: BindingStateWire {
                included: true,
                enabled: true,
                shown: true,
                blocked: false,
            },
            settings: settings(),
            conditions: BindingConditionsWire::default(),
            local_menu_action: action,
            diagnostic: None,
        }
    }

    fn live_server() -> LiveServerIdentity {
        LiveServerIdentity {
            host: HostKind::Herdr,
            discovery_key: "socket".into(),
            server_id: ServerId::new("server"),
        }
    }

    fn welcome() -> WireMessage {
        WireMessage::Welcome {
            request_id: RequestId([1; 16]),
            welcome: Welcome {
                broker_version: "0.1.0".into(),
                live_server: live_server(),
                accepted_frame_len: MAX_FRAME_LEN,
            },
        }
    }

    fn hello() -> WireMessage {
        WireMessage::Hello {
            request_id: RequestId([1; 16]),
            hello: Hello {
                process_version: "0.1.0".into(),
                live_server: live_server(),
            },
        }
    }

    fn attachment() -> UiAttachmentWire {
        UiAttachmentWire {
            menu: MenuViewWire {
                generation: 1,
                root: MenuId::new("root"),
                menus: vec![MenuViewMenuWire {
                    id: MenuId::new("root"),
                    title: None,
                    layout: layout(),
                    bindings: Vec::new(),
                }],
            },
            keyboard: KeyboardProfileWire::Vt100 {
                escape_timeout_millis: 25,
            },
            inactivity_timeout_millis: None,
            theme: CompiledThemeWire {
                common: ThemeSectionWire {
                    styles: Vec::new(),
                    templates: Vec::new(),
                },
                menu: ThemeSectionWire {
                    styles: Vec::new(),
                    templates: Vec::new(),
                },
                settings: Vec::new(),
                scheme: ColorSchemeWire {
                    title: String::new(),
                    palette: Vec::new(),
                    colors: Vec::new(),
                },
            },
        }
    }

    fn append_message(bytes: &mut Vec<u8>, message: &WireMessage) {
        let frame = crate::encode_frame(message).unwrap();
        bytes.extend_from_slice(frame.prefix());
        bytes.extend_from_slice(frame.payload());
    }

    fn client_stream(message: &WireMessage) -> Vec<u8> {
        let mut bytes = crate::Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application())
            .encode()
            .to_vec();
        append_message(&mut bytes, &welcome());
        append_message(&mut bytes, message);
        bytes
    }

    fn broker_stream(message: &WireMessage) -> Vec<u8> {
        let mut bytes = crate::Prelude::rkyv(PeerRole::Ui, SchemaFingerprint::application())
            .encode()
            .to_vec();
        append_message(&mut bytes, &hello());
        append_message(&mut bytes, message);
        bytes
    }

    fn client_decoder() -> crate::ConnectionDecoder {
        crate::ConnectionDecoder::new(crate::ConnectionPolicy::client(
            PeerRole::Ui,
            SchemaFingerprint::application(),
        ))
    }

    fn broker_decoder() -> crate::ConnectionDecoder {
        crate::ConnectionDecoder::new(crate::ConnectionPolicy::broker(
            PeerRole::Ui,
            SchemaFingerprint::application(),
        ))
    }

    #[test]
    fn decoder_rejects_archived_response_theme_and_event_generation_payloads() {
        let mut snapshot = attachment();
        snapshot.theme.common.styles.push(NamedStyleWire {
            name: "cell".into(),
            style: StyleWire {
                foreground: Some("bad\ncolor".into()),
                background: None,
                bold: false,
                dim: false,
                italic: false,
                underline: false,
                strikethrough: false,
            },
        });
        let response = WireMessage::Response {
            request_id: RequestId([2; 16]),
            response: BrokerResponse::UiAttached {
                session: UiSessionId::new("ui"),
                snapshot,
            },
        };
        assert!(matches!(
            client_decoder().push(&client_stream(&response), |_| {}),
            Err(crate::DecodeError::Semantic(SemanticError::Control {
                field: "style color"
            }))
        ));

        let event = WireMessage::Event {
            event_id: EventId([3; 16]),
            event: BrokerEvent::BindingAvailabilityChanged {
                session: UiSessionId::new("ui"),
                generation: 0,
                binding: BindingId {
                    generation: 0,
                    ordinal: 0,
                },
                availability: BindingAvailability::Enabled,
                diagnostic: None,
            },
        };
        assert!(matches!(
            client_decoder().push(&client_stream(&event), |_| {}),
            Err(crate::DecodeError::Semantic(SemanticError::ZeroGeneration))
        ));
    }
    fn deep_condition(depth: usize) -> ConditionIrWire {
        let mut condition = ConditionIrWire::Bool(true);
        for _ in 0..depth {
            condition = ConditionIrWire::Not(Box::new(condition));
        }
        condition
    }

    fn attached_with_condition(condition: ConditionIrWire) -> WireMessage {
        let mut snapshot = attachment();
        let mut view_binding = binding(1, 0, None);
        view_binding.conditions.include = Some(condition);
        snapshot.menu.menus[0].bindings.push(view_binding);
        WireMessage::Response {
            request_id: RequestId([2; 16]),
            response: BrokerResponse::UiAttached {
                session: UiSessionId::new("ui"),
                snapshot,
            },
        }
    }

    #[test]
    fn decoder_rejects_condition_nesting_beyond_transport_depth() {
        // Serialization itself recurses; build the hostile frame off the
        // worker-sized test stack so the test measures the serving pipeline.
        let hostile = std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| client_stream(&attached_with_condition(deep_condition(3000))))
            .expect("hostile frame builder spawns")
            .join()
            .expect("hostile frame builds");
        let Err(crate::DecodeError::InvalidArchive(diagnostic)) =
            client_decoder().push(&hostile, |_| {})
        else {
            panic!("a 3000-deep condition frame must not validate");
        };
        assert!(
            diagnostic.contains("SubtreeDepth"),
            "rejection names the nesting bound"
        );
    }
    #[test]
    fn decoder_accepts_condition_nesting_within_transport_depth() {
        let mut frames = 0;
        assert!(
            client_decoder()
                .push(
                    &client_stream(&attached_with_condition(deep_condition(64))),
                    |_| {
                        frames += 1;
                    }
                )
                .is_ok()
        );
        assert_eq!(frames, 2);
    }
    #[test]
    fn decoder_rejects_archived_attach_origin_and_caller_boundaries() {
        let origin = WireMessage::Request {
            request_id: RequestId([2; 16]),
            request: ClientRequest::AttachUi(AttachUi {
                root: MenuId::new("root"),
                pane: HostPaneId::new("ui-pane"),
                pending_launch: Some(PendingLaunchToken([3; 16])),
                origin: Some(UiOriginBootstrap {
                    workspace: WorkspaceId::new("workspace"),
                    tab: HostTabId::new("tab"),
                    pane: HostPaneId::new("origin-pane"),
                    cwd: Some("relative".into()),
                }),
                caller_identity: None,
                theme: None,
                color_scheme: None,
            }),
        };
        assert!(matches!(
            broker_decoder().push(&broker_stream(&origin), |_| {}),
            Err(crate::DecodeError::Semantic(SemanticError::RelativePath {
                field: "origin cwd"
            }))
        ));

        let caller = WireMessage::Request {
            request_id: RequestId([4; 16]),
            request: ClientRequest::AttachUi(AttachUi {
                root: MenuId::new("root"),
                pane: HostPaneId::new("ui-pane"),
                pending_launch: Some(PendingLaunchToken([5; 16])),
                origin: Some(UiOriginBootstrap {
                    workspace: WorkspaceId::new("workspace"),
                    tab: HostTabId::new("tab"),
                    pane: HostPaneId::new("origin-pane"),
                    cwd: Some("/origin".into()),
                }),
                caller_identity: Some(UiCallerIdentityWire {
                    workspace: WorkspaceId::new("workspace"),
                    tab: HostTabId::new("tab"),
                    pane: HostPaneId::new("caller-pane"),
                    cwd: "relative".into(),
                }),
                theme: None,
                color_scheme: None,
            }),
        };
        assert!(matches!(
            broker_decoder().push(&broker_stream(&caller), |_| {}),
            Err(crate::DecodeError::Semantic(SemanticError::RelativePath {
                field: "caller cwd"
            }))
        ));
    }
    #[test]
    fn decoder_accepts_archived_attach_origin_without_cwd() {
        let request = WireMessage::Request {
            request_id: RequestId([8; 16]),
            request: ClientRequest::AttachUi(AttachUi {
                root: MenuId::new("root"),
                pane: HostPaneId::new("ui-pane"),
                pending_launch: Some(PendingLaunchToken([9; 16])),
                origin: Some(UiOriginBootstrap {
                    workspace: WorkspaceId::new("workspace"),
                    tab: HostTabId::new("tab"),
                    pane: HostPaneId::new("origin-pane"),
                    cwd: None,
                }),
                caller_identity: Some(UiCallerIdentityWire {
                    workspace: WorkspaceId::new("workspace"),
                    tab: HostTabId::new("tab"),
                    pane: HostPaneId::new("caller-pane"),
                    cwd: "/live-caller".into(),
                }),
                theme: None,
                color_scheme: None,
            }),
        };
        let mut frames = 0;
        broker_decoder()
            .push(&broker_stream(&request), |_| {
                frames += 1;
            })
            .expect(
                "valid origin IDs with absent cwd must validate; the live snapshot enriches it",
            );
        assert_eq!(frames, 2);
    }

    #[test]
    fn decoder_rejects_archived_attach_override_boundaries() {
        let request = WireMessage::Request {
            request_id: RequestId([6; 16]),
            request: ClientRequest::AttachUi(AttachUi {
                root: MenuId::new("root"),
                pane: HostPaneId::new("ui-pane"),
                pending_launch: None,
                origin: None,
                caller_identity: None,
                theme: Some("invalid theme".into()),
                color_scheme: None,
            }),
        };
        assert!(matches!(
            broker_decoder().push(&broker_stream(&request), |_| {}),
            Err(crate::DecodeError::Semantic(SemanticError::Whitespace {
                field: "theme override"
            }))
        ));
    }

    #[test]
    fn accepts_absent_titles_and_unbounded_acyclic_navigation_depth() {
        let depth: u64 = 256;
        let menus = (0..depth)
            .map(|index| {
                let action = (index + 1 < depth).then(|| LocalMenuActionWire::Open {
                    target: MenuId::new(format!("menu-{}", index + 1)),
                });
                MenuViewMenuWire {
                    id: MenuId::new(format!("menu-{index}")),
                    title: None,
                    layout: layout(),
                    bindings: vec![binding(7, index, action)],
                }
            })
            .collect();
        MenuViewWire {
            generation: 7,
            root: MenuId::new("menu-0"),
            menus,
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn rejects_stale_binding_generation_and_missing_local_target() {
        let stale = InvokeBinding {
            session: UiSessionId::new("ui"),
            generation: 8,
            binding: BindingId {
                generation: 7,
                ordinal: 1,
            },
        };
        assert_eq!(
            stale.validate(),
            Err(SemanticError::BindingGenerationMismatch)
        );

        let view = MenuViewWire {
            generation: 7,
            root: MenuId::new("root"),
            menus: vec![MenuViewMenuWire {
                id: MenuId::new("root"),
                title: Some("Root".into()),
                layout: layout(),
                bindings: vec![binding(
                    7,
                    1,
                    Some(LocalMenuActionWire::Open {
                        target: MenuId::new("not-present"),
                    }),
                )],
            }],
        };
        assert_eq!(view.validate(), Err(SemanticError::UnknownMenuTarget));
    }

    #[test]
    fn fingerprints_schema_and_fixed_serialization_profile() {
        assert_eq!(
            SchemaFingerprint::application(),
            fingerprint_for(WIRE_SCHEMA_SOURCE, RKYV_SERIALIZATION_PROFILE)
        );
        assert_ne!(
            SchemaFingerprint::application(),
            fingerprint_for(WIRE_SCHEMA_SOURCE, b"rkyv=changed")
        );
        assert_ne!(
            SchemaFingerprint::application(),
            fingerprint_for(b"different schema", RKYV_SERIALIZATION_PROFILE)
        );
    }
    #[test]
    fn evaluates_checked_archived_conditions_without_a_menu_graph_clone() {
        let condition = ConditionIrWire::Greater(
            Box::new(ConditionIrWire::PagesCurrent),
            Box::new(ConditionIrWire::Integer(1)),
        );
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&condition).unwrap();
        let archived =
            rkyv::access::<ArchivedConditionIrWire, rkyv::rancor::Error>(&bytes).unwrap();
        assert!(
            !evaluate_archived_condition(
                archived,
                PagesContextWire {
                    count: 3,
                    current: 1,
                },
            )
            .unwrap()
        );
        assert!(
            evaluate_archived_condition(
                archived,
                PagesContextWire {
                    count: 3,
                    current: 2,
                },
            )
            .unwrap()
        );
    }
}
