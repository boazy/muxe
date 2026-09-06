use std::sync::LazyLock;

use rkyv::{Archive, Deserialize, Serialize};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use sha2::{Digest, Sha256};
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

    pub fn application() -> Self {
        *APPLICATION_SCHEMA_FINGERPRINT
    }

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
    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Rkyv),
            2 => Some(Self::ControlJsonV1),
            _ => None,
        }
    }

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
nonce_id!(BridgeRegistrationId);
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
        for condition in [&self.include, &self.enable, &self.show] {
            if let Some(condition) = condition {
                condition.validate()?;
            }
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
pub fn evaluate_archived_condition(
    condition: &ArchivedConditionIrWire,
    pages: PagesContextWire,
) -> Result<bool, ConditionEvaluationErrorWire> {
    match evaluate_archived_condition_value(condition, pages)? {
        ConditionValueWire::Bool(value) => Ok(value),
        ConditionValueWire::Integer(_) => Err(ConditionEvaluationErrorWire::NonBooleanResult),
    }
}

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
                if let Some(LocalMenuActionWire::Open { target }) = &binding.local_menu_action {
                    if !self.menus.iter().any(|candidate| candidate.id == *target) {
                        return Err(SemanticError::UnknownMenuTarget);
                    }
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
        for color in [&self.foreground, &self.background] {
            if let Some(color) = color {
                validate_clean_text("style color", color)?;
            }
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

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct AttachUi {
    pub root: MenuId,
    pub pane: HostPaneId,
    pub pending_launch: Option<PendingLaunchToken>,
}

impl Validate for AttachUi {
    fn validate(&self) -> Result<(), SemanticError> {
        self.root.validate()?;
        self.pane.validate()?;
        if let Some(token) = self.pending_launch {
            token.validate()?;
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
    pub const fn allowed_for(self: &Self, role: PeerRole) -> bool {
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
            Self::InvocationAccepted { execution } => execution.validate(),
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
            ArchivedWireMessage::Response { request_id, .. },
        ) => {
            validate_archived_nonce(&request_id.0, "RequestId")?;
            Ok(ConnectionPhase::Ready)
        }
        (
            MessageDirection::BrokerToPeer,
            ConnectionPhase::Ready,
            ArchivedWireMessage::Event { event_id, .. },
        ) => {
            validate_archived_nonce(&event_id.0, "EventId")?;
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
pub enum OriginInvocationSource {
    RootBinding = 1,
    CommandLine = 2,
    Link = 3,
    Automation = 4,
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
pub enum OriginPaneType {
    Tiled = 1,
    Floating = 2,
    Plugin = 3,
    Terminal = 4,
    Other = 5,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct OriginContextWire {
    pub host: HostKind,
    pub server: ServerId,
    pub client: Option<HostClientId>,
    pub session: Option<HostSessionId>,
    pub workspace: Option<WorkspaceId>,
    pub tab: Option<HostTabId>,
    pub tab_index: Option<u64>,
    pub pane: Option<HostPaneId>,
    pub pane_type: Option<OriginPaneType>,
    pub pane_cwd: Option<String>,
    pub selection_text: Option<String>,
    pub invocation_source: OriginInvocationSource,
    pub worktree: Option<WorktreeId>,
    pub worktree_path: Option<String>,
    pub agent: Option<AgentId>,
    pub link_url: Option<String>,
    pub link_handler: Option<LinkHandlerId>,
}

impl Validate for OriginContextWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.server.validate()?;
        for identifier in [
            self.client.as_ref().map(|value| value.validate()),
            self.session.as_ref().map(|value| value.validate()),
            self.workspace.as_ref().map(|value| value.validate()),
            self.tab.as_ref().map(|value| value.validate()),
            self.pane.as_ref().map(|value| value.validate()),
            self.worktree.as_ref().map(|value| value.validate()),
            self.agent.as_ref().map(|value| value.validate()),
            self.link_handler.as_ref().map(|value| value.validate()),
        ] {
            if let Some(result) = identifier {
                result?;
            }
        }
        for text in [
            self.pane_cwd.as_deref(),
            self.selection_text.as_deref(),
            self.worktree_path.as_deref(),
            self.link_url.as_deref(),
        ] {
            if let Some(value) = text {
                validate_text("origin context value", value)?;
            }
        }
        Ok(())
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct BridgeRegistration {
    pub registration: BridgeRegistrationId,
    pub live_server: LiveServerIdentity,
    pub client: HostClientId,
}

impl Validate for BridgeRegistration {
    fn validate(&self) -> Result<(), SemanticError> {
        self.registration.validate()?;
        self.live_server.validate()?;
        self.client.validate()
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct CaptureRequest {
    pub scope: ModalScopeId,
    pub ui: UiSessionId,
}

impl Validate for CaptureRequest {
    fn validate(&self) -> Result<(), SemanticError> {
        self.scope.validate()?;
        self.ui.validate()
    }
}

/// An adapter-issued opaque lease. The adapter alone owns host-mode restoration details.
#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct CaptureLease {
    pub lease: CaptureLeaseId,
    pub expires_after_millis: u32,
}

impl Validate for CaptureLease {
    fn validate(&self) -> Result<(), SemanticError> {
        self.lease.validate()?;
        if self.expires_after_millis == 0 {
            return Err(SemanticError::ZeroLease);
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
pub enum CaptureEndReason {
    UiDetached = 1,
    Replaced = 2,
    LaunchAborted = 3,
    BrokerRetiring = 4,
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
pub enum CaptureLostReason {
    UserModeChanged = 1,
    AdapterHealth = 2,
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub enum BridgeMessage<P> {
    Register(BridgeRegistration),
    RegistrationAccepted,
    Heartbeat,
    BeginInputCapture(CaptureRequest),
    InputCaptureReady(CaptureLease),
    EndInputCapture {
        lease: CaptureLeaseId,
        reason: CaptureEndReason,
    },
    InputCaptureLost {
        lease: CaptureLeaseId,
        reason: CaptureLostReason,
    },
    OriginContextRequested {
        session: UiSessionId,
    },
    OriginContextProvided {
        session: UiSessionId,
        context: OriginContextWire,
    },
    DispatchAccepted {
        execution: ExecutionId,
    },
    DispatchCompleted {
        execution: ExecutionId,
        outcome: ExecutionOutcome,
        diagnostic: Option<ProtocolDiagnostic>,
    },
    Health {
        diagnostic: Option<ProtocolDiagnostic>,
    },
    Retire,
    Shutdown,
    Host(P),
}

impl<P: Validate> Validate for BridgeMessage<P> {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::Register(value) => value.validate(),
            Self::BeginInputCapture(value) => value.validate(),
            Self::InputCaptureReady(value) => value.validate(),
            Self::EndInputCapture { lease, .. } | Self::InputCaptureLost { lease, .. } => {
                lease.validate()
            }
            Self::OriginContextRequested { session } => session.validate(),
            Self::OriginContextProvided { session, context } => {
                session.validate()?;
                context.validate()
            }
            Self::DispatchAccepted { execution } => execution.validate(),
            Self::DispatchCompleted {
                execution,
                diagnostic,
                ..
            } => {
                execution.validate()?;
                validate_optional(diagnostic.as_ref())
            }
            Self::Health { diagnostic } => validate_optional(diagnostic.as_ref()),
            Self::Host(payload) => payload.validate(),
            Self::RegistrationAccepted | Self::Heartbeat | Self::Retire | Self::Shutdown => Ok(()),
        }
    }
}

#[derive(
    Archive, Deserialize, Serialize, SerdeSerialize, SerdeDeserialize, Clone, Debug, PartialEq, Eq,
)]
pub struct BridgeEnvelope<P> {
    pub sequence: u64,
    pub message: BridgeMessage<P>,
}

impl<P: Validate> Validate for BridgeEnvelope<P> {
    fn validate(&self) -> Result<(), SemanticError> {
        if self.sequence == 0 {
            return Err(SemanticError::ZeroSequence);
        }
        self.message.validate()
    }
}

pub trait Validate {
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
    #[error("bridge sequence must be nonzero")]
    ZeroSequence,
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
    if value.chars().any(|character| character.is_control()) {
        return Err(SemanticError::Control { field });
    }
    Ok(())
}

fn validate_clean_text(field: &'static str, value: &str) -> Result<(), SemanticError> {
    if value.contains('\0') {
        return Err(SemanticError::Nul { field });
    }
    if value.chars().any(|character| character.is_control()) {
        return Err(SemanticError::Control { field });
    }
    Ok(())
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

    #[test]
    fn accepts_absent_titles_and_unbounded_acyclic_navigation_depth() {
        let depth = 256;
        let menus = (0..depth)
            .map(|index| {
                let action = (index + 1 < depth).then(|| LocalMenuActionWire::Open {
                    target: MenuId::new(format!("menu-{}", index + 1)),
                });
                MenuViewMenuWire {
                    id: MenuId::new(format!("menu-{index}")),
                    title: None,
                    layout: layout(),
                    bindings: vec![binding(7, index as u64, action)],
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
