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
        let mut hasher = Sha256::new();
        hasher.update(RKYV_SERIALIZATION_PROFILE);
        hasher.update(WIRE_SCHEMA_SOURCE);
        Self(hasher.finalize().into())
    }

    pub fn is_zero(self) -> bool {
        self == Self::ZERO
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
}

impl PeerRole {
    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Ui),
            2 => Some(Self::Launcher),
            3 => Some(Self::Bridge),
            4 => Some(Self::ActivationCoordinator),
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
string_id!(BindingId);
string_id!(HostPaneId);
string_id!(HostTabId);
string_id!(HostClientId);
string_id!(HostSessionId);
string_id!(ServerId);
string_id!(WorkspaceId);
string_id!(WorktreeId);
string_id!(AgentId);
string_id!(LinkHandlerId);
string_id!(BridgeLeaseId);

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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
pub enum BindingAvailability {
    Enabled = 1,
    Blocked = 2,
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
)]
pub struct BindingViewWire {
    pub id: BindingId,
    pub key: String,
    pub label: String,
    pub availability: BindingAvailability,
    pub diagnostic: Option<ProtocolDiagnostic>,
}

impl Validate for BindingViewWire {
    fn validate(&self) -> Result<(), SemanticError> {
        self.id.validate()?;
        validate_text("binding key", &self.key)?;
        validate_text("binding label", &self.label)?;
        match (&self.availability, &self.diagnostic) {
            (BindingAvailability::Enabled, Some(_)) => Err(SemanticError::UnexpectedDiagnostic),
            (BindingAvailability::Blocked, None) => Err(SemanticError::MissingDiagnostic),
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
    Debug,
    PartialEq,
    Eq,
)]
pub struct MenuViewWire {
    pub generation: u64,
    pub menu_id: MenuId,
    /// `None` is semantically distinct from an empty title: selectors and the UI do not fall
    /// back to the menu ID for an absent title.
    pub title: Option<String>,
    pub breadcrumb: Vec<String>,
    pub bindings: Vec<BindingViewWire>,
}

impl Validate for MenuViewWire {
    fn validate(&self) -> Result<(), SemanticError> {
        validate_generation(self.generation)?;
        self.menu_id.validate()?;
        if let Some(title) = &self.title {
            validate_text("menu title", title)?;
        }
        for crumb in &self.breadcrumb {
            validate_text("menu breadcrumb item", crumb)?;
        }
        for binding in &self.bindings {
            binding.validate()?;
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
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
        self.binding.validate()
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
            PeerRole::Bridge | PeerRole::ActivationCoordinator => false,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Debug,
    PartialEq,
    Eq,
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
        view: MenuViewWire,
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
            Self::UiAttached { session, view } => {
                session.validate()?;
                view.validate()
            }
            Self::InvocationAccepted { execution } => execution.validate(),
            Self::Error(error) => error.validate(),
            Self::PendingPaneRegistered | Self::AttachPending | Self::Detached | Self::Acknowledged => {
                Ok(())
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
                    (BindingAvailability::Enabled, Some(_)) => Err(SemanticError::UnexpectedDiagnostic),
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
                Self::Welcome { request_id, welcome },
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
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
    Archive,
    Deserialize,
    Serialize,
    SerdeSerialize,
    SerdeDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
)]
pub struct CaptureLease {
    pub lease: BridgeLeaseId,
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
    Debug,
    PartialEq,
    Eq,
)]
pub enum BridgeMessage<P> {
    Register(BridgeRegistration),
    RegistrationAccepted(CaptureLease),
    Heartbeat,
    BeginInputCapture(CaptureLease),
    EndInputCapture { lease: BridgeLeaseId },
    OriginContextRequested { session: UiSessionId },
    OriginContextProvided {
        session: UiSessionId,
        context: OriginContextWire,
    },
    DispatchAccepted { execution: ExecutionId },
    DispatchCompleted {
        execution: ExecutionId,
        outcome: ExecutionOutcome,
        diagnostic: Option<ProtocolDiagnostic>,
    },
    Health { diagnostic: Option<ProtocolDiagnostic> },
    Retire,
    Shutdown,
    Host(P),
}

impl<P: Validate> Validate for BridgeMessage<P> {
    fn validate(&self) -> Result<(), SemanticError> {
        match self {
            Self::Register(value) => value.validate(),
            Self::RegistrationAccepted(value) | Self::BeginInputCapture(value) => value.validate(),
            Self::EndInputCapture { lease } => lease.validate(),
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
            Self::Heartbeat | Self::Retire | Self::Shutdown => Ok(()),
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
