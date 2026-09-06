//! Async constructor-injected host-adapter contract.
//!
//! The contract is intentionally transport- and host-payload-free. Concrete adapters translate
//! their own typed host requests at this boundary; `muxe-core` remains free of async, IPC, host,
//! and terminal dependencies.

#![forbid(unsafe_code)]

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use muxe_core::{
    ActionValidator, CompiledGeneration, ConfigDiagnostic, ContextResolutionError,
    ExecutionCapabilities, ExecutionId, NativeActionCandidate, OriginContext, PaneId,
    PortableAction, PortableActionResolutionError, TabId, WorkspaceId,
};

macro_rules! opaque_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(value: impl Into<Arc<str>>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

opaque_id!(UiSessionId, "Opaque broker UI-session identity.");
opaque_id!(ModalScopeId, "Opaque host modal-scope identity.");
opaque_id!(
    CaptureLeaseId,
    "Opaque exclusive active-input-capture lease identity."
);
opaque_id!(
    ExecutionCorrelationId,
    "Adapter correlation identity for a broker execution."
);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HostKind {
    Zellij,
    Herdr,
}

/// Identity of the exact live host server, not merely its discovery key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostIdentity {
    pub kind: HostKind,
    pub discovery_key: String,
    pub live_server_id: String,
}

#[expect(clippy::struct_excessive_bools, reason = "four independent Kitty protocol flag bits negotiated with the host")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KeyboardCapabilities {
    pub kitty_baseline: bool,
    pub kitty_event_types: bool,
    pub kitty_alternate_keys: bool,
    pub kitty_all_keys_as_escape_codes: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterCapabilities {
    pub keyboard: KeyboardCapabilities,
    pub supports_capture: bool,
    pub supports_notifications: bool,
    pub supports_native_cancellation: bool,
}

/// Input required to establish exclusive input capture for one ready UI.
///
/// The broker resolves the modal scope before invoking this method. The adapter owns all remaining
/// host-specific targeting. For example, Zellij retains live-server, client, bridge-registration,
/// and prior-mode details internally; pane bootstrap is not a generic capture target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureRequest {
    pub ui_session: UiSessionId,
    pub modal_scope: ModalScopeId,
}

/// Exclusive capture lease. A successful begin means host capture is already confirmed, not merely
/// requested. End is guarded by the adapter: it restores state only while this lease owns capture.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureLease {
    pub id: CaptureLeaseId,
    pub ui_session: UiSessionId,
    pub modal_scope: ModalScopeId,
}

/// Broker-registered pending UI placement. The pending-launch token remains broker-only; adapters
/// receive only concrete host identities to revalidate before idempotent cleanup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingPaneRegistration {
    pub ui_session: UiSessionId,
    pub pane: PaneId,
    pub temporary_tab: Option<TabId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CaptureReleaseReason {
    UiDismissed,
    Replaced,
    LeaseExpired,
    UserModeChanged,
    AdapterShutdown,
}

/// An atomic, typed launch-time origin tuple. The adapter must treat it as untrusted and validate
/// all IDs together against the live host before returning an immutable [`OriginContext`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UntrustedOriginHint {
    pub workspace_id: WorkspaceId,
    pub tab_id: TabId,
    pub pane_id: PaneId,
    pub cwd: Option<std::path::PathBuf>,
    pub source: OriginHintSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OriginHintSource {
    LauncherBootstrap,
    DirectInvocation,
}

/// The UI process's own identity, captured separately from the saved origin. The adapter verifies
/// it identifies the attached UI pane and never substitutes broker focus or process environment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostCallerIdentity {
    pub workspace_id: WorkspaceId,
    pub tab_id: TabId,
    pub pane_id: PaneId,
    pub cwd: Option<std::path::PathBuf>,
}

/// Inputs used for immutable origin capture before later focus changes can affect dispatch.
/// Pending launch tokens are broker-gate identities and deliberately do not appear here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OriginCaptureRequest {
    pub ui_session: UiSessionId,
    pub ui_pane: PaneId,
    pub origin_hint: Option<UntrustedOriginHint>,
    pub caller_identity: Option<HostCallerIdentity>,
}
/// An adapter-independent native request whose field values were context-resolved by the broker.
/// It retains source metadata for diagnostics but has no JSON-value escape hatch.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedNativeAction {
    pub candidate: NativeActionCandidate,
}

impl ResolvedNativeAction {
    /// Resolves the candidate's context references against the immutable origin.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`ContextResolutionError`] when a field reference
    /// cannot resolve against the origin.
    pub fn from_origin(
        candidate: &NativeActionCandidate,
        origin: &OriginContext,
    ) -> Result<Self, ContextResolutionError> {
        Ok(Self {
            candidate: candidate.resolve_context(origin)?,
        })
    }
}

/// An adapter-independent portable request after every scalar context reference has resolved and
/// been concretely revalidated against the immutable origin.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedPortableAction {
    pub action: PortableAction,
}

impl ResolvedPortableAction {
    /// Resolves the portable action against the immutable origin.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`PortableActionResolutionError`] when the action
    /// cannot resolve against the origin.
    pub fn from_origin(
        action: &PortableAction,
        origin: &OriginContext,
    ) -> Result<Self, PortableActionResolutionError> {
        Ok(Self {
            action: action.resolve_context(origin)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PortableDispatchRequest {
    pub execution: ExecutionId,
    pub action: ResolvedPortableAction,
    pub origin: OriginContext,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NativeDispatchRequest {
    pub execution: ExecutionId,
    pub action: ResolvedNativeAction,
    pub origin: OriginContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchAccepted {
    pub correlation: ExecutionCorrelationId,
    pub execution: ExecutionId,
    pub capabilities: ExecutionCapabilities,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchCompletion {
    Succeeded {
        execution: ExecutionId,
    },
    Failed {
        execution: ExecutionId,
        error: AdapterError,
    },
    OutcomeUnknown {
        execution: ExecutionId,
        error: AdapterError,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CaptureLossReason {
    UserModeChanged,
    AdapterHealth,
    LeaseReplaced,
}

pub enum AdapterHealthEvent {
    Healthy {
        identity: HostIdentity,
    },
    /// `None` means server-wide health; `Some` isolates loss to one modal scope.
    Unhealthy {
        modal_scope: Option<ModalScopeId>,
        error: AdapterError,
    },
    Reconnected {
        previous: HostIdentity,
        current: HostIdentity,
    },
    CaptureReady {
        lease: CaptureLease,
    },
    CaptureLost {
        lease: CaptureLease,
        reason: CaptureLossReason,
    },
    DispatchCompleted(DispatchCompletion),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdapterErrorKind {
    Unavailable,
    InvalidRequest,
    ContextUnavailable,
    Incompatible,
    DispatchFailed,
    OutcomeUnknown,
    CaptureLost,
    CancelUnsupported,
    /// The selected host cannot participate in this lifecycle transition.
    Unsupported,
    Shutdown,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdapterError {
    pub kind: AdapterErrorKind,
    pub message: String,
    pub diagnostic: Option<ConfigDiagnostic>,
}

impl AdapterError {
    pub fn new(kind: AdapterErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            diagnostic: None,
        }
    }

    #[must_use]
    pub fn with_diagnostic(mut self, diagnostic: ConfigDiagnostic) -> Self {
        self.diagnostic = Some(diagnostic);
        self
    }
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AdapterError {}

/// Per-client commit-gate evidence for activation readiness: which current
/// clients hold a fresh compatible registration in this attempt, measured
/// against the authoritative membership of the same snapshot round. Client
/// IDs only, never payloads. The broker maps this to the protocol readiness
/// record; adapters that cannot observe per-client registration report `None`
/// through the default method and the broker gates on adapter health instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivationReadiness {
    /// Client IDs holding a fresh compatible registration in this attempt.
    pub registered_clients: Vec<String>,
    /// Authoritative member IDs of the same snapshot round, canonical order,
    /// deduplicated. A count alone cannot prove coverage: a newcomer could
    /// mask a missing member.
    pub member_clients: Vec<String>,
}

/// One constructor-injected adapter per broker. Implementations must retain their own typed host
/// payloads; bridge envelopes belong to protocol crates and never appear here.
#[async_trait]
pub trait HostAdapter: ActionValidator + Send + Sync {
    async fn identity(&self) -> Result<HostIdentity, AdapterError>;

    async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError>;

    /// Returns a host-defined scope used to enforce v1's one-ready-UI modal rule.
    async fn modal_scope(&self, ui_pane: &PaneId) -> Result<ModalScopeId, AdapterError>;

    /// Starts capture only after the host confirms it. Implementations serialize replacement and
    /// guarded restoration for their own host state.
    async fn begin_capture(&self, request: CaptureRequest) -> Result<CaptureLease, AdapterError>;

    /// Releases a lease idempotently. A host mode changed by the user is never restored from a
    /// stale capture snapshot.
    async fn end_capture(
        &self,
        lease: CaptureLease,
        reason: CaptureReleaseReason,
    ) -> Result<(), AdapterError>;

    /// Idempotently closes only a revalidated pending UI pane. Implementations must never infer or
    /// close an origin pane/tab from stale launch metadata.
    async fn close_pending_pane(
        &self,
        registration: PendingPaneRegistration,
    ) -> Result<(), AdapterError>;

    /// Captures and enriches a typed immutable origin before dispatchable focus can move.
    async fn capture_origin(
        &self,
        request: OriginCaptureRequest,
    ) -> Result<OriginContext, AdapterError>;

    async fn dispatch_portable(
        &self,
        request: PortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError>;

    /// Dispatches a load-time validated action after the adapter performs mandatory immediate
    /// runtime validation of the fully resolved candidate.
    async fn dispatch_native(
        &self,
        request: NativeDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError>;

    async fn cancel(&self, execution: ExecutionId) -> Result<(), AdapterError>;

    /// Waits for the next host lifecycle, capture, or completion event without exposing a runtime
    /// stream type through this minimal contract.
    async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError>;

    /// Stops the retained host subscription after broker-owned UI state drains and before the
    /// old endpoint is released to a target. Implementations must return an explicit unsupported
    /// error rather than silently retaining the old host adapter.
    async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
        Err(AdapterError::new(
            AdapterErrorKind::Unsupported,
            "this host adapter cannot suspend for activation",
        ))
    }

    /// Re-establishes the old adapter after an activation abort before the old endpoint accepts
    /// dispatch again. Implementations must revalidate live compatibility and retained transport.
    async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
        Err(AdapterError::new(
            AdapterErrorKind::Unsupported,
            "this host adapter cannot resume after an activation abort",
        ))
    }

    /// Reports per-client commit-gate evidence for activation readiness.
    /// Defaults to `None`: adapters that cannot observe per-client
    /// registration leave the broker gating on adapter health.
    async fn activation_readiness(&self) -> Result<Option<ActivationReadiness>, AdapterError> {
        Ok(None)
    }

    async fn shutdown(&self) -> Result<(), AdapterError>;
}

/// Key for broker-owned runtime compatibility state, distinct from immutable configuration.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BindingCompatibilityKey {
    pub generation: CompiledGeneration,
    pub binding_ordinal: u64,
    pub active_host_schema_fingerprint: String,
}
