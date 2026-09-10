mod support {
    #[expect(
        dead_code,
        reason = "the contract test shares the complete production-connect surface with suspend and transport tests"
    )]
    pub mod production_connect;
    #[expect(
        dead_code,
        reason = "the production-connect fixture shares the complete recorded socket surface with focused transport tests"
    )]
    pub mod recorded_socket;
}

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use muxe_adapter_api::{
    AdapterCapabilities, AdapterError, AdapterErrorKind, AdapterHealthEvent, CaptureLease,
    CaptureReleaseReason, CaptureRequest, DispatchAccepted, DispatchCompletion,
    ExecutionCorrelationId, HostAdapter, HostIdentity, HostKind, KeyboardCapabilities,
    ModalScopeId, NativeDispatchRequest, OriginCaptureRequest, PendingPaneRegistration,
    PortableDispatchRequest,
};
use muxe_adapter_herdr::HerdrAdapter;
use muxe_adapter_zellij::{PipeChannel, PipeTransportError, ZellijAdapter, ZellijAdapterConfig};
use muxe_core::{
    ActionValidation, ActionValidator, ConfigDiagnostic, DiagnosticCode, ExecutionCapabilities,
    ExecutionId, NativeActionCandidate, OriginContext, OriginHostKind, OriginInvocationSource,
    PaneId, PortableAction, ServerId, SourceId, SourceSpan,
};
use muxe_zellij_protocol::{
    BRIDGE_PROTOCOL_VERSION, BridgeIdentity, BridgeRequest, ChannelGeneration, CommandOutcome,
    PipeEvent, PipeEventKind, RegistrationId, bridge_build_id, bridge_protocol_fingerprint,
    decode_request_line, encode_event_line, generated_action_fingerprint, pinned_source_revision,
};
use support::production_connect::ProductionConnectFixture;
use tokio::sync::Notify;

/// Minimal recorded endpoint used only by this contract test. It implements the
/// public pipe boundary, rather than reaching into Zellij's private test module.
struct RecordedPipeChannel {
    inbound: Mutex<VecDeque<String>>,
    outbound: Mutex<Vec<String>>,
    available: Notify,
    closed: AtomicBool,
}

impl RecordedPipeChannel {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inbound: Mutex::new(VecDeque::new()),
            outbound: Mutex::new(Vec::new()),
            available: Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    fn push_line(&self, line: String) {
        self.inbound
            .lock()
            .expect("recorded inbound queue is not poisoned")
            .push_back(line);
        self.available.notify_one();
    }

    fn take_outbound(&self) -> Vec<String> {
        std::mem::take(
            &mut *self
                .outbound
                .lock()
                .expect("recorded outbound queue is not poisoned"),
        )
    }
}

#[async_trait]
impl PipeChannel for RecordedPipeChannel {
    async fn send_line(&self, line: String) -> Result<(), PipeTransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PipeTransportError::Closed);
        }
        self.outbound
            .lock()
            .expect("recorded outbound queue is not poisoned")
            .push(line);
        self.available.notify_waiters();
        Ok(())
    }

    async fn next_line(&self) -> Result<String, PipeTransportError> {
        loop {
            let notified = self.available.notified();
            if let Some(line) = self
                .inbound
                .lock()
                .expect("recorded inbound queue is not poisoned")
                .pop_front()
            {
                return Ok(line);
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(PipeTransportError::Closed);
            }
            notified.await;
        }
    }

    async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.available.notify_waiters();
    }
}

/// A narrow contract double, not a reusable mock framework. It records only
/// the broker-visible dispatch/cancel transition shared by the host adapters.
struct RecordedContractAdapter {
    correlation: AtomicU64,
    completions: Mutex<VecDeque<AdapterHealthEvent>>,
    dispatches: Mutex<Vec<ExecutionId>>,
    cancellations: Mutex<Vec<ExecutionId>>,
}

impl RecordedContractAdapter {
    fn new() -> Self {
        Self {
            correlation: AtomicU64::new(1),
            completions: Mutex::new(VecDeque::new()),
            dispatches: Mutex::new(Vec::new()),
            cancellations: Mutex::new(Vec::new()),
        }
    }

    fn unsupported() -> AdapterError {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "not exercised by the recorded contract",
        )
    }
}

impl ActionValidator for RecordedContractAdapter {
    fn validate_portable(
        &self,
        _action: &PortableAction,
        _action_span: &SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        Ok(ActionValidation {
            execution: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        let mut accepted = Vec::with_capacity(candidates.len());
        let mut diagnostics = Vec::new();
        for candidate in candidates {
            if candidate.type_name == "native.recorded:complete" {
                accepted.push(ActionValidation {
                    execution: ExecutionCapabilities::ASYNCHRONOUS,
                });
            } else {
                diagnostics.push(ConfigDiagnostic::error(
                    DiagnosticCode::InvalidAction,
                    "the recorded contract accepts only native.recorded:complete",
                    candidate.type_span.clone(),
                ));
            }
        }
        if diagnostics.is_empty() {
            Ok(accepted)
        } else {
            Err(diagnostics)
        }
    }
}

#[async_trait]
impl HostAdapter for RecordedContractAdapter {
    async fn identity(&self) -> Result<HostIdentity, AdapterError> {
        Ok(HostIdentity {
            kind: HostKind::Herdr,
            discovery_key: "recorded-contract".to_owned(),
            live_server_id: "recorded-server".to_owned(),
        })
    }

    async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
        Ok(AdapterCapabilities {
            keyboard: KeyboardCapabilities {
                kitty_baseline: false,
                kitty_event_types: false,
                kitty_alternate_keys: false,
                kitty_all_keys_as_escape_codes: false,
            },
            supports_capture: false,
            supports_notifications: false,
            supports_native_cancellation: true,
        })
    }

    async fn modal_scope(&self, _ui_pane: &PaneId) -> Result<ModalScopeId, AdapterError> {
        Err(Self::unsupported())
    }

    async fn begin_capture(&self, _request: CaptureRequest) -> Result<CaptureLease, AdapterError> {
        Err(Self::unsupported())
    }

    async fn end_capture(
        &self,
        _lease: CaptureLease,
        _reason: CaptureReleaseReason,
    ) -> Result<(), AdapterError> {
        Err(Self::unsupported())
    }

    async fn register_pending_pane(
        &self,
        registration: PendingPaneRegistration,
    ) -> Result<muxe_adapter_api::PendingPaneLease, AdapterError> {
        Ok(muxe_adapter_api::PendingPaneLease {
            id: muxe_adapter_api::PendingPaneLeaseId::new(format!(
                "recorded:{}",
                registration.ui_session
            )),
            ui_session: registration.ui_session,
        })
    }

    async fn close_pending_pane(
        &self,
        _registration: PendingPaneRegistration,
        _lease: muxe_adapter_api::PendingPaneLease,
    ) -> Result<(), AdapterError> {
        Err(Self::unsupported())
    }

    async fn release_pending_pane(
        &self,
        _lease: muxe_adapter_api::PendingPaneLease,
    ) -> Result<(), AdapterError> {
        Err(Self::unsupported())
    }

    async fn capture_origin(
        &self,
        _request: OriginCaptureRequest,
    ) -> Result<OriginContext, AdapterError> {
        Err(Self::unsupported())
    }

    async fn dispatch_portable(
        &self,
        _request: PortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        Err(Self::unsupported())
    }

    async fn dispatch_native(
        &self,
        request: NativeDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        let candidate = &request.action.candidate;
        self.validate_native_batch(std::slice::from_ref(&candidate))
            .map_err(|diagnostics| {
                let message = diagnostics.first().map_or_else(
                    || "the recorded contract rejected the native action".to_owned(),
                    |diagnostic| diagnostic.message.clone(),
                );
                AdapterError::new(AdapterErrorKind::InvalidRequest, message)
            })?;
        self.dispatches
            .lock()
            .expect("recorded dispatches are not poisoned")
            .push(request.execution);
        self.completions
            .lock()
            .expect("recorded completions are not poisoned")
            .push_back(AdapterHealthEvent::DispatchCompleted(
                DispatchCompletion::Succeeded {
                    execution: request.execution,
                },
            ));
        Ok(DispatchAccepted {
            correlation: ExecutionCorrelationId::new(format!(
                "recorded-{}",
                self.correlation.fetch_add(1, Ordering::Relaxed)
            )),
            execution: request.execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    async fn cancel(&self, execution: ExecutionId) -> Result<(), AdapterError> {
        self.cancellations
            .lock()
            .expect("recorded cancellations are not poisoned")
            .push(execution);
        Ok(())
    }

    async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
        self.completions
            .lock()
            .expect("recorded completions are not poisoned")
            .pop_front()
            .ok_or_else(|| AdapterError::new(AdapterErrorKind::Shutdown, "recording is complete"))
    }

    async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
        // Mirrors the production adapters: resume without a prior suspend is
        // an Unavailable error, never a fabricated healthy subscription.
        Err(AdapterError::new(
            AdapterErrorKind::Unavailable,
            "the recorded contract was never suspended; refusing to fabricate a resumed subscription",
        ))
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        Ok(())
    }
}

fn origin() -> OriginContext {
    OriginContext {
        host_kind: OriginHostKind::Zellij,
        server_id: ServerId::new("session-alpha"),
        client_id: Some(muxe_core::ClientId::new("client-1")),
        session_id: Some(muxe_core::SessionId::new("session-alpha")),
        workspace_id: None,
        tab_id: None,
        tab_index: None,
        pane_id: Some(PaneId::new("terminal_2")),
        pane_type: None,
        pane_cwd: None,
        selection_text: None,
        invocation_source: OriginInvocationSource::RootBinding,
        worktree_id: None,
        worktree_path: None,
        agent_id: None,
        link_url: None,
        link_handler_id: None,
    }
}

fn candidate(type_name: &str) -> NativeActionCandidate {
    NativeActionCandidate {
        type_name: type_name.to_owned(),
        type_span: SourceSpan::new(SourceId::new("<recorded-contract>"), 0, 1),
        fields: Vec::new(),
    }
}

fn registration(seed: u8) -> RegistrationId {
    RegistrationId::from_random_bytes([seed; 16]).expect("test registration")
}

fn event_for(
    registration: RegistrationId,
    request_id: Option<muxe_zellij_protocol::RequestId>,
    event: PipeEventKind,
) -> PipeEvent {
    PipeEvent {
        protocol: BRIDGE_PROTOCOL_VERSION,
        request_id,
        channel_generation: ChannelGeneration::INITIAL,
        registration,
        event,
    }
}

fn register_event(registration_id: [u8; 16], build_id: muxe_protocol::SchemaFingerprint) -> PipeEvent {
    event_for(
        registration(registration_id[0]),
        None,
        PipeEventKind::Register {
            client_id: "client-1".to_owned(),
            current_pane: Some("terminal_2".to_owned()),
            plugin_id: Some(3),
            identity: BridgeIdentity {
                muxe_version: env!("CARGO_PKG_VERSION").to_owned(),
                source_revision: pinned_source_revision().to_owned(),
                action_fingerprint: generated_action_fingerprint().0,
                protocol_fingerprint: bridge_protocol_fingerprint().0,
                bridge_build_id: Some(build_id),
            },
        },
    )
}

async fn next_outbound(channel: &RecordedPipeChannel) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(line) = channel.take_outbound().into_iter().next() {
            return line;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the recorded Zellij request"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
/// Host-specific expectations for the one shared observable driver.
struct ContractExpectations {
    kind: HostKind,
    supports_native_cancellation: bool,
    valid_native: &'static str,
}

/// The one shared observable driver for the host-neutral boundary (DES1642).
///
/// It runs the transport-independent assertions against any production
/// constructor: live-server identity shape, capability truthfulness,
/// load-time batch validation, validation-before-dispatch (a never-validated
/// candidate is never accepted), truthful cancellation, and the
/// no-fabricated-resume guard. Dispatch acceptance, completion correlation,
/// capture-origin turnover, and subscription reconnect stay per-host: Herdr
/// dispatches need a captured continuity epoch and Zellij dispatches need a
/// bridge registration first, so no single dispatch path can observe them.
async fn drive_shared_contract(adapter: &dyn HostAdapter, expectations: &ContractExpectations) {
    let identity = adapter
        .identity()
        .await
        .expect("the shared contract reports a live identity");
    assert_eq!(identity.kind, expectations.kind);
    assert!(
        !identity.discovery_key.is_empty(),
        "the shared contract reports a discovery key"
    );
    assert!(
        !identity.live_server_id.is_empty(),
        "the shared contract reports a live server id"
    );
    assert_eq!(
        adapter
            .capabilities()
            .await
            .expect("the shared contract reports capabilities")
            .supports_native_cancellation,
        expectations.supports_native_cancellation,
        "cancellation capability is truthful"
    );
    assert!(
        adapter
            .validate_native_batch(&[&candidate(expectations.valid_native)])
            .is_ok(),
        "the shared contract accepts its host namespace"
    );
    assert!(
        !adapter
            .validate_native_batch(&[&candidate("native.unknown:does-not-exist")])
            .expect_err("an unknown native action is rejected")
            .is_empty(),
        "rejection carries diagnostics"
    );
    assert!(
        adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(9_999_001),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate("native.unknown:does-not-exist"),
                },
                origin: origin(),
            })
            .await
            .is_err(),
        "a never-validated candidate is never accepted"
    );
    let cancellation = adapter.cancel(ExecutionId(9_999_002)).await;
    assert_eq!(
        cancellation.is_ok(),
        expectations.supports_native_cancellation,
        "cancellation behavior matches the reported capability"
    );
    if !expectations.supports_native_cancellation {
        assert_eq!(
            cancellation
                .expect_err("unsupported cancellation reports its kind")
                .kind,
            AdapterErrorKind::CancelUnsupported
        );
    }
    assert_eq!(
        adapter
            .resume_after_activation_abort()
            .await
            .expect_err("resume without suspend never fabricates health")
            .kind,
        AdapterErrorKind::Unavailable
    );
}
/// The host-neutral boundary promises observable identity/capability/action
/// validation/correlation/cancellation semantics independent of host transport.
#[tokio::test]
async fn recorded_common_contract_preserves_broker_visible_transitions() {
    let adapter = RecordedContractAdapter::new();
    assert_eq!(
        adapter
            .validate_native_batch(&[&candidate("native.recorded:unknown")])
            .expect_err("unknown native action is rejected")[0]
            .code,
        DiagnosticCode::InvalidAction
    );

    let execution = ExecutionId(41);
    let accepted = adapter
        .dispatch_native(NativeDispatchRequest {
            execution,
            action: muxe_adapter_api::ResolvedNativeAction {
                candidate: candidate("native.recorded:complete"),
            },
            origin: origin(),
        })
        .await
        .expect("validated native action is accepted");
    assert_eq!(accepted.execution, execution);
    assert_eq!(accepted.correlation.as_str(), "recorded-1");
    assert!(matches!(
        adapter.next_health_event().await.expect("completion event"),
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded {
            execution: ExecutionId(41)
        })
    ));
    adapter
        .cancel(execution)
        .await
        .expect("recorded cancellation");
    assert_eq!(
        *adapter
            .dispatches
            .lock()
            .expect("recorded dispatches are not poisoned"),
        vec![execution]
    );
    assert_eq!(
        *adapter
            .cancellations
            .lock()
            .expect("recorded cancellations are not poisoned"),
        vec![execution]
    );
    drive_shared_contract(
        &adapter,
        &ContractExpectations {
            kind: HostKind::Herdr,
            supports_native_cancellation: true,
            valid_native: "native.recorded:complete",
        },
    )
    .await;
}

/// Exercises the actual Herdr constructor boundary with an owned shell schema
/// child and a recorded Unix peer; no Herdr host process is started.
#[tokio::test]
async fn recorded_herdr_production_connect_reports_raw_identity_and_messages() {
    let fixture =
        ProductionConnectFixture::start().expect("recorded production-connect fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect(
            "production Herdr connect accepts the recorded schema, ping, probe, and subscription",
        );

    let observed = adapter
        .identity()
        .await
        .expect("retained subscription is healthy");
    assert_eq!(
        observed,
        fixture
            .raw_identity()
            .await
            .expect("fixture reads the same raw endpoint identity")
    );
    assert!(
        !adapter
            .capabilities()
            .await
            .expect("Herdr capabilities")
            .supports_native_cancellation
    );
    assert_eq!(
        adapter
            .validate_native_batch(&[&candidate("native.zellij.command:close-focus")])
            .expect_err("other host namespace is rejected")[0]
            .code,
        DiagnosticCode::NativeActionRejected
    );
    drive_shared_contract(
        &*adapter,
        &ContractExpectations {
            kind: HostKind::Herdr,
            supports_native_cancellation: false,
            valid_native: "native.herdr.agent:list",
        },
    )
    .await;
    fixture.wait_for_requests(2).await;
    assert_eq!(
        fixture
            .requests()
            .await
            .into_iter()
            .map(|request| request["method"].clone())
            .collect::<Vec<_>>(),
        vec![
            serde_json::json!("ping"),
            serde_json::json!("events.subscribe")
        ]
    );
    adapter.shutdown().await.expect("Herdr shutdown");
}

async fn self_attested_registration_is_contained() {
    let request = RecordedPipeChannel::new();
    let event = RecordedPipeChannel::new();
    let contained = ZellijAdapter::new(
        ZellijAdapterConfig {
            session_name: "session-alpha".to_owned(),
            zellij_exe: PathBuf::from("/nonexistent/zellij"),
        },
        Arc::clone(&request) as Arc<dyn PipeChannel>,
        Arc::clone(&event) as Arc<dyn PipeChannel>,
    );
    event.push_line(
        encode_event_line(&register_event(
            [9; 16],
            muxe_protocol::SchemaFingerprint([1; 32]),
        ))
        .expect("registration encodes"),
    );
    let rejection = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = contained
                .dispatch_native(NativeDispatchRequest {
                    execution: ExecutionId(8),
                    action: muxe_adapter_api::ResolvedNativeAction {
                        candidate: candidate("native.zellij.command:close-focus"),
                    },
                    origin: origin(),
                })
                .await;
            if let Err(error) = result
                && error.kind == AdapterErrorKind::Incompatible
            {
                return error;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("self-attestation is contained before timeout");
    assert!(rejection.message.contains("incompatible"));
    assert!(request.take_outbound().is_empty());
    contained
        .shutdown()
        .await
        .expect("contained Zellij adapter shutdown");
}

async fn await_registration_accepted(
    adapter: &ZellijAdapter,
    execution: ExecutionId,
) -> DispatchAccepted {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match adapter
                .dispatch_native(NativeDispatchRequest {
                    execution,
                    action: muxe_adapter_api::ResolvedNativeAction {
                        candidate: candidate("native.zellij.command:close-focus"),
                    },
                    origin: origin(),
                })
                .await
            {
                Ok(accepted) => break accepted,
                Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
    .await
    .expect("registration becomes active before dispatch timeout")
}

/// Replays a typed bridge registration and dispatch correlation through the
/// public Zellij pipe boundary, including the self-attestation containment path.
#[tokio::test]
async fn recorded_zellij_bridge_contract_targets_registration_and_contains_self_attestation() {
    let request = RecordedPipeChannel::new();
    let event = RecordedPipeChannel::new();
    let adapter = ZellijAdapter::new(
        ZellijAdapterConfig {
            session_name: "session-alpha".to_owned(),
            zellij_exe: PathBuf::from("/nonexistent/zellij"),
        },
        Arc::clone(&request) as Arc<dyn PipeChannel>,
        Arc::clone(&event) as Arc<dyn PipeChannel>,
    );
    assert!(
        !adapter
            .capabilities()
            .await
            .expect("Zellij capabilities")
            .supports_native_cancellation
    );
    assert!(
        adapter
            .validate_native_batch(&[&candidate("native.zellij.command:close-focus")])
            .is_ok()
    );

    event.push_line(
        encode_event_line(&register_event([7; 16], bridge_build_id()))
            .expect("registration encodes"),
    );
    let execution = ExecutionId(7);
    let accepted = await_registration_accepted(&adapter, execution).await;
    assert_eq!(accepted.execution, execution);
    assert_eq!(
        adapter
            .cancel(execution)
            .await
            .expect_err("Zellij native dispatch cannot be cancelled")
            .kind,
        AdapterErrorKind::CancelUnsupported
    );

    let frame = decode_request_line(&next_outbound(&request).await).expect("typed request frame");
    assert_eq!(frame.target.client_id, "client-1");
    assert_eq!(frame.target.registration, registration(7));
    assert!(matches!(frame.payload, BridgeRequest::Dispatch { .. }));
    event.push_line(
        encode_event_line(&event_for(
            registration(7),
            Some(frame.request_id),
            PipeEventKind::RequestReleased,
        ))
        .expect("release encodes"),
    );
    event.push_line(
        encode_event_line(&event_for(
            registration(7),
            Some(frame.request_id),
            PipeEventKind::DispatchCompleted {
                execution: execution.0.to_string(),
                outcome: CommandOutcome::succeeded(),
            },
        ))
        .expect("completion encodes"),
    );
    let first = tokio::time::timeout(Duration::from_secs(2), adapter.next_health_event())
        .await
        .expect("registration health event arrives")
        .expect("registration health event is valid");
    let completion = match first {
        AdapterHealthEvent::Healthy { .. } => adapter
            .next_health_event()
            .await
            .expect("dispatch completion arrives"),
        event => event,
    };
    assert!(matches!(
        completion,
        AdapterHealthEvent::DispatchCompleted(DispatchCompletion::Succeeded {
            execution: ExecutionId(7)
        })
    ));
    drive_shared_contract(
        &adapter,
        &ContractExpectations {
            kind: HostKind::Zellij,
            supports_native_cancellation: false,
            valid_native: "native.zellij.command:close-focus",
        },
    )
    .await;
    adapter.shutdown().await.expect("Zellij adapter shutdown");
    self_attested_registration_is_contained().await;
}

/// Drives the production monitor through a retained-subscription loss with
/// recorded ping/probe/subscribe exchanges: the adapter reports Unhealthy,
/// reconnects without restarting the broker, and reports the reconnected
/// endpoint identity. No Herdr host process is started.
#[tokio::test]
async fn recorded_herdr_reconnect_reestablishes_subscription_with_recorded_messages() {
    let mut exchanges = ProductionConnectFixture::initial_handshake();
    exchanges.push(ProductionConnectFixture::ping_exchange());
    exchanges.push(ProductionConnectFixture::subscription_exchange());
    let fixture = ProductionConnectFixture::start_scripted(exchanges)
        .expect("recorded reconnect fixture starts");
    let adapter = HerdrAdapter::connect(fixture.adapter_config())
        .await
        .expect("production Herdr connect accepts the recorded handshake");
    let before = adapter
        .identity()
        .await
        .expect("identity before continuity loss");
    let connected = tokio::time::timeout(Duration::from_secs(10), adapter.next_health_event())
        .await
        .expect("connect reports promptly")
        .expect("connect is a valid event");
    assert!(
        matches!(
            connected,
            AdapterHealthEvent::Healthy { ref identity } if *identity == before
        ),
        "connect reports the live endpoint as Healthy"
    );
    fixture.lose_retained_subscriptions();
    let unhealthy = tokio::time::timeout(Duration::from_secs(10), adapter.next_health_event())
        .await
        .expect("continuity loss surfaces promptly")
        .expect("continuity loss is a valid event");
    assert!(
        matches!(unhealthy, AdapterHealthEvent::Unhealthy { .. }),
        "continuity loss reports Unhealthy, never silent health"
    );
    let reconnected = tokio::time::timeout(Duration::from_secs(10), adapter.next_health_event())
        .await
        .expect("the monitor reconnects through recorded messages")
        .expect("reconnect is a valid event");
    let AdapterHealthEvent::Reconnected { previous, current } = reconnected else {
        panic!("expected a Reconnected event after the recorded reconnect")
    };
    assert_eq!(previous, before);
    // Endpoint peer credentials are diagnostic only and may legitimately
    // differ after a reconnect; continuity rests on the fresh subscription
    // plus the epoch, never on equal endpoint observations.
    assert_eq!(current.kind, HostKind::Herdr);
    assert_eq!(current.discovery_key, before.discovery_key);
    assert_eq!(
        adapter.identity().await.expect("identity after reconnect"),
        current
    );
    // The fresh epoch still enforces origin currency: an uncaptured origin
    // is rejected as stale-context, proving the adapter is back to
    // healthy-epoch operation rather than stuck unhealthy or promiscuous.
    assert_eq!(
        adapter
            .dispatch_native(NativeDispatchRequest {
                execution: ExecutionId(7_700_001),
                action: muxe_adapter_api::ResolvedNativeAction {
                    candidate: candidate("native.herdr.agent:list"),
                },
                origin: origin(),
            })
            .await
            .expect_err("an uncaptured origin stays rejected after reconnect")
            .kind,
        AdapterErrorKind::ContextUnavailable
    );
    adapter.shutdown().await.expect("Herdr shutdown");
    assert!(matches!(
        adapter.next_health_event().await,
        Err(error) if error.kind == AdapterErrorKind::Shutdown
    ));
}
