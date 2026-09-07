use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use muxe_adapter_api::{
    AdapterCapabilities, AdapterError, AdapterErrorKind, AdapterHealthEvent, CaptureLease,
    CaptureReleaseReason, CaptureRequest, DispatchAccepted, DispatchCompletion,
    ExecutionCorrelationId, HostAdapter, HostIdentity, KeyboardCapabilities, ModalScopeId,
    NativeDispatchRequest, OriginCaptureRequest, PendingPaneLease, PendingPaneLeaseId,
    PendingPaneRegistration, PortableDispatchRequest,
};
use muxe_core::{
    ActionScalar, ActionValidation, ActionValidator, ConfigDiagnostic, ConfigValueKind,
    DiagnosticCode, ExecutionCapabilities, NativeActionCandidate, PaneAction, PortableAction,
    TabAction,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;

use crate::{
    ApiSchema, ComparisonKey, DeliveryState, EventSubscription, HerdrAdapterConfig, HerdrCache,
    HerdrResponse, HerdrRuntime, HerdrSocketClient, SocketError, SubscriptionConfig,
    fields_to_json,
    generated::{BUNDLED_REQUEST_SCHEMA_SHA256, method_metadata},
    validate_candidate,
};

const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const SUBSCRIPTION_SILENCE: Duration = Duration::from_secs(30);
const RECONNECT_RETRY: Duration = Duration::from_secs(1);

pub struct HerdrAdapter {
    runtime: RwLock<Arc<HerdrRuntime>>,
    config: HerdrAdapterConfig,
    cache: HerdrCache,
    identity: RwLock<HostIdentity>,
    continuity: RwLock<ContinuityState>,
    events_tx: mpsc::Sender<AdapterHealthEvent>,
    events_rx: Mutex<mpsc::Receiver<AdapterHealthEvent>>,
    next_correlation: AtomicU64,
    shutdown: AtomicBool,
    suspended: AtomicBool,
    suspend_wake: Notify,
    suspended_ack: Notify,
    resume_wake: Notify,
    resume_slot: Mutex<Option<EventSubscription>>,
    // The monitor owns the retained subscription socket. Shutdown takes and awaits it
    // so return proves the subscription task stopped; witnesses can observe the stop
    // by the released adapter references.
    monitor: Mutex<Option<JoinHandle<()>>>,
    pending_leases: Mutex<HashMap<String, (u64, muxe_adapter_api::UiSessionId)>>,
}
struct ContinuityState {
    epoch: u64,
    healthy: bool,
}

impl HerdrAdapter {
    /// Connects one adapter: loads the runtime schema, establishes the retained
    /// event subscription, and starts its monitor.
    ///
    /// # Errors
    ///
    /// Returns `AdapterError` when the runtime cannot be loaded or the retained
    /// event subscription cannot be established.
    pub async fn connect(config: HerdrAdapterConfig) -> Result<Arc<Self>, AdapterError> {
        let cache = HerdrCache::new(&config.cache_dir);
        let runtime = Arc::new(HerdrRuntime::connect(config.clone()).await?);
        let subscription_config = subscription_config();
        let (subscription, _) = EventSubscription::connect(runtime.client(), subscription_config)
            .await
            .map_err(|error| socket_error(&error))?;
        let identity = runtime.identity().clone();
        let (events_tx, events_rx) = mpsc::channel(64);
        let adapter = Arc::new(Self {
            runtime: RwLock::new(runtime),
            config,
            cache,
            identity: RwLock::new(identity.clone()),
            continuity: RwLock::new(ContinuityState {
                epoch: 1,
                healthy: true,
            }),
            events_tx,
            events_rx: Mutex::new(events_rx),
            next_correlation: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            suspended: AtomicBool::new(false),
            suspend_wake: Notify::new(),
            pending_leases: Mutex::new(HashMap::new()),
            suspended_ack: Notify::new(),
            resume_wake: Notify::new(),
            resume_slot: Mutex::new(None),
            monitor: Mutex::new(None),
        });
        let _ = adapter
            .events_tx
            .try_send(AdapterHealthEvent::Healthy { identity });
        adapter
            .monitor
            .lock()
            .await
            .replace(tokio::spawn(monitor_subscription(
                Arc::clone(&adapter),
                subscription,
            )));
        Ok(adapter)
    }

    pub fn runtime(&self) -> Arc<HerdrRuntime> {
        self.runtime
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn identity(&self) -> HostIdentity {
        self.identity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn require_continuity(&self) -> Result<u64, AdapterError> {
        let continuity = self
            .continuity
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if continuity.healthy {
            Ok(continuity.epoch)
        } else {
            Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Herdr retained event-subscription continuity is unavailable; host-bound operations are blocked until a new epoch completes compatibility validation",
            ))
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn require_current_origin(
        &self,
        origin: &muxe_core::OriginContext,
    ) -> Result<u64, AdapterError> {
        let epoch = self.require_continuity()?;
        if !origin_is_current(origin, &self.identity(), epoch) {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "captured Herdr origin belongs to a prior continuity epoch",
            ));
        }
        Ok(epoch)
    }

    fn validate_native_batch_cached(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let runtime = self.runtime();
        let key = ComparisonKey {
            bundled_schema_hash: BUNDLED_REQUEST_SCHEMA_SHA256.to_owned(),
            runtime_schema_hash: runtime.schema().canonical_request_sha256().to_owned(),
            configured_requests_hash: crate::hash_configured_request_refs(candidates),
        };
        if let Some(outcomes) = self.cache.comparison_lookup(&key)
            && outcomes.len() == candidates.len()
        {
            let diagnostics = candidates
                .iter()
                .zip(outcomes)
                .filter(|&(_, outcome)| !outcome)
                .map(|(candidate, _)| {
                    native_diagnostic(candidate, "cached Herdr compatibility rejection")
                })
                .collect::<Vec<_>>();
            return if diagnostics.is_empty() {
                Ok(vec![
                    ActionValidation {
                        execution: ExecutionCapabilities::ASYNCHRONOUS,
                    };
                    candidates.len()
                ])
            } else {
                Err(diagnostics)
            };
        }

        let mut outcomes = Vec::with_capacity(candidates.len());
        let mut diagnostics = Vec::new();
        for candidate in candidates {
            match validate_candidate(runtime.schema(), candidate) {
                Ok(_) => outcomes.push(true),
                Err(error) => {
                    outcomes.push(false);
                    diagnostics.push(native_diagnostic(candidate, &error.error.to_string()));
                }
            }
        }
        if let Err(error) = self.cache.comparison_store(&key, &outcomes) {
            diagnostics.push(native_diagnostic(
                candidates[0],
                &format!("could not update Herdr compatibility cache: {error}"),
            ));
        }
        if diagnostics.is_empty() {
            Ok(vec![
                ActionValidation {
                    execution: ExecutionCapabilities::ASYNCHRONOUS,
                };
                candidates.len()
            ])
        } else {
            Err(diagnostics)
        }
    }

    fn portable_compile_validation(
        &self,
        action: &PortableAction,
    ) -> Result<ExecutionCapabilities, String> {
        let runtime = self.runtime();
        let method = match action {
            PortableAction::Menu(_) | PortableAction::Config(_) => {
                return Ok(ExecutionCapabilities::SYNCHRONOUS);
            }
            // Commands run in the broker, not the host adapter. Their ownership and capability
            // contract must therefore remain independent of Herdr's RPC inventory.
            PortableAction::Command(_) => {
                return Ok(ExecutionCapabilities {
                    awaitable: true,
                    detachable: true,
                    cancellable: true,
                });
            }
            PortableAction::Keyboard(muxe_core::KeyboardAction::SendKeys(_)) => "pane.send_keys",
            PortableAction::Keyboard(muxe_core::KeyboardAction::SendText(_)) => "pane.send_text",
            PortableAction::Tab(TabAction::Create { .. }) => "tab.create",
            PortableAction::Tab(TabAction::Close) => "tab.close",
            PortableAction::Tab(TabAction::Rename { name: Some(_) }) => "tab.rename",
            PortableAction::Tab(TabAction::Rename { name: None }) => {
                return Err(
                    "Herdr tab.rename requires `label`; the portable bare `tab:rename` has no specified Herdr prompt mapping"
                        .to_owned(),
                );
            }
            PortableAction::Tab(TabAction::Move(target)) if target_is_index(target) => "tab.move",
            PortableAction::Tab(TabAction::Swap(target)) if target_is_index(target) => {
                for method in ["tab.list", "tab.move"] {
                    if method_metadata(method).is_none() || runtime.schema().method(method).is_none() {
                        return Err(format!(
                            "active Herdr schema does not declare required method {method}"
                        ));
                    }
                }
                return Ok(ExecutionCapabilities::ASYNCHRONOUS);
            }
            PortableAction::Pane(PaneAction::Create) => {
                return Err(
                    "Herdr has no pane.create method; pane.split requires an explicit right or down direction"
                        .to_owned(),
                );
            }
            PortableAction::Pane(PaneAction::Split {
                direction: Some(direction),
            }) if split_direction_is_supported(direction) => "pane.split",
            PortableAction::Pane(PaneAction::Split { direction: None }) => {
                return Err(
                    "Herdr pane.split requires an explicit right or down direction".to_owned(),
                );
            }
            PortableAction::Pane(PaneAction::Split { .. }) => {
                return Err("Herdr pane.split supports only right or down directions".to_owned());
            }
            PortableAction::Pane(PaneAction::Close) => "pane.close",
            PortableAction::Pane(PaneAction::Focus(target))
                if target_is_cardinal_direction(target) =>
            {
                "pane.focus_direction"
            }
            PortableAction::Pane(PaneAction::Swap(target))
                if target_is_cardinal_direction(target) =>
            {
                "pane.swap"
            }
            PortableAction::Pane(PaneAction::Resize { .. }) => "pane.resize",
            PortableAction::Pane(PaneAction::Zoom { .. }) => "pane.zoom",
            PortableAction::Tab(TabAction::Focus(_)) => return Err("Herdr tab.focus targets a tab ID; portable index/direction focus requires a list-to-ID bridge that protocol 20 does not expose as a typed action".to_owned()),
            PortableAction::Tab(TabAction::Move(_)) => return Err("Herdr tab.move supports only a concrete insert index".to_owned()),
            PortableAction::Tab(TabAction::Swap(_)) => return Err("Herdr tab:swap supports only an index; the schema offers tab.list plus tab.move, not directional tab targeting".to_owned()),
            PortableAction::Pane(PaneAction::Focus(_)) => return Err("Herdr pane.focus supports only cardinal directions".to_owned()),
            PortableAction::Pane(PaneAction::Move(_)) => return Err("Herdr pane.move requires an explicit tab/new-tab destination, not a portable index or direction".to_owned()),
            PortableAction::Pane(PaneAction::Swap(_)) => return Err("Herdr pane.swap supports only cardinal directions".to_owned()),
            PortableAction::Pane(PaneAction::Fullscreen { .. }) => return Err("Herdr protocol 20 exposes no pane fullscreen method".to_owned()),
            PortableAction::Pane(PaneAction::Floating { .. }) => return Err("Herdr protocol 20 exposes no pane floating method".to_owned()),
            PortableAction::Pane(PaneAction::Frame { .. }) => return Err("Herdr protocol 20 exposes no pane frame method".to_owned()),
            PortableAction::Session(_) => return Err("Herdr protocol 20 exposes only read-only session.snapshot; it has no portable session lifecycle methods".to_owned()),
        };
        if method_metadata(method).is_none() || runtime.schema().method(method).is_none() {
            return Err(format!(
                "active Herdr schema does not declare required method {method}"
            ));
        }
        Ok(ExecutionCapabilities::ASYNCHRONOUS)
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn dispatch(
        &self,
        execution: muxe_core::ExecutionId,
        invocation: Invocation,
    ) -> Result<DispatchAccepted, AdapterError> {
        let runtime = self.runtime();
        runtime
            .schema()
            .validate_method(invocation.method, &invocation.params)
            .map_err(|error| {
                incompatible(format!(
                    "active Herdr schema rejects {}: {error}",
                    invocation.method
                ))
            })?;
        let metadata = method_metadata(invocation.method).ok_or_else(|| {
            incompatible(format!(
                "bundled Herdr metadata does not declare {}",
                invocation.method
            ))
        })?;
        let client = Arc::clone(runtime.client());
        let sender = self.events_tx.clone();
        tokio::spawn(async move {
            let completion = match client.unary(metadata, invocation.params).await {
                Ok(HerdrResponse::Success(_)) => DispatchCompletion::Succeeded { execution },
                Ok(HerdrResponse::Error { code, message }) => DispatchCompletion::Failed {
                    execution,
                    error: AdapterError::new(
                        AdapterErrorKind::DispatchFailed,
                        format!(
                            "Herdr {} rejected request with {code}: {message}",
                            metadata.method
                        ),
                    ),
                },
                Err(error) if error.delivery() == DeliveryState::MayHaveReachedHost => {
                    DispatchCompletion::OutcomeUnknown {
                        execution,
                        error: socket_error(&error),
                    }
                }
                Err(error) => DispatchCompletion::Failed {
                    execution,
                    error: socket_error(&error),
                },
            };
            let _ = sender
                .send(AdapterHealthEvent::DispatchCompleted(completion))
                .await;
        });
        Ok(DispatchAccepted {
            correlation: ExecutionCorrelationId::new(format!(
                "herdr-{}",
                self.next_correlation.fetch_add(1, Ordering::Relaxed)
            )),
            execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn dispatch_tab_swap(
        &self,
        execution: muxe_core::ExecutionId,
        origin: &muxe_core::OriginContext,
        target_index: u64,
    ) -> Result<DispatchAccepted, AdapterError> {
        let runtime = self.runtime();
        let source_tab = origin_tab(origin)?.to_owned();
        let workspace = origin
            .workspace_id
            .as_ref()
            .map(|workspace| workspace.as_str().to_owned())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Herdr tab:swap requires the captured origin workspace",
                )
            })?;
        for method in ["tab.list", "tab.move"] {
            if runtime.schema().method(method).is_none() {
                return Err(incompatible(format!(
                    "active Herdr schema does not declare required method {method}"
                )));
            }
            if method_metadata(method).is_none() {
                return Err(incompatible(format!(
                    "bundled Herdr metadata does not declare required method {method}"
                )));
            }
        }
        let client = Arc::clone(runtime.client());
        let schema = Arc::clone(runtime.schema());
        let sender = self.events_tx.clone();
        tokio::spawn(async move {
            let completion =
                match perform_tab_swap(&client, &schema, &workspace, &source_tab, target_index)
                    .await
                {
                    Ok(()) => DispatchCompletion::Succeeded { execution },
                    Err(TabSwapError::Known(message)) => DispatchCompletion::Failed {
                        execution,
                        error: AdapterError::new(AdapterErrorKind::DispatchFailed, message),
                    },
                    Err(TabSwapError::Unknown(message)) => DispatchCompletion::OutcomeUnknown {
                        execution,
                        error: AdapterError::new(AdapterErrorKind::OutcomeUnknown, message),
                    },
                };
            let _ = sender
                .send(AdapterHealthEvent::DispatchCompleted(completion))
                .await;
        });
        Ok(DispatchAccepted {
            correlation: ExecutionCorrelationId::new(format!(
                "herdr-{}",
                self.next_correlation.fetch_add(1, Ordering::Relaxed)
            )),
            execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    async fn invoke_unary(&self, method: &str, params: Value) -> Result<Value, AdapterError> {
        self.require_continuity()?;
        let runtime = self.runtime();
        runtime
            .schema()
            .validate_method(method, &params)
            .map_err(|error| {
                incompatible(format!("active Herdr schema rejects {method}: {error}"))
            })?;
        let metadata = method_metadata(method).ok_or_else(|| {
            incompatible(format!("bundled Herdr metadata does not declare {method}"))
        })?;
        match runtime
            .client()
            .unary(metadata, params)
            .await
            .map_err(|error| socket_error(&error))?
        {
            HerdrResponse::Success(result) => Ok(result),
            HerdrResponse::Error { code, message } => Err(AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} rejected request with {code}: {message}"),
            )),
        }
    }
}

impl ActionValidator for HerdrAdapter {
    fn validate_portable(
        &self,
        action: &PortableAction,
        action_span: &muxe_core::SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        self.portable_compile_validation(action)
            .map(|execution| ActionValidation { execution })
            .map_err(|message| {
                ConfigDiagnostic::error(DiagnosticCode::InvalidAction, message, action_span.clone())
            })
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        self.validate_native_batch_cached(candidates)
    }
}

#[async_trait]
impl HostAdapter for HerdrAdapter {
    async fn identity(&self) -> Result<HostIdentity, AdapterError> {
        self.require_continuity()?;
        Ok(self.identity())
    }

    async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
        Ok(AdapterCapabilities {
            // Static compile-time caps only: these gate binding validation, not the
            // runtime pty negotiation the UI performs directly. All false is
            // fail-closed (kitty enhancements rejected at compile, vt100 default
            // works; kitty_baseline is currently unread). DESIGN lists Herdr
            // defaults event-types/alternate as true, but flipping them requires a
            // live 0.8.2 forwarding proof through a real pane — do not change
            // without it.
            keyboard: KeyboardCapabilities {
                kitty_baseline: false,
                kitty_event_types: false,
                kitty_alternate_keys: false,
                kitty_all_keys_as_escape_codes: false,
            },
            supports_capture: false,
            supports_notifications: method_metadata("notification.show").is_some()
                && self
                    .runtime()
                    .schema()
                    .method("notification.show")
                    .is_some(),
            supports_native_cancellation: false,
        })
    }

    async fn modal_scope(&self, ui_pane: &muxe_core::PaneId) -> Result<ModalScopeId, AdapterError> {
        let result = self
            .invoke_unary("pane.get", json!({ "pane_id": ui_pane.as_str() }))
            .await?;
        let workspace = crate::pane_info(&result)
            .and_then(|pane| pane.get("workspace_id"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Herdr pane.get lacks pane.workspace_id",
                )
            })?;
        Ok(ModalScopeId::new(workspace))
    }

    async fn begin_capture(&self, _request: CaptureRequest) -> Result<CaptureLease, AdapterError> {
        Err(incompatible(
            "Herdr protocol 20 has no host input-capture or restoration method; refusing to fabricate a capture lease",
        ))
    }

    async fn end_capture(
        &self,
        _lease: CaptureLease,
        _reason: CaptureReleaseReason,
    ) -> Result<(), AdapterError> {
        Err(incompatible(
            "Herdr protocol 20 has no host input-capture restoration method",
        ))
    }

    async fn register_pending_pane(
        &self,
        registration: PendingPaneRegistration,
    ) -> Result<PendingPaneLease, AdapterError> {
        let epoch = self.require_continuity()?;
        let pane = self
            .invoke_unary("pane.get", json!({ "pane_id": registration.pane.as_str() }))
            .await?;
        let object = crate::pane_info(&pane).ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr pane.get returned an invalid pane_info response",
            )
        })?;
        let actual = object.get("pane_id").and_then(Value::as_str);
        if actual != Some(registration.pane.as_str()) {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr registered pane is not live",
            ));
        }
        if let Some(tab) = &registration.temporary_tab
            && object.get("tab_id").and_then(Value::as_str) != Some(tab.as_str())
        {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr registered pane is not in its temporary tab",
            ));
        }
        let id = format!(
            "herdr:{epoch}:{}:{}",
            registration.ui_session, registration.pane
        );
        self.pending_leases
            .lock()
            .await
            .insert(id.clone(), (epoch, registration.ui_session.clone()));
        Ok(PendingPaneLease {
            id: PendingPaneLeaseId::new(id),
            ui_session: registration.ui_session,
        })
    }

    async fn close_pending_pane(
        &self,
        registration: PendingPaneRegistration,
        lease: PendingPaneLease,
    ) -> Result<(), AdapterError> {
        let epoch = self.require_continuity()?;
        if lease.ui_session != registration.ui_session
            || !self
                .pending_leases
                .lock()
                .await
                .get(lease.id.as_str())
                .is_some_and(|(owned_epoch, session)| {
                    *owned_epoch == epoch && session == &lease.ui_session
                })
        {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "pending Herdr cleanup lease is stale",
            ));
        }
        let pane = self
            .invoke_unary("pane.get", json!({ "pane_id": registration.pane.as_str() }))
            .await?;
        let object = crate::pane_info(&pane).ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr pane.get returned an invalid pane_info response",
            )
        })?;
        if object.get("pane_id").and_then(Value::as_str) != Some(registration.pane.as_str()) {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "pending Herdr pane identity changed",
            ));
        }
        if let Some(tab) = registration.temporary_tab {
            if object.get("tab_id").and_then(Value::as_str) == Some(tab.as_str()) {
                self.invoke_unary("tab.close", json!({ "tab_id": tab.as_str() }))
                    .await?;
            } else {
                self.invoke_unary(
                    "pane.close",
                    json!({ "pane_id": registration.pane.as_str() }),
                )
                .await?;
            }
        } else {
            self.invoke_unary(
                "pane.close",
                json!({ "pane_id": registration.pane.as_str() }),
            )
            .await?;
        }
        self.pending_leases.lock().await.remove(lease.id.as_str());
        Ok(())
    }

    async fn release_pending_pane(&self, lease: PendingPaneLease) -> Result<(), AdapterError> {
        let epoch = self.require_continuity()?;
        if !self
            .pending_leases
            .lock()
            .await
            .get(lease.id.as_str())
            .is_some_and(|(owned_epoch, session)| {
                *owned_epoch == epoch && session == &lease.ui_session
            })
        {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "pending Herdr cleanup lease is stale",
            ));
        }
        self.pending_leases.lock().await.remove(lease.id.as_str());
        Ok(())
    }

    async fn capture_origin(
        &self,
        request: OriginCaptureRequest,
    ) -> Result<muxe_core::OriginContext, AdapterError> {
        let epoch = self.require_continuity()?;
        let runtime = self.runtime();
        let origin = crate::origin::capture_origin(
            runtime.client(),
            &request,
            muxe_core::ServerId::new(origin_epoch_token(&self.identity(), epoch)),
        )
        .await?;
        Ok(origin)
    }

    async fn dispatch_portable(
        &self,
        request: PortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_current_origin(&request.origin)?;
        if let PortableAction::Tab(TabAction::Swap(muxe_core::IndexOrDirection::Index(index))) =
            &request.action.action
        {
            return self.dispatch_tab_swap(
                request.execution,
                &request.origin,
                scalar_index(index)?,
            );
        }
        let invocation = portable_invocation(&request.action.action, &request.origin)?;
        self.dispatch(request.execution, invocation)
    }

    async fn dispatch_native(
        &self,
        request: NativeDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_current_origin(&request.origin)?;
        let runtime = self.runtime();
        let metadata =
            validate_candidate(runtime.schema(), &request.action.candidate).map_err(|error| {
                incompatible(format!(
                    "active Herdr schema rejects native action: {}",
                    error.error
                ))
            })?;
        let params = fields_to_json(&request.action.candidate.fields)
            .map(Value::Object)
            .map_err(|error| {
                incompatible(format!("could not serialize native Herdr action: {error}"))
            })?;
        self.dispatch(
            request.execution,
            Invocation {
                method: metadata.method,
                params,
            },
        )
    }

    async fn cancel(&self, _execution: muxe_core::ExecutionId) -> Result<(), AdapterError> {
        Err(AdapterError::new(
            AdapterErrorKind::CancelUnsupported,
            "Herdr protocol 20 has no cancellation request for unary operations",
        ))
    }

    async fn next_health_event(&self) -> Result<AdapterHealthEvent, AdapterError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Herdr adapter is shut down",
            ));
        }
        self.events_rx.lock().await.recv().await.ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Herdr adapter event channel closed",
            )
        })
    }

    async fn suspend_for_activation(&self) -> Result<(), AdapterError> {
        if self.suspended.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // Fail closed while the old stream drains: no host-bound operation may
        // proceed on a subscription that is about to be released to a target.
        {
            let mut continuity = self
                .continuity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continuity.healthy = false;
        }
        self.pending_leases.lock().await.clear();

        self.suspend_wake.notify_one();
        // The monitor acks only after dropping the subscription socket, so this
        // await proves the old server observed the disconnect before any target
        // connects. Single-flight: the coordinator serializes Prepare/Abort.
        self.suspended_ack.notified().await;
        let _ = self
            .events_tx
            .send(AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Herdr retained event-subscription suspended for activation",
                ),
            })
            .await;
        Ok(())
    }

    async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
        if !self.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Herdr adapter is not suspended for activation; refusing to fabricate a resumed subscription",
            ));
        }
        let previous = self.identity();
        // Fresh schema validation from the exact installed executable plus a live
        // probe. Any failure leaves the adapter unhealthy with the journal
        // preserved; rollback is never claimed healthy.
        let refreshed = HerdrRuntime::connect(self.config.clone())
            .await
            .map_err(|error| {
                AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!(
                        "Herdr activation resume could not revalidate the retained host: {error}"
                    ),
                )
            })?;
        let refreshed = Arc::new(refreshed);
        let (subscription, _) =
            EventSubscription::connect(refreshed.client(), subscription_config())
                .await
                .map_err(|error| socket_error(&error))?;
        let current = refreshed.identity().clone();
        {
            let mut runtime = self
                .runtime
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *runtime = refreshed;
        }
        {
            let mut identity = self
                .identity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *identity = current.clone();
        }
        {
            // A fresh local epoch: raw endpoint equality is diagnostic only and
            // never continuity proof, so stale origins stay rejected.
            let mut continuity = self
                .continuity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continuity.epoch = continuity.epoch.saturating_add(1);
            continuity.healthy = true;
        }
        {
            let mut slot = self.resume_slot.lock().await;
            *slot = Some(subscription);
        }
        // Clear the flag before waking: the parked monitor must observe a live
        // adapter when it takes the handed-over subscription.
        self.suspended.store(false, Ordering::SeqCst);
        self.resume_wake.notify_one();
        let _ = self
            .events_tx
            .send(AdapterHealthEvent::Reconnected { previous, current })
            .await;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        self.shutdown.store(true, Ordering::Relaxed);
        self.pending_leases.lock().await.clear();
        // Wake a monitor parked in event reads, reconnect backoff, or the
        // post-suspend resume wait so shutdown never wedges on a live stream.
        self.suspend_wake.notify_one();
        self.resume_wake.notify_one();
        // Await the monitor instead of merely waking it: return proves the retained
        // subscription socket and its task stopped. The monitor releases its adapter
        // reference on exit, so witnesses observe the stop through reference release.
        if let Some(monitor) = self.monitor.lock().await.take() {
            let _ = monitor.await;
        }
        Ok(())
    }
}

fn subscription_config() -> SubscriptionConfig {
    SubscriptionConfig {
        params: json!({ "subscriptions": [{ "type": "tab.focused" }] }),
        subscribe_timeout: SUBSCRIBE_TIMEOUT,
        max_silence: SUBSCRIPTION_SILENCE,
    }
}

fn origin_epoch_token(identity: &HostIdentity, epoch: u64) -> String {
    format!("{}#continuity-{epoch}", identity.live_server_id)
}

fn origin_is_current(
    origin: &muxe_core::OriginContext,
    identity: &HostIdentity,
    epoch: u64,
) -> bool {
    origin.server_id.as_str() == origin_epoch_token(identity, epoch)
}

async fn monitor_subscription(adapter: Arc<HerdrAdapter>, mut subscription: EventSubscription) {
    loop {
        if adapter.suspended.load(Ordering::SeqCst) {
            // Prove the retained stream is closed before a target connects:
            // dropping the subscription closes its socket, then ack. The
            // suspender waits for exactly this ack, so suspend returns only
            // after the old server observes the disconnect.
            drop(subscription);
            adapter.suspended_ack.notify_one();
            match take_resumed_subscription(&adapter).await {
                Some(next) => {
                    subscription = next;
                    continue;
                }
                None => return,
            }
        }
        if adapter.shutdown.load(Ordering::Relaxed) {
            return;
        }
        let failed = tokio::select! {
            result = subscription.next_event() => result.is_err(),
            () = adapter.suspend_wake.notified() => false,
        };
        if !failed {
            continue;
        }

        {
            let mut continuity = adapter
                .continuity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continuity.epoch = continuity.epoch.saturating_add(1);
            continuity.healthy = false;
        }
        adapter.pending_leases.lock().await.clear();
        let _ = adapter
            .events_tx
            .send(AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Herdr retained event-subscription continuity was lost",
                ),
            })
            .await;

        match reconnect_subscription(&adapter, &mut subscription).await {
            ReconnectOutcome::Reconnected | ReconnectOutcome::SuspendRequested => {}
            ReconnectOutcome::Stop => return,
        }
    }
}

/// Outcome of one monitor reconnect attempt loop.
enum ReconnectOutcome {
    /// A fresh subscription is installed and the monitor resumes event reads.
    Reconnected,
    /// Shutdown was requested; the monitor must exit.
    Stop,
    /// Suspend was requested; the caller drops the old stream, acks, and parks.
    SuspendRequested,
}

async fn reconnect_subscription(
    adapter: &Arc<HerdrAdapter>,
    subscription: &mut EventSubscription,
) -> ReconnectOutcome {
    loop {
        if adapter.shutdown.load(Ordering::Relaxed) {
            return ReconnectOutcome::Stop;
        }
        if adapter.suspended.load(Ordering::SeqCst) {
            return ReconnectOutcome::SuspendRequested;
        }
        let refreshed = tokio::select! {
            refreshed = HerdrRuntime::connect(adapter.config.clone()) => Some(refreshed),
            () = adapter.suspend_wake.notified() => None,
        };
        // A suspend wake re-checks the flag at the loop head so the old stream
        // is dropped and acked promptly. Dropping the connect future kills only
        // the owned schema child via `kill_on_drop`; no global cleanup runs.
        let Some(refreshed) = refreshed else {
            continue;
        };
        let Ok(refreshed) = refreshed else {
            if !interruptible_sleep(adapter).await {
                return ReconnectOutcome::Stop;
            }
            continue;
        };
        let refreshed = Arc::new(refreshed);
        let reconnected = tokio::select! {
            reconnected = EventSubscription::connect(refreshed.client(), subscription_config()) => {
                Some(reconnected)
            }
            () = adapter.suspend_wake.notified() => None,
        };
        let Some(reconnected) = reconnected else {
            continue;
        };
        let Ok((next_subscription, _)) = reconnected else {
            if !interruptible_sleep(adapter).await {
                return ReconnectOutcome::Stop;
            }
            continue;
        };
        // Endpoint metadata is diagnostic observation only. A new subscription plus this
        // monotonically increasing local epoch is the sole authority after continuity loss:
        // no equal device/inode/peer/version observation can revive stale origins.
        let previous = adapter.identity();
        let current = refreshed.identity().clone();
        {
            let mut runtime = adapter
                .runtime
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *runtime = refreshed;
        }
        {
            let mut identity = adapter
                .identity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *identity = current.clone();
        }
        {
            let mut continuity = adapter
                .continuity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continuity.healthy = true;
        }
        let _ = adapter
            .events_tx
            .send(AdapterHealthEvent::Reconnected { previous, current })
            .await;
        *subscription = next_subscription;
        return ReconnectOutcome::Reconnected;
    }
}

/// Sleeps between reconnect attempts. Returns false only for terminal shutdown;
/// a suspend wake returns true so the caller re-checks the suspend flag promptly.
async fn interruptible_sleep(adapter: &Arc<HerdrAdapter>) -> bool {
    tokio::select! {
        () = tokio::time::sleep(RECONNECT_RETRY) => true,
        () = adapter.suspend_wake.notified() => !adapter.shutdown.load(Ordering::Relaxed),
    }
}

/// Parks the monitor after it proved the old stream closed, until resume hands
/// over a fresh subscription or shutdown arrives. Returns `None` on shutdown.
async fn take_resumed_subscription(adapter: &Arc<HerdrAdapter>) -> Option<EventSubscription> {
    loop {
        {
            let mut slot = adapter.resume_slot.lock().await;
            if let Some(next) = slot.take() {
                return Some(next);
            }
        }
        if adapter.shutdown.load(Ordering::Relaxed) {
            return None;
        }
        tokio::select! {
            () = adapter.resume_wake.notified() => {}
            () = adapter.suspend_wake.notified() => {}
        }
    }
}

struct Invocation {
    method: &'static str,
    params: Value,
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn portable_invocation(
    action: &PortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<Invocation, AdapterError> {
    let pane = origin_pane(origin)?;
    match action {
        PortableAction::Keyboard(muxe_core::KeyboardAction::SendKeys(keys)) => Ok(Invocation {
            method: "pane.send_keys",
            params: json!({ "pane_id": pane, "keys": keys.iter().map(scalar_string).collect::<Result<Vec<_>, _>>()? }),
        }),
        PortableAction::Keyboard(muxe_core::KeyboardAction::SendText(text)) => Ok(Invocation {
            method: "pane.send_text",
            params: json!({ "pane_id": pane, "text": scalar_string(text)? }),
        }),
        PortableAction::Tab(TabAction::Create { workspace_id }) => Ok(Invocation {
            method: "tab.create",
            params: json!({ "workspace_id": workspace_id.as_ref().map(scalar_string).transpose()? }),
        }),
        PortableAction::Tab(TabAction::Close) => Ok(Invocation {
            method: "tab.close",
            params: json!({ "tab_id": origin_tab(origin)? }),
        }),
        PortableAction::Tab(TabAction::Rename { name }) => {
            let label = name.as_ref().ok_or_else(|| {
                incompatible(
                    "Herdr tab.rename requires `label`; bare portable tab:rename has no host prompt mapping",
                )
            })?;
            Ok(Invocation {
                method: "tab.rename",
                params: json!({ "tab_id": origin_tab(origin)?, "label": scalar_string(label)? }),
            })
        }
        PortableAction::Tab(TabAction::Move(muxe_core::IndexOrDirection::Index(index))) => {
            Ok(Invocation {
                method: "tab.move",
                params: json!({ "tab_id": origin_tab(origin)?, "insert_index": scalar_index(index)? }),
            })
        }
        PortableAction::Pane(PaneAction::Create) => Err(incompatible(
            "Herdr has no pane.create method; pane.split requires an explicit direction",
        )),
        PortableAction::Pane(PaneAction::Split {
            direction: Some(direction),
        }) => Ok(Invocation {
            method: "pane.split",
            params: json!({ "target_pane_id": pane, "direction": scalar_split_direction(direction)? }),
        }),
        PortableAction::Pane(PaneAction::Split { direction: None }) => Err(incompatible(
            "Herdr pane.split requires an explicit right or down direction",
        )),
        PortableAction::Pane(PaneAction::Close) => Ok(Invocation {
            method: "pane.close",
            params: json!({ "pane_id": pane }),
        }),
        PortableAction::Pane(PaneAction::Focus(muxe_core::IndexOrDirection::Direction(
            direction,
        ))) => Ok(Invocation {
            method: "pane.focus_direction",
            params: json!({ "pane_id": pane, "direction": scalar_pane_direction(direction)? }),
        }),
        PortableAction::Pane(PaneAction::Swap(muxe_core::IndexOrDirection::Direction(
            direction,
        ))) => Ok(Invocation {
            method: "pane.swap",
            params: json!({ "pane_id": pane, "direction": scalar_pane_direction(direction)? }),
        }),
        PortableAction::Pane(PaneAction::Resize { direction, amount }) => Ok(Invocation {
            method: "pane.resize",
            params: json!({ "pane_id": pane, "direction": scalar_pane_direction(direction)?, "amount": amount.as_ref().map(scalar_number).transpose()? }),
        }),
        PortableAction::Pane(PaneAction::Zoom { enabled }) => Ok(Invocation {
            method: "pane.zoom",
            params: json!({ "pane_id": pane, "mode": enabled.as_ref().map(scalar_bool).transpose()?.map_or("toggle", |value| if value { "on" } else { "off" }) }),
        }),
        _ => Err(AdapterError::new(
            AdapterErrorKind::Incompatible,
            "portable action form is unavailable in Herdr protocol 20",
        )),
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderedTab {
    id: String,
    workspace: String,
    number: u64,
}
#[derive(Debug)]
enum TabSwapError {
    Known(String),
    Unknown(String),
}

async fn perform_tab_swap(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    workspace: &str,
    source_tab: &str,
    target_index: u64,
) -> Result<(), TabSwapError> {
    let initial = tab_list(
        &request_tab_swap(
            client,
            schema,
            "tab.list",
            json!({ "workspace_id": workspace }),
            "reading initial tab order",
            false,
        )
        .await?,
        "initial tab list",
    )?;
    let source = tab_at_id(&initial, source_tab, workspace, "captured origin tab")?;
    let target = tab_at_number(&initial, target_index, workspace, "requested tab index")?;
    if source.id == target.id {
        return Ok(());
    }

    let after_first = tab_list(
        &request_tab_swap(
            client,
            schema,
            "tab.move",
            json!({ "tab_id": target.id, "insert_index": source.number }),
            "moving target tab to captured tab index",
            true,
        )
        .await?,
        "first tab.move result",
    )?;
    if tab_at_id(&after_first, &target.id, workspace, "moved target tab")?.number != source.number {
        return Err(TabSwapError::Known(format!(
            "Herdr first tab.move did not place target tab at index {}; tab ordering may be partially changed and Muxe will not replay",
            source.number
        )));
    }

    let after_second = tab_list(
        &request_tab_swap(
            client,
            schema,
            "tab.move",
            json!({ "tab_id": source.id, "insert_index": target.number }),
            "moving captured tab to requested index",
            true,
        )
        .await?,
        "second tab.move result",
    )?;
    let source_after = tab_at_id(&after_second, &source.id, workspace, "captured origin tab")?;
    let target_after = tab_at_id(&after_second, &target.id, workspace, "target tab")?;
    if source_after.number != target.number || target_after.number != source.number {
        return Err(TabSwapError::Known(format!(
            "Herdr tab moves completed without producing the requested swap (expected {}<->{}) ; tab ordering is partially changed and Muxe will not replay",
            source.number, target.number
        )));
    }
    Ok(())
}

async fn request_tab_swap(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    method: &str,
    params: Value,
    phase: &str,
    state_changing: bool,
) -> Result<Value, TabSwapError> {
    schema.validate_method(method, &params).map_err(|error| {
        TabSwapError::Known(format!(
            "active Herdr schema rejects {method} during tab swap {phase}: {error}"
        ))
    })?;
    let metadata = method_metadata(method).ok_or_else(|| {
        TabSwapError::Known(format!(
            "bundled Herdr metadata does not declare {method} during tab swap"
        ))
    })?;
    match client.unary(metadata, params).await {
        Ok(HerdrResponse::Success(result)) => Ok(result),
        Ok(HerdrResponse::Error { code, message }) => Err(TabSwapError::Known(format!(
            "Herdr rejected {method} during tab swap {phase} with {code}: {message}; {}",
            if state_changing {
                "an earlier ordered move may have changed tab ordering and Muxe will not replay"
            } else {
                "no tab move was accepted for this request"
            }
        ))),
        Err(error) if state_changing && error.delivery() == DeliveryState::MayHaveReachedHost => {
            Err(TabSwapError::Unknown(format!(
                "Herdr response was lost during tab swap {phase}; this tab.move may have applied and Muxe will not replay: {error}"
            )))
        }
        Err(error) => Err(TabSwapError::Known(format!(
            "Herdr transport failed during tab swap {phase}: {error}"
        ))),
    }
}

fn tab_list(result: &Value, phase: &str) -> Result<Vec<OrderedTab>, TabSwapError> {
    let object = result
        .as_object()
        .ok_or_else(|| TabSwapError::Known(format!("Herdr {phase} is not an object")))?;
    if object.get("type").and_then(Value::as_str) != Some("tab_list")
        && object.get("type").and_then(Value::as_str) != Some("tab_moved")
    {
        return Err(TabSwapError::Known(format!(
            "Herdr {phase} has no tab_list or tab_moved result type"
        )));
    }
    let tabs = object
        .get("tabs")
        .and_then(Value::as_array)
        .ok_or_else(|| TabSwapError::Known(format!("Herdr {phase} lacks a tabs array")))?;
    let mut ordered = Vec::with_capacity(tabs.len());
    for tab in tabs {
        let tab = tab.as_object().ok_or_else(|| {
            TabSwapError::Known(format!("Herdr {phase} contains a non-object tab"))
        })?;
        let id = required_tab_field(tab, "tab_id", phase)?;
        let workspace = required_tab_field(tab, "workspace_id", phase)?;
        let number = tab.get("number").and_then(Value::as_u64).ok_or_else(|| {
            TabSwapError::Known(format!(
                "Herdr {phase} tab {id:?} lacks a nonnegative number"
            ))
        })?;
        if ordered.iter().any(|existing: &OrderedTab| {
            existing.workspace == workspace && existing.number == number
        }) {
            return Err(TabSwapError::Known(format!(
                "Herdr {phase} repeats tab number {number} in workspace {workspace:?}"
            )));
        }
        ordered.push(OrderedTab {
            id: id.to_owned(),
            workspace: workspace.to_owned(),
            number,
        });
    }
    Ok(ordered)
}

fn required_tab_field<'a>(
    tab: &'a serde_json::Map<String, Value>,
    field: &str,
    phase: &str,
) -> Result<&'a str, TabSwapError> {
    tab.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| TabSwapError::Known(format!("Herdr {phase} tab lacks nonempty {field}")))
}

fn tab_at_id<'a>(
    tabs: &'a [OrderedTab],
    id: &str,
    workspace: &str,
    role: &str,
) -> Result<&'a OrderedTab, TabSwapError> {
    tabs.iter()
        .find(|tab| tab.id == id && tab.workspace == workspace)
        .ok_or_else(|| {
            TabSwapError::Known(format!(
                "Herdr tab list has no {role} {id:?} in captured workspace {workspace:?}"
            ))
        })
}

fn tab_at_number<'a>(
    tabs: &'a [OrderedTab],
    number: u64,
    workspace: &str,
    role: &str,
) -> Result<&'a OrderedTab, TabSwapError> {
    tabs.iter()
        .find(|tab| tab.number == number && tab.workspace == workspace)
        .ok_or_else(|| {
            TabSwapError::Known(format!(
                "Herdr tab list has no {role} {number} in captured workspace {workspace:?}"
            ))
        })
}

fn target_is_index(target: &muxe_core::IndexOrDirection) -> bool {
    matches!(
        target,
        muxe_core::IndexOrDirection::Index(index) if scalar_is_unsigned_or_context(index)
    )
}

fn target_is_cardinal_direction(target: &muxe_core::IndexOrDirection) -> bool {
    matches!(
        target,
        muxe_core::IndexOrDirection::Direction(direction)
            if scalar_is_cardinal_or_context(direction)
    )
}

fn split_direction_is_supported(direction: &ActionScalar) -> bool {
    match &direction.value.kind {
        ConfigValueKind::Context(_) => true,
        ConfigValueKind::String(value) => matches!(value.as_str(), "right" | "down"),
        _ => false,
    }
}

fn scalar_is_cardinal_or_context(scalar: &ActionScalar) -> bool {
    match &scalar.value.kind {
        ConfigValueKind::Context(_) => true,
        ConfigValueKind::String(value) => {
            matches!(value.as_str(), "left" | "right" | "up" | "down")
        }
        _ => false,
    }
}

fn scalar_is_unsigned_or_context(scalar: &ActionScalar) -> bool {
    match &scalar.value.kind {
        ConfigValueKind::Integer(value) => *value >= 0,
        ConfigValueKind::Context(_) => true,
        _ => false,
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn origin_pane(origin: &muxe_core::OriginContext) -> Result<&str, AdapterError> {
    origin
        .pane_id
        .as_ref()
        .map(muxe_core::PaneId::as_str)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "portable pane action requires the captured origin pane",
            )
        })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn origin_tab(origin: &muxe_core::OriginContext) -> Result<&str, AdapterError> {
    origin
        .tab_id
        .as_ref()
        .map(muxe_core::TabId::as_str)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "portable tab action requires the captured origin tab",
            )
        })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_string(value: &ActionScalar) -> Result<&str, AdapterError> {
    value.value.as_str().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "resolved portable scalar must be a string",
        )
    })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_index(value: &ActionScalar) -> Result<u64, AdapterError> {
    let muxe_core::ConfigValueKind::Integer(value) = value.value.kind else {
        return Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "resolved portable index must be nonnegative",
        ));
    };
    u64::try_from(value).map_err(|_| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "resolved portable index must be nonnegative",
        )
    })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_bool(value: &ActionScalar) -> Result<bool, AdapterError> {
    value.value.as_bool().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "resolved portable scalar must be boolean",
        )
    })
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
#[expect(
    clippy::cast_precision_loss,
    reason = "Herdr wire params are JSON numbers, which cannot represent i64 magnitudes above 2^53 exactly"
)]
fn scalar_number(value: &ActionScalar) -> Result<f64, AdapterError> {
    match value.value.kind {
        muxe_core::ConfigValueKind::Integer(value) => Ok(value as f64),
        muxe_core::ConfigValueKind::Float(value) => Ok(value),
        _ => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "resolved pane resize amount must be numeric",
        )),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_pane_direction(value: &ActionScalar) -> Result<&str, AdapterError> {
    match scalar_string(value)? {
        "left" | "right" | "up" | "down" => scalar_string(value),
        _ => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "Herdr pane direction must be left, right, up, or down",
        )),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_split_direction(value: &ActionScalar) -> Result<&str, AdapterError> {
    match scalar_string(value)? {
        "right" | "down" => scalar_string(value),
        _ => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "Herdr pane split direction must be right or down",
        )),
    }
}

fn native_diagnostic(candidate: &NativeActionCandidate, message: &str) -> ConfigDiagnostic {
    ConfigDiagnostic::error(
        DiagnosticCode::NativeActionRejected,
        message,
        candidate.type_span.clone(),
    )
}

fn socket_error(error: &SocketError) -> AdapterError {
    AdapterError::new(
        if error.delivery() == DeliveryState::MayHaveReachedHost {
            AdapterErrorKind::OutcomeUnknown
        } else {
            AdapterErrorKind::Unavailable
        },
        error.to_string(),
    )
}

fn incompatible(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Incompatible, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use muxe_core::{
        ConfigValue, ContextReference, OriginContext, OriginHostKind, OriginInvocationSource,
        ServerId, SourceId, SourceSpan,
    };

    fn origin() -> OriginContext {
        OriginContext {
            host_kind: OriginHostKind::Herdr,
            server_id: ServerId::new("server"),
            client_id: None,
            session_id: None,
            workspace_id: None,
            tab_id: Some(muxe_core::TabId::new("tab")),
            tab_index: None,
            pane_id: Some(muxe_core::PaneId::new("pane")),
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

    #[test]
    fn a_new_local_epoch_blocks_an_origin_with_the_same_observed_server() {
        let identity = HostIdentity {
            kind: muxe_adapter_api::HostKind::Herdr,
            discovery_key: "/owned/herdr.sock".to_owned(),
            live_server_id: "observed-server".to_owned(),
        };
        let mut captured = origin();
        captured.server_id = ServerId::new(origin_epoch_token(&identity, 1));

        assert!(!origin_is_current(&captured, &identity, 2));
        assert_eq!(identity.live_server_id, "observed-server");
    }

    #[test]
    fn unsupported_portable_forms_do_not_receive_invented_herdr_defaults() {
        for action in [
            PortableAction::Tab(TabAction::Rename { name: None }),
            PortableAction::Pane(PaneAction::Create),
            PortableAction::Pane(PaneAction::Split { direction: None }),
        ] {
            let Err(error) = portable_invocation(&action, &origin()) else {
                panic!("form without a schema-defined Herdr mapping must fail");
            };
            assert_eq!(error.kind, AdapterErrorKind::Incompatible);
        }
    }

    #[test]
    fn pane_zoom_preserves_its_direct_host_mode() {
        let invocation = portable_invocation(
            &PortableAction::Pane(PaneAction::Zoom {
                enabled: Some(ActionScalar::new(ConfigValue::synthetic(
                    ConfigValueKind::Boolean(true),
                ))),
            }),
            &origin(),
        )
        .expect("zoom has a direct Herdr schema mapping");

        assert_eq!(invocation.method, "pane.zoom");
        assert_eq!(
            invocation.params,
            json!({ "pane_id": "pane", "mode": "on" })
        );
    }

    #[test]
    fn tab_moved_results_preserve_the_numbered_workspace_order_needed_for_swap() {
        let tabs = tab_list(
            &json!({
                "type": "tab_moved",
                "tab_id": "target",
                "workspace_id": "workspace",
                "insert_index": 4,
                "tabs": [
                    { "tab_id": "target", "workspace_id": "workspace", "number": 4 },
                    { "tab_id": "source", "workspace_id": "workspace", "number": 7 },
                    { "tab_id": "other-workspace", "workspace_id": "other", "number": 4 }
                ]
            }),
            "tab.move response",
        )
        .expect("a tab.move response supplies the ordered tabs for the second move");

        assert_eq!(
            tab_at_number(&tabs, 4, "workspace", "requested tab index")
                .expect("the target tab must be selected in its own workspace")
                .id,
            "target"
        );
        assert_eq!(
            tab_at_id(&tabs, "source", "workspace", "captured origin tab")
                .expect("the captured tab must remain addressable for the second move")
                .number,
            7
        );
    }

    #[test]
    fn unresolved_typed_context_forms_remain_routable_until_origin_capture() {
        let source = SourceSpan::new(SourceId::new("<test>"), 0, 0);
        let index = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::Context(
            ContextReference::parse("origin.tab.index", source.clone())
                .expect("known unsigned context path"),
        )));
        let direction = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::Context(
            ContextReference::parse("origin.selection.text", source)
                .expect("known textual context path"),
        )));

        assert!(target_is_index(&muxe_core::IndexOrDirection::Index(index)));
        assert!(split_direction_is_supported(&direction));
    }
}
