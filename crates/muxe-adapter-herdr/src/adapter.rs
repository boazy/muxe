use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, RwLock,
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
    PendingPaneRegistration, PortableDispatchRequest, PostDismissalPortableDispatchRequest,
};
use muxe_core::{
    ActionScalar, ActionValidation, ActionValidator, ConfigDiagnostic, ConfigValueKind,
    ContextType, DiagnosticCode, ExecutionCapabilities, NativeActionCandidate, PaneAction,
    PortableAction, TabAction,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;

use crate::{
    ApiSchema, CandidateValidationError, ComparisonKey, DeliveryState, EndpointIdentity,
    EventSubscription, HerdrAdapterConfig, HerdrCache, HerdrResponse, HerdrRuntime,
    HerdrSocketClient, SocketError, SubscriptionConfig, SubscriptionEvent, fields_to_json,
    generated::{BUNDLED_REQUEST_SCHEMA_SHA256, method_metadata},
    validate_candidate,
};

const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_RETRY: Duration = Duration::from_secs(1);
/// Ten seconds covers a normal Herdr restart while ensuring a permanently lost host
/// reaches the broker's terminal lifecycle path instead of retrying forever.
const HOST_LOSS_GRACE: Duration = Duration::from_secs(10);

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
    // A host dispatch publishes its owned terminal value here before any bounded
    // public health-event delivery. One registry lock makes admission and
    // shutdown mutually exclusive, so every admitted host request remains
    // retained until either the health consumer or shutdown joins it.
    dispatch_results_tx: mpsc::UnboundedSender<DispatchTerminal>,
    dispatch_results_rx: Mutex<mpsc::UnboundedReceiver<DispatchTerminal>>,
    dispatch_wake: Notify,
    dispatch_tasks: StdMutex<DispatchTaskRegistry>,
    pending_leases: StdMutex<HashMap<PendingPaneLeaseId, PendingPaneLeaseRecord>>,
    post_dismissal: Mutex<HashMap<String, Vec<PostDismissalPortableDispatchRequest>>>,
    health_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
}

struct DispatchTaskRegistry {
    closed: bool,
    finalized: bool,
    joining: usize,
    tasks: HashMap<muxe_core::ExecutionId, JoinHandle<()>>,
}

struct DispatchTerminal {
    execution: muxe_core::ExecutionId,
    completion: DispatchCompletion,
}

/// Transport-free validator for inspecting configuration against the exact
/// installed Herdr request schema.
pub struct HerdrConfigValidator {
    schema: Arc<ApiSchema>,
}

impl HerdrConfigValidator {
    /// Loads the installed schema through the bounded owned-child path without
    /// probing or subscribing to a Herdr socket.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the schema command, cache, protocol check,
    /// or schema parser fails.
    pub async fn load(binary: &Path, cache_dir: &Path) -> Result<Self, AdapterError> {
        let (schema, _) = crate::runtime::load_installed_schema(binary, cache_dir).await?;
        Ok(Self { schema })
    }
}
#[doc(hidden)]
#[derive(Default)]
pub struct WaitHook {
    pub entered: Notify,
    pub release: Notify,
}

#[doc(hidden)]
impl WaitHook {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

struct ContinuityState {
    epoch: u64,
    healthy: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingPaneCloseState {
    Open,
    CloseMayHaveApplied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingPaneLeaseRecord {
    host_epoch: u64,
    endpoint: EndpointIdentity,
    ui_session: muxe_adapter_api::UiSessionId,
    pane: muxe_core::PaneId,
    temporary_tab: Option<muxe_core::TabId>,
    close_state: PendingPaneCloseState,
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
        let (dispatch_results_tx, dispatch_results_rx) = mpsc::unbounded_channel();
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
            health_wait_hook: StdMutex::new(None),
            pending_leases: StdMutex::new(HashMap::new()),
            suspended_ack: Notify::new(),
            resume_wake: Notify::new(),
            resume_slot: Mutex::new(None),
            monitor: Mutex::new(None),
            dispatch_results_tx,
            dispatch_results_rx: Mutex::new(dispatch_results_rx),
            dispatch_wake: Notify::new(),
            dispatch_tasks: StdMutex::new(DispatchTaskRegistry {
                closed: false,
                finalized: false,
                joining: 0,
                tasks: HashMap::new(),
            }),
            post_dismissal: Mutex::new(HashMap::new()),
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

    #[doc(hidden)]
    pub fn set_health_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .health_wait_hook
            .lock()
            .expect("Herdr health hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn send_dispatch_terminal_for_test(
        &self,
        execution: muxe_core::ExecutionId,
        completion: DispatchCompletion,
    ) {
        let _ = self.dispatch_results_tx.send(DispatchTerminal {
            execution,
            completion,
        });
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn admit_dispatch_task(
        &self,
        execution: muxe_core::ExecutionId,
        spawn: impl FnOnce() -> JoinHandle<()>,
    ) -> Result<(), AdapterError> {
        let mut registry = self
            .dispatch_tasks
            .lock()
            .expect("retained Herdr dispatch registry is not poisoned");
        if registry.closed {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Herdr adapter is shutting down and cannot admit another dispatch",
            ));
        }
        if registry.tasks.contains_key(&execution) {
            return Err(AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Herdr adapter already owns this dispatch execution",
            ));
        }
        // Keep the admission lock through spawn and insertion. Shutdown cannot
        // observe an unregistered task after its host future is allowed to run.
        registry.tasks.insert(execution, spawn());
        Ok(())
    }

    async fn finish_dispatch_terminal(&self, terminal: DispatchTerminal) -> AdapterHealthEvent {
        let task = self
            .dispatch_tasks
            .lock()
            .expect("retained Herdr dispatch registry is not poisoned")
            .tasks
            .remove(&terminal.execution);
        if let Some(task) = task {
            let _ = task.await;
        }
        AdapterHealthEvent::DispatchCompleted(terminal.completion)
    }

    fn dispatches_fully_drained(&self) -> bool {
        let registry = self
            .dispatch_tasks
            .lock()
            .expect("retained Herdr dispatch registry is not poisoned");
        registry.closed && registry.finalized && registry.tasks.is_empty() && registry.joining == 0
    }

    /// One explicit adapter lifecycle gate for every host-bound operation,
    /// mirroring Zellij's `require_active` shape: a shut-down adapter fails
    /// closed with `Shutdown` first, a suspended adapter is `Unavailable`
    /// while its retained stream is released, and only then does the retained
    /// continuity epoch authorize the call. `identity` stays on continuity
    /// alone (it returns the retained identity, never a new host request) so
    /// its error behavior is unchanged.
    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn require_lifecycle(&self) -> Result<u64, AdapterError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Herdr adapter is shut down; host-bound operations are closed",
            ));
        }
        if self.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Herdr adapter is suspended for activation; host-bound operations are blocked until resume or commit",
            ));
        }
        self.require_continuity()
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
        let epoch = self.require_lifecycle()?;
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
        let structural = candidates
            .iter()
            .copied()
            .map(crate::validation::structural_cache_key)
            .collect::<Vec<_>>();
        if structural.iter().any(Result::is_err) {
            let mut diagnostics = Vec::new();
            for (candidate, structural) in candidates.iter().zip(structural) {
                if let Err(error) = structural {
                    diagnostics.push(native_diagnostic(candidate, &error.to_string()));
                    continue;
                }
                if let Err(error) = validate_candidate(runtime.schema(), candidate) {
                    diagnostics.push(native_candidate_diagnostic(candidate, &error));
                }
            }
            return Err(diagnostics);
        }
        let configured_requests_hash = match crate::validated_requests_hash(candidates) {
            Ok(hash) => hash,
            Err(error) => {
                return Err(vec![native_diagnostic(candidates[0], &error.to_string())]);
            }
        };
        let key = ComparisonKey {
            bundled_schema_hash: BUNDLED_REQUEST_SCHEMA_SHA256.to_owned(),
            runtime_schema_hash: runtime.schema().canonical_request_sha256().to_owned(),
            configured_requests_hash,
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
        portable_compile_validation(runtime.schema(), action)
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
        // The closed registry already rejects admission after shutdown, but
        // the lifecycle gate owns the ordering (shutdown → suspended →
        // continuity) and covers the window before shutdown closes it.
        self.require_lifecycle()?;
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
        let results = self.dispatch_results_tx.clone();
        self.admit_dispatch_task(execution, move || {
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
                let _ = results.send(DispatchTerminal {
                    execution,
                    completion,
                });
            })
        })?;
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
        self.require_lifecycle()?;
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
        let results = self.dispatch_results_tx.clone();
        self.admit_dispatch_task(execution, move || {
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
                let _ = results.send(DispatchTerminal {
                    execution,
                    completion,
                });
            })
        })?;
        Ok(DispatchAccepted {
            correlation: ExecutionCorrelationId::new(format!(
                "herdr-{}",
                self.next_correlation.fetch_add(1, Ordering::Relaxed)
            )),
            execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        })
    }

    fn post_dismissal_accepted(&self, execution: muxe_core::ExecutionId) -> DispatchAccepted {
        DispatchAccepted {
            correlation: ExecutionCorrelationId::new(format!(
                "herdr-{}",
                self.next_correlation.fetch_add(1, Ordering::Relaxed)
            )),
            execution,
            capabilities: ExecutionCapabilities::ASYNCHRONOUS,
        }
    }

    async fn ui_pane_is_live(&self, pane: &muxe_core::PaneId) -> Result<bool, AdapterError> {
        let snapshot = self.invoke_unary("session.snapshot", json!({})).await?;
        snapshot_contains_pane(&snapshot, pane)
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn start_post_dismissal(
        &self,
        request: &PostDismissalPortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_current_origin(&request.origin)?;
        if creation_has_program(&request.action.action) {
            return self.dispatch_command_creation(
                request.execution,
                &request.action.action,
                &request.origin,
            );
        }
        let invocation = portable_invocation(&request.action.action, &request.origin)?;
        self.dispatch(request.execution, invocation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn dispatch_command_creation(
        &self,
        execution: muxe_core::ExecutionId,
        action: &PortableAction,
        origin: &muxe_core::OriginContext,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_lifecycle()?;
        let runtime = self.runtime();
        let results = self.dispatch_results_tx.clone();
        let action = action.clone();
        let origin = origin.clone();
        self.admit_dispatch_task(execution, move || {
            tokio::spawn(async move {
                let completion = match perform_command_creation(
                    runtime.client(),
                    runtime.schema(),
                    &action,
                    &origin,
                )
                .await
                {
                    Ok(()) => DispatchCompletion::Succeeded { execution },
                    Err(error) if error.kind == AdapterErrorKind::OutcomeUnknown => {
                        DispatchCompletion::OutcomeUnknown { execution, error }
                    }
                    Err(error) => DispatchCompletion::Failed { execution, error },
                };
                let _ = results.send(DispatchTerminal {
                    execution,
                    completion,
                });
            })
        })?;
        Ok(self.post_dismissal_accepted(execution))
    }
    async fn dispatch_when_ui_is_gone(
        &self,
        request: PostDismissalPortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_current_origin(&request.origin)?;
        let pane = request.ui_pane.as_str().to_owned();
        let execution = request.execution;
        self.post_dismissal
            .lock()
            .await
            .entry(pane.clone())
            .or_default()
            .push(request);
        let is_live = match self
            .ui_pane_is_live(&muxe_core::PaneId::new(pane.clone()))
            .await
        {
            Ok(is_live) => is_live,
            Err(error) => {
                let still_queued = snapshot_failure_reclaims_request(
                    &mut *self.post_dismissal.lock().await,
                    &pane,
                    execution,
                );
                if still_queued {
                    return Err(error);
                }
                return Ok(self.post_dismissal_accepted(execution));
            }
        };
        if is_live {
            return Ok(self.post_dismissal_accepted(execution));
        }
        let queued = self.post_dismissal.lock().await.remove(&pane);
        if let Some(queued) = queued {
            self.start_post_dismissals(queued).await;
        }
        Ok(self.post_dismissal_accepted(execution))
    }

    async fn observe_subscription_event(&self, event: Value) {
        if event.get("type").and_then(Value::as_str) != Some("pane_closed") {
            return;
        }
        let Some(pane) = event.get("pane_id").and_then(Value::as_str) else {
            return;
        };
        let queued = self.post_dismissal.lock().await.remove(pane);
        if let Some(queued) = queued {
            self.start_post_dismissals(queued).await;
        }
    }

    async fn start_post_dismissals(&self, queued: Vec<PostDismissalPortableDispatchRequest>) {
        for request in queued {
            let execution = request.execution;
            if let Err(error) = self.start_post_dismissal(&request) {
                let _ = self.dispatch_results_tx.send(DispatchTerminal {
                    execution,
                    completion: DispatchCompletion::Failed { execution, error },
                });
            }
        }
    }

    async fn fail_post_dismissals(&self, message: &str) {
        let queued = std::mem::take(&mut *self.post_dismissal.lock().await);
        for request in queued.into_values().flatten() {
            let _ = self.dispatch_results_tx.send(DispatchTerminal {
                execution: request.execution,
                completion: DispatchCompletion::Failed {
                    execution: request.execution,
                    error: AdapterError::new(AdapterErrorKind::Unavailable, message),
                },
            });
        }
    }

    async fn invoke_unary_response(
        &self,
        method: &str,
        params: Value,
    ) -> Result<HerdrResponse, AdapterError> {
        self.require_lifecycle()?;
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
        runtime
            .client()
            .unary(metadata, params)
            .await
            .map_err(|error| socket_error(&error))
    }

    async fn invoke_unary_response_on_endpoint(
        &self,
        method: &str,
        params: Value,
        endpoint: &EndpointIdentity,
    ) -> Result<HerdrResponse, AdapterError> {
        self.require_lifecycle()?;
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
        runtime
            .client()
            .unary_on_expected_endpoint(metadata, params, endpoint)
            .await
            .map_err(|error| socket_error(&error))
    }

    async fn invoke_unary(&self, method: &str, params: Value) -> Result<Value, AdapterError> {
        match self.invoke_unary_response(method, params).await? {
            HerdrResponse::Success(result) => Ok(result),
            HerdrResponse::Error { code, message } => Err(host_rejection(method, &code, &message)),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn pending_close_record(
        &self,
        registration: &PendingPaneRegistration,
        lease: &PendingPaneLease,
        epoch: u64,
    ) -> Result<PendingPaneLeaseRecord, AdapterError> {
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .get(&lease.id)
            .cloned()
            .filter(|record| {
                lease.ui_session == registration.ui_session
                    && record.host_epoch == epoch
                    && record.ui_session == lease.ui_session
                    && record.pane == registration.pane
                    && record.temporary_tab == registration.temporary_tab
            })
            .ok_or_else(pending_cleanup_lease_stale)
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn claim_pending_close(
        &self,
        lease: &PendingPaneLeaseId,
        record: &PendingPaneLeaseRecord,
    ) -> Result<(), AdapterError> {
        let mut leases = self
            .pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned");
        let Some(current) = leases.get_mut(lease) else {
            return Err(pending_cleanup_lease_stale());
        };
        if current == record {
            current.close_state = PendingPaneCloseState::CloseMayHaveApplied;
            Ok(())
        } else {
            Err(pending_cleanup_outcome_unknown())
        }
    }

    fn reopen_pending_close(&self, lease: &PendingPaneLeaseId, record: &PendingPaneLeaseRecord) {
        let mut leases = self
            .pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned");
        if let Some(current) = leases.get_mut(lease)
            && current
                == &(PendingPaneLeaseRecord {
                    close_state: PendingPaneCloseState::CloseMayHaveApplied,
                    ..record.clone()
                })
        {
            current.close_state = PendingPaneCloseState::Open;
        }
    }
}

fn portable_compile_validation(
    schema: &ApiSchema,
    action: &PortableAction,
) -> Result<ExecutionCapabilities, String> {
    if let Some(methods) = command_creation_methods(action) {
        return validate_required_methods(schema, methods);
    }
    if let Some(description) = portable_request_description(action)? {
        return validate_portable_request(schema, action, &description);
    }
    // Broker-owned forms (menu/config/command), command-bearing creations, and tab:swap
    // carry no single emitted request: preserve their existing capability contract.
    match action {
        PortableAction::Menu(_) | PortableAction::Config(_) => {
            Ok(ExecutionCapabilities::SYNCHRONOUS)
        }
        PortableAction::Command(_) => Ok(ExecutionCapabilities {
            awaitable: true,
            detachable: true,
            cancellable: true,
        }),
        PortableAction::Tab(TabAction::Create { .. })
        | PortableAction::Pane(PaneAction::Split { .. }) => validate_required_methods(
            schema,
            command_creation_methods(action).expect("command-bearing creation lists helper RPCs"),
        ),
        PortableAction::Tab(TabAction::Swap(_)) => {
            validate_required_methods(schema, &["tab.list", "tab.move"])
        }
        _ => unreachable!("portable_request_description covers every remaining portable form"),
    }
}

fn command_creation_methods(action: &PortableAction) -> Option<&'static [&'static str]> {
    match action {
        PortableAction::Tab(TabAction::Create { command, .. }) if command.program.is_some() => {
            Some(&["layout.apply"])
        }
        PortableAction::Pane(PaneAction::Split {
            direction: Some(direction),
            command,
            ..
        }) if command.program.is_some() && split_direction_is_supported(direction) => {
            Some(&["session.snapshot", "layout.apply", "pane.move", "tab.close"])
        }
        _ => None,
    }
}

/// Every JSON-RPC method a portable production path can require for one action:
/// the command-creation helper RPCs, the single emitted-request method, and the
/// composite-action methods (today `tab:swap` needs its `tab.list` + `tab.move`
/// bridge, which the emitted-request table cannot express alone). Broker-owned
/// forms (menu/config/command) require no host method. The verified-methods test
/// derives coverage from this, so a new production method fails the test instead
/// of silently under-declaring the exercised surface.
#[doc(hidden)]
#[must_use]
pub fn production_required_methods(action: &PortableAction) -> Vec<&'static str> {
    if let Some(methods) = command_creation_methods(action) {
        return methods.to_vec();
    }
    match portable_request_description(action) {
        Ok(Some(description)) => vec![description.method],
        Ok(None) => match action {
            PortableAction::Tab(TabAction::Swap(_)) => vec!["tab.list", "tab.move"],
            _ => Vec::new(),
        },
        Err(_) => Vec::new(),
    }
}

fn validate_required_methods(
    schema: &ApiSchema,
    methods: &[&str],
) -> Result<ExecutionCapabilities, String> {
    for method in methods {
        if method_metadata(method).is_none() || schema.method(method).is_none() {
            return Err(format!(
                "active Herdr schema does not declare required method {method}"
            ));
        }
    }
    Ok(ExecutionCapabilities::ASYNCHRONOUS)
}

/// Validates the described emitted request against the runtime schema: the method must exist
/// (as before), every emitted field must be declared by the schema, every required property
/// must be emitted, resolved literals must satisfy the declared type/domain, and unresolved
/// context values must be acceptable to the declared parameter type/domain. Probes derive
/// from the schema itself; no dummy concrete values are invented.
fn validate_portable_request(
    schema: &ApiSchema,
    action: &PortableAction,
    description: &PortableRequestDescription,
) -> Result<ExecutionCapabilities, String> {
    let method = description.method;
    if method_metadata(method).is_none() || schema.method(method).is_none() {
        return Err(format!(
            "active Herdr schema does not declare required method {method}"
        ));
    }
    validate_emitted_fields(schema, action, description)?;
    Ok(ExecutionCapabilities::ASYNCHRONOUS)
}

/// Checks the emitted field set and each field's value domain against the method's schema.
fn validate_emitted_fields(
    schema: &ApiSchema,
    action: &PortableAction,
    description: &PortableRequestDescription,
) -> Result<(), String> {
    let (params_schema, _) = schema
        .method_params_schema(description.method)
        .ok_or_else(|| {
            format!(
                "active Herdr schema does not declare required method {}",
                description.method
            )
        })?;
    let Some(params_object) = params_schema.as_object() else {
        return Err(format!(
            "active Herdr schema declares {} with a non-object params schema",
            description.method
        ));
    };
    let Some(properties) = params_object.get("properties").and_then(Value::as_object) else {
        // No declared properties (e.g. an empty-params method): the whole probe params must
        // still validate, so a method narrowed to no properties rejects its emitted fields.
        let params = build_probe_params(action, description)?;
        return schema
            .validate_method(description.method, &params)
            .map_err(|error| {
                format!(
                    "active Herdr schema rejects {}: {error}",
                    description.method
                )
            });
    };
    let required: Vec<&str> = match params_object.get("required") {
        None => Vec::new(),
        Some(required) => {
            let Some(required) = required.as_array() else {
                return Err(format!(
                    "active Herdr schema declares {} with a malformed required clause",
                    description.method
                ));
            };
            let mut names = Vec::with_capacity(required.len());
            for entry in required {
                let Some(name) = entry.as_str() else {
                    return Err(format!(
                        "active Herdr schema declares {} with a malformed required entry",
                        description.method
                    ));
                };
                names.push(name);
            }
            names
        }
    };
    // Every required property must be emitted: a newly required parameter rejects at reload
    // instead of failing at invocation after the user selected the binding.
    for name in &required {
        if !description.fields.iter().any(|field| field.name == *name) {
            return Err(format!(
                "active Herdr schema requires parameter {name:?} for {} which the portable action does not emit",
                description.method
            ));
        }
    }
    for field in description.fields {
        let Some(property_schema) = properties.get(field.name) else {
            return Err(format!(
                "Herdr {} declares no parameter {:?} emitted by the portable action",
                description.method, field.name
            ));
        };
        validate_emitted_field(schema, action, description.method, field, property_schema)?;
    }
    Ok(())
}

/// Checks one emitted field's value domain. Config literals face the same scalar conversion
/// the dispatch builder applies, then the schema's own domain check on the converted probe;
/// unresolved context markers face the declared-type check against the property's accepted
/// JSON types; origin strings probe as their typed representative. The verdict always
/// derives from the schema itself.
fn validate_emitted_field(
    schema: &ApiSchema,
    action: &PortableAction,
    method: &str,
    field: &PortableRequestField,
    property_schema: &Value,
) -> Result<(), String> {
    match field.origin {
        PortableValueOrigin::Origin(context_type) => {
            let probe = context_probe_value(context_type);
            probe_value_against_property(schema, method, field.name, &probe, property_schema)
        }
        PortableValueOrigin::Literal(kind) | PortableValueOrigin::Default(kind) => {
            validate_literal_field(schema, action, method, field, property_schema, kind)
        }
    }
}

/// Probes one value against one property schema by validating a single-property object.
/// Property-level probing keeps the diagnostic on the offending parameter while still
/// deriving the verdict from the schema itself.
fn probe_value_against_property(
    schema: &ApiSchema,
    method: &str,
    field_name: &str,
    probe: &Value,
    property_schema: &Value,
) -> Result<(), String> {
    let instance_path = format!("#/{field_name}");
    schema
        .validate_value(property_schema, probe, &instance_path, "#")
        .map_err(|error| {
            format!("active Herdr schema rejects the {method} parameter {field_name:?}: {error}")
        })
}

/// Validates one config-backed field: converts the configured scalar exactly as the dispatch
/// builder does, then probes the converted value against the property schema. An unresolved
/// context marker passes only when its declared type is among the property's accepted JSON
/// types; absent optionals probe as the builder's default.
fn validate_literal_field(
    schema: &ApiSchema,
    action: &PortableAction,
    method: &str,
    field: &PortableRequestField,
    property_schema: &Value,
    kind: PortableScalarKind,
) -> Result<(), String> {
    if kind == PortableScalarKind::Keys {
        let probe = converted_keys_probe(action).map_err(|message| {
            format!(
                "portable action supplies an invalid value for parameter {:?}: {message}",
                field.name
            )
        })?;
        return probe_value_against_property(schema, method, field.name, &probe, property_schema);
    }
    // The zoom `mode` field converts the `enabled` boolean (`None` emits the `toggle`
    // default); every other field converts its same-named config scalar.
    if field.name == "mode" {
        return validate_zoom_mode_field(schema, action, method, field.name, property_schema);
    }
    let scalar = literal_scalar_for(action, field.name);
    let Some(scalar) = scalar else {
        // The config leaves this optional absent, so dispatch emits the builder default.
        let probe = default_probe_value(field, kind);
        return probe_value_against_property(schema, method, field.name, &probe, property_schema);
    };
    if let ConfigValueKind::Context(reference) = &scalar.value.kind {
        return validate_context_marker(schema, method, field.name, reference, property_schema);
    }
    let probe = converted_literal_probe(scalar, field.name, kind)?;
    probe_value_against_property(schema, method, field.name, &probe, property_schema)
}

/// Validates the zoom `mode` field, which converts the `enabled` boolean exactly as the
/// dispatch builder does: absent emits `"toggle"`, `true` emits `"on"`, `false` emits
/// `"off"`. A context marker cannot convert to a boolean, so it rejects here (dispatch
/// would also reject it via `scalar_bool`); the converted literal then faces the schema.
fn validate_zoom_mode_field(
    schema: &ApiSchema,
    action: &PortableAction,
    method: &str,
    field_name: &str,
    property_schema: &Value,
) -> Result<(), String> {
    let PortableAction::Pane(PaneAction::Zoom { enabled }) = action else {
        return Err(format!(
            "portable action supplies an invalid value for parameter {field_name:?}: mode field does not reference a zoom action"
        ));
    };
    let probe = match enabled {
        None => Value::String("toggle".to_owned()),
        Some(scalar) => match &scalar.value.kind {
            ConfigValueKind::Context(_) => {
                return Err(format!(
                    "portable action supplies an invalid value for parameter {field_name:?}: enabled must be boolean"
                ));
            }
            _ => scalar_bool_value(scalar)
                .map(|enabled| Value::String(if enabled { "on" } else { "off" }.to_owned()))
                .map_err(|message| {
                    format!("portable action supplies an invalid value for parameter {field_name:?}: {message}")
                })?,
        },
    };
    probe_value_against_property(schema, method, field_name, &probe, property_schema)
}

/// Converts a `keys` list: every entry faces the same string conversion the dispatch
/// builder applies, and context entries probe as their declared type's representative.
/// A non-string entry (literal or wrongly typed marker) rejects at load time.
fn converted_keys_probe(action: &PortableAction) -> Result<Value, String> {
    let PortableAction::Keyboard(muxe_core::KeyboardAction::SendKeys(keys)) = action else {
        return Err("keys field does not reference the action key list".to_owned());
    };
    keys.iter()
        .map(|key| match &key.value.kind {
            ConfigValueKind::Context(reference) => {
                if reference.expected_type() == ContextType::String {
                    Ok(Value::String("muxe-context".to_owned()))
                } else {
                    Err(format!(
                        "key context type {:?} does not fit the declared string parameter",
                        reference.expected_type(),
                    ))
                }
            }
            _ => scalar_string_value(key).map(Value::String),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Value::Array)
}

/// Checks an unresolved context marker against the property's accepted JSON types, resolved
/// through `$ref` and `anyOf`/`oneOf` by the schema itself. The marker's declared type maps
/// to the JSON types its resolved values inhabit (unsigned integers are JSON integers and
/// therefore also JSON numbers; every id/path/text type is a JSON string).
fn validate_context_marker(
    schema: &ApiSchema,
    method: &str,
    field_name: &str,
    reference: &muxe_core::ContextReference,
    property_schema: &Value,
) -> Result<(), String> {
    let accepted = schema
        .property_json_types(property_schema)
        .map_err(|error| {
            format!(
                "active Herdr schema is malformed for {method} parameter {field_name:?}: {error}"
            )
        })?;
    let fitting: &[&str] = match reference.expected_type() {
        ContextType::UnsignedInteger => &["integer", "number"],
        _ => &["string"],
    };
    if fitting.iter().any(|fitting| accepted.contains(*fitting)) {
        return Ok(());
    }
    Err(format!(
        "active Herdr schema rejects the {method} parameter {field_name:?}: context type {:?} does not fit the declared parameter type",
        reference.expected_type(),
    ))
}

/// Converts one configured literal exactly as the dispatch builder converts it, returning
/// the JSON probe dispatch will emit for this field.
fn converted_literal_probe(
    scalar: &ActionScalar,
    field_name: &str,
    kind: PortableScalarKind,
) -> Result<Value, String> {
    match kind {
        PortableScalarKind::String => scalar_string_value(scalar).map(Value::String),
        PortableScalarKind::Bool => scalar_bool_value(scalar).map(Value::Bool),
        PortableScalarKind::Index => scalar_index_value(scalar).map(|index| Value::Number(index.into())),
        PortableScalarKind::Number => scalar_number_value(scalar).and_then(number_to_json),
        PortableScalarKind::SplitDirection => scalar_split_direction_value(scalar).map(Value::String),
        PortableScalarKind::PaneDirection => scalar_pane_direction_value(scalar).map(Value::String),
        PortableScalarKind::Keys => {
            Err(format!("portable action supplies an invalid value for parameter {field_name:?}: keys convert as a list, not a scalar"))
        }
    }
    .map_err(|message| {
        format!("portable action supplies an invalid value for parameter {field_name:?}: {message}")
    })
}

/// Typed probe for a context-backed value: the canonical inhabitant of the declared
/// context type. A `String` probe is the only honest universal string; numeric probes use
/// zero; `PaneId`/`TabId` use representative ids. The schema decides acceptance.
fn context_probe_value(context_type: ContextType) -> Value {
    match context_type {
        ContextType::UnsignedInteger => Value::Number(0.into()),
        ContextType::PaneId => Value::String("pane".to_owned()),
        ContextType::TabId => Value::String("tab".to_owned()),
        ContextType::WorkspaceId => Value::String("workspace".to_owned()),
        _ => Value::String("muxe-context".to_owned()),
    }
}

/// Builds the probe params for the whole-request fallback path: every described field gets
/// its representative probe value (literals become their converted value; origin fields
/// become their typed probe).
fn build_probe_params(
    action: &PortableAction,
    description: &PortableRequestDescription,
) -> Result<Value, String> {
    let mut params = serde_json::Map::new();
    for field in description.fields {
        params.insert(field.name.to_owned(), probe_field_value(action, field)?);
    }
    Ok(Value::Object(params))
}

/// Locates the configured scalar behind one described field, or `None` when the config
/// leaves the optional absent (dispatch then emits the builder default). Field names are
/// the description's emitted names (`label` for the config `name`, `mode` for `enabled`,
/// `insert_index` for the move index).
fn literal_scalar_for<'a>(
    action: &'a PortableAction,
    field_name: &str,
) -> Option<&'a ActionScalar> {
    match (action, field_name) {
        (PortableAction::Tab(TabAction::Create { workspace_id, .. }), "workspace_id") => {
            workspace_id.as_ref()
        }
        (
            PortableAction::Tab(TabAction::Create { name, .. } | TabAction::Rename { name }),
            "label",
        ) => name.as_ref(),
        (
            PortableAction::Tab(TabAction::Create { focus, .. })
            | PortableAction::Pane(PaneAction::Split { focus, .. }),
            "focus",
        ) => focus.as_ref(),
        (
            PortableAction::Tab(TabAction::Create { command, .. })
            | PortableAction::Pane(PaneAction::Split { command, .. }),
            "cwd",
        ) => command.cwd.as_ref(),
        (
            PortableAction::Tab(TabAction::Move(muxe_core::IndexOrDirection::Index(index))),
            "insert_index",
        ) => Some(index),
        (PortableAction::Pane(PaneAction::Split { direction, .. }), "direction") => {
            direction.as_ref()
        }
        (
            PortableAction::Pane(
                PaneAction::Focus(muxe_core::IndexOrDirection::Direction(direction))
                | PaneAction::Swap(muxe_core::IndexOrDirection::Direction(direction)),
            ),
            "direction",
        ) => Some(direction),
        (PortableAction::Pane(PaneAction::Resize { direction, .. }), "direction") => {
            Some(direction)
        }
        (PortableAction::Pane(PaneAction::Resize { amount, .. }), "amount") => amount.as_ref(),
        (PortableAction::Pane(PaneAction::Zoom { enabled }), "mode") => enabled.as_ref(),
        (PortableAction::Keyboard(muxe_core::KeyboardAction::SendText(text)), "text") => Some(text),
        _ => None,
    }
}

/// Builder-default probe for an absent optional, derived from the description's `Default`
/// variant so the probe is exactly what dispatch emits: `focus` (Default Bool) defaults to
/// `true`, zoom's `mode` (Default String) defaults to `"toggle"`, and every other absent
/// optional emits `null` (nullable strings/numbers) or `[]` (keys).
fn default_probe_value(field: &PortableRequestField, kind: PortableScalarKind) -> Value {
    match field.origin {
        PortableValueOrigin::Default(PortableScalarKind::Bool) => Value::Bool(true),
        PortableValueOrigin::Default(PortableScalarKind::String) if field.name == "mode" => {
            Value::String("toggle".to_owned())
        }
        _ => match kind {
            PortableScalarKind::Bool => Value::Bool(true),
            PortableScalarKind::String
            | PortableScalarKind::Index
            | PortableScalarKind::Number
            | PortableScalarKind::SplitDirection
            | PortableScalarKind::PaneDirection => Value::Null,
            PortableScalarKind::Keys => Value::Array(Vec::new()),
        },
    }
}

/// Representative probe for one described field, used only by the whole-request fallback
/// path for methods whose params schema declares no `properties`.
fn probe_field_value(
    action: &PortableAction,
    field: &PortableRequestField,
) -> Result<Value, String> {
    match field.origin {
        PortableValueOrigin::Origin(context_type) => Ok(context_probe_value(context_type)),
        PortableValueOrigin::Literal(kind) | PortableValueOrigin::Default(kind) => {
            literal_probe_value(action, field.name, kind)
        }
    }
}

/// Probe for one config-backed field on the fallback path: the converted literal, the
/// context marker's typed probe, or the builder default when absent.
fn literal_probe_value(
    action: &PortableAction,
    field_name: &str,
    kind: PortableScalarKind,
) -> Result<Value, String> {
    if kind == PortableScalarKind::Keys {
        return converted_keys_probe(action);
    }
    let scalar = literal_scalar_for(action, field_name);
    let Some(scalar) = scalar else {
        // The fallback path has no described field to derive the default from. Absent
        // `focus` emits `true` and absent zoom `mode` emits `"toggle"`; every other absent
        // optional emits `null` (nullable strings/numbers) or `[]` (keys).
        if field_name == "focus" {
            return Ok(Value::Bool(true));
        }
        if field_name == "mode" {
            return Ok(Value::String("toggle".to_owned()));
        }
        let fallback = PortableRequestField {
            name: "",
            origin: PortableValueOrigin::Default(kind),
        };
        return Ok(default_probe_value(&fallback, kind));
    };
    if let ConfigValueKind::Context(reference) = &scalar.value.kind {
        return Ok(context_probe_value(reference.expected_type()));
    }
    if field_name == "mode" {
        return scalar_bool_value(scalar)
            .map(|enabled| Value::String(if enabled { "on" } else { "off" }.to_owned()))
            .map_err(|message| {
                format!("portable action supplies an invalid value for parameter {field_name:?}: {message}")
            });
    }
    converted_literal_probe(scalar, field_name, kind)
}

impl ActionValidator for HerdrConfigValidator {
    fn validate_portable(
        &self,
        action: &PortableAction,
        action_span: &muxe_core::SourceSpan,
    ) -> Result<ActionValidation, ConfigDiagnostic> {
        portable_compile_validation(&self.schema, action)
            .map(|execution| ActionValidation { execution })
            .map_err(|message| {
                ConfigDiagnostic::error(DiagnosticCode::InvalidAction, message, action_span.clone())
            })
    }

    fn validate_native_batch(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        let mut validations = Vec::with_capacity(candidates.len());
        let mut diagnostics = Vec::new();
        for candidate in candidates {
            match validate_candidate(&self.schema, candidate) {
                Ok(_) => validations.push(ActionValidation {
                    execution: ExecutionCapabilities::ASYNCHRONOUS,
                }),
                Err(error) => {
                    diagnostics.push(native_candidate_diagnostic(candidate, &error));
                }
            }
        }
        if diagnostics.is_empty() {
            Ok(validations)
        } else {
            Err(diagnostics)
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
        let epoch = self.require_lifecycle()?;
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
        let id = PendingPaneLeaseId::new(format!(
            "herdr:{epoch}:{}:{}",
            registration.ui_session, registration.pane
        ));
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .insert(
                id.clone(),
                PendingPaneLeaseRecord {
                    host_epoch: epoch,
                    endpoint: self.runtime().endpoint().clone(),
                    ui_session: registration.ui_session.clone(),
                    pane: registration.pane.clone(),
                    temporary_tab: registration.temporary_tab.clone(),
                    close_state: PendingPaneCloseState::Open,
                },
            );
        Ok(PendingPaneLease {
            id,
            ui_session: registration.ui_session,
        })
    }

    async fn close_pending_pane(
        &self,
        registration: PendingPaneRegistration,
        lease: PendingPaneLease,
    ) -> Result<(), AdapterError> {
        let epoch = self.require_lifecycle()?;
        let record = self.pending_close_record(&registration, &lease, epoch)?;
        let pane = match self
            .invoke_unary_response_on_endpoint(
                "pane.get",
                json!({ "pane_id": record.pane.as_str() }),
                &record.endpoint,
            )
            .await?
        {
            HerdrResponse::Success(pane) => pane,
            HerdrResponse::Error { code, .. } if pane_is_proven_absent(&code) => {
                self.pending_leases
                    .lock()
                    .expect("Herdr pending lease registry is not poisoned")
                    .remove(&lease.id);
                return Ok(());
            }
            HerdrResponse::Error { code, message } => {
                return Err(host_rejection("pane.get", &code, &message));
            }
        };
        if record.close_state == PendingPaneCloseState::CloseMayHaveApplied {
            return Err(pending_cleanup_outcome_unknown());
        }
        let object = crate::pane_info(&pane).ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr pane.get returned an invalid pane_info response",
            )
        })?;
        if object.get("pane_id").and_then(Value::as_str) != Some(record.pane.as_str()) {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "pending Herdr pane identity changed",
            ));
        }
        if self.require_lifecycle()? != record.host_epoch {
            return Err(pending_cleanup_lease_stale());
        }
        // Re-check immediately before the state-changing send: a shutdown
        // that raced the pane.get probe must not let a cloned lease's
        // endpoint request reach the host after shutdown returned.
        self.require_lifecycle()?;
        let runtime = self.runtime();
        let close_params = json!({ "pane_id": record.pane.as_str() });
        runtime
            .schema()
            .validate_method("pane.close", &close_params)
            .map_err(|error| {
                incompatible(format!("active Herdr schema rejects pane.close: {error}"))
            })?;
        let metadata = method_metadata("pane.close")
            .ok_or_else(|| incompatible("bundled Herdr metadata does not declare pane.close"))?;
        self.claim_pending_close(&lease.id, &record)?;
        match runtime
            .client()
            .unary_on_expected_endpoint(metadata, close_params, &record.endpoint)
            .await
        {
            Ok(HerdrResponse::Success(_)) => {
                self.pending_leases
                    .lock()
                    .expect("Herdr pending lease registry is not poisoned")
                    .remove(&lease.id);
                Ok(())
            }
            Ok(HerdrResponse::Error { code, .. }) if pane_is_proven_absent(&code) => {
                self.pending_leases
                    .lock()
                    .expect("Herdr pending lease registry is not poisoned")
                    .remove(&lease.id);
                Ok(())
            }
            Ok(HerdrResponse::Error { code, message }) => {
                self.reopen_pending_close(&lease.id, &record);
                Err(host_rejection("pane.close", &code, &message))
            }
            Err(error) => {
                if error.delivery() == DeliveryState::NotSent {
                    self.reopen_pending_close(&lease.id, &record);
                }
                Err(socket_error(&error))
            }
        }
    }

    fn release_pending_pane(&self, lease: PendingPaneLease) {
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .remove(&lease.id);
    }

    async fn capture_origin(
        &self,
        request: OriginCaptureRequest,
    ) -> Result<muxe_core::OriginContext, AdapterError> {
        let epoch = self.require_lifecycle()?;
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
        if creation_requires_post_dismissal(&request.action.action)? {
            return Err(AdapterError::new(
                AdapterErrorKind::InvalidRequest,
                "focused creation must use post-dismissal dispatch after the Muxe UI closes",
            ));
        }
        if creation_has_program(&request.action.action) {
            return self.dispatch_command_creation(
                request.execution,
                &request.action.action,
                &request.origin,
            );
        }
        let invocation = portable_invocation(&request.action.action, &request.origin)?;
        self.dispatch(request.execution, invocation)
    }

    async fn dispatch_portable_after_ui_dismissal(
        &self,
        request: PostDismissalPortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.dispatch_when_ui_is_gone(request).await
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
        loop {
            // This is the sole health consumer (the broker monitor). Register
            // the dispatch wake before observing the queue or finalized status
            // so a notify_waiters during that observation cannot be lost.
            let wake = self.dispatch_wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            {
                let hook = self
                    .health_wait_hook
                    .lock()
                    .expect("Herdr health hook is not poisoned")
                    .clone();
                if let Some(hook) = hook {
                    hook.entered.notify_one();
                    hook.release.notified().await;
                }
            }
            let mut terminals = self.dispatch_results_rx.lock().await;
            if let Ok(terminal) = terminals.try_recv() {
                drop(terminals);
                return Ok(self.finish_dispatch_terminal(terminal).await);
            }
            if self.dispatches_fully_drained() {
                return Err(AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "Herdr adapter has drained every admitted dispatch",
                ));
            }
            let mut events = self.events_rx.lock().await;
            tokio::select! {
                biased;
                terminal = terminals.recv() => {
                    let terminal = terminal.ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::Shutdown,
                            "Herdr adapter dispatch-result channel closed",
                        )
                    })?;
                    drop(events);
                    drop(terminals);
                    return Ok(self.finish_dispatch_terminal(terminal).await);
                }
                event = events.recv() => {
                    return event.ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::Shutdown,
                            "Herdr adapter event channel closed",
                        )
                    });
                }
                () = &mut wake => {
                    drop(events);
                    drop(terminals);
                }
            }
        }
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
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .clear();

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
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "Herdr adapter is shut down; refusing to install a resumed subscription",
            ));
        }
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
            // never continuity proof, so stale origins stay rejected. A shutdown
            // that raced the resume must win: re-check before installing a new
            // epoch so no fresh subscription revives a shut-down adapter.
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "Herdr adapter shut down during activation resume; refusing to install a resumed subscription",
                ));
            }
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
        let tasks = {
            let mut registry = self
                .dispatch_tasks
                .lock()
                .expect("retained Herdr dispatch registry is not poisoned");
            registry.closed = true;
            registry.joining = registry.joining.saturating_add(registry.tasks.len());
            std::mem::take(&mut registry.tasks)
        };
        self.shutdown.store(true, Ordering::Relaxed);
        // Invalidate continuity before stopping the stream: no later path may
        // see a healthy epoch, and a racing resume/reconnect cannot install a
        // fresh subscription after this point. The parked monitor also drops
        // its resume slot below so a handed-over subscription cannot revive it.
        {
            let mut continuity = self
                .continuity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continuity.healthy = false;
        }
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .clear();
        self.fail_post_dismissals("Herdr adapter shut down before UI dismissal completed")
            .await;
        // Every monitor send is interruptible by the shutdown wake below, so
        // joining it cannot depend on a consumer freeing bounded health space.
        // Clearing the resume slot first means a suspend-parked monitor can
        // only observe shutdown, never a stale handed-over subscription.
        *self.resume_slot.lock().await = None;
        self.suspend_wake.notify_one();
        self.resume_wake.notify_one();
        if let Some(monitor) = self.monitor.lock().await.take() {
            let _ = monitor.await;
        }
        for (_, task) in tasks {
            let _ = task.await;
            let mut registry = self
                .dispatch_tasks
                .lock()
                .expect("retained Herdr dispatch registry is not poisoned");
            registry.joining = registry.joining.saturating_sub(1);
        }
        self.dispatch_tasks
            .lock()
            .expect("retained Herdr dispatch registry is not poisoned")
            .finalized = true;
        self.dispatch_wake.notify_waiters();
        Ok(())
    }
}

async fn send_health_or_shutdown(adapter: &Arc<HerdrAdapter>, event: AdapterHealthEvent) -> bool {
    tokio::select! {
        sent = adapter.events_tx.send(event) => sent.is_ok(),
        () = adapter.suspend_wake.notified() => !adapter.shutdown.load(Ordering::Relaxed),
    }
}
fn subscription_config() -> SubscriptionConfig {
    SubscriptionConfig {
        params: json!({
            "subscriptions": [
                { "type": "tab.focused" },
                { "type": "pane.closed" },
            ],
        }),
        subscribe_timeout: SUBSCRIBE_TIMEOUT,
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
            result = subscription.next_event() => match result {
                Ok(SubscriptionEvent::Event(event)) => {
                    adapter.observe_subscription_event(event).await;
                    false
                }
                Err(_) => true,
            },
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
        adapter
            .pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .clear();
        adapter
            .fail_post_dismissals("Herdr retained event-subscription continuity was lost")
            .await;
        if !send_health_or_shutdown(
            &adapter,
            AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Herdr retained event-subscription continuity was lost",
                ),
            },
        )
        .await
        {
            return;
        }

        match reconnect_subscription(&adapter, &mut subscription).await {
            ReconnectOutcome::Reconnected | ReconnectOutcome::SuspendRequested => {}
            ReconnectOutcome::Stop => return,
            ReconnectOutcome::HostLost(error) => {
                let _ = adapter
                    .events_tx
                    .send(AdapterHealthEvent::HostLost {
                        identity: adapter.identity(),
                        error,
                    })
                    .await;
                return;
            }
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
    /// The host did not recover within the bounded grace.
    HostLost(AdapterError),
}

async fn reconnect_subscription(
    adapter: &Arc<HerdrAdapter>,
    subscription: &mut EventSubscription,
) -> ReconnectOutcome {
    let deadline = tokio::time::Instant::now() + HOST_LOSS_GRACE;
    let mut last_error = None;

    loop {
        if adapter.shutdown.load(Ordering::Relaxed) {
            return ReconnectOutcome::Stop;
        }
        if adapter.suspended.load(Ordering::SeqCst) {
            return ReconnectOutcome::SuspendRequested;
        }
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        let attempt = tokio::select! {
            attempt = tokio::time::timeout(remaining, reconnect_attempt(adapter)) => Some(attempt),
            () = adapter.suspend_wake.notified() => None,
        };
        // A suspend wake re-checks the flag at the loop head so the old stream
        // is dropped and acked promptly. Dropping the attempt future kills only
        // the owned schema child via `kill_on_drop`; no global cleanup runs.
        let Some(attempt) = attempt else {
            continue;
        };
        let Ok(attempt) = attempt else {
            last_error = Some(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Herdr reconnect attempt exceeded the host-loss grace",
            ));
            break;
        };
        let Ok((refreshed, next_subscription)) = attempt else {
            last_error = attempt.err();
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                break;
            }
            if !interruptible_sleep(adapter, RECONNECT_RETRY.min(remaining)).await {
                return ReconnectOutcome::Stop;
            }
            continue;
        };
        let refreshed = Arc::new(refreshed);
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
            // A shutdown that raced the reconnect must win: re-check before
            // marking continuity healthy so no fresh subscription revives a
            // shut-down adapter.
            if adapter.shutdown.load(Ordering::Relaxed) {
                return ReconnectOutcome::Stop;
            }
            let mut continuity = adapter
                .continuity
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            continuity.healthy = true;
        }
        if !send_health_or_shutdown(
            adapter,
            AdapterHealthEvent::Reconnected { previous, current },
        )
        .await
        {
            return ReconnectOutcome::Stop;
        }
        *subscription = next_subscription;
        return ReconnectOutcome::Reconnected;
    }

    ReconnectOutcome::HostLost(last_error.unwrap_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr host did not recover within the bounded reconnect grace",
        )
    }))
}

async fn reconnect_attempt(
    adapter: &Arc<HerdrAdapter>,
) -> Result<(HerdrRuntime, EventSubscription), AdapterError> {
    let refreshed = HerdrRuntime::connect(adapter.config.clone()).await?;
    let (next_subscription, _) =
        EventSubscription::connect(refreshed.client(), subscription_config())
            .await
            .map_err(|error| socket_error(&error))?;
    Ok((refreshed, next_subscription))
}

/// Sleeps between reconnect attempts. Returns false only for terminal shutdown;
/// a suspend wake returns true so the caller re-checks the suspend flag promptly.
async fn interruptible_sleep(adapter: &Arc<HerdrAdapter>, delay: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(delay) => true,
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

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn snapshot_contains_pane(
    response: &Value,
    pane: &muxe_core::PaneId,
) -> Result<bool, AdapterError> {
    let panes = response
        .get("snapshot")
        .and_then(|snapshot| snapshot.get("panes"))
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "Herdr session.snapshot response lacks a panes array; cannot prove Muxe UI dismissal",
            )
        })?;
    Ok(panes
        .iter()
        .any(|candidate| candidate.get("pane_id").and_then(Value::as_str) == Some(pane.as_str())))
}

/// Removes exactly one waiting creation on a failed snapshot. A false result
/// means a pane-close or terminal lifecycle path already claimed its request,
/// so the snapshot error cannot retract work that may have begun.
fn snapshot_failure_reclaims_request(
    pending: &mut HashMap<String, Vec<PostDismissalPortableDispatchRequest>>,
    pane: &str,
    execution: muxe_core::ExecutionId,
) -> bool {
    let Some(requests) = pending.get_mut(pane) else {
        return false;
    };
    let Some(index) = requests
        .iter()
        .position(|request| request.execution == execution)
    else {
        return false;
    };
    requests.remove(index);
    if requests.is_empty() {
        pending.remove(pane);
    }
    true
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
    let description = portable_request_description(action)
        .map_err(incompatible)?
        .ok_or_else(|| {
            // Broker-owned forms (menu/config/command), command-bearing creations, and tab:swap
            // never reach the single-request builder: dispatch routes them to their own paths.
            // The description already reported every unsupported form as `Err`, so reaching
            // here with `None` means dispatch misrouted a multi-request action.
            incompatible("portable action form is unavailable in Herdr protocol 20")
        })?;
    let invocation = build_portable_invocation(&description, action, origin)?;
    debug_assert_eq!(invocation.method, description.method);
    debug_assert_eq!(
        invocation.params.as_object().map(|params| {
            let mut names: Vec<&str> = params.keys().map(String::as_str).collect();
            names.sort_unstable();
            names
        }),
        Some({
            let mut names: Vec<&str> = description.fields.iter().map(|field| field.name).collect();
            names.sort_unstable();
            names
        }),
        "the dispatch builder must emit exactly the described fields",
    );
    Ok(invocation)
}

/// Builds the dispatch-time invocation from the shared request description plus the
/// concrete config scalars. The field list (method and parameter names) comes only from
/// the description; this function only converts each field's runtime value.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn build_portable_invocation(
    description: &PortableRequestDescription,
    action: &PortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<Invocation, AdapterError> {
    // The direction diagnostic outranks the command-lifecycle diagnostic: a split without a
    // direction reports the missing direction even when it also carries a program.
    if let PortableAction::Pane(PaneAction::Split {
        direction: None, ..
    }) = action
    {
        return Err(incompatible(
            "Herdr pane.split requires an explicit right or down direction",
        ));
    }
    if creation_has_program(action) {
        // Command-bearing creations never reach the single-request builder: dispatch routes
        // them through the ordered dismiss-and-dispatch lifecycle with its helper RPCs.
        let message = match action {
            PortableAction::Tab(TabAction::Create { .. }) => {
                "Herdr command-bearing tab:create requires the ordered dismiss-and-dispatch lifecycle"
            }
            _ => {
                "Herdr command-bearing pane:split requires the ordered dismiss-and-dispatch lifecycle"
            }
        };
        return Err(incompatible(message));
    }
    single_request_invocation(description, action, origin)
}

/// Builds one single-request invocation from the shared description. Callers guarantee the
/// action carries no command program and, for splits, a direction; every remaining arm
/// converts exactly the described fields.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn single_request_invocation(
    description: &PortableRequestDescription,
    action: &PortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<Invocation, AdapterError> {
    let pane = origin_pane(origin)?;
    match action {
        PortableAction::Keyboard(muxe_core::KeyboardAction::SendKeys(keys)) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "keys": keys.iter().map(scalar_string).collect::<Result<Vec<_>, _>>()? }),
        }),
        PortableAction::Keyboard(muxe_core::KeyboardAction::SendText(text)) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "text": scalar_string(text)? }),
        }),
        PortableAction::Tab(TabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        }) => Ok(Invocation {
            method: description.method,
            params: json!({
                "workspace_id": workspace_id.as_ref().map(scalar_string).transpose()?,
                "label": name.as_ref().map(scalar_string).transpose()?,
                "focus": focus.as_ref().map(scalar_bool).transpose()?.unwrap_or(true),
                "cwd": command.cwd.as_ref().map(scalar_string).transpose()?,
            }),
        }),
        PortableAction::Tab(TabAction::Close) => Ok(Invocation {
            method: description.method,
            params: json!({ "tab_id": origin_tab(origin)? }),
        }),
        PortableAction::Tab(TabAction::Rename { name: Some(label) }) => Ok(Invocation {
            method: description.method,
            params: json!({ "tab_id": origin_tab(origin)?, "label": scalar_string(label)? }),
        }),
        PortableAction::Tab(TabAction::Move(muxe_core::IndexOrDirection::Index(index))) => {
            Ok(Invocation {
                method: description.method,
                params: json!({ "tab_id": origin_tab(origin)?, "insert_index": scalar_index(index)? }),
            })
        }
        PortableAction::Pane(PaneAction::Create) => Err(incompatible(
            "Herdr has no pane.create method; pane.split requires an explicit direction",
        )),
        PortableAction::Pane(PaneAction::Split {
            direction: Some(direction),
            focus,
            command,
        }) => Ok(Invocation {
            method: description.method,
            params: json!({
                "target_pane_id": pane,
                "direction": scalar_split_direction(direction)?,
                "focus": focus.as_ref().map(scalar_bool).transpose()?.unwrap_or(true),
                "cwd": command.cwd.as_ref().map(scalar_string).transpose()?,
            }),
        }),
        PortableAction::Pane(PaneAction::Close) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane }),
        }),
        PortableAction::Pane(
            PaneAction::Focus(muxe_core::IndexOrDirection::Direction(direction))
            | PaneAction::Swap(muxe_core::IndexOrDirection::Direction(direction)),
        ) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "direction": scalar_pane_direction(direction)? }),
        }),
        PortableAction::Pane(PaneAction::Resize { direction, amount }) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "direction": scalar_pane_direction(direction)?, "amount": amount.as_ref().map(scalar_number).transpose()? }),
        }),
        PortableAction::Pane(PaneAction::Zoom { enabled }) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "mode": enabled.as_ref().map(scalar_bool).transpose()?.map_or("toggle", |value| if value { "on" } else { "off" }) }),
        }),
        _ => Err(AdapterError::new(
            AdapterErrorKind::Incompatible,
            "portable action form is unavailable in Herdr protocol 20",
        )),
    }
}
/// One portable action's emitted Herdr request, shared by load-time validation and dispatch.
///
/// This is the single source of truth for "which method and which parameter fields this
/// portable action emits, and where each field's value comes from". Both the load-time
/// validator and the dispatch-time builder derive from it so the two phases can never
/// validate different contracts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PortableRequestDescription {
    method: &'static str,
    /// Fields the action always emits. Origin-derived values (pane/tab ids) are marked
    /// `Origin` so load-time checks know they resolve to concrete host strings at dispatch;
    /// unresolved context references stay marked `Unresolved` with their declared type.
    fields: &'static [PortableRequestField],
}

/// One emitted parameter field and the typed origin of its value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PortableRequestField {
    name: &'static str,
    origin: PortableValueOrigin,
}

/// Where one emitted field's value comes from. Concrete scalars resolve at dispatch to the
/// adapter's checked scalar conversion; `Origin` fields resolve to captured host identity
/// strings; `Unresolved` fields are context references whose declared type must be accepted
/// by the schema's parameter type/domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PortableValueOrigin {
    /// A config scalar converted by the named adapter scalar check. A literal faces full
    /// schema probing; an unresolved context marker faces the declared-type check, since its
    /// concrete value only exists after broker origin resolution.
    Literal(PortableScalarKind),
    /// A captured origin string (pane id, tab id, or workspace id).
    Origin(ContextType),
    /// A builder default the dispatch path always supplies (e.g. `focus` defaults to `true`,
    /// zoom's `toggle` mode). Literals that violate the declared parameter type still reject.
    Default(PortableScalarKind),
}

/// The adapter scalar conversion one literal field passes through at dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PortableScalarKind {
    String,
    Bool,
    Index,
    Number,
    SplitDirection,
    PaneDirection,
    Keys,
}
/// Derives the single emitted-request description for one portable action, or reports the
/// same unsupported-form diagnostic the dispatch builder reports. Optional config scalars
/// that stay absent still emit their builder default, so they appear as `Default` fields;
/// `None` here means the action has no Herdr mapping at all. Every `PortableAction` variant
/// is covered below, so the match needs no wildcard arm.
#[expect(
    clippy::too_many_lines,
    reason = "The explicit action-to-request table is the validation and dispatch contract."
)]
fn portable_request_description(
    action: &PortableAction,
) -> Result<Option<PortableRequestDescription>, String> {
    use PortableScalarKind as Scalar;
    use PortableValueOrigin as Origin;
    let description = match action {
        PortableAction::Menu(_) | PortableAction::Config(_) | PortableAction::Command(_) => return Ok(None),
        PortableAction::Keyboard(muxe_core::KeyboardAction::SendKeys(_)) => PortableRequestDescription {
            method: "pane.send_keys",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "keys", origin: Origin::Literal(Scalar::Keys) },
            ],
        },
        PortableAction::Keyboard(muxe_core::KeyboardAction::SendText(_)) => PortableRequestDescription {
            method: "pane.send_text",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "text", origin: Origin::Literal(Scalar::String) },
            ],
        },
        PortableAction::Tab(TabAction::Create { command, .. }) => {
            if command.program.is_some() {
                return Ok(None);
            }
            PortableRequestDescription {
                method: "tab.create",
                fields: &[
                    PortableRequestField { name: "workspace_id", origin: Origin::Literal(Scalar::String) },
                    PortableRequestField { name: "label", origin: Origin::Literal(Scalar::String) },
                    PortableRequestField { name: "focus", origin: Origin::Default(Scalar::Bool) },
                    PortableRequestField { name: "cwd", origin: Origin::Literal(Scalar::String) },
                ],
            }
        }
        PortableAction::Tab(TabAction::Close) => PortableRequestDescription {
            method: "tab.close",
            fields: &[PortableRequestField { name: "tab_id", origin: Origin::Origin(ContextType::TabId) }],
        },
        PortableAction::Tab(TabAction::Rename { name: Some(_) }) => PortableRequestDescription {
            method: "tab.rename",
            fields: &[
                PortableRequestField { name: "tab_id", origin: Origin::Origin(ContextType::TabId) },
                PortableRequestField { name: "label", origin: Origin::Literal(Scalar::String) },
            ],
        },
        PortableAction::Tab(TabAction::Rename { name: None }) => {
            return Err(
                "Herdr tab.rename requires `label`; the portable bare `tab:rename` has no specified Herdr prompt mapping"
                    .to_owned(),
            );
        }
        PortableAction::Tab(TabAction::Move(target)) if target_is_index(target) => PortableRequestDescription {
            method: "tab.move",
            fields: &[
                PortableRequestField { name: "tab_id", origin: Origin::Origin(ContextType::TabId) },
                PortableRequestField { name: "insert_index", origin: Origin::Literal(Scalar::Index) },
            ],
        },
        PortableAction::Tab(TabAction::Swap(target)) if target_is_index(target) => return Ok(None),
        PortableAction::Pane(PaneAction::Create) => {
            return Err(
                "Herdr has no pane.create method; pane.split requires an explicit right or down direction"
                    .to_owned(),
            );
        }
        PortableAction::Pane(PaneAction::Split { direction: None, .. }) => {
            return Err("Herdr pane.split requires an explicit right or down direction".to_owned());
        }
        PortableAction::Pane(PaneAction::Split { direction: Some(direction), command, .. })
            if command.program.is_none() && split_direction_is_supported(direction) =>
        {
            PortableRequestDescription {
                method: "pane.split",
                fields: &[
                    PortableRequestField { name: "target_pane_id", origin: Origin::Origin(ContextType::PaneId) },
                    PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::SplitDirection) },
                    PortableRequestField { name: "focus", origin: Origin::Default(Scalar::Bool) },
                    PortableRequestField { name: "cwd", origin: Origin::Literal(Scalar::String) },
                ],
            }
        }
        PortableAction::Pane(PaneAction::Split { direction: Some(direction), .. })
            if !split_direction_is_supported(direction) =>
        {
            return Err("Herdr pane.split supports only right or down directions".to_owned());
        }
        PortableAction::Pane(PaneAction::Split { .. }) => {
            return Err(
                "Herdr command-bearing pane:split requires the ordered dismiss-and-dispatch lifecycle"
                    .to_owned(),
            );
        }
        PortableAction::Pane(PaneAction::Close) => PortableRequestDescription {
            method: "pane.close",
            fields: &[PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) }],
        },
        PortableAction::Pane(PaneAction::Focus(target)) if target_is_cardinal_direction(target) => PortableRequestDescription {
            method: "pane.focus_direction",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::PaneDirection) },
            ],
        },
        PortableAction::Pane(PaneAction::Swap(target)) if target_is_cardinal_direction(target) => PortableRequestDescription {
            method: "pane.swap",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::PaneDirection) },
            ],
        },
        PortableAction::Pane(PaneAction::Resize { .. }) => PortableRequestDescription {
            method: "pane.resize",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::PaneDirection) },
                PortableRequestField { name: "amount", origin: Origin::Literal(Scalar::Number) },
            ],
        },
        PortableAction::Pane(PaneAction::Zoom { .. }) => PortableRequestDescription {
            method: "pane.zoom",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "mode", origin: Origin::Default(Scalar::String) },
            ],
        },
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
    Ok(Some(description))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderedTab {
    id: String,
    workspace: String,
    number: u64,
}
fn creation_has_program(action: &PortableAction) -> bool {
    match action {
        PortableAction::Tab(TabAction::Create { command, .. })
        | PortableAction::Pane(PaneAction::Split { command, .. }) => command.program.is_some(),
        _ => false,
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn creation_requires_post_dismissal(action: &PortableAction) -> Result<bool, AdapterError> {
    match action {
        PortableAction::Tab(TabAction::Create { focus, .. })
        | PortableAction::Pane(PaneAction::Split { focus, .. }) => {
            Ok(focus.as_ref().map(scalar_bool).transpose()?.unwrap_or(true))
        }
        _ => Ok(false),
    }
}

async fn perform_command_creation(
    client: &HerdrSocketClient,
    schema: &ApiSchema,
    action: &PortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<(), AdapterError> {
    match action {
        PortableAction::Tab(TabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        }) if command.program.is_some() => {
            let workspace = workspace_id
                .as_ref()
                .map(scalar_string)
                .transpose()?
                .map(muxe_core::WorkspaceId::new)
                .or_else(|| origin.workspace_id.clone())
                .ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::ContextUnavailable,
                        "Herdr command tab creation requires the captured origin workspace",
                    )
                })?;
            let label = name
                .as_ref()
                .map(scalar_string)
                .transpose()?
                .map(str::to_owned);
            crate::open_command_tab(
                client,
                schema,
                crate::CommandTabLaunch {
                    workspace,
                    label,
                    cwd: creation_cwd(command, origin)?,
                    argv: creation_argv(command)?,
                    focus: focus.as_ref().map(scalar_bool).transpose()?.unwrap_or(true),
                },
            )
            .await
        }
        PortableAction::Pane(PaneAction::Split {
            direction: Some(direction),
            focus,
            command,
        }) if command.program.is_some() => {
            let workspace = origin.workspace_id.clone().ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Herdr command pane creation requires the captured origin workspace",
                )
            })?;
            let tab = origin.tab_id.clone().ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Herdr command pane creation requires the captured origin tab",
                )
            })?;
            let pane = origin.pane_id.clone().ok_or_else(|| {
                AdapterError::new(
                    AdapterErrorKind::ContextUnavailable,
                    "Herdr command pane creation requires the captured origin pane",
                )
            })?;
            let destination = crate::pane_by_identity(client, schema, workspace, tab, pane).await?;
            let direction = match scalar_split_direction(direction)? {
                "right" => crate::UiSplitDirection::Right,
                "down" => crate::UiSplitDirection::Down,
                _ => {
                    unreachable!("scalar_split_direction validates the closed Herdr direction set")
                }
            };
            crate::open_command_pane(
                client,
                schema,
                crate::CommandPaneLaunch {
                    origin: destination.clone(),
                    destination,
                    cwd: creation_cwd(command, origin)?,
                    argv: creation_argv(command)?,
                    direction,
                    ratio: 0.5,
                    focus: focus.as_ref().map(scalar_bool).transpose()?.unwrap_or(true),
                },
            )
            .await
            .map(|_| ())
        }
        _ => Err(incompatible(
            "post-dismissal command creation requires tab:create or pane:split with program",
        )),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn creation_argv(command: &muxe_core::CreateCommand) -> Result<Vec<String>, AdapterError> {
    let program = command
        .program
        .as_ref()
        .ok_or_else(|| incompatible("creation command requires program"))?;
    let program = scalar_string(program)?;
    if program.is_empty() {
        return Err(incompatible("creation command program must not be empty"));
    }
    let mut argv = Vec::with_capacity(command.args.len().saturating_add(1));
    argv.push(program.to_owned());
    for argument in &command.args {
        argv.push(scalar_string(argument)?.to_owned());
    }
    Ok(argv)
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn creation_cwd(
    command: &muxe_core::CreateCommand,
    origin: &muxe_core::OriginContext,
) -> Result<PathBuf, AdapterError> {
    let cwd = command
        .cwd
        .as_ref()
        .map(scalar_string)
        .transpose()?
        .map(PathBuf::from)
        .or_else(|| origin.pane_cwd.clone())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "creation command requires explicit cwd or captured origin pane cwd",
            )
        })?;
    if !cwd.is_absolute() {
        return Err(incompatible("creation command cwd must be absolute"));
    }
    Ok(cwd)
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

/// Load-time scalar readers mirroring the dispatch-time `scalar_*` conversions without
/// allocating an `AdapterError`: they return the converted plain value or a diagnostic
/// fragment the caller attributes to the emitting parameter.
fn scalar_string_value(value: &ActionScalar) -> Result<String, String> {
    scalar_string(value)
        .map(str::to_owned)
        .map_err(|error| error.to_string())
}

fn scalar_bool_value(value: &ActionScalar) -> Result<bool, String> {
    scalar_bool(value).map_err(|error| error.to_string())
}

fn scalar_index_value(value: &ActionScalar) -> Result<u64, String> {
    scalar_index(value).map_err(|error| error.to_string())
}

fn scalar_number_value(value: &ActionScalar) -> Result<f64, String> {
    scalar_number(value).map_err(|error| error.to_string())
}

fn scalar_split_direction_value(value: &ActionScalar) -> Result<String, String> {
    scalar_split_direction(value)
        .map(str::to_owned)
        .map_err(|error| error.to_string())
}

fn scalar_pane_direction_value(value: &ActionScalar) -> Result<String, String> {
    scalar_pane_direction(value)
        .map(str::to_owned)
        .map_err(|error| error.to_string())
}

/// JSON-number probe for a resize amount. Non-finite floats have no JSON encoding, so
/// they reject here exactly as `validate_candidate` rejects non-finite native floats.
fn number_to_json(value: f64) -> Result<Value, String> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| "non-finite floating point values are not valid JSON".to_owned())
}

fn native_candidate_diagnostic(
    candidate: &NativeActionCandidate,
    error: &CandidateValidationError,
) -> ConfigDiagnostic {
    let span = error
        .field
        .as_deref()
        .and_then(|name| candidate.fields.iter().find(|field| field.name == name))
        .map_or_else(
            || candidate.type_span.clone(),
            |field| field.value.span.clone(),
        );
    ConfigDiagnostic::error(
        DiagnosticCode::NativeActionRejected,
        error.error.to_string(),
        span,
    )
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

fn host_rejection(method: &str, code: &str, message: &str) -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::DispatchFailed,
        format!("Herdr {method} rejected request with {code}: {message}"),
    )
}

fn pane_is_proven_absent(code: &str) -> bool {
    code == "pane_not_found"
}

fn pending_cleanup_lease_stale() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::ContextUnavailable,
        "pending Herdr cleanup lease is stale",
    )
}

fn pending_cleanup_outcome_unknown() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::OutcomeUnknown,
        "Herdr may already have closed the pending pane; refusing to close a possibly reused pane ID",
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
            PortableAction::Pane(PaneAction::Split {
                direction: None,
                focus: None,
                command: muxe_core::CreateCommand::default(),
            }),
        ] {
            let Err(error) = portable_invocation(&action, &origin()) else {
                panic!("form without a schema-defined Herdr mapping must fail");
            };
            assert_eq!(error.kind, AdapterErrorKind::Incompatible);
        }
    }

    #[test]
    fn split_without_direction_reports_the_direction_diagnostic_before_any_command() {
        let scalar = |value: &str| {
            ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
                value.to_owned(),
            )))
        };
        let command = muxe_core::CreateCommand {
            program: Some(scalar("tool")),
            args: Vec::new(),
            cwd: None,
        };

        let missing_direction = PortableAction::Pane(PaneAction::Split {
            direction: None,
            focus: None,
            command: command.clone(),
        });
        let Err(error) = portable_invocation(&missing_direction, &origin()) else {
            panic!("split without a direction must fail");
        };
        assert_eq!(
            error.to_string(),
            "Herdr pane.split requires an explicit right or down direction",
            "the direction diagnostic outranks the command-lifecycle diagnostic"
        );

        let command_bearing = PortableAction::Pane(PaneAction::Split {
            direction: Some(scalar("right")),
            focus: None,
            command,
        });
        let Err(error) = portable_invocation(&command_bearing, &origin()) else {
            panic!("command-bearing split must fail without the dismiss lifecycle");
        };
        assert_eq!(
            error.to_string(),
            "Herdr command-bearing pane:split requires the ordered dismiss-and-dispatch lifecycle"
        );
    }

    #[test]
    fn command_creations_require_every_helper_rpc_at_compile_time() {
        let scalar = |value: &str| {
            ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
                value.to_owned(),
            )))
        };
        let command = || muxe_core::CreateCommand {
            program: Some(scalar("tool")),
            args: Vec::new(),
            cwd: None,
        };
        let tab = PortableAction::Tab(TabAction::Create {
            workspace_id: None,
            name: None,
            focus: None,
            command: command(),
        });
        let split = PortableAction::Pane(PaneAction::Split {
            direction: Some(scalar("right")),
            focus: None,
            command: command(),
        });

        assert_eq!(command_creation_methods(&tab), Some(&["layout.apply"][..]));
        assert_eq!(
            command_creation_methods(&split),
            Some(&["session.snapshot", "layout.apply", "pane.move", "tab.close",][..])
        );
    }

    fn deferred_request() -> PostDismissalPortableDispatchRequest {
        PostDismissalPortableDispatchRequest {
            execution: muxe_core::ExecutionId(1),
            action: muxe_adapter_api::ResolvedPortableAction {
                action: PortableAction::Tab(TabAction::Create {
                    workspace_id: None,
                    name: None,
                    focus: None,
                    command: muxe_core::CreateCommand::default(),
                }),
            },
            origin: origin(),
            ui_pane: muxe_core::PaneId::new("muxe-ui"),
        }
    }

    #[test]
    fn pane_close_claim_makes_later_snapshot_failure_nonrejecting() {
        let request = deferred_request();
        let mut pending =
            HashMap::from([(request.ui_pane.as_str().to_owned(), vec![request.clone()])]);

        let claimed = pending.remove(request.ui_pane.as_str());
        assert_eq!(claimed, Some(vec![request.clone()]));
        assert!(
            !snapshot_failure_reclaims_request(
                &mut pending,
                request.ui_pane.as_str(),
                request.execution,
            ),
            "a later snapshot failure must not retract the pane-close dispatch"
        );
    }

    #[test]
    fn malformed_snapshot_does_not_prove_ui_dismissal() {
        let error = snapshot_contains_pane(
            &json!({ "snapshot": {} }),
            &muxe_core::PaneId::new("muxe-ui"),
        )
        .expect_err("a snapshot without panes cannot prove the UI pane is gone");

        assert_eq!(error.kind, AdapterErrorKind::DispatchFailed);
    }

    #[test]
    fn only_focused_creations_require_post_dismissal_dispatch() {
        let focused = PortableAction::Tab(TabAction::Create {
            workspace_id: None,
            name: None,
            focus: None,
            command: muxe_core::CreateCommand::default(),
        });
        let unfocused = PortableAction::Pane(PaneAction::Split {
            direction: Some(ActionScalar::new(ConfigValue::synthetic(
                ConfigValueKind::String("right".to_owned()),
            ))),
            focus: Some(ActionScalar::new(ConfigValue::synthetic(
                ConfigValueKind::Boolean(false),
            ))),
            command: muxe_core::CreateCommand::default(),
        });

        assert!(
            creation_requires_post_dismissal(&focused)
                .expect("default tab creation focus is valid")
        );
        assert!(
            !creation_requires_post_dismissal(&unfocused)
                .expect("explicit unfocused split is valid")
        );
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
    fn bundled_schema() -> ApiSchema {
        let raw = serde_json::from_str(include_str!(
            "../../../fixtures/herdr/herdr-api.schema.json"
        ))
        .expect("bundled fixture schema is valid JSON");
        ApiSchema::parse(raw).expect("bundled schema parses")
    }

    fn mutated_schema(mutate: impl FnOnce(&mut serde_json::Value)) -> ApiSchema {
        let mut raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/herdr/herdr-api.schema.json"
        ))
        .expect("bundled fixture schema is valid JSON");
        mutate(&mut raw);
        ApiSchema::parse(raw).expect("mutated schema parses")
    }

    #[test]
    fn added_required_close_parameter_rejects_at_load_time() {
        // The finding's core case: pane.close is still declared, but the runtime schema
        // demands a new required field the portable action never emits. Load-time
        // validation must reject; the binding must never be published.
        let schema = mutated_schema(|raw| {
            let defs = raw["schemas"]["request"]["$defs"]
                .as_object_mut()
                .expect("bundled schema declares request defs");
            let target = defs
                .get_mut("PaneTarget")
                .expect("bundled schema declares PaneTarget")
                .as_object_mut()
                .expect("PaneTarget is an object schema");
            target
                .entry("properties")
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
                .expect("properties is an object")
                .insert("force".to_owned(), serde_json::json!({ "type": "boolean" }));
            target
                .entry("required")
                .or_insert_with(|| serde_json::json!([]))
                .as_array_mut()
                .expect("required is an array")
                .push(serde_json::json!("force"));
        });
        let error = portable_compile_validation(&schema, &PortableAction::Pane(PaneAction::Close))
            .expect_err("a newly required close parameter must reject at load time");
        assert!(
            error.contains("force"),
            "the diagnostic names the missing required parameter: {error}"
        );
        assert!(
            error.contains("pane.close"),
            "the diagnostic names the affected method: {error}"
        );
        // The rejection must surface as a diagnostic attached to the action span, so the
        // compiler can pin the binding that can never dispatch.
        let validator = HerdrConfigValidator {
            schema: std::sync::Arc::new(schema),
        };
        let span = SourceSpan::new(SourceId::new("<test>"), 7, 3);
        let diagnostic = ActionValidator::validate_portable(
            &validator,
            &PortableAction::Pane(PaneAction::Close),
            &span,
        )
        .expect_err("the validator reports the unusable binding");
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidAction);
        assert!(
            diagnostic.labels.iter().any(|label| label.span == span),
            "the diagnostic is attached to the action span"
        );
    }

    #[test]
    fn narrowed_resize_direction_enum_rejects_literal_at_load_time() {
        // A resize direction the runtime schema's enum dropped must reject at load time,
        // not at invocation after the user selected the binding.
        let schema = mutated_schema(|raw| {
            let pane_direction = raw["schemas"]["request"]["$defs"]["PaneDirection"]
                .as_object_mut()
                .expect("bundled schema declares PaneDirection");
            pane_direction.insert("enum".to_owned(), serde_json::json!(["left", "up"]));
        });
        let direction = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
            "right".to_owned(),
        )));
        let action = PortableAction::Pane(PaneAction::Resize {
            direction,
            amount: None,
        });
        let error = portable_compile_validation(&schema, &action)
            .expect_err("a dropped enum direction must reject at load time");
        assert!(
            error.contains("direction"),
            "the diagnostic names the offending parameter: {error}"
        );
    }

    #[test]
    fn forbidden_emitted_field_rejects_at_load_time() {
        // An emitted field the schema forbids (closed object without that property) must
        // reject at load time rather than at dispatch.
        let mut schema = bundled_schema();
        let description = portable_request_description(&PortableAction::Pane(PaneAction::Close))
            .expect("pane:close has a description")
            .expect("pane:close emits a single request");
        // Sanity: the unmutated schema accepts the described request.
        validate_portable_request(
            &schema,
            &PortableAction::Pane(PaneAction::Close),
            &description,
        )
        .expect("bundled schema accepts pane:close");
        // Narrow PaneTarget to declare none of the emitted properties while still
        // requiring pane_id: the emitted field set is then forbidden.
        schema = mutated_schema(|raw| {
            let defs = raw["schemas"]["request"]["$defs"]
                .as_object_mut()
                .expect("bundled schema declares request defs");
            let target = defs
                .get_mut("PaneTarget")
                .expect("bundled schema declares PaneTarget")
                .as_object_mut()
                .expect("PaneTarget is an object schema");
            target.insert("properties".to_owned(), serde_json::json!({}));
        });
        let error = portable_compile_validation(&schema, &PortableAction::Pane(PaneAction::Close))
            .expect_err("an undeclared emitted field must reject at load time");
        assert!(
            error.contains("pane_id"),
            "the diagnostic names the forbidden parameter: {error}"
        );
    }

    #[test]
    fn pane_close_dispatch_emits_exact_close_request() {
        // Guards the restored arm: portable pane:close dispatches exactly
        // method "pane.close" with params { "pane_id": <origin pane> }.
        let invocation = portable_invocation(&PortableAction::Pane(PaneAction::Close), &origin())
            .expect("pane:close dispatches");
        assert_eq!(invocation.method, "pane.close");
        assert_eq!(
            invocation.params,
            serde_json::json!({ "pane_id": "pane" }),
            "pane:close emits exactly the origin pane id"
        );
    }

    #[test]
    fn context_domain_mismatch_rejects_at_load_time() {
        // An unsigned-integer context marker in a string-typed parameter must reject at
        // load time: `origin.tab.index` can never satisfy `tab.rename`'s `label`.
        let source = SourceSpan::new(SourceId::new("<test>"), 0, 0);
        let name = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::Context(
            ContextReference::parse("origin.tab.index", source)
                .expect("known unsigned context path"),
        )));
        let action = PortableAction::Tab(TabAction::Rename { name: Some(name) });
        let schema = bundled_schema();
        let error = portable_compile_validation(&schema, &action)
            .expect_err("an integer context marker in a string parameter must reject");
        assert!(
            error.contains("label"),
            "the diagnostic names the offending parameter: {error}"
        );
        assert!(
            error.contains("tab.rename"),
            "the diagnostic names the affected method: {error}"
        );
        assert!(
            error.contains("does not fit"),
            "the diagnostic names the domain mismatch: {error}"
        );
        // The rejection must surface as a diagnostic attached to the action span, so the
        // compiler can pin the binding that can never dispatch.
        let validator = HerdrConfigValidator {
            schema: std::sync::Arc::new(schema),
        };
        let span = SourceSpan::new(SourceId::new("<test>"), 7, 3);
        let diagnostic = ActionValidator::validate_portable(&validator, &action, &span)
            .expect_err("the validator reports the unusable binding");
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidAction);
        assert!(
            diagnostic.labels.iter().any(|label| label.span == span),
            "the diagnostic is attached to the action span"
        );
    }

    #[test]
    fn pane_resize_dispatch_emits_exact_resize_request() {
        // Guards the multi-field arm: portable pane:resize dispatches exactly method
        // "pane.resize" with params { "pane_id", "direction", "amount" }, so a deleted
        // or narrowed arm fails loudly instead of dispatching a silent wrong shape.
        let direction = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
            "right".to_owned(),
        )));
        let amount = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::Float(2.0)));
        let action = PortableAction::Pane(PaneAction::Resize {
            direction,
            amount: Some(amount),
        });
        let invocation = portable_invocation(&action, &origin()).expect("pane:resize dispatches");
        assert_eq!(invocation.method, "pane.resize");
        assert_eq!(
            invocation.params,
            serde_json::json!({ "pane_id": "pane", "direction": "right", "amount": 2.0 }),
            "pane:resize emits exactly the origin pane id with direction and amount"
        );
    }
}
