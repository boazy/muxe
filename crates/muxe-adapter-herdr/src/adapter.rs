use std::{
    collections::HashMap,
    future::{Future, poll_fn},
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
    ExecutionCorrelationId, HostAdapter, HostContinuityEpoch, HostIdentity, HostSchemaFingerprint,
    KeyboardCapabilities, ModalScopeId, NativeCompatibilityIdentity, NativeCompatibilityOutcome,
    NativeCompatibilitySnapshot, NativeCompatibilityValidator, NativeDispatchRequest,
    OriginCaptureRequest, PendingPaneLease, PendingPaneLeaseId, PendingPaneRegistration,
    PortableDispatchRequest, PostDismissalPortableDispatchRequest,
};
use muxe_core::{
    ActionScalar, ActionValidation, ActionValidator, ConfigDiagnostic, ConfigValueKind,
    ContextType, DiagnosticCode, Direction, ExecutionCapabilities, NativeActionCandidate,
    PaneAction, PortableAction, ResizeAmount, ResolvedCreateCommand, ResolvedKeyboardAction,
    ResolvedPaneAction, ResolvedPaneTarget, ResolvedPortableAction, ResolvedTabAction,
    ResolvedTabTarget, TabAction,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;

use crate::{
    ApiSchema, CandidateValidationError, ComparisonKey, DeliveryState, EventSubscription,
    HerdrAdapterConfig, HerdrCache, HerdrResponse, HerdrRuntime, SocketError, SubscriptionConfig,
    SubscriptionEvent, fields_to_json,
    generated::{BUNDLED_REQUEST_SCHEMA_SHA256, method_metadata},
    runtime::{
        ClassifiedInvokeError, GuardedHerdrInvoker, HerdrRequestAuthority, IncarnationEpoch,
        IncarnationLease, OrderedReceiver, PreparedInvocation,
    },
    validate_candidate,
};

const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_RETRY: Duration = Duration::from_secs(1);
/// Ten seconds covers a normal Herdr restart while ensuring a permanently lost host
/// reaches the broker's terminal lifecycle path instead of retrying forever.
const HOST_LOSS_GRACE: Duration = Duration::from_secs(10);

pub struct HerdrAdapter {
    incarnation: RwLock<IncarnationState>,
    config: HerdrAdapterConfig,
    cache: HerdrCache,
    events_tx: mpsc::Sender<AdapterHealthEvent>,
    events_rx: Mutex<mpsc::Receiver<AdapterHealthEvent>>,
    continuity_loss_tx: mpsc::UnboundedSender<ContinuityLoss>,
    next_correlation: AtomicU64,
    shutdown: AtomicBool,
    suspended: AtomicBool,
    suspend_wake: Notify,
    suspend: StdMutex<SuspendCoordinator>,
    suspend_changed: Notify,
    resume_wake: Notify,
    resume_slot: Mutex<Option<EventSubscription>>,
    resume_registry: StdMutex<ResumeRegistry>,
    resumes_drained: Notify,
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
    pending_leases: Arc<StdMutex<HashMap<PendingPaneLeaseId, PendingPaneLeaseRecord>>>,
    post_dismissal: Mutex<HashMap<muxe_core::PaneId, Vec<PostDismissalPortableDispatchRequest>>>,
    health_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    reconnect_install_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    resume_install_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    resume_drain_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    suspend_release_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
    host_lost_reserve_pending_hook: StdMutex<Option<Arc<WaitHook>>>,
    request_connect_wait_hook: StdMutex<Option<Arc<WaitHook>>>,
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

struct ResumeRegistry {
    closed: bool,
    active: usize,
}

struct ResumeGuard<'a> {
    adapter: &'a HerdrAdapter,
}

impl Drop for ResumeGuard<'_> {
    fn drop(&mut self) {
        let mut resumes = self
            .adapter
            .resume_registry
            .lock()
            .expect("Herdr resume registry is not poisoned");
        resumes.active = resumes.active.saturating_sub(1);
        if resumes.active == 0 {
            self.adapter.resumes_drained.notify_waiters();
        }
    }
}

/// Transport-free validator for inspecting configuration against the exact
/// installed Herdr request schema.
pub struct HerdrConfigValidator {
    schema: Arc<ApiSchema>,
}

#[derive(Clone, Debug)]
struct HerdrNativeCompatibilityValidator {
    schema: Arc<ApiSchema>,
    cache: HerdrCache,
}

impl NativeCompatibilityValidator for HerdrNativeCompatibilityValidator {
    fn validate_native(&self, candidate: &NativeActionCandidate) -> NativeCompatibilityOutcome {
        match validate_native_batch_cached(&self.schema, &self.cache, &[candidate]) {
            Ok(mut validations) => NativeCompatibilityOutcome::Compatible(
                validations
                    .pop()
                    .expect("one candidate produces one compatibility validation"),
            ),
            Err(diagnostics) => NativeCompatibilityOutcome::Blocked(diagnostics),
        }
    }
}

impl HerdrConfigValidator {
    /// Loads the installed schema through the bounded owned-child path without
    /// probing or subscribing to a Herdr socket.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] when the schema command, cache, or schema
    /// parser fails.
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

async fn wait_on_hook(hook: &StdMutex<Option<Arc<WaitHook>>>) {
    let hook = hook
        .lock()
        .expect("Herdr lifecycle hook is not poisoned")
        .clone();
    if let Some(hook) = hook {
        hook.entered.notify_one();
        hook.release.notified().await;
    }
}

struct IncarnationState {
    runtime: Arc<HerdrRuntime>,
    epoch: IncarnationEpoch,
    continuity: HostContinuityEpoch,
    healthy: bool,
}
impl IncarnationState {
    fn transition_to_lost(&mut self, lease: &IncarnationLease) -> bool {
        if !self.healthy || self.epoch != lease.epoch() || self.runtime.lease(self.epoch) != *lease
        {
            return false;
        }
        self.healthy = false;
        self.epoch = self.epoch.next();
        self.continuity = self
            .continuity
            .successor()
            .expect("Herdr continuity epoch exhausted");
        true
    }

    fn install(
        &mut self,
        runtime: Arc<HerdrRuntime>,
        lease: &IncarnationLease,
        compatibility: NativeCompatibilitySnapshot,
    ) -> Option<(HostIdentity, HostIdentity, NativeCompatibilitySnapshot)> {
        if self.healthy || self.epoch != lease.epoch() || runtime.lease(self.epoch) != *lease {
            return None;
        }
        let previous = self.runtime.identity().clone();
        let current = runtime.identity().clone();
        self.runtime = runtime;
        self.healthy = true;
        Some((previous, current, compatibility))
    }
}

#[derive(Clone)]
struct ContinuityLoss {
    lease: IncarnationLease,
    error: AdapterError,
}

#[derive(Clone)]
struct IncarnationAuthority {
    runtime: Arc<HerdrRuntime>,
    lease: IncarnationLease,
    continuity_loss_tx: mpsc::UnboundedSender<ContinuityLoss>,
    request_connect_wait_hook: Option<Arc<WaitHook>>,
}

impl IncarnationAuthority {
    async fn invoke_response_with_delivery(
        &self,
        method: &str,
        params: Value,
    ) -> Result<HerdrResponse, (AdapterError, DeliveryState)> {
        let invocation = self
            .runtime
            .prepare_invocation(method, params)
            .map_err(|error| (error, DeliveryState::NotSent))?;
        let direct = self.clone();
        self.runtime
            .run_ordered(move |invoker| async move {
                direct
                    .transaction_authority(invoker)
                    .invoke_prepared_with_delivery(invocation)
                    .await
            })
            .await
            .map_err(|error| (error, DeliveryState::NotSent))?
    }

    async fn invoke_response(
        &self,
        method: &str,
        params: Value,
    ) -> Result<HerdrResponse, AdapterError> {
        self.invoke_response_with_delivery(method, params)
            .await
            .map_err(|(error, _)| error)
    }

    async fn invoke(&self, method: &str, params: Value) -> Result<Value, AdapterError> {
        match self.invoke_response(method, params).await? {
            HerdrResponse::Success(result) => Ok(result),
            HerdrResponse::Error { code, message } => Err(host_rejection(method, &code, &message)),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn verify_endpoint_file(&self) -> Result<(), AdapterError> {
        self.runtime
            .verify_endpoint_file(&self.lease)
            .inspect_err(|error| {
                let _ = self.continuity_loss_tx.send(ContinuityLoss {
                    lease: self.lease.clone(),
                    error: error.clone(),
                });
            })
    }

    async fn run_ordered<T, F, Fut>(&self, operation: F) -> Result<T, AdapterError>
    where
        T: Send + 'static,
        F: FnOnce(IncarnationTransactionAuthority) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let authority = self.clone();
        self.runtime
            .run_ordered(move |invoker| operation(authority.transaction_authority(invoker)))
            .await
    }

    #[expect(
        clippy::result_large_err,
        reason = "ordered dispatch preserves the adapter's shared error type"
    )]
    fn try_run_ordered<T, F, Fut>(&self, operation: F) -> Result<OrderedReceiver<T>, AdapterError>
    where
        T: Send + 'static,
        F: FnOnce(IncarnationTransactionAuthority) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let authority = self.clone();
        self.runtime
            .try_run_ordered(move |invoker| operation(authority.transaction_authority(invoker)))
    }

    fn transaction_authority(
        &self,
        invoker: GuardedHerdrInvoker,
    ) -> IncarnationTransactionAuthority {
        IncarnationTransactionAuthority {
            invoker,
            lease: self.lease.clone(),
            continuity_loss_tx: self.continuity_loss_tx.clone(),
            request_connect_wait_hook: self.request_connect_wait_hook.clone(),
        }
    }
}

struct IncarnationTransactionAuthority {
    invoker: GuardedHerdrInvoker,
    lease: IncarnationLease,
    continuity_loss_tx: mpsc::UnboundedSender<ContinuityLoss>,
    request_connect_wait_hook: Option<Arc<WaitHook>>,
}

impl IncarnationTransactionAuthority {
    async fn invoke_prepared_with_delivery(
        &self,
        invocation: PreparedInvocation,
    ) -> Result<HerdrResponse, (AdapterError, DeliveryState)> {
        if let Some(hook) = &self.request_connect_wait_hook {
            hook.entered.notify_one();
            hook.release.notified().await;
        }
        self.invoker
            .invoke_prepared(&self.lease, invocation)
            .await
            .map_err(|error| self.classify(error))
    }

    async fn invoke_response_with_delivery(
        &self,
        method: &str,
        params: Value,
    ) -> Result<HerdrResponse, (AdapterError, DeliveryState)> {
        let invocation = self
            .invoker
            .prepare(method, params)
            .map_err(|error| (error, DeliveryState::NotSent))?;
        self.invoke_prepared_with_delivery(invocation).await
    }
    async fn invoke_response(
        &self,
        method: &str,
        params: Value,
    ) -> Result<HerdrResponse, AdapterError> {
        self.invoke_response_with_delivery(method, params)
            .await
            .map_err(|(error, _)| error)
    }

    fn classify(&self, error: ClassifiedInvokeError) -> (AdapterError, DeliveryState) {
        if error.continuity_lost {
            let _ = self.continuity_loss_tx.send(ContinuityLoss {
                lease: self.lease.clone(),
                error: error.error.clone(),
            });
        }
        (error.error, error.delivery)
    }
}

#[async_trait]
impl HerdrRequestAuthority for IncarnationTransactionAuthority {
    async fn request(&self, method: &str, params: Value) -> Result<HerdrResponse, AdapterError> {
        self.invoke_response_with_delivery(method, params)
            .await
            .map_err(|(error, _)| error)
    }
}

#[async_trait]
impl HerdrRequestAuthority for IncarnationAuthority {
    async fn request(&self, method: &str, params: Value) -> Result<HerdrResponse, AdapterError> {
        self.invoke_response(method, params).await
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SuspendGeneration(u64);

impl SuspendGeneration {
    fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Debug)]
enum SuspendPhase {
    Requested,
    Released,
    HostLost(AdapterError),
    Shutdown,
}

#[derive(Clone, Debug)]
struct SuspendAttempt {
    generation: SuspendGeneration,
    lease: IncarnationLease,
    phase: SuspendPhase,
}

#[derive(Clone, Debug)]
struct TerminalLoss {
    lease: IncarnationLease,
    error: AdapterError,
}

struct SuspendCoordinator {
    next_generation: SuspendGeneration,
    current: Option<SuspendAttempt>,
    terminal_loss: Option<TerminalLoss>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HostLostSendOutcome {
    Published,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncarnationInstallOutcome {
    Installed,
    Retry,
    LifecycleChanged,
    Stop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingPaneCloseState {
    Open,
    CloseMayHaveApplied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingPaneLeaseRecord {
    incarnation: IncarnationLease,
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
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let subscription_config = subscription_config();
        let (subscription, _) =
            EventSubscription::connect_expected(&runtime, lease, subscription_config)
                .await
                .map_err(|error| socket_error(&error))?;
        let identity = runtime.identity().clone();
        let (events_tx, events_rx) = mpsc::channel(64);
        let (continuity_loss_tx, continuity_loss_rx) = mpsc::unbounded_channel();
        let (dispatch_results_tx, dispatch_results_rx) = mpsc::unbounded_channel();
        let adapter = Arc::new(Self {
            incarnation: RwLock::new(IncarnationState {
                runtime,
                epoch: IncarnationEpoch::INITIAL,
                continuity: HostContinuityEpoch::initial(),
                healthy: true,
            }),
            config,
            cache,
            events_tx,
            events_rx: Mutex::new(events_rx),
            continuity_loss_tx,
            next_correlation: AtomicU64::new(1),
            shutdown: AtomicBool::new(false),
            suspended: AtomicBool::new(false),
            suspend_wake: Notify::new(),
            suspend: StdMutex::new(SuspendCoordinator {
                next_generation: SuspendGeneration(1),
                current: None,
                terminal_loss: None,
            }),
            suspend_changed: Notify::new(),
            health_wait_hook: StdMutex::new(None),
            reconnect_install_wait_hook: StdMutex::new(None),
            resume_install_wait_hook: StdMutex::new(None),
            resume_drain_wait_hook: StdMutex::new(None),
            suspend_release_wait_hook: StdMutex::new(None),
            host_lost_reserve_pending_hook: StdMutex::new(None),
            request_connect_wait_hook: StdMutex::new(None),
            pending_leases: Arc::new(StdMutex::new(HashMap::new())),
            resume_wake: Notify::new(),
            resume_slot: Mutex::new(None),
            resume_registry: StdMutex::new(ResumeRegistry {
                closed: false,
                active: 0,
            }),
            resumes_drained: Notify::new(),
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
                continuity_loss_rx,
            )));
        Ok(adapter)
    }

    fn runtime(&self) -> Arc<HerdrRuntime> {
        self.incarnation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .runtime
            .clone()
    }

    fn identity(&self) -> HostIdentity {
        self.incarnation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .runtime
            .identity()
            .clone()
    }
    fn transition_to_lost(&self, lease: &IncarnationLease) -> bool {
        let mut incarnation = self
            .incarnation
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shutdown.load(Ordering::Relaxed) || self.suspended.load(Ordering::SeqCst) {
            return false;
        }
        let transitioned = incarnation.transition_to_lost(lease);
        if transitioned {
            incarnation.runtime.retire_send_queue();
        }
        transitioned
    }

    fn reconnect_epoch(&self) -> Option<IncarnationEpoch> {
        let incarnation = self
            .incarnation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (!incarnation.healthy).then_some(incarnation.epoch)
    }

    async fn install_reconnected(
        &self,
        runtime: Arc<HerdrRuntime>,
        lease: &IncarnationLease,
    ) -> IncarnationInstallOutcome {
        wait_on_hook(&self.reconnect_install_wait_hook).await;
        let permit = match self.events_tx.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(())) => {
                return IncarnationInstallOutcome::Retry;
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                return IncarnationInstallOutcome::Stop;
            }
        };
        let mut incarnation = self
            .incarnation
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shutdown.load(Ordering::Relaxed) || self.suspended.load(Ordering::SeqCst) {
            return IncarnationInstallOutcome::LifecycleChanged;
        }
        let compatibility =
            native_compatibility_snapshot(&runtime, incarnation.continuity, &self.cache);
        let Some((previous, current, compatibility)) =
            incarnation.install(runtime, lease, compatibility)
        else {
            return IncarnationInstallOutcome::Retry;
        };
        permit.send(AdapterHealthEvent::Reconnected {
            previous,
            current,
            compatibility,
        });
        IncarnationInstallOutcome::Installed
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would make resume admission inconsistent with the adapter lifecycle API"
    )]
    fn admit_resume(&self) -> Result<ResumeGuard<'_>, AdapterError> {
        let mut resumes = self
            .resume_registry
            .lock()
            .expect("Herdr resume registry is not poisoned");
        if resumes.closed || self.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "The Herdr adapter is shut down and cannot resume after activation was aborted.",
            ));
        }
        resumes.active = resumes.active.saturating_add(1);
        Ok(ResumeGuard { adapter: self })
    }

    async fn shutdown_cancellable<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, AdapterError> {
        tokio::pin!(future);
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "Herdr adapter shut down during activation resume",
                ));
            }
            tokio::select! {
                result = &mut future => return Ok(result),
                () = self.suspend_wake.notified() => {
                    if self.shutdown.load(Ordering::Relaxed) {
                        return Err(AdapterError::new(
                            AdapterErrorKind::Shutdown,
                            "Herdr adapter shut down during activation resume",
                        ));
                    }
                }
            }
        }
    }

    async fn close_resumes_and_wait(&self) {
        {
            let mut resumes = self
                .resume_registry
                .lock()
                .expect("Herdr resume registry is not poisoned");
            resumes.closed = true;
        }
        self.suspend_wake.notify_waiters();
        loop {
            let drained = self.resumes_drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if self
                .resume_registry
                .lock()
                .expect("Herdr resume registry is not poisoned")
                .active
                == 0
            {
                return;
            }
            wait_on_hook(&self.resume_drain_wait_hook).await;
            drained.await;
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would make suspend admission inconsistent with the lifecycle API"
    )]
    fn begin_or_join_suspend(&self) -> Result<(SuspendGeneration, bool), AdapterError> {
        let mut incarnation = self
            .incarnation
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "The Herdr adapter is shut down and cannot be suspended for activation.",
            ));
        }
        let mut suspend = self
            .suspend
            .lock()
            .expect("Herdr suspend coordinator is not poisoned");
        if let Some(attempt) = &suspend.current {
            return Ok((attempt.generation, false));
        }
        let generation = suspend.next_generation;
        suspend.next_generation = generation.next();
        let (lease, phase, requested) = if let Some(loss) = &suspend.terminal_loss {
            (
                loss.lease.clone(),
                SuspendPhase::HostLost(loss.error.clone()),
                false,
            )
        } else {
            (
                incarnation.runtime.lease(incarnation.epoch),
                SuspendPhase::Requested,
                true,
            )
        };
        suspend.current = Some(SuspendAttempt {
            generation,
            lease,
            phase,
        });
        self.suspended.store(true, Ordering::SeqCst);
        incarnation.healthy = false;
        incarnation.runtime.retire_send_queue();
        Ok((generation, requested))
    }

    async fn wait_for_suspend(&self, generation: SuspendGeneration) -> Result<(), AdapterError> {
        loop {
            let changed = self.suspend_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let phase = self
                .suspend
                .lock()
                .expect("Herdr suspend coordinator is not poisoned")
                .current
                .as_ref()
                .filter(|attempt| attempt.generation == generation)
                .map(|attempt| attempt.phase.clone());
            match phase {
                Some(SuspendPhase::Requested) => changed.await,
                Some(SuspendPhase::Released) => return Ok(()),
                Some(SuspendPhase::HostLost(error)) => return Err(error),
                Some(SuspendPhase::Shutdown) | None => {
                    return Err(AdapterError::new(
                        AdapterErrorKind::Shutdown,
                        "Herdr suspend attempt ended during shutdown",
                    ));
                }
            }
        }
    }

    fn requested_suspend_for(&self, lease: Option<&IncarnationLease>) -> Option<SuspendGeneration> {
        self.suspend
            .lock()
            .expect("Herdr suspend coordinator is not poisoned")
            .current
            .as_ref()
            .filter(|attempt| {
                lease.is_none_or(|lease| attempt.lease == *lease)
                    && matches!(attempt.phase, SuspendPhase::Requested)
            })
            .map(|attempt| attempt.generation)
    }

    fn complete_suspend(&self, generation: SuspendGeneration, phase: SuspendPhase) {
        let mut suspend = self
            .suspend
            .lock()
            .expect("Herdr suspend coordinator is not poisoned");
        if let Some(attempt) = &mut suspend.current
            && attempt.generation == generation
            && matches!(attempt.phase, SuspendPhase::Requested)
        {
            attempt.phase = phase;
            self.suspend_changed.notify_waiters();
        }
    }

    fn record_terminal_loss(&self, lease: &IncarnationLease, error: &AdapterError) {
        let mut suspend = self
            .suspend
            .lock()
            .expect("Herdr suspend coordinator is not poisoned");
        suspend.terminal_loss = Some(TerminalLoss {
            lease: lease.clone(),
            error: error.clone(),
        });
        if let Some(attempt) = &mut suspend.current
            && attempt.lease == *lease
            && matches!(attempt.phase, SuspendPhase::Requested)
        {
            attempt.phase = SuspendPhase::HostLost(error.clone());
        }
        self.suspend_changed.notify_waiters();
    }

    fn complete_suspend_shutdown(&self) {
        let mut suspend = self
            .suspend
            .lock()
            .expect("Herdr suspend coordinator is not poisoned");
        if let Some(attempt) = &mut suspend.current {
            attempt.phase = SuspendPhase::Shutdown;
        }
        self.suspend_changed.notify_waiters();
    }

    #[doc(hidden)]
    pub fn set_health_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .health_wait_hook
            .lock()
            .expect("Herdr health hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn set_reconnect_install_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .reconnect_install_wait_hook
            .lock()
            .expect("Herdr reconnect-install hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn set_resume_install_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .resume_install_wait_hook
            .lock()
            .expect("Herdr resume-install hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn set_resume_drain_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .resume_drain_wait_hook
            .lock()
            .expect("Herdr resume-drain hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn set_suspend_release_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .suspend_release_wait_hook
            .lock()
            .expect("Herdr suspend-release hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn set_host_lost_reserve_pending_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .host_lost_reserve_pending_hook
            .lock()
            .expect("Herdr HostLost reserve-pending hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn set_request_connect_wait_hook(&self, hook: Option<Arc<WaitHook>>) {
        *self
            .request_connect_wait_hook
            .lock()
            .expect("Herdr request-connect hook is not poisoned") = hook;
    }

    #[doc(hidden)]
    pub fn fill_health_queue_for_test(&self) -> usize {
        let mut filled = 0;
        while self
            .events_tx
            .try_send(AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "deterministic health-queue filler",
                ),
            })
            .is_ok()
        {
            filled += 1;
        }
        filled
    }

    #[doc(hidden)]
    pub async fn drain_health_queue_for_test(&self) -> Vec<AdapterHealthEvent> {
        let mut events = self.events_rx.lock().await;
        let mut drained = Vec::new();
        while let Ok(event) = events.try_recv() {
            drained.push(event);
        }
        drained
    }

    #[doc(hidden)]
    pub fn shutdown_started_for_test(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn suspended_for_test(&self) -> bool {
        self.suspended.load(Ordering::SeqCst)
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
        spawn: impl FnOnce() -> Result<JoinHandle<()>, AdapterError>,
    ) -> Result<(), AdapterError> {
        let mut registry = self
            .dispatch_tasks
            .lock()
            .expect("retained Herdr dispatch registry is not poisoned");
        if registry.closed {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "The Herdr adapter is shutting down and cannot accept another action.",
            ));
        }
        if registry.tasks.contains_key(&execution) {
            return Err(AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                "The Herdr adapter has already accepted this action's execution ID.",
            ));
        }
        // Keep the admission lock through queue insertion and task registration.
        // Queue insertion is dispatch acceptance; shutdown cannot observe an
        // accepted request without also owning its completion waiter.
        let task = spawn()?;
        registry.tasks.insert(execution, task);
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
    /// while its retained stream is released, and only then does the exact
    /// runtime/epoch lease authorize the call. `identity` stays on continuity
    /// alone (it returns the retained identity, never a new host request) so
    /// its error behavior is unchanged.
    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn require_lifecycle(&self) -> Result<IncarnationAuthority, AdapterError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(AdapterError::new(
                AdapterErrorKind::Shutdown,
                "The Herdr adapter is shut down. Host operations are blocked.",
            ));
        }
        if self.suspended.load(Ordering::SeqCst) {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "The Herdr adapter is suspended for activation. Host operations are blocked until activation is completed or the adapter resumes.",
            ));
        }
        self.require_continuity()
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn require_continuity(&self) -> Result<IncarnationAuthority, AdapterError> {
        let incarnation = self
            .incarnation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if incarnation.healthy {
            Ok(IncarnationAuthority {
                runtime: Arc::clone(&incarnation.runtime),
                lease: incarnation.runtime.lease(incarnation.epoch),
                continuity_loss_tx: self.continuity_loss_tx.clone(),
                request_connect_wait_hook: self
                    .request_connect_wait_hook
                    .lock()
                    .expect("Herdr request-connect hook is not poisoned")
                    .clone(),
            })
        } else {
            Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Muxe cannot verify continuity of its Herdr event subscription. Host operations are blocked until a new subscription passes compatibility checks.",
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
    ) -> Result<IncarnationAuthority, AdapterError> {
        let authority = self.require_lifecycle()?;
        if !origin_is_current(
            origin,
            authority.runtime.identity(),
            authority.lease.epoch(),
        ) {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "The host context captured for this action does not match the current Herdr connection.",
            ));
        }
        Ok(authority)
    }

    fn validate_native_batch_cached(
        &self,
        candidates: &[&NativeActionCandidate],
    ) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
        let runtime = self.runtime();
        validate_native_batch_cached(runtime.schema(), &self.cache, candidates)
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
        authority: IncarnationAuthority,
        execution: muxe_core::ExecutionId,
        invocation: Invocation,
    ) -> Result<DispatchAccepted, AdapterError> {
        authority.verify_endpoint_file()?;
        // The authority joins origin validation, one schema validation, and
        // the exact endpoint lease used by the queue owner.
        let method = invocation.method;
        let prepared = authority
            .runtime
            .prepare_invocation(method, invocation.params)?;
        let results = self.dispatch_results_tx.clone();
        self.admit_dispatch_task(execution, move || {
            let receiver = authority.try_run_ordered(move |direct| async move {
                match direct.invoke_prepared_with_delivery(prepared).await {
                    Ok(HerdrResponse::Success(_)) => DispatchCompletion::Succeeded { execution },
                    Ok(HerdrResponse::Error { code, message }) => DispatchCompletion::Failed {
                        execution,
                        error: AdapterError::new(
                            AdapterErrorKind::DispatchFailed,
                            format!("Herdr rejected {method} ({code}): {message}"),
                        ),
                    },
                    Err((error, DeliveryState::MayHaveReachedHost)) => {
                        DispatchCompletion::OutcomeUnknown { execution, error }
                    }
                    Err((error, DeliveryState::NotSent)) => {
                        DispatchCompletion::Failed { execution, error }
                    }
                }
            })?;
            Ok(tokio::spawn(async move {
                let completion = HerdrRuntime::await_ordered(receiver)
                    .await
                    .unwrap_or_else(|error| DispatchCompletion::Failed { execution, error });
                let _ = results.send(DispatchTerminal {
                    execution,
                    completion,
                });
            }))
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
        target_selector: muxe_core::TabIndex,
    ) -> Result<DispatchAccepted, AdapterError> {
        let authority = self.require_current_origin(origin)?;
        let source_tab = origin.tab_id.clone().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "portable tab action requires the captured origin tab",
            )
        })?;
        let workspace = origin.workspace_id.clone().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr tab:swap requires the captured origin workspace",
            )
        })?;
        authority
            .runtime
            .validate_method_set(&["tab.list", "tab.move"])?;
        let target_number = PublicTabNumber::from_selector(target_selector);
        let results = self.dispatch_results_tx.clone();
        self.admit_dispatch_task(execution, move || {
            let receiver = authority.try_run_ordered(move |direct| async move {
                match perform_tab_swap(&direct, &workspace, &source_tab, target_number).await {
                    Ok(()) => DispatchCompletion::Succeeded { execution },
                    Err(TabSwapError::Known(message)) => DispatchCompletion::Failed {
                        execution,
                        error: AdapterError::new(AdapterErrorKind::DispatchFailed, message),
                    },
                    Err(TabSwapError::Unknown(message)) => DispatchCompletion::OutcomeUnknown {
                        execution,
                        error: AdapterError::new(AdapterErrorKind::OutcomeUnknown, message),
                    },
                }
            })?;
            Ok(tokio::spawn(async move {
                let completion = HerdrRuntime::await_ordered(receiver)
                    .await
                    .unwrap_or_else(|error| DispatchCompletion::Failed { execution, error });
                let _ = results.send(DispatchTerminal {
                    execution,
                    completion,
                });
            }))
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
        let authority = self.require_current_origin(&request.origin)?;
        if creation_has_program(&request.action) {
            return self.dispatch_command_creation(
                request.execution,
                &request.action,
                &request.origin,
            );
        }
        let invocation = portable_invocation(&request.action, &request.origin)?;
        self.dispatch(authority, request.execution, invocation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn dispatch_command_creation(
        &self,
        execution: muxe_core::ExecutionId,
        action: &ResolvedPortableAction,
        origin: &muxe_core::OriginContext,
    ) -> Result<DispatchAccepted, AdapterError> {
        let authority = self.require_current_origin(origin)?;
        if let Some(methods) = command_creation_methods(action) {
            authority.runtime.validate_method_set(methods)?;
        }
        let results = self.dispatch_results_tx.clone();
        let action = action.clone();
        let origin = origin.clone();
        self.admit_dispatch_task(execution, move || {
            let receiver = authority.try_run_ordered(move |direct| async move {
                match perform_command_creation(&direct, &action, &origin).await {
                    Ok(()) => DispatchCompletion::Succeeded { execution },
                    Err(error) if error.kind == AdapterErrorKind::OutcomeUnknown => {
                        DispatchCompletion::OutcomeUnknown { execution, error }
                    }
                    Err(error) => DispatchCompletion::Failed { execution, error },
                }
            })?;
            Ok(tokio::spawn(async move {
                let completion = HerdrRuntime::await_ordered(receiver)
                    .await
                    .unwrap_or_else(|error| DispatchCompletion::Failed { execution, error });
                let _ = results.send(DispatchTerminal {
                    execution,
                    completion,
                });
            }))
        })?;
        Ok(self.post_dismissal_accepted(execution))
    }
    async fn dispatch_when_ui_is_gone(
        &self,
        request: PostDismissalPortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        self.require_current_origin(&request.origin)?;
        let pane = request.ui_pane.clone();
        let execution = request.execution;
        self.post_dismissal
            .lock()
            .await
            .entry(pane.clone())
            .or_default()
            .push(request);
        let is_live = match self.ui_pane_is_live(&pane).await {
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
            self.start_post_dismissals(queued);
        }
        Ok(self.post_dismissal_accepted(execution))
    }

    async fn observe_subscription_event(&self, mut event: Value) {
        let Some(pane) = take_dismissed_pane(&mut event) else {
            return;
        };
        let queued = self.post_dismissal.lock().await.remove(&pane);
        if let Some(queued) = queued {
            self.start_post_dismissals(queued);
        }
    }

    fn start_post_dismissals(&self, queued: Vec<PostDismissalPortableDispatchRequest>) {
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

    async fn invoke_unary(&self, method: &str, params: Value) -> Result<Value, AdapterError> {
        self.require_lifecycle()?.invoke(method, params).await
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    fn pending_close_record(
        &self,
        registration: &PendingPaneRegistration,
        lease: &PendingPaneLease,
        incarnation: &IncarnationLease,
    ) -> Result<PendingPaneLeaseRecord, AdapterError> {
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .get(&lease.id)
            .cloned()
            .filter(|record| {
                lease.ui_session == registration.ui_session
                    && record.incarnation == *incarnation
                    && record.ui_session == lease.ui_session
                    && record.pane == registration.pane
                    && record.temporary_tab == registration.temporary_tab
            })
            .ok_or_else(pending_cleanup_lease_stale)
    }
}

/// Takes the pane ID out of a `pane_closed` or `pane_exited` subscription event.
///
/// Herdr emits `pane_exited` when the UI process ends naturally, without a `pane_closed` event.
/// The envelope's `event` and `data.type` must agree; unknown fields are ignored.
fn take_dismissed_pane(event: &mut Value) -> Option<muxe_core::PaneId> {
    let event = event.as_object_mut()?;
    let kind = event.get("event")?.as_str()?;
    if !matches!(kind, "pane_closed" | "pane_exited") {
        return None;
    }
    let data = event.get("data")?.as_object()?;
    if data.get("type").and_then(Value::as_str) != Some(kind) {
        return None;
    }
    let data = event.get_mut("data")?.as_object_mut()?;
    match data.remove("pane_id")? {
        Value::String(pane) => Some(muxe_core::PaneId::new(pane)),
        _ => None,
    }
}

fn native_compatibility_snapshot(
    runtime: &Arc<HerdrRuntime>,
    continuity: HostContinuityEpoch,
    cache: &HerdrCache,
) -> NativeCompatibilitySnapshot {
    let schema = Arc::clone(runtime.schema());
    let fingerprint = HostSchemaFingerprint::parse(schema.canonical_request_sha256().to_owned())
        .expect("Herdr canonical schema fingerprint is a SHA-256 digest");
    NativeCompatibilitySnapshot::new(
        NativeCompatibilityIdentity::new(continuity, fingerprint),
        Arc::new(HerdrNativeCompatibilityValidator {
            schema,
            cache: cache.clone(),
        }),
    )
}

fn validate_native_batch_cached(
    schema: &Arc<ApiSchema>,
    cache: &HerdrCache,
    candidates: &[&NativeActionCandidate],
) -> Result<Vec<ActionValidation>, Vec<ConfigDiagnostic>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
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
            if let Err(error) = validate_candidate(schema, candidate) {
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
        runtime_schema_hash: schema.canonical_request_sha256().to_owned(),
        configured_requests_hash,
    };
    if let Some(outcomes) = cache.comparison_lookup(&key)
        && outcomes.len() == candidates.len()
    {
        let mut diagnostics = Vec::new();
        for (candidate, outcome) in candidates.iter().zip(&outcomes) {
            if !outcome {
                match validate_candidate(schema, candidate) {
                    Ok(_) => diagnostics.push(native_diagnostic(
                        candidate,
                        "Herdr compatibility cache disagrees with the immutable runtime schema",
                    )),
                    Err(error) => diagnostics.push(native_candidate_diagnostic(candidate, &error)),
                }
            }
        }
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
        match validate_candidate(schema, candidate) {
            Ok(_) => outcomes.push(true),
            Err(error) => {
                outcomes.push(false);
                diagnostics.push(native_candidate_diagnostic(candidate, &error));
            }
        }
    }
    if let Err(error) = cache.comparison_store(&key, &outcomes) {
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
fn reopen_pending_close_record(
    leases: &StdMutex<HashMap<PendingPaneLeaseId, PendingPaneLeaseRecord>>,
    lease: &PendingPaneLeaseId,
    record: &PendingPaneLeaseRecord,
) {
    let mut leases = leases
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

fn command_creation_methods(
    action: impl Into<PortableRequestKind>,
) -> Option<&'static [&'static str]> {
    match action.into() {
        PortableRequestKind::TabCreate { command: true } => Some(&["layout.apply"]),
        PortableRequestKind::PaneSplit {
            direction: Some(true),
            command: true,
        } => Some(&["session.snapshot", "layout.apply", "pane.move", "tab.close"]),
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
            "The active Herdr schema for {} must define params as an object",
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
                    "The active Herdr schema for {} must define required as an array",
                    description.method
                ));
            };
            let mut names = Vec::with_capacity(required.len());
            for entry in required {
                let Some(name) = entry.as_str() else {
                    return Err(format!(
                        "The active Herdr schema for {} must use strings in the required array",
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
            "portable action has an invalid {field_name:?} parameter: mode requires a zoom action"
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
                        "context value {:?} cannot be used as a key string",
                        reference.path.as_str(),
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
        "The active Herdr schema rejects the {method} parameter {field_name:?}: context value {:?} is incompatible with this parameter's declared type",
        reference.path.as_str(),
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
        PortableScalarKind::Index => {
            scalar_index_value(scalar).map(|index| Value::Number(index.into()))
        }
        PortableScalarKind::Number => scalar_number_value(scalar).and_then(number_to_json),
        PortableScalarKind::SplitDirection => scalar_split_direction_value(scalar)
            .map(|direction| Value::String(direction.wire().to_owned())),
        PortableScalarKind::PaneDirection => scalar_pane_direction_value(scalar)
            .map(|direction| Value::String(direction.as_str().to_owned())),
        PortableScalarKind::Keys => Err("keys must be a list, not a scalar".to_owned()),
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
    fn matches_host(&self, host: muxe_core::OriginHostKind) -> bool {
        host == muxe_core::OriginHostKind::Herdr
    }

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
    fn matches_host(&self, host: muxe_core::OriginHostKind) -> bool {
        host == muxe_core::OriginHostKind::Herdr
    }

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

    fn config_override_filename(&self) -> &'static str {
        crate::CONFIG_OVERRIDE_FILENAME
    }

    fn native_compatibility_snapshot(&self) -> Option<NativeCompatibilitySnapshot> {
        let incarnation = self
            .incarnation
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        incarnation.healthy.then(|| {
            native_compatibility_snapshot(&incarnation.runtime, incarnation.continuity, &self.cache)
        })
    }

    async fn capabilities(&self) -> Result<AdapterCapabilities, AdapterError> {
        Ok(AdapterCapabilities {
            // Compile-time validation caps, not the UI's PTY negotiation;
            // kitty_baseline is not a compiler gate. In pinned Herdr 0.8.2,
            // unmodified Enter/Tab/Backspace and function-key repeats retain
            // legacy press bytes and their releases are omitted. Ordinary
            // shifted text bypasses alternate-key CSI-u, no base-layout
            // identity is encoded, and navigation can use keypad CSI-u codes.
            // These are source findings, not a live enhanced-key pane proof.
            // Keep optional capabilities fail-closed and VT100 as the default.
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
                    "Herdr pane.get did not return a nonempty pane.workspace_id field",
                )
            })?;
        Ok(ModalScopeId::new(workspace))
    }

    async fn begin_capture(&self, _request: CaptureRequest) -> Result<CaptureLease, AdapterError> {
        Err(incompatible(
            "The Herdr socket API does not support capturing host input.",
        ))
    }

    async fn end_capture(
        &self,
        _lease: CaptureLease,
        _reason: CaptureReleaseReason,
    ) -> Result<(), AdapterError> {
        Err(incompatible(
            "The Herdr socket API does not support restoring host input after capture.",
        ))
    }

    async fn register_pending_pane(
        &self,
        registration: PendingPaneRegistration,
    ) -> Result<PendingPaneLease, AdapterError> {
        let authority = self.require_lifecycle()?;
        let pane = authority
            .invoke("pane.get", json!({ "pane_id": registration.pane.as_str() }))
            .await?;
        let object = crate::pane_info(&pane).ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr pane.get must return a pane_info result containing a pane object",
            )
        })?;
        let actual = object.get("pane_id").and_then(Value::as_str);
        if actual != Some(registration.pane.as_str()) {
            return Err(AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "Herdr pane.get did not return the pane ID being registered",
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
            "herdr:{}:{}:{}",
            authority.lease.epoch().get(),
            registration.ui_session,
            registration.pane
        ));
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .insert(
                id.clone(),
                PendingPaneLeaseRecord {
                    incarnation: authority.lease,
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
        let authority = self.require_lifecycle()?;
        authority
            .runtime
            .validate_method_set(&["pane.get", "pane.close"])?;
        let record = self.pending_close_record(&registration, &lease, &authority.lease)?;
        let pending_leases = Arc::clone(&self.pending_leases);
        let lease_id = lease.id.clone();
        authority
            .run_ordered(move |direct| async move {
                let pane = match direct
                    .invoke_response("pane.get", json!({ "pane_id": record.pane.as_str() }))
                    .await?
                {
                    HerdrResponse::Success(pane) => pane,
                    HerdrResponse::Error { code, .. } if pane_is_proven_absent(&code) => {
                        pending_leases
                            .lock()
                            .expect("Herdr pending lease registry is not poisoned")
                            .remove(&lease_id);
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
                        "Herdr pane.get must return a pane_info result containing a pane object",
                    )
                })?;
                if object.get("pane_id").and_then(Value::as_str) != Some(record.pane.as_str()) {
                    return Err(AdapterError::new(
                        AdapterErrorKind::ContextUnavailable,
                        "Herdr pane.get returned a different pane ID. The pending pane was not closed.",
                    ));
                }
                {
                    let mut leases = pending_leases
                        .lock()
                        .expect("Herdr pending lease registry is not poisoned");
                    let Some(current) = leases.get_mut(&lease_id) else {
                        return Err(pending_cleanup_lease_stale());
                    };
                    if current != &record {
                        return Err(pending_cleanup_outcome_unknown());
                    }
                    current.close_state = PendingPaneCloseState::CloseMayHaveApplied;
                }
                let response = direct
                    .invoke_response_with_delivery(
                        "pane.close",
                        json!({ "pane_id": record.pane.as_str() }),
                    )
                    .await;
                match response {
                    Ok(HerdrResponse::Success(_)) => {
                        pending_leases
                            .lock()
                            .expect("Herdr pending lease registry is not poisoned")
                            .remove(&lease_id);
                        Ok(())
                    }
                    Ok(HerdrResponse::Error { code, .. }) if pane_is_proven_absent(&code) => {
                        pending_leases
                            .lock()
                            .expect("Herdr pending lease registry is not poisoned")
                            .remove(&lease_id);
                        Ok(())
                    }
                    Ok(HerdrResponse::Error { code, message }) => {
                        reopen_pending_close_record(&pending_leases, &lease_id, &record);
                        Err(host_rejection("pane.close", &code, &message))
                    }
                    Err((error, delivery)) => {
                        if delivery == DeliveryState::NotSent {
                            reopen_pending_close_record(&pending_leases, &lease_id, &record);
                        }
                        Err(error)
                    }
                }
            })
            .await?
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
        let authority = self.require_lifecycle()?;
        let snapshot = authority
            .invoke_response("session.snapshot", Value::Object(serde_json::Map::new()))
            .await?;
        crate::origin::capture_origin_from_snapshot(
            &request,
            muxe_core::ServerId::new(origin_epoch_token(
                authority.runtime.identity(),
                authority.lease.epoch(),
            )),
            snapshot,
        )
        .map_err(|error| *error)
    }

    async fn dispatch_portable(
        &self,
        request: PortableDispatchRequest,
    ) -> Result<DispatchAccepted, AdapterError> {
        let authority = self.require_current_origin(&request.origin)?;
        if let ResolvedPortableAction::Tab(ResolvedTabAction::Swap(ResolvedTabTarget::Index(
            index,
        ))) = &request.action
        {
            return self.dispatch_tab_swap(request.execution, &request.origin, *index);
        }
        if creation_requires_post_dismissal(&request.action) {
            return Err(AdapterError::new(
                AdapterErrorKind::InvalidRequest,
                "Actions that create and focus a pane or tab must run after the Muxe UI closes.",
            ));
        }
        if creation_has_program(&request.action) {
            return self.dispatch_command_creation(
                request.execution,
                &request.action,
                &request.origin,
            );
        }
        let invocation = portable_invocation(&request.action, &request.origin)?;
        self.dispatch(authority, request.execution, invocation)
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
        let authority = self.require_current_origin(&request.origin)?;
        let metadata = validate_candidate(authority.runtime.schema(), &request.action.candidate)
            .map_err(|error| {
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
            authority,
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
            "The Herdr socket API cannot cancel single-response requests.",
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
                    "The Herdr adapter has finished all actions it accepted.",
                ));
            }
            let mut events = self.events_rx.lock().await;
            tokio::select! {
                biased;
                terminal = terminals.recv() => {
                    let terminal = terminal.ok_or_else(|| {
                        AdapterError::new(
                            AdapterErrorKind::Shutdown,
                            "The Herdr adapter's action-result channel closed.",
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
        let (generation, requested) = self.begin_or_join_suspend()?;
        if requested {
            self.pending_leases
                .lock()
                .expect("Herdr pending lease registry is not poisoned")
                .clear();
            self.suspend_wake.notify_one();
        }
        self.wait_for_suspend(generation).await
    }

    #[expect(
        clippy::too_many_lines,
        reason = "activation resume keeps schema, subscription, runtime, compatibility snapshot, and health publication in one auditable transaction"
    )]
    async fn resume_after_activation_abort(&self) -> Result<(), AdapterError> {
        let _resume_guard = self.admit_resume()?;
        let (prior_epoch, prior_continuity, prior_lease) = {
            let incarnation = self
                .incarnation
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                incarnation.epoch,
                incarnation.continuity,
                incarnation.runtime.lease(incarnation.epoch),
            )
        };
        let released = self
            .suspend
            .lock()
            .expect("Herdr suspend coordinator is not poisoned")
            .current
            .as_ref()
            .is_some_and(|attempt| {
                attempt.lease == prior_lease && matches!(attempt.phase, SuspendPhase::Released)
            });
        if !released {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "The Herdr adapter cannot resume until its current event subscription has been fully suspended.",
            ));
        }
        let fresh_epoch = prior_epoch.next();
        let fresh_continuity = prior_continuity
            .successor()
            .expect("Herdr continuity epoch exhausted");
        // Fresh schema validation, live identity, and the successful guarded
        // subscription are assembled before any part becomes current. Both
        // futures are adapter-owned and dropped immediately when shutdown wins.
        let refreshed = Arc::new(
            self.shutdown_cancellable(HerdrRuntime::connect(self.config.clone()))
                .await??,
        );
        let lease = refreshed.lease(fresh_epoch);
        let (subscription, _) = self
            .shutdown_cancellable(EventSubscription::connect_expected(
                &refreshed,
                lease,
                subscription_config(),
            ))
            .await?
            .map_err(|error| socket_error(&error))?;
        self.shutdown_cancellable(wait_on_hook(&self.resume_install_wait_hook))
            .await?;
        let permit = self.events_tx.try_reserve().map_err(|error| {
            let kind = match error {
                mpsc::error::TrySendError::Full(()) => AdapterErrorKind::Unavailable,
                mpsc::error::TrySendError::Closed(()) => AdapterErrorKind::Shutdown,
            };
            AdapterError::new(
                kind,
                "Muxe could not resume the Herdr adapter because it could not queue the adapter-health update.",
            )
        })?;
        let mut slot = self.shutdown_cancellable(self.resume_slot.lock()).await?;
        {
            let mut incarnation = self
                .incarnation
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut suspend = self
                .suspend
                .lock()
                .expect("Herdr suspend coordinator is not poisoned");
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "The Herdr adapter shut down while resuming after activation was aborted. The new event subscription was not installed.",
                ));
            }
            let released = suspend.current.as_ref().is_some_and(|attempt| {
                attempt.lease == prior_lease && matches!(attempt.phase, SuspendPhase::Released)
            });
            if !self.suspended.load(Ordering::SeqCst)
                || incarnation.epoch != prior_epoch
                || incarnation.healthy
                || !released
            {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "The Herdr connection state changed while resuming after activation was aborted. The new event subscription was not installed.",
                ));
            }
            let previous = incarnation.runtime.identity().clone();
            let current = refreshed.identity().clone();
            let compatibility =
                native_compatibility_snapshot(&refreshed, fresh_continuity, &self.cache);
            *slot = Some(subscription);
            incarnation.runtime = refreshed;
            incarnation.epoch = fresh_epoch;
            incarnation.continuity = fresh_continuity;
            incarnation.healthy = true;
            suspend.current = None;
            suspend.terminal_loss = None;
            self.suspended.store(false, Ordering::SeqCst);
            permit.send(AdapterHealthEvent::Reconnected {
                previous,
                current,
                compatibility,
            });
            self.suspend_changed.notify_waiters();
        }
        drop(slot);
        self.resume_wake.notify_one();
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), AdapterError> {
        // Lifecycle flags and incarnation health change under one write
        // authority. An install linearized before this point is invalidated;
        // one arriving after it must observe shutdown and reject publication.
        let runtime = {
            let mut incarnation = self
                .incarnation
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.shutdown.store(true, Ordering::Relaxed);
            incarnation.healthy = false;
            incarnation.runtime.retire_send_queue();
            Arc::clone(&incarnation.runtime)
        };
        self.complete_suspend_shutdown();
        self.resume_wake.notify_waiters();
        self.close_resumes_and_wait().await;
        let tasks = {
            let mut registry = self
                .dispatch_tasks
                .lock()
                .expect("retained Herdr dispatch registry is not poisoned");
            registry.closed = true;
            registry.joining = registry.joining.saturating_add(registry.tasks.len());
            std::mem::take(&mut registry.tasks)
        };
        self.pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .clear();
        self.fail_post_dismissals("The Herdr adapter shut down before the Muxe UI closed. The queued action was not sent.")
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
        let queue_result = runtime.shutdown_send_queue().await;
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
        queue_result
    }
}

async fn send_health_or_shutdown(adapter: &Arc<HerdrAdapter>, event: AdapterHealthEvent) -> bool {
    let mut event = Some(event);
    loop {
        if adapter.shutdown.load(Ordering::Relaxed) {
            return false;
        }
        let reserve = adapter.events_tx.reserve();
        tokio::pin!(reserve);
        tokio::select! {
            permit = &mut reserve => {
                let Ok(permit) = permit else {
                    return false;
                };
                let _incarnation = adapter
                    .incarnation
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if adapter.shutdown.load(Ordering::Relaxed) {
                    return false;
                }
                permit.send(event.take().expect("health event is published once"));
                return true;
            }
            () = adapter.suspend_wake.notified() => {
                if adapter.shutdown.load(Ordering::Relaxed) {
                    return false;
                }
            }
        }
    }
}

async fn send_host_lost_or_shutdown(
    adapter: &Arc<HerdrAdapter>,
    event: AdapterHealthEvent,
) -> HostLostSendOutcome {
    let pending_hook = adapter
        .host_lost_reserve_pending_hook
        .lock()
        .expect("Herdr HostLost reserve-pending hook is not poisoned")
        .clone();
    let mut event = Some(event);
    loop {
        if adapter.shutdown.load(Ordering::Relaxed) {
            return HostLostSendOutcome::Shutdown;
        }
        let reserve = adapter.events_tx.reserve();
        tokio::pin!(reserve);
        let mut pending_reported = false;
        let observed_reserve = poll_fn(|context| {
            let result = reserve.as_mut().poll(context);
            if result.is_pending() && !pending_reported {
                pending_reported = true;
                if let Some(hook) = &pending_hook {
                    hook.entered.notify_one();
                }
            }
            result
        });
        tokio::pin!(observed_reserve);
        tokio::select! {
            permit = &mut observed_reserve => {
                let Ok(permit) = permit else {
                    return HostLostSendOutcome::Shutdown;
                };
                let _incarnation = adapter
                    .incarnation
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if adapter.shutdown.load(Ordering::Relaxed) {
                    return HostLostSendOutcome::Shutdown;
                }
                permit.send(event.take().expect("HostLost event is published once"));
                return HostLostSendOutcome::Published;
            }
            () = adapter.suspend_wake.notified() => {
                if adapter.shutdown.load(Ordering::Relaxed) {
                    return HostLostSendOutcome::Shutdown;
                }
            }
        }
    }
}
fn subscription_config() -> SubscriptionConfig {
    SubscriptionConfig {
        params: json!({
            "subscriptions": [
                { "type": "tab.focused" },
                { "type": "pane.closed" },
                { "type": "pane.exited" },
            ],
        }),
        subscribe_timeout: SUBSCRIBE_TIMEOUT,
    }
}

fn origin_epoch_token(identity: &HostIdentity, epoch: IncarnationEpoch) -> String {
    format!("{}#continuity-{}", identity.live_server_id, epoch.get())
}

fn origin_is_current(
    origin: &muxe_core::OriginContext,
    identity: &HostIdentity,
    epoch: IncarnationEpoch,
) -> bool {
    origin.server_id.as_str() == origin_epoch_token(identity, epoch)
}

async fn park_suspended_monitor(
    adapter: &Arc<HerdrAdapter>,
    subscription: &mut Option<EventSubscription>,
) -> bool {
    let lease = subscription.as_ref().map(EventSubscription::lease);
    let Some(generation) = adapter.requested_suspend_for(lease) else {
        return true;
    };
    wait_on_hook(&adapter.suspend_release_wait_hook).await;
    // The exact monitor-owned stream is gone before the generation can be
    // released. A reconnect path may already have dropped it; that is the
    // same proof for the lease captured by the suspend attempt.
    drop(subscription.take());
    if let Err(error) = adapter.runtime().shutdown_send_queue().await {
        adapter.complete_suspend(generation, SuspendPhase::HostLost(error));
        return false;
    }
    let published = send_health_or_shutdown(
        adapter,
        AdapterHealthEvent::Unhealthy {
            modal_scope: None,
            error: AdapterError::new(
                AdapterErrorKind::Unavailable,
                "Herdr event subscription is suspended for activation",
            ),
        },
    )
    .await;
    if !published {
        adapter.complete_suspend(generation, SuspendPhase::Shutdown);
        return false;
    }
    adapter.complete_suspend(generation, SuspendPhase::Released);
    let Some(next) = take_resumed_subscription(adapter).await else {
        adapter.complete_suspend_shutdown();
        return false;
    };
    *subscription = Some(next);
    true
}

async fn monitor_subscription(
    adapter: Arc<HerdrAdapter>,
    subscription: EventSubscription,
    mut continuity_loss_rx: mpsc::UnboundedReceiver<ContinuityLoss>,
) {
    let mut subscription = Some(subscription);
    loop {
        if adapter.shutdown.load(Ordering::Relaxed) {
            adapter.complete_suspend_shutdown();
            return;
        }
        if adapter.suspended.load(Ordering::SeqCst) {
            if !park_suspended_monitor(&adapter, &mut subscription).await {
                return;
            }
            continue;
        }
        let lease = subscription
            .as_ref()
            .expect("active Herdr monitor owns a subscription")
            .lease()
            .clone();
        let loss = tokio::select! {
            biased;
            report = continuity_loss_rx.recv() => report,
            result = subscription
                .as_mut()
                .expect("active Herdr monitor owns a subscription")
                .next_event() => match result {
                    Ok(SubscriptionEvent::Event(event)) => {
                        adapter.observe_subscription_event(event).await;
                        None
                    }
                    Err(error) => {
                        let _ = adapter.continuity_loss_tx.send(ContinuityLoss {
                            lease,
                            error: socket_error(&error),
                        });
                        None
                    }
                },
            () = adapter.suspend_wake.notified() => None,
        };
        let Some(loss) = loss else {
            continue;
        };
        if !adapter.transition_to_lost(&loss.lease) {
            continue;
        }
        let lost_lease = loss.lease.clone();
        drop(subscription.take());
        let queue_error = adapter.runtime().shutdown_send_queue().await.err();
        adapter
            .pending_leases
            .lock()
            .expect("Herdr pending lease registry is not poisoned")
            .clear();
        adapter
            .fail_post_dismissals("Muxe can no longer verify its Herdr event subscription. The queued action was not sent.")
            .await;
        if !send_health_or_shutdown(
            &adapter,
            AdapterHealthEvent::Unhealthy {
                modal_scope: None,
                error: queue_error.unwrap_or(loss.error),
            },
        )
        .await
        {
            adapter.complete_suspend_shutdown();
            return;
        }

        match reconnect_subscription(&adapter, &mut subscription).await {
            ReconnectOutcome::Reconnected | ReconnectOutcome::SuspendRequested => {}
            ReconnectOutcome::Stop => {
                adapter.complete_suspend_shutdown();
                return;
            }
            ReconnectOutcome::HostLost(error) => {
                adapter.record_terminal_loss(&lost_lease, &error);
                if matches!(
                    send_host_lost_or_shutdown(
                        &adapter,
                        AdapterHealthEvent::HostLost {
                            identity: adapter.identity(),
                            error,
                        },
                    )
                    .await,
                    HostLostSendOutcome::Shutdown
                ) {
                    adapter.complete_suspend_shutdown();
                }
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
    subscription: &mut Option<EventSubscription>,
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
        // A suspend wake re-checks the flag at the loop head. Dropping the
        // attempt future kills only the owned schema child via `kill_on_drop`.
        let Some(attempt) = attempt else {
            continue;
        };
        let Ok(attempt) = attempt else {
            last_error = Some(AdapterError::new(
                AdapterErrorKind::Unavailable,
                "The Herdr reconnect attempt timed out before the host recovery deadline.",
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
        let lease = next_subscription.lease().clone();
        *subscription = Some(next_subscription);
        match adapter.install_reconnected(refreshed, &lease).await {
            IncarnationInstallOutcome::Installed => return ReconnectOutcome::Reconnected,
            IncarnationInstallOutcome::Stop => return ReconnectOutcome::Stop,
            IncarnationInstallOutcome::LifecycleChanged => {
                if adapter.shutdown.load(Ordering::Relaxed) {
                    return ReconnectOutcome::Stop;
                }
                if adapter.suspended.load(Ordering::SeqCst) {
                    return ReconnectOutcome::SuspendRequested;
                }
            }
            IncarnationInstallOutcome::Retry => {}
        }
        drop(subscription.take());
        last_error = Some(AdapterError::new(
            AdapterErrorKind::Unavailable,
            "The Herdr connection state changed before the replacement connection could be installed.",
        ));
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default();
        if remaining.is_zero() {
            break;
        }
        if !interruptible_sleep(adapter, RECONNECT_RETRY.min(remaining)).await {
            return ReconnectOutcome::Stop;
        }
    }

    ReconnectOutcome::HostLost(last_error.unwrap_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr did not reconnect before the host recovery deadline.",
        )
    }))
}

async fn reconnect_attempt(
    adapter: &Arc<HerdrAdapter>,
) -> Result<(Arc<HerdrRuntime>, EventSubscription), AdapterError> {
    let epoch = adapter.reconnect_epoch().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "The Herdr adapter cannot reconnect because its current connection has not been marked as lost.",
        )
    })?;
    let refreshed = Arc::new(HerdrRuntime::connect(adapter.config.clone()).await?);
    let lease = refreshed.lease(epoch);
    let (next_subscription, _) =
        EventSubscription::connect_expected(&refreshed, lease, subscription_config())
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
                "Herdr session.snapshot did not return a snapshot.panes array. Muxe cannot confirm that its UI pane has closed.",
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
    pending: &mut HashMap<muxe_core::PaneId, Vec<PostDismissalPortableDispatchRequest>>,
    pane: &muxe_core::PaneId,
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
    action: &ResolvedPortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<Invocation, AdapterError> {
    let description = portable_request_description(action)
        .map_err(incompatible)?
        .ok_or_else(|| {
            // Broker-owned forms (menu/config/command), command-bearing creations, and tab:swap
            // never reach the single-request builder: dispatch routes them to their own paths.
            // The description already reported every unsupported form as `Err`, so reaching
            // here with `None` means dispatch misrouted a multi-request action.
            incompatible("Muxe cannot run this portable action through the Herdr socket API.")
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

/// Builds the dispatch-time invocation from the shared description and typed IR.
/// The description excludes command-bearing creations and unsupported directions.
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn build_portable_invocation(
    description: &PortableRequestDescription,
    action: &ResolvedPortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<Invocation, AdapterError> {
    let pane = origin_pane(origin)?;
    match action {
        ResolvedPortableAction::Keyboard(ResolvedKeyboardAction::SendKeys(keys)) => {
            Ok(Invocation {
                method: description.method,
                params: json!({ "pane_id": pane, "keys": keys.iter().map(muxe_core::CanonicalKey::canonical_string).collect::<Vec<_>>() }),
            })
        }
        ResolvedPortableAction::Keyboard(ResolvedKeyboardAction::SendText(text)) => {
            Ok(Invocation {
                method: description.method,
                params: json!({ "pane_id": pane, "text": text }),
            })
        }
        ResolvedPortableAction::Tab(ResolvedTabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        }) => Ok(Invocation {
            method: description.method,
            params: json!({
                "workspace_id": workspace_id.as_ref().map(muxe_core::WorkspaceId::as_str),
                "label": name,
                "focus": focus.unwrap_or(true),
                "cwd": command.cwd.as_ref().map(|cwd| json_path(cwd.as_path(), "tab.cwd")).transpose()?,
            }),
        }),
        ResolvedPortableAction::Tab(ResolvedTabAction::Close) => Ok(Invocation {
            method: description.method,
            params: json!({ "tab_id": origin_tab(origin)? }),
        }),
        ResolvedPortableAction::Tab(ResolvedTabAction::Rename { name: Some(label) }) => {
            Ok(Invocation {
                method: description.method,
                params: json!({ "tab_id": origin_tab(origin)?, "label": label }),
            })
        }
        ResolvedPortableAction::Tab(ResolvedTabAction::Move(ResolvedTabTarget::Index(index))) => {
            Ok(Invocation {
                method: description.method,
                params: json!({ "tab_id": origin_tab(origin)?, "insert_index": index.get() }),
            })
        }
        ResolvedPortableAction::Pane(ResolvedPaneAction::Create) => Err(incompatible(
            "Herdr has no pane.create method; pane.split requires an explicit direction",
        )),
        ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
            direction: Some(direction),
            focus,
            command,
        }) => Ok(Invocation {
            method: description.method,
            params: json!({
                "target_pane_id": pane,
                "direction": split_direction(*direction).map_err(direction_request_error)?.wire(),
                "focus": focus.unwrap_or(true),
                "cwd": command.cwd.as_ref().map(|cwd| json_path(cwd.as_path(), "pane.cwd")).transpose()?,
            }),
        }),
        ResolvedPortableAction::Pane(ResolvedPaneAction::Close) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane }),
        }),
        ResolvedPortableAction::Pane(
            ResolvedPaneAction::Focus(ResolvedPaneTarget::Direction(direction))
            | ResolvedPaneAction::Swap(ResolvedPaneTarget::Direction(direction)),
        ) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "direction": HerdrPaneDirection::try_from(*direction).map_err(direction_request_error)?.as_str() }),
        }),
        ResolvedPortableAction::Pane(ResolvedPaneAction::Resize { direction, amount }) => {
            Ok(Invocation {
                method: description.method,
                params: json!({ "pane_id": pane, "direction": HerdrPaneDirection::try_from(*direction).map_err(direction_request_error)?.as_str(), "amount": amount.as_ref().map(resize_number).transpose()? }),
            })
        }
        ResolvedPortableAction::Pane(ResolvedPaneAction::Zoom { enabled }) => Ok(Invocation {
            method: description.method,
            params: json!({ "pane_id": pane, "mode": enabled.map_or("toggle", |value| if value { "on" } else { "off" }) }),
        }),
        _ => Err(AdapterError::new(
            AdapterErrorKind::Incompatible,
            "Muxe cannot run this portable action through the Herdr socket API.",
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
    /// Fields the action always emits. `Origin` marks captured host identity fields;
    /// config-backed fields are checked against the schema before origin resolution.
    fields: &'static [PortableRequestField],
}

/// One emitted parameter field and the typed origin of its value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PortableRequestField {
    name: &'static str,
    origin: PortableValueOrigin,
}

/// Where one emitted field's value comes from. Config literals face the adapter's
/// conversion and the schema domain. Deferred markers face their declared type;
/// execution values arrive through the resolved IR, never through config scalars.
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

/// The adapter conversion used to probe a configured literal's wire value.
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
/// Shape-only projection shared by config validation and the resolved execution IR.
/// It never reconstructs config values; values stay owned by their source domain.
enum PortableRequestKind {
    BrokerOwned,
    SendKeys,
    SendText,
    TabCreate {
        command: bool,
    },
    TabClose,
    TabRename {
        named: bool,
    },
    TabFocus,
    TabMove {
        index: bool,
    },
    TabSwap {
        index: bool,
    },
    PaneCreate,
    PaneSplit {
        direction: Option<bool>,
        command: bool,
    },
    PaneClose,
    PaneFocus {
        cardinal: bool,
    },
    PaneMove,
    PaneSwap {
        cardinal: bool,
    },
    PaneResize,
    PaneZoom,
    PaneFullscreen,
    PaneFloating,
    PaneFrame,
    Session,
}

impl From<&PortableAction> for PortableRequestKind {
    fn from(action: &PortableAction) -> Self {
        match action {
            PortableAction::Menu(_) | PortableAction::Config(_) | PortableAction::Command(_) => {
                Self::BrokerOwned
            }
            PortableAction::Keyboard(muxe_core::KeyboardAction::SendKeys(_)) => Self::SendKeys,
            PortableAction::Keyboard(muxe_core::KeyboardAction::SendText(_)) => Self::SendText,
            PortableAction::Tab(TabAction::Create { command, .. }) => Self::TabCreate {
                command: command.program.is_some(),
            },
            PortableAction::Tab(TabAction::Close) => Self::TabClose,
            PortableAction::Tab(TabAction::Rename { name }) => Self::TabRename {
                named: name.is_some(),
            },
            PortableAction::Tab(TabAction::Focus(_)) => Self::TabFocus,
            PortableAction::Tab(TabAction::Move(target)) => Self::TabMove {
                index: target_is_index(target),
            },
            PortableAction::Tab(TabAction::Swap(target)) => Self::TabSwap {
                index: target_is_index(target),
            },
            PortableAction::Pane(PaneAction::Create) => Self::PaneCreate,
            PortableAction::Pane(PaneAction::Split {
                direction, command, ..
            }) => Self::PaneSplit {
                direction: direction.as_ref().map(split_direction_is_supported),
                command: command.program.is_some(),
            },
            PortableAction::Pane(PaneAction::Close) => Self::PaneClose,
            PortableAction::Pane(PaneAction::Focus(target)) => Self::PaneFocus {
                cardinal: target_is_cardinal_direction(target),
            },
            PortableAction::Pane(PaneAction::Move(_)) => Self::PaneMove,
            PortableAction::Pane(PaneAction::Swap(target)) => Self::PaneSwap {
                cardinal: target_is_cardinal_direction(target),
            },
            PortableAction::Pane(PaneAction::Resize { .. }) => Self::PaneResize,
            PortableAction::Pane(PaneAction::Zoom { .. }) => Self::PaneZoom,
            PortableAction::Pane(PaneAction::Fullscreen { .. }) => Self::PaneFullscreen,
            PortableAction::Pane(PaneAction::Floating { .. }) => Self::PaneFloating,
            PortableAction::Pane(PaneAction::Frame { .. }) => Self::PaneFrame,
            PortableAction::Session(_) => Self::Session,
        }
    }
}

impl From<&ResolvedPortableAction> for PortableRequestKind {
    fn from(action: &ResolvedPortableAction) -> Self {
        let cardinal = |target: &ResolvedPaneTarget| {
            matches!(target,
            ResolvedPaneTarget::Direction(direction) if HerdrPaneDirection::try_from(*direction).is_ok())
        };
        match action {
            ResolvedPortableAction::Menu(_)
            | ResolvedPortableAction::Config(_)
            | ResolvedPortableAction::Command(_) => Self::BrokerOwned,
            ResolvedPortableAction::Keyboard(ResolvedKeyboardAction::SendKeys(_)) => Self::SendKeys,
            ResolvedPortableAction::Keyboard(ResolvedKeyboardAction::SendText(_)) => Self::SendText,
            ResolvedPortableAction::Tab(ResolvedTabAction::Create { command, .. }) => {
                Self::TabCreate {
                    command: command.program.is_some(),
                }
            }
            ResolvedPortableAction::Tab(ResolvedTabAction::Close) => Self::TabClose,
            ResolvedPortableAction::Tab(ResolvedTabAction::Rename { name }) => Self::TabRename {
                named: name.is_some(),
            },
            ResolvedPortableAction::Tab(ResolvedTabAction::Focus(_)) => Self::TabFocus,
            ResolvedPortableAction::Tab(ResolvedTabAction::Move(target)) => Self::TabMove {
                index: matches!(target, ResolvedTabTarget::Index(_)),
            },
            ResolvedPortableAction::Tab(ResolvedTabAction::Swap(target)) => Self::TabSwap {
                index: matches!(target, ResolvedTabTarget::Index(_)),
            },
            ResolvedPortableAction::Pane(ResolvedPaneAction::Create) => Self::PaneCreate,
            ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
                direction, command, ..
            }) => Self::PaneSplit {
                direction: direction.map(|direction| split_direction(direction).is_ok()),
                command: command.program.is_some(),
            },
            ResolvedPortableAction::Pane(ResolvedPaneAction::Close) => Self::PaneClose,
            ResolvedPortableAction::Pane(ResolvedPaneAction::Focus(target)) => Self::PaneFocus {
                cardinal: cardinal(target),
            },
            ResolvedPortableAction::Pane(ResolvedPaneAction::Move(_)) => Self::PaneMove,
            ResolvedPortableAction::Pane(ResolvedPaneAction::Swap(target)) => Self::PaneSwap {
                cardinal: cardinal(target),
            },
            ResolvedPortableAction::Pane(ResolvedPaneAction::Resize { .. }) => Self::PaneResize,
            ResolvedPortableAction::Pane(ResolvedPaneAction::Zoom { .. }) => Self::PaneZoom,
            ResolvedPortableAction::Pane(ResolvedPaneAction::Fullscreen { .. }) => {
                Self::PaneFullscreen
            }
            ResolvedPortableAction::Pane(ResolvedPaneAction::Floating { .. }) => Self::PaneFloating,
            ResolvedPortableAction::Pane(ResolvedPaneAction::Frame { .. }) => Self::PaneFrame,
            ResolvedPortableAction::Session(_) => Self::Session,
        }
    }
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
    action: impl Into<PortableRequestKind>,
) -> Result<Option<PortableRequestDescription>, String> {
    use PortableScalarKind as Scalar;
    use PortableValueOrigin as Origin;
    let description = match action.into() {
        PortableRequestKind::BrokerOwned | PortableRequestKind::TabSwap { index: true } => return Ok(None),
        PortableRequestKind::SendKeys => PortableRequestDescription {
            method: "pane.send_keys",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "keys", origin: Origin::Literal(Scalar::Keys) },
            ],
        },
        PortableRequestKind::SendText => PortableRequestDescription {
            method: "pane.send_text",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "text", origin: Origin::Literal(Scalar::String) },
            ],
        },
        PortableRequestKind::TabCreate { command } => {
            if command {
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
        PortableRequestKind::TabClose => PortableRequestDescription {
            method: "tab.close",
            fields: &[PortableRequestField { name: "tab_id", origin: Origin::Origin(ContextType::TabId) }],
        },
        PortableRequestKind::TabRename { named: true } => PortableRequestDescription {
            method: "tab.rename",
            fields: &[
                PortableRequestField { name: "tab_id", origin: Origin::Origin(ContextType::TabId) },
                PortableRequestField { name: "label", origin: Origin::Literal(Scalar::String) },
            ],
        },
        PortableRequestKind::TabRename { named: false } => {
            return Err(
                "Herdr tab.rename requires label. Muxe does not support opening a Herdr rename prompt for bare tab:rename."
                    .to_owned(),
            );
        }
        PortableRequestKind::TabMove { index: true } => PortableRequestDescription {
            method: "tab.move",
            fields: &[
                PortableRequestField { name: "tab_id", origin: Origin::Origin(ContextType::TabId) },
                PortableRequestField { name: "insert_index", origin: Origin::Literal(Scalar::Index) },
            ],
        },
        PortableRequestKind::PaneCreate => {
            return Err(
                "Herdr has no pane.create method; pane.split requires an explicit right or down direction"
                    .to_owned(),
            );
        }
        PortableRequestKind::PaneSplit { direction: None, .. } => {
            return Err("Herdr pane.split requires an explicit right or down direction".to_owned());
        }
        PortableRequestKind::PaneSplit { direction: Some(true), command: false } => {
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
        PortableRequestKind::PaneSplit { direction: Some(false), .. } => {
            return Err("Herdr pane.split supports only right or down directions".to_owned());
        }
        PortableRequestKind::PaneSplit { command: true, .. } => {
            return Err(
                "A pane:split action with a command must run after the Muxe UI closes."
                    .to_owned(),
            );
        }
        PortableRequestKind::PaneClose => PortableRequestDescription {
            method: "pane.close",
            fields: &[PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) }],
        },
        PortableRequestKind::PaneFocus { cardinal: true } => PortableRequestDescription {
            method: "pane.focus_direction",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::PaneDirection) },
            ],
        },
        PortableRequestKind::PaneSwap { cardinal: true } => PortableRequestDescription {
            method: "pane.swap",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::PaneDirection) },
            ],
        },
        PortableRequestKind::PaneResize => PortableRequestDescription {
            method: "pane.resize",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "direction", origin: Origin::Literal(Scalar::PaneDirection) },
                PortableRequestField { name: "amount", origin: Origin::Literal(Scalar::Number) },
            ],
        },
        PortableRequestKind::PaneZoom => PortableRequestDescription {
            method: "pane.zoom",
            fields: &[
                PortableRequestField { name: "pane_id", origin: Origin::Origin(ContextType::PaneId) },
                PortableRequestField { name: "mode", origin: Origin::Default(Scalar::String) },
            ],
        },
        PortableRequestKind::TabFocus => return Err("Herdr tab.focus requires a tab ID. Muxe does not support focusing Herdr tabs by index or direction.".to_owned()),
        PortableRequestKind::TabMove { index: false } => return Err("Herdr tab.move supports only a concrete insert index".to_owned()),
        PortableRequestKind::TabSwap { index: false } => return Err("Muxe supports Herdr tab:swap only by index, not by direction.".to_owned()),
        PortableRequestKind::PaneFocus { cardinal: false } => return Err("Herdr pane.focus supports only left, right, up, or down.".to_owned()),
        PortableRequestKind::PaneMove => return Err("Herdr pane.move requires an explicit tab/new-tab destination, not a portable index or direction".to_owned()),
        PortableRequestKind::PaneSwap { cardinal: false } => return Err("Herdr pane.swap supports only left, right, up, or down.".to_owned()),
        PortableRequestKind::PaneFullscreen => return Err("the Herdr socket API exposes no pane fullscreen method".to_owned()),
        PortableRequestKind::PaneFloating => return Err("the Herdr socket API exposes no pane floating method".to_owned()),
        PortableRequestKind::PaneFrame => return Err("the Herdr socket API exposes no pane frame method".to_owned()),
        PortableRequestKind::Session => return Err("the Herdr socket API exposes only read-only session.snapshot; it has no portable session lifecycle methods".to_owned()),
    };
    Ok(Some(description))
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct PublicTabNumber(u64);

impl PublicTabNumber {
    const fn from_response(number: u64) -> Self {
        Self(number)
    }

    const fn from_selector(selector: muxe_core::TabIndex) -> Self {
        Self(selector.get())
    }

    const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct TabPosition(usize);

impl TabPosition {
    const fn from_list_order(position: usize) -> Self {
        Self(position)
    }

    const fn get(self) -> usize {
        self.0
    }

    const fn insert_after(self) -> usize {
        self.0 + 1
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderedTab {
    id: muxe_core::TabId,
    workspace: muxe_core::WorkspaceId,
    public_number: PublicTabNumber,
    position: TabPosition,
}
fn creation_has_program(action: &ResolvedPortableAction) -> bool {
    match action {
        ResolvedPortableAction::Tab(ResolvedTabAction::Create { command, .. })
        | ResolvedPortableAction::Pane(ResolvedPaneAction::Split { command, .. }) => {
            command.program.is_some()
        }
        _ => false,
    }
}

fn creation_requires_post_dismissal(action: &ResolvedPortableAction) -> bool {
    match action {
        ResolvedPortableAction::Tab(ResolvedTabAction::Create { focus, .. })
        | ResolvedPortableAction::Pane(ResolvedPaneAction::Split { focus, .. }) => {
            focus.unwrap_or(true)
        }
        _ => false,
    }
}

async fn perform_command_creation(
    authority: &IncarnationTransactionAuthority,
    action: &ResolvedPortableAction,
    origin: &muxe_core::OriginContext,
) -> Result<(), AdapterError> {
    match action {
        ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
            direction: None, ..
        }) => Err(incompatible(
            "Herdr pane.split requires an explicit right or down direction",
        )),
        ResolvedPortableAction::Tab(ResolvedTabAction::Create {
            workspace_id,
            name,
            focus,
            command,
        }) if command.program.is_some() => {
            let workspace = workspace_id
                .clone()
                .or_else(|| origin.workspace_id.clone())
                .ok_or_else(|| {
                    AdapterError::new(
                        AdapterErrorKind::ContextUnavailable,
                        "Herdr command tab creation requires the captured origin workspace",
                    )
                })?;
            let label = name.clone();
            crate::launch::open_command_tab_with(
                authority,
                crate::CommandTabLaunch {
                    workspace,
                    label,
                    cwd: creation_cwd(command, origin, "tab.cwd")?,
                    argv: creation_argv(command, "tab.program", "tab.args")?,
                    focus: focus.unwrap_or(true),
                },
            )
            .await
        }
        ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
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
            let destination =
                crate::launch::pane_by_identity_with(authority, workspace, tab, pane).await?;
            let direction = split_direction(*direction).map_err(direction_request_error)?;
            crate::launch::open_command_pane_with(
                authority,
                crate::CommandPaneLaunch {
                    origin: destination.clone(),
                    destination,
                    cwd: creation_cwd(command, origin, "pane.cwd")?,
                    argv: creation_argv(command, "pane.program", "pane.args")?,
                    direction,
                    ratio: 0.5,
                    focus: focus.unwrap_or(true),
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
fn creation_argv(
    command: &ResolvedCreateCommand,
    program_field: &str,
    args_field: &str,
) -> Result<Vec<String>, AdapterError> {
    let program = command
        .program
        .as_ref()
        .ok_or_else(|| incompatible("creation command requires program"))?;
    let program = json_word(program, program_field)?;
    if program.is_empty() {
        return Err(incompatible("creation command program must not be empty"));
    }
    let mut argv = Vec::with_capacity(command.args.len().saturating_add(1));
    argv.push(program.to_owned());
    for argument in &command.args {
        argv.push(json_word(argument, args_field)?.to_owned());
    }
    Ok(argv)
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn creation_cwd(
    command: &ResolvedCreateCommand,
    origin: &muxe_core::OriginContext,
    field: &str,
) -> Result<PathBuf, AdapterError> {
    let cwd = command
        .cwd
        .as_ref()
        .map(|cwd| cwd.as_path().to_owned())
        .or_else(|| origin.pane_cwd.clone())
        .ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::ContextUnavailable,
                "The creation command needs an explicit working directory or one captured from the origin pane.",
            )
        })?;
    if !cwd.is_absolute() {
        return Err(incompatible(
            "The creation command's working directory must be an absolute path.",
        ));
    }
    json_path(&cwd, field)?;
    Ok(cwd)
}

#[derive(Debug)]
enum TabSwapError {
    Known(String),
    Unknown(String),
}

async fn perform_tab_swap(
    authority: &IncarnationTransactionAuthority,
    workspace: &muxe_core::WorkspaceId,
    source_tab: &muxe_core::TabId,
    target_number: PublicTabNumber,
) -> Result<(), TabSwapError> {
    let initial = tab_list(
        &request_tab_swap(
            authority,
            "tab.list",
            json!({ "workspace_id": workspace.as_str() }),
            "reading initial tab order",
            false,
        )
        .await?,
        "initial tab list",
    )?;
    let source = tab_at_id(&initial, source_tab, workspace, "captured origin tab")?;
    let target = tab_at_public_number(&initial, target_number, workspace, "requested tab number")?;
    if source.id == target.id {
        return Ok(());
    }

    let first_insert_index = if target.position.get() < source.position.get() {
        source.position.insert_after()
    } else {
        source.position.get()
    };
    let after_first = tab_list(
        &request_tab_swap(
            authority,
            "tab.move",
            json!({
                "tab_id": target.id.as_str(),
                "insert_index": first_insert_index,
            }),
            "moving target tab to captured tab position",
            true,
        )
        .await?,
        "first tab.move result",
    )?;
    let target_after_first = tab_at_id(&after_first, &target.id, workspace, "moved target tab")?;
    if target_after_first.position != source.position {
        return Err(TabSwapError::Known(format!(
            "The first Herdr tab.move did not place the target tab at position {}. Tab ordering may have changed partially. Muxe will not repeat the swap.",
            source.position.get()
        )));
    }
    let source_after_first = tab_at_id(&after_first, source_tab, workspace, "captured origin tab")?;

    let second_insert_index = if source_after_first.position.get() < target.position.get() {
        target.position.insert_after()
    } else {
        target.position.get()
    };
    let after_second = tab_list(
        &request_tab_swap(
            authority,
            "tab.move",
            json!({
                "tab_id": source_tab.as_str(),
                "insert_index": second_insert_index,
            }),
            "moving captured tab to requested tab position",
            true,
        )
        .await?,
        "second tab.move result",
    )?;
    let source_after = tab_at_id(&after_second, source_tab, workspace, "captured origin tab")?;
    let target_after = tab_at_id(&after_second, &target.id, workspace, "target tab")?;
    if source_after.position != target.position || target_after.position != source.position {
        return Err(TabSwapError::Known(format!(
            "Herdr completed the tab moves, but did not swap positions {} and {} as requested. Tab ordering may have changed partially. Muxe will not repeat the swap.",
            source.position.get(),
            target.position.get()
        )));
    }
    Ok(())
}

async fn request_tab_swap(
    authority: &IncarnationTransactionAuthority,
    method: &str,
    params: Value,
    phase: &str,
    state_changing: bool,
) -> Result<Value, TabSwapError> {
    match authority.invoke_response(method, params).await {
        Ok(HerdrResponse::Success(result)) => Ok(result),
        Ok(HerdrResponse::Error { code, message }) => Err(TabSwapError::Known(format!(
            "Herdr rejected {method} during the tab swap ({phase}, {code}): {message}. {}",
            if state_changing {
                "An earlier move may have changed tab ordering. Muxe will not repeat the swap."
            } else {
                "Herdr did not accept a tab move for this request."
            }
        ))),
        Err(error) if state_changing && error.kind == AdapterErrorKind::OutcomeUnknown => {
            Err(TabSwapError::Unknown(format!(
                "Muxe could not confirm the response during the tab swap ({phase}). This tab.move may have executed. Muxe will not repeat the swap: {error}"
            )))
        }
        Err(error) => Err(TabSwapError::Known(format!(
            "The Herdr request failed during the tab swap ({phase}): {error}"
        ))),
    }
}

/// Workspace-scoped tab.list preserves row order; its stable number is not the current position.
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
    for (list_position, tab) in tabs.iter().enumerate() {
        let tab = tab.as_object().ok_or_else(|| {
            TabSwapError::Known(format!("Herdr {phase} contains a non-object tab"))
        })?;
        let id = muxe_core::TabId::new(required_tab_field(tab, "tab_id", phase)?);
        let workspace =
            muxe_core::WorkspaceId::new(required_tab_field(tab, "workspace_id", phase)?);
        let public_number = tab
            .get("number")
            .and_then(Value::as_u64)
            .map(PublicTabNumber::from_response)
            .ok_or_else(|| {
                TabSwapError::Known(format!(
                    "Herdr {phase} tab {:?} lacks a nonnegative number",
                    id.as_str()
                ))
            })?;
        if ordered.iter().any(|existing: &OrderedTab| {
            existing.workspace == workspace && existing.public_number == public_number
        }) {
            return Err(TabSwapError::Known(format!(
                "Herdr {phase} repeats tab number {} in workspace {:?}",
                public_number.get(),
                workspace.as_str()
            )));
        }
        ordered.push(OrderedTab {
            id,
            workspace,
            public_number,
            position: TabPosition::from_list_order(list_position),
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
    id: &muxe_core::TabId,
    workspace: &muxe_core::WorkspaceId,
    role: &str,
) -> Result<&'a OrderedTab, TabSwapError> {
    tabs.iter()
        .find(|tab| &tab.id == id && &tab.workspace == workspace)
        .ok_or_else(|| {
            TabSwapError::Known(format!(
                "Herdr tab list has no {role} {:?} in captured workspace {:?}",
                id.as_str(),
                workspace.as_str()
            ))
        })
}

fn tab_at_public_number<'a>(
    tabs: &'a [OrderedTab],
    number: PublicTabNumber,
    workspace: &muxe_core::WorkspaceId,
    role: &str,
) -> Result<&'a OrderedTab, TabSwapError> {
    tabs.iter()
        .find(|tab| tab.public_number == number && &tab.workspace == workspace)
        .ok_or_else(|| {
            TabSwapError::Known(format!(
                "Herdr tab list has no {role} {} in captured workspace {:?}",
                number.get(),
                workspace.as_str()
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
    matches!(direction.value.kind, ConfigValueKind::Context(_))
        || scalar_split_direction(direction).is_ok()
}

fn scalar_is_cardinal_or_context(scalar: &ActionScalar) -> bool {
    matches!(scalar.value.kind, ConfigValueKind::Context(_))
        || scalar_pane_direction(scalar).is_ok()
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
            "The action parameter must be a string",
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
            "The action index must be a nonnegative integer",
        ));
    };
    u64::try_from(value).map_err(|_| {
        AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "The action index must be a nonnegative integer",
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
            "The action parameter must be a boolean",
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
            "The pane resize amount must be a number",
        )),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_pane_direction(value: &ActionScalar) -> Result<HerdrPaneDirection, AdapterError> {
    let direction = scalar_string(value)?
        .parse::<Direction>()
        .map_err(|_| direction_request_error(PANE_DIRECTION_ERROR))?;
    HerdrPaneDirection::try_from(direction).map_err(direction_request_error)
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn scalar_split_direction(value: &ActionScalar) -> Result<crate::UiSplitDirection, AdapterError> {
    let direction = scalar_string(value)?
        .parse::<Direction>()
        .map_err(|_| direction_request_error(SPLIT_DIRECTION_ERROR))?;
    split_direction(direction).map_err(direction_request_error)
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

fn scalar_split_direction_value(value: &ActionScalar) -> Result<crate::UiSplitDirection, String> {
    scalar_split_direction(value).map_err(|error| error.to_string())
}

fn scalar_pane_direction_value(value: &ActionScalar) -> Result<HerdrPaneDirection, String> {
    scalar_pane_direction(value).map_err(|error| error.to_string())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HerdrPaneDirection {
    Left,
    Right,
    Up,
    Down,
}

impl HerdrPaneDirection {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Left => "left",
            Self::Right => "right",
            Self::Up => "up",
            Self::Down => "down",
        }
    }
}

impl TryFrom<Direction> for HerdrPaneDirection {
    type Error = &'static str;

    fn try_from(direction: Direction) -> Result<Self, Self::Error> {
        match direction {
            Direction::Left => Ok(Self::Left),
            Direction::Right => Ok(Self::Right),
            Direction::Up => Ok(Self::Up),
            Direction::Down => Ok(Self::Down),
            Direction::Next | Direction::Previous => Err(PANE_DIRECTION_ERROR),
        }
    }
}

const PANE_DIRECTION_ERROR: &str = "Herdr pane direction must be left, right, up, or down";
const SPLIT_DIRECTION_ERROR: &str = "Herdr pane split direction must be right or down";

fn direction_request_error(message: &'static str) -> AdapterError {
    AdapterError::new(AdapterErrorKind::InvalidRequest, message)
}

fn split_direction(direction: Direction) -> Result<crate::UiSplitDirection, &'static str> {
    match direction {
        Direction::Right => Ok(crate::UiSplitDirection::Right),
        Direction::Down => Ok(crate::UiSplitDirection::Down),
        Direction::Left | Direction::Up | Direction::Next | Direction::Previous => {
            Err(SPLIT_DIRECTION_ERROR)
        }
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Herdr resize amounts are floating-point JSON numbers"
)]
#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the shared public error type"
)]
fn resize_number(amount: &ResizeAmount) -> Result<f64, AdapterError> {
    match amount {
        ResizeAmount::Integer(value) => Ok(*value as f64),
        ResizeAmount::Unsigned(value) => Ok(*value as f64),
        ResizeAmount::Float(value) => Ok(*value),
        ResizeAmount::Boolean(_) | ResizeAmount::Text(_) => Err(AdapterError::new(
            AdapterErrorKind::InvalidRequest,
            "The pane resize amount must be a number",
        )),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the shared public error type"
)]
fn json_word<'a>(word: &'a muxe_core::CommandWord, field: &str) -> Result<&'a str, AdapterError> {
    word.as_os_str()
        .to_str()
        .ok_or_else(|| incompatible(format!("Herdr {field} cannot be represented as UTF-8 text")))
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the shared public error type"
)]
fn json_path<'a>(path: &'a Path, field: &str) -> Result<&'a str, AdapterError> {
    path.to_str()
        .ok_or_else(|| incompatible(format!("Herdr {field} cannot be represented as UTF-8 text")))
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
        format!("Herdr rejected {method} ({code}): {message}"),
    )
}

fn pane_is_proven_absent(code: &str) -> bool {
    code == "pane_not_found"
}

fn pending_cleanup_lease_stale() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::ContextUnavailable,
        "The saved permission to close the pending Herdr pane no longer matches the current pane, UI session, or connection.",
    )
}

fn pending_cleanup_outcome_unknown() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::OutcomeUnknown,
        "Herdr may already have closed the pending pane. Muxe will not send another close request because the pane ID may have been reused.",
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
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
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
    async fn test_incarnation_runtime(
        temp: &tempfile::TempDir,
        socket_name: &str,
    ) -> (Arc<HerdrRuntime>, Arc<UnixListener>) {
        let socket = temp.path().join(socket_name);
        let listener = Arc::new(UnixListener::bind(&socket).unwrap());
        let answer = async {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            let request: Value = serde_json::from_slice(&line).unwrap();
            assert_eq!(request["method"], "ping");
            let id = request["id"].as_str().unwrap();
            reader
                .write_all(
                    format!(
                        "{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\",\"protocol\":{},\"version\":\"0.8.2\"}}}}\n",
                        crate::generated::BUNDLED_PROTOCOL
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        };
        let (runtime, ()) = tokio::join!(HerdrRuntime::for_subscription_test(&socket), answer);
        (Arc::new(runtime.unwrap()), listener)
    }

    #[tokio::test]
    async fn stale_eof_from_old_lease_is_a_noop_after_new_runtime_installation() {
        let temp = tempfile::TempDir::new().unwrap();
        let (old_runtime, _old_listener) = test_incarnation_runtime(&temp, "old-herdr.sock").await;
        let mut state = IncarnationState {
            runtime: Arc::clone(&old_runtime),
            epoch: IncarnationEpoch::INITIAL,
            continuity: HostContinuityEpoch::initial(),
            healthy: true,
        };
        let old_lease = old_runtime.lease(IncarnationEpoch::INITIAL);
        assert!(state.transition_to_lost(&old_lease));

        let (new_runtime, _new_listener) = test_incarnation_runtime(&temp, "new-herdr.sock").await;
        let new_epoch = IncarnationEpoch::INITIAL.next();
        let new_lease = new_runtime.lease(new_epoch);
        let compatibility = native_compatibility_snapshot(
            &new_runtime,
            state.continuity,
            &HerdrCache::new(temp.path()),
        );
        assert!(
            state
                .install(Arc::clone(&new_runtime), &new_lease, compatibility)
                .is_some()
        );

        assert!(!state.transition_to_lost(&old_lease));
        assert!(state.healthy);
        assert_eq!(state.epoch, new_epoch);
        assert!(Arc::ptr_eq(&state.runtime, &new_runtime));
    }

    #[tokio::test]
    async fn eof_and_guard_mismatch_share_one_idempotent_loss_transition() {
        let temp = tempfile::TempDir::new().unwrap();
        let (runtime, _listener) = test_incarnation_runtime(&temp, "herdr.sock").await;
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let mut state = IncarnationState {
            runtime,
            epoch: IncarnationEpoch::INITIAL,
            continuity: HostContinuityEpoch::initial(),
            healthy: true,
        };

        assert!(state.transition_to_lost(&lease), "EOF wins the transition");
        assert!(
            !state.transition_to_lost(&lease),
            "a simultaneous guarded mismatch is stale after the winning loss"
        );
        assert!(!state.healthy);
        assert_eq!(state.epoch, IncarnationEpoch::INITIAL.next());
    }

    #[test]
    fn a_new_local_epoch_blocks_an_origin_with_the_same_observed_server() {
        let identity = HostIdentity {
            kind: muxe_adapter_api::HostKind::Herdr,
            discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(
                "/owned/herdr.sock".to_owned(),
            )
            .expect("validated host discovery key"),
            live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
                "observed-server".to_owned(),
            )
            .expect("validated live server incarnation"),
        };
        let mut captured = origin();
        captured.server_id =
            ServerId::new(origin_epoch_token(&identity, IncarnationEpoch::INITIAL));

        assert!(!origin_is_current(
            &captured,
            &identity,
            IncarnationEpoch::INITIAL.next()
        ));
        assert_eq!(identity.live_server_id.as_str(), "observed-server");
    }

    #[test]
    fn unsupported_portable_forms_do_not_receive_invented_herdr_defaults() {
        for action in [
            ResolvedPortableAction::Tab(ResolvedTabAction::Rename { name: None }),
            ResolvedPortableAction::Pane(ResolvedPaneAction::Create),
            ResolvedPortableAction::Pane(ResolvedPaneAction::Split {
                direction: None,
                focus: None,
                command: ResolvedCreateCommand::default(),
            }),
        ] {
            let Err(error) = portable_invocation(&action, &origin()) else {
                panic!("form without a schema-defined Herdr mapping must fail");
            };
            assert_eq!(error.kind, AdapterErrorKind::Incompatible);
        }
    }

    fn deferred_request() -> PostDismissalPortableDispatchRequest {
        PostDismissalPortableDispatchRequest {
            execution: muxe_core::ExecutionId(1),
            action: ResolvedPortableAction::Tab(ResolvedTabAction::Create {
                workspace_id: None,
                name: None,
                focus: None,
                command: ResolvedCreateCommand::default(),
            }),
            origin: origin(),
            ui_pane: muxe_core::PaneId::new("muxe-ui"),
        }
    }

    #[test]
    fn pane_close_claim_makes_later_snapshot_failure_nonrejecting() {
        let request = deferred_request();
        let mut pending = HashMap::from([(request.ui_pane.clone(), vec![request.clone()])]);

        let claimed = pending.remove(&request.ui_pane);
        assert_eq!(claimed, Some(vec![request.clone()]));
        assert!(
            !snapshot_failure_reclaims_request(&mut pending, &request.ui_pane, request.execution),
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
    fn pane_zoom_preserves_its_direct_host_mode() {
        let invocation = portable_invocation(
            &ResolvedPortableAction::Pane(ResolvedPaneAction::Zoom {
                enabled: Some(true),
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
    fn tab_swap_selector_uses_public_number_and_workspace_local_position() {
        let workspace = muxe_core::WorkspaceId::new("workspace");
        let other_workspace = muxe_core::WorkspaceId::new("other");
        let tabs = tab_list(
            &json!({
                "type": "tab_list",
                "tabs": [
                    { "tab_id": "workspace:t1", "workspace_id": "workspace", "number": 1 },
                    { "tab_id": "workspace:t3", "workspace_id": "workspace", "number": 3 },
                    { "tab_id": "workspace:t4", "workspace_id": "workspace", "number": 4 },
                    { "tab_id": "workspace:t5", "workspace_id": "workspace", "number": 5 }
                ]
            }),
            "workspace tab list",
        )
        .expect("tab.list returns rows in workspace order");
        let other_tabs = tab_list(
            &json!({
                "type": "tab_list",
                "tabs": [
                    { "tab_id": "other:t1", "workspace_id": "other", "number": 1 },
                    { "tab_id": "other:t2", "workspace_id": "other", "number": 2 },
                    { "tab_id": "other:t3", "workspace_id": "other", "number": 3 },
                    { "tab_id": "other:t4", "workspace_id": "other", "number": 4 },
                    { "tab_id": "other:t5", "workspace_id": "other", "number": 5 }
                ]
            }),
            "other workspace tab list",
        )
        .expect("other workspace has an independent ordered list");
        let selector = PublicTabNumber::from_selector(muxe_core::TabIndex::new(5));

        let target = tab_at_public_number(&tabs, selector, &workspace, "requested tab number")
            .expect("public number 5 selects workspace:t5");
        assert_eq!(target.id.as_str(), "workspace:t5");
        assert_eq!(target.position.get(), 3);
        let other_target = tab_at_public_number(
            &other_tabs,
            selector,
            &other_workspace,
            "requested tab number",
        )
        .expect("the same public number selects other:t5 in its workspace");
        assert_eq!(other_target.id.as_str(), "other:t5");
        assert_eq!(other_target.position.get(), 4);
    }

    #[tokio::test]
    async fn tab_swap_preserves_public_number_selector_and_insert_positions() {
        let temp = tempfile::TempDir::new().expect("owned tab swap directory");
        let (runtime, listener) = test_incarnation_runtime(&temp, "tab-swap.sock").await;
        let lease = runtime.lease(IncarnationEpoch::INITIAL);
        let (continuity_loss_tx, _continuity_loss_rx) = tokio::sync::mpsc::unbounded_channel();
        let authority = IncarnationAuthority {
            runtime: Arc::clone(&runtime),
            lease,
            continuity_loss_tx,
            request_connect_wait_hook: None,
        };
        let response_tabs = |order: &[u64]| {
            order
                .iter()
                .map(|number| {
                    json!({
                        "tab_id": format!("workspace:t{number}"),
                        "workspace_id": "workspace",
                        "number": number,
                    })
                })
                .collect::<Vec<_>>()
        };
        let snapshots = [
            json!({
                "type": "tab_list",
                "tabs": response_tabs(&[1, 2, 3, 4, 5, 6, 7, 8, 9])
            }),
            json!({
                "type": "tab_list",
                "tabs": response_tabs(&[1, 2, 3, 4, 6, 7, 8, 5, 9])
            }),
            json!({
                "type": "tab_list",
                "tabs": response_tabs(&[1, 2, 3, 4, 8, 6, 7, 5, 9])
            }),
        ];
        let host = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(snapshots.len());
            for snapshot in snapshots {
                let (stream, _) = listener.accept().await.expect("accept ordered tab request");
                let mut reader = BufReader::new(stream);
                let mut line = Vec::new();
                reader
                    .read_until(b'\n', &mut line)
                    .await
                    .expect("read ordered tab request");
                let request: Value = serde_json::from_slice(&line).expect("Herdr request is JSON");
                let id = request["id"].as_str().expect("request has an ID");
                let response = json!({ "id": id, "result": snapshot });
                reader
                    .get_mut()
                    .write_all(format!("{response}\n").as_bytes())
                    .await
                    .expect("write ordered tab response");
                requests.push(request);
            }
            requests
        });

        let workspace = muxe_core::WorkspaceId::new("workspace");
        let source = muxe_core::TabId::new("workspace:t8");
        let completion = authority
            .run_ordered(move |direct| async move {
                perform_tab_swap(
                    &direct,
                    &workspace,
                    &source,
                    PublicTabNumber::from_selector(muxe_core::TabIndex::new(5)),
                )
                .await
            })
            .await
            .expect("ordered swap executes");
        completion.expect("pinned row snapshots confirm the tab swap");

        let requests = host.await.expect("tab swap host fixture completes");
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0]["method"], "tab.list");
        assert_eq!(
            requests[0]["params"],
            json!({ "workspace_id": "workspace" })
        );
        assert_eq!(requests[1]["method"], "tab.move");
        assert_eq!(
            requests[1]["params"],
            json!({ "tab_id": "workspace:t5", "insert_index": 8 })
        );
        assert_eq!(requests[2]["method"], "tab.move");
        assert_eq!(
            requests[2]["params"],
            json!({ "tab_id": "workspace:t8", "insert_index": 4 })
        );
    }

    #[test]
    fn pane_and_split_direction_domains_match_the_emitted_host_requests() {
        let schema = bundled_schema();
        for (text, direction) in [
            ("left", Direction::Left),
            ("right", Direction::Right),
            ("up", Direction::Up),
            ("down", Direction::Down),
            ("next", Direction::Next),
            ("previous", Direction::Previous),
        ] {
            let scalar = || {
                ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::String(
                    text.to_owned(),
                )))
            };
            let target = || muxe_core::IndexOrDirection::Direction(scalar());
            for (action, method) in [
                (
                    PortableAction::Pane(PaneAction::Focus(target())),
                    "pane.focus_direction",
                ),
                (
                    PortableAction::Pane(PaneAction::Swap(target())),
                    "pane.swap",
                ),
                (
                    PortableAction::Pane(PaneAction::Resize {
                        direction: scalar(),
                        amount: None,
                    }),
                    "pane.resize",
                ),
            ] {
                let resolved = action
                    .resolve_context(&origin())
                    .expect("core direction resolves");
                let load = portable_compile_validation(&schema, &action);
                let dispatch = portable_invocation(&resolved, &origin());
                if matches!(direction, Direction::Next | Direction::Previous) {
                    assert!(load.is_err(), "{method} must reject {text} at load");
                    assert!(matches!(
                        dispatch,
                        Err(AdapterError {
                            kind: AdapterErrorKind::Incompatible | AdapterErrorKind::InvalidRequest,
                            ..
                        })
                    ));
                } else {
                    load.expect("cardinal pane direction passes the pinned schema");
                    let invocation = dispatch.expect("cardinal pane direction dispatches");
                    assert_eq!(invocation.method, method);
                    assert_eq!(invocation.params["direction"], text);
                    assert_eq!(invocation.params["pane_id"], "pane");
                }
            }
            let action = PortableAction::Pane(PaneAction::Split {
                direction: Some(scalar()),
                focus: None,
                command: muxe_core::CreateCommand::default(),
            });
            let resolved = action
                .resolve_context(&origin())
                .expect("core split direction resolves");
            let load = portable_compile_validation(&schema, &action);
            let dispatch = portable_invocation(&resolved, &origin());
            if matches!(direction, Direction::Right | Direction::Down) {
                load.expect("right/down split direction passes the pinned schema");
                let invocation = dispatch.expect("right/down split dispatches");
                assert_eq!(invocation.method, "pane.split");
                assert_eq!(
                    invocation.params,
                    json!({
                        "target_pane_id": "pane", "direction": text, "focus": true, "cwd": null,
                    })
                );
            } else {
                assert!(load.is_err(), "split must reject {text} at load");
                assert!(matches!(
                    dispatch,
                    Err(AdapterError {
                        kind: AdapterErrorKind::Incompatible,
                        ..
                    })
                ));
            }
        }
    }

    #[test]
    fn selection_text_direction_is_resolved_before_host_restrictions() {
        let marker = || {
            ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::Context(
                ContextReference::parse(
                    "origin.selection.text",
                    SourceSpan::new(SourceId::new("<test>"), 0, 0),
                )
                .expect("known textual context path"),
            )))
        };
        let pane = PortableAction::Pane(PaneAction::Focus(muxe_core::IndexOrDirection::Direction(
            marker(),
        )));
        let split = PortableAction::Pane(PaneAction::Split {
            direction: Some(marker()),
            focus: Some(ActionScalar::new(ConfigValue::synthetic(
                ConfigValueKind::Boolean(false),
            ))),
            command: muxe_core::CreateCommand::default(),
        });
        let schema = bundled_schema();
        portable_compile_validation(&schema, &pane)
            .expect("selection direction is deferred at load");
        portable_compile_validation(&schema, &split)
            .expect("selection split direction is deferred at load");
        for (text, pane_supported, split_supported) in [
            ("left", true, false),
            ("right", true, true),
            ("up", true, false),
            ("down", true, true),
            ("next", false, false),
            ("previous", false, false),
        ] {
            let mut captured = origin();
            captured.selection_text = Some(text.to_owned());
            for (action, supported, is_split) in [
                (&pane, pane_supported, false),
                (&split, split_supported, true),
            ] {
                let resolved = action
                    .resolve_context(&captured)
                    .expect("selection resolves to a core direction");
                // Dispatch must not consult changed context after the action is resolved.
                captured.selection_text = Some("not-a-direction".to_owned());
                let invocation = portable_invocation(&resolved, &captured);
                captured.selection_text = Some(text.to_owned());
                if supported {
                    let invocation = invocation.expect("resolved supported direction dispatches");
                    assert_eq!(invocation.params["direction"], text);
                    if is_split {
                        assert_eq!(invocation.params["focus"], false);
                    }
                } else {
                    assert!(matches!(
                        invocation,
                        Err(AdapterError {
                            kind: AdapterErrorKind::Incompatible,
                            ..
                        })
                    ));
                }
            }
        }
    }

    #[test]
    fn command_creation_rejects_non_utf8_json_fields() {
        use muxe_core::{AbsolutePath, CommandCwd, CommandWord};
        use std::{ffi::OsString, os::unix::ffi::OsStringExt};
        let path = || {
            AbsolutePath::new(PathBuf::from(OsString::from_vec(b"/bad\xff".to_vec())))
                .expect("absolute OS path")
        };
        for (program, args, cwd, field) in [
            (
                Some(CommandWord::Path(path())),
                Vec::new(),
                None,
                "tab.program",
            ),
            (
                Some(CommandWord::Text("tool".to_owned())),
                vec![CommandWord::Path(path())],
                None,
                "tab.args",
            ),
            (
                Some(CommandWord::Text("tool".to_owned())),
                Vec::new(),
                Some(CommandCwd::Origin(path())),
                "tab.cwd",
            ),
        ] {
            let command = ResolvedCreateCommand { program, args, cwd };
            let error = if field == "tab.cwd" {
                creation_cwd(&command, &origin(), field)
                    .expect_err("JSON cannot encode non-UTF8 cwd")
            } else {
                creation_argv(&command, "tab.program", "tab.args")
                    .expect_err("JSON cannot encode non-UTF8 argv")
            };
            assert_eq!(error.kind, AdapterErrorKind::Incompatible);
            assert!(error.to_string().contains(field));
        }
    }

    #[test]
    fn captured_tab_move_index_retains_the_full_unsigned_range() {
        let index = ActionScalar::new(ConfigValue::synthetic(ConfigValueKind::Context(
            ContextReference::parse(
                "origin.tab.index",
                SourceSpan::new(SourceId::new("<test>"), 0, 0),
            )
            .expect("known unsigned context path"),
        )));
        let action =
            PortableAction::Tab(TabAction::Move(muxe_core::IndexOrDirection::Index(index)));
        portable_compile_validation(&bundled_schema(), &action)
            .expect("unsigned context index passes load validation");
        let mut captured = origin();
        captured.tab_index = Some(u64::MAX);
        let resolved = action
            .resolve_context(&captured)
            .expect("unsigned index resolves without an i64 intermediate");
        let invocation = portable_invocation(&resolved, &captured)
            .expect("tab move serializes its resolved index");
        assert_eq!(invocation.method, "tab.move");
        assert_eq!(
            invocation.params,
            json!({ "tab_id": "tab", "insert_index": u64::MAX })
        );
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
    fn command_bindings_reject_missing_host_helpers_at_load_time() {
        for (form, helpers) in [
            ("tab:create program=tool", &["layout.apply"][..]),
            (
                "pane:split direction=right program=tool",
                &["session.snapshot", "layout.apply", "pane.move", "tab.close"][..],
            ),
        ] {
            for missing in helpers {
                let schema = mutated_schema(|raw| {
                    raw["schemas"]["request"]["oneOf"]
                        .as_array_mut()
                        .expect("request catalog")
                        .retain(|branch| {
                            branch["properties"]["method"]["const"].as_str() != Some(*missing)
                        });
                });
                let validator = HerdrConfigValidator {
                    schema: Arc::new(schema),
                };
                let yaml = format!(
                    "version: 1\nmenus:\n  main:\n    bindings:\n      c:\n        label: command\n        action: {form}\n"
                );
                let diagnostics = muxe_core::compile_yaml(
                    muxe_core::CompiledGeneration(1),
                    SourceId::new("missing-helper.yml"),
                    yaml.as_str(),
                    muxe_core::KeyCapabilities::default(),
                    Some(&validator),
                )
                .expect_err("a binding requiring an unavailable helper must not be published");
                assert!(diagnostics.iter().any(|diagnostic| diagnostic.code
                    == DiagnosticCode::InvalidAction
                    && diagnostic.message.contains(missing)));
                assert!(
                    diagnostics
                        .iter()
                        .flat_map(|diagnostic| &diagnostic.labels)
                        .any(|label| label.span.source.as_str() == "missing-helper.yml")
                );
            }
        }
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
        let invocation = portable_invocation(
            &ResolvedPortableAction::Pane(ResolvedPaneAction::Close),
            &origin(),
        )
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
        let action = ResolvedPortableAction::Pane(ResolvedPaneAction::Resize {
            direction: Direction::Right,
            amount: Some(ResizeAmount::Float(2.0)),
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
