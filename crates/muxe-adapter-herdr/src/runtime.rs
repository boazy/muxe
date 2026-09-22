use std::{
    any::Any,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use muxe_adapter_api::{AdapterError, AdapterErrorKind, HostIdentity, HostKind};
use serde_json::{Map, Value};
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    ApiSchema, DeliveryState, EventSubscription, HerdrCache, HerdrResponse, SocketError,
    SubscriptionConfig,
    generated::{BUNDLED_PROTOCOL, MethodMetadata, method_metadata},
    transport::{EndpointContinuityToken, HerdrSocketClient},
};

/// Wall-clock bound for one `herdr api schema --json` invocation. Exceeding it fails
/// closed; the owned child is explicitly killed and reaped (see `runtime_schema`).
const SCHEMA_TIMEOUT: Duration = Duration::from_secs(10);
/// Bound for reaping the owned schema child after killing it on timeout. SIGKILL
/// cannot be caught, so this only guards against a wedged reaper, not the child.
const REAP_TIMEOUT: Duration = Duration::from_secs(5);
/// Absolute cap on accepted schema stdout. Anything larger is rejected before parsing.
const MAX_SCHEMA_BYTES: u64 = 8 * 1024 * 1024;
/// Retained stderr bytes for failure diagnostics; the remainder is discarded.
const MAX_DIAGNOSTIC_BYTES: u64 = 8 * 1024;
/// Maximum number of accepted unary transactions waiting behind the active
/// transaction for one exact Herdr runtime incarnation.
const SEND_QUEUE_CAPACITY: usize = 64;

type ErasedOutput = Box<dyn Any + Send + 'static>;
type OrderedFuture = Pin<Box<dyn Future<Output = ErasedOutput> + Send + 'static>>;
type OrderedRun = Box<dyn FnOnce(GuardedHerdrInvoker) -> OrderedFuture + Send + 'static>;
type OrderedFinish = Box<dyn FnOnce(Result<ErasedOutput, QueueExecutionError>) + Send + 'static>;

struct OrderedJob {
    run: OrderedRun,
    finish: OrderedFinish,
}

#[derive(Debug)]
pub(crate) enum QueueExecutionError {
    Panicked(String),
    TypeMismatch,
}

impl QueueExecutionError {
    fn into_adapter_error(self) -> AdapterError {
        let message = match self {
            Self::Panicked(message) => {
                format!("Herdr ordered transaction panicked internally: {message}")
            }
            Self::TypeMismatch => {
                "Herdr send-queue owner produced an invalid internal result type".to_owned()
            }
        };
        AdapterError::new(AdapterErrorKind::Unavailable, message)
    }
}
pub(crate) type OrderedReceiver<T> = oneshot::Receiver<Result<T, QueueExecutionError>>;

pub(crate) struct PreparedInvocation {
    metadata: &'static MethodMetadata,
    params: Value,
}

struct QueueLifecycle {
    admission_open: StdMutex<bool>,
    retirement_tx: watch::Sender<bool>,
}

impl QueueLifecycle {
    fn new() -> Arc<Self> {
        let (retirement_tx, _) = watch::channel(false);
        Arc::new(Self {
            admission_open: StdMutex::new(true),
            retirement_tx,
        })
    }

    fn is_open(&self) -> bool {
        *self
            .admission_open
            .lock()
            .expect("Herdr send-queue admission lock is not poisoned")
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.retirement_tx.subscribe()
    }

    /// Returns true only for the transition that closes admission.
    fn retire(&self) -> bool {
        let mut open = self
            .admission_open
            .lock()
            .expect("Herdr send-queue admission lock is not poisoned");
        if !*open {
            return false;
        }
        *open = false;
        let _ = self.retirement_tx.send(true);
        true
    }
}

#[derive(Clone)]
pub(crate) struct GuardedHerdrInvoker {
    client: Arc<HerdrSocketClient>,
    schema: Arc<ApiSchema>,
    expected: EndpointContinuityToken,
    lifecycle: Arc<QueueLifecycle>,
    replacement_reported: Arc<AtomicBool>,
}

pub(crate) struct ClassifiedInvokeError {
    pub(crate) error: AdapterError,
    pub(crate) delivery: DeliveryState,
    pub(crate) continuity_lost: bool,
}

impl GuardedHerdrInvoker {
    #[expect(
        clippy::result_large_err,
        reason = "prepared invocation preserves the adapter's shared error type"
    )]
    pub(crate) fn prepare(
        &self,
        method: &str,
        params: Value,
    ) -> Result<PreparedInvocation, AdapterError> {
        let metadata = checked_metadata(&self.schema, method, &params)?;
        Ok(PreparedInvocation { metadata, params })
    }

    pub(crate) async fn invoke_prepared(
        &self,
        lease: &IncarnationLease,
        invocation: PreparedInvocation,
    ) -> Result<HerdrResponse, ClassifiedInvokeError> {
        self.invoke_prepared_deadline(lease, invocation, None).await
    }

    pub(crate) async fn invoke_prepared_with_timeout(
        &self,
        lease: &IncarnationLease,
        invocation: PreparedInvocation,
        timeout: Duration,
    ) -> Result<HerdrResponse, ClassifiedInvokeError> {
        self.invoke_prepared_deadline(lease, invocation, Some(timeout))
            .await
    }

    async fn invoke_prepared_deadline(
        &self,
        lease: &IncarnationLease,
        invocation: PreparedInvocation,
        timeout: Option<Duration>,
    ) -> Result<HerdrResponse, ClassifiedInvokeError> {
        if !self.lifecycle.is_open() {
            return Err(retired_before_send());
        }
        if self.expected.proven_replacement(&lease.expected) {
            self.lifecycle.retire();
            let continuity_lost = !self.replacement_reported.swap(true, Ordering::AcqRel);
            return Err(ClassifiedInvokeError {
                error: socket_error(&SocketError::EndpointReplaced {
                    socket: self.client.socket().to_path_buf(),
                }),
                delivery: DeliveryState::NotSent,
                continuity_lost,
            });
        }
        let lifecycle = Arc::clone(&self.lifecycle);
        let retirement = lifecycle.subscribe();
        let result = match timeout {
            Some(timeout) => {
                self.client
                    .unary_on_expected_token_guarded_with_timeout(
                        invocation.metadata,
                        invocation.params,
                        &lease.expected,
                        timeout,
                        retirement,
                        move || {
                            lifecycle.retire();
                        },
                    )
                    .await
            }
            None => {
                self.client
                    .unary_on_expected_token_guarded(
                        invocation.metadata,
                        invocation.params,
                        &lease.expected,
                        retirement,
                        move || {
                            lifecycle.retire();
                        },
                    )
                    .await
            }
        };
        result.map_err(|error| {
            let continuity_lost = matches!(error, SocketError::EndpointReplaced { .. })
                && !self.replacement_reported.swap(true, Ordering::AcqRel);
            ClassifiedInvokeError {
                error: socket_error(&error),
                delivery: error.delivery(),
                continuity_lost,
            }
        })
    }
}

fn retired_before_send() -> ClassifiedInvokeError {
    ClassifiedInvokeError {
        error: AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr runtime incarnation retired before the request was sent",
        ),
        delivery: DeliveryState::NotSent,
        continuity_lost: false,
    }
}

struct QueueCompletion {
    finished: AtomicBool,
    notify: tokio::sync::Notify,
    owner_failure: StdMutex<Option<String>>,
}

struct OwnerCompletionGuard(Arc<QueueCompletion>);

impl Drop for OwnerCompletionGuard {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }
}

struct SendQueue {
    tx: mpsc::Sender<OrderedJob>,
    lifecycle: Arc<QueueLifecycle>,
    owner: Mutex<Option<JoinHandle<()>>>,
    completion: Arc<QueueCompletion>,
}

struct QueueReservation {
    queue: Arc<SendQueue>,
    permit: Option<mpsc::OwnedPermit<OrderedJob>>,
}

impl QueueReservation {
    #[expect(
        clippy::result_large_err,
        reason = "queue admission uses the adapter's shared error type at its internal boundary"
    )]
    fn submit(mut self, job: OrderedJob) -> Result<(), AdapterError> {
        let open = self
            .queue
            .lifecycle
            .admission_open
            .lock()
            .expect("Herdr send-queue admission lock is not poisoned");
        if !*open {
            return Err(queue_closed());
        }
        self.permit
            .take()
            .expect("Herdr queue reservation is consumed once")
            .send(job);
        Ok(())
    }
}

impl SendQueue {
    fn start(invoker: GuardedHerdrInvoker, lifecycle: Arc<QueueLifecycle>) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<OrderedJob>(SEND_QUEUE_CAPACITY);
        let mut retirement = lifecycle.subscribe();
        let completion = Arc::new(QueueCompletion {
            finished: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            owner_failure: StdMutex::new(None),
        });
        let worker_invoker = invoker;
        let worker_completion = Arc::clone(&completion);
        let owner = tokio::spawn(async move {
            let _completion_guard = OwnerCompletionGuard(worker_completion);
            loop {
                tokio::select! {
                    biased;
                    changed = retirement.changed() => {
                        if changed.is_err() || *retirement.borrow() {
                            rx.close();
                            while let Some(job) = rx.recv().await {
                                execute_ordered_job(job, worker_invoker.clone()).await;
                            }
                            break;
                        }
                    }
                    job = rx.recv() => {
                        let Some(job) = job else {
                            break;
                        };
                        execute_ordered_job(job, worker_invoker.clone()).await;
                    }
                }
            }
        });
        Arc::new(Self {
            tx,
            lifecycle,
            owner: Mutex::new(Some(owner)),
            completion,
        })
    }

    async fn reserve(self: &Arc<Self>) -> Result<QueueReservation, AdapterError> {
        let permit = self
            .tx
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| queue_closed())?;
        Ok(QueueReservation {
            queue: Arc::clone(self),
            permit: Some(permit),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "queue admission uses the adapter's shared error type at its internal boundary"
    )]
    fn try_reserve(self: &Arc<Self>) -> Result<QueueReservation, AdapterError> {
        let permit = self
            .tx
            .clone()
            .try_reserve_owned()
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    "Herdr send queue is full; request was not accepted",
                ),
                mpsc::error::TrySendError::Closed(_) => queue_closed(),
            })?;
        Ok(QueueReservation {
            queue: Arc::clone(self),
            permit: Some(permit),
        })
    }

    fn retire(&self) {
        self.lifecycle.retire();
    }

    async fn join(&self) -> Result<(), AdapterError> {
        self.retire();
        let mut owner = self.owner.lock().await;
        if let Some(handle) = owner.take()
            && let Err(error) = handle.await
        {
            *self
                .completion
                .owner_failure
                .lock()
                .expect("Herdr queue completion lock is not poisoned") = Some(error.to_string());
        }
        drop(owner);
        while !self.completion.finished.load(Ordering::Acquire) {
            let finished = self.completion.notify.notified();
            if self.completion.finished.load(Ordering::Acquire) {
                break;
            }
            finished.await;
        }
        if let Some(error) = self
            .completion
            .owner_failure
            .lock()
            .expect("Herdr queue completion lock is not poisoned")
            .clone()
        {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!("Herdr send-queue owner failed: {error}"),
            ));
        }
        Ok(())
    }
}

async fn execute_ordered_job(job: OrderedJob, invoker: GuardedHerdrInvoker) {
    let OrderedJob { run, finish } = job;
    let outcome = tokio::spawn(async move { run(invoker).await }).await;
    match outcome {
        Ok(output) => finish(Ok(output)),
        Err(error) => finish(Err(QueueExecutionError::Panicked(error.to_string()))),
    }
}

fn queue_closed() -> AdapterError {
    AdapterError::new(
        AdapterErrorKind::Shutdown,
        "Herdr runtime send queue is closed",
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HerdrAdapterConfig {
    /// Explicit socket of the selected Herdr server. No default or ambient endpoint is used.
    pub socket_path: PathBuf,
    /// Exact installed Herdr executable used to obtain its runtime request schema.
    pub herdr_binary: PathBuf,
    /// Cache base; runtime schema records are stored beneath its `herdr` child.
    pub cache_dir: PathBuf,
}

/// The server version parsed directly from the establishing pong for one
/// runtime epoch. Consumers never recover it by parsing the opaque live-server
/// identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HerdrServerVersion(String);

impl HerdrServerVersion {
    fn parse(value: &str) -> Option<Self> {
        (!value.is_empty()).then(|| Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HerdrServerVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IncarnationEpoch(u64);

impl IncarnationEpoch {
    pub(crate) const INITIAL: Self = Self(1);

    #[must_use]
    pub(crate) const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    #[must_use]
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

/// Private authority joining one adapter epoch to the exact endpoint token
/// observed by its runtime. It never exposes raw inode/process components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IncarnationLease {
    epoch: IncarnationEpoch,
    expected: EndpointContinuityToken,
}

impl IncarnationLease {
    #[must_use]
    pub(crate) const fn epoch(&self) -> IncarnationEpoch {
        self.epoch
    }
}

#[async_trait]
pub(crate) trait HerdrRequestAuthority: Sync {
    async fn request(&self, method: &str, params: Value) -> Result<HerdrResponse, AdapterError>;
}

pub struct HerdrRuntime {
    client: Arc<HerdrSocketClient>,
    schema: Arc<ApiSchema>,
    schema_cache_hit: bool,
    identity: HostIdentity,
    expected: EndpointContinuityToken,
    server_version: HerdrServerVersion,
    send_queue: Arc<SendQueue>,
}
impl HerdrRuntime {
    /// Acquires one runtime schema from the configured executable, verifies protocol compatibility,
    /// records its normalized cache key, and probes the exact server currently accepting at
    /// `socket_path`. The resulting OS observation is not continuity authority.
    ///
    /// # Errors
    ///
    /// Returns `AdapterError` when the schema binary cannot be executed, the live
    /// schema is incompatible, the cache cannot be updated, or the server cannot
    /// be probed.
    pub async fn connect(config: HerdrAdapterConfig) -> Result<Self, AdapterError> {
        let (schema, schema_cache_hit) =
            load_installed_schema(&config.herdr_binary, &config.cache_dir).await?;

        let client = Arc::new(HerdrSocketClient::new(config.socket_path.clone()));
        let (identity, expected, server_version) = establish_live_identity(&client).await?;
        Ok(Self::from_parts(
            client,
            schema,
            schema_cache_hit,
            identity,
            expected,
            server_version,
        ))
    }

    /// Establishes a retained event subscription guarded by this runtime's
    /// exact live endpoint observation. The guard is checked before the first
    /// subscribe byte without exposing its private lease/token.
    ///
    /// # Errors
    ///
    /// Returns [`SocketError`] for an endpoint replacement, transport failure,
    /// rejected subscription, invalid response, or correlation mismatch.
    pub async fn subscribe(
        &self,
        config: SubscriptionConfig,
    ) -> Result<(EventSubscription, Value), SocketError> {
        EventSubscription::connect_expected(self, self.lease(IncarnationEpoch::INITIAL), config)
            .await
    }
    #[cfg(test)]
    pub(crate) async fn for_subscription_test(socket: &Path) -> Result<Self, AdapterError> {
        let client = Arc::new(HerdrSocketClient::new(socket));
        let (identity, expected, server_version) = establish_live_identity(&client).await?;
        let schema = Arc::new(
            ApiSchema::parse(
                serde_json::from_str(include_str!(
                    "../../../fixtures/herdr/herdr-api.schema.json"
                ))
                .expect("bundled schema JSON"),
            )
            .expect("bundled schema parses"),
        );
        Ok(Self::from_parts(
            client,
            schema,
            false,
            identity,
            expected,
            server_version,
        ))
    }

    #[must_use]
    pub(crate) fn client(&self) -> &Arc<HerdrSocketClient> {
        &self.client
    }

    #[must_use]
    pub(crate) fn schema(&self) -> &Arc<ApiSchema> {
        &self.schema
    }

    /// Whether this connection parsed the cache-verified normalized request representation rather
    /// than freshly canonicalizing the same request surface.
    #[must_use]
    pub fn used_cached_schema_representation(&self) -> bool {
        self.schema_cache_hit
    }

    #[must_use]
    pub fn identity(&self) -> &HostIdentity {
        &self.identity
    }

    #[must_use]
    pub fn server_version(&self) -> &HerdrServerVersion {
        &self.server_version
    }

    #[must_use]
    pub(crate) fn lease(&self, epoch: IncarnationEpoch) -> IncarnationLease {
        IncarnationLease {
            epoch,
            expected: self.expected.clone(),
        }
    }

    pub(crate) async fn connect_expected_subscription(
        &self,
        lease: &IncarnationLease,
    ) -> Result<tokio::net::UnixStream, SocketError> {
        if self.expected.proven_replacement(&lease.expected) {
            return Err(SocketError::EndpointReplaced {
                socket: self.client.socket().to_path_buf(),
            });
        }
        self.client.connect_on_expected_token(&lease.expected).await
    }
    /// Validates once, accepts, and executes one unary against this exact
    /// runtime incarnation. Ordinary response processing waits for the
    /// schema-valid host operation to complete while the runtime stays live;
    /// retirement remains immediately cancellation-selectable.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] for schema/metadata incompatibility, a proved
    /// endpoint replacement before write, transport failure, retirement, or an
    /// invalid reply.
    pub async fn invoke_response(
        &self,
        method: &str,
        params: Value,
    ) -> Result<HerdrResponse, AdapterError> {
        let invocation = self.prepare_invocation(method, params)?;
        let lease = self.lease(IncarnationEpoch::INITIAL);
        self.run_ordered(
            move |invoker| async move { invoker.invoke_prepared(&lease, invocation).await },
        )
        .await?
        .map_err(|error| error.error)
    }

    /// Timeout variant of [`Self::invoke_response`]. The expected-incarnation
    /// check still occurs before any request byte is written.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] under the same conditions as
    /// [`Self::invoke_response`], plus a bounded response timeout.
    pub async fn invoke_response_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<HerdrResponse, AdapterError> {
        let invocation = self.prepare_invocation(method, params)?;
        let lease = self.lease(IncarnationEpoch::INITIAL);
        self.run_ordered(move |invoker| async move {
            invoker
                .invoke_prepared_with_timeout(&lease, invocation, timeout)
                .await
        })
        .await?
        .map_err(|error| error.error)
    }

    /// Returns the success payload of a guarded unary request.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError`] for transport/schema errors or a host
    /// rejection.
    pub async fn invoke(&self, method: &str, params: Value) -> Result<Value, AdapterError> {
        match self.invoke_response(method, params).await? {
            HerdrResponse::Success(result) => Ok(result),
            HerdrResponse::Error { code, message } => Err(AdapterError::new(
                AdapterErrorKind::DispatchFailed,
                format!("Herdr {method} rejected request with {code}: {message}"),
            )),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
    )]
    pub(crate) fn checked_metadata(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<&'static MethodMetadata, AdapterError> {
        checked_metadata(&self.schema, method, params)
    }
    #[expect(
        clippy::result_large_err,
        reason = "method-set validation preserves the adapter's shared error type"
    )]
    pub(crate) fn validate_method_set(&self, methods: &[&str]) -> Result<(), AdapterError> {
        for method in methods {
            if self.schema.method(method).is_none() {
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
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "prepared invocation preserves the adapter's shared error type"
    )]
    pub(crate) fn prepare_invocation(
        &self,
        method: &str,
        params: Value,
    ) -> Result<PreparedInvocation, AdapterError> {
        let metadata = self.checked_metadata(method, &params)?;
        Ok(PreparedInvocation { metadata, params })
    }

    pub(crate) async fn run_ordered<T, F, Fut>(&self, operation: F) -> Result<T, AdapterError>
    where
        T: Send + 'static,
        F: FnOnce(GuardedHerdrInvoker) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let reservation = self.send_queue.reserve().await?;
        let receiver = submit_ordered(reservation, operation)?;
        Self::await_ordered(receiver).await
    }

    #[expect(
        clippy::result_large_err,
        reason = "ordered invocation preserves the adapter's shared error type"
    )]
    pub(crate) fn try_run_ordered<T, F, Fut>(
        &self,
        operation: F,
    ) -> Result<OrderedReceiver<T>, AdapterError>
    where
        T: Send + 'static,
        F: FnOnce(GuardedHerdrInvoker) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        submit_ordered(self.send_queue.try_reserve()?, operation)
    }
    pub(crate) async fn await_ordered<T>(receiver: OrderedReceiver<T>) -> Result<T, AdapterError> {
        receiver
            .await
            .map_err(|_| {
                AdapterError::new(
                    AdapterErrorKind::Shutdown,
                    "Herdr send-queue owner stopped before completing accepted work",
                )
            })?
            .map_err(QueueExecutionError::into_adapter_error)
    }

    pub(crate) fn retire_send_queue(&self) {
        self.send_queue.retire();
    }

    pub(crate) async fn shutdown_send_queue(&self) -> Result<(), AdapterError> {
        self.send_queue.join().await
    }

    fn from_parts(
        client: Arc<HerdrSocketClient>,
        schema: Arc<ApiSchema>,
        schema_cache_hit: bool,
        identity: HostIdentity,
        expected: EndpointContinuityToken,
        server_version: HerdrServerVersion,
    ) -> Self {
        let lifecycle = QueueLifecycle::new();
        let invoker = GuardedHerdrInvoker {
            client: Arc::clone(&client),
            schema: Arc::clone(&schema),
            expected: expected.clone(),
            lifecycle: Arc::clone(&lifecycle),
            replacement_reported: Arc::new(AtomicBool::new(false)),
        };
        Self {
            client,
            schema,
            schema_cache_hit,
            identity,
            expected,
            server_version,
            send_queue: SendQueue::start(invoker, lifecycle),
        }
    }
}
#[expect(
    clippy::result_large_err,
    reason = "ordered invocation preserves the adapter's shared error type"
)]
fn submit_ordered<T, F, Fut>(
    reservation: QueueReservation,
    operation: F,
) -> Result<OrderedReceiver<T>, AdapterError>
where
    T: Send + 'static,
    F: FnOnce(GuardedHerdrInvoker) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    let (result_tx, result_rx) = oneshot::channel();
    let job = OrderedJob {
        run: Box::new(move |invoker| {
            Box::pin(async move { Box::new(operation(invoker).await) as ErasedOutput })
        }),
        finish: Box::new(move |outcome| {
            let typed = outcome.and_then(|output| {
                output
                    .downcast::<T>()
                    .map(|value| *value)
                    .map_err(|_| QueueExecutionError::TypeMismatch)
            });
            let _ = result_tx.send(typed);
        }),
    };
    reservation.submit(job)?;
    Ok(result_rx)
}

pub(crate) struct RuntimeTransactionAuthority {
    invoker: GuardedHerdrInvoker,
    lease: IncarnationLease,
}

impl RuntimeTransactionAuthority {
    pub(crate) fn new(invoker: GuardedHerdrInvoker, lease: IncarnationLease) -> Self {
        Self { invoker, lease }
    }
}

#[async_trait]
impl HerdrRequestAuthority for RuntimeTransactionAuthority {
    async fn request(&self, method: &str, params: Value) -> Result<HerdrResponse, AdapterError> {
        let invocation = self.invoker.prepare(method, params)?;
        self.invoker
            .invoke_prepared(&self.lease, invocation)
            .await
            .map_err(|error| error.error)
    }
}

#[async_trait]
impl HerdrRequestAuthority for HerdrRuntime {
    async fn request(&self, method: &str, params: Value) -> Result<HerdrResponse, AdapterError> {
        self.invoke_response(method, params).await
    }
}

/// Loads and normalizes the exact installed Herdr request schema without
/// opening the configured host socket.
pub(crate) async fn load_installed_schema(
    binary: &Path,
    cache_dir: &Path,
) -> Result<(Arc<ApiSchema>, bool), AdapterError> {
    let raw_schema = runtime_schema(binary).await?;
    let (protocol, schema_version) = ApiSchema::metadata(&raw_schema)
        .map_err(|error| incompatible(format!("installed Herdr API schema is invalid: {error}")))?;
    if protocol != BUNDLED_PROTOCOL {
        return Err(incompatible(format!(
            "Herdr protocol {protocol} is incompatible with required protocol {BUNDLED_PROTOCOL}"
        )));
    }
    let (normalized_request, schema_cache_hit) = HerdrCache::new(cache_dir)
        .normalized_schema(protocol, schema_version, &raw_schema)
        .map_err(|error| {
            AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!("could not update Herdr runtime-schema cache: {error}"),
            )
        })?;
    let normalized_request = serde_json::from_slice(&normalized_request).map_err(|error| {
        incompatible(format!(
            "Herdr runtime-schema cache returned an invalid normalized request representation: {error}"
        ))
    })?;
    let schema = ApiSchema::parse_with_request(raw_schema, &normalized_request)
        .map_err(|error| incompatible(format!("installed Herdr API schema is invalid: {error}")))?;
    Ok((Arc::new(schema), schema_cache_hit))
}

async fn runtime_schema(binary: &Path) -> Result<Value, AdapterError> {
    runtime_schema_with_timeouts(binary, SCHEMA_TIMEOUT, REAP_TIMEOUT).await
}

async fn runtime_schema_with_timeouts(
    binary: &Path,
    schema_timeout: Duration,
    reap_timeout: Duration,
) -> Result<Value, AdapterError> {
    let mut child = Command::new(binary)
        .arg("api")
        .arg("schema")
        .arg("--json")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // `kill_on_drop` is only a backstop: the timeout path below explicitly
        // kills and reaps exactly this owned child.
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| {
            AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!(
                    "could not execute {} api schema --json: {error}",
                    binary.display()
                ),
            )
        })?;
    let outcome = tokio::time::timeout(schema_timeout, collect_schema_output(&mut child)).await;
    let (status, stdout, diagnostics) = if let Ok(collected) = outcome {
        collected?
    } else {
        // Explicitly signal and then reap exactly this retained child handle. There is no
        // name or PID search, no process-group signal, and no global cleanup.
        // `kill_on_drop` remains only as a backstop. A failed kill or reap is surfaced
        // instead of pretending the owned child is gone.
        child.start_kill().map_err(|error| {
            AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!("could not kill timed-out Herdr schema child: {error}"),
            )
        })?;
        match tokio::time::timeout(reap_timeout, child.wait()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!("could not reap timed-out Herdr schema child: {error}"),
                ));
            }
            Err(_) => {
                return Err(AdapterError::new(
                    AdapterErrorKind::Unavailable,
                    format!(
                        "timed out after {}s while reaping the owned Herdr schema child",
                        reap_timeout.as_secs()
                    ),
                ));
            }
        }
        return Err(AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!(
                "{} api schema --json timed out after {}s",
                binary.display(),
                schema_timeout.as_secs()
            ),
        ));
    };
    if !status.success() {
        return Err(AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!(
                "{} api schema --json exited with {status}: {}",
                binary.display(),
                String::from_utf8_lossy(&diagnostics)
            ),
        ));
    }
    if stdout.len() as u64 > MAX_SCHEMA_BYTES {
        return Err(incompatible(format!(
            "{} api schema --json emitted {} bytes, above the {}-byte bound",
            binary.display(),
            stdout.len(),
            MAX_SCHEMA_BYTES
        )));
    }
    serde_json::from_slice(&stdout).map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Incompatible,
            format!(
                "{} api schema --json emitted invalid JSON: {error}",
                binary.display()
            ),
        )
    })
}

/// Reads bounded stdout/stderr from an already spawned schema child and waits for its
/// exit. Stdout beyond [`MAX_SCHEMA_BYTES`] is still fully consumed (so the child is
/// never left blocked on a full pipe) but rejected by the caller before parsing.
async fn collect_schema_output(
    child: &mut Child,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), AdapterError> {
    let mut stdout = child.stdout.take().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr schema child has no captured stdout",
        )
    })?;
    let mut stderr = child.stderr.take().ok_or_else(|| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            "Herdr schema child has no captured stderr",
        )
    })?;
    let diagnostics = tokio::spawn(async move {
        let mut drained = Vec::new();
        let mut limited = (&mut stderr).take(MAX_DIAGNOSTIC_BYTES + 1);
        let _ = limited.read_to_end(&mut drained).await;
        drained
    });
    let mut schema = Vec::new();
    let mut limited = (&mut stdout).take(MAX_SCHEMA_BYTES + 1);
    limited.read_to_end(&mut schema).await.map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!("could not read Herdr schema stdout: {error}"),
        )
    })?;
    drop(limited);
    drop(stdout);
    let status = child.wait().await.map_err(|error| {
        AdapterError::new(
            AdapterErrorKind::Unavailable,
            format!("could not reap Herdr schema child: {error}"),
        )
    })?;
    let diagnostics = diagnostics.await.unwrap_or_default();
    Ok((status, schema, diagnostics))
}

async fn establish_live_identity(
    client: &HerdrSocketClient,
) -> Result<(HostIdentity, EndpointContinuityToken, HerdrServerVersion), AdapterError> {
    let metadata = method_metadata("ping")
        .ok_or_else(|| incompatible("bundled Herdr metadata does not declare ping"))?;
    let (response, expected) = client
        .establish_unary(metadata, Value::Object(Map::new()))
        .await
        .map_err(|error| socket_error(&error))?;
    let result = match response {
        HerdrResponse::Success(result) => result,
        HerdrResponse::Error { code, message } => {
            return Err(AdapterError::new(
                AdapterErrorKind::Unavailable,
                format!("Herdr rejected ping with {code}: {message}"),
            ));
        }
    };
    let (identity, version) =
        identity_from_ping_result(client.socket().display().to_string(), &expected, &result)?;
    Ok((identity, expected, version))
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn identity_from_ping_result(
    discovery_key: String,
    endpoint: &EndpointContinuityToken,
    result: &Value,
) -> Result<(HostIdentity, HerdrServerVersion), AdapterError> {
    let object = result
        .as_object()
        .ok_or_else(|| incompatible("Herdr ping result is not an object"))?;
    if object.get("type").and_then(Value::as_str) != Some("pong") {
        return Err(incompatible("Herdr ping result has unexpected type"));
    }
    let protocol = object
        .get("protocol")
        .and_then(Value::as_u64)
        .ok_or_else(|| incompatible("Herdr ping result lacks integer protocol"))?;
    if protocol != BUNDLED_PROTOCOL {
        return Err(incompatible(format!(
            "live Herdr protocol {protocol} is incompatible with required protocol {BUNDLED_PROTOCOL}"
        )));
    }
    let version = HerdrServerVersion::parse(
        object
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    )
    .ok_or_else(|| incompatible("Herdr ping result lacks nonempty version"))?;
    // Protocol 20 exposes no server nonce. The configured socket identifies
    // discovery, while the observed-connect token contributes an opaque live
    // identity. Equality remains inconclusive; only inequality proves change.
    let identity = HostIdentity {
        kind: HostKind::Herdr,
        discovery_key: muxe_adapter_api::HostDiscoveryKey::parse(discovery_key)
            .expect("validated Herdr discovery key"),
        live_server_id: muxe_adapter_api::LiveServerIncarnationId::parse(
            endpoint.live_server_id(protocol, version.as_str()),
        )
        .expect("validated Herdr endpoint incarnation"),
    };
    Ok((identity, version))
}

#[expect(
    clippy::result_large_err,
    reason = "AdapterError is the crate's shared public error type; boxing it would break the public API"
)]
fn checked_metadata(
    schema: &ApiSchema,
    method: &str,
    params: &Value,
) -> Result<&'static MethodMetadata, AdapterError> {
    schema
        .validate_method(method, params)
        .map_err(|error| incompatible(format!("active Herdr schema rejects {method}: {error}")))?;
    method_metadata(method)
        .ok_or_else(|| incompatible(format!("bundled Herdr metadata does not declare {method}")))
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
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::UnixListener,
    };

    fn test_schema() -> Arc<ApiSchema> {
        Arc::new(
            ApiSchema::parse(
                serde_json::from_str(include_str!(
                    "../../../fixtures/herdr/herdr-api.schema.json"
                ))
                .expect("bundled schema JSON"),
            )
            .expect("bundled schema parses"),
        )
    }

    async fn answer_ping(listener: Arc<UnixListener>) {
        let (stream, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(stream);
        let mut request = Vec::new();
        reader.read_until(b'\n', &mut request).await.unwrap();
        let id = serde_json::from_slice::<Value>(&request).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        reader
            .write_all(
                format!(
                    "{{\"id\":\"{id}\",\"result\":{{\"type\":\"pong\",\"protocol\":{BUNDLED_PROTOCOL},\"version\":\"0.8.2\"}}}}\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
    }

    async fn accept_zero_bytes(listener: UnixListener) -> Vec<u8> {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut bytes))
            .await
            .expect("mismatched runtime stream closes promptly")
            .unwrap();
        bytes
    }

    async fn capture_at(path: &std::path::Path) -> EndpointContinuityToken {
        let listener = tokio::net::UnixListener::bind(path).unwrap();
        let endpoint = HerdrSocketClient::new(path).observed_token().await.unwrap();
        drop(listener);
        endpoint
    }

    fn pong() -> Value {
        serde_json::json!({ "type": "pong", "protocol": BUNDLED_PROTOCOL, "version": "0.8.2" })
    }

    #[tokio::test]
    async fn pong_identity_binds_to_endpoint_incarnation() {
        let temp = tempfile::TempDir::new().unwrap();
        let endpoint = capture_at(&temp.path().join("herdr.sock")).await;
        let (identity, version) =
            identity_from_ping_result("/owned/socket".to_owned(), &endpoint, &pong())
                .expect("protocol 20 pong has type, version, and protocol");

        assert_eq!(identity.discovery_key.as_str(), "/owned/socket");
        assert_eq!(version.as_str(), "0.8.2");
        assert_eq!(
            identity.live_server_id.as_str(),
            endpoint.live_server_id(BUNDLED_PROTOCOL, version.as_str())
        );
    }

    #[tokio::test]
    async fn identical_endpoint_observation_is_not_a_continuity_claim() {
        let temp = tempfile::TempDir::new().unwrap();
        let endpoint = capture_at(&temp.path().join("herdr.sock")).await;

        assert!(
            !endpoint.proven_replacement(&endpoint),
            "an equal observation is deliberately inconclusive, not continuity proof"
        );
    }

    #[tokio::test]
    async fn guarded_runtime_rejects_rebound_endpoint_before_write() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener_a = Arc::new(UnixListener::bind(&path).unwrap());
        let server_a = tokio::spawn(answer_ping(Arc::clone(&listener_a)));
        let client = Arc::new(HerdrSocketClient::new(path.clone()));
        let (identity, expected, server_version) =
            establish_live_identity(client.as_ref()).await.unwrap();
        server_a.await.unwrap();
        let runtime = HerdrRuntime::from_parts(
            client,
            test_schema(),
            false,
            identity,
            expected,
            server_version,
        );

        std::fs::remove_file(&path).unwrap();
        let listener_b = UnixListener::bind(&path).unwrap();
        let server_b = tokio::spawn(accept_zero_bytes(listener_b));
        let error = runtime
            .invoke_response("ping", Value::Object(Map::new()))
            .await
            .expect_err("runtime rejects replacement before request write");

        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
        assert!(
            error
                .message
                .contains("replaced before the request was sent")
        );
        let after = runtime
            .invoke_response("ping", Value::Object(Map::new()))
            .await
            .expect_err("standalone runtime admission stays closed after replacement");
        assert_eq!(after.kind, AdapterErrorKind::Shutdown);
        assert!(
            server_b.await.unwrap().is_empty(),
            "guarded runtime sends zero request bytes to the replacement"
        );
        drop(listener_a);
    }

    #[tokio::test]
    async fn retirement_linearizes_against_reserved_and_future_admission() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let server = tokio::spawn(answer_ping(Arc::clone(&listener)));
        let client = Arc::new(HerdrSocketClient::new(path));
        let (identity, expected, server_version) =
            establish_live_identity(client.as_ref()).await.unwrap();
        server.await.unwrap();
        let runtime = HerdrRuntime::from_parts(
            client,
            test_schema(),
            false,
            identity,
            expected,
            server_version,
        );

        let accepted = runtime
            .try_run_ordered(|_| async { 7_u8 })
            .expect("work accepted before retirement");
        let reserved = runtime
            .send_queue
            .try_reserve()
            .expect("capacity reserved before retirement");
        runtime.retire_send_queue();
        let raced = submit_ordered(reserved, |_| async { 8_u8 })
            .expect_err("a pre-retirement reservation cannot submit after retirement");
        assert_eq!(raced.kind, AdapterErrorKind::Shutdown);
        let after = runtime
            .try_run_ordered(|_| async { 9_u8 })
            .expect_err("future admission remains closed after retirement");
        assert_eq!(after.kind, AdapterErrorKind::Shutdown);
        assert_eq!(
            HerdrRuntime::await_ordered(accepted).await.unwrap(),
            7,
            "accepted-before-retirement work remains owned"
        );
        tokio::time::timeout(Duration::from_secs(2), runtime.shutdown_send_queue())
            .await
            .expect("retired owner joins within two seconds")
            .expect("retired owner exits normally");
        drop(listener);
    }
    #[tokio::test]
    async fn cancellation_while_internal_queue_is_full_never_accepts_work() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let server = tokio::spawn(answer_ping(Arc::clone(&listener)));
        let client = Arc::new(HerdrSocketClient::new(path));
        let (identity, expected, server_version) =
            establish_live_identity(client.as_ref()).await.unwrap();
        server.await.unwrap();
        let runtime = HerdrRuntime::from_parts(
            client,
            test_schema(),
            false,
            identity,
            expected,
            server_version,
        );
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let active = runtime
            .try_run_ordered({
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                move |_| async move {
                    entered.notify_one();
                    release.notified().await;
                }
            })
            .expect("active transaction is accepted");
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("queue owner starts the active transaction");

        let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut accepted = Vec::new();
        loop {
            let ran = Arc::clone(&ran);
            match runtime.try_run_ordered(move |_| async move {
                ran.fetch_add(1, Ordering::SeqCst);
            }) {
                Ok(receiver) => accepted.push(receiver),
                Err(error) => {
                    assert_eq!(error.kind, AdapterErrorKind::Unavailable);
                    assert!(error.message.contains("queue is full"));
                    break;
                }
            }
        }
        assert!(!accepted.is_empty(), "bounded queue accepts waiting work");

        let cancelled_ran = Arc::new(AtomicBool::new(false));
        let mut cancelled = Box::pin(runtime.run_ordered({
            let cancelled_ran = Arc::clone(&cancelled_ran);
            move |_| async move {
                cancelled_ran.store(true, Ordering::SeqCst);
            }
        }));
        std::future::poll_fn(|context| match cancelled.as_mut().poll(context) {
            std::task::Poll::Pending => std::task::Poll::Ready(()),
            std::task::Poll::Ready(_) => {
                panic!("full-queue work completed before capacity became available")
            }
        })
        .await;
        drop(cancelled);

        let accepted_count = accepted.len();
        release.notify_one();
        HerdrRuntime::await_ordered(active)
            .await
            .expect("active transaction completes");
        for receiver in accepted {
            HerdrRuntime::await_ordered(receiver)
                .await
                .expect("accepted waiting transaction completes");
        }
        assert_eq!(ran.load(Ordering::SeqCst), accepted_count);
        assert!(
            !cancelled_ran.load(Ordering::SeqCst),
            "dropping a reserve waiter before admission sends no work to the owner"
        );
        runtime
            .shutdown_send_queue()
            .await
            .expect("queue owner exits normally");
        drop(listener);
    }

    #[tokio::test]
    async fn panicking_transaction_has_typed_terminal_and_owner_continues() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("herdr.sock");
        let listener = Arc::new(UnixListener::bind(&path).unwrap());
        let server = tokio::spawn(answer_ping(Arc::clone(&listener)));
        let client = Arc::new(HerdrSocketClient::new(path));
        let (identity, expected, server_version) =
            establish_live_identity(client.as_ref()).await.unwrap();
        server.await.unwrap();
        let runtime = HerdrRuntime::from_parts(
            client,
            test_schema(),
            false,
            identity,
            expected,
            server_version,
        );

        let panicked: Result<(), AdapterError> = runtime
            .run_ordered(|_| async {
                panic!("deterministic queued transaction panic");
            })
            .await;
        let error = panicked.expect_err("panicking work gets a typed terminal");
        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
        assert!(error.message.contains("panicked internally"));
        assert_eq!(
            runtime
                .run_ordered(|_| async { 42_u8 })
                .await
                .expect("owner continues after isolated transaction panic"),
            42
        );
        tokio::time::timeout(Duration::from_secs(2), runtime.shutdown_send_queue())
            .await
            .expect("queue join returns within two seconds")
            .expect("queue owner exits normally after transaction panic");
        drop(listener);
    }

    /// The timeout path owns one concrete nonterminating schema child, kills it, and reaps it
    /// before returning. The short injected bounds exercise the production algorithm without a
    /// ten-second wall-clock test.
    #[cfg(unix)]
    #[tokio::test]
    async fn schema_timeout_kills_and_reaps_the_owned_child() {
        use nix::{
            errno::Errno,
            sys::wait::{WaitPidFlag, waitpid},
            unistd::Pid,
        };
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::TempDir::new().unwrap();
        let script = temp.path().join("herdr");
        let pid_file = temp.path().join("pid");
        std::fs::write(
            &script,
            "#!/bin/sh\nprintf '%s' \"$$\" > \"$(dirname \"$0\")/pid\"\nexec sleep 3600\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let worker = tokio::spawn({
            let script = script.clone();
            async move {
                runtime_schema_with_timeouts(
                    &script,
                    Duration::from_millis(50),
                    Duration::from_secs(1),
                )
                .await
            }
        });
        let mut child_pid = None;
        for _ in 0..100 {
            if let Ok(pid) = std::fs::read_to_string(&pid_file)
                && let Ok(pid) = pid.trim().parse::<i32>()
            {
                child_pid = Some(pid);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let pid = Pid::from_raw(
            child_pid.expect("schema child writes its PID marker before its timeout"),
        );

        let error = worker
            .await
            .expect("schema worker task does not panic")
            .expect_err("a nonterminating schema child must time out");
        assert_eq!(error.kind, AdapterErrorKind::Unavailable);
        assert!(
            error.message.contains("api schema --json timed out"),
            "the timeout must only return after the owned child is reaped"
        );
        assert_eq!(
            waitpid(pid, Some(WaitPidFlag::WNOHANG)),
            Err(Errno::ECHILD),
            "the owned schema child must be reaped rather than left as a zombie"
        );
    }
}
