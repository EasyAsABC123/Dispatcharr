//! Cancellation-safe bounded Redis transport shared by input/output stores.
//! Original Dispatcharr application attribution remains in ATTRIBUTION.md.
#[path = "bounded_subscription.rs"]
mod bounded_subscription;
#[path = "shared_subscription.rs"]
mod shared_subscription;
use crate::Error;
#[path = "result_diagnostics.rs"]
mod result_diagnostics;
pub use bounded_subscription::{
    Error as SubscriptionError, Receipt as SubscriptionReceipt, Subscription as BoundedSubscription,
};
use redis::{
    aio::{ConnectionManager, ConnectionManagerConfig},
    Script,
};
pub use result_diagnostics::CommandDiagnostic;
pub use shared_subscription::{
    Confirmation as SubscriptionConfirmation, LogicalSubscription, Message as SubscriptionMessage,
    SharedSubscription,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub max_operations: usize,
    pub max_queued_operations: usize,
    pub command_timeout: Duration,
}
#[derive(Clone)]
pub struct RedisTransport {
    inner: Arc<Inner>,
}
/// Distinguishes Source cache fallbacks from server/data failures without
/// exposing backend error text (which may contain keys or credentials).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClassifiedError {
    Configuration,
    Capacity,
    Backend,
    Server,
}
enum OperationError {
    Configuration,
    Capacity,
    AdmissionTimeout,
    Backend,
    Server,
    Redis { backend: bool, corrupt: bool },
}
impl OperationError {
    fn redis(error: redis::RedisError) -> Self {
        Self::Redis {
            backend: error.is_io_error() || error.is_timeout() || error.is_connection_dropped(),
            corrupt: error.to_string().contains("CORRUPT"),
        }
    }
    fn classified(self) -> ClassifiedError {
        match self {
            Self::Configuration => ClassifiedError::Configuration,
            Self::Capacity | Self::AdmissionTimeout => ClassifiedError::Capacity,
            Self::Backend | Self::Redis { backend: true, .. } => ClassifiedError::Backend,
            Self::Server | Self::Redis { backend: false, .. } => ClassifiedError::Server,
        }
    }
    fn legacy(self) -> Error {
        match self {
            Self::Configuration => Error::Configuration,
            Self::Capacity => Error::Capacity,
            Self::Redis { corrupt: true, .. } => Error::Corrupt,
            _ => Error::Unavailable,
        }
    }
}
enum CommandScript {
    OneOff(Script),
    ResultWithPreflight(Script),
    PublicationWithPreflight(Script),
    Prepared(Arc<Script>),
}
impl CommandScript {
    fn script(&self) -> &Script {
        match self {
            Self::OneOff(script)
            | Self::ResultWithPreflight(script)
            | Self::PublicationWithPreflight(script) => script,
            Self::Prepared(script) => script,
        }
    }
}
struct Inner {
    shared_subscription: tokio::sync::Mutex<Option<SharedSubscription>>,
    endpoint: redis::ConnectionInfo,
    connection: ConnectionManager,
    operations: Arc<Semaphore>,
    queued: Arc<Semaphore>,
    config: TransportConfig,
}
impl RedisTransport {
    pub async fn connect(url: &str, config: TransportConfig) -> Result<Self, Error> {
        Self::connect_inner(url, config)
            .await
            .map_err(|error| match error {
                OperationError::Configuration => Error::Configuration,
                _ => Error::Unavailable,
            })
    }
    pub async fn connect_classified(
        url: &str,
        config: TransportConfig,
    ) -> Result<Self, ClassifiedError> {
        Self::connect_inner(url, config)
            .await
            .map_err(OperationError::classified)
    }
    async fn connect_inner(url: &str, config: TransportConfig) -> Result<Self, OperationError> {
        if config.max_operations == 0
            || config.max_operations > 65536
            || config.max_queued_operations == 0
            || config.max_queued_operations > 65536
            || config.command_timeout.is_zero()
        {
            return Err(OperationError::Configuration);
        }
        let client = redis::Client::open(url).map_err(|_| OperationError::Configuration)?;
        // Share one bounded reconnection future across clones. The manager
        // returns a failing command's error; it never retries that command on
        // the replacement connection. Keep response_timeout unset so a logical
        // deadline cannot free admission/caller before a real reply/disconnect.
        let connection = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new()
                    .set_number_of_retries(0)
                    .set_connection_timeout(config.command_timeout),
            )
            .await
            .map_err(OperationError::redis)?;
        Ok(Self {
            inner: Arc::new(Inner {
                shared_subscription: tokio::sync::Mutex::new(None),
                endpoint: client.get_connection_info().clone(),
                connection,
                operations: Arc::new(Semaphore::new(config.max_operations)),
                queued: Arc::new(Semaphore::new(config.max_queued_operations)),
                config,
            }),
        })
    }
    /// Caller cancellation or deadline detaches a bounded supervised operation.
    /// The task retains admission until Redis actually responds or disconnects;
    /// dropping a timed-out multiplexed response future would release too early.
    pub async fn command<T: redis::FromRedisValue + Send + 'static, G: Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), Error> {
        self.command_inner(source, keys, args, guard)
            .await
            .map_err(OperationError::legacy)
    }
    /// Same bounded/supervised physical operation as command(), with only the
    /// failure classification changed. Timeouts detach rather than releasing
    /// the underlying operation/guard while Redis still has a queued response.
    pub async fn command_classified<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), ClassifiedError> {
        self.command_inner(source, keys, args, guard)
            .await
            .map_err(OperationError::classified)
    }
    /// Optional fixed-vocabulary diagnostics over the SAME owned command.
    /// No timeout/retry/admission/result semantics change; raw errors stay private.
    pub async fn command_classified_with_diagnostic<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
        diagnostic: Option<CommandDiagnostic>,
    ) -> Result<(T, G), ClassifiedError> {
        self.command_diagnostic_inner(
            CommandScript::OneOff(Script::new(source)),
            keys,
            args,
            guard,
            diagnostic,
        )
        .await
    }
    /// Celery result persistence health check before the unchanged one mutation.
    /// Only the fixed-response read-only PING may repeat after a settled dropped
    /// connection. The owned permit/guard/deadline covers both checks and script.
    pub async fn command_result_classified_with_diagnostic<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
        diagnostic: Option<CommandDiagnostic>,
    ) -> Result<(T, G), ClassifiedError> {
        self.command_diagnostic_inner(
            CommandScript::ResultWithPreflight(Script::new(source)),
            keys,
            args,
            guard,
            diagnostic,
        )
        .await
    }
    /// Root-artwork publication socket readiness before its unchanged single
    /// mutation. Only the read-only PING may repeat after a settled disconnect;
    /// admission, owner, timeout and physical completion match result preflight.
    pub async fn command_publication_classified_with_diagnostic<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
        diagnostic: Option<CommandDiagnostic>,
    ) -> Result<(T, G), ClassifiedError> {
        self.command_diagnostic_inner(
            CommandScript::PublicationWithPreflight(Script::new(source)),
            keys,
            args,
            guard,
            diagnostic,
        )
        .await
    }
    async fn command_diagnostic_inner<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        script: CommandScript,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
        diagnostic: Option<CommandDiagnostic>,
    ) -> Result<(T, G), ClassifiedError> {
        let trace = diagnostic.map(result_diagnostics::Trace::new);
        if let Some(t) = &trace {
            t.emit(
                result_diagnostics::Stage::Admission,
                result_diagnostics::Kind::None,
            );
        }
        let operation = match self.acquire_operation().await {
            Ok(operation) => operation,
            Err(error) => {
                if let Some(t) = &trace {
                    let kind = match &error {
                        OperationError::Capacity | OperationError::AdmissionTimeout => {
                            result_diagnostics::Kind::Capacity
                        }
                        OperationError::Configuration => result_diagnostics::Kind::Configuration,
                        _ => result_diagnostics::Kind::Unobserved,
                    };
                    t.emit(result_diagnostics::Stage::AdmissionRejected, kind);
                }
                return Err(error.classified());
            }
        };
        self.command_admitted_observed(script, keys, args, guard, operation, trace)
            .await
            .map_err(OperationError::classified)
    }
    /// Reuse a caller-owned compiled script without a transport-wide cache.
    /// Admission, physical response ownership and error classification are the
    /// same as command_classified(); Redis still handles NOSCRIPT normally.
    pub async fn command_script_classified<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        script: Arc<Script>,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), ClassifiedError> {
        let operation = self
            .acquire_operation()
            .await
            .map_err(OperationError::classified)?;
        self.command_admitted(
            CommandScript::Prepared(script),
            keys,
            args,
            guard,
            operation,
        )
        .await
        .map_err(OperationError::classified)
    }
    async fn command_inner<T: redis::FromRedisValue + Send + 'static, G: Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), OperationError> {
        let operation = self.acquire_operation().await?;
        self.command_admitted(
            CommandScript::OneOff(Script::new(source)),
            keys,
            args,
            guard,
            operation,
        )
        .await
    }
    async fn command_admitted<T: redis::FromRedisValue + Send + 'static, G: Send + 'static>(
        &self,
        script: CommandScript,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
        operation: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(T, G), OperationError> {
        self.command_admitted_observed(script, keys, args, guard, operation, None)
            .await
    }
    async fn command_admitted_observed<
        T: redis::FromRedisValue + Send + 'static,
        G: Send + 'static,
    >(
        &self,
        script: CommandScript,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
        operation: tokio::sync::OwnedSemaphorePermit,
        trace: Option<Arc<result_diagnostics::Trace>>,
    ) -> Result<(T, G), OperationError> {
        let keys = keys.to_vec();
        let args: Vec<Vec<u8>> = args.iter().map(|value| value.to_vec()).collect();
        let mut connection = self.inner.connection.clone();
        let physical_trace = trace.clone();
        let task = tokio::spawn(async move {
            let mut observed = result_diagnostics::PhysicalObservation::start(physical_trace);
            let mut invocation = script.script().prepare_invoke();
            for key in keys {
                invocation.key(key);
            }
            for arg in args {
                invocation.arg(arg);
            }
            let result: Result<T, redis::RedisError> = async {
                if matches!(
                    &script,
                    CommandScript::ResultWithPreflight(_)
                        | CommandScript::PublicationWithPreflight(_)
                ) {
                    result_preflight(&mut connection).await?;
                }
                // Do not retry a script after any error: its mutation/reply may
                // already have happened, including a disconnect at this point.
                invocation.invoke_async(&mut connection).await
            }
            .await;
            observed.returned(&result);
            (result, guard, operation)
        });
        await_owned_command(task, self.inner.config.command_timeout, trace).await
    }
    async fn acquire_operation(&self) -> Result<tokio::sync::OwnedSemaphorePermit, OperationError> {
        match self.inner.operations.clone().try_acquire_owned() {
            Ok(permit) => Ok(permit),
            Err(_) => {
                let _queued = self
                    .inner
                    .queued
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| OperationError::Capacity)?;
                tokio::time::timeout(
                    self.inner.config.command_timeout,
                    self.inner.operations.clone().acquire_owned(),
                )
                .await
                .map_err(|_| OperationError::AdmissionTimeout)?
                .map_err(|_| OperationError::Backend)
            }
        }
    }
    /// Same manager and actual operation/caller lifetime; raw errors remain
    /// private to dependency response parsing, never diagnostic tracing.
    pub async fn dependency_pipeline<G: Send + 'static>(
        &self,
        pipeline: redis::Pipeline,
        offset: usize,
        count: usize,
        guard: G,
    ) -> Result<(Result<Vec<redis::Value>, redis::RedisError>, G), ClassifiedError> {
        use redis::aio::ConnectionLike;
        if count == 0 || count > 257 || offset > 257 {
            return Err(ClassifiedError::Configuration);
        }
        let operation = self
            .acquire_operation()
            .await
            .map_err(OperationError::classified)?;
        let mut connection = self.inner.connection.clone();
        let task = tokio::spawn(async move {
            let result = connection
                .req_packed_commands(&pipeline, offset, count)
                .await;
            (result, guard, operation)
        });
        let (result, guard, _operation) =
            tokio::time::timeout(self.inner.config.command_timeout, task)
                .await
                .map_err(|_| ClassifiedError::Backend)?
                .map_err(|_| ClassifiedError::Server)?;
        Ok((result, guard))
    }
    /// Lazy SINGLE upstream for this same result transport. Initializer is
    /// supervised so caller loss cannot leave a second orphan connection or an
    /// unrecorded budget reservation. Logical reads consume no additional slot.
    pub async fn dependency_subscriptions(
        &self,
        host: Arc<dyn Send + Sync>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<SharedSubscription, SubscriptionError> {
        let transport = self.clone();
        tokio::spawn(async move {
            let _caller = caller.clone();
            let mut cached = transport.inner.shared_subscription.lock().await;
            if let Some(current) = cached.as_ref() {
                if !current.closed() {
                    return Ok(current.clone());
                }
                // Never overlap a replacement socket with an unsettled old one.
                current
                    .close(transport.inner.config.command_timeout)
                    .await?;
            }
            if transport.inner.config.max_operations < 2 {
                return Err(SubscriptionError::Capacity);
            }
            let physical = transport.open_dependency_subscription(host, caller).await?;
            let hub = SharedSubscription::spawn(physical, transport.inner.config.command_timeout);
            *cached = Some(hub.clone());
            Ok(hub)
        })
        .await
        .map_err(|_| SubscriptionError::Closed)?
    }
    pub async fn close_dependency_subscriptions(
        &self,
        timeout: Duration,
    ) -> Result<Option<SubscriptionReceipt>, SubscriptionError> {
        let cached = self.inner.shared_subscription.lock().await;
        match cached.as_ref() {
            Some(hub) => hub.close(timeout).await.map(Some),
            None => Ok(None),
        }
    }
    pub fn configured_endpoint(&self) -> &redis::ConnectionInfo {
        &self.inner.endpoint
    }
    /// Exactly one SHARED upstream subscriber must be retained by the Jobs
    /// hub; never one permanent connection/slot per Source host. Admission is
    /// from this same existing transport so configured resource bounds hold.
    pub async fn open_dependency_subscription(
        &self,
        host: Arc<dyn Send + Sync>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<BoundedSubscription, SubscriptionError> {
        let transport = self.clone();
        tokio::spawn(async move {
            let operation = transport
                .acquire_operation()
                .await
                .map_err(|_| SubscriptionError::Capacity)?;
            BoundedSubscription::connect(
                transport.inner.endpoint.clone(),
                transport.inner.config.command_timeout,
                operation,
                host,
                caller,
            )
            .await
        })
        .await
        .map_err(|_| SubscriptionError::Closed)?
    }
}

// redis-rs swaps its shared manager connection AFTER returning the dropped
// command's error. Repeating one settled read-only PING waits that replacement;
// no owner/admission is released and no result mutation has begun yet.
async fn result_preflight(connection: &mut ConnectionManager) -> redis::RedisResult<()> {
    async fn ping(connection: &mut ConnectionManager) -> redis::RedisResult<()> {
        let reply: String = redis::cmd("PING").query_async(connection).await?;
        if reply != "PONG" {
            return Err(redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "Unexpected result health response",
            )));
        }
        Ok(())
    }
    match ping(connection).await {
        Err(error) if error.is_connection_dropped() => ping(connection).await,
        outcome => outcome,
    }
}

// Existing detached command continuation, shared by production and the finite
// physical-boundary control. Timeout drops only the JoinHandle observer.
async fn await_owned_command<T: Send + 'static, G: Send + 'static>(
    task: tokio::task::JoinHandle<(redis::RedisResult<T>, G, tokio::sync::OwnedSemaphorePermit)>,
    timeout: Duration,
    trace: Option<Arc<result_diagnostics::Trace>>,
) -> Result<(T, G), OperationError> {
    let mut observer = result_diagnostics::LogicalObservation::new(trace.clone());
    let outcome = tokio::time::timeout(timeout, task).await;
    observer.finished = true;
    let (result, guard, _operation) = match outcome {
        Err(_) => {
            if let Some(t) = &trace {
                t.emit(
                    result_diagnostics::Stage::LogicalDeadline,
                    result_diagnostics::Kind::LogicalTimeout,
                );
            }
            return Err(OperationError::Backend);
        }
        Ok(Err(_)) => {
            if let Some(t) = &trace {
                t.emit(
                    result_diagnostics::Stage::SupervisorFailed,
                    result_diagnostics::Kind::SupervisorFailure,
                );
            }
            return Err(OperationError::Server);
        }
        Ok(Ok(tuple)) => tuple,
    };
    result
        .map(|value| (value, guard))
        .map_err(OperationError::redis)
}
