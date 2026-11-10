//! Cancellation-safe bounded Redis transport shared by input/output stores.
//! Original Dispatcharr application attribution remains in ATTRIBUTION.md.
use crate::Error;
use redis::{aio::MultiplexedConnection, AsyncConnectionConfig, Script};
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
struct Inner {
    connection: MultiplexedConnection,
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
        let connection = client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new().set_connection_timeout(config.command_timeout),
            )
            .await
            .map_err(OperationError::redis)?;
        Ok(Self {
            inner: Arc::new(Inner {
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
    async fn command_inner<T: redis::FromRedisValue + Send + 'static, G: Send + 'static>(
        &self,
        source: &str,
        keys: &[String],
        args: &[&[u8]],
        guard: G,
    ) -> Result<(T, G), OperationError> {
        let operation = match self.inner.operations.clone().try_acquire_owned() {
            Ok(permit) => permit,
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
                .map_err(|_| OperationError::Backend)?
            }
        };
        let script = Script::new(source);
        let keys = keys.to_vec();
        let args: Vec<Vec<u8>> = args.iter().map(|value| value.to_vec()).collect();
        let mut connection = self.inner.connection.clone();
        let task = tokio::spawn(async move {
            let mut invocation = script.prepare_invoke();
            for key in keys {
                invocation.key(key);
            }
            for arg in args {
                invocation.arg(arg);
            }
            let result: Result<T, redis::RedisError> =
                invocation.invoke_async(&mut connection).await;
            (result, guard, operation)
        });
        let (result, guard, _operation) =
            tokio::time::timeout(self.inner.config.command_timeout, task)
                .await
                .map_err(|_| OperationError::Backend)?
                .map_err(|_| OperationError::Server)?;
        result
            .map(|value| (value, guard))
            .map_err(OperationError::redis)
    }
}
