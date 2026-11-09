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
struct Inner {
    connection: MultiplexedConnection,
    operations: Arc<Semaphore>,
    queued: Arc<Semaphore>,
    config: TransportConfig,
}
impl RedisTransport {
    pub async fn connect(url: &str, config: TransportConfig) -> Result<Self, Error> {
        if config.max_operations == 0
            || config.max_operations > 65536
            || config.max_queued_operations == 0
            || config.max_queued_operations > 65536
            || config.command_timeout.is_zero()
        {
            return Err(Error::Configuration);
        }
        let client = redis::Client::open(url).map_err(|_| Error::Configuration)?;
        let connection = client
            .get_multiplexed_async_connection_with_config(
                &AsyncConnectionConfig::new().set_connection_timeout(config.command_timeout),
            )
            .await
            .map_err(|_| Error::Unavailable)?;
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
        let operation = match self.inner.operations.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let _queued = self
                    .inner
                    .queued
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Capacity)?;
                tokio::time::timeout(
                    self.inner.config.command_timeout,
                    self.inner.operations.clone().acquire_owned(),
                )
                .await
                .map_err(|_| Error::Unavailable)?
                .map_err(|_| Error::Unavailable)?
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
                .map_err(|_| Error::Unavailable)?
                .map_err(|_| Error::Unavailable)?;
        result.map(|value| (value, guard)).map_err(|error| {
            if error.to_string().contains("CORRUPT") {
                Error::Corrupt
            } else {
                Error::Unavailable
            }
        })
    }
}
