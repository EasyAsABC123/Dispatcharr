//! Retained Source epg_settings cache policy, separate from the shared UI cache.
//! Original Dispatcharr authors retain all application/algorithm credit.
use crate::{ImportError, SwapPolicy, SwapPolicyContext, SwapPolicyFuture, SwapPolicyProvider};
use chrono::{DateTime, Utc};
use dispatcharr_output_store::transport::{ClassifiedError, RedisTransport, TransportConfig};
use dispatcharr_store::{
    decode_source_cache, encode_source_cache, source_cache_values_equal, source_epg_history_policy,
    Store,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
const GROUP: &str = ":1:coresettings:group:epg_settings";
const VERSION: &str = ":1:coresettings:groupver:epg_settings";
const MAX_BYTES: usize = 1024 * 1024;
const GET: &str = r#"
local kind=redis.call('TYPE',KEYS[1]).ok
if kind~='none' and kind~='string' then return {-1,false} end
if redis.call('STRLEN',KEYS[1])>1048576 then return {-2,false} end
local value=redis.call('GET',KEYS[1])
if not value then return {0,false} end
return {1,value}
"#;
const SET: &str = "return redis.call('SET',KEYS[1],ARGV[1],'EX',300)";
struct PolicyWork {
    _permit: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Connection {
    generation: u64,
    transport: RedisTransport,
}
struct Connections {
    next: u64,
    current: Option<Connection>,
}
/// A clock is sampled at Source's retention boundary, before cache/policy I/O.
pub type PolicyClock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
pub struct CachedSwapPolicy {
    store: Store,
    redis_url: Option<String>,
    connection: Mutex<Connections>,
    transport_config: TransportConfig,
    clock: PolicyClock,
    admission: Arc<Semaphore>,
}
enum Read {
    Value(Option<Value>),
    Backend,
}
impl CachedSwapPolicy {
    /// None explicitly bypasses cache, matching DISPATCHARR_SKIP_REDIS_CACHE.
    /// Connections are acquired lazily inside an already-admitted physical job.
    pub fn new(store: Store, redis_url: Option<String>) -> Self {
        Self::with_clock(store, redis_url, Arc::new(Utc::now))
    }
    pub fn with_clock(store: Store, redis_url: Option<String>, clock: PolicyClock) -> Self {
        Self {
            store,
            redis_url,
            connection: Mutex::new(Connections {
                next: 1,
                current: None,
            }),
            transport_config: TransportConfig {
                max_operations: 2,
                max_queued_operations: 4,
                command_timeout: Duration::from_secs(2),
            },
            clock,
            admission: Arc::new(Semaphore::new(4)),
        }
    }
    /// Includes commands whose logical waiter timed out but whose physical
    /// reply still retains the caller guard and this adapter's admission.
    pub fn physical_operations(&self) -> usize {
        4 - self.admission.available_permits()
    }
    /// Explicit bounded command deadline, useful for caller-supervised I/O.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, ImportError> {
        if timeout < Duration::from_millis(1) || timeout > Duration::from_secs(5) {
            return Err(ImportError::Capacity("policy cache timeout"));
        }
        self.transport_config.command_timeout = timeout;
        Ok(self)
    }
    async fn transport(
        &self,
        context: &SwapPolicyContext,
    ) -> Result<Option<(u64, RedisTransport)>, ImportError> {
        context.ensure_active()?;
        let Some(url) = self.redis_url.as_ref() else {
            return Ok(None);
        };
        let mut connections = self.connection.lock().await;
        if let Some(connection) = connections.current.as_ref() {
            return Ok(Some((connection.generation, connection.transport.clone())));
        }
        context.ensure_active()?;
        match RedisTransport::connect_classified(url, self.transport_config.clone()).await {
            Ok(transport) => {
                let generation = connections.next;
                connections.next = connections.next.wrapping_add(1);
                connections.current = Some(Connection {
                    generation,
                    transport: transport.clone(),
                });
                Ok(Some((generation, transport)))
            }
            Err(ClassifiedError::Backend) => Ok(None),
            Err(ClassifiedError::Capacity) => Err(ImportError::Capacity("policy cache admission")),
            Err(_) => Err(ImportError::Parser),
        }
    }
    async fn invalidate(&self, generation: u64) {
        let mut connections = self.connection.lock().await;
        if connections
            .current
            .as_ref()
            .is_some_and(|value| value.generation == generation)
        {
            connections.current = None;
        }
    }
    async fn read(&self, key: &str, context: &SwapPolicyContext) -> Result<Read, ImportError> {
        let Some((generation, transport)) = self.transport(context).await? else {
            return Ok(Read::Backend);
        };
        context.ensure_active()?;
        let result = transport
            .command_classified::<(i64, Option<Vec<u8>>), _>(
                GET,
                &[key.into()],
                &[],
                context.retained.clone(),
            )
            .await;
        match result {
            Ok(((0, _), _)) => Ok(Read::Value(None)),
            Ok(((1, Some(raw)), _)) => {
                if raw.len() > MAX_BYTES {
                    return Err(ImportError::Capacity("policy cache value"));
                }
                decode_source_cache(&raw)
                    .map(|value| Read::Value(Some(value)))
                    .ok_or(ImportError::Parser)
            }
            Ok(((-2, _), _)) => Err(ImportError::Capacity("policy cache value")),
            Ok(_) => Err(ImportError::Parser),
            Err(ClassifiedError::Backend) => {
                self.invalidate(generation).await;
                Ok(Read::Backend)
            }
            Err(ClassifiedError::Capacity) => Err(ImportError::Capacity("policy cache admission")),
            Err(_) => Err(ImportError::Parser),
        }
    }
    async fn database(&self, context: &SwapPolicyContext) -> Result<Value, ImportError> {
        context.ensure_active()?;
        let store = self.store.clone();
        let retained = context.retained.clone();
        // The private owned task retains the job/lock guard through the actual
        // SQL reply even if a direct caller drops this policy future.
        let value=tokio::spawn(async move{
            let _retained=retained;
            let row=sqlx::query("SELECT octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=1048576 THEN value ELSE NULL END AS value FROM core_coresettings WHERE key='epg_settings'")
                .fetch_optional(store.pool()).await?;
            let value=if let Some(row)=row{
                if row.try_get::<Option<i32>,_>("bytes")?.unwrap_or(0)>MAX_BYTES as i32{return Err(ImportError::Capacity("EPG settings SQL value"));}
                row.try_get::<Option<Value>,_>("value")?.unwrap_or(Value::Null)
            }else{Value::Null};
            Ok(if value.as_object().is_some_and(|map|!map.is_empty()){value}else{defaults()})
        }).await.map_err(|_|ImportError::Worker)??;
        context.ensure_active()?;
        Ok(value)
    }
    async fn fill(&self, value: &Value, context: &SwapPolicyContext) -> Result<(), ImportError> {
        let Some((generation, transport)) = self.transport(context).await? else {
            return Ok(());
        };
        context.ensure_active()?;
        let encoded =
            encode_source_cache(value).ok_or(ImportError::Capacity("policy cache encoding"))?;
        if encoded.len() > MAX_BYTES {
            return Err(ImportError::Capacity("policy cache encoding"));
        }
        match transport
            .command_classified::<String, _>(
                SET,
                &[GROUP.into()],
                &[encoded.as_slice()],
                context.retained.clone(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(ClassifiedError::Backend) => {
                self.invalidate(generation).await;
                Ok(())
            }
            Err(ClassifiedError::Capacity) => Err(ImportError::Capacity("policy cache admission")),
            Err(_) => Err(ImportError::Parser),
        }
    }
    async fn group(&self, context: &SwapPolicyContext) -> Result<Value, ImportError> {
        match self.read(GROUP, context).await? {
            Read::Value(Some(value)) if value.is_object() => return Ok(value),
            Read::Backend => return self.database(context).await,
            _ => {}
        }
        let before = match self.read(VERSION, context).await? {
            Read::Value(value) => value.unwrap_or(Value::Null),
            Read::Backend => return self.database(context).await,
        };
        let value = self.database(context).await?;
        let after = match self.read(VERSION, context).await? {
            Read::Value(value) => value.unwrap_or(Value::Null),
            Read::Backend => return Ok(value),
        };
        if source_cache_values_equal(&before, &after) {
            self.fill(&value, context).await?;
        }
        Ok(value)
    }
}
impl SwapPolicyProvider for CachedSwapPolicy {
    fn resolve<'a>(&'a self, context: SwapPolicyContext) -> SwapPolicyFuture<'a> {
        Box::pin(async move {
            context.ensure_active()?;
            let permit = self
                .admission
                .clone()
                .try_acquire_owned()
                .map_err(|_| ImportError::Capacity("policy physical work"))?;
            let context = SwapPolicyContext {
                cancelled: context.cancelled,
                retained: Arc::new(PolicyWork {
                    _caller: context.retained,
                    _permit: permit,
                }),
            };
            let now = (self.clock)();
            let value = self.group(&context).await?;
            context.ensure_active()?;
            let history = source_epg_history_policy(&value).map_err(|_| ImportError::Parser)?;
            Ok(SwapPolicy { now, history })
        })
    }
}
fn defaults() -> Value {
    json!({"epg_match_mode":"default","epg_match_ignore_prefixes":[],"epg_match_ignore_suffixes":[],"epg_match_ignore_custom":[],"epg_history_min_days":1,"epg_history_max_days":30,"epg_default_prev_days":0,"epg_default_days":0})
}
