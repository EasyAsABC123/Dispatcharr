//! Retained Source epg_settings cache policy, separate from the shared UI cache.
//! Original Dispatcharr authors retain all application/algorithm credit.
use crate::{ImportError, SwapPolicy, SwapPolicyContext, SwapPolicyFuture, SwapPolicyProvider};
use chrono::{DateTime, Utc};
use dispatcharr_output_store::transport::{ClassifiedError, RedisTransport, TransportConfig};
use dispatcharr_store::{
    decode_source_cache, encode_source_cache, source_cache_values_equal, source_epg_history_policy,
    source_system_settings_defaults, Store,
};
use serde_json::{json, Value};
use sqlx::{Connection as _, Row};
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
#[derive(Clone, Copy)]
enum Group {
    Epg,
    System,
    Dvr,
}
impl Group {
    fn name(self) -> &'static str {
        match self {
            Self::Epg => "epg_settings",
            Self::System => "system_settings",
            Self::Dvr => "dvr_settings",
        }
    }
    fn defaults(self) -> Value {
        match self {
            Self::Epg => defaults(),
            Self::System => source_system_settings_defaults(),
            Self::Dvr => dvr_defaults(),
        }
    }
    fn key(self) -> String {
        format!(":1:coresettings:group:{}", self.name())
    }
    fn version(self) -> String {
        format!(":1:coresettings:groupver:{}", self.name())
    }
}
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
    // Rust drops fields in declaration order: release the caller guard before
    // advertising this physical operation's admission as available.
    _caller: Arc<dyn Send + Sync>,
    _permit: OwnedSemaphorePermit,
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
#[derive(Debug)]
pub enum ConfiguredSettingsStage {
    Cache,
    Admission,
    Acquire,
    Query,
    Decode,
    Ready,
    Close,
    Owner,
}
#[derive(Debug)]
pub struct ConfiguredSettingsError {
    pub stage: ConfiguredSettingsStage,
    pub error: Box<ImportError>,
    pub ready_error: Option<Box<sqlx::Error>>,
    pub close_error: Option<Box<sqlx::Error>>,
}
impl ConfiguredSettingsError {
    fn at(stage: ConfiguredSettingsStage, error: ImportError) -> Self {
        Self {
            stage,
            error: Box::new(error),
            ready_error: None,
            close_error: None,
        }
    }
}
const DATABASE_SQL:&str="SELECT octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=1048576 THEN value ELSE NULL END AS value FROM core_coresettings WHERE key=$1";
async fn database_row<'e, E: sqlx::Executor<'e, Database = sqlx::Postgres>>(
    executor: E,
    key: &'static str,
) -> Result<Option<sqlx::postgres::PgRow>, sqlx::Error> {
    sqlx::query(DATABASE_SQL)
        .bind(key)
        .fetch_optional(executor)
        .await
}
fn database_value(row: Option<sqlx::postgres::PgRow>, group: Group) -> Result<Value, ImportError> {
    let value = if let Some(row) = row {
        if row.try_get::<Option<i32>, _>("bytes")?.unwrap_or(0) > MAX_BYTES as i32 {
            return Err(ImportError::Capacity("EPG settings SQL value"));
        }
        row.try_get::<Option<Value>, _>("value")?
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    Ok(if value.as_object().is_some_and(|map| !map.is_empty()) {
        value
    } else {
        group.defaults()
    })
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
    /// Exact pinned Source system settings through the same retained cache/SQL
    /// transport. Full defaults preserve Django grouped-cache interoperability.
    /// Non-UTC deployment defaults require their own environment proof.
    pub async fn retained_system_settings(
        &self,
        context: SwapPolicyContext,
    ) -> Result<Value, ImportError> {
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
        self.group(Group::System, &context).await
    }
    /// Exact Source DVR settings for the narrowly supported SeriesRules branch.
    /// Cached dictionaries are returned unchanged; only missing/empty/nonmap DB
    /// values use all thirteen defaults. The caller remains physically retained.
    pub async fn retained_dvr_settings(
        &self,
        context: SwapPolicyContext,
    ) -> Result<Value, ImportError> {
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
        self.group(Group::Dvr, &context).await
    }
    /// Configured recording path only: same Source cache/default/version
    /// decisions, but SQL fallback physically fences ErrorResponse/Ready/close.
    /// Existing retained_system_settings behavior/API is unchanged.
    pub async fn retained_configured_system_settings(
        &self,
        context: SwapPolicyContext,
    ) -> Result<Value, ConfiguredSettingsError> {
        self.configured_group(Group::System, context).await
    }
    pub async fn retained_configured_dvr_settings(
        &self,
        context: SwapPolicyContext,
    ) -> Result<Value, ConfiguredSettingsError> {
        self.configured_group(Group::Dvr, context).await
    }
    async fn configured_group(
        &self,
        group: Group,
        context: SwapPolicyContext,
    ) -> Result<Value, ConfiguredSettingsError> {
        context
            .ensure_active()
            .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Admission, e))?;
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            ConfiguredSettingsError::at(
                ConfiguredSettingsStage::Admission,
                ImportError::Capacity("policy physical work"),
            )
        })?;
        let context = SwapPolicyContext {
            cancelled: context.cancelled,
            retained: Arc::new(PolicyWork {
                _caller: context.retained,
                _permit: permit,
            }),
        };
        self.group_mode(group, &context, true).await
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
    async fn database(
        &self,
        group: Group,
        context: &SwapPolicyContext,
    ) -> Result<Value, ImportError> {
        context.ensure_active()?;
        let store = self.store.clone();
        let retained = context.retained.clone();
        // The private owned task retains the job/lock guard through the actual
        // SQL reply even if a direct caller drops this policy future.
        let value = tokio::spawn(async move {
            let _retained = retained;
            database_value(database_row(store.pool(), group.name()).await?, group)
        })
        .await
        .map_err(|_| ImportError::Worker)??;
        context.ensure_active()?;
        Ok(value)
    }
    async fn database_settled(
        &self,
        group: Group,
        context: &SwapPolicyContext,
    ) -> Result<Value, ConfiguredSettingsError> {
        context
            .ensure_active()
            .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Admission, e))?;
        let store = self.store.clone();
        let retained = context.retained.clone();
        let value = tokio::spawn(async move {
            let _retained = retained;
            let mut connection = store.pool().acquire().await.map_err(|e| {
                ConfiguredSettingsError::at(ConfiguredSettingsStage::Acquire, e.into())
            })?;
            let (observed, stage) = match database_row(&mut *connection, group.name()).await {
                Ok(row) => (database_value(row, group), ConfiguredSettingsStage::Decode),
                Err(error) => (Err(error.into()), ConfiguredSettingsStage::Query),
            };
            // A SQLx query can return an ErrorResponse before the driver's
            // trailing ReadyForQuery/pool cleanup. Keep the actual owner here.
            let ready = connection.ping().await;
            let closed = connection.close().await;
            match observed {
                Err(error) => Err(ConfiguredSettingsError {
                    stage,
                    error: Box::new(error),
                    ready_error: ready.err().map(Box::new),
                    close_error: closed.err().map(Box::new),
                }),
                Ok(value) => {
                    if let Err(error) = ready {
                        return Err(ConfiguredSettingsError {
                            stage: ConfiguredSettingsStage::Ready,
                            error: Box::new(error.into()),
                            ready_error: None,
                            close_error: closed.err().map(Box::new),
                        });
                    }
                    closed.map_err(|e| {
                        ConfiguredSettingsError::at(ConfiguredSettingsStage::Close, e.into())
                    })?;
                    Ok(value)
                }
            }
        })
        .await
        .map_err(|_| {
            ConfiguredSettingsError::at(ConfiguredSettingsStage::Owner, ImportError::Worker)
        })??;
        context
            .ensure_active()
            .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Admission, e))?;
        Ok(value)
    }
    async fn database_mode(
        &self,
        group: Group,
        context: &SwapPolicyContext,
        settled: bool,
    ) -> Result<Value, ConfiguredSettingsError> {
        if settled {
            self.database_settled(group, context).await
        } else {
            self.database(group, context)
                .await
                .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Query, e))
        }
    }
    async fn fill(
        &self,
        key: String,
        value: &Value,
        context: &SwapPolicyContext,
    ) -> Result<(), ImportError> {
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
                &[key],
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
    async fn group(&self, group: Group, context: &SwapPolicyContext) -> Result<Value, ImportError> {
        self.group_mode(group, context, false)
            .await
            .map_err(|e| *e.error)
    }
    async fn group_mode(
        &self,
        group: Group,
        context: &SwapPolicyContext,
        settled: bool,
    ) -> Result<Value, ConfiguredSettingsError> {
        match self
            .read(&group.key(), context)
            .await
            .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Cache, e))?
        {
            Read::Value(Some(value)) if value.is_object() => return Ok(value),
            Read::Backend => return self.database_mode(group, context, settled).await,
            _ => {}
        }
        let before = match self
            .read(&group.version(), context)
            .await
            .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Cache, e))?
        {
            Read::Value(value) => value.unwrap_or(Value::Null),
            Read::Backend => return self.database_mode(group, context, settled).await,
        };
        let value = self.database_mode(group, context, settled).await?;
        let after = match self
            .read(&group.version(), context)
            .await
            .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Cache, e))?
        {
            Read::Value(value) => value.unwrap_or(Value::Null),
            Read::Backend => return Ok(value),
        };
        if source_cache_values_equal(&before, &after) {
            self.fill(group.key(), &value, context)
                .await
                .map_err(|e| ConfiguredSettingsError::at(ConfiguredSettingsStage::Cache, e))?;
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
            let value = self.group(Group::Epg, &context).await?;
            context.ensure_active()?;
            let history = source_epg_history_policy(&value).map_err(|_| ImportError::Parser)?;
            Ok(SwapPolicy { now, history })
        })
    }
}
fn dvr_defaults() -> Value {
    json!({
        "tv_template":"TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv",
        "movie_template":"Movies/{title} ({year}).mkv",
        "tv_fallback_dir":"TV_Shows",
        "tv_fallback_template":"TV_Shows/{show}/{start}.mkv",
        "movie_fallback_template":"Movies/{start}.mkv",
        "comskip_enabled":false,"comskip_custom_path":"","comskip_mode":"cut",
        "comskip_hw_accel":"none","pre_offset_minutes":0,"post_offset_minutes":0,
        "series_rules":[],"output_profile_id":null
    })
}
fn defaults() -> Value {
    json!({"epg_match_mode":"default","epg_match_ignore_prefixes":[],"epg_match_ignore_suffixes":[],"epg_match_ignore_custom":[],"epg_history_min_days":1,"epg_history_max_days":30,"epg_default_prev_days":0,"epg_default_days":0})
}

#[cfg(test)]
mod retained_group_tests {
    use super::*;
    #[test]
    fn original_group_names_and_full_defaults_are_separate() {
        assert_eq!(Group::Epg.key(), ":1:coresettings:group:epg_settings");
        assert_eq!(
            Group::System.version(),
            ":1:coresettings:groupver:system_settings"
        );
        assert_eq!(Group::Epg.defaults().as_object().unwrap().len(), 8);
        let system = Group::System.defaults();
        assert_eq!(system.as_object().unwrap().len(), 9);
        assert_eq!(system["max_system_events"], 100);
        assert!(system.get("epg_history_min_days").is_none());
    }
    #[test]
    fn admission_cannot_report_complete_before_caller_guard_drops() {
        use std::sync::{mpsc, Mutex};
        struct BlockDrop {
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl Drop for BlockDrop {
            fn drop(&mut self) {
                self.entered.send(()).unwrap();
                self.release.lock().unwrap().recv().unwrap();
            }
        }
        let (entered, observed) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let admission = Arc::new(Semaphore::new(1));
        let work = PolicyWork {
            _caller: Arc::new(BlockDrop {
                entered,
                release: Mutex::new(released),
            }),
            _permit: admission.clone().try_acquire_owned().unwrap(),
        };
        let worker = std::thread::spawn(move || drop(work));
        observed.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(admission.available_permits(), 0);
        release.send(()).unwrap();
        worker.join().unwrap();
        assert_eq!(admission.available_permits(), 1);
    }
}
