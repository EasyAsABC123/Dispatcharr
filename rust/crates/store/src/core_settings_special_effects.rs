//! Source51 real special-group effects, opt-in and unassembled.
//! No new database/Redis pool, task worker or writer allowance is introduced.
use crate::{AuthService, StoreError};
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::OwnedSemaphorePermit;

const RAW: usize = 65536;
const WORK: usize = 4 * 1024 * 1024;
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"CoreSettings resource capacity is unavailable."}),
    )
}

/// The cached tree retains ONE actual already-admitted writer token. It never
/// takes a second token or fabricates an Arc owner. It cannot cycle back to the
/// CoreSettings State/Lease: only the original semaphore token is retained.
struct ProxyEntry {
    value: Value,
    _permit: Arc<OwnedSemaphorePermit>,
    active: Arc<AtomicUsize>,
}
impl Drop for ProxyEntry {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}
#[derive(Clone)]
pub struct ProxySettingsRead {
    entry: Arc<ProxyEntry>,
}
impl ProxySettingsRead {
    pub fn value(&self) -> &Value {
        &self.entry.value
    }
}
/// Only three copied health scalars. It never retains a cached Value/permit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyHealthKnobs {
    pub startup: Duration,
    pub connection: Duration,
    pub initialization: Duration,
}
impl ProxyHealthKnobs {
    pub(crate) fn from_value(value: &Value) -> Result<Self, StoreError> {
        if !value.is_object() {
            return Err(StoreError::Unsupported(
                "Uncaptured proxy health settings shape",
            ));
        }
        let field = |key: &str, default: f64| -> Result<Duration, StoreError> {
            let seconds = match value.get(key) {
                None => default,
                Some(Value::Number(v)) => v
                    .as_f64()
                    .ok_or(StoreError::Unsupported("Uncaptured proxy health number"))?,
                // Python bool participates in numeric health comparisons.
                Some(Value::Bool(v)) => f64::from(u8::from(*v)),
                _ => return Err(StoreError::Unsupported("Uncaptured proxy health scalar")),
            };
            Duration::try_from_secs_f64(seconds).map_err(|_| {
                StoreError::Unsupported("Proxy health duration outside supported finite range")
            })
        };
        Ok(Self {
            startup: field("stream_startup_timeout", 8.0)?,
            connection: field("connection_timeout", 10.0)?,
            initialization: field("channel_init_grace_period", 60.0)?,
        })
    }
}
impl ProxySettingsRead {
    /// Copy before dropping this read; no frame or session retains the tree.
    pub fn health_knobs(&self) -> Result<ProxyHealthKnobs, StoreError> {
        ProxyHealthKnobs::from_value(&self.entry.value)
    }
}
fn wall_seconds() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}
fn cache_fresh(now: f64, sampled_before_load: f64) -> bool {
    now.is_finite() && sampled_before_load.is_finite() && now - sampled_before_load < 10.0
}

struct ProxySlot {
    at: f64,
    entry: Arc<ProxyEntry>,
}
struct Inner {
    log_directory: PathBuf,
    proxy: Mutex<Option<ProxySlot>>,
    cache_entries: Arc<AtomicUsize>,
    generation: AtomicUsize,
}
#[derive(Clone)]
pub struct CoreSettingsSpecialEffects {
    inner: Arc<Inner>,
}
impl CoreSettingsSpecialEffects {
    /// Explicit trusted fixture/application directory, never an API path.
    pub fn new(log_directory: PathBuf) -> Result<Self, StoreError> {
        if !log_directory.is_absolute() || log_directory.as_os_str().len() > 512 {
            return Err(StoreError::Unsupported(
                "Explicit owned log directory required",
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                log_directory,
                proxy: Mutex::new(None),
                cache_entries: Arc::new(AtomicUsize::new(0)),
                generation: AtomicUsize::new(0),
            }),
        })
    }
    pub(crate) fn cached_proxy_read(&self) -> Option<ProxySettingsRead> {
        self.inner
            .proxy
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| ProxySettingsRead {
                entry: s.entry.clone(),
            })
    }
    /// A production cache hit must check Source's strict wall-time TTL. This
    /// adds no permit: the cloned entry retains its original actual token.
    pub(crate) fn fresh_proxy_read(&self) -> Option<ProxySettingsRead> {
        let now = wall_seconds();
        self.inner
            .proxy
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .filter(|slot| cache_fresh(now, slot.at))
            .map(|slot| ProxySettingsRead {
                entry: slot.entry.clone(),
            })
    }
    pub(crate) fn clear_proxy(&self) {
        let mut slot = self.inner.proxy.lock().unwrap_or_else(|e| e.into_inner());
        self.inner.generation.fetch_add(1, Ordering::AcqRel);
        slot.take();
    }
    pub(crate) fn retained_cache_entries(&self) -> usize {
        self.inner.cache_entries.load(Ordering::Acquire)
    }
    /// Helper semantics only until an explicit Main knob consumer is wired.
    /// Source's ten-second cache is lazy-expired; the bounded cached tree stays
    /// charged to its original writer until replacement/invalidation/shutdown.
    pub async fn proxy_settings(
        &self,
        auth: &AuthService,
    ) -> Result<ProxySettingsRead, StoreError> {
        let sampled_before_load = wall_seconds();
        {
            let cache = self.inner.proxy.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(slot) = cache.as_ref() {
                if cache_fresh(sampled_before_load, slot.at) {
                    return Ok(ProxySettingsRead {
                        entry: slot.entry.clone(),
                    });
                }
            }
        }
        // Retire an expired cache slot first. A still-borrowed old tree keeps
        // its original charge and prevents another persistent allocation.
        let generation = {
            let mut cache = self.inner.proxy.lock().unwrap_or_else(|e| e.into_inner());
            cache.take();
            self.inner.generation.load(Ordering::Acquire)
        };
        let permit = auth
            .request_writer_permit
            .clone()
            .ok_or(StoreError::Unsupported(
                "Proxy settings read requires the original admitted writer token",
            ))?;
        if !Arc::ptr_eq(permit.semaphore(), &auth.user_agent_workers) {
            return Err(StoreError::Unsupported(
                "Foreign proxy settings writer token",
            ));
        }
        let this = self.clone();
        let auth = auth.clone();
        // Detaching this physical task on caller loss keeps the same token
        // and SQL Ready fence alive; PoolConnection.drop cannot retire it early.
        let task = tokio::spawn(async move {
            this.inner
                .cache_entries
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| capacity())?;
            // A guard covers cancellation/error during actual cache/SQL decode;
            // on success the same guard becomes the returned/cached tree owner.
            let mut entry = ProxyEntry {
                value: Value::Null,
                _permit: permit,
                active: this.inner.cache_entries.clone(),
            };
            entry.value = load_proxy(&auth).await?;
            let entry = Arc::new(entry);
            let mut cache = this.inner.proxy.lock().unwrap_or_else(|e| e.into_inner());
            if this.inner.generation.load(Ordering::Acquire) == generation {
                cache.replace(ProxySlot {
                    at: sampled_before_load,
                    entry: entry.clone(),
                });
            }
            Ok::<_, StoreError>(ProxySettingsRead { entry })
        });
        task.await
            .map_err(|_| StoreError::Unsupported("Owned proxy loader did not settle"))?
    }
    pub(crate) async fn system_save(
        &self,
        auth: &AuthService,
        value: &Value,
        scratch: &crate::core_settings_special_budget::Scratch,
    ) -> Result<(), StoreError> {
        // Source signal ordering: timezone setter first, then config write.
        auth.set_display_timezone_on_settings_save("system_settings", value);
        let map = value.as_object().ok_or(StoreError::Unsupported(
            "Uncaptured system-settings signal value shape",
        ))?;
        let persist = map.get("log_persist") != Some(&Value::Bool(false));
        let number = |key: &str, default: i64| -> Result<i64, StoreError> {
            let value = map.get(key).filter(|v| crate::channels::truthy(Some(v)));
            match value {
                None => Ok(default),
                Some(Value::Number(v)) => v.as_i64().ok_or(StoreError::Unsupported(
                    "Uncaptured system log number conversion",
                )),
                Some(Value::String(v)) => dispatcharr_ingest_core::source_integer_literal(v)
                    .and_then(|v| v.parse().ok())
                    .ok_or(StoreError::Unsupported(
                        "Uncaptured system log number conversion",
                    )),
                Some(Value::Bool(v)) => Ok(i64::from(*v)),
                _ => Err(StoreError::Unsupported("Uncaptured system log scalar")),
            }
        };
        let max = number("log_max_mb", 5)?;
        let keep = number("log_keep", 5)?;
        let zone = match map
            .get("time_zone")
            .filter(|v| crate::channels::truthy(Some(v)))
        {
            None => "UTC",
            Some(Value::String(v)) => v,
            _ => return Err(StoreError::Unsupported("Uncaptured log timezone scalar")),
        };
        // Charge a conservative complete config buffer before formatting.
        let config_bytes = zone.len().checked_add(128).ok_or_else(capacity)?;
        let charge = scratch.charge(config_bytes)?;
        let text = format!(
            "persist={}\nmax_mb={max}\nkeep={keep}\ntime_zone={zone}\n",
            u8::from(persist)
        );
        if text.len() > RAW {
            return Err(capacity());
        }
        let directory = self.inner.log_directory.clone();
        auth.request_writer_permit
            .as_ref()
            .ok_or(StoreError::Unsupported(
                "System config requires the original writer token",
            ))?;
        let owner = auth.clone();
        // This owned blocking task retains the SAME actual token if caller or
        // observer leaves. No detached filesystem work can release admission.
        let task = tokio::task::spawn_blocking(move || {
            let _owner = owner;
            let _charge = charge;
            write_config(&directory, text.as_bytes())
        });
        match task.await {
            Ok(Ok(())) => {}
            // Pinned write_conf swallows OSError, not scalar conversion errors.
            Ok(Err(error)) => tracing::warn!(%error, "Owned log configuration write failed"),
            Err(_) => return Err(StoreError::Unsupported("Owned log writer did not settle")),
        }
        Ok(())
    }
}
fn write_config(directory: &Path, text: &[u8]) -> std::io::Result<()> {
    let config = directory.join("config");
    std::fs::create_dir_all(&config)?;
    // Do not follow a configured directory symlink into an unrelated owner.
    if std::fs::symlink_metadata(&config)?.file_type().is_symlink() {
        return Err(std::io::Error::other("owned config directory is a symlink"));
    }
    let temporary = config.join(format!(
        "collector.conf.tmp-rust-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(text)?;
        file.flush()?;
        std::fs::rename(&temporary, config.join("collector.conf"))
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
async fn finish(
    mut connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
) -> Result<(), StoreError> {
    if let Err(error) = Connection::ping(&mut *connection).await {
        let _ = connection.close().await;
        return Err(error.into());
    }
    Connection::shrink_buffers(&mut *connection);
    connection.return_to_pool().await;
    drop(connection);
    Ok(())
}
pub(crate) async fn load_proxy(auth: &AuthService) -> Result<Value, StoreError> {
    let defaults = json!({"buffering_timeout":15,"buffering_speed":1.0,"redis_chunk_ttl":60,"channel_shutdown_delay":0,"channel_init_grace_period":60,"channel_client_wait_period":5,"new_client_behind_seconds":5,"stream_startup_timeout":8,"connection_timeout":10,"validate_redirect_urls":true});
    load_group(auth, "proxy_settings", defaults).await
}
pub(crate) async fn load_group(
    auth: &AuthService,
    key: &str,
    defaults: Value,
) -> Result<Value, StoreError> {
    let load = || async move {
        let mut c = auth.store.pool().acquire().await?;
        let result=sqlx::query("SELECT octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=65536 THEN value::text END AS value_text FROM core_coresettings WHERE key=$1").bind(key).fetch_optional(&mut*c).await;
        finish(c).await?;
        let row = result?;
        let Some(row) = row else { return Ok(defaults) };
        let size: i32 = row.try_get("bytes")?;
        let raw: Option<String> = row.try_get("value_text")?;
        if size < 0 || size as usize > RAW {
            return Err(capacity());
        }
        let raw = raw.ok_or_else(capacity)?;
        let (value, _) = crate::xc_bounded_user::checked_properties(
            &raw,
            RAW,
            crate::XcUserReadLimits {
                max_raw_bytes: RAW,
                max_nodes: 2048,
                max_depth: 32,
                work_bytes: WORK,
            },
        )?;
        Ok(if value.as_object().is_some_and(|m| !m.is_empty()) {
            value
        } else {
            defaults
        })
    };
    match &auth.notification_cache {
        Some(cache) => cache.settings_group_bounded(key, load).await,
        None => Err(StoreError::Unsupported(
            "Real shared settings cache required",
        )),
    }
}

/// Real bounded default-UA helper for the declared Source51 stream warm step.
/// Uses the same shared cache and actual SQL Ready fence. Main's existing
/// image/default-UA consumers are unchanged and not qualified by this helper.
pub(crate) async fn default_user_agent(auth: &AuthService) -> Result<String, StoreError> {
    let cache = auth.notification_cache.as_ref().ok_or_else(capacity)?;
    let get = |key: &'static str| async move {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg("if redis.call('STRLEN',KEYS[1])>65536 then return redis.error_reply('cache value exceeds limit') end return redis.call('GET',KEYS[1])").arg(1).arg(key);
        cache.command::<Option<Vec<u8>>>(cmd).await
    };
    let load = || async {
        let group = load_group(auth, "stream_settings", json!({"default_user_agent":null})).await?;
        let id = group.get("default_user_agent").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_bool().map(i64::from))
                .or_else(|| {
                    v.as_f64()
                        .filter(|n| {
                            n.is_finite() && *n >= i64::MIN as f64 && *n < (i64::MAX as f64)
                        })
                        .map(|n| n as i64)
                })
                .or_else(|| {
                    dispatcharr_ingest_core::source_integer_literal(v.as_str()?)
                        .and_then(|n| n.parse().ok())
                })
        });
        drop(group);
        let fallback = || "Dispatcharr/0.33.0".to_owned();
        let Some(id) = id else { return Ok(fallback()) };
        let mut c = auth.store.pool().acquire().await?;
        let result=sqlx::query("SELECT octet_length(user_agent) AS bytes,CASE WHEN octet_length(user_agent)<=65536 THEN user_agent END AS value FROM core_useragent WHERE id::bigint=$1").bind(id).fetch_optional(&mut*c).await;
        finish(c).await?;
        let Some(row) = result? else {
            return Ok(fallback());
        };
        let bytes: i32 = row.try_get("bytes")?;
        if !(0..=65536).contains(&bytes) {
            return Err(capacity());
        }
        let value: Option<String> = row.try_get("value")?;
        Ok::<_, StoreError>(value.filter(|s| !s.is_empty()).unwrap_or_else(fallback))
    };
    const KEY: &str = ":1:coresettings:default_user_agent";
    const VERSION: &str = ":1:coresettings:default_user_agent:ver";
    match get(KEY).await {
        Ok(Some(raw)) => {
            if let Some(Value::String(value)) = crate::notifications::decode_settings_cache(&raw) {
                if !value.is_empty() {
                    return Ok(value);
                }
            }
        }
        Ok(None) => {}
        Err(_) => return load().await,
    }
    let before = match get(VERSION).await {
        Ok(raw) => raw,
        Err(_) => return load().await,
    };
    let value = load().await?;
    let after = match get(VERSION).await {
        Ok(raw) => raw,
        Err(_) => return Ok(value),
    };
    if before == after {
        let _ = cache.set_for(KEY, &json!(value), 300).await;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn invalidation_retires_cached_tree_only_after_last_real_reader() {
        let effects = CoreSettingsSpecialEffects::new(std::env::current_dir().unwrap()).unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(4));
        let permit = Arc::new(permits.clone().try_acquire_owned().unwrap());
        effects.inner.cache_entries.store(1, Ordering::Release);
        let entry = Arc::new(ProxyEntry {
            value: json!({"buffering_timeout":31}),
            _permit: permit,
            active: effects.inner.cache_entries.clone(),
        });
        effects.inner.proxy.lock().unwrap().replace(ProxySlot {
            at: wall_seconds(),
            entry: entry.clone(),
        });
        let reader = ProxySettingsRead { entry };
        let cloned = reader.clone();
        assert_eq!(permits.available_permits(), 3);
        let generation = effects.inner.generation.load(Ordering::Acquire);
        effects.clear_proxy();
        assert!(effects.inner.proxy.lock().unwrap().is_none());
        assert_eq!(
            effects.inner.generation.load(Ordering::Acquire),
            generation + 1
        );
        assert_eq!(reader.value()["buffering_timeout"], 31);
        assert_eq!(effects.retained_cache_entries(), 1);
        assert!(effects
            .inner
            .cache_entries
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err());
        drop(reader);
        assert_eq!(effects.retained_cache_entries(), 1);
        drop(cloned);
        assert_eq!(effects.retained_cache_entries(), 0);
        assert_eq!(permits.available_permits(), 4);
    }
    #[tokio::test]
    async fn warm_read_reuses_existing_token_when_all_four_are_occupied() {
        let effects = CoreSettingsSpecialEffects::new(std::env::current_dir().unwrap()).unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(4));
        let cached_permit = Arc::new(permits.clone().try_acquire_owned().unwrap());
        let others: Vec<_> = (0..3)
            .map(|_| permits.clone().try_acquire_owned().unwrap())
            .collect();
        effects.inner.cache_entries.store(1, Ordering::Release);
        effects.inner.proxy.lock().unwrap().replace(ProxySlot {
            at: wall_seconds(),
            entry: Arc::new(ProxyEntry {
                value: json!({"connection_timeout":4}),
                _permit: cached_permit,
                active: effects.inner.cache_entries.clone(),
            }),
        });
        assert_eq!(permits.available_permits(), 0);
        let read = effects
            .fresh_proxy_read()
            .expect("warm hit needs no fifth token");
        assert_eq!(
            read.health_knobs().unwrap().connection,
            Duration::from_secs(4)
        );
        assert_eq!(permits.available_permits(), 0);
        effects.clear_proxy();
        assert_eq!(permits.available_permits(), 0);
        drop(read);
        assert_eq!(permits.available_permits(), 1);
        drop(others);
        assert_eq!(permits.available_permits(), 4);
    }
    #[test]
    fn preload_wall_timestamp_is_not_completion_time() {
        let pre_load = 100.0;
        assert!(cache_fresh(102.4, pre_load));
        assert!(!cache_fresh(110.0, pre_load));
        assert!(!cache_fresh(112.4, pre_load));
        assert!(cache_fresh(99.0, pre_load));
        assert!(!cache_fresh(f64::NAN, pre_load));
        assert!(!cache_fresh(102.0, f64::INFINITY));
    }
    #[test]
    fn typed_health_copy_does_not_retain_the_original_cache_owner() {
        let permits = Arc::new(tokio::sync::Semaphore::new(4));
        let active = Arc::new(AtomicUsize::new(1));
        let read = ProxySettingsRead {
            entry: Arc::new(ProxyEntry {
                value: json!({"stream_startup_timeout":3,"connection_timeout":4,"channel_init_grace_period":5}),
                _permit: Arc::new(permits.clone().try_acquire_owned().unwrap()),
                active: active.clone(),
            }),
        };
        let copied = read.health_knobs().unwrap();
        drop(read);
        assert_eq!(permits.available_permits(), 4);
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert_eq!(copied.startup, Duration::from_secs(3));
        assert_eq!(copied.connection, Duration::from_secs(4));
        assert_eq!(copied.initialization, Duration::from_secs(5));
    }
    #[test]
    fn typed_health_rejects_unsupported_shapes_and_preserves_python_bool() {
        assert_eq!(
            ProxyHealthKnobs::from_value(&json!({})).unwrap().startup,
            Duration::from_secs(8)
        );
        assert_eq!(
            ProxyHealthKnobs::from_value(&json!({"connection_timeout":false}))
                .unwrap()
                .connection,
            Duration::ZERO
        );
        for value in [
            json!([]),
            json!({"connection_timeout":"4"}),
            json!({"connection_timeout":null}),
            json!({"connection_timeout":-1}),
            json!({"connection_timeout":1e100}),
        ] {
            assert!(ProxyHealthKnobs::from_value(&value).is_err());
        }
    }
}
