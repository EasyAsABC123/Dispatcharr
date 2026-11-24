//! Explicit Main TS health adapter; Original Dispatcharr authors retain credit.
//! Source TSConfig owns a separate class slot after its first load. BaseConfig
//! invalidation clears an inherited Base hit; after a genuine cold TS load it
//! does not clear the independent typed TS snapshot. This was
//! measured in m235-new-source-cohort-2; not full stream or exceptional-shape parity.
use dispatcharr_contracts::PlaybackError;
use dispatcharr_store::CoreSettingsWrites;
use dispatcharr_stream_http::{HealthSettingsSource, HealthTimeouts};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;

#[derive(Clone, Copy)]
struct TypedTsSlot {
    at: f64,
    value: HealthTimeouts,
}

#[derive(Clone, Copy)]
enum ReadOutcome {
    Loaded(HealthTimeouts),
    DatabaseUnavailable,
    Refused,
}

// Retain one copied snapshot and at most one physical refresh. Joining the
// refresh cannot reserve another writer or retain a cached settings tree.
#[derive(Default)]
struct TypedTsCache {
    slot: Option<TypedTsSlot>,
    refresh: Option<watch::Receiver<Option<ReadOutcome>>>,
}

impl TypedTsCache {
    fn start_or_join(
        &mut self,
        cache: &Arc<Mutex<Self>>,
        sampled_before_load: f64,
        load: impl Future<Output = ReadOutcome> + Send + 'static,
    ) -> watch::Receiver<Option<ReadOutcome>> {
        if let Some(refresh) = self.active_refresh() {
            return refresh.clone();
        }
        let (sender, receiver) = watch::channel(None);
        self.refresh = Some(receiver.clone());
        let cache = cache.clone();
        tokio::spawn(async move {
            let outcome = load.await;
            let mut cache = cache.lock().unwrap_or_else(|e| e.into_inner());
            if let ReadOutcome::Loaded(value) = outcome {
                cache.slot = Some(TypedTsSlot {
                    at: sampled_before_load,
                    value,
                });
            }
            sender.send_replace(Some(outcome));
            cache.refresh = None;
        });
        receiver
    }

    fn active_refresh(&self) -> Option<&watch::Receiver<Option<ReadOutcome>>> {
        self.refresh
            .as_ref()
            .filter(|refresh| refresh.has_changed().is_ok())
    }

    fn fresh(&self, now: f64) -> Option<HealthTimeouts> {
        self.slot
            .filter(|slot| now - slot.at < 10.0)
            .map(|slot| slot.value)
    }
}

pub struct CoreProxyHealthSource {
    core: CoreSettingsWrites,
    ts: Arc<Mutex<TypedTsCache>>,
}

fn wall_seconds() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}

fn unavailable() -> PlaybackError {
    PlaybackError::new(503, "Proxy health settings unavailable")
}

async fn wait_for_refresh(
    mut receiver: watch::Receiver<Option<ReadOutcome>>,
) -> Result<HealthTimeouts, PlaybackError> {
    receiver
        .wait_for(Option::is_some)
        .await
        .map_err(|_| unavailable())
        .and_then(|state| match *state {
            Some(ReadOutcome::Loaded(value)) => Ok(value),
            // Source does not assign a cache slot or refresh its age on error.
            Some(ReadOutcome::DatabaseUnavailable) => Ok(HealthTimeouts::default()),
            Some(ReadOutcome::Refused) | None => Err(unavailable()),
        })
}

impl CoreProxyHealthSource {
    /// Construct from pre-engine AuthService: never an EngineControl cycle.
    /// Reuses the exact Core/Auth/cache context, admission and physical owners.
    pub fn new(core: CoreSettingsWrites) -> Self {
        Self {
            core,
            ts: Arc::new(Mutex::new(TypedTsCache::default())),
        }
    }
}

fn from_knobs(knobs: dispatcharr_store::ProxyHealthKnobs) -> HealthTimeouts {
    HealthTimeouts {
        startup: knobs.startup,
        connection: knobs.connection,
        initialization: knobs.initialization,
    }
}

impl HealthSettingsSource for CoreProxyHealthSource {
    fn snapshot(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HealthTimeouts, PlaybackError>> + Send + '_>> {
        Box::pin(async move {
            if !self.core.proxy_health_reads_open() {
                return Err(PlaybackError::new(
                    503,
                    "Proxy health settings are stopping",
                ));
            }
            // The strict ten-second window starts before the original load,
            // never at completion or when another consumer joins it.
            let sampled_before_load = wall_seconds();
            let refresh = {
                let mut cache = self.ts.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(value) = cache.fresh(sampled_before_load) {
                    return Ok(value);
                }
                // Source inherits a Base hit without assigning a TS-owned slot.
                // A TS load already in progress must finish before using Base.
                let inherited = if cache.slot.is_none() && cache.active_refresh().is_none() {
                    self.core
                        .inherited_proxy_health_knobs()
                        .map_err(|_| unavailable())?
                } else {
                    None
                };
                if let Some(knobs) = inherited {
                    return Ok(from_knobs(knobs));
                }
                let core = self.core.clone();
                cache.start_or_join(&self.ts, sampled_before_load, async move {
                    match core.proxy_ts_health_knobs().await {
                        Ok(Some(knobs)) => ReadOutcome::Loaded(from_knobs(knobs)),
                        Ok(None) => ReadOutcome::DatabaseUnavailable,
                        Err(_) => ReadOutcome::Refused,
                    }
                })
            };
            wait_for_refresh(refresh).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn join(
        cache: &Arc<Mutex<TypedTsCache>>,
        sampled: f64,
        load: impl Future<Output = ReadOutcome> + Send + 'static,
    ) -> watch::Receiver<Option<ReadOutcome>> {
        cache.lock().unwrap().start_or_join(cache, sampled, load)
    }

    #[tokio::test]
    async fn detached_waiter_and_later_ticks_reuse_physical_refresh_and_preload_clock() {
        let cache = Arc::new(Mutex::new(TypedTsCache::default()));
        let (started, claimed) = oneshot::channel();
        let (settle, pending) = oneshot::channel();
        let first = join(&cache, 20.0, async move {
            started.send(()).unwrap();
            ReadOutcome::Loaded(pending.await.unwrap())
        });
        claimed.await.unwrap();
        drop(first);
        let second = join(&cache, 28.0, async { panic!("refresh did not join") });
        assert!(second.borrow().is_none());
        let third = join(&cache, 29.0, async { panic!("refresh started twice") });
        let expected = HealthTimeouts {
            connection: Duration::from_secs(4),
            ..HealthTimeouts::default()
        };
        settle.send(expected).unwrap();
        assert_eq!(wait_for_refresh(second).await.unwrap(), expected);
        assert_eq!(wait_for_refresh(third).await.unwrap(), expected);
        let state = cache.lock().unwrap();
        assert!(state.refresh.is_none());
        assert_eq!(state.fresh(29.999), Some(expected));
        assert_eq!(state.fresh(30.0), None);
    }

    #[tokio::test]
    async fn database_failure_returns_original_defaults_without_replacing_the_expired_slot() {
        let previous = HealthTimeouts {
            startup: Duration::from_secs(3),
            connection: Duration::from_secs(4),
            initialization: Duration::from_secs(5),
        };
        let cache = Arc::new(Mutex::new(TypedTsCache {
            slot: Some(TypedTsSlot {
                at: 20.0,
                value: previous,
            }),
            refresh: None,
        }));
        let (settle, pending) = oneshot::channel();
        let first = join(&cache, 30.0, async move {
            pending.await.unwrap();
            ReadOutcome::DatabaseUnavailable
        });
        let joined = join(&cache, 31.0, async {
            panic!("database retry started twice")
        });
        settle.send(()).unwrap();
        for reader in [first, joined] {
            assert_eq!(
                wait_for_refresh(reader).await.unwrap(),
                HealthTimeouts::default()
            );
        }

        {
            let old = cache.lock().unwrap();
            let expired = old.slot.unwrap();
            assert_eq!(expired.at, 20.0);
            assert_eq!(expired.value, previous);
            assert!(old.fresh(31.0).is_none());
        }
        let retry = join(&cache, 31.0, async move { ReadOutcome::Loaded(previous) });
        assert_eq!(wait_for_refresh(retry).await.unwrap(), previous);
        assert_eq!(cache.lock().unwrap().fresh(31.0), Some(previous));
    }

    #[tokio::test]
    async fn failure_reaches_joiners_without_caching_or_blocking_next_refresh() {
        let cache = Arc::new(Mutex::new(TypedTsCache::default()));
        let (settle, pending) = oneshot::channel::<()>();
        let first = join(&cache, 20.0, async move {
            let _ = pending.await;
            ReadOutcome::Refused
        });
        let second = join(&cache, 21.0, async { panic!("refresh started twice") });
        settle.send(()).unwrap();
        for reader in [first, second] {
            let error = wait_for_refresh(reader).await.unwrap_err();
            assert_eq!(error.status, 503);
        }
        assert!(cache.lock().unwrap().slot.is_none());
        let expected = HealthTimeouts::default();
        let retry = join(&cache, 22.0, async move { ReadOutcome::Loaded(expected) });
        assert_eq!(wait_for_refresh(retry).await.unwrap(), expected);
    }
}
