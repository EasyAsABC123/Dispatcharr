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
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

/// Only a fixed three-Duration snapshot and one wall timestamp. No Value tree,
/// permit, independent pool or variable-size persistent data is retained here.
#[derive(Clone, Copy)]
struct TypedTsSlot {
    at: f64,
    value: HealthTimeouts,
}
pub struct CoreProxyHealthSource {
    core: CoreSettingsWrites,
    ts: Mutex<Option<TypedTsSlot>>,
}
fn wall_seconds() -> f64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs_f64(),
        Err(e) => -e.duration().as_secs_f64(),
    }
}
impl CoreProxyHealthSource {
    /// Construct from pre-engine AuthService: never an EngineControl cycle.
    /// Reuses the exact Core/Auth/cache context, admission and physical owners.
    pub fn new(core: CoreSettingsWrites) -> Self {
        Self {
            core,
            ts: Mutex::new(None),
        }
    }
}
impl HealthSettingsSource for CoreProxyHealthSource {
    fn snapshot(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<HealthTimeouts, PlaybackError>> + Send + '_>> {
        Box::pin(async move {
            // Captured BEFORE loading, as Source's classmethod does. A load
            // taking over ten seconds can return once, then the next call misses.
            if !self.core.proxy_health_reads_open() {
                return Err(PlaybackError::new(
                    503,
                    "Proxy health settings are stopping",
                ));
            }
            let sampled_before_load = wall_seconds();
            let ts_had_own_slot;
            {
                let cache = self.ts.lock().unwrap_or_else(|e| e.into_inner());
                ts_had_own_slot = cache.is_some();
                if let Some(slot) = *cache {
                    if sampled_before_load - slot.at < 10.0 {
                        return Ok(slot.value);
                    }
                }
            }
            // Source returns an inherited fresh Base hit WITHOUT assigning TS.
            // The actual Base-first Source cohort measures this precise branch.
            if let Some(copied) = if ts_had_own_slot {
                None
            } else {
                self.core
                    .inherited_proxy_health_knobs()
                    .map_err(|_| PlaybackError::new(503, "Proxy health settings unavailable"))?
            } {
                return Ok(HealthTimeouts {
                    startup: copied.startup,
                    connection: copied.connection,
                    initialization: copied.initialization,
                });
            }
            // Direct bounded group load, not the distinct BaseConfig cache.
            // Caller loss leaves the same original permit in the Core-owned task
            // until physical SQL/cache completion. No separate pool is created.
            let copied = self
                .core
                .proxy_ts_health_knobs()
                .await
                .map_err(|_| PlaybackError::new(503, "Proxy health settings unavailable"))?;
            let value = HealthTimeouts {
                startup: copied.startup,
                connection: copied.connection,
                initialization: copied.initialization,
            };
            *self.ts.lock().unwrap_or_else(|e| e.into_inner()) = Some(TypedTsSlot {
                at: sampled_before_load,
                value,
            });
            Ok(value)
        })
    }
}
