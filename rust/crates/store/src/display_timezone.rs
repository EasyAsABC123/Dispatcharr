//! Inert Source request_started display-zone callback; Original Dispatcharr credit.
//! Call explicitly at real request ingress, before authentication/body extraction.
use crate::{AuthService, StoreError};
use serde_json::Value;
use sqlx::Row;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

const REFRESH: Duration = Duration::from_secs(5);
const SETTINGS_RAW_LIMIT: usize = 65536;
const SYSTEM_SQL: &str = "SELECT octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=65536 THEN value::text END AS value_text FROM core_coresettings WHERE key=$1";
fn bounded_system_value(bytes: i32, raw: Option<&str>) -> Result<Value, StoreError> {
    let raw = raw.ok_or(StoreError::Unsupported(
        "Display timezone settings exceed qualified capacity",
    ))?;
    if bytes < 0 || bytes as usize > SETTINGS_RAW_LIMIT || raw.len() != bytes as usize {
        return Err(StoreError::Unsupported(
            "Display timezone settings exceed qualified capacity",
        ));
    }
    let (value, _retained_quote) = crate::xc_bounded_user::checked_properties(
        raw,
        0,
        crate::XcUserReadLimits {
            max_raw_bytes: SETTINGS_RAW_LIMIT,
            max_nodes: 2048,
            max_depth: 32,
            work_bytes: 4 * 1024 * 1024,
        },
    )?;
    Ok(if value.as_object().is_some_and(|v| !v.is_empty()) {
        value
    } else {
        crate::source_system_settings_defaults()
    })
}

#[derive(Clone)]
pub(crate) struct DisplayTimezone {
    state: Arc<Mutex<ZoneState>>,
    environment: String,
}
struct ZoneState {
    zone: Option<String>,
    checked: Option<Instant>,
}
fn valid_zone(name: &str) -> bool {
    name.parse::<chrono_tz::Tz>().is_ok()
}
fn environment_zone(dispatcharr: Option<&str>, tz: Option<&str>) -> String {
    let name = dispatcharr
        .filter(|v| !v.is_empty())
        .or(tz.filter(|v| !v.is_empty()))
        .unwrap_or("UTC");
    if valid_zone(name) {
        name.into()
    } else {
        "UTC".into()
    }
}
impl DisplayTimezone {
    pub(crate) fn new() -> Self {
        Self::from_environment(
            std::env::var("DISPATCHARR_TIME_ZONE").ok().as_deref(),
            std::env::var("TZ").ok().as_deref(),
        )
    }
    fn from_environment(dispatcharr: Option<&str>, tz: Option<&str>) -> Self {
        Self {
            state: Arc::new(Mutex::new(ZoneState {
                zone: None,
                checked: None,
            })),
            environment: environment_zone(dispatcharr, tz),
        }
    }
    fn zone(&self) -> String {
        self.state
            .lock()
            .unwrap()
            .zone
            .clone()
            .unwrap_or_else(|| self.environment.clone())
    }
    fn claim(&self, force: bool) -> bool {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap();
        if !force
            && state
                .checked
                .is_some_and(|then| now.duration_since(then) < REFRESH)
        {
            return false;
        }
        // Source stamps BEFORE try: errors, invalid zones and admission refusal
        // must not turn every subsequent request into another refresh attempt.
        state.checked = Some(now);
        true
    }
    fn finish(&self, value: &Value) {
        if let Some(name) = value.as_str().filter(|name| valid_zone(name)) {
            self.state.lock().unwrap().zone = Some(name.into());
        }
    }
    fn set(&self, value: &Value) {
        if let Some(name) = value.as_str().filter(|name| valid_zone(name)) {
            let mut state = self.state.lock().unwrap();
            state.zone = Some(name.into());
            state.checked = Some(Instant::now());
        }
    }
    async fn refresh<F, Fut>(
        &self,
        force: bool,
        workers: Arc<Semaphore>,
        inherited: Option<Arc<OwnedSemaphorePermit>>,
        work: F,
    ) where
        F: FnOnce(Arc<OwnedSemaphorePermit>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Value, StoreError>> + Send + 'static,
    {
        if !self.claim(force) {
            return;
        }
        let permit = match inherited {
            Some(p) => p,
            None => match workers.try_acquire_owned() {
                Ok(p) => Arc::new(p),
                Err(_) => return,
            },
        };
        let state = self.clone();
        // Dropping/aborting the observer does not abort actual SQL/cache work.
        // Its existing writer permit is also passed to the cache retained owner,
        // so a late physical reply outlives both this task and logical deadline.
        let _ = tokio::spawn(async move {
            if let Ok(value) = work(permit.clone()).await {
                state.finish(&value);
            }
            drop(permit);
        })
        .await;
    }
}
impl AuthService {
    // Unlike pool.fetch_optional, explicit connection ownership keeps the
    // callback's original permit alive after ErrorResponse through actual Ready.
    async fn callback_system_settings(&self) -> Result<Value, StoreError> {
        async fn load(auth: &AuthService) -> Result<Value, StoreError> {
            let mut connection = auth.store.pool().acquire().await?;
            let result = sqlx::query(SYSTEM_SQL)
                .bind("system_settings")
                .fetch_optional(&mut *connection)
                .await;
            if let Err(drain_error) = sqlx::Connection::ping(&mut *connection).await {
                // Consume/close the actual connection under the same owner.
                // No second close, new pool, query retry or early lease release.
                let _ = connection.close().await;
                return Err(result.err().unwrap_or(drain_error).into());
            }
            // Await SQLx return/after_release instead of spawning Drop cleanup
            // after dropping the callback AuthService and its original token.
            connection.return_to_pool().await;
            match result? {
                Some(row) => {
                    let bytes: i32 = row.try_get("bytes")?;
                    let raw: Option<String> = row.try_get("value_text")?;
                    bounded_system_value(bytes, raw.as_deref())
                }
                None => Ok(crate::source_system_settings_defaults()),
            }
        }
        match &self.notification_cache {
            Some(cache) => cache.settings_group("system_settings", || load(self)).await,
            None => load(self).await,
        }
    }
    /// Reuse an actual ingress/writer lease; reject tokens from another budget.
    /// No new permit allowance or artificial caller owner is constructed.
    pub fn with_request_writer_permit(
        mut self,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> Result<Self, StoreError> {
        if !Arc::ptr_eq(permit.semaphore(), &self.user_agent_workers) {
            return Err(StoreError::Unsupported("Foreign request writer permit"));
        }
        self.request_writer_permit = Some(permit.clone());
        Ok(self.with_retained_scope(permit))
    }
    /// Original normal startup warms the system group without touching the
    /// request display-zone debounce. Startup failures keep the existing fallback.
    pub async fn startup_system_settings_group(&self) {
        let permit = match self.request_writer_permit.clone() {
            Some(permit) => permit,
            None => match self.user_agent_workers.clone().try_acquire_owned() {
                Ok(permit) => Arc::new(permit),
                Err(_) => return,
            },
        };
        let auth = self.clone();
        // Keep the same original writer owner through physical SQL/cache work
        // even if the startup observer is dropped. No new budget is created.
        let _ = tokio::spawn(async move {
            if let Ok(scoped) = auth.with_request_writer_permit(permit) {
                let _ = scoped.callback_system_settings().await;
            }
        })
        .await;
    }
    /// Explicit inert pre-auth hook. Main/API middleware activation is separate.
    /// Must run even for anonymous/malformed requests; never call only after auth.
    pub async fn request_started_display_timezone(&self) {
        self.refresh_display_timezone(false).await;
    }
    pub async fn refresh_display_timezone(&self, force: bool) {
        let auth = self.clone();
        self.display_timezone
            .refresh(
                force,
                self.user_agent_workers.clone(),
                self.request_writer_permit.clone(),
                move |permit| async move {
                    let scoped = auth.with_request_writer_permit(permit)?;
                    let settings = scoped.callback_system_settings().await?;
                    let value = settings
                        .get("time_zone")
                        .filter(|v| crate::channels::truthy(Some(v)))
                        .cloned()
                        .unwrap_or(Value::String("UTC".into()));
                    Ok(value)
                },
            )
            .await;
    }
    /// Source post_save setter: valid system time_zone stamps checked; invalid,
    /// absent or non-string values preserve both previous zone and checked time.
    pub fn set_display_timezone_on_settings_save(&self, key: &str, value: &Value) {
        if key == "system_settings" {
            self.display_timezone
                .set(&value.get("time_zone").cloned().unwrap_or(Value::Null));
        }
    }
    /// Display/logging consumers may read the cached zone without database I/O.
    pub fn display_timezone_name(&self) -> String {
        self.display_timezone.zone()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn zone() -> DisplayTimezone {
        DisplayTimezone::from_environment(Some("America/New_York"), None)
    }
    #[test]
    fn bounded_system_row_refuses_size_before_decode() {
        assert!(bounded_system_value(65537, None).is_err());
        assert!(bounded_system_value(65537, Some("{}")).is_err());
        assert!(bounded_system_value(-1, Some("{}")).is_err());
        assert!(bounded_system_value(3, Some("{}")).is_err());
        let exact = format!("\"{}\"", "x".repeat(65534));
        assert_eq!(exact.len(), 65536);
        assert_eq!(
            bounded_system_value(65536, Some(&exact)).unwrap(),
            crate::source_system_settings_defaults()
        );
    }
    #[test]
    fn bounded_system_row_refuses_node_amplification_and_depth() {
        let nodes = format!("[{}0]", "0,".repeat(2048));
        assert!(bounded_system_value(nodes.len() as i32, Some(&nodes)).is_err());
        let deep = format!("{}0{}", "[".repeat(33), "]".repeat(33));
        assert!(bounded_system_value(deep.len() as i32, Some(&deep)).is_err());
    }
    #[test]
    fn bounded_system_source_defaults_and_primitive_types() {
        for raw in ["null", "false", "0", "[]", "{}"] {
            assert_eq!(
                bounded_system_value(raw.len() as i32, Some(raw)).unwrap(),
                crate::source_system_settings_defaults()
            );
        }
        let raw = r#"{"time_zone":"UTC","enable_ip_lookup":false,"max_system_events":100}"#;
        assert_eq!(
            bounded_system_value(raw.len() as i32, Some(raw)).unwrap(),
            serde_json::json!({"time_zone":"UTC","enable_ip_lookup":false,"max_system_events":100})
        );
    }
    #[test]
    fn bounded_system_malformed_input_never_produces_group() {
        assert!(bounded_system_value(1, Some("{")).is_err());
    }
    #[test]
    fn environment_precedence_and_invalid_fallback() {
        assert_eq!(
            environment_zone(Some(""), Some("Europe/London")),
            "Europe/London"
        );
        assert_eq!(
            environment_zone(Some("invalid zone"), Some("Europe/London")),
            "UTC"
        );
        assert_eq!(environment_zone(None, None), "UTC");
    }
    #[tokio::test(start_paused = true)]
    async fn checked_before_failure_and_exact_five_seconds() {
        let z = zone();
        let n = Arc::new(AtomicUsize::new(0));
        let w = Arc::new(Semaphore::new(4));
        for wait in [0, 0, 4999, 1] {
            tokio::time::advance(Duration::from_millis(wait)).await;
            let n = n.clone();
            z.refresh(false, w.clone(), None, move |_| async move {
                n.fetch_add(1, Ordering::SeqCst);
                Err(StoreError::Unsupported("owned failure"))
            })
            .await;
        }
        assert_eq!(n.load(Ordering::SeqCst), 2);
        assert_eq!(z.zone(), "America/New_York");
    }
    #[tokio::test(start_paused = true)]
    async fn invalid_zone_preserves_previous_and_force_bypasses_debounce() {
        let z = zone();
        let w = Arc::new(Semaphore::new(4));
        z.refresh(false, w.clone(), None, |_| async {
            Ok(Value::String("Europe/London".into()))
        })
        .await;
        z.refresh(true, w, None, |_| async {
            Ok(Value::String("invalid zone".into()))
        })
        .await;
        assert_eq!(z.zone(), "Europe/London");
        assert!(!z.claim(false));
    }
    #[tokio::test(start_paused = true)]
    async fn setter_only_valid_system_zone_changes_state() {
        let z = zone();
        z.set(&Value::Null);
        assert!(z.claim(false));
        tokio::time::advance(REFRESH).await;
        z.set(&Value::String("Europe/London".into()));
        assert_eq!(z.zone(), "Europe/London");
        assert!(!z.claim(false));
        z.set(&Value::Bool(false));
        assert_eq!(z.zone(), "Europe/London");
    }
    #[tokio::test]
    async fn abort_observer_keeps_actual_work_and_permit_until_settlement() {
        let z = zone();
        let w = Arc::new(Semaphore::new(4));
        let (began, started) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel();
        let copy = z.clone();
        let workers = w.clone();
        let observer = tokio::spawn(async move {
            copy.refresh(false, workers, None, move |_| async move {
                let _ = began.send(());
                held.await.unwrap();
                Ok(Value::String("Europe/London".into()))
            })
            .await;
        });
        started.await.unwrap();
        assert_eq!(w.available_permits(), 3);
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        assert_eq!(w.available_permits(), 3);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while w.available_permits() != 4 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(z.zone(), "Europe/London");
    }
    #[tokio::test]
    async fn reuse_actual_permit_when_all_four_slots_are_owned() {
        let z = zone();
        let w = Arc::new(Semaphore::new(4));
        let shared = Arc::new(w.clone().acquire_owned().await.unwrap());
        let rest = w.clone().acquire_many_owned(3).await.unwrap();
        z.refresh(false, w.clone(), Some(shared.clone()), |_| async {
            Ok(Value::String("UTC".into()))
        })
        .await;
        assert_eq!(z.zone(), "UTC");
        assert_eq!(w.available_permits(), 0);
        drop(rest);
        drop(shared);
        assert_eq!(w.available_permits(), 4);
    }
    #[tokio::test]
    async fn saturated_budget_is_best_effort_but_debounced() {
        let z = zone();
        let w = Arc::new(Semaphore::new(0));
        z.refresh(false, w, None, |_| async { panic!("must not enter work") })
            .await;
        assert!(!z.claim(false));
        assert_eq!(z.zone(), "America/New_York");
    }
}

#[cfg(test)]
#[path = "display_timezone_physical_tests.rs"]
mod physical_tests;
