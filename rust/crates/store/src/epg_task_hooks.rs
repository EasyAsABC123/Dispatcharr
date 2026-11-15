//! One claimed native EPG task's lazy audit/template/plugin/SQL scope.
//! Original Dispatcharr/Django/Celery authors retain all algorithm credit.
use crate::{
    AuthService, NativeTaskTrail, PluginHookReport, PluginHookSession, PluginHookTaskContext,
    PluginRuntime, StoreError, TaskSqlSession,
};
use serde_json::Value;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc,
};
use tokio::sync::Mutex;
type Owner = Arc<dyn Send + Sync>;
#[derive(Clone)]
pub struct EpgTaskHooks {
    inner: Arc<Inner>,
}
/// Sealed completion of the actual scoped actor and physical SQL session.
/// It certifies no backend result or unrelated child publication.
pub struct CompletedEpgHooks {
    scope: Arc<Inner>,
    report: Option<PluginHookReport>,
}
impl CompletedEpgHooks {
    pub fn report(&self) -> Option<&PluginHookReport> {
        self.report.as_ref()
    }
}
/// A real handled download failure whose scoped context is NOT yet retired.
/// It cannot be used as the legacy physical-close Rejected proof.
#[derive(Clone)]
pub struct PendingXmlRejection {
    scope: Arc<Inner>,
    proof: crate::xmltv_download::Rejected,
}
impl std::fmt::Debug for PendingXmlRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingXmlRejection")
            .field("source", &self.proof.source_id())
            .field("kind", self.proof.kind())
            .finish()
    }
}
impl PartialEq for PendingXmlRejection {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.scope, &other.scope) && self.proof == other.proof
    }
}
impl Eq for PendingXmlRejection {}
impl PendingXmlRejection {
    pub fn source_id(&self) -> i64 {
        self.proof.source_id()
    }
    pub fn settle(
        self,
        completed: &CompletedEpgHooks,
    ) -> Result<crate::xmltv_download::Rejected, StoreError> {
        if !Arc::ptr_eq(&self.scope, &completed.scope) {
            return Err(StoreError::Unsupported(
                "XML rejection requires its actual completed scope",
            ));
        }
        Ok(self.proof)
    }
}
struct Inner {
    auth: AuthService,
    runtime: PluginRuntime,
    context: PluginHookTaskContext,
    trail: Arc<dyn NativeTaskTrail>,
    owner: Owner,
    cancelled: Arc<AtomicBool>,
    sql: Mutex<Option<TaskSqlSession>>,
    hooks: Mutex<Option<Arc<PluginHookSession>>>,
    phase: AtomicU8,
}
impl EpgTaskHooks {
    /// Only the worker's actual post-CAS raw frame and settled publication port
    /// are supplied; construction starts no actor or SQL connection.
    pub fn prepare(
        auth: AuthService,
        runtime: PluginRuntime,
        context: PluginHookTaskContext,
        trail: Arc<dyn NativeTaskTrail>,
        owner: Owner,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, StoreError> {
        if context.task_id() != trail.claimed_task_id() {
            return Err(StoreError::Unsupported("EPG hook task identity mismatch"));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                runtime,
                context,
                trail,
                owner,
                cancelled,
                sql: Mutex::new(None),
                hooks: Mutex::new(None),
                phase: AtomicU8::new(0),
            }),
        })
    }
    pub fn task_id(&self) -> &str {
        self.inner.context.task_id()
    }
    async fn session(&self) -> Result<TaskSqlSession, StoreError> {
        let mut sql = self.inner.sql.lock().await;
        if self.inner.phase.load(Ordering::Acquire) != 0
            || self.inner.cancelled.load(Ordering::Acquire)
        {
            return Err(StoreError::Unsupported("EPG hook task scope closed"));
        }
        if let Some(session) = sql.as_ref() {
            return Ok(session.clone());
        }
        // Accepted task backpressure retries ONLY confirmed unused writer
        // capacity. It never replays SQL, an HTTP request or a queue frame.
        let session = loop {
            match self
                .inner
                .auth
                .borrow_task_sql_session(self.inner.owner.clone())
                .await
            {
                Ok(session) => break session,
                Err(StoreError::StatusBody(503, _)) => {
                    if self.inner.phase.load(Ordering::Acquire) != 0
                        || self.inner.cancelled.load(Ordering::Acquire)
                    {
                        return Err(StoreError::Unsupported("EPG hook admission stopped"));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        };
        *sql = Some(session.clone());
        Ok(session)
    }
    pub(crate) async fn download_session(&self) -> Result<TaskSqlSession, StoreError> {
        self.session().await
    }
    pub(crate) fn pending_rejection(
        &self,
        proof: crate::xmltv_download::Rejected,
    ) -> PendingXmlRejection {
        PendingXmlRejection {
            scope: self.inner.clone(),
            proof,
        }
    }
    /// Source log_system_event internal order. Caller keeps its own independent
    /// Source status/WS-before/after-audit ordering. Known server failures are
    /// caught after actual Ready; unknown transport/actor fate stays an error.
    pub async fn audit(
        &self,
        event: &str,
        details: Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StoreError> {
        let scope = self.clone();
        let event = event.to_owned();
        tokio::spawn(async move { scope.audit_owned(event, details, now).await })
            .await
            .map_err(|_| StoreError::Unsupported("EPG audit supervisor unavailable"))?
    }
    async fn audit_owned(
        &self,
        event: String,
        details: Value,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StoreError> {
        let session = self.session().await?;
        let event_insert = event.clone();
        let payload = details.clone();
        let inserted=session.run(move|c|Box::pin(async move{sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$3,NULL,NULL,$2)").bind(event_insert).bind(details).bind(now).execute(c).await.map(|_|()).map_err(StoreError::from)})).await?;
        match inserted {
            Ok(()) => {}
            Err(error) if known(&error) => return Ok(()),
            Err(error) => return Err(error),
        }
        let wanted = event.clone();
        let enabled=session.run(move|c|Box::pin(async move{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event=$1) OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)").bind(wanted).fetch_one(c).await.map_err(StoreError::from)})).await?;
        let enabled = match enabled {
            Ok(enabled) => enabled,
            Err(error) if known(&error) => false,
            Err(error) => return Err(error),
        };
        if enabled {
            let mut guard = self.inner.hooks.lock().await;
            let hooks = match guard.as_ref() {
                Some(hooks) => hooks.clone(),
                None => {
                    let hooks = Arc::new(
                        self.inner
                            .runtime
                            .begin_plugin_hooks(
                                self.inner.context.clone(),
                                self.inner.trail.clone(),
                                session.clone(),
                                self.inner.owner.clone(),
                            )
                            .await?,
                    );
                    *guard = Some(hooks.clone());
                    hooks
                }
            };
            drop(guard);
            crate::epg_connect::dispatch_scoped_event(
                event,
                dispatch_payload(payload),
                session.clone(),
                hooks,
                self.inner.owner.clone(),
            )
            .await
            .map_err(|_| StoreError::Unsupported("EPG scoped integration outcome unavailable"))?;
        }
        let settings = match session.system_settings(self.inner.auth.clone()).await {
            Ok(settings) => settings,
            Err(error) if known(&error) => Value::Null,
            Err(error) => return Err(error),
        };
        let maximum = crate::sd_guide::max_events(settings.get("max_system_events"));
        if maximum < 0 {
            return Ok(());
        }
        let trimmed=session.run(move|c|Box::pin(async move{
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemevent").fetch_one(&mut*c).await?;
            if count>maximum {
                let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(maximum).fetch_optional(&mut*c).await?;
                if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(c).await?;}
            }
            Ok::<_,StoreError>(())
        })).await?;
        match trimmed {
            Ok(()) => Ok(()),
            Err(error) if known(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }
    /// Invoke after the ACTUAL native publisher has closed its trail and all
    /// reporters joined. Context retirement precedes this same SQL close.
    pub async fn finish(&self) -> Result<Option<PluginHookReport>, StoreError> {
        Ok(self.complete().await?.report)
    }
    pub async fn complete(&self) -> Result<CompletedEpgHooks, StoreError> {
        let scope = self.clone();
        tokio::spawn(async move {
            scope
                .inner
                .phase
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .map_err(|_| {
                    StoreError::Unsupported("EPG scope is already completing or aborted")
                })?;
            let hooks = scope.inner.hooks.lock().await.clone();
            let report = match hooks {
                Some(hooks) => Some(hooks.finish().await?),
                None => None,
            };
            if let Some(session) = scope.inner.sql.lock().await.as_ref() {
                session.close().await?;
            }
            scope.inner.phase.store(2, Ordering::Release);
            Ok(CompletedEpgHooks {
                scope: scope.inner.clone(),
                report,
            })
        })
        .await
        .map_err(|_| StoreError::Unsupported("EPG hook completion supervisor unavailable"))?
    }
    pub async fn abort(&self) -> Result<(), StoreError> {
        let scope = self.clone();
        tokio::spawn(async move {
            if scope.inner.phase.load(Ordering::Acquire) == 2 {
                return Ok(());
            }
            scope.inner.phase.store(3, Ordering::Release);
            if let Some(hooks) = scope.inner.hooks.lock().await.as_ref() {
                hooks.abort().await?;
            }
            if let Some(session) = scope.inner.sql.lock().await.as_ref() {
                session.close().await?;
            }
            scope.inner.phase.store(2, Ordering::Release);
            Ok(())
        })
        .await
        .map_err(|_| StoreError::Unsupported("EPG hook abort supervisor unavailable"))?
    }
}
/// core.utils.dispatch_event_system removes only top-level None/empty-string
/// keys immediately before trigger_event. Audit details remain original, and
/// later template/plugin mutations must NOT pass this filter a second time.
fn dispatch_payload(mut payload: Value) -> Value {
    if let Some(object) = payload.as_object_mut() {
        object.retain(|_, value| !value.is_null() && value.as_str() != Some(""));
    }
    payload
}
fn known(error: &StoreError) -> bool {
    matches!(error, StoreError::Database(sqlx::Error::Database(_)))
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.phase.load(Ordering::Acquire) == 2 {
            return;
        }
        let hooks = self.hooks.get_mut().take();
        let sql = self.sql.get_mut().take();
        let owner = self.owner.clone();
        if hooks.is_none() && sql.is_none() {
            return;
        }
        // One actual Inner destructor, not Arc strong-count guesses. Losing the
        // observer never loses context/SQL ownership or emits a fake terminal.
        tokio::spawn(async move {
            let _owner = owner;
            if let Some(hooks) = hooks {
                if hooks.abort().await.is_err() {
                    eprintln!("Unobserved XML hook scope abort remains unsettled");
                    return;
                }
            }
            if let Some(session) = sql {
                if session.close().await.is_err() {
                    eprintln!("Unobserved XML hook SQL close remains unsettled");
                }
            }
        });
    }
}

#[cfg(test)]
mod dispatch_payload_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn source_dispatch_filter_keeps_false_zero_containers_and_audit_details() {
        let audit = json!({"source_name":"","missing":null,"speed":0.0,"flag":false,"array":[],"object":{},"space":" ","nested":{"empty":"","null":null}});
        let mut sent = dispatch_payload(audit.clone());
        assert_eq!(
            sent,
            json!({"speed":0.0,"flag":false,"array":[],"object":{},"space":" ","nested":{"empty":"","null":null}})
        );
        assert_eq!(audit["source_name"], json!(""));
        assert!(audit.get("missing").is_some_and(Value::is_null));
        // The filter is before Connect; Source shared template/plugin additions are
        // retained verbatim and never re-filtered between ordered action calls.
        sent["later_empty"] = json!("");
        sent["later_null"] = Value::Null;
        assert!(sent.get("later_empty").is_some());
        assert!(sent.get("later_null").is_some());
    }
}
