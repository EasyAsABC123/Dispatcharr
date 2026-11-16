//! Actual recording-task Connect composition, restricted to empty plugin domain.
//! Original Dispatcharr/Django/Celery authors retain application/algorithm credit.
use super::*;
use crate::{
    AuthService, NativeTaskTrail, PluginHookSession, PluginHookTaskContext, PluginRuntime,
    StoreError, TaskSqlSession,
};
use std::sync::Mutex;
type Owner = Arc<dyn Send + Sync>;
pub(super) struct Ports {
    auth: AuthService,
    runtime: PluginRuntime,
    // Every entry retains one of the SAME four original Auth writer permits.
    // No eviction or synthetic settled receipt is allowed on unknown close.
    uncertain: Mutex<Vec<UnknownEvent>>,
}
struct UnknownEvent {
    _session: TaskSqlSession,
    _hooks: Option<Arc<PluginHookSession>>,
    _owner: Owner,
}
fn scoped(error: StoreError) -> Error {
    Error::Scoped(Box::new(error))
}
impl CaptureStore {
    pub fn bind_recording_connect(
        &self,
        auth: AuthService,
        runtime: PluginRuntime,
    ) -> Result<(), Error> {
        if !std::ptr::eq(self.pool.options(), auth.store.pool().options())
            || !runtime.shares_recording_auth(&auth)
            || auth.request_writer_permit.is_some()
        {
            return Err(Error::Unsupported(
                "Recording Connect requires actual shared Main Auth/pool ports",
            ));
        }
        self.recording_connect
            .set(Ports {
                auth,
                runtime,
                uncertain: Mutex::new(Vec::with_capacity(4)),
            })
            .map_err(|_| Error::Unsupported("Recording Connect ports already bound"))
    }
    pub fn recording_connect_bound(&self) -> bool {
        self.recording_connect.get().is_some()
    }
    pub(super) async fn read_unbound_event_domain(
        connection: &mut PgConnection,
        kind: NormalEvent,
    ) -> Result<(), Error> {
        let forbidden:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event=$1)")
            .bind(kind.as_str()).fetch_one(connection).await.map_err(|source|Error::Sql{stage:Stage::Context,source,settlement:None})?;
        if forbidden {
            return Err(Error::Unsupported(
                "Recording event requires actual claimed task Connect ports",
            ));
        }
        Ok(())
    }
    /// Recovery/HTTP cancellation have no actual recording-task context yet.
    /// Their real callers must invoke this before claim/effects, not discard hooks.
    pub async fn require_unbound_event_domain(
        self: Arc<Self>,
        kind: NormalEvent,
        owner: Owner,
    ) -> Result<(), Error> {
        tokio::spawn(async move {
            let _owner = owner;
            let mut connection = self.pool.acquire().await.map_err(|source| Error::Sql {
                stage: Stage::Acquire,
                source,
                settlement: None,
            })?;
            let result = Self::read_unbound_event_domain(&mut connection, kind).await;
            settle(connection, result).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    async fn recording_connect_needed(
        self: Arc<Self>,
        kind: NormalEvent,
        owner: Owner,
    ) -> Result<bool, Error> {
        tokio::spawn(async move{
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|Error::Sql{stage:Stage::Acquire,source,settlement:None})?;
            let result=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event=$1)")
                .bind(kind.as_str()).fetch_one(&mut*connection).await.map_err(|source|Error::Sql{stage:Stage::Context,source,settlement:None});
            settle(connection,result).await
        }).await.map_err(|_|Error::Supervisor)?
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_claimed_runtime_event(
        self: Arc<Self>,
        kind: NormalEvent,
        channel: Uuid,
        channel_name: String,
        details: Value,
        legacy_maximum: i64,
        context: PluginHookTaskContext,
        trail: Arc<dyn NativeTaskTrail>,
        owner: Owner,
    ) -> Result<Option<i64>, Error> {
        if context.task_name() != "apps.channels.tasks.run_recording"
            || context.task_id() != trail.claimed_task_id()
        {
            return Err(Error::Unsupported(
                "Recording event actual claim identity mismatch",
            ));
        }
        if channel_name.len() > 255
            || !details.is_object()
            || serde_json::to_vec(&details)
                .map_err(|_| Error::Unsupported("Recording event encoding"))?
                .len()
                > 8192
        {
            return Err(Error::Unsupported("Recording event capacity"));
        }
        if !self
            .clone()
            .recording_connect_needed(kind, owner.clone())
            .await?
        {
            return self
                .insert_runtime_event(kind, channel, channel_name, details, legacy_maximum, owner)
                .await;
        }
        let ports = self
            .recording_connect
            .get()
            .ok_or(Error::Unsupported("Recording Connect runtime unbound"))?;
        let session = ports
            .auth
            .borrow_task_sql_session(owner.clone())
            .await
            .map_err(scoped)?;
        let mut hooks: Option<Arc<PluginHookSession>> = None;
        let result=async{
            // Live guard remains even after actual pre-CAS normal snapshot.
            let forbidden=session.run(|c|Box::pin(async move{
                sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM plugins_pluginconfig) OR EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event IN('recording_start','recording_end') AND i.type='script')").fetch_one(c).await
            })).await.map_err(scoped)?.map_err(|source|Error::Sql{stage:Stage::Context,source,settlement:None})?;
            if forbidden{return Err(Error::Unsupported("Recording script/plugin execution remains unsupported"))}
            let event=kind.as_str().to_owned();let audit_event=event.clone();let audit_details=details.clone();let audit_name=channel_name.clone();
            let inserted=session.run(move|c|Box::pin(async move{
                sqlx::query_scalar::<_,i64>("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$2,$3,$4,$5)RETURNING id::bigint")
                    .bind(audit_event).bind(Utc::now()).bind(channel).bind(audit_name).bind(audit_details).fetch_one(c).await
            })).await.map_err(scoped)?;
            let id=match inserted{
                Ok(id)=>id,
                Err(sqlx::Error::Database(_))=>return Ok(None),
                Err(source)=>return Err(Error::Sql{stage:Stage::Audit,source,settlement:None}),
            };
            let payload=super::enrichment::enrich(&ports.auth,&session,Some(channel),Some(channel_name),details,owner.clone()).await.map_err(scoped)?;
            let current=Arc::new(ports.runtime.begin_plugin_hooks(context,trail,session.clone(),owner.clone()).await.map_err(scoped)?);
            hooks=Some(current.clone());
            // Reuses actual scoped Template peer and HTTP/DeliveryLog ports.
            // Empty plugin/config + script guards remain; no Source task body.
            crate::epg_connect::dispatch_scoped_event(event,payload,session.clone(),current.clone(),owner.clone()).await.map_err(|e|Error::Connect(Box::new(e)))?;
            let actual=current.finish().await.map_err(scoped)?;
            if !actual.actual_published_children.is_empty()||actual.observed_request_children!=serde_json::json!([]){return Err(Error::Unsupported("Recording Connect-only event unexpectedly published children"))}
            // Source reads max history AFTER Connect/plugin phase, using live cache.
            let settings=session.system_settings(ports.auth.clone()).await.map_err(scoped)?;
            let maximum=crate::sd_guide::max_events(settings.get("max_system_events"));
            if maximum>=0{
                let trim=session.run(move|c|Box::pin(async move{
                    let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(maximum).fetch_optional(&mut*c).await?;
                    if let Some(cutoff)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(cutoff).execute(c).await?;}Ok::<_,sqlx::Error>(())
                })).await.map_err(scoped)?;
                match trim{Ok(())=>{},Err(sqlx::Error::Database(_))=>return Ok(None),Err(source)=>return Err(Error::Sql{stage:Stage::Audit,source,settlement:None})}
            }
            Ok(Some(id))
        }.await;
        if result.is_err() {
            if let Some(active) = &hooks {
                if let Err(error) = active.abort().await {
                    ports
                        .uncertain
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(UnknownEvent {
                            _session: session,
                            _hooks: hooks,
                            _owner: owner,
                        });
                    return Err(scoped(error));
                }
            }
        }
        if let Err(error) = session.close().await {
            ports
                .uncertain
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(UnknownEvent {
                    _session: session,
                    _hooks: hooks,
                    _owner: owner,
                });
            return Err(scoped(error));
        }
        // Explicit finish/abort and SQL closure precede actor/owner release.
        drop(hooks);
        result
    }
}
