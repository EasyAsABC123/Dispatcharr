//! Normal mapped/single SD guide service over admitted Main SQL/cache/events.
//! Original Dispatcharr authors retain all task, API and ingestion credit.
use super::{
    sd_guide_auth::{self as auth, Source},
    sd_guide_fetch::{self as fetch, Outcome},
    sd_guide_post, sd_guide_posters,
    sd_guide_progress::Progress,
    sd_guide_state::State,
};
use crate::{AuthService, StoreError, TaskSqlSession};
use chrono::Utc;
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
#[derive(Clone, Copy)]
pub enum Request {
    Mapped { source: i64, force: bool },
    Single { epg: i64, force: bool },
}
#[derive(Debug)]
pub enum Error {
    Store(StoreError),
    Unsupported,
    Capacity,
    Operational,
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::Store(StoreError::Database(e))
    }
}
impl From<auth::Error> for Error {
    fn from(e: auth::Error) -> Self {
        match e {
            auth::Error::Store(e) => Self::Store(e),
            auth::Error::Capacity => Self::Capacity,
            auth::Error::Unsupported => Self::Unsupported,
            auth::Error::Http(_) => Self::Operational,
        }
    }
}
impl From<fetch::Error> for Error {
    fn from(e: fetch::Error) -> Self {
        match e {
            fetch::Error::Auth(e) => e.into(),
            fetch::Error::State(super::sd_guide_state::Error::Store(e)) => Self::Store(e),
            fetch::Error::State(super::sd_guide_state::Error::Capacity)
            | fetch::Error::Projection(super::sd_guide_programme::Error::Capacity) => {
                Self::Capacity
            }
            fetch::Error::State(_) | fetch::Error::Projection(_) | fetch::Error::Unsupported => {
                Self::Unsupported
            }
            _ => Self::Operational,
        }
    }
}
impl From<sd_guide_post::Error> for Error {
    fn from(e: sd_guide_post::Error) -> Self {
        match e {
            sd_guide_post::Error::Store(e) => Self::Store(e),
            sd_guide_post::Error::Unsupported => Self::Unsupported,
        }
    }
}
impl From<sd_guide_posters::Error> for Error {
    fn from(e: sd_guide_posters::Error) -> Self {
        match e {
            sd_guide_posters::Error::Auth(e) => e.into(),
            sd_guide_posters::Error::Store(e) => Self::Store(e),
            sd_guide_posters::Error::Capacity => Self::Capacity,
            sd_guide_posters::Error::Unsupported => Self::Unsupported,
        }
    }
}
pub struct Admission {
    source: Option<Source>,
    wrong_type: bool,
    pub(crate) session: TaskSqlSession,
    request: Request,
    already_present: bool,
    events: Arc<dyn UpdateEventSink>,
    owner: Owner,
    hook_runtime: Option<crate::PluginRuntime>,
    hook_task: Option<HookTask>,
    hooks: tokio::sync::Mutex<Option<Arc<crate::PluginHookSession>>>,
}
struct HookTask {
    context: crate::PluginHookTaskContext,
    trail: Arc<dyn crate::NativeTaskTrail>,
    owner: Owner,
}
impl Admission {
    pub async fn prepare(
        auth: AuthService,
        request: Request,
        events: Arc<dyn UpdateEventSink>,
        owner: Owner,
    ) -> Result<Self, Error> {
        Self::prepare_inner(auth, request, events, owner, None).await
    }
    pub async fn prepare_with_hook_runtime(
        auth: AuthService,
        request: Request,
        events: Arc<dyn UpdateEventSink>,
        owner: Owner,
        runtime: crate::PluginRuntime,
    ) -> Result<Self, Error> {
        Self::prepare_inner(auth, request, events, owner, Some(runtime)).await
    }
    async fn prepare_inner(
        auth: AuthService,
        request: Request,
        events: Arc<dyn UpdateEventSink>,
        owner: Owner,
        hook_runtime: Option<crate::PluginRuntime>,
    ) -> Result<Self, Error> {
        let session = auth.borrow_task_sql_session(owner.clone()).await?;
        let read=async{
   let (source_id,already_present)=match request{Request::Mapped{source,..}=>(Some(source),false),Request::Single{epg,..}=>session.run(move|c|Box::pin(async move{let row=sqlx::query("SELECT epg_source_id::bigint,EXISTS(SELECT 1 FROM epg_programdata p WHERE p.epg_id=e.id) AS has_data FROM epg_epgdata e WHERE e.id=$1").bind(epg).fetch_optional(c).await?;match row{None=>Ok::<(Option<i64>,bool),sqlx::Error>((None,false)),Some(row)=>Ok((row.try_get("epg_source_id")?,row.try_get("has_data")?))}})).await??};
   let (source,wrong_type)=match source_id{None=>(None,false),Some(id)=>match Source::read(id,session.clone(),auth.clone(),owner.clone()).await?{auth::Read::Missing=>(None,false),auth::Read::Other=>(None,true),auth::Read::Source(source)=>(Some(*source),false)}};
   // Legacy constructors remain gated. The typed real runtime supplies
   // the scoped template/plugin port; scripts still need their physical port.
   let bound=hook_runtime.is_some();
   let forbidden=session.run(move|c|Box::pin(async move{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event IN('epg_refresh','epg_error') AND (NOT $1 OR i.type='script')) OR (NOT $1 AND EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled))").bind(bound).fetch_one(c).await.map_err(StoreError::from)})).await??;
   if forbidden{return Err(Error::Unsupported)}
   Ok::<_,Error>((source,wrong_type,already_present))
  }.await;
        match read {
            Ok((source, wrong_type, already_present)) => Ok(Self {
                source,
                wrong_type,
                session,
                request,
                already_present,
                events,
                owner,
                hook_runtime,
                hook_task: None,
                hooks: tokio::sync::Mutex::new(None),
            }),
            Err(error) => {
                session.close().await?;
                Err(error)
            }
        }
    }
    /// Bind actual already-claimed bytes only. Empty/default event paths start
    /// no interpreter; enabled subscriptions/plugins lazily acquire the real
    /// task actor only at the idle audit boundary after INSERT.
    pub fn bind_native_hooks(
        &mut self,
        raw: &[u8],
        trail: Arc<dyn crate::NativeTaskTrail>,
        owner: Owner,
    ) -> Result<(), Error> {
        if self.hook_runtime.is_some() {
            self.hook_task = Some(HookTask {
                context: crate::PluginHookTaskContext::from_native_claim(raw)?,
                trail,
                owner,
            });
        }
        Ok(())
    }
    async fn event_hooks(
        &self,
        event: &str,
    ) -> Result<Option<Arc<crate::PluginHookSession>>, Error> {
        let event = event.to_owned();
        let needed=self.session.run(move|c|Box::pin(async move{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event=$1) OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)").bind(event).fetch_one(c).await.map_err(StoreError::from)})).await??;
        if !needed {
            return Ok(None);
        }
        let mut stored = self.hooks.lock().await;
        if let Some(hooks) = stored.as_ref() {
            return Ok(Some(hooks.clone()));
        }
        let runtime = self.hook_runtime.as_ref().ok_or(Error::Unsupported)?;
        let task = self.hook_task.as_ref().ok_or(Error::Unsupported)?;
        let hooks = Arc::new(
            runtime
                .begin_plugin_hooks(
                    task.context.clone(),
                    task.trail.clone(),
                    self.session.clone(),
                    task.owner.clone(),
                )
                .await?,
        );
        *stored = Some(hooks.clone());
        Ok(Some(hooks))
    }
    pub async fn finish_hooks(&self) -> Result<Option<crate::PluginHookReport>, Error> {
        let hooks = self.hooks.lock().await.clone();
        match hooks {
            Some(hooks) => Ok(Some(hooks.finish().await?)),
            None => Ok(None),
        }
    }
    pub fn source_id(&self) -> Option<i64> {
        self.source.as_ref().map(|s| s.id)
    }
    pub fn early_result(&self) -> Option<Value> {
        match (self.request, self.source.is_some(), self.already_present) {
            (Request::Mapped { .. }, false, _) => Some(if self.wrong_type {
                json!("Not a Schedules Direct source")
            } else {
                Value::Null
            }),
            (Request::Single { .. }, false, _) => Some(json!("Not a Schedules Direct EPG entry")),
            (Request::Single { force: false, .. }, true, true) => {
                Some(json!("Guide data already present"))
            }
            _ => None,
        }
    }
    pub async fn close(&self) -> Result<(), Error> {
        let hooks = self.hooks.lock().await.clone();
        if let Some(hooks) = hooks {
            hooks.abort().await?;
        }
        self.session.close().await?;
        Ok(())
    }
    async fn event(
        &self,
        source: i64,
        action: &str,
        progress: i64,
        status: Option<&str>,
        message: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let mut data =
            json!({"progress":progress,"type":"epg_refresh","source":source,"action":action});
        if let Some(s) = status {
            data["status"] = json!(s)
        }
        if let Some(s) = message {
            data["message"] = json!(s)
        }
        if let Some(s) = error {
            data["error"] = json!(s)
        }
        let _ = self
            .events
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                self.owner.clone(),
            )
            .await;
        Ok(())
    }
    async fn status(&self, id: i64, status: Option<&str>, message: &str) -> Result<(), Error> {
        let status = status.map(str::to_owned);
        let message = message.to_owned();
        self.session
            .run(move |c| {
                Box::pin(async move {
                    match status {
                        Some(status) => {
                            sqlx::query(
                                "UPDATE epg_epgsource SET status=$2,last_message=$3 WHERE id=$1",
                            )
                            .bind(id)
                            .bind(status)
                            .bind(message)
                            .execute(c)
                            .await?;
                        }
                        None => {
                            sqlx::query("UPDATE epg_epgsource SET last_message=$2 WHERE id=$1")
                                .bind(id)
                                .bind(message)
                                .execute(c)
                                .await?;
                        }
                    }
                    Ok::<_, StoreError>(())
                })
            })
            .await??;
        Ok(())
    }
    async fn audit(&self, kind: &str, details: Value) -> Result<(), Error> {
        let event_kind = kind.to_owned();
        let event_payload = details.clone();
        let kind = kind.to_owned();
        let inserted=self.session.run(move|c|Box::pin(async move{sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$3,NULL,NULL,$2)").bind(kind).bind(details).bind(Utc::now()).execute(c).await.map(|_|()).map_err(StoreError::from)})).await?;
        match inserted {
            Ok(()) => {}
            Err(StoreError::Database(sqlx::Error::Database(_))) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        if let Some(hooks) = self.event_hooks(&event_kind).await? {
            // A known Connect abort skips plugins but still permits the Source
            // outer audit trim. Unsupported/unknown physical outcomes fault.
            crate::epg_connect::dispatch_scoped_event(
                event_kind,
                event_payload,
                self.session.clone(),
                hooks.clone(),
                self.owner.clone(),
            )
            .await
            .map_err(|_| Error::Operational)?;
        }
        // Source reads current cached settings AFTER actual event creation, not
        // once at admission before a potentially long provider operation.
        let auth = self.source.as_ref().ok_or(Error::Unsupported)?.auth.clone();
        let settings = self.session.system_settings(auth).await?;
        let maximum = max_events(settings.get("max_system_events"));
        if maximum < 0 {
            return Ok(());
        } // Source's negative queryset index is caught.
        let trimmed=self.session.run(move|c|Box::pin(async move{let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(maximum).fetch_optional(&mut*c).await?;if let Some(id)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(id).execute(c).await?;}Ok::<_,StoreError>(())})).await?;
        match trimmed {
            Ok(()) | Err(StoreError::Database(sqlx::Error::Database(_))) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    pub async fn execute(
        &mut self,
        stopped: &(dyn Fn() -> bool + Send + Sync),
    ) -> Result<Value, Error> {
        if let Some(value) = self.early_result() {
            return Ok(value);
        }
        let id = self.source_id().ok_or(Error::Unsupported)?;
        let single = match self.request {
            Request::Single { epg, .. } => Some(epg),
            _ => None,
        };
        if self
            .source
            .as_ref()
            .is_some_and(|s| s.username.is_empty() || s.password.is_empty())
        {
            let message = "Schedules Direct source requires both a username and password.";
            let name = self.source.as_ref().ok_or(Error::Unsupported)?.name.clone();
            self.status(id, Some("error"), message).await?;
            self.event(id, "refresh", 100, Some("error"), None, Some(message))
                .await?;
            self.audit("epg_error", json!({"source_name":name,"message":message}))
                .await?;
            return Ok(json!(if single.is_some() {
                "SD guide fetch complete"
            } else {
                "SD mapped guide batch complete"
            }));
        }
        self.event(
            id,
            "parsing_programs",
            2,
            None,
            Some("Authenticating with Schedules Direct..."),
            None,
        )
        .await?;
        // Python date.today observes the container's actual local date; dates are
        // explicitly UTC when constructing programme/cache windows later.
        let mut state = State::begin(
            self.session.clone(),
            id,
            single,
            chrono::Local::now().date_naive(),
        )
        .await
        .map_err(|e| match e {
            super::sd_guide_state::Error::Store(e) => Error::Store(e),
            _ => Error::Unsupported,
        })?;
        let progress = Progress::new(id, self.events.clone(), self.owner.clone());
        let source = self.source.as_mut().ok_or(Error::Unsupported)?;
        let name = source.name.clone();
        let result = fetch::ingest(source, &mut state, stopped, &progress).await?;
        let parsing_error = matches!(&result, Outcome::Md5Unavailable | Outcome::NoSchedules);
        let empty_mapping = matches!(
            &result,
            Outcome::EmptyMapping | Outcome::SkippedSingle { .. }
        );
        let setup_error = matches!(
            &result,
            Outcome::InvalidStations | Outcome::InvalidSingle(_)
        );
        let mut success_audit = None;
        let (status,message,error,success)=match result{
   Outcome::AuthDeclined(auth)=>{let status=if auth.soft{"idle"}else{"error"};(Some(status),auth.message,!auth.soft,false)},
   Outcome::Offline=>(Some("idle"),"Schedules Direct system is currently offline. Do not retry for at least 1 hour.".into(),false,false),
   Outcome::EmptyMapping=>(None,"No channels mapped to this Schedules Direct source.".into(),false,false),
   Outcome::InvalidStations=>(None,"Mapped channels have no valid Schedules Direct station IDs.".into(),true,false),
   Outcome::InvalidSingle(epg)=>(None,format!("Schedules Direct EPG entry {epg} not found or missing station ID."),true,false),
   Outcome::SkippedSingle{mut token}=>{sd_guide_posters::run(source,&state,&mut token,&progress).await.map_err(Error::from)?;sd_guide_post::run(source,&state).await?;(None,"No mapped channel found for this EPG entry; guide fetch skipped.".into(),false,false)},
   Outcome::Md5Unavailable=>(Some("error"),"Failed to fetch schedule MD5s from Schedules Direct, guide was not refreshed this cycle.".into(),true,false),
   Outcome::NoSchedules=>(Some("error"),"No schedule data returned from Schedules Direct.".into(),true,false),
   Outcome::Unchanged{mut token}=>{let posters=sd_guide_posters::run(source,&state,&mut token,&progress).await.map_err(Error::from)?;sd_guide_post::run(source,&state).await?;(None,if posters==0{"No schedule changes detected. Guide data is up to date.".into()}else{format!("No schedule changes detected. Updated poster artwork for {posters} program{}.",if posters==1{""}else{"s"})},false,false)},
   Outcome::Written{programmes,stations,skipped_unmapped,mut token}=>{
    sd_guide_posters::run(source,&state,&mut token,&progress).await.map_err(Error::from)?;
    sd_guide_post::run(source,&state).await?;
    let message=if let Some(epg)=single{let label:Option<String>=self.session.run(move|c|Box::pin(async move{sqlx::query_scalar("SELECT name FROM epg_epgdata WHERE id=$1").bind(epg).fetch_optional(c).await.map_err(StoreError::from)})).await??;format!("Fetched {} programs for {} from Schedules Direct.",commas(programmes),label.filter(|v|!v.is_empty()).unwrap_or_else(||epg.to_string()))}else{format!("Fetched {} programs for {} mapped stations from Schedules Direct ({} programs skipped for unmapped stations).",commas(programmes),stations,commas(skipped_unmapped))};
    success_audit=Some(json!({"source_name":name,"programs":programmes,"channels":if single.is_some(){1}else{stations},"skipped_programs":skipped_unmapped}));(None,message,false,true)
   }
  };
        self.status(id, status, &message).await?;
        let action = if status.is_some() && !parsing_error {
            "refresh"
        } else {
            "parsing_programs"
        };
        self.event(
            id,
            action,
            100,
            Some(if error {
                "error"
            } else if !empty_mapping && (success || status.is_none()) {
                "success"
            } else {
                "idle"
            }),
            if error { None } else { Some(&message) },
            if error { Some(&message) } else { None },
        )
        .await?;
        if let Some(details) = success_audit {
            self.audit("epg_refresh", details).await?;
        }
        if error && !setup_error {
            self.audit("epg_error", json!({"source_name":name,"message":message}))
                .await?;
        }
        Ok(json!(if single.is_some() {
            "SD guide fetch complete"
        } else {
            "SD mapped guide batch complete"
        }))
    }
}
pub(crate) fn max_events(value: Option<&Value>) -> i64 {
    let Some(value) = value.filter(|v| crate::xc_live_provider::truthy(v)) else {
        return 100;
    };
    match value {
        Value::Bool(value) => i64::from(*value),
        Value::Number(value)
            if !value
                .to_string()
                .bytes()
                .any(|b| matches!(b, b'.' | b'e' | b'E')) =>
        {
            value.as_i64().unwrap_or_else(|| {
                if value.to_string().starts_with('-') {
                    -1
                } else {
                    i64::MAX
                }
            })
        }
        Value::Number(value) => {
            value
                .as_i64()
                .unwrap_or_else(|| match value.as_f64().filter(|n| n.is_finite()) {
                    Some(number) if number >= i64::MAX as f64 => i64::MAX,
                    Some(number) if number <= i64::MIN as f64 => -1,
                    Some(number) => number.trunc() as i64,
                    None => 100,
                })
        }
        Value::String(value) => match dispatcharr_ingest_core::source_integer_literal(value) {
            None => 100,
            Some(integer) => integer.parse().unwrap_or_else(|_| {
                if integer.starts_with('-') {
                    -1
                } else {
                    i64::MAX
                }
            }),
        },
        _ => 100,
    }
}
fn commas(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',')
        }
        out.push(c)
    }
    out
}

#[cfg(test)]
mod policy_controls {
    use super::*;
    #[test]
    fn source_or_then_int_coercion_retains_string_zero_and_negative_catch() {
        assert_eq!(max_events(Some(&json!(0))), 100);
        assert_eq!(max_events(Some(&json!("0"))), 0);
        assert_eq!(max_events(Some(&json!(1.9))), 1);
        assert_eq!(max_events(Some(&json!(-2))), -2);
        assert_eq!(max_events(Some(&json!("not an integer"))), 100);
        assert_eq!(max_events(Some(&json!(true))), 1);
        assert_eq!(max_events(Some(&json!([1]))), 100);
    }
}
