//! Actual bounded Source fetch_xmltv port over shared Main Auth/SQL/HTTP ports.
//! Original Dispatcharr/Requests contributors retain application/algorithm credit.
use crate::{AuthService, StoreError, TaskSqlSession};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_upstream_http::Attempt;
use futures_util::StreamExt;
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
#[path = "xmltv_download_files.rs"]
mod files;
#[path = "xmltv_download_http_diagnostics.rs"]
mod http_diagnostics;
type Owner = Arc<dyn Send + Sync>;
const DEFAULT_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0";
#[derive(Debug)]
pub enum Error {
    Store(StoreError),
    Capacity,
    Unsupported,
    Cancelled,
    BodyReadTimeout,
    Unsettled(&'static str),
}
impl From<StoreError> for Error {
    fn from(value: StoreError) -> Self {
        match value {
            StoreError::StatusBody(503, _) => Self::Capacity,
            other => Self::Store(other),
        }
    }
}
#[derive(Clone)]
pub struct Downloader {
    inner: Arc<Inner>,
}
struct Inner {
    auth: AuthService,
    events: Arc<dyn UpdateEventSink>,
    directory: files::Directory,
}
pub struct Admission {
    download: Downloader,
    source: Source,
    session: TaskSqlSession,
    owner: Owner,
    missing_tvg: bool,
    hooks: Option<crate::epg_task_hooks::EpgTaskHooks>,
}
struct Source {
    id: i64,
    name: String,
    url: String,
    agent: String,
    file_path: Option<String>,
}
pub struct Installed {
    source: i64,
    path: PathBuf,
}
impl Installed {
    pub fn source_id(&self) -> i64 {
        self.source
    }
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejection {
    MissingUrl,
    Http(u16),
    Incomplete,
    Extraction,
    Connection,
    Timeout,
}
/// Minted only by actual handled fetch failure after status/audit SQL settlement;
/// execute additionally fences physical close before returning it to a caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejected {
    source: i64,
    kind: Rejection,
}
impl Rejected {
    pub fn source_id(&self) -> i64 {
        self.source
    }
    pub fn kind(&self) -> &Rejection {
        &self.kind
    }
}
pub enum Outcome {
    Installed(Installed),
    HandledFailure(Rejected),
}
/// Source effects have settled but the actual scoped actor/SQL may remain open.
/// HandledFailure cannot become legacy Rejected without its completed scope.
pub enum ScopedOutcome {
    Installed(Installed),
    HandledFailure(crate::epg_task_hooks::PendingXmlRejection),
}
impl Downloader {
    pub fn prepare(
        auth: AuthService,
        events: Arc<dyn UpdateEventSink>,
        cache: PathBuf,
    ) -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                events,
                directory: files::Directory::prepare(cache).map_err(|_| Error::Unsupported)?,
            }),
        })
    }
    pub fn cache_seed(&self, id: i64) -> Result<PathBuf, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&id) {
            return Err(Error::Unsupported);
        }
        Ok(self.inner.directory.cache_seed(id))
    }
    pub async fn admit(&self, id: i64, owner: Owner) -> Result<Admission, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&id) {
            return Err(Error::Unsupported);
        }
        let session = self
            .inner
            .auth
            .borrow_task_sql_session(owner.clone())
            .await?;
        self.admit_on(id, owner, session, None).await
    }
    async fn admit_on(
        &self,
        id: i64,
        owner: Owner,
        session: TaskSqlSession,
        hooks: Option<crate::epg_task_hooks::EpgTaskHooks>,
    ) -> Result<Admission, Error> {
        let read=async {
   let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT CASE WHEN octet_length(name)<=65536 THEN name END AS name,CASE WHEN octet_length(url)<=65536 THEN url END AS url,source_type,CASE WHEN octet_length(file_path)<=4096 THEN file_path END AS file_path,octet_length(name)>65536 OR coalesce(octet_length(url)>65536,false) OR coalesce(octet_length(file_path)>4096,false) AS oversized FROM epg_epgsource WHERE id=$1").bind(id).fetch_optional(c).await.map_err(StoreError::from)})).await??;
   let row=row.ok_or(Error::Unsupported)?;if row.try_get::<bool,_>("oversized").map_err(StoreError::from)?||row.try_get::<String,_>("source_type").map_err(StoreError::from)?!="xmltv" {return Err(Error::Unsupported)}
   let name:String=row.try_get("name").map_err(StoreError::from)?;let url:Option<String>=row.try_get("url").map_err(StoreError::from)?;let url=url.unwrap_or_default();
   let forbidden=session.run(|c|Box::pin(async move{sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event='epg_error') OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled)").fetch_one(c).await.map_err(StoreError::from)})).await??;
   if forbidden && hooks.is_none(){return Err(Error::Unsupported)}
   let group=stream_settings(&session,&self.inner.auth).await?;
   let agent_id=group.get("default_user_agent").filter(|v|crate::xc_live_provider::truthy(v)).and_then(source_integer);
   let agent=if let Some(agent_id)=agent_id {
    let agent:Option<(Option<bool>,Option<String>)>=session.run(move|c|Box::pin(async move{sqlx::query_as("SELECT octet_length(user_agent)>65536,CASE WHEN octet_length(user_agent)<=65536 THEN user_agent END FROM core_useragent WHERE id=$1").bind(agent_id).fetch_optional(c).await.map_err(StoreError::from)})).await??;
    match agent {Some((Some(true),_))=>return Err(Error::Capacity),Some((_,Some(value)))if !value.is_empty()=>value,_=>DEFAULT_AGENT.into()}
   }else{DEFAULT_AGENT.into()};
   Ok::<_,Error>(Source{id,name,url,agent,file_path:row.try_get("file_path").map_err(StoreError::from)?})
  }.await;
        match read {
            Ok(source) => Ok(Admission {
                download: self.clone(),
                source,
                owner: Arc::new((owner, session.clone())),
                session,
                missing_tvg: false,
                hooks,
            }),
            Err(error) => {
                if hooks.is_none() {
                    session.close().await?;
                }
                Err(error)
            }
        }
    }
    pub async fn run_scoped(
        &self,
        id: i64,
        owner: Owner,
        cancel: Arc<AtomicBool>,
        hooks: crate::epg_task_hooks::EpgTaskHooks,
    ) -> Result<ScopedOutcome, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&id) {
            return Err(Error::Unsupported);
        }
        let session = hooks.download_session().await?;
        let admission = self
            .admit_on(id, owner, session, Some(hooks.clone()))
            .await?;
        tokio::spawn(async move {
            // The same task scope owns SQL/context after this supervisor. On
            // failure caller must abort it; on success finish it only after all
            // subsequent parsing and native child publications have joined.
            match admission.fetch(cancel).await? {
                Outcome::Installed(file) => Ok(ScopedOutcome::Installed(file)),
                Outcome::HandledFailure(proof) => Ok(ScopedOutcome::HandledFailure(
                    hooks.pending_rejection(proof),
                )),
            }
        })
        .await
        .map_err(|_| Error::Unsettled("scoped download supervisor"))?
    }
    /// Accepted refresh callers backpressure only known unused Auth capacity;
    /// same source/caller is retained and unknown SQL outcomes are never retried.
    pub async fn run(
        &self,
        id: i64,
        owner: Owner,
        cancel: Arc<AtomicBool>,
    ) -> Result<Outcome, Error> {
        loop {
            if cancel.load(Ordering::Acquire) {
                return Err(Error::Cancelled);
            }
            match self.admit(id, owner.clone()).await {
                Ok(admission) => return admission.execute(cancel).await,
                Err(Error::Capacity) => tokio::time::sleep(Duration::from_millis(100)).await,
                Err(error) => return Err(error),
            }
        }
    }
}
fn source_integer(value: &Value) -> Option<i64> {
    match value {
        Value::Bool(v) => Some(i64::from(*v)),
        Value::Number(v) => v.as_i64().or_else(|| {
            v.as_f64()
                .filter(|v| v.is_finite() && *v > i64::MIN as f64 && *v < i64::MAX as f64)
                .map(|v| v.trunc() as i64)
        }),
        Value::String(v) => dispatcharr_ingest_core::source_integer_literal(v)?
            .parse()
            .ok(),
        _ => None,
    }
}
async fn stream_settings(session: &TaskSqlSession, auth: &AuthService) -> Result<Value, Error> {
    let cloned = session.clone();
    let load = move || async move {
        cloned.run(|c|Box::pin(async move{
  let row=sqlx::query("SELECT CASE WHEN octet_length(value::text)<=65536 THEN value::text END AS raw FROM core_coresettings WHERE key='stream_settings'").fetch_optional(c).await?;
  let defaults=||json!({"default_user_agent":null,"default_stream_profile":null,"m3u_hash_key":"","default_output_format":"mpegts","hdhr_output_profile_id":null});
  let Some(row)=row else{return Ok(defaults())};let raw:Option<String>=row.try_get("raw")?;let raw=raw.ok_or(StoreError::Unsupported("XML stream settings capacity"))?;
  let(value,_)=crate::xc_bounded_user::checked_properties(&raw,0,crate::XcUserReadLimits{max_raw_bytes:65536,max_nodes:2048,max_depth:32,work_bytes:4*1024*1024})?;
  Ok::<_,StoreError>(if value.as_object().is_some_and(|v|!v.is_empty()){value}else{defaults()})
 })).await?
    };
    Ok(match &auth.notification_cache {
        Some(cache) => cache.settings_group("stream_settings", load).await?,
        None => load().await?,
    })
}
impl Admission {
    /// Actual missing selected input has already been observed by LocalFiles.
    /// The Source get_cache_file extensionless/existing-alternate-file edge
    /// needs that separate file proof; do not invent a suffix from URL alone.
    pub async fn for_missing_tvg(mut self) -> Result<Self, Error> {
        if self
            .source
            .file_path
            .as_ref()
            .is_some_and(|path| std::fs::metadata(path).is_ok())
        {
            self.session.close().await?;
            return Err(Error::Unsupported);
        }
        self.missing_tvg = true;
        Ok(self)
    }
    pub async fn close(self) -> Result<(), Error> {
        self.session.close().await?;
        Ok(())
    }
    pub async fn execute(self, cancel: Arc<AtomicBool>) -> Result<Outcome, Error> {
        tokio::spawn(async move {
            let result = self.fetch(cancel).await;
            let closed = self.session.close().await;
            closed.map_err(Error::Store)?;
            result
        })
        .await
        .map_err(|_| Error::Unsettled("download supervisor"))?
    }
    /// Continue accepted TVG work on this SAME real admitted writer/session.
    /// The caller must close it after staging/swap; failures close it here.
    pub async fn execute_retained(
        self,
        cancel: Arc<AtomicBool>,
    ) -> Result<(Outcome, TaskSqlSession), Error> {
        tokio::spawn(async move {
            match self.fetch(cancel).await {
                Ok(value) => Ok((value, self.session.clone())),
                Err(error) => {
                    self.session.close().await?;
                    Err(error)
                }
            }
        })
        .await
        .map_err(|_| Error::Unsettled("retained download supervisor"))?
    }
    async fn event(&self, data: Value) -> Result<(), Error> {
        self.download
            .inner
            .events
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                self.owner.clone(),
            )
            .await
            .map_err(|_| Error::Unsettled("event send"))
    }
    async fn progress(&self, action: &str, percent: u8, extra: Value) -> Result<(), Error> {
        let mut data = json!({"progress":percent,"type":"epg_refresh","source":self.source.id,"action":action});
        if let Some(extra) = extra.as_object() {
            for (k, v) in extra {
                data[k] = v.clone();
            }
        }
        // Source send_epg_update catches ordinary Celery channel send failures.
        // Real sink ownership still settles; received frames need independent proof.
        let _ = self.event(data).await;
        Ok(())
    }
    async fn status(&self, status: &str, message: Option<&str>) -> Result<(), Error> {
        let id = self.source.id;
        let status = status.to_owned();
        let message = message.map(str::to_owned);
        let updated = self
            .session
            .run(move |c| {
                Box::pin(async move {
                    let result = if let Some(message) = message {
                        sqlx::query(
                            "UPDATE epg_epgsource SET status=$2,last_message=$3 WHERE id=$1",
                        )
                        .bind(id)
                        .bind(status)
                        .bind(message)
                        .execute(c)
                        .await?
                    } else {
                        sqlx::query("UPDATE epg_epgsource SET status=$2 WHERE id=$1")
                            .bind(id)
                            .bind(status)
                            .execute(c)
                            .await?
                    };
                    Ok::<_, StoreError>(result.rows_affected())
                })
            })
            .await??;
        if updated != 1 {
            return Err(Error::Unsettled("source status row disappeared"));
        }
        Ok(())
    }
    async fn failure(
        &self,
        message: String,
        action: &str,
        public_error: String,
        kind: Rejection,
    ) -> Result<Outcome, Error> {
        self.status("error", Some(&message)).await?;
        self.progress(action, 100, json!({"status":"error","error":public_error}))
            .await?;
        if let Some(hooks) = &self.hooks {
            hooks
                .audit(
                    "epg_error",
                    json!({"source_name":self.source.name,"message":public_error}),
                    chrono::Utc::now(),
                )
                .await?;
            return Ok(Outcome::HandledFailure(Rejected {
                source: self.source.id,
                kind,
            }));
        }
        let name = self.source.name.clone();
        let audit=self.session.run(move|c|Box::pin(async move{sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES('epg_error',$1,NULL,NULL,$2)").bind(chrono::Utc::now()).bind(json!({"source_name":name,"message":public_error})).execute(c).await.map(|_|()).map_err(StoreError::from)})).await?;
        if let Err(error) = audit {
            match error {
                StoreError::Database(sqlx::Error::Database(_)) => {
                    return Ok(Outcome::HandledFailure(Rejected {
                        source: self.source.id,
                        kind,
                    }))
                }
                other => return Err(other.into()),
            }
        }
        let settings = match self
            .session
            .system_settings(self.download.inner.auth.clone())
            .await
        {
            Ok(settings) => settings,
            Err(StoreError::Database(sqlx::Error::Database(_))) => {
                return Ok(Outcome::HandledFailure(Rejected {
                    source: self.source.id,
                    kind,
                }))
            }
            Err(other) => return Err(other.into()),
        };
        let max = source_max_events(settings.get("max_system_events"));
        if max >= 0 {
            let trimmed=self.session.run(move|c|Box::pin(async move{let cutoff:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM core_systemevent ORDER BY timestamp DESC LIMIT 1 OFFSET $1").bind(max).fetch_optional(&mut*c).await?;if let Some(id)=cutoff{sqlx::query("DELETE FROM core_systemevent WHERE id<$1").bind(id).execute(c).await?;}Ok::<_,StoreError>(())})).await?;
            if let Err(error) = trimmed {
                match error {
                    StoreError::Database(sqlx::Error::Database(_)) => {}
                    other => return Err(other.into()),
                }
            }
        }
        Ok(Outcome::HandledFailure(Rejected {
            source: self.source.id,
            kind,
        }))
    }
    async fn connection_failure(&self, timeout: bool) -> Result<Outcome, Error> {
        let message = if timeout {
            format!(
                "Timeout error: EPG source '{}' took too long to respond",
                self.source.name
            )
        } else {
            format!(
                "Connection error: Unable to connect to EPG source '{}'",
                self.source.name
            )
        };
        if !timeout {
            self.event(json!({"success":false,"type":"epg_fetch_error","source_id":self.source.id,"source_name":self.source.name,"error_code":"connection_error","message":message})).await?;
        }
        self.failure(
            message.clone(),
            "downloading",
            message,
            if timeout {
                Rejection::Timeout
            } else {
                Rejection::Connection
            },
        )
        .await
    }
    async fn fetch(&self, cancel: Arc<AtomicBool>) -> Result<Outcome, Error> {
        if self.source.url.is_empty() {
            let msg = "No URL provided and no valid local file exists".to_owned();
            return self
                .failure(msg.clone(), "downloading", msg, Rejection::MissingUrl)
                .await;
        }
        if cancel.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if self.missing_tvg {
            let id = self.source.id;
            let seed = self
                .download
                .inner
                .directory
                .cache_seed(id)
                .to_str()
                .ok_or(Error::Unsupported)?
                .to_owned();
            let updated = self
                .session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query("UPDATE epg_epgsource SET file_path=$2 WHERE id=$1")
                            .bind(id)
                            .bind(seed)
                            .execute(c)
                            .await
                            .map(|v| v.rows_affected())
                            .map_err(StoreError::from)
                    })
                })
                .await??;
            if updated != 1 {
                return Err(Error::Unsettled("missing source cache seed row"));
            }
        }
        self.status("fetching", None).await?;
        self.progress("downloading", 0, json!({})).await?;
        let attempt = Attempt::new_requests(Duration::from_secs(60), Duration::from_secs(60))
            .map_err(|_| Error::Unsupported)?;
        let response = match attempt
            .request(&self.source.url, &self.source.agent, &[])
            .map_err(|_| Error::Unsupported)?
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if error.is_builder() || error.is_redirect() => {
                http_diagnostics::send_error(&error);
                return Err(Error::Unsupported);
            }
            Err(error) => {
                http_diagnostics::send_error(&error);
                return self
                    .connection_failure(error.is_timeout() && !error.is_connect())
                    .await;
            }
        };
        if attempt.cookie_budget_exceeded() {
            return Err(Error::Capacity);
        }
        let status = response.status().as_u16();
        if status >= 400 {
            let message = if status == 404 {
                format!(
                    "EPG source '{}' returned 404 error - will retry on next scheduled run",
                    self.source.name
                )
            } else {
                format!(
                    "EPG source '{}' encountered HTTP error {status}",
                    self.source.name
                )
            };
            self.event(json!({"success":false,"type":"epg_fetch_error","source_id":self.source.id,"source_name":self.source.name,"error_code":status,"message":message})).await?;
            let public = if status == 404 {
                "URL not found (404)".into()
            } else {
                message.clone()
            };
            return self
                .failure(message, "downloading", public, Rejection::Http(status))
                .await;
        }
        let total = match response.headers().get(reqwest::header::CONTENT_LENGTH) {
            None => 0,
            Some(v) => source_integer(&json!(v.to_str().map_err(|_| Error::Unsupported)?))
                .ok_or(Error::Unsupported)?
                .max(0) as u64,
        };
        let incoming = self
            .download
            .inner
            .directory
            .incoming()
            .map_err(|_| Error::Unsettled("incoming create"))?;
        let wire = Arc::new(AtomicU64::new(0));
        let limits = dispatcharr_ingest_core::UploadLimits::default();
        let mut body = dispatcharr_upstream_http::body_measured(
            response,
            limits.source_bytes as usize,
            wire.clone(),
        )
        .map_err(|_| Error::Unsupported)?;
        let (file_tx, file_done) = files::writer(
            incoming
                .writer()
                .map_err(|_| Error::Unsettled("incoming writer"))?,
            self.owner.clone(),
        );
        let began = Instant::now();
        let mut last = began;
        let mut decoded = 0u64;
        let result=async {
   while let Some(chunk)=body.next().await {
    if cancel.load(Ordering::Acquire){return Err(Error::Cancelled)}
    let chunk=chunk.map_err(http_error)?;
    decoded=decoded.checked_add(chunk.len()as u64).filter(|v|*v<=limits.decoded_bytes).ok_or(Error::Capacity)?;
    for chunk in chunk.chunks(16384){file_tx.send(chunk.to_vec()).await.map_err(|_|Error::Unsettled("file writer"))?;}
    let elapsed=began.elapsed().as_secs_f64();let received=wire.load(Ordering::Acquire);
    let progress=if total>0 {((received as f64/total as f64)*100.0).min(100.0)as u8}else{((decoded as f64/(10.0*1024.0*1024.0))*100.0).min(95.0)as u8};
    if last.elapsed()>=Duration::from_millis(500)&&progress>0 {
     let speed=if elapsed>0.0{decoded as f64/elapsed/1024.0}else{0.0};let remaining=if total>0&&received>0{(total as f64-received as f64)/(received as f64/elapsed)}else{0.0};
     self.progress("downloading",progress,json!({"speed":(speed*100.0).round_ties_even()/100.0,"elapsed_time":(elapsed*10.0).round_ties_even()/10.0,"time_remaining":(remaining*10.0).round_ties_even()/10.0,"downloaded":format!("{:.2} MB",decoded as f64/(1024.0*1024.0))})).await?;last=Instant::now();
    }
   }
   Ok::<_,Error>(())
  }.await;
        drop(file_tx);
        file_done
            .await
            .map_err(|_| Error::Unsettled("writer supervisor"))?
            .map_err(|_| Error::Unsettled("writer close"))?;
        match result {
            Err(Error::BodyReadTimeout) => return self.connection_failure(false).await,
            other => other?,
        }
        if total > 0 && wire.load(Ordering::Acquire) != total {
            let received = wire.load(Ordering::Acquire);
            let msg=format!("EPG download incomplete ({received} of {total} bytes). Keeping previous data when available; will retry on next refresh.");
            return self
                .failure(msg.clone(), "downloading", msg, Rejection::Incomplete)
                .await;
        }
        let url = self.source.url.clone();
        let retained = self.owner.clone();
        let (incoming, compressed) = tokio::task::spawn_blocking(move || {
            let _retained = retained;
            incoming
                .compressed(&url)
                .map(|compressed| (incoming, compressed))
        })
        .await
        .map_err(|_| Error::Unsettled("format supervisor"))?
        .map_err(|_| Error::Unsettled("format read"))?;
        if compressed {
            self.progress(
                "extracting",
                0,
                json!({"message":"Extracting downloaded file"}),
            )
            .await?;
        }
        let id = self.source.id;
        let url = self.source.url.clone();
        let retained = self.owner.clone();
        let cancelled = cancel.clone();
        let installed = tokio::task::spawn_blocking(move || {
            let _retained = retained;
            incoming.finish(id, &url, &cancelled)
        })
        .await
        .map_err(|_| Error::Unsettled("extract supervisor"))?
        .map_err(|error| match error {
            files::FinishError::Capacity => Error::Capacity,
            files::FinishError::Io(error) => {
                let _kind = error.kind();
                Error::Unsettled("extract/promotion")
            }
        })?;
        let (path, compressed) = match installed {
            files::Promotion::Installed(path, compressed) => (path, compressed),
            files::Promotion::Incomplete => {
                let message="EPG file appears incomplete (missing closing </tv>). Keeping previous data when available; will retry on next refresh.".to_owned();
                return self
                    .failure(
                        message.clone(),
                        "downloading",
                        message,
                        Rejection::Incomplete,
                    )
                    .await;
            }
            files::Promotion::ExtractionFailed => {
                return self
                    .failure(
                        "Failed to extract downloaded EPG file".into(),
                        "extracting",
                        "Failed to extract downloaded EPG file".into(),
                        Rejection::Extraction,
                    )
                    .await
            }
        };
        let stored = path.to_str().ok_or(Error::Unsupported)?.to_owned();
        let updated=self.session.run(move|c|Box::pin(async move{sqlx::query("UPDATE epg_epgsource SET file_path=$2,status='parsing',extracted_file_path=NULL WHERE id=$1").bind(id).bind(stored).execute(c).await.map(|v|v.rows_affected()).map_err(StoreError::from)})).await??;
        if updated != 1 {
            return Err(Error::Unsettled("installed source row disappeared"));
        }
        if compressed {
            self.progress(
                "extracting",
                100,
                json!({"message":"File extracted successfully"}),
            )
            .await?;
        }
        self.progress("downloading", 100, json!({})).await?;
        Ok(Outcome::Installed(Installed { source: id, path }))
    }
}

fn http_error(error: std::io::Error) -> Error {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(item) = cause {
        if item.is::<dispatcharr_upstream_http::WireLimitExceeded>()
            || item.is::<dispatcharr_upstream_http::ResourceLimitExceeded>()
        {
            return Error::Capacity;
        }
        if item
            .downcast_ref::<reqwest::Error>()
            .is_some_and(|error| error.is_timeout())
        {
            return Error::BodyReadTimeout;
        }
        cause = item.source();
    }
    Error::Unsupported
}

fn source_max_events(value: Option<&Value>) -> i64 {
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
