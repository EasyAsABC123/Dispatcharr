//! Narrow Source SD HTTP producers on real shared Auth/SQL/SD consumer ports.
//! Original Dispatcharr/Django/Celery authors retain all API/algorithm credit.
use crate::{
    sd_guide_auth::{self as auth, Read, Source},
    sd_guide_http as http, AuthService, StoreError,
};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use chrono::{DateTime, SecondsFormat, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
type Scope = Arc<dyn Send + Sync>;
#[derive(Clone, Copy)]
pub enum Trigger {
    Created,
    LineupAdded,
}
pub trait Publisher: Send + Sync {
    fn auth_service(&self) -> AuthService;
    fn admission(&self) -> Result<(), StoreError>;
    fn publish(
        &self,
        source: i64,
        trigger: Trigger,
        owner: Scope,
    ) -> BoxFuture<'_, Result<Uuid, StoreError>>;
}
struct Inner {
    auth: AuthService,
    jobs: Arc<dyn Publisher>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct SdSourceHttp {
    inner: Arc<Inner>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    inner: Arc<Inner>,
}
impl SdSourceHttp {
    pub fn prepare(jobs: Arc<dyn Publisher>) -> Self {
        let auth = jobs.auth_service();
        Self {
            inner: Arc::new(Inner {
                auth,
                jobs,
                slots: Arc::new(Semaphore::new(4)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release)
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(timeout, self.inner.slots.clone().acquire_many_owned(4))
            .await
            .is_ok_and(|v| v.is_ok())
    }
    async fn handle(&self, peer: SocketAddr, request: Request, lineup: bool) -> Response {
        if self.inner.closed.load(Ordering::Acquire) {
            return unavailable("SD HTTP admission closed").into_response();
        }
        let Ok(slot) = self.inner.slots.clone().try_acquire_owned() else {
            return unavailable("SD HTTP capacity").into_response();
        };
        let owner = Arc::new(Owner {
            _slot: slot,
            inner: self.inner.clone(),
        });
        let result=tokio::spawn(async move{
   if lineup && request.method()!=Method::POST { return extended_lineup(peer,request,owner).await; }
   let scoped=owner.inner.auth.clone().with_retained_scope(owner.clone());
   let user=scoped.account_user(peer,request.headers().clone(),false).await?;if user.user_level<10{return Err(StoreError::PermissionDenied)}
   if request.method()!=Method::POST{return Err(StoreError::Unsupported("SD GET/DELETE/OPTIONS have not been ported"))}
   owner.inner.jobs.admission()?;
   // Source plugins can register arbitrary model signal handlers. Until that
   // model-write capability is joined, use the existing physical namespace /
   // persisted-config guard, not a caller-provided empty-plugin assertion.
   crate::m3u_refresh_all::require_supported_domain(&scoped,owner.clone()).await?;
   let path=request.uri().path().to_owned();
   let media=request.headers().get("content-type").and_then(|v|v.to_str().ok()).map(|v|v.split(';').next().unwrap_or(v).trim().to_ascii_lowercase());
   if media.as_deref().is_some_and(|v|v!="application/json"){return Err(StoreError::StatusBody(415,json!({"detail":format!("Unsupported media type \"{}\" in request.",media.unwrap_or_default())})))}
   let bytes=to_bytes(request.into_body(),65536).await.map_err(|_|StoreError::StatusBody(413,json!({"detail":"Request body too large."})))?;
   let value=if bytes.is_empty(){json!({})}else{serde_json::from_slice(&bytes).map_err(|e|StoreError::StatusBody(400,json!({"detail":format!("JSON parse error - {}",crate::source_json_error(&bytes,e))})))?};
   if lineup{
    let source=path.trim_matches('/').split('/').nth(3).and_then(|s|s.parse::<i64>().ok()).filter(|n|(1..=i64::from(i32::MAX)).contains(n)).ok_or_else(missing)?;
    let session=scoped.borrow_task_sql_session(owner.clone()).await?;
    let operation=async{
     let mut source=match Source::read(source,session.clone(),scoped.clone(),owner.clone()).await.map_err(auth_error)?{Read::Missing=>return Err(missing()),Read::Other=>return Ok((400,json!({"error":"This action is only available for Schedules Direct sources."}),false)),Read::Source(s)=>*s};
     let result=add_lineup(&mut source,&value).await?;
     Ok::<_,StoreError>(result)
    }.await;
    let(status,reply,publish)=crate::sd_source_writes::close_result(&session,operation).await?;
    if publish{owner.inner.jobs.publish(source,Trigger::LineupAdded,owner.clone()).await?;}
    Ok(StoreError::StatusBody(status,reply).into_response())
   }else{
    let input=crate::sd_source_writes::validate(&value)?;
    let(id,active,reply)=crate::sd_source_writes::create(scoped,input,owner.clone()).await?;
    if active{owner.inner.jobs.publish(id,Trigger::Created,owner.clone()).await?;}
    Ok((StatusCode::CREATED,axum::Json(reply)).into_response())
   }
  }).await;
        let mut response = match result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => error.into_response(),
            Err(_) => StoreError::UnhandledRestFault.into_response(),
        };
        if lineup && response.status() != StatusCode::INTERNAL_SERVER_ERROR {
            // Captured DRF content negotiation varies JSON and browsable HTML.
            response
                .headers_mut()
                .insert("vary", axum::http::HeaderValue::from_static("Accept"));
            response.headers_mut().insert(
                "allow",
                axum::http::HeaderValue::from_static("GET, POST, DELETE, HEAD, OPTIONS"),
            );
        }
        response
    }
}
/// Source20 plus separately recovered21 and new Source4 scoped action captures.
async fn extended_lineup(
    peer: SocketAddr,
    request: Request,
    owner: Arc<Owner>,
) -> Result<Response, StoreError> {
    let scoped = owner.inner.auth.clone().with_retained_scope(owner.clone());
    let user = scoped
        .account_user(peer, request.headers().clone(), false)
        .await?;
    let method = request.method().clone();
    let minimum = if method == Method::GET { 1 } else { 10 };
    if user.user_level < minimum {
        return Err(StoreError::PermissionDenied);
    }
    if method == Method::OPTIONS {
        // DRF metadata does not resolve the object; a missing PK yields the
        // same metadata200 as an existing source, as actually captured.
        let value: Value =
            serde_json::from_str(LINEUP_OPTIONS).map_err(|_| StoreError::UnhandledRestFault)?;
        return Ok(axum::Json(value).into_response());
    }
    if method == Method::PUT {
        // New Source4 administrator PUT capture: DRF rejects the
        // method after authentication/permissions, before object/provider work.
        return Err(StoreError::StatusBody(
            405,
            json!({"detail":"Method \"PUT\" not allowed."}),
        ));
    }
    if method != Method::GET && method != Method::DELETE && method != Method::HEAD {
        return Err(StoreError::Unsupported(
            "SD wider method contracts remain unqualified",
        ));
    }
    let writer = scoped.task_sql_writer()?;
    let scoped = scoped.with_request_writer_permit(writer)?;
    let physical = scoped.core_publication_owner(owner);
    crate::m3u_refresh_all::require_supported_domain(&scoped, physical.clone()).await?;
    let id = request
        .uri()
        .path()
        .trim_matches('/')
        .split('/')
        .nth(3)
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| (1..=i64::from(i32::MAX)).contains(v))
        .ok_or_else(missing)?;
    let session = scoped.borrow_task_sql_session(physical.clone()).await?;
    let operation=async {
        let mut source=match Source::read(id,session.clone(),scoped,physical).await.map_err(auth_error)? {
            Read::Missing=>return Err(missing()),
            Read::Other=>return Ok(StoreError::StatusBody(400,json!({"error":"This action is only available for Schedules Direct sources."})).into_response()),
            Read::Source(source)=>*source,
        };
        let custom:bool=source.session.run(|c|Box::pin(async move {sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled <> 'D' AND tgrelid='epg_epgsource'::regclass)").fetch_one(c).await})).await??;
        if custom {return Err(StoreError::Unsupported("SD action custom SQL triggers require separate Source authority"))}
        // Source authenticates BEFORE touching DELETE request.data/lineup.
        let token=match lineup_token(&mut source).await? {Ok(token)=>token,Err(response)=>return Ok(response)};
        if method==Method::HEAD {
            // Source resolves the object and authenticates its SD token before
            // falling through the action's GET/POST/DELETE branches. DRF's
            // missing Response becomes Django's pinned stock500; HEAD emits
            // no body, retaining the existing error representation's length.
            // This does not run GET or fetch countries/provider lineups.
            let mut response=StoreError::UnhandledRestFault.into_response();
            *response.body_mut()=axum::body::Body::empty();
            return Ok(response);
        }
        if method==Method::GET {return get_lineups(&mut source,token).await}
        let media=request.headers().get("content-type").and_then(|v|v.to_str().ok()).map(|v|v.split(';').next().unwrap_or(v).trim().to_ascii_lowercase());
        if media.as_deref().is_some_and(|v|v!="application/json"){return Err(StoreError::StatusBody(415,json!({"detail":format!("Unsupported media type \"{}\" in request.",media.unwrap_or_default())})))}
        let bytes=to_bytes(request.into_body(),65536).await.map_err(|_|StoreError::StatusBody(413,json!({"detail":"Request body too large."})))?;
        let value=if bytes.is_empty(){json!({})}else{serde_json::from_slice(&bytes).map_err(|e|StoreError::StatusBody(400,json!({"detail":format!("JSON parse error - {}",crate::source_json_error(&bytes,e))})))?};
        delete_lineup(&mut source,token,&value).await
    }.await;
    let closed = session.close().await;
    let response = operation?;
    closed?;
    Ok(response)
}
async fn lineup_token(source: &mut Source) -> Result<Result<String, Response>, StoreError> {
    let result = source
        .obtain_with_timeout(Duration::from_secs(15))
        .await
        .map_err(auth_error)?;
    if let Some(token) = result.token {
        return Ok(Ok(token));
    }
    let status = if result.debug_rejected || result.message == "Username and password are required."
    {
        400
    } else if matches!(result.code, Some(4002 | 4003)) {
        401
    } else if result.lockout || result.soft || result.code.is_some() {
        503
    } else {
        502
    };
    Ok(Err(StoreError::StatusBody(
        status,
        json!({"error":result.message}),
    )
    .into_response()))
}
async fn get_lineups(source: &mut Source, mut token: String) -> Result<Response, StoreError> {
    // Countries has neither token nor Content-Type, exactly Source's public
    // Requests GET. Its HTTP/network failure does not prevent lineup retrieval.
    let countries = match http::countries(source.owner.clone()).await {
        Ok(reply) if reply.request_error().is_none() => reply.value.unwrap_or(Value::Null),
        Ok(_) | Err(http::Error::Transport) => Value::Null,
        Err(error) => return Err(auth_error(auth::Error::Http(error))),
    };
    let reply = source
        .authorized_api(
            reqwest::Method::GET,
            "/lineups",
            &mut token,
            None,
            Duration::from_secs(15),
        )
        .await
        .map_err(|error| match error {
            auth::Error::Http(http::Error::Transport) => StoreError::StatusBody(
                502,
                json!({"error":"Failed to fetch lineups: Connection error"}),
            ),
            other => auth_error(other),
        })?;
    if reply.status == 400 && reply.payload_code == Some(4102) {
        let count = remaining(source).await?;
        return Ok(axum::Json(json!({"lineups":[],"max_lineups":4,"changes_remaining":count,"changes_reset_at":source.properties.get("sd_changes_reset_at").cloned().unwrap_or(Value::Null),"notice":"No lineups are currently configured on this Schedules Direct account. Use the search below to add one.","countries":countries})).into_response());
    }
    if let Some(error) = reply.request_error() {
        return Ok(StoreError::StatusBody(
            502,
            json!({"error":format!("Failed to fetch lineups: {error}")}),
        )
        .into_response());
    }
    let data = reply
        .value
        .as_ref()
        .and_then(Value::as_object)
        .ok_or(StoreError::Unsupported(
            "SD lineup JSON object response requires Source authority",
        ))?;
    let rows = data
        .get("lineups")
        .map_or(Some(&[][..]), |value| value.as_array().map(Vec::as_slice))
        .ok_or(StoreError::Unsupported(
            "SD lineup collection shape requires Source authority",
        ))?;
    let mut lineups = Vec::new();
    for row in rows {
        let value = row.as_object().ok_or(StoreError::Unsupported(
            "SD lineup row shape requires Source authority",
        ))?;
        if !value
            .get("isDeleted")
            .is_some_and(crate::xc_live_provider::truthy)
        {
            lineups.push(row.clone())
        }
    }
    let count = remaining(source).await?;
    Ok(axum::Json(json!({"lineups":lineups,"max_lineups":4,"changes_remaining":count,"changes_reset_at":source.properties.get("sd_changes_reset_at").cloned().unwrap_or(Value::Null),"countries":countries})).into_response())
}
async fn delete_lineup(
    source: &mut Source,
    mut token: String,
    value: &Value,
) -> Result<Response, StoreError> {
    let object = value.as_object().ok_or(StoreError::Unsupported(
        "SD DELETE nonobject body unqualified",
    ))?;
    let id = object.get("lineup");
    if id.is_none_or(|v| !crate::xc_live_provider::truthy(v)) {
        return Ok(
            StoreError::StatusBody(400, json!({"error":"lineup field is required."}))
                .into_response(),
        );
    }
    let id = id.and_then(Value::as_str).ok_or(StoreError::Unsupported(
        "SD DELETE nontext lineup unqualified",
    ))?;
    if id.len() > 512
        || id.is_empty()
        || !id
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || matches!(v, b'-' | b'_'))
    {
        return Err(StoreError::Unsupported(
            "SD DELETE lineup URL grammar outside qualified domain",
        ));
    }
    if zero(&remaining(source).await?) {
        return Ok(axum::Json(json!({"error":"daily_limit_reached","message":DAILY,"changes_remaining":0,"changes_reset_at":source.properties.get("sd_changes_reset_at").cloned().unwrap_or(Value::Null),"docs_url":DOCS})).into_response());
    }
    let reply = source
        .authorized_api(
            reqwest::Method::DELETE,
            &format!("/lineups/{id}"),
            &mut token,
            None,
            Duration::from_secs(15),
        )
        .await
        .map_err(|error| match error {
            auth::Error::Http(http::Error::Transport) => StoreError::StatusBody(
                502,
                json!({"error":"Failed to remove lineup: Connection error"}),
            ),
            other => auth_error(other),
        })?;
    // Source4 actual service-error DELETE was provider503. Requests
    // raises before decoding/persisting its payload; preserve the actual URL.
    // Other service statuses still need independent Source characterization.
    if reply.status == 503 {
        let error = reply
            .request_error()
            .ok_or(StoreError::UnhandledRestFault)?;
        return Ok(StoreError::StatusBody(
            502,
            json!({"error":format!("Failed to remove lineup: {error}")}),
        )
        .into_response());
    }
    if reply.status >= 500 {
        return Err(StoreError::Unsupported(
            "SD DELETE other service-error responses remain unqualified",
        ));
    }
    let data = reply
        .value
        .as_ref()
        .and_then(Value::as_object)
        .ok_or(StoreError::Unsupported(
            "SD DELETE JSON object response unqualified",
        ))?;
    if reply.status == 400 {
        match reply.payload_code{
            Some(2103)=>return Ok(axum::Json(json!({"response":"OK","code":0,"message":"Lineup not found on account — already removed.","changes_remaining":remaining(source).await?})).into_response()),
            Some(4100)=>{save_remaining(source,json!(0)).await?;return Ok(axum::Json(json!({"error":"daily_limit_reached","message":DAILY,"changes_remaining":0,"docs_url":DOCS})).into_response())},
            _=>return Err(StoreError::Unsupported("SD DELETE unmeasured provider400 code")),
        }
    }
    if let Some(error) = reply.request_error() {
        return Err(StoreError::Unsupported(if error.is_empty() {
            "SD DELETE unmeasured response"
        } else {
            "SD DELETE unmeasured HTTP fault"
        }));
    }
    let count = data.get("changesRemaining").cloned().unwrap_or(Value::Null);
    if !count.is_null() {
        save_remaining(source, count).await?
    }
    let mut result = data.clone();
    result.insert("changes_remaining".into(), remaining(source).await?);
    Ok(axum::Json(Value::Object(result)).into_response())
}

// Exact captured Source administrator metadata; no runtime field values.
const LINEUP_OPTIONS: &str = r#"{"name":"Sd lineups","description":"GET    — list lineups currently on the SD account\nPOST   — add a lineup (body: {\"lineup\": \"USA-NJ29486-X\"})\nDELETE — remove a lineup (body: {\"lineup\": \"USA-NJ29486-X\"})","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"],"actions":{"POST":{"id":{"type":"integer","required":false,"read_only":true,"label":"ID"},"name":{"type":"string","required":true,"read_only":false,"label":"Name","max_length":255},"source_type":{"type":"choice","required":true,"read_only":false,"label":"Source type","choices":[{"value":"xmltv","display_name":"XMLTV URL"},{"value":"schedules_direct","display_name":"Schedules Direct API"},{"value":"dummy","display_name":"Custom Dummy EPG"}]},"url":{"type":"string","required":false,"read_only":false,"label":"Url"},"username":{"type":"string","required":false,"read_only":false,"label":"Username","help_text":"Username for credential-based EPG sources (e.g. Schedules Direct)","max_length":255},"password":{"type":"string","required":false,"read_only":false,"label":"Password","help_text":"Password for credential-based EPG sources (e.g. Schedules Direct)","max_length":255},"is_active":{"type":"boolean","required":false,"read_only":false,"label":"Is active"},"file_path":{"type":"string","required":false,"read_only":false,"label":"File path","max_length":1024},"refresh_interval":{"type":"integer","required":false,"read_only":false,"label":"Refresh interval","min_value":-2147483648,"max_value":2147483647},"cron_expression":{"type":"string","required":false,"read_only":false,"label":"Cron expression"},"priority":{"type":"integer","required":false,"read_only":false,"label":"Priority","help_text":"Priority for EPG matching (higher numbers = higher priority). Used when multiple EPG sources have matching entries for a channel.","min_value":0,"max_value":2147483647},"status":{"type":"choice","required":false,"read_only":false,"label":"Status","choices":[{"value":"idle","display_name":"Idle"},{"value":"fetching","display_name":"Fetching"},{"value":"parsing","display_name":"Parsing"},{"value":"error","display_name":"Error"},{"value":"success","display_name":"Success"},{"value":"disabled","display_name":"Disabled"}]},"last_message":{"type":"string","required":false,"read_only":false,"label":"Last message","help_text":"Last status message, including success results or error information"},"created_at":{"type":"datetime","required":false,"read_only":true,"label":"Created at","help_text":"Time when this source was created"},"updated_at":{"type":"datetime","required":false,"read_only":false,"label":"Updated at","help_text":"Time when this source was last successfully refreshed"},"custom_properties":{"type":"field","required":false,"read_only":false,"label":"Custom properties","help_text":"Custom properties for source-specific configuration"},"epg_data_count":{"type":"field","required":false,"read_only":true,"label":"Epg data count"},"has_channels":{"type":"boolean","required":false,"read_only":true,"label":"Has channels"}}}}"#;

fn unavailable(message: &str) -> StoreError {
    StoreError::StatusBody(503, json!({"error":message}))
}
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No EPGSource matches the given query."}),
    )
}
fn auth_error(error: auth::Error) -> StoreError {
    match error {
        auth::Error::Store(e) => e,
        auth::Error::Capacity => unavailable("SD HTTP provider capacity"),
        auth::Error::Unsupported => StoreError::Unsupported("SD HTTP provider response domain"),
        auth::Error::Http(_) => StoreError::StatusBody(
            502,
            json!({"error":"Network error authenticating with Schedules Direct: Connection error"}),
        ),
    }
}
fn zero(v: &Value) -> bool {
    matches!(v, Value::Bool(false)) || v.as_f64() == Some(0.0)
}
async fn remaining(source: &mut Source) -> Result<Value, StoreError> {
    let value = source
        .properties
        .get("sd_changes_remaining")
        .cloned()
        .unwrap_or(Value::Null);
    if zero(&value) {
        if let Some(Value::String(reset)) = source.properties.get("sd_changes_reset_at") {
            if let Ok(reset) = DateTime::parse_from_rfc3339(reset) {
                if Utc::now() >= reset.with_timezone(&Utc) {
                    source.properties.remove("sd_changes_remaining");
                    source.properties.remove("sd_changes_reset_at");
                    source.save_properties().await.map_err(auth_error)?;
                    return Ok(Value::Null);
                }
            }
        }
    }
    Ok(value)
}
async fn save_remaining(source: &mut Source, value: Value) -> Result<(), StoreError> {
    let empty = zero(&value);
    source
        .properties
        .insert("sd_changes_remaining".into(), value);
    if empty {
        let next = Utc::now()
            .date_naive()
            .succ_opt()
            .ok_or(StoreError::UnhandledRestFault)?
            .and_hms_opt(0, 0, 0)
            .ok_or(StoreError::UnhandledRestFault)?
            .and_utc();
        source.properties.insert(
            "sd_changes_reset_at".into(),
            json!(next.to_rfc3339_opts(SecondsFormat::Secs, false)),
        );
    } else {
        source.properties.remove("sd_changes_reset_at");
    }
    source.save_properties().await.map_err(auth_error)
}
const DOCS:&str="https://github.com/SchedulesDirect/JSON-Service/wiki/API-20141201#tasks-your-client-must-perform";
const DAILY:&str="You have reached your daily Schedules Direct lineup change limit (6 add/delete operations per 24 hours). Resets at midnight UTC.";
async fn add_lineup(source: &mut Source, value: &Value) -> Result<(u16, Value, bool), StoreError> {
    // Source authenticates before inspecting the POST's lineup field.
    let auth = source
        .obtain_with_timeout(Duration::from_secs(15))
        .await
        .map_err(auth_error)?;
    let Some(mut token) = auth.token else {
        let status = if auth.debug_rejected || auth.message == "Username and password are required."
        {
            400
        } else if matches!(auth.code, Some(4002 | 4003)) {
            401
        } else if auth.lockout || auth.soft || auth.code.is_some() {
            503
        } else {
            502
        };
        return Ok((status, json!({"error":auth.message}), false));
    };
    let object = value.as_object().ok_or(StoreError::UnhandledRestFault)?;
    let id = object.get("lineup");
    if id.is_none_or(|v| !crate::xc_live_provider::truthy(v)) {
        return Ok((400, json!({"error":"lineup field is required."}), false));
    }
    let id = id.and_then(Value::as_str).ok_or(StoreError::Unsupported(
        "Nontext lineup identifiers need Source characterization",
    ))?;
    let count = remaining(source).await?;
    if zero(&count) {
        return Ok((
            200,
            json!({"error":"daily_limit_reached","message":DAILY,"changes_remaining":0,"changes_reset_at":source.properties.get("sd_changes_reset_at").cloned().unwrap_or(Value::Null),"docs_url":DOCS}),
            false,
        ));
    }
    let reply = source
        .authorized_api(
            reqwest::Method::PUT,
            &format!("/lineups/{id}"),
            &mut token,
            None,
            Duration::from_secs(15),
        )
        .await
        .map_err(|e| match e {
            auth::Error::Http(http::Error::Transport) => StoreError::StatusBody(
                502,
                json!({"error":"Failed to add lineup: Connection error"}),
            ),
            other => auth_error(other),
        })?;
    let data = reply
        .value
        .as_ref()
        .and_then(Value::as_object)
        .ok_or(StoreError::Unsupported(
            "SD lineup response is not an object",
        ))?;
    let code = data.get("code").and_then(Value::as_i64);
    if matches!(reply.status, 400 | 403) {
        let response = match code {
            Some(4100) => {
                save_remaining(source, json!(0)).await?;
                json!({"error":"daily_limit_reached","message":DAILY,"changes_remaining":0,"docs_url":DOCS})
            }
            Some(4101) => {
                json!({"error":"max_lineups_reached","message":"Your Schedules Direct account has reached the maximum of 4 lineups. Remove one before adding another.","changes_remaining":remaining(source).await?})
            }
            Some(2100) => {
                json!({"error":"duplicate_lineup","message":"This lineup is already on your Schedules Direct account.","changes_remaining":remaining(source).await?})
            }
            _ => {
                json!({"error":data.get("message").cloned().unwrap_or_else(||json!("Failed to add lineup.")),"changes_remaining":remaining(source).await?})
            }
        };
        return Ok((200, response, false));
    }
    if let Some(error) = reply.request_error() {
        return Ok((
            502,
            json!({"error":format!("Failed to add lineup: {error}")}),
            false,
        ));
    }
    let count = data.get("changesRemaining").cloned().unwrap_or(Value::Null);
    if !count.is_null() {
        save_remaining(source, count.clone()).await?
    }
    let mut response = data.clone();
    response.insert("changes_remaining".into(), count);
    Ok((200, Value::Object(response), true))
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    api: Option<Extension<SdSourceHttp>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    match api {
        Some(Extension(api)) => api.handle(peer, request, false).await,
        None => {
            match auth
                .account_user(peer, request.headers().clone(), true)
                .await
            {
                Ok(_) => StoreError::Unsupported("This management mutation has not been ported.")
                    .into_response(),
                Err(e) => e.into_response(),
            }
        }
    }
}
pub(crate) async fn lineup(
    State(auth): State<AuthService>,
    api: Option<Extension<SdSourceHttp>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    match api {
        Some(Extension(api)) => api.handle(peer, request, true).await,
        None => {
            match auth
                .account_user(peer, request.headers().clone(), true)
                .await
            {
                Ok(_) => StoreError::Unsupported(
                    "Schedules Direct lineup writes require the prepared station consumer",
                )
                .into_response(),
                Err(e) => e.into_response(),
            }
        }
    }
}
