//! Source channel task producers with actual shared Auth/Jobs. Authors retain
//! full Dispatcharr/Django/Celery credit. Router construction starts no intake.
use crate::{
    channel_maintenance::{Kind, Request as TaskRequest},
    AuthService, StoreError,
};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::Method,
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
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
pub trait Publisher: Send + Sync {
    fn publish(
        &self,
        request: TaskRequest,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Uuid, StoreError>>;
}
struct Inner {
    auth: AuthService,
    jobs: Arc<dyn Publisher>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct ChannelMaintenanceHttp {
    inner: Arc<Inner>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    inner: Arc<Inner>,
}
impl ChannelMaintenanceHttp {
    pub fn prepare(auth: AuthService, jobs: Arc<dyn Publisher>) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth,
                jobs,
                slots: Arc::new(Semaphore::new(4)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/channels/channels/from-stream/bulk/", any(handle))
            .route("/api/channels/channels/set-names-from-epg/", any(handle))
            .route("/api/channels/channels/set-logos-from-epg/", any(handle))
            .route("/api/channels/channels/set-tvg-ids-from-epg/", any(handle))
            .with_state(self)
    }
    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(timeout, self.inner.slots.clone().acquire_many_owned(4))
            .await
            .is_ok()
    }
}
fn bad(message: &str) -> StoreError {
    StoreError::StatusBody(400, json!({"error":message}))
}
fn ids(raw: &Value) -> Result<Vec<Value>, StoreError> {
    raw.as_array().cloned().ok_or(StoreError::Unsupported(
        "Channel list capacity; child coercion belongs to task execution",
    ))
}
async fn handle(
    State(api): State<ChannelMaintenanceHttp>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return StoreError::StatusBody(503, json!({"error":"Channel task admission closed"}))
            .into_response();
    }
    let Ok(slot) = api.inner.slots.clone().try_acquire_owned() else {
        return StoreError::StatusBody(503, json!({"error":"Channel task capacity"}))
            .into_response();
    };
    let owner = Arc::new(Owner {
        _slot: slot,
        inner: api.inner.clone(),
    });
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = match to_bytes(request.into_body(), 65536).await {
        Ok(v) => v,
        Err(_) => {
            return StoreError::StatusBody(413, json!({"detail":"Request body too large."}))
                .into_response()
        }
    };
    match tokio::spawn(async move{
    let auth=owner.inner.auth.clone().with_retained_scope(owner.clone());let user=auth.account_user(peer,headers,false).await?;if user.user_level<10{return Err(StoreError::PermissionDenied);}
    if method!=Method::POST{return Err(StoreError::StatusBody(405,json!({"detail":format!("Method \"{method}\" not allowed.")})));}
    let value:Value=serde_json::from_slice(&body).map_err(|_|StoreError::StatusBody(400,json!({"detail":"JSON parse error"})))?;
    let object=value.as_object().ok_or(StoreError::UnhandledRestFault)?;
    let kind=if path.ends_with("from-stream/bulk/"){Kind::Bulk}else if path.ends_with("set-names-from-epg/"){Kind::Names}else if path.ends_with("set-logos-from-epg/"){Kind::Logos}else{Kind::TvgIds};
    let mut task=TaskRequest{kind,ids:Vec::new(),source:None,profiles:None,starting:None};let mut count;let mut count_source=None;
    if kind==Kind::Logos{
        let channel=object.get("channel_ids");let source=object.get("epg_source_id");let c=crate::channels::truthy(channel);let s=crate::channels::truthy(source);
        if c&&s{return Err(bad("Provide either channel_ids or epg_source_id, not both"));}
        if !c&&!s{return Err(bad("channel_ids or epg_source_id is required"));}
        if let Some(channel)=channel.filter(|v|!v.is_null()){if !channel.is_array(){return Err(bad("channel_ids must be a list"));}
        if channel.as_array().is_some_and(Vec::is_empty){return Err(bad("channel_ids cannot be empty"));}task.ids=ids(channel)?;count=task.ids.len();}
        else {task.source=Some(source.cloned().ok_or(StoreError::UnhandledRestFault)?);count=0;count_source=task.source.clone();}
    }else{
        let key=if kind==Kind::Bulk{"stream_ids"}else{"channel_ids"};let selected=object.get(key);if !crate::channels::truthy(selected){return Err(bad(if kind==Kind::Bulk{"stream_ids is required and cannot be empty"}else{"channel_ids is required"}));}
        let selected=selected.unwrap();if !selected.is_array(){return Err(bad(if kind==Kind::Bulk{"stream_ids must be a list of integers"}else{"channel_ids must be a list"}));}task.ids=ids(selected)?;count=task.ids.len();
        if kind==Kind::Bulk{task.profiles=match object.get("channel_profile_ids"){None|Some(Value::Null)=>None,Some(v)if v.is_array()=>Some(ids(v)?),Some(v)=>Some(ids(&json!([v]))?)};task.starting=match object.get("starting_channel_number"){None|Some(Value::Null)=>None,Some(v)=>Some(v.clone())};}
    }
    let id=owner.inner.jobs.publish(task,owner.clone()).await?;
    if let Some(source)=count_source{count=crate::channel_maintenance::source_logo_count(&auth.store,&source).await?;}
    let(status,reply)=if kind==Kind::Bulk{(202,json!({"task_id":id,"message":format!("Bulk channel creation task started for {count} streams"),"stream_count":count,"status":"started"}))}else{let label=if kind==Kind::Names{"name"}else if kind==Kind::Logos{"logo"}else{"TVG-ID"};(200,json!({"message":format!("Started EPG {label} setting task for {count} channels"),"task_id":id,"channel_count":count}))};
    Ok(StoreError::StatusBody(status,reply).into_response())
 }).await{Ok(Ok(v))=>v,Ok(Err(e))=>e.into_response(),Err(_)=>StoreError::UnhandledRestFault.into_response()}
}
