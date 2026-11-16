//! Dispatcharr matching actions; Original Dispatcharr/Django/Celery authors retain credit.
use crate::{channel_epg_match_task::Request as TaskRequest, AuthService, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
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
pub struct ChannelEpgMatchHttp {
    inner: Arc<Inner>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    inner: Arc<Inner>,
}
impl ChannelEpgMatchHttp {
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
            .route("/api/channels/channels/match-epg/", any(handle))
            .route("/api/channels/channels/{id}/match-epg/", any(handle))
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
async fn handle(
    State(api): State<ChannelEpgMatchHttp>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let mut response = handle_inner(api, peer, request).await;
    if response.status() != StatusCode::INTERNAL_SERVER_ERROR {
        response
            .headers_mut()
            .insert("allow", HeaderValue::from_static("POST, OPTIONS"));
    }
    response
}
async fn handle_inner(api: ChannelEpgMatchHttp, peer: SocketAddr, request: Request) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return StoreError::StatusBody(503, json!({"error":"Matching admission closed"}))
            .into_response();
    }
    let Ok(slot) = api.inner.slots.clone().try_acquire_owned() else {
        return StoreError::StatusBody(503, json!({"error":"Matching request capacity"}))
            .into_response();
    };
    let owner = Arc::new(Owner {
        _slot: slot,
        inner: api.inner.clone(),
    });
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let body = match to_bytes(request.into_body(), 65536).await {
        Ok(v) => v,
        Err(_) => {
            return StoreError::StatusBody(413, json!({"detail":"Request body too large."}))
                .into_response()
        }
    };
    match tokio::spawn(async move{
 let auth=owner.inner.auth.clone().with_retained_scope(owner.clone());
 let user=auth.account_user(peer,headers,false).await?;if user.user_level<10{return Err(StoreError::PermissionDenied);}
 // OPTIONS serializer characterization remains outside this limited producer slice.
 if method==Method::OPTIONS{return Err(StoreError::Unsupported("Matching OPTIONS metadata unqualified"));}
 if method!=Method::POST{return Err(StoreError::StatusBody(405,json!({"detail":format!("Method \"{method}\" not allowed.")})));}
 let pieces:Vec<_>=uri.path().trim_matches('/').split('/').collect();
 let detail=pieces.len()==5;
 let task;let reply;
 if detail{
 let id=pieces[3].parse::<i64>().map_err(|_|StoreError::NotFound)?;
 let params:HashMap<String,String>=url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()).into_owned().collect();
 let (_,from)=crate::channels::SELECT.split_once(" FROM dispatcharr_channels_channel c ").ok_or(StoreError::UnhandledRestFault)?;
 let mut query=sqlx::QueryBuilder::<sqlx::Postgres>::new(format!("SELECT c.id::bigint,c.name FROM dispatcharr_channels_channel c {from}"));
 crate::channels::filters(&mut query,&params,&user,&auth,true).await?;
 query.push(" AND c.id=").push_bind(id).push(" LIMIT 1");
 let row:Option<(i64,String)>=query.build_query_as().fetch_optional(auth.store.pool()).await?;
 let(id,name)=row.ok_or(StoreError::NotFound)?;
 task=TaskRequest::Single(json!(id));reply=json!({"message":format!("EPG matching started for channel '{name}'"),"accepted":true,"channel_id":id});
 }else{
 let value:Value=if body.is_empty(){json!({})}else{serde_json::from_slice(&body).map_err(|e|StoreError::StatusBody(400,json!({"detail":format!("JSON parse error - {}",crate::source_json_error(&body,e))})))?};
 let object=value.as_object().ok_or(StoreError::UnhandledRestFault)?;
 let ids=object.get("channel_ids").cloned().unwrap_or_else(||json!([]));
 task=if crate::channels::truthy(Some(&ids)){TaskRequest::Selected(ids)}else{TaskRequest::All};reply=Value::Null;
 }
 // This admission checks capability only, never catalog matching or an API-time plan.
 crate::channel_epg_match_task::require_supported_domain(&auth,owner.clone()).await?;
 let selected=match &task{TaskRequest::Selected(v)=>Some(v.clone()),_=>None};
 owner.inner.jobs.publish(task,owner.clone()).await?;
 // Source publishes BEFORE evaluating len(): truthy scalar publishes then raises.
 let reply=if !reply.is_null(){reply}else if let Some(ids)=selected{
 let count=match ids{Value::Array(v)=>v.len(),Value::Object(v)=>v.len(),Value::String(v)=>v.chars().count(),_=>return Err(StoreError::UnhandledRestFault)};
 json!({"message":format!("EPG matching task initiated for {count} selected channel(s).")})
 }else{json!({"message":"EPG matching task initiated for all channels without EPG."})};
 Ok(StoreError::StatusBody(202,reply).into_response())
 }).await{Ok(Ok(v))=>v,Ok(Err(e))=>e.into_response(),Err(_)=>StoreError::UnhandledRestFault.into_response()}
}
