//! Source RefreshAccountInfoAPIView, actual producer port and shared Auth budget.
//! Original Dispatcharr/DRF authors retain all credit.
use crate::{AuthService, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Path, Request, State},
    http::Method,
    response::{IntoResponse, Response},
    routing::any,
    Json, Router,
};
use dispatcharr_contracts::BoxFuture;
use serde_json::json;
use sqlx::Row;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
pub trait Publisher: Send + Sync {
    fn publish_vod(
        &self,
        _account: i64,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<serde_json::Value, StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "VOD ingestion producer unavailable",
            ))
        })
    }

    fn publish_all_refresh(
        &self,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "M3U aggregate refresh producer unavailable",
            ))
        })
    }

    fn publish_refresh(
        &self,
        _account: i64,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async {
            Err(StoreError::Unsupported(
                "Normal account refresh producer is unavailable",
            ))
        })
    }

    fn publish_info(
        &self,
        profile: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>>;
}
#[derive(Clone)]
pub struct Http {
    inner: Arc<Inner>,
}
struct Inner {
    auth: AuthService,
    publisher: Arc<dyn Publisher>,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
}
impl Http {
    pub fn prepare(auth: AuthService, publisher: Arc<dyn Publisher>) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth,
                publisher,
                closed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(4)),
            }),
        }
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/m3u/refresh-account-info/{profile}/", any(handle))
            .route(
                "/api/m3u/refresh/{account}/",
                any(refresh_handle).layer(axum::middleware::map_response(refresh_response_headers)),
            )
            .route("/api/m3u/refresh/", any(refresh_all_handle))
            .route("/api/m3u/accounts/{account}/refresh-vod/", any(vod_handle))
            .route(
                "/api/m3u/accounts/{account}/repack-group/",
                any(repack_handle),
            )
            .with_state(self)
    }
    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release)
    }
    pub async fn shutdown(&self, duration: Duration) -> bool {
        self.close_admission();
        match tokio::time::timeout(duration, self.inner.slots.clone().acquire_many_owned(4)).await {
            Ok(Ok(held)) => {
                drop(held);
                true
            }
            _ => false,
        }
    }
}
async fn handle(
    State(api): State<Http>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(profile): Path<i64>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return unavailable().into_response();
    }
    let slot = match api.inner.slots.clone().try_acquire_owned() {
        Ok(v) => v,
        Err(_) => return unavailable().into_response(),
    };
    let method = request.method().clone();
    let headers = request.headers().clone();
    if to_bytes(request.into_body(), 65536).await.is_err() {
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"detail":"Request body too large."})),
        )
            .into_response();
    }
    // The supervised operation outlives HTTP observer loss until actual SQL and
    // Redis acknowledgement, keeping the same Auth4 writer session throughout.
    let result=tokio::spawn(async move{
        let owner=Arc::new((slot,api.inner.clone()));
        let auth=api.inner.auth.clone().with_retained_scope(owner.clone());let user=auth.account_user(peer,headers,false).await?;
        let required=if matches!(method,Method::GET|Method::HEAD){1}else{10};
        if user.user_level<required{return Err(StoreError::PermissionDenied)}
        if method!=Method::POST{return Ok((405,json!({"detail":format!("Method \"{method}\" not allowed.")})))}
        let session=auth.borrow_task_sql_session(owner.clone()).await?;
        let work=async{
            if api.inner.closed.load(Ordering::Acquire){return Err(unavailable())}
            let row=session.run(move|c|Box::pin(async move{sqlx::query("SELECT p.name,a.account_type FROM m3u_m3uaccountprofile p JOIN m3u_m3uaccount a ON a.id=p.m3u_account_id WHERE p.id=$1::bigint").bind(profile).fetch_optional(&mut*c).await})).await??;
            let Some(row)=row else{return Ok((404,json!({"success":false,"error":"Profile not found"})))};
            if row.try_get::<String,_>("account_type")?!="XC"{return Ok((400,json!({"success":false,"error":"Account info refresh is only available for XtreamCodes accounts"})))}
            let name:String=row.try_get("name")?;api.inner.publisher.publish_info(profile,Arc::new((owner.clone(),session.clone()))).await?;
            Ok::<_,StoreError>((202,json!({"success":true,"message":format!("Account info refresh initiated for profile {name}.")})))
        }.await;
        let close=session.close().await;close?;work
    }).await;
    match result {
        Ok(Ok((status, value))) => (
            axum::http::StatusCode::from_u16(status)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            Json(value),
        )
            .into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(_) => unavailable().into_response(),
    }
}
async fn refresh_response_headers(mut response: Response) -> Response {
    if response.status() != axum::http::StatusCode::INTERNAL_SERVER_ERROR {
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("POST, OPTIONS"),
        );
        response.headers_mut().insert(
            axum::http::header::VARY,
            axum::http::HeaderValue::from_static("Accept"),
        );
    }
    response
}
async fn refresh_handle(
    State(api): State<Http>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(account): Path<i64>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return unavailable().into_response();
    }
    let slot = match api.inner.slots.clone().try_acquire_owned() {
        Ok(v) => v,
        Err(_) => return unavailable().into_response(),
    };
    let method = request.method().clone();
    let headers = request.headers().clone();
    if to_bytes(request.into_body(), 65536).await.is_err() {
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"detail":"Request body too large."})),
        )
            .into_response();
    }
    // The supervised operation outlives HTTP observer loss until actual SQL and
    // Redis acknowledgement, keeping the same Auth4 writer session throughout.
    let result = tokio::spawn(async move {
        let owner = Arc::new((slot, api.inner.clone()));
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth.account_user(peer, headers, false).await?;
        let required = if matches!(method, Method::GET | Method::HEAD) {
            1
        } else {
            10
        };
        if user.user_level < required {
            return Err(StoreError::PermissionDenied);
        }
        if method != Method::POST {
            return Ok((
                405,
                json!({"detail":format!("Method \"{method}\" not allowed.")}),
            ));
        }
        if api.inner.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        api.inner.publisher.publish_refresh(account, owner).await?;
        Ok::<_, StoreError>((
            202,
            json!({"success":true,"message":format!("M3U account {account} refresh initiated.")}),
        ))
    })
    .await;
    match result {
        Ok(Ok((status, value))) => (
            axum::http::StatusCode::from_u16(status)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            Json(value),
        )
            .into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(_) => unavailable().into_response(),
    }
}
async fn vod_handle(
    State(api): State<Http>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(account): Path<i64>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return unavailable().into_response();
    }
    let slot = match api.inner.slots.clone().try_acquire_owned() {
        Ok(v) => v,
        Err(_) => return unavailable().into_response(),
    };
    let method = request.method().clone();
    let headers = request.headers().clone();
    if to_bytes(request.into_body(), 65536).await.is_err() {
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"detail":"Request body too large."})),
        )
            .into_response();
    }
    // The supervised operation outlives HTTP observer loss until actual SQL and
    // Redis acknowledgement, keeping the same Auth4 writer session throughout.
    let result = tokio::spawn(async move {
        let owner = Arc::new((slot, api.inner.clone()));
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth.account_user(peer, headers, false).await?;
        let required = if matches!(method, Method::GET | Method::HEAD) {
            1
        } else {
            10
        };
        if user.user_level < required {
            return Err(StoreError::PermissionDenied);
        }
        if method != Method::POST {
            return Ok((
                405,
                json!({"detail":format!("Method \"{method}\" not allowed.")}),
            ));
        }
        if api.inner.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        let value = api.inner.publisher.publish_vod(account, owner).await?;
        Ok::<_, StoreError>((202, value))
    })
    .await;
    match result {
        Ok(Ok((status, value))) => (
            axum::http::StatusCode::from_u16(status)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            Json(value),
        )
            .into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(_) => unavailable().into_response(),
    }
}
async fn refresh_all_handle(
    State(api): State<Http>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return unavailable().into_response();
    }
    let slot = match api.inner.slots.clone().try_acquire_owned() {
        Ok(v) => v,
        Err(_) => return unavailable().into_response(),
    };
    let method = request.method().clone();
    let headers = request.headers().clone();
    if to_bytes(request.into_body(), 65536).await.is_err() {
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"detail":"Request body too large."})),
        )
            .into_response();
    }
    // The supervised operation outlives HTTP observer loss until actual SQL and
    // Redis acknowledgement, keeping the same Auth4 writer session throughout.
    let result = tokio::spawn(async move {
        let owner = Arc::new((slot, api.inner.clone()));
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth.account_user(peer, headers, false).await?;
        let required = if matches!(method, Method::GET | Method::HEAD) {
            1
        } else {
            10
        };
        if user.user_level < required {
            return Err(StoreError::PermissionDenied);
        }
        if method != Method::POST {
            return Ok((
                405,
                json!({"detail":format!("Method \"{method}\" not allowed.")}),
            ));
        }
        if api.inner.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        api.inner.publisher.publish_all_refresh(owner).await?;
        Ok::<_, StoreError>((
            202,
            json!({"success":true,"message":"M3U refresh initiated."}),
        ))
    })
    .await;
    match result {
        Ok(Ok((status, value))) => (
            axum::http::StatusCode::from_u16(status)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            Json(value),
        )
            .into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(_) => unavailable().into_response(),
    }
}
async fn repack_handle(
    State(api): State<Http>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return unavailable().into_response();
    }
    let slot = match api.inner.slots.clone().try_acquire_owned() {
        Ok(slot) => slot,
        Err(_) => return unavailable().into_response(),
    };
    let method = request.method().clone();
    let headers = request.headers().clone();
    let query = request.uri().query().unwrap_or("").to_owned();
    if to_bytes(request.into_body(), 65536).await.is_err() {
        return (
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"detail":"Request body too large."})),
        )
            .into_response();
    }
    let result = tokio::spawn(async move {
        let owner: Arc<dyn Send + Sync> = Arc::new((slot, api.inner.clone()));
        let auth = api.inner.auth.clone().with_retained_scope(owner.clone());
        auth.account_user(peer, headers, true).await?;
        if method == Method::OPTIONS {
            return Err(StoreError::Unsupported(
                "Compact OPTIONS serializer metadata is not composed",
            ));
        }
        if method != Method::POST {
            return Ok((
                405,
                json!({"detail":format!("Method \"{method}\" not allowed.")}),
            ));
        }
        let account = dispatcharr_ingest_core::source_integer_literal(&raw)
            .and_then(|s| s.parse::<i64>().ok())
            .filter(|id| (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(id))
            .ok_or_else(|| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
        let session = auth.borrow_task_sql_session(owner.clone()).await?;
        let work = async {
            let exists = session
                .run(move |c| {
                    Box::pin(async move {
                        sqlx::query_scalar::<_, bool>(
                            "SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE id=$1::bigint)",
                        )
                        .bind(account)
                        .fetch_one(c)
                        .await
                    })
                })
                .await??;
            if !exists {
                return Ok((
                    404,
                    json!({"detail":"No M3UAccount matches the given query."}),
                ));
            }
            let raw_group = url::form_urlencoded::parse(query.as_bytes())
                .filter(|(key, _)| key == "channel_group_id")
                .map(|(_, value)| value.into_owned())
                .last();
            let Some(raw_group) = raw_group.filter(|s| !s.is_empty()) else {
                return Ok((400, json!({"detail":"channel_group_id is required"})));
            };
            if raw_group.len() > 8192 {
                return Err(StoreError::Unsupported(
                    "Compact query integer capacity exceeded",
                ));
            }
            let Some(canonical) = dispatcharr_ingest_core::source_integer_literal(&raw_group)
            else {
                return Ok((400, json!({"detail":"channel_group_id must be an integer"})));
            };
            // Python accepts arbitrary integer spelling; beyond the actual
            // schema's ID range it is a missing relation AFTER lock acquisition.
            let group = canonical.parse::<i64>().ok();
            let value = auth
                .manual_compact_group(&session, account, group, Arc::new((owner, session.clone())))
                .await?;
            Ok::<_, StoreError>((200, value))
        }
        .await;
        let closed = session.close().await;
        closed?;
        match work {
            Err(StoreError::Database(sqlx::Error::Database(_))) => {
                Err(StoreError::UnhandledRestFault)
            }
            result => result,
        }
    })
    .await;
    let mut response = match result {
        Ok(Ok((status, value))) => (
            axum::http::StatusCode::from_u16(status)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            Json(value),
        )
            .into_response(),
        Ok(Err(error)) => error.into_response(),
        Err(_) => unavailable().into_response(),
    };
    response.headers_mut().insert(
        axum::http::header::ALLOW,
        axum::http::HeaderValue::from_static("POST, OPTIONS"),
    );
    response
}
fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"error":"XC account refresh intake or physical capacity unavailable"}),
    )
}
