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
            .route("/api/m3u/refresh/{account}/", any(refresh_handle))
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
fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"error":"XC account refresh intake or physical capacity unavailable"}),
    )
}
