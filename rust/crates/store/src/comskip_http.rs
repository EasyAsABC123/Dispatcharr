//! Bounded admin manual recording comskip producer. Original authors retain credit.
use crate::{AuthService, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Path, Request, State},
    http::Method,
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use dispatcharr_contracts::BoxFuture;
use serde_json::json;
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
pub const EXISTS: &str = "SELECT id::bigint FROM dispatcharr_channels_recording WHERE id=$1";
pub trait Publisher: Send + Sync {
    fn auth_service(&self) -> AuthService;
    fn admission(&self) -> Result<(), StoreError>;
    fn publish(&self, recording: i64, owner: Scope) -> BoxFuture<'_, Result<Uuid, StoreError>>;
}
struct Inner {
    auth: AuthService,
    publisher: Arc<dyn Publisher>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct ComskipHttp {
    inner: Arc<Inner>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    inner: Arc<Inner>,
}
impl ComskipHttp {
    pub fn prepare(publisher: Arc<dyn Publisher>) -> Self {
        Self {
            inner: Arc::new(Inner {
                auth: publisher.auth_service(),
                publisher,
                slots: Arc::new(Semaphore::new(4)),
                closed: AtomicBool::new(false),
            }),
        }
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/api/channels/recordings/{id}/comskip/", any(handle))
            .with_state(self)
    }
    pub fn close_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    pub async fn shutdown(&self, budget: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(budget, self.inner.slots.clone().acquire_many_owned(4))
            .await
            .is_ok_and(|v| v.is_ok())
    }
}
async fn handle(
    State(api): State<ComskipHttp>,
    Path(id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    if api.inner.closed.load(Ordering::Acquire) {
        return StoreError::Unsupported("Comskip intake closed").into_response();
    }
    let Ok(slot) = api.inner.slots.clone().try_acquire_owned() else {
        return StoreError::Unsupported("Comskip request capacity").into_response();
    };
    let owner = Arc::new(Owner {
        _slot: slot,
        inner: api.inner.clone(),
    });
    match tokio::spawn(async move {
        let auth = owner.inner.auth.clone().with_retained_scope(owner.clone());
        let user = auth
            .account_user(peer, request.headers().clone(), false)
            .await?;
        // Initial declared slice uses actual admin principal; non-admin scoped DVR
        // object/permission matrix stays unqualified instead of bypassing Source.
        if user.user_level < 10 {
            return Err(StoreError::Unsupported(
                "Comskip non-admin DVR scope unqualified",
            ));
        }
        if request.method() != Method::POST {
            return Err(StoreError::Unsupported(
                "Comskip method metadata unqualified",
            ));
        }
        let _body = to_bytes(request.into_body(), 65536)
            .await
            .map_err(|_| StoreError::Unsupported("Comskip request body capacity"))?;
        let recording = id
            .parse::<i64>()
            .ok()
            .filter(|v| *v > 0 && *v <= i64::from(i32::MAX))
            .ok_or(StoreError::NotFound)?;
        owner.inner.publisher.admission()?;
        let session = auth.borrow_task_sql_session(owner.clone()).await?;
        let found = session
            .run(move |c| {
                Box::pin(async move {
                    sqlx::query_scalar::<_, i64>(EXISTS)
                        .bind(recording)
                        .fetch_optional(c)
                        .await
                        .map_err(StoreError::from)
                })
            })
            .await;
        session.close().await?;
        if found??.is_none() {
            return Err(StoreError::NotFound);
        }
        // Exactly the real shared publisher; API does not run task or inspect file.
        owner
            .inner
            .publisher
            .publish(recording, owner.clone())
            .await?;
        Ok::<_, StoreError>(
            StoreError::StatusBody(200, json!({"success":true,"queued":true})).into_response(),
        )
    })
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => e.into_response(),
        Err(_) => StoreError::UnhandledRestFault.into_response(),
    }
}
