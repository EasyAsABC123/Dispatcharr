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
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub trait Publisher: Send + Sync {
    /// Publishes the root after a settings edit has committed its owned SQL.
    fn publish_vod_settings_root(
        &self,
        _account: i64,
        _owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async { Err(StoreError::Unsupported("VOD settings producer unavailable")) })
    }

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
    publisher: Arc<dyn Publisher>,
    repack: Repack,
    vod_settings: Arc<VodSettings>,
}

const REQUEST_SLOTS: u32 = 4;
const CLOSED: u8 = 1;
const REPACK_USED: u8 = 2;

pub(crate) struct VodSettings {
    publisher: Arc<dyn Publisher>,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
}

pub(crate) struct AdmittedVodSettings {
    publisher: Arc<dyn Publisher>,
    slot: OwnedSemaphorePermit,
}

impl VodSettings {
    fn new(publisher: Arc<dyn Publisher>) -> Self {
        Self {
            publisher,
            closed: AtomicBool::new(false),
            slots: Arc::new(Semaphore::new(REQUEST_SLOTS as usize)),
        }
    }

    fn admit(&self) -> Result<AdmittedVodSettings, StoreError> {
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        Ok(AdmittedVodSettings {
            publisher: self.publisher.clone(),
            slot,
        })
    }
}

impl AuthService {
    pub(crate) fn admit_xc_vod_settings(&self) -> Result<AdmittedVodSettings, StoreError> {
        self.xc_vod_settings
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(StoreError::Unsupported("VOD settings producer unavailable"))?
            .admit()
    }
}

impl AdmittedVodSettings {
    pub(crate) async fn publish(
        self,
        account: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), StoreError> {
        self.publisher
            .publish_vod_settings_root(account, Arc::new((owner, self.slot)))
            .await
    }
}

#[derive(Clone)]
pub struct Repack {
    inner: Arc<RepackIntake>,
}

struct RepackIntake {
    auth: AuthService,
    admission: AtomicU8,
    slots: Arc<Semaphore>,
}
impl Http {
    pub fn prepare(auth: AuthService, publisher: Arc<dyn Publisher>) -> Self {
        Self {
            inner: Arc::new(Inner {
                vod_settings: Arc::new(VodSettings::new(publisher.clone())),
                publisher,
                repack: Repack::prepare(auth),
            }),
        }
    }

    pub fn bind_vod_settings(&self, auth: &AuthService) -> Result<(), StoreError> {
        auth.xc_vod_settings
            .set(Arc::downgrade(&self.inner.vod_settings))
            .map_err(|_| StoreError::Unsupported("VOD settings producer already bound"))
    }
    pub fn router(self) -> Router {
        let repack = self.inner.repack.clone().router();
        Router::new()
            .route("/api/m3u/refresh-account-info/{profile}/", any(handle))
            .route(
                "/api/m3u/refresh/{account}/",
                any(refresh_handle).layer(axum::middleware::map_response(refresh_response_headers)),
            )
            .route("/api/m3u/refresh/", any(refresh_all_handle))
            .route("/api/m3u/accounts/{account}/refresh-vod/", any(vod_handle))
            .with_state(self)
            .merge(repack)
    }
    pub fn refresh_all_router(self) -> Router {
        Router::new()
            .route("/api/m3u/refresh/", any(refresh_all_handle))
            .with_state(self)
    }
    pub fn close_admission(&self) {
        self.inner
            .vod_settings
            .closed
            .store(true, Ordering::Release);
        self.inner.repack.close_admission();
    }

    pub async fn shutdown(&self, duration: Duration) -> bool {
        self.close_admission();
        let (repack, vod_settings) = tokio::join!(
            self.inner.repack.shutdown(duration),
            tokio::time::timeout(
                duration,
                self.inner
                    .vod_settings
                    .slots
                    .clone()
                    .acquire_many_owned(REQUEST_SLOTS),
            ),
        );
        repack && vod_settings.is_ok_and(|result| result.is_ok())
    }
}

impl Repack {
    pub fn prepare(auth: AuthService) -> Self {
        Self {
            inner: Arc::new(RepackIntake {
                auth,
                admission: AtomicU8::new(0),
                slots: Arc::new(Semaphore::new(REQUEST_SLOTS as usize)),
            }),
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route(
                "/api/m3u/accounts/{account}/repack-group/",
                any(repack_handle),
            )
            .with_state(self)
    }

    fn is_closed(&self) -> bool {
        self.inner.admission.load(Ordering::Acquire) & CLOSED != 0
    }

    fn admit_operation(&self) -> bool {
        // The HTTP slot is already held. Record use in the same transition
        // that checks closure, before creating a detached SQL owner.
        self.inner
            .admission
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state & CLOSED == 0).then_some(state | REPACK_USED)
            })
            .is_ok()
    }

    pub fn close_admission(&self) {
        self.inner.admission.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub fn was_used(&self) -> bool {
        self.inner.admission.load(Ordering::Acquire) & REPACK_USED != 0
    }

    pub async fn shutdown(&self, duration: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(
            duration,
            self.inner.slots.clone().acquire_many_owned(REQUEST_SLOTS),
        )
        .await
        .is_ok_and(|result| result.is_ok())
    }
}

async fn handle(
    State(api): State<Http>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(profile): Path<i64>,
    request: Request,
) -> Response {
    if api.inner.repack.is_closed() {
        return unavailable().into_response();
    }
    let slot = match api.inner.repack.inner.slots.clone().try_acquire_owned() {
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
        let auth=api.inner.repack.inner.auth.clone().with_retained_scope(owner.clone());let user=auth.account_user(peer,headers,false).await?;
        let required=if matches!(method,Method::GET|Method::HEAD){1}else{10};
        if user.user_level<required{return Err(StoreError::PermissionDenied)}
        if method!=Method::POST{return Ok((405,json!({"detail":format!("Method \"{method}\" not allowed.")})))}
        let session=auth.borrow_task_sql_session(owner.clone()).await?;
        let work=async{
            if api.inner.repack.is_closed(){return Err(unavailable())}
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
    if api.inner.repack.is_closed() {
        return unavailable().into_response();
    }
    let slot = match api.inner.repack.inner.slots.clone().try_acquire_owned() {
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
        let auth = api
            .inner
            .repack
            .inner
            .auth
            .clone()
            .with_retained_scope(owner.clone());
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
        if api.inner.repack.is_closed() {
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
    if api.inner.repack.is_closed() {
        return unavailable().into_response();
    }
    let slot = match api.inner.repack.inner.slots.clone().try_acquire_owned() {
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
        let auth = api
            .inner
            .repack
            .inner
            .auth
            .clone()
            .with_retained_scope(owner.clone());
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
        if api.inner.repack.is_closed() {
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
    if api.inner.repack.is_closed() {
        return unavailable().into_response();
    }
    let slot = match api.inner.repack.inner.slots.clone().try_acquire_owned() {
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
        let auth = api
            .inner
            .repack
            .inner
            .auth
            .clone()
            .with_retained_scope(owner.clone());
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
        if api.inner.repack.is_closed() {
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
    State(api): State<Repack>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    request: Request,
) -> Response {
    if api.is_closed() {
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
    if !api.admit_operation() {
        return unavailable().into_response();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> AuthService {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@localhost/unused")
            .unwrap();
        AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("test-signing-secret-only".into()).unwrap(),
        )
    }

    #[tokio::test]
    async fn repack_owner_retains_the_http_drain_slot_until_completion() {
        let repack = Repack::prepare(auth());
        let slot = repack.inner.slots.clone().try_acquire_owned().unwrap();
        assert!(repack.admit_operation());
        assert!(repack.was_used());

        assert!(!repack.shutdown(Duration::ZERO).await);
        assert!(!repack.admit_operation());
        drop(slot);
        assert!(repack.shutdown(Duration::ZERO).await);
    }

    #[tokio::test]
    async fn unused_repack_intake_cannot_begin_after_closure() {
        let repack = Repack::prepare(auth());
        let waiting_request = repack.inner.slots.clone().try_acquire_owned().unwrap();
        repack.close_admission();

        assert!(!repack.admit_operation());
        assert!(!repack.was_used());
        drop(waiting_request);
        assert!(repack.shutdown(Duration::ZERO).await);
    }

    #[tokio::test]
    async fn xc_vod_settings_owner_holds_admission_until_transport_settles() {
        type PhysicalOwner = Arc<dyn Send + Sync>;
        struct Capture(std::sync::Mutex<Option<PhysicalOwner>>);
        impl Publisher for Capture {
            fn publish_info(
                &self,
                _id: i64,
                _owner: PhysicalOwner,
            ) -> BoxFuture<'_, Result<(), StoreError>> {
                Box::pin(async { panic!("settings publishes only VOD roots") })
            }

            fn publish_vod_settings_root(
                &self,
                id: i64,
                owner: PhysicalOwner,
            ) -> BoxFuture<'_, Result<(), StoreError>> {
                Box::pin(async move {
                    assert_eq!(id, 45);
                    assert!(self.0.lock().unwrap().replace(owner).is_none());
                    Ok(())
                })
            }
        }
        let auth = auth();
        let publisher = Arc::new(Capture(std::sync::Mutex::new(None)));
        let http = Http::prepare(auth.clone(), publisher.clone());
        assert!(auth.admit_xc_vod_settings().is_err());
        http.bind_vod_settings(&auth).unwrap();
        let admitted = auth.admit_xc_vod_settings().unwrap();
        http.close_admission();
        assert!(auth.admit_xc_vod_settings().is_err());
        admitted.publish(45, Arc::new(())).await.unwrap();
        assert!(!http.shutdown(Duration::ZERO).await);
        publisher.0.lock().unwrap().take().unwrap();
        assert!(http.shutdown(Duration::ZERO).await);
    }
}
