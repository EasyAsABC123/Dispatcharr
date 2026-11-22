//! Admin Connect collection, ported from Original Dispatcharr's IntegrationViewSet.
//! Original Dispatcharr authors and contributors retain full application credit.
use crate::{AuthService, RestReadLimits, StoreError};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::Response,
    routing::{get, post, put},
    Router,
};
use bytes::Bytes;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

#[path = "connect_create.rs"]
mod create_integration;
#[path = "connect_delivery_logs.rs"]
mod delivery_logs;
#[path = "connect_subscription_manage.rs"]
mod subscription_manage;
#[path = "connect_subscriptions.rs"]
mod subscriptions;
#[path = "connect_test.rs"]
mod test_integration;

// Bound the total projection in the same read that obtains the response.
// The quote covers nested rows, JSON-escaped strings, field names and separators.
const RESPONSE_LIMIT: i64 = 1024 * 1024;
const LIST: &str = r#"
WITH size_bound AS (
    SELECT (SELECT COALESCE(SUM(octet_length(to_json(i.name)::text)::bigint
                           + octet_length(to_json(i.type)::text)
                           + octet_length(i.config::text) + 256),0)
              FROM dispatcharr_connect_integration i)
         + (SELECT COALESCE(SUM(octet_length(to_json(s.event)::text)::bigint
                           + COALESCE(octet_length(to_json(s.payload_template)::text),4) + 192),0)
              FROM dispatcharr_connect_eventsubscription s) + 2 AS bytes
)
SELECT CASE WHEN size_bound.bytes <= $1::bigint THEN (
    SELECT COALESCE(json_agg(json_build_object(
        'id',i.id,'name',i.name,'type',i.type,'config',i.config,'enabled',i.enabled,
        'created_at',to_char(i.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS')
          || CASE WHEN MOD(EXTRACT(MICROSECONDS FROM i.created_at)::bigint,1000000)=0
                  THEN '' ELSE to_char(i.created_at AT TIME ZONE 'UTC','.US') END || 'Z',
        'subscriptions',COALESCE((SELECT json_agg(json_build_object(
            'id',s.id,'event',s.event,'enabled',s.enabled,
            'payload_template',s.payload_template,'integration',s.integration_id))
            FROM dispatcharr_connect_eventsubscription s WHERE s.integration_id=i.id),'[]'::json)
    )),'[]'::json)::text FROM dispatcharr_connect_integration i
) END FROM size_bound
"#;

struct RetainedResponse {
    bytes: Vec<u8>,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for RetainedResponse {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Connect collection capacity unavailable."}),
    )
}

pub(crate) fn router(auth: AuthService) -> Router {
    let subscription_collection = get(list_subscriptions).post(create_subscription);
    let subscription_detail = get(retrieve_subscription)
        .put(write_subscription)
        .patch(write_subscription)
        .delete(delete_subscription);
    Router::new()
        .route("/api/connect/integrations/", get(list).post(create))
        .route("/api/connect/logs/", get(logs))
        .route("/api/connect/logs.json", get(logs))
        .route("/api/connect/logs.json/", get(logs))
        .route("/api/connect/logs/{id}/", get(retrieve_log))
        .route("/api/connect/logs/{id}", get(retrieve_log))
        .route(
            "/api/connect/integrations/{id}/",
            get(retrieve_integration)
                .put(update)
                .patch(patch_integration)
                .delete(delete),
        )
        .route(
            "/api/connect/integrations/{id}",
            get(retrieve_integration).patch(patch_integration),
        )
        .route(
            "/api/connect/integrations/{id}/subscriptions/",
            get(integration_subscriptions),
        )
        .route(
            "/api/connect/integrations/{id}/subscriptions.json",
            get(integration_subscriptions),
        )
        .route(
            "/api/connect/integrations/{id}/subscriptions.json/",
            get(integration_subscriptions),
        )
        .route(
            "/api/connect/integrations/{id}/subscriptions/set/",
            put(replace_subscriptions),
        )
        .route("/api/connect/integrations/{id}/test/", post(test_saved))
        .route(
            "/api/connect/subscriptions/",
            subscription_collection.clone(),
        )
        .route(
            "/api/connect/subscriptions.json",
            subscription_collection.clone(),
        )
        .route("/api/connect/subscriptions.json/", subscription_collection)
        .route(
            "/api/connect/subscriptions/{id}/",
            subscription_detail.clone(),
        )
        .route("/api/connect/subscriptions/{id}", subscription_detail)
        .with_state(auth)
}

fn detail_path(raw: &str, uri: &Uri) -> Result<String, StoreError> {
    // Matchit's dynamic capture occupies the whole path segment. DefaultRouter
    // uses the same lookup for canonical slash and JSON (+ optional slash).
    if let Some(id) = raw
        .strip_suffix(".json")
        .filter(|id| !id.is_empty() && !id.contains('.'))
    {
        Ok(id.into())
    } else if uri.path().ends_with('/') && !raw.contains('.') {
        Ok(raw.into())
    } else {
        Err(StoreError::NotFound)
    }
}

fn lookup(raw: &str) -> Result<Option<i64>, StoreError> {
    dispatcharr_ingest_core::source_integer_literal(raw)
        .map(|id| id.parse().ok())
        .ok_or(StoreError::NotFound)
}

async fn retrieve_integration(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    let id = detail_path(&raw, &uri)?;
    execute(auth, peer, headers, ConnectRequest::Integration(id)).await
}

async fn patch_integration(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    let id = detail_path(&raw, request.uri())?;
    execute(
        auth,
        peer,
        request.headers().clone(),
        ConnectRequest::Update(id, request, true),
    )
    .await
}

async fn integration_subscriptions(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    // Source's custom list_subscriptions action has no format argument.
    let json_suffix = uri
        .path()
        .trim_end_matches('/')
        .ends_with("/subscriptions.json");
    execute(
        auth,
        peer,
        headers,
        ConnectRequest::IntegrationSubscriptions(id, json_suffix),
    )
    .await
}

async fn list_subscriptions(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    execute(auth, peer, headers, ConnectRequest::Subscriptions).await
}

async fn create_subscription(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    execute(
        auth,
        peer,
        request.headers().clone(),
        ConnectRequest::CreateSubscription(request),
    )
    .await
}

async fn retrieve_subscription(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    let id = detail_path(&raw, &uri)?;
    execute(auth, peer, headers, ConnectRequest::Subscription(id)).await
}

async fn write_subscription(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    let id = detail_path(&raw, request.uri())?;
    let partial = request.method() == Method::PATCH;
    execute(
        auth,
        peer,
        request.headers().clone(),
        ConnectRequest::WriteSubscription(id, request, partial),
    )
    .await
}

async fn delete_subscription(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    let id = detail_path(&raw, &uri)?;
    execute(auth, peer, headers, ConnectRequest::DeleteSubscription(id)).await
}

async fn retrieve_log(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(raw): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    let id = detail_path(&raw, &uri)?;
    execute(auth, peer, headers, ConnectRequest::Log(id, uri)).await
}

fn no_content() -> Result<Response, StoreError> {
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|_| StoreError::UnhandledRestFault)
}

async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    execute(auth, peer, headers, ConnectRequest::Integrations).await
}

async fn logs(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    execute(auth, peer, headers, ConnectRequest::Logs(uri)).await
}

enum ConnectRequest {
    Integrations,
    Integration(String),
    IntegrationSubscriptions(String, bool),
    Subscriptions,
    Subscription(String),
    CreateSubscription(Request),
    WriteSubscription(String, Request, bool),
    DeleteSubscription(String),
    Logs(Uri),
    Log(String, Uri),
    Create(Request),
    Update(String, Request, bool),
    Delete(String),
    Test(String),
    ReplaceSubscriptions(String, Request),
}

async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Response, StoreError> {
    execute(
        auth,
        peer,
        request.headers().clone(),
        ConnectRequest::Create(request),
    )
    .await
}

async fn replace_subscriptions(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    execute(
        auth,
        peer,
        request.headers().clone(),
        ConnectRequest::ReplaceSubscriptions(id, request),
    )
    .await
}

async fn update(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    request: Request,
) -> Result<Response, StoreError> {
    execute(
        auth,
        peer,
        request.headers().clone(),
        ConnectRequest::Update(id, request, false),
    )
    .await
}

async fn delete(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    execute(auth, peer, headers, ConnectRequest::Delete(id)).await
}

async fn test_saved(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    execute(auth, peer, headers, ConnectRequest::Test(id)).await
}

async fn execute(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    request: ConnectRequest,
) -> Result<Response, StoreError> {
    let permit = Arc::new(
        auth.user_agent_workers
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?,
    );
    let auth = auth.with_request_writer_permit(permit.clone())?;
    // The established Store permit survives client cancellation and remains
    // with the returned body until Hyper consumes or discards it.
    tokio::spawn(async move {
        auth.retained_rest_reads(RestReadLimits::default())?
            .account_user(peer, headers.clone(), true)
            .await?;
        let (status, payload) = match request {
            ConnectRequest::Integrations => (
                StatusCode::OK,
                sqlx::query_scalar::<_, Option<String>>(LIST)
                    .bind(RESPONSE_LIMIT)
                    .fetch_one(auth.store.pool())
                    .await?
                    .ok_or_else(capacity)?,
            ),
            ConnectRequest::Integration(id) => (
                StatusCode::OK,
                create_integration::retrieve(&auth.store, &id).await?,
            ),
            ConnectRequest::IntegrationSubscriptions(_, true) => {
                return Err(StoreError::UnhandledRestFault);
            }
            ConnectRequest::IntegrationSubscriptions(id, false) => (
                StatusCode::OK,
                subscription_manage::list(&auth.store, Some(&id)).await?,
            ),
            ConnectRequest::Subscriptions => (
                StatusCode::OK,
                subscription_manage::list(&auth.store, None).await?,
            ),
            ConnectRequest::Subscription(id) => (
                StatusCode::OK,
                subscription_manage::retrieve(&auth.store, &id).await?,
            ),
            ConnectRequest::CreateSubscription(request) => (
                StatusCode::CREATED,
                subscription_manage::create(&auth.store, request).await?,
            ),
            ConnectRequest::WriteSubscription(id, request, partial) => (
                StatusCode::OK,
                subscription_manage::update(&auth.store, &id, request, partial).await?,
            ),
            ConnectRequest::DeleteSubscription(id) => {
                subscription_manage::delete(&auth.store, &id).await?;
                return no_content();
            }
            ConnectRequest::Logs(uri) => (
                StatusCode::OK,
                delivery_logs::list(&auth.store, &headers, &uri).await?,
            ),
            ConnectRequest::Log(id, uri) => (
                StatusCode::OK,
                delivery_logs::retrieve(&auth.store, &id, &uri).await?,
            ),
            ConnectRequest::Create(request) => (
                StatusCode::CREATED,
                create_integration::create(&auth.store, request).await?,
            ),
            ConnectRequest::Update(id, request, partial) => (
                StatusCode::OK,
                create_integration::update(&auth.store, &id, request, partial).await?,
            ),
            ConnectRequest::Delete(id) => {
                create_integration::delete(&auth.store, &id).await?;
                return no_content();
            }
            ConnectRequest::Test(id) => test_integration::test_saved(&auth.store, &id).await?,
            ConnectRequest::ReplaceSubscriptions(id, request) => (
                StatusCode::OK,
                subscriptions::replace(&auth.store, id, request).await?,
            ),
        };
        // Keep the projection and allocation intact, including config JSON
        // numbers outside primitive Rust ranges.
        let bytes = payload.into_bytes();
        if bytes.len() > RESPONSE_LIMIT as usize {
            return Err(capacity());
        }
        let length = bytes.len();
        let body = Body::from(Bytes::from_owner(RetainedResponse {
            bytes,
            _permit: permit,
        }));
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, length)
            .body(body)
            .map_err(|_| StoreError::UnhandledRestFault)
    })
    .await
    .map_err(|_| capacity())?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_detail_suffix_preserves_the_same_lookup() {
        for uri in [
            "/api/connect/subscriptions/17/",
            "/api/connect/subscriptions/17.json",
            "/api/connect/subscriptions/17.json/",
        ] {
            let raw = uri.trim_end_matches('/').rsplit('/').next().unwrap();
            assert_eq!(detail_path(raw, &uri.parse().unwrap()).unwrap(), "17");
        }
        for uri in [
            "/api/connect/subscriptions/17",
            "/api/connect/subscriptions/17.xml/",
            "/api/connect/subscriptions/17.xml.json",
        ] {
            let raw = uri.trim_end_matches('/').rsplit('/').next().unwrap();
            assert!(detail_path(raw, &uri.parse().unwrap()).is_err());
        }
    }
}
