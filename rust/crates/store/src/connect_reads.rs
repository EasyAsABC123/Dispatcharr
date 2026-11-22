//! Admin Connect collection, ported from Original Dispatcharr's IntegrationViewSet.
//! Original Dispatcharr authors and contributors retain full application credit.
use crate::{AuthService, RestReadLimits, StoreError};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
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
        .route("/api/connect/logs.json", get(fixed_json_logs))
        .route("/api/connect/logs.json/", get(fixed_json_logs))
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

async fn fixed_json_logs(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    // Explicit format suffix restricts DRF to JSON before it checks Accept.
    // Keep content negotiation ahead of the shared auth and SQL read owner.
    let mut response = if accepts_log_json(&headers) {
        execute(auth, peer, headers, ConnectRequest::Logs(uri))
            .await
            .into_response()
    } else {
        StoreError::StatusBody(
            StatusCode::NOT_ACCEPTABLE.as_u16(),
            json!({"detail":"Could not satisfy the request Accept header."}),
        )
        .into_response()
    };
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Accept"));
    response
}

fn accepts_log_json(headers: &HeaderMap) -> bool {
    let accepts = headers.get_all(header::ACCEPT);
    let mut values = accepts.iter().peekable();
    if values.peek().is_none() {
        return true;
    }
    values.any(|value| {
        let value = dispatcharr_contracts::source_header_value(value.as_bytes());
        value.split(',').any(|media| {
            let kind = media.split(';').next().unwrap_or("").trim();
            kind == "*/*"
                || kind.eq_ignore_ascii_case("application/*")
                || kind.eq_ignore_ascii_case("application/json")
        })
    })
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
    fn fixed_json_accept_matches_normal_drf_media_types() {
        for (accept, expected) in [
            (None, true),
            (Some("*/*"), true),
            (Some("application/*"), true),
            (Some("application/json"), true),
            (Some("APPLICATION/JSON"), true),
            (Some("text/html, application/json;q=0.3"), true),
            (Some("application/json; indent=2"), true),
            (Some("text/html"), false),
            (Some("text/*"), false),
            (Some("application/xml"), false),
            (Some(""), false),
        ] {
            let mut headers = HeaderMap::new();
            if let Some(accept) = accept {
                headers.insert(header::ACCEPT, HeaderValue::from_static(accept));
            }
            assert_eq!(accepts_log_json(&headers), expected, "{accept:?}");
        }
    }

    #[tokio::test]
    async fn fixed_json_log_routes_qualify_exact_406_and_keep_other_access() {
        use crate::{AuthConfig, Store};
        use axum::{body::to_bytes, http::Request};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused@127.0.0.1:1/unused")
            .unwrap();
        let app = router(AuthService::new(
            Store { pool },
            AuthConfig::new("local-fixture".into()).unwrap(),
        ));
        for uri in ["/api/connect/logs.json", "/api/connect/logs.json/"] {
            for (accept, code) in [
                (Some("text/html"), StatusCode::NOT_ACCEPTABLE),
                (Some("application/json"), StatusCode::UNAUTHORIZED),
                (Some("*/*"), StatusCode::UNAUTHORIZED),
                (None, StatusCode::UNAUTHORIZED),
            ] {
                let mut request = Request::builder()
                    .uri(uri)
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 19000))));
                if let Some(accept) = accept {
                    request = request.header(header::ACCEPT, accept);
                }
                let response = app
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), code);
                assert_eq!(response.headers()[header::VARY], "Accept");
                if code == StatusCode::NOT_ACCEPTABLE {
                    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
                    assert!(!response.headers().contains_key(header::WWW_AUTHENTICATE));
                    let body = to_bytes(response.into_body(), 57).await.unwrap();
                    assert_eq!(
                        &body[..],
                        br#"{"detail":"Could not satisfy the request Accept header."}"#
                    );
                } else {
                    assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
                }
            }
        }
        for (uri, code) in [
            ("/api/connect/logs/", StatusCode::UNAUTHORIZED),
            ("/api/connect/logs.api", StatusCode::NOT_FOUND),
        ] {
            let request = Request::builder()
                .uri(uri)
                .header(header::ACCEPT, "text/html")
                .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 19000))))
                .body(Body::empty())
                .unwrap();
            assert_eq!(app.clone().oneshot(request).await.unwrap().status(), code);
        }
    }

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
