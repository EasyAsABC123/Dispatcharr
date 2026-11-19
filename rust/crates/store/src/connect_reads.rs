//! Admin Connect collection, ported from Original Dispatcharr's IntegrationViewSet.
//! Original Dispatcharr authors and contributors retain full application credit.
use crate::{AuthService, RestReadLimits, StoreError};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, HeaderMap},
    response::Response,
    routing::get,
    Router,
};
use bytes::Bytes;
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

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
    Router::new()
        .route("/api/connect/integrations/", get(list))
        .with_state(auth)
}

async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
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
            .account_user(peer, headers, true)
            .await?;
        let payload = sqlx::query_scalar::<_, Option<String>>(LIST)
            .bind(RESPONSE_LIMIT)
            .fetch_one(auth.store.pool())
            .await?
            .ok_or_else(capacity)?;
        // PostgreSQL produces valid JSON here. Keep its config tokens and
        // allocation intact, including numbers outside primitive Rust ranges.
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
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_LENGTH, length)
            .body(body)
            .map_err(|_| StoreError::UnhandledRestFault)
    })
    .await
    .map_err(|_| capacity())?
}
