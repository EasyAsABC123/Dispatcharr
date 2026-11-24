//! Original Dispatcharr core helper read APIs. Original authors retain credit.
//! The timezone dataset is pinned to the original runtime, not the host OS.
use crate::{
    channels::{
        channel_options_headers, channel_options_indentation, options_json_response,
        serialize_options_json,
    },
    AuthService, StoreError,
};
#[path = "persisted_log_list.rs"]
mod persisted_logs;
use axum::{
    extract::{ConnectInfo, RawQuery, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, SecondsFormat, Utc};
use num_bigint::BigInt;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{net::SocketAddr, sync::OnceLock};

const TIMEZONE_ALLOW: HeaderValue = HeaderValue::from_static("GET, HEAD, OPTIONS");

fn timezone_data() -> &'static Value {
    static DATA: OnceLock<Value> = OnceLock::new();
    DATA.get_or_init(|| {
        let zones: Vec<String> =
            serde_json::from_str(include_str!("../datasets/pytz-common-timezones.json"))
                .expect("pinned pytz dataset");
        let mut grouped = Map::new();
        for zone in &zones {
            let region = zone.split_once('/').map_or("Other", |(region, _)| region);
            grouped
                .entry(region)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("timezone group")
                .push(zone.clone().into());
        }
        json!({"timezones":zones,"grouped":grouped,"count":zones.len()})
    })
}
async fn timezones(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let response = match auth.account_user(peer, headers, false).await {
        Ok(_) => Json(timezone_data().clone()).into_response(),
        Err(error) => error.into_response(),
    };
    channel_options_headers(response, TIMEZONE_ALLOW)
}

fn utility_options(
    name: &str,
    description: &str,
    indentation: usize,
) -> Result<Response, StoreError> {
    let metadata = json!({
        "name": name,
        "description": description,
        "renders": ["application/json", "text/html"],
        "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"]
    });
    options_json_response(
        serialize_options_json(
            metadata.as_object().expect("timezone metadata"),
            indentation,
        )
        .ok_or(StoreError::UnhandledRestFault)?,
    )
}

fn timezone_options_entity(indentation: usize) -> Result<Response, StoreError> {
    utility_options(
        "Timezone List",
        "API endpoint that returns all available timezones supported by pytz.\nReturns a list of timezone names grouped by region for easy selection.\nThis is a general utility endpoint that can be used throughout the application.",
        indentation,
    )
}

async fn timezone_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let Ok(indentation) = channel_options_indentation(&uri, &headers) else {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::ALLOW, "GET,HEAD")],
        )
            .into_response();
    };
    let response = match auth.account_user(peer, headers, false).await {
        Ok(_) => timezone_options_entity(indentation).unwrap_or_else(IntoResponse::into_response),
        Err(error) => error.into_response(),
    };
    channel_options_headers(response, TIMEZONE_ALLOW)
}

fn event_error() -> StoreError {
    StoreError::StatusBody(500, json!({"error":"Failed to fetch system events"}))
}
fn integer(raw: &str) -> Result<BigInt, StoreError> {
    let literal = dispatcharr_ingest_core::source_integer_literal(raw).ok_or_else(event_error)?;
    BigInt::parse_bytes(literal.as_bytes(), 10).ok_or_else(event_error)
}
struct Page {
    limit: BigInt,
    offset: BigInt,
    fetch: Option<(i64, i64)>,
    kind: Option<String>,
}
fn page(raw: Option<String>) -> Result<Page, StoreError> {
    let params: std::collections::HashMap<String, String> =
        url::form_urlencoded::parse(raw.as_deref().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    let limit = integer(params.get("limit").map_or("100", String::as_str))?.min(1000.into());
    let offset = integer(params.get("offset").map_or("0", String::as_str))?;
    let stop = &offset + &limit;
    if offset < BigInt::from(0) || stop < BigInt::from(0) {
        return Err(event_error());
    }
    let fetch = if limit <= 0.into() {
        None
    } else {
        Some((
            limit.to_string().parse().map_err(|_| event_error())?,
            offset.to_string().parse().map_err(|_| event_error())?,
        ))
    };
    Ok(Page {
        limit,
        offset,
        fetch,
        kind: params.get("event_type").filter(|v| !v.is_empty()).cloned(),
    })
}
fn display(kind: &str) -> &str {
    match kind {
        "channel_start" => "Channel Started",
        "channel_stop" => "Channel Stopped",
        "channel_buffering" => "Channel Buffering",
        "channel_failover" => "Channel Failover",
        "channel_reconnect" => "Channel Reconnected",
        "channel_error" => "Channel Error",
        "client_connect" => "Client Connected",
        "client_disconnect" => "Client Disconnected",
        "recording_start" => "Recording Started",
        "recording_end" => "Recording Ended",
        "stream_switch" => "Stream Switched",
        "m3u_refresh" => "M3U Refreshed",
        "m3u_download" => "M3U Downloaded",
        "m3u_error" => "M3U Error",
        "epg_refresh" => "EPG Refreshed",
        "epg_download" => "EPG Downloaded",
        "epg_error" => "EPG Error",
        "login_success" => "Login Successful",
        "login_failed" => "Login Failed",
        "logout" => "User Logged Out",
        "m3u_blocked" => "M3U Download Blocked",
        "epg_blocked" => "EPG Download Blocked",
        "vod_start" => "VOD Started",
        "vod_stop" => "VOD Stopped",
        other => other,
    }
}
async fn event_data(auth: &AuthService, raw: Option<String>) -> Result<Value, StoreError> {
    let page = page(raw)?;
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core_systemevent WHERE ($1::text IS NULL OR event_type=$1)",
    )
    .bind(&page.kind)
    .fetch_one(auth.store.pool())
    .await?;
    let mut events = Vec::new();
    if let Some((limit, offset)) = page.fetch {
        let rows=sqlx::query("SELECT id::bigint AS id,event_type,timestamp,channel_id,channel_name,details FROM core_systemevent WHERE ($1::text IS NULL OR event_type=$1) ORDER BY timestamp DESC LIMIT $2 OFFSET $3").bind(&page.kind).bind(limit).bind(offset).fetch_all(auth.store.pool()).await?;
        for row in rows {
            let kind: String = row.try_get("event_type")?;
            let timestamp: DateTime<Utc> = row.try_get("timestamp")?;
            let timestamp = timestamp.to_rfc3339_opts(
                if timestamp.timestamp_subsec_micros() == 0 {
                    SecondsFormat::Secs
                } else {
                    SecondsFormat::Micros
                },
                false,
            );
            events.push(json!({"id":row.try_get::<i64,_>("id")?,"event_type":kind,"event_type_display":display(&kind),"timestamp":timestamp,"channel_id":row.try_get::<Option<uuid::Uuid>,_>("channel_id")?,"channel_name":row.try_get::<Option<String>,_>("channel_name")?,"details":row.try_get::<Value,_>("details")?}));
        }
    }
    Ok(
        json!({"events":events,"count":events.len(),"total":total,"offset":serde_json::from_str::<Value>(&page.offset.to_string()).expect("integer"),"limit":serde_json::from_str::<Value>(&page.limit.to_string()).expect("integer")}),
    )
}
const SYSTEM_EVENTS_ALLOW: HeaderValue = HeaderValue::from_static("GET, OPTIONS");
const SYSTEM_EVENTS_DESCRIPTION: &str = "Get recent system events (channel start/stop, buffering, client connections, etc.)\n\nQuery Parameters:\n    limit: Number of events to return per page (default: 100, max: 1000)\n    offset: Number of events to skip (for pagination, default: 0)\n    event_type: Filter by specific event type (optional)";

async fn events(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw): RawQuery,
    uri: Uri,
    method: Method,
) -> Response {
    let result = async {
        crate::epg_program_http::json_negotiation(&headers, &uri)?;
        let indentation = (method == Method::OPTIONS)
            .then(|| channel_options_indentation(&Uri::from_static("/"), &headers).unwrap_or(0));
        auth.account_user(peer, headers, true).await?;
        match method {
            Method::GET => event_data(&auth, raw)
                .await
                .map(|data| Json(data).into_response())
                .map_err(|_| event_error()),
            Method::OPTIONS => utility_options(
                "Get System Events",
                SYSTEM_EVENTS_DESCRIPTION,
                indentation.unwrap_or_default(),
            ),
            _ => Err(StoreError::StatusBody(
                405,
                json!({"detail": format!("Method \"{method}\" not allowed.")}),
            )),
        }
    }
    .await;
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    // A pagination/database error is a handled JSON response in Original.
    // An uncaught HTML server response keeps its framework headers.
    if response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|content_type| content_type == "application/json")
    {
        response
            .headers_mut()
            .insert(header::ALLOW, SYSTEM_EVENTS_ALLOW);
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
    }
    response
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/core/timezones/",
            get(timezones).options(timezone_options),
        )
        .route("/api/core/system-events/", get(events).fallback(events))
        .with_state(auth.clone())
        .merge(persisted_logs::router(auth))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_zones_are_sorted_grouped_and_complete() {
        let data = timezone_data();
        let zones = data["timezones"].as_array().unwrap();
        assert_eq!(zones.len(), 433);
        assert!(zones.windows(2).all(|w| w[0].as_str() < w[1].as_str()));
        assert_eq!(
            data["grouped"]
                .as_object()
                .unwrap()
                .values()
                .map(|v| v.as_array().unwrap().len())
                .sum::<usize>(),
            433
        );
        assert!(data["grouped"]["Other"]
            .as_array()
            .unwrap()
            .contains(&json!("UTC")));
    }
    #[tokio::test]
    async fn timezone_json_and_options_match_the_pinned_drf_renderer() {
        use axum::{
            body::{to_bytes, Body},
            http::{Method, Request},
        };
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;

        let data = serde_json::to_vec(timezone_data()).unwrap();
        assert_eq!(data.len(), 16_054);
        assert_eq!(
            format!("{:x}", Sha256::digest(&data)),
            "4e01d773719fbe4eaa56f514e2a02b8d054b9fed5aa63941153f82ac22eb00ea"
        );
        for (indentation, length, digest) in [
            (
                0,
                393,
                "69d37635de0de78d2d82933c5b1ed2125bf366e0dcaa3b1a57a7596b62341f73",
            ),
            (
                2,
                441,
                "181027cd4d580975606bb00aebe9b2776961be646cc7b3d977e9ac83e40a4de6",
            ),
        ] {
            let reply = channel_options_headers(
                timezone_options_entity(indentation).unwrap(),
                TIMEZONE_ALLOW,
            );
            assert_eq!(reply.status(), StatusCode::OK);
            assert_eq!(reply.headers()[header::ALLOW], "GET, HEAD, OPTIONS");
            assert_eq!(reply.headers()[header::VARY], "Accept");
            assert_eq!(reply.headers()[header::CONTENT_LENGTH], length.to_string());
            let body = to_bytes(reply.into_body(), length).await.unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(&body)), digest);
        }

        let auth = AuthService::new(
            crate::Store::from_pool(
                sqlx::postgres::PgPoolOptions::new()
                    .connect_lazy_with(sqlx::postgres::PgConnectOptions::new()),
            ),
            crate::AuthConfig::new("synthetic-timezone-component".into()).unwrap(),
        );
        let app = router(auth);
        for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
            let mut request = Request::builder()
                .method(method.clone())
                .uri("/api/core/timezones/")
                .header(header::ACCEPT, "application/json")
                .body(Body::empty())
                .unwrap();
            request
                .extensions_mut()
                .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 1234))));
            let reply = app.clone().oneshot(request).await.unwrap();
            assert_eq!(reply.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(reply.headers()[header::ALLOW], "GET, HEAD, OPTIONS");
            assert_eq!(reply.headers()[header::VARY], "Accept");
            let body = to_bytes(reply.into_body(), 4096).await.unwrap();
            assert_eq!(body.is_empty(), method == Method::HEAD);
        }
    }

    #[tokio::test]
    async fn system_event_metadata_matches_pinned_methods() {
        use axum::{
            body::{to_bytes, Body},
            extract::Request,
            Extension,
        };
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("synthetic-events-component".into()).unwrap(),
        );
        let app = router(auth).layer(Extension(ConnectInfo(SocketAddr::from((
            [127, 0, 0, 1],
            1234,
        )))));
        for method in [Method::GET, Method::HEAD, Method::OPTIONS, Method::POST] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/core/system-events/")
                        .method(method.clone())
                        .header(header::ACCEPT, "application/json")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(response.headers()[header::ALLOW], SYSTEM_EVENTS_ALLOW);
            assert_eq!(response.headers()[header::VARY], "Accept");
        }
        for (indentation, expected) in [
            (
                0,
                "a5b757e5410596a8b31646533652e99cd0c25e6e0c274ad751637b1a3d0e3f25",
            ),
            (
                4,
                "2ab8eb8e3d71f70ffe0451dfea29d326e0f868a6131a13bf303c4a1a1bd6d7c9",
            ),
        ] {
            let response =
                utility_options("Get System Events", SYSTEM_EVENTS_DESCRIPTION, indentation)
                    .unwrap();
            let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(&bytes)), expected);
        }
    }

    #[test]
    fn system_events_pagination_decodes_source_unicode_integers() {
        assert_eq!(
            page(Some("limit=%EF%BC%92&offset=%D9%A0%D9%A1".into()))
                .unwrap()
                .fetch,
            Some((2, 1))
        );
        assert_eq!(
            page(Some("limit=bad&limit=%D9%A2".into())).unwrap().fetch,
            Some((2, 0))
        );
        assert!(page(Some("limit=%D9%A2&limit=bad".into())).is_err());
        assert!(page(Some("offset=-%D9%A1".into())).is_err());
        let p = page(Some("offset=%D9%A1&limit=-%D9%A1".into())).unwrap();
        assert!(p.fetch.is_none());
        assert_eq!(p.offset, 1.into());
        assert_eq!(p.limit, (-1).into());
    }

    #[test]
    fn source_pagination_retains_empty_negative_slice() {
        let p = page(Some("offset=10&limit=-1".into())).unwrap();
        assert!(p.fetch.is_none());
        assert_eq!(p.limit, (-1).into());
        assert!(page(Some("limit=-1".into())).is_err());
        assert!(page(Some("limit=1.0".into())).is_err());
        let p = page(Some("limit=9_999&offset=+2&event_type=&limit=2000".into())).unwrap();
        assert_eq!(p.fetch, Some((1000, 2)));
        assert_eq!(p.kind, None);
    }
}
