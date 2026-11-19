//! Original Dispatcharr's saved webhook IntegrationViewSet Test action.
//! The original Dispatcharr and Requests authors retain full application credit.
use crate::{Store, StoreError};
use axum::http::StatusCode;
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Value};
use sqlx::types::Json;
use std::time::Duration;

const TRANSPORT_BUDGET: Duration = Duration::from_secs(25);

fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail": "Integration not found"}))
}

fn payload() -> Value {
    let now = Utc::now();
    let precision = if now.timestamp_subsec_micros() == 0 {
        SecondsFormat::Secs
    } else {
        SecondsFormat::Micros
    };
    json!({
        "event": "test",
        "timestamp": now.to_rfc3339_opts(precision, false),
        "channel_name": "Test Channel",
        "stream_name": "Test Stream",
        "stream_url": "http://example.com/stream.m3u8",
        "channel_url": "http://example.com/stream.m3u8",
        "provider_name": "Test Provider",
        "profile_used": "Default",
        "test": true,
    })
}

pub(super) async fn test_saved(
    store: &Store,
    raw_id: &str,
) -> Result<(StatusCode, String), StoreError> {
    let id = dispatcharr_ingest_core::source_integer_literal(raw_id)
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(missing)?;
    let (kind, saved) = sqlx::query_as::<_, (String, Option<Json<Value>>)>(
        "SELECT type, CASE WHEN octet_length(config::text) <= 65536 THEN config END \
         FROM dispatcharr_connect_integration WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(store.pool())
    .await?
    .ok_or_else(missing)?;

    match kind.as_str() {
        "webhook" => {}
        "script" => return Err(StoreError::Unsupported("Connect Test script execution")),
        _ => {
            return Err(StoreError::InvalidInput(json!({
                "success": false,
                "error": format!("Unsupported integration type: {kind}"),
            })))
        }
    }

    let request_payload = payload();
    let outcome = if let Some(Json(config)) = saved {
        tokio::time::timeout(
            TRANSPORT_BUDGET,
            crate::epg_connect::send_saved_webhook(&config, request_payload.clone()),
        )
        .await
        .ok()
        .and_then(Result::ok)
    } else {
        None
    };
    // A receiver's HTTP failure is still a successful integration Test
    // transaction. Transport failures receive the 502 envelope, with a
    // bounded local message so client errors cannot expose URL credentials.
    let (status, response) = match outcome {
        Some(result) => (
            StatusCode::OK,
            json!({
                "success": result.get("success").and_then(Value::as_bool).unwrap_or(false),
                "type": "webhook",
                "request_payload": request_payload,
                "result": result,
            }),
        ),
        None => (
            StatusCode::BAD_GATEWAY,
            json!({
                "success": false,
                "type": "webhook",
                "request_payload": request_payload,
                "error": "Webhook test delivery failed",
            }),
        ),
    };
    Ok((status, response.to_string()))
}
