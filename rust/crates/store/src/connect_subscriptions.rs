//! Connect subscription replacement ported from Original Dispatcharr.
//! Original Dispatcharr authors and contributors retain full application credit.
use super::{capacity, create_integration, RESPONSE_LIMIT};
use crate::{channels::truthy, Store, StoreError};
use axum::extract::Request;
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};

pub(super) const EVENTS: &[&str] = &[
    "channel_start",
    "channel_stop",
    "channel_reconnect",
    "channel_error",
    "channel_buffering",
    "channel_failover",
    "stream_switch",
    "recording_start",
    "recording_end",
    "epg_refresh",
    "epg_error",
    "m3u_refresh",
    "m3u_error",
    "client_connect",
    "client_disconnect",
    "login_failed",
    "epg_blocked",
    "m3u_blocked",
    "vod_start",
    "vod_stop",
];

fn detail(message: impl Into<String>) -> StoreError {
    StoreError::InvalidInput(json!({"detail":message.into()}))
}

fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"Integration not found"}))
}

struct Incoming {
    event: String,
    enabled: bool,
    payload_template: Option<String>,
}

#[derive(Serialize)]
struct Subscription {
    id: i64,
    event: String,
    enabled: bool,
    payload_template: Option<String>,
    integration: i64,
}

fn validated(data: Option<Value>, webhook: bool) -> Result<Vec<Incoming>, StoreError> {
    let Some(Value::Array(data)) = data else {
        return Err(detail("Expected a list of subscriptions"));
    };
    let mut estimated = 2;
    let mut incoming = Vec::new();
    for item in data {
        let Value::Object(mut item) = item else {
            return Err(detail("Each subscription must be an object"));
        };
        let event = match item.remove("event").unwrap_or(Value::Null) {
            Value::String(event) if EVENTS.contains(&event.as_str()) => event,
            Value::Array(_) | Value::Object(_) => return Err(StoreError::UnhandledRestFault),
            value => {
                let rendered = match value {
                    Value::String(text) => text,
                    Value::Null => "None".into(),
                    Value::Bool(true) => "True".into(),
                    Value::Bool(false) => "False".into(),
                    value => value.to_string(),
                };
                return Err(detail(format!("Invalid event: {rendered}")));
            }
        };
        let payload_template = if webhook {
            match item.remove("payload_template").unwrap_or(Value::Null) {
                Value::Null => None,
                Value::String(template) => Some(template),
                _ => {
                    return Err(StoreError::Unsupported(
                        "Connect webhook template must be text or null",
                    ))
                }
            }
        } else {
            None
        };
        let template_size = serde_json::to_string(&payload_template)
            .map_err(|_| StoreError::UnhandledRestFault)?
            .len();
        // Bound the entire response before any writes, including duplicate
        // snapshots. The fixed quote covers keys, scalars and array separators.
        estimated += 192 + event.len() + template_size;
        if estimated > RESPONSE_LIMIT as usize {
            return Err(capacity());
        }
        let enabled = item.get("enabled").is_none_or(|value| truthy(Some(value)));
        incoming.push(Incoming {
            event,
            enabled,
            payload_template,
        });
    }
    Ok(incoming)
}

pub(super) async fn settle<T>(
    transaction: Transaction<'_, Postgres>,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    match result {
        Ok(result) => {
            transaction.commit().await?;
            Ok(result)
        }
        Err(error) => {
            transaction.rollback().await?;
            Err(error)
        }
    }
}

async fn delete_stale(
    store: &Store,
    integration: i64,
    retained: &[&str],
) -> Result<(), StoreError> {
    let mut transaction = store.pool().begin().await?;
    let result = async {
        sqlx::query("DELETE FROM dispatcharr_connect_deliverylog d USING dispatcharr_connect_eventsubscription s WHERE d.subscription_id=s.id AND s.integration_id=$1 AND NOT (s.event=ANY($2::text[]))")
            .bind(integration).bind(retained).execute(&mut *transaction).await?;
        sqlx::query("DELETE FROM dispatcharr_connect_eventsubscription s WHERE s.integration_id=$1 AND NOT (s.event=ANY($2::text[]))")
            .bind(integration).bind(retained).execute(&mut *transaction).await?;
        Ok(())
    }.await;
    settle(transaction, result).await
}

async fn upsert(
    store: &Store,
    integration: i64,
    item: Incoming,
) -> Result<Subscription, StoreError> {
    let mut transaction = store.pool().begin().await?;
    let result = async {
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM dispatcharr_connect_eventsubscription WHERE integration_id=$1 AND event=$2 LIMIT 2 FOR UPDATE")
            .bind(integration).bind(&item.event).fetch_all(&mut *transaction).await?;
        let id = match ids.as_slice() {
            [] => sqlx::query_scalar("INSERT INTO dispatcharr_connect_eventsubscription(integration_id,event,enabled,payload_template) VALUES($1,$2,$3,$4) RETURNING id")
                .bind(integration).bind(&item.event).bind(item.enabled).bind(item.payload_template.as_deref())
                .fetch_one(&mut *transaction).await?,
            [id] => {
                sqlx::query("UPDATE dispatcharr_connect_eventsubscription SET enabled=$2,payload_template=$3 WHERE id=$1")
                    .bind(id).bind(item.enabled).bind(item.payload_template.as_deref()).execute(&mut *transaction).await?;
                *id
            }
            _ => return Err(StoreError::UnhandledRestFault),
        };
        Ok(Subscription { id, event: item.event, enabled: item.enabled, payload_template: item.payload_template, integration })
    }.await;
    settle(transaction, result).await
}

pub(super) async fn replace(
    store: &Store,
    raw: String,
    request: Request,
) -> Result<String, StoreError> {
    let id = dispatcharr_ingest_core::source_integer_literal(&raw)
        .ok_or(StoreError::UnhandledRestFault)?
        .parse::<i64>()
        .map_err(|_| missing())?;
    let kind: String =
        sqlx::query_scalar("SELECT type FROM dispatcharr_connect_integration WHERE id=$1")
            .bind(id)
            .fetch_optional(store.pool())
            .await?
            .ok_or_else(missing)?;
    let incoming = validated(
        create_integration::json_payload(request).await?,
        kind == "webhook",
    )?;
    let retained = incoming
        .iter()
        .map(|entry| entry.event.as_str())
        .collect::<Vec<_>>();
    delete_stale(store, id, &retained).await?;
    drop(retained);
    // Source commits deletion and each upsert independently. A later stored
    // duplicate preserves committed effects and returns the ordinary 500 body.
    let mut snapshots = Vec::with_capacity(incoming.len());
    for item in incoming {
        snapshots.push(upsert(store, id, item).await?);
    }
    serde_json::to_string(&snapshots).map_err(|_| StoreError::UnhandledRestFault)
}
