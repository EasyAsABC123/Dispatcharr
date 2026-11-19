//! Canonical React channel rename operation from Original Dispatcharr.
//! Original Dispatcharr authors retain full workflow and algorithm credit.
use super::{child_deadline, StreamRegexPreview, REQUEST_BUDGET};
use crate::{channels::channel_mutation_json, StoreError};
use axum::{
    extract::{ConnectInfo, Request, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::{collections::BTreeSet, net::SocketAddr};
use tokio::time::{timeout_at, Instant};

pub(super) const PATH: &str = "/api/channels/channels/edit/bulk-regex/";
const MAX_IDS: usize = 5_000;
const MAX_PATTERN_BYTES: usize = 65_536;
const BATCH: usize = 256;

struct Rename<'a> {
    pattern: &'a str,
    replacement: &'a str,
    ignore_case: bool,
    ids: Result<Vec<i64>, StoreError>,
}

fn invalid(reason: impl Into<String>) -> StoreError {
    StoreError::InvalidInput(json!({"error": reason.into()}))
}

fn unavailable() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Channel rename capacity unavailable."}),
    )
}

fn django_integer(value: &Value) -> Result<Option<i64>, StoreError> {
    let id = match value {
        Value::Null => None,
        Value::Bool(value) => Some(i64::from(*value)),
        Value::Number(value) => value.as_i64().or_else(|| {
            if value.is_f64() {
                value
                    .as_f64()
                    .filter(|id| *id >= i64::MIN as f64 && *id < -(i64::MIN as f64))
                    .map(|id| id as i64)
            } else {
                None
            }
        }),
        Value::String(value) => dispatcharr_ingest_core::source_integer_literal(value)
            .ok_or(StoreError::UnhandledRestFault)?
            .parse()
            .ok(),
        _ => return Err(StoreError::UnhandledRestFault),
    };
    Ok(id)
}

impl<'a> Rename<'a> {
    fn parse(fields: &'a Map<String, Value>) -> Result<Self, StoreError> {
        let ids = fields
            .get("channel_ids")
            .and_then(Value::as_array)
            .filter(|ids| !ids.is_empty())
            .ok_or_else(|| invalid("channel_ids must be a non-empty list"))?;
        let pattern = fields
            .get("find")
            .and_then(Value::as_str)
            .filter(|pattern| !pattern.trim().is_empty())
            .ok_or_else(|| invalid("find (regex pattern) is required"))?;
        let replacement = match fields.get("replace") {
            None => "",
            Some(value) => value
                .as_str()
                .ok_or_else(|| invalid("replace must be a string"))?,
        };
        let ignore_case = match fields.get("flags") {
            None | Some(Value::Null | Value::Bool(false)) => false,
            Some(Value::String(flags)) => flags.contains('i'),
            Some(Value::Array(flags)) => flags.iter().any(|value| value.as_str() == Some("i")),
            Some(Value::Object(flags)) => flags.contains_key("i"),
            Some(Value::Number(value)) if value.as_f64() == Some(0.0) => false,
            _ => return Err(StoreError::UnhandledRestFault),
        };
        if ids.len() > MAX_IDS
            || pattern.len() > MAX_PATTERN_BYTES
            || replacement.len() > MAX_PATTERN_BYTES
        {
            return Err(unavailable());
        }
        let ids = ids
            .iter()
            .map(django_integer)
            .collect::<Result<BTreeSet<_>, _>>()
            .map(|ids| ids.into_iter().flatten().collect());
        Ok(Self {
            pattern,
            replacement,
            ignore_case,
            ids,
        })
    }
}

#[derive(Deserialize)]
struct RegexResult {
    error: Option<String>,
    overflow: bool,
    names: Vec<Option<String>>,
}

pub(super) async fn apply(
    State(service): State<StreamRegexPreview>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    let permit = service.auth.task_sql_writer()?;
    let auth = service.auth.clone().with_request_writer_permit(permit)?;
    auth.account_user(peer, request.headers().clone(), true)
        .await?;
    let body = channel_mutation_json(request).await?;
    let input = Rename::parse(&body)?;
    let deadline = Instant::now() + REQUEST_BUDGET;
    let child_deadline_ns = child_deadline()?;
    let owner = service.admit().map_err(|_| unavailable())?;
    let storage = auth.store.pool();
    let original = match &input.ids {
        Ok(ids) => timeout_at(
            deadline,
            sqlx::query_as::<_, (i64, String)>(
                "SELECT id, name FROM dispatcharr_channels_channel WHERE id = ANY($1) ORDER BY id",
            )
            .bind(ids)
            .fetch_all(storage),
        )
        .await
        .map_err(|_| unavailable())??,
        Err(_) => Vec::new(),
    };
    let mut ids = Vec::new();
    let mut names = Vec::new();
    // An empty collection still evaluates pattern syntax before the 404 (or
    // the pending id-coercion error) just as the Original handler does.
    let empty = original.is_empty().then_some([].as_slice());
    for rows in empty.into_iter().chain(original.chunks(BATCH)) {
        let values: Vec<_> = rows.iter().map(|(_, name)| name.as_str()).collect();
        let call = service.inner.regex.execute(json!({"kind":"channel_bulk_rename","names":values,
            "config":{"find":input.pattern,"replace":input.replacement,"ignore_case":input.ignore_case,
                      "deadline_ns":child_deadline_ns}}), owner.clone());
        let reply = timeout_at(deadline, call)
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        let result: RegexResult = serde_json::from_value(reply).map_err(|_| unavailable())?;
        if let Some(error) = result.error {
            return Err(invalid(error));
        }
        if result.overflow {
            return Err(StoreError::UnhandledRestFault);
        }
        if result.names.len() != rows.len() {
            return Err(unavailable());
        }
        for ((id, _), changed) in rows.iter().zip(result.names) {
            if let Some(name) = changed {
                ids.push(*id);
                names.push(name);
            }
        }
    }
    input.ids?;
    if original.is_empty() {
        return Err(StoreError::StatusBody(
            404,
            json!({"error":"No matching channels found for provided IDs"}),
        ));
    }
    let updated_count = ids.len();
    if updated_count > 0 {
        tokio::spawn(async move {
            let _owner = owner;
            let mut tx = timeout_at(deadline, auth.store.pool().begin())
                .await.map_err(|_| unavailable())??;
            let remaining = deadline.saturating_duration_since(Instant::now()).as_millis();
            if remaining == 0 {
                return Err(unavailable());
            }
            sqlx::query("SELECT set_config('statement_timeout', $1, true)")
                .bind(format!("{}ms", remaining))
                .execute(&mut *tx).await?;
            sqlx::query("UPDATE dispatcharr_channels_channel AS c SET name = v.name FROM unnest($1::bigint[], $2::text[]) AS v(id,name) WHERE c.id = v.id")
                .bind(ids).bind(names).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok::<_, StoreError>(())
        }).await.map_err(|_| unavailable())??;
    }
    Ok(Json(
        json!({"success": true, "updated_count": updated_count}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_owned_ids_and_defers_conversion_until_the_pattern_is_compiled() {
        let payload = json!({"channel_ids":[1,"+٠_٢",1,null,3.0,true],"find":"old","flags":"ig"});
        let rename = Rename::parse(payload.as_object().unwrap()).unwrap();
        assert_eq!(rename.ids.unwrap(), [1, 2, 3]);
        assert!(rename.ignore_case);
        assert_eq!(rename.replacement, "");
        let payload = json!({"channel_ids":["bad"],"find":"["});
        let rename = Rename::parse(payload.as_object().unwrap()).unwrap();
        assert!(rename.ids.is_err());
    }
}
