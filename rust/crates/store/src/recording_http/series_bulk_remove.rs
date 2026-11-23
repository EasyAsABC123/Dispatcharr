//! Bulk removal from Original Dispatcharr at 1241b3f7.
//! Original Dispatcharr, Django and DRF contributors retain behavior credit.
use super::{
    access, admit, delete_periodic_in_collector, effect, publish, settle, Owner,
    RecordingEffectError, RecordingHttp, RecordingTaskAbsent, RAW,
};
use crate::{channels::truthy, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderValue, Method},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub(super) const PATH: &str = "/api/channels/series-rules/bulk-remove/";

struct Selection {
    tvg_id: String,
    title: Option<Value>,
    source: Option<String>,
}

fn source_id(value: &Value) -> Option<String> {
    let integer = match value {
        Value::String(value) => dispatcharr_ingest_core::source_integer_literal(value)?,
        Value::Bool(true) => "1".into(),
        Value::Number(number) if number.is_i64() || number.is_u64() => number.to_string(),
        Value::Number(number) => format!("{:.0}", number.as_f64()?.trunc()),
        _ => return None,
    };
    if integer.starts_with('-') {
        return None;
    }
    let digits = integer.trim_start_matches('+').trim_start_matches('0');
    (!digits.is_empty()).then(|| digits.to_owned())
}

impl Selection {
    fn parse(body: &Value) -> Result<Self, StoreError> {
        let object = body.as_object().ok_or(StoreError::UnhandledRestFault)?;
        let tvg_id = match object.get("tvg_id").filter(|v| truthy(Some(v))) {
            None => String::new(),
            Some(Value::String(text)) => text.clone(),
            Some(Value::Bool(true)) => "True".to_owned(),
            Some(Value::Number(number)) => number.to_string(),
            Some(_) => return Err(StoreError::Unsupported("Series TVG identifier type")),
        };
        // Python str.strip also recognizes the four ASCII information separators.
        let tvg_id = tvg_id
            .trim_matches(|ch: char| ch.is_whitespace() || matches!(ch, '\u{1c}'..='\u{1f}'))
            .to_owned();
        let title = object.get("title").filter(|v| truthy(Some(v)));
        let scope = match object.get("scope").filter(|v| truthy(Some(v))) {
            None => true,
            Some(Value::String(scope)) => scope.eq_ignore_ascii_case("title"),
            Some(_) => return Err(StoreError::UnhandledRestFault),
        };
        if tvg_id.is_empty() && title.is_none() {
            return Err(StoreError::StatusBody(
                400,
                json!({"error":"tvg_id or title is required"}),
            ));
        }
        Ok(Self {
            tvg_id,
            title: title.filter(|_| scope).cloned(),
            source: object.get("epg_source_id").and_then(source_id),
        })
    }
}

async fn remove(
    connection: &mut PgConnection,
    owner: &Arc<Owner>,
    selection: Selection,
) -> Result<usize, StoreError> {
    sqlx::query("SET LOCAL statement_timeout = '5s'")
        .execute(&mut *connection)
        .await?;
    let mut query = QueryBuilder::<Postgres>::new(
        "SELECT id::bigint, task_id FROM dispatcharr_channels_recording WHERE start_time >= ",
    );
    query.push_bind(Utc::now());
    if !selection.tvg_id.is_empty() {
        query
            .push(" AND custom_properties #> '{program,tvg_id}' = ")
            .push_bind(json!(selection.tvg_id));
    }
    if let Some(title) = selection.title {
        query
            .push(" AND custom_properties #> '{program,title}' = ")
            .push_bind(title);
    }
    if let Some(source) = selection.source {
        query
            .push(" AND (custom_properties #> '{program,epg_source_id}' = to_jsonb((")
            .push_bind(source)
            .push("::text)::numeric) OR custom_properties #> '{program,epg_source_id}' IS NULL)");
    }
    let rows = query
        .push(" ORDER BY id FOR UPDATE")
        .build_query_as::<(i64, Option<String>)>()
        .fetch_all(&mut *connection)
        .await?;
    let ids: Vec<_> = rows.iter().map(|(id, _)| *id).collect();
    if ids.is_empty() {
        return Ok(0);
    }
    let deleted = sqlx::query("DELETE FROM dispatcharr_channels_recording WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&mut *connection)
        .await?;
    if deleted.rows_affected() != ids.len() as u64 {
        return Err(StoreError::UnhandledRestFault);
    }
    // Collector sends post_delete in descending primary-key order.
    for (_, task) in rows.into_iter().rev() {
        if let Some(task) = task.filter(|task| !task.is_empty()) {
            if !delete_periodic_in_collector(connection, &task).await? {
                match owner
                    .state
                    .jobs
                    .revoke_observed_absent(
                        RecordingTaskAbsent::from_collector(task),
                        owner.clone(),
                    )
                    .await
                {
                    Ok(()) | Err(RecordingEffectError::SettledFailure) => {}
                    Err(error) => return Err(effect(error)),
                }
            }
        }
    }
    Ok(ids.len())
}

async fn execute(
    owner: Arc<Owner>,
    peer: SocketAddr,
    request: Request,
) -> Result<Response, StoreError> {
    crate::epg_program_http::json_negotiation(request.headers(), request.uri())?;
    let auth = owner.state.auth.clone().with_retained_scope(owner.clone());
    let user = auth
        .account_user(peer, request.headers().clone(), false)
        .await?;
    access(&user, true)?;
    let method = request.method();
    if *method != Method::POST {
        return Err(StoreError::StatusBody(
            405,
            json!({"detail":format!("Method \"{method}\" not allowed.")}),
        ));
    }
    let media = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let raw = tokio::time::timeout(Duration::from_secs(5), to_bytes(request.into_body(), RAW))
        .await
        .map_err(|_| StoreError::StatusBody(408, json!({"detail":"Request body timed out."})))?
        .map_err(|_| StoreError::StatusBody(413, json!({"detail":"Request body too large."})))?;
    let body = if raw.is_empty() {
        json!({})
    } else {
        if media.split(';').next().map(str::trim) != Some("application/json") {
            return Err(StoreError::StatusBody(
                415,
                json!({"detail":format!("Unsupported media type \"{media}\" in request.")}),
            ));
        }
        serde_json::from_slice(&raw).map_err(|error| {
            StoreError::InvalidInput(json!({"detail":format!(
                "JSON parse error - {}", crate::source_json_error(&raw, error)
            )}))
        })?
    };
    let selection = Selection::parse(&body)?;
    let _writer = auth.task_sql_writer()?;
    let mut connection = auth.store.pool().acquire().await?;
    let result = async {
        let mut tx = connection.begin().await?;
        match remove(&mut tx, &owner, selection).await {
            Ok(removed) => {
                tx.commit().await?;
                Ok(removed)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
    .await;
    let removed = settle(connection, result).await?;
    publish(
        &owner,
        json!({"success":true,"type":"recordings_refreshed","removed":removed}),
    )
    .await;
    let response = serde_json::to_vec(&BulkRemoved {
        success: true,
        removed,
    })
    .map_err(|_| StoreError::UnhandledRestFault)?;
    super::reply_bytes(200, response, owner)
}

#[derive(serde::Serialize)]
struct BulkRemoved {
    success: bool,
    removed: usize,
}

pub(super) async fn handle(
    State(api): State<RecordingHttp>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let result = async {
        let owner = admit(&api)?;
        tokio::spawn(execute(owner, peer, request))
            .await
            .map_err(|_| StoreError::UnhandledRestFault)?
    }
    .await;
    let mut response = result.unwrap_or_else(IntoResponse::into_response);
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static("POST, OPTIONS"));
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept"));
    response
}

pub(super) async fn with_playback(
    State((api, _)): State<(RecordingHttp, crate::RecordingPlayback)>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    handle(State(api), peer, request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_original_selection_input() {
        let selected = Selection::parse(&json!({
            "tvg_id":"\u{1f} ch.x ", "title":" A Show ", "scope":"TITLE", "epg_source_id":" +٠_٧ "
        }))
        .unwrap();
        assert_eq!(selected.tvg_id, "ch.x");
        assert_eq!(selected.title, Some(json!(" A Show ")));
        assert_eq!(selected.source.as_deref(), Some("7"));

        let selected = Selection::parse(&json!({"title":"A Show", "scope":"CHANNEL"})).unwrap();
        assert!(selected.tvg_id.is_empty());
        assert_eq!(selected.title, None);
        assert_eq!(selected.source, None);

        assert!(matches!(
            Selection::parse(&json!({"tvg_id":" ", "title":null})),
            Err(StoreError::StatusBody(400, payload))
            if payload == json!({"error":"tvg_id or title is required"})
        ));
    }

    #[test]
    fn matches_original_positive_source_coercion() {
        for (input, expected) in [
            (json!(true), Some("1")),
            (json!(7.9), Some("7")),
            (json!("18446744073709551616"), Some("18446744073709551616")),
            (json!("0"), None),
            (json!("-7"), None),
            (json!("7.0"), None),
            (json!({}), None),
            (Value::Null, None),
        ] {
            assert_eq!(source_id(&input).as_deref(), expected, "{input}");
        }
    }
}
