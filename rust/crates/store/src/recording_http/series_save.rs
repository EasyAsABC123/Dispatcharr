//! Signed Series-rule upsert from Original Dispatcharr 1241b3f7.
//! Original Dispatcharr, Django and DRF authors retain the behavior credit.
use super::{access, admit, reply, settle, Owner, RecordingHttp, RAW, RESPONSE};
use crate::{channels::truthy, RecordingPlayback, StoreError};
use axum::{
    body::to_bytes,
    extract::{ConnectInfo, Request, State},
    http::{header, HeaderValue, Method},
    response::{IntoResponse, Response},
};
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection};
use std::{net::SocketAddr, sync::Arc, time::Duration};

pub(crate) const PATH: &str = "/api/channels/series-rules/";

fn invalid(message: &str) -> StoreError {
    StoreError::StatusBody(400, json!({"error":message}))
}

fn stripped(value: &str) -> &str {
    value.trim_matches(|ch: char| ch.is_whitespace() || matches!(ch, '\u{1c}'..='\u{1f}'))
}

fn text<'a>(body: &'a Value, key: &str, default: &'a str) -> Result<&'a str, StoreError> {
    body.get(key)
        .filter(|value| truthy(Some(value)))
        .map_or(Ok(default), |value| {
            value.as_str().ok_or(StoreError::UnhandledRestFault)
        })
}

fn tvg(value: Option<&Value>) -> Result<String, StoreError> {
    let value = match value.filter(|value| truthy(Some(value))) {
        None => String::new(),
        Some(Value::String(value)) => value.to_owned(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(true)) => "True".into(),
        _ => return Err(StoreError::Unsupported("Series TVG identifier type")),
    };
    Ok(value)
}

fn source(value: Option<&Value>) -> Result<Option<i64>, StoreError> {
    Ok(super::python_int(value)?.filter(|id| *id > 0))
}

fn parse(body: &Value) -> Result<Value, StoreError> {
    if truthy(Some(body)) && !body.is_object() {
        return Err(StoreError::UnhandledRestFault);
    }
    let tvg_id = tvg(body.get("tvg_id"))?;
    let mode = text(body, "mode", "all")?.to_lowercase();
    let title = text(body, "title", "")?;
    let title_mode = text(body, "title_mode", "exact")?.to_lowercase();
    let description = text(body, "description", "")?;
    let description_mode = text(body, "description_mode", "contains")?.to_lowercase();
    let epg_source_id = source(body.get("epg_source_id"))?;

    if !matches!(mode.as_str(), "all" | "new") {
        return Err(invalid("mode must be 'all' or 'new'"));
    }
    if !matches!(
        title_mode.as_str(),
        "exact" | "contains" | "search" | "regex"
    ) {
        return Err(invalid(
            "title_mode must be one of exact, contains, search, regex",
        ));
    }
    if !matches!(description_mode.as_str(), "contains" | "search" | "regex") {
        return Err(invalid(
            "description_mode must be one of contains, search, regex",
        ));
    }
    if stripped(title).is_empty() && stripped(description).is_empty() {
        return Err(invalid("A title or description is required"));
    }
    let mut rule = json!({
        "tvg_id": stripped(&tvg_id),
        "mode":mode,
        "title":title,
        "title_mode":title_mode,
        "description":description,
        "description_mode":description_mode,
    });
    if mode == "new" && truthy(body.get("untagged_is_new")) {
        rule["untagged_is_new"] = json!(true);
    }
    if let Some(value) = body
        .get("channel_id")
        .filter(|v| !v.is_null() && v.as_str() != Some(""))
    {
        rule["channel_id"] = json!(super::python_int(Some(value))?
            .ok_or_else(|| invalid("channel_id must be an integer"))?);
    }
    if let Some(id) = epg_source_id {
        rule["epg_source_id"] = json!(id);
    }
    Ok(rule)
}

fn upsert(value: &mut Value, incoming: Value) -> Result<Value, StoreError> {
    let mut rules = value["series_rules"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|rule| rule.is_object())
        .cloned()
        .collect::<Vec<_>>();
    let incoming_source = source(incoming.get("epg_source_id"))?;
    let mut exact = None;
    let mut legacy = None;
    for (index, rule) in rules.iter().enumerate() {
        if incoming["tvg_id"].as_str() != Some(tvg(rule.get("tvg_id"))?.as_str())
            || incoming["title"].as_str() != Some(tvg(rule.get("title"))?.as_str())
        {
            continue;
        }
        match source(rule.get("epg_source_id"))? {
            id if id == incoming_source => {
                exact = Some(index);
                break;
            }
            None => {
                legacy.get_or_insert(index);
            }
            Some(_) => {}
        }
    }
    if let Some(index) = exact.or(legacy) {
        rules[index] = incoming;
    } else {
        rules.push(incoming);
    }
    if !value.is_object() {
        *value = json!({});
    }
    value["series_rules"] = json!(rules);
    Ok(json!({"success":true,"rules":rules}))
}

async fn lock_dvr_group(connection: &mut PgConnection) -> Result<Option<Value>, StoreError> {
    let row = sqlx::query_as::<_, (bool, Option<Value>)>(
        "SELECT value IS NULL,
                CASE WHEN octet_length(value::text)<=$1 THEN value END
         FROM core_coresettings WHERE key='dvr_settings' FOR UPDATE",
    )
    .bind(RESPONSE as i32)
    .fetch_optional(connection)
    .await?;
    row.map(|(null, value)| {
        if null {
            Ok(Value::Null)
        } else {
            value.ok_or(StoreError::Unsupported("DVR settings response budget"))
        }
    })
    .transpose()
}

async fn save(connection: &mut PgConnection, rule: Value) -> Result<Value, StoreError> {
    sqlx::query("SET LOCAL statement_timeout = '5s'")
        .execute(&mut *connection)
        .await?;
    if let Some(id) = rule.get("channel_id").and_then(Value::as_i64) {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel WHERE id::bigint=$1)",
        )
        .bind(id)
        .fetch_one(&mut *connection)
        .await?;
        if !exists {
            return Err(invalid("channel_id does not exist"));
        }
    }
    let mut existing = lock_dvr_group(connection).await?;
    if existing.is_none() {
        // Original get_or_create does not advance the sequence for an existing
        // row. A concurrent first creator can win the insert and we then use
        // its locked settings, preserving unrelated keys.
        sqlx::query("INSERT INTO core_coresettings (key,name,value) VALUES ('dvr_settings','DVR Settings','{}') ON CONFLICT (key) DO NOTHING")
            .execute(&mut *connection)
            .await?;
        existing = lock_dvr_group(connection).await?;
    }
    let mut group = existing.ok_or(StoreError::UnhandledRestFault)?;
    let response = upsert(&mut group, rule)?;
    if serde_json::to_vec(&group)
        .map_err(|_| StoreError::UnhandledRestFault)?
        .len()
        > RESPONSE
    {
        return Err(StoreError::Unsupported("DVR settings response budget"));
    }
    sqlx::query("UPDATE core_coresettings SET value=$1 WHERE key='dvr_settings'")
        .bind(group)
        .execute(connection)
        .await?;
    Ok(response)
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
            StoreError::InvalidInput(json!({
                "detail":format!("JSON parse error - {}",crate::source_json_error(&raw,error))
            }))
        })?
    };
    let rule = parse(&body)?;
    if auth.notification_cache.is_none() {
        return Err(StoreError::Unsupported("CoreSettings cache not configured"));
    }
    let _writer = auth.task_sql_writer()?;
    let mut connection = auth.store.pool().acquire().await?;
    let result = async {
        let mut tx = connection.begin().await?;
        let result = save(&mut tx, rule).await;
        if result.is_ok() {
            tx.commit().await?;
        } else {
            tx.rollback().await?;
        }
        result
    }
    .await;
    let response = settle(connection, result).await?;
    crate::core_settings_writes::invalidate(&auth, "dvr_settings").await?;
    reply(200, response, owner)
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
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::from_static("GET, POST, DELETE, HEAD, OPTIONS"),
    );
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept"));
    response
}

pub(super) async fn with_playback(
    State((api, playback)): State<(RecordingHttp, RecordingPlayback)>,
    peer: ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    match *request.method() {
        Method::POST => handle(State(api), peer, request).await,
        Method::GET => crate::recording_playback::library_read(State(playback), peer, request)
            .await
            .into_response(),
        _ => StoreError::NotFound.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_editor_persists_default_fields_and_upgrades_only_matching_legacy_rule() {
        let mut group = json!({"pre_offset_minutes":5,"series_rules":[false,
            {"tvg_id":"news","title":"Evening News","epg_source_id":4,"mode":"new"},
            {"tvg_id":"news","title":"Evening News","other":true}]});
        let record = parse(&json!({"tvg_id":" news ", "title":"Evening News",
            "channel_id":"7", "epg_source_id":"13", "untagged_is_new":true}))
        .unwrap();
        assert_eq!(
            record,
            json!({"tvg_id":"news","title":"Evening News","mode":"all",
            "title_mode":"exact","description":"","description_mode":"contains",
            "channel_id":7,"epg_source_id":13})
        );
        let result = upsert(&mut group, record.clone()).unwrap();
        assert_eq!(group["pre_offset_minutes"], 5);
        assert_eq!(result["success"], true);
        assert_eq!(group["series_rules"], result["rules"]);
        assert_eq!(result["rules"][0]["epg_source_id"], 4);
        assert_eq!(result["rules"][1], record);
        let duplicate = upsert(&mut group, record).unwrap();
        assert_eq!(duplicate, result);
    }

    #[test]
    fn source_exact_source_precedes_legacy_and_new_mode_preserves_untagged() {
        let legacy = json!({"tvg_id":"news","title":"News"});
        let mut group = json!({"series_rules":[legacy, {"tvg_id":"news","title":"News","epg_source_id":"2","mode":"all"}]});
        let rule = parse(&json!({"tvg_id":"news","title":"News","epg_source_id":2,"mode":"NEW","untagged_is_new":true})).unwrap();
        let result = upsert(&mut group, rule.clone()).unwrap();
        assert_eq!(rule["untagged_is_new"], true);
        assert_eq!(result["rules"], json!([legacy, rule]));
        assert!(matches!(
            parse(&json!({"title":" ","channel_id":"bad"})),
            Err(StoreError::StatusBody(400, _))
        ));
    }
}
