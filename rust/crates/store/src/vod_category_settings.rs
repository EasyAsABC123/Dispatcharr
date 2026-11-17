//! PRIVATE UNQUALIFIED combined Source group/category settings draft.
//! Original Dispatcharr and DRF contributors retain all application credit.
//! Success/error/sequence behavior still requires the new combined Source batch.
use crate::{AuthService, StoreError};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::{Connection, Row};
use std::{collections::HashMap, net::SocketAddr};
type Body = Result<Json<Value>, JsonRejection>;
fn unsupported() -> StoreError {
    StoreError::Unsupported("Category settings value domain unavailable")
}
fn error(message: String) -> StoreError {
    StoreError::StatusBody(400, json!({"error":message}))
}
fn properties(value: Option<&Value>) -> Result<Map<String, Value>, &'static str> {
    let mut props = match value {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(props)) => props.clone(),
        _ => return Err("custom_properties must be an object."),
    };
    if let Some(language) = props.get("language").filter(|v| !v.is_null()) {
        let language = language
            .as_str()
            .filter(|s| s.len() == 2 && s.bytes().all(|v| v.is_ascii_alphabetic()))
            .ok_or("language must be a 2-letter ISO 639-1 code or null.")?;
        props.insert("language".into(), json!(language.to_ascii_lowercase()));
    }
    if let Some(quality) = props.get("quality").filter(|v| !v.is_null()) {
        if !quality
            .as_str()
            .is_some_and(|s| ["4K", "1080p", "720p", "480p", "SD"].contains(&s))
        {
            return Err("quality must be one of 4K, 1080p, 720p, 480p, SD or null.");
        }
    }
    Ok(props)
}
// Actual Source React12 observes PostgreSQL cardinality and deferred-FK
// errors. Format structured diagnostics like psycopg; never claim sqlx Display
// matches Python. Other SQL states/fault domains remain unqualified.
fn database_error(error: sqlx::Error) -> StoreError {
    let Some(pg) = error
        .as_database_error()
        .and_then(|db| db.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
    else {
        return StoreError::Database(error);
    };
    if !matches!(pg.code(), "21000" | "23503")
        || pg.severity() != sqlx::postgres::PgSeverity::Error
        || pg.position().is_some()
        || pg.r#where().is_some()
    {
        return StoreError::Database(error);
    }
    let Some(message) = postgres_diagnostic(pg.message(), pg.detail(), pg.hint()) else {
        return StoreError::Database(error);
    };
    error_response(format!("Failed to update group settings: {message}"))
}
fn postgres_diagnostic(message: &str, detail: Option<&str>, hint: Option<&str>) -> Option<String> {
    if [Some(message), detail, hint]
        .into_iter()
        .flatten()
        .any(|value| value.len() > 8192)
    {
        return None;
    }
    let mut result = message.to_owned();
    for (label, value) in [("DETAIL", detail), ("HINT", hint)] {
        if let Some(value) = value {
            result.push_str(&format!("\n{label}:  {value}"));
        }
    }
    (result.len() <= 8192).then_some(result)
}
fn error_response(message: String) -> StoreError {
    error(message)
}
fn profile_integer(value: &Value) -> Result<Option<i64>, StoreError> {
    match value {
        Value::Bool(value) => Ok(Some(i64::from(*value))),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                return Ok(Some(integer));
            }
            let number = number
                .as_f64()
                .filter(|v| v.is_finite() && v.abs() <= 9_007_199_254_740_991.0)
                .ok_or_else(unsupported)?;
            Ok(Some(number.trunc() as i64))
        }
        Value::String(text) => match dispatcharr_ingest_core::source_integer_literal(text) {
            None => Ok(None),
            Some(text) => text.parse::<i64>().map(Some).map_err(|_| unsupported()),
        },
        _ => Ok(None),
    }
}
fn live_properties(value: Option<&Value>) -> Result<Map<String, Value>, StoreError> {
    let mut props = match value {
        Some(Value::Object(value)) => value.clone(),
        Some(Value::String(value)) if value.len() <= 256 * 1024 => {
            match serde_json::from_str::<Value>(value) {
                Ok(Value::Object(value)) => value,
                _ => Map::new(),
            }
        }
        Some(Value::String(_)) => return Err(unsupported()),
        _ => Map::new(),
    };
    if let Some(raw) = props.get("channel_profile_ids") {
        let items = match raw {
            Value::Array(items) if items.len() <= 1000 => items.clone(),
            Value::Array(_) => return Err(unsupported()),
            Value::Null => Vec::new(),
            Value::String(value) if value.is_empty() => Vec::new(),
            value => vec![value.clone()],
        };
        let mut ids = Vec::new();
        for item in items {
            if let Some(id) = profile_integer(&item)? {
                ids.push(id);
            }
        }
        props.insert("channel_profile_ids".into(), json!(ids));
    }
    if serde_json::to_vec(&props).map_err(|_| unsupported())?.len() > 256 * 1024 {
        return Err(unsupported());
    }
    Ok(props)
}
fn live_range(row: &Map<String, Value>, key: &str) -> Result<Option<f64>, StoreError> {
    match row.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_f64()
            .filter(|n| n.is_finite())
            .map(Some)
            .ok_or_else(unsupported),
        _ => Err(unsupported()),
    }
}
fn live_settings(raw: Option<&Value>) -> Result<Vec<Value>, StoreError> {
    let empty = Vec::new();
    let rows = match raw {
        None => &empty,
        Some(Value::Array(rows)) if rows.len() <= 1000 => rows,
        _ => return Err(unsupported()),
    };
    // Source checks every range, even entries later skipped for a false ID.
    for value in rows {
        let row = value.as_object().ok_or_else(unsupported)?;
        let start = live_range(row, "auto_sync_channel_start")?;
        let end = live_range(row, "auto_sync_channel_end")?;
        let group = crate::vod_api_draft::vod_api_provider_refresh_value::text(
            row.get("channel_group").unwrap_or(&Value::Null),
        )
        .map_err(|_| unsupported())?;
        if start.is_some_and(|v| v < 1.0) || end.is_some_and(|v| v < 1.0) {
            return Err(error(format!(
                "Channel group {group}: channel range must be >= 1."
            )));
        }
        if start.zip(end).is_some_and(|(start, end)| end < start) {
            return Err(error(format!(
                "Channel group {group}: auto_sync_channel_end must be >= auto_sync_channel_start."
            )));
        }
    }
    let mut result = Vec::new();
    for value in rows {
        let row = value.as_object().ok_or_else(unsupported)?;
        let Some(raw) = row
            .get("channel_group")
            .filter(|v| crate::xc_live_provider::truthy(v))
        else {
            continue;
        };
        let group = raw.as_i64().filter(|v| *v > 0).ok_or_else(unsupported)?;
        let flag = |key: &str, default| match row.get(key) {
            None => Ok(default),
            Some(Value::Bool(value)) => Ok(*value),
            _ => Err(unsupported()),
        };
        result.push(json!({"group":group,"enabled":flag("enabled",true)?,"auto_sync":flag("auto_channel_sync",false)?,"start":live_range(row,"auto_sync_channel_start")?,"end":live_range(row,"auto_sync_channel_end")?,"properties":live_properties(row.get("custom_properties"))?}));
    }
    Ok(result)
}
const LIVE_UPSERT: &str = "INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,custom_properties,last_seen,is_stale) SELECT (v->>'group')::bigint,$1::bigint,(v->>'enabled')::boolean,(v->>'auto_sync')::boolean,(v->>'start')::double precision,(v->>'end')::double precision,v->'properties',clock_timestamp(),false FROM jsonb_array_elements($2) WITH ORDINALITY AS input(v,ordinal) ORDER BY input.ordinal ON CONFLICT(channel_group_id,m3u_account_id) DO UPDATE SET enabled=EXCLUDED.enabled,auto_channel_sync=EXCLUDED.auto_channel_sync,auto_sync_channel_start=EXCLUDED.auto_sync_channel_start,auto_sync_channel_end=EXCLUDED.auto_sync_channel_end,custom_properties=EXCLUDED.custom_properties";
struct Setting {
    category: i64,
    enabled: bool,
    properties: Map<String, Value>,
}
struct Settings {
    categories: Vec<Setting>,
    live: Vec<Value>,
}
fn settings(body: Body) -> Result<Settings, StoreError> {
    let body = crate::admin_writes::body_object(body)?;
    let empty = Vec::new();
    let rows = match body.get("category_settings") {
        None => &empty,
        Some(Value::Array(rows)) if rows.len() <= 1000 => rows,
        _ => return Err(unsupported()),
    };
    // Source validates every entry first, with the last properties value for a
    // repeated category ID used by every corresponding bulk-upsert object.
    let mut validated = HashMap::new();
    for value in rows {
        let row = value.as_object().ok_or_else(unsupported)?;
        let raw = row.get("id").unwrap_or(&Value::Null);
        let id = match raw {
            Value::Null => None,
            Value::Number(n) => Some(n.as_i64().ok_or_else(unsupported)?),
            _ => return Err(unsupported()),
        };
        let props = properties(row.get("custom_properties")).map_err(|message| {
            let id = crate::vod_api_draft::vod_api_provider_refresh_value::text(raw)
                .unwrap_or_else(|_| "None".into());
            error(format!("Category {id}: {message}"))
        })?;
        validated.insert(id, props);
    }
    let mut result = Vec::new();
    for value in rows {
        let row = value.as_object().ok_or_else(unsupported)?;
        let Some(category) = row
            .get("id")
            .filter(|v| crate::xc_live_provider::truthy(v))
            .and_then(Value::as_i64)
        else {
            continue;
        };
        if category <= 0 {
            return Err(unsupported());
        }
        let enabled = match row.get("enabled") {
            None => true,
            Some(Value::Bool(value)) => *value,
            _ => return Err(unsupported()),
        };
        result.push(Setting {
            category,
            enabled,
            properties: validated
                .get(&Some(category))
                .cloned()
                .ok_or_else(unsupported)?,
        });
    }
    // Source validates all category properties before inspecting live ranges.
    let live = live_settings(body.get("group_settings"))?;
    Ok(Settings {
        categories: result,
        live,
    })
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = crate::channel_memberships::primary_key(&Value::String(raw))
        .map_err(|_| StoreError::NotFound)?
        .ok_or(StoreError::NotFound)?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE id::bigint=$1)")
            .bind(id)
            .fetch_one(auth.store.pool())
            .await?;
    if !exists {
        return Err(StoreError::StatusBody(
            404,
            json!({"detail":"No M3UAccount matches the given query."}),
        ));
    }
    let settings = settings(body)?;
    let connection = auth.store.pool().acquire().await?;
    apply_settings_owned(connection, id, settings).await
}
async fn apply_settings_owned(
    mut connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    id: i64,
    settings: Settings,
) -> Result<Json<Value>, StoreError> {
    // Keep the actual connection owned beyond Transaction::commit(self).
    // A failed commit drops that wrapper and only queues SQLx rollback.
    let mut tx = (*connection).begin().await?;
    let ids: Vec<i64> = settings.categories.iter().map(|s| s.category).collect();
    // Source performs the live bulk upsert first, in the same transaction as
    // category reads/upserts. Do not pre-refuse duplicates or foreign keys:
    // their real sequence effects and caught errors belong to Source authority.
    let operation: Result<(), StoreError> = async {
        if !settings.live.is_empty() {
            sqlx::query(LIVE_UPSERT)
                .bind(id)
                .bind(json!(settings.live))
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        let rows = sqlx::query(EXISTING)
            .bind(id)
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(database_error)?;
        let mut existing = HashMap::<i64, Map<String, Value>>::new();
        for row in rows {
            let category: i64 = row.try_get("category")?;
            let props: Option<Value> = row.try_get("properties")?;
            let props = match props {
                None | Some(Value::Null) => Map::new(),
                Some(Value::Object(value)) => value,
                _ => return Err(unsupported()),
            };
            existing.insert(category, props);
        }
        let mut values = Vec::new();
        for setting in settings.categories {
            let mut props = existing.get(&setting.category).cloned().unwrap_or_default();
            props.extend(setting.properties);
            values.push(
                json!({"category":setting.category,"enabled":setting.enabled,"properties":props}),
            );
        }
        // A single ordered INSERT preserves Source conflict sequence allocation,
        // existing IDs/clocks for the admitted distinct, existing categories.
        if !values.is_empty() {
            sqlx::query(UPSERT)
                .bind(id)
                .bind(json!(values))
                .execute(&mut *tx)
                .await
                .map_err(database_error)?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = operation {
        #[cfg(test)]
        LIVE_OPERATION_ERRORS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Err(rollback_error) = tx.rollback().await {
            connection.close().await?;
            return Err(StoreError::Database(rollback_error));
        }
        return Err(error);
    }
    if let Err(error) = tx.commit().await {
        #[cfg(test)]
        LIVE_COMMIT_ERRORS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Transaction Drop may have queued a rollback. Execute and await an
        // explicit command on the SAME retained connection: SQLx drains queued
        // protocol work and ReadyForQuery before this query completes. Only
        // then may the mapped Source400 escape or the connection return to pool.
        if let Err(rollback_error) = sqlx::query("ROLLBACK")
            .persistent(false)
            .execute(&mut *connection)
            .await
        {
            // Never return a connection with unknown transaction state to pool.
            connection.close().await?;
            return Err(StoreError::Database(rollback_error));
        }
        return Err(database_error(error));
    }
    Ok(Json(
        json!({"message":"Group settings updated successfully"}),
    ))
}
const EXISTING: &str = "SELECT category_id::bigint AS category,custom_properties AS properties FROM vod_m3uvodcategoryrelation WHERE m3u_account_id::bigint=$1 AND category_id::bigint=ANY($2)";
const UPSERT: &str = "INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) SELECT $1::bigint,(v->>'category')::bigint,(v->>'enabled')::boolean,v->'properties',clock_timestamp(),clock_timestamp() FROM jsonb_array_elements($2) WITH ORDINALITY AS input(v,ordinal) ORDER BY input.ordinal ON CONFLICT(m3u_account_id,category_id) DO UPDATE SET enabled=EXCLUDED.enabled,custom_properties=EXCLUDED.custom_properties";
#[cfg(test)]
static LIVE_OPERATION_ERRORS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static LIVE_COMMIT_ERRORS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
pub(crate) fn physical_error_branches() -> (usize, usize) {
    use std::sync::atomic::Ordering;
    (
        LIVE_OPERATION_ERRORS.load(Ordering::SeqCst),
        LIVE_COMMIT_ERRORS.load(Ordering::SeqCst),
    )
}
#[cfg(test)]
pub(crate) async fn physical_apply_settings(
    connection: sqlx::pool::PoolConnection<sqlx::Postgres>,
    id: i64,
    body: Value,
) -> Result<Json<Value>, StoreError> {
    apply_settings_owned(connection, id, settings(Ok(Json(body)))?).await
}
#[cfg(test)]
pub(crate) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        ("vod_live_group_settings_upsert", LIVE_UPSERT.into()),
        ("vod_category_settings_existing", EXISTING.into()),
        ("vod_category_settings_upsert", UPSERT.into()),
    ]
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_pg_diagnostics_preserve_labels_spaces_and_no_trailing_newline() {
        assert_eq!(
            postgres_diagnostic("duplicate target", None, Some("distinct keys")),
            Some("duplicate target\nHINT:  distinct keys".into())
        );
        assert_eq!(
            postgres_diagnostic(
                "missing foreign key",
                Some("Key (id)=(17) is absent."),
                None
            ),
            Some("missing foreign key\nDETAIL:  Key (id)=(17) is absent.".into())
        );
        assert_eq!(
            postgres_diagnostic("plain", None, None),
            Some("plain".into())
        );
        assert!(postgres_diagnostic(&"x".repeat(8193), None, None).is_none());
    }

    #[test]
    fn source_language_validation_preserves_partial_update_and_explicit_clear() {
        assert_eq!(
            properties(Some(&json!({"language":"EN","quality":"1080p","extra":1}))).unwrap(),
            json!({"language":"en","quality":"1080p","extra":1})
                .as_object()
                .unwrap()
                .clone()
        );
        assert!(!properties(Some(&json!({"quality":"SD"})))
            .unwrap()
            .contains_key("language"));
        assert_eq!(
            properties(Some(&json!({"language":null})))
                .unwrap()
                .get("language"),
            Some(&Value::Null)
        );
        for value in [
            json!(" eng"),
            json!("e"),
            json!("éé"),
            json!(1),
            json!(false),
        ] {
            assert_eq!(
                properties(Some(&json!({"language":value}))).unwrap_err(),
                "language must be a 2-letter ISO 639-1 code or null."
            );
        }
    }
}
