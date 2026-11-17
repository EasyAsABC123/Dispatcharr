//! Source M3UAccountViewSet.update_group_settings category branch.
//! Original Dispatcharr and DRF contributors retain all application credit.
//! Live channel-group mutations remain a separate capability.
use crate::{AuthService, StoreError};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::HeaderMap,
    Json,
};
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};
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
struct Setting {
    category: i64,
    enabled: bool,
    properties: Map<String, Value>,
}
fn settings(body: Body) -> Result<Vec<Setting>, StoreError> {
    let body = crate::admin_writes::body_object(body)?;
    if body.get("group_settings").is_some_and(|v| v != &json!([])) {
        return Err(unsupported());
    }
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
    let mut admitted_ids = HashSet::new();
    for value in rows {
        let row = value.as_object().ok_or_else(unsupported)?;
        let Some(category) = row
            .get("id")
            .filter(|v| crate::xc_live_provider::truthy(v))
            .and_then(Value::as_i64)
        else {
            continue;
        };
        if category <= 0 || !admitted_ids.insert(category) {
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
    Ok(result)
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
    let mut tx = auth.store.pool().begin().await?;
    let ids: Vec<i64> = settings.iter().map(|s| s.category).collect();
    // Invalid foreign keys and duplicate-target failure responses require
    // separate Source error characterization; refuse those domains explicitly.
    let valid: Vec<i64> = sqlx::query_scalar(VALID_CATEGORIES)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
    if valid.len() != ids.len() {
        return Err(unsupported());
    }
    let rows = sqlx::query(EXISTING)
        .bind(id)
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await?;
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
    for setting in settings {
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
            .await?;
    }
    tx.commit().await?;
    Ok(Json(
        json!({"message":"Group settings updated successfully"}),
    ))
}
const VALID_CATEGORIES: &str =
    "SELECT id::bigint AS category FROM vod_vodcategory WHERE id::bigint=ANY($1)";
const EXISTING: &str = "SELECT category_id::bigint AS category,custom_properties AS properties FROM vod_m3uvodcategoryrelation WHERE m3u_account_id::bigint=$1 AND category_id::bigint=ANY($2)";
const UPSERT: &str = "INSERT INTO vod_m3uvodcategoryrelation(m3u_account_id,category_id,enabled,custom_properties,created_at,updated_at) SELECT $1::bigint,(v->>'category')::bigint,(v->>'enabled')::boolean,v->'properties',clock_timestamp(),clock_timestamp() FROM jsonb_array_elements($2) WITH ORDINALITY AS input(v,ordinal) ORDER BY input.ordinal ON CONFLICT(m3u_account_id,category_id) DO UPDATE SET enabled=EXCLUDED.enabled,custom_properties=EXCLUDED.custom_properties";
#[cfg(test)]
pub(crate) fn schema_statements() -> Vec<(&'static str, String)> {
    vec![
        ("vod_category_settings_valid", VALID_CATEGORIES.into()),
        ("vod_category_settings_existing", EXISTING.into()),
        ("vod_category_settings_upsert", UPSERT.into()),
    ]
}
#[cfg(test)]
mod tests {
    use super::*;
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
