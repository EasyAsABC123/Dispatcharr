//! Profile membership writes ported from the original Dispatcharr API views.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, boolean},
    channel_profiles::lookup,
    AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::HeaderMap,
    routing::patch,
    Json, Router,
};
use num_bigint::BigInt;
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};
type Body = Result<Json<Value>, JsonRejection>;
fn error(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}
pub(crate) fn datatype(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
        Value::Number(value) => {
            if value.is_f64() {
                "float"
            } else {
                "int"
            }
        }
    }
}
fn missing_channel() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Channel matches the given query."}))
}
fn path_id(value: &str) -> Result<i64, StoreError> {
    value
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))
}

// DRF's plain IntegerField converts the decimal spelling, not bools or arbitrary
// float exponent spellings. Keep arbitrary integers for source-shaped errors.
pub(crate) fn integer(value: &Value) -> Result<Value, StoreError> {
    if value
        .as_str()
        .is_some_and(|value| value.chars().count() > 1000)
    {
        return Err(error("channel_id", "String value too large."));
    }
    if value.is_null() {
        return Err(error("channel_id", "This field may not be null."));
    }
    let text = match value {
        Value::String(value) => value.trim().to_owned(),
        Value::Number(value) if value.is_f64() => crate::json_compat::float_string(
            value
                .as_f64()
                .ok_or_else(|| error("channel_id", "A valid integer is required."))?,
        ),
        Value::Number(value) => value.to_string(),
        _ => return Err(error("channel_id", "A valid integer is required.")),
    };
    let text = text.trim();
    let text = text
        .rsplit_once('.')
        .filter(|(_, fraction)| fraction.bytes().all(|byte| byte == b'0'))
        .map_or(text, |(whole, _)| whole);
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    if unsigned.is_empty()
        || unsigned.starts_with('_')
        || unsigned.ends_with('_')
        || unsigned.contains("__")
    {
        return Err(error("channel_id", "A valid integer is required."));
    }
    let normalized = text.replace('_', "");
    let number = BigInt::parse_bytes(normalized.as_bytes(), 10)
        .ok_or_else(|| error("channel_id", "A valid integer is required."))?;
    serde_json::from_str(&number.to_string())
        .map_err(|_| error("channel_id", "A valid integer is required."))
}
pub(crate) fn primary_key(value: &Value) -> Result<Option<i64>, StoreError> {
    if value.is_null() || value.as_str() == Some("") {
        return Err(error("channel", "This field may not be null."));
    }
    match value {
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                return Ok(Some(value));
            }
            if number.is_f64() {
                return Ok(number
                    .as_f64()
                    .filter(|v| v.is_finite() && *v >= i64::MIN as f64 && *v < (i64::MAX as f64))
                    .map(|value| value as i64));
            }
            Ok(None)
        }
        Value::String(value) => {
            let raw = value.trim();
            let unsigned = raw.strip_prefix(['+', '-']).unwrap_or(raw);
            if !unsigned.starts_with('_')
                && !unsigned.ends_with('_')
                && !unsigned.contains("__")
                && BigInt::parse_bytes(raw.replace('_', "").as_bytes(), 10).is_some()
            {
                Ok(raw.replace('_', "").parse().ok())
            } else {
                Err(error(
                    "channel",
                    "Incorrect type. Expected pk value, received str.",
                ))
            }
        }
        other => Err(error(
            "channel",
            &format!(
                "Incorrect type. Expected pk value, received {}.",
                datatype(other)
            ),
        )),
    }
}
async fn channel_exists(auth: &AuthService, id: Option<i64>) -> Result<bool, StoreError> {
    Ok(
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel WHERE id=$1)")
            .bind(id)
            .fetch_one(auth.store.pool())
            .await?,
    )
}

async fn single(
    State(auth): State<AuthService>,
    Path((profile, channel)): Path<(String, String)>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    let profile = path_id(&profile)?;
    let channel = path_id(&channel)?;
    let mut tx = auth.store.pool().begin().await?;
    lookup(&mut tx, &user, profile).await?;
    tx.commit().await?;
    if !channel_exists(&auth, Some(channel)).await? {
        return Err(missing_channel());
    }
    // The source creates the disabled row before parsing/validating request.data.
    // This write intentionally survives a subsequent serializer validation error.
    sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) VALUES($1,$2,false) ON CONFLICT(channel_profile_id,channel_id) DO NOTHING").bind(profile).bind(channel).execute(auth.store.pool()).await?;
    let row=sqlx::query("SELECT id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_profile_id=$1 AND channel_id=$2").bind(profile).bind(channel).fetch_one(auth.store.pool()).await?;
    let member_id: i64 = row.try_get("id")?;
    let old_enabled: bool = row.try_get("enabled")?;
    let body = body_object(body)?;
    let mut errors = Map::new();
    let mut target = channel;
    let mut enabled = old_enabled;
    if let Some(value) = body.get("channel") {
        match primary_key(value) {
            Ok(id) if channel_exists(&auth, id).await? => {
                target = id.expect("existing channel has ID")
            }
            Ok(_) => {
                let display = if let Some(value) = value.as_str() {
                    value.to_owned()
                } else {
                    value.to_string()
                };
                errors.insert(
                    "channel".into(),
                    json!([format!("Invalid pk \"{display}\" - object does not exist.")]),
                );
            }
            Err(StoreError::InvalidInput(Value::Object(value))) => errors.extend(value),
            Err(error) => return Err(error),
        }
    }
    if let Some(value) = body.get("enabled") {
        match boolean("enabled", value) {
            Ok(value) => enabled = value,
            Err(StoreError::InvalidInput(Value::Object(value))) => errors.extend(value),
            Err(error) => return Err(error),
        }
    }
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    sqlx::query("UPDATE dispatcharr_channels_channelprofilemembership SET channel_id=$1,enabled=$2 WHERE id=$3").bind(target).bind(enabled).bind(member_id).execute(auth.store.pool()).await?;
    Ok(Json(json!({"channel":target,"enabled":enabled})))
}

#[derive(Clone)]
struct Update {
    raw: Value,
    id: Option<i64>,
    enabled: bool,
}
fn bulk_validation(body: Map<String, Value>) -> Result<Vec<Update>, StoreError> {
    let values = body
        .get("channels")
        .ok_or_else(|| error("channels", "This field is required."))?;
    if values.is_null() {
        return Err(error("channels", "This field may not be null."));
    }
    let values = values.as_array().ok_or_else(|| {
        error(
            "channels",
            &format!(
                "Expected a list of items but got type \"{}\".",
                datatype(values)
            ),
        )
    })?;
    if values.is_empty() {
        return Err(error("channels", "This list may not be empty."));
    }
    let mut errors = Map::new();
    let mut updates = Vec::new();
    for (index, value) in values.iter().enumerate() {
        if value.is_null() {
            errors.insert(index.to_string(), json!(["This field may not be null."]));
            continue;
        }
        let mut fields = Map::new();
        let mut id = None;
        let mut enabled = None;
        if let Some(value) = value.as_object() {
            if let Some(value) = value.get("channel_id") {
                match integer(value) {
                    Ok(value) => id = Some(value),
                    Err(StoreError::InvalidInput(Value::Object(value))) => fields.extend(value),
                    Err(error) => return Err(error),
                }
            } else {
                fields.insert("channel_id".into(), json!(["This field is required."]));
            }
            if let Some(value) = value.get("enabled") {
                match boolean("enabled", value) {
                    Ok(value) => enabled = Some(value),
                    Err(StoreError::InvalidInput(Value::Object(value))) => fields.extend(value),
                    Err(error) => return Err(error),
                }
            } else {
                fields.insert("enabled".into(), json!(["This field is required."]));
            }
        } else {
            fields.insert(
                "non_field_errors".into(),
                json!([format!(
                    "Invalid data. Expected a dictionary, but got {}.",
                    datatype(value)
                )]),
            );
        }
        if fields.is_empty() {
            let raw = id.expect("validated integer");
            updates.push(Update {
                id: raw.as_i64(),
                raw,
                enabled: enabled.expect("validated boolean"),
            });
        } else {
            errors.insert(index.to_string(), Value::Object(fields));
        }
    }
    if errors.is_empty() {
        Ok(updates)
    } else {
        Err(StoreError::InvalidInput(json!({"channels":errors})))
    }
}
async fn bulk(
    State(auth): State<AuthService>,
    Path(profile): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    let mut user = auth.account_user(peer, headers, true).await?;
    user.user_level = 10;
    let profile = path_id(&profile)?;
    let mut tx = auth.store.pool().begin().await?;
    lookup(&mut tx, &user, profile).await?;
    let updates = bulk_validation(body_object(body)?)?;
    let ids = updates
        .iter()
        .filter_map(|update| update.id)
        .collect::<Vec<_>>();
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM dispatcharr_channels_channel WHERE id=ANY($1)",
    )
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .collect::<HashSet<_>>();
    let invalid = updates
        .iter()
        .filter(|update| !update.id.is_some_and(|id| existing.contains(&id)))
        .map(|update| update.raw.clone())
        .collect::<Vec<_>>();
    if !invalid.is_empty() {
        return Err(StoreError::InvalidInput(
            json!({"error":"Some channels do not exist","invalid_channels":invalid}),
        ));
    }
    let rows=sqlx::query("SELECT id,channel_id FROM dispatcharr_channels_channelprofilemembership WHERE channel_profile_id=$1 AND channel_id=ANY($2)").bind(profile).bind(&ids).fetch_all(&mut *tx).await?;
    let memberships = rows
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get::<i64, _>("channel_id")?,
                row.try_get::<i64, _>("id")?,
            ))
        })
        .collect::<Result<HashMap<_, _>, sqlx::Error>>()?;
    let mut changes = HashMap::new();
    let mut creates = Vec::new();
    let mut count = 0;
    for update in updates {
        let channel = update.id.expect("validated existing channel");
        if let Some(id) = memberships.get(&channel) {
            changes.insert(*id, update.enabled);
            count += 1;
        } else {
            creates.push((channel, update.enabled));
        }
    }
    for (id, enabled) in changes {
        sqlx::query(
            "UPDATE dispatcharr_channels_channelprofilemembership SET enabled=$1 WHERE id=$2",
        )
        .bind(enabled)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    }
    for (channel, enabled) in &creates {
        sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) VALUES($1,$2,$3)").bind(profile).bind(channel).bind(enabled).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(Json(
        json!({"status":"success","updated":count,"created":creates.len(),"invalid_channels":[]}),
    ))
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/channels/profiles/{id}/channels/{channel}/",
            patch(single),
        )
        .route(
            "/api/channels/profiles/{id}/channels/bulk-update/",
            patch(bulk),
        )
        .with_state(auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bulk_plain_integer_field_keeps_large_values_and_rejects_boolean_ids() {
        assert_eq!(integer(&json!(" +123.000 ")).unwrap(), json!(123));
        assert_eq!(integer(&json!("1_000")).unwrap(), json!(1000));
        assert_eq!(integer(&json!(1.0)).unwrap(), json!(1));
        assert!(integer(&json!(true)).is_err());
        assert!(integer(&json!(1.5)).is_err());
        assert!(integer(&json!("1__0")).is_err());
        let big: Value = serde_json::from_str("123456789012345678901234567890").unwrap();
        assert_eq!(integer(&big).unwrap(), big);
    }
    #[test]
    fn bulk_errors_collect_nested_required_fields_and_boolean_coercions() {
        let value = json!({"channels":[{}, {"channel_id":true,"enabled":null}]});
        let Err(StoreError::InvalidInput(errors)) =
            bulk_validation(value.as_object().unwrap().clone())
        else {
            panic!("expected validation error")
        };
        assert_eq!(
            errors,
            json!({"channels":{"0":{"channel_id":["This field is required."],"enabled":["This field is required."]},"1":{"channel_id":["A valid integer is required."],"enabled":["This field may not be null."]}}})
        );
        let updates = bulk_validation(
            json!({"channels":[{"channel_id":"1.","enabled":1.0}]})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(updates[0].id, Some(1));
        assert!(updates[0].enabled);
    }
}
