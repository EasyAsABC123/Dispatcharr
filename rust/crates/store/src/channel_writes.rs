//! Channel mutations following original Dispatcharr serializer and model contracts.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, boolean, character},
    channel_memberships::{integer, primary_key},
    channels, AuthService, StoreError,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder, Row, Transaction};
use std::{collections::HashMap, net::SocketAddr};
use uuid::Uuid;
type Body = Result<Json<Value>, JsonRejection>;
const COLUMNS:&str="channel_number,name,logo_id,channel_group_id,tvg_id,tvc_guide_stationid,epg_data_id,stream_profile_id,uuid,user_level,is_adult,auto_created,auto_created_by_id,is_catchup,catchup_days,is_radio,hidden_from_output,created_at,updated_at";
const OVERRIDABLE: &[&str] = &[
    "name",
    "channel_number",
    "channel_group_id",
    "logo_id",
    "tvg_id",
    "tvc_guide_stationid",
    "epg_data_id",
    "stream_profile_id",
    "is_radio",
];
fn err(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}
fn rename(error: StoreError, from: &str, to: &str) -> StoreError {
    match error {
        StoreError::InvalidInput(Value::Object(mut fields)) => {
            if let Some(value) = fields.remove(from) {
                fields.insert(to.into(), value);
            }
            StoreError::InvalidInput(Value::Object(fields))
        }
        other => other,
    }
}
fn now() -> Value {
    Value::String(Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true))
}
pub(crate) fn decimal(
    field: &str,
    value: &Value,
    minimum: Option<f64>,
) -> Result<Value, StoreError> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    let value = match value {
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::String(value) => value.trim().parse().ok(),
        Value::Number(value) => value.as_f64(),
        _ => None,
    }
    .ok_or_else(|| {
        err(
            field,
            if field == "channel_number" && minimum.is_none() {
                "Channel number must be a valid decimal number."
            } else {
                "A valid number is required."
            },
        )
    })?;
    if !value.is_finite() {
        return Err(StoreError::Unsupported(
            "Non-finite channel numbers require original renderer-error parity",
        ));
    }
    if let Some(minimum) = minimum {
        if value < minimum {
            return Err(err(
                field,
                &format!("Ensure this value is greater than or equal to {minimum}."),
            ));
        }
    }
    Ok(json!(value))
}
pub(crate) async fn foreign(
    auth: &AuthService,
    field: &str,
    value: &Value,
    table: &str,
    nullable: bool,
) -> Result<Value, StoreError> {
    if (value.is_null() || value.as_str() == Some("")) && nullable {
        return Ok(Value::Null);
    }
    let id = primary_key(value).map_err(|error| rename(error, "channel", field))?;
    let exists: bool =
        sqlx::query_scalar(&format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id=$1)"))
            .bind(id)
            .fetch_one(auth.store.pool())
            .await?;
    if !exists {
        let display = value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        return Err(err(
            field,
            &format!("Invalid pk \"{display}\" - object does not exist."),
        ));
    }
    Ok(json!(id.expect("existing foreign key")))
}
#[derive(Default)]
struct Validated {
    scalars: Map<String, Value>,
    streams: Option<Vec<i64>>,
    overrides: Option<Value>,
}
async fn validate(
    auth: &AuthService,
    body: &Map<String, Value>,
    partial: bool,
    override_only: bool,
) -> Result<Validated, StoreError> {
    let mut result = Validated::default();
    let mut errors = Map::new();
    if !partial && !body.contains_key("name") {
        errors.insert("name".into(), json!(["This field is required."]));
    }
    for (field, value) in body {
        if override_only && !OVERRIDABLE.contains(&field.as_str()) {
            continue;
        }
        let parsed = match field.as_str() {
            "name" | "tvg_id" | "tvc_guide_stationid" => {
                if value.is_null() && (override_only || field != "name") {
                    Some(Ok(Value::Null))
                } else {
                    Some(
                        character(
                            field,
                            value,
                            Some(if field == "name" { 512 } else { 255 }),
                            override_only || field != "name",
                        )
                        .map(Value::String),
                    )
                }
            }
            "channel_number" => Some(decimal(
                field,
                value,
                if override_only { Some(0.0001) } else { None },
            )),
            "user_level" | "catchup_days" => {
                let parsed = integer(value)
                    .map_err(|error| rename(error, "channel_id", field))
                    .and_then(|value| {
                        let number = value.as_i64().ok_or_else(|| {
                            err(
                                field,
                                "Ensure this value is less than or equal to 2147483647.",
                            )
                        })?;
                        let minimum = if field == "catchup_days" {
                            0
                        } else {
                            i32::MIN as i64
                        };
                        if number < minimum {
                            return Err(err(
                                field,
                                &format!(
                                    "Ensure this value is greater than or equal to {minimum}."
                                ),
                            ));
                        }
                        if number > i32::MAX as i64 {
                            return Err(err(
                                field,
                                "Ensure this value is less than or equal to 2147483647.",
                            ));
                        }
                        Ok(json!(number))
                    });
                Some(parsed)
            }
            "is_adult" | "is_catchup" | "is_radio" | "hidden_from_output" | "auto_created" => {
                Some(if override_only && value.is_null() {
                    Ok(Value::Null)
                } else {
                    boolean(field, value).map(Value::Bool)
                })
            }
            "channel_group_id" => Some(
                foreign(
                    auth,
                    field,
                    value,
                    "dispatcharr_channels_channelgroup",
                    override_only,
                )
                .await,
            ),
            "logo_id" => Some(foreign(auth, field, value, "dispatcharr_channels_logo", true).await),
            "epg_data_id" => Some(foreign(auth, field, value, "epg_epgdata", true).await),
            "stream_profile_id" => {
                Some(foreign(auth, field, value, "core_streamprofile", true).await)
            }
            "auto_created_by" => Some(foreign(auth, field, value, "m3u_m3uaccount", true).await),
            "streams" => {
                if let Some(values) = value.as_array() {
                    let mut ids = Vec::new();
                    let mut invalid = None;
                    for value in values {
                        match foreign(auth, "streams", value, "dispatcharr_channels_stream", false)
                            .await
                        {
                            Ok(value) => ids.push(value.as_i64().expect("validated stream ID")),
                            Err(error) => {
                                invalid = Some(error);
                                break;
                            }
                        }
                    }
                    if let Some(error) = invalid {
                        Some(Err(error))
                    } else {
                        result.streams = Some(ids);
                        None
                    }
                } else {
                    let message = if value.is_null() {
                        "This field may not be null.".to_owned()
                    } else {
                        format!(
                            "Expected a list of items but got type \"{}\".",
                            crate::channel_memberships::datatype(value)
                        )
                    };
                    Some(Err(err("streams", &message)))
                }
            }
            "override" => None,
            _ => None,
        };
        if let Some(parsed) = parsed {
            match parsed {
                Ok(value) => {
                    result.scalars.insert(
                        if field == "auto_created_by" {
                            "auto_created_by_id"
                        } else {
                            field
                        }
                        .to_string(),
                        value,
                    );
                }
                Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
                Err(error) => return Err(error),
            }
        }
    }
    if errors.is_empty() {
        Ok(result)
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}
async fn validation(
    auth: &AuthService,
    body: &Map<String, Value>,
    partial: bool,
) -> Result<Validated, StoreError> {
    let (mut result, mut errors) = match validate(auth, body, partial, false).await {
        Ok(value) => (value, Map::new()),
        Err(StoreError::InvalidInput(Value::Object(fields))) => (Validated::default(), fields),
        Err(error) => return Err(error),
    };
    if let Some(value) = body.get("override") {
        if value.is_null() {
            result.overrides = Some(Value::Null);
        } else if let Some(value) = value.as_object() {
            match validate(auth, value, true, true).await {
                Ok(value) => result.overrides = Some(Value::Object(value.scalars)),
                Err(StoreError::InvalidInput(value)) => {
                    errors.insert("override".into(), value);
                }
                Err(error) => return Err(error),
            }
        } else {
            errors.insert("override".into(),json!({"non_field_errors":[format!("Invalid data. Expected a dictionary, but got {}.",crate::channel_memberships::datatype(value))]}));
        }
    }
    if errors.is_empty() {
        Ok(result)
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}

pub(crate) async fn mark_group(
    tx: &mut Transaction<'_, Postgres>,
    group: &Value,
) -> Result<(), StoreError> {
    if let Some(group) = group.as_i64() {
        sqlx::query("INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,last_seen,is_stale) SELECT $1,id,'{}'::jsonb,true,false,NULL,NULL,now(),false FROM m3u_m3uaccount WHERE name='custom' AND locked ON CONFLICT(channel_group_id,m3u_account_id) DO NOTHING").bind(group).execute(&mut **tx).await?;
    }
    Ok(())
}
async fn default_group(tx: &mut Transaction<'_, Postgres>) -> Result<Value, StoreError> {
    let existing = sqlx::query_scalar::<_, i64>(
        "SELECT id FROM dispatcharr_channels_channelgroup WHERE name='Default Group'",
    )
    .fetch_optional(&mut **tx)
    .await?;
    let id = if let Some(id) = existing {
        id
    } else {
        match sqlx::query_scalar::<_,i64>("INSERT INTO dispatcharr_channels_channelgroup(name) VALUES('Default Group') ON CONFLICT(name) DO NOTHING RETURNING id").fetch_optional(&mut **tx).await?{
            Some(id)=>id,None=>sqlx::query_scalar::<_,i64>("SELECT id FROM dispatcharr_channels_channelgroup WHERE name='Default Group'").fetch_one(&mut **tx).await?,
        }
    };
    mark_group(tx, &json!(id)).await?;
    Ok(json!(id))
}
async fn next_number(tx: &mut Transaction<'_, Postgres>) -> Result<Value, StoreError> {
    let number:i64=sqlx::query_scalar("WITH reserved AS MATERIALIZED(SELECT channel_number FROM dispatcharr_channels_channel WHERE channel_number IS NOT NULL UNION SELECT channel_number FROM dispatcharr_channels_channeloverride WHERE channel_number IS NOT NULL) SELECT min(n) FROM generate_series(1,(SELECT count(*)+1 FROM reserved)) n LEFT JOIN reserved r ON r.channel_number=n WHERE r.channel_number IS NULL").fetch_one(&mut **tx).await?;
    Ok(json!(number))
}
fn manual_override_error() -> StoreError {
    StoreError::InvalidInput(
        json!({"override":"Cannot set override on a manual channel; overrides only apply to auto-created channels."}),
    )
}
async fn overrides(
    tx: &mut Transaction<'_, Postgres>,
    id: i64,
    value: &Value,
) -> Result<(), StoreError> {
    if value.is_null() {
        sqlx::query("DELETE FROM dispatcharr_channels_channeloverride WHERE channel_id=$1")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        return Ok(());
    }
    let changes = value.as_object().expect("validated override object");
    if changes.is_empty() {
        return Ok(());
    }
    let old: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(o) FROM dispatcharr_channels_channeloverride o WHERE channel_id=$1",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let mut merged = old
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let previous = merged.get("epg_data_id").cloned().unwrap_or(Value::Null);
    for field in OVERRIDABLE {
        merged.entry(field.to_string()).or_insert(Value::Null);
    }
    merged.extend(changes.clone());
    if merged.get("epg_data_id") != Some(&previous) {
        return Err(StoreError::Unsupported(
            "EPG override cache invalidation and programme import jobs are not migrated",
        ));
    }
    if OVERRIDABLE.iter().all(|field| merged[*field].is_null()) {
        sqlx::query("DELETE FROM dispatcharr_channels_channeloverride WHERE channel_id=$1")
            .bind(id)
            .execute(&mut **tx)
            .await?;
        return Ok(());
    }
    let stamp = now();
    merged
        .entry("created_at".to_string())
        .or_insert(stamp.clone());
    merged.insert("updated_at".into(), stamp);
    merged.insert("channel_id".into(), json!(id));
    let cols="channel_id,name,channel_number,channel_group_id,logo_id,tvg_id,tvc_guide_stationid,epg_data_id,stream_profile_id,is_radio,created_at,updated_at";
    let selected = cols
        .split(',')
        .map(|col| format!("x.{col}"))
        .collect::<Vec<_>>()
        .join(",");
    let assignments = cols
        .split(',')
        .filter(|col| *col != "channel_id" && *col != "created_at")
        .map(|col| format!("{col}=EXCLUDED.{col}"))
        .collect::<Vec<_>>()
        .join(",");
    sqlx::query(&format!("INSERT INTO dispatcharr_channels_channeloverride({cols}) SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_channeloverride,$1) x ON CONFLICT(channel_id) DO UPDATE SET {assignments}")).bind(Value::Object(merged)).execute(&mut **tx).await?;
    Ok(())
}
pub(crate) async fn rollup_ids(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[i64],
) -> Result<(), StoreError> {
    sqlx::query("UPDATE dispatcharr_channels_channel c SET is_catchup=EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active),catchup_days=COALESCE((SELECT max(s.catchup_days) FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id WHERE cs.channel_id=c.id AND s.is_catchup AND a.is_active),0) WHERE c.id=ANY($1)").bind(ids).execute(&mut **tx).await?;
    Ok(())
}
async fn rollup(tx: &mut Transaction<'_, Postgres>, id: i64) -> Result<(), StoreError> {
    rollup_ids(tx, &[id]).await
}
async fn streams(
    tx: &mut Transaction<'_, Postgres>,
    id: i64,
    streams: Vec<i64>,
) -> Result<(), StoreError> {
    let current = sqlx::query(
        "SELECT id,stream_id FROM dispatcharr_channels_channelstream WHERE channel_id=$1",
    )
    .bind(id)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|row| {
        Ok((
            row.try_get::<i64, _>("stream_id")?,
            row.try_get::<i64, _>("id")?,
        ))
    })
    .collect::<Result<HashMap<_, _>, sqlx::Error>>()?;
    let removed=sqlx::query("DELETE FROM dispatcharr_channels_channelstream WHERE channel_id=$1 AND NOT(stream_id=ANY($2))").bind(id).bind(&streams).execute(&mut **tx).await?.rows_affected();
    let mut changed = removed > 0;
    for (index, stream) in streams.into_iter().enumerate() {
        if let Some(link) = current.get(&stream) {
            sqlx::query("UPDATE dispatcharr_channels_channelstream SET \"order\"=$1 WHERE id=$2")
                .bind(index as i32)
                .bind(link)
                .execute(&mut **tx)
                .await?;
        } else {
            sqlx::query("INSERT INTO dispatcharr_channels_channelstream(channel_id,stream_id,\"order\") VALUES($1,$2,$3)").bind(id).bind(stream).bind(index as i32).execute(&mut **tx).await?;
            changed = true;
        }
    }
    if changed {
        rollup(tx, id).await?;
    }
    Ok(())
}
async fn profile_memberships(
    tx: &mut Transaction<'_, Postgres>,
    id: i64,
    value: Option<&Value>,
) -> Result<Option<Value>, StoreError> {
    let values = match value {
        None | Some(Value::Null) => None,
        Some(Value::Array(values)) => Some(values.clone()),
        Some(value) => Some(vec![value.clone()]),
    };
    let all = values.as_ref().is_none_or(|values| {
        values
            .iter()
            .any(|value| crate::json_compat::python_equal(value, &json!(0)))
    });
    let profiles = if all {
        sqlx::query_scalar::<_, i64>("SELECT id FROM dispatcharr_channels_channelprofile")
            .fetch_all(&mut **tx)
            .await?
    } else {
        let values = values.expect("explicit IDs");
        let mut ids = Vec::new();
        for value in &values {
            let parsed = match value {
                Value::Null => Ok(None),
                Value::Bool(value) => Ok(Some(i64::from(*value))),
                _ => primary_key(value),
            };
            match parsed {
                Ok(Some(id)) => ids.push(id),
                Ok(None) => {}
                Err(_) => {
                    return Ok(Some(
                        json!({"error":format!("Error creating profile memberships: Field 'id' expected a number but got {}.",python_repr(value))}),
                    ))
                }
            }
        }
        let profiles = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM dispatcharr_channels_channelprofile WHERE id=ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&mut **tx)
        .await?;
        if profiles.len() != values.len() {
            let mut missing = Vec::new();
            let mut seen: Vec<&Value> = Vec::new();
            for value in &values {
                if !profiles
                    .iter()
                    .any(|id| crate::json_compat::python_equal(value, &json!(id)))
                    && !seen
                        .iter()
                        .any(|other| crate::json_compat::python_equal(value, other))
                {
                    seen.push(value);
                    missing.push(python_repr(value));
                }
            }
            return Ok(Some(
                json!({"error":format!("Channel profiles with IDs [{}] not found",missing.join(", "))}),
            ));
        }
        profiles
    };
    for profile in profiles {
        sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) VALUES($1,$2,true)").bind(profile).bind(id).execute(&mut **tx).await?;
    }
    Ok(None)
}
fn python_repr(value: &Value) -> String {
    match value {
        Value::String(value) => format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'")),
        Value::Null => "None".into(),
        Value::Bool(value) => {
            if *value {
                "True".into()
            } else {
                "False".into()
            }
        }
        other => other.to_string(),
    }
}
async fn response(
    auth: AuthService,
    user: crate::User,
    params: HashMap<String, String>,
    base: url::Url,
    id: i64,
    scalars: &Map<String, Value>,
    default_number: bool,
) -> Result<Value, StoreError> {
    let params = params
        .into_iter()
        .filter(|(key, _)| key == "include_streams")
        .collect();
    let Json(mut result) = channels::read(auth, user, params, base, Some(id)).await?;
    for key in ["is_catchup", "catchup_days"] {
        if let Some(value) = scalars.get(key) {
            result[key] = value.clone();
        }
    }
    if default_number && result["override"]["channel_number"].is_null() {
        result["effective_channel_number"] = scalars["channel_number"].clone();
    }
    Ok(result)
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), true).await?;
    let base = channels::base_url(&headers, &uri)?;
    let body = body_object(body)?;
    let validated = validation(&auth, &body, false).await?;
    let mut data=json!({"name":"","logo_id":null,"channel_group_id":null,"tvg_id":null,"tvc_guide_stationid":null,"epg_data_id":null,"stream_profile_id":null,"uuid":Uuid::new_v4().to_string(),"user_level":0,"is_adult":false,"auto_created":false,"auto_created_by_id":null,"is_catchup":false,"catchup_days":0,"is_radio":false,"hidden_from_output":false,"created_at":now(),"updated_at":now()}).as_object().expect("object").clone();
    data.extend(validated.scalars);
    if !data["epg_data_id"].is_null() {
        return Err(StoreError::Unsupported(
            "EPG channel creation cache invalidation and programme import jobs are not migrated",
        ));
    }
    let has_override = validated
        .overrides
        .as_ref()
        .is_some_and(|value| value.as_object().is_some_and(|value| !value.is_empty()));
    if has_override && data["auto_created"] != true {
        return Err(manual_override_error());
    }
    let default_number = !data.contains_key("channel_number");
    let mut tx = auth.store.pool().begin().await?;
    if !data.contains_key("channel_number") {
        data.insert("channel_number".into(), next_number(&mut tx).await?);
    }
    if data["channel_group_id"].is_null() {
        data.insert("channel_group_id".into(), default_group(&mut tx).await?);
    }
    if data["auto_created"] != true {
        mark_group(&mut tx, &data["channel_group_id"]).await?;
    }
    let selected = COLUMNS
        .split(',')
        .map(|field| format!("x.{field}"))
        .collect::<Vec<_>>()
        .join(",");
    let id:i64=sqlx::query_scalar(&format!("INSERT INTO dispatcharr_channels_channel({COLUMNS}) SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_channel,$1) x RETURNING id")).bind(Value::Object(data.clone())).fetch_one(&mut *tx).await?;
    if let Some(stream_ids) = validated.streams {
        streams(&mut tx, id, stream_ids).await?;
    }
    if let Some(value) = validated.overrides {
        overrides(&mut tx, id, &value).await?;
    }
    let profile_error = profile_memberships(&mut tx, id, body.get("channel_profile_ids")).await?;
    tx.commit().await?;
    if let Some(error) = profile_error {
        return Ok((StatusCode::BAD_REQUEST, Json(error)).into_response());
    }
    Ok((
        StatusCode::CREATED,
        Json(response(auth, user, params, base, id, &data, default_number).await?),
    )
        .into_response())
}

async fn object(
    auth: &AuthService,
    user: &crate::User,
    id: i64,
    params: &HashMap<String, String>,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Value, StoreError> {
    let mut query = QueryBuilder::new(channels::SELECT);
    channels::filters(&mut query, params, user, auth, true).await?;
    query
        .push(" AND c.id=")
        .push_bind(id)
        .push(" FOR UPDATE OF c");
    let row = query
        .build()
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| {
            StoreError::StatusBody(404, json!({"detail":"No Channel matches the given query."}))
        })?;
    Ok(row.try_get("raw")?)
}
struct ChannelRequest {
    peer: SocketAddr,
    headers: HeaderMap,
    uri: Uri,
    params: HashMap<String, String>,
}
async fn update(
    auth: AuthService,
    raw: String,
    context: ChannelRequest,
    body: Body,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    let ChannelRequest {
        peer,
        headers,
        uri,
        params,
    } = context;
    let user = auth.account_user(peer, headers.clone(), true).await?;
    let base = channels::base_url(&headers, &uri)?;
    let id = raw
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    let mut tx = auth.store.pool().begin().await?;
    let old = object(&auth, &user, id, &params, &mut tx).await?;
    let body = body_object(body)?;
    let validated = validation(&auth, &body, partial).await?;
    if validated
        .overrides
        .as_ref()
        .is_some_and(|value| value.as_object().is_some_and(|value| !value.is_empty()))
        && old["auto_created"] != true
    {
        return Err(manual_override_error());
    }
    let mut data = old.as_object().expect("database row object").clone();
    let scalar_change = !validated.scalars.is_empty();
    data.extend(validated.scalars);
    if scalar_change {
        if data["auto_created"] == true && !data["auto_created_by_id"].is_null() {
            let compact:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r WHERE r.m3u_account_id=$1 AND (r.channel_group_id=$2 OR r.custom_properties->>'group_override'=$3) AND (jsonb_typeof(r.custom_properties) NOT IN ('object','null') OR COALESCE(r.custom_properties->'compact_numbering','null'::jsonb) NOT IN ('null'::jsonb,'false'::jsonb,'0'::jsonb,'\"\"'::jsonb,'[]'::jsonb,'{}'::jsonb)))").bind(data["auto_created_by_id"].as_i64()).bind(data["channel_group_id"].as_i64()).bind(data["channel_group_id"].as_i64().map(|value|value.to_string())).fetch_one(&mut *tx).await?;
            if compact {
                return Err(StoreError::Unsupported("Compact auto-channel numbering requires the original shared numbering lock and assignment implementation"));
            }
        }
        data.insert("updated_at".into(), now());
        let selected = COLUMNS
            .split(',')
            .map(|field| format!("x.{field}"))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!("UPDATE dispatcharr_channels_channel SET ({COLUMNS})=(SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_channel,$1) x) WHERE id=$2")).bind(Value::Object(data.clone())).bind(id).execute(&mut *tx).await?;
        if data["auto_created"] != true {
            mark_group(&mut tx, &data["channel_group_id"]).await?;
        }
    }
    if let Some(value) = validated.overrides {
        overrides(&mut tx, id, &value).await?;
    }
    if let Some(value) = validated.streams {
        streams(&mut tx, id, value).await?;
    }
    tx.commit().await?;
    Ok(Json(
        response(auth, user, params, base, id, &data, false).await?,
    ))
}
pub(crate) async fn patch(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    update(
        auth,
        raw,
        ChannelRequest {
            peer,
            headers,
            uri,
            params,
        },
        body,
        true,
    )
    .await
}
pub(crate) async fn put(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    update(
        auth,
        raw,
        ChannelRequest {
            peer,
            headers,
            uri,
            params,
        },
        body,
        false,
    )
    .await
}
pub(crate) async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = raw
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    let mut lookup_tx = auth.store.pool().begin().await?;
    let original = object(&auth, &user, id, &params, &mut lookup_tx).await?;
    lookup_tx.commit().await?;
    if params.get("stop_stream").is_some_and(|value| {
        ["1", "true", "yes", "on"].contains(&value.trim().to_lowercase().as_str())
    }) {
        let control = auth
            .channel_control
            .as_ref()
            .ok_or(StoreError::Unsupported(
                "Stopping playback before channel deletion requires the engine control callback",
            ))?;
        let uuid = original["uuid"].as_str().ok_or(StoreError::Unsupported(
            "Channel UUID unavailable for playback stop",
        ))?;
        if let Err(error) = control.stop_channel(uuid.to_owned()).await {
            // Original ChannelService.stop_channels catches every stop error,
            // including inactive sessions, then proceeds with the DB deletion.
            tracing::warn!(
                status = error.status,
                "Best-effort channel stop failed; continuing requested channel deletion"
            );
        }
    }
    let mut tx = auth.store.pool().begin().await?;
    object(&auth, &user, id, &params, &mut tx).await?;
    let recordings: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_recording WHERE channel_id=$1)",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if recordings {
        return Err(StoreError::Unsupported(
            "Deleting recording-owned channels requires DVR job revocation and schedule cleanup",
        ));
    }
    for query in [
        "DELETE FROM dispatcharr_channels_channelstream WHERE channel_id=$1",
        "DELETE FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=$1",
        "DELETE FROM dispatcharr_channels_channeloverride WHERE channel_id=$1",
        "DELETE FROM dispatcharr_channels_recurringrecordingrule WHERE channel_id=$1",
        "DELETE FROM dispatcharr_channels_channel WHERE id=$1",
    ] {
        sqlx::query(query).bind(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
