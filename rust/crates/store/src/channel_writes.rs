//! Channel mutations following original Dispatcharr serializer and model contracts.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, boolean, character},
    channel_memberships::{integer, primary_key},
    channels, AuthService, StoreError,
};
use axum::{
    body::to_bytes,
    extract::{rejection::JsonRejection, ConnectInfo, OriginalUri, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sqlx::{PgConnection, Postgres, QueryBuilder, Row, Transaction};
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
        sqlx::query("INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,auto_sync_channel_start,auto_sync_channel_end,last_seen,is_stale) SELECT $1,id,'{}'::jsonb,true,false,NULL,NULL,now(),false FROM m3u_m3uaccount a WHERE name='custom' AND locked AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r WHERE r.channel_group_id=$1 AND r.m3u_account_id=a.id) ON CONFLICT(channel_group_id,m3u_account_id) DO NOTHING").bind(group).execute(&mut **tx).await?;
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
/// Source25 positive channel/guide signal scope. Main's existing four-writer
/// permit and shared cache/broker owners cover every physical side effect.
#[derive(Clone, Copy)]
enum GuideTarget {
    CacheOnly,
    Programme(i64),
}
fn guide_unsupported() -> StoreError {
    StoreError::Unsupported("Channel guide coupling outside Source25 positive domain")
}
async fn guide_target(
    auth: &AuthService,
    c: &mut PgConnection,
    epg: Option<i64>,
) -> Result<GuideTarget, StoreError> {
    if auth.notification_cache.is_none() {
        return Err(guide_unsupported());
    }
    // The captured channel-profile table was empty; do not silently admit a
    // different precommit membership/signal ordering in this first port.
    let profiles: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofile)")
            .fetch_one(&mut *c)
            .await?;
    if profiles {
        return Err(guide_unsupported());
    }
    let triggers: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled <> 'D' AND tgrelid IN ('dispatcharr_channels_channel'::regclass,'dispatcharr_channels_channeloverride'::regclass,'dispatcharr_channels_channelgroup'::regclass,'dispatcharr_channels_channelgroupm3uaccount'::regclass))")
        .fetch_one(&mut *c).await?;
    if triggers {
        return Err(guide_unsupported());
    }
    let Some(epg) = epg else {
        return Ok(GuideTarget::CacheOnly);
    };
    let kind: Option<(Option<String>,)> = sqlx::query_as("SELECT s.source_type FROM epg_epgdata e LEFT JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE e.id=$1").bind(epg).fetch_optional(c).await?;
    match kind {
        Some((Some(kind),)) if kind == "dummy" => Ok(GuideTarget::CacheOnly),
        Some((kind,)) if kind.as_deref().is_none_or(|v| v == "xmltv") => {
            // Main binds this sealed broker on the shared cell before serving.
            // normal_core_task_broker belongs to the private CoreSettings Auth
            // clone; channel/router clones instead prove the actual port here.
            if auth.core_task_broker.get().is_none() {
                return Err(guide_unsupported());
            }
            Ok(GuideTarget::Programme(epg))
        }
        _ => Err(guide_unsupported()),
    }
}
async fn guide_after_save(
    auth: &AuthService,
    target: GuideTarget,
    owner: crate::compact_numbering::Owner,
) -> Result<(), StoreError> {
    let cache = auth
        .notification_cache
        .as_ref()
        .ok_or_else(guide_unsupported)?
        .clone()
        .with_retained_scope(owner);
    let mut command = redis::cmd("INCR");
    command.arg("epg_content_generation");
    // Source catches cache invalidation errors, then still attempts publication.
    // Physical command ownership remains retained even after a logical error.
    let _ = cache.command::<i64>(command).await;
    if let GuideTarget::Programme(epg) = target {
        crate::core_settings_special_broker::publish(
            auth,
            &crate::core_settings_special_budget::Scratch::new(),
            "apps.epg.tasks.parse_programs_for_tvg_id",
            json!([epg]),
            json!({}),
        )
        .await?;
    }
    Ok(())
}

async fn overrides(
    auth: &AuthService,
    owner: crate::compact_numbering::Owner,
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
    let existed = old.is_some();
    let mut merged = old
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let previous = merged.get("epg_data_id").cloned().unwrap_or(Value::Null);
    for field in OVERRIDABLE {
        merged.entry(field.to_string()).or_insert(Value::Null);
    }
    merged.extend(changes.clone());
    let guide = if merged.get("epg_data_id") != Some(&previous) {
        if OVERRIDABLE.iter().all(|field| merged[*field].is_null()) {
            // Source25 cleared an EPG while retaining a name; the all-null
            // save-then-delete variant needs its own causal observation.
            return Err(guide_unsupported());
        }
        Some(guide_target(auth, tx, merged["epg_data_id"].as_i64()).await?)
    } else {
        None
    };
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
    if existed {
        let fields = cols
            .split(',')
            .filter(|col| *col != "channel_id" && *col != "created_at")
            .collect::<Vec<_>>()
            .join(",");
        let values = fields
            .split(',')
            .map(|col| format!("x.{col}"))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(&format!("UPDATE dispatcharr_channels_channeloverride SET ({fields})=(SELECT {values} FROM jsonb_populate_record(NULL::dispatcharr_channels_channeloverride,$1) x) WHERE channel_id=$2"))
            .bind(Value::Object(merged)).bind(id).execute(&mut **tx).await?;
    } else {
        // Source update_or_create reads first: an ordinary existing-row save
        // must not consume a new PostgreSQL identity on conflict.
        sqlx::query(&format!("INSERT INTO dispatcharr_channels_channeloverride({cols}) SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_channeloverride,$1) x ON CONFLICT(channel_id) DO UPDATE SET {assignments}")).bind(Value::Object(merged)).execute(&mut **tx).await?;
    }
    // Actual model post_save occurs inside the surrounding serializer atomic.
    if let Some(target) = guide {
        guide_after_save(auth, target, owner).await?;
    }
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
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer.clone())?;
    let owner = auth.core_publication_owner(writer);
    tokio::spawn(async move { create_owned(auth, peer, headers, uri, params, body, owner).await })
        .await
        .map_err(|_| StoreError::UnhandledRestFault)?
}
async fn create_owned(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    uri: Uri,
    params: HashMap<String, String>,
    body: Body,
    owner: crate::compact_numbering::Owner,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), true).await?;
    let base = channels::base_url(&headers, &uri)?;
    let body = body_object(body)?;
    let validated = validation(&auth, &body, false).await?;
    let mut data=json!({"name":"","logo_id":null,"channel_group_id":null,"tvg_id":null,"tvc_guide_stationid":null,"epg_data_id":null,"stream_profile_id":null,"uuid":Uuid::new_v4().to_string(),"user_level":0,"is_adult":false,"auto_created":false,"auto_created_by_id":null,"is_catchup":false,"catchup_days":0,"is_radio":false,"hidden_from_output":false,"created_at":now(),"updated_at":now()}).as_object().expect("object").clone();
    data.extend(validated.scalars);
    let guide_coupled = !data["epg_data_id"].is_null()
        || validated
            .overrides
            .as_ref()
            .is_some_and(|v| v.get("epg_data_id").is_some_and(|v| !v.is_null()));
    if guide_coupled
        && (body.contains_key("streams")
            || body.contains_key("channel_profile_ids")
            || body.contains_key("channel_group_id")
            || !data["auto_created_by_id"].is_null())
    {
        return Err(guide_unsupported());
    }
    let has_override = validated
        .overrides
        .as_ref()
        .is_some_and(|value| value.as_object().is_some_and(|value| !value.is_empty()));
    if has_override && data["auto_created"] != true {
        return Err(manual_override_error());
    }
    let default_number = !data.contains_key("channel_number");
    let mut connection = auth.store.pool().acquire().await?;
    let work=async {
    use sqlx::Connection;
    let mut tx = connection.begin().await?;
    let created_guide=if let Some(epg)=data["epg_data_id"].as_i64(){Some(guide_target(&auth,&mut tx,Some(epg)).await?)}else{None};
    if let Some(epg)=validated.overrides.as_ref().and_then(|v|v.get("epg_data_id")).and_then(Value::as_i64){guide_target(&auth,&mut tx,Some(epg)).await?;}
    if !data.contains_key("channel_number") {
        data.insert("channel_number".into(), next_number(&mut tx).await?);
    }
    if data["channel_group_id"].is_null() {
        data.insert("channel_group_id".into(), default_group(&mut tx).await?);
    }
    if data["auto_created"] != true && created_guide.is_none() {
        mark_group(&mut tx, &data["channel_group_id"]).await?;
    }
    let selected = COLUMNS
        .split(',')
        .map(|field| format!("x.{field}"))
        .collect::<Vec<_>>()
        .join(",");
    let id:i64=sqlx::query_scalar(&format!("INSERT INTO dispatcharr_channels_channel({COLUMNS}) SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_channel,$1) x RETURNING id")).bind(Value::Object(data.clone())).fetch_one(&mut *tx).await?;
    if let Some(target)=created_guide {
        guide_after_save(&auth,target,owner.clone()).await?;
        if data["auto_created"] != true { mark_group(&mut tx,&data["channel_group_id"]).await?; }
    }
    if let Some(stream_ids) = validated.streams {
        streams(&mut tx, id, stream_ids).await?;
    }
    if let Some(value) = validated.overrides {
        overrides(&auth, owner.clone(), &mut tx, id, &value).await?;
    }
    let profile_error = profile_memberships(&mut tx, id, body.get("channel_profile_ids")).await?;
    tx.commit().await?;
    Ok::<_,StoreError>((id,profile_error))
    }.await;
    use sqlx::Connection;
    let ready = connection.ping().await;
    let closed = connection.close().await;
    let (id, profile_error) = work?;
    ready?;
    closed?;
    if let Some(error) = profile_error {
        return Ok((StatusCode::BAD_REQUEST, Json(error)).into_response());
    }
    Ok((
        StatusCode::CREATED,
        Json(response(auth, user, params, base, id, &data, default_number).await?),
    )
        .into_response())
}

fn missing_channel() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Channel matches the given query."}))
}

async fn channel_object_query(
    auth: &AuthService,
    user: &crate::User,
    id: i64,
    params: &HashMap<String, String>,
) -> Result<QueryBuilder<'static, Postgres>, StoreError> {
    let mut query = QueryBuilder::new(channels::SELECT);
    channels::filters(&mut query, params, user, auth, true).await?;
    query.push(" AND c.id=").push_bind(id);
    Ok(query)
}

async fn object(
    auth: &AuthService,
    user: &crate::User,
    id: i64,
    params: &HashMap<String, String>,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<Value, StoreError> {
    let mut query = channel_object_query(auth, user, id, params).await?;
    let row = query
        .push(" FOR UPDATE OF c")
        .build()
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(missing_channel)?;
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
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer.clone())?;
    let owner = auth.core_publication_owner(writer);
    tokio::spawn(async move { update_owned(auth, raw, context, body, partial, owner).await })
        .await
        .map_err(|_| StoreError::UnhandledRestFault)?
}
async fn update_owned(
    auth: AuthService,
    raw: String,
    context: ChannelRequest,
    body: Body,
    partial: bool,
    owner: crate::compact_numbering::Owner,
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
    let mut connection = auth.store.pool().acquire().await?;
    let work=async {
    use sqlx::Connection;
    let mut tx=connection.begin().await?;
    let old = object(&auth, &user, id, &params, &mut tx).await?;
    let body = body_object(body)?;
    let validated = validation(&auth, &body, partial).await?;
    // Source25 measured override-only changes. A combined parent update can
    // introduce additional signal ordering, so retain that explicit boundary.
    if validated.overrides.as_ref().and_then(Value::as_object)
        .is_some_and(|v| v.contains_key("epg_data_id"))
        && (!validated.scalars.is_empty() || validated.streams.is_some()) {
        return Err(guide_unsupported());
    }
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
            if compact && auth.compact_port().is_none() {
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
        auth.compact_saved_on(&mut tx,id,&data,owner.clone()).await?;
        if data["auto_created"] != true {
            mark_group(&mut tx, &data["channel_group_id"]).await?;
        }
    }
    if let Some(value) = validated.overrides {
        overrides(&auth, owner.clone(), &mut tx, id, &value).await?;
    }
    if let Some(value) = validated.streams {
        streams(&mut tx, id, value).await?;
    }
    tx.commit().await?;
    Ok::<_,StoreError>(data)
    }.await;
    use sqlx::Connection;
    let ready = connection.ping().await;
    let closed = connection.close().await;
    ready?;
    closed?;
    let data = match work {
        Err(StoreError::Database(sqlx::Error::Database(_))) => {
            return Err(StoreError::UnhandledRestFault)
        }
        result => result?,
    };
    Ok(Json(
        response(auth, user, params, base, id, &data, false).await?,
    ))
}
fn set_epg_id(value: &Value) -> Result<Option<i64>, StoreError> {
    if value.is_null()
        || matches!(value.as_str(), Some("" | "0"))
        || crate::json_compat::python_equal(value, &json!(0))
    {
        return Ok(None);
    }
    let parsed = match value {
        Value::Bool(true) => Some(1),
        Value::String(value) => {
            let normalized = dispatcharr_ingest_core::source_integer_literal(value)
                .ok_or_else(|| invalid_epg_id(&json!(value)))?;
            normalized.parse().ok()
        }
        _ => primary_key(value).map_err(|_| invalid_epg_id(value))?,
    };
    parsed.map(Some).ok_or_else(missing_epg_data)
}

fn invalid_epg_id(value: &Value) -> StoreError {
    StoreError::StatusBody(
        400,
        json!({"error": format!("Field 'id' expected a number but got {}.", python_repr(value))}),
    )
}

fn missing_epg_data() -> StoreError {
    StoreError::StatusBody(
        400,
        json!({"error": "EPGData matching query does not exist."}),
    )
}

async fn set_epg_body(request: Request) -> Result<Option<i64>, StoreError> {
    let media = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let raw = to_bytes(request.into_body(), 64 * 1024)
        .await
        .map_err(|_| StoreError::StatusBody(413, json!({"detail": "Request body too large."})))?;
    if raw.is_empty() {
        return Ok(None);
    }
    match media.split(';').next().map(str::trim) {
        Some("application/json") => {
            let value = serde_json::from_slice::<Value>(&raw).map_err(|error| {
                StoreError::InvalidInput(json!({"detail": format!(
                    "JSON parse error - {}", crate::startup_bootstrap::json_error(&raw, error))}))
            })?;
            let fields = value.as_object().ok_or(StoreError::UnhandledRestFault)?;
            set_epg_id(fields.get("epg_data_id").unwrap_or(&Value::Null))
        }
        Some("application/x-www-form-urlencoded") => {
            let value = url::form_urlencoded::parse(&raw)
                .filter(|(key, _)| key == "epg_data_id")
                .map(|(_, value)| value.into_owned())
                .last();
            set_epg_id(&value.map_or(Value::Null, Value::String))
        }
        _ => Err(StoreError::StatusBody(
            415,
            json!({"detail": format!("Unsupported media type \"{media}\" in request.")}),
        )),
    }
}

pub(crate) async fn set_epg(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    request: Request,
) -> Result<Json<Value>, StoreError> {
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer.clone())?;
    let owner = auth.core_publication_owner(writer);
    let context = ChannelRequest {
        peer,
        headers,
        uri,
        params,
    };
    tokio::spawn(set_epg_owned(auth, raw, context, request, owner))
        .await
        .map_err(|_| StoreError::UnhandledRestFault)?
}

async fn set_epg_owned(
    auth: AuthService,
    raw: String,
    context: ChannelRequest,
    request: Request,
    owner: crate::compact_numbering::Owner,
) -> Result<Json<Value>, StoreError> {
    let ChannelRequest {
        peer,
        headers,
        uri,
        params,
    } = context;
    let user = auth.account_user(peer, headers.clone(), true).await?;
    let raw = dispatcharr_ingest_core::source_integer_literal(&raw)
        .ok_or_else(|| StoreError::StatusBody(404, json!({"detail": "Not found."})))?;
    let id = raw.parse().map_err(|_| missing_channel())?;

    // Source performs the scoped detail lookup before it accesses request.data.
    // No SQL transaction is held while waiting for the request body.
    let mut query = channel_object_query(&auth, &user, id, &params).await?;
    let old: Value = query
        .build()
        .fetch_optional(auth.store.pool())
        .await?
        .ok_or_else(missing_channel)?
        .try_get("raw")?;
    let guide_id = set_epg_body(request).await?;

    let mut tx = auth.store.pool().begin().await?;
    let saved = async {
        let guide: Option<(Option<String>, Option<String>)> = if let Some(guide_id) = guide_id {
            Some(
                sqlx::query_as(
                    "SELECT e.tvg_id, s.source_type FROM epg_epgdata e \
                 LEFT JOIN epg_epgsource s ON s.id=e.epg_source_id WHERE e.id=$1",
                )
                .bind(guide_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(missing_epg_data)?,
            )
        } else {
            None
        };
        let target = match (&guide, guide_id) {
            (Some((_, source_type)), Some(epg)) if source_type.as_deref() != Some("dummy") => {
                if auth.core_task_broker.get().is_none() {
                    return Err(guide_unsupported());
                }
                GuideTarget::Programme(epg)
            }
            _ => GuideTarget::CacheOnly,
        };
        if auth.notification_cache.is_none() {
            return Err(guide_unsupported());
        }

        // Original save(update_fields=["epg_data"]) does not touch updated_at
        // or invoke compact numbering. Its field write precedes post_save.
        let updated =
            sqlx::query("UPDATE dispatcharr_channels_channel SET epg_data_id=$1 WHERE id=$2")
                .bind(guide_id)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        if updated.rows_affected() == 0 {
            return Err(missing_channel());
        }
        Ok::<_, StoreError>((guide, target))
    }
    .await;
    let (guide, target) = match saved {
        Ok(saved) => {
            tx.commit().await?;
            saved
        }
        Err(error) => {
            tx.rollback().await?;
            return Err(error);
        }
    };

    // Source uses autocommit for the channel update. A failure in post_save
    // cannot roll the field back. The group signal follows the guide signal.
    guide_after_save(&auth, target, owner.clone()).await?;
    if old["auto_created"] != true && old["channel_group_id"].is_i64() {
        let mut tx = auth.store.pool().begin().await?;
        match mark_group(&mut tx, &old["channel_group_id"]).await {
            Ok(()) => tx.commit().await?,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        }
    }

    let name = old["name"].as_str().ok_or(StoreError::UnhandledRestFault)?;
    let Some((tvg_id, source_type)) = guide else {
        return Ok(Json(
            json!({"message": format!("EPG data removed from channel {name}")}),
        ));
    };
    let Some(source_type) = source_type else {
        // Source has already saved and queued a source-less EPGData row when
        // it accesses epg_data.epg_source.source_type for the response.
        return Err(StoreError::StatusBody(
            400,
            json!({"error": "'NoneType' object has no attribute 'source_type'"}),
        ));
    };
    let task_status = (source_type != "dummy").then_some("EPG refresh queued");
    let tvg_id = tvg_id.as_deref().unwrap_or("None");
    let mut message = format!("EPG data set to {tvg_id} for channel {name}");
    if let Some(task_status) = task_status {
        message.push_str(". ");
        message.push_str(task_status);
    }
    let old = old.as_object().ok_or(StoreError::UnhandledRestFault)?;
    let base = channels::base_url(&headers, &uri)?;
    let channel = response(auth, user, params, base, id, old, false).await?;
    Ok(Json(
        json!({"message": message, "channel": channel, "task_status": task_status}),
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
    let writer = auth.task_sql_writer()?;
    let auth = auth.with_request_writer_permit(writer.clone())?;
    let owner = auth.core_publication_owner(writer);
    let recording_jobs = auth.recording_collector_jobs()?;
    // Accepted stop/Collector/control effects retain the original HTTP writer,
    // owner and this one upgraded bridge through actual physical settlement.
    tokio::spawn(async move { delete_owned(auth, user, id, params, owner, recording_jobs).await })
        .await
        .map_err(|_| StoreError::UnhandledRestFault)?
}
async fn delete_owned(
    auth: AuthService,
    user: crate::User,
    id: i64,
    params: HashMap<String, String>,
    owner: crate::compact_numbering::Owner,
    recording_jobs: Option<std::sync::Arc<dyn crate::recording_http::RecordingJobs>>,
) -> Result<StatusCode, StoreError> {
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
    if let Some(jobs) = recording_jobs {
        // Normal Main reuses the actual same-session Collector and exact-key
        // Recording/PT/revoke effects. No second checkout inside its atomic.
        // Source default delete does NOT stop the running media worker.
        let session = auth.borrow_task_sql_session(owner.clone()).await?;
        let collected = async {
            let mut selection = auth
                .prepare_task_channel_deletion(session.clone(), jobs.clone(), owner)
                .await?;
            selection.append(vec![id]).await?;
            selection.execute_without_stops().await
        }
        .await;
        let closed = session.close().await;
        let report = collected?;
        closed?;
        if matches!(
            report.effect_outcome,
            Some(
                crate::recording_http::RecordingEffectError::Unsupported
                    | crate::recording_http::RecordingEffectError::Unsettled
            )
        ) {
            return Err(StoreError::UnhandledRestFault);
        }
        // Source's caught, physically settled revoke/stop failures still permit
        // the committed Collector204. Completed recording files stay orphaned.
        return Ok(StatusCode::NO_CONTENT);
    }
    // Legacy constructors without the normal recording port keep their previous
    // finite path/explicit guard, never synthesize a successful revoke.
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
