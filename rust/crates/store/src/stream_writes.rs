//! Stream CRUD against original Django schema and serializer/model behavior.
//! Original Dispatcharr authors and contributors retain full credit.
use crate::{
    admin_writes::{body_object, boolean, character, integer, parse_datetime},
    channel_writes::{decimal, foreign},
    channels::{base_url, stream},
    stream_validation::flexible_url,
    streams, AuthService, StoreError, User,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use chrono::{SecondsFormat, Utc};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::{collections::HashMap, net::SocketAddr};
type Body = Result<Json<Value>, JsonRejection>;
const COLUMNS:&str="name,url,m3u_account_id,logo_url,tvg_id,local_file,current_viewers,updated_at,channel_group_id,stream_profile_id,is_custom,stream_hash,last_seen,is_stale,is_adult,custom_properties,stream_id,stream_chno,stream_stats,stream_stats_updated_at,is_catchup,catchup_days,is_radio";
fn err(field: &str, message: &str) -> StoreError {
    StoreError::InvalidInput(json!({field:[message]}))
}
fn now() -> Value {
    json!(Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true))
}
fn id(value: String) -> Result<i64, StoreError> {
    value
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))
}
fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Stream matches the given query."}))
}
fn multipart(headers: &HeaderMap) -> Result<(), StoreError> {
    if headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("multipart/"))
    {
        Err(StoreError::Unsupported(
            "Stream file uploads require the original upload/storage contract",
        ))
    } else {
        Ok(())
    }
}
async fn validate(
    auth: &AuthService,
    body: Map<String, Value>,
    protected: bool,
) -> Result<Map<String, Value>, StoreError> {
    let mut values = Map::new();
    let mut errors = Map::new();
    for (field, value) in body {
        if protected
            && [
                "id",
                "name",
                "url",
                "m3u_account",
                "tvg_id",
                "channel_group",
            ]
            .contains(&field.as_str())
        {
            continue;
        }
        let parsed = match field.as_str() {
            "name" | "logo_url" | "tvg_id" | "stream_hash" => {
                Some(if value.is_null() && field != "name" {
                    Ok(Value::Null)
                } else {
                    character(
                        &field,
                        &value,
                        if field == "logo_url" {
                            None
                        } else {
                            Some(if field == "name" { 512 } else { 255 })
                        },
                        field != "name",
                    )
                    .map(Value::String)
                })
            }
            "url" => Some(if value.is_null() {
                Ok(Value::Null)
            } else {
                character(&field, &value, None, true).and_then(|value| {
                    if flexible_url(&value) {
                        Ok(json!(value))
                    } else {
                        Err(err("url", "Enter a valid URL."))
                    }
                })
            }),
            "m3u_account" => Some(foreign(auth, &field, &value, "m3u_m3uaccount", true).await),
            "channel_group" => Some(
                foreign(
                    auth,
                    &field,
                    &value,
                    "dispatcharr_channels_channelgroup",
                    true,
                )
                .await,
            ),
            "stream_profile_id" => {
                Some(foreign(auth, &field, &value, "core_streamprofile", true).await)
            }
            "current_viewers" | "catchup_days" | "stream_id" => {
                Some(if value.is_null() && field == "stream_id" {
                    Ok(Value::Null)
                } else {
                    integer(&field, &value).and_then(|value| {
                        if field != "stream_id" && value < 0 {
                            Err(err(
                                &field,
                                "Ensure this value is greater than or equal to 0.",
                            ))
                        } else {
                            Ok(json!(value))
                        }
                    })
                })
            }
            "stream_chno" => Some(decimal(&field, &value, None)),
            "is_stale" | "is_adult" | "is_custom" | "is_catchup" | "is_radio" => {
                Some(boolean(&field, &value).map(Value::Bool))
            }
            "last_seen" | "stream_stats_updated_at" => Some(
                parse_datetime(&field, &value, field != "last_seen").map(|value| {
                    value.map_or(Value::Null, |value| {
                        json!(value.to_rfc3339_opts(SecondsFormat::Micros, true))
                    })
                }),
            ),
            "local_file" => Some(if value.is_null() {
                Ok(Value::Null)
            } else {
                Err(err(
                    &field,
                    "The submitted data was not a file. Check the encoding type on the form.",
                ))
            }),
            "stream_stats" => {
                let mut value = value;
                Some(
                    crate::json_compat::normalize_input_numbers(&mut value)
                        .map(|()| value)
                        .map_err(|_| err(&field, "Value must be valid JSON.")),
                )
            }
            _ => None,
        };
        if let Some(parsed) = parsed {
            match parsed {
                Ok(value) => {
                    values.insert(
                        match field.as_str() {
                            "m3u_account" => "m3u_account_id",
                            "channel_group" => "channel_group_id",
                            _ => &field,
                        }
                        .into(),
                        value,
                    );
                }
                Err(StoreError::InvalidInput(Value::Object(fields))) => errors.extend(fields),
                Err(error) => return Err(error),
            }
        }
    }
    if errors.is_empty() {
        Ok(values)
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}
async fn custom_account(
    tx: &mut Transaction<'_, Postgres>,
    data: &mut Map<String, Value>,
) -> Result<(), StoreError> {
    if data["m3u_account_id"].is_null() {
        let id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM m3u_m3uaccount WHERE name='custom' AND locked")
                .fetch_optional(&mut **tx)
                .await?;
        let id = id.ok_or(StoreError::Unsupported("Seeded custom M3U account missing"))?;
        data.insert("m3u_account_id".into(), json!(id));
        data.insert("is_custom".into(), json!(true));
    }
    Ok(())
}
async fn unique_hash(
    tx: &mut Transaction<'_, Postgres>,
    data: &Map<String, Value>,
    except: Option<i64>,
) -> Result<(), StoreError> {
    if let Some(hash) = data["stream_hash"].as_str().filter(|hash| !hash.is_empty()) {
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_stream WHERE stream_hash=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(hash).bind(except).fetch_one(&mut **tx).await?;
        if exists {
            return Err(err(
                "stream_hash",
                "Stream with this stream hash already exists.",
            ));
        }
    }
    Ok(())
}
async fn raw(
    tx: &mut Transaction<'_, Postgres>,
    id: i64,
    params: &HashMap<String, String>,
) -> Result<Value, StoreError> {
    let mut query = QueryBuilder::new(format!("SELECT to_jsonb(s){}", streams::FROM));
    streams::filters(&mut query, params)?;
    query
        .push(" AND s.id=")
        .push_bind(id)
        .push(" FOR UPDATE OF s");
    query
        .build_query_scalar::<Value>()
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(missing)
}
async fn response(
    auth: &AuthService,
    user: &User,
    id: i64,
    base: &url::Url,
    data: &Map<String, Value>,
) -> Result<Value, StoreError> {
    let raw: Value =
        sqlx::query_scalar("SELECT to_jsonb(s) FROM dispatcharr_channels_stream s WHERE id=$1")
            .bind(id)
            .fetch_one(auth.store.pool())
            .await?;
    let mut result = stream(&json!({"raw":raw}), user.user_level >= 10, base);
    result["stream_stats"] = data["stream_stats"].clone();
    Ok(result)
}
pub(crate) async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers.clone(), true).await?;
    multipart(&headers)?;
    let base = base_url(&headers, &uri)?;
    let values = validate(&auth, body_object(body)?, false).await?;
    let mut data=json!({"name":"Default Stream","url":null,"m3u_account_id":null,"logo_url":null,"tvg_id":null,"local_file":null,"current_viewers":0,"updated_at":now(),"channel_group_id":null,"stream_profile_id":null,"is_custom":false,"stream_hash":null,"last_seen":now(),"is_stale":false,"is_adult":false,"custom_properties":{},"stream_id":null,"stream_chno":null,"stream_stats":null,"stream_stats_updated_at":null,"is_catchup":false,"catchup_days":0,"is_radio":false}).as_object().expect("stream model defaults").clone();
    data.extend(values);
    let mut tx = auth.store.pool().begin().await?;
    custom_account(&mut tx, &mut data).await?;
    unique_hash(&mut tx, &data, None).await?;
    let selected = COLUMNS
        .split(',')
        .map(|field| format!("x.{field}"))
        .collect::<Vec<_>>()
        .join(",");
    let id:i64=sqlx::query_scalar(&format!("INSERT INTO dispatcharr_channels_stream({COLUMNS}) SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_stream,$1) x RETURNING id")).bind(Value::Object(data.clone())).fetch_one(&mut *tx).await?;
    let generated =
        data["is_custom"] == true && (data["stream_hash"].is_null() || data["stream_hash"] == "");
    if generated {
        let hash = format!(
            "{:x}",
            Sha256::digest(format!("custom_stream_{id}").as_bytes())
        );
        sqlx::query("UPDATE dispatcharr_channels_stream SET stream_hash=$1 WHERE id=$2")
            .bind(hash)
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok((
        StatusCode::CREATED,
        Json(response(&auth, &user, id, &base, &data).await?),
    )
        .into_response())
}
struct Context {
    peer: SocketAddr,
    headers: HeaderMap,
    uri: Uri,
    params: HashMap<String, String>,
}
async fn update(
    auth: AuthService,
    raw_id: String,
    context: Context,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    let Context {
        peer,
        headers,
        uri,
        params,
    } = context;
    let user = auth.account_user(peer, headers.clone(), true).await?;
    multipart(&headers)?;
    let base = base_url(&headers, &uri)?;
    let id = id(raw_id)?;
    let mut tx = auth.store.pool().begin().await?;
    let old = raw(&mut tx, id, &params).await?;
    let protected = !old["m3u_account_id"].is_null() && old["is_custom"] != true;
    let values = validate(&auth, body_object(body)?, protected).await?;
    let mut data = old.as_object().expect("stream row").clone();
    data.extend(values);
    data.insert("updated_at".into(), now());
    custom_account(&mut tx, &mut data).await?;
    unique_hash(&mut tx, &data, Some(id)).await?;
    let selected = COLUMNS
        .split(',')
        .map(|field| format!("x.{field}"))
        .collect::<Vec<_>>()
        .join(",");
    sqlx::query(&format!("UPDATE dispatcharr_channels_stream SET ({COLUMNS})=(SELECT {selected} FROM jsonb_populate_record(NULL::dispatcharr_channels_stream,$1) x) WHERE id=$2")).bind(Value::Object(data.clone())).bind(id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Json(response(&auth, &user, id, &base, &data).await?))
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
        Context {
            peer,
            headers,
            uri,
            params,
        },
        body,
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
        Context {
            peer,
            headers,
            uri,
            params,
        },
        body,
    )
    .await
}
pub(crate) async fn remove(
    tx: &mut Transaction<'_, Postgres>,
    ids: &[i64],
) -> Result<(), StoreError> {
    let channels=sqlx::query_scalar::<_,i64>("SELECT DISTINCT channel_id FROM dispatcharr_channels_channelstream WHERE stream_id=ANY($1)").bind(ids).fetch_all(&mut **tx).await?;
    sqlx::query("DELETE FROM dispatcharr_channels_channelstream WHERE stream_id=ANY($1)")
        .bind(ids)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM dispatcharr_channels_stream WHERE id=ANY($1)")
        .bind(ids)
        .execute(&mut **tx)
        .await?;
    crate::channel_writes::rollup_ids(tx, &channels).await?;
    Ok(())
}
pub(crate) async fn delete(
    State(auth): State<AuthService>,
    Path(raw_id): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = id(raw_id)?;
    let mut tx = auth.store.pool().begin().await?;
    raw(&mut tx, id, &params).await?;
    remove(&mut tx, &[id]).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
pub(crate) async fn bulk_delete(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let body = body_object(body)?;
    let values = match body.get("stream_ids") {
        None => Vec::new(),
        Some(Value::Array(values)) => values.clone(),
        _ => {
            return Err(StoreError::Unsupported(
                "Non-list stream IDs require original filter coercion compatibility",
            ))
        }
    };
    let mut ids = Vec::new();
    for value in values {
        let value = match value {
            Value::Null => None,
            Value::Bool(value) => Some(i64::from(value)),
            _ => crate::channel_memberships::primary_key(&value).map_err(|_| {
                StoreError::StatusBody(500, json!({"detail":"Invalid stream IDs."}))
            })?,
        };
        if let Some(value) = value {
            ids.push(value);
        }
    }
    let mut tx = auth.store.pool().begin().await?;
    remove(&mut tx, &ids).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
