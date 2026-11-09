//! Channel-group contracts from original Dispatcharr models/serializers/views.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, character},
    channel_writes::mark_group,
    channels::{float, timestamp, truthy},
    AuthService, StoreError, User,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use std::{collections::HashMap, net::SocketAddr};
type Body = Result<Json<Value>, JsonRejection>;
const ADMIN:&str="SELECT g.id::bigint AS id,g.name,EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND a.locked) AS is_custom,count(DISTINCT rel.id) FILTER(WHERE NOT a.locked) AS provider_count FROM dispatcharr_channels_channelgroup g LEFT JOIN dispatcharr_channels_channelgroupm3uaccount rel ON rel.channel_group_id=g.id LEFT JOIN m3u_m3uaccount a ON a.id=rel.m3u_account_id WHERE ($1::bigint IS NULL OR g.id=$1) GROUP BY g.id";
const MEMBER:&str="SELECT g.id::bigint AS id,g.name,EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND a.locked) AS is_custom FROM dispatcharr_channels_channelgroup g WHERE ($1::bigint IS NULL OR g.id=$1) AND EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=g.id AND c.user_level<=$2 AND (NOT $3 OR NOT c.is_adult))";
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No ChannelGroup matches the given query."}),
    )
}
fn id(value: String) -> Result<i64, StoreError> {
    value
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))
}
fn unique() -> StoreError {
    StoreError::InvalidInput(json!({"name":["channel group with this name already exists."]}))
}
fn write_error(error: sqlx::Error) -> StoreError {
    if error
        .as_database_error()
        .is_some_and(|error| error.is_unique_violation())
    {
        unique()
    } else {
        error.into()
    }
}
async fn rows(auth: &AuthService, user: &User, id: Option<i64>) -> Result<Vec<PgRow>, StoreError> {
    if user.user_level >= 10 {
        Ok(sqlx::query(ADMIN)
            .bind(id)
            .fetch_all(auth.store.pool())
            .await?)
    } else {
        Ok(sqlx::query(MEMBER)
            .bind(id)
            .bind(user.user_level)
            .bind(truthy(user.custom_properties.get("hide_adult_content")))
            .fetch_all(auth.store.pool())
            .await?)
    }
}
fn provider(row: &PgRow) -> Result<Value, StoreError> {
    let raw: Value = row.try_get("raw")?;
    let mut value = Map::new();
    // The source read-only m3u_accounts.id path is absent on this through
    // model. DRF omits it; do not invent an account-ID key.
    for field in [
        "channel_group_id",
        "enabled",
        "auto_channel_sync",
        "auto_sync_channel_start",
        "auto_sync_channel_end",
        "custom_properties",
        "is_stale",
        "last_seen",
    ] {
        let item = match field {
            "auto_sync_channel_start" | "auto_sync_channel_end" => float(raw[field].clone()),
            "last_seen" => timestamp(&raw[field]),
            "custom_properties" => crate::provider_reads::profile_properties(&raw[field]),
            _ => raw[field].clone(),
        };
        value.insert(
            if field == "channel_group_id" {
                "channel_group"
            } else {
                field
            }
            .into(),
            item,
        );
    }
    value.insert(
        "stream_count".into(),
        json!(row.try_get::<i64, _>("stream_count")?),
    );
    Ok(Value::Object(value))
}
fn group(row: PgRow, count: i64, accounts: Vec<Value>) -> Result<Value, StoreError> {
    Ok(
        json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?,"channel_count":count,"is_custom":row.try_get::<bool,_>("is_custom")?,"m3u_account_count":accounts.len(),"m3u_accounts":accounts}),
    )
}
async fn serialize(auth: &AuthService, row: PgRow) -> Result<Value, StoreError> {
    let id: i64 = row.try_get("id")?;
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=$1").bind(id).fetch_one(auth.store.pool()).await?;
    let providers=sqlx::query("SELECT to_jsonb(r) AS raw,(SELECT count(*) FROM dispatcharr_channels_stream s WHERE s.m3u_account_id=r.m3u_account_id AND s.channel_group_id=r.channel_group_id) AS stream_count FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=$1 AND NOT a.locked").bind(id).fetch_all(auth.store.pool()).await?;
    group(
        row,
        count,
        providers.iter().map(provider).collect::<Result<_, _>>()?,
    )
}
async fn detail(auth: &AuthService, user: &User, id: i64) -> Result<Value, StoreError> {
    let row = rows(auth, user, Some(id))
        .await?
        .into_iter()
        .next()
        .ok_or_else(missing)?;
    serialize(auth, row).await
}
async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let groups = rows(&auth, &user, None).await?;
    if groups.is_empty() {
        return Ok(Json(json!([])));
    }
    let ids = groups
        .iter()
        .map(|row| row.try_get::<i64, _>("id"))
        .collect::<Result<Vec<_>, _>>()?;
    let counts=sqlx::query("SELECT COALESCE(o.channel_group_id,c.channel_group_id) AS gid,count(*) AS n FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=ANY($1) AND ($2 OR (c.user_level<=$3 AND (NOT $4 OR NOT c.is_adult))) GROUP BY COALESCE(o.channel_group_id,c.channel_group_id)").bind(&ids).bind(user.user_level>=10).bind(user.user_level).bind(truthy(user.custom_properties.get("hide_adult_content"))).fetch_all(auth.store.pool()).await?.into_iter().map(|row|Ok((row.try_get::<i64,_>("gid")?,row.try_get::<i64,_>("n")?))).collect::<Result<HashMap<_,_>,sqlx::Error>>()?;
    let mut relations: HashMap<i64, Vec<Value>> = HashMap::new();
    if user.user_level >= 10 {
        let providers=sqlx::query("SELECT r.channel_group_id,to_jsonb(r) AS raw,COALESCE(s.n,0)::bigint AS stream_count FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id LEFT JOIN (SELECT m3u_account_id,channel_group_id,count(*) AS n FROM dispatcharr_channels_stream WHERE channel_group_id=ANY($1) GROUP BY m3u_account_id,channel_group_id) s ON s.m3u_account_id=r.m3u_account_id AND s.channel_group_id=r.channel_group_id WHERE r.channel_group_id=ANY($1) AND NOT a.locked").bind(&ids).fetch_all(auth.store.pool()).await?;
        for row in providers {
            relations
                .entry(row.try_get("channel_group_id")?)
                .or_default()
                .push(provider(&row)?);
        }
    }
    let mut values = Vec::with_capacity(groups.len());
    for row in groups {
        let id: i64 = row.try_get("id")?;
        values.push(group(
            row,
            counts.get(&id).copied().unwrap_or(0),
            relations.remove(&id).unwrap_or_default(),
        )?);
    }
    Ok(Json(Value::Array(values)))
}
async fn retrieve(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    Ok(Json(detail(&auth, &user, id(raw)?).await?))
}
fn name(body: Map<String, Value>, partial: bool) -> Result<Option<String>, StoreError> {
    match body.get("name") {
        Some(value) => character("name", value, None, false).map(Some),
        None if partial => Ok(None),
        None => Err(StoreError::InvalidInput(
            json!({"name":["This field is required."]}),
        )),
    }
}
async fn check_unique(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    except: Option<i64>,
) -> Result<(), StoreError> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroup WHERE name=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(name).bind(except).fetch_one(&mut **tx).await?;
    if exists {
        Err(unique())
    } else {
        Ok(())
    }
}
async fn get_group(tx: &mut Transaction<'_, Postgres>, id: i64) -> Result<PgRow, StoreError> {
    sqlx::query("SELECT g.name,EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND NOT a.locked) AS has_provider,EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND a.locked) AS is_custom FROM dispatcharr_channels_channelgroup g WHERE g.id=$1 FOR UPDATE OF g").bind(id).fetch_optional(&mut **tx).await?.ok_or_else(missing)
}
async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let name = name(body_object(body)?, false)?.expect("required name validated");
    let mut tx = auth.store.pool().begin().await?;
    check_unique(&mut tx, &name, None).await?;
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO dispatcharr_channels_channelgroup(name) VALUES($1) RETURNING id",
    )
    .bind(name)
    .fetch_one(&mut *tx)
    .await
    .map_err(write_error)?;
    mark_group(&mut tx, &json!(id)).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(detail(&auth, &user, id).await?)).into_response())
}
async fn update(
    auth: AuthService,
    raw: String,
    peer: SocketAddr,
    headers: HeaderMap,
    body: Body,
    partial: bool,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    let old = get_group(&mut tx, id).await?;
    let provider: bool = old.try_get("has_provider")?;
    let custom: bool = old.try_get("is_custom")?;
    if provider && !custom {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot edit group with M3U account associations"}),
        ));
    }
    let name = name(body_object(body)?, partial)?.unwrap_or(old.try_get("name")?);
    check_unique(&mut tx, &name, Some(id)).await?;
    if provider && name != old.try_get::<String, _>("name")? {
        let new: i64 = sqlx::query_scalar(
            "INSERT INTO dispatcharr_channels_channelgroup(name) VALUES($1) RETURNING id",
        )
        .bind(name)
        .fetch_one(&mut *tx)
        .await
        .map_err(write_error)?;
        mark_group(&mut tx, &json!(new)).await?;
        sqlx::query("UPDATE dispatcharr_channels_channel SET channel_group_id=$1 WHERE channel_group_id=$2 AND NOT auto_created").bind(new).bind(id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount r USING m3u_m3uaccount a WHERE r.m3u_account_id=a.id AND r.channel_group_id=$1 AND a.locked").bind(id).execute(&mut *tx).await?;
        tx.commit().await?;
        let mut value = detail(&auth, &user, new).await?;
        value["demoted_group"] = detail(&auth, &user, id).await?;
        return Ok(Json(value));
    }
    sqlx::query("UPDATE dispatcharr_channels_channelgroup SET name=$1 WHERE id=$2")
        .bind(name)
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(write_error)?;
    tx.commit().await?;
    Ok(Json(detail(&auth, &user, id).await?))
}
async fn patch_group(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    update(auth, raw, peer, headers, body, true).await
}
async fn put_group(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    update(auth, raw, peer, headers, body, false).await
}
async fn has_channels(tx: &mut Transaction<'_, Postgres>, id: i64) -> Result<bool, StoreError> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=$1)").bind(id).fetch_one(&mut **tx).await?)
}
async fn remove(tx: &mut Transaction<'_, Postgres>, id: i64) -> Result<(), StoreError> {
    for query in ["UPDATE dispatcharr_channels_channel SET channel_group_id=NULL WHERE channel_group_id=$1","UPDATE dispatcharr_channels_channeloverride SET channel_group_id=NULL WHERE channel_group_id=$1","UPDATE dispatcharr_channels_stream SET channel_group_id=NULL WHERE channel_group_id=$1","DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=$1","DELETE FROM dispatcharr_channels_channelgroup WHERE id=$1"]{sqlx::query(query).bind(id).execute(&mut **tx).await?;}
    Ok(())
}
async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    let group = get_group(&mut tx, id).await?;
    if has_channels(&mut tx, id).await? {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot delete group with associated channels"}),
        ));
    }
    if group.try_get::<bool, _>("has_provider")? {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot delete group with M3U account associations"}),
        ));
    }
    remove(&mut tx, id).await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn claim(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    get_group(&mut tx, id).await?;
    mark_group(&mut tx, &json!(id)).await?;
    tx.commit().await?;
    Ok(Json(detail(&auth, &user, id).await?))
}
async fn unclaim(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    get_group(&mut tx, id).await?;
    if has_channels(&mut tx, id).await? {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot unclaim group with associated channels"}),
        ));
    }
    sqlx::query("DELETE FROM dispatcharr_channels_channelgroupm3uaccount r USING m3u_m3uaccount a WHERE r.m3u_account_id=a.id AND r.channel_group_id=$1 AND a.locked").bind(id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Json(detail(&auth, &user, id).await?))
}
async fn cleanup(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let body = body_object(body)?;
    let include = body.get("include_custom").is_some_and(|value| match value {
        Value::Bool(value) => *value,
        Value::String(value) => ["1", "true", "yes"].contains(&value.to_lowercase().as_str()),
        Value::Number(value) => value.to_string() == "1",
        _ => false,
    });
    let mut tx = auth.store.pool().begin().await?;
    let rows=sqlx::query("SELECT g.id,g.name FROM dispatcharr_channels_channelgroup g WHERE NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id WHERE COALESCE(o.channel_group_id,c.channel_group_id)=g.id) AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND NOT a.locked) AND ($1 OR NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=g.id AND a.locked)) FOR UPDATE OF g").bind(include).fetch_all(&mut *tx).await?;
    let mut names = Vec::new();
    for row in rows {
        let id: i64 = row.try_get("id")?;
        names.push(row.try_get::<String, _>("name")?);
        remove(&mut tx, id).await?;
    }
    tx.commit().await?;
    Ok(Json(
        json!({"message":format!("Successfully deleted {} unused channel groups",names.len()),"deleted_count":names.len(),"deleted_groups":names}),
    ))
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/channels/groups/", get(list).post(create))
        .route("/api/channels/groups/cleanup/", post(cleanup))
        .route(
            "/api/channels/groups/{id}/",
            get(retrieve)
                .patch(patch_group)
                .put(put_group)
                .delete(delete),
        )
        .route("/api/channels/groups/{id}/claim-custom/", post(claim))
        .route("/api/channels/groups/{id}/unclaim-custom/", post(unclaim))
        .with_state(auth)
}
