//! Channel-group contracts from original Dispatcharr models/serializers/views.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, character},
    channel_writes::mark_group,
    channels::{
        channel_options_headers, channel_options_indentation, float, options_json_response,
        serialize_options_json, timestamp, truthy,
    },
    AuthService, StoreError, User,
};
use axum::{
    body::Bytes,
    extract::{rejection::JsonRejection, ConnectInfo, OriginalUri, Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};
use std::{collections::HashMap, net::SocketAddr, sync::OnceLock};
type Body = Result<Json<Value>, JsonRejection>;
const COLLECTION_ALLOW: &str = "GET, POST, HEAD, OPTIONS";
const DETAIL_ALLOW: &str = "GET, PUT, PATCH, DELETE, HEAD, OPTIONS";
const CLAIM_ALLOW: &str = "POST, OPTIONS";
const GROUP_OPTIONS: &[u8; 1939] = include_bytes!("../datasets/source-channel-group-options.json");

#[derive(Clone, Copy, Eq, PartialEq)]
enum GroupOptions {
    Collection,
    ExistingDetail,
    MissingDetail,
    Claim,
}

impl GroupOptions {
    fn allow(self) -> HeaderValue {
        let allow = match self {
            Self::Collection => COLLECTION_ALLOW,
            Self::ExistingDetail | Self::MissingDetail => DETAIL_ALLOW,
            Self::Claim => CLAIM_ALLOW,
        };
        HeaderValue::from_static(allow)
    }

    fn entity(self, indentation: usize) -> Result<Response, StoreError> {
        static RENDERED: [[OnceLock<Option<Bytes>>; 9]; 4] =
            [const { [const { OnceLock::new() }; 9] }; 4];
        let body = if self == Self::Collection && indentation == 0 {
            Bytes::from_static(GROUP_OPTIONS)
        } else {
            RENDERED[self as usize][indentation]
                .get_or_init(|| {
                    let mut metadata: Map<String, Value> =
                        serde_json::from_slice(GROUP_OPTIONS).ok()?;
                    match self {
                        Self::Collection => {}
                        Self::ExistingDetail | Self::MissingDetail => {
                            *metadata.get_mut("name")? = "Channel Group Instance".into();
                            if self == Self::MissingDetail {
                                metadata.shift_remove("actions");
                            } else {
                                let actions = metadata.get_mut("actions")?.as_object_mut()?;
                                let fields = actions.shift_remove("POST")?;
                                actions.insert("PUT".into(), fields);
                            }
                        }
                        Self::Claim => {
                            *metadata.get_mut("name")? = "Claim custom".into();
                            *metadata.get_mut("description")? =
                                "Mark a group as custom so provider cleanup cannot remove it."
                                    .into();
                        }
                    }
                    serialize_options_json(&metadata, indentation)
                })
                .clone()
                .ok_or(StoreError::UnhandledRestFault)?
        };
        options_json_response(body)
    }
}

async fn group_options(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    uri: Uri,
    requested: GroupOptions,
    detail_id: Option<String>,
) -> Response {
    let result = async {
        auth.account_user(peer, headers.clone(), true).await?;
        let indentation = channel_options_indentation(&uri, &headers)?;
        let scope = match detail_id {
            Some(raw) => {
                let exists = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroup WHERE id = $1)",
                )
                .bind(id(raw)?)
                .fetch_one(auth.store.pool())
                .await?;
                if exists {
                    GroupOptions::ExistingDetail
                } else {
                    GroupOptions::MissingDetail
                }
            }
            None => requested,
        };
        scope.entity(indentation)
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        requested.allow(),
    )
}

async fn collection_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    group_options(auth, peer, headers, uri, GroupOptions::Collection, None).await
}

async fn detail_options(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    group_options(
        auth,
        peer,
        headers,
        uri,
        GroupOptions::ExistingDetail,
        Some(raw),
    )
    .await
}

async fn claim_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    // DRF's POST metadata does not look up the group or execute the action.
    group_options(auth, peer, headers, uri, GroupOptions::Claim, None).await
}

fn group_response(body: Json<Value>, allow: Option<&'static str>) -> Response {
    match allow {
        Some(allow) => ([(header::ALLOW, allow)], body).into_response(),
        None => body.into_response(),
    }
}

fn get_response(body: Json<Value>, method: Method, allow: &'static str) -> Response {
    // Axum also dispatches HEAD through these GET handlers.
    group_response(body, (method == Method::GET).then_some(allow))
}
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
    method: Method,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let groups = rows(&auth, &user, None).await?;
    if groups.is_empty() {
        return Ok(get_response(Json(json!([])), method, COLLECTION_ALLOW));
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
    Ok(get_response(
        Json(Value::Array(values)),
        method,
        COLLECTION_ALLOW,
    ))
}
async fn retrieve(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let body = Json(detail(&auth, &user, id(raw)?).await?);
    Ok(get_response(body, method, DETAIL_ALLOW))
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
) -> Result<Response, StoreError> {
    let body = update(auth, raw, peer, headers, body, true).await?;
    Ok(group_response(body, Some(DETAIL_ALLOW)))
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
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    get_group(&mut tx, id).await?;
    mark_group(&mut tx, &json!(id)).await?;
    tx.commit().await?;
    let body = Json(detail(&auth, &user, id).await?);
    Ok(group_response(body, Some(CLAIM_ALLOW)))
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
        .route(
            "/api/channels/groups/",
            get(list).post(create).options(collection_options),
        )
        .route("/api/channels/groups/cleanup/", post(cleanup))
        .route(
            "/api/channels/groups/{id}/",
            get(retrieve)
                .patch(patch_group)
                .put(put_group)
                .delete(delete)
                .options(detail_options),
        )
        .route(
            "/api/channels/groups/{id}/claim-custom/",
            post(claim).options(claim_options),
        )
        .route("/api/channels/groups/{id}/unclaim-custom/", post(unclaim))
        .with_state(auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn group_options_matches_all_observed_stock_entities() {
        for (scope, length, digest) in [
            (
                GroupOptions::Collection,
                1939,
                "e5fe36198f3be26ac6b71a637b494010aa9d70d17e8cd2b209e50b0cb31c1ed9",
            ),
            (
                GroupOptions::ExistingDetail,
                1942,
                "9d8322d462656f2525af589399792adda5e2123eea2d656a14ca957d5eaa6978",
            ),
            (
                GroupOptions::MissingDetail,
                181,
                "64d48b525c2b1ad9376cf09fcd39b8bdf663e5ff1bdf8ba5917e26826941e794",
            ),
            (
                GroupOptions::Claim,
                1993,
                "35069a5e5a547591f2198efa6619b8d8dc7d464c1eaa52eebabffe5bdb491135",
            ),
        ] {
            let response = channel_options_headers(scope.entity(0).unwrap(), scope.allow());
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::ALLOW], scope.allow());
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                length.to_string()
            );
            let body = to_bytes(response.into_body(), length).await.unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(&body)), digest);
        }
    }

    #[tokio::test]
    async fn group_options_routes_require_the_existing_admin_authentication() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://unused:unused@localhost/unused")
            .unwrap();
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("test-group-options-signing-key".into()).unwrap(),
        );
        for (path, allow) in [
            ("/api/channels/groups/", COLLECTION_ALLOW),
            ("/api/channels/groups/1/", DETAIL_ALLOW),
            ("/api/channels/groups/1/claim-custom/", CLAIM_ALLOW),
        ] {
            let request = Request::builder()
                .method(Method::OPTIONS)
                .uri(path)
                .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9192))))
                .body(Body::empty())
                .unwrap();
            let response = router(auth.clone()).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(response.headers()[header::ALLOW], allow);
            assert_eq!(response.headers()[header::VARY], "Accept");
            assert_eq!(
                response.headers()[header::WWW_AUTHENTICATE],
                "Bearer realm=\"api\""
            );
        }
    }

    #[tokio::test]
    async fn group_get_and_mutation_allow_preserve_json_and_head_handling() {
        let body = Json(json!({"id": 9}));
        for (response, expected) in [
            (
                get_response(body.clone(), Method::GET, COLLECTION_ALLOW),
                Some(COLLECTION_ALLOW),
            ),
            (
                get_response(body.clone(), Method::GET, DETAIL_ALLOW),
                Some(DETAIL_ALLOW),
            ),
            (get_response(body.clone(), Method::HEAD, DETAIL_ALLOW), None),
            (
                group_response(body.clone(), Some(DETAIL_ALLOW)),
                Some(DETAIL_ALLOW),
            ),
            (group_response(body, Some(CLAIM_ALLOW)), Some(CLAIM_ALLOW)),
        ] {
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(
                response
                    .headers()
                    .get(header::ALLOW)
                    .map(|value| value.to_str().unwrap()),
                expected
            );
            assert_eq!(
                to_bytes(response.into_body(), 32).await.unwrap().as_ref(),
                br#"{"id":9}"#
            );
        }
    }
}
