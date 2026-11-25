//! Channel-group contracts from original Dispatcharr models/serializers/views.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, character},
    catalogue_options,
    channels::{
        channel_options_headers, channel_options_indentation, float, options_json_response,
        serialize_options_json, timestamp, truthy,
    },
    core_reads::{negotiate_literal_json_alias, omitted_json_method, JsonReadId},
    AuthService, StoreError, User,
};
use axum::{
    body::Bytes,
    extract::{rejection::JsonRejection, ConnectInfo, OriginalUri, Path, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::from_fn_with_state,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Map, Value};
use sqlx::{postgres::PgRow, Row};
use std::{collections::HashMap, net::SocketAddr, sync::OnceLock};

mod write;

#[cfg(feature = "sqlite-identity")]
#[path = "sqlite_channel_groups.rs"]
pub(crate) mod sqlite;

use self::write::GroupWrite;

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
    Unclaim,
    Cleanup,
}

impl GroupOptions {
    fn allow(self) -> HeaderValue {
        let allow = match self {
            Self::Collection => COLLECTION_ALLOW,
            Self::ExistingDetail | Self::MissingDetail => DETAIL_ALLOW,
            Self::Claim | Self::Unclaim | Self::Cleanup => CLAIM_ALLOW,
        };
        HeaderValue::from_static(allow)
    }

    fn entity(self, indentation: usize) -> Result<Response, StoreError> {
        static RENDERED: [[OnceLock<Option<Bytes>>; 9]; 6] =
            [const { [const { OnceLock::new() }; 9] }; 6];
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
                        Self::Claim | Self::Unclaim | Self::Cleanup => {
                            let (name, description) = match self {
                                Self::Claim => (
                                    "Claim custom",
                                    "Mark a group as custom so provider cleanup cannot remove it.",
                                ),
                                Self::Unclaim => (
                                    "Unclaim custom",
                                    "Remove custom ownership from a group with no assigned channels.",
                                ),
                                Self::Cleanup => (
                                    "Cleanup unused groups",
                                    "Delete unused provider-only groups, optionally including custom groups.",
                                ),
                                _ => unreachable!(),
                            };
                            *metadata.get_mut("name")? = name.into();
                            *metadata.get_mut("description")? = description.into();
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

async fn detail_json_options(
    auth: State<AuthService>,
    JsonReadId(id): JsonReadId,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: OriginalUri,
) -> Result<Response, StoreError> {
    negotiate_literal_json_alias(&headers)?;
    Ok(detail_options(auth, Path(id), peer, headers, uri).await)
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

async fn unclaim_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    group_options(auth, peer, headers, uri, GroupOptions::Unclaim, None).await
}

async fn cleanup_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    group_options(auth, peer, headers, uri, GroupOptions::Cleanup, None).await
}

fn group_response(body: impl IntoResponse, allow: Option<&'static str>) -> Response {
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
    Ok(provider_json(&raw, row.try_get("stream_count")?))
}

fn provider_json(raw: &Value, stream_count: i64) -> Value {
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
    value.insert("stream_count".into(), json!(stream_count));
    Value::Object(value)
}

fn group(row: PgRow, count: i64, accounts: Vec<Value>) -> Result<Value, StoreError> {
    Ok(group_json(
        row.try_get("id")?,
        row.try_get("name")?,
        count,
        row.try_get("is_custom")?,
        accounts,
    ))
}

fn group_json(id: i64, name: String, count: i64, is_custom: bool, accounts: Vec<Value>) -> Value {
    json!({"id":id,"name":name,"channel_count":count,"is_custom":is_custom,"m3u_account_count":accounts.len(),"m3u_accounts":accounts})
}

pub(crate) fn collection_response(values: Vec<Value>, method: Method) -> Response {
    get_response(Json(Value::Array(values)), method, COLLECTION_ALLOW)
}

pub(crate) fn collection_permission(user: &User) -> Result<(), StoreError> {
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    Ok(())
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
    collection_permission(&user)?;
    let groups = rows(&auth, &user, None).await?;
    if groups.is_empty() {
        return Ok(collection_response(Vec::new(), method));
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
    Ok(collection_response(values, method))
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
    let group = detail(&auth, &user, id(raw)?).await;
    match group {
        Ok(value) => Ok(get_response(Json(value), method, DETAIL_ALLOW)),
        Err(error @ StoreError::StatusBody(404, _)) if method == Method::GET => {
            Ok(group_response(error, Some(DETAIL_ALLOW)))
        }
        Err(error) => Err(error),
    }
}
async fn retrieve_json_alias(
    auth: State<AuthService>,
    JsonReadId(id): JsonReadId,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    negotiate_literal_json_alias(&headers)?;
    let mut response = retrieve(auth, Path(id), peer, Method::GET, headers).await?;
    if response.status() == StatusCode::OK {
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
    }
    Ok(response)
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
async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let name = name(body_object(body)?, false)?.expect("required name validated");
    let mut tx = GroupWrite::begin(auth.store.pool()).await?;
    tx.check_unique(&name, None).await?;
    let id = tx.insert(name).await?;
    tx.mark_custom(id).await?;
    tx.commit().await?;
    let body = Json(detail(&auth, &user, id).await?);
    Ok(group_response(
        (StatusCode::CREATED, body),
        Some(COLLECTION_ALLOW),
    ))
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
    let mut tx = GroupWrite::begin(auth.store.pool()).await?;
    let old = tx.lock_group(id).await?;
    let provider = old.has_provider()?;
    let custom = old.is_custom()?;
    if provider && !custom {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot edit group with M3U account associations"}),
        ));
    }
    let name = name(body_object(body)?, partial)?.unwrap_or(old.name()?);
    tx.check_unique(&name, Some(id)).await?;
    if provider && name != old.name()? {
        let new = tx.insert(name).await?;
        tx.mark_custom(new).await?;
        tx.move_manual_channels(id, new).await?;
        tx.remove_custom_marker(id).await?;
        tx.commit().await?;
        let mut value = detail(&auth, &user, new).await?;
        value["demoted_group"] = detail(&auth, &user, id).await?;
        return Ok(Json(value));
    }
    tx.rename(id, name).await?;
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
) -> Result<Response, StoreError> {
    let body = update(auth, raw, peer, headers, body, false).await?;
    Ok(group_response(body, Some(DETAIL_ALLOW)))
}
async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = GroupWrite::begin(auth.store.pool()).await?;
    let group = tx.lock_group(id).await?;
    if tx.has_effective_channels(id).await? {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot delete group with associated channels"}),
        ));
    }
    if group.has_provider()? {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot delete group with M3U account associations"}),
        ));
    }
    tx.remove(id).await?;
    tx.commit().await?;
    Ok(group_response(StatusCode::NO_CONTENT, Some(DETAIL_ALLOW)))
}
async fn claim(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = GroupWrite::begin(auth.store.pool()).await?;
    tx.lock_group(id).await?;
    tx.mark_custom(id).await?;
    tx.commit().await?;
    let body = Json(detail(&auth, &user, id).await?);
    Ok(group_response(body, Some(CLAIM_ALLOW)))
}
async fn unclaim(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = GroupWrite::begin(auth.store.pool()).await?;
    tx.lock_group(id).await?;
    if tx.has_effective_channels(id).await? {
        return Err(StoreError::InvalidInput(
            json!({"error":"Cannot unclaim group with associated channels"}),
        ));
    }
    tx.remove_custom_marker(id).await?;
    tx.commit().await?;
    let body = Json(detail(&auth, &user, id).await?);
    Ok(group_response(body, Some(CLAIM_ALLOW)))
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
    let mut tx = GroupWrite::begin(auth.store.pool()).await?;
    let names = tx.remove_unused(include).await?;
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
        .route(
            "/api/channels/groups.json",
            catalogue_options::json_alias(list, catalogue_options::COLLECTION_ALLOW),
        )
        .route(
            "/api/channels/groups/cleanup/",
            post(cleanup).options(cleanup_options),
        )
        .route(
            "/api/channels/groups/{json_id}",
            catalogue_options::read_denial_headers(
                get(retrieve_json_alias),
                catalogue_options::DETAIL_ALLOW,
            )
            .options(detail_json_options)
            .fallback(omitted_json_method),
        )
        .route(
            "/api/channels/groups/{id}/",
            get(retrieve)
                .layer(from_fn_with_state(
                    DETAIL_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .patch(patch_group)
                .put(put_group)
                .delete(delete)
                .options(detail_options),
        )
        .route(
            "/api/channels/groups/{id}/claim-custom/",
            post(claim).options(claim_options),
        )
        .route(
            "/api/channels/groups/{id}/unclaim-custom/",
            post(unclaim).options(unclaim_options),
        )
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
            (
                GroupOptions::Unclaim,
                1998,
                "1d5a5c6c35f788b84bbe3b569434d41a3296485828dbc1909a89c69c239d0bf8",
            ),
            (
                GroupOptions::Cleanup,
                2013,
                "a113630b2a6ce35d465a0fbc2bdce277c74c3474d40c64b2fccf6b4831203a47",
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
            ("/api/channels/groups/1/unclaim-custom/", CLAIM_ALLOW),
            ("/api/channels/groups/cleanup/", CLAIM_ALLOW),
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

    #[tokio::test]
    async fn group_allow_preserves_created_empty_and_missing_responses() {
        let response = group_response(
            (StatusCode::CREATED, Json(json!({"id":9}))),
            Some(COLLECTION_ALLOW),
        );
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()[header::ALLOW], COLLECTION_ALLOW);
        assert_eq!(
            to_bytes(response.into_body(), 32).await.unwrap().as_ref(),
            br#"{"id":9}"#
        );

        let response = group_response(StatusCode::NO_CONTENT, Some(DETAIL_ALLOW));
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(response.headers()[header::ALLOW], DETAIL_ALLOW);
        assert!(to_bytes(response.into_body(), 1).await.unwrap().is_empty());

        let response = group_response(missing(), Some(DETAIL_ALLOW));
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(response.headers()[header::ALLOW], DETAIL_ALLOW);
        assert_eq!(
            to_bytes(response.into_body(), 128).await.unwrap().as_ref(),
            br#"{"detail":"No ChannelGroup matches the given query."}"#
        );
    }
}
