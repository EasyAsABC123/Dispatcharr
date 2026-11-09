//! Channel-profile CRUD and duplicate contracts from the original Dispatcharr.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, boolean, character},
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
use sqlx::{Postgres, Row, Transaction};
use std::net::SocketAddr;
type Body = Result<Json<Value>, JsonRejection>;
const SELECT:&str="SELECT p.id::bigint AS id,p.name,COALESCE((SELECT jsonb_agg(m.channel_id ORDER BY m.id) FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_profile_id=p.id AND m.enabled),'[]'::jsonb) AS channels FROM dispatcharr_channels_channelprofile p WHERE ($1::bigint IS NULL OR p.id=$1) AND ($2 OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.channelprofile_id=p.id AND up.user_id=$3)) ORDER BY p.id";
fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No ChannelProfile matches the given query."}),
    )
}
fn id(value: String) -> Result<i64, StoreError> {
    value
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))
}
fn serialize(row: sqlx::postgres::PgRow) -> Result<Value, sqlx::Error> {
    Ok(
        json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?,"channels":row.try_get::<Value,_>("channels")?}),
    )
}
pub(crate) async fn lookup(
    tx: &mut Transaction<'_, Postgres>,
    user: &User,
    id: i64,
) -> Result<Value, StoreError> {
    let row = sqlx::query(SELECT)
        .bind(Some(id))
        .bind(user.user_level == 10)
        .bind(user.id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(missing)?;
    Ok(serialize(row)?)
}
fn validation(
    body: Map<String, Value>,
    partial: bool,
) -> Result<(Option<String>, bool), StoreError> {
    let mut errors = Map::new();
    let mut name = None;
    let mut empty = false;
    if let Some(value) = body.get("name") {
        match character("name", value, Some(100), false) {
            Ok(value) => name = Some(value),
            Err(StoreError::InvalidInput(Value::Object(value))) => errors.extend(value),
            Err(error) => return Err(error),
        }
    } else if !partial {
        errors.insert("name".into(), json!(["This field is required."]));
    }
    if let Some(value) = body.get("start_empty") {
        match boolean("start_empty", value) {
            Ok(value) => empty = value,
            Err(StoreError::InvalidInput(Value::Object(value))) => errors.extend(value),
            Err(error) => return Err(error),
        }
    }
    if errors.is_empty() {
        Ok((name, empty))
    } else {
        Err(StoreError::InvalidInput(Value::Object(errors)))
    }
}
fn unique() -> StoreError {
    StoreError::InvalidInput(json!({"name":["channel profile with this name already exists."]}))
}
async fn check_unique(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    except: Option<i64>,
) -> Result<(), StoreError> {
    let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelprofile WHERE name=$1 AND ($2::bigint IS NULL OR id<>$2))").bind(name).bind(except).fetch_one(&mut **tx).await?;
    if exists {
        Err(unique())
    } else {
        Ok(())
    }
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
async fn insert(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    empty: bool,
) -> Result<i64, StoreError> {
    let id = sqlx::query_scalar::<_, i64>(
        "INSERT INTO dispatcharr_channels_channelprofile(name) VALUES($1) RETURNING id",
    )
    .bind(name)
    .fetch_one(&mut **tx)
    .await
    .map_err(write_error)?;
    if !empty {
        sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) SELECT $1,id,true FROM dispatcharr_channels_channel ORDER BY id").bind(id).execute(&mut **tx).await?;
    }
    Ok(id)
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
    let rows = sqlx::query(SELECT)
        .bind(None::<i64>)
        .bind(user.user_level == 10)
        .bind(user.id)
        .fetch_all(auth.store.pool())
        .await?;
    Ok(Json(Value::Array(
        rows.into_iter().map(serialize).collect::<Result<_, _>>()?,
    )))
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
    let mut tx = auth.store.pool().begin().await?;
    Ok(Json(lookup(&mut tx, &user, id(raw)?).await?))
}
async fn create(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let (name, empty) = validation(body_object(body)?, false)?;
    let name = name.expect("required name validated");
    let mut tx = auth.store.pool().begin().await?;
    check_unique(&mut tx, &name, None).await?;
    let id = insert(&mut tx, &name, empty).await?;
    // Creation serializes the created object directly, even for an admin above level 10.
    let mut all = user;
    all.user_level = 10;
    let value = lookup(&mut tx, &all, id).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(value)).into_response())
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
    lookup(&mut tx, &user, id).await?;
    let (name, _) = validation(body_object(body)?, partial)?;
    if let Some(name) = name {
        check_unique(&mut tx, &name, Some(id)).await?;
        sqlx::query("UPDATE dispatcharr_channels_channelprofile SET name=$1 WHERE id=$2")
            .bind(name)
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(write_error)?;
    }
    let value = lookup(&mut tx, &user, id).await?;
    tx.commit().await?;
    Ok(Json(value))
}
async fn patch(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    update(auth, raw, peer, headers, body, true).await
}
async fn put(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Value>, StoreError> {
    update(auth, raw, peer, headers, body, false).await
}
async fn delete(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<StatusCode, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let id = id(raw)?;
    let mut tx = auth.store.pool().begin().await?;
    lookup(&mut tx, &user, id).await?;
    sqlx::query("DELETE FROM accounts_user_channel_profiles WHERE channelprofile_id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "DELETE FROM dispatcharr_channels_channelprofilemembership WHERE channel_profile_id=$1",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM dispatcharr_channels_channelprofile WHERE id=$1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn duplicate(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, StoreError> {
    let user = auth.account_user(peer, headers, true).await?;
    let body = body_object(body)?;
    let name = match body.get("name") {
        None => String::new(),
        Some(Value::String(v)) => v.clone(),
        Some(Value::Bool(v)) => {
            if *v {
                "True".into()
            } else {
                "False".into()
            }
        }
        Some(Value::Null) => "None".into(),
        Some(v) => v.to_string(),
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(StoreError::InvalidInput(
            json!({"detail":"Name is required to duplicate a profile."}),
        ));
    }
    let mut tx = auth.store.pool().begin().await?;
    if let Err(error) = check_unique(&mut tx, name, None).await {
        if !matches!(error, StoreError::InvalidInput(_)) {
            return Err(error);
        }
        return Err(StoreError::InvalidInput(
            json!({"detail":"A channel profile with this name already exists."}),
        ));
    }
    let source = id(raw)?;
    lookup(&mut tx, &user, source).await?;
    let target = insert(&mut tx, name, true).await?;
    sqlx::query("INSERT INTO dispatcharr_channels_channelprofilemembership(channel_profile_id,channel_id,enabled) SELECT $1,c.id,COALESCE(m.enabled,false) FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_channelprofilemembership m ON m.channel_id=c.id AND m.channel_profile_id=$2 ORDER BY c.id").bind(target).bind(source).execute(&mut *tx).await?;
    let mut all = user;
    all.user_level = 10;
    let value = lookup(&mut tx, &all, target).await?;
    tx.commit().await?;
    Ok((StatusCode::CREATED, Json(value)).into_response())
}
pub(crate) fn router(auth: AuthService) -> Router {
    let memberships = crate::channel_memberships::router(auth.clone());
    Router::new()
        .route("/api/channels/profiles/", get(list).post(create))
        .route(
            "/api/channels/profiles/{id}/",
            get(retrieve).patch(patch).put(put).delete(delete),
        )
        .route("/api/channels/profiles/{id}/duplicate/", post(duplicate))
        .with_state(auth)
        .merge(memberships)
}
