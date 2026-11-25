//! Channel-profile CRUD and duplicate contracts from the original Dispatcharr.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    admin_writes::{body_object, boolean, character},
    catalogue_options, AuthService, StoreError, User,
};
use axum::{
    extract::{rejection::JsonRejection, ConnectInfo, Path, State},
    http::{header, HeaderMap, StatusCode, Uri},
    middleware::from_fn_with_state,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use serde_json::{json, ser::PrettyFormatter, Map, Value};
use sqlx::{Postgres, Row, Transaction};
use std::net::SocketAddr;
type Body = Result<Json<Value>, JsonRejection>;
const SELECT: &str = "SELECT p.id::bigint AS id,p.name,COALESCE((SELECT jsonb_agg(m.channel_id ORDER BY m.id) FROM dispatcharr_channels_channelprofilemembership m WHERE m.channel_profile_id=p.id AND m.enabled),'[]'::jsonb) AS channels FROM dispatcharr_channels_channelprofile p WHERE ($1::bigint IS NULL OR p.id=$1) AND ($2 OR EXISTS(SELECT 1 FROM accounts_user_channel_profiles up WHERE up.channelprofile_id=p.id AND up.user_id=$3)) ORDER BY p.id";
pub(crate) fn missing() -> StoreError {
    StoreError::StatusBody(
        404,
        json!({"detail":"No ChannelProfile matches the given query."}),
    )
}
pub(crate) fn id(value: String) -> Result<i64, StoreError> {
    value
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))
}
fn serialize(row: sqlx::postgres::PgRow) -> Result<Value, sqlx::Error> {
    Ok(
        json!({"id":row.try_get::<i64,_>("id")?,"name":row.try_get::<String,_>("name")?,"channels":row.try_get::<Value,_>("channels")?}),
    )
}

pub(crate) fn detail_read_id(mut raw: String, path: &str) -> Result<(String, bool), StoreError> {
    if let Some(id) = raw
        .strip_suffix(".json")
        .filter(|id| !id.is_empty() && !id.contains(['.', '/']))
    {
        raw.truncate(id.len());
        return Ok((raw, true));
    }
    if path.ends_with('/') {
        Ok((raw, false))
    } else {
        Err(StoreError::NotFound)
    }
}

pub(crate) fn explicit_json_indent(accept: &str) -> Option<usize> {
    let mut selected = None::<(u8, Option<&str>)>;
    for range in accept.split(',') {
        let (media, parameters) = range.split_once(';').unwrap_or((range, ""));
        let media = media.trim();
        let rank = match media {
            "*/*" => 0,
            media if media.eq_ignore_ascii_case("application/*") => 1,
            media if media.eq_ignore_ascii_case("application/json") => 2,
            _ => continue,
        };
        let mut has_non_quality_parameter = false;
        let mut indent = None;
        for (name, value) in parameters
            .split(';')
            .filter_map(|part| part.split_once('='))
        {
            let name = name.trim();
            has_non_quality_parameter |= !name.eq_ignore_ascii_case("q");
            if name.eq_ignore_ascii_case("indent") {
                indent = Some(value);
            }
        }
        // Source ranks wildcard type, wildcard subtype, exact and
        // parameterized exact ranges. Compact JSON is a selected result.
        let rank = rank + u8::from(rank == 2 && has_non_quality_parameter);
        if selected.is_none_or(|(previous, _)| rank > previous) {
            selected = Some((rank, indent));
        }
    }
    selected
        .and_then(|(_, indent)| indent)
        .and_then(crate::vod_api_draft::json_indent_value)
}

pub(crate) fn negotiate_read_json(
    uri: &Uri,
    headers: &HeaderMap,
    suffix: bool,
) -> Result<Option<usize>, StoreError> {
    // DRF's format suffix wins over the query parameter. Reuse the JSON
    // renderer negotiation with that resolved format.
    let explicit_json = suffix
        || url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .filter(|(key, _)| key == "format")
            .last()
            .is_some_and(|(_, value)| value == "json");
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|accept| accept.to_str().ok());
    if explicit_json {
        crate::epg_program_http::json_negotiation(headers, &Uri::from_static("/?format=json"))
            .map_err(|error| {
                if matches!(error, StoreError::Unsupported(_)) {
                    StoreError::StatusBody(
                        406,
                        json!({"detail":"Could not satisfy the request Accept header."}),
                    )
                } else {
                    error
                }
            })?;
        return Ok(accept.and_then(explicit_json_indent));
    }
    let indent = accept.and_then(|accept| {
        accept.split(',').find_map(|range| {
            let (media, _) = range.split_once(';')?;
            if !media.trim().eq_ignore_ascii_case("application/json") {
                return None;
            }
            crate::vod_api_draft::json_indent_for_range(range)
        })
    });
    Ok(indent)
}

pub(crate) fn json_response(value: Value, indent: Option<usize>) -> Result<Response, StoreError> {
    let Some(indent) = indent else {
        return Ok(Json(value).into_response());
    };
    let mut bytes = Vec::new();
    let spaces = [b' '; 8];
    let formatter = PrettyFormatter::with_indent(&spaces[..indent]);
    value
        .serialize(&mut serde_json::Serializer::with_formatter(
            &mut bytes, formatter,
        ))
        .map_err(|_| StoreError::UnhandledRestFault)?;
    let mut body = String::from_utf8(bytes).map_err(|_| StoreError::UnhandledRestFault)?;
    if body.contains(['\u{2028}', '\u{2029}']) {
        body = body
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
    }
    Ok(([(header::CONTENT_TYPE, "application/json")], body).into_response())
}

#[cfg(feature = "sqlite-copied-data-runtime")]
pub(crate) fn canonical_json_response(
    value: Value,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<Response, StoreError> {
    json_response(value, negotiate_read_json(uri, headers, false)?)
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
    uri: Uri,
) -> Result<Response, StoreError> {
    let suffix = uri.path().trim_end_matches('/').ends_with(".json");
    let indent = negotiate_read_json(&uri, &headers, suffix)?;
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
    let values = rows.into_iter().map(serialize).collect::<Result<_, _>>()?;
    json_response(Value::Array(values), indent)
}
async fn retrieve(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Response, StoreError> {
    let (raw, suffix) = detail_read_id(raw, uri.path())?;
    let indent = negotiate_read_json(&uri, &headers, suffix)?;
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let mut tx = auth.store.pool().begin().await?;
    json_response(lookup(&mut tx, &user, id(raw)?).await?, indent)
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
async fn omitted_profile_read_method() -> StoreError {
    StoreError::NotFound
}

pub(crate) fn router(auth: AuthService) -> Router {
    let memberships = crate::channel_memberships::router(auth.clone());
    Router::new()
        .route(
            "/api/channels/profiles/",
            get(list)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .post(create)
                .options(catalogue_options::channel_profile),
        )
        .route(
            "/api/channels/profiles/{id}/",
            get(retrieve)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .patch(patch)
                .put(put)
                .delete(delete)
                .options(catalogue_options::channel_profile),
        )
        .route(
            "/api/channels/profiles.json",
            get(list)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::channel_profile_json_alias)
                .fallback(omitted_profile_read_method),
        )
        .route(
            "/api/channels/profiles.json/",
            get(list).fallback(omitted_profile_read_method),
        )
        .route(
            "/api/channels/profiles/{id}",
            get(retrieve)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    catalogue_options::typed_catalogue_read_headers,
                ))
                .options(catalogue_options::channel_profile_json_alias)
                .fallback(omitted_profile_read_method),
        )
        .route("/api/channels/profiles/{id}/duplicate/", post(duplicate))
        .with_state(auth)
        .merge(memberships)
}

#[cfg(test)]
mod read_format_tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn detail_read_distinguishes_slash_and_both_json_suffix_paths() {
        for (raw, path, suffix) in [
            ("41", "/api/channels/profiles/41/", false),
            ("41.json", "/api/channels/profiles/41.json", true),
            ("41.json", "/api/channels/profiles/41.json/", true),
        ] {
            assert_eq!(
                detail_read_id(raw.into(), path).unwrap(),
                ("41".into(), suffix)
            );
        }
        for raw in ["41", ".json", "4.1.json", "41.api"] {
            assert!(matches!(
                detail_read_id(raw.into(), &format!("/api/channels/profiles/{raw}")),
                Err(StoreError::NotFound)
            ));
        }
    }

    #[test]
    fn explicit_profile_json_reuses_renderer_and_suffix_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", HeaderValue::from_static("application/json"));
        for (path, suffix) in [
            ("/api/channels/profiles/?format=json", false),
            ("/api/channels/profiles.json?format=json", true),
            ("/api/channels/profiles/41.json?format=api", true),
        ] {
            let uri: Uri = path.parse().unwrap();
            assert!(negotiate_read_json(&uri, &headers, suffix).is_ok());
        }
        headers.insert("accept", HeaderValue::from_static("text/html"));
        assert!(matches!(
            negotiate_read_json(&Uri::from_static("/?format=json"), &headers, false),
            Err(StoreError::StatusBody(406, _))
        ));
        assert!(negotiate_read_json(&Uri::from_static("/"), &headers, false).is_ok());
    }

    #[test]
    fn explicit_profile_json_selects_source_media_precedence_and_compact() {
        let uri = Uri::from_static("/api/channels/profiles.json");
        let cases = [
            ("application/*; indent=2", Some(2)),
            ("*/*; indent=3", Some(3)),
            ("application/json, application/*; indent=2", None),
            ("application/json; indent = 2", Some(2)),
            ("application/json; indent=0", None),
            ("application/json; indent=-1", None),
            ("text/html, application/json; indent=4", Some(4)),
        ];
        let mut headers = HeaderMap::new();
        for (accept, expected) in cases {
            headers.insert(header::ACCEPT, HeaderValue::from_static(accept));
            assert_eq!(negotiate_read_json(&uri, &headers, true).unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn profile_indent_matches_json_renderer_bound_and_separator_escaping() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json; indent=99"),
        );
        let uri = Uri::from_static("/api/channels/profiles/41.json?format=api");
        assert_eq!(negotiate_read_json(&uri, &headers, true).unwrap(), Some(8));
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/json; indent=\"2\""),
        );
        let uri = Uri::from_static("/api/channels/profiles/41/");
        let indent = negotiate_read_json(&uri, &headers, false).unwrap();
        assert_eq!(indent, Some(2));
        let response = json_response(
            json!({"id":41,"name":"Č\u{2028}sample\u{2029}","channels":[5]}),
            indent,
        )
        .unwrap();
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert_eq!(
            body.as_ref(),
            "{\n  \"id\": 41,\n  \"name\": \"Č\\u2028sample\\u2029\",\n  \"channels\": [\n    5\n  ]\n}".as_bytes()
        );
    }
}
