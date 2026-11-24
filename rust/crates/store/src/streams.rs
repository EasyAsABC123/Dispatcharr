//! Stream management read contracts from original Dispatcharr views/serializers.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    catalogue_options,
    channels::{
        base_url, channel_options_headers, channel_options_indentation, like,
        options_json_response, page_url, search_terms, serialize_options_json, stream,
    },
    core_reads::JsonReadId,
    AuthService, StoreError, User,
};
use axum::{
    body::Bytes,
    extract::{ConnectInfo, OriginalUri, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue},
    middleware::from_fn_with_state,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use futures_util::TryStreamExt;
use serde_json::{json, Map, Value};
use sqlx::{Postgres, QueryBuilder};
use std::{collections::HashMap, net::SocketAddr, sync::OnceLock};
use url::Url;
pub(crate) const FROM:&str=" FROM dispatcharr_channels_stream s LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=s.channel_group_id WHERE (a.is_active IS DISTINCT FROM false)";
fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"No Stream matches the given query."}))
}
fn number(value: &str) -> Result<i64, StoreError> {
    value
        .trim()
        .parse()
        .map_err(|_| StoreError::StatusBody(500, json!({"detail":"Invalid stream filter."})))
}
fn flag(value: Option<&String>) -> bool {
    value.is_some_and(|value| ["1", "true", "yes", "on"].contains(&value.to_lowercase().as_str()))
}
const CHAR_FILTERS: [(&str, &str); 3] = [
    ("name", "s.name"),
    ("m3u_account_name", "a.name"),
    ("tvg_id", "s.tvg_id"),
];

fn invalid_char_filters(
    params: &HashMap<String, String>,
) -> impl Iterator<Item = &'static str> + '_ {
    CHAR_FILTERS
        .into_iter()
        .map(|(field, _)| field)
        .filter(|field| params.get(*field).is_some_and(|value| value.contains('\0')))
}

fn char_filter_value(value: &str) -> Option<&str> {
    // Django CharFilter strips Python whitespace before checking for empty.
    let value = value.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'));
    (!value.is_empty()).then_some(value)
}

pub(crate) fn filters(
    query: &mut QueryBuilder<'_, Postgres>,
    params: &HashMap<String, String>,
) -> Result<(), StoreError> {
    if let Some(assigned) = params.get("assigned") {
        query.push(" AND EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs WHERE cs.stream_id=s.id AND cs.channel_id=").push_bind(number(assigned)?).push(")");
    }
    if flag(params.get("unassigned")) {
        query.push(" AND NOT EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs WHERE cs.stream_id=s.id)");
    }
    for (param, condition) in [
        ("hide_stale", " AND NOT s.is_stale"),
        ("is_catchup", " AND s.is_catchup"),
        ("is_radio", " AND s.is_radio"),
    ] {
        if flag(params.get(param)) {
            query.push(condition);
        }
    }
    // Normal FilterBackend validation aggregates every invalid CharField
    // in declaration order before Django applies the valid predicates.
    let errors: Map<String, Value> = invalid_char_filters(params)
        .map(|field| {
            (
                field.to_owned(),
                json!(["Null characters are not allowed."]),
            )
        })
        .collect();
    if !errors.is_empty() {
        return Err(StoreError::InvalidInput(Value::Object(errors)));
    }
    for (param, column) in CHAR_FILTERS {
        if let Some(value) = params.get(param).and_then(|value| char_filter_value(value)) {
            query
                .push(" AND UPPER(")
                .push(column)
                .push(") LIKE UPPER(")
                .push_bind(like(value))
                .push(")");
        }
    }
    for param in ["channel_group", "channel_group_name"] {
        if let Some(value) = params.get(param).filter(|value| !value.is_empty()) {
            query
                .push(" AND g.name=ANY(")
                .push_bind(value.split(',').map(str::to_owned).collect::<Vec<_>>())
                .push(")");
        }
    }
    if let Some(value) = params.get("m3u_account").filter(|value| !value.is_empty()) {
        let ids = value
            .split(',')
            .map(number)
            .collect::<Result<Vec<_>, _>>()?;
        query
            .push(" AND s.m3u_account_id=ANY(")
            .push_bind(ids)
            .push(")");
    }
    if let Some(value) = params.get("m3u_account_is_active") {
        match value.to_lowercase().as_str() {
            "true" | "1" => {
                query.push(" AND a.is_active");
            }
            "false" | "0" => {
                query.push(" AND NOT a.is_active");
            }
            _ => {}
        }
    }
    if let Some(search) = params.get("search").filter(|value| !value.is_empty()) {
        if search.contains('\0') {
            return Err(StoreError::InvalidInput(json!([
                "Null characters are not allowed."
            ])));
        }
        for term in search_terms(search) {
            query
                .push(" AND (UPPER(s.name) LIKE UPPER(")
                .push_bind(like(&term))
                .push(") OR UPPER(g.name) LIKE UPPER(")
                .push_bind(like(&term))
                .push("))");
        }
    }
    Ok(())
}
fn ordering(query: &mut QueryBuilder<'_, Postgres>, params: &HashMap<String, String>) {
    query.push(" ORDER BY ").push(ordering_sql(params));
}

pub(crate) fn ordering_sql(params: &HashMap<String, String>) -> String {
    let clauses = params
        .get("ordering")
        .map(String::as_str)
        .unwrap_or("")
        .split(',')
        .filter_map(|value| {
            let value = value.trim();
            let desc = value.starts_with('-');
            let column = match value.trim_start_matches('-') {
                "name" => "s.name",
                "channel_group__name" => "g.name",
                "m3u_account__name" => "a.name",
                "tvg_id" => "s.tvg_id",
                _ => return None,
            };
            Some(format!("{column} {}", if desc { "DESC" } else { "ASC" }))
        })
        .collect::<Vec<_>>();
    if clauses.is_empty() {
        "s.name DESC".into()
    } else {
        clauses.join(",")
    }
}
pub(crate) fn page(
    params: &HashMap<String, String>,
    count: usize,
) -> Result<(usize, usize, usize), StoreError> {
    let size = params
        .get("page_size")
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(50)
        .min(10000);
    let pages = count.div_ceil(size).max(1);
    let page = if params.get("page").is_some_and(|value| value == "last") {
        pages
    } else {
        params
            .get("page")
            .filter(|value| !value.is_empty())
            .map(|value| value.trim().parse::<usize>())
            .transpose()
            .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Invalid page."})))?
            .unwrap_or(1)
    };
    if page == 0 || page > pages {
        return Err(StoreError::StatusBody(
            404,
            json!({"detail":"Invalid page."}),
        ));
    }
    Ok((page, size, pages))
}
pub(crate) fn previous(base: &Url, page: usize) -> Option<String> {
    if page <= 1 {
        return None;
    }
    if page > 2 {
        return Some(page_url(base, page - 1));
    }
    let mut url = base.clone();
    let mut pairs = url
        .query_pairs()
        .filter(|(key, _)| key != "page")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    url.query_pairs_mut().clear().extend_pairs(pairs);
    if url.query() == Some("") {
        url.set_query(None);
    }
    Some(url.to_string())
}
async fn read(
    auth: AuthService,
    user: User,
    params: HashMap<String, String>,
    base: Url,
    detail: Option<i64>,
    only_ids: bool,
) -> Result<Json<Value>, StoreError> {
    if detail.is_none() && !only_ids {
        if let Some(ids) = params.get("ids").filter(|value| !value.is_empty()) {
            // Original list override bypasses active-account, filter and pagination
            // predicates and uses Stream.Meta.updated_at ordering.
            let ids = ids.split(',').map(number).collect::<Result<Vec<_>, _>>()?;
            let rows =
                selected_streams(auth.store.pool(), &ids, user.user_level >= 10, &base).await?;
            if rows.is_empty() {
                return Err(missing());
            }
            return Ok(Json(Value::Array(rows)));
        }
    }
    let count = if detail.is_none() && !only_ids {
        let mut query = QueryBuilder::new(format!("SELECT count(*){FROM}"));
        filters(&mut query, &params)?;
        query
            .build_query_scalar::<i64>()
            .fetch_one(auth.store.pool())
            .await? as usize
    } else {
        0
    };
    let page_info = if detail.is_none() && !only_ids {
        Some(page(&params, count)?)
    } else {
        None
    };
    let mut query = QueryBuilder::new(format!(
        "SELECT {}{FROM}",
        if only_ids {
            "s.id::bigint"
        } else {
            "to_jsonb(s)"
        }
    ));
    filters(&mut query, &params)?;
    if let Some(id) = detail {
        query.push(" AND s.id=").push_bind(id);
    }
    ordering(&mut query, &params);
    if let Some((page, size, _)) = page_info {
        query
            .push(" LIMIT ")
            .push_bind(size as i64)
            .push(" OFFSET ")
            .push_bind(((page - 1) * size) as i64);
    }
    if only_ids {
        return Ok(Json(json!(
            query
                .build_query_scalar::<i64>()
                .fetch_all(auth.store.pool())
                .await?
        )));
    }
    let values = query
        .build_query_scalar::<Value>()
        .fetch_all(auth.store.pool())
        .await?
        .into_iter()
        .map(|raw| stream(&json!({"raw":raw}), user.user_level >= 10, &base))
        .collect::<Vec<_>>();
    if detail.is_some() {
        return Ok(Json(values.into_iter().next().ok_or_else(missing)?));
    }
    let (page, _, pages) = page_info.expect("stream lists always paginated");
    Ok(Json(
        json!({"count":count,"next":if page<pages{Some(page_url(&base,page+1))}else{None},"previous":previous(&base,page),"results":values}),
    ))
}
async fn selected_streams(
    pool: &sqlx::PgPool,
    ids: &[i64],
    admin: bool,
    base: &Url,
) -> Result<Vec<Value>, StoreError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    // The two bulk lookups use Stream.objects directly and retain its model
    // ordering. The active-account and list ordering predicates do not apply.
    let mut rows = sqlx::query_scalar::<_, Value>(
        "SELECT to_jsonb(s) FROM dispatcharr_channels_stream s
         WHERE s.id = ANY($1) ORDER BY s.updated_at DESC",
    )
    .bind(ids)
    .fetch(pool);
    let mut values = Vec::new();
    while let Some(raw) = rows.try_next().await? {
        values.push(stream(&json!({"raw":raw}), admin, base));
    }
    Ok(values)
}

fn post_stream_ids(body: &Map<String, Value>) -> Result<Vec<i64>, StoreError> {
    let Some(ids) = body.get("ids") else {
        return Ok(Vec::new());
    };
    let ids = ids.as_array().ok_or_else(|| {
        StoreError::InvalidInput(json!({"error":"ids must be a list of integers"}))
    })?;
    ids.iter()
        .filter(|id| !id.is_null())
        .map(|id| {
            crate::json_compat::python_json_int(id)
                .map(|value| value.as_i64())
                .map_err(|_| StoreError::UnhandledRestFault)
        })
        .filter_map(Result::transpose)
        .collect()
}

async fn by_ids(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    request: Request,
) -> Response {
    let result = async {
        let suffix = uri.path().trim_end_matches('/').ends_with(".json");
        let indent = crate::channel_profiles::negotiate_read_json(&uri, &headers, suffix)?;
        let user = auth.account_user(peer, headers.clone(), false).await?;
        if user.user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        let ids = post_stream_ids(&crate::channels::channel_mutation_json(request).await?)?;
        let base = base_url(&headers, &uri)?;
        let rows = selected_streams(auth.store.pool(), &ids, user.user_level >= 10, &base).await?;
        crate::channel_profiles::json_response(Value::Array(rows), indent)
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        HeaderValue::from_static("POST, OPTIONS"),
    )
}

const BY_IDS_OPTIONS: &[u8; 3303] = include_bytes!("../datasets/source-stream-by-ids-options.json");

fn by_ids_options_entity(indent: usize) -> Result<Response, StoreError> {
    static RENDERED: [OnceLock<Option<Bytes>>; 9] = [const { OnceLock::new() }; 9];
    let bytes = if indent == 0 {
        Bytes::from_static(BY_IDS_OPTIONS)
    } else {
        RENDERED[indent]
            .get_or_init(|| {
                let schema: Map<String, Value> = serde_json::from_slice(BY_IDS_OPTIONS).ok()?;
                serialize_options_json(&schema, indent)
            })
            .clone()
            .ok_or(StoreError::UnhandledRestFault)?
    };
    options_json_response(bytes)
}

async fn by_ids_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let result = async {
        let indent = if uri.path().trim_end_matches('/').ends_with(".json") {
            crate::channel_profiles::negotiate_read_json(&uri, &headers, true)?.unwrap_or(0)
        } else {
            channel_options_indentation(&uri, &headers)?
        };
        // DRF names this action 'metadata', which uses the administrator
        // default rather than the POST action's Standard-user permission.
        auth.account_user(peer, headers, true).await?;
        by_ids_options_entity(indent)
    }
    .await;
    channel_options_headers(
        result.unwrap_or_else(IntoResponse::into_response),
        HeaderValue::from_static("POST, OPTIONS"),
    )
}

async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    read(auth, user, params, base_url(&headers, &uri)?, None, false).await
}
async fn ids(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    read(auth, user, params, base_url(&headers, &uri)?, None, true).await
}
async fn retrieve(
    State(auth): State<AuthService>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let id = raw
        .parse()
        .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?;
    read(
        auth,
        user,
        params,
        base_url(&headers, &uri)?,
        Some(id),
        false,
    )
    .await
}

async fn retrieve_json_alias(
    auth: State<AuthService>,
    JsonReadId(id): JsonReadId,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: OriginalUri,
    query: Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    retrieve(auth, Path(id), peer, headers, uri, query).await
}

async fn groups(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Vec<String>>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    // Original selects all distinct group names that have a stream; the
    // separate Filter Options endpoint restricts this by active account.
    let groups = sqlx::query_scalar(
        "SELECT g.name FROM dispatcharr_channels_channelgroup g \
         WHERE EXISTS(SELECT 1 FROM dispatcharr_channels_stream s WHERE s.channel_group_id=g.id) \
         ORDER BY g.name",
    )
    .fetch_all(auth.store.pool())
    .await?;
    Ok(Json(groups))
}
// Original action deliberately omits DRF SearchFilter and removes group
// filters for both option lists, then account IDs only for account choices.
async fn filter_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(mut params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers, false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let slow = [
        "name",
        "m3u_account",
        "m3u_account_name",
        "m3u_account_is_active",
        "tvg_id",
    ]
    .iter()
    .any(|key| params.get(*key).is_some_and(|value| !value.is_empty()));
    params.remove("channel_group");
    params.remove("channel_group_name");
    params.remove("search");
    // This action reads FilterSet.qs manually, so Django skips invalid fields
    // while keeping every valid one. Preserve the original slow-path choice.
    for field in invalid_char_filters(&params).collect::<Vec<_>>() {
        params.remove(field);
    }
    let mut groups = QueryBuilder::new(format!("SELECT DISTINCT g.name{FROM}"));
    if slow {
        filters(&mut groups, &params)?;
    }
    groups.push(" AND g.id IS NOT NULL ORDER BY g.name");
    let groups = groups
        .build_query_scalar::<String>()
        .fetch_all(auth.store.pool())
        .await?;
    params.remove("m3u_account");
    let mut accounts = QueryBuilder::new(format!("SELECT DISTINCT a.id::bigint,a.name{FROM}"));
    if slow {
        filters(&mut accounts, &params)?;
    }
    accounts.push(" AND a.id IS NOT NULL ORDER BY a.name");
    let accounts = accounts
        .build_query_as::<(i64, String)>()
        .fetch_all(auth.store.pool())
        .await?
        .into_iter()
        .map(|(id, name)| json!({"id":id,"name":name}))
        .collect::<Vec<_>>();
    Ok(Json(json!({"groups":groups,"m3u_accounts":accounts})))
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/channels/streams/",
            get(list)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    catalogue_options::typed_get_headers,
                ))
                .post(crate::stream_writes::create),
        )
        .route(
            "/api/channels/streams.json",
            catalogue_options::json_alias(list, catalogue_options::COLLECTION_ALLOW),
        )
        .route(
            "/api/channels/streams/ids.json",
            catalogue_options::json_alias(ids, catalogue_options::READ_ONLY_ALLOW),
        )
        .route(
            "/api/channels/streams/groups.json",
            catalogue_options::json_alias(groups, catalogue_options::READ_ONLY_ALLOW),
        )
        .route(
            "/api/channels/streams/filter-options.json",
            catalogue_options::json_alias(filter_options, catalogue_options::READ_ONLY_ALLOW),
        )
        .route(
            "/api/channels/streams/{json_id}",
            catalogue_options::json_alias(retrieve_json_alias, catalogue_options::DETAIL_ALLOW),
        )
        .route(
            "/api/channels/streams/by-ids/",
            post(by_ids).options(by_ids_options),
        )
        .route(
            "/api/channels/streams/by-ids.json",
            post(by_ids).options(by_ids_options),
        )
        .route(
            "/api/channels/streams/by-ids.json/",
            post(by_ids).options(by_ids_options),
        )
        .route("/api/channels/streams/ids/", get(ids))
        .route(
            "/api/channels/streams/groups/",
            get(groups).layer(from_fn_with_state(
                catalogue_options::READ_ONLY_ALLOW,
                catalogue_options::typed_get_headers,
            )),
        )
        .route("/api/channels/streams/filter-options/", get(filter_options))
        .route(
            "/api/channels/streams/bulk-delete/",
            delete(crate::stream_writes::bulk_delete),
        )
        .route(
            "/api/channels/streams/{id}/",
            get(retrieve)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    catalogue_options::typed_get_headers,
                ))
                .patch(crate::stream_writes::patch)
                .put(crate::stream_writes::put)
                .delete(crate::stream_writes::delete),
        )
        .with_state(auth)
}

#[cfg(test)]
mod by_ids_tests {
    use super::*;

    #[test]
    fn post_stream_ids_retains_django_values_and_error_shapes() {
        fn decode(input: Value) -> Result<Vec<i64>, StoreError> {
            post_stream_ids(input.as_object().unwrap())
        }
        assert_eq!(decode(json!({})).unwrap(), Vec::<i64>::new());
        assert_eq!(decode(json!({"ids":[]})).unwrap(), Vec::<i64>::new());
        assert_eq!(
            decode(json!({"ids":[1,2,"٢",2.9,true,null,9999]})).unwrap(),
            [1, 2, 2, 2, 1, 9999]
        );
        assert!(matches!(
            decode(json!({"ids":"1,2"})),
            Err(StoreError::InvalidInput(value)) if value == json!({"error":"ids must be a list of integers"})
        ));
        assert!(matches!(
            decode(json!({"ids":[1,"missing"]})),
            Err(StoreError::UnhandledRestFault)
        ));
    }

    #[tokio::test]
    async fn post_stream_ids_authenticates_before_reading_json_on_all_bound_routes() {
        use axum::{
            body::{to_bytes, Body},
            http::{header, Method, StatusCode},
        };
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("owned-stream-lookup-test".into()).unwrap(),
        );
        let app = router(auth);
        for uri in [
            "/api/channels/streams/by-ids/",
            "/api/channels/streams/by-ids.json",
            "/api/channels/streams/by-ids.json/",
        ] {
            let req = Request::builder()
                .method(Method::POST)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9192))))
                .body(Body::from("[1,2]"))
                .unwrap();
            let reply = app.clone().oneshot(req).await.unwrap();
            assert_eq!(reply.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(reply.headers()[header::ALLOW], "POST, OPTIONS");
            assert_eq!(reply.headers()[header::VARY], "Accept");
            let body = to_bytes(reply.into_body(), 128).await.unwrap();
            assert_eq!(
                body,
                r#"{"detail":"Authentication credentials were not provided."}"#
            );
        }
    }

    #[tokio::test]
    async fn options_stream_ids_matches_observed_metadata_bytes() {
        use axum::{body::to_bytes, http::header};
        use sha2::{Digest, Sha256};

        for (indent, length, digest) in [
            (
                0,
                3303,
                "d285ffe5952d25852b72d72db60e1eb06c90b47bcd65dab79faf73bbd3a73bd3",
            ),
            (
                2,
                4844,
                "f5665d52470e42e962a3c0ca837abc0b4b733b4a696367bcfce07bc057e9b6b3",
            ),
        ] {
            let reply = by_ids_options_entity(indent).unwrap();
            assert_eq!(reply.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(reply.headers()[header::CONTENT_LENGTH], length.to_string());
            let body = to_bytes(reply.into_body(), length).await.unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(body)), digest);
        }
    }

    #[tokio::test]
    async fn options_stream_ids_routes_enforce_authentication_and_suffix_negotiation() {
        use axum::{
            body::{to_bytes, Body},
            http::{header, Method, StatusCode},
        };
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("owned-stream-metadata-test".into()).unwrap(),
        );
        let app = router(auth);
        for (uri, accept, expected, detail) in [
            (
                "/api/channels/streams/by-ids/",
                "application/json",
                StatusCode::UNAUTHORIZED,
                "Authentication credentials were not provided.",
            ),
            (
                "/api/channels/streams/by-ids.json",
                "application/json",
                StatusCode::UNAUTHORIZED,
                "Authentication credentials were not provided.",
            ),
            (
                "/api/channels/streams/by-ids.json/",
                "application/json",
                StatusCode::UNAUTHORIZED,
                "Authentication credentials were not provided.",
            ),
            (
                "/api/channels/streams/by-ids.json",
                "text/html",
                StatusCode::NOT_ACCEPTABLE,
                "Could not satisfy the request Accept header.",
            ),
            (
                "/api/channels/streams/by-ids.json/",
                "text/html",
                StatusCode::NOT_ACCEPTABLE,
                "Could not satisfy the request Accept header.",
            ),
        ] {
            let request = Request::builder()
                .method(Method::OPTIONS)
                .uri(uri)
                .header(header::ACCEPT, accept)
                .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 9192))))
                .body(Body::empty())
                .unwrap();
            let reply = app.clone().oneshot(request).await.unwrap();
            assert_eq!(reply.status(), expected);
            assert_eq!(reply.headers()[header::ALLOW], "POST, OPTIONS");
            assert_eq!(reply.headers()[header::VARY], "Accept");
            let body = to_bytes(reply.into_body(), 128).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                json!({ "detail": detail })
            );
        }
    }

    #[tokio::test]
    #[ignore = "requires an owned disposable Unix-only PostgreSQL database"]
    async fn selected_streams_preserves_bulk_order_and_projection() {
        use sha2::{Digest, Sha256};
        use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

        let socket = std::env::var("DISPATCHARR_OWNED_BULK_STREAM_SOCKET").unwrap();
        assert!(socket.starts_with("/tmp/dispatcharr-stream-bulk-") && socket.ends_with("/socket"));
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                PgConnectOptions::new()
                    .socket(&socket)
                    .port(54941)
                    .username("fixture_owner")
                    .database("postgres"),
            )
            .await
            .unwrap();
        assert!(sqlx::query_scalar::<_, bool>(
            "SELECT inet_server_addr() IS NULL AND current_setting('listen_addresses') = ''",
        )
        .fetch_one(&pool)
        .await
        .unwrap());
        sqlx::raw_sql(r#"
            SET TIME ZONE 'UTC';
            CREATE TEMP TABLE dispatcharr_channels_stream(
                id bigint PRIMARY KEY, name text, url text, m3u_account_id bigint, logo_url text,
                tvg_id text, local_file text, current_viewers integer DEFAULT 0,
                updated_at timestamptz, last_seen timestamptz, is_stale boolean DEFAULT false,
                is_adult boolean DEFAULT false, stream_profile_id bigint, is_custom boolean DEFAULT false,
                channel_group_id bigint, stream_hash text, stream_stats jsonb, stream_stats_updated_at timestamptz,
                stream_id integer, stream_chno double precision, is_catchup boolean DEFAULT false,
                catchup_days integer DEFAULT 0, is_radio boolean DEFAULT false
            );
            INSERT INTO dispatcharr_channels_stream
                (id, name, url, m3u_account_id, updated_at, last_seen, stream_stats)
            SELECT i, 'Owned ' || lpad(i::text, 2, '0'),
                'http://owned.invalid/media-' || i || '.ts', CASE WHEN i=2 THEN 2 ELSE 1 END,
                '2026-02-01T12:00:00Z'::timestamptz + i * interval '1 second',
                '2026-02-01T12:00:00Z'::timestamptz, CASE WHEN i=2 THEN '{"width":640}'::jsonb END
            FROM generate_series(1,52) i;
        "#).execute(&pool).await.unwrap();
        let base = Url::parse("http://testserver/api/channels/streams/by-ids/").unwrap();
        let ids = (1..=51).collect::<Vec<_>>();
        for (admin, expected) in [
            (
                true,
                "f0589707896b1c3e2f853e05c4a14276d2b218af7d0a9bd578a27b39805a5019",
            ),
            (
                false,
                "18955316795b44e109f0aa18c557c37e53b43b14e36535dfc1d08504db459423",
            ),
        ] {
            let rows = selected_streams(&pool, &ids, admin, &base).await.unwrap();
            let bytes = serde_json::to_vec(&rows).unwrap();
            assert_eq!(format!("{:x}", Sha256::digest(bytes)), expected);
        }
        assert!(selected_streams(&pool, &[], false, &base)
            .await
            .unwrap()
            .is_empty());
        assert!(selected_streams(&pool, &[9999], false, &base)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatcharr_channels_stream")
                .fetch_one(&pool)
                .await
                .unwrap(),
            52
        );
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;

    #[test]
    fn source_char_filters_strip_only_the_outer_python_whitespace() {
        for (raw, expected) in [
            ("Alpha", Some("Alpha")),
            ("\u{a0}Alpha\t  ", Some("Alpha")),
            ("\u{1e}Alpha\u{1f}", Some("Alpha")),
            ("\u{2003} \t", None),
            ("  Alpha  News ", Some("Alpha  News")),
            ("\tOwned Source \u{2003}", Some("Owned Source")),
            (" news.1\t ", Some("news.1")),
            ("", None),
        ] {
            assert_eq!(char_filter_value(raw), expected);
        }
    }

    #[test]
    fn source_char_filter_errors_are_aggregated_in_declaration_order() {
        let params = HashMap::from([
            ("tvg_id".into(), "news.1\0".into()),
            ("m3u_account_name".into(), " Owned\0 Source ".into()),
            ("name".into(), "\0Alpha".into()),
        ]);
        let mut query = QueryBuilder::<Postgres>::new("SELECT 1 WHERE true");
        let Err(StoreError::InvalidInput(errors)) = filters(&mut query, &params) else {
            panic!("invalid text must fail before SQL execution");
        };
        assert_eq!(
            errors.to_string(),
            r#"{"name":["Null characters are not allowed."],"m3u_account_name":["Null characters are not allowed."],"tvg_id":["Null characters are not allowed."]}"#
        );
        assert_eq!(query.sql(), "SELECT 1 WHERE true");
    }

    #[test]
    fn source_filter_options_can_ignore_invalid_fields_and_keep_the_valid_predicate() {
        let mut params = HashMap::from([
            ("name".into(), "Alpha\0".into()),
            ("tvg_id".into(), " sports.2 ".into()),
        ]);
        let invalid: Vec<_> = invalid_char_filters(&params).collect();
        assert_eq!(invalid, ["name"]);
        for field in invalid {
            params.remove(field);
        }
        let mut query = QueryBuilder::<Postgres>::new("SELECT 1 WHERE true");
        filters(&mut query, &params).unwrap();
        assert_eq!(
            query.sql(),
            "SELECT 1 WHERE true AND UPPER(s.tvg_id) LIKE UPPER($1)"
        );
    }

    #[test]
    fn source_char_filter_empty_is_absent_but_exact_group_filter_is_still_applied() {
        let sql = |params: &[(&str, &str)]| {
            let params = params
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            let mut query = QueryBuilder::<Postgres>::new("SELECT 1 WHERE true");
            filters(&mut query, &params).unwrap();
            query.sql().to_owned()
        };
        for (param, column) in [
            ("name", "s.name"),
            ("m3u_account_name", "a.name"),
            ("tvg_id", "s.tvg_id"),
        ] {
            assert_eq!(sql(&[(param, " \u{a0}\t")]), "SELECT 1 WHERE true");
            assert_eq!(
                sql(&[(param, " value ")]),
                format!("SELECT 1 WHERE true AND UPPER({column}) LIKE UPPER($1)")
            );
        }
        for param in ["channel_group", "channel_group_name"] {
            assert_eq!(
                sql(&[(param, " Owned News Group ")]),
                "SELECT 1 WHERE true AND g.name=ANY($1)"
            );
        }
    }
}
