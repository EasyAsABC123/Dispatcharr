//! Stream management read contracts from original Dispatcharr views/serializers.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    channels::{base_url, like, page_url, search_terms, stream},
    AuthService, StoreError, User,
};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, Query, State},
    http::HeaderMap,
    routing::{delete, get},
    Json, Router,
};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::{collections::HashMap, net::SocketAddr};
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
    for (param, column) in [
        ("name", "s.name"),
        ("m3u_account_name", "a.name"),
        ("tvg_id", "s.tvg_id"),
    ] {
        if let Some(value) = params.get(param).filter(|value| !value.is_empty()) {
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
    query.push(" ORDER BY ").push(if clauses.is_empty() {
        "s.name DESC".into()
    } else {
        clauses.join(",")
    });
}
fn page(
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
fn previous(base: &Url, page: usize) -> Option<String> {
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
            let rows=sqlx::query_scalar::<_,Value>("SELECT to_jsonb(s) FROM dispatcharr_channels_stream s WHERE s.id=ANY($1) ORDER BY s.updated_at DESC").bind(ids).fetch_all(auth.store.pool()).await?;
            if rows.is_empty() {
                return Err(missing());
            }
            return Ok(Json(Value::Array(
                rows.into_iter()
                    .map(|raw| stream(&json!({"raw":raw}), user.user_level >= 10, &base))
                    .collect(),
            )));
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
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/channels/streams/",
            get(list).post(crate::stream_writes::create),
        )
        .route("/api/channels/streams/ids/", get(ids))
        .route(
            "/api/channels/streams/bulk-delete/",
            delete(crate::stream_writes::bulk_delete),
        )
        .route(
            "/api/channels/streams/{id}/",
            get(retrieve)
                .patch(crate::stream_writes::patch)
                .put(crate::stream_writes::put)
                .delete(crate::stream_writes::delete),
        )
        .with_state(auth)
}
