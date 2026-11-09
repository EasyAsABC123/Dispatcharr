//! Logo read API derived from original Dispatcharr views and serializers.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    channels::{like, page_url},
    AuthService, StoreError,
};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, State},
    http::{HeaderMap, Uri},
    routing::get,
    Json, Router,
};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::OnceLock,
    time::UNIX_EPOCH,
};
use url::Url;
const FROM: &str = " FROM dispatcharr_channels_logo l WHERE true";
pub(crate) fn missing() -> StoreError {
    StoreError::StatusBody(404, json!({"detail":"Not found."}))
}
pub(crate) fn params(uri: &Uri) -> Vec<(String, String)> {
    url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}
// .isdigit() includes non-decimal superscripts; int() then raises and the
// original queryset becomes empty. Negative and wholly invalid IDs are ignored.
fn ids(values: impl Iterator<Item = String>) -> Option<Vec<String>> {
    static DIGITS: OnceLock<HashMap<u32, Option<u8>>> = OnceLock::new();
    let digits = DIGITS.get_or_init(|| {
        serde_json::from_str::<Vec<(u32, Option<u8>)>>(include_str!(
            "../datasets/python-digits-15-1.json"
        ))
        .expect("pinned original digit table")
        .into_iter()
        .collect()
    });
    let mut out = Vec::new();
    for raw in values {
        if raw.is_empty() || !raw.chars().all(|c| digits.contains_key(&(c as u32))) {
            continue;
        }
        let mut normalized = String::new();
        for c in raw.chars() {
            normalized.push((b'0' + (*digits.get(&(c as u32))?)?) as char);
        }
        if normalized.len() > 4300 {
            return None;
        }
        let normalized = normalized.trim_start_matches('0');
        out.push(if normalized.is_empty() {
            "0".into()
        } else {
            normalized.into()
        });
    }
    Some(out)
}
pub(crate) fn filters(q: &mut QueryBuilder<'_, Postgres>, pairs: &[(String, String)]) {
    match ids(pairs
        .iter()
        .filter(|(key, _)| key == "ids")
        .map(|(_, value)| value.clone()))
    {
        None => {
            q.push(" AND false");
        }
        Some(ids) if !ids.is_empty() => {
            q.push(" AND l.id::text=ANY(").push_bind(ids).push(")");
        }
        _ => {}
    }
    let value = |key| {
        pairs
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    if let Some(used @ ("true" | "false")) = value("used") {
        q.push(if used == "true" {
            " AND EXISTS"
        } else {
            " AND NOT EXISTS"
        })
        .push("(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id)");
    }
    if let Some(name) = value("name").filter(|value| !value.is_empty()) {
        q.push(" AND UPPER(l.name) LIKE UPPER(")
            .push_bind(like(name))
            .push(")");
    }
}
// Same trusted-socket forwarding rules as core.utils.build_absolute_uri_with_port.
pub(crate) fn origin(auth: &AuthService, peer: IpAddr, headers: &HeaderMap) -> String {
    let identity = crate::auth::request(SocketAddr::new(peer, 0), headers.clone());
    let header = |key| identity.header(key).filter(|value| !value.is_empty());
    let trusted = auth.is_trusted_proxy(peer);
    let scheme = if trusted {
        header("x-forwarded-proto")
            .and_then(|v| v.split(',').next())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("http")
    } else {
        "http"
    };
    let standard = if scheme == "https" { "443" } else { "80" };
    let with_port = |host: &str, port: &str| {
        if port == standard {
            format!("{scheme}://{host}")
        } else {
            format!("{scheme}://{host}:{port}")
        }
    };
    if trusted {
        if let Some(host) = header("x-forwarded-host") {
            if let Some((host, port)) = host.split_once(':') {
                return with_port(host, port);
            }
            if let Some(port) = header("x-forwarded-port") {
                return with_port(host, port);
            }
            if header("x-forwarded-proto").is_some() {
                return format!("{scheme}://{host}");
            }
        }
    }
    let host = header("host").unwrap_or("localhost");
    if let Some((host, port)) = host.split_once(':') {
        return with_port(host, port);
    }
    if trusted {
        if let Some(port) = header("x-forwarded-port") {
            return with_port(host, port);
        }
    }
    format!("{scheme}://{host}")
}
pub(crate) fn serialize(mut row: Value, origin: &str) -> Value {
    let url = row["url"].as_str().unwrap_or("");
    let mut cache_key = url.to_owned();
    if url.starts_with("/data/logos") {
        if let Ok(mtime) = std::fs::metadata(url).and_then(|m| m.modified()) {
            let seconds = match mtime.duration_since(UNIX_EPOCH) {
                Ok(v) => v.as_secs_f64(),
                Err(v) => -v.duration().as_secs_f64(),
            };
            cache_key.push(':');
            cache_key.push_str(&crate::json_compat::float_string(seconds));
        }
    }
    let hash = format!("{:x}", Md5::digest(cache_key.as_bytes()));
    row["cache_url"] = json!(format!(
        "{origin}/api/channels/logos/{}/cache/?v={}",
        row["id"],
        &hash[..8]
    ));
    let count = row["channel_count"].as_i64().unwrap_or(0);
    row["is_used"] = json!(count > 0);
    if count > 5 {
        row["channel_names"]
            .as_array_mut()
            .expect("SQL array")
            .push(json!(format!("...and {} more", count - 5)));
    }
    row
}
pub(crate) async fn lookup(auth: &AuthService, raw: &str, uri: &Uri) -> Result<Value, StoreError> {
    let id = raw.parse::<i64>().map_err(|_| missing())?;
    let mut query=QueryBuilder::new(format!("SELECT json_build_object('id',l.id,'name',l.name,'url',l.url,'channel_count',(SELECT count(*) FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id),'channel_names',COALESCE((SELECT json_agg('Channel: '||n.name) FROM (SELECT c.name FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id LIMIT 5)n),'[]'::json)){FROM}"));
    filters(&mut query, &params(uri));
    query.push(" AND l.id=").push_bind(id);
    query
        .build_query_scalar::<Value>()
        .fetch_optional(auth.store.pool())
        .await?
        .ok_or_else(|| {
            StoreError::StatusBody(404, json!({"detail":"No Logo matches the given query."}))
        })
}
pub(crate) async fn serialized(
    auth: &AuthService,
    id: i64,
    peer: IpAddr,
    headers: &HeaderMap,
) -> Result<Value, StoreError> {
    let row = lookup(
        auth,
        &id.to_string(),
        &Uri::from_static("/api/channels/logos/"),
    )
    .await?;
    let origin = origin(auth, peer, headers);
    tokio::task::spawn_blocking(move || serialize(row, &origin))
        .await
        .map_err(|_| StoreError::StatusBody(503, json!({"detail":"Logo metadata unavailable."})))
}
async fn read(
    auth: AuthService,
    peer: SocketAddr,
    headers: HeaderMap,
    uri: Uri,
    detail: Option<String>,
) -> Result<Json<Value>, StoreError> {
    let user = auth.account_user(peer, headers.clone(), false).await?;
    if user.user_level < 1 {
        return Err(StoreError::PermissionDenied);
    }
    let pairs = params(&uri);
    let values = pairs.iter().cloned().collect::<HashMap<_, _>>();
    let detail = detail
        .map(|v| v.parse::<i64>().map_err(|_| missing()))
        .transpose()?;
    let no_pagination =
        detail.is_some() || values.get("no_pagination").is_some_and(|v| v == "true");
    let mut count = QueryBuilder::new(format!("SELECT count(*){FROM}"));
    filters(&mut count, &pairs);
    if let Some(id) = detail {
        count.push(" AND l.id=").push_bind(id);
    }
    let count = count
        .build_query_scalar::<i64>()
        .fetch_one(auth.store.pool())
        .await? as usize;
    if detail.is_some() && count == 0 {
        return Err(StoreError::StatusBody(
            404,
            json!({"detail":"No Logo matches the given query."}),
        ));
    }
    let size = values
        .get("page_size")
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(50)
        .min(1000);
    let pages = count.div_ceil(size).max(1);
    let page = if no_pagination {
        1
    } else if values.get("page").is_some_and(|v| v == "last") {
        pages
    } else {
        values
            .get("page")
            .filter(|v| !v.is_empty())
            .map(|v| v.trim().parse::<usize>())
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
    let mut query=QueryBuilder::new(format!("SELECT json_build_object('id',l.id,'name',l.name,'url',l.url,'channel_count',(SELECT count(*) FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id),'channel_names',COALESCE((SELECT json_agg('Channel: '||n.name) FROM (SELECT c.name FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id LIMIT 5)n),'[]'::json)){FROM}"));
    filters(&mut query, &pairs);
    if let Some(id) = detail {
        query.push(" AND l.id=").push_bind(id);
    }
    query.push(" ORDER BY l.name");
    if !no_pagination {
        query
            .push(" LIMIT ")
            .push_bind(size as i64)
            .push(" OFFSET ")
            .push_bind(((page - 1) * size) as i64);
    }
    let rows = query
        .build_query_scalar::<Value>()
        .fetch_all(auth.store.pool())
        .await?;
    let origin = origin(&auth, peer.ip(), &headers);
    let base = Url::parse(&format!("{origin}{uri}")).map_err(|_| missing())?;
    // Filesystem metadata is evaluated on a blocking worker, away from HTTP IO.
    let rows = tokio::task::spawn_blocking(move || {
        rows.into_iter()
            .map(|v| serialize(v, &origin))
            .collect::<Vec<_>>()
    })
    .await
    .map_err(|_| StoreError::StatusBody(503, json!({"detail":"Logo metadata unavailable."})))?;
    if detail.is_some() {
        return Ok(Json(rows.into_iter().next().ok_or_else(missing)?));
    }
    if no_pagination {
        return Ok(Json(json!(rows)));
    }
    let next = if page < pages {
        Some(page_url(&base, page + 1))
    } else {
        None
    };
    let previous = if page > 2 {
        Some(page_url(&base, page - 1))
    } else if page == 2 {
        let mut previous = base.clone();
        let mut pairs = previous
            .query_pairs()
            .filter(|(k, _)| k != "page")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect::<Vec<_>>();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        previous.query_pairs_mut().clear().extend_pairs(pairs);
        if previous.query() == Some("") {
            previous.set_query(None);
        }
        Some(previous.to_string())
    } else {
        None
    };
    Ok(Json(
        json!({"count":count,"next":next,"previous":previous,"results":rows}),
    ))
}
async fn list(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Json<Value>, StoreError> {
    read(auth, peer, headers, uri, None).await
}
async fn retrieve(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Path(id): Path<String>,
) -> Result<Json<Value>, StoreError> {
    read(auth, peer, headers, uri, Some(id)).await
}
pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/channels/logos/",
            get(list).post(crate::logo_writes::create),
        )
        .route(
            "/api/channels/logos/{id}/",
            get(retrieve)
                .patch(crate::logo_writes::patch)
                .put(crate::logo_writes::put)
                .delete(crate::logo_writes::delete),
        )
        .route(
            "/api/channels/logos/{id}/cache/",
            get(crate::image_proxy::cache),
        )
        .route(
            "/api/channels/logos/{id}/cache",
            get(crate::image_proxy::cache),
        )
        .route(
            "/api/channels/logos/upload/",
            axum::routing::post(crate::logo_writes::upload)
                .layer(axum::extract::DefaultBodyLimit::max(8 * 1024 * 1024)),
        )
        .with_state(auth)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_id_filter_ignores_invalid_but_digit_conversion_failure_empties() {
        let parse = |v: &[&str]| ids(v.iter().map(|v| (*v).to_owned()));
        assert_eq!(parse(&["abc", "-1", ""]), Some(vec![]));
        assert_eq!(parse(&["٠٠١", "42"]), Some(vec!["1".into(), "42".into()]));
        assert_eq!(parse(&["²", "42"]), None);
        assert_eq!(parse(&["000"]), Some(vec!["0".into()]));
    }
}
