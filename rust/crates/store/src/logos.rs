//! Logo read API derived from original Dispatcharr views and serializers.
//! Original authors and contributors retain full credit; see ATTRIBUTION.md.
use crate::{
    catalogue_options,
    channels::{like, page_url},
    core_reads::JsonReadId,
    AuthService, StoreError,
};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, State},
    http::{HeaderMap, Uri},
    middleware::from_fn_with_state,
    routing::get,
    Json, Router,
};
use md5::{Digest, Md5};
use num_bigint::BigInt;
use serde_json::{json, Map, Value};
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
pub(crate) fn ids(values: impl Iterator<Item = String>) -> Option<Vec<String>> {
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
fn serialize_read(row: Value, origin: &str) -> Value {
    let mut row = serialize(row, origin);
    let fields = row.as_object_mut().expect("SQL logo object");
    let mut ordered = Map::with_capacity(fields.len());
    for name in [
        "id",
        "name",
        "url",
        "cache_url",
        "channel_count",
        "is_used",
        "channel_names",
    ] {
        if let Some((key, value)) = fields.shift_remove_entry(name) {
            ordered.insert(key, value);
        }
    }
    ordered.extend(std::mem::take(fields));
    Value::Object(ordered)
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
pub(crate) fn page_window(
    values: &HashMap<String, String>,
    count: usize,
) -> Result<(usize, usize, usize), StoreError> {
    use crate::json_compat::python_int_string;
    let invalid_page = || StoreError::StatusBody(404, json!({"detail":"Invalid page."}));
    let size = values
        .get("page_size")
        .and_then(|value| python_int_string(value).ok())
        .and_then(|value| usize::try_from(value.min(BigInt::from(1000))).ok())
        .filter(|&value| value != 0)
        .unwrap_or(50);
    let pages = count.div_ceil(size).max(1);
    let number = match values.get("page").map(String::as_str) {
        Some("last") => pages,
        None | Some("") => 1,
        Some(raw) => {
            let number = python_int_string(raw).map_err(|_| invalid_page())?;
            usize::try_from(number).map_err(|_| invalid_page())?
        }
    };
    if number == 0 || number > pages {
        return Err(invalid_page());
    }
    Ok((number, size, pages))
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
    let (page, size, pages) = if no_pagination {
        (1, 50, 1)
    } else {
        page_window(&values, count)?
    };
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
            .map(|v| serialize_read(v, &origin))
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
    let previous = crate::streams::previous(&base, page);
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

async fn retrieve_json_alias(
    auth: State<AuthService>,
    peer: ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: OriginalUri,
    JsonReadId(id): JsonReadId,
) -> Result<Json<Value>, StoreError> {
    retrieve(auth, peer, headers, uri, Path(id)).await
}

pub(crate) fn router(auth: AuthService) -> Router {
    Router::new()
        .route(
            "/api/channels/logos/",
            get(list)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    catalogue_options::typed_get_headers,
                ))
                .post(crate::logo_writes::create),
        )
        .route(
            "/api/channels/logos.json",
            catalogue_options::json_alias(list, catalogue_options::COLLECTION_ALLOW),
        )
        .route(
            "/api/channels/logos.json/",
            catalogue_options::json_alias(list, catalogue_options::COLLECTION_ALLOW),
        )
        .route(
            "/api/channels/logos/{json_id}",
            catalogue_options::json_alias(retrieve_json_alias, catalogue_options::DETAIL_ALLOW),
        )
        .route(
            "/api/channels/logos/bulk-delete/",
            axum::routing::delete(crate::logo_writes::bulk_delete),
        )
        .route(
            "/api/channels/logos/cleanup/",
            axum::routing::post(crate::logo_writes::cleanup_unused),
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
    #[tokio::test]
    async fn logo_collection_json_accepts_optional_slash_without_expanding_methods() {
        use axum::{
            body::Body,
            http::{header, Method, Request, StatusCode},
            Extension,
        };
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;

        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("logo-list-alias-fixture".into()).unwrap(),
        );
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let router = router(auth).layer(Extension(ConnectInfo(peer)));
        for path in ["/api/channels/logos.json", "/api/channels/logos.json/"] {
            for (method, accept, expected) in [
                (Method::GET, "application/json", StatusCode::UNAUTHORIZED),
                (Method::GET, "text/html", StatusCode::NOT_ACCEPTABLE),
                (Method::POST, "application/json", StatusCode::NOT_FOUND),
            ] {
                let request = Request::builder()
                    .method(method)
                    .uri(format!("{path}?used=true"))
                    .header(header::ACCEPT, accept)
                    .body(Body::empty())
                    .unwrap();
                let response = router.clone().oneshot(request).await.unwrap();
                assert_eq!(response.status(), expected, "{path} {accept}");
                assert!(!response.headers().contains_key(header::SET_COOKIE));
                if expected == StatusCode::UNAUTHORIZED {
                    assert_eq!(
                        response.headers()[header::WWW_AUTHENTICATE],
                        "Bearer realm=\"api\""
                    );
                    let body = axum::body::to_bytes(response.into_body(), 512)
                        .await
                        .unwrap();
                    assert_eq!(
                        format!("{:x}", Sha256::digest(body)),
                        "dbff0849bc109eee7f9b7b1fe12e40fc8add21823b2f1fb600b37f620cbd7862",
                    );
                }
            }
        }
    }

    #[test]
    fn logo_read_orders_cache_and_usage_fields_in_detail_and_paginated_entity() {
        let payload = json!({
            "id":1,"name":"Owned","url":"","channel_count":1,"channel_names":["Channel: Owned"],
        });
        let detail = serialize_read(payload.clone(), "http://localhost");
        let row = r#"{"id":1,"name":"Owned","url":"","cache_url":"http://localhost/api/channels/logos/1/cache/?v=d41d8cd9","channel_count":1,"is_used":true,"channel_names":["Channel: Owned"]}"#;
        assert_eq!(serde_json::to_string(&detail).unwrap(), row);
        let list = json!({"count":1,"next":null,"previous":null,"results":[detail]});
        let expected = format!(r#"{{"count":1,"next":null,"previous":null,"results":[{row}]}}"#);
        assert_eq!(serde_json::to_string(&list).unwrap(), expected);
        let original_serialization = serialize(payload, "http://localhost");
        assert_eq!(original_serialization, list["results"][0]);
        assert_ne!(serde_json::to_string(&original_serialization).unwrap(), row);
    }

    #[test]
    fn postgres_logo_pagination_accepts_original_integer_forms() {
        let options = |query| {
            params(&format!("/api/channels/logos/?{query}").parse().unwrap())
                .into_iter()
                .collect()
        };
        for query in [
            "page=%D9%A2&page_size=1_0",
            "page=%2B2&page_size=%20%2B10%20",
            "page=last&page=2&page_size=3&page_size=10",
        ] {
            assert_eq!(page_window(&options(query), 51).unwrap(), (2, 10, 6));
        }
        assert_eq!(
            page_window(&options("page=last&page_size=999999"), 2030).unwrap(),
            (3, 1000, 3)
        );
        assert!(matches!(
            page_window(&options("page=1__0"), 51),
            Err(StoreError::StatusBody(404, _))
        ));
    }

    #[test]
    fn original_id_filter_ignores_invalid_but_digit_conversion_failure_empties() {
        let parse = |v: &[&str]| ids(v.iter().map(|v| (*v).to_owned()));
        assert_eq!(parse(&["abc", "-1", ""]), Some(vec![]));
        assert_eq!(parse(&["٠٠١", "42"]), Some(vec!["1".into(), "42".into()]));
        assert_eq!(parse(&["²", "42"]), None);
        assert_eq!(parse(&["000"]), Some(vec!["0".into()]));
    }
}
