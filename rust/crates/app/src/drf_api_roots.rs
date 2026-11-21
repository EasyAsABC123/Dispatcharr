//! Ordinary JSON directories and captured bare admin HTML contexts from the pinned Original.
//! Original Dispatcharr and Django REST Framework authors retain full credit.
use super::combined_statistics_wire::{self as wire, Output};
use axum::{
    extract::{ConnectInfo, OriginalUri, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use dispatcharr_contracts::{source_headers, PlaybackRequest};
use dispatcharr_store::{AuthService, StoreError};
use serde_json::{Map, Value};
use std::net::SocketAddr;

struct Directory {
    prefix: &'static str,
    bare: bool,
    resources: &'static [&'static str],
}

const ALLOW: HeaderValue = HeaderValue::from_static("GET, HEAD, OPTIONS");
const BROWSER_ACCEPT: &str = concat!(
    "text/html,application/xhtml+xml,application/xml;q=0.9,",
    "image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
);
const CHANNELS_HTML: &str = include_str!("../datasets/drf-root-html/channels.html.in");
const M3U_HTML: &str = include_str!("../datasets/drf-root-html/m3u.html.in");
const CORE_HTML: &str = include_str!("../datasets/drf-root-html/core.html.in");
const CONNECT_HTML: &str = include_str!("../datasets/drf-root-html/connect.html.in");
const ACTOR_SLOT: &str = "@@DRF_ROOT_ACTOR@@";
const CSRF_SLOT: &str = "@@DRF_ROOT_CSRF@@";
const HOST_SLOT: &str = "@@DRF_ROOT_HOST@@";
const GET_REQUEST_LINE: &str = r#"<pre class="prettyprint"><b>GET</b>"#;
const HEAD_REQUEST_LINE: &str = r#"<pre class="prettyprint"><b>HEAD</b>"#;

const DIRECTORIES: &[Directory] = &[
    Directory {
        prefix: "/api/accounts/",
        bare: true,
        resources: &["users", "groups", "api-keys"],
    },
    Directory {
        prefix: "/api/channels/",
        bare: true,
        resources: &[
            "streams",
            "groups",
            "channels",
            "logos",
            "profiles",
            "recordings",
            "recurring-rules",
        ],
    },
    Directory {
        prefix: "/api/epg/",
        bare: true,
        resources: &["sources", "programs", "epgdata"],
    },
    Directory {
        prefix: "/api/m3u/",
        bare: true,
        resources: &["accounts", "server-groups"],
    },
    Directory {
        prefix: "/api/core/",
        bare: true,
        resources: &[
            "useragents",
            "streamprofiles",
            "outputprofiles",
            "settings",
            "notifications",
        ],
    },
    Directory {
        prefix: "/api/vod/",
        bare: true,
        resources: &[
            "movies",
            "episodes",
            "series",
            "categories",
            "all",
            "vodlogos",
        ],
    },
    Directory {
        prefix: "/api/connect/",
        bare: true,
        resources: &["integrations", "subscriptions", "logs"],
    },
    Directory {
        prefix: "/api/hdhr/",
        bare: false,
        resources: &["devices"],
    },
    Directory {
        prefix: "/hdhr/",
        bare: false,
        resources: &["devices"],
    },
];

pub(super) fn router(auth: AuthService) -> Router {
    let mut router = Router::new();
    let methods = get(directory_response)
        .options(directory_options)
        .fallback(|| async { prior_method_not_allowed() });
    for directory in DIRECTORIES {
        if directory.bare {
            router = router.route(directory.prefix, methods.clone());
        }
        router = router.route(&format!("{}.json", directory.prefix), methods.clone());
    }
    router.with_state(auth)
}

fn directory_links(directory: &Directory, origin: &str, suffix: bool) -> Map<String, Value> {
    let extension = if suffix { ".json" } else { "/" };
    directory
        .resources
        .iter()
        .map(|resource| {
            (
                (*resource).to_owned(),
                Value::String(format!("{origin}{}{resource}{extension}", directory.prefix)),
            )
        })
        .collect()
}

fn browsable_template(
    method: &Method,
    uri: &axum::http::Uri,
    headers: &HeaderMap,
) -> Option<&'static str> {
    if uri.query().is_some() {
        return None;
    }
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    let accept = accepts.next()?.to_str().ok()?;
    if accepts.next().is_some() || !matches!(accept, "text/html" | BROWSER_ACCEPT) {
        return None;
    }
    let authorization = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    if !authorization.starts_with("Bearer ") {
        return None;
    }
    match (method, uri.path()) {
        (&Method::GET, "/api/channels/") => Some(CHANNELS_HTML),
        (&Method::GET, "/api/m3u/") => Some(M3U_HTML),
        (&Method::GET, "/api/core/") => Some(CORE_HTML),
        (&Method::GET, "/api/connect/") => Some(CONNECT_HTML),
        (&Method::HEAD, "/api/core/") if accept == "text/html" => Some(CORE_HTML),
        (&Method::HEAD, "/api/connect/") if accept == "text/html" => Some(CONNECT_HTML),
        _ => None,
    }
}

fn render_template(template: &str, actor: &str, origin: &str, masked: &str) -> Result<Vec<u8>, ()> {
    let mut rendered = Output::new(32 * 1024);
    let mut remaining = template;
    while let Some(start) = remaining.find("@@DRF_ROOT_") {
        let (leading, slot) = remaining.split_at(start);
        if let Some(suffix) = slot.strip_prefix(ACTOR_SLOT) {
            rendered.text(leading)?;
            wire::escape(&mut rendered, actor)?;
            remaining = suffix;
        } else if let Some(suffix) = slot.strip_prefix(CSRF_SLOT) {
            rendered.text(leading)?;
            rendered.text(masked)?;
            remaining = suffix;
        } else if let (Some(leading), Some(suffix)) = (
            leading.strip_suffix("http://"),
            slot.strip_prefix(HOST_SLOT),
        ) {
            rendered.text(leading)?;
            wire::escape(&mut rendered, origin)?;
            remaining = suffix;
        } else {
            return Err(());
        }
    }
    rendered.text(remaining)?;
    Ok(rendered.finish())
}

fn directory_html(
    template: &str,
    actor: &str,
    origin: &str,
    headers: &HeaderMap,
    method: &Method,
) -> Result<Response, ()> {
    let cookie = headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok());
    let (_, masked, set_cookie) = dispatcharr_store::source_browsable_csrf(cookie);
    let head = method == Method::HEAD;
    let head_template;
    let template = if head {
        let (before, after) = template.split_once(GET_REQUEST_LINE).ok_or(())?;
        head_template = format!("{before}{HEAD_REQUEST_LINE}{after}");
        &head_template
    } else {
        template
    };
    let body = render_template(template, actor, origin, &masked)?;
    let length = HeaderValue::from_str(&body.len().to_string()).map_err(|_| ())?;
    let mut response = Html(if head { Vec::new() } else { body }).into_response();
    let headers = response.headers_mut();
    headers.insert(header::ALLOW, ALLOW);
    headers.insert(
        header::VARY,
        HeaderValue::from_static("Accept, origin, Cookie"),
    );
    headers.insert(header::CONTENT_LENGTH, length);
    headers.insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&set_cookie).map_err(|_| ())?,
    );
    Ok(response)
}

fn ordinary_json_options(uri: &Uri, headers: &HeaderMap) -> bool {
    if uri.query().is_some()
        || [
            header::ORIGIN,
            header::ACCESS_CONTROL_REQUEST_METHOD,
            header::ACCESS_CONTROL_REQUEST_HEADERS,
        ]
        .iter()
        .any(|name| headers.contains_key(name))
    {
        return false;
    }
    let mut accepts = headers.get_all(header::ACCEPT).iter();
    accepts
        .next()
        .is_some_and(|value| value == "application/json")
        && accepts.next().is_none()
}

fn prior_method_not_allowed() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [(header::ALLOW, "GET,HEAD")],
    )
        .into_response()
}

fn options_metadata() -> Response {
    Json(serde_json::json!({
        "name": "Api Root",
        "description": "The default basic root view for DefaultRouter",
        "renders": ["application/json", "text/html"],
        "parses": ["application/json", "application/x-www-form-urlencoded", "multipart/form-data"],
    }))
    .into_response()
}

async fn directory_options(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    if !ordinary_json_options(&uri, &headers) {
        return prior_method_not_allowed();
    }
    let mut response = match auth.account_user(peer, headers, true).await {
        Ok(_) => options_metadata(),
        Err(error) => error.into_response(),
    };
    response.headers_mut().insert(header::ALLOW, ALLOW);
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Accept"));
    response
}

async fn directory_response(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let authorized = auth.account_user(peer, headers.clone(), true).await;
    let mut response = match authorized {
        Ok(actor) => {
            let (prefix, suffix) = uri
                .path()
                .strip_suffix(".json")
                .map_or((uri.path(), false), |path| (path, true));
            let Some(directory) = DIRECTORIES
                .iter()
                .find(|directory| directory.prefix == prefix)
            else {
                return StoreError::NotFound.into_response();
            };
            let request = PlaybackRequest {
                channel_id: String::new(),
                query: Vec::new(),
                peer_ip: peer.ip(),
                headers: source_headers(
                    headers
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.as_bytes())),
                ),
            };
            let origin = dispatcharr_output_http::source_output_origin(&auth, &request);
            if let Some(template) = browsable_template(&method, &uri, &headers) {
                return directory_html(template, &actor.username, &origin, &headers, &method)
                    .unwrap_or_else(|()| {
                        StoreError::Unsupported("API directory HTML exceeded its bounds")
                            .into_response()
                    });
            }
            Json(directory_links(directory, &origin, suffix)).into_response()
        }
        Err(error) => error.into_response(),
    };
    response.headers_mut().insert(header::ALLOW, ALLOW);
    response
        .headers_mut()
        .append(header::VARY, HeaderValue::from_static("Accept"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directories_keep_original_link_order_absolute_paths_and_narrow_hdhr_scope() {
        let accounts = &DIRECTORIES[0];
        let value = directory_links(accounts, "http://127.0.0.1:5656", true);
        assert_eq!(
            serde_json::to_string(&value).unwrap(),
            r#"{"users":"http://127.0.0.1:5656/api/accounts/users.json","groups":"http://127.0.0.1:5656/api/accounts/groups.json","api-keys":"http://127.0.0.1:5656/api/accounts/api-keys.json"}"#
        );
        assert_eq!(DIRECTORIES.len(), 9);
        assert_eq!(
            DIRECTORIES
                .iter()
                .filter(|directory| directory.bare)
                .count(),
            7
        );
        assert!(DIRECTORIES
            .iter()
            .filter(|directory| !directory.bare)
            .all(|directory| {
                directory.prefix.ends_with("/hdhr/") && directory.resources == ["devices"]
            }));
    }

    #[test]
    fn only_captured_bare_admin_get_contexts_use_browsable_html() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer fixture-marker"),
        );
        for accept in ["text/html", BROWSER_ACCEPT] {
            headers.insert(header::ACCEPT, HeaderValue::from_static(accept));
            for (path, template) in [
                ("/api/channels/", CHANNELS_HTML),
                ("/api/m3u/", M3U_HTML),
                ("/api/core/", CORE_HTML),
                ("/api/connect/", CONNECT_HTML),
            ] {
                assert_eq!(
                    browsable_template(&Method::GET, &path.parse().unwrap(), &headers),
                    Some(template)
                );
                let expected_head = (accept == "text/html"
                    && matches!(path, "/api/core/" | "/api/connect/"))
                .then_some(template);
                assert_eq!(
                    browsable_template(&Method::HEAD, &path.parse().unwrap(), &headers),
                    expected_head
                );
                assert!(
                    browsable_template(&Method::OPTIONS, &path.parse().unwrap(), &headers)
                        .is_none()
                );
            }
            for path in [
                "/api/channels/.json",
                "/api/m3u/?format=api",
                "/api/core/.json",
                "/api/core/?format=api",
                "/api/connect/.json",
                "/api/connect/?format=api",
                "/api/epg/",
            ] {
                assert!(
                    browsable_template(&Method::GET, &path.parse().unwrap(), &headers).is_none()
                );
            }
        }
        let uri = "/api/channels/".parse().unwrap();
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(browsable_template(&Method::GET, &uri, &headers).is_none());
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/html"));
        headers.append(header::ACCEPT, HeaderValue::from_static("text/html"));
        assert!(browsable_template(&Method::GET, &uri, &headers).is_none());
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/html"));
        headers.remove(header::AUTHORIZATION);
        assert!(browsable_template(&Method::GET, &uri, &headers).is_none());
    }

    #[tokio::test]
    async fn browsable_success_has_exact_static_bytes_fresh_csrf_and_escaped_slots() {
        let username = "owned-api-root-browser-admin";
        let mask_location = "\"csrfToken\": \"";
        for (template, bytes, links) in [
            (CHANNELS_HTML, 11055, 14),
            (M3U_HTML, 10271, 4),
            (CORE_HTML, 10743, 10),
            (CONNECT_HTML, 10457, 6),
        ] {
            assert_eq!(template.matches(ACTOR_SLOT).count(), 1);
            assert_eq!(template.matches(CSRF_SLOT).count(), 1);
            assert_eq!(template.matches(HOST_SLOT).count(), links);
            let mut observed = Vec::new();
            for _ in 0..2 {
                let response = directory_html(
                    template,
                    username,
                    "http://localhost",
                    &HeaderMap::new(),
                    &Method::GET,
                )
                .unwrap();
                assert_eq!(response.status(), axum::http::StatusCode::OK);
                assert_eq!(response.headers()[header::ALLOW], ALLOW);
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "text/html; charset=utf-8"
                );
                assert_eq!(response.headers()[header::VARY], "Accept, origin, Cookie");
                assert_eq!(
                    response.headers()[header::CONTENT_LENGTH],
                    bytes.to_string()
                );
                let cookie = response.headers()[header::SET_COOKIE]
                    .to_str()
                    .unwrap()
                    .to_owned();
                assert!(cookie.starts_with("csrftoken="));
                let raw = axum::body::to_bytes(response.into_body(), 32 * 1024)
                    .await
                    .unwrap();
                let html = std::str::from_utf8(&raw).unwrap();
                assert_eq!(html.len(), bytes);
                let mask = html
                    .split_once(mask_location)
                    .unwrap()
                    .1
                    .split_once('"')
                    .unwrap()
                    .0;
                assert_eq!(mask.len(), 64);
                assert!(mask.bytes().all(|byte| byte.is_ascii_alphanumeric()));
                let reconstructed = template
                    .replace(ACTOR_SLOT, username)
                    .replace(CSRF_SLOT, mask)
                    .replace("http://@@DRF_ROOT_HOST@@", "http://localhost");
                assert_eq!(html, reconstructed);
                observed.push((mask.to_owned(), cookie));
            }
            assert_ne!(observed[0].0, observed[1].0);
            assert_ne!(observed[0].1, observed[1].1);
            let escaped = render_template(
                template,
                "<&>@@DRF_ROOT_CSRF@@",
                "https://example.invalid:8443",
                &"Z".repeat(64),
            )
            .unwrap();
            let escaped = std::str::from_utf8(&escaped).unwrap();
            assert!(escaped.contains("&lt;&amp;&gt;@@DRF_ROOT_CSRF@@"));
            assert_eq!(
                escaped.matches("https://example.invalid:8443").count(),
                links
            );
            assert!(!escaped.contains("http://https://"));
        }
    }

    #[tokio::test]
    async fn core_and_connect_head_render_the_request_line_without_sending_an_entity() {
        for (template, get_length) in [(CORE_HTML, 10743), (CONNECT_HTML, 10457)] {
            assert_eq!(template.matches(GET_REQUEST_LINE).count(), 1);
            let response = directory_html(
                template,
                "owned-api-root-browser-admin",
                "http://localhost",
                &HeaderMap::new(),
                &Method::HEAD,
            )
            .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let headers = response.headers();
            assert_eq!(headers[header::ALLOW], ALLOW);
            assert_eq!(headers[header::CONTENT_TYPE], "text/html; charset=utf-8");
            assert_eq!(headers[header::VARY], "Accept, origin, Cookie");
            assert_eq!(
                headers[header::CONTENT_LENGTH],
                (get_length + 1).to_string()
            );
            assert!(headers[header::SET_COOKIE]
                .to_str()
                .unwrap()
                .starts_with("csrftoken="));
            assert!(axum::body::to_bytes(response.into_body(), 32 * 1024)
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn core_and_connect_keep_the_captured_json_link_order() {
        let origin = "http://localhost";
        for (prefix, expected) in [
            (
                "/api/core/",
                r#"{"useragents":"http://localhost/api/core/useragents/","streamprofiles":"http://localhost/api/core/streamprofiles/","outputprofiles":"http://localhost/api/core/outputprofiles/","settings":"http://localhost/api/core/settings/","notifications":"http://localhost/api/core/notifications/"}"#,
            ),
            (
                "/api/connect/",
                r#"{"integrations":"http://localhost/api/connect/integrations/","subscriptions":"http://localhost/api/connect/subscriptions/","logs":"http://localhost/api/connect/logs/"}"#,
            ),
        ] {
            let directory = DIRECTORIES
                .iter()
                .find(|directory| directory.prefix == prefix)
                .unwrap();
            assert_eq!(
                serde_json::to_string(&directory_links(directory, origin, false)).unwrap(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn ordinary_options_metadata_has_pinned_order_and_no_dynamic_fields() {
        let response = options_metadata();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert!(!response.headers().contains_key(header::SET_COOKIE));
        let raw = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .unwrap();
        assert_eq!(
            &raw[..],
            br#"{"name":"Api Root","description":"The default basic root view for DefaultRouter","renders":["application/json","text/html"],"parses":["application/json","application/x-www-form-urlencoded","multipart/form-data"]}"#,
        );
    }

    #[test]
    fn only_the_ordinary_json_options_profile_is_admitted() {
        let uri = "/api/channels/".parse().unwrap();
        let mut headers = HeaderMap::new();
        assert!(!ordinary_json_options(&uri, &headers));
        headers.insert(header::ACCEPT, HeaderValue::from_static("application/json"));
        assert!(ordinary_json_options(&uri, &headers));
        let reply = prior_method_not_allowed();
        assert_eq!(reply.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(reply.headers()[header::ALLOW], "GET,HEAD");
        for name in [
            header::ORIGIN,
            header::ACCESS_CONTROL_REQUEST_METHOD,
            header::ACCESS_CONTROL_REQUEST_HEADERS,
        ] {
            headers.insert(&name, HeaderValue::from_static("fixture"));
            assert!(!ordinary_json_options(&uri, &headers));
            headers.remove(name);
        }
        assert!(!ordinary_json_options(
            &"/api/channels/?format=json".parse().unwrap(),
            &headers,
        ));
        headers.append(header::ACCEPT, HeaderValue::from_static("text/html"));
        assert!(!ordinary_json_options(&uri, &headers));
        headers.insert(header::ACCEPT, HeaderValue::from_static("text/html"));
        assert!(!ordinary_json_options(&uri, &headers));
    }
}
