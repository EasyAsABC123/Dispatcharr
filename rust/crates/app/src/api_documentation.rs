//! Public documentation from pinned Original discovery and drf-spectacular templates.
use axum::{
    extract::{ConnectInfo, OriginalUri, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dispatcharr_contracts::{source_headers, PlaybackRequest};
use dispatcharr_store::{AuthService, StoreError};
use std::{borrow::Cow, net::SocketAddr};

const OPENAPI_JSON: &str = include_str!("../datasets/api-docs/openapi.json");
const OPENAPI_YAML: &str = include_str!("../datasets/api-docs/openapi.yaml");
const SWAGGER: &str = include_str!("../datasets/api-docs/swagger_ui.html");
const REDOC: &str = include_str!("../datasets/api-docs/redoc.html");
const SCHEMA_OPTIONS_JSON: &str = include_str!("../datasets/api-docs/schema_options.json");
const SCHEMA_OPTIONS_YAML: &str = include_str!("../datasets/api-docs/schema_options.yaml");
const REDOC_OPTIONS: &str = include_str!("../datasets/api-docs/redoc_options.html");
const CSRF_SLOT: &str = "DISPATCHARRCSRFRENDERINGSLOT";

#[derive(Clone, Copy)]
struct Renderer {
    mime: &'static str,
    format: &'static str,
}

const SCHEMA_RENDERERS: &[Renderer] = &[
    Renderer {
        mime: "application/vnd.oai.openapi; charset=utf-8",
        format: "yaml",
    },
    Renderer {
        mime: "application/yaml; charset=utf-8",
        format: "yaml",
    },
    Renderer {
        mime: "application/vnd.oai.openapi+json",
        format: "json",
    },
    Renderer {
        mime: "application/json",
        format: "json",
    },
];
const HTML_RENDERERS: &[Renderer] = &[Renderer {
    mime: "text/html",
    format: "html",
}];

pub(super) fn router(auth: AuthService) -> Router {
    Router::new()
        .route("/api/schema/", get(documentation).options(documentation))
        .route(
            "/api/swagger.json",
            get(documentation).options(documentation),
        )
        .route("/api/swagger", get(documentation).options(documentation))
        .route("/api/swagger/", get(documentation).options(documentation))
        .route("/api/redoc/", get(documentation).options(documentation))
        .with_state(auth)
}

fn match_specificity(mime: &str, accepted: &str) -> Option<u8> {
    let (kind, subtype) = mime.split_once('/')?;
    let (accepted_kind, accepted_subtype) = accepted.trim().split_once('/')?;
    if accepted_kind == "*" && accepted_subtype == "*" {
        Some(0)
    } else if !kind.eq_ignore_ascii_case(accepted_kind) {
        None
    } else if accepted_subtype == "*" {
        Some(1)
    } else if subtype.eq_ignore_ascii_case(accepted_subtype) {
        Some(2)
    } else {
        None
    }
}

enum NegotiationError {
    VariantUnavailable,
    UnknownFormat,
    Unacceptable,
}

impl NegotiationError {
    fn response(self, schema: bool) -> Response {
        let (status, body) = match (self, schema) {
            (Self::VariantUnavailable, _) => return StoreError::NotFound.into_response(),
            (Self::UnknownFormat, true) => (
                StatusCode::NOT_FOUND,
                "detail:\n  string: Not found.\n  code: not_found\n",
            ),
            (Self::Unacceptable, true) => (
                StatusCode::NOT_ACCEPTABLE,
                "detail:\n  string: Could not satisfy the request Accept header.\n  code: not_acceptable\n",
            ),
            (Self::UnknownFormat, false) => (StatusCode::NOT_FOUND, "404 Not Found"),
            (Self::Unacceptable, false) => (StatusCode::NOT_ACCEPTABLE, "406 Not Acceptable"),
        };
        let mime = if schema {
            SCHEMA_RENDERERS[0].mime
        } else {
            "text/html; charset=utf-8"
        };
        let vary = if schema { "Accept, origin" } else { "origin" };
        (
            status,
            [(header::CONTENT_TYPE, mime), (header::VARY, vary)],
            body,
        )
            .into_response()
    }
}

#[derive(Default)]
struct Selectors<'a> {
    format: Option<Cow<'a, str>>,
    language: Option<Cow<'a, str>>,
    version: Option<Cow<'a, str>>,
}

impl<'a> Selectors<'a> {
    fn from_query(query: &'a str) -> Self {
        let mut selected = Self::default();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "format" => selected.format = Some(value),
                "lang" => selected.language = Some(value),
                "version" => selected.version = Some(value),
                _ => {}
            }
        }
        selected
    }

    fn schema_version(&self, schema: bool) -> Result<Option<&str>, NegotiationError> {
        let lang = self.language.as_deref().filter(|value| !value.is_empty());
        let version = self.version.as_deref().filter(|value| !value.is_empty());
        if lang.is_some_and(|value| !schema || value != "en")
            || version.is_some_and(|value| {
                !schema
                    || value.len() > 64
                    || !value.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
                    })
            })
        {
            return Err(NegotiationError::VariantUnavailable);
        }
        Ok(version)
    }
}

fn negotiate(
    format: Option<&str>,
    headers: &HeaderMap,
    renderers: &[Renderer],
) -> Result<Renderer, NegotiationError> {
    let eligible = |r: &Renderer| format.is_none_or(|value| value.is_empty() || value == r.format);
    if !renderers.iter().any(eligible) {
        return Err(NegotiationError::UnknownFormat);
    }
    let mut accepts = headers.get_all(header::ACCEPT).iter().peekable();
    if accepts.peek().is_none() {
        return Ok(*renderers
            .iter()
            .find(|r| eligible(r))
            .expect("checked renderers"));
    }

    // DRF tries the most specific media range, then renderer order within it.
    let mut selected = None;
    let mut best = None;
    for renderer in renderers.iter().filter(|r| eligible(r)) {
        let score = headers
            .get_all(header::ACCEPT)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|line| line.split(','))
            .filter_map(|range| {
                match_specificity(renderer.mime.split(';').next()?, range.split(';').next()?)
            })
            .max();
        if score.is_some() && score > best {
            best = score;
            selected = Some(*renderer);
        }
    }
    selected.ok_or(NegotiationError::Unacceptable)
}

fn supplied_auth(peer: SocketAddr, headers: &HeaderMap) -> PlaybackRequest {
    let mut request = PlaybackRequest {
        channel_id: String::new(),
        query: Vec::new(),
        peer_ip: peer.ip(),
        headers: source_headers(
            headers
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_bytes())),
        ),
    };
    // Pinned Source uses SimpleJWT Bearer and ApiKey/X-API-Key. Its REST views use AllowAny
    // after authenticating supplied credentials; there is no signed role gate.
    request.headers.retain(|(name, value)| {
        name != "authorization"
            || value
                .split_whitespace()
                .next()
                .is_some_and(|scheme| scheme == "Bearer" || scheme.eq_ignore_ascii_case("ApiKey"))
    });
    request
}

fn schema_success(renderer: Renderer, version: Option<&str>) -> Response {
    let (body, prefix) = if renderer.format == "json" {
        (
            OPENAPI_JSON,
            "\n        \"title\": \"Dispatcharr API\",\n        \"version\": \"1.0.0",
        )
    } else {
        (OPENAPI_YAML, "\n  title: Dispatcharr API\n  version: 1.0.0")
    };
    let (body, suffix) = match version {
        None => (Cow::Borrowed(body), String::new()),
        Some(version) => (
            Cow::Owned(body.replacen(prefix, &format!("{prefix} ({version})"), 1)),
            format!(" ({version})"),
        ),
    };
    let disposition = format!(
        "inline; filename=\"Dispatcharr API{suffix}.{}\"",
        renderer.format
    );
    (
        [
            (header::CONTENT_TYPE, renderer.mime),
            (header::VARY, "Accept"),
        ],
        [(header::CONTENT_DISPOSITION, disposition)],
        body,
    )
        .into_response()
}

fn html_success(uri: &Uri, headers: &HeaderMap, html_format: bool) -> Response {
    let mut response = if uri.path() == "/api/redoc/" {
        let body = if html_format {
            REDOC.replace(
                "spec-url=\"/api/schema/\"",
                "spec-url=\"/api/schema/?format=html\"",
            )
        } else {
            REDOC.to_owned()
        };
        ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response()
    } else {
        let cookie = crate::admin_channelgroup::cookie(headers, "csrftoken");
        let (_, masked, set_cookie) = dispatcharr_store::source_browsable_csrf(cookie);
        let mut body = SWAGGER.replace(CSRF_SLOT, &masked);
        if html_format {
            body = body.replace(
                "url: \"/api/schema/\"",
                r#"url: "/api/schema/?format\u003Dhtml""#,
            );
        }
        (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("text/html; charset=utf-8"),
                ),
                (
                    header::SET_COOKIE,
                    HeaderValue::from_str(&set_cookie).expect("validated CSRF cookie"),
                ),
                (
                    header::HeaderName::from_static("cross-origin-opener-policy"),
                    HeaderValue::from_static("unsafe-none"),
                ),
            ],
            body,
        )
            .into_response()
    };
    if uri.path() != "/api/redoc/" {
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("origin, Cookie"));
    }
    response
}

fn schema_options(renderer: Renderer, headers: &HeaderMap) -> Response {
    let body = if renderer.format == "json" {
        let mime = renderer.mime;
        let params = headers
            .get_all(header::ACCEPT)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .filter_map(|range| {
                let mut parts = range.split(';');
                let specificity = match_specificity(mime, parts.next()?)?;
                let indent = parts
                    .filter_map(|part| part.trim().split_once('='))
                    .find(|(key, _)| key.eq_ignore_ascii_case("indent"))
                    .and_then(|(_, value)| value.trim().trim_matches('"').parse::<i32>().ok())
                    .map(|value| value.clamp(0, 8))
                    .filter(|&value| value != 0)
                    .unwrap_or(4);
                Some((specificity, indent))
            })
            .max_by_key(|&(specificity, _)| specificity);
        let count = params.map_or(4, |(_, value)| value) as usize;
        if count == 4 {
            SCHEMA_OPTIONS_JSON.as_bytes().to_vec()
        } else {
            // The frozen metadata has four-space indentation and no literal
            // newlines inside strings. Rewrite its structural indentation only.
            SCHEMA_OPTIONS_JSON
                .split('\n')
                .map(|line| {
                    let text = line.trim_start_matches(' ');
                    let depth = (line.len() - text.len()) / 4;
                    format!("{}{text}", " ".repeat(depth * count))
                })
                .collect::<Vec<_>>()
                .join("\n")
                .into_bytes()
        }
    } else {
        SCHEMA_OPTIONS_YAML.as_bytes().to_vec()
    };
    (
        [
            (header::CONTENT_TYPE, renderer.mime),
            (header::VARY, "Accept"),
        ],
        body,
    )
        .into_response()
}

async fn documentation(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    method: Method,
) -> Result<Response, StoreError> {
    let schema = matches!(uri.path(), "/api/schema/" | "/api/swagger.json");
    let renderers = if schema {
        SCHEMA_RENDERERS
    } else {
        HTML_RENDERERS
    };
    let redoc = uri.path() == "/api/redoc/";
    let selected = Selectors::from_query(uri.query().unwrap_or_default());
    let negotiated = selected.schema_version(schema).and_then(|version| {
        negotiate(selected.format.as_deref(), &headers, renderers)
            .map(|renderer| (renderer, version))
    });
    let mut response = match negotiated {
        Ok((renderer, version)) => {
            auth.authenticate(&supplied_auth(peer, &headers)).await?;
            if method == Method::OPTIONS && schema {
                schema_options(renderer, &headers)
            } else if method == Method::OPTIONS && redoc {
                (
                    [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                    REDOC_OPTIONS,
                )
                    .into_response()
            } else if method == Method::OPTIONS {
                // Original's successful Swagger negotiation invokes an invalid
                // template context and faults. Keep the Native refusal explicit.
                return Ok((
                    StatusCode::METHOD_NOT_ALLOWED,
                    [(header::ALLOW, "GET,HEAD")],
                )
                    .into_response());
            } else if schema {
                schema_success(renderer, version)
            } else {
                html_success(&uri, &headers, selected.format.as_deref() == Some("html"))
            }
        }
        Err(error) => error.response(schema),
    };
    response.headers_mut().insert(
        header::ALLOW,
        HeaderValue::from_static("GET, HEAD, OPTIONS"),
    );
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
        Extension,
    };
    use tower::ServiceExt;

    fn fixture() -> Router {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = AuthService::new(
            dispatcharr_store::Store::from_pool(pool),
            dispatcharr_store::AuthConfig::new("docs-router-test".into()).unwrap(),
        );
        router(auth)
            .route("/api/{*path}", get(crate::api_not_found))
            .layer(Extension(ConnectInfo(SocketAddr::from((
                [127, 0, 0, 1],
                9192,
            )))))
    }

    async fn request(path: &str, accept: &str) -> Response {
        fixture()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::ACCEPT, accept)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[test]
    fn source_api_key_authorization_is_preserved_for_authentication() {
        let peer = SocketAddr::from(([127, 0, 0, 1], 9192));
        for supplied in [
            "ApiKey owned-key",
            "apikey revoked-key",
            "APIKEY inactive-key",
            "Bearer invalid-token",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(supplied).unwrap(),
            );
            assert_eq!(
                supplied_auth(peer, &headers).header("authorization"),
                Some(supplied)
            );
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic ignored"),
        );
        headers.insert("x-api-key", HeaderValue::from_static("owned-header-key"));
        let request = supplied_auth(peer, &headers);
        assert_eq!(request.header("authorization"), None);
        assert_eq!(request.header("x-api-key"), Some("owned-header-key"));
    }

    #[tokio::test]
    async fn swagger_reuses_browser_csrf_secret_with_fresh_masks() {
        let secret = "abcdefghijklmnopqrstuvwxyz012345";
        let cookie = format!("sessionid=owned-session; csrftoken={secret}; theme=dark");
        let mut masks = Vec::new();
        for _ in 0..2 {
            let response = fixture()
                .oneshot(
                    Request::builder()
                        .uri("/api/swagger/")
                        .header(header::COOKIE, &cookie)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let set_cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
            assert!(set_cookie.starts_with(&format!("csrftoken={secret};")));
            let raw = to_bytes(response.into_body(), 8192).await.unwrap();
            let body = std::str::from_utf8(&raw).unwrap();
            // Locate the sole actual rendered CSRF JavaScript value without
            // depending on any other template bytes or generated entropy.
            let marker = "X-CSRFTOKEN";
            let token_area = body.split_once(marker).unwrap().1;
            let masked = token_area
                .split('\"')
                .find(|part| part.len() == 64 && part.bytes().all(|b| b.is_ascii_alphanumeric()))
                .unwrap();
            let (unmasked, _, _) = dispatcharr_store::source_browsable_csrf(Some(masked));
            assert_eq!(unmasked, secret);
            masks.push(masked.to_owned());
        }
        assert_ne!(masks[0], masks[1]);
    }

    #[tokio::test]
    async fn schema_selectors_keep_the_graph_and_guard_the_filename() {
        let response = request("/api/schema/?lang=en", "*/*").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            &to_bytes(response.into_body(), 1_000_000).await.unwrap()[..],
            OPENAPI_YAML.as_bytes()
        );

        let response = request(
            "/api/swagger.json?version=ignored&version=owned-v1",
            "application/json",
        )
        .await;
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            "inline; filename=\"Dispatcharr API (owned-v1).json\""
        );
        let raw = to_bytes(response.into_body(), 1_000_000).await.unwrap();
        let mut actual: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(actual["info"]["version"], "1.0.0 (owned-v1)");
        actual["info"]["version"] = "1.0.0".into();
        assert_eq!(
            actual,
            serde_json::from_str::<serde_json::Value>(OPENAPI_JSON).unwrap()
        );
        let yaml = request("/api/schema/?version=owned-v1", "*/*").await;
        let raw = to_bytes(yaml.into_body(), 1_000_000).await.unwrap();
        assert!(std::str::from_utf8(&raw)
            .unwrap()
            .contains("  version: 1.0.0 (owned-v1)\n"));

        for path in [
            "/api/schema/?lang=fr",
            "/api/schema/?version=a%22b",
            "/api/redoc/?lang=en",
        ] {
            assert_eq!(request(path, "*/*").await.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn swagger_allow_and_options_rejections_match_original_metadata() {
        for path in ["/api/swagger", "/api/swagger/"] {
            let get = request(path, "text/html").await;
            assert_eq!(get.headers()[header::ALLOW], "GET, HEAD, OPTIONS");
            for (target, accept, status, body) in [
                (
                    format!("{path}?format=owned-unknown"),
                    "*/*",
                    StatusCode::NOT_FOUND,
                    "404 Not Found",
                ),
                (
                    path.to_owned(),
                    "application/json",
                    StatusCode::NOT_ACCEPTABLE,
                    "406 Not Acceptable",
                ),
            ] {
                let response = fixture()
                    .oneshot(
                        Request::builder()
                            .method(Method::OPTIONS)
                            .uri(target)
                            .header(header::ACCEPT, accept)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), status);
                assert_eq!(response.headers()[header::ALLOW], "GET, HEAD, OPTIONS");
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    "text/html; charset=utf-8"
                );
                assert_eq!(to_bytes(response.into_body(), 100).await.unwrap(), body);
            }
        }
    }

    #[tokio::test]
    async fn measured_options_advertise_only_supported_routes() {
        for (path, accept, expected_status, expected_size) in [
            ("/api/schema/", "*/*", StatusCode::OK, 306),
            ("/api/swagger.json", "application/json", StatusCode::OK, 417),
            (
                "/api/swagger.json",
                "application/json; indent=2",
                StatusCode::OK,
                377,
            ),
            (
                "/api/schema/?format=json",
                "application/yaml",
                StatusCode::NOT_ACCEPTABLE,
                86,
            ),
            ("/api/redoc/?format=html", "*/*", StatusCode::OK, 640),
        ] {
            let response = fixture()
                .oneshot(
                    Request::builder()
                        .method(Method::OPTIONS)
                        .uri(path)
                        .header(header::ACCEPT, accept)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(response.headers()[header::ALLOW], "GET, HEAD, OPTIONS");
            assert!(!response.headers().contains_key(header::CONTENT_DISPOSITION));
            assert!(!response.headers().contains_key(header::SET_COOKIE));
            assert_eq!(
                to_bytes(response.into_body(), 6000).await.unwrap().len(),
                expected_size
            );
        }
        let response = fixture()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/swagger/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn source_format_targets_vary_and_rejection_bodies() {
        let response = request("/api/swagger?format=html", "*/*").await;
        assert_eq!(response.headers()[header::VARY], "origin, Cookie");
        let html = to_bytes(response.into_body(), 5000).await.unwrap();
        assert_eq!(html.len(), 4665);
        assert!(std::str::from_utf8(&html)
            .unwrap()
            .contains(r#"?format\u003Dhtml"#));
        let response = request("/api/redoc/?format=html", "*/*").await;
        let html = to_bytes(response.into_body(), 1000).await.unwrap();
        assert_eq!(html.len(), 743);
        assert!(std::str::from_utf8(&html)
            .unwrap()
            .contains("/api/schema/?format=html"));
        let response = request("/api/schema/?format=json", "application/yaml").await;
        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            SCHEMA_RENDERERS[0].mime
        );
        let body = to_bytes(response.into_body(), 100).await.unwrap();
        assert_eq!(body.len(), 86);
        let response = request("/api/swagger/?format=xml", "*/*").await;
        let body = to_bytes(response.into_body(), 100).await.unwrap();
        assert_eq!(&body[..], b"404 Not Found");
    }

    #[tokio::test]
    async fn exact_schema_aliases_and_media() {
        for (path, accept, content_type) in [
            (
                "/api/schema/",
                "*/*",
                "application/vnd.oai.openapi; charset=utf-8",
            ),
            (
                "/api/schema/",
                "application/yaml",
                "application/yaml; charset=utf-8",
            ),
            (
                "/api/schema/?format=json",
                "*/*",
                "application/vnd.oai.openapi+json",
            ),
            (
                "/api/swagger.json?format=json",
                "application/json",
                "application/json",
            ),
        ] {
            let response = request(path, accept).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
            assert!(!response
                .headers()
                .contains_key("x-dispatcharr-schema-source"));
            let raw = to_bytes(response.into_body(), 1_000_000).await.unwrap();
            if content_type.contains("json") {
                let document: serde_json::Value = serde_json::from_slice(&raw).unwrap();
                assert_eq!(document["openapi"], "3.0.3");
                let paths = document["paths"].as_object().unwrap();
                assert_eq!(paths.len(), 246);
                assert!(paths["/api/core/useragents/"]["get"]["operationId"].is_string());
            } else {
                assert_eq!(&raw[..], OPENAPI_YAML.as_bytes());
            }
        }
        assert_eq!(
            request("/api/schema/", "text/html").await.status(),
            StatusCode::NOT_ACCEPTABLE
        );
        assert_eq!(
            request("/api/schema/?format=xml", "*/*").await.status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn html_targets_and_unrelated_fallback() {
        for path in ["/api/swagger", "/api/swagger/", "/api/redoc/"] {
            let response = request(path, "text/html").await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
            let expect_cookie = path != "/api/redoc/";
            assert_eq!(
                response.headers().contains_key(header::SET_COOKIE),
                expect_cookie
            );
            let raw = to_bytes(response.into_body(), 8192).await.unwrap();
            let body = std::str::from_utf8(&raw).unwrap();
            assert!(body.contains("/api/schema/") && !body.contains(CSRF_SLOT));
            if expect_cookie {
                assert!(body.contains("SwaggerUIBundle"));
            } else {
                assert_eq!(body, REDOC);
            }
        }
        for path in ["/api/redoc", "/api/swagger.json/", "/api/schema/unrelated/"] {
            let response = request(path, "*/*").await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let raw = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(&raw[..], br#"{"detail":"Not found."}"#);
        }
    }

    #[tokio::test]
    async fn supplied_invalid_signed_credentials_are_rejected_and_head_is_bodyless() {
        let response = fixture()
            .oneshot(
                Request::builder()
                    .uri("/api/schema/?format=json")
                    .header(header::AUTHORIZATION, "Bearer invalid-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = fixture()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/api/redoc/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            REDOC.len().to_string()
        );
        assert!(to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty());
    }
}
