//! Public documentation from pinned Original discovery and drf-spectacular templates.
use axum::{
    extract::{ConnectInfo, OriginalUri, State},
    http::{header, HeaderMap, HeaderValue, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use dispatcharr_contracts::{source_headers, PlaybackRequest};
use dispatcharr_store::{AuthService, StoreError};
use serde_json::json;
use std::net::SocketAddr;

const OPENAPI_JSON: &str = include_str!("../datasets/api-docs/openapi.json");
const OPENAPI_YAML: &str = include_str!("../datasets/api-docs/openapi.yaml");
const SWAGGER: &str = include_str!("../datasets/api-docs/swagger_ui.html");
const REDOC: &str = include_str!("../datasets/api-docs/redoc.html");
const CSRF_SLOT: &str = "DISPATCHARRCSRFRENDERINGSLOT";

#[derive(Clone, Copy)]
struct Renderer {
    mime: &'static str,
    format: &'static str,
}

const SCHEMA_RENDERERS: &[Renderer] = &[
    Renderer {
        mime: "application/vnd.oai.openapi",
        format: "yaml",
    },
    Renderer {
        mime: "application/yaml",
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
        .route("/api/schema/", get(documentation))
        .route("/api/swagger.json", get(documentation))
        .route("/api/swagger", get(documentation))
        .route("/api/swagger/", get(documentation))
        .route("/api/redoc/", get(documentation))
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

fn negotiate(
    uri: &Uri,
    headers: &HeaderMap,
    renderers: &[Renderer],
) -> Result<Renderer, StoreError> {
    let query = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes());
    let mut format = None;
    for (key, value) in query {
        match key.as_ref() {
            "format" => format = Some(value),
            // Only the default public discovery snapshot has exact retained provenance.
            "lang" | "version" if !value.is_empty() => return Err(StoreError::NotFound),
            _ => {}
        }
    }
    let eligible = |r: &Renderer| {
        format
            .as_deref()
            .is_none_or(|value| value.is_empty() || value == r.format)
    };
    if !renderers.iter().any(eligible) {
        return Err(StoreError::NotFound);
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
            .filter_map(|range| match_specificity(renderer.mime, range.split(';').next()?))
            .max();
        if score.is_some() && score > best {
            best = score;
            selected = Some(*renderer);
        }
    }
    selected.ok_or_else(|| {
        StoreError::StatusBody(
            406,
            json!({"detail":"Could not satisfy the request Accept header."}),
        )
    })
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

fn success(uri: &Uri, headers: &HeaderMap, renderer: Renderer, schema: bool) -> Response {
    let mut response = if schema {
        let (body, disposition) = if renderer.format == "json" {
            (OPENAPI_JSON, "inline; filename=\"Dispatcharr API.json\"")
        } else {
            (OPENAPI_YAML, "inline; filename=\"Dispatcharr API.yaml\"")
        };
        (
            [
                (
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(renderer.mime),
                ),
                (
                    header::CONTENT_DISPOSITION,
                    HeaderValue::from_static(disposition),
                ),
                (
                    header::HeaderName::from_static("x-dispatcharr-schema-source"),
                    HeaderValue::from_static("original-discovery-1241b3f7"),
                ),
            ],
            body,
        )
            .into_response()
    } else if uri.path() == "/api/redoc/" {
        ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], REDOC).into_response()
    } else {
        let cookie = crate::admin_channelgroup::cookie(headers, "csrftoken");
        let (_, masked, set_cookie) = dispatcharr_store::source_browsable_csrf(cookie);
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
            SWAGGER.replace(CSRF_SLOT, &masked),
        )
            .into_response()
    };
    if schema {
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
    } else if uri.path() != "/api/redoc/" {
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Cookie"));
    }
    response
}

async fn documentation(
    State(auth): State<AuthService>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Result<Response, StoreError> {
    let schema = matches!(uri.path(), "/api/schema/" | "/api/swagger.json");
    let renderers = if schema {
        SCHEMA_RENDERERS
    } else {
        HTML_RENDERERS
    };
    let renderer = negotiate(&uri, &headers, renderers)?;
    auth.authenticate(&supplied_auth(peer, &headers)).await?;
    Ok(success(&uri, &headers, renderer, schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
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
    async fn exact_schema_aliases_and_media() {
        for (path, accept, content_type) in [
            ("/api/schema/", "*/*", "application/vnd.oai.openapi"),
            ("/api/schema/", "application/yaml", "application/yaml"),
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
            assert_eq!(
                response.headers()["x-dispatcharr-schema-source"],
                "original-discovery-1241b3f7"
            );
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
