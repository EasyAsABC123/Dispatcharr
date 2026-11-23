//! Public admin nginx ingress compatibility. Original Dispatcharr and nginx
//! authors retain credit for the routing behavior and response representation.
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use url::Url;

const PUBLIC_ORIGIN: &str = "DISPATCHARR_PUBLIC_ORIGIN";
const INVALID_ORIGIN: &str = "DISPATCHARR_PUBLIC_ORIGIN must be an HTTP or HTTPS origin";
const REDIRECT_BODY: &str = concat!(
    "<html>\r\n",
    "<head><title>301 Moved Permanently</title></head>\r\n",
    "<body>\r\n",
    "<center><h1>301 Moved Permanently</h1></center>\r\n",
    "<hr><center>nginx/1.28.3</center>\r\n",
    "</body>\r\n",
    "</html>\r\n",
);

#[derive(Clone)]
pub(super) struct AdminIngress {
    pub(super) xc_live: bool,
    pub(super) location: HeaderValue,
    pub(super) channel_groups: Option<dispatcharr_store::DjangoAdminSessions>,
}

pub(super) fn configured_location() -> Result<HeaderValue, &'static str> {
    match std::env::var(PUBLIC_ORIGIN) {
        Ok(value) => login_location(Some(&value)),
        Err(std::env::VarError::NotPresent) => login_location(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(INVALID_ORIGIN),
    }
}

fn login_location(value: Option<&str>) -> Result<HeaderValue, &'static str> {
    let Some(value) = value else {
        return Ok(HeaderValue::from_static("/login"));
    };
    let authority = value
        .strip_prefix("http://")
        .or_else(|| value.strip_prefix("https://"))
        .ok_or(INVALID_ORIGIN)?;
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    if authority.is_empty()
        || authority.contains(['/', '\\', '?', '#', '@', '%'])
        || authority
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(INVALID_ORIGIN);
    }
    let url = Url::parse(value).map_err(|_| INVALID_ORIGIN)?;
    if url.host().is_none() {
        return Err(INVALID_ORIGIN);
    }
    HeaderValue::from_str(&format!("{}/login", url.origin().ascii_serialization()))
        .map_err(|_| INVALID_ORIGIN)
}

fn two_nonempty_tails(tail: &str) -> bool {
    tail.split_once('/').is_some_and(|(first, second)| {
        !first.is_empty() && !second.is_empty() && !second.contains('/')
    })
}

pub(super) async fn apply(
    State(ingress): State<AdminIngress>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if let Some(asset) = super::admin_channelgroup_assets::serve(path, request.method()) {
        return asset;
    }
    if path == super::admin_channelgroup::PATH && request.method() == Method::GET {
        let (parts, _) = request.into_parts();
        return super::admin_channelgroup::handle(
            ingress.channel_groups.as_ref(),
            &parts.uri,
            &parts.headers,
        )
        .await;
    }
    if let Some(tail) = path.strip_prefix("/admin/") {
        if ingress.xc_live && two_nonempty_tails(tail) {
            return next.run(request).await;
        }
        // nginx exempts two components even with a terminal slash. Other
        // dedicated admin paths are still outside this native route.
        if two_nonempty_tails(tail.strip_suffix('/').unwrap_or(tail)) {
            return super::api_not_found().await.into_response();
        }
    } else if path != "/admin" {
        return next.run(request).await;
    }

    let body = if request.method() == Method::HEAD {
        Body::empty()
    } else {
        Body::from(REDIRECT_BODY)
    };
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::MOVED_PERMANENTLY;
    response.headers_mut().extend([
        (header::LOCATION, ingress.location),
        (header::CONTENT_TYPE, HeaderValue::from_static("text/html")),
        (
            header::CONTENT_LENGTH,
            HeaderValue::from(REDIRECT_BODY.len()),
        ),
        (header::SERVER, HeaderValue::from_static("nginx/1.28.3")),
    ]);
    response
        .extensions_mut()
        .insert(super::response_headers::PublicAdminRedirect);
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, middleware, routing::get, Router};
    use tower::ServiceExt;

    fn router(location: HeaderValue, xc_live: bool) -> Router {
        Router::new()
            .route(
                "/{username}/{password}/{channel}",
                get(|| async { StatusCode::OK }),
            )
            .fallback(|| async { StatusCode::ACCEPTED })
            .layer(middleware::from_fn_with_state(
                AdminIngress {
                    xc_live,
                    location,
                    channel_groups: None,
                },
                apply,
            ))
            .layer(middleware::from_fn(crate::response_headers::apply))
    }

    #[tokio::test]
    async fn redirects_the_measured_nginx_forms_without_using_request_authority() {
        let origin = "http://owned-admin.test:9192";
        let app = router(login_location(Some(origin)).unwrap(), true);
        for (method, uri) in [
            (Method::GET, "/admin"),
            (Method::GET, "/admin/"),
            (Method::GET, "/admin/login/"),
            (Method::GET, "/admin/login/?next=%2Fadmin%2F"),
            (Method::GET, "/admin/accounts/user/add/"),
            (Method::HEAD, "/admin/login/"),
            (Method::POST, "/admin/login/"),
            (Method::OPTIONS, "/admin/login/"),
            (Method::DELETE, "/admin/login/"),
        ] {
            let request = Request::builder()
                .method(method.clone())
                .uri(uri)
                .header(header::HOST, "untrusted.example")
                .header("forwarded", "proto=https;host=untrusted.example")
                .header("x-forwarded-host", "untrusted.example")
                .body(Body::from("field=value"))
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{uri}");
            let headers = response.headers();
            assert_eq!(
                headers[header::LOCATION],
                "http://owned-admin.test:9192/login"
            );
            assert_eq!(headers[header::CONTENT_TYPE], "text/html");
            assert_eq!(headers[header::CONTENT_LENGTH], "169");
            assert_eq!(headers[header::SERVER], "nginx/1.28.3");
            assert!(!headers.contains_key(header::VARY));
            assert!(!headers.contains_key("x-content-type-options"));
            let body = to_bytes(response.into_body(), 169).await.unwrap();
            let expected = if method == Method::HEAD {
                ""
            } else {
                REDIRECT_BODY
            };
            assert_eq!(body.as_ref(), expected.as_bytes());
        }
    }

    #[tokio::test]
    async fn preserves_exclusions_fallback_and_relative_redirect() {
        for (xc_live, uri, expected) in [
            (true, "/admin/owned/123.ts", StatusCode::OK),
            (false, "/admin/owned/123.ts", StatusCode::NOT_FOUND),
            (true, "/admin/owned/123.ts/", StatusCode::NOT_FOUND),
            (true, "/admin/accounts/user/", StatusCode::NOT_FOUND),
            (true, "/administrator", StatusCode::ACCEPTED),
            (true, "/admin/", StatusCode::MOVED_PERMANENTLY),
        ] {
            let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let response = router(login_location(None).unwrap(), xc_live)
                .oneshot(request)
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{uri}");
            if expected == StatusCode::MOVED_PERMANENTLY {
                assert_eq!(response.headers()[header::LOCATION], "/login");
            } else {
                assert!(!response.headers().contains_key(header::LOCATION));
            }
        }
    }

    #[tokio::test]
    async fn exact_channelgroup_read_and_admin_assets_precede_the_legacy_boundary() {
        let app = router(login_location(None).unwrap(), true);
        let path = super::super::admin_channelgroup::PATH;
        for (method, uri, status) in [
            (Method::GET, path, StatusCode::FOUND),
            (Method::POST, path, StatusCode::NOT_FOUND),
            (Method::GET, "/admin/login/", StatusCode::MOVED_PERMANENTLY),
            (Method::GET, "/admin/owned/123.ts", StatusCode::OK),
            (Method::GET, "/static/admin/css/base.css", StatusCode::OK),
            (Method::HEAD, "/static/admin/css/base.css", StatusCode::OK),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method.clone())
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status, "{method} {uri}");
            if uri == "/static/admin/css/base.css" {
                let length: usize = response.headers()[header::CONTENT_LENGTH]
                    .to_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                assert_eq!(response.headers()[header::CONTENT_TYPE], "text/css");
                let body = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
                assert_eq!(body.len(), if method == Method::HEAD { 0 } else { length });
            }
        }
    }

    #[test]
    fn accepts_only_configured_http_authorities() {
        for (origin, expected) in [
            (
                "https://Dispatcharr.Example/",
                "https://dispatcharr.example/login",
            ),
            ("http://[::1]:9192", "http://[::1]:9192/login"),
        ] {
            assert_eq!(login_location(Some(origin)).unwrap(), expected);
        }
        for origin in [
            "",
            "//untrusted.example",
            "ftp://example.com",
            "http://u:p@example.com",
            "http://@example.com",
            "http://example.com:65536",
            "http://example.com/%2f",
            "http://example.com/path",
            "http://example.com/?x=y",
            "http://example.com/#gone",
            "http://example.com\\elsewhere",
            "http://example.com\r\nInjected: injected",
        ] {
            assert!(login_location(Some(origin)).is_err(), "{origin:?}");
        }
    }
}
