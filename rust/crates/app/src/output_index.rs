//! Bare output index redirect from the original Dispatcharr URL configuration.
//! Original application credit retained in ATTRIBUTION.md.
use axum::{
    body,
    extract::Request,
    http::{header, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};

pub(super) fn router() -> Router {
    Router::new().route("/output", any(redirect))
}

async fn redirect(request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let form = if parts.method == Method::POST
        && parts
            .headers
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| {
                value
                    .as_bytes()
                    .starts_with(b"application/x-www-form-urlencoded")
            }) {
        match body::to_bytes(body, 64 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        }
    } else {
        body::Bytes::new()
    };
    if let Some(failure) =
        dispatcharr_output_http::source_csrf_failure(&parts.method, &parts.headers, &form)
    {
        return (
            StatusCode::FORBIDDEN,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            failure,
        )
            .into_response();
    }
    const HTML: (header::HeaderName, &str) = (header::CONTENT_TYPE, "text/html; charset=utf-8");
    const EMPTY: (header::HeaderName, &str) = (header::CONTENT_LENGTH, "0");
    if matches!(
        parts.method,
        Method::GET
            | Method::HEAD
            | Method::POST
            | Method::PUT
            | Method::PATCH
            | Method::DELETE
            | Method::OPTIONS
    ) {
        return (
            StatusCode::MOVED_PERMANENTLY,
            [HTML, EMPTY, (header::LOCATION, "/output/")],
        )
            .into_response();
    }
    (
        StatusCode::METHOD_NOT_ALLOWED,
        [
            HTML,
            EMPTY,
            (
                header::ALLOW,
                "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS",
            ),
        ],
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    #[tokio::test]
    async fn output_index_redirects_to_the_literal_relative_target_with_empty_body() {
        for method in [Method::GET, Method::HEAD, Method::OPTIONS] {
            let request = Request::builder()
                .method(method.clone())
                .uri("/output?days=2")
                .body(Body::empty())
                .unwrap();
            let response = router().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{method}");
            assert_eq!(response.headers()[header::LOCATION], "/output/");
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
            assert_eq!(response.headers()[header::CONTENT_LENGTH], "0");
            assert!(body::to_bytes(response.into_body(), 0)
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[tokio::test]
    async fn output_index_keeps_csrf_and_allows_no_trace_or_child_redirect() {
        let send = |method, path| {
            router().oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
        };
        let post = send(Method::POST, "/output").await.unwrap();
        assert_eq!(post.status(), StatusCode::FORBIDDEN);
        assert!(!post.headers().contains_key(header::LOCATION));
        let trace = send(Method::TRACE, "/output").await.unwrap();
        assert_eq!(trace.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(
            trace.headers()[header::ALLOW],
            "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS"
        );
        for path in ["/output/", "/output/m3u", "/output/epg"] {
            let response = send(Method::GET, path).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert!(!response.headers().contains_key(header::LOCATION));
        }
    }
}
