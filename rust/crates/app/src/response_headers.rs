//! Response defaults captured from the pinned original Django middleware.
//! Original application credit retained in ATTRIBUTION.md.
use axum::{
    extract::Request,
    http::{header, HeaderValue},
    middleware::Next,
    response::Response,
};

#[derive(Clone)]
pub struct SourceStaticResponse;

/// This response originates at the public ingress before Django middleware.
#[derive(Clone)]
pub(super) struct PublicAdminRedirect;

pub async fn apply(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    if response
        .extensions()
        .get::<SourceStaticResponse>()
        .is_some()
        || response.extensions().get::<PublicAdminRedirect>().is_some()
    {
        return response;
    }
    let raw_image = response
        .extensions()
        .get::<dispatcharr_store::SourceImageResponse>()
        .is_some();
    let headers = response.headers_mut();
    if !raw_image
        && headers
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "text/html")
    {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
    }
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        ("referrer-policy", "same-origin"),
        ("cross-origin-opener-policy", "same-origin"),
    ] {
        headers
            .entry(name)
            .or_insert(HeaderValue::from_static(value));
    }
    // CorsMiddleware varies even responses to requests without an Origin.
    // Preserve existing transport/cache variation instead of overwriting it.
    let varies = headers
        .get_all(header::VARY)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|part| part.trim() == "*" || part.trim().eq_ignore_ascii_case("origin"));
    if !varies {
        // Django patch_vary_headers keeps a single comma-separated field.
        // Separate Vary lines otherwise hide Accept from simple HTTP readers.
        let existing = headers
            .get_all(header::VARY)
            .iter()
            .map(|value| value.to_str().map(str::to_owned))
            .collect::<Result<Vec<_>, _>>();
        if let Ok(existing) = existing {
            let value = if existing.is_empty() {
                "origin".to_owned()
            } else {
                format!("{}, origin", existing.join(", "))
            };
            if let Ok(value) = HeaderValue::from_str(&value) {
                headers.insert(header::VARY, value);
            }
        } else {
            // Preserve an unqualified non-text upstream field losslessly.
            headers.append(header::VARY, HeaderValue::from_static("origin"));
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, middleware, routing::get, Router};
    use tower::ServiceExt;

    #[tokio::test]
    async fn cors_preserves_negotiation_and_cookie_variation_in_one_http_field() {
        let app = Router::new()
            .route(
                "/",
                get(|| async {
                    let mut response = Response::new(Body::empty());
                    response
                        .headers_mut()
                        .append(header::VARY, HeaderValue::from_static("Accept"));
                    response
                        .headers_mut()
                        .append(header::VARY, HeaderValue::from_static("Cookie"));
                    response
                }),
            )
            .layer(middleware::from_fn(apply));
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.headers().get_all(header::VARY).iter().count(), 1);
        assert_eq!(response.headers()[header::VARY], "Accept, Cookie, origin");
    }

    #[tokio::test]
    async fn cors_respects_an_existing_wildcard_and_source_static_bypass() {
        let app = Router::new()
            .route(
                "/wildcard",
                get(|| async {
                    let mut response = Response::new(Body::empty());
                    response
                        .headers_mut()
                        .insert(header::VARY, HeaderValue::from_static("*"));
                    response
                }),
            )
            .route(
                "/static",
                get(|| async {
                    let mut response = Response::new(Body::empty());
                    response.extensions_mut().insert(SourceStaticResponse);
                    response
                }),
            )
            .layer(middleware::from_fn(apply));
        for (path, expected) in [("/wildcard", Some("*")), ("/static", None)] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response
                    .headers()
                    .get(header::VARY)
                    .map(|v| v.to_str().unwrap()),
                expected
            );
        }
    }
}
