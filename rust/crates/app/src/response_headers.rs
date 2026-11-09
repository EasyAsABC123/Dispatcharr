//! Response defaults captured from the pinned original Django middleware.
//! Original application credit retained in ATTRIBUTION.md.
use axum::{
    extract::Request,
    http::{header, HeaderValue},
    middleware::Next,
    response::Response,
};

pub async fn apply(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
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
        headers.append(header::VARY, HeaderValue::from_static("origin"));
    }
    response
}
