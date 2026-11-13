//! Pinned Django VOD converter nonmatches reach its final React TemplateView.
//! Source context and encoded-path limits are documented in the private proposal.
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use std::{path::PathBuf, sync::Arc};

// UUIDConverter's lexical domain, not Rust's more permissive UUID parser.
fn source_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

fn source_int(value: &str) -> bool {
    // Actual pinned IntConverter/Python witness includes the 4300-digit limit.
    !value.is_empty() && value.len() <= 4300 && value.bytes().all(|b| b.is_ascii_digit())
}

fn source_vod_match(path: &str) -> bool {
    if matches!(path, "stats/" | "stop_client/") {
        return true;
    }
    // Only six stack slots, never an allocation proportional to segment count.
    let mut parts = path.split('/');
    let parts = [
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ];
    let (Some(kind), Some(content)) = (parts[0], parts[1]) else {
        return false;
    };
    if kind.is_empty() || !source_uuid(content) {
        return false;
    }
    match (parts[2], parts[3], parts[4], parts[5]) {
        (None, None, None, None) => true,
        (Some(session), None, None, None) => !session.is_empty(),
        (Some(profile), Some(""), None, None) => source_int(profile),
        (Some(session), Some(profile), Some(""), None) => {
            !session.is_empty() && source_int(profile)
        }
        _ => false,
    }
}

fn fallthrough(path: &str) -> bool {
    // URI/WSGI percent-decoding equivalence has not been qualified. Preserve the
    // existing native behavior for encoded or raw non-ASCII paths in this slice.
    if !path.is_ascii() || path.contains('%') {
        return false;
    }
    path.strip_prefix("/proxy/vod/")
        .is_some_and(|tail| !source_vod_match(tail))
}

pub fn with_index(router: Router, index: PathBuf) -> Router {
    router.layer(middleware::from_fn_with_state(Arc::new(index), apply))
}

async fn apply(State(index): State<Arc<PathBuf>>, mut request: Request, next: Next) -> Response {
    if request.method() != Method::GET || !fallthrough(request.uri().path()) {
        return next.run(request).await;
    }
    // TemplateView ignores file ranges/validators. This does not rewrite the
    // URI or forward Authorization to any auth/provider route.
    for name in [
        header::RANGE,
        header::IF_RANGE,
        header::IF_MODIFIED_SINCE,
        header::IF_UNMODIFIED_SINCE,
        header::IF_NONE_MATCH,
        header::IF_MATCH,
    ] {
        request.headers_mut().remove(name);
    }
    // Reuse Main's existing static-file primitive: the body owns the open file,
    // fixed read chunks, and pending reads. No read-to-end/cache/new allowance.
    match tower_http::services::ServeFile::new(index.as_ref())
        .with_buf_chunk_size(16 * 1024)
        .try_call(request)
        .await
    {
        Ok(response) if response.status() == StatusCode::OK => {
            let mut response = response.map(Body::new);
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("text/html; charset=utf-8"),
            );
            response.headers_mut().remove(header::ACCEPT_RANGES);
            response
        }
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Bundled React unavailable",
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::Request, routing::any};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;

    const UUID: &str = "abcdef01-2345-6789-abcd-0123456789ef";
    const PAGE: &[u8] = b"<!doctype html><html>owned router index</html>";

    struct Index(PathBuf);
    impl Index {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = std::env::temp_dir().join(format!(
                "dispatcharr-vod-react-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join("index.html"), PAGE).unwrap();
            Self(root)
        }
        fn path(&self) -> PathBuf {
            self.0.join("index.html")
        }
    }
    impl Drop for Index {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn router(index: &Index, calls: Arc<AtomicUsize>) -> Router {
        let handler = move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                (
                    StatusCode::IM_A_TEAPOT,
                    "downstream auth/provider/API sentinel",
                )
            }
        };
        let routes = Router::new()
            .route(
                "/proxy/vod/{kind}/{content}/{session}",
                any(handler.clone()),
            )
            .route("/proxy/vod/stop_client/", any(handler.clone()))
            .route("/api/accounts/users/me/", any(handler.clone()))
            .fallback(handler);
        with_index(routes, index.path())
    }

    #[tokio::test]
    async fn malformed_uuid_serves_actual_index_before_auth_or_provider() {
        let index = Index::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let response = router(&index, calls.clone())
            .oneshot(
                Request::get("/proxy/vod/movie/not-a-uuid/owned-method-route?ignored=query")
                    .header(header::AUTHORIZATION, "Bearer invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            PAGE
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn malformed_converter_ignores_raw_invalid_authentication_header() {
        let index = Index::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let response = router(&index, calls.clone())
            .oneshot(
                Request::get("/proxy/vod/movie/not-a-uuid/session")
                    .header(
                        header::AUTHORIZATION,
                        axum::http::HeaderValue::from_bytes(&[0xff]).unwrap(),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            PAGE
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn valid_vod_and_api_and_non_get_keep_downstream_dispatch() {
        let index = Index::new();
        let calls = Arc::new(AtomicUsize::new(0));
        for (method, path) in [
            (Method::GET, format!("/proxy/vod/movie/{UUID}/session")),
            (Method::HEAD, "/proxy/vod/movie/not-a-uuid/session".into()),
            (Method::POST, "/proxy/vod/movie/not-a-uuid/session".into()),
            (Method::GET, "/proxy/vod/stop_client/".into()),
            (Method::GET, "/api/accounts/users/me/".into()),
            (Method::GET, "/proxy/vod/movie/%6eot-a-uuid/session".into()),
        ] {
            let response = router(&index, calls.clone())
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn template_get_ignores_file_range_and_conditionals() {
        let index = Index::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let response = router(&index, calls.clone())
            .oneshot(
                Request::get("/proxy/vod/movie/not-a-uuid/session")
                    .header(header::RANGE, "bytes=0-1")
                    .header(header::IF_NONE_MATCH, "*")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key(header::CONTENT_RANGE));
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
            PAGE
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn missing_configured_index_is_small_error_without_downstream() {
        let index = Index::new();
        std::fs::remove_file(index.path()).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let response = router(&index, calls.clone())
            .oneshot(
                Request::get("/proxy/vod/movie/not-a-uuid/session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(to_bytes(response.into_body(), 1024).await.unwrap().len() < 128);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn streamed_response_owns_configured_file_until_body_drop() {
        use std::os::unix::fs::MetadataExt;
        let index = Index::new();
        std::fs::write(index.path(), vec![b'x'; 128 * 1024]).unwrap();
        let metadata = std::fs::metadata(index.path()).unwrap();
        let is_open = || {
            std::fs::read_dir("/proc/self/fd")
                .unwrap()
                .filter_map(Result::ok)
                .any(|fd| {
                    std::fs::metadata(fd.path()).is_ok_and(|value| {
                        value.ino() == metadata.ino() && value.dev() == metadata.dev()
                    })
                })
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let response = router(&index, calls.clone())
            .oneshot(
                Request::get("/proxy/vod/movie/not-a-uuid/session")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(is_open(), "the retained response body owns the actual file");
        drop(response);
        for _ in 0..40 {
            if !is_open() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(!is_open(), "body retirement releases its file owner");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn pinned_converter_boundaries_not_uuid_or_u64_parser_approximations() {
        for path in [
            format!("movie/{UUID}"),
            format!("anything/{UUID}/session"),
            format!("movie/{UUID}/18446744073709551616/"),
            format!("movie/{UUID}/session/0001/"),
            format!("movie/{UUID}/{}/", "0".repeat(4300)),
            "stats/".into(),
            "stop_client/".into(),
        ] {
            assert!(source_vod_match(&path), "{path}");
        }
        for path in [
            format!("movie/{}/session", UUID.to_uppercase()),
            format!("movie/{}/session", UUID.replace('-', "")),
            format!("movie/{UUID}/+1/"),
            format!("movie/{UUID}/١/"),
            format!("movie/{UUID}/{}/", "0".repeat(4301)),
            format!("movie/{UUID}/session/"),
            format!("movie/{UUID}/session/1"),
            format!("movie/{UUID}//1/"),
            format!("movie/{UUID}/session/1/extra/"),
        ] {
            assert!(!source_vod_match(&path), "{path}");
        }
    }

    #[test]
    fn encoded_and_other_backend_domains_are_explicitly_unchanged() {
        for path in [
            "/api/missing",
            "/proxy/ts/not-a-uuid",
            "/proxy/vod",
            "/proxy/vod/movie/%61/session",
            "/proxy/vod/movie/é/session",
        ] {
            assert!(!fallthrough(path), "{path}");
        }
        assert!(fallthrough(
            "/proxy/vod/movie/not-a-uuid/owned-method-route"
        ));
    }
}
