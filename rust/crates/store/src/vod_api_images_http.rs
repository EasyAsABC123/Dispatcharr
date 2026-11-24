//! Source-derived VOD image/cache component and ownership controls.
//! Actual Original/native240 and physical/mixed host gates passed;
//! canonical app, unchanged React and broader image contracts are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_owned_read::HttpScope;
use super::{
    vod_api_http::{json_indent, pretty_document, HttpGate},
    vod_api_images_read::{Image, Images},
    vod_api_owned_read::{Document, ReadError, ReadRuntime},
    vod_api_read_component::HttpRead,
};
use axum::{
    body::{to_bytes, Body},
    extract::{rejection::PathRejection, ConnectInfo, Path},
    http::{HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use bytes::Bytes;
use std::{net::SocketAddr, sync::Arc};

struct Frame {
    bytes: Bytes,
    _slot: HttpScope,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}
#[derive(Clone)]
pub(super) struct ImageHttp {
    auth: crate::AuthService,
    reader: Images,
    gate: HttpGate,
}
fn headers(response: &mut Response) {
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("GET, HEAD, OPTIONS"));
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Accept, origin"));
    response.headers_mut().insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
}
impl ImageHttp {
    pub(super) fn shared(auth: crate::AuthService, runtime: ReadRuntime, gate: HttpGate) -> Self {
        Self {
            reader: Images::shared(auth.clone(), runtime),
            auth,
            gate,
        }
    }
    pub(super) fn router(&self) -> Router {
        let mut router = Router::new();
        for (path, image) in [
            ("/api/vod/movies/{id}/image/", Image::Movie),
            ("/api/vod/series/{id}/image/", Image::Series),
            ("/api/vod/episodes/{id}/image/", Image::Episode),
            ("/api/vod/vodlogos/{id}/cache/", Image::Logo),
        ] {
            let state = self.clone();
            router = router.route(
                path,
                any(
                    move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                          request: axum::extract::Request| {
                        let state = state.clone();
                        async move { state.respond(image, peer, request).await }
                    },
                ),
            );
            // The pinned Django URLconf uses the numeric path converter on
            // these explicit routes. Serve them directly through image/cache.
            let state = self.clone();
            router = router.route(
                path.trim_end_matches('/'),
                any(
                    move |path_id: Result<Path<String>, PathRejection>,
                          ConnectInfo(peer): ConnectInfo<SocketAddr>,
                          request: axum::extract::Request| {
                        let state = state.clone();
                        async move {
                            let Ok(Path(id)) = path_id else {
                                return StatusCode::NOT_FOUND.into_response();
                            };
                            if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
                                return StatusCode::NOT_FOUND.into_response();
                            }
                            state.respond_with_id(image, peer, request, Some(id)).await
                        }
                    },
                ),
            );
        }
        router
    }
    pub(super) async fn respond(
        &self,
        image: Image,
        peer: SocketAddr,
        request: axum::extract::Request,
    ) -> Response {
        let raw = request
            .uri()
            .path()
            .trim_end_matches('/')
            .rsplit('/')
            .nth(1)
            .map(str::to_owned);
        self.respond_with_id(image, peer, request, raw).await
    }

    async fn respond_with_id(
        &self,
        image: Image,
        peer: SocketAddr,
        request: axum::extract::Request,
        raw: Option<String>,
    ) -> Response {
        let (parts, body) = request.into_parts();
        drop(body);
        let raw_image = matches!(parts.method, Method::GET | Method::HEAD);
        let head = parts.method == Method::HEAD;
        let input = parts
            .headers
            .iter()
            .fold(
                parts.uri.to_string().len().checked_add(1024),
                |total, (key, value)| {
                    total
                        .and_then(|n| n.checked_add(key.as_str().len()))
                        .and_then(|n| n.checked_add(value.as_bytes().len()))
                },
            )
            .and_then(|total| {
                parts
                    .headers
                    .capacity()
                    .checked_mul(128)
                    .and_then(|metadata| total.checked_add(metadata))
            });
        let Some((slot, deadline)) = (match input {
            Some(bytes) => self.gate.acquire(bytes).await,
            None => None,
        }) else {
            let mut response = crate::StoreError::StatusBody(
                503,
                serde_json::json!({"detail":"VOD HTTP capacity unavailable."}),
            )
            .into_response();
            headers(&mut response);
            return response;
        };
        let indent = if raw_image {
            None
        } else {
            json_indent(&parts.headers)
        };
        let result = self
            .reader
            .dispatch(
                image,
                peer,
                parts.headers,
                HttpRead {
                    method: parts.method,
                    uri: parts.uri,
                    raw_detail: raw,
                    scope: Some(slot.clone()),
                    deadline,
                },
            )
            .await;
        let mut response = match result {
            Ok(bytes) if raw_image => {
                let Ok(url) = std::str::from_utf8(&bytes) else {
                    return super::vod_api_category_http::source_server_error(slot, head);
                };
                // Scope remains attached through cached settings replies even when caller
                // cancellation drops the local image future. Image body bytes retain the
                // existing shared ImageRuntime's admission, not another response pool.
                let auth = self
                    .auth
                    .clone()
                    .with_retained_scope(Arc::new((slot.clone(), bytes.clone())));
                match auth.image_response(url).await {
                    Ok(response) => response,
                    Err(error) => {
                        let response = error.into_response();
                        let (parts, body) = response.into_parts();
                        let bytes = to_bytes(body, 64 * 1024).await.unwrap_or_else(|_| {
                            Bytes::from_static(
                                b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                            )
                        });
                        let length = bytes.len();
                        let mut response = Response::from_parts(
                            parts,
                            Body::from(Bytes::from_owner(Frame {
                                bytes,
                                _slot: slot.clone(),
                            })),
                        );
                        response
                            .headers_mut()
                            .insert("content-length", length.into());
                        response
                    }
                }
            }
            Ok(bytes) => {
                let bytes = match indent {
                    None => bytes,
                    Some(indent) => {
                        match pretty_document(&bytes, indent, Document::bounded(64 * 1024)) {
                            Ok(data) => Bytes::from(data),
                            Err(_) => Bytes::from_static(
                                b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                            ),
                        }
                    }
                };
                let length = bytes.len();
                let mut response = Response::new(Body::from(Bytes::from_owner(Frame {
                    bytes,
                    _slot: slot.clone(),
                })));
                response
                    .headers_mut()
                    .insert("content-type", HeaderValue::from_static("application/json"));
                response
                    .headers_mut()
                    .insert("content-length", length.into());
                response
            }
            Err(
                ReadError::Source(
                    crate::StoreError::Database(_) | crate::StoreError::UiPermissionFault,
                )
                | ReadError::SourceUnhandledFault(_),
            ) => {
                let mut response = super::vod_api_category_http::source_server_error(slot, head);
                response.headers_mut().insert(
                    "x-content-type-options",
                    HeaderValue::from_static("nosniff"),
                );
                return response;
            }
            Err(error) => {
                let error = match error {
                    ReadError::Source(error) => error,
                    ReadError::Unsupported(message) => crate::StoreError::Unsupported(message),
                    _ => crate::StoreError::StatusBody(
                        503,
                        serde_json::json!({"detail":"VOD read capacity unavailable."}),
                    ),
                };
                let response = error.into_response();
                let (parts, body) = response.into_parts();
                let mut bytes = to_bytes(body, 64 * 1024).await.unwrap_or_else(|_| {
                    Bytes::from_static(
                        b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                    )
                });
                if let Some(indent) = indent {
                    if let Ok(data) = pretty_document(&bytes, indent, Document::bounded(64 * 1024))
                    {
                        bytes = data.into();
                    }
                }
                let length = bytes.len();
                let mut response = Response::from_parts(
                    parts,
                    Body::from(Bytes::from_owner(Frame {
                        bytes,
                        _slot: slot.clone(),
                    })),
                );
                response
                    .headers_mut()
                    .insert("content-length", length.into());
                response
            }
        };
        headers(&mut response);
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        http::{header, Request},
        Extension,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn vod_artwork_direct_paths_use_the_numeric_source_converter() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy_with(sqlx::postgres::PgConnectOptions::new());
        let auth = crate::AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("owned-vod-artwork-route-fixture".into()).unwrap(),
        );
        let limits = super::super::vod_api_owned_read::ReadLimits::default();
        let reads = super::super::VodReads::new(auth, limits).unwrap();
        let peer: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let app = reads.vod_image_router().layer(Extension(ConnectInfo(peer)));
        for (resource, action) in [
            ("movies", "image"),
            ("series", "image"),
            ("episodes", "image"),
            ("vodlogos", "cache"),
        ] {
            for (id, status) in [
                ("12", StatusCode::NOT_ACCEPTABLE),
                ("0%31", StatusCode::NOT_ACCEPTABLE),
                ("sample", StatusCode::NOT_FOUND),
            ] {
                let uri = format!("/api/vod/{resource}/{id}/{action}");
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(&uri)
                            .method(Method::OPTIONS)
                            .header(header::ACCEPT, "application/xml")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), status, "{uri}");
                assert!(!response.headers().contains_key(header::LOCATION), "{uri}");
            }
        }
    }
}
