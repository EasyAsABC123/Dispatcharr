//! Explicit provider/action router sharing ordinary read admission and memory.
//! Actual Source/native component gates passed; app/browser acceptance is
//! separate. Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_actions_read::{Action, ActionReads},
    vod_api_http::{json_indent, pretty_document, HttpGate},
    vod_api_owned_read::{Document, ReadError, ReadRuntime},
    vod_api_read_component::HttpRead,
};
use axum::{
    body::{to_bytes, Body},
    extract::ConnectInfo,
    http::{HeaderValue, Method},
    response::{IntoResponse, Response},
    routing::any,
    Router,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;
struct Frame {
    data: bytes::Bytes,
    _slot: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
#[derive(Clone)]
pub(super) struct ActionHttp {
    reader: ActionReads,
    gate: HttpGate,
}
impl ActionHttp {
    pub(super) fn shared(auth: crate::AuthService, runtime: ReadRuntime, gate: HttpGate) -> Self {
        Self {
            reader: ActionReads::shared(auth, runtime),
            gate,
        }
    }
    pub(super) fn router(&self) -> Router {
        let mut router = Router::new();
        for (path, action) in [
            ("/api/vod/movies/{id}/provider-info/", Action::MovieInfo),
            ("/api/vod/series/{id}/provider-info/", Action::SeriesInfo),
            ("/api/vod/movies/{id}/providers/", Action::MovieProviders),
            ("/api/vod/series/{id}/providers/", Action::SeriesProviders),
            ("/api/vod/series/{id}/episodes/", Action::SeriesEpisodes),
        ] {
            let state = self.clone();
            router = router.route(
                path,
                any(
                    move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                          request: axum::extract::Request| {
                        let state = state.clone();
                        async move { state.respond(action, peer, request).await }
                    },
                ),
            );
        }
        router
    }
    pub(super) async fn respond(
        &self,
        action: Action,
        peer: SocketAddr,
        request: axum::extract::Request,
    ) -> Response {
        let (parts, body) = request.into_parts();
        drop(body);
        let head = parts.method == Method::HEAD;
        let input = parts
            .headers
            .iter()
            .fold(
                parts
                    .uri
                    .path_and_query()
                    .map_or(0, |path| path.as_str().len())
                    .checked_add(1024),
                |total, (key, value)| {
                    total
                        .and_then(|n| n.checked_add(key.as_str().len()))
                        .and_then(|n| n.checked_add(value.as_bytes().len()))
                },
            )
            .and_then(|n| {
                parts
                    .headers
                    .capacity()
                    .checked_mul(128)
                    .and_then(|metadata| n.checked_add(metadata))
            });
        let Some((slot, deadline)) = (match input {
            Some(input) => self.gate.acquire(input).await,
            None => None,
        }) else {
            return representation(
                503,
                bytes::Bytes::from_static(b"{\"detail\":\"VOD HTTP capacity unavailable.\"}"),
                None,
                head,
            );
        };
        let raw = parts
            .uri
            .path()
            .trim_end_matches('/')
            .rsplit('/')
            .nth(1)
            .map(str::to_owned);
        let indent = json_indent(&parts.headers);
        let result = self
            .reader
            .dispatch(
                action,
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
        match result {
            Ok(data) => representation(200, data, Some(slot), head),
            Err(
                ReadError::Source(
                    crate::StoreError::Database(_) | crate::StoreError::UiPermissionFault,
                )
                | ReadError::SourceUnhandledFault(_),
            ) => super::vod_api_category_http::source_server_error(slot, head),
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
                let (mut parts, body) = response.into_parts();
                let data = match to_bytes(body, 64 * 1024).await {
                    Ok(data) => match indent {
                        Some(indent) => {
                            match pretty_document(&data, indent, Document::bounded(64 * 1024)) {
                                Ok(data) => bytes::Bytes::from(data),
                                Err(_) => {
                                    parts.status = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                    bytes::Bytes::from_static(b"{\"detail\":\"VOD error representation exceeds capacity.\"}")
                                }
                            }
                        }
                        None => data,
                    },
                    Err(_) => {
                        parts.status = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                        bytes::Bytes::from_static(
                            b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                        )
                    }
                };
                let length = data.len();
                let mut response = Response::from_parts(
                    parts,
                    if head {
                        Body::empty()
                    } else {
                        Body::from(bytes::Bytes::from_owner(Frame { data, _slot: slot }))
                    },
                );
                response
                    .headers_mut()
                    .insert("content-length", length.into());
                headers(&mut response);
                response
            }
        }
    }
}
fn headers(response: &mut Response) {
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("GET, HEAD, OPTIONS"));
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Accept, origin"));
}
fn representation(
    status: u16,
    data: bytes::Bytes,
    slot: Option<Arc<OwnedSemaphorePermit>>,
    head: bool,
) -> Response {
    let length = data.len();
    let body = if head {
        Body::empty()
    } else {
        match slot {
            Some(slot) => Body::from(bytes::Bytes::from_owner(Frame { data, _slot: slot })),
            None => Body::from(data),
        }
    };
    let mut response = Response::new(body);
    *response.status_mut() =
        axum::http::StatusCode::from_u16(status).expect("fixed internal status");
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
        .headers_mut()
        .insert("content-length", length.into());
    headers(&mut response);
    response
}
