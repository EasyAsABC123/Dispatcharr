//! Scoped unified JSON HTTP extension sharing ordinary/category admission.
//! Actual TCP/body/header/ownership gates passed; app/React/browsable HTML
//! acceptance remains separate. No additional runtime or shutdown service.
//! Original Dispatcharr authors/contributors retain credit and AGPL licensing.
use super::{
    vod_api_http::{json_indent, pretty_document, HttpGate},
    vod_api_owned_read::{ReadError, ReadRuntime},
    vod_api_read_component::HttpRead,
    vod_api_unified_dispatch::{UnifiedReads, UnifiedRequest},
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
        &self.data
    }
}
#[derive(Clone)]
pub(super) struct UnifiedHttp {
    pub reader: UnifiedReads,
    pub gate: HttpGate,
    auth: crate::AuthService,
}
impl UnifiedHttp {
    pub fn shared(auth: crate::AuthService, runtime: ReadRuntime, gate: HttpGate) -> Self {
        Self {
            reader: UnifiedReads::from_runtime(auth.clone(), runtime),
            gate,
            auth,
        }
    }
    pub fn router(&self) -> Router {
        let mut router = Router::new();
        for (path, detail) in [("/api/vod/all/", false), ("/api/vod/all/{id}/", true)] {
            let state = self.clone();
            router = router.route(
                path,
                any(
                    move |ConnectInfo(peer): ConnectInfo<SocketAddr>,
                          request: axum::extract::Request| {
                        let state = state.clone();
                        async move { state.respond(peer, detail, request).await }
                    },
                ),
            );
        }
        router
    }
    pub async fn respond(
        &self,
        peer: SocketAddr,
        detail: bool,
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
                    .map_or(0, |v| v.as_str().len())
                    .checked_add(1024),
                |total, (name, value)| {
                    total
                        .and_then(|v| v.checked_add(name.as_str().len()))
                        .and_then(|v| v.checked_add(value.as_bytes().len()))
                },
            )
            .and_then(|v| {
                parts
                    .headers
                    .capacity()
                    .checked_mul(128)
                    .and_then(|metadata| v.checked_add(metadata))
            });
        let acquired = match input {
            Some(input) => self.gate.acquire(input).await,
            None => None,
        };
        let Some((slot, deadline)) = acquired else {
            return response(
                503,
                bytes::Bytes::from_static(b"{\"detail\":\"VOD HTTP capacity unavailable.\"}"),
                None,
                head,
            );
        };
        let origin = crate::logos::origin(&self.auth, peer.ip(), &parts.headers);
        let indent = json_indent(&parts.headers);
        let raw_detail = detail.then(|| {
            parts
                .uri
                .path()
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("")
                .to_owned()
        });
        let input = UnifiedRequest {
            peer,
            headers: parts.headers,
            origin,
            path: parts.uri.path().into(),
        };
        let result = self
            .reader
            .dispatch(
                input,
                HttpRead {
                    method: parts.method,
                    uri: parts.uri,
                    raw_detail,
                    scope: Some(slot.clone()),
                    deadline,
                },
            )
            .await;
        match result {
            Ok(reply) => {
                if let Some(html)=reply.html {
                    let length=reply.bytes.len();let body=if head{Body::empty()}else{Body::from(bytes::Bytes::from_owner(Frame{data:reply.bytes,_slot:slot}))};
                    let mut response=Response::new(body);*response.status_mut()=axum::http::StatusCode::from_u16(html.status).expect("internal renderer status");
                    response.headers_mut().insert("content-type",HeaderValue::from_static("text/html; charset=utf-8"));
                    response.headers_mut().insert("content-length",length.into());response.headers_mut().insert("allow",HeaderValue::from_static("GET, HEAD, OPTIONS"));response.headers_mut().insert("vary",HeaderValue::from_static("Accept, origin, Cookie"));
                    response.headers_mut().insert("set-cookie",HeaderValue::from_str(&html.set_cookie).expect("bounded ASCII CSRF cookie"));
                    if html.authenticate{response.headers_mut().insert("www-authenticate",HeaderValue::from_static("Bearer realm=\"api\""));}response
                }else{response(200,reply.bytes,Some(slot),head)}
            },
            Err(ReadError::SourceUnhandledFault(_) | ReadError::Source(crate::StoreError::UiPermissionFault)) => {
                super::vod_api_category_http::source_server_error(slot, head)
            }
            Err(error) => {
                let error = match error {
                    ReadError::Source(error) => error,
                    ReadError::Unsupported(reason) => crate::StoreError::Unsupported(reason),
                    _ => crate::StoreError::StatusBody(
                        503,
                        serde_json::json!({"detail":"VOD read capacity unavailable."}),
                    ),
                };
                let rendered = error.into_response();
                let (mut parts, body) = rendered.into_parts();
                let bytes = match to_bytes(body, 64 * 1024).await {
                    Ok(bytes) => {
                        match indent {
                            Some(indent) => match pretty_document(
                                &bytes,
                                indent,
                                super::vod_api_owned_read::Document::bounded(64 * 1024),
                            ) {
                                Ok(bytes) => bytes::Bytes::from(bytes),
                                Err(_) => {
                                    parts.status = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                                    bytes::Bytes::from_static(b"{\"detail\":\"VOD error representation exceeds capacity.\"}")
                                }
                            },
                            None => bytes,
                        }
                    }
                    Err(_) => {
                        parts.status = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                        bytes::Bytes::from_static(
                            b"{\"detail\":\"VOD error representation exceeds capacity.\"}",
                        )
                    }
                };
                let length = bytes.len();
                let body = if head {
                    Body::empty()
                } else {
                    Body::from(bytes::Bytes::from_owner(Frame {
                        data: bytes,
                        _slot: slot,
                    }))
                };
                let mut response = Response::from_parts(parts, body);
                response
                    .headers_mut()
                    .insert("content-length", length.into());
                common(&mut response);
                response
            }
        }
    }
}
fn common(response: &mut Response) {
    response
        .headers_mut()
        .insert("allow", HeaderValue::from_static("GET, HEAD, OPTIONS"));
    response
        .headers_mut()
        .insert("vary", HeaderValue::from_static("Accept, origin"));
}
fn response(
    status: u16,
    bytes: bytes::Bytes,
    slot: Option<Arc<OwnedSemaphorePermit>>,
    head: bool,
) -> Response {
    let length = bytes.len();
    let body = if head {
        Body::empty()
    } else {
        Body::from(match slot {
            Some(slot) => bytes::Bytes::from_owner(Frame {
                data: bytes,
                _slot: slot,
            }),
            None => bytes,
        })
    };
    let mut response = Response::new(body);
    *response.status_mut() = axum::http::StatusCode::from_u16(status).unwrap();
    response
        .headers_mut()
        .insert("content-type", HeaderValue::from_static("application/json"));
    response
        .headers_mut()
        .insert("content-length", length.into());
    common(&mut response);
    response
}

#[cfg(test)]
#[path = "vod_api_unified_http_tests.rs"]
mod tests;
