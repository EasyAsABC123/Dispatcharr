//! Signed copied-SQLite profile read aliases. Original Dispatcharr authors retain credit.
use super::CopiedSqliteRuntime;
use crate::{catalogue_options, channel_profiles, RetainedSqliteRestReads, StoreError};
use axum::{
    extract::{ConnectInfo, OriginalUri, Path, Request, State},
    http::{HeaderMap, HeaderValue, Method},
    middleware::{from_fn_with_state, Next},
    response::Response,
    routing::get,
    Router,
};
use dispatcharr_contracts::BoxFuture;
use std::net::SocketAddr;

// The production owner supplies its normal bounded read admission. Component
// tests replace only that boundary, retaining ordinary JWT/SQL account checks.
pub(crate) trait Owner: Clone + Send + Sync + 'static {
    fn read(&self) -> BoxFuture<'_, Result<RetainedSqliteRestReads, StoreError>>;
}
impl Owner for CopiedSqliteRuntime {
    fn read(&self) -> BoxFuture<'_, Result<RetainedSqliteRestReads, StoreError>> {
        Box::pin(self.allocate_read())
    }
}

async fn copied_profile_headers(
    State(allow): State<&'static str>,
    request: Request,
    next: Next,
) -> Response {
    let read = matches!(*request.method(), Method::GET | Method::HEAD);
    let response = next.run(request).await;
    if read {
        super::copied_read_headers(response, HeaderValue::from_static(allow))
    } else {
        response
    }
}

async fn omitted() -> StoreError {
    StoreError::NotFound
}

pub(crate) fn router() -> Router<CopiedSqliteRuntime> {
    router_for::<CopiedSqliteRuntime>()
}
pub(crate) fn router_for<O: Owner>() -> Router<O> {
    Router::new()
        .route(
            "/api/channels/profiles.json",
            get(collection::<O>)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    copied_profile_headers,
                ))
                .fallback(omitted),
        )
        .route(
            "/api/channels/profiles.json/",
            get(collection::<O>)
                .layer(from_fn_with_state(
                    catalogue_options::COLLECTION_ALLOW,
                    copied_profile_headers,
                ))
                .fallback(omitted),
        )
        .route(
            "/api/channels/profiles/{id}",
            get(detail::<O>)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    copied_profile_headers,
                ))
                .fallback(omitted),
        )
        .route(
            "/api/channels/profiles/{id}/",
            get(detail::<O>)
                .layer(from_fn_with_state(
                    catalogue_options::DETAIL_ALLOW,
                    copied_profile_headers,
                ))
                .fallback(omitted),
        )
}

pub(crate) async fn collection<O: Owner>(
    State(owner): State<O>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, StoreError> {
    let indent = channel_profiles::negotiate_read_json(&uri, &headers, true)?;
    let value = owner
        .read()
        .await?
        .channel_profile_collection(peer, headers)
        .await?;
    channel_profiles::json_response(value, indent)
}

pub(crate) async fn detail<O: Owner>(
    State(owner): State<O>,
    Path(raw): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, StoreError> {
    let (raw, suffix) = channel_profiles::detail_read_id(raw, uri.path())?;
    let indent = channel_profiles::negotiate_read_json(&uri, &headers, suffix)?;
    let value = owner
        .read()
        .await?
        .channel_profile_detail(peer, headers, raw)
        .await?;
    channel_profiles::json_response(value, indent)
}
