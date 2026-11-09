//! Dispatch the original live URL to the selected native output transport.
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use dispatcharr_contracts::{OutputFormat, PlaybackError, PlaybackRequest, PlaybackResolver};
use dispatcharr_hls_http::HlsGateway;
use dispatcharr_stream_http::StreamEngine;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};
#[derive(Clone)]
struct Playback {
    engine: StreamEngine,
    resolver: Arc<dyn PlaybackResolver>,
    hls: Option<HlsGateway>,
}
pub fn router(
    engine: StreamEngine,
    resolver: Arc<dyn PlaybackResolver>,
    hls: Option<HlsGateway>,
) -> Router {
    Router::new()
        .route("/proxy/ts/stream/{channel_id}", get(stream))
        .with_state(Playback {
            engine,
            resolver,
            hls,
        })
}
fn error(reason: PlaybackError) -> Response {
    (
        StatusCode::from_u16(reason.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(reason.body),
    )
        .into_response()
}
async fn stream(
    State(app): State<Playback>,
    Path(channel_id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    for name in ["authorization", "x-api-key"] {
        let values = headers.get_all(name);
        if values.iter().count() > 1 || values.iter().any(|value| value.to_str().is_err()) {
            return error(PlaybackError::new(401, "Invalid authentication header"));
        }
    }
    let request = PlaybackRequest {
        channel_id,
        query,
        headers: headers
            .into_iter()
            .filter_map(|(name, value)| {
                Some((name?.as_str().to_owned(), value.to_str().ok()?.to_owned()))
            })
            .collect(),
        peer_ip: peer.map_or(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            |Extension(ConnectInfo(address))| address.ip(),
        ),
    };
    let source = match app.resolver.resolve(request).await {
        Ok(source) => source,
        Err(reason) => return error(reason),
    };
    let response = match source.output_format {
        OutputFormat::MpegTs => app.engine.play(source).await,
        OutputFormat::Hls => match app.hls {
            Some(gateway) => gateway.mint(source).await,
            None => Err(PlaybackError::new(503, "Redis HLS sessions unavailable")),
        },
        OutputFormat::Fmp4 => Err(PlaybackError::new(501, "Output format is not implemented")),
    };
    match response {
        Ok(response) => response,
        Err(reason) => error(reason),
    }
}
