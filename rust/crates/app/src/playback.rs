//! Dispatch the original live URL to the selected native output transport.
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use dispatcharr_contracts::{OutputFormat, PlaybackError, PlaybackRequest, PlaybackResolver};
use dispatcharr_fmp4_http::Fmp4Gateway;
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
    fmp4: Option<Fmp4Gateway>,
}
pub fn router(
    engine: StreamEngine,
    resolver: Arc<dyn PlaybackResolver>,
    hls: Option<HlsGateway>,
    fmp4: Option<Fmp4Gateway>,
) -> Router {
    Router::new()
        .route("/proxy/ts/stream/{channel_id}", get(stream).head(stream))
        .with_state(Playback {
            engine,
            resolver,
            hls,
            fmp4,
        })
}
fn error(reason: PlaybackError) -> Response {
    let unauthorized = reason.status == 401;
    let mut response = (
        StatusCode::from_u16(reason.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(reason.body),
    )
        .into_response();
    if unauthorized {
        response
            .headers_mut()
            .insert("www-authenticate", "Bearer realm=\"api\"".parse().unwrap());
    }
    response
}
async fn stream(
    method: Method,
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
    if method == Method::HEAD {
        if let Err(reason) = app.resolver.authenticate_optional(request).await {
            return error(reason);
        }
        let mut response = (
            StatusCode::METHOD_NOT_ALLOWED,
            Json(serde_json::json!({"detail":"Method \"HEAD\" not allowed."})),
        )
            .into_response();
        response
            .headers_mut()
            .insert("allow", "GET, OPTIONS".parse().unwrap());
        return response;
    }
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
        OutputFormat::Fmp4 => match app.fmp4 {
            Some(gateway) => gateway.play(source).await,
            None => Err(PlaybackError::new(503, "Redis fMP4 output unavailable")),
        },
    };
    match response {
        Ok(response) => response,
        Err(reason) => error(reason),
    }
}
