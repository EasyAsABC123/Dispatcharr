//! Dispatch the original live URL to the selected native output transport.
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use dispatcharr_contracts::{
    OutputFormat, PlaybackError, PlaybackRequest, PlaybackResolution, PlaybackResolver,
};
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
        headers: dispatcharr_contracts::source_headers(
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes())),
        ),
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
    let resolution = match app.resolver.resolve_response(request).await {
        Ok(resolution) => resolution,
        Err(reason) => return error(reason),
    };
    let response = dispatch_resolution(app.engine, app.hls, app.fmp4, resolution).await;
    match response {
        Ok(response) => response,
        Err(reason) => error(reason),
    }
}
pub fn dispatcher(
    engine: StreamEngine,
    hls: Option<HlsGateway>,
    fmp4: Option<Fmp4Gateway>,
) -> dispatcharr_xc_http::LiveDispatcher {
    Arc::new(move |resolution| {
        let engine = engine.clone();
        let hls = hls.clone();
        let fmp4 = fmp4.clone();
        Box::pin(async move { dispatch_resolution(engine, hls, fmp4, resolution).await })
    })
}
async fn dispatch_resolution(
    engine: StreamEngine,
    hls: Option<HlsGateway>,
    fmp4: Option<Fmp4Gateway>,
    resolution: PlaybackResolution,
) -> Result<Response, PlaybackError> {
    let source = match resolution {
        PlaybackResolution::Stream(source) => *source,
        PlaybackResolution::Redirect(plan) => return engine.redirect(*plan).await,
    };
    match source.output_format {
        OutputFormat::MpegTs => engine.play(source).await,
        OutputFormat::Hls => match hls {
            Some(gateway) => gateway.mint(source).await,
            None => Err(PlaybackError::new(503, "Redis HLS sessions unavailable")),
        },
        OutputFormat::Fmp4 => match fmp4 {
            Some(gateway) => gateway.play(source).await,
            None => Err(PlaybackError::new(503, "Redis fMP4 output unavailable")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_contracts::{PlaybackRedirectPlan, PlaybackViewer, UserLimitPolicy};

    #[tokio::test]
    async fn xc_dispatcher_uses_shared_redirect_engine() {
        let engine = StreamEngine::new(dispatcharr_stream_http::EngineConfig::default()).unwrap();
        let dispatch = dispatcher(engine, None, None);
        let resolution = PlaybackResolution::Redirect(Box::new(PlaybackRedirectPlan {
            channel_id: "owned-test".to_owned(),
            candidates: Vec::new(),
            viewer: PlaybackViewer {
                user_id: Some(7),
                username: Some("owned-test".to_owned()),
                ip_address: "127.0.0.1".parse().unwrap(),
                user_agent: String::new(),
                stream_limit: 0,
                limit_policy: UserLimitPolicy::default(),
            },
            capacity_only: true,
            assignment: None,
            validate_urls: false,
            selection_error: None,
        }));
        let result = dispatch(resolution).await.unwrap_err();
        assert_eq!(result.status, 501);
        assert_eq!(
            result.body["error"],
            "Redirect client-policy transport unavailable"
        );
    }
}
