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
        .route(
            "/proxy/ts/stream/{channel_id}",
            get(stream).head(stream_head),
        )
        .with_state(Playback {
            engine,
            resolver,
            hls,
            fmp4,
        })
}
fn error(reason: PlaybackError) -> Response {
    if let Some(response) = dispatcharr_stream_http::provider_capacity_error_response(&reason) {
        return response;
    }
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
async fn stream_head(
    state: State<Playback>,
    path: Path<String>,
    query: Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let mut response = stream(Method::HEAD, state, path, query, headers, peer).await;
    response
        .headers_mut()
        .insert("allow", "GET, OPTIONS".parse().unwrap());
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
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            Json(serde_json::json!({"detail":"Method \"HEAD\" not allowed."})),
        )
            .into_response();
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

    struct HeadResolver;

    impl PlaybackResolver for HeadResolver {
        fn resolve(
            &self,
            _request: PlaybackRequest,
        ) -> dispatcharr_contracts::BoxFuture<
            '_,
            Result<dispatcharr_contracts::PlaybackSource, PlaybackError>,
        > {
            panic!("HEAD must not start playback")
        }

        fn authenticate_optional(
            &self,
            request: PlaybackRequest,
        ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), PlaybackError>> {
            Box::pin(async move {
                if request.header("authorization") == Some("Bearer fixture-inactive") {
                    return Err(PlaybackError {
                        status: 401,
                        body: serde_json::json!({"detail": "Fixture credential was rejected."}),
                    });
                }
                Ok(())
            })
        }

        fn authorize_admin(
            &self,
            request: PlaybackRequest,
        ) -> dispatcharr_contracts::BoxFuture<'_, Result<(), PlaybackError>> {
            Box::pin(async move {
                if request.header("authorization") == Some("Bearer fixture-admin") {
                    return Ok(());
                }
                Err(PlaybackError {
                    status: 401,
                    body: serde_json::json!({"detail": "Authentication credentials were not provided."}),
                })
            })
        }
    }

    #[tokio::test]
    async fn original_ts_head_routes_keep_allow_without_starting_playback() {
        use axum::{body::Body, http::header};
        use tower::ServiceExt;

        let engine = StreamEngine::new(dispatcharr_stream_http::EngineConfig::default()).unwrap();
        let resolver = Arc::new(HeadResolver);
        let app = engine
            .controls_router(resolver.clone())
            .merge(router(engine, resolver, None, None));
        let playback = "/proxy/ts/stream/00000000-0000-4000-8000-000000000001?output_format=ts";
        let status = "/proxy/ts/status";
        let outcomes = [
            (playback, None, 405, "41"),
            (playback, Some("fixture-admin"), 405, "41"),
            (playback, Some("fixture-inactive"), 401, "45"),
            (status, None, 401, "58"),
            (status, Some("fixture-admin"), 405, "41"),
        ];
        for (path, actor, code, content_length) in outcomes {
            let mut request = axum::http::Request::builder()
                .method(Method::HEAD)
                .uri(path);
            if let Some(actor) = actor {
                request = request.header(header::AUTHORIZATION, format!("Bearer {actor}"));
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), code);
            assert_eq!(response.headers()[header::ALLOW], "GET, OPTIONS");
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(response.headers()[header::CONTENT_LENGTH], content_length);
            if code == 401 {
                assert_eq!(
                    response.headers()[header::WWW_AUTHENTICATE],
                    "Bearer realm=\"api\""
                );
            }
            assert!(axum::body::to_bytes(response.into_body(), 0)
                .await
                .unwrap()
                .is_empty());
        }
        let admin = "fixture-admin";
        let request = axum::http::Request::builder()
            .uri(status)
            .header(header::AUTHORIZATION, format!("Bearer {admin}"))
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(!response.headers().contains_key(header::ALLOW));
    }

    #[tokio::test]
    async fn ordinary_capacity_error_keeps_original_wire_format() {
        let response = error(PlaybackError {
            status: 503,
            body: serde_json::json!({
                "error": "All active M3U profiles have reached maximum connection limits",
                "waited": "2s",
            }),
        });
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()["content-type"], "application/json");
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            body.as_ref(),
            br#"{"error": "All active M3U profiles have reached maximum connection limits", "waited": "2s"}"#
        );
    }

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
