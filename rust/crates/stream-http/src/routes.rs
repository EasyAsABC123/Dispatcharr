use crate::StreamEngine;
use axum::body::Bytes;
use axum::{
    extract::{ConnectInfo, Path, Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use dispatcharr_contracts::{PlaybackError, PlaybackRequest, PlaybackResolver};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

#[derive(Clone)]
struct App {
    engine: StreamEngine,
    resolver: Arc<dyn PlaybackResolver>,
}

impl StreamEngine {
    pub fn router(&self, resolver: Arc<dyn PlaybackResolver>) -> Router {
        self.controls_router(resolver.clone()).route(
            "/proxy/ts/stream/{channel_id}",
            get(stream).head(stream_head).with_state(App {
                engine: self.clone(),
                resolver,
            }),
        )
    }

    pub fn controls_router(&self, resolver: Arc<dyn PlaybackResolver>) -> Router {
        Router::new()
            .route("/proxy/ts/status", get(status).head(status_head))
            .route(
                "/proxy/ts/change_stream/{channel_id}",
                post(change_stream).get(change_stream),
            )
            .route("/internal/proxy/ts/v1/memory", get(memory).head(admin_head))
            .route(
                "/proxy/ts/stop_client/{channel_id}",
                post(stop_client).head(admin_channel_head),
            )
            .route(
                "/proxy/ts/status/{channel_id}",
                get(channel_status).head(admin_channel_head),
            )
            .route(
                "/proxy/ts/stop/{channel_id}",
                post(stop).delete(stop).head(admin_channel_head),
            )
            .with_state(App {
                engine: self.clone(),
                resolver,
            })
    }
}

fn request(
    channel_id: String,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Result<PlaybackRequest, PlaybackError> {
    for name in ["authorization", "x-api-key"] {
        let values = headers.get_all(name);
        if values.iter().count() > 1 || values.iter().any(|value| value.to_str().is_err()) {
            return Err(PlaybackError::new(401, "Invalid authentication header"));
        }
    }
    Ok(PlaybackRequest {
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
    })
}

/// Render the ordinary live provider-capacity response at every TS entry point.
pub fn provider_capacity_error_response(error: &PlaybackError) -> Option<Response> {
    let waited = error.body.get("waited").filter(|value| {
        error.status == 503
            && error.body["error"].as_str() == Some(crate::retry::PROVIDER_CAPACITY_ERROR)
            && value.is_string()
    })?;
    // Django's ordinary live-capacity JsonResponse includes its default
    // separator spaces. `waited` is still encoded as a JSON string.
    let reason = crate::retry::PROVIDER_CAPACITY_ERROR;
    Some(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CONTENT_TYPE, "application/json")],
            format!(r#"{{"error": "{reason}", "waited": {waited}}}"#),
        )
            .into_response(),
    )
}

fn error(error: PlaybackError) -> Response {
    if let Some(response) = provider_capacity_error_response(&error) {
        return response;
    }
    let unauthorized = error.status == 401;
    let mut response = (
        StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(error.body),
    )
        .into_response();
    if unauthorized {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            "Bearer realm=\"api\"".parse().unwrap(),
        );
    }
    response
}

async fn stream_head(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let result = async {
        app.resolver
            .authenticate_optional(request(id, query, headers, peer)?)
            .await?;
        Err::<(), _>(PlaybackError {
            status: 405,
            body: serde_json::json!({"detail":"Method \"HEAD\" not allowed."}),
        })
    }
    .await;
    error(result.unwrap_err())
}
async fn status_head(
    state: State<App>,
    query: Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let mut response = admin_head(state, query, headers, peer).await;
    response
        .headers_mut()
        .insert(header::ALLOW, "GET, OPTIONS".parse().unwrap());
    response
}

async fn admin_head(
    State(app): State<App>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    admin_head_result(app, String::new(), query, headers, peer).await
}
async fn admin_channel_head(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    admin_head_result(app, id, query, headers, peer).await
}
async fn admin_head_result(
    app: App,
    id: String,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let result = async {
        app.resolver
            .authorize_admin(request(id, query, headers, peer)?)
            .await?;
        Err::<(), _>(PlaybackError {
            status: 405,
            body: serde_json::json!({"detail":"Method \"HEAD\" not allowed."}),
        })
    }
    .await;
    error(result.unwrap_err())
}
async fn stream(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let request = match request(id, query, headers, peer) {
        Ok(request) => request,
        Err(failure) => return error(failure),
    };
    let source = match app.resolver.resolve_response(request).await {
        Ok(dispatcharr_contracts::PlaybackResolution::Stream(source)) => *source,
        Ok(dispatcharr_contracts::PlaybackResolution::Redirect(plan)) => {
            return match app.engine.redirect(*plan).await {
                Ok(response) => response,
                Err(reason) => error(reason),
            }
        }
        Err(failure) => return error(failure),
    };
    if source.output_format != dispatcharr_contracts::OutputFormat::MpegTs {
        return error(PlaybackError::new(501, "Output format is not implemented"));
    }
    match app.engine.play(source).await {
        Ok(response) => response,
        Err(failure) => error(failure),
    }
}

async fn memory(
    State(app): State<App>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let request = match request(String::new(), query, headers, peer) {
        Ok(request) => request,
        Err(failure) => return error(failure),
    };
    if let Err(failure) = app.resolver.authorize_admin(request).await {
        return error(failure);
    }
    match app.engine.memory_diagnostics().await {
        Ok(value) => Json(value).into_response(),
        Err(failure) => error(failure),
    }
}
async fn status(
    State(app): State<App>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let request = match request(String::new(), query, headers, peer) {
        Ok(request) => request,
        Err(failure) => return error(failure),
    };
    if let Err(failure) = app.resolver.authorize_admin(request).await {
        return error(failure);
    }
    if let Some(reader) = app.engine.live_statistics_reader() {
        return match reader.snapshot().await {
            Ok(bytes) => ([(header::CONTENT_TYPE, "application/json")], bytes).into_response(),
            Err(reason) => error(reason),
        };
    }
    let mut channels = Vec::new();
    for local in app.engine.status() {
        match app.engine.legacy_status(&local.channel_id, false).await {
            Ok(Some(info)) => channels.push(info),
            Ok(None) => {}
            Err(reason) => return error(reason),
        }
    }
    Json(serde_json::json!({"count":channels.len(),"channels":channels})).into_response()
}

async fn channel_status(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let request = match request(id.clone(), query, headers, peer) {
        Ok(request) => request,
        Err(failure) => return error(failure),
    };
    if let Err(failure) = app.resolver.authorize_admin(request).await {
        return error(failure);
    }
    match app.engine.legacy_status(&id, true).await {
        Ok(Some(channel)) => Json(channel).into_response(),
        Ok(None) => error(PlaybackError::new(404, &format!("Channel {id} not found"))),
        Err(reason) => error(reason),
    }
}

async fn change_stream(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    incoming: Request,
) -> Response {
    let request = match request(id.clone(), query, incoming.headers().clone(), peer) {
        Ok(request) => request,
        Err(failure) => return error(failure),
    };
    let mut response = if let Err(reason) = app.resolver.authorize_admin(request).await {
        error(reason)
    } else if incoming.method() != axum::http::Method::POST {
        error(PlaybackError {
            status: 405,
            body: serde_json::json!({
                "detail": format!("Method \"{}\" not allowed.", incoming.method()),
            }),
        })
    } else {
        match change_assigned_stream(&app.engine, &id, incoming).await {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(reason) => error(reason),
        }
    };
    response.headers_mut().insert(
        header::ALLOW,
        axum::http::HeaderValue::from_static("OPTIONS, POST"),
    );
    response
        .headers_mut()
        .append(header::VARY, axum::http::HeaderValue::from_static("Accept"));
    response
}

async fn change_assigned_stream(
    engine: &StreamEngine,
    channel: &str,
    request: Request,
) -> Result<String, PlaybackError> {
    let body = tokio::time::timeout(
        engine.local_source_switch_body_deadline(),
        axum::body::to_bytes(request.into_body(), 16 * 1024),
    )
    .await
    .map_err(|_| PlaybackError::new(408, "Stream change request body timed out"))?
    .map_err(|_| PlaybackError::new(400, "Invalid JSON"))?;
    let data: serde_json::Map<String, serde_json::Value> =
        serde_json::from_slice(&body).map_err(|_| PlaybackError::new(400, "Invalid JSON"))?;
    // Original resolves a supplied stream ID and replaces both caller fields.
    // The Engine receives only that ID, never a caller URL or user agent.
    let Some(stream_id) = data
        .get("stream_id")
        .and_then(serde_json::Value::as_i64)
        .filter(|id| *id > 0)
    else {
        return Err(
            if data.contains_key("url") || data.contains_key("stream_id") {
                PlaybackError::new(501, "Only an assigned stream ID is supported")
            } else {
                PlaybackError::new(400, "Either url or stream_id must be provided")
            },
        );
    };
    let worker = engine
        .local_source_switch_worker()
        .ok_or_else(|| PlaybackError::new(501, "Local source switch requires worker identity"))?;
    let url = engine.switch_local_source(channel, stream_id).await?;
    Ok(changed_stream_response(channel, &url, worker, stream_id))
}

fn changed_stream_response(channel: &str, url: &str, worker: &str, stream_id: i64) -> String {
    let channel = serde_json::Value::from(channel);
    let url = serde_json::Value::from(url);
    let worker = serde_json::Value::from(worker);
    format!(
        r#"{{"message": "Stream changed successfully", "channel": {channel}, "url": {url}, "owner": true, "worker_id": {worker}, "stream_id": {stream_id}}}"#
    )
}

async fn stop(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let request = match request(id.clone(), query, headers, peer) {
        Ok(request) => request,
        Err(failure) => return error(failure),
    };
    if let Err(failure) = app.resolver.authorize_admin(request).await {
        return error(failure);
    }
    let previous = app
        .engine
        .legacy_status(&id, false)
        .await
        .ok()
        .flatten()
        .and_then(|info| {
            info.get("state")
                .and_then(serde_json::Value::as_str)
                .map(|state| serde_json::json!({"state":state}))
        })
        .unwrap_or(serde_json::Value::Null);
    let stopped = match app.engine.stop_global(&id).await {
        Ok(stopped) => stopped,
        Err(reason) => return error(reason),
    };
    if stopped {
        Json(serde_json::json!({"message":"Channel stop request sent", "channel_id": id, "previous_state":previous})).into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"Channel not found"})),
        )
            .into_response()
    }
}

async fn stop_client(
    State(app): State<App>,
    Path(id): Path<String>,
    Query(query): Query<Vec<(String, String)>>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
    body: Bytes,
) -> Response {
    let request = match request(id.clone(), query, headers, peer) {
        Ok(request) => request,
        Err(reason) => return error(reason),
    };
    if let Err(reason) = app.resolver.authorize_admin(request).await {
        return error(reason);
    }
    let data: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(data) => data,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error":"Invalid JSON"})),
            )
                .into_response()
        }
    };
    let Some(client_id) = data
        .get("client_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error":"No client_id provided"})),
        )
            .into_response();
    };
    let exists = match app.engine.has_channel(&id).await {
        Ok(exists) => exists,
        Err(reason) => return error(reason),
    };
    if !exists {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error":"Channel not found"})),
        )
            .into_response();
    }
    match app.engine.stop_client(&id,client_id).await{
        Ok(local)=>Json(serde_json::json!({"message":"Client stop request processed","channel_id":id,"client_id":client_id,"locally_processed":local})).into_response(),
        Err(reason)=>error(reason),
    }
}
