use crate::StreamEngine;
use axum::body::Bytes;
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::{HeaderMap, StatusCode},
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
            get(stream).with_state(App {
                engine: self.clone(),
                resolver,
            }),
        )
    }

    pub fn controls_router(&self, resolver: Arc<dyn PlaybackResolver>) -> Router {
        Router::new()
            .route("/proxy/ts/status", get(status))
            .route("/proxy/ts/stop_client/{channel_id}", post(stop_client))
            .route("/proxy/ts/status/{channel_id}", get(channel_status))
            .route("/proxy/ts/stop/{channel_id}", post(stop).delete(stop))
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
    })
}

fn error(error: PlaybackError) -> Response {
    (
        StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(error.body),
    )
        .into_response()
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
    let source = match app.resolver.resolve(request).await {
        Ok(source) => source,
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
    if app.engine.stop(&id) {
        Json(serde_json::json!({"message":"Channel stop request sent", "channel_id": id, "previous_state":"active"})).into_response()
    } else {
        error(PlaybackError::new(404, &format!("Channel {id} not found")))
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
    if !app
        .engine
        .status()
        .iter()
        .any(|channel| channel.channel_id == id)
    {
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
