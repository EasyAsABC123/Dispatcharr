//! HLS capability routes follow the original views/session lifecycle.
use crate::gateway::{
    failure, response_bytes, session_segment, wall_time, HlsGateway, Inner, Session,
};
use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get},
    Extension, Json, Router,
};
use bytes::Bytes;
use dispatcharr_contracts::{PlaybackError, PlaybackRequest};
use std::{
    convert::Infallible,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

impl HlsGateway {
    pub fn router(&self) -> Router {
        Router::new()
            .route("/proxy/hls/{token}/index.m3u8", get(playlist))
            .route("/proxy/hls/{token}/{segment}", get(segment))
            .route("/api/proxy/hls/sessions/{token}/", delete(stop_session))
            .with_state(self.clone())
    }
}
fn error(error: PlaybackError) -> Response {
    (
        StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(error.body),
    )
        .into_response()
}
fn body(body: Body, content_type: &'static str) -> Response {
    let mut response = Response::new(body);
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-cache".parse().unwrap());
    response
}
fn request_permit(session: &Session) -> Result<Arc<OwnedSemaphorePermit>, PlaybackError> {
    session
        .requests
        .clone()
        .try_acquire_owned()
        .map(Arc::new)
        .map_err(|_| failure(429, "Too many HLS requests"))
}
async fn playlist(State(gateway): State<HlsGateway>, Path(token): Path<String>) -> Response {
    match playlist_result(gateway, token).await {
        Ok(response) => response,
        Err(reason) => error(reason),
    }
}
async fn playlist_result(gateway: HlsGateway, token: String) -> Result<Response, PlaybackError> {
    let session = gateway.inner.lookup(&token).await?;
    let permit = request_permit(&session)?;
    if let Some(payload) = session.playlist()? {
        return Ok(body(
            Body::from(response_bytes(payload, session, permit)),
            "application/vnd.apple.mpegurl",
        ));
    }
    if gateway
        .inner
        .clients
        .enter_hls_cold_start(&token)
        .await
        .map_err(|_| failure(503, "Session unavailable"))?
        .is_none()
    {
        return Err(failure(410, "Session expired"));
    }
    let guard = ColdGuard {
        inner: gateway.inner.clone(),
        session: session.clone(),
        completed: AtomicBool::new(false),
        _request: permit.clone(),
    };
    let slots = Arc::new(Semaphore::new(1));
    let output = async_stream::stream! {
        let guard=guard;let start=Instant::now();let mut touch=Instant::now();let mut count=0u64;
        let slot=slots.clone().acquire_owned().await.unwrap();
        yield Ok::<Bytes,Infallible>(Bytes::from_owner(Comment {data:b"#EXTM3U\n".to_vec(),_slot:slot,_session:session.clone(),_request:permit.clone()}));
        loop {
            let notified=session.manager.notify.notified();tokio::pin!(notified);notified.as_mut().enable();
            if session.expired.load(Ordering::Acquire) {guard.completed.store(true,Ordering::Release);break;}
            match session.playlist() {
                Ok(Some(payload))=>{
                    // The initial marker has already been sent; retain the original payload owner.
                    let bytes=response_bytes(payload,session.clone(),permit.clone());
                    guard.completed.store(true,Ordering::Release);
                    yield Ok(bytes.slice(b"#EXTM3U\n".len()..));break;
                },
                Err(_)=>{guard.completed.store(true,Ordering::Release);break;},
                Ok(None)=>{},
            }
            let ready=session.manager.state.lock().unwrap_or_else(|e|e.into_inner()).ready;
            if ready.is_some_and(|time|time.elapsed()>=gateway.inner.config.ready_timeout)||start.elapsed()>=gateway.inner.config.initialization_timeout {
                guard.completed.store(true,Ordering::Release);break;
            }
            if touch.elapsed()>=Duration::from_secs(3) {
                if gateway.inner.lookup(&token).await.is_err() {guard.completed.store(true,Ordering::Release);break;}
                touch=Instant::now();
            }
            tokio::select! {_=notified=>continue,_=tokio::time::sleep(Duration::from_millis(250))=>{}}
            count+=1;let slot=slots.clone().acquire_owned().await.unwrap();
            yield Ok(Bytes::from_owner(Comment {data:format!("# dispatcharr: waiting {count}\n").into_bytes(),_slot:slot,_session:session.clone(),_request:permit.clone()}));
        }
    };
    Ok(body(
        Body::from_stream(output),
        "application/vnd.apple.mpegurl",
    ))
}
struct Comment {
    data: Vec<u8>,
    _slot: OwnedSemaphorePermit,
    _session: Arc<Session>,
    _request: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for Comment {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
struct ColdGuard {
    inner: Arc<Inner>,
    session: Arc<Session>,
    completed: AtomicBool,
    _request: Arc<OwnedSemaphorePermit>,
}
impl Drop for ColdGuard {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let session = self.session.clone();
        let completed = self.completed.load(Ordering::Acquire);
        let disconnected = wall_time();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let count = inner.clients.exit_hls_cold_start(&session.token).await;
                if completed || !matches!(count, Ok(Some(0))) {
                    return;
                }
                tokio::time::sleep(inner.config.reconnect_grace).await;
                if session.playlist().ok().flatten().is_some() {
                    return;
                }
                let key = format!(
                    "live:channel:{}:output:hls:playlist",
                    session.manager.channel
                );
                if let Ok(Some((channel, client))) = inner
                    .clients
                    .claim_abandoned_hls(&session.token, &key, disconnected)
                    .await
                {
                    let _ = inner.clients.stop_client(&channel, &client).await;
                    inner.expire(&session).await;
                }
            });
        }
    }
}
async fn segment(
    State(gateway): State<HlsGateway>,
    Path((token, name)): Path<(String, String)>,
) -> Response {
    let result = async {
        let session = gateway.inner.lookup(&token).await?;
        let permit = request_permit(&session)?;
        let seq = name
            .strip_suffix(".ts")
            .and_then(|text| text.parse::<u64>().ok())
            .ok_or_else(|| failure(404, "Segment expired"))?;
        let bytes = session_segment(&session, seq, permit, gateway.inner.config.segment_ttl)?;
        Ok::<_, PlaybackError>(body(Body::from(bytes), "video/mp2t"))
    }
    .await;
    match result {
        Ok(response) => response,
        Err(reason) => error(reason),
    }
}
async fn stop_session(
    State(gateway): State<HlsGateway>,
    Path(token): Path<String>,
    headers: HeaderMap,
    peer: Option<Extension<ConnectInfo<SocketAddr>>>,
) -> Response {
    let result = async {
        for name in ["authorization", "x-api-key"] {
            if headers.get_all(name).iter().count() > 1
                || headers
                    .get_all(name)
                    .iter()
                    .any(|value| value.to_str().is_err())
            {
                return Err(failure(401, "Invalid authentication header"));
            }
        }
        let info = gateway
            .inner
            .clients
            .get_hls_session(&token)
            .await
            .map_err(|_| failure(503, "Session unavailable"))?
            .ok_or_else(|| failure(404, "Session not found"))?;
        let request = PlaybackRequest {
            channel_id: info.channel_id.clone(),
            query: Vec::new(),
            headers: headers
                .iter()
                .filter_map(|(name, value)| {
                    Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned()))
                })
                .collect(),
            peer_ip: peer.map_or(
                IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                |Extension(ConnectInfo(address))| address.ip(),
            ),
        };
        gateway
            .inner
            .resolver
            .authorize_hls_owner(request, info.user_id)
            .await?;
        gateway
            .inner
            .clients
            .stop_client(&info.channel_id, &info.client_id)
            .await
            .map_err(|_| failure(503, "Session unavailable"))?;
        let local = gateway
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&token)
            .cloned();
        if let Some(session) = local {
            gateway.inner.expire(&session).await;
        }
        Ok::<_, PlaybackError>(StatusCode::NO_CONTENT.into_response())
    }
    .await;
    match result {
        Ok(response) => response,
        Err(reason) => error(reason),
    }
}
