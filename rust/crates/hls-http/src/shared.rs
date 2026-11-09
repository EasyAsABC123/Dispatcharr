//! Shared Redis capability reads, retaining original upstream HLS attribution.
use crate::gateway::{failure, lock, wall_time, Inner};
use axum::{body::Body, response::Response};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackError;
use dispatcharr_hls_core::{render_media_playlist, SegmentDescriptor};
use dispatcharr_output_store::SegmentBytes;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct RemoteSession {
    requests: Arc<Semaphore>,
    _admission: OwnedSemaphorePermit,
}
struct RemoteFrame {
    data: Vec<u8>,
    _budget: OwnedSemaphorePermit,
    _session: Arc<RemoteSession>,
    _request: Arc<OwnedSemaphorePermit>,
    _frame: Option<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for RemoteFrame {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
struct RemoteMedia {
    data: SegmentBytes,
    _session: Arc<RemoteSession>,
    _request: Arc<OwnedSemaphorePermit>,
}
impl AsRef<[u8]> for RemoteMedia {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
pub(crate) fn descriptor(
    value: &serde_json::Value,
    limit: usize,
) -> Result<(Vec<SegmentDescriptor>, u64, f64, f64), PlaybackError> {
    let unavailable = || failure(500, "Playlist unavailable");
    let items = value
        .get("window")
        .and_then(|value| value.as_array())
        .ok_or_else(unavailable)?;
    if items.len() > limit {
        return Err(unavailable());
    }
    let mut window = Vec::with_capacity(items.len());
    let mut previous = 0;
    for item in items {
        let seq = item
            .get("seq")
            .and_then(|value| value.as_u64())
            .ok_or_else(unavailable)?;
        let duration = item
            .get("dur")
            .and_then(|value| value.as_f64())
            .filter(|value| value.is_finite() && *value > 0.0)
            .ok_or_else(unavailable)?;
        if seq <= previous {
            return Err(unavailable());
        }
        previous = seq;
        window.push(SegmentDescriptor {
            seq,
            duration,
            discontinuity: item
                .get("disc")
                .and_then(|value| value.as_bool())
                .unwrap_or(false),
        });
    }
    let disc_seq = value
        .get("disc_seq")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let behind = value
        .get("start_behind")
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(0.0);
    let ts = value
        .get("ts")
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite())
        .ok_or_else(unavailable)?;
    Ok((window, disc_seq, behind, ts))
}
fn render(value: &serde_json::Value, limit: usize) -> Result<Vec<u8>, PlaybackError> {
    let (window, disc, behind, ts) = descriptor(value, limit)?;
    let advertised = value
        .get("adv_target")
        .and_then(|value| value.as_u64())
        .unwrap_or(6);
    if wall_time() - ts > advertised.saturating_mul(3).max(45) as f64 {
        return Err(failure(404, "Playlist stale"));
    }
    Ok(render_media_playlist(
        &window,
        4.0,
        "{seq}.ts",
        Some(advertised),
        disc,
        Some(behind),
    )
    .into_bytes())
}
fn admit(
    inner: &Inner,
    token: &str,
) -> Result<(Arc<RemoteSession>, Arc<OwnedSemaphorePermit>), PlaybackError> {
    if inner.stopped.load(Ordering::Acquire) {
        return Err(failure(503, "HLS is shutting down"));
    }
    if token.len() != 32
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(failure(410, "Session expired"));
    }
    let mut remote = lock(&inner.remote);
    remote.retain(|_, entry| entry.strong_count() > 0);
    let session = if let Some(session) = remote.get(token).and_then(std::sync::Weak::upgrade) {
        session
    } else {
        let admission = inner
            .session_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(503, "HLS session capacity exhausted"))?;
        let session = Arc::new(RemoteSession {
            requests: Arc::new(Semaphore::new(inner.config.max_requests_per_session)),
            _admission: admission,
        });
        remote.insert(token.to_owned(), Arc::downgrade(&session));
        session
    };
    let request = Arc::new(
        session
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(429, "Too many HLS requests"))?,
    );
    Ok((session, request))
}
fn frame(
    inner: &Inner,
    data: Vec<u8>,
    session: Arc<RemoteSession>,
    request: Arc<OwnedSemaphorePermit>,
    frame: Option<OwnedSemaphorePermit>,
) -> Result<Bytes, PlaybackError> {
    let budget = inner
        .bytes
        .clone()
        .try_acquire_many_owned(data.len().max(1) as u32)
        .map_err(|_| failure(503, "HLS byte capacity exhausted"))?;
    Ok(Bytes::from_owner(RemoteFrame {
        data,
        _budget: budget,
        _session: session,
        _request: request,
        _frame: frame,
    }))
}
fn format(info: &dispatcharr_clients::HlsSessionInfo) -> Result<String, PlaybackError> {
    match info
        .metadata
        .get("output_profile_id")
        .filter(|value| !value.is_empty())
    {
        None => Ok("hls".into()),
        Some(value) => value
            .parse::<u64>()
            .ok()
            .filter(|id| *id > 0)
            .map(|id| format!("hls:p{id}"))
            .ok_or_else(|| failure(500, "Playlist unavailable")),
    }
}
async fn touch(
    inner: &Inner,
    token: &str,
) -> Result<dispatcharr_clients::HlsSessionInfo, PlaybackError> {
    match inner.clients.touch_hls_session(token).await {
        Ok(Some(info)) => Ok(info),
        Ok(None) => Err(failure(410, "Session expired")),
        Err(dispatcharr_clients::Error::Stopped) => Err(failure(410, "Stream stopped")),
        Err(_) => Err(failure(503, "Session unavailable")),
    }
}
async fn retry<T, F, Fut>(inner: &Inner, token: &str, mut operation: F) -> Result<T, PlaybackError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, dispatcharr_output_store::Error>>,
{
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut touched = Instant::now();
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(dispatcharr_output_store::Error::Capacity) if Instant::now() < deadline => {
                if touched.elapsed() >= Duration::from_secs(3) {
                    touch(inner, token).await?;
                    touched = Instant::now();
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(_) => return Err(failure(503, "Shared output unavailable")),
        }
    }
}
pub(crate) async fn playlist(inner: Arc<Inner>, token: String) -> Result<Response, PlaybackError> {
    let (session, request) = admit(&inner, &token)?;
    let info = touch(&inner, &token).await?;
    let format = format(&info)?;
    let outputs = inner
        .outputs
        .clone()
        .ok_or_else(|| failure(503, "Shared output unavailable"))?;
    let snapshot = retry(&inner, &token, || {
        outputs.snapshot(&info.channel_id, &format)
    })
    .await?;
    if let Some(descriptor) = snapshot.descriptor {
        let data = render(&descriptor, inner.config.max_stored_segments)?;
        return Ok(crate::routes::body(
            Body::from(frame(&inner, data, session, request, None)?),
            "application/vnd.apple.mpegurl",
        ));
    }
    if inner
        .clients
        .enter_hls_cold_start(&token)
        .await
        .map_err(|_| failure(503, "Session unavailable"))?
        .is_none()
    {
        return Err(failure(410, "Session expired"));
    }
    let guard = RemoteCold {
        inner: inner.clone(),
        token: token.clone(),
        channel: info.channel_id.clone(),
        format: format.clone(),
        completed: AtomicBool::new(false),
        _session: session.clone(),
        _request: request.clone(),
    };
    let slots = Arc::new(Semaphore::new(1));
    let stream = async_stream::stream! {
        let guard=guard;let start=Instant::now();let mut active=None;let mut touched=Instant::now();let mut count=0;
        match frame(&inner,b"#EXTM3U\n".to_vec(),session.clone(),request.clone(),Some(slots.clone().acquire_owned().await.unwrap())) {Ok(bytes)=>yield Ok::<_,Infallible>(bytes),Err(_)=>{guard.completed.store(true,Ordering::Release);return;}}
        loop {
            if inner.stopped.load(Ordering::Acquire) {guard.completed.store(true,Ordering::Release);break;}
            match outputs.snapshot(&info.channel_id,&format).await {
                Ok(snapshot)=>if let Some(descriptor)=snapshot.descriptor {
                    guard.completed.store(true,Ordering::Release);
                    if let Ok(data)=render(&descriptor,inner.config.max_stored_segments) {if let Ok(bytes)=frame(&inner,data,session.clone(),request.clone(),Some(slots.clone().acquire_owned().await.unwrap())) {yield Ok(bytes.slice(8..));}}
                    break;
                },
                Err(_)=>{guard.completed.store(true,Ordering::Release);break;},
            }
            if active.is_none()&&matches!(outputs.state(&info.channel_id,&format).await,Ok(Some(state)) if state=="active") {active=Some(Instant::now());}
            if start.elapsed()>=inner.config.initialization_timeout||active.is_some_and(|time:Instant|time.elapsed()>=inner.config.ready_timeout) {guard.completed.store(true,Ordering::Release);break;}
            if touched.elapsed()>=Duration::from_secs(3) {if touch(&inner,&token).await.is_err() {guard.completed.store(true,Ordering::Release);break;}touched=Instant::now();}
            tokio::time::sleep(Duration::from_millis(250)).await;count+=1;
            match frame(&inner,format!("# dispatcharr: waiting {count}\n").into_bytes(),session.clone(),request.clone(),Some(slots.clone().acquire_owned().await.unwrap())) {Ok(bytes)=>yield Ok(bytes),Err(_)=>{guard.completed.store(true,Ordering::Release);break;}}
        }
    };
    Ok(crate::routes::body(
        Body::from_stream(stream),
        "application/vnd.apple.mpegurl",
    ))
}
pub(crate) async fn segment(
    inner: Arc<Inner>,
    token: String,
    seq: u64,
) -> Result<Response, PlaybackError> {
    let (session, request) = admit(&inner, &token)?;
    let info = touch(&inner, &token).await?;
    let format = format(&info)?;
    let outputs = inner
        .outputs
        .as_ref()
        .ok_or_else(|| failure(503, "Shared output unavailable"))?;
    let data = retry(&inner, &token, || {
        outputs.get(&info.channel_id, &format, seq)
    })
    .await?
    .ok_or_else(|| failure(404, "Segment expired"))?;
    Ok(crate::routes::body(
        Body::from(Bytes::from_owner(RemoteMedia {
            data,
            _session: session,
            _request: request,
        })),
        "video/mp2t",
    ))
}
struct RemoteCold {
    inner: Arc<Inner>,
    token: String,
    channel: String,
    format: String,
    completed: AtomicBool,
    _session: Arc<RemoteSession>,
    _request: Arc<OwnedSemaphorePermit>,
}
impl Drop for RemoteCold {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let token = self.token.clone();
        let channel = self.channel.clone();
        let format = self.format.clone();
        let completed = self.completed.load(Ordering::Acquire);
        let since = wall_time();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if !matches!(inner.clients.exit_hls_cold_start(&token).await, Ok(Some(0)))
                    || completed
                {
                    return;
                }
                tokio::time::sleep(inner.config.reconnect_grace).await;
                if let Ok(Some((channel, client))) = inner
                    .clients
                    .claim_abandoned_hls(
                        &token,
                        &format!("live:channel:{channel}:output:{format}:playlist"),
                        since,
                    )
                    .await
                {
                    let _ = inner.clients.stop_client(&channel, &client).await;
                }
            });
        }
    }
}
