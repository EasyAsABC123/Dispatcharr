//! Shared Redis capability reads, retaining original upstream HLS attribution.
use crate::gateway::{cold_wait_exceeded, failure, lock, wall_time, Inner};
use axum::{body::Body, response::Response};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackError;
use dispatcharr_hls_core::{render_media_playlist, SegmentDescriptor};
use dispatcharr_output_store::{HlsColdState, SegmentBytes};
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
fn playlist_fields(
    value: &serde_json::Value,
    limit: usize,
) -> Result<(Vec<SegmentDescriptor>, u64, f64), PlaybackError> {
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
    Ok((window, disc_seq, behind))
}

fn playlist_timestamp(value: &serde_json::Value) -> Result<Option<f64>, PlaybackError> {
    let Some(timestamp) = value.get("ts").filter(|timestamp| !timestamp.is_null()) else {
        return Ok(None);
    };
    timestamp
        .as_f64()
        .filter(|value| value.is_finite())
        .map(Some)
        .ok_or_else(|| failure(500, "Playlist unavailable"))
}

pub(crate) fn descriptor(
    value: &serde_json::Value,
    limit: usize,
) -> Result<(Vec<SegmentDescriptor>, u64, f64, f64), PlaybackError> {
    let (window, disc_seq, behind) = playlist_fields(value, limit)?;
    let timestamp =
        playlist_timestamp(value)?.ok_or_else(|| failure(500, "Playlist unavailable"))?;
    Ok((window, disc_seq, behind, timestamp))
}

fn render(value: &serde_json::Value, limit: usize) -> Result<Vec<u8>, PlaybackError> {
    let (window, disc, behind) = playlist_fields(value, limit)?;
    let advertised = value.get("adv_target").and_then(|value| value.as_u64());
    let stale_after = advertised.unwrap_or_default().saturating_mul(3).max(45) as f64;
    if playlist_timestamp(value)?
        .is_some_and(|timestamp| timestamp != 0.0 && wall_time() - timestamp > stale_after)
    {
        return Err(failure(404, "Playlist stale"));
    }
    let target = value
        .get("target")
        .and_then(|value| value.as_f64())
        .filter(|value| value.is_finite())
        .unwrap_or(4.0);
    Ok(
        render_media_playlist(&window, target, "{seq}.ts", advertised, disc, Some(behind))
            .into_bytes(),
    )
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
    let poll = retry(&inner, &token, || {
        outputs.poll_hls_cold_playlist(&info.channel_id, &format, &info.client_id)
    })
    .await?;
    if poll.state == HlsColdState::Stopped {
        let _ = inner.clients.drop_hls_token(&token).await;
        return Err(failure(410, "Stream stopped"));
    }
    let init_grace = inner
        .engine
        .hls_initialization_grace(inner.config.initialization_timeout)
        .await?;
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
        let guard = guard;
        let started = Instant::now();
        let mut ready_since = None;
        let mut touched = started;
        let mut sent_marker = false;
        let mut count = 0;
        loop {
            if inner.stopped.load(Ordering::Acquire) {
                guard.completed.store(true, Ordering::Release);
                break;
            }
            let poll = match outputs.poll_hls_cold_playlist(&info.channel_id, &format, &info.client_id).await {
                Ok(poll) => poll,
                Err(_) => {
                    guard.completed.store(true, Ordering::Release);
                    break;
                }
            };
            if poll.state == HlsColdState::Stopped {
                guard.completed.store(true, Ordering::Release);
                let _ = inner.clients.drop_hls_token(&token).await;
                break;
            }
            let slot = slots.clone().acquire_owned().await.unwrap();
            if let Some(descriptor) = poll.descriptor {
                guard.completed.store(true, Ordering::Release);
                if let Ok(data) = render(&descriptor, inner.config.max_stored_segments) {
                    if let Ok(bytes) = frame(&inner, data, session.clone(), request.clone(), Some(slot)) {
                        yield Ok::<_, Infallible>(if sent_marker { bytes.slice(b"#EXTM3U\n".len()..) } else { bytes });
                    }
                }
                break;
            }
            let data = if sent_marker {
                count += 1;
                format!("# dispatcharr: waiting {count}\n").into_bytes()
            } else {
                sent_marker = true;
                b"#EXTM3U\n".to_vec()
            };
            match frame(&inner, data, session.clone(), request.clone(), Some(slot)) {
                Ok(bytes) => yield Ok(bytes),
                Err(_) => {
                    guard.completed.store(true, Ordering::Release);
                    break;
                }
            }
            let now = Instant::now();
            if cold_wait_exceeded(now, started, poll.state == HlsColdState::Ready, &mut ready_since, init_grace, inner.config.ready_timeout) {
                guard.completed.store(true, Ordering::Release);
                break;
            }
            if now.duration_since(touched) >= Duration::from_secs(3) {
                if touch(&inner, &token).await.is_err() {
                    guard.completed.store(true, Ordering::Release);
                    break;
                }
                touched = now;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
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
    let snapshot = retry(&inner, &token, || {
        outputs.snapshot(&info.channel_id, &format)
    })
    .await?;
    let expected = snapshot
        .owner
        .ok_or_else(|| failure(404, "Segment expired"))?;
    let (data, _retained) = retry(&inner, &token, || {
        outputs.get_owned_retained(
            &info.channel_id,
            &format,
            &expected,
            seq,
            (inner.clone(), session.clone(), request.clone()),
        )
    })
    .await?;
    let data = data.ok_or_else(|| failure(404, "Segment expired"))?;
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

#[cfg(test)]
mod playlist_compatibility_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_playlist_reads_preserve_source_target_fallbacks() {
        let populated = json!({"window": [{"seq": 7, "dur": 3.25}], "target": 9});
        let playlist = String::from_utf8(render(&populated, 5).unwrap()).unwrap();
        assert!(playlist.contains("#EXT-X-TARGETDURATION:4\n"));
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:7\n#EXTINF:3.250,\n7.ts\n"));

        let empty = json!({"window": [], "target": 7.5, "ts": 0});
        let playlist = String::from_utf8(render(&empty, 5).unwrap()).unwrap();
        assert!(playlist.contains("#EXT-X-TARGETDURATION:8\n#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert!(descriptor(&populated, 5).is_err());
    }

    #[test]
    fn announced_target_and_stale_window_still_match_source() {
        let mut value =
            json!({"window": [{"seq": 7, "dur": 3.25}], "adv_target": 8, "ts": wall_time()});
        let playlist = String::from_utf8(render(&value, 5).unwrap()).unwrap();
        assert!(playlist.contains("#EXT-X-TARGETDURATION:8\n"));

        value["ts"] = json!(wall_time() - 120.0);
        let error = render(&value, 5).unwrap_err();
        assert_eq!(error.status, 404);
        assert_eq!(error.body, json!({"error": "Playlist stale"}));
    }
}
