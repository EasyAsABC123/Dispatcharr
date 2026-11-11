//! Component idle entry actual physical reply ownership; no mutation retries.
//! Original Dispatcharr and redis-rs authors retain application/client credit.
use redis::AsyncCommands;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
fn end_frame(data: &[u8], start: usize, depth: usize) -> std::io::Result<Option<usize>> {
    if depth > 16 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let Some(&tag) = data.get(start) else {
        return Ok(None);
    };
    let Some(line) = data[start..]
        .windows(2)
        .position(|v| v == b"\r\n")
        .map(|v| v + start)
    else {
        return Ok(None);
    };
    let next = line + 2;
    if matches!(tag, b'+' | b'-' | b':' | b'_') {
        return Ok(Some(next));
    }
    let value = std::str::from_utf8(&data[start + 1..line])
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .ok_or(std::io::ErrorKind::InvalidData)?;
    if value < 0 {
        return Ok(Some(next));
    }
    if value > 65536 {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    match tag {
        b'$' => {
            let end = next
                .checked_add(value as usize)
                .and_then(|n| n.checked_add(2))
                .ok_or(std::io::ErrorKind::InvalidData)?;
            Ok((end <= data.len()).then_some(end))
        }
        b'*' => {
            if value > 1024 {
                return Err(std::io::ErrorKind::InvalidData.into());
            }
            let mut end = next;
            for _ in 0..value {
                let Some(value) = end_frame(data, end, depth + 1)? else {
                    return Ok(None);
                };
                end = value;
            }
            Ok(Some(end))
        }
        _ => Err(std::io::ErrorKind::InvalidData.into()),
    }
}
async fn frame(stream: &mut TcpStream, data: &mut Vec<u8>) -> std::io::Result<Vec<u8>> {
    loop {
        if let Some(end) = end_frame(data, 0, 0)? {
            return Ok(data.drain(..end).collect());
        }
        let mut chunk = [0u8; 4096];
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        if data.len() + count > 65536 {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        data.extend_from_slice(&chunk[..count]);
    }
}
fn arguments(frame: &[u8]) -> Option<Vec<Vec<u8>>> {
    if frame.first() != Some(&b'*') {
        return None;
    }
    let end = frame.windows(2).position(|x| x == b"\r\n")?;
    let count = std::str::from_utf8(&frame[1..end])
        .ok()?
        .parse::<usize>()
        .ok()?;
    if count > 1024 {
        return None;
    }
    let mut offset = end + 2;
    let mut args = Vec::new();
    for _ in 0..count {
        if frame.get(offset) != Some(&b'$') {
            return None;
        }
        let end = offset + frame[offset..].windows(2).position(|x| x == b"\r\n")?;
        let size = std::str::from_utf8(&frame[offset + 1..end])
            .ok()?
            .parse::<usize>()
            .ok()?;
        let start = end + 2;
        let finish = start.checked_add(size)?;
        if frame.get(finish..finish + 2) != Some(b"\r\n") {
            return None;
        }
        args.push(frame[start..finish].to_vec());
        offset = finish + 2;
    }
    Some(args)
}

struct Proxy {
    url: String,
    seen: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy(kind: &str, drop_reply: bool) -> Proxy {
    let extract = |source: &str, name: &str| {
        source
            .split_once(&format!("const {name}: &str = r#\""))
            .unwrap()
            .1
            .split_once("\"#;")
            .unwrap()
            .0
            .to_owned()
    };
    let script = if kind == "RESERVE_PROVIDER" {
        let source = include_str!("../../coordination/src/scripts.rs");
        extract(source, "REAP") + &extract(source, "RESERVE_PROVIDER")
    } else {
        extract(include_str!("../src/generation_idle.rs"), kind)
    };
    let hash = redis::Script::new(&script).get_hash().to_owned();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(AtomicUsize::new(0));
    let count = seen.clone();
    let (release, ready) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept(),if jobs.len()<4=>{
                    let(mut client,_)=accepted.unwrap();let script=script.clone();let hash=hash.clone();let count=count.clone();let mut ready=ready.clone();
                    jobs.spawn(async move {
                        let mut server=TcpStream::connect("127.0.0.1:6379").await?;let(mut input,mut output)=(Vec::new(),Vec::new());
                        loop{
                            let command=frame(&mut client,&mut input).await?;let args=arguments(&command).ok_or(std::io::ErrorKind::InvalidData)?;
                            let target=args.first().is_some_and(|x|x==b"EVALSHA")&&args.get(1).is_some_and(|x|x==hash.as_bytes())
                                ||args.first().is_some_and(|x|x==b"EVAL")&&args.get(1).is_some_and(|x|x==script.as_bytes());
                            server.write_all(&command).await?;let response=frame(&mut server,&mut output).await?;
                            if target&&!response.starts_with(b"-NOSCRIPT") {
                                let n=count.fetch_add(1,Ordering::SeqCst);
                                if n==0 {ready.wait_for(|x|*x).await.map_err(|_|std::io::ErrorKind::BrokenPipe)?;if drop_reply{return Ok::<(),std::io::Error>(());}}
                            }
                            client.write_all(&response).await?;
                        }
                    });
                },_=jobs.join_next(),if !jobs.is_empty()=>{}
            }
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        seen,
        release,
        task,
    }
}
use axum::{routing::get, Router};
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{StoreError, User};
use dispatcharr_vod_http::{
    ContentKind, VodCandidate, VodConfig, VodEngine, VodRequest, VodResolver, VodSource,
};
use futures_util::stream;
use std::net::SocketAddr;
struct Resolver(VodSource);
impl VodResolver for Resolver {
    fn authorize<'a>(
        &'a self,
        _: &'a VodRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async { Ok(None) })
    }
    fn resolve<'a>(
        &'a self,
        _: &'a VodRequest,
        _: Option<User>,
    ) -> BoxFuture<'a, Result<VodSource, StoreError>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async { Ok(false) })
    }
    fn admin<'a>(&'a self, _: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}
async fn serve(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{address}"), task)
}
async fn until(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn prefix(response: &mut reqwest::Response) -> Vec<u8> {
    let mut retained = Vec::with_capacity(8192);
    while retained.len() < 8192 {
        let chunk = tokio::time::timeout(Duration::from_secs(5), response.chunk())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(chunk.len() <= 1024 * 1024);
        let keep = (8192 - retained.len()).min(chunk.len());
        retained.extend_from_slice(&chunk[..keep]);
    }
    retained
}
async fn actual_pending(busy: bool, cancel: bool) {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    assert_eq!(url, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let media = Arc::new(std::fs::read(std::env::var("VOD_RECOVERY_MEDIA").unwrap()).unwrap());
    assert!(media.len() > 1000000 && media.len() <= 5 * 1024 * 1024);
    let data = media.clone();
    let (origin, origin_task) = serve(Router::new().route(
        "/fixture.mp4",
        get(move || {
            let data = data.clone();
            async move {
                let len = data.len();
                let body = stream::unfold((data, 0), |(data, at)| async move {
                    if at == data.len() {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let end = (at + 8192).min(data.len());
                    Some((Ok::<_, std::io::Error>(data[at..end].to_vec()), (data, end)))
                });
                axum::response::Response::builder()
                    .header("content-type", "video/mp4")
                    .header("content-length", len)
                    .body(axum::body::Body::from_stream(body))
                    .unwrap()
            }
        }),
    ))
    .await;
    let p = proxy("RESERVE_PROVIDER", false).await;
    let namespace = format!("pending-http-{}", uuid::Uuid::new_v4().simple());
    let coordinator = Coordinator::connect(
        &p.url,
        LeaseConfig {
            namespace: namespace.clone(),
            heartbeat_interval: None,
            reap_interval: None,
            command_timeout: Duration::from_secs(5),
            ttl: Duration::from_secs(60),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let profile = (uuid::Uuid::new_v4().as_u128() % 1000000000) as i64 + 1000;
    let content = uuid::Uuid::new_v4().to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let source = VodSource {
        kind: ContentKind::Movie,
        content_uuid: content.clone(),
        name: "Owned pending follower HTTP".into(),
        user: None,
        redirect: false,
        limit_policy: Default::default(),
        candidates: vec![VodCandidate {
            relation_id: 1,
            account_id: 1,
            profile_id: profile,
            max_streams: 1,
            url: format!("{origin}/fixture.mp4"),
            user_agent: "owned-pending-http".into(),
        }],
    };
    let engine = VodEngine::with_resolver(
        Arc::new(Resolver(source)),
        coordinator.clone(),
        &url,
        VodConfig {
            max_sessions: 2,
            max_requests: 4,
            frame_bytes: 64 * 1024,
            idle_grace: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_redis_recovery(&namespace)
    .unwrap();
    let (base, app_task) = serve(engine.clone().router()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let path = format!("{base}/proxy/vod/movie/{content}/{id}");
    let mut first = client.get(&path).send().await.unwrap();
    assert_eq!(first.status(), 200);
    assert_eq!(prefix(&mut first).await.as_slice(), &media[..8192]);
    drop(first);
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let metadata = format!("vod_persistent_connection:{id}");
    let counter = format!("profile_connections:{profile}");
    until(async || {
        redis
            .hget::<_, _, Option<i64>>(&metadata, "active_streams")
            .await
            .unwrap()
            == Some(0)
    })
    .await;
    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 0);
    let other = if busy {
        Some(
            coordinator
                .acquire_vod(dispatcharr_coordination::AcquireRequest {
                    channel_id: uuid::Uuid::new_v4().to_string(),
                    provider_profile_id: Some(profile),
                    provider_max_streams: 1,
                    credential_pool: None,
                })
                .await
                .unwrap(),
        )
    } else {
        None
    };
    let target = tokio::spawn({
        let client = client.clone();
        let path = path.clone();
        async move { client.get(path).send().await.unwrap() }
    });
    until(async || p.seen.load(Ordering::SeqCst) == 1).await;
    let stopped = client
        .post(format!("{base}/proxy/vod/stop_client/"))
        .json(&serde_json::json!({"client_id":id}))
        .send()
        .await
        .unwrap();
    assert_eq!(stopped.status(), 200);
    let mut follower = client.get(&path).send().await.unwrap();
    assert_eq!(follower.status(), 200, "actual pending follower response");
    assert_eq!(prefix(&mut follower).await.as_slice(), &media[..8192]);
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "active_streams")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "request_count")
            .await
            .unwrap(),
        2
    );
    assert!(
        redis
            .exists::<_, bool>(format!("vod_proxy:client:{id}:stop"))
            .await
            .unwrap(),
        "no premature stop consumption before100completedSourceunits"
    );
    // Exceed this fixture's previous one-second cleanup timer while the
    // provider reply stays held. The admitted follower must keep metadata and
    // physical reservation alive; this is not a latency/performance sample.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "active_streams")
            .await
            .unwrap(),
        2
    );
    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
    let mut target = Some(target);
    if cancel {
        target.take().unwrap().abort();
    }
    p.release.send_replace(true);
    let mut response = None;
    if let Some(target) = target {
        let target = target.await.unwrap();
        assert_eq!(target.status(), if busy { 429 } else { 200 });
        if busy {
            assert_eq!(
                target.bytes().await.unwrap().as_ref(),
                b"Connection limit exceeded for profile"
            );
        } else {
            response = Some(target);
        }
    }
    if cancel || busy {
        until(async || {
            redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap()
                == 1
        })
        .await;
    }
    drop(follower);
    drop(response);
    until(async || {
        redis
            .hget::<_, _, Option<i64>>(&metadata, "active_streams")
            .await
            .unwrap()
            .unwrap_or(0)
            == 0
    })
    .await;
    if let Some(other) = other {
        assert_eq!(
            redis.get::<_, i64>(&counter).await.unwrap(),
            1,
            "foreign physical reservation remains despite Source public undercount"
        );
        assert!(other.release().await.unwrap());
        other.settlement().wait().await;
    }
    assert!(engine.shutdown().await);
    assert_eq!(
        redis
            .get::<_, Option<i64>>(&counter)
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );
    assert_eq!(
        p.seen.load(Ordering::SeqCst),
        1,
        "provider reserve is never replayed"
    );
    app_task.abort();
    origin_task.abort();
}
#[tokio::test]
#[ignore = "actual HTTP/provider reply held; Source TCP early pending follower200+bytes, target200"]
async fn pending_http_success() {
    actual_pending(false, false).await;
}
#[tokio::test]
#[ignore = "actual HTTP/provider-full held; follower200+bytes, exact target429, foreign reservation preserved"]
async fn pending_http_busy() {
    actual_pending(true, false).await;
}
#[tokio::test]
#[ignore = "actual pending HTTP client disconnect; existing follower and physical reservation remain owned"]
async fn pending_http_client_abort() {
    actual_pending(false, true).await;
}
