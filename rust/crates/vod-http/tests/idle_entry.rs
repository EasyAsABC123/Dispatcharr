//! Component idle entry actual physical reply ownership; no mutation retries.
//! Original Dispatcharr and redis-rs authors retain application/client credit.
#[path = "generation_scope.rs"]
mod scoped;
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
async fn wait(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn begin(
    p: &Proxy,
) -> (
    scoped::functional::Fixture,
    scoped::functional::prototype::local::LocalScope,
) {
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&p.url)).await;
    let body = scope
        .attach_reserved(Arc::new(()), false, None)
        .await
        .unwrap();
    tokio::spawn({
        let scope = scope.clone();
        async move {
            let _ = scope.close(&body).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if f.redis.get::<_, i64>(f.counter()).await.unwrap() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    (f, scope)
}
fn admission(f: &scoped::functional::Fixture) -> scoped::functional::prototype::NewAdmission {
    let mut data = f.admission();
    data.fields
        .insert("content_obj_type".into(), "movie".into());
    data.fields
        .insert("m3u_profile_id".into(), f.profile.to_string());
    data
}
async fn empty(
    mut f: scoped::functional::Fixture,
    scope: scoped::functional::prototype::local::LocalScope,
) {
    // Explicit owned component teardown after assertions; no broad Redis flush.
    let _ = f.old.release().await;
    f.old.settlement().wait().await;
    let _: usize = f.redis.del(&[f.metadata(), f.stop()]).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), scope.drain())
        .await
        .unwrap();
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual CLAIM reply4s/caller abort retains physical body and control; pending follower unsupported"]
async fn idle_entry_claim_held_abort_rejects_follower_and_retains_physical_owner() {
    use scoped::functional::prototype::Error;
    let p = proxy("CLAIM", false).await;
    let (mut f, scope) = begin(&p).await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_entry_pending(caller, None).await.unwrap();
    let data = admission(&f);
    let task = tokio::spawn({
        let scope = scope.clone();
        let body = body.clone();
        async move { scope.enter_idle(&body, data, 1).await }
    });
    wait(|| p.seen.load(Ordering::SeqCst) == 1).await;
    task.abort();
    let _ = task.await;
    assert!(matches!(
        scope.attach_entry_pending(Arc::new(()), None).await,
        Err(Error::PartialState)
    ));
    scope.close(&body).await.unwrap();
    drop(body);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), scope.drain())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(weak.upgrade().is_some());
    assert_eq!(scope.control_slots(), 1);
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        "1"
    );
    p.release.send_replace(true);
    wait(|| weak.upgrade().is_none()).await;
    assert_eq!(p.seen.load(Ordering::SeqCst), 1);
    // Unacknowledged CLAIM is not Source270's acknowledged active claim +
    // held provider return. Its deadline/timer schedule remains unaccepted.
    let public: Option<String> = f.redis.hget(f.metadata(), "active_streams").await.unwrap();
    println!(
        "IDLE_UNACKNOWLEDGED_CLAIM_TIMER_METADATA_PRESENT {}",
        public.is_some()
    );
    assert!(public.is_none() || public.as_deref() == Some("0"));
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 0);
    empty(f, scope).await;
}
#[tokio::test]
#[ignore = "actual COMMIT executed reply lost: no replay, original receipt cleanup and reservation drain"]
async fn idle_entry_commit_lost_reply_cleans_once_without_request_replay() {
    let p = proxy("COMMIT", true).await;
    let (mut f, scope) = begin(&p).await;
    let body = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    let data = admission(&f);
    let task = tokio::spawn({
        let scope = scope.clone();
        let body = body.clone();
        async move { scope.enter_idle(&body, data, 1).await }
    });
    wait(|| p.seen.load(Ordering::SeqCst) == 1).await;
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "request_count")
            .await
            .unwrap(),
        "2"
    );
    p.release.send_replace(true);
    assert!(task.await.unwrap().is_err());
    wait(|| scope.control_slots() == 2).await;
    assert_eq!(p.seen.load(Ordering::SeqCst), 1);
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 0);
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        "0"
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "request_count")
            .await
            .unwrap(),
        "2"
    );
    scope.close(&body).await.unwrap();
    empty(f, scope).await;
}
#[tokio::test]
#[ignore = "actual held CLAIM + service stop: one claim/no reserve after physical reply"]
async fn idle_entry_claim_held_shutdown_does_not_release_physical_owner_early() {
    let p = proxy("CLAIM", false).await;
    let (mut f, scope) = begin(&p).await;
    let body = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    let data = admission(&f);
    let task = tokio::spawn({
        let scope = scope.clone();
        let body = body.clone();
        async move { scope.enter_idle(&body, data, 1).await }
    });
    wait(|| p.seen.load(Ordering::SeqCst) == 1).await;
    scope.stop();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), scope.drain())
            .await
            .is_err()
    );
    p.release.send_replace(true);
    assert!(task.await.unwrap().is_err());
    wait(|| scope.control_slots() == 2).await;
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        "0"
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 0);
    scope.close(&body).await.unwrap();
    empty(f, scope).await;
}
#[tokio::test]
#[ignore = "actual4s provider reservation reply retains caller/scope; ambiguous reservation is never replayed or admitted"]
async fn idle_entry_provider_reply_timeout_retains_physical_owner_then_rolls_back() {
    use dispatcharr_coordination::{Coordinator, LeaseConfig};
    use scoped::functional::{prototype, Fixture};
    let p = proxy("RESERVE_PROVIDER", false).await;
    let mut f = Fixture::new().await;
    assert!(f.old.release().await.unwrap());
    f.old.settlement().wait().await;
    f.coordinator = Coordinator::connect(
        &p.url,
        LeaseConfig {
            namespace: f.namespace.clone(),
            heartbeat_interval: None,
            reap_interval: None,
            ttl: Duration::from_secs(60),
            command_timeout: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    f.old = Arc::new(f.coordinator.acquire_vod(f.request()).await.unwrap());
    let (mut f, scope, _) = scoped::initial_from_fixture(f, None).await;
    let old = scope
        .attach_reserved(Arc::new(()), false, None)
        .await
        .unwrap();
    tokio::spawn({
        let scope = scope.clone();
        async move {
            let _ = scope.close(&old).await;
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if f.redis.get::<_, i64>(f.counter()).await.unwrap() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let body = scope.attach_entry_pending(caller, None).await.unwrap();
    let data = admission(&f);
    let task = tokio::spawn({
        let body = body.clone();
        let scope = scope.clone();
        async move { scope.enter_idle(&body, data, 1).await }
    });
    wait(|| p.seen.load(Ordering::SeqCst) == 1).await;
    task.abort();
    let _ = task.await;
    scope.close(&body).await.unwrap();
    drop(body);
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(weak.upgrade().is_some());
    assert_eq!(scope.control_slots(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), scope.drain())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), f.old.settlement().wait())
            .await
            .is_err()
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "request_count")
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    assert!(matches!(
        scope.attach_entry_pending(Arc::new(()), None).await,
        Err(prototype::Error::PartialState)
    ));
    p.release.send_replace(true);
    wait(|| weak.upgrade().is_none()).await;
    assert_eq!(p.seen.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        "0"
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "request_count")
            .await
            .unwrap(),
        "1"
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 0);
    empty(f, scope).await;
}
#[tokio::test]
#[ignore = "actual owner+public metadata replacement while idle claim reply held; no foreign decrement/reserve/delete"]
async fn idle_entry_foreign_publication_during_claim_is_not_touched() {
    let p = proxy("CLAIM", false).await;
    let (mut f, scope) = begin(&p).await;
    let body = scope
        .attach_entry_pending(Arc::new(()), None)
        .await
        .unwrap();
    let data = admission(&f);
    let task = tokio::spawn({
        let scope = scope.clone();
        let body = body.clone();
        async move { scope.enter_idle(&body, data, 1).await }
    });
    wait(|| p.seen.load(Ordering::SeqCst) == 1).await;
    let _: usize = f.redis.del(f.owner()).await.unwrap();
    let replacement = f.coordinator.acquire_vod(f.request()).await.unwrap();
    let _: () = f
        .redis
        .hset_multiple(
            f.metadata(),
            &[
                ("rust_owner_token", "replacement-public"),
                ("active_streams", "3"),
                ("request_count", "7"),
            ],
        )
        .await
        .unwrap();
    p.release.send_replace(true);
    assert!(task.await.unwrap().is_err());
    wait(|| scope.control_slots() == 2).await;
    assert_eq!(p.seen.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        replacement.token()
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "rust_owner_token")
            .await
            .unwrap(),
        "replacement-public"
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        "3"
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "request_count")
            .await
            .unwrap(),
        "7"
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    scope.close(&body).await.unwrap();
    assert!(replacement.release().await.unwrap());
    empty(f, scope).await;
}
