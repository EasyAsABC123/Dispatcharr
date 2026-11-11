//! Pure CLASSIFY dropped-reply controls; no mutation/admission retries.
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
    reads: Arc<AtomicUsize>,
    noscript: Arc<AtomicUsize>,
    mutations: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy(mode: u8) -> Proxy {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("REDIS_TEST_URL").as_deref(),
        Ok("redis://:playback-fixture-only@127.0.0.1:6379/0")
    );
    let source = include_str!("../src/generation_scope.rs");
    let script = source
        .split_once("const CLASSIFY: &str = r#\"")
        .unwrap()
        .1
        .split_once("\"#;")
        .unwrap()
        .0
        .to_owned();
    let hash = redis::Script::new(&script).get_hash().to_owned();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let reads = Arc::new(AtomicUsize::new(0));
    let seen = reads.clone();
    let noscript = Arc::new(AtomicUsize::new(0));
    let misses = noscript.clone();
    let mutations = Arc::new(AtomicUsize::new(0));
    let writes = mutations.clone();
    let (release, ready) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept(),if jobs.len()<4=>{
                    let(mut client,_)=accepted.unwrap();let script=script.clone();let hash=hash.clone();
                    let seen=seen.clone();let misses=misses.clone();let writes=writes.clone();let mut ready=ready.clone();
                    jobs.spawn(async move {
                        let mut server=TcpStream::connect("127.0.0.1:6379").await?;
                        let(mut input,mut output)=(Vec::new(),Vec::new());
                        loop {
                            let command=frame(&mut client,&mut input).await?;
                            let args=arguments(&command).ok_or(std::io::ErrorKind::InvalidData)?;
                            let eval=args.first().is_some_and(|v|v==b"EVALSHA"||v==b"EVAL");
                            let classify=args.first().is_some_and(|v|v==b"EVALSHA")&&args.get(1).is_some_and(|v|v==hash.as_bytes())
                                ||args.first().is_some_and(|v|v==b"EVAL")&&args.get(1).is_some_and(|v|v==script.as_bytes());
                            if eval&&!classify&&seen.load(Ordering::SeqCst)>0 {writes.fetch_add(1,Ordering::SeqCst);}
                            server.write_all(&command).await?;let response=frame(&mut server,&mut output).await?;
                            if classify {
                                if response.starts_with(b"-NOSCRIPT") {misses.fetch_add(1,Ordering::SeqCst);}
                                else {
                                    let sequence=seen.fetch_add(1,Ordering::SeqCst)+1;
                                    if sequence==1&&(mode==0||mode==4){return Ok::<(),std::io::Error>(());}
                                    if sequence==1&&(mode==1||mode==2)||sequence==2&&mode==4 {
                                        if ready.wait_for(|v|*v).await.is_err(){return Ok(());}
                                        if mode==2{return Ok(());}
                                    }
                                }
                            }
                            client.write_all(&response).await?;
                        }
                    });
                },
                _=jobs.join_next(),if !jobs.is_empty()=>{}
            }
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        reads,
        noscript,
        mutations,
        release,
        task,
    }
}
async fn wait(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "actual lost readonly CLASSIFY reply plus NOSCRIPT; no admission/mutation repeat"]
async fn classify_read_lost_reply_repeats_only_one_pure_read() {
    let p = proxy(0).await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&p.url)).await;
    f.erase_owned_generation().await;
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut f.redis)
        .await
        .unwrap();
    let result = scope.retire_missing().await;
    let reads = p.reads.load(Ordering::SeqCst);
    let writes = p.mutations.load(Ordering::SeqCst);
    assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
    assert!(!f.redis.exists::<_, bool>(f.owner()).await.unwrap());
    assert!(p.noscript.load(Ordering::SeqCst) >= 1);
    println!(
        "CLASSIFY_READ_PROOF {}",
        serde_json::json!({"logical_ok":result.is_ok(),"actual_reads":reads,"other_mutations":writes})
    );
    drop(scope);
    f.empty().await;
    assert!(
        result.is_ok(),
        "a dropped pure classification can be re-read, never interpreted as missing"
    );
    assert_eq!(reads, 2);
    assert_eq!(writes, 0);
}
#[tokio::test]
#[ignore = "actual WRONGTYPE is not an I/O retry or missing-state inference"]
async fn classify_read_server_wrongtype_is_not_retried() {
    let p = proxy(3).await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&p.url)).await;
    f.erase_owned_generation().await;
    let _: () = f
        .redis
        .set(f.metadata(), "owned-malformed-fixture")
        .await
        .unwrap();
    assert!(scope.retire_missing().await.is_err());
    assert_eq!(p.reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.redis.get::<_, String>(f.metadata()).await.unwrap(),
        "owned-malformed-fixture"
    );
    let _: usize = f.redis.del(f.metadata()).await.unwrap();
    drop(scope);
    f.empty().await;
}
#[tokio::test]
#[ignore = "fresh retry must discover an actual replacement lease/metadata and preserve counters"]
async fn classify_read_foreign_replacement_between_lost_reply_and_retry_is_fenced() {
    let p = proxy(2).await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&p.url)).await;
    f.erase_owned_generation().await;
    let worker = scope.clone();
    let task = tokio::spawn(async move { worker.retire_missing().await });
    wait(|| p.reads.load(Ordering::SeqCst) == 1).await;
    let replacement = f.coordinator.acquire_vod(f.request()).await.unwrap();
    let _: () = f
        .redis
        .hset_multiple(
            f.metadata(),
            &[
                ("rust_owner_token", "owned-foreign-meta"),
                ("user_id", "foreign"),
                ("active_streams", "1"),
            ],
        )
        .await
        .unwrap();
    p.release.send_replace(true);
    let result = task.await.unwrap();
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        replacement.token()
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "rust_owner_token")
            .await
            .unwrap(),
        "owned-foreign-meta"
    );
    let reads = p.reads.load(Ordering::SeqCst);
    replacement.release().await.unwrap();
    replacement.settlement().wait().await;
    let _: usize = f.redis.del(f.metadata()).await.unwrap();
    drop(scope);
    f.empty().await;
    assert!(matches!(
        result,
        Err(scoped::functional::prototype::Error::ForeignOwner)
    ));
    assert_eq!(reads, 2);
    assert_eq!(p.mutations.load(Ordering::SeqCst), 0);
}
#[tokio::test]
#[ignore = "hard service stop before dropped read settles forbids another operation"]
async fn classify_read_stop_between_error_and_retry_starts_no_second_read() {
    let p = proxy(2).await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&p.url)).await;
    f.erase_owned_generation().await;
    let worker = scope.clone();
    let task = tokio::spawn(async move { worker.retire_missing().await });
    wait(|| p.reads.load(Ordering::SeqCst) == 1).await;
    scope.stop();
    p.release.send_replace(true);
    assert!(task.await.unwrap().is_err());
    assert_eq!(p.reads.load(Ordering::SeqCst), 1);
    assert_eq!(p.mutations.load(Ordering::SeqCst), 0);
    drop(scope);
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual4s read reply survives cancelled observer and shutdown/drain"]
async fn classify_read_cancelled_observer_retains_caller_and_control_until_actual_reply() {
    held_read(1, 1).await;
}
#[tokio::test]
#[ignore = "actual4s retried read reply retains the same caller/control after observer abort"]
async fn classify_read_cancelled_observer_retains_actual_retry_reply() {
    held_read(4, 2).await;
}
async fn held_read(mode: u8, expected: usize) {
    let p = proxy(mode).await;
    let (mut f, scope, _) = scoped::initial_with_connection(Some(&p.url)).await;
    f.erase_owned_generation().await;
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let worker = scope.clone();
    let task = tokio::spawn(async move { worker.retire_missing_retained(caller).await });
    wait(|| p.reads.load(Ordering::SeqCst) == expected).await;
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    scope.stop();
    assert!(weak.upgrade().is_some());
    assert_eq!(scope.control_slots(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), scope.drain())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(1800)).await;
    p.release.send_replace(true);
    wait(|| weak.upgrade().is_none()).await;
    scope.drain().await;
    assert_eq!(p.reads.load(Ordering::SeqCst), expected);
    assert_eq!(p.mutations.load(Ordering::SeqCst), 0);
    drop(scope);
    f.empty().await;
}
