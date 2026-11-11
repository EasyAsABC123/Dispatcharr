//! Owned exact ADMIT/PendingCreate reply tests; no production transport changes.
//! Original Dispatcharr and redis-rs authors retain algorithm/client credit.
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
#[derive(Default)]
struct Counts {
    admit_noscript: AtomicUsize,
    admit_written: AtomicUsize,
    cleanup_noscript: AtomicUsize,
    cleanup_done: AtomicUsize,
}
struct Proxy {
    url: String,
    counts: Arc<Counts>,
    release_admit: watch::Sender<bool>,
    release_cleanup: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy(key: String, drop_admit: bool) -> Proxy {
    let (url, _) = super::config();
    let target = redis::Client::open(url)
        .unwrap()
        .get_connection_info()
        .addr
        .clone();
    let address = match target {
        redis::ConnectionAddr::Tcp(host, 6379) if host == "127.0.0.1" => "127.0.0.1:6379",
        _ => panic!("owned loopback only"),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let counts = Arc::new(Counts::default());
    let seen = counts.clone();
    let (admit_tx, admit_rx) = watch::channel(drop_admit);
    let (cleanup_tx, cleanup_rx) = watch::channel(!drop_admit);
    let cleanup_sha=redis::Script::new("if redis.call('HGET',KEYS[1],'rust_owner_token')==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0").get_hash().to_owned();
    let task = tokio::spawn(async move {
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {connection=listener.accept(),if jobs.len()<4=>{let(mut client,_)=connection.unwrap();let mut admit=admit_rx.clone();let mut cleanup=cleanup_rx.clone();let key=key.clone();let sha=cleanup_sha.clone();let seen=seen.clone();jobs.spawn(async move {let mut server=TcpStream::connect(address).await?;let mut incoming=Vec::new();let mut outgoing=Vec::new();loop{let command=frame(&mut client,&mut incoming).await?;let args=arguments(&command).ok_or(std::io::ErrorKind::InvalidData)?;let is_admit=args.first().is_some_and(|x|x==b"EVALSHA")&&args.get(3).is_some_and(|x|x==key.as_bytes())&&args.get(9).is_some_and(|x|x==b"create");let is_cleanup=args.first().is_some_and(|x|x==b"EVALSHA")&&args.get(1).is_some_and(|x|x==sha.as_bytes())&&args.get(3).is_some_and(|x|x==key.as_bytes());server.write_all(&command).await?;let response=frame(&mut server,&mut outgoing).await?;
                       if response.starts_with(b"-NOSCRIPT") {if is_admit{seen.admit_noscript.fetch_add(1,Ordering::SeqCst);}
            if is_cleanup{seen.cleanup_noscript.fetch_add(1,Ordering::SeqCst);}}
                       if is_admit&&arguments(&response).is_some_and(|x|x.first().is_some_and(|v|v==b"OK")){seen.admit_written.fetch_add(1,Ordering::SeqCst);if drop_admit{return Err::<(),std::io::Error>(std::io::ErrorKind::ConnectionReset.into());}
            if admit.wait_for(|release|*release).await.is_err(){return Ok(());}}
                       if is_cleanup&&response==b":1\r\n"{seen.cleanup_done.fetch_add(1,Ordering::SeqCst);if cleanup.wait_for(|release|*release).await.is_err(){return Ok(());}}
                       client.write_all(&response).await?;
                       }});},_=jobs.join_next(),if !jobs.is_empty()=>{}}
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        counts,
        release_admit: admit_tx,
        release_cleanup: cleanup_tx,
        task,
    }
}
async fn wait(mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(6), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn run(drop_reply: bool) {
    let (_, mut control, source, request, id) = super::physical_fixture(if drop_reply {
        "lost-admit"
    } else {
        "delayed-admit"
    })
    .await;
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut control)
        .await
        .unwrap();
    let proxy = proxy(super::super::key(&id), drop_reply).await;
    let (url, _) = super::config();
    let coordinator = dispatcharr_coordination::Coordinator::connect(
        &url,
        dispatcharr_coordination::LeaseConfig {
            namespace: id.clone(),
            heartbeat_interval: None,
            reap_interval: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let pool = super::super::SessionPool::connect(
        &proxy.url,
        coordinator,
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let worker = pool.clone();
    let session_id = id.clone();
    let task = tokio::spawn(async move { worker.enter(&session_id, source, &request).await });
    wait(|| proxy.counts.admit_written.load(Ordering::SeqCst) == 1).await;
    if drop_reply {
        wait(|| proxy.counts.cleanup_done.load(Ordering::SeqCst) == 1).await;
    }
    let result = tokio::time::timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap();
    assert!(
        result.is_err(),
        "lost/delayed ADMIT observer must not report success"
    );
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(pool.inner.requests.available_permits(), 0);
    assert_eq!(pool.inner.slots.available_permits(), 0);
    assert_eq!(pool.inner.pending_cleanups.load(Ordering::Acquire), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(60), pool.drain())
            .await
            .is_err(),
        "drain must not complete before actual cleanup response"
    );
    assert_eq!(proxy.counts.admit_noscript.load(Ordering::SeqCst), 1);
    assert_eq!(
        proxy.counts.admit_written.load(Ordering::SeqCst),
        1,
        "uncertain ADMIT must never be replayed"
    );
    proxy.release_admit.send_replace(true);
    proxy.release_cleanup.send_replace(true);
    tokio::time::timeout(Duration::from_secs(6), pool.drain())
        .await
        .unwrap();
    assert!(pool.cleanup_succeeded());
    assert_eq!(proxy.counts.cleanup_noscript.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.counts.cleanup_done.load(Ordering::SeqCst), 1);
    let exists: bool = redis::cmd("EXISTS")
        .arg(super::super::key(&id))
        .query_async(&mut control)
        .await
        .unwrap();
    assert!(!exists);
    let count: Option<i64> = redis::cmd("GET")
        .arg("profile_connections:777")
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(count.unwrap_or(0), 0);
    assert_eq!(pool.inner.requests.available_permits(), 1);
    assert_eq!(pool.inner.slots.available_permits(), 1);
}
#[tokio::test]
#[ignore = "owned real Redis lost ADMIT and explicit NOSCRIPT cleanup"]
async fn lost_admit_reply_retains_pending_cleanup_through_noscript_and_real_reply() {
    run(true).await;
}
#[tokio::test]
#[ignore = "owned real Redis ADMIT logical timeout and later mutation/cleanup"]
async fn delayed_admit_reply_retains_pending_cleanup_after_logical_timeout() {
    run(false).await;
}

#[path = "session_lookup_reply.rs"]
mod lookup_reply;
