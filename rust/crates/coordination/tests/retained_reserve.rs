//! Actual held/lost replies for ordinary coordinator lease operations.
//! Original Dispatcharr and redis-rs authors retain algorithm/client credit.
use dispatcharr_coordination::{AcquireRequest, Coordinator, LeaseConfig, ProviderReservation};
use redis::AsyncCommands;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
#[allow(dead_code)]
mod source {
    include!("../src/scripts.rs");
}
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

struct ReplyProxy {
    url: String,
    seen: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for ReplyProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn reply_proxy(drop_reply: bool) -> ReplyProxy {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert_eq!(
        std::env::var("REDIS_TEST_URL").unwrap(),
        "redis://:playback-fixture-only@127.0.0.1:6379/0"
    );
    let sha = redis::Script::new(&format!("{}{}", source::REAP, source::RESERVE_PROVIDER))
        .get_hash()
        .to_owned();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(AtomicUsize::new(0));
    let count = seen.clone();
    let (release, ready) = watch::channel(false);
    let once = Arc::new(AtomicBool::new(true));
    let task = tokio::spawn(async move {
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
            accepted=listener.accept(),if jobs.len()<4=>{let(mut client,_)=accepted.unwrap();let sha=sha.clone();let count=count.clone();let once=once.clone();let mut ready=ready.clone();jobs.spawn(async move{let mut server=TcpStream::connect("127.0.0.1:6379").await?;let(mut incoming,mut outgoing)=(Vec::new(),Vec::new());loop{
            let request=frame(&mut client,&mut incoming).await?;let args=arguments(&request).ok_or(std::io::ErrorKind::InvalidData)?;let target=args.first().is_some_and(|v|v==b"EVALSHA")&&args.get(1).is_some_and(|v|v==sha.as_bytes());server.write_all(&request).await?;let response=frame(&mut server,&mut outgoing).await?;
            if target&&!response.starts_with(b"-NOSCRIPT"){count.fetch_add(1,Ordering::SeqCst);if once.swap(false,Ordering::SeqCst){ready.wait_for(|v|*v).await.map_err(|_|std::io::ErrorKind::BrokenPipe)?;if drop_reply{return Ok::<(),std::io::Error>(());}}}client.write_all(&response).await?;
            } });},_=jobs.join_next(),if !jobs.is_empty()=>{}}
        }
    });
    ReplyProxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        seen,
        release,
        task,
    }
}
async fn until(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn retained_case(kind: &str) {
    let p = reply_proxy(kind == "lost").await;
    let namespace = format!("retained-reserve-{}", uuid::Uuid::new_v4().simple());
    let profile = (uuid::Uuid::new_v4().as_u128() % 1000000000) as i64 + 10000;
    let counter = format!("profile_connections:{profile}");
    let coordinator = Coordinator::connect(
        &p.url,
        LeaseConfig {
            namespace: namespace.clone(),
            heartbeat_interval: None,
            reap_interval: None,
            max_inflight_commands: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let lease = Arc::new(
        coordinator
            .acquire_vod(AcquireRequest {
                channel_id: namespace.clone(),
                provider_profile_id: None,
                provider_max_streams: 0,
                credential_pool: None,
            })
            .await
            .unwrap(),
    );
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let other = if kind == "busy" {
        Some(
            coordinator
                .acquire_vod(AcquireRequest {
                    channel_id: format!("{namespace}-other"),
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
    if kind == "error" {
        let _: () = redis.set(&counter, "not-a-counter").await.unwrap();
    }
    let current = lease.clone();
    let ordinary = kind == "legacy";
    let mut task = tokio::spawn(async move {
        let request = ProviderReservation {
            profile_id: Some(profile),
            max_streams: 1,
            credential_pool: None,
        };
        if ordinary {
            current.reserve_provider(request).await
        } else {
            current.reserve_provider_retained_result(request).await
        }
    });
    until(|| p.seen.load(Ordering::SeqCst) == 1).await;
    let held = std::time::Instant::now();
    let caller_aborted = kind == "caller-loss";
    if caller_aborted {
        task.abort();
        let _ = (&mut task).await;
    }
    if kind == "outer-expired" {
        assert!(tokio::time::timeout(Duration::from_secs(3), &mut task)
            .await
            .is_err());
        assert!(!coordinator.is_idle());
        assert!(!lease.settlement().is_settled());
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            held + Duration::from_millis(3400),
        ))
        .await;
    } else {
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            held + Duration::from_millis(2400),
        ))
        .await;
    }
    assert!(!coordinator.is_idle());
    assert!(!lease.settlement().is_settled());
    if !caller_aborted && kind != "legacy" {
        assert!(
            !task.is_finished(),
            "retained result stays owned beyond ordinary2s observer"
        );
    }
    if kind == "legacy" {
        assert!(task.is_finished(), "existing global2s result unchanged");
    }
    p.release.send_replace(true);
    let outcome = if caller_aborted {
        None
    } else {
        Some(task.await.unwrap())
    };
    lease.settlement().wait().await;
    match kind {
        "success" | "outer-expired" => assert_eq!(outcome, Some(Ok(()))),
        "busy" => assert_eq!(
            outcome,
            Some(Err(dispatcharr_coordination::Error::ProfileFull))
        ),
        "error" | "lost" | "legacy" => assert_eq!(
            outcome,
            Some(Err(dispatcharr_coordination::Error::RedisUnavailable))
        ),
        "caller-loss" => {}
        _ => unreachable!(),
    }
    let ledger = format!("{namespace}:lease:{}", lease.token());
    let reserved: String = redis.hget(&ledger, "reserved").await.unwrap();
    assert_eq!(
        reserved,
        if matches!(kind, "busy" | "error") {
            "0"
        } else {
            "1"
        }
    );
    if kind == "error" {
        let _: u64 = redis.del(&counter).await.unwrap();
    }
    assert!(lease.release().await.unwrap());
    lease.settlement().wait().await;
    if let Some(other) = other {
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        assert!(other.release().await.unwrap());
        other.settlement().wait().await;
    }
    until(|| coordinator.is_idle()).await;
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
        "no uncertain mutation replay"
    );
    let keys: Vec<String> = redis.keys(format!("{namespace}:*")).await.unwrap();
    assert!(keys.len() < 16);
    if !keys.is_empty() {
        let _: u64 = redis.del(keys).await.unwrap();
    }
    let _: u64 = redis.del(counter).await.unwrap();
    println!("RETAINED_RESULT kind={kind} physical_owned=true no_replay=true final_counter=0");
}
macro_rules! retained_test {
    ($name:ident,$kind:literal) => {
        #[tokio::test]
        #[ignore = "actual disposable Redis physical reply/no replay"]
        async fn $name() {
            retained_case($kind).await;
        }
    };
}
retained_test!(retained_success_actual_reply_after_two_seconds, "success");
retained_test!(retained_busy_actual_typed_reply_after_two_seconds, "busy");
retained_test!(retained_actual_redis_error_not_success, "error");
retained_test!(retained_lost_reply_never_infers_success_or_replays, "lost");
retained_test!(retained_caller_loss_keeps_physical_admission, "caller-loss");
retained_test!(
    retained_outer_observer_expires_but_transition_still_settles,
    "outer-expired"
);
retained_test!(ordinary_two_second_observer_remains_unchanged, "legacy");
