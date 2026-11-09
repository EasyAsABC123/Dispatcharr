//! Delayed actual Redis replies: cancellation cannot race archive publication.
//! All original application credit belongs to upstream Dispatcharr contributors.
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_timeshift_http::{ArchivePool, ArchivePoolConfig, ArchivePoolRequest};
use redis::AsyncCommands;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
struct Proxy {
    url: String,
    hold: watch::Sender<bool>,
    task: JoinHandle<()>,
    pause_after_reply: Arc<AtomicBool>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    let target = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    let upstream = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(6379)
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut rewritten = target;
    rewritten.set_host(Some("127.0.0.1")).unwrap();
    rewritten.set_port(Some(address.port())).unwrap();
    let (hold, receiver) = watch::channel(false);
    let pause_after_reply = Arc::new(AtomicBool::new(false));
    let gate = pause_after_reply.clone();
    let reply_hold = hold.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                            accepted=listener.accept()=>{let(client,_)=accepted.unwrap();let upstream=upstream.clone();let mut hold=receiver.clone();let gate=gate.clone();let reply_hold=reply_hold.clone();connections.spawn(async move{
                                use tokio::io::AsyncReadExt;let server=TcpStream::connect(upstream).await.unwrap();let(mut client_read,mut client_write)=client.into_split();let(mut server_read,mut server_write)=server.into_split();
                                let send=tokio::io::copy(&mut client_read,&mut server_write);let receive=async move{let mut buffer=[0u8;16384];loop{let count=server_read.read(&mut buffer).await?;if count==0{return Ok::<(),std::io::Error>(());}
            if hold.wait_for(|value|!*value).await.is_err(){return Ok(());}client_write.write_all(&buffer[..count]).await?;if gate.swap(false,Ordering::SeqCst){reply_hold.send_replace(true);}}};let _=tokio::try_join!(async{send.await.map(|_|())},receive);
                            });},_=connections.join_next(),if !connections.is_empty()=>{}
                        }
        }
    });
    Proxy {
        url: rewritten.to_string(),
        hold,
        task,
        pause_after_reply,
    }
}
fn fixture_user(sid: &str) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sid.hash(&mut hasher);
    (hasher.finish() % 1_000_000_000_000) as i64 + 1000
}
fn request(sid: &str, profile: i64) -> ArchivePoolRequest {
    ArchivePoolRequest {
        session_id: sid.into(),
        channel_id: 123,
        virtual_channel_id: format!("123_fixture_{sid}"),
        provider_profile_id: profile,
        provider_max_streams: 1,
        stream_limit: 0,
        limit_policy: Default::default(),
        pool_fields: BTreeMap::from([
            ("user_id".into(), fixture_user(sid).to_string()),
            ("media_id".into(), "123_fixture".into()),
        ]),
        client_fields: BTreeMap::from([("user_id".into(), fixture_user(sid).to_string())]),
        channel_fields: BTreeMap::from([("channel_id".into(), "123".into())]),
    }
}
async fn wait(mut predicate: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("archive delayed-reply deadline");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable REDIS_TEST_URL and loopback TCP"]
async fn delayed_mutations_hold_reservation_and_heartbeat_cancellation_cannot_double_count_bytes() {
    let target = std::env::var("REDIS_TEST_URL").unwrap();
    let proxy = proxy().await;
    let mut redis = redis::Client::open(target.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let coordinator = Coordinator::connect(
        &target,
        LeaseConfig {
            namespace: format!("test:archive-delay:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let config = ArchivePoolConfig {
        max_sessions: 1,
        max_redis_operations: 1,
        max_queued_redis_operations: 4,
        command_timeout: Duration::from_millis(150),
        startup_idle: Duration::from_millis(500),
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let pool = ArchivePool::connect(&proxy.url, coordinator, config)
        .await
        .unwrap();
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as i64 + 1000;
    let profile_key = format!("profile_connections:{profile}");
    // Warm every mutation script before holding replies, so observing hashes
    // proves actual CREATE/START execution rather than a held NOSCRIPT response.
    let warm = pool
        .open(request(&uuid::Uuid::new_v4().to_string(), profile))
        .await
        .unwrap();
    warm.start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    warm.heartbeat().await.unwrap();
    warm.abort().await.unwrap();
    let admitted_sid = uuid::Uuid::new_v4().to_string();
    let admitted_key = format!("timeshift:channel:123_{admitted_sid}:clients:{admitted_sid}");
    proxy.hold.send_replace(true);
    let cloned = pool.clone();
    let req = request(&admitted_sid, profile);
    let call = tokio::spawn(async move { cloned.open(req).await });
    wait(async || redis.exists::<_, bool>(&admitted_key).await.unwrap()).await;
    assert!(call.await.unwrap().is_err());
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 0);
    assert!(!redis
        .exists::<_, bool>(format!("timeshift:pool:{admitted_sid}"))
        .await
        .unwrap());
    proxy.hold.send_replace(false);
    wait(async || pool.is_idle()).await;
    assert!(!redis.exists::<_, bool>(&admitted_key).await.unwrap());
    let sid = uuid::Uuid::new_v4().to_string();
    let key = format!("timeshift:pool:{sid}");
    proxy.pause_after_reply.store(true, Ordering::SeqCst);
    let cloned = pool.clone();
    let req = request(&sid, profile);
    let call = tokio::spawn(async move { cloned.open(req).await });
    wait(async || redis.exists::<_, bool>(&key).await.unwrap()).await;
    assert!(call.await.unwrap().is_err());
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    assert!(pool
        .open(request(&uuid::Uuid::new_v4().to_string(), profile))
        .await
        .is_err());
    proxy.hold.send_replace(false);
    wait(async || {
        !redis.exists::<_, bool>(&key).await.unwrap()
            && redis.get::<_, i64>(&profile_key).await.unwrap() == 0
    })
    .await;
    wait(async || pool.is_idle()).await;
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    let client_key = format!("timeshift:channel:123_{sid}:clients:{sid}");
    proxy.hold.send_replace(true);
    let call = tokio::spawn(async move {
        let result = ticket
            .start("http://fixture.invalid/archive.ts", None, None)
            .await;
        drop(ticket);
        result
    });
    wait(async || redis.exists::<_, bool>(&client_key).await.unwrap()).await;
    assert!(call.await.unwrap().is_err());
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    assert!(redis.exists::<_, bool>(&key).await.unwrap());
    proxy.hold.send_replace(false);
    wait(async || {
        !redis.exists::<_, bool>(&key).await.unwrap()
            && !redis.exists::<_, bool>(&client_key).await.unwrap()
            && redis.get::<_, i64>(&profile_key).await.unwrap() == 0
    })
    .await;
    wait(async || pool.is_idle()).await;
    let sid = uuid::Uuid::new_v4().to_string();
    let key = format!("timeshift:pool:{sid}");
    let metadata = format!("timeshift:channel:123_{sid}:metadata");
    let ticket = pool.open(request(&sid, profile)).await.unwrap();
    ticket
        .start("http://fixture.invalid/archive.ts", None, None)
        .await
        .unwrap();
    ticket.forwarded(100);
    proxy.hold.send_replace(true);
    let call = tokio::spawn(async move {
        let result = ticket.heartbeat().await;
        drop(ticket);
        result
    });
    wait(async || redis.hget::<_, _, u64>(&metadata, "total_bytes").await.ok() == Some(100)).await;
    assert!(call.await.unwrap().is_err());
    assert_eq!(redis.get::<_, i64>(&profile_key).await.unwrap(), 1);
    proxy.hold.send_replace(false);
    wait(async || {
        redis
            .hget::<_, _, String>(&key, "busy")
            .await
            .ok()
            .as_deref()
            == Some("0")
    })
    .await;
    assert_eq!(
        redis
            .hget::<_, _, u64>(&metadata, "total_bytes")
            .await
            .unwrap(),
        100
    );
    wait(async || pool.is_idle()).await;
    assert!(pool.shutdown().await);
    let _: () = redis.del(profile_key).await.unwrap();
}
