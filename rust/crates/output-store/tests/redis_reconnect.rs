//! Real Redis response loss, no replay, reconnect singleflight and ownership.
//! Original Dispatcharr/redis-rs authors retain application/client credit.
use dispatcharr_output_store::transport::{ClassifiedError, RedisTransport, TransportConfig};
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
struct Proxy {
    url: String,
    hold: watch::Sender<bool>,
    drop_reply: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
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
async fn proxy() -> Proxy {
    let info = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_connection_info()
        .clone();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    let address = match info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "127.0.0.1" | "localhost") =>
        {
            format!("{host}:{port}")
        }
        _ => panic!("owned loopback only"),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let pause = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicUsize::new(0));
    let connected = accepted.clone();
    let gate = pause.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let count = counter.clone();

    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {accepted=listener.accept(),if tasks.len()<4=>{let(client,_)=accepted.unwrap();connected.fetch_add(1,Ordering::SeqCst);let address=address.clone();let mut hold=receiver.clone();let gate=gate.clone();let count=count.clone();tasks.spawn(async move{let server=TcpStream::connect(address).await?;let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();let inbound=async move{let mut data=Vec::with_capacity(65536);let mut buf=[0u8;16384];loop{let n=sr.read(&mut buf).await?;if n==0{return Ok::<(),std::io::Error>(())}
                       if data.len()+n>65536{return Err(std::io::ErrorKind::InvalidData.into());}data.extend_from_slice(&buf[..n]);while let Some(end)=end_frame(&data,0,0)?{count.fetch_add(1,Ordering::SeqCst);if gate.swap(false,Ordering::SeqCst){return Err(std::io::ErrorKind::ConnectionReset.into());}
            if hold.wait_for(|value|!*value).await.is_err(){return Ok(())}cw.write_all(&data[..end]).await?;data.drain(..end);}}};tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},inbound).map(|_|())});},_=tasks.join_next(),if !tasks.is_empty()=>{}}
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        hold,
        drop_reply: pause,
        accepted,
        task,
    }
}

async fn wait(mut predicate: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("owned reconnect fixture deadline");
}
async fn raw() -> redis::aio::MultiplexedConnection {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
struct Owner(Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn config(operations: usize, timeout: Duration) -> TransportConfig {
    TransportConfig {
        max_operations: operations,
        max_queued_operations: 1,
        command_timeout: timeout,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires actual isolated Redis and dropped response proxy"]
async fn lost_publication_reply_is_not_replayed_and_future_commands_reconnect() {
    let mut redis = raw().await;
    let script = "return redis.call('LPUSH',KEYS[1],ARGV[1])";
    let _: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(script)
        .query_async(&mut redis)
        .await
        .unwrap();
    let proxy = proxy().await;
    let transport =
        RedisTransport::connect_classified(&proxy.url, config(1, Duration::from_millis(500)))
            .await
            .unwrap();
    let key = format!("owned:reconnect:publication:{}", uuid_suffix());
    let dropped = Arc::new(AtomicUsize::new(0));
    proxy.drop_reply.store(true, Ordering::SeqCst);
    let result = transport
        .command_classified::<i64, _>(
            script,
            std::slice::from_ref(&key),
            &[b"owned-fixture-message"],
            Arc::new(Owner(dropped.clone())),
        )
        .await;
    assert!(matches!(result, Err(ClassifiedError::Backend)));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        redis.lrange::<_, Vec<String>>(&key, 0, -1).await.unwrap(),
        vec!["owned-fixture-message"]
    );
    let mut recovered = false;
    for _ in 0..3 {
        if let Ok((count, _)) = transport
            .command_classified::<i64, _>(
                "return redis.call('LLEN',KEYS[1])",
                std::slice::from_ref(&key),
                &[],
                (),
            )
            .await
        {
            assert_eq!(count, 1);
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    assert!(recovered);
    assert_eq!(
        redis.llen::<_, i64>(&key).await.unwrap(),
        1,
        "no automatic repeat of uncertain LPUSH"
    );
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 2);
    let _: () = redis.del(key).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires actual Redis and held reconnect handshake"]
async fn concurrent_future_operations_share_one_bounded_reconnect_attempt() {
    let mut redis = raw().await;
    let proxy = proxy().await;
    let transport =
        RedisTransport::connect_classified(&proxy.url, config(16, Duration::from_secs(2)))
            .await
            .unwrap();
    proxy.hold.send_replace(true);
    proxy.drop_reply.store(true, Ordering::SeqCst);
    assert!(matches!(
        transport
            .command_classified::<String, _>("return 'owned-trigger'", &[], &[], ())
            .await,
        Err(ClassifiedError::Backend)
    ));
    wait(async || proxy.accepted.load(Ordering::SeqCst) == 2).await;
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let copy = transport.clone();
        let owner = Arc::new(Owner(dropped.clone()));
        tasks.push(tokio::spawn(async move {
            copy.command_classified::<i64, _>("return 7", &[], &[], owner)
                .await
        }));
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 2);
    proxy.hold.send_replace(false);
    for task in tasks {
        let (value, owner) = task.await.unwrap().unwrap();
        assert_eq!(value, 7);
        drop(owner);
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 16);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 2);
    let _: String = redis::cmd("PING").query_async(&mut redis).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires actual Redis held script reply"]
async fn logical_timeout_does_not_reconnect_or_free_a_still_pending_physical_reply() {
    let mut redis = raw().await;
    let script = "return redis.call('INCR',KEYS[1])";
    let _: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(script)
        .query_async(&mut redis)
        .await
        .unwrap();
    let proxy = proxy().await;
    let transport =
        RedisTransport::connect_classified(&proxy.url, config(1, Duration::from_millis(75)))
            .await
            .unwrap();
    let key = format!("owned:reconnect:held:{}", uuid_suffix());
    let dropped = Arc::new(AtomicUsize::new(0));
    proxy.hold.send_replace(true);
    let result = transport
        .command_classified::<i64, _>(
            script,
            std::slice::from_ref(&key),
            &[],
            Arc::new(Owner(dropped.clone())),
        )
        .await;
    assert!(matches!(result, Err(ClassifiedError::Backend)));
    assert_eq!(redis.get::<_, i64>(&key).await.unwrap(), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1);
    assert!(matches!(
        transport
            .command_classified::<i64, _>("return 0", &[], &[], ())
            .await,
        Err(ClassifiedError::Capacity)
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    proxy.hold.send_replace(false);
    wait(async || dropped.load(Ordering::SeqCst) == 1).await;
    let (value, _) = transport
        .command_classified::<i64, _>(
            "return redis.call('GET',KEYS[1])",
            std::slice::from_ref(&key),
            &[],
            (),
        )
        .await
        .unwrap();
    assert_eq!(value, 1);
    assert_eq!(proxy.accepted.load(Ordering::SeqCst), 1);
    let _: () = redis.del(key).await.unwrap();
}
fn uuid_suffix() -> String {
    format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
