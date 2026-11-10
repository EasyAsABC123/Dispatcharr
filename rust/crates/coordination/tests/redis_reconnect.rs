//! Task-lock SET reply loss must not replay SET or release callers before cleanup.
//! Original Dispatcharr/redis-rs authors retain application/client credit.
use dispatcharr_coordination::{
    TaskLockCleanupOutcome, TaskLockConfig, TaskLockDetailedError, TaskLocks,
};
use redis::AsyncCommands;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
struct Proxy {
    _hold: watch::Sender<bool>,
    url: String,
    drop_reply: Arc<AtomicBool>,
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
        _hold: hold,
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        drop_reply: pause,
        task,
    }
}

struct Owner(Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires actual Redis and dropped SET response proxy"]
async fn uncertain_set_is_not_replayed_and_its_owned_cleanup_uses_recovered_connection() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _:String=redis::cmd("SCRIPT").arg("LOAD").arg("if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end").query_async(&mut redis).await.unwrap();
    let proxy = proxy().await;
    let locks = TaskLocks::connect(
        &proxy.url,
        TaskLockConfig {
            max_leases: 1,
            max_commands: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let task = format!("owned_reconnect_set_{}", uuid::Uuid::new_v4().simple());
    let key = format!("task_lock_{task}_1");
    let dropped = Arc::new(AtomicUsize::new(0));
    proxy.drop_reply.store(true, Ordering::SeqCst);
    let result = locks
        .acquire_retained_detailed(&task, 1, Arc::new(Owner(dropped.clone())))
        .await;
    match result {Err(TaskLockDetailedError::Acquisition{cause,cleanup:TaskLockCleanupOutcome::Released})=>assert_eq!(cause.kind,redis::ErrorKind::IoError),_=>panic!("uncertain SET must remain an error after real owner-checked cleanup, not replay to Held/success")}
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(locks.available_leases(), 1);
    assert!(!redis.exists::<_, bool>(&key).await.unwrap());
    let lease = locks
        .acquire_retained_detailed(&task, 2, Arc::new(Owner(dropped.clone())))
        .await
        .unwrap();
    assert!(lease.release_owned_detailed().await.unwrap());
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert_eq!(locks.available_leases(), 1);
}
