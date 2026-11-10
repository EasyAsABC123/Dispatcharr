//! Actual Redis detailed-cause and physical caller-retention contracts.
//! Original Dispatcharr authors retain application/algorithm credit.
use dispatcharr_coordination::{
    TaskLockCleanupOutcome, TaskLockConfig, TaskLockDetailedError, TaskLockError, TaskLocks,
};
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
const RELEASE: &str =
    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end";
struct Proxy {
    url: String,
    hold: watch::Sender<bool>,
    pause_after_reply: Arc<AtomicBool>,
    replies: Arc<AtomicUsize>,
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
    let mut target = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    assert_eq!(target.scheme(), "redis");
    assert!(matches!(target.host_str(), Some("127.0.0.1" | "localhost")));
    let address = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(6379)
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    target.set_host(Some("127.0.0.1")).unwrap();
    target
        .set_port(Some(listener.local_addr().unwrap().port()))
        .unwrap();
    let (hold, receiver) = watch::channel(false);
    let pause = Arc::new(AtomicBool::new(false));
    let gate = pause.clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let count = counter.clone();
    let control = hold.clone();
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {accepted=listener.accept(),if tasks.len()<4=>{let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let gate=gate.clone();let control=control.clone();let count=count.clone();tasks.spawn(async move{let server=TcpStream::connect(address).await?;let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();let inbound=async move{let mut data=Vec::with_capacity(65536);let mut buf=[0u8;16384];loop{let n=sr.read(&mut buf).await?;if n==0{return Ok::<(),std::io::Error>(())}
            if data.len()+n>65536{return Err(std::io::ErrorKind::InvalidData.into());}data.extend_from_slice(&buf[..n]);while let Some(end)=end_frame(&data,0,0)?{count.fetch_add(1,Ordering::SeqCst);if hold.wait_for(|value|!*value).await.is_err(){return Ok(())}cw.write_all(&data[..end]).await?;data.drain(..end);if gate.swap(false,Ordering::SeqCst){control.send_replace(true);}}}};tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},inbound).map(|_|())});},_=tasks.join_next(),if !tasks.is_empty()=>{}}
        }
    });
    Proxy {
        url: target.to_string(),
        hold,
        pause_after_reply: pause,
        replies: counter,
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
    .expect("owned detailed physical deadline");
}
struct Owner(Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn config() -> TaskLockConfig {
    TaskLockConfig {
        max_leases: 1,
        max_commands: 1,
        ..Default::default()
    }
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned actual Redis and delayed reply proxy"]
async fn cancelled_acquisition_and_release_keep_caller_and_single_budget_through_actual_replies() {
    let mut redis = raw().await;
    let _: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(RELEASE)
        .query_async(&mut redis)
        .await
        .unwrap();
    let proxy = proxy().await;
    let locks = TaskLocks::connect(&proxy.url, config()).await.unwrap();
    let task = format!("detail_{}", uuid::Uuid::new_v4().simple());
    let key = format!("task_lock_{task}_1");
    let dropped = Arc::new(AtomicUsize::new(0));
    proxy.hold.send_replace(true);
    let copy = locks.clone();
    let name = task.clone();
    let caller = Arc::new(Owner(dropped.clone()));
    let pending =
        tokio::spawn(async move { copy.acquire_retained_detailed(&name, 1, caller).await });
    wait(async || redis.exists::<_, bool>(&key).await.unwrap()).await;
    pending.abort();
    let _ = pending.await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(locks.available_leases(), 0);
    proxy.pause_after_reply.store(true, Ordering::SeqCst);
    proxy.hold.send_replace(false);
    wait(async || !redis.exists::<_, bool>(&key).await.unwrap()).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(locks.available_leases(), 0);
    proxy.hold.send_replace(false);
    wait(async || locks.available_leases() == 1).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let lease = locks
        .acquire_retained_detailed(&task, 2, Arc::new(Owner(dropped.clone())))
        .await
        .unwrap();
    let key = format!("task_lock_{task}_2");
    proxy.hold.send_replace(true);
    let release = tokio::spawn(async move { lease.release_owned_detailed().await });
    wait(async || !redis.exists::<_, bool>(&key).await.unwrap()).await;
    release.abort();
    let _ = release.await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(locks.available_leases(), 0);
    proxy.hold.send_replace(false);
    wait(async || locks.available_leases() == 1).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires actual Redis ACL error and delayed cleanup response"]
async fn detailed_set_error_settles_cleanup_without_single_command_deadlock_and_preserves_legacy_mapping(
) {
    let mut redis = raw().await;
    let _: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(RELEASE)
        .query_async(&mut redis)
        .await
        .unwrap();
    let user = format!("owned_detail_{}", uuid::Uuid::new_v4().simple());
    let _: () = redis::cmd("ACL")
        .arg("SETUSER")
        .arg(&user)
        .arg("on")
        .arg(">playback-fixture-only")
        .arg("~*")
        .arg("+@all")
        .arg("-set")
        .query_async(&mut redis)
        .await
        .unwrap();
    let proxy = proxy().await;
    let mut url = url::Url::parse(&proxy.url).unwrap();
    url.set_username(&user).unwrap();
    url.set_password(Some("playback-fixture-only")).unwrap();
    let locks = TaskLocks::connect(url.as_str(), config()).await.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let before = proxy.replies.load(Ordering::SeqCst);
    proxy.hold.send_replace(true);
    let copy = locks.clone();
    let owner = Arc::new(Owner(dropped.clone()));
    let pending = tokio::spawn(async move {
        copy.acquire_retained_detailed("owned_set_error", 1, owner)
            .await
    });
    wait(async || proxy.replies.load(Ordering::SeqCst) > before).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(locks.available_leases(), 0);
    proxy.pause_after_reply.store(true, Ordering::SeqCst);
    proxy.hold.send_replace(false);
    wait(async || proxy.replies.load(Ordering::SeqCst) > before + 1).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(!pending.is_finished());
    proxy.hold.send_replace(false);
    match pending.await.unwrap() {
        Err(TaskLockDetailedError::Acquisition {
            cause,
            cleanup: TaskLockCleanupOutcome::NotOwner,
        }) => {
            assert!(cause.detail.as_deref().unwrap().contains("set"));
            assert!(!cause.detail_oversized);
        }
        _ => panic!("must preserve actual SET error plus settled no-owner cleanup"),
    };
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(locks.available_leases(), 1);
    assert!(matches!(
        locks.acquire("owned_set_error", 2).await,
        Err(TaskLockError::RedisUnavailable)
    ));
    wait(async || locks.available_leases() == 1).await;
    let _: i64 = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(user)
        .query_async(&mut redis)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires actual Redis owner replacement and Lua error"]
async fn detailed_release_keeps_server_cause_and_never_deletes_replacement() {
    let mut redis = raw().await;
    let locks = TaskLocks::connect(&std::env::var("REDIS_TEST_URL").unwrap(), config())
        .await
        .unwrap();
    let task = format!("detail_release_{}", uuid::Uuid::new_v4().simple());
    let dropped = Arc::new(AtomicUsize::new(0));
    let lease = locks
        .acquire_retained_detailed(&task, 1, Arc::new(Owner(dropped.clone())))
        .await
        .unwrap();
    let key = lease.key().to_owned();
    let _: () = redis.del(&key).await.unwrap();
    let _: i64 = redis.lpush(&key, "owned-replacement").await.unwrap();
    match lease.release_owned_detailed().await {
        Err(TaskLockDetailedError::Redis(cause)) => {
            assert_eq!(cause.code.as_deref(), Some("WRONGTYPE"));
            assert!(cause
                .detail
                .as_deref()
                .unwrap()
                .to_ascii_lowercase()
                .contains("wrong kind of value"));
        }
        _ => panic!("actual Lua wrong-type error required"),
    };
    assert_eq!(
        redis.lrange::<_, Vec<String>>(&key, 0, -1).await.unwrap(),
        vec!["owned-replacement"]
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let _: () = redis.del(key).await.unwrap();
    let lease = locks
        .acquire_retained_detailed(&task, 2, Arc::new(Owner(dropped.clone())))
        .await
        .unwrap();
    let key = lease.key().to_owned();
    let _: () = redis.set(&key, "other-owner").await.unwrap();
    assert!(!lease.release_owned_detailed().await.unwrap());
    assert_eq!(redis.get::<_, String>(&key).await.unwrap(), "other-owner");
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    let _: () = redis.del(key).await.unwrap();
}
