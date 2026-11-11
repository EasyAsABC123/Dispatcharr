//! Actual held admin SETEX reply ownership probe; child of session_restart_peer::admit_reply.
//! Original Dispatcharr and redis-rs authors retain application/client credit.
use super::{arguments, frame};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
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
struct ReadProxy {
    url: String,
    observed: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Drop for ReadProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn read_proxy(id: &str, operation: &'static str, hold: bool) -> ReadProxy {
    let (url, _) = super::super::config();
    assert_eq!(url, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let target = if operation == "EXISTS" {
        format!("vod_persistent_connection:{id}")
    } else {
        format!("vod_proxy:client:{id}:stop")
    };
    let observed = Arc::new(AtomicUsize::new(0));
    let seen = observed.clone();
    let (release, ready) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut jobs = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept(),if jobs.len()<4=>{
                    let(mut client,_)=accepted.unwrap();let target=target.clone();let seen=seen.clone();let mut ready=ready.clone();
                    jobs.spawn(async move {
                        let mut server=TcpStream::connect("127.0.0.1:6379").await?;
                        let(mut input,mut output)=(Vec::new(),Vec::new());
                        loop {
                            let command=frame(&mut client,&mut input).await?;
                            let args=arguments(&command).ok_or(std::io::ErrorKind::InvalidData)?;
                            let plain=args.first().is_some_and(|v|v==operation.as_bytes())&&args.get(1).is_some_and(|v|v==target.as_bytes());
                            let matches=plain;
                            server.write_all(&command).await?;let reply=frame(&mut server,&mut output).await?;
                            if matches && !reply.starts_with(b"-NOSCRIPT") && seen.fetch_add(1,Ordering::SeqCst)==0 {
                                if !hold { return Err::<(),std::io::Error>(std::io::ErrorKind::ConnectionReset.into()); }
                                if ready.wait_for(|value|*value).await.is_err(){return Ok(());}
                            }
                            client.write_all(&reply).await?;
                        }
                    });
                },
                _=jobs.join_next(),if !jobs.is_empty()=>{}
            }
        }
    });
    ReadProxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        observed,
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
#[ignore = "actual4s admin SETEX reply after caller abort must retain service/control ownership"]
async fn admin_stop_writer_retains_actual_reply_after_cancel() {
    use redis::AsyncCommands;
    let (base, mut control, _, _, id) = super::super::physical_fixture("admin-stop-held").await;
    let metadata = super::super::super::key(&id);
    let _: usize = control.hset(&metadata, "active_streams", 1).await.unwrap();
    let proxy = read_proxy(&id, "SETEX", true).await;
    let pool = super::super::super::SessionPool::connect(
        &proxy.url,
        base.inner.coordinator.clone(),
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    let body_credit = pool.inner.requests.clone().try_acquire_owned().unwrap();
    let weak = Arc::downgrade(&pool.inner);
    let worker = pool.clone();
    let stop_id = id.clone();
    let task = tokio::spawn(async move { worker.stop(&stop_id).await });
    wait(|| proxy.observed.load(Ordering::SeqCst) == 1).await;
    // The write was admitted despite every body request credit being occupied.
    drop(body_credit);
    assert_eq!(pool.inner.admin_controls.available_permits(), 0);
    assert!(
        pool.stop(&id).await.is_err(),
        "second control is bounded before any write"
    );
    pool.stop_admission();
    assert!(
        pool.stop(&id).await.is_err(),
        "shutdown closes new controls"
    );
    let stop_key = format!("vod_proxy:client:{id}:stop");
    let written: Option<String> = control.get(&stop_key).await.unwrap();
    assert_eq!(written.as_deref(), Some("true"));
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let drained_early = tokio::time::timeout(Duration::from_millis(50), pool.drain())
        .await
        .is_ok();
    drop(pool);
    let service_held = weak.upgrade().is_some();
    println!(
        "ADMIN_STOP_PROOF {}",
        serde_json::json!({"written":true,"held_reply":true,"drained_early":drained_early,"service_held":service_held,"command_count":proxy.observed.load(Ordering::SeqCst)})
    );
    tokio::time::sleep(Duration::from_millis(1800)).await;
    proxy.release.send_replace(true);
    wait(|| weak.upgrade().is_none()).await;
    let _: usize = control.del(&[metadata, stop_key]).await.unwrap();
    assert!(
        service_held && !drained_early,
        "cancelled admin observer must retain physical writer/service until actual reply"
    );
}

#[tokio::test]
#[ignore = "lost actual SETEX reply is never replayed"]
async fn admin_stop_writer_lost_set_reply_never_replays_write() {
    use redis::AsyncCommands;
    let (base, mut control, _, _, id) = super::super::physical_fixture("admin-stop-lost").await;
    let metadata = super::super::super::key(&id);
    let _: usize = control.hset(&metadata, "active_streams", 1).await.unwrap();
    let proxy = read_proxy(&id, "SETEX", false).await;
    let pool = super::super::super::SessionPool::connect(
        &proxy.url,
        base.inner.coordinator.clone(),
        1,
        1,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    assert!(pool.stop(&id).await.is_err());
    let key = format!("vod_proxy:client:{id}:stop");
    assert_eq!(control.get::<_, String>(&key).await.unwrap(), "true");
    assert_eq!(proxy.observed.load(Ordering::SeqCst), 1);
    assert_eq!(pool.inner.admin_controls.available_permits(), 1);
    pool.drain().await;
    let _: usize = control.del(&[metadata, key]).await.unwrap();
}
#[tokio::test]
#[ignore = "Source admin false/true response and60s TTL unchanged"]
async fn admin_stop_writer_source_order_value_and_ttl() {
    use redis::AsyncCommands;
    let (pool, mut control, _, _, id) = super::super::physical_fixture("admin-stop-wire").await;
    assert!(!pool.stop(&id).await.unwrap());
    let metadata = super::super::super::key(&id);
    let key = format!("vod_proxy:client:{id}:stop");
    assert!(!control.exists::<_, bool>(&key).await.unwrap());
    let _: usize = control.hset(&metadata, "active_streams", 1).await.unwrap();
    assert!(pool.stop(&id).await.unwrap());
    assert_eq!(control.get::<_, String>(&key).await.unwrap(), "true");
    let ttl: i64 = control.ttl(&key).await.unwrap();
    assert!((59..=60).contains(&ttl));
    assert_eq!(pool.inner.admin_controls.available_permits(), 1);
    let _: usize = control.del(&[metadata, key]).await.unwrap();
    pool.drain().await;
}
