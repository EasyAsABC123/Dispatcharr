//! Actual Redis + delayed-response TCP proxy cancellation contracts.
//! Original Dispatcharr authors retain application credit; ATTRIBUTION.md.
use dispatcharr_coordination::{AcquireRequest, Coordinator, Error, LeaseConfig};
use redis::AsyncCommands;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
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
    task: JoinHandle<()>,
    pause_after_reply: Arc<AtomicBool>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    let target =
        url::Url::parse(&std::env::var("REDIS_TEST_URL").expect("disposable Redis required"))
            .unwrap();
    assert_eq!(target.scheme(), "redis");
    let target_address = format!(
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
    let pause_gate = pause_after_reply.clone();
    let reply_hold = hold.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let target_address=target_address.clone();let mut hold=receiver.clone();let reply_hold=reply_hold.clone();let pause_gate=pause_gate.clone();
                    connections.spawn(async move{
                        let server=TcpStream::connect(target_address).await.unwrap();
                        let(mut client_read,mut client_write)=client.into_split();let(mut server_read,mut server_write)=server.into_split();
                        let outbound=tokio::io::copy(&mut client_read,&mut server_write);
                        let inbound=async move{
                            let mut buffer=[0u8;16384];
                            loop {
                                let count=server_read.read(&mut buffer).await?;
                                if count==0{return Ok::<(),std::io::Error>(());}
                                if hold.wait_for(|paused|!*paused).await.is_err(){return Ok(());}
                                client_write.write_all(&buffer[..count]).await?;
                                if pause_gate.swap(false,Ordering::SeqCst){reply_hold.send_replace(true);}
                            }
                        };
                        let _=tokio::try_join!(async{outbound.await.map(|_|())},inbound);
                    });
                },
                _=connections.join_next(),if !connections.is_empty()=>{},
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
fn request(profile: i64) -> AcquireRequest {
    AcquireRequest {
        channel_id: uuid::Uuid::new_v4().to_string(),
        provider_profile_id: Some(profile),
        provider_max_streams: 1,
        credential_pool: None,
    }
}
async fn wait(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("delayed Redis lifecycle deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn cancelled_and_timed_out_all_scopes_keep_budget_until_reply_and_fenced_cleanup() {
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for scope in ["live", "vod", "timeshift"] {
        for cancel in [false, true] {
            let proxy = proxy().await;
            let namespace = format!("test:coord:cancel:{}", uuid::Uuid::new_v4());
            let coordinator = Coordinator::connect(
                &proxy.url,
                LeaseConfig {
                    namespace: namespace.clone(),
                    ttl: Duration::from_secs(10),
                    heartbeat_interval: None,
                    reap_interval: None,
                    command_timeout: Duration::from_millis(150),
                    max_inflight_commands: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // Load both scripts before delaying replies, so the observed
            // owner proves ACQUIRE ran rather than merely SCRIPT LOAD.
            let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as i64 + 1000;
            let warm = coordinator.acquire(request(profile)).await.unwrap();
            warm.release().await.unwrap();
            let acquisition = request(profile);
            let owner = if scope == "live" {
                format!("live:channel:{}:owner", acquisition.channel_id)
            } else {
                format!(
                    "{namespace}:{scope}:session:{}:owner",
                    acquisition.channel_id
                )
            };
            proxy.hold.send_replace(true);
            let c = coordinator.clone();
            let r = acquisition.clone();
            let call = tokio::spawn(async move {
                match scope {
                    "vod" => c.acquire_vod(r).await,
                    "timeshift" => c.acquire_timeshift(r).await,
                    _ => c.acquire(r).await,
                }
            });
            wait(async || redis.exists::<_, bool>(&owner).await.unwrap()).await;
            assert!(!coordinator.is_idle());
            assert_eq!(
                redis
                    .get::<_, i64>(format!("profile_connections:{profile}"))
                    .await
                    .unwrap(),
                1
            );
            if cancel {
                call.abort();
                let error = match call.await {
                    Err(error) => error,
                    Ok(_) => panic!("canceled caller unexpectedly completed"),
                };
                assert!(error.is_cancelled());
            } else {
                assert!(matches!(call.await.unwrap(), Err(Error::RedisUnavailable)));
            }
            // Admission is still held while Redis's actual reply is pending.
            assert!(matches!(
                coordinator.acquire(request(profile)).await,
                Err(Error::RedisUnavailable)
            ));
            assert_eq!(
                redis
                    .get::<_, i64>(format!("profile_connections:{profile}"))
                    .await
                    .unwrap(),
                1
            );
            proxy.pause_after_reply.store(true, Ordering::SeqCst);
            proxy.hold.send_replace(false);
            wait(async || {
                !redis.exists::<_, bool>(&owner).await.unwrap()
                    && redis
                        .get::<_, i64>(format!("profile_connections:{profile}"))
                        .await
                        .unwrap()
                        == 0
            })
            .await;
            // ACQUIRE's reply is released, but the cleanup RELEASE reply is
            // delayed next. Admission must remain held through that reply too.
            let queued = request(profile);
            let queued_owner = format!("live:channel:{}:owner", queued.channel_id);
            assert!(matches!(
                coordinator.acquire(queued).await,
                Err(Error::RedisUnavailable)
            ));
            assert!(!redis.exists::<_, bool>(&queued_owner).await.unwrap());
            assert!(!coordinator.is_idle());
            proxy.hold.send_replace(false);
            wait(async || coordinator.is_idle()).await;
            let successor = coordinator.acquire(request(profile)).await.unwrap();
            successor.release().await.unwrap();
            assert_eq!(
                redis
                    .get::<_, i64>(format!("profile_connections:{profile}"))
                    .await
                    .unwrap(),
                0
            );
            let _: () = redis
                .del(&[
                    format!("profile_connections:{profile}"),
                    format!("{namespace}:fence"),
                    format!("{namespace}:leases"),
                ])
                .await
                .unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable actual Redis retained acquisition reply"]
async fn retained_vod_acquire_context_survives_observer_and_actual_cleanup_reply() {
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    for cancel in [false, true] {
        let proxy = proxy().await;
        let namespace = format!("retained-acquire:{}", uuid::Uuid::new_v4());
        let coordinator = Coordinator::connect(
            &proxy.url,
            LeaseConfig {
                namespace: namespace.clone(),
                ttl: Duration::from_secs(30),
                heartbeat_interval: None,
                reap_interval: None,
                command_timeout: Duration::from_millis(100),
                max_inflight_commands: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000;
        let warm = coordinator.acquire_vod(request(profile)).await.unwrap();
        warm.release().await.unwrap();
        warm.settlement().wait().await;
        let acquired = request(profile);
        let owner = format!("{namespace}:vod:session:{}:owner", acquired.channel_id);
        let context = Arc::new(());
        let weak = Arc::downgrade(&context);
        proxy.hold.send_replace(true);
        let c = coordinator.clone();
        let call = tokio::spawn(async move { c.acquire_vod_retained(acquired, context).await });
        wait(async || redis.exists::<_, bool>(&owner).await.unwrap()).await;
        if cancel {
            call.abort();
            assert!(call.await.is_err());
        } else {
            assert!(matches!(call.await.unwrap(), Err(Error::RedisUnavailable)));
        }
        assert!(weak.upgrade().is_some());
        assert!(!coordinator.is_idle());
        proxy.pause_after_reply.store(true, Ordering::SeqCst);
        proxy.hold.send_replace(false);
        wait(async || !redis.exists::<_, bool>(&owner).await.unwrap()).await;
        tokio::time::sleep(Duration::from_millis(2200)).await;
        assert!(
            weak.upgrade().is_some(),
            "caller scope must survive actual cleanup reply"
        );
        assert!(!coordinator.is_idle());
        assert_eq!(
            redis
                .get::<_, i64>(format!("profile_connections:{profile}"))
                .await
                .unwrap(),
            0
        );
        proxy.hold.send_replace(false);
        wait(async || weak.upgrade().is_none() && coordinator.is_idle()).await;
    }
}
