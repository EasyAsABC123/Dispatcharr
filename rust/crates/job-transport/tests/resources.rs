//! Explicitly ignored realRedis physical-owner tests; disposable DB8/9 only.
//! Derived from existing transparent reply-gate test pattern; original
//! Dispatcharr application authors retain full credit and AGPL licensing.
use dispatcharr_job_transport::{
    protocol::Task,
    transport::{Config, Error, Jobs},
};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use uuid::Uuid;
struct Proxy {
    base: String,
    hold: watch::Sender<bool>,
    blocked: watch::Receiver<bool>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn proxy() -> Proxy {
    let raw =
        std::env::var("JOB_REDIS_TEST_URL").expect("Explicit owned JOB_REDIS_TEST_URL required");
    let client = redis::Client::open(raw).unwrap();
    let info = client.get_connection_info();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    let address = match &info.addr {
        redis::ConnectionAddr::Tcp(host, port)
            if matches!(host.as_str(), "localhost" | "127.0.0.1") =>
        {
            format!("{host}:{port}")
        }
        _ => panic!("Owned loopback fixture required"),
    };
    let mut bound = None;
    for port in 61000..61128 {
        if let Ok(value) = TcpListener::bind(("127.0.0.1", port)).await {
            bound = Some(value);
            break;
        }
    }
    let listener = bound.expect("Proxy port bound");
    let port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let(client,_)=accepted.unwrap();let address=address.clone();let mut hold=receiver.clone();let blocked=blocked.clone();
                    connections.spawn(async move{
                        let server=TcpStream::connect(address).await.unwrap();
                        let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();
                        let incoming=async move{
                            let mut buffer=[0u8;16384];
                            loop{
                                let count=sr.read(&mut buffer).await?;
                                if count==0{return Ok::<(),std::io::Error>(());}
                                if *hold.borrow(){blocked.send_replace(true);}
                                if hold.wait_for(|paused|!*paused).await.is_err(){return Ok(());}
                                cw.write_all(&buffer[..count]).await?;
                            }
                        };
                        let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);
                    });
                },
                _=connections.join_next(),if !connections.is_empty()=>{},
            }
        }
    });
    Proxy {
        base: format!("redis://:playback-fixture-only@127.0.0.1:{port}"),
        hold,
        blocked: observed,
        task,
    }
}
async fn jobs(proxy: &Proxy) -> Jobs {
    Jobs::connect_fixture(
        &format!("{}/8", proxy.base),
        &format!("{}/9", proxy.base),
        &std::env::var("POSTGRES_DB").expect("Owned copied DB required"),
        Config {
            operations: 1,
            command_timeout: Duration::from_millis(200),
            max_queued_jobs: 100,
        },
    )
    .await
    .unwrap()
}
struct Caller;
async fn still_held(jobs: &Jobs, weak: &Weak<Caller>) {
    assert_eq!(jobs.available_slots(), 0);
    assert!(
        weak.upgrade().is_some(),
        "Caller was released before physical reply"
    );
    assert_eq!(
        jobs.read(Uuid::new_v4(), Arc::new(())).await,
        Err(Error::Capacity)
    );
}
async fn wait_free(jobs: &Jobs, weak: &Weak<Caller>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while jobs.available_slots() != 1 || weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
#[ignore = "Requires explicitly owned copied DB and real Redis; never silently skipped."]
async fn accepted_publication_timeout_retains_owner_admission_and_failed_drain() {
    let mut proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    // Warm script SHA before pausing; a NOSCRIPT handshake must not be confused
    // with an accepted LPUSH whose reply is held.
    jobs.publish(&Task::CreateBackup, Uuid::new_v4(), Arc::new(()))
        .await
        .unwrap();
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let id = Uuid::new_v4();
    let cloned = jobs.clone();
    let task = tokio::spawn(async move { cloned.publish(&Task::CreateBackup, id, caller).await });
    tokio::time::timeout(Duration::from_secs(3), proxy.blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    // Observe the actual server-side LPUSH through an independent connection;
    // a paused NOSCRIPT/auth response must never count as committed publication.
    let direct = redis::Client::open(std::env::var("JOB_REDIS_TEST_URL").unwrap()).unwrap();
    let mut info = direct.get_connection_info().clone();
    info.redis.db = 8;
    let mut connection = redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let queued: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg("celery")
        .arg(0)
        .arg(100)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(queued.iter().any(
        |raw| serde_json::from_slice::<serde_json::Value>(raw).unwrap()["headers"]["id"]
            == id.to_string()
    ));
    assert_eq!(task.await.unwrap(), Err(Error::AmbiguousPublication(id)));
    still_held(&jobs, &weak).await;
    assert_eq!(
        jobs.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    assert_eq!(
        jobs.read(Uuid::new_v4(), Arc::new(())).await,
        Err(Error::Closed)
    );
    assert!(weak.upgrade().is_some());
    proxy.hold.send_replace(false);
    wait_free(&jobs, &weak).await;
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}
#[tokio::test]
#[ignore = "Requires explicitly owned copied DB and real Redis; never silently skipped."]
async fn canceled_result_caller_keeps_physical_guard_until_reply_and_shutdown_waits() {
    let mut proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    jobs.read(Uuid::new_v4(), Arc::new(())).await.unwrap();
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let cloned = jobs.clone();
    let task = tokio::spawn(async move { cloned.read(Uuid::new_v4(), caller).await });
    tokio::time::timeout(Duration::from_secs(3), proxy.blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    task.abort();
    let _ = task.await;
    still_held(&jobs, &weak).await;
    let cloned = jobs.clone();
    let draining = tokio::spawn(async move { cloned.shutdown(Duration::from_secs(2)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!draining.is_finished());
    proxy.hold.send_replace(false);
    wait_free(&jobs, &weak).await;
    assert_eq!(draining.await.unwrap(), Ok(()));
}
