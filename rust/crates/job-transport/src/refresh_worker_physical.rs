// Actual pending Redis reply tests for the refresh helper only.
// Original Dispatcharr authors retain full application credit/AGPL.
use super::{
    ChildPublisher, Error, ExecuteFuture, NormalReturn, RefreshExecutor, RefreshMessage,
    RefreshWorker, Run,
};
use crate::{
    refresh_protocol::RefreshCall,
    transport::{Config, Error as TransportError, Jobs},
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
async fn jobs(proxy: &Proxy, timeout: Duration) -> Jobs {
    Jobs::connect_refresh(
        &format!("{}/8", proxy.base),
        &format!("{}/9", proxy.base),
        Config {
            operations: 1,
            command_timeout: timeout,
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
        Err(TransportError::Capacity)
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

fn packet(id: Uuid) -> RefreshMessage {
    crate::refresh_publish::message(id, RefreshCall::initial(1))
}
struct Held {
    entered: Arc<std::sync::atomic::AtomicBool>,
    open: watch::Sender<bool>,
}
impl Held {
    fn new(open: bool) -> Arc<Self> {
        let (tx, _) = watch::channel(open);
        Arc::new(Self {
            entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            open: tx,
        })
    }
}
impl RefreshExecutor for Held {
    fn execute<'a>(
        &'a self,
        _message: RefreshMessage,
        _publisher: ChildPublisher,
        _owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            self.entered
                .store(true, std::sync::atomic::Ordering::Release);
            let mut open = self.open.subscribe();
            open.wait_for(|v| *v).await.map_err(|_| Error::Execution)?;
            Ok(NormalReturn::Null)
        })
    }
}
async fn entered(h: &Held) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !h.entered.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap()
}
async fn direct(id: Uuid) -> Option<Vec<u8>> {
    let client = redis::Client::open(std::env::var("JOB_REDIS_TEST_URL").unwrap()).unwrap();
    let mut info = client.get_connection_info().clone();
    info.redis.db = 9;
    let mut c = redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut c)
        .await
        .unwrap()
}
async fn queue_len() -> i64 {
    let client = redis::Client::open(std::env::var("JOB_REDIS_TEST_URL").unwrap()).unwrap();
    let mut info = client.get_connection_info().clone();
    info.redis.db = 8;
    let mut c = redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let (unacked, list): (i64, i64) = redis::pipe()
        .cmd("HLEN")
        .arg("unacked")
        .cmd("LLEN")
        .arg("celery")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(unacked, 0);
    list
}
#[tokio::test]
#[ignore = "Copied ownedDB refresh normal result actual server reply timeout/drain"]
async fn result_reply_timeout_retains_worker_caller_and_both_admissions() {
    let proxy = proxy().await;
    let jobs = jobs(&proxy, Duration::from_millis(200)).await;
    let warm = RefreshWorker::new(jobs.clone(), Held::new(true)).unwrap();
    let warmid = Uuid::new_v4();
    jobs.publish_refresh(&packet(warmid), Arc::new(()))
        .await
        .unwrap();
    warm.run_once(Arc::new(())).await.unwrap();
    warm.shutdown(Duration::from_secs(1)).await.unwrap();
    let held = Held::new(false);
    let worker = RefreshWorker::new(jobs.clone(), held.clone()).unwrap();
    let id = Uuid::new_v4();
    jobs.publish_refresh(&packet(id), Arc::new(()))
        .await
        .unwrap();
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let copy = worker.clone();
    let task = tokio::spawn(async move { copy.run_once(caller).await });
    entered(&held).await;
    assert_eq!(queue_len().await, 0);
    assert!(direct(id).await.is_none());
    proxy.hold.send_replace(true);
    held.open.send_replace(true);
    tokio::time::timeout(Duration::from_secs(3), async {
        while direct(id).await.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        task.await.unwrap(),
        Err(Error::Transport(TransportError::AmbiguousResult(id)))
    );
    assert!(*proxy.blocked.borrow());
    assert_eq!(worker.available_slots(), 0);
    still_held(&jobs, &weak).await;
    assert_eq!(worker.run_once(Arc::new(())).await, Err(Error::Capacity));
    assert_eq!(
        worker.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    assert_eq!(
        jobs.shutdown(Duration::from_millis(30)).await,
        Err(TransportError::DrainTimeout)
    );
    proxy.hold.send_replace(false);
    wait_free(&jobs, &weak).await;
    worker.shutdown(Duration::from_secs(1)).await.unwrap();
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}
#[tokio::test]
#[ignore = "Copied ownedDB actual earlyACK/caller abort/owned worker completion"]
async fn aborted_waiter_keeps_earlyacked_execution_owned_until_result_and_drain() {
    let proxy = proxy().await;
    let jobs = jobs(&proxy, Duration::from_secs(2)).await;
    let held = Held::new(false);
    let worker = RefreshWorker::new(jobs.clone(), held.clone()).unwrap();
    let id = Uuid::new_v4();
    jobs.publish_refresh(&packet(id), Arc::new(()))
        .await
        .unwrap();
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let copy = worker.clone();
    let task = tokio::spawn(async move { copy.run_once(caller).await });
    entered(&held).await;
    assert_eq!(queue_len().await, 0);
    assert!(direct(id).await.is_none());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(weak.upgrade().is_some());
    assert_eq!(worker.available_slots(), 0);
    assert_eq!(worker.run_once(Arc::new(())).await, Err(Error::Capacity));
    assert_eq!(
        worker.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    held.open.send_replace(true);
    worker.shutdown(Duration::from_secs(2)).await.unwrap();
    assert!(weak.upgrade().is_none());
    let raw = direct(id).await.unwrap();
    assert!(crate::refresh_result::parse_success(&raw, id, 1).is_ok());
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}
#[tokio::test]
#[ignore = "Copied ownedDB idle family shutdown cannot close active shared jobs"]
async fn idle_refresh_worker_shutdown_preserves_active_shared_transport() {
    let proxy = proxy().await;
    let jobs = jobs(&proxy, Duration::from_secs(2)).await;
    let held = Held::new(false);
    let active = RefreshWorker::new(jobs.clone(), held.clone()).unwrap();
    let idle = RefreshWorker::new(jobs.clone(), Held::new(true)).unwrap();
    let id = Uuid::new_v4();
    jobs.publish_refresh(&packet(id), Arc::new(()))
        .await
        .unwrap();
    let copy = active.clone();
    let task = tokio::spawn(async move { copy.run_once(Arc::new(())).await });
    entered(&held).await;
    idle.shutdown(Duration::from_millis(30)).await.unwrap();
    held.open.send_replace(true);
    assert_eq!(task.await.unwrap(), Ok(Run::Completed(id)));
    assert!(direct(id).await.is_some());
    active.shutdown(Duration::from_secs(1)).await.unwrap();
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
#[ignore = "Owned copiedDB production refresh authority and unsupported FIFO retention"]
async fn production_refresh_driver_preserves_foreign_head_and_denies_backups() {
    use crate::refresh_driver::{RefreshDriver, State};
    let proxy = proxy().await;
    let jobs = jobs(&proxy, Duration::from_secs(2)).await;
    assert!(jobs.copied_database().is_none());
    assert_eq!(
        jobs.publish(
            &crate::protocol::Task::CreateBackup,
            Uuid::new_v4(),
            Arc::new(())
        )
        .await,
        Err(TransportError::UnsupportedTask)
    );
    let fixture = Jobs::connect_fixture(
        &format!("{}/8", proxy.base),
        &format!("{}/9", proxy.base),
        &std::env::var("POSTGRES_DB").unwrap(),
        Config {
            operations: 1,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 100,
        },
    )
    .await
    .unwrap();
    let id = Uuid::new_v4();
    fixture
        .publish(&crate::protocol::Task::CreateBackup, id, Arc::new(()))
        .await
        .unwrap();
    let held = Held::new(true);
    let worker = RefreshWorker::new(jobs.clone(), held.clone()).unwrap();
    let driver = RefreshDriver::start(worker, Duration::from_millis(25)).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while driver.state() != State::UnsupportedHead {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!held.entered.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(queue_len().await, 1);
    assert!(direct(id).await.is_none());
    driver.shutdown(Duration::from_secs(2)).await.unwrap();
    assert_eq!(driver.state(), State::Stopped);
    assert_eq!(queue_len().await, 1);
    // Only the explicit fixture Create consumer removes its owned sentinel.
    let claimed = fixture.claim_create(Arc::new(())).await.unwrap();
    assert_eq!(claimed, Some(id));
    jobs.shutdown(Duration::from_secs(2)).await.unwrap();
    fixture.shutdown(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
#[ignore = "Owned copiedDB production poller execution retention and bounded shutdown"]
async fn production_refresh_driver_shutdown_retains_active_execution() {
    use crate::refresh_driver::RefreshDriver;
    let proxy = proxy().await;
    let jobs = jobs(&proxy, Duration::from_secs(2)).await;
    let id = Uuid::new_v4();
    jobs.publish_refresh(&packet(id), Arc::new(()))
        .await
        .unwrap();
    let held = Held::new(false);
    let worker = RefreshWorker::new(jobs.clone(), held.clone()).unwrap();
    let driver = RefreshDriver::start(worker.clone(), Duration::from_millis(25)).unwrap();
    entered(&held).await;
    assert_eq!(queue_len().await, 0);
    assert_eq!(worker.available_slots(), 0);
    assert!(direct(id).await.is_none());
    assert_eq!(
        driver.shutdown(Duration::from_millis(25)).await,
        Err(Error::DrainTimeout)
    );
    assert_eq!(worker.available_slots(), 0);
    held.open.send_replace(true);
    driver.shutdown(Duration::from_secs(3)).await.unwrap();
    let value = jobs
        .read_refresh(id, 1, Arc::new(()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value.task_id, id);
    assert_eq!(worker.available_slots(), 1);
    jobs.shutdown(Duration::from_secs(2)).await.unwrap();
}
