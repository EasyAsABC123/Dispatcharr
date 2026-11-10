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

struct HeldCreate {
    entered: Arc<std::sync::atomic::AtomicBool>,
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}
impl dispatcharr_job_transport::worker::CreateHandler for HeldCreate {
    fn create(&self) -> Result<dispatcharr_job_transport::worker::Completed, String> {
        self.entered
            .store(true, std::sync::atomic::Ordering::Release);
        let (lock, ready) = self.gate.as_ref();
        let guard = lock.lock().unwrap();
        drop(ready.wait_while(guard, |open| !*open).unwrap());
        Ok(dispatcharr_job_transport::worker::Completed {
            filename: "fixture-physical-only.zip".into(),
            size: 1,
        })
    }
}
fn held_create(open: bool) -> Arc<HeldCreate> {
    Arc::new(HeldCreate {
        entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        gate: Arc::new((std::sync::Mutex::new(open), std::sync::Condvar::new())),
    })
}
fn release_create(handler: &HeldCreate) {
    *handler.gate.0.lock().unwrap() = true;
    handler.gate.1.notify_all();
}
async fn entered_create(handler: &HeldCreate) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !handler.entered.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn direct_result(id: Uuid) -> Option<String> {
    let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let mut info = redis::Client::open(raw)
        .unwrap()
        .get_connection_info()
        .clone();
    info.redis.db = 9;
    let mut conn = redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut conn)
        .await
        .unwrap()
}
#[tokio::test]
#[ignore = "Requires owned copied DB and actual Redis; fixture Create physical result reply only."]
async fn worker_result_reply_timeout_retains_task_owner_admission_and_shutdown() {
    use dispatcharr_job_transport::worker::{Error as WorkerError, Worker};
    let proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    // Warm all three scripts, then hold the actual result reply rather than a
    // harmless SCRIPT LOAD/NOSCRIPT exchange or the early-ACK reply.
    let warm = Worker::new_fixture(jobs.clone(), held_create(true));
    jobs.publish(&Task::CreateBackup, Uuid::new_v4(), Arc::new(()))
        .await
        .unwrap();
    warm.run_once(Arc::new(())).await.unwrap();
    let handler = held_create(false);
    let worker = Worker::new_fixture(jobs.clone(), handler.clone());
    let id = Uuid::new_v4();
    jobs.publish(&Task::CreateBackup, id, Arc::new(()))
        .await
        .unwrap();
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let clone = worker.clone();
    let task = tokio::spawn(async move { clone.run_once(caller).await });
    entered_create(&handler).await;
    proxy.hold.send_replace(true);
    release_create(&handler);
    tokio::time::timeout(Duration::from_secs(3), async {
        while direct_result(id).await.is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        task.await.unwrap(),
        Err(WorkerError::Transport(Error::AmbiguousResult(id)))
    );
    assert!(weak.upgrade().is_some());
    assert_eq!(worker.available_slots(), 0);
    assert_eq!(
        worker.run_once(Arc::new(())).await,
        Err(WorkerError::Capacity)
    );
    assert_eq!(
        worker.shutdown(Duration::from_millis(30)).await,
        Err(WorkerError::DrainTimeout)
    );
    assert_eq!(
        worker.run_once(Arc::new(())).await,
        Err(WorkerError::Closed)
    );
    proxy.hold.send_replace(false);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() || worker.available_slots() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown(Duration::from_secs(2)).await.unwrap();
}
#[tokio::test]
#[ignore = "Requires owned copied DB and actual Redis; fixture Create blocking-handler lifecycle only."]
async fn worker_canceled_caller_keeps_handler_owner_until_result_and_warm_drain() {
    use dispatcharr_job_transport::worker::{Error as WorkerError, Worker};
    let proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    let handler = held_create(false);
    let worker = Worker::new_fixture(jobs.clone(), handler.clone());
    let id = Uuid::new_v4();
    jobs.publish(&Task::CreateBackup, id, Arc::new(()))
        .await
        .unwrap();
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let clone = worker.clone();
    let task = tokio::spawn(async move { clone.run_once(caller).await });
    entered_create(&handler).await;
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert_eq!(worker.available_slots(), 0);
    assert_eq!(
        worker.run_once(Arc::new(())).await,
        Err(WorkerError::Capacity)
    );
    assert_eq!(
        worker.shutdown(Duration::from_millis(30)).await,
        Err(WorkerError::DrainTimeout)
    );
    assert!(direct_result(id).await.is_none());
    release_create(&handler);
    worker.shutdown(Duration::from_secs(3)).await.unwrap();
    assert!(weak.upgrade().is_none());
    let meta: serde_json::Value = serde_json::from_str(&direct_result(id).await.unwrap()).unwrap();
    assert_eq!(meta["status"], "SUCCESS");
    assert_eq!(meta["result"]["status"], "completed");
}

async fn index_jobs(proxy: &Proxy) -> Jobs {
    Jobs::connect_index(
        &format!("{}/8", proxy.base),
        &format!("{}/9", proxy.base),
        Config {
            operations: 1,
            command_timeout: Duration::from_millis(200),
            max_queued_jobs: 100,
        },
    )
    .await
    .unwrap()
}
async fn newest_index(source_id: i64) -> bool {
    let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let mut info = redis::Client::open(raw)
        .unwrap()
        .get_connection_info()
        .clone();
    info.redis.db = 8;
    let mut connection = redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let raw: Option<Vec<u8>> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(0)
        .query_async(&mut connection)
        .await
        .unwrap();
    raw.is_some_and(|bytes| {
        dispatcharr_job_transport::index_protocol::parse(&bytes)
            .is_ok_and(|message| message.source_id == source_id)
    })
}
#[tokio::test]
#[ignore = "Requires owned Redis/copy fixture; actual public index dispatcher timeout and authority."]
async fn public_index_dispatch_timeout_retains_context_and_denies_backup_authority() {
    use dispatcharr_ingest_epg::{
        current_lookup::MissingIndexDispatcher, ImportError, SwapPolicyContext,
    };
    use dispatcharr_job_transport::{
        index_dispatch::ProgrammeIndexDispatcher, index_protocol::IndexTask,
    };
    let proxy = proxy().await;
    let jobs = index_jobs(&proxy).await;
    assert!(jobs.copied_database().is_none());
    for task in [
        Task::CreateBackup,
        Task::RestoreBackup {
            filename: "never-opened.zip".into(),
        },
    ] {
        assert_eq!(
            jobs.publish(&task, Uuid::new_v4(), Arc::new(())).await,
            Err(Error::UnsupportedTask)
        );
    }
    let dispatcher = ProgrammeIndexDispatcher::new(jobs.clone(), 1).unwrap();
    dispatcher.publish(88000, Arc::new(())).await.unwrap();
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(true));
    assert!(matches!(
        dispatcher
            .dispatch(
                88001,
                SwapPolicyContext {
                    cancelled,
                    retained: Arc::new(())
                }
            )
            .await,
        Err(ImportError::Cancelled)
    ));
    assert!(!newest_index(88001).await);
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let clone = dispatcher.clone();
    let task = tokio::spawn(async move {
        clone
            .dispatch(
                88002,
                SwapPolicyContext {
                    cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    retained: caller,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !newest_index(88002).await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(task.await.unwrap(), Err(ImportError::Worker)));
    assert!(weak.upgrade().is_some());
    assert_eq!(dispatcher.available_slots(), 0);
    assert_eq!(
        dispatcher.publish(88003, Arc::new(())).await,
        Err(Error::Capacity)
    );
    assert_eq!(
        dispatcher.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    proxy.hold.send_replace(false);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    dispatcher.shutdown(Duration::from_secs(2)).await.unwrap();
    // Closing only initial dispatch cannot prevent an accepted worker from
    // publishing its child/result through the shared physical Jobs transport.
    jobs.publish_index(
        &IndexTask::Initial { source_id: 88004 },
        Uuid::new_v4(),
        Arc::new(()),
    )
    .await
    .unwrap();
    jobs.shutdown(Duration::from_secs(2)).await.unwrap();
}
#[tokio::test]
#[ignore = "Requires owned Redis/copy fixture; actual aborted public dispatch and physical drain."]
async fn public_index_dispatch_aborted_caller_retains_physical_job_and_shutdown() {
    use dispatcharr_ingest_epg::{current_lookup::MissingIndexDispatcher, SwapPolicyContext};
    use dispatcharr_job_transport::index_dispatch::ProgrammeIndexDispatcher;
    let proxy = proxy().await;
    let jobs = index_jobs(&proxy).await;
    let dispatcher = ProgrammeIndexDispatcher::new(jobs.clone(), 1).unwrap();
    dispatcher.publish(88100, Arc::new(())).await.unwrap();
    proxy.hold.send_replace(true);
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let clone = dispatcher.clone();
    let task = tokio::spawn(async move {
        clone
            .dispatch(
                88101,
                SwapPolicyContext {
                    cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                    retained: caller,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !newest_index(88101).await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    assert!(weak.upgrade().is_some());
    assert_eq!(dispatcher.available_slots(), 0);
    assert_eq!(
        dispatcher.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    assert_eq!(
        dispatcher.publish(88102, Arc::new(())).await,
        Err(Error::Closed)
    );
    proxy.hold.send_replace(false);
    dispatcher.shutdown(Duration::from_secs(3)).await.unwrap();
    assert!(weak.upgrade().is_none());
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
}

// Post-publication lifecycle draft; execute after GRID quiet release. These
// fake handlers isolate shared transport ownership, not archive/index algorithms.
struct EmptyIndex;
impl dispatcharr_job_transport::index_worker::IndexExecutor for EmptyIndex {
    fn execute<'a>(
        &'a self,
        _message: dispatcharr_job_transport::index_protocol::IndexMessage,
        _publisher: dispatcharr_job_transport::index_worker::ChildPublisher,
        _owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_job_transport::index_worker::ExecuteFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
struct HeldIndex {
    entered: Arc<std::sync::atomic::AtomicBool>,
    release: Arc<std::sync::atomic::AtomicBool>,
}
impl dispatcharr_job_transport::index_worker::IndexExecutor for HeldIndex {
    fn execute<'a>(
        &'a self,
        message: dispatcharr_job_transport::index_protocol::IndexMessage,
        publisher: dispatcharr_job_transport::index_worker::ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_job_transport::index_worker::ExecuteFuture<'a> {
        Box::pin(async move {
            self.entered
                .store(true, std::sync::atomic::Ordering::Release);
            while !self.release.load(std::sync::atomic::Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            publisher
                .defer(message.source_id, message.retry + 1, 5, owner)
                .await?;
            Ok(())
        })
    }
}
#[tokio::test]
#[ignore = "Requires copied fixture/Redis; shared-family shutdown regression, fake handlers only."]
async fn families_idle_index_drain_cannot_close_active_create_result_transport() {
    use dispatcharr_job_transport::{index_worker::IndexWorker, worker::Worker};
    let proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    let handler = held_create(false);
    let create = Worker::new_fixture(jobs.clone(), handler.clone());
    let index = IndexWorker::new(jobs.clone(), Arc::new(EmptyIndex));
    let id = Uuid::new_v4();
    jobs.publish(&Task::CreateBackup, id, Arc::new(()))
        .await
        .unwrap();
    let clone = create.clone();
    let running = tokio::spawn(async move { clone.run_once(Arc::new(())).await });
    entered_create(&handler).await;
    index.shutdown(Duration::from_secs(1)).await.unwrap();
    release_create(&handler);
    running.await.unwrap().unwrap();
    create.shutdown(Duration::from_secs(1)).await.unwrap();
    let meta: serde_json::Value = serde_json::from_str(&direct_result(id).await.unwrap()).unwrap();
    assert_eq!(meta["status"], "SUCCESS");
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}
#[tokio::test]
#[ignore = "Requires copied fixture/Redis; shared-family shutdown with actual child/result writes, fake handler."]
async fn families_idle_create_drain_cannot_close_active_index_child_transport() {
    use dispatcharr_job_transport::{
        index_protocol::IndexTask, index_worker::IndexWorker, worker::Worker,
    };
    let proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    let handler = Arc::new(HeldIndex {
        entered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        release: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    let index = IndexWorker::new(jobs.clone(), handler.clone());
    let create = Worker::new_fixture(jobs.clone(), held_create(true));
    let id = Uuid::new_v4();
    jobs.publish_index(&IndexTask::Initial { source_id: 99001 }, id, Arc::new(()))
        .await
        .unwrap();
    let clone = index.clone();
    let running = tokio::spawn(async move { clone.run_once(Arc::new(())).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !handler.entered.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    create.shutdown(Duration::from_secs(1)).await.unwrap();
    handler
        .release
        .store(true, std::sync::atomic::Ordering::Release);
    running.await.unwrap().unwrap();
    index.shutdown(Duration::from_secs(1)).await.unwrap();
    let meta: serde_json::Value = serde_json::from_str(&direct_result(id).await.unwrap()).unwrap();
    assert_eq!(meta["status"], "SUCCESS");
    assert!(meta["result"].is_null());
    assert_eq!(meta["children"].as_array().unwrap().len(), 1);
    // Remove only this test's exact known child so the next family's FIFO head
    // is not an unsupported ETA. Never purge unrelated queue messages.
    let child = Uuid::parse_str(meta["children"][0][0][0].as_str().unwrap()).unwrap();
    let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let mut info = redis::Client::open(raw)
        .unwrap()
        .get_connection_info()
        .clone();
    info.redis.db = 8;
    let mut conn = redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let queued: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg("celery")
        .arg(0)
        .arg(15)
        .query_async(&mut conn)
        .await
        .unwrap();
    let owned = queued
        .into_iter()
        .find(|raw| {
            dispatcharr_job_transport::index_protocol::parse(raw)
                .is_ok_and(|message| message.task_id == child && message.source_id == 99001)
        })
        .expect("Only owned test child");
    let removed: i64 = redis::cmd("LREM")
        .arg("celery")
        .arg(1)
        .arg(owned)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(removed, 1);
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
#[ignore = "Requires owned Redis fixture; separate child validates constructor without fixture environment."]
async fn public_index_constructor_without_fixture_environment() {
    if std::env::var("INDEX_CONSTRUCTOR_ENV_CHILD").as_deref() == Ok("probe") {
        assert!(std::env::var_os("JOB_DISPOSABLE_FIXTURE").is_none());
        assert!(std::env::var_os("POSTGRES_DB").is_none());
        let raw = std::env::var("JOB_REDIS_TEST_URL").unwrap();
        let mut broker = redis::Client::open(raw)
            .unwrap()
            .get_connection_info()
            .clone();
        assert_eq!(
            broker.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(broker.addr,redis::ConnectionAddr::Tcp(ref host,_)if matches!(host.as_str(),"127.0.0.1"|"localhost"))
        );
        broker.redis.db = 8;
        let mut result = broker.clone();
        result.redis.db = 9;
        let url = |info: &redis::ConnectionInfo| match &info.addr {
            redis::ConnectionAddr::Tcp(host, port) => format!(
                "redis://:playback-fixture-only@{host}:{port}/{}",
                info.redis.db
            ),
            _ => unreachable!(),
        };
        let jobs = Jobs::connect_index(&url(&broker), &url(&result), Config::default())
            .await
            .unwrap();
        assert!(jobs.copied_database().is_none());
        assert_eq!(
            jobs.publish(
                &Task::RestoreBackup {
                    filename: "never-opened.zip".into()
                },
                Uuid::new_v4(),
                Arc::new(())
            )
            .await,
            Err(Error::UnsupportedTask)
        );
        jobs.shutdown(Duration::from_secs(2)).await.unwrap();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "public_index_constructor_without_fixture_environment",
            "--nocapture",
        ])
        .env("INDEX_CONSTRUCTOR_ENV_CHILD", "probe")
        .env_remove("JOB_DISPOSABLE_FIXTURE")
        .env_remove("POSTGRES_DB")
        .status()
        .unwrap();
    assert!(status.success());
}

#[tokio::test]
#[ignore = "Requires copied fixture/Redis; supervised reserve physical reconciliation."]
async fn index_driver_settled_reserve_waits_actual_reply_before_acceptance() {
    use dispatcharr_job_transport::{eta::Limits, index_protocol::IndexTask};
    let mut proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    assert!(matches!(
        dispatcharr_job_transport::index_driver::IndexDriver::start(
            jobs.clone(),
            Arc::new(EmptyIndex),
            Default::default(),
            Arc::new(())
        ),
        Err(dispatcharr_job_transport::index_worker::Error::Capacity)
    ));
    let id = Uuid::new_v4();
    jobs.publish_index(&IndexTask::Initial { source_id: 98101 }, id, Arc::new(()))
        .await
        .unwrap();
    let limits = Limits::default();
    let candidate = jobs
        .peek_index_candidate(Arc::new(()), limits)
        .await
        .unwrap()
        .unwrap();
    let owner = Arc::new(Caller);
    let weak = Arc::downgrade(&owner);
    proxy.hold.send_replace(true);
    let reserve = {
        let jobs = jobs.clone();
        tokio::spawn(async move {
            jobs.reserve_index_candidate_settled(candidate, owner, limits)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), proxy.blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        !reserve.is_finished(),
        "Logical timeout must not accept or discard unknown physical reservation"
    );
    assert!(weak.upgrade().is_some());
    assert_eq!(
        jobs.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    proxy.hold.send_replace(false);
    let receipt = tokio::time::timeout(Duration::from_secs(3), reserve)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(receipt.message().task_id, id);
    assert!(weak.upgrade().is_none());
    jobs.restore_index_eta(&receipt, Arc::new(()))
        .await
        .unwrap();
    drop(receipt);
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    // Remove only this exact restored synthetic message, leaving other heads.
    let base = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let mut raw = redis::Client::open(format!("{}/8", base.rsplit_once('/').unwrap().0))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let head: Vec<u8> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(&mut raw)
        .await
        .unwrap();
    let message: serde_json::Value = serde_json::from_slice(&head).unwrap();
    assert_eq!(message["headers"]["id"], id.to_string());
    let n: i64 = redis::cmd("LREM")
        .arg("celery")
        .arg(1)
        .arg(head)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(n, 1);
}

#[tokio::test]
#[ignore = "Requires copied fixture/Redis; supervised driver lifecycle, fake handler."]
async fn index_driver_intake_result_foreign_head_and_shared_close_order() {
    use dispatcharr_job_transport::{
        index_driver::{Config as DriverConfig, IndexDriver},
        index_protocol::IndexTask,
    };
    let proxy = proxy().await;
    let jobs = Jobs::connect_fixture(
        &format!("{}/8", proxy.base),
        &format!("{}/9", proxy.base),
        &std::env::var("POSTGRES_DB").unwrap(),
        Config {
            operations: 4,
            command_timeout: Duration::from_millis(200),
            max_queued_jobs: 100,
        },
    )
    .await
    .unwrap();
    let id = Uuid::new_v4();
    let owner = Arc::new(Caller);
    let weak = Arc::downgrade(&owner);
    jobs.publish_index(&IndexTask::Initial { source_id: 98102 }, id, Arc::new(()))
        .await
        .unwrap();
    let driver = IndexDriver::start(
        jobs.clone(),
        Arc::new(EmptyIndex),
        DriverConfig::default(),
        owner,
    )
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match jobs.read_index(id, Arc::new(())).await {
            Ok(Some(_)) => break,
            Ok(None) | Err(Error::Capacity) => {}
            value => panic!("Unexpected result: {value:?}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Execution error: {:?}, intake error: {:?}",
            driver.last_execution_error(),
            driver.last_intake_error()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(driver.last_execution_error(), None);
    // Foreign family is published only by fixture authority and is retained.
    let foreign = Uuid::new_v4();
    loop {
        match jobs
            .publish(&Task::CreateBackup, foreign, Arc::new(()))
            .await
        {
            Ok(_) => break,
            Err(Error::Capacity) => tokio::time::sleep(Duration::from_millis(10)).await,
            other => panic!("Unexpected foreign publication: {other:?}"),
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while driver.last_intake_error() != Some(Error::UnsupportedTask) {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    driver.shutdown(Duration::from_secs(3)).await.unwrap();
    assert!(weak.upgrade().is_none());
    assert!(jobs.read_index(id, Arc::new(())).await.unwrap().is_some());
    jobs.shutdown(Duration::from_secs(3)).await.unwrap();
    let base = std::env::var("JOB_REDIS_TEST_URL").unwrap();
    let mut raw = redis::Client::open(format!("{}/8", base.rsplit_once('/').unwrap().0))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let head: Vec<u8> = redis::cmd("LINDEX")
        .arg("celery")
        .arg(-1)
        .query_async(&mut raw)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&head).unwrap();
    assert_eq!(value["headers"]["id"], foreign.to_string());
    let n: i64 = redis::cmd("LREM")
        .arg("celery")
        .arg(1)
        .arg(head)
        .query_async(&mut raw)
        .await
        .unwrap();
    assert_eq!(n, 1);
}
