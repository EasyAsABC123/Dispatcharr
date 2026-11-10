//! Actual per-run exhaustion/report/release ownership controls, fixture only.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    cache_policy::CachedSwapPolicy,
    channels::{ChannelConfig, ChannelImporter},
    file_job::{
        ChildTask, ExhaustionReason, ExhaustionRelease, ExhaustionReporting, FileJobConfig,
        FileJobOutcome, FileJobReport, FileJobRequest, FileJobRunner, FileJobServices,
        GuideTaskDispatcher,
    },
    reporter::GuideReporter,
    EpgImporter, ImportConfig, ImportError, SwapPolicyContext,
};
use dispatcharr_store::{Store, StoreError};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    io::Read,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
    task::{JoinHandle, JoinSet},
};
struct Gate {
    entered: AtomicBool,
    open: Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicBool::new(false),
            open: Semaphore::new(0),
        })
    }
}
#[derive(Default)]
struct Sink {
    gates: Mutex<HashMap<i64, Arc<Gate>>>,
}
impl Sink {
    fn arm(&self, id: i64) -> Arc<Gate> {
        let gate = Gate::new();
        self.gates.lock().unwrap().insert(id, gate.clone());
        gate
    }
}
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        _: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            let id = message["data"]["source"].as_i64().unwrap();
            let gate = self.gates.lock().unwrap().remove(&id);
            if let Some(gate) = gate {
                gate.entered.store(true, Ordering::Release);
                gate.open.acquire().await.unwrap().forget();
            }
            Ok(())
        })
    }
}
struct Dispatcher(AtomicUsize);
impl GuideTaskDispatcher for Dispatcher {
    fn dispatch<'a>(
        &'a self,
        _: ChildTask,
        _: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ImportError::Worker)
        })
    }
}
struct Witness(Arc<AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Bundle {
    runner: FileJobRunner,
    reporter: GuideReporter,
    channels: ChannelImporter,
    programmes: EpgImporter,
    locks: TaskLocks,
    sink: Arc<Sink>,
    dispatcher: Arc<Dispatcher>,
}
impl Bundle {
    async fn new(store: &Store, cache: &str, locks_url: &str, max: usize) -> Self {
        let clock = Arc::new(chrono::Utc::now);
        let sink = Arc::new(Sink::default());
        let reporter =
            GuideReporter::with_clock(store.clone(), cache, sink.clone(), max, clock.clone())
                .await
                .unwrap();
        let channels = ChannelImporter::new(store.clone(), ChannelConfig::default()).unwrap();
        let programmes = EpgImporter::new(store.clone(), ImportConfig::default()).unwrap();
        let locks = TaskLocks::connect(locks_url, TaskLockConfig::default())
            .await
            .unwrap();
        let dispatcher = Arc::new(Dispatcher(AtomicUsize::new(0)));
        let runner = FileJobRunner::connect(
            FileJobServices {
                store: store.clone(),
                locks: locks.clone(),
                reporter: reporter.clone(),
                channels: channels.clone(),
                programmes: programmes.clone(),
                policy: Arc::new(CachedSwapPolicy::with_clock(
                    store.clone(),
                    Some(cache.to_owned()),
                    clock.clone(),
                )),
                dispatcher: dispatcher.clone(),
                clock,
            },
            FileJobConfig {
                max_jobs: max,
                cache_dir: "/evidence/exhaustion-cache".into(),
                release_timeout: Duration::from_secs(5),
            },
            cache,
        )
        .await
        .unwrap();
        Self {
            runner,
            reporter,
            channels,
            programmes,
            locks,
            sink,
            dispatcher,
        }
    }
    async fn close(&self) {
        assert!(self.runner.shutdown(Duration::from_secs(3)).await);
        assert!(self.reporter.shutdown(Duration::from_secs(3)).await);
        assert!(self.channels.shutdown(Duration::from_secs(3)).await);
        assert!(self.programmes.shutdown(Duration::from_secs(3)).await);
        assert_eq!(self.runner.physical_jobs(), 0);
        assert_eq!(self.locks.available_leases(), 4);
        assert_eq!(self.dispatcher.0.load(Ordering::SeqCst), 0);
    }
    fn run(
        &self,
        id: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> JoinHandle<Result<FileJobReport, ImportError>> {
        let runner = self.runner.clone();
        let dispatcher = self.dispatcher.clone();
        tokio::spawn(async move {
            runner
                .run_report_with_dispatcher(request(id), caller, dispatcher)
                .await
        })
    }
}
fn request(id: i64) -> FileJobRequest {
    FileJobRequest {
        source_id: id,
        force: true,
        defer_retry: 480,
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}
async fn entered(gate: &Gate) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !gate.entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn redis(cache: &str) -> redis::aio::MultiplexedConnection {
    redis::Client::open(cache)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn prepare(
    store: &Store,
    connection: &mut redis::aio::MultiplexedConnection,
    file: i64,
    parser: i64,
) {
    for id in [file, parser] {
        sqlx::query("UPDATE epg_epgsource SET status='idle',last_message=NULL WHERE id=$1")
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE epg_epgsourceindex SET data=$2 WHERE source_id=$1")
            .bind(id)
            .bind(json!({"owned":"before"}))
            .execute(store.pool())
            .await
            .unwrap();
        let _: i64 = redis::cmd("DEL")
            .arg(format!("task_lock_refresh_epg_data_{id}"))
            .arg(format!("task_lock_epg_source_file_{id}"))
            .arg(format!("epg_source_tvg_parse_{id}"))
            .query_async(connection)
            .await
            .unwrap();
    }
    let _: () = redis::cmd("SET")
        .arg(format!("task_lock_epg_source_file_{file}"))
        .arg("owned-busy")
        .arg("EX")
        .arg(300)
        .query_async(connection)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(format!("epg_source_tvg_parse_{parser}"))
        .arg(2)
        .arg("EX")
        .arg(300)
        .query_async(connection)
        .await
        .unwrap();
}
async fn refresh(connection: &mut redis::aio::MultiplexedConnection, id: i64) -> Option<String> {
    redis::cmd("GET")
        .arg(format!("task_lock_refresh_epg_data_{id}"))
        .query_async(connection)
        .await
        .unwrap()
}
fn reason(report: &FileJobReport, expected: ExhaustionReason) {
    assert_eq!(report.outcome, FileJobOutcome::Failed);
    let settled = report.settled_exhaustion.as_ref().unwrap();
    assert_eq!(settled.reason, expected);
    assert_eq!(settled.reporting, ExhaustionReporting::StatusSaved);
}
struct Proxy {
    url: String,
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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (hold, receiver) = watch::channel(false);
    let (blocked, observed) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
            accepted=listener.accept(),if tasks.len()<4=>{let(client,_)=accepted.unwrap();let mut hold=receiver.clone();let blocked=blocked.clone();tasks.spawn(async move{
                let server=TcpStream::connect("127.0.0.1:6379").await.unwrap();let(mut cr,mut cw)=client.into_split();let(mut sr,mut sw)=server.into_split();
                let incoming=async move{let mut buffer=[0u8;16384];loop{let count=sr.read(&mut buffer).await?;if count==0{return Ok::<(),std::io::Error>(())}
                                if *hold.borrow(){blocked.send_replace(true);}
                                if hold.wait_for(|paused|!*paused).await.is_err(){return Ok(())}cw.write_all(&buffer[..count]).await?;}};
                let _=tokio::try_join!(async{tokio::io::copy(&mut cr,&mut sw).await.map(|_|())},incoming);
            });},_=tasks.join_next(),if !tasks.is_empty()=>{}}
        }
    });
    Proxy {
        url: format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        hold,
        blocked: observed,
        task,
    }
}
#[tokio::test]
#[ignore = "requires copied Source exhaustion fixture PostgreSQL/Redis and owned source IDs"]
async fn native_file_job_exhaustion_resources() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
    );
    let mut raw = Vec::new();
    std::io::stdin().take(4097).read_to_end(&mut raw).unwrap();
    assert!(raw.len() <= 4096);
    let input: Value = serde_json::from_slice(&raw).unwrap();
    let file = input["file_id"].as_i64().unwrap();
    let parser = input["parser_id"].as_i64().unwrap();
    assert!(file > 0 && parser > 0 && file != parser);
    let url = format!("postgres://dispatch:playback-fixture-only@127.0.0.1:5432/{database}");
    let store = Store::connect(&url, 2).await.unwrap();
    for id in [file, parser] {
        let name: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert!(
            name.starts_with("rust-refresh-exhausted_")
                || name.starts_with("rust-native-refresh-exhausted_")
        );
    }
    let cache = "redis://:playback-fixture-only@127.0.0.1:6379/0";
    let mut connection = redis(cache).await;
    // Concurrent independent reasons, with one physical reporter held. Old run()
    // retains Failed, while the additive detailed API has truthful narrow data.
    prepare(&store, &mut connection, file, parser).await;
    let bundle = Bundle::new(&store, cache, cache, 2).await;
    let gate = bundle.sink.arm(file);
    let file_call = bundle.run(file, Arc::new(()));
    entered(&gate).await;
    let parser_report = bundle.run(parser, Arc::new(())).await.unwrap().unwrap();
    reason(&parser_report, ExhaustionReason::ParserBusy);
    assert!(!file_call.is_finished());
    assert!(refresh(&mut connection, file).await.is_some());
    gate.open.add_permits(1);
    let file_report = file_call.await.unwrap().unwrap();
    reason(&file_report, ExhaustionReason::FileBusy);
    assert_eq!(
        bundle
            .runner
            .run(request(file), Arc::new(()))
            .await
            .unwrap(),
        FileJobOutcome::Failed
    );
    let _: () = redis::cmd("SET")
        .arg(format!("epg_source_tvg_parse_{parser}"))
        .arg("not-an-integer")
        .query_async(&mut connection)
        .await
        .unwrap();
    let coarse = bundle.run(parser, Arc::new(())).await.unwrap().unwrap();
    assert_eq!(coarse.outcome, FileJobOutcome::Failed);
    assert!(coarse.settled_exhaustion.is_none());
    bundle.close().await;
    // Actual physical reporter pending after logical waiter abort retains the
    // guard/slot/refresh lease. Its return is unobserved, not synthesized.
    prepare(&store, &mut connection, file, parser).await;
    let bundle = Bundle::new(&store, cache, cache, 1).await;
    let gate = bundle.sink.arm(file);
    let drops = Arc::new(AtomicUsize::new(0));
    let call = bundle.run(file, Arc::new(Witness(drops.clone())));
    entered(&gate).await;
    call.abort();
    let _ = call.await;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(bundle.runner.physical_jobs(), 1);
    assert_eq!(bundle.reporter.physical_operations(), 1);
    assert!(refresh(&mut connection, file).await.is_some());
    assert!(!bundle.runner.shutdown(Duration::from_millis(20)).await);
    gate.open.add_permits(1);
    bundle.close().await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(refresh(&mut connection, file).await.is_none());
    // Only TaskLocks use this proxy: all reporter SQL/cache/audit work uses the
    // real endpoint. Warm RELEASE prevents NOSCRIPT being the held response.
    prepare(&store, &mut connection, file, parser).await;
    let mut proxy = proxy().await;
    let bundle = Bundle::new(&store, cache, &proxy.url, 1).await;
    bundle
        .locks
        .acquire("rust_exhaustion_warm", file)
        .await
        .unwrap()
        .release_owned()
        .await
        .unwrap();
    let gate = bundle.sink.arm(file);
    let drops = Arc::new(AtomicUsize::new(0));
    let call = bundle.run(file, Arc::new(Witness(drops.clone())));
    entered(&gate).await;
    proxy.hold.send_replace(true);
    gate.open.add_permits(1);
    tokio::time::timeout(
        Duration::from_secs(3),
        proxy.blocked.wait_for(|blocked| *blocked),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        refresh(&mut connection, file).await.is_none(),
        "server performed actual owner DEL before reply hold"
    );
    assert!(!call.is_finished());
    assert_eq!(bundle.runner.physical_jobs(), 1);
    assert_eq!(bundle.locks.available_leases(), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    call.abort();
    let _ = call.await;
    assert!(!bundle.runner.shutdown(Duration::from_millis(20)).await);
    proxy.hold.send_replace(false);
    bundle.close().await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    // Replacement fencing is not successful Released. The detailed typed
    // report records actual NotOwner and preserves the replacement key.
    prepare(&store, &mut connection, file, parser).await;
    let bundle = Bundle::new(&store, cache, cache, 1).await;
    let gate = bundle.sink.arm(file);
    let call = bundle.run(file, Arc::new(()));
    entered(&gate).await;
    let _: () = redis::cmd("SET")
        .arg(format!("task_lock_refresh_epg_data_{file}"))
        .arg("owned-replacement")
        .arg("EX")
        .arg(300)
        .query_async(&mut connection)
        .await
        .unwrap();
    gate.open.add_permits(1);
    let detailed = call.await.unwrap().unwrap();
    assert_eq!(detailed.outcome, FileJobOutcome::Failed);
    assert_eq!(
        detailed.settled_exhaustion.as_ref().unwrap().release,
        Some(ExhaustionRelease::NotOwnerForeignPreserved)
    );
    bundle.close().await;
    assert_eq!(
        refresh(&mut connection, file).await.as_deref(),
        Some("owned-replacement")
    );
    // Legacy run callers retain the prior NotOwner error, independently from
    // the detailed API's known Source-normal return disposition.
    prepare(&store, &mut connection, file, parser).await;
    let bundle = Bundle::new(&store, cache, cache, 1).await;
    let gate = bundle.sink.arm(file);
    let runner = bundle.runner.clone();
    let call = tokio::spawn(async move { runner.run(request(file), Arc::new(())).await });
    entered(&gate).await;
    let _: () = redis::cmd("SET")
        .arg(format!("task_lock_refresh_epg_data_{file}"))
        .arg("owned-replacement")
        .arg("EX")
        .arg(300)
        .query_async(&mut connection)
        .await
        .unwrap();
    gate.open.add_permits(1);
    assert!(matches!(call.await.unwrap(), Err(ImportError::Worker)));
    bundle.close().await;
    // Distinct guarded SERVER errors are normal authority at this exact stage,
    // independent of exception class/text; diagnostics remain bounded data.
    prepare(&store, &mut connection, file, parser).await;
    let function = format!("rust_exhaust_unproved_{file}");
    sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned unproved status' USING ERRCODE='P0002'; END; $$")).execute(store.pool()).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {function}_trigger BEFORE UPDATE ON epg_epgsource FOR EACH ROW WHEN (OLD.id={file} AND NEW.status='error') EXECUTE FUNCTION {function}()")).execute(store.pool()).await.unwrap();
    let bundle = Bundle::new(&store, cache, cache, 1).await;
    let unknown = bundle.run(file, Arc::new(())).await.unwrap().unwrap();
    assert_eq!(unknown.outcome, FileJobOutcome::Failed);
    let known = unknown.settled_exhaustion.as_ref().unwrap();
    assert_eq!(known.reason, ExhaustionReason::FileBusy);
    match &known.reporting {
        ExhaustionReporting::CaughtGuardedStatusSql(diag) => assert_eq!(diag.sqlstate, "P0002"),
        _ => panic!("guarded physical SQL cause required"),
    };
    bundle.close().await;
    sqlx::query(&format!("DROP FUNCTION {function}() CASCADE"))
        .execute(store.pool())
        .await
        .unwrap();
    // Native stopped admission is not a caught Source server exception.
    prepare(&store, &mut connection, file, parser).await;
    let bundle = Bundle::new(&store, cache, cache, 1).await;
    assert!(bundle.reporter.shutdown(Duration::from_secs(1)).await);
    assert!(matches!(
        bundle.run(file, Arc::new(())).await.unwrap(),
        Err(ImportError::Stopping)
    ));
    bundle.close().await;
    // Closing an independent pool after actual reporting cannot be relabeled
    // as a caught SELECT error. This fails at native acquisition, before SQL.
    prepare(&store, &mut connection, file, parser).await;
    let terminal_store = Store::connect(&url, 2).await.unwrap();
    let bundle = Bundle::new(&terminal_store, cache, cache, 1).await;
    let gate = bundle.sink.arm(file);
    let call = bundle.run(file, Arc::new(()));
    entered(&gate).await;
    terminal_store.pool().close().await;
    gate.open.add_permits(1);
    assert!(matches!(
        call.await.unwrap(),
        Err(ImportError::Store(StoreError::Database(
            sqlx::Error::PoolClosed
        )))
    ));
    bundle.close().await;
    for id in [file, parser] {
        let _: i64 = redis::cmd("DEL")
            .arg(format!("task_lock_refresh_epg_data_{id}"))
            .arg(format!("task_lock_epg_source_file_{id}"))
            .arg(format!("epg_source_tvg_parse_{id}"))
            .query_async(&mut connection)
            .await
            .unwrap();
    }
    store.pool().close().await;
    println!(
        "NATIVE_EXHAUSTION_RESOURCE_JSON={}",
        json!({"concurrent_reason_isolation":true,"old_run_outcome_preserved":true,"coarse_failed_stays_unclassified":true,"held_report_caller_abort":true,"held_physical_release_reply_abort":true,"replacement_release_typed_notowner":true,"legacy_notowner_error_preserved":true,"guarded_server_SQL_observed":true,"reporter_stopping_no_typed_result":true,"terminal_pool_acquire_no_typed_result":true,"aborted_waiter_return_observed":false,"owned_jobs_drained":true})
    );
}
