//! Explicitly ignored realRedis physical-owner tests; disposable DB8/9 only.
//! Derived from existing transparent reply-gate test pattern; original
//! Dispatcharr application authors retain full credit and AGPL licensing.
use dispatcharr_job_transport::{
    eta::Limits,
    index_protocol::{IndexMessage, IndexTask},
    index_terminal::{Failure, FailureKind, Terminal},
    index_worker::{
        ChildPublisher, Error as WorkerError, ExecuteFuture, IndexExecutor, IndexWorker,
        TerminalFuture,
    },
    scheduled_index::{Config as ScheduleConfig, ScheduledIndexWorker},
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
struct Controlled {
    hold: Option<watch::Sender<bool>>,
    raised: bool,
}
impl IndexExecutor for Controlled {
    fn execute<'a>(
        &'a self,
        _: IndexMessage,
        _: ChildPublisher,
        _: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async { Err(WorkerError::Execution) })
    }
    fn execute_terminal<'a>(
        &'a self,
        _: IndexMessage,
        _: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> TerminalFuture<'a> {
        Box::pin(async move {
            let _owner = owner;
            if let Some(hold) = &self.hold {
                hold.send_replace(true);
            }
            Ok(if self.raised {
                Terminal::Raised(
                    Failure::new(
                        FailureKind::Directory,
                        vec![serde_json::json!(21), serde_json::json!("Is a directory")],
                        "physical_probe",
                        "Controlled terminal resource probe",
                    )
                    .unwrap(),
                )
            } else {
                Terminal::NullSuccess
            })
        })
    }
}
async fn result_connection() -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(std::env::var("JOB_REDIS_TEST_URL").unwrap()).unwrap();
    let mut info = client.get_connection_info().clone();
    info.redis.db = 9;
    redis::Client::open(info)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
async fn publish(jobs: &Jobs, scheduled: bool) -> Uuid {
    let id = Uuid::new_v4();
    let task = if scheduled {
        IndexTask::Deferred {
            source_id: 1,
            retry: 1,
            root: id,
            parent: Uuid::new_v4(),
            eta: chrono::Utc::now() - chrono::Duration::seconds(1),
        }
    } else {
        IndexTask::Initial { source_id: 1 }
    };
    jobs.publish_index(&task, id, Arc::new(())).await.unwrap();
    id
}
async fn canceled_terminal(scheduled: bool) {
    let mut proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    // Warm the shared result script with an actual normal worker execution.
    publish(&jobs, false).await;
    let warm = IndexWorker::new(
        jobs.clone(),
        Arc::new(Controlled {
            hold: None,
            raised: false,
        }),
    );
    warm.run_once(Arc::new(())).await.unwrap();
    warm.shutdown(Duration::from_secs(1)).await.unwrap();
    let id = publish(&jobs, scheduled).await;
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    let executor = Arc::new(Controlled {
        hold: Some(proxy.hold.clone()),
        raised: true,
    });
    let immediate = IndexWorker::new(jobs.clone(), executor.clone());
    let scheduled_worker =
        ScheduledIndexWorker::new(jobs.clone(), executor, ScheduleConfig::default()).unwrap();
    let waiter = if scheduled {
        let receipt = jobs
            .reserve_index_eta(Arc::new(()), Limits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receipt.message().task_id, id);
        let job = scheduled_worker.start(&receipt, caller).unwrap();
        drop(receipt);
        tokio::spawn(async move {
            let _ = job.wait().await;
        })
    } else {
        let worker = immediate.clone();
        tokio::spawn(async move {
            let _ = worker.run_once(caller).await;
        })
    };
    tokio::time::timeout(Duration::from_secs(3), proxy.blocked.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
    let mut direct = result_connection().await;
    let raw: String = redis::cmd("GET")
        .arg(format!("celery-tasks:celery-task-meta-{id}"))
        .query_async(&mut direct)
        .await
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(metadata["status"], "FAILURE");
    assert_eq!(metadata["result"]["exc_type"], "IsADirectoryError");
    waiter.abort();
    let _ = waiter.await;
    // Let the logical command deadline expire; actual I/O still owns admission.
    tokio::time::sleep(Duration::from_millis(250)).await;
    still_held(&jobs, &weak).await;
    let drained = if scheduled {
        scheduled_worker.shutdown(Duration::from_millis(30)).await
    } else {
        immediate.shutdown(Duration::from_millis(30)).await
    };
    assert_eq!(drained, Err(WorkerError::DrainTimeout));
    assert_eq!(
        jobs.shutdown(Duration::from_millis(30)).await,
        Err(Error::DrainTimeout)
    );
    assert!(weak.upgrade().is_some());
    proxy.hold.send_replace(false);
    wait_free(&jobs, &weak).await;
    immediate.shutdown(Duration::from_secs(2)).await.unwrap();
    scheduled_worker
        .shutdown(Duration::from_secs(2))
        .await
        .unwrap();
    jobs.shutdown(Duration::from_secs(2)).await.unwrap();
}
#[tokio::test]
#[ignore = "Explicit owned copied DB and real Redis required"]
async fn immediate_failure_reply_retains_physical_owners_after_caller_abort() {
    canceled_terminal(false).await;
}
#[tokio::test]
#[ignore = "Explicit owned copied DB and real Redis required"]
async fn scheduled_failure_reply_retains_physical_owners_after_caller_abort() {
    canceled_terminal(true).await;
}
#[tokio::test]
#[ignore = "Explicit owned copied DB and real Redis required"]
async fn failed_backend_write_preserves_wrongtype_and_returns_transport_error() {
    let proxy = proxy().await;
    let jobs = jobs(&proxy).await;
    let id = publish(&jobs, false).await;
    let mut direct = result_connection().await;
    let key = format!("celery-tasks:celery-task-meta-{id}");
    let _: i64 = redis::cmd("RPUSH")
        .arg(&key)
        .arg("owned-wrongtype")
        .query_async(&mut direct)
        .await
        .unwrap();
    let worker = IndexWorker::new(
        jobs.clone(),
        Arc::new(Controlled {
            hold: None,
            raised: true,
        }),
    );
    let caller = Arc::new(Caller);
    let weak = Arc::downgrade(&caller);
    assert!(matches!(
        worker.run_once(caller).await,
        Err(WorkerError::Transport(_))
    ));
    wait_free(&jobs, &weak).await;
    let values: Vec<String> = redis::cmd("LRANGE")
        .arg(&key)
        .arg(0)
        .arg(-1)
        .query_async(&mut direct)
        .await
        .unwrap();
    assert_eq!(values, ["owned-wrongtype"]);
    worker.shutdown(Duration::from_secs(1)).await.unwrap();
    jobs.shutdown(Duration::from_secs(1)).await.unwrap();
}
