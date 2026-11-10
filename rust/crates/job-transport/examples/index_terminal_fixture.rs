//! Controlled executor boundary probe; not an IndexService classifier.
//! Original Dispatcharr/Celery/Kombu authors retain application/framework credit.
use dispatcharr_job_transport::{
    eta::Limits,
    index_protocol::IndexMessage,
    index_terminal::{Failure, FailureKind, Terminal},
    index_worker::{
        ChildPublisher, Error, ExecuteFuture, IndexExecutor, IndexWorker, TerminalFuture,
    },
    scheduled_index::{Config as ScheduleConfig, ScheduledIndexWorker},
    transport::{Config, Jobs},
};
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use uuid::Uuid;

struct Controlled {
    value: Option<Terminal>,
    calls: Arc<AtomicUsize>,
}
impl IndexExecutor for Controlled {
    fn execute<'a>(
        &'a self,
        _: IndexMessage,
        _: ChildPublisher,
        _: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async { Err(Error::Execution) })
    }
    fn execute_terminal<'a>(
        &'a self,
        _: IndexMessage,
        _: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> TerminalFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let value = self.value.clone();
        Box::pin(async move {
            let _owner = owner;
            value.ok_or(Error::Capacity)
        })
    }
}

#[tokio::main]
async fn main() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").expect("Copied fixture DB");
    let mut raw = Vec::new();
    io::stdin().take(32769).read_to_end(&mut raw).unwrap();
    assert!(raw.len() <= 32768);
    let input: Value = serde_json::from_slice(&raw).unwrap();
    let text = |key: &str| input[key].as_str().expect("Fixture field");
    let expected = Uuid::parse_str(text("task_id")).unwrap();
    let jobs = Jobs::connect_fixture(
        text("broker_url"),
        text("result_url"),
        &database,
        Config::default(),
    )
    .await
    .unwrap();
    let terminal = match text("kind") {
        "normal" => Some(Terminal::NullSuccess),
        "operational" => None,
        kind => {
            let kind = match kind {
                "directory" => FailureKind::Directory,
                "programming" => FailureKind::Programming,
                "cache_connection" => FailureKind::CacheConnection,
                "cache_authentication" => FailureKind::CacheAuthentication,
                "uninitialized_cache" => FailureKind::UninitializedCache,
                "child_publication" => FailureKind::ChildPublication,
                _ => panic!("Unsupported controlled fixture kind"),
            };
            Some(Terminal::Raised(Failure::new(kind, input["arguments"].as_array().unwrap().clone(), "controlled_executor", "Synthetic terminal plumbing probe; no native IndexService classification is claimed.").unwrap()))
        }
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(Controlled {
        value: terminal,
        calls: calls.clone(),
    });
    let outcome = match text("action") {
        "immediate" => {
            let worker = IndexWorker::new(jobs.clone(), executor);
            let outcome = worker.run_once(Arc::new(())).await;
            worker.shutdown(Duration::from_secs(4)).await.unwrap();
            format!("{outcome:?}")
        }
        "scheduled" => {
            let reservation = jobs
                .reserve_index_eta(Arc::new(()), Limits::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reservation.message().task_id, expected);
            let worker =
                ScheduledIndexWorker::new(jobs.clone(), executor, ScheduleConfig::default())
                    .unwrap();
            let job = worker.start(&reservation, Arc::new(())).unwrap();
            drop(reservation);
            let outcome = tokio::time::timeout(Duration::from_secs(10), job.wait())
                .await
                .unwrap();
            worker.shutdown(Duration::from_secs(4)).await.unwrap();
            format!("{outcome:?}")
        }
        _ => panic!("Owned fixture action"),
    };
    jobs.shutdown(Duration::from_secs(4)).await.unwrap();
    println!(
        "{}",
        json!({"task_id":expected.to_string(),"outcome":outcome,"calls":calls.load(Ordering::SeqCst),"scope":"Controlled executor terminal plumbing only; no native failure classifier"})
    );
}
