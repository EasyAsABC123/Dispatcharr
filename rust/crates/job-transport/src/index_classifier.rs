//! Explicit known terminal adapter over physically completed index reports.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
//! PostgreSQL/Django/psycopg/Celery/Kombu authors retain framework credit/licenses.
use crate::{
    index_protocol::IndexMessage,
    index_terminal::{Failure, FailureKind, Terminal},
    index_worker::{
        ChildPublisher, Error, ExecuteFuture, IndexExecutor, ServiceExecutor, TerminalFuture,
    },
    pg_failure_format,
};
use dispatcharr_ingest_epg::{
    index_service::{
        IndexCommitObservation as Commit, IndexExecutionReport, IndexFailureCause, IndexOutcome,
        IndexReleaseObservation as Release, IndexRequest, IndexService, IndexStage,
    },
    ImportError, SwapPolicyContext,
};
use serde_json::json;
use std::sync::{atomic::AtomicBool, Arc};
use std::{future::Future, pin::Pin};

/// Classification is restricted to Source136/139 outcomes plus the separately
/// verified P0001 field formatter. Unclassified controls stay operational.
pub fn classify(report: IndexExecutionReport) -> Result<Terminal, Error> {
    match (&report.operation, &report.release, report.commit) {
        (Ok(IndexOutcome::Written), Release::Released, Commit::Written)
        | (
            Ok(IndexOutcome::MissingSource | IndexOutcome::MissingFile),
            Release::Released,
            Commit::NotWritten,
        )
        | (
            Ok(IndexOutcome::Deferred | IndexOutcome::Exhausted),
            Release::NotAcquired,
            Commit::NotWritten,
        ) => return Ok(Terminal::NullSuccess),
        _ => {}
    }
    // A release failure, CAS ownership loss, timeout or ambiguous commit is not
    // silently rewritten into an observed Python exception. No rollback/deletion
    // of an earlier committed index is attempted by this adapter.
    if !matches!(report.release, Release::Released) || report.commit != Commit::NotWritten {
        return Err(Error::Execution);
    }
    let failed = report.operation.err().ok_or(Error::Execution)?;
    match failed.cause {
        IndexFailureCause::Operation(ImportError::Io(error))
            if failed.stage==IndexStage::FileOpen
                &&error.raw_os_error()==Some(21)
                &&error.kind()==std::io::ErrorKind::IsADirectory=>{
            Failure::new(FailureKind::Directory,vec![json!(21),json!("Is a directory")],"file_open",
                "Physically completed directory input failure; no index commit; file lease released.")
                .map(Terminal::Raised).map_err(|_|Error::Execution)
        }
        IndexFailureCause::Operation(ImportError::Store(dispatcharr_store::StoreError::Database(error)))
            if failed.stage==IndexStage::Write=>{
            let pg=error.as_database_error().and_then(|error|error.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()).ok_or(Error::Execution)?;
            let message=pg_failure_format::render_pg(pg).map_err(|_|Error::Execution)?;
            Failure::new(FailureKind::Programming,vec![json!(message)],"index_write",
                "Physically completed bounded P0001 index write failure; transaction not committed; file lease released.")
                .map(Terminal::Raised).map_err(|_|Error::Execution)
        }
        _=>Err(Error::Execution),
    }
}
struct Dispatcher(ChildPublisher);
impl dispatcharr_ingest_epg::index_service::IndexDispatcher for Dispatcher {
    fn defer<'a>(
        &'a self,
        id: i64,
        retry: u32,
        seconds: u32,
        context: SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + 'a>> {
        Box::pin(async move {
            context.ensure_active()?;
            self.0
                .defer(id, retry, seconds, Arc::new(context))
                .await
                .map_err(|_| ImportError::Worker)?;
            Ok(())
        })
    }
}
/// Existing execute() callers retain their original coarse API. Only the
/// explicit terminal method uses accepted detailed report/known mappings.
pub struct ClassifiedServiceExecutor {
    service: IndexService,
}
impl ClassifiedServiceExecutor {
    pub fn new(service: IndexService) -> Self {
        Self { service }
    }
}
impl IndexExecutor for ClassifiedServiceExecutor {
    fn execute<'a>(
        &'a self,
        message: IndexMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> ExecuteFuture<'a> {
        Box::pin(async move {
            ServiceExecutor::new(self.service.clone())
                .execute(message, publisher, owner)
                .await
        })
    }
    fn execute_terminal<'a>(
        &'a self,
        message: IndexMessage,
        publisher: ChildPublisher,
        owner: Arc<dyn Send + Sync>,
    ) -> TerminalFuture<'a> {
        Box::pin(async move {
            let report = self
                .service
                .build_report_with_dispatcher(
                    IndexRequest {
                        source_id: message.source_id,
                        retry: message.retry,
                        cancelled: Arc::new(AtomicBool::new(false)),
                    },
                    owner,
                    Arc::new(Dispatcher(publisher)),
                )
                .await
                .map_err(|_| Error::Execution)?;
            classify(report)
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use dispatcharr_ingest_epg::index_service::IndexStageFailure;
    fn normal(outcome: IndexOutcome, commit: Commit, release: Release) -> IndexExecutionReport {
        IndexExecutionReport {
            operation: Ok(outcome),
            commit,
            release,
        }
    }
    fn directory(commit: Commit, release: Release) -> IndexExecutionReport {
        IndexExecutionReport {
            operation: Err(IndexStageFailure {
                stage: IndexStage::FileOpen,
                cause: IndexFailureCause::Operation(ImportError::Io(
                    std::io::Error::from_raw_os_error(21),
                )),
            }),
            commit,
            release,
        }
    }
    #[test]
    fn normal_outcomes_require_consistent_commit_and_actual_release() {
        for outcome in [IndexOutcome::MissingSource, IndexOutcome::MissingFile] {
            assert_eq!(
                classify(normal(outcome, Commit::NotWritten, Release::Released)),
                Ok(Terminal::NullSuccess)
            );
        }
        for outcome in [IndexOutcome::Deferred, IndexOutcome::Exhausted] {
            assert_eq!(
                classify(normal(outcome, Commit::NotWritten, Release::NotAcquired)),
                Ok(Terminal::NullSuccess)
            );
        }
        assert_eq!(
            classify(normal(
                IndexOutcome::Written,
                Commit::Written,
                Release::Released
            )),
            Ok(Terminal::NullSuccess)
        );
        assert_eq!(
            classify(normal(
                IndexOutcome::Written,
                Commit::NotWritten,
                Release::Released
            )),
            Err(Error::Execution)
        );
        assert_eq!(
            classify(normal(
                IndexOutcome::Written,
                Commit::Written,
                Release::TimedOut
            )),
            Err(Error::Execution)
        );
    }
    #[test]
    fn known_directory_has_typed_args_and_truthful_native_diagnostic() {
        let Terminal::Raised(failure) =
            classify(directory(Commit::NotWritten, Release::Released)).unwrap()
        else {
            panic!("expected raised directory")
        };
        assert_eq!(failure.kind(), FailureKind::Directory);
        let value: serde_json::Value = serde_json::from_slice(
            &failure
                .metadata(
                    uuid::Uuid::new_v4(),
                    None,
                    &[],
                    "2026-11-10T00:00:00.000000+00:00",
                )
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            value["result"],
            json!({"exc_type":"IsADirectoryError","exc_module":"builtins","exc_message":[21,"Is a directory"]})
        );
        assert!(value["traceback"]
            .as_str()
            .unwrap()
            .starts_with("Native Dispatcharr index task diagnostic"));
        assert!(!value["traceback"]
            .as_str()
            .unwrap()
            .contains("Traceback (most recent call last)"));
    }
    #[test]
    fn controls_release_uncertainty_and_inconsistent_stages_never_fabricate_failure() {
        for release in [
            Release::NotOwner,
            Release::TimedOut,
            Release::RuntimeUnavailable,
            Release::NotAcquired,
        ] {
            assert_eq!(
                classify(directory(Commit::NotWritten, release)),
                Err(Error::Execution)
            );
        }
        for commit in [Commit::Written, Commit::Ambiguous] {
            assert_eq!(
                classify(directory(commit, Release::Released)),
                Err(Error::Execution)
            );
        }
        for cause in [
            ImportError::Cancelled,
            ImportError::Capacity("owned"),
            ImportError::Worker,
            ImportError::Parser,
        ] {
            assert_eq!(
                classify(IndexExecutionReport {
                    operation: Err(IndexStageFailure {
                        stage: IndexStage::Scan,
                        cause: IndexFailureCause::Operation(cause)
                    }),
                    commit: Commit::NotWritten,
                    release: Release::Released
                }),
                Err(Error::Execution)
            );
        }
    }
    #[tokio::test]
    #[ignore = "Guarded copiedDB real IndexService classified worker peer; stdin JSON, no app hook."]
    async fn native_classified_service_peer() {
        use crate::{
            eta::Limits,
            index_worker::IndexWorker,
            scheduled_index::{Config as ScheduleConfig, ScheduledIndexWorker},
            transport::{Config as JobsConfig, Jobs},
        };
        use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
        use dispatcharr_ingest_epg::{
            index_service::{IndexConfig, IndexServices},
            programme_index::IndexLimits,
        };
        use std::{
            io::Read,
            path::PathBuf,
            sync::atomic::{AtomicUsize, Ordering},
            time::Duration,
        };
        assert_eq!(
            std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let db = std::env::var("POSTGRES_DB").unwrap();
        assert!(
            db.starts_with("rust_probe_backup_jobs_")
                && db.len() <= 63
                && db
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        );
        let mut bytes = Vec::new();
        std::io::stdin()
            .take(16385)
            .read_to_end(&mut bytes)
            .unwrap();
        assert!(bytes.len() <= 16384);
        let input: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let field = |key: &str| input[key].as_str().unwrap();
        let mode = field("action");
        assert!(matches!(mode, "immediate" | "scheduled"));
        let task_id = uuid::Uuid::parse_str(field("task_id")).unwrap();
        let source_id = input["source_id"].as_i64().unwrap();
        assert!(source_id > 0);
        let database = field("database_url");
        assert!(["postgres", "postgresql"]
            .into_iter()
            .any(
                |scheme| ["127.0.0.1", "localhost"].into_iter().any(|host| database
                    == format!("{scheme}://dispatch:playback-fixture-only@{host}:5432/{db}"))
            ));
        let broker = field("broker_url");
        let backend = field("result_url");
        let cache = field("locks_url");
        let info = redis::Client::open(cache)
            .unwrap()
            .get_connection_info()
            .clone();
        assert_eq!(info.redis.db, 0);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port)if matches!(host.as_str(),"127.0.0.1"|"localhost")&&(*port==6379||(61000..=65000).contains(port)))
        );
        assert_eq!(field("source_cwd"), "/app");
        assert_eq!(field("media_root"), "/app/media");
        let jobs = Jobs::connect_fixture(broker, backend, &db, JobsConfig::default())
            .await
            .unwrap();
        let candidate = jobs
            .peek_index_candidate(Arc::new(()), Limits::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(candidate.message().task_id, task_id);
        assert_eq!(candidate.message().source_id, source_id);
        let store = dispatcharr_store::Store::connect(database, 1)
            .await
            .unwrap();
        let locks = TaskLocks::connect(
            cache,
            TaskLockConfig {
                max_leases: 1,
                max_commands: 4,
                ..TaskLockConfig::default()
            },
        )
        .await
        .unwrap();
        struct NoDefault;
        impl dispatcharr_ingest_epg::index_service::IndexDispatcher for NoDefault {
            fn defer<'a>(
                &'a self,
                _: i64,
                _: u32,
                _: u32,
                _: SwapPolicyContext,
            ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + 'a>> {
                Box::pin(async { Err(ImportError::Worker) })
            }
        }
        let service = IndexService::new(
            IndexServices {
                store: store.clone(),
                locks: locks.clone(),
                dispatcher: Arc::new(NoDefault),
                clock: Arc::new(chrono::Utc::now),
            },
            IndexConfig {
                max_jobs: 1,
                limits: IndexLimits {
                    state_bytes: 1 << 20,
                    file_bytes: 16 << 20,
                    max_keys: 10000,
                    max_tags: 10000,
                },
                cache_dir: PathBuf::from("/evidence/classifier-cache"),
                release_timeout: Duration::from_secs(5),
                max_index_json_bytes: 1 << 20,
                max_memory_bytes: 32 << 20,
                path_context: Some(
                    dispatcharr_ingest_epg::current_lookup::SourcePathContext::new(
                        PathBuf::from(field("source_cwd")),
                        PathBuf::from(field("media_root")),
                    )
                    .unwrap(),
                ),
            },
        )
        .unwrap();
        struct Expected {
            real: ClassifiedServiceExecutor,
            id: uuid::Uuid,
            source: i64,
            calls: Arc<AtomicUsize>,
        }
        impl IndexExecutor for Expected {
            fn execute<'a>(
                &'a self,
                message: IndexMessage,
                publisher: ChildPublisher,
                owner: Arc<dyn Send + Sync>,
            ) -> ExecuteFuture<'a> {
                Box::pin(async move {
                    assert_eq!(message.task_id, self.id);
                    assert_eq!(message.source_id, self.source);
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    self.real.execute(message, publisher, owner).await
                })
            }
            fn execute_terminal<'a>(
                &'a self,
                message: IndexMessage,
                publisher: ChildPublisher,
                owner: Arc<dyn Send + Sync>,
            ) -> TerminalFuture<'a> {
                Box::pin(async move {
                    assert_eq!(message.task_id, self.id);
                    assert_eq!(message.source_id, self.source);
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    self.real.execute_terminal(message, publisher, owner).await
                })
            }
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = Arc::new(Expected {
            real: ClassifiedServiceExecutor::new(service.clone()),
            id: task_id,
            source: source_id,
            calls: calls.clone(),
        });
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let outcome = if mode == "immediate" {
            assert!(candidate.message().eta.is_none());
            drop(candidate);
            let worker = IndexWorker::new(jobs.clone(), executor);
            let result = worker.run_once(owner).await;
            worker.shutdown(Duration::from_secs(5)).await.unwrap();
            format!("{result:?}")
        } else {
            assert!(candidate.message().eta.is_some());
            let worker =
                ScheduledIndexWorker::new(jobs.clone(), executor, ScheduleConfig::default())
                    .unwrap();
            let receipt = jobs
                .reserve_index_candidate(candidate, Arc::new(()), Limits::default())
                .await
                .unwrap();
            let pending = worker.start(&receipt, owner).unwrap();
            drop(receipt);
            let result = pending.wait().await;
            worker.shutdown(Duration::from_secs(5)).await.unwrap();
            format!("{result:?}")
        };
        assert!(service.shutdown(Duration::from_secs(5)).await);
        assert_eq!(service.physical_jobs(), 0);
        assert_eq!(locks.available_leases(), 1);
        jobs.shutdown(Duration::from_secs(5)).await.unwrap();
        store.pool().close().await;
        assert!(weak.upgrade().is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        println!(
            "NATIVE_INDEX_TERMINAL_JSON={}",
            json!({"calls":1,"outcome":outcome,"owner_released":true,"physical_jobs":0,"file_lease_slots":1,"worker_drained":true,"service_drained":true,"shared_transport_drained":true})
        );
    }
}
