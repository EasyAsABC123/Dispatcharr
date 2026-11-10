//! Real native scanner/SQL effects for Source136's tolerant normal outcomes.
//! Original Dispatcharr authors retain all application/algorithm credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_ingest_epg::{
    index_service::{
        IndexConfig, IndexDispatcher, IndexOutcome, IndexRequest, IndexService, IndexServices,
    },
    programme_index::IndexLimits,
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::Store;
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct NoDefer;
impl IndexDispatcher for NoDefer {
    fn defer<'a>(
        &'a self,
        _: i64,
        _: u32,
        _: u32,
        _: SwapPolicyContext,
    ) -> BoxFuture<'a, Result<(), ImportError>> {
        Box::pin(async { panic!("No busy Source136 scanner case may defer") })
    }
}
struct Owner(Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
#[ignore = "requires copied Original136 worker corpus and actual PostgreSQL/Redis"]
async fn tolerant_raw_index_cases_commit_exact_original_json_before_normal_success() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_"));
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let locks = TaskLocks::connect(&redis, TaskLockConfig::default())
        .await
        .unwrap();
    let service = IndexService::new(
        IndexServices {
            store: store.clone(),
            locks,
            dispatcher: Arc::new(NoDefer),
            clock: Arc::new(chrono::Utc::now),
        },
        IndexConfig {
            path_context: None,
            max_jobs: 1,
            limits: IndexLimits {
                state_bytes: 1 << 20,
                file_bytes: 16 << 20,
                max_keys: 10000,
                max_tags: 10000,
            },
            cache_dir: "/evidence/native-failure-scan-cache".into(),
            release_timeout: Duration::from_secs(5),
            max_index_json_bytes: 1 << 20,
            max_memory_bytes: 32 << 20,
        },
    )
    .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut compared = 0;
    for case in fixture["index_failure_cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        if !matches!(name, "malformed-xml" | "invalid-encoding" | "empty-file") {
            continue;
        }
        assert_eq!(case["result"]["status"], "SUCCESS");
        assert!(case["result"]["result"].is_null());
        let id = case["source_id"].as_i64().unwrap();
        let owner: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(owner, format!("rust-index-failure-{name}"));
        sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
            .bind(id)
            .execute(store.pool())
            .await
            .unwrap();
        let outcome = service
            .build(
                IndexRequest {
                    source_id: id,
                    retry: 0,
                    cancelled: Arc::new(AtomicBool::new(false)),
                },
                Arc::new(Owner(dropped.clone())),
            )
            .await
            .unwrap();
        assert_eq!(
            outcome,
            IndexOutcome::Written,
            "{name}: must really write, not swallow parser errors"
        );
        let actual: Value =
            sqlx::query_scalar("SELECT data FROM epg_epgsourceindex WHERE source_id=$1")
                .bind(id)
                .fetch_one(store.pool())
                .await
                .unwrap();
        assert_eq!(
            actual, case["index"],
            "{name}: literal partial/lossy/empty source JSON"
        );
        compared += 1;
        assert_eq!(dropped.load(Ordering::SeqCst), compared);
    }
    assert_eq!(compared, 3);
    assert!(service.shutdown(Duration::from_secs(3)).await);
    assert_eq!(service.physical_jobs(), 0);
    store.pool().close().await;
}
