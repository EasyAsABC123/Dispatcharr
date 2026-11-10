//! Independent actual SQL schedule/physical gates; no production HTTP route.
//! Original Dispatcharr authors/contributors retain credit and AGPL licensing.
use super::super::{
    vod_api_owned_read::{ReadLimits, ReadRuntime},
    vod_api_projection::visibility,
};
use super::*;
use bytes::Bytes;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{pool::PoolConnection, postgres::PgPoolOptions, Postgres};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

async fn execute(
    runtime: &ReadRuntime,
    pool: PgPool,
    pairs: Vec<(String, String)>,
    hooks: PhaseHooks,
) -> Result<Bytes, ReadError> {
    runtime
        .run(1024, move |work| async move {
            let visible = visibility(&json!({}), 1).unwrap();
            match read_with_hooks(
                &pool,
                &work,
                &visible,
                &pairs,
                "http://localhost",
                "/api/vod/all/",
                &hooks,
            )
            .await
            {
                Ok(bytes) => Ok(bytes),
                Err(UnifiedFault::Read(error)) => Err(error),
                Err(UnifiedFault::View(message)) => {
                    Err(crate::StoreError::StatusBody(500, json!({"error":message})).into())
                }
                Err(UnifiedFault::Renderer) => Err(ReadError::SourceUnhandledFault(
                    "unified nonfinite renderer",
                )),
            }
        })
        .await
}
fn observed(result: Result<Bytes, ReadError>) -> Value {
    match result {
        Ok(bytes) => {
            json!({"status":200,"body_sha256":format!("{:x}",Sha256::digest(&bytes)),"json":serde_json::from_slice::<Value>(&bytes).unwrap()})
        }
        Err(ReadError::Source(crate::StoreError::StatusBody(status, body))) => {
            json!({"status":status,"body_sha256":format!("{:x}",Sha256::digest(crate::json_compat::compact_json_bytes(&body).unwrap())),"json":body})
        }
        Err(error) => json!({"unexpected_error":format!("{error:?}")}),
    }
}
fn output(name: &str, value: &Value) {
    let directory = std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap());
    std::fs::write(
        directory.join(name),
        serde_json::to_vec_pretty(value).unwrap(),
    )
    .unwrap();
}
async fn fixture() -> (PgPool, Value) {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_UNIFIED_SCHEDULE_REFERENCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(source["passed"], true);
    assert_eq!(source["owned_rows_restored"], true);
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    (pool, source)
}

#[tokio::test]
#[ignore = "Requires owned actual Source/PG schedule reference"]
async fn source_materialized_page_fresh_count_and_error_priority() {
    let (pool, source) = fixture().await;
    let rows=sqlx::query("SELECT id::bigint AS id,name,rating FROM vod_movie WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 2").fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    let first: i64 = rows[0].try_get("id").unwrap();
    let second: i64 = rows[1].try_get("id").unwrap();
    let name: String = rows[0].try_get("name").unwrap();
    let ratings: Vec<Option<String>> = rows.iter().map(|r| r.try_get("rating").unwrap()).collect();
    let account:i64=sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccount WHERE name LIKE 'rust-probe-vod-rest-%' AND is_active ORDER BY id LIMIT 1").fetch_one(&pool).await.unwrap();
    let mut records = Vec::new();
    let mut differences = Vec::new();
    for reference in source["records"].as_array().unwrap().iter().take(4) {
        let kind = reference["kind"].as_str().unwrap();
        let mut limits = ReadLimits::default();
        if kind == "nan_count_timeout" {
            limits.statement_deadline = Duration::from_millis(200);
        }
        let runtime = ReadRuntime::new(limits).unwrap();
        let counts = Arc::new(AtomicUsize::new(0));
        let count_copy = counts.clone();
        let blocker: Arc<tokio::sync::Mutex<Option<PoolConnection<Postgres>>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let mut hooks = PhaseHooks {
            before_count: Some(Arc::new(move || {
                count_copy.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })),
            ..Default::default()
        };
        if matches!(kind, "raw_page" | "fresh_count" | "nan_count_timeout") {
            let pool_copy = pool.clone();
            let kind = kind.to_owned();
            let blocker_copy = blocker.clone();
            hooks.after_page = Some(Arc::new(move || {
                let pool = pool_copy.clone();
                let kind = kind.clone();
                let blocker = blocker_copy.clone();
                Box::pin(async move {
                    match kind.as_str() {
                        "raw_page" => {
                            sqlx::query("UPDATE vod_movie SET name='rust-probe-vod-rest-mutated-after-raw-page' WHERE id=$1").bind(first).execute(&pool).await?;
                        }
                        "fresh_count" => {
                            sqlx::query("UPDATE m3u_m3uaccount SET is_active=false WHERE id=$1")
                                .bind(account)
                                .execute(&pool)
                                .await?;
                        }
                        "nan_count_timeout" => {
                            let mut connection = pool.acquire().await?;
                            sqlx::query("BEGIN").execute(&mut *connection).await?;
                            sqlx::query("LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE")
                                .execute(&mut *connection)
                                .await?;
                            *blocker.lock().await = Some(connection);
                        }
                        _ => unreachable!(),
                    }
                    Ok(())
                })
            }));
        }
        if matches!(kind, "nan_then_bad" | "nan_count_timeout") {
            sqlx::query("UPDATE vod_movie SET rating='NaN' WHERE id=$1")
                .bind(first)
                .execute(&pool)
                .await
                .unwrap();
        }
        if kind == "nan_then_bad" {
            sqlx::query("UPDATE vod_movie SET rating='bad' WHERE id=$1")
                .bind(second)
                .execute(&pool)
                .await
                .unwrap();
        }
        let actual = observed(
            execute(
                &runtime,
                pool.clone(),
                vec![(
                    "page_size".into(),
                    if kind == "nan_then_bad" { "2" } else { "1" }.into(),
                )],
                hooks,
            )
            .await,
        );
        if let Some(mut connection) = blocker.lock().await.take() {
            sqlx::query("ROLLBACK")
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        sqlx::query("UPDATE vod_movie SET name=$1,rating=$2 WHERE id=$3")
            .bind(&name)
            .bind(&ratings[0])
            .bind(first)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE vod_movie SET rating=$1 WHERE id=$2")
            .bind(&ratings[1])
            .bind(second)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE m3u_m3uaccount SET is_active=true WHERE id=$1")
            .bind(account)
            .execute(&pool)
            .await
            .unwrap();
        let same = actual["status"] == reference["status"]
            && actual["body_sha256"] == reference["body_sha256"];
        if !same {
            differences.push(json!({"kind":kind,"source":reference,"native":actual}));
        }
        let expected_count = usize::from(kind != "nan_then_bad");
        assert_eq!(counts.load(Ordering::SeqCst), expected_count);
        assert!(runtime.shutdown(Duration::from_secs(5)).await);
        assert_eq!(runtime.retained_memory_bytes(), 0);
        records.push(json!({"kind":kind,"same":same,"actual":actual,"count_attempts":counts.load(Ordering::SeqCst)}));
    }
    output(
        "unified-schedule-native.json",
        &json!({"passed":differences.is_empty(),"records":records,"differences":differences,"scope":"Actual Original Factory/SQL vs native actualSQL controlled phase mutations; noHTTP/auth acceptance"}),
    );
    pool.close().await;
    assert!(differences.is_empty());
}

#[tokio::test]
#[ignore = "Requires actual owned Source229-row fixture/reference"]
async fn source_large_page_no_row_clamp_and_owned_frame_capacity() {
    let (pool, source) = fixture().await;
    let corpus: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_UNIFIED_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    let before = super::tests::actual_owned_rows(&pool, &corpus).await;
    let movies = source["large_fixture"]["movies"].as_array().unwrap();
    let relations = source["large_fixture"]["relations"].as_array().unwrap();
    assert_eq!(movies.len(), 200);
    assert_eq!(relations.len(), 200);
    let ids: Vec<i64> = movies
        .iter()
        .map(|row| {
            assert!(row["name"]
                .as_str()
                .unwrap()
                .starts_with("rust-probe-vod-rest-bounded-unified-"));
            row["id"].as_i64().unwrap()
        })
        .collect();
    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM vod_movie WHERE id=ANY($1)")
        .bind(&ids)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(existing, 0);
    // Test fixture import only: exact Original model-column values, including
    // IDs/UUIDs/timestamps. No ORM/SQL defaults or invented sort keys.
    sqlx::query(
        "INSERT INTO vod_movie SELECT * FROM jsonb_populate_recordset(NULL::vod_movie,$1::jsonb)",
    )
    .bind(&source["large_fixture"]["movies"])
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO vod_m3umovierelation SELECT * FROM jsonb_populate_recordset(NULL::vod_m3umovierelation,$1::jsonb)").bind(&source["large_fixture"]["relations"]).execute(&pool).await.unwrap();
    let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
    let body = execute(
        &runtime,
        pool.clone(),
        vec![("page_size".into(), "250".into())],
        PhaseHooks::default(),
    )
    .await
    .unwrap();
    let body_hash = format!("{:x}", Sha256::digest(&body));
    let reference = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "large_small_rows")
        .unwrap();
    let exact = reference["body_sha256"] == body_hash;
    let value: Value = serde_json::from_slice(&body).unwrap();
    let emitted = value["results"].as_array().unwrap().len();
    let original = body;
    let clone = original.clone();
    let retained = runtime.retained_memory_bytes();
    assert!(retained > 0);
    drop(original);
    assert_eq!(runtime.retained_memory_bytes(), retained);
    let drained_with_frame = runtime.shutdown(Duration::from_secs(2)).await;
    assert!(drained_with_frame);
    assert_eq!(runtime.retained_memory_bytes(), retained);
    drop(clone);
    assert_eq!(runtime.retained_memory_bytes(), 0);

    let small = ReadRuntime::new(ReadLimits {
        response_bytes: 4096,
        ..ReadLimits::default()
    })
    .unwrap();
    let response_cap = matches!(
        execute(
            &small,
            pool.clone(),
            vec![("page_size".into(), "250".into())],
            PhaseHooks::default()
        )
        .await,
        Err(ReadError::Capacity)
    );
    assert!(small.shutdown(Duration::from_secs(3)).await);
    assert_eq!(small.retained_memory_bytes(), 0);
    let capped = ReadRuntime::new(ReadLimits::default()).unwrap();
    sqlx::query("UPDATE vod_movie SET description=$1 WHERE id=$2")
        .bind("x".repeat(ReadLimits::default().row_bytes + 100))
        .bind(ids[0])
        .execute(&pool)
        .await
        .unwrap();
    let row_cap = matches!(
        execute(
            &capped,
            pool.clone(),
            vec![("page_size".into(), "250".into())],
            PhaseHooks::default()
        )
        .await,
        Err(ReadError::Capacity)
    );
    sqlx::query("UPDATE vod_movie SET description=$1 WHERE id=ANY($2)")
        .bind("x".repeat(64 * 1024))
        .bind(&ids)
        .execute(&pool)
        .await
        .unwrap();
    let aggregate_cap = matches!(
        execute(
            &capped,
            pool.clone(),
            vec![("page_size".into(), "250".into())],
            PhaseHooks::default()
        )
        .await,
        Err(ReadError::Capacity)
    );
    assert!(capped.shutdown(Duration::from_secs(3)).await);
    assert_eq!(capped.retained_memory_bytes(), 0);
    sqlx::query("DELETE FROM vod_m3umovierelation WHERE movie_id=ANY($1)")
        .bind(&ids)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM vod_movie WHERE id=ANY($1)AND name LIKE 'rust-probe-vod-rest-bounded-unified-%'").bind(&ids).execute(&pool).await.unwrap();
    let after = super::tests::actual_owned_rows(&pool, &corpus).await;
    let passed =
        exact && emitted == 229 && response_cap && row_cap && aggregate_cap && before == after;
    output(
        "unified-large-resources.json",
        &json!({"passed":passed,"Source_body_sha256":reference["body_sha256"],"native_body_sha256":body_hash,"rows":emitted,"source_uncapped_page_exact":exact,"final_clone_retained_bytes":retained,"shutdown_drained_with_frame":drained_with_frame,"response_capacity_no_partial200":response_cap,"row_capacity_no_partial200":row_cap,"aggregate_raw_capacity_no_partial200":aggregate_cap,"owned_rows_restored":before==after,"actual_before":before,"actual_after":after,"capacity_scope":"Explicit bounded native rejection, not Source fault/status parity or allocator-wide measurement","sequence_scope":"Source fixture sequences may advance; no claim of sequence restoration"}),
    );
    pool.close().await;
    assert!(passed);
}

#[tokio::test]
#[ignore = "Requires owned real PostgreSQL relation locks and read-runtime accounting"]
async fn physical_sql_abort_timeout_capacity_and_shutdown() {
    let (pool, _) = fixture().await;
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *blocker).await.unwrap();
    sqlx::query("LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let limits = ReadLimits {
        logical_deadline: Duration::from_millis(250),
        statement_deadline: Duration::from_secs(2),
        ..ReadLimits::default()
    };
    let runtime = ReadRuntime::new(limits).unwrap();
    let first = tokio::spawn({
        let runtime = runtime.clone();
        let pool = pool.clone();
        async move { execute(&runtime, pool, vec![], PhaseHooks::default()).await }
    });
    let second = tokio::spawn({
        let runtime = runtime.clone();
        let pool = pool.clone();
        async move { execute(&runtime, pool, vec![], PhaseHooks::default()).await }
    });
    tokio::time::timeout(Duration::from_secs(1),async {loop {
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid()AND wait_event_type='Lock'AND query LIKE 'WITH source_page AS MATERIALIZED%' ").fetch_one(&pool).await.unwrap();
        if count==2 {break;}
        tokio::time::sleep(Duration::from_millis(5)).await;
    }}).await.unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let timed_out = matches!(second.await.unwrap(), Err(ReadError::Deadline));
    let workers_after_logical = runtime.physical_workers();
    let memory_held = runtime.retained_memory_bytes();
    let third_capacity = matches!(
        execute(&runtime, pool.clone(), vec![], PhaseHooks::default()).await,
        Err(ReadError::Capacity)
    );
    let physical_queries:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid()AND wait_event_type='Lock'AND query LIKE 'WITH source_page AS MATERIALIZED%'").fetch_one(&pool).await.unwrap();
    let early_shutdown = runtime.shutdown(Duration::from_millis(50)).await;
    let workers_while_locked = runtime.physical_workers();
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();
    drop(blocker);
    let final_shutdown = runtime.shutdown(Duration::from_secs(3)).await;
    let final_memory = runtime.retained_memory_bytes();
    let passed = timed_out
        && workers_after_logical == 2
        && workers_while_locked == 2
        && physical_queries == 2
        && third_capacity
        && !early_shutdown
        && final_shutdown
        && final_memory == 0
        && memory_held <= limits.memory_bytes;
    output(
        "unified-physical-sql.json",
        &json!({"passed":passed,"caller_abort_and_logical_deadline":timed_out,"physical_workers_after_logical":workers_after_logical,"physical_workers_while_locked":workers_while_locked,"actual_locked_SQL_queries":physical_queries,"third_rejected_without_SQL":third_capacity,"retained_memory_while_SQL_pending":memory_held,"configured_memory_limit":limits.memory_bytes,"shutdown_early_false":!early_shutdown,"shutdown_after_reply_and_rollback":final_shutdown,"final_memory":final_memory,"scope":"Real SQL locks, actual canceled callers and physical worker/memory accounting; HTTP input/frame-slot queue remains separate"}),
    );
    pool.close().await;
    assert!(passed);
}

fn identities(rows: &[PgRow]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let id = row
                .try_get::<i64, _>("id")
                .unwrap_or_else(|_| i64::from(row.try_get::<i32, _>("id").unwrap()));
            json!([id, row.try_get::<String, _>("content_type").unwrap()])
        })
        .collect()
}
#[tokio::test]
#[ignore = "Requires captured actual SourceSQL and tied-page PostgreSQL fixture"]
async fn tied_raw_page_source_shape_explain_and_ordinals() {
    let (pool, _) = fixture().await;
    let corpus: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_UNIFIED_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    let source_cases = corpus["records"].as_array().unwrap();
    let mut cases = vec![source_cases
        .iter()
        .find(|r| r["role"] == "standard" && r["path"] == "/api/vod/all/" && r["label"].is_null())
        .unwrap()];
    cases.extend(
        source_cases
            .iter()
            .filter(|r| r["label"] == "cross-kind LOWER(name),id tie"),
    );
    assert_eq!(cases.len(), 7);
    let series=sqlx::query("SELECT id::bigint AS id,name FROM vod_series WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1").fetch_one(&pool).await.unwrap();
    let series_id: i64 = series.try_get("id").unwrap();
    let series_name: String = series.try_get("name").unwrap();
    let movie_name: String = sqlx::query_scalar(
        "SELECT name FROM vod_movie WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let visible = visibility(&json!({}), 1).unwrap();
    let mut records = Vec::new();
    let mut differences = Vec::new();
    for case in cases {
        if case["label"] == "cross-kind LOWER(name),id tie" {
            sqlx::query("UPDATE vod_series SET name=$1 WHERE id=$2")
                .bind(&movie_name)
                .bind(series_id)
                .execute(&pool)
                .await
                .unwrap();
        }
        let path = case["path"].as_str().unwrap();
        let (paired_expected, fresh_binding) = if case["label"] == "cross-kind LOWER(name),id tie" {
            let (expected, binding) = fresh_original_tie(&pool, &corpus, case).await;
            (expected, Some(binding))
        } else {
            (case["expected"].clone(), None)
        };
        let no_write_before = super::tests::actual_owned_rows(&pool, &corpus).await;
        let pairs =
            url::form_urlencoded::parse(path.split_once('?').map_or("", |v| v.1).as_bytes())
                .map(|(a, b)| (a.into_owned(), b.into_owned()))
                .collect::<Vec<_>>();
        let query = UnifiedQuery::parse(&pairs, &visible).unwrap().unwrap();
        let native = query
            .page(&visible, ReadLimits::default().row_bytes)
            .unwrap();
        let raw_sql = native
            .sql
            .strip_prefix("WITH source_page AS MATERIALIZED (")
            .unwrap()
            .split_once(") SELECT p.id::bigint AS id")
            .unwrap()
            .0;
        let inner = super::super::vod_api_unified_sql::Statement {
            sql: raw_sql.into(),
            binds: native.binds[..native.binds.len() - 1].to_vec(),
        };
        let source_sql = case["actual_page_then_count_SQL"][0].as_str().unwrap();
        assert!(
            source_sql
                .trim_start()
                .starts_with("WITH unified_content AS (")
                && source_sql.len() < 131072
        );
        let original_rows = sqlx::query(source_sql)
            .persistent(false)
            .fetch_all(&pool)
            .await
            .unwrap();
        let inner_rows = inner.query().fetch_all(&pool).await.unwrap();
        let final_rows = native.query().fetch_all(&pool).await.unwrap();
        let original_ids = identities(&original_rows);
        let inner_ids = identities(&inner_rows);
        let final_ids = identities(&final_rows);
        let expected = paired_expected["json"]["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| json!([r["id"], r["content_type"]]))
            .collect::<Vec<_>>();
        let source_plan: Value = sqlx::query(&format!(
            "EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {source_sql}"
        ))
        .persistent(false)
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get(0)
        .unwrap();
        let mut explain = native.clone();
        explain.sql = format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {}", explain.sql);
        let native_plan: Value = explain
            .query()
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get(0)
            .unwrap();
        let no_write_after = super::tests::actual_owned_rows(&pool, &corpus).await;
        assert_eq!(no_write_before, no_write_after);
        let same = expected == original_ids && expected == inner_ids && expected == final_ids;
        if !same {
            differences.push(json!({"path":path,"expected_Source_order":expected,"source_replay":original_ids,"inner":inner_ids,"final":final_ids}));
        }
        records.push(json!({"path":path,"same":same,"expected_Source_order":expected,"source_replay":original_ids,"native_inner":inner_ids,"native_final":final_ids,"source_SQL":source_sql,"native_SQL":native.sql,"source_plan":source_plan,"native_plan":native_plan,"fresh_Source_HTTP":fresh_binding,"all13tables_before":no_write_before,"all13tables_after":no_write_after}));
    }
    sqlx::query("UPDATE vod_series SET name=$1 WHERE id=$2")
        .bind(&series_name)
        .bind(series_id)
        .execute(&pool)
        .await
        .unwrap();
    output(
        "unified-ordering-plans.json",
        &json!({"passed":differences.is_empty(),"cases":records.len(),"records":records,"differences":differences,"scope":"Captured Original page SQL replay, raw native inner page and guarded final ordinals/EXPLAIN on same actual tied fixture; no general planner equivalence claim"}),
    );
    pool.close().await;
    assert!(differences.is_empty());
}

#[tokio::test]
#[ignore = "Requires actual Original zero-logo-ID SQL fixture"]
async fn raw_logo_id_python_truth_not_option_presence() {
    let (pool, source) = fixture().await;
    let logo = &source["zero_logo_fixture"];
    assert_eq!(logo["id"], 0);
    assert_eq!(logo["name"], "rust-probe-vod-rest-zero-id-logo");
    let existing: i64 = sqlx::query_scalar("SELECT count(*) FROM vod_vodlogo WHERE id=0")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(existing, 0);
    let row=sqlx::query("SELECT id::bigint AS id,logo_id::bigint AS logo FROM vod_movie WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1").fetch_one(&pool).await.unwrap();
    let id: i64 = row.try_get("id").unwrap();
    let old: Option<i64> = row.try_get("logo").unwrap();
    sqlx::query("INSERT INTO vod_vodlogo SELECT * FROM jsonb_populate_recordset(NULL::vod_vodlogo,$1::jsonb)").bind(json!([logo])).execute(&pool).await.unwrap();
    sqlx::query("UPDATE vod_movie SET logo_id=0 WHERE id=$1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
    let actual = observed(
        execute(
            &runtime,
            pool.clone(),
            vec![("page_size".into(), "1".into())],
            PhaseHooks::default(),
        )
        .await,
    );
    sqlx::query("UPDATE vod_movie SET logo_id=$1 WHERE id=$2")
        .bind(old)
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM vod_vodlogo WHERE id=0 AND name='rust-probe-vod-rest-zero-id-logo'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(runtime.shutdown(Duration::from_secs(3)).await);
    assert_eq!(runtime.retained_memory_bytes(), 0);
    let reference = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["kind"] == "zero_logo")
        .unwrap();
    let passed = actual["status"] == reference["status"]
        && actual["body_sha256"] == reference["body_sha256"];
    output(
        "unified-zero-logo.json",
        &json!({"passed":passed,"Source":reference,"native":actual,"scope":"Actual raw SQL zero-ID logo uses Original Python truthiness; no inferred positive-PK constraint"}),
    );
    pool.close().await;
    assert!(passed);
}

/// Fixed six Source-declared tied cases, never chosen from observed failures.
/// HTTP authority is current because legal physical heap/plan changes can
/// change equal-(LOWER(name),id) order even for the exact Original SQL.
pub(in crate::vod_api_draft) async fn fresh_original_tie(
    pool: &PgPool,
    corpus: &Value,
    case: &Value,
) -> (Value, Value) {
    const PATHS: [&str; 6] = [
        "/api/vod/all/?page_size=1",
        "/api/vod/all/?page_size=1&page=2",
        "/api/vod/all/?page_size=2",
        "/api/vod/all/?page_size=3",
        "/api/vod/all/?page_size=1&page=3",
        "/api/vod/all/?page_size=101",
    ];
    let policy_bytes = std::fs::read(std::env::var("VOD_UNIFIED_TIE_POLICY").unwrap()).unwrap();
    let policy: Value = serde_json::from_slice(&policy_bytes).unwrap();
    assert_eq!(policy["paths"], json!(PATHS));
    assert_eq!(policy["require_actual_SQL_sort_key_tie"], true);
    let path = case["path"].as_str().unwrap();
    assert!(PATHS.contains(&path));
    assert_eq!(case["label"], "cross-kind LOWER(name),id tie");
    assert_eq!(case["role"], "standard");
    let tie:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vod_movie m JOIN vod_series s ON m.id=s.id AND LOWER(m.name)=LOWER(s.name)WHERE m.name LIKE 'rust-probe-vod-rest-%')").fetch_one(pool).await.unwrap();
    assert!(tie);
    let base = std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap();
    let parsed = url::Url::parse(&base).unwrap();
    assert_eq!(parsed.scheme(), "http");
    assert_eq!(parsed.host_str(), Some("127.0.0.1"));
    assert!(parsed.port().is_some());
    let original: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let bearer = original["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["role"] == "standard" && row["path"] == "/api/vod/movies/" && row["method"] == "GET"
        })
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(bearer.starts_with("Bearer ") && bearer.len() < 16384);
    let client = reqwest::Client::builder()
        .no_proxy()
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    async fn get(client: &reqwest::Client, base: &str, path: &str, bearer: &str) -> Vec<u8> {
        let mut response = client
            .get(format!("{base}{path}"))
            .header("Host", "localhost")
            .header("Accept", "application/json")
            .header("Accept-Encoding", "identity")
            .header("Authorization", bearer)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(
            response.headers().get("Content-Type").unwrap(),
            "application/json"
        );
        let expected_length = response
            .headers()
            .get("Content-Length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let mut document = super::super::vod_api_owned_read::Document::bounded(2 * 1024 * 1024);
        while let Some(bytes) = response.chunk().await.unwrap() {
            document.write_all(&bytes).unwrap();
        }
        let bytes = document.finish();
        assert_eq!(bytes.len(), expected_length);
        bytes
    }
    let before = super::tests::actual_owned_rows(pool, corpus).await;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(database, "dispatcharr");
    assert_eq!(corpus["database_witness"]["actual_database"], database);
    assert_eq!(corpus["database_witness"]["configured_database"], database);
    // Binding only: random fixture UUIDs + complete active catalogue count
    // prove this HTTP worker serves the same owned tmpfs DB as native SQL.
    // This identity-set check does not normalize any protocol/body comparison.
    let sentinel = get(&client, &base, "/api/vod/all/?page_size=250", &bearer).await;
    let sentinel_json: Value = serde_json::from_slice(&sentinel).unwrap();
    let visible = visibility(&json!({}), 1).unwrap();
    let query = UnifiedQuery::parse(&[("page_size".into(), "250".into())], &visible)
        .unwrap()
        .unwrap();
    let sql_rows = query
        .page(&visible, ReadLimits::default().row_bytes)
        .unwrap()
        .query()
        .fetch_all(pool)
        .await
        .unwrap();
    let sql_ids = sql_rows
        .iter()
        .map(|row| {
            format!(
                "{}:{}:{}",
                row.try_get::<String, _>("content_type").unwrap(),
                row.try_get::<i64, _>("id").unwrap(),
                row.try_get::<String, _>("uuid").unwrap()
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    let http_ids = sentinel_json["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            format!(
                "{}:{}:{}",
                row["content_type"].as_str().unwrap(),
                row["id"].as_i64().unwrap(),
                row["uuid"].as_str().unwrap()
            )
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(http_ids, sql_ids);
    assert_eq!(Some(http_ids.len() as u64), sentinel_json["count"].as_u64());
    assert_eq!(http_ids.len(), 29);
    let bytes = get(&client, &base, path, &bearer).await;
    let expected = json!({"status":200,"body_bytes":bytes.len(),"body_sha256":format!("{:x}",Sha256::digest(&bytes)),"json":serde_json::from_slice::<Value>(&bytes).unwrap()});
    for key in ["count", "next", "previous"] {
        assert_eq!(expected["json"][key], case["expected"]["json"][key]);
    }
    let historical_full = corpus["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["label"] == "cross-kind LOWER(name),id tie"
                && row["path"] == "/api/vod/all/?page_size=101"
        })
        .unwrap();
    for row in expected["json"]["results"].as_array().unwrap() {
        let prior = historical_full["expected"]["json"]["results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|prior| prior["id"] == row["id"] && prior["content_type"] == row["content_type"])
            .unwrap();
        assert_eq!(
            row, prior,
            "All non-order fields retain exact historical Source values"
        );
    }
    let after = super::tests::actual_owned_rows(pool, corpus).await;
    assert_eq!(before, after);
    let binding = json!({"observation":"Actual Original HTTP, current same owned data; historical Factory record retained separately","path":path,"predeclared_tie_case":true,"database":database,"configured_Source_database":corpus["database_witness"]["configured_database"],"fixture_UUID_identity_set_sha256":format!("{:x}",Sha256::digest(serde_json::to_vec(&sql_ids).unwrap())),"HTTP_sentinel_raw_body_sha256":format!("{:x}",Sha256::digest(&sentinel)),"fixture_catalogue_count":http_ids.len(),"historical_Factory_body_sha256":case["expected"]["body_sha256"],"current_HTTP_body_sha256":expected["body_sha256"],"current_HTTP_raw_body":String::from_utf8(bytes).unwrap(),"all_historical_non_order_fields_exact":true,"all13tables_before":before,"all13tables_after_Source_HTTP":after,"no_intervening_data_writes":before==after,"credentials_recorded":false,"tie_policy_sha256":format!("{:x}",Sha256::digest(&policy_bytes))});
    (expected, binding)
}
