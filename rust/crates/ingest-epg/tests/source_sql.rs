//! Actual original parse_programs_for_source file/SQL component differential.
//! Application and algorithm credit belongs entirely to Dispatcharr authors.
use base64::Engine;
use chrono::{DateTime, Utc};
use dispatcharr_ingest_epg::{
    EpgImporter, ImportConfig, ImportRequest, SwapPolicy, SwapPolicyContext, SwapPolicyFuture,
    SwapPolicyProvider,
};
use dispatcharr_store::{EpgHistoryPolicy, Store};
use serde_json::{json, Value};
use sqlx::Row;
use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
fn value_time(value: &Value) -> DateTime<Utc> {
    value.as_str().unwrap().parse().unwrap()
}
async fn rows(pool: &sqlx::PgPool, ids: &[i64]) -> Vec<Value> {
    sqlx::query("SELECT id,epg_id,start_time,end_time,title,sub_title,description,tvg_id,program_id,custom_properties FROM epg_programdata WHERE epg_id=ANY($1)")
        .bind(ids).fetch_all(pool).await.unwrap().into_iter().map(|row|json!({
            "id":row.get::<i64,_>("id"),"epg_id":row.get::<i64,_>("epg_id"),
            "start_time":row.get::<DateTime<Utc>,_>("start_time").to_rfc3339(),"end_time":row.get::<DateTime<Utc>,_>("end_time").to_rfc3339(),
            "title":row.get::<String,_>("title"),"sub_title":row.get::<Option<String>,_>("sub_title"),
            "description":row.get::<Option<String>,_>("description"),"tvg_id":row.get::<Option<String>,_>("tvg_id"),
            "program_id":row.get::<Option<String>,_>("program_id"),"custom_properties":row.get::<Option<Value>,_>("custom_properties"),
        })).collect()
}
fn normalize(mut rows: Vec<Value>, before: &[Value]) -> Vec<String> {
    let existing: HashSet<_> = before
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect();
    for row in &mut rows {
        if !existing.contains(&row["id"].as_i64().unwrap()) {
            row["id"] = json!("new");
        }
    }
    let mut normalized: Vec<_> = rows.into_iter().map(|row| row.to_string()).collect();
    normalized.sort();
    normalized
}
struct FileFixture(PathBuf);
impl Drop for FileFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
struct Guard(Arc<AtomicBool>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[tokio::test]
#[ignore = "requires actual original disposable file-to-guide oracle"]
async fn original_file_import_rows_skips_errors_and_retention() {
    assert_eq!(
        std::env::var("EPG_IMPORT_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let captured: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_IMPORT_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    let store = Store::connect(&database, 8).await.unwrap();
    let service = EpgImporter::new(
        store.clone(),
        ImportConfig {
            batch_rows: 3,
            queue_batches: 1,
            insert_batch_rows: 2,
            ..ImportConfig::default()
        },
    )
    .unwrap();
    let cases = captured["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 15);
    for (index, case) in cases.iter().enumerate() {
        if case["name"] == "policy-clock-change" || case["name"] == "cached-policy-clock-change" {
            continue;
        }
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-epg-import-sql-{}-{index}",
            std::process::id()
        ));
        let file = FileFixture(path.clone());
        if case["file_exists"].as_bool().unwrap() {
            std::fs::write(
                &path,
                base64::engine::general_purpose::STANDARD
                    .decode(case["xml_base64"].as_str().unwrap())
                    .unwrap(),
            )
            .unwrap();
        }
        let before = case["before"].as_array().unwrap();
        let expected = case["expected"].as_array().unwrap();
        let ids = case["epg_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect::<Vec<_>>();
        let dropped = Arc::new(AtomicBool::new(false));
        let policy: EpgHistoryPolicy = serde_json::from_value(case["policy"].clone()).unwrap();
        let request = ImportRequest {
            source_id: case["source_id"].as_i64().unwrap(),
            path,
            now: value_time(&case["now"]),
            policy,
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let forbidden = Arc::new(ForbiddenPolicy(std::sync::atomic::AtomicUsize::new(0)));
        let prove_no_policy = case["parsed"].as_u64() == Some(0)
            || case["name"] == "no-mappings-no-file"
            || case["passed"] == false;
        let result = if prove_no_policy {
            service
                .import_file_with_swap_policy(
                    request,
                    Arc::new(Guard(dropped.clone())),
                    forbidden.clone(),
                )
                .await
        } else {
            service
                .import_file(request, Arc::new(Guard(dropped.clone())))
                .await
        };
        assert_eq!(
            forbidden.0.load(Ordering::Acquire),
            0,
            "unexpected policy I/O on empty/failed catalogue {}",
            case["name"]
        );
        assert_eq!(
            result.is_ok(),
            case["passed"].as_bool().unwrap(),
            "case {}: {result:?}",
            case["name"]
        );
        if let Ok(report) = result {
            if let Some(count) = case["parsed"].as_u64() {
                assert_eq!(report.parsed_programmes, count, "parsed {}", case["name"]);
            }
            if let Some(count) = case["channels"].as_u64() {
                assert_eq!(
                    report.channels_with_programmes as u64, count,
                    "channels {}",
                    case["name"]
                );
            }
            if let Some(count) = case["skipped_unmapped"].as_u64() {
                assert_eq!(report.skipped_unmapped, count, "skips {}", case["name"]);
            }
            assert_eq!(
                report.no_mapped_channels,
                case["name"] == "no-mappings-no-file"
            );
        }
        assert_eq!(
            normalize(rows(store.pool(), &ids).await, before),
            normalize(expected.clone(), before),
            "stored rows {}",
            case["name"]
        );
        assert!(
            dropped.load(Ordering::Acquire),
            "retained physical guard {}",
            case["name"]
        );
        assert_eq!(service.physical_jobs(), 0);
        drop(file);
    }
    assert!(service.shutdown(Duration::from_secs(2)).await);
    physical_fault_scenarios(&captured, &database).await;
}

/// A disposable-only DDL hook puts an advisory gate on the private staging
/// INSERT. This deterministically blocks actual physical SQL after footer
/// validation, while the bounded parser queue fills. It changes no live rows.
async fn physical_fault_scenarios(captured: &Value, database: &str) {
    use dispatcharr_ingest_epg::ImportError;
    let admin = sqlx::PgPool::connect(database).await.unwrap();
    sqlx::query("CREATE FUNCTION public.rust_epg_stage_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_lock(724060411); PERFORM pg_advisory_unlock(724060411); RETURN NEW; END $$").execute(&admin).await.unwrap();
    sqlx::query("CREATE FUNCTION public.rust_epg_stage_ddl_gate() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('application_name') LIKE 'rust-epg-fault-%' AND to_regclass('pg_temp.epg_program_staging') IS NOT NULL THEN EXECUTE 'CREATE TRIGGER rust_epg_private_gate BEFORE INSERT ON pg_temp.epg_program_staging FOR EACH ROW EXECUTE FUNCTION public.rust_epg_stage_gate()'; END IF; END $$").execute(&admin).await.unwrap();
    sqlx::query("CREATE EVENT TRIGGER rust_epg_private_stage_gate ON ddl_command_end WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION public.rust_epg_stage_ddl_gate()").execute(&admin).await.unwrap();
    let initial_case = &captured["cases"][0];
    let initial_ids = initial_case["epg_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect::<Vec<_>>();
    for mode in [
        "mutation",
        "overflow",
        "cancel",
        "policy-clock",
        "cached-policy-clock",
        "provider-cancel",
    ] {
        let case = if mode == "policy-clock" || mode == "cached-policy-clock" {
            captured["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| {
                    row["name"]
                        == if mode == "cached-policy-clock" {
                            "cached-policy-clock-change"
                        } else {
                            "policy-clock-change"
                        }
                })
                .unwrap()
        } else {
            initial_case
        };
        let ids = if mode == "policy-clock" || mode == "cached-policy-clock" {
            case["epg_ids"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect::<Vec<_>>()
        } else {
            initial_ids.clone()
        };
        let label = format!("rust-epg-fault-{mode}");
        let store = Store::connect(
            &format!(
                "{database}{}application_name={label}",
                if database.contains('?') { "&" } else { "?" }
            ),
            4,
        )
        .await
        .unwrap();
        let service = EpgImporter::new(
            store.clone(),
            ImportConfig {
                max_jobs: 1,
                queue_batches: 1,
                batch_rows: 1,
                batch_bytes: 65536,
                max_programmes: if mode == "overflow" { 2 } else { 1000 },
                ..ImportConfig::default()
            },
        )
        .unwrap();
        let before = rows(&admin, &ids).await;
        let file = FileFixture(std::env::temp_dir().join(format!(
            "dispatcharr-epg-fault-{}-{mode}",
            std::process::id()
        )));
        let programme="<programme channel=\"mapped\" start=\"20261109110000 +0000\" stop=\"20261109120000 +0000\"><title>Staged, never swapped</title></programme>";
        let payload = if mode == "policy-clock" || mode == "cached-policy-clock" {
            base64::engine::general_purpose::STANDARD
                .decode(case["xml_base64"].as_str().unwrap())
                .unwrap()
        } else {
            format!("<tv>{}</tv>", programme.repeat(100)).into_bytes()
        };
        std::fs::write(&file.0, payload).unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let request = ImportRequest {
            source_id: case["source_id"].as_i64().unwrap(),
            path: file.0.clone(),
            now: value_time(&case["now"]),
            policy: serde_json::from_value(case["policy"].clone()).unwrap(),
            cancelled: cancelled.clone(),
        };
        let mut blocker = admin.acquire().await.unwrap();
        sqlx::query("SELECT pg_advisory_lock(724060411)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let provider = Arc::new(LatePolicy {
            value: std::sync::Mutex::new(SwapPolicy {
                now: value_time(&case["now"]),
                history: serde_json::from_value(case["policy"].clone()).unwrap(),
            }),
            calls: std::sync::atomic::AtomicUsize::new(0),
            paused: mode == "provider-cancel",
            entered: AtomicBool::new(false),
            released: tokio::sync::Notify::new(),
        });
        let cached_clock = Arc::new(std::sync::Mutex::new(value_time(&case["now"])));
        let clock_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut redis_admin = if mode == "cached-policy-clock" {
            Some(
                redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
                    .unwrap()
                    .get_multiplexed_async_connection()
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        if let Some(client) = redis_admin.as_mut() {
            sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('epg_settings','Cached file policy fixture',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .bind(sqlx::types::Json(json!({"epg_history_min_days":1,"epg_history_max_days":30}))).execute(&admin).await.unwrap();
            let _: String = redis::cmd("SET")
                .arg(":1:coresettings:group:epg_settings")
                .arg(
                    dispatcharr_store::encode_source_cache(
                        &json!({"epg_history_min_days":1,"epg_history_max_days":30}),
                    )
                    .unwrap(),
                )
                .arg("EX")
                .arg(300)
                .query_async(client)
                .await
                .unwrap();
        }
        let policy_provider: Arc<dyn SwapPolicyProvider> = if mode == "cached-policy-clock" {
            let clock = cached_clock.clone();
            let reads = clock_reads.clone();
            Arc::new(
                dispatcharr_ingest_epg::cache_policy::CachedSwapPolicy::with_clock(
                    store.clone(),
                    Some(std::env::var("REDIS_TEST_URL").unwrap()),
                    Arc::new(move || {
                        reads.fetch_add(1, Ordering::AcqRel);
                        *clock.lock().unwrap()
                    }),
                ),
            )
        } else {
            provider.clone()
        };
        let engine = service.clone();
        let physical_guard = Arc::new(Guard(dropped.clone()));
        let task = tokio::spawn(async move {
            engine
                .import_file_with_swap_policy(request, physical_guard, policy_provider)
                .await
        });
        tokio::time::timeout(Duration::from_secs(5),async{
            loop{
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'INSERT INTO epg_program_staging%')")
                    .bind(&label).fetch_one(&admin).await.unwrap();
                if waiting{break;}tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert_eq!(service.physical_jobs(), 1);
        assert!(!dropped.load(Ordering::Acquire));
        assert_eq!(
            provider.calls.load(Ordering::Acquire),
            0,
            "provider ran before staging completed"
        );
        assert_eq!(
            clock_reads.load(Ordering::Acquire),
            0,
            "cached clock sampled during parsing"
        );
        if mode == "policy-clock" || mode == "cached-policy-clock" {
            *provider.value.lock().unwrap() = SwapPolicy {
                now: value_time(&case["late_swap_policy"]["now"]),
                history: serde_json::from_value(case["late_swap_policy"]["policy"].clone())
                    .unwrap(),
            };
        }
        if mode == "cached-policy-clock" {
            *cached_clock.lock().unwrap() = value_time(&case["late_swap_policy"]["now"]);
            let _: String = redis::cmd("SET")
                .arg(":1:coresettings:group:epg_settings")
                .arg(
                    dispatcharr_store::encode_source_cache(
                        &json!({"epg_history_min_days":0,"epg_history_max_days":0}),
                    )
                    .unwrap(),
                )
                .arg("EX")
                .arg(300)
                .query_async(redis_admin.as_mut().unwrap())
                .await
                .unwrap();
        }
        if mode == "mutation" {
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&file.0)
                .unwrap()
                .write_all(b"incomplete replacement tail")
                .unwrap();
        }
        if mode == "cancel" {
            let rejected = Arc::new(AtomicBool::new(false));
            let refusal = service
                .import_file(
                    ImportRequest {
                        source_id: case["source_id"].as_i64().unwrap(),
                        path: file.0.clone(),
                        now: value_time(&case["now"]),
                        policy: serde_json::from_value(case["policy"].clone()).unwrap(),
                        cancelled: Arc::new(AtomicBool::new(false)),
                    },
                    Arc::new(Guard(rejected.clone())),
                )
                .await;
            assert!(matches!(refusal, Err(ImportError::Busy)));
            assert!(rejected.load(Ordering::Acquire));
            task.abort();
            wait_caller_cancellation(&cancelled).await;
            assert!(!service.shutdown(Duration::from_millis(30)).await);
            assert!(!dropped.load(Ordering::Acquire));
        }
        sqlx::query("SELECT pg_advisory_unlock(724060411)")
            .execute(&mut *blocker)
            .await
            .unwrap();
        if mode == "provider-cancel" {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !provider.entered.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            task.abort();
            wait_caller_cancellation(&cancelled).await;
            assert!(!service.shutdown(Duration::from_millis(30)).await);
            assert!(!dropped.load(Ordering::Acquire));
            assert_eq!(service.physical_jobs(), 1);
            assert_eq!(
                rows(&admin, &ids).await,
                before,
                "blocked policy changed live rows"
            );
            provider.released.notify_one();
        }
        if mode == "cancel" || mode == "provider-cancel" {
            let _ = task.await;
        } else {
            let result = tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
            if mode == "mutation" {
                assert!(
                    matches!(result, Err(ImportError::FileChanged)),
                    "{result:?}"
                );
            } else if mode == "overflow" {
                assert!(
                    matches!(result, Err(ImportError::Capacity(_))),
                    "{result:?}"
                );
            } else {
                assert!(result.is_ok(), "{result:?}");
                if mode == "cached-policy-clock" {
                    assert_eq!(clock_reads.load(Ordering::Acquire), 1);
                } else {
                    assert_eq!(provider.calls.load(Ordering::Acquire), 1);
                }
            }
        }
        assert!(
            service.shutdown(Duration::from_secs(5)).await,
            "physical job did not drain: {mode}"
        );
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(service.physical_jobs(), 0);
        if mode == "policy-clock" || mode == "cached-policy-clock" {
            assert_eq!(
                normalize(rows(&admin, &ids).await, case["before"].as_array().unwrap()),
                normalize(
                    case["expected"].as_array().unwrap().clone(),
                    case["before"].as_array().unwrap()
                ),
                "late clock/policy differs from Source"
            );
        } else {
            assert_eq!(
                rows(&admin, &ids).await,
                before,
                "{mode} changed live programme rows"
            );
            assert_eq!(
                provider.calls.load(Ordering::Acquire),
                usize::from(mode == "provider-cancel")
            );
        }
        drop(file);
    }
    sqlx::query("DROP EVENT TRIGGER rust_epg_private_stage_gate")
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION public.rust_epg_stage_ddl_gate()")
        .execute(&admin)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION public.rust_epg_stage_gate() CASCADE")
        .execute(&admin)
        .await
        .unwrap();
}

struct LatePolicy {
    value: std::sync::Mutex<SwapPolicy>,
    calls: std::sync::atomic::AtomicUsize,
    paused: bool,
    entered: AtomicBool,
    released: tokio::sync::Notify,
}
impl SwapPolicyProvider for LatePolicy {
    fn resolve<'a>(&'a self, context: SwapPolicyContext) -> SwapPolicyFuture<'a> {
        Box::pin(async move {
            context.ensure_active()?;
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.entered.store(true, Ordering::Release);
            if self.paused {
                self.released.notified().await;
            }
            context.ensure_active()?;
            let value = *self.value.lock().unwrap();
            Ok(value)
        })
    }
}

struct ForbiddenPolicy(std::sync::atomic::AtomicUsize);
impl SwapPolicyProvider for ForbiddenPolicy {
    fn resolve<'a>(&'a self, _context: SwapPolicyContext) -> SwapPolicyFuture<'a> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Box::pin(async { Err(dispatcharr_ingest_epg::ImportError::Parser) })
    }
}

async fn wait_caller_cancellation(flag: &AtomicBool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !flag.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("dropping the caller failed to request cancellation");
}
