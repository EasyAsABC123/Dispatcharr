//! Guarded original-network, SQL/Redis and independent native HTTP gates.
//! Original Dispatcharr authors retain full application credit. Exact fixture
//! guards are mandatory; canonical packaged/browser acceptance is separate.
use super::{
    vod_api_owned_read::{ReadError, ReadLimits},
    vod_api_read_component::{ReadRequest, RetainedUiAuthorizer, SqlReads},
    vod_api_sql::Resource,
};
use crate::{AuthConfig, AuthService, Store};
use axum::{
    http::{HeaderMap, HeaderName, HeaderValue},
    response::IntoResponse,
};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, Row};
use std::{net::SocketAddr, sync::Arc, time::Duration};
struct PgOnlyAuth(AuthService);
impl RetainedUiAuthorizer for PgOnlyAuth {
    fn authorize<'a>(
        &'a self,
        peer: SocketAddr,
        headers: HeaderMap,
        guard: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<crate::User, crate::StoreError>> {
        Box::pin(async move {
            let _physical = guard;
            self.0.account_user(peer, headers, false).await
        })
    }
}
async fn fixture() -> (SqlReads, Value) {
    let (reader, source, _) = fixture_limits(ReadLimits::default()).await;
    (reader, source)
}
async fn fixture_limits(limits: ReadLimits) -> (SqlReads, Value, sqlx::PgPool) {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").expect("fresh owned original fixture DB");
    assert!(url.contains("playback-fixture-only"));
    let path = std::env::var("VOD_SOURCE_CORPUS").expect("actual fresh Source corpus");
    let source: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(source["capture_complete"], true);
    assert_eq!(source["provider_trap_attempts"], json!([]));
    assert_eq!(source["fixtures"]["prefix"], "rust-probe-vod-rest-");
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='3s'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    let store = Store::from_pool(pool.clone());
    // No NotificationCache/event sink: all authentication in this component
    // fixture is direct awaited PostgreSQL work under the retained scope. This
    // is not the pending production Redis/cache authorizer implementation.
    let auth = AuthService::new(
        store,
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let reader = if std::env::var("VOD_TEST_CACHED_AUTH").as_deref() == Ok("1") {
        let redis = std::env::var("REDIS_TEST_URL").expect("owned cachedAuth Redis");
        assert!(redis.contains("playback-fixture-only"));
        let cache = crate::NotificationCache::connect(&redis).await.unwrap();
        SqlReads::from_auth(auth.with_notification_cache(cache), limits).unwrap()
    } else {
        SqlReads::new(pool.clone(), Arc::new(PgOnlyAuth(auth)), limits).unwrap()
    };
    (reader, source, pool)
}
fn request(case: &Value) -> Option<ReadRequest> {
    if case["method"] != "GET" {
        return None;
    }
    let (path, query) = case["path"]
        .as_str()?
        .split_once('?')
        .unwrap_or((case["path"].as_str()?, ""));
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if parts.len() < 3 || parts[0] != "api" || parts[1] != "vod" {
        return None;
    }
    let resource = match parts[2] {
        "movies" => Resource::Movie,
        "series" => Resource::Series,
        "episodes" => Resource::Episode,
        _ => return None,
    };
    let detail = match parts.len() {
        3 => None,
        4 => Some(parts[3].parse().ok()?),
        _ => return None,
    };
    let mut headers = HeaderMap::new();
    for (name, value) in case["headers"].as_object()? {
        let name = name
            .strip_prefix("HTTP_")
            .unwrap_or(name)
            .replace('_', "-")
            .to_ascii_lowercase();
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).ok()?,
            HeaderValue::from_str(value.as_str()?).ok()?,
        );
    }
    Some(ReadRequest {
        peer: "127.0.0.1:30000".parse().unwrap(),
        headers,
        resource,
        detail,
        query: url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect(),
        origin: "http://localhost".into(),
        path: path.into(),
    })
}
async fn response(result: Result<bytes::Bytes, ReadError>) -> (u16, Vec<u8>, bool) {
    match result {
        Ok(body) => (200, body.to_vec(), false),
        Err(ReadError::Source(error)) => {
            let response = error.into_response();
            let status = response.status().as_u16();
            (
                status,
                axum::body::to_bytes(response.into_body(), 1024 * 1024)
                    .await
                    .unwrap()
                    .to_vec(),
                false,
            )
        }
        Err(ReadError::Unsupported(message)) => (
            501,
            serde_json::to_vec(&json!({"unsupported":message})).unwrap(),
            true,
        ),
        Err(error) => (
            503,
            serde_json::to_vec(&json!({"component_error":format!("{error:?}")})).unwrap(),
            false,
        ),
    }
}
fn output(name: &str, value: &Value) {
    let directory = std::env::var("VOD_NATIVE_EVIDENCE").unwrap();
    std::fs::write(
        std::path::Path::new(&directory).join(name),
        serde_json::to_vec_pretty(value).unwrap(),
    )
    .unwrap();
}
#[tokio::test]
#[ignore = "requires guarded fresh actual Source PG/HTTP fixture and private corpus"]
async fn ordinary_source_database_rows_permissions_and_serialization() {
    let (reader, source, pool) = fixture_limits(ReadLimits::default()).await;
    let mut records = Vec::new();
    let mut differences = Vec::new();
    let mut unsupported = Vec::new();
    let mut outside = 0;
    for (index, case) in source["cases"]
        .as_array()
        .unwrap()
        .iter()
        .take(463)
        .enumerate()
    {
        let Some(request) = request(case) else {
            outside += 1;
            continue;
        };
        let (status, body, unimplemented) = response(reader.read(request).await).await;
        let expected = &case["expected"];
        let actual_json = serde_json::from_slice::<Value>(&body).ok();
        let hash = format!("{:x}", Sha256::digest(&body));
        let record = json!({"source_index":index,"path":case["path"],"role":case["role"],"method":case["method"],
            "status":status,"body_bytes":body.len(),"body_sha256":hash});
        if unimplemented {
            unsupported.push(record);
            continue;
        }
        if (expected["status"] != status
            || expected["json"] != actual_json.clone().unwrap_or(Value::Null)
            || expected["body_sha256"] != hash)
            && differences.len() < 12
        {
            differences.push(json!({"case":record,"source_status":expected["status"],"source_body_sha256":expected["body_sha256"],"native_json":actual_json,"source_json":expected["json"]}));
        }
        records.push(record);
    }
    output(
        "ordinary-sql-cases.json",
        &json!({"scope":"Original ordinary GET vsNativeSQL component before plan diagnostics; noHTTPheader/fullVODacceptance", "records":records,"differences":differences,"unsupported_not_counted_pass":unsupported,"outside_component_cases":outside}),
    );
    let selection = super::vod_api_query::ordinary(Resource::Movie, None, &[]).unwrap();
    let visible = super::vod_api_projection::visibility(&json!({}), 1).unwrap();
    let page = super::vod_api_pagination::ordinary(&[], 26).unwrap();
    let statement = selection
        .rows(&visible, ReadLimits::default().row_bytes, Some(&page))
        .unwrap();
    let native_plan: Value = statement
        .explain()
        .query()
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get(0)
        .unwrap();
    let inner_rows = statement
        .page_probe()
        .unwrap()
        .query()
        .fetch_all(&pool)
        .await
        .unwrap();
    let inner_ids: Vec<i64> = inner_rows
        .iter()
        .map(|row| {
            row.try_get::<i64, _>("id")
                .unwrap_or_else(|_| i64::from(row.try_get::<i32, _>("id").unwrap()))
        })
        .collect();
    let reference: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_DEFAULT_SQL").unwrap()).unwrap(),
    )
    .unwrap();
    let query = reference["queries"]["movies"]["sql"].as_str().unwrap();
    assert!(query.starts_with("SELECT DISTINCT ") && !query.contains(';') && query.len() < 131072);
    let source_plan: Value = sqlx::query(&format!("EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) {query}"))
        .persistent(false)
        .fetch_one(&pool)
        .await
        .unwrap()
        .try_get(0)
        .unwrap();
    let source_rows = sqlx::query(query)
        .persistent(false)
        .fetch_all(&pool)
        .await
        .unwrap();
    let source_ids: Vec<i64> = source_rows
        .iter()
        .map(|row| {
            row.try_get::<i64, _>(0)
                .unwrap_or_else(|_| i64::from(row.try_get::<i32, _>(0).unwrap()))
        })
        .collect();
    output(
        "ordering-plans.json",
        &json!({"scope":"CapturedSourceFactory default SQL replay on same actualfixture vsNative inner/final plans; no general ordering acceptance",
        "source_reference":reference,"source_replayed_ids":source_ids,"native_inner_page_ids":inner_ids,"source_plan":source_plan,"native_final_plan":native_plan}),
    );
    let drained = reader.shutdown(Duration::from_secs(5)).await;
    let passed = differences.is_empty() && unsupported.is_empty() && records.len() > 150 && drained;
    output(
        "ordinary-sql.json",
        &json!({"scope":"Actual Source view/network corpus vs native ordinary SQL component; no native HTTP/header or fullVOD acceptance",
        "passed":passed,"source_observation":source["observation"],"compared":records.len(),"outside_component_cases":outside,
        "unsupported_not_counted_pass":unsupported,"differences":differences,"records":records,"workers_drained":drained,
        "physical_workers":reader.physical_workers(),"retained_memory_bytes":reader.retained_memory_bytes(),
        "authorization":if std::env::var("VOD_TEST_CACHED_AUTH").as_deref()==Ok("1"){"Real sharedAuthService with original-compatible Redis cache and request-retained scope"}else{"Real sharedAuthService, PostgreSQL only; production retained Redis/cache auth remains open"},
        "known_gaps":["concurrent logo metadata snapshot","JOIN/DISTINCT versus EXISTS tied order outside this fixture","raw-query grammar beyond captured subset","OPTIONS/artwork/provider/category/unified actions"]}),
    );
    assert!(
        passed,
        "ordinary SQL differences/unsupported/resource state; see private ordinary-sql.json"
    );
}

fn standard_movie(source: &Value) -> &Value {
    source["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| {
            case["path"] == "/api/vod/movies/"
                && case["role"] == "standard"
                && case["method"] == "GET"
        })
        .unwrap()
}
async fn active_reads(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND query LIKE '/* rust-vod-ordinary-%'")
        .fetch_one(pool).await.unwrap()
}
async fn wait_active(pool: &sqlx::PgPool, count: i64) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while active_reads(pool).await < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("actual locked native SQL must be witnessed");
}
async fn wait_drained(reader: &SqlReads) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while reader.physical_workers() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("physical query/cleanup drain");
}
#[tokio::test]
#[ignore = "requires guarded realPG; callerabort/blocking SQL/finalclone ownership"]
async fn owned_sql_caller_abort_and_detached_frame_are_bounded() {
    let (reader, source, pool) = fixture_limits(ReadLimits::default()).await;
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *blocker).await.unwrap();
    sqlx::query("LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let one = {
        let reader = reader.clone();
        let req = request(standard_movie(&source)).unwrap();
        tokio::spawn(async move { reader.read(req).await })
    };
    let two = {
        let reader = reader.clone();
        let req = request(standard_movie(&source)).unwrap();
        tokio::spawn(async move { reader.read(req).await })
    };
    wait_active(&pool, 2).await;
    assert_eq!(reader.physical_workers(), 2);
    one.abort();
    let _ = one.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(active_reads(&pool).await, 2);
    assert_eq!(reader.physical_workers(), 2);
    assert!(matches!(
        reader.read(request(standard_movie(&source)).unwrap()).await,
        Err(ReadError::Capacity)
    ));
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();
    drop(blocker);
    let frame = two.await.unwrap().unwrap();
    let clone = frame.clone();
    let length = frame.len();
    drop(frame);
    wait_drained(&reader).await;
    let retained = reader.retained_memory_bytes();
    assert!(retained >= length && retained > 0);
    assert!(reader.shutdown(Duration::from_millis(100)).await);
    assert_eq!(reader.retained_memory_bytes(), retained);
    drop(clone);
    assert_eq!(reader.retained_memory_bytes(), 0);
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(timeout, "3s");
    pool.close().await;
    output(
        "owned-caller-frame.json",
        &json!({"passed":true,"scope":"ActualPG component ownership; not HTTP/app performance",
        "actual_blocked_read_workers":2,"aborted_caller_did_not_release_physical_admission":true,
        "third_request_capacity_before_auth_sql":true,"detached_frame_len":length,"detached_frame_retained_capacity":retained,
        "worker_shutdown_with_held_frame":true,"lastclone_released_memory":true,"pool_statement_timeout_restored":timeout}),
    );
}
#[tokio::test]
#[ignore = "requires guarded realPG; logicaldeadline retains pending physical query"]
async fn owned_sql_logical_deadline_does_not_release_pending_reply() {
    let limits = ReadLimits {
        logical_deadline: Duration::from_millis(150),
        statement_deadline: Duration::from_secs(2),
        ..ReadLimits::default()
    };
    let (reader, source, pool) = fixture_limits(limits).await;
    let mut blocker = pool.acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *blocker).await.unwrap();
    sqlx::query("LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let pending = {
        let reader = reader.clone();
        let req = request(standard_movie(&source)).unwrap();
        tokio::spawn(async move { reader.read(req).await })
    };
    wait_active(&pool, 1).await;
    assert!(matches!(pending.await.unwrap(), Err(ReadError::Deadline)));
    assert_eq!(active_reads(&pool).await, 1);
    assert_eq!(reader.physical_workers(), 1);
    let bytes = reader.retained_memory_bytes();
    assert!(bytes > 0);
    sqlx::query("ROLLBACK")
        .execute(&mut *blocker)
        .await
        .unwrap();
    drop(blocker);
    wait_drained(&reader).await;
    assert_eq!(reader.retained_memory_bytes(), 0);
    assert!(reader.shutdown(Duration::from_millis(100)).await);
    pool.close().await;
    output(
        "owned-logical-timeout.json",
        &json!({"passed":true,"scope":"ActualPG component ownership; no native HTTP/global authcache proof",
        "logical_timeout_ms":150,"physical_statement_timeout_ms":2000,"pending_query_witness_after_logical_timeout":true,
        "retained_bytes_while_pending":bytes,"all_memory_returned_after_physical_rollback":true}),
    );
}

#[tokio::test]
#[ignore = "requires guarded actual Source page/serializer interleaving fixture"]
async fn selected_logo_snapshot_and_fresh_metadata_follow_source_schedule() {
    let (reader, source, pool) = fixture_limits(ReadLimits::default()).await;
    let reference: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_SCHEDULE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(reference["passed"], true);
    let id = reference["logo_id"].as_i64().unwrap();
    let new_url = reference["new_logo_url"].as_str().unwrap().to_owned();
    let name = reference["shadow_name"].as_str().unwrap().to_owned();
    let changed = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let hook = {
        let pool = pool.clone();
        let changed = changed.clone();
        Arc::new(move || -> BoxFuture<'static, Result<(), ReadError>> {
            let pool = pool.clone();
            let changed = changed.clone();
            let new_url = new_url.clone();
            let name = name.clone();
            Box::pin(async move {
                sqlx::query("UPDATE vod_vodlogo SET url=$1 WHERE id=$2")
                    .bind(new_url)
                    .bind(id)
                    .execute(&pool)
                    .await?;
                let inserted:i64=sqlx::query_scalar("INSERT INTO vod_movie(uuid,name,language,is_adult,custom_properties,created_at,updated_at,logo_id) VALUES($1,$2,'',false,'{}'::jsonb,now(),now(),$3) RETURNING id::bigint")
                .bind(uuid::Uuid::new_v4()).bind(name).bind(id).fetch_one(&pool).await?;
                changed.store(inserted, std::sync::atomic::Ordering::Release);
                Ok(())
            })
        })
    };
    let reader = reader.with_page_hook(hook);
    let mut req = request(standard_movie(&source)).unwrap();
    req.query = vec![("page_size".into(), "1".into())];
    let result = reader.read(req).await;
    // Restore owned fixture state before asserting; preserve any failure output.
    sqlx::query("UPDATE vod_vodlogo SET url=$1 WHERE id=$2")
        .bind(reference["old_logo_url"].as_str().unwrap())
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM vod_movie WHERE id=$1 AND name=$2")
        .bind(changed.load(std::sync::atomic::Ordering::Acquire))
        .bind(reference["shadow_name"].as_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let (status, body, unsupported) = response(result).await;
    let hash = format!("{:x}", Sha256::digest(&body));
    let payload = serde_json::from_slice::<Value>(&body).ok();
    let passed = !unsupported
        && status == reference["status"]
        && hash == reference["body_sha256"]
        && payload.as_ref() == Some(&reference["json"]);
    output(
        "metadata-schedule.json",
        &json!({"scope":"OriginalFactory vsNativeSQL same phase interleaving; not native HTTP/fullconcurrency proof",
        "passed":passed,"source_status":reference["status"],"native_status":status,"source_sha256":reference["body_sha256"],"native_sha256":hash,
        "source_json":reference["json"],"native_json":payload,"selected_logo_scalars_from_page":true,"counts_names_fresh_after_page":true}),
    );
    assert!(reader.shutdown(Duration::from_secs(3)).await);
    pool.close().await;
    assert!(
        passed,
        "metadata phase interleaving mismatch; see private metadata-schedule.json"
    );
}

async fn reset_uncategorized(pool: &sqlx::PgPool) {
    let ids: Vec<i64> =
        sqlx::query_scalar("SELECT id::bigint FROM vod_vodcategory WHERE name='Uncategorized'")
            .fetch_all(pool)
            .await
            .unwrap();
    assert!(ids.len() <= 2);
    for table in ["vod_m3umovierelation", "vod_m3useriesrelation"] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE category_id=ANY($1)"
        ))
        .bind(&ids)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, 0);
    }
    sqlx::query("DELETE FROM vod_m3uvodcategoryrelation WHERE category_id=ANY($1)")
        .bind(&ids)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM vod_vodcategory WHERE id=ANY($1) AND name='Uncategorized'")
        .bind(&ids)
        .execute(pool)
        .await
        .unwrap();
}
async fn seed(
    runtime: super::vod_api_owned_read::ReadRuntime,
    pool: sqlx::PgPool,
) -> Result<bytes::Bytes, ReadError> {
    runtime
        .run(0, move |work| async move {
            let mut connection = work.connection(&pool).await?;
            let result = super::vod_api_category_seed::prepare(&work, &mut connection).await;
            let cleanup = connection.finish().await;
            let value = result?;
            cleanup?;
            let mut document = work.document();
            serde_json::to_writer(
                &mut document,
                &json!({"movie":value.movie_category,"series":value.series_category,
            "accounts":value.examined_accounts,"created_links":value.created_links}),
            )
            .map_err(|_| ReadError::Capacity)?;
            Ok(document.finish())
        })
        .await
}
async fn relation_state(pool: &sqlx::PgPool, accounts: &[i64]) -> Vec<Value> {
    sqlx::query("SELECT id::bigint AS id,category_id::bigint AS category_id,m3u_account_id::bigint AS account_id,enabled,custom_properties FROM vod_m3uvodcategoryrelation WHERE m3u_account_id=ANY($1) ORDER BY id")
        .bind(accounts).fetch_all(pool).await.unwrap().iter().map(|row|json!({"id":row.try_get::<i64,_>("id").unwrap(),
            "category":row.try_get::<i64,_>("category_id").unwrap(),"account":row.try_get::<i64,_>("account_id").unwrap(),
            "enabled":row.try_get::<bool,_>("enabled").unwrap(),"properties":row.try_get::<Value,_>("custom_properties").unwrap()})).collect()
}
#[tokio::test]
#[ignore = "requires guarded original category effects and actualPG injectedfailure"]
async fn category_lazy_defaults_partial_failure_and_retained_owner() {
    use super::vod_api_owned_read::ReadRuntime;
    let (reader, source, pool) = fixture_limits(ReadLimits::default()).await;
    assert_eq!(source["initial_effects"]["uncategorized"], json!([]));
    let accounts: Vec<i64> = source["fixtures"]["account_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    reset_uncategorized(&pool).await;
    let original = relation_state(&pool, &accounts).await;
    assert_eq!(original.len(), 9);
    let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
    let response = seed(runtime.clone(), pool.clone()).await.unwrap();
    let ids: Value = serde_json::from_slice(&response).unwrap();
    drop(response);
    let after = relation_state(&pool, &accounts).await;
    assert_eq!(after.len(), 13);
    assert_eq!(&after[..9], original.as_slice());
    let movie = ids["movie"].as_i64().unwrap();
    let series = ids["series"].as_i64().unwrap();
    let old: std::collections::BTreeMap<i64, String> = source["final_effects"]["uncategorized"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row[0].as_i64().unwrap(),
                row[1].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let expected: std::collections::BTreeSet<_> = source["final_effects"]
        ["owned_category_relations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            let category = row[1].as_i64().unwrap();
            (
                old.get(&category)
                    .cloned()
                    .unwrap_or_else(|| format!("original:{category}")),
                row[2].as_i64().unwrap(),
                row[3].as_bool().unwrap(),
            )
        })
        .collect();
    let actual: std::collections::BTreeSet<_> = after
        .iter()
        .map(|row| {
            let category = row["category"].as_i64().unwrap();
            (
                if category == movie {
                    "movie".into()
                } else if category == series {
                    "series".into()
                } else {
                    format!("original:{category}")
                },
                row["account"].as_i64().unwrap(),
                row["enabled"].as_bool().unwrap(),
            )
        })
        .collect();
    assert_eq!(actual, expected);
    let again = seed(runtime.clone(), pool.clone()).await.unwrap();
    drop(again);
    assert_eq!(relation_state(&pool, &accounts).await, after);
    assert_eq!(runtime.physical_workers(), 0);
    assert_eq!(runtime.retained_memory_bytes(), 0);
    // Failure prefix uses the same actualPG trigger condition as Original,
    // adding a delay solely to make in-flight retained ownership observable.
    reset_uncategorized(&pool).await;
    sqlx::query("CREATE FUNCTION public.rust_vod_fixture_fail_series() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.name='Uncategorized' AND NEW.category_type='series' THEN PERFORM pg_sleep(0.35); RAISE EXCEPTION 'owned-vod-series-failure'; END IF; RETURN NEW; END$$").execute(&pool).await.unwrap();
    sqlx::query("CREATE TRIGGER rust_vod_fixture_fail_series BEFORE INSERT ON vod_vodcategory FOR EACH ROW EXECUTE FUNCTION public.rust_vod_fixture_fail_series()").execute(&pool).await.unwrap();
    let pending = {
        let runtime = runtime.clone();
        let pool = pool.clone();
        tokio::spawn(async move { seed(runtime, pool).await })
    };
    tokio::time::timeout(Duration::from_secs(2),async {
        loop {
            let prefix:Vec<String>=sqlx::query_scalar("SELECT category_type FROM vod_vodcategory WHERE name='Uncategorized' ORDER BY category_type").fetch_all(&pool).await.unwrap();
            let active:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND query LIKE 'INSERT INTO vod_vodcategory%'").fetch_one(&pool).await.unwrap();
            if prefix==vec!["movie"]&&active==1{break;}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    pending.abort();
    let _ = pending.await;
    assert_eq!(runtime.physical_workers(), 1);
    assert!(runtime.retained_memory_bytes() > 0);
    tokio::time::timeout(Duration::from_secs(3), async {
        while runtime.physical_workers() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let prefix:Vec<String>=sqlx::query_scalar("SELECT category_type FROM vod_vodcategory WHERE name='Uncategorized' ORDER BY category_type").fetch_all(&pool).await.unwrap();
    let reference: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CATEGORY_FAULT").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(reference["passed"], true);
    assert_eq!(
        serde_json::to_value(&prefix).unwrap(),
        reference["retained_category_types"]
    );
    assert_eq!(runtime.retained_memory_bytes(), 0);
    sqlx::query("DROP TRIGGER rust_vod_fixture_fail_series ON vod_vodcategory")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DROP FUNCTION public.rust_vod_fixture_fail_series()")
        .execute(&pool)
        .await
        .unwrap();
    reset_uncategorized(&pool).await;
    drop(seed(runtime.clone(), pool.clone()).await.unwrap());
    assert!(runtime.shutdown(Duration::from_secs(1)).await);
    assert!(reader.shutdown(Duration::from_secs(1)).await);
    pool.close().await;
    output(
        "category-effects.json",
        &json!({"passed":true,"scope":"Actual Source normal/categoryPGfailure prefix vs lowlevel nativehelper; no nativeHTTP/categoryroute acceptance",
        "owned_relations_before":9,"owned_relations_after":13,"source_effects_equal_by_semantic_category_kind":true,
        "normalization":"Uncategorized row IDs differ after explicit fixture reset; kinds/account/enabled preserved; original relations/properties exact",
        "existing_defaults_unchanged":true,"idempotent_repeat":true,"source_failure":reference["failure"],
        "retained_category_prefix":prefix,"aborted_caller_kept_owner_through_pending_insert_and_failure_cleanup":true,
        "all_memory_and_workers_returned":true,"restored_normal_fixture_state":true}),
    );
}

#[tokio::test]
#[ignore = "requires guarded originalPG/Redis; real cachedAuth logical fallback retains VOD slot"]
async fn cached_authorizer_fallback_retains_request_until_physical_redis_reply() {
    let (unused, source, pool) = fixture_limits(ReadLimits::default()).await;
    assert!(unused.shutdown(Duration::from_secs(1)).await);
    drop(unused);
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let cache = crate::NotificationCache::connect(&url).await.unwrap();
    let auth = AuthService::new(
        Store::from_pool(pool.clone()),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let reader = SqlReads::from_auth(auth, ReadLimits::default()).unwrap();
    let mut control = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(4000)
        .arg("ALL")
        .query_async(&mut control)
        .await
        .unwrap();
    let frame = reader
        .read(request(standard_movie(&source)).unwrap())
        .await
        .unwrap();
    // Cache's2s logical failure falls back to actualPG policy. The successful
    // data read must still retain the request lease for its pending cache GET.
    assert_eq!(reader.physical_workers(), 1);
    assert_eq!(cache.physical_operations(), 1);
    let held = reader.retained_memory_bytes();
    assert!(held > ReadLimits::default().response_bytes);
    tokio::time::timeout(Duration::from_secs(4), async {
        while cache.physical_operations() != 0 || reader.physical_workers() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let frame_capacity = reader.retained_memory_bytes();
    assert!(frame_capacity >= frame.len() && frame_capacity < held);
    drop(frame);
    assert_eq!(reader.retained_memory_bytes(), 0);
    assert!(reader.shutdown(Duration::from_secs(1)).await);
    pool.close().await;
    output(
        "cached-auth-owner.json",
        &json!({"passed":true,"scope":"Real cached sharedAuthService UI policy, actual delayedRedis and PG fallback; no HTTP/app acceptance",
        "successful_pg_fallback_kept_vod_worker_until_cache_reply":true,"retained_bytes_while_cache_pending":held,
        "frame_capacity_after_worker_drain":frame_capacity,"all_memory_returned":true,"no_cache_or_auth_policy_bypass":true}),
    );
}

#[tokio::test]
#[ignore = "actual original HTTP corpus + isolated native TCP HTTP/PG/Redis comparison"]
async fn ordinary_native_http_methods_headers_and_permissions() {
    assert_eq!(std::env::var("VOD_TEST_NATIVE_HTTP").as_deref(), Ok("1"));
    let (unused, source, pool) = fixture_limits(ReadLimits::default()).await;
    assert!(unused.shutdown(Duration::from_secs(1)).await);
    drop(unused);
    let cache = crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let auth = AuthService::new(
        Store::from_pool(pool.clone()),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let gateway = super::VodReads::new(auth, ReadLimits::default()).unwrap();
    let reader = gateway.reader.clone();
    let router = gateway.router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let mut records = Vec::new();
    let mut differences = Vec::new();
    let mut excluded = Vec::new();
    for (index, case) in source["cases"].as_array().unwrap().iter().enumerate() {
        let path = case["path"].as_str().unwrap();
        let bare = path.split('?').next().unwrap();
        let parts: Vec<_> = bare.trim_matches('/').split('/').collect();
        if parts.len() < 3
            || parts.len() > 4
            || !matches!(parts[2], "movies" | "series" | "episodes")
            || !bare.ends_with('/')
        {
            continue;
        }
        let mut request = client.request(
            case["method"].as_str().unwrap().parse().unwrap(),
            format!("http://{address}{path}"),
        );
        for (name, value) in case["headers"].as_object().unwrap() {
            request = request.header(
                name.strip_prefix("HTTP_").unwrap_or(name).replace('_', "-"),
                value.as_str().unwrap(),
            );
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.unwrap();
        if status == 501 {
            excluded.push(json!({"index":index,"path":path,"reason":"Explicit unimplemented representation; not a parity pass","body":String::from_utf8_lossy(&body)}));
            continue;
        }
        let expected = &case["expected"];
        let hash = format!("{:x}", Sha256::digest(&body));
        let mut header_diffs = Vec::new();
        for header in ["Content-Length", "Allow", "Vary", "WWW-Authenticate"] {
            let actual = headers.get(header).and_then(|v| v.to_str().ok());
            let wanted = expected["selected_headers"][header].as_str();
            if actual != wanted {
                header_diffs.push(json!({"header":header,"source":wanted,"native":actual}));
            }
        }
        let mime = headers.get("content-type").and_then(|v| v.to_str().ok());
        let mismatch = status != expected["status"].as_u64().unwrap() as u16
            || hash != expected["body_sha256"]
            || mime != expected["content_type"].as_str()
            || !header_diffs.is_empty();
        if mismatch && differences.len() < 20 {
            differences.push(json!({"index":index,"path":path,"role":case["role"],"method":case["method"],"source_status":expected["status"],"native_status":status,"native_body":String::from_utf8_lossy(&body),"source_hash":expected["body_sha256"],"native_hash":hash,"header_differences":header_diffs}));
        }

        records.push(json!({"index":index,"status":status,"bytes":body.len(),"hash":hash}));
    }
    let source_burst: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_BURST").unwrap()).unwrap())
            .unwrap();
    assert_eq!(source_burst["passed"], true);
    let barrier = Arc::new(tokio::sync::Barrier::new(11));
    let epoch = tokio::time::Instant::now();
    let mut jobs = Vec::new();
    for index in 0..10 {
        let client = client.clone();
        let barrier = barrier.clone();
        let case = standard_movie(&source).clone();
        jobs.push(tokio::spawn(async move {
            let mut request=client.get(format!("http://{address}/api/vod/movies/"));
            for(name,value)in case["headers"].as_object().unwrap(){request=request.header(name.strip_prefix("HTTP_").unwrap_or(name).replace('_',"-"),value.as_str().unwrap());}
            barrier.wait().await;let began=epoch.elapsed().as_secs_f64();let response=request.send().await.unwrap();let status=response.status().as_u16();let body=response.bytes().await.unwrap();
            json!({"index":index,"started_seconds":began,"status":status,"body_bytes":body.len(),"body_sha256":format!("{:x}",Sha256::digest(&body))})
        }));
    }
    barrier.wait().await;
    let mut burst = Vec::new();
    for job in jobs {
        burst.push(job.await.unwrap());
    }
    let source_hash = &source_burst["records"][0]["body_sha256"];
    let burst_pass = burst
        .iter()
        .all(|row| row["status"] == 200 && &row["body_sha256"] == source_hash);
    output(
        "http-burst.json",
        &json!({"passed":burst_pass,"scope":"Actual original/native simultaneous10 ordinary MovieList reads; barrier launch, no staggering; not performance", "source":source_burst,"native_records":burst}),
    );
    let drained = gateway.shutdown(Duration::from_secs(5)).await;
    server.abort();
    let _ = server.await;
    pool.close().await;
    let passed = differences.is_empty() && records.len() > 250 && drained && burst_pass;
    output(
        "ordinary-http.json",
        &json!({"passed":passed,"scope":"Actual original network vs independent native TCP HTTP peer; app security/CORS middleware and canonical packaged/React gates excluded", "records":records,"differences":differences,"explicit_representation_exclusions":excluded,"workers_drained":drained,"remaining_memory":reader.retained_memory_bytes()}),
    );
    assert!(passed, "HTTP differences; see ordinary-http.json");
}

#[tokio::test]
#[ignore = "actual owned PG/Redis delayed cached-auth and independent HTTP error-frame ownership"]
async fn http_frames_and_cancelled_cached_authorization_keep_admission() {
    assert_eq!(std::env::var("VOD_TEST_NATIVE_HTTP").as_deref(), Ok("1"));
    use futures_util::StreamExt;
    let (unused, source, pool) = fixture_limits(ReadLimits::default()).await;
    assert!(unused.shutdown(Duration::from_secs(1)).await);
    drop(unused);
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let cache = crate::NotificationCache::connect(&url).await.unwrap();
    let auth = AuthService::new(
        Store::from_pool(pool.clone()),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let reader = SqlReads::from_auth(auth.clone(), ReadLimits::default()).unwrap();
    let gate = super::vod_api_http::HttpGate::new(ReadLimits {
        logical_deadline: Duration::from_millis(150),
        ..ReadLimits::default()
    })
    .unwrap();
    let slots = gate.slots.clone();
    let peer = "127.0.0.1:30000".parse().unwrap();
    let anonymous = || {
        axum::extract::Request::builder()
            .uri("/api/vod/movies/")
            .header("host", "localhost")
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let first = super::vod_api_http::respond(
        reader.clone(),
        auth.clone(),
        gate.clone(),
        peer,
        Resource::Movie,
        false,
        anonymous(),
    )
    .await;
    assert_eq!(first.status(), 401);
    let mut stream = first.into_body().into_data_stream();
    let data = stream.next().await.unwrap().unwrap();
    drop(stream);
    let clone = data.clone();
    drop(data);
    assert_eq!(slots.available_permits(), 1);
    let second = super::vod_api_http::respond(
        reader.clone(),
        auth.clone(),
        gate.clone(),
        peer,
        Resource::Movie,
        false,
        anonymous(),
    )
    .await;
    assert_eq!(second.status(), 401);
    assert_eq!(slots.available_permits(), 0);
    let queued_memory = gate.input.available_permits();
    let authenticated = || {
        let mut request = axum::extract::Request::builder()
            .uri("/api/vod/movies/")
            .body(axum::body::Body::empty())
            .unwrap();
        *request.headers_mut() = self::request(standard_movie(&source)).unwrap().headers;
        request
    };
    let pending = authenticated();
    let queued_reader = reader.clone();
    let queued_auth = auth.clone();
    let queued_gate = gate.clone();
    let queued = tokio::spawn(async move {
        super::vod_api_http::respond(
            queued_reader,
            queued_auth,
            queued_gate,
            peer,
            Resource::Movie,
            false,
            pending,
        )
        .await
    });
    tokio::time::timeout(Duration::from_millis(100), async {
        while gate.waiters.available_permits() != 255 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(gate.input.available_permits() < queued_memory);
    assert_eq!(reader.physical_workers(), 0);
    assert_eq!(cache.physical_operations(), 0);
    queued.abort();
    let _ = queued.await;
    assert_eq!(gate.waiters.available_permits(), 256);
    assert_eq!(gate.input.available_permits(), queued_memory);
    let rejected = super::vod_api_http::respond(
        reader.clone(),
        auth.clone(),
        gate.clone(),
        peer,
        Resource::Movie,
        false,
        authenticated(),
    )
    .await;
    assert_eq!(rejected.status(), 503);
    drop(rejected);
    assert_eq!(slots.available_permits(), 0);
    assert_eq!(reader.physical_workers(), 0);
    assert_eq!(cache.physical_operations(), 0);
    assert_eq!(gate.input.available_permits(), queued_memory);
    let pending = authenticated();
    let queued_reader = reader.clone();
    let queued_auth = auth.clone();
    let queued_gate = gate.clone();
    let queued = tokio::spawn(async move {
        super::vod_api_http::respond(
            queued_reader,
            queued_auth,
            queued_gate,
            peer,
            Resource::Movie,
            false,
            pending,
        )
        .await
    });
    tokio::time::timeout(Duration::from_millis(100), async {
        while gate.waiters.available_permits() != 255 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    gate.slots.close();
    assert_eq!(queued.await.unwrap().status(), 503);
    assert_eq!(reader.physical_workers(), 0);
    assert_eq!(gate.waiters.available_permits(), 256);
    assert_eq!(gate.input.available_permits(), queued_memory);
    drop(clone);
    assert_eq!(slots.available_permits(), 1);
    drop(second);
    assert_eq!(slots.available_permits(), 2);
    let gate = super::vod_api_http::HttpGate::new(ReadLimits {
        logical_deadline: Duration::from_millis(150),
        ..ReadLimits::default()
    })
    .unwrap();
    let slots = gate.slots.clone();
    let input = request(standard_movie(&source)).unwrap();
    let mut control = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(4000)
        .arg("ALL")
        .query_async(&mut control)
        .await
        .unwrap();
    let mut pending = axum::extract::Request::builder()
        .uri("/api/vod/movies/")
        .body(axum::body::Body::empty())
        .unwrap();
    *pending.headers_mut() = input.headers;
    let held_reader = reader.clone();
    let held_auth = auth.clone();
    let held_slots = gate.clone();
    let task = tokio::spawn(async move {
        super::vod_api_http::respond(
            held_reader,
            held_auth,
            held_slots,
            peer,
            Resource::Movie,
            false,
            pending,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while cache.physical_operations() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(reader.physical_workers(), 1);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(cache.physical_operations(), 1);
    assert_eq!(slots.available_permits(), 1);
    assert_eq!(reader.physical_workers(), 1);
    tokio::time::timeout(Duration::from_secs(4), async {
        while cache.physical_operations() != 0
            || slots.available_permits() != 2
            || reader.physical_workers() != 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(reader.retained_memory_bytes(), 0);
    assert!(reader.shutdown(Duration::from_secs(1)).await);
    pool.close().await;
    output(
        "http-ownership.json",
        &json!({"passed":true,"scope":"Actual independent HTTP body frames and direct handler abort with realPG/Redis; TCP representation proof is separate ordinary-http.json",
        "unauthorized_error_frame_lastclone_keeps_slot":true,"expired_waiter_did_not_authenticate_or_start_SQL":true,"cancelled_waiter_returned_input_and_queue_lease":true,"shutdown_woke_queued_request_without_SQL":true,"cancelled_handler_keeps_http_and_vod_slots_through_cache_logical_timeout":true,
        "all_slots_and_memory_return_after_actual_reply":true}),
    );
}
