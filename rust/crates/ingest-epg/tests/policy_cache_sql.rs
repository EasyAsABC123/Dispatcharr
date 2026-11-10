//! Actual Source cached history policy, original Django codec and physical guards.
use base64::Engine;
use chrono::{DateTime, Utc};
use dispatcharr_ingest_epg::{
    cache_policy::CachedSwapPolicy, SwapPolicyContext, SwapPolicyProvider,
};
use dispatcharr_store::{decode_source_cache, encode_source_cache, Store};
use redis::aio::MultiplexedConnection;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
const GROUP: &str = ":1:coresettings:group:epg_settings";
const VERSION: &str = ":1:coresettings:groupver:epg_settings";
struct Guard(Arc<AtomicBool>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
async fn command<T: redis::FromRedisValue>(
    connection: &mut MultiplexedConnection,
    cmd: redis::Cmd,
) -> T {
    cmd.query_async(connection).await.unwrap()
}
async fn set(connection: &mut MultiplexedConnection, key: &str, value: &Value) {
    let mut cmd = redis::cmd("SET");
    cmd.arg(key)
        .arg(encode_source_cache(value).unwrap())
        .arg("EX")
        .arg(if key == GROUP { 123 } else { 300 });
    let _: String = command(connection, cmd).await;
}
async fn read(connection: &mut MultiplexedConnection, key: &str) -> (String, Option<Vec<u8>>, i64) {
    let mut kind = redis::cmd("TYPE");
    kind.arg(key);
    let kind: String = command(connection, kind).await;
    let raw = if kind == "string" {
        let mut get = redis::cmd("GET");
        get.arg(key);
        command(connection, get).await
    } else {
        None
    };
    let mut ttl = redis::cmd("TTL");
    ttl.arg(key);
    (kind, raw, command(connection, ttl).await)
}
async fn database(pool: &sqlx::PgPool, case: &Value) {
    if case["db_present"] == true {
        sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('epg_settings','Native isolated policy fixture',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value").bind(sqlx::types::Json(&case["db"])).execute(pool).await.unwrap();
    } else {
        sqlx::query("DELETE FROM core_coresettings WHERE key='epg_settings'")
            .execute(pool)
            .await
            .unwrap();
    }
}
async fn seed(connection: &mut MultiplexedConnection, case: &Value) {
    let mut del = redis::cmd("DEL");
    del.arg(GROUP).arg(VERSION);
    let _: i64 = command(connection, del).await;
    if case["cached_present"] == true {
        set(connection, GROUP, &case["cached"]).await;
    }
    if case["version_before_present"] == true {
        set(connection, VERSION, &case["version_before"]).await;
    }
    let key = match case["fault"].as_str() {
        Some("group-hash") => Some(GROUP),
        Some("version-hash") => Some(VERSION),
        _ => None,
    };
    if let Some(key) = key {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key).arg("field").arg("value");
        let _: i64 = command(connection, cmd).await;
    }
    if case["fault"] == "malformed" {
        let mut cmd = redis::cmd("SET");
        cmd.arg(GROUP).arg(b"not-a-pickle".as_slice());
        let _: String = command(connection, cmd).await;
    }
}
#[tokio::test]
#[ignore = "requires actual original disposable cached history policy oracle"]
async fn original_cached_history_policy_and_version_fence() {
    assert_eq!(
        std::env::var("EPG_POLICY_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("EPG_POLICY_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 18);
    let db = std::env::var("DATABASE_TEST_URL").unwrap();
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let admin = sqlx::PgPool::connect(&db).await.unwrap();
    let mut connection = redis::Client::open(redis.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let now: DateTime<Utc> = "2026-11-09T00:00:00Z".parse().unwrap();
    let mut interop = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        database(&admin, case).await;
        seed(&mut connection, case).await;
        let label = format!("rust-epg-policy-{index}");
        let uri = format!(
            "{db}{}application_name={label}",
            if db.contains('?') { "&" } else { "?" }
        );
        let store = Store::connect(&uri, 4).await.unwrap();
        let cache_uri = match case["fault"].as_str() {
            Some("skip") => None,
            Some("auth") => Some(redis.replace(":playback-fixture-only@", ":fixture-wrong@")),
            Some("backend") => Some("redis://:playback-fixture-only@127.0.0.1:1/0".into()),
            _ => Some(redis.clone()),
        };
        let provider = Arc::new(CachedSwapPolicy::with_clock(
            store,
            cache_uri,
            Arc::new(move || now),
        ));
        let dropped = Arc::new(AtomicBool::new(false));
        let context = SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: Arc::new(Guard(dropped.clone())),
        };
        let locked = case["version_after_present"] == true || case["expected_loads"] == 0;
        let mut blocker = if locked {
            let mut transaction = admin.begin().await.unwrap();
            sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await
                .unwrap();
            Some(transaction)
        } else {
            None
        };
        let engine = provider.clone();
        let mut task = tokio::spawn(async move { engine.resolve(context).await });
        if case["version_after_present"] == true {
            tokio::time::timeout(Duration::from_secs(5),async{loop{
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT octet_length%')").bind(&label).fetch_one(&admin).await.unwrap();
                if waiting{break;}tokio::time::sleep(Duration::from_millis(10)).await;
            }}).await.unwrap();
            set(&mut connection, VERSION, &case["version_after"]).await;
            blocker.take().unwrap().rollback().await.unwrap();
        }
        let result = if case["expected_loads"] == 0 {
            let completed = tokio::time::timeout(Duration::from_secs(1), &mut task).await;
            blocker.take().unwrap().rollback().await.unwrap();
            match completed {
                Ok(result) => result.unwrap(),
                Err(_) => {
                    let _ = task.await;
                    panic!("{} unexpectedly queried SQL", case["name"]);
                }
            }
        } else {
            task.await.unwrap()
        };
        assert_eq!(
            result.is_err(),
            !case["expected_error"].is_null(),
            "{}: {result:?}",
            case["name"]
        );
        if let Ok(policy) = result {
            assert_eq!(
                serde_json::to_value(policy.history).unwrap(),
                case["expected_policy"],
                "{}",
                case["name"]
            );
            assert_eq!(policy.now, now);
        }
        assert!(
            dropped.load(Ordering::Acquire),
            "normal policy command retained caller guard {}",
            case["name"]
        );
        for (name, key) in [("group", GROUP), ("version", VERSION)] {
            let (kind, raw, ttl) = read(&mut connection, key).await;
            let expected = &case["expected_cache"];
            assert_eq!(
                json!(kind),
                expected[format!("{name}_type")],
                "{} {name}",
                case["name"]
            );
            if let Some(raw) = raw {
                if expected.get(format!("{name}_error")).is_none() {
                    let value = decode_source_cache(&raw).unwrap();
                    assert_eq!(value, expected[name], "{} {name}", case["name"]);
                    if name == "group" {
                        interop.push(json!({"name":case["name"],"encoded_base64":base64::engine::general_purpose::STANDARD.encode(raw),"expected_value":expected[name]}));
                    }
                }
            }
            let expected_ttl = expected[format!("{name}_ttl")].as_i64().unwrap();
            if expected_ttl < 0 {
                assert_eq!(ttl, expected_ttl);
            } else {
                assert!(
                    (expected_ttl - 5..=expected_ttl + 1).contains(&ttl),
                    "{} {name} TTL{ttl}",
                    case["name"]
                );
            }
        }
    }
    physical_guard_scenarios(&db, &redis, &admin, &mut connection, now).await;
    std::fs::write(
        std::env::var("EPG_POLICY_NATIVE_CACHE").unwrap(),
        serde_json::to_vec_pretty(&interop).unwrap(),
    )
    .unwrap();
}

async fn physical_guard_scenarios(
    db: &str,
    redis: &str,
    admin: &sqlx::PgPool,
    connection: &mut MultiplexedConnection,
    now: DateTime<Utc>,
) {
    // A timed-out Redis GET may remain physically queued. Source returns the SQL
    // fallback, but our guard/operation slot must survive until that real reply.
    let store = Store::connect(db, 4).await.unwrap();
    sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('epg_settings','Physical policy fixture',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .bind(sqlx::types::Json(json!({"epg_history_min_days":1,"epg_history_max_days":30}))).execute(admin).await.unwrap();
    set(
        connection,
        GROUP,
        &json!({"epg_history_min_days":3,"epg_history_max_days":9}),
    )
    .await;
    let provider = CachedSwapPolicy::with_clock(store, Some(redis.into()), Arc::new(move || now))
        .with_timeout(Duration::from_millis(50))
        .unwrap();
    provider
        .resolve(SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: Arc::new(()),
        })
        .await
        .unwrap();
    let mut pause = redis::cmd("CLIENT");
    pause.arg("PAUSE").arg(400).arg("ALL");
    let _: String = command(connection, pause).await;
    let dropped = Arc::new(AtomicBool::new(false));
    let policy = provider
        .resolve(SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: Arc::new(Guard(dropped.clone())),
        })
        .await
        .unwrap();
    assert_eq!(
        policy.history.min_days, 1,
        "backend timeout must return SQL without cache fill"
    );
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(provider.physical_operations(), 1);
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(provider.physical_operations(), 0);
    let policy = provider
        .resolve(SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: Arc::new(()),
        })
        .await
        .unwrap();
    assert_eq!(
        policy.history.min_days, 3,
        "recovering backend must reconnect and read unchanged shared cache"
    );
    // An aborted SQL fallback waiter must retain the same admission/context
    // through the actual PostgreSQL reply, not merely until caller cancellation.
    let label = "rust-epg-policy-held-sql";
    let store = Store::connect(
        &format!(
            "{db}{}application_name={label}",
            if db.contains('?') { "&" } else { "?" }
        ),
        4,
    )
    .await
    .unwrap();
    let provider = Arc::new(CachedSwapPolicy::with_clock(
        store,
        None,
        Arc::new(move || now),
    ));
    let mut blocker = admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let flag = Arc::new(AtomicBool::new(false));
    let context = SwapPolicyContext {
        cancelled: flag.clone(),
        retained: Arc::new(Guard(dropped.clone())),
    };
    let actor = provider.clone();
    let task = tokio::spawn(async move { actor.resolve(context).await });
    tokio::time::timeout(Duration::from_secs(5),async{loop{
        let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'SELECT octet_length%')").bind(label).fetch_one(admin).await.unwrap();
        if waiting{break;}tokio::time::sleep(Duration::from_millis(10)).await;
    }}).await.unwrap();
    flag.store(true, Ordering::Release);
    task.abort();
    let _ = task.await;
    assert!(!dropped.load(Ordering::Acquire));
    assert_eq!(provider.physical_operations(), 1);
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(provider.physical_operations(), 0);
}
