//! Copied-database peer for the same normal M3U protocol/service/worker paths.
use crate::{
    logo_task::Clock,
    scanner_m3u::{Roots, Service},
    scanner_m3u_protocol::Scope,
    scanner_m3u_worker::Worker,
    transport::{Config, M3uPorts},
};
use dispatcharr_coordination::{TaskLockConfig, TaskLocks};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use serde_json::{json, Value};
use std::{
    io::{BufRead, Write},
    sync::Arc,
    time::Duration,
};
struct Fixed;
impl Clock for Fixed {
    fn now(&self) -> f64 {
        1000.125
    }
}
fn read(input: &mut impl BufRead) -> Value {
    let mut raw = Vec::new();
    loop {
        let v = input.fill_buf().unwrap();
        assert!(!v.is_empty());
        let n = v
            .iter()
            .position(|v| *v == b'\n')
            .map_or(v.len(), |n| n + 1);
        assert!(raw.len() + n <= 8192);
        raw.extend_from_slice(&v[..n]);
        input.consume(n);
        if raw.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&raw).unwrap()
}
fn output(v: Value) {
    println!("NATIVE_SCANNER_M3U_JSON={v}");
    std::io::stdout().flush().unwrap();
}
fn endpoint(v: &str, db: i64) {
    let c = redis::Client::open(v).unwrap();
    let i = c.get_connection_info();
    assert_eq!(i.redis.db, db);
    assert!(
        matches!(&i.addr,redis::ConnectionAddr::Tcp(h,p)if matches!(h.as_str(),"localhost"|"127.0.0.1")&&(*p==6379||(61000..=65000).contains(p)))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn native_scanner_m3u_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(database.starts_with("rust_probe_backup_jobs_"));
    crate::default_peer::validate_fixture_redis();
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let cfg = read(&mut input);
    let parent = uuid::Uuid::parse_str(cfg["parent_id"].as_str().unwrap()).unwrap();
    let account = cfg["account_id"].as_i64().unwrap();
    let broker = cfg["broker_url"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| std::env::var("CELERY_BROKER_URL").unwrap());
    let backend = cfg["result_url"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| std::env::var("CELERY_RESULT_BACKEND").unwrap());
    let cache = cfg["cache_url"]
        .as_str()
        .unwrap_or("redis://:playback-fixture-only@127.0.0.1:6379/0");
    let locks_url = cfg["locks_url"]
        .as_str()
        .unwrap_or("redis://:playback-fixture-only@127.0.0.1:6379/0");
    let events_url = cfg["events_url"]
        .as_str()
        .unwrap_or("redis://:playback-fixture-only@127.0.0.1:6379/0");
    endpoint(&broker, 8);
    endpoint(&backend, 9);
    for v in [cache, locks_url, events_url] {
        endpoint(v, 0);
    }
    let guard_url = std::env::var("DATABASE_URL").unwrap();
    let guard_store = Store::connect(&guard_url, 1).await.unwrap();
    let name: String = sqlx::query_scalar("SELECT name FROM m3u_m3uaccount WHERE id=$1")
        .bind(account)
        .fetch_one(guard_store.pool())
        .await
        .unwrap();
    assert!(name.starts_with("rust-owned-children-"));
    guard_store.pool().close().await;
    let database_url = cfg["database_url"].as_str().unwrap_or(&guard_url);
    let options: sqlx::postgres::PgConnectOptions = database_url.parse().unwrap();
    assert_eq!(options.get_database(), Some(database.as_str()));
    assert!(
        options.get_username() == "dispatch"
            || options.get_username() == format!("rust_m3u_preflight_{account}")
    );
    assert!(
        matches!(options.get_host(), "127.0.0.1" | "localhost")
            && (options.get_port() == 5432 || (61000..=65000).contains(&options.get_port()))
    );
    let store = Store::connect(database_url, 1).await.unwrap();
    let ports = M3uPorts::connect(&broker, &backend, cache, Config::default())
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("owned-unused-M3U-settings-fixture".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(cache).await.unwrap());
    let sink = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(events_url, Default::default())
            .await
            .unwrap(),
    );
    let service = Service::new(
        store.clone(),
        auth,
        sink,
        TaskLocks::connect(locks_url, TaskLockConfig::default())
            .await
            .unwrap(),
        ports.clone(),
        Roots {
            input: "/data/m3us".into(),
            cache: "/app/media/cached_m3u".into(),
        },
        Arc::new(chrono::Utc::now),
        Arc::new(Fixed),
    )
    .unwrap();
    let worker = Worker::new(
        service.clone(),
        ports.clone(),
        Scope::new(parent, vec![account]).unwrap(),
    );
    let foreign = Worker::new(
        service.clone(),
        ports.clone(),
        Scope::new(uuid::Uuid::new_v4(), vec![account]).unwrap(),
    );
    let mut pending = None;
    let mut weak: Option<std::sync::Weak<()>> = None;
    output(json!({"prepared":true}));
    loop {
        let request = read(&mut input);
        match request["action"].as_str().unwrap() {
            "run" => {
                let run = worker.run_once(Arc::new(())).await;
                output(json!({"run":format!("{run:?}"),"slots":worker.available_slots()}));
            }
            "foreign" => {
                let run = foreign.run_once(Arc::new(())).await;
                output(json!({"run":format!("{run:?}")}));
            }
            "start" => {
                assert!(pending.is_none());
                let caller = Arc::new(());
                weak = Some(Arc::downgrade(&caller));
                let w = worker.clone();
                pending = Some(tokio::spawn(async move { w.run_once(caller).await }));
                output(json!({"started":true}));
            }
            "abort" => {
                let t = pending.take().unwrap();
                t.abort();
                assert!(t.await.unwrap_err().is_cancelled());
                let admission = worker.run_once(Arc::new(())).await;
                let drain = worker.shutdown(Duration::from_millis(30)).await;
                output(
                    json!({"caller_retained":weak.as_ref().unwrap().upgrade().is_some(),"slots":worker.available_slots(),"admission":format!("{admission:?}"),"drain":format!("{drain:?}")}),
                );
            }
            "observe" => {
                let drain = worker.shutdown(Duration::from_millis(30)).await;
                output(
                    json!({"caller_retained":weak.as_ref().unwrap().upgrade().is_some(),"slots":worker.available_slots(),"drain":format!("{drain:?}")}),
                );
            }
            "settled" => {
                let end = tokio::time::Instant::now() + Duration::from_secs(10);
                while worker.available_slots() == 0 {
                    assert!(tokio::time::Instant::now() < end);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                output(
                    json!({"caller_released":weak.as_ref().unwrap().upgrade().is_none(),"slots":worker.available_slots()}),
                );
            }
            "shutdown" => {
                worker.shutdown(Duration::from_secs(5)).await.unwrap();
                foreign.shutdown(Duration::from_secs(5)).await.unwrap();
                service.shutdown(Duration::from_secs(5)).await.unwrap();
                ports.shutdown(Duration::from_secs(5)).await.unwrap();
                output(json!({"drained":true}));
                break;
            }
            _ => panic!("Unknown owned peer action"),
        }
    }
    store.pool().close().await;
}
