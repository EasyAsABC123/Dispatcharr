//! Actual copied-fixture helper + independently scoped Index intake assembly.
use super::{
    index_application::{IndexApplication, Options},
    logo_scanner::{Limits, Roots},
    logo_scanner_effects::RetainedEffects,
    logo_task::Clock,
    scanner_index_helper::IndexedLogoApplication,
    transport::{Config, Jobs},
};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use std::{
    io::{BufRead, Write},
    sync::Arc,
    time::Duration,
};
struct Fixed(f64);
impl Clock for Fixed {
    fn now(&self) -> f64 {
        self.0
    }
}
fn output(value: serde_json::Value) {
    println!("NATIVE_SCANNER_INDEX_JSON={value}");
    std::io::stdout().flush().unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn native_scanner_index_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_jobs_"));
    crate::default_peer::validate_fixture_redis();
    let stdin = std::io::stdin();
    let mut reader = stdin.lock();
    let mut first = String::new();
    reader.read_line(&mut first).unwrap();
    assert!(first.len() <= 8192);
    let config: serde_json::Value = serde_json::from_str(&first).unwrap();
    let broker = std::env::var("CELERY_BROKER_URL").unwrap();
    let result = std::env::var("CELERY_RESULT_BACKEND").unwrap();
    let cache_url = "redis://:playback-fixture-only@localhost:6379/0";
    let jobs = Jobs::connect_defaults(&broker, &result, Config::default())
        .await
        .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let cache = RedisTransport::connect(
        cache_url,
        TransportConfig {
            max_operations: 4,
            max_queued_operations: 4,
            command_timeout: Duration::from_secs(3),
        },
    )
    .await
    .unwrap();
    let sink = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(cache_url, Default::default())
            .await
            .unwrap(),
    );
    let effects = Arc::new(RetainedEffects::new(pool.clone(), cache, sink));
    let application = if config["spooled"] == true {
        std::fs::create_dir_all("/data/scanner-work").unwrap();
        IndexedLogoApplication::prepare_spooled(
            jobs.clone(),
            pool.clone(),
            effects,
            crate::logo_scanner::SpoolOptions {
                roots: Roots {
                    m3u: "/data/m3us".into(),
                    epg: "/data/epgs".into(),
                    logos: "/data/logos".into(),
                },
                limits: Limits {
                    files: 4096,
                    ..Limits::default()
                },
                scratch: crate::logo_scanner::SpoolConfig {
                    directory: "/data/scanner-work".into(),
                    max_bytes: 256 * 1024,
                },
            },
            Arc::new(Fixed(config["now"].as_f64().unwrap())),
            Duration::from_millis(25),
        )
        .unwrap()
    } else {
        IndexedLogoApplication::prepare(
            jobs.clone(),
            pool.clone(),
            effects,
            Roots {
                m3u: "/data/m3us".into(),
                epg: "/data/epgs".into(),
                logos: "/data/logos".into(),
            },
            Limits::default(),
            Arc::new(Fixed(config["now"].as_f64().unwrap())),
            Duration::from_millis(25),
        )
        .unwrap()
    };
    let index = IndexApplication::connect(
        dispatcharr_store::Store::from_pool(pool.clone()),
        Options {
            broker,
            backend: result,
            cache: cache_url.into(),
            source_cwd: "/app".into(),
            media_root: "/app/media".into(),
            cache_dir: "/app/media/cached_epg".into(),
        },
    )
    .await
    .unwrap();
    output(
        serde_json::json!({"ready":true,"first":application.first_scan_completed(),"state":application.state().summary()}),
    );
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 {
            break;
        }
        assert!(line.len() <= 8192);
        let command: serde_json::Value = serde_json::from_str(&line).unwrap();
        match command["action"].as_str().unwrap() {
            "start" => {
                application.start().unwrap();
                output(serde_json::json!({"started":true}));
            }
            "start_index" => {
                index.start(Arc::new(())).unwrap();
                output(serde_json::json!({"index_started":true}));
            }
            "state" => output(
                serde_json::json!({"first":application.first_scan_completed(),"state":application.state().summary(),"index_error":format!("{:?}",index.last_execution_error())}),
            ),
            "shutdown" => {
                let end = tokio::time::Instant::now() + Duration::from_secs(10);
                let drain = application
                    .shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
                    .await;
                assert!(drain.drained);
                assert!(
                    index
                        .shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
                        .await
                );
                jobs.shutdown(end.saturating_duration_since(tokio::time::Instant::now()))
                    .await
                    .unwrap();
                pool.close().await;
                output(serde_json::json!({"drained":true}));
                break;
            }
            _ => panic!("unknown owned action"),
        }
    }
}
