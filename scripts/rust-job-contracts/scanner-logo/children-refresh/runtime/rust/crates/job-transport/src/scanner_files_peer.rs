//! Copied-database peer of the normal mapped-file Service/Worker; fixture only.
use crate::{
    logo_scanner::Roots,
    logo_task::Clock,
    scanner_files::Service,
    scanner_files_worker::Worker,
    transport::{Config, ScannerFilesPorts, ScannerRoute},
};
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
fn read(input: &mut impl BufRead) -> serde_json::Value {
    let mut raw = Vec::new();
    loop {
        let bytes = input.fill_buf().unwrap();
        assert!(!bytes.is_empty());
        let n = bytes
            .iter()
            .position(|b| *b == b'\n')
            .map_or(bytes.len(), |n| n + 1);
        assert!(raw.len() + n <= 8192);
        raw.extend_from_slice(&bytes[..n]);
        input.consume(n);
        if raw.ends_with(b"\n") {
            break;
        }
    }
    serde_json::from_slice(&raw).unwrap()
}
fn output(value: serde_json::Value) {
    println!("NATIVE_SCANNER_FILES_JSON={value}");
    std::io::stdout().flush().unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn native_scanner_files_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    assert!(std::env::var("POSTGRES_DB")
        .unwrap()
        .starts_with("rust_probe_backup_jobs_"));
    crate::default_peer::validate_fixture_redis();
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let config = read(&mut input);
    let cache = if config["marker_acl_user"] == "rust-owned-scanner-no-set" {
        "redis://rust-owned-scanner-no-set:playback-fixture-only@localhost:6379/0"
    } else {
        "redis://:playback-fixture-only@localhost:6379/0"
    };
    let ports = ScannerFilesPorts::connect(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        cache,
        ScannerRoute::OwnedFixtureParent,
        Config::default(),
    )
    .await
    .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let sink = Arc::new(
        dispatcharr_realtime::RealtimeBus::connect(
            "redis://:playback-fixture-only@localhost:6379/0",
            Default::default(),
        )
        .await
        .unwrap(),
    );
    let service = Service::new(
        pool.clone(),
        ports.clone(),
        sink,
        Roots {
            m3u: "/data/m3us".into(),
            epg: "/data/epgs".into(),
            logos: "/data/logos".into(),
        },
    )
    .unwrap();
    let worker = Worker::new(
        service,
        ports.clone(),
        Arc::new(Fixed(config["now"].as_f64().unwrap())),
    );
    output(serde_json::json!({"prepared":true,"first":worker.first_completed()}));
    loop {
        let request = read(&mut input);
        match request["action"].as_str().unwrap() {
            "run" => {
                let result = worker.run_once(Arc::new(())).await;
                output(
                    serde_json::json!({"run":format!("{result:?}"),"first":worker.first_completed(),"attempts":ports.publication_attempts().unwrap()}),
                );
            }
            "shutdown" => {
                assert!(worker.shutdown(Duration::from_secs(5)).await);
                output(serde_json::json!({"drained":true}));
                break;
            }
            _ => panic!("Unknown fixture action"),
        }
    }
    pool.close().await;
}
