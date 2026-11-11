//! Fixture-only execution peer. Original Dispatcharr authors retain credit.
use crate::{
    default_application::DefaultApplication,
    default_domains::KnownDomains,
    default_protocol::{Kind, Message},
    default_worker::DefaultWorker,
    transport::{Config, Jobs},
};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use std::{
    io::{self, Read},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;
pub(crate) fn validate_fixture_redis() {
    let mut address = None;
    for (name, db) in [("CELERY_BROKER_URL", 8), ("CELERY_RESULT_BACKEND", 9)] {
        let client = redis::Client::open(std::env::var(name).unwrap()).unwrap();
        let info = client.get_connection_info();
        assert_eq!(info.redis.db, db);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port) if matches!(host.as_str(),"localhost"|"127.0.0.1"|"::1") && (*port==6379 || (61000..=65000).contains(port)))
        );
        if let Some(previous) = &address {
            assert_eq!(previous, &info.addr);
        } else {
            address = Some(info.addr.clone());
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
async fn native_default_family_peer() {
    let mut input = String::new();
    io::stdin().take(65537).read_to_string(&mut input).unwrap();
    assert!(input.len() <= 65536);
    let request: Value = serde_json::from_str(&input).unwrap();
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(database.starts_with("rust_probe_backup_jobs_"));
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let bus = dispatcharr_realtime::RealtimeBus::connect(
        "redis://:playback-fixture-only@localhost:6379/0",
        dispatcharr_realtime::BusConfig::default(),
    )
    .await
    .unwrap();
    let _validated_fixture = KnownDomains::copied_fixture(pool.clone(), &database).unwrap();
    validate_fixture_redis();
    let domains = Arc::new(KnownDomains::with_notifications(
        pool.clone(),
        Arc::new(bus),
    ));
    let jobs = Jobs::connect_defaults(
        &std::env::var("CELERY_BROKER_URL").unwrap(),
        &std::env::var("CELERY_RESULT_BACKEND").unwrap(),
        Config {
            operations: 2,
            command_timeout: Duration::from_secs(2),
            max_queued_jobs: 64,
        },
    )
    .await
    .unwrap();
    let worker = DefaultWorker::new(jobs.clone(), domains).unwrap();
    let action = request["action"].as_str().unwrap();
    let result = match action {
        "publish" => {
            let kind = match request["kind"].as_str().unwrap() {
                "cleanup" => Kind::RedisBackendCleanup,
                "recurring" => Kind::RecurringMaintenance,
                "inactive" => Kind::InactiveAccountNotifications,
                _ => panic!("Unsupported fixture family"),
            };
            let message = Message {
                task_id: Uuid::parse_str(request["task_id"].as_str().unwrap()).unwrap(),
                kind,
            };
            jobs.publish_default(&message, Arc::new(()))
                .await
                .map(|v| json!({"task_id":v.task_id,"queue_length":v.queue_length}))
                .map_err(|e| format!("{e:?}"))
        }
        "run" => worker
            .run_once(Arc::new(()))
            .await
            .map(|v| json!({"outcome":v.summary()}))
            .map_err(|e| format!("{e}")),
        "poll" | "prepare" => {
            let app =
                DefaultApplication::prepare(worker.clone(), Duration::from_millis(25)).unwrap();
            assert!(matches!(
                app.state(),
                crate::default_application::State::Prepared
            ));
            if action == "poll" {
                app.start().unwrap();
                let ids: Vec<Uuid> = request
                    .get("task_ids")
                    .and_then(Value::as_array)
                    .map(|rows| {
                        assert!(rows.len() <= 8);
                        rows.iter()
                            .map(|id| Uuid::parse_str(id.as_str().unwrap()).unwrap())
                            .collect()
                    })
                    .unwrap_or_default();
                let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                loop {
                    let mut ready = true;
                    for id in &ids {
                        match jobs.read(*id, Arc::new(())).await {
                            Ok(Some(_)) => {}
                            Ok(None) | Err(crate::transport::Error::Capacity) => ready = false,
                            Err(error) => panic!("Fixture result observation {error:?}"),
                        }
                    }
                    assert!(
                        !matches!(app.state(), crate::default_application::State::Fault(_)),
                        "Operational poller fault"
                    );
                    if ready {
                        break;
                    }
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "Owned polling task deadline"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            let drain = app.shutdown(Duration::from_secs(5)).await;
            Ok(json!({"drained":drain.drained,"state":drain.state.summary()}))
        }
        _ => panic!("Unknown fixture action"),
    };
    let drained = worker.shutdown(Duration::from_secs(5)).await;
    let available = jobs.available_slots();
    let jobs_drained = jobs.shutdown(Duration::from_secs(5)).await.is_ok();
    pool.close().await;
    assert!(drained && jobs_drained && available == 2);
    let error = result.as_ref().err().cloned();
    println!(
        "NATIVE_DEFAULT_PAIR_JSON={}",
        json!({"ok":result.is_ok(),"error":error,"result":result.ok(),"drained":drained,"jobs_drained":jobs_drained,"available_slots":available,"pool_closed":pool.is_closed()})
    );
}
