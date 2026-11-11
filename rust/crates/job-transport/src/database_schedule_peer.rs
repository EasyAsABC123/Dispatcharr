//! Fixture-only driver. Original Dispatcharr/framework authors retain credit.
use super::*;
use std::io::{BufRead, Write};
struct FixtureSink(Arc<std::sync::atomic::AtomicUsize>);
impl dispatcharr_contracts::UpdateEventSink for FixtureSink {
    fn send<'a>(
        &'a self,
        _: &'a str,
        _: Value,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<(), dispatcharr_contracts::UpdateEventError>>
    {
        self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Box::pin(async { Ok(()) })
    }
}
struct ObservedPublisher(
    Arc<dyn tick::PeriodicPublisher>,
    Arc<std::sync::Mutex<Vec<String>>>,
);
impl tick::PeriodicPublisher for ObservedPublisher {
    fn publish<'a>(
        &'a self,
        task: tick::KnownTask,
        name: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> tick::PublishFuture<'a> {
        self.1.lock().unwrap().push(name.to_owned());
        self.0.publish(task, name, caller)
    }
}
/// Fixture timing barrier after the real broker reply and before SQL sync.
struct CrashPublisher(Arc<tick::KnownPublisher>, std::path::PathBuf);
impl tick::PeriodicPublisher for CrashPublisher {
    fn publish<'a>(
        &'a self,
        task: tick::KnownTask,
        name: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> tick::PublishFuture<'a> {
        Box::pin(async move {
            let value = tick::PeriodicPublisher::publish(&*self.0, task, name, caller).await?;
            std::fs::write(&self.1, "before-sync").unwrap();
            std::future::pending::<()>().await;
            Ok(value)
        })
    }
}
fn crash_mode() -> Option<(String, std::path::PathBuf)> {
    let mode = std::env::var("SCHEDULER_CRASH_MODE").ok()?;
    assert!(matches!(mode.as_str(), "before-sync" | "after-sync"));
    let marker = std::path::PathBuf::from(std::env::var("SCHEDULER_CRASH_GATE").unwrap());
    assert_eq!(marker.parent(), Some(std::path::Path::new("/evidence")));
    assert!(marker
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("native-scheduler-crash-"));
    Some((mode, marker))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Requires guarded disposable copied PostgreSQL fixture"]
async fn native_database_scheduler_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(db.starts_with("rust_probe_backup_jobs_"));
    assert!(
        db.len() <= 63
            && db
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_')
    );
    let url = std::env::var("SCHEDULER_DATABASE_URL").unwrap();
    assert!(url.ends_with(&format!("/{db}")));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let mut service = DatabaseSchedule::new(pool.clone());
    let mut publisher: Option<Arc<tick::KnownPublisher>> = None;
    let mut driver: Option<driver::Driver> = None;
    let mut consumers: Option<(
        crate::refresh_application::RefreshApplication,
        crate::series_application::SeriesApplication,
    )> = None;
    let events = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    for line in std::io::stdin().lock().lines() {
        let command: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let action = command["action"].as_str().unwrap();
        let mut out = serde_json::json!({"action":action});
        match action {
            "heap-clock" => {
                let fixed = command["now"]
                    .as_str()
                    .unwrap()
                    .parse::<DateTime<Utc>>()
                    .unwrap();
                assert!(driver.is_none() && consumers.is_none());
                assert!(service.shutdown(Duration::from_secs(5)).await);
                service =
                    DatabaseSchedule::new_with_heap_clock(pool.clone(), Arc::new(move || fixed));
                out["configured"] = Value::Bool(true);
            }
            "populate-heap" => {
                let now = command
                    .get("now")
                    .map(|value| value.as_str().unwrap().parse::<DateTime<Utc>>().unwrap())
                    .unwrap_or_else(Utc::now);
                let this = service.clone();
                service
                    .owned(Arc::new(()), move |_| async move {
                        let mut state = this.inner.state.lock().await;
                        this.refresh_locked(&mut state, now).await?;
                        this.populate_locked(&mut state, now).await
                    })
                    .await
                    .unwrap();
                out["populated"] = Value::Bool(true);
            }
            "inspect" => {
                if let Some((refresh, series)) = consumers.as_ref() {
                    out["refresh_intake_error"] =
                        serde_json::json!(format!("{:?}", refresh.last_intake_error()));
                    out["series_state"] = serde_json::json!(format!("{:?}", series.state()));
                }
                out["driver_task_finished"] =
                    serde_json::json!(driver.as_ref().map(driver::Driver::task_finished));
                out["available_admissions"] =
                    serde_json::json!(service.inner.slots.available_permits());
                let state = service.inner.state.lock().await;
                out["load_order"] = serde_json::json!(state.load_order);
                out["heap"] = Value::Array(state.heap.iter().map(|event|serde_json::json!({"when":event.when,"priority":event.priority,"name":event.name,"identity":event.identity()})).collect());
            }
            "consumers-start" => {
                assert!(consumers.is_none());
                let broker = std::env::var("CELERY_BROKER_URL").unwrap();
                let backend = std::env::var("CELERY_RESULT_BACKEND").unwrap();
                let cache = "redis://:playback-fixture-only@localhost:6379/0".to_owned();
                let store = dispatcharr_store::Store::from_pool(pool.clone());
                let auth = dispatcharr_store::AuthService::new(
                    store.clone(),
                    dispatcharr_store::AuthConfig::new(
                        "scheduler-disposable-fixture-only-key".to_owned(),
                    )
                    .unwrap(),
                );
                let refresh = crate::refresh_application::RefreshApplication::prepare(
                    store.clone(),
                    auth,
                    Arc::new(FixtureSink(events.clone())),
                    crate::refresh_application::RefreshApplicationOptions {
                        broker: broker.clone(),
                        backend: backend.clone(),
                        cache: cache.clone(),
                        cache_dir: std::path::PathBuf::from("/evidence/native-timer-cache"),
                    },
                )
                .await
                .unwrap();
                let series = crate::series_application::SeriesApplication::prepare(
                    store,
                    crate::series_application::Options {
                        broker,
                        backend,
                        cache,
                        poll_interval: Duration::from_millis(25),
                    },
                )
                .await
                .unwrap();
                // Both families are fully prepared before either opens intake.
                refresh.start().unwrap();
                series.start().unwrap();
                consumers = Some((refresh, series));
                out["consumers_ready"] = Value::Bool(true);
            }
            "setup" => {
                let value = service.setup(Arc::new(())).await.unwrap();
                out["installed"] = serde_json::json!(value.installed);
                out["failures"] = serde_json::json!(value.failures);
            }
            "refresh" => {
                let now = command
                    .get("now")
                    .map(|v| v.as_str().unwrap().parse::<DateTime<Utc>>().unwrap())
                    .unwrap_or_else(Utc::now);
                let entries = service.refresh(now, Arc::new(())).await.unwrap();
                out["entries"]=Value::Array(entries.into_iter().map(|v|serde_json::json!({"id":v.id,"name":v.name,"task":v.task,"enabled":v.enabled,"args":v.args,"kwargs":v.kwargs,"last_run_at":v.last_run_at.to_rfc3339(),"count":v.total_run_count,"interval":v.interval,"crontab":v.crontab})).collect());
            }
            "tick" | "driver-start" => {
                if publisher.is_none() {
                    let broker = std::env::var("CELERY_BROKER_URL").unwrap();
                    let backend = std::env::var("CELERY_RESULT_BACKEND").unwrap();
                    let config = crate::transport::Config {
                        operations: 1,
                        command_timeout: Duration::from_secs(2),
                        max_queued_jobs: 64,
                    };
                    publisher = Some(Arc::new(tick::KnownPublisher {
                        refresh: crate::transport::Jobs::connect_refresh(
                            &broker,
                            &backend,
                            config.clone(),
                        )
                        .await
                        .unwrap(),
                        series: crate::transport::Jobs::connect_series(&broker, &backend, config)
                            .await
                            .unwrap(),
                    }));
                }
                if action == "driver-start" {
                    assert!(driver.is_none());
                    driver = Some(
                        driver::Driver::start(
                            service.clone(),
                            publisher.as_ref().unwrap().clone(),
                            Duration::from_secs(1),
                            Arc::new(()),
                        )
                        .unwrap(),
                    );
                    out["started"] = Value::Bool(true);
                    println!("NATIVE_DATABASE_SCHEDULER_JSON={out}");
                    std::io::stdout().flush().unwrap();
                    continue;
                }
                let crash = crash_mode();
                let active: Arc<dyn tick::PeriodicPublisher> = match &crash {
                    Some((mode, marker)) if mode == "before-sync" => Arc::new(CrashPublisher(
                        publisher.as_ref().unwrap().clone(),
                        marker.clone(),
                    )),
                    _ => publisher.as_ref().unwrap().clone(),
                };
                let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
                let active = Arc::new(ObservedPublisher(active, observations.clone()));
                let value = service
                    .tick(
                        command
                            .get("now")
                            .map(|value| value.as_str().unwrap().parse::<DateTime<Utc>>().unwrap())
                            .unwrap_or_else(Utc::now),
                        active,
                        Arc::new(()),
                    )
                    .await
                    .unwrap();
                if let Some((mode, marker)) = &crash {
                    if mode == "after-sync" {
                        std::fs::write(marker, "after-sync").unwrap();
                        std::future::pending::<()>().await;
                    }
                }
                out["publication_names"] = serde_json::json!(*observations.lock().unwrap());
                out["delay_seconds"] = serde_json::json!(value.delay_seconds);
                out["publication"] = match value.publication {
                    None => Value::Null,
                    Some(Ok(v)) => {
                        serde_json::json!({"accepted":true,"task_id":v.task_id.to_string(),"queue_length":v.queue_length})
                    }
                    Some(Err(error)) => {
                        serde_json::json!({"accepted":false,"diagnostic":format!("{error:?}")})
                    }
                };
                out["sync"]=value.sync.map(|v|serde_json::json!({"saved":v.saved,"missing":v.missing,"server_error":v.server_error})).unwrap_or(Value::Null);
            }
            "sync" => {
                let value = service.sync(Arc::new(())).await.unwrap();
                out["saved"] = serde_json::json!(value.saved);
                out["missing"] = serde_json::json!(value.missing);
                out["server_error"] = serde_json::json!(value.server_error);
            }
            "shutdown" => {
                if let Some(driver) = driver.as_mut() {
                    out["driver_drained"] =
                        Value::Bool(driver.shutdown(Duration::from_secs(5)).await.unwrap());
                }
                out["drained"] = serde_json::json!(service.shutdown(Duration::from_secs(5)).await);
                if let Some((refresh, series)) = consumers.take() {
                    out["refresh_drained"] =
                        Value::Bool(refresh.shutdown(Duration::from_secs(5)).await);
                    out["series_drained"] =
                        Value::Bool(series.shutdown(Duration::from_secs(5)).await.is_ok());
                    out["event_count"] =
                        serde_json::json!(events.load(std::sync::atomic::Ordering::Acquire));
                }
                if let Some(publisher) = publisher.as_ref() {
                    publisher
                        .refresh
                        .shutdown(Duration::from_secs(5))
                        .await
                        .unwrap();
                    publisher
                        .series
                        .shutdown(Duration::from_secs(5))
                        .await
                        .unwrap();
                }
                pool.close().await;
            }
            _ => panic!("Unknown guarded fixture action"),
        }
        println!("NATIVE_DATABASE_SCHEDULER_JSON={out}");
        std::io::stdout().flush().unwrap();
        if action == "shutdown" {
            break;
        }
    }
}
