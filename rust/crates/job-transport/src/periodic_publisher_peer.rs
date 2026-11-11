//! Guarded fixture CLI for actual periodic producer/Source worker interoperability.
//! Original Dispatcharr/Celery/framework authors retain credit/licenses.
use crate::{
    refresh_protocol::{RefreshCall, RefreshMessage},
    series_protocol::SeriesMessage,
    transport::{Config, Error, Jobs},
};
use serde_json::{json, Value};
use std::{io::Read, sync::Arc, time::Duration};
use uuid::Uuid;
#[tokio::test]
#[ignore = "Requires isolated copied fixture broker8/result9; never starts a worker"]
async fn native_periodic_publisher_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        db.starts_with("rust_probe_backup_jobs_")
            && db.len() <= 63
            && db
                .bytes()
                .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_')
    );
    let mut raw = String::new();
    std::io::stdin()
        .take(8193)
        .read_to_string(&mut raw)
        .unwrap();
    assert!(raw.len() <= 8192);
    let input: Value = serde_json::from_str(&raw).unwrap();
    let broker = input["broker_url"].as_str().unwrap();
    let backend = input["result_url"].as_str().unwrap();
    for (url, db) in [(broker, 8), (backend, 9)] {
        let client = redis::Client::open(url).unwrap();
        let info = client.get_connection_info();
        assert_eq!(info.redis.db, db);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port)if matches!(host.as_str(),"localhost"|"127.0.0.1"|"::1")&&*port==6379)
        );
    }
    let config = Config {
        operations: 1,
        command_timeout: Duration::from_secs(2),
        max_queued_jobs: 64,
    };
    let refresh = Jobs::connect_refresh(broker, backend, config.clone())
        .await
        .unwrap();
    let series = Jobs::connect_series(broker, backend, config.clone())
        .await
        .unwrap();
    let index = Jobs::connect_index(broker, backend, config).await.unwrap();
    let id = Uuid::parse_str(input["task_id"].as_str().unwrap()).unwrap();
    let source_id = input["source_id"].as_i64().filter(|v| *v > 0).unwrap();
    let name = input["periodic_name"].as_str().unwrap();
    let message = RefreshMessage {
        task_id: id,
        source_id,
        force: true,
        retry: 0,
        root: id,
        parent: None,
        eta: None,
        call: RefreshCall {
            args: vec![json!(source_id)],
            kwargs: serde_json::Map::from_iter([("force".to_owned(), json!(true))]),
            list_args: true,
        },
    };
    let rule = SeriesMessage {
        task_id: id,
        root: id,
        parent: None,
    };
    let mut report = json!({"passed":true});
    match input["action"].as_str().unwrap() {
        "publish" => {
            assert!(matches!(
                index
                    .publish_periodic_refresh(&message, name, Arc::new(()))
                    .await,
                Err(Error::UnsupportedTask)
            ));
            assert!(matches!(
                index
                    .publish_periodic_series(&rule, name, Arc::new(()))
                    .await,
                Err(Error::UnsupportedTask)
            ));
            assert!(matches!(
                series
                    .publish_periodic_refresh(&message, name, Arc::new(()))
                    .await,
                Err(Error::UnsupportedTask)
            ));
            assert!(matches!(
                refresh
                    .publish_periodic_series(&rule, name, Arc::new(()))
                    .await,
                Err(Error::UnsupportedTask)
            ));
            let published = match input["family"].as_str().unwrap() {
                "refresh" => refresh
                    .publish_periodic_refresh(&message, name, Arc::new(()))
                    .await
                    .unwrap(),
                "series" => series
                    .publish_periodic_series(&rule, name, Arc::new(()))
                    .await
                    .unwrap(),
                _ => panic!("Unsupported known periodic family"),
            };
            report["task_id"] = json!(published.task_id.to_string());
            report["queue_length"] = json!(published.queue_length);
        }
        "read" => {
            let record = refresh.read(id, Arc::new(())).await.unwrap().unwrap();
            assert_eq!(record.state, crate::decode::State::Success);
            report["result"] = record.result;
        }
        _ => panic!("Unknown guarded fixture action"),
    }
    refresh.shutdown(Duration::from_secs(2)).await.unwrap();
    series.shutdown(Duration::from_secs(2)).await.unwrap();
    index.shutdown(Duration::from_secs(2)).await.unwrap();
    report["drained"] = json!(true);
    println!("NATIVE_PERIODIC_PUBLISHER_JSON={report}");
}
