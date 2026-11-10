//! Test-only known refresh publisher interoperability, no consumer/writer/app.
//! Original Dispatcharr authors retain full application/algorithm credit/AGPL.
use crate::{
    refresh_protocol::{RefreshCall, RefreshMessage},
    transport::{Config, Error, Jobs},
};
use serde_json::{json, Value};
use std::{
    io::{self, Read},
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;
pub(crate) fn message(id: Uuid, call: RefreshCall) -> RefreshMessage {
    let (source_id, force, retry) = call.resolve().unwrap();
    RefreshMessage {
        task_id: id,
        source_id,
        force,
        retry,
        root: id,
        parent: None,
        eta: None,
        call,
    }
}
#[tokio::test]
#[ignore = "Actual copiedDB fixture broker8/result9 publisher or result read; no native worker"]
async fn native_refresh_publication_peer() {
    assert_eq!(
        std::env::var("JOB_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    assert!(
        database.starts_with("rust_probe_backup_jobs_")
            && database.len() <= 63
            && database
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    );
    let mut raw = String::new();
    io::stdin().take(16385).read_to_string(&mut raw).unwrap();
    assert!(raw.len() <= 16384);
    let input: Value = serde_json::from_str(&raw).unwrap();
    let id = Uuid::parse_str(input["task_id"].as_str().unwrap()).unwrap();
    let source_id = input["source_id"].as_i64().filter(|v| *v > 0).unwrap();
    let broker = input["broker_url"].as_str().unwrap();
    let result = input["result_url"].as_str().unwrap();
    assert!(matches!(
        input["action"].as_str(),
        Some("publish" | "publish_deferred" | "read" | "authority")
    ));
    let mut address = None;
    for (raw, db) in [(broker, 8), (result, 9)] {
        let client = redis::Client::open(raw).unwrap();
        let info = client.get_connection_info();
        assert_eq!(info.redis.db, db);
        assert_eq!(
            info.redis.password.as_deref(),
            Some("playback-fixture-only")
        );
        assert!(
            matches!(&info.addr,redis::ConnectionAddr::Tcp(host,port) if matches!(host.as_str(),"127.0.0.1"|"localhost"|"::1")&&(*port==6379||(61000..=65000).contains(port)))
        );
        if let Some(old) = address.as_ref() {
            assert_eq!(old, &info.addr)
        } else {
            address = Some(info.addr.clone())
        }
    }
    let config = Config {
        operations: 1,
        command_timeout: Duration::from_secs(2),
        max_queued_jobs: 64,
    };
    let index_only = input["action"] == "authority";
    let jobs = if index_only {
        Jobs::connect_index(broker, result, config).await.unwrap()
    } else {
        Jobs::connect_fixture(broker, result, &database, config)
            .await
            .unwrap()
    };
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let audit = if input["action"] == "read" {
        let record = jobs
            .read_refresh(id, source_id, caller)
            .await
            .unwrap();
        match record {
            None => json!({"present":false}),
            Some(value) => {
                json!({"present":true,"task_id":value.task_id.to_string(),"missing_source":value.missing_source,"parent":value.parent.map(|v|v.to_string()),"children":value.children.iter().map(Uuid::to_string).collect::<Vec<_>>(),"date_done":value.date_done})
            }
        }
    } else {
        let call = RefreshCall {
            args: input["args"].as_array().unwrap().clone(),
            kwargs: input["kwargs"].as_object().unwrap().clone(),
            list_args: input["list_args"].as_bool().unwrap(),
        };
        let mut msg = message(id, call);
        if input["action"] == "publish_deferred" {
            assert!((1..=480).contains(&msg.retry));
            assert_eq!(
                msg.call,
                RefreshCall::deferred(source_id, msg.force, msg.retry)
            );
            msg.root = Uuid::parse_str(input["root_id"].as_str().unwrap()).unwrap();
            msg.parent = Some(Uuid::parse_str(input["parent_id"].as_str().unwrap()).unwrap());
            let eta = chrono::DateTime::parse_from_rfc3339(input["eta"].as_str().unwrap()).unwrap();
            assert_eq!(eta.offset().local_minus_utc(), 0);
            msg.eta = Some(eta.with_timezone(&chrono::Utc));
        }
        assert_eq!(msg.source_id, source_id);
        let published = jobs.publish_refresh(&msg, caller).await;
        if index_only {
            assert_eq!(published, Err(Error::UnsupportedTask));
            json!({"index_authority_rejected":true})
        } else {
            let value = published.unwrap();
            assert_eq!(value.task_id, id);
            json!({"published":true,"task_id":id.to_string(),"queue_length":value.queue_length})
        }
    };
    assert!(weak.upgrade().is_none());
    assert_eq!(jobs.available_slots(), 1);
    jobs.shutdown(Duration::from_secs(5)).await.unwrap();
    println!(
        "NATIVE_REFRESH_PUBLISH_JSON={}",
        json!({"audit":audit,"caller_released":true,"slots":1,"drained":true,"native_worker":false})
    );
}

#[cfg(test)]
#[path = "refresh_publish_physical.rs"]
mod physical;
