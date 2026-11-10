//! Cached Redis restart differential component; no runtime reconnect changes.
//! Original Dispatcharr authors retain all application and algorithm credit.
use dispatcharr_coordination::{
    AcquireRequest, Coordinator, LeaseConfig, TaskLease, TaskLockConfig, TaskLocks,
};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use serde_json::{json, Value};
use std::{
    io::{BufRead, Read, Write},
    sync::Arc,
    time::Duration,
};
#[tokio::main(flavor = "current_thread")]
async fn main() {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let info = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection_info()
        .clone();
    assert_eq!(
        info.redis.password.as_deref(),
        Some("playback-fixture-only")
    );
    assert_eq!(info.redis.db, 0);
    assert!(matches!(info.addr,redis::ConnectionAddr::Tcp(ref host,6379)if host=="127.0.0.1"));
    let task = std::env::var("RESTART_NATIVE_TASK").unwrap();
    assert!(
        task.starts_with("owned_redis_restart_")
            && task.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        loop {
            let mut line = String::new();
            let read = input.by_ref().take(16385).read_line(&mut line).unwrap();
            if read == 0 {
                break;
            }
            if read > 16384 || tx.blocking_send(line).is_err() {
                break;
            }
        }
    });
    let mut locks: Option<TaskLocks> = None;
    let mut coordinator: Option<Coordinator> = None;
    let mut transport: Option<RedisTransport> = None;
    let mut lease: Option<TaskLease> = None;
    while let Some(line) = rx.recv().await {
        let command: Value = serde_json::from_str(&line).unwrap();
        let action = command["action"].as_str().unwrap();
        let begin = std::time::Instant::now();
        let mut record = json!({"action":action,"pid":std::process::id()});
        match action {
            "init" | "rebuild" => {
                assert!(lease.is_none());
                let fresh = TaskLocks::connect(
                    &url,
                    TaskLockConfig {
                        max_leases: 1,
                        max_commands: 1,
                        ..Default::default()
                    },
                )
                .await;
                match fresh {
                    Ok(value) => {
                        locks = Some(value);
                        record["locks_ready"] = json!(true);
                    }
                    Err(error) => record["locks_error"] = json!(format!("{error:?}")),
                }
                let fresh = Coordinator::connect(
                    &url,
                    LeaseConfig {
                        namespace: task.clone(),
                        heartbeat_interval: None,
                        reap_interval: None,
                        command_timeout: Duration::from_millis(300),
                        ..Default::default()
                    },
                )
                .await;
                match fresh {
                    Ok(value) => {
                        coordinator = Some(value);
                        record["coordinator_ready"] = json!(true);
                    }
                    Err(error) => record["coordinator_error"] = json!(format!("{error:?}")),
                }
                let fresh = RedisTransport::connect_classified(
                    &url,
                    TransportConfig {
                        max_operations: 1,
                        max_queued_operations: 1,
                        command_timeout: Duration::from_millis(300),
                    },
                )
                .await;
                match fresh {
                    Ok(value) => {
                        transport = Some(value);
                        record["transport_ready"] = json!(true);
                    }
                    Err(error) => record["transport_error"] = json!(format!("{error:?}")),
                }
            }
            "coordinate" => {
                match coordinator
                    .as_ref()
                    .unwrap()
                    .acquire(AcquireRequest {
                        channel_id: format!("{}-{}", task, command["id"]),
                        provider_profile_id: Some(42),
                        provider_max_streams: 1,
                        credential_pool: None,
                    })
                    .await
                {
                    Ok(value) => match value.release().await {
                        Ok(removed) => record["result"] = json!(removed),
                        Err(error) => record["error"] = json!(format!("{error:?}")),
                    },
                    Err(error) => record["error"] = json!(format!("{error:?}")),
                }
            }
            "acquire" => {
                assert!(lease.is_none());
                match locks
                    .as_ref()
                    .unwrap()
                    .acquire(&task, command["id"].as_i64().unwrap())
                    .await
                {
                    Ok(value) => {
                        lease = Some(value);
                        record["result"] = json!(true);
                    }
                    Err(error) => record["error"] = json!(format!("{error:?}")),
                }
            }
            "release" => match lease.take().unwrap().release_owned_detailed().await {
                Ok(value) => record["result"] = json!(value),
                Err(error) => record["error"] = json!(format!("{error:?}")),
            },
            "ping" => {
                match transport
                    .as_ref()
                    .unwrap()
                    .command_classified::<String, _>(
                        "return redis.call('PING')",
                        &[],
                        &[],
                        Arc::new(()),
                    )
                    .await
                {
                    Ok((value, _)) => record["result"] = json!(value),
                    Err(error) => record["error"] = json!(format!("{error:?}")),
                }
            }
            "settle" => {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                while locks.as_ref().unwrap().available_leases() != 1
                    && tokio::time::Instant::now() < deadline
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                record["settled"] = json!(locks.as_ref().unwrap().available_leases() == 1);
            }
            "shutdown" => {
                assert!(lease.is_none());
                locks = None;
                coordinator = None;
                transport = None;
                record["done"] = json!(true);
            }
            _ => panic!("Owned probe action only"),
        }
        record["available_leases"] = json!(locks.as_ref().map(TaskLocks::available_leases));
        record["elapsed_seconds"] = json!(begin.elapsed().as_secs_f64());
        println!("{}", record);
        std::io::stdout().flush().unwrap();
        if action == "shutdown" {
            break;
        }
    }
}
