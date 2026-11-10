//! Disposable cached-owner restart protocol; no deployment or provider work.
//! Original Dispatcharr authors retain full application and algorithm credit.
use serde_json::{json, Value};
use std::{
    io::{BufRead, Read, Write},
    time::Duration,
};
fn input() -> tokio::sync::mpsc::Receiver<String> {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        loop {
            let mut line = String::new();
            let n = input.by_ref().take(16385).read_line(&mut line).unwrap();
            if n == 0 || n > 16384 || tx.blocking_send(line).is_err() {
                break;
            }
        }
    });
    rx
}
fn config() -> (String, String) {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    assert_eq!(url, "redis://:playback-fixture-only@127.0.0.1:6379/0");
    let channel = std::env::var("RESTART_PROBE_TASK").unwrap();
    assert!(
        channel.starts_with("owned_redis_restart_")
            && channel
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
    );
    (url, channel)
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "owned restart protocol fixture only"]
async fn persistent_vod_owner_protocol() {
    use super::{source_has_active_streams, SessionPool};
    use dispatcharr_coordination::{Coordinator, LeaseConfig};
    let (url, channel) = config();
    let mut rx = input();
    let mut owner: Option<SessionPool> = None;
    while let Some(line) = rx.recv().await {
        let command: Value = serde_json::from_str(&line).unwrap();
        let action = command["action"].as_str().unwrap();
        let mut result = json!({"action":action,"pid":std::process::id()});
        match action {
            "init" | "rebuild" => {
                let coordinator = Coordinator::connect(
                    &url,
                    LeaseConfig {
                        namespace: channel.clone(),
                        heartbeat_interval: None,
                        reap_interval: None,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                match SessionPool::connect(&url, coordinator, 1, 1, Duration::from_millis(20)).await
                {
                    Ok(value) => {
                        owner = Some(value);
                        result["ready"] = json!(true);
                    }
                    Err(_) => result["error"] = json!("session unavailable"),
                }
            }
            "read" => {
                let mut connection = owner.as_ref().unwrap().inner.connection.clone();
                result["result"] =
                    json!(source_has_active_streams(&mut connection, &channel).await);
            }
            "shutdown" => {
                owner.as_ref().unwrap().drain().await;
                result["done"] = json!(true);
            }
            _ => panic!("owned action only"),
        }
        println!("{result}");
        std::io::stdout().flush().unwrap();
        if action == "shutdown" {
            break;
        }
    }
}

async fn physical_fixture(
    suffix: &str,
) -> (
    super::SessionPool,
    redis::aio::MultiplexedConnection,
    crate::VodSource,
    crate::VodRequest,
    String,
) {
    let (url, base) = config();
    let id = format!("{base}-{suffix}");
    let coordinator = dispatcharr_coordination::Coordinator::connect(
        &url,
        dispatcharr_coordination::LeaseConfig {
            namespace: id.clone(),
            heartbeat_interval: None,
            reap_interval: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let pool = super::SessionPool::connect(&url, coordinator, 1, 1, Duration::from_millis(10))
        .await
        .unwrap();
    let control = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let source = crate::VodSource {
        kind: crate::ContentKind::Movie,
        content_uuid: id.clone(),
        name: "owned delayed Redis fixture".into(),
        user: None,
        candidates: vec![crate::VodCandidate {
            relation_id: 1,
            account_id: 1,
            profile_id: 777,
            max_streams: 1,
            url: "http://fixture.invalid/no-network".into(),
            user_agent: "owned-restart".into(),
        }],
        redirect: false,
        limit_policy: Default::default(),
    };
    let request = crate::VodRequest {
        resolved_client_ip: None,
        playback: dispatcharr_contracts::PlaybackRequest {
            channel_id: id.clone(),
            query: vec![],
            headers: vec![],
            peer_ip: "127.0.0.1".parse().unwrap(),
        },
        kind: "movie".into(),
        content_id: id.clone(),
        session_id: None,
        profile_id: None,
    };
    (pool, control, source, request, id)
}
async fn pause(control: &mut redis::aio::MultiplexedConnection) {
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3000)
        .arg("ALL")
        .query_async(control)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "owned Redis physical delayed reply beyond former response deadline"]
async fn physical_dropped_ticket_retains_request_and_session_until_actual_reply() {
    let (pool, mut control, source, request, id) = physical_fixture("drop-ticket").await;
    let ticket = pool.enter(&id, source, &request).await.unwrap();
    pause(&mut control).await;
    drop(ticket);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(
        pool.inner.requests.available_permits(),
        0,
        "logical two-second deadline must not release a pending cleanup owner"
    );
    assert_eq!(pool.inner.slots.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(5), pool.drain())
        .await
        .unwrap();
    assert!(pool.cleanup_succeeded());
    let exists: bool = redis::cmd("EXISTS")
        .arg(super::key(&id))
        .query_async(&mut control)
        .await
        .unwrap();
    assert!(!exists);
    let count: Option<u64> = redis::cmd("GET")
        .arg("profile_connections:777")
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(count.unwrap_or(0), 0);
}
#[tokio::test]
#[ignore = "owned Redis canceled setup must retain physical admission"]
async fn physical_cancelled_setup_retains_request_and_session_until_actual_reply() {
    let (pool, mut control, source, request, id) = physical_fixture("cancel-setup").await;
    pause(&mut control).await;
    let worker = pool.clone();
    let task = tokio::spawn(async move { worker.enter(&id, source, &request).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while pool.inner.requests.available_permits() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(
        pool.inner.requests.available_permits(),
        0,
        "cancelled observer must not free physical setup admission"
    );
    assert_eq!(pool.inner.slots.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(5), pool.drain())
        .await
        .unwrap();
    assert!(pool.cleanup_succeeded());
}

#[path = "session_admit_reply.rs"]
mod admit_reply;

#[path = "session_recovery_causes.rs"]
mod recovery_causes;
