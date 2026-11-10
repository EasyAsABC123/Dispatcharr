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
async fn persistent_client_owner_protocol() {
    use dispatcharr_clients::{ClientConfig, ClientStore};
    let (url, channel) = config();
    let mut rx = input();
    let mut owner: Option<ClientStore> = None;
    let mut probe: Option<dispatcharr_clients::ClientLease> = None;
    while let Some(line) = rx.recv().await {
        let command: Value = serde_json::from_str(&line).unwrap();
        let action = command["action"].as_str().unwrap();
        let mut result = json!({"action":action,"pid":std::process::id()});
        match action {
            "init" | "rebuild" => {
                drop(probe.take());
                if let Some(old) = owner.take() {
                    old.shutdown().await;
                }
                match ClientStore::connect(
                    &url,
                    ClientConfig {
                        worker_id: "owned-restart-peer".into(),
                        max_clients: 2,
                        command_timeout: Duration::from_millis(300),
                        flush_interval: Duration::from_millis(30),
                        ..Default::default()
                    },
                )
                .await
                {
                    Ok(value) => {
                        owner = Some(value);
                        result["ready"] = json!(true);
                    }
                    Err(e) => result["error"] = json!(format!("{e:?}")),
                }
            }
            "probe_register" => {
                let viewer = dispatcharr_contracts::PlaybackViewer {
                    user_id: None,
                    username: None,
                    ip_address: "127.0.0.1".parse().unwrap(),
                    user_agent: "owned-restart-accounting".into(),
                    stream_limit: 0,
                    limit_policy: Default::default(),
                };
                match owner
                    .as_ref()
                    .unwrap()
                    .register(&channel, &viewer, "mpegts")
                    .await
                {
                    Ok(lease) => {
                        lease.record_bytes(8192);
                        result["id"] = json!(lease.id());
                        probe = Some(lease);
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                    Err(error) => result["error"] = json!(format!("{error:?}")),
                }
            }
            "read" => match owner.as_ref().unwrap().channel_clients(&channel).await {
                Ok(value) => result["result"] = json!(value.len()),
                Err(e) => result["error"] = json!(format!("{e:?}")),
            },
            "shutdown" => {
                if let Some(old) = owner.take() {
                    old.shutdown().await;
                }
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
