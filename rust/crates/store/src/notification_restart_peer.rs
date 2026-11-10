//! Disposable cached-owner restart protocol; no deployment or provider work.
//! Original Dispatcharr authors retain full application and algorithm credit.
use serde_json::{json, Value};
use std::{
    io::{BufRead, Read, Write},
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
async fn persistent_notification_owner_protocol() {
    use super::NotificationCache;
    let (url, _channel) = config();
    let mut rx = input();
    let mut owner: Option<NotificationCache> = None;
    while let Some(line) = rx.recv().await {
        let command: Value = serde_json::from_str(&line).unwrap();
        let action = command["action"].as_str().unwrap();
        let mut result = json!({"action":action,"pid":std::process::id()});
        match action {
            "init" | "rebuild" => match NotificationCache::connect(&url).await {
                Ok(value) => {
                    owner = Some(value);
                    result["ready"] = json!(true);
                }
                Err(_) => result["error"] = json!("cache unavailable"),
            },
            "read" => match owner
                .as_ref()
                .unwrap()
                .get(":1:coresettings:group:system_settings")
                .await
            {
                Ok(value) => {
                    result["result"] = value
                        .as_deref()
                        .and_then(crate::decode_source_cache)
                        .unwrap_or(Value::Null)
                }
                Err(_) => result["error"] = json!("cache unavailable"),
            },
            "shutdown" => {
                result["physical_operations"] =
                    json!(owner.as_ref().unwrap().physical_operations());
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
