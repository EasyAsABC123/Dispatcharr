//! Isolated original-schema HDHR fixture. Original Dispatcharr author credit retained.
use dispatcharr_hdhr_http::{HdhrConfig, HdhrGateway};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
#[tokio::main]
async fn main() {
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new(std::env::var("SECRET_KEY").unwrap()).unwrap(),
    )
    .with_notification_cache(
        NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    );
    let gateway = HdhrGateway::new(store, auth, HdhrConfig::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let router = gateway.router();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    println!("{}", json!({"ready":true,"port":port}));
    io::stdout().flush().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let reader = tokio::task::spawn_blocking(move || {
        for line in io::stdin().lock().lines() {
            let line = line.unwrap();
            if line.len() > 1024 {
                break;
            }
            let value: Value = serde_json::from_str(&line).unwrap();
            let close = value["op"] == "close";
            if tx.blocking_send(value).is_err() || close {
                break;
            }
        }
    });
    while let Some(value) = rx.recv().await {
        if value["op"] == "close" {
            break;
        }
        println!(
            "{}",
            json!({"active_requests":gateway.active_requests(),"available_bytes":gateway.available_response_bytes()})
        );
        io::stdout().flush().unwrap();
    }
    gateway.shutdown().await;
    let _ = stop.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), server).await;
    reader.abort();
    println!(
        "{}",
        json!({"closed":true,"active_requests":gateway.active_requests(),"available_bytes":gateway.available_response_bytes()})
    );
    io::stdout().flush().unwrap();
}
