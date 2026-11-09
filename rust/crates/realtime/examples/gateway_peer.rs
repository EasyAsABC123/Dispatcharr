//! Isolated native WebSocket fixture; original Dispatcharr authors retain credit.
//! Credentials enter through environment and are never printed.
use dispatcharr_realtime::{BusConfig, GatewayConfig, RealtimeBus, RealtimeGateway};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use serde_json::json;
use std::io::{self, BufRead, Write};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let bus = RealtimeBus::connect(&redis, BusConfig::default())
        .await
        .unwrap();
    let auth = AuthService::new(
        store,
        AuthConfig::new(std::env::var("SECRET_KEY").unwrap()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&redis).await.unwrap())
    .with_event_sink(std::sync::Arc::new(bus.clone()));
    let gateway = RealtimeGateway::new(auth, bus, GatewayConfig::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (stop, stopped) = oneshot::channel();
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
    let (commands, mut input) = tokio::sync::mpsc::channel(1);
    let reader = tokio::task::spawn_blocking(move || {
        for line in io::stdin().lock().lines() {
            let line = line.unwrap();
            if line.len() > 1024 {
                break;
            }
            let closed = line == "close";
            if commands.blocking_send(line).is_err() || closed {
                break;
            }
        }
    });
    while let Some(line) = input.recv().await {
        match line.as_str() {
            "diagnostics" => println!("{}", gateway.diagnostics()),
            "close" => break,
            _ => println!("{}", json!({"unknown_command":true})),
        }
        io::stdout().flush().unwrap();
    }
    let clean = gateway.shutdown().await;
    let _ = stop.send(());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), server).await;
    reader.abort();
    println!(
        "{}",
        json!({"closed":true,"clean":clean,"diagnostics":gateway.diagnostics()})
    );
    io::stdout().flush().unwrap();
}
