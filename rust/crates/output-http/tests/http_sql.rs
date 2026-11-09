//! Native HTTP/auth/cache/body comparison on actual original Django schema.
//! All original application credit belongs to Dispatcharr's authors/contributors.
use dispatcharr_output_http::{OutputConfig, OutputGateway};
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use std::time::Duration;

#[tokio::test]
#[ignore = "requires disposable Django PostgreSQL/Redis fixture and native driver"]
async fn actual_m3u_http_auth_csrf_cache_and_bytes_match_original() {
    assert_eq!(
        std::env::var("M3U_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(database.contains("playback-fixture-only@"));
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis.contains("playback-fixture-only@"));
    let transport = RedisTransport::connect(
        &redis,
        TransportConfig {
            max_operations: 1,
            max_queued_operations: 1,
            command_timeout: Duration::from_secs(3),
        },
    )
    .await
    .unwrap();
    // Clear only export content/dedup keys in this isolated fixture. Both
    // backends must render their own cold data rather than borrowing Source output.
    let (_,_):(i64,())=transport.command("local cursor='0';local n=0;repeat local r=redis.call('SCAN',cursor,'MATCH',':1:m3u_*','COUNT',100);cursor=r[1];for _,key in ipairs(r[2]) do redis.call('DEL',key);n=n+1;if n>50000 then return redis.error_reply('fixture limit') end end until cursor=='0';return n",&[],&[],()).await.unwrap();
    let store = Store::connect(&database, 16).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&redis).await.unwrap());
    let gateway = OutputGateway::new(store, auth, &redis, OutputConfig::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = gateway.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let mut command = tokio::process::Command::new("python");
    command
        .arg(std::env::var("M3U_HTTP_DRIVER").unwrap())
        .env("M3U_HTTP_BASE", base)
        .kill_on_drop(true);
    let result = tokio::time::timeout(Duration::from_secs(180), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        gateway.shutdown().await,
        "all request/body admission must drain"
    );
    server.abort();
    assert!(
        result.status.success(),
        "native HTTP driver failed: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
