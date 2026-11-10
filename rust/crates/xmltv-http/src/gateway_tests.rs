//! Real SQL/Redis admission, held-body and cache-hit witnesses. Upstream credit.
use super::*;
use dispatcharr_store::{AuthConfig, NotificationCache};
use futures_util::StreamExt;

async fn fixture() -> Gateway {
    let redis = std::env::var("REDIS_TEST_URL").expect("owned actual Redis");
    let database = std::env::var("DATABASE_TEST_URL").expect("owned original schema");
    assert!(redis.contains("playback-fixture-only") && database.contains("playback-fixture-only"));
    let store = Store::connect(&database, 4).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&redis).await.unwrap());
    let config = Config {
        max_requests: 2,
        max_number_bytes: 8 * 1024 * 1024,
        rows: rows::Limits {
            max_channels: 1000,
            max_metadata_bytes: 8 * 1024 * 1024,
            ..Config::default().rows
        },
        ..Config::default()
    };
    Gateway::new(store, auth, &redis, config).await.unwrap()
}
fn unique_host() -> String {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).unwrap();
    format!("xmltv-{:x}.fixture.invalid", u64::from_be_bytes(bytes))
}
async fn response(gateway: &Gateway, host: &str) -> Response {
    let mut request = Request::builder()
        .uri("/output/epg?days=1")
        .header("host", host)
        .body(Body::empty())
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:43210".parse::<SocketAddr>().unwrap(),
    ));
    dispatch(
        State(gateway.clone()),
        OriginalUri(request.uri().clone()),
        request,
    )
    .await
}
#[tokio::test]
#[ignore = "requires real original-schema PG/Redis XMLTV fixture"]
async fn unpolled_bodies_admission_and_shutdown_are_bounded() {
    let gateway = fixture().await;
    let host = unique_host();
    let one = response(&gateway, &host).await;
    let two = response(&gateway, &host).await;
    assert_eq!(one.status(), StatusCode::OK);
    assert_eq!(two.status(), StatusCode::OK);
    assert_eq!(gateway.active_requests(), 2);
    assert_eq!(
        response(&gateway, &host).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(
        gateway.available_memory_bytes(),
        gateway.inner.config.max_memory_bytes
    );
    drop(one);
    drop(two);
}
#[tokio::test]
#[ignore = "requires real original-schema PG/Redis XMLTV fixture"]
async fn detached_frame_retains_only_memory_until_final_clone() {
    let gateway = fixture().await;
    let host = unique_host();
    let mut stream = response(&gateway, &host)
        .await
        .into_body()
        .into_data_stream();
    let frame = stream.next().await.unwrap().unwrap();
    assert_eq!(frame.as_ref(), content::PREAMBLE.as_bytes());
    let held = frame.clone();
    drop(frame);
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert!(gateway.available_memory_bytes() < gateway.inner.config.max_memory_bytes);
    drop(held);
    drop(stream);
    assert_eq!(
        gateway.available_memory_bytes(),
        gateway.inner.config.max_memory_bytes
    );
}
#[tokio::test]
#[ignore = "requires real original-schema PG/Redis XMLTV fixture"]
async fn cache_hit_serves_while_channel_table_is_locked() {
    let gateway = fixture().await;
    let host = unique_host();
    let original = consume(response(&gateway, &host).await.into_body()).await;
    assert!(original.ends_with(b"</tv>\n"));
    let mut transaction = gateway.inner.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE dispatcharr_channels_channel IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let replay = tokio::time::timeout(Duration::from_millis(300), async {
        consume(response(&gateway, &host).await.into_body()).await
    })
    .await;
    transaction.rollback().await.unwrap();
    assert_eq!(replay.unwrap(), original);
    assert!(gateway.shutdown().await);
}
#[tokio::test]
#[ignore = "requires real original-schema PG/Redis XMLTV fixture"]
async fn unsupported_custom_guides_are_explicit_before_xml_headers() {
    let gateway = fixture().await;
    let previous: serde_json::Value = sqlx::query_scalar(
        "SELECT custom_properties FROM epg_epgsource WHERE name='XMLTV empty dummy'",
    )
    .fetch_one(gateway.inner.store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE epg_epgsource SET custom_properties=$1 WHERE name='XMLTV empty dummy'")
        .bind(json!({"unverified_regex":".*"}))
        .execute(gateway.inner.store.pool())
        .await
        .unwrap();
    let result = response(&gateway, &unique_host()).await;
    sqlx::query("UPDATE epg_epgsource SET custom_properties=$1 WHERE name='XMLTV empty dummy'")
        .bind(previous)
        .execute(gateway.inner.store.pool())
        .await
        .unwrap();
    assert_eq!(result.status(), StatusCode::NOT_IMPLEMENTED);
    assert_eq!(result.headers()["content-type"], "application/json");
    assert!(gateway.shutdown().await);
}

async fn consume(body: Body) -> Vec<u8> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(frame) = stream.next().await {
        bytes.extend_from_slice(&frame.unwrap());
        assert!(bytes.len() < 8 * 1024 * 1024);
    }
    bytes
}
