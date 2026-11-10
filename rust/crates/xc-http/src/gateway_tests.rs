//! Actual original-schema cancellation/retained-response resource witnesses.
//! Original Dispatcharr application authors retain credit.
use super::*;
use dispatcharr_store::{AuthConfig, NotificationCache};
use http_body_util::BodyExt;
async fn fixture() -> (XcGateway, String) {
    let url = std::env::var("DATABASE_TEST_URL").expect("disposable original PostgreSQL fixture");
    assert!(url.contains("playback-fixture-only"));
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let store = Store::connect(&url, 4).await.unwrap();
    let user = store
        .user_by_username("xc-read-standard")
        .await
        .unwrap()
        .unwrap();
    let password = user.custom_properties["xc_password"].as_str().unwrap();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("username", &user.username)
        .append_pair("password", password)
        .finish();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new(std::env::var("SECRET_KEY").unwrap()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&redis).await.unwrap());
    let counts = ConnectionCounts::connect(&redis, |value| {
        dispatcharr_ingest_core::source_float_literal(value).is_some()
    })
    .await
    .unwrap();
    let config = XcConfig {
        max_requests: 1,
        ..XcConfig::default()
    };
    (XcGateway::new(store, auth, counts, config).unwrap(), query)
}
fn request(query: &str, live: bool) -> Request<Body> {
    let query = if live {
        format!("{query}&action=get_live_streams")
    } else {
        query.to_owned()
    };
    Request::builder()
        .uri(format!("/player_api.php?{query}"))
        .header("host", "xc.fixture.invalid:8088")
        .body(Body::empty())
        .unwrap()
}
#[tokio::test]
#[ignore = "requires disposable original PostgreSQL/Redis XC fixture"]
async fn held_final_frame_keeps_exact_capacity_lease_after_shutdown() {
    let (gateway, query) = fixture().await;
    let maximum = gateway.inner.config.max_memory_bytes;
    let response = gateway.handle(request(&query, false)).await.unwrap();
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let length = frame.len();
    let clone = frame.clone();
    drop(frame);
    drop(body);
    let charged = maximum - gateway.available_memory_bytes();
    assert!(charged >= length);
    assert!(
        charged < gateway.inner.config.max_response_bytes,
        "small responses must not retain the whole maximum buffer"
    );
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(gateway.available_memory_bytes(), maximum - charged);
    drop(clone);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}
#[tokio::test]
#[ignore = "requires disposable original PostgreSQL/Redis XC fixture"]
async fn cancelled_sql_caller_retains_admission_until_owned_worker_finishes() {
    let (gateway, query) = fixture().await;
    let mut locked = gateway.inner.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE dispatcharr_channels_channel IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *locked)
        .await
        .unwrap();
    let worker = gateway.clone();
    let requested = request(&query, true);
    let caller = tokio::spawn(async move { worker.handle(requested).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while gateway.active_requests() != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(gateway.active_requests(), 1);
    assert!(gateway.handle(request(&query, true)).await.is_err());
    locked.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.active_requests() != 0
            || gateway.available_memory_bytes() != gateway.inner.config.max_memory_bytes
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(gateway.shutdown().await);
}

#[tokio::test]
#[ignore = "requires disposable original PostgreSQL/Redis XC fixture"]
async fn epg_sql_cancellation_and_detached_body_keep_bounded_lifetimes() {
    let (gateway, query) = fixture().await;
    let gateway = gateway.with_epg();
    let pk: i64 = sqlx::query_scalar(
        "SELECT id::bigint FROM dispatcharr_channels_channel WHERE name='XC channel 0'",
    )
    .fetch_one(gateway.inner.store.pool())
    .await
    .unwrap();
    let query = format!("{query}&action=get_short_epg&stream_id={pk}");
    let maximum = gateway.inner.config.max_memory_bytes;
    let mut locked = gateway.inner.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE dispatcharr_channels_channel IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *locked)
        .await
        .unwrap();
    let worker = gateway.clone();
    let requested = request(&query, false);
    let caller = tokio::spawn(async move { worker.handle(requested).await });
    tokio::time::timeout(Duration::from_secs(3),async{
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT c.id::bigint AS id,COALESCE(o.channel_number%')").fetch_one(gateway.inner.store.pool()).await.unwrap();
            if waiting{break}tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    caller.abort();
    let _ = caller.await;
    assert_eq!(gateway.active_requests(), 1);
    assert!(gateway.handle(request(&query, false)).await.is_err());
    locked.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.active_requests() != 0 || gateway.available_memory_bytes() != maximum {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let response = gateway.handle(request(&query, false)).await.unwrap();
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let clone = frame.clone();
    drop(frame);
    drop(body);
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert!(gateway.available_memory_bytes() < maximum);
    drop(clone);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}
