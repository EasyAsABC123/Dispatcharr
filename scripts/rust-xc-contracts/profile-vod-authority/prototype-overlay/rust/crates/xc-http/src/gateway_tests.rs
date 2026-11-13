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
    assert_eq!(gateway.active_requests(), 1);
    assert!(
        !gateway.shutdown().await,
        "held final bytes must prevent a drain claim"
    );
    assert_eq!(gateway.active_requests(), 1);
    assert_eq!(gateway.available_memory_bytes(), maximum - charged);
    drop(clone);
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
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
    assert_eq!(gateway.active_requests(), 1);
    let shutdown_gateway = gateway.clone();
    let shutdown = tokio::spawn(async move { shutdown_gateway.shutdown().await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!shutdown.is_finished());
    assert!(gateway.available_memory_bytes() < maximum);
    drop(clone);
    assert!(shutdown.await.unwrap());
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}

/// Real PG lock witness: the observer's fixed 15-second deadline and caller
/// loss must not cancel a pending query or release its existing byte/request
/// grants. Shutdown remains pending until that same query settles physically.
async fn category_sql_physical_owner(observer_deadline: bool) {
    let (gateway, query) = fixture().await;
    assert_eq!(
        gateway.inner.config.request_deadline,
        Duration::from_secs(15)
    );
    let maximum = gateway.available_memory_bytes();
    let mut locked = gateway.inner.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE vod_vodcategory IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *locked)
        .await
        .unwrap();
    let query = format!("{query}&action=get_vod_categories");
    let worker = gateway.clone();
    let requested = request(&query, false);
    let caller = tokio::spawn(async move { worker.handle(requested).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT id,CASE WHEN octet_length(name)%')"
            ).fetch_one(gateway.inner.store.pool()).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    if observer_deadline {
        let observed = tokio::time::timeout(Duration::from_secs(16), caller)
            .await
            .unwrap()
            .unwrap();
        assert!(observed.is_err());
    } else {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
    }
    assert_eq!(gateway.active_requests(), 1);
    assert!(gateway.available_memory_bytes() < maximum);
    let closing = gateway.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!shutdown.is_finished());
    assert_eq!(gateway.active_requests(), 1);
    locked.rollback().await.unwrap();
    assert!(shutdown.await.unwrap());
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}
#[tokio::test]
#[ignore = "requires disposable original PostgreSQL/Redis XC fixture; fixed 15s observer"]
async fn category_sql_after_observer_deadline_keeps_physical_owner() {
    category_sql_physical_owner(true).await;
}
#[tokio::test]
#[ignore = "requires disposable original PostgreSQL/Redis XC fixture"]
async fn category_sql_after_caller_loss_keeps_physical_owner() {
    category_sql_physical_owner(false).await;
}

/// Local task/channel controls test actual JoinHandle detachment and permits,
/// not a model of SQL settlement. Real PostgreSQL tests above remain required.
async fn observer_keeps_same_physical_grants(caller_loss: bool) {
    let requests = Arc::new(Semaphore::new(1));
    let memory = Arc::new(Semaphore::new(4096));
    let request = requests.clone().acquire_owned().await.unwrap();
    let bytes = memory.clone().acquire_many_owned(4096).await.unwrap();
    let (settle, settled) = tokio::sync::oneshot::channel::<()>();
    let (finished, final_ack) = tokio::sync::oneshot::channel::<()>();
    let physical = tokio::spawn(async move {
        let data = vec![1u8; 4096];
        settled.await.unwrap();
        drop(data);
        drop(bytes);
        drop(request);
        finished.send(()).unwrap();
        Ok::<(), StoreError>(())
    });
    let observer = tokio::spawn(observe_physical(Duration::from_millis(20), physical));
    if caller_loss {
        tokio::task::yield_now().await;
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
    } else {
        assert!(observer.await.unwrap().is_err());
    }
    assert_eq!(requests.available_permits(), 0);
    assert_eq!(memory.available_permits(), 0);
    settle.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), final_ack)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(requests.available_permits(), 1);
    assert_eq!(memory.available_permits(), 4096);
}
#[tokio::test]
async fn observer_expiry_does_not_abort_owned_task_or_release_grants() {
    observer_keeps_same_physical_grants(false).await;
}
#[tokio::test]
async fn observer_caller_loss_does_not_abort_owned_task_or_release_grants() {
    observer_keeps_same_physical_grants(true).await;
}

async fn pause_owned_auth_cache() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let value = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(value.contains("playback-fixture-only"));
    let url = url::Url::parse(&value).unwrap();
    let socket =
        tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap_or(6379)))
            .await
            .unwrap();
    let mut socket = BufReader::new(socket);
    let password = url.password().unwrap();
    let auth = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{password}\r\n", password.len());
    socket.get_mut().write_all(auth.as_bytes()).await.unwrap();
    let mut line = String::new();
    socket.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");
    // Only this fixture's Redis. The real 2s cache observers return before
    // the actual replies; no timeout/provider/application method replacement.
    socket
        .get_mut()
        .write_all(b"*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$4\r\n6200\r\n")
        .await
        .unwrap();
    line.clear();
    socket.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");
}
async fn scoped_auth_cache_owner(caller_loss: bool) {
    let (gateway, query) = fixture().await;
    let maximum = gateway.available_memory_bytes();
    let mut locked = gateway.inner.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *locked)
        .await
        .unwrap();
    pause_owned_auth_cache().await;
    let task_gateway = gateway.clone();
    let requested = request(&format!("{query}&action=get_vod_categories"), false);
    let caller = tokio::spawn(async move { task_gateway.handle(requested).await });
    // Observe the actual cache-timeout -> SQL fallback, not merely an admitted
    // request or a sleep. The owned table lock prevents that SQL from settling.
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%FROM core_coresettings%')"
            ).fetch_one(gateway.inner.store.pool()).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    assert_eq!(gateway.active_requests(), 1);
    if caller_loss {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let closing = gateway.clone();
        let shutdown = tokio::spawn(async move { closing.shutdown().await });
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(!shutdown.is_finished());
        assert_eq!(gateway.active_requests(), 1);
        locked.rollback().await.unwrap();
        assert!(shutdown.await.unwrap());
    } else {
        locked.rollback().await.unwrap();
        // The subsequent real throttle-cache timeout returns an error while
        // physical replies from this same request remain pending.
        assert!(tokio::time::timeout(Duration::from_secs(5), caller)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(gateway.active_requests(), 1);
        assert_eq!(gateway.available_memory_bytes(), maximum);
    }
    tokio::time::timeout(Duration::from_secs(8), async {
        while gateway.active_requests() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(gateway.available_memory_bytes(), maximum);
    assert!(gateway.shutdown().await);
}
#[tokio::test]
#[ignore = "requires fresh disposable PG/Redis; pauses only owned Redis 6200ms"]
async fn auth_cache_logical_error_keeps_admission_until_actual_reply() {
    scoped_auth_cache_owner(false).await;
}
#[tokio::test]
#[ignore = "requires fresh disposable PG/Redis; pauses only owned Redis 6200ms"]
async fn auth_cache_caller_loss_and_shutdown_keep_admission_until_actual_reply() {
    scoped_auth_cache_owner(true).await;
}
