//! Actual original-schema cancellation/retained-response resource witnesses.
//! Original Dispatcharr application authors retain credit.
use super::*;
use dispatcharr_store::{AuthConfig, NotificationCache};
use http_body_util::BodyExt;
use std::future::IntoFuture;
async fn fixture() -> (XcGateway, String) {
    let url = std::env::var("DATABASE_TEST_URL").expect("disposable original PostgreSQL fixture");
    assert!(url.contains("playback-fixture-only"));
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let store = Store::connect(&url, 4).await.unwrap();
    let user = store
        .user_by_username(
            &std::env::var("XC_FIXTURE_STANDARD_USERNAME")
                .unwrap_or_else(|_| "xc-read-standard".to_owned()),
        )
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

/// Component HTTP peer: real bounded SQL/Redis authorization, never Main. The
/// host controller owns all requests, stop, process/PID and SQL audit receipts.
#[tokio::test]
#[ignore = "requires reviewed Source26 owned fixture and once cue"]
async fn source26_bounded_vod_list_http_peer() {
    use axum::routing::get;
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(database.contains("playback-fixture-only"));
    let token = std::env::var("XC_LIST_FIXTURE_TOKEN").unwrap();
    assert_eq!(token.len(), 32);
    assert!(token.bytes().all(|b| b.is_ascii_hexdigit()));
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let store = Store::connect(&database, 4).await.unwrap();
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
    let gateway = XcGateway::new(store, auth, counts, XcConfig::default())
        .unwrap()
        .with_vod_lists_for_test();
    let stopped = Arc::new(Notify::new());
    let signal = stopped.clone();
    let health_token = token.clone();
    let control = Router::new()
        .route(&format!("/__owned/{token}/ready"), get(move || { let token=health_token.clone(); async move {
            axum::Json(json!({"fixture_token":token,"PID":std::process::id(),"peer":"Source26-bounded-VOD-list"}))
        }}))
        .route(&format!("/__owned/{token}/stop"), get(move || { let signal=signal.clone(); async move {
            signal.notify_one(); axum::Json(json!({"stop_requested":true}))
        }}));
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 5657))
        .await
        .unwrap();
    let server = axum::serve(
        listener,
        gateway
            .router()
            .merge(control)
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        stopped.notified().await;
    });
    tokio::time::timeout(Duration::from_secs(180), server.into_future())
        .await
        .expect("finite peer lifetime")
        .unwrap();
    assert!(
        gateway.shutdown().await,
        "peer must physically settle all work/frames"
    );
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(
        gateway.available_memory_bytes(),
        gateway.inner.config.max_memory_bytes
    );
}

async fn bounded_user_sql_owner(observer_deadline: bool) {
    let (gateway, query) = fixture().await;
    let maximum = gateway.available_memory_bytes();
    let mut locked = gateway.inner.store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE accounts_user IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *locked)
        .await
        .unwrap();
    let worker = gateway.clone();
    let requested = request(&format!("{query}&action=get_vod_categories"), false);
    let caller = tokio::spawn(async move { worker.handle(requested).await });
    tokio::time::timeout(Duration::from_secs(4),async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'SELECT id::bigint AS id,is_active,user_level,stream_limit,CASE WHEN octet_length(username)%')").fetch_one(gateway.inner.store.pool()).await.unwrap();
            if blocked { break; }tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    if observer_deadline {
        assert!(tokio::time::timeout(Duration::from_secs(16), caller)
            .await
            .unwrap()
            .unwrap()
            .is_err());
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
#[ignore = "requires reviewed owned PG/Redis fixture; unchanged15s observer"]
async fn bounded_user_sql_after_deadline_keeps_existing_grants() {
    bounded_user_sql_owner(true).await;
}
#[tokio::test]
#[ignore = "requires reviewed owned PG/Redis fixture"]
async fn bounded_user_sql_after_caller_loss_keeps_existing_grants() {
    bounded_user_sql_owner(false).await;
}

async fn owned_throttle_value() -> Option<Vec<u8>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
    async fn line(socket: &mut BufReader<tokio::net::TcpStream>) -> String {
        let mut raw = Vec::with_capacity(32);
        for _ in 0..32 {
            raw.push(socket.read_u8().await.unwrap());
            if raw.ends_with(b"\r\n") {
                return String::from_utf8(raw).unwrap();
            }
        }
        panic!("bounded owned RESP line exceeded32bytes")
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        let address = std::env::var("REDIS_TEST_URL").unwrap();
        assert!(address.contains("playback-fixture-only"));
        let url = url::Url::parse(&address).unwrap();
        let socket =
            tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap_or(6379)))
                .await
                .unwrap();
        let mut socket = BufReader::with_capacity(8192, socket);
        let password = url.password().unwrap();
        let auth = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{password}\r\n", password.len());
        socket.get_mut().write_all(auth.as_bytes()).await.unwrap();
        assert_eq!(line(&mut socket).await, "+OK\r\n");
        let key = ":1:throttle_xc_auth_127.0.0.77";
        let request = format!("*2\r\n$3\r\nGET\r\n${}\r\n{key}\r\n", key.len());
        socket
            .get_mut()
            .write_all(request.as_bytes())
            .await
            .unwrap();
        let header = line(&mut socket).await;
        let result = if header == "$-1\r\n" {
            None
        } else {
            assert!(header.starts_with('$') && header.ends_with("\r\n"));
            let length = header[1..header.len() - 2].parse::<usize>().unwrap();
            assert!(length <= 8192);
            let mut value = vec![0u8; length + 2];
            socket.read_exact(&mut value).await.unwrap();
            assert_eq!(&value[length..], b"\r\n");
            value.truncate(length);
            Some(value)
        };
        socket.get_mut().shutdown().await.unwrap();
        drop(socket);
        result
    })
    .await
    .expect("finite owned RESP observation")
}

#[tokio::test]
#[ignore = "requires disposable owned user; exact SQL restore and Redis-key observation"]
async fn bounded_user_oversize_is_not_a_failed_login_mutation() {
    let (gateway, query) = fixture().await;
    let username =
        std::env::var("XC_FIXTURE_STANDARD_USERNAME").unwrap_or_else(|_| "xc-read-standard".into());
    let user = gateway
        .inner
        .store
        .user_by_username(&username)
        .await
        .unwrap()
        .unwrap();
    let original = user.custom_properties;
    let before = owned_throttle_value().await;
    assert!(
        before.is_none(),
        "separate predeclared fixture IP must start with no throttle state"
    );
    let before_events: i64 = sqlx::query_scalar("SELECT count(*) FROM core_systemevent")
        .fetch_one(gateway.inner.store.pool())
        .await
        .unwrap();
    let oversized = json!({"xc_password":original["xc_password"],"padding":"x".repeat(70*1024)});
    sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2")
        .bind(&oversized)
        .bind(user.id)
        .execute(gateway.inner.store.pool())
        .await
        .unwrap();
    let mut requested = request(&query, false);
    requested
        .extensions_mut()
        .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 77], 44000))));
    let observed = gateway.handle(requested).await;
    let after = owned_throttle_value().await;
    let after_events: i64 = sqlx::query_scalar("SELECT count(*) FROM core_systemevent")
        .fetch_one(gateway.inner.store.pool())
        .await
        .unwrap();
    // Restore the committed row before asserting the Native result. Source
    // setup rows/sequences and host full manifests are independently bracketed.
    sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2")
        .bind(&original)
        .bind(user.id)
        .execute(gateway.inner.store.pool())
        .await
        .unwrap();
    assert!(matches!(observed, Err(StoreError::StatusBody(503, _))));
    assert_eq!(before, after);
    assert_eq!(before_events, after_events);
    assert!(gateway.shutdown().await);
    assert_eq!(
        gateway.available_memory_bytes(),
        gateway.inner.config.max_memory_bytes
    );
}

#[tokio::test]
#[ignore = "requires reviewed Source26 owned PG/Redis fixture and new list producer"]
async fn bounded_list_final_frame_keeps_same_grant_through_shutdown() {
    let (gateway, query) = fixture().await;
    let gateway = gateway.with_vod_lists_for_test();
    let maximum = gateway.available_memory_bytes();
    let response = gateway
        .handle(request(&format!("{query}&action=get_vod_streams"), false))
        .await
        .unwrap();
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(!frame.is_empty());
    let held = frame.clone();
    drop(frame);
    drop(body);
    let charged = maximum - gateway.available_memory_bytes();
    assert!(charged >= held.len());
    assert!(charged < gateway.inner.config.max_response_bytes);
    assert!(!gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 1);
    drop(held);
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}

#[test]
fn smaller_custom_request_quote_cannot_retain_larger_headers() {
    assert!(bounded_header_size(4, 4).is_ok());
    assert!(matches!(
        bounded_header_size(5, 4),
        Err(StoreError::StatusBody(503, _))
    ));
    assert!(bounded_header_size(64 * 1024, 1024 * 1024).is_ok());
    assert!(bounded_header_size(64 * 1024 + 1, 1024 * 1024).is_err());
}

/// Component HTTP peer: real bounded SQL/Redis authorization, never Main. The
/// host controller owns all requests, stop, process/PID and SQL audit receipts.
#[tokio::test]
#[ignore = "requires reviewed Source12 owned fixture and once cue"]
async fn source12_bounded_cached_info_http_peer() {
    use axum::routing::get;
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(database.contains("playback-fixture-only"));
    let token = std::env::var("XC_INFO_FIXTURE_TOKEN").unwrap();
    assert_eq!(token.len(), 32);
    assert!(token.bytes().all(|b| b.is_ascii_hexdigit()));
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    let store = Store::connect(&database, 4).await.unwrap();
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
    let gateway = XcGateway::new(store, auth, counts, XcConfig::default())
        .unwrap()
        .with_vod_info_for_test(
            std::env::var("XC_INFO_ELIGIBILITY_MICROS")
                .unwrap()
                .parse()
                .unwrap(),
        );
    let stopped = Arc::new(Notify::new());
    let signal = stopped.clone();
    let health_token = token.clone();
    let control = Router::new()
        .route(&format!("/__owned/{token}/ready"), get(move || { let token=health_token.clone(); async move {
            axum::Json(json!({"fixture_token":token,"PID":std::process::id(),"peer":"Source12-bounded-cached-info"}))
        }}))
        .route(&format!("/__owned/{token}/stop"), get(move || { let signal=signal.clone(); async move {
            signal.notify_one(); axum::Json(json!({"stop_requested":true}))
        }}));
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 5657))
        .await
        .unwrap();
    let server = axum::serve(
        listener,
        gateway
            .router()
            .merge(control)
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        stopped.notified().await;
    });
    tokio::time::timeout(Duration::from_secs(180), server.into_future())
        .await
        .expect("finite peer lifetime")
        .unwrap();
    assert!(
        gateway.shutdown().await,
        "peer must physically settle all work/frames"
    );
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(
        gateway.available_memory_bytes(),
        gateway.inner.config.max_memory_bytes
    );
}

// Native definition-bound ownership controls, not new Source interleaving authority.
async fn cached_info_fixture(series: bool) -> (XcGateway, String) {
    let (gateway, query) = fixture().await;
    let clock = std::env::var("XC_INFO_ELIGIBILITY_MICROS")
        .unwrap()
        .parse()
        .unwrap();
    let gateway = gateway.with_vod_info_for_test(clock);
    let id: i64 = std::env::var(if series {
        "XC_INFO_SERIES_ID"
    } else {
        "XC_INFO_MOVIE_ID"
    })
    .unwrap()
    .parse()
    .unwrap();
    assert!(id > 0);
    let action = if series {
        "get_series_info&series_id"
    } else {
        "get_vod_info&vod_id"
    };
    (gateway, format!("{query}&action={action}={id}"))
}
async fn cached_info_sql_physical_owner(series: bool, observer_deadline: bool) {
    let (gateway, query) = cached_info_fixture(series).await;
    assert_eq!(
        gateway.inner.config.request_deadline,
        Duration::from_secs(15)
    );
    let maximum = gateway.available_memory_bytes();
    let mut locked = gateway.inner.store.pool().begin().await.unwrap();
    let lock = if series {
        "LOCK TABLE vod_episode IN ACCESS EXCLUSIVE MODE"
    } else {
        "LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE"
    };
    sqlx::query(lock).execute(&mut *locked).await.unwrap();
    let worker = gateway.clone();
    let requested = request(&query, false);
    let caller = tokio::spawn(async move { worker.handle(requested).await });
    let prefix = if series {
        "SELECT id,air_date,duration_secs,%"
    } else {
        "SELECT relation_id,account_id,%"
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE $1)").bind(prefix).fetch_one(gateway.inner.store.pool()).await.unwrap();
            if blocked { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    // In the Series case this query is reached only after the separate parent
    // lookup and cached eligibility preparation. The parent remains charged.
    if observer_deadline {
        assert!(tokio::time::timeout(Duration::from_secs(16), caller)
            .await
            .unwrap()
            .unwrap()
            .is_err());
    } else {
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
    }
    assert_eq!(gateway.active_requests(), 1);
    assert!(gateway.available_memory_bytes() < maximum);
    assert!(gateway.handle(request(&query, false)).await.is_err());
    let closing = gateway.clone();
    let shutdown = tokio::spawn(async move { closing.shutdown().await });
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!shutdown.is_finished());
    locked.rollback().await.unwrap();
    assert!(shutdown.await.unwrap());
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}
#[tokio::test]
#[ignore = "requires reviewed cached-info PG/Redis fixture and fixed eligibility clock"]
async fn cached_info_parent_after_deadline_keeps_existing_owners() {
    cached_info_sql_physical_owner(false, true).await;
}
#[tokio::test]
#[ignore = "requires reviewed cached-info PG/Redis fixture and fixed eligibility clock"]
async fn cached_info_parent_after_caller_loss_keeps_existing_owners() {
    cached_info_sql_physical_owner(false, false).await;
}
#[tokio::test]
#[ignore = "requires reviewed cached-info PG/Redis fixture and fixed eligibility clock"]
async fn cached_info_episode_after_caller_loss_keeps_parent_and_existing_owners() {
    cached_info_sql_physical_owner(true, false).await;
}
#[tokio::test]
#[ignore = "requires reviewed cached-info PG/Redis fixture and fixed eligibility clock"]
async fn cached_info_final_frame_keeps_existing_owners_through_shutdown() {
    let (gateway, query) = cached_info_fixture(true).await;
    let maximum = gateway.available_memory_bytes();
    let response = gateway.handle(request(&query, false)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert!(!frame.is_empty());
    let held = frame.clone();
    drop(frame);
    drop(body);
    let charged = maximum - gateway.available_memory_bytes();
    assert!(charged >= held.len());
    assert!(charged < gateway.inner.config.max_response_bytes);
    assert!(!gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 1);
    drop(held);
    assert!(gateway.shutdown().await);
    assert_eq!(gateway.active_requests(), 0);
    assert_eq!(gateway.available_memory_bytes(), maximum);
}
