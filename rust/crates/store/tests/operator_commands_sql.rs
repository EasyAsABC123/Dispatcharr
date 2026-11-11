//! Actual owned PostgreSQL/Redis physical ownership controls.
//! Original Dispatcharr authors retain application credit.
use dispatcharr_store::{
    operator_commands::{OperatorCommand, OperatorCommands, OperatorError},
    Store,
};
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Guard(Arc<AtomicBool>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst)
    }
}
async fn until(mut f: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !f().await {
            tokio::time::sleep(Duration::from_millis(5)).await
        }
    })
    .await
    .unwrap()
}
async fn fixture() -> Store {
    assert_eq!(
        std::env::var("OPERATOR_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(database.starts_with("rust_probe_backup_operator_"));
    store
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis only"]
async fn blocked_sql_cancel_retains_slot_caller_and_shutdown() {
    let store = fixture().await;
    let name: String = sqlx::query_scalar(
        "SELECT username FROM accounts_user WHERE username='rust-operator-owned-b'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let mut lock = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE accounts_user IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let service = OperatorCommands::new(
        store.clone(),
        std::env::var("REDIS_TEST_URL").unwrap(),
        None,
    );
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Arc::new(Guard(dropped.clone()));
    let own = service.clone();
    let task =
        tokio::spawn(async move { own.run(OperatorCommand::ResetUser(Some(name)), guard).await });
    until(async||sqlx::query_scalar::<_,i64>("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT id::bigint AS id, octet_length(custom_properties%' ").fetch_one(store.pool()).await.unwrap()>0).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(matches!(
        service
            .run(OperatorCommand::ResetUser(None), Arc::new(()))
            .await,
        Err(OperatorError::Capacity)
    ));
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    assert!(!dropped.load(Ordering::SeqCst));
    lock.rollback().await.unwrap();
    assert!(service.shutdown(Duration::from_secs(5)).await);
    assert!(dropped.load(Ordering::SeqCst));
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis only"]
async fn oversized_row_is_refused_before_any_user_write() {
    let store = fixture().await;
    let row=sqlx::query("SELECT id::bigint AS id,custom_properties FROM accounts_user WHERE username='rust-operator-owned-a'").fetch_one(store.pool()).await.unwrap();
    let id: i64 = row.get("id");
    let original: Option<Value> = row.get("custom_properties");
    let value = json!({"allowed_networks":["192.0.2.0/24"],"keep":"x".repeat(1024*1024)});
    sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2")
        .bind(&value)
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    let service = OperatorCommands::new(
        store.clone(),
        std::env::var("REDIS_TEST_URL").unwrap(),
        None,
    );
    let result = service
        .run(
            OperatorCommand::ResetUser(Some("rust-operator-owned-a".into())),
            Arc::new(()),
        )
        .await;
    let after: Value =
        sqlx::query_scalar("SELECT custom_properties FROM accounts_user WHERE id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2")
        .bind(original)
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(result, Err(OperatorError::Capacity)));
    assert_eq!(after, value);
    assert!(service.shutdown(Duration::from_secs(2)).await);
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis only"]
async fn applied_del_held_reply_over_two_seconds_keeps_physical_owner() {
    let store = fixture().await;
    sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('network_access','Owned network','{}') ON CONFLICT(key) DO NOTHING").execute(store.pool()).await.unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let target = url::Url::parse(&redis_url).unwrap();
    let backend = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(6379)
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(AtomicBool::new(false));
    let arrived = Arc::new(tokio::sync::Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let (allow, _) = tokio::sync::watch::channel(false);
    let (marked, signal, applications, release) =
        (seen.clone(), arrived.clone(), count.clone(), allow.clone());
    let proxy = tokio::spawn(async move {
        let (front, _) = listener.accept().await.unwrap();
        let back = tokio::net::TcpStream::connect(backend).await.unwrap();
        let (mut input, mut output) = front.into_split();
        let (mut replies, mut requests) = back.into_split();
        let state = marked.clone();
        let send = async move {
            let mut bytes = Vec::new();
            let mut buffer = [0; 8192];
            loop {
                let n = input.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&buffer[..n]);
                if bytes
                    .windows(b":1:coresettings:group:network_access".len())
                    .any(|w| w == b":1:coresettings:group:network_access")
                {
                    state.store(true, Ordering::SeqCst);
                    applications.fetch_add(1, Ordering::SeqCst);
                    bytes.clear()
                }
                if bytes.len() > 8192 {
                    bytes.drain(..bytes.len() - 1024);
                }
                requests.write_all(&buffer[..n]).await.unwrap();
            }
        };
        let receive = async move {
            let mut buffer = [0; 8192];
            let mut ready = release.subscribe();
            loop {
                let n = replies.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                if marked.load(Ordering::SeqCst) && !*ready.borrow() {
                    signal.notify_one();
                    while !*ready.borrow_and_update() {
                        ready.changed().await.unwrap();
                    }
                }
                output.write_all(&buffer[..n]).await.unwrap();
            }
        };
        tokio::select! {_=send=>{},_=receive=>{}}
    });
    let mut real = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(":1:coresettings:group:network_access")
        .arg("owned-stale")
        .query_async(&mut real)
        .await
        .unwrap();
    let service = OperatorCommands::new(
        store.clone(),
        format!("redis://:playback-fixture-only@127.0.0.1:{port}/0"),
        None,
    );
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Arc::new(Guard(dropped.clone()));
    let own = service.clone();
    let task = tokio::spawn(async move { own.run(OperatorCommand::ResetNetwork, guard).await });
    tokio::time::timeout(Duration::from_secs(5), arrived.notified())
        .await
        .unwrap();
    let exists: i64 = redis::cmd("EXISTS")
        .arg(":1:coresettings:group:network_access")
        .query_async(&mut real)
        .await
        .unwrap();
    assert_eq!(exists, 0, "DEL actually applied before held reply");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert!(!dropped.load(Ordering::SeqCst));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    allow.send_replace(true);
    assert!(service.shutdown(Duration::from_secs(5)).await);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let version: Option<String> = redis::cmd("GET")
        .arg(":1:coresettings:groupver:network_access")
        .query_async(&mut real)
        .await
        .unwrap();
    assert!(version.unwrap().parse::<u128>().is_ok());
    proxy.abort();
    let _ = proxy.await;
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis only"]
async fn row_count_and_aggregate_byte_limits_preserve_all_properties() {
    let store = fixture().await;
    for (count, bytes) in [(4097_i64, 0_usize), (6, 800_000)] {
        let prefix = format!("operator-limits-{}-", uuid::Uuid::new_v4());
        let value = json!({"allowed_networks":[],"keep":"x".repeat(bytes)});
        sqlx::query("INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) SELECT '!',NULL,FALSE,$1||n::text,'','','',FALSE,TRUE,NOW(),0,$2,'{}'::jsonb,0,NULL FROM generate_series(1,$3) n").bind(&prefix).bind(&value).bind(count).execute(store.pool()).await.unwrap();
        let service = OperatorCommands::new(
            store.clone(),
            std::env::var("REDIS_TEST_URL").unwrap(),
            None,
        );
        let result = service
            .run(OperatorCommand::ResetUser(None), Arc::new(()))
            .await;
        let preserved: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM accounts_user WHERE username LIKE $1 AND custom_properties=$2",
        )
        .bind(format!("{prefix}%"))
        .bind(&value)
        .fetch_one(store.pool())
        .await
        .unwrap();
        sqlx::query("DELETE FROM accounts_user WHERE username LIKE $1")
            .bind(format!("{prefix}%"))
            .execute(store.pool())
            .await
            .unwrap();
        assert!(matches!(result, Err(OperatorError::Capacity)));
        assert_eq!(preserved, count);
        assert!(service.shutdown(Duration::from_secs(2)).await);
    }
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis only"]
async fn oversized_condition_key_is_explicit_without_flushdb() {
    let store = fixture().await;
    sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('network_access','Owned network','{}') ON CONFLICT(key) DO NOTHING").execute(store.pool()).await.unwrap();
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!(":1:dev_notif_condition_{}", "x".repeat(2048));
    let keep = "operator_oversized_condition_preserve";
    for name in [&key, keep] {
        let _: () = redis::cmd("SET")
            .arg(name)
            .arg("owned")
            .query_async(&mut redis)
            .await
            .unwrap();
    }
    let service = OperatorCommands::new(store, url, None);
    let result = service
        .run(OperatorCommand::ResetNetwork, Arc::new(()))
        .await;
    let value: Option<String> = redis::cmd("GET")
        .arg(keep)
        .query_async(&mut redis)
        .await
        .unwrap();
    let exists: i64 = redis::cmd("EXISTS")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .arg(keep)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(matches!(result, Err(OperatorError::UnsupportedSignal)));
    assert_eq!(value.as_deref(), Some("owned"));
    assert_eq!(exists, 1);
    assert!(service.shutdown(Duration::from_secs(2)).await);
}
struct LateSink {
    release: tokio::sync::watch::Sender<bool>,
    arrived: Arc<tokio::sync::Notify>,
}
impl dispatcharr_contracts::UpdateEventSink for LateSink {
    fn send<'a>(
        &'a self,
        _: &'a str,
        _: Value,
    ) -> futures_util::future::BoxFuture<'a, Result<(), dispatcharr_contracts::UpdateEventError>>
    {
        Box::pin(async { Err(dispatcharr_contracts::UpdateEventError::Unavailable) })
    }
    fn send_retained<'a>(
        &'a self,
        _: &'a str,
        _: Value,
        guard: Arc<dyn Send + Sync>,
    ) -> futures_util::future::BoxFuture<'a, Result<(), dispatcharr_contracts::UpdateEventError>>
    {
        let mut release = self.release.subscribe();
        let arrived = self.arrived.clone();
        Box::pin(async move {
            tokio::spawn(async move {
                let _physical = guard;
                arrived.notify_one();
                while !*release.borrow_and_update() {
                    if release.changed().await.is_err() {
                        break;
                    }
                }
            });
            Err(dispatcharr_contracts::UpdateEventError::Unavailable)
        })
    }
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis only"]
async fn retained_sink_logical_timeout_keeps_operator_slot_until_settlement() {
    let store = fixture().await;
    let (release, _) = tokio::sync::watch::channel(false);
    let arrived = Arc::new(tokio::sync::Notify::new());
    let sink = Arc::new(LateSink {
        release: release.clone(),
        arrived: arrived.clone(),
    });
    let service =
        OperatorCommands::new(store, std::env::var("REDIS_TEST_URL").unwrap(), Some(sink));
    let dropped = Arc::new(AtomicBool::new(false));
    let result = service
        .run(
            OperatorCommand::ResetUser(Some("rust-operator-owned-b".into())),
            Arc::new(Guard(dropped.clone())),
        )
        .await;
    assert!(result.is_ok());
    tokio::time::timeout(Duration::from_secs(2), arrived.notified())
        .await
        .unwrap();
    assert!(!dropped.load(Ordering::SeqCst));
    let second = service
        .run(OperatorCommand::ResetUser(None), Arc::new(()))
        .await;
    let settled = service.shutdown(Duration::from_millis(30)).await;
    release.send_replace(true);
    assert!(service.shutdown(Duration::from_secs(5)).await);
    assert!(
        matches!(second, Err(OperatorError::Capacity)),
        "late publication must keep the same operator slot"
    );
    assert!(
        !settled,
        "shutdown cannot claim settlement before actual sink completion"
    );
    assert!(dropped.load(Ordering::SeqCst));
}
