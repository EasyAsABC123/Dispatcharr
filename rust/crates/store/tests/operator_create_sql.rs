//! Actual owned SQL cancellation/admission control for superuser creation.
//! Original Dispatcharr authors retain application/algorithm credit.
use dispatcharr_store::{
    operator_commands::{OperatorCommand, OperatorCommands, OperatorError},
    operator_create::CreateSuperuser,
    Store,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
struct Caller(Arc<AtomicBool>);
impl Drop for Caller {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis fixture only"]
async fn cancelled_observer_retains_sql_admission_caller_and_settles_single_insert() {
    assert_eq!(
        std::env::var("OPERATOR_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let db: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(db.starts_with("rust_probe_backup_operator_"));
    let name = "rust-create-owned-physical";
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM accounts_user WHERE username=$1")
            .bind(name)
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
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
    let input = CreateSuperuser::parse(
        &[
            "--noinput".into(),
            format!("--username={name}"),
            "--email=physical@example.invalid".into(),
        ],
        |_| None,
    )
    .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let caller = Arc::new(Caller(dropped.clone()));
    let owner = service.clone();
    let observer = tokio::spawn(async move {
        owner
            .run(OperatorCommand::CreateSuperuser(input), caller)
            .await
    });
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'SELECT EXISTS(SELECT 1 FROM accounts_user WHERE username=%'")
                .fetch_one(store.pool()).await.unwrap();
            if pending > 0 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(matches!(
        service
            .run(OperatorCommand::ResetUser(None), Arc::new(()))
            .await,
        Err(OperatorError::Capacity)
    ));
    assert!(!service.shutdown(Duration::from_millis(30)).await);
    assert!(!dropped.load(Ordering::SeqCst));
    lock.rollback().await.unwrap();
    assert!(service.shutdown(Duration::from_secs(8)).await);
    assert!(dropped.load(Ordering::SeqCst));
    let row: (i64, bool, bool, bool, i32) = sqlx::query_as("SELECT count(*) OVER()::bigint,is_superuser,is_staff,password LIKE '!%',user_level FROM accounts_user WHERE username=$1")
        .bind(name).fetch_one(store.pool()).await.unwrap();
    assert_eq!(row, (1, true, true, true, 10));
}

#[tokio::test]
#[ignore = "owned copied PostgreSQL/Redis fixture only"]
async fn error_response_held_ready_keeps_caller_admission_until_sync_and_close() {
    assert_eq!(
        std::env::var("OPERATOR_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    let monitor = Store::connect(&database, 4).await.unwrap();
    let db: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(monitor.pool())
        .await
        .unwrap();
    assert!(db.starts_with("rust_probe_backup_operator_"));
    let name = "rust-create-owned-ready-A";
    sqlx::query("INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) VALUES('!',NULL,FALSE,$1,'','','',FALSE,TRUE,NOW(),1,'{}','{}',0,NULL)")
        .bind(name).execute(monitor.pool()).await.unwrap();
    let target = url::Url::parse(&database).unwrap();
    let backend = format!(
        "{}:{}",
        target.host_str().unwrap(),
        target.port().unwrap_or(5432)
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut proxied = target.clone();
    proxied.set_host(Some("127.0.0.1")).unwrap();
    proxied
        .set_port(Some(listener.local_addr().unwrap().port()))
        .unwrap();
    proxied.query_pairs_mut().append_pair("sslmode", "disable");
    let held = Arc::new(AtomicBool::new(false));
    let closed = Arc::new(AtomicBool::new(false));
    let (release, _) = tokio::sync::watch::channel(false);
    let (gate, physical_close, control) = (held.clone(), closed.clone(), release.clone());
    let proxy = tokio::spawn(async move {
        let mut peers = tokio::task::JoinSet::new();
        loop {
            let (front, _) = listener.accept().await.unwrap();
            let back = tokio::net::TcpStream::connect(&backend).await.unwrap();
            let (mut request, mut response) = front.into_split();
            let (mut replies, mut commands) = back.into_split();
            let (gate, physical_close, mut control) =
                (gate.clone(), physical_close.clone(), control.subscribe());
            peers.spawn(async move {
                let mut saw_error = false;
                let forward = async {
                    let result = tokio::io::copy(&mut request, &mut commands).await;
                    let _ = commands.shutdown().await;
                    result
                };
                let reverse = async {
                    loop {
                        let mut header = [0u8; 5];
                        if let Err(error) = replies.read_exact(&mut header).await {
                            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                                break;
                            }
                            return Err(error);
                        }
                        let length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                        assert!((4..=1024 * 1024).contains(&length));
                        let mut body = vec![0; length - 4];
                        replies.read_exact(&mut body).await?;
                        if header[0] == b'E' && body.windows(6).any(|s| s == b"23505\0") {
                            saw_error = true;
                        }
                        if header[0] == b'Z' && saw_error {
                            gate.store(true, Ordering::SeqCst);
                            while !*control.borrow() {
                                control.changed().await.unwrap();
                            }
                        }
                        response.write_all(&header).await?;
                        response.write_all(&body).await?;
                    }
                    Ok::<_, std::io::Error>(())
                };
                let _ = tokio::join!(forward, reverse);
                if saw_error {
                    physical_close.store(true, Ordering::SeqCst);
                }
            });
        }
    });
    let store = Store::connect(proxied.as_str(), 2).await.unwrap();
    let service = OperatorCommands::new(
        store.clone(),
        std::env::var("REDIS_TEST_URL").unwrap(),
        None,
    );
    // Raw key does not exist. Manager NFKC collides at the actual INSERT,
    // producing a genuine server23505 before trailing ReadyForQuery.
    let input = CreateSuperuser::parse(
        &[
            "--noinput".into(),
            "--username=rust-create-owned-ready-Ａ".into(),
            "--email=ready@example.invalid".into(),
        ],
        |_| None,
    )
    .unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let caller = Arc::new(Caller(dropped.clone()));
    let own = service.clone();
    let observer = tokio::spawn(async move {
        own.run(OperatorCommand::CreateSuperuser(input), caller)
            .await
    });
    tokio::time::timeout(Duration::from_secs(8), async {
        while !held.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        !observer.is_finished(),
        "ERROR must not release physical ownership before ReadyForQuery"
    );
    observer.abort();
    assert!(observer.await.unwrap_err().is_cancelled());
    assert!(matches!(
        service
            .run(OperatorCommand::ResetUser(None), Arc::new(()))
            .await,
        Err(OperatorError::Capacity)
    ));
    assert!(!service.shutdown(Duration::from_millis(40)).await);
    assert!(!dropped.load(Ordering::SeqCst));
    release.send(true).unwrap();
    assert!(service.shutdown(Duration::from_secs(8)).await);
    assert!(dropped.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !closed.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let row: (i64, bool, bool, i32, String) = sqlx::query_as("SELECT count(*) OVER()::bigint,is_superuser,is_staff,user_level,password FROM accounts_user WHERE username=$1")
        .bind(name).fetch_one(monitor.pool()).await.unwrap();
    assert_eq!(row, (1, false, false, 1, "!".into()));
    store.pool().close().await;
    proxy.abort();
}
