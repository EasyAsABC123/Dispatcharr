//! Real Redis proves client admission survives cancelled REGISTER/RELEASE callers.
//! Original Dispatcharr policy Lua and author credit are retained unchanged.
use dispatcharr_clients::{ClientConfig, ClientLease, ClientStore, Error};
use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
use redis::AsyncCommands;
use std::time::Duration;
use tokio::{sync::Mutex, time};
static EXCLUSIVE: Mutex<()> = Mutex::const_new(());
fn url() -> String {
    std::env::var("REDIS_TEST_URL").expect("isolated real Redis required")
}
fn viewer() -> PlaybackViewer {
    PlaybackViewer {
        user_id: None,
        username: None,
        ip_address: "127.0.0.1".parse().unwrap(),
        user_agent: "admission-cancellation-fixture".into(),
        stream_limit: 0,
        limit_policy: UserLimitPolicy::default(),
    }
}
fn channel() -> String {
    format!("admission-{}", uuid::Uuid::new_v4())
}
async fn store() -> ClientStore {
    ClientStore::connect(
        &url(),
        ClientConfig {
            max_clients: 1,
            command_timeout: Duration::from_millis(40),
            flush_interval: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
async fn pause() -> redis::aio::MultiplexedConnection {
    let mut connection = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(300)
        .arg("ALL")
        .query_async(&mut connection)
        .await
        .unwrap();
    connection
}
async fn resumed(store: &ClientStore, channel: &str) -> ClientLease {
    time::timeout(Duration::from_secs(3), async {
        loop {
            match store.register(channel, &viewer(), "mpegts").await {
                Ok(lease) => return lease,
                Err(Error::Capacity) => time::sleep(Duration::from_millis(10)).await,
                error => panic!("unexpected admission result: {:?}", error.err()),
            }
        }
    })
    .await
    .unwrap()
}
async fn one_row(mut connection: redis::aio::MultiplexedConnection, channel: &str) {
    let count: usize = connection
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "cancelled admission must be fenced-released before its permit can be reused"
    );
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn cancelled_register_keeps_local_budget_through_actual_reply_and_cleanup() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store().await;
    let channel = channel();
    let connection = pause().await;
    let pending_store = store.clone();
    let pending_channel = channel.clone();
    let caller = tokio::spawn(async move {
        pending_store
            .register(&pending_channel, &viewer(), "mpegts")
            .await
    });
    time::sleep(Duration::from_millis(15)).await;
    caller.abort();
    time::sleep(Duration::from_millis(80)).await;
    assert!(
        matches!(
            store.register(&channel, &viewer(), "mpegts").await,
            Err(Error::Capacity)
        ),
        "request cancellation cannot free pending Redis admission"
    );
    let lease = resumed(&store, &channel).await;
    one_row(connection, &channel).await;
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn register_deadline_keeps_budget_until_undelivered_lease_is_released() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store().await;
    let channel = channel();
    let connection = pause().await;
    assert!(matches!(
        store.register(&channel, &viewer(), "mpegts").await,
        Err(Error::RedisUnavailable)
    ));
    assert!(matches!(
        store.register(&channel, &viewer(), "mpegts").await,
        Err(Error::Capacity)
    ));
    let lease = resumed(&store, &channel).await;
    one_row(connection, &channel).await;
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn cancelled_explicit_release_keeps_budget_until_actual_cleanup_reply() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store().await;
    let channel = channel();
    let lease = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let connection = pause().await;
    let caller = tokio::spawn(async move { lease.release().await });
    time::sleep(Duration::from_millis(15)).await;
    caller.abort();
    time::sleep(Duration::from_millis(80)).await;
    assert!(matches!(
        store.register(&channel, &viewer(), "mpegts").await,
        Err(Error::Capacity)
    ));
    let lease = resumed(&store, &channel).await;
    one_row(connection, &channel).await;
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn final_drop_on_plain_thread_schedules_supervised_cleanup_on_creation_runtime() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store().await;
    let channel = channel();
    let lease = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let connection = pause().await;
    std::thread::spawn(move || drop(lease)).join().unwrap();
    time::sleep(Duration::from_millis(80)).await;
    assert!(matches!(
        store.register(&channel, &viewer(), "mpegts").await,
        Err(Error::Capacity)
    ));
    let lease = resumed(&store, &channel).await;
    one_row(connection, &channel).await;
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn held_lease_clone_retains_admission_after_acknowledged_release() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store().await;
    let channel = channel();
    let lease = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let held = lease.clone();
    lease.release().await.unwrap();
    drop(lease);
    assert!(
        matches!(
            store.register(&channel, &viewer(), "mpegts").await,
            Err(Error::Capacity)
        ),
        "HTTP body lease clones must retain admission after Redis release"
    );
    drop(held);
    let lease = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    lease.release().await.unwrap();
    store.shutdown().await;
}

async fn configured(max_clients: usize, timeout: Duration) -> ClientStore {
    ClientStore::connect(
        &url(),
        ClientConfig {
            max_clients,
            command_timeout: timeout,
            flush_interval: Duration::from_secs(30),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn local_channel_stop_removes_accounting_without_polling_held_lease_and_preserves_remote() {
    let _exclusive = EXCLUSIVE.lock().await;
    let local = configured(4, Duration::from_secs(1)).await;
    let remote = configured(2, Duration::from_secs(1)).await;
    let channel = channel();
    let other = channel.clone() + "-other";
    let held = local.register(&channel, &viewer(), "mpegts").await.unwrap();
    let elsewhere = local.register(&other, &viewer(), "mpegts").await.unwrap();
    let foreign = remote
        .register(&channel, &viewer(), "mpegts")
        .await
        .unwrap();
    assert_eq!(local.stop_local_channel(&channel).await.unwrap(), 1);
    assert!(*held.stopped().borrow());
    assert!(!*elsewhere.stopped().borrow());
    assert!(!*foreign.stopped().borrow());
    let mut connection = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let count: usize = connection
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(count, 1, "only remote accounting remains");
    let own_exists: bool = connection
        .exists(format!("live:channel:{channel}:clients:{}", held.id()))
        .await
        .unwrap();
    assert!(!own_exists, "held HTTP lease cannot pin Redis metadata");
    elsewhere.release().await.unwrap();
    foreign.release().await.unwrap();
    local.shutdown().await;
    remote.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn local_stop_waits_for_pending_register_and_prevents_late_metadata_resurrection() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = configured(1, Duration::from_secs(1)).await;
    let channel = channel();
    let mut connection = pause().await;
    let pending = store.clone();
    let target = channel.clone();
    let caller = tokio::spawn(async move { pending.register(&target, &viewer(), "mpegts").await });
    time::sleep(Duration::from_millis(15)).await;
    let stopping = store.clone();
    let target = channel.clone();
    let stop = tokio::spawn(async move { stopping.stop_local_channel(&target).await });
    time::sleep(Duration::from_millis(80)).await;
    assert!(
        !stop.is_finished(),
        "stop must wait for pending registration and actual cleanup"
    );
    assert_eq!(stop.await.unwrap().unwrap(), 1);
    assert!(matches!(caller.await.unwrap(), Err(Error::Stopped)));
    let count: usize = connection
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(count, 0);
    let lease = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn pending_rejected_registration_does_not_deadlock_local_stop_supervisor() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = configured(2, Duration::from_secs(1)).await;
    let channel = channel();
    let mut user = viewer();
    user.user_id = Some(918273);
    user.stream_limit = 1;
    user.limit_policy.terminate_on_limit_exceeded = false;
    let existing = store
        .register(&(channel.clone() + "-existing"), &user, "mpegts")
        .await
        .unwrap();
    let _connection = pause().await;
    let pending = store.clone();
    let target = channel.clone();
    let caller = tokio::spawn(async move { pending.register(&target, &user, "mpegts").await });
    time::sleep(Duration::from_millis(15)).await;
    assert_eq!(store.stop_local_channel(&channel).await.unwrap(), 1);
    assert!(matches!(
        caller.await.unwrap(),
        Err(Error::UserLimit) | Err(Error::Stopped)
    ));
    assert!(!*existing.stopped().borrow());
    existing.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn local_stop_deadline_does_not_cancel_owned_cleanup_or_free_held_body_budget() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = store().await;
    let channel = channel();
    let held = store.register(&channel, &viewer(), "mpegts").await.unwrap();
    let mut connection = pause().await;
    assert_eq!(
        store.stop_local_channel(&channel).await,
        Err(Error::RedisUnavailable)
    );
    assert!(*held.stopped().borrow());
    assert!(matches!(
        store.register(&channel, &viewer(), "mpegts").await,
        Err(Error::Capacity)
    ));
    let count: usize = connection
        .scard(format!("live:channel:{channel}:clients"))
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert!(
        matches!(
            store.register(&channel, &viewer(), "mpegts").await,
            Err(Error::Capacity)
        ),
        "held body retains physical admission after cleanup ACK"
    );
    drop(held);
    let lease = resumed(&store, &channel).await;
    lease.release().await.unwrap();
    store.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated Redis"]
async fn concurrent_cross_channel_drop_and_local_stop_do_not_deadlock_registry() {
    let _exclusive = EXCLUSIVE.lock().await;
    let store = configured(64, Duration::from_secs(1)).await;
    let prefix = channel();
    time::timeout(Duration::from_secs(10), async {
        let mut workers = Vec::new();
        for lane in 0..4 {
            let store = store.clone();
            let channel = format!("{prefix}-{lane}");
            workers.push(tokio::spawn(async move {
                for _ in 0..40 {
                    let lease = resumed(&store, &channel).await;
                    tokio::task::yield_now().await;
                    drop(lease);
                }
            }));
        }
        for _ in 0..200 {
            assert_eq!(
                store
                    .stop_local_channel(&(prefix.clone() + "-absent"))
                    .await
                    .unwrap(),
                0
            );
            tokio::task::yield_now().await;
        }
        for worker in workers {
            worker.await.unwrap();
        }
    })
    .await
    .expect("cross-channel final Arc drops must not reacquire an already-held registry mutex");
    store.shutdown().await;
}
