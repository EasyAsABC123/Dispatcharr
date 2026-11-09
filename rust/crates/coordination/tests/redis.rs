//! Requires an explicit disposable Redis; ignored in offline core-only checks.
use dispatcharr_coordination::{AcquireRequest, Coordinator, CredentialPool, Error, LeaseConfig};
use redis::AsyncCommands;
use std::time::Duration;
use tokio::time;

fn config() -> LeaseConfig {
    LeaseConfig {
        namespace: format!("test:rust:coord:{}", uuid::Uuid::new_v4()),
        ttl: Duration::from_millis(200),
        heartbeat_interval: None,
        reap_interval: None,
        command_timeout: Duration::from_secs(2),
        max_inflight_commands: 16,
        reap_batch: 128,
    }
}

fn request() -> AcquireRequest {
    let bytes = *uuid::Uuid::new_v4().as_bytes();
    let profile =
        (u64::from_le_bytes(bytes[..8].try_into().unwrap()) & 0x3fff_ffff_ffff_ffff) as i64 + 1;
    AcquireRequest {
        channel_id: uuid::Uuid::new_v4().to_string(),
        provider_profile_id: Some(profile),
        provider_max_streams: 1,
        credential_pool: None,
    }
}

async fn setup(config: LeaseConfig) -> (Coordinator, redis::aio::MultiplexedConnection) {
    let url =
        std::env::var("REDIS_TEST_URL").expect("REDIS_TEST_URL must point to disposable Redis");
    let coordinator = Coordinator::connect(&url, config).await.unwrap();
    let connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    (coordinator, connection)
}

fn profile(request: &AcquireRequest) -> String {
    format!(
        "profile_connections:{}",
        request.provider_profile_id.unwrap()
    )
}

fn owner(request: &AcquireRequest) -> String {
    format!("live:channel:{}:owner", request.channel_id)
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn vod_owner_is_scoped_but_provider_capacity_is_shared_with_live() {
    let mut configuration = config();
    configuration.ttl = Duration::from_secs(10);
    let namespace = configuration.namespace.clone();
    let (coordinator, mut redis) = setup(configuration).await;
    let vod = request();
    let mut live = vod.clone();
    live.channel_id = uuid::Uuid::new_v4().to_string();
    let vod_owner = format!("{namespace}:vod:session:{}:owner", vod.channel_id);
    let lease = coordinator.acquire_vod(vod.clone()).await.unwrap();
    assert_eq!(redis.get::<_, String>(&vod_owner).await.unwrap(), lease.token());
    assert!(!redis.exists::<_, bool>(owner(&vod)).await.unwrap());
    assert_eq!(redis.get::<_, i64>(profile(&vod)).await.unwrap(), 1);
    assert!(matches!(coordinator.acquire(live.clone()).await, Err(Error::ProfileFull)));
    assert!(!redis.exists::<_, bool>(owner(&live)).await.unwrap());
    assert!(lease.release().await.unwrap());
    assert!(!redis.exists::<_, bool>(&vod_owner).await.unwrap());
    let live_lease = coordinator.acquire(live.clone()).await.unwrap();
    assert!(matches!(coordinator.acquire_vod(vod.clone()).await, Err(Error::ProfileFull)));
    assert!(!redis.exists::<_, bool>(&vod_owner).await.unwrap());
    assert_eq!(redis.get::<_, i64>(profile(&vod)).await.unwrap(), 1);
    live_lease.release().await.unwrap();
    let _: () = redis.del(profile(&vod)).await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn same_channel_has_exactly_one_fenced_owner() {
    let (coordinator, mut redis) = setup(config()).await;
    let request = request();
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let coordinator = coordinator.clone();
        let request = request.clone();
        tasks.push(tokio::spawn(
            async move { coordinator.acquire(request).await },
        ));
    }
    let mut winners = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            Ok(lease) => winners.push(lease),
            Err(Error::OwnerHeld) => {}
            other => panic!("unexpected result: {}", other.err().unwrap()),
        }
    }
    assert_eq!(winners.len(), 1);
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 1);
    assert!(winners[0].fence() > 0);
    assert_eq!(
        redis.get::<_, String>(owner(&request)).await.unwrap(),
        winners[0].token()
    );
    assert!(winners[0].release().await.unwrap());
    assert!(!winners[0].release().await.unwrap());
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 0);
    assert!(!redis.exists::<_, bool>(owner(&request)).await.unwrap());
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn independent_replicas_share_provider_capacity_and_python_counters() {
    let configuration = config();
    let (first, mut redis) = setup(configuration.clone()).await;
    let (second, _) = setup(configuration).await;
    let request = request();
    let _: () = redis.set(profile(&request), 1).await.unwrap(); // Existing Python reservation.
    assert!(matches!(
        first.acquire(request.clone()).await,
        Err(Error::ProfileFull)
    ));
    assert!(!redis.exists::<_, bool>(owner(&request)).await.unwrap());
    let _: () = redis.set(profile(&request), 0).await.unwrap();
    let mut another = request.clone();
    another.channel_id = uuid::Uuid::new_v4().to_string();
    let (a, b) = tokio::join!(first.acquire(request.clone()), second.acquire(another));
    let lease = match (a, b) {
        (Ok(lease), Err(Error::ProfileFull)) | (Err(Error::ProfileFull), Ok(lease)) => lease,
        _ => panic!("exactly one replica must reserve the provider"),
    };
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 1);
    lease.release().await.unwrap();
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn credential_capacity_is_atomic_with_profile_and_release_mapping() {
    let (coordinator, mut redis) = setup(config()).await;
    let mut a = request();
    let credential = format!("server_group_connections:{}:fixture", uuid::Uuid::new_v4());
    a.provider_max_streams = 5;
    a.credential_pool = Some(CredentialPool {
        counter_key: credential.clone(),
        max_streams: 1,
    });
    let mut b = request();
    b.provider_max_streams = 5;
    b.credential_pool = a.credential_pool.clone();
    let lease = coordinator.acquire(a.clone()).await.unwrap();
    assert!(matches!(
        coordinator.acquire(b.clone()).await,
        Err(Error::CredentialFull)
    ));
    assert!(!redis.exists::<_, bool>(profile(&b)).await.unwrap());
    assert_eq!(redis.get::<_, i64>(&credential).await.unwrap(), 1);
    assert_eq!(redis.get::<_, i64>(profile(&a)).await.unwrap(), 1);
    lease.release().await.unwrap();
    assert_eq!(redis.get::<_, i64>(&credential).await.unwrap(), 0);
    let release_key = format!(
        "profile_credential_release:{}",
        a.provider_profile_id.unwrap()
    );
    assert!(!redis.exists::<_, bool>(release_key).await.unwrap());
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn expired_owner_is_reaped_and_stale_heartbeat_or_release_cannot_touch_successor() {
    let mut configuration = config();
    configuration.ttl = Duration::from_millis(60);
    let (coordinator, mut redis) = setup(configuration).await;
    let request = request();
    let old = coordinator.acquire(request.clone()).await.unwrap();
    let token = old.token().to_owned();
    time::sleep(Duration::from_millis(100)).await;
    let fresh = coordinator.acquire(request.clone()).await.unwrap();
    assert_ne!(fresh.token(), token);
    assert!(fresh.fence() > old.fence());
    assert!(!old.heartbeat().await.unwrap());
    assert!(!old.release().await.unwrap());
    assert_eq!(
        redis.get::<_, String>(owner(&request)).await.unwrap(),
        fresh.token()
    );
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 1);
    fresh.release().await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn heartbeat_extends_owner_and_drop_releases_once() {
    let mut configuration = config();
    configuration.ttl = Duration::from_millis(150);
    configuration.heartbeat_interval = Some(Duration::from_millis(25));
    let (coordinator, mut redis) = setup(configuration).await;
    let request = request();
    let lease = coordinator.acquire(request.clone()).await.unwrap();
    time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        redis.get::<_, String>(owner(&request)).await.unwrap(),
        lease.token()
    );
    assert!(!*lease.lost().borrow());
    drop(lease);
    time::timeout(Duration::from_secs(2), async {
        loop {
            if redis.get::<_, i64>(profile(&request)).await.unwrap() == 0 {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!redis.exists::<_, bool>(owner(&request)).await.unwrap());
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn reaper_recovers_expired_profile_and_credential_without_new_channel() {
    let mut configuration = config();
    configuration.ttl = Duration::from_millis(50);
    let (coordinator, mut redis) = setup(configuration).await;
    let mut request = request();
    let credential = format!("server_group_connections:{}:fixture", uuid::Uuid::new_v4());
    request.credential_pool = Some(CredentialPool {
        counter_key: credential.clone(),
        max_streams: 1,
    });
    let lease = coordinator.acquire(request.clone()).await.unwrap();
    time::sleep(Duration::from_millis(90)).await;
    assert_eq!(coordinator.reap_expired().await.unwrap(), 1);
    assert_eq!(coordinator.reap_expired().await.unwrap(), 0);
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 0);
    assert_eq!(redis.get::<_, i64>(credential).await.unwrap(), 0);
    assert!(!lease.release().await.unwrap());
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn corrupt_credential_counter_cannot_partially_decrement_profile_on_release() {
    let (coordinator, mut redis) = setup(config()).await;
    let mut request = request();
    let credential = format!("server_group_connections:{}:fixture", uuid::Uuid::new_v4());
    request.credential_pool = Some(CredentialPool {
        counter_key: credential.clone(),
        max_streams: 1,
    });
    let lease = coordinator.acquire(request.clone()).await.unwrap();
    let _: () = redis.set(&credential, "corrupt").await.unwrap();
    assert!(matches!(
        lease.release().await,
        Err(Error::RedisUnavailable)
    ));
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 1);
    let _: () = redis.set(&credential, 1).await.unwrap();
    // release() is single-use; reaper handles an interrupted/failed release.
    time::sleep(Duration::from_millis(250)).await;
    assert_eq!(coordinator.reap_expired().await.unwrap(), 1);
    assert_eq!(redis.get::<_, i64>(profile(&request)).await.unwrap(), 0);
    assert_eq!(redis.get::<_, i64>(&credential).await.unwrap(), 0);
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn provider_handoff_keeps_fence_and_failed_backup_claims_no_slot() {
    use dispatcharr_coordination::ProviderReservation;
    let (coordinator, mut redis) = setup(config()).await;
    let primary = request();
    let backup = request();
    let lease = coordinator.acquire(primary.clone()).await.unwrap();
    let token = lease.token().to_owned();
    assert!(matches!(
        lease
            .reserve_provider(ProviderReservation {
                profile_id: backup.provider_profile_id,
                max_streams: 1,
                credential_pool: None,
            })
            .await,
        Err(Error::AlreadyReserved)
    ));
    assert!(!redis.exists::<_, bool>(profile(&backup)).await.unwrap());
    assert!(lease.release_provider().await.unwrap());
    assert!(!lease.release_provider().await.unwrap());
    assert_eq!(redis.get::<_, i64>(profile(&primary)).await.unwrap(), 0);
    assert_eq!(
        redis.get::<_, String>(owner(&primary)).await.unwrap(),
        token
    );
    let _: () = redis.set(profile(&backup), 1).await.unwrap();
    assert!(matches!(
        lease
            .reserve_provider(ProviderReservation {
                profile_id: backup.provider_profile_id,
                max_streams: 1,
                credential_pool: None,
            })
            .await,
        Err(Error::ProfileFull)
    ));
    assert_eq!(redis.get::<_, i64>(profile(&primary)).await.unwrap(), 0);
    assert_eq!(redis.get::<_, i64>(profile(&backup)).await.unwrap(), 1);
    assert_eq!(
        redis.get::<_, String>(owner(&primary)).await.unwrap(),
        token
    );
    lease
        .reserve_provider(ProviderReservation {
            profile_id: primary.provider_profile_id,
            max_streams: 1,
            credential_pool: None,
        })
        .await
        .unwrap();
    assert_eq!(redis.get::<_, i64>(profile(&primary)).await.unwrap(), 1);
    assert!(lease.release().await.unwrap());
    assert_eq!(redis.get::<_, i64>(profile(&primary)).await.unwrap(), 0);
    assert_eq!(redis.get::<_, i64>(profile(&backup)).await.unwrap(), 1);
}
