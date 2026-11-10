//! Actual Redis transition tests; no application/body integration claim.
//! Original Dispatcharr authors retain application and algorithm credit.
#[path = "../src/recovery_generation.rs"]
pub(crate) mod prototype;
use dispatcharr_contracts::UserLimitPolicy;
use dispatcharr_coordination::{AcquireRequest, Coordinator, Lease, LeaseConfig};
use prototype::{Error, NewAdmission, Recovery};
use redis::{aio::ConnectionManagerConfig, AsyncCommands};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

pub(crate) struct Fixture {
    pub(crate) coordinator: Coordinator,
    pub(crate) redis: redis::aio::MultiplexedConnection,
    pub(crate) recovery: Recovery,
    pub(crate) namespace: String,
    pub(crate) id: String,
    pub(crate) profile: i64,
    pub(crate) old: Arc<Lease>,
    pub(crate) token: String,
}
impl Fixture {
    pub(crate) async fn new() -> Self {
        assert_eq!(
            std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let url = std::env::var("REDIS_TEST_URL").unwrap();
        assert_eq!(url, "redis://:playback-fixture-only@127.0.0.1:6379/0");
        let namespace = format!("recovery-{}", uuid::Uuid::new_v4().simple());
        let id = uuid::Uuid::new_v4().simple().to_string();
        let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000;
        let coordinator = Coordinator::connect(
            &url,
            LeaseConfig {
                namespace: namespace.clone(),
                heartbeat_interval: None,
                reap_interval: None,
                ttl: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let client = redis::Client::open(url).unwrap();
        let redis = client.get_multiplexed_async_connection().await.unwrap();
        let connection = client
            .get_connection_manager_with_config(
                ConnectionManagerConfig::new().set_number_of_retries(0),
            )
            .await
            .unwrap();
        let recovery = Recovery::new(
            coordinator.clone(),
            connection,
            namespace.clone(),
            1,
            Duration::from_secs(5),
        )
        .unwrap();
        let old = Arc::new(
            coordinator
                .acquire_vod(AcquireRequest {
                    channel_id: id.clone(),
                    provider_profile_id: Some(profile),
                    provider_max_streams: 1,
                    credential_pool: None,
                })
                .await
                .unwrap(),
        );
        Self {
            coordinator,
            redis,
            recovery,
            namespace,
            id,
            profile,
            old,
            token: "original-generation-metadata".into(),
        }
    }
    pub(crate) fn owner(&self) -> String {
        format!("{}:vod:session:{}:owner", self.namespace, self.id)
    }
    pub(crate) fn metadata(&self) -> String {
        format!("vod_persistent_connection:{}", self.id)
    }
    pub(crate) fn stop(&self) -> String {
        format!("vod_proxy:client:{}:stop", self.id)
    }
    pub(crate) fn ledger(&self) -> String {
        format!("{}:lease:{}", self.namespace, self.old.token())
    }
    pub(crate) fn counter(&self) -> String {
        format!("profile_connections:{}", self.profile)
    }
    pub(crate) fn request(&self) -> AcquireRequest {
        AcquireRequest {
            channel_id: self.id.clone(),
            provider_profile_id: Some(self.profile),
            provider_max_streams: 1,
            credential_pool: None,
        }
    }
    pub(crate) fn admission(&self) -> NewAdmission {
        NewAdmission {
            fields: BTreeMap::from([
                ("session_id".into(), self.id.clone()),
                ("active_streams".into(), "1".into()),
                ("request_count".into(), "1".into()),
                ("user_id".into(), "prototype-user".into()),
                ("content_uuid".into(), self.id.clone()),
                ("created_at".into(), "2".into()),
            ]),
            limit: 0,
            policy: UserLimitPolicy::default(),
        }
    }
    pub(crate) async fn erase_owned_generation(&mut self) {
        // Controlled per-test simulation of Redis state loss, never FLUSHDB.
        let _: usize = self
            .redis
            .del(&[self.owner(), self.ledger(), self.counter(), self.metadata()])
            .await
            .unwrap();
        assert!(!self.old.heartbeat().await.unwrap());
        self.old.settlement().wait().await;
    }
    pub(crate) async fn retire(&self) -> Result<prototype::Retired, Error> {
        self.recovery
            .retire(
                self.id.clone(),
                self.old.clone(),
                self.token.clone(),
                Arc::new(()),
            )
            .await
    }
    pub(crate) async fn empty(&mut self) {
        assert!(!self.redis.exists::<_, bool>(self.owner()).await.unwrap());
        assert!(!self.redis.exists::<_, bool>(self.metadata()).await.unwrap());
        assert_eq!(
            self.redis
                .get::<_, Option<i64>>(self.counter())
                .await
                .unwrap()
                .unwrap_or(0),
            0
        );
        assert_eq!(self.recovery.available_slots(), 1);
        assert!(self.recovery.cleanup_succeeded());
    }
}
#[tokio::test]
#[ignore = "actual owned Redis generation retirement and current-owner admission"]
async fn source_public_counts_belong_only_to_new_generation_and_old_cleanup_is_fenced() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    assert_eq!(retired.old_token(), f.old.token());
    assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
    assert_eq!(f.recovery.available_slots(), 0);
    let generation = retired.reopen(f.request(), f.admission()).await.unwrap();
    assert_ne!(generation.owner_token(), f.old.token());
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    assert_eq!(
        f.redis
            .hget::<_, _, i64>(f.metadata(), "active_streams")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "rust_owner_token")
            .await
            .unwrap(),
        generation.metadata_token()
    );
    f.old.release().await.unwrap();
    f.old.settlement().wait().await;
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        generation.owner_token()
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    let closed = generation.close().await.unwrap();
    assert_eq!(closed.metadata, prototype::MetadataCleanup::Deleted);
    assert!(closed.lease_released);
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis stop fence between classification and new admission"]
async fn stop_arriving_after_retirement_survives_refused_admission() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let _: () = f.redis.set(f.stop(), "stop").await.unwrap();
    assert!(matches!(
        retired.reopen(f.request(), f.admission()).await,
        Err(Error::Admission(dispatcharr_clients::Error::Stopped))
    ));
    assert_eq!(f.redis.get::<_, String>(f.stop()).await.unwrap(), "stop");
    f.empty().await;
    let _: usize = f.redis.del(f.stop()).await.unwrap();
}
#[tokio::test]
#[ignore = "actual owned Redis foreign metadata fence"]
async fn foreign_metadata_arriving_after_retirement_is_preserved() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let _: usize = f
        .redis
        .hset(f.metadata(), "rust_owner_token", "foreign")
        .await
        .unwrap();
    assert!(matches!(
        retired.reopen(f.request(), f.admission()).await,
        Err(Error::ForeignOwner)
    ));
    assert_eq!(
        f.redis
            .hget::<_, _, String>(f.metadata(), "rust_owner_token")
            .await
            .unwrap(),
        "foreign"
    );
    assert_eq!(
        f.redis
            .get::<_, Option<i64>>(f.counter())
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );
    let _: usize = f.redis.del(f.metadata()).await.unwrap();
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis foreign owner and normal provider capacity"]
async fn normal_profile_capacity_is_not_bypassed_by_retired_body() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let mut foreign = f.request();
    foreign.channel_id = uuid::Uuid::new_v4().to_string();
    let held = f.coordinator.acquire_vod(foreign).await.unwrap();
    assert!(matches!(
        retired.reopen(f.request(), f.admission()).await,
        Err(Error::Acquisition(
            dispatcharr_coordination::Error::ProfileFull
        ))
    ));
    assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    held.release().await.unwrap();
    held.settlement().wait().await;
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis positive user cap during new-generation admission"]
async fn positive_user_limit_releases_only_its_new_provider_reservation() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let other = format!(
        "vod_persistent_connection:{}",
        uuid::Uuid::new_v4().simple()
    );
    let _: () = f
        .redis
        .hset_multiple(
            &other,
            &[
                ("user_id", "prototype-user"),
                ("content_uuid", "different"),
                ("created_at", "1"),
            ],
        )
        .await
        .unwrap();
    let mut admission = f.admission();
    admission.limit = 1;
    admission.policy.terminate_on_limit_exceeded = false;
    assert!(matches!(
        retired.reopen(f.request(), admission).await,
        Err(Error::Admission(dispatcharr_clients::Error::UserLimit))
    ));
    assert!(f.redis.exists::<_, bool>(&other).await.unwrap());
    assert!(!f
        .redis
        .exists::<_, bool>(format!(
            "vod_proxy:client:{}:stop",
            other.strip_prefix("vod_persistent_connection:").unwrap()
        ))
        .await
        .unwrap());
    let _: usize = f.redis.del(other).await.unwrap();
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis partial owned ledger fail-closed"]
async fn partial_old_ledger_is_not_reacquired_or_double_counted() {
    let mut f = Fixture::new().await;
    let _: usize = f.redis.del(f.owner()).await.unwrap();
    assert!(!f.old.heartbeat().await.unwrap());
    assert!(matches!(f.retire().await, Err(Error::PartialState)));
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    f.old.release().await.unwrap();
    f.old.settlement().wait().await;
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual owned Redis bounded retained recovery metadata"]
async fn oversized_metadata_is_rejected_before_acquisition_or_public_mutation() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let mut admission = f.admission();
    admission
        .fields
        .insert("content_name".into(), "x".repeat(65536));
    assert!(matches!(
        retired.reopen(f.request(), admission).await,
        Err(Error::Capacity)
    ));
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual owned Redis foreign coordination owner between phases"]
async fn foreign_coordination_owner_after_retirement_is_never_overwritten_or_released() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let foreign = f.coordinator.acquire_vod(f.request()).await.unwrap();
    assert!(matches!(
        retired.reopen(f.request(), f.admission()).await,
        Err(Error::Acquisition(
            dispatcharr_coordination::Error::OwnerHeld
        ))
    ));
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        foreign.token()
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    assert!(!f.redis.exists::<_, bool>(f.metadata()).await.unwrap());
    foreign.release().await.unwrap();
    foreign.settlement().wait().await;
    f.empty().await;
}
#[tokio::test]
#[ignore = "actual owned Redis foreign owner and explicit stop classification"]
async fn classification_preserves_foreign_owner_and_existing_stop() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let foreign = f.coordinator.acquire_vod(f.request()).await.unwrap();
    assert!(matches!(f.retire().await, Err(Error::ForeignOwner)));
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        foreign.token()
    );
    foreign.release().await.unwrap();
    foreign.settlement().wait().await;
    let _: () = f.redis.set(f.stop(), "stop").await.unwrap();
    assert!(matches!(f.retire().await, Err(Error::Stopped)));
    assert_eq!(f.redis.get::<_, String>(f.stop()).await.unwrap(), "stop");
    let _: usize = f.redis.del(f.stop()).await.unwrap();
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual foreign coordination replacement with retained older metadata"]
async fn foreign_coordination_replacement_preserves_even_matching_old_metadata_on_close() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let generation = retired.reopen(f.request(), f.admission()).await.unwrap();
    let metadata_token = generation.metadata_token().to_owned();
    // Own a controlled partial state: Redis loses coordinator/quota keys while
    // retaining the prior hash. A real new lease now owns the same logical ID.
    let previous_ledger = format!("{}:lease:{}", f.namespace, generation.owner_token());
    let _: usize = f
        .redis
        .del(&[f.owner(), previous_ledger, f.counter()])
        .await
        .unwrap();
    let foreign = f.coordinator.acquire_vod(f.request()).await.unwrap();
    let closed = generation.close().await.unwrap();
    assert_eq!(closed.metadata, prototype::MetadataCleanup::OwnerNotCurrent);
    assert!(!closed.lease_released);
    assert_eq!(
        f.redis.get::<_, String>(f.owner()).await.unwrap(),
        foreign.token()
    );
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    assert_eq!(
        f.redis
            .hget::<_, _, Option<String>>(f.metadata(), "rust_owner_token")
            .await
            .unwrap(),
        Some(metadata_token),
        "cleanup must refuse a foreign current owner even if an older metadata token remains"
    );
    foreign.release().await.unwrap();
    foreign.settlement().wait().await;
    let _: usize = f.redis.del(f.metadata()).await.unwrap();
    f.empty().await;
}

#[tokio::test]
#[ignore = "actual successful new-request scope handoff without generation-lifetime pin"]
async fn delivered_generation_releases_only_extra_setup_request_reference() {
    let mut f = Fixture::new().await;
    f.erase_owned_generation().await;
    let retired = f.retire().await.unwrap();
    let caller = Arc::new(());
    let weak = Arc::downgrade(&caller);
    let generation = retired
        .reopen_retained(f.request(), f.admission(), caller)
        .await
        .unwrap();
    assert!(
        weak.upgrade().is_none(),
        "successful handoff must not pin request scope for generation lifetime"
    );
    assert_eq!(f.recovery.available_slots(), 0);
    assert_eq!(f.redis.get::<_, i64>(f.counter()).await.unwrap(), 1);
    generation.close().await.unwrap();
    f.empty().await;
}
