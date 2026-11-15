//! Real Redis Stream preview scope/counter lifecycle; not Source Main acceptance.
use dispatcharr_coordination::{
    AcquireRequest, Coordinator, LeaseConfig, LivePreviewAssignment, ProviderReservation,
};
use std::time::Duration;
async fn setup() -> (Coordinator, redis::aio::MultiplexedConnection, i64, String) {
    let url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let coordinator = Coordinator::connect(
        &url,
        LeaseConfig {
            heartbeat_interval: None,
            reap_interval: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    (coordinator, connection, id, format!("preview-{id}"))
}
fn request(worker: String) -> AcquireRequest {
    AcquireRequest {
        channel_id: worker,
        provider_profile_id: None,
        provider_max_streams: 0,
        credential_pool: None,
    }
}
fn provider(profile: i64) -> ProviderReservation {
    ProviderReservation {
        profile_id: Some(profile),
        max_streams: 2,
        credential_pool: None,
    }
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn scoped_and_default_previews_share_no_bare_channel_alias_and_release_actual_slots_once() {
    let (coordinator, mut redis, id, hash) = setup().await;
    let p = id + 1;
    let q = id + 2;
    let mut leases = Vec::new();
    for (profile, scoped) in [(p, true), (q, true), (p, false)] {
        let assignment = LivePreviewAssignment::new(id, &hash, profile, scoped).unwrap();
        let owner = coordinator
            .acquire(request(assignment.worker_id()))
            .await
            .unwrap();
        assert!(!owner
            .prepare_preview_assignment(assignment.clone())
            .await
            .unwrap());
        owner.reserve_provider(provider(profile)).await.unwrap();
        owner
            .publish_preview_assignment(assignment, Duration::from_secs(60))
            .await
            .unwrap();
        leases.push(owner);
    }
    let actual: (i64, i64, i64, i64, i64, i64, i64) = redis::pipe()
        .get(format!("stream_profile:{id}:p{p}"))
        .get(format!("stream_profile:{id}:p{q}"))
        .get(format!("stream_profile:{id}:preview"))
        .get(format!("profile_connections:{p}"))
        .get(format!("profile_connections:{q}"))
        .exists(format!("stream_profile:{id}"))
        .exists(format!("channel_stream:{id}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(actual, (p, q, p, 2, 1, 0, 0));
    for owner in leases {
        assert!(owner.release().await.unwrap());
    }
    let actual: (i64, i64, i64, i64, i64) = redis::pipe()
        .get(format!("profile_connections:{p}"))
        .get(format!("profile_connections:{q}"))
        .exists(format!("stream_profile:{id}:p{p}"))
        .exists(format!("stream_profile:{id}:p{q}"))
        .exists(format!("stream_profile:{id}:preview"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(actual, (0, 0, 0, 0, 0));
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn preview_missing_metadata_borrows_but_actual_stopped_metadata_releases_before_reserving() {
    let (coordinator, mut redis, id, hash) = setup().await;
    let profile = id + 1;
    for stopped in [false, true] {
        let assignment = LivePreviewAssignment::new(id, &hash, profile, true).unwrap();
        let worker = assignment.worker_id();
        let alias = format!("stream_profile:{id}:p{profile}");
        let metadata = format!("live:channel:{worker}:metadata");
        let counter = format!("profile_connections:{profile}");
        let _: () = redis::pipe()
            .del(&metadata)
            .set(&alias, profile)
            .set(&counter, 1)
            .query_async(&mut redis)
            .await
            .unwrap();
        if stopped {
            let _: () = redis::cmd("HSET")
                .arg(&metadata)
                .arg("state")
                .arg("stopped")
                .arg("m3u_profile")
                .arg(profile)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        let owner = coordinator.acquire(request(worker)).await.unwrap();
        assert_eq!(
            owner
                .prepare_preview_assignment(assignment.clone())
                .await
                .unwrap(),
            !stopped
        );
        let count: i64 = redis::cmd("GET")
            .arg(&counter)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert_eq!(count, if stopped { 0 } else { 1 });
        if stopped {
            owner.reserve_provider(provider(profile)).await.unwrap();
        }
        owner
            .publish_preview_assignment(assignment, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(owner.release().await.unwrap());
        let actual: (i64, i64) = redis::pipe()
            .get(&counter)
            .exists(&alias)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert_eq!(actual, (0, 0));
    }
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn borrowed_preview_stopped_before_publication_does_not_leak_after_initial_metadata_without_slot_field(
) {
    let (coordinator, mut redis, id, hash) = setup().await;
    let profile = id + 1;
    let assignment = LivePreviewAssignment::new(id, &hash, profile, true).unwrap();
    let worker = assignment.worker_id();
    let metadata = format!("live:channel:{worker}:metadata");
    let alias = format!("stream_profile:{id}:p{profile}");
    let counter = format!("profile_connections:{profile}");
    let _: () = redis::pipe()
        .set(&alias, profile)
        .set(&counter, 1)
        .query_async(&mut redis)
        .await
        .unwrap();
    let owner = coordinator.acquire(request(worker)).await.unwrap();
    assert!(owner.prepare_preview_assignment(assignment).await.unwrap());
    let _: () = redis::cmd("HSET")
        .arg(&metadata)
        .arg("state")
        .arg("initializing")
        .arg("owner")
        .arg(owner.token())
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(owner.release().await.unwrap());
    let actual: (i64, i64) = redis::pipe()
        .get(counter)
        .exists(alias)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(actual, (0, 0));
}
