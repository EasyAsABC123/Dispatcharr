//! Actual Redis common Redirect alias/reservation contracts; no Source runtime claim.
use dispatcharr_coordination::{Coordinator, LeaseConfig, ProviderReservation, RedirectClaim};
use std::{sync::Arc, time::Duration};
async fn setup() -> (
    Coordinator,
    redis::aio::MultiplexedConnection,
    String,
    i64,
    i64,
) {
    let url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
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
    let conn = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    (
        coordinator,
        conn,
        format!("redirect-contract-{id}"),
        id,
        id + 1,
    )
}
fn provider(profile: i64) -> ProviderReservation {
    ProviderReservation {
        profile_id: Some(profile),
        max_streams: 1,
        credential_pool: None,
    }
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn concurrent_missing_metadata_redirect_borrows_real_public_alias_and_releases_no_foreign_slot(
) {
    let (coordinator, mut conn, channel, numeric, profile) = setup().await;
    let stream = profile + 1;
    let RedirectClaim::Owned(grant) = coordinator
        .reserve_redirect_assignment(numeric, &channel, stream, provider(profile), Arc::new(()))
        .await
        .unwrap()
    else {
        panic!("actual new reservation required")
    };
    let actual: (String, String, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("stream_profile:{stream}"))
        .get(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(actual, (stream.to_string(), profile.to_string(), 1));
    let RedirectClaim::Borrowed(borrowed) = coordinator
        .reserve_redirect_assignment(numeric, &channel, stream, provider(profile), Arc::new(()))
        .await
        .unwrap()
    else {
        panic!("reservation-before-metadata must borrow even at max1")
    };
    assert_eq!(
        (borrowed.stream_id(), borrowed.profile_id()),
        (stream, profile)
    );
    let count: i64 = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
    assert!(grant.release().await.unwrap());
    let actual: (Option<String>, Option<String>, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("stream_profile:{stream}"))
        .get(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(actual, (None, None, 0));
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn only_existing_inactive_metadata_releases_stale_aliases_with_single_profile_claim() {
    let (coordinator, mut conn, channel, numeric, profile) = setup().await;
    let stream = profile + 1;
    let key = format!("live:channel:{channel}:metadata");
    let _: () = redis::pipe()
        .set(format!("channel_stream:{numeric}"), stream)
        .set(format!("stream_profile:{stream}"), profile)
        .set(format!("profile_connections:{profile}"), 1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(coordinator
        .observe_redirect_assignment(numeric, &channel, Arc::new(()))
        .await
        .unwrap()
        .is_some());
    for state in [
        "active",
        "waiting_for_clients",
        "buffering",
        "initializing",
        "connecting",
    ] {
        let _: usize = redis::cmd("HSET")
            .arg(&key)
            .arg("state")
            .arg(state)
            .arg("m3u_profile")
            .arg(profile)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(coordinator
            .observe_redirect_assignment(numeric, &channel, Arc::new(()))
            .await
            .unwrap()
            .is_some());
    }
    let _: usize = redis::cmd("HSET")
        .arg(&key)
        .arg("state")
        .arg("stopping")
        .arg("stream_id")
        .arg(stream)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(coordinator
        .observe_redirect_assignment(numeric, &channel, Arc::new(()))
        .await
        .unwrap()
        .is_none());
    assert!(coordinator
        .observe_redirect_assignment(numeric, &channel, Arc::new(()))
        .await
        .unwrap()
        .is_none());
    let count: i64 = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let token: Option<String> = redis::cmd("HGET")
        .arg(&key)
        .arg("m3u_profile")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(token.is_none());
    let _: usize = redis::cmd("DEL")
        .arg(key)
        .query_async(&mut conn)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn changed_foreign_alias_is_not_deleted_or_decremented_by_old_redirect_grant() {
    let (coordinator, mut conn, channel, numeric, profile) = setup().await;
    let stream = profile + 1;
    let RedirectClaim::Owned(grant) = coordinator
        .reserve_redirect_assignment(numeric, &channel, stream, provider(profile), Arc::new(()))
        .await
        .unwrap()
    else {
        panic!("owned")
    };
    let foreign_stream = stream + 1;
    let _: () = redis::pipe()
        .set(format!("channel_stream:{numeric}"), foreign_stream)
        .set(format!("stream_profile:{foreign_stream}"), profile)
        .set(format!("profile_connections:{profile}"), 2)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!grant.release().await.unwrap());
    let actual: (i64, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(actual, (foreign_stream, 2));
    // This control establishes changed-value preservation, not a witness of a
    // foreign Source GETDEL/SET with byte-identical values (known Source ABA).
    let _: usize = redis::cmd("DEL")
        .arg(format!("channel_stream:{numeric}"))
        .arg(format!("stream_profile:{stream}"))
        .arg(format!("stream_profile:{foreign_stream}"))
        .arg(format!("profile_connections:{profile}"))
        .arg(format!("rust:coordination:redirect:channel:{numeric}"))
        .arg(format!("rust:coordination:redirect:stream:{stream}"))
        .query_async(&mut conn)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires real isolated Redis; CLIENT PAUSE must affect no unrelated fixture"]
async fn late_reserve_reply_after_observer_timeout_delivers_cleanup_not_a_leaked_provider_grant() {
    let (normal, mut conn, channel, numeric, profile) = setup().await;
    drop(normal);
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = Coordinator::connect(
        &url,
        LeaseConfig {
            command_timeout: Duration::from_millis(40),
            heartbeat_interval: None,
            reap_interval: None,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let retained = Arc::new(());
    let weak = Arc::downgrade(&retained);
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("ALL")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(coordinator
        .reserve_redirect_assignment(
            numeric,
            &channel,
            profile + 1,
            provider(profile),
            retained.clone()
        )
        .await
        .is_err());
    drop(retained);
    assert!(
        weak.upgrade().is_some(),
        "queued physical reservation still owns its accepted caller"
    );
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let actual: (Option<String>, Option<i64>) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        actual,
        (None, Some(0)),
        "unobserved real owned grant must execute actual alias/counter cleanup"
    );
}

#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn live_alias_publication_borrows_existing_reserve_and_tracks_failover_without_an_extra_slot()
{
    use dispatcharr_coordination::{AcquireRequest, LiveChannelAssignment};
    let (coordinator, mut conn, _, numeric, profile) = setup().await;
    let channel = uuid::Uuid::new_v4().to_string();
    let stream = profile + 1;
    let raw = coordinator
        .acquire(AcquireRequest {
            channel_id: channel.clone(),
            provider_profile_id: Some(profile),
            provider_max_streams: 1,
            credential_pool: None,
        })
        .await
        .unwrap();
    raw.publish_source_assignment(LiveChannelAssignment::new(numeric, stream).unwrap())
        .await
        .unwrap();
    let actual: (i64, i64, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("stream_profile:{stream}"))
        .get(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(actual, (stream, profile, 1));
    let borrowed = coordinator
        .observe_redirect_assignment(numeric, &channel, Arc::new(()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (borrowed.stream_id(), borrowed.profile_id()),
        (stream, profile)
    );
    assert!(raw.release_provider().await.unwrap());
    let after: Option<String> = redis::cmd("GET")
        .arg(format!("channel_stream:{numeric}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(after.is_none());
    raw.reserve_provider(provider(profile + 2)).await.unwrap();
    raw.publish_source_assignment(LiveChannelAssignment::new(numeric, stream + 2).unwrap())
        .await
        .unwrap();
    let after: (i64, i64, i64, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("stream_profile:{}", stream + 2))
        .get(format!("profile_connections:{profile}"))
        .get(format!("profile_connections:{}", profile + 2))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(after, (stream + 2, profile + 2, 0, 1));
    assert!(raw.release().await.unwrap());
    let after: (Option<String>, Option<String>, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("stream_profile:{}", stream + 2))
        .get(format!("profile_connections:{}", profile + 2))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(after, (None, None, 0));
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn source_stale_claim_updates_actual_native_ledger_so_later_raw_teardown_does_not_release_twice(
) {
    use dispatcharr_coordination::{AcquireRequest, LiveChannelAssignment};
    let (coordinator, mut conn, _, numeric, profile) = setup().await;
    let channel = uuid::Uuid::new_v4().to_string();
    let stream = profile + 1;
    let raw = coordinator
        .acquire(AcquireRequest {
            channel_id: channel.clone(),
            provider_profile_id: Some(profile),
            provider_max_streams: 1,
            credential_pool: None,
        })
        .await
        .unwrap();
    raw.publish_source_assignment(LiveChannelAssignment::new(numeric, stream).unwrap())
        .await
        .unwrap();
    let _: usize = redis::cmd("HSET")
        .arg(format!("live:channel:{channel}:metadata"))
        .arg("state")
        .arg("stopped")
        .arg("m3u_profile")
        .arg(profile)
        .arg("stream_id")
        .arg(stream)
        .query_async(&mut conn)
        .await
        .unwrap();
    // Exercise actual metadata fallback after the public stream alias vanished.
    let _: usize = redis::cmd("DEL")
        .arg(format!("stream_profile:{stream}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(coordinator
        .observe_redirect_assignment(numeric, &channel, Arc::new(()))
        .await
        .unwrap()
        .is_none());
    let count: i64 = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    // Another real owner may now reserve the free provider. Old raw teardown
    // must observe its actual ledger was already released by the stale claim.
    let other_channel = uuid::Uuid::new_v4().to_string();
    let other = coordinator
        .acquire(AcquireRequest {
            channel_id: other_channel,
            provider_profile_id: Some(profile),
            provider_max_streams: 1,
            credential_pool: None,
        })
        .await
        .unwrap();
    raw.release().await.unwrap();
    let count: i64 = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
    let receipts: usize = redis::cmd("EXISTS")
        .arg(format!("rust:coordination:redirect:channel:{numeric}"))
        .arg(format!("rust:coordination:redirect:stream:{stream}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(receipts, 0);
    other.release().await.unwrap();
    let _: usize = redis::cmd("DEL")
        .arg(format!("live:channel:{channel}:metadata"))
        .query_async(&mut conn)
        .await
        .unwrap();
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn vod_and_profile_scoped_preview_owners_cannot_mint_bare_channel_assignments() {
    use dispatcharr_coordination::{AcquireRequest, LiveChannelAssignment};
    let (coordinator, mut conn, _, numeric, profile) = setup().await;
    let stream = profile + 1;
    let vod = coordinator
        .acquire_vod(AcquireRequest {
            channel_id: uuid::Uuid::new_v4().to_string(),
            provider_profile_id: Some(profile),
            provider_max_streams: 1,
            credential_pool: None,
        })
        .await
        .unwrap();
    assert_eq!(
        vod.publish_source_assignment(LiveChannelAssignment::new(numeric, stream).unwrap())
            .await,
        Err(dispatcharr_coordination::Error::InvalidRequest)
    );
    vod.release().await.unwrap();
    let preview = coordinator
        .acquire(AcquireRequest {
            channel_id: format!("{}.p7", uuid::Uuid::new_v4()),
            provider_profile_id: Some(profile),
            provider_max_streams: 1,
            credential_pool: None,
        })
        .await
        .unwrap();
    assert_eq!(
        preview
            .publish_source_assignment(LiveChannelAssignment::new(numeric, stream).unwrap())
            .await,
        Err(dispatcharr_coordination::Error::InvalidRequest)
    );
    preview.release().await.unwrap();
    let after: Option<String> = redis::cmd("GET")
        .arg(format!("channel_stream:{numeric}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(after.is_none());
}
