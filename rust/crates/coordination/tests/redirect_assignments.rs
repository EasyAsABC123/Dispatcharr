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

#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn pending_redirect_and_raw_borrower_share_one_slot_and_either_teardown_claims_it_once() {
    use dispatcharr_coordination::{AcquireRequest, LiveChannelAssignment};
    for parent_first in [true, false] {
        let (coordinator, mut conn, _, numeric, profile) = setup().await;
        let channel = uuid::Uuid::new_v4().to_string();
        let stream = profile + 1;
        let RedirectClaim::Owned(parent) = coordinator
            .reserve_redirect_assignment(numeric, &channel, stream, provider(profile), Arc::new(()))
            .await
            .unwrap()
        else {
            panic!("actual new redirect grant")
        };
        let raw = coordinator
            .acquire(AcquireRequest {
                channel_id: channel.clone(),
                provider_profile_id: None,
                provider_max_streams: 0,
                credential_pool: None,
            })
            .await
            .unwrap();
        let observed = coordinator
            .observe_redirect_assignment(numeric, &channel, Arc::new(()))
            .await
            .unwrap()
            .unwrap();
        raw.adopt_source_assignment(&observed).await.unwrap();
        raw.publish_source_assignment_with_metadata(
            LiveChannelAssignment::new(numeric, stream).unwrap(),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
        let count: i64 = redis::cmd("GET")
            .arg(format!("profile_connections:{profile}"))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(count, 1, "adoption and metadata publication do not INCR");
        if parent_first {
            assert!(parent.release().await.unwrap());
            let actual: (i64, Option<String>) = redis::pipe()
                .get(format!("profile_connections:{profile}"))
                .hget(format!("live:channel:{channel}:metadata"), "m3u_profile")
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(actual, (0, None));
            let other = coordinator
                .acquire(AcquireRequest {
                    channel_id: uuid::Uuid::new_v4().to_string(),
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
            assert_eq!(
                count, 1,
                "later raw cleanup cannot release a genuinely new owner"
            );
            other.release().await.unwrap();
        } else {
            raw.release().await.unwrap();
            let other = coordinator
                .acquire(AcquireRequest {
                    channel_id: uuid::Uuid::new_v4().to_string(),
                    provider_profile_id: Some(profile),
                    provider_max_streams: 1,
                    credential_pool: None,
                })
                .await
                .unwrap();
            assert!(!parent.release().await.unwrap());
            let count: i64 = redis::cmd("GET")
                .arg(format!("profile_connections:{profile}"))
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(
                count, 1,
                "late redirect finally cannot release a genuinely new owner"
            );
            other.release().await.unwrap();
        }
        let _: usize = redis::cmd("DEL")
            .arg(format!("live:channel:{channel}:metadata"))
            .query_async(&mut conn)
            .await
            .unwrap();
    }
}
#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn parent_release_before_raw_metadata_publication_cannot_resurrect_public_assignment() {
    use dispatcharr_coordination::{AcquireRequest, LiveChannelAssignment};
    let (coordinator, mut conn, _, numeric, profile) = setup().await;
    let channel = uuid::Uuid::new_v4().to_string();
    let stream = profile + 1;
    let RedirectClaim::Owned(parent) = coordinator
        .reserve_redirect_assignment(numeric, &channel, stream, provider(profile), Arc::new(()))
        .await
        .unwrap()
    else {
        panic!("parent")
    };
    let raw = coordinator
        .acquire(AcquireRequest {
            channel_id: channel.clone(),
            provider_profile_id: None,
            provider_max_streams: 0,
            credential_pool: None,
        })
        .await
        .unwrap();
    let observed = coordinator
        .observe_redirect_assignment(numeric, &channel, Arc::new(()))
        .await
        .unwrap()
        .unwrap();
    raw.adopt_source_assignment(&observed).await.unwrap();
    parent.release().await.unwrap();
    assert!(raw
        .publish_source_assignment_with_metadata(
            LiveChannelAssignment::new(numeric, stream).unwrap(),
            Duration::from_secs(60)
        )
        .await
        .is_err());
    raw.release().await.unwrap();
    let actual: (Option<String>, i64) = redis::pipe()
        .get(format!("channel_stream:{numeric}"))
        .get(format!("profile_connections:{profile}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(actual, (None, 0));
}

#[tokio::test]
#[ignore = "requires real isolated Redis"]
async fn raw_unlimited_source_switch_moves_one_count_and_keeps_initial_alias() {
    use dispatcharr_coordination::{AcquireRequest, LiveChannelAssignment};

    let (coordinator, mut conn, _, numeric, old_profile) = setup().await;
    let channel = uuid::Uuid::new_v4().to_string();
    let new_profile = old_profile + 1;
    let first_stream = old_profile + 2;
    let second_stream = old_profile + 3;
    let metadata = format!("live:channel:{channel}:metadata");
    let alias = format!("channel_stream:{numeric}");
    let first_profile_alias = format!("stream_profile:{first_stream}");
    let second_profile_alias = format!("stream_profile:{second_stream}");
    let old_count = format!("profile_connections:{old_profile}");
    let new_count = format!("profile_connections:{new_profile}");
    let lease = coordinator
        .acquire(AcquireRequest {
            channel_id: channel,
            provider_profile_id: None,
            provider_max_streams: 0,
            credential_pool: None,
        })
        .await
        .unwrap();
    lease
        .reserve_provider(ProviderReservation {
            profile_id: Some(old_profile),
            max_streams: 0,
            credential_pool: None,
        })
        .await
        .unwrap();
    lease
        .publish_source_assignment_with_metadata(
            LiveChannelAssignment::new(numeric, first_stream).unwrap(),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(&metadata)
        .arg("state")
        .arg("active")
        .query_async::<()>(&mut conn)
        .await
        .unwrap();
    let token = lease.token();
    let fence = lease.fence();
    lease
        .switch_active_source_to_unlimited_profile(second_stream, old_profile, new_profile)
        .await
        .unwrap();
    let crossed: (i64, i64, Option<i64>, Option<i64>, i64, i64, i64) = redis::pipe()
        .get(&alias)
        .get(&first_profile_alias)
        .get(&second_profile_alias)
        .get(&old_count)
        .get(&new_count)
        .hget(&metadata, "stream_id")
        .hget(&metadata, "m3u_profile")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        crossed,
        (
            first_stream,
            new_profile,
            None,
            None,
            1,
            second_stream,
            new_profile
        )
    );
    lease
        .switch_active_source_to_unlimited_profile(first_stream, new_profile, old_profile)
        .await
        .unwrap();
    let same_owner: (i64, i64, i64, i64, String) = redis::pipe()
        .get(&alias)
        .get(&first_profile_alias)
        .get(&old_count)
        .get(&new_count)
        .hget(&metadata, "owner")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(same_owner, (first_stream, old_profile, 1, 0, token.into()));
    assert_eq!(lease.fence(), fence);
    lease.release().await.unwrap();
    let retired: (Option<i64>, Option<i64>, Option<i64>, i64, i64) = redis::pipe()
        .get(&alias)
        .get(&first_profile_alias)
        .get(&second_profile_alias)
        .get(&old_count)
        .get(&new_count)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(retired, (None, None, None, 0, 0));
}
