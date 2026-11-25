//! Real Redis VOD/live admission differential boundaries.
use dispatcharr_clients::{
    vod::{
        admit, admit_signed_create, check_signed_claim, enter_signed_idle_alias, Admission,
        SignedIdleAlias,
    },
    ClientConfig, ClientStore, Error,
};
use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
use redis::AsyncCommands;
use std::{collections::BTreeMap, time::Duration};
fn fields(id: &str, user: i64, media: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("session_id".into(), id.into()),
        ("user_id".into(), user.to_string()),
        ("content_uuid".into(), media.into()),
        ("created_at".into(), "1".into()),
        ("rust_owner_token".into(), id.into()),
    ])
}
async fn add(
    connection: &mut redis::aio::MultiplexedConnection,
    id: &str,
    data: &BTreeMap<String, String>,
    policy: &UserLimitPolicy,
) -> Result<bool, Error> {
    admit(
        connection,
        Admission {
            session_id: id,
            fields: data,
            limit: 1,
            policy,
            create: true,
            token: id,
            scan_limit: 50000,
            timeout: Duration::from_secs(2),
            ttl: Duration::from_secs(3600),
        },
    )
    .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn mixed_live_vod_race_admits_one_and_preemption_stops_correct_namespace() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let store = ClientStore::connect(&url, ClientConfig::default())
        .await
        .unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let user = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000;
    let policy = UserLimitPolicy {
        terminate_on_limit_exceeded: false,
        ..Default::default()
    };
    let viewer = PlaybackViewer {
        user_id: Some(user),
        username: Some("fixture".into()),
        ip_address: "127.0.0.1".parse().unwrap(),
        user_agent: "fixture".into(),
        stream_limit: 1,
        limit_policy: policy.clone(),
    };
    let channel = uuid::Uuid::new_v4().to_string();
    let id = format!("vod-{}", uuid::Uuid::new_v4());
    let data = fields(&id, user, &uuid::Uuid::new_v4().to_string());
    let (live, vod) = tokio::join!(
        store.register(&channel, &viewer, "ts"),
        add(&mut redis, &id, &data, &policy)
    );
    match (live, vod) {
        (Ok(lease), Err(Error::UserLimit)) => lease.release().await.unwrap(),
        (Err(Error::UserLimit), Ok(true)) => {
            let _: () = redis
                .del(format!("vod_persistent_connection:{id}"))
                .await
                .unwrap();
        }
        _ => panic!("atomic mixed admission must have exactly one winner"),
    }
    // A VOD admission preempts a real live client using its original stop key.
    let live = store.register(&channel, &viewer, "ts").await.unwrap();
    let terminate = UserLimitPolicy::default();
    assert!(add(&mut redis, &id, &data, &terminate).await.unwrap());
    assert!(redis
        .exists::<_, bool>(format!("live:channel:{channel}:client:{}:stop", live.id()))
        .await
        .unwrap());
    assert!(!redis
        .exists::<_, bool>(format!("live:channel:{id}:clients"))
        .await
        .unwrap());
    live.release().await.unwrap();
    // A live admission sees the actual VOD hash and uses the VOD stop namespace.
    let mut preempt = viewer.clone();
    preempt.limit_policy = terminate;
    let live = store.register(&channel, &preempt, "ts").await.unwrap();
    assert!(redis
        .exists::<_, bool>(format!("vod_proxy:client:{id}:stop"))
        .await
        .unwrap());
    live.release().await.unwrap();
    let _: () = redis
        .del(&[
            format!("vod_persistent_connection:{id}"),
            format!("vod_proxy:client:{id}:stop"),
        ])
        .await
        .unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn vod_never_gets_live_same_channel_cap_exemption_and_corrupt_stop_is_atomic() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let user = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000;
    let media = uuid::Uuid::new_v4().to_string();
    let first = format!("vod-{}", uuid::Uuid::new_v4());
    let second = format!("vod-{}", uuid::Uuid::new_v4());
    let a = fields(&first, user, &media);
    let b = fields(&second, user, &media);
    let policy = UserLimitPolicy {
        terminate_on_limit_exceeded: false,
        ignore_same_channel_connections: true,
        ..Default::default()
    };
    assert!(add(&mut redis, &first, &a, &policy).await.unwrap());
    assert_eq!(
        add(&mut redis, &second, &b, &policy).await.unwrap_err(),
        Error::UserLimit
    );
    let stop = format!("vod_proxy:client:{first}:stop");
    let _: () = redis.hset(&stop, "bad", "type").await.unwrap();
    assert_eq!(
        add(&mut redis, &second, &b, &UserLimitPolicy::default())
            .await
            .unwrap_err(),
        Error::CorruptMetadata
    );
    assert!(!redis
        .exists::<_, bool>(format!("vod_persistent_connection:{second}"))
        .await
        .unwrap());
    assert!(!redis
        .hexists::<_, _, bool>(
            format!("vod_persistent_connection:{first}"),
            "rust_preempted"
        )
        .await
        .unwrap());
    let _: () = redis
        .del(&[format!("vod_persistent_connection:{first}"), stop])
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn fresh_signed_alias_preserves_both_users_quota_and_reserves_one_retained_row() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let suffix = uuid::Uuid::new_v4();
    let prior = format!("vod_prior_{suffix}");
    let requested = format!("vod_request_{suffix}");
    let prior_other = format!("vod_prior_other_{suffix}");
    let requester_other = format!("vod_requester_other_{suffix}");
    let key = |id: &str| format!("vod_persistent_connection:{id}");
    let prior_key = key(&prior);
    let marker = format!("vod_signed_idle_alias:{requested}");
    let stop = format!("vod_proxy:client:{requested}:stop");
    let mapping = format!("vod_session_user:{requested}");
    let lease_key = format!("exact_vod_lease_test:{suffix}");
    let prior_viewer = (suffix.as_u128() % 1_000_000_000) as i64 + 1000;
    let requester = prior_viewer + 1;
    let claim = format!("movie:{suffix}:{requester}");
    let deny = UserLimitPolicy {
        terminate_on_limit_exceeded: false,
        ..Default::default()
    };
    assert!(add(
        &mut redis,
        &prior,
        &fields(&prior, prior_viewer, "primary"),
        &deny
    )
    .await
    .unwrap());
    let _: () = redis
        .hset_multiple(
            &prior_key,
            &[
                ("active_streams", "0"),
                ("request_count", "1"),
                ("last_activity", "1"),
                ("m3u_profile_id", "17"),
            ],
        )
        .await
        .unwrap();
    let _: () = redis.set(&lease_key, "retained").await.unwrap();
    let _: () = redis.set_ex(&mapping, requester, 300).await.unwrap();
    let mut alias = SignedIdleAlias {
        requested: &requested,
        prior: &prior,
        claim: &claim,
        requester,
        prior_viewer,
        requester_limit: 1,
        content: "primary",
        profile: 17,
        metadata_token: &prior,
        lease_key: &lease_key,
        lease_token: "retained",
        now: "2",
    };
    assert!(!enter_signed_idle_alias(&mut redis, &alias).await.unwrap());
    let _: () = redis.set_ex(&marker, &claim, 300).await.unwrap();
    let request_fields = fields(&requested, requester, "primary");
    let create = || Admission {
        session_id: &requested,
        fields: &request_fields,
        limit: 1,
        policy: &deny,
        create: true,
        token: &requested,
        scan_limit: 1000,
        timeout: Duration::from_secs(2),
        ttl: Duration::from_secs(10),
    };
    assert_eq!(
        check_signed_claim(&mut redis, &requested, Some(&claim), Some(requester)).await,
        Ok(true)
    );
    assert_eq!(
        check_signed_claim(&mut redis, &requested, Some(&claim), Some(prior_viewer)).await,
        Err(Error::Stopped)
    );
    assert_eq!(
        admit_signed_create(&mut redis, create(), false, None).await,
        Err(Error::Stopped)
    );
    let _: () = redis.set_ex(&stop, "true", 30).await.unwrap();
    assert_eq!(
        admit_signed_create(&mut redis, create(), false, Some(&claim)).await,
        Err(Error::Stopped)
    );
    assert_eq!(
        enter_signed_idle_alias(&mut redis, &alias).await,
        Err(Error::Stopped)
    );
    let _: () = redis.del(&stop).await.unwrap();
    assert_eq!(
        admit_signed_create(&mut redis, create(), false, Some(&claim)).await,
        Ok(true)
    );
    assert_eq!(
        enter_signed_idle_alias(&mut redis, &alias).await,
        Err(Error::Stopped)
    );
    let _: () = redis.del(key(&requested)).await.unwrap();

    alias.lease_token = "different-generation";
    assert_eq!(
        enter_signed_idle_alias(&mut redis, &alias).await,
        Err(Error::Stopped)
    );
    alias.lease_token = "retained";
    alias.profile = 18;
    assert_eq!(
        enter_signed_idle_alias(&mut redis, &alias).await,
        Err(Error::Stopped)
    );
    alias.profile = 17;
    let requester_data = fields(&requester_other, requester, "other");
    assert!(add(&mut redis, &requester_other, &requester_data, &deny)
        .await
        .unwrap());
    assert_eq!(
        enter_signed_idle_alias(&mut redis, &alias).await,
        Err(Error::UserLimit)
    );
    let _: () = redis.del(key(&requester_other)).await.unwrap();
    let mut rival = redis.clone();
    let (one, two) = tokio::join!(
        enter_signed_idle_alias(&mut redis, &alias),
        enter_signed_idle_alias(&mut rival, &alias)
    );
    assert!(matches!(
        (one, two),
        (Ok(true), Err(Error::Stopped)) | (Err(Error::Stopped), Ok(true))
    ));
    let stored: (i64, i64, i64) = redis
        .hget(&prior_key, &["user_id", "active_streams", "request_count"])
        .await
        .unwrap();
    assert_eq!(stored, (prior_viewer, 1, 2));
    assert!(!redis.exists::<_, bool>(key(&requested)).await.unwrap());
    assert_eq!(redis.get::<_, i64>(&mapping).await.unwrap(), requester);
    assert_eq!(
        add(
            &mut redis,
            &prior_other,
            &fields(&prior_other, prior_viewer, "other"),
            &deny
        )
        .await,
        Err(Error::UserLimit)
    );
    assert!(add(&mut redis, &requester_other, &requester_data, &deny)
        .await
        .unwrap());
    assert_eq!(
        admit_signed_create(&mut redis, create(), false, Some(&claim)).await,
        Err(Error::Stopped)
    );
    assert_eq!(
        check_signed_claim(&mut redis, &requested, Some(&claim), Some(requester)).await,
        Err(Error::Stopped)
    );
    let _: () = redis
        .del(&[
            prior_key,
            key(&requested),
            key(&prior_other),
            key(&requester_other),
            marker,
            mapping,
            lease_key,
        ])
        .await
        .unwrap();
}
