//! Real Redis VOD/live admission differential boundaries.
use dispatcharr_clients::{
    vod::{admit, Admission},
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
