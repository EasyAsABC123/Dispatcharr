//! Queued real Redis Timeshift admission contracts; original application credit
//! belongs entirely to Dispatcharr's original authors and contributors.
use dispatcharr_clients::{
    timeshift::{self, Admission},
    ClientConfig, ClientStore, Error,
};
use dispatcharr_contracts::{PlaybackViewer, UserLimitPolicy};
use redis::{AsyncCommands, Script};
use std::{collections::BTreeMap, time::Duration};
fn user() -> i64 {
    (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as i64 + 1000
}
async fn connection() -> redis::aio::MultiplexedConnection {
    redis::Client::open(std::env::var("REDIS_TEST_URL").expect("disposable Redis required"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}
fn fields(user: i64, programme: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("user_id".into(), user.to_string()),
        ("username".into(), "fixture-viewer".into()),
        ("programme_vid".into(), programme.into()),
    ])
}
async fn add(
    redis: &mut redis::aio::MultiplexedConnection,
    sid: &str,
    channel: i64,
    user: i64,
    policy: &UserLimitPolicy,
) -> Result<bool, Error> {
    let data = fields(user, &format!("{channel}_programme_provider"));
    let media = format!("{channel}_programme");
    let prepared = timeshift::prepare(Admission {
        mode: timeshift::AdmissionMode::Create,
        session_id: sid,
        channel_id: channel,
        programme_media_id: &media,
        fields: &data,
        stream_limit: 1,
        policy,
        owner_token: sid,
        now: "2",
        ttl: Duration::from_secs(60),
        scan_limit: 50000,
    })?;
    let script = Script::new(timeshift::SCRIPT);
    let mut invocation = script.prepare_invoke();
    for key in prepared.keys {
        invocation.key(key);
    }
    for arg in prepared.args {
        invocation.arg(arg);
    }
    let result = invocation
        .invoke_async(redis)
        .await
        .map_err(|_| Error::RedisUnavailable)?;
    timeshift::decode(result)
}
fn client_key(channel: i64, sid: &str) -> String {
    format!("timeshift:channel:{channel}_{sid}:clients:{sid}")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn mixed_live_timeshift_one_winner_and_bidirectional_preemption_use_actual_namespaces() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let store = ClientStore::connect(&url, ClientConfig::default())
        .await
        .unwrap();
    let mut redis = connection().await;
    let user = user();
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
    let live_channel = uuid::Uuid::new_v4().to_string();
    let sid = uuid::Uuid::new_v4().to_string();
    let channel = user;
    let (live, timeshift) = tokio::join!(
        store.register(&live_channel, &viewer, "ts"),
        add(&mut redis, &sid, channel, user, &policy)
    );
    match (live, timeshift) {
        (Ok(lease), Err(Error::UserLimit)) => lease.release().await.unwrap(),
        (Err(Error::UserLimit), Ok(true)) => {
            let _: () = redis.del(client_key(channel, &sid)).await.unwrap();
        }
        _ => panic!("mixed source-accounting admission requires one winner"),
    }
    let live = store.register(&live_channel, &viewer, "ts").await.unwrap();
    assert!(
        add(&mut redis, &sid, channel, user, &UserLimitPolicy::default())
            .await
            .unwrap()
    );
    let live_stop = format!("live:channel:{live_channel}:client:{}:stop", live.id());
    if let Some(value) = redis.get::<_, Option<String>>(&live_stop).await.unwrap() {
        assert_eq!(value, "true");
        assert!((0..=30).contains(&redis.ttl::<_, i64>(&live_stop).await.unwrap()));
    } else {
        assert!(*live.stopped().borrow());
    }
    live.release().await.unwrap();
    let mut preempt = viewer.clone();
    preempt.limit_policy = UserLimitPolicy::default();
    let live = store.register(&live_channel, &preempt, "ts").await.unwrap();
    let stop = format!("timeshift:channel:{channel}_programme_provider:client:{sid}:stop");
    assert_eq!(redis.get::<_, String>(&stop).await.unwrap(), "limit");
    assert!(!redis
        .exists::<_, bool>(format!("live:channel:{sid}:clients"))
        .await
        .unwrap());
    live.release().await.unwrap();
    let _: () = redis
        .del(&[client_key(channel, &sid), stop, live_stop])
        .await
        .unwrap();
    store.shutdown().await;
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn literal_same_channel_hop_happens_before_another_live_connection_causes_cap_refusal() {
    let mut redis = connection().await;
    let user = user();
    let channel = user;
    let old = uuid::Uuid::new_v4().to_string();
    let new = uuid::Uuid::new_v4().to_string();
    let old_key = client_key(channel, &old);
    let set = format!("timeshift:channel:{channel}_{old}:clients");
    let metadata = format!("timeshift:channel:{channel}_{old}:metadata");
    let stop = format!("timeshift:channel:{channel}_programme_provider:client:{old}:stop");
    let live = format!(
        "live:channel:{}:clients:{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    let _: () = redis
        .hset_multiple(
            &old_key,
            &[
                ("user_id", user.to_string()),
                ("connected_at", "1".into()),
                ("programme_vid", format!("{channel}_programme_provider")),
            ],
        )
        .await
        .unwrap();
    let _: () = redis.sadd(&set, &old).await.unwrap();
    let _: () = redis.hset(&metadata, "channel_id", channel).await.unwrap();
    let _: () = redis
        .hset_multiple(
            &live,
            &[("user_id", user.to_string()), ("connected_at", "1".into())],
        )
        .await
        .unwrap();
    let policy = UserLimitPolicy {
        terminate_on_limit_exceeded: false,
        ..Default::default()
    };
    assert!(matches!(
        add(&mut redis, &new, channel, user, &policy).await,
        Err(Error::UserLimit)
    ));
    assert!(!redis.exists::<_, bool>(&old_key).await.unwrap());
    assert!(!redis.exists::<_, bool>(&set).await.unwrap());
    assert!(!redis.exists::<_, bool>(&metadata).await.unwrap());
    assert_eq!(redis.get::<_, String>(&stop).await.unwrap(), "hop");
    assert!(redis.exists::<_, bool>(&live).await.unwrap());
    assert!(!redis
        .exists::<_, bool>(client_key(channel, &new))
        .await
        .unwrap());
    let _: () = redis.del(&[live, stop]).await.unwrap();
}
#[tokio::test]
#[ignore = "requires REDIS_TEST_URL pointing to disposable real Redis"]
async fn vod_preemption_retains_literal_stop_value_and_corrupt_stop_fails_before_hop_mutations() {
    let mut redis = connection().await;
    let user = user();
    let channel = user;
    let sid = uuid::Uuid::new_v4().to_string();
    let vod = uuid::Uuid::new_v4().to_string();
    let key = format!("vod_persistent_connection:{vod}");
    let stop = format!("vod_proxy:client:{vod}:stop");
    let _: () = redis
        .hset_multiple(
            &key,
            &[
                ("user_id", user.to_string()),
                ("created_at", "1".into()),
                ("content_uuid", "fixture-content".into()),
            ],
        )
        .await
        .unwrap();
    let _: () = redis.sadd(&stop, "corrupt").await.unwrap();
    assert!(matches!(
        add(&mut redis, &sid, channel, user, &UserLimitPolicy::default()).await,
        Err(Error::CorruptMetadata)
    ));
    assert_eq!(
        redis
            .hget::<_, _, Option<String>>(&key, "rust_preempted")
            .await
            .unwrap(),
        None
    );
    assert!(!redis
        .exists::<_, bool>(client_key(channel, &sid))
        .await
        .unwrap());
    let _: () = redis.del(&stop).await.unwrap();
    assert!(
        add(&mut redis, &sid, channel, user, &UserLimitPolicy::default())
            .await
            .unwrap()
    );
    assert_eq!(redis.get::<_, String>(&stop).await.unwrap(), "true");
    assert!((0..=60).contains(&redis.ttl::<_, i64>(&stop).await.unwrap()));
    let _: () = redis
        .del(&[key, stop, client_key(channel, &sid)])
        .await
        .unwrap();
}
