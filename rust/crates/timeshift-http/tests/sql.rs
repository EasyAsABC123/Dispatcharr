//! Actual Django PostgreSQL + authenticated HTTP + real Redis fixture contract.
use dispatcharr_store::{AuthConfig, AuthService, Store};
use dispatcharr_timeshift_http::*;
use redis::AsyncCommands;
use serde_json::{json, Value};

#[tokio::test]
#[ignore = "requires explicitly seeded disposable Django catch-up fixture, PostgreSQL/Redis and JWT"]
async fn actual_store_auth_channel_visibility_bound_playback_and_active_user_contract() {
    assert_eq!(
        std::env::var("VOD_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let service = CatchupSessions::new(store.clone(), auth, &redis_url, SessionConfig::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = service.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let token = std::env::var("TIMESHIFT_OWNER_ACCESS").expect("private fixture token required");
    let channel = uuid::Uuid::parse_str("0282858c-3869-471b-a58b-e913c1b6bd33").unwrap();
    let owner = store
        .user_by_username("rust-catchup-owner")
        .await
        .unwrap()
        .unwrap();
    let response = client
        .post(format!("http://{address}/api/catchup/sessions/"))
        .bearer_auth(&token)
        .json(&json!({"channel_uuid":channel,"start":"2026-11-09T11:00:00Z","duration":30}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let body = response.json::<Value>().await.unwrap();
    let session = body["session_id"].as_str().unwrap();
    let binding = service.resolve(session, channel).await.unwrap().unwrap();
    assert_eq!(binding.user.id, owner.id);
    assert_eq!(binding.duration.as_deref(), Some("30"));
    assert_eq!(binding.channel.uuid, channel);
    let provider = SqlCatchupResolver::new(store.clone());
    let descriptor = provider
        .resolve(
            owner.clone(),
            binding.channel.clone(),
            binding.start.clone(),
            Some("30"),
        )
        .await
        .unwrap();
    assert_eq!(descriptor.duration_window, 35);
    assert!(!descriptor.redirect);
    assert_eq!(descriptor.candidates.len(), 1);
    assert_eq!(descriptor.candidates[0].urls.len(), 7);
    assert_eq!(descriptor.candidates[0].max_streams, 2);
    assert!(descriptor.candidates[0].urls[0]
        .contains("/timeshift/fixture/fixture/35/2026-11-09:11-00/fixture.ts"));
    assert_eq!(
        provider
            .resolve(
                owner.clone(),
                binding.channel.clone(),
                binding.start.clone(),
                None
            )
            .await
            .unwrap()
            .duration_window,
        50
    );
    assert_eq!(
        provider
            .resolve(
                owner.clone(),
                binding.channel.clone(),
                "2026-11-09T13:00:00Z".into(),
                None
            )
            .await
            .unwrap()
            .duration_window,
        120
    );
    sqlx::query("UPDATE m3u_m3uaccountprofile SET custom_properties=jsonb_build_object('server_info',jsonb_build_object('timezone','America/New_York')) WHERE id=$1").bind(descriptor.candidates[0].profile_id).execute(store.pool()).await.unwrap();
    assert_eq!(
        provider
            .resolve(
                owner.clone(),
                binding.channel.clone(),
                binding.start.clone(),
                Some("30")
            )
            .await
            .unwrap()
            .candidates[0]
            .provider_timestamp,
        "2026-11-09:06-00"
    );
    sqlx::query("UPDATE m3u_m3uaccountprofile SET custom_properties='{}'::jsonb WHERE id=$1")
        .bind(descriptor.candidates[0].profile_id)
        .execute(store.pool())
        .await
        .unwrap();
    // Authorization is bound at mint: a later channel permission change affects
    // new handshakes, while the valid original capability still resolves.
    sqlx::query("UPDATE dispatcharr_channels_channel SET user_level=99 WHERE uuid=$1")
        .bind(channel)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(store
        .channel_for_user(&owner, channel)
        .await
        .unwrap()
        .is_none());
    assert!(service.resolve(session, channel).await.unwrap().is_some());
    sqlx::query("UPDATE dispatcharr_channels_channel SET user_level=0 WHERE uuid=$1")
        .bind(channel)
        .execute(store.pool())
        .await
        .unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("timeshift:session:{session}");
    let _: () = redis.expire(&key, 5).await.unwrap();
    sqlx::query("UPDATE accounts_user SET is_active=false WHERE id=$1")
        .bind(owner.id)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(service.resolve(session, channel).await.unwrap().is_none());
    assert!(redis.ttl::<_, i64>(&key).await.unwrap() <= 5);
    assert_eq!(
        client
            .post(format!("http://{address}/api/catchup/sessions/"))
            .bearer_auth(&token)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    sqlx::query("UPDATE accounts_user SET is_active=true WHERE id=$1")
        .bind(owner.id)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(service.delete(session, owner.id).await);
    assert!(!redis.exists::<_, bool>(&key).await.unwrap());
    server.abort();
}
