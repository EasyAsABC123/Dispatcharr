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
    let service = CatchupSessions::new(
        store.clone(),
        auth.clone(),
        &redis_url,
        SessionConfig::default(),
    )
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
    // A Redirect channel still proxies established API capabilities. Only a
    // bare Redirect handoff applies the user's allowed-provider profile list.
    let original_profile: Option<i64> = sqlx::query_scalar(
        "SELECT stream_profile_id::bigint FROM dispatcharr_channels_channel WHERE uuid=$1",
    )
    .bind(channel)
    .fetch_one(store.pool())
    .await
    .unwrap();
    let redirect_profile: i64 = sqlx::query_scalar(
        "SELECT id::bigint FROM core_streamprofile WHERE locked AND name='Redirect'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE dispatcharr_channels_channel SET stream_profile_id=$1 WHERE uuid=$2")
        .bind(redirect_profile)
        .bind(channel)
        .execute(store.pool())
        .await
        .unwrap();
    let mut restricted = owner.clone();
    restricted
        .custom_properties
        .as_object_mut()
        .unwrap()
        .insert("allowed_m3u_profile_ids".into(), json!([]));
    let proxy = provider
        .resolve(
            restricted.clone(),
            binding.channel.clone(),
            binding.start.clone(),
            Some("30"),
        )
        .await
        .unwrap();
    assert!(proxy.redirect);
    assert!(!proxy.candidates.is_empty());
    let redirect = provider
        .resolve_redirect(
            restricted,
            binding.channel.clone(),
            binding.start.clone(),
            Some("30"),
        )
        .await;
    assert!(matches!(
        redirect,
        Err(dispatcharr_store::StoreError::StatusBody(503, _))
    ));
    sqlx::query("UPDATE dispatcharr_channels_channel SET stream_profile_id=$1 WHERE uuid=$2")
        .bind(original_profile)
        .bind(channel)
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
    let mut redis = redis::Client::open(redis_url.as_str())
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
    let _: () = redis.expire(&key, 600).await.unwrap();
    if let Ok(origin) = std::env::var("TIMESHIFT_ARCHIVE_ORIGIN") {
        let original_limits: Option<Value> = sqlx::query_scalar(
            "SELECT value FROM core_coresettings WHERE key='user_limit_settings'",
        )
        .fetch_optional(store.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE accounts_user SET stream_limit=1 WHERE id=$1")
            .bind(owner.id)
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES ('user_limit_settings','Fixture user limits',$1) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value").bind(json!({"terminate_on_limit_exceeded":false,"prioritize_single_client_channels":true,"ignore_same_channel_connections":false,"terminate_oldest":true})).execute(store.pool()).await.unwrap();
        let original_server: String = sqlx::query_scalar(
            "SELECT server_url FROM m3u_m3uaccount WHERE name='Rust VOD fixture XC'",
        )
        .fetch_one(store.pool())
        .await
        .unwrap();
        sqlx::query("UPDATE m3u_m3uaccount SET server_url=$1 WHERE name='Rust VOD fixture XC'")
            .bind(format!("{origin}/player_api.php"))
            .execute(store.pool())
            .await
            .unwrap();
        let coordinator = dispatcharr_coordination::Coordinator::connect(
            &redis_url,
            dispatcharr_coordination::LeaseConfig {
                namespace: format!("test:actual-native-archive:{}", uuid::Uuid::new_v4()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let media = CatchupMedia::new(
            store.clone(),
            auth,
            service.clone(),
            coordinator,
            &redis_url,
            CatchupMediaConfig::default(),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let media_address = listener.local_addr().unwrap();
        let router = media.clone().router();
        let media_server = tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
            .unwrap();
        });
        let created = client
            .post(format!("http://{address}/api/catchup/sessions/"))
            .bearer_auth(&token)
            .json(&json!({"channel_uuid":channel,"start":"2026-11-09T11:00:00Z","duration":30}))
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), 201);
        let created = created.json::<Value>().await.unwrap();
        let media_sid = created["session_id"].as_str().unwrap();
        let live_key = format!(
            "live:channel:{}:clients:{}",
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4()
        );
        let _: () = redis
            .hset_multiple(
                &live_key,
                &[
                    ("user_id", owner.id.to_string()),
                    ("connected_at", "1".into()),
                ],
            )
            .await
            .unwrap();
        let refused = client
            .get(format!(
                "http://{media_address}{}",
                created["playback_url"].as_str().unwrap()
            ))
            .header("Range", "bytes=0-1879999")
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 403);
        assert_eq!(refused.text().await.unwrap(), "Stream limit exceeded");
        assert_eq!(
            redis
                .get::<_, Option<i64>>(format!(
                    "profile_connections:{}",
                    descriptor.candidates[0].profile_id
                ))
                .await
                .unwrap()
                .unwrap_or(0),
            0
        );
        assert!(!redis
            .exists::<_, bool>(format!("timeshift:pool:{media_sid}"))
            .await
            .unwrap());
        let _: () = redis.del(&live_key).await.unwrap();
        let response = client
            .get(format!(
                "http://{media_address}{}",
                created["playback_url"].as_str().unwrap()
            ))
            .header("Range", "bytes=0-1879999")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 206);
        assert_eq!(
            response.headers()["content-range"],
            "bytes 0-1879999/15001836"
        );
        assert_eq!(response.headers()["content-length"], "1880000");
        let bytes = response.bytes().await.unwrap();
        assert_eq!(bytes.len(), 1_880_000);
        use sha2::{Digest, Sha256};
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            "5eb05561584b30d6ef1c1f27a26d27bf2572e5580e29dff45ec9bd2b6c19f8a0"
        );
        std::fs::write(
            std::env::var("TIMESHIFT_RUST_MEDIA_FILE")
                .expect("bounded media evidence path required"),
            &bytes,
        )
        .unwrap();
        let media_key = format!("timeshift:session:{media_sid}");
        tokio::time::timeout(std::time::Duration::from_secs(12), async {
            while redis.exists::<_, bool>(&media_key).await.unwrap() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("source native post-body grace cleanup");
        assert!(!redis
            .exists::<_, bool>(format!("timeshift:pool:{media_sid}"))
            .await
            .unwrap());
        assert!(!redis
            .exists::<_, bool>(format!("live:channel:{media_sid}:owner"))
            .await
            .unwrap());
        assert_eq!(
            redis
                .get::<_, i64>(format!(
                    "profile_connections:{}",
                    descriptor.candidates[0].profile_id
                ))
                .await
                .unwrap(),
            0
        );
        if std::env::var("TIMESHIFT_RUST_IDLE").as_deref() == Ok("1") {
            let cap = client
                .post(format!("http://{address}/api/catchup/sessions/"))
                .bearer_auth(&token)
                .json(&json!({"channel_uuid":channel,"start":"2026-11-09T11:00:00Z","duration":30}))
                .send()
                .await
                .unwrap()
                .json::<Value>()
                .await
                .unwrap();
            let sid = cap["session_id"].as_str().unwrap();
            let poolkey = format!("timeshift:pool:{sid}");
            let clientkey = format!(
                "timeshift:channel:{}_{sid}:clients:{sid}",
                descriptor.channel.id
            );
            let metakey = format!("timeshift:channel:{}_{sid}:metadata", descriptor.channel.id);
            let mut connected = String::new();
            let mut initialized = String::new();
            for (index, (range, length, digest)) in [
                (
                    "bytes=0-255",
                    256,
                    "222cc5871d7518afec470f6f067a46b5dd78b302f239ac7654f217b8876ecf38",
                ),
                (
                    "bytes=0-1879999",
                    1_880_000,
                    "5eb05561584b30d6ef1c1f27a26d27bf2572e5580e29dff45ec9bd2b6c19f8a0",
                ),
                (
                    "bytes=1880000-1880255",
                    256,
                    "869eee20827b8ebaa0fe00cd53e1a1cf5b6802c884f3382f50ea9fe71647fde2",
                ),
            ]
            .into_iter()
            .enumerate()
            {
                let response = client
                    .get(format!(
                        "http://{media_address}{}",
                        cap["playback_url"].as_str().unwrap()
                    ))
                    .header("Range", range)
                    .header("User-Agent", "fixture-idle-same-capability")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), 206);
                let payload = response.bytes().await.unwrap();
                assert_eq!(payload.len(), length);
                assert_eq!(format!("{:x}", Sha256::digest(&payload)), digest);
                tokio::time::timeout(std::time::Duration::from_secs(3), async {
                    while redis
                        .hget::<_, _, String>(&poolkey, "busy")
                        .await
                        .unwrap_or_default()
                        != "0"
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap();
                let next_connected: String = redis.hget(&clientkey, "connected_at").await.unwrap();
                let next_initialized: String = redis.hget(&metakey, "init_time").await.unwrap();
                if index == 0 {
                    connected = next_connected;
                    initialized = next_initialized;
                } else {
                    assert_eq!(next_connected, connected);
                    assert_eq!(next_initialized, initialized);
                }
                assert_eq!(
                    redis
                        .get::<_, i64>(format!(
                            "profile_connections:{}",
                            descriptor.candidates[0].profile_id
                        ))
                        .await
                        .unwrap(),
                    0
                );
            }
            assert_eq!(
                redis
                    .hget::<_, _, u64>(&metakey, "total_bytes")
                    .await
                    .unwrap(),
                1_880_512
            );
            assert!(
                (redis
                    .hget::<_, _, f64>(&clientkey, "playback_base_secs")
                    .await
                    .unwrap()
                    - 263.16778826271667)
                    .abs()
                    < 1e-9
            );
            tokio::time::sleep(std::time::Duration::from_secs(9)).await;
            assert!(redis.exists::<_, bool>(&poolkey).await.unwrap());
            assert!(redis
                .exists::<_, bool>(format!("timeshift:session:{sid}"))
                .await
                .unwrap());
            assert!(service.delete(sid, owner.id).await);
        }
        if std::env::var("TIMESHIFT_RUST_BUSY").as_deref() == Ok("1") {
            let golden: Value = serde_json::from_str(include_str!(
                "../../../../scripts/rust-timeshift-contracts/python-busy-seek-goldens.json"
            ))
            .unwrap();
            for (index, range) in [
                Some("bytes=1880000-1880255"),
                None,
                Some("bytes=14999900-15000155"),
            ]
            .into_iter()
            .enumerate()
            {
                let cap=client.post(format!("http://{address}/api/catchup/sessions/"))
                    .bearer_auth(&token).json(&json!({"channel_uuid":channel,"start":"2026-11-09T11:00:00Z","duration":30}))
                    .send().await.unwrap().json::<Value>().await.unwrap();
                let sid = cap["session_id"].as_str().unwrap();
                let route = format!(
                    "http://{media_address}{}",
                    cap["playback_url"].as_str().unwrap()
                );
                let mut first = client
                    .get(&route)
                    .header("Range", "bytes=0-")
                    .send()
                    .await
                    .unwrap();
                assert_eq!(first.status(), 206);
                let mut initial = Vec::with_capacity(65536);
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while initial.len() < 65536 {
                        let chunk = first
                            .chunk()
                            .await
                            .unwrap()
                            .expect("active initial archive body");
                        initial
                            .extend_from_slice(&chunk[..(65536 - initial.len()).min(chunk.len())]);
                    }
                })
                .await
                .unwrap();
                let began = std::time::Instant::now();
                let key = format!("timeshift:pool:{sid}");
                let counter = format!(
                    "profile_connections:{}",
                    descriptor.candidates[0].profile_id
                );
                assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
                let mut request = client.get(&route);
                if let Some(range) = range {
                    request = request.header("Range", range);
                }
                let mut replacement = request.send().await.unwrap();
                assert_eq!(
                    replacement.status().as_u16(),
                    if index == 1 { 200 } else { 206 }
                );
                let mut captured = Vec::with_capacity(65536);
                if index == 1 {
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        while captured.len() < 65536 {
                            let chunk = replacement
                                .chunk()
                                .await
                                .unwrap()
                                .expect("restarted archive prefix");
                            captured.extend_from_slice(
                                &chunk[..(65536 - captured.len()).min(chunk.len())],
                            );
                        }
                    })
                    .await
                    .unwrap();
                    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
                    drop(replacement);
                } else {
                    captured.extend_from_slice(&replacement.bytes().await.unwrap());
                    assert_eq!(captured.len(), 256);
                }
                assert_eq!(
                    format!("{:x}", Sha256::digest(&captured)),
                    golden["events"][index]["captured_sha256"].as_str().unwrap()
                );
                if index == 2 {
                    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
                    assert_eq!(redis.hget::<_, _, String>(&key, "busy").await.unwrap(), "1");
                    assert!(first.chunk().await.unwrap().is_some());
                    tokio::time::sleep(std::time::Duration::from_secs_f64(
                        (2.5 - began.elapsed().as_secs_f64()).max(0.0),
                    ))
                    .await;
                } else {
                    tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        let mut queued = 0;
                        while let Ok(Some(chunk)) = first.chunk().await {
                            queued += chunk.len();
                            assert!(queued < 2_000_001, "old generation must terminate");
                        }
                    })
                    .await
                    .expect("old busy generation must terminate");
                }
                drop(first);
                tokio::time::sleep(std::time::Duration::from_secs(9)).await;
                assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 0);
                if index != 0 {
                    assert!(!redis.exists::<_, bool>(&key).await.unwrap());
                    assert!(!redis
                        .exists::<_, bool>(format!("timeshift:session:{sid}"))
                        .await
                        .unwrap());
                } else {
                    assert!(redis.exists::<_, bool>(&key).await.unwrap());
                }
                let _ = service.delete(sid, owner.id).await;
                println!("actual_busy_archive_case_{}: exact source bytes, quota and generation cleanup passed",index);
            }
        }
        assert!(media.shutdown().await);
        media_server.abort();
        sqlx::query("UPDATE accounts_user SET stream_limit=$1 WHERE id=$2")
            .bind(owner.stream_limit)
            .bind(owner.id)
            .execute(store.pool())
            .await
            .unwrap();
        if let Some(value) = original_limits {
            sqlx::query("UPDATE core_coresettings SET value=$1 WHERE key='user_limit_settings'")
                .bind(value)
                .execute(store.pool())
                .await
                .unwrap();
        } else {
            sqlx::query("DELETE FROM core_coresettings WHERE key='user_limit_settings'")
                .execute(store.pool())
                .await
                .unwrap();
        }
        sqlx::query("UPDATE m3u_m3uaccount SET server_url=$1 WHERE name='Rust VOD fixture XC'")
            .bind(original_server)
            .execute(store.pool())
            .await
            .unwrap();
    }
    assert!(service.delete(session, owner.id).await);
    assert!(!redis.exists::<_, bool>(&key).await.unwrap());
    server.abort();
}
