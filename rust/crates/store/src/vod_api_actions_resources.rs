//! Ignored real PostgreSQL/Redis controls; no resource performance claims.
use super::super::{
    vod_api_actions_http::ActionHttp,
    vod_api_actions_read::Action,
    vod_api_http::{HttpGate, VodReads},
    vod_api_owned_read::ReadLimits,
};
use axum::body::{to_bytes, Body};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{net::SocketAddr, sync::Arc, time::Duration};
fn request(path: &str, token: &str) -> axum::extract::Request {
    axum::extract::Request::builder()
        .uri(path)
        .header("Host", "localhost")
        .header("Accept", "application/json")
        .header("Authorization", token)
        .body(Body::empty())
        .unwrap()
}
fn auth(
    pool: &PgPool,
    redis: &str,
) -> impl std::future::Future<Output = crate::AuthService> + Send {
    let pool = pool.clone();
    let redis = redis.to_owned();
    async move {
        crate::AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(crate::NotificationCache::connect(&redis).await.unwrap())
    }
}
async fn drain(ordinary: &VodReads) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while ordinary.reader.physical_workers() != 0
            || ordinary.reader.retained_memory_bytes() != 0
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
async fn burst(
    client: &reqwest::Client,
    origin: &str,
    paths: &[String],
    token: &str,
) -> Vec<Value> {
    let barrier = Arc::new(tokio::sync::Barrier::new(11));
    let mut jobs = Vec::new();
    let epoch = tokio::time::Instant::now();
    for index in 0..10 {
        let barrier = barrier.clone();
        let client = client.clone();
        let url = format!("{origin}{}", paths[index % paths.len()]);
        let token = token.to_owned();
        jobs.push(tokio::spawn(async move{barrier.wait().await;let began=epoch.elapsed().as_secs_f64();let response=client.get(&url).header("Host","localhost").header("Accept","application/json").header("Authorization",token).send().await.unwrap();let status=response.status().as_u16();let headers=response.headers().clone();let body=response.bytes().await.unwrap();json!({"index":index,"status":status,"body_sha256":format!("{:x}",Sha256::digest(&body)),"body_text":String::from_utf8_lossy(&body),"Content-Type":headers.get("content-type").and_then(|v|v.to_str().ok()),"Allow":headers.get("allow").and_then(|v|v.to_str().ok()),"Vary":headers.get("vary").and_then(|v|v.to_str().ok()),"started_seconds":began})}));
    }
    barrier.wait().await;
    let mut rows = Vec::new();
    for job in jobs {
        rows.push(job.await.unwrap());
    }
    rows
}
#[tokio::test]
#[ignore = "Requires owned actual PostgreSQL/Redis +Original HTTP fixture"]
async fn shared_actions_mixed_frames_cancel_sql_and_shutdown() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis_url.contains("playback-fixture-only"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_PROVIDER_ACTION_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let token = base["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["role"] == "standard" && row["method"] == "GET")
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap();
    let movie = source["records"][0]["path"].as_str().unwrap().to_owned();
    let series = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["path"]
                .as_str()
                .unwrap()
                .starts_with("/api/vod/series/")
                && row["path"].as_str().unwrap().ends_with("/providers/")
        })
        .unwrap()["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let episodes = series.replace("/providers/", "/episodes/");
    let auth = auth(&pool, &redis_url).await;
    let limits = ReadLimits::default();
    let ordinary = VodReads::new(auth.clone(), limits).unwrap();
    let gate = ordinary.shared_gate();
    let gateway = ActionHttp::shared(auth.clone(), ordinary.reader.shared_runtime(), gate.clone());
    let router = ordinary
        .router()
        .merge(ordinary.category_router())
        .merge(ordinary.provider_router());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let paths = vec![
        movie.clone(),
        series.clone(),
        episodes.clone(),
        "/api/vod/movies/".into(),
        "/api/vod/categories/".into(),
    ];
    let original = std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap();
    let before = super::full_state(&pool).await;
    let source_burst = burst(&client, &original, &paths, token).await;
    assert_eq!(super::full_state(&pool).await, before);
    let native_burst = burst(&client, &format!("http://{address}"), &paths, token).await;
    assert_eq!(super::full_state(&pool).await, before);
    std::fs::write(
        std::path::Path::new(&std::env::var("VOD_NATIVE_EVIDENCE").unwrap())
            .join("provider-actions-mixed-observations.json"),
        serde_json::to_vec_pretty(
            &json!({"Source":source_burst,"native":native_burst,"fullstate":before}),
        )
        .unwrap(),
    )
    .unwrap();
    for (index, (source, native)) in source_burst.iter().zip(&native_burst).enumerate() {
        assert_eq!(source["status"], 200);
        for field in ["status", "body_sha256", "Content-Type", "Allow", "Vary"] {
            assert_eq!(
                source[field], native[field],
                "Unstaggered mixed10 index{index} {field}"
            );
        }
    }
    server.abort();
    let _ = server.await;
    drain(&ordinary).await;
    let peer: SocketAddr = "127.0.0.1:30000".parse().unwrap();
    // A real cache command retains SAME work bytes/HTTP slot after caller abort.
    let mut redis = redis::Client::open(redis_url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(400)
        .arg("ALL")
        .query_async::<()>(&mut redis)
        .await
        .unwrap();
    let copy = gateway.clone();
    let input = request(&movie, token);
    let pending =
        tokio::spawn(async move { copy.respond(Action::MovieProviders, peer, input).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while ordinary.reader.physical_workers() != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    pending.abort();
    let _ = pending.await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(ordinary.reader.physical_workers(), 1);
    assert_eq!(ordinary.reader.retained_memory_bytes(), 68 * 1024 * 1024);
    assert_eq!(gate.slots.available_permits(), 1);
    drain(&ordinary).await;
    assert_eq!(gate.slots.available_permits(), 2);
    // Cross-family body frames retain a shared slot through the FINAL clone.
    let response = gateway
        .respond(Action::MovieProviders, peer, request(&movie, token))
        .await;
    assert_eq!(response.status(), 200);
    let data = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    let held = data.clone();
    drop(data);
    let response = super::super::vod_api_http::respond(
        ordinary.reader.clone(),
        auth.clone(),
        gate.clone(),
        peer,
        super::super::vod_api_sql::Resource::Movie,
        false,
        request("/api/vod/movies/", token),
    )
    .await;
    assert_eq!(response.status(), 200);
    let movie_data = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(gate.slots.available_permits(), 0);
    assert_eq!(ordinary.reader.physical_workers(), 0);
    let held_memory = ordinary.reader.retained_memory_bytes();
    assert!(held_memory > 0);
    let copy = gateway.clone();
    let input = request(&series, token);
    let queued =
        tokio::spawn(async move { copy.respond(Action::SeriesProviders, peer, input).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!queued.is_finished());
    assert_eq!(gate.waiters.available_permits(), 255);
    assert_eq!(ordinary.reader.physical_workers(), 0);
    queued.abort();
    let _ = queued.await;
    assert_eq!(gate.waiters.available_permits(), 256);
    let copy = gateway.clone();
    let input = request(&series, token);
    let queued =
        tokio::spawn(async move { copy.respond(Action::SeriesProviders, peer, input).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!queued.is_finished());
    drop(held);
    let response = queued.await.unwrap();
    assert_eq!(response.status(), 200);
    drop(
        to_bytes(response.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap(),
    );
    drop(movie_data);
    assert_eq!(gate.slots.available_permits(), 2);
    drain(&ordinary).await;
    // Controlled queue deadline: waiters never acquire read/auth/SQL admission.
    let fast = HttpGate::new(ReadLimits {
        logical_deadline: Duration::from_millis(80),
        ..limits
    })
    .unwrap();
    let first = fast.slots.clone().acquire_owned().await.unwrap();
    let second = fast.slots.clone().acquire_owned().await.unwrap();
    let fast_gateway =
        ActionHttp::shared(auth.clone(), ordinary.reader.shared_runtime(), fast.clone());
    let response = fast_gateway
        .respond(Action::MovieProviders, peer, request(&movie, token))
        .await;
    assert_eq!(response.status(), 503);
    drop(response);
    assert_eq!(ordinary.reader.physical_workers(), 0);
    drop(first);
    drop(second);
    // Shutdown closes the SAME gate and leaves detached body credits truthful.
    let response = gateway
        .respond(Action::MovieProviders, peer, request(&movie, token))
        .await;
    let first = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    let response = gateway
        .respond(Action::SeriesProviders, peer, request(&series, token))
        .await;
    let second = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(gate.slots.available_permits(), 0);
    let copy = gateway.clone();
    let input = request(&series, token);
    let queued =
        tokio::spawn(async move { copy.respond(Action::SeriesProviders, peer, input).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(ordinary.shutdown(Duration::from_secs(1)).await);
    let response = queued.await.unwrap();
    assert_eq!(response.status(), 503);
    drop(response);
    assert!(ordinary.reader.retained_memory_bytes() > 0);
    drop(first);
    drop(second);
    assert_eq!(ordinary.reader.retained_memory_bytes(), 0);
    // New isolated controller configuration, SAME declared2/192M limits. SQL
    // caller cancellation retains memory/HTTP admission until lock reply+rollback.
    let sql_owner = VodReads::new(auth.clone(), limits).unwrap();
    let sql_gate = sql_owner.shared_gate();
    let sql_gateway = ActionHttp::shared(
        auth.clone(),
        sql_owner.reader.shared_runtime(),
        sql_gate.clone(),
    );
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE m3u_m3uaccountprofile IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let copy = sql_gateway.clone();
    let input = request(&movie, token);
    let task = tokio::spawn(async move { copy.respond(Action::MovieProviders, peer, input).await });
    tokio::time::timeout(Duration::from_secs(1),async{loop{let waiting:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND wait_event_type='Lock'").fetch_one(&pool).await.unwrap();if waiting>0{break;}tokio::time::sleep(Duration::from_millis(3)).await;}}).await.unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(sql_owner.reader.physical_workers(), 1);
    assert_eq!(sql_owner.reader.retained_memory_bytes(), 68 * 1024 * 1024);
    assert_eq!(sql_gate.slots.available_permits(), 1);
    assert!(!sql_owner.shutdown(Duration::from_millis(20)).await);
    blocker.rollback().await.unwrap();
    drain(&sql_owner).await;
    assert_eq!(sql_gate.slots.available_permits(), 2);
    assert!(sql_owner.shutdown(Duration::from_secs(1)).await);
    // Explicit output capacity returns503, never a partial/clamped successful list.
    let small = VodReads::new(
        auth.clone(),
        ReadLimits {
            response_bytes: 1024,
            ..limits
        },
    )
    .unwrap();
    let tiny = ActionHttp::shared(auth, small.reader.shared_runtime(), small.shared_gate());
    let response = tiny
        .respond(Action::MovieProviders, peer, request(&movie, token))
        .await;
    assert_eq!(response.status(), 503);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body.get("detail").is_some());
    drain(&small).await;
    assert!(small.shutdown(Duration::from_secs(1)).await);
    let report = json!({"passed":true,"scope":"Actual mixed10 Original/native raw wire; separate controlled physicalReply/callerCancel/queue/frame/shutdown/capacity; no performance acceptance","mixed_Source":source_burst,"mixed_native":native_burst,"workers":2,"memory_bytes":192*1024*1024,"Redis400ms_abort_retains_68MiB_and_HTTPslot":true,"PG_relation_lock_abort_retains_68MiB_and_HTTPslot_until_reply_rollback":true,"shutdown_waits_for_pending_SQL":true,"shared_final_frame_clone_and_queue":true,"held_frame_memory_bytes":held_memory,"canceled_expired_waiters_no_read_worker":true,"shutdown_releases_waiters_keeps_body_credits":true,"output_cap_503_not_partial200":true,"full_state_unchanged":super::full_state(&pool).await==before});
    assert_eq!(report["full_state_unchanged"], true);
    std::fs::write(
        std::path::Path::new(&std::env::var("VOD_NATIVE_EVIDENCE").unwrap())
            .join("provider-actions-resources.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}
