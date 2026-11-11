//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::super::{
    vod_api_actions_http::ActionHttp,
    vod_api_actions_read::Action,
    vod_api_http::{HttpGate, VodReads},
    vod_api_owned_read::ReadLimits,
};
use axum::body::{to_bytes, Body};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::{net::SocketAddr, time::Duration};
fn request(path: &str, token: &str) -> axum::extract::Request {
    axum::extract::Request::builder()
        .uri(path)
        .header("Host", "localhost")
        .header("Accept", "application/json")
        .header("Authorization", token)
        .body(Body::empty())
        .unwrap()
}
async fn drain(owner: &VodReads) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while owner.reader.physical_workers() != 0 || owner.reader.retained_memory_bytes() != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
pub(super) async fn controls(pool: &PgPool, base: &Value, source: &Value) {
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool.clone()),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    );
    let limits = ReadLimits::default();
    let owner = VodReads::new(auth.clone(), limits).unwrap();
    let gate = owner.shared_gate();
    let gateway = ActionHttp::shared(auth.clone(), owner.reader.shared_runtime(), gate.clone());
    let peer: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let token = base["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["role"] == "standard"
                && r["method"] == "GET"
                && r["headers"]["HTTP_AUTHORIZATION"].is_string()
        })
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap();
    let movie = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["endpoint"] == "movies"
                && r["role"] == "standard"
                && r["label"] == "baseline"
                && r["method"] == "GET"
                && r["query"] == ""
        })
        .unwrap();
    let series = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["endpoint"] == "series"
                && r["role"] == "standard"
                && r["label"] == "baseline"
                && r["method"] == "GET"
                && r["query"] == ""
        })
        .unwrap();
    let movie_path = movie["path"].as_str().unwrap();
    let series_path = series["path"].as_str().unwrap();
    // Two cached-info bodies consume the existing ordinary/category gate. Their
    // final clones retain credits after shutdown; no second pool/service exists.
    let response = gateway
        .respond(Action::MovieInfo, peer, request(movie_path, token))
        .await;
    assert_eq!(response.status(), 200);
    let bytes = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        bytes.as_ref(),
        movie["expected"]["body_text"].as_str().unwrap().as_bytes()
    );
    let final_clone = bytes.clone();
    drop(bytes);
    let response = gateway
        .respond(Action::SeriesInfo, peer, request(series_path, token))
        .await;
    assert_eq!(response.status(), 200);
    let series_bytes = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(
        series_bytes.as_ref(),
        series["expected"]["body_text"].as_str().unwrap().as_bytes()
    );
    assert_eq!(gate.slots.available_permits(), 0);
    let retained = owner.reader.retained_memory_bytes();
    assert!(retained > 0);
    let copy = gateway.clone();
    let input = request(movie_path, token);
    let task = tokio::spawn(async move { copy.respond(Action::MovieInfo, peer, input).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!task.is_finished());
    assert_eq!(owner.reader.physical_workers(), 0);
    task.abort();
    let _ = task.await;
    assert_eq!(gate.waiters.available_permits(), 256);
    let fast = HttpGate::new(ReadLimits {
        logical_deadline: Duration::from_millis(80),
        ..limits
    })
    .unwrap();
    let first = fast.slots.clone().acquire_owned().await.unwrap();
    let second = fast.slots.clone().acquire_owned().await.unwrap();
    let fast_gateway = ActionHttp::shared(auth.clone(), owner.reader.shared_runtime(), fast);
    let response = fast_gateway
        .respond(Action::MovieInfo, peer, request(movie_path, token))
        .await;
    assert_eq!(response.status(), 503);
    drop(response);
    assert_eq!(owner.reader.physical_workers(), 0);
    drop(first);
    drop(second);
    assert!(owner.shutdown(Duration::from_secs(1)).await);
    assert_eq!(owner.reader.retained_memory_bytes(), retained);
    drop(final_clone);
    assert!(owner.reader.retained_memory_bytes() > 0);
    drop(series_bytes);
    assert_eq!(owner.reader.retained_memory_bytes(), 0);
    // Actual blocked SQL: caller cancellation cannot release the physical worker,
    // its 68MiB admission credits, or the shared HTTP slot before reply+rollback.
    let sql_owner = VodReads::new(auth.clone(), limits).unwrap();
    let sql_gate = sql_owner.shared_gate();
    let sql_gateway = ActionHttp::shared(
        auth.clone(),
        sql_owner.reader.shared_runtime(),
        sql_gate.clone(),
    );
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE vod_m3useriesrelation IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let input = request(series_path, token);
    let task =
        tokio::spawn(async move { sql_gateway.respond(Action::SeriesInfo, peer, input).await });
    tokio::time::timeout(Duration::from_secs(2),async{loop{let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND wait_event_type='Lock'").persistent(false).fetch_one(pool).await.unwrap();if count>0{break;}tokio::time::sleep(Duration::from_millis(3)).await;}}).await.unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(sql_owner.reader.physical_workers(), 1);
    assert_eq!(sql_owner.reader.retained_memory_bytes(), 68 * 1024 * 1024);
    assert_eq!(sql_gate.slots.available_permits(), 1);
    assert!(!sql_owner.shutdown(Duration::from_millis(20)).await);
    blocker.rollback().await.unwrap();
    drain(&sql_owner).await;
    assert_eq!(sql_gate.slots.available_permits(), 2);
    assert!(sql_owner.shutdown(Duration::from_secs(1)).await);
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
        .respond(Action::SeriesInfo, peer, request(series_path, token))
        .await;
    assert_eq!(response.status(), 503);
    drop(response);
    drain(&small).await;
    assert!(small.shutdown(Duration::from_secs(1)).await);
    std::fs::write(std::env::var("VOD_CACHED_PROVIDER_RESOURCES_REPORT").unwrap(),serde_json::to_vec_pretty(&json!({"passed":true,"workers":2,"memory_bytes":192*1024*1024,"shared_final_frame_clone":true,"retained_frame_credits":retained,"canceled_expired_waiter_no_worker":true,"actual_SQL_lock_abort_owns_68MiB_until_reply_rollback":true,"shutdown_waits_SQL_and_final_frame_remains_owned":true,"output_cap_503_not_partial200":true})).unwrap()).unwrap();
}
