//! Original enabled environment/IP worker with real HTTP, SQL and Django cache.
//! Original Dispatcharr authors retain full application credit.
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use dispatcharr_store::{AuthConfig, AuthService, IpLookupConfig, NotificationCache, Store};
use dispatcharr_upstream_http::{body, Attempt};
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[derive(Default)]
struct Sink(Mutex<Vec<Value>>);
impl UpdateEventSink for Sink {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            assert_eq!(group, "updates");
            self.0.lock().unwrap().push(message);
            Ok(())
        })
    }
}
async fn get(url: &str, token: Option<&str>) -> Value {
    let attempt = Attempt::new(Duration::from_secs(5), Duration::from_secs(5)).unwrap();
    let headers = token
        .map(|token| vec![("Authorization".into(), format!("Bearer {token}"))])
        .unwrap_or_default();
    let response = attempt
        .request(url, "", &headers)
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut stream = body(response).unwrap();
    let mut bytes = Vec::new();
    while let Some(value) = stream.next().await {
        bytes.extend(value.unwrap());
        assert!(bytes.len() < 1024 * 1024)
    }
    serde_json::from_slice(&bytes).unwrap()
}
fn hex(raw: &str) -> Vec<u8> {
    raw.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
#[tokio::test]
#[ignore = "requires original enabled-IP fixture, isolated SQL/Redis and private provider origin"]
async fn original_environment_cache_worker_errors_coalescing_and_reverification() {
    assert_eq!(
        std::env::var("IP_LOOKUP_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("IP_LOOKUP_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let origin = std::env::var("IP_LOOKUP_ORIGIN").unwrap();
    assert!(origin.starts_with("http://127.0.0.1:"));
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let sink = Arc::new(Sink::default());
    let auth = AuthService::new(
        store,
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(NotificationCache::connect(&redis_url).await.unwrap())
    .with_event_sink(sink.clone())
    .with_ip_lookup_config(IpLookupConfig {
        ipify_url: origin.clone() + "/ipify",
        geo_url_template: origin.clone() + "/geo/{ip}",
        fallback_geo_url: origin.clone() + "/fallback",
        local_probe: "127.0.0.1:80".parse().unwrap(),
    });
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let token=jsonwebtoken::encode(&jsonwebtoken::Header::default(),&json!({"token_type":"access","exp":now+3600,"iat":now,"jti":uuid::Uuid::new_v4().to_string(),"user_id":golden["user_id"]}),&jsonwebtoken::EncodingKey::from_secret(b"playback-fixture-only")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = auth.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let endpoint = format!("http://{address}/api/core/settings/env/");
    let old = std::env::var("DISPATCHARR_ENABLE_IP_LOOKUP").ok();
    for step in golden["steps"].as_array().unwrap() {
        get(
            &(origin.clone() + "/control?mode=" + step["mode"].as_str().unwrap()),
            None,
        )
        .await;
        sink.0.lock().unwrap().clear();
        let _: i64 = redis
            .del(&[
                ":1:dispatcharr:ip_lookup_result",
                ":1:dispatcharr:ip_lookup_lock",
                ":1:coresettings:group:system_settings",
            ])
            .await
            .unwrap();
        if let Some(encoded) = step["settings_hex"].as_str() {
            let _: () = redis
                .set_ex(":1:coresettings:group:system_settings", hex(encoded), 300)
                .await
                .unwrap();
        }
        if let Some(encoded) = step["seed_hex"].as_str() {
            let mut encoded = hex(encoded);
            let offset = step["verified_offset"].as_u64().unwrap() as usize;
            let value = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
                - if step["seed"] == "stale" { 120.0 } else { 0.0 };
            encoded[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
            let _: () = redis
                .set_ex(":1:dispatcharr:ip_lookup_result", encoded, 3600)
                .await
                .unwrap();
        }
        if step["lock_present"] == true {
            let _: () = redis
                .set_ex(
                    ":1:dispatcharr:ip_lookup_lock",
                    vec![0x80, 4, 0x88, b'.'],
                    30,
                )
                .await
                .unwrap();
        }
        std::env::set_var(
            "DISPATCHARR_ENABLE_IP_LOOKUP",
            if step["env_enabled"] == true {
                "true"
            } else {
                "false"
            },
        );
        let initial = get(&endpoint, Some(&token)).await;
        assert_eq!(
            initial, step["initial"],
            "source initial case {}",
            step["name"]
        );
        if !step["repeat"].is_null() {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            loop {
                let stats = get(&(origin.clone() + "/stats"), None).await;
                if !stats["calls"].as_array().unwrap().is_empty() {
                    break;
                }
                assert!(tokio::time::Instant::now() < deadline);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                get(&endpoint, Some(&token)).await,
                step["repeat"],
                "source coalesced request"
            );
            get(&(origin.clone() + "/release"), None).await;
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let cached = auth
                .websocket_cached_ip()
                .await
                .unwrap()
                .unwrap_or(Value::Null);
            let lock: bool = redis.exists(":1:dispatcharr:ip_lookup_lock").await.unwrap();
            let count = sink.0.lock().unwrap().len();
            let calls = get(&(origin.clone() + "/stats"), None).await;
            if cached == step["cached"]
                && lock == step["lock_remaining"]
                && count == step["published"].as_array().unwrap().len()
                && calls["calls"] == step["calls"]
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "source IP worker case {} did not converge",
                step["name"]
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            auth.system_settings().await.unwrap(),
            step["system_settings"],
            "source complete group-cache/default map"
        );
        assert_eq!(
            get(&endpoint, Some(&token)).await,
            step["after"],
            "source post-worker environment"
        );
        // An unchanged failed re-check must have advanced verified_at too:
        // a subsequent HTTP call must not start another private provider call.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(
            get(&(origin.clone() + "/stats"), None).await["calls"],
            step["calls"],
            "unexpected recheck/cookie/agent difference"
        );
        assert_eq!(
            *sink.0.lock().unwrap(),
            step["published"].as_array().unwrap().clone()
        );
        let ttl: i64 = redis.ttl(":1:dispatcharr:ip_lookup_result").await.unwrap();
        assert_eq!(ttl > 0, step["cache_ttl_positive"] == true);
        if ttl > 0 {
            assert!(ttl <= 3600)
        }
    }
    if let Some(old) = old {
        std::env::set_var("DISPATCHARR_ENABLE_IP_LOOKUP", old)
    } else {
        std::env::remove_var("DISPATCHARR_ENABLE_IP_LOOKUP")
    }
    server.abort();
    let _ = server.await;
}
