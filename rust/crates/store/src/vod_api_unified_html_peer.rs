//! Independent Source TCP HTML peer. Only the identified64char CSRF slot is
//! excluded from literal byte equality after both sides' mask/cookie validation.
use super::super::{vod_api_http::VodReads, vod_api_owned_read::ReadLimits};
use super::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, time::Duration};
fn token_span(body: &str) -> Option<(usize, usize)> {
    const PREFIX: &str = "\"csrfToken\": \"";
    if body.matches(PREFIX).count() != 1 {
        return None;
    }
    let start = body.find(PREFIX)? + PREFIX.len();
    let end = start + 64;
    if body.get(end..end + 1)? != "\"" || indices(body.get(start..end)?).is_none() {
        return None;
    }
    Some((start, end))
}
fn cookie_fields(raw: &str) -> Option<(String, std::collections::BTreeMap<String, String>)> {
    let mut pieces = raw.split(';');
    let (name, secret) = pieces.next()?.trim().split_once('=')?;
    if name != "csrftoken" || secret.len() != 32 || indices(secret).is_none() {
        return None;
    }
    let mut attrs = std::collections::BTreeMap::new();
    for piece in pieces {
        let (key, value) = piece.trim().split_once('=')?;
        if attrs
            .insert(key.to_ascii_lowercase(), value.to_owned())
            .is_some()
        {
            return None;
        }
    }
    Some((secret.to_owned(), attrs))
}
pub(super) async fn compare_original_network() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let evidence = std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap());
    let reference: Value =
        serde_json::from_slice(&std::fs::read(evidence.join("unified-html-source.json")).unwrap())
            .unwrap();
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis.contains("playback-fixture-only"));
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool.clone()),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(crate::NotificationCache::connect(&redis).await.unwrap());
    let gateway = VodReads::new(auth.clone(), ReadLimits::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = gateway.unified_router();
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
        .http1_only()
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let mut records = Vec::new();
    let mut differences = Vec::new();
    for (index, case) in reference["records"].as_array().unwrap().iter().enumerate() {
        let method = case["method"].as_str().unwrap();
        let mut request = client
            .request(
                reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                format!("http://{address}{}", case["path"].as_str().unwrap()),
            )
            .header("Host", "localhost")
            .header("Accept", case["accept"].as_str().unwrap());
        if let Some(role) = case["role"].as_str() {
            let token = source["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| {
                    v["role"] == role && v["path"] == "/api/vod/movies/" && v["method"] == "GET"
                })
                .unwrap()["headers"]["HTTP_AUTHORIZATION"]
                .as_str()
                .unwrap();
            request = request.header("Authorization", token);
        }
        if let Some(cookie) = case["request_cookie"].as_str().filter(|v| !v.is_empty()) {
            request = request.header("Cookie", cookie);
        }
        let before = chrono::Utc::now();
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = response.bytes().await.unwrap();
        let after = chrono::Utc::now();
        let source_headers = case["raw_response_headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item[0].as_str().unwrap().to_ascii_lowercase(),
                    item[1].as_str().unwrap().to_owned(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let same_headers = [
            "content-type",
            "content-length",
            "allow",
            "vary",
            "www-authenticate",
        ]
        .iter()
        .all(|name| {
            headers.get(*name).and_then(|v| v.to_str().ok())
                == source_headers.get(*name).map(String::as_str)
        });
        let mut body_equal = false;
        let mut csrf_consistent = true;
        let mut expiry_consistent = true;
        let mut cookie_attributes = true;
        let mut declared_random_slots = 0;
        let is_html = case["content_type"]
            .as_str()
            .unwrap()
            .starts_with("text/html");
        if is_html {
            let source_cookie = cookie_fields(source_headers.get("set-cookie").unwrap()).unwrap();
            let native_cookie = headers
                .get("set-cookie")
                .and_then(|v| v.to_str().ok())
                .and_then(cookie_fields);
            if let Some((secret, attrs)) = native_cookie {
                cookie_attributes = attrs.keys().map(String::as_str).collect::<Vec<_>>()
                    == ["expires", "max-age", "path", "samesite"]
                    && attrs.get("max-age").map(String::as_str) == Some("31449600")
                    && attrs.get("path").map(String::as_str) == Some("/")
                    && attrs.get("samesite").map(String::as_str) == Some("Lax")
                    && headers.get_all("set-cookie").iter().count() == 1;
                expiry_consistent = attrs
                    .get("expires")
                    .and_then(|date| {
                        chrono::NaiveDateTime::parse_from_str(date, "%a, %d %b %Y %H:%M:%S GMT")
                            .ok()
                    })
                    .is_some_and(|date| {
                        let expiry = date.and_utc();
                        expiry >= before + chrono::Duration::seconds(31_449_599)
                            && expiry <= after + chrono::Duration::seconds(31_449_600)
                    });
                if let Some(raw) = case["request_cookie"].as_str().filter(|v| !v.is_empty()) {
                    csrf_consistent &= raw
                        .split(';')
                        .filter_map(|v| v.trim().split_once('='))
                        .any(|(name, value)| name == "csrftoken" && value == secret);
                }
                if method == "HEAD" {
                    body_equal = bytes.is_empty();
                } else if let Ok(native) = std::str::from_utf8(&bytes) {
                    let original = case["body_text"].as_str().unwrap();
                    if let (Some((a, b)), Some((c, d))) = (token_span(original), token_span(native))
                    {
                        csrf_consistent &= unmask(&original[a..b]).as_deref()
                            == Some(source_cookie.0.as_str())
                            && unmask(&native[c..d]).as_deref() == Some(secret.as_str());
                        body_equal = original[..a] == native[..c] && original[b..] == native[d..];
                        declared_random_slots = 1;
                    }
                }
            } else {
                csrf_consistent = false;
            }
        } else {
            body_equal =
                format!("{:x}", Sha256::digest(&bytes)) == case["body_sha256"].as_str().unwrap();
            csrf_consistent = headers.get("set-cookie").is_none();
        }
        let same = status == case["status"].as_u64().unwrap() as u16
            && same_headers
            && body_equal
            && csrf_consistent
            && expiry_consistent
            && cookie_attributes;
        let record = json!({"index":index,"spec":case["spec"],"method":method,"repeat":case["repeat"],"role":case["role"],"status":status,"same_intrinsic_headers":same_headers,"exact_body_outside_only_validated_CSRF_slot":body_equal,"declared_independent_random_body_slots":declared_random_slots,"cookie_mask_internal_consistency":csrf_consistent,"expiry_exact_MaxAge_window":expiry_consistent,"cookie_attributes":cookie_attributes,"same":same,"native_body_sha256":format!("{:x}",Sha256::digest(&bytes))});
        if !same {
            differences.push(record.clone());
        }
        records.push(record);
    }
    let token = source["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| {
            case["role"] == "standard"
                && case["path"] == "/api/vod/movies/"
                && case["method"] == "GET"
        })
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap()
        .to_owned();
    let make_request = || {
        axum::extract::Request::builder()
            .uri("/api/vod/all/")
            .header("Host", "localhost")
            .header("Accept", "text/html")
            .header("Authorization", &token)
            .body(axum::body::Body::empty())
            .unwrap()
    };
    let state = super::super::vod_api_unified_http::UnifiedHttp::shared(
        auth.clone(),
        gateway.reader.shared_runtime(),
        gateway.shared_gate(),
    );
    let mut connection = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(400)
        .arg("ALL")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let waiting = state.clone();
    let request = make_request();
    let task = tokio::spawn(async move {
        waiting
            .respond("127.0.0.1:30000".parse().unwrap(), false, request)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while gateway.reader.shared_runtime().physical_workers() != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(gateway.reader.shared_runtime().physical_workers(), 1);
    assert_eq!(gateway.shared_gate().slots.available_permits(), 1);
    let physical_memory = gateway.reader.shared_runtime().retained_memory_bytes();
    assert!(physical_memory >= 68 * 1024 * 1024);
    tokio::time::timeout(Duration::from_secs(3), async {
        while gateway.reader.shared_runtime().physical_workers() != 0
            || gateway.shared_gate().slots.available_permits() != 2
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let small = VodReads::new(
        auth.clone(),
        ReadLimits {
            response_bytes: 24 * 1024,
            ..ReadLimits::default()
        },
    )
    .unwrap();
    let small_state = super::super::vod_api_unified_http::UnifiedHttp::shared(
        auth,
        small.reader.shared_runtime(),
        small.shared_gate(),
    );
    let response = small_state
        .respond("127.0.0.1:30000".parse().unwrap(), false, make_request())
        .await;
    assert_eq!(response.status(), 503);
    let capacity_body = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    assert!(!capacity_body.starts_with(b"<!DOCTYPE"));
    drop(capacity_body);
    assert!(small.shutdown(Duration::from_secs(2)).await);
    assert_eq!(small.reader.shared_runtime().retained_memory_bytes(), 0);
    let response = state
        .respond("127.0.0.1:30000".parse().unwrap(), false, make_request())
        .await;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["content-type"],
        "text/html; charset=utf-8"
    );
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(gateway.shared_gate().slots.available_permits(), 1);
    let clone = body.clone();
    drop(body);
    assert_eq!(gateway.shared_gate().slots.available_permits(), 1);
    let frame_memory = gateway.reader.shared_runtime().retained_memory_bytes();
    assert!(frame_memory >= clone.len());
    assert!(gateway.shutdown(Duration::from_secs(2)).await);
    assert!(gateway.reader.shared_runtime().retained_memory_bytes() > 0);
    drop(clone);
    assert_eq!(gateway.reader.shared_runtime().retained_memory_bytes(), 0);
    server.abort();
    pool.close().await;
    let report = json!({"passed":differences.is_empty(),"HTML_physical_guards":{"Redis400ms_reply_retains_68MiB_and_HTTPslot_after_caller_abort":true,"physical_retained_memory_bytes":physical_memory,"24KiB_response_cap_returns_HTTP503_not_partial_HTML200":true,"final_HTML_frame_clone_retains_HTTPslot_and_memory_through_shutdown":true,"held_frame_memory_bytes":frame_memory,"all_memory_released":true},"scope":"Actual Source/nativeTCP56GET/HEAD, Accept/format/role/cookie repeats. Exact deterministic bytes and5intrinsic headers; only single independently random CSRF64char field excluded after both cookies/masks validate. No wholeHTML normalization. Globalapphardening/assets/sessionauth and unknowncontext/linkification not acceptance.","compared":records.len(),"records":records,"differences":differences,"assets_served":false,"session_auth_added":false});
    std::fs::write(
        evidence.join("unified-html-native.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(report["passed"], true, "actual HTML peer mismatch");
}
