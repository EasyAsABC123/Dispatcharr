//! Actual Original-network corpus versus independent native TCP adapter.
//! No production app/React/performance claim. Original authors retain credit.
use super::super::{vod_api_http::VodReads, vod_api_owned_read::ReadLimits};
use super::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, Row};
use std::time::Duration;

fn headers(case: &Value, base: &Value) -> Vec<(String, String)> {
    let mut headers = case["headers"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().unwrap().to_owned()))
        .collect::<Vec<_>>();
    if let Some(index) = case["label"].get("presented_auth_case") {
        let raw = [
            "Bearer invalid",
            "Bearer",
            "Bearer a b",
            "ApiKey invalid",
            "Basic dGVzdDph",
            "",
        ][index.as_u64().unwrap() as usize];
        headers.push(("Authorization".into(), raw.into()));
    } else if let Some(role) = case["role"].as_str() {
        let actual = base["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["role"] == role && v["method"] == "GET" && v["path"] == "/api/vod/movies/")
            .unwrap();
        headers.push((
            "Authorization".into(),
            actual["headers"]["HTTP_AUTHORIZATION"]
                .as_str()
                .unwrap()
                .into(),
        ));
    }
    headers
}

#[tokio::test]
#[ignore = "Requires actual Original network VOD corpus, owned PostgreSQL/Redis"]
async fn actual_original_unified_http_auth_methods_json_and_errors() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_UNIFIED_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(source["observation"], "http");
    assert_eq!(source["passed"], true);
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let pool = PgPoolOptions::new()
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
    let ordinary = VodReads::new(auth.clone(), ReadLimits::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = ordinary.unified_router();
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
    let rows=sqlx::query("SELECT id::bigint AS id,rating,custom_properties FROM vod_movie WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1").fetch_one(&pool).await.unwrap();
    let movie: i64 = rows.try_get("id").unwrap();
    let rating: Option<String> = rows.try_get("rating").unwrap();
    let props: Value = rows.try_get("custom_properties").unwrap();
    let series=sqlx::query("SELECT id::bigint AS id,name FROM vod_series WHERE name LIKE 'rust-probe-vod-rest-%' ORDER BY id LIMIT 1").fetch_one(&pool).await.unwrap();
    let series_id: i64 = series.try_get("id").unwrap();
    let series_name: String = series.try_get("name").unwrap();
    let movie_name: String = sqlx::query_scalar("SELECT name FROM vod_movie WHERE id=$1")
        .bind(movie)
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut records = Vec::new();
    let mut differences = Vec::new();
    let mut exclusions = Vec::new();
    for (index, case) in source["records"].as_array().unwrap().iter().enumerate() {
        let path = case["path"].as_str().unwrap();
        let label = &case["label"];
        if case["headers"]["Accept"] == "text/html" {
            exclusions.push(json!({"source_index":index,"path":path,"reason":"HTML is excluded from this JSON comparison and checked separately by the56observation renderer peer"}));
            continue;
        }
        if let Some(value) = label.get("rating").or_else(|| label.get("rating_HEAD")) {
            sqlx::query("UPDATE vod_movie SET rating=$1 WHERE id=$2")
                .bind(value.as_str())
                .bind(movie)
                .execute(&pool)
                .await
                .unwrap();
        }
        if let Some(value) = label.get("properties") {
            sqlx::query("UPDATE vod_movie SET custom_properties=$1 WHERE id=$2")
                .bind(if value.is_null() { None } else { Some(value) })
                .bind(movie)
                .execute(&pool)
                .await
                .unwrap();
        }
        if label == "cross-kind LOWER(name),id tie" {
            sqlx::query("UPDATE vod_series SET name=$1 WHERE id=$2")
                .bind(&movie_name)
                .bind(series_id)
                .execute(&pool)
                .await
                .unwrap();
        }
        let (expected, binding) = if label == "cross-kind LOWER(name),id tie" {
            let (value, binding) =
                super::super::vod_api_unified_read::phase_tests::fresh_original_tie(
                    &pool, &source, case,
                )
                .await;
            (value, Some(binding))
        } else {
            (case["expected"].clone(), None)
        };
        let method = case["method"].as_str().unwrap();
        let mut request = client.request(
            reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
            format!("http://{address}{path}"),
        );
        for (name, value) in headers(case, &base) {
            request = request.header(name, value);
        }
        if !matches!(method, "GET" | "HEAD") {
            request = request.body("{}");
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let actual_headers = response.headers().clone();
        let bytes = response.bytes().await.unwrap();
        sqlx::query("UPDATE vod_movie SET rating=$1,custom_properties=$2 WHERE id=$3")
            .bind(&rating)
            .bind(&props)
            .bind(movie)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE vod_series SET name=$1 WHERE id=$2")
            .bind(&series_name)
            .bind(series_id)
            .execute(&pool)
            .await
            .unwrap();
        let hash = format!("{:x}", Sha256::digest(&bytes));
        let content = actual_headers
            .get("content-type")
            .and_then(|v| v.to_str().ok());
        let selected = ["Allow", "Vary", "WWW-Authenticate", "Content-Length"]
            .into_iter()
            .map(|name| {
                (
                    name.to_owned(),
                    actual_headers
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let mut expected_headers = case["expected"]["selected_headers"].clone();
        if binding.is_some() {
            expected_headers["Content-Length"] =
                expected["body_bytes"].as_u64().unwrap().to_string().into();
        }
        expected_headers.as_object_mut().unwrap().retain(|name, _| {
            ["Allow", "Vary", "WWW-Authenticate", "Content-Length"].contains(&name.as_str())
        });
        let same = status == expected["status"].as_u64().unwrap() as u16
            && expected["body_sha256"] == hash
            && case["expected"]["content_type"] == content.map(Value::from).unwrap_or(Value::Null)
            && expected_headers == Value::Object(selected.clone());
        let row = json!({"source_index":index,"path":path,"method":method,"role":case["role"],"status":status,"body_sha256":hash,"content_type":content,"selected_headers":selected,"same":same,"fresh_Source_tie_HTTP":binding});
        if !same {
            differences.push(json!({"case":row,"Source":case["expected"],"expected_authority":expected,"native_json":serde_json::from_slice::<Value>(&bytes).ok()}));
        }
        records.push(row);
    }
    let drained = ordinary.shutdown(Duration::from_secs(5)).await;
    server.abort();
    let _ = server.await;
    pool.close().await;
    let passed = differences.is_empty() && records.len() == 188 && exclusions.len() == 1 && drained;
    std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("unified-http.json"),serde_json::to_vec_pretty(&json!({"passed":passed,"scope":"Actual Original network vs independent nativeTCP shared cached-auth/gate adapter; global app security headers/React/production/performance excluded","compared":records.len(),"records":records,"differences":differences,"explicit_unsupported_not_parity_pass":exclusions,"workers_drained":drained})).unwrap()).unwrap();
    assert!(passed, "Unified HTTP differences retained");
}

#[tokio::test]
#[ignore = "Requires actual Original10-request mixed burst and owned Redis/PG fixture"]
async fn shared_movie_category_unified_queue_frames_and_cached_auth() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_UNIFIED_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let reference: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_UNIFIED_BURST_REFERENCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(reference["passed"], true);
    assert_eq!(reference["owned_rows_unchanged"], true);
    let case = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["role"] == "standard"
                && row["path"] == "/api/vod/all/"
                && row["method"] == "GET"
                && row["label"].is_null()
        })
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis_url.contains("playback-fixture-only"));
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool.clone()),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(crate::NotificationCache::connect(&redis_url).await.unwrap());
    let ordinary = VodReads::new(auth.clone(), ReadLimits::default()).unwrap();
    let gate = ordinary.shared_gate();
    let gateway = UnifiedHttp::shared(auth.clone(), ordinary.reader.shared_runtime(), gate.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = ordinary
        .router()
        .merge(ordinary.category_router())
        .merge(ordinary.unified_router());
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
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(11));
    let mut jobs = Vec::new();
    let actual_headers = headers(case, &base);
    let epoch = tokio::time::Instant::now();
    for index in 0..10 {
        let client = client.clone();
        let barrier = barrier.clone();
        let headers = actual_headers.clone();
        let path = ["/api/vod/all/", "/api/vod/movies/", "/api/vod/categories/"][index % 3];
        jobs.push(tokio::spawn(async move {
        let mut request=client.get(format!("http://{address}{path}"));for(name,value)in headers {request=request.header(name,value);}
        barrier.wait().await;let began=epoch.elapsed().as_secs_f64();let response=request.send().await.unwrap();let status=response.status().as_u16();let bytes=response.bytes().await.unwrap();json!({"index":index,"path":path,"status":status,"body_sha256":format!("{:x}",Sha256::digest(&bytes)),"started_seconds":began})
    }));
    }
    barrier.wait().await;
    let mut burst = Vec::new();
    for job in jobs {
        burst.push(job.await.unwrap());
    }
    let burst_pass = burst.iter().all(|row| {
        row["status"] == 200
            && reference["records"]
                .as_array()
                .unwrap()
                .iter()
                .find(|v| v["index"] == row["index"])
                .unwrap()["body_sha256"]
                == row["body_sha256"]
    });
    let request = |path: &str| {
        let mut request = axum::extract::Request::builder().uri(path);
        for (name, value) in &actual_headers {
            request = request.header(name, value);
        }
        request.body(Body::empty()).unwrap()
    };
    let peer = "127.0.0.1:30000".parse().unwrap();
    let all = gateway.respond(peer, false, request("/api/vod/all/")).await;
    assert_eq!(all.status(), 200);
    let all = to_bytes(all.into_body(), 16 * 1024 * 1024).await.unwrap();
    let movie = super::super::vod_api_http::respond(
        ordinary.reader.clone(),
        auth.clone(),
        gate.clone(),
        peer,
        super::super::vod_api_sql::Resource::Movie,
        false,
        request("/api/vod/movies/"),
    )
    .await;
    assert_eq!(movie.status(), 200);
    let movie = to_bytes(movie.into_body(), 16 * 1024 * 1024).await.unwrap();
    let clone = all.clone();
    drop(all);
    assert_eq!(gate.slots.available_permits(), 0);
    assert_eq!(ordinary.reader.physical_workers(), 0);
    let held_memory = ordinary.reader.retained_memory_bytes();
    assert!(held_memory > 0);
    let waiting_gateway = gateway.clone();
    let waiting_request = request("/api/vod/all/");
    let queued =
        tokio::spawn(async move { waiting_gateway.respond(peer, false, waiting_request).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!queued.is_finished());
    assert_eq!(gate.waiters.available_permits(), 255);
    assert_eq!(ordinary.reader.physical_workers(), 0);
    queued.abort();
    assert!(queued.await.unwrap_err().is_cancelled());
    assert_eq!(gate.waiters.available_permits(), 256);
    assert_eq!(gate.slots.available_permits(), 0);
    let waiting_gateway = gateway.clone();
    let waiting_request = request("/api/vod/all/");
    let queued =
        tokio::spawn(async move { waiting_gateway.respond(peer, false, waiting_request).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!queued.is_finished());
    drop(clone);
    let resumed = queued.await.unwrap();
    assert_eq!(resumed.status(), 200);
    drop(
        to_bytes(resumed.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap(),
    );
    drop(movie);
    assert_eq!(gate.slots.available_permits(), 2);

    // A real cached-auth reply owns the same HTTP slot and read work after
    // caller cancellation. No new limiter or authentication implementation.
    let mut redis = redis::Client::open(redis_url)
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
    let cancel_gateway = gateway.clone();
    let cancel_request = request("/api/vod/all/");
    let cancelled =
        tokio::spawn(async move { cancel_gateway.respond(peer, false, cancel_request).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while ordinary.reader.physical_workers() != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    cancelled.abort();
    let _ = cancelled.await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let physical_after_abort = ordinary.reader.physical_workers();
    let slots_after_abort = gate.slots.available_permits();
    let auth_memory = ordinary.reader.retained_memory_bytes();
    assert_eq!(physical_after_abort, 1);
    assert_eq!(slots_after_abort, 1);
    tokio::time::timeout(Duration::from_secs(3), async {
        while ordinary.reader.physical_workers() != 0 || gate.slots.available_permits() != 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    // Controlled100ms queued deadline: held401 final frames consume both
    // slots, while no read/auth worker is running. Expiry cannot start auth.
    let short_limits = ReadLimits {
        logical_deadline: Duration::from_millis(100),
        ..ReadLimits::default()
    };
    let short_gate = HttpGate::new(short_limits).unwrap();
    let short_runtime = ReadRuntime::new(short_limits).unwrap();
    let short = UnifiedHttp::shared(auth.clone(), short_runtime, short_gate.clone());
    let bad = || {
        axum::extract::Request::builder()
            .uri("/api/vod/all/")
            .header("authorization", "Bearer invalid")
            .body(Body::empty())
            .unwrap()
    };
    let bad_one = short.respond(peer, false, bad()).await;
    assert_eq!(bad_one.status(), 401);
    let bad_one = to_bytes(bad_one.into_body(), 65536).await.unwrap();
    let bad_two = short.respond(peer, false, bad()).await;
    assert_eq!(bad_two.status(), 401);
    let bad_two = to_bytes(bad_two.into_body(), 65536).await.unwrap();
    assert_eq!(short_gate.slots.available_permits(), 0);
    assert_eq!(short.reader.runtime.physical_workers(), 0);
    let expired = short.respond(peer, false, request("/api/vod/all/")).await;
    assert_eq!(expired.status(), 503);
    drop(to_bytes(expired.into_body(), 65536).await.unwrap());
    assert_eq!(short.reader.runtime.physical_workers(), 0);
    assert_eq!(short_gate.waiters.available_permits(), 256);
    short_gate.slots.close();
    assert!(short.reader.runtime.shutdown(Duration::from_secs(1)).await);
    drop(bad_one);
    drop(bad_two);
    assert_eq!(short_gate.slots.available_permits(), 2);
    let response = gateway.respond(peer, false, request("/api/vod/all/")).await;
    let retained = to_bytes(response.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    let retained_clone = retained.clone();
    drop(retained);
    let second = gateway.respond(peer, false, request("/api/vod/all/")).await;
    let second = to_bytes(second.into_body(), 16 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(gate.slots.available_permits(), 0);
    let closing_gateway = gateway.clone();
    let closing_request = request("/api/vod/all/");
    let closing_waiter =
        tokio::spawn(async move { closing_gateway.respond(peer, false, closing_request).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!closing_waiter.is_finished());
    assert_eq!(ordinary.reader.physical_workers(), 0);
    let memory_before_shutdown = ordinary.reader.retained_memory_bytes();
    let drained = ordinary.shutdown(Duration::from_secs(3)).await;
    assert!(drained);
    let closed = closing_waiter.await.unwrap();
    assert_eq!(closed.status(), 503);
    drop(to_bytes(closed.into_body(), 65536).await.unwrap());
    assert_eq!(
        ordinary.reader.retained_memory_bytes(),
        memory_before_shutdown
    );
    drop(retained_clone);
    drop(second);
    assert_eq!(ordinary.reader.retained_memory_bytes(), 0);
    server.abort();
    let _ = server.await;
    pool.close().await;
    let passed = burst_pass && drained;
    std::fs::write(std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap()).join("unified-http-shared.json"),serde_json::to_vec_pretty(&json!({"passed":passed,"scope":"Actual Original/native mixed10 unstaggered requests, shared2workers/192MiB, finalframes, canceledqueue, cached-auth physicalreply and sharedshutdown; no performance or production acceptance","Original":reference,"native_burst":burst,"shared_workers":2,"shared_memory_limit":ReadLimits::default().memory_bytes,"two_family_frames_memory":held_memory,"queued_cancel_no_SQL":true,"controlled_queued100ms_expiry_no_auth_or_SQL":true,"shutdown_releases_queued_request_without_SQL":true,"final_All_clone_unblocks_queued_All":true,"cached_auth_physical_workers_after_abort":physical_after_abort,"cached_auth_slots_available_after_abort":slots_after_abort,"cached_auth_memory_held":auth_memory,"held_frame_memory_survives_shutdown":memory_before_shutdown,"shared_shutdown":drained,"all_memory_released":true})).unwrap()).unwrap();
    assert!(passed);
}

#[tokio::test]
#[ignore="Requires actual Original HTML56observations, owned PostgreSQL/Redis"]
async fn actual_original_browsable_html_cookies_heads_and_roles(){
    super::super::vod_api_unified_html::compare_original_network().await;
}

#[tokio::test]
#[ignore = "Requires owned Source priority matrix and actual PostgreSQL/Redis"]
async fn actual_original_auth_priority_network_matrix() {
    assert_eq!(std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(), Ok("playback-fixture-only"));
    let evidence=std::path::PathBuf::from(std::env::var("VOD_NATIVE_EVIDENCE").unwrap());
    let source:Value=serde_json::from_slice(&std::fs::read(evidence.join("shared-auth-priority-source.json")).unwrap()).unwrap();
    let base:Value=serde_json::from_slice(&std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap()).unwrap();
    let url=std::env::var("DATABASE_URL").unwrap();assert!(url.contains("playback-fixture-only"));
    let pool=PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();
    let redis_url=std::env::var("REDIS_TEST_URL").unwrap();assert!(redis_url.contains("playback-fixture-only"));
    let cache=crate::NotificationCache::connect(&redis_url).await.unwrap();
    let auth=crate::AuthService::new(crate::Store::from_pool(pool.clone()),crate::AuthConfig::new("playback-fixture-only".into()).unwrap()).with_notification_cache(cache.clone());
    let ordinary=VodReads::new(auth.clone(),ReadLimits::default()).unwrap();
    let router=auth.clone().router().merge(ordinary.router()).merge(ordinary.category_router()).merge(ordinary.unified_router());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
    let server=tokio::spawn(async move { axum::serve(listener,router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap(); });
    let client=reqwest::Client::builder().no_proxy().http1_only().timeout(Duration::from_secs(8)).build().unwrap();
    let mut connection=redis::Client::open(redis_url).unwrap().get_multiplexed_async_connection().await.unwrap();
    const KEY:&str=":1:coresettings:group:network_access";
    let saved:Option<Vec<u8>>=redis::cmd("GET").arg(KEY).query_async(&mut connection).await.unwrap();
    let ttl:i64=redis::cmd("PTTL").arg(KEY).query_async(&mut connection).await.unwrap();let started=std::time::Instant::now();
    let mut users=std::collections::BTreeMap::new();
    for role in ["basic","standard","admin"] {
        let row=sqlx::query("SELECT id::bigint AS id,custom_properties FROM accounts_user WHERE username=$1").bind(format!("rust-probe-vod-rest-{role}")).fetch_one(&pool).await.unwrap();
        users.insert(role.to_owned(),(row.get::<i64,_>("id"),row.get::<Value,_>("custom_properties")));
    }
    let mut records=Vec::new();let mut differences=Vec::new();let mut invalid_peer_source_only=0;
    for (index,case) in source["records"].as_array().unwrap().iter().enumerate() {
        if case["global_profile"]=="invalid-REMOTE_ADDR" {
            assert_eq!(case["expected"]["status"],403);assert!("not-an-ip".parse::<std::net::IpAddr>().is_err());invalid_peer_source_only+=1;continue;
        }
        let role=case["role"].as_str().unwrap();let (id,_) = &users[role];
        sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2").bind(&case["custom_properties"]).bind(id).execute(&pool).await.unwrap();
        cache.set_for(KEY,&json!({"UI":if case["global_profile"]=="deny" {"192.0.2.0/24"}else{"0.0.0.0/0,::/0"}}),120).await.unwrap();
        let token=base["cases"].as_array().unwrap().iter().find(|row|row["role"]==role&&row["path"]=="/api/vod/movies/"&&row["method"]=="GET").unwrap()["headers"]["HTTP_AUTHORIZATION"].as_str().unwrap();
        let mut request=client.get(format!("http://{address}{}",case["path"].as_str().unwrap())).header("Host","localhost").header("Accept","application/json").header("Authorization",token);
        if case["global_profile"]=="invalid-forwarded" {request=request.header("X-Real-IP","not-an-ip").header("X-Forwarded-For","not-an-ip");}
        let response=request.send().await.unwrap();let status=response.status().as_u16();let headers=response.headers().clone();let body=response.bytes().await.unwrap();
        let expected=&case["expected"];let content_type=headers.get("content-type").and_then(|v|v.to_str().ok());let hash=format!("{:x}",Sha256::digest(&body));
        let same=status==expected["status"].as_u64().unwrap()as u16&&content_type==expected["content_type"].as_str()&&hash==expected["body_sha256"].as_str().unwrap();
        let fault_headers=if status==500 {headers.get("allow").is_none()&&headers.get("www-authenticate").is_none()&&headers.get("vary").and_then(|v|v.to_str().ok())==Some("origin")}else{true};
        let record=json!({"index":index,"role":role,"profile":case["global_profile"],"path":case["path"],"status":status,"body_sha256":hash,"same_status_body_MIME":same,"generic500_selected_headers":fault_headers});
        if !same||!fault_headers {differences.push(record.clone());}records.push(record);
    }
    // Cross-prototype authority: pre-view UI faults must bypass the new
    // documenting renderer, retain generic145bytes and never create CSRF.
    let mut html_pre_view_faults=Vec::new();
    for role in ["basic","standard","admin"] {
        let(id,_)=&users[role];sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2").bind(json!(true)).bind(id).execute(&pool).await.unwrap();
        cache.set_for(KEY,&json!({"UI":"0.0.0.0/0,::/0"}),120).await.unwrap();
        let token=base["cases"].as_array().unwrap().iter().find(|row|row["role"]==role&&row["path"]=="/api/vod/movies/"&&row["method"]=="GET").unwrap()["headers"]["HTTP_AUTHORIZATION"].as_str().unwrap();
        let mut observations=Vec::new();for origin in [std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap(),format!("http://{address}")] {
            let response=client.get(format!("{origin}/api/vod/all/")).header("Host","localhost").header("Accept","text/html").header("Authorization",token).send().await.unwrap();let status=response.status().as_u16();let h=response.headers().clone();let body=response.bytes().await.unwrap();
            observations.push(json!({"status":status,"body_bytes":body.len(),"body_sha256":format!("{:x}",Sha256::digest(&body)),"MIME":h.get("content-type").and_then(|v|v.to_str().ok()),"Vary":h.get("vary").and_then(|v|v.to_str().ok()),"Allow":h.get("allow").and_then(|v|v.to_str().ok()),"WWW":h.get("www-authenticate").and_then(|v|v.to_str().ok()),"SetCookie":h.get("set-cookie").is_some()}));
        }
        let exact=observations[0]==observations[1]&&observations[0]["status"]==500&&observations[0]["body_bytes"]==145&&observations[0]["SetCookie"]==false;
        html_pre_view_faults.push(json!({"role":role,"exact_Source_native":exact,"Source":observations[0],"native":observations[1]}));
    }
    let mut category_controls=Vec::new();
    for case in source["records"].as_array().unwrap().iter().filter(|case|case["path"]=="/api/vod/movies/"&&case["global_profile"]!="invalid-REMOTE_ADDR") {
        let role=case["role"].as_str().unwrap();let(id,_)=&users[role];
        sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2").bind(&case["custom_properties"]).bind(id).execute(&pool).await.unwrap();
        cache.set_for(KEY,&json!({"UI":if case["global_profile"]=="deny"{"192.0.2.0/24"}else{"0.0.0.0/0,::/0"}}),120).await.unwrap();
        let token=base["cases"].as_array().unwrap().iter().find(|row|row["role"]==role&&row["path"]=="/api/vod/movies/"&&row["method"]=="GET").unwrap()["headers"]["HTTP_AUTHORIZATION"].as_str().unwrap();
        let original_base=std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap();let mut pair=Vec::new();
        for origin in [original_base,format!("http://{address}")] {
            let mut request=client.get(format!("{origin}/api/vod/categories/")).header("Host","localhost").header("Accept","application/json").header("Authorization",token);
            if case["global_profile"]=="invalid-forwarded"{request=request.header("X-Real-IP","not-an-ip").header("X-Forwarded-For","not-an-ip");}
            let response=request.send().await.unwrap();let status=response.status().as_u16();let headers=response.headers().clone();let body=response.bytes().await.unwrap();
            pair.push(json!({"status":status,"MIME":headers.get("content-type").and_then(|v|v.to_str().ok()),"body_sha256":format!("{:x}",Sha256::digest(&body))}));
        }
        category_controls.push(json!({"role":role,"profile":case["global_profile"],"custom_properties":case["custom_properties"],"exact_status_body_MIME":pair[0]==pair[1],"Source":pair[0],"native":pair[1]}));
    }
    for (id,props) in users.values(){sqlx::query("UPDATE accounts_user SET custom_properties=$1 WHERE id=$2").bind(props).bind(id).execute(&pool).await.unwrap();}
    match &saved {
        Some(raw)=>{let mut cmd=redis::cmd("SET");cmd.arg(KEY).arg(raw);if ttl>=0 {cmd.arg("PX").arg(ttl.saturating_sub(started.elapsed().as_millis()as i64).max(1));}cmd.query_async::<()>(&mut connection).await.unwrap();},
        None=>{redis::cmd("DEL").arg(KEY).query_async::<i64>(&mut connection).await.unwrap();}
    }
    // Independent existing serializer bug: preserve first raw12 failures,
    // then compare nonempty Source/native events on the SAME owned SQL rows.
    let sequence=sqlx::query("SELECT last_value,is_called FROM core_systemevent_id_seq").fetch_one(&pool).await.unwrap();
    let event:i64=sqlx::query_scalar("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) VALUES('login_success','2026-11-10 12:34:56.123456+00',NULL,'rust-probe-vod-rest-auth-priority',$1) RETURNING id::bigint").bind(json!({"fixture":"auth-priority","number":7})).fetch_one(&pool).await.unwrap();
    cache.set_for(KEY,&json!({"UI":"0.0.0.0/0,::/0"}),120).await.unwrap();
    let admin=base["cases"].as_array().unwrap().iter().find(|row|row["role"]=="admin"&&row["path"]=="/api/vod/movies/"&&row["method"]=="GET").unwrap()["headers"]["HTTP_AUTHORIZATION"].as_str().unwrap();
    let mut events_controls=Vec::new();
    for query in ["limit=1","limit=1&offset=1","event_type=login_success","limit=0","limit=5000","event_type=fixture-no-events"] {
        let path=format!("/api/core/system-events/?{query}");let original_base=std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap();
        let original=client.get(format!("{original_base}{path}")).header("Host","localhost").header("Accept","application/json").header("Authorization",admin).send().await.unwrap();let original_status=original.status();let original_body=original.bytes().await.unwrap();
        let native=client.get(format!("http://{address}{path}")).header("Host","localhost").header("Accept","application/json").header("Authorization",admin).send().await.unwrap();let native_status=native.status();let native_body=native.bytes().await.unwrap();
        let same=original_status==native_status&&original_body==native_body;
        events_controls.push(json!({"query":query,"Source_status":original_status.as_u16(),"native_status":native_status.as_u16(),"exact_raw_body":same,"Source_body_sha256":format!("{:x}",Sha256::digest(&original_body)),"native_body_sha256":format!("{:x}",Sha256::digest(&native_body))}));
    }
    sqlx::query("DELETE FROM core_systemevent WHERE id=$1").bind(event).execute(&pool).await.unwrap();
    sqlx::query("SELECT setval('core_systemevent_id_seq',$1,$2)").bind(sequence.get::<i64,_>("last_value")).bind(sequence.get::<bool,_>("is_called")).execute(&pool).await.unwrap();
    match &saved {Some(raw)=>{let mut cmd=redis::cmd("SET");cmd.arg(KEY).arg(raw);if ttl>=0{cmd.arg("PX").arg(ttl.saturating_sub(started.elapsed().as_millis()as i64).max(1));}cmd.query_async::<()>(&mut connection).await.unwrap();},None=>{redis::cmd("DEL").arg(KEY).query_async::<i64>(&mut connection).await.unwrap();}}
    assert!(ordinary.shutdown(Duration::from_secs(2)).await);server.abort();pool.close().await;
    let event_pass=events_controls.iter().all(|value|value["exact_raw_body"]==true)&&category_controls.iter().all(|value|value["exact_status_body_MIME"]==true)&&html_pre_view_faults.iter().all(|value|value["exact_Source_native"]==true);
    let report=json!({"passed":differences.is_empty()&&event_pass,"SystemEvents_nonempty_same_DB_exact_raw_controls":events_controls,"cross_prototype_HTML_pre_view_faults":html_pre_view_faults,"additional_Category_Source_native_TCP_controls":category_controls,"scope":"297 actual Original/native TCP status/body/MIME +generic500 selectedheaders.99 invalid REMOTE_ADDR remain Source Factory authority plus native parser rejection, NOT nativeHTTP invalidsocket parity","compared_real_HTTP":records.len(),"invalid_peer_Source_Factory_only":invalid_peer_source_only,"records":records,"differences":differences,"owned_user_properties_and_cache_restored":true});
    std::fs::write(evidence.join("shared-ui-auth-priority-native.json"),serde_json::to_vec_pretty(&report).unwrap()).unwrap();assert_eq!(report["compared_real_HTTP"],297);assert_eq!(invalid_peer_source_only,99);assert_eq!(report["passed"],true,"shared UI permission matrix mismatch");
}
