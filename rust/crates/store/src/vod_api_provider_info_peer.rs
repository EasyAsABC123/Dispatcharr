//! Source-derived cached provider-info extension and host controls.
//! Actual Source90 TCP and owned-resource gates passed; canonical app,
//! provider refresh, image serving and full React modal gates are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_http::VodReads;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{net::SocketAddr, time::Duration};
async fn fingerprints(pool: &PgPool, tables: &Value) -> Value {
    let mut result = serde_json::Map::new();
    for table in tables.as_object().unwrap().keys() {
        assert!(table
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_'));
        let sql = format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb)::text FROM {table} t"
        );
        let text: String = sqlx::query_scalar(&sql)
            .persistent(false)
            .fetch_one(pool)
            .await
            .unwrap();
        result.insert(
            table.clone(),
            format!("{:x}", Sha256::digest(text.as_bytes())).into(),
        );
    }
    result.into()
}
async fn restore(pool: &PgPool, metadata: &Value) {
    let mut tx = pool.begin().await.unwrap();
    for (table, rows) in [
        ("vod_movie", vec![metadata["movie"].clone()]),
        ("vod_series", vec![metadata["series"].clone()]),
        (
            "vod_m3umovierelation",
            metadata["movie_relations"].as_array().unwrap().clone(),
        ),
        (
            "vod_m3useriesrelation",
            vec![metadata["series_relation"].clone()],
        ),
    ] {
        for row in rows {
            let fields: Vec<_> = row
                .as_object()
                .unwrap()
                .keys()
                .map(|field| {
                    assert!(field
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_'));
                    format!("\"{field}\"")
                })
                .collect();
            let fields = fields.join(",");
            sqlx::query(&format!("UPDATE {table} t SET ({fields})=(SELECT {fields} FROM json_populate_record(NULL::{table},$1::json)) WHERE id=$2")).persistent(false).bind(row.to_string()).bind(row["id"].as_i64().unwrap()).execute(&mut *tx).await.unwrap();
        }
    }
    tx.commit().await.unwrap();
}
#[tokio::test]
#[ignore = "Requires disposable actual Original cached-info PostgreSQL/Redis fixture"]
async fn actual_source90_cached_info() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_CACHED_PROVIDER_INFO_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(source["passed"], true);
    assert_eq!(source["records"].as_array().unwrap().len(), 90);
    assert_eq!(source["provider_trap_attempts"], json!([]));
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool.clone()),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    );
    let ordinary = VodReads::new(auth, Default::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = ordinary.provider_router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap();
    let initial_sql = fingerprints(&pool, &source["full18_exact_microseconds_before"]).await;
    let mut differences = Vec::new();
    let mut observations = Vec::new();
    for (index, case) in source["records"].as_array().unwrap().iter().enumerate() {
        restore(&pool, &case["private_pre_request_metadata"]).await;
        let before = fingerprints(&pool, &source["full18_exact_microseconds_before"]).await;
        let path = case["path"].as_str().unwrap();
        let mut request = client
            .request(
                case["method"].as_str().unwrap().parse().unwrap(),
                format!("http://{address}{path}"),
            )
            .header("Host", "localhost")
            .header("Accept", case["accept"].as_str().unwrap());
        if let Some(role) = case["role"].as_str() {
            let token = base["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| {
                    r["role"] == role
                        && r["method"] == "GET"
                        && r["headers"]["HTTP_AUTHORIZATION"].is_string()
                })
                .unwrap()["headers"]["HTTP_AUTHORIZATION"]
                .as_str()
                .unwrap();
            request = request.header("Authorization", token);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.unwrap();
        let mut deltas = Vec::new();
        if case["expected"]["status"] != status {
            deltas.push("status".to_owned());
        }
        if case["expected"]["body_text"].as_str().unwrap().as_bytes() != body.as_ref() {
            deltas.push("body".to_owned());
        }
        for key in [
            "Content-Type",
            "Content-Length",
            "Allow",
            "Vary",
            "WWW-Authenticate",
        ] {
            let expected = case["expected"]["headers"][key].as_str();
            let actual = headers.get(key).map(|v| v.to_str().unwrap());
            if actual != expected {
                deltas.push(format!("header:{key}"));
            }
        }
        assert_eq!(
            before,
            fingerprints(&pool, &source["full18_exact_microseconds_before"]).await,
            "request {index} changed SQL state"
        );
        let observed = json!({"index":index,"actual_request_path":path,"status":status,"headers":headers.iter().map(|(k,v)|(k.to_string(),Value::from(v.to_str().unwrap()))).collect::<serde_json::Map<_,_>>(),"body_text":String::from_utf8(body.to_vec()).unwrap(),"body_sha256":format!("{:x}",Sha256::digest(&body)),"differences":deltas});
        if !deltas.is_empty() {
            differences.push(observed.clone());
        }
        observations.push(observed);
    }
    restore(&pool, &source["records"][0]["private_pre_request_metadata"]).await;
    assert_eq!(
        initial_sql,
        fingerprints(&pool, &source["full18_exact_microseconds_before"]).await
    );
    let mut burst_reports = Vec::new();
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
        .unwrap()
        .to_owned();
    let selected: Vec<_> = source["records"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| {
            r["role"] == "standard"
                && r["method"] == "GET"
                && r["label"] == "baseline"
                && r["query"] == ""
        })
        .cloned()
        .collect();
    assert_eq!(selected.len(), 2);
    for origin in [
        std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap(),
        format!("http://{address}"),
    ] {
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(11));
        let mut jobs = Vec::new();
        for index in 0..10 {
            let barrier = barrier.clone();
            let client = client.clone();
            let token = token.clone();
            let case = selected[index % 2].clone();
            let url = format!("{origin}{}", case["path"].as_str().unwrap());
            jobs.push(tokio::spawn(async move{barrier.wait().await;let response=client.get(url).header("Host","localhost").header("Accept","application/json").header("Authorization",token).send().await.unwrap();assert_eq!(response.status().as_u16(),200);let bytes=response.bytes().await.unwrap();assert_eq!(bytes.as_ref(),case["expected"]["body_text"].as_str().unwrap().as_bytes());json!({"index":index,"status":200,"body_sha256":format!("{:x}",Sha256::digest(bytes))})}));
        }
        barrier.wait().await;
        let mut rows = Vec::new();
        for job in jobs {
            rows.push(job.await.unwrap());
        }
        burst_reports.push(rows);
    }
    assert_eq!(
        initial_sql,
        fingerprints(&pool, &source["full18_exact_microseconds_before"]).await
    );
    std::fs::write(std::env::var("VOD_CACHED_PROVIDER_BURST_REPORT").unwrap(),serde_json::to_vec_pretty(&json!({"passed":true,"Source_unstaggered10":burst_reports[0],"native_unstaggered10":burst_reports[1],"scope":"Original and native actual TCP burst correctness only; no isolated resource performance"})).unwrap()).unwrap();
    resources::controls(&pool, &base, &source).await;
    assert_eq!(
        initial_sql,
        fingerprints(&pool, &source["full18_exact_microseconds_before"]).await
    );
    assert!(ordinary.shutdown(Duration::from_secs(3)).await);
    server.abort();
    let report = json!({"passed":differences.is_empty(),"compared":90,"differences":differences,"records":observations,"scope":"actual Original Source90 TCP versus isolated cached native TCP; no app/browser/refresh/image-byte acceptance","sql_request_effects":"all18 exact PostgreSQL JSONB text fingerprints unchanged per request"});
    std::fs::write(
        std::env::var("VOD_CACHED_PROVIDER_INFO_REPORT").unwrap(),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(
        report["passed"],
        true,
        "{} differences",
        report["differences"].as_array().unwrap().len()
    );
}

#[path = "vod_api_provider_info_resources.rs"]
mod resources;
