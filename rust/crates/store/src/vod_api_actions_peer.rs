//! Actual Original/network comparator for isolated provider action prototype.
//! Does not establish canonical app/browser/performance acceptance.
use super::{vod_api_http::VodReads, vod_api_owned_read::ReadLimits};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{net::SocketAddr, time::Duration};
const PREFIX: &str = "rust-probe-vod-rest-";
const TABLES: [(&str, &str); 6] = [
    ("m3u_m3uaccountprofile", "m3u_account_id=ANY($1)"),
    ("m3u_m3ufilter", "m3u_account_id=ANY($1)"),
    (
        "dispatcharr_channels_channelgroup",
        "name LIKE 'rust-probe-vod-rest-%'",
    ),
    (
        "dispatcharr_channels_channelgroupm3uaccount",
        "m3u_account_id=ANY($1)",
    ),
    (
        "dispatcharr_channels_stream",
        "name LIKE 'rust-probe-vod-rest-%'",
    ),
    ("vod_m3uepisoderelation", "m3u_account_id=ANY($1)"),
];
async fn capture(pool: &PgPool, accounts: &[i64], movie: i64) -> Value {
    let mut state = serde_json::Map::new();
    for (table, predicate) in TABLES {
        let sql = format!("SELECT to_jsonb(t) AS raw FROM {table} t WHERE {predicate} ORDER BY id");
        let rows = if predicate.contains("$1") {
            sqlx::query(&sql)
                .persistent(false)
                .bind(accounts)
                .fetch_all(pool)
                .await
                .unwrap()
        } else {
            sqlx::query(&sql)
                .persistent(false)
                .fetch_all(pool)
                .await
                .unwrap()
        };
        state.insert(
            table.into(),
            rows.iter()
                .map(|row| row.try_get::<Value, _>("raw").unwrap())
                .collect(),
        );
    }
    state.insert("movie_relation".into(),sqlx::query("SELECT jsonb_build_object('id',id,'category_id',category_id,'custom_properties',custom_properties) AS raw FROM vod_m3umovierelation WHERE movie_id=$1 ORDER BY id").persistent(false).bind(movie).fetch_all(pool).await.unwrap().iter().map(|row|row.try_get::<Value,_>("raw").unwrap()).collect());
    state.insert("account_preferences".into(),sqlx::query("SELECT jsonb_build_object('id',id,'custom_properties',custom_properties) AS raw FROM m3u_m3uaccount WHERE id=ANY($1) ORDER BY id").persistent(false).bind(accounts).fetch_all(pool).await.unwrap().iter().map(|row|row.try_get::<Value,_>("raw").unwrap()).collect());
    state.into()
}
async fn restore(pool: &PgPool, accounts: &[i64], state: &Value) {
    let mut tx = pool.begin().await.unwrap();
    for index in [5, 4, 3, 2, 1, 0] {
        let (table, predicate) = TABLES[index];
        let sql = format!("DELETE FROM {table} WHERE {predicate}");
        if predicate.contains("$1") {
            sqlx::query(&sql)
                .persistent(false)
                .bind(accounts)
                .execute(&mut *tx)
                .await
                .unwrap();
        } else {
            sqlx::query(&sql)
                .persistent(false)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
    }
    for index in [2, 0, 1, 3, 4, 5] {
        let (table, _) = TABLES[index];
        let sql = format!(
            "INSERT INTO {table} SELECT * FROM json_populate_recordset(NULL::{table},$1::json)"
        );
        sqlx::query(&sql)
            .persistent(false)
            .bind(state[table].to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    for row in state["movie_relation"].as_array().unwrap() {
        sqlx::query(
            "UPDATE vod_m3umovierelation SET custom_properties=$1,category_id=$2 WHERE id=$3",
        )
        .persistent(false)
        .bind((!row["custom_properties"].is_null()).then(|| row["custom_properties"].clone()))
        .bind(row["category_id"].as_i64())
        .bind(row["id"].as_i64().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    for row in state["account_preferences"].as_array().unwrap() {
        sqlx::query("UPDATE m3u_m3uaccount SET custom_properties=$1 WHERE id=$2")
            .persistent(false)
            .bind((!row["custom_properties"].is_null()).then(|| row["custom_properties"].clone()))
            .bind(row["id"].as_i64().unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
}
#[tokio::test]
#[ignore = "Requires owned actual Original PostgreSQL/Redis action fixture"]
async fn actual_source150_modal_actions() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_PROVIDER_ACTION_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(source["mode"], "http");
    assert_eq!(source["passed"], true);
    assert_eq!(source["records"].as_array().unwrap().len(), 150);
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    let accounts: Vec<i64> =
        sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccount WHERE name LIKE $1 ORDER BY id")
            .persistent(false)
            .bind(format!("{PREFIX}%"))
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(accounts.len(), 3);
    let movie = source["records"][0]["path"]
        .as_str()
        .unwrap()
        .split('/')
        .nth(4)
        .unwrap()
        .parse::<i64>()
        .unwrap();
    let original = capture(&pool, &accounts, movie).await;
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
    let ordinary = VodReads::new(auth, limits).unwrap();
    // Same runtime and gate as ordinary reads; mixed physical controls remain
    // separate gates beyond this initial exact wire comparison.
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
    let mut differences = Vec::new();
    let mut records = Vec::new();
    let policy: Value = serde_json::from_str(include_str!(
        "../datasets/vod-provider-unordered-reference-policy.json"
    ))
    .unwrap();
    let fresh_indices: Vec<usize> = policy["case_indices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap() as usize)
        .collect();
    assert_eq!(fresh_indices.len(), 25);
    let original_base = std::env::var("VOD_UNIFIED_ORIGINAL_HTTP_BASE").unwrap();
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(database, "dispatcharr");
    let standard = base["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["role"] == "standard" && row["method"] == "GET")
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap();
    let sentinel = client
        .get(format!("{original_base}/api/vod/movies/{movie}/"))
        .header("Host", "localhost")
        .header("Accept", "application/json")
        .header("Authorization", standard)
        .send()
        .await
        .unwrap();
    assert_eq!(sentinel.status().as_u16(), 200);
    let sentinel: Value = serde_json::from_slice(&sentinel.bytes().await.unwrap()).unwrap();
    let uuid: String = sqlx::query_scalar("SELECT uuid::text FROM vod_movie WHERE id=$1")
        .bind(movie)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sentinel["uuid"], uuid);
    assert!(sentinel["name"].as_str().unwrap().starts_with(PREFIX));
    let mut fresh_references = Vec::new();
    for (index, case) in source["records"].as_array().unwrap().iter().enumerate() {
        restore(
            &pool,
            &accounts,
            &case["private_pre_request_native_replay_state"],
        )
        .await;
        let path = case["path"].as_str().unwrap();
        let role = case["role"].as_str();
        let method = case["method"].as_str().unwrap();
        let mut request = client
            .request(method.parse().unwrap(), format!("http://{address}{path}"))
            .header("Host", "localhost")
            .header("Accept", case["accept"].as_str().unwrap());
        if let Some(role) = role {
            let token = base["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|row| {
                    row["role"] == role
                        && row["method"] == "GET"
                        && row["headers"]["HTTP_AUTHORIZATION"].is_string()
                })
                .unwrap()["headers"]["HTTP_AUTHORIZATION"]
                .as_str()
                .unwrap();
            request = request.header("Authorization", token);
        }
        let unchanged = full_state(&pool).await;
        let mut expected = case["expected"].clone();
        if fresh_indices.contains(&index) {
            let mut fresh = client
                .request(method.parse().unwrap(), format!("{original_base}{path}"))
                .header("Host", "localhost")
                .header("Accept", case["accept"].as_str().unwrap());
            if let Some(role) = role {
                let token = base["cases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|row| {
                        row["role"] == role
                            && row["method"] == "GET"
                            && row["headers"]["HTTP_AUTHORIZATION"].is_string()
                    })
                    .unwrap()["headers"]["HTTP_AUTHORIZATION"]
                    .as_str()
                    .unwrap();
                fresh = fresh.header("Authorization", token);
            }
            let fresh = fresh.send().await.unwrap();
            let fresh_status = fresh.status().as_u16();
            let fresh_headers = fresh.headers().clone();
            let fresh_body = fresh.bytes().await.unwrap();
            let fresh_json: Value = serde_json::from_slice(&fresh_body).unwrap();
            assert_eq!(fresh_status, 200);
            assert_eq!(stable_relation_fields(&fresh_json),stable_relation_fields(&case["expected"]["json"]),"Historical fields differ beyond declared unordered relation sequence / first-account counts");
            source_context_counts(&pool, &fresh_json).await;
            let mut headers = serde_json::Map::new();
            for name in [
                "Content-Type",
                "Content-Length",
                "Allow",
                "Vary",
                "WWW-Authenticate",
            ] {
                headers.insert(
                    name.into(),
                    fresh_headers
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(Value::from)
                        .unwrap_or(Value::Null),
                );
            }
            expected = json!({"status":fresh_status,"headers":headers,"body_sha256":format!("{:x}",Sha256::digest(&fresh_body)),"body_bytes":fresh_body.len(),"body_text":String::from_utf8_lossy(&fresh_body),"json":fresh_json});
            assert_eq!(
                full_state(&pool).await,
                unchanged,
                "Fresh Original HTTP changed full state"
            );
            fresh_references.push(json!({"index":index,"historical":case["expected"],"fresh_actual_Original":expected,"same_fixture_uuid":uuid,"full_state_exact_microseconds":unchanged,"historical_fields_equal_except_declared_order_and_context_counts":true,"Source_context_count_rule_checked":true}));
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.unwrap();
        let hash = format!("{:x}", Sha256::digest(&body));
        let expected = &expected;
        let mut diff = Vec::new();
        if Some(u64::from(status)) != expected["status"].as_u64() {
            diff.push("status");
        }
        if hash != expected["body_sha256"].as_str().unwrap() {
            diff.push("body");
        }
        for name in [
            "Content-Type",
            "Content-Length",
            "Allow",
            "Vary",
            "WWW-Authenticate",
        ] {
            let got = headers.get(name).and_then(|value| value.to_str().ok());
            if got != expected["headers"][name].as_str() {
                diff.push(name);
            }
        }
        if !diff.is_empty() {
            differences.push(json!({"index":index,"path":path,"role":role,"method":method,"label":case["label"],"fields":diff,"Source_status":expected["status"],"native_status":status,"Source_hash":expected["body_sha256"],"native_hash":hash,"native_body":String::from_utf8_lossy(&body)}));
        }
        assert_eq!(
            full_state(&pool).await,
            unchanged,
            "Native action changed full state"
        );
        records.push(json!({"index":index,"native_status":status,"body_sha256":hash,"matched":diff.is_empty()}));
    }
    restore(&pool, &accounts, &original).await;
    assert_eq!(
        capture(&pool, &accounts, movie).await,
        original,
        "Owned mutation state restoration"
    );
    assert!(ordinary.shutdown(Duration::from_secs(3)).await);
    server.abort();
    let _ = server.await;
    let report = json!({"passed":differences.is_empty(),"scope":"Actual Original/nativeTCP150 permission/method/DTO/SQL-effect controls; no app/browser/performance acceptance","compared":records.len(),"predeclared_fresh_references":fresh_references,"Original_same_fixture_uuid":uuid,"Original_database":database,"records":records,"differences":differences,"owned_metadata_restored":true});
    std::fs::write(
        std::path::Path::new(&std::env::var("VOD_NATIVE_EVIDENCE").unwrap())
            .join("provider-actions-native.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(
        differences.is_empty(),
        "Source/native action differences: {differences:#?}"
    );
}

const STATE_TABLES: [&str; 18] = [
    "accounts_user",
    "m3u_m3uaccount",
    "m3u_m3uaccountprofile",
    "vod_movie",
    "vod_series",
    "vod_episode",
    "vod_vodlogo",
    "vod_vodcategory",
    "vod_m3umovierelation",
    "vod_m3useriesrelation",
    "vod_m3uepisoderelation",
    "vod_m3uvodcategoryrelation",
    "core_coresettings",
    "core_systemevent",
    "m3u_m3ufilter",
    "dispatcharr_channels_channelgroup",
    "dispatcharr_channels_channelgroupm3uaccount",
    "dispatcharr_channels_stream",
];
pub(super) async fn full_state(pool: &PgPool) -> Value {
    let mut state = serde_json::Map::new();
    for table in STATE_TABLES {
        let sql = format!(
            "SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id)::text,'[]')) FROM {table} t"
        );
        let hash: String = sqlx::query_scalar(&sql)
            .persistent(false)
            .fetch_one(pool)
            .await
            .unwrap();
        state.insert(table.into(), hash.into());
    }
    state.into()
}
fn stable_relation_fields(rows: &Value) -> Value {
    let mut result = serde_json::Map::new();
    for row in rows.as_array().unwrap() {
        let mut value = row.clone();
        for group in value["m3u_account"]["channel_groups"]
            .as_array_mut()
            .unwrap()
        {
            group.as_object_mut().unwrap().remove("stream_count");
        }
        result.insert(row["id"].to_string(), value);
    }
    result.into()
}
async fn source_context_counts(pool: &PgPool, rows: &Value) {
    let rows = rows.as_array().unwrap();
    let first = rows[0]["m3u_account"]["id"].as_i64().unwrap();
    for row in rows {
        let account = row["m3u_account"]["id"].as_i64().unwrap();
        for group in row["m3u_account"]["channel_groups"].as_array().unwrap() {
            let expected = if account == first {
                sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM dispatcharr_channels_stream WHERE m3u_account_id=$1 AND channel_group_id=$2").persistent(false).bind(account).bind(group["channel_group"].as_i64().unwrap()).fetch_one(pool).await.unwrap()
            } else {
                0
            };
            assert_eq!(
                group["stream_count"], expected,
                "Observed Source first-account serializer context rule"
            );
        }
    }
}

#[path = "vod_api_actions_resources.rs"]
mod resources;

#[tokio::test]
#[ignore = "Requires owned Source Factory/realSQL controlled phase reference"]
async fn controlled_source_snapshot_phases() {
    super::vod_api_actions_schedule::compare().await;
}

#[tokio::test]
#[ignore = "Requires owned Original network nonempty-cron reference"]
async fn cron_actual_source2() {
    super::vod_api_actions_schedule::compare_cron().await;
}
