//! Original generic null/nonobject serializer error-family and no-mutation proof.
//! Original Dispatcharr authors retain full application credit.
#[path = "../../app/src/response_headers.rs"]
mod response_headers;
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, OutputProfileWrites, Store};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};
async fn fingerprint(store: &Store, tables: &Value) -> Value {
    let mut result = BTreeMap::new();
    for table in tables.as_array().unwrap() {
        let table = table.as_str().unwrap();
        assert!(table
            .bytes()
            .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit() || v == b'_'));
        let query=format!("SELECT md5(COALESCE(jsonb_agg(to_jsonb(t)ORDER BY t.id)::text,'[]'))FROM \"{table}\" t");
        let hash: String = sqlx::query_scalar(&query)
            .fetch_one(store.pool())
            .await
            .unwrap();
        result.insert(table.to_string(), hash);
    }
    json!(result)
}
fn canonical(value: &Value) -> Value {
    match value {
        Value::Array(values) => json!(values.iter().map(canonical).collect::<Vec<_>>()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), canonical(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        value => value.clone(),
    }
}
#[tokio::test]
#[ignore = "requires owned original serializer fixture, real PostgreSQL/Redis; explicitly run after capture"]
async fn original_serializer_null_and_nonobject_contracts() {
    assert_eq!(
        std::env::var("SERIALIZER_NULL_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("SERIALIZER_NULL_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache);
    let profiles = OutputProfileWrites::new(auth.clone(), Some(&redis_url))
        .await
        .unwrap();
    let router = auth
        .router()
        .merge(profiles.clone().router())
        .layer(axum::middleware::from_fn(response_headers::apply));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut checks = vec![];
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        let before = fingerprint(&store, &fixture["protected_tables"]).await;
        let raw = serde_json::to_vec(&case["body"]).unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&raw)),
            case["request_body_sha256"].as_str().unwrap()
        );
        let mut request = client
            .request(
                case["method"]
                    .as_str()
                    .unwrap()
                    .parse::<reqwest::Method>()
                    .unwrap(),
                format!("http://{address}{}", case["path"].as_str().unwrap()),
            )
            .body(raw);
        for (key, value) in case["headers"].as_object().unwrap() {
            request = request.header(key, value.as_str().unwrap())
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let mut headers = serde_json::Map::new();
        for key in [
            "content-type",
            "content-length",
            "www-authenticate",
            "x-content-type-options",
        ] {
            if let Some(value) = response.headers().get(key) {
                headers.insert(key.into(), json!(value.to_str().unwrap()));
            }
        }
        let raw = response.bytes().await.unwrap();
        let mut actual = json!({"status":status,"headers":headers});
        if actual["headers"]["content-type"]
            .as_str()
            .unwrap_or("")
            .contains("json")
        {
            actual["json"] = serde_json::from_slice(&raw).unwrap();
        } else {
            actual["body_length"] = json!(raw.len());
            actual["body_sha256"] = json!(format!("{:x}", Sha256::digest(&raw)));
        }
        let mut expected = case["expected"].clone();
        expected.as_object_mut().unwrap().remove("body_hex");
        let after = fingerprint(&store, &fixture["protected_tables"]).await;
        let mut errors = vec![];
        if canonical(&actual) != canonical(&expected) {
            errors.push("response")
        }
        if before != after {
            errors.push("model_mutation")
        }
        checks.push(json!({"case":index,"name":case["name"],"passed":errors.is_empty(),"mismatches":errors,"native_status":status,"source_status":case["expected"]["status"]}));
    }
    server.abort();
    let _ = server.await;
    assert!(profiles.shutdown(Duration::from_secs(1)).await);
    let passed = checks.iter().all(|check| check["passed"] == true);
    eprintln!(
        "SERIALIZER_NULL_CHECKS {}",
        serde_json::to_string(&json!({"passed":passed,"checks":checks})).unwrap()
    );
    assert!(
        passed,
        "Source/native serializer error-family differs; see redacted check categories"
    );
}
