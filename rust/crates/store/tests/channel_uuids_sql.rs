//! Original Dispatcharr UUID action contracts on disposable real PostgreSQL.
//! Original authors retain all application/algorithm credit and licenses.
use dispatcharr_store::{AuthConfig, AuthService, Store};
use serde_json::{json, Value};
use std::time::{SystemTime, UNIX_EPOCH};
#[tokio::test]
#[ignore = "requires owned channel UUID HTTP fixture and PostgreSQL"]
async fn actual_source_uuid_action_http_and_acl() {
    assert_eq!(
        std::env::var("CHANNEL_UUIDS_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let golden: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("CHANNEL_UUIDS_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(golden["actual_Original_HTTP"], true);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let audit_store = store.clone();
    for (table, expected) in golden["row_fingerprints"].as_object().unwrap() {
        let sql = format!(
            "SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.id)::text,'[]')) FROM {table} t"
        );
        let hash: String = sqlx::query_scalar(&sql)
            .fetch_one(audit_store.pool())
            .await
            .unwrap();
        assert_eq!(
            hash,
            expected.as_str().unwrap(),
            "Source row fingerprint {table}"
        );
    }
    let router = AuthService::new(
        store,
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let cases = golden["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 39);
    for case in cases {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut request = client
            .post(format!(
                "http://{address}/api/channels/channels/by-uuids/{}",
                case["query"].as_str().unwrap()
            ))
            .header("Host", "127.0.0.1:5656")
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&case["body"]).unwrap());
        if let Some(id) = case["user_id"].as_i64() {
            let token=jsonwebtoken::encode(&jsonwebtoken::Header::default(),&json!({"token_type":"access","exp":now+600,"iat":now,"jti":uuid::Uuid::new_v4().to_string(),"user_id":id.to_string()}),&jsonwebtoken::EncodingKey::from_secret(b"playback-fixture-only")).unwrap();
            request = request.bearer_auth(token);
        } else if case["role"] == "invalid" {
            request = request.bearer_auth("invalid");
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = response.bytes().await.unwrap();
        let actual = if case["expected"].get("json").is_some() {
            json!({"json":serde_json::from_slice::<Value>(&bytes).unwrap()})
        } else {
            json!({"text":std::str::from_utf8(&bytes).unwrap()})
        };
        assert_eq!(
            json!({"status":status,"content_type":content_type,"expected":actual}),
            json!({"status":case["status"],"content_type":case["content_type"],"expected":case["expected"]}),
            "case {}",
            case["label"]
        );
    }
    for (table, expected) in golden["row_fingerprints"].as_object().unwrap() {
        let sql = format!(
            "SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.id)::text,'[]')) FROM {table} t"
        );
        let hash: String = sqlx::query_scalar(&sql)
            .fetch_one(audit_store.pool())
            .await
            .unwrap();
        assert_eq!(
            hash,
            expected.as_str().unwrap(),
            "Native row fingerprint {table}"
        );
    }
    stop.send(()).unwrap();
    server.await.unwrap();
    println!(
        "Actual Original/native UUID action comparisons passed: {}",
        cases.len()
    );
}
