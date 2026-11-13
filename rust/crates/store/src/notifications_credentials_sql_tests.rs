//! Owned Native real-credential20 capture; no Source application execution.
//! Original Dispatcharr contributors retain application/algorithm credit.
use super::*;
use crate::{initialize_empty_database, AuthConfig, SchemaInitialization, Store};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::Path};
const TABLES: [&str; 11] = [
    "accounts_user",
    "m3u_m3uaccount",
    "m3u_m3uaccountprofile",
    "vod_movie",
    "vod_m3umovierelation",
    "vod_series",
    "vod_episode",
    "vod_m3uepisoderelation",
    "core_coresettings",
    "core_systemnotification",
    "core_notificationdismissal",
];
fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}
fn hash(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}
fn save(path: &Path, value: &Value) {
    let bytes = serde_json::to_vec_pretty(value).unwrap();
    assert!(bytes.len() <= 134217728);
    std::fs::write(path, bytes).unwrap();
}
async fn rows(store: &Store, table: &str) -> Value {
    assert!(TABLES.contains(&table) || table == "core_systemevent");
    let value: Value = sqlx::query_scalar(&format!(
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM {table} t"
    ))
    .fetch_one(store.pool())
    .await
    .unwrap();
    let a = value.as_array().unwrap();
    assert!(a.len() <= 10000);
    assert!(serde_json::to_vec(&value).unwrap().len() <= 8388608);
    value
}
async fn snapshot(store: &Store) -> Value {
    let mut tables = BTreeMap::new();
    let mut sequences = BTreeMap::new();
    for table in TABLES {
        tables.insert(table, rows(store, table).await);
        let sequence:Value=sqlx::query_scalar(&format!("SELECT jsonb_build_object('last_value',last_value,'is_called',is_called) FROM {table}_id_seq")).fetch_one(store.pool()).await.unwrap();
        sequences.insert(table, sequence);
    }
    json!({"tables":tables,"sequences":sequences})
}
async fn cache_snapshot(
    cache: &NotificationCache,
    connection: &mut redis::aio::MultiplexedConnection,
) -> Value {
    let mut cursor = 0u64;
    let mut output = BTreeMap::new();
    let mut used = 0usize;
    let mut done = false;
    for _ in 0..128 {
        let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(100)
            .query_async(connection)
            .await
            .unwrap();
        cursor = next;
        for key in keys {
            assert!(key.len() <= 1024 && output.len() < 1000);
            let dump: Option<Vec<u8>> = redis::cmd("DUMP")
                .arg(&key)
                .query_async(connection)
                .await
                .unwrap();
            if let Some(dump) = dump {
                let ttl: i64 = redis::cmd("PTTL")
                    .arg(&key)
                    .query_async(connection)
                    .await
                    .unwrap();
                used += key.len() + dump.len();
                assert!(dump.len() <= 65536 && used <= 1048576);
                output.insert(
                    hex(&key),
                    json!({"key_hex":hex(&key),"dump_hex":hex(&dump),"pttl":ttl}),
                );
            }
        }
        if cursor == 0 {
            done = true;
            break;
        }
    }
    assert!(done);
    let raw = cache
        .get(":1:throttle_api_key_auth_127.0.0.1")
        .await
        .unwrap();
    let history = raw
        .as_ref()
        .map(|v| crate::django_pickle::decode(v).expect("literal Django failed-auth pickle"))
        .unwrap_or(Value::Null);
    assert!(
        history.is_null()
            || history
                .as_array()
                .is_some_and(|v| v.len() <= 10 && v.iter().all(Value::is_number))
    );
    json!({"rows":output.into_values().collect::<Vec<_>>(),"capture_bytes":used,"api_key_failed_auth_history":history,"api_key_failed_auth_raw_hex":raw.as_ref().map(|v|hex(v)),"scope":"owned Redis cache DB only; raw TTL retained, no exact TTL equality"})
}
fn material(
    spec: &Value,
    users: &BTreeMap<String, Value>,
    tokens: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (kind, value) in spec["auth"].as_object().unwrap() {
        let value = value.as_str().unwrap();
        match kind.as_str() {
            "bearer" => {
                out.insert("authorization".into(), format!("Bearer {}", tokens[value]));
            }
            "bearer_literal" => {
                out.insert("authorization".into(), format!("Bearer {value}"));
            }
            "x_api_key" => {
                out.insert(
                    "x-api-key".into(),
                    users[value]["api_key"].as_str().unwrap().into(),
                );
            }
            "x_api_key_literal" => {
                out.insert("x-api-key".into(), value.into());
            }
            "authorization_literal" => {
                out.insert("authorization".into(), value.into());
            }
            _ => panic!("unknown fixed auth selector"),
        }
    }
    out
}
#[tokio::test]
#[ignore = "requires reviewed owned Native credential20 PostgreSQL/Redis fixture; never default/Source"]
async fn real_credentials_twenty_match_retained_source_authority() {
    assert_eq!(
        std::env::var("NOTIFICATION_CREDENTIAL_NATIVE").as_deref(),
        Ok("owned-native-credentials-only")
    );
    let database = std::env::var("NOTIFICATION_CREDENTIAL_DATABASE").unwrap();
    let nonce = database
        .strip_prefix("rust_probe_notifcred_native_")
        .unwrap();
    assert_eq!(nonce.len(), 10);
    assert!(nonce
        .bytes()
        .all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase()));
    let root = std::path::PathBuf::from(std::env::var("NOTIFICATION_CREDENTIAL_ROOT").unwrap());
    let output = std::path::PathBuf::from(std::env::var("NOTIFICATION_CREDENTIAL_REPORT").unwrap());
    assert!(!output.exists());
    let plan_bytes = std::fs::read(root.join("inputs/plan.json")).unwrap();
    let source_bytes = std::fs::read(root.join("inputs/Source20-private.json")).unwrap();
    let binding: Value =
        serde_json::from_slice(&std::fs::read(root.join("inputs/Source-binding.json")).unwrap())
            .unwrap();
    assert_eq!(hash(&plan_bytes), binding["plan_sha256"]);
    assert_eq!(hash(&source_bytes), binding["Source20_sha256"]);
    let plan: Value = serde_json::from_slice(&plan_bytes).unwrap();
    let source: Value = serde_json::from_slice(&source_bytes).unwrap();
    assert_eq!(source["capture_valid"], true);
    assert_eq!(source["actual_request_count"], 20);
    assert_eq!(plan["cases"].as_array().unwrap().len(), 20);
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let guard = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&guard)
            .await
            .unwrap(),
        database
    );
    guard.close().await;
    assert!(matches!(
        initialize_empty_database(&url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let store = Store::connect(&url, 1).await.unwrap();
    let mut report = json!({"passed":false,"Native_only":true,"Source_application_runs":0,"database":database,"nonce":nonce,"runtime_base":"044f3ad0fb3ee68ffb17b23bbb796ccdae954298","Source20_sha256":hash(&source_bytes),"plan_sha256":hash(&plan_bytes),"actual_auth_backend_observed":false,"auth_observation_scope":"real Native router response and full side effects; no fabricated handler/backend introspection","requests":[]});
    save(&output, &report);
    report["initialized_before_owned_isolation"] = snapshot(&store).await;
    report["initialized_events"] = rows(&store, "core_systemevent").await;
    save(&output, &report);
    let changed = sqlx::query("UPDATE core_coresettings SET value=$1 WHERE key='network_access'")
        .bind(plan["global_network_settings"].clone())
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(changed.rows_affected(), 1);
    let mut transaction = store.pool().begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&mut *transaction)
            .await
            .unwrap(),
        database
    );
    sqlx::query("DELETE FROM core_notificationdismissal")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_systemnotification")
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM accounts_user")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
    report["seed_started_utc"] = json!(Utc::now().to_rfc3339());
    let mut users = BTreeMap::new();
    let mut tokens = BTreeMap::new();
    let seed = Utc::now().to_rfc3339();
    for role in plan["roles"].as_array().unwrap() {
        let alias = role["alias"].as_str().unwrap();
        let mut user = source["seed_users"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["id"] == role["id"])
            .unwrap()
            .clone();
        user["username"] = json!(format!("rust-probe-notifcred-{nonce}-{alias}"));
        user["api_key"] = json!(format!("fixture-key-{nonce}-{alias}"));
        user["date_joined"] = json!(seed);
        sqlx::query(
            "INSERT INTO accounts_user SELECT * FROM jsonb_populate_record(NULL::accounts_user,$1)",
        )
        .bind(user.clone())
        .execute(store.pool())
        .await
        .unwrap();
        users.insert(alias.to_owned(), user);
        if role["is_active"] == true {
            let now = Utc::now().timestamp();
            let claims = json!({"token_type":"access","exp":now+1800,"iat":now,"jti":uuid::Uuid::new_v4().simple().to_string(),"user_id":role["id"].as_i64().unwrap().to_string()});
            let token = encode(
                &Header::new(Algorithm::HS256),
                &claims,
                &EncodingKey::from_secret(b"notification-credential-fixture-only"),
            )
            .unwrap();
            tokens.insert(alias.to_owned(), token);
        }
    }
    for notice in source["seed_notifications"]["notifications"]
        .as_array()
        .unwrap()
    {
        sqlx::query("INSERT INTO core_systemnotification SELECT * FROM jsonb_populate_record(NULL::core_systemnotification,$1)").bind(notice).execute(store.pool()).await.unwrap();
    }
    for statement in [
        "SELECT setval('accounts_user_id_seq',6,true)",
        "SELECT setval('core_systemnotification_id_seq',2,true)",
        "SELECT setval('core_notificationdismissal_id_seq',1,false)",
    ] {
        sqlx::query(statement).execute(store.pool()).await.unwrap();
    }
    report["seed_completed_utc"] = json!(Utc::now().to_rfc3339());
    report["seed_SQL"] = snapshot(&store).await;
    report["seed_users"] = rows(&store, "accounts_user").await;
    report["tokens_private"] = tokens
        .iter()
        .map(|(k, v)| (k.clone(), json!({"raw":v,"sha256":hash(v.as_bytes())})))
        .collect::<Map<_, _>>()
        .into();
    save(&output, &report);
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    report["cache_after_seed"] = cache_snapshot(&cache, &mut redis).await;
    save(&output, &report);
    let service = AuthService::new(
        store.clone(),
        AuthConfig::new("notification-credential-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            service
                .router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    report["listener_address"] = json!(address.to_string());
    save(&output, &report);
    for spec in plan["cases"].as_array().unwrap() {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT current_database()")
                .fetch_one(store.pool())
                .await
                .unwrap(),
            database
        );
        let headers = material(spec, &users, &tokens);
        let before = snapshot(&store).await;
        let before_events = rows(&store, "core_systemevent").await;
        let before_cache = cache_snapshot(&cache, &mut redis).await;
        let mut row = json!({"ordinal":spec["ordinal"],"name":spec["name"],"method":spec["method"],"path":spec["path"],"auth_selectors":spec["auth"],"request_payload":spec["payload"],"raw_headers_private":headers.iter().map(|(k,v)|(k.clone(),json!(hex(v.as_bytes())))).collect::<Map<_,_>>(),"before_SQL":before,"before_events":before_events,"before_cache":before_cache,"began_utc":Utc::now().to_rfc3339()});
        report["requests"].as_array_mut().unwrap().push(row.clone());
        save(&output, &report);
        let mut request = client
            .request(
                spec["method"]
                    .as_str()
                    .unwrap()
                    .parse::<reqwest::Method>()
                    .unwrap(),
                format!("http://{address}{}", spec["path"].as_str().unwrap()),
            )
            .header("accept", "application/json");
        for (k, v) in headers {
            request = request.header(k, v);
        }
        if !spec["payload"].is_null() {
            request = request
                .header("content-type", "application/json")
                .body(serde_json::to_vec(&spec["payload"]).unwrap());
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let response_headers = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), json!(hex(v.as_bytes()))))
            .collect::<Map<_, _>>();
        assert!(serde_json::to_vec(&response_headers).unwrap().len() <= 131072);
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= 65536);
        let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        row["completed_utc"] = json!(Utc::now().to_rfc3339());
        row["status"] = json!(status);
        row["headers_hex"] = response_headers.into();
        row["body_hex"] = json!(hex(&bytes));
        row["body_bytes"] = json!(bytes.len());
        row["body_sha256"] = json!(hash(&bytes));
        row["json"] = parsed;
        row["after_SQL"] = snapshot(&store).await;
        row["after_events"] = rows(&store, "core_systemevent").await;
        row["after_cache"] = cache_snapshot(&cache, &mut redis).await;
        *report["requests"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap() = row;
        save(&output, &report);
        // Capture all20 independently of semantic qualification. The external
        // portable validator reports every mismatch and determines acceptance.
    }
    drop(client);
    assert!(stop.send(()).is_ok());
    assert!(tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
    report["HTTP_listener_joined"] = json!(true);
    drop(redis);
    drop(cache);
    store.pool().close().await;
    report["pool_after_first_close"] = json!({"size":store.pool().size(),"num_idle":store.pool().num_idle(),"is_closed":store.pool().is_closed(),"physical_backend_absence":"external owned controller must verify after process exit; no assumption from close return"});
    report["capture_complete"] = json!(true);
    report["actual_request_count"] = json!(report["requests"].as_array().unwrap().len());
    save(&output, &report);
}
