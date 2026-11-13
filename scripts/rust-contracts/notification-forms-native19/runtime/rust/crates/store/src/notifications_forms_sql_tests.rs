//! Native notification forms Source12 plus seven predeclared legacy transport checks.
//! Capture only; system_settings request_started cache gap remains unqualified.
//! Original Dispatcharr contributors retain application/algorithm credit.
use super::*;
use crate::{initialize_empty_database, AuthConfig, SchemaInitialization, Store};
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
    let mut named = BTreeMap::new();
    for key in [
        ":1:coresettings:group:system_settings",
        ":1:coresettings:group:network_access",
        ":1:coresettings:groupver:network_access",
    ] {
        let raw = cache.get(key).await.unwrap();
        let decoded = raw
            .as_ref()
            .map(|v| decode_cache(v).expect("Existing bounded Django pickle decoder"));
        named.insert(
            key,
            json!({"raw_GET_hex":raw.as_ref().map(|v|hex(v)),"decoded":decoded}),
        );
    }
    json!({"named_decoded":named,"rows":output.into_values().collect::<Vec<_>>(),"capture_bytes":used,"api_key_failed_auth_history":history,"api_key_failed_auth_raw_hex":raw.as_ref().map(|v|hex(v)),"scope":"owned Redis cache DB only; raw TTL retained, no exact TTL equality"})
}
async fn redis_snapshot(url: &str, db: u8) -> Value {
    let client = redis::Client::open(url).unwrap();
    assert_eq!(client.get_connection_info().redis.db, i64::from(db));
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut cursor = 0u64;
    let mut rows = BTreeMap::new();
    let mut used = 0usize;
    let mut done = false;
    for _ in 0..128 {
        let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut connection)
            .await
            .unwrap();
        cursor = next;
        for key in keys {
            assert!(key.len() <= 1024 && rows.len() < 1000);
            let kind: String = redis::cmd("TYPE")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            let memory: Option<u64> = redis::cmd("MEMORY")
                .arg("USAGE")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            assert!(
                memory.unwrap_or(0) <= 1048576,
                "OwnedRedisPerKeyPreflight1MiB"
            );
            let dump: Option<Vec<u8>> = redis::cmd("DUMP")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            let ttl: i64 = redis::cmd("PTTL")
                .arg(&key)
                .query_async(&mut connection)
                .await
                .unwrap();
            let mut row = json!({"key_hex":hex(&key),"type":kind,"dump_hex":dump.as_ref().map(|v|hex(v)),"pttl":ttl});
            used += key.len() + dump.as_ref().map_or(0, Vec::len);
            assert!(used <= 8388608);
            if kind == "string" {
                let value: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                row["value_hex"] = json!(value.map(|v| hex(&v)));
            } else if kind == "list" {
                let count: u64 = redis::cmd("LLEN")
                    .arg(&key)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(count <= 1000);
                let values: Vec<Vec<u8>> = redis::cmd("LRANGE")
                    .arg(&key)
                    .arg(0)
                    .arg(-1)
                    .query_async(&mut connection)
                    .await
                    .unwrap();
                assert!(values.iter().map(Vec::len).sum::<usize>() <= 8388608);
                row["list_hex"] = json!(values.iter().map(|v| hex(v)).collect::<Vec<_>>());
            }
            rows.insert(hex(&key), row);
        }
        if cursor == 0 {
            done = true;
            break;
        }
    }
    assert!(done);
    json!({"database":db,"capture_bytes":used,"rows":rows.into_values().collect::<Vec<_>>()})
}
async fn raw_redis_all(base: &str) -> Value {
    let mut scopes = Vec::new();
    for database in 0..=3u8 {
        let mut url = url::Url::parse(base).unwrap();
        url.set_path(&format!("/{database}"));
        scopes.push(
            tokio::time::timeout(
                Duration::from_secs(5),
                redis_snapshot(url.as_str(), database),
            )
            .await
            .unwrap(),
        );
    }
    json!(scopes)
}
async fn events(store: &Store) -> Value {
    rows(store, "core_systemevent").await
}
fn target(spec: &Value, aliases: &BTreeMap<String, i64>) -> String {
    let alias = spec["target_alias"].as_str().unwrap();
    if alias == "collection" {
        "/api/core/notifications/".into()
    } else {
        format!("/api/core/notifications/{}/", aliases[alias])
    }
}
#[tokio::test]
#[ignore = "requires reviewed isolated Native forms Source12 fixture; no Source app"]
async fn real_api_keys_forms_twelve_and_seven_transport_regressions() {
    assert_eq!(
        std::env::var("NOTIFICATION_FORMS_NATIVE").as_deref(),
        Ok("owned-native-forms-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    let nonce = database
        .strip_prefix("rust_probe_notifforms_native_")
        .unwrap();
    assert_eq!(nonce.len(), 10);
    assert!(nonce
        .bytes()
        .all(|v| v.is_ascii_hexdigit() && !v.is_ascii_uppercase()));
    let root = std::path::PathBuf::from(std::env::var("NOTIFICATION_FORMS_NATIVE_ROOT").unwrap());
    let output =
        std::path::PathBuf::from(std::env::var("NOTIFICATION_FORMS_NATIVE_REPORT").unwrap());
    assert!(!output.exists());
    let source_bytes = std::fs::read(root.join("inputs/Source12-projection.json")).unwrap();
    assert_eq!(
        hash(&source_bytes),
        "1c3f5dcce14ffe9a773cb29833c4caef52ed15c813cf9473f6c397dc34ca240f"
    );
    let binding: Value =
        serde_json::from_slice(&std::fs::read(root.join("inputs/Source-binding.json")).unwrap())
            .unwrap();
    assert_eq!(
        binding["mode"],
        "projected-notification-forms-source12-reference-v1"
    );
    assert_eq!(
        binding["Original_raw_sha256"],
        "b72af1de2683fa9e2345281d24cdaa7418368e1af0404eecba9ba018555a8d84"
    );
    let plan_bytes = std::fs::read(root.join("inputs/plan.json")).unwrap();
    assert_eq!(hash(&plan_bytes), binding["plan_sha256"]);
    let source: Value = serde_json::from_slice(&source_bytes).unwrap();
    let plan: Value = serde_json::from_slice(&plan_bytes).unwrap();
    assert_eq!(plan["cases"].as_array().unwrap().len(), 12);
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    let guard = Store::connect(&url, 1).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(guard.pool())
            .await
            .unwrap(),
        database
    );
    guard.pool().close().await;
    assert!(matches!(
        initialize_empty_database(&url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let store = Store::connect(&url, 4).await.unwrap();
    let mut report = json!({"capture_complete":false,"passed":false,"capture_only_passed_field":true,"database":database,"runtime_base":"e194c9e1d96db7e33857b93df85bfb9b0a97808f","Source_application_runs":0,"Main_application_runs":0,"Source_settings_values_are_owned_fixture_context":true,"default_worker_or_schedule_activation_claimed":false,"actual_auth_backend_introspection":false,"Source_reference_mode":binding["mode"],"Source_projection_sha256":hash(&source_bytes),"Original_raw_sha256":binding["Original_raw_sha256"],"initial_SQL":snapshot(&store).await,"initial_events":events(&store).await,"actual_request_count":0,"requests":[],"Native_transport_diagnostics":[],"full_cache_parity_claimed":false,"known_full_cache_gap":"No notification request_started system_settings callback; natural Native empty cache and Source version/group differences are retained, not imported or normalized"});
    save(&output, &report);
    report["seed_started_utc"] = json!(Utc::now().to_rfc3339());
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("DELETE FROM core_notificationdismissal")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_systemnotification")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(rows(&store, "accounts_user").await, json!([]));
    let mut users = BTreeMap::new();
    let seed_clock = Utc::now().to_rfc3339();
    for role in plan["roles"].as_array().unwrap() {
        let alias = role["alias"].as_str().unwrap();
        let mut user = source["seed"]["users"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == role["id"])
            .unwrap()
            .clone();
        // Source key evidence/date are never accepted as Native credentials.
        user["api_key"] = json!(format!("native-forms-api-{nonce}-{alias}"));
        user["username"] = json!(format!("native-forms-{nonce}-{alias}"));
        user["date_joined"] = json!(seed_clock);
        sqlx::query(
            "INSERT INTO accounts_user SELECT * FROM jsonb_populate_record(NULL::accounts_user,$1)",
        )
        .bind(&user)
        .execute(&mut *tx)
        .await
        .unwrap();
        users.insert(alias.to_owned(), user);
    }
    for row in plan["seed_notifications"].as_array().unwrap() {
        sqlx::query("INSERT INTO core_systemnotification SELECT * FROM jsonb_populate_record(NULL::core_systemnotification,$1)").bind(row).execute(&mut *tx).await.unwrap();
    }
    for row in plan["seed_dismissals"].as_array().unwrap() {
        sqlx::query("INSERT INTO core_notificationdismissal SELECT * FROM jsonb_populate_record(NULL::core_notificationdismissal,$1)").bind(row).execute(&mut *tx).await.unwrap();
    }
    for (table, value) in plan["seed_sequences"].as_object().unwrap() {
        assert!(matches!(
            table.as_str(),
            "accounts_user" | "core_systemnotification" | "core_notificationdismissal"
        ));
        sqlx::query("SELECT setval($1::regclass,$2,$3)")
            .bind(format!("{table}_id_seq"))
            .bind(value["last_value"].as_i64().unwrap())
            .bind(value["is_called"].as_bool().unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    // Exact Source settings values are explicit fixture context before requests.
    // Other baseline records are retained and compared within this Native DB.
    for row in source["seed"]["audit"]["tables"]["core_coresettings"]["raw_rows"]
        .as_array()
        .unwrap()
    {
        sqlx::query("INSERT INTO core_coresettings(key,name,value)VALUES($1,$2,$3)ON CONFLICT(key)DO UPDATE SET name=EXCLUDED.name,value=EXCLUDED.value").bind(row["key"].as_str().unwrap()).bind(row["name"].as_str().unwrap()).bind(&row["value"]).execute(&mut *tx).await.unwrap();
    }
    tx.commit().await.unwrap();
    report["seed_completed_utc"] = json!(Utc::now().to_rfc3339());
    report["seed_SQL"] = snapshot(&store).await;
    report["seed_events"] = events(&store).await;
    report["seed_users"] = rows(&store, "accounts_user").await;
    save(&output, &report);
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    report["cache_after_seed"] = cache_snapshot(&cache, &mut redis).await;
    assert_eq!(
        report["cache_after_seed"]["rows"],
        json!([]),
        "NaturalOwnedNativeEmptyCacheNoSourceWarmImport"
    );
    save(&output, &report);
    let service = AuthService::new(
        store.clone(),
        AuthConfig::new("notification-forms-native-only".into()).unwrap(),
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
    let mut aliases = BTreeMap::from([
        ("aliasDelete".into(), 10),
        ("foreign".into(), 11),
        ("derived_missing".into(), 1000011),
    ]);
    for spec in plan["cases"].as_array().unwrap() {
        let index = spec["index"].as_u64().unwrap();
        let path = target(spec, &aliases);
        let body = std::fs::read(
            root.join("inputs")
                .join(spec["body_path"].as_str().unwrap()),
        )
        .unwrap();
        assert_eq!(hash(&body), spec["body_sha256"]);
        assert_eq!(body.len() as u64, spec["body_bytes"].as_u64().unwrap());
        let actor = spec["actor"].as_str().unwrap();
        let key = users[actor]["api_key"].as_str().unwrap();
        let mut row = json!({"index":index,"actor":actor,"method":spec["method"],"path":path,"target_alias":spec["target_alias"],"body_sha256":hash(&body),"content_type":spec["content_type"],"actual_request_invoked":true,"raw_headers_private":{"HTTP_X_API_KEY":hex(key.as_bytes())},"before_SQL":snapshot(&store).await,"before_events":events(&store).await,"before_cache":cache_snapshot(&cache,&mut redis).await,"before_raw_Redis":raw_redis_all(&std::env::var("REDIS_TEST_URL").unwrap()).await,"began_utc":Utc::now().to_rfc3339()});
        report["actual_request_count"] = json!(index);
        report["requests"].as_array_mut().unwrap().push(row.clone());
        save(&output, &report);
        let response = client
            .request(
                reqwest::Method::from_bytes(spec["method"].as_str().unwrap().as_bytes()).unwrap(),
                format!("http://{address}{path}"),
            )
            .header("x-api-key", key)
            .header("accept", "application/json")
            .header(
                "content-type",
                spec["content_type"]
                    .as_str()
                    .unwrap_or("application/octet-stream"),
            )
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_str().unwrap().to_owned())))
            .collect::<Map<_, _>>();
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= 65536);
        row["status"] = json!(status);
        row["headers"] = headers.into();
        row["response_body_bytes"] = json!(bytes.len());
        row["response_body_hex"] = json!(hex(&bytes));
        row["response_body_sha256"] = json!(hash(&bytes));
        row["json"] = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        row["completed_utc"] = json!(Utc::now().to_rfc3339());
        row["after_SQL"] = snapshot(&store).await;
        row["after_events"] = events(&store).await;
        row["after_cache"] = cache_snapshot(&cache, &mut redis).await;
        row["after_raw_Redis"] = raw_redis_all(&std::env::var("REDIS_TEST_URL").unwrap()).await;
        if index == 1 || index == 7 {
            let name = if index == 1 { "FormA" } else { "FormB" };
            let id = row["json"]["id"]
                .as_i64()
                .expect("Natural actual create alias required; no fabricated fallback");
            aliases.insert(name.into(), id);
        }
        report["requests"][index as usize - 1] = row;
        save(&output, &report);
    }
    // Predeclared Native-only preservation checks, not new Source authority.
    let diagnostics = [
        ("anonymous", false, "application/json", 401u16),
        ("anonymous", true, "application/json", 401),
        ("standard", false, "application/json", 403),
        ("standard", true, "application/json", 403),
        ("admin", false, "application/json", 400),
        ("admin", true, "application/json", 400),
        ("admin", false, "text/plain", 400),
    ];
    for (offset, (actor, oversize, content_type, predicted)) in diagnostics.into_iter().enumerate()
    {
        let before = snapshot(&store).await;
        let before_events = events(&store).await;
        let body = if oversize {
            vec![b'x'; 2 * 1024 * 1024 + 1]
        } else {
            b"{".to_vec()
        };
        let mut row = json!({"index":offset+13,"scope":"Native-only existing create extractor/auth regression, not Source12 parity","actor":actor,"oversize":oversize,"content_type":content_type,"body_bytes":body.len(),"body_sha256":hash(&body),"predicted_status_predeclared":predicted,"before_SQL":before,"before_events":before_events,"before_cache":cache_snapshot(&cache,&mut redis).await,"before_raw_Redis":raw_redis_all(&std::env::var("REDIS_TEST_URL").unwrap()).await,"began_utc":Utc::now().to_rfc3339()});
        let mut request = client
            .post(format!("http://{address}/api/core/notifications/"))
            .header("content-type", content_type)
            .body(body);
        if actor != "anonymous" {
            request = request.header("x-api-key", users[actor]["api_key"].as_str().unwrap());
        }
        row["actual_request_invoked"] = json!(true);
        row["raw_headers_private"] = if actor == "anonymous" {
            json!({})
        } else {
            json!({"HTTP_X_API_KEY":hex(users[actor]["api_key"].as_str().unwrap().as_bytes())})
        };
        report["Native_transport_diagnostics"]
            .as_array_mut()
            .unwrap()
            .push(row.clone());
        report["actual_request_count"] = json!(offset + 13);
        save(&output, &report);
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        row["headers"] = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_str().unwrap().to_owned())))
            .collect::<Map<_, _>>()
            .into();
        let bytes = response.bytes().await.unwrap();
        assert!(bytes.len() <= 65536);
        row["status"] = json!(status);
        row["response_body_bytes"] = json!(bytes.len());
        row["response_body_hex"] = json!(hex(&bytes));
        row["response_body_sha256"] = json!(hash(&bytes));
        row["json"] = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        row["completed_utc"] = json!(Utc::now().to_rfc3339());
        row["after_SQL"] = snapshot(&store).await;
        row["after_events"] = events(&store).await;
        row["after_cache"] = cache_snapshot(&cache, &mut redis).await;
        row["after_raw_Redis"] = raw_redis_all(&std::env::var("REDIS_TEST_URL").unwrap()).await;
        report["Native_transport_diagnostics"][offset] = row;
        report["actual_request_count"] = json!(offset + 13);
        save(&output, &report);
    }
    drop(client);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    report["server_graceful_wait0"] = json!(true);
    report["final_SQL"] = snapshot(&store).await;
    report["final_events"] = events(&store).await;
    report["final_cache"] = cache_snapshot(&cache, &mut redis).await;
    report["final_raw_Redis"] = raw_redis_all(&std::env::var("REDIS_TEST_URL").unwrap()).await;
    report["aliases"] = serde_json::to_value(aliases).unwrap();
    drop(redis);
    drop(cache);
    store.pool().close().await;
    report["capture_complete"] = json!(true);
    save(&output, &report);
}
