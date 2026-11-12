//! Unexecuted Source48-backed Native mutation QA. Original Dispatcharr authors
//! retain model/serializer/algorithm credit. No Source application is started.
use dispatcharr_store::{
    initialize_empty_database, AuthConfig, AuthService, NotificationCache, SchemaInitialization,
    Store,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};
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
const SOURCE_SHA: &str = "68542935c46471505dfbc9e1442e45fa9cbe498ef2c587fe0c257fb2ebd8b1bd";
fn clock(v: &Value) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(v.as_str().unwrap())
        .unwrap()
        .with_timezone(&chrono::Utc)
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|v| format!("{v:02x}")).collect()
}
fn unhex(v: &str) -> Vec<u8> {
    assert_eq!(v.len() % 2, 0);
    (0..v.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&v[i..i + 2], 16).unwrap())
        .collect()
}
fn save(p: &Path, v: &Value) {
    let bytes = serde_json::to_vec_pretty(v).unwrap();
    assert!(bytes.len() <= 134217728);
    std::fs::write(p, bytes).unwrap();
}
async fn binding(store: &Store, expected: &str) {
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        expected
    );
}
async fn snapshot(store: &Store) -> Value {
    let mut tables = BTreeMap::new();
    let mut sequences = BTreeMap::new();
    let mut total = 0;
    for table in TABLES {
        let rows:Value=sqlx::query_scalar(&format!("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM (SELECT * FROM {table} ORDER BY id LIMIT 10001) t")).fetch_one(store.pool()).await.unwrap();
        assert!(rows.as_array().unwrap().len() <= 10000);
        for r in rows.as_array().unwrap() {
            let size = serde_json::to_vec(r).unwrap().len();
            assert!(size <= 131072);
            total += size;
        }
        assert!(total <= 8388608);
        tables.insert(table, rows);
        let seq:Value=sqlx::query_scalar(&format!("SELECT jsonb_build_object('last_value',last_value,'is_called',is_called) FROM {table}_id_seq")).fetch_one(store.pool()).await.unwrap();
        sequences.insert(table, seq);
    }
    json!({"tables":tables,"sequences":sequences})
}
async fn credentials(store: &Store) -> Value {
    let users: Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM accounts_user t",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    let events:Value=sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM (SELECT * FROM core_systemevent ORDER BY id LIMIT 10001) t").fetch_one(store.pool()).await.unwrap();
    assert!(events.as_array().unwrap().len() <= 10000);
    let seq:Value=sqlx::query_scalar("SELECT jsonb_build_object('last_value',last_value,'is_called',is_called) FROM core_systemevent_id_seq").fetch_one(store.pool()).await.unwrap();
    let result =
        json!({"accounts_user":users,"core_systemevent":events,"core_systemevent_sequence":seq});
    assert!(serde_json::to_vec(&result).unwrap().len() <= 8388608);
    result
}
async fn cache_snapshot(
    connection: &mut redis::aio::MultiplexedConnection,
    source: &Value,
    expected: &str,
) -> Value {
    let mut cursor = 0u64;
    let mut rows = BTreeMap::new();
    let mut used = 0usize;
    let mut complete = false;
    for _ in 0..128 {
        let (next, keys): (u64, Vec<Vec<u8>>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(100)
            .query_async(connection)
            .await
            .unwrap();
        for key in keys {
            assert!(key.len() <= 1024);
            let dump: Option<Vec<u8>> = redis::cmd("DUMP")
                .arg(&key)
                .query_async(connection)
                .await
                .unwrap();
            if let Some(dump) = dump {
                assert!(dump.len() <= 65536);
                let ttl: i64 = redis::cmd("PTTL")
                    .arg(&key)
                    .query_async(connection)
                    .await
                    .unwrap();
                used += key.len() + dump.len();
                assert!(used <= 1048576);
                rows.insert(
                    hex(&key),
                    json!({"key_hex":hex(&key),"dump_hex":hex(&dump),"pttl":ttl}),
                );
                assert!(rows.len() <= 1000);
            }
        }
        cursor = next;
        if cursor == 0 {
            complete = true;
            break;
        }
    }
    assert!(complete);
    let named = named_cache(connection, source, expected).await;
    used += named["capture_bytes"].as_u64().unwrap() as usize;
    assert!(used <= 1048576);
    json!({"rows":rows.into_values().collect::<Vec<_>>(),"capture_bytes":used,"named":named})
}
async fn named_cache(
    connection: &mut redis::aio::MultiplexedConnection,
    source: &Value,
    expected: &str,
) -> Value {
    async fn entry(
        connection: &mut redis::aio::MultiplexedConnection,
        key: &str,
        marker: bool,
    ) -> Value {
        let raw:Option<Vec<u8>>=redis::cmd("EVAL").arg("if redis.call('STRLEN',KEYS[1])>65536 then return redis.error_reply('named cache capture exceeds limit') end return redis.call('GET',KEYS[1])").arg(1).arg(key).query_async(connection).await.unwrap();
        let pttl: i64 = redis::cmd("PTTL")
            .arg(key)
            .query_async(connection)
            .await
            .unwrap();
        let (value, decoded) = match raw.as_deref() {
            None => (Value::Null, true),
            Some(v) if marker => match std::str::from_utf8(v) {
                Ok(text) => (json!(text), true),
                Err(_) => (Value::Null, false),
            },
            Some([0x80, 4, 0x88, b'.']) => (json!(true), true),
            Some([0x80, 4, 0x89, b'.']) => (json!(false), true),
            Some(_) => (Value::Null, false),
        };
        json!({"key_hex":hex(key.as_bytes()),"present":raw.is_some(),"raw_hex":raw.as_ref().map(|v|hex(v)),"raw_bytes":raw.as_ref().map_or(0,Vec::len),"raw_sha256":raw.as_ref().map(|v|format!("{:x}",Sha256::digest(v))),"value":value,"decoded":decoded,"pttl":pttl})
    }
    let marker = entry(connection, "rust-probe-notification-native-owner", true).await;
    let mut values = serde_json::Map::new();
    let mut entries = serde_json::Map::new();
    for alias in ["H", "V", "U", "D"] {
        let key = format!(
            ":1:dev_notif_condition_{}_{}",
            source["seed_aliases"][alias].as_i64().unwrap(),
            source["seed_binding"]["actors"]["admin"].as_i64().unwrap()
        );
        let value = entry(connection, &key, false).await;
        values.insert(alias.into(), value["value"].clone());
        entries.insert(alias.into(), value);
    }
    let captured = marker["raw_bytes"].as_u64().unwrap()
        + "rust-probe-notification-native-owner".len() as u64
        + entries
            .values()
            .map(|v| {
                v["raw_bytes"].as_u64().unwrap() + v["key_hex"].as_str().unwrap().len() as u64 / 2
            })
            .sum::<u64>();
    json!({"expected_database":expected,"sentinel":marker,"values":values,"entries":entries,"capture_bytes":captured})
}
fn assert_named_cache(cache: &Value, expected_values: &Value, expected_database: &str) {
    let named = &cache["named"];
    assert_eq!(named["expected_database"], expected_database);
    assert_eq!(named["sentinel"]["value"], expected_database);
    assert_eq!(named["sentinel"]["present"], true);
    assert_eq!(named["sentinel"]["decoded"], true);
    assert_eq!(named["sentinel"]["pttl"], -1);
    assert_eq!(&named["values"], expected_values);
    for alias in ["H", "V", "U", "D"] {
        assert_eq!(named["entries"][alias]["decoded"], true);
        assert_eq!(
            named["entries"][alias]["present"],
            !expected_values[alias].is_null()
        );
    }
}
fn compare_values(
    source: &Value,
    native: &Value,
    source_case: &Value,
    began: chrono::DateTime<chrono::Utc>,
    ended: chrono::DateTime<chrono::Utc>,
    mapping: &mut BTreeMap<chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>>,
    path: &str,
) {
    match source {
        Value::Object(a) => {
            let b = native.as_object().unwrap();
            assert_eq!(a.len(), b.len(), "{path}");
            for (k, v) in a {
                compare_values(
                    v,
                    b.get(k).unwrap(),
                    source_case,
                    began,
                    ended,
                    mapping,
                    &format!("{path}/{k}"),
                );
            }
        }
        Value::Array(a) => {
            let b = native.as_array().unwrap();
            assert_eq!(a.len(), b.len(), "{path}");
            for (i, (a, b)) in a.iter().zip(b).enumerate() {
                compare_values(
                    a,
                    b,
                    source_case,
                    began,
                    ended,
                    mapping,
                    &format!("{path}/{i}"),
                );
            }
        }
        Value::String(_) if path.rsplit('/').next() == Some("expires_at") => {
            // PostgreSQL to_jsonb may trim fractional zeros; preserve raw text,
            // require the same expiry instant, and never map user-supplied expiry.
            assert_eq!(clock(source), clock(native), "exact expiry instant {path}");
        }
        Value::String(_)
            if matches!(
                path.rsplit('/').next(),
                Some("created_at" | "updated_at" | "dismissed_at")
            ) =>
        {
            let old = clock(source);
            let new = clock(native);
            if let Some(mapped) = mapping.get(&old) {
                assert_eq!(*mapped, new, "{path}");
                return;
            }
            if clock(&source_case["began_utc"]) <= old
                && old <= clock(&source_case["completed_utc"])
            {
                // Register generated clocks even when Source/Native coincide.
                assert!(began <= new && new <= ended);
                mapping.insert(old, new);
            } else {
                // Only as-yet-unmapped seed/unchanged instants may compare equal.
                assert_eq!(old, new, "unmapped seed or unchanged clock {path}");
            }
        }
        _ => assert_eq!(source, native, "{path}"),
    }
}
async fn call(
    client: &reqwest::Client,
    base: &str,
    case: &Value,
    keys: &BTreeMap<String, String>,
) -> (u16, Value, Vec<u8>, Option<Value>) {
    let mut request = client
        .request(
            case["method"]
                .as_str()
                .unwrap()
                .parse::<reqwest::Method>()
                .unwrap(),
            format!("{base}{}", case["path"].as_str().unwrap()),
        )
        .header("accept", "application/json");
    if let Some(key) = keys.get(case["role"].as_str().unwrap()) {
        request = request.header("x-api-key", key);
    }
    request = request.header("content-type", "application/json").body(
        if case["request_body_encoding"] == "raw-invalid-json" {
            case["request_payload"]
                .as_str()
                .unwrap()
                .as_bytes()
                .to_vec()
        } else {
            serde_json::to_vec(&case["request_payload"]).unwrap()
        },
    );
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let headers: Value = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap())))
        .collect::<serde_json::Map<_, _>>()
        .into();
    let raw = response.bytes().await.unwrap().to_vec();
    assert!(raw.len() <= 65536);
    let parsed = serde_json::from_slice(&raw).ok();
    (status, headers, raw, parsed)
}
#[tokio::test]
#[ignore = "requires exact reviewed owned Native mutation Source48 inputs/DB/Redis and fresh report"]
async fn mutations_match_retained_source48_and_native_order_controls() {
    assert_eq!(
        std::env::var("NOTIFICATION_NATIVE_FIXTURE").as_deref(),
        Ok("owned-native-only")
    );
    let expected = std::env::var("NOTIFICATION_NATIVE_DATABASE").unwrap();
    let suffix = expected.strip_prefix("rust_probe_notif_native_").unwrap();
    assert_eq!(suffix.len(), 10);
    assert!(suffix
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    let report_path = PathBuf::from(std::env::var("NOTIFICATION_NATIVE_REPORT").unwrap());
    assert!(!report_path.exists());
    let source_bytes =
        std::fs::read(std::env::var("NOTIFICATION_MUTATION_SOURCE48").unwrap()).unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(&source_bytes)), SOURCE_SHA);
    let source: Value = serde_json::from_slice(&source_bytes).unwrap();
    assert_eq!(source["actual_request_count"], 48);
    assert_eq!(source["seed_binding_passed"], true);
    let database_url = std::env::var("DATABASE_TEST_URL").unwrap();
    let guard = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&guard)
            .await
            .unwrap(),
        expected
    );
    guard.close().await;
    assert!(matches!(
        initialize_empty_database(&database_url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let store = Store::connect(&database_url, 1).await.unwrap();
    sqlx::query("SET statement_timeout='5s'")
        .execute(store.pool())
        .await
        .unwrap();
    let initial = snapshot(&store).await;
    let mut report = json!({"passed":false,"database":expected,"Source48_raw_sha256":SOURCE_SHA,"scope":"Native credentials/router versus retained forced-active-actor Source48; no new Source app or full endpoint acceptance","initial_state":initial,"cases":[],"extra_cases":[],"final500_renderer_gap_predeclared":true,"source_framework_exception":source["requests"][47]["framework_exception"],"clock_mapping":[]});
    save(&report_path, &report);
    binding(&store, &expected).await;
    let mut tx = store.pool().begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        expected
    );
    sqlx::query("DELETE FROM core_notificationdismissal")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_systemnotification")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let empty = snapshot(&store).await;
    assert!(empty["tables"]["core_systemnotification"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(empty["tables"]["core_notificationdismissal"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(empty["tables"]["accounts_user"]
        .as_array()
        .unwrap()
        .is_empty());
    report["isolated_state"] = empty;
    save(&report_path, &report);
    let mut keys = BTreeMap::new();
    for actor in source["seed_users"].as_array().unwrap() {
        binding(&store, &expected).await;
        let role = actor["username"]
            .as_str()
            .unwrap()
            .split('_')
            .nth(1)
            .unwrap()
            .to_owned();
        let key = format!("notifmut-native-{role}-{suffix}");
        sqlx::query("INSERT INTO accounts_user(id,password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,avatar_config,user_level,custom_properties,api_key,stream_limit) VALUES($1,'!',NULL,false,$2,'','','',false,true,$3,NULL,$4,$5,$6,0)")
            .bind(actor["id"].as_i64().unwrap()).bind(actor["username"].as_str().unwrap()).bind(chrono::Utc::now()).bind(actor["user_level"].as_i64().unwrap()as i32).bind(&actor["custom_properties"]).bind(&key).execute(store.pool()).await.unwrap();
        keys.insert(role, key);
    }
    for row in source["seed_rows"]["notifications"].as_array().unwrap() {
        binding(&store, &expected).await;
        sqlx::query("INSERT INTO core_systemnotification(id,notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)")
        .bind(row["id"].as_i64().unwrap()).bind(row["notification_key"].as_str().unwrap()).bind(row["notification_type"].as_str().unwrap()).bind(row["priority"].as_str().unwrap()).bind(row["source"].as_str().unwrap()).bind(row["title"].as_str().unwrap()).bind(row["message"].as_str().unwrap()).bind(&row["action_data"]).bind(row["is_active"].as_bool().unwrap()).bind(row["admin_only"].as_bool().unwrap()).bind(row["expires_at"].as_str().map(|_|clock(&row["expires_at"]))).bind(clock(&row["created_at"])).bind(clock(&row["updated_at"])).execute(store.pool()).await.unwrap();
    }
    for row in source["seed_rows"]["dismissals"].as_array().unwrap() {
        binding(&store, &expected).await;
        sqlx::query("INSERT INTO core_notificationdismissal(id,user_id,notification_id,dismissed_at,action_taken) VALUES($1,$2,$3,$4,$5)").bind(row["id"].as_i64().unwrap()).bind(row["user_id"].as_i64().unwrap()).bind(row["notification_id"].as_i64().unwrap()).bind(clock(&row["dismissed_at"])).bind(row["action_taken"].as_str()).execute(store.pool()).await.unwrap();
    }
    for table in [
        "accounts_user",
        "core_systemnotification",
        "core_notificationdismissal",
    ] {
        binding(&store, &expected).await;
        let seq = &source["seed_audit"]["sequences"][format!("public.{table}_id_seq")];
        sqlx::query(&format!("SELECT setval('{table}_id_seq',$1,$2)"))
            .bind(seq["last_value"].as_i64().unwrap())
            .bind(seq["is_called"].as_bool().unwrap())
            .execute(store.pool())
            .await
            .unwrap();
    }
    let seeded = snapshot(&store).await;
    let mut clocks = BTreeMap::new();
    for (kind, table) in [
        ("notifications", "core_systemnotification"),
        ("dismissals", "core_notificationdismissal"),
    ] {
        compare_values(
            &source["seed_rows"][kind],
            &seeded["tables"][table],
            &source["requests"][0],
            chrono::Utc::now(),
            chrono::Utc::now(),
            &mut clocks,
            table,
        );
        assert_eq!(
            seeded["sequences"][table],
            source["seed_audit"]["sequences"][format!("public.{table}_id_seq")]
        );
    }
    assert!(clocks.is_empty());
    report["seeded_state"] = seeded;
    report["seeded_credentials"] = credentials(&store).await;
    save(&report_path, &report);
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut redis = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let marker: Option<String> = redis::cmd("GET")
        .arg("rust-probe-notification-native-owner")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(marker.as_deref(), Some(expected.as_str()));
    report["cache_before_seed"] = cache_snapshot(&mut redis, &source, &expected).await;
    for alias in ["H", "V"] {
        let id = source["seed_aliases"][alias].as_i64().unwrap();
        let admin = source["seed_binding"]["actors"]["admin"].as_i64().unwrap();
        let key = format!(":1:dev_notif_condition_{id}_{admin}");
        let absent: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&key)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(absent.is_none());
        let row = source["cache_after_setup"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| unhex(r["key_hex"].as_str().unwrap()) == key.as_bytes())
            .unwrap();
        let _: () = redis::cmd("RESTORE")
            .arg(&key)
            .arg(300000)
            .arg(unhex(row["dump_hex"].as_str().unwrap()))
            .query_async(&mut redis)
            .await
            .unwrap();
    }
    let u = format!(
        ":1:dev_notif_condition_{}_{}",
        source["seed_aliases"]["U"].as_i64().unwrap(),
        source["seed_binding"]["actors"]["admin"].as_i64().unwrap()
    );
    let absent: Option<Vec<u8>> = redis::cmd("GET")
        .arg(&u)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(absent.is_none());
    report["cache_after_seed"] = cache_snapshot(&mut redis, &source, &expected).await;
    assert_named_cache(
        &report["cache_after_seed"],
        &json!({"H":false,"V":true,"U":null,"D":null}),
        &expected,
    );
    save(&report_path, &report);
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("notifmut-native-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache);
    let router = auth.router();
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
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let base = format!("http://{address}");
    let mut cases = source["requests"].as_array().unwrap().clone();
    let mut extra = source["requests"][47].clone();
    extra["path"] = json!("/api/core/notifications/not-an-integer/");
    extra["request_payload"] = json!("{");
    extra["request_body_encoding"] = json!("raw-invalid-json");
    for (role, method, status, name) in [
        (
            "admin",
            "PATCH",
            500,
            "native-poison-before-malformed-pk-body",
        ),
        ("standard", "PATCH", 403, "native-standard-before-poison"),
        ("anonymous", "DELETE", 401, "native-anonymous-before-poison"),
    ] {
        let mut c = extra.clone();
        c["role"] = json!(role);
        c["method"] = json!(method);
        c["actual_status"] = json!(status);
        c["name"] = json!(name);
        cases.push(c);
    }
    for (index, case) in cases.iter().enumerate() {
        binding(&store, &expected).await;
        let before = snapshot(&store).await;
        let before_credentials = credentials(&store).await;
        let before_cache = cache_snapshot(&mut redis, &source, &expected).await;
        let began = chrono::Utc::now();
        let group = if index < 48 { "cases" } else { "extra_cases" };
        report[group].as_array_mut().unwrap().push(json!({"index":index,"name":case["name"],"role":case["role"],"method":case["method"],"path":case["path"],"request_payload":case["request_payload"],"before":before,"before_credentials":before_credentials,"before_cache":before_cache,"began_utc":began,"request_in_progress":true,"capture_complete":false}));
        save(&report_path, &report);
        assert_eq!(before_cache["named"]["sentinel"]["value"], expected);
        let (status, headers, raw, parsed) = call(&client, &base, case, &keys).await;
        let completed = chrono::Utc::now();
        let after = snapshot(&store).await;
        let after_credentials = credentials(&store).await;
        let after_cache = cache_snapshot(&mut redis, &source, &expected).await;
        let record = json!({"index":index,"name":case["name"],"role":case["role"],"method":case["method"],"path":case["path"],"request_payload":case["request_payload"],"request_body_encoding":case["request_body_encoding"],"status":status,"headers":headers,"body_hex":hex(&raw),"body_sha256":format!("{:x}",Sha256::digest(&raw)),"body_bytes":raw.len(),"json":parsed,"began_utc":began,"completed_utc":completed,"before":before,"after":after,"before_credentials":before_credentials,"after_credentials":after_credentials,"before_cache":before_cache,"after_cache":after_cache});
        *report[group].as_array_mut().unwrap().last_mut().unwrap() = record;
        report[group].as_array_mut().unwrap().last_mut().unwrap()["capture_complete"] = json!(true);
        save(&report_path, &report);
        assert_eq!(status, case["actual_status"].as_u64().unwrap() as u16);
        if index == 47 || index == 48 {
            assert_eq!(
                parsed.as_ref().unwrap(),
                &json!({"detail":"Notification processing failed."})
            );
        }
        assert_eq!(before["sequences"], after["sequences"]);
        assert_eq!(before_credentials, after_credentials);
        let source_case = &source["requests"][if index < 48 { index } else { 47 }];
        let before_values = if index < 48 {
            &source_case["before_condition_cache"]
        } else {
            &source_case["after_condition_cache"]
        };
        assert_named_cache(&before_cache, before_values, &expected);
        assert_named_cache(
            &after_cache,
            &source_case["after_condition_cache"],
            &expected,
        );
        for table in TABLES {
            if !["core_systemnotification", "core_notificationdismissal"].contains(&table) {
                assert_eq!(before["tables"][table], after["tables"][table]);
            }
        }
        if index < 48 {
            if status == 204 {
                assert!(raw.is_empty());
                assert!(parsed.is_none());
            } else if index != 47 {
                compare_values(
                    &case["json"],
                    parsed.as_ref().unwrap(),
                    case,
                    began,
                    completed,
                    &mut clocks,
                    "response",
                );
            }
            for (kind, table) in [
                ("notifications", "core_systemnotification"),
                ("dismissals", "core_notificationdismissal"),
            ] {
                compare_values(
                    &case["after"][kind],
                    &after["tables"][table],
                    case,
                    began,
                    completed,
                    &mut clocks,
                    table,
                );
                assert_eq!(
                    after["sequences"][table],
                    case["after_named_audit"]["sequences"][format!("public.{table}_id_seq")]
                );
            }
        } else {
            assert_eq!(before, after);
        }
        report["clock_mapping"] = json!(clocks
            .iter()
            .map(|(s, n)| json!({"Source":s,"Native":n}))
            .collect::<Vec<_>>());
        save(&report_path, &report);
    }
    report["final_credentials"] = credentials(&store).await;
    report["final_cache"] = cache_snapshot(&mut redis, &source, &expected).await;
    drop(client);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    drop(redis);
    store.pool().close().await;
    report["owned_server_aborted_and_pool_closed"] = json!(true);
    report["passed"] = json!(true);
    save(&report_path, &report);
}

#[test]
fn mapped_generated_clock_cannot_rewind_to_source() {
    let s = json!("2026-11-12T10:00:00Z");
    let n = json!("2026-11-12T10:00:01Z");
    let context = json!({"began_utc":s,"completed_utc":s});
    let mut map = BTreeMap::new();
    map.insert(clock(&s), clock(&n));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        compare_values(
            &s,
            &s,
            &context,
            clock(&n),
            clock(&n),
            &mut map,
            "/updated_at",
        )
    }));
    assert!(result.is_err());
}
#[test]
fn coincident_generated_clock_is_registered_and_retained() {
    let s = json!("2026-11-12T10:00:00Z");
    let context = json!({"began_utc":s,"completed_utc":s});
    let mut map = BTreeMap::new();
    compare_values(
        &s,
        &s,
        &context,
        clock(&s),
        clock(&s),
        &mut map,
        "/updated_at",
    );
    assert_eq!(map.get(&clock(&s)), Some(&clock(&s)));
}
