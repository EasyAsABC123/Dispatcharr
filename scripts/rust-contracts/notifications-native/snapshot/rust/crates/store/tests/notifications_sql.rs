//! Unexecuted Native QA for two retained Original notification observations.
//! Original Dispatcharr contributors retain application/algorithm credit.
//! Raw Source clocks remain in the fixture. Native clocks/rows/bodies are saved
//! before assertions; comparisons below never replace captured timestamps.
use dispatcharr_store::{
    initialize_empty_database, AuthConfig, AuthService, NotificationCache, SchemaInitialization,
    Store,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
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
fn clock(value: &Value) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339(value.as_str().expect("timestamp string"))
        .unwrap()
        .with_timezone(&chrono::Utc)
}
fn assert_rows(actual: &Value, source: &Value, dynamic_id: Option<i64>) {
    let actual = actual.as_array().unwrap();
    let source = source.as_array().unwrap();
    assert_eq!(actual.len(), source.len());
    for (a, b) in actual.iter().zip(source) {
        assert_eq!(a.as_object().unwrap().len(), b.as_object().unwrap().len());
        for (key, expected) in b.as_object().unwrap() {
            if ["created_at", "updated_at", "dismissed_at"].contains(&key.as_str()) {
                // Newly generated native clocks have their own declared bounds.
                // Seeded clock values must retain the exact Source instant.
                if Some(a["id"].as_i64().unwrap()) != dynamic_id {
                    assert_eq!(clock(&a[key]), clock(expected));
                }
            } else {
                assert_eq!(&a[key], expected, "SQL field {key}");
            }
        }
    }
}
async fn snapshot(store: &Store) -> Value {
    let mut tables = BTreeMap::new();
    let mut sequences = BTreeMap::new();
    for table in TABLES {
        let value: Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY id),'[]'::jsonb) FROM {table} t"
        ))
        .fetch_one(store.pool())
        .await
        .unwrap();
        tables.insert(table, value);
        let sequence:Value=sqlx::query_scalar(&format!("SELECT jsonb_build_object('last_value',last_value,'is_called',is_called) FROM {table}_id_seq")).fetch_one(store.pool()).await.unwrap();
        sequences.insert(table, sequence);
    }
    json!({"tables":tables,"sequences":sequences})
}
fn save(path: &std::path::Path, report: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(report).unwrap()).unwrap();
}
fn notification_rows(state: &Value) -> Value {
    state["tables"]["core_systemnotification"].clone()
}
fn dismissal_rows(state: &Value) -> Value {
    state["tables"]["core_notificationdismissal"].clone()
}
async fn request(
    client: &reqwest::Client,
    base: &str,
    case: &Value,
    keys: &BTreeMap<&str, String>,
) -> (u16, Value, Value, Vec<u8>) {
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
    if ["POST", "PUT", "PATCH"].contains(&case["method"].as_str().unwrap()) {
        request = request
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&case["request_payload"]).unwrap());
    }
    let response = request.send().await.unwrap();
    let status = response.status().as_u16();
    let headers: Value = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap())))
        .collect::<serde_json::Map<_, _>>()
        .into();
    let bytes = response.bytes().await.unwrap().to_vec();
    let value = serde_json::from_slice(&bytes).unwrap();
    (status, headers, value, bytes)
}
#[tokio::test]
#[ignore = "requires reviewed owned empty native notification PostgreSQL/Redis fixture and new report path"]
async fn create_and_sequential_repeat_match_retained_source_effects() {
    assert_eq!(
        std::env::var("NOTIFICATION_NATIVE_FIXTURE").as_deref(),
        Ok("owned-native-only")
    );
    let expected = std::env::var("NOTIFICATION_NATIVE_DATABASE").unwrap();
    let suffix = expected
        .strip_prefix("rust_probe_notif_native_")
        .expect("owned database prefix");
    assert_eq!(suffix.len(), 10);
    assert!(suffix
        .bytes()
        .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)));
    let report_path = PathBuf::from(std::env::var("NOTIFICATION_NATIVE_REPORT").unwrap());
    assert!(!report_path.exists(), "new report required");
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
    let source: Value =
        serde_json::from_str(include_str!("fixtures/notification-source8.json")).unwrap();
    let mut report = json!({"passed":false,"scope":"Native-only router/SQL checks against retained Source framework evidence, not paired Source credentials or TCP","database":expected,"Source_fixture_raw_sha256":format!("{:x}",Sha256::digest(include_bytes!("fixtures/notification-source8.json"))),"Source_controller_exit_preserved":1,"normalization":"none: raw Source and Native clocks retained; only declared own-clock comparisons differ","cases":[],"extra_cases":[]});
    save(&report_path, &report);
    // Source explicitly isolates notices/dismissals inside its owned database.
    // Native initialization legitimately includes a developer notice. Retain
    // those initial rows, then clear only this guarded target before fixed IDs.
    let initialized = snapshot(&store).await;
    report["native_initialized_notification_rows_before_isolation"] = json!({
        "notifications":notification_rows(&initialized),
        "dismissals":dismissal_rows(&initialized)
    });
    report["source_initial_notification_rows_before_isolation"] =
        source["owned_rows_before_isolation"].clone();
    save(&report_path, &report);
    let mut isolation = store.pool().begin().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(&mut *isolation)
            .await
            .unwrap(),
        expected
    );
    sqlx::query("DELETE FROM core_notificationdismissal")
        .execute(&mut *isolation)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core_systemnotification")
        .execute(&mut *isolation)
        .await
        .unwrap();
    isolation.commit().await.unwrap();
    let isolated = snapshot(&store).await;
    report["owned_notification_rows_after_isolation"] = json!({
        "notifications":notification_rows(&isolated),
        "dismissals":dismissal_rows(&isolated)
    });
    report["isolation_database"] = json!(expected);
    save(&report_path, &report);
    assert_eq!(
        report["owned_notification_rows_after_isolation"],
        json!({"notifications":[],"dismissals":[]})
    );
    assert_eq!(
        source["owned_rows_before_isolation"],
        json!({"notifications":[],"dismissals":[]})
    );
    let mut keys = BTreeMap::new();
    for (index, role) in ["standard", "admin"].into_iter().enumerate() {
        let actor = &source["seed_users"][index];
        let key = format!("notification-native-{role}-{suffix}");
        sqlx::query("INSERT INTO accounts_user(id,password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,avatar_config,user_level,custom_properties,api_key,stream_limit) VALUES($1,'!',NULL,false,$2,'','','',false,true,$3,NULL,$4,'{}'::jsonb,$5,0)")
            .bind(actor["id"].as_i64().unwrap()).bind(actor["username"].as_str().unwrap()).bind(chrono::Utc::now()).bind(actor["user_level"].as_i64().unwrap() as i32).bind(&key).execute(store.pool()).await.unwrap();
        keys.insert(role, key);
    }
    sqlx::query("SELECT setval('accounts_user_id_seq',$1,true)")
        .bind(source["seed"]["admin_user_id"].as_i64().unwrap())
        .execute(store.pool())
        .await
        .unwrap();
    let notice = &source["seed_rows"]["notifications"][0];
    sqlx::query("INSERT INTO core_systemnotification(id,notification_key,notification_type,priority,source,title,message,action_data,is_active,admin_only,expires_at,created_at,updated_at) VALUES($1,'test.notif','info','normal','system','Hello','World','{}'::jsonb,true,false,NULL,$2,$3)")
        .bind(notice["id"].as_i64().unwrap()).bind(clock(&notice["created_at"])).bind(clock(&notice["updated_at"])).execute(store.pool()).await.unwrap();
    // Declare a copied fixture starting position BEFORE request observation.
    // No sequences are reset inside the eight requests or repeat checks.
    sqlx::query("SELECT setval('core_systemnotification_id_seq',$1,true)")
        .bind(notice["id"].as_i64().unwrap())
        .execute(store.pool())
        .await
        .unwrap();
    let seeded = snapshot(&store).await;
    report["native_seed_notification_rows_before_requests"] = json!({
        "notifications":notification_rows(&seeded),
        "dismissals":dismissal_rows(&seeded)
    });
    report["source_seed_notification_rows"] = source["seed_rows"].clone();
    report["native_seed_notice_sequence"] = seeded["sequences"]["core_systemnotification"].clone();
    save(&report_path, &report);
    assert_rows(
        &notification_rows(&seeded),
        &source["seed_rows"]["notifications"],
        None,
    );
    assert_rows(
        &dismissal_rows(&seeded),
        &source["seed_rows"]["dismissals"],
        None,
    );
    assert_eq!(
        report["native_seed_notice_sequence"],
        source["setup_after_seed_named_audit"]["sequences"]
            ["public.core_systemnotification_id_seq"]
    );
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut ownership = redis::Client::open(redis_url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let marker: Option<String> = redis::cmd("GET")
        .arg("rust-probe-notification-native-owner")
        .query_async(&mut ownership)
        .await
        .unwrap();
    assert_eq!(
        marker.as_deref(),
        Some(expected.as_str()),
        "owned Redis sentinel required before cache use"
    );
    drop(ownership);
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("notification-native-fixture-only".into()).unwrap(),
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
    let mut dismissal_time = None;
    for (index, case) in source["requests"].as_array().unwrap().iter().enumerate() {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT current_database()")
                .fetch_one(store.pool())
                .await
                .unwrap(),
            expected
        );
        let before = snapshot(&store).await;
        let began = chrono::Utc::now();
        let (status, headers, body, raw) = request(&client, &base, case, &keys).await;
        let completed = chrono::Utc::now();
        let after = snapshot(&store).await;
        report["cases"].as_array_mut().unwrap().push(json!({"name":case["name"],"status":status,"headers":headers,"body":body,"body_hex":raw.iter().map(|b|format!("{b:02x}")).collect::<String>(),"body_sha256":format!("{:x}",Sha256::digest(&raw)),"began_utc":began,"completed_utc":completed,"before":before,"after":after}));
        save(&report_path, &report);
        assert_eq!(status, case["actual_status"].as_u64().unwrap() as u16);
        if index == 7 {
            assert_eq!(
                body.as_object().unwrap().len(),
                case["json"].as_object().unwrap().len()
            );
            for (key, value) in case["json"].as_object().unwrap() {
                if key != "created_at" {
                    assert_eq!(&body[key], value);
                }
            }
            let observed = clock(&body["created_at"]);
            assert!(began <= observed && observed <= completed);
            assert!(!headers.as_object().unwrap().contains_key("location"));
        } else {
            assert_eq!(body, case["json"]);
        }
        assert_rows(
            &notification_rows(&after),
            &case["after"]["notifications"],
            if index == 7 { Some(2) } else { None },
        );
        assert_rows(
            &dismissal_rows(&after),
            &case["after"]["dismissals"],
            if index >= 3 { Some(1) } else { None },
        );
        if index == 3 {
            let t = clock(&after["tables"]["core_notificationdismissal"][0]["dismissed_at"]);
            assert!(began <= t && t <= completed);
            dismissal_time = Some(t);
        }
        if index >= 4 {
            assert_eq!(
                clock(&after["tables"]["core_notificationdismissal"][0]["dismissed_at"]),
                dismissal_time.unwrap()
            );
        }
        let mutable = if index == 3 {
            Some("core_notificationdismissal")
        } else if index == 7 {
            Some("core_systemnotification")
        } else {
            None
        };
        for table in TABLES {
            if Some(table) != mutable {
                assert_eq!(before["tables"][table], after["tables"][table]);
                assert_eq!(before["sequences"][table], after["sequences"][table]);
            }
        }
        for table in ["core_systemnotification", "core_notificationdismissal"] {
            let name = format!("public.{table}_id_seq");
            assert_eq!(
                after["sequences"][table],
                case["after_named_audit"]["sequences"][name]
            );
        }
        if index == 7 {
            let row = &after["tables"]["core_systemnotification"][1];
            assert_eq!(clock(&row["created_at"]), clock(&body["created_at"]));
            assert!(began <= clock(&row["updated_at"]) && clock(&row["updated_at"]) <= completed);
        }
    }
    // Additional definition-derived Native controls, explicitly outside Source8.
    let original_create = source["requests"][7].clone();
    let mut extra = vec![];
    extra.push((
        "duplicate-key-no-sequence",
        original_create.clone(),
        400u16,
        false,
    ));
    let mut invalid = original_create.clone();
    invalid["request_payload"] = json!({});
    extra.push(("required-fields-no-mutation", invalid.clone(), 400, false));
    invalid["role"] = json!("standard");
    invalid["request_payload"] = json!([]);
    extra.push((
        "standard-permission-before-invalid-body",
        invalid,
        403,
        false,
    ));
    for (label, payload, change) in [
        (
            "repeat-truthy-update-only",
            json!({"action_taken":"changed"}),
            true,
        ),
        (
            "repeat-falsey-retains-action",
            json!({"action_taken":false}),
            false,
        ),
    ] {
        let mut case = source["requests"][4].clone();
        case["request_payload"] = payload;
        extra.push((label, case, 200, change));
    }
    for (method, label) in [
        ("PUT", "other-crud-put-stays-guarded"),
        ("PATCH", "other-crud-patch-stays-guarded"),
        ("DELETE", "other-crud-delete-stays-guarded"),
    ] {
        let mut case = original_create.clone();
        case["method"] = json!(method);
        case["path"] = json!("/api/core/notifications/1/");
        case["request_payload"] = json!({});
        extra.push((label, case, 501, false));
    }
    for (label, case, expected_status, action_update) in extra {
        let before = snapshot(&store).await;
        let began = chrono::Utc::now();
        let (status, headers, body, raw) = request(&client, &base, &case, &keys).await;
        let completed = chrono::Utc::now();
        let after = snapshot(&store).await;
        report["extra_cases"].as_array_mut().unwrap().push(json!({"name":label,"definition_derived_not_new_Source":true,"status":status,"headers":headers,"body":body,"body_hex":raw.iter().map(|b|format!("{b:02x}")).collect::<String>(),"began_utc":began,"completed_utc":completed,"before":before,"after":after}));
        save(&report_path, &report);
        assert_eq!(status, expected_status);
        assert_eq!(before["sequences"], after["sequences"]);
        if action_update {
            let mut expected = before.clone();
            expected["tables"]["core_notificationdismissal"][0]["action_taken"] = json!("changed");
            assert_eq!(expected, after);
        } else {
            assert_eq!(before, after);
        }
    }
    drop(client);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    store.pool().close().await;
    report["passed"] = json!(true);
    report["owned_server_aborted_and_pool_closed"] = json!(true);
    save(&report_path, &report);
}
