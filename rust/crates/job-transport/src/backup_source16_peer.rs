//! Actual Native Source16 credential/router/worker/archive capture. No Source app.
//! Original Dispatcharr contributors retain application credit and licensing.
use super::*;
use crate::{
    transport::Config,
    worker::{ArchiveHandler, Run},
};
use chrono::{Datelike, Timelike, Utc};
use dispatcharr_backups::{Error as ArchiveError, SourceClock};
use dispatcharr_store::{
    initialize_empty_database, AuthConfig, AuthService, NotificationCache, SchemaInitialization,
    Store,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
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
async fn state(store: &Store, urls: &[String; 3], root: &Path) -> Value {
    let mut redis = Vec::new();
    for (db, url) in urls.iter().enumerate() {
        redis.push(redis_snapshot(url, (db + 1) as u8).await);
    }
    let mut files = Vec::new();
    if root.exists() {
        for entry in std::fs::read_dir(root).unwrap() {
            let p = entry.unwrap().path();
            assert!(p.is_file() && !p.is_symlink());
            assert!(p.metadata().unwrap().len() <= 8388608);
            let bytes = std::fs::read(&p).unwrap();
            files.push(json!({"name":p.file_name().unwrap().to_str().unwrap(),"bytes":bytes.len(),"sha256":hash(&bytes)}));
        }
    }
    files.sort_by_key(|x| x["name"].as_str().unwrap().to_owned());
    json!({"SQL":snapshot(store).await,"users":rows(store,"accounts_user").await,"events":rows(store,"core_systemevent").await,"redis":redis,"files":files})
}
struct OwnUtcClock;
impl SourceClock for OwnUtcClock {
    fn filename_timestamp(&self) -> Result<String, ArchiveError> {
        Ok(Utc::now().format("%Y.%m.%d.%H.%M.%S").to_string())
    }
    fn created_at_utc(&self) -> Result<String, ArchiveError> {
        Ok(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, false))
    }
    fn zip_timestamp(&self, time: SystemTime) -> Result<zip::DateTime, ArchiveError> {
        let dt: chrono::DateTime<Utc> = time.into();
        zip::DateTime::from_date_and_time(
            dt.year().try_into().map_err(|_| ArchiveError::Metadata)?,
            dt.month() as u8,
            dt.day() as u8,
            dt.hour() as u8,
            dt.minute() as u8,
            dt.second() as u8,
        )
        .map_err(|_| ArchiveError::Metadata)
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "reviewed owned Native Source16 PG/Redis/archive fixture only; no Source/Main"]
async fn source16_real_credentials_router_and_worker_capture() {
    assert_eq!(
        std::env::var("BACKUP_SOURCE16_NATIVE").as_deref(),
        Ok("owned-native-only")
    );
    let database = std::env::var("POSTGRES_DB").unwrap();
    let nonce = database
        .strip_prefix("rust_probe_backupjob_native_")
        .unwrap();
    assert_eq!(nonce.len(), 10);
    assert!(nonce
        .bytes()
        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
    let input = PathBuf::from(std::env::var("BACKUP_SOURCE16_INPUTS").unwrap());
    let output = PathBuf::from(std::env::var("BACKUP_SOURCE16_OUTPUT").unwrap());
    assert!(output.is_dir());
    let report_path = output.join("native-source16.json");
    assert!(!report_path.exists());
    let source_bytes = std::fs::read(input.join("Source16-projection.json")).unwrap();
    assert_eq!(
        hash(&source_bytes),
        "5f0ce76f0d308da14cce1c0f57899577dd14732e1bc818ee916864d2d1902760"
    );
    let source: Value = serde_json::from_slice(&source_bytes).unwrap();
    let plan: Value =
        serde_json::from_slice(&std::fs::read(input.join("plan.json")).unwrap()).unwrap();
    let binding: Value =
        serde_json::from_slice(&std::fs::read(input.join("Source-binding.json")).unwrap()).unwrap();
    assert_eq!(binding["mode"], "projected-backup-source16-reference-v1");
    assert_eq!(
        binding["Original_raw_sha256"],
        "daa2835e76df10e6dece88473d4b08d2ce1e4ad82680de3296de99604b54a288"
    );
    assert_eq!(binding["Source16_projection_sha256"], hash(&source_bytes));
    assert_eq!(
        binding["plan_sha256"],
        hash(&std::fs::read(input.join("plan.json")).unwrap())
    );
    assert_eq!(plan["cases"].as_array().unwrap().len(), 16);
    let url = std::env::var("DATABASE_URL").unwrap();
    let guard = Store::connect(&url, 1).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_database()")
            .fetch_one(guard.pool())
            .await
            .unwrap(),
        database
    );
    guard.pool().close().await;
    drop(guard);
    assert!(matches!(
        initialize_empty_database(&url).await.unwrap(),
        SchemaInitialization::Created { .. }
    ));
    let store = Store::connect(&url, 4).await.unwrap();
    let urls: [String; 3] = [
        std::env::var("BACKUP_CACHE_URL").unwrap(),
        std::env::var("CELERY_BROKER_URL").unwrap(),
        std::env::var("CELERY_RESULT_BACKEND").unwrap(),
    ];
    for (db, url) in urls.iter().enumerate() {
        let c = redis::Client::open(url.as_str()).unwrap();
        let i = c.get_connection_info();
        assert_eq!(i.redis.db, (db + 1) as i64);
        assert_eq!(i.redis.password.as_deref(), Some("playback-fixture-only"));
        assert!(
            matches!(&i.addr,redis::ConnectionAddr::Tcp(host,port)if host=="127.0.0.1"&&*port==6379)
        );
    }
    let backup_root = PathBuf::from(format!("/data/backups/{database}"));
    let scratch_root = PathBuf::from(format!("/data/scratch/{database}"));
    assert!(!backup_root.exists());
    let mut report = json!({"capture_complete":false,"passed":false,"capture_only_passed_field":true,"Source_application_runs":0,"Main_application_runs":0,"Source_reference_mode":binding["mode"],"Source_reference_sha256":hash(&source_bytes),"Original_Source_raw_sha256":binding["Original_raw_sha256"],"database":database,"actual_request_count":0,"requests":[],"initial":state(&store,&urls,&backup_root).await,"native_actual_queue":"celery","Source_fixture_queue_override":source["before_Celery_configuration"]["owned_queue"],"fixture_UTC_clock_only":true});
    save(&report_path, &report);
    sqlx::query("INSERT INTO core_coresettings(key,name,value)VALUES('network_access','Network Access',$1)ON CONFLICT(key)DO UPDATE SET value=EXCLUDED.value").bind(json!({"UI":"127.0.0.0/8"})).execute(store.pool()).await.unwrap();
    assert_eq!(rows(&store, "accounts_user").await, json!([]));
    let seed_clock = Utc::now().to_rfc3339();
    let mut users = BTreeMap::new();
    for role in plan["roles"].as_array().unwrap() {
        let alias = role["alias"].as_str().unwrap();
        let mut user = source["seed_users"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["id"] == role["id"])
            .unwrap()
            .clone();
        user["username"] = json!(format!("rust-probe-backupjob-native-{nonce}-{alias}"));
        user["api_key"] = json!(format!("backup-native-{nonce}-{alias}"));
        user["password"] = json!("!");
        user["date_joined"] = json!(seed_clock);
        sqlx::query(
            "INSERT INTO accounts_user SELECT * FROM jsonb_populate_record(NULL::accounts_user,$1)",
        )
        .bind(&user)
        .execute(store.pool())
        .await
        .unwrap();
        users.insert(alias.to_owned(), user);
    }
    sqlx::query("SELECT setval('accounts_user_id_seq',3,true)")
        .execute(store.pool())
        .await
        .unwrap();
    // Match only the reviewed Source fixture's initial named cache context.
    // No auth history, event, request effect or arbitrary Source key is reset.
    let seed_cache = &source["seed_snapshot"]["redis"][0];
    assert_eq!(seed_cache["database"], 1);
    let expected_keys = std::collections::BTreeSet::from([
        ":1:coresettings:groupver:network_access",
        "core:version_check_dispatch_lock",
        "dvr:recover_dispatch_lock",
    ]);
    let cache_client = redis::Client::open(urls[0].as_str()).unwrap();
    let mut seed_connection = cache_client
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let initial_cache_keys: u64 = redis::cmd("DBSIZE")
        .query_async(&mut seed_connection)
        .await
        .unwrap();
    assert_eq!(
        initial_cache_keys, 0,
        "FreshOwnedCacheBeforeSourceContextSeed"
    );
    let mut actual_keys = std::collections::BTreeSet::new();
    let mut seed_receipts = Vec::new();
    for row in seed_cache["rows"].as_array().unwrap() {
        let key = decode_hex(row["key_hex"].as_str().unwrap());
        let name = std::str::from_utf8(&key).unwrap().to_owned();
        assert!(expected_keys.contains(name.as_str()));
        assert!(actual_keys.insert(name.clone()));
        assert_eq!(row["type"], "string");
        let dump = decode_hex(row["dump_hex"].as_str().unwrap());
        assert!(dump.len() <= 65536);
        let ttl = row["pttl"].as_i64().unwrap();
        if name == ":1:coresettings:groupver:network_access" {
            let version = decode_hex(row["value_hex"].as_str().unwrap());
            assert!(
                std::str::from_utf8(&version)
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
                    > 0
            );
            assert_eq!(ttl, -1);
        } else {
            assert_eq!(row["value_hex"], "31");
            assert!((1..=300000).contains(&ttl));
        }
        let restore_ttl = if ttl == -1 { 0 } else { ttl };
        let restored: String = redis::cmd("RESTORE")
            .arg(&key)
            .arg(restore_ttl)
            .arg(&dump)
            .query_async(&mut seed_connection)
            .await
            .unwrap();
        assert_eq!(restored, "OK");
        let actual_ttl: i64 = redis::cmd("PTTL")
            .arg(&key)
            .query_async(&mut seed_connection)
            .await
            .unwrap();
        assert!(if ttl == -1 {
            actual_ttl == -1
        } else {
            0 < actual_ttl && actual_ttl <= ttl
        });
        let read: Option<Vec<u8>> = redis::cmd("DUMP")
            .arg(&key)
            .query_async(&mut seed_connection)
            .await
            .unwrap();
        assert_eq!(read.as_deref(), Some(dump.as_slice()));
        seed_receipts.push(json!({"key_hex":row["key_hex"],"Source_dump_sha256":hash(&dump),"restored_dump_sha256":hash(read.as_ref().unwrap()),"Source_seed_pttl":ttl,"RESTORE_ttl":restore_ttl,"actual_readback_pttl":actual_ttl,"restored":restored}));
    }
    assert_eq!(
        actual_keys,
        expected_keys.into_iter().map(str::to_owned).collect()
    );
    drop(seed_connection);
    report["Source_cache_context_seed"] = json!({"before_owned_cache_keys":0,"strict_three_key_allowlist":true,"before_any_request_or_auth_worker":true,"auth_history_or_events_reset":false,"rows":seed_receipts});
    report["seed"] = state(&store, &urls, &backup_root).await;
    report["seed_clock"] = json!(seed_clock);
    save(&report_path, &report);
    let handler = ArchiveHandler::new_configured(
        dispatcharr_backups::PgConfig {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "dispatch".into(),
            database: database.clone(),
            password: Some("playback-fixture-only".into()),
            options: BTreeMap::new(),
        },
        dispatcharr_backups::Runtime {
            pg_dump: PathBuf::from("/usr/lib/postgresql/17/bin/pg_dump"),
            pg_restore: PathBuf::from("/usr/lib/postgresql/17/bin/pg_restore"),
            psql: PathBuf::from("/usr/bin/psql"),
        },
        backup_root.clone(),
        scratch_root,
        Arc::new(OwnUtcClock),
        dispatcharr_backups::Limits {
            database_bytes: 67108864,
            archive_bytes: 8388608,
            command_timeout: Duration::from_secs(90),
            ..Default::default()
        },
    )
    .unwrap();
    let app = BackupApplication::prepare(&urls[1], &urls[2], Arc::new(handler))
        .await
        .unwrap();
    let cache = NotificationCache::connect(&urls[0]).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("backup-job-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(cache);
    let files = app.attach(
        BackupFiles::new(auth, backup_root.clone(), "backup-job-fixture-only".into()).unwrap(),
    );
    let retained_files = files.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            files
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
    let mut created_id = String::new();
    let mut created_token = String::new();
    let unknown = format!("never-created-{nonce}");
    let mut filename = String::new();
    for spec in plan["cases"].as_array().unwrap() {
        let ordinal = spec["ordinal"].as_u64().unwrap();
        if ordinal == 13 {
            report["worker_began_utc"] = json!(Utc::now().to_rfc3339());
            save(&report_path, &report);
            let run = tokio::time::timeout(Duration::from_secs(90), app.run_once(Arc::new(())))
                .await
                .unwrap()
                .unwrap();
            report["worker_completed_utc"] = json!(Utc::now().to_rfc3339());
            report["worker_run"] = json!(format!("{run:?}"));
            assert!(matches!(run, Run::Completed(_)));
            let jobs = Jobs::connect_backup(&urls[1], &urls[2], Config::default())
                .await
                .unwrap();
            let meta = jobs
                .read_backup(&created_id, Arc::new(()))
                .await
                .unwrap()
                .unwrap();
            filename = meta.result["filename"].as_str().unwrap().to_owned();
            report["actual_result_metadata"] =
                json!({"task_id":meta.task_id,"result":meta.result,"date_done":meta.date_done});
            assert!(jobs.shutdown(Duration::from_secs(5)).await.is_ok());
            save(&report_path, &report);
        }
        let mut path = spec["path_template"]
            .as_str()
            .unwrap()
            .replace("{created_task}", &created_id)
            .replace("{unknown_task}", &unknown)
            .replace("{completed_filename}", &filename);
        if let Some(selector) = spec["token_selector"].as_str() {
            let token = match selector {
                "created_task_token" => created_token.clone(),
                "unknown_task_token" => backup_token(&unknown),
                "invalid_literal" => "invalid-fixture-token".into(),
                _ => panic!("UnknownFixedTokenSelector"),
            };
            path.push_str("?token=");
            path.push_str(&token);
        }
        let mut headers = BTreeMap::new();
        if let Some(alias) = spec["auth"]["x_api_key"].as_str() {
            headers.insert(
                "x-api-key".to_owned(),
                users[alias]["api_key"].as_str().unwrap().to_owned(),
            );
        }
        if let Some(literal) = spec["auth"]["bearer_literal"].as_str() {
            headers.insert("authorization".into(), format!("Bearer {literal}"));
        }
        let mut row = json!({"ordinal":ordinal,"name":spec["name"],"method":spec["method"],"path":path,"auth_selectors":spec["auth"],"raw_headers_private":headers.iter().map(|(k,v)|(k.clone(),json!(hex(v.as_bytes())))).collect::<Map<_,_>>(),"before":state(&store,&urls,&backup_root).await,"began_utc":Utc::now().to_rfc3339()});
        report["requests"].as_array_mut().unwrap().push(row.clone());
        save(&report_path, &report);
        let mut request = client
            .request(
                spec["method"]
                    .as_str()
                    .unwrap()
                    .parse::<reqwest::Method>()
                    .unwrap(),
                format!("http://{address}{path}"),
            )
            .header("accept", "application/json");
        for (k, v) in headers {
            request = request.header(k, v);
        }
        if let Some(raw) = spec["raw_body_ascii"].as_str() {
            request = request
                .header("content-type", spec["content_type"].as_str().unwrap())
                .body(raw.to_owned());
        }
        let mut response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let response_headers = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), json!(hex(v.as_bytes()))))
            .collect::<Map<_, _>>();
        assert!(serde_json::to_vec(&response_headers).unwrap().len() <= 65536);
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.unwrap() {
            assert!(bytes.len() + chunk.len() <= if ordinal == 16 { 8388608 } else { 65536 });
            bytes.extend_from_slice(&chunk);
        }
        let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        row["completed_utc"] = json!(Utc::now().to_rfc3339());
        row["status"] = json!(status);
        row["headers_hex"] = response_headers.into();
        row["body_hex"] = json!(hex(&bytes));
        row["body_bytes"] = json!(bytes.len());
        row["body_sha256"] = json!(hash(&bytes));
        row["json"] = parsed.clone();
        row["after"] = state(&store, &urls, &backup_root).await;
        *report["requests"]
            .as_array_mut()
            .unwrap()
            .last_mut()
            .unwrap() = row;
        report["actual_request_count"] = json!(ordinal);
        save(&report_path, &report);
        if ordinal == 4 {
            created_id = parsed["task_id"]
                .as_str()
                .expect("CannotInventTaskIDAfterFailedCreate")
                .to_owned();
            created_token = parsed["task_token"].as_str().unwrap().to_owned();
            assert_eq!(created_token, backup_token(&created_id));
        }
    }
    let before_guards = state(&store, &urls, &backup_root).await;
    let jobs = &app.inner.jobs;
    let id = uuid::Uuid::new_v4();
    assert_eq!(
        jobs.publish(
            &crate::protocol::Task::RestoreBackup {
                filename: "unexecuted.zip".into()
            },
            id,
            Arc::new(())
        )
        .await
        .unwrap_err(),
        crate::transport::Error::UnsupportedTask
    );
    assert_eq!(
        jobs.publish_index(
            &crate::index_protocol::IndexTask::Initial { source_id: 1 },
            id,
            Arc::new(())
        )
        .await
        .unwrap_err(),
        crate::transport::Error::UnsupportedTask
    );
    assert_eq!(
        jobs.read_index(id, Arc::new(())).await.unwrap_err(),
        crate::transport::Error::UnsupportedTask
    );
    assert_eq!(
        jobs.read_refresh(id, 1, Arc::new(())).await.unwrap_err(),
        crate::transport::Error::UnsupportedTask
    );
    assert_eq!(
        jobs.peek_series(Arc::new(())).await.unwrap_err(),
        crate::transport::Error::UnsupportedTask
    );
    report["native_only_family_guards"] = json!({"controls":5,"all_UnsupportedTask":true,"before":before_guards,"after":state(&store,&urls,&backup_root).await,"new_Source_requests":0});
    save(&report_path, &report);
    drop(client);
    assert!(stop.send(()).is_ok());
    assert!(tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
    report["HTTP_listener_joined"] = json!(true);
    report["backups_shutdown"] = json!(retained_files.shutdown(Duration::from_secs(15)).await);
    report["final"] = state(&store, &urls, &backup_root).await;
    store.pool().close().await;
    report["pool_metadata_after_close"] = json!({"size":store.pool().size(),"idle":store.pool().num_idle(),"closed":store.pool().is_closed(),"external_actual_backend_absence_required":true});
    report["capture_complete"] = json!(true);
    save(&report_path, &report);
}
fn backup_token(id: &str) -> String {
    let mut block = [0u8; 64];
    let key = b"backup-job-fixture-only";
    block[..key.len()].copy_from_slice(key);
    let mut inner = Sha256::new();
    inner.update(block.map(|x| x ^ 0x36));
    inner.update(id.as_bytes());
    let mut outer = Sha256::new();
    outer.update(block.map(|x| x ^ 0x5c));
    outer.update(inner.finalize());
    format!("{:x}", outer.finalize())[..32].to_owned()
}

fn decode_hex(raw: &str) -> Vec<u8> {
    assert!(raw.len() <= 131072 && raw.len().is_multiple_of(2));
    raw.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
