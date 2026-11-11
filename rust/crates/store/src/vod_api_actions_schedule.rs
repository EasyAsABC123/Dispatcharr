//! Test-only controlled snapshot observer. No production hooks or fake Source
//! network authority. Original Factory/real SQL body is compared to nativeTCP.
use super::{vod_api_http::VodReads, vod_api_owned_read::ReadError};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, OnceLock,
};
type Hook = Arc<dyn Fn(String) -> BoxFuture<'static, Result<(), ReadError>> + Send + Sync>;
fn slot() -> &'static Mutex<Option<Hook>> {
    static SLOT: OnceLock<Mutex<Option<Hook>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        *slot().lock().unwrap() = None;
    }
}
fn install(hook: Hook) -> Reset {
    let mut slot = slot().lock().unwrap();
    assert!(slot.is_none());
    *slot = Some(hook);
    Reset
}
pub(super) async fn observe(source: &str) -> Result<(), ReadError> {
    let hook = slot().lock().unwrap().clone();
    if let Some(hook) = hook {
        hook(source.into()).await?;
    }
    Ok(())
}
pub(super) async fn compare() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let reference: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_PROVIDER_SCHEDULE_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(reference["passed"], true);
    assert_eq!(reference["owned_metadata_restored"], true);
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let token = base["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["role"] == "standard" && row["method"] == "GET")
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    let ids = &reference["fixture_ids"];
    let mut saved = serde_json::Map::new();
    for (table, key) in [
        ("vod_series", "series"),
        ("vod_episode", "episode"),
        ("vod_vodlogo", "logo"),
        ("m3u_m3uaccount", "account"),
        ("vod_m3uepisoderelation", "relation"),
        ("m3u_m3uaccountprofile", "profile"),
    ] {
        let row: Value =
            sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM {table} t WHERE id=$1"))
                .persistent(false)
                .bind(ids[key].as_i64().unwrap())
                .fetch_one(&pool)
                .await
                .unwrap();
        saved.insert(table.into(), row);
    }
    let before = full_state(&pool).await;
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool.clone()),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    );
    let owner = VodReads::new(auth, Default::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = owner.provider_router();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let series_fired = Arc::new(AtomicBool::new(false));
    let profiles_fired = Arc::new(AtomicBool::new(false));
    let observer = {
        let pool = pool.clone();
        let reference = reference.clone();
        let series_fired = series_fired.clone();
        let profiles_fired = profiles_fired.clone();
        install(Arc::new(move |source| {
            let pool = pool.clone();
            let reference = reference.clone();
            let series_fired = series_fired.clone();
            let profiles_fired = profiles_fired.clone();
            Box::pin(async move {
                let ids = &reference["fixture_ids"];
                let changes = &reference["changes"];
                if source.contains("FROM vod_series c WHERE c.id=$1")
                    && !series_fired.swap(true, Ordering::SeqCst)
                {
                    for (table, key, field, value) in [
                        (
                            "vod_series",
                            "series",
                            "name",
                            changes["Series_name"].clone(),
                        ),
                        (
                            "vod_episode",
                            "episode",
                            "name",
                            changes["Episode_name"].clone(),
                        ),
                        ("vod_vodlogo", "logo", "name", changes["Logo_name"].clone()),
                        ("vod_vodlogo", "logo", "url", changes["Logo_url"].clone()),
                    ] {
                        sqlx::query(&format!("UPDATE {table} SET {field}=$1 WHERE id=$2"))
                            .persistent(false)
                            .bind(value.as_str().ok_or(ReadError::WorkerFailed)?)
                            .bind(ids[key].as_i64().ok_or(ReadError::WorkerFailed)?)
                            .execute(&pool)
                            .await?;
                    }
                    sqlx::query("UPDATE m3u_m3uaccount SET is_active=false WHERE id=$1")
                        .persistent(false)
                        .bind(ids["account"].as_i64().ok_or(ReadError::WorkerFailed)?)
                        .execute(&pool)
                        .await?;
                    sqlx::query(
                        "UPDATE vod_m3uepisoderelation SET custom_properties=$1 WHERE id=$2",
                    )
                    .persistent(false)
                    .bind(changes["Relation_properties"].clone())
                    .bind(ids["relation"].as_i64().ok_or(ReadError::WorkerFailed)?)
                    .execute(&pool)
                    .await?;
                    let inserted = reference["events"]
                        .as_array()
                        .ok_or(ReadError::WorkerFailed)?
                        .iter()
                        .find(|event| event.get("new_episode").is_some())
                        .ok_or(ReadError::WorkerFailed)?;
                    sqlx::query("INSERT INTO vod_episode SELECT * FROM json_populate_record(NULL::vod_episode,$1::json)").persistent(false).bind(inserted["new_episode"].to_string()).execute(&pool).await?;
                }
                if source.contains("FROM m3u_m3uaccountprofile p WHERE")
                    && !profiles_fired.swap(true, Ordering::SeqCst)
                {
                    let date = chrono::DateTime::parse_from_rfc3339(
                        changes["Profile_expiry"]
                            .as_str()
                            .ok_or(ReadError::WorkerFailed)?,
                    )
                    .map_err(|_| ReadError::WorkerFailed)?
                    .with_timezone(&chrono::Utc);
                    sqlx::query("UPDATE m3u_m3uaccountprofile SET exp_date=$1 WHERE id=$2")
                        .persistent(false)
                        .bind(date)
                        .bind(ids["profile"].as_i64().ok_or(ReadError::WorkerFailed)?)
                        .execute(&pool)
                        .await?;
                }
                Ok(())
            })
        }))
    };
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!(
            "http://{address}{}",
            reference["path"].as_str().unwrap()
        ))
        .header("Host", "localhost")
        .header("Accept", "application/json")
        .header("Authorization", token)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let mime = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let body = response.bytes().await.unwrap();
    drop(observer);
    // Restore exact source rows and microseconds, preserving original table state.
    for (table, row) in &saved {
        let id = row["id"].as_i64().unwrap();
        let assignments = match table.as_str() {
            "vod_series" | "vod_episode" => vec![("name", row["name"].clone())],
            "vod_vodlogo" => vec![("name", row["name"].clone()), ("url", row["url"].clone())],
            _ => Vec::new(),
        };
        for (field, value) in assignments {
            sqlx::query(&format!("UPDATE {table} SET {field}=$1 WHERE id=$2"))
                .persistent(false)
                .bind(value.as_str().unwrap())
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
    sqlx::query("UPDATE m3u_m3uaccount SET is_active=$1 WHERE id=$2")
        .persistent(false)
        .bind(saved["m3u_m3uaccount"]["is_active"].as_bool().unwrap())
        .bind(ids["account"].as_i64().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE vod_m3uepisoderelation SET custom_properties=$1 WHERE id=$2")
        .persistent(false)
        .bind(
            (!saved["vod_m3uepisoderelation"]["custom_properties"].is_null())
                .then(|| saved["vod_m3uepisoderelation"]["custom_properties"].clone()),
        )
        .bind(ids["relation"].as_i64().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let date = saved["m3u_m3uaccountprofile"]["exp_date"]
        .as_str()
        .map(|date| {
            chrono::DateTime::parse_from_rfc3339(date)
                .unwrap()
                .with_timezone(&chrono::Utc)
        });
    sqlx::query("UPDATE m3u_m3uaccountprofile SET exp_date=$1 WHERE id=$2")
        .persistent(false)
        .bind(date)
        .bind(ids["profile"].as_i64().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let inserted = reference["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event.get("new_episode").is_some())
        .unwrap();
    sqlx::query("DELETE FROM vod_episode WHERE id=$1")
        .persistent(false)
        .bind(inserted["new_episode"]["id"].as_i64().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    let after = full_state(&pool).await;
    assert_eq!(before, after);
    assert!(owner.shutdown(std::time::Duration::from_secs(2)).await);
    server.abort();
    let _ = server.await;
    let hash = format!("{:x}", Sha256::digest(&body));
    let passed = Some(u64::from(status)) == reference["expected"]["status"].as_u64()
        && mime == reference["expected"]["MIME"].as_str().unwrap()
        && hash == reference["expected"]["body_sha256"].as_str().unwrap()
        && series_fired.load(Ordering::SeqCst)
        && profiles_fired.load(Ordering::SeqCst);
    let report = json!({"passed":passed,"scope":"OriginalFactory+realSQL controlled cache/phase body vsnativeTCP; NOT SourceNetwork authority","Source_hash":reference["expected"]["body_sha256"],"native_hash":hash,"native_status":status,"native_MIME":mime,"native_body":String::from_utf8_lossy(&body),"series_phase_fired":series_fired.load(Ordering::SeqCst),"profile_phase_fired":profiles_fired.load(Ordering::SeqCst),"full_state_before":before,"full_state_after":after,"owned_metadata_restored":true});
    std::fs::write(
        std::path::Path::new(&std::env::var("VOD_NATIVE_EVIDENCE").unwrap())
            .join("provider-actions-schedule-native.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(
        passed,
        "Controlled Source/native schedule mismatch: {report:#}"
    );
}
async fn full_state(pool: &sqlx::PgPool) -> Value {
    super::vod_api_actions_peer::full_state(pool).await
}

async fn cron_state(pool: &sqlx::PgPool) -> Value {
    let mut state = full_state(pool).await.as_object().unwrap().clone();
    for (table, key) in [
        ("django_celery_beat_crontabschedule", "id"),
        ("django_celery_beat_periodictask", "id"),
        ("django_celery_beat_periodictasks", "ident"),
    ] {
        let sql = format!(
            "SELECT md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY {key})::text,'[]')) FROM {table} t"
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
pub(super) async fn compare_cron() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let source: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_PROVIDER_CRON_SOURCE").unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(source["passed"], true);
    assert_eq!(source["records"].as_array().unwrap().len(), 2);
    let base: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_SOURCE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    let state = &source["private_fixture_state"];
    let account = state["account_id"].as_i64().unwrap();
    let before = cron_state(&pool).await;
    let old: Option<i64> =
        sqlx::query_scalar("SELECT refresh_task_id::bigint FROM m3u_m3uaccount WHERE id=$1")
            .bind(account)
            .fetch_one(&pool)
            .await
            .unwrap();
    for (table, key) in [
        ("django_celery_beat_crontabschedule", "crontab"),
        ("django_celery_beat_periodictask", "task"),
    ] {
        let exists: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE id=$1"))
            .bind(state[key]["id"].as_i64().unwrap())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(exists, 0);
        sqlx::query(&format!(
            "INSERT INTO {table} SELECT * FROM json_populate_record(NULL::{table},$1::json)"
        ))
        .persistent(false)
        .bind(state[key].to_string())
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE m3u_m3uaccount SET refresh_task_id=$1 WHERE id=$2")
        .bind(state["task"]["id"].as_i64().unwrap())
        .bind(account)
        .execute(&pool)
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
    let owner = VodReads::new(auth, Default::default()).unwrap();
    let router = owner.provider_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let mut records = Vec::new();
    let mut differences = Vec::new();
    for case in source["records"].as_array().unwrap() {
        let role = case["role"].as_str().unwrap();
        let token = base["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["role"] == role && row["method"] == "GET")
            .unwrap()["headers"]["HTTP_AUTHORIZATION"]
            .as_str()
            .unwrap();
        let baseline = cron_state(&pool).await;
        let response = client
            .get(format!(
                "http://{address}{}",
                case["path"].as_str().unwrap()
            ))
            .header("Host", "localhost")
            .header("Accept", "application/json")
            .header("Authorization", token)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.unwrap();
        let hash = format!("{:x}", Sha256::digest(&body));
        let mut diff = Vec::new();
        if Some(u64::from(status)) != case["expected"]["status"].as_u64() {
            diff.push("status");
        }
        if hash != case["expected"]["body_sha256"].as_str().unwrap() {
            diff.push("body");
        }
        for name in [
            "Content-Type",
            "Content-Length",
            "Allow",
            "Vary",
            "WWW-Authenticate",
        ] {
            if headers.get(name).and_then(|v| v.to_str().ok())
                != case["expected"]["headers"][name].as_str()
            {
                diff.push(name);
            }
        }
        assert_eq!(cron_state(&pool).await, baseline);
        if !diff.is_empty() {
            differences.push(json!({"role":role,"fields":diff,"native_status":status,"native_body":String::from_utf8_lossy(&body)}));
        }
        records.push(
            json!({"role":role,"native_status":status,"native_hash":hash,"same":diff.is_empty()}),
        );
    }
    sqlx::query("UPDATE m3u_m3uaccount SET refresh_task_id=$1 WHERE id=$2")
        .bind(old)
        .bind(account)
        .execute(&pool)
        .await
        .unwrap();
    for (table, key) in [
        ("django_celery_beat_periodictask", "task"),
        ("django_celery_beat_crontabschedule", "crontab"),
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE id=$1"))
            .bind(state[key]["id"].as_i64().unwrap())
            .execute(&pool)
            .await
            .unwrap();
    }
    assert_eq!(cron_state(&pool).await, before);
    assert!(owner.shutdown(std::time::Duration::from_secs(2)).await);
    server.abort();
    let _ = server.await;
    let report = json!({"passed":differences.is_empty(),"scope":"Actual Original/nativeTCP2 nonempty nested-account cron fields; disabled tasks/no worker execution","records":records,"differences":differences,"full21state_restored":true});
    std::fs::write(
        std::path::Path::new(&std::env::var("VOD_NATIVE_EVIDENCE").unwrap())
            .join("provider-actions-cron-native.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert!(
        differences.is_empty(),
        "Cron body differences: {differences:#?}"
    );
}
