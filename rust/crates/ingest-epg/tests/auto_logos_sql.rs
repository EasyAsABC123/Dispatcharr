//! Actual original automatic-logo helper flags, bulk side effects and SQL error
//! policy. All application/algorithm credit belongs to Dispatcharr's authors.
use dispatcharr_ingest_epg::{auto_logos, SwapPolicyContext};
use dispatcharr_store::Store;
use serde_json::{json, Value};
use std::sync::{atomic::AtomicBool, Arc};
fn normalized(mut value: Value, before: &Value) -> Value {
    let old = before["logos"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect::<std::collections::HashSet<_>>();
    let urls = value["logos"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (row["id"].as_i64().unwrap(), row["url"].clone()))
        .collect::<std::collections::HashMap<_, _>>();
    for row in value["channels"].as_array_mut().unwrap() {
        if let Some(id) = row["logo_id"].as_i64() {
            if !old.contains(&id) {
                row["logo_id"] = json!({"new_url":urls[&id]});
            }
        }
    }
    for row in value["logos"].as_array_mut().unwrap() {
        if !old.contains(&row["id"].as_i64().unwrap()) {
            row["id"] = json!("new");
        }
    }
    value["logos"]
        .as_array_mut()
        .unwrap()
        .sort_by_key(|row| row["url"].as_str().unwrap().to_string());
    value
}
async fn state(store: &Store, id: i64, prefix: &str) -> Value {
    let channels:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('id',c.id,'epg_data_id',c.epg_data_id,'logo_id',c.logo_id,'logo__url',l.url) ORDER BY c.id),'[]') FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON c.epg_data_id=e.id LEFT JOIN dispatcharr_channels_logo l ON c.logo_id=l.id WHERE e.epg_source_id=$1").bind(id).fetch_one(store.pool()).await.unwrap();
    let logos:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('id',id,'name',name,'url',url) ORDER BY id),'[]') FROM dispatcharr_channels_logo WHERE url LIKE $1").bind(format!("{prefix}%")).fetch_one(store.pool()).await.unwrap();
    json!({"channels":channels,"logos":logos})
}
#[tokio::test]
#[ignore = "requires isolated original automatic EPG-logo fixture and PostgreSQL"]
async fn original_auto_logo_properties_bulk_mutations_and_swallowed_sql() {
    assert_eq!(
        std::env::var("EPG_LOGO_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_LOGO_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let id = case["source_id"].as_i64().unwrap();
        let before = &case["before"];
        for channel in before["channels"].as_array().unwrap() {
            sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=$2 WHERE id=$1")
                .bind(channel["id"].as_i64().unwrap())
                .bind(channel["logo_id"].as_i64())
                .execute(store.pool())
                .await
                .unwrap();
        }
        let ids = before["logos"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_i64().unwrap())
            .collect::<Vec<_>>();
        let prefix = format!("https://fixture.invalid/rust-epg-auto-logo-{name}/");
        sqlx::query("DELETE FROM dispatcharr_channels_logo WHERE url LIKE $1 AND id<>ALL($2)")
            .bind(format!("{prefix}%"))
            .bind(ids)
            .execute(store.pool())
            .await
            .unwrap();
        if name == "forced-db-error" {
            let mut constraint=sqlx::QueryBuilder::<sqlx::Postgres>::new("ALTER TABLE dispatcharr_channels_logo ADD CONSTRAINT rust_epg_auto_logo_fixture CHECK(url NOT LIKE '");
            constraint.push(&prefix).push("new%') NOT VALID");
            constraint.build().execute(store.pool()).await.unwrap();
        }
        let result = auto_logos::maybe_apply(
            store.clone(),
            id,
            &case["properties"],
            SwapPolicyContext {
                cancelled: Arc::new(AtomicBool::new(false)),
                retained: Arc::new(()),
            },
        )
        .await;
        if name == "forced-db-error" {
            sqlx::query(
                "ALTER TABLE dispatcharr_channels_logo DROP CONSTRAINT rust_epg_auto_logo_fixture",
            )
            .execute(store.pool())
            .await
            .unwrap();
        }
        if case["failure"].is_null() {
            assert!(result.is_ok(), "result {name}: {result:?}");
            assert_eq!(
                result.unwrap().map(|stats| stats.source_value()),
                case["result"].as_object().map(|_| case["result"].clone()),
                "stats {name}"
            );
        } else {
            let failure = result.unwrap_err();
            assert_eq!(
                failure.to_string(),
                case["failure"]["message"].as_str().unwrap(),
                "error {name}"
            );
        }
        assert_eq!(
            normalized(state(&store, id, &prefix).await, before),
            normalized(case["after"].clone(), before),
            "effects {name}"
        );
        compared += 1;
    }
    assert_eq!(compared, 18);
    println!("EPG_AUTO_LOGO_SOURCE_CASES={compared}");
}

struct Witness(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for Witness {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
}
#[tokio::test]
#[ignore = "requires isolated original automatic EPG-logo fixture and physical PostgreSQL"]
async fn physical_sql_admission_and_oversized_driver_row() {
    use dispatcharr_ingest_epg::auto_logos::LogoError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    assert_eq!(
        std::env::var("EPG_LOGO_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_LOGO_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let case = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "true")
        .unwrap();
    let id = case["source_id"].as_i64().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    for channel in case["before"]["channels"].as_array().unwrap() {
        sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=$2 WHERE id=$1")
            .bind(channel["id"].as_i64().unwrap())
            .bind(channel["logo_id"].as_i64())
            .execute(store.pool())
            .await
            .unwrap();
    }
    sqlx::query("DELETE FROM dispatcharr_channels_logo WHERE url='https://fixture.invalid/rust-epg-auto-logo-true/new'").execute(store.pool()).await.unwrap();
    let mut blocker = store.pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE dispatcharr_channels_logo IN SHARE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..4 {
        let store = store.clone();
        let witness = dropped.clone();
        tasks.push(tokio::spawn(async move {
            auto_logos::maybe_apply(
                store,
                id,
                &json!({"auto_apply_epg_logos":true}),
                SwapPolicyContext {
                    cancelled: Arc::new(AtomicBool::new(false)),
                    retained: Arc::new(Witness(witness)),
                },
            )
            .await
        }));
    }
    tokio::time::timeout(std::time::Duration::from_secs(10),async{loop{let blocked:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO dispatcharr_channels_logo%' ").fetch_one(store.pool()).await.unwrap();if blocked==4{break;}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}}).await.unwrap();
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    let result = auto_logos::maybe_apply(
        store.clone(),
        id,
        &json!({"auto_apply_epg_logos":true}),
        SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: Arc::new(()),
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(LogoError::Resource(
            dispatcharr_ingest_epg::ImportError::Busy
        ))
    ));
    blocker.rollback().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while dropped.load(Ordering::Acquire) != 4 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let current = &case["before"]["logos"][1];
    let current_id = current["id"].as_i64().unwrap();
    sqlx::query("UPDATE dispatcharr_channels_logo SET url=$2 WHERE id=$1")
        .bind(current_id)
        .bind("x".repeat(160 * 1024))
        .execute(store.pool())
        .await
        .unwrap();
    let result = auto_logos::maybe_apply_with_config(
        store.clone(),
        id,
        &json!({"auto_apply_epg_logos":true}),
        SwapPolicyContext {
            cancelled: Arc::new(AtomicBool::new(false)),
            retained: Arc::new(()),
        },
        auto_logos::LogoConfig {
            max_row_url_bytes: 128 * 1024,
        },
    )
    .await;
    sqlx::query("UPDATE dispatcharr_channels_logo SET url=$2 WHERE id=$1")
        .bind(current_id)
        .bind(current["url"].as_str().unwrap())
        .execute(store.pool())
        .await
        .unwrap();
    assert!(
        matches!(
            result,
            Err(LogoError::Resource(
                dispatcharr_ingest_epg::ImportError::Capacity("automatic-logo row")
            ))
        ),
        "oversized valid row is an explicit resource limit: {result:?}"
    );
    println!("EPG_AUTO_LOGO_PHYSICAL_SCENARIOS=2");
}
