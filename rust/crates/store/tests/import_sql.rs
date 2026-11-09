//! Source M3U batch goldens against the explicitly disposable migrated database.
use chrono::DateTime;
use dispatcharr_store::{ImportedStream, Store};
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path};

async fn group_snapshot(store: &Store, prefix: &str) -> Value {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM dispatcharr_channels_channelgroup WHERE name LIKE $1 ORDER BY name",
    )
    .bind(format!("{prefix}%"))
    .fetch_all(store.pool())
    .await
    .unwrap();
    let relations:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('channel_group__name',g.name,'m3u_account_id',r.m3u_account_id,'custom_properties',r.custom_properties,'enabled',r.enabled,'auto_channel_sync',r.auto_channel_sync,'auto_sync_channel_start',r.auto_sync_channel_start,'auto_sync_channel_end',r.auto_sync_channel_end,'last_seen',r.last_seen,'is_stale',r.is_stale) FROM dispatcharr_channels_channelgroupm3uaccount r JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id WHERE g.name LIKE $1 ORDER BY g.name,r.m3u_account_id")
        .bind(format!("{prefix}%")).fetch_all(store.pool()).await.unwrap();
    json!({"groups":names,"relations":relations})
}
fn normalize_group_timestamps(mut value: Value) -> Value {
    for relation in value["relations"].as_array_mut().unwrap() {
        let parsed = DateTime::parse_from_rfc3339(relation["last_seen"].as_str().unwrap()).unwrap();
        relation["last_seen"] = json!(parsed.to_rfc3339_opts(chrono::SecondsFormat::Micros, true));
    }
    value
}

#[tokio::test]
#[ignore = "requires INGEST_DISPOSABLE_FIXTURE and original-group-golden disposable database"]
async fn original_groups_preserve_settings_xc_identity_and_bulk_write_boundaries() {
    use dispatcharr_store::ImportedGroup;
    assert_eq!(
        std::env::var("INGEST_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("INGEST_BATCH_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let prefix = fixture["prefix"].as_str().unwrap();
    assert!(prefix.starts_with("rust-ingest-"));
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    for step in fixture["group_steps"].as_array().unwrap() {
        if let Some(mutation) = step["mutation"].as_object() {
            sqlx::query("UPDATE dispatcharr_channels_channelgroupm3uaccount SET custom_properties=$1,is_stale=true WHERE channel_group_id=$2 AND m3u_account_id=$3")
                .bind(&mutation["custom_properties"]).bind(mutation["group_id"].as_i64().unwrap()).bind(mutation["account_id"].as_i64().unwrap())
                .execute(store.pool()).await.unwrap();
        }
        let groups: Vec<ImportedGroup> = serde_json::from_value(step["groups"].clone()).unwrap();
        let result = store
            .import_m3u_groups(
                step["account_id"].as_i64().unwrap(),
                &step["account_properties"],
                groups,
                DateTime::parse_from_rfc3339(step["scan_time"].as_str().unwrap())
                    .unwrap()
                    .to_utc(),
            )
            .await;
        assert_eq!(
            result.is_err(),
            step["error"].as_bool().unwrap(),
            "{}: {result:?}",
            step["name"]
        );
        assert_eq!(
            normalize_group_timestamps(group_snapshot(&store, prefix).await),
            normalize_group_timestamps(step["expected"].clone()),
            "{}",
            step["name"]
        );
    }
    let before = group_snapshot(&store, prefix).await;
    let repeated = ImportedGroup {
        name: format!("{prefix} bounded"),
        custom_properties: Default::default(),
    };
    assert!(store
        .import_m3u_groups(
            fixture["account_id"].as_i64().unwrap(),
            &json!({}),
            vec![repeated; 32769],
            chrono::Utc::now()
        )
        .await
        .is_err());
    assert_eq!(group_snapshot(&store, prefix).await, before);
}

async fn snapshot(store: &Store, prefix: &str) -> Vec<Value> {
    let rows:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(s)-'id' FROM dispatcharr_channels_stream s WHERE tvg_id LIKE $1 ORDER BY stream_hash")
        .bind(format!("{prefix}%")).fetch_all(store.pool()).await.unwrap();
    rows.into_iter()
        .map(|mut row| {
            // Django returns this FloatField as float even when its value is whole;
            // PostgreSQL to_jsonb(float8) may otherwise spell2.0 as integer token2.
            if let Some(number) = row["stream_chno"].as_f64() {
                row["stream_chno"] = json!(number);
            }
            row
        })
        .collect()
}
fn normalized(rows: Vec<Value>, previous: &[Value]) -> Value {
    let old = previous
        .iter()
        .map(|row| (row["stream_hash"].as_str().unwrap(), row))
        .collect::<HashMap<_, _>>();
    let rows = rows
        .into_iter()
        .map(|mut row| {
            let before = old.get(row["stream_hash"].as_str().unwrap());
            let parsed =
                |value: &Value| DateTime::parse_from_rfc3339(value.as_str().unwrap()).unwrap();
            let changed = before
                .is_none_or(|before| parsed(&before["updated_at"]) != parsed(&row["updated_at"]));
            let advanced = before
                .is_none_or(|before| parsed(&before["last_seen"]) < parsed(&row["last_seen"]));
            let object = row.as_object_mut().unwrap();
            object.remove("updated_at");
            object.remove("last_seen");
            object.insert("updated_at_changed".into(), json!(changed));
            object.insert("last_seen_advanced".into(), json!(advanced));
            row
        })
        .collect::<Vec<_>>();
    json!(rows)
}

#[tokio::test]
#[ignore = "requires INGEST_DISPOSABLE_FIXTURE and original-batch-golden disposable database"]
async fn original_batch_create_touch_change_dedup_and_transaction_rollback() {
    assert_eq!(
        std::env::var("INGEST_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture_path = std::env::var("INGEST_BATCH_FIXTURE").unwrap();
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(Path::new(&fixture_path)).unwrap()).unwrap();
    let account = fixture["account_id"].as_i64().unwrap();
    let prefix = fixture["prefix"].as_str().unwrap();
    assert!(prefix.starts_with("rust-ingest-"));
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    for step in fixture["steps"].as_array().unwrap() {
        let before = snapshot(&store, prefix).await;
        let rows: Vec<ImportedStream> = if let Some(playlist) = step["playlist"].as_str() {
            use dispatcharr_ingest_core::{project_entry, Entries, Limits, ProjectionContext};
            let context = &step["projection_context"];
            let groups = serde_json::from_value(context["groups"].clone()).unwrap();
            let keys = context["hash_keys"]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap())
                .collect::<Vec<_>>();
            let context = ProjectionContext {
                account_id: step
                    .get("account_id")
                    .and_then(Value::as_i64)
                    .unwrap_or(account),
                account_type: context["account_type"].as_str().unwrap(),
                groups: &groups,
                hash_keys: &keys,
                max_name_characters: context["max_name_characters"].as_u64().unwrap() as usize,
            };
            let mut rows = Vec::new();
            for entry in
                Entries::new(std::io::Cursor::new(playlist.as_bytes()), Limits::default()).unwrap()
            {
                if let Some(row) = project_entry(&entry.unwrap(), &context).unwrap() {
                    rows.push(row.into());
                }
            }
            assert_eq!(serde_json::to_value(&rows).unwrap(), step["rows"]);
            rows
        } else {
            serde_json::from_value(step["rows"].clone()).unwrap()
        };
        let step_account = step
            .get("account_id")
            .and_then(Value::as_i64)
            .unwrap_or(account);
        let result = store.import_m3u_batch(step_account, rows).await;
        assert_eq!(
            result.is_err(),
            step["error"].as_bool().unwrap(),
            "{}",
            step["name"]
        );
        if let Ok(counts) = result {
            assert_eq!(
                json!([counts.created, counts.updated, counts.unchanged]),
                step["counts"],
                "{}",
                step["name"]
            );
        }
        let actual = normalized(snapshot(&store, prefix).await, &before);
        assert_eq!(actual, step["expected"], "{}", step["name"]);
    }
    // Rejected input bounds must leave the already verified rows unchanged.
    let before = snapshot(&store, prefix).await;
    let template: ImportedStream =
        serde_json::from_value(fixture["steps"][0]["rows"][0].clone()).unwrap();
    assert!(store
        .import_m3u_batch(account, vec![template.clone(); 2001])
        .await
        .is_err());
    let mut oversized = template;
    oversized.custom_properties = json!({"fixture":"x".repeat(8*1024*1024)});
    assert!(store
        .import_m3u_batch(account, vec![oversized])
        .await
        .is_err());
    assert_eq!(snapshot(&store, prefix).await, before);
    sqlx::query("DELETE FROM dispatcharr_channels_stream WHERE tvg_id LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(store.pool())
        .await
        .unwrap();
}
