//! Source M3U batch goldens against the explicitly disposable migrated database.
use chrono::DateTime;
use dispatcharr_store::{ImportedStream, Store};
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path};

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
