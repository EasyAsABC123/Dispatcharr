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

async fn cleanup_snapshot(store: &Store, prefix: &str, channel: i64) -> Value {
    let streams:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('tvg_id',tvg_id,'m3u_account_id',m3u_account_id,'channel_group_id',channel_group_id,'is_catchup',is_catchup,'catchup_days',catchup_days) FROM dispatcharr_channels_stream WHERE tvg_id LIKE $1 ORDER BY tvg_id")
        .bind(format!("{prefix}%")).fetch_all(store.pool()).await.unwrap();
    let channel_flags:Value=sqlx::query_scalar("SELECT jsonb_build_object('is_catchup',is_catchup,'catchup_days',catchup_days) FROM dispatcharr_channels_channel WHERE id=$1")
        .bind(channel).fetch_one(store.pool()).await.unwrap();
    let links:Vec<String>=sqlx::query_scalar("SELECT s.tvg_id FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE cs.channel_id=$1 ORDER BY s.tvg_id")
        .bind(channel).fetch_all(store.pool()).await.unwrap();
    json!({"streams":streams,"channel":channel_flags,"links":links})
}
#[tokio::test]
#[ignore = "requires INGEST_DISPOSABLE_FIXTURE and original-cleanup-golden disposable database"]
async fn original_cleanup_preserves_overlap_counts_boundaries_and_channel_rollups() {
    assert_eq!(
        std::env::var("INGEST_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("INGEST_BATCH_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let data = &fixture["cleanup"];
    let account = data["account_id"].as_i64().unwrap();
    let prefix = data["prefix"].as_str().unwrap();
    assert!(prefix.starts_with("rust-cleanup-"));
    let channel = data["channel_id"].as_i64().unwrap();
    let scan = DateTime::parse_from_rfc3339(data["scan_time"].as_str().unwrap())
        .unwrap()
        .to_utc();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    for step in data["steps"].as_array().unwrap() {
        sqlx::query("UPDATE m3u_m3uaccount SET stale_stream_days=$1,is_active=$2 WHERE id=$3")
            .bind(step["stale_stream_days"].as_i64().unwrap() as i32)
            .bind(step["is_active"].as_bool().unwrap())
            .bind(account)
            .execute(store.pool())
            .await
            .unwrap();
        let result = store.cleanup_m3u_streams(account, scan).await;
        assert_eq!(
            result.is_err(),
            step["error"].as_bool().unwrap(),
            "{}: {result:?}",
            step["name"]
        );
        if let Ok(counts) = result {
            assert_eq!(
                serde_json::to_value(counts).unwrap(),
                json!({"disabled_group_matches":step["disabled_group_matches"],"stale_matches":step["stale_matches"],"reported_total":step["reported_total"],"deleted_streams":step["deleted_streams"]}),
                "{}",
                step["name"]
            );
        }
        assert_eq!(
            cleanup_snapshot(&store, prefix, channel).await,
            step["expected"],
            "{}",
            step["name"]
        );
    }
}

async fn group_cleanup_snapshot(store: &Store, data: &Value) -> Value {
    let prefix = data["prefix"].as_str().unwrap();
    let groups: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM dispatcharr_channels_channelgroup WHERE name LIKE $1 ORDER BY name",
    )
    .bind(format!("{prefix}%"))
    .fetch_all(store.pool())
    .await
    .unwrap();
    let relations:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('channel_group__name',g.name,'m3u_account_id',r.m3u_account_id,'enabled',r.enabled,'last_seen',r.last_seen,'is_stale',r.is_stale) FROM dispatcharr_channels_channelgroupm3uaccount r JOIN dispatcharr_channels_channelgroup g ON g.id=r.channel_group_id WHERE g.name LIKE $1 ORDER BY g.name,r.m3u_account_id")
        .bind(format!("{prefix}%")).fetch_all(store.pool()).await.unwrap();
    let mut references = serde_json::Map::new();
    let mut timestamps = serde_json::Map::new();
    for (name, table, key) in [
        ("stream", "dispatcharr_channels_stream", "stream_id"),
        ("channel", "dispatcharr_channels_channel", "channel_id"),
        (
            "override",
            "dispatcharr_channels_channeloverride",
            "override_id",
        ),
    ] {
        let time = DateTime::parse_from_rfc3339(data["times"][name].as_str().unwrap())
            .unwrap()
            .to_utc();
        let row:Value=sqlx::query_scalar(&format!("SELECT jsonb_build_object('group_id',channel_group_id,'unchanged',updated_at=$2::timestamptz) FROM {table} WHERE id=$1"))
            .bind(data[key].as_i64().unwrap()).bind(time).fetch_one(store.pool()).await.unwrap();
        references.insert(name.into(), row["group_id"].clone());
        timestamps.insert(name.into(), row["unchanged"].clone());
    }
    json!({"groups":groups,"relations":relations,"references":references,"timestamps_unchanged":timestamps})
}
#[tokio::test]
#[ignore = "requires INGEST_DISPOSABLE_FIXTURE and original-group-cleanup-golden disposable database"]
async fn original_group_cleanup_preserves_protected_groups_and_nullable_reference_timestamps() {
    use dispatcharr_store::GroupCleanupAccount;
    assert_eq!(
        std::env::var("INGEST_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("INGEST_BATCH_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let data = &fixture["group_cleanup"];
    assert!(data["prefix"]
        .as_str()
        .unwrap()
        .starts_with("rust-group-cleanup-"));
    let scan = DateTime::parse_from_rfc3339(data["scan_time"].as_str().unwrap())
        .unwrap()
        .to_utc();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    for step in data["steps"].as_array().unwrap() {
        let before = group_cleanup_snapshot(&store, data).await;
        let account: GroupCleanupAccount = serde_json::from_value(step["account"].clone()).unwrap();
        let result = store.cleanup_m3u_group_relationships(&account, scan).await;
        assert_eq!(
            result.is_err(),
            step["error"].as_bool().unwrap(),
            "{}: {result:?}",
            step["name"]
        );
        let actual = group_cleanup_snapshot(&store, data).await;
        if let Ok(counts) = result {
            assert_eq!(
                counts.removed_relationships,
                step["reported"].as_u64().unwrap(),
                "{}",
                step["name"]
            );
            assert_eq!(
                counts.removed_groups,
                (before["groups"].as_array().unwrap().len()
                    - actual["groups"].as_array().unwrap().len()) as u64,
                "{}",
                step["name"]
            );
        }
        assert_eq!(
            normalize_group_timestamps(actual),
            normalize_group_timestamps(step["expected"].clone()),
            "{}",
            step["name"]
        );
    }
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
        let result = if let Some(playlist) = step["playlist"].as_str() {
            use dispatcharr_ingest_core::{prepare_playlist, CacheReadLimits, PreparationLimits};
            let path = Path::new(&fixture_path).with_extension("parsed-cache.json");
            prepare_playlist(
                std::io::Cursor::new(playlist.as_bytes()),
                &path,
                PreparationLimits::default(),
            )
            .unwrap();
            let context = &step["projection_context"];
            let request = dispatcharr_store::CacheBatchRequest {
                account_id: step_account,
                path: path.clone(),
                groups: serde_json::from_value(context["groups"].clone()).unwrap(),
                hash_keys: serde_json::from_value(context["hash_keys"].clone()).unwrap(),
                max_name_characters: context["max_name_characters"].as_u64().unwrap() as usize,
                limits: CacheReadLimits::default(),
            };
            let result = store.import_standard_cache_batches(request).await;
            if let Some(expected) = step["expected_batches"].as_u64() {
                assert_eq!(result.as_ref().unwrap().batches, expected as usize);
            }
            std::fs::remove_file(path).unwrap();
            result.map(|counts| counts.streams)
        } else {
            store.import_m3u_batch(step_account, rows).await
        };
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
    // A corrupt parsed-cache tail must not touch rows from any earlier batch.
    // A configured filter must fail explicitly rather than importing unfiltered.
    let parsed = fixture["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["playlist"].is_string())
        .unwrap();
    let path = Path::new(&fixture_path).with_extension("rejected-cache.json");
    let playlist = parsed["playlist"].as_str().unwrap();
    let request = || dispatcharr_store::CacheBatchRequest {
        account_id: account,
        path: path.clone(),
        groups: serde_json::from_value(parsed["projection_context"]["groups"].clone()).unwrap(),
        hash_keys: serde_json::from_value(parsed["projection_context"]["hash_keys"].clone())
            .unwrap(),
        max_name_characters: 512,
        limits: Default::default(),
    };
    dispatcharr_ingest_core::prepare_playlist(
        std::io::Cursor::new(playlist),
        &path,
        Default::default(),
    )
    .unwrap();
    let valid_cache = std::fs::read(&path).unwrap();
    let before = snapshot(&store, prefix).await;
    std::fs::write(&path, &valid_cache[..valid_cache.len() - 1]).unwrap();
    assert!(store
        .import_standard_cache_batches(request())
        .await
        .is_err());
    assert_eq!(snapshot(&store, prefix).await, before);
    std::fs::write(&path, valid_cache).unwrap();
    let filter: i64 = sqlx::query_scalar("INSERT INTO m3u_m3ufilter (m3u_account_id,filter_type,regex_pattern,exclude,\"order\",custom_properties) VALUES ($1,'name','.',true,0,'{}') RETURNING id")
        .bind(account).fetch_one(store.pool()).await.unwrap();
    assert!(matches!(
        store.import_standard_cache_batches(request()).await,
        Err(dispatcharr_store::StoreError::Unsupported(_))
    ));
    assert_eq!(snapshot(&store, prefix).await, before);
    sqlx::query("DELETE FROM m3u_m3ufilter WHERE id=$1")
        .bind(filter)
        .execute(store.pool())
        .await
        .unwrap();
    std::fs::remove_file(path).unwrap();
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
