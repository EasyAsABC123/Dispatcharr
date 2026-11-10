//! Original Dispatcharr authors retain full application and algorithm credit.
//! Actual relation-lock schedule; requires the original disposable grid oracle.
use dispatcharr_store::Store;
use serde_json::Value;
use std::time::Duration;

#[tokio::test]
#[ignore = "requires disposable original grid concurrency fixture"]
async fn original_visibility_during_source_relation_lock() {
    assert_eq!(
        std::env::var("GRID_CONCURRENCY_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(url.contains("playback-fixture-only@"));
    let data: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GRID_CONCURRENCY_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&url, 2).await.unwrap();
    let actor = sqlx::PgPool::connect(&url).await.unwrap();
    let user = store
        .user_by_username("rust-grid-concurrency-admin")
        .await
        .unwrap()
        .unwrap();
    let channel = data["channel_id"].as_i64().unwrap();
    let profile = data["profile_id"].as_i64().unwrap().to_string();
    sqlx::query("UPDATE dispatcharr_channels_channel SET hidden_from_output=false WHERE id=$1")
        .bind(channel)
        .execute(&actor)
        .await
        .unwrap();
    let mut lock = actor.begin().await.unwrap();
    sqlx::query("LOCK TABLE epg_epgsource IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let query_store = store.clone();
    let worker = tokio::spawn(async move {
        query_store
            .grid_visible_channels(&user, Some(&profile))
            .await
    });
    let witness = tokio::time::timeout(Duration::from_millis(1200), async {
        loop {
            let query: Option<String> = sqlx::query_scalar(
                "SELECT query FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query LIKE '%dispatcharr_channels_channel%'",
            ).fetch_optional(&actor).await.unwrap();
            if let Some(query) = query {
                break query;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if witness.is_ok() {
        sqlx::query("UPDATE dispatcharr_channels_channel SET hidden_from_output=true WHERE id=$1")
            .bind(channel)
            .execute(&mut *lock)
            .await
            .unwrap();
    }
    lock.commit().await.unwrap();
    let rows = worker.await.unwrap().unwrap();
    let witness = witness.expect("Native metadata query did not physically block");
    println!("GRID_METADATA_LOCK_WITNESS={witness}");
    assert!(
        rows.is_empty(),
        "Original returns no channels after the committed hide during metadata relation-lock wait"
    );
    assert_eq!(data["cases"][3]["body"], serde_json::json!({"data":[]}));
    actor.close().await;
    store.pool().close().await;
}
