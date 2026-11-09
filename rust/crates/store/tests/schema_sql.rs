//! Fresh-only original schema/defaults proof on real PostgreSQL, no Python DDL.
//! Original Dispatcharr contributors retain all application credit.
use dispatcharr_store::{initialize_empty_database, SchemaInitialization, Store};
use serde_json::Value;
async fn rows(store: &Store, table: &str) -> Value {
    assert!(table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    let query=format!("SELECT COALESCE(jsonb_agg(data ORDER BY data::text),'[]'::jsonb) FROM(SELECT to_jsonb(t)-ARRAY['created_at','updated_at','date_changed','last_update','applied'] AS data FROM public.\"{table}\" t)s");
    sqlx::query_scalar(&query)
        .fetch_one(store.pool())
        .await
        .unwrap()
}
#[tokio::test]
#[ignore = "requires SCHEMA_DISPOSABLE_FIXTURE and three owned empty DBs plus original fresh reference"]
async fn original_schema_defaults_atomic_concurrent_init_and_existing_objects_remain_untouched() {
    assert_eq!(
        std::env::var("SCHEMA_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let manifest: Value =
        serde_json::from_str(include_str!("../schema/original-1241b3-manifest.json")).unwrap();
    let native_url = std::env::var("DATABASE_TEST_URL").unwrap();
    let reference_url = std::env::var("DATABASE_REFERENCE_URL").unwrap();
    assert_eq!(
        initialize_empty_database(&native_url).await.unwrap(),
        SchemaInitialization::Created {
            tables: 62,
            migrations: 184
        }
    );
    let native = Store::connect(&native_url, 2).await.unwrap();
    let reference = Store::connect(&reference_url, 2).await.unwrap();
    for (table, count) in manifest["tables"].as_object().unwrap() {
        let expected = rows(&reference, table).await;
        let actual = rows(&native, table).await;
        assert_eq!(
            expected.as_array().unwrap().len() as u64,
            count.as_u64().unwrap(),
            "original default inventory for{table}"
        );
        assert_eq!(actual, expected, "native original default rows for{table}");
    }
    assert!(matches!(
        initialize_empty_database(&native_url).await.unwrap(),
        SchemaInitialization::SkippedExisting { .. }
    ));
    assert!(matches!(
        initialize_empty_database(&reference_url).await.unwrap(),
        SchemaInitialization::SkippedExisting { .. }
    ));
    for (table, _) in manifest["tables"].as_object().unwrap() {
        assert_eq!(
            rows(&native, table).await,
            rows(&reference, table).await,
            "repeat/existing initialization changed rows for{table}"
        );
    }
    let sentinel_url = std::env::var("DATABASE_SENTINEL_URL").unwrap();
    let sentinel = Store::connect(&sentinel_url, 1).await.unwrap();
    sqlx::raw_sql("CREATE SCHEMA pgholder;CREATE TABLE pgholder.keep(value integer);INSERT INTO pgholder.keep VALUES(7)").execute(sentinel.pool()).await.unwrap();
    assert!(matches!(
        initialize_empty_database(&sentinel_url).await.unwrap(),
        SchemaInitialization::SkippedExisting { .. }
    ));
    let value: i32 = sqlx::query_scalar("SELECT value FROM pgholder.keep")
        .fetch_one(sentinel.pool())
        .await
        .unwrap();
    assert_eq!(value, 7);
    let absent: bool = sqlx::query_scalar("SELECT to_regclass('public.accounts_user') IS NULL")
        .fetch_one(sentinel.pool())
        .await
        .unwrap();
    assert!(absent);
    let race_url = std::env::var("DATABASE_RACE_URL").unwrap();
    let (first, second) = tokio::join!(
        initialize_empty_database(&race_url),
        initialize_empty_database(&race_url)
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, SchemaInitialization::Created { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|v| matches!(v, SchemaInitialization::SkippedExisting { .. }))
            .count(),
        1
    );
    let race = Store::connect(&race_url, 2).await.unwrap();
    for (table, _) in manifest["tables"].as_object().unwrap() {
        assert_eq!(
            rows(&race, table).await,
            rows(&reference, table).await,
            "concurrent original defaults for{table}"
        );
    }
    native.pool().close().await;
    reference.pool().close().await;
    sentinel.pool().close().await;
    race.pool().close().await;
}
