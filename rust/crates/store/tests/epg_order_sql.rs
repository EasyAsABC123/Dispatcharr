//! Actual original EPG list order across custom/generic prepared plans.
//! Original Dispatcharr authors retain full credit. Disposable fixture only.
use dispatcharr_store::{EpgReadResource, Store};
use futures_util::TryStreamExt;
use serde_json::Value;

#[tokio::test]
#[ignore = "requires disposable original EPG read fixture and PostgreSQL"]
async fn original_epg_sources_order_survives_prepared_plan_reuse() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_READ_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    let case = cases
        .iter()
        .find(|case| {
            case["path"] == "/api/epg/sources/"
                && case["role"] == "admin"
                && case["method"] == "GET"
        })
        .unwrap();
    let expected = case["expected"]["json"].as_array().unwrap();
    assert_eq!(
        expected.len(),
        3,
        "test requires cron plus two NULL-task rows"
    );
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let mut compared = 0;
    for mode in ["force_custom_plan", "force_generic_plan", "auto"] {
        sqlx::query(&format!("SET plan_cache_mode = {mode}"))
            .execute(store.pool())
            .await
            .unwrap();
        for _ in 0..12 {
            let mut rows = store.epg_read_rows(EpgReadResource::Sources, None, String::new());
            let mut actual = Vec::new();
            while let Some(row) = rows.try_next().await.unwrap() {
                actual.push(row);
            }
            drop(rows);
            assert_eq!(&actual, expected, "source list fields/order under {mode}");
            compared += 1;
            for row in expected {
                let id = row["id"].as_i64().unwrap();
                let mut rows =
                    store.epg_read_rows(EpgReadResource::Sources, Some(id), String::new());
                assert_eq!(rows.try_next().await.unwrap().as_ref(), Some(row));
                assert!(rows.try_next().await.unwrap().is_none());
                compared += 1;
            }
            let mut missing =
                store.epg_read_rows(EpgReadResource::Sources, Some(999999), String::new());
            assert!(missing.try_next().await.unwrap().is_none());
        }
    }
    println!("EPG_PREPARED_ORDER_CASES={compared}");
}
