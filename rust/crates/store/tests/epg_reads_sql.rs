//! Draft actual-schema row-projection comparison. Original authors retain credit.
//! Requires an explicitly disposable Source fixture; no HTTP/resource acceptance.
use dispatcharr_store::{EpgReadResource, Store};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use std::path::Path;

#[tokio::test]
#[ignore = "requires disposable original EPG read oracle and PostgreSQL"]
async fn original_stored_epg_read_row_projection() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(Path::new(&std::env::var("EPG_READ_FIXTURE").unwrap())).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let mut compared = 0usize;
    for case in fixture["cases"].as_array().unwrap() {
        // Permission, renderer, route parsing and HEAD are separate HTTP gates.
        if case["method"] != "GET"
            || case["expected"]["status"] != 200
            || case["expected"].get("json").is_none()
        {
            continue;
        }
        let path = case["path"].as_str().unwrap();
        let fields = path
            .split('?')
            .next()
            .unwrap()
            .trim_matches('/')
            .split('/')
            .collect::<Vec<_>>();
        let resource = match fields[2] {
            "sources" => EpgReadResource::Sources,
            "epgdata" => EpgReadResource::EpgData,
            "programs" => EpgReadResource::Programs,
            _ => panic!("unknown fixture resource"),
        };
        let id = fields.get(3).map(|raw| raw.parse::<i64>().unwrap());
        // Origin handling is an HTTP gate. This projection test receives the
        // captured original origin and only compares row/poster serialization.
        let origin = case["component_origin"].as_str().unwrap();
        let mut stream = store.epg_read_rows(resource, id, origin.into());
        let mut rows = Vec::new();
        while let Some(row) = stream.try_next().await.unwrap() {
            assert!(rows.len() < 1000, "owned fixture unexpectedly grew");
            rows.push(row);
        }
        let expected = case["expected"]["json"].clone();
        let actual = if id.is_some() {
            assert_eq!(rows.len(), 1);
            rows.pop().unwrap()
        } else {
            json!(rows)
        };
        assert_eq!(actual, expected, "original projection mismatch for {path}");
        compared += 1;
    }
    assert!(compared >= 40, "oracle scope unexpectedly shrank");
    println!("EPG_READ_ROW_CASES={compared}");
}
