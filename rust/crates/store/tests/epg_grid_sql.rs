//! Actual original guide visibility/stored programme component oracle.
//! All application credit remains with original Dispatcharr contributors.
use dispatcharr_store::{grid_channel_profile, grid_parse_datetime, grid_resolve_window, Store};
use serde_json::{json, Value};
use std::collections::HashSet;

#[tokio::test]
#[ignore = "requires actual original guide fixture and disposable PostgreSQL"]
async fn original_grid_metadata_and_stored_frames() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GUIDE_READ_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let now = grid_parse_datetime(fixture["clock"].as_str().unwrap(), "now").unwrap();
    let mut compared = 0;
    for case in fixture["cases"].as_array().unwrap() {
        let Some(expected) = case.get("grid_channels") else {
            continue;
        };
        let params: Vec<(String, String)> = url::form_urlencoded::parse(
            case["path"]
                .as_str()
                .unwrap()
                .split_once('?')
                .map(|(_, q)| q)
                .unwrap_or("")
                .as_bytes(),
        )
        .into_owned()
        .collect();
        let profile = grid_channel_profile(&params).unwrap();
        let user = store
            .user_by_username(&format!(
                "rust-probe-guide-http-{}",
                case["role"].as_str().unwrap()
            ))
            .await
            .unwrap()
            .unwrap();
        let rows = store
            .grid_visible_channels(&user, profile.as_deref())
            .await
            .unwrap();
        let actual:Vec<Value>=rows.iter().map(|c|json!({"id":c.id,"uuid":c.uuid,"name":c.name,"epg_id":c.epg_id,"source_type":c.source_type,"source_properties":c.source_properties})).collect();
        assert_eq!(
            json!(actual),
            *expected,
            "metadata {} role{}",
            case["path"],
            case["role"]
        );
        let ids: HashSet<i64> = rows
            .iter()
            .filter(|c| c.source_type.as_deref() != Some("dummy"))
            .filter_map(|c| c.epg_id)
            .collect();
        let (start, end) = grid_resolve_window(
            &params,
            now.clone(),
            fixture["history_max_days"].as_i64().unwrap(),
        )
        .unwrap();
        let utc = |time: dispatcharr_store::GridTimestamp| {
            time.local.and_utc() - chrono::Duration::microseconds(time.offset_micros)
        };
        let frames = store
            .grid_stored_frames(&ids.into_iter().collect::<Vec<_>>(), utc(start), utc(end))
            .await
            .unwrap();
        let mut body = b"{\"data\":[".to_vec();
        for frame in &frames {
            body.extend_from_slice(frame);
        }
        body.extend_from_slice(b"]}");
        let body: Value = serde_json::from_slice(&body).unwrap();
        let expected_rows: Vec<Value> = case["expected"]["json"]["data"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["id"].is_number())
            .cloned()
            .collect();
        assert_eq!(
            body["data"],
            json!(expected_rows),
            "stored {} role{}",
            case["path"],
            case["role"]
        );
        compared += 1;
    }
    assert!(
        compared >= 20,
        "insufficient Source guide coverage: {compared}"
    );
    println!("GRID_METADATA_STORED_CASES={compared}");
}
