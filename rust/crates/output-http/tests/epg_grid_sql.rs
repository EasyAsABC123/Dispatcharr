//! Actual Source complete grid entities, independent of HTTP permission gates.
//! All application credit remains with original Dispatcharr contributors.
use dispatcharr_store::Store;
use serde_json::Value;
use sha2::{Digest, Sha256};
#[tokio::test]
#[ignore = "requires actual original grid corpus and disposable PostgreSQL"]
async fn original_complete_stored_and_standard_grid_entities() {
    assert_eq!(
        std::env::var("EPG_READ_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("GUIDE_READ_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let now = chrono::DateTime::parse_from_rfc3339(fixture["clock"].as_str().unwrap())
        .unwrap()
        .with_timezone(&chrono::Utc);
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 4)
        .await
        .unwrap();
    let mut count = 0;
    for case in fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c.get("grid_channels").is_some())
    {
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
        let user = store
            .user_by_username(&format!(
                "rust-probe-guide-http-{}",
                case["role"].as_str().unwrap()
            ))
            .await
            .unwrap()
            .unwrap();
        let frames = dispatcharr_output_http::epg_grid::stored_standard_entity(
            &store,
            &user,
            &params,
            now,
            fixture["history_max_days"].as_i64().unwrap(),
        )
        .await
        .unwrap();
        let body: Vec<u8> = frames.concat();
        assert_eq!(
            body.len() as u64,
            case["expected"]["body_bytes"].as_u64().unwrap(),
            "length {} role{}",
            case["path"],
            case["role"]
        );
        let actual: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            actual, case["expected"]["json"],
            "entity {} role{}",
            case["path"], case["role"]
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&body)),
            case["expected"]["body_sha256"].as_str().unwrap(),
            "bytes {} role{}",
            case["path"],
            case["role"]
        );
        count += 1;
    }
    assert_eq!(count, 22);
    println!("GRID_EXACT_ENTITY_CASES={count}");
}
