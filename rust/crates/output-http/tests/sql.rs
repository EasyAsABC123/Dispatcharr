//! Actual original Django export schema and Source body-hash comparisons.
//! All original application credit belongs to Dispatcharr authors/contributors.
use dispatcharr_output_http::{
    reader::{self, Read},
    render::{self, Context},
};
use dispatcharr_store::Store;
use sha2::{Digest, Sha256};

#[tokio::test]
#[ignore = "requires disposable actual original Django export fixture"]
async fn original_m3u_bytes_match_actual_sql_and_bounded_native_renderer() {
    assert_eq!(
        std::env::var("M3U_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let database = std::env::var("DATABASE_TEST_URL").unwrap();
    assert!(database.contains("playback-fixture-only@"));
    let store = Store::connect(&database, 8).await.unwrap();
    let oracle: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var("M3U_SOURCE_ORACLE").unwrap()).unwrap(),
    )
    .unwrap();
    let mut count = 0;
    for event in oracle["events"].as_array().unwrap() {
        let Some(fixture) = event.get("render_fixture") else {
            continue;
        };
        let query: Vec<(String, String)> =
            serde_json::from_value(fixture["query"].clone()).unwrap();
        let mut user = if let Some(kind) = fixture["user_kind"].as_str() {
            Some(
                store
                    .user_by_username(&format!("rust-export-{kind}"))
                    .await
                    .unwrap()
                    .unwrap(),
            )
        } else {
            None
        };
        if fixture["provider_allow_none"] == true {
            user.as_mut().unwrap().custom_properties["allowed_m3u_profile_ids"] =
                serde_json::json!([]);
        }
        let direct = query
            .iter()
            .rev()
            .find(|(key, _)| key == "direct")
            .is_some_and(|(_, value)| value.eq_ignore_ascii_case("true"))
            && user.as_ref().is_none_or(|user| user.user_level >= 10);
        let profile = fixture["profile_name"].as_str();
        let channels = reader::read(
            &store,
            Read {
                user: user.as_ref(),
                profile,
                direct,
                max_channels: 50000,
                max_metadata_bytes: 16 * 1024 * 1024,
            },
        )
        .await
        .unwrap();
        let context = Context {
            origin: fixture["origin"].as_str().unwrap(),
            profile,
            query: &query,
            xc: user
                .as_ref()
                .map(|user| (user.username.as_str(), "export-fixture-only")),
            direct,
            restricted_direct: channels.restricted_direct,
            catchup: fixture["catchup"].as_bool().unwrap(),
            max_bytes: 8 * 1024 * 1024,
        };
        let body = render::render(&context, &channels.rows).unwrap();
        let digest = format!("{:x}", Sha256::digest(&body));
        assert_eq!(
            body.len(),
            event["bytes"].as_u64().unwrap() as usize,
            "{} body length",
            event["case"]
        );
        assert_eq!(
            digest,
            event["sha256"].as_str().unwrap(),
            "{} exact Source M3U bytes",
            event["case"]
        );
        count += 1;
    }
    assert!(
        count
            >= if oracle.get("large_catalog").is_some() {
                3
            } else {
                25
            },
        "all captured positive render cases must be compared"
    );
    for (max_channels, max_metadata_bytes) in [(1, 16 * 1024 * 1024), (50000, 1)] {
        assert!(
            reader::read(
                &store,
                Read {
                    user: None,
                    profile: Some("rust-export-profile"),
                    direct: false,
                    max_channels,
                    max_metadata_bytes
                }
            )
            .await
            .is_err(),
            "capacity overflow must not silently truncate output"
        );
    }
    println!(
        "{}",
        serde_json::json!({"actual_source_render_cases":count,"bounded_capacity_refusals":2,"http_auth_and_cache_parity_verified":false})
    );
}
