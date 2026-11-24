//! Read-only opt-in test against a disposable Django-migrated PostgreSQL fixture.
use dispatcharr_contracts::{PlaybackRequest, PlaybackResolver};
use dispatcharr_store::{AuthConfig, AuthService, Store};

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("Missing isolated player fixture variable {name}"))
}

#[tokio::test]
#[ignore = "requires a disposable Django-migrated PostgreSQL fixture with three grouped STD channels"]
async fn grouped_std_pool_is_preserved_for_automatic_and_configured_input_commands() {
    assert_eq!(required("DISPATCHARR_STORE_TEST_FIXTURE"), "isolated");
    let store = Store::connect(&required("TEST_DATABASE_URL"), 2)
        .await
        .expect("isolated PostgreSQL must be available");
    let auth = AuthService::new(
        store,
        AuthConfig::new(required("TEST_DJANGO_SECRET_KEY")).unwrap(),
    );
    let token = required("TEST_DJANGO_ADMIN_ACCESS_TOKEN");
    let expected_counter = required("TEST_STD_SAMPLE_COUNTER_KEY");
    let mut seen_channels = std::collections::BTreeSet::new();
    for (fixture, executable) in [
        ("TEST_STD_PROXY_TS_CHANNEL_UUID", None),
        ("TEST_STD_PROXY_HLS_CHANNEL_UUID", Some("ffmpeg")),
        ("TEST_STD_CONFIGURED_TS_CHANNEL_UUID", Some("cvlc")),
    ] {
        let channel_id = required(fixture);
        assert!(seen_channels.insert(channel_id.clone()));
        let source = auth
            .resolve(PlaybackRequest {
                channel_id,
                query: vec![("output_format".into(), "ts".into())],
                headers: vec![("authorization".into(), format!("Bearer {token}"))],
                peer_ip: "127.0.0.1".parse().unwrap(),
            })
            .await
            .unwrap_or_else(|error| {
                panic!("{fixture} failed to resolve with status {}", error.status)
            });
        let input = source.input_command.as_ref().map(|command| {
            assert!(command.argv.contains(&source.url));
            std::path::Path::new(&command.argv[0])
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap()
        });
        assert_eq!(input, executable, "{fixture} input transport");
        assert_eq!(
            source.url.ends_with(".m3u8"),
            fixture == "TEST_STD_PROXY_HLS_CHANNEL_UUID"
        );
        let pool = source.credential_pool.expect("finite grouped STD pool");
        assert!(
            pool.counter_key == expected_counter,
            "{fixture} pool identity"
        );
        assert_eq!(pool.max_streams, 1);
        assert_eq!(source.provider_max_streams, 1);
        if fixture == "TEST_STD_CONFIGURED_TS_CHANNEL_UUID" {
            assert_eq!(source.alternates.len(), 1);
            let alternate = &source.alternates[0];
            assert_eq!(alternate.credential_pool.as_ref(), Some(&pool));
            assert_eq!(alternate.provider_max_streams, 1);
            assert!(alternate
                .input_command
                .as_ref()
                .is_some_and(|command| command.argv.contains(&alternate.url)));
        }
    }
}
