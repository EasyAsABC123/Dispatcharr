//! Read-only opt-in test against a disposable Django-migrated PostgreSQL fixture.
use dispatcharr_contracts::{PlaybackRequest, PlaybackResolution, PlaybackResolver};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};

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

#[tokio::test]
#[ignore = "requires a disposable Django-migrated PostgreSQL fixture with a grouped STD Redirect channel and signed roles"]
async fn grouped_std_redirect_carries_pool_for_both_redirect_selection_modes() {
    assert_eq!(required("DISPATCHARR_STORE_TEST_FIXTURE"), "isolated");
    let store = Store::connect(&required("TEST_DATABASE_URL"), 2)
        .await
        .expect("isolated PostgreSQL must be available");
    let auth = AuthService::new(
        store,
        AuthConfig::new(required("TEST_DJANGO_SECRET_KEY")).unwrap(),
    );
    let channel_id = required("TEST_STD_REDIRECT_CHANNEL_UUID");
    let expected_counter = required("TEST_STD_SAMPLE_COUNTER_KEY");
    for (token_env, capacity_only) in [
        ("TEST_DJANGO_ADMIN_ACCESS_TOKEN", false),
        ("TEST_DJANGO_ALLOWED_STD_ACCESS_TOKEN", true),
    ] {
        let token = required(token_env);
        let resolved = auth
            .resolve_response(PlaybackRequest {
                channel_id: channel_id.clone(),
                query: Vec::new(),
                headers: vec![("authorization".into(), format!("Bearer {token}"))],
                peer_ip: "127.0.0.1".parse().unwrap(),
            })
            .await
            .unwrap_or_else(|error| panic!("redirect resolution returned {}", error.status));
        let PlaybackResolution::Redirect(plan) = resolved else {
            panic!("expected ordinary Redirect resolution");
        };
        assert_eq!(plan.capacity_only, capacity_only);
        let candidates = if let Some(assignment) = plan.assignment {
            assert!(!capacity_only && assignment.has_assigned_streams);
            let candidates = assignment
                .resolver
                .candidates()
                .await
                .expect("resolve assigned candidates");
            let primary = candidates.first().expect("grouped redirect candidate");
            let borrowed = assignment
                .resolver
                .resolve(primary.stream_id, primary.provider_profile_id.unwrap())
                .await
                .expect("resolve owned fixture assignment")
                .expect("assigned candidate");
            assert_eq!(
                borrowed.credential_pool.as_ref(),
                primary.credential_pool.as_ref()
            );
            candidates
        } else {
            assert!(capacity_only);
            plan.candidates
        };
        assert!(!candidates.is_empty());
        for candidate in candidates {
            assert!(candidate.input_command.is_none());
            let pool = candidate.credential_pool.expect("finite grouped STD pool");
            assert!(
                pool.counter_key == expected_counter,
                "redirect pool identity"
            );
            assert_eq!(pool.max_streams, 1);
        }
    }
}

#[tokio::test]
#[ignore = "requires owned Django-migrated PostgreSQL and Redis with one grouped STD preview"]
async fn grouped_std_preview_resolves_the_same_pool_for_default_and_scoped_workers() {
    assert_eq!(required("DISPATCHARR_STORE_TEST_FIXTURE"), "isolated");
    let store = Store::connect(&required("TEST_DATABASE_URL"), 2)
        .await
        .expect("isolated PostgreSQL must be available");
    let cache = NotificationCache::connect(&required("TEST_REDIS_URL"))
        .await
        .expect("isolated Redis must be available");
    let auth = AuthService::new(
        store,
        AuthConfig::new(required("TEST_DJANGO_SECRET_KEY")).unwrap(),
    )
    .with_notification_cache(cache);
    let hash = required("TEST_STD_PREVIEW_HASH");
    let provider: i64 = required("TEST_STD_PREVIEW_PROFILE_ID").parse().unwrap();
    let token = required("TEST_DJANGO_ADMIN_ACCESS_TOKEN");
    let counter = required("TEST_STD_SAMPLE_COUNTER_KEY");
    for scoped in [false, true] {
        let mut query = vec![("output_format".into(), "ts".into())];
        if scoped {
            query.push(("skip_redirect".into(), "TRUE".into()));
        }
        let request = PlaybackRequest {
            channel_id: hash.clone(),
            query,
            headers: vec![("authorization".into(), format!("Bearer {token}"))],
            peer_ip: "127.0.0.1".parse().unwrap(),
        };
        let resolved = auth
            .resolve_response(request)
            .await
            .unwrap_or_else(|error| panic!("preview failed with status {}", error.status));
        let PlaybackResolution::Stream(source) = resolved else {
            panic!("preview should produce media")
        };
        let worker = if scoped {
            format!("{hash}.p{provider}")
        } else {
            hash.clone()
        };
        assert_eq!(source.channel_id, worker);
        assert_eq!(source.provider_profile_id, Some(provider));
        assert_eq!(source.provider_max_streams, 1);
        let pool = source
            .credential_pool
            .expect("finite grouped STD preview pool");
        assert!(pool.counter_key == counter, "owned preview pool identity");
        assert_eq!(pool.max_streams, 1);
        let preview = source
            .metadata
            .iter()
            .find(|(name, _)| name == "source_preview")
            .unwrap();
        let projection: serde_json::Value = serde_json::from_str(&preview.1).unwrap();
        assert_eq!(projection["scoped"], scoped);
        assert_eq!(projection["hash"], hash);
    }
}
