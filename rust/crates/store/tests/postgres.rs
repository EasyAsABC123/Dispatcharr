//! Explicit opt-in real-schema check. Never silently passes without its fixture.
use dispatcharr_contracts::{PlaybackRequest, PlaybackResolver};
use dispatcharr_store::{AuthConfig, AuthService, Store};

fn required(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("Required isolated test fixture variable missing: {name}"))
}

#[tokio::test]
#[ignore = "requires an isolated Django-migrated PostgreSQL fixture and Django admin JWT; see store README"]
async fn real_django_schema_serialization_and_playback_authorization() {
    assert_eq!(required("DISPATCHARR_STORE_TEST_FIXTURE"), "isolated");
    let store = Store::connect(&required("TEST_DATABASE_URL"), 2)
        .await
        .expect("isolated PostgreSQL must be available");
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new(required("TEST_DJANGO_SECRET_KEY")).unwrap(),
    );
    let request = PlaybackRequest {
        channel_id: required("TEST_CHANNEL_UUID"),
        query: vec![("output_format".into(), "ts".into())],
        headers: vec![(
            "authorization".into(),
            format!("Bearer {}", required("TEST_DJANGO_ADMIN_ACCESS_TOKEN")),
        )],
        peer_ip: "127.0.0.1".parse().unwrap(),
    };
    let user = auth
        .authenticate(&request)
        .await
        .expect("Django token authentication must work")
        .expect("admin credential must resolve");
    assert!(user.is_active && user.user_level >= 10);
    auth.authorize_admin(request.clone())
        .await
        .expect("fixture admin must pass network policy");
    let serialized = store
        .serialized_user(user.id)
        .await
        .expect("existing UserSerializer columns and relation must load");
    assert_eq!(serialized["id"], user.id);
    assert!(serialized.get("password").is_none());
    assert!(serialized["channel_profiles"].is_array());
    assert!(store.serialized_permissions().await.unwrap().is_array());
    assert!(store.serialized_groups(None).await.unwrap().is_array());
    let source = auth
        .resolve(request)
        .await
        .expect("native Proxy fixture channel must resolve");
    assert!(source.url.starts_with(&required("TEST_UPSTREAM_PREFIX")));
    assert_eq!(source.provider_max_streams, 0);
}
