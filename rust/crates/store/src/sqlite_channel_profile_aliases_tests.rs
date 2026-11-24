//! Scoped ordinary signed route checks; captured DDL, zero fake migrations.
use super::*;
use crate::auth::sqlite_runtime::channel_profile_aliases::{self as entry, Owner};
use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, OriginalUri, State},
    http::{header, HeaderValue, Method, Request, StatusCode},
    response::Response,
    Router,
};
use dispatcharr_contracts::BoxFuture;
use sha2::{Digest, Sha256};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Connection, SqliteConnection,
};
use tower::ServiceExt;

#[derive(Clone)]
struct TestOwner {
    pool: SqlitePool,
    config: AuthConfig,
    slots: Arc<Semaphore>,
    limit: usize,
}
impl Owner for TestOwner {
    fn read(&self) -> BoxFuture<'_, Result<RetainedSqliteRestReads, StoreError>> {
        Box::pin(async move {
            let permit = Arc::new(
                self.slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| capacity())?,
            );
            let mut limits = RestReadLimits::default();
            limits.work_bytes = self.limit;
            Ok(RetainedSqliteRestReads {
                config: self.config.clone(),
                cache: None,
                pool: self.pool.clone(),
                permit,
                limits,
            })
        })
    }
}
async fn owner(path: Option<&std::path::Path>, profiles: bool) -> TestOwner {
    let options = if let Some(path) = path {
        SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
    } else {
        SqliteConnectOptions::new().in_memory(true)
    }
    .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let mut db = pool.acquire().await.unwrap();
    let catalog: Vec<Vec<Value>> = serde_json::from_str(include_str!(
        "../datasets/sqlite-operator-upgraded-catalog.json"
    ))
    .unwrap();
    for kind in ["table", "index"] {
        for row in &catalog {
            if row[0] == kind {
                sqlx::raw_sql(row[3].as_str().unwrap())
                    .execute(&mut *db)
                    .await
                    .unwrap();
            }
        }
    }
    sqlx::raw_sql("INSERT INTO accounts_user(id,password,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,stream_limit) VALUES
      (1,'!',1,'profile-admin','','','',1,1,'2026-01-01',10,'{}',0),
      (2,'!',0,'profile-standard','','','',0,1,'2026-01-01',1,'{}',0),
      (3,'!',1,'profile-inactive','','','',1,0,'2026-01-01',10,'{}',0),
      (4,'!',0,'profile-viewer','','','',0,1,'2026-01-01',0,'{}',0),
      (5,'!',0,'profile-unassigned','','','',0,1,'2026-01-01',1,'{}',0)")
        .execute(&mut *db).await.unwrap();
    if profiles {
        sqlx::raw_sql("INSERT INTO dispatcharr_channels_channelprofile(id,name) VALUES(7,'Assigned'),(12,'Peer'),(20,'Empty');
          INSERT INTO dispatcharr_channels_channel(id,name,uuid,user_level,auto_created,created_at,updated_at,is_adult,hidden_from_output,is_catchup,catchup_days,is_radio) VALUES
          (42,'Visible','00000000000000000000000000000042',0,0,'2026-01-01','2026-01-01',0,0,0,0,0),
          (18,'Second','00000000000000000000000000000018',0,0,'2026-01-01','2026-01-01',0,0,0,0,0),
          (11,'Disabled','00000000000000000000000000000011',0,0,'2026-01-01','2026-01-01',0,0,0,0,0),
          (99,'Peer','00000000000000000000000000000099',0,0,'2026-01-01','2026-01-01',0,0,0,0,0);
          INSERT INTO dispatcharr_channels_channelprofilemembership(id,enabled,channel_id,channel_profile_id) VALUES
          (1,1,42,7),(2,0,11,7),(3,1,18,7),(4,1,99,12);
          INSERT INTO accounts_user_channel_profiles(user_id,channelprofile_id) VALUES(2,20),(2,7)")
            .execute(&mut *db).await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM django_migrations")
            .fetch_one(&mut *db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sqlite_schema WHERE type='table'")
            .fetch_one(&mut *db)
            .await
            .unwrap(),
        63
    );
    drop(db);
    TestOwner {
        pool,
        config: AuthConfig::new("offline-signed-profile-read-only".into()).unwrap(),
        slots: Arc::new(Semaphore::new(1)),
        limit: RestReadLimits::default().work_bytes,
    }
}
async fn canonical(
    State(owner): State<TestOwner>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, StoreError> {
    let value = owner
        .read()
        .await?
        .channel_profile_collection(peer, headers.clone())
        .await?;
    crate::channel_profiles::canonical_json_response(value, &uri, &headers)
}
fn router(owner: TestOwner) -> Router {
    entry::router_for::<TestOwner>()
        .route("/api/channels/profiles/", axum::routing::get(canonical))
        .with_state(owner)
}
fn token(owner: &TestOwner, id: i64) -> String {
    crate::auth::mint_token(&owner.config, json!(id), "access").unwrap()
}
fn request(path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .uri(path)
        .header(header::ACCEPT, "application/json");
    if let Some(token) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let mut request = builder.body(Body::empty()).unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo("127.0.0.1:3030".parse::<SocketAddr>().unwrap()));
    request
}
async fn bytes(response: Response, status: StatusCode) -> Vec<u8> {
    assert_eq!(response.status(), status);
    to_bytes(response.into_body(), 65536)
        .await
        .unwrap()
        .to_vec()
}
async fn changes(owner: &TestOwner) -> i64 {
    sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(&owner.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn copied_profile_alias_collection_signed_roles_match_canonical_exact_bytes() {
    let owner = owner(None, true).await;
    let initial = changes(&owner).await;
    for (id, expected) in [
        (
            1,
            json!([{"id":7,"name":"Assigned","channels":[42,18]},
       {"id":12,"name":"Peer","channels":[99]},{"id":20,"name":"Empty","channels":[]}]),
        ),
        (
            2,
            json!([{"id":7,"name":"Assigned","channels":[42,18]},{"id":20,"name":"Empty","channels":[]}]),
        ),
    ] {
        let bearer = token(&owner, id);
        let original = bytes(
            router(owner.clone())
                .oneshot(request(
                    "/api/channels/profiles/?format=json",
                    Some(&bearer),
                ))
                .await
                .unwrap(),
            StatusCode::OK,
        )
        .await;
        assert_eq!(
            serde_json::from_slice::<Value>(&original).unwrap(),
            expected
        );
        for path in [
            "/api/channels/profiles.json",
            "/api/channels/profiles.json/",
            "/api/channels/profiles.json?format=api",
            "/api/channels/profiles.json/?format=api",
        ] {
            let response = router(owner.clone())
                .oneshot(request(path, Some(&bearer)))
                .await
                .unwrap();
            assert_eq!(
                response.headers()[header::ALLOW],
                "GET, POST, HEAD, OPTIONS"
            );
            assert_eq!(response.headers()[header::VARY], "Accept, origin");
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                original.len().to_string()
            );
            assert_eq!(bytes(response, StatusCode::OK).await, original);
        }
    }
    assert_eq!(changes(&owner).await, initial);
    assert_eq!(owner.slots.available_permits(), 1);
    owner.pool.close().await;
}

#[tokio::test]
async fn copied_profile_alias_detail_shares_signed_scope_and_suffix_precedence() {
    let owner = owner(None, true).await;
    for (user, id, expected) in [
        (1, 12, json!({"id":12,"name":"Peer","channels":[99]})),
        (2, 7, json!({"id":7,"name":"Assigned","channels":[42,18]})),
    ] {
        let bearer = token(&owner, user);
        let mut first = None;
        for path in [
            format!("/api/channels/profiles/{id}/?format=json"),
            format!("/api/channels/profiles/{id}.json"),
            format!("/api/channels/profiles/{id}.json/"),
            format!("/api/channels/profiles/{id}.json?format=api"),
        ] {
            let response = router(owner.clone())
                .oneshot(request(&path, Some(&bearer)))
                .await
                .unwrap();
            assert_eq!(
                response.headers()[header::ALLOW],
                "GET, PUT, PATCH, DELETE, HEAD, OPTIONS"
            );
            let value = bytes(response, StatusCode::OK).await;
            assert_eq!(serde_json::from_slice::<Value>(&value).unwrap(), expected);
            if let Some(first) = &first {
                assert_eq!(&value, first);
            } else {
                first = Some(value);
            }
        }
    }
    let standard = token(&owner, 2);
    for path in [
        "/api/channels/profiles/12.json",
        "/api/channels/profiles/999.json/",
    ] {
        let value = bytes(
            router(owner.clone())
                .oneshot(request(path, Some(&standard)))
                .await
                .unwrap(),
            StatusCode::NOT_FOUND,
        )
        .await;
        assert_eq!(
            serde_json::from_slice::<Value>(&value).unwrap(),
            json!({"detail":"No ChannelProfile matches the given query."})
        );
    }
    owner.pool.close().await;
}

#[tokio::test]
async fn copied_profile_alias_real_signature_account_role_and_network_denials() {
    let owner = owner(None, true).await;
    let initial = changes(&owner).await;
    let wrong = crate::auth::mint_token(
        &AuthConfig::new("another-offline-signing-key".into()).unwrap(),
        json!(1),
        "access",
    )
    .unwrap();
    let refresh = crate::auth::mint_token(&owner.config, json!(1), "refresh").unwrap();
    for (bearer, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(wrong), StatusCode::UNAUTHORIZED),
        (Some(refresh), StatusCode::UNAUTHORIZED),
        (Some(token(&owner, 3)), StatusCode::UNAUTHORIZED),
        (Some(token(&owner, 4)), StatusCode::FORBIDDEN),
    ] {
        for path in [
            "/api/channels/profiles.json",
            "/api/channels/profiles/7.json",
        ] {
            let response = router(owner.clone())
                .oneshot(request(path, bearer.as_deref()))
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()[header::VARY], "Accept, origin");
        }
    }
    assert_eq!(changes(&owner).await, initial);
    sqlx::query(
        "INSERT INTO core_coresettings(key,name,value) VALUES('network_access','owned-policy',?)",
    )
    .bind(r#"{"UI":"198.51.100.0/24"}"#)
    .execute(&owner.pool)
    .await
    .unwrap();
    let bearer = token(&owner, 1);
    let configured = changes(&owner).await;
    assert_eq!(
        router(owner.clone())
            .oneshot(request("/api/channels/profiles/7.json", Some(&bearer)))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(changes(&owner).await, configured);
    assert_eq!(owner.slots.available_permits(), 1);
    owner.pool.close().await;
}

#[tokio::test]
async fn copied_profile_alias_renderer_head_and_unimplemented_methods_preserve_read_only_scope() {
    let owner = owner(None, true).await;
    let bearer = token(&owner, 1);
    let initial = changes(&owner).await;
    let mut pretty = request("/api/channels/profiles/7.json?format=api", Some(&bearer));
    pretty.headers_mut().insert(
        header::ACCEPT,
        HeaderValue::from_static("application/json; indent=2"),
    );
    let body = bytes(
        router(owner.clone()).oneshot(pretty).await.unwrap(),
        StatusCode::OK,
    )
    .await;
    assert!(body.contains(&b'\n'));
    assert_eq!(serde_json::from_slice::<Value>(&body).unwrap()["id"], 7);
    let mut html = request("/api/channels/profiles.json", None);
    html.headers_mut()
        .insert(header::ACCEPT, HeaderValue::from_static("text/html"));
    assert_eq!(
        router(owner.clone()).oneshot(html).await.unwrap().status(),
        StatusCode::NOT_ACCEPTABLE
    );
    for path in [
        "/api/channels/profiles.json/",
        "/api/channels/profiles/7.json/",
    ] {
        let mut head = request(path, Some(&bearer));
        *head.method_mut() = Method::HEAD;
        let response = router(owner.clone()).oneshot(head).await.unwrap();
        assert!(response.headers().contains_key(header::CONTENT_LENGTH));
        assert!(bytes(response, StatusCode::OK).await.is_empty());
        for method in [Method::POST, Method::PATCH, Method::DELETE, Method::OPTIONS] {
            let mut write = request(path, Some(&bearer));
            *write.method_mut() = method;
            assert_eq!(
                router(owner.clone()).oneshot(write).await.unwrap().status(),
                StatusCode::NOT_FOUND
            );
        }
    }
    for path in [
        "/api/channels/profiles/7",
        "/api/channels/profiles/7.json.json/",
        "/api/channels/profiles/nope.json",
    ] {
        assert_eq!(
            router(owner.clone())
                .oneshot(request(path, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    assert_eq!(changes(&owner).await, initial);
    owner.pool.close().await;
}

#[tokio::test]
async fn copied_profile_alias_file_is_unchanged_reopens_with_no_migrations_or_sidecars() {
    let path =
        std::env::temp_dir().join(format!("copied-profile-read-{}.db", uuid::Uuid::new_v4()));
    let owner = owner(Some(&path), true).await;
    let before = Sha256::digest(std::fs::read(&path).unwrap());
    let initial = changes(&owner).await;
    let bearer = token(&owner, 2);
    for uri in [
        "/api/channels/profiles.json",
        "/api/channels/profiles/7.json/",
        "/api/channels/profiles/12.json",
    ] {
        let response = router(owner.clone())
            .oneshot(request(uri, Some(&bearer)))
            .await
            .unwrap();
        let expected = if uri.ends_with("12.json") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::OK
        };
        bytes(response, expected).await;
    }
    assert_eq!(changes(&owner).await, initial);
    owner.pool.close().await;
    assert_eq!(Sha256::digest(std::fs::read(&path).unwrap()), before);
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new().filename(&path).read_only(true),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM django_migrations")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        0
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut db)
        .await
        .unwrap()
        .is_empty());
    db.close().await.unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(!std::path::Path::new(&format!("{}{suffix}", path.display())).exists());
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn copied_profile_alias_work_and_request_capacity_errors_release_same_owner_slot() {
    let owner = owner(None, true).await;
    let bearer = token(&owner, 1);
    let mut bounded = owner.clone();
    bounded.limit = 1;
    for path in [
        "/api/channels/profiles.json",
        "/api/channels/profiles/7.json/",
    ] {
        assert_eq!(
            router(bounded.clone())
                .oneshot(request(path, Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(owner.slots.available_permits(), 1);
    }
    let held = owner.slots.clone().acquire_owned().await.unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(request("/api/channels/profiles.json", Some(&bearer)))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(held);
    assert_eq!(
        router(owner.clone())
            .oneshot(request("/api/channels/profiles/7.json", Some(&bearer)))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    owner.pool.close().await;
}

#[tokio::test]
async fn copied_profile_alias_empty_and_unassigned_contexts_never_create_profiles() {
    for seeded in [false, true] {
        let owner = owner(None, seeded).await;
        let bearer = token(&owner, 5);
        let initial = changes(&owner).await;
        assert_eq!(
            bytes(
                router(owner.clone())
                    .oneshot(request("/api/channels/profiles.json", Some(&bearer)))
                    .await
                    .unwrap(),
                StatusCode::OK
            )
            .await,
            b"[]"
        );
        assert_eq!(
            router(owner.clone())
                .oneshot(request("/api/channels/profiles/7.json", Some(&bearer)))
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(changes(&owner).await, initial);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM dispatcharr_channels_channelprofile"
            )
            .fetch_one(&owner.pool)
            .await
            .unwrap(),
            if seeded { 3 } else { 0 }
        );
        owner.pool.close().await;
    }
}
