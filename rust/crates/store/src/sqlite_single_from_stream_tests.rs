//! Component route tests using exact captured DDL, ordinary signed JWTs and real SQLite.
//! They never manufacture migration history or call Main datastore admission.
use super::*;
use crate::auth::sqlite_runtime::single_from_stream::{self as entry, Owner, Sink};
use axum::{
    body::{to_bytes, Body},
    extract::{ConnectInfo, OriginalUri, State},
    http::{Request, StatusCode},
    Json, Router,
};
use dispatcharr_contracts::{BoxFuture, UpdateEventError, UpdateEventSink};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Connection, SqliteConnection,
};
use std::sync::Mutex;
use tower::ServiceExt;

#[derive(Clone)]
struct TestOwner {
    pool: SqlitePool,
    config: AuthConfig,
    slots: Arc<Semaphore>,
    sink: Option<Sink>,
}
impl Owner for TestOwner {
    fn request(&self) -> Result<RetainedSqliteRestReads, StoreError> {
        let permit = Arc::new(
            self.slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| capacity())?,
        );
        Ok(RetainedSqliteRestReads {
            config: self.config.clone(),
            cache: None,
            pool: self.pool.clone(),
            permit,
            limits: RestReadLimits::default(),
        })
    }
    fn sink(&self) -> Option<Sink> {
        self.sink.clone()
    }
}
#[derive(Default)]
struct Capture {
    messages: Mutex<Vec<(String, Value)>>,
    fail: bool,
}
impl UpdateEventSink for Capture {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            self.messages.lock().unwrap().push((group.into(), message));
            if self.fail {
                Err(UpdateEventError::Unavailable)
            } else {
                Ok(())
            }
        })
    }
}
struct Gate {
    entered: Semaphore,
    release: Semaphore,
    messages: Mutex<Vec<(String, Value)>>,
}
impl UpdateEventSink for Gate {
    fn send<'a>(
        &'a self,
        group: &'a str,
        message: Value,
    ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
        Box::pin(async move {
            self.messages.lock().unwrap().push((group.into(), message));
            self.entered.add_permits(1);
            let _release = self.release.acquire().await.unwrap();
            Ok(())
        })
    }
}
async fn schema(db: &mut SqliteConnection) {
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
    sqlx::raw_sql(r#"
      INSERT INTO accounts_user(id,password,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,stream_limit)
        VALUES(1,'!',1,'unit-admin','','','',1,1,'2026-01-01',10,'{}',0),
              (2,'!',0,'unit-standard','','','',0,1,'2026-01-01',1,'{}',0),
              (3,'!',1,'unit-inactive','','','',1,0,'2026-01-01',10,'{}',0);
      INSERT INTO m3u_m3uaccount(id,name,max_streams,is_active,created_at,locked,refresh_interval,stale_stream_days,account_type,status,priority)
        VALUES(1,'custom',0,1,'2026-01-01',1,0,0,'STD','idle',0);
      INSERT INTO dispatcharr_channels_channelprofile(id,name) VALUES(1,'Default unit'),(2,'Second unit');
      INSERT INTO dispatcharr_channels_stream(id,name,current_viewers,updated_at,is_custom,last_seen,is_stale,is_adult,is_catchup,catchup_days,is_radio,m3u_account_id,url,stream_chno,tvg_id,custom_properties)
        VALUES(1,'Unit selected stream',0,'2026-01-01 00:00:00',1,'2026-01-01 00:00:00',0,1,1,7,1,1,'http://unit.invalid/stream',30.5,'unit-no-guide','{"tvc-guide-stationid":"unit-station"}');
    "#).execute(&mut *db).await.unwrap();
    let migrations: i64 = sqlx::query_scalar("SELECT count(*) FROM django_migrations")
        .fetch_one(&mut *db)
        .await
        .unwrap();
    assert_eq!(migrations, 0);
}
async fn owner(path: Option<&std::path::Path>, sink: Option<Sink>) -> TestOwner {
    let options = match path {
        Some(p) => SqliteConnectOptions::new()
            .filename(p)
            .create_if_missing(true),
        None => SqliteConnectOptions::new().in_memory(true),
    }
    .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    {
        let mut db = pool.acquire().await.unwrap();
        schema(&mut db).await;
    }
    TestOwner {
        pool,
        config: AuthConfig::new("offline-unit-signature-only".into()).unwrap(),
        slots: Arc::new(Semaphore::new(1)),
        sink,
    }
}
async fn readback(
    State(owner): State<TestOwner>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
    OriginalUri(uri): OriginalUri,
) -> Result<Json<Value>, StoreError> {
    Ok(Json(
        owner
            .request()?
            .channel_read(peer, headers, &uri, HashMap::new(), ChannelRead::Detail(id))
            .await?,
    ))
}
fn router(owner: TestOwner) -> Router {
    Router::new()
        .route(
            "/api/channels/channels/from-stream/",
            axum::routing::post(entry::handle::<TestOwner>),
        )
        .route("/api/channels/channels/{id}/", axum::routing::get(readback))
        .with_state(owner)
}
fn token(owner: &TestOwner, id: i64) -> String {
    crate::auth::mint_token(&owner.config, json!(id), "access").unwrap()
}
fn request(path: &str, body: Option<Value>, bearer: Option<&str>) -> Request<Body> {
    let mut request = Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri(path)
        .header("host", "localhost")
        .header("content-type", "application/json");
    if let Some(bearer) = bearer {
        request = request.header("authorization", format!("Bearer {bearer}"));
    }
    let mut request = request
        .body(body.map_or_else(Body::empty, |v| Body::from(serde_json::to_vec(&v).unwrap())))
        .unwrap();
    request
        .extensions_mut()
        .insert(ConnectInfo("127.0.0.1:3030".parse::<SocketAddr>().unwrap()));
    request
}
async fn value(response: axum::response::Response, status: StatusCode) -> Value {
    assert_eq!(response.status(), status);
    serde_json::from_slice(&to_bytes(response.into_body(), 262144).await.unwrap()).unwrap()
}
async fn counts(pool: &SqlitePool) -> Vec<i64> {
    let mut result = vec![];
    for table in [
        "dispatcharr_channels_channel",
        "dispatcharr_channels_channelstream",
        "dispatcharr_channels_channelprofilemembership",
        "dispatcharr_channels_channelgroup",
        "dispatcharr_channels_channelgroupm3uaccount",
    ] {
        result.push(
            sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(pool)
                .await
                .unwrap(),
        );
    }
    result
}

#[tokio::test]
async fn sqlite_single_entry_signed_admin_durable_default_profiles_and_post_commit_event() {
    let path = std::env::temp_dir().join(format!(
        "dispatcharr-entry-unit-{}.db",
        uuid::Uuid::new_v4()
    ));
    let capture = Arc::new(Capture::default());
    let owner = owner(Some(&path), Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let response = router(owner.clone())
        .oneshot(request(
            "/api/channels/channels/from-stream/",
            Some(json!({"stream_id":1,"channel_number":null,"channel_profile_ids":null})),
            Some(&bearer),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.headers()[axum::http::header::ALLOW],
        "POST, OPTIONS"
    );
    let created = value(response, StatusCode::CREATED).await;
    let id = created["id"].as_i64().unwrap();
    assert_eq!(created["name"], "Unit selected stream");
    assert_eq!(created["channel_number"], 30.5);
    assert_eq!(created["streams"], json!([1]));
    assert_eq!(created["tvg_id"], "unit-no-guide");
    assert_eq!(created["tvc_guide_stationid"], "unit-station");
    assert_eq!(created["is_radio"], true);
    assert_eq!(created["is_adult"], true);
    assert_eq!(created["is_catchup"], false);
    assert_eq!(created["catchup_days"], 0);
    let read = value(
        router(owner.clone())
            .oneshot(request(
                &format!("/api/channels/channels/{id}/"),
                None,
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(read["is_catchup"], true);
    assert_eq!(read["catchup_days"], 7);
    assert_eq!(read["streams"], json!([1]));
    let links: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT stream_id,\"order\" FROM dispatcharr_channels_channelstream WHERE channel_id=?",
    )
    .bind(id)
    .fetch_all(&owner.pool)
    .await
    .unwrap();
    assert_eq!(links, vec![(1, 0)]);
    let memberships:Vec<(i64,bool)>=sqlx::query_as("SELECT channel_profile_id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=? ORDER BY channel_profile_id").bind(id).fetch_all(&owner.pool).await.unwrap();
    assert_eq!(memberships, vec![(1, true), (2, true)]);
    let group: (String,bool,bool,String)=sqlx::query_as("SELECT g.name,r.enabled,r.auto_channel_sync,r.custom_properties FROM dispatcharr_channels_channelgroup g JOIN dispatcharr_channels_channelgroupm3uaccount r ON r.channel_group_id=g.id WHERE r.m3u_account_id=1").fetch_one(&owner.pool).await.unwrap();
    assert_eq!(group, ("Default Group".into(), true, false, "{}".into()));
    let messages = capture.messages.lock().unwrap().clone();
    assert_eq!(
        messages,
        vec![(
            "updates".into(),
            json!({"type":"update","data":{"type":"channels_created","count":1,"channel_id":id,"channel_name":"Unit selected stream","channel_number":30.5}})
        )]
    );
    owner.pool.close().await;
    let reopened = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let mut reopened = reopened;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM dispatcharr_channels_channelprofilemembership WHERE enabled"
        )
        .fetch_one(&mut reopened)
        .await
        .unwrap(),
        2
    );
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut reopened)
        .await
        .unwrap()
        .is_empty());
    reopened.close().await.unwrap();
    for suffix in ["-wal", "-shm", "-journal"] {
        assert!(!std::path::Path::new(&format!("{}{suffix}", path.display())).exists());
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn sqlite_single_entry_denies_unsigned_wrong_key_inactive_nonadmin_and_unsupported_without_writes(
) {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture.clone())).await;
    let before = counts(&owner.pool).await;
    let admin = token(&owner, 1);
    let standard = token(&owner, 2);
    let inactive = token(&owner, 3);
    let wrong = crate::auth::mint_token(
        &AuthConfig::new("wrong-offline-key".into()).unwrap(),
        json!(1),
        "access",
    )
    .unwrap();
    for (bearer, status) in [
        (None, StatusCode::UNAUTHORIZED),
        (Some(wrong.as_str()), StatusCode::UNAUTHORIZED),
        (Some(inactive.as_str()), StatusCode::UNAUTHORIZED),
        (Some(standard.as_str()), StatusCode::FORBIDDEN),
    ] {
        let response = router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                bearer,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), status);
    }
    for (body, status) in [
        (json!({}), StatusCode::BAD_REQUEST),
        (json!({"stream_id":999}), StatusCode::NOT_FOUND),
        (
            json!({"stream_id":1,"channel_profile_ids":[1,2]}),
            StatusCode::NOT_IMPLEMENTED,
        ),
    ] {
        assert_eq!(
            router(owner.clone())
                .oneshot(request(
                    "/api/channels/channels/from-stream/",
                    Some(body),
                    Some(&admin)
                ))
                .await
                .unwrap()
                .status(),
            status
        );
    }
    let mut no_sink = owner.clone();
    no_sink.sink = None;
    assert_eq!(
        router(no_sink)
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&admin)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    sqlx::query("UPDATE dispatcharr_channels_stream SET logo_url='http://unit.invalid/logo'")
        .execute(&owner.pool)
        .await
        .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&admin)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(counts(&owner.pool).await, before);
    assert!(capture.messages.lock().unwrap().is_empty());
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_reserved_provider_number_and_late_budget_failure_are_atomic() {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let first = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let second = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(first["channel_number"], 30.5);
    assert_eq!(second["channel_number"], 31.5);
    assert_eq!(counts(&owner.pool).await, vec![2, 2, 4, 1, 1]);
    let before = counts(&owner.pool).await;
    let actor = User {
        id: 1,
        username: "unit-admin".into(),
        password: "!".into(),
        is_active: true,
        user_level: 10,
        stream_limit: 0,
        custom_properties: json!({}),
    };
    {
        let mut db = owner.pool.acquire().await.unwrap();
        let result = entry::command(
            &mut db,
            &actor,
            Ok(Json(json!({"stream_id":1}))),
            &url::Url::parse("http://localhost/api/channels/channels/from-stream/").unwrap(),
            1,
        )
        .await;
        assert!(result.is_err());
        db.ping().await.unwrap();
    }
    assert_eq!(counts(&owner.pool).await, before);
    assert_eq!(capture.messages.lock().unwrap().len(), 2);
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_commit_precedes_publication_and_observer_loss_retains_slot() {
    let gate = Arc::new(Gate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        messages: Mutex::new(vec![]),
    });
    let owner = owner(None, Some(gate.clone())).await;
    let bearer = token(&owner, 1);
    let task = tokio::spawn(router(owner.clone()).oneshot(request(
        "/api/channels/channels/from-stream/",
        Some(json!({"stream_id":1})),
        Some(&bearer),
    )));
    let _entered = tokio::time::timeout(std::time::Duration::from_secs(3), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap();
    // Pool size is one: this query also proves SQL settled before sink delivery.
    assert_eq!(counts(&owner.pool).await, vec![1, 1, 2, 1, 1]);
    assert_eq!(owner.slots.available_permits(), 0);
    task.abort();
    let _ = task.await;
    assert_eq!(owner.slots.available_permits(), 0);
    gate.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while owner.slots.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(gate.messages.lock().unwrap().len(), 1);
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_plain_source_publication_failure_keeps_committed_success() {
    let capture = Arc::new(Capture {
        messages: Mutex::new(vec![]),
        fail: true,
    });
    let owner = owner(None, Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    assert_eq!(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    assert_eq!(counts(&owner.pool).await, vec![1, 1, 2, 1, 1]);
    assert_eq!(capture.messages.lock().unwrap().len(), 1);
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_number_modes_reserve_raw_and_override_and_preserve_existing_group_marker(
) {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture)).await;
    let bearer = token(&owner, 1);
    let first = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let id = first["id"].as_i64().unwrap();
    sqlx::query("INSERT INTO dispatcharr_channels_channeloverride(channel_id,channel_number,created_at,updated_at) VALUES(?,40.25,'2026-01-01','2026-01-01')").bind(id).execute(&owner.pool).await.unwrap();
    sqlx::query("UPDATE dispatcharr_channels_channelgroupm3uaccount SET enabled=0,auto_channel_sync=1,last_seen='2026-01-01'").execute(&owner.pool).await.unwrap();
    for (number, expected) in [(0.0, 1.0), (-1.0, 41.0), (40.25, 41.25)] {
        let result = value(
            router(owner.clone())
                .oneshot(request(
                    "/api/channels/channels/from-stream/",
                    Some(json!({"stream_id":1,"channel_number":number})),
                    Some(&bearer),
                ))
                .await
                .unwrap(),
            StatusCode::CREATED,
        )
        .await;
        assert_eq!(result["channel_number"], expected);
    }
    let marker:(bool,bool,String)=sqlx::query_as("SELECT enabled,auto_channel_sync,last_seen FROM dispatcharr_channels_channelgroupm3uaccount").fetch_one(&owner.pool).await.unwrap();
    assert_eq!(marker, (false, true, "2026-01-01".into()));
    assert_eq!(counts(&owner.pool).await, vec![4, 4, 8, 1, 1]);
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_policy_guide_renderer_and_capacity_guards_precede_writes() {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let before = counts(&owner.pool).await;
    sqlx::query(
        "INSERT INTO core_coresettings(key,name,value) VALUES('network_access','unit-policy',?)",
    )
    .bind(r#"{"UI":"198.51.100.0/24"}"#)
    .execute(&owner.pool)
    .await
    .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    sqlx::query("DELETE FROM core_coresettings WHERE key='network_access'")
        .execute(&owner.pool)
        .await
        .unwrap();
    let mut html = request(
        "/api/channels/channels/from-stream/",
        Some(json!({"stream_id":1})),
        Some(&bearer),
    );
    html.headers_mut()
        .insert("accept", axum::http::HeaderValue::from_static("text/html"));
    assert_eq!(
        router(owner.clone()).oneshot(html).await.unwrap().status(),
        StatusCode::NOT_IMPLEMENTED
    );
    sqlx::query("INSERT INTO epg_epgdata(name,tvg_id) VALUES('unit-guide','unit-no-guide')")
        .execute(&owner.pool)
        .await
        .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    sqlx::query("DELETE FROM epg_epgdata")
        .execute(&owner.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE dispatcharr_channels_stream SET custom_properties=?")
        .bind(json!({"large":"x".repeat(66000)}).to_string())
        .execute(&owner.pool)
        .await
        .unwrap();
    assert_eq!(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer)
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_IMPLEMENTED
    );
    assert_eq!(counts(&owner.pool).await, before);
    assert!(capture.messages.lock().unwrap().is_empty());
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_default_all_does_not_fabricate_a_profile_when_none_exists() {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    sqlx::query("DELETE FROM dispatcharr_channels_channelprofile")
        .execute(&owner.pool)
        .await
        .unwrap();
    let created = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1,"channel_profile_ids":null})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(created["streams"], json!([1]));
    assert_eq!(counts(&owner.pool).await, vec![1, 1, 0, 1, 1]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatcharr_channels_channelprofile")
            .fetch_one(&owner.pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(capture.messages.lock().unwrap().len(), 1);
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_existing_stream_group_gets_manual_marker_without_default_group() {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    sqlx::query("INSERT INTO dispatcharr_channels_channelgroup(id,name) VALUES(9,'unit existing stream group')").execute(&owner.pool).await.unwrap();
    sqlx::query("UPDATE dispatcharr_channels_stream SET channel_group_id=9")
        .execute(&owner.pool)
        .await
        .unwrap();
    let created = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    assert_eq!(created["channel_group_id"], 9);
    assert_eq!(created["effective_channel_group_id"], 9);
    assert_eq!(counts(&owner.pool).await, vec![1, 1, 2, 1, 1]);
    let marker:(i64,bool)=sqlx::query_as("SELECT channel_group_id,enabled FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=1").fetch_one(&owner.pool).await.unwrap();
    assert_eq!(marker, (9, true));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM dispatcharr_channels_channelgroup WHERE name='Default Group'"
        )
        .fetch_one(&owner.pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(capture.messages.lock().unwrap().len(), 1);
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_one_existing_selected_profile_is_durable_and_publishes_once() {
    let path = std::env::temp_dir().join(format!(
        "dispatcharr-selected-profile-unit-{}.db",
        uuid::Uuid::new_v4()
    ));
    let capture = Arc::new(Capture::default());
    let owner = owner(Some(&path), Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let created = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1,"channel_number":0,"channel_profile_ids":[2]})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let id = created["id"].as_i64().unwrap();
    assert_eq!(created["channel_number"], 1.0);
    assert_eq!(created["streams"], json!([1]));
    let members:Vec<(i64,bool)>=sqlx::query_as("SELECT channel_profile_id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=? ORDER BY channel_profile_id").bind(id).fetch_all(&owner.pool).await.unwrap();
    assert_eq!(members, vec![(2, true)]);
    assert_eq!(counts(&owner.pool).await, vec![1, 1, 1, 1, 1]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatcharr_channels_channelprofile")
            .fetch_one(&owner.pool)
            .await
            .unwrap(),
        2
    );
    let read = value(
        router(owner.clone())
            .oneshot(request(
                &format!("/api/channels/channels/{id}/"),
                None,
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(read["is_catchup"], true);
    assert_eq!(read["catchup_days"], 7);
    let messages = capture.messages.lock().unwrap().clone();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].0, "updates");
    assert_eq!(messages[0].1["data"]["channel_id"], id);
    owner.pool.close().await;
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let members:Vec<(i64,bool)>=sqlx::query_as("SELECT channel_profile_id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=?").bind(id).fetch_all(&mut db).await.unwrap();
    assert_eq!(members, vec![(2, true)]);
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
async fn sqlite_single_entry_unqualified_profile_errors_never_approximate_source_partial_commit() {
    let capture = Arc::new(Capture::default());
    let owner = owner(None, Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let before = counts(&owner.pool).await;
    for selection in [
        json!([999]),
        json!([2, 2]),
        json!([1, 2]),
        json!(2),
        json!(["2"]),
        json!([false]),
        json!([-1]),
        json!([0]),
    ] {
        let result = router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1,"channel_profile_ids":selection})),
                Some(&bearer),
            ))
            .await
            .unwrap();
        assert_eq!(result.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(counts(&owner.pool).await, before);
    }
    assert!(capture.messages.lock().unwrap().is_empty());
    owner.pool.close().await;
}

#[tokio::test]
async fn sqlite_single_entry_explicit_none_profile_is_durable_and_publishes_once() {
    let path = std::env::temp_dir().join(format!(
        "dispatcharr-none-profile-unit-{}.db",
        uuid::Uuid::new_v4()
    ));
    let capture = Arc::new(Capture::default());
    let owner = owner(Some(&path), Some(capture.clone())).await;
    let bearer = token(&owner, 1);
    let created = value(
        router(owner.clone())
            .oneshot(request(
                "/api/channels/channels/from-stream/",
                Some(json!({"stream_id":1,"channel_number":-1,"channel_profile_ids":[]})),
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::CREATED,
    )
    .await;
    let id = created["id"].as_i64().unwrap();
    assert_eq!(created["channel_number"], 1.0);
    assert_eq!(created["streams"], json!([1]));
    let members:Vec<(i64,bool)>=sqlx::query_as("SELECT channel_profile_id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=? ORDER BY channel_profile_id").bind(id).fetch_all(&owner.pool).await.unwrap();
    assert_eq!(members, Vec::<(i64, bool)>::new());
    assert_eq!(counts(&owner.pool).await, vec![1, 1, 0, 1, 1]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatcharr_channels_channelprofile")
            .fetch_one(&owner.pool)
            .await
            .unwrap(),
        2
    );
    let read = value(
        router(owner.clone())
            .oneshot(request(
                &format!("/api/channels/channels/{id}/"),
                None,
                Some(&bearer),
            ))
            .await
            .unwrap(),
        StatusCode::OK,
    )
    .await;
    assert_eq!(read["is_catchup"], true);
    assert_eq!(read["catchup_days"], 7);
    let messages = capture.messages.lock().unwrap().clone();
    assert_eq!(
        messages,
        vec![(
            "updates".into(),
            json!({"type":"update","data":{"type":"channels_created","count":1,"channel_id":id,"channel_name":"Unit selected stream","channel_number":1.0}})
        )]
    );
    owner.pool.close().await;
    let mut db = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(&path)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let members:Vec<(i64,bool)>=sqlx::query_as("SELECT channel_profile_id,enabled FROM dispatcharr_channels_channelprofilemembership WHERE channel_id=?").bind(id).fetch_all(&mut db).await.unwrap();
    assert_eq!(members, Vec::<(i64, bool)>::new());
    let links: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT stream_id,\"order\" FROM dispatcharr_channels_channelstream WHERE channel_id=?",
    )
    .bind(id)
    .fetch_all(&mut db)
    .await
    .unwrap();
    assert_eq!(links, vec![(1, 0)]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM dispatcharr_channels_channelprofile")
            .fetch_one(&mut db)
            .await
            .unwrap(),
        2
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
