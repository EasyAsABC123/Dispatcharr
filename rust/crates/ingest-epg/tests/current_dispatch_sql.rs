//! Source missing-index publication failure classification; app route unhooked.
//! Original Dispatcharr authors retain all application/algorithm credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_ingest_epg::{
    current_file::CurrentFileError,
    current_lookup::{
        CurrentLookup, CurrentLookupConfig, CurrentLookupServices, MissingIndexDispatcher,
    },
    ImportError, SwapPolicyContext,
};
use dispatcharr_store::{CurrentFileLookup, CurrentFileRequest, Store, StoreError};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct Failure {
    calls: AtomicUsize,
}
impl MissingIndexDispatcher for Failure {
    fn dispatch(
        &self,
        _: i64,
        context: SwapPolicyContext,
    ) -> BoxFuture<'_, Result<(), ImportError>> {
        Box::pin(async move {
            context.ensure_active()?;
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(ImportError::Worker)
        })
    }
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
#[ignore = "requires Original dependency-failure HTTP golden and copied PostgreSQL"]
async fn dispatch_failure_is_distinct_http500_and_never_parsing_in_real_store_bridge() {
    assert_eq!(
        std::env::var("EPG_PATH_DISPOSABLE_FIXTURE").unwrap(),
        "playback-fixture-only"
    );
    let data: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("EPG_PATH_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let case = &data["current_dispatch_failure"];
    assert_eq!(case["status"], 500);
    assert_eq!(case["mime"], "text/html; charset=utf-8");
    assert_eq!(case["calls"].as_array().unwrap().len(), 1);
    let id = case["source_id"].as_i64().unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 1)
        .await
        .unwrap();
    let owner: String = sqlx::query_scalar("SELECT name FROM epg_epgsource WHERE id=$1")
        .bind(id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(owner, "rust-current-dispatch-owned");
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM epg_epgsourceindex WHERE source_id=$1")
            .bind(id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    let failure = Arc::new(Failure {
        calls: AtomicUsize::new(0),
    });
    let gateway = CurrentLookup::new(
        CurrentLookupServices {
            store: store.clone(),
            dispatcher: failure.clone(),
            clock: Arc::new(chrono::Utc::now),
        },
        CurrentLookupConfig {
            max_jobs: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let request = CurrentFileRequest {
        source_id: id,
        epg_entry_id: case["epg_entry_id"].as_i64().unwrap(),
        source_type: "xmltv".into(),
        tvg_id: case["tvg_id"].as_str().unwrap().into(),
        file_path: Some(case["file_path"].as_str().unwrap().into()),
        extracted_file_path: None,
        cache_file_path: "/evidence/unused-dispatch-cache".into(),
    };
    let dropped = Arc::new(AtomicUsize::new(0));
    let raw = gateway
        .lookup(request.clone(), Arc::new(DropCount(dropped.clone())))
        .await;
    assert!(matches!(
        raw,
        Err(CurrentFileError::Dispatch(ImportError::Worker))
    ));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let mapped = CurrentFileLookup::lookup(
        &gateway,
        request.clone(),
        Arc::new(DropCount(dropped.clone())),
    )
    .await;
    assert!(matches!(mapped, Err(StoreError::StatusBody(500, _))));
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    let result = store
        .current_epg_with_file_lookup(
            vec![request.epg_entry_id],
            chrono::Utc::now(),
            4 * 1024 * 1024,
            "/evidence/unused-dispatch-cache".into(),
            Arc::new(gateway.clone()),
            Arc::new(DropCount(dropped.clone())),
        )
        .await;
    assert!(matches!(result, Err(StoreError::StatusBody(500, _))));
    assert_eq!(failure.calls.load(Ordering::SeqCst), 3);
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    // Exercise the actual native HTTP adapter and its unchanged HTML renderer,
    // rather than inferring HTTP behavior from the component error variant.
    let auth = dispatcharr_store::AuthService::new(
        store.clone(),
        dispatcharr_store::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let service = dispatcharr_store::EpgProgramReads::new(
        auth,
        dispatcharr_store::EpgProgramReadLimits::default(),
    )
    .unwrap()
    .with_current_file_lookup(
        Arc::new(gateway.clone()),
        "/evidence/unused-dispatch-cache".into(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();
    let router = service
        .clone()
        .current_router()
        .layer(axum::Extension(axum::extract::ConnectInfo(peer)));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let now = chrono::Utc::now().timestamp();
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &serde_json::json!({"user_id":case["user_id"],"token_type":"access","iat":now,"exp":now+60,"jti":"owned-current-dispatch-http"}),
        &jsonwebtoken::EncodingKey::from_secret(b"playback-fixture-only"),
    ).unwrap();
    let response = reqwest::Client::new()
        .post(format!("http://{address}/api/epg/current-programs/"))
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(serde_json::json!({"epg_data_ids":[request.epg_entry_id]}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 500);
    assert_eq!(
        response.headers()["content-type"],
        case["mime"].as_str().unwrap()
    );
    assert_eq!(response.headers()["allow"], "POST, OPTIONS");
    let body = response.bytes().await.unwrap();
    use base64::Engine;
    let expected = base64::engine::general_purpose::STANDARD
        .decode(case["body_base64"].as_str().unwrap())
        .unwrap();
    assert_eq!(body.as_ref(), expected);
    assert_eq!(failure.calls.load(Ordering::SeqCst), 4);
    let malformed = data["malformed_index_http"]
        .as_array()
        .expect("actual Original six-shape HTTP corpus");
    assert_eq!(malformed.len(), 6);
    for observed in malformed {
        sqlx::query("INSERT INTO epg_epgsourceindex(source_id,data,updated_at) VALUES($1,$2,now()) ON CONFLICT(source_id) DO UPDATE SET data=excluded.data,updated_at=now()")
            .bind(id).bind(sqlx::types::Json(observed["index"].clone())).execute(store.pool()).await.unwrap();
        let response = reqwest::Client::new().post(format!("http://{address}/api/epg/current-programs/"))
            .bearer_auth(jsonwebtoken::encode(
                &jsonwebtoken::Header::default(),
                &serde_json::json!({"user_id":case["user_id"],"token_type":"access","iat":now,"exp":now+60,"jti":"owned-malformed-index-http"}),
                &jsonwebtoken::EncodingKey::from_secret(b"playback-fixture-only"),
            ).unwrap())
            .header("content-type", "application/json")
            .body(serde_json::json!({"epg_data_ids":[request.epg_entry_id]}).to_string())
            .send().await.unwrap();
        assert_eq!(
            response.status().as_u16(),
            observed["status"].as_u64().unwrap() as u16,
            "{}",
            observed["name"]
        );
        assert_eq!(
            response.headers()["content-type"],
            observed["mime"].as_str().unwrap()
        );
        let bytes = response.bytes().await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            base64::engine::general_purpose::STANDARD
                .decode(observed["body_base64"].as_str().unwrap())
                .unwrap()
        );
    }
    assert_eq!(
        failure.calls.load(Ordering::SeqCst),
        4,
        "malformed present indices must not publish"
    );
    sqlx::query("DELETE FROM epg_epgsourceindex WHERE source_id=$1")
        .bind(id)
        .execute(store.pool())
        .await
        .unwrap();

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert!(service.shutdown(Duration::from_secs(2)).await);

    assert!(gateway.shutdown(Duration::from_secs(2)).await);
    assert_eq!(gateway.physical_jobs(), 0);
    assert_eq!(gateway.retained_memory_bytes(), 0);
    store.pool().close().await;
}
