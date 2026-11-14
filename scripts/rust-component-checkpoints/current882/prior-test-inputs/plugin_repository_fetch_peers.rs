//! Ignored real-library peers. No Source application or default test workload.
//! Controller must bind schema-only owned PG/Redis, exact Source epoch/imported
//! seed cache, and the real public-class internal HTTP origin. No SSRF override.
use super::*;
use crate::{AuthConfig, NotificationCache, Store};
use sha2::Sha256;
use std::{
    path::{Path, PathBuf},
    sync::{Mutex, Weak},
    time::Instant,
};
use tokio::sync::OwnedSemaphorePermit;
use tower::ServiceExt;
fn save(path: &Path, value: &Value) {
    let raw = serde_json::to_vec_pretty(value).unwrap();
    assert!(raw.len() <= 131072);
    std::fs::write(path, raw).unwrap();
}
struct Context {
    auth: AuthService,
    store: Store,
    output: PathBuf,
    reference: Value,
    count: usize,
}
async fn context() -> Context {
    assert_eq!(
        std::env::var("PLUGIN_UNSIGNED_AUTHORITY").as_deref(),
        Ok("owned-unsigned-literal-http-only")
    );
    let db = std::env::var("POSTGRES_DB").unwrap();
    let tail = db.strip_prefix("rust_probe_pluginrepo_").unwrap();
    assert_eq!(tail.len(), 10);
    assert!(tail.bytes().all(|b| b.is_ascii_hexdigit()));
    let url = std::env::var("DATABASE_TEST_URL").unwrap();
    assert_eq!(reqwest::Url::parse(&url).unwrap().path(), format!("/{db}"));
    let epoch = std::env::var("PLUGIN_UNSIGNED_EPOCH").unwrap();
    let (filename, count) = match epoch.as_str() {
        "retained24" => ("Source24-private.json", 24),
        "fresh404" => ("Source404-private.json", 1),
        _ => panic!("ExactSourceEpoch"),
    };
    let root = PathBuf::from(std::env::var("PLUGIN_UNSIGNED_INPUTS").unwrap());
    let output = PathBuf::from(std::env::var("PLUGIN_UNSIGNED_OUTPUT").unwrap());
    assert!(output.is_dir());
    let raw = std::fs::read(root.join(filename)).unwrap();
    assert!(raw.len() <= 67108864);
    assert_eq!(
        format!("{:x}", <Sha256 as sha2::Digest>::digest(&raw)),
        std::env::var("PLUGIN_UNSIGNED_SOURCE_SHA256").unwrap()
    );
    let reference: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(reference["actual_request_count"], count);
    assert_eq!(reference["Source_framework_APIClient_only"], true);
    assert_eq!(reference["force_authenticate_used"], false);
    assert_eq!(reference["client_raise_request_exception"], false);
    let redis = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(redis.ends_with("/1"));
    let store = Store::connect(&url, 4).await.unwrap();
    let cache = NotificationCache::connect(&redis).await.unwrap();
    let auth = AuthService::new(
        store.clone(),
        AuthConfig::new("owned-unsigned-native-signing-only".into()).unwrap(),
    )
    .with_notification_cache(cache);
    let settings = reference["seed"]["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["key"] == "system_settings")
        .unwrap()["value"]
        .clone();
    auth.set_display_timezone_on_settings_save("system_settings", &settings);
    assert_eq!(auth.user_agent_workers.available_permits(), 4);
    save(
        &output.join("context.json"),
        &json!({"epoch":epoch,"Source_authority_sha256":format!("{:x}",<Sha256 as sha2::Digest>::digest(raw)),"actual_reference_requests":count,"source_seed_context":"strict imported exact initial Source cache state; no fabricated versions/history reset","Native_model_signal_claim":false,"Main":false,"domain":PluginRepositories::unsigned_manifest_domain()}),
    );
    Context {
        auth,
        store,
        output,
        reference,
        count,
    }
}
async fn wait_file(path: &Path, seconds: u64) {
    let end = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while !path.is_file() {
        assert!(
            tokio::time::Instant::now() < end,
            "FiniteFileCue:{}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
async fn settle(ctx: &Context) {
    let workers = ctx.auth.user_agent_workers.clone();
    let drained = tokio::time::timeout(
        Duration::from_secs(15),
        workers.clone().acquire_many_owned(4),
    )
    .await
    .unwrap()
    .unwrap();
    drop(drained);
    assert_eq!(workers.available_permits(), 4);
    ctx.store.pool().close().await;
    assert!(ctx.store.pool().is_closed());
    assert_eq!(ctx.store.pool().size(), 0);
    save(
        &ctx.output.join("pool-closed-ready.json"),
        &json!({"actual_four_slots":workers.available_permits(),"closed":ctx.store.pool().is_closed(),"size":ctx.store.pool().size(),"idle_diagnostic":ctx.store.pool().num_idle(),"max":4,"backend_absence":"requires actual independent while-ELF query"}),
    );
    wait_file(&ctx.output.join("backend-observer-done"), 15).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires exact sealed Native Source24/fresh1 epoch, owned PG/Redis/internal origin; no app by default"]
async fn actual_unsigned_manifest_source_epoch_peer() {
    let ctx = context().await;
    let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = counter.clone();
    let app = PluginRepositories::new(ctx.auth.clone())
        .with_unsigned_manifest_fetches()
        .router()
        .layer(axum::middleware::from_fn(
            move |request: Request, next: axum::middleware::Next| {
                let count = observed.clone();
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    next.run(request).await
                }
            },
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = rx.await;
        })
        .await
    });
    save(
        &ctx.output.join("peer-ready.json"),
        &json!({"address":address.to_string(),"expected_exact_requests":ctx.count,"actual_router":"PluginRepositories::with_unsigned_manifest_fetches","Main":false,"HTTP_readiness_probes":0,"same_four_slots":ctx.auth.user_agent_workers.available_permits()}),
    );
    wait_file(&ctx.output.join("peer-stop"), 300).await;
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), ctx.count);
    settle(&ctx).await;
    save(
        &ctx.output.join("peer-terminal.json"),
        &json!({"passed":true,"actual_requests":counter.load(std::sync::atomic::Ordering::SeqCst),"actual_server_exit0":true,"pool_closed_size0":true,"same_four_slots":ctx.auth.user_agent_workers.available_permits(),"Main_or_complete_M8":false}),
    );
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires exact sealed owned HTTP held-wire context and real AuthService/PG/Redis; no default sockets"]
async fn actual_owned_http_held_reply_peer() {
    let ctx = context().await;
    assert_eq!(
        std::env::var("PLUGIN_UNSIGNED_EPOCH").as_deref(),
        Ok("retained24")
    );
    let stage = std::env::var("PLUGIN_UNSIGNED_PHYSICAL_STAGE").unwrap();
    assert!(matches!(stage.as_str(), "headers-abort" | "body-deadline"));
    let target = std::env::var("PLUGIN_UNSIGNED_PHYSICAL_ORIGIN").unwrap();
    let url = reqwest::Url::parse(&target).unwrap();
    assert_eq!(url.scheme(), "http");
    assert_eq!(url.host_str(), Some("11.253.250.2"));
    assert_eq!(url.port(), Some(18080));
    assert_eq!(url.path(), format!("/physical/{stage}.json"));
    assert!(policy(&url).is_ok());
    let original = Arc::new(Mutex::new(Vec::<Weak<OwnedSemaphorePermit>>::new()));
    let observer = original.clone();
    let app = PluginRepositories::new(ctx.auth.clone())
        .with_unsigned_manifest_fetches()
        .observe_original_writer(Arc::new(move |weak| observer.lock().unwrap().push(weak)))
        .router();
    let admin = ctx.reference["seed"]["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["user_level"] == 10 && row["custom_properties"] == json!({}))
        .unwrap();
    let key = admin["api_key"].as_str().unwrap();
    let body = serde_json::to_vec(&json!({"url":target})).unwrap();
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/plugins/repos/preview/")
        .header("content-type", "application/json")
        .header("x-api-key", key)
        .body(axum::body::Body::from(body))
        .unwrap();
    request.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:19001".parse::<SocketAddr>().unwrap(),
    ));
    let mut task = Some(tokio::spawn(app.oneshot(request)));
    wait_file(&ctx.output.join("origin-wire-held.json"), 15).await;
    let weak = {
        let values = original.lock().unwrap();
        assert_eq!(values.len(), 1);
        values[0].clone()
    };
    assert!(
        !task.as_ref().unwrap().is_finished(),
        "NoResponseBeforeRealHeldWire"
    );
    let began = Instant::now();
    let fate;
    if stage == "headers-abort" {
        let observer = task.take().unwrap();
        observer.abort();
        assert!(observer.await.unwrap_err().is_cancelled());
        fate="actual Router observer JoinHandle abort; supervised work retained, not inferred client cancellation";
    } else {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), task.as_mut().unwrap())
                .await
                .is_err()
        );
        fate = "actual100ms observer deadline; join handle borrowed, runtime actor/HTTP unchanged";
    }
    let mut samples = Vec::new();
    for _ in 0..2 {
        assert!(weak.upgrade().is_some());
        assert_eq!(ctx.auth.user_agent_workers.available_permits(), 3);
        assert!(ctx.output.join("origin-wire-held.json").is_file());
        samples.push(json!({"elapsed_seconds":began.elapsed().as_secs_f64(),"original_weak_some":weak.upgrade().is_some(),"same_four_slots_available":ctx.auth.user_agent_workers.available_permits()}));
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    save(
        &ctx.output.join("native-held-observations.json"),
        &json!({"stage":stage,"fate":fate,"samples":samples,"no_dummy_owner":true,"target":target,"transport":"actual owned TcpStream, unmodified policy; no mock/bypass"}),
    );
    std::fs::write(
        ctx.output.join("origin-wire-release"),
        b"release after sustained actual owner samples\n",
    )
    .unwrap();
    if stage == "body-deadline" {
        let response = tokio::time::timeout(Duration::from_secs(15), task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), RESPONSE_LIMIT)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["valid"],
            true
        );
        let clone = bytes.clone();
        drop(bytes);
        assert!(weak.upgrade().is_some());
        assert_eq!(ctx.auth.user_agent_workers.available_permits(), 3);
        drop(clone);
    }
    let end = tokio::time::Instant::now() + Duration::from_secs(15);
    while weak.upgrade().is_some() {
        assert!(
            tokio::time::Instant::now() < end,
            "ActualOriginalOwnerRetirement"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(ctx.auth.user_agent_workers.available_permits(), 4);
    wait_file(&ctx.output.join("origin-client-eof.json"), 15).await;
    settle(&ctx).await;
    save(
        &ctx.output.join("peer-terminal.json"),
        &json!({"passed":true,"actual_requests":1,"stage":stage,"same_four_slots":4,"original_weak_none_after_settlement":weak.upgrade().is_none(),"actual_origin_eof_receipt":"origin-client-eof.json independently validate real socket generation/fate","actual_held_samples":samples,"Main_or_SQLReady_or_clientFDloss_claim":false}),
    );
}
