//! Owned actual Source/native task-phase SQL comparator, not API acceptance.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::{
    vod_api_owned_read::{ReadLimits, ReadRuntime},
    vod_api_provider_refresh_actor as actor,
};
use serde_json::Value;
use std::{io::Read, time::Duration};

// This standalone component peer does not run Main's middleware stack. Keep
// its captured Django security defaults explicit; do not waive those headers
// in the real Original/native HTTP comparison.
async fn source_response_defaults(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut response = next.run(request).await;
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        ("referrer-policy", "same-origin"),
        ("cross-origin-opener-policy", "same-origin"),
    ] {
        response
            .headers_mut()
            .entry(name)
            .or_insert(axum::http::HeaderValue::from_static(value));
    }
    response
}
#[tokio::test]
#[ignore = "Owned local-provider Original SQL mutation fixture only"]
async fn native_refresh_movie_actor_peer() {
    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input).unwrap();
    assert!(input.len() < 256 * 1024);
    let config: Value = serde_json::from_str(&input).unwrap();
    assert_eq!(config["fixture_guard"], "playback-fixture-only");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect("postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/dispatcharr")
        .await
        .unwrap();
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool.clone()),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect("redis://:playback-fixture-only@127.0.0.1:6379/0")
            .await
            .unwrap(),
    );
    let runtime = ReadRuntime::new(ReadLimits::default()).unwrap();
    let result = runtime
        .run(1, move |work| async move {
            let clock = actor::Clock {
                fixed: config["fixed_clock"].as_str().map(|v| {
                    chrono::DateTime::parse_from_rfc3339(v)
                        .unwrap()
                        .with_timezone(&chrono::Utc)
                }),
            };
            let result = if config["kind"] == "series" {
                let mut connection = work.connection(&pool).await?;
                let result = super::vod_api_provider_refresh_sql::series_view(
                    &work,
                    &mut connection,
                    config["relation_id"].as_i64().unwrap(),
                )
                .await;
                connection.finish().await?;
                let snapshot = result?.ok_or(super::vod_api_owned_read::ReadError::WorkerFailed)?;
                let props = &snapshot["relation"]["custom_properties"];
                let last = snapshot["relation"]["last_episode_refresh"]
                    .as_str()
                    .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok());
                let now = clock.fixed.unwrap_or_else(chrono::Utc::now);
                let refresh = config["force"].as_bool().unwrap()
                    || !super::vod_api_projection::truthy(&props["detailed_fetched"])
                    || !super::vod_api_projection::truthy(&props["episodes_fetched"])
                    || last.is_none_or(|last| {
                        now.signed_duration_since(last)
                            .num_microseconds()
                            .is_some_and(|age| {
                                age > config["refresh_hours"].as_i64().unwrap_or(24) * 3_600_000_000
                            })
                    });
                if refresh {
                    actor::series(&work, &auth, &snapshot, clock).await
                } else {
                    Ok(())
                }
            } else {
                actor::movie(
                    &work,
                    &auth,
                    config["relation_id"].as_i64().unwrap(),
                    config["force"].as_bool().unwrap(),
                    clock,
                )
                .await
            };
            result?;
            Ok(b"{\"outcome\":\"complete\"}".to_vec())
        })
        .await;
    let report = match result {
        Ok(frame) => {
            let result = serde_json::from_slice::<Value>(&frame).unwrap();
            drop(frame);
            result
        }
        Err(error) => serde_json::json!({"outcome":"error","error":format!("{error:?}")}),
    };
    assert!(runtime.shutdown(Duration::from_secs(5)).await);
    assert_eq!(runtime.physical_workers(), 0);
    assert_eq!(runtime.retained_memory_bytes(), 0);
    println!("REFRESH_ACTOR_RESULT={report}");
}
// Fixture-only cancellation witness; no production/auth/request identifiers.
#[derive(Default)]
struct ObserverControl {
    waiting: std::sync::atomic::AtomicBool,
    dropped: std::sync::atomic::AtomicUsize,
    completed: std::sync::atomic::AtomicUsize,
    forced: std::sync::atomic::AtomicUsize,
    signal: tokio::sync::Notify,
}
struct ObserverWait {
    control: std::sync::Arc<ObserverControl>,
    completed: bool,
}
impl Drop for ObserverWait {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        if self.completed {
            self.control.completed.fetch_add(1, Ordering::AcqRel);
        } else {
            self.control.dropped.fetch_add(1, Ordering::AcqRel);
        }
        self.control.waiting.store(false, Ordering::Release);
    }
}
async fn observer_loss_witness(
    axum::extract::State(control): axum::extract::State<std::sync::Arc<ObserverControl>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use std::sync::atomic::Ordering;
    if request
        .headers()
        .get("x-owned-observer-loss")
        .is_none_or(|value| value.as_bytes() != b"playback-fixture-only")
    {
        return next.run(request).await;
    }
    if control.waiting.swap(true, Ordering::AcqRel) {
        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    // The future is declared after the guard, so it drops before the counter
    // acknowledges actual handler/ReadRuntime receiver cancellation.
    let mut guard = ObserverWait {
        control: control.clone(),
        completed: false,
    };
    let future = next.run(request);
    tokio::pin!(future);
    tokio::select! {
        reply = &mut future => { guard.completed=true; reply },
        _ = control.signal.notified() => {
            control.forced.fetch_add(1, Ordering::AcqRel);
            axum::http::StatusCode::BAD_GATEWAY.into_response()
        }
    }
}
#[tokio::test]
#[ignore = "Owned actual provider refresh HTTP fixture"]
async fn native_refresh_http_server() {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect("postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/dispatcharr")
        .await
        .unwrap();
    let auth = crate::AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect("redis://:playback-fixture-only@127.0.0.1:6379/0")
            .await
            .unwrap(),
    );
    let reader = super::vod_api_http::VodReads::new(auth, Default::default()).unwrap();
    // Fixture-only scalar observer/control. The server itself is guarded above
    // by VOD_API_NATIVE_FIXTURE; neither route is added to production routers.
    // Deliberately bypass work admission so an exhausted/closed lane remains
    // observable without SQL/auth or another ledger grant.
    let observed = reader.clone();
    let close = reader.clone();
    let control = std::sync::Arc::new(ObserverControl::default());
    let observed_control = control.clone();
    let drop_control = control.clone();
    let router = reader
        .router()
        .merge(reader.provider_router())
        .route(
            "/__owned_provider_refresh__/state",
            axum::routing::get(move || {
                let observed = observed.clone();
                let control = observed_control.clone();
                async move {
                    use std::sync::atomic::Ordering;
                    let mut state = observed.scheduler_state();
                    state["observer_waiting"] = control.waiting.load(Ordering::Acquire).into();
                    state["observers_dropped"] = control.dropped.load(Ordering::Acquire).into();
                    state["observers_completed"] = control.completed.load(Ordering::Acquire).into();
                    state["explicit_observer_drops"] =
                        control.forced.load(Ordering::Acquire).into();
                    axum::Json(state)
                }
            }),
        )
        .route(
            "/__owned_provider_refresh__/close-admission",
            axum::routing::post(move || {
                let close = close.clone();
                async move {
                    let drained = close.shutdown(Duration::from_millis(20)).await;
                    axum::Json(
                        serde_json::json!({"drained":drained,"state":close.scheduler_state()}),
                    )
                }
            }),
        )
        .route(
            "/__owned_provider_refresh__/drop-observer",
            axum::routing::post(move || {
                let control = drop_control.clone();
                async move {
                    use std::sync::atomic::Ordering;
                    let triggered = control.waiting.load(Ordering::Acquire);
                    if triggered {
                        control.signal.notify_one();
                    }
                    axum::Json(serde_json::json!({"triggered":triggered}))
                }
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            control,
            observer_loss_witness,
        ))
        .layer(axum::middleware::from_fn(source_response_defaults));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:9193")
        .await
        .unwrap();
    std::fs::write("/evidence/native-refresh-ready", "ready").unwrap();
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        while !std::path::Path::new("/evidence/native-refresh-stop").exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(reader.shutdown(Duration::from_secs(10)).await);
}
