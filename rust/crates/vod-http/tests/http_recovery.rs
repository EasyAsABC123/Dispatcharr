//! Real HTTP/valid local media state-loss contract for opt-in recovery.
//! Original Dispatcharr authors retain full application and algorithm credit.
//! Resolver is fixture-only: actual SQL/auth and actual Redis restart follow in
//! the packaged Source/native replay. This test removes only its own keys.
use axum::{body::Body, routing::get, Router};
use dispatcharr_contracts::{BoxFuture, PlaybackRequest};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_store::{StoreError, User};
use dispatcharr_vod_http::{
    ContentKind, VodCandidate, VodConfig, VodEngine, VodRequest, VodResolver, VodSource,
};
use futures_util::stream;
use redis::AsyncCommands;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};
struct Resolver(VodSource);
impl VodResolver for Resolver {
    fn authorize<'a>(
        &'a self,
        _: &'a VodRequest,
    ) -> BoxFuture<'a, Result<Option<User>, StoreError>> {
        Box::pin(async { Ok(None) })
    }
    fn resolve<'a>(
        &'a self,
        request: &'a VodRequest,
        _: Option<User>,
    ) -> BoxFuture<'a, Result<VodSource, StoreError>> {
        Box::pin(async move {
            let mut source = self.0.clone();
            // Test-only fresh resolver snapshot; production receives fresh SQL.
            if source.name == "Owned fresh idle capacity movie"
                && request.playback.header("x-owned-profile-cap") == Some("1")
            {
                for candidate in &mut source.candidates {
                    candidate.max_streams = 1;
                }
            }
            // Test-only request subject switch; no SQL/JWT authentication claim.
            if source.name == "Owned request identity movie"
                && request.playback.header("x-owned-initial-user").is_none()
            {
                source.user = None;
            }
            Ok(source)
        })
    }
    fn default_redirect(&self) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async { Ok(false) })
    }
    fn admin<'a>(&'a self, _: &'a PlaybackRequest) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async { Ok(()) })
    }
}
async fn serve(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{address}"), task)
}
struct Reader {
    response: reqwest::Response,
    pending: bytes::Bytes,
}
impl Reader {
    async fn take(&mut self, size: usize) -> Vec<u8> {
        assert!(size <= 8192);
        let mut output = Vec::with_capacity(size);
        while output.len() < size {
            if self.pending.is_empty() {
                self.pending = tokio::time::timeout(Duration::from_secs(12), self.response.chunk())
                    .await
                    .expect("bounded response read")
                    .expect("body error")
                    .expect("unexpected EOF");
                // HTTP client may coalesce unread chunks. Retain its bounded
                // immutable fixture frame and slice; never duplicate/accumulate it.
                assert!(self.pending.len() <= 5 * 1024 * 1024);
            }
            let copied = (size - output.len()).min(self.pending.len());
            output.extend_from_slice(&self.pending[..copied]);
            self.pending = self.pending.slice(copied..);
        }
        output
    }
}
async fn wait(mut condition: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(12), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP/Redis opt-in generation recovery"]
async fn actual_http_old_and_new_bodies_survive_state_loss_and_source_close_accounting() {
    run_case(false, false, false, None).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP current-generation additional entry"]
async fn actual_http_recovered_current_additional_entry_reuses_provider() {
    run_case(true, false, false, None).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP public-retired generation reopen"]
async fn actual_http_public_retired_reopen_preserves_younger_body() {
    run_case(false, true, false, None).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "owned valid MP4 + actual HTTP immediate state-loss reopen before monitor"]
async fn actual_http_immediate_reopen_does_not_wait_for_periodic_loss_monitor() {
    run_case(true, true, true, None).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual cap1 admission public stop ends only old body at100 completed yields"]
async fn actual_http_capped_recovery_consumes_policy_stop_in_only_one_body() {
    run_case(true, true, false, Some("cap")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual fixture-resolver admin stop permits new request and ends only one body at100"]
async fn actual_http_admin_public_stop_allows_pending_entry_and_younger_continuity() {
    run_case(true, true, false, Some("admin")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Source public stop at final complete100th chunk with app-default64KiB pump frames"]
async fn actual_http_public_stop_final_full_hundredth_has_no_progress_or_replay() {
    run_case(false, false, false, Some("final-full")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Source public stop at final partial100th chunk with app-default64KiB pump frames"]
async fn actual_http_public_stop_final_partial_hundredth_has_no_progress_or_replay() {
    run_case(false, false, false, Some("final-partial")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in upstream EOF holds true body until downstream completion; hard shutdown drains unpolled tail"]
async fn actual_http_source_finish_wait_preserves_shutdown_of_held_unpolled_tail() {
    run_case(false, false, false, Some("held-eof")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual HTTP Source238 authenticated-subject to headerless generation transition; fixture resolver only"]
async fn actual_http_anonymous_recreated_generation_preserves_old_body_and_subject() {
    run_case(true, true, true, Some("identity")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual HTTP Source255 qualified idle same generation success/timer/younger continuation; fixture resolver"]
async fn actual_http_public_idle_same_generation_success() {
    run_case(true, false, true, Some("idle")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual HTTP Source265 bare429 and reserved-other-owner preservation; fixture resolver"]
async fn actual_http_public_idle_busy_profile_bare429() {
    run_case(true, false, true, Some("idle-busy")).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "actual initial non-recovered idle requires immediate provider release on reclose"]
async fn actual_http_initial_public_idle_provider_release_is_immediate() {
    run_case(false, false, true, Some("idle-initial")).await;
}
async fn run_case(
    extra_entry: bool,
    retired_reopen: bool,
    immediate: bool,
    public_stop: Option<&str>,
) {
    assert_eq!(
        std::env::var("REDIS_RESTART_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let mut media =
        std::fs::read(std::env::var("VOD_RECOVERY_MEDIA").expect("owned valid MP4 fixture"))
            .unwrap();
    assert!(media.len() > 1_000_000 && media.len() <= 5 * 1024 * 1024);
    assert_eq!(&media[4..8], b"ftyp");
    let idle_case = matches!(public_stop, Some("idle" | "idle-busy"));
    let idle_busy = public_stop == Some("idle-busy");
    let identity_case = public_stop == Some("identity");
    let stopping_case = matches!(public_stop, Some("cap" | "admin"));
    let final_case = public_stop.is_some_and(|mode| mode.starts_with("final-"));
    let held_case = public_stop == Some("held-eof");
    if final_case {
        media.truncate(if public_stop == Some("final-full") {
            819200
        } else {
            819199
        });
    }
    if held_case {
        media.truncate(16384);
    }
    let media = Arc::new(media);
    let requests = Arc::new(AtomicUsize::new(0));
    let origin_data = media.clone();
    let origin_requests = requests.clone();
    // Irregular provider fragments must not shift Source's8192-byte stop units.
    let provider_fragment = if public_stop.is_some() {
        8193usize
    } else {
        8192
    };
    let (origin, origin_task) = serve(Router::new().route(
        "/fixture.mp4",
        get(move || {
            let data = origin_data.clone();
            let count = origin_requests.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let length = data.len();
                let body = stream::unfold(
                    (data, 0usize, provider_fragment),
                    |(data, position, fragment)| async move {
                        if position == data.len() {
                            return None;
                        }
                        tokio::time::sleep(Duration::from_micros(
                            (fragment as u64 * 250_000) / 8192,
                        ))
                        .await;
                        let end = (position + fragment).min(data.len());
                        Some((
                            Ok::<_, std::io::Error>(data[position..end].to_vec()),
                            (data, end, fragment),
                        ))
                    },
                );
                axum::http::Response::builder()
                    .status(200)
                    .header("content-type", "video/mp4")
                    .header("content-length", length)
                    .body(Body::from_stream(body))
                    .unwrap()
            }
        }),
    ))
    .await;
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let namespace = format!("http-recovery:{}", uuid::Uuid::new_v4());
    let coordinator = Coordinator::connect(
        &url,
        LeaseConfig {
            namespace: namespace.clone(),
            heartbeat_interval: Some(Duration::from_secs(1)),
            ttl: Duration::from_secs(10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1000;
    let content = uuid::Uuid::new_v4().to_string();
    let id = uuid::Uuid::new_v4().to_string();
    let resolver = Arc::new(Resolver(VodSource {
        kind: ContentKind::Movie,
        content_uuid: content.clone(),
        name: if identity_case {
            "Owned request identity movie"
        } else if idle_busy {
            "Owned fresh idle capacity movie"
        } else {
            "Owned recovery movie"
        }
        .into(),
        user: (public_stop == Some("cap") || identity_case).then(|| User {
            id: (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 1,
            username: "owned-capped-viewer".into(),
            password: "!".into(),
            is_active: true,
            user_level: 0,
            stream_limit: if identity_case { 0 } else { 1 },
            custom_properties: serde_json::json!({}),
        }),
        redirect: false,
        limit_policy: Default::default(),
        candidates: vec![VodCandidate {
            relation_id: 1,
            account_id: 1,
            profile_id: profile,
            max_streams: if idle_busy { 2 } else { 1 },
            url: format!("{origin}/fixture.mp4"),
            user_agent: "owned-recovery".into(),
        }],
    }));
    let engine = VodEngine::with_resolver(
        resolver,
        coordinator.clone(),
        &url,
        VodConfig {
            max_sessions: 1,
            max_requests: 3,
            frame_bytes: if public_stop.is_some() {
                64 * 1024
            } else {
                8192
            },
            idle_grace: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_redis_recovery(&namespace)
    .unwrap();
    if held_case {
        use http_body_util::BodyExt;
        let mut response = engine
            .respond(VodRequest {
                playback: PlaybackRequest {
                    channel_id: id.clone(),
                    query: vec![],
                    headers: vec![],
                    peer_ip: "127.0.0.1".parse().unwrap(),
                },
                kind: "movie".into(),
                content_id: content.clone(),
                session_id: Some(id.clone()),
                profile_id: None,
                resolved_client_ip: None,
            })
            .await;
        assert_eq!(response.status(), 200);
        let first = response
            .body_mut()
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(first.len(), 8192);
        tokio::time::sleep(Duration::from_millis(2200)).await;
        let mut redis = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        assert!(
            redis
                .exists::<_, bool>(format!("vod_persistent_connection:{id}"))
                .await
                .unwrap(),
            "upstream EOF alone must not retire downstream body"
        );
        assert_eq!(
            redis
                .get::<_, i64>(format!("profile_connections:{profile}"))
                .await
                .unwrap(),
            1
        );
        assert!(
            engine.shutdown().await,
            "shutdown settles provider/metadata while response tail remains unpolled"
        );
        assert!(!redis
            .exists::<_, bool>(format!("vod_persistent_connection:{id}"))
            .await
            .unwrap());
        assert_eq!(
            redis
                .get::<_, Option<i64>>(format!("profile_connections:{profile}"))
                .await
                .unwrap()
                .unwrap_or(0),
            0
        );
        drop(response);
        origin_task.abort();
        return;
    }
    let (base, app_task) = serve(engine.clone().router()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(90))
        .build()
        .unwrap();
    let mut initial_request = client.get(format!("{base}/proxy/vod/movie/{content}/{id}"));
    if identity_case {
        initial_request = initial_request.header("x-owned-initial-user", "yes");
    }
    let response = initial_request.send().await.unwrap();
    assert_eq!(response.status(), 200);
    let mut old = Reader {
        response,
        pending: bytes::Bytes::new(),
    };
    let mut old_bytes = old.take(8192).await;
    assert_eq!(&old_bytes, &media[..8192]);
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let owner = format!("{namespace}:vod:session:{id}:owner");
    let metadata = format!("vod_persistent_connection:{id}");
    let counter = format!("profile_connections:{profile}");
    if identity_case {
        let user: String = redis.hget(&metadata, "user_id").await.unwrap();
        assert_ne!(user, "unknown");
    }
    let token: String = redis.get(&owner).await.unwrap();
    let ledger = format!("{namespace}:lease:{token}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    if public_stop == Some("idle-initial") {
        let created: String = redis.hget(&metadata, "created_at").await.unwrap();
        drop(old);
        wait(async || {
            redis
                .hget::<_, _, Option<i64>>(&metadata, "active_streams")
                .await
                .unwrap()
                == Some(0)
                && redis
                    .get::<_, Option<i64>>(&counter)
                    .await
                    .unwrap()
                    .unwrap_or(0)
                    == 0
        })
        .await;
        let response = client
            .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut resumed = Reader {
            response,
            pending: bytes::Bytes::new(),
        };
        assert_eq!(resumed.take(8192).await, &media[..8192]);
        assert_eq!(
            redis
                .hget::<_, _, String>(&metadata, "created_at")
                .await
                .unwrap(),
            created
        );
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "request_count")
                .await
                .unwrap(),
            2
        );
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        drop(resumed);
        wait(async || {
            redis
                .hget::<_, _, Option<i64>>(&metadata, "active_streams")
                .await
                .unwrap()
                == Some(0)
        })
        .await;
        assert_eq!(
            redis.get::<_, i64>(&counter).await.unwrap(),
            0,
            "Source normal-close provider release precedes its delayed metadata timer"
        );
        wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
        assert!(engine.shutdown().await);
        app_task.abort();
        origin_task.abort();
        return;
    }
    if final_case {
        let stopped = client
            .post(format!("{base}/proxy/vod/stop_client/"))
            .json(&serde_json::json!({"client_id":id}))
            .send()
            .await
            .unwrap();
        assert_eq!(stopped.status(), 200);
        while old_bytes.len() < media.len() {
            old_bytes.extend(old.take((media.len() - old_bytes.len()).min(8192)).await);
        }
        assert_eq!(old_bytes.as_slice(), media.as_slice());
        let tail = tokio::time::timeout(Duration::from_secs(12), old.response.chunk())
            .await
            .unwrap()
            .unwrap();
        assert!(
            tail.is_none(),
            "known-length completed body has no extra frame"
        );
        assert!(!redis
            .exists::<_, bool>(format!("vod_proxy:client:{id}:stop"))
            .await
            .unwrap());
        let bytes: Option<u64> = redis.hget(&metadata, "bytes_sent").await.unwrap();
        assert!(
            bytes.is_none() || bytes == Some(0),
            "consumed stop precedes final100th progress"
        );
        // A later signal must not be consumed by duplicate final-frame callbacks.
        let _: () = redis
            .set_ex(format!("vod_proxy:client:{id}:stop"), "later-stop", 60)
            .await
            .unwrap();
        drop(old);
        wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
        assert_eq!(
            redis
                .get::<_, String>(format!("vod_proxy:client:{id}:stop"))
                .await
                .unwrap(),
            "later-stop"
        );
        assert!(engine.shutdown().await);
        assert!(!redis.exists::<_, bool>(&owner).await.unwrap());
        assert_eq!(
            redis
                .get::<_, Option<i64>>(&counter)
                .await
                .unwrap()
                .unwrap_or(0),
            0
        );
        let _: usize = redis
            .del(format!("vod_proxy:client:{id}:stop"))
            .await
            .unwrap();
        app_task.abort();
        origin_task.abort();
        return;
    }
    let _: usize = redis
        .del(&[owner.clone(), metadata.clone(), ledger, counter.clone()])
        .await
        .unwrap();
    let before_reopen = if immediate { 1 } else { 34 };
    for _ in 1..before_reopen {
        old_bytes.extend(old.take(8192).await);
    }
    assert_eq!(old_bytes, &media[..before_reopen * 8192]);
    assert!(!redis.exists::<_, bool>(&metadata).await.unwrap());
    let response = client
        .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut young = Reader {
        response,
        pending: bytes::Bytes::new(),
    };
    let mut young_bytes = young.take(8192).await;
    assert_eq!(young_bytes, &media[..8192]);
    if identity_case {
        assert_eq!(
            redis
                .hget::<_, _, String>(&metadata, "user_id")
                .await
                .unwrap(),
            "unknown"
        );
    }
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    if extra_entry {
        let response = client
            .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut third = Reader {
            response,
            pending: bytes::Bytes::new(),
        };
        assert_eq!(third.take(8192).await, &media[..8192]);
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "request_count")
                .await
                .unwrap(),
            2
        );
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        drop(third);
        wait(async || {
            redis
                .hget::<_, _, Option<i64>>(&metadata, "active_streams")
                .await
                .unwrap()
                == Some(1)
        })
        .await;
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
    }
    let pending_admin_entry = public_stop == Some("admin");
    if pending_admin_entry {
        let response = client
            .post(format!("{base}/proxy/vod/stop_client/"))
            .json(&serde_json::json!({"client_id":id}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let response = client
            .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut pending = Reader {
            response,
            pending: bytes::Bytes::new(),
        };
        assert_eq!(pending.take(8192).await, &media[..8192]);
        assert_eq!(
            redis
                .get::<_, String>(format!("vod_proxy:client:{id}:stop"))
                .await
                .unwrap(),
            "true"
        );
        drop(pending);
        wait(async || {
            redis
                .hget::<_, _, Option<i64>>(&metadata, "active_streams")
                .await
                .unwrap()
                == Some(1)
        })
        .await;
    }
    if stopping_case {
        assert_eq!(
            redis
                .get::<_, String>(format!("vod_proxy:client:{id}:stop"))
                .await
                .unwrap(),
            "true"
        );
        assert!(!redis
            .hexists::<_, _, bool>(&metadata, "rust_preempted")
            .await
            .unwrap());
    }
    assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
    assert_eq!(
        redis
            .hget::<_, _, i64>(&metadata, "active_streams")
            .await
            .unwrap(),
        1
    );
    let old_count = if stopping_case { 100 } else { 101 };
    for _ in before_reopen..old_count {
        old_bytes.extend(old.take(8192).await);
    }
    assert_eq!(old_bytes, &media[..old_count * 8192]);
    if stopping_case {
        assert!(old.pending.is_empty());
        let tail = tokio::time::timeout(Duration::from_secs(12), old.response.chunk())
            .await
            .unwrap();
        assert!(
            !matches!(tail, Ok(Some(_))),
            "no101st frame after consumed stop"
        );
        assert!(!redis
            .exists::<_, bool>(format!("vod_proxy:client:{id}:stop"))
            .await
            .unwrap());
    }
    if !stopping_case {
        wait(async || {
            redis
                .hget::<_, _, Option<u64>>(&metadata, "bytes_sent")
                .await
                .unwrap()
                == Some(819200)
        })
        .await;
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap(),
            1
        );
    }
    let before_idle: std::collections::BTreeMap<String, String> = if idle_case {
        redis.hgetall(&metadata).await.unwrap()
    } else {
        Default::default()
    };
    drop(old);
    if idle_case {
        wait(async || {
            redis
                .hget::<_, _, Option<i64>>(&metadata, "active_streams")
                .await
                .unwrap()
                == Some(0)
                && redis
                    .get::<_, Option<i64>>(&counter)
                    .await
                    .unwrap()
                    .unwrap_or(0)
                    == 0
        })
        .await;
        // Actual independent fenced provider reservation, not seeded counters.
        // Full real-other-HTTP/SQL/auth parity remains the packaged gate.
        let other = if idle_busy {
            Some(
                coordinator
                    .acquire_vod(dispatcharr_coordination::AcquireRequest {
                        channel_id: uuid::Uuid::new_v4().to_string(),
                        provider_profile_id: Some(profile),
                        provider_max_streams: 1,
                        credential_pool: None,
                    })
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        let mut idle_request = client.get(format!("{base}/proxy/vod/movie/{content}/{id}"));
        if idle_busy {
            idle_request = idle_request.header("x-owned-profile-cap", "1");
        }
        let response = idle_request.send().await.unwrap();
        if idle_busy {
            println!(
                "IDLE_FRESH_CAP status={} active={} requests={} provider={}",
                response.status().as_u16(),
                redis
                    .hget::<_, _, String>(&metadata, "active_streams")
                    .await
                    .unwrap(),
                redis
                    .hget::<_, _, String>(&metadata, "request_count")
                    .await
                    .unwrap(),
                redis.get::<_, i64>(&counter).await.unwrap()
            );
            assert_eq!(response.status(), 429);
            assert_eq!(response.headers()["vary"], "Accept, origin");
            assert_eq!(
                response.headers()["content-type"],
                "text/html; charset=utf-8"
            );
            assert_eq!(response.headers()["content-length"], "37");
            assert_eq!(response.headers()["allow"], "GET, OPTIONS");
            assert!(response.headers().get("retry-after").is_none());
            assert_eq!(
                response.text().await.unwrap(),
                "Connection limit exceeded for profile"
            );
            assert_eq!(
                redis
                    .hget::<_, _, String>(&metadata, "request_count")
                    .await
                    .unwrap(),
                before_idle["request_count"]
            );
            assert_eq!(
                redis
                    .hget::<_, _, String>(&metadata, "active_streams")
                    .await
                    .unwrap(),
                "0"
            );
            assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
            wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
            assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
            assert!(other.unwrap().release().await.unwrap());
        } else {
            assert_eq!(response.status(), 200);
            assert_eq!(response.headers()["vary"], "Accept, origin");
            let mut resumed = Reader {
                response,
                pending: bytes::Bytes::new(),
            };
            assert_eq!(resumed.take(8192).await, &media[..8192]);
            assert_eq!(
                redis
                    .hget::<_, _, String>(&metadata, "created_at")
                    .await
                    .unwrap(),
                before_idle["created_at"]
            );
            assert_eq!(
                redis
                    .hget::<_, _, i64>(&metadata, "request_count")
                    .await
                    .unwrap(),
                before_idle["request_count"].parse::<i64>().unwrap() + 1
            );
            assert_eq!(
                redis
                    .hget::<_, _, i64>(&metadata, "active_streams")
                    .await
                    .unwrap(),
                1
            );
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert!(redis.exists::<_, bool>(&metadata).await.unwrap());
            assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
            drop(resumed);
        }
    }
    wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
    assert_eq!(
        redis
            .get::<_, Option<i64>>(&counter)
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );
    for _ in 1..103 {
        young_bytes.extend(young.take(8192).await);
    }
    assert_eq!(young_bytes, &media[..843776]);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        if extra_entry { 3 } else { 2 }
            + usize::from(pending_admin_entry)
            + usize::from(idle_case && !idle_busy)
    );
    if retired_reopen {
        let response = client
            .get(format!("{base}/proxy/vod/movie/{content}/{id}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
        let mut replacement = Reader {
            response,
            pending: bytes::Bytes::new(),
        };
        assert_eq!(replacement.take(8192).await, &media[..8192]);
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "active_streams")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            redis
                .hget::<_, _, i64>(&metadata, "request_count")
                .await
                .unwrap(),
            1
        );
        assert_eq!(redis.get::<_, i64>(&counter).await.unwrap(), 1);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            if extra_entry { 4 } else { 3 } + usize::from(pending_admin_entry)
        );
        let next = young.take(8192).await;
        assert_eq!(next, &media[843776..851968]);
        young_bytes.extend(next);
        drop(replacement);
        wait(async || !redis.exists::<_, bool>(&metadata).await.unwrap()).await;
    }
    // Retain bounded valid MP4 prefixes for independent strict first-second decode.
    std::fs::write("/evidence/old-prefix.mp4", old_bytes).unwrap();
    std::fs::write("/evidence/young-prefix.mp4", young_bytes).unwrap();
    drop(young);
    assert!(
        engine.shutdown().await,
        "all body/control/provider work must physically drain"
    );
    assert!(!redis.exists::<_, bool>(&owner).await.unwrap());
    assert!(!redis.exists::<_, bool>(&metadata).await.unwrap());
    assert_eq!(
        redis
            .get::<_, Option<i64>>(&counter)
            .await
            .unwrap()
            .unwrap_or(0),
        0
    );
    app_task.abort();
    origin_task.abort();
}
