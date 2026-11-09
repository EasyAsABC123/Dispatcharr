//! Actual native HTTP engine transport gates. Original Dispatcharr authors retain full credit.
use axum::{
    body::Body,
    extract::Path,
    http::{HeaderMap, HeaderValue, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use dispatcharr_contracts::PlaybackSource;
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use flate2::{
    write::{DeflateEncoder, GzEncoder, ZlibEncoder},
    Compression,
};
use http_body_util::BodyExt;
use std::{
    convert::Infallible,
    io::Write,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, time};
fn payload() -> Vec<u8> {
    [vec![0x47, 0x1f, 0xff, 0x10], vec![0; 184]]
        .concat()
        .repeat(128)
}
fn encode(data: &[u8], kind: &str) -> Vec<u8> {
    match kind {
        "gzip" => {
            let mut w = GzEncoder::new(Vec::new(), Compression::default());
            w.write_all(data).unwrap();
            w.finish().unwrap()
        }
        "deflate" => {
            let mut w = ZlibEncoder::new(Vec::new(), Compression::default());
            w.write_all(data).unwrap();
            w.finish().unwrap()
        }
        _ => {
            let mut w = DeflateEncoder::new(Vec::new(), Compression::default());
            w.write_all(data).unwrap();
            w.finish().unwrap()
        }
    }
}
fn source(url: String, channel: &str) -> PlaybackSource {
    PlaybackSource {
        channel_id: channel.into(),
        channel_name: "Upstream fixture".into(),
        stream_id: 1,
        profile_id: 1,
        provider_profile_id: None,
        provider_max_streams: 0,
        url,
        user_agent: String::new(),
        headers: vec![],
        metadata: vec![],
        output_format: Default::default(),
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::from_secs(5),
        alternates: vec![],
        viewer: None,
    }
}
fn config() -> EngineConfig {
    EngineConfig {
        max_channels: 4,
        retained_bytes_per_channel: 188 * 4096,
        chunk_bytes: 188 * 16,
        connect_timeout: Duration::from_secs(1),
        read_timeout: Duration::from_secs(1),
        startup_timeout: Duration::from_secs(2),
        max_retries: 0,
        flush_interval: Duration::from_millis(5),
        ..Default::default()
    }
}
async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (base, task)
}
fn body(data: Vec<u8>, encoding: Option<&str>) -> Response<Body> {
    let mut response = Response::builder().header("content-type", "video/mp2t");
    if let Some(encoding) = encoding {
        response = response.header("content-encoding", encoding);
    }
    response.body(Body::from_stream(async_stream::stream!{yield Ok::<_,Infallible>(Bytes::from(data));time::sleep(Duration::from_millis(50)).await;})).unwrap()
}
async fn read(engine: &StreamEngine, source: PlaybackSource) -> Vec<u8> {
    let response = engine.play(source).await.unwrap();
    time::timeout(Duration::from_secs(3), async {
        let mut body = response.into_body();
        let mut bytes = Vec::new();
        while let Some(frame) = body.frame().await {
            if let Ok(data) = frame.unwrap().into_data() {
                bytes.extend_from_slice(&data);
            }
        }
        bytes
    })
    .await
    .unwrap()
}
async fn idle(engine: &StreamEngine) {
    time::timeout(Duration::from_secs(2), async {
        while !engine.status().is_empty() {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn native_transport_decodes_identity_gzip_zlib_raw_and_stacked_streams() {
    let app = Router::new().route(
        "/{kind}",
        get(|Path(kind): Path<String>| async move {
            let data = payload();
            match kind.as_str() {
                "identity" => body(data, None),
                "gzip" => body(encode(&data, "gzip"), Some("gzip")),
                "deflate" => body(encode(&data, "deflate"), Some("deflate")),
                "raw" => body(encode(&data, "raw"), Some("deflate")),
                "stacked" => body(
                    encode(&encode(&data, "gzip"), "deflate"),
                    Some("gzip, deflate"),
                ),
                "five" => {
                    let mut data = data;
                    for _ in 0..5 {
                        data = encode(&data, "gzip");
                    }
                    body(data, Some("gzip, gzip, gzip, gzip, gzip"))
                }
                _ => unreachable!(),
            }
        }),
    );
    let (base, server) = serve(app).await;
    let engine = StreamEngine::new(config()).unwrap();
    for kind in ["identity", "gzip", "deflate", "raw", "stacked", "five"] {
        let actual = read(&engine, source(format!("{base}/{kind}"), kind)).await;
        assert!(
            actual == payload(),
            "{kind}: actual length {}, expected {}, first mismatch {:?}",
            actual.len(),
            payload().len(),
            actual.iter().zip(payload()).position(|(a, b)| *a != b)
        );
        idle(&engine).await;
    }
    server.abort();
}
#[tokio::test]
async fn cookie_redirect_works_but_new_provider_attempt_has_no_prior_cookie() {
    let app = Router::new()
        .route(
            "/start",
            get(|| async {
                Response::builder()
                    .status(302)
                    .header("location", "/required")
                    .header("set-cookie", "attempt_cookie=fixed; Path=/")
                    .body(Body::empty())
                    .unwrap()
            }),
        )
        .route(
            "/required",
            get(|headers: HeaderMap| async move {
                if headers
                    .get("cookie")
                    .is_some_and(|value| value == "attempt_cookie=fixed")
                {
                    body(payload(), None)
                } else {
                    Response::builder().status(403).body(Body::empty()).unwrap()
                }
            }),
        )
        .route(
            "/fresh",
            get(|headers: HeaderMap| async move {
                if headers.contains_key("cookie") {
                    Response::builder().status(403).body(Body::empty()).unwrap()
                } else {
                    body(payload(), None)
                }
            }),
        );
    let (base, server) = serve(app).await;
    let engine = StreamEngine::new(config()).unwrap();
    assert_eq!(
        read(&engine, source(format!("{base}/start"), "redirect")).await,
        payload()
    );
    idle(&engine).await;
    assert_eq!(
        read(&engine, source(format!("{base}/fresh"), "fresh")).await,
        payload()
    );
    idle(&engine).await;
    assert_eq!(
        engine
            .play(source(format!("{base}/required"), "required-fresh"))
            .await
            .unwrap_err()
            .status,
        502
    );
    idle(&engine).await;
    server.abort();
}
#[tokio::test]
async fn request_defaults_latin1_and_invalid_user_agents_match_source_boundaries() {
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let app = Router::new().route(
        "/{kind}",
        get(move |Path(kind): Path<String>, headers: HeaderMap| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(headers["accept"], "*/*");
                assert_eq!(headers["accept-encoding"], "gzip, deflate");
                assert_eq!(headers["connection"], "keep-alive");
                if kind == "latin1" {
                    assert_eq!(headers["user-agent"].as_bytes(), b"\xe9");
                } else {
                    assert_eq!(headers["user-agent"], "python-requests/2.34.2");
                }
                body(payload(), None)
            }
        }),
    );
    let (base, server) = serve(app).await;
    let engine = StreamEngine::new(config()).unwrap();
    assert_eq!(
        read(&engine, source(format!("{base}/default"), "default")).await,
        payload()
    );
    idle(&engine).await;
    let mut latin = source(format!("{base}/latin1"), "latin1");
    latin.user_agent = "é".into();
    assert_eq!(read(&engine, latin).await, payload());
    idle(&engine).await;
    for (index, agent) in [" Agent", " ", "\tAgent", "bad\r\nInjected:x", "emoji😀"]
        .into_iter()
        .enumerate()
    {
        let mut bad = source(format!("{base}/default"), &format!("bad-{index}"));
        bad.user_agent = agent.into();
        assert_eq!(engine.play(bad).await.unwrap_err().status, 502);
        idle(&engine).await;
    }
    assert_eq!(requests.load(Ordering::SeqCst), 2);
    server.abort();
}
#[tokio::test]
async fn decoding_chain_and_cookie_capacity_fail_closed_without_publishing_junk() {
    let app = Router::new()
        .route(
            "/six",
            get(|| async { body(payload(), Some("gzip, gzip, gzip, gzip, gzip, gzip")) }),
        )
        .route(
            "/cookies",
            get(|| async {
                let mut response = body(payload(), None);
                for _ in 0..513 {
                    response
                        .headers_mut()
                        .append("set-cookie", HeaderValue::from_static("key=fixed"));
                }
                response
            }),
        );
    let (base, server) = serve(app).await;
    let engine = StreamEngine::new(config()).unwrap();
    for (index, path) in ["six", "cookies"].into_iter().enumerate() {
        assert_eq!(
            engine
                .play(source(format!("{base}/{path}"), &format!("limit-{index}")))
                .await
                .unwrap_err()
                .status,
            502
        );
        idle(&engine).await;
    }
    server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn compressed_cancellation_and_decode_failure_release_exact_provider_quota() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = dispatcharr_coordination::Coordinator::connect(
        &url,
        dispatcharr_coordination::LeaseConfig::default(),
    )
    .await
    .unwrap();
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    let count = active.clone();
    let hits = requests.clone();
    let app=Router::new().route("/live",get(move||{let count=count.clone();let hits=hits.clone();async move{
        count.fetch_add(1,Ordering::SeqCst);hits.fetch_add(1,Ordering::SeqCst);let guard=Guard(count);let data=encode(&payload(),"gzip");
        Response::builder().header("content-encoding","gzip").body(Body::from_stream(async_stream::stream!{
            let _guard=guard;yield Ok::<_,Infallible>(Bytes::from(data));futures_util::future::pending::<()>().await;
        })).unwrap()
    }})).route("/bad",get(||async{body(b"invalid gzip bytes".to_vec(),Some("gzip"))}));
    let (base, server) = serve(app).await;
    let engine = StreamEngine::new_with_coordinator(config(), Some(coordinator.clone())).unwrap();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let profile = 8_000_000 + (unique % 1_000_000) as i64;
    let mut first = source(format!("{base}/live"), &format!("compressed-live-{unique}"));
    first.provider_profile_id = Some(profile);
    first.provider_max_streams = 1;
    let mut second = first.clone();
    second.channel_id = format!("compressed-second-{unique}");
    let mut running = engine.play(first).await.unwrap().into_body();
    let frame = running.frame().await.unwrap().unwrap();
    assert_eq!(frame.into_data().unwrap().len(), 188 * 16);
    assert_eq!(engine.play(second).await.unwrap_err().status, 503);
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    let key = format!("profile_connections:{profile}");
    let slots: Option<i64> = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(slots, Some(1));
    drop(running);
    time::timeout(Duration::from_secs(3), async {
        loop {
            let value: Option<i64> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut redis)
                .await
                .unwrap();
            if value.unwrap_or(0) == 0 && active.load(Ordering::SeqCst) == 0 {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    idle(&engine).await;
    let mut broken = source(format!("{base}/bad"), &format!("compressed-bad-{unique}"));
    broken.provider_profile_id = Some(profile);
    broken.provider_max_streams = 1;
    assert_eq!(engine.play(broken).await.unwrap_err().status, 502);
    idle(&engine).await;
    time::timeout(Duration::from_secs(3), async {
        loop {
            let value: Option<i64> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut redis)
                .await
                .unwrap();
            if value.unwrap_or(0) == 0 {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test]
async fn statuses_redirect_boundaries_and_url_auth_match_original_reader() {
    let cross_auth = Arc::new(AtomicUsize::new(0));
    let count = cross_auth.clone();
    let (other, other_server) = serve(Router::new().route(
        "/final",
        get(move |headers: HeaderMap| {
            let count = count.clone();
            async move {
                if headers.contains_key("authorization") {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                body(payload(), None)
            }
        }),
    ))
    .await;
    let target = other.clone();
    let app = Router::new()
        .route(
            "/status/{code}",
            get(|Path(code): Path<u16>| async move {
                Response::builder()
                    .status(code)
                    .body(Body::empty())
                    .unwrap()
            }),
        )
        .route(
            "/redirect/{code}",
            get(|Path(code): Path<u16>| async move {
                Response::builder()
                    .status(code)
                    .header("location", "/auth/final")
                    .body(Body::empty())
                    .unwrap()
            }),
        )
        .route(
            "/chain/{count}",
            get(|Path(count): Path<u16>| async move {
                if count == 0 {
                    body(payload(), None)
                } else {
                    Response::builder()
                        .status(302)
                        .header("location", format!("/chain/{}", count - 1))
                        .body(Body::empty())
                        .unwrap()
                }
            }),
        )
        .route(
            "/auth/same",
            get(|| async {
                Response::builder()
                    .status(302)
                    .header("location", "/auth/required")
                    .body(Body::empty())
                    .unwrap()
            }),
        )
        .route(
            "/auth/required",
            get(|headers: HeaderMap| async move {
                assert!(headers.contains_key("authorization"));
                body(payload(), None)
            }),
        )
        .route("/auth/final", get(|| async { body(payload(), None) }))
        .route(
            "/auth/cross",
            get(move || {
                let target = target.clone();
                async move {
                    Response::builder()
                        .status(302)
                        .header("location", format!("{target}/final"))
                        .body(Body::empty())
                        .unwrap()
                }
            }),
        );
    let (base, server) = serve(app).await;
    let engine = StreamEngine::new(config()).unwrap();
    for code in [201, 202, 203, 204, 205, 206, 300, 304, 404, 503] {
        assert_eq!(
            engine
                .play(source(
                    format!("{base}/status/{code}"),
                    &format!("status-{code}")
                ))
                .await
                .unwrap_err()
                .status,
            502
        );
        idle(&engine).await;
    }
    for code in [301, 302, 303, 307, 308] {
        assert_eq!(
            read(
                &engine,
                source(
                    format!("{base}/redirect/{code}"),
                    &format!("redirect-{code}")
                )
            )
            .await
            .len(),
            payload().len()
        );
        idle(&engine).await;
    }
    for code in [300, 304, 305, 306] {
        assert_eq!(
            engine
                .play(source(
                    format!("{base}/redirect/{code}"),
                    &format!("no-redirect-{code}")
                ))
                .await
                .unwrap_err()
                .status,
            502
        );
        idle(&engine).await;
    }
    for count in [29, 30] {
        assert_eq!(
            read(
                &engine,
                source(format!("{base}/chain/{count}"), &format!("chain-{count}"))
            )
            .await
            .len(),
            payload().len()
        );
        idle(&engine).await;
    }
    assert_eq!(
        engine
            .play(source(format!("{base}/chain/31"), "chain-31"))
            .await
            .unwrap_err()
            .status,
        502
    );
    idle(&engine).await;
    for path in ["same", "cross"] {
        let url = format!("{base}/auth/{path}").replacen(
            "http://",
            "http://fixture-user:fixture-pass@",
            1,
        );
        assert_eq!(
            read(&engine, source(url, &format!("auth-{path}")))
                .await
                .len(),
            payload().len()
        );
        idle(&engine).await;
    }
    assert_eq!(cross_auth.load(Ordering::SeqCst), 0);
    server.abort();
    other_server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn retained_diagnostics_separate_nonzero_local_ring_and_shared_redis_copy() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = dispatcharr_coordination::Coordinator::connect(&url, Default::default())
        .await
        .unwrap();
    let clients = dispatcharr_clients::ClientStore::connect(&url, Default::default())
        .await
        .unwrap();
    let inputs = dispatcharr_input_store::InputStore::connect(&url, Default::default())
        .await
        .unwrap();
    let app=Router::new().route("/live",get(||async{let data=payload();Response::new(Body::from_stream(async_stream::stream!{yield Ok::<_,Infallible>(Bytes::from(data));futures_util::future::pending::<()>().await;}))}));
    let (base, server) = serve(app).await;
    let mut limits = config();
    limits.read_timeout = Duration::from_secs(5);
    let engine = StreamEngine::new_with_shared_inputs(
        limits,
        Some(coordinator),
        Some(clients.clone()),
        Some(inputs),
    )
    .unwrap();
    let id = format!(
        "memory-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut body = engine
        .play(source(format!("{base}/live"), &id))
        .await
        .unwrap()
        .into_body();
    drop(body.frame().await.unwrap().unwrap());
    let diagnostics = time::timeout(Duration::from_secs(2), async {
        loop {
            let value = engine.memory_diagnostics().await.unwrap();
            if value["local_retained_payload_bytes"].as_u64() == Some(payload().len() as u64)
                && value["redis_logical_payload_bytes"].as_u64() == Some(payload().len() as u64)
            {
                break value;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(diagnostics["format_version"], 1);
    assert_eq!(diagnostics["sampled_channels"], 1);
    assert_eq!(
        diagnostics["channels"][0]["shared_input"]["fence_matches_owner"],
        true
    );
    assert!(
        diagnostics["redis_known_key_allocation_bytes"]
            .as_u64()
            .unwrap()
            > payload().len() as u64
    );
    drop(body);
    idle(&engine).await;
    clients.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn four_large_chunks_become_ready_with_one_local_chunk_and_full_shared_history() {
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    let coordinator = dispatcharr_coordination::Coordinator::connect(&url, Default::default())
        .await
        .unwrap();
    let clients = dispatcharr_clients::ClientStore::connect(&url, Default::default())
        .await
        .unwrap();
    let inputs = dispatcharr_input_store::InputStore::connect(&url, Default::default())
        .await
        .unwrap();
    let fourth = Arc::new(tokio::sync::Notify::new());
    let release = fourth.clone();
    let size = 188 * 16_384;
    let app = Router::new().route(
        "/large",
        get(move || {
            let release = release.clone();
            async move {
                Response::new(Body::from_stream(async_stream::stream! {
                    for number in 1..=4 {
                        if number == 4 {release.notified().await;}
                        let mut packet = [number;188];
                        packet[..4].copy_from_slice(&[0x47,0x1f,0xff,0x10]);
                        yield Ok::<_,Infallible>(Bytes::from(packet.repeat(size/188)));
                    }
                    futures_util::future::pending::<()>().await;
                }))
            }
        }),
    );
    let (base, server) = serve(app).await;
    let mut limits = config();
    limits.retained_bytes_per_channel = 16 * 1024 * 1024;
    limits.shared_hot_cache_bytes = 4 * 1024 * 1024;
    limits.chunk_bytes = size;
    limits.retention = Duration::from_secs(60);
    limits.initial_buffer_chunks = 4;
    limits.flush_partial_chunks = false;
    limits.source_initialization = true;
    limits.read_timeout = Duration::from_secs(3);
    limits.startup_timeout = Duration::from_secs(5);
    let engine = StreamEngine::new_with_shared_inputs(
        limits,
        Some(coordinator),
        Some(clients.clone()),
        Some(inputs.clone()),
    )
    .unwrap();
    let channel = format!("large-startup-{}", uuid::Uuid::new_v4());
    let profile = (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i64 + 100;
    let mut playback = source(format!("{base}/large"), &channel);
    playback.provider_profile_id = Some(profile);
    playback.provider_max_streams = 1;
    playback.startup_behind = Duration::from_secs(60);
    let running = engine.clone();
    let task = tokio::spawn(async move { running.play(playback).await });
    time::timeout(Duration::from_secs(3), async {
        loop {
            if inputs.snapshot(&channel).await.unwrap().index == 3 {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !task.is_finished(),
        "three canonical chunks cannot satisfy four-chunk readiness"
    );
    assert_eq!(engine.status()[0].retained_bytes, size);
    fourth.notify_one();
    let response = time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let mut body = response.into_body();
    let data = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(data.len(), size);
    assert_eq!(
        data[4], 1,
        "oldest startup chunk must come from shared history"
    );
    assert!(data
        .chunks_exact(188)
        .all(|packet| packet[0] == 0x47 && packet[4] == 1));
    let memory = engine.memory_diagnostics().await.unwrap();
    assert_eq!(memory["local_retained_payload_bytes"], size);
    assert_eq!(memory["redis_logical_payload_bytes"], size * 4);
    assert_eq!(
        memory["configured_local_limit_bytes_per_channel"],
        4 * 1024 * 1024
    );
    let metadata = engine
        .legacy_status(&channel, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata["state"], "active");
    drop(data);
    drop(body);
    idle(&engine).await;
    clients.shutdown().await;
    assert!(inputs.snapshot(&channel).await.unwrap().owner.is_none());
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let count: Option<u64> = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(count.unwrap_or(0), 0);
    server.abort();
}
