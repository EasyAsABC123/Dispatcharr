//! Real Redis, loopback transport and FFmpeg integration; explicitly opt-in.
use axum::{body::Body, routing::get, Router};
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_contracts::{PlaybackSource, PlaybackViewer};
use dispatcharr_fmp4_http::{Config, Fmp4Gateway};
use dispatcharr_output_store::{OutputConfig, OutputStore};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use http_body_util::BodyExt;
use redis::AsyncCommands;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, time};

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and FMP4_TEST_FIXTURE H264/AAC TS"]
async fn actual_remux_shared_upstream_and_late_viewer_decode_and_cleanup() {
    exercise(false).await;
}
#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and FMP4_TEST_FIXTURE H264/AAC TS"]
async fn explicit_shutdown_reaps_remux_even_while_a_response_frame_is_held() {
    exercise(true).await;
}
struct Fixture {
    gateway: Fmp4Gateway,
    engine: StreamEngine,
    clients: ClientStore,
    source: PlaybackSource,
    connections: Arc<AtomicUsize>,
    origin: tokio::task::JoinHandle<()>,
}
async fn setup() -> Fixture {
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let fixture = std::fs::read(std::env::var("FMP4_TEST_FIXTURE").unwrap()).unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let counts = connections.clone();
    let router=Router::new().route("/live",get(move||{let fixture=fixture.clone();let counts=counts.clone();async move{
        counts.fetch_add(1,Ordering::SeqCst);
        Body::from_stream(async_stream::stream!{for bytes in fixture.chunks(65536){yield Ok::<_,std::convert::Infallible>(bytes.to_vec());time::sleep(Duration::from_millis(10)).await;}std::future::pending::<()>().await;})
    }}));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let clients = ClientStore::connect(
        &redis_url,
        ClientConfig {
            flush_interval: Duration::from_millis(25),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let outputs = OutputStore::connect(
        &redis_url,
        OutputConfig {
            max_segment_bytes: 8 * 1024 * 1024,
            max_read_bytes: 32 * 1024 * 1024,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let engine = StreamEngine::new_with_services(
        EngineConfig {
            chunk_bytes: 188 * 128,
            flush_interval: Duration::from_millis(5),
            max_retries: 0,
            read_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        None,
        Some(clients.clone()),
    )
    .unwrap();
    let gateway = Fmp4Gateway::new(
        engine.clone(),
        clients.clone(),
        outputs.clone(),
        Config::default(),
    )
    .unwrap();
    let channel = uuid::Uuid::new_v4().to_string();
    let source = PlaybackSource {
        channel_id: channel.clone(),
        channel_name: "Fixture".into(),
        stream_id: 1,
        profile_id: 1,
        provider_profile_id: None,
        provider_max_streams: 0,
        url: format!("http://{address}/live"),
        user_agent: "Fixture".into(),
        headers: vec![],
        metadata: vec![],
        output_format: dispatcharr_contracts::OutputFormat::Fmp4,
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::from_secs(5),
        alternates: vec![],
        viewer: Some(PlaybackViewer {
            user_id: None,
            username: None,
            ip_address: "127.0.0.1".parse().unwrap(),
            user_agent: "fmp4-test".into(),
            stream_limit: 0,
            limit_policy: Default::default(),
        }),
    };
    Fixture {
        gateway,
        engine,
        clients,
        source,
        connections,
        origin,
    }
}
async fn exercise(hold_frame: bool) {
    let Fixture {
        gateway,
        engine,
        clients,
        source,
        connections,
        origin,
    } = setup().await;
    let channel = source.channel_id.clone();
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let mut first = gateway.play(source.clone()).await.unwrap().into_body();
    let mut first_capture = Vec::new();
    for _ in 0..3 {
        let bytes = time::timeout(Duration::from_secs(20), first.frame())
            .await
            .expect("fMP4 startup deadline")
            .expect("fMP4 closed")
            .unwrap()
            .into_data()
            .unwrap();
        first_capture.extend_from_slice(&bytes);
    }
    let mut second = gateway.play(source).await.unwrap().into_body();
    let mut second_capture = Vec::new();
    let mut held = None;
    for index in 0..3 {
        let bytes = time::timeout(Duration::from_secs(10), second.frame())
            .await
            .unwrap()
            .expect("late fMP4 closed")
            .unwrap()
            .into_data()
            .unwrap();
        second_capture.extend_from_slice(&bytes);
        if hold_frame && index == 2 {
            held = Some(bytes.clone());
        }
    }
    let pids = gateway.active_process_ids();
    assert_eq!(pids.len(), 1);
    assert_eq!(
        connections.load(Ordering::SeqCst),
        1,
        "one shared upstream/remux"
    );
    for (name, data) in [("first", first_capture), ("late", second_capture)] {
        let path = std::env::temp_dir().join(format!(
            "dispatcharr-fmp4-{}-{name}.mp4",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, &data).unwrap();
        let decoded = std::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-xerror",
                "-progress",
                "pipe:1",
                "-nostats",
                "-i",
            ])
            .arg(&path)
            .args(["-map", "0:v:0", "-map", "0:a:0", "-f", "null", "-"])
            .output()
            .unwrap();
        std::fs::remove_file(path).unwrap();
        assert!(
            decoded.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&decoded.stderr)
        );
        let progress = String::from_utf8_lossy(&decoded.stdout);
        assert!(progress.contains("progress=end"));
        let frames = progress
            .lines()
            .filter_map(|line| line.strip_prefix("frame="))
            .filter_map(|value| value.parse::<u64>().ok())
            .max()
            .unwrap_or(0);
        assert!(frames >= 50, "{name} decoded only {frames} frames");
    }
    drop(first);
    drop(second);
    if hold_frame {
        gateway.shutdown().await;
        assert!(!held.as_ref().unwrap().is_empty());
        for pid in &pids {
            assert!(
                !std::path::Path::new(&format!("/proc/{pid}")).exists(),
                "FFmpeg child survived shutdown acknowledgment"
            );
        }
        drop(held);
    }
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    time::timeout(Duration::from_secs(5), async {
        loop {
            let count: usize = redis
                .scard(format!("live:channel:{channel}:clients"))
                .await
                .unwrap();
            let owner: bool = redis
                .exists(format!("live:channel:{channel}:output:fmp4:owner"))
                .await
                .unwrap();
            if count == 0 && !owner {
                break;
            }
            time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    gateway.shutdown().await;
    for pid in &pids {
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "FFmpeg child survived final-viewer cleanup"
        );
    }
    engine.shutdown();
    clients.shutdown().await;
    origin.abort();
}

#[tokio::test]
#[ignore = "requires disposable REDIS_TEST_URL and FMP4_TEST_FIXTURE"]
async fn hundred_viewers_retry_bounded_read_admission_without_dropping_streams() {
    let Fixture {
        gateway,
        engine,
        clients,
        source,
        connections,
        origin,
    } = setup().await;
    let mut slow = gateway.play(source.clone()).await.unwrap().into_body();
    let initial = time::timeout(Duration::from_secs(20), slow.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(&initial[4..8], b"ftyp");
    drop(initial);
    let held = time::timeout(Duration::from_secs(10), slow.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(&held[4..8], b"moof");
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..99 {
        let gateway = gateway.clone();
        let source = source.clone();
        tasks.spawn(async move {
            let mut body = gateway.play(source).await.unwrap().into_body();
            let mut bytes_sent = 0;
            for index in 0..3 {
                let frame = time::timeout(Duration::from_secs(15), body.frame())
                    .await
                    .expect("fanout deadline")
                    .expect("fanout body closed")
                    .unwrap()
                    .into_data()
                    .unwrap();
                assert_eq!(&frame[4..8], if index == 0 { b"ftyp" } else { b"moof" });
                bytes_sent += frame.len();
            }
            bytes_sent
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert!(result.unwrap() > 1000);
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert_eq!(gateway.active_process_ids().len(), 1);
    assert_eq!(&held[4..8], b"moof");
    drop(held);
    drop(slow);
    gateway.shutdown().await;
    engine.shutdown();
    clients.shutdown().await;
    origin.abort();
}
