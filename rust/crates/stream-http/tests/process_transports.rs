//! Actual Engine -> configured FFmpeg -> owned RTSP TCP peer admission.
//! It intentionally supplies no media; this is not codec/Source acceptance.
use dispatcharr_contracts::{PlaybackSource, StreamCommand};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    time,
};
#[tokio::test]
#[ignore = "requires isolated Redis and the pinned production FFmpeg on PATH"]
async fn actual_process_rtsp_reaches_owned_peer_with_one_real_provider_slot_then_drains() {
    let redis_url = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let uri = format!("rtsp://{address}/owned");
    let id = uuid::Uuid::new_v4().to_string();
    let profile = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let (arrived, received) = oneshot::channel();
    let (reply, release) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut connection, _) = time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("actual input process must reach RTSP TCP")
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            let count = time::timeout(Duration::from_secs(3), connection.read(&mut byte))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(count, 1);
            request.push(byte[0]);
            assert!(request.len() <= 8192);
        }
        let first = request.split(|byte| *byte == b'\n').next().unwrap();
        assert!(first.starts_with(b"OPTIONS rtsp://"));
        assert!(first.ends_with(b" RTSP/1.0\r"));
        arrived.send(()).unwrap();
        release.await.unwrap();
        connection
            .write_all(b"RTSP/1.0 404 Not Found\r\nCSeq: 1\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        connection.shutdown().await.unwrap();
    });
    let coordinator = Coordinator::connect(&redis_url, LeaseConfig::default())
        .await
        .unwrap();
    let engine = StreamEngine::new_with_coordinator(
        EngineConfig {
            startup_timeout: Duration::from_secs(8),
            read_timeout: Duration::from_secs(3),
            max_retries: 0,
            ..Default::default()
        },
        Some(coordinator),
    )
    .unwrap();
    let command=StreamCommand::build("owned-RTSP",false,"ffmpeg","-hide_banner -loglevel error -threads 1 -rtsp_transport tcp -i {streamUrl} -map 0:v:0 -map 0:a:0 -c copy -f mpegts pipe:1",Some(&uri),Some("owned transport control"),&serde_json::json!(1)).unwrap();
    let source = PlaybackSource {
        output_profile: None,
        input_command: Some(command),
        channel_id: id,
        channel_name: "owned RTSP admission".into(),
        stream_id: 1,
        profile_id: 1,
        provider_profile_id: Some(profile),
        provider_max_streams: 1,
        credential_pool: None,
        url: uri,
        user_agent: "owned transport control".into(),
        headers: vec![],
        metadata: vec![],
        output_format: Default::default(),
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::ZERO,
        alternates: vec![],
        viewer: None,
    };
    let selected = engine.clone();
    let play = tokio::spawn(async move { selected.play(source).await });
    let admission = time::timeout(Duration::from_secs(6), received).await;
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let count: Option<i64> = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    // Retain/settle every independent observation before assertions, including
    // the old pre-process rejection path where the owned peer times out.
    let _ = reply.send(());
    let peer_outcome = time::timeout(Duration::from_secs(4), peer).await;
    let play_outcome = time::timeout(Duration::from_secs(12), play).await;
    let drained = engine.shutdown_wait(Duration::from_secs(5)).await;
    let final_count: Option<i64> = redis::cmd("GET")
        .arg(format!("profile_connections:{profile}"))
        .query_async(&mut redis)
        .await
        .unwrap();
    eprintln!(
        "{}",
        serde_json::json!({"actual_RTSP_TCP_admission":matches!(&admission,Ok(Ok(()))),"provider_count_during_admission":count,"peer_task_settled":matches!(&peer_outcome,Ok(Ok(()))),"play_failed_without_fabricated_media":matches!(&play_outcome,Ok(Ok(Err(_)))),"actual_engine_drained":drained,"final_provider_count":final_count,"scope":"process transport admission/slot teardown only; codec/Source acceptance not claimed"})
    );
    assert!(matches!(admission, Ok(Ok(()))));
    assert_eq!(count, Some(1));
    assert!(matches!(peer_outcome, Ok(Ok(()))));
    assert!(matches!(play_outcome, Ok(Ok(Err(_)))));
    assert!(drained);
    assert_eq!(final_count, Some(0));
    assert!(engine.status().is_empty());
}
