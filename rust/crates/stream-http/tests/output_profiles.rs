//! Actual Redis/raw-owner/profile-process regression. Original authors retain credit.
use axum::{body::Body, routing::get, Router};
use bytes::Bytes;
use dispatcharr_clients::{ClientConfig, ClientStore};
use dispatcharr_contracts::{PlaybackSource, StreamCommand};
use dispatcharr_coordination::{Coordinator, LeaseConfig};
use dispatcharr_input_store::{InputConfig, InputStore};
use dispatcharr_output_store::{OutputConfig, OutputStore};
use dispatcharr_stream_http::{EngineConfig, StreamEngine};
use http_body_util::BodyExt;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, time};
fn source(channel: &str, url: &str) -> PlaybackSource {
    PlaybackSource {
        output_profile: None,
        input_command: None,
        channel_id: channel.into(),
        channel_name: "Profile physical peer".into(),
        stream_id: 1,
        profile_id: 1,
        provider_profile_id: Some(7),
        provider_max_streams: 1,
        credential_pool: None,
        url: url.into(),
        user_agent: "Source profile physical peer".into(),
        headers: vec![],
        metadata: vec![],
        output_format: Default::default(),
        shutdown_delay: Duration::ZERO,
        startup_behind: Duration::ZERO,
        alternates: vec![],
        viewer: None,
    }
}
async fn engine(url: &str) -> (StreamEngine, ClientStore, OutputStore) {
    let coordinator = Coordinator::connect(url, LeaseConfig::default())
        .await
        .unwrap();
    let clients = ClientStore::connect(url, ClientConfig::default())
        .await
        .unwrap();
    let outputs = OutputStore::connect(url, OutputConfig::default())
        .await
        .unwrap();
    let inputs = InputStore::connect(url, InputConfig::default())
        .await
        .unwrap();
    let config = EngineConfig {
        max_channels: 4,
        chunk_bytes: 188 * 2,
        initial_buffer_chunks: 1,
        read_timeout: Duration::from_secs(2),
        ..EngineConfig::default()
    };
    let engine = StreamEngine::new_with_shared_inputs(
        config,
        Some(coordinator),
        Some(clients.clone()),
        Some(inputs),
    )
    .unwrap()
    .with_output_profiles(outputs.clone(), Arc::new(()))
    .unwrap();
    assert!(engine.output_profiles_ready(Duration::from_secs(2)).await);
    (engine, clients, outputs)
}
async fn diagnostics(
    first: &StreamEngine,
    second: &StreamEngine,
    clients: &ClientStore,
    outputs: &OutputStore,
    channel: &str,
) -> serde_json::Value {
    let metadata = clients.channel_clients(channel).await.map(|rows| {
        rows.into_iter().map(|row|serde_json::json!({
        "output_format":row["output_format"],"output_profile_id":row["output_profile_id"],
    })).collect::<Vec<_>>()
    });
    let lane = outputs.profile(7).unwrap();
    let snapshot=lane.snapshot(channel).await.map(|value|serde_json::json!({
        "owner_present":value.owner.is_some(),"index":value.index,"retained_chunks":value.segments.len(),
    }));
    serde_json::json!({"first":first.output_profile_diagnostics(),"second":second.output_profile_diagnostics(),
        "first_raw":first.status(),"second_raw":second.status(),"clients":metadata.ok(),
        "profile":snapshot.ok(),"profile_state":lane.state(channel).await.ok()})
}
#[tokio::test]
#[ignore = "requires real isolated Redis and actual child processes"]
async fn raw_owner_serves_two_replica_profile_viewers_with_one_process_one_provider_and_selective_retirement(
) {
    let redis = std::env::var("REDIS_TEST_URL").expect("isolated Redis required");
    let channel = format!(
        "profile-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let launches = std::env::temp_dir().join(format!("{channel}-launches"));
    let counts = Arc::new(AtomicUsize::new(0));
    let hits = counts.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app=Router::new().route("/live",get(move||{let counts=hits.clone();async move{
        counts.fetch_add(1,Ordering::SeqCst);
        Body::from_stream(async_stream::stream!{loop{
            let mut packet=[0u8;188];packet[..4].copy_from_slice(&[0x47,0,100,0x10]);
            yield Ok::<_,Infallible>(Bytes::copy_from_slice(&packet));time::sleep(Duration::from_millis(2)).await;
        }})
    }}));
    let origin = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("http://{address}/live");
    let (first, first_clients, outputs) = engine(&redis).await;
    let (second, second_clients, _) = engine(&redis).await;
    // First replica is already the real TS owner before remote ensure request.
    let mut raw = first
        .play(source(&channel, &url))
        .await
        .unwrap()
        .into_body();
    drop(raw.frame().await.unwrap().unwrap());
    let command = StreamCommand {
        argv: vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf '%s\\n' $$ >> \"$1\"; exec cat".into(),
            "profile-peer".into(),
            launches.to_str().unwrap().into(),
        ],
    };
    let mut remote = second
        .play_output_profile(source(&channel, &url), 7, command.clone())
        .await
        .unwrap()
        .into_body();
    let mut local = first
        .play_output_profile(source(&channel, &url), 7, command)
        .await
        .unwrap()
        .into_body();
    let remote_frame = remote.frame().await;
    eprintln!(
        "actual-profile-before-frame-check {}",
        diagnostics(&first, &second, &first_clients, &outputs, &channel).await
    );
    eprintln!(
        "actual-profile-launch-log {:?}",
        std::fs::read_to_string(&launches)
    );
    let held_remote = remote_frame
        .expect("actual remote profile frame; diagnostics retained")
        .unwrap()
        .into_data()
        .unwrap();
    let held_local = local
        .frame()
        .await
        .expect("actual local profile frame")
        .unwrap()
        .into_data()
        .unwrap();
    assert!(!held_remote.is_empty());
    assert!(!held_local.is_empty());
    assert_eq!(counts.load(Ordering::SeqCst), 1);
    let launches_raw = std::fs::read_to_string(&launches).unwrap();
    assert_eq!(
        launches_raw.lines().count(),
        1,
        "a follower must not create another transcoder"
    );
    assert_eq!(
        first_clients
            .output_profile_client_count(&channel, 7, ())
            .await
            .unwrap(),
        2
    );
    let mut counter_connection = redis::Client::open(redis.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let actual_provider_count: Option<u32> = redis::cmd("GET")
        .arg("profile_connections:7")
        .query_async(&mut counter_connection)
        .await
        .unwrap();
    assert_eq!(
        actual_provider_count,
        Some(1),
        "profile consumers cannot reserve another upstream provider slot"
    );
    let actual_clients = first_clients.channel_clients(&channel).await.unwrap();
    assert_eq!(
        actual_clients.len(),
        3,
        "exact actual raw1 + profile2 client metadata, no phantom ordinary client"
    );
    assert_eq!(
        actual_clients
            .iter()
            .filter(|client| client["output_profile_id"].as_i64().is_none())
            .count(),
        1
    );
    // Cancelling the remote body with its actual frame still held must retain
    // both actual metadata and internal follower demand until that frame drops.
    drop(remote);
    assert_eq!(
        first_clients
            .output_profile_client_count(&channel, 7, ())
            .await
            .unwrap(),
        2
    );
    assert_eq!(second.status().len(), 1);
    assert_eq!(
        second.status()[0].viewer_count,
        0,
        "internal demand is not another raw transport viewer"
    );
    assert_eq!(
        second.output_profile_diagnostics()["local_channel_demand"][0]["internal_profile_demands"],
        1
    );
    drop(held_remote);
    time::timeout(Duration::from_secs(3), async {
        while !second.status().is_empty()
            || first_clients
                .output_profile_client_count(&channel, 7, ())
                .await
                .unwrap()
                != 1
        {
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !local
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .is_empty(),
        "other replica profile viewer survives remote follower retirement"
    );
    drop(held_local);
    let profile = outputs.profile(7).unwrap();
    assert_eq!(
        profile.state(&channel).await.unwrap().as_deref(),
        Some("active")
    );
    drop(local);
    time::timeout(Duration::from_secs(10), async {
        while profile.snapshot(&channel).await.unwrap().owner.is_some() {
            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.load(Ordering::SeqCst), 1);
    assert!(
        !raw.frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap()
            .is_empty(),
        "raw viewer survives profile retirement"
    );
    drop(raw);
    assert!(second.shutdown_wait(Duration::from_secs(10)).await);
    assert!(first.shutdown_wait(Duration::from_secs(10)).await);
    first_clients.shutdown().await;
    second_clients.shutdown().await;
    origin.abort();
    let _ = origin.await;
    std::fs::remove_file(launches).unwrap();
}
