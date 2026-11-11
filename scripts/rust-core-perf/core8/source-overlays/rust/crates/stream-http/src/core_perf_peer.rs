//! DRAFT production core composition. Original Dispatcharr authors retain credit.
use super::*;
use dispatcharr_input_store::InputConfig;
use serde_json::json;
fn now_ms() -> u64 {
    (epoch_seconds() * 1000.0) as u64
}
async fn start_at(at: u64) {
    while now_ms() < at {
        time::sleep(Duration::from_millis(at.saturating_sub(now_ms()).min(20))).await;
    }
}
async fn drain_gate(end: u64, path: &str, force: bool) {
    if force || now_ms() >= end {
        while !std::path::Path::new(path).exists() { time::sleep(Duration::from_millis(5)).await; }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires owned M1 core benchmark fixture; never run as default tests"]
async fn actual_production_input_store_channel_history_workload() {
    assert_eq!(
        std::env::var("M1_CORE_DISPOSABLE_FIXTURE").unwrap(),
        "owned-core-only"
    );
    let plan: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(std::env::var("M1_CORE_PLAN").unwrap()).unwrap(),
    )
    .unwrap();
    let viewers: usize = std::env::var("M1_CORE_VIEWERS").unwrap().parse().unwrap();
    let period: u64 = std::env::var("M1_CORE_PERIOD_MS").unwrap().parse().unwrap();
    assert!(plan["viewers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v.as_u64() == Some(viewers as u64)));
    let start: u64 = std::env::var("M1_CORE_START_MS").unwrap().parse().unwrap();
    let warm = plan["warmup_seconds"].as_u64().unwrap() * 1000;
    let duration = warm + plan["measure_seconds"].as_u64().unwrap() * 1000;
    assert_eq!(duration % period, 0);
    let total = duration / period;
    let end = start + duration;
    let output = std::env::var("M1_CORE_OUTPUT").unwrap();
    let release = std::env::var("M1_CORE_DRAIN_RELEASE").unwrap();
    assert!(release.starts_with("/evidence/"));
    let url = std::env::var("REDIS_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let channel = format!("m1-core-{}", uuid::Uuid::new_v4());
    let owner = "owned-core-benchmark-producer".to_owned();
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{channel}:owner"))
        .arg(&owner)
        .query_async(&mut redis)
        .await
        .unwrap();
    let store = InputStore::connect(
        &url,
        InputConfig {
            max_chunk_bytes: 255868,
            max_channel_bytes: 128 * 1024 * 1024,
            max_chunks: 1024,
            preserve_source_era: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cache = Channel::new(4 * 1024 * 1024, Duration::from_secs(60), viewers).unwrap();
    let writer = input_writer::InputWriter::new(store.clone(),channel.clone(),owner.clone(),cache.clone(),Arc::new(AtomicU64::new(0)),true,None).await.unwrap();
    let history = Arc::new(history::History {
        store: store.clone(),
        channel: channel.clone(),
        owner: owner.clone(),
        initial_behind_chunks: 0,
        read_timeout: Duration::from_secs(3),
        retained: None,
    });
    let mut packet = [71u8; 188];
    packet[..6].copy_from_slice(&[0x47, 1, 0, 0x30, 1, 0]);
    let expected = Arc::new(packet.repeat(1361));
    let mut tasks = Vec::new();
    for index in 0..viewers {
        let mut viewer = cache.subscribe(Duration::from_secs(60)).unwrap();
        viewer.seek_sequence(1).unwrap();
        let history = history.clone();
        let expected = expected.clone();
        let release = release.clone();
        tasks.push(tokio::spawn(async move {
   start_at(start).await;let slow=viewers>1&&index%10==0;let mut delivered=0u64;let mut measured=0u64;let mut historical=0u64;
   while delivered<total {
    drain_gate(end,&release,false).await;
    let payload=time::timeout(Duration::from_secs(30),history::next(&mut viewer,Some(&history))).await.unwrap().unwrap().unwrap();
    drain_gate(end,&release,false).await;
    assert_eq!(payload.as_ref(),expected.as_slice(),"Every delivered byte must match Source fixture");assert_eq!(viewer.next_sequence(),delivered+2,"Contiguous canonical next-to-read cursor required");
    if matches!(&payload,history::Payload::Historical {..}){historical+=1;}
    let now=now_ms();delivered+=1;if start+warm<=now&&now<end {measured+=1;}
    if slow&&now<end {time::sleep(Duration::from_millis(period*2)).await;}
    drop(payload);
   }
   json!({"delivered":delivered,"measured":measured,"bytes":delivered*255868,"historical_reads":historical})
  }));
    }
    std::fs::write(
        format!("{output}.ready"),
        serde_json::to_vec(&json!({"ready":true,"viewers":viewers,"chunk_bytes":255868})).unwrap(),
    )
    .unwrap();
    start_at(start).await;
    let mut input = InputAccumulator::new(255868);
    let mut produced = 0u64;
    let mut late = 0u64;
    for index in 0..total {
        drain_gate(end,&release,false).await;
        let target = start + index * period;
        start_at(target).await;
        if now_ms().saturating_sub(target) > period / 2 {
            late += 1;
        }
        for frame in expected.chunks(8192) {
            drain_gate(end,&release,false).await;
            if let Some((data, marked)) = assemble_input_window(
                &mut input.assembler,
                &mut input.marker,
                &mut input.mark_chunk,
                &mut input.chunk,
                frame,
                255868,
            ) {
                let sequence = writer.publish(data,marked,epoch_seconds()).await.unwrap();
                produced += 1;
                assert_eq!(sequence, produced);
            }
        }
    }
    assert_eq!(produced, total);
    let mut readers = Vec::new();
    for task in tasks {
        readers.push(
            time::timeout(Duration::from_secs(30), task)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    drain_gate(end,&release,true).await;
    let retained = store.snapshot(&channel).await.unwrap().chunks.len();
    writer.finish().await.unwrap();
    cache.stop();
    assert!(store.cleanup(&channel, &owner).await.unwrap());
    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{channel}:owner"))
        .query_async(&mut redis)
        .await
        .unwrap();
    let report = json!({"passed":true,"backend":"native","scope":"Production InputWriter+bounded assembler +InputStore+Channel+History with real Redis; no HTTP/auth/application benchmark acceptance","viewers":viewers,"period_ms":period,"produced":produced,"producer_late":late,"chunk_bytes":255868,"final_full_bytes_per_viewer":total*255868,"readers":readers,"retained_key_count":retained,"bounded_cached_bytes":cache.retained_bytes(),"drain_release_seen":std::path::Path::new(&release).exists()});
    std::fs::write(output, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
}

// Private functional-only admission/data ownership controls, outside timing.
#[tokio::test]
#[ignore = "owned M1 core Redis and held final-frame control"]
async fn held_cached_and_history_payloads_retain_final_admission_after_stop() {
    assert_eq!(std::env::var("M1_CORE_DISPOSABLE_FIXTURE").as_deref(), Ok("owned-core-only"));
    let payload: Arc<[u8]> = Arc::from(vec![71u8; 188]);
    let cached = Channel::new(188, Duration::from_secs(60), 1).unwrap();
    let mut viewer = cached.subscribe(Duration::from_secs(60)).unwrap();
    viewer.seek_sequence(1).unwrap(); cached.publish(payload.clone()).unwrap();
    let frame = history::next(&mut viewer, None).await.unwrap().unwrap();
    assert!(matches!(frame, history::Payload::Cached(_)));
    assert!(time::timeout(Duration::from_millis(20), history::next(&mut viewer, None)).await.is_err());
    drop(viewer); cached.stop(); assert_eq!(cached.viewer_count(), 1);
    assert_eq!(frame.as_ref(), payload.as_ref()); drop(frame); assert_eq!(cached.viewer_count(), 0);

    let url = std::env::var("REDIS_URL").unwrap();
    let mut redis = redis::Client::open(url.clone()).unwrap().get_multiplexed_async_connection().await.unwrap();
    let channel = format!("m1-core-held-{}", uuid::Uuid::new_v4());
    let owner = "owned-core-held-control".to_owned();
    let _: () = redis::cmd("SET").arg(format!("live:channel:{channel}:owner")).arg(&owner).query_async(&mut redis).await.unwrap();
    let store = InputStore::connect(&url, InputConfig { max_chunk_bytes:255868,max_channel_bytes:128*1024*1024,max_chunks:1024,preserve_source_era:true,..Default::default() }).await.unwrap();
    let cache = Channel::new(188, Duration::from_secs(60), 1).unwrap();
    let mut viewer = cache.subscribe(Duration::from_secs(60)).unwrap(); viewer.seek_sequence(1).unwrap();
    let writer=input_writer::InputWriter::new(store.clone(),channel.clone(),owner.clone(),cache.clone(),Arc::new(AtomicU64::new(0)),true,None).await.unwrap();
    for value in 1..=3u8 {
        let bytes: Arc<[u8]> = Arc::from(vec![value;188]);
        writer.publish(bytes,false,epoch_seconds()).await.unwrap();
    }
    let history = history::History {store:store.clone(),channel:channel.clone(),owner:owner.clone(),initial_behind_chunks:0,read_timeout:Duration::from_secs(3),retained:None};
    let frame = history::next(&mut viewer,Some(&history)).await.unwrap().unwrap();
    assert!(matches!(frame,history::Payload::Historical{..}));
    assert_eq!(frame.as_ref(), &[1u8;188]);
    assert!(time::timeout(Duration::from_millis(20),history::next(&mut viewer,Some(&history))).await.is_err());
    writer.finish().await.unwrap();
    drop(viewer); cache.stop(); assert_eq!(cache.viewer_count(),1);
    assert!(store.cleanup(&channel,&owner).await.unwrap());
    let _: () = redis::cmd("DEL").arg(format!("live:channel:{channel}:owner")).query_async(&mut redis).await.unwrap();
    assert_eq!(frame.as_ref(), &[1u8;188]); drop(frame); assert_eq!(cache.viewer_count(),0);
    std::fs::write(std::env::var("M1_CORE_OUTPUT").unwrap(), serde_json::to_vec_pretty(&json!({"passed":true,"cached_final_viewer_held":true,"cached_bytes_after_stop":true,"history_final_viewer_held":true,"history_bytes_after_owned_redis_cleanup":true,"single_frame_credit_enforced":true})).unwrap()).unwrap();
}
