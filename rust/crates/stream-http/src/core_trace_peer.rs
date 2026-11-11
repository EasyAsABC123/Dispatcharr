//! Actual production assembly, shared writer, Redis, Channel and History trace.
//! Original Dispatcharr authors retain algorithm credit and licenses.
use super::*;
use dispatcharr_input_store::InputConfig;
use serde_json::{json, Value};
fn packet(seed: u8, pid: u16) -> Vec<u8> {
    let mut p = vec![seed % 251; 188];
    p[..6].copy_from_slice(&[0x47, (pid >> 8) as u8, pid as u8, 0x30 | (seed % 16), 1, 0]);
    p
}
fn hex(data: &[u8]) -> String {
    data.iter().map(|v| format!("{v:02x}")).collect()
}
async fn feed(input: &mut InputAccumulator, writer: &input_writer::InputWriter, data: &[u8]) {
    let mut published = false;
    for part in data.chunks(376) {
        if let Some((data, disc)) = assemble_input_window(
            &mut input.assembler,
            &mut input.marker,
            &mut input.mark_chunk,
            &mut input.chunk,
            part,
            376,
        ) {
            writer.publish(data, disc, epoch_seconds()).await.unwrap();
            published = true;
        }
    }
    if published {
        input.marker.disarm();
    }
}
async fn mark(input: &mut InputAccumulator, writer: &input_writer::InputWriter) {
    if let Some((data, disc)) = input.flush_for_switch() {
        writer.publish(data, disc, epoch_seconds()).await.unwrap();
    }
    writer.mark_discontinuity().await.unwrap();
    input.arm_switch();
}
async fn state(
    label: &str,
    input: &InputAccumulator,
    store: &InputStore,
    channel: &str,
    redis: &mut redis::aio::MultiplexedConnection,
    writer: &input_writer::InputWriter,
) -> Value {
    let snapshot = store.snapshot(channel).await.unwrap();
    let mut chunks = Vec::new();
    for stamp in snapshot.chunks.iter() {
        let data = store.get(channel, stamp.sequence).await.unwrap().unwrap();
        chunks.push(
            json!({"sequence":stamp.sequence,"bytes":data.as_ref().len(),"hex":hex(data.as_ref())}),
        );
    }
    let discs: Vec<u64> = redis::cmd("ZRANGE")
        .arg(format!(
            "live:channel:{channel}:input:buffer:discontinuities"
        ))
        .arg(0)
        .arg(-1)
        .query_async(redis)
        .await
        .unwrap();
    assert!(discs.len() <= 64);
    json!({"label":label,"index":writer.index(),"write_buffer_bytes":input.chunk.len(),"partial_packet_bytes":input.assembler.pending_bytes(),"chunks":chunks,"discontinuities":discs,"oldest_last_consumed":snapshot.chunks.iter().map(|v|v.sequence-1).min(),"age0_last_consumed":snapshot.cursor_for_age(0.0,epoch_seconds()),"age60_last_consumed":snapshot.cursor_for_age(60.0,epoch_seconds())})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "owned copied-DB real Redis core trace fixture"]
async fn actual_production_source13_trace() {
    assert_eq!(
        std::env::var("M1_CORE_DISPOSABLE_FIXTURE").unwrap(),
        "owned-core-only"
    );
    let url = std::env::var("REDIS_URL").unwrap();
    let mut redis = redis::Client::open(url.clone())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let channel = format!("core-trace-{}", uuid::Uuid::new_v4());
    let owner = "actual-core-trace-owner".to_owned();
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{channel}:owner"))
        .arg(&owner)
        .query_async(&mut redis)
        .await
        .unwrap();
    let store = InputStore::connect(
        &url,
        InputConfig {
            max_chunk_bytes: 376,
            max_channel_bytes: 376 * 64,
            max_chunks: 64,
            chunk_ttl: Duration::from_secs(2),
            preserve_source_era: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cache = Channel::new(376, Duration::from_secs(2), 1).unwrap();
    let writer = input_writer::InputWriter::new(
        store.clone(),
        channel.clone(),
        owner.clone(),
        cache.clone(),
        Arc::new(AtomicU64::new(0)),
        true,
        None,
    )
    .await
    .unwrap();
    let mut input = InputAccumulator::new(376);
    let mut records = Vec::new();
    records.push(state("empty", &input, &store, &channel, &mut redis, &writer).await);
    let data = [packet(1, 256), packet(2, 257)].concat();
    feed(&mut input, &writer, &data[..100]).await;
    records.push(state("partial100", &input, &store, &channel, &mut redis, &writer).await);
    feed(&mut input, &writer, &data[100..]).await;
    records.push(
        state(
            "published_two_packets",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    let history = history::History {
        store: store.clone(),
        channel: channel.clone(),
        owner: owner.clone(),
        initial_behind_chunks: 0,
        read_timeout: Duration::from_secs(1),
        retained: None,
    };
    let mut viewer = cache.subscribe(Duration::ZERO).unwrap();
    viewer.seek_sequence(1).unwrap();
    let before = viewer.next_sequence();
    let first = history::next(&mut viewer, Some(&history))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.as_ref(), data);
    drop(first);
    records.push(json!({"label":"cursor_adapter","native_next_before":before,"native_next_after":viewer.next_sequence(),"full_bytes_verified":true,"native_execution_observed":true}));
    drop(viewer);
    feed(
        &mut input,
        &writer,
        &[packet(3, 256), packet(4, 256)[..17].to_vec()].concat(),
    )
    .await;
    records.push(
        state(
            "pending_complete_and_partial",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    mark(&mut input, &writer).await;
    records.push(
        state(
            "mark_flushes_complete_drops_partial",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    feed(
        &mut input,
        &writer,
        &[packet(5, 256), packet(6, 256), packet(7, 258)].concat(),
    )
    .await;
    records.push(
        state(
            "post_mark_stamping_first_public_chunk",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    mark(&mut input, &writer).await;
    records.push(
        state(
            "second_mark_flushes_previously_stamped_pending_PID",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    // Actual cache miss, retained Redis payload and final-viewer ownership.
    let mut slow = cache.subscribe(Duration::ZERO).unwrap();
    slow.seek_sequence(1).unwrap();
    let held = history::next(&mut slow, Some(&history))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(held, history::Payload::Historical { .. }));
    assert_eq!(held.as_ref(), data);
    assert!(time::timeout(
        Duration::from_millis(12),
        history::next(&mut slow, Some(&history))
    )
    .await
    .is_err());
    drop(slow);
    assert_eq!(cache.viewer_count(), 1);
    assert!(cache.subscribe(Duration::ZERO).is_err());
    drop(held);
    assert_eq!(cache.viewer_count(), 0);
    input.reset_position();
    records.push(
        state(
            "reset_discards_local_leftovers",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    time::sleep(Duration::from_millis(2200)).await;
    records.push(
        state(
            "real_ttl_expired",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    feed(
        &mut input,
        &writer,
        &[packet(8, 256), packet(9, 256)].concat(),
    )
    .await;
    records.push(
        state(
            "fresh_after_expiry",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    input.finish_position();
    writer.finish().await.unwrap();
    records.push(
        state(
            "stop_preserves_redis_reads",
            &input,
            &store,
            &channel,
            &mut redis,
            &writer,
        )
        .await,
    );
    assert_eq!(
        writer
            .publish(packet(10, 256).into(), false, epoch_seconds())
            .await,
        Err(dispatcharr_stream_runtime::Error::Stopped)
    );
    records.push(json!({"label":"stopped_write_rejected","passed":true}));
    let mut retained = cache.subscribe(Duration::ZERO).unwrap();
    retained.seek_sequence(5).unwrap();
    let retained_data = history::next(&mut retained, Some(&history))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained_data.as_ref().len(), 376);
    drop(retained_data);
    drop(retained);
    // Actual Original constructor reopens the persistent index, including
    // after payload expiry; this is independent of manager ownership change.
    let reopened_cache = Channel::new(376, Duration::from_secs(2), 1).unwrap();
    let reopened = input_writer::InputWriter::new(
        store.clone(),
        channel.clone(),
        owner.clone(),
        reopened_cache.clone(),
        Arc::new(AtomicU64::new(0)),
        true,
        None,
    )
    .await
    .unwrap();
    let mut reopened_input = InputAccumulator::new(376);
    records.push(
        state(
            "new_writer_reads_existing_index",
            &reopened_input,
            &store,
            &channel,
            &mut redis,
            &reopened,
        )
        .await,
    );
    let reopen_history = history::History {
        store: store.clone(),
        channel: channel.clone(),
        owner: owner.clone(),
        initial_behind_chunks: 0,
        read_timeout: Duration::from_secs(1),
        retained: None,
    };
    let mut reopen_reader = reopened_cache.subscribe(Duration::ZERO).unwrap();
    reopen_reader.seek_sequence(reopened.index()).unwrap();
    let reopen_payload = history::next(&mut reopen_reader, Some(&reopen_history))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        reopen_payload,
        history::Payload::Historical { .. }
    ));
    assert_eq!(reopen_payload.as_ref().len(), 376);
    drop(reopen_payload);
    drop(reopen_reader);
    feed(
        &mut reopened_input,
        &reopened,
        &[packet(36, 256), packet(37, 256)].concat(),
    )
    .await;
    records.push(
        state(
            "new_writer_publishes_next_existing_index",
            &reopened_input,
            &store,
            &channel,
            &mut redis,
            &reopened,
        )
        .await,
    );
    reopened.finish().await.unwrap();
    time::sleep(Duration::from_millis(2200)).await;
    let after_cache = Channel::new(376, Duration::from_secs(2), 1).unwrap();
    let after = input_writer::InputWriter::new(
        store.clone(),
        channel.clone(),
        owner.clone(),
        after_cache.clone(),
        Arc::new(AtomicU64::new(0)),
        true,
        None,
    )
    .await
    .unwrap();
    let mut after_input = InputAccumulator::new(376);
    records.push(
        state(
            "new_writer_reads_persistent_index_after_ttl",
            &after_input,
            &store,
            &channel,
            &mut redis,
            &after,
        )
        .await,
    );
    feed(
        &mut after_input,
        &after,
        &[packet(38, 256), packet(39, 256)].concat(),
    )
    .await;
    records.push(
        state(
            "new_writer_continues_after_ttl",
            &after_input,
            &store,
            &channel,
            &mut redis,
            &after,
        )
        .await,
    );
    after.finish().await.unwrap();
    for mode in [
        "initial-mark",
        "partial-stop",
        "complete-partial-stop",
        "reset-partial",
        "delayed-marker",
    ] {
        let key = format!("core-independent-{}", uuid::Uuid::new_v4());
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{key}:owner"))
            .arg(&owner)
            .query_async(&mut redis)
            .await
            .unwrap();
        let local = Channel::new(376, Duration::from_secs(2), 1).unwrap();
        let producer = input_writer::InputWriter::new(
            store.clone(),
            key.clone(),
            owner.clone(),
            local.clone(),
            Arc::new(AtomicU64::new(0)),
            true,
            None,
        )
        .await
        .unwrap();
        let mut pending = InputAccumulator::new(376);
        match mode {
            "initial-mark" => {
                mark(&mut pending, &producer).await;
                records.push(
                    state(
                        "initial_mark_before_first_publication",
                        &pending,
                        &store,
                        &key,
                        &mut redis,
                        &producer,
                    )
                    .await,
                );
                feed(
                    &mut pending,
                    &producer,
                    &[packet(31, 256), packet(32, 256)].concat(),
                )
                .await;
                records.push(
                    state(
                        "initial_mark_first_publication",
                        &pending,
                        &store,
                        &key,
                        &mut redis,
                        &producer,
                    )
                    .await,
                );
                pending.finish_position();
                producer.finish().await.unwrap();
            }
            "partial-stop" => {
                feed(&mut pending, &producer, &packet(33, 256)[..100]).await;
                pending.finish_position();
                producer.finish().await.unwrap();
                records.push(
                    state(
                        "stop_retains_only_partial_packet_source_boundary",
                        &pending,
                        &store,
                        &key,
                        &mut redis,
                        &producer,
                    )
                    .await,
                );
            }
            "complete-partial-stop" => {
                feed(
                    &mut pending,
                    &producer,
                    &[packet(34, 256), packet(35, 256)[..17].to_vec()].concat(),
                )
                .await;
                pending.finish_position();
                producer.finish().await.unwrap();
                records.push(
                    state(
                        "stop_discards_complete_and_partial",
                        &pending,
                        &store,
                        &key,
                        &mut redis,
                        &producer,
                    )
                    .await,
                );
            }
            "delayed-marker" => {
                mark(&mut pending, &producer).await;
                time::sleep(Duration::from_millis(1200)).await;
                feed(
                    &mut pending,
                    &producer,
                    &[packet(50, 256), packet(51, 256)].concat(),
                )
                .await;
                time::sleep(Duration::from_millis(1050)).await;
                let mut row = state(
                    "marker_expires_while_media_retained",
                    &pending,
                    &store,
                    &key,
                    &mut redis,
                    &producer,
                )
                .await;
                let metadata_history = history::History {
                    store: store.clone(),
                    channel: key.clone(),
                    owner: owner.clone(),
                    initial_behind_chunks: 0,
                    read_timeout: Duration::from_secs(1),
                    retained: None,
                };
                let mut reader = local.subscribe(Duration::from_secs(1)).unwrap();
                reader.seek_sequence(1).unwrap();
                let payload = history::next(&mut reader, Some(&metadata_history))
                    .await
                    .unwrap()
                    .unwrap();
                assert!(matches!(payload, history::Payload::Cached(_)));
                row["reader_discontinuity"] = json!(payload.discontinuity());
                drop(payload);
                drop(reader);
                records.push(row);
                producer.finish().await.unwrap();
            }
            _ => {
                feed(&mut pending, &producer, &packet(40, 256)[..100]).await;
                pending.reset_position();
                records.push(
                    state(
                        "reset_discards_only_partial_packet",
                        &pending,
                        &store,
                        &key,
                        &mut redis,
                        &producer,
                    )
                    .await,
                );
                producer.finish().await.unwrap();
            }
        }
        local.stop();
        let _ = store.cleanup(&key, &owner).await.unwrap();
        let _: () = redis::cmd("DEL")
            .arg(format!("live:channel:{key}:owner"))
            .query_async(&mut redis)
            .await
            .unwrap();
    }
    reopened_cache.stop();
    after_cache.stop();
    // stop() remains owner/manager teardown, physically separate from finish.
    cache.stop();
    assert_eq!(cache.retained_bytes(), 0);
    let removed = store.cleanup(&channel, &owner).await.unwrap();
    if !removed {
        assert!(store.snapshot(&channel).await.unwrap().chunks.is_empty());
    }
    // Expired Rust publication fences intentionally make ordinary cleanup a
    // no-op; this is not represented as Source manager cleanup parity.

    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{channel}:owner"))
        .query_async(&mut redis)
        .await
        .unwrap();
    physical_finish_controls(&url).await;
    logical_timeout_retains_writer_until_reply(&url).await;
    physical_reader_reply_controls(&url).await;
    let report = json!({"passed":true,"scope":"actualproductionwriter/assembly/InputStore/Channel/History13trace; no performance transfer","records":records,"producer_finish_retains_actual_reads":true,"manager_stop_separate":true,"expired_payload_cleanup_was_immediate":removed,"actual_history_and_final_owner":true,"physical_cached_metadata_and_historical_read_scopes":true,"physical_cancelled_publish_and_finish":true,"logical_timeout_retains_writer_until_actual_reply":true,"native_owner_fences_separate_from_source_buffer":true});
    std::fs::write(
        std::env::var("M1_CORE_OUTPUT").unwrap(),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
}

async fn physical_finish_controls(url: &str) {
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("core-physical-{}", uuid::Uuid::new_v4());
    let owner = "owned-physical-source";
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{key}:owner"))
        .arg(owner)
        .query_async(&mut redis)
        .await
        .unwrap();
    let store = InputStore::connect(
        url,
        InputConfig {
            max_chunk_bytes: 376,
            max_channel_bytes: 376 * 16,
            max_chunks: 16,
            chunk_ttl: Duration::from_secs(2),
            preserve_source_era: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cache = Channel::new(376, Duration::from_secs(2), 1).unwrap();
    let writer = input_writer::InputWriter::new(
        store.clone(),
        key.clone(),
        owner.to_owned(),
        cache.clone(),
        Arc::new(AtomicU64::new(0)),
        true,
        None,
    )
    .await
    .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(350)
        .arg("WRITE")
        .query_async(&mut redis)
        .await
        .unwrap();
    let publisher = writer.clone();
    let caller = tokio::spawn(async move {
        publisher
            .publish(
                [packet(20, 256), packet(21, 256)].concat().into(),
                false,
                epoch_seconds(),
            )
            .await
    });
    time::sleep(Duration::from_millis(35)).await;
    assert_eq!(writer.index(), 0);
    let finisher = writer.clone();
    let finish = tokio::spawn(async move { finisher.finish().await });
    time::sleep(Duration::from_millis(20)).await;
    caller.abort();
    finish.abort();
    let _ = caller.await;
    let _ = finish.await;
    assert_eq!(
        writer
            .publish(packet(22, 256).into(), false, epoch_seconds())
            .await,
        Err(dispatcharr_stream_runtime::Error::Stopped)
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while writer.index() != 1 {
        assert!(Instant::now() < deadline);
        time::sleep(Duration::from_millis(10)).await;
    }
    writer.finish().await.unwrap();
    assert_eq!(cache.retained_bytes(), 376);
    assert_eq!(
        cache.publish(packet(30, 256).into()),
        Err(dispatcharr_stream_runtime::Error::Stopped)
    );
    let stale = input_writer::InputWriter::new(
        store.clone(),
        key.clone(),
        owner.to_owned(),
        Channel::new(376, Duration::from_secs(2), 1).unwrap(),
        Arc::new(AtomicU64::new(1)),
        true,
        None,
    )
    .await
    .unwrap();
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{key}:owner"))
        .arg("owned-replacement-token")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        stale
            .publish(packet(23, 256).into(), false, epoch_seconds())
            .await,
        Err(dispatcharr_stream_runtime::Error::Stopped)
    );
    assert!(store.mark_next_discontinuity(&key, owner).await.is_err());
    assert!(!store.cleanup(&key, owner).await.unwrap());
    let index: u64 = redis::cmd("GET")
        .arg(format!("live:channel:{key}:input:buffer:index"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(index, 1);
    assert!(store.get(&key, 1).await.unwrap().is_none());
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{key}:owner"))
        .arg(owner)
        .query_async(&mut redis)
        .await
        .unwrap();
    let mut viewer = cache.subscribe(Duration::from_secs(1)).unwrap();
    let held = match viewer.next_owned().await {
        dispatcharr_stream_runtime::OwnedDelivery::Chunk(v) => v,
        _ => panic!("finished writer lost bytes"),
    };
    drop(viewer);
    assert_eq!(cache.viewer_count(), 1);
    assert!(store.cleanup(&key, owner).await.unwrap());
    cache.stop();
    assert_eq!(held.as_ref().len(), 376);
    drop(held);
    assert_eq!(cache.viewer_count(), 0);
    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{key}:owner"))
        .query_async(&mut redis)
        .await
        .unwrap();
}

async fn logical_timeout_retains_writer_until_reply(url: &str) {
    let mut redis = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("core-timeout-{}", uuid::Uuid::new_v4());
    let owner = "owned-timeout-source";
    let _: () = redis::cmd("SET")
        .arg(format!("live:channel:{key}:owner"))
        .arg(owner)
        .query_async(&mut redis)
        .await
        .unwrap();
    let store = InputStore::connect(
        url,
        InputConfig {
            max_chunk_bytes: 376,
            max_channel_bytes: 376 * 16,
            max_chunks: 16,
            command_timeout: Duration::from_millis(40),
            chunk_ttl: Duration::from_secs(2),
            preserve_source_era: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let cache = Channel::new(376, Duration::from_secs(2), 1).unwrap();
    let writer = input_writer::InputWriter::new(
        store.clone(),
        key.clone(),
        owner.to_owned(),
        cache.clone(),
        Arc::new(AtomicU64::new(0)),
        true,
        None,
    )
    .await
    .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(350)
        .arg("WRITE")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        writer
            .publish(
                [packet(44, 256), packet(45, 256)].concat().into(),
                false,
                epoch_seconds()
            )
            .await,
        Err(dispatcharr_stream_runtime::Error::Stopped)
    );
    assert_eq!(writer.index(), 0);
    assert_eq!(cache.retained_bytes(), 0);
    assert!(
        time::timeout(Duration::from_millis(70), writer.finish())
            .await
            .is_err(),
        "logical timeout released physical writer slot"
    );
    assert_eq!(
        writer
            .publish(packet(46, 256).into(), false, epoch_seconds())
            .await,
        Err(dispatcharr_stream_runtime::Error::Stopped)
    );
    writer.finish().await.unwrap();
    assert_eq!(writer.index(), 0);
    assert_eq!(cache.retained_bytes(), 0);
    // Ambiguous late Redis commit is not fabricated as a successful local write.
    let actual: u64 = redis::cmd("GET")
        .arg(format!("live:channel:{key}:input:buffer:index"))
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(actual, 1);
    assert!(store.cleanup(&key, owner).await.unwrap());
    cache.stop();
    let _: () = redis::cmd("DEL")
        .arg(format!("live:channel:{key}:owner"))
        .query_async(&mut redis)
        .await
        .unwrap();
}

struct ScopeMarker(Arc<AtomicBool>);
impl Drop for ScopeMarker {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
async fn physical_reader_reply_controls(url: &str) {
    for cached in [false, true] {
        let mut redis = redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!("core-read-scope-{}", uuid::Uuid::new_v4());
        let owner = "owned-reader-scope";
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{key}:owner"))
            .arg(owner)
            .query_async(&mut redis)
            .await
            .unwrap();
        let store = InputStore::connect(
            url,
            InputConfig {
                max_chunk_bytes: 376,
                max_channel_bytes: 376 * 16,
                max_chunks: 16,
                command_timeout: Duration::from_millis(40),
                chunk_ttl: Duration::from_secs(2),
                preserve_source_era: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cache = Channel::new(376, Duration::from_secs(2), 1).unwrap();
        let writer = input_writer::InputWriter::new(
            store.clone(),
            key.clone(),
            owner.to_owned(),
            cache.clone(),
            Arc::new(AtomicU64::new(0)),
            true,
            None,
        )
        .await
        .unwrap();
        if cached {
            writer.mark_discontinuity().await.unwrap();
        }
        writer
            .publish(
                [packet(60, 256), packet(61, 256)].concat().into(),
                false,
                epoch_seconds(),
            )
            .await
            .unwrap();
        if !cached {
            writer
                .publish(
                    [packet(62, 256), packet(63, 256)].concat().into(),
                    false,
                    epoch_seconds(),
                )
                .await
                .unwrap();
        }
        let settled = Arc::new(AtomicBool::new(false));
        let owned = history::History {
            store: store.clone(),
            channel: key.clone(),
            owner: owner.to_owned(),
            initial_behind_chunks: 0,
            read_timeout: Duration::from_secs(1),
            retained: Some(Arc::new(ScopeMarker(settled.clone()))),
        };
        let mut viewer = cache.subscribe(Duration::ZERO).unwrap();
        viewer.seek_sequence(1).unwrap();
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(350)
            .arg("ALL")
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(history::next(&mut viewer, Some(&owned)).await.is_err());
        drop(viewer);
        drop(owned);
        cache.stop();
        assert_eq!(cache.viewer_count(), 1);
        assert!(!settled.load(Ordering::SeqCst));
        let deadline = Instant::now() + Duration::from_secs(2);
        while !settled.load(Ordering::SeqCst) || cache.viewer_count() != 0 {
            assert!(Instant::now() < deadline);
            time::sleep(Duration::from_millis(10)).await;
        }
        assert!(store.cleanup(&key, owner).await.unwrap());
        let _: () = redis::cmd("DEL")
            .arg(format!("live:channel:{key}:owner"))
            .query_async(&mut redis)
            .await
            .unwrap();
    }
}

mod pending_marker_boundary {
    //! QA-only observation of the frozen production pending marker capacity.
    use super::*;
    use dispatcharr_input_store::InputConfig;
    use serde_json::{json, Value};
    fn packet(seed: u8) -> Vec<u8> {
        let mut p = vec![seed % 251; 188];
        p[..6].copy_from_slice(&[0x47, 1, 0, 0x30 | (seed % 16), 1, 0]);
        p
    }
    async fn state(
        label: &str,
        store: &InputStore,
        key: &str,
        writer: &input_writer::InputWriter,
        redis: &mut redis::aio::MultiplexedConnection,
    ) -> Value {
        let snapshot = store.snapshot(key).await.unwrap();
        let mut rows = Vec::new();
        let mut total = 0;
        for v in snapshot.chunks.iter() {
            let data = store.get(key, v.sequence).await.unwrap().unwrap();
            total += data.as_ref().len();
            rows.push(json!({"sequence":v.sequence,"bytes":data.as_ref().len(),"hex":data.as_ref().iter().map(|b|format!("{b:02x}")).collect::<String>()}));
        }
        let discs: Vec<u64> = redis::cmd("ZRANGE")
            .arg(format!("live:channel:{key}:input:buffer:discontinuities"))
            .arg(0)
            .arg(-1)
            .query_async(redis)
            .await
            .unwrap();
        json!({"label":label,"index":writer.index(),"write_buffer_bytes":0,"partial_packet_bytes":0,"chunks":rows,"discontinuities":discs,"oldest_last_consumed":snapshot.chunks.iter().map(|c|c.sequence-1).min(),"age0_last_consumed":snapshot.cursor_for_age(0.0,epoch_seconds()),"age60_last_consumed":snapshot.cursor_for_age(60.0,epoch_seconds()),"payload_bytes":total})
    }
    #[tokio::test]
    #[ignore = "owned Source/native small capacity counterexample"]
    async fn actual_pending_marker_at_payload_cap() {
        assert_eq!(
            std::env::var("M1_CORE_DISPOSABLE_FIXTURE").unwrap(),
            "owned-core-only"
        );
        let url = std::env::var("REDIS_URL").unwrap();
        let mut redis = redis::Client::open(url.clone())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!("core-cap-{}", uuid::Uuid::new_v4());
        let owner = "owned-cap-producer";
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{key}:owner"))
            .arg(owner)
            .query_async(&mut redis)
            .await
            .unwrap();
        let store = InputStore::connect(
            &url,
            InputConfig {
                max_chunk_bytes: 376,
                max_channel_bytes: 752,
                max_chunks: 2,
                chunk_ttl: Duration::from_secs(2),
                preserve_source_era: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let cache = Channel::new(752, Duration::from_secs(2), 1).unwrap();
        let writer = input_writer::InputWriter::new(
            store.clone(),
            key.clone(),
            owner.to_owned(),
            cache.clone(),
            Arc::new(AtomicU64::new(0)),
            true,
            None,
        )
        .await
        .unwrap();
        let mut input = InputAccumulator::new(376);
        let mut rows = Vec::new();
        for number in [1, 2] {
            let mark = writer.mark_discontinuity().await.unwrap();
            input.arm_switch();
            let mut row = state(&format!("mark{number}"), &store, &key, &writer, &mut redis).await;
            row["mark_result"] = json!(mark);
            rows.push(row);
            let data = [packet(number * 2 - 1), packet(number * 2)].concat();
            let ready = assemble_input_window(
                &mut input.assembler,
                &mut input.marker,
                &mut input.mark_chunk,
                &mut input.chunk,
                &data,
                376,
            )
            .unwrap();
            writer
                .publish(ready.0, ready.1, epoch_seconds())
                .await
                .unwrap();
            input.marker.disarm();
            rows.push(
                state(
                    &format!("publish{number}"),
                    &store,
                    &key,
                    &writer,
                    &mut redis,
                )
                .await,
            );
        }
        let mark = store.mark_next_discontinuity(&key, owner).await;
        let mut row = state(
            "pending_mark3_at_full_payload_cap",
            &store,
            &key,
            &writer,
            &mut redis,
        )
        .await;
        match mark {
            Ok(value) => row["mark_result"] = json!(value),
            Err(error) => row["mark_error"] = json!(format!("{error:?}")),
        };
        rows.push(row);
        assert_eq!(rows.last().unwrap()["mark_result"], 3);
        assert_eq!(rows.last().unwrap()["payload_bytes"], 752);
        assert_eq!(rows.last().unwrap()["discontinuities"], json!([1, 2, 3]));
        let pending = store.retained_diagnostics(&key).await.unwrap();
        assert_eq!(pending["retained_chunks"], 2);
        assert_eq!(pending["logical_payload_bytes"], 752);
        // More than one pending sidecar beyond the media cap remains corruption.
        let _: () = redis::cmd("ZADD")
            .arg(format!("live:channel:{key}:input:buffer:discontinuities"))
            .arg(999)
            .arg("999")
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(matches!(
            store.mark_next_discontinuity(&key, owner).await,
            Err(dispatcharr_input_store::Error::Corrupt)
        ));
        assert!(store.snapshot(&key).await.is_err());
        let _: () = redis::cmd("ZREM")
            .arg(format!("live:channel:{key}:input:buffer:discontinuities"))
            .arg("999")
            .query_async(&mut redis)
            .await
            .unwrap();
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{key}:owner"))
            .arg("owned-cap-successor")
            .query_async(&mut redis)
            .await
            .unwrap();
        assert!(matches!(
            store.mark_next_discontinuity(&key, owner).await,
            Err(dispatcharr_input_store::Error::LostOwnership)
        ));
        assert!(!store.cleanup(&key, owner).await.unwrap());
        let _: () = redis::cmd("SET")
            .arg(format!("live:channel:{key}:owner"))
            .arg(owner)
            .query_async(&mut redis)
            .await
            .unwrap();
        // A third actual payload remains bounded to two; this is explicitly outside
        // the Source/common 752-byte history domain, not claimed as byte parity.
        input.arm_switch();
        let data = [packet(5), packet(6)].concat();
        let ready = assemble_input_window(
            &mut input.assembler,
            &mut input.marker,
            &mut input.mark_chunk,
            &mut input.chunk,
            &data,
            376,
        )
        .unwrap();
        writer
            .publish(ready.0, ready.1, epoch_seconds())
            .await
            .unwrap();
        input.marker.disarm();
        let bounded = store.snapshot(&key).await.unwrap();
        assert_eq!(bounded.chunks.len(), 2);
        assert_eq!(bounded.chunks[0].sequence, 2);
        assert_eq!(bounded.chunks[1].sequence, 3);
        drop(bounded);
        let bytes = store.retained_diagnostics(&key).await.unwrap();
        assert_eq!(bytes["logical_payload_bytes"], 752);
        let _: () = redis::cmd("CLIENT")
            .arg("PAUSE")
            .arg(250)
            .arg("ALL")
            .query_async(&mut redis)
            .await
            .unwrap();
        let run = writer.clone();
        let caller = tokio::spawn(async move { run.mark_discontinuity().await });
        time::sleep(Duration::from_millis(25)).await;
        caller.abort();
        let _ = caller.await;
        assert_eq!(
            writer
                .publish(packet(8).into(), false, epoch_seconds())
                .await,
            Err(dispatcharr_stream_runtime::Error::AtCapacity)
        );
        assert!(time::timeout(Duration::from_millis(50), writer.finish())
            .await
            .is_err());
        writer.finish().await.unwrap();
        let final_di: Vec<u64> = redis::cmd("ZRANGE")
            .arg(format!("live:channel:{key}:input:buffer:discontinuities"))
            .arg(0)
            .arg(-1)
            .query_async(&mut redis)
            .await
            .unwrap();
        assert_eq!(final_di, vec![2, 3, 4]);
        cache.stop();
        store.cleanup(&key, owner).await.unwrap();
        let _: () = redis::cmd("DEL")
            .arg(format!("live:channel:{key}:owner"))
            .query_async(&mut redis)
            .await
            .unwrap();
        std::fs::write(std::env::var("M1_CORE_OUTPUT").unwrap(),serde_json::to_vec_pretty(&json!({"passed":true,"scope":"capturedfrozenproductioncapacityoutcome; observercompletionisnotSourceparity","max_payload_chunks":2,"max_payload_bytes":752,"records":rows,"producer_finish_retains_actual_reads":false,"one_pending_marker_extra_within_payload_cap":true,"explicit_overcap_foreign_cancel_controls":true,"third_publication_still_payload_bounded":true})).unwrap()).unwrap();
    }
}
