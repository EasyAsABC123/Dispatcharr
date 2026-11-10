//! Actual Redis cache/fence/legacy/retention witnesses. Original author credit.
use super::*;
use std::time::{SystemTime, UNIX_EPOCH};
fn prefix(label: &str) -> String {
    format!(
        "epg_content:owned-native-{label}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
async fn fixture(config: CacheConfig) -> Cache {
    let url = std::env::var("REDIS_TEST_URL").expect("owned Redis required");
    assert!(url.contains("playback-fixture-only"));
    Cache::connect(&url, config).await.unwrap()
}
fn guard() -> Arc<dyn Send + Sync> {
    Arc::new(())
}
async fn raw(cache: &Cache, script: &str, keys: &[String], args: &[&[u8]]) {
    let _: (i64, _) = cache
        .inner
        .transport
        .command(script, keys, args, guard())
        .await
        .unwrap();
}
async fn remove(cache: &Cache, bases: &[String]) {
    assert!(bases
        .iter()
        .all(|v| v.starts_with("epg_content:owned-native-")));
    raw(cache,"local c='rust:xmltv:v1:catalog';local n=tonumber(redis.call('GET',c..':bytes')or'0');for _,k in ipairs(KEYS)do local old=redis.call('HGET',c,k);if old then n=n-cjson.decode(old).bytes end;redis.call('HDEL',c,k);redis.call('ZREM',c..':expiry',k);redis.call('DEL',k..':chunks',k..':status',k..':ready',k..':lock',k..':rust_fence')end;redis.call('SET',c..':bytes',n);return 1",bases,&[]).await;
}
async fn body(cache: &Cache, base: &str) -> Vec<u8> {
    let mut cursor = Cursor::default();
    let mut out = Vec::new();
    loop {
        match cache.read(base, &mut cursor, true, guard()).await.unwrap() {
            Read::Data(data) => out.extend_from_slice(data.as_ref()),
            Read::End => return out,
            _ => panic!("ready cache expected"),
        }
    }
}
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn legacy_chunks_generation_and_stale_owner_fences() {
    let cache = fixture(CacheConfig {
        chunk_bytes: 16,
        ..CacheConfig::default()
    })
    .await;
    let name = prefix("fence");
    let old = cache
        .open(&name, "old-owner-aaaaaaaa", guard())
        .await
        .unwrap();
    assert_eq!(old.state, EntryState::Leader);
    old.put("old-first", guard()).await.unwrap();
    assert_eq!(
        cache
            .open(&name, "new-owner-bbbbbbbb", guard())
            .await
            .unwrap()
            .state,
        EntryState::Follower
    );
    raw(
        &cache,
        "redis.call('DEL',KEYS[1]);return 1",
        &[format!("{}:lock", old.base)],
        &[],
    )
    .await;
    let next = cache
        .open(&name, "new-owner-bbbbbbbb", guard())
        .await
        .unwrap();
    next.put("new-only", guard()).await.unwrap();
    next.finish(guard()).await.unwrap();
    assert_eq!(
        old.put("old-last", guard()).await.err(),
        Some(CacheError::LostOwner)
    );
    assert!(!old.abort(guard()).await.unwrap());
    assert_eq!(body(&cache, &next.base).await, b"new-only");
    let name2 = prefix("generation");
    let older = cache
        .open(&name2, "old-owner-cccccccc", guard())
        .await
        .unwrap();
    older.put("old-first", guard()).await.unwrap();
    raw(
        &cache,
        "redis.call('INCR','epg_content_generation');return 1",
        &[],
        &[],
    )
    .await;
    let newer = cache
        .open(&name2, "new-owner-dddddddd", guard())
        .await
        .unwrap();
    assert_ne!(older.base, newer.base);
    newer.put("new", guard()).await.unwrap();
    newer.finish(guard()).await.unwrap();
    older.put("old-last", guard()).await.unwrap();
    older.finish(guard()).await.unwrap();
    assert_eq!(body(&cache, &older.base).await, b"old-firstold-last");
    assert_eq!(body(&cache, &newer.base).await, b"new");
    let legacy = prefix("legacy");
    let legacybase = format!("{legacy}:g=0");
    raw(&cache,"redis.call('RPUSH',KEYS[1]..':chunks',ARGV[1]);redis.call('SET',KEYS[1]..':ready','1','EX',300);redis.call('SET',KEYS[1]..':status','ready','EX',300);return 1",std::slice::from_ref(&legacybase),&["Legacy UTF8 😀 東京 with oversized logical chunk".as_bytes()]).await;
    assert_eq!(
        body(&cache, &legacybase).await,
        "Legacy UTF8 😀 東京 with oversized logical chunk".as_bytes()
    );
    remove(&cache, &[old.base, older.base, newer.base, legacybase]).await;
}
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn byte_entry_caps_expiry_and_retained_frame_accounting() {
    let cache = fixture(CacheConfig {
        max_entries: 1,
        max_cache_bytes: 8,
        max_export_bytes: 8,
        chunk_bytes: 8,
        read_batch_bytes: 8,
        max_read_bytes: 64,
        cache_ttl: Duration::from_secs(1),
        lock_ttl: Duration::from_secs(1),
        error_ttl: Duration::from_secs(1),
        ..CacheConfig::default()
    })
    .await;
    let a = cache
        .open(&prefix("capacity"), "old-owner-aaaaaaaa", guard())
        .await
        .unwrap();
    a.put("12345678", guard()).await.unwrap();
    assert_eq!(a.put("9", guard()).await.err(), Some(CacheError::Capacity));
    a.finish(guard()).await.unwrap();
    assert_eq!(
        cache
            .open(&prefix("overflow"), "new-owner-bbbbbbbb", guard())
            .await
            .err(),
        Some(CacheError::Capacity)
    );
    let mut cursor = Cursor::default();
    let frame = match cache
        .read(&a.base, &mut cursor, false, guard())
        .await
        .unwrap()
    {
        Read::Data(data) => bytes::Bytes::from_owner(data),
        _ => panic!("data expected"),
    };
    let clone = frame.clone();
    drop(frame);
    assert_eq!(cache.available_io_bytes(), 48);
    tokio::time::sleep(Duration::from_millis(1150)).await;
    let b = cache
        .open(&prefix("after-expiry"), "new-owner-cccccccc", guard())
        .await
        .unwrap();
    b.put("next", guard()).await.unwrap();
    b.finish(guard()).await.unwrap();
    assert_eq!(clone.as_ref(), b"12345678");
    assert_eq!(cache.available_io_bytes(), 48);
    drop(clone);
    assert_eq!(cache.available_io_bytes(), 64);
    remove(&cache, &[a.base, b.base]).await;
}
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn abort_is_fenced_and_error_rebuilds() {
    let cache = fixture(CacheConfig::default()).await;
    let name = prefix("abort");
    let first = cache
        .open(&name, "old-owner-aaaaaaaa", guard())
        .await
        .unwrap();
    first.put("partial", guard()).await.unwrap();
    assert!(first.abort(guard()).await.unwrap());
    let mut cursor = Cursor::default();
    assert!(matches!(
        cache
            .read(&first.base, &mut cursor, false, guard())
            .await
            .unwrap(),
        Read::Failed
    ));
    let second = cache
        .open(&name, "new-owner-bbbbbbbb", guard())
        .await
        .unwrap();
    second.put("replacement", guard()).await.unwrap();
    assert!(!first.abort(guard()).await.unwrap());
    second.finish(guard()).await.unwrap();
    assert_eq!(body(&cache, &second.base).await, b"replacement");
    remove(&cache, &[first.base]).await;
}
async fn pause() {
    use std::io::{BufRead, Write};
    let url = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    let socket =
        std::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap_or(6379)))
            .unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut socket = std::io::BufReader::new(socket);
    let password = url.password().unwrap();
    let auth = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{password}\r\n", password.len());
    socket.get_mut().write_all(auth.as_bytes()).unwrap();
    let mut line = String::new();
    socket.read_line(&mut line).unwrap();
    assert_eq!(line, "+OK\r\n");
    socket
        .get_mut()
        .write_all(b"*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$3\r\n300\r\n")
        .unwrap();
    line.clear();
    socket.read_line(&mut line).unwrap();
    assert_eq!(line, "+OK\r\n");
}
#[tokio::test]
#[ignore = "requires isolated real Redis: pauses only owned fixture"]
async fn cancelled_open_and_reads_keep_physical_admission_and_cleanup() {
    let cache = fixture(CacheConfig::default()).await;
    let name = prefix("cancel-open");
    let requests = Arc::new(Semaphore::new(1));
    let request: Arc<dyn Send + Sync> = Arc::new(requests.clone().acquire_owned().await.unwrap());
    pause().await;
    let opened = cache.clone();
    let requested = name.clone();
    let caller = tokio::spawn(async move {
        opened
            .open(&requested, "cancel-owner-eeeeeeee", request)
            .await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    caller.abort();
    let _ = caller.await;
    assert_eq!(requests.available_permits(), 0);
    assert!(!cache.drain_cleanup(Duration::from_millis(15)).await);
    tokio::time::timeout(Duration::from_secs(2), async {
        while requests.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(cache.drain_cleanup(Duration::from_secs(1)).await);
    let entry = cache
        .open(&name, "successor-owner-ffffff", guard())
        .await
        .unwrap();
    assert_eq!(entry.state, EntryState::Leader);
    entry.put("data", guard()).await.unwrap();
    entry.finish(guard()).await.unwrap();
    let maximum = cache.available_io_bytes();
    let request: Arc<dyn Send + Sync> = Arc::new(requests.clone().acquire_owned().await.unwrap());
    pause().await;
    let reading = cache.clone();
    let base = entry.base.clone();
    let caller = tokio::spawn(async move {
        reading
            .read(&base, &mut Cursor::default(), false, request)
            .await
    });
    tokio::time::timeout(Duration::from_millis(100), async {
        while cache.available_io_bytes() == maximum {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    assert!(cache.available_io_bytes() < maximum);
    assert_eq!(requests.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        while requests.available_permits() != 1 || cache.available_io_bytes() != maximum {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(body(&cache, &entry.base).await, b"data");
    remove(&cache, &[entry.base]).await;
}
#[tokio::test]
#[ignore = "requires isolated real Redis: REDIS_TEST_URL"]
async fn final_frame_clone_blocks_next_read_until_released() {
    let cache = fixture(CacheConfig {
        chunk_bytes: 8,
        read_batch_bytes: 8,
        ..CacheConfig::default()
    })
    .await;
    let entry = cache
        .open(&prefix("one-frame"), "frame-owner-aaaaaaaa", guard())
        .await
        .unwrap();
    entry.put("first", guard()).await.unwrap();
    entry.put("second", guard()).await.unwrap();
    entry.finish(guard()).await.unwrap();
    let mut cursor = Cursor::default();
    let frame = match cache
        .read(&entry.base, &mut cursor, false, guard())
        .await
        .unwrap()
    {
        Read::Data(data) => bytes::Bytes::from_owner(data),
        _ => panic!("data expected"),
    };
    assert_eq!(frame.as_ref(), b"firstsec");
    let held = frame.clone();
    drop(frame);
    let available = cache.available_io_bytes();
    let reading = cache.clone();
    let base = entry.base.clone();
    let mut caller =
        tokio::spawn(async move { reading.read(&base, &mut cursor, false, guard()).await });
    assert!(tokio::time::timeout(Duration::from_millis(25), &mut caller)
        .await
        .is_err());
    assert_eq!(cache.available_io_bytes(), available);
    drop(held);
    let data = tokio::time::timeout(Duration::from_secs(1), caller)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match data {
        Read::Data(data) => assert_eq!(data.as_ref(), b"ond"),
        _ => panic!("second frame expected"),
    };
    remove(&cache, &[entry.base]).await;
}

#[tokio::test]
#[ignore = "requires isolated actual Redis: REDIS_TEST_URL"]
async fn corrupt_catalog_cannot_partially_delete_current_owner() {
    let cache = fixture(CacheConfig::default()).await;
    let entry = cache
        .open(&prefix("corrupt-total"), "corrupt-total-aaaaaaaa", guard())
        .await
        .unwrap();
    entry.put("retained", guard()).await.unwrap();
    let original:i64=cache.inner.transport.command("local key='rust:xmltv:v1:catalog:bytes';local old=tonumber(redis.call('GET',key));redis.call('SET',key,0);return old",&[],&[],guard()).await.unwrap().0;
    assert_eq!(
        entry.put("blocked", guard()).await,
        Err(CacheError::Corrupt)
    );
    assert_eq!(entry.finish(guard()).await, Err(CacheError::Corrupt));
    assert_eq!(entry.abort(guard()).await, Err(CacheError::Corrupt));
    let count: i64 = cache
        .inner
        .transport
        .command(
            "return redis.call('LLEN',KEYS[1])",
            &[format!("{}:chunks", entry.base)],
            &[],
            guard(),
        )
        .await
        .unwrap()
        .0;
    assert_eq!(count, 1);
    raw(
        &cache,
        "redis.call('SET','rust:xmltv:v1:catalog:bytes',ARGV[1]);return 1",
        &[],
        &[original.to_string().as_bytes()],
    )
    .await;
    assert!(entry.abort(guard()).await.unwrap());
    remove(&cache, &[entry.base]).await;
}

#[tokio::test]
#[ignore = "requires isolated actual Redis: REDIS_TEST_URL"]
async fn bounded_pull_delivery_singleflight_utf8_and_one_source() {
    use crate::delivery::{Delivery, Source};
    let cache = fixture(CacheConfig {
        chunk_bytes: 17,
        ..CacheConfig::default()
    })
    .await;
    let key = prefix("delivery");
    let expected = "日本語<&>café repeated body".repeat(100);
    let entry = cache
        .open(&key, "delivery-leader-aaaaa", guard())
        .await
        .unwrap();
    let base = entry.base.clone();
    let frames = vec![Ok(expected.clone()), Ok("last".into())];
    let source: Source = Box::pin(futures_util::stream::iter(frames));
    let mut leader =
        Delivery::new(cache.clone(), entry, Some(source), Duration::from_secs(10)).unwrap();
    let mut followers = Vec::new();
    for index in 0..8 {
        let entry = cache
            .open(&key, &format!("delivery-follower-{index:08}"), guard())
            .await
            .unwrap();
        assert_eq!(entry.state, EntryState::Follower);
        let mut reader =
            Delivery::new(cache.clone(), entry, None, Duration::from_secs(10)).unwrap();
        followers.push(tokio::spawn(async move {
            let mut data = Vec::new();
            while let Some(frame) = reader.next(guard()).await.unwrap() {
                data.extend_from_slice(frame.as_ref());
            }
            data
        }));
    }
    let mut data = Vec::new();
    while let Some(frame) = leader.next(guard()).await.unwrap() {
        data.extend_from_slice(frame.as_ref());
    }
    assert!(leader.completed_build());
    let expected = format!("{expected}last").into_bytes();
    assert_eq!(data, expected);
    for task in followers {
        assert_eq!(task.await.unwrap(), expected);
    }
    remove(&cache, &[base]).await;
}

#[tokio::test]
#[ignore = "requires isolated actual Redis; compares same stored members under two bounded read policies"]
async fn bounded_batch_reduces_reads_without_mutating_members_or_legacy_bytes() {
    use sha2::{Digest, Sha256};
    let batched = fixture(CacheConfig::default()).await;
    let single = fixture(CacheConfig {
        read_batch_bytes: 64 * 1024,
        ..CacheConfig::default()
    })
    .await;
    let entry = batched
        .open(&prefix("batch-shape"), "batch-owner-aaaaaaaa", guard())
        .await
        .unwrap();
    let expected = vec![b'x'; 13_015_448];
    for member in expected.chunks(64 * 1024) {
        entry
            .put(std::str::from_utf8(member).unwrap(), guard())
            .await
            .unwrap();
    }
    entry.finish(guard()).await.unwrap();
    async fn shape(cache: &Cache, base: &str) -> Vec<i64> {
        cache.inner.transport.command::<Vec<i64>,_>(
            "local n=redis.call('LLEN',KEYS[1]);local total=0;local lo=2147483647;local hi=0;for i=0,n-1 do local size=string.len(redis.call('LINDEX',KEYS[1],i));total=total+size;lo=math.min(lo,size);hi=math.max(hi,size)end;return{n,total,lo,hi}",
            &[format!("{base}:chunks")],&[],guard()).await.unwrap().0
    }
    async fn digest(cache: &Cache, base: &str) -> (String, usize) {
        let mut cursor = Cursor::default();
        let mut hash = Sha256::new();
        let mut calls = 0;
        loop {
            calls += 1;
            match cache.read(base, &mut cursor, false, guard()).await.unwrap() {
                Read::Data(data) => {
                    assert!(data.as_ref().len() <= cache.read_batch_bytes());
                    hash.update(data.as_ref());
                }
                Read::End => return (format!("{:x}", hash.finalize()), calls),
                _ => panic!("ready data expected"),
            }
        }
    }
    let before = shape(&batched, &entry.base).await;
    let (old_hash, old_reads) = digest(&single, &entry.base).await;
    let (new_hash, new_reads) = digest(&batched, &entry.base).await;
    assert_eq!(old_hash, format!("{:x}", Sha256::digest(&expected)));
    assert_eq!(old_hash, new_hash);
    assert_eq!(before, shape(&batched, &entry.base).await);
    assert_eq!(before[0], 199);
    assert_eq!(old_reads, 200);
    assert_eq!(new_reads, 26);
    assert_eq!(
        batched.available_io_bytes(),
        CacheConfig::default().max_read_bytes
    );
    // Source-written members exceed the new batch bound: slice, never reject
    // merely for exceeding batch size, and do not mutate the legacy member.
    let legacy_base = format!("{}:g=0", prefix("batch-legacy"));
    let legacy = "東京😀".repeat(60_000);
    raw(&batched,"redis.call('RPUSH',KEYS[1]..':chunks',ARGV[1]);redis.call('SET',KEYS[1]..':ready','1','EX',300);redis.call('SET',KEYS[1]..':status','ready','EX',300);return 1",std::slice::from_ref(&legacy_base),&[legacy.as_bytes()]).await;
    let legacy_shape = shape(&batched, &legacy_base).await;
    let (legacy_hash, legacy_reads) = digest(&batched, &legacy_base).await;
    assert_eq!(
        legacy_hash,
        format!("{:x}", Sha256::digest(legacy.as_bytes()))
    );
    assert_eq!(legacy_reads, 3);
    assert_eq!(legacy_shape, shape(&batched, &legacy_base).await);
    // A bounded walk also handles empty members without spinning forever.
    let empty_base = format!("{}:g=0", prefix("batch-empty"));
    raw(&batched,"for i=1,40 do redis.call('RPUSH',KEYS[1]..':chunks','')end;redis.call('RPUSH',KEYS[1]..':chunks','tail');redis.call('SET',KEYS[1]..':ready','1','EX',300);redis.call('SET',KEYS[1]..':status','ready','EX',300);return 1",std::slice::from_ref(&empty_base),&[]).await;
    let (empty_hash, empty_reads) = digest(&batched, &empty_base).await;
    assert_eq!(empty_hash, format!("{:x}", Sha256::digest(b"tail")));
    assert_eq!(empty_reads, 3);
    println!("XMLTV_BATCH_DIAGNOSTIC={{\"body_bytes\":{},\"stored_members\":{},\"single64k_physical_reads\":{old_reads},\"batch512k_physical_reads\":{new_reads},\"legacy_member_bytes\":{},\"legacy_physical_reads\":{legacy_reads},\"unchanged_member_shape\":true}}",expected.len(),before[0],legacy.len());
    remove(&batched, &[entry.base, legacy_base, empty_base]).await;
}

#[tokio::test]
#[ignore = "requires isolated actual Redis; oversized later member preserves valid prefix"]
async fn bounded_batch_reports_bad_member_after_valid_prefix() {
    let cache = fixture(CacheConfig {
        max_legacy_chunk_bytes: 64 * 1024,
        ..CacheConfig::default()
    })
    .await;
    let base = format!("{}:g=0", prefix("batch-invalid"));
    let bad = vec![b'x'; 64 * 1024 + 1];
    raw(&cache,"redis.call('RPUSH',KEYS[1]..':chunks','valid',ARGV[1],'tail');redis.call('SET',KEYS[1]..':ready','1','EX',300);redis.call('SET',KEYS[1]..':status','ready','EX',300);return 1",std::slice::from_ref(&base),&[&bad]).await;
    let mut cursor = Cursor::default();
    match cache
        .read(&base, &mut cursor, false, guard())
        .await
        .unwrap()
    {
        Read::Data(data) => assert_eq!(data.as_ref(), b"valid"),
        _ => panic!("valid prefix expected"),
    }
    assert_eq!(cursor.chunk, 1);
    assert_eq!(
        cache.read(&base, &mut cursor, false, guard()).await.err(),
        Some(CacheError::Capacity)
    );
    assert_eq!(cursor.chunk, 1);
    remove(&cache, &[base]).await;
}

#[tokio::test]
#[ignore = "requires isolated actual Redis;100 bounded ready readers and one held slow frame"]
async fn bounded_batch_fanout100_progresses_with_detached_slow_frame() {
    use crate::delivery::Delivery;
    use sha2::{Digest, Sha256};
    let cache = fixture(CacheConfig::default()).await;
    let key = prefix("batch-fanout");
    let entry = cache
        .open(&key, "batch-fanout-owner-aaaa", guard())
        .await
        .unwrap();
    let base = entry.base.clone();
    let expected = "bounded UTF8 body &amp; channel data\n".repeat(32_000);
    let expected_hash = format!("{:x}", Sha256::digest(expected.as_bytes()));
    for bytes in expected.as_bytes().chunks(64 * 1024) {
        entry
            .put(std::str::from_utf8(bytes).unwrap(), guard())
            .await
            .unwrap();
    }
    entry.finish(guard()).await.unwrap();
    drop(entry); // Completed leader handle retains an acquisition guard.
    let mut cursor = Cursor::default();
    let held = match cache
        .read(&base, &mut cursor, false, guard())
        .await
        .unwrap()
    {
        Read::Data(data) => bytes::Bytes::from_owner(data),
        _ => panic!("data expected"),
    };
    let last = held.clone();
    drop(held);
    let barrier = Arc::new(tokio::sync::Barrier::new(101));
    let mut readers = Vec::new();
    for id in 0..100 {
        let cache = cache.clone();
        let key = key.clone();
        let barrier = barrier.clone();
        let expected_hash = expected_hash.clone();
        readers.push(tokio::spawn(async move {
            let entry = cache
                .open(&key, &format!("batch-ready-reader-{id:04}"), guard())
                .await
                .unwrap();
            assert_eq!(entry.state, EntryState::Ready);
            let mut delivery = Delivery::new(cache, entry, None, Duration::from_secs(10)).unwrap();
            barrier.wait().await;
            let mut hash = Sha256::new();
            while let Some(frame) = delivery.next(guard()).await.unwrap() {
                hash.update(frame.as_ref());
            }
            assert_eq!(format!("{:x}", hash.finalize()), expected_hash);
        }));
    }
    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        for reader in readers {
            reader.await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        cache.available_io_bytes(),
        CacheConfig::default().max_read_bytes - 2 * cache.read_batch_bytes()
    );
    drop(last);
    assert_eq!(
        cache.available_io_bytes(),
        CacheConfig::default().max_read_bytes
    );
    assert!(cache.drain_cleanup(Duration::from_secs(1)).await);
    remove(&cache, &[base]).await;
    println!(
        "XMLTV_BATCH_FANOUT=100;HELD_SLOW_FRAME_INDEPENDENT=true;FINAL_CLONE_BYTES_RETURNED=true"
    );
}

#[tokio::test]
#[ignore = "requires isolated real Redis; partial-batch publication and delivery error ordering"]
async fn multi_put_preserves_valid_prefix_and_deferred_capacity_error() {
    use crate::delivery::{Delivery, Source};
    for batch in [8, 16] {
        let cache = fixture(CacheConfig {
            chunk_bytes: 8,
            read_batch_bytes: batch,
            max_read_bytes: 128,
            max_export_bytes: 9,
            max_cache_bytes: 1024,
            ..CacheConfig::default()
        })
        .await;
        let entry = cache
            .open(
                &prefix(&format!("put-prefix-{batch}")),
                "put-prefix-owner-aaaa",
                guard(),
            )
            .await
            .unwrap();
        let base = entry.base.clone();
        let source: Source = Box::pin(futures_util::stream::iter(vec![Ok("12345678abc".into())]));
        let mut delivery =
            Delivery::new(cache.clone(), entry, Some(source), Duration::from_secs(2)).unwrap();
        let frame = delivery.next(guard()).await.unwrap().unwrap();
        assert_eq!(frame.as_ref(), b"12345678");
        drop(frame);
        assert_eq!(
            delivery.next(guard()).await.err(),
            Some(CacheError::Capacity)
        );
        drop(delivery);
        raw(&cache,"if redis.call('LLEN',KEYS[1]..':chunks')~=0 or redis.call('GET',KEYS[1]..':status')~='error'then return redis.error_reply('prefix cleanup')end;return 1",std::slice::from_ref(&base),&[]).await;
        remove(&cache, &[base]).await;
    }
    let cache = fixture(CacheConfig {
        chunk_bytes: 8,
        read_batch_bytes: 32,
        max_read_bytes: 256,
        ..CacheConfig::default()
    })
    .await;
    let entry = cache
        .open(&prefix("put-malformed"), "put-malformed-owner-aa", guard())
        .await
        .unwrap();
    let result = entry
        .put_batch(&["valid", "123456789", "tail"], guard())
        .await
        .unwrap();
    assert_eq!(result.accepted, 1);
    assert!(result.capacity_reached);
    let mut cursor = Cursor::default();
    match cache
        .read(&entry.base, &mut cursor, false, guard())
        .await
        .unwrap()
    {
        Read::Data(data) => assert_eq!(data.as_ref(), b"valid"),
        _ => panic!("valid prefix expected"),
    }
    entry.abort(guard()).await.unwrap();
    remove(&cache, std::slice::from_ref(&entry.base)).await;
    drop(entry);
    assert!(cache.drain_cleanup(Duration::from_secs(1)).await);
    println!("XMLTV_MULTI_PUT_PREFIX=single8_and_batch16_equal;MALFORMED_NEXT_MEMBER=prefix_preserved_explicit_error");
}

#[tokio::test]
#[ignore = "requires isolated real Redis; batched write quota, UTF8 and stale fencing"]
async fn multi_put_bounds_member_count_quota_and_stale_owner() {
    let cache = fixture(CacheConfig {
        chunk_bytes: 8,
        read_batch_bytes: 64,
        max_read_bytes: 512,
        max_chunks: 2,
        ..CacheConfig::default()
    })
    .await;
    let key = prefix("put-fences");
    let old = cache
        .open(&key, "put-old-owner-aaaaaa", guard())
        .await
        .unwrap();
    let result = old
        .put_batch(&["東京", "café", "later"], guard())
        .await
        .unwrap();
    assert_eq!(result.accepted, 2);
    assert!(result.capacity_reached);
    let mut cursor = Cursor::default();
    match cache
        .read(&old.base, &mut cursor, false, guard())
        .await
        .unwrap()
    {
        Read::Data(data) => assert_eq!(data.as_ref(), "東京café".as_bytes()),
        _ => panic!("committed valid UTF8 prefix expected"),
    }
    // Reacquire the same generation after retiring only its ready/lock flags.
    raw(
        &cache,
        "redis.call('DEL',KEYS[1]..':ready',KEYS[1]..':lock');return 1",
        std::slice::from_ref(&old.base),
        &[],
    )
    .await;
    let newer = cache
        .open(&key, "put-new-owner-bbbbbb", guard())
        .await
        .unwrap();
    assert_eq!(newer.state, EntryState::Leader);
    assert_eq!(
        old.put_batch(&["stale"], guard())
            .await
            .err()
            .map(|e| e == CacheError::LostOwner),
        Some(true)
    );
    let result = newer.put_batch(&["current"], guard()).await.unwrap();
    assert_eq!(result.accepted, 1);
    assert!(!result.capacity_reached);
    newer.finish(guard()).await.unwrap();
    assert_eq!(body(&cache, &newer.base).await, b"current");
    remove(&cache, std::slice::from_ref(&newer.base)).await;
    drop(old);
    drop(newer);
    assert!(cache.drain_cleanup(Duration::from_secs(1)).await);
}

#[tokio::test]
#[ignore = "requires isolated actual Redis; delayed batch reply and cancelled leader cleanup"]
async fn cancelled_multi_put_holds_bytes_and_owner_until_physical_reply() {
    let cache = fixture(CacheConfig::default()).await;
    let entry = cache
        .open(
            &prefix("put-cancelled"),
            "put-cancelled-owner-aaaa",
            guard(),
        )
        .await
        .unwrap();
    let base = entry.base.clone();
    let permits = Arc::new(Semaphore::new(1));
    let request: Arc<dyn Send + Sync> = Arc::new(permits.clone().acquire_owned().await.unwrap());
    let maximum = cache.available_io_bytes();
    pause().await;
    let task = tokio::spawn(async move { entry.put_batch(&["first", "second"], request).await });
    tokio::time::timeout(Duration::from_millis(100), async {
        while cache.available_io_bytes() == maximum {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    assert!(cache.available_io_bytes() < maximum);
    assert_eq!(permits.available_permits(), 0);
    assert!(!cache.drain_cleanup(Duration::from_millis(10)).await);
    tokio::time::timeout(Duration::from_secs(2), async {
        while cache.available_io_bytes() != maximum || permits.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(cache.drain_cleanup(Duration::from_secs(2)).await);
    raw(&cache,"if redis.call('LLEN',KEYS[1]..':chunks')~=0 then return redis.error_reply('abandoned batch retained')end;return 1",std::slice::from_ref(&base),&[]).await;
    remove(&cache, &[base]).await;
}
