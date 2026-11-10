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
        Read::Data(data) => assert_eq!(data.as_ref(), b"second"),
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
