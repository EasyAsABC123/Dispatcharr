//! Owned ready-cache diagnostic; never a performance benchmark.
//! Original Dispatcharr authors retain full application credit.
use dispatcharr_output_store::transport::{RedisTransport, TransportConfig};
use dispatcharr_xmltv_http::cache::{Cache, CacheConfig, Cursor, Read};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    io::{self, Read as IoRead},
    sync::Arc,
};
#[tokio::main]
async fn main() {
    assert_eq!(
        std::env::var("XMLTV_CACHE_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let url = std::env::var("REDIS_TEST_URL").unwrap();
    assert!(url.contains("playback-fixture-only"));
    let mut input = String::new();
    io::stdin().take(65537).read_to_string(&mut input).unwrap();
    assert!(input.len() <= 65536);
    let request: Value = serde_json::from_str(&input).unwrap();
    let base = request["base"].as_str().unwrap();
    assert!(base.starts_with("epg_content:") && base.len() <= 8192);
    let transport = RedisTransport::connect(
        &url,
        TransportConfig {
            max_operations: 1,
            max_queued_operations: 1,
            command_timeout: std::time::Duration::from_secs(3),
        },
    )
    .await
    .unwrap();
    const SHAPE:&str="local n=redis.call('LLEN',KEYS[1]);if n>65536 then return redis.error_reply('fixture member bound')end;local total=0;local lo=2147483647;local hi=0;for first=0,n-1,64 do local rows=redis.call('LRANGE',KEYS[1],first,math.min(first+63,n-1));for _,data in ipairs(rows)do if #data>8388608 then return redis.error_reply('fixture member bytes')end;total=total+#data;if total>33554432 then return redis.error_reply('fixture export bytes')end;lo=math.min(lo,#data);hi=math.max(hi,#data)end end;return{n,total,lo,hi}";
    let (before, _): (Vec<i64>, _) = transport
        .command(SHAPE, &[format!("{base}:chunks")], &[], ())
        .await
        .unwrap();
    assert!(before[0] > 0);
    let mut results = Vec::new();
    for batch in [64 * 1024, 512 * 1024] {
        let cache = Cache::connect(
            &url,
            CacheConfig {
                read_batch_bytes: batch,
                ..CacheConfig::default()
            },
        )
        .await
        .unwrap();
        let mut cursor = Cursor::default();
        let mut hash = Sha256::new();
        let mut calls = 0usize;
        let mut bytes = 0usize;
        let mut largest = 0usize;
        loop {
            calls += 1;
            assert!(calls <= 131073);
            match cache
                .read(base, &mut cursor, false, Arc::new(()))
                .await
                .unwrap()
            {
                Read::Data(data) => {
                    let data = data.as_ref();
                    bytes += data.len();
                    largest = largest.max(data.len());
                    assert!(bytes <= 32 * 1024 * 1024 && data.len() <= batch);
                    hash.update(data);
                }
                Read::End => break,
                _ => panic!("actual ready cache required"),
            }
        }
        assert_eq!(
            cache.available_io_bytes(),
            CacheConfig::default().max_read_bytes
        );
        results.push(json!({"batch_bytes":batch,"read_script_invocations_including_eof":calls,"bytes":bytes,"sha256":format!("{:x}",hash.finalize()),"largest_reply_bytes":largest}));
    }
    let (after, _): (Vec<i64>, _) = transport
        .command(SHAPE, &[format!("{base}:chunks")], &[], ())
        .await
        .unwrap();
    assert_eq!(before, after);
    assert_eq!(results[0]["bytes"], results[1]["bytes"]);
    assert_eq!(results[0]["sha256"], results[1]["sha256"]);
    println!(
        "{}",
        json!({"scope":"Ready-cache functional diagnostic, not performance; same stored members under two read policies","passed":true,"stored_members":before[0],"stored_bytes":before[1],"smallest_member_bytes":before[2],"largest_member_bytes":before[3],"member_shape_unchanged":true,"policies":results})
    );
}
