//! Real Redis witnesses for original count semantics and cancelled admission.
//! Original Dispatcharr application authors retain credit.
use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
const KEYS: [&str; 3] = [
    "live:channel:xc-native-counter.p5:clients:owned",
    "timeshift:channel:xc-native-counter:clients:owned",
    "vod_persistent_connection:xc-native-counter-owned",
];
async fn fixture() -> ConnectionCounts {
    let url = std::env::var("REDIS_TEST_URL").expect("disposable real Redis fixture");
    assert!(url.contains("playback-fixture-only"));
    ConnectionCounts::connect(&url, |value| {
        dispatcharr_ingest_core::source_float_literal(value).is_some()
    })
    .await
    .unwrap()
}
async fn set(
    counts: &ConnectionCounts,
    kind: &str,
    namespace: usize,
    user: Option<Vec<u8>>,
    time: Option<Vec<u8>>,
) {
    let script = r#"redis.call('DEL',unpack(KEYS)); local k=KEYS[tonumber(ARGV[2])]; if ARGV[1]=='wrong-type' then redis.call('SET',k,'wrong-type') elseif ARGV[1]=='late' then redis.call('HSET',KEYS[1],'user_id','424242','connected_at','1');redis.call('HSET',KEYS[2],'user_id','invalid','connected_at','2') elseif ARGV[1]~='clear' then redis.call('HSET',k,'content_uuid','owned');if ARGV[3]=='1' then redis.call('HSET',k,'user_id',ARGV[4]) end;if ARGV[5]=='1' then redis.call('HSET',k,ARGV[7],ARGV[6]) end end; return 1"#;
    let namespace = (namespace + 1).to_string();
    let has_user = if user.is_some() { b"1" } else { b"0" };
    let has_time = if time.is_some() { b"1" } else { b"0" };
    let field = if namespace == "3" {
        b"created_at".as_slice()
    } else {
        b"connected_at".as_slice()
    };
    let args = [
        kind.as_bytes(),
        namespace.as_bytes(),
        has_user.as_slice(),
        user.as_deref().unwrap_or(&[]),
        has_time.as_slice(),
        time.as_deref().unwrap_or(&[]),
        field,
    ];
    let _: (i64, ()) = counts
        .transport
        .command(script, &KEYS.map(str::to_owned), &args, ())
        .await
        .unwrap();
}
fn raw(value: &serde_json::Value) -> Option<Vec<u8>> {
    if value.is_null() {
        None
    } else if let Some(value) = value.as_str() {
        Some(value.as_bytes().to_vec())
    } else {
        let hex = value["bytes_hex"].as_str().unwrap();
        Some(
            (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
                .collect(),
        )
    }
}
#[tokio::test]
#[ignore = "requires disposable original Redis"]
async fn original_connection_count_corpus() {
    let counts = fixture().await;
    let semaphore = Arc::new(Semaphore::new(1));
    let request = Arc::new(semaphore.acquire_owned().await.unwrap());
    let source: serde_json::Value =
        serde_json::from_str(include_str!("../datasets/source-counts.json")).unwrap();
    for row in source["cases"].as_array().unwrap() {
        let namespace = match row["namespace"].as_str() {
            Some("live") => 0,
            Some("timeshift") => 1,
            Some("vod") => 2,
            None => 0,
            _ => panic!("bad Source namespace"),
        };
        let kind = if row["case"] == "late-invalid-user-discards-early-row" {
            "late"
        } else if row["case"] == "wrong-type" {
            "wrong-type"
        } else {
            "hash"
        };
        set(
            &counts,
            kind,
            namespace,
            raw(&row["user"]),
            raw(&row["timestamp"]),
        )
        .await;
        let all = counts.count(None, request.clone()).await;
        let filtered = counts.count(Some(424242), request.clone()).await;
        // Always clean only owned keys before asserting, including failures.
        set(&counts, "clear", 0, None, None).await;
        assert_eq!(
            all.unwrap(),
            row["all_count"].as_u64().unwrap() as usize,
            "{} {}",
            row["namespace"],
            row["case"]
        );
        assert_eq!(
            filtered.unwrap(),
            row["filtered_count"].as_u64().unwrap() as usize,
            "{} {}",
            row["namespace"],
            row["case"]
        );
    }
}
async fn pause_redis() {
    let url = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    let socket =
        tokio::net::TcpStream::connect((url.host_str().unwrap(), url.port().unwrap_or(6379)))
            .await
            .unwrap();
    let mut socket = BufReader::new(socket);
    let password = url.password().unwrap();
    let auth = format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{password}\r\n", password.len());
    socket.get_mut().write_all(auth.as_bytes()).await.unwrap();
    let mut line = String::new();
    socket.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");
    socket
        .get_mut()
        .write_all(b"*3\r\n$6\r\nCLIENT\r\n$5\r\nPAUSE\r\n$3\r\n300\r\n")
        .await
        .unwrap();
    line.clear();
    socket.read_line(&mut line).await.unwrap();
    assert_eq!(line, "+OK\r\n");
}
#[tokio::test]
#[ignore = "requires disposable real Redis; pauses only that owned instance"]
async fn cancelled_reads_retain_request_and_reply_admission_until_reply() {
    let counts = fixture().await;
    let requests = Arc::new(Semaphore::new(8));
    pause_redis().await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let counts = counts.clone();
        let requests = requests.clone();
        tasks.push(tokio::spawn(async move {
            let request = Arc::new(requests.acquire_owned().await.unwrap());
            counts.count(None, request).await
        }));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    while counts.replies.available_permits() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "all bounded reads should reach paused Redis"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    assert_eq!(requests.available_permits(), 0);
    assert_eq!(counts.replies.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        while requests.available_permits() != 8 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counts.replies.available_permits(), 8 * 128 * 1024);
}

#[tokio::test]
#[ignore = "requires disposable real Redis"]
async fn classified_transport_preserves_server_backend_and_legacy_distinctions() {
    use dispatcharr_output_store::transport::ClassifiedError;
    let counts = fixture().await;
    set(&counts, "wrong-type", 0, None, None).await;
    let source = "return redis.call('HGET',KEYS[1],'value')";
    let key = [KEYS[0].to_owned()];
    let typed: Result<(String, ()), _> = counts
        .transport
        .command_classified(source, &key, &[], ())
        .await;
    let legacy: Result<(String, ()), _> = counts.transport.command(source, &key, &[], ()).await;
    set(&counts, "clear", 0, None, None).await;
    assert_eq!(typed.err(), Some(ClassifiedError::Server));
    assert_eq!(legacy.err(), Some(Error::Unavailable));
    let typed: Result<(String, ()), _> = counts
        .transport
        .command_classified("return string.char(255)", &[], &[], ())
        .await;
    assert_eq!(typed.err(), Some(ClassifiedError::Server));
    let typed: Result<(i64, ()), _> = counts
        .transport
        .command_classified(
            "return redis.error_reply('CORRUPT owned fixture')",
            &[],
            &[],
            (),
        )
        .await;
    let legacy: Result<(i64, ()), _> = counts
        .transport
        .command(
            "return redis.error_reply('CORRUPT owned fixture')",
            &[],
            &[],
            (),
        )
        .await;
    assert_eq!(typed.err(), Some(ClassifiedError::Server));
    assert_eq!(legacy.err(), Some(Error::Corrupt));
    let config = || TransportConfig {
        max_operations: 1,
        max_queued_operations: 1,
        command_timeout: Duration::from_millis(300),
    };
    let mut wrong = url::Url::parse(&std::env::var("REDIS_TEST_URL").unwrap()).unwrap();
    wrong
        .set_password(Some("deliberately-wrong-owned-fixture"))
        .unwrap();
    assert_eq!(
        RedisTransport::connect_classified(wrong.as_str(), config())
            .await
            .err(),
        Some(ClassifiedError::Server)
    );
    assert_eq!(
        RedisTransport::connect(wrong.as_str(), config())
            .await
            .err(),
        Some(Error::Unavailable)
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    assert_eq!(
        RedisTransport::connect_classified(&format!("redis://127.0.0.1:{port}/0"), config())
            .await
            .err(),
        Some(ClassifiedError::Backend)
    );
    assert_eq!(
        RedisTransport::connect_classified("invalid://", config())
            .await
            .err(),
        Some(ClassifiedError::Configuration)
    );
}

#[tokio::test]
#[ignore = "requires disposable real Redis; pauses only that owned instance"]
async fn classified_cancelled_command_keeps_physical_guard_until_reply() {
    let counts = fixture().await;
    let slots = Arc::new(Semaphore::new(1));
    let guard = slots.clone().acquire_owned().await.unwrap();
    pause_redis().await;
    let transport = counts.transport.clone();
    let caller = tokio::spawn(async move {
        transport
            .command_classified::<i64, _>("return 1", &[], &[], guard)
            .await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    caller.abort();
    let _ = caller.await;
    assert_eq!(slots.available_permits(), 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        while slots.available_permits() != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
