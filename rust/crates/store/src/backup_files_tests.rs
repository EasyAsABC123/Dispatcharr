//! Actual original backup HTTP/filesystem proof, no jobs or production restore.
use super::*;
use std::path::Path;
#[path = "../../app/src/response_headers.rs"]
mod response_headers;
#[test]
fn signed_token_rfc_and_original_vector() {
    assert_eq!(
        mac(&[0x0b; 20], "Hi There"),
        "b0344c61d8db38535ca8afceaf0bf12b"
    );
    assert_eq!(
        mac(b"playback-fixture-only", "fixture-directory"),
        "f79a4fcf4136478631ea8ba5f343bdc3"
    );
}
fn bytes(raw: &str) -> Vec<u8> {
    raw.as_bytes()
        .chunks_exact(2)
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn reset(fixture: &Value) {
    let root = Path::new("/data/backups");
    std::fs::create_dir_all(root).unwrap();
    for file in std::fs::read_dir(root).unwrap() {
        let file = file.unwrap().path();
        if file.is_dir() {
            std::fs::remove_dir(file).unwrap()
        } else {
            std::fs::remove_file(file).unwrap()
        }
    }
    for item in fixture["seed_files"].as_array().unwrap() {
        let path = root.join(item["filename"].as_str().unwrap());
        std::fs::write(&path, bytes(item["body_hex"].as_str().unwrap())).unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        let time = std::time::UNIX_EPOCH + Duration::from_secs(item["mtime"].as_u64().unwrap());
        file.set_times(
            std::fs::FileTimes::new()
                .set_modified(time)
                .set_accessed(time),
        )
        .unwrap()
    }
    for dir in fixture["seed_directories"].as_array().unwrap() {
        std::fs::create_dir(root.join(dir.as_str().unwrap())).unwrap()
    }
}
fn snapshot() -> Value {
    let mut result = serde_json::Map::new();
    for item in std::fs::read_dir("/data/backups").unwrap() {
        let item = item.unwrap();
        let meta = item.metadata().unwrap();
        result.insert(
            item.file_name().into_string().unwrap(),
            if meta.is_dir() {
                json!({"directory":true})
            } else {
                json!({"size":meta.len(),"sha256":format!("{:x}",file_digest(&item.path()))})
            },
        );
    }
    json!(result)
}
#[tokio::test]
#[ignore = "Requires guarded original backup HTTP fixture and real PostgreSQL/Redis; no jobs"]
async fn original_job_free_backup_file_contracts() {
    assert_eq!(
        std::env::var("BACKUP_API_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("BACKUP_API_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = crate::Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 8)
        .await
        .unwrap();
    let auth = AuthService::new(
        store,
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    );
    let service = BackupFiles::new(
        auth,
        PathBuf::from("/data/backups"),
        "playback-fixture-only".into(),
    )
    .unwrap();
    let router = service
        .clone()
        .router()
        .layer(axum::middleware::from_fn(response_headers::apply));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut checks = vec![];
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        reset(&fixture);
        assert_eq!(snapshot(), case["pre_files"]);
        let mut request = client.request(
            case["method"]
                .as_str()
                .unwrap()
                .parse::<reqwest::Method>()
                .unwrap(),
            format!("http://{address}{}", case["path"].as_str().unwrap()),
        );
        for (key, value) in case["headers"].as_object().unwrap() {
            request = request.header(key, value.as_str().unwrap())
        }
        let response = request
            .body(bytes(case["request_body_hex"].as_str().unwrap()))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let mut headers = serde_json::Map::new();
        for key in [
            "content-type",
            "content-length",
            "content-disposition",
            "www-authenticate",
            "x-accel-redirect",
            "x-content-type-options",
        ] {
            if let Some(value) = response.headers().get(key) {
                headers.insert(
                    key.into(),
                    json!(dispatcharr_contracts::source_header_value(value.as_bytes())),
                );
            }
        }
        let body = response.bytes().await.unwrap();
        let mut actual = json!({"status":status,"headers":headers});
        if case["method"] != "HEAD"
            && !body.is_empty()
            && actual["headers"]["content-type"]
                .as_str()
                .unwrap_or("")
                .contains("json")
        {
            actual["json"] = serde_json::from_slice::<Value>(&body).unwrap()
        } else {
            actual["body_length"] = json!(body.len());
            actual["body_sha256"] = json!(format!("{:x}", Sha256::digest(&body)));
        }
        let mut failures = vec![];
        if serde_json::to_string(&actual).unwrap()
            != serde_json::to_string(&case["expected"]).unwrap()
        {
            // Object insertion order is not a wire contract; numeric types remain exact.
            let canonical = |v: &Value| serde_json::to_string(&sort(v)).unwrap();
            if canonical(&actual) != canonical(&case["expected"]) {
                failures.push("response")
            }
        }
        if snapshot() != case["post_files"] {
            failures.push("filesystem")
        }
        let declared_names = [
            "delete-admin",
            "delete-name-dispatcharr-backup-rust-probe-a.zip",
            "delete-name-arbitrary-upload.txt",
            "delete-name-café.zip",
        ];
        let declared_delta = if declared_names.contains(&case["name"].as_str().unwrap())
            && failures == ["response"]
            && actual["status"] == 204
            && case["expected"]["status"] == 204
            && actual["body_length"] == 0
            && case["expected"]["body_length"] == 0
            && actual["headers"].get("content-length").is_none()
            && case["expected"]["headers"]["content-length"] == "40"
        {
            let mut expected = case["expected"].clone();
            expected["headers"]
                .as_object_mut()
                .unwrap()
                .remove("content-length");
            sort(&actual) == sort(&expected)
        } else {
            false
        };
        checks.push(json!({"case":index,"name":case["name"],"passed":failures.is_empty(),"secondary_passed":failures.is_empty()||declared_delta,"declared_source_invalid_204_length_delta":declared_delta,"mismatches":failures,"native_status":status,"source_status":case["expected"]["status"]}));
    }
    for case in fixture["large_uploads"].as_array().unwrap() {
        reset(&fixture);
        assert_eq!(snapshot(), case["pre_files"]);
        let segments = case["segments"].as_array().unwrap().clone();
        let stream = async_stream::stream! {for segment in segments{if let Some(raw)=segment.get("body_hex"){yield Ok::<Bytes,std::io::Error>(Bytes::from(bytes(raw.as_str().unwrap())))}else{let pattern=bytes(segment["repeat_hex"].as_str().unwrap());let length=segment["length"].as_u64().unwrap();let mut offset=0;while offset<length{let count=(length-offset).min(65536)as usize;let block=(0..count).map(|index|pattern[(offset as usize+index)%pattern.len()]).collect::<Vec<_>>();offset+=count as u64;yield Ok::<Bytes,std::io::Error>(Bytes::from(block));}}}};
        let mut request = client.post(format!(
            "http://{address}{}",
            case["path"].as_str().unwrap()
        ));
        for (key, value) in case["headers"].as_object().unwrap() {
            request = request.header(key, value.as_str().unwrap())
        }
        let response = request
            .body(reqwest::Body::wrap_stream(stream))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let mut headers = serde_json::Map::new();
        for key in [
            "content-type",
            "content-length",
            "content-disposition",
            "www-authenticate",
            "x-accel-redirect",
            "x-content-type-options",
        ] {
            if let Some(value) = response.headers().get(key) {
                headers.insert(
                    key.into(),
                    json!(dispatcharr_contracts::source_header_value(value.as_bytes())),
                );
            }
        }
        let body = response.bytes().await.unwrap();
        let actual = json!({"status":status,"headers":headers,"json":serde_json::from_slice::<Value>(&body).unwrap()});
        let mut failures = vec![];
        if sort(&actual) != sort(&case["expected"]) {
            failures.push("response")
        }
        if snapshot() != case["post_files"] {
            failures.push("filesystem")
        }
        let passed = failures.is_empty();
        checks.push(json!({"name":case["name"],"passed":passed,"secondary_passed":passed,"declared_source_invalid_204_length_delta":false,"mismatches":failures,"native_status":status,"source_status":case["expected"]["status"],"streamed_request_bytes":case["body_length"]}));
    }
    // Dropped multipart observer must not publish a partial file or leak its spool.
    let temporary_names = || {
        std::fs::read_dir(std::env::temp_dir())
            .unwrap()
            .filter_map(Result::ok)
            .map(|v| v.path())
            .filter(|v| {
                v.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("dispatcharr-backup-upload-")
            })
            .collect::<std::collections::BTreeSet<_>>()
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while service.workers.available_permits() != 4 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let before_spools = temporary_names();
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    let mut partial = socket.connect(address).await.unwrap();
    let auth = fixture["cases"][0]["headers"]["Authorization"]
        .as_str()
        .unwrap();
    let header=format!("POST /api/backups/upload/ HTTP/1.1\r\nHost: localhost\r\nAuthorization: {auth}\r\nContent-Type: multipart/form-data; boundary=held-boundary\r\nContent-Length: 33554432\r\n\r\n--held-boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"held-upload.zip\"\r\n\r\n");
    partial.write_all(header.as_bytes()).await.unwrap();
    partial.write_all(&vec![b'x'; 131072]).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while temporary_names() == before_spools {
        assert!(
            tokio::time::Instant::now() < deadline,
            "No physical upload spool observed"
        );
        tokio::time::sleep(Duration::from_millis(5)).await
    }
    drop(partial);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while temporary_names() != before_spools || service.workers.available_permits() != 4 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "Canceled upload leaked spool/ownership"
        );
        tokio::time::sleep(Duration::from_millis(5)).await
    }
    assert!(!Path::new("/data/backups/held-upload.zip").exists());
    // Actual unread response retains admission; body drop releases it.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let held = Path::new("/data/backups/held-fixture-download.zip");
    std::fs::File::create(held)
        .unwrap()
        .set_len(32 << 20)
        .unwrap();
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut stream = socket.connect(address).await.unwrap();
    let token = mac(b"playback-fixture-only", "held-fixture-download.zip");
    stream.write_all(format!("GET /api/backups/held-fixture-download.zip/download/?token={token} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut prefix = [0u8; 128];
    let count = stream.read(&mut prefix).await.unwrap();
    assert!(prefix[..count].starts_with(b"HTTP/1.1 200"));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        service.workers.available_permits(),
        3,
        "Unread file body must retain an owned lease"
    );
    drop(stream);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while service.workers.available_permits() != 4 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await
    }
    std::fs::remove_file(held).unwrap();
    let lease = Arc::new(service.workers.clone().try_acquire_owned().unwrap());
    let state = service.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let observer = tokio::spawn(async move {
        state
            .io(&lease, move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap()
            })
            .await
    });
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();
    observer.abort();
    let _ = observer.await;
    assert_eq!(
        service.workers.available_permits(),
        3,
        "Canceled observer released physical worker prematurely"
    );
    let mut held_leases = vec![];
    for _ in 0..3 {
        held_leases.push(service.workers.clone().try_acquire_owned().unwrap())
    }
    let response = client
        .get(format!("http://{address}/api/backups/"))
        .header(
            "Authorization",
            fixture["cases"][0]["headers"]["Authorization"]
                .as_str()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    let response = client
        .get(format!("http://{address}/api/backups/schedule/"))
        .header(
            "Authorization",
            fixture["cases"][0]["headers"]["Authorization"]
                .as_str()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        200,
        "Schedule-only SQL read must not wait for unrelated filesystem admission"
    );
    drop(held_leases);
    assert!(
        !service.shutdown(Duration::from_millis(40)).await,
        "Shutdown falsely completed with a held physical worker"
    );
    release_tx.send(()).unwrap();
    assert!(service.shutdown(Duration::from_secs(2)).await);
    eprintln!("BACKUP_API_RESOURCES {{\"unread_download_retains_lease\":true,\"drop_releases_download\":true,\"canceled_observer_retains_physical_worker\":true,\"capacity_503\":true,\"shutdown_waits_physical_reply\":true,\"canceled_upload_spool_cleanup\":true,\"schedule_read_independent_of_filesystem_capacity\":true}}");
    server.abort();
    let _ = server.await;
    assert!(service.shutdown(Duration::from_secs(2)).await);
    let passed = checks.iter().all(|v| v["passed"] == true);
    let secondary = checks.iter().all(|v| v["secondary_passed"] == true);
    let deltas = checks
        .iter()
        .filter(|v| v["declared_source_invalid_204_length_delta"] == true)
        .count();
    assert_eq!(deltas, 4);
    eprintln!(
        "BACKUP_API_CHECKS {}",
        json!({"passed":passed,"secondary_passed":secondary,"declared_source_invalid_204_length_deltas":deltas,"checks":checks})
    );
    assert!(
        secondary,
        "Original/native backup file contract has an undeclared mismatch; raw four protocol deltas are retained"
    );
}
fn sort(value: &Value) -> Value {
    match value {
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(k, v)| (k.clone(), sort(v)))
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => json!(values.iter().map(sort).collect::<Vec<_>>()),
        value => value.clone(),
    }
}

fn file_digest(path: &Path) -> sha2::digest::Output<Sha256> {
    let mut digest = Sha256::new();
    let mut file = std::fs::File::open(path).unwrap();
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count])
    }
    digest.finalize()
}

#[test]
fn private_spool_cleanup_outside_runtime_retains_lease_until_unlink() {
    let path = std::env::temp_dir().join(format!(
        "dispatcharr-backup-upload-test-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::write(&path, b"owned fixture").unwrap();
    let permits = Arc::new(Semaphore::new(1));
    let lease = Arc::new(permits.clone().try_acquire_owned().unwrap());
    let temp = TemporaryUpload {
        path: path.clone(),
        lease,
    };
    assert_eq!(permits.available_permits(), 0);
    drop(temp);
    assert!(!path.exists());
    assert_eq!(permits.available_permits(), 1);
}

#[test]
fn filename_decoding_keeps_query_delimiters_literal_and_decodes_once() {
    assert_eq!(decode_filename("amp&plus+=.zip"), "amp&plus+=.zip");
    assert_eq!(decode_filename("amp%26plus%2B%3D.zip"), "amp&plus+=.zip");
    assert_eq!(decode_filename("literal%2526.zip"), "literal%26.zip");
}
