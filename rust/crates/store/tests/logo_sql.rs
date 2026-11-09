//! Original logo bytes/headers/mutations with actual PostgreSQL/DNS/HTTP/files.
//! Runs inside the disposable original container, never on operator data.
//! Original Dispatcharr contributors retain all application credit.
#[path = "../../app/src/response_headers.rs"]
mod app_response_headers;
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_store::{AuthConfig, AuthService, NotificationCache, Store};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, UNIX_EPOCH},
};
fn blob(spec: &Value) -> Vec<u8> {
    let mut out = STANDARD
        .decode(spec["prefix"].as_str().unwrap_or(""))
        .unwrap();
    out.extend(std::iter::repeat_n(
        spec["byte"].as_u64().unwrap_or(120) as u8,
        spec["repeat"].as_u64().unwrap_or(0) as usize,
    ));
    out
}
fn wire(spec: &Value) -> (Vec<u8>, String) {
    if spec.is_null() {
        return (vec![], "application/json".into());
    }
    if let Some(raw) = spec["raw"].as_str() {
        return (
            STANDARD.decode(raw).unwrap(),
            spec["content_type"]
                .as_str()
                .unwrap_or("application/json")
                .into(),
        );
    }
    if let Some(value) = spec.get("json") {
        return (
            serde_json::to_vec(value).unwrap(),
            "application/json".into(),
        );
    }
    if let Some(values) = spec["form"].as_object() {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (k, v) in values {
            serializer.append_pair(k, v.as_str().unwrap());
        }
        return (
            serializer.finish().into_bytes(),
            "application/x-www-form-urlencoded".into(),
        );
    }
    let mut out = vec![];
    for part in spec["multipart"].as_array().unwrap() {
        out.extend_from_slice(b"--rust-probe-boundary\r\n");
        let name = part["name"].as_str().unwrap();
        if let Some(filename) = part["filename"].as_str() {
            out.extend_from_slice(format!("Content-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: {}\r\n\r\n",part["mime"].as_str().unwrap_or("image/png")).as_bytes());
            out.extend(blob(part));
        } else {
            out.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"{name}\"\r\n\r\n{}",
                    part["value"].as_str().unwrap_or("")
                )
                .as_bytes(),
            );
        }
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"--rust-probe-boundary--\r\n");
    (
        out,
        "multipart/form-data; boundary=rust-probe-boundary".into(),
    )
}
fn prepare_files(fixture: &Value) {
    for value in fixture["owned_paths"].as_array().unwrap() {
        let path = Path::new(value.as_str().unwrap());
        assert!(
            path.to_str()
                .unwrap()
                .starts_with("/data/logos/rust-probe-")
                || path.to_str().unwrap().starts_with("/tmp/rust-probe-logo-")
        );
        if path.is_symlink() || path.is_file() {
            std::fs::remove_file(path).unwrap();
        } else if path.is_dir() {
            std::fs::remove_dir(path).unwrap();
        }
    }
    std::fs::create_dir_all("/data/logos").unwrap();
    for (path, body) in fixture["files"].as_object().unwrap() {
        std::fs::write(path, STANDARD.decode(body.as_str().unwrap()).unwrap()).unwrap();
        let modified = UNIX_EPOCH + Duration::from_millis(1_700_000_000_125);
        std::fs::File::open(path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
    }
    std::os::unix::fs::symlink(
        "/tmp/rust-probe-logo-outside.png",
        "/data/logos/rust-probe-logo-link.png",
    )
    .unwrap();
    std::fs::create_dir("/data/logos/rust-probe-logo-dir").unwrap();
}
fn files(fixture: &Value) -> Value {
    let mut result = serde_json::Map::new();
    for value in fixture["owned_paths"].as_array().unwrap() {
        let path = value.as_str().unwrap();
        let p = Path::new(path);
        let value = if p.is_symlink() {
            json!({"kind":"symlink","target":std::fs::read_link(p).unwrap().to_str().unwrap()})
        } else if p.is_file() {
            let body = std::fs::read(p).unwrap();
            json!({"kind":"file","size":body.len(),"sha256":format!("{:x}",Sha256::digest(body))})
        } else if p.is_dir() {
            json!({"kind":"directory"})
        } else {
            json!({"kind":"missing"})
        };
        result.insert(path.into(), value);
    }
    Value::Object(result)
}
async fn restore(store: &Store, case: &Value) {
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=NULL WHERE logo_id IS NOT NULL")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE dispatcharr_channels_channeloverride SET logo_id=NULL WHERE logo_id IS NOT NULL",
    )
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("DELETE FROM dispatcharr_channels_logo")
        .execute(&mut *tx)
        .await
        .unwrap();
    for logo in case["pre"]["logos"].as_array().unwrap() {
        sqlx::query("INSERT INTO dispatcharr_channels_logo(id,name,url) VALUES($1,$2,$3)")
            .bind(logo["id"].as_i64().unwrap())
            .bind(logo["name"].as_str().unwrap())
            .bind(logo["url"].as_str().unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    for (name, table) in [
        ("channels", "dispatcharr_channels_channel"),
        ("overrides", "dispatcharr_channels_channeloverride"),
    ] {
        for row in case["pre"][name].as_array().unwrap() {
            sqlx::query(&format!("UPDATE {table} SET logo_id=$2 WHERE id=$1"))
                .bind(row["id"].as_i64().unwrap())
                .bind(row["logo_id"].as_i64())
                .execute(&mut *tx)
                .await
                .unwrap();
        }
    }
    sqlx::query("SELECT setval('dispatcharr_channels_logo_id_seq',$1,true)")
        .bind(case["sequence"].as_i64().unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
async fn db(store: &Store, fixture: &Value) -> Value {
    let logos=sqlx::query_scalar::<_,Value>("SELECT json_build_object('id',id,'name',name,'url',url) FROM dispatcharr_channels_logo ORDER BY id").fetch_all(store.pool()).await.unwrap();
    let ids = fixture["channel_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect::<Vec<_>>();
    let channels=sqlx::query_scalar::<_,Value>("SELECT json_build_object('id',id,'logo_id',logo_id) FROM dispatcharr_channels_channel WHERE id=ANY($1) ORDER BY id").bind(ids).fetch_all(store.pool()).await.unwrap();
    let overrides=sqlx::query_scalar::<_,Value>("SELECT json_build_object('id',id,'logo_id',logo_id) FROM dispatcharr_channels_channeloverride WHERE id=$1").bind(fixture["override_id"].as_i64().unwrap()).fetch_all(store.pool()).await.unwrap();
    json!({"logos":logos,"channels":channels,"overrides":overrides})
}
fn normalize(mut value: Value, clock: bool) -> Value {
    if clock {
        if let Some(url) = value.get_mut("cache_url") {
            if let Some(raw) = url.as_str() {
                if let Some((path, _)) = raw.rsplit_once("?v=") {
                    *url = json!(format!("{path}?v=<mtime>"));
                }
            }
        }
    }
    value
}
#[tokio::test]
#[ignore = "requires LOGO_DISPOSABLE_FIXTURE inside original container with owned files/catalogs"]
async fn original_logo_bytes_roles_headers_crud_uploads_and_failure_boundaries() {
    assert_eq!(
        std::env::var("LOGO_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value =
        serde_json::from_slice(&std::fs::read(std::env::var("LOGO_FIXTURE").unwrap()).unwrap())
            .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let unowned: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM dispatcharr_channels_logo WHERE name NOT LIKE 'rust-probe-%'",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(unowned, 0);
    let redis_url = std::env::var("REDIS_TEST_URL").unwrap();
    let cache = NotificationCache::connect(&redis_url).await.unwrap();
    let mut redis = redis::Client::open(redis_url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(45))
        .build()
        .unwrap();
    let mut failures = Vec::new();
    macro_rules! same {($a:expr,$b:expr,$($message:tt)*)=>{{let actual=&$a;let expected=&$b;if actual!=expected{let label=format!($($message)*);eprintln!("{label}: actual={actual:?} expected={expected:?}");failures.push(label);}}};}
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        prepare_files(&fixture);
        restore(&store, case).await;
        let _: i64 = redis::cmd("DEL")
            .arg(":1:coresettings:default_user_agent")
            .query_async(&mut redis)
            .await
            .unwrap();
        if let Some(agent) = case["cached_agent"].as_str() {
            let _: () = redis::cmd("SET")
                .arg(":1:coresettings:default_user_agent")
                .arg(dispatcharr_store::encode_source_cache(&json!(agent)).unwrap())
                .arg("EX")
                .arg(300)
                .query_async(&mut redis)
                .await
                .unwrap();
        }
        client.get("http://origin:8080/reset").send().await.unwrap();
        let auth = AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(cache.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = auth
            .router()
            .layer(axum::middleware::from_fn(app_response_headers::apply))
            .layer(axum::Extension(axum::extract::ConnectInfo(
                "127.0.0.1:12345".parse::<std::net::SocketAddr>().unwrap(),
            )));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let (body, mime) = wire(&case["body"]);
        same!(
            format!("{:x}", Sha256::digest(&body)),
            case["request_body_sha256"].as_str().unwrap(),
            "case {index} request bytes"
        );
        for expected in case["expected"]["responses"].as_array().unwrap() {
            let mut request = client
                .request(
                    case["method"]
                        .as_str()
                        .unwrap()
                        .parse::<reqwest::Method>()
                        .unwrap(),
                    format!("http://{address}{}", case["path"].as_str().unwrap()),
                )
                .header("content-type", &mime)
                .body(body.clone());
            for (key, value) in case["headers"].as_object().unwrap() {
                request = request.header(
                    key.strip_prefix("HTTP_").unwrap().replace('_', "-"),
                    value.as_str().unwrap(),
                );
            }
            let response = request.send().await.unwrap();
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .filter(|(key, _)| {
                    [
                        "content-type",
                        "content-length",
                        "cache-control",
                        "last-modified",
                        "content-disposition",
                        "x-content-type-options",
                        "content-security-policy",
                        "www-authenticate",
                    ]
                    .contains(&key.as_str())
                })
                .map(|(key, value)| {
                    (
                        key.as_str().to_owned(),
                        value
                            .as_bytes()
                            .iter()
                            .map(|byte| char::from(*byte))
                            .collect::<String>(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let body = response.bytes().await.unwrap();
            same!(
                status,
                expected["status"].as_u64().unwrap() as u16,
                "case {index} {}",
                case["name"]
            );
            same!(
                json!(headers),
                expected["headers"],
                "case {index} {} headers",
                case["name"]
            );
            if let Some(expected) = expected.get("json") {
                same!(
                    normalize(
                        serde_json::from_slice(&body).unwrap(),
                        case["clock_hash"] == true
                    ),
                    normalize(expected.clone(), case["clock_hash"] == true),
                    "case {index} {} JSON",
                    case["name"]
                );
            } else {
                same!(
                    body.len(),
                    expected["body_length"].as_u64().unwrap() as usize,
                    "case {index} {} length",
                    case["name"]
                );
                same!(
                    format!("{:x}", Sha256::digest(body)),
                    expected["body_sha256"].as_str().unwrap(),
                    "case {index} {} bytes",
                    case["name"]
                );
            }
        }
        same!(
            db(&store, &fixture).await,
            case["expected"]["db"],
            "case {index} {} DB",
            case["name"]
        );
        same!(
            files(&fixture),
            case["expected"]["files"],
            "case {index} {} files",
            case["name"]
        );
        let actual_bytes = client
            .get("http://origin:8080/counts")
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let actual: Value = serde_json::from_slice(&actual_bytes).unwrap();
        same!(
            actual,
            case["expected"]["origin_calls"],
            "case {index} {} upstream attempts/negative cache",
            case["name"]
        );
        server.abort();
    }

    if failures.is_empty() {
        use futures_util::StreamExt;
        prepare_files(&fixture);
        let auth = AuthService::new(
            store.clone(),
            AuthConfig::new("playback-fixture-only".into()).unwrap(),
        )
        .with_notification_cache(cache.clone());
        let response = auth
            .image_response("http://origin:8080/limit/exact")
            .await
            .unwrap();
        let mut stream = response.into_body().into_data_stream();
        let frame = stream.next().await.unwrap().unwrap();
        drop(stream);
        assert_eq!(frame.len(), 5 * 1024 * 1024);
        assert_eq!(auth.image_resources()["admitted"], 1);
        assert_eq!(
            auth.image_resources()["retained_response_bytes"],
            5 * 1024 * 1024
        );
        let clone = frame.clone();
        drop(frame);
        assert_eq!(auth.image_resources()["admitted"], 1);
        drop(clone);
        assert_eq!(auth.image_resources()["admitted"], 0);
        assert_eq!(auth.image_resources()["retained_response_bytes"], 0);
        sqlx::query("UPDATE dispatcharr_channels_logo SET url=$2 WHERE id=$1")
            .bind(1i64)
            .bind("http://origin:8080/headers-stall?resource=1")
            .execute(store.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE dispatcharr_channels_logo SET url=$2 WHERE id=$1")
            .bind(2i64)
            .bind("/data/logos/rust-probe-logo.png")
            .execute(store.pool())
            .await
            .unwrap();
        client.get("http://origin:8080/reset").send().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = auth
            .clone()
            .router()
            .layer(axum::Extension(axum::extract::ConnectInfo(
                "127.0.0.1:12345".parse::<std::net::SocketAddr>().unwrap(),
            )));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .get(format!("http://{address}/api/channels/logos/1/cache/"))
                    .send()
                    .await
            }));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while auth.image_resources()["admitted"] != 4 {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for _ in 0..64 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .get(format!("http://{address}/api/channels/logos/2/cache/"))
                    .send()
                    .await
            }));
        }
        while auth.image_resources()["waiting"] != 64 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "bounded waiters did not fill"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let overloaded = client
            .get(format!("http://{address}/api/channels/logos/2/cache/"))
            .send()
            .await
            .unwrap();
        assert_eq!(overloaded.status(), 503);
        let counts: Value = serde_json::from_slice(
            &client
                .get("http://origin:8080/counts")
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(counts["/headers-stall"], 4);
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
        let drained = tokio::time::Instant::now() + Duration::from_secs(7);
        while auth.image_resources()["admitted"] != 0 || auth.image_resources()["waiting"] != 0 {
            assert!(
                tokio::time::Instant::now() < drained,
                "canceled HTTP image work did not drain"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let recovered = client
            .get(format!("http://{address}/api/channels/logos/2/cache/"))
            .send()
            .await
            .unwrap();
        assert_eq!(recovered.status(), 200);
        drop(recovered);
        server.abort();
        println!("NATIVE_LOGO_RESOURCES held_5MiB_frame_clones=pass admitted4_waiting64=pass overload503_no_extra_origin=pass canceled_work_drains_and_recovers=pass");
    }
    assert!(
        failures.is_empty(),
        "{} source mismatches: {}",
        failures.len(),
        failures.join("; ")
    );
}
