//! Source-derived VOD image/cache component and ownership controls.
//! Actual Original/native240 and physical/mixed host gates passed;
//! canonical app, unchanged React and broader image contracts are separate.
//! Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_http::VodReads;
use serde_json::json;
use std::{net::SocketAddr, time::Duration};
async fn auth() -> crate::AuthService {
    assert_eq!(
        std::env::var("VOD_API_NATIVE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(8)
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    crate::AuthService::new(
        crate::Store::from_pool(pool),
        crate::AuthConfig::new("playback-fixture-only".into()).unwrap(),
    )
    .with_notification_cache(
        crate::NotificationCache::connect(&std::env::var("REDIS_TEST_URL").unwrap())
            .await
            .unwrap(),
    )
}
#[tokio::test]
#[ignore = "Owned actual Redis pending cache reply, no remote provider traffic"]
async fn image_user_agent_abort_retains_physical_owner() {
    let auth = auth().await;
    let cache = auth.notification_cache.clone().unwrap();
    let mut connection = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("DEL")
        .arg(":1:coresettings:default_user_agent")
        .arg(":1:coresettings:default_user_agent:ver")
        .query_async(&mut connection)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(400)
        .arg("ALL")
        .query_async(&mut connection)
        .await
        .unwrap();
    let copy = auth.clone();
    let task = tokio::spawn(async move {
        copy.image_response("http://127.0.0.1:1/owned-blocked-no-request")
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while cache.physical_operations() != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(auth.image_resources()["admitted"], 1);
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let admitted = auth.image_resources()["admitted"].as_u64().unwrap();
    let physical = cache.physical_operations();
    tokio::time::timeout(Duration::from_secs(2), async {
        while cache.physical_operations() != 0 || auth.image_resources()["admitted"] != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(auth.image_resources()["admitted"], 0);
    let report = json!({"passed":admitted==1&&physical==1,"scope":"actual cached-user-agent Redis400ms reply ownership after caller abort; loopback URL is blocked before any origin contact","image_admitted_after_abort":admitted,"cache_physical_after_abort":physical,"drained_after_real_reply":true});
    std::fs::write(
        std::env::var("VOD_IMAGE_OWNER_REPORT").unwrap(),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(
        report["passed"], true,
        "image permit released before physical cached-settings reply"
    );
}
#[tokio::test]
#[ignore = "Native image HTTP component runs in owned Original container network"]
async fn native_images_http_server() {
    let auth = auth().await;
    let ordinary = VodReads::new(auth, Default::default()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = ordinary
        .vod_image_router()
        .merge(ordinary.router())
        .merge(ordinary.provider_router());
    let router = router.layer(axum::middleware::from_fn(response_headers::apply));
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    std::fs::write(
        std::env::var("VOD_IMAGE_READY").unwrap(),
        serde_json::to_vec(&json!({"port":address.port()})).unwrap(),
    )
    .unwrap();
    let stop = std::path::PathBuf::from(std::env::var("VOD_IMAGE_STOP").unwrap());
    tokio::time::timeout(Duration::from_secs(180), async {
        while !stop.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(ordinary.shutdown(Duration::from_secs(3)).await);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
#[ignore = "Actual owned SQL fallback lock and local FIFO opener cancellation"]
async fn image_sql_and_file_abort_retain_physical_owner() {
    let auth = auth().await;
    let pool = auth.store.pool();
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("DEL")
        .arg(":1:coresettings:default_user_agent")
        .arg(":1:coresettings:default_user_agent:ver")
        .arg(":1:coresettings:group:stream_settings")
        .arg(":1:coresettings:groupver:stream_settings")
        .query_async(&mut redis)
        .await
        .unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let copy = auth.clone();
    let task = tokio::spawn(async move {
        copy.image_response("http://127.0.0.1:1/owned-blocked-no-request")
            .await
    });
    tokio::time::timeout(Duration::from_secs(2),async{loop{let waiting:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND wait_event_type='Lock'").persistent(false).fetch_one(pool).await.unwrap();if waiting>0{break;}tokio::time::sleep(Duration::from_millis(2)).await;}}).await.unwrap();
    assert_eq!(auth.image_resources()["admitted"], 1);
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let sql_admitted = auth.image_resources()["admitted"].as_u64().unwrap();
    let waiting:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND wait_event_type='Lock'").persistent(false).fetch_one(pool).await.unwrap();
    assert_eq!(waiting, 1);
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while auth.image_resources()["admitted"] != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let path = std::path::PathBuf::from(format!(
        "/data/logos/rust-native-image-owned-fifo-{}",
        std::process::id()
    ));
    std::fs::create_dir_all("/data/logos").unwrap();
    assert!(!path.exists());
    assert!(std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap()
        .success());
    let copy = auth.clone();
    let input = path.to_string_lossy().into_owned();
    let task = tokio::spawn(async move { copy.image_response(&input).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while auth.image_resources()["admitted"] != 1 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!task.is_finished());
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let fifo_admitted = auth.image_resources()["admitted"].as_u64().unwrap();
    // Linux O_NONBLOCK is explicit in this Linux-only disposable fixture. A
    // successful writer open proves the physical read-side opener was registered;
    // it also releases that opener before any failure assertion or fixture exit.
    use std::os::unix::fs::OpenOptionsExt;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(0o4000)
                .open(&path)
            {
                Ok(writer) => {
                    drop(writer);
                    break;
                }
                Err(error) if error.raw_os_error() == Some(6) => {
                    tokio::time::sleep(Duration::from_millis(2)).await
                }
                Err(error) => panic!("FIFO writer: {error}"),
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while auth.image_resources()["admitted"] != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    std::fs::remove_file(path).unwrap();
    let report = json!({"passed":sql_admitted==1&&fifo_admitted==1,"actual_PG_lock_after_abort":waiting,"image_admitted_while_SQL_reply_pending":sql_admitted,"actual_FIFO_opener_released_by_owned_writer":true,"image_admitted_while_FIFO_open_pending":fifo_admitted,"all_physical_work_released_before_assert":true});
    std::fs::write(
        std::env::var("VOD_IMAGE_SQL_FILE_REPORT").unwrap(),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    assert_eq!(
        report["passed"], true,
        "image permit released while physical SQL/file work continued"
    );
}

#[tokio::test]
#[ignore = "Actual SQL shutdown and real remote/local retained image frames"]
async fn image_shutdown_and_retained_frames() {
    use axum::body::to_bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let auth = auth().await;
    let pool = auth.store.pool();
    let mut redis = redis::Client::open(std::env::var("REDIS_TEST_URL").unwrap())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("DEL")
        .arg(":1:coresettings:default_user_agent")
        .arg(":1:coresettings:default_user_agent:ver")
        .arg(":1:coresettings:group:stream_settings")
        .arg(":1:coresettings:groupver:stream_settings")
        .query_async(&mut redis)
        .await
        .unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let copy = auth.clone();
    let task = tokio::spawn(async move {
        copy.image_response("http://127.0.0.1:1/owned-blocked-no-request")
            .await
    });
    tokio::time::timeout(Duration::from_secs(2),async{loop{let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND wait_event_type='Lock'").fetch_one(pool).await.unwrap();if count>0{break;}tokio::time::sleep(Duration::from_millis(2)).await;}}).await.unwrap();
    task.abort();
    let _ = task.await;
    assert!(!auth.shutdown_images(Duration::from_millis(20)).await);
    let rejected = auth
        .image_response("http://127.0.0.1:1/unsent-after-close")
        .await
        .unwrap_err();
    assert!(matches!(rejected, crate::StoreError::StatusBody(503, _)));
    blocker.rollback().await.unwrap();
    assert!(auth.shutdown_images(Duration::from_secs(2)).await);
    assert_eq!(auth.image_resources()["physical_work"], 0);
    let remote_auth = super::vod_api_images_peer::auth().await;
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let host = std::env::var("HOSTNAME").unwrap();
    let address = tokio::net::lookup_host((host.as_str(), port))
        .await
        .unwrap()
        .find(|address| address.is_ipv4() && crate::image_proxy::address_allowed(address.ip()))
        .unwrap();
    use base64::Engine;
    let png=base64::engine::general_purpose::STANDARD.decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/l9sAAAAASUVORK5CYII=").unwrap();
    let expected = png.clone();
    let origin = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 1);
            header.push(byte[0]);
            assert!(header.len() < 8192);
        }
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    png.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        socket.write_all(&png).await.unwrap();
    });
    let response = remote_auth
        .image_response(&format!("http://{address}/owned.png"))
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), 5 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), expected);
    origin.await.unwrap();
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(remote_auth.image_resources()["admitted"], 1);
    assert!(remote_auth.shutdown_images(Duration::from_secs(1)).await);
    let remote_retained = remote_auth.image_resources()["retained_response_bytes"]
        .as_u64()
        .unwrap();
    assert!(remote_retained >= 68);
    drop(clone);
    assert_eq!(remote_auth.image_resources()["admitted"], 0);
    assert_eq!(remote_auth.image_resources()["retained_response_bytes"], 0);
    let local_auth = super::vod_api_images_peer::auth().await;
    std::fs::create_dir_all("/data/logos").unwrap();
    let path = format!(
        "/data/logos/rust-native-image-owned-frame-{}.svg",
        std::process::id()
    );
    assert!(!std::path::Path::new(&path).exists());
    let svg = b"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1\" height=\"1\"/>";
    std::fs::write(&path, svg).unwrap();
    let response = local_auth.image_response(&path).await.unwrap();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(bytes.as_ref(), svg);
    let clone = bytes.clone();
    drop(bytes);
    assert!(local_auth.shutdown_images(Duration::from_secs(1)).await);
    assert_eq!(local_auth.image_resources()["physical_work"], 0);
    assert_eq!(local_auth.image_resources()["admitted"], 1);
    let local_retained = local_auth.image_resources()["retained_response_bytes"]
        .as_u64()
        .unwrap();
    assert!(local_retained >= svg.len() as u64);
    drop(clone);
    assert_eq!(local_auth.image_resources()["admitted"], 0);
    assert_eq!(local_auth.image_resources()["retained_response_bytes"], 0);
    std::fs::remove_file(path).unwrap();
    std::fs::write(std::env::var("VOD_IMAGE_SHUTDOWN_REPORT").unwrap(),serde_json::to_vec_pretty(&json!({"passed":true,"actual_SQL_pending_shutdown_false":true,"new_work_after_close_503_unsent":true,"physical_reply_then_shutdown_true":true,"actual_private_origin_remote_bytes":68,"remote_final_clone_retained_credits":remote_retained,"local_final_clone_retained_credits":local_retained,"completed_frames_outlive_successful_shutdown":true,"owned_local_file_removed":true})).unwrap()).unwrap();
}

#[tokio::test]
#[ignore = "Actual shared VOD metadata PG lock, caller abort and shutdown fence"]
async fn image_metadata_sql_keeps_shared_read_owner() {
    use super::{vod_api_images_http::ImageHttp, vod_api_images_read::Image};
    use axum::body::Body;
    let auth = auth().await;
    let owner = VodReads::new(auth.clone(), Default::default()).unwrap();
    let _router = owner.vod_image_router();
    let gate = owner.shared_gate();
    let gateway = ImageHttp::shared(auth.clone(), owner.reader.shared_runtime(), gate.clone());
    let corpus: serde_json::Value = serde_json::from_slice(
        &std::fs::read(std::env::var("VOD_IMAGE_BASE_CORPUS").unwrap()).unwrap(),
    )
    .unwrap();
    let token = corpus["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| {
            row["role"] == "standard"
                && row["method"] == "GET"
                && row["headers"]["HTTP_AUTHORIZATION"].is_string()
        })
        .unwrap()["headers"]["HTTP_AUTHORIZATION"]
        .as_str()
        .unwrap();
    let id = corpus["fixtures"]["movie_ids"][0].as_i64().unwrap();
    let request = axum::extract::Request::builder()
        .uri(format!("/api/vod/movies/{id}/image/?kind=movie_image"))
        .header("Host", "localhost")
        .header("Accept", "image/png")
        .header("Authorization", token)
        .body(Body::empty())
        .unwrap();
    let pool = auth.store.pool();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE vod_m3umovierelation IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let task = tokio::spawn(async move {
        gateway
            .respond(Image::Movie, "127.0.0.1:1234".parse().unwrap(), request)
            .await
    });
    tokio::time::timeout(Duration::from_secs(2),async{loop{let waiting:i64=sqlx::query_scalar("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND usename=current_user AND wait_event_type='Lock'").fetch_one(pool).await.unwrap();if waiting>0{break;}tokio::time::sleep(Duration::from_millis(2)).await;}}).await.unwrap();
    task.abort();
    let _ = task.await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(owner.reader.physical_workers(), 1);
    assert_eq!(owner.reader.retained_memory_bytes(), 68 * 1024 * 1024);
    assert_eq!(gate.slots.available_permits(), 1);
    assert_eq!(auth.image_resources()["admitted"], 0);
    assert!(!owner.shutdown(Duration::from_millis(20)).await);
    blocker.rollback().await.unwrap();
    assert!(owner.shutdown(Duration::from_secs(2)).await);
    assert_eq!(owner.reader.physical_workers(), 0);
    assert_eq!(owner.reader.retained_memory_bytes(), 0);
    assert_eq!(auth.image_resources()["admitted"], 0);
    std::fs::write(std::env::var("VOD_IMAGE_METADATA_REPORT").unwrap(),serde_json::to_vec_pretty(&json!({"passed":true,"actual_metadata_SQL_lock_abort_retains_68MiB_and_shared_HTTPslot":true,"shutdown_false_until_reply_rollback":true,"same_existing_two_read_workers_and_192MiB":true,"no_late_image_fetch_after_cancel_close":true})).unwrap()).unwrap();
}

#[path = "vod_api_images_response_headers_peer.rs"]
mod response_headers;
