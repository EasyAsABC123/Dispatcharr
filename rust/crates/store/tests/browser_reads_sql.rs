//! Deferred React reads against original PostgreSQL and original HTTP goldens.
//! Original Dispatcharr contributors retain all application credit.
use dispatcharr_store::{AuthConfig, AuthService, Store};
use serde_json::{json, Value};
#[tokio::test]
#[ignore = "requires owned disposable original PostgreSQL fixture and source golden file"]
async fn original_logo_and_stream_filter_option_reads() {
    assert_eq!(
        std::env::var("BROWSER_READS_DISPOSABLE_FIXTURE").as_deref(),
        Ok("playback-fixture-only")
    );
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("BROWSER_READS_FIXTURE").unwrap()).unwrap(),
    )
    .unwrap();
    let store = Store::connect(&std::env::var("DATABASE_TEST_URL").unwrap(), 2)
        .await
        .unwrap();
    let auth = AuthService::new(
        store,
        AuthConfig::new("playback-fixture-only".into()).unwrap(),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = auth
        .router()
        .layer(axum::Extension(axum::extract::ConnectInfo(
            "127.0.0.1:12345".parse::<std::net::SocketAddr>().unwrap(),
        )));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    for (index, case) in fixture["cases"].as_array().unwrap().iter().enumerate() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let mut request = format!(
            "GET {} HTTP/1.1\r\nConnection: close\r\n",
            case["path"].as_str().unwrap()
        );
        for (key, value) in case["headers"].as_object().unwrap() {
            request.push_str(&format!(
                "{}: {}\r\n",
                key.strip_prefix("HTTP_").unwrap().replace('_', "-"),
                value.as_str().unwrap()
            ));
        }
        request.push_str("\r\n");
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut raw = vec![];
        socket.read_to_end(&mut raw).await.unwrap();
        let boundary = raw.windows(4).position(|v| v == b"\r\n\r\n").unwrap();
        let head = std::str::from_utf8(&raw[..boundary]).unwrap();
        let status = head
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        let body: Value = serde_json::from_slice(&raw[boundary + 4..]).unwrap();
        assert_eq!(
            json!({"status":status,"body":body}),
            case["expected"],
            "original deferred read case {index} path {}",
            case["path"]
        );
    }
    server.abort();
}
