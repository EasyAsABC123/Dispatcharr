//! Local differential adapter; original Dispatcharr credit retained.
use dispatcharr_ingest_http::{Downloader, FetchConfig, FetchError};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, BufRead},
    time::Duration,
};
#[tokio::main]
async fn main() {
    let downloader = Downloader::new(FetchConfig::default()).unwrap();
    for (index, line) in io::stdin().lock().lines().enumerate() {
        let request: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let directory = std::env::temp_dir().join(format!(
            "dispatcharr-fetch-oracle-{}-{index}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("1.m3u");
        fs::write(&path, b"previous cache").unwrap();
        let result = downloader
            .fetch(request["url"].as_str().unwrap(), "Café", &path)
            .await;
        let (success, error_class, error_message, status) = match result {
            Ok(value) => (true, None, None, Some(value.status)),
            Err(FetchError::Status { status, .. }) => (false, Some("status"), None, Some(status)),
            Err(FetchError::Cache(dispatcharr_ingest_core::CacheError::Invalid(message))) => {
                (false, Some("cache"), Some(message), None)
            }
            Err(FetchError::Transport(_) | FetchError::Body(_)) => {
                (false, Some("network"), None, None)
            }
            Err(error) => panic!("Unexpected fixture result: {error:?}"),
        };
        let bytes = fs::read(&path).unwrap();
        let result = json!({"success":success,"error_class":error_class,
            "error_message":error_message.map(|message|message.replace(request["base"].as_str().unwrap(),"<origin>")),"status":status,
            "cache_bytes":bytes.len(),"cache_sha256":format!("{:x}",Sha256::digest(bytes)),"temporary_exists":directory.join("1.m3u.tmp").exists()});
        fs::remove_dir_all(directory).unwrap();
        println!("{result}");
    }
    assert!(downloader.shutdown_wait(Duration::from_secs(3)).await);
}
