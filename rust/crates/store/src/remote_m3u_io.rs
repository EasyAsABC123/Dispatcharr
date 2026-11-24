//! Owned remote M3U fetch/preparation using existing Requests/cache/parser ports.
//! Original Dispatcharr and Requests authors retain all application credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_ingest_core::{
    prepare_playlist, CacheDownload, CacheError, DownloadStats, PreparationLimits, PreparedPlaylist,
};
use futures_util::StreamExt;
use std::{
    fs::File,
    io::BufReader,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
type Owner = Arc<dyn Send + Sync>;
#[derive(Debug)]
pub enum Error {
    Caught(String),
    DownloadFailed {
        message: String,
        groups_result: String,
    },
    Unsupported,
    Unsettled,
}
pub struct Progress {
    pub progress: f64,
    pub speed: f64,
    pub elapsed: f64,
    pub remaining: f64,
    pub message: String,
}
pub trait Observer: Send + Sync {
    fn starting(&self, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
    fn began(&self, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
    fn progress(&self, value: Progress, owner: Owner) -> BoxFuture<'_, Result<(), Error>>;
}
pub struct Input {
    pub account: i64,
    pub url: String,
    pub agent: String,
    pub directory: PathBuf,
    pub use_cache: bool,
}
pub struct Downloaded {
    pub path: PathBuf,
    pub parsed: PathBuf,
    pub statistics: Option<DownloadStats>,
    pub catalog: PreparedPlaylist,
}
enum WriterEnd {
    Finished(DownloadStats),
    Rejected(String),
    Aborted,
}
fn download_failed(message: String, progress: Option<&str>) -> Error {
    // Source persists the failure via another account query, then the Groups
    // result reads the original account's last successfully stored progress.
    Error::DownloadFailed {
        message,
        groups_result: progress.unwrap_or("Starting download...").to_owned(),
    }
}
enum Write {
    Chunk(Vec<u8>),
    Finish,
}
/// The caller already holds actual M3U admission + Source account lock. The
/// blocking writer retains that same owner and has a ONE-chunk bounded mailbox.
/// Dropping the HTTP observer cannot turn an accepted transfer into success.
pub async fn transfer(
    input: Input,
    observer: Arc<dyn Observer>,
    owner: Owner,
) -> Result<Downloaded, Error> {
    if input.account <= 0 || input.account > i64::from(i32::MAX) {
        return Err(Error::Unsupported);
    }
    let raw = input.directory.join(format!("{}.m3u", input.account));
    let parsed = input.directory.join(format!("{}.json", input.account));
    let root = input.directory.clone();
    let retained = owner.clone();
    tokio::task::spawn_blocking(move || {
        let _owner = retained;
        std::fs::create_dir_all(root)
    })
    .await
    .map_err(|_| Error::Unsettled)?
    .map_err(|_| Error::Unsettled)?;
    if input.use_cache {
        let path = raw.clone();
        let retained = owner.clone();
        let exists = tokio::task::spawn_blocking(move || {
            let _owner = retained;
            std::fs::symlink_metadata(path)
        })
        .await
        .map_err(|_| Error::Unsettled)?;
        match exists {
            Ok(meta) if meta.is_file() && meta.len() > 0 => {
                return prepare(raw, parsed, None, owner).await
            }
            Ok(_) => return Err(Error::Unsupported),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Error::Unsettled),
        }
    }
    observer.starting(owner.clone()).await?;
    let attempt = dispatcharr_upstream_http::Attempt::new_requests(
        Duration::from_secs(30),
        Duration::from_secs(60),
    )
    .map_err(|_| Error::Unsupported)?;
    let response = attempt
        .request(&input.url, &input.agent, &[])
        .map_err(|_| Error::Unsupported)?
        .send()
        .await
        .map_err(|_| Error::Unsupported)?;
    if attempt.cookie_budget_exceeded() {
        return Err(Error::Unsupported);
    }
    let status = response.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(error_response(response, status, &input.url).await?);
    }
    let total = match response.headers().get(reqwest::header::CONTENT_LENGTH) {
        None => 0,
        Some(value) => value
            .to_str()
            .map_err(|_| Error::Unsupported)?
            .parse::<u64>()
            .map_err(|_| Error::Unsupported)?,
    };
    let limit = PreparationLimits::default().cache_bytes;
    let plain_fixed_length = status == 200
        && total > 0
        && total <= limit
        && matches!(
            response.version(),
            reqwest::Version::HTTP_10 | reqwest::Version::HTTP_11
        )
        && !response
            .headers()
            .contains_key(reqwest::header::CONTENT_ENCODING)
        && !response
            .headers()
            .contains_key(reqwest::header::TRANSFER_ENCODING);
    let mut body = dispatcharr_upstream_http::body_limited(response, limit as usize)
        .map_err(|_| Error::Unsupported)?;
    let path = raw.clone();
    let url = input.url.clone();
    let retained = owner.clone();
    let (send, mut receive) = mpsc::channel(1);
    let writer = tokio::task::spawn_blocking(move || {
        let _owner = retained;
        let mut cache = CacheDownload::begin_owned(
            &path,
            PreparationLimits::default().cache_bytes,
            *uuid::Uuid::new_v4().as_bytes(),
        )
        .map_err(classify_cache)?;
        loop {
            match receive.blocking_recv() {
                Some(Write::Chunk(chunk)) => cache.write_chunk(&chunk).map_err(classify_cache)?,
                Some(Write::Finish) => {
                    return match cache.finish(status, &url) {
                        Ok(statistics) => Ok(WriterEnd::Finished(statistics)),
                        Err(CacheError::Invalid(message)) => Ok(WriterEnd::Rejected(message)),
                        Err(error) => Err(classify_cache(error)),
                    };
                }
                None => return Ok(WriterEnd::Aborted),
            }
        }
    });
    let mut last_progress_message: Option<String> = None;
    let flow = async {
        observer.began(owner.clone()).await?;
        let start = Instant::now();
        let mut notified = Instant::now();
        let mut downloaded = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| {
                // Reqwest wraps incoming body framing errors as decoding errors.
                // In this plain, fixed-length response there is no content decoder.
                if plain_fixed_length
                    && downloaded < total
                    && error
                        .get_ref()
                        .and_then(|cause| cause.downcast_ref::<reqwest::Error>())
                        .is_some_and(|cause| cause.is_decode() && !cause.is_timeout())
                {
                    download_failed(
                        format!(
                            "Connection error while fetching M3U file from URL: {}",
                            input.url
                        ),
                        last_progress_message.as_deref(),
                    )
                } else {
                    Error::Unsupported
                }
            })?;
            downloaded = downloaded
                .checked_add(chunk.len() as u64)
                .ok_or(Error::Unsupported)?;
            send.send(Write::Chunk(chunk.to_vec()))
                .await
                .map_err(|_| Error::Unsettled)?;
            let elapsed = start.elapsed().as_secs_f64();
            if total > 0 && notified.elapsed() >= Duration::from_millis(500) && elapsed > 0.0 {
                let speed = downloaded as f64 / elapsed / 1024.0;
                let progress = downloaded as f64 / total as f64 * 100.0;
                let remaining = (total as f64 - downloaded as f64) / (speed * 1024.0);
                let message = format!(
                    "Downloading: {progress:.1}% - {speed:.1} KB/s - {remaining:.1}s remaining"
                );
                observer
                    .progress(
                        Progress {
                            progress,
                            speed,
                            elapsed,
                            remaining,
                            message: message.clone(),
                        },
                        owner.clone(),
                    )
                    .await?;
                last_progress_message = Some(message);
                notified = Instant::now();
            }
        }
        if attempt.cookie_budget_exceeded() {
            return Err(Error::Unsupported);
        }
        send.send(Write::Finish).await.map_err(|_| Error::Unsettled)
    }
    .await;
    drop(send);
    drop(body);
    let settled = writer.await.map_err(|_| Error::Unsettled)?;
    let end = settled?;
    flow?;
    let statistics = match end {
        WriterEnd::Finished(statistics) => statistics,
        WriterEnd::Rejected(message) => {
            return Err(download_failed(message, last_progress_message.as_deref()))
        }
        WriterEnd::Aborted => return Err(Error::Unsettled),
    };
    prepare(raw, parsed, Some(statistics), owner).await
}
fn classify_cache(error: CacheError) -> Error {
    match error {
        CacheError::Invalid(message) => Error::Caught(message),
        CacheError::Capacity => Error::Unsupported,
        CacheError::Io(_) => Error::Unsettled,
    }
}
async fn prepare(
    path: PathBuf,
    parsed: PathBuf,
    statistics: Option<DownloadStats>,
    owner: Owner,
) -> Result<Downloaded, Error> {
    let source = path.clone();
    let destination = parsed.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        let reader =
            BufReader::with_capacity(65536, File::open(source).map_err(|_| Error::Unsettled)?);
        prepare_playlist(reader, &destination, PreparationLimits::default())
            .map_err(|_| Error::Unsupported)
    })
    .await
    .map_err(|_| Error::Unsettled)??;
    Ok(Downloaded {
        path,
        parsed,
        statistics,
        catalog: result,
    })
}
async fn error_response(
    response: reqwest::Response,
    status: u16,
    url: &str,
) -> Result<Error, Error> {
    let kind = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|v| v.to_str())
        .transpose()
        .map_err(|_| Error::Unsupported)?
        .unwrap_or("")
        .to_ascii_lowercase();
    let mut stream = dispatcharr_upstream_http::body_limited(response, 1024 * 1024)
        .map_err(|_| Error::Unsupported)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| Error::Unsupported)?;
        if chunk.len() > 1024 * 1024 - bytes.len() {
            return Err(Error::Unsupported);
        }
        bytes.extend_from_slice(&chunk)
    }
    let text = if kind.contains("charset=utf-8") || kind.contains("charset=\"utf-8\"") {
        String::from_utf8_lossy(&bytes).into_owned()
    } else if kind.starts_with("text/") && !kind.contains("charset=") {
        bytes.iter().map(|b| char::from(*b)).collect()
    } else if bytes.is_ascii() {
        String::from_utf8(bytes).map_err(|_| Error::Unsupported)?
    } else {
        return Err(Error::Unsupported);
    };
    let preview = text.chars().take(1000).collect::<String>();
    let message=match status{884=>format!("Server returned HTTP 884 (authentication/authorization failure) from URL: {url}. Server message: {preview}"),800..=u16::MAX=>format!("Server returned non-standard HTTP status {status} from URL: {url}. Server message: {preview}"),404=>format!("M3U file not found (404) at URL: {url}. Server message: {preview}"),403=>format!("Access forbidden (403) to M3U file at URL: {url}. Server message: {preview}"),401=>format!("Authentication required (401) for M3U file at URL: {url}. Server message: {preview}"),500=>format!("Server error (500) while fetching M3U file from URL: {url}. Server message: {preview}"),_=>format!("HTTP error ({status}) while fetching M3U file from URL: {url}. Server message: {preview}")};
    Ok(download_failed(message, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        io::{Read, Write},
        net::TcpListener,
    };

    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("remote-m3u-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct NoopObserver;
    impl Observer for NoopObserver {
        fn starting(&self, _owner: Owner) -> BoxFuture<'_, Result<(), Error>> {
            Box::pin(async { Ok(()) })
        }

        fn began(&self, _owner: Owner) -> BoxFuture<'_, Result<(), Error>> {
            Box::pin(async { Ok(()) })
        }

        fn progress(&self, _value: Progress, _owner: Owner) -> BoxFuture<'_, Result<(), Error>> {
            Box::pin(async { Ok(()) })
        }
    }

    async fn short_transfer(content_encoding: &str, content_length: u64) -> (Error, String) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {content_length}\r\n{content_encoding}Connection: close\r\n\r\n"
        );
        rejected_transfer(
            response,
            b"#EXTM3U\n#EXTINF:-1,Fixture\nhttp://fixture/live\n".to_vec(),
        )
        .await
    }

    async fn rejected_transfer(response: String, body: Vec<u8>) -> (Error, String) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://{}/owned-playlist.m3u",
            listener.local_addr().unwrap()
        );
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                assert!(request.len() < 8192);
                let mut byte = [0u8];
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            socket.write_all(response.as_bytes()).unwrap();
            socket.write_all(&body).unwrap();
            socket.flush().unwrap();
        });

        let scratch = Scratch::new();
        fs::write(scratch.0.join("17.m3u"), b"previous cache").unwrap();
        fs::write(scratch.0.join("17.json"), b"previous parsed cache").unwrap();
        let result = transfer(
            Input {
                account: 17,
                url: url.clone(),
                agent: "Café".into(),
                directory: scratch.0.clone(),
                use_cache: false,
            },
            Arc::new(NoopObserver),
            Arc::new(()),
        )
        .await;
        server.join().unwrap();
        let Err(error) = result else {
            panic!("short transfer unexpectedly succeeded")
        };
        assert_eq!(
            fs::read(scratch.0.join("17.m3u")).unwrap(),
            b"previous cache"
        );
        assert_eq!(
            fs::read(scratch.0.join("17.json")).unwrap(),
            b"previous parsed cache"
        );
        assert_eq!(fs::read_dir(&scratch.0).unwrap().count(), 2);
        (error, url)
    }

    #[tokio::test]
    async fn interrupted_plain_playlist_uses_original_caught_message_after_writer_retirement() {
        let (error, url) = short_transfer("", 144).await;
        assert!(matches!(
            error, Error::DownloadFailed { message, groups_result }
                if message == format!("Connection error while fetching M3U file from URL: {url}")
                   && groups_result == "Starting download..."
        ));
    }

    #[tokio::test]
    async fn http_error_preserves_the_groups_message_and_bounds_the_account_preview() {
        for body in ["denied".to_owned(), "é".repeat(1001)] {
            let response = format!(
                "HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let preview: String = body.chars().take(1000).collect();
            let (error, url) = rejected_transfer(response, body.into_bytes()).await;
            assert!(matches!(
                error, Error::DownloadFailed { message, groups_result }
                    if message == format!("Access forbidden (403) to M3U file at URL: {url}. Server message: {preview}")
                       && groups_result == "Starting download..."
            ));
        }
    }

    #[tokio::test]
    async fn rejected_success_response_preserves_both_prior_caches_and_groups_message() {
        for body in [b"".as_slice(), b"<html>unauthorized"] {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let (error, url) = rejected_transfer(response, body.to_vec()).await;
            let expected = if body.is_empty() {
                format!("Server responded successfully (HTTP 200) but provided empty M3U file from URL: {url}")
            } else {
                format!("Server returned HTML page instead of M3U file from URL: {url}. This usually indicates an error or authentication issue.")
            };
            assert!(matches!(
                error, Error::DownloadFailed { message, groups_result }
                    if message == expected && groups_result == "Starting download..."
            ));
        }
    }

    #[tokio::test]
    async fn encoded_and_oversized_transfers_do_not_use_plain_connection_error() {
        let (encoded, _) = short_transfer("Content-Encoding: gzip\r\n", 144).await;
        assert!(matches!(encoded, Error::Unsupported));
        let (oversized, _) = short_transfer("", PreparationLimits::default().cache_bytes + 1).await;
        assert!(matches!(oversized, Error::Unsupported));
    }
}
