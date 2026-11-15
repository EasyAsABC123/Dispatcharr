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
    Aborted,
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
    let mut body = dispatcharr_upstream_http::body_limited(
        response,
        PreparationLimits::default().cache_bytes as usize,
    )
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
                    return cache
                        .finish(status, &url)
                        .map(WriterEnd::Finished)
                        .map_err(classify_cache)
                }
                None => return Ok(WriterEnd::Aborted),
            }
        }
    });
    let flow=async {
  observer.began(owner.clone()).await?;let start=Instant::now();let mut notified=Instant::now();let mut downloaded=0u64;
  while let Some(chunk)=body.next().await {
   let chunk=chunk.map_err(|_|Error::Unsupported)?;downloaded=downloaded.checked_add(chunk.len()as u64).ok_or(Error::Unsupported)?;
   send.send(Write::Chunk(chunk.to_vec())).await.map_err(|_|Error::Unsettled)?;
   let elapsed=start.elapsed().as_secs_f64();if total>0&&notified.elapsed()>=Duration::from_millis(500)&&elapsed>0.0 {
    let speed=downloaded as f64/elapsed/1024.0;let progress=downloaded as f64/total as f64*100.0;let remaining=(total as f64-downloaded as f64)/(speed*1024.0);
    observer.progress(Progress{progress,speed,elapsed,remaining,message:format!("Downloading: {progress:.1}% - {speed:.1} KB/s - {remaining:.1}s remaining")},owner.clone()).await?;notified=Instant::now();
   }
  }
  if attempt.cookie_budget_exceeded(){return Err(Error::Unsupported)}
  send.send(Write::Finish).await.map_err(|_|Error::Unsettled)
 }.await;
    drop(send);
    drop(body);
    let settled = writer.await.map_err(|_| Error::Unsettled)?;
    let end = settled?;
    flow?;
    let statistics = match end {
        WriterEnd::Finished(statistics) => statistics,
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
    Ok(Error::Caught(message))
}
