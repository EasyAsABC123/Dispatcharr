//! Bounded HTTP-to-disk ingestion using original Dispatcharr fetch semantics.
//! Full application and algorithm credit belongs to the original authors.
//! This component does not run refresh jobs or write account/progress events.
use bytes::Bytes;
use dispatcharr_ingest_core::{CacheDownload, CacheError, DownloadStats};
use dispatcharr_upstream_http::{body, Attempt};
use futures_util::StreamExt;
use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore};
const FRAME: usize = 8192;

#[derive(Clone, Debug)]
pub struct FetchConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub maximum_bytes: u64,
    pub concurrent_jobs: usize,
    pub queued_frames: usize,
    pub error_body_bytes: usize,
}
impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            maximum_bytes: 2 * 1024 * 1024 * 1024,
            concurrent_jobs: 4,
            queued_frames: 8,
            error_body_bytes: 64 * 1024,
        }
    }
}
#[derive(Debug)]
pub enum FetchError {
    Configuration,
    Shutdown,
    Transport(reqwest::Error),
    Body(io::Error),
    Cache(CacheError),
    HeaderBudget,
    ErrorBodyCapacity,
    /// Error text decoding and account status/event formatting belong to the
    /// source job layer; preserve bytes instead of inventing UTF-8 replacement.
    Status {
        status: u16,
        url: String,
        content_type: Option<Vec<u8>>,
        body: Vec<u8>,
    },
    Worker,
}
impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Configuration => f.write_str("Invalid M3U download configuration"),
            Self::Shutdown => f.write_str("M3U downloader is stopping"),
            Self::Transport(e) => e.fmt(f),
            Self::Body(e) => e.fmt(f),
            Self::Cache(e) => e.fmt(f),
            Self::HeaderBudget => f.write_str("M3U response exceeds header/cookie capacity"),
            Self::ErrorBodyCapacity => f.write_str("M3U error body exceeds configured byte limit"),
            Self::Status { status, .. } => write!(f, "M3U origin returned HTTP {status}"),
            Self::Worker => f.write_str("M3U spool worker failed"),
        }
    }
}
impl std::error::Error for FetchError {}
#[derive(Clone, Debug)]
pub struct FetchedPlaylist {
    pub stats: DownloadStats,
    pub final_url: String,
    pub status: u16,
}
struct State {
    closed: bool,
    active: usize,
}
struct Control {
    state: Mutex<State>,
    jobs: Arc<Semaphore>,
    idle: Notify,
    stop: watch::Sender<bool>,
}
struct JobLease {
    control: Arc<Control>,
    _permit: OwnedSemaphorePermit,
}
impl Drop for JobLease {
    fn drop(&mut self) {
        let mut state = self.control.state.lock().unwrap();
        state.active -= 1;
        self.control.idle.notify_waiters();
    }
}
#[derive(Clone)]
pub struct Downloader {
    config: Arc<FetchConfig>,
    control: Arc<Control>,
}
enum SpoolMessage {
    Data(Bytes),
    Finish { status: u16, url: String },
}
async fn cancelled(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

impl Downloader {
    pub fn new(config: FetchConfig) -> Result<Self, FetchError> {
        if config.concurrent_jobs == 0
            || config.concurrent_jobs > 32
            || config.queued_frames == 0
            || config.queued_frames > 64
            || config.error_body_bytes == 0
            || config.error_body_bytes > 1024 * 1024
            || config.maximum_bytes == 0
            || config.connect_timeout.is_zero()
            || config.read_timeout.is_zero()
        {
            return Err(FetchError::Configuration);
        }
        let (stop, _) = watch::channel(false);
        let control = Arc::new(Control {
            state: Mutex::new(State {
                closed: false,
                active: 0,
            }),
            jobs: Arc::new(Semaphore::new(config.concurrent_jobs)),
            idle: Notify::new(),
            stop,
        });
        Ok(Self {
            config: Arc::new(config),
            control,
        })
    }
    pub fn active_jobs(&self) -> usize {
        self.control.state.lock().unwrap().active
    }
    pub async fn fetch(
        &self,
        url: &str,
        agent: &str,
        destination: &Path,
    ) -> Result<FetchedPlaylist, FetchError> {
        let permit = self
            .control
            .jobs
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| FetchError::Shutdown)?;
        {
            let mut state = self.control.state.lock().unwrap();
            if state.closed {
                return Err(FetchError::Shutdown);
            }
            state.active += 1;
        }
        let lease = JobLease {
            control: self.control.clone(),
            _permit: permit,
        };
        let mut stop = self.control.stop.subscribe();
        tokio::select! {biased;
            _=cancelled(&mut stop)=>Err(FetchError::Shutdown),
            result=self.execute(url,agent,destination.to_owned(),lease)=>result,
        }
    }
    async fn execute(
        &self,
        url: &str,
        agent: &str,
        destination: PathBuf,
        lease: JobLease,
    ) -> Result<FetchedPlaylist, FetchError> {
        let attempt = Attempt::new(self.config.connect_timeout, self.config.read_timeout)
            .map_err(FetchError::Transport)?;
        let response = attempt
            .request(url, agent, &[])
            .map_err(FetchError::Body)?
            .send()
            .await
            .map_err(FetchError::Transport)?;
        if attempt.cookie_budget_exceeded() {
            return Err(FetchError::HeaderBudget);
        }
        let status = response.status().as_u16();
        let final_url = response.url().to_string();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .map(|value| value.as_bytes().to_vec());
        let mut stream = body(response).map_err(FetchError::Body)?;
        if !(200..300).contains(&status) {
            let mut preview = Vec::new();
            while let Some(frame) = stream.next().await {
                let frame = frame.map_err(FetchError::Body)?;
                if frame.len() > self.config.error_body_bytes - preview.len() {
                    return Err(FetchError::ErrorBodyCapacity);
                }
                preview.extend_from_slice(&frame);
            }
            return Err(FetchError::Status {
                status,
                url: url.to_owned(),
                content_type,
                body: preview,
            });
        }
        let (sender, mut receiver) = mpsc::channel(self.config.queued_frames);
        let maximum = self.config.maximum_bytes;
        let mut worker = tokio::task::spawn_blocking(move || {
            let _lease = lease;
            let mut cache =
                CacheDownload::begin(&destination, maximum).map_err(FetchError::Cache)?;
            while let Some(message) = receiver.blocking_recv() {
                match message {
                    SpoolMessage::Data(bytes) => {
                        cache.write_chunk(&bytes).map_err(FetchError::Cache)?
                    }
                    SpoolMessage::Finish { status, url } => {
                        return cache.finish(status, &url).map_err(FetchError::Cache)
                    }
                }
            }
            Err(FetchError::Shutdown)
        });
        let mut failure = None;
        'body: loop {
            let frame = tokio::select! {biased;
                result=&mut worker=>return match result {Ok(Err(error))=>Err(error),_=>Err(FetchError::Worker)},
                frame=stream.next()=>match frame {Some(frame)=>frame,None=>break},
            };
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    failure = Some(FetchError::Body(error));
                    break;
                }
            };
            // Copy only bounded frames: queued slices must not retain a large
            // underlying network allocation through Bytes reference counting.
            for chunk in frame.chunks(FRAME) {
                if sender
                    .send(SpoolMessage::Data(Bytes::copy_from_slice(chunk)))
                    .await
                    .is_err()
                {
                    break 'body;
                }
            }
        }
        if failure.is_none() {
            let _ = sender
                .send(SpoolMessage::Finish {
                    status,
                    url: url.to_owned(),
                })
                .await;
        }
        drop(sender);
        // On ordinary failure await cleanup. If this future is aborted, the
        // worker still holds admission until its channel drains and cache drops.
        let result = worker.await.map_err(|_| FetchError::Worker)?;
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(FetchedPlaylist {
            stats: result?,
            final_url,
            status,
        })
    }
    /// Stop admission, cancel request/body producers and wait for actual disk
    /// workers to finish cleanup. A queued Finish is the cache commit point.
    pub async fn shutdown_wait(&self, timeout: Duration) -> bool {
        {
            let mut state = self.control.state.lock().unwrap();
            state.closed = true;
            self.control.jobs.close();
            self.control.stop.send_replace(true);
        }
        tokio::time::timeout(timeout, async {
            loop {
                let changed = self.control.idle.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.active_jobs() == 0 {
                    return;
                }
                changed.await;
            }
        })
        .await
        .is_ok()
    }
}
