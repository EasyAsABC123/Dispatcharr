//! Bounded HTTP archive selection ported from apps/timeshift/views.py.
//! All original application credit: upstream Dispatcharr contributors.
use bytes::{Bytes, BytesMut};
use reqwest::{header, Client, Response, StatusCode};
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct ArchiveClient {
    follow: Client,
    direct: Client,
    requests: Arc<Semaphore>,
}
/// Deliberately does not implement Debug: URLs can contain provider secrets.
pub struct ArchiveRequest<'a> {
    pub urls: &'a [String],
    pub cached_format: Option<usize>,
    pub final_url: Option<&'a str>,
    pub user_agent: &'a str,
    pub range: Option<&'a str>,
}
pub struct ArchiveFailure {
    pub status: StatusCode,
    pub message: &'static str,
    pub decisive: bool,
}
pub enum ArchiveOpen {
    Media(ArchiveMedia),
    Unsatisfiable { content_range: Option<String> },
}
/// Owns one admitted request for its entire upstream body lifetime. A consumer
/// forwards the inspected prefix before later chunks; partial bytes are untrimmed.
pub struct ArchiveMedia {
    response: Response,
    prefix: Option<Bytes>,
    pub format_index: Option<usize>,
    pub used_cached_final: bool,
    _permit: OwnedSemaphorePermit,
}
impl ArchiveMedia {
    pub fn status(&self) -> StatusCode {
        self.response.status()
    }
    pub fn headers(&self) -> &header::HeaderMap {
        self.response.headers()
    }
    /// This may contain provider credentials; callers must never log it.
    pub fn final_url(&self) -> &str {
        self.response.url().as_str()
    }
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>, reqwest::Error> {
        if let Some(prefix) = self.prefix.take() {
            return Ok(Some(prefix));
        }
        self.response.chunk().await
    }
}
fn failure(status: u16, message: &'static str, decisive: bool) -> ArchiveFailure {
    ArchiveFailure {
        status: StatusCode::from_u16(status).unwrap(),
        message,
        decisive,
    }
}
impl ArchiveClient {
    /// Source busy-tail passthrough skips MPEG-TS inspection and redirects.
    pub async fn open_probe(
        &self,
        url: &str,
        user_agent: &str,
        range: &str,
    ) -> Result<ArchiveOpen, ArchiveFailure> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(503, "Stream slot busy", false))?;
        let mut builder = self
            .direct
            .get(url)
            .header(header::ACCEPT_ENCODING, "identity")
            .header(header::RANGE, range);
        if !user_agent.is_empty() {
            builder = builder.header(header::USER_AGENT, user_agent);
        }
        let response = builder
            .send()
            .await
            .map_err(|_| failure(503, "Stream slot busy", false))?;
        if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            return Ok(ArchiveOpen::Unsatisfiable {
                content_range: response
                    .headers()
                    .get(header::CONTENT_RANGE)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            });
        }
        if response.status() != StatusCode::OK && response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(failure(503, "Stream slot busy", false));
        }
        Ok(ArchiveOpen::Media(ArchiveMedia {
            response,
            prefix: None,
            format_index: None,
            used_cached_final: true,
            _permit: permit,
        }))
    }
    pub fn new(
        max_requests: usize,
        connect: Duration,
        read: Duration,
    ) -> Result<Self, &'static str> {
        if max_requests == 0 || connect.is_zero() || read.is_zero() {
            return Err("invalid archive HTTP limits");
        }
        let builder = || {
            Client::builder()
                .connect_timeout(connect)
                .read_timeout(read)
                .pool_max_idle_per_host(2)
        };
        let follow = builder()
            .redirect(reqwest::redirect::Policy::limited(30))
            .build()
            .map_err(|_| "archive client initialization failed")?;
        let direct = builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| "archive client initialization failed")?;
        Ok(Self {
            follow,
            direct,
            requests: Arc::new(Semaphore::new(max_requests)),
        })
    }
    pub async fn open(&self, request: ArchiveRequest<'_>) -> Result<ArchiveOpen, ArchiveFailure> {
        let permit = self
            .requests
            .clone()
            .try_acquire_owned()
            .map_err(|_| failure(503, "Archive request capacity reached", false))?;
        let mut order: Vec<usize> = (0..request.urls.len()).collect();
        if let Some(index) = request.cached_format.filter(|index| *index < order.len()) {
            order.retain(|value| *value != index);
            order.insert(0, index);
        }
        let attempts = request
            .final_url
            .map(|url| (url, None, true))
            .into_iter()
            .chain(
                order
                    .into_iter()
                    .map(|index| (request.urls[index].as_str(), Some(index), false)),
            );
        let mut last_status = 0;
        let mut decisive = false;
        for (url, index, cached) in attempts {
            let client = if cached { &self.direct } else { &self.follow };
            let mut builder = client.get(url).header(header::ACCEPT_ENCODING, "identity");
            if !request.user_agent.is_empty() {
                builder = builder.header(header::USER_AGENT, request.user_agent);
            }
            if let Some(range) = request.range.filter(|value| !value.is_empty()) {
                builder = builder.header(header::RANGE, range);
            }
            let mut response = match builder.send().await {
                Ok(response) => response,
                Err(_) if cached => continue,
                Err(_) => return Err(failure(400, "Provider connection error", false)),
            };
            let status = response.status();
            last_status = status.as_u16();
            if status == StatusCode::RANGE_NOT_SATISFIABLE {
                return Ok(ArchiveOpen::Unsatisfiable {
                    content_range: response
                        .headers()
                        .get(header::CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned),
                });
            }
            if status == StatusCode::OK || status == StatusCode::PARTIAL_CONTENT {
                // Requests raw.read(1024) fills the prefix, rather than treating
                // a short network frame as EOF. Keep every byte of the final
                // chunk, including bytes past the inspection prefix.
                let mut prefix = BytesMut::new();
                while prefix.len() < 1024 {
                    match response.chunk().await {
                        Ok(Some(chunk)) => prefix.extend_from_slice(&chunk),
                        Ok(None) => break,
                        Err(_) => return Err(failure(400, "Provider connection error", false)),
                    }
                }
                let content_type = response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                let partial = status == StatusCode::PARTIAL_CONTENT
                    && request.range.is_some_and(|value| !value.is_empty());
                let inspected = &prefix[..prefix.len().min(1024)];
                let offset = if partial
                    && !prefix.is_empty()
                    && !content_type.contains("html")
                    && !content_type.contains("json")
                {
                    Some(0)
                } else {
                    find_sync(inspected)
                };
                if let Some(offset) = offset {
                    return Ok(ArchiveOpen::Media(ArchiveMedia {
                        response,
                        prefix: Some(prefix.freeze().slice(offset..)),
                        format_index: index,
                        used_cached_final: cached,
                        _permit: permit,
                    }));
                }
                last_status = 404;
                continue;
            }
            if cached {
                continue;
            }
            if matches!(status.as_u16(), 401 | 403 | 406) || status.is_redirection() {
                decisive = true;
                break;
            }
        }
        Err(match last_status {
            404 => failure(404, "Catch-up not available yet", decisive),
            403 => failure(403, "Provider denied access", decisive),
            _ => failure(400, "Provider error", decisive),
        })
    }
}
fn find_sync(data: &[u8]) -> Option<usize> {
    (0..data.len().saturating_sub(376)).find(|&index| {
        data[index] == 0x47 && data[index + 188] == 0x47 && data[index + 376] == 0x47
    })
}
