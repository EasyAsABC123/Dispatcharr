//! Pull-driven XMLTV chunk delivery, preserving bounded leader/follower flow.
//! Original Dispatcharr streaming_chunk_cache authors retain full credit.
use crate::cache::{Cache, CacheBytes, CacheError, Cursor, Entry, EntryState, Read};
use futures_util::{Stream, StreamExt};
use std::{pin::Pin, sync::Arc, time::Duration};
pub type Source = Pin<Box<dyn Stream<Item = Result<String, dispatcharr_store::StoreError>> + Send>>;

/// The request actor owns this object until cleanup finishes. A leader creates
/// only one source iterator; followers never perform SQL/render work. The HTTP
/// actor must retain its admission on every physical operation using `guard`.
pub struct Delivery {
    cache: Cache,
    entry: Entry,
    cursor: Cursor,
    source: Option<Source>,
    pending: Option<String>,
    offset: usize,
    complete: bool,
    deadline: tokio::time::Instant,
    refreshed: Option<tokio::time::Instant>,
}
impl Delivery {
    pub fn new(
        cache: Cache,
        entry: Entry,
        source: Option<Source>,
        wait: Duration,
    ) -> Result<Self, CacheError> {
        if (entry.state == EntryState::Leader) != source.is_some()
            || wait.is_zero()
            || wait > Duration::from_secs(600)
        {
            return Err(CacheError::Configuration);
        }
        Ok(Self {
            cache,
            entry,
            cursor: Cursor::default(),
            source,
            pending: None,
            offset: 0,
            complete: false,
            deadline: tokio::time::Instant::now() + wait,
            refreshed: None,
        })
    }
    pub fn completed_build(&self) -> bool {
        self.complete
    }
    pub async fn next(
        &mut self,
        guard: Arc<dyn Send + Sync>,
    ) -> Result<Option<CacheBytes>, CacheError> {
        if self.source.is_some() {
            if self.pending.is_none() {
                match self.source.as_mut().expect("leader source").next().await {
                    Some(Ok(value)) => {
                        self.pending = Some(value);
                        self.offset = 0;
                    }
                    Some(Err(_)) => {
                        let _ = self.entry.abort(guard.clone()).await;
                        self.source = None;
                        return Err(CacheError::Unavailable);
                    }
                    None => {
                        self.entry.finish(guard.clone()).await?;
                        self.source = None;
                        self.complete = true;
                    }
                }
            }
            if let Some(value) = &self.pending {
                let mut end = (self.offset + self.cache.chunk_bytes()).min(value.len());
                while end > self.offset && !value.is_char_boundary(end) {
                    end -= 1;
                }
                if end == self.offset && end < value.len() {
                    return Err(CacheError::Capacity);
                }
                self.entry
                    .put(&value[self.offset..end], guard.clone())
                    .await?;
                self.offset = end;
                if end == value.len() {
                    self.pending = None;
                }
            }
        }
        loop {
            if tokio::time::Instant::now() >= self.deadline {
                return Err(CacheError::Unavailable);
            }
            let renew = self
                .refreshed
                .is_none_or(|at| at.elapsed() >= self.cache.refresh_interval());
            match self
                .cache
                .read(&self.entry.base, &mut self.cursor, renew, guard.clone())
                .await
            {
                Ok(Read::Data(bytes)) => {
                    if renew {
                        self.refreshed = Some(tokio::time::Instant::now())
                    };
                    return Ok(Some(bytes));
                }
                Ok(Read::End) => return Ok(None),
                Ok(Read::Failed) => return Err(CacheError::Unavailable),
                Ok(Read::Wait) | Err(CacheError::Capacity) => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(error) => return Err(error),
            }
        }
    }
}
