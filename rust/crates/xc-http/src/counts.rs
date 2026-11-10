//! Draft bounded port of apps/proxy/utils.py get_user_active_connections.
//! Original Dispatcharr authors retain credit. Not executed or accepted yet.
use dispatcharr_output_store::{
    transport::{RedisTransport, TransportConfig},
    Error,
};
use dispatcharr_store::StoreError;
use std::{sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Redis COUNT is a hint, not a response bound. Cap selected fields in Lua before
// returning them to the multiplexed driver; never deserialize an unbounded SCAN.
const PAGE: &str = r#"
local scanned=redis.call('SCAN',ARGV[1],'MATCH',ARGV[2],'COUNT',128)
local rows={}; local total=0
if #scanned[2]>tonumber(ARGV[3]) then return {-1,'0',0,{}} end
for _,key in ipairs(scanned[2]) do
  if #key>4096 then return {-1,'0',0,{}} end
  local valid=true
  if ARGV[4]~='created_at' then
    local prefix=ARGV[5]; local at=string.find(key,':clients:',#prefix+1,true)
    valid=at and at>#prefix+1 and at+9<=#key
  end
  if valid then
    if redis.call('HSTRLEN',key,'user_id')>16384 or redis.call('HSTRLEN',key,ARGV[4])>16384 then return {-1,'0',0,{}} end
    local fields=redis.call('HMGET',key,'user_id',ARGV[4])
    total=total+#key+#(fields[1] or '')+#(fields[2] or '')+64
    if total>tonumber(ARGV[6]) then return {-1,'0',0,{}} end
    rows[#rows+1]=fields
  end
end
return {1,scanned[1],#scanned[2],rows}
"#;
type Page = (i64, String, usize, Vec<(Option<String>, Option<String>)>);

#[derive(Clone)]
pub struct ConnectionCounts {
    transport: RedisTransport,
    replies: Arc<Semaphore>,
    timestamp_valid: fn(&str) -> bool,
    max_scanned: usize,
    max_pages: usize,
    page_bytes: usize,
}
impl ConnectionCounts {
    /// Timestamp grammar verified against the pinned original Python corpus.
    pub async fn connect_source(redis_url: &str) -> Result<Self, StoreError> {
        Self::connect(redis_url, |value| {
            dispatcharr_ingest_core::source_float_literal(value).is_some()
        })
        .await
    }
    /// No unverified Rust float parser is silently selected. Supply the shared
    /// Source-compatible parser after the original Redis/helper corpus passes.
    pub async fn connect(
        redis_url: &str,
        timestamp_valid: fn(&str) -> bool,
    ) -> Result<Self, StoreError> {
        let transport = RedisTransport::connect(
            redis_url,
            TransportConfig {
                max_operations: 8,
                max_queued_operations: 32,
                command_timeout: Duration::from_secs(3),
            },
        )
        .await
        .map_err(|_| crate::reader::capacity())?;
        Ok(Self {
            transport,
            replies: Arc::new(Semaphore::new(8 * 128 * 1024)),
            timestamp_valid,
            max_scanned: 50000,
            max_pages: 1024,
            page_bytes: 64 * 1024,
        })
    }
    pub async fn count(
        &self,
        user: Option<i64>,
        request: Arc<OwnedSemaphorePermit>,
    ) -> Result<usize, StoreError> {
        match self.read(user, request).await {
            Ok(value) => Ok(value),
            // Original helper catches every outer Redis/decode/conversion
            // exception and discards all previously counted namespaces.
            Err(Failure::Source) => Ok(0),
            Err(Failure::Capacity) => Err(crate::reader::capacity()),
        }
    }
    async fn read(
        &self,
        user: Option<i64>,
        request: Arc<OwnedSemaphorePermit>,
    ) -> Result<usize, Failure> {
        let mut count = 0usize;
        let mut scanned = 0usize;
        let mut pages = 0usize;
        for (pattern, field, prefix) in [
            ("live:channel:*:clients:*", "connected_at", "live:channel:"),
            (
                "timeshift:channel:*:clients:*",
                "connected_at",
                "timeshift:channel:",
            ),
            (
                "vod_persistent_connection:*",
                "created_at",
                "vod_persistent_connection:",
            ),
        ] {
            let mut cursor = "0".to_owned();
            loop {
                pages = pages.checked_add(1).ok_or(Failure::Capacity)?;
                if pages > self.max_pages {
                    return Err(Failure::Capacity);
                }
                // Covers decoded Strings, tuples and reply-array capacity, not
                // merely wire length. Guard survives cancelled Redis callers.
                let guard = self
                    .replies
                    .clone()
                    .try_acquire_many_owned((self.page_bytes * 2) as u32)
                    .map_err(|_| Failure::Capacity)?;
                let row_limit = self.max_scanned.saturating_sub(scanned).to_string();
                let bytes = self.page_bytes.to_string();
                let args = [
                    cursor.as_bytes(),
                    pattern.as_bytes(),
                    row_limit.as_bytes(),
                    field.as_bytes(),
                    prefix.as_bytes(),
                    bytes.as_bytes(),
                ];
                let (page, guard): (Page, _) = self
                    .transport
                    .command(PAGE, &[], &args, (guard, request.clone()))
                    .await
                    .map_err(|error| {
                        if error == Error::Capacity {
                            Failure::Capacity
                        } else {
                            Failure::Source
                        }
                    })?;
                let (status, next, seen, rows) = page;
                if status != 1 {
                    return Err(Failure::Capacity);
                }
                scanned = scanned.checked_add(seen).ok_or(Failure::Capacity)?;
                if scanned > self.max_scanned {
                    return Err(Failure::Capacity);
                }
                for (row_user, time) in rows {
                    if let Some(user) = user {
                        let Some(row_user) = row_user.filter(|value| !value.is_empty()) else {
                            continue;
                        };
                        let parsed = dispatcharr_ingest_core::source_integer_literal(&row_user)
                            .ok_or(Failure::Source)?;
                        if parsed != user.to_string() {
                            continue;
                        }
                    }
                    if let Some(time) = time.filter(|value| !value.is_empty()) {
                        if !(self.timestamp_valid)(&time) {
                            continue;
                        }
                    }
                    count = count.checked_add(1).ok_or(Failure::Capacity)?;
                }
                drop(guard);
                if next == "0" {
                    break;
                }
                cursor = next;
            }
        }
        Ok(count)
    }
}
enum Failure {
    Source,
    Capacity,
}
#[cfg(test)]
#[path = "counts_tests.rs"]
mod tests;
