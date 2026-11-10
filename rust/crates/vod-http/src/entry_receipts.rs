//! Private bookkeeping for one unpublished, uncertain current-generation entry.
//! Original Dispatcharr authors retain full application/algorithm credit.
//! Public close accounting never depends on this private receipt surviving TTL.
use redis::{aio::ConnectionManager, Script};
use std::sync::Arc;

pub(crate) const FORGET: &str = r#"
local kind=redis.call('TYPE',KEYS[1]).ok
if kind=='none' then return 0 end
if kind~='hash' then return redis.error_reply('PRIVATE_RECEIPT_TYPE') end
if redis.call('HSTRLEN',KEYS[1],ARGV[1])~=string.len(ARGV[2]) then return 0 end
if redis.call('HGET',KEYS[1],ARGV[1])~=ARGV[2] then return 0 end
return redis.call('HDEL',KEYS[1],ARGV[1])
"#;

#[derive(Clone)]
pub(crate) struct EntryReceipt {
    pub key: String,
    pub field: String,
    pub metadata_token: String,
    pub lease_token: String,
    pub max_fields: usize,
    pub max_bytes: usize,
}
impl EntryReceipt {
    pub fn new(
        namespace: &str,
        id: &str,
        local: &str,
        lease: &str,
        metadata: &str,
        bodies: usize,
    ) -> Self {
        Self {
            key: format!("{namespace}:vod:entries:{id}:{local}:{lease}"),
            field: uuid::Uuid::new_v4().to_string(),
            metadata_token: metadata.into(),
            lease_token: lease.into(),
            max_fields: bodies,
            max_bytes: bodies * 512 + 2048,
        }
    }
    /// This private forget is independent of current public ownership. A closed
    /// old body may discard only its own bounded field/value after retirement.
    /// Caller must retain the body/control ownership through the actual reply.
    pub async fn forget(
        &self,
        connection: &mut ConnectionManager,
    ) -> Result<(), redis::RedisError> {
        Script::new(FORGET)
            .key(&self.key)
            .arg(&self.field)
            .arg(&self.metadata_token)
            .invoke_async::<i64>(connection)
            .await
            .map(|_| ())
    }
}

/// Hold a successful physical ENTER's data until its observer claims it, or
/// until conditional same-generation cleanup and private forgetting settle.
/// The actual integration retains its existing true-body and control receipts;
/// this type does not own any engine/session and cannot form an actor cycle.
pub(crate) type SharedReceipt = Arc<EntryReceipt>;
