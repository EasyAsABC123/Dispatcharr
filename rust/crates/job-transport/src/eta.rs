//! Bounded Source117/119 ETA reservation codec; transport acceptance is separate.
//! Warm restore uses RPUSH in reservation arrival order, so delivery reverses
//! that order. Visible restore traverses ZREVRANGEBYSCORE before RPUSH.
//! Original Dispatcharr, Celery and Kombu authors retain transport/algorithm credit.
use crate::{
    index_protocol::{self, IndexMessage},
    protocol::ProtocolError,
};
use serde_json::{json, Value};
use uuid::Uuid;
pub const MAX_UNACKED: usize = 131072;
pub const RESERVATION_MEMORY: usize = 8 * 1024 * 1024;
#[derive(Clone, Debug)]
pub(crate) struct Receipt {
    pub message: IndexMessage,
    pub delivery_tag: Uuid,
    pub redelivered: bool,
    /// Original queue bytes used for atomic compare/reserve, never logged.
    pub queued_bytes: Vec<u8>,
    /// Exact stored bytes are retained for owner-checked ACK/restore.
    pub tuple_bytes: Vec<u8>,
    pub restored_bytes: Vec<u8>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_pending: usize,
    pub max_pending_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_pending: 16,
            max_pending_bytes: 2 * 1024 * 1024,
        }
    }
}
pub(crate) struct Pool {
    pub limits: Limits,
    pub count: std::sync::Arc<tokio::sync::Semaphore>,
}
pub(crate) struct Budget {
    _count: tokio::sync::OwnedSemaphorePermit,
}
impl Pool {
    pub fn new(limits: Limits) -> Result<Self, crate::transport::Error> {
        if limits.max_pending == 0
            || limits.max_pending > 128
            || limits.max_pending_bytes == 0
            || limits.max_pending_bytes > 16 * 1024 * 1024
        {
            return Err(crate::transport::Error::Configuration);
        }
        // Each receipt has <=64KiB queue +128KiB stored tuple +64KiB restored
        // bytes plus transient bounded JSON validation state. A conservative
        // max_pending*8MiB bounds local receipt/validation admission;
        // max_pending_bytes separately bounds the server's stored tuple bytes.
        Ok(Self {
            limits,
            count: std::sync::Arc::new(tokio::sync::Semaphore::new(limits.max_pending)),
        })
    }
    pub fn acquire(&self) -> Result<Budget, crate::transport::Error> {
        Ok(Budget {
            _count: self
                .count
                .clone()
                .try_acquire_owned()
                .map_err(|_| crate::transport::Error::Capacity)?,
        })
    }
}
#[derive(Clone)]
pub struct Candidate {
    pub(crate) receipt: std::sync::Arc<Receipt>,
    pub(crate) budget: std::sync::Arc<Budget>,
    pub(crate) pool: std::sync::Arc<Pool>,
}
impl Candidate {
    pub fn message(&self) -> &IndexMessage {
        &self.receipt.message
    }
}
#[derive(Clone)]
pub struct Reservation {
    pub(crate) receipt: std::sync::Arc<Receipt>,
    _budget: std::sync::Arc<Budget>,
    pub(crate) pool: std::sync::Arc<Pool>,
}
impl Reservation {
    pub(crate) fn new(
        receipt: Receipt,
        budget: std::sync::Arc<Budget>,
        pool: std::sync::Arc<Pool>,
    ) -> Self {
        Self {
            receipt: std::sync::Arc::new(receipt),
            _budget: budget,
            pool,
        }
    }
    pub(crate) fn new_shared(
        receipt: std::sync::Arc<Receipt>,
        budget: std::sync::Arc<Budget>,
        pool: std::sync::Arc<Pool>,
    ) -> Self {
        Self {
            receipt,
            _budget: budget,
            pool,
        }
    }
    pub fn message(&self) -> &IndexMessage {
        &self.receipt.message
    }
    pub fn delivery_tag(&self) -> Uuid {
        self.receipt.delivery_tag
    }
    pub fn tuple_bytes(&self) -> &[u8] {
        &self.receipt.tuple_bytes
    }
    pub fn redelivered(&self) -> bool {
        self.receipt.redelivered
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
fn bounded_json(bytes: &[u8], maximum: usize) -> Result<Value, ProtocolError> {
    if bytes.len() > maximum {
        return Err(ProtocolError::Capacity);
    }
    serde_json::from_slice(bytes).map_err(|_| invalid())
}
fn remove_redelivery(envelope: &mut Value) -> Result<bool, ProtocolError> {
    let header = envelope["headers"].get("redelivered");
    let delivery = envelope["properties"]["delivery_info"].get("redelivered");
    match (header, delivery) {
        (None, None) => Ok(false),
        (Some(Value::Bool(true)), Some(Value::Bool(true))) => {
            envelope["headers"]
                .as_object_mut()
                .ok_or_else(invalid)?
                .remove("redelivered");
            envelope["properties"]["delivery_info"]
                .as_object_mut()
                .ok_or_else(invalid)?
                .remove("redelivered");
            Ok(true)
        }
        _ => Err(invalid()),
    }
}
impl Receipt {
    pub fn prepare(bytes: &[u8]) -> Result<Self, ProtocolError> {
        let original = bounded_json(bytes, crate::protocol::MAX_ENVELOPE)?;
        let mut normalized = original.clone();
        let redelivered = remove_redelivery(&mut normalized)?;
        let message =
            index_protocol::parse(&serde_json::to_vec(&normalized).map_err(|_| invalid())?)?;
        let delivery_tag = Uuid::parse_str(
            original["properties"]["delivery_tag"]
                .as_str()
                .ok_or_else(invalid)?,
        )
        .map_err(|_| invalid())?;
        let tuple_bytes =
            serde_json::to_vec(&json!([original, "", "celery"])).map_err(|_| invalid())?;
        if tuple_bytes.len() > MAX_UNACKED {
            return Err(ProtocolError::Capacity);
        }
        let mut restored = original;
        restored["headers"]["redelivered"] = json!(true);
        restored["properties"]["delivery_info"]["redelivered"] = json!(true);
        let restored_bytes = serde_json::to_vec(&restored).map_err(|_| invalid())?;
        if restored_bytes.len() > crate::protocol::MAX_ENVELOPE {
            return Err(ProtocolError::Capacity);
        }
        Ok(Self {
            message,
            delivery_tag,
            redelivered,
            queued_bytes: bytes.to_vec(),
            tuple_bytes,
            restored_bytes,
        })
    }
    pub fn from_stored(bytes: &[u8], tag: Uuid) -> Result<Self, ProtocolError> {
        let tuple = bounded_json(bytes, MAX_UNACKED)?;
        let parts = tuple
            .as_array()
            .filter(|v| v.len() == 3 && v[1] == "" && v[2] == "celery")
            .ok_or_else(invalid)?;
        let raw = serde_json::to_vec(&parts[0]).map_err(|_| invalid())?;
        let mut receipt = Self::prepare(&raw)?;
        if receipt.delivery_tag != tag {
            return Err(invalid());
        }
        // Python and Rust JSON whitespace/escaping may differ. Owner comparison
        // uses the actual existing Redis bytes, not reconstructed serialization.
        receipt.tuple_bytes = bytes.to_vec();
        Ok(receipt)
    }
}
// Source-derived atomic operations; real Redis lifecycle tests establish their
// implementation acceptance separately from the actual Source codec corpus.
// KEYS: queue, unacked hash, unacked_index zset. ARGV: queue bytes, tag,
// exact tuple bytes, Source-compatible wall epoch seconds.
pub const RESERVE: &str = r#"
local q=redis.call('TYPE',KEYS[1]).ok
local h=redis.call('TYPE',KEYS[2]).ok
local z=redis.call('TYPE',KEYS[3]).ok
if q=='none'then return 0 end
if q~='list'or(h~='none'and h~='hash')or(z~='none'and z~='zset')then return -3 end
if #ARGV[1]>65536 or #ARGV[3]>131072 or #ARGV[2]~=36 then return -3 end
local score=tonumber(ARGV[4]);if not score or score<=0 then return -3 end
local maximum=tonumber(ARGV[5]);local bytes=tonumber(ARGV[6])
if not maximum or maximum<1 or maximum>128 or not bytes or bytes<1 or bytes>16777216 then return -3 end
if redis.call('HLEN',KEYS[2])>=maximum then return -4 end
local size=#ARGV[3]
for _,tag in ipairs(redis.call('HKEYS',KEYS[2]))do
 if #tag~=36 then return -3 end
 local n=redis.call('HSTRLEN',KEYS[2],tag);if n>131072 then return -3 end
 size=size+n
end
if size>bytes then return -4 end
if redis.call('LINDEX',KEYS[1],-1)~=ARGV[1]then return 0 end
if redis.call('HEXISTS',KEYS[2],ARGV[2])~=0 or redis.call('ZSCORE',KEYS[3],ARGV[2])then return -2 end
redis.call('RPOP',KEYS[1]);redis.call('ZADD',KEYS[3],ARGV[4],ARGV[2]);redis.call('HSET',KEYS[2],ARGV[2],ARGV[3]);return 1
"#;
// ARGV tag, exact original tuple. Removing one owned member preserves unrelated
// tags/score/TTL. A lost owner cannot ACK another consumer's reservation.
pub const ACK: &str = r#"
local h=redis.call('TYPE',KEYS[2]).ok;local z=redis.call('TYPE',KEYS[3]).ok
if h=='none'and z=='none'then return 0 end
if h~='hash'or z~='zset'or #ARGV[1]~=36 or #ARGV[2]>131072 then return -3 end
if redis.call('HSTRLEN',KEYS[2],ARGV[1])~=#ARGV[2]then return 0 end
if redis.call('HGET',KEYS[2],ARGV[1])~=ARGV[2]then return 0 end
redis.call('ZREM',KEYS[3],ARGV[1]);redis.call('HDEL',KEYS[2],ARGV[1]);return 1
"#;
// Source default RPUSH is proven by119 with existing queue markers/multiple
// ETAs. Caller warm-restores reservation arrival order; visible order differs.
// ARGV tag, exact tuple bytes, restored message bytes.
pub const RESTORE: &str = r#"
local q=redis.call('TYPE',KEYS[1]).ok
local h=redis.call('TYPE',KEYS[2]).ok;local z=redis.call('TYPE',KEYS[3]).ok
if h=='none'and z=='none'then return 0 end
if(q~='none'and q~='list')or h~='hash'or z~='zset'then return -3 end
if #ARGV[1]~=36 or #ARGV[2]>131072 or #ARGV[3]>65536 then return -3 end
if redis.call('HSTRLEN',KEYS[2],ARGV[1])~=#ARGV[2]then return 0 end
if redis.call('HGET',KEYS[2],ARGV[1])~=ARGV[2]then return 0 end
redis.call('RPUSH',KEYS[1],ARGV[3]);redis.call('ZREM',KEYS[3],ARGV[1]);redis.call('HDEL',KEYS[2],ARGV[1]);return 1
"#;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source117_real_tuple_and_restored_fields_preserve_identity() {
        let data: Value =
            serde_json::from_str(include_str!("../datasets/source-index-eta-117.json")).unwrap();
        assert_eq!(data["reservations"].as_array().unwrap().len(), 3);
        for row in data["reservations"].as_array().unwrap() {
            let bytes = serde_json::to_vec(&row["tuple"]).unwrap();
            let tag = Uuid::parse_str(row["delivery_tag"].as_str().unwrap()).unwrap();
            let receipt = Receipt::from_stored(&bytes, tag).unwrap();
            assert_eq!(receipt.tuple_bytes, bytes);
            assert!(receipt.message.eta.is_some());
            assert_eq!(
                row["types"],
                json!({"unacked":"hash","unacked_index":"zset","celery":"none"})
            );
        }
        assert_eq!(data["restored"].as_array().unwrap().len(), 2);
        for row in data["restored"].as_array().unwrap() {
            let receipt = Receipt::prepare(&serde_json::to_vec(&row["envelope"]).unwrap()).unwrap();
            assert!(receipt.redelivered);
            let restored: Value = serde_json::from_slice(&receipt.restored_bytes).unwrap();
            assert_eq!(restored, row["envelope"]);
        }
    }
    #[test]
    fn mismatched_owner_or_partial_redelivery_is_not_accepted() {
        let data: Value =
            serde_json::from_str(include_str!("../datasets/source-index-eta-117.json")).unwrap();
        let row = &data["restored"][0];
        let mut e = row["envelope"].clone();
        e["headers"].as_object_mut().unwrap().remove("redelivered");
        assert!(Receipt::prepare(&serde_json::to_vec(&e).unwrap()).is_err());
        let tuple = serde_json::to_vec(&data["reservations"][0]["tuple"]).unwrap();
        assert!(Receipt::from_stored(&tuple, Uuid::new_v4()).is_err());
        assert!(Receipt::prepare(&vec![b'x'; 65537]).is_err());
    }
}
