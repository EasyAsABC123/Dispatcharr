//! Bounded typed normal XML TVG ETA reservation codec (shared Source117/119 geometry); transport acceptance is separate.
//! Warm restore uses RPUSH in reservation arrival order, so delivery reverses
//! that order. Visible restore traverses ZREVRANGEBYSCORE before RPUSH.
//! Original Dispatcharr, Celery and Kombu authors retain transport/algorithm credit.
use crate::{
    protocol::ProtocolError,
    xml_tvg_protocol::{self, Message},
};
use serde_json::{json, Value};
use uuid::Uuid;
pub const MAX_UNACKED: usize = 131072;
#[derive(Clone, Debug)]
pub(crate) struct Receipt {
    pub message: Message,
    pub delivery_tag: Uuid,
    /// Original queue bytes used for atomic compare/reserve, never logged.
    pub queued_bytes: Vec<u8>,
    /// Exact stored bytes are retained for owner-checked ACK/restore.
    pub tuple_bytes: Vec<u8>,
    pub restored_bytes: Vec<u8>,
}
use crate::eta::{Budget, Pool};
#[derive(Clone)]
pub(crate) struct Candidate {
    pub(crate) receipt: std::sync::Arc<Receipt>,
    pub(crate) budget: std::sync::Arc<Budget>,
    pub(crate) pool: std::sync::Arc<Pool>,
}
#[derive(Clone)]
pub(crate) struct Reservation {
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
    pub fn message(&self) -> &Message {
        &self.receipt.message
    }
    pub fn delivery_tag(&self) -> Uuid {
        self.receipt.delivery_tag
    }
    pub fn tuple_bytes(&self) -> &[u8] {
        &self.receipt.tuple_bytes
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
    pub fn prepare(bytes: &[u8], immediate: bool) -> Result<Self, ProtocolError> {
        let original = bounded_json(bytes, crate::protocol::MAX_ENVELOPE)?;
        let mut normalized = original.clone();
        remove_redelivery(&mut normalized)?;
        let normalized = serde_json::to_vec(&normalized).map_err(|_| invalid())?;
        let message = if immediate {
            xml_tvg_protocol::parse(&normalized).and_then(|message| {
                if message.eta.is_none() {
                    Ok(message)
                } else {
                    Err(())
                }
            })
        } else {
            xml_tvg_protocol::parse_eta(&normalized)
        }
        .map_err(|_| invalid())?;
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
        // Visibility recovery may find a revoked immediate XML request left
        // unacked by an interrupted result write. Both paths use the same
        // canonical XML family parser and the exact stored tag/tuple fence.
        let immediate = parts[0]["headers"]["eta"].is_null();
        let mut receipt = Self::prepare(&raw, immediate)?;
        if receipt.delivery_tag != tag {
            return Err(invalid());
        }
        // Python and Rust JSON whitespace/escaping may differ. Owner comparison
        // uses the actual existing Redis bytes, not reconstructed serialization.
        receipt.tuple_bytes = bytes.to_vec();
        Ok(receipt)
    }
}
