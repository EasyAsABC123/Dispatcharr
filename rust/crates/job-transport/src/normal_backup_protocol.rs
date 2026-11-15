//! Normal backup roots/parented tasks; Original Dispatcharr/Celery credit retained.
use crate::{
    periodic_headers,
    protocol::{ProtocolError, MAX_ENVELOPE},
    worker,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub(crate) enum Kind {
    Create,
    Scheduled(Value),
}
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
    pub parent: Option<Uuid>,
    pub kind: Kind,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ProtocolError> {
    if raw.len() > MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut frame: Value =
        serde_json::from_slice(raw).map_err(|_| ProtocolError::InvalidArgument)?;
    periodic_headers::take(&mut frame)?;
    let name = frame["headers"]["task"]
        .as_str()
        .ok_or(ProtocolError::InvalidArgument)?
        .to_owned();
    let id = Uuid::parse_str(
        frame["headers"]["id"]
            .as_str()
            .ok_or(ProtocolError::InvalidArgument)?,
    )
    .map_err(|_| ProtocolError::InvalidArgument)?;
    let parent = match &frame["headers"]["parent_id"] {
        Value::Null => None,
        Value::String(v) => Some(Uuid::parse_str(v).map_err(|_| ProtocolError::InvalidArgument)?),
        _ => return Err(ProtocolError::InvalidArgument),
    };
    let _root = Uuid::parse_str(
        frame["headers"]["root_id"]
            .as_str()
            .ok_or(ProtocolError::InvalidArgument)?,
    )
    .map_err(|_| ProtocolError::InvalidArgument)?;
    let decoded = STANDARD
        .decode(
            frame["body"]
                .as_str()
                .ok_or(ProtocolError::InvalidArgument)?,
        )
        .map_err(|_| ProtocolError::InvalidArgument)?;
    if decoded.len() > MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut body: Value =
        serde_json::from_slice(&decoded).map_err(|_| ProtocolError::InvalidArgument)?;
    let list = body
        .as_array()
        .filter(|v| v.len() == 3)
        .ok_or(ProtocolError::InvalidArgument)?;
    let args = list[0].as_array().ok_or(ProtocolError::InvalidArgument)?;
    let kwargs = list[1].as_object().ok_or(ProtocolError::InvalidArgument)?;
    let kind = match name.as_str() {
        "apps.backups.tasks.create_backup_task" if args.is_empty() && kwargs.is_empty() => {
            Kind::Create
        }
        "apps.backups.tasks.scheduled_backup_task"
            if args.len() <= 1
                && kwargs.len() <= 1
                && kwargs.keys().all(|v| v == "retention_count")
                && !(args.len() == 1 && kwargs.contains_key("retention_count")) =>
        {
            Kind::Scheduled(
                args.first()
                    .or_else(|| kwargs.get("retention_count"))
                    .cloned()
                    .unwrap_or(json!(0)),
            )
        }
        _ => return Err(ProtocolError::InvalidArgument),
    };
    // Common shape validation only: exact original bytes remain the CAS operand.
    // Logging repr and validated lineage do not grant claim/publication authority.
    body[0] = json!([]);
    body[1] = json!({});
    frame["body"] = json!(
        STANDARD.encode(serde_json::to_vec(&body).map_err(|_| ProtocolError::InvalidArgument)?)
    );
    frame["headers"]["task"] = json!("apps.backups.tasks.create_backup_task");
    frame["headers"]["argsrepr"] = json!("()");
    frame["headers"]["kwargsrepr"] = json!("{}");
    frame["headers"]["parent_id"] = Value::Null;
    frame["headers"]["root_id"] = json!(id.to_string());
    let validated = worker::create_identity(
        &serde_json::to_vec(&frame).map_err(|_| ProtocolError::InvalidArgument)?,
    )
    .map_err(|_| ProtocolError::InvalidArgument)?;
    if validated != id {
        return Err(ProtocolError::InvalidArgument);
    }
    Ok(Message { id, parent, kind })
}
