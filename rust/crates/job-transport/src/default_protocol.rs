//! Bounded inert default-task library. Original Dispatcharr/Celery authors retain credit.
//! No Main registration or automatic intake; unsupported domains remain explicit.
use crate::{
    periodic_headers,
    protocol::{self, ProtocolError},
    worker,
};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    RedisBackendCleanup,
    RecurringMaintenance,
    InactiveAccountNotifications,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::RedisBackendCleanup => "celery.backend_cleanup",
            Self::InactiveAccountNotifications => "apps.m3u.tasks.check_account_expirations",
            Self::RecurringMaintenance => "apps.channels.tasks.maintain_recurring_recordings",
        }
    }
    fn from_name(name: &str) -> Option<Self> {
        [
            Self::RedisBackendCleanup,
            Self::RecurringMaintenance,
            Self::InactiveAccountNotifications,
        ]
        .into_iter()
        .find(|kind| kind.name() == name)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub task_id: Uuid,
    pub kind: Kind,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Normal {
    Null,
    Zero,
}
impl Normal {
    pub fn value(self, kind: Kind) -> Result<Value, ProtocolError> {
        match (kind, self) {
            (Kind::RecurringMaintenance, Self::Zero) => Ok(json!(0)),
            (Kind::RecurringMaintenance, _) => Err(ProtocolError::InvalidArgument),
            (_, Self::Null) => Ok(Value::Null),
            _ => Err(ProtocolError::InvalidArgument),
        }
    }
}
pub fn parse(raw: &[u8]) -> Result<Message, ProtocolError> {
    if raw.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut frame: Value =
        serde_json::from_slice(raw).map_err(|_| ProtocolError::InvalidArgument)?;
    periodic_headers::take(&mut frame)?;
    let kind = Kind::from_name(
        frame["headers"]["task"]
            .as_str()
            .ok_or(ProtocolError::InvalidArgument)?,
    )
    .ok_or(ProtocolError::InvalidArgument)?;
    if !matches!(frame["headers"]["argsrepr"].as_str(), Some("[]" | "()")) {
        return Err(ProtocolError::InvalidArgument);
    }
    // Reuse exact bounded common no-argument validation. Parented/ETA/expiry,
    // unknown options or bodies are refused before destructive claim.
    frame["headers"]["task"] = json!("apps.backups.tasks.create_backup_task");
    frame["headers"]["argsrepr"] = json!("()");
    let id = worker::create_identity(
        &serde_json::to_vec(&frame).map_err(|_| ProtocolError::InvalidArgument)?,
    )
    .map_err(|_| ProtocolError::InvalidArgument)?;
    Ok(Message { task_id: id, kind })
}
pub fn encode(message: &Message) -> Result<Vec<u8>, ProtocolError> {
    let raw = protocol::encode(
        &protocol::Task::CreateBackup,
        message.task_id,
        Uuid::new_v4(),
        Uuid::new_v4(),
        "rust@default-family-fixture",
    )?;
    let mut value: Value =
        serde_json::from_slice(&raw).map_err(|_| ProtocolError::InvalidArgument)?;
    value["headers"]["task"] = json!(message.kind.name());
    let bytes = serde_json::to_vec(&value).map_err(|_| ProtocolError::InvalidArgument)?;
    if parse(&bytes)? != *message {
        return Err(ProtocolError::InvalidArgument);
    }
    Ok(bytes)
}

// Prepared controls only: this private module has not been compiled/executed.
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_two_known_families_and_typed_normal_values() {
        for kind in [Kind::RedisBackendCleanup, Kind::RecurringMaintenance] {
            let message = Message {
                task_id: Uuid::new_v4(),
                kind,
            };
            assert_eq!(parse(&encode(&message).unwrap()), Ok(message.clone()));
            assert_eq!(
                Normal::Null.value(kind).is_ok(),
                kind == Kind::RedisBackendCleanup
            );
            assert_eq!(
                Normal::Zero.value(kind).is_ok(),
                kind == Kind::RecurringMaintenance
            );
            let good: Value = serde_json::from_slice(&encode(&message).unwrap()).unwrap();
            for (field, value) in [
                ("task", json!("core.tasks.scan_and_process_files")),
                ("parent_id", json!(Uuid::new_v4().to_string())),
                ("eta", json!("2026-11-11T04:00:00+00:00")),
                ("expires", json!("2026-11-12T04:00:00+00:00")),
                ("argsrepr", json!("[1]")),
            ] {
                let mut bad = good.clone();
                bad["headers"][field] = value;
                assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
            }
        }
    }
}

#[test]
fn actual_captured_two_family_direct_envelopes_and_typed_results() {
    let data: Value =
        serde_json::from_str(include_str!("../datasets/source-default-pair.json")).unwrap();
    for source in data["default_wire"].as_array().unwrap() {
        let raw = serde_json::to_vec(&source["envelope"]).unwrap();
        let parsed = parse(&raw).unwrap();
        assert_eq!(parsed.kind.name(), source["task"]);
        assert_eq!(
            parsed.task_id.to_string(),
            source["envelope"]["headers"]["id"]
        );
    }
    for source in data["worker_cases"].as_array().unwrap() {
        let expected = if source["task"] == Kind::RedisBackendCleanup.name() {
            Value::Null
        } else {
            json!(0)
        };
        assert_eq!(source["metadata"]["status"], "SUCCESS");
        assert_eq!(source["metadata"]["result"], expected);
    }
}
