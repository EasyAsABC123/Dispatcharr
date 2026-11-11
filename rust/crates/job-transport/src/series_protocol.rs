//! Actual observed no-argument SeriesRules wire and known no-recording returns.
//! Original Dispatcharr/Celery/Kombu authors retain full code/protocol credit.
//! No consumer, nonempty-rule execution or task publication authority here.
use crate::{
    decode::{self, State},
    protocol::{self, ProtocolError},
    worker,
};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeriesMessage {
    pub task_id: Uuid,
    pub root: Uuid,
    pub parent: Option<Uuid>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KnownReturn {
    NoRules,
    Busy,
}
impl KnownReturn {
    pub fn value(self) -> Value {
        match self {
            Self::NoRules => json!({"scheduled":0,"details":[]}),
            Self::Busy => {
                json!({"scheduled":0,"details":[{"status":"skipped","reason":"concurrent evaluation in progress"}]})
            }
        }
    }
}
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
pub fn parse(bytes: &[u8]) -> Result<SeriesMessage, ProtocolError> {
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut envelope: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    // Actual Source periodic worker34 preserves this bounded marker only in
    // task.request.headers; ordinary result metadata/body semantics are unchanged.
    crate::periodic_headers::take(&mut envelope)?;
    let headers = &envelope["headers"];
    if headers["task"] != "apps.channels.tasks.evaluate_series_rules" {
        return Err(invalid());
    }
    let uuid =
        |value: &Value| Uuid::parse_str(value.as_str().ok_or_else(invalid)?).map_err(|_| invalid());
    let task_id = uuid(&headers["id"])?;
    let root = uuid(&headers["root_id"])?;
    let parent = if headers["parent_id"].is_null() {
        None
    } else {
        Some(uuid(&headers["parent_id"])?)
    };
    if parent.is_none() && root != task_id {
        return Err(invalid());
    }
    // Preserve the known shared protocol2 options/body validation. Only the
    // observed family/lineage differ from the no-argument reference task.
    envelope["headers"]["task"] = json!("apps.backups.tasks.create_backup_task");
    envelope["headers"]["root_id"] = json!(task_id.to_string());
    envelope["headers"]["parent_id"] = Value::Null;
    worker::create_identity(&serde_json::to_vec(&envelope).map_err(|_| invalid())?)
        .map_err(|_| invalid())?;
    Ok(SeriesMessage {
        task_id,
        root,
        parent,
    })
}
pub fn parse_success(bytes: &[u8], message: &SeriesMessage) -> Result<KnownReturn, ProtocolError> {
    let record = decode::decode(bytes, message.task_id).map_err(|_| invalid())?;
    if record.state != State::Success
        || record.traceback.is_some()
        || record.date_done.is_none()
        || !record.tagged_values.is_empty()
    {
        return Err(invalid());
    }
    let raw: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    if raw["children"] != json!([]) {
        return Err(invalid());
    }
    let fields = raw.as_object().ok_or_else(invalid)?;
    let allowed = [
        "status",
        "result",
        "traceback",
        "children",
        "date_done",
        "task_id",
        "parent_id",
    ];
    if fields.len() != 6 + usize::from(message.parent.is_some())
        || fields.keys().any(|key| !allowed.contains(&key.as_str()))
    {
        return Err(invalid());
    }
    match message.parent {
        None if fields.contains_key("parent_id") => return Err(invalid()),
        Some(parent) if raw["parent_id"] != parent.to_string() => return Err(invalid()),
        _ => {}
    }
    [KnownReturn::NoRules, KnownReturn::Busy]
        .into_iter()
        .find(|value| record.result == value.value())
        .ok_or_else(invalid)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_original_noargs_and_parented_wire_validates_without_family_widening() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-series-rules-wire.json"))
                .unwrap();
        for row in source["cases"].as_array().unwrap() {
            let bytes = serde_json::to_vec(&row["envelope"]).unwrap();
            let message = parse(&bytes).unwrap();
            assert_eq!(message.parent.is_some(), row["name"] == "parented-empty");
            let mut foreign = row["envelope"].clone();
            foreign["headers"]["task"] = json!("apps.channels.tasks.run_recording");
            assert!(parse(&serde_json::to_vec(&foreign).unwrap()).is_err());
            let mut args = row["envelope"].clone();
            args["headers"]["argsrepr"] = json!("('unobserved',)");
            assert!(parse(&serde_json::to_vec(&args).unwrap()).is_err());
        }
        assert_eq!(
            parse(&vec![0; protocol::MAX_ENVELOPE + 1]),
            Err(ProtocolError::Capacity)
        );
    }
    #[test]
    fn only_observed_normal_zero_scheduled_returns_are_accepted() {
        let message = SeriesMessage {
            task_id: Uuid::nil(),
            root: Uuid::nil(),
            parent: None,
        };
        for value in [KnownReturn::NoRules, KnownReturn::Busy] {
            let record = json!({"status":"SUCCESS","result":value.value(),"traceback":null,"children":[],"date_done":"2026-01-01T00:00:00.000000+00:00","task_id":Uuid::nil().to_string()});
            assert_eq!(
                parse_success(&serde_json::to_vec(&record).unwrap(), &message),
                Ok(value)
            );
            let mut unproved = record.clone();
            unproved["result"]["scheduled"] = json!(1);
            assert!(parse_success(&serde_json::to_vec(&unproved).unwrap(), &message).is_err());
        }
    }
}
