//! Actual captured Channel-signal producer only, not execution/ACK authority.
//! Original Dispatcharr/Celery authors retain application/protocol credit.
use crate::{index_protocol, protocol::ProtocolError};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub task_id: Uuid,
    pub epg_data_id: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UnqualifiedProgrammeExecution;
pub fn parse_initial(raw: &[u8]) -> Result<Envelope, ProtocolError> {
    let invalid = || ProtocolError::InvalidArgument;
    if raw.len() > 65536 {
        return Err(ProtocolError::Capacity);
    }
    let mut value: Value = serde_json::from_slice(raw).map_err(|_| invalid())?;
    let headers = &value["headers"];
    if headers["task"] != "apps.epg.tasks.parse_programs_for_tvg_id"
        || headers["timelimit"] != json!([3600, 3500])
        || !headers["parent_id"].is_null()
        || !headers["eta"].is_null()
        || headers["kwargsrepr"] != "{}"
        || headers["retries"] != 0
    {
        return Err(invalid());
    }
    let spelling = headers["id"].as_str().ok_or_else(invalid)?;
    let id = Uuid::parse_str(spelling).map_err(|_| invalid())?;
    if id.to_string() != spelling
        || id.get_version_num() != 4
        || id.get_variant() != uuid::Variant::RFC4122
        || headers["root_id"] != spelling
    {
        return Err(invalid());
    }
    // The already-qualified common envelope parser retains all body, header,
    // exchange and celery-routing constraints. Only this family's captured
    // timelimit is projected into that validation clone; original bytes remain
    // unchanged and never become a claim/retry/republication operation.
    value["headers"]["task"] = json!("apps.epg.tasks.build_programme_index_task");
    value["headers"]["timelimit"] = json!([null, null]);
    let clone = serde_json::to_vec(&value).map_err(|_| invalid())?;
    let message = index_protocol::parse(&clone)?;
    if message.explicit_retry
        || message.retry != 0
        || message.parent.is_some()
        || message.eta.is_some()
        || message.list_args_repr
        || !(1..=i64::from(i32::MAX)).contains(&message.source_id)
    {
        return Err(invalid());
    }
    Ok(Envelope {
        task_id: id,
        epg_data_id: message.source_id,
    })
}
impl Envelope {
    pub fn execution(&self) -> Result<(), UnqualifiedProgrammeExecution> {
        Err(UnqualifiedProgrammeExecution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};
    fn actual() -> Value {
        let receipt: Value =
            serde_json::from_str(include_str!("../datasets/tvg-channel-signal-source.json"))
                .unwrap();
        serde_json::from_slice(
            &STANDARD
                .decode(receipt["raw_frame_base64"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn captured_channel_signal_is_producer_only() {
        let raw = serde_json::to_vec(&actual()).unwrap();
        let parsed = parse_initial(&raw).unwrap();
        assert_eq!(parsed.epg_data_id, 1);
        assert_eq!(parsed.execution(), Err(UnqualifiedProgrammeExecution));
    }
    #[test]
    fn other_queue_and_inherited_identity_are_refused() {
        let mut frame = actual();
        frame["properties"]["delivery_info"]["routing_key"] = json!("other");
        assert!(parse_initial(&serde_json::to_vec(&frame).unwrap()).is_err());
        let mut frame = actual();
        frame["headers"]["parent_id"] = json!(frame["headers"]["id"]);
        assert!(parse_initial(&serde_json::to_vec(&frame).unwrap()).is_err());
    }
    #[test]
    fn unsupported_force_and_boolean_id_refuse() {
        for body in [
            json!([[1],{"force":true},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]),
            json!([[true],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]),
        ] {
            let mut frame = actual();
            frame["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
            assert!(parse_initial(&serde_json::to_vec(&frame).unwrap()).is_err());
        }
    }
}
