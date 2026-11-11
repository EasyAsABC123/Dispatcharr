//! Observed no-argument fetch-channel-stats protocol, fixture-only authority.
//! Original Dispatcharr/Celery authors retain application/protocol credit.
use crate::{periodic_headers, protocol, worker};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Message {
    pub task_id: Uuid,
}
pub(crate) fn parse(bytes: &[u8]) -> Result<Message, protocol::ProtocolError> {
    use protocol::ProtocolError::{Capacity, InvalidArgument};
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(Capacity);
    }
    let mut envelope: Value = serde_json::from_slice(bytes).map_err(|_| InvalidArgument)?;
    periodic_headers::take(&mut envelope)?;
    if envelope["headers"]["task"] != "apps.proxy.tasks.fetch_channel_stats"
        || !matches!(envelope["headers"]["argsrepr"].as_str(), Some("[]" | "()"))
    {
        return Err(InvalidArgument);
    }
    envelope["headers"]["task"] = json!("apps.backups.tasks.create_backup_task");
    envelope["headers"]["argsrepr"] = json!("()");
    let id = worker::create_identity(&serde_json::to_vec(&envelope).map_err(|_| InvalidArgument)?)
        .map_err(|_| InvalidArgument)?;
    Ok(Message { task_id: id })
}
pub(crate) fn encode(id: Uuid) -> Result<Vec<u8>, protocol::ProtocolError> {
    let encoded = protocol::encode(
        &protocol::Task::CreateBackup,
        id,
        Uuid::new_v4(),
        Uuid::new_v4(),
        "rust@fetch-stats-fixture",
    )?;
    let mut envelope: Value =
        serde_json::from_slice(&encoded).map_err(|_| protocol::ProtocolError::InvalidArgument)?;
    envelope["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    let bytes =
        serde_json::to_vec(&envelope).map_err(|_| protocol::ProtocolError::InvalidArgument)?;
    if parse(&bytes)? != (Message { task_id: id }) {
        return Err(protocol::ProtocolError::InvalidArgument);
    }
    Ok(bytes)
}

#[test]
fn actual_default_stats_wire_validates_without_other_family_authority() {
    let captured: Value =
        serde_json::from_str(include_str!("../datasets/source-default-stats-wire.json")).unwrap();
    let source = captured["defaults"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["task"] == "apps.proxy.tasks.fetch_channel_stats")
        .unwrap();
    let bytes = serde_json::to_vec(&source["envelope"]).unwrap();
    let message = parse(&bytes).unwrap();
    assert_eq!(
        message.task_id.to_string(),
        source["envelope"]["headers"]["id"]
    );
    assert_eq!(parse(&encode(message.task_id).unwrap()), Ok(message));
    for row in captured["defaults"].as_array().unwrap() {
        let accepted = parse(&serde_json::to_vec(&row["envelope"]).unwrap()).is_ok();
        assert_eq!(
            accepted,
            row["task"] == "apps.proxy.tasks.fetch_channel_stats"
        );
    }
    for (field, value) in [
        ("eta", json!("2026-11-11T04:00:00+00:00")),
        ("expires", json!("2026-11-11T04:00:00+00:00")),
        ("parent_id", json!(Uuid::new_v4().to_string())),
    ] {
        let mut envelope = source["envelope"].clone();
        envelope["headers"][field] = value;
        assert!(parse(&serde_json::to_vec(&envelope).unwrap()).is_err());
    }
}
