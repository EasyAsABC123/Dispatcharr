//! Captured no-argument Source version task; no generic family authority.
//! Original Dispatcharr/Celery authors retain all protocol/application credit.
use crate::{protocol::ProtocolError, stats_protocol};
use serde_json::{json, Value};
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub(crate) task_id: uuid::Uuid,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ProtocolError> {
    if raw.len() > 65536 {
        return Err(ProtocolError::Capacity);
    }
    let mut frame: Value =
        serde_json::from_slice(raw).map_err(|_| ProtocolError::InvalidArgument)?;
    if frame["headers"]["task"] != "core.tasks.check_for_version_update" {
        return Err(ProtocolError::InvalidArgument);
    }
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    // Reuse the qualified common no-arg periodic envelope checks on a validation
    // copy. Original bytes remain untouched for the eventual exact FIFO CAS.
    let checked = stats_protocol::parse(
        &serde_json::to_vec(&frame).map_err(|_| ProtocolError::InvalidArgument)?,
    )?;
    Ok(Message {
        task_id: checked.task_id,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_default_version_frame_admits_only_its_original_family() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-default-stats-wire.json"))
                .unwrap();
        for row in source["defaults"].as_array().unwrap() {
            let raw = serde_json::to_vec(&row["envelope"]).unwrap();
            assert_eq!(
                parse(&raw).is_ok(),
                row["task"] == "core.tasks.check_for_version_update"
            );
        }
    }
}
