//! Captured no-argument Source version task; no generic family authority.
//! Original Dispatcharr/Celery authors retain all protocol/application credit.
use crate::{protocol::ProtocolError, stats_protocol};
use serde_json::{json, Value};

pub(crate) const TASK: &str = "core.tasks.check_for_version_update";
pub(crate) const STARTUP_LOCK: &str = "core:version_check_dispatch_lock";
pub(crate) const STARTUP_LOCK_SECONDS: u32 = 300;

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
    if frame["headers"]["task"] != TASK {
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
    fn ordinary_startup_frame_preserves_the_source_root_and_default_route() {
        use base64::{engine::general_purpose::STANDARD, Engine};

        let id = uuid::Uuid::new_v4();
        let reply_to = uuid::Uuid::new_v4();
        let bytes =
            crate::normal_recording_publication::startup_frame(TASK, id, reply_to, "gen1@owned")
                .unwrap();
        assert_eq!(parse(&bytes).unwrap().task_id, id);
        let raw: Value = serde_json::from_slice(&bytes).unwrap();
        let protocol: Value =
            serde_json::from_slice(&STANDARD.decode(raw["body"].as_str().unwrap()).unwrap())
                .unwrap();
        assert_eq!(raw["headers"]["root_id"], id.to_string());
        assert_eq!(raw["headers"]["parent_id"], Value::Null);
        assert_eq!(raw["properties"]["reply_to"], reply_to.to_string());
        assert_eq!(raw["properties"]["delivery_info"]["routing_key"], "celery");
        assert_eq!(protocol[0], json!([]));
        assert_eq!(protocol[1], json!({}));
    }

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
