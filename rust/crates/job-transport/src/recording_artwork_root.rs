//! Normal direct/root artwork task, additive to the qualified child protocol.
//! Original Dispatcharr/Celery authors retain protocol/application credit.
use crate::{clocked_recording_envelope::json_value, worker};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub(crate) task_id: Uuid,
    pub(crate) recording: i64,
    pub(crate) eta: Option<DateTime<Utc>>,
}
pub(crate) fn parse(raw: &[u8]) -> Option<Message> {
    let mut value = json_value(raw).ok()?;
    if value["headers"]["task"] != "apps.channels.tasks.prefetch_recording_artwork"
        || !value["headers"]["parent_id"].is_null()
    {
        return None;
    }
    let task_id = Uuid::parse_str(value["headers"]["id"].as_str()?).ok()?;
    if task_id.get_version_num() != 4
        || task_id.get_variant() != uuid::Variant::RFC4122
        || value["headers"]["root_id"] != task_id.to_string()
    {
        return None;
    }
    let body = json_value(&STANDARD.decode(value["body"].as_str()?).ok()?).ok()?;
    let recording = body.get(0)?.get(0)?.as_i64()?;
    if recording <= 0
        || body
            != json!([[recording],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        || value["headers"]["argsrepr"] != format!("[{recording}]")
    {
        return None;
    }
    let eta = if value["headers"]["eta"].is_null() {
        None
    } else {
        let eta = DateTime::parse_from_rfc3339(value["headers"]["eta"].as_str()?).ok()?;
        if eta.offset().local_minus_utc() != 0 {
            return None;
        }
        Some(eta.to_utc())
    };
    value["headers"]["task"] = json!("apps.backups.tasks.create_backup_task");
    value["headers"]["argsrepr"] = json!("()");
    value["headers"]["eta"] = Value::Null;
    value["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .ok()?
    ));
    if worker::create_identity(&serde_json::to_vec(&value).ok()?).ok() != Some(task_id) {
        return None;
    }
    Some(Message {
        task_id,
        recording,
        eta,
    })
}
#[cfg(test)]
mod controls {
    use super::*;
    #[test]
    fn root_and_captured_child_authority_remain_disjoint_and_unknown_callbacks_stay_refused() {
        let source: Value = serde_json::from_str(include_str!(
            "../datasets/source-local-recording-artwork.json"
        ))
        .unwrap();
        let raw = STANDARD
            .decode(source["frames_raw_base64"][0].as_str().unwrap())
            .unwrap();
        let mut child: Value = serde_json::from_slice(&raw).unwrap();
        child["properties"]["delivery_info"]["routing_key"] = json!("celery");
        let child_raw = serde_json::to_vec(&child).unwrap();
        assert!(parse(&child_raw).is_none());
        assert!(crate::recording_artwork_protocol::parse_normal(&child_raw).is_some());
        let mut root = child;
        root["headers"]["root_id"] = root["headers"]["id"].clone();
        root["headers"]["parent_id"] = Value::Null;
        let root_raw = serde_json::to_vec(&root).unwrap();
        assert!(parse(&root_raw).is_some());
        assert!(crate::recording_artwork_protocol::parse_normal(&root_raw).is_none());
        let mut body: Value =
            serde_json::from_slice(&STANDARD.decode(root["body"].as_str().unwrap()).unwrap())
                .unwrap();
        body[2]["callbacks"] = json!([{"task":"unadmitted.callback"}]);
        root["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).unwrap()));
        assert!(parse(&serde_json::to_vec(&root).unwrap()).is_none());
    }
}
