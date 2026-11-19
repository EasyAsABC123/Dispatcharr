//! Natural refresh_vod_content root/actual M3U child envelope.
//! Original Dispatcharr/Celery/Kombu authors retain credit.
use crate::{protocol, stats_protocol};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use uuid::Uuid;
pub(crate) const TASK: &str = "apps.vod.tasks.refresh_vod_content";
pub(crate) struct Message {
    pub id: Uuid,
    pub root: Uuid,
    pub parent: Option<Uuid>,
    pub account: i64,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    if frame["headers"]["task"] != TASK {
        return Err(());
    }
    let body = STANDARD
        .decode(frame["body"].as_str().ok_or(())?)
        .map_err(|_| ())?;
    if body.len() > 4096 {
        return Err(());
    }
    let body = crate::clocked_recording_envelope::json_value(&body).map_err(|_| ())?;
    if body.as_array().is_none_or(|body| body.len() != 3)
        || body[0].as_array().is_none_or(|args| args.len() != 1)
        || body[1] != json!({})
        || body[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
    {
        return Err(());
    }
    let account = body[0][0]
        .as_i64()
        .filter(|id| *id > 0 && *id <= i64::from(i32::MAX))
        .ok_or(())?;
    let repr = frame["headers"]["argsrepr"].as_str().ok_or(())?;
    if repr != format!("({account},)") && repr != format!("[{account}]") {
        return Err(());
    }
    let id = Uuid::parse_str(frame["headers"]["id"].as_str().ok_or(())?).map_err(|_| ())?;
    let root = Uuid::parse_str(frame["headers"]["root_id"].as_str().ok_or(())?).map_err(|_| ())?;
    let parent = match &frame["headers"]["parent_id"] {
        Value::Null => None,
        Value::String(parent) => Some(Uuid::parse_str(parent).map_err(|_| ())?),
        _ => return Err(()),
    };
    if id.is_nil()
        || root.is_nil()
        || parent.is_some_and(|parent| parent.is_nil() || parent == id)
        || (parent.is_none() && root != id)
    {
        return Err(());
    }
    frame["headers"]["parent_id"] = Value::Null;
    frame["headers"]["root_id"] = json!(id);
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    frame["headers"]["argsrepr"] = json!("()");
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ())?
    ));
    stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?).map_err(|_| ())?;
    Ok(Message {
        id,
        root,
        parent,
        account,
    })
}
pub(crate) fn encode(
    account: i64,
    identity: &dispatcharr_store::ProducerIdentity,
) -> Result<Vec<u8>, ()> {
    encode_with_parent(account, identity, None)
}

pub(crate) fn encode_child(
    account: i64,
    identity: &dispatcharr_store::ProducerIdentity,
    parent: Uuid,
    root: Uuid,
) -> Result<Vec<u8>, ()> {
    encode_with_parent(account, identity, Some((parent, root)))
}

fn encode_with_parent(
    account: i64,
    identity: &dispatcharr_store::ProducerIdentity,
    lineage: Option<(Uuid, Uuid)>,
) -> Result<Vec<u8>, ()> {
    let id = Uuid::new_v4();
    let bytes = protocol::encode(
        &protocol::Task::CreateBackup,
        id,
        identity.reply_to,
        Uuid::new_v4(),
        &identity.origin,
    )
    .map_err(|_| ())?;
    let mut frame: Value = serde_json::from_slice(&bytes).map_err(|_| ())?;
    frame["headers"]["task"] = json!(TASK);
    if let Some((parent, root)) = lineage {
        frame["headers"]["parent_id"] = json!(parent);
        frame["headers"]["root_id"] = json!(root);
    }
    frame["headers"]["argsrepr"] = json!(format!("({account},)"));
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[account],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| ())?
    ));
    let bytes = serde_json::to_vec(&frame).map_err(|_| ())?;
    let _ = parse(&bytes)?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn child_carries_the_publishing_task_and_its_root() {
        let identity = dispatcharr_store::ProducerIdentity {
            reply_to: Uuid::new_v4(),
            origin: "owned-vod-producer".into(),
        };
        let parent = Uuid::new_v4();
        let root = Uuid::new_v4();
        let message = parse(&encode_child(5, &identity, parent, root).unwrap()).unwrap();
        assert_eq!(message.account, 5);
        assert_eq!(message.parent, Some(parent));
        assert_eq!(message.root, root);
        assert_ne!(message.id, parent);
        assert!(encode_child(5, &identity, Uuid::nil(), root).is_err());
    }

    #[test]
    fn natural_root_rejects_replacement_options_and_preserves_owned_account() {
        let identity = dispatcharr_store::ProducerIdentity {
            reply_to: Uuid::new_v4(),
            origin: "owned-vod-producer".into(),
        };
        let raw = encode(5, &identity).unwrap();
        let parsed = parse(&raw).unwrap();
        assert_eq!(parsed.account, 5);
        assert_eq!(parsed.root, parsed.id);
        assert!(parsed.parent.is_none());
        for field in ["id", "root_id", "timelimit", "task"] {
            let mut changed: Value = serde_json::from_slice(&raw).unwrap();
            changed["headers"].as_object_mut().unwrap().remove(field);
            assert!(
                parse(&serde_json::to_vec(&changed).unwrap()).is_err(),
                "{field}"
            );
        }
    }
}
