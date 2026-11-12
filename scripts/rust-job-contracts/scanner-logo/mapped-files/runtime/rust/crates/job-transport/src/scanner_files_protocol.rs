//! Source217 observed scanner children only; no generic task/consumer authority.
//! Original Dispatcharr/Celery/Kombu authors retain code/algorithm credit.
use crate::{
    protocol::{self, ProtocolError, Task},
    refresh_protocol::{self, RefreshCall, RefreshMessage},
};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    M3uGroups,
    M3uSingle,
    EpgRefresh,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::M3uGroups => "apps.m3u.tasks.refresh_m3u_groups",
            Self::M3uSingle => "apps.m3u.tasks.refresh_single_m3u_account",
            Self::EpgRefresh => "apps.epg.tasks.refresh_epg_data",
        }
    }
    fn limits(self) -> Value {
        match self {
            Self::M3uGroups => json!([null, null]),
            Self::M3uSingle => json!([3600, 3500]),
            Self::EpgRefresh => json!([14400, null]),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Message {
    pub kind: Kind,
    pub object: i64,
    pub id: Uuid,
    pub parent: Uuid,
}
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
pub fn encode(
    message: Message,
    reply: Uuid,
    delivery: Uuid,
    origin: &str,
) -> Result<Vec<u8>, ProtocolError> {
    if message.object <= 0 || message.parent.is_nil() || message.id.is_nil() {
        return Err(invalid());
    }
    if message.kind == Kind::EpgRefresh {
        return refresh_protocol::encode(
            &RefreshMessage {
                task_id: message.id,
                source_id: message.object,
                force: false,
                retry: 0,
                root: message.parent,
                parent: Some(message.parent),
                eta: None,
                call: RefreshCall::initial(message.object),
            },
            reply,
            delivery,
            origin,
        );
    }
    let mut e: Value = serde_json::from_slice(&protocol::encode(
        &Task::CreateBackup,
        message.id,
        reply,
        delivery,
        origin,
    )?)
    .map_err(|_| invalid())?;
    e["headers"]["task"] = json!(message.kind.name());
    e["headers"]["root_id"] = json!(message.parent.to_string());
    e["headers"]["parent_id"] = json!(message.parent.to_string());
    e["headers"]["argsrepr"] = json!(format!("({},)", message.object));
    e["headers"]["timelimit"] = message.kind.limits();
    e["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([[message.object],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|invalid())?));
    let bytes = serde_json::to_vec(&e).map_err(|_| invalid())?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
/// This validates only our canonical encoder, not arbitrary Source options.
/// Actual Original-produced frames are a separate interoperability gate.
pub fn parse_native(bytes: &[u8]) -> Result<Message, ProtocolError> {
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let e: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let id =
        |value: &Value| Uuid::parse_str(value.as_str().ok_or_else(invalid)?).map_err(|_| invalid());
    let h = &e["headers"];
    let parent = id(&h["parent_id"])?;
    let body: Value = serde_json::from_slice(
        &STANDARD
            .decode(e["body"].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?,
    )
    .map_err(|_| invalid())?;
    let kind = match h["task"].as_str() {
        Some("apps.m3u.tasks.refresh_m3u_groups") => Kind::M3uGroups,
        Some("apps.m3u.tasks.refresh_single_m3u_account") => Kind::M3uSingle,
        Some("apps.epg.tasks.refresh_epg_data") => Kind::EpgRefresh,
        _ => return Err(invalid()),
    };
    let message = Message {
        kind,
        object: body[0][0].as_i64().ok_or_else(invalid)?,
        id: id(&h["id"])?,
        parent,
    };
    let expected = encode(
        message,
        id(&e["properties"]["reply_to"])?,
        id(&e["properties"]["delivery_tag"])?,
        h["origin"].as_str().ok_or_else(invalid)?,
    )?;
    if expected != bytes {
        return Err(invalid());
    }
    Ok(message)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_source217_children_match_all_typed_headers_bodies_and_relations() {
        let source: Value = serde_json::from_str(include_str!(
            "../datasets/source-scanner-file-branches-2.json"
        ))
        .unwrap();
        let mut count = 0;
        for case in source["cases"].as_array().unwrap() {
            for child in case["children"].as_array().unwrap() {
                let kind = match child["task"].as_str().unwrap() {
                    "apps.m3u.tasks.refresh_m3u_groups" => Kind::M3uGroups,
                    "apps.m3u.tasks.refresh_single_m3u_account" => Kind::M3uSingle,
                    "apps.epg.tasks.refresh_epg_data" => Kind::EpgRefresh,
                    _ => continue,
                };
                let parent = Uuid::parse_str(case["parent_id"].as_str().unwrap()).unwrap();
                let m = Message {
                    kind,
                    object: child["body"][0][0].as_i64().unwrap(),
                    parent,
                    id: Uuid::new_v4(),
                };
                let bytes =
                    encode(m, Uuid::new_v4(), Uuid::new_v4(), "rust@scanner-files").unwrap();
                assert_eq!(parse_native(&bytes).unwrap(), m);
                let e: Value = serde_json::from_slice(&bytes).unwrap();
                let body: Value =
                    serde_json::from_slice(&STANDARD.decode(e["body"].as_str().unwrap()).unwrap())
                        .unwrap();
                assert_eq!(body, child["body"]);
                for name in [
                    "lang",
                    "task",
                    "shadow",
                    "eta",
                    "expires",
                    "group",
                    "group_index",
                    "retries",
                    "timelimit",
                    "root_id",
                    "parent_id",
                    "argsrepr",
                    "kwargsrepr",
                    "ignore_result",
                    "replaced_task_nesting",
                    "stamped_headers",
                    "stamps",
                ] {
                    assert_eq!(
                        e["headers"][name], child["headers"][name],
                        "{} {name}",
                        case["name"]
                    )
                }
                count += 1;
            }
        }
        assert!(count >= 20);
    }
    #[test]
    fn unknown_child_options_or_crossfield_mutations_reject_before_publication() {
        for kind in [Kind::M3uGroups, Kind::M3uSingle, Kind::EpgRefresh] {
            let m = Message {
                kind,
                object: 1,
                id: Uuid::new_v4(),
                parent: Uuid::new_v4(),
            };
            let bytes = encode(m, Uuid::new_v4(), Uuid::new_v4(), "rust@scanner-files").unwrap();
            for field in ["task", "root_id", "parent_id", "eta", "expires", "argsrepr"] {
                let mut value: Value = serde_json::from_slice(&bytes).unwrap();
                value["headers"][field] = json!("unqualified");
                assert!(parse_native(&serde_json::to_vec(&value).unwrap()).is_err());
            }
            let mut bad = m;
            bad.object = 0;
            assert!(encode(bad, Uuid::new_v4(), Uuid::new_v4(), "rust@scanner-files").is_err())
        }
    }
}
