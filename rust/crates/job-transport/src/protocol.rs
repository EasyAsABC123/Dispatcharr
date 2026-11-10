//! Supported Source protocol2 writer, independent of dispatch/authentication.
//! Original Dispatcharr authors retain application/integration credit and AGPL licensing.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use uuid::Uuid;

pub const MAX_ENVELOPE: usize = 65536;
#[derive(Clone, Debug)]
pub enum Task {
    CreateBackup,
    RestoreBackup { filename: String },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Published {
    pub task_id: Uuid,
    pub queue_length: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidArgument,
    Capacity,
}
fn filename_repr(value: &str) -> Result<String, ProtocolError> {
    if value.is_empty()
        || value.len() > 255
        || value.contains(['/', '\\', '\0'])
        || value.contains("..")
    {
        return Err(ProtocolError::InvalidArgument);
    }
    // Scope this first component to the observed safe filename family. General
    // Python repr quoting/control semantics require their own Source vectors.
    if value.contains(['\'', '"']) || value.chars().any(char::is_control) {
        return Err(ProtocolError::InvalidArgument);
    }
    Ok(format!("('{value}',)"))
}
pub fn encode(
    task: &Task,
    id: Uuid,
    reply_to: Uuid,
    delivery_tag: Uuid,
    origin: &str,
) -> Result<Vec<u8>, ProtocolError> {
    if origin.is_empty() || origin.len() > 128 || origin.chars().any(char::is_control) {
        return Err(ProtocolError::InvalidArgument);
    }
    let (name, args, repr) = match task {
        Task::CreateBackup => (
            "apps.backups.tasks.create_backup_task",
            json!([]),
            "()".to_owned(),
        ),
        Task::RestoreBackup { filename } => (
            "apps.backups.tasks.restore_backup_task",
            json!([filename]),
            filename_repr(filename)?,
        ),
    };
    let payload = json!([args,{}, {"callbacks":null,"errbacks":null,"chain":null,"chord":null}]);
    let body =
        STANDARD.encode(serde_json::to_vec(&payload).map_err(|_| ProtocolError::InvalidArgument)?);
    let envelope = json!({"body":body,"content-encoding":"utf-8","content-type":"application/json",
        "headers":{"lang":"py","task":name,"id":id.to_string(),"shadow":null,"eta":null,"expires":null,
            "group":null,"group_index":null,"retries":0,"timelimit":[null,null],"root_id":id.to_string(),
            "parent_id":null,"argsrepr":repr,"kwargsrepr":"{}","origin":origin,"ignore_result":false,
            "replaced_task_nesting":0,"stamped_headers":null,"stamps":{}},
        "properties":{"correlation_id":id.to_string(),"reply_to":reply_to.to_string(),"delivery_mode":2,
            "delivery_info":{"exchange":"","routing_key":"celery"},"priority":0,"body_encoding":"base64",
            "delivery_tag":delivery_tag.to_string()}});
    let bytes = serde_json::to_vec(&envelope).map_err(|_| ProtocolError::InvalidArgument)?;
    if bytes.len() > MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
// Encoding whitespace/escaping is intentionally not claimed byte-identical to
// Python's JSON serializer. Decoded types/values, routing and Source execution
// interoperability must be verified before enabling even component acceptance.

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn supported_wire_values_and_identity_relations_match_observed_source() {
        let id = Uuid::new_v4();
        let reply = Uuid::new_v4();
        let delivery = Uuid::new_v4();
        for task in [
            Task::CreateBackup,
            Task::RestoreBackup {
                filename: "owned invalid-é &+=.zip".to_owned(),
            },
        ] {
            let bytes = encode(&task, id, reply, delivery, "fixture-origin").unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let body: serde_json::Value =
                serde_json::from_slice(&STANDARD.decode(value["body"].as_str().unwrap()).unwrap())
                    .unwrap();
            assert_eq!(value["headers"]["id"], id.to_string());
            assert_eq!(value["headers"]["root_id"], id.to_string());
            assert_eq!(value["properties"]["correlation_id"], id.to_string());
            assert_eq!(value["properties"]["delivery_tag"], delivery.to_string());
            assert_eq!(
                value["properties"]["delivery_info"],
                json!({"exchange":"","routing_key":"celery"})
            );
            assert_eq!(body[1], json!({}));
            assert_eq!(
                body[2],
                json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null})
            );
            match task {
                Task::CreateBackup => assert_eq!(body[0], json!([])),
                Task::RestoreBackup { filename } => assert_eq!(body[0], json!([filename])),
            }
        }
    }
    #[test]
    fn unsupported_path_or_repr_shapes_are_rejected_before_publication() {
        for filename in [
            "../bad",
            "slash/name",
            "back\\slash",
            "quote'",
            "quote\"",
            "nul\0",
            "",
        ] {
            assert_eq!(
                encode(
                    &Task::RestoreBackup {
                        filename: filename.to_owned()
                    },
                    Uuid::nil(),
                    Uuid::nil(),
                    Uuid::nil(),
                    "fixture"
                ),
                Err(ProtocolError::InvalidArgument)
            );
        }
    }
}
