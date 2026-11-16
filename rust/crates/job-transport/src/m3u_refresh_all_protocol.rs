//! Private aggregate refresh protocol draft. Original Celery/Dispatcharr credit.
use crate::{protocol, stats_protocol};
use base64::{engine::general_purpose::STANDARD, Engine};
use dispatcharr_store::ProducerIdentity;
use serde_json::{json, Value};
use uuid::Uuid;

pub(crate) enum Terminal {
    Success(Value),
    Revoked,
    Failure(String),
}

pub(crate) const TASK: &str = "apps.m3u.tasks.refresh_m3u_accounts";

/// Restricted to actual HTTP/Beat roots; unsupported descendant roots remain
/// unclaimed. Common parser validates body, delivery/properties and no ETA,
/// expires/canvas replacement; this does not create weaker envelope authority.
pub(crate) fn parse(bytes: &[u8]) -> Result<Uuid, protocol::ProtocolError> {
    use protocol::ProtocolError::{Capacity, InvalidArgument};
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(Capacity);
    }
    let mut frame =
        crate::clocked_recording_envelope::json_value(bytes).map_err(|_| InvalidArgument)?;
    if frame["headers"]["task"] != TASK {
        return Err(InvalidArgument);
    }
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| InvalidArgument)?)
        .map(|message| message.task_id)
}

pub(crate) fn encode_root(
    id: Uuid,
    identity: &ProducerIdentity,
) -> Result<Vec<u8>, protocol::ProtocolError> {
    use protocol::ProtocolError::InvalidArgument;
    let encoded = protocol::encode(
        &protocol::Task::CreateBackup,
        id,
        identity.reply_to,
        Uuid::new_v4(),
        &identity.origin,
    )?;
    let mut frame: Value = serde_json::from_slice(&encoded).map_err(|_| InvalidArgument)?;
    frame["headers"]["task"] = json!(TASK);
    let bytes = serde_json::to_vec(&frame).map_err(|_| InvalidArgument)?;
    if parse(&bytes)? != id {
        return Err(InvalidArgument);
    }
    Ok(bytes)
}

/// Construct each child only after the aggregate was actually claimed. The
/// new narrow Jobs publication port must validate this exact lineage AND the
/// existing single-account parser, then record the acknowledged UUID in order.
pub(crate) fn encode_child(
    parent: Uuid,
    account: i64,
    identity: &ProducerIdentity,
) -> Result<(Uuid, Vec<u8>), protocol::ProtocolError> {
    use protocol::ProtocolError::InvalidArgument;
    if parent.is_nil() || !(1..=i64::from(i32::MAX)).contains(&account) {
        return Err(InvalidArgument);
    }
    let id = Uuid::new_v4();
    let encoded = protocol::encode(
        &protocol::Task::CreateBackup,
        id,
        identity.reply_to,
        Uuid::new_v4(),
        &identity.origin,
    )?;
    let mut frame: Value = serde_json::from_slice(&encoded).map_err(|_| InvalidArgument)?;
    frame["headers"]["task"] = json!("apps.m3u.tasks.refresh_single_m3u_account");
    frame["headers"]["root_id"] = json!(parent);
    frame["headers"]["parent_id"] = json!(parent);
    frame["headers"]["argsrepr"] = json!(format!("({account},)"));
    frame["headers"]["timelimit"] = json!([3600, 3500]);
    frame["body"] = json!(STANDARD.encode(
        serde_json::to_vec(&json!([
            [account], {},
            {"callbacks":null,"errbacks":null,"chain":null,"chord":null}
        ]))
        .map_err(|_| InvalidArgument)?
    ));
    let bytes = serde_json::to_vec(&frame).map_err(|_| InvalidArgument)?;
    let observed = crate::normal_xc_live::parse(&bytes).map_err(|_| InvalidArgument)?;
    if observed.id != id || observed.parent != Some(parent) || observed.root != parent {
        return Err(InvalidArgument);
    }
    Ok((id, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_root_and_child_contracts_do_not_grant_foreign_lineage() {
        let identity = ProducerIdentity {
            reply_to: Uuid::new_v4(),
            origin: "rust@aggregate-peer".into(),
        };
        let parent = Uuid::new_v4();
        let root = encode_root(parent, &identity).unwrap();
        assert_eq!(parse(&root).unwrap(), parent);
        let (_, child) = encode_child(parent, 7, &identity).unwrap();
        assert!(parse(&child).is_err());
        let actual = crate::normal_xc_live::parse(&child).unwrap();
        assert_eq!(
            (actual.root, actual.parent, actual.account),
            (parent, Some(parent), 7)
        );
        // Local-file STD fallback uses the authenticated broker scope as well.
        let local = crate::scanner_m3u_protocol::parse(&child).unwrap();
        assert_eq!(
            (local.root, local.parent, local.account),
            (parent, parent, 7)
        );
        let mut foreign: Value = serde_json::from_slice(&root).unwrap();
        foreign["headers"]["root_id"] = json!(Uuid::new_v4());
        assert!(parse(&serde_json::to_vec(&foreign).unwrap()).is_err());
        let mut replacement: Value = serde_json::from_slice(&root).unwrap();
        replacement["body"] = json!(STANDARD.encode(
            serde_json::to_vec(
                &json!([[],{},{"callbacks":[],"errbacks":null,"chain":null,"chord":null}])
            )
            .unwrap()
        ));
        assert!(parse(&serde_json::to_vec(&replacement).unwrap()).is_err());
    }
}
