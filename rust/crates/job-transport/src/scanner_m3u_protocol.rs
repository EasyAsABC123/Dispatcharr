//! Only the two observed scanner-created, immediate parented standard-M3U calls.
//! Original Dispatcharr/Celery/Kombu authors retain protocol/algorithm credit.
use crate::{
    protocol::ProtocolError,
    refresh_protocol::{self, RefreshCall},
};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    Groups,
    Single,
}
impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Groups => "apps.m3u.tasks.refresh_m3u_groups",
            Self::Single => "apps.m3u.tasks.refresh_single_m3u_account",
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Message {
    pub id: Uuid,
    pub root: Uuid,
    pub parent: Uuid,
    pub account: i64,
    pub kind: Kind,
}
#[derive(Clone, Debug)]
pub struct Scope {
    parent: Uuid,
    accounts: Vec<i64>,
}
impl Scope {
    /// Trusted scanner-assembly authority, never an untrusted HTTP request.
    pub fn new(parent: Uuid, accounts: Vec<i64>) -> Result<Self, ProtocolError> {
        if parent.is_nil()
            || accounts.is_empty()
            || accounts.len() > 4
            || accounts.iter().any(|v| *v <= 0 || *v > i64::from(i32::MAX))
            || accounts
                .iter()
                .enumerate()
                .any(|(n, v)| accounts[..n].contains(v))
        {
            return Err(ProtocolError::InvalidArgument);
        }
        Ok(Self { parent, accounts })
    }
    pub(crate) fn parent(&self) -> Uuid {
        self.parent
    }
    pub(crate) fn accepts(&self, m: &Message) -> bool {
        m.parent == self.parent && m.root == self.parent && self.accounts.contains(&m.account)
    }
}
pub fn parse(bytes: &[u8]) -> Result<Message, ProtocolError> {
    if bytes.len() > crate::protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut frame: Value =
        serde_json::from_slice(bytes).map_err(|_| ProtocolError::InvalidArgument)?;
    let kind = match frame["headers"]["task"].as_str() {
        Some("apps.m3u.tasks.refresh_m3u_groups") => Kind::Groups,
        Some("apps.m3u.tasks.refresh_single_m3u_account") => Kind::Single,
        _ => return Err(ProtocolError::InvalidArgument),
    };
    let expected = match kind {
        Kind::Groups => json!([null, null]),
        Kind::Single => json!([3600, 3500]),
    };
    if !frame["headers"]["expires"].is_null()
        || frame["properties"].get("expiration").is_some()
        || frame["headers"]["timelimit"] != expected
        || frame["headers"].get("periodic_task_name").is_some()
    {
        return Err(ProtocolError::InvalidArgument);
    }
    // Reuse the strict common frame validator; only two proven task/timelimit
    // literals are transformed. Unknown headers/body/options remain unmodified.
    frame["headers"]["task"] = json!("apps.epg.tasks.refresh_epg_data");
    frame["headers"]["timelimit"] = json!([14400, null]);
    let value = refresh_protocol::parse(
        &serde_json::to_vec(&frame).map_err(|_| ProtocolError::InvalidArgument)?,
    )?;
    let parent = value.parent.ok_or(ProtocolError::InvalidArgument)?;
    if value.eta.is_some()
        || value.force
        || value.retry != 0
        || value.call != RefreshCall::initial(value.source_id)
        || value.root != parent
        || value.source_id > i64::from(i32::MAX)
    {
        return Err(ProtocolError::InvalidArgument);
    }
    Ok(Message {
        id: value.task_id,
        root: value.root,
        parent,
        account: value.source_id,
        kind,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source217_both_scanner_children_and_mutations() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../datasets/source-scanner-file-branches-2.json"
        ))
        .unwrap();
        let case = corpus["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["name"] == "bool-true")
            .unwrap();
        for kind in [Kind::Groups, Kind::Single] {
            let frame = case["after"]["queue"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| &v["message"])
                .find(|v| v["headers"]["task"] == kind.name())
                .unwrap()
                .clone();
            let message = parse(&serde_json::to_vec(&frame).unwrap()).unwrap();
            assert_eq!(message.kind, kind);
            assert!(Scope::new(message.parent, vec![message.account])
                .unwrap()
                .accepts(&message));
            for field in ["task", "timelimit", "parent_id", "root_id"] {
                let mut bad = frame.clone();
                bad["headers"].as_object_mut().unwrap().remove(field);
                assert!(
                    parse(&serde_json::to_vec(&bad).unwrap()).is_err(),
                    "{field}"
                );
            }
            let mut bad = frame.clone();
            bad["headers"]["eta"] = json!("2026-11-12T00:00:00+00:00");
            assert!(parse(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
        assert!(Scope::new(Uuid::new_v4(), vec![1, 1]).is_err());
    }
}
