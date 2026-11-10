//! Bounded observed normal refresh result geometry, no worker/result writer.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use crate::{
    decode::{self, State},
    protocol::ProtocolError,
};
use serde_json::Value;
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshSuccess {
    pub task_id: Uuid,
    pub parent: Option<Uuid>,
    pub children: Vec<Uuid>,
    pub date_done: String,
    pub missing_source: bool,
}
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
/// Only actual SUCCESS/null or the exact missing-source cleanup string is
/// supported. This does not establish native orphan cleanup or exception policy.
pub fn parse_success(
    bytes: &[u8],
    task_id: Uuid,
    source_id: i64,
) -> Result<RefreshSuccess, ProtocolError> {
    if bytes.len() > decode::MAX_RESULT_BYTES {
        return Err(ProtocolError::Capacity);
    }
    if source_id <= 0 {
        return Err(invalid());
    }
    let m = decode::decode(bytes, task_id).map_err(|_| invalid())?;
    if m.state != State::Success || m.traceback.is_some() || !m.tagged_values.is_empty() {
        return Err(invalid());
    }
    let missing_source = if m.result.is_null() {
        false
    } else if m.result == format!("EPG source {source_id} does not exist, task cleaned up") {
        true
    } else {
        return Err(invalid());
    };
    let e: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let fields = e.as_object().ok_or_else(invalid)?;
    let allowed = [
        "status",
        "result",
        "traceback",
        "children",
        "date_done",
        "task_id",
        "parent_id",
    ];
    if fields.len() != 6 + usize::from(fields.contains_key("parent_id"))
        || fields.keys().any(|k| !allowed.contains(&k.as_str()))
    {
        return Err(invalid());
    }
    let parent = match fields.get("parent_id") {
        None => None,
        Some(Value::String(v)) => Some(Uuid::parse_str(v).map_err(|_| invalid())?),
        _ => return Err(invalid()),
    };
    let date_done = m
        .date_done
        .filter(|v| {
            v.len() <= 64
                && v.ends_with("+00:00")
                && chrono::DateTime::parse_from_rfc3339(v).is_ok()
        })
        .ok_or_else(invalid)?;
    let rows = e["children"]
        .as_array()
        .filter(|v| v.len() <= 64)
        .ok_or_else(invalid)?;
    let mut children = Vec::with_capacity(rows.len());
    for row in rows {
        let pair = row
            .as_array()
            .filter(|v| v.len() == 2 && v[1].is_null())
            .ok_or_else(invalid)?;
        let identity = pair[0]
            .as_array()
            .filter(|v| v.len() == 2 && v[1].is_null())
            .ok_or_else(invalid)?;
        children.push(
            Uuid::parse_str(identity[0].as_str().ok_or_else(invalid)?).map_err(|_| invalid())?,
        );
    }
    Ok(RefreshSuccess {
        task_id,
        parent,
        children,
        date_done,
        missing_source,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn corpus() -> Value {
        serde_json::from_str(include_str!("../datasets/source-epg-refresh-success.json")).unwrap()
    }
    #[test]
    fn actual_initial_import_missing_and_executed_descendant_results_are_distinct() {
        let c = corpus();
        let rows = c["results"].as_array().unwrap();
        assert_eq!(rows.len(), 18);
        let (mut missing, mut descendants, mut two_children) = (0, 0, 0);
        for row in rows {
            let e = &row["metadata"];
            let id = Uuid::parse_str(e["task_id"].as_str().unwrap()).unwrap();
            let m = parse_success(
                &serde_json::to_vec(e).unwrap(),
                id,
                row["source_id"].as_i64().unwrap(),
            )
            .unwrap();
            assert_eq!(m.children.len(), e["children"].as_array().unwrap().len());
            missing += usize::from(m.missing_source);
            descendants += usize::from(m.parent.is_some());
            two_children += usize::from(m.children.len() == 2);
        }
        assert_eq!(missing, 1);
        assert_eq!(descendants, 1);
        assert_eq!(two_children, 3)
    }
    #[test]
    fn identity_state_unknown_fields_returns_and_geometry_fail_closed() {
        let c = corpus();
        let row = &c["results"][0];
        let e = &row["metadata"];
        let id = Uuid::parse_str(e["task_id"].as_str().unwrap()).unwrap();
        let sid = row["source_id"].as_i64().unwrap();
        for (field, value) in [
            ("status", json!("FAILURE")),
            ("result", json!("unknown handler return")),
            ("traceback", json!("not-null")),
            ("parent_id", json!(null)),
            ("children", json!([[[id.to_string(), null], ["group"]]])),
            ("date_done", json!(null)),
            ("extra", json!(true)),
        ] {
            let mut n = e.clone();
            n[field] = value;
            assert!(
                parse_success(&serde_json::to_vec(&n).unwrap(), id, sid).is_err(),
                "{field}"
            )
        }
        let mut n = e.clone();
        n["result"] = json!(format!(
            "EPG source {} does not exist, task cleaned up",
            sid + 1
        ));
        assert!(parse_success(&serde_json::to_vec(&n).unwrap(), id, sid).is_err());
        n["result"] = Value::Null;
        n["children"] = json!(vec![json!([[id.to_string(), null], null]); 65]);
        assert!(parse_success(&serde_json::to_vec(&n).unwrap(), id, sid).is_err());
        assert!(parse_success(&serde_json::to_vec(e).unwrap(), Uuid::new_v4(), sid).is_err());
        assert!(parse_success(&vec![b' '; decode::MAX_RESULT_BYTES + 1], id, sid).is_err());
        assert!(parse_success(&serde_json::to_vec(e).unwrap(), id, 0).is_err());
    }
}
