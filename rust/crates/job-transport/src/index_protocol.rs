//! Bounded original109 index task protocol; no index execution/dispatch authority.
//! Original Dispatcharr authors retain task/integration credit and AGPL licensing.
use crate::{
    decode,
    protocol::{self, ProtocolError, Task},
    worker,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexTask {
    Initial {
        source_id: i64,
    },
    /// Source manual busy deferral is a new child with retries header0, not RETRY.
    Deferred {
        source_id: i64,
        retry: u32,
        root: Uuid,
        parent: Uuid,
        eta: DateTime<Utc>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexMessage {
    pub task_id: Uuid,
    pub source_id: i64,
    pub retry: u32,
    pub explicit_retry: bool,
    pub root: Uuid,
    pub parent: Option<Uuid>,
    pub eta: Option<DateTime<Utc>>,
    pub list_args_repr: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexResult {
    pub task_id: Uuid,
    pub parent: Option<Uuid>,
    pub children: Vec<Uuid>,
    pub date_done: Option<String>,
}
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
pub fn encode(
    task: &IndexTask,
    id: Uuid,
    reply: Uuid,
    delivery: Uuid,
    origin: &str,
) -> Result<Vec<u8>, ProtocolError> {
    let mut e: Value = serde_json::from_slice(&protocol::encode(
        &Task::CreateBackup,
        id,
        reply,
        delivery,
        origin,
    )?)
    .map_err(|_| invalid())?;
    let (source_id, retry, root, parent, eta) = match task {
        IndexTask::Initial { source_id } => (*source_id, None, id, None, None),
        IndexTask::Deferred {
            source_id,
            retry,
            root,
            parent,
            eta,
        } => {
            if *retry == 0 || *retry > 480 {
                return Err(invalid());
            }
            (*source_id, Some(*retry), *root, Some(*parent), Some(*eta))
        }
    };
    if source_id <= 0 {
        return Err(invalid());
    }
    let kwargs = retry.map_or_else(|| json!({}), |n| json!({"_defer_retry":n}));
    let body =
        json!([[source_id],kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]);
    e["body"] = json!(STANDARD.encode(serde_json::to_vec(&body).map_err(|_| invalid())?));
    let h = &mut e["headers"];
    h["task"] = json!("apps.epg.tasks.build_programme_index_task");
    h["root_id"] = json!(root.to_string());
    h["parent_id"] = parent.map_or(Value::Null, |v| json!(v.to_string()));
    h["eta"] = eta.map_or(Value::Null, |v| {
        json!(v.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string())
    });
    h["argsrepr"] = json!(if retry.is_some() {
        format!("[{source_id}]")
    } else {
        format!("({source_id},)")
    });
    h["kwargsrepr"] =
        json!(retry.map_or_else(|| "{}".into(), |n| format!("{{'_defer_retry': {n}}}")));
    let bytes = serde_json::to_vec(&e).map_err(|_| invalid())?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
pub fn parse(bytes: &[u8]) -> Result<IndexMessage, ProtocolError> {
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut e: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let h = &e["headers"];
    if h["task"] != "apps.epg.tasks.build_programme_index_task" {
        return Err(invalid());
    }
    let uuid = |v: &Value| Uuid::parse_str(v.as_str().ok_or_else(invalid)?).map_err(|_| invalid());
    let task_id = uuid(&h["id"])?;
    let root = uuid(&h["root_id"])?;
    let parent = if h["parent_id"].is_null() {
        None
    } else {
        Some(uuid(&h["parent_id"])?)
    };
    if parent.is_none() && root != task_id {
        return Err(invalid());
    }
    let eta = if h["eta"].is_null() {
        None
    } else {
        let value = DateTime::parse_from_rfc3339(h["eta"].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        if value.offset().local_minus_utc() != 0 {
            return Err(invalid());
        }
        Some(value.with_timezone(&Utc))
    };
    let decoded = STANDARD
        .decode(e["body"].as_str().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    if decoded.len() > 32768 {
        return Err(ProtocolError::Capacity);
    }
    let body: Value = serde_json::from_slice(&decoded).map_err(|_| invalid())?;
    let parts = body
        .as_array()
        .filter(|v| v.len() == 3)
        .ok_or_else(invalid)?;
    let args = parts[0]
        .as_array()
        .filter(|v| v.len() == 1)
        .ok_or_else(invalid)?;
    let source_id = args[0].as_i64().filter(|v| *v > 0).ok_or_else(invalid)?;
    let kwargs = parts[1].as_object().ok_or_else(invalid)?;
    let retry = if kwargs.is_empty() {
        None
    } else {
        if kwargs.len() != 1 {
            return Err(invalid());
        }
        Some(
            kwargs
                .get("_defer_retry")
                .and_then(Value::as_u64)
                .filter(|n| *n <= 480)
                .ok_or_else(invalid)? as u32,
        )
    };
    if parts[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null}) {
        return Err(invalid());
    }
    let list_args_repr = h["argsrepr"] == format!("[{source_id}]");
    if !list_args_repr && h["argsrepr"] != format!("({source_id},)") {
        return Err(invalid());
    }
    if h["kwargsrepr"] != retry.map_or_else(|| "{}".into(), |n| format!("{{'_defer_retry': {n}}}"))
    {
        return Err(invalid());
    }
    // Reuse exactly the common captured envelope checks, replacing only the
    // index-specific fields already validated above. No other options widen.
    let h = &mut e["headers"];
    h["task"] = json!("apps.backups.tasks.create_backup_task");
    h["root_id"] = json!(task_id.to_string());
    h["parent_id"] = Value::Null;
    h["eta"] = Value::Null;
    h["argsrepr"] = json!("()");
    h["kwargsrepr"] = json!("{}");
    e["body"] = json!(STANDARD.encode(
        serde_json::to_vec(
            &json!([[],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])
        )
        .map_err(|_| invalid())?
    ));
    worker::create_identity(&serde_json::to_vec(&e).map_err(|_| invalid())?)
        .map_err(|_| invalid())?;
    Ok(IndexMessage {
        task_id,
        source_id,
        retry: retry.unwrap_or(0),
        explicit_retry: retry.is_some(),
        root,
        parent,
        eta,
        list_args_repr,
    })
}
/// Only observed normal index SUCCESS/null results and their lineage tuple are
/// accepted here. Index exception/result writing needs its separate evidence.
pub fn parse_result(bytes: &[u8], expected: Uuid) -> Result<IndexResult, ProtocolError> {
    let meta = decode::decode(bytes, expected).map_err(|_| invalid())?;
    if meta.state != decode::State::Success || !meta.result.is_null() || meta.traceback.is_some() {
        return Err(invalid());
    }
    let value: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let parent = match value.get("parent_id") {
        None => None,
        Some(Value::String(v)) => Some(Uuid::parse_str(v).map_err(|_| invalid())?),
        _ => return Err(invalid()),
    };
    let rows = value["children"]
        .as_array()
        .filter(|v| v.len() <= 1)
        .ok_or_else(invalid)?;
    let mut children = Vec::new();
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
    Ok(IndexResult {
        task_id: expected,
        parent,
        children,
        date_done: meta.date_done,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn corpus() -> Value {
        serde_json::from_str(include_str!("../datasets/source-index-109.json")).unwrap()
    }
    #[test]
    fn actual_source109_envelopes_results_and_manual_defer_remain_distinct() {
        let v = corpus();
        let rows = v["messages"].as_array().unwrap();
        assert_eq!(rows.len(), 9);
        let mut deferred = 0;
        for row in rows {
            let e = &row["envelope"];
            let parsed = parse(&serde_json::to_vec(e).unwrap()).unwrap();
            assert_eq!(parsed.task_id.to_string(), e["headers"]["id"]);
            if parsed.parent.is_some() {
                deferred += 1;
                assert!(parsed.list_args_repr);
                assert!(parsed.explicit_retry);
                assert!(parsed.eta.is_some());
                assert_eq!(e["headers"]["retries"], 0)
            }
        }
        assert!(deferred >= 3);
        let results = v["results"].as_array().unwrap();
        assert_eq!(results.len(), 6);
        for row in results {
            let meta = &row["metadata"];
            let id = Uuid::parse_str(meta["task_id"].as_str().unwrap()).unwrap();
            parse_result(&serde_json::to_vec(meta).unwrap(), id).unwrap();
        }
    }
    #[test]
    fn native_initial_and_deferred_match_actual_source_semantic_fields() {
        let v = corpus();
        let mut comparisons = 0;
        for row in v["messages"].as_array().unwrap() {
            let original = &row["envelope"];
            let parsed = parse(&serde_json::to_vec(original).unwrap()).unwrap();
            let task = if let (Some(parent), Some(eta)) = (parsed.parent, parsed.eta) {
                IndexTask::Deferred {
                    source_id: parsed.source_id,
                    retry: parsed.retry,
                    root: parsed.root,
                    parent,
                    eta,
                }
            } else if !parsed.explicit_retry && !parsed.list_args_repr {
                IndexTask::Initial {
                    source_id: parsed.source_id,
                }
            } else {
                continue;
            };
            let p = &original["properties"];
            let encoded = encode(
                &task,
                parsed.task_id,
                Uuid::parse_str(p["reply_to"].as_str().unwrap()).unwrap(),
                Uuid::parse_str(p["delivery_tag"].as_str().unwrap()).unwrap(),
                "source-fixture@redacted",
            )
            .unwrap();
            let mut native: Value = serde_json::from_slice(&encoded).unwrap();
            let body = |value: &Value| -> Value {
                serde_json::from_slice(&STANDARD.decode(value["body"].as_str().unwrap()).unwrap())
                    .unwrap()
            };
            assert_eq!(body(&native), body(original));
            // Existing protocol acceptance is decoded-value parity; Python JSON
            // spaces inside base64 are not claimed byte-identical.
            native["body"] = original["body"].clone();
            assert_eq!(native, *original);
            assert_eq!(parse(&encoded).unwrap(), parsed);
            comparisons += 1;
        }
        assert!(comparisons >= 7);
    }
    #[test]
    fn changed_task_options_or_noninteger_ids_do_not_widen_supported_family() {
        let v = corpus();
        let original = &v["messages"][0]["envelope"];
        for key in ["task", "eta", "root_id", "retries", "kwargsrepr", "stamps"] {
            let mut e = original.clone();
            e["headers"][key] = json!("unsupported");
            assert!(parse(&serde_json::to_vec(&e).unwrap()).is_err());
        }
        assert!(encode(
            &IndexTask::Initial { source_id: 0 },
            Uuid::nil(),
            Uuid::nil(),
            Uuid::nil(),
            "fixture"
        )
        .is_err());
    }
}
