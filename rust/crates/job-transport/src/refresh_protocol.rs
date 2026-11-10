//! Observed Original refresh protocol, bounded and component-only.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL.
use crate::{
    protocol::{self, ProtocolError, Task},
    worker,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use uuid::Uuid;
const NAME: &str = "apps.epg.tasks.refresh_epg_data";
fn invalid() -> ProtocolError {
    ProtocolError::InvalidArgument
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshCall {
    pub args: Vec<Value>,
    pub kwargs: Map<String, Value>,
    pub list_args: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshMessage {
    pub task_id: Uuid,
    pub source_id: i64,
    pub force: bool,
    pub retry: u32,
    pub root: Uuid,
    pub parent: Option<Uuid>,
    pub eta: Option<DateTime<Utc>>,
    pub call: RefreshCall,
}
impl RefreshCall {
    pub fn initial(source_id: i64) -> Self {
        Self {
            args: vec![json!(source_id)],
            kwargs: Map::new(),
            list_args: false,
        }
    }
    pub fn deferred(source_id: i64, force: bool, retry: u32) -> Self {
        Self {
            args: vec![json!(source_id)],
            kwargs: json!({"force":force,"_file_defer_retry":retry})
                .as_object()
                .unwrap()
                .clone(),
            list_args: true,
        }
    }
    pub fn resolve(&self) -> Result<(i64, bool, u32), ProtocolError> {
        if self.args.len() > 3 || self.kwargs.len() > 3 {
            return Err(invalid());
        }
        let names = ["source_id", "force", "_file_defer_retry"];
        if self.kwargs.keys().any(|k| !names.contains(&k.as_str())) {
            return Err(invalid());
        }
        for k in names.iter().take(self.args.len()) {
            if self.kwargs.contains_key(*k) {
                return Err(invalid());
            }
        }
        let get = |i: usize| self.args.get(i).or_else(|| self.kwargs.get(names[i]));
        let source = get(0)
            .and_then(Value::as_i64)
            .filter(|v| *v > 0)
            .ok_or_else(invalid)?;
        let force = get(1)
            .map(|v| v.as_bool().ok_or_else(invalid))
            .transpose()?
            .unwrap_or(false);
        let retry = get(2)
            .map(|v| v.as_u64().filter(|v| *v <= 480).ok_or_else(invalid))
            .transpose()?
            .unwrap_or(0);
        Ok((source, force, retry as u32))
    }
    fn repr(&self) -> Result<(String, String), ProtocolError> {
        self.resolve()?;
        let literal = |v: &Value| {
            if let Some(b) = v.as_bool() {
                if b {
                    "True".to_owned()
                } else {
                    "False".to_owned()
                }
            } else {
                v.to_string()
            }
        };
        let joined = self.args.iter().map(literal).collect::<Vec<_>>().join(", ");
        let args = if self.list_args {
            format!("[{joined}]")
        } else if self.args.len() == 1 {
            format!("({joined},)")
        } else {
            format!("({joined})")
        };
        let kwargs = format!(
            "{{{}}}",
            self.kwargs
                .iter()
                .map(|(k, v)| format!("'{k}': {}", literal(v)))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok((args, kwargs))
    }
}
pub fn encode(
    message: &RefreshMessage,
    reply: Uuid,
    delivery: Uuid,
    origin: &str,
) -> Result<Vec<u8>, ProtocolError> {
    let resolved = message.call.resolve()?;
    if resolved != (message.source_id, message.force, message.retry)
        || message.parent.is_none() && message.root != message.task_id
    {
        return Err(invalid());
    }
    let mut e: Value = serde_json::from_slice(&protocol::encode(
        &Task::CreateBackup,
        message.task_id,
        reply,
        delivery,
        origin,
    )?)
    .map_err(|_| invalid())?;
    let (args, kwargs) = message.call.repr()?;
    e["body"]=json!(STANDARD.encode(serde_json::to_vec(&json!([message.call.args,message.call.kwargs,{"callbacks":null,"errbacks":null,"chain":null,"chord":null}])).map_err(|_|invalid())?));
    let h = &mut e["headers"];
    h["task"] = json!(NAME);
    h["root_id"] = json!(message.root.to_string());
    h["parent_id"] = message.parent.map_or(Value::Null, |v| json!(v.to_string()));
    h["eta"] = message.eta.map_or(Value::Null, |v| {
        json!(v.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string())
    });
    h["timelimit"] = json!([14400, null]);
    h["argsrepr"] = json!(args);
    h["kwargsrepr"] = json!(kwargs);
    let bytes = serde_json::to_vec(&e).map_err(|_| invalid())?;
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    Ok(bytes)
}
pub fn parse(bytes: &[u8]) -> Result<RefreshMessage, ProtocolError> {
    if bytes.len() > protocol::MAX_ENVELOPE {
        return Err(ProtocolError::Capacity);
    }
    let mut e: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
    let h = &e["headers"];
    if h["task"] != NAME || h["timelimit"] != json!([14400, null]) {
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
        let d = DateTime::parse_from_rfc3339(h["eta"].as_str().ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        if d.offset().local_minus_utc() != 0 {
            return Err(invalid());
        }
        Some(d.with_timezone(&Utc))
    };
    let raw = STANDARD
        .decode(e["body"].as_str().ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    if raw.len() > 32768 {
        return Err(ProtocolError::Capacity);
    }
    let body: Value = serde_json::from_slice(&raw).map_err(|_| invalid())?;
    let parts = body
        .as_array()
        .filter(|v| v.len() == 3)
        .ok_or_else(invalid)?;
    if parts[2] != json!({"callbacks":null,"errbacks":null,"chain":null,"chord":null}) {
        return Err(invalid());
    }
    let call = RefreshCall {
        args: parts[0].as_array().ok_or_else(invalid)?.clone(),
        kwargs: parts[1].as_object().ok_or_else(invalid)?.clone(),
        list_args: h["argsrepr"].as_str().is_some_and(|v| v.starts_with('[')),
    };
    let (source_id, force, retry) = call.resolve()?;
    let (args, kwargs) = call.repr()?;
    if h["argsrepr"] != args || h["kwargsrepr"] != kwargs {
        return Err(invalid());
    }
    // Reuse captured common options without widening task, timing, lineage or body.
    let h = &mut e["headers"];
    h["task"] = json!("apps.backups.tasks.create_backup_task");
    h["root_id"] = json!(task_id.to_string());
    h["parent_id"] = Value::Null;
    h["eta"] = Value::Null;
    h["timelimit"] = json!([null, null]);
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
    Ok(RefreshMessage {
        task_id,
        source_id,
        force,
        retry,
        root,
        parent,
        eta,
        call,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn corpus() -> Value {
        serde_json::from_str(include_str!("../datasets/source-epg-refresh-import-2.json")).unwrap()
    }
    #[test]
    fn actual_source_default_positional_keyword_and_deferred_forms_roundtrip() {
        let mut count = 0;
        for row in corpus()["messages"].as_array().unwrap() {
            let e = &row["envelope"];
            if e["headers"]["task"] != NAME {
                continue;
            }
            let m = parse(&serde_json::to_vec(e).unwrap()).unwrap();
            let p = &e["properties"];
            let native = encode(
                &m,
                Uuid::parse_str(p["reply_to"].as_str().unwrap()).unwrap(),
                Uuid::parse_str(p["delivery_tag"].as_str().unwrap()).unwrap(),
                e["headers"]["origin"].as_str().unwrap(),
            )
            .unwrap();
            let mut n: Value = serde_json::from_slice(&native).unwrap();
            let mut original = e.clone();
            // JSON body whitespace is not wire-byte equality: compare decoded
            // structure, while every envelope/header/property field stays exact.
            for frame in [&mut n, &mut original] {
                frame["body"] = serde_json::from_slice::<Value>(
                    &STANDARD.decode(frame["body"].as_str().unwrap()).unwrap(),
                )
                .unwrap();
            }
            assert_eq!(n, original);
            assert_eq!(parse(&native).unwrap(), m);
            count += 1
        }
        assert_eq!(count, 11)
    }
    #[test]
    fn argument_types_duplicates_unknowns_and_bounds_are_rejected() {
        for (args, kwargs) in [
            (json!([1, true, 481]), json!({})),
            (json!([1]), json!({"source_id":1})),
            (json!([1, 1]), json!({})),
            (json!([0]), json!({})),
            (json!([1]), json!({"x":0})),
            (json!([]), json!({"source_id":1.5})),
            (json!([1, false, 1, 0]), json!({})),
        ] {
            let c = RefreshCall {
                args: args.as_array().unwrap().clone(),
                kwargs: kwargs.as_object().unwrap().clone(),
                list_args: false,
            };
            assert!(c.resolve().is_err())
        }
        assert!(parse(&vec![b' '; protocol::MAX_ENVELOPE + 1]).is_err());
        assert_eq!(
            RefreshCall::initial(i64::MAX).resolve().unwrap(),
            (i64::MAX, false, 0)
        );
        assert_eq!(
            RefreshCall::deferred(1, true, 480).resolve().unwrap(),
            (1, true, 480)
        );
    }
    #[test]
    fn unsupported_options_wrong_family_and_repr_are_not_normalized_away() {
        let c = corpus();
        let e = &c["messages"][0]["envelope"];
        for (field, value) in [
            ("task", json!("other.family")),
            ("argsrepr", json!("[999]")),
            ("timelimit", json!([null, null])),
            ("retries", json!(1)),
            ("expires", json!("2026-11-11")),
        ] {
            let mut n = e.clone();
            n["headers"][field] = value;
            assert!(parse(&serde_json::to_vec(&n).unwrap()).is_err(), "{field}")
        }
    }
}
