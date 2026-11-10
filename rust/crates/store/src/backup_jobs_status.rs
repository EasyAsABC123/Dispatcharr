//! Pure observed original backup_status projection; test-only, no route/queue/202.
//! Original Dispatcharr authors retain full application credit and AGPL licensing.
use serde_json::{json, Value};
use std::io::{self, Write};
const MAX_BYTES: usize = 1024 * 1024;
const MAX_NODES: usize = 8192;
const MAX_DEPTH: usize = 32;
const MAX_MESSAGE: usize = 65536;
#[derive(Debug, PartialEq)]
pub(crate) struct StatusReply {
    pub status: u16,
    pub body: Value,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MappingError {
    Capacity,
    UnobservedState,
}
/// Decoded transport snapshot only: no durable transport/exception codec claim.
pub(crate) enum Snapshot<'a> {
    Unready(&'a str),
    Success(&'a Value),
    PropagatedFailure(&'a str),
    BackendError(&'a str),
}
struct Budget(usize);
impl Write for Budget {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() > MAX_BYTES.saturating_sub(self.0) {
            return Err(io::Error::other("job result budget"));
        }
        self.0 += data.len();
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn bounded(value: &Value) -> Result<(), MappingError> {
    let mut pending = vec![(value, 0usize)];
    let mut nodes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > MAX_NODES || depth > MAX_DEPTH {
            return Err(MappingError::Capacity);
        }
        match value {
            Value::Array(items) => {
                if items.len() > MAX_NODES.saturating_sub(nodes.saturating_add(pending.len())) {
                    return Err(MappingError::Capacity);
                }
                pending.extend(items.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(items) => {
                if items.len() > MAX_NODES.saturating_sub(nodes.saturating_add(pending.len())) {
                    return Err(MappingError::Capacity);
                }
                if items.keys().any(|key| key.len() > MAX_MESSAGE) {
                    return Err(MappingError::Capacity);
                }
                pending.extend(items.values().map(|v| (v, depth + 1)));
            }
            Value::String(text) if text.len() > MAX_MESSAGE => return Err(MappingError::Capacity),
            _ => {}
        }
    }
    serde_json::to_writer(&mut Budget(0), value).map_err(|_| MappingError::Capacity)
}
fn failed(message: &str) -> Result<StatusReply, MappingError> {
    if message.len() > MAX_MESSAGE {
        return Err(MappingError::Capacity);
    }
    Ok(StatusReply {
        status: 500,
        body: json!({"detail":format!("Failed to get task status: {message}")}),
    })
}
pub(crate) fn project(snapshot: Snapshot<'_>) -> Result<StatusReply, MappingError> {
    match snapshot {
        Snapshot::Unready(state) => {
            let state = match state {
                "PENDING" => "pending",
                "STARTED" => "started",
                "RETRY" => "retry",
                _ => return Err(MappingError::UnobservedState),
            };
            Ok(StatusReply {
                status: 200,
                body: json!({"state":state}),
            })
        }
        Snapshot::PropagatedFailure(message) | Snapshot::BackendError(message) => failed(message),
        Snapshot::Success(result) => {
            bounded(result)?;
            if let Value::Object(fields) = result {
                if fields.get("status").and_then(Value::as_str) == Some("completed") {
                    Ok(StatusReply {
                        status: 200,
                        body: json!({"state":"completed","result":result}),
                    })
                } else {
                    Ok(StatusReply {
                        status: 200,
                        body: json!({"state":"failed","error":fields.get("error").cloned().unwrap_or(json!("Unknown error"))}),
                    })
                }
            } else {
                let name = match result {
                    Value::Null => "NoneType",
                    Value::Array(_) => "list",
                    Value::String(_) => "str",
                    Value::Bool(_) => "bool",
                    Value::Number(number) if number.is_f64() => "float",
                    Value::Number(_) => "int",
                    Value::Object(_) => unreachable!(),
                };
                failed(&format!("'{name}' object has no attribute 'get'"))
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn captured_source_entities_match_exactly() {
        let fixtures: Value =
            serde_json::from_str(include_str!("backup_jobs_source_vectors.json")).unwrap();
        let mut observed = 0;
        for row in fixtures["vectors"].as_array().unwrap() {
            let snapshot = match row["kind"].as_str().unwrap() {
                "unready" => Snapshot::Unready(row["state"].as_str().unwrap()),
                "success" => Snapshot::Success(&row["result"]),
                "propagated" => Snapshot::PropagatedFailure(row["message"].as_str().unwrap()),
                "backend" => Snapshot::BackendError(row["message"].as_str().unwrap()),
                _ => panic!("unknown fixture kind"),
            };
            let actual = project(snapshot).unwrap();
            assert_eq!(
                actual.status,
                row["expected"]["status"].as_u64().unwrap() as u16,
                "{}",
                row["name"]
            );
            assert_eq!(actual.body, row["expected"]["body"], "{}", row["name"]);
            observed += 1;
        }
        assert_eq!(observed, 11);
    }
    #[test]
    fn code_derived_completed_echo_and_typed_errors_are_not_new_wire_claims() {
        let completed = json!({"status":"completed","filename":"fixture.zip","size":123});
        assert_eq!(
            project(Snapshot::Success(&completed)).unwrap().body,
            json!({"state":"completed","result":completed})
        );
        for value in [
            json!({"status":true,"error":false}),
            json!({"status":1,"error":0}),
            json!({"error":[]}),
        ] {
            assert_eq!(
                project(Snapshot::Success(&value)).unwrap().body,
                json!({"state":"failed","error":value["error"]})
            );
        }
        for (value, name) in [
            (json!(1), "int"),
            (json!(1.0), "float"),
            (json!(true), "bool"),
        ] {
            assert_eq!(
                project(Snapshot::Success(&value)).unwrap().body,
                json!({"detail":format!("Failed to get task status: '{name}' object has no attribute 'get'")})
            );
        }
    }
    #[test]
    fn oversized_or_unobserved_data_never_becomes_fake_success() {
        assert_eq!(
            project(Snapshot::Unready("REVOKED")),
            Err(MappingError::UnobservedState)
        );
        assert_eq!(
            project(Snapshot::Success(&json!("x".repeat(MAX_MESSAGE + 1)))),
            Err(MappingError::Capacity)
        );
        assert_eq!(
            project(Snapshot::BackendError(&"x".repeat(MAX_MESSAGE + 1))),
            Err(MappingError::Capacity)
        );
        let mut deep = Value::Null;
        for _ in 0..MAX_DEPTH + 1 {
            deep = json!([deep]);
        }
        assert_eq!(
            project(Snapshot::Success(&deep)),
            Err(MappingError::Capacity)
        );
        assert_eq!(
            project(Snapshot::Success(&json!(vec![0; MAX_NODES + 1]))),
            Err(MappingError::Capacity)
        );
        assert_eq!(
            project(Snapshot::Success(&json!(vec!["a".repeat(MAX_MESSAGE); 17]))),
            Err(MappingError::Capacity)
        );
    }
}
