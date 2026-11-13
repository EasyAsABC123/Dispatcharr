//! Bounded observed Source result metadata. No Python imports or HTTP projection.
//! Original Dispatcharr authors retain integration credit and AGPL licensing.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;
use uuid::Uuid;
pub const MAX_RESULT_BYTES: usize = 262144;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Capacity,
    Malformed,
    Identity,
    UnsupportedState,
    UnsupportedException,
    UnsupportedTag,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Started,
    Retry,
    Success,
    Failure,
    Revoked,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Exception {
    pub class: String,
    pub arguments: Vec<Value>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TaggedValue {
    pub kind: String,
    pub encoded_value: Value,
}
/// Tags remain explicitly encoded; passing tagged dictionaries to the existing
/// test-only HTTP mapper would be incorrect. No Python value emulation is claimed.
#[derive(Clone, Debug, PartialEq)]
pub struct Metadata {
    pub task_id: Uuid,
    pub state: State,
    pub result: Value,
    pub date_done: Option<String>,
    pub traceback: Option<String>,
    pub exception: Option<Exception>,
    pub tagged_values: Vec<TaggedValue>,
}
fn bounded(value: &Value) -> Result<Vec<TaggedValue>, DecodeError> {
    let mut pending = vec![(value, 0usize)];
    let mut nodes = 0usize;
    let mut tags = vec![];
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > 8192 || depth > 32 {
            return Err(DecodeError::Capacity);
        }
        match value {
            Value::String(value) if value.len() > 65536 => return Err(DecodeError::Capacity),
            Value::Array(items) => {
                if items.len() > 8192usize.saturating_sub(nodes.saturating_add(pending.len())) {
                    return Err(DecodeError::Capacity);
                }
                pending.extend(items.iter().map(|v| (v, depth + 1)));
            }
            Value::Object(items) => {
                if items.len() > 8192usize.saturating_sub(nodes.saturating_add(pending.len()))
                    || items.keys().any(|key| key.len() > 1024)
                {
                    return Err(DecodeError::Capacity);
                }
                if let Some(kind) = items.get("__type__") {
                    if items.len() != 2 {
                        return Err(DecodeError::UnsupportedTag);
                    }
                    let kind = kind.as_str().ok_or(DecodeError::Malformed)?;
                    let encoded = items.get("__value__").ok_or(DecodeError::Malformed)?;
                    match kind {
                        "bytes" | "date" | "datetime" | "decimal" => {
                            if !encoded.is_string() {
                                return Err(DecodeError::Malformed);
                            }
                        }
                        "base64" => {
                            STANDARD
                                .decode(encoded.as_str().ok_or(DecodeError::Malformed)?)
                                .map_err(|_| DecodeError::Malformed)?;
                        }
                        "uuid" => {
                            let fields = encoded.as_object().ok_or(DecodeError::Malformed)?;
                            if fields.len() != 1 {
                                return Err(DecodeError::Malformed);
                            }
                            Uuid::parse_str(
                                fields
                                    .get("hex")
                                    .and_then(Value::as_str)
                                    .ok_or(DecodeError::Malformed)?,
                            )
                            .map_err(|_| DecodeError::Malformed)?;
                        }
                        _ => return Err(DecodeError::UnsupportedTag),
                    }
                    tags.push(TaggedValue {
                        kind: kind.to_owned(),
                        encoded_value: encoded.clone(),
                    });
                }
                pending.extend(items.values().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(tags)
}
pub fn decode(raw: &[u8], expected: Uuid) -> Result<Metadata, DecodeError> {
    if raw.len() > MAX_RESULT_BYTES {
        return Err(DecodeError::Capacity);
    }
    let value: Value = serde_json::from_slice(raw).map_err(|_| DecodeError::Malformed)?;
    let tags = bounded(&value)?;
    let fields = value.as_object().ok_or(DecodeError::Malformed)?;
    let task_id = fields
        .get("task_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or(DecodeError::Malformed)?;
    if task_id != expected {
        return Err(DecodeError::Identity);
    }
    let state = match fields.get("status").and_then(Value::as_str) {
        Some("PENDING") => State::Pending,
        Some("STARTED") => State::Started,
        Some("RETRY") => State::Retry,
        Some("SUCCESS") => State::Success,
        Some("FAILURE") => State::Failure,
        Some("REVOKED") => State::Revoked,
        _ => return Err(DecodeError::UnsupportedState),
    };
    let result = fields.get("result").ok_or(DecodeError::Malformed)?.clone();
    if !fields.get("children").is_some_and(Value::is_array) {
        return Err(DecodeError::Malformed);
    }
    let optional = |name: &str| -> Result<Option<String>, DecodeError> {
        match fields.get(name) {
            Some(Value::Null) => Ok(None),
            Some(Value::String(v)) => Ok(Some(v.clone())),
            _ => Err(DecodeError::Malformed),
        }
    };
    let exception = if matches!(state, State::Failure | State::Retry | State::Revoked) {
        let object = result
            .as_object()
            .ok_or(DecodeError::UnsupportedException)?;
        let class = object
            .get("exc_type")
            .and_then(Value::as_str)
            .ok_or(DecodeError::UnsupportedException)?;
        let module = object
            .get("exc_module")
            .and_then(Value::as_str)
            .ok_or(DecodeError::UnsupportedException)?;
        let arguments = object
            .get("exc_message")
            .and_then(Value::as_array)
            .ok_or(DecodeError::UnsupportedException)?;
        if arguments.len() > 8 {
            return Err(DecodeError::Capacity);
        }
        // These are data descriptors from captured Source records. No module
        // is imported and no exception constructor or HTTP mapper is invoked.
        let supported = if state == State::Revoked {
            module == "celery.exceptions"
                && class == "TaskRevokedError"
                && object.len() == 3
                && arguments.len() == 1
                && matches!(arguments[0].as_str(), Some("expired" | "revoked"))
                && fields.get("traceback") == Some(&Value::Null)
                && fields["children"].as_array().is_some_and(Vec::is_empty)
                && optional("date_done")?.is_some_and(|v| {
                    v.len() <= 40 && chrono::DateTime::parse_from_rfc3339(&v).is_ok()
                })
        } else {
            match (module, class) {
                ("builtins", "ValueError" | "KeyError") => true,
                ("builtins", "FileNotFoundError") => {
                    state == State::Failure
                        && object.len() == 3
                        && arguments.len() == 2
                        && arguments[0].as_i64() == Some(2)
                        && arguments[1].as_str() == Some("No such file or directory")
                }
                ("builtins", "IsADirectoryError") => {
                    arguments.len() == 2
                        && arguments[0].as_i64() == Some(21)
                        && arguments[1].as_str() == Some("Is a directory")
                }
                ("builtins", "AttributeError")
                | ("django.db.utils", "ProgrammingError")
                | (
                    "redis.exceptions",
                    "ConnectionError" | "AuthenticationError" | "NoPermissionError",
                )
                | ("kombu.exceptions", "OperationalError") => {
                    if arguments.len() != 1 || !arguments[0].is_string() {
                        return Err(DecodeError::UnsupportedException);
                    }
                    if arguments[0].as_str().unwrap().len() > 8192 {
                        return Err(DecodeError::Capacity);
                    }
                    true
                }
                _ => false,
            }
        };
        if !supported {
            return Err(DecodeError::UnsupportedException);
        }
        Some(Exception {
            class: class.to_owned(),
            arguments: arguments.clone(),
        })
    } else {
        None
    };
    Ok(Metadata {
        task_id,
        state,
        result,
        date_done: optional("date_done")?,
        traceback: optional("traceback")?,
        exception,
        tagged_values: tags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_missing_file_descriptor_is_data_only_and_not_generic_oserror() {
        let id = Uuid::new_v4();
        let value = serde_json::json!({"task_id":id,"status":"FAILURE","result":{"exc_module":"builtins","exc_type":"FileNotFoundError","exc_message":[2,"No such file or directory"]},"children":[],"date_done":"2026-11-12T05:00:00.000000+00:00","traceback":"Native opaque diagnostic"});
        assert_eq!(
            decode(&serde_json::to_vec(&value).unwrap(), id)
                .unwrap()
                .state,
            State::Failure
        );
        for args in [
            serde_json::json!([13, "No such file or directory"]),
            serde_json::json!([2, "other"]),
            serde_json::json!(["2", "No such file or directory"]),
            serde_json::json!([2, "No such file or directory", "extra"]),
        ] {
            let mut bad = value.clone();
            bad["result"]["exc_message"] = args;
            assert_eq!(
                decode(&serde_json::to_vec(&bad).unwrap(), id).unwrap_err(),
                DecodeError::UnsupportedException
            );
        }
        let mut retry = value.clone();
        retry["status"] = serde_json::json!("RETRY");
        assert_eq!(
            decode(&serde_json::to_vec(&retry).unwrap(), id).unwrap_err(),
            DecodeError::UnsupportedException
        );
        let mut bad = value.clone();
        bad["result"]["exc_type"] = serde_json::json!("OSError");
        assert_eq!(
            decode(&serde_json::to_vec(&bad).unwrap(), id).unwrap_err(),
            DecodeError::UnsupportedException
        );
    }
    #[test]
    fn observed_index_failure_metadata_is_read_without_importing_exception_classes() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../datasets/source-index-failure-reader-1.json"
        ))
        .unwrap();
        assert_eq!(corpus["rows"].as_array().unwrap().len(), 19);
        for row in corpus["rows"].as_array().unwrap() {
            let metadata = &row["metadata"];
            let id = Uuid::parse_str(metadata["task_id"].as_str().unwrap()).unwrap();
            let value = decode(&serde_json::to_vec(metadata).unwrap(), id).unwrap();
            assert_eq!(value.state, State::Failure);
            assert_eq!(value.result, metadata["result"]);
            let exception = value.exception.unwrap();
            assert_eq!(exception.class, metadata["result"]["exc_type"]);
            assert_eq!(
                exception.arguments,
                *metadata["result"]["exc_message"].as_array().unwrap()
            );
            assert_eq!(value.traceback.as_deref(), metadata["traceback"].as_str());
        }
    }
    #[test]
    fn observed_exception_names_do_not_authorize_foreign_modules_or_argument_shapes() {
        let corpus: Value = serde_json::from_str(include_str!(
            "../datasets/source-index-failure-reader-1.json"
        ))
        .unwrap();
        let original = &corpus["rows"][0]["metadata"];
        let id = Uuid::parse_str(original["task_id"].as_str().unwrap()).unwrap();
        for descriptor in [
            serde_json::json!({"exc_module":"not_imported","exc_type":"NoPermissionError","exc_message":["denied"]}),
            serde_json::json!({"exc_module":"builtins","exc_type":"NoPermissionError","exc_message":["denied"]}),
            serde_json::json!({"exc_module":"redis.exceptions","exc_type":"NoPermissionError","exc_message":[21,"denied"]}),
            serde_json::json!({"exc_module":"builtins","exc_type":"IsADirectoryError","exc_message":[21.0,"Is a directory"]}),
            serde_json::json!({"exc_module":"builtins","exc_type":"IsADirectoryError","exc_message":[13,"Is a directory"]}),
        ] {
            let mut metadata = original.clone();
            metadata["result"] = descriptor;
            assert_eq!(
                decode(&serde_json::to_vec(&metadata).unwrap(), id),
                Err(DecodeError::UnsupportedException)
            );
        }
        let mut metadata = original.clone();
        metadata["result"] = serde_json::json!({"exc_module":"redis.exceptions","exc_type":"NoPermissionError","exc_message":["x".repeat(8193)]});
        assert_eq!(
            decode(&serde_json::to_vec(&metadata).unwrap(), id),
            Err(DecodeError::Capacity)
        );
    }
    #[test]
    fn actual_source_raw_metadata_shapes_decode_with_declared_normalization() {
        let values: Value =
            serde_json::from_str(include_str!("../datasets/source-results.json")).unwrap();
        let id = Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap();
        let mut tags = 0;
        let mut failures = 0;
        for row in values["rows"].as_array().unwrap() {
            let raw = serde_json::to_vec(&row["metadata"]).unwrap();
            let value = decode(&raw, id).unwrap();
            assert_eq!(value.result, row["metadata"]["result"]);
            assert_eq!(value.task_id, id);
            tags += value.tagged_values.len();
            failures += usize::from(value.exception.is_some());
        }
        assert_eq!(values["rows"].as_array().unwrap().len(), 24);
        assert!(tags >= 6);
        assert!(failures >= 4);
    }
    #[test]
    fn corrupt_oversized_wrong_identity_or_unobserved_types_never_become_success() {
        let id = Uuid::nil();
        assert_eq!(
            decode(&vec![b'x'; MAX_RESULT_BYTES + 1], id),
            Err(DecodeError::Capacity)
        );
        assert_eq!(decode(b"invalid-json", id), Err(DecodeError::Malformed));
        let mut value = serde_json::json!({"task_id":id.to_string(),"status":"SUCCESS","result":{},"children":[],"date_done":null,"traceback":null});
        assert!(decode(&serde_json::to_vec(&value).unwrap(), Uuid::new_v4()).is_err());
        value["result"] = serde_json::json!({"__type__":"untrusted","__value__":"not evaluated"});
        assert_eq!(
            decode(&serde_json::to_vec(&value).unwrap(), id),
            Err(DecodeError::UnsupportedTag)
        );
        value["result"] = serde_json::json!({"exc_type":"Dangerous","exc_module":"not_imported","exc_message":[]});
        value["status"] = Value::String("FAILURE".to_owned());
        assert_eq!(
            decode(&serde_json::to_vec(&value).unwrap(), id),
            Err(DecodeError::UnsupportedException)
        );
    }
}
