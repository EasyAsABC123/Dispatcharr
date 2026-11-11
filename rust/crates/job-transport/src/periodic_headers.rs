//! Bounded marker for captured periodic Refresh/Series task envelopes only.
//! Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
//! Generic task validation remains strict; callers choose the known family first.
use crate::protocol::ProtocolError;
use serde_json::Value;
fn validate(value: &str) -> Result<(), ProtocolError> {
    if value.is_empty() || value.chars().count() > 200 || value.contains('\0') {
        return Err(ProtocolError::InvalidArgument);
    }
    Ok(())
}
pub(crate) fn take(envelope: &mut Value) -> Result<Option<String>, ProtocolError> {
    let headers = envelope
        .get_mut("headers")
        .and_then(Value::as_object_mut)
        .ok_or(ProtocolError::InvalidArgument)?;
    let Some(value) = headers.remove("periodic_task_name") else {
        return Ok(None);
    };
    let name = value.as_str().ok_or(ProtocolError::InvalidArgument)?;
    validate(name)?;
    Ok(Some(name.to_owned()))
}
pub(crate) fn put(envelope: &mut Value, name: &str) -> Result<(), ProtocolError> {
    validate(name)?;
    let headers = envelope
        .get_mut("headers")
        .and_then(Value::as_object_mut)
        .ok_or(ProtocolError::InvalidArgument)?;
    if headers
        .insert(
            "periodic_task_name".to_owned(),
            Value::String(name.to_owned()),
        )
        .is_some()
    {
        return Err(ProtocolError::InvalidArgument);
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn only_known_typed_marker_is_removed_and_other_headers_stay_strict() {
        let mut value =
            json!({"headers":{"periodic_task_name":"owned","foreign":"kept-for-validator"}});
        assert_eq!(take(&mut value).unwrap(), Some("owned".to_owned()));
        assert_eq!(value, json!({"headers":{"foreign":"kept-for-validator"}}));
        for bad in [
            Value::Null,
            json!([]),
            json!({}),
            json!(false),
            json!(1),
            json!(""),
            json!("x".repeat(201)),
            json!("owned\0name"),
        ] {
            let mut value = json!({"headers":{"periodic_task_name":bad}});
            assert!(take(&mut value).is_err());
        }
    }
}

#[cfg(test)]
mod observed_workers {
    use serde_json::{json, Value};
    #[test]
    fn actual_source_periodic_workers_keep_body_result_and_common_validation() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-periodic-workers-1.json"))
                .unwrap();
        for case in source["cases"].as_array().unwrap() {
            let envelope = &case["envelope"];
            let bytes = serde_json::to_vec(envelope).unwrap();
            let mut plain = envelope.clone();
            plain["headers"]
                .as_object_mut()
                .unwrap()
                .remove("periodic_task_name");
            let plain = serde_json::to_vec(&plain).unwrap();
            if case["name"] == "refresh" {
                let message = crate::refresh_protocol::parse(&bytes).unwrap();
                assert_eq!(message, crate::refresh_protocol::parse(&plain).unwrap());
                let metadata = serde_json::to_vec(&case["metadata"]).unwrap();
                let result = crate::refresh_result::parse_success(
                    &metadata,
                    message.task_id,
                    message.source_id,
                )
                .unwrap();
                assert!(
                    !result.missing_source && result.children.is_empty() && result.parent.is_none()
                );
            } else {
                let message = crate::series_protocol::parse(&bytes).unwrap();
                assert_eq!(message, crate::series_protocol::parse(&plain).unwrap());
                let metadata = serde_json::to_vec(&case["metadata"]).unwrap();
                assert_eq!(
                    crate::series_protocol::parse_success(&metadata, &message).unwrap(),
                    crate::series_protocol::KnownReturn::NoRules
                );
            }
            assert!(crate::worker::create_identity(&bytes).is_err());
            let mut unknown = envelope.clone();
            unknown["headers"]["unknown_authority"] = json!(true);
            let bad = serde_json::to_vec(&unknown).unwrap();
            assert!(if case["name"] == "refresh" {
                crate::refresh_protocol::parse(&bad).is_err()
            } else {
                crate::series_protocol::parse(&bad).is_err()
            });
        }
    }
}

#[cfg(test)]
#[path = "periodic_publisher_peer.rs"]
mod peer;
