//! Original consumer dispatch and Python truthiness for captured principals.
use serde_json::Value;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Visibility {
    Public,
    Administrative,
    Invalid,
}

fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_none_or(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

/// Invalid reproduces an exceptional Source consumer dispatch: close1011.
/// Falsy nonobjects become an empty mapping for the policy but the original
/// envelope is still sent unchanged. This distinction matters for false/0/[].
pub fn visibility(event: &Value) -> Visibility {
    if event.get("type").and_then(Value::as_str) != Some("update") {
        return Visibility::Invalid;
    }
    let Some(data) = event.get("data").filter(|data| truthy(data)) else {
        return Visibility::Public;
    };
    let Some(data) = data.as_object() else {
        return Visibility::Invalid;
    };
    match data.get("type") {
        Some(Value::Array(_) | Value::Object(_)) => return Visibility::Invalid, // unhashable frozenset lookup
        Some(Value::String(kind)) => {
            if matches!(
                kind.as_str(),
                "channel_stats" | "vod_stats" | "timeshift_stats" | "vod_started" | "vod_stopped"
            ) {
                return Visibility::Administrative;
            }
            if kind == "system_notification" {
                if let Some(notification) = data.get("notification").filter(|value| truthy(value)) {
                    let Some(notification) = notification.as_object() else {
                        return Visibility::Invalid;
                    };
                    if notification.get("admin_only").is_some_and(truthy) {
                        return Visibility::Administrative;
                    }
                }
            }
        }
        _ => {}
    }
    Visibility::Public
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn event_filter_matches_actual_pinned_daphne_wire_cases() {
        let source: Value = serde_json::from_str(include_str!(
            "../../../../scripts/rust-realtime-contracts/verified-source-wire-1.json"
        ))
        .unwrap();
        let mut compared = 0;
        for row in source["cases"].as_array().unwrap() {
            let data = match row["case"].as_str().unwrap() {
                "event-filter" | "falsy-update" => row["data"].clone(),
                "notification-filter" => {
                    json!({"type":"system_notification","notification":{"admin_only":row["flag"]}})
                }
                _ => continue,
            };
            let expected = if row["received"][1].is_null() {
                Visibility::Administrative
            } else {
                Visibility::Public
            };
            assert_eq!(visibility(&json!({"type":"update","data":data})), expected);
            compared += 1;
        }
        assert_eq!(compared, 22);
    }
    #[test]
    fn exceptional_payload_shapes_remain_distinct_from_falsy_updates() {
        for data in [
            json!(true),
            json!(1),
            json!("truthy"),
            json!([1]),
            json!({"type":[]}),
            json!({"type":{}}),
            json!({"type":"system_notification","notification":[1]}),
        ] {
            assert_eq!(
                visibility(&json!({"type":"update","data":data})),
                Visibility::Invalid
            );
        }
        for event in [json!({}), json!({"type":"unknown"}), json!({"type":false})] {
            assert_eq!(visibility(&event), Visibility::Invalid);
        }
        for data in [
            Value::Null,
            json!(false),
            json!(0),
            json!(""),
            json!([]),
            json!({}),
            json!({"type":false}),
            json!({"type":"future"}),
        ] {
            assert_eq!(
                visibility(&json!({"type":"update","data":data})),
                Visibility::Public
            );
        }
    }
}
