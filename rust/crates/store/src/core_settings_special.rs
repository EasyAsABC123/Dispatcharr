//! Inert special-group Source51 policy and serialization boundaries.
//! Original Dispatcharr model/serializer/signal authors retain full credit.
//! This file is private and unregistered until the concrete retained effects
//! are complete. It does not enable a CoreSettings write or Main/default task.
use crate::StoreError;
use serde_json::{json, Map, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Group {
    System,
    Network,
    ReverseProxy,
    Proxy,
    Stream,
    Dvr,
}
impl Group {
    pub(crate) fn parse(key: &str) -> Option<Self> {
        Some(match key {
            "system_settings" => Self::System,
            "network_access" => Self::Network,
            "reverse_proxy_auth" => Self::ReverseProxy,
            "proxy_settings" => Self::Proxy,
            "stream_settings" => Self::Stream,
            "dvr_settings" => Self::Dvr,
            _ => return None,
        })
    }
}

/// Separate old-key serializer/view dispatch from resulting-key signals.
/// Source51 cases7/8,17/18,25/26,33/34,41/42,50/51 reached both branches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Dispatch {
    pub(crate) old: Option<Group>,
    pub(crate) new: Option<Group>,
    pub(crate) creation: bool,
    pub(crate) deletion: bool,
}
impl Dispatch {
    pub(crate) fn new(old: Option<&str>, new: Option<&str>) -> Self {
        Self {
            old: old.and_then(Group::parse),
            new: new.and_then(Group::parse),
            creation: old.is_none(),
            deletion: new.is_none(),
        }
    }
    #[cfg(test)]
    pub(crate) fn serializer(self) -> Option<Group> {
        (!self.creation && !self.deletion)
            .then_some(self.old)
            .flatten()
    }
    #[cfg(test)]
    pub(crate) fn signal(self) -> Option<Group> {
        if self.deletion {
            self.old
        } else {
            self.new
        }
    }
    /// Only save runs network notice sync. Delete only invalidates group cache.
    #[cfg(test)]
    pub(crate) fn network_sync(self) -> bool {
        !self.deletion && self.new == Some(Group::Network)
    }
}

fn uncaptured() -> StoreError {
    StoreError::Unsupported("Special CoreSettings value shape is outside Source51")
}
fn truthy(value: Option<&Value>) -> bool {
    crate::channels::truthy(value)
}

/// Uses the already bounded generic JSON value. The supplied scalar clamp is
/// the existing Source integer parser; no duplicate grammar/Cargo edge exists.
/// Create has no special normalization (Source create only clamps EPG).
pub(crate) fn normalize_update(
    group: Group,
    value: &mut Value,
    clamp: impl Fn(Option<&Value>, i64, i64) -> i64,
) -> Result<(), StoreError> {
    match group {
        Group::System => {
            let object = value.as_object_mut().ok_or_else(uncaptured)?;
            if object.contains_key("log_max_mb") {
                let n = clamp(object.get("log_max_mb"), 5, 20).max(1);
                object.insert("log_max_mb".into(), json!(n));
            }
            if object.contains_key("log_keep") {
                let n = clamp(object.get("log_keep"), 5, 50).max(2);
                object.insert("log_keep".into(), json!(n));
            }
            if object.contains_key("log_persist") {
                // Python `is not False`: integer0 and null become True.
                let persist = object.get("log_persist") != Some(&Value::Bool(false));
                object.insert("log_persist".into(), json!(persist));
            }
        }
        Group::Network => {
            let object = value.as_object().ok_or_else(uncaptured)?;
            let mut invalid = Map::new();
            for (key, entry) in object {
                let text = entry.as_str().ok_or_else(uncaptured)?;
                let bad = text
                    .split(',')
                    .filter(|part| crate::xc_auth::network(part, true).is_err())
                    .map(|part| json!(part))
                    .collect::<Vec<_>>();
                if !bad.is_empty() {
                    invalid.insert(key.clone(), Value::Array(bad));
                }
            }
            if !invalid.is_empty() {
                return Err(StoreError::InvalidInput(json!({
                    "message":"Invalid CIDRs", "value": invalid
                })));
            }
        }
        Group::ReverseProxy => {
            let object = value.as_object_mut().ok_or_else(uncaptured)?;
            // Source's `(value or {})` makes a NEW map for an empty dict;
            // mutations to that temporary are not assigned back to validated_data.
            if object.is_empty() {
                return Ok(());
            }
            let header = match object.get("header") {
                None | Some(Value::Null) | Some(Value::Bool(false)) => "",
                Some(Value::String(text)) => text.trim(),
                _ => return Err(uncaptured()),
            };
            let enabled = truthy(object.get("enabled"));
            if enabled && header.is_empty() {
                return Err(StoreError::InvalidInput(json!({
                    "message":"A header name is required to enable reverse proxy auth."
                })));
            }
            if !header.is_empty()
                && (header.len() > 64
                    || !header.as_bytes()[0].is_ascii_alphanumeric()
                    || !header
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-'))
            {
                return Err(StoreError::InvalidInput(json!({
                    "message":"Invalid header name. Use letters, digits and dashes only (for example X-Forwarded-User).",
                    "value":header
                })));
            }
            let header = header.to_owned();
            object.insert("header".into(), json!(header));
            object.insert("enabled".into(), json!(enabled));
        }
        Group::Dvr => {
            if let Some(object) = value.as_object_mut() {
                if let Some(rules) = object.get_mut("series_rules") {
                    match rules {
                        Value::Array(rows) => rows.retain(Value::is_object),
                        _ => *rules = json!([]),
                    }
                }
            }
        }
        Group::Proxy | Group::Stream => {}
    }
    Ok(())
}

/// Actual Source checks raw request offsets against the old row after update,
/// including a rename away from DVR. Omitted offsets are None, not defaults.
pub(crate) fn dvr_publish(dispatch: Dispatch, old: Option<&Value>, incoming: &Value) -> bool {
    let Some(new) = incoming.as_object() else {
        return false;
    };
    if dispatch.creation {
        return dispatch.new == Some(Group::Dvr)
            && (new.contains_key("pre_offset_minutes") || new.contains_key("post_offset_minutes"));
    }
    if dispatch.old != Some(Group::Dvr) || dispatch.deletion {
        return false;
    }
    let Some(old) = old.and_then(Value::as_object) else {
        return false;
    };
    old.get("pre_offset_minutes") != new.get("pre_offset_minutes")
        || old.get("post_offset_minutes") != new.get("post_offset_minutes")
}

/// Same allowed fields/order-preserving de-duplication as pinned M3U parser.
/// Set comparison, not list comparison, controls global rehash pre-admission.
pub(crate) fn hash_fields(value: Option<&Value>) -> Result<Vec<String>, StoreError> {
    let text = match value {
        None | Some(Value::Null) => "",
        Some(Value::String(v)) => v,
        _ => return Err(uncaptured()),
    };
    Ok(hash_fields_text(text))
}
pub(crate) fn hash_fields_text(text: &str) -> Vec<String> {
    let mut result = Vec::new();
    for part in text.split(',').map(str::trim) {
        if ["name", "url", "tvg_id", "m3u_id", "group"].contains(&part)
            && !result.iter().any(|v| v == part)
        {
            result.push(part.to_owned());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renamed_group_dispatches_old_serializer_and_new_signal() {
        for group in [
            "system_settings",
            "network_access",
            "reverse_proxy_auth",
            "proxy_settings",
            "stream_settings",
            "dvr_settings",
        ] {
            let away = Dispatch::new(Some(group), Some("owned.renamed"));
            assert_eq!(away.serializer(), Group::parse(group));
            assert_eq!(away.signal(), None);
            assert!(!away.network_sync());
            let back = Dispatch::new(Some("owned.renamed"), Some(group));
            assert_eq!(back.serializer(), None);
            assert_eq!(back.signal(), Group::parse(group));
            assert_eq!(back.network_sync(), group == "network_access");
            let deleted = Dispatch::new(Some(group), None);
            assert_eq!(deleted.signal(), Group::parse(group));
            assert!(!deleted.network_sync());
        }
    }
    #[test]
    fn reordering_hash_fields_does_not_publish_and_offsets_use_incoming_missing() {
        assert!(!crate::core_settings_special_broker::changed(
            &hash_fields(Some(&json!("name,url"))).unwrap(),
            &hash_fields(Some(&json!("url,name,name"))).unwrap()
        ));
        let away = Dispatch::new(Some("dvr_settings"), Some("owned.renamed"));
        assert!(dvr_publish(
            away,
            Some(&json!({"pre_offset_minutes":1,"post_offset_minutes":2})),
            &json!({"series_rules":[]})
        ));
        assert!(!dvr_publish(
            Dispatch::new(Some("owned.renamed"), Some("dvr_settings")),
            Some(&json!({})),
            &json!({"pre_offset_minutes":2})
        ));
    }
}
