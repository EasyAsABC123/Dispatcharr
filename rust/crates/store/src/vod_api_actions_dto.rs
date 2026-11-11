//! Source-validated provider/action DTOs with original serializer field order.
//! Actual Source/native component gates passed; app/browser acceptance is
//! separate. Original Dispatcharr authors retain application/algorithm credit.
use super::vod_api_projection::truthy;
use serde_json::{json, Map, Value};
#[derive(Debug, PartialEq, Eq)]
pub(super) enum ActionFault {
    NonMappingQuality,
    InvalidTimestamp,
}
fn fields(raw: &Value, names: &[&str]) -> Value {
    Value::Object(
        names
            .iter()
            .map(|name| ((*name).into(), raw[*name].clone()))
            .collect(),
    )
}
/// Current Movie/Episode model schemas have neither video nor bitrate fields.
/// Their serializer's dynamic-attribute branches cannot be populated by SQL.
/// Do not invent those attributes from custom_properties.
pub(super) fn quality(properties: &Value, name: &str) -> Result<Value, ActionFault> {
    if truthy(properties) {
        let object = properties
            .as_object()
            .ok_or(ActionFault::NonMappingQuality)?;
        for key in ["quality", "resolution"] {
            if let Some(value) = object.get(key).filter(|value| truthy(value)) {
                return Ok(Value::Object(Map::from_iter([(key.into(), value.clone())])));
            }
        }
    }
    for (needles, label) in [
        (&["4K", "2160p"][..], "4K"),
        (&["1080p", "FHD"][..], "1080p"),
        (&["720p", "HD"][..], "720p"),
        (&["480p"][..], "480p"),
    ] {
        if needles.iter().any(|needle| name.contains(needle)) {
            return Ok(json!({"quality":label}));
        }
    }
    Ok(Value::Null)
}
fn timestamp(raw: &Value) -> Result<Value, ActionFault> {
    let Some(text) = raw.as_str() else {
        return Ok(Value::Null);
    };
    let parsed = chrono::DateTime::parse_from_rfc3339(text)
        .map_err(|_| ActionFault::InvalidTimestamp)?
        .with_timezone(&chrono::Utc);
    Ok(if parsed.timestamp_subsec_micros() == 0 {
        parsed.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        parsed.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
    }
    .into())
}
#[derive(Clone, Copy)]
pub(super) enum RelationKind {
    Movie,
    Series,
    Episode,
}
/// Nested serializers have no request context, including for admin. They use
/// relative logo URLs and never re-add the account's write-only password.
pub(super) fn relation(
    kind: RelationKind,
    raw: &Value,
    content: Value,
    category: Value,
    account: Value,
) -> Result<Value, ActionFault> {
    let content_key = match kind {
        RelationKind::Movie => "movie",
        RelationKind::Series => "series",
        RelationKind::Episode => "episode",
    };
    let quality_value = if matches!(kind, RelationKind::Series) {
        None
    } else {
        Some(quality(
            &raw["custom_properties"],
            content["name"].as_str().unwrap_or(""),
        )?)
    };
    let mut output = Map::new();
    output.insert("id".into(), raw["id"].clone());
    output.insert(content_key.into(), content);
    if !matches!(kind, RelationKind::Episode) {
        output.insert("category".into(), category);
    }
    output.insert("m3u_account".into(), account);
    if let Some(quality) = quality_value {
        output.insert("quality_info".into(), quality);
    }
    output.insert("custom_properties".into(), raw["custom_properties"].clone());
    let columns: &[&str] = match kind {
        RelationKind::Movie => &["stream_id", "container_extension"],
        RelationKind::Series => &["external_series_id"],
        RelationKind::Episode => &["stream_id", "container_extension"],
    };
    for key in columns {
        output.insert((*key).into(), raw[*key].clone());
    }
    for key in ["created_at", "updated_at"] {
        output.insert(key.into(), timestamp(&raw[key])?);
    }
    let refresh = match kind {
        RelationKind::Movie => Some("last_advanced_refresh"),
        RelationKind::Series => Some("last_episode_refresh"),
        RelationKind::Episode => None,
    };
    if let Some(key) = refresh {
        output.insert(key.into(), timestamp(&raw[key])?);
    }
    output.insert("last_seen".into(), timestamp(&raw["last_seen"])?);
    if matches!(kind, RelationKind::Episode) {
        output.insert("series_relation".into(), raw["series_relation_id"].clone());
    }
    Ok(output.into())
}
/// `profiles`/`filters`/`groups` must be supplied by bounded Source-scheduled
/// readers; no aggregation silently discards rows or changes duplicate order.
/// This projector orders exactly the fields declared by M3UAccountSerializer.
pub(super) struct AccountMetadata {
    pub profiles: Value,
    pub filters: Value,
    pub groups: Value,
    pub cron: Value,
    pub earliest: Value,
    pub expirations: Value,
    pub default_expiry: Value,
}
pub(super) fn account(raw: &Value, metadata: AccountMetadata) -> Result<Value, ActionFault> {
    let AccountMetadata {
        profiles,
        filters,
        groups,
        cron,
        earliest,
        expirations,
        default_expiry,
    } = metadata;
    let mut output = fields(
        raw,
        &[
            "id",
            "name",
            "server_url",
            "file_path",
            "server_group_id",
            "max_streams",
            "is_active",
            "created_at",
            "updated_at",
        ],
    );
    let map = output.as_object_mut().expect("fields object");
    // Rename in place without moving its declared ordinal.
    let ordered = std::mem::take(map);
    for (key, value) in ordered {
        map.insert(
            if key == "server_group_id" {
                "server_group".into()
            } else {
                key
            },
            value,
        );
    }
    for key in ["created_at", "updated_at"] {
        map.insert(key.into(), timestamp(&raw[key])?);
    }
    map.insert("filters".into(), filters);
    map.insert("user_agent".into(), raw["user_agent_id"].clone());
    map.insert("profiles".into(), profiles);
    map.insert("locked".into(), raw["locked"].clone());
    map.insert("channel_groups".into(), groups);
    map.insert("refresh_interval".into(), raw["refresh_interval"].clone());
    map.insert("cron_expression".into(), cron);
    for key in [
        "custom_properties",
        "account_type",
        "username",
        "stale_stream_days",
        "priority",
        "status",
        "last_message",
    ] {
        map.insert(key.into(), raw[key].clone());
    }
    map.insert("earliest_expiration".into(), earliest);
    map.insert("all_expirations".into(), expirations);
    let props = &raw["custom_properties"];
    if truthy(props) && !props.is_object() {
        return Err(ActionFault::NonMappingQuality);
    }
    for (key, default) in [
        ("enable_vod", false),
        ("auto_enable_new_groups_live", true),
        ("auto_enable_new_groups_vod", true),
        ("auto_enable_new_groups_series", true),
    ] {
        map.insert(
            key.into(),
            props.get(key).cloned().unwrap_or(default.into()),
        );
    }
    map.insert("hash_key".into(), props["hash_key"].clone());
    map.insert("exp_date".into(), default_expiry);
    Ok(output)
}
/// EpisodeWithProvidersSerializer declares providers before model fields.
/// It does not filter the outer episode list by provider presence/activity.
pub(super) fn episode_with_providers(episode: Value, providers: Value) -> Value {
    let mut result = Map::new();
    let mut providers = Some(providers);
    if let Value::Object(fields) = episode {
        for (key, value) in fields {
            result.insert(key.clone(), value);
            if key == "custom_properties" {
                result.insert(
                    "providers".into(),
                    providers.take().expect("single custom_properties field"),
                );
            }
        }
    }
    Value::Object(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source150_nested_relation_occurrences() {
        let source: Value = serde_json::from_str(include_str!(
            "../datasets/vod-provider-action-dto-source-3.json"
        ))
        .unwrap();
        let rows = source["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 88);
        for expected in rows {
            let (kind, key) = if expected.get("movie").is_some() {
                (RelationKind::Movie, "movie")
            } else if expected.get("episode").is_some() {
                (RelationKind::Episode, "episode")
            } else {
                (RelationKind::Series, "series")
            };
            let mut raw = expected.clone();
            raw["series_relation_id"] = raw["series_relation"].clone();
            let acc = &expected["m3u_account"];
            let mut account_raw = acc.clone();
            account_raw["server_group_id"] = acc["server_group"].clone();
            account_raw["user_agent_id"] = acc["user_agent"].clone();
            let account = account(
                &account_raw,
                AccountMetadata {
                    profiles: acc["profiles"].clone(),
                    filters: acc["filters"].clone(),
                    groups: acc["channel_groups"].clone(),
                    cron: acc["cron_expression"].clone(),
                    earliest: acc["earliest_expiration"].clone(),
                    expirations: acc["all_expirations"].clone(),
                    default_expiry: acc["exp_date"].clone(),
                },
            )
            .unwrap();
            assert_eq!(
                serde_json::to_vec(&account).unwrap(),
                serde_json::to_vec(acc).unwrap(),
                "exact nested account fields/order"
            );
            let projected = relation(
                kind,
                &raw,
                expected[key].clone(),
                expected["category"].clone(),
                account,
            )
            .unwrap();
            assert_eq!(
                serde_json::to_vec(&projected).unwrap(),
                serde_json::to_vec(expected).unwrap(),
                "exact relation fields/order"
            );
        }
    }
    #[test]
    fn captured_nonmapping_quality_fault_and_declared_episode_position() {
        assert_eq!(
            quality(&json!("truthy-nonmapping"), "Alpha"),
            Err(ActionFault::NonMappingQuality)
        );
        let result = episode_with_providers(
            json!({"id":1,"series":{},"custom_properties":null,"uuid":"x"}),
            json!([]),
        );
        assert_eq!(
            serde_json::to_string(&result).unwrap(),
            r#"{"id":1,"series":{},"custom_properties":null,"providers":[],"uuid":"x"}"#
        );
    }
}
