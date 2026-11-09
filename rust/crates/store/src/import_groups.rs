//! Provider group refresh port from original Dispatcharr process_groups.
//! Original authors retain full application/algorithm credit; see ATTRIBUTION.md.
use crate::{json_compat::python_equal, Store, StoreError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sqlx::Row;
use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
};
pub const MAX_IMPORT_GROUPS: usize = 32768;
pub const MAX_IMPORT_GROUP_BYTES: usize = 16 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImportedGroup {
    pub name: String,
    pub custom_properties: Map<String, Value>,
}
#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub struct GroupImportCounts {
    pub created_groups: usize,
    pub created_relationships: usize,
    pub updated_relationships: usize,
}
struct Budget(usize);
impl Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| io::Error::other("Group import byte capacity"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn invalid() -> StoreError {
    StoreError::InvalidInput(json!({"detail":"Group import capacity or properties are invalid."}))
}
fn dictionary(value: &Value) -> Map<String, Value> {
    if let Some(value) = value.as_object() {
        return value.clone();
    }
    value
        .as_str()
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}
fn django_boolean(value: &Value) -> Option<bool> {
    if python_equal(value, &json!(true)) {
        return Some(true);
    }
    if python_equal(value, &json!(false)) {
        return Some(false);
    }
    match value.as_str() {
        Some("t" | "True" | "1") => Some(true),
        Some("f" | "False" | "0") => Some(false),
        _ => None,
    }
}
fn refreshed_properties(raw: &Value, incoming: &Map<String, Value>) -> Value {
    let mut old = dictionary(raw);
    let next = incoming.get("xc_id").unwrap_or(&Value::Null);
    if !python_equal(next, old.get("xc_id").unwrap_or(&Value::Null)) {
        if next.is_null() {
            old.remove("xc_id");
        } else {
            old.insert("xc_id".into(), next.clone());
        }
        Value::Object(old)
    } else if raw.is_string() {
        Value::Object(old)
    } else {
        raw.clone()
    }
}
impl Store {
    /// Caller supplies the same account-settings snapshot and scan timestamp as
    /// the source task. Group creation and relation writes retain the original
    /// separate bulk-write boundaries; this is not one invented transaction.
    /// No model-save hooks, manual-group marking or refresh jobs are invoked.
    pub async fn import_m3u_groups(
        &self,
        account_id: i64,
        account_properties: &Value,
        groups: Vec<ImportedGroup>,
        scan_time: DateTime<Utc>,
    ) -> Result<GroupImportCounts, StoreError> {
        if groups.len() > MAX_IMPORT_GROUPS {
            return Err(invalid());
        }
        serde_json::to_writer(&mut Budget(MAX_IMPORT_GROUP_BYTES), &groups)
            .map_err(|_| invalid())?;
        let mut seen = HashSet::new();
        if groups.iter().any(|group| !seen.insert(group.name.as_str())) {
            return Err(invalid());
        }
        if groups.is_empty() {
            return Ok(GroupImportCounts::default());
        }
        let names = groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>();
        let existing=sqlx::query("SELECT id::bigint AS id,name FROM dispatcharr_channels_channelgroup WHERE name=ANY($1)")
            .bind(&names).fetch_all(&self.pool).await?;
        let mut all = existing
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<i64, _>("id")?,
                    row.try_get::<String, _>("name")?,
                ))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        let existing_names = all
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<HashSet<_>>();
        let missing = groups
            .iter()
            .filter(|group| !existing_names.contains(group.name.as_str()))
            .map(|group| group.name.clone())
            .collect::<Vec<_>>();
        let mut counts = GroupImportCounts {
            created_groups: missing.len(),
            ..GroupImportCounts::default()
        };
        if !missing.is_empty() {
            sqlx::query("INSERT INTO dispatcharr_channels_channelgroup(name) SELECT value FROM jsonb_array_elements_text($1) AS names(value)")
                .bind(json!(missing)).execute(&self.pool).await?;
            let created=sqlx::query("SELECT id::bigint AS id,name FROM dispatcharr_channels_channelgroup WHERE name=ANY($1)")
                .bind(&missing).fetch_all(&self.pool).await?;
            for row in created {
                all.push((row.try_get("id")?, row.try_get("name")?));
            }
        }
        let ids = all.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let existing_bytes:i64=sqlx::query_scalar("SELECT COALESCE(sum(octet_length(custom_properties::text)),0)::bigint FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 AND channel_group_id=ANY($2)")
            .bind(account_id).bind(&ids).fetch_one(&self.pool).await?;
        if existing_bytes > MAX_IMPORT_GROUP_BYTES as i64 {
            return Err(invalid());
        }
        let existing=sqlx::query("SELECT id::bigint AS id,channel_group_id::bigint AS channel_group_id,custom_properties FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 AND channel_group_id=ANY($2)")
            .bind(account_id).bind(&ids).fetch_all(&self.pool).await?;
        let existing = existing
            .into_iter()
            .map(|row| Ok((row.try_get::<i64, _>("channel_group_id")?, row)))
            .collect::<Result<HashMap<_, _>, sqlx::Error>>()?;
        let incoming = groups
            .iter()
            .map(|group| (group.name.as_str(), &group.custom_properties))
            .collect::<HashMap<_, _>>();
        let auto = dictionary(account_properties)
            .get("auto_enable_new_groups_live")
            .cloned()
            .unwrap_or(json!(true));
        let mut creates = Vec::new();
        let mut updates = Vec::new();
        for (id, name) in all {
            let properties = incoming.get(name.as_str()).expect("requested group");
            if let Some(old) = existing.get(&id) {
                let raw = old
                    .try_get::<Option<Value>, _>("custom_properties")?
                    .unwrap_or(Value::Null);
                updates.push(json!({"id":old.try_get::<i64,_>("id")?,"custom_properties":refreshed_properties(&raw,properties),"last_seen":scan_time}));
            } else {
                // Django BooleanField conversion happens only when a new
                // relation is serialized, not while touching existing ones.
                let enabled = django_boolean(&auto).ok_or_else(invalid)?;
                creates.push(json!({"channel_group_id":id,"m3u_account_id":account_id,"custom_properties":properties,
                    "enabled":enabled,"auto_channel_sync":false,"last_seen":scan_time,"is_stale":false}));
            }
        }
        counts.created_relationships = creates.len();
        counts.updated_relationships = updates.len();
        if !creates.is_empty() {
            sqlx::query("INSERT INTO dispatcharr_channels_channelgroupm3uaccount(channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,last_seen,is_stale) SELECT channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,last_seen,is_stale FROM jsonb_populate_recordset(NULL::dispatcharr_channels_channelgroupm3uaccount,$1) ON CONFLICT DO NOTHING")
            .bind(json!(creates)).execute(&self.pool).await?;
        }
        if !updates.is_empty() {
            sqlx::query("UPDATE dispatcharr_channels_channelgroupm3uaccount AS target SET custom_properties=incoming.custom_properties,last_seen=incoming.last_seen,is_stale=false FROM jsonb_populate_recordset(NULL::dispatcharr_channels_channelgroupm3uaccount,$1) AS incoming WHERE target.id=incoming.id")
            .bind(json!(updates)).execute(&self.pool).await?;
        }
        Ok(counts)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_python_numeric_equality_legacy_strings_and_nonobject_unchanged_values() {
        let props = serde_json::from_value::<Map<String, Value>>(json!({"xc_id":1})).unwrap();
        assert_eq!(
            refreshed_properties(&json!({"xc_id":true,"keep":"user"}), &props),
            json!({"xc_id":true,"keep":"user"})
        );
        assert_eq!(
            refreshed_properties(&json!("{\"xc_id\":1,\"keep\":true}"), &props),
            json!({"xc_id":1,"keep":true})
        );
        assert_eq!(
            refreshed_properties(&json!([1, 2]), &Map::new()),
            json!([1, 2])
        );
        assert_eq!(
            refreshed_properties(&json!({"xc_id":1,"keep":true}), &Map::new()),
            json!({"keep":true})
        );
        assert_eq!(
            refreshed_properties(&json!("not json"), &Map::new()),
            json!({})
        );
    }
}
