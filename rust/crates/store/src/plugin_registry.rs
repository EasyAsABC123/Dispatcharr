//! Disabled plugin registry and exact stored settings, without executing Python.
use super::*;
#[derive(Clone)]
pub(super) struct Snapshot {
    pub configs: Vec<Value>,
    pub repos: Vec<Value>,
}
fn guard() -> StoreError {
    StoreError::Unsupported("Enabled Python plugins require the owned compatibility host")
}
pub(super) async fn snapshot(session: &crate::TaskSqlSession) -> Result<Snapshot, StoreError> {
    session.run(|c|Box::pin(async move{
  let mut result=Vec::new();for table in ["plugins_pluginconfig","plugins_pluginrepo"]{
   let sql=if table=="plugins_pluginconfig"{"WITH bounded AS(SELECT c.*,r.name AS source_repo_name,r.is_official AS is_official_repo FROM plugins_pluginconfig c LEFT JOIN plugins_pluginrepo r ON r.id=c.source_repo_id ORDER BY c.id LIMIT 257),payload AS(SELECT count(*) AS n,COALESCE(jsonb_agg(to_jsonb(bounded)),'[]'::jsonb)::text AS raw FROM bounded)SELECT n,octet_length(raw),CASE WHEN n<=256 AND octet_length(raw)<=1048576 THEN raw END FROM payload"}else{"WITH bounded AS(SELECT * FROM plugins_pluginrepo ORDER BY is_official DESC,name LIMIT 257),payload AS(SELECT count(*) AS n,COALESCE(jsonb_agg(to_jsonb(bounded)),'[]'::jsonb)::text AS raw FROM bounded)SELECT n,octet_length(raw),CASE WHEN n<=256 AND octet_length(raw)<=1048576 THEN raw END FROM payload"};
   let (count,size,raw):(i64,i32,Option<String>)=sqlx::query_as(sql).fetch_one(&mut *c).await?;
   let raw=raw.ok_or_else(guard)?;if count>256||size<0||size as usize!=raw.len(){return Err(guard());}
   let mut values:Value=serde_json::from_str(&raw).map_err(|_|guard())?;crate::json_compat::normalize_input_numbers(&mut values).map_err(|_|guard())?;result.push(values.as_array().ok_or_else(guard)?.clone());
  }
  Ok::<_,StoreError>(Snapshot{configs:result.remove(0),repos:result.remove(0)})
 })).await?
}
pub(super) fn disabled(snapshot: &Snapshot) -> Result<(), StoreError> {
    if snapshot.configs.iter().any(|c| c["enabled"] == true) {
        return Err(guard());
    }
    Ok(())
}
pub(super) fn no_history(snapshot: &Snapshot) -> Result<(), StoreError> {
    disabled(snapshot)?;
    if snapshot.configs.iter().any(|c| {
        c["owned_tasks"].as_array().is_none_or(|v| !v.is_empty())
            || c["suspended_schedules"]
                .as_array()
                .is_none_or(|v| !v.is_empty())
    }) {
        return Err(guard());
    }
    Ok(())
}
pub(super) fn mutable(snapshot: &Snapshot) -> Result<(), StoreError> {
    disabled(snapshot)?;
    if snapshot.configs.iter().any(|c| {
        c["ever_enabled"] == true
            || c["owned_tasks"].as_array().is_none_or(|v| !v.is_empty())
            || c["suspended_schedules"]
                .as_array()
                .is_none_or(|v| !v.is_empty())
    }) {
        return Err(guard());
    }
    Ok(())
}
pub(super) async fn sync(
    session: &crate::TaskSqlSession,
    plugins: Vec<files::Discovered>,
) -> Result<(), StoreError> {
    session.run(move|c|Box::pin(async move{
  use sqlx::Connection;
  let mut tx=c.begin().await?;
  for p in plugins{
   let name=p.metadata["name"].as_str().filter(|v|!v.is_empty()).unwrap_or(&p.folder);
   let version=p.metadata["version"].as_str().map(str::to_owned);let description=p.metadata["description"].as_str().map(str::to_owned);
   sqlx::query("INSERT INTO plugins_pluginconfig(key,name,version,description,enabled,ever_enabled,settings,owned_tasks,suspended_schedules,slug,installed_version_is_prerelease,deprecated,created_at,updated_at) VALUES($1,$2,COALESCE($3,''),COALESCE($4,''),false,false,'{}','[]','[]','',false,false,$5,$5) ON CONFLICT(key) DO UPDATE SET name=EXCLUDED.name,version=COALESCE($3,plugins_pluginconfig.version),description=COALESCE($4,plugins_pluginconfig.description),updated_at=CASE WHEN plugins_pluginconfig.name IS DISTINCT FROM EXCLUDED.name OR ($3 IS NOT NULL AND plugins_pluginconfig.version IS DISTINCT FROM $3) OR ($4 IS NOT NULL AND plugins_pluginconfig.description IS DISTINCT FROM $4) THEN EXCLUDED.updated_at ELSE plugins_pluginconfig.updated_at END WHERE plugins_pluginconfig.name IS DISTINCT FROM EXCLUDED.name OR ($3 IS NOT NULL AND plugins_pluginconfig.version IS DISTINCT FROM $3) OR ($4 IS NOT NULL AND plugins_pluginconfig.description IS DISTINCT FROM $4)")
    .bind(p.key).bind(name).bind(version).bind(description).bind(chrono::Utc::now()).execute(&mut *tx).await?;
  }
  tx.commit().await?;Ok::<_,StoreError>(())
 })).await?
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
pub(super) fn list(
    snapshot: &Snapshot,
    plugins: &[files::Discovered],
    origin: &str,
) -> Result<Vec<Value>, StoreError> {
    disabled(snapshot)?;
    let mut latest = std::collections::BTreeMap::new();
    // Source catches a malformed repository traversal, retaining preceding entries.
    'repos: for repo in snapshot.repos.iter().filter(|r| r["enabled"] == true) {
        let data = if truthy(&repo["cached_manifest"]) {
            &repo["cached_manifest"]
        } else {
            continue;
        };
        let manifest = data.get("manifest").unwrap_or(data);
        if !data.is_object() || !manifest.is_object() {
            break;
        }
        let entries = match manifest.get("plugins") {
            None => continue,
            Some(v) => match v.as_array() {
                Some(v) => v,
                None => break,
            },
        };
        for item in entries {
            let Some(item) = item.as_object() else {
                break 'repos;
            };
            if let Some(slug) = item
                .get("slug")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                latest.insert(
                    slug.to_owned(),
                    item.get("latest_version").cloned().unwrap_or(json!("")),
                );
            }
        }
    }
    let mut result = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for p in plugins {
        if p.metadata["fields"].as_array().is_some_and(|fields| {
            fields.iter().any(|field| {
                ["min", "max", "step"].iter().any(|key| {
                    field
                        .get(*key)
                        .and_then(Value::as_str)
                        .and_then(dispatcharr_ingest_core::source_float_literal)
                        .is_some_and(|v| !v.is_finite())
                })
            })
        }) {
            return Err(guard());
        }
        let config = snapshot.configs.iter().find(|c| c["key"] == p.key);
        let mut c=config.cloned().unwrap_or(json!({"enabled":false,"ever_enabled":false,"settings":{},"slug":"","installed_version_is_prerelease":false,"deprecated":false}));
        c["key"] = json!(p.key);
        c["name"] = p.metadata["name"]
            .as_str()
            .filter(|v| !v.is_empty())
            .map(|v| json!(v))
            .unwrap_or(json!(p.folder));
        for key in ["version", "description"] {
            c[key] = p.metadata[key]
                .as_str()
                .map(|v| json!(v))
                .unwrap_or_else(|| config.map(|c| c[key].clone()).unwrap_or(json!("")));
        }
        c["author"] = p.metadata["author"]
            .as_str()
            .map(|v| json!(v))
            .unwrap_or(json!(""));
        c["help_url"] = p.metadata["help_url"]
            .as_str()
            .map(|v| json!(v))
            .unwrap_or(json!(""));
        c["fields"] = json!(manifest_fields::fields(&p.metadata["fields"]));
        c["actions"] = json!(manifest_fields::actions(&p.metadata["actions"]));
        c["missing"] = json!(false);
        c["legacy"] = json!(p.legacy);
        c["logo_url"] = if p.logo {
            json!(format!("{origin}/api/plugins/plugins/{}/logo/", p.key))
        } else {
            Value::Null
        };
        seen.insert(p.key.clone());
        result.push(finalize(c, &latest));
    }
    for config in &snapshot.configs {
        if seen.contains(config["key"].as_str().ok_or_else(guard)?) {
            continue;
        }
        let mut c = config.clone();
        for key in ["author", "help_url"] {
            c[key] = json!("");
        }
        for key in ["fields", "actions"] {
            c[key] = json!([]);
        }
        if !truthy(&c["settings"]) {
            c["settings"] = json!({});
        }
        c["missing"] = json!(true);
        c["legacy"] = json!(false);
        c["logo_url"] = Value::Null;
        result.push(finalize(c, &latest));
    }
    Ok(result)
}
fn finalize(c: Value, latest: &std::collections::BTreeMap<String, Value>) -> Value {
    let repo = !c["source_repo_id"].is_null();
    let slug = c["slug"].as_str().unwrap_or("");
    let version = latest.get(slug).cloned().unwrap_or(json!(""));
    let update = repo
        && !slug.is_empty()
        && c["installed_version_is_prerelease"] != true
        && truthy(&version)
        && c["version"] != version;
    json!({"key":c["key"],"name":c["name"],"version":c["version"],"description":c["description"],"author":c["author"],"help_url":c["help_url"],"enabled":c["enabled"],"ever_enabled":c["ever_enabled"],"fields":c["fields"],"settings":c["settings"],"actions":c["actions"],"missing":c["missing"],"trusted":c["enabled"]==true||c["ever_enabled"]==true,"loaded":false,"legacy":c["legacy"],"logo_url":c["logo_url"],"source_repo":c["source_repo_id"],"source_repo_name":c["source_repo_name"],"is_official_repo":repo&&c["is_official_repo"]==true,"slug":c["slug"],"is_managed":repo,"installed_version_is_prerelease":c["installed_version_is_prerelease"],"update_available":update,"latest_version":version,"deprecated":c["deprecated"]})
}

pub(super) async fn sync_caught(
    session: &crate::TaskSqlSession,
    plugins: Vec<files::Discovered>,
) -> Result<(), StoreError> {
    match sync(session, plugins).await {
        Ok(()) => Ok(()),
        Err(StoreError::Database(error)) if error.as_database_error().is_some() => {
            tracing::debug!(target:"dispatcharr::plugin_files","Plugin DB sync deferred after settled server error");
            Ok(())
        }
        Err(error) => Err(error),
    }
}
