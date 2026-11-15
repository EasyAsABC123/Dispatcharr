//! Original Dispatcharr managed plugin catalogue/detail algorithms. Full credit
//! belongs to its original authors; bounded guards are explicit, not parity.
use super::*;
use md5::{Digest, Md5};
fn text(value: Option<&Value>) -> Result<String, StoreError> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(v)) => Ok(v.clone()),
        _ => Err(guard()),
    }
}
fn bases(manifest: &Value) -> Result<(String, String), StoreError> {
    if !manifest.is_object() {
        return Err(guard());
    }
    let root = text(manifest.get("root_url"))?
        .trim_end_matches('/')
        .to_owned();
    let download = text(manifest.get("download_base_url"))?
        .trim_end_matches('/')
        .to_owned();
    let metadata = text(manifest.get("metadata_base_url"))?
        .trim_end_matches('/')
        .to_owned();
    Ok((
        if download.is_empty() {
            root.clone()
        } else {
            download
        },
        if metadata.is_empty() { root } else { metadata },
    ))
}
fn absolute(value: &str) -> bool {
    value.starts_with("http://") || value.starts_with("https://")
}
fn raw_manifest(repo: &Value) -> &Value {
    let value = &repo["cached_manifest"];
    value.get("manifest").unwrap_or(value)
}
pub(super) fn available(snap: &registry::Snapshot) -> Result<Value, StoreError> {
    let mut slugs = std::collections::BTreeMap::new();
    let mut keys = std::collections::BTreeMap::new();
    let mut normalized = std::collections::BTreeMap::new();
    for config in &snap.configs {
        let key = text(config.get("key"))?;
        keys.insert(key.clone(), config);
        normalized.insert(key.replace('-', "_"), key);
        let slug = text(config.get("slug"))?;
        if !slug.is_empty() {
            slugs.insert(slug.clone(), config);
            slugs.entry(slug.replace('-', "_")).or_insert(config);
        }
    }
    let mut result = Vec::new();
    let mut bytes = 0usize;
    for repo in snap.repos.iter().filter(|v| v["enabled"] == true) {
        let empty = json!({});
        let manifest = if truthy(&repo["cached_manifest"]) {
            raw_manifest(repo)
        } else {
            &empty
        };
        let (download, metadata) = bases(manifest)?;
        let registry = text(manifest.get("registry_url"))?
            .trim_end_matches('/')
            .to_owned();
        let entries = match manifest.get("plugins") {
            None => continue,
            Some(v) => v.as_array().ok_or_else(guard)?,
        };
        for entry in entries {
            let mut item = entry.as_object().cloned().ok_or_else(guard)?;
            let slug = text(item.get("slug"))?;
            if !metadata.is_empty() {
                for field in ["manifest_url", "icon_url"] {
                    let value = text(item.get(field))?;
                    if !value.is_empty() && !absolute(&value) {
                        item.insert(field.into(), json!(format!("{metadata}/{value}")));
                    }
                }
            }
            if !download.is_empty() {
                let value = text(item.get("latest_url"))?;
                if !value.is_empty() && !absolute(&value) {
                    item.insert("latest_url".into(), json!(format!("{download}/{value}")));
                }
            }
            if !item.get("icon_url").is_some_and(truthy)
                && manifest.get("metadata_base_url").is_some_and(truthy)
                && item.get("manifest_url").is_some_and(truthy)
            {
                let url = text(item.get("manifest_url"))?;
                let directory = url.rsplit_once('/').map(|v| v.0).unwrap_or(&url);
                item.insert("icon_url".into(), json!(format!("{directory}/logo.png")));
            }
            if !item.get("icon_url").is_some_and(truthy) && !registry.is_empty() {
                let raw =
                    registry.replace("https://github.com/", "https://raw.githubusercontent.com/");
                item.insert(
                    "icon_url".into(),
                    json!(format!("{raw}/refs/heads/main/plugins/{slug}/logo.png")),
                );
            }
            let managed = slugs
                .get(&slug)
                .or_else(|| slugs.get(&slug.replace('-', "_")))
                .copied();
            let key = files::sanitize_full(&slug);
            let key_match = keys
                .get(&key)
                .copied()
                .or_else(|| normalized.get(&key).and_then(|k| keys.get(k)).copied());
            let (status, version) = if let Some(config) = managed {
                let same = config["source_repo_id"] == repo["id"];
                let latest = item.get("latest_version");
                let installed = &config["version"];
                let status = if !same {
                    "different_repo"
                } else if truthy(installed)
                    && latest.is_some_and(truthy)
                    && Some(installed) != latest
                    && config["installed_version_is_prerelease"] != true
                {
                    "update_available"
                } else {
                    "installed"
                };
                (status, installed.clone())
            } else if let Some(config) = key_match {
                ("unmanaged", config["version"].clone())
            } else {
                ("not_installed", Value::Null)
            };
            for (field, value) in [
                ("repo_id", repo["id"].clone()),
                ("repo_name", repo["name"].clone()),
                ("is_official_repo", repo["is_official"].clone()),
                ("signature_verified", repo["signature_verified"].clone()),
                ("installed", json!(status != "not_installed")),
                ("installed_version", version),
                (
                    "installed_version_is_prerelease",
                    json!(managed.is_some_and(|v| v["installed_version_is_prerelease"] == true)),
                ),
                ("install_status", json!(status)),
                ("key", json!(key)),
            ] {
                item.insert(field.into(), value);
            }
            if status == "different_repo" {
                item.insert(
                    "installed_source_repo_name".into(),
                    managed.ok_or_else(guard)?["source_repo_name"].clone(),
                );
            }
            let item = Value::Object(item);
            bytes = bytes
                .checked_add(serde_json::to_vec(&item).map_err(|_| guard())?.len())
                .ok_or_else(guard)?;
            if result.len() >= 2048 || bytes > 1024 * 1024 {
                return Err(guard());
            }
            result.push(item);
        }
    }
    Ok(json!({"plugins":result}))
}
pub(super) async fn detail(
    app: &PluginActions,
    session: &TaskSqlSession,
    request: Request,
    live: Arc<Live>,
) -> Result<Response, StoreError> {
    let fields = json_body(request).await?;
    let fields = fields.as_object().ok_or(StoreError::UnhandledRestFault)?;
    if ["repo_id", "manifest_url"]
        .iter()
        .any(|key| !fields.get(*key).is_some_and(truthy))
    {
        return Ok(reply(
            400,
            json!({"error":"repo_id and manifest_url are required"}),
        ));
    }
    let id = managed::repo_id(&fields["repo_id"])?;
    let url = text(fields.get("manifest_url"))?;
    let snapshot = registry::snapshot(session).await?;
    let Some(repo) = snapshot.repos.iter().find(|v| v["id"] == id) else {
        return Ok(reply(404, json!({"error":"Repo not found"})));
    };
    let auth = app.inner.auth.clone().with_retained_scope(live);
    let cache = auth.notification_cache.as_ref().ok_or_else(guard)?;
    let key = format!(":1:plugin_detail:{id}:{:x}", Md5::digest(url.as_bytes()));
    let mut read = redis::cmd("EVAL");
    read.arg("if redis.call('STRLEN',KEYS[1])>1048576 then return redis.error_reply('cache value exceeds limit') end return redis.call('GET',KEYS[1])").arg(1).arg(&key);
    let cached: Option<Vec<u8>> = cache.command_settled(read).await?;
    if let Some(raw) = cached {
        let value = crate::cache_codec::decode_source_cache(&raw).ok_or_else(guard)?;
        if !value.is_null() {
            return Ok(reply(200, value));
        }
    }
    let public_key = if repo["is_official"] == true {
        None
    } else {
        repo["public_key"].as_str()
    };
    let (mut manifest, verified) =
        crate::plugin_repositories::fetch_plugin_detail(&url, public_key).await?;
    let empty_manifest = json!({});
    let (download, _) = bases(if truthy(&repo["cached_manifest"]) {
        raw_manifest(repo)
    } else {
        &empty_manifest
    })?;
    resolve_versions(&mut manifest, &download)?;
    let value = json!({"manifest":manifest,"signature_verified":verified});
    let raw = crate::cache_codec::encode_source_cache(&value).ok_or_else(guard)?;
    let mut set = redis::cmd("SETEX");
    set.arg(key).arg(300).arg(raw);
    let _: () = cache.command_settled(set).await?;
    Ok(reply(200, value))
}
fn resolve_versions(manifest: &mut Value, download: &str) -> Result<(), StoreError> {
    let object = manifest.as_object_mut().ok_or_else(guard)?;
    if download.is_empty() {
        return Ok(());
    }
    if let Some(Value::Array(versions)) = object.get_mut("versions") {
        for version in versions {
            let item = version.as_object_mut().ok_or_else(guard)?;
            let url = text(item.get("url"))?;
            if !url.is_empty() && !absolute(&url) {
                item.insert("url".into(), json!(format!("{download}/{url}")));
            }
        }
    }
    if let Some(Value::Object(latest)) = object.get_mut("latest") {
        for field in ["url", "latest_url"] {
            let url = text(latest.get(field))?;
            if !url.is_empty() && !absolute(&url) {
                latest.insert(field.into(), json!(format!("{download}/{url}")));
            }
        }
    }
    Ok(())
}
fn interval(value: Option<&Value>) -> Value {
    let integer = match value {
        None => Some("6".into()),
        Some(Value::Bool(value)) => Some(if *value { "1".into() } else { "0".into() }),
        Some(Value::Number(value)) if value.to_string().contains(['.', 'e', 'E']) => value
            .as_f64()
            .filter(|v| v.is_finite())
            .map(|v| format!("{:.0}", v.trunc())),
        Some(Value::Number(value)) => Some(value.to_string()),
        Some(Value::String(value)) => dispatcharr_ingest_core::source_integer_literal(value),
        _ => None,
    }
    .and_then(|v| dispatcharr_ingest_core::source_integer_literal(&v))
    .unwrap_or_else(|| "6".into());
    if integer.starts_with('-') {
        json!(0)
    } else {
        serde_json::from_str(&integer).unwrap_or(json!(6))
    }
}
pub(super) async fn settings(
    app: &PluginActions,
    session: &TaskSqlSession,
    request: Request,
    live: Arc<Live>,
) -> Result<Response, StoreError> {
    if request.method() == Method::GET || request.method() == Method::HEAD {
        let value=session.run(|c|Box::pin(async move{
            let raw:Option<(i32,Option<String>)>=sqlx::query_as("SELECT octet_length(value::text),CASE WHEN octet_length(value::text)<=65536 THEN value::text END FROM core_coresettings WHERE key='plugin_repo_settings'").fetch_optional(c).await?;
            match raw{None=>Ok(json!({"refresh_interval_hours":6})),Some((size,Some(raw)))if size>=0&&size as usize==raw.len()=>serde_json::from_str(&raw).map_err(|_|guard()),_=>Err(guard())}
        })).await??;
        return Ok(reply(200, value));
    }
    let schedule = app.inner.schedule.as_ref().ok_or_else(guard)?.clone();
    let fields = json_body(request).await?;
    let fields = fields.as_object().ok_or(StoreError::UnhandledRestFault)?;
    let value = json!({"refresh_interval_hours":interval(fields.get("refresh_interval_hours"))});
    let auth = app.inner.auth.clone().with_retained_scope(live.clone());
    let cache = auth.notification_cache.clone().ok_or_else(guard)?;
    let write = value.clone();
    let owner = live.clone();
    let wrote=session.run(move|c|Box::pin(async move{
        use sqlx::Connection;
        let _owner=owner;let mut tx=c.begin().await?;
        sqlx::query("INSERT INTO core_coresettings(key,name,value) VALUES('plugin_repo_settings','Plugin Repo Settings',$1) ON CONFLICT(key) DO UPDATE SET name=EXCLUDED.name,value=EXCLUDED.value").bind(write).execute(&mut*tx).await?;
        let version=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|guard())?.as_nanos().to_string();
        // Source post_save invalidation runs inside update_or_create's atomic
        // block, BEFORE commit and BEFORE interval/PT mutation. Same callback.
        cache.invalidate_settings_group("plugin_repo_settings",&version).await?;
        tx.commit().await?;Ok::<_,StoreError>(())
    })).await?;
    if wrote.is_err() {
        return Err(StoreError::UnhandledRestFault);
    }
    match schedule.synchronize(session.clone(), live).await {
        Ok(()) => Ok(reply(200, value)),
        Err(StoreError::Database(_)) => Err(StoreError::UnhandledRestFault),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detail_resolves_version_links_only_after_verification_and_preserves_absolute_urls() {
        let mut value = json!({"versions":[{"url":"v1.zip"},{"url":"https://other/v2.zip"}],"latest":{"url":"release.zip","latest_url":"https://other/latest.zip"}});
        resolve_versions(&mut value, "https://owned/releases").unwrap();
        assert_eq!(value["versions"][0]["url"], "https://owned/releases/v1.zip");
        assert_eq!(value["versions"][1]["url"], "https://other/v2.zip");
        assert_eq!(value["latest"]["url"], "https://owned/releases/release.zip");
        assert_eq!(value["latest"]["latest_url"], "https://other/latest.zip");
    }
    #[test]
    fn available_keeps_different_repo_and_unmanaged_key_resolution() {
        let snap = registry::Snapshot {
            configs: vec![
                json!({"key":"manual_key","slug":"","version":"2"}),
                json!({"key":"other","slug":"sample-plugin","version":"1","source_repo_id":9,"source_repo_name":"Prior","installed_version_is_prerelease":false}),
            ],
            repos: vec![
                json!({"id":2,"name":"Owned","enabled":true,"is_official":false,"signature_verified":null,"cached_manifest":{"manifest":{"download_base_url":"https://owned/releases","metadata_base_url":"https://owned/details","plugins":[{"slug":"sample-plugin","latest_version":"2","manifest_url":"sample/info.json"},{"slug":"manual-key"},{"slug":"new"}]}}}),
            ],
        };
        let value = available(&snap).unwrap();
        assert_eq!(value["plugins"][0]["install_status"], "different_repo");
        assert_eq!(value["plugins"][0]["installed_source_repo_name"], "Prior");
        assert_eq!(
            value["plugins"][0]["icon_url"],
            "https://owned/details/sample/logo.png"
        );
        assert_eq!(value["plugins"][1]["install_status"], "unmanaged");
        assert_eq!(value["plugins"][2]["install_status"], "not_installed");
    }
}
