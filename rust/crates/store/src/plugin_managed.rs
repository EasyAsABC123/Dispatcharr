//! Managed disabled-plugin install/update from original Dispatcharr's API.
//! Original authors retain algorithm/application credit. No Python is evaluated.
use super::*;
fn required() -> Response {
    reply(
        400,
        json!({"error":"repo_id, slug, version, and download_url are required"}),
    )
}
fn text(value: Option<&Value>) -> Result<String, StoreError> {
    match value {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(v)) => Ok(v.clone()),
        _ => Err(guard()),
    }
}
pub(super) fn repo_id(value: &Value) -> Result<i64, StoreError> {
    match value {
        Value::String(v) => dispatcharr_ingest_core::source_integer_literal(v)
            .and_then(|v| v.parse().ok())
            .ok_or(StoreError::UnhandledRestFault),
        Value::Number(v) => v.as_i64().ok_or_else(guard),
        Value::Bool(v) => Ok(i64::from(*v)),
        _ => Err(StoreError::UnhandledRestFault),
    }
}
/// Source compares numeric dot segments; prereleases only test exact spelling.
fn compare(a: &str, b: &str) -> Result<std::cmp::Ordering, StoreError> {
    use num_bigint::BigInt;
    if !a.is_ascii() || !b.is_ascii() {
        return Err(guard());
    }
    let a = a.trim_start_matches('v');
    let b = b.trim_start_matches('v');
    let numeric = |v: &str| {
        v.split('.')
            .all(|p| !p.is_empty() && p.bytes().all(|c| c.is_ascii_digit()))
    };
    if !numeric(a) || !numeric(b) {
        return Ok(if a == b {
            std::cmp::Ordering::Equal
        } else {
            std::cmp::Ordering::Greater
        });
    }
    let parse = |v: &str| {
        v.split('.')
            .map(|p| p.parse::<BigInt>().map_err(|_| guard()))
            .collect::<Result<Vec<_>, _>>()
    };
    let (a, b) = (parse(a)?, parse(b)?);
    let zero = BigInt::from(0);
    for index in 0..a.len().max(b.len()) {
        let order = a
            .get(index)
            .unwrap_or(&zero)
            .cmp(b.get(index).unwrap_or(&zero));
        if !order.is_eq() {
            return Ok(order);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}
fn compatibility(fields: &serde_json::Map<String, Value>) -> Result<Option<Response>, StoreError> {
    const VERSION: &str = "0.33.0"; // pinned Original version.py; no derivative credit
    for (field, label, order) in [
        ("min_dispatcharr_version", "newer", std::cmp::Ordering::Less),
        (
            "max_dispatcharr_version",
            "older",
            std::cmp::Ordering::Greater,
        ),
    ] {
        let value = fields.get(field);
        if !value.is_some_and(truthy) {
            continue;
        }
        // Non-string truthy constraints raise AttributeError before Source
        // can catch its narrower ValueError/TypeError version exceptions.
        let value = value
            .and_then(Value::as_str)
            .ok_or(StoreError::UnhandledRestFault)?;
        if compare(VERSION, value)? == order {
            return Ok(Some(reply(
                400,
                json!({"error":format!("This plugin version requires Dispatcharr {value} or {label} (you have {VERSION})")}),
            )));
        }
    }
    Ok(None)
}
fn matching<'a>(snap: &'a registry::Snapshot, id: i64, slug: &str) -> Vec<&'a Value> {
    snap.configs
        .iter()
        .filter(|c| c["source_repo_id"] == id && c["slug"] == slug)
        .collect()
}
fn deprecated(repo: &Value, slug: &str) -> Result<bool, StoreError> {
    let value = &repo["cached_manifest"];
    if !truthy(value) {
        return Ok(false);
    }
    let inner = value.get("manifest").unwrap_or(value);
    let entries = match inner.get("plugins") {
        None => return Ok(false),
        Some(v) => v.as_array().ok_or_else(guard)?,
    };
    for entry in entries {
        if !entry.is_object() {
            return Err(guard());
        }
        if entry["slug"] == slug {
            return Ok(entry.get("deprecated").is_some_and(truthy));
        }
    }
    Ok(false)
}
pub(super) async fn install(
    app: &PluginActions,
    session: &TaskSqlSession,
    request: Request,
    origin: &str,
    live: Arc<Live>,
) -> Result<Response, StoreError> {
    let fields = json_body(request).await?;
    let fields = fields.as_object().ok_or(StoreError::UnhandledRestFault)?;
    if ["repo_id", "slug", "version", "download_url"]
        .iter()
        .any(|key| !fields.get(*key).is_some_and(truthy))
    {
        return Ok(required());
    }
    let id = repo_id(&fields["repo_id"])?;
    let slug = text(fields.get("slug"))?;
    let version = text(fields.get("version"))?;
    let url = text(fields.get("download_url"))?;
    let expected = text(fields.get("sha256"))?.to_lowercase();
    let expected = expected.trim().to_owned();
    let prerelease = fields.get("prerelease").is_some_and(truthy);
    let first = registry::snapshot(session).await?;
    let Some(repo) = first.repos.iter().find(|r| r["id"] == id).cloned() else {
        return Ok(reply(404, json!({"error":"Repo not found"})));
    };
    if let Some(response) = compatibility(fields)? {
        return Ok(response);
    }
    registry::mutable(&first)?;
    let was_managed = !matching(&first, id, &slug).is_empty();
    // Validate finite manifest shape before any physical mutation; unsupported
    // arbitrary malformed repo objects retain an explicit pre-effect guard.
    let deprecated = deprecated(&repo, &slug)?;
    let root = app.inner.root.clone();
    let owner = live.clone();
    let lock = Arc::new(
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            files::MutationLock::acquire(&root)
        })
        .await
        .map_err(|_| guard())??,
    );
    if was_managed && matching(&registry::snapshot(session).await?, id, &slug).is_empty() {
        return Ok(reply(
            404,
            json!({"error":"Plugin was deleted while its update was pending"}),
        ));
    }
    let path = app.inner.root.join(format!(
        ".plugin-release-{}.zip",
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = Upload(path.clone());
    let create = path.clone();
    let owner = live.clone();
    let file = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(create)
            .map_err(|_| guard())
    })
    .await
    .map_err(|_| guard())??;
    let mut file = tokio::fs::File::from_std(file);
    let fetched =
        crate::plugin_repositories::download_managed_plugin(&url, &mut file, !expected.is_empty())
            .await;
    use tokio::io::AsyncWriteExt;
    let flushed = file.flush().await;
    drop(file.into_std().await);
    // All actual Tokio file work is joined before error/response/admission exit.
    flushed.map_err(|_| guard())?;
    let digest = fetched?;
    if !expected.is_empty() && digest.as_deref() != Some(expected.as_str()) {
        return Ok(reply(
            400,
            json!({"error":"SHA256 integrity check failed - download discarded. The file may be corrupted or tampered with."}),
        ));
    }
    let snap = registry::snapshot(session).await?;
    registry::mutable(&snap)?;
    let managed = matching(&snap, id, &slug);
    let sanitized = files::sanitize(&slug);
    let existing = managed
        .first()
        .copied()
        .or_else(|| snap.configs.iter().find(|c| c["key"] == sanitized))
        .or_else(|| {
            let dashed = sanitized.replace('_', "-");
            snap.configs.iter().find(|c| c["key"] == dashed)
        });
    let overwrite_key = existing.map(|c| text(c.get("key"))).transpose()?;
    let install_key = if managed.is_empty() {
        None
    } else {
        overwrite_key.clone()
    };
    let root = app.inner.root.clone();
    let filename = format!("{slug}.zip");
    let install_path = path.clone();
    let hold = lock.clone();
    let owner = live.clone();
    let applied = tokio::task::spawn_blocking(move || {
        let _owner = owner;
        files::install_managed(
            &install_path,
            &root,
            &filename,
            false,
            overwrite_key.as_deref(),
            install_key.as_deref(),
            &hold,
        )
    })
    .await
    .map_err(|_| guard())??;
    if applied["success"] != true {
        return Ok(reply(400, applied));
    }
    let actual = text(applied.get("plugin_key"))?;
    if managed.len() > 1 {
        let keep = managed[0]["id"].as_i64().ok_or_else(guard)?;
        let ids = managed
            .iter()
            .map(|c| c["id"].as_i64().ok_or_else(guard))
            .collect::<Result<Vec<_>, _>>()?;
        let root = app.inner.root.clone();
        let slug = slug.clone();
        let owner = live.clone();
        let lock = lock.clone();
        let consolidated=session.run(move|c|Box::pin(async move{
            use sqlx::Connection;
            let mut tx=c.begin().await?;
            let rows:Vec<(i64,String)>=sqlx::query_as("SELECT id,key FROM plugins_pluginconfig WHERE id=ANY($1) AND source_repo_id=$2 AND slug=$3 ORDER BY id FOR UPDATE").bind(ids).bind(id).bind(slug).fetch_all(&mut*tx).await?;
            if !rows.iter().any(|(id,_)|*id==keep){return Err(guard());}
            for(stale,key)in rows{
                if stale==keep{continue;}
                let path=root.join(key);let owner=owner.clone();let lock=lock.clone();
                tokio::task::spawn_blocking(move||{let _held=(owner,lock);match std::fs::remove_dir_all(path){Ok(())=>Ok(()),Err(e)if e.kind()==std::io::ErrorKind::NotFound=>Ok(()),Err(_)=>Err(guard())}}).await.map_err(|_|guard())??;
                sqlx::query("DELETE FROM plugins_pluginconfig WHERE id=$1").bind(stale).execute(&mut*tx).await?;
            }
            sqlx::query("UPDATE plugins_pluginconfig SET owned_tasks='[]'::jsonb,suspended_schedules='[]'::jsonb,updated_at=$2 WHERE id=$1").bind(keep).bind(chrono::Utc::now()).execute(&mut*tx).await?;
            tx.commit().await?;Ok::<_,StoreError>(())
        })).await?;
        if let Err(error) = consolidated {
            if let Some(message) = setting_error(&error) {
                return Ok(reply(
                    500,
                    json!({"success":false,"error":format!("Failed to remove stale plugin: {message}")}),
                ));
            }
            return Err(error);
        }
    }
    let key = actual.clone();
    let slug_copy = slug.clone();
    let version_copy = version.clone();
    let wrote=session.run(move|c|Box::pin(async move{
        let now=chrono::Utc::now();
        let created:Option<i64>=sqlx::query_scalar("INSERT INTO plugins_pluginconfig(key,name,version,description,enabled,ever_enabled,settings,owned_tasks,suspended_schedules,source_repo_id,slug,installed_version_is_prerelease,deprecated,created_at,updated_at) VALUES($1,$2,$3,'',false,false,'{}','[]','[]',$4,$2,$5,$6,$7,$7) ON CONFLICT(key) DO NOTHING RETURNING id")
            .bind(&key).bind(&slug_copy).bind(&version_copy).bind(id).bind(prerelease).bind(deprecated).bind(now).fetch_optional(&mut*c).await?;
        if created.is_none(){sqlx::query("UPDATE plugins_pluginconfig SET version=$2,source_repo_id=$3,slug=$4,installed_version_is_prerelease=$5,deprecated=$6,updated_at=$7 WHERE key=$1").bind(key).bind(version_copy).bind(id).bind(slug_copy).bind(prerelease).bind(deprecated).bind(now).execute(&mut*c).await?;}
        Ok::<_,StoreError>(created.is_some())
    })).await?;
    let created = match wrote {
        Ok(v) => v,
        Err(StoreError::Database(_)) => return Err(StoreError::UnhandledRestFault),
        Err(error) => return Err(error),
    };
    let plugins = discovery(app.inner.clone(), true).await?;
    registry::sync_caught(session, plugins.clone()).await?;
    let snap = registry::snapshot(session).await?;
    let entry = registry::list(&snap, &plugins, origin)?
        .into_iter()
        .find(|v| v["key"] == actual)
        .unwrap_or_else(|| json!({"key":actual,"slug":slug,"version":version}));
    drop(cleanup);
    drop(lock);
    Ok(reply(
        if created { 201 } else { 200 },
        json!({"success":true,"plugin":entry}),
    ))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_numeric_version_and_prerelease_ordering() {
        use std::cmp::Ordering::*;
        for (a, b, expected) in [
            ("v1.2", "1.2.0", Equal),
            ("0.33.0", "0.34", Less),
            ("1.10", "1.9", Greater),
            ("1.0-rc1", "1.0", Greater),
            ("v1.0-rc1", "1.0-rc1", Equal),
        ] {
            assert_eq!(compare(a, b).unwrap(), expected);
        }
    }
}
#[cfg(test)]
mod physical_archive_tests {
    use super::*;
    use std::io::Write;
    struct Owned(std::path::PathBuf);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn archive(path: &std::path::Path, entry: &str, body: &[u8]) {
        let mut writer = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
        writer
            .start_file(entry, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(body).unwrap();
        writer.finish().unwrap();
    }
    #[test]
    fn managed_update_retains_actual_legacy_key_and_retires_backup() {
        let owned = Owned(std::env::temp_dir().join(format!(
            "owned-managed-plugin-{}",
            uuid::Uuid::new_v4().simple()
        )));
        std::fs::create_dir(&owned.0).unwrap();
        let root = owned.0.join("plugins");
        let first = owned.0.join("first.zip");
        archive(&first, "old-layout/plugin.py", b"old code must never run");
        assert_eq!(
            files::install(&first, &root, "first.zip", false).unwrap()["plugin_key"],
            "old_layout"
        );
        let second = owned.0.join("second.zip");
        archive(
            &second,
            "new-layout/plugin.py",
            b"replacement must never run",
        );
        let lock = files::MutationLock::acquire(&root).unwrap();
        let actual = files::install_managed(
            &second,
            &root,
            "slug.zip",
            false,
            Some("old_layout"),
            Some("old_layout"),
            &lock,
        )
        .unwrap();
        assert_eq!(actual, json!({"success":true,"plugin_key":"old_layout"}));
        assert_eq!(
            std::fs::read(root.join("old_layout/plugin.py")).unwrap(),
            b"replacement must never run"
        );
        assert!(!root.join("new_layout").exists());
        assert!(!root.join("old_layout.__backup__").exists());
    }
    #[test]
    fn unmanaged_key_match_does_not_authorize_overwriting_another_archive_key() {
        let owned = Owned(std::env::temp_dir().join(format!(
            "owned-managed-plugin-{}",
            uuid::Uuid::new_v4().simple()
        )));
        std::fs::create_dir(&owned.0).unwrap();
        let root = owned.0.join("plugins");
        let first = owned.0.join("first.zip");
        archive(&first, "different/plugin.py", b"keep");
        files::install(&first, &root, "first.zip", false).unwrap();
        let second = owned.0.join("second.zip");
        archive(&second, "different/plugin.py", b"must not replace");
        let lock = files::MutationLock::acquire(&root).unwrap();
        let actual =
            files::install_managed(&second, &root, "slug.zip", false, Some("slug"), None, &lock)
                .unwrap();
        assert_eq!(
            actual,
            json!({"success":false,"error":"Plugin 'different' already exists"})
        );
        assert_eq!(
            std::fs::read(root.join("different/plugin.py")).unwrap(),
            b"keep"
        );
    }
}
