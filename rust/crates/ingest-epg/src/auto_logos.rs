//! Original automatic EPG-logo bulk helper with bounded physical SQL ownership.
//! Application/algorithm credit belongs entirely to Dispatcharr's authors.
use crate::{ImportError, SwapPolicyContext};
use dispatcharr_store::Store;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder, Row};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tokio::sync::Semaphore;
#[derive(Clone, Copy, Debug)]
pub struct LogoConfig {
    pub max_row_url_bytes: usize,
}
impl Default for LogoConfig {
    fn default() -> Self {
        Self {
            max_row_url_bytes: 1024 * 1024,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogoStats {
    pub updated_count: u64,
    pub created_logos_count: u64,
    pub error_count: u64,
    pub errors: Vec<String>,
}
impl LogoStats {
    pub fn source_value(&self) -> Value {
        json!({"updated_count":self.updated_count,"created_logos_count":self.created_logos_count,"error_count":self.error_count,"errors":self.errors})
    }
}
#[derive(Debug)]
pub enum LogoError {
    Properties(&'static str),
    Resource(ImportError),
}
impl std::fmt::Display for LogoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Properties(kind) => write!(f, "'{kind}' object has no attribute 'get'"),
            Self::Resource(value) => write!(f, "{value}"),
        }
    }
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_none_or(|n| n != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}
pub fn enabled(properties: &Value) -> Result<bool, LogoError> {
    if !truthy(properties) {
        return Ok(false);
    }
    if let Value::Object(map) = properties {
        return Ok(map.get("auto_apply_epg_logos").is_some_and(truthy));
    }
    Err(LogoError::Properties(match properties {
        Value::Bool(_) => "bool",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Number(value) if value.as_str().contains(['.', 'e', 'E']) => "float",
        _ => "int",
    }))
}
/// Bad property types escape before the Source nonfatal SQL try/except. SQL
/// errors after that boundary return None; explicit native resource/cancellation
/// errors remain typed rather than being reported as successful work.
pub async fn maybe_apply(
    store: Store,
    source_id: i64,
    properties: &Value,
    context: SwapPolicyContext,
) -> Result<Option<LogoStats>, LogoError> {
    maybe_apply_with_config(store, source_id, properties, context, LogoConfig::default()).await
}
pub async fn maybe_apply_with_config(
    store: Store,
    source_id: i64,
    properties: &Value,
    context: SwapPolicyContext,
    config: LogoConfig,
) -> Result<Option<LogoStats>, LogoError> {
    if !(4096..=1024 * 1024).contains(&config.max_row_url_bytes) {
        return Err(LogoError::Resource(ImportError::Capacity(
            "automatic-logo configuration",
        )));
    }
    if !bounded_properties(properties) {
        return Err(LogoError::Resource(ImportError::Capacity(
            "automatic-logo properties",
        )));
    }
    if !enabled(properties)? {
        return Ok(None);
    }
    context.ensure_active().map_err(LogoError::Resource)?;
    static ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let permit = ADMISSION
        .get_or_init(|| Arc::new(Semaphore::new(4)))
        .clone()
        .try_acquire_owned()
        .map_err(|_| LogoError::Resource(ImportError::Busy))?;
    tokio::spawn(async move {
        let _permit = permit;
        let _guard = context.retained.clone();
        let result = apply(store, source_id, context, config).await;
        match result {
            Ok(stats) => Ok(Some(stats)),
            Err(ImportError::Store(_)) => Ok(None),
            Err(error) => Err(LogoError::Resource(error)),
        }
    })
    .await
    .map_err(|_| LogoError::Resource(ImportError::Worker))?
}
async fn apply(
    store: Store,
    source_id: i64,
    context: SwapPolicyContext,
    config: LogoConfig,
) -> Result<LogoStats, ImportError> {
    let mut connection = store.pool().acquire().await?.detach();
    // Original iterator observes one ordered ID snapshot, then reads each batch
    // afresh. A holdable private cursor keeps that snapshot without a Rust Vec
    // proportional to the complete catalogue or an open write transaction.
    sqlx::query("BEGIN").execute(&mut connection).await?;
    let declare=format!("DECLARE rust_epg_auto_logos CURSOR WITH HOLD FOR SELECT c.id FROM dispatcharr_channels_channel c JOIN epg_epgdata e ON e.id=c.epg_data_id WHERE e.epg_source_id={source_id} AND e.icon_url IS NOT NULL AND e.icon_url<>'' ORDER BY c.id ASC");
    sqlx::query(&declare).execute(&mut connection).await?;
    sqlx::query("COMMIT").execute(&mut connection).await?;
    let result = batches(&mut connection, context, config).await;
    // Finish actual cursor cleanup before the retained guard/slot is released.
    let closed = sqlx::query("CLOSE rust_epg_auto_logos")
        .execute(&mut connection)
        .await;
    let _ = connection.close().await;
    closed?;
    result
}
async fn batches(
    connection: &mut PgConnection,
    context: SwapPolicyContext,
    config: LogoConfig,
) -> Result<LogoStats, ImportError> {
    let mut stats = LogoStats::default();
    let mut seen = 0usize;
    loop {
        context.ensure_active()?;
        let ids: Vec<i64> = sqlx::query_scalar("FETCH FORWARD 500 FROM rust_epg_auto_logos")
            .fetch_all(&mut *connection)
            .await?;
        if ids.is_empty() {
            return Ok(stats);
        }
        seen += ids.len();
        if seen > 100_000 {
            return Err(ImportError::Capacity("automatic-logo catalogue"));
        }
        let batch = apply_batch(connection, &ids, &context, config).await?;
        stats.updated_count += batch.updated_count;
        stats.created_logos_count += batch.created_logos_count;
        stats.error_count += batch.error_count;
        stats.errors.extend(
            batch
                .errors
                .into_iter()
                .take(100usize.saturating_sub(stats.errors.len())),
        );
    }
}
struct Work {
    id: i64,
    old_logo: Option<i64>,
    url: String,
}
async fn apply_batch(
    connection: &mut PgConnection,
    ids: &[i64],
    context: &SwapPolicyContext,
    config: LogoConfig,
) -> Result<LogoStats, ImportError> {
    // Preserve the original join target/IN shape and unnamed custom planning;
    // no invented ID sort changes first-name choice for repeated icon URLs.
    let mut select = QueryBuilder::<Postgres>::new(
        "SELECT c.*, l.id AS logo_pk,l.name AS logo_name,CASE WHEN octet_length(l.url)<=",
    );
    select.push_bind(config.max_row_url_bytes as i64).push(" THEN l.url END AS logo_url,octet_length(l.url)>").push_bind(config.max_row_url_bytes as i64).push(" AS oversized_logo,e.id AS epg_pk,e.tvg_id AS epg_tvg,e.name AS epg_name,e.icon_url AS epg_icon,e.epg_source_id AS epg_source FROM dispatcharr_channels_channel c LEFT JOIN dispatcharr_channels_logo l ON c.logo_id=l.id LEFT JOIN epg_epgdata e ON c.epg_data_id=e.id WHERE c.id IN (");
    {
        let mut values = select.separated(",");
        for id in ids {
            values.push_bind(id);
        }
    }
    select.push(")");
    let query = select.build();
    let mut rows = query.persistent(false).fetch(&mut *connection);
    let mut work = Vec::new();
    let mut meta = HashMap::new();
    let mut unique = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = rows.try_next().await? {
        if row
            .get::<Option<bool>, _>("oversized_logo")
            .unwrap_or(false)
        {
            return Err(ImportError::Capacity("automatic-logo row"));
        }
        if row.get::<Option<i64>, _>("epg_pk").is_none() {
            continue;
        }
        let icon: Option<String> = row.get("epg_icon");
        let icon = icon.as_deref().unwrap_or("");
        let icon = crate::channel_core::strip(icon);
        if icon.is_empty() || row.get::<Option<String>, _>("logo_url").as_deref() == Some(icon) {
            continue;
        }
        let name: String = row.get("epg_name");
        let tvg: Option<String> = row.get("epg_tvg");
        bytes = bytes.saturating_add(
            icon.len() * 3 + name.capacity() + tvg.as_ref().map_or(0, String::capacity) + 128,
        );
        if bytes > 8 * 1024 * 1024 {
            return Err(ImportError::Capacity("automatic-logo batch"));
        }
        if !meta.contains_key(icon) {
            unique.push(icon.to_string());
            meta.insert(
                icon.to_string(),
                if name.is_empty() {
                    format!("Logo for {}", tvg.as_deref().unwrap_or("None"))
                } else {
                    name
                },
            );
        }
        work.push(Work {
            id: row.get("id"),
            old_logo: row.get("logo_id"),
            url: icon.to_string(),
        });
    }
    drop(rows);
    if work.is_empty() {
        return Ok(LogoStats::default());
    }
    let mut logos = lookup(connection, &unique).await?;
    let missing = unique
        .iter()
        .filter(|url| !logos.contains_key(url.as_str()))
        .collect::<Vec<_>>();
    let created = missing.len();
    if !missing.is_empty() {
        context.ensure_active()?;
        let mut insert =
            QueryBuilder::<Postgres>::new("INSERT INTO dispatcharr_channels_logo(name,url) ");
        insert.push_values(missing, |mut row, url| {
            row.push_bind(meta[url].chars().take(255).collect::<String>())
                .push_bind(url);
        });
        insert.push(" ON CONFLICT DO NOTHING");
        insert
            .build()
            .persistent(false)
            .execute(&mut *connection)
            .await?;
        logos = lookup(connection, &unique).await?;
    }
    let mut changes = Vec::new();
    let mut stats = LogoStats {
        created_logos_count: created as u64,
        ..LogoStats::default()
    };
    for item in work {
        if let Some(logo) = logos.get(&item.url) {
            if item.old_logo != Some(*logo) {
                changes.push((item.id, *logo));
            }
        } else {
            stats.error_count += 1;
            stats.errors.push(format!(
                "Channel {}: Logo not found for {}",
                item.id, item.url
            ));
        }
    }
    if !changes.is_empty() {
        context.ensure_active()?;
        let mut update =
            QueryBuilder::<Postgres>::new("UPDATE dispatcharr_channels_channel SET logo_id=(CASE ");
        for (id, logo) in &changes {
            update
                .push("WHEN id=")
                .push_bind(id)
                .push(" THEN ")
                .push_bind(logo)
                .push(" ");
        }
        update.push("ELSE NULL END)::bigint WHERE id IN (");
        {
            let mut selected = update.separated(",");
            for (id, _) in &changes {
                selected.push_bind(id);
            }
        }
        update.push(")");
        update
            .build()
            .persistent(false)
            .execute(&mut *connection)
            .await?;
    }
    stats.updated_count = changes.len() as u64;
    Ok(stats)
}
async fn lookup(
    connection: &mut PgConnection,
    urls: &[String],
) -> Result<HashMap<String, i64>, ImportError> {
    let rows =
        sqlx::query("SELECT id::bigint AS id,url FROM dispatcharr_channels_logo WHERE url=ANY($1)")
            .bind(urls)
            .persistent(false)
            .fetch_all(connection)
            .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get("url"), row.get("id")))
        .collect())
}

/// Bound retained source-property allocations before admitting a channel job.
/// Source has no corresponding cap; exceeding it remains an explicit resource
/// limitation, not successful catalogue parity.
pub(crate) fn bounded_properties(value: &Value) -> bool {
    let mut pending = vec![(value, 0usize)];
    let mut bytes = 0usize;
    let mut nodes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > 4096 || depth > 64 {
            return false;
        }
        bytes = bytes.saturating_add(std::mem::size_of::<Value>());
        match value {
            Value::String(value) => bytes = bytes.saturating_add(value.capacity()),
            Value::Number(value) => bytes = bytes.saturating_add(value.as_str().len()),
            Value::Array(values) => {
                bytes = bytes.saturating_add(
                    values
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Value>()),
                );
                if values.len() > 4096 || pending.len() + values.len() > 4096 {
                    return false;
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                if values.len() > 4096 || pending.len() + values.len() > 4096 {
                    return false;
                }
                for (key, value) in values {
                    bytes = bytes.saturating_add(key.capacity() + 128);
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
        if bytes > 256 * 1024 {
            return false;
        }
    }
    true
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_policy_and_retained_allocations() {
        assert!(!enabled(&json!([])).unwrap());
        assert!(enabled(&json!({"auto_apply_epg_logos":"false"})).unwrap());
        assert_eq!(
            enabled(&json!([false])).unwrap_err().to_string(),
            "'list' object has no attribute 'get'"
        );
        let mut large = String::with_capacity(512 * 1024);
        large.push('x');
        assert!(!bounded_properties(&Value::String(large)));
        let mut deep = json!(null);
        for _ in 0..66 {
            deep = json!([deep]);
        }
        assert!(!bounded_properties(&deep));
        assert!(bounded_properties(&json!({"auto_apply_epg_logos":true})));
    }
}
