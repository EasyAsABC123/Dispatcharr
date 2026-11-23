//! Source backup services/tasks over the normal Main database and cached policy.
//! Original Dispatcharr authors retain all application/algorithm credit.
use crate::{
    recording_settings::Settings,
    worker::{ArchiveHandler, CreateHandler},
};
use chrono::{DateTime, Datelike, Local, Timelike, Utc};
use dispatcharr_backups::{Error as ArchiveError, Limits, PgConfig, Runtime, SourceClock};
use dispatcharr_store::{AuthService, PluginActions, Store};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::SystemTime};
#[derive(Debug)]
pub enum Error {
    Settings,
    Supervisor,
    Configuration,
}
type RestoreScope = (
    Arc<dispatcharr_store::BackupRestoreAdmission>,
    Arc<dyn Send + Sync>,
);
type RestoreUncertain = Arc<std::sync::Mutex<Vec<RestoreScope>>>;
#[derive(Clone)]
pub struct Executor {
    store: Store,
    auth: AuthService,
    pub(super) restore_plugins: Arc<std::sync::OnceLock<PluginActions>>,
    #[cfg(target_os = "linux")]
    pub(super) taskless: Arc<std::sync::OnceLock<super::normal_backup::SelectedTaskless>>,
    uncertain_restore: RestoreUncertain,
    restore_scheduler: Arc<std::sync::OnceLock<crate::normal_beat::RestorePauseClient>>,
    cache: String,
    pub(super) config: PgConfig,
    pub(super) runtime: Runtime,
    pub(super) root: PathBuf,
    pub(super) scratch: PathBuf,
}
struct Clock {
    timezone: chrono_tz::Tz,
}
impl Clock {
    fn new(system: &Value) -> Self {
        // pytz resolves zone spellings case-insensitively; invalid/non-string values
        // are caught by Source services.create_backup and use UTC.
        let requested = system
            .get("time_zone")
            .and_then(Value::as_str)
            .unwrap_or("UTC");
        let timezone = chrono_tz::TZ_VARIANTS
            .iter()
            .copied()
            .find(|zone| zone.name().eq_ignore_ascii_case(requested))
            .unwrap_or(chrono_tz::UTC);
        Self { timezone }
    }
}
impl SourceClock for Clock {
    fn filename_timestamp(&self) -> Result<String, ArchiveError> {
        Ok(Utc::now()
            .with_timezone(&self.timezone)
            .format("%Y.%m.%d.%H.%M.%S")
            .to_string())
    }
    fn created_at_utc(&self) -> Result<String, ArchiveError> {
        Ok(Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string())
    }
    fn zip_timestamp(&self, time: SystemTime) -> Result<zip::DateTime, ArchiveError> {
        let local: DateTime<Local> = time.into();
        let year = u16::try_from(local.year()).map_err(|_| ArchiveError::Metadata)?;
        zip::DateTime::from_date_and_time(
            year,
            local.month() as u8,
            local.day() as u8,
            local.hour() as u8,
            local.minute() as u8,
            local.second() as u8,
        )
        .map_err(|_| ArchiveError::Metadata)
    }
}
impl Executor {
    #[allow(clippy::too_many_arguments)]
    pub fn prepare(
        store: Store,
        auth: AuthService,
        cache: String,
        config: PgConfig,
        runtime: Runtime,
        root: PathBuf,
        scratch: PathBuf,
    ) -> Result<Self, Error> {
        if !root.is_absolute() || !scratch.is_absolute() || cache.is_empty() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            store,
            auth,
            restore_plugins: Arc::new(std::sync::OnceLock::new()),
            #[cfg(target_os = "linux")]
            taskless: Arc::new(std::sync::OnceLock::new()),
            uncertain_restore: Arc::new(std::sync::Mutex::new(Vec::with_capacity(1))),
            restore_scheduler: Arc::new(std::sync::OnceLock::new()),
            cache,
            config,
            runtime,
            root,
            scratch,
        })
    }
    pub async fn execute(
        &self,
        retention: Option<Value>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, Error> {
        // After actual claim, read through the existing SQL/cache/physical receipt.
        let system = Settings::new(self.store.clone(), self.cache.clone())
            .normal_effect(true, owner.clone())
            .await
            .map_err(|_| Error::Settings)?;
        let clock = Arc::new(Clock::new(&system));
        let owned = self.clone();
        tokio::task::spawn_blocking(move||{
   let _physical=owner;
   let handler=match ArchiveHandler::new_configured(owned.config,owned.runtime,owned.root.clone(),owned.scratch,clock,Limits::default()){
    Ok(handler)=>handler,Err(_)=>return json!({"status":"failed","error":"Native backup configuration unavailable"})
   };
   let complete=match handler.create(){Ok(value)=>value,Err(error)=>return json!({"status":"failed","error":error})};
   let mut result=json!({"status":"completed","filename":complete.filename,"size":complete.size});
   if let Some(count)=retention{
    match retain(&owned.root,&count){Ok(deleted)=>result["deleted_count"]=json!(deleted),Err(error)=>return json!({"status":"failed","error":error})}
    // Source stats backup_file AFTER cleanup: filename ordering may delete
    // the freshly created file after a timezone reversal/future-name archive.
    match std::fs::metadata(owned.root.join(&complete.filename)){Ok(meta)=>result["size"]=json!(meta.len()),Err(_)=>return json!({"status":"failed","error":"Native backup metadata error after retention"})}
   }
   result
  }).await.map_err(|_|Error::Supervisor)
    }
}
#[path = "normal_backup_restore.rs"]
mod restore;
fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
        Value::Number(_) => "float",
    }
}
fn retain(root: &std::path::Path, count: &Value) -> Result<usize, String> {
    #[derive(Clone, Copy)]
    enum Count {
        Integer(usize),
        Float(f64),
    }
    let number = match count {
        Value::Bool(value) => Count::Integer(usize::from(*value)),
        Value::Number(value) => {
            let text = value.to_string();
            if text.contains(['.', 'e', 'E']) {
                Count::Float(value.as_f64().unwrap_or(if text.starts_with('-') {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                }))
            } else if text.starts_with('-') {
                return Ok(0);
            } else {
                Count::Integer(text.parse().unwrap_or(usize::MAX))
            }
        }
        other => {
            return Err(format!(
                "'>' not supported between instances of '{}' and 'int'",
                type_name(other)
            ))
        }
    };
    if matches!(number, Count::Integer(0)) || matches!(number,Count::Float(value)if value<=0.0) {
        return Ok(0);
    }
    let mut files = Vec::new();
    let mut bytes = 0usize;
    for entry in std::fs::read_dir(root).map_err(|_| "Native backup listing unavailable")? {
        let path = entry
            .map_err(|_| "Native backup listing unavailable")?
            .path();
        let Some(name) = path.file_name().and_then(|v| v.to_str()) else {
            continue;
        };
        if !name.starts_with("dispatcharr-backup-") || !name.ends_with(".zip") {
            continue;
        }
        // Source list_backups stats each selected path before slicing; metadata errors
        // are task failure, while individual deletion errors below are caught.
        path.metadata()
            .map_err(|_| "Native backup metadata unavailable")?;
        bytes = bytes
            .checked_add(name.len())
            .ok_or("Native backup listing capacity unavailable")?;
        if files.len() >= 100000 || bytes > 16 * 1024 * 1024 {
            return Err("Native backup listing capacity unavailable".into());
        }
        files.push(path);
    }
    files.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
    let keep = match number {
        Count::Integer(keep) => keep,
        Count::Float(value) => {
            if files.len() as f64 <= value {
                return Ok(0);
            }
            return Err(
                "slice indices must be integers or None or have an __index__ method".into(),
            );
        }
    };
    let mut deleted = 0;
    for path in files.into_iter().skip(keep) {
        if path.is_file() && std::fs::remove_file(path).is_ok() {
            deleted += 1
        }
    }
    Ok(deleted)
}
