//! Bounded mapped-file branches from actual Source217; separate inert assembly.
//! Original Dispatcharr/Django/Celery authors retain algorithms/code credit.
use crate::{
    logo_scanner::{FirstScanHook, Roots},
    scanner_files_protocol::Kind,
    scanner_index_helper::{Execution, Helper},
    transport::{ScannerFilesPorts, ScannerIoError},
};
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use sqlx::{Connection, PgConnection, PgPool, Row};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use tokio::sync::{oneshot, Semaphore};
use uuid::Uuid;
pub type Owner = Arc<dyn Send + Sync>;
#[derive(Debug)]
pub enum Error {
    Unsupported,
    Capacity,
    Closed,
    Operational,
    Sql(sqlx::Error),
    Insert { epg: bool, error: sqlx::Error },
    Io(ScannerIoError),
    Known(Failure),
}
impl From<sqlx::Error> for Error {
    fn from(e: sqlx::Error) -> Self {
        Self::Sql(e)
    }
}
impl From<ScannerIoError> for Error {
    fn from(e: ScannerIoError) -> Self {
        Self::Io(e)
    }
}
#[derive(Debug)]
pub struct Failure {
    module: &'static str,
    class: &'static str,
    argument: String,
    stage: &'static str,
}
impl Failure {
    fn new(
        module: &'static str,
        class: &'static str,
        argument: String,
        stage: &'static str,
    ) -> Result<Self, Error> {
        if argument.len() > 8192 {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            module,
            class,
            argument,
            stage,
        })
    }
}
#[derive(Debug)]
pub struct Outcome {
    pub children: Vec<Uuid>,
    pub failure: Option<Failure>,
    pub first_complete: bool,
}
impl Outcome {
    pub(crate) fn metadata(&self, id: Uuid) -> Result<Vec<u8>, Error> {
        if self.children.len() > 32 {
            return Err(Error::Unsupported);
        }
        let tuples: Vec<Value> = self
            .children
            .iter()
            .map(|id| json!([[id.to_string(), null], null]))
            .collect();
        let date = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
            .to_string();
        let(status,result,trace)=match &self.failure{None=>("SUCCESS",Value::Null,Value::Null),Some(f)=>("FAILURE",json!({"exc_type":f.class,"exc_module":f.module,"exc_message":[f.argument]}),json!(format!("Native Dispatcharr mapped scanner diagnostic [{}]; settled known operation failure. Original Python frames are not reproduced.",f.stage)))};
        let bytes=serde_json::to_vec(&json!({"status":status,"result":result,"traceback":trace,"children":tuples,"date_done":date,"task_id":id.to_string()})).map_err(|_|Error::Operational)?;
        crate::decode::decode(&bytes, id).map_err(|_| Error::Unsupported)?;
        Ok(bytes)
    }
}
#[derive(Clone)]
pub struct Service {
    inner: Arc<Inner>,
}
struct Inner {
    pool: PgPool,
    ports: ScannerFilesPorts,
    sink: Arc<dyn UpdateEventSink>,
    roots: Roots,
    slot: Arc<Semaphore>,
    first: std::sync::atomic::AtomicBool,
}
#[derive(Clone)]
struct WatchFile {
    path: String,
    name: String,
    mtime: f64,
    epg: bool,
}
struct Object {
    id: i64,
    active: bool,
}
struct RunContext {
    parent: Uuid,
    owner: Owner,
    children: Vec<Uuid>,
}
impl Service {
    pub fn new(
        pool: PgPool,
        ports: ScannerFilesPorts,
        sink: Arc<dyn UpdateEventSink>,
        roots: Roots,
    ) -> Result<Self, Error> {
        let values = [&roots.m3u, &roots.epg, &roots.logos];
        for (i, path) in values.iter().enumerate() {
            let normalized: PathBuf = path.components().collect();
            if !path.is_absolute()
                || path.to_str().is_none()
                || path.as_os_str().len() > 4096
                || normalized.as_os_str() != path.as_os_str()
                || path.components().any(|v| {
                    matches!(
                        v,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                })
                || values
                    .iter()
                    .skip(i + 1)
                    .any(|other| path.starts_with(other) || other.starts_with(path))
            {
                return Err(Error::Unsupported);
            }
        }
        Ok(Self {
            inner: Arc::new(Inner {
                pool,
                ports,
                sink,
                roots,
                slot: Arc::new(Semaphore::new(1)),
                first: std::sync::atomic::AtomicBool::new(false),
            }),
        })
    }
    #[cfg(test)]
    pub(crate) fn resource_ports(&self) -> ScannerFilesPorts {
        self.inner.ports.clone()
    }
    pub fn first_completed(&self) -> bool {
        self.inner.first.load(std::sync::atomic::Ordering::Acquire)
    }
    async fn files(&self, owner: Owner) -> Result<Vec<WatchFile>, Error> {
        let roots = self.inner.roots.clone();
        tokio::task::spawn_blocking(move || {
            let _owner = owner;
            inspect(roots)
        })
        .await
        .map_err(|_| Error::Operational)?
    }
    pub async fn preflight(&self, owner: Owner) -> Result<(), Error> {
        let this = self.clone();
        let permit = Arc::new(self.inner.slot.clone().try_acquire_owned().map_err(|_| {
            if self.inner.slot.is_closed() {
                Error::Closed
            } else {
                Error::Capacity
            }
        })?);
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _permit = permit;
            let result = this.preflight_owned(owner).await;
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::Operational)?
    }
    async fn preflight_owned(&self, owner: Owner) -> Result<(), Error> {
        let files = self.files(owner.clone()).await?;
        for f in files {
            if let Some(raw) = self.inner.ports.marker(&f.path, owner.clone()).await? {
                parse_marker(&raw)?;
            }
        }
        let mut c = self.connection().await?;
        let settings=sqlx::query_scalar::<_,Option<Value>>("SELECT CASE WHEN octet_length(value::text)<=65536 THEN value END FROM core_coresettings WHERE key='system_settings' LIMIT 1").fetch_optional(&mut c).await;
        let validation = match &settings {
            Ok(Some(None)) => Err(Error::Unsupported),
            Ok(value) => auto_import(value.as_ref().and_then(Option::as_ref)).map(|_| ()),
            Err(_) => Err(Error::Operational),
        };
        if validation.is_err() {
            settle(c).await?;
            return validation;
        }
        let trigger_query=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT t.tgisinternal AND n.nspname=current_schema() AND c.relname IN('m3u_m3uaccount','m3u_m3uaccountprofile','epg_epgsource','django_celery_beat_periodictask','django_celery_beat_intervalschedule','django_celery_beat_periodictasks') AND NOT(c.relname IN('m3u_m3uaccount','epg_epgsource') AND t.tgname='rust_owned_branch_insert_fail'))").fetch_one(&mut c).await;
        if !matches!(trigger_query, Ok(false)) {
            settle(c).await?;
            return match trigger_query {
                Ok(true) => Err(Error::Unsupported),
                Err(error) => Err(Error::Sql(error)),
                _ => Err(Error::Operational),
            };
        }
        let result=sqlx::query_scalar::<_,i64>("SELECT count(*)::bigint FROM (SELECT s.id FROM epg_epgsource s LEFT JOIN epg_epgsourceindex i ON s.id=i.source_id WHERE s.is_active AND s.source_type NOT IN ('dummy','schedules_direct') AND (i.source_id IS NULL OR i.data IS NULL) LIMIT 17) bounded").fetch_one(&mut c).await;
        settle(c).await?;
        if result? > 16 {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
    async fn connection(&self) -> Result<PgConnection, Error> {
        self.inner
            .pool
            .acquire()
            .await
            .map(|c| c.detach())
            .map_err(Error::Sql)
    }
    pub async fn run(&self, parent: Uuid, now: f64, owner: Owner) -> Result<Outcome, Error> {
        if parent.is_nil() || !now.is_finite() || !(100_000_000.0..10_000_000_000.0).contains(&now)
        {
            return Err(Error::Unsupported);
        }
        let permit = Arc::new(self.inner.slot.clone().try_acquire_owned().map_err(|_| {
            if self.inner.slot.is_closed() {
                Error::Closed
            } else {
                Error::Capacity
            }
        })?);
        let this = self.clone();
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let owner: Owner = Arc::new((owner, permit));
            let result = this.owned(parent, now, owner).await;
            let _ = tx.send(result);
        });
        rx.await.map_err(|_| Error::Operational)?
    }
    async fn owned(&self, parent: Uuid, now: f64, owner: Owner) -> Result<Outcome, Error> {
        self.preflight_owned(owner.clone()).await?;
        let files = self.files(owner.clone()).await?;
        let mut context = RunContext {
            parent,
            owner: owner.clone(),
            children: Vec::new(),
        };
        for file in files {
            match self.file(&file, now, &mut context).await {
                Ok(()) => {}
                Err(Error::Known(failure)) if !file.epg => {
                    return Ok(Outcome {
                        children: context.children,
                        failure: Some(failure),
                        first_complete: self.first_completed(),
                    })
                }
                Err(Error::Known(_)) => {}
                Err(error) => return Err(error),
            }
        }
        if !self.first_completed() {
            let execution = Execution::new(
                Arc::new(Helper::new_scoped(
                    self.inner.pool.clone(),
                    self.inner.ports.jobs(),
                    self.inner.ports.reply_id(),
                    self.inner.ports.origin(),
                )),
                parent,
            );
            let (receipt, done) = crate::logo_scanner::scanner_files_receipt(owner);
            let result = execution.finish(receipt.clone()).await;
            drop(receipt);
            done.await.map_err(|_| Error::Operational)?;
            result.map_err(|_| Error::Operational)?;
            context
                .children
                .extend_from_slice(execution.children().map_err(|_| Error::Operational)?.ids());
            if context.children.len() > 32 {
                return Err(Error::Unsupported);
            }
        }
        self.inner
            .first
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(Outcome {
            children: context.children,
            failure: None,
            first_complete: true,
        })
    }
    async fn file(
        &self,
        file: &WatchFile,
        now: f64,
        context: &mut RunContext,
    ) -> Result<(), Error> {
        let marker = self
            .inner
            .ports
            .marker(&file.path, context.owner.clone())
            .await?
            .filter(|v| !v.is_empty());
        let age = now - file.mtime;
        if marker.is_none() && age > 30.0 && self.exists(file).await? {
            self.marker(file, context.owner.clone())
                .await
                .map_err(|error| match error {
                    Error::Known(_) => Error::Operational,
                    other => other,
                })?;
            return Ok(());
        }
        if age < 6.0 {
            return Ok(());
        }
        if let Some(raw) = marker {
            if parse_marker(&raw)?.is_some_and(|value| value >= file.mtime) {
                return Ok(());
            }
        }
        let object = self.get_or_create(file, context).await?;
        self.marker(file, context.owner.clone()).await?;
        if !object.active {
            return Ok(());
        }
        self.publish(
            if file.epg {
                Kind::EpgRefresh
            } else {
                Kind::M3uSingle
            },
            object.id,
            context,
        )
        .await?;
        if !file.epg {
            self.inner.sink.send_retained("updates",json!({"type":"update","data":{"success":true,"type":"m3u_file","filename":file.name}}),context.owner.clone()).await.map_err(|_|Error::Operational)?;
        }
        Ok(())
    }
    async fn marker(&self, file: &WatchFile, owner: Owner) -> Result<(), Error> {
        let text =
            dispatcharr_store::source_scanner_epoch_string(file.mtime).ok_or(Error::Unsupported)?;
        match self.inner.ports.set_marker(&file.path, &text, owner).await {
            Ok(()) => Ok(()),
            Err(ScannerIoError::Server(e)) if e.code == "NOPERM" => {
                Err(Error::Known(Failure::new(
                    "redis.exceptions",
                    "NoPermissionError",
                    e.detail,
                    "marker SET",
                )?))
            }
            Err(e) => Err(Error::Io(e)),
        }
    }
    async fn publish(&self, kind: Kind, id: i64, context: &mut RunContext) -> Result<(), Error> {
        match self
            .inner
            .ports
            .publish(kind, id, context.parent, context.owner.clone())
            .await
        {
            Ok(id) => {
                if context.children.len() == 32 {
                    return Err(Error::Unsupported);
                }
                context.children.push(id);
                Ok(())
            }
            Err(ScannerIoError::Server(e)) if e.code == "NOPERM" => {
                Err(Error::Known(Failure::new(
                    "kombu.exceptions",
                    "OperationalError",
                    e.detail,
                    "child LPUSH",
                )?))
            }
            Err(e) => Err(Error::Io(e)),
        }
    }
    async fn exists(&self, file: &WatchFile) -> Result<bool, Error> {
        let mut c = self.connection().await?;
        let sql = if file.epg {
            "SELECT EXISTS(SELECT 1 FROM epg_epgsource WHERE file_path=$1)"
        } else {
            "SELECT EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE file_path=$1)"
        };
        let result = sqlx::query_scalar::<_, bool>(sql)
            .bind(&file.path)
            .fetch_one(&mut c)
            .await;
        settle(c).await?;
        result.map_err(Error::Sql)
    }
    async fn get_or_create(
        &self,
        file: &WatchFile,
        context: &mut RunContext,
    ) -> Result<Object, Error> {
        let mut c = self.connection().await?;
        let result = self.create_owned(&mut c, file, context).await;
        settle(c).await?;
        match result {
            Err(Error::Insert { epg, error }) if epg == file.epg => {
                if let Some(pg) = error
                    .as_database_error()
                    .and_then(|e| e.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
                {
                    if pg.severity() == sqlx::postgres::PgSeverity::Error && pg.code() == "P0001" {
                        let arg = crate::pg_failure_format::render_pg(pg)
                            .map_err(|_| Error::Unsupported)?;
                        return Err(Error::Known(Failure::new(
                            "django.db.utils",
                            "ProgrammingError",
                            arg,
                            "model INSERT",
                        )?));
                    }
                }
                Err(Error::Sql(error))
            }
            other => other,
        }
    }
    async fn create_owned(
        &self,
        c: &mut PgConnection,
        file: &WatchFile,
        context: &mut RunContext,
    ) -> Result<Object, Error> {
        let settings:Option<Value>=sqlx::query_scalar("SELECT CASE WHEN octet_length(value::text)<=65536 THEN value END FROM core_coresettings WHERE key='system_settings' LIMIT 1").fetch_optional(&mut *c).await?.flatten();
        let active = auto_import(settings.as_ref())?;
        let select = if file.epg {
            "SELECT id::bigint,is_active FROM epg_epgsource WHERE file_path=$1 LIMIT 2"
        } else {
            "SELECT id::bigint,is_active FROM m3u_m3uaccount WHERE file_path=$1 LIMIT 2"
        };
        let found = sqlx::query(select)
            .bind(&file.path)
            .fetch_all(&mut *c)
            .await?;
        if found.len() > 1 {
            return Err(Error::Unsupported);
        }
        if let Some(row) = found.first() {
            return Ok(Object {
                id: row.try_get(0)?,
                active: row.try_get(1)?,
            });
        }
        let mut tx = c.begin().await?;
        let result=async{
   let insert=if file.epg{"INSERT INTO epg_epgsource(name,source_type,url,username,password,is_active,file_path,extracted_file_path,refresh_interval,refresh_task_id,custom_properties,priority,status,last_message,created_at,updated_at) VALUES($1,'xmltv',NULL,NULL,NULL,$2,$3,NULL,0,NULL,'{}',0,'idle',NULL,clock_timestamp(),NULL) RETURNING id::bigint"}else{"INSERT INTO m3u_m3uaccount(name,server_url,file_path,server_group_id,max_streams,is_active,created_at,updated_at,status,last_message,user_agent_id,locked,stream_profile_id,account_type,username,password,custom_properties,refresh_interval,refresh_task_id,stale_stream_days,priority) VALUES($1,NULL,$3,NULL,0,$2,clock_timestamp(),NULL,'idle',NULL,NULL,false,NULL,'STD',NULL,NULL,'{}',0,NULL,7,0) RETURNING id::bigint"};
   let id:i64=sqlx::query_scalar(insert).bind(&file.name).bind(active).bind(&file.path).fetch_one(&mut *tx).await.map_err(|error|Error::Insert{epg:file.epg,error})?;
   if !file.epg{let profile:i64=sqlx::query_scalar("INSERT INTO m3u_m3uaccountprofile(m3u_account_id,name,is_default,max_streams,is_active,search_pattern,replace_pattern,current_viewers,custom_properties,exp_date) VALUES($1,$2,true,0,true,'^(.*)$','$1',0,'{}',NULL) RETURNING id::bigint").bind(id).bind(format!("{} Default",file.name)).fetch_one(&mut *tx).await?;let stale:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core_systemnotification WHERE notification_key IN($1,$2))").bind(format!("xc-exp-warning-{profile}")).bind(format!("xc-exp-expired-{profile}")).fetch_one(&mut *tx).await?;if stale{return Err(Error::Unsupported)}}
   let schedule=schedule(&mut tx,id,file.epg).await?;
   let link=if file.epg{"UPDATE epg_epgsource SET refresh_task_id=$2 WHERE id=$1"}else{"UPDATE m3u_m3uaccount SET refresh_task_id=$2 WHERE id=$1"};sqlx::query(link).bind(id).bind(schedule).execute(&mut *tx).await?;
   if file.epg{sentinel(&mut tx).await?;sqlx::query("UPDATE django_celery_beat_periodictask SET enabled=false,date_changed=clock_timestamp() WHERE id=$1").bind(schedule).execute(&mut *tx).await?;sentinel(&mut tx).await?;}
   if !file.epg||active{self.publish(if file.epg{Kind::EpgRefresh}else{Kind::M3uGroups},id,context).await?;}Ok(Object{id,active})
  }.await;
        match result {
            Ok(object) => {
                tx.commit().await?;
                Ok(object)
            }
            Err(error) => {
                tx.rollback().await.map_err(|_| Error::Operational)?;
                Err(error)
            }
        }
    }
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        self.inner.slot.close();
        tokio::time::timeout(timeout, async {
            while self.inner.slot.available_permits() != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await
            }
        })
        .await
        .is_ok()
    }
}
async fn settle(mut c: PgConnection) -> Result<(), Error> {
    let barrier = c.ping().await;
    let close = c.close().await;
    if barrier.is_err() || close.is_err() {
        Err(Error::Operational)
    } else {
        Ok(())
    }
}
async fn sentinel(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<(), Error> {
    sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,clock_timestamp()) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").execute(&mut **tx).await?;
    Ok(())
}
async fn schedule(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i64,
    epg: bool,
) -> Result<i64, Error> {
    let intervals:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_intervalschedule WHERE every=1 AND period='hours' LIMIT 2").fetch_all(&mut **tx).await?;
    if intervals.len() > 1 {
        return Err(Error::Unsupported);
    }
    let interval = if let Some(id) = intervals.first() {
        *id
    } else {
        sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES(1,'hours') RETURNING id::bigint").fetch_one(&mut **tx).await?
    };
    let name = format!(
        "{}-refresh-{id}",
        if epg { "epg_source" } else { "m3u_account" }
    );
    let existing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE name=$1)",
    )
    .bind(&name)
    .fetch_one(&mut **tx)
    .await?;
    if existing {
        return Err(Error::Unsupported);
    }
    sentinel(tx).await?;
    let task = if epg {
        "apps.epg.tasks.refresh_epg_data"
    } else {
        "apps.m3u.tasks.refresh_single_m3u_account"
    };
    let kwargs = format!(
        "{{\"{}\": {id}}}",
        if epg { "source_id" } else { "account_id" }
    );
    let result=sqlx::query_scalar("INSERT INTO django_celery_beat_periodictask(name,task,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off,interval_id) VALUES($1,$2,'[]',$3,'{}',false,0,clock_timestamp(),'',false,$4) RETURNING id::bigint").bind(name).bind(task).bind(kwargs).bind(interval).fetch_one(&mut **tx).await?;
    sentinel(tx).await?;
    Ok(result)
}
fn auto_import(settings: Option<&Value>) -> Result<bool, Error> {
    let value = match settings {
        None => return Ok(false),
        Some(Value::Object(v)) => v.get("auto_import_mapped_files").unwrap_or(&Value::Null),
        _ => return Err(Error::Unsupported),
    };
    match value {
        Value::Null => Ok(false),
        Value::Bool(v) => Ok(*v),
        Value::String(v) if matches!(v.as_str(), "true" | "True" | "TRUE") => Ok(v != "TRUE"),
        Value::Number(v) if v.as_i64() == Some(1) => Ok(true),
        _ => Err(Error::Unsupported),
    }
}
fn parse_marker(raw: &str) -> Result<Option<f64>, Error> {
    if raw.is_empty() {
        return Ok(None);
    }
    if raw.len() > 64 || !raw.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return Err(Error::Unsupported);
    }
    let value = raw.parse::<f64>().map_err(|_| Error::Unsupported)?;
    if !value.is_finite() {
        return Err(Error::Unsupported);
    }
    Ok(Some(value))
}
fn inspect(roots: Roots) -> Result<Vec<WatchFile>, Error> {
    for root in [&roots.m3u, &roots.epg, &roots.logos] {
        for ancestor in root.ancestors() {
            let m = std::fs::symlink_metadata(ancestor).map_err(|_| Error::Operational)?;
            if !m.is_dir() || m.file_type().is_symlink() {
                return Err(Error::Unsupported);
            }
        }
    }
    if std::fs::read_dir(&roots.logos)
        .map_err(|_| Error::Operational)?
        .next()
        .is_some()
    {
        return Err(Error::Unsupported);
    }
    let mut files = Vec::new();
    for (epg, root) in [(false, &roots.m3u), (true, &roots.epg)] {
        let mut entries = 0;
        let mut supported = 0;
        for entry in std::fs::read_dir(root).map_err(|_| Error::Operational)? {
            entries += 1;
            if entries > 8 {
                return Err(Error::Unsupported);
            }
            let path = entry.map_err(|_| Error::Operational)?.path();
            let m = std::fs::symlink_metadata(&path).map_err(|_| Error::Operational)?;
            if !m.is_file() || m.file_type().is_symlink() {
                return Err(Error::Unsupported);
            }
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .ok_or(Error::Unsupported)?
                .to_owned();
            let ext = path.extension().and_then(|v| v.to_str()).unwrap_or("");
            let known = if epg {
                matches!(ext, "xml" | "gz" | "zip" | "xz")
            } else {
                matches!(ext, "m3u" | "m3u8")
            };
            if !known {
                continue;
            }
            supported += 1;
            if supported > 1 {
                return Err(Error::Unsupported);
            }
            let text = path.to_str().ok_or(Error::Unsupported)?.to_owned();
            if text.len() > 4096 || name.chars().count() > if epg { 255 } else { 247 } {
                return Err(Error::Unsupported);
            }
            let time = m
                .modified()
                .map_err(|_| Error::Operational)?
                .duration_since(UNIX_EPOCH)
                .map_err(|_| Error::Unsupported)?
                .as_secs_f64();
            if !(100_000_000.0..10_000_000_000.0).contains(&time) {
                return Err(Error::Unsupported);
            }
            files.push(WatchFile {
                path: text,
                name,
                mtime: time,
                epg,
            })
        }
    }
    Ok(files)
}
