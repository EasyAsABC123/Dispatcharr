//! Actual startup-recovery SQL contexts and normal media/control boundary.
//! Original Dispatcharr/Django/Celery authors retain all algorithm/application credit.
//! No default startup or scheduler is enabled by merely preparing this service.
use chrono::{DateTime, Utc};
use dispatcharr_contracts::BoxFuture;
use serde_json::Value;
use sqlx::{Connection, PgPool, Row};
use std::sync::Arc;
use uuid::Uuid;
#[derive(Debug)]
pub enum Error {
    Capacity,
    Unsupported(&'static str),
    Database {
        primary: Option<Box<sqlx::Error>>,
        ready: Option<Box<sqlx::Error>>,
        close: Option<Box<sqlx::Error>>,
    },
    Supervisor,
    Media(&'static str, RecoveryMediaError),
    Publication(Box<crate::normal_recording_publication::Error>),
    Unsettled,
}
#[derive(Clone)]
struct RecordingRow {
    recording: i64,
    channel: i64,
    channel_uuid: Uuid,
    channel_name: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    task_key: Option<String>,
    properties: Value,
}
/// Sealed actual in-window Recording row, not caller-defined task arguments.
#[derive(Clone)]
pub struct ActiveRecording {
    row: Arc<RecordingRow>,
}
/// Sealed actual expired Recording row whose original row window is retained.
#[derive(Clone)]
pub(crate) struct UpcomingRecording {
    row: Arc<RecordingRow>,
}
pub struct ExpiredRecording {
    row: Arc<RecordingRow>,
}
macro_rules! row_getters {
    () => {
        pub fn recording_id(&self) -> i64 {
            self.row.recording
        }
        pub fn channel_id(&self) -> i64 {
            self.row.channel
        }
        pub fn channel_uuid(&self) -> Uuid {
            self.row.channel_uuid
        }
        pub fn channel_name(&self) -> &str {
            &self.row.channel_name
        }
        pub fn recording_start(&self) -> DateTime<Utc> {
            self.row.start
        }
        pub fn recording_end(&self) -> DateTime<Utc> {
            self.row.end
        }
        pub fn old_task_key(&self) -> Option<&str> {
            self.row.task_key.as_deref()
        }
        pub fn properties(&self) -> &Value {
            &self.row.properties
        }
    };
}
impl ActiveRecording {
    row_getters!();
}
impl UpcomingRecording {
    pub(crate) fn recording_id(&self) -> i64 {
        self.row.recording
    }
    pub(crate) fn channel_id(&self) -> i64 {
        self.row.channel
    }
    pub(crate) fn recording_start(&self) -> DateTime<Utc> {
        self.row.start
    }
    pub(crate) fn recording_end(&self) -> DateTime<Utc> {
        self.row.end
    }
}
impl ExpiredRecording {
    row_getters!();
}
#[derive(Clone, Copy, Debug)]
pub enum RecoveryMediaError {
    /// Actual operation failed after its SQL/process/file ownership settled.
    /// Source catches this per-recording failure and continues following rows.
    SettledFailure,
    Unsupported,
    Unsettled,
}
/// Real normal media/store/event/control implementation, never a no-op peer.
/// It keeps the actual owner until file/process/SQL/Channels/control settlement.
pub trait RecoveryMedia: Send + Sync {
    /// Admission capability only; never a successful recovery/task outcome.
    fn ensure_domain_supported(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecoveryMediaError>> {
        Box::pin(async move {
            let _owner = owner;
            Ok(())
        })
    }
    fn finalize_expired(
        &self,
        context: ExpiredRecording,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecoveryMediaError>>;
    fn revoke_old_task(
        &self,
        context: ActiveRecording,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), RecoveryMediaError>>;
}
/// Actual successfully claimed recovery-root invocation. Only this module's
/// root worker creates it after exact queue CAS; callers cannot mint UUID roots.
pub(crate) struct Invocation {
    task_id: Uuid,
    root: Uuid,
    now: DateTime<Utc>,
    _owner: Arc<dyn Send + Sync>,
}
impl Invocation {
    pub(crate) fn task_id(&self) -> Uuid {
        self.task_id
    }
    pub(crate) fn root(&self) -> Uuid {
        self.root
    }
    pub(crate) fn now(&self) -> DateTime<Utc> {
        self.now
    }
}
/// Minted only after real interrupted-state SQL succeeds and Ready/close settle.
pub(crate) struct InterruptedRecording {
    row: Arc<RecordingRow>,
}
impl InterruptedRecording {
    pub(crate) fn recording_id(&self) -> i64 {
        self.row.recording
    }
    pub(crate) fn channel_id(&self) -> i64 {
        self.row.channel
    }
    pub(crate) fn recording_start(&self) -> DateTime<Utc> {
        self.row.start
    }
    pub(crate) fn recording_end(&self) -> DateTime<Utc> {
        self.row.end
    }
}
#[derive(Clone, Copy)]
enum Bucket {
    Active,
    Expired,
    Upcoming,
}
fn database(error: sqlx::Error) -> Error {
    Error::Database {
        primary: Some(Box::new(error)),
        ready: None,
        close: None,
    }
}
async fn rows(
    pool: PgPool,
    now: DateTime<Utc>,
    bucket: Bucket,
    after_id: i64,
    owner: Arc<dyn Send + Sync>,
) -> Result<Vec<Arc<RecordingRow>>, Error> {
    tokio::spawn(async move{
  let _owner=owner;let mut c=pool.acquire().await.map_err(database)?.detach();
  let result=async{
   let predicate=match bucket{Bucket::Active=>"r.start_time<=$1 AND r.end_time>$1",Bucket::Expired=>"r.end_time<=$1 AND r.custom_properties->>'status'='recording'",Bucket::Upcoming=>"r.start_time>$1 AND r.end_time>$1"};
   let query=format!("SELECT r.id::bigint AS recording,r.channel_id::bigint AS channel,r.start_time,r.end_time,CASE WHEN COALESCE(octet_length(r.task_id),0)<=255 THEN r.task_id END AS task_key,CASE WHEN r.custom_properties IS NULL THEN '{{}}'::jsonb WHEN octet_length(r.custom_properties::text)<=8192 THEN r.custom_properties END AS properties,(COALESCE(octet_length(r.task_id),0)<=255 AND (r.custom_properties IS NULL OR octet_length(r.custom_properties::text)<=8192)) AS fits,c.uuid AS channel_uuid,CASE WHEN octet_length(c.name)<=1024 THEN c.name END AS channel_name FROM dispatcharr_channels_recording r JOIN dispatcharr_channels_channel c ON c.id=r.channel_id WHERE ({predicate}) AND r.id>$2 ORDER BY r.id LIMIT 64");
   let selected=sqlx::query(&query).bind(now).bind(after_id).fetch_all(&mut c).await.map_err(database)?;
   let mut result=Vec::with_capacity(selected.len());for row in selected{
    if !row.try_get::<bool,_>("fits").map_err(database)?{return Err(Error::Capacity);}
    result.push(Arc::new(RecordingRow{recording:row.try_get("recording").map_err(database)?,channel:row.try_get("channel").map_err(database)?,channel_uuid:row.try_get("channel_uuid").map_err(database)?,channel_name:row.try_get::<Option<String>,_>("channel_name").map_err(database)?.ok_or(Error::Capacity)?,start:row.try_get("start_time").map_err(database)?,end:row.try_get("end_time").map_err(database)?,task_key:row.try_get("task_key").map_err(database)?,properties:row.try_get::<Option<Value>,_>("properties").map_err(database)?.ok_or(Error::Capacity)?}));
   }Ok(result)
  }.await;
  let ready=c.ping().await.err().map(Box::new);let close=c.close().await.err().map(Box::new);
  match result{Err(Error::Database{primary,..})=>Err(Error::Database{primary,ready,close}),other=>if ready.is_some()||close.is_some(){Err(Error::Database{primary:None,ready,close})}else{other}}
 }).await.map_err(|_|Error::Supervisor)?
}
async fn interrupt(
    pool: PgPool,
    row: Arc<RecordingRow>,
    owner: Arc<dyn Send + Sync>,
) -> Result<Option<InterruptedRecording>, Error> {
    let Some(mut properties) = row.properties.as_object().cloned() else {
        return Ok(None);
    };
    if properties
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| matches!(status, "completed" | "stopped"))
    {
        return Ok(None);
    }
    properties.insert("status".into(), serde_json::json!("interrupted"));
    properties.insert(
        "interrupted_reason".into(),
        serde_json::json!("server_restarted"),
    );
    let properties = Value::Object(properties);
    tokio::spawn(async move {
        let _owner = owner;
        let mut c = pool.acquire().await.map_err(database)?.detach();
        let observed = sqlx::query(
            "UPDATE dispatcharr_channels_recording SET custom_properties=$1 WHERE id=$2",
        )
        .bind(&properties)
        .bind(row.recording)
        .execute(&mut c)
        .await;
        let ready = c.ping().await.err().map(Box::new);
        let close = c.close().await.err().map(Box::new);
        let changed = match observed {
            Ok(value) => value.rows_affected(),
            Err(error) => {
                return Err(Error::Database {
                    primary: Some(Box::new(error)),
                    ready,
                    close,
                })
            }
        };
        if ready.is_some() || close.is_some() {
            return Err(Error::Database {
                primary: None,
                ready,
                close,
            });
        }
        if changed != 1 {
            return Err(Error::Unsupported(
                "recording disappeared during interruption",
            ));
        }
        let mut actual = (*row).clone();
        actual.properties = properties;
        Ok(Some(InterruptedRecording {
            row: Arc::new(actual),
        }))
    })
    .await
    .map_err(|_| Error::Supervisor)?
}

/// Prepared real recovery execution. The root invocation is admitted only by
/// RecoveryWorker after exact FIFO claim; SQL contexts cannot be caller minted.
#[derive(Clone)]
pub struct Service {
    pool: PgPool,
    publisher: crate::normal_recording_publication::NormalRecordingPublisher,
    media: Arc<dyn RecoveryMedia>,
    cache: Option<RecoveryCache>,
}
#[derive(Debug)]
pub(crate) struct Report {
    pub(crate) result: &'static str,
    pub(crate) children: Vec<String>,
}
#[derive(Clone)]
struct RecoveryCache {
    connection: redis::aio::ConnectionManager,
    slot: Arc<tokio::sync::Semaphore>,
}
impl RecoveryCache {
    async fn connect(url: String, owner: Arc<dyn Send + Sync>) -> Result<Self, Error> {
        tokio::spawn(async move {
            let _owner = owner;
            let client = redis::Client::open(url).map_err(|_| Error::Unsettled)?;
            let connection = client
                .get_connection_manager_with_config(
                    redis::aio::ConnectionManagerConfig::new()
                        .set_number_of_retries(0)
                        .set_connection_timeout(std::time::Duration::from_secs(2)),
                )
                .await
                .map_err(|_| Error::Unsettled)?;
            Ok(Self {
                connection,
                slot: Arc::new(tokio::sync::Semaphore::new(1)),
            })
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    async fn command<T: redis::FromRedisValue + Send + 'static>(
        &self,
        command: redis::Cmd,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<T, Error> {
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let mut connection = self.connection.clone();
        tokio::spawn(async move {
            let (_owner, _permit) = (owner, permit);
            command
                .query_async(&mut connection)
                .await
                .map_err(|_| Error::Unsettled)
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
}
impl Service {
    pub async fn prepare(
        pool: PgPool,
        publisher: crate::normal_recording_publication::NormalRecordingPublisher,
        media: Arc<dyn RecoveryMedia>,
        cache_url: Option<&str>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self, Error> {
        if !publisher.uses_pool(&pool) {
            return Err(Error::Unsupported("recovery publisher must share SQL pool"));
        }
        let cache = match cache_url {
            Some(url) => Some(RecoveryCache::connect(url.into(), owner).await?),
            None => None,
        };
        Ok(Self {
            pool,
            publisher,
            media,
            cache,
        })
    }
    pub(crate) async fn execute(&self, invocation: &Invocation) -> Result<Report, Error> {
        self.media
            .ensure_domain_supported(invocation._owner.clone())
            .await
            .map_err(|error| Error::Media("domain_before_effects", error))?;
        let owner = invocation._owner.clone();
        let Some(cache) = &self.cache else {
            return Ok(Report {
                result: "Redis unavailable",
                children: vec![],
            });
        };
        let mut lock = redis::cmd("SET");
        lock.arg("dvr:recover_lock")
            .arg("1")
            .arg("EX")
            .arg(600)
            .arg("NX");
        let acquired: Option<String> = cache.command(lock, owner.clone()).await?;
        match acquired.as_deref() {
            None => {
                return Ok(Report {
                    result: "Recovery already in progress",
                    children: vec![],
                })
            }
            Some("OK") => {}
            _ => return Err(Error::Unsettled),
        }
        let admitted = Invocation {
            task_id: invocation.task_id,
            root: invocation.root,
            now: Utc::now(),
            _owner: owner.clone(),
        };
        let invocation = &admitted;
        // This now value belongs to the admitted root invocation, not row starts
        // or copied Source timestamps. All three phase queries share it.
        let mut children = Vec::new();
        {
            let mut cursor = 0;
            loop {
                let page = rows(
                    self.pool.clone(),
                    invocation.now,
                    Bucket::Active,
                    cursor,
                    owner.clone(),
                )
                .await?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().expect("nonempty page").recording;
                for row in page {
                    if row
                        .properties
                        .get("status")
                        .and_then(Value::as_str)
                        .is_some_and(|s| matches!(s, "completed" | "stopped"))
                    {
                        continue;
                    }
                    let mut exists = redis::cmd("EXISTS");
                    exists.arg(format!("dvr:recording-active:{}", row.recording));
                    if cache.command::<i64>(exists, owner.clone()).await? != 0 {
                        continue;
                    }
                    let context = ActiveRecording { row: row.clone() };
                    let interrupted = match interrupt(self.pool.clone(), row, owner.clone()).await {
                        Ok(Some(value)) => value,
                        Ok(None) => continue,
                        Err(Error::Database {
                            primary: Some(_),
                            ready: None,
                            close: None,
                        }) => continue,
                        Err(error) => return Err(error),
                    };
                    match self.media.revoke_old_task(context, owner.clone()).await {
                        Ok(()) | Err(RecoveryMediaError::SettledFailure) => {}
                        Err(error) => return Err(Error::Media("revoke_old_task", error)),
                    }
                    children.push(
                        self.publisher
                            .publish_recovery(interrupted, invocation, owner.clone())
                            .await
                            .map_err(publication_error)?,
                    );
                }
            }
        }
        {
            let mut cursor = 0;
            loop {
                let page = rows(
                    self.pool.clone(),
                    invocation.now,
                    Bucket::Expired,
                    cursor,
                    owner.clone(),
                )
                .await?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().expect("nonempty page").recording;
                for row in page {
                    match self
                        .media
                        .finalize_expired(ExpiredRecording { row }, owner.clone())
                        .await
                    {
                        Ok(()) | Err(RecoveryMediaError::SettledFailure) => {}
                        Err(error) => return Err(Error::Media("finalize_expired", error)),
                    }
                }
            }
        }
        {
            let mut cursor = 0;
            loop {
                let page = rows(
                    self.pool.clone(),
                    invocation.now,
                    Bucket::Upcoming,
                    cursor,
                    owner.clone(),
                )
                .await?;
                if page.is_empty() {
                    break;
                }
                cursor = page.last().expect("nonempty page").recording;
                for row in page {
                    let upcoming = UpcomingRecording { row };
                    let repair =
                        match repair_future(self.pool.clone(), &upcoming, owner.clone()).await {
                            Ok(value) => value,
                            Err(Error::Database {
                                primary: Some(_),
                                ready: None,
                                close: None,
                            }) => continue,
                            Err(error) => return Err(error),
                        };
                    match repair {
                        Repair::Scheduled => {}
                        Repair::Immediate => {
                            let key = self
                                .publisher
                                .publish_immediate_repair(&upcoming, invocation, owner.clone())
                                .await
                                .map_err(publication_error)?;
                            assign_task(
                                self.pool.clone(),
                                upcoming.recording_id(),
                                key.clone(),
                                owner.clone(),
                            )
                            .await?;
                            children.push(key);
                        }
                    }
                }
            }
        }
        let mut release = redis::cmd("DEL");
        release.arg("dvr:recover_lock");
        let _: i64 = cache.command(release, owner).await?;
        Ok(Report {
            result: "Recovery complete",
            children,
        })
    }
}
fn publication_error(error: crate::normal_recording_publication::Error) -> Error {
    Error::Publication(Box::new(error))
}
enum Repair {
    Scheduled,
    Immediate,
}
async fn assign_task(
    pool: PgPool,
    recording: i64,
    key: String,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    tokio::spawn(async move {
        let _owner = owner;
        let mut connection = pool.acquire().await.map_err(database)?.detach();
        let result =
            sqlx::query("UPDATE dispatcharr_channels_recording SET task_id=$1 WHERE id=$2")
                .bind(key)
                .bind(recording)
                .execute(&mut connection)
                .await;
        let ready = connection.ping().await.err().map(Box::new);
        let close = connection.close().await.err().map(Box::new);
        match result {
            Err(error) => Err(Error::Database {
                primary: Some(Box::new(error)),
                ready,
                close,
            }),
            Ok(result) => {
                if ready.is_some() || close.is_some() {
                    Err(Error::Database {
                        primary: None,
                        ready,
                        close,
                    })
                } else if result.rows_affected() != 1 {
                    Err(Error::Unsupported("recording removed during assignment"))
                } else {
                    Ok(())
                }
            }
        }
    })
    .await
    .map_err(|_| Error::Supervisor)?
}
async fn repair_future(
    pool: PgPool,
    upcoming: &UpcomingRecording,
    owner: Arc<dyn Send + Sync>,
) -> Result<Repair, Error> {
    let row = upcoming.row.clone();
    tokio::spawn(async move {let _owner=owner;let mut connection=pool.acquire().await.map_err(database)?.detach();
  let observed=async {
   let name=format!("dvr-recording-{}",row.recording);
   let existing:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE name=$1)").bind(&name).fetch_one(&mut connection).await.map_err(database)?;
   if !existing {
    if row.start<=Utc::now(){return Ok(Repair::Immediate);}
    let clocked:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_clockedschedule WHERE clocked_time=$1 LIMIT 2").bind(row.start).fetch_all(&mut connection).await.map_err(database)?;
    if clocked.len()>1{return Err(Error::Unsupported("multiple clocked schedules at one time"));}
    let mut transaction=connection.begin().await.map_err(database)?;
    let changed=async {
     let clocked=match clocked.first(){Some(id)=>*id,None=>sqlx::query_scalar("INSERT INTO django_celery_beat_clockedschedule (clocked_time) VALUES ($1) RETURNING id::bigint").bind(row.start).fetch_one(&mut *transaction).await.map_err(database)?};
     let args=serde_json::to_string(&serde_json::json!([row.recording,row.channel,crate::normal_recording_publication::python_datetime(row.start,false),crate::normal_recording_publication::python_datetime(row.end,false)])).map_err(|_|Error::Unsupported("recording args"))?;
     sqlx::query("INSERT INTO django_celery_beat_periodictask (name,task,interval_id,crontab_id,solar_id,clocked_id,args,kwargs,queue,exchange,routing_key,headers,priority,expires,expire_seconds,one_off,start_time,enabled,last_run_at,total_run_count,date_changed,description) VALUES ($1,'apps.channels.tasks.run_recording',NULL,NULL,NULL,$2,$3,'{}',NULL,NULL,NULL,'{}',NULL,NULL,NULL,true,NULL,true,NULL,0,$4,'') ON CONFLICT (name) DO UPDATE SET task=EXCLUDED.task,clocked_id=EXCLUDED.clocked_id,args=EXCLUDED.args,one_off=true,enabled=true,date_changed=EXCLUDED.date_changed")
      .bind(&name).bind(clocked).bind(args).bind(Utc::now()).execute(&mut *transaction).await.map_err(database)?;
     sqlx::query("INSERT INTO django_celery_beat_periodictasks (ident,last_update) VALUES (1,$1) ON CONFLICT (ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&mut *transaction).await.map_err(database)?;
     Ok::<_,Error>(())
    }.await;
    match changed {Ok(())=>transaction.commit().await.map_err(database)?,Err(error)=>{let rollback=transaction.rollback().await.err();if let Some(error)=rollback{return Err(database(error));}return Err(error);}}
   }
   let assigned=sqlx::query("UPDATE dispatcharr_channels_recording SET task_id=$1 WHERE id=$2").bind(name).bind(row.recording).execute(&mut connection).await.map_err(database)?;
   if assigned.rows_affected()!=1{return Err(Error::Unsupported("upcoming recording removed"));}Ok(Repair::Scheduled)
  }.await;
  let ready=connection.ping().await.err().map(Box::new);let close=connection.close().await.err().map(Box::new);
  match observed {Err(Error::Database{primary,..})=>Err(Error::Database{primary,ready,close}),other=>if ready.is_some()||close.is_some(){Err(Error::Database{primary:None,ready,close})}else{other}}
 }).await.map_err(|_|Error::Supervisor)?
}

struct WorkerInner {
    jobs: crate::transport::RecoveryJobs,
    service: Service,
    permits: Arc<tokio::sync::Semaphore>,
    closed: std::sync::atomic::AtomicBool,
    uncertain: std::sync::Mutex<Option<Arc<RecoveryOwner>>>,
    startup_recovery_deferred: std::sync::atomic::AtomicBool,
}
struct RecoveryOwner {
    _caller: Arc<dyn Send + Sync>,
    _slot: tokio::sync::OwnedSemaphorePermit,
}
#[derive(Clone)]
pub struct Worker {
    inner: Arc<WorkerInner>,
}
/// Actual startup publications, not worker_ready/Ready evidence by themselves.
#[derive(Debug)]
pub struct StartupPublications {
    pub recovery: Option<Uuid>,
    pub version: Option<Uuid>,
    pub recovery_deferred_unsupported_domain: bool,
}
impl Worker {
    pub fn prepare(jobs: crate::transport::RecoveryJobs, service: Service) -> Self {
        Self {
            inner: Arc::new(WorkerInner {
                jobs,
                service,
                permits: Arc::new(tokio::sync::Semaphore::new(1)),
                closed: std::sync::atomic::AtomicBool::new(false),
                uncertain: std::sync::Mutex::new(None),
                startup_recovery_deferred: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }
    /// Call only after actual normal queue graph is installed. Uses Source300s
    /// worker_ready dispatch locks and publishes each permitted root only once.
    /// Redis lock failure allows one attempt as Source, but no publish retry.
    pub async fn publish_startup(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<StartupPublications, Error> {
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Unsupported("recovery startup closed"));
        }
        let slot = self
            .inner
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::Unsupported("recovery startup closed"));
        }
        let owner: Arc<dyn Send + Sync> = Arc::new(RecoveryOwner {
            _caller: owner,
            _slot: slot,
        });
        let worker = self.clone();
        tokio::spawn(async move { worker.publish_startup_owned(owner).await })
            .await
            .map_err(|_| Error::Supervisor)?
    }
    pub fn startup_recovery_deferred(&self) -> bool {
        self.inner
            .startup_recovery_deferred
            .load(std::sync::atomic::Ordering::Acquire)
    }
    async fn publish_startup_owned(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<StartupPublications, Error> {
        let mut published = [None, None];
        for (index, key, task) in [
            (
                0,
                "dvr:recover_dispatch_lock",
                "apps.channels.tasks.recover_recordings_on_startup",
            ),
            (
                1,
                crate::version_protocol::STARTUP_LOCK,
                crate::version_protocol::TASK,
            ),
        ] {
            if index == 0 {
                match self
                    .inner
                    .service
                    .media
                    .ensure_domain_supported(owner.clone())
                    .await
                {
                    Ok(()) => self
                        .inner
                        .startup_recovery_deferred
                        .store(false, std::sync::atomic::Ordering::Release),
                    Err(RecoveryMediaError::Unsupported) => {
                        self.inner
                            .startup_recovery_deferred
                            .store(true, std::sync::atomic::Ordering::Release);
                        // No Redis startup lock, publication, ACK or fake result.
                        continue;
                    }
                    Err(error) => return Err(Error::Media("startup_domain", error)),
                }
            }
            let allowed = match &self.inner.service.cache {
                Some(cache) => {
                    let mut command = redis::cmd("SET");
                    command
                        .arg(key)
                        .arg("1")
                        .arg("NX")
                        .arg("EX")
                        .arg(crate::version_protocol::STARTUP_LOCK_SECONDS);
                    match cache
                        .command::<Option<String>>(command, owner.clone())
                        .await
                    {
                        Ok(None) => false,
                        Ok(Some(value)) if value == "OK" => true,
                        Ok(Some(_)) => return Err(Error::Unsettled),
                        Err(_) => true,
                    }
                }
                None => true,
            };
            if allowed {
                let id = Uuid::new_v4();
                let raw = self
                    .inner
                    .service
                    .publisher
                    .startup_frame(task, id)
                    .map_err(publication_error)?;
                self.inner
                    .jobs
                    .publish(&raw, owner.clone())
                    .await
                    .map_err(|_| Error::Unsettled)?;
                published[index] = Some(id);
            }
        }
        Ok(StartupPublications {
            recovery: published[0],
            version: published[1],
            recovery_deferred_unsupported_domain: self.startup_recovery_deferred(),
        })
    }
    async fn run(
        &self,
        expected: Option<Arc<[u8]>>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>
    {
        use crate::milestone_dispatch::Disposition;
        if self.inner.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Disposition::Operational("recovery intake closed".into()));
        }
        let slot = self
            .inner
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Disposition::Capacity)?;
        let owner = Arc::new(RecoveryOwner {
            _caller: caller,
            _slot: slot,
        });
        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner
                .service
                .media
                .ensure_domain_supported(owner.clone())
                .await
                .map_err(|error| match error {
                    RecoveryMediaError::Unsupported => Disposition::UnsupportedDomain,
                    _ => Disposition::Operational("recovery domain unavailable".into()),
                })?;
            let message = match inner.jobs.claim(expected.as_deref(), owner.clone()).await {
                Ok(Some(message)) => message,
                Ok(None) => return Ok(Disposition::Empty),
                Err(crate::transport::Error::ClaimRace) => return Ok(Disposition::HeadChanged),
                Err(crate::transport::Error::UnsupportedTask) => {
                    return Ok(Disposition::UnsupportedHead)
                }
                Err(error) => {
                    if matches!(error, crate::transport::Error::AmbiguousClaim(_)) {
                        *inner.uncertain.lock().map_err(|_| {
                            Disposition::Operational("recovery owner lock".into())
                        })? = Some(owner);
                    }
                    return Err(Disposition::Operational(format!(
                        "recovery claim: {error:?}"
                    )));
                }
            };
            let invocation = Invocation {
                task_id: message.task_id,
                root: message.task_id,
                now: Utc::now(),
                _owner: owner.clone(),
            };
            let result = inner.service.execute(&invocation).await;
            let report =
                match result {
                    Ok(report) => report,
                    Err(error) => {
                        *inner.uncertain.lock().map_err(|_| {
                            Disposition::Operational("recovery owner lock".into())
                        })? = Some(owner);
                        return Err(Disposition::Operational(format!(
                            "recovery incomplete: {error:?}"
                        )));
                    }
                };
            match inner.jobs.write(&message, &report, owner.clone()).await {
                Ok(_) => Ok(Disposition::Completed(message.task_id)),
                Err(error) => {
                    *inner
                        .uncertain
                        .lock()
                        .map_err(|_| Disposition::Operational("recovery owner lock".into()))? =
                        Some(owner);
                    Err(Disposition::Operational(format!(
                        "recovery backend: {error:?}"
                    )))
                }
            }
        })
        .await
        .map_err(|_| Disposition::Operational("recovery supervisor".into()))?
    }
}
impl crate::milestone_dispatch::Handler for Worker {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_once(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(self.run(None, owner))
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<
        '_,
        Result<crate::milestone_dispatch::Disposition, crate::milestone_dispatch::Disposition>,
    > {
        Box::pin(self.run(Some(expected), owner))
    }
}
impl crate::milestone_dispatch::Lifecycle for Worker {
    fn stop_admission(&self) {
        self.inner
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
    }
    fn shutdown(
        &self,
        timeout: std::time::Duration,
    ) -> BoxFuture<'_, Result<(), crate::milestone_dispatch::Disposition>> {
        Box::pin(async move {
            use crate::milestone_dispatch::Disposition;
            self.inner
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            let deadline = tokio::time::Instant::now() + timeout;
            let _quiet =
                tokio::time::timeout_at(deadline, self.inner.permits.clone().acquire_owned())
                    .await
                    .map_err(|_| Disposition::Operational("recovery physical drain".into()))?
                    .map_err(|_| Disposition::Operational("recovery closed".into()))?;
            self.inner
                .jobs
                .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await
                .map_err(|e| Disposition::Operational(format!("recovery port drain: {e:?}")))
        })
    }
}
