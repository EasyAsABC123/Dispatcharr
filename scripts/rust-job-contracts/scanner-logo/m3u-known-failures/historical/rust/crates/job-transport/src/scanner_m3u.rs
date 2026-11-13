//! Owned local standard-M3U task domain; no Main/poller/default registration.
//! Original Dispatcharr authors retain application and algorithm credit.
use crate::{
    logo_task::Clock,
    scanner_m3u_files::{self as files, Catalog},
    scanner_m3u_protocol::Kind,
    transport::M3uPorts,
};
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_coordination::{TaskLease, TaskLockDetailedError, TaskLockError, TaskLocks};
use dispatcharr_ingest_core::{project_entry, stream_hash, ProjectionContext, StreamIdentity};
use dispatcharr_store::{
    AuthService, BoundedM3uBatchError, GroupCleanupAccount, ImportedGroup, ImportedStream,
    M3uGroupImportFailure, M3uGroupImportStage, ScannerM3uLookupCap, Store, StoreError,
};
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub type Owner = Arc<dyn Send + Sync>;
pub type DateClock = Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Configuration,
    Capacity,
    Closed,
    Unsupported,
    Operational,
    DrainTimeout,
}
/// Only two observed raised M3U Groups failures, after actual operation and
/// ownership settlement. Unknown errors never construct this type.
#[derive(Clone, Debug)]
pub(crate) struct KnownFailure {
    class: &'static str,
    module: &'static str,
    arguments: Vec<Value>,
    diagnostic: String,
}
impl KnownFailure {
    fn missing(error: &std::io::Error) -> Result<Self, Error> {
        if error.raw_os_error() != Some(2) {
            return Err(Error::Operational);
        }
        Ok(Self { class: "FileNotFoundError", module: "builtins",
            arguments: vec![json!(2), json!("No such file or directory")],
            diagnostic: "Native Dispatcharr M3U groups diagnostic [owned_std_open]: actual ENOENT; filesystem work completed; owner-confirmed groups key retained until its existing TTL, native renewal stopped. No Python frames executed.".into() })
    }
    fn group_create(failure: &M3uGroupImportFailure) -> Result<Self, Error> {
        let StoreError::Database(error) = &failure.source else {
            return Err(Error::Operational);
        };
        let Some(pg) = error
            .as_database_error()
            .and_then(|e| e.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
        else {
            return Err(Error::Operational);
        };
        Self::programming(
            failure.stage,
            pg.severity(),
            crate::pg_failure_format::Fields {
                sqlstate: pg.code(),
                message: pg.message(),
                detail: pg.detail(),
                hint: pg.hint(),
                context: pg.r#where(),
                has_position: pg.position().is_some(),
            },
        )
    }
    fn programming(
        stage: M3uGroupImportStage,
        severity: sqlx::postgres::PgSeverity,
        fields: crate::pg_failure_format::Fields<'_>,
    ) -> Result<Self, Error> {
        if stage != M3uGroupImportStage::CreateGroups
            || severity != sqlx::postgres::PgSeverity::Error
        {
            return Err(Error::Operational);
        }
        let message = crate::pg_failure_format::render(fields).map_err(|_| Error::Operational)?;
        Ok(Self { class: "ProgrammingError", module: "django.db.utils", arguments: vec![json!(message)],
            diagnostic: "Native Dispatcharr M3U groups diagnostic [create_groups]: actual PostgreSQL ERROR P0001; ReadyForQuery and dedicated connection close settled; parsed cache retained; owner-confirmed groups key retained until its existing TTL, native renewal stopped. No Python frames executed.".into() })
    }
    pub(crate) fn metadata(
        &self,
        message: &crate::scanner_m3u_protocol::Message,
        date: &str,
    ) -> Result<Vec<u8>, crate::transport::Error> {
        if self
            .arguments
            .iter()
            .filter_map(Value::as_str)
            .any(|v| v.len() > 8192)
            || self.diagnostic.len() > 16384
            || date.len() > 40
            || chrono::DateTime::parse_from_rfc3339(date).is_err()
        {
            return Err(crate::transport::Error::Protocol);
        }
        let bytes = serde_json::to_vec(&json!({"status":"FAILURE","result":{"exc_type":self.class,"exc_module":self.module,"exc_message":self.arguments},"traceback":self.diagnostic,"children":[],"date_done":date,"task_id":message.id.to_string(),"parent_id":message.parent.to_string()})).map_err(|_| crate::transport::Error::Protocol)?;
        crate::decode::decode(&bytes, message.id).map_err(|_| crate::transport::Error::Protocol)?;
        Ok(bytes)
    }
}
pub(crate) enum Terminal {
    Normal(Value),
    Raised(KnownFailure),
}
enum GroupsError {
    Operational(Error),
    Missing(std::io::Error),
    Import(M3uGroupImportFailure),
}
impl From<Error> for GroupsError {
    fn from(error: Error) -> Self {
        Self::Operational(error)
    }
}
// Only these observed audit statements may produce a caught normal return.
// The owned error is constructed into this type only after Ready/close settles.
#[derive(Clone, Copy)]
enum AuditStage {
    Insert,
    Delete,
}
struct SettledAuditFailure {
    stage: AuditStage,
    source: sqlx::Error,
}
impl SettledAuditFailure {
    fn caught(self) -> bool {
        let allowed_stage = matches!(self.stage, AuditStage::Insert | AuditStage::Delete);
        allowed_stage
            && self.source.as_database_error().is_some_and(|error| {
                error.code().as_deref() == Some("P0001")
                    && error
                        .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                        .is_some_and(|pg| pg.severity() == sqlx::postgres::PgSeverity::Error)
            })
    }
}
#[derive(Clone)]
pub struct Roots {
    pub input: PathBuf,
    pub cache: PathBuf,
}
#[derive(Clone)]
struct Account {
    id: i64,
    name: String,
    path: PathBuf,
    active: bool,
    status: String,
    message: Option<String>,
    locked: bool,
    days: i64,
}
struct Done(Option<tokio::sync::oneshot::Sender<()>>);
impl Drop for Done {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
struct Operation {
    _permit: OwnedSemaphorePermit,
    _caller: Owner,
    _done: Done,
}
#[derive(Clone, Copy)]
enum HashMode {
    Empty,
    Url,
}
impl HashMode {
    fn keys(self) -> &'static [&'static str] {
        match self {
            Self::Empty => &[],
            Self::Url => &["url"],
        }
    }
}
pub(crate) struct Prepared {
    hash_mode: HashMode,
    account: Account,
    input: Option<Catalog>,
    missing_input: bool,
    cached: Option<Catalog>,
    owner: Arc<Operation>,
    done: tokio::sync::oneshot::Receiver<()>,
}
impl Prepared {
    pub(crate) fn output_cache_needed(&self, kind: Kind) -> bool {
        kind == Kind::Single
            && self.account.active
            && self
                .cached
                .as_ref()
                .or(self.input.as_ref())
                .is_some_and(|v| !v.entries.is_empty())
    }
}
struct Inner {
    store: Store,
    auth: AuthService,
    sink: Arc<dyn UpdateEventSink>,
    locks: TaskLocks,
    ports: M3uPorts,
    roots: Roots,
    date: DateClock,
    elapsed: Arc<dyn Clock>,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct Service {
    inner: Arc<Inner>,
}
impl Service {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Store,
        auth: AuthService,
        sink: Arc<dyn UpdateEventSink>,
        locks: TaskLocks,
        ports: M3uPorts,
        roots: Roots,
        date: DateClock,
        elapsed: Arc<dyn Clock>,
    ) -> Result<Self, Error> {
        if !auth.uses_store_pool(&store)
            || store.pool().options().get_max_connections() != 1
            || !roots.input.is_absolute()
            || !roots.cache.is_absolute()
            || roots
                .input
                .components()
                .chain(roots.cache.components())
                .any(|v| {
                    matches!(
                        v,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    )
                })
            || roots.input == roots.cache
        {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                store,
                auth,
                sink,
                locks,
                ports,
                roots,
                date,
                elapsed,
                slots: Arc::new(Semaphore::new(1)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        let v = (self.inner.date)();
        chrono::DateTime::from_timestamp_micros(v.timestamp_micros()).expect("valid clock")
    }
    pub(crate) async fn prepare(
        &self,
        id: i64,
        kind: Kind,
        caller: Owner,
    ) -> Result<Prepared, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let (sender, done) = tokio::sync::oneshot::channel();
        let owner = Arc::new(Operation {
            _permit: permit,
            _caller: caller,
            _done: Done(Some(sender)),
        });
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let result = self.prepare_inner(id, kind, owner.clone(), done).await;
        // All outcomes, including unsupported/error exits, wait for the raw
        // dedicated pool's actual Ready/close before local ownership is dropped.
        let fenced = self.settle().await;
        drop(owner);
        fenced?;
        result
    }
    async fn prepare_inner(
        &self,
        id: i64,
        kind: Kind,
        owner: Arc<Operation>,
        done: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<Prepared, Error> {
        let row=sqlx::query("SELECT id::bigint,name,file_path,is_active,status,CASE WHEN COALESCE(octet_length(last_message),0)<=8192 THEN last_message ELSE NULL END AS last_message,locked,stale_stream_days::bigint, (account_type='STD' AND server_url IS NULL AND user_agent_id IS NULL AND stream_profile_id IS NULL AND username IS NULL AND password IS NULL AND custom_properties='{}'::jsonb AND octet_length(name)<=512 AND octet_length(file_path)<=4096 AND COALESCE(octet_length(last_message),0)<=8192) AS known FROM m3u_m3uaccount WHERE id=$1").bind(id).fetch_optional(self.inner.store.pool()).await.map_err(|_|Error::Operational)?.ok_or(Error::Unsupported)?;
        if !row
            .try_get::<bool, _>("known")
            .map_err(|_| Error::Unsupported)?
        {
            return Err(Error::Unsupported);
        }
        let account = Account {
            id,
            name: row.try_get("name").map_err(|_| Error::Operational)?,
            path: PathBuf::from(
                row.try_get::<String, _>("file_path")
                    .map_err(|_| Error::Unsupported)?,
            ),
            active: row.try_get("is_active").map_err(|_| Error::Operational)?,
            status: row.try_get("status").map_err(|_| Error::Operational)?,
            message: row
                .try_get("last_message")
                .map_err(|_| Error::Operational)?,
            locked: row.try_get("locked").map_err(|_| Error::Operational)?,
            days: row
                .try_get("stale_stream_days")
                .map_err(|_| Error::Operational)?,
        };
        if !account.name.is_ascii()
            || !account.path.starts_with(&self.inner.roots.input)
            || account.path.parent() != Some(self.inner.roots.input.as_path())
            || account.path.extension().and_then(|v| v.to_str()) != Some("m3u")
            || account.days < 0
            || account.days > 3650
        {
            return Err(Error::Unsupported);
        }
        let hash_mode = self.hash_setting(owner.clone()).await?;
        let forbidden:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM m3u_m3ufilter WHERE m3u_account_id=$1) OR EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 AND auto_channel_sync) OR EXISTS(SELECT 1 FROM dispatcharr_connect_eventsubscription s JOIN dispatcharr_connect_integration i ON i.id=s.integration_id WHERE s.enabled AND i.enabled AND s.event IN('m3u_refresh','m3u_error')) OR EXISTS(SELECT 1 FROM plugins_pluginconfig WHERE enabled) OR EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE s.m3u_account_id=$1) OR EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN('m3u_m3uaccount'::regclass,'dispatcharr_channels_channelgroup'::regclass,'dispatcharr_channels_channelgroupm3uaccount'::regclass,'core_systemevent'::regclass))").bind(id).fetch_one(self.inner.store.pool()).await.map_err(|_|Error::Operational)?;
        if forbidden {
            return Err(Error::Unsupported);
        }
        let domain:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM(SELECT id FROM dispatcharr_channels_stream WHERE m3u_account_id=$1 LIMIT 65)s)+(SELECT count(*) FROM(SELECT id FROM dispatcharr_channels_channelgroupm3uaccount WHERE m3u_account_id=$1 LIMIT 17)g)").bind(id).fetch_one(self.inner.store.pool()).await.map_err(|_|Error::Operational)?;
        if domain > 64 {
            return Err(Error::Capacity);
        }
        let mut prepared = Prepared {
            hash_mode,
            account,
            input: None,
            missing_input: false,
            cached: None,
            owner,
            done,
        };
        if prepared.account.active {
            let path = prepared.account.path.clone();
            let cache = self.cache_path(id);
            let retained = prepared.owner.clone();
            let (input, cached) = tokio::task::spawn_blocking(move || {
                let _retained = retained;
                Ok::<_, Error>((files::probe_input(&path)?, files::cached(&cache)?))
            })
            .await
            .map_err(|_| Error::Operational)??;
            match input {
                files::InputProbe::Present(catalog) => prepared.input = Some(catalog),
                files::InputProbe::Missing => prepared.missing_input = true,
            }
            prepared.cached = cached;
        } else if kind == Kind::Single {
            return Err(Error::Unsupported);
        }
        if kind == Kind::Single
            && prepared.missing_input
            && prepared
                .cached
                .as_ref()
                .is_none_or(|v| v.entries.is_empty())
            && !self
                .inner
                .ports
                .groups_lock_present(id, prepared.owner.clone())
                .await
                .map_err(|_| Error::Operational)?
        {
            return Err(Error::Unsupported);
        }
        // Bounded conservative superset from both owned catalogs. This is
        // admission only: never change input order or first-row deduplication.
        let mut hashes = Vec::new();
        if matches!(hash_mode, HashMode::Empty) {
            hashes.push(
                "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".to_owned(),
            );
        } else {
            for catalog in prepared.input.iter().chain(prepared.cached.iter()) {
                for entry in &catalog.entries {
                    let url = entry.url.as_deref().ok_or(Error::Unsupported)?;
                    hashes.push(stream_hash(
                        &StreamIdentity {
                            name: &entry.name,
                            url,
                            tvg_id: "",
                            m3u_id: Some(id),
                            group: None,
                            account_type: "STD",
                            stream_id: None,
                        },
                        hash_mode.keys(),
                    ));
                }
            }
        }
        if hashes.len() > 16 {
            return Err(Error::Capacity);
        }
        hashes.sort();
        hashes.dedup();
        let mapped: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_channelstream cs JOIN dispatcharr_channels_stream s ON s.id=cs.stream_id WHERE s.stream_hash=ANY($1))")
            .bind(&hashes).fetch_one(self.inner.store.pool()).await.map_err(|_|Error::Operational)?;
        if mapped {
            return Err(Error::Unsupported);
        }
        // Fetch only a boolean, never a large matched field. This rejects
        // known unsupported rows before ACK, but is not a stability promise:
        // the actual Store SELECT still performs its own CASE/flag checks.
        // Serialized JSON text size matters, not compressed TOAST storage.
        let supported: bool = sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM dispatcharr_channels_stream WHERE stream_hash=ANY($1) AND ((logo_url IS NOT NULL AND octet_length(logo_url)>$2) OR (custom_properties IS NOT NULL AND octet_length(custom_properties::text)>$2)))")
            .bind(&hashes).bind(ScannerM3uLookupCap::FIELD_TEXT_BYTES)
            .fetch_one(self.inner.store.pool()).await.map_err(|_| Error::Operational)?;
        if !supported {
            return Err(Error::Unsupported);
        }
        if kind == Kind::Single && prepared.account.active {
            let settings = self
                .inner
                .auth
                .clone()
                .with_retained_scope(prepared.owner.clone())
                .system_settings()
                .await
                .map_err(|_| Error::Operational)?;
            let limit = audit_limit(&settings)?;
            // Count and tie materialization share one bounded statement snapshot.
            // Even an unqualified concurrent writer cannot grow the GROUP BY input.
            let history = sqlx::query(
                "WITH history AS MATERIALIZED(SELECT timestamp FROM core_systemevent LIMIT 10001) SELECT (SELECT count(*) FROM history)::bigint AS total,EXISTS(SELECT 1 FROM history GROUP BY timestamp HAVING count(*)>1) AS tied",
            )
            .fetch_one(self.inner.store.pool())
            .await
            .map_err(|_| Error::Operational)?;
            let existing: i64 = history.try_get("total").map_err(|_| Error::Operational)?;
            if existing > 10000 {
                return Err(Error::Unsupported);
            }
            if existing >= limit
                && history
                    .try_get::<bool, _>("tied")
                    .map_err(|_| Error::Operational)?
            {
                // Source has no secondary ordering; the captured equal-timestamp
                // observation is not stable native tie authority.
                return Err(Error::Unsupported);
            }
        }
        Ok(prepared)
    }
    async fn hash_setting(&self, owner: Owner) -> Result<HashMode, Error> {
        let cached = self
            .inner
            .ports
            .stream_settings_cached(owner.clone())
            .await
            .map_err(hash_cache_error)?;
        let hash = if let Some(group) = cached {
            // Source returns any cached dict as stored, including an empty dict.
            group.get("m3u_hash_key").cloned().unwrap_or(json!(""))
        } else {
            let before = self
                .inner
                .ports
                .stream_settings_version(owner.clone())
                .await
                .map_err(hash_cache_error)?;
            // Never transfer/parse an unbounded CoreSettings value. Only a
            // bounded projected hash field/default may leave PostgreSQL.
            // This bounds fetched data, not PostgreSQL CPU/TOAST/scan work.
            let row: Option<(bool, Option<Value>)> = sqlx::query_as(
                "SELECT octet_length(value::text)<=65536 AS bounded,CASE WHEN octet_length(value::text)<=65536 THEN CASE WHEN jsonb_typeof(value)='object' THEN COALESCE(value->'m3u_hash_key','\"\"'::jsonb) ELSE '\"\"'::jsonb END ELSE NULL END AS hash_value FROM core_coresettings WHERE key='stream_settings'",
            ).fetch_optional(self.inner.store.pool()).await.map_err(|_|Error::Operational)?;
            let after = self
                .inner
                .ports
                .stream_settings_version(owner)
                .await
                .map_err(hash_cache_error)?;
            if !dispatcharr_store::source_cache_values_equal(&before, &after) {
                // Unlike Source's fill-only version check, admission refuses an
                // observed changing snapshot. Concurrent config parity is open.
                return Err(Error::Operational);
            }
            match row {
                None => json!(""),
                Some((true, Some(value))) => value,
                Some((false, _)) => return Err(Error::Unsupported),
                _ => return Err(Error::Operational),
            }
        };
        match hash {
            Value::Null => Ok(HashMode::Empty),
            Value::String(value) if value.is_empty() => Ok(HashMode::Empty),
            Value::String(value) if value == "url" => Ok(HashMode::Url),
            _ => Err(Error::Unsupported),
        }
    }
    fn cache_path(&self, id: i64) -> PathBuf {
        self.inner.roots.cache.join(format!("{id}.json"))
    }
    async fn settle(&self) -> Result<(), Error> {
        let pool = self.inner.store.pool();
        loop {
            match pool.acquire().await {
                Ok(mut connection) => {
                    let ready = connection.ping().await;
                    let closed = connection.close().await;
                    return if ready.is_ok() && closed.is_ok() {
                        Ok(())
                    } else {
                        Err(Error::Operational)
                    };
                }
                Err(sqlx::Error::PoolClosed) => {
                    pool.close().await;
                    return Err(Error::Operational);
                }
                Err(_) => {
                    // SQLx return_to_pool retains the max1 connection's permit
                    // while ping/rollback consumes trailing ReadyForQuery. A
                    // logical acquire timeout must not release our admission.
                    // No application statement is retried here. Once size0,
                    // SQLx's close/decrement guard proves no pool socket remains.
                    if pool.size() == 0 {
                        return Err(Error::Operational);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }
    async fn lease(&self, kind: Kind, id: i64, owner: Owner) -> Result<Option<TaskLease>, Error> {
        let key = match kind {
            Kind::Groups => "refresh_m3u_account_groups",
            Kind::Single => "refresh_single_m3u_account",
        };
        match self
            .inner
            .locks
            .acquire_retained_detailed(key, id, owner)
            .await
        {
            Ok(v) => Ok(Some(v)),
            Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => Ok(None),
            Err(_) => Err(Error::Operational),
        }
    }
    async fn release(&self, lease: TaskLease) -> Result<(), Error> {
        self.settle().await?;
        match lease.release_owned_detailed().await {
            Ok(true) => Ok(()),
            _ => Err(Error::Operational),
        }
    }
    async fn update(
        &self,
        account: &Account,
        status: &str,
        message: Option<&str>,
        timestamp: bool,
    ) -> Result<(), Error> {
        let changed = if timestamp {
            sqlx::query(
                "UPDATE m3u_m3uaccount SET status=$2,last_message=$3,updated_at=$4 WHERE id=$1",
            )
            .bind(account.id)
            .bind(status)
            .bind(message)
            .bind(self.now())
            .execute(self.inner.store.pool())
            .await
        } else {
            sqlx::query("UPDATE m3u_m3uaccount SET status=$2,last_message=$3 WHERE id=$1")
                .bind(account.id)
                .bind(status)
                .bind(message)
                .execute(self.inner.store.pool())
                .await
        }
        .map_err(|_| Error::Operational)?;
        if changed.rows_affected() != 1 {
            return Err(Error::Operational);
        }
        self.settle().await
    }
    #[allow(clippy::too_many_arguments)]
    async fn event(
        &self,
        id: i64,
        action: &str,
        progress: i64,
        status: &str,
        message: Option<&str>,
        extra: Value,
        owner: Owner,
    ) -> Result<(), Error> {
        let mut data = json!({"progress":progress,"type":"m3u_refresh","account":id,"action":action,"status":status});
        if let Some(v) = message.filter(|v| !v.is_empty()) {
            data["message"] = json!(v);
        }
        for (k, v) in extra.as_object().ok_or(Error::Operational)? {
            data[k] = v.clone();
        }
        self.inner
            .sink
            .send_retained("updates", json!({"type":"update","data":data}), owner)
            .await
            .map_err(|_| Error::Operational)
    }
    async fn settled_audit_failure(
        &self,
        stage: AuditStage,
        source: sqlx::Error,
    ) -> Result<SettledAuditFailure, Error> {
        // Retain the actual cause and outer operation/caller through trailing
        // ReadyForQuery, pool rollback/ping and explicit physical close.
        self.settle().await?;
        Ok(SettledAuditFailure { stage, source })
    }
    async fn audit(&self, kind: &str, details: Value, owner: Owner) -> Result<(), Error> {
        let settings = self
            .inner
            .auth
            .clone()
            .with_retained_scope(owner)
            .system_settings()
            .await
            .map_err(|_| Error::Operational)?;
        let max = audit_limit(&settings)?;
        let inserted = sqlx::query("INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details)VALUES($1,$3,NULL,NULL,$2)")
            .bind(kind).bind(details).bind(self.now()).execute(self.inner.store.pool()).await;
        if let Err(source) = inserted {
            return if self
                .settled_audit_failure(AuditStage::Insert, source)
                .await?
                .caught()
            {
                Ok(())
            } else {
                Err(Error::Operational)
            };
        }
        // The server may read at most10002 rows into this materialization.
        // Count/ties/cutoff use that same snapshot, so a separate check cannot
        // race an unbounded GROUP BY or ORDER BY. Concurrency parity stays open.
        let history = sqlx::query(
            "WITH history AS MATERIALIZED(SELECT id,timestamp FROM core_systemevent LIMIT 10002) SELECT (SELECT count(*) FROM history)::bigint AS total,EXISTS(SELECT 1 FROM history GROUP BY timestamp HAVING count(*)>1) AS tied,(SELECT id FROM history ORDER BY timestamp DESC LIMIT 1 OFFSET $1) AS cutoff",
        )
        .bind(max)
        .fetch_one(self.inner.store.pool())
        .await
        .map_err(|_| Error::Operational)?;
        let count: i64 = history.try_get("total").map_err(|_| Error::Operational)?;
        // At most10000 pre-admitted rows plus this owned insertion. An observed
        // crossing of that bound remains operational, never guessed normal.
        if count > 10001 {
            return Err(Error::Capacity);
        }
        if count > max {
            if history
                .try_get::<bool, _>("tied")
                .map_err(|_| Error::Operational)?
            {
                return Err(Error::Unsupported);
            }
            // Exact Source ordering/cutoff: no id tie-break, no OFFSET max-1,
            // and DELETE id<cutoff can remove zero rows or keep max+1 rows.
            let cutoff = history
                .try_get::<Option<i64>, _>("cutoff")
                .map_err(|_| Error::Operational)?
                .ok_or(Error::Operational)?;
            let removed = sqlx::query("DELETE FROM core_systemevent WHERE id<$1")
                .bind(cutoff)
                .execute(self.inner.store.pool())
                .await;
            if let Err(source) = removed {
                return if self
                    .settled_audit_failure(AuditStage::Delete, source)
                    .await?
                    .caught()
                {
                    Ok(())
                } else {
                    Err(Error::Operational)
                };
            }
        }
        self.settle().await
    }
    async fn error_status(
        &self,
        account: &Account,
        message: &str,
        owner: Owner,
    ) -> Result<(), Error> {
        self.update(account, "error", Some(message), false).await?;
        self.event(
            account.id,
            "parsing",
            100,
            "error",
            Some(message),
            json!({"error":message}),
            owner.clone(),
        )
        .await?;
        self.audit(
            "m3u_error",
            json!({"account_name":account.name,"message":message}),
            owner,
        )
        .await
    }
    async fn groups(
        &self,
        prepared: &Prepared,
        full: bool,
        status: &str,
        message: Option<&str>,
    ) -> Result<Terminal, Error> {
        let account = &prepared.account;
        let owner: Owner = prepared.owner.clone();
        let Some(lease) = self.lease(Kind::Groups, account.id, owner.clone()).await? else {
            return Ok(Terminal::Normal(json!([
                format!("Task already running for account_id={}.", account.id),
                null
            ])));
        };
        let body: Result<Value, GroupsError> = async {
            if !account.active {
                return Ok(json!([
                    format!("M3UAccount with ID={} not found or inactive.", account.id),
                    null
                ]));
            }
            if prepared.missing_input {
                let path = account.path.clone();
                let retained = owner.clone();
                let missing = tokio::task::spawn_blocking(move || {
                    let _retained = retained;
                    files::confirm_missing_open(&path)
                })
                .await
                .map_err(|_| Error::Operational)??;
                return Err(GroupsError::Missing(missing));
            }
            let catalog = prepared.input.as_ref().ok_or(Error::Operational)?;
            let raw = catalog.raw.clone();
            let path = self.cache_path(account.id);
            let retained = owner.clone();
            tokio::task::spawn_blocking(move || {
                let _retained = retained;
                files::store(path, &raw)
            })
            .await
            .map_err(|_| Error::Operational)??;
            self.event(
                account.id,
                "processing_groups",
                0,
                status,
                message,
                json!({}),
                owner.clone(),
            )
            .await?;
            let groups = catalog
                .groups
                .iter()
                .map(|name| ImportedGroup {
                    name: name.clone(),
                    custom_properties: serde_json::Map::new(),
                })
                .collect();
            self.inner
                .store
                .import_m3u_groups_report(account.id, &json!({}), groups, self.now())
                .await
                .map_err(GroupsError::Import)?;
            Ok(files::result(catalog))
        }
        .await;
        let result = match body {
            Ok(value) => {
                self.release(lease).await?;
                value
            }
            Err(GroupsError::Operational(error)) => {
                self.release(lease).await?;
                return Err(error);
            }
            Err(GroupsError::Missing(error)) => {
                self.settle().await?;
                let failure = KnownFailure::missing(&error)?;
                if !lease
                    .preserve_failed_m3u_groups_until_expiry()
                    .await
                    .map_err(|_| Error::Operational)?
                {
                    return Err(Error::Operational);
                }
                return Ok(Terminal::Raised(failure));
            }
            Err(GroupsError::Import(error)) => {
                // Classify only after the failed operation's actual Ready/close.
                self.settle().await?;
                let failure = match KnownFailure::group_create(&error) {
                    Ok(failure) => failure,
                    Err(error) => {
                        self.release(lease).await?;
                        return Err(error);
                    }
                };
                if !lease
                    .preserve_failed_m3u_groups_until_expiry()
                    .await
                    .map_err(|_| Error::Operational)?
                {
                    return Err(Error::Operational);
                }
                return Ok(Terminal::Raised(failure));
            }
        };
        if account.active && !full {
            let text = "M3U groups loaded. Please select groups or refresh M3U to complete setup.";
            self.update(account, "pending_setup", Some(text), false)
                .await?;
            self.event(
                account.id,
                "processing_groups",
                100,
                "pending_setup",
                Some(text),
                json!({}),
                owner,
            )
            .await?;
        }
        Ok(Terminal::Normal(result))
    }
    pub(crate) async fn execute_terminal(
        &self,
        kind: Kind,
        prepared: Prepared,
    ) -> Result<Terminal, Error> {
        let result = if kind == Kind::Groups {
            self.groups(
                &prepared,
                false,
                &prepared.account.status,
                prepared.account.message.as_deref(),
            )
            .await
        } else {
            self.single(&prepared).await.map(Terminal::Normal)
        };
        // Also fence raw SQL on every execution error (including a late
        // pending-setup/audit ErrorResponse) before waiting retained receipts.
        let result = match self.settle().await {
            Ok(()) => result,
            Err(error) => Err(error),
        };
        // Every retained cache/sink/lease operation owns Operation. Drop our last
        // local Arc, then wait for its physical-drop receipt before task metadata.
        let Prepared { owner, done, .. } = prepared;
        drop(owner);
        tokio::time::timeout(Duration::from_secs(5), done)
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Operational)?;
        result
    }
    async fn single(&self, prepared: &Prepared) -> Result<Value, Error> {
        let owner: Owner = prepared.owner.clone();
        let account = &prepared.account;
        let Some(lease) = self.lease(Kind::Single, account.id, owner).await? else {
            return Ok(json!(format!(
                "Task already running for account_id={}.",
                account.id
            )));
        };
        let result = self.single_body(prepared).await;
        let released = self.release(lease).await;
        released?;
        result
    }
    async fn single_body(&self, p: &Prepared) -> Result<Value, Error> {
        let account = &p.account;
        let owner: Owner = p.owner.clone();
        let start = self.now();
        let elapsed_start = self.inner.elapsed.now();
        self.update(account, "fetching", Some("Refresh in progress..."), false)
            .await?;
        let cached = p.cached.as_ref().filter(|v| !v.entries.is_empty());
        let catalog = if let Some(cached) = cached {
            cached
        } else {
            let groups = match self
                .groups(p, true, "fetching", Some("Refresh in progress..."))
                .await?
            {
                Terminal::Normal(value) => value,
                Terminal::Raised(_) => return Err(Error::Operational),
            };
            if groups[1].is_null() {
                let message = groups[0]
                    .as_str()
                    .unwrap_or("Failed to refresh M3U groups - download failed or other error");
                self.error_status(account, message, owner).await?;
                return Ok(json!(
                    "Failed to update m3u account - download failed or other error"
                ));
            }
            let parsed = p.input.as_ref().ok_or(Error::Operational)?;
            if parsed.entries.is_empty() {
                self.error_status(account, "No streams found in M3U source", owner)
                    .await?;
                return Ok(json!("Failed to update m3u account, no streams found"));
            }
            parsed
        };
        self.update(account, "parsing", Some("Refresh in progress..."), false)
            .await?;
        let rows=sqlx::query("SELECT g.id::bigint,g.name FROM dispatcharr_channels_channelgroup g JOIN dispatcharr_channels_channelgroupm3uaccount r ON r.channel_group_id=g.id WHERE r.m3u_account_id=$1 AND r.enabled LIMIT 17").bind(account.id).fetch_all(self.inner.store.pool()).await.map_err(|_|Error::Operational)?;
        if rows.len() > 16 {
            return Err(Error::Capacity);
        }
        let groups = rows
            .into_iter()
            .map(|r| Ok((r.try_get::<String, _>("name")?, r.try_get::<i64, _>("id")?)))
            .collect::<Result<BTreeMap<_, _>, sqlx::Error>>()
            .map_err(|_| Error::Operational)?;
        let context = ProjectionContext {
            account_id: account.id,
            account_type: "STD",
            groups: &groups,
            hash_keys: p.hash_mode.keys(),
            max_name_characters: 512,
        };
        let projected = catalog
            .entries
            .iter()
            .map(|e| project_entry(e, &context))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| Error::Unsupported)?
            .into_iter()
            .flatten()
            .map(ImportedStream::from)
            .collect::<Vec<_>>();
        let body = self
            .process(account, projected, start, elapsed_start, owner.clone())
            .await;
        let path = self.cache_path(account.id);
        let retained = owner;
        let removed = tokio::task::spawn_blocking(move || {
            let _retained = retained;
            files::remove(&path)
        })
        .await
        .map_err(|_| Error::Operational)?;
        removed?;
        body
    }
    async fn process(
        &self,
        account: &Account,
        rows: Vec<ImportedStream>,
        start: chrono::DateTime<chrono::Utc>,
        elapsed_start: f64,
        owner: Owner,
    ) -> Result<Value, Error> {
        let first = self
            .inner
            .store
            .import_m3u_batch_with_lookup_cap(
                account.id,
                rows.clone(),
                ScannerM3uLookupCap::local_std_file8(),
            )
            .await;
        let counts = match first {
            Ok(v) => v,
            Err(e) => {
                self.settle().await?;
                if !caught_batch(&e) {
                    return Err(Error::Operational);
                }
                match self
                    .inner
                    .store
                    .import_m3u_batch_with_lookup_cap(
                        account.id,
                        rows,
                        ScannerM3uLookupCap::local_std_file8(),
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        self.settle().await?;
                        if !caught_batch(&e) {
                            return Err(Error::Operational);
                        }
                        self.error_status(
                            account,
                            "Refresh incomplete: 1 batch(es) failed to process.",
                            owner.clone(),
                        )
                        .await?;
                        self.inner
                            .ports
                            .invalidate(owner)
                            .await
                            .map_err(|_| Error::Operational)?;
                        return Ok(json!(
                            "Failed to update m3u account, one or more batches failed to process"
                        ));
                    }
                }
            }
        };
        self.settle().await?;
        let progress_elapsed = self.inner.elapsed.now() - elapsed_start;
        if !progress_elapsed.is_finite() || !(0.0..=3600.0).contains(&progress_elapsed) {
            return Err(Error::Operational);
        }
        let processed = counts.created + counts.updated + counts.unchanged;
        let rounded_progress = format!("{progress_elapsed:.2}")
            .parse::<f64>()
            .map_err(|_| Error::Operational)?;
        self.event(account.id,"parsing",100,"parsing",Some("Refresh in progress..."),json!({"elapsed_time":rounded_progress,"time_remaining":0,"streams_processed":processed}),owner.clone()).await?;
        let stale=sqlx::query("UPDATE dispatcharr_channels_stream SET is_stale=true WHERE m3u_account_id=$1 AND last_seen<$2").bind(account.id).bind(start).execute(self.inner.store.pool()).await.map_err(|_|Error::Operational)?.rows_affected();
        sqlx::query("UPDATE dispatcharr_channels_channelgroupm3uaccount SET is_stale=true WHERE m3u_account_id=$1 AND last_seen<$2").bind(account.id).bind(start).execute(self.inner.store.pool()).await.map_err(|_|Error::Operational)?;
        let deleted = self
            .inner
            .store
            .cleanup_m3u_streams(account.id, start)
            .await
            .map_err(|_| Error::Operational)?
            .reported_total;
        self.inner
            .store
            .cleanup_m3u_group_relationships(
                &GroupCleanupAccount {
                    id: account.id,
                    locked: account.locked,
                    name: account.name.clone(),
                    stale_stream_days: account.days,
                },
                start,
            )
            .await
            .map_err(|_| Error::Operational)?;
        let elapsed = self.inner.elapsed.now() - elapsed_start;
        if !elapsed.is_finite() || !(0.0..=3600.0).contains(&elapsed) {
            return Err(Error::Operational);
        }
        let text=format!("Processing completed in {elapsed:.1} seconds. Streams: {} created, {} updated, {stale} marked stale, {deleted} removed. Total processed: {processed}.",counts.created,counts.updated);
        self.update(account, "success", Some(&text), true).await?;
        self.inner
            .ports
            .invalidate(owner.clone())
            .await
            .map_err(|_| Error::Operational)?;
        self.audit("m3u_refresh",json!({"account_name":account.name,"elapsed_time":format!("{elapsed:.2}").parse::<f64>().map_err(|_|Error::Operational)?,"streams_created":counts.created,"streams_updated":counts.updated,"streams_stale":stale,"streams_deleted":deleted,"total_processed":processed}),owner.clone()).await?;
        self.event(account.id,"parsing",100,"success",Some(&text),json!({"elapsed_time":elapsed,"time_remaining":0,"streams_processed":processed,"streams_created":counts.created,"streams_updated":counts.updated,"streams_stale":stale,"streams_deleted":deleted,"channels_created":0,"channels_updated":0,"channels_deleted":0,"channels_failed":0,"failed_stream_details":[]}),owner).await?;
        Ok(json!("Dispatched jobs complete."))
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let _slot = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
fn hash_cache_error(error: crate::transport::Error) -> Error {
    match error {
        crate::transport::Error::Capacity => Error::Unsupported,
        _ => Error::Operational,
    }
}
fn caught_batch(error: &BoundedM3uBatchError) -> bool {
    let BoundedM3uBatchError::MutationRolledBack {
        source: sqlx::Error::Database(error),
        ..
    } = error
    else {
        return false;
    };
    error.code().as_deref() == Some("P0001")
        && error
            .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            .is_some_and(|v| v.severity() == sqlx::postgres::PgSeverity::Error)
}

fn audit_limit(settings: &Value) -> Result<i64, Error> {
    if !settings.is_object() {
        return Err(Error::Unsupported);
    }
    match settings.get("max_system_events") {
        None | Some(Value::Null) => Ok(100),
        Some(Value::Number(v)) => match v.as_i64() {
            Some(0) => Ok(100),
            Some(v) if (1..=10000).contains(&v) => Ok(v),
            _ => Err(Error::Unsupported),
        },
        _ => Err(Error::Unsupported),
    }
}
#[cfg(test)]
mod boundary_tests {
    use super::*;
    #[test]
    fn audit_limit_has_no_unsupported_configuration_fallback() {
        for v in [
            json!({}),
            json!({"max_system_events":null}),
            json!({"max_system_events":0}),
            json!({"max_system_events":100}),
        ] {
            assert_eq!(audit_limit(&v), Ok(100));
        }
        assert_eq!(audit_limit(&json!({"max_system_events":1})), Ok(1));
        for v in [
            json!({"max_system_events":"1"}),
            json!({"max_system_events":-1}),
            json!({"max_system_events":10001}),
            json!({"max_system_events":1.0}),
            json!({"max_system_events":true}),
            json!([]),
        ] {
            assert_eq!(audit_limit(&v), Err(Error::Unsupported));
        }
    }
}

#[cfg(test)]
mod known_failure_tests {
    use super::*;
    fn message(metadata: &Value) -> crate::scanner_m3u_protocol::Message {
        let parent = uuid::Uuid::parse_str(metadata["parent_id"].as_str().unwrap()).unwrap();
        crate::scanner_m3u_protocol::Message {
            id: uuid::Uuid::parse_str(metadata["task_id"].as_str().unwrap()).unwrap(),
            root: parent,
            parent,
            account: 1,
            kind: Kind::Groups,
        }
    }
    #[test]
    fn actual_two_source_failure_descriptors_and_owned_metadata_without_python_frames() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-m3u-known-failures.json"))
                .unwrap();
        for row in source["cases"].as_array().unwrap() {
            let original = &row["metadata"];
            let failure = if row["name"] == "m3u-missing-local-file" {
                KnownFailure::missing(&std::io::Error::from_raw_os_error(2)).unwrap()
            } else {
                KnownFailure::programming(
                    M3uGroupImportStage::CreateGroups,
                    sqlx::postgres::PgSeverity::Error,
                    crate::pg_failure_format::Fields {
                        sqlstate: "P0001",
                        message: "rust-owned M3U worker failure",
                        detail: None,
                        hint: None,
                        context: Some(
                            "PL/pgSQL function rust_owned_m3u_worker_fail() line 1 at RAISE",
                        ),
                        has_position: false,
                    },
                )
                .unwrap()
            };
            let raw = failure
                .metadata(&message(original), original["date_done"].as_str().unwrap())
                .unwrap();
            let actual: Value = serde_json::from_slice(&raw).unwrap();
            for key in [
                "status",
                "result",
                "task_id",
                "parent_id",
                "children",
                "date_done",
            ] {
                assert_eq!(actual.get(key), original.get(key), "{}:{key}", row["name"]);
            }
            assert!(actual["traceback"]
                .as_str()
                .unwrap()
                .starts_with("Native Dispatcharr M3U groups diagnostic"));
            assert!(!actual["traceback"]
                .as_str()
                .unwrap()
                .contains("Traceback (most recent call last)"));
            assert_ne!(actual["traceback"], original["traceback"]);
            assert_eq!(
                crate::decode::decode(&raw, message(original).id)
                    .unwrap()
                    .state,
                crate::decode::State::Failure
            );
        }
    }
    #[test]
    fn unrelated_fs_error_or_sql_stage_severity_code_position_never_becomes_known_failure() {
        for code in [1, 5, 13, 20, 21, 28] {
            assert!(KnownFailure::missing(&std::io::Error::from_raw_os_error(code)).is_err());
        }
        let fields = crate::pg_failure_format::Fields {
            sqlstate: "P0001",
            message: "owned",
            detail: None,
            hint: None,
            context: None,
            has_position: false,
        };
        for stage in [
            M3uGroupImportStage::Validation,
            M3uGroupImportStage::ReadGroups,
            M3uGroupImportStage::ReadCreatedGroups,
            M3uGroupImportStage::ReadRelationBytes,
            M3uGroupImportStage::ReadRelations,
            M3uGroupImportStage::CreateRelations,
            M3uGroupImportStage::UpdateRelations,
            M3uGroupImportStage::Decode,
        ] {
            assert!(
                KnownFailure::programming(stage, sqlx::postgres::PgSeverity::Error, fields)
                    .is_err()
            );
        }
        for severity in [
            sqlx::postgres::PgSeverity::Fatal,
            sqlx::postgres::PgSeverity::Panic,
        ] {
            assert!(
                KnownFailure::programming(M3uGroupImportStage::CreateGroups, severity, fields)
                    .is_err()
            );
        }
        for code in ["P0002", "23505", "08006"] {
            assert!(KnownFailure::programming(
                M3uGroupImportStage::CreateGroups,
                sqlx::postgres::PgSeverity::Error,
                crate::pg_failure_format::Fields {
                    sqlstate: code,
                    ..fields
                }
            )
            .is_err());
        }
        assert!(KnownFailure::programming(
            M3uGroupImportStage::CreateGroups,
            sqlx::postgres::PgSeverity::Error,
            crate::pg_failure_format::Fields {
                has_position: true,
                ..fields
            }
        )
        .is_err());
        let large = "x".repeat(8193);
        assert!(KnownFailure::programming(
            M3uGroupImportStage::CreateGroups,
            sqlx::postgres::PgSeverity::Error,
            crate::pg_failure_format::Fields {
                message: &large,
                ..fields
            }
        )
        .is_err());
    }
    #[test]
    fn create_stage_alone_does_not_classify_io_protocol_or_input_failure() {
        for source in [
            StoreError::Database(sqlx::Error::Io(std::io::Error::from_raw_os_error(5))),
            StoreError::Database(sqlx::Error::Protocol("owned".into())),
            StoreError::InvalidInput(json!({"detail":"owned"})),
        ] {
            let report = M3uGroupImportFailure {
                stage: M3uGroupImportStage::CreateGroups,
                source,
            };
            assert!(KnownFailure::group_create(&report).is_err());
        }
    }
    #[test]
    fn invalid_date_or_oversized_diagnostic_cannot_reach_writer() {
        let id = uuid::Uuid::new_v4();
        let metadata = json!({"parent_id":id,"task_id":uuid::Uuid::new_v4()});
        let mut failure = KnownFailure::missing(&std::io::Error::from_raw_os_error(2)).unwrap();
        assert!(failure.metadata(&message(&metadata), "not-a-date").is_err());
        failure.diagnostic = "x".repeat(16385);
        assert!(failure
            .metadata(&message(&metadata), "2026-11-12T05:00:00.000000+00:00")
            .is_err());
    }
}
