//! Bounded database scheduler setup/reload component derived from pinned Source.
//! Original Dispatcharr and django-celery-beat/Celery authors retain credit.
//! Known zero/one-interval tick/publication/owned timer constructors are explicit.
//! No general heap/calendar, default worker intake or application/main hook.
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Mutex, Semaphore};
#[derive(Debug)]
pub enum Error {
    Closed,
    Capacity,
    Database(sqlx::Error),
    Join,
    Unsupported,
}
impl From<sqlx::Error> for Error {
    fn from(v: sqlx::Error) -> Self {
        Self::Database(v)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Entry {
    pub id: i64,
    pub name: String,
    pub task: String,
    pub enabled: bool,
    pub args: Value,
    pub kwargs: Value,
    pub last_run_at: DateTime<Utc>,
    pub start_time: Option<DateTime<Utc>>,
    pub expires: Option<DateTime<Utc>>,
    pub one_off: bool,
    pub headers: Value,
    pub queue: Option<String>,
    pub exchange: Option<String>,
    pub routing_key: Option<String>,
    pub priority: Option<i32>,
    pub expire_seconds: Option<i32>,
    pub total_run_count: i64,
    pub interval: Option<(i32, String)>,
    pub crontab: Option<(String, String, String, String, String, String)>,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<String, Entry>,
    initial: bool,
    last_marker: Option<DateTime<Utc>>,
    last_full: Option<DateTime<Utc>>,
    dirty: HashMap<String, (i64, DateTime<Utc>, i64)>,
    load_order: Vec<String>,
    heap: Vec<heap::Event>,
    last_sync: Option<std::time::Instant>,
    heap_initialized: bool,
}
struct Inner {
    pool: PgPool,
    slots: Arc<Semaphore>,
    closed: AtomicBool,
    state: Mutex<State>,
    heap_clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
}
#[derive(Clone)]
pub struct DatabaseSchedule {
    inner: Arc<Inner>,
}
#[derive(Debug)]
pub struct Setup {
    pub installed: Vec<String>,
    pub failures: Vec<(String, Option<String>)>,
}
impl DatabaseSchedule {
    pub fn new(pool: PgPool) -> Self {
        Self::new_with_heap_clock(pool, Arc::new(Utc::now))
    }
    fn new_with_heap_clock(
        pool: PgPool,
        heap_clock: Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                slots: Arc::new(Semaphore::new(2)),
                closed: AtomicBool::new(false),
                state: Mutex::new(State::default()),
                heap_clock,
            }),
        }
    }
    async fn owned<T, F, Fut>(&self, caller: Arc<dyn Send + Sync>, job: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn Send + Sync>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, Error>> + Send + 'static,
    {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        // The per-job admission and inner state travel through actual physical
        // broker completion even if that command returns a logical deadline.
        let retained: Arc<dyn Send + Sync> = Arc::new((caller, permit, self.inner.clone()));
        tokio::spawn(async move {
            let _scope = retained.clone();
            job(retained).await
        })
        .await
        .map_err(|_| Error::Join)?
    }
    /// Setup performs Source per-entry commits and catches individual server DB
    /// errors. It does not publish or replace existing enabled/count/one-off state.
    pub async fn setup(&self, caller: Arc<dyn Send + Sync>) -> Result<Setup, Error> {
        let this = self.clone();
        self.owned(caller, move |_retained| async move {
            let specs = [
                ("celery.backend_cleanup", "celery.backend_cleanup", None),
                (
                    "fetch-channel-statuses",
                    "apps.proxy.tasks.fetch_channel_stats",
                    Some(2),
                ),
                ("scan-files", "core.tasks.scan_and_process_files", Some(20)),
                (
                    "maintain-recurring-recordings",
                    "apps.channels.tasks.maintain_recurring_recordings",
                    Some(3600),
                ),
                (
                    "check-version-updates",
                    "core.tasks.check_for_version_update",
                    Some(86400),
                ),
                (
                    "check-account-expirations",
                    "apps.m3u.tasks.check_account_expirations",
                    Some(86400),
                ),
            ];
            let mut out = Setup {
                installed: vec![],
                failures: vec![],
            };
            for (name, task, every) in specs {
                match this.install(name, task, every).await {
                    Ok(()) => out.installed.push(name.to_owned()),
                    Err(Error::Database(sqlx::Error::Database(error))) => out
                        .failures
                        .push((name.to_owned(), error.code().map(|v| v.into_owned()))),
                    Err(error) => return Err(error),
                }
            }
            // Source setup reads the schedule and establishes the change timestamp.
            this.refresh_owned(Utc::now()).await?;
            this.refresh_owned(Utc::now()).await?;
            Ok(out)
        })
        .await
    }
    async fn marker(&self) -> Result<Option<DateTime<Utc>>, Error> {
        Ok(sqlx::query_scalar(
            "SELECT last_update FROM django_celery_beat_periodictasks WHERE ident=1",
        )
        .fetch_optional(&self.inner.pool)
        .await?)
    }
    async fn install(&self, name: &str, task: &str, every: Option<i32>) -> Result<(), Error> {
        let (mut interval, mut cron) = (None, None);
        if let Some(every) = every {
            let id:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_intervalschedule WHERE every=$1 AND period='seconds' ORDER BY id LIMIT 1").bind(every).fetch_optional(&self.inner.pool).await?;
            interval = Some(if let Some(id) = id {
                sqlx::query("UPDATE django_celery_beat_intervalschedule SET every=$2,period='seconds' WHERE id=$1").bind(id).bind(every).execute(&self.inner.pool).await?;
                id
            } else {
                sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES($1,'seconds') RETURNING id::bigint").bind(every).fetch_one(&self.inner.pool).await?
            });
        } else {
            let id:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_crontabschedule WHERE minute='0' AND hour='4' AND day_of_week='*' AND day_of_month='*' AND month_of_year='*' AND timezone='UTC' ORDER BY id LIMIT 1").fetch_optional(&self.inner.pool).await?;
            cron = Some(if let Some(id) = id {
                id
            } else {
                sqlx::query_scalar("INSERT INTO django_celery_beat_crontabschedule(minute,hour,day_of_week,day_of_month,month_of_year,timezone) VALUES('0','4','*','*','*','UTC') RETURNING id::bigint").fetch_one(&self.inner.pool).await?
            });
        }
        // Model schedule save/change signal commits before PeriodicTask.from_entry.
        sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&self.inner.pool).await?;
        let mut tx = self.inner.pool.begin().await?;
        let result=sqlx::query("INSERT INTO django_celery_beat_periodictask(name,task,interval_id,crontab_id,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off,expire_seconds) VALUES($1,$2,$3,$4,'[]','{}','{}',$5,0,$6,'',false,$7) ON CONFLICT(name) DO UPDATE SET task=EXCLUDED.task,interval_id=EXCLUDED.interval_id,crontab_id=EXCLUDED.crontab_id,solar_id=NULL,clocked_id=NULL,args='[]',kwargs='{}',headers='{}',queue=NULL,exchange=NULL,routing_key=NULL,priority=NULL,expire_seconds=EXCLUDED.expire_seconds,enabled=CASE WHEN EXCLUDED.name='fetch-channel-statuses' THEN false ELSE django_celery_beat_periodictask.enabled END,last_run_at=CASE WHEN NOT django_celery_beat_periodictask.enabled OR EXCLUDED.name='fetch-channel-statuses' THEN NULL ELSE django_celery_beat_periodictask.last_run_at END,date_changed=EXCLUDED.date_changed")
   .bind(name).bind(task).bind(interval).bind(cron).bind(name!="fetch-channel-statuses").bind(Utc::now()).bind(if every.is_none(){Some(43200i32)}else{None}).execute(&mut *tx).await;
        if let Err(error) = result {
            tx.rollback().await?;
            return Err(Error::Database(error));
        }
        let result=sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&mut *tx).await;
        if let Err(error) = result {
            tx.rollback().await?;
            return Err(Error::Database(error));
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn refresh(
        &self,
        now: DateTime<Utc>,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Vec<Entry>, Error> {
        let this = self.clone();
        self.owned(caller, move |_retained| async move {
            this.refresh_owned(now).await
        })
        .await
    }
    async fn refresh_owned(&self, now: DateTime<Utc>) -> Result<Vec<Entry>, Error> {
        let mut state = self.inner.state.lock().await;
        self.refresh_locked(&mut state, now).await
    }
    async fn refresh_locked(
        &self,
        state: &mut State,
        now: DateTime<Utc>,
    ) -> Result<Vec<Entry>, Error> {
        let initial = !state.initial;
        let mut reload = initial;
        if initial {
            state.initial = true;
        } else {
            let stamp = self.marker().await?;
            if let (Some(last), Some(current)) = (state.last_marker, stamp) {
                reload = current > last;
            }
            state.last_marker = stamp;
        }
        if state
            .last_full
            .is_none_or(|last| (now - last).num_seconds() >= 300)
        {
            reload = true;
        }
        if reload {
            let _ = self.sync_locked(state).await?;
            let (entries, order) = self.load(now).await?;
            state.entries = entries;
            state.load_order = order;
            state.heap.clear();
            state.heap_initialized = false;
            state.last_full = Some(now);
        }
        Ok(state
            .load_order
            .iter()
            .filter_map(|name| state.entries.get(name).cloned())
            .collect())
    }
    async fn load(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(BTreeMap<String, Entry>, Vec<String>), Error> {
        // Return bounded rows or one metadata-only capacity sentinel. CASE
        // guards prevent oversized text materialization; the same MVCC
        // statement computes count/aggregate before transfer. Correlated PK
        // lookups preserve the parent scan order, like Source's queryset plus
        // lazy schedule FKs; hash-right joins may reverse parent chains.
        let rows=sqlx::query("WITH candidates AS MATERIALIZED (SELECT p.id::bigint,p.enabled,p.last_run_at,p.date_changed,p.start_time,p.expires,p.one_off,p.priority::integer,p.expire_seconds,p.total_run_count::bigint,i.every,cl.clocked_time,CASE WHEN COALESCE(octet_length(p.queue),0)<=4096 THEN p.queue END AS queue,CASE WHEN COALESCE(octet_length(p.exchange),0)<=4096 THEN p.exchange END AS exchange,CASE WHEN COALESCE(octet_length(p.routing_key),0)<=4096 THEN p.routing_key END AS routing_key,CASE WHEN COALESCE(octet_length(p.name),0)<=4096 THEN p.name END AS name,CASE WHEN COALESCE(octet_length(p.task),0)<=4096 THEN p.task END AS task,CASE WHEN COALESCE(octet_length(p.args),0)<=65536 THEN p.args END AS args,CASE WHEN COALESCE(octet_length(p.kwargs),0)<=65536 THEN p.kwargs END AS kwargs,CASE WHEN COALESCE(octet_length(p.headers),0)<=65536 THEN p.headers END AS headers,CASE WHEN COALESCE(octet_length(i.period),0)<=4096 THEN i.period END AS period,CASE WHEN COALESCE(octet_length(c.minute),0)<=4096 THEN c.minute END AS minute,CASE WHEN COALESCE(octet_length(c.hour),0)<=4096 THEN c.hour END AS hour,CASE WHEN COALESCE(octet_length(c.day_of_week),0)<=4096 THEN c.day_of_week END AS day_of_week,CASE WHEN COALESCE(octet_length(c.day_of_month),0)<=4096 THEN c.day_of_month END AS day_of_month,CASE WHEN COALESCE(octet_length(c.month_of_year),0)<=4096 THEN c.month_of_year END AS month_of_year,CASE WHEN COALESCE(octet_length(c.timezone),0)<=4096 THEN c.timezone END AS timezone,(COALESCE(octet_length(p.queue),0)<=4096 AND COALESCE(octet_length(p.exchange),0)<=4096 AND COALESCE(octet_length(p.routing_key),0)<=4096 AND COALESCE(octet_length(p.name),0)<=4096 AND COALESCE(octet_length(p.task),0)<=4096 AND COALESCE(octet_length(p.args),0)<=65536 AND COALESCE(octet_length(p.kwargs),0)<=65536 AND COALESCE(octet_length(p.headers),0)<=65536 AND COALESCE(octet_length(i.period),0)<=4096 AND COALESCE(octet_length(c.minute),0)<=4096 AND COALESCE(octet_length(c.hour),0)<=4096 AND COALESCE(octet_length(c.day_of_week),0)<=4096 AND COALESCE(octet_length(c.day_of_month),0)<=4096 AND COALESCE(octet_length(c.month_of_year),0)<=4096 AND COALESCE(octet_length(c.timezone),0)<=4096) AS row_fits,(COALESCE(octet_length(p.queue),0)::bigint+COALESCE(octet_length(p.exchange),0)::bigint+COALESCE(octet_length(p.routing_key),0)::bigint+COALESCE(octet_length(p.name),0)::bigint+COALESCE(octet_length(p.task),0)::bigint+COALESCE(octet_length(p.args),0)::bigint+COALESCE(octet_length(p.kwargs),0)::bigint+COALESCE(octet_length(p.headers),0)::bigint+COALESCE(octet_length(i.period),0)::bigint+COALESCE(octet_length(c.minute),0)::bigint+COALESCE(octet_length(c.hour),0)::bigint+COALESCE(octet_length(c.day_of_week),0)::bigint+COALESCE(octet_length(c.day_of_month),0)::bigint+COALESCE(octet_length(c.month_of_year),0)::bigint+COALESCE(octet_length(c.timezone),0)::bigint) AS row_bytes FROM django_celery_beat_periodictask p LEFT JOIN LATERAL (SELECT every,period FROM django_celery_beat_intervalschedule WHERE id=p.interval_id OFFSET 0) i ON true LEFT JOIN LATERAL (SELECT minute,hour,day_of_week,day_of_month,month_of_year,timezone FROM django_celery_beat_crontabschedule WHERE id=p.crontab_id OFFSET 0) c ON true LEFT JOIN LATERAL (SELECT clocked_time FROM django_celery_beat_clockedschedule WHERE id=p.clocked_id OFFSET 0) cl ON true WHERE p.enabled LIMIT 257), budget AS (SELECT count(*)::bigint AS selected_count,COALESCE(sum(row_bytes),0)::bigint AS reply_bytes,COALESCE(bool_and(row_fits),true) AS fits FROM candidates) SELECT c.*,b.selected_count,b.reply_bytes,b.fits FROM budget b LEFT JOIN candidates c ON b.selected_count<=256 AND b.reply_bytes<=1048576 AND b.fits").fetch_all(&self.inner.pool).await?;
        let budget = rows.first().ok_or(Error::Capacity)?;
        let count: i64 = budget.get("selected_count");
        let reply_bytes: i64 = budget.get("reply_bytes");
        if count > 256 || reply_bytes > 1024 * 1024 || !budget.get::<bool, _>("fits") {
            return Err(Error::Capacity);
        }
        let mut map = BTreeMap::new();
        let mut order = Vec::new();
        let mut bytes = 0usize;
        for row in rows {
            if row.get::<Option<i64>, _>("id").is_none() {
                continue;
            }
            let name: String = row.get("name");
            let task: String = row.get("task");
            let args: String = row.get("args");
            let kwargs: String = row.get("kwargs");
            let headers: String = row.get("headers");
            bytes = bytes
                .checked_add(name.len() + task.len() + args.len() + kwargs.len() + headers.len())
                .ok_or(Error::Capacity)?;
            if bytes > 1024 * 1024 {
                return Err(Error::Capacity);
            }
            let clocked: Option<DateTime<Utc>> = row.get("clocked_time");
            if clocked.is_some_and(|v| v > now + chrono::Duration::seconds(300)) {
                continue;
            }
            let hour: Option<String> = row.get("hour");
            let timezone: Option<String> = row.get("timezone");
            if let Some(zone) = timezone.as_deref() {
                if zone != "UTC" {
                    return Err(Error::Unsupported);
                }
            }
            if let Some(hour) = hour.as_deref() {
                let numeric =
                    (0..24).any(|h| hour == h.to_string() || (h < 10 && hour == format!("{h:02}")));
                if numeric {
                    let h: u32 = hour.parse().map_err(|_| Error::Unsupported)?;
                    use chrono::Timelike;
                    let server = now.hour();
                    if h != 4 && !(0..5).any(|offset| (server + 22 + offset) % 24 == h) {
                        continue;
                    }
                }
            }
            let id: i64 = row.get("id");
            let mut enabled = true;
            let mut cached_last: Option<DateTime<Utc>> = row.get("last_run_at");
            let mut changed: DateTime<Utc> = row.get("date_changed");
            let args = serde_json::from_str(if args.is_empty() { "[]" } else { &args }).ok();
            let kwargs = args.as_ref().and_then(|_| {
                serde_json::from_str(if kwargs.is_empty() { "{}" } else { &kwargs }).ok()
            });
            if args.is_none() || kwargs.is_none() {
                changed=sqlx::query_scalar("UPDATE django_celery_beat_periodictask SET enabled=false,last_run_at=NULL,date_changed=$2 WHERE id=$1 RETURNING date_changed").bind(id).bind(Utc::now()).fetch_one(&self.inner.pool).await?;
                cached_last = None;
                enabled = false;
            }
            let parsed_headers: Value =
                match serde_json::from_str(if headers.is_empty() { "{}" } else { &headers }) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
            if !parsed_headers.is_object() {
                return Err(Error::Unsupported);
            }
            let last = cached_last;
            let start: Option<DateTime<Utc>> = row.get("start_time");
            let mut last = last.unwrap_or(changed);
            if cached_last.is_none() && start.is_some() {
                last = last
                    .checked_sub_signed(chrono::Duration::days(365 * 30))
                    .ok_or(Error::Capacity)?;
            }
            let interval = row
                .get::<Option<i32>, _>("every")
                .map(|v| (v, row.get::<Option<String>, _>("period").unwrap()));
            let crontab = hour.map(|h| {
                (
                    row.get("minute"),
                    h,
                    row.get("day_of_week"),
                    row.get("day_of_month"),
                    row.get("month_of_year"),
                    timezone.unwrap(),
                )
            });
            order.push(name.clone());
            map.insert(
                name.clone(),
                Entry {
                    id,
                    name,
                    task,
                    enabled,
                    args: args.unwrap_or(Value::Null),
                    kwargs: kwargs.unwrap_or(Value::Null),
                    last_run_at: last,
                    start_time: start,
                    expires: row.get("expires"),
                    one_off: row.get("one_off"),
                    headers: parsed_headers,
                    queue: row.get("queue"),
                    exchange: row.get("exchange"),
                    routing_key: row.get("routing_key"),
                    priority: row.get("priority"),
                    expire_seconds: row.get("expire_seconds"),
                    total_run_count: row.get("total_run_count"),
                    interval,
                    crontab,
                },
            );
        }
        Ok((map, order))
    }
    pub async fn shutdown(&self, deadline: Duration) -> bool {
        self.inner.closed.store(true, Ordering::Release);
        let this = self.clone();
        tokio::time::timeout(deadline, async move {
            let Ok(permits) = this.inner.slots.clone().acquire_many_owned(2).await else {
                return false;
            };
            // Once final sync starts, caller timeout cannot cancel its actual
            // SQL/rollback ownership. A later shutdown waits the same permits.
            tokio::spawn(async move {
                let _permits = permits;
                let mut state = this.inner.state.lock().await;
                this.sync_locked(&mut state).await.is_ok()
            })
            .await
            .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }
}

#[cfg(test)]
#[path = "database_schedule_peer.rs"]
mod peer;

#[cfg(test)]
#[path = "database_schedule_physical.rs"]
mod physical;

#[path = "database_tick.rs"]
pub mod tick;

#[path = "database_driver.rs"]
pub mod driver;

#[path = "database_heap.rs"]
mod heap;
