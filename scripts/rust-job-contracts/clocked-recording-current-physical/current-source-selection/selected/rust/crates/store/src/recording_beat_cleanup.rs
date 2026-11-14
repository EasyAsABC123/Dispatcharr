//! PRIVATE UNCOMPILED normal DVR cleanup from actual Source3 SQL rows1303..1320
//! and pinned beat2.9.0 models36bd19../signalsda6509.. FILE witness. Original
//! Dispatcharr/Django/django-celery-beat authors retain application/algorithm credit.
use super::recording_capture::RecordingIdentity;
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{pool::PoolConnection, Connection, PgConnection, PgPool, Postgres};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Acquire,
    Read,
    ForeignReferences,
    BeginPeriodic,
    PreDeleteTracking,
    ClearEpg,
    ClearM3u,
    DeletePeriodic,
    CommitPeriodic,
    BeginOverride,
    OverrideTracking,
    CommitOverride,
    UnusedClocked,
    BeginClocked,
    DeleteClocked,
    ClockedTracking,
    CommitClocked,
    Ready,
    Close,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Commit {
    NotReached,
    Unknown,
    Acknowledged,
}
#[derive(Clone, Copy, Debug)]
pub struct Prefix {
    pub periodic: Commit,
    pub override_touch: Commit,
    pub clocked: Commit,
}
impl Default for Prefix {
    fn default() -> Self {
        Self {
            periodic: Commit::NotReached,
            override_touch: Commit::NotReached,
            clocked: Commit::NotReached,
        }
    }
}
#[derive(Debug)]
pub enum Error {
    Unsupported(&'static str),
    Sql {
        stage: Stage,
        source: sqlx::Error,
        prefix: Prefix,
        rollback: Option<sqlx::Error>,
        ready: Option<sqlx::Error>,
        close: Option<sqlx::Error>,
    },
    Boundary {
        cause: Box<Error>,
        prefix: Prefix,
        rollback: Option<sqlx::Error>,
        ready: Option<sqlx::Error>,
        close: Option<sqlx::Error>,
    },
    Supervisor,
}
fn sql(stage: Stage, source: sqlx::Error, prefix: Prefix) -> Error {
    Error::Sql {
        stage,
        source,
        prefix,
        rollback: None,
        ready: None,
        close: None,
    }
}
#[derive(Clone, Debug, PartialEq)]
struct Snapshot {
    periodic: i64,
    clocked: i64,
    spec: Value,
}
pub struct Prepared {
    origin: Arc<()>,
    expected: RecordingIdentity,
    snapshot: Snapshot,
}
pub struct Store {
    pool: PgPool,
    origin: Arc<()>,
}
impl Store {
    pub fn source3_normal(pool: PgPool) -> Self {
        Self {
            pool,
            origin: Arc::new(()),
        }
    }
    pub async fn prepare(
        self: Arc<Self>,
        expected: RecordingIdentity,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Prepared, Error> {
        if !(1..=i64::from(i32::MAX)).contains(&expected.recording)
            || !(1..=i64::from(i32::MAX)).contains(&expected.channel)
            || expected.end - expected.start != chrono::Duration::seconds(30)
            || expected.start.timestamp_subsec_nanos() != 0
        {
            return Err(Error::Unsupported("Source3 future-clocked identity"));
        }
        tokio::spawn(async move {
            let _owner = owner;
            let mut connection = self
                .pool
                .acquire()
                .await
                .map_err(|source| sql(Stage::Acquire, source, Prefix::default()))?;
            let observed = read(&mut connection, &expected)
                .await
                .map(|snapshot| Prepared {
                    origin: self.origin.clone(),
                    expected,
                    snapshot,
                });
            settle(connection, observed, Prefix::default(), false).await
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
    pub async fn execute(
        self: Arc<Self>,
        prepared: Prepared,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Prefix, Error> {
        if !Arc::ptr_eq(&self.origin, &prepared.origin) {
            return Err(Error::Unsupported("foreign cleanup preparation"));
        }
        tokio::spawn(async move {
            let _owner=owner;let mut connection=self.pool.acquire().await.map_err(|source|sql(Stage::Acquire,source,Prefix::default()))?;
            let mut prefix=Prefix::default();let mut transaction_attempted=false;
            let observed=async {
                let current=read(&mut connection,&prepared.expected).await?;
                if current!=prepared.snapshot {return Err(Error::Unsupported("post-admission clocked task changed"));}
                // GroupA is DjangoCollector's atomic deletion: pre_delete touch,
                // SET_NULL FKs, then PT DELETE. No invented single global TX.
                transaction_attempted=true;
                command(&mut connection,"BEGIN",Stage::BeginPeriodic,prefix).await?;
                command(&mut connection,"SAVEPOINT native_dvr_predelete",Stage::PreDeleteTracking,prefix).await?;
                touch(&mut connection,Stage::PreDeleteTracking,prefix).await?;
                command(&mut connection,"RELEASE SAVEPOINT native_dvr_predelete",Stage::PreDeleteTracking,prefix).await?;
                for (query,stage) in [("UPDATE epg_epgsource SET refresh_task_id=NULL WHERE refresh_task_id=$1",Stage::ClearEpg),
                                      ("UPDATE m3u_m3uaccount SET refresh_task_id=NULL WHERE refresh_task_id=$1",Stage::ClearM3u)] {
                    let rows=sqlx::query(query).bind(current.periodic).execute(&mut *connection).await.map_err(|source|sql(stage,source,prefix))?.rows_affected();
                    if rows!=0 {return Err(Error::Unsupported("unqualified concurrent FK reference; rollback required"));}
                }
                let deleted=sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE id=$1 AND name=$2 AND clocked_id=$3")
                    .bind(current.periodic).bind(format!("dvr-recording-{}",prepared.expected.recording)).bind(current.clocked)
                    .execute(&mut *connection).await.map_err(|source|sql(Stage::DeletePeriodic,source,prefix))?.rows_affected();
                if deleted!=1 {return Err(Error::Unsupported("periodic delete changed"));}
                prefix.periodic=Commit::Unknown;
                command(&mut connection,"COMMIT",Stage::CommitPeriodic,prefix).await?;
                prefix.periodic=Commit::Acknowledged;
                // GroupB is PeriodicTask.delete override's independent changed().
                transaction_attempted=true;
                command(&mut connection,"BEGIN",Stage::BeginOverride,prefix).await?;
                touch(&mut connection,Stage::OverrideTracking,prefix).await?;
                prefix.override_touch=Commit::Unknown;
                command(&mut connection,"COMMIT",Stage::CommitOverride,prefix).await?;
                prefix.override_touch=Commit::Acknowledged;transaction_attempted=false;
                let remaining:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$1)")
                    .bind(current.clocked).fetch_one(&mut *connection).await.map_err(|source|sql(Stage::UnusedClocked,source,prefix))?;
                if remaining {return Err(Error::Unsupported("shared clocked/concurrent writer outside Source3 scope"));}
                // GroupC deletes the unused Clocked row; its post_delete touch
                // occurs AFTER DELETE inside that separate Collector transaction.
                transaction_attempted=true;
                command(&mut connection,"BEGIN",Stage::BeginClocked,prefix).await?;
                let deleted=sqlx::query("DELETE FROM django_celery_beat_clockedschedule WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$1)")
                    .bind(current.clocked).execute(&mut *connection).await.map_err(|source|sql(Stage::DeleteClocked,source,prefix))?.rows_affected();
                if deleted!=1 {return Err(Error::Unsupported("clocked delete changed"));}
                command(&mut connection,"SAVEPOINT native_dvr_clocked_postdelete",Stage::ClockedTracking,prefix).await?;
                touch(&mut connection,Stage::ClockedTracking,prefix).await?;
                command(&mut connection,"RELEASE SAVEPOINT native_dvr_clocked_postdelete",Stage::ClockedTracking,prefix).await?;
                prefix.clocked=Commit::Unknown;
                command(&mut connection,"COMMIT",Stage::CommitClocked,prefix).await?;
                prefix.clocked=Commit::Acknowledged;transaction_attempted=false;
                Ok(prefix)
            }.await;
            settle(connection,observed,prefix,transaction_attempted).await
        }).await.map_err(|_|Error::Supervisor)?
    }
}
async fn command(
    connection: &mut PgConnection,
    text: &str,
    stage: Stage,
    prefix: Prefix,
) -> Result<(), Error> {
    sqlx::query(text)
        .execute(&mut *connection)
        .await
        .map(|_| ())
        .map_err(|source| sql(stage, source, prefix))
}
async fn touch(connection: &mut PgConnection, stage: Stage, prefix: Prefix) -> Result<(), Error> {
    let singleton: Option<i64> = sqlx::query_scalar(
        "SELECT ident::bigint FROM django_celery_beat_periodictasks WHERE ident=1 FOR UPDATE",
    )
    .fetch_optional(&mut *connection)
    .await
    .map_err(|source| sql(stage, source, prefix))?;
    if singleton != Some(1) {
        return Err(Error::Unsupported("existing tracking singleton only"));
    }
    let changed =
        sqlx::query("UPDATE django_celery_beat_periodictasks SET last_update=$1 WHERE ident=1")
            .bind(Utc::now())
            .execute(&mut *connection)
            .await
            .map_err(|source| sql(stage, source, prefix))?
            .rows_affected();
    if changed != 1 {
        return Err(Error::Unsupported("tracking singleton changed"));
    }
    Ok(())
}
async fn read(
    connection: &mut PgConnection,
    expected: &RecordingIdentity,
) -> Result<Snapshot, Error> {
    // Every unbounded text value is CASE-bounded in the actual SELECT before it
    // can be transferred, not merely rejected after allocating a whole row.
    let rows:Vec<(i64,i64,chrono::DateTime<Utc>,Value)>=sqlx::query_as(
        "SELECT p.id::bigint,p.clocked_id::bigint,c.clocked_time,jsonb_build_object('task',p.task,'args',CASE WHEN octet_length(p.args)<=2048 THEN p.args ELSE NULL END,'kwargs',CASE WHEN octet_length(p.kwargs)<=1024 THEN p.kwargs ELSE NULL END,'headers',CASE WHEN octet_length(p.headers)<=1024 THEN p.headers ELSE NULL END,'bounded',octet_length(p.args)<=2048 AND octet_length(p.kwargs)<=1024 AND (p.headers IS NULL OR octet_length(p.headers)<=1024),'queue',p.queue,'exchange',p.exchange,'routing_key',p.routing_key,'priority',p.priority,'expires',p.expires,'expire_seconds',p.expire_seconds,'one_off',p.one_off,'start_time',p.start_time,'other_schedule',p.interval_id IS NOT NULL OR p.crontab_id IS NOT NULL OR p.solar_id IS NOT NULL) FROM django_celery_beat_periodictask p JOIN django_celery_beat_clockedschedule c ON c.id=p.clocked_id WHERE p.name=$1 LIMIT 2"
    ).bind(format!("dvr-recording-{}",expected.recording)).fetch_all(&mut *connection).await.map_err(|source|sql(Stage::Read,source,Prefix::default()))?;
    let [(periodic, clocked, time, spec)] = rows.as_slice() else {
        return Err(Error::Unsupported("unique known future clocked task"));
    };
    if *periodic <= 0
        || *clocked <= 0
        || *time != expected.start
        || spec["task"] != "apps.channels.tasks.run_recording"
        || spec["bounded"] != true
        || spec["one_off"] != true
        || spec["other_schedule"] != false
    {
        return Err(Error::Unsupported("natural future clocked row"));
    }
    for name in [
        "queue",
        "exchange",
        "routing_key",
        "priority",
        "expires",
        "expire_seconds",
        "start_time",
    ] {
        if !spec[name].is_null() {
            return Err(Error::Unsupported("periodic route/options domain"));
        }
    }
    let args: Value = serde_json::from_str(
        spec["args"]
            .as_str()
            .ok_or(Error::Unsupported("periodic args bound"))?,
    )
    .map_err(|_| Error::Unsupported("periodic args JSON"))?;
    if args
        != json!([
            expected.recording,
            expected.channel,
            expected.start.format("%Y-%m-%d %H:%M:%S+00:00").to_string(),
            expected.end.format("%Y-%m-%d %H:%M:%S+00:00").to_string()
        ])
    {
        return Err(Error::Unsupported("four exact Source datetime arguments"));
    }
    for name in ["kwargs", "headers"] {
        let raw = spec[name].as_str().unwrap_or("{}");
        let value: Value =
            serde_json::from_str(raw).map_err(|_| Error::Unsupported("periodic JSON options"))?;
        if value != json!({}) {
            return Err(Error::Unsupported("empty periodic options only"));
        }
    }
    let context:(bool,bool,bool,bool)=sqlx::query_as("SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictasks WHERE ident=1),EXISTS(SELECT 1 FROM epg_epgsource WHERE refresh_task_id=$1),EXISTS(SELECT 1 FROM m3u_m3uaccount WHERE refresh_task_id=$1),EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id=$2 AND id<>$1)")
        .bind(periodic).bind(clocked).fetch_one(&mut *connection).await.map_err(|source|sql(Stage::ForeignReferences,source,Prefix::default()))?;
    if !context.0 || context.1 || context.2 || context.3 {
        return Err(Error::Unsupported(
            "tracking/foreign-reference Source3 domain",
        ));
    }
    Ok(Snapshot {
        periodic: *periodic,
        clocked: *clocked,
        spec: spec.clone(),
    })
}
async fn settle<T>(
    mut connection: PoolConnection<Postgres>,
    observed: Result<T, Error>,
    prefix: Prefix,
    transaction_attempted: bool,
) -> Result<T, Error> {
    // No logical timeout drops a pending SQL transaction/Ready operation. A
    // failed COMMIT is Unknown, not a license to replay mutation. Rollback may
    // discover the server already committed; preserve original provenance.
    let rollback = if observed.is_err() && transaction_attempted {
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .err()
    } else {
        None
    };
    let ready = connection.ping().await;
    let close = connection.close().await;
    match observed {
        Err(Error::Sql {
            stage,
            source,
            prefix,
            ..
        }) => Err(Error::Sql {
            stage,
            source,
            prefix,
            rollback,
            ready: ready.err(),
            close: close.err(),
        }),
        Err(cause) => Err(Error::Boundary {
            cause: Box::new(cause),
            prefix,
            rollback,
            ready: ready.err(),
            close: close.err(),
        }),
        Ok(value) => {
            if let Err(source) = ready {
                return Err(Error::Sql {
                    stage: Stage::Ready,
                    source,
                    prefix,
                    rollback,
                    ready: None,
                    close: close.err(),
                });
            }
            if let Err(source) = close {
                return Err(Error::Sql {
                    stage: Stage::Close,
                    source,
                    prefix,
                    rollback,
                    ready: None,
                    close: None,
                });
            }
            if let Some(source) = rollback {
                return Err(sql(Stage::Ready, source, prefix));
            }
            Ok(value)
        }
    }
}
