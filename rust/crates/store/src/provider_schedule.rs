//! Original core.scheduling interval/cron model-save effects.
//! All application/model/scheduling credit belongs to Dispatcharr's authors.
use crate::StoreError;
use chrono::Utc;
use sqlx::{PgConnection, Row};

pub(crate) struct Spec<'a> {
    pub name: &'a str,
    pub task: &'a str,
    /// Python json.dumps spelling is retained by the known producer caller.
    pub kwargs: &'a str,
    pub interval_hours: i32,
    pub cron: &'a str,
    pub active: bool,
    pub timezone: &'a str,
}
pub(crate) async fn touched(c: &mut PgConnection) -> Result<(), StoreError> {
    let now = Utc::now();
    if sqlx::query("UPDATE django_celery_beat_periodictasks SET last_update=$1 WHERE ident=1")
        .bind(now)
        .execute(&mut *c)
        .await?
        .rows_affected()
        == 0
    {
        sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1)")
            .bind(now)
            .execute(&mut *c)
            .await?;
    }
    Ok(())
}
async fn clean_interval(c: &mut PgConnection, id: i64) -> Result<(), StoreError> {
    let deleted = sqlx::query("DELETE FROM django_celery_beat_intervalschedule i WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask t WHERE t.interval_id=i.id)")
        .bind(id).execute(&mut *c).await?.rows_affected();
    if deleted != 0 {
        touched(c).await?;
    }
    Ok(())
}
async fn clean_crontab(c: &mut PgConnection, id: i64) -> Result<(), StoreError> {
    let deleted = sqlx::query("DELETE FROM django_celery_beat_crontabschedule i WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask t WHERE t.crontab_id=i.id)")
        .bind(id).execute(&mut *c).await?.rows_affected();
    if deleted != 0 {
        touched(c).await?;
    }
    Ok(())
}
pub(crate) async fn upsert(c: &mut PgConnection, spec: Spec<'_>) -> Result<i64, StoreError> {
    let old = sqlx::query("SELECT id::bigint,interval_id::bigint,crontab_id::bigint,solar_id,clocked_id FROM django_celery_beat_periodictask WHERE name=$1")
        .bind(spec.name).fetch_optional(&mut *c).await?;
    let old_interval = old
        .as_ref()
        .map(|r| r.try_get::<Option<i64>, _>("interval_id"))
        .transpose()?
        .flatten();
    let old_crontab = old
        .as_ref()
        .map(|r| r.try_get::<Option<i64>, _>("crontab_id"))
        .transpose()?
        .flatten();
    let use_cron = !spec.cron.trim().is_empty();
    let (interval, crontab) = if use_cron {
        let p = spec.cron.split_whitespace().collect::<Vec<_>>();
        if p.len() != 5 {
            return Err(StoreError::UnhandledRestFault);
        }
        let rows:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_crontabschedule WHERE minute=$1 AND hour=$2 AND day_of_month=$3 AND month_of_year=$4 AND day_of_week=$5 AND timezone=$6 LIMIT 2")
            .bind(p[0]).bind(p[1]).bind(p[2]).bind(p[3]).bind(p[4]).bind(spec.timezone).fetch_all(&mut *c).await?;
        let id = match rows.as_slice() {
            [id] => *id,
            [] => {
                let id=sqlx::query_scalar("INSERT INTO django_celery_beat_crontabschedule(minute,hour,day_of_month,month_of_year,day_of_week,timezone) VALUES($1,$2,$3,$4,$5,$6) RETURNING id::bigint")
                .bind(p[0]).bind(p[1]).bind(p[2]).bind(p[3]).bind(p[4]).bind(spec.timezone).fetch_one(&mut *c).await?;
                touched(c).await?;
                id
            }
            _ => return Err(StoreError::UnhandledRestFault),
        };
        (None, Some(id))
    } else {
        let every = spec.interval_hours.max(1);
        let rows:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_intervalschedule WHERE every=$1 AND period='hours' LIMIT 2")
            .bind(every).fetch_all(&mut *c).await?;
        let id = match rows.as_slice() {
            [id] => *id,
            [] => {
                let id=sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES($1,'hours') RETURNING id::bigint")
                .bind(every).fetch_one(&mut *c).await?;
                touched(c).await?;
                id
            }
            _ => return Err(StoreError::UnhandledRestFault),
        };
        (Some(id), None)
    };
    let enabled = spec.active && (use_cron || spec.interval_hours > 0);
    let id = if let Some(old) = old {
        // Original PeriodicTask.validate_unique rejects a leftover solar/clocked
        // schedule; never silently erase an unobserved other schedule family.
        if old.try_get::<Option<i32>, _>("solar_id")?.is_some()
            || old.try_get::<Option<i32>, _>("clocked_id")?.is_some()
        {
            return Err(StoreError::UnhandledRestFault);
        }
        let id: i64 = old.try_get("id")?;
        sqlx::query("UPDATE django_celery_beat_periodictask SET task=$2,interval_id=$3,crontab_id=$4,enabled=$5,kwargs=$6,last_run_at=CASE WHEN $5 THEN last_run_at ELSE NULL END,date_changed=$7 WHERE id=$1")
            .bind(id).bind(spec.task).bind(interval).bind(crontab).bind(enabled).bind(spec.kwargs).bind(Utc::now()).execute(&mut *c).await?;
        id
    } else {
        sqlx::query_scalar("INSERT INTO django_celery_beat_periodictask(name,task,interval_id,crontab_id,solar_id,clocked_id,args,kwargs,queue,exchange,routing_key,headers,priority,expires,expire_seconds,one_off,start_time,enabled,last_run_at,total_run_count,date_changed,description) VALUES($1,$2,$3,$4,NULL,NULL,'[]',$5,NULL,NULL,NULL,'{}',NULL,NULL,NULL,false,NULL,$6,NULL,0,$7,'') RETURNING id::bigint")
            .bind(spec.name).bind(spec.task).bind(interval).bind(crontab).bind(spec.kwargs).bind(enabled).bind(Utc::now()).fetch_one(&mut *c).await?
    };
    touched(c).await?;
    if let Some(old) = old_interval.filter(|old| Some(*old) != interval) {
        clean_interval(c, old).await?;
    }
    if let Some(old) = old_crontab.filter(|old| Some(*old) != crontab) {
        clean_crontab(c, old).await?;
    }
    Ok(id)
}
/// The legacy M3U/EPG delete helpers remove the task and orphan interval only;
/// unlike core.delete_periodic_task they leave orphan crontabs behind.
pub(crate) async fn legacy_delete(c: &mut PgConnection, name: &str) -> Result<bool, StoreError> {
    let row = sqlx::query(
        "SELECT id::bigint,interval_id::bigint FROM django_celery_beat_periodictask WHERE name=$1",
    )
    .bind(name)
    .fetch_optional(&mut *c)
    .await?;
    let Some(row) = row else { return Ok(false) };
    let id: i64 = row.try_get("id")?;
    let interval: Option<i64> = row.try_get("interval_id")?;
    sqlx::query("UPDATE m3u_m3uaccount SET refresh_task_id=NULL WHERE refresh_task_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    sqlx::query("UPDATE epg_epgsource SET refresh_task_id=NULL WHERE refresh_task_id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE id=$1")
        .bind(id)
        .execute(&mut *c)
        .await?;
    touched(c).await?;
    if let Some(interval) = interval {
        clean_interval(c, interval).await?;
    }
    Ok(true)
}
