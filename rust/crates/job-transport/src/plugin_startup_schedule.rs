//! PluginsConfig boot scheduling using the same normal Beat owner and pool.
//! Original Dispatcharr/django-celery-beat authors retain all algorithm credit.
use super::*;
type ExistingSchedule = (i64, Option<i64>, Option<i64>, Option<i64>, Option<i64>);
const NAME: &str = "plugin-repo-refresh-task";
#[derive(Debug, PartialEq)]
enum Plan {
    Delete,
    Interval { every: i32, enabled: bool },
}
fn plan(value: Option<Value>) -> Result<Plan, Error> {
    let interval = match value {
        None => json!(6),
        Some(v) => v
            .as_object()
            .ok_or(Error::Configuration("plugin settings object"))?
            .get("refresh_interval_hours")
            .cloned()
            .unwrap_or(json!(6)),
    };
    let number = match interval {
        Value::Bool(v) => {
            if v {
                1.0
            } else {
                0.0
            }
        }
        Value::Number(v) => v
            .as_f64()
            .ok_or(Error::Configuration("plugin interval number"))?,
        _ => return Err(Error::Configuration("plugin interval type")),
    };
    if number == 0.0 {
        return Ok(Plan::Delete);
    }
    if !number.is_finite() || number.trunc() > f64::from(i32::MAX) {
        return Err(Error::Configuration("plugin interval range"));
    }
    Ok(Plan::Interval {
        every: number.trunc().max(1.0) as i32,
        enabled: number > 0.0,
    })
}
async fn setup(connection: &mut PgConnection) -> Result<(), Error> {
    // Keep the session lock through the separate interval and periodic-row
    // writes. A transaction advisory lock would release too early. The lock
    // transaction bounds the wait without changing the caller's SQL settings.
    let mut lock = connection.begin().await?;
    sqlx::query("SET LOCAL lock_timeout = '5s'")
        .execute(&mut *lock)
        .await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext('dispatcharr.plugin.repository_schedule'), 0)")
        .execute(&mut *lock)
        .await?;

    let observed = match lock.commit().await {
        Ok(()) => synchronize_rows(connection).await,
        Err(error) => Err(Error::from(error)),
    };
    let released: bool = sqlx::query_scalar(
        "SELECT pg_advisory_unlock(hashtext('dispatcharr.plugin.repository_schedule'), 0)",
    )
    .fetch_one(&mut *connection)
    .await?;
    if !released {
        return Err(Error::Configuration("plugin repository schedule lock was not retained"));
    }
    observed
}

async fn synchronize_rows(connection: &mut PgConnection) -> Result<(), Error> {
    let row:Option<(i32,Option<String>)>=sqlx::query_as("SELECT octet_length(value::text),CASE WHEN octet_length(value::text)<=65536 THEN value::text END FROM core_coresettings WHERE key='plugin_repo_settings'").fetch_optional(&mut *connection).await?;
    let value = match row {
        None => None,
        Some((bytes, Some(raw))) if bytes >= 0 && bytes as usize == raw.len() => Some(
            serde_json::from_str(&raw).map_err(|_| Error::Configuration("plugin settings JSON"))?,
        ),
        _ => return Err(Error::Capacity),
    };
    let plan = plan(value)?;
    let old:Option<ExistingSchedule>=sqlx::query_as("SELECT id::bigint,interval_id::bigint,crontab_id::bigint,clocked_id::bigint,solar_id::bigint FROM django_celery_beat_periodictask WHERE name=$1").bind(NAME).fetch_optional(&mut *connection).await?;
    let new_interval = match plan {
        Plan::Delete => {
            if old.is_none() {
                return Ok(());
            }
            None
        }
        Plan::Interval { every, .. } => {
            let existing:Option<i64>=sqlx::query_scalar("SELECT id::bigint FROM django_celery_beat_intervalschedule WHERE every=$1 AND period='hours' ORDER BY id LIMIT 1").bind(every).fetch_optional(&mut *connection).await?;
            Some(match existing{Some(id)=>id,None=>sqlx::query_scalar("INSERT INTO django_celery_beat_intervalschedule(every,period) VALUES($1,'hours') RETURNING id::bigint").bind(every).fetch_one(&mut *connection).await?})
        }
    };
    if matches!(plan, Plan::Interval { .. })
        && old.as_ref().is_some_and(|v| v.3.is_some() || v.4.is_some())
    {
        // Source validation fails after the separate get_or_create interval effect.
        return Err(Error::Configuration("plugin periodic conflicting schedule"));
    }
    let mut tx = connection.begin().await?;
    match plan {
        Plan::Delete => {
            sqlx::query("DELETE FROM django_celery_beat_periodictask WHERE name=$1")
                .bind(NAME)
                .execute(&mut *tx)
                .await?;
        }
        Plan::Interval { enabled, .. } => {
            // Django updates an existing named row without evaluating its
            // INSERT-only sequence default. Keep the conflict fallback for
            // a creator outside the native advisory lock.
            let now = Utc::now();
            let updated = sqlx::query(concat!(
                "UPDATE django_celery_beat_periodictask SET task='apps.plugins.tasks.refresh_plugin_repos',",
                "interval_id=$2,crontab_id=NULL,enabled=$3,kwargs='{}',",
                "last_run_at=CASE WHEN $3 THEN last_run_at ELSE NULL END,date_changed=$4 ",
                "WHERE name=$1"
            ))
            .bind(NAME)
            .bind(new_interval)
            .bind(enabled)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() == 0 {
                sqlx::query(concat!(
                    "INSERT INTO django_celery_beat_periodictask ",
                    "(name,task,interval_id,crontab_id,args,kwargs,headers,enabled,total_run_count,date_changed,description,one_off) ",
                    "VALUES($1,'apps.plugins.tasks.refresh_plugin_repos',$2,NULL,'[]','{}','{}',$3,0,$4,'',false) ",
                    "ON CONFLICT(name) DO UPDATE SET task=EXCLUDED.task,interval_id=EXCLUDED.interval_id,",
                    "crontab_id=NULL,enabled=EXCLUDED.enabled,kwargs='{}',",
                    "last_run_at=CASE WHEN EXCLUDED.enabled THEN django_celery_beat_periodictask.last_run_at ELSE NULL END,",
                    "date_changed=EXCLUDED.date_changed"
                ))
                .bind(NAME)
                .bind(new_interval)
                .bind(enabled)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            }
        }
    }
    sqlx::query("INSERT INTO django_celery_beat_periodictasks(ident,last_update) VALUES(1,$1) ON CONFLICT(ident) DO UPDATE SET last_update=EXCLUDED.last_update").bind(Utc::now()).execute(&mut *tx).await?;
    tx.commit().await?;
    if let Some((_, interval, cron, _, _)) = old {
        if let Some(id) = interval.filter(|id| Some(*id) != new_interval) {
            sqlx::query("DELETE FROM django_celery_beat_intervalschedule s WHERE s.id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask t WHERE t.interval_id=s.id)").bind(id).execute(&mut *connection).await?;
        }
        if let Some(id) = cron {
            sqlx::query("DELETE FROM django_celery_beat_crontabschedule s WHERE s.id=$1 AND NOT EXISTS(SELECT 1 FROM django_celery_beat_periodictask t WHERE t.crontab_id=s.id)").bind(id).execute(&mut *connection).await?;
        }
    }
    Ok(())
}
impl Scheduler {
    /// Source boot hook is independent of whether the Beat timer is enabled.
    pub async fn initialize_plugin_schedule(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if self.inner.closed.load(Ordering::Acquire) || self.inner.started.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let _owner = Owner {
                _caller: caller,
                _slot: slot,
                _inner: inner.clone(),
            };
            let mut connection = inner.pool.acquire().await?;
            connection.close_on_drop();
            let observed = setup(&mut connection).await;
            let ready = connection.ping().await.err().map(Box::new);
            let close = connection.close().await.err().map(Box::new);
            if ready.is_some() || close.is_some() {
                return Err(Error::Settlement {
                    primary: observed.err().map(Box::new),
                    ready,
                    close,
                });
            }
            observed
        })
        .await
        .map_err(|_| Error::Supervisor)?
    }
}
/// Reuse the exact startup scheduling body on an already-admitted HTTP task SQL
/// session. No Scheduler timer, checkout, client or separate writer is started.
pub struct PluginRepositoryScheduleAdapter;

impl PluginRepositoryScheduleAdapter {
    /// Plain Main owns the boot-time row sync while its worker and Beat
    /// flags are off. Reuse the ordinary HTTP scheduler callback on the same
    /// Auth writer budget and close the actual connection before returning.
    pub async fn initialize_core(
        &self,
        auth: &dispatcharr_store::AuthService,
    ) -> Result<(), dispatcharr_store::StoreError> {
        let owner = Arc::new(());
        let session = auth.borrow_task_sql_session(owner.clone()).await?;
        let observed =
            dispatcharr_store::PluginRepositorySchedule::synchronize(self, session.clone(), owner)
                .await;
        session.close().await?;
        observed
    }
}

impl dispatcharr_store::PluginRepositorySchedule for PluginRepositoryScheduleAdapter {
    fn synchronize<'a>(
        &'a self,
        session: dispatcharr_store::TaskSqlSession,
        owner: Arc<dyn Send + Sync>,
    ) -> dispatcharr_contracts::BoxFuture<'a, Result<(), dispatcharr_store::StoreError>> {
        Box::pin(async move {
            session
                .run(move |connection| {
                    Box::pin(async move {
                        let _owner = owner;
                        setup(connection).await.map_err(|error| match error {
                            Error::Database(error) => {
                                dispatcharr_store::StoreError::Database(*error)
                            }
                            _ => dispatcharr_store::StoreError::Unsupported(
                                "Plugin periodic settings exceed bounded Source schedule domain",
                            ),
                        })
                    })
                })
                .await?
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_plugin_interval_boolean_numeric_and_type_branches() {
        assert_eq!(
            plan(None).unwrap(),
            Plan::Interval {
                every: 6,
                enabled: true
            }
        );
        for value in [json!(0), json!(false), json!(0.0)] {
            assert_eq!(
                plan(Some(json!({"refresh_interval_hours":value}))).unwrap(),
                Plan::Delete
            );
        }
        assert_eq!(
            plan(Some(json!({"refresh_interval_hours":0.2}))).unwrap(),
            Plan::Interval {
                every: 1,
                enabled: true
            }
        );
        assert_eq!(
            plan(Some(json!({"refresh_interval_hours":-8}))).unwrap(),
            Plan::Interval {
                every: 1,
                enabled: false
            }
        );
        for value in [json!("0"), Value::Null, json!([])] {
            assert!(plan(Some(json!({"refresh_interval_hours":value}))).is_err());
        }
        assert!(plan(Some(json!([]))).is_err());
    }
}
