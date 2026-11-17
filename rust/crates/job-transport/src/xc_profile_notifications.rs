//! Immediate M3UAccountProfile post_save effects on the actual borrowed session.
//! Original Dispatcharr/Django/Channels authors retain full behavior credit.
use crate::{
    active_notifications::{self as shared, NotificationClock, UtcNotificationClock},
    default_domains::Error,
    notification_terminal::{Effect, ReportError},
};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_store::{
    xc_account_tasks::{ProfileSaveSignal, SavedProfile, SignalOutcome},
    StoreError, TaskSqlSession,
};
use serde_json::json;
use std::sync::Arc;
type Owner = Arc<dyn Send + Sync>;
#[derive(Clone)]
pub(crate) struct Callback {
    sink: Arc<dyn UpdateEventSink>,
    clock: Arc<dyn NotificationClock>,
}
impl Callback {
    pub(crate) fn new(sink: Arc<dyn UpdateEventSink>) -> Self {
        Self {
            sink,
            clock: Arc::new(UtcNotificationClock),
        }
    }
    async fn send(&self, event: serde_json::Value, owner: Owner) -> Result<bool, StoreError> {
        let sent = shared::send_observed(self.sink.clone(), event, owner)
            .await
            .map_err(|_| unsupported())?;
        // Both Source send helpers catch/log every exception locally, then the
        // signal continues. Only actual retained physical settlement permits it.
        if sent.is_err() {
            eprintln!("XC profile signal: caught settled notification send failure");
        }
        Ok(sent.is_err())
    }
    async fn apply(
        &self,
        session: TaskSqlSession,
        profile: SavedProfile,
        owner: Owner,
    ) -> Result<SignalOutcome, StoreError> {
        let _retained = owner.clone();
        let now = self.clock.now();
        let plan = shared::profile_plan(profile.id, profile.expiration, now);
        let remove = plan.remove;
        let deleted = session
            .run(move |c| {
                Box::pin(shared::collect_on_connection(
                    c,
                    Some(remove),
                    None,
                    Effect::OppositeCollector,
                ))
            })
            .await?;
        let Some(keys) = settled_signal_sql(deleted)? else {
            return Ok(SignalOutcome::CaughtSettledFailure);
        };
        let mut caught = false;
        for key in keys {
            caught|=self.send(json!({"type":"update","data":{"type":"notification_dismissed","notification_key":key}}),owner.clone()).await?;
        }
        if let Some(key) = plan.key {
            let expiration = profile.expiration.ok_or_else(unsupported)?;
            let p = (
                profile.id,
                profile.account_id,
                profile.name,
                profile.account_name,
                expiration,
            );
            let row = session
                .run(move |c| {
                    Box::pin(async move {
                        shared::upsert_on_connection(c, &p, now, plan.expired, &key).await
                    })
                })
                .await?;
            let Some(row) = settled_signal_sql(row)? else {
                return Ok(SignalOutcome::CaughtSettledFailure);
            };
            caught|=self.send(json!({"type":"update","data":{"type":"system_notification","notification":row}}),owner).await?;
        }
        Ok(if caught {
            SignalOutcome::CaughtSettledFailure
        } else {
            SignalOutcome::Applied
        })
    }
}
fn unsupported() -> StoreError {
    StoreError::Unsupported("XC profile signal outside physically settled supported domain")
}
/// The SQL actor has already observed Ready on the SAME session. Source catches
/// an actual DB ErrorResponse from its signal; IO/decoder/budget/panic failures
/// are not fabricated Python exceptions or successful signal completions.
fn settled_signal_sql<T>(result: Result<T, ReportError>) -> Result<Option<T>, StoreError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ReportError {
            error: Error::Sql(error),
            ..
        }) if error.as_database_error().is_some() => {
            eprintln!("XC profile signal: caught settled PostgreSQL notification signal failure");
            Ok(None)
        }
        Err(ReportError {
            error: Error::Sql(error),
            ..
        }) => Err(StoreError::Database(error)),
        Err(_) => Err(unsupported()),
    }
}
impl Callback {
    async fn delete_in_collector(
        &self,
        c: &mut sqlx::PgConnection,
        account_id: i64,
        profile_id: i64,
        owner: Owner,
        already_deleted: bool,
    ) -> Result<bool, StoreError> {
        use sqlx::Row;
        let _retained = owner.clone();
        if c.protocol_transaction_is_idle().await? {
            return Err(unsupported());
        }
        if !already_deleted {
            let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM m3u_m3uaccountprofile WHERE id=$1 AND m3u_account_id=$2)",
        )
        .bind(profile_id)
        .bind(account_id)
        .fetch_one(&mut *c)
        .await?;
            if !exists {
                return Ok(false);
            }
        }
        // Positive Source schema only. Unknown application triggers
        // can add arbitrary caught-SQL semantics or side effects;
        // reject that unqualified domain before deleting anything.
        let triggers:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled <> 'D' AND tgrelid=ANY(ARRAY['m3u_m3uaccountprofile'::regclass,'core_systemnotification'::regclass,'core_notificationdismissal'::regclass,'accounts_user'::regclass]))")
                        .fetch_one(&mut *c).await?;
        if triggers {
            return Err(unsupported());
        }
        // Finite positive draft domain. Preflight all capacity
        // before DELETE; broader user scans await their own gate.
        let(count,maximum):(i64,i32)=sqlx::query_as("SELECT count(*),COALESCE(max(octet_length(custom_properties::text)),0) FROM accounts_user WHERE custom_properties ? 'allowed_m3u_profile_ids'")
                        .fetch_one(&mut *c).await?;
        if count > 64 || maximum > 65536 {
            return Err(unsupported());
        }
        let keys = vec![
            format!("xc-exp-warning-{profile_id}"),
            format!("xc-exp-expired-{profile_id}"),
        ];
        let notice_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM core_systemnotification WHERE notification_key=ANY($1)",
        )
        .bind(&keys)
        .fetch_one(&mut *c)
        .await?;
        let dismissal_count:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key=ANY($1))").bind(&keys).fetch_one(&mut *c).await?;
        if notice_count > 128 || dismissal_count > 1024 {
            return Err(unsupported());
        }
        // Source Collector deletes the profile, then invokes its
        // post_delete receivers inside the SAME outer transaction.
        if !already_deleted {
            sqlx::query("DELETE FROM m3u_m3uaccountprofile WHERE id=$1 AND m3u_account_id=$2")
                .bind(profile_id)
                .bind(account_id)
                .execute(&mut *c)
                .await?;
        }
        // Source queries notices inside post_delete, after the
        // profile DELETE has physically completed. Preflight must
        // not supply a stale snapshot across that row-lock wait.
        let deleted_keys =
            shared::collect_in_open_transaction(c, Some(keys), None, Effect::OppositeCollector)
                .await
                .map_err(|report| match report.error {
                    Error::Sql(error) => StoreError::Database(error),
                    _ => unsupported(),
                })?;
        for key in deleted_keys {
            let event = json!({"type":"update","data":{"type":"notification_dismissed","notification_key":key}});
            let sent = shared::send_observed(self.sink.clone(), event, owner.clone())
                .await
                .map_err(|_| unsupported())?;
            if sent.is_err() {
                eprintln!("Profile delete: caught settled notification send failure");
            }
        }
        // Read actual user values at Source's scrub boundary,
        // after the retained dismissal sends; preflight above did
        // not snapshot user fields before the notification effects.
        let users=sqlx::query("SELECT id::bigint,octet_length(custom_properties::text) AS bytes,CASE WHEN octet_length(custom_properties::text)<=65536 THEN custom_properties END AS properties FROM accounts_user WHERE custom_properties ? 'allowed_m3u_profile_ids' LIMIT 65")
                        .fetch_all(&mut *c).await?;
        if users.len() > 64 {
            return Err(unsupported());
        }
        let mut updates = Vec::new();
        for user in users {
            let bytes: i32 = user.try_get("bytes")?;
            if !(0..=65536).contains(&bytes) {
                return Err(unsupported());
            }
            let properties: serde_json::Value = user.try_get("properties")?;
            if let Some(next) = dispatcharr_store::xc_account_tasks::scrub_deleted_profile_value(
                &properties,
                profile_id,
            ) {
                updates.push((user.try_get::<i64, _>("id")?, next));
            }
        }
        for (id, properties) in updates {
            sqlx::query("UPDATE accounts_user SET custom_properties=$2 WHERE id=$1")
                .bind(id)
                .bind(sqlx::types::Json(properties))
                .execute(&mut *c)
                .await?;
        }
        Ok::<_, StoreError>(true)
    }
}
impl ProfileSaveSignal for Callback {
    fn delete_profile(
        &self,
        session: TaskSqlSession,
        account_id: i64,
        profile_id: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        let callback = self.clone();
        Box::pin(async move {
            session
                .run(move |c| {
                    Box::pin(async move {
                        use sqlx::Connection;
                        let mut tx = c.begin().await?;
                        let result = callback
                            .delete_in_collector(&mut tx, account_id, profile_id, owner, false)
                            .await;
                        match result {
                            Ok(deleted) => {
                                tx.commit().await?;
                                Ok(deleted)
                            }
                            Err(error) => {
                                tx.rollback().await?;
                                Err(error)
                            }
                        }
                    })
                })
                .await?
        })
    }
    fn after_profile_delete_in_collector<'a>(
        &'a self,
        c: &'a mut sqlx::PgConnection,
        account_id: i64,
        profile_id: i64,
        owner: Owner,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.delete_in_collector(c, account_id, profile_id, owner, true)
                .await
                .map(|_| ())
        })
    }
    fn deactivate_account(
        &self,
        session: TaskSqlSession,
        account_id: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>> {
        let callback = self.clone();
        Box::pin(async move {
            let _retained = owner.clone();
            let keys=session.run(move|c|Box::pin(async move{
                let ids:Vec<i64>=sqlx::query_scalar("SELECT id::bigint FROM m3u_m3uaccountprofile WHERE m3u_account_id=$1 LIMIT 65").bind(account_id).fetch_all(&mut*c).await?;
                if ids.len()>64{return Err(unsupported());}
                Ok::<_,StoreError>(ids.into_iter().flat_map(|id|[format!("xc-exp-warning-{id}"),format!("xc-exp-expired-{id}")]).collect::<Vec<_>>())
            })).await??;
            if keys.is_empty() {
                return Ok(SignalOutcome::Applied);
            }
            let deleted = session
                .run(move |c| {
                    Box::pin(shared::collect_on_connection(
                        c,
                        Some(keys),
                        None,
                        Effect::OppositeCollector,
                    ))
                })
                .await?;
            let Some(keys) = settled_signal_sql(deleted)? else {
                return Ok(SignalOutcome::CaughtSettledFailure);
            };
            let mut caught = false;
            for key in keys {
                caught|=callback.send(json!({"type":"update","data":{"type":"notification_dismissed","notification_key":key}}),owner.clone()).await?;
            }
            Ok(if caught {
                SignalOutcome::CaughtSettledFailure
            } else {
                SignalOutcome::Applied
            })
        })
    }

    fn guard(
        &self,
        session: TaskSqlSession,
        profile_id: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let _retained = owner;
            let keys = vec![
                format!("xc-exp-warning-{profile_id}"),
                format!("xc-exp-expired-{profile_id}"),
            ];
            session.run(move|c|Box::pin(async move{
                let n:i64=sqlx::query_scalar("SELECT count(*) FROM core_systemnotification WHERE notification_key=ANY($1)").bind(&keys).fetch_one(&mut*c).await?;
                let d:i64=sqlx::query_scalar("SELECT count(*) FROM core_notificationdismissal WHERE notification_id IN(SELECT id FROM core_systemnotification WHERE notification_key=ANY($1))").bind(&keys).fetch_one(&mut*c).await?;
                if n>128||d>1024{return Err(unsupported());}
                Ok::<_,StoreError>(())
            })).await?
        })
    }
    fn after_save(
        &self,
        session: TaskSqlSession,
        profile: SavedProfile,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>> {
        Box::pin(self.apply(session, profile, owner))
    }
}

#[cfg(test)]
mod retained_signal_event_tests {
    use super::*;
    use dispatcharr_contracts::UpdateEventError;
    use std::sync::Mutex;
    struct HeldSink {
        owner: Mutex<Option<Owner>>,
        called: tokio::sync::Notify,
    }
    impl UpdateEventSink for HeldSink {
        fn send<'a>(
            &'a self,
            _group: &'a str,
            _event: serde_json::Value,
        ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
            Box::pin(async { panic!("signal must use retained publication") })
        }
        fn send_retained<'a>(
            &'a self,
            _group: &'a str,
            _event: serde_json::Value,
            owner: Owner,
        ) -> BoxFuture<'a, Result<(), UpdateEventError>> {
            Box::pin(async move {
                *self.owner.lock().unwrap() = Some(owner);
                self.called.notify_one();
                Err(UpdateEventError::Capacity)
            })
        }
    }
    #[tokio::test]
    async fn caught_event_error_still_waits_for_actual_retained_owner_drop() {
        let sink = Arc::new(HeldSink {
            owner: Mutex::new(None),
            called: tokio::sync::Notify::new(),
        });
        let callback = Arc::new(Callback::new(sink.clone()));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let owner: Owner = Arc::new(slots.clone().acquire_owned().await.unwrap());
        let active = callback.clone();
        let work =
            tokio::spawn(
                async move { active.send(json!({"type":"update","data":{}}), owner).await },
            );
        sink.called.notified().await;
        assert!(!work.is_finished());
        assert_eq!(slots.available_permits(), 0);
        drop(sink.owner.lock().unwrap().take());
        assert!(work.await.unwrap().unwrap());
        assert_eq!(slots.available_permits(), 1);
    }
}
