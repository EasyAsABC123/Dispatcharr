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
impl ProfileSaveSignal for Callback {
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
