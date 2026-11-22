//! Admission for one copied default-queue cleanup schedule.
//! Original Dispatcharr, Celery and django-celery-beat authors retain full credit.
use super::{daily_utc_crontab, Entry, Error, CLEANUP_PERIODIC_TASK};

use std::{
    future::{poll_fn, Future},
    panic::{catch_unwind, AssertUnwindSafe},
    pin::pin,
    sync::Mutex,
    task::Poll,
};

const MAX_NAME_CHARS: usize = 200;
const CLEANUP_EXPIRY_SECONDS: i32 = 43_200;

/// Exact saved-row admission for a future independent scheduler.
///
/// This identifies and validates a row. Ownership, SQL loading, and dispatch
/// remain the caller's responsibility.
pub struct SavedCleanupSchedule {
    name: String,
    pending: Mutex<Option<uuid::Uuid>>,
}

impl SavedCleanupSchedule {
    pub fn new(name: &str) -> Result<Self, Error> {
        if name.len() > MAX_NAME_CHARS * 4
            || name.trim().is_empty()
            || name.chars().count() > MAX_NAME_CHARS
            || name.chars().any(char::is_control)
        {
            return Err(Error::Configuration("saved cleanup schedule name"));
        }
        Ok(Self {
            name: name.into(),
            pending: Mutex::new(None),
        })
    }

    pub(super) fn pending(&self) -> Option<uuid::Uuid> {
        *self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn set_pending(&self, value: Option<uuid::Uuid>) {
        *self
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = value;
    }

    /// Exact name and task bindings for the bounded candidate query.
    pub fn row_identity(&self) -> (&str, &'static str) {
        (&self.name, CLEANUP_PERIODIC_TASK)
    }

    /// Check the loaded shape before reserving a tick.
    ///
    /// The loader must pass the raw foreign keys. Its normalized `Entry`
    /// can no longer distinguish a mixed cron and clocked or solar row.
    pub fn admits(&self, entry: &Entry, clocked_id: Option<i64>, solar_id: Option<i64>) -> bool {
        let (name, task) = self.row_identity();
        entry.id > 0
            && entry.total_run_count >= 0
            && entry.name == name
            && entry.task == task
            && entry.enabled
            && entry.interval.is_none()
            && clocked_id.is_none()
            && solar_id.is_none()
            && daily_utc_crontab(entry).is_some()
            && !entry.one_off
            && entry.start_time.is_none()
            && entry.expires.is_none()
            && entry.expire_seconds == Some(CLEANUP_EXPIRY_SECONDS)
            && entry.queue.is_none()
            && entry.exchange.is_none()
            && entry.routing_key.is_none()
            && entry.priority.is_none()
            && entry.args.as_array().is_some_and(Vec::is_empty)
            && entry
                .kwargs
                .as_object()
                .is_some_and(|kwargs| kwargs.is_empty())
            && entry
                .headers
                .as_object()
                .is_some_and(|headers| headers.is_empty())
    }
}

pub(super) async fn observe_saved_cleanup_tick<T>(
    future: impl Future<Output = T>,
) -> Result<T, Error> {
    let mut future = pin!(future);
    poll_fn(
        |context| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(polled) => polled.map(Ok),
            Err(_) => Poll::Ready(Err(Error::Supervisor)),
        },
    )
    .await
}

pub(super) struct SavedCleanupSnapshot {
    pub periodic: String,
    pub cron: String,
}

impl SavedCleanupSnapshot {
    fn unchanged(&self, actual: Option<(String, String)>) -> bool {
        actual.is_some_and(|(periodic, cron)| self.periodic == periodic && self.cron == cron)
    }
}

const LOCK_CURRENT_CLEANUP: &str = r#"
SELECT to_jsonb(p)::text, to_jsonb(cr)::text
FROM django_celery_beat_periodictask p
JOIN django_celery_beat_crontabschedule cr ON cr.id=p.crontab_id
WHERE p.id=$1 AND p.enabled
FOR NO KEY UPDATE OF p NOWAIT FOR SHARE OF cr NOWAIT
"#;

const WRITE_CURRENT_CLEANUP: &str = r#"
UPDATE django_celery_beat_periodictask
SET last_run_at=$2,total_run_count=$3,date_changed=$4
WHERE id=$1 AND enabled AND total_run_count=$5
"#;

pub(super) async fn publish_saved_cleanup(
    inner: &super::Inner,
    connection: &mut sqlx::PgConnection,
    stable: &SavedCleanupSnapshot,
    before: &Entry,
    owner: std::sync::Arc<super::BeatTickOwner>,
) -> Result<Option<uuid::Uuid>, Error> {
    use super::{prepare_publication, transport};
    use chrono::Utc;
    use sqlx::Connection;

    let selected = inner.saved_cleanup.as_ref().ok_or(Error::Supervisor)?;
    let mut transaction = connection.begin().await?;
    let current = sqlx::query_as::<_, (String, String)>(LOCK_CURRENT_CLEANUP)
        .bind(before.id)
        .fetch_optional(&mut *transaction)
        .await;
    let current = match current {
        Ok(current) => current,
        Err(error) => {
            transaction.rollback().await?;
            if error
                .as_database_error()
                .and_then(|database| database.code())
                .as_deref()
                == Some("55P03")
            {
                return Ok(None);
            }
            return Err(error.into());
        }
    };
    if !stable.unchanged(current) {
        transaction.rollback().await?;
        return Ok(None);
    }
    let mut after = before.clone();
    after.last_run_at = Utc::now();
    after.total_run_count = before
        .total_run_count
        .checked_add(1)
        .ok_or(Error::Capacity)?;
    let id = uuid::Uuid::new_v4();
    let frame = prepare_publication(&after, id, &inner.identity)?;
    selected.set_pending(Some(id));
    match inner.jobs.publish_normal_beat(&frame, owner).await {
        Ok(_) => {}
        Err(transport::Error::AmbiguousPublication(_)) => {
            let _ = transaction.rollback().await;
            return Err(Error::SavedCleanupUncertain(id));
        }
        Err(error) => {
            selected.set_pending(None);
            transaction.rollback().await?;
            return Err(Error::Transport(error));
        }
    }
    let checkpoint = sqlx::query(WRITE_CURRENT_CLEANUP)
        .bind(after.id)
        .bind(after.last_run_at)
        .bind(after.total_run_count)
        .bind(Utc::now())
        .bind(before.total_run_count)
        .execute(&mut *transaction)
        .await;
    if !checkpoint.is_ok_and(|updated| updated.rows_affected() == 1) {
        let _ = transaction.rollback().await;
        return Err(Error::SavedCleanupUncertain(id));
    }
    if transaction.commit().await.is_err() {
        return Err(Error::SavedCleanupUncertain(id));
    }
    selected.set_pending(None);
    Ok(Some(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::{json, Value};

    const OWNED_NAME: &str = "owned-restart-backend-cleanup";

    #[test]
    fn guard_requires_both_unchanged_row_and_cron() {
        let row = SavedCleanupSnapshot {
            periodic: "periodic".into(),
            cron: "cron".into(),
        };
        assert!(row.unchanged(Some(("periodic".into(), "cron".into()))));
        assert!(!row.unchanged(Some(("periodic changed".into(), "cron".into()))));
        assert!(!row.unchanged(Some(("periodic".into(), "cron changed".into()))));
        assert!(!row.unchanged(None));
    }

    #[test]
    fn tick_panic_becomes_supervisor_fault_without_losing_pending_identity() {
        let role = SavedCleanupSchedule::new(OWNED_NAME).unwrap();
        let task = uuid::Uuid::from_u128(47);
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let (physical, mut returned) =
            super::super::PhysicalPermit::new(slots.clone().try_acquire_owned().unwrap());
        let owner = std::sync::Arc::new(physical);
        let transport = owner.clone();
        let role_ref = &role;
        let future = observe_saved_cleanup_tick(async move {
            let _owner = owner;
            role_ref.set_pending(Some(task));
            panic!("test-owned scheduler tick");
        });
        let mut future = pin!(future);
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Err(Error::Supervisor))
        ));
        assert_eq!(role.pending(), Some(task));
        assert!(matches!(
            returned.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(slots.available_permits(), 0);
        drop(transport);
        drop(returned.try_recv().unwrap());
        assert_eq!(slots.available_permits(), 1);
    }

    fn saved_row() -> Entry {
        Entry {
            id: 17,
            name: OWNED_NAME.into(),
            task: CLEANUP_PERIODIC_TASK.into(),
            enabled: true,
            args: json!([]),
            kwargs: json!({}),
            last_run_at: Utc.with_ymd_and_hms(2026, 11, 21, 14, 21, 0).unwrap(),
            start_time: None,
            expires: None,
            one_off: false,
            headers: json!({}),
            queue: None,
            exchange: None,
            routing_key: None,
            priority: None,
            expire_seconds: Some(43_200),
            total_run_count: 0,
            interval: None,
            crontab: Some((
                "22".into(),
                "14".into(),
                "*".into(),
                "*".into(),
                "*".into(),
                "UTC".into(),
            )),
        }
    }

    #[test]
    fn retains_configured_name_and_admits_a_persisted_occurrence() {
        let scope = SavedCleanupSchedule::new(OWNED_NAME).unwrap();
        assert_eq!(scope.row_identity(), (OWNED_NAME, CLEANUP_PERIODIC_TASK));
        let mut entry = saved_row();
        assert!(scope.admits(&entry, None, None));
        entry.last_run_at = Utc.with_ymd_and_hms(2026, 11, 21, 14, 22, 1).unwrap();
        entry.total_run_count = 1;
        assert!(scope.admits(&entry, None, None));
        assert_eq!(entry.name, OWNED_NAME);
    }

    #[test]
    fn copied_name_is_bounded_and_kept_verbatim() {
        for invalid in [
            "",
            "  ",
            "a\0b",
            "a\nb",
            &"x".repeat(201),
            &"界".repeat(201),
        ] {
            assert!(SavedCleanupSchedule::new(invalid).is_err(), "{invalid:?}");
        }
        let accepted = "界".repeat(200);
        assert_eq!(
            SavedCleanupSchedule::new(&accepted)
                .unwrap()
                .row_identity()
                .0,
            accepted
        );
        assert_eq!(
            SavedCleanupSchedule::new("Owned cleanup")
                .unwrap()
                .row_identity()
                .0,
            "Owned cleanup"
        );
    }

    #[test]
    fn admits_only_the_selected_default_queue_cleanup_shape() {
        let scope = SavedCleanupSchedule::new(OWNED_NAME).unwrap();
        type Mutation = (&'static str, fn(&mut Entry));
        let cases: [Mutation; 21] = [
            ("stock alias", |entry| {
                entry.name = CLEANUP_PERIODIC_TASK.into()
            }),
            ("other task", |entry| {
                entry.task = "core.tasks.check_for_version_update".into()
            }),
            ("unpersisted", |entry| entry.id = 0),
            ("bad count", |entry| entry.total_run_count = -1),
            ("disabled", |entry| entry.enabled = false),
            ("interval", |entry| {
                entry.interval = Some((60, "seconds".into()))
            }),
            ("missing cron", |entry| entry.crontab = None),
            ("zone", |entry| {
                entry.crontab.as_mut().unwrap().5 = "America/Chicago".into()
            }),
            ("minute range", |entry| {
                entry.crontab.as_mut().unwrap().0 = "60".into()
            }),
            ("weekday", |entry| {
                entry.crontab.as_mut().unwrap().2 = "1".into()
            }),
            ("one-off", |entry| entry.one_off = true),
            ("starts", |entry| entry.start_time = Some(entry.last_run_at)),
            ("absolute expiry", |entry| {
                entry.expires = Some(entry.last_run_at)
            }),
            ("wrong expiry", |entry| entry.expire_seconds = None),
            ("queue override", |entry| {
                entry.queue = Some("celery".into())
            }),
            ("exchange override", |entry| {
                entry.exchange = Some(String::new())
            }),
            ("routing override", |entry| {
                entry.routing_key = Some("celery".into())
            }),
            ("priority override", |entry| entry.priority = Some(0)),
            ("positional", |entry| entry.args = json!([1])),
            ("named", |entry| entry.kwargs = json!({"x": true})),
            ("headers shape", |entry| entry.headers = Value::Null),
        ];
        for (label, change) in cases {
            let mut entry = saved_row();
            change(&mut entry);
            assert!(!scope.admits(&entry, None, None), "{label}");
        }
        let entry = saved_row();
        assert!(!scope.admits(&entry, Some(4), None));
        assert!(!scope.admits(&entry, None, Some(5)));
        assert!(!scope.admits(&entry, Some(4), Some(5)));
    }
}
