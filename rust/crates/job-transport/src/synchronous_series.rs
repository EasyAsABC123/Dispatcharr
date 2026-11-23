//! Normal inline Series evaluation and model-signal publication.
//! Original Dispatcharr, Celery and Django contributors retain behavior credit.
use crate::{
    normal_recording_publication::{Error, NormalRecordingPublisher},
    recording_dvr_normal::NormalDvr,
};
use dispatcharr_contracts::{BoxFuture, UpdateEventSink};
use dispatcharr_coordination::{
    TaskLockCleanupOutcome, TaskLockDetailedError, TaskLockError, TaskLocks,
};
use dispatcharr_store::{
    series_evaluate::{self, Effects, Owner, Report, SignalOutcome},
    Store, StoreError,
};
use serde_json::json;
use std::sync::Arc;

pub struct SynchronousSeries {
    store: Store,
    cache: String,
    locks: TaskLocks,
    publisher: NormalRecordingPublisher,
    events: Arc<dyn UpdateEventSink>,
    dvr: Arc<NormalDvr>,
}

impl SynchronousSeries {
    pub fn new(
        store: Store,
        cache: String,
        locks: TaskLocks,
        publisher: NormalRecordingPublisher,
        events: Arc<dyn UpdateEventSink>,
        dvr: Arc<NormalDvr>,
    ) -> Self {
        Self {
            store,
            cache,
            locks,
            publisher,
            events,
            dvr,
        }
    }

    /// Retain the authenticated caller through lock cleanup and all real effects.
    pub async fn evaluate(
        self: Arc<Self>,
        tvg_id: Option<String>,
        owner: Owner,
    ) -> Result<Report, StoreError> {
        tokio::spawn(async move {
            let lease = match self
                .locks
                .acquire_series_rules_retained(owner.clone())
                .await
            {
                Ok(lease) => Some(lease),
                Err(TaskLockDetailedError::Operational(TaskLockError::Held)) => {
                    return Ok(Report::busy());
                }
                Err(error) if settled_io_loss(&error) => None,
                Err(_) => return Err(unsettled()),
            };
            let result = async {
                let group = self
                    .publisher
                    .booking_dvr_settings(self.store.clone(), &self.cache, owner.clone())
                    .await
                    .map_err(|_| unsettled())?;
                series_evaluate::evaluate_locked(
                    &self.store,
                    self.as_ref(),
                    &group,
                    tvg_id.as_deref(),
                    chrono::Utc::now(),
                    owner,
                )
                .await
            }
            .await;
            if let Some(lease) = lease {
                match lease.release_owned_detailed().await {
                    Ok(_) => {}
                    Err(TaskLockDetailedError::Redis(cause)) if cause.io_kind.is_some() => {}
                    Err(_) => return Err(unsettled()),
                }
            }
            result
        })
        .await
        .map_err(|_| StoreError::UnhandledRestFault)?
    }
}

fn settled_io_loss(error: &TaskLockDetailedError) -> bool {
    match error {
        TaskLockDetailedError::Redis(cause) => cause.io_kind.is_some(),
        TaskLockDetailedError::Acquisition { cause, cleanup } => {
            cause.io_kind.is_some()
                && matches!(
                    cleanup,
                    TaskLockCleanupOutcome::Released | TaskLockCleanupOutcome::NotOwner
                )
        }
        _ => false,
    }
}

fn unsettled() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"error":"Series evaluation physically unsettled"}),
    )
}

fn signal<T>(result: Result<T, Error>) -> Result<SignalOutcome, StoreError> {
    match result {
        Ok(_) => Ok(SignalOutcome::Settled),
        Err(Error::Database {
            primary: Some(error),
            ready: None,
            close: None,
        }) if error.as_database_error().is_some() => {
            // Original catches physically completed signal errors after INSERT.
            Ok(SignalOutcome::SourceCaught)
        }
        Err(Error::Unsupported(message)) => Err(StoreError::Unsupported(message)),
        Err(_) => Err(unsettled()),
    }
}

impl Effects for SynchronousSeries {
    fn admit_booking(&self) -> Result<(), StoreError> {
        self.dvr
            .require_recording_domain()
            .map_err(|error| match error {
                crate::recording_dvr_normal::Error::Unsupported(message) => {
                    StoreError::Unsupported(message)
                }
                _ => unsettled(),
            })
    }

    fn booking(
        &self,
        recording: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>> {
        Box::pin(async move { signal(self.publisher.schedule_booking(recording, owner).await) })
    }

    fn explicit_artwork(
        &self,
        recording: i64,
        owner: Owner,
    ) -> BoxFuture<'_, Result<SignalOutcome, StoreError>> {
        Box::pin(async move {
            signal(
                self.publisher
                    .publish_series_evaluation_artwork(recording, owner)
                    .await,
            )
        })
    }

    fn refresh(&self, scheduled: usize, owner: Owner) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let (finished, settled) = tokio::sync::oneshot::channel();
            let receipt = Arc::new(EventReceipt {
                finished: Some(finished),
                _owner: owner,
            });
            let _ = self
                .events
                .send_retained(
                    "updates",
                    json!({"type":"update","data":{
                        "success":true,"type":"recordings_refreshed","scheduled":scheduled
                    }}),
                    receipt.clone(),
                )
                .await;
            drop(receipt);
            let _ = settled.await;
        })
    }
}

struct EventReceipt {
    finished: Option<tokio::sync::oneshot::Sender<()>>,
    _owner: Owner,
}

impl Drop for EventReceipt {
    fn drop(&mut self) {
        if let Some(finished) = self.finished.take() {
            let _ = finished.send(());
        }
    }
}
