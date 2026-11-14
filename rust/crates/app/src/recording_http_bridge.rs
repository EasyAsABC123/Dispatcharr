//! Actual normal Recording HTTP→jobs/control/selective media/audit ports.
//! Original Dispatcharr and dependency authors retain all application credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_job_transport::{
    normal_control::Control,
    normal_recording_publication::{Error, NormalRecordingPublisher},
};
use dispatcharr_store::{
    recording_http::{RecordingChannelId, RecordingEffectError as Effect, RecordingJobs},
    Store,
};
use dispatcharr_stream_http::StreamEngine;
use serde_json::Value;
use std::sync::Arc;
pub fn port(
    store: Store,
    cache: String,
    publisher: NormalRecordingPublisher,
    control: Arc<Control>,
    engine: StreamEngine,
) -> Arc<dyn RecordingJobs> {
    Arc::new(Bridge {
        store,
        cache,
        publisher,
        control,
        engine,
    })
}
struct Bridge {
    store: Store,
    cache: String,
    publisher: NormalRecordingPublisher,
    control: Arc<Control>,
    engine: StreamEngine,
}
impl RecordingJobs for Bridge {
    fn schedule(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            self.publisher
                .schedule_booking(recording, owner)
                .await
                .map(|_| ())
                .map_err(publication_error)
        })
    }
    fn revoke(
        &self,
        task: String,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            let deleted = Arc::new(dispatcharr_store::recording_beat_cleanup::Store::normal(
                self.store.pool().clone(),
            ))
            .delete_by_name(task.clone(), owner.clone())
            .await
            .map_err(cleanup_error)?;
            if !deleted {
                self.control
                    .revoke_one(&task, owner)
                    .await
                    .map_err(|_| Effect::Unsettled)?;
            }
            Ok(())
        })
    }
    fn revoke_observed_absent(
        &self,
        proof: dispatcharr_store::recording_http::RecordingTaskAbsent,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        // Collector still owns its SQL transaction. Its private proof records
        // actual same-connection absence; no second pool acquire/deletion can
        // deadlock pool1 or erase a newly-created unrelated schedule.
        Box::pin(async move {
            self.control
                .revoke_one(proof.task_key(), owner)
                .await
                .map(|_| ())
                .map_err(|_| Effect::Unsettled)
        })
    }
    fn stop_clients(
        &self,
        channel: RecordingChannelId,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            self.engine
                .stop_recording_clients(&channel.to_string(), recording, owner)
                .await
                .map(|_| ())
                .map_err(|_| Effect::Unsettled)
        })
    }
    fn dvr_settings(&self, owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<Value, Effect>> {
        Box::pin(async move {
            self.publisher
                .booking_dvr_settings(self.store.clone(), &self.cache, owner)
                .await
                .map_err(publication_error)
        })
    }
    fn cancelled_event(
        &self,
        details: Value,
        channel: RecordingChannelId,
        name: String,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            self.publisher
                .booking_cancelled_event(
                    self.store.clone(),
                    &self.cache,
                    details,
                    channel,
                    name,
                    owner,
                )
                .await
                .map_err(publication_error)
        })
    }
}
fn publication_error(error: Error) -> Effect {
    match error {
        Error::Database {
            primary: Some(source),
            ready: None,
            close: None,
        } if source.as_database_error().is_some() => Effect::SettledFailure,
        Error::Audit(error) => match *error {
            dispatcharr_store::recording_capture::Error::Sql {
                source,
                settlement: None,
                ..
            } if source.as_database_error().is_some() => Effect::SettledFailure,
            dispatcharr_store::recording_capture::Error::Unsupported(_) => Effect::Unsupported,
            _ => Effect::Unsettled,
        },
        Error::Unsupported(_) => Effect::Unsupported,
        _ => Effect::Unsettled,
    }
}
fn cleanup_error(error: dispatcharr_store::recording_beat_cleanup::Error) -> Effect {
    use dispatcharr_store::recording_beat_cleanup::{Commit, Error};
    match error {
        Error::Unsupported(_) => Effect::Unsupported,
        Error::Sql {
            source,
            prefix,
            rollback: None,
            ready: None,
            close: None,
            ..
        } if source.as_database_error().is_some()
            && prefix.periodic != Commit::Unknown
            && prefix.override_touch != Commit::Unknown
            && prefix.clocked != Commit::Unknown =>
        {
            Effect::SettledFailure
        }
        _ => Effect::Unsettled,
    }
}
