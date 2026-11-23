//! Actual normal Recording HTTP→jobs/control/selective media/audit ports.
//! Original Dispatcharr and dependency authors retain all application credit.
use dispatcharr_contracts::BoxFuture;
use dispatcharr_job_transport::{
    normal_control::{Control, Error as ControlError},
    normal_recording_publication::{Error, NormalRecordingPublisher},
};
use dispatcharr_store::{
    recording_http::{RecordingChannelId, RecordingEffectError as Effect, RecordingJobs},
    Store, StoreError,
};
use dispatcharr_stream_http::StreamEngine;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
pub fn port(
    store: Store,
    cache: String,
    publisher: NormalRecordingPublisher,
    control: Arc<Control>,
    engine: StreamEngine,
    dvr: Arc<dispatcharr_job_transport::recording_dvr_normal::NormalDvr>,
    series: Arc<dispatcharr_job_transport::synchronous_series::SynchronousSeries>,
) -> Arc<dyn RecordingJobs> {
    Arc::new(Bridge {
        store,
        cache,
        publisher,
        control,
        engine,
        dvr,
        series,
    })
}
struct Bridge {
    store: Store,
    cache: String,
    publisher: NormalRecordingPublisher,
    control: Arc<Control>,
    engine: StreamEngine,
    dvr: Arc<dispatcharr_job_transport::recording_dvr_normal::NormalDvr>,
    series: Arc<dispatcharr_job_transport::synchronous_series::SynchronousSeries>,
}
impl RecordingJobs for Bridge {
    fn evaluate_series(
        &self,
        tvg_id: Option<String>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<dispatcharr_store::series_evaluate::Report, StoreError>> {
        Box::pin(self.series.clone().evaluate(tvg_id, owner))
    }

    fn admit_scheduling(&self, owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            let _owner = owner;
            self.dvr
                .require_recording_domain()
                .map_err(|error| match error {
                    dispatcharr_job_transport::recording_dvr_normal::Error::Unsupported(_) => {
                        Effect::Unsupported
                    }
                    _ => Effect::Unsettled,
                })
        })
    }
    fn supports_task_session(&self) -> bool {
        true
    }
    fn schedule_from_task_on<'a>(
        &'a self,
        recording: i64,
        parent: dispatcharr_store::recording_http::RecordingChannelId,
        revoked_before_date_save: bool,
        connection: &'a mut dispatcharr_store::recording_http::RecordingTaskConnection,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Vec<String>, Effect>> {
        Box::pin(async move {
            self.admit_scheduling(owner.clone()).await?;
            let published = self
                .publisher
                .schedule_booking_child_on(
                    recording,
                    parent,
                    revoked_before_date_save,
                    connection,
                    owner,
                )
                .await
                .map_err(publication_error)?;
            let mut children = Vec::new();
            if let dispatcharr_job_transport::normal_recording_publication::BookingTask::Immediate{task_key}=published.task {children.push(task_key);}
            if let Some(artwork) = published.artwork {
                children.push(artwork.to_string());
            }
            Ok(children)
        })
    }
    fn revoke_on<'a>(
        &'a self,
        task: String,
        connection: &'a mut dispatcharr_store::recording_http::RecordingTaskConnection,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<(), Effect>> {
        Box::pin(async move {
            let deleted =
                dispatcharr_store::recording_beat_cleanup::Store::normal(self.store.pool().clone())
                    .delete_by_name_on(connection, &task, owner.clone())
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
    fn supports_task_signals(&self) -> bool {
        true
    }
    fn schedule_from_task(
        &self,
        recording: i64,
        parent: dispatcharr_store::recording_http::RecordingChannelId,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Vec<String>, Effect>> {
        Box::pin(async move {
            self.admit_scheduling(owner.clone()).await?;
            let published = self
                .publisher
                .schedule_booking_child(recording, parent, owner)
                .await
                .map_err(publication_error)?;
            let mut children = Vec::new();
            if let dispatcharr_job_transport::normal_recording_publication::BookingTask::Immediate { task_key } = published.task { children.push(task_key); }
            if let Some(artwork) = published.artwork {
                children.push(artwork.to_string());
            }
            Ok(children)
        })
    }
    fn schedule(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            self.admit_scheduling(owner.clone()).await?;
            self.publisher
                .schedule_booking(recording, owner)
                .await
                .map(|_| ())
                .map_err(publication_error)
        })
    }
    fn supports_recurring_booking(&self) -> bool {
        true
    }
    fn schedule_recurring(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            self.admit_scheduling(owner.clone()).await?;
            self.publisher
                .schedule_recurring_booking(recording, owner)
                .await
                .map(|_| ())
                .map_err(publication_error)
        })
    }
    fn supports_recurring_task_booking(&self) -> bool {
        true
    }
    fn schedule_recurring_from_task(
        &self,
        recording: i64,
        parent: RecordingChannelId,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Option<RecordingChannelId>, Effect>> {
        Box::pin(async move {
            self.admit_scheduling(owner.clone()).await?;
            let published = self
                .publisher
                .schedule_recurring_booking_child(recording, parent, owner)
                .await
                .map_err(publication_error)?;
            if !matches!(
                published.task,
                dispatcharr_job_transport::normal_recording_publication::BookingTask::Periodic { .. }
            ) {
                return Err(Effect::Unsettled);
            }
            Ok(published.artwork)
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
    fn revoke_series_observed_absent(
        &self,
        proof: dispatcharr_store::recording_http::RecordingTaskAbsent,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Effect>> {
        Box::pin(async move {
            self.control
                .revoke_one(proof.task_key(), owner)
                .await
                .map(|_| ())
                .map_err(series_revoke_error)
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
    fn stop_after_local_recording(
        &self,
        channel: RecordingChannelId,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Option<BoxFuture<'_, Result<(), Effect>>> {
        if !self.dvr.has_local_stop_handoff() {
            return None;
        }
        let request = self.dvr.begin_local_recording_stop(channel, recording);
        Some(Box::pin(async move {
            let _owner = owner;
            let Some(request) = request else {
                // Pending tasks are still revoked independently. A foreign
                // worker or an expired marker cannot authorize a forced close.
                return Ok(());
            };
            let channel = channel.to_string();
            let handoff = async {
                let clients = self
                    .engine
                    .snapshot_local_recording_clients(&channel, recording)
                    .await
                    .map_err(|_| Effect::Unsettled)?;
                // A replacement could have registered during the snapshot.
                if self.dvr.recording_stop_snapshot_is_current(&request) && request.settled().await
                {
                    self.engine
                        .stop_local_recording_clients(&channel, clients)
                        .await
                        .map_err(|_| Effect::Unsettled)?;
                }
                Ok(())
            };
            match tokio::time::timeout(Duration::from_secs(12), handoff).await {
                Ok(result) => result,
                Err(_) => {
                    eprintln!("Local DVR stop handoff exceeded its deadline");
                    Ok(())
                }
            }
        }))
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
        Error::Cleanup(error) => cleanup_error(*error),
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

fn series_revoke_error(error: ControlError) -> Effect {
    match error {
        // These errors return before the control writer is invoked. Original
        // treats a failed immediate revoke as best effort in this collector.
        ControlError::Capacity | ControlError::Closed | ControlError::NotReady => {
            Effect::SettledFailure
        }
        // Control maps all writer-command errors to Io. A late publish can
        // still happen, so do not swallow that error or send a second revoke.
        _ => Effect::Unsettled,
    }
}

#[cfg(test)]
mod series_revoke_tests {
    use super::*;
    use dispatcharr_job_transport::normal_revocation::Revoked;

    #[tokio::test]
    async fn series_revoke_continues_only_on_proven_early_nonpublication() {
        // This constructor only parses the URL. The test never starts the
        // control listener or establishes a broker connection.
        let control = Control::connect("redis://127.0.0.1:1/0", Revoked::new())
            .await
            .unwrap();
        let not_ready = control
            .revoke_one("owned-recording-revoke", Arc::new(()))
            .await
            .unwrap_err();
        assert_eq!(not_ready, ControlError::NotReady);
        assert!(control.shutdown(Duration::from_secs(1)).await);
        let closed = control
            .revoke_one("owned-recording-revoke", Arc::new(()))
            .await
            .unwrap_err();
        assert_eq!(closed, ControlError::Closed);
        for no_publish in [not_ready, closed, ControlError::Capacity] {
            assert!(matches!(
                series_revoke_error(no_publish),
                Effect::SettledFailure
            ));
        }
        for unknown in [
            ControlError::Io,
            ControlError::Protocol,
            ControlError::Timeout,
        ] {
            assert!(matches!(series_revoke_error(unknown), Effect::Unsettled));
        }
    }
}
