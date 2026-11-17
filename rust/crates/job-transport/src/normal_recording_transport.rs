//! Typed normal DVR publication only. Original Dispatcharr/Celery credit retained.
use super::*;
impl Jobs {
    pub(crate) async fn publish_normal_recording(
        &self,
        message: &crate::clocked_recording_envelope::ObservedProducerEnvelope,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        if crate::clocked_recording_envelope::parse_normal_head(message.raw_frame())
            .map_err(|_| Error::Protocol)?
            != *message
        {
            return Err(Error::Protocol);
        }
        let limit = self.inner.config.max_queued_jobs.to_string();
        let diagnostic = (std::env::var("RUST_AUTO_STAGE_DIAGNOSTICS").as_deref() == Ok("1"))
            .then_some(
                dispatcharr_output_store::transport::CommandDiagnostic::RecordingPublication {
                    task: message.task_id().as_u128(),
                },
            );
        let (reply, _held) = self
            .inner
            .broker
            .command_publication_classified_with_diagnostic::<i64, _>(
                PUBLISH,
                &["dvr".into(), "_kombu.binding.dvr".into()],
                &[message.raw_frame(), limit.as_bytes()],
                self.lease(owner)?,
                diagnostic,
            )
            .await
            .map_err(|error| match error {
                ClassifiedError::Backend => Error::AmbiguousPublication(message.task_id()),
                other => map(other),
            })?;
        match reply {
            n if n > 0 => Ok(Published {
                task_id: message.task_id(),
                queue_length: n as u64,
            }),
            -2 => Err(Error::Capacity),
            _ => Err(Error::Corrupt),
        }
    }
}

/// Sealed authenticated Redis observation. It is not a producer publication
/// receipt: a surviving Source frame need not have a live/finished parent.
pub(crate) struct ExistingRecordingHead {
    raw: Vec<u8>,
}
impl ExistingRecordingHead {
    pub(crate) fn confirms(&self, raw: &[u8]) -> bool {
        self.raw == raw
    }
}
impl Jobs {
    pub(crate) async fn observe_existing_recording(
        &self,
        expected: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<ExistingRecordingHead, Error> {
        if !self.allows_recording_dvr() || expected.is_empty() || expected.len() > 65536 {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["dvr".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        if reply.0 != 1 || reply.1 != expected {
            return Err(Error::ClaimRace);
        }
        Ok(ExistingRecordingHead { raw: reply.1 })
    }
}
impl Jobs {
    pub(crate) async fn peek_existing_recording(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["dvr".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(reply.1)),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn write_normal_recording_revoked(
        &self,
        message: &crate::clocked_recording_envelope::ObservedProducerEnvelope,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        let value = serde_json::json!({"status":"REVOKED","result":{"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"},"traceback":null,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.task_id().to_string()});
        self.write_metadata(
            message.task_id(),
            serde_json::to_vec(&value).map_err(|_| Error::Protocol)?,
            owner,
        )
        .await
    }
}

#[cfg(test)]
#[path = "normal_recording_publication_readiness_tests.rs"]
mod publication_readiness_tests;
