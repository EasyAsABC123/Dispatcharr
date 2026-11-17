//! Actual named DVR-only broker/result operations, with unchanged named keys.
//! Original Dispatcharr/Celery/Redis authors retain all credit.
use super::*;
use crate::normal_named_recording::{self, Message};
pub(crate) struct NamedPublicationReply {
    task_key: String,
}
impl NamedPublicationReply {
    pub(crate) fn confirms(&self, message: &Message) -> bool {
        self.task_key == message.task_key()
    }
}
pub(crate) struct NamedDvrCandidate {
    pub message: Message,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn publish_named_recording(
        &self,
        message: &Message,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<NamedPublicationReply, Error> {
        if !self.allows_recording_dvr()
            || normal_named_recording::parse_head(message.raw_frame())
                .map_err(|_| Error::Protocol)?
                != *message
        {
            return Err(Error::UnsupportedTask);
        }
        let limit = self.inner.config.max_queued_jobs.to_string();
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["dvr".into(), "_kombu.binding.dvr".into()],
                &[message.raw_frame(), limit.as_bytes()],
                self.lease(owner)?,
            )
            .await
            .map_err(|error| match error {
                ClassifiedError::Backend => {
                    Error::AmbiguousNamedRecordingPublication(message.recording_id())
                }
                other => map(other),
            })?;
        match reply {
            value if value > 0 => Ok(NamedPublicationReply {
                task_key: message.task_key().to_owned(),
            }),
            -2 => Err(Error::Capacity),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn peek_named_recording_dvr(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<NamedDvrCandidate>, Error> {
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
            0 if expected.is_some() => Err(Error::ClaimRace),
            0 => Ok(None),
            1 if expected.is_some_and(|raw| raw != reply.1.as_slice()) => Err(Error::ClaimRace),
            1 => Ok(Some(NamedDvrCandidate {
                message: normal_named_recording::parse_head(&reply.1)
                    .map_err(|_| Error::UnsupportedTask)?,
                origin: self.inner.clone(),
            })),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_named_recording_dvr(
        &self,
        candidate: NamedDvrCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_recording_dvr() || !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::UnsupportedTask);
        }
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["dvr".into()],
                &[candidate.message.raw_frame()],
                self.lease(owner)?,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousNamedRecordingClaim(
                candidate.message.recording_id(),
            )),
            Err(other) => Err(map(other)),
        }
    }
    pub(crate) async fn write_named_recording_dvr(
        &self,
        message: &Message,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        self.write_named_recording_dvr_with_children(message, vec![], owner)
            .await
    }
    pub(crate) async fn write_named_recording_dvr_with_children(
        &self,
        message: &Message,
        children: Vec<uuid::Uuid>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        let children: Vec<_> = children
            .into_iter()
            .map(|child| serde_json::json!([[child.to_string(), null], null]))
            .collect();
        let mut value = serde_json::json!({"status":"SUCCESS","result":null,"traceback":null,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.task_key()});
        if let Some(parent) = message.parent() {
            value["parent_id"] = serde_json::json!(parent);
        }
        let bytes = serde_json::to_vec(&value).map_err(|_| Error::Protocol)?;
        let key = format!("celery-tasks:celery-task-meta-{}", message.task_key());
        match self
            .inner
            .result
            .command_classified::<i64, _>(WRITE_SUCCESS, &[key], &[&bytes], self.lease(owner)?)
            .await
        {
            Ok((1, _)) => Ok(true),
            Ok((0, _)) => Ok(false),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousNamedRecordingResult(message.recording_id()))
            }
            Err(other) => Err(map(other)),
        }
    }
}
impl Jobs {
    pub(crate) async fn write_named_recording_revoked(
        &self,
        message: &Message,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        let mut value = serde_json::json!({"status":"REVOKED","result":{"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"},"traceback":null,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.task_key()});
        if let Some(parent) = message.parent() {
            value["parent_id"] = serde_json::json!(parent);
        }
        let bytes = serde_json::to_vec(&value).map_err(|_| Error::Protocol)?;
        match self
            .inner
            .result
            .command_classified::<i64, _>(
                WRITE_SUCCESS,
                &[format!(
                    "celery-tasks:celery-task-meta-{}",
                    message.task_key()
                )],
                &[&bytes],
                self.lease(owner)?,
            )
            .await
        {
            Ok((1, _)) => Ok(true),
            Ok((0, _)) => Ok(false),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousNamedRecordingResult(message.recording_id()))
            }
            Err(error) => Err(map(error)),
        }
    }
}
