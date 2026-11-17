//! One concrete manual comskip root over the existing Main shared Jobs ports.
//! Original Dispatcharr/Celery/Kombu authors retain protocol/application credit.
use super::*;
use crate::normal_comskip::{parse, Message};
pub(crate) struct ComskipCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn peek_comskip_task(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<ComskipCandidate>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        if let Some(expected) = expected {
            if expected.is_empty() || reply.0 != 1 || reply.1 != expected {
                return Err(Error::ClaimRace);
            }
        }
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        Ok(Some(ComskipCandidate {
            message: parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
        }))
    }
    pub(crate) async fn claim_comskip_task(
        &self,
        candidate: ComskipCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !self.allows_defaults() || !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::UnsupportedTask);
        }
        let id = candidate.message.id;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[&candidate.raw],
                self.lease(owner)?,
            )
            .await
        {
            Ok((1, _)) => Ok(candidate.message),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_comskip_task(
        &self,
        message: &Message,
        result: serde_json::Value,
        revoked: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let (status, result, traceback) = if revoked {
            (
                "REVOKED",
                serde_json::json!({"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"}),
                serde_json::Value::Null,
            )
        } else {
            ("SUCCESS", result, serde_json::Value::Null)
        };
        let mut metadata = serde_json::json!({"status":status,"result":result,"traceback":traceback,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id.to_string()});
        if let Some(parent) = &message.parent {
            metadata["parent_id"] = serde_json::json!(parent);
        }
        let bytes = serde_json::to_vec(&metadata).map_err(|_| Error::Protocol)?;
        crate::decode::decode(&bytes, message.id).map_err(|_| Error::Protocol)?;
        self.write_metadata(message.id, bytes, owner).await
    }
}
impl Jobs {
    pub(crate) async fn publish_comskip_task(
        &self,
        id: uuid::Uuid,
        bytes: Vec<u8>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if !self.allows_defaults() || parse(&bytes).map_err(|_| Error::Protocol)?.id != id {
            return Err(Error::UnsupportedTask);
        }
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[&bytes, limit.as_bytes()],
                self.lease(owner)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(e) => Err(map(e)),
        }
    }
}
