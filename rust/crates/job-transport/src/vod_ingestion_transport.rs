//! Actual default-queue VOD read/CAS/publication/backend over shared Jobs.
//! Original Dispatcharr/Celery/Kombu authors retain application credit.
use super::*;
use crate::vod_ingestion_protocol::{self as vod, Message};
pub(crate) struct VodCandidate {
    pub message: Message,
    pub raw: Vec<u8>,
    origin: Arc<Inner>,
    _held: Lease,
}
impl Jobs {
    pub(crate) async fn peek_vod(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<VodCandidate>, Error> {
        if !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
        let (reply, held) = self
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
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1
            || expected.is_some_and(|expected| expected.is_empty() || expected != reply.1)
        {
            return Err(Error::ClaimRace);
        }
        Ok(Some(VodCandidate {
            message: vod::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
            _held: held,
        }))
    }
    pub(crate) async fn claim_vod(
        &self,
        candidate: VodCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !Arc::ptr_eq(&self.inner, &candidate.origin) || !self.allows_defaults() {
            return Err(Error::UnsupportedTask);
        }
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
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn publish_vod(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        self.publish_vod_checked(raw, None, owner).await
    }

    pub(crate) async fn publish_vod_child(
        &self,
        raw: &[u8],
        parent: Uuid,
        root: Uuid,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        self.publish_vod_checked(raw, Some((parent, root)), owner)
            .await
    }

    async fn publish_vod_checked(
        &self,
        raw: &[u8],
        lineage: Option<(Uuid, Uuid)>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = vod::parse(raw).map_err(|_| Error::Protocol)?;
        let actual_lineage = message.parent.map(|parent| (parent, message.root));
        if !self.allows_defaults() || actual_lineage != lineage {
            return Err(Error::UnsupportedTask);
        }
        let maximum = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[raw, maximum.as_bytes()],
                self.lease(owner)?,
            )
            .await
        {
            Ok((length, _)) if length > 0 => Ok(Published {
                task_id: message.id,
                queue_length: length as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_vod(
        &self,
        message: &Message,
        result: serde_json::Value,
        revoked: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !revoked {
            return self
                .write_result(message.id, result, message.parent, Vec::new(), owner)
                .await;
        }
        let mut metadata = serde_json::json!({"status":"REVOKED","result":{"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"},"traceback":null,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id});
        if let Some(parent) = message.parent {
            metadata["parent_id"] = serde_json::json!(parent);
        }
        let bytes = serde_json::to_vec(&metadata).map_err(|_| Error::Protocol)?;
        crate::decode::decode(&bytes, message.id).map_err(|_| Error::Protocol)?;
        self.write_metadata(message.id, bytes, owner).await
    }
}
