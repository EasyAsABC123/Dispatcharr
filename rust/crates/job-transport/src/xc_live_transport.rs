//! Exact authenticated broker read/CAS and actual settled XC child publication.
//! Original Celery/Kombu/Dispatcharr authors retain protocol credit.
use super::*;
use crate::normal_xc_live::{parse, Message};
pub(crate) struct XcLiveCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
    _held: Lease,
}
impl Jobs {
    pub(crate) async fn peek_xc_live(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<XcLiveCandidate>, Error> {
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
        if expected.is_some_and(|v| v.is_empty() || reply.0 != 1 || v != reply.1) {
            return Err(Error::ClaimRace);
        }
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        Ok(Some(XcLiveCandidate {
            message: parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
            _held: held,
        }))
    }
    pub(crate) async fn claim_xc_live(
        &self,
        candidate: XcLiveCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !Arc::ptr_eq(&self.inner, &candidate.origin) {
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
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_xc_live(
        &self,
        message: &Message,
        result: serde_json::Value,
        children: Vec<Uuid>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.write_result(message.id, result, message.parent, children, owner)
            .await
    }
    pub(crate) async fn publish_xc_profile_child(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = crate::normal_xc_accounts::parse(raw).map_err(|_| Error::Protocol)?;
        if message.kind != crate::normal_xc_accounts::Kind::Profiles || message.parent.is_none() {
            return Err(Error::Protocol);
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
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: message.id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.id)),
            Err(e) => Err(map(e)),
        }
    }
}

impl Jobs {
    pub(crate) async fn write_xc_live_revoked(
        &self,
        message: &Message,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let mut metadata = serde_json::json!({"status":"REVOKED","result":{"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"},"traceback":null,"children":[],"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id});
        if let Some(parent) = message.parent {
            metadata["parent_id"] = serde_json::json!(parent)
        }
        self.write_metadata(
            message.id,
            serde_json::to_vec(&metadata).map_err(|_| Error::Protocol)?,
            owner,
        )
        .await
    }
    pub(crate) async fn publish_xc_live_root(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = parse(raw).map_err(|_| Error::Protocol)?;
        if message.parent.is_some() {
            return Err(Error::Protocol);
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
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: message.id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.id)),
            Err(e) => Err(map(e)),
        }
    }
}
