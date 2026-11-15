//! Opaque actual raw-head/CAS authority for normal standalone auto-sync tasks.
use super::*;
use crate::sd_guide_protocol::{parse, Message};
pub(crate) struct SdGuideCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
    _held: Lease,
}
impl SdGuideCandidate {
    pub(crate) fn queued_bytes(&self) -> &[u8] {
        &self.raw
    }
}
impl Jobs {
    /// Actual scoped Context trail metadata ONLY. No contained UUID becomes an
    /// execution/parent-publication authority. Same existing result lease,
    /// SUCCESS preservation/EX3600/PUBLISH and explicit legacy wire budget.
    pub(crate) async fn write_sd_hook_success(
        &self,
        message: &crate::normal_xc_live::Message,
        result: serde_json::Value,
        children: serde_json::Value,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !children.is_array() {
            return Err(Error::Protocol);
        }
        let mut meta = serde_json::json!({"status":"SUCCESS","result":result,"traceback":null,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()});
        if let Some(parent) = message.parent {
            meta["parent_id"] = serde_json::json!(parent.to_string());
        }
        meta["task_id"] = serde_json::json!(message.id.to_string());
        let bytes = serde_json::to_vec(&meta).map_err(|_| Error::Protocol)?;
        self.write_metadata(message.id, bytes, owner).await
    }
    pub(crate) async fn peek_sd_tvg_head(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Vec<u8>>, Error> {
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
        match reply.0 {
            0 => Ok(None),
            1 if reply.1.len() <= crate::protocol::MAX_ENVELOPE => Ok(Some(reply.1)),
            1 => Err(Error::UnsupportedTask),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) fn same_sd_guide_scope(&self, other: &Jobs) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub(crate) async fn peek_sd_guide(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<SdGuideCandidate>, Error> {
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
        if expected
            .is_some_and(|expected| expected.is_empty() || reply.0 != 1 || expected != reply.1)
        {
            return Err(Error::ClaimRace);
        }
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(SdGuideCandidate {
                message: parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
                raw: reply.1,
                origin: self.inner.clone(),
                _held: held,
            })),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_sd_guide(
        &self,
        candidate: SdGuideCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !Arc::ptr_eq(&candidate.origin, &self.inner) {
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
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousClaim(candidate.message.common.id))
            }
            Err(error) => Err(map(error)),
        }
    }
}

impl Jobs {
    pub(crate) async fn publish_sd_guide(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = crate::sd_guide_protocol::parse(raw).map_err(|_| Error::Protocol)?;
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
                task_id: message.common.id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.common.id)),
            Err(e) => Err(map(e)),
        }
    }
}
