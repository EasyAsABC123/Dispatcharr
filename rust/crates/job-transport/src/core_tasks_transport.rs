//! Two concrete Core-produced roots over the existing Main shared Jobs ports.
use super::*;
use crate::normal_core_tasks::{parse, Message};
pub(crate) struct CoreCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn peek_core_task(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<CoreCandidate>, Error> {
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
        Ok(Some(CoreCandidate {
            message: parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
        }))
    }
    pub(crate) async fn claim_core_task(
        &self,
        candidate: CoreCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Message, Error> {
        if !Arc::ptr_eq(&self.inner, &candidate.origin) {
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
            Ok((1, _)) => {
                if crate::normal_core_tasks::owned_provider_rehash(&candidate.message.kind) {
                    self.inner.provider_rehash_claim.claimed(id, &candidate.raw);
                }
                Ok(candidate.message)
            }
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_core_task(
        &self,
        message: &Message,
        result: serde_json::Value,
        children: Vec<String>,
        revoked: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let status = if revoked { "REVOKED" } else { "SUCCESS" };
        let result = if revoked {
            serde_json::json!({"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"})
        } else {
            result
        };
        let children: Vec<serde_json::Value> = children
            .into_iter()
            .map(|id| serde_json::json!([[id, null], null]))
            .collect();
        let bytes=serde_json::to_vec(&serde_json::json!({"status":status,"result":result,"traceback":null,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id.to_string()})).map_err(|_|Error::Protocol)?;
        self.write_metadata(message.id, bytes, owner).await
    }
}
