//! Provider account info/profile roots over existing Main shared Jobs ports.
use super::*;
use crate::normal_xc_accounts::{parse, Message};
pub(crate) struct XcAccountCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn peek_xc_account(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<XcAccountCandidate>, Error> {
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
        Ok(Some(XcAccountCandidate {
            message: parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
            raw: reply.1,
            origin: self.inner.clone(),
        }))
    }
    pub(crate) async fn claim_xc_account(
        &self,
        candidate: XcAccountCandidate,
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
            Ok((1, _)) => Ok(candidate.message),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write_xc_account(
        &self,
        message: &Message,
        result: serde_json::Value,
        revoked: bool,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        let status = if revoked { "REVOKED" } else { "SUCCESS" };
        let result = if revoked {
            serde_json::json!({"exc_type":"TaskRevokedError","exc_message":["revoked"],"exc_module":"celery.exceptions"})
        } else {
            result
        };
        let children = serde_json::json!([]);
        let bytes=serde_json::to_vec(&serde_json::json!({"status":status,"result":result,"traceback":null,"children":children,"date_done":chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),"task_id":message.id.to_string()})).map_err(|_|Error::Protocol)?;
        let mut value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)?;
        if let Some(parent) = message.parent {
            value["parent_id"] = serde_json::json!(parent.to_string());
        }
        self.write_metadata(
            message.id,
            serde_json::to_vec(&value).map_err(|_| Error::Protocol)?,
            owner,
        )
        .await
    }
}

impl Jobs {
    pub(crate) async fn publish_xc_info(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = parse(raw).map_err(|_| Error::Protocol)?;
        if message.kind != crate::normal_xc_accounts::Kind::Info {
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
