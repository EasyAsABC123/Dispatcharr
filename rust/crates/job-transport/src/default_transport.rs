//! UNCOMPILED private child module, fixture authority only. Original
//! Dispatcharr/Celery/Kombu authors retain application/protocol credit/licenses.
use super::*;
use crate::default_protocol::{self, Message, Normal};

// Non-cloneable candidate owns a Jobs operation permit from BEFORE peek until
// claim/drop. The permit also owns its caller, and the real Redis command keeps
// the same Arc until physical completion after caller cancellation/timeout.
// Thus retained candidate count is bounded by existing config.operations even
// for a caller outside the sole admitted worker. There is no second pool whose
// repeated constructors can bypass the shared bound.
pub struct Candidate {
    pub message: Message,
    bytes: Vec<u8>,
    origin: Arc<Inner>,
    held: Arc<Lease>,
}
impl Jobs {
    fn require_default(&self) -> Result<(), Error> {
        if self.allows_defaults() {
            Ok(())
        } else {
            Err(Error::UnsupportedTask)
        }
    }
    pub async fn publish_default(
        &self,
        message: &Message,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        self.require_default()?;
        let bytes = default_protocol::encode(message).map_err(|_| Error::Protocol)?;
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[&bytes, limit.as_bytes()],
                self.lease(caller)?,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: message.task_id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub async fn peek_default(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Candidate>, Error> {
        self.require_default()?;
        let held = Arc::new(self.lease(owner)?);
        let (reply, _physical) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                held.clone(),
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let message =
                    default_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
                Ok(Some(Candidate {
                    message,
                    bytes: reply.1,
                    origin: self.inner.clone(),
                    held,
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn claim_default(&self, candidate: Candidate) -> Result<Message, Error> {
        self.require_default()?;
        if !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::Configuration);
        }
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let id = candidate.message.task_id;
        // Reuse candidate permit: capacity1 can claim its own retained peek.
        // Physical Redis supervision keeps this Arc after a logical timeout.
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[&candidate.bytes],
                candidate.held.clone(),
            )
            .await
        {
            Ok((1, _)) => Ok(candidate.message),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(id)),
            Err(error) => Err(map(error)),
        }
    }
    pub async fn write_default_success(
        &self,
        message: &Message,
        result: Normal,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.require_default()?;
        let value = result.value(message.kind).map_err(|_| Error::Protocol)?;
        self.write_result(message.task_id, value, None, Vec::new(), owner)
            .await
    }
}
