//! Opaque actual raw-head/CAS authority for normal standalone auto-sync tasks.
use super::*;
use crate::auto_sync_protocol::{parse, Message};
pub(crate) struct AutoCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
    _held: Lease,
}
impl Jobs {
    pub(crate) async fn peek_auto_sync(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<AutoCandidate>, Error> {
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
            1 => Ok(Some(AutoCandidate {
                message: parse(&reply.1).map_err(|_| Error::UnsupportedTask)?,
                raw: reply.1,
                origin: self.inner.clone(),
                _held: held,
            })),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_auto_sync(
        &self,
        candidate: AutoCandidate,
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
