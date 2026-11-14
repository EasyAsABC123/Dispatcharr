//! PRIVATE UNCOMPILED explicit DVR-only transport. Parent module registration
//! and Authority::Dvr are proposal hunks only. Legacy/default intake unchanged.
use super::*;
use crate::clocked_recording_envelope::{self, ObservedProducerEnvelope};
pub(crate) struct DvrCandidate {
    pub message: ObservedProducerEnvelope,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn connect_recording_dvr(
        broker: &str,
        result: &str,
        config: Config,
    ) -> Result<Self, Error> {
        Self::connect_owned(broker, result, config, String::new(), Authority::Dvr).await
    }
    pub(crate) fn allows_recording_dvr(&self) -> bool {
        self.inner.authority == Authority::Dvr
    }
    pub(crate) async fn peek_recording_dvr(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<DvrCandidate>, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(owner)?;
        let (reply, _held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &["dvr".into()], &[], lease)
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => {
                let message = clocked_recording_envelope::parse_head(&reply.1)
                    .map_err(|_| Error::UnsupportedTask)?;
                Ok(Some(DvrCandidate {
                    message,
                    origin: self.inner.clone(),
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_recording_dvr(
        &self,
        candidate: DvrCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if !self.allows_recording_dvr() || !Arc::ptr_eq(&self.inner, &candidate.origin) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.lease(owner)?;
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["dvr".into()],
                &[candidate.message.raw_frame()],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => {
                Err(Error::AmbiguousClaim(candidate.message.task_id()))
            }
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_recording_dvr(
        &self,
        message: &ObservedProducerEnvelope,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !self.allows_recording_dvr() {
            return Err(Error::UnsupportedTask);
        }
        // Actual Source3 normal return None; shared supervised TTL/oldSUCCESS
        // guard writes no invented traceback/extended Python worker fields.
        self.write_result(
            message.task_id(),
            serde_json::Value::Null,
            None,
            vec![],
            owner,
        )
        .await
    }
}
