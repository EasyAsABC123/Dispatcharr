//! Opaque normal check_for_version_update port. No new generic Jobs enum authority.
use super::*;
#[derive(Clone)]
pub struct VersionJobs {
    jobs: Jobs,
}
impl VersionJobs {
    pub async fn connect(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        Ok(Self {
            jobs: Jobs::connect_series(broker, result, config).await?,
        })
    }
    pub(crate) async fn claim(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<crate::version_protocol::Message>, Error> {
        let keys = vec!["celery".to_owned()];
        let lease = self.jobs.lease(caller.clone())?;
        let (reply, held) = self
            .jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &keys, &[], lease)
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message =
            crate::version_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        drop(held);
        match self
            .jobs
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &keys,
                &[reply.1.as_slice()],
                self.jobs.lease(caller)?,
            )
            .await
        {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(e) => Err(map(e)),
        }
    }
    pub(crate) async fn write(
        &self,
        message: &crate::version_protocol::Message,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.jobs
            .write_result(
                message.task_id,
                serde_json::Value::Null,
                None,
                Vec::new(),
                caller,
            )
            .await
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}
