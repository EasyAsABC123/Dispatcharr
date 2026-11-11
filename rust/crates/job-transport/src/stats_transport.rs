//! Typed fixture-only fetch-statistics publication/early-ACK/result operations.
//! Original Dispatcharr/Celery/Kombu authors retain protocol credit/licenses.
use super::*;
use crate::stats_protocol::{self, Message};
impl Jobs {
    fn require_stats_fixture(&self) -> Result<(), Error> {
        if self.inner.authority == Authority::Fixture {
            Ok(())
        } else {
            Err(Error::UnsupportedTask)
        }
    }
    pub(crate) async fn publish_stats_fixture(
        &self,
        id: Uuid,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        self.require_stats_fixture()?;
        let bytes = stats_protocol::encode(id).map_err(|_| Error::Protocol)?;
        let lease = self.lease(caller)?;
        let keys = vec!["celery".to_owned(), "_kombu.binding.celery".to_owned()];
        let limit = self.inner.config.max_queued_jobs.to_string();
        match self
            .inner
            .broker
            .command_classified::<i64, _>(PUBLISH, &keys, &[&bytes, limit.as_bytes()], lease)
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn claim_stats_fixture(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Option<Message>, Error> {
        self.require_stats_fixture()?;
        let keys = vec!["celery".to_owned()];
        let (reply, held) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &keys,
                &[],
                self.lease(caller.clone())?,
            )
            .await
            .map_err(map)?;
        if reply.0 == 0 {
            return Ok(None);
        }
        if reply.0 != 1 {
            return Err(Error::Corrupt);
        }
        let message = stats_protocol::parse(&reply.1).map_err(|_| Error::UnsupportedTask)?;
        drop(held);
        match self
            .inner
            .broker
            .command_classified::<i64, _>(CLAIM_CREATE, &keys, &[&reply.1], self.lease(caller)?)
            .await
        {
            Ok((1, _)) => Ok(Some(message)),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_stats_success_fixture(
        &self,
        message: &Message,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        self.require_stats_fixture()?;
        self.write_result(
            message.task_id,
            serde_json::Value::Null,
            None,
            Vec::new(),
            caller,
        )
        .await
    }
}
