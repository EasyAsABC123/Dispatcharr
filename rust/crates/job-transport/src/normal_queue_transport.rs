//! Normal read-only queue selector. No pop/publication/claim capability escapes.
//! Original Dispatcharr/Celery/Redis authors retain credit.
use super::*;
use crate::milestone_dispatch::{Disposition, Head, Queue};
use dispatcharr_contracts::BoxFuture;
pub(crate) struct NormalHead {
    jobs: Jobs,
    queue: Queue,
    // One immutable compiled script per owned lane, never an unbounded cache.
    peek: Arc<redis::Script>,
}
impl NormalHead {
    pub(crate) fn core_scheduled(jobs: Jobs) -> Self {
        Self {
            jobs,
            queue: Queue::Celery,
            peek: Arc::new(redis::Script::new(PEEK_CREATE)),
        }
    }

    pub(crate) async fn connect(broker: &str, backend: &str, queue: Queue) -> Result<Self, Error> {
        Ok(Self {
            jobs: Jobs::connect_series(broker, backend, Config::default()).await?,
            queue,
            peek: Arc::new(redis::Script::new(PEEK_CREATE)),
        })
    }
    pub(crate) fn uses_configured_ports(&self, jobs: &Jobs) -> bool {
        self.jobs.same_configured_ports(jobs)
    }
    pub(crate) async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}
impl Head for NormalHead {
    fn queue(&self) -> Queue {
        self.queue
    }
    fn peek(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Option<Vec<u8>>, Disposition>> {
        Box::pin(async move {
            let key = match self.queue {
                Queue::Celery => "celery",
                Queue::Dvr => "dvr",
            };
            let lease = self
                .jobs
                .lease(owner)
                .map_err(|e| Disposition::Operational(format!("queue admission {e:?}")))?;
            let (reply, _held) = self
                .jobs
                .inner
                .broker
                .command_script_classified::<(i64, Vec<u8>), _>(
                    self.peek.clone(),
                    &[key.into()],
                    &[],
                    lease,
                )
                .await
                .map_err(|e| Disposition::Operational(format!("queue observation {:?}", map(e))))?;
            match reply.0 {
                0 => Ok(None),
                1 if reply.1.len() <= crate::protocol::MAX_ENVELOPE => Ok(Some(reply.1)),
                1 => Err(Disposition::UnsupportedHead),
                _ => Err(Disposition::Operational("queue type/frame response".into())),
            }
        })
    }
}
