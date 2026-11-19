//! Concrete adapter to the SAME Main Jobs broker/binding/operation ownership.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
use crate::transport::{Error, Jobs};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{CoreProfilePublication, CoreTaskBroker, CoreTaskPublication, StoreError};
use std::sync::Arc;
pub struct Publisher {
    jobs: Jobs,
}
impl Publisher {
    pub fn new(jobs: Jobs) -> Self {
        Self { jobs }
    }
}
impl CoreTaskBroker for Publisher {
    fn publish(
        &self,
        publication: CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            self.jobs
                .publish_core_settings(publication, owner)
                .await
                .map_err(|_| {
                    StoreError::Unsupported(
                        "Actual shared Core broker publication did not acknowledge",
                    )
                })?;
            Ok(())
        })
    }

    fn publish_profiles_root(
        &self,
        publication: CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CoreProfilePublication, StoreError>> {
        Box::pin(async move {
            match self.jobs.publish_core_settings(publication, owner).await {
                Ok(()) => Ok(CoreProfilePublication::Acknowledged),
                // Jobs and broker admission return Capacity before dispatch.
                // The Lua queue-bound branch returns it before SADD or LPUSH.
                Err(Error::Capacity) => Ok(CoreProfilePublication::CapacityRefused),
                Err(_) => Err(StoreError::Unsupported(
                    "Actual shared Core broker publication did not acknowledge",
                )),
            }
        })
    }
}
