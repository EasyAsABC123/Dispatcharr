//! Concrete adapter to the SAME Main Jobs broker/binding/operation ownership.
//! Original Dispatcharr/Celery authors retain all application/protocol credit.
use crate::transport::{Error, Jobs};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{
    CoreOffsetPublication, CoreProfilePublication, CoreTaskBroker, CoreTaskPublication, StoreError,
};
use dispatcharr_store::{
    ProviderRehashAdmission, ProviderRehashBroker, ProviderRehashPublication, ProviderRehashResult,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
pub struct Publisher {
    jobs: Jobs,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CoreSettingsPublication {
    Acknowledged,
    GlobalRehashRefused,
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
            match self
                .jobs
                .publish_core_settings(publication, owner)
                .await
                .map_err(|_| {
                    StoreError::Unsupported(
                        "Actual shared Core broker publication did not acknowledge",
                    )
                })? {
                CoreSettingsPublication::Acknowledged => Ok(()),
                CoreSettingsPublication::GlobalRehashRefused => Err(StoreError::UnhandledRestFault),
            }
        })
    }

    fn publish_dvr_offsets(
        &self,
        publication: CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CoreOffsetPublication, StoreError>> {
        Box::pin(async move {
            self.jobs
                .publish_dvr_offsets(publication, owner)
                .await
                .map_err(|_| {
                    StoreError::Unsupported(
                        "Actual DVR offset broker publication did not acknowledge",
                    )
                })
        })
    }

    fn publish_profiles_root(
        &self,
        publication: CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CoreProfilePublication, StoreError>> {
        Box::pin(async move {
            match self.jobs.publish_core_settings(publication, owner).await {
                Ok(CoreSettingsPublication::Acknowledged) => {
                    Ok(CoreProfilePublication::Acknowledged)
                }
                // Jobs and broker admission return Capacity before dispatch.
                // The Lua queue-bound branch returns it before SADD or LPUSH.
                Err(Error::Capacity) => Ok(CoreProfilePublication::CapacityRefused),
                Ok(CoreSettingsPublication::GlobalRehashRefused) | Err(_) => {
                    Err(StoreError::Unsupported(
                        "Actual shared Core broker publication did not acknowledge",
                    ))
                }
            }
        })
    }
}

/// Scoped provider producer over the registry's same Jobs. The route guards
/// are unrelated to the broad Selected broker above.
pub struct ProviderRehashProducer {
    jobs: Jobs,
    closed: AtomicBool,
    slot: Arc<Semaphore>,
}

impl ProviderRehashProducer {
    pub fn new(jobs: Jobs) -> Self {
        Self {
            jobs,
            closed: AtomicBool::new(false),
            slot: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn stop_admission(&self) {
        self.closed.store(true, Ordering::Release);
    }

    pub async fn drain(&self, budget: Duration) -> bool {
        self.stop_admission();
        tokio::time::timeout(budget, self.slot.clone().acquire_owned())
            .await
            .is_ok_and(|result| result.is_ok())
    }
}

struct RehashAdmission {
    jobs: Jobs,
    _permit: OwnedSemaphorePermit,
}

fn provider_rehash_unavailable() -> StoreError {
    StoreError::Unsupported("Scoped provider rehash publisher unavailable")
}

impl ProviderRehashBroker for ProviderRehashProducer {
    fn admit(&self) -> Result<Arc<dyn ProviderRehashAdmission>, StoreError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(provider_rehash_unavailable());
        }
        let permit = self
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| provider_rehash_unavailable())?;
        if self.closed.load(Ordering::Acquire) {
            return Err(provider_rehash_unavailable());
        }
        Ok(Arc::new(RehashAdmission {
            jobs: self.jobs.clone(),
            _permit: permit,
        }))
    }
}

impl ProviderRehashAdmission for RehashAdmission {
    fn publish(
        &self,
        publication: ProviderRehashPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<ProviderRehashResult, StoreError>> {
        Box::pin(async move {
            let frame = publication.into_core();
            let parsed = crate::normal_core_tasks::parse(frame.bytes())
                .map_err(|_| provider_rehash_unavailable())?;
            if !crate::normal_core_tasks::owned_provider_rehash(&parsed.kind) {
                return Err(provider_rehash_unavailable());
            }
            match self
                .jobs
                .publish_provider_rehash(frame, owner, parsed.id)
                .await
            {
                Ok(result) => Ok(result),
                Err(
                    Error::Capacity
                    | Error::Closed
                    | Error::Configuration
                    | Error::Protocol
                    | Error::UnsupportedTask,
                ) => Err(provider_rehash_unavailable()),
                Err(_) => Ok(ProviderRehashResult::Uncertain),
            }
        })
    }
}
