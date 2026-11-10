//! Index-only production dispatch adapter, app-unwired until lifecycle gates.
//! Original Dispatcharr authors retain application/algorithm credit and AGPL license.
use crate::{
    index_protocol::IndexTask,
    protocol::Published,
    transport::{Error, Jobs},
};
use dispatcharr_ingest_epg::{
    current_lookup::MissingIndexDispatcher, ImportError, SwapPolicyContext,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
struct Lease {
    _slot: OwnedSemaphorePermit,
    _caller: Arc<dyn Send + Sync>,
}
struct Inner {
    jobs: Jobs,
    slots: Arc<Semaphore>,
    capacity: u32,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct ProgrammeIndexDispatcher {
    inner: Arc<Inner>,
}
impl ProgrammeIndexDispatcher {
    pub fn new(jobs: Jobs, capacity: usize) -> Result<Self, Error> {
        if capacity == 0 || capacity > 16 {
            return Err(Error::Configuration);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                slots: Arc::new(Semaphore::new(capacity)),
                capacity: capacity as u32,
                closed: AtomicBool::new(false),
            }),
        })
    }
    /// Return only after the actual broker reply. Caller timeout/drop cannot
    /// free this adapter's own admission or caller while physical I/O continues.
    pub async fn publish(
        &self,
        source_id: i64,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let retained = Arc::new(Lease {
            _slot: slot,
            _caller: caller,
        });
        self.inner
            .jobs
            .publish_index(&IndexTask::Initial { source_id }, Uuid::new_v4(), retained)
            .await
    }
    pub fn available_slots(&self) -> usize {
        self.inner.slots.available_permits()
    }
    /// Close/drain initial publication only. Active workers must still be able
    /// to publish children/results; their own later drain closes the shared Jobs.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.inner.closed.store(true, Ordering::Release);
        let drain = self
            .inner
            .slots
            .clone()
            .acquire_many_owned(self.inner.capacity);
        let _held = tokio::time::timeout(timeout, drain)
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::Closed)?;
        Ok(())
    }
}
impl MissingIndexDispatcher for ProgrammeIndexDispatcher {
    fn dispatch(
        &self,
        source_id: i64,
        context: SwapPolicyContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), ImportError>> + Send + '_>> {
        Box::pin(async move {
            context.ensure_active()?;
            self.publish(source_id, Arc::new(context))
                .await
                .map_err(|_| ImportError::Worker)?;
            Ok(())
        })
    }
}
