//! The production shared-input writer. Producer finish differs from owner
//! cleanup: finish rejects writes but preserves retained media until its TTL.
use dispatcharr_input_store::InputStore;
use dispatcharr_stream_runtime::{Channel, Error};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use tokio::sync::Semaphore;
#[derive(Clone)]
pub(super) struct InputWriter {
    inner: Arc<Inner>,
}
struct Inner {
    store: InputStore,
    channel: String,
    owner: String,
    cache: Channel,
    finished: AtomicBool,
    index: Arc<AtomicU64>,
    source_lifecycle: bool,
    _admission: Option<Arc<dyn Send + Sync>>,
    slot: Arc<Semaphore>,
}
impl InputWriter {
    pub async fn new(
        store: InputStore,
        channel: String,
        owner: String,
        cache: Channel,
        index: Arc<AtomicU64>,
        source_lifecycle: bool,
        admission: Option<Arc<dyn Send + Sync>>,
    ) -> Result<Self, Error> {
        tokio::spawn(async move {
            let (sequence, _scope) = store
                .producer_index_retained(
                    &channel,
                    &owner,
                    (cache.clone(), index.clone(), admission.clone()),
                )
                .await
                .map_err(|_| Error::Stopped)?;
            cache.initialize_position(sequence)?;
            index.store(sequence, Ordering::Relaxed);
            Ok(Self {
                inner: Arc::new(Inner {
                    store,
                    channel,
                    owner,
                    cache,
                    finished: AtomicBool::new(false),
                    index,
                    source_lifecycle,
                    _admission: admission,
                    slot: Arc::new(Semaphore::new(1)),
                }),
            })
        })
        .await
        .map_err(|_| Error::Stopped)?
    }
    pub fn index(&self) -> u64 {
        self.inner.index.load(Ordering::Relaxed)
    }
    pub async fn publish(
        &self,
        data: Arc<[u8]>,
        discontinuity: bool,
        received_at: f64,
    ) -> Result<u64, Error> {
        if self.inner.finished.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let permit = self
            .inner
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::AtCapacity)?;
        if self.inner.finished.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let own = self.clone();
        // Caller cancellation cannot drop publication admission/media before
        // the actual Redis reply and local cache commit have settled.
        tokio::spawn(async move {
            let scope = (permit, own.clone(), data.clone());
            let ((sequence, discontinuity), _scope) = if own.inner.source_lifecycle {
                own.inner
                    .store
                    .put_source_retained(
                        &own.inner.channel,
                        &own.inner.owner,
                        &data,
                        received_at,
                        scope,
                    )
                    .await
                    .map_err(|_| Error::Stopped)?
            } else {
                let (sequence, scope) = own
                    .inner
                    .store
                    .put_retained(
                        &own.inner.channel,
                        &own.inner.owner,
                        &data,
                        received_at,
                        discontinuity,
                        scope,
                    )
                    .await
                    .map_err(|_| Error::Stopped)?;
                ((sequence, discontinuity), scope)
            };
            own.inner.cache.publish_numbered(
                data,
                discontinuity,
                std::time::Instant::now(),
                sequence,
            )?;
            own.inner.index.store(sequence, Ordering::Relaxed);
            Ok(sequence)
        })
        .await
        .map_err(|_| Error::Stopped)?
    }
    pub async fn mark_discontinuity(&self) -> Result<u64, Error> {
        if !self.inner.source_lifecycle {
            return Ok(self.index().saturating_add(1));
        }
        let permit = self
            .inner
            .slot
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::AtCapacity)?;
        if self.inner.finished.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let own = self.clone();
        tokio::spawn(async move {
            let (sequence, _scope) = own
                .inner
                .store
                .mark_next_discontinuity_retained(
                    &own.inner.channel,
                    &own.inner.owner,
                    (permit, own.clone()),
                )
                .await
                .map_err(|_| Error::Stopped)?;
            Ok(sequence)
        })
        .await
        .map_err(|_| Error::Stopped)?
    }
    pub async fn finish(&self) -> Result<(), Error> {
        self.inner.finished.store(true, Ordering::Release);
        let own = self.clone();
        tokio::spawn(async move {
            let _permit = own
                .inner
                .slot
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Stopped)?;
            own.inner.cache.finish();
            Ok(())
        })
        .await
        .map_err(|_| Error::Stopped)?
    }
}
