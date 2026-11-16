//! Natural VOD ingestion over existing M3U worker admission and Main ports.
//! Original Dispatcharr/Celery authors retain all algorithms/application credit.
use super::*;
use crate::milestone_dispatch::{Handler, Lifecycle};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::vod_ingestion as service;
struct Vod(Worker);
impl service::Cancellation for Inner {
    fn is_draining(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}
impl Worker {
    pub(crate) fn vod_handler(&self) -> Arc<dyn Handler> {
        Arc::new(Vod(self.clone()))
    }
    async fn run_vod(&self, expected: Option<Arc<[u8]>>, caller: Owner) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Ok(D::Empty);
        }
        let slot = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| D::Capacity)?;
        let owner: Owner = Arc::new((slot, caller, self.inner.clone()));
        let inner = self.inner.clone();
        let failure_inner = inner.clone();
        let failure_owner = owner.clone();
        match tokio::spawn(async move {
            let mut guard = FaultGuard {
                inner: inner.clone(),
                owner: owner.clone(),
                armed: true,
            };
            let result = async {
                let candidate = match inner
                    .jobs
                    .peek_vod(expected.as_deref(), owner.clone())
                    .await
                {
                    Ok(Some(candidate)) => candidate,
                    Ok(None) => return Ok(D::Empty),
                    Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                    Err(crate::transport::Error::UnsupportedTask) => {
                        return Err(D::UnsupportedHead)
                    }
                    Err(_) => return Err(D::Operational("VOD head observation unsettled".into())),
                };
                let revoked = inner
                    .revoked
                    .contains(&candidate.message.id.to_string())
                    .map_err(|_| D::Operational("VOD revoked membership unavailable".into()))?;
                let prepared = if revoked {
                    None
                } else {
                    match service::preflight(&inner.auth, candidate.message.account, owner.clone())
                        .await
                    {
                        Ok(prepared) => Some(prepared),
                        Err(service::AdmissionError::Unsupported) => {
                            return Err(D::UnsupportedDomain)
                        }
                        Err(service::AdmissionError::Operational(StoreError::StatusBody(
                            503,
                            _,
                        ))) => return Ok(D::Capacity),
                        Err(_) => return Err(D::Operational("VOD SQL admission unsettled".into())),
                    }
                };
                if inner.closed.load(Ordering::Acquire) {
                    return Ok(D::Empty);
                }
                guard.owner = Arc::new((
                    owner.clone(),
                    candidate.message.id,
                    candidate.message.root,
                    candidate.raw.clone(),
                ));
                let message = match inner.jobs.claim_vod(candidate, owner.clone()).await {
                    Ok(message) => message,
                    Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                    Err(_) => return Err(D::Operational("VOD claim outcome unsettled".into())),
                };
                if revoked {
                    if !inner
                        .jobs
                        .write_vod(&message, Value::Null, true, owner.clone())
                        .await
                        .map_err(|_| D::Operational("VOD revoked backend unsettled".into()))?
                    {
                        return Err(D::Operational("VOD revoked backend rejected".into()));
                    }
                    return Ok(D::Revoked(message.id));
                }
                let value = service::execute(
                    inner.auth.clone(),
                    inner.events.clone(),
                    prepared
                        .ok_or_else(|| D::Operational("VOD actual preparation absent".into()))?,
                    inner.clone(),
                    owner.clone(),
                )
                .await
                .map_err(|_| D::Operational("VOD ingestion execution unsettled".into()))?;
                if !inner
                    .jobs
                    .write_vod(&message, value, false, owner.clone())
                    .await
                    .map_err(|_| D::Operational("VOD actual backend unsettled".into()))?
                {
                    return Err(D::Operational("VOD actual backend rejected".into()));
                }
                Ok(D::Completed(message.id))
            }
            .await;
            if !matches!(&result, Err(D::Operational(_))) {
                guard.armed = false;
            }
            result
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                failure_inner
                    .uncertain
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .push(failure_owner);
                Err(D::Operational("VOD owned task join unsettled".into()))
            }
        }
    }
}
impl Lifecycle for Vod {
    fn stop_admission(&self) {
        self.0.stop();
    }
    fn begin_drain(&self, budget: Duration) {
        self.0.begin_drain(budget);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            if self.0.shutdown(timeout).await {
                Ok(())
            } else {
                Err(D::Operational(
                    "VOD ingestion physical drain unsettled".into(),
                ))
            }
        })
    }
}
impl Handler for Vod {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(&self, raw: Arc<[u8]>, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.0.run_vod(Some(raw), owner))
    }
    fn run_once(&self, owner: Owner) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.0.run_vod(None, owner))
    }
}
