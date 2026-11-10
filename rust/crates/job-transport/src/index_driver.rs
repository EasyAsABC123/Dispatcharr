//! Supervised bounded programme-index intake; no application/router attachment.
//! Original Dispatcharr/Celery/Kombu authors retain algorithm/transport credit.
use crate::{
    eta::Limits,
    index_worker::{Error, IndexExecutor},
    scheduled_index::{self, ScheduledIndexWorker},
    transport::{Error as TransportError, Jobs},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub scheduling: scheduled_index::Config,
    pub reservations: Limits,
    pub poll_interval: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            scheduling: scheduled_index::Config::default(),
            reservations: Limits {
                max_pending: 21,
                max_pending_bytes: 2 * 1024 * 1024,
            },
            poll_interval: Duration::from_millis(100),
        }
    }
}
impl Config {
    fn validate(self, operations: usize) -> Result<(), Error> {
        let commands = self
            .scheduling
            .active
            .checked_add(1)
            .ok_or(Error::Capacity)?;
        let residents = self
            .scheduling
            .prefetch
            .checked_add(self.scheduling.pending_eta)
            .and_then(|n| n.checked_add(1))
            .ok_or(Error::Capacity)?;
        if self.scheduling.active == 0
            || self.scheduling.active > 4
            || self.scheduling.prefetch < self.scheduling.active
            || self.scheduling.prefetch > 16
            || self.scheduling.pending_eta == 0
            || self.scheduling.pending_eta > 128
            || operations < commands
            || self.reservations.max_pending < residents
            || self.reservations.max_pending > 128
            || self.reservations.max_pending_bytes == 0
            || self.reservations.max_pending_bytes > 16 * 1024 * 1024
            || self.poll_interval < Duration::from_millis(10)
            || self.poll_interval > Duration::from_secs(5)
        {
            return Err(Error::Capacity);
        }
        Ok(())
    }
}
struct Inner {
    jobs: Jobs,
    scheduler: ScheduledIndexWorker,
    config: Config,
    stop: watch::Sender<bool>,
    finished: watch::Sender<Option<Result<(), Error>>>,
    last_error: Mutex<Option<TransportError>>,
    execution_error: Mutex<Option<Error>>,
}
#[derive(Clone)]
pub struct IndexDriver {
    inner: Arc<Inner>,
}
impl IndexDriver {
    /// Starts intake only for the exact supported index family. Foreign heads
    /// remain byte-identical and visible through last_intake_error(). No shared
    /// transport close or automatic visibility-expiry recovery is performed.
    pub fn start(
        jobs: Jobs,
        executor: Arc<dyn IndexExecutor>,
        config: Config,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Self, Error> {
        config.validate(jobs.operation_capacity())?;
        jobs.eta_pool(config.reservations)
            .map_err(Error::Transport)?;
        let scheduler = ScheduledIndexWorker::new(jobs.clone(), executor, config.scheduling)?;
        let (stop, _) = watch::channel(false);
        let (finished, _) = watch::channel(None);
        let inner = Arc::new(Inner {
            jobs,
            scheduler,
            config,
            stop,
            finished,
            last_error: Mutex::new(None),
            execution_error: Mutex::new(None),
        });
        let work = inner.clone();
        let task = tokio::spawn(async move { intake(work, owner).await });
        let monitor = inner.clone();
        tokio::spawn(async move {
            let result = task.await.unwrap_or(Err(Error::WorkerPanic));
            monitor.finished.send_replace(Some(result));
        });
        Ok(Self { inner })
    }
    pub fn last_execution_error(&self) -> Option<Error> {
        *self.inner.execution_error.lock().unwrap()
    }
    pub fn last_intake_error(&self) -> Option<TransportError> {
        *self.inner.last_error.lock().unwrap()
    }
    /// Stop and settle physical intake first, then warm-restore unstarted ETA /
    /// drain active work. One absolute budget; timeout leaves owned work alive.
    /// The application closes shared Jobs only after all worker families drain.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.inner.stop.send_replace(true);
        let mut finished = self.inner.finished.subscribe();
        let result = tokio::time::timeout(timeout, finished.wait_for(Option::is_some))
            .await
            .map_err(|_| Error::DrainTimeout)?
            .map_err(|_| Error::WorkerPanic)?
            .expect("finished driver");
        // Drain accepted work even when intake itself reports a fatal error.
        self.inner
            .scheduler
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await?;
        result
    }
}
async fn intake(inner: Arc<Inner>, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
    let mut stop = inner.stop.subscribe();
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        match inner
            .jobs
            .peek_index_candidate(owner.clone(), inner.config.reservations)
            .await
        {
            Ok(Some(candidate)) => {
                let available = if candidate.message().eta.is_some() {
                    inner.scheduler.available_eta()
                } else {
                    inner.scheduler.available_prefetch()
                };
                if available > 0 && !*stop.borrow() {
                    match inner
                        .jobs
                        .reserve_index_candidate_settled(
                            candidate,
                            owner.clone(),
                            inner.config.reservations,
                        )
                        .await
                    {
                        Ok(receipt) => {
                            // shutdown waits for this intake loop before closing
                            // scheduler admission, so accepted receipts cannot
                            // race the warm-restore snapshot.
                            let job = inner.scheduler.start(&receipt, owner.clone())?;
                            let observed = inner.clone();
                            tokio::spawn(async move {
                                if let Err(error) = job.wait().await {
                                    *observed.execution_error.lock().unwrap() = Some(error);
                                }
                            });
                            *inner.last_error.lock().unwrap() = None;
                            continue;
                        }
                        Err(error) => {
                            *inner.last_error.lock().unwrap() = Some(error);
                            if !matches!(
                                error,
                                TransportError::ClaimRace
                                    | TransportError::Capacity
                                    | TransportError::Backend
                            ) {
                                return Err(Error::Transport(error));
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                *inner.last_error.lock().unwrap() = None;
            }
            Err(error) => {
                *inner.last_error.lock().unwrap() = Some(error);
                if matches!(
                    error,
                    TransportError::Closed | TransportError::Configuration
                ) {
                    return Err(Error::Transport(error));
                }
                // Unsupported/corrupt foreign heads are never removed. A
                // supported independent consumer/operator must resolve them.
            }
        }
        tokio::select! {
            _ = stop.changed() => {},
            _ = tokio::time::sleep(inner.config.poll_interval) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_configuration_rejects_extreme_counts_without_overflow() {
        assert!(Config::default().validate(4).is_ok());
        assert_eq!(Config::default().validate(1), Err(Error::Capacity));
        for field in 0..3 {
            let mut config = Config::default();
            match field {
                0 => config.scheduling.active = usize::MAX,
                1 => config.scheduling.prefetch = usize::MAX,
                _ => config.scheduling.pending_eta = usize::MAX,
            }
            assert_eq!(config.validate(usize::MAX), Err(Error::Capacity));
        }
    }
}
