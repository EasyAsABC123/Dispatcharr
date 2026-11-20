//! Owned create/status consumer for a separately declared Celery namespace.
//! The original Dispatcharr and Celery authors retain application credit.

use super::{Application, Disposition, Error};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{BackupJobApi, StoreError};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
    time::{timeout_at, Instant},
};

enum Poller {
    Prepared,
    Running(JoinHandle<bool>),
    Settled(bool),
}

impl Poller {
    async fn join(&mut self, deadline: Instant) -> Option<bool> {
        match self {
            Self::Prepared => Some(true),
            Self::Running(task) => {
                let clean = timeout_at(deadline, task).await.ok()?.unwrap_or(false);
                *self = Self::Settled(clean);
                Some(clean)
            }
            Self::Settled(clean) => Some(*clean),
        }
    }
}

struct Owner {
    stop: watch::Sender<bool>,
    poller: Mutex<Poller>,
}

fn keep_polling(outcome: &Result<Disposition, Disposition>) -> bool {
    matches!(
        outcome,
        Ok(Disposition::Empty | Disposition::HeadChanged | Disposition::NotDue)
            | Ok(Disposition::Completed(_) | Disposition::Revoked(_))
            | Ok(Disposition::Capacity)
            | Err(Disposition::Capacity)
    )
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[derive(Clone)]
pub struct ManualApplication {
    app: Application,
    owner: Arc<Owner>,
}

impl ManualApplication {
    pub fn new(app: Application) -> Result<Self, Error> {
        if app.inner.jobs.allows_defaults() || !app.inner.jobs.allows_backup() {
            return Err(Error::Configuration);
        }
        Ok(Self {
            app,
            owner: Arc::new(Owner {
                stop: watch::channel(false).0,
                poller: Mutex::new(Poller::Prepared),
            }),
        })
    }

    pub fn start(&self) -> Result<(), Error> {
        let mut task = self
            .owner
            .poller
            .try_lock()
            .map_err(|_| Error::Configuration)?;
        if !matches!(*task, Poller::Prepared) || *self.owner.stop.borrow() {
            return Err(Error::Configuration);
        }
        let app = self.app.clone();
        let mut stop = self.owner.stop.subscribe();
        *task = Poller::Running(tokio::spawn(async move {
            while !*stop.borrow_and_update() {
                if !keep_polling(&app.run(None, Arc::new(())).await) {
                    app.close_admission();
                    eprintln!("Manual backup consumer stopped without a clean queue result");
                    return false;
                }
                tokio::select! {
                    _ = stop.changed() => {}
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }
            true
        }));
        Ok(())
    }

    pub fn stop_admission(&self) {
        self.owner.stop.send_replace(true);
        self.app.close_admission();
    }

    async fn finish(&self, grace: Duration) -> bool {
        let deadline = Instant::now() + grace;
        self.stop_admission();
        let Ok(mut poller) = timeout_at(deadline, self.owner.poller.lock()).await else {
            return false;
        };
        let Some(clean_worker) = poller.join(deadline).await else {
            return false;
        };
        if !self
            .app
            .drain(deadline.saturating_duration_since(Instant::now()))
            .await
        {
            return false;
        }
        let closed = self
            .app
            .inner
            .jobs
            .shutdown(deadline.saturating_duration_since(Instant::now()))
            .await
            .is_ok();
        clean_worker && closed
    }
}

impl BackupJobApi for ManualApplication {
    fn create(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<String, StoreError>> {
        self.app.create(caller)
    }

    fn status<'a>(
        &'a self,
        id: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Value, StoreError>> {
        self.app.status(id, caller)
    }

    fn shutdown(&self, grace: Duration) -> BoxFuture<'_, bool> {
        Box::pin(self.finish(grace))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_known_no_claim_capacity_response_is_retryable() {
        let admission = tokio::sync::Semaphore::new(0);
        let exhausted = admission
            .try_acquire()
            .map(|_| Disposition::Empty)
            .map_err(|_| Disposition::Capacity);
        assert!(keep_polling(&exhausted));
        assert!(keep_polling(&Ok(Disposition::Capacity)));
        assert!(!keep_polling(&Ok(Disposition::UnsupportedHead)));
        assert!(!keep_polling(&Err(Disposition::Operational(
            "claim unsettled".into()
        ))));
    }

    #[tokio::test]
    async fn a_settled_worker_fault_remains_visible_to_shutdown_retries() {
        let mut task = Poller::Running(tokio::spawn(async { false }));
        assert_eq!(
            task.join(Instant::now() + Duration::from_secs(1)).await,
            Some(false)
        );
        assert_eq!(
            task.join(Instant::now() + Duration::from_secs(1)).await,
            Some(false)
        );
    }
}
