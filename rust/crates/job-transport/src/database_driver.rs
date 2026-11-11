//! Owned timer for the proven zero/one-known-interval scheduler lane.
//! Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
//! Not registered in the application and never starts default intake.
use super::{tick::PeriodicPublisher, DatabaseSchedule, Error};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

pub struct Driver {
    scheduler: DatabaseSchedule,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), Error>>>,
}
impl Driver {
    /// Setup/producer construction belong before this explicit start. Each
    /// unsupported task/option is a terminal driver error, never an ACK/skip.
    pub fn start(
        scheduler: DatabaseSchedule,
        publisher: Arc<dyn PeriodicPublisher>,
        max_sleep: Duration,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Self, Error> {
        if max_sleep.is_zero() || max_sleep > Duration::from_secs(5) {
            return Err(Error::Unsupported);
        }
        if scheduler
            .inner
            .closed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Closed);
        }
        let (stop, mut stopped) = watch::channel(false);
        let owned = scheduler.clone();
        let task = tokio::spawn(async move {
            loop {
                if *stopped.borrow() {
                    return Ok(());
                }
                // A stop never cancels an admitted tick. Its physical command
                // ownership survives both caller loss and logical deadlines.
                let tick = owned
                    .tick(chrono::Utc::now(), publisher.clone(), caller.clone())
                    .await?;
                if !tick.delay_seconds.is_finite() {
                    return Err(Error::Unsupported);
                }
                if tick.delay_seconds > 0.0 {
                    let wait = Duration::from_secs_f64(tick.delay_seconds).min(max_sleep);
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {},
                        _ = stopped.changed() => return Ok(()),
                    }
                }
            }
        });
        Ok(Self {
            scheduler,
            stop,
            task: Some(task),
        })
    }
    /// One absolute deadline covers the active tick, all physical replies and
    /// final retained SQL sync. A timed-out call leaves the join handle intact.
    pub async fn shutdown(&mut self, timeout: Duration) -> Result<bool, Error> {
        let deadline = Instant::now().checked_add(timeout).ok_or(Error::Capacity)?;
        self.stop.send_replace(true);
        let result = if let Some(task) = self.task.as_mut() {
            match tokio::time::timeout_at(deadline, task).await {
                Err(_) => return Ok(false),
                Ok(value) => {
                    self.task = None;
                    Some(value.unwrap_or(Err(Error::Join)))
                }
            }
        } else {
            None
        };
        let drained = self
            .scheduler
            .shutdown(deadline.saturating_duration_since(Instant::now()))
            .await;
        if let Some(result) = result {
            result?;
        }
        Ok(drained)
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        // The detached task retains scheduler/caller/physical admission until
        // its current tick finishes, then observes stop. No force-abort.
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
impl Driver {
    pub(super) fn task_finished(&self) -> bool {
        self.task.as_ref().is_none_or(|task| task.is_finished())
    }
}
