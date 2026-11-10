//! Supervised bounded no-recording Series intake. Original Dispatcharr authors
//! retain application/algorithm credit. No ETA, foreign-task ACK or scheduler.
use crate::series_worker::{Error, Run, SeriesWorker};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Starting,
    Empty,
    Completed,
    /// The FIFO head was not consumed. Operators can distinguish an unsupported
    /// family from an empty queue without silently dropping unrelated work.
    UnsupportedHead,
    UnsupportedRules,
    Fault(Error),
    Stopped,
}
#[derive(Clone)]
pub struct SeriesDriver {
    worker: SeriesWorker,
    stop: watch::Sender<bool>,
    state: watch::Receiver<State>,
    done: watch::Receiver<bool>,
}
impl SeriesDriver {
    pub fn start(worker: SeriesWorker, poll: Duration) -> Result<Self, Error> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll) {
            return Err(Error::Configuration);
        }
        let (stop, mut stopping) = watch::channel(false);
        let (state_tx, state) = watch::channel(State::Starting);
        let (done_tx, done) = watch::channel(false);
        let owned = worker.clone();
        tokio::spawn(async move {
            loop {
                if *stopping.borrow() {
                    break;
                }
                let observation = match owned.run_once(Arc::new(())).await {
                    Ok(Run::Empty) => State::Empty,
                    Ok(Run::Completed(_) | Run::AlreadySucceeded(_)) => State::Completed,
                    Err(Error::Transport(crate::transport::Error::UnsupportedTask)) => {
                        State::UnsupportedHead
                    }
                    Err(Error::UnsupportedRules) => State::UnsupportedRules,
                    Err(Error::Closed) => break,
                    Err(error) => State::Fault(error),
                };
                state_tx.send_replace(observation);
                tokio::select! {
                    _ = tokio::time::sleep(poll) => {},
                    _ = stopping.changed() => {},
                }
            }
            state_tx.send_replace(State::Stopped);
            done_tx.send_replace(true);
        });
        Ok(Self {
            worker,
            stop,
            state,
            done,
        })
    }
    pub fn state(&self) -> State {
        self.state.borrow().clone()
    }
    /// One absolute deadline covers intake closure, actual worker drain and loop
    /// completion. Timeout never aborts physically retained work or closes shared
    /// Jobs; the owner closes that transport after all families/services drain.
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.stop.send_replace(true);
        self.worker
            .shutdown(deadline.saturating_duration_since(tokio::time::Instant::now()))
            .await?;
        let mut done = self.done.clone();
        tokio::time::timeout_at(deadline, async {
            while !*done.borrow_and_update() {
                done.changed().await.map_err(|_| Error::Panic)?;
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::DrainTimeout)?
    }
}
