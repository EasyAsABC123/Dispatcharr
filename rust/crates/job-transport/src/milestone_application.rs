//! One prepared default-FIFO poller/lifetime, not fourteen independent spinners.
//! Root app owns producer stop and shared transports after actual family drain.
use crate::milestone_dispatch::{Disposition, Family, Prepared};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;
#[derive(Clone, Debug)]
pub enum State {
    Prepared,
    Starting,
    Observed(Disposition),
    Stopped,
    Fault(Disposition),
}
struct JobCaller {
    _application: Arc<dyn Send + Sync>,
}
struct Lifetime {
    stop: watch::Sender<bool>,
}
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
struct Inner {
    prepared: Prepared,
    poll: Duration,
    started: Mutex<bool>,
    state: watch::Sender<State>,
    done: watch::Sender<bool>,
}
#[derive(Clone)]
pub struct Application {
    inner: Arc<Inner>,
    lifetime: Arc<Lifetime>,
}
#[derive(Debug)]
pub struct Drain {
    pub poller_settled: bool,
    pub families: Vec<(Family, Result<(), Disposition>)>,
    pub state: State,
}
impl Application {
    pub fn prepare(prepared: Prepared, poll: Duration) -> Result<Self, Disposition> {
        if !(Duration::from_millis(25)..=Duration::from_secs(5)).contains(&poll) {
            return Err(Disposition::Operational("invalid poll interval".into()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                prepared,
                poll,
                started: Mutex::new(false),
                state: watch::channel(State::Prepared).0,
                done: watch::channel(false).0,
            }),
            lifetime: Arc::new(Lifetime {
                stop: watch::channel(false).0,
            }),
        })
    }
    pub fn state(&self) -> State {
        self.inner.state.borrow().clone()
    }
    pub fn start(&self, owner: Arc<dyn Send + Sync>) -> Result<(), Disposition> {
        let mut started = self
            .inner
            .started
            .lock()
            .map_err(|_| Disposition::Operational("lifecycle poisoned".into()))?;
        if *started || *self.lifetime.stop.borrow() {
            return Err(Disposition::Operational(
                "intake already started/closed".into(),
            ));
        }
        *started = true;
        self.inner.state.send_replace(State::Starting);
        let inner = self.inner.clone();
        let mut stop = self.lifetime.stop.subscribe();
        tokio::spawn(async move {
            while !*stop.borrow() {
                let task_owner: Arc<dyn Send + Sync> = Arc::new(JobCaller {
                    _application: owner.clone(),
                });
                match inner.prepared.run_once(task_owner).await {
                    Ok(value) => {
                        inner.state.send_replace(State::Observed(value));
                    }
                    Err(error) => {
                        inner.state.send_replace(State::Fault(error));
                        inner.done.send_replace(true);
                        return;
                    }
                }
                tokio::select! {_=stop.changed()=>{},_=tokio::time::sleep(inner.poll)=>{}}
            }
            inner.state.send_replace(State::Stopped);
            inner.done.send_replace(true);
        });
        Ok(())
    }
    pub async fn shutdown(&self, timeout: Duration) -> Drain {
        self.lifetime.stop.send_replace(true);
        let deadline = tokio::time::Instant::now() + timeout;
        let families = self.inner.prepared.shutdown_all(deadline).await;
        let started = self.inner.started.lock().map(|v| *v).unwrap_or(true);
        let mut done = self.inner.done.subscribe();
        let poller_settled = !started
            || tokio::time::timeout_at(deadline, async {
                loop {
                    if *done.borrow_and_update() {
                        return true;
                    }
                    if done.changed().await.is_err() {
                        return false;
                    }
                }
            })
            .await
            .unwrap_or(false);
        Drain {
            poller_settled,
            families,
            state: self.state(),
        }
    }
}
