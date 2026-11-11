//! Explicit phase subtask accounting prototype; not yet compiled or integrated.
//! Original Dispatcharr authors retain full application/algorithm credit.
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    Auth,
    Sql,
    Document,
    Other,
}
impl Kind {
    fn index(self) -> usize {
        match self {
            Self::Auth => 0,
            Self::Sql => 1,
            Self::Document => 2,
            Self::Other => 3,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Error {
    Sealed,
    Capacity,
    Deadline,
    WrongEpoch,
}
struct State {
    sealed: bool,
    counts: [usize; 4],
}
pub(super) struct Epoch {
    state: Mutex<State>,
    changed: Notify,
    maximum: usize,
}
struct Registration {
    epoch: Arc<Epoch>,
    kind: Kind,
    parent: Option<Arc<dyn Send + Sync>>,
}
#[derive(Clone)]
pub(super) struct PhysicalChild {
    registration: Arc<Registration>,
}
pub(super) struct Sealing {
    epoch: Arc<Epoch>,
}
pub(super) struct Quiescent {
    epoch: Arc<Epoch>,
}
impl Epoch {
    pub(super) fn new(maximum: usize) -> Result<Arc<Self>, Error> {
        if maximum == 0 || maximum > 256 {
            return Err(Error::Capacity);
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                sealed: false,
                counts: [0; 4],
            }),
            changed: Notify::new(),
            maximum,
        }))
    }
    pub(super) fn register(
        self: &Arc<Self>,
        kind: Kind,
        parent: Arc<dyn Send + Sync>,
    ) -> Result<PhysicalChild, Error> {
        let mut state = self.state.lock().expect("phase children");
        if state.sealed {
            return Err(Error::Sealed);
        }
        let live: usize = state.counts.iter().sum();
        if live == self.maximum {
            return Err(Error::Capacity);
        }
        state.counts[kind.index()] += 1;
        drop(state);
        Ok(PhysicalChild {
            registration: Arc::new(Registration {
                epoch: self.clone(),
                kind,
                parent: Some(parent),
            }),
        })
    }
    pub(super) fn counts(&self) -> [usize; 4] {
        self.state.lock().expect("phase children").counts
    }
    pub(super) fn seal(self: &Arc<Self>) -> Sealing {
        self.state.lock().expect("phase children").sealed = true;
        Sealing {
            epoch: self.clone(),
        }
    }
}
impl PhysicalChild {
    pub(super) fn erased(&self) -> Arc<dyn Send + Sync> {
        self.registration.clone()
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        // Release the physical parent's last ownership before advertising quiescence.
        // A cloned erased registration cannot disappear until its last task ends.
        drop(self.parent.take());
        let mut state = self.epoch.state.lock().expect("phase children");
        let count = &mut state.counts[self.kind.index()];
        *count = count.checked_sub(1).expect("registered child accounting");
        drop(state);
        self.epoch.changed.notify_waiters();
    }
}
impl Sealing {
    pub(super) async fn wait(
        self,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Quiescent, Error> {
        loop {
            let changed = self.epoch.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.epoch.counts() == [0; 4] {
                return Ok(Quiescent {
                    epoch: self.epoch.clone(),
                });
            }
            match deadline {
                Some(end) => tokio::time::timeout_at(end, changed)
                    .await
                    .map_err(|_| Error::Deadline)?,
                None => changed.await,
            }
        }
    }
}
impl Quiescent {
    pub(super) fn require(&self, epoch: &Arc<Epoch>) -> Result<(), Error> {
        if Arc::ptr_eq(&self.epoch, epoch) {
            Ok(())
        } else {
            Err(Error::WrongEpoch)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Parent(Arc<AtomicBool>);
    impl Drop for Parent {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release)
        }
    }
    #[tokio::test]
    async fn erased_clones_keep_explicit_registration_and_parent() {
        let epoch = Epoch::new(8).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let child = epoch
            .register(Kind::Auth, Arc::new(Parent(dropped.clone())))
            .unwrap();
        let physical = child.erased();
        let nested = physical.clone();
        drop(child);
        let seal = epoch.seal();
        assert!(matches!(
            epoch.register(Kind::Sql, Arc::new(())),
            Err(Error::Sealed)
        ));
        let waiting = tokio::spawn(async move { seal.wait(None).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(physical);
        assert!(!dropped.load(Ordering::Acquire));
        drop(nested);
        let proof = waiting.await.unwrap().unwrap();
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(epoch.counts(), [0; 4]);
        proof.require(&epoch).unwrap();
    }
    #[tokio::test(start_paused = true)]
    async fn logical_timeout_does_not_unseal_or_fake_quiescence() {
        let epoch = Epoch::new(8).unwrap();
        let child = epoch.register(Kind::Sql, Arc::new(())).unwrap();
        let result = epoch
            .seal()
            .wait(Some(
                tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            ))
            .await;
        assert!(matches!(result, Err(Error::Deadline)));
        assert_eq!(epoch.counts(), [0, 1, 0, 0]);
        assert!(matches!(
            epoch.register(Kind::Other, Arc::new(())),
            Err(Error::Sealed)
        ));
        drop(child);
        epoch.seal().wait(None).await.unwrap();
    }
    #[tokio::test]
    async fn held_document_vetoes_loan_until_drop() {
        let epoch = Epoch::new(8).unwrap();
        let document = epoch.register(Kind::Document, Arc::new(())).unwrap();
        let own = epoch.clone();
        let waiting = tokio::spawn(async move { own.seal().wait(None).await });
        tokio::task::yield_now().await;
        assert_eq!(epoch.counts(), [0, 0, 1, 0]);
        assert!(!waiting.is_finished());
        drop(document);
        waiting.await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn another_epoch_cannot_authorize_resource_loan() {
        let a = Epoch::new(8).unwrap();
        let b = Epoch::new(8).unwrap();
        let proof = a.seal().wait(None).await.unwrap();
        assert_eq!(proof.require(&b), Err(Error::WrongEpoch));
    }
    #[test]
    fn registered_child_limit_is_bounded() {
        let epoch = Epoch::new(1).unwrap();
        let first = epoch.register(Kind::Other, Arc::new(())).unwrap();
        assert!(matches!(
            epoch.register(Kind::Other, Arc::new(())),
            Err(Error::Capacity)
        ));
        drop(first);
        epoch.register(Kind::Auth, Arc::new(())).unwrap();
    }
}
