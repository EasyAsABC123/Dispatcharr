//! Local recording-worker ownership for the Selected AIO stop handoff.
//! Original Dispatcharr authors retain application credit.
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};
use tokio::sync::watch;
use uuid::Uuid;

type Recording = (Uuid, i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Process {
    Idle,
    Attempt,
    Settled,
    Unknown,
}

#[derive(Clone, Copy)]
struct State {
    stopping: bool,
    process: Process,
}

pub(crate) struct Generation {
    state: watch::Sender<State>,
}

impl Generation {
    fn new() -> Self {
        Self {
            state: watch::channel(State {
                stopping: false,
                process: Process::Idle,
            })
            .0,
        }
    }

    // Reserve in the blocking job before it launches FFmpeg. A STOP on an
    // idle generation prevents both a first child and a retry.
    pub(crate) fn begin_attempt(&self) -> bool {
        self.state.send_if_modified(|state| {
            if state.stopping || state.process != Process::Idle {
                return false;
            }
            state.process = Process::Attempt;
            true
        })
    }

    // Called only after the actual attempt returns an affirmative child and
    // stderr receipt, or an explicitly settled no-child spawn failure.
    pub(crate) fn attempt_settled(&self) {
        self.state.send_modify(|state| {
            assert_eq!(state.process, Process::Attempt);
            state.process = if state.stopping {
                Process::Settled
            } else {
                Process::Idle
            };
        });
    }

    pub(crate) fn attempt_uncertain(&self) {
        self.state.send_modify(|state| {
            assert_eq!(state.process, Process::Attempt);
            state.process = Process::Unknown;
        });
    }

    fn request_stop(&self) -> bool {
        self.state.send_if_modified(|state| {
            if state.stopping {
                return false;
            }
            state.stopping = true;
            if state.process == Process::Idle {
                state.process = Process::Settled;
            }
            true
        })
    }

    fn retire(&self) {
        self.state.send_modify(|state| {
            state.process = match state.process {
                Process::Attempt | Process::Unknown => Process::Unknown,
                Process::Idle | Process::Settled => Process::Settled,
            };
        });
    }
}

pub(crate) struct Registry {
    current: Mutex<HashMap<Recording, Arc<Generation>>>,
    shutdown: watch::Sender<bool>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            current: Mutex::new(HashMap::new()),
            shutdown: watch::channel(false).0,
        }
    }
}

impl Registry {
    pub(crate) fn register(&self, channel: Uuid, recording: i64) -> Option<Registration<'_>> {
        let key = (channel, recording);
        let mut current = self.current.lock().unwrap();
        let Entry::Vacant(entry) = current.entry(key) else {
            return None;
        };
        let generation = Arc::new(Generation::new());
        entry.insert(generation.clone());
        Some(Registration {
            registry: self,
            key,
            generation,
        })
    }

    pub(crate) fn stop(&self, channel: Uuid, recording: i64) -> Option<LocalRecordingStop> {
        let key = (channel, recording);
        let generation = self.current.lock().unwrap().get(&key)?.clone();
        if !generation.request_stop() {
            return None;
        }
        Some(LocalRecordingStop {
            key,
            generation,
            shutdown: self.shutdown.subscribe(),
        })
    }

    pub(crate) fn still_current(&self, request: &LocalRecordingStop) -> bool {
        self.current
            .lock()
            .unwrap()
            .get(&request.key)
            .is_some_and(|generation| Arc::ptr_eq(generation, &request.generation))
    }

    pub(crate) fn request_shutdown(&self) {
        self.shutdown.send_replace(true);
    }
}

pub(crate) struct Registration<'a> {
    registry: &'a Registry,
    key: Recording,
    generation: Arc<Generation>,
}

impl Registration<'_> {
    pub(crate) fn generation(&self) -> &Arc<Generation> {
        &self.generation
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut current = self.registry.current.lock().unwrap();
        self.generation.retire();
        current.remove(&self.key);
    }
}

/// Positive proof only. The caller must bound the wait. Dropping or timing out
/// the receiver never authorizes stopping a client.
pub struct LocalRecordingStop {
    key: Recording,
    generation: Arc<Generation>,
    shutdown: watch::Receiver<bool>,
}

impl LocalRecordingStop {
    pub async fn settled(self) -> bool {
        let Self {
            generation,
            mut shutdown,
            ..
        } = self;
        let mut state = generation.state.subscribe();
        loop {
            if *shutdown.borrow() {
                return false;
            }
            match state.borrow_and_update().process {
                Process::Settled => return true,
                Process::Unknown => return false,
                Process::Idle | Process::Attempt => {}
            }
            tokio::select! {
                changed = state.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                _ = shutdown.changed() => return false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn stop_latches_before_spawn_and_retry() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        assert!(registry.stop(channel, 1).is_none());
        let capture = registry.register(channel, 1).unwrap();
        let stop = registry.stop(channel, 1).unwrap();
        assert!(registry.stop(channel, 1).is_none());
        assert!(stop.settled().await);
        assert!(!capture.generation().begin_attempt());
        drop(capture);
        assert!(registry.stop(channel, 1).is_none());

        let retrying = registry.register(channel, 1).unwrap();
        assert!(retrying.generation().begin_attempt());
        retrying.generation().attempt_settled();
        let stop = registry.stop(channel, 1).unwrap();
        assert!(!retrying.generation().begin_attempt());
        assert!(stop.settled().await);
    }

    #[tokio::test]
    async fn queued_blocking_attempt_does_not_start_after_stop() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        let capture = registry.register(channel, 1).unwrap();
        let generation = capture.generation().clone();
        let (resume, released) = tokio::sync::oneshot::channel();
        let blocked = tokio::task::spawn_blocking(move || {
            released.blocking_recv().unwrap();
            generation.begin_attempt()
        });
        assert!(registry.stop(channel, 1).unwrap().settled().await);
        resume.send(()).unwrap();
        assert!(!blocked.await.unwrap());
    }

    #[tokio::test]
    async fn started_attempt_requires_positive_settlement() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        let capture = registry.register(channel, 1).unwrap();
        assert!(capture.generation().begin_attempt());
        let stop = registry.stop(channel, 1).unwrap();
        let stop = stop.settled();
        tokio::pin!(stop);
        assert!(tokio::time::timeout(Duration::ZERO, stop.as_mut())
            .await
            .is_err());
        capture.generation().attempt_settled();
        assert!(stop.await);
        assert!(!capture.generation().begin_attempt());
        drop(capture);
        assert!(registry.stop(channel, 1).is_none());
    }

    #[tokio::test]
    async fn unknown_retirement_and_stale_generation_cannot_settle() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        let capture = registry.register(channel, 1).unwrap();
        assert!(capture.generation().begin_attempt());
        assert!(registry.register(channel, 1).is_none());
        let stop = registry.stop(channel, 1).unwrap();
        assert!(registry.still_current(&stop));
        drop(capture);
        let successor = registry.register(channel, 1).unwrap();
        assert!(successor.generation().begin_attempt());
        assert!(!registry.still_current(&stop));
        assert!(!stop.settled().await);
        assert!(registry.stop(channel, 2).is_none());
    }

    #[tokio::test]
    async fn failed_wait_or_reader_cannot_grant_positive_proof() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        let capture = registry.register(channel, 1).unwrap();
        assert!(capture.generation().begin_attempt());
        let stop = registry.stop(channel, 1).unwrap();
        capture.generation().attempt_uncertain();
        assert!(!stop.settled().await);
        assert!(!capture.generation().begin_attempt());
    }

    #[tokio::test]
    async fn settled_predecessor_does_not_authorize_successor_snapshot() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        let predecessor = registry.register(channel, 1).unwrap();
        let old_stop = registry.stop(channel, 1).unwrap();
        assert!(registry.still_current(&old_stop));
        drop(predecessor);
        let successor = registry.register(channel, 1).unwrap();
        assert!(successor.generation().begin_attempt());
        assert!(!registry.still_current(&old_stop));
        assert!(old_stop.settled().await);
    }

    #[tokio::test]
    async fn shutdown_releases_waiter_without_proving_child_retired() {
        let registry = Registry::default();
        let channel = Uuid::nil();
        let capture = registry.register(channel, 1).unwrap();
        assert!(capture.generation().begin_attempt());
        let stop = registry.stop(channel, 1).unwrap();
        registry.request_shutdown();
        assert!(!stop.settled().await);
    }
}
