//! Idle-only observer accounting. Dispatcharr's original authors retain credit.
//! A Source Redis first-read window is not its retry or total-request deadline.
use super::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::{sync::watch, task::JoinHandle, time::Instant};

const SOURCE_FIRST_READ: Duration = Duration::from_secs(60);
struct Phase {
    started: Instant,
    reserve_started: Option<Instant>,
    reserve_elapsed: Duration,
}
impl Phase {
    fn remaining(&self, now: Instant, control: Duration, reserve: Duration) -> Option<Duration> {
        // A completed over-budget physical phase stays expired even when both
        // watch notifications coalesce before the observer is scheduled.
        if self.reserve_elapsed >= reserve {
            return None;
        }
        let in_reserve = self
            .reserve_started
            .map(|at| now.saturating_duration_since(at));
        let spent = now
            .saturating_duration_since(self.started)
            .saturating_sub(self.reserve_elapsed)
            .saturating_sub(in_reserve.unwrap_or_default());
        let active_remaining = control.checked_sub(spent).filter(|left| !left.is_zero())?;
        match in_reserve {
            Some(spent) => reserve.checked_sub(spent).filter(|left| !left.is_zero()),
            None => Some(active_remaining),
        }
    }
}
/// One bounded scalar state and one watch cell per existing admitted idle job.
/// Updating the phase under the mutex precedes polling the physical mutation.
/// The watch is a wakeup only: remaining time is recomputed from authoritative
/// timestamps, so coalesced notifications never lose an interval or extend it.
pub(super) struct Observer {
    phase: Mutex<Phase>,
    changed: watch::Sender<()>,
    control: Duration,
    reserve: Duration,
}
impl Observer {
    pub(super) fn new(control: Duration) -> Arc<Self> {
        Self::with_limits(control, SOURCE_FIRST_READ)
    }
    fn with_limits(control: Duration, reserve: Duration) -> Arc<Self> {
        let (changed, _) = watch::channel(());
        Arc::new(Self {
            phase: Mutex::new(Phase {
                started: Instant::now(),
                reserve_started: None,
                reserve_elapsed: Duration::ZERO,
            }),
            changed,
            control,
            reserve,
        })
    }
    pub(super) fn reserve(self: &Arc<Self>) -> Reserve {
        let mut phase = self.phase.lock().unwrap();
        assert!(
            phase.reserve_started.is_none(),
            "one idle reservation phase"
        );
        phase.reserve_started = Some(Instant::now());
        self.changed.send_replace(());
        Reserve(self.clone())
    }
    pub(super) async fn observe(
        &self,
        mut task: JoinHandle<Result<(), Error>>,
    ) -> Result<(), Error> {
        let mut changed = self.changed.subscribe();
        loop {
            let remaining =
                self.phase
                    .lock()
                    .unwrap()
                    .remaining(Instant::now(), self.control, self.reserve);
            let Some(remaining) = remaining else {
                return Err(Error::ObserverTimeout);
            };
            tokio::select! {
                biased;
                result = &mut task => return result.map_err(|_| Error::Backend)?,
                result = changed.changed() => {
                    if result.is_err() { return Err(Error::Backend); }
                }
                () = tokio::time::sleep(remaining) => {}
            }
        }
    }
}
pub(super) struct Reserve(Arc<Observer>);
impl Drop for Reserve {
    fn drop(&mut self) {
        let mut phase = self.0.phase.lock().unwrap();
        let started = phase
            .reserve_started
            .take()
            .expect("owned reservation phase");
        phase.reserve_elapsed += Instant::now().saturating_duration_since(started);
        self.0.changed.send_replace(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reserve_time_never_replenishes_control_budget() {
        let start = Instant::now();
        let mut phase = Phase {
            started: start,
            reserve_started: Some(start + Duration::from_secs(1)),
            reserve_elapsed: Duration::ZERO,
        };
        assert_eq!(
            phase.remaining(
                start + Duration::from_secs(5),
                Duration::from_secs(3),
                SOURCE_FIRST_READ
            ),
            Some(Duration::from_secs(56))
        );
        phase.reserve_started = None;
        phase.reserve_elapsed = Duration::from_secs(4);
        assert_eq!(
            phase.remaining(
                start + Duration::from_secs(5),
                Duration::from_secs(3),
                SOURCE_FIRST_READ
            ),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            phase.remaining(
                start + Duration::from_secs(7),
                Duration::from_secs(3),
                SOURCE_FIRST_READ
            ),
            None
        );
    }
    #[test]
    fn late_reserve_transition_cannot_rescue_expired_control() {
        let start = Instant::now();
        let phase = Phase {
            started: start,
            reserve_started: Some(start + Duration::from_secs(4)),
            reserve_elapsed: Duration::ZERO,
        };
        assert_eq!(
            phase.remaining(
                start + Duration::from_secs(5),
                Duration::from_secs(3),
                SOURCE_FIRST_READ
            ),
            None
        );
    }
    #[test]
    fn first_read_window_has_its_own_finite_boundary() {
        let start = Instant::now();
        let phase = Phase {
            started: start,
            reserve_started: Some(start),
            reserve_elapsed: Duration::ZERO,
        };
        assert_eq!(
            phase.remaining(
                start + Duration::from_secs(60),
                Duration::from_secs(3),
                SOURCE_FIRST_READ
            ),
            None
        );
    }
    #[tokio::test]
    async fn coalesced_phase_notifications_keep_actual_result() {
        let observer = Observer::with_limits(Duration::from_millis(30), Duration::from_millis(200));
        let state = observer.clone();
        let task = tokio::spawn(async move {
            let phase = state.reserve();
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(phase);
            Err(Error::ProfileFull)
        });
        assert_eq!(observer.observe(task).await, Err(Error::ProfileFull));
    }
    #[tokio::test]
    async fn observer_expiry_does_not_abort_physical_job() {
        let observer = Observer::with_limits(Duration::from_millis(30), Duration::from_millis(20));
        let state = observer.clone();
        let (sent, received) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _phase = state.reserve();
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = sent.send(());
            Ok(())
        });
        assert_eq!(observer.observe(task).await, Err(Error::ObserverTimeout));
        received.await.unwrap();
    }
    #[tokio::test]
    async fn actual_error_after_reserve_is_not_inferred_success() {
        let observer =
            Observer::with_limits(Duration::from_millis(100), Duration::from_millis(100));
        let state = observer.clone();
        let task = tokio::spawn(async move {
            let phase = state.reserve();
            tokio::time::sleep(Duration::from_millis(10)).await;
            drop(phase);
            Err(Error::Backend)
        });
        assert_eq!(observer.observe(task).await, Err(Error::Backend));
    }
    #[tokio::test]
    async fn caller_loss_drops_only_observation_not_physical_job() {
        let observer =
            Observer::with_limits(Duration::from_millis(100), Duration::from_millis(100));
        let state = observer.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (settled, settlement) = tokio::sync::oneshot::channel();
        let (release, held) = tokio::sync::oneshot::channel();
        let physical = tokio::spawn(async move {
            let _phase = state.reserve();
            let _ = started.send(());
            held.await.unwrap();
            let _ = settled.send(());
            Ok(())
        });
        let public = tokio::spawn(async move { observer.observe(physical).await });
        ready.await.unwrap();
        public.abort();
        assert!(public.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        settlement.await.unwrap();
    }
    #[tokio::test]
    async fn pre_reserve_deadline_is_not_extended() {
        let observer = Observer::with_limits(Duration::from_millis(20), Duration::from_millis(200));
        let task = tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok(())
        });
        assert_eq!(observer.observe(task).await, Err(Error::ObserverTimeout));
    }
    #[tokio::test]
    async fn post_reserve_deadline_uses_only_remaining_budget() {
        let observer = Observer::with_limits(Duration::from_millis(30), Duration::from_millis(200));
        let state = observer.clone();
        let task = tokio::spawn(async move {
            let phase = state.reserve();
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(phase);
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok(())
        });
        assert_eq!(observer.observe(task).await, Err(Error::ObserverTimeout));
    }
    #[test]
    fn completed_overlimit_reserve_is_sticky_after_coalesced_start_end() {
        let start = Instant::now();
        for seconds in [60, 61] {
            let phase = Phase {
                started: start,
                reserve_started: None,
                reserve_elapsed: Duration::from_secs(seconds),
            };
            assert_eq!(
                phase.remaining(
                    start + Duration::from_secs(seconds),
                    Duration::from_secs(3),
                    SOURCE_FIRST_READ
                ),
                None,
                "finished physical reserve may not erase its expired observation window"
            );
        }
        let phase = Phase {
            started: start,
            reserve_started: None,
            reserve_elapsed: Duration::from_secs(59),
        };
        assert_eq!(
            phase.remaining(
                start + Duration::from_secs(60),
                Duration::from_secs(3),
                SOURCE_FIRST_READ
            ),
            Some(Duration::from_secs(2))
        );
    }
}
