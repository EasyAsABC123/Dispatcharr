use std::{collections::HashSet, time::Duration};
use tokio::time::Instant;

/// Port of StreamManager's failure-idle and stable-playback bookkeeping.
pub(crate) struct RetryState {
    pub failures: usize,
    last_failure: Option<Instant>,
    pub tried: HashSet<usize>,
    pub rotations: usize,
    pub had_media: bool,
}

impl RetryState {
    pub fn new() -> Self {
        Self {
            failures: 0,
            last_failure: None,
            tried: HashSet::new(),
            rotations: 0,
            had_media: false,
        }
    }

    pub fn failure(&mut self, now: Instant, window: Duration) -> usize {
        if self
            .last_failure
            .is_some_and(|last| now.duration_since(last) > window)
        {
            self.failures = 0;
        }
        self.last_failure = Some(now);
        self.failures = self.failures.saturating_add(1);
        self.failures
    }

    pub fn new_source(&mut self, index: usize) {
        self.failures = 0;
        self.last_failure = None;
        self.tried.insert(index);
    }

    pub fn stable(&mut self, current: usize) {
        self.tried.clear();
        self.tried.insert(current);
        self.rotations = 0;
        // Stable media resets source rotation, not connection-failure history.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failures_accumulate_and_reset_only_after_idle_window() {
        let now = Instant::now();
        let mut state = RetryState::new();
        assert_eq!(state.failure(now, Duration::from_secs(60)), 1);
        assert_eq!(
            state.failure(now + Duration::from_secs(60), Duration::from_secs(60)),
            2
        );
        assert_eq!(
            state.failure(now + Duration::from_secs(121), Duration::from_secs(60)),
            1
        );
    }

    #[test]
    fn stable_playback_resets_rotation_without_erasing_failure_history() {
        let mut state = RetryState::new();
        state.failure(Instant::now(), Duration::from_secs(1800));
        state.failure(Instant::now(), Duration::from_secs(1800));
        state.tried.extend([0, 1, 2]);
        state.rotations = 4;
        state.stable(1);
        assert_eq!(state.failures, 2);
        assert_eq!(state.tried, HashSet::from([1]));
        assert_eq!(state.rotations, 0);
        state.new_source(2);
        assert_eq!(state.failures, 0);
    }
}
