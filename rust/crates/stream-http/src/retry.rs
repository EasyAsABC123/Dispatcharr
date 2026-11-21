use dispatcharr_contracts::PlaybackCandidate;
use std::{collections::HashSet, time::Duration};
use tokio::time::Instant;

pub(super) const PROVIDER_CAPACITY_ERROR: &str =
    "All active M3U profiles have reached maximum connection limits";

/// Source's short admission retry applies before any input opens, when every
/// available provider reports its connection limit reached.
pub(super) struct ProviderCapacityWait {
    started: Instant,
    delay: Duration,
    final_attempt: bool,
}

impl ProviderCapacityWait {
    pub(super) fn new(started: Instant) -> Self {
        Self {
            started,
            delay: Duration::from_millis(100),
            final_attempt: false,
        }
    }

    pub(super) fn elapsed(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.started)
    }

    pub(super) fn next_delay(&mut self, now: Instant) -> Option<Duration> {
        let remaining = Duration::from_secs(3).saturating_sub(self.elapsed(now));
        if remaining.is_zero() || self.final_attempt {
            return None;
        }
        if remaining <= self.delay {
            self.final_attempt = true;
            return Some(Duration::ZERO);
        }
        let delay = self.delay;
        self.delay += Duration::from_millis(25);
        Some(delay)
    }
}

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

    pub(super) fn source_http_reconnect_delay(&self) -> Duration {
        // Original waits 250 ms per consecutive failure, capped at 3 s.
        Duration::from_millis(250 * self.failures.min(12) as u64)
    }

    pub fn new_source(&mut self, index: usize) {
        self.failures = 0;
        self.last_failure = None;
        self.tried.insert(index);
    }

    pub fn next_candidate(
        &mut self,
        candidates: &[PlaybackCandidate],
        opened_url: Option<&str>,
    ) -> Option<usize> {
        for (index, candidate) in candidates.iter().enumerate() {
            if self.tried.contains(&index) {
                continue;
            }
            // Original marks each alternate considered, including a distinct
            // stream row that regenerates the outgoing source URL.
            if opened_url == Some(candidate.url.as_str()) {
                self.tried.insert(index);
                continue;
            }
            return Some(index);
        }
        None
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
    fn provider_capacity_wait_has_one_final_attempt_without_sleep() {
        let start = Instant::now();
        let mut retry = ProviderCapacityWait::new(start);
        let mut now = start;
        for delay in (100..=375).step_by(25) {
            let delay = Duration::from_millis(delay);
            assert_eq!(retry.next_delay(now), Some(delay));
            now += delay;
        }
        assert_eq!(retry.elapsed(now), Duration::from_millis(2850));
        assert_eq!(retry.next_delay(now), Some(Duration::ZERO));
        assert_eq!(retry.next_delay(now), None);
        assert_eq!(
            ProviderCapacityWait::new(start).next_delay(start + Duration::from_secs(3)),
            None
        );
    }

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
    fn ordinary_http_reconnect_delay_matches_source() {
        let mut state = RetryState::new();
        for (failures, milliseconds) in [
            (0, 0),
            (1, 250),
            (2, 500),
            (3, 750),
            (11, 2750),
            (12, 3000),
            (13, 3000),
            (usize::MAX, 3000),
        ] {
            state.failures = failures;
            assert_eq!(
                state.source_http_reconnect_delay(),
                Duration::from_millis(milliseconds)
            );
        }
    }

    #[test]
    fn new_source_and_expired_failure_window_restart_the_delay() {
        let now = Instant::now();
        let window = Duration::from_secs(1800);
        let mut state = RetryState::new();
        state.failure(now, window);
        state.failure(now + window, window);
        assert_eq!(
            state.source_http_reconnect_delay(),
            Duration::from_millis(500)
        );
        state.failure(now + window + window + Duration::from_millis(1), window);
        assert_eq!(
            state.source_http_reconnect_delay(),
            Duration::from_millis(250)
        );
        state.new_source(1);
        assert_eq!(state.source_http_reconnect_delay(), Duration::ZERO);
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
