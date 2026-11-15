//! Source owner's no-data backoff and health-gated keepalive decisions.
//! Original Dispatcharr authors retain full algorithm and application credit.
//! Authority: live_proxy/output/ts/generator.py, _stream_data_generator and
//! _should_send_keepalive. The existing Source packet bytes remain unchanged.
use std::time::{Duration, Instant};

pub(super) enum Decision {
    Wait,
    Send,
    Stop,
}

pub(super) struct SourceKeepalive {
    consecutive_empty: u64,
    delay: Duration,
    started: Option<Instant>,
}

impl SourceKeepalive {
    pub fn new() -> Self {
        Self {
            consecutive_empty: 0,
            delay: Duration::ZERO,
            started: None,
        }
    }

    pub fn delay(&self) -> Duration {
        self.delay
    }

    pub fn received(&mut self) {
        self.consecutive_empty = 0;
        self.delay = Duration::ZERO;
        self.started = None;
    }

    /// The next empty observation must be Source's fifth (or later) before
    /// consulting remote health. Never issue a Redis GET for early/behind waits.
    pub fn health_check_due(&self, at_head: bool) -> bool {
        at_head && self.consecutive_empty >= 4
    }

    pub fn empty(
        &mut self,
        at_head: bool,
        healthy: bool,
        now: Instant,
        interval: Duration,
        maximum: Duration,
    ) -> Decision {
        if !at_head {
            // History recovery owns the cursor movement; never add control
            // packets while waiting for a chunk that is behind the real head.
            self.consecutive_empty = 0;
            self.delay = Duration::from_millis(100);
            return Decision::Wait;
        }
        self.consecutive_empty = self.consecutive_empty.saturating_add(1);
        if self.consecutive_empty < 5 || healthy {
            self.delay = Duration::from_millis(100 * self.consecutive_empty.min(10));
            return Decision::Wait;
        }
        let started = *self.started.get_or_insert(now);
        if now.duration_since(started) > maximum {
            return Decision::Stop;
        }
        self.consecutive_empty = 0;
        self.delay = interval;
        Decision::Send
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_source_never_inserts_control_packets_at_a_continuous_head() {
        let mut policy = SourceKeepalive::new();
        let start = Instant::now();
        for second in 0..30 {
            assert!(matches!(
                policy.empty(
                    true,
                    true,
                    start + Duration::from_secs(second),
                    Duration::from_millis(500),
                    Duration::from_secs(5)
                ),
                Decision::Wait
            ));
        }
        assert!(policy.started.is_none());
        assert_eq!(policy.delay(), Duration::from_secs(1));
    }

    #[test]
    fn unhealthy_owner_uses_source_empty_read_backoff_and_recovery_resets_cap() {
        let mut policy = SourceKeepalive::new();
        let start = Instant::now();
        let interval = Duration::from_millis(500);
        let maximum = Duration::from_secs(2);
        let mut now = start;
        for delay in [100, 200, 300, 400] {
            assert!(matches!(
                policy.empty(true, false, now, interval, maximum),
                Decision::Wait
            ));
            assert_eq!(policy.delay(), Duration::from_millis(delay));
            now += policy.delay();
        }
        assert_eq!(now.duration_since(start), Duration::from_secs(1));
        assert!(matches!(
            policy.empty(true, false, now, interval, maximum),
            Decision::Send
        ));
        assert_eq!(policy.delay(), interval);
        now += interval;
        for _ in 0..4 {
            assert!(matches!(
                policy.empty(true, false, now, interval, maximum),
                Decision::Wait
            ));
            now += policy.delay();
        }
        assert_eq!(now.duration_since(start), Duration::from_millis(2500));
        assert!(matches!(
            policy.empty(true, false, now, interval, maximum),
            Decision::Send
        ));
        policy.received();
        assert!(policy.started.is_none());
        assert_eq!(policy.delay(), Duration::ZERO);
        for _ in 0..4 {
            assert!(matches!(
                policy.empty(true, false, now + maximum, interval, maximum),
                Decision::Wait
            ));
        }
        assert!(matches!(
            policy.empty(true, false, now + maximum, interval, maximum),
            Decision::Send
        ));
    }

    #[test]
    fn behind_head_never_emits_and_cap_is_strictly_after_first_keepalive() {
        let mut policy = SourceKeepalive::new();
        let start = Instant::now();
        let interval = Duration::from_millis(500);
        let maximum = Duration::from_secs(2);
        for _ in 0..10 {
            assert!(matches!(
                policy.empty(false, false, start, interval, maximum),
                Decision::Wait
            ));
        }
        for _ in 0..4 {
            assert!(matches!(
                policy.empty(true, false, start, interval, maximum),
                Decision::Wait
            ));
        }
        assert!(matches!(
            policy.empty(true, false, start, interval, maximum),
            Decision::Send
        ));
        for _ in 0..4 {
            assert!(matches!(
                policy.empty(true, false, start + maximum, interval, maximum),
                Decision::Wait
            ));
        }
        assert!(matches!(
            policy.empty(true, false, start + maximum, interval, maximum),
            Decision::Send
        ));
        for _ in 0..4 {
            assert!(matches!(
                policy.empty(true, false, start + maximum + interval, interval, maximum),
                Decision::Wait
            ));
        }
        assert!(matches!(
            policy.empty(true, false, start + maximum + interval, interval, maximum),
            Decision::Stop
        ));
    }
    #[test]
    fn remote_query_gate_waits_for_fifth_empty_and_never_queries_behind_head() {
        let mut policy = SourceKeepalive::new();
        let now = Instant::now();
        for _ in 0..4 {
            assert!(!policy.health_check_due(true));
            assert!(matches!(
                policy.empty(
                    true,
                    true,
                    now,
                    Duration::from_millis(500),
                    Duration::from_secs(60)
                ),
                Decision::Wait
            ));
        }
        assert!(policy.health_check_due(true));
        assert!(!policy.health_check_due(false));
        assert!(matches!(
            policy.empty(
                true,
                false,
                now,
                Duration::from_millis(500),
                Duration::from_secs(60)
            ),
            Decision::Send
        ));
        assert!(!policy.health_check_due(true));
        policy.received();
        assert!(!policy.health_check_due(true));
    }
}
