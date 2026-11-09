//! Original StreamManager health rules; Dispatcharr authors retain credit.
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Timing {
    pub startup: Duration,
    pub connection: Duration,
    pub initialization: Duration,
    pub interval: Duration,
}
#[derive(Clone, Debug)]
pub(crate) struct Health {
    pub healthy: bool,
    pub connected: bool,
    pub last_data_time: f64,
    pub awaiting: bool,
    pub exhausted: bool,
    pub needs_switch: bool,
    pub needs_reconnect: bool,
    pub switching: bool,
    pub running: bool,
    pub next_check: f64,
}
impl Health {
    pub fn new(now: f64) -> Self {
        Self {
            healthy: true,
            connected: false,
            last_data_time: now,
            awaiting: true,
            exhausted: false,
            needs_switch: false,
            needs_reconnect: false,
            switching: false,
            running: true,
            next_check: now,
        }
    }
    pub fn arm(&mut self, now: f64) {
        self.last_data_time = now;
        self.awaiting = true;
        self.healthy = true;
        self.needs_switch = false;
    }
    pub fn data(&mut self, now: f64) {
        self.last_data_time = now;
        if self.awaiting {
            self.awaiting = false;
            self.exhausted = false;
        }
    }
    pub fn poll_delay(&self, now: f64, timing: Timing) -> f64 {
        if self.connected
            && self.awaiting
            && !self.exhausted
            && !self.needs_switch
            && !self.switching
        {
            let remaining = self.last_data_time + timing.startup.as_secs_f64() - now;
            if remaining > 0.0 {
                timing.interval.as_secs_f64().min(remaining)
            } else {
                0.05
            }
        } else {
            timing.interval.as_secs_f64()
        }
    }
    /// This projects health only. Upstream retry/switch control remains with
    /// its existing supervisor, rather than changing playback from a stats read.
    pub fn tick(&mut self, now: f64, index: u64, timing: Timing) {
        let startup = self.connected && self.awaiting && !self.exhausted;
        let threshold = if !self.connected {
            timing.connection
        } else if startup {
            timing.startup
        } else if self.awaiting && index == 0 {
            timing.initialization
        } else {
            timing.connection
        };
        let inactive = now - self.last_data_time;
        if startup && inactive > threshold.as_secs_f64() {
            if !self.needs_switch && !self.switching && self.running {
                self.healthy = false;
                self.needs_switch = true;
            }
        } else if self.connected && inactive > threshold.as_secs_f64() {
            self.healthy = false;
        } else if self.connected && !self.healthy {
            self.healthy = true;
            self.needs_reconnect = false;
            self.needs_switch = false;
        }
        self.next_check = now + self.poll_delay(now, timing);
    }
    pub fn tick_if_due(&mut self, now: f64, index: u64, timing: Timing) {
        if now >= self.next_check {
            self.tick(now, index, timing);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    #[test]
    fn original_manager_one_tick_health_and_poll_vectors() {
        let data: Value =
            serde_json::from_str(include_str!("../datasets/source-health.json")).unwrap();
        let timing = Timing {
            startup: Duration::from_secs(8),
            connection: Duration::from_secs(10),
            initialization: Duration::from_secs(60),
            interval: Duration::from_secs(5),
        };
        for case in data["cases"].as_array().unwrap() {
            let state = &case["state"];
            let mut health = Health::new(state["last_data_time"].as_f64().unwrap());
            health.healthy = state["healthy"].as_bool().unwrap();
            health.connected = state["connected"].as_bool().unwrap();
            health.awaiting = state["awaiting"].as_bool().unwrap();
            health.exhausted = state["exhausted"].as_bool().unwrap();
            health.needs_switch = state["needs_switch"].as_bool().unwrap();
            health.needs_reconnect = state["needs_reconnect"].as_bool().unwrap();
            health.switching = state["switching"].as_bool().unwrap();
            let now = case["now"].as_f64().unwrap();
            health.tick(now, case["index"].as_u64().unwrap(), timing);
            let expected = &case["expected"];
            assert_eq!(
                health.healthy,
                expected["healthy"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                health.needs_switch,
                expected["needs_switch"].as_bool().unwrap(),
                "{case}"
            );
            assert_eq!(
                health.needs_reconnect,
                expected["needs_reconnect"].as_bool().unwrap(),
                "{case}"
            );
            assert!(
                (health.poll_delay(now, timing) - expected["poll_delay"].as_f64().unwrap()).abs()
                    < 1e-9,
                "{case}"
            );
        }
    }
}
