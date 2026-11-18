//! Bounded pinned packaging.version dependency, no Source application ports.
//! Original Dispatcharr/packaging/CPython authors retain all behavior credit.
use dispatcharr_store::{PythonRegexError, RegexExecutor};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    Parsed(bool),
    InvalidVersion,
}
fn decision(value: Value) -> Result<Decision, PythonRegexError> {
    let object = value.as_object().ok_or(PythonRegexError::Protocol)?;
    match object.get("kind").and_then(Value::as_str) {
        Some("invalid")
            if object.len() == 2
                && object.get("exception").and_then(Value::as_str) == Some("InvalidVersion") =>
        {
            Ok(Decision::InvalidVersion)
        }
        Some("parsed") if object.len() == 3 => {
            let newer = object
                .get("newer")
                .and_then(Value::as_bool)
                .ok_or(PythonRegexError::Protocol)?;
            let ordering = object
                .get("ordering")
                .and_then(Value::as_i64)
                .filter(|n| (-1..=1).contains(n))
                .ok_or(PythonRegexError::Protocol)?;
            if newer != (ordering == 1) {
                return Err(PythonRegexError::Protocol);
            }
            Ok(Decision::Parsed(newer))
        }
        _ => Err(PythonRegexError::Protocol),
    }
}
struct State {
    executor: Option<RegexExecutor>,
    deadline: Option<Instant>,
    closed: bool,
    settled: bool,
    shutdown_gate: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone)]
pub(crate) struct Comparator(Arc<Mutex<State>>);
impl Comparator {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(State {
            executor: None,
            deadline: None,
            closed: false,
            settled: false,
            shutdown_gate: Arc::new(tokio::sync::Mutex::new(())),
        })))
    }
    pub(crate) async fn newer(
        &self,
        current: &str,
        latest: &str,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Decision, PythonRegexError> {
        if current.is_empty() || current.len() > 128 || latest.is_empty() || latest.len() > 247 {
            return Err(PythonRegexError::Capacity);
        }
        let executor = {
            let mut state = self.0.lock().map_err(|_| PythonRegexError::Unavailable)?;
            if state.closed {
                return Err(PythonRegexError::Closed);
            }
            let executor = state
                .executor
                .get_or_insert_with(|| {
                    RegexExecutor::prepare(
                        "/usr/local/bin/dispatcharr-regex-python".into(),
                        Arc::new(()),
                    )
                })
                .clone();
            if let Some(deadline) = state.deadline {
                executor.interrupt_at(deadline);
            }
            executor
        };
        // Persistent family dependency; SAME already-admitted version task owns
        // its actual request/reply. Its lifetime never captures the first task's
        // semaphore permit. No new queue or independent task admission is added.
        let reply = executor
            .execute(
                json!({"kind":"version_compare","current":current,"latest":latest}),
                owner,
            )
            .await?;
        decision(reply)
    }
    pub(crate) fn begin_drain(&self, deadline: Instant) {
        if let Ok(mut state) = self.0.lock() {
            let deadline = state.deadline.map_or(deadline, |old| old.min(deadline));
            state.deadline = Some(deadline);
            if let Some(executor) = &state.executor {
                executor.interrupt_at(deadline);
            }
        }
    }
    pub(crate) async fn shutdown(&self, budget: Duration) -> Result<(), PythonRegexError> {
        let deadline = Instant::now() + budget;
        let gate = self
            .0
            .lock()
            .map_err(|_| PythonRegexError::Unavailable)?
            .shutdown_gate
            .clone();
        let _retirement = tokio::time::timeout_at(deadline, gate.lock())
            .await
            .map_err(|_| PythonRegexError::Drain)?;
        let executor = {
            let mut state = self.0.lock().map_err(|_| PythonRegexError::Unavailable)?;
            if state.settled {
                return Ok(());
            }
            state.closed = true;
            state.executor.clone()
        };
        if let Some(executor) = executor {
            let receipt = executor
                .shutdown(deadline.saturating_duration_since(Instant::now()))
                .await?;
            match receipt {
                Some(receipt) if receipt.wait_observed => {}
                Some(_) => return Err(PythonRegexError::Drain),
                None => {
                    while executor.no_spawn_retirement().is_none() {
                        if Instant::now() >= deadline {
                            return Err(PythonRegexError::Drain);
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
        }
        self.0
            .lock()
            .map_err(|_| PythonRegexError::Unavailable)?
            .settled = true;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_actual_invalid_version_and_consistent_ordering_are_accepted() {
        assert_eq!(
            decision(json!({"kind":"invalid","exception":"InvalidVersion"})).unwrap(),
            Decision::InvalidVersion
        );
        for ordering in [-1, 0, 1] {
            assert_eq!(
                decision(json!({"kind":"parsed","ordering":ordering,"newer":ordering==1})).unwrap(),
                Decision::Parsed(ordering == 1)
            );
        }
        for value in [
            json!({"kind":"invalid","exception":"MemoryError"}),
            json!({"kind":"parsed","ordering":0,"newer":true}),
            json!({"kind":"parsed","ordering":2,"newer":true}),
            json!({"kind":"invalid","exception":"InvalidVersion","extra":true}),
        ] {
            assert!(decision(value).is_err());
        }
    }
    #[tokio::test]
    async fn unneeded_dependency_never_spawns_and_closed_dependency_cannot_reopen() {
        let comparator = Comparator::new();
        assert!(comparator.0.lock().unwrap().executor.is_none());
        comparator.begin_drain(Instant::now());
        comparator.shutdown(Duration::from_secs(1)).await.unwrap();
        comparator.shutdown(Duration::from_secs(1)).await.unwrap();
        assert!(matches!(
            comparator.newer("0.33.0", "1!0.1", Arc::new(())).await,
            Err(PythonRegexError::Closed)
        ));
        assert!(comparator.0.lock().unwrap().executor.is_none());
    }
}
