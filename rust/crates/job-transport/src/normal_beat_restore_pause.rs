//! Owned draft. Original Dispatcharr and dependency authors retain full credit.
//! No constructor changes defaults; the restore worker must explicitly bind this port.
use super::*;
use std::sync::Weak;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Phase {
    Active,
    Requested,
    Parked,
    Resume,
    SettledOnStop,
    Quarantined,
}
struct State {
    phase: Phase,
    elected: bool,
    stopped: bool,
    identity: Option<(String, i64)>,
}
struct Shared {
    state: StateMutex<State>,
    changes: watch::Sender<Phase>,
    // A cancelled restore must not silently relinquish its physical caller.
    retained: StateMutex<Vec<Arc<dyn Send + Sync>>>,
}
pub(super) struct Control {
    shared: Arc<Shared>,
    pub(super) changes: watch::Sender<Phase>,
}
impl Control {
    pub(super) fn new() -> Self {
        let changes = watch::channel(Phase::Active).0;
        Self {
            shared: Arc::new(Shared {
                state: StateMutex::new(State {
                    phase: Phase::Active,
                    elected: false,
                    stopped: false,
                    identity: None,
                }),
                changes: changes.clone(),
                retained: StateMutex::new(Vec::new()),
            }),
            changes,
        }
    }
    pub(super) fn blocked(&self) -> bool {
        self.shared
            .state
            .lock()
            .map_or(true, |s| s.phase != Phase::Active)
    }
    pub(super) fn stopped(&self) -> bool {
        self.shared.state.lock().map_or(true, |s| s.stopped)
    }
    pub(super) fn settled_on_stop(&self) -> bool {
        self.shared
            .state
            .lock()
            .is_ok_and(|s| s.stopped && s.phase == Phase::SettledOnStop)
    }
    pub(super) fn stop(&self) {
        if let Ok(mut s) = self.shared.state.lock() {
            s.stopped = true;
            if s.phase == Phase::Requested {
                s.phase = Phase::Active;
            } else if s.phase == Phase::Resume {
                s.phase = Phase::SettledOnStop;
            }
            self.changes.send_replace(s.phase);
        }
    }
    fn acknowledge(&self, identity: (String, i64)) -> Result<(), Error> {
        let mut s = self.shared.state.lock().map_err(|_| Error::Supervisor)?;
        if s.phase == Phase::Requested {
            if s.stopped {
                s.phase = Phase::Active;
            } else {
                s.identity = Some(identity);
                s.phase = Phase::Parked;
            }
            self.changes.send_replace(s.phase);
        }
        Ok(())
    }
    pub(super) fn elected(&self, elected: bool) {
        if let Ok(mut s) = self.shared.state.lock() {
            s.elected = elected;
        }
    }
}
#[derive(Clone)]
pub(crate) struct RestorePauseClient {
    shared: Weak<Shared>,
}
/// No Clone and no implicit resume. A dropped token quarantines the parked owner.
pub(crate) struct RestorePause {
    shared: Arc<Shared>,
    owner: Option<Arc<dyn Send + Sync>>,
    destructive: bool,
    disposed: bool,
}
impl Drop for RestorePause {
    fn drop(&mut self) {
        if self.disposed {
            return;
        }
        if let Ok(mut s) = self.shared.state.lock() {
            // No acknowledgement means this request never held a restore scope.
            // Withdrawal races the owner's acknowledgement under THIS mutex.
            if s.phase == Phase::Requested {
                s.phase = Phase::Active;
                self.shared.changes.send_replace(s.phase);
                return;
            }
            if s.phase == Phase::Active && !self.destructive {
                return;
            }
            s.phase = Phase::Quarantined;
            self.shared.changes.send_replace(s.phase);
        }
        if let Some(owner) = self.owner.take() {
            // Poison is itself uncertainty: retain the caller rather than release it.
            match self.shared.retained.lock() {
                Ok(mut retained) => retained.push(owner),
                Err(_) => std::mem::forget(owner),
            }
        }
    }
}
impl RestorePauseClient {
    pub(crate) async fn pause(
        &self,
        owner: Arc<dyn Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<RestorePause, Error> {
        let shared = self.shared.upgrade().ok_or(Error::Closed)?;
        let mut changes = shared.changes.subscribe();
        {
            let mut s = shared.state.lock().map_err(|_| Error::Supervisor)?;
            // A standby cannot pause another process's actual elected session.
            if !s.elected || s.stopped || s.phase != Phase::Active {
                return Err(Error::Capacity);
            }
            s.phase = Phase::Requested;
            shared.changes.send_replace(s.phase);
        }
        let token = RestorePause {
            shared,
            owner: Some(owner),
            destructive: false,
            disposed: false,
        };
        // Cancellation drops token and quarantines; no detached false acknowledgement.
        tokio::time::timeout_at(deadline, async {
            loop {
                let phase = *changes.borrow_and_update();
                match phase {
                    Phase::Parked => return Ok(()),
                    Phase::Requested => {}
                    _ => return Err(Error::Drain),
                }
                changes.changed().await.map_err(|_| Error::Closed)?;
            }
        })
        .await
        .map_err(|_| Error::Drain)??;
        Ok(token)
    }
}
impl RestorePause {
    pub(crate) fn validate_admission(
        &self,
        admission: &dispatcharr_store::BackupRestoreAdmission,
    ) -> Result<(), Error> {
        let s = self.shared.state.lock().map_err(|_| Error::Supervisor)?;
        let matches = s.identity.as_ref().is_some_and(|(database, server)| {
            database == admission.current_database() && *server == admission.server_started_micros()
        });
        if s.phase != Phase::Parked || !s.elected || !matches {
            return Err(Error::Configuration(
                "restore scheduler SQL authority mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) fn destructive_submission(&mut self) {
        self.destructive = true;
    }
    fn resume(mut self, reconciled: bool) -> Result<(), Error> {
        if self.destructive != reconciled {
            return Err(Error::Drain);
        }
        {
            let mut s = self.shared.state.lock().map_err(|_| Error::Supervisor)?;
            if !s.elected || s.phase != Phase::Parked {
                return Err(Error::Drain);
            }
            s.phase = if s.stopped {
                Phase::SettledOnStop
            } else {
                Phase::Resume
            };
            self.shared.changes.send_replace(s.phase);
        }
        self.disposed = true;
        Ok(())
    }
    // Only the owning restore adapter may call these after exact SQL close.
    pub(crate) fn decline_before_submission(self) -> Result<(), Error> {
        self.resume(false)
    }
    pub(crate) fn complete_reconciled(self) -> Result<(), Error> {
        self.resume(true)
    }
}
impl Scheduler {
    pub(crate) fn restore_pause_client(&self) -> Result<RestorePauseClient, Error> {
        if !self.inner.scope.uses_selected_receivers() {
            return Err(Error::Configuration(
                "restore pause requires ordinary Selected/Core",
            ));
        }
        Ok(RestorePauseClient {
            shared: Arc::downgrade(&self.inner.restore_pause.shared),
        })
    }
    pub(super) async fn park_for_restore(
        &self,
        connection: &mut PgConnection,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<bool, Error> {
        let control = &self.inner.restore_pause;
        let mut changes = control.changes.subscribe();
        // This is the existing elected connection, not a second checkpoint writer.
        self.finish_owned(connection, Ok(())).await?;
        let identity: (String, i64) = tokio::time::timeout(
            self.inner.pool.options().get_acquire_timeout(),
            sqlx::query_as("SELECT current_database(),(extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint").fetch_one(&mut *connection),
        ).await.map_err(|_| Error::RoleDatabaseTimeout)??;
        control.acknowledge(identity)?;
        loop {
            let phase = *changes.borrow_and_update();
            if phase == Phase::SettledOnStop {
                return Ok(false);
            }
            if phase == Phase::Active {
                return Ok(!control.stopped());
            }
            if phase == Phase::Parked {
                self.inner.lifecycle.status.send_replace(Status::Acquired);
            }
            if phase == Phase::Resume {
                connection.clear_cached_statements().await?;
                let mut cache = self.inner.cache.lock().await;
                if !cache.dirty.is_empty() {
                    return Err(Error::Drain);
                }
                *cache = Cache::default();
                let mut s = control.shared.state.lock().map_err(|_| Error::Supervisor)?;
                if s.stopped && s.phase == Phase::SettledOnStop {
                    return Ok(false);
                }
                if s.phase != Phase::Resume {
                    return Err(Error::Drain);
                }
                s.identity = None;
                s.phase = Phase::Active;
                control.changes.send_replace(s.phase);
                return Ok(true);
            }
            tokio::select! {
                _ = changes.changed() => {},
                _ = stop.changed(), if !control.stopped() => { control.stop(); },
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn parked() -> RestorePause {
        let c = Control::new();
        c.elected(true);
        c.shared.state.lock().unwrap().phase = Phase::Parked;
        RestorePause {
            shared: c.shared,
            owner: Some(Arc::new(())),
            destructive: false,
            disposed: false,
        }
    }
    #[test]
    fn cancel_never_resumes_or_drops_owner() {
        let token = parked();
        let shared = token.shared.clone();
        drop(token);
        assert_eq!(shared.state.lock().unwrap().phase, Phase::Quarantined);
        assert_eq!(shared.retained.lock().unwrap().len(), 1);
    }
    #[test]
    fn known_unstarted_decline_resumes() {
        let token = parked();
        let shared = token.shared.clone();
        token.decline_before_submission().unwrap();
        assert_eq!(shared.state.lock().unwrap().phase, Phase::Resume);
    }
    #[test]
    fn ambiguous_submission_cannot_decline() {
        let mut token = parked();
        let shared = token.shared.clone();
        token.destructive_submission();
        assert!(token.decline_before_submission().is_err());
        assert_eq!(shared.state.lock().unwrap().phase, Phase::Quarantined);
    }
    #[test]
    fn only_submitted_reconciled_success_resumes() {
        let mut token = parked();
        let shared = token.shared.clone();
        token.destructive_submission();
        token.complete_reconciled().unwrap();
        assert_eq!(shared.state.lock().unwrap().phase, Phase::Resume);
        let token = parked();
        assert!(token.complete_reconciled().is_err());
    }
    #[test]
    fn service_stop_allows_verified_decline_without_ticks() {
        let token = parked();
        let shared = token.shared.clone();
        shared.state.lock().unwrap().stopped = true;
        token.decline_before_submission().unwrap();
        assert_eq!(shared.state.lock().unwrap().phase, Phase::SettledOnStop);
    }
    #[tokio::test]
    async fn standby_cannot_borrow_another_owner() {
        let c = Control::new();
        let client = RestorePauseClient {
            shared: Arc::downgrade(&c.shared),
        };
        assert!(matches!(
            client
                .pause(Arc::new(()), tokio::time::Instant::now())
                .await,
            Err(Error::Capacity)
        ));
    }
    #[tokio::test]
    async fn expired_request_withdraws_without_ack() {
        let c = Control::new();
        c.elected(true);
        let client = RestorePauseClient {
            shared: Arc::downgrade(&c.shared),
        };
        assert!(client
            .pause(Arc::new(()), tokio::time::Instant::now())
            .await
            .is_err());
        assert_eq!(c.shared.state.lock().unwrap().phase, Phase::Active);
        assert_eq!(c.shared.retained.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL in DISPATCHARR_BEAT_TEST_DATABASE_URL"]
    async fn elected_session_denies_second_owner_until_actual_settlement() {
        let url = std::env::var("DISPATCHARR_BEAT_TEST_DATABASE_URL").unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        let owner = BeatSession::connect(&pool)
            .await
            .unwrap()
            .try_claim()
            .await
            .unwrap()
            .unwrap();
        // The paused design retains THIS session, including across restore.
        for _ in 0..2 {
            assert!(BeatSession::connect(&pool)
                .await
                .unwrap()
                .try_claim()
                .await
                .unwrap()
                .is_none());
        }
        owner.settle(Ok(())).await.unwrap();
        let next = BeatSession::connect(&pool)
            .await
            .unwrap()
            .try_claim()
            .await
            .unwrap()
            .unwrap();
        next.settle(Ok(())).await.unwrap();
        pool.close().await;
    }

    #[test]
    fn withdraw_wins_ack_race_without_restore_admission() {
        let c = Control::new();
        c.elected(true);
        c.shared.state.lock().unwrap().phase = Phase::Requested;
        let token = RestorePause {
            shared: c.shared.clone(),
            owner: Some(Arc::new(())),
            destructive: false,
            disposed: false,
        };
        drop(token);
        c.acknowledge(("inert-state-only".into(), 1)).unwrap();
        assert_eq!(c.shared.state.lock().unwrap().phase, Phase::Active);
    }
    #[test]
    fn ack_wins_cancel_race_retains_uncertainty() {
        let c = Control::new();
        c.elected(true);
        c.shared.state.lock().unwrap().phase = Phase::Requested;
        let token = RestorePause {
            shared: c.shared.clone(),
            owner: Some(Arc::new(())),
            destructive: false,
            disposed: false,
        };
        c.acknowledge(("inert-state-only".into(), 1)).unwrap();
        drop(token);
        assert_eq!(c.shared.state.lock().unwrap().phase, Phase::Quarantined);
    }
    #[test]
    fn stop_verified_success_settles_without_restarting() {
        let mut token = parked();
        let shared = token.shared.clone();
        token.destructive_submission();
        shared.state.lock().unwrap().stopped = true;
        token.complete_reconciled().unwrap();
        assert_eq!(shared.state.lock().unwrap().phase, Phase::SettledOnStop);
    }
    #[test]
    fn stop_uncertain_submission_stays_quarantined() {
        let mut token = parked();
        let shared = token.shared.clone();
        token.destructive_submission();
        shared.state.lock().unwrap().stopped = true;
        drop(token);
        assert_eq!(shared.state.lock().unwrap().phase, Phase::Quarantined);
    }
    #[tokio::test]
    async fn stop_rejects_new_pause_before_owner_observes_stop() {
        let c = Control::new();
        c.elected(true);
        c.stop();
        let client = RestorePauseClient {
            shared: Arc::downgrade(&c.shared),
        };
        assert!(matches!(
            client
                .pause(Arc::new(()), tokio::time::Instant::now())
                .await,
            Err(Error::Capacity)
        ));
        assert_eq!(c.shared.state.lock().unwrap().phase, Phase::Active);
    }

    #[test]
    fn stop_withdraws_unacknowledged_request() {
        let c = Control::new();
        c.elected(true);
        c.shared.state.lock().unwrap().phase = Phase::Requested;
        let token = RestorePause {
            shared: c.shared.clone(),
            owner: Some(Arc::new(())),
            destructive: false,
            disposed: false,
        };
        c.stop();
        drop(token);
        assert_eq!(c.shared.state.lock().unwrap().phase, Phase::Active);
        assert_eq!(c.shared.retained.lock().unwrap().len(), 0);
    }
}
