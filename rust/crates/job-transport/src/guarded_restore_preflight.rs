//! Owned, no-effect prerequisite for a future ordinary guarded restore adapter.
//! This port grants no task claim, registry authority, helper apply or commit.
//! Original Dispatcharr, Celery and PostgreSQL authors retain full credit.
use crate::normal_beat::{self, RestorePause, Scheduler};
use dispatcharr_store::{
    BackupRestoreError, PluginActions, TasklessRestoreReservation, TasklessRestoreTarget,
};
use std::sync::Arc;
use tokio::time::Instant;

#[path = "guarded_restore_retention.rs"]
mod retention;
use retention::Retained;

#[derive(Debug)]
pub enum Error {
    Scheduler(normal_beat::Error),
    Reservation(BackupRestoreError),
    Ownership,
}

/// Fields are private: callers cannot extract an effect-capable reservation.
/// Dropping the pause first quarantines its owner, retaining this reservation.
pub struct OwnedPreflight {
    pause: Option<RestorePause>,
    reservation: Arc<TasklessRestoreReservation>,
    retained: Arc<Retained<TasklessRestoreReservation>>,
}

impl OwnedPreflight {
    /// Actual held registry rows are read only through the same retained SQL
    /// session. The borrowed lease cannot outlive or consume this owner.
    pub(crate) async fn read_core_registry(
        &self,
    ) -> Result<Option<normal_beat::ReadOnlyCoreRegistry<'_>>, Error> {
        self.pause
            .as_ref()
            .ok_or(Error::Ownership)?
            .registry_sublease(&self.reservation)
            .await
            .map_err(|error| Error::Reservation(error.into()))
    }
    /// Park the actual elected Selected/Core scheduler before acquiring SQL.
    /// Existing scheduler and Store constructors supply all actual authority.
    pub async fn prepare(
        scheduler: &Scheduler,
        plugins: &PluginActions,
        expected_database: String,
        caller: Arc<dyn Send + Sync>,
        deadline: Instant,
    ) -> Result<Self, Error> {
        let retained = Arc::new(Retained::new(caller.clone()));
        let client = scheduler.restore_pause_client().map_err(Error::Scheduler)?;
        let pause = client
            .pause(retained.clone(), deadline)
            .await
            .map_err(Error::Scheduler)?;
        Self::reserve_paused(
            pause,
            retained,
            plugins,
            expected_database,
            caller,
            deadline,
        )
        .await
    }

    /// Consume only an actual already-parked, never-submitted worker pause.
    /// The original caller is retained even if this added reservation fails.
    pub(crate) async fn adopt_pause(
        pause: RestorePause,
        plugins: &PluginActions,
        expected_database: String,
        caller: Arc<dyn Send + Sync>,
        deadline: Instant,
    ) -> Result<Self, Error> {
        let retained = Arc::new(Retained::new(caller.clone()));
        let pause = pause
            .retain_noeffect(retained.clone())
            .map_err(Error::Scheduler)?;
        Self::reserve_paused(
            pause,
            retained,
            plugins,
            expected_database,
            caller,
            deadline,
        )
        .await
    }

    async fn reserve_paused(
        pause: RestorePause,
        retained: Arc<Retained<TasklessRestoreReservation>>,
        plugins: &PluginActions,
        expected_database: String,
        caller: Arc<dyn Send + Sync>,
        deadline: Instant,
    ) -> Result<Self, Error> {
        let reservation = match plugins
            .prepare_taskless_restore_reservation(expected_database, caller, deadline)
            .await
        {
            Ok(reservation) => Arc::new(reservation),
            Err(error) => {
                // Match the existing ordinary paused-restore closure contract.
                // All other errors/cancellation leave Drop to quarantine.
                if matches!(
                    &error,
                    BackupRestoreError::Capacity
                        | BackupRestoreError::UnsupportedSchema
                        | BackupRestoreError::UnsupportedPlugins
                ) {
                    pause
                        .decline_before_submission()
                        .map_err(Error::Scheduler)?;
                }
                return Err(Error::Reservation(error));
            }
        };
        // No await between receiving the reservation and attaching it to the
        // pause owner. This owner is NOT passed back into Store: no Arc cycle.
        if let Err(unretained) = retained.install(reservation.clone()) {
            // A private one-shot invariant failed: never release an unclosed
            // physical reservation by guessing that it was the old slot.
            std::mem::forget(unretained);
            return Err(Error::Ownership);
        }
        pause
            .validate_taskless_reservation(&reservation)
            .map_err(Error::Scheduler)?;
        Ok(Self {
            pause: Some(pause),
            reservation,
            retained,
        })
    }

    /// Re-observe the actual held SQL/file/host/module generation, then the pause.
    pub async fn revalidate(&self) -> Result<(), Error> {
        self.reservation
            .revalidate()
            .await
            .map_err(|error| Error::Reservation(error.into()))?;
        self.pause
            .as_ref()
            .ok_or(Error::Ownership)?
            .validate_taskless_reservation(&self.reservation)
            .map_err(Error::Scheduler)
    }

    /// Read-only identity; this does not return a claim or reservation capability.
    pub fn target(&self) -> &TasklessRestoreTarget {
        self.reservation.target()
    }

    /// This type exposes no effect entry point. Close actual SQL before releasing
    /// a known no-effect pause. Any error/drop retains and quarantines the hold.
    pub async fn decline(self) -> Result<(), Error> {
        self.revalidate().await?;
        self.reservation
            .close()
            .await
            .map_err(|error| Error::Reservation(error.into()))?;
        self.retained
            .release_closed(&self.reservation)
            .map_err(|_| Error::Ownership)?;
        // Release the closed reservation's file/host holds before ticks resume.
        // No await separates this release from the consuming pause decline.
        let Self {
            pause,
            reservation,
            retained,
        } = self;
        drop(reservation);
        drop(retained);
        pause
            .ok_or(Error::Ownership)?
            .decline_before_submission()
            .map_err(Error::Scheduler)
    }
}

/// Known untouched admission must keep the original verified no-effect decline.
pub(crate) fn decline_unconfigured(pause: RestorePause) -> Result<(), Error> {
    pause.decline_before_submission().map_err(Error::Scheduler)
}

/// The ordinary worker's real no-effect consumer. None means unqualified,
/// never permission to apply. All read-only outcomes settle before returning.
pub(crate) async fn observe_core_noeffect(
    pause: RestorePause,
    plugins: Option<&PluginActions>,
    expected_database: String,
    caller: Arc<dyn Send + Sync>,
    deadline: Instant,
) -> Result<Option<(usize, usize)>, Error> {
    let Some(plugins) = plugins else {
        decline_unconfigured(pause)?;
        return Ok(None);
    };
    if !pause.core_registry_available() {
        decline_unconfigured(pause)?;
        return Ok(None);
    }
    let preflight =
        OwnedPreflight::adopt_pause(pause, plugins, expected_database, caller, deadline).await?;
    // Convert the borrowed lease to bounded counts before consuming the owner.
    // No raw task names, IDs, plugin settings, or registry rows escape.
    let observed = match preflight.read_core_registry().await {
        Ok(Some(view)) => view
            .validate()
            .map(|_| Some(view.summary()))
            .map_err(Error::Scheduler),
        Ok(None) => Ok(None),
        Err(error) => Err(error),
    };
    preflight.decline().await?;
    observed
}
