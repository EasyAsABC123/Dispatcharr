//! Actual current-schema restore orchestration on the existing backup worker.
//! Original Dispatcharr/Django/PostgreSQL authors retain all application credit.
use super::*;
use dispatcharr_backups::{Cancellation, CurrentSchemaRestoreAuthority};
use dispatcharr_store::{BackupRestoreAdmission, BackupRestoreError, StoreError};
#[derive(Default)]
pub(crate) struct RestoreEffect {
    submitted: std::sync::atomic::AtomicBool,
    no_submission_closed: std::sync::atomic::AtomicBool,
}
impl RestoreEffect {
    fn submitted(&self) -> bool {
        self.submitted.load(std::sync::atomic::Ordering::Acquire)
    }
    fn no_submission_closed(&self) -> bool {
        self.no_submission_closed
            .load(std::sync::atomic::Ordering::Acquire)
            && !self.submitted()
    }
}
struct Authority(Arc<BackupRestoreAdmission>, Option<Arc<RestoreEffect>>);
impl CurrentSchemaRestoreAuthority for Authority {
    fn before_destructive_submission(&self) {
        if let Some(effect) = &self.1 {
            effect
                .submitted
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
    fn current_database(&self) -> &str {
        self.0.current_database()
    }
    fn server_started_micros(&self) -> i64 {
        self.0.server_started_micros()
    }
    fn expected_migrations(&self) -> &std::collections::BTreeSet<(String, String)> {
        self.0.expected_migrations()
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestoreScopeDisposition {
    Close,
    RetainLive,
}
fn restore_scope_disposition(
    observed: bool,
    submitted: bool,
    failed_value: bool,
) -> RestoreScopeDisposition {
    if observed && submitted && failed_value {
        RestoreScopeDisposition::RetainLive
    } else {
        RestoreScopeDisposition::Close
    }
}
fn failed(message: &'static str) -> Value {
    json!({"status":"failed","error":message})
}
fn archive_failure(error: ArchiveError) -> Result<Value, Error> {
    match error {
        ArchiveError::Cancelled|ArchiveError::Timeout|ArchiveError::WorkerPanic => Err(Error::Supervisor),
        ArchiveError::Zip => Ok(failed("Native restore archive is not a readable ZIP backup")),
        ArchiveError::MissingMetadata => Ok(failed("Invalid backup: missing metadata.json")),
        ArchiveError::Metadata => Ok(failed("Native restore requires valid current-schema metadata and disabled plugins without task history")),
        ArchiveError::WrongDatabaseType => Ok(failed("Database type mismatch: current database is postgresql")),
        ArchiveError::SqliteDatabaseTypeMismatch => Ok(failed(
            "Database type mismatch: backup is sqlite, but current database is postgresql",
        )),
        ArchiveError::Capacity => Ok(failed("Native restore archive capacity exceeded")),
        _ => Ok(failed("Native PostgreSQL restore failed")),
    }
}
impl Executor {
    pub(crate) fn bind_restore_plugins(&self, plugins: PluginActions) -> Result<(), Error> {
        self.restore_plugins
            .set(plugins)
            .map_err(|_| Error::Configuration)
    }
    pub(crate) fn restore_ready(&self) -> bool {
        self.restore_plugins.get().is_some()
    }
    pub(crate) fn restore_unsettled(&self) -> bool {
        self.uncertain_restore
            .lock()
            .map_or(true, |v| !v.is_empty())
    }
    pub(crate) fn retain_restore(
        &self,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) {
        if let Ok(mut pending) = self.uncertain_restore.lock() {
            if !pending
                .iter()
                .any(|(existing, _)| Arc::ptr_eq(existing, &admission))
            {
                pending.push((admission, owner));
            }
        }
    }
    pub(crate) async fn prepare_restore(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Arc<BackupRestoreAdmission>, BackupRestoreError> {
        self.prepare_restore_until(
            owner,
            tokio::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .await
    }
    async fn prepare_restore_until(
        &self,
        owner: Arc<dyn Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<BackupRestoreAdmission>, BackupRestoreError> {
        let plugins = self.restore_plugins.get().ok_or_else(|| {
            BackupRestoreError::Store(Box::new(StoreError::Unsupported(
                "Restore plugin reconciliation port is not bound",
            )))
        })?;
        plugins
            .require_empty_restore_plugins(owner.clone())
            .await
            .map_err(|_| BackupRestoreError::UnsupportedPlugins)?;
        self.auth
            .prepare_backup_restore(self.config.database.clone(), owner, deadline)
            .await
            .map(Arc::new)
    }
    pub(crate) async fn release_restore(
        &self,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if admission.close().await.is_err() {
            self.retain_restore(admission, owner);
            return Err(Error::Supervisor);
        }
        Ok(())
    }
    pub(crate) async fn restore(
        &self,
        filename: String,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, Error> {
        self.restore_with_effect(filename, admission, owner, None)
            .await
    }
    async fn restore_with_effect(
        &self,
        filename: String,
        admission: Arc<BackupRestoreAdmission>,
        owner: Arc<dyn Send + Sync>,
        effect: Option<Arc<RestoreEffect>>,
    ) -> Result<Value, Error> {
        let plugins = self
            .restore_plugins
            .get()
            .ok_or(Error::Configuration)?
            .clone();
        let scoped = admission.clone();
        let physical = owner.clone();
        let owned = self.clone();
        let archive_name = filename.clone();
        let observed_effect = effect.clone();
        let archive = tokio::task::spawn_blocking(move || {
            let _owner = physical;
            dispatcharr_backups::restore_current_schema(
                &owned.config,
                &owned.runtime,
                &owned.root.join(archive_name),
                &owned.scratch,
                &Limits::default(),
                &Cancellation::default(),
                &Authority(scoped, observed_effect),
            )
        })
        .await;
        let mut value = match archive {
            Ok(Ok(())) => None,
            Ok(Err(error)) => match archive_failure(error) {
                Ok(value) => Some(value),
                Err(error) => {
                    self.retain_restore(admission, owner);
                    return Err(error);
                }
            },
            Err(_) => {
                self.retain_restore(admission, owner);
                return Err(Error::Supervisor);
            }
        };
        if value.is_none() {
            match admission.reconcile_current_schema().await {
                Ok(()) => {}
                Err(BackupRestoreError::UnsupportedSchema) => {
                    value = Some(failed("Native restored schema is incompatible"))
                }
                Err(BackupRestoreError::Store(error))
                    if matches!(&*error, StoreError::Database(sqlx::Error::Database(_))) =>
                {
                    value = Some(failed("Native restored database reconciliation failed"))
                }
                Err(_) => {
                    self.retain_restore(admission, owner);
                    return Err(Error::Supervisor);
                }
            }
        }
        if value.is_none() {
            match plugins
                .reconcile_backup_restore(admission.clone(), owner.clone())
                .await
            {
                Ok(()) => {}
                // Source plugin post_migrate receiver catches real SQL exceptions.
                // Native capability guards/unknown physical outcomes remain errors.
                Err(StoreError::Database(sqlx::Error::Database(_))) => {}
                Err(_) => {
                    self.retain_restore(admission, owner);
                    return Err(Error::Supervisor);
                }
            }
        }
        // A failed wire Value after destructive submission does not establish a
        // healthy schema. Retain BOTH live SQL admission and parked Beat owner
        // before returning that unchanged result for ordinary Redis publication.
        if restore_scope_disposition(
            effect.is_some(),
            effect.as_ref().is_some_and(|e| e.submitted()),
            value.is_some(),
        ) == RestoreScopeDisposition::RetainLive
        {
            self.retain_restore(admission, owner);
            return Ok(value.expect("failed Value selected by typed disposition"));
        }
        if admission.close().await.is_err() {
            self.retain_restore(admission, owner);
            return Err(Error::Supervisor);
        }
        if value.is_some() {
            if let Some(effect) = &effect {
                // Only reached after a joined, typed archive refusal and exact
                // admission.close. Panic/timeout/uncertain branches returned
                // above and cannot manufacture this settlement receipt.
                if !effect.submitted() {
                    effect
                        .no_submission_closed
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
        Ok(value.unwrap_or_else(|| json!({"status":"completed","filename":filename})))
    }
}

#[cfg(test)]
mod archive_failure_tests {
    use super::*;

    #[test]
    fn normal_restore_reports_the_two_observed_source_refusals() {
        for (error, message) in [
            (
                ArchiveError::MissingMetadata,
                "Invalid backup: missing metadata.json",
            ),
            (
                ArchiveError::SqliteDatabaseTypeMismatch,
                "Database type mismatch: backup is sqlite, but current database is postgresql",
            ),
        ] {
            assert_eq!(
                archive_failure(error).unwrap(),
                json!({"status":"failed","error":message}),
            );
        }
    }
}

/// Owned draft port: must replace the worker's plain restore scope before use.
/// This cannot be reconstructed from a filename, UUID or a caller-written proof.
pub(crate) struct PausedRestore {
    executor: Executor,
    admission: Arc<BackupRestoreAdmission>,
    pause: Option<crate::normal_beat::RestorePause>,
    settled: bool,
    owner: Arc<dyn Send + Sync>,
}
pub(crate) enum PausedPrepareError {
    Scheduler,
    Admission(BackupRestoreError),
}
fn guarded_preflight_error(error: crate::guarded_restore_preflight::Error) -> PausedPrepareError {
    match error {
        crate::guarded_restore_preflight::Error::Reservation(error) => {
            PausedPrepareError::Admission(error)
        }
        crate::guarded_restore_preflight::Error::Scheduler(_)
        | crate::guarded_restore_preflight::Error::Ownership => PausedPrepareError::Scheduler,
    }
}
impl Executor {
    pub(crate) fn owns_core_restore_reservation(
        &self,
        reservation: &dispatcharr_store::TasklessRestoreReservation,
    ) -> bool {
        reservation.belongs_to_auth(&self.auth)
    }
    async fn observe_guarded_preflight(
        &self,
        pause: crate::normal_beat::RestorePause,
        owner: Arc<dyn Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<(), PausedPrepareError> {
        let observed = crate::guarded_restore_preflight::observe_core_noeffect(
            pause,
            self.restore_plugins.get(),
            self.config.database.clone(),
            owner,
            deadline,
        )
        .await
        .map_err(guarded_preflight_error)?;
        let Some((rows, families)) = observed else {
            return Err(PausedPrepareError::Admission(
                BackupRestoreError::UnsupportedPlugins,
            ));
        };
        // Bounded internal receipt only; the outer worker still refuses restore.
        eprintln!("Read-only guarded-restore snapshot closed: rows={rows} families={families}");
        Ok(())
    }

    pub(crate) async fn prepare_paused_restore(
        &self,
        scheduler: &crate::normal_beat::RestorePauseClient,
        owner: Arc<dyn Send + Sync>,
        deadline: tokio::time::Instant,
    ) -> Result<PausedRestore, PausedPrepareError> {
        // No pool fence until the elected scheduler has synced and parked.
        // Cancellation retains pause; the worker must retain its SQL owner too.
        let pause = scheduler
            .pause(owner.clone(), deadline)
            .await
            .map_err(|_| PausedPrepareError::Scheduler)?;
        let admission = match self.prepare_restore_until(owner.clone(), deadline).await {
            Ok(admission) => admission,
            Err(BackupRestoreError::UnsupportedPlugins) => {
                // This is still the original unclaimed, no-effect parked scope.
                // Exercise the real guarded reservation prerequisite, settle it,
                // and retain the existing refusal until registry/effects qualify.
                self.observe_guarded_preflight(pause, owner, deadline)
                    .await?;
                return Err(PausedPrepareError::Admission(
                    BackupRestoreError::UnsupportedPlugins,
                ));
            }
            Err(error) => {
                // These existing typed outcomes occur before destructive submit;
                // store admission returns them only after acquired scope closure.
                if matches!(
                    &error,
                    BackupRestoreError::Capacity
                        | BackupRestoreError::UnsupportedSchema
                        | BackupRestoreError::UnsupportedPlugins
                ) {
                    pause
                        .decline_before_submission()
                        .map_err(|_| PausedPrepareError::Scheduler)?;
                }
                // Other failures drop/quarantine pause: closure may be uncertain.
                return Err(PausedPrepareError::Admission(error));
            }
        };
        if pause.validate_admission(&admission).is_err() {
            self.retain_restore(admission, owner);
            return Err(PausedPrepareError::Scheduler);
        }
        Ok(PausedRestore {
            executor: self.clone(),
            admission,
            pause: Some(pause),
            settled: false,
            owner,
        })
    }
}
impl Drop for PausedRestore {
    fn drop(&mut self) {
        if !self.settled {
            self.executor
                .retain_restore(self.admission.clone(), self.owner.clone());
        }
        // Token field Drop retains/quarantines; never auto-resume on cancellation.
    }
}
impl PausedRestore {
    /// Closed intake, declined claim, claim race, capacity or revoke BEFORE
    /// destructive submission. Real SQL closure must succeed before resume.
    pub(crate) async fn decline_unstarted(mut self) -> Result<(), Error> {
        self.executor
            .release_restore(self.admission.clone(), self.owner.clone())
            .await?;
        self.pause
            .take()
            .ok_or(Error::Supervisor)?
            .decline_before_submission()
            .map_err(|_| Error::Supervisor)?;
        self.settled = true;
        Ok(())
    }
    pub(crate) async fn execute(mut self, filename: String) -> Result<Value, Error> {
        let effect = Arc::new(RestoreEffect::default());
        // Per-owned-execution marker is carried into the actual blocking authority.
        let value = self
            .executor
            .restore_with_effect(
                filename,
                self.admission.clone(),
                self.owner.clone(),
                Some(effect.clone()),
            )
            .await?;
        if effect.submitted() {
            self.pause
                .as_mut()
                .ok_or(Error::Supervisor)?
                .destructive_submission();
        }
        if value.get("status").and_then(Value::as_str) == Some("completed") {
            // Existing restore returns completed only after schema/plugin
            // reconciliation and actual admission.close have succeeded.
            self.pause
                .take()
                .ok_or(Error::Supervisor)?
                .complete_reconciled()
                .map_err(|_| Error::Supervisor)?;
            self.settled = true;
        } else if effect.no_submission_closed() {
            self.pause
                .take()
                .ok_or(Error::Supervisor)?
                .decline_before_submission()
                .map_err(|_| Error::Supervisor)?;
            self.settled = true;
        }
        // Every ambiguous response/error still leaves token Drop to quarantine.
        Ok(value)
    }
}

/// Keeps SQL and reversible pause ownership in one worker scope. Plain remains
/// exactly the old path when no ordinary Core weak binding is installed.
pub(crate) struct WorkerRestoreScope {
    executor: Executor,
    admission: Arc<BackupRestoreAdmission>,
    owner: Arc<dyn Send + Sync>,
    paused: Option<PausedRestore>,
    bound: bool,
}
impl Executor {
    pub(crate) fn bind_restore_scheduler(
        &self,
        scheduler: &crate::normal_beat::Scheduler,
    ) -> Result<(), Error> {
        let client = scheduler
            .restore_pause_client()
            .map_err(|_| Error::Configuration)?;
        self.restore_scheduler
            .set(client)
            .map_err(|_| Error::Configuration)
    }
    pub(crate) async fn prepare_worker_restore(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<WorkerRestoreScope, BackupRestoreError> {
        if let Some(client) = self.restore_scheduler.get() {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let paused = self
                .prepare_paused_restore(client, owner.clone(), deadline)
                .await
                .map_err(|e| match e {
                    PausedPrepareError::Admission(error) => error,
                    // No broker claim occurred; an unowned/uncertain pause cannot
                    // license a restore. Next attempts still face the held gate.
                    PausedPrepareError::Scheduler => BackupRestoreError::Capacity,
                })?;
            Ok(WorkerRestoreScope {
                executor: self.clone(),
                admission: paused.admission.clone(),
                owner,
                paused: Some(paused),
                bound: true,
            })
        } else {
            let admission = self.prepare_restore(owner.clone()).await?;
            Ok(WorkerRestoreScope {
                executor: self.clone(),
                admission,
                owner,
                paused: None,
                bound: false,
            })
        }
    }
}
impl WorkerRestoreScope {
    pub(crate) async fn decline_unstarted(mut self) -> Result<(), Error> {
        if self.bound {
            self.paused
                .take()
                .ok_or(Error::Supervisor)?
                .decline_unstarted()
                .await
        } else {
            self.executor
                .release_restore(self.admission.clone(), self.owner.clone())
                .await
        }
    }
    pub(crate) fn retain_uncertain(self) {
        self.executor
            .retain_restore(self.admission.clone(), self.owner.clone());
        // If bound, token Drop quarantines; there is deliberately no resume.
    }
    pub(crate) async fn execute(&mut self, filename: String) -> Result<Value, Error> {
        if self.bound {
            self.paused
                .take()
                .ok_or(Error::Supervisor)?
                .execute(filename)
                .await
        } else {
            self.executor
                .restore(filename, self.admission.clone(), self.owner.clone())
                .await
        }
    }
}

#[cfg(test)]
mod restore_effect_tests {
    use super::*;
    use std::sync::atomic::Ordering;
    #[test]
    fn untouched_marker_is_not_a_settled_no_submission_receipt() {
        let effect = RestoreEffect::default();
        assert!(!effect.submitted());
        assert!(!effect.no_submission_closed());
        // Worker panic/timeout cannot create a physical close receipt.
        for error in [
            ArchiveError::Timeout,
            ArchiveError::WorkerPanic,
            ArchiveError::Cancelled,
        ] {
            assert!(archive_failure(error).is_err());
            assert!(!effect.no_submission_closed());
        }
    }
    #[test]
    fn submitted_marker_cannot_resume_as_no_submission_even_with_close() {
        let effect = RestoreEffect::default();
        effect.submitted.store(true, Ordering::Release);
        effect.no_submission_closed.store(true, Ordering::Release);
        assert!(!effect.no_submission_closed());
    }
    #[test]
    fn exact_closed_no_submission_receipt_is_per_execution() {
        let first = RestoreEffect::default();
        let second = RestoreEffect::default();
        first.no_submission_closed.store(true, Ordering::Release);
        assert!(first.no_submission_closed());
        assert!(!second.no_submission_closed());
        assert_eq!(
            archive_failure(ArchiveError::Zip).unwrap(),
            json!({"status":"failed","error":"Native restore archive is not a readable ZIP backup"})
        );
    }
}

#[cfg(test)]
mod live_admission_disposition_tests {
    use super::*;
    #[test]
    fn submitted_failed_value_never_authorizes_close() {
        let wire = json!({"status":"failed","error":"Native restored schema is incompatible"});
        assert_eq!(
            restore_scope_disposition(true, true, true),
            RestoreScopeDisposition::RetainLive
        );
        assert_eq!(wire["status"], "failed");
    }
    #[test]
    fn no_submission_refusal_authorizes_exact_close() {
        assert_eq!(
            restore_scope_disposition(true, false, true),
            RestoreScopeDisposition::Close
        );
    }
    #[test]
    fn reconciled_success_authorizes_exact_close() {
        assert_eq!(
            restore_scope_disposition(true, true, false),
            RestoreScopeDisposition::Close
        );
    }
    #[test]
    fn unobserved_callers_keep_prior_disposition() {
        for submitted in [false, true] {
            for failed in [false, true] {
                assert_eq!(
                    restore_scope_disposition(false, submitted, failed),
                    RestoreScopeDisposition::Close
                );
            }
        }
    }
}

#[cfg(test)]
mod guarded_consumer_tests {
    use super::*;
    #[test]
    fn no_effect_preflight_errors_cannot_grant_restore_or_hide_store_uncertainty() {
        use crate::guarded_restore_preflight::Error as Guarded;
        assert!(matches!(
            guarded_preflight_error(Guarded::Ownership),
            PausedPrepareError::Scheduler
        ));
        assert!(matches!(
            guarded_preflight_error(Guarded::Scheduler(crate::normal_beat::Error::Drain)),
            PausedPrepareError::Scheduler
        ));
        assert!(matches!(
            guarded_preflight_error(Guarded::Reservation(BackupRestoreError::UnsupportedPlugins)),
            PausedPrepareError::Admission(BackupRestoreError::UnsupportedPlugins)
        ));
        assert!(matches!(
            guarded_preflight_error(Guarded::Reservation(BackupRestoreError::Store(Box::new(
                StoreError::Unsupported("unit uncertainty")
            )))),
            PausedPrepareError::Admission(BackupRestoreError::Store(_))
        ));
    }
}
