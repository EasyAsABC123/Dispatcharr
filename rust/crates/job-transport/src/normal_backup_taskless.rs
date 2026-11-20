//! Bounded taskless restore under the actual Selected backup worker.
//! Original Dispatcharr, Celery and PostgreSQL contributors retain application and protocol credit.
use crate::{normal_queue::TasklessBuiltins, transport::ClaimedBackup};
use dispatcharr_backups::{
    prepare_taskless_archive, Cancellation, Error as ArchiveError, Limits, PgConfig,
    PreparedTasklessArchive, Runtime,
};
use dispatcharr_store::{StoreError, TasklessRestoreReservation};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Debug)]
pub enum Error {
    Archive(ArchiveError),
    Store(StoreError),
    ClaimMismatch,
    UnsettledPreflight,
    BuiltinAuthority,
}

/// Scoped exact staged archive and the original Main plugin/SQL/file owner.
pub struct Prepared {
    reservation: Arc<TasklessRestoreReservation>,
    expected_id: Uuid,
    expected_filename: String,
    archive: Arc<PreparedTasklessArchive>,
    authority: TasklessBuiltins,
}
impl Prepared {
    #[allow(clippy::too_many_arguments)]
    pub async fn inspect(
        reservation: Arc<TasklessRestoreReservation>,
        expected_id: Uuid,
        expected_filename: String,
        archive: PathBuf,
        scratch: PathBuf,
        config: PgConfig,
        runtime: Runtime,
        authority: TasklessBuiltins,
        deadline: tokio::time::Instant,
    ) -> Result<Self, Error> {
        if Path::new(&expected_filename).file_name() != Some(expected_filename.as_ref())
            || archive.file_name() != Some(expected_filename.as_ref())
            || config.database != reservation.current_database()
        {
            return Err(Error::ClaimMismatch);
        }
        let current = reservation.observe_registry().await.map_err(Error::Store)?;
        let builtins = authority
            .qualify(&reservation, &current.periodic)
            .map_err(|_| Error::BuiltinAuthority)?;
        let command_timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        if command_timeout.is_zero() {
            return Err(Error::Archive(ArchiveError::Timeout));
        }
        let prepared = reservation.clone();
        let cancel = Cancellation::default();
        let physical_cancel = cancel.clone();
        let mut archive = tokio::task::spawn_blocking(move || {
            prepared.check_lifetime().map_err(Error::Store)?;
            prepare_taskless_archive(
                &config,
                &runtime,
                &archive,
                &scratch,
                &Limits {
                    command_timeout,
                    ..Limits::default()
                },
                &physical_cancel,
                prepared.expected_migrations(),
                &current.plugin,
                &current.periodic,
                &builtins,
            )
            .map_err(Error::Archive)
        });
        let archive = tokio::select! {
            done = &mut archive => done.map_err(|_| Error::UnsettledPreflight)??,
            () = tokio::time::sleep_until(deadline) => {
                cancel.cancel();
                // Keep the caller and physical fence until inspection has actually returned.
                drop(archive.await.map_err(|_| Error::UnsettledPreflight)?);
                return Err(Error::Archive(ArchiveError::Timeout));
            },
        };
        reservation.revalidate().await.map_err(Error::Store)?;
        Ok(Self {
            reservation,
            expected_id,
            expected_filename,
            archive: Arc::new(archive),
            authority,
        })
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn reservation(&self) -> Arc<TasklessRestoreReservation> {
        self.reservation.clone()
    }

    /// Only the real transport CAS reply constructs the supplied claim.
    pub fn bind_claim(
        self: Arc<Self>,
        claim: ClaimedBackup,
    ) -> Result<Claimed, (Error, Box<ClaimedBackup>)> {
        if !claim.matches_restore(self.expected_id, &self.expected_filename) {
            return Err((Error::ClaimMismatch, Box::new(claim)));
        }
        if let Err(error) = self.reservation.check_lifetime() {
            return Err((Error::Store(error), Box::new(claim)));
        }
        if !self.authority.matches_owner(&self.reservation) {
            return Err((Error::BuiltinAuthority, Box::new(claim)));
        }
        Ok(Claimed {
            prepared: self,
            actual_claim: claim,
        })
    }
}

/// Binds the genuine claim to the retained staged dump and owner.
pub struct Claimed {
    prepared: Arc<Prepared>,
    actual_claim: ClaimedBackup,
}
impl Claimed {
    /// Establish the pre-effect barrier under the real CAS claim. This
    /// staging slice supplies no permission for the helper to apply or commit.
    #[cfg(target_os = "linux")]
    async fn prepare_helper(
        &self,
        helper: PathBuf,
        config: PgConfig,
        timeout: std::time::Duration,
    ) -> Result<dispatcharr_backups::ReadyGuardedRestore, Error> {
        let prepared = &self.prepared;
        if !prepared.authority.matches_owner(&prepared.reservation) {
            return Err(Error::BuiltinAuthority);
        }
        prepared
            .reservation
            .revalidate()
            .await
            .map_err(Error::Store)?;
        let reservation = prepared.reservation.clone();
        let archive = prepared.archive.clone();
        let parked = tokio::task::spawn_blocking(move || {
            reservation.check_lifetime().map_err(Error::Store)?;
            let target = reservation.target();
            archive
                .wait_guarded_ready(
                    &helper,
                    &config,
                    dispatcharr_backups::GuardedRestoreIdentity {
                        database: target.database(),
                        database_oid: target.database_oid(),
                        server_started_micros: target.server_started_micros(),
                        fence_pid: target.fence_pid(),
                        fence_started_micros: target.fence_started_micros(),
                    },
                    timeout,
                )
                .map_err(Error::Archive)
        })
        .await
        .map_err(|_| Error::UnsettledPreflight)??;
        prepared
            .reservation
            .revalidate()
            .await
            .map_err(Error::Store)?;
        Ok(parked)
    }

    /// Perform the read-only comparison after the actual restore has settled.
    /// The owner, exact staged dump and claim identity remain held by this object.
    pub async fn verify_registry_readback(&self) -> Result<bool, Error> {
        if !self
            .prepared
            .authority
            .matches_owner(&self.prepared.reservation)
        {
            return Err(Error::BuiltinAuthority);
        }
        let observed = self
            .prepared
            .reservation
            .observe_registry()
            .await
            .map_err(Error::Store)?;
        Ok(self
            .prepared
            .archive
            .matches_post_restore(&observed.plugin, &observed.periodic))
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementStage {
    Preparing,
    PreparedPreclaim,
    ClaimUnsettled,
    ApplyGranted,
    CommitMayHaveApplied,
    DatabaseCommitted,
    ResultMayHavePublished,
    ResultAcknowledged,
    SqlClosed,
}

#[cfg(target_os = "linux")]
pub(crate) struct RetainedTasklessRestore {
    claim: Claimed,
    owner: Arc<dyn Send + Sync>,
    stage: std::sync::atomic::AtomicU8,
}

#[cfg(target_os = "linux")]
impl RetainedTasklessRestore {
    pub(crate) fn new(claim: Claimed, owner: Arc<dyn Send + Sync>) -> Self {
        Self {
            claim,
            owner,
            stage: std::sync::atomic::AtomicU8::new(SettlementStage::Preparing as u8),
        }
    }

    fn mark(&self, stage: SettlementStage) {
        self.stage
            .store(stage as u8, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn stage(&self) -> SettlementStage {
        match self.stage.load(std::sync::atomic::Ordering::Acquire) {
            0 => SettlementStage::Preparing,
            1 => SettlementStage::PreparedPreclaim,
            2 => SettlementStage::ClaimUnsettled,
            3 => SettlementStage::ApplyGranted,
            4 => SettlementStage::CommitMayHaveApplied,
            5 => SettlementStage::DatabaseCommitted,
            6 => SettlementStage::ResultMayHavePublished,
            7 => SettlementStage::ResultAcknowledged,
            _ => SettlementStage::SqlClosed,
        }
    }

    pub(crate) async fn apply_through_result(
        &self,
        helper: &dispatcharr_backups::VerifiedGuardedHelper,
        config: PgConfig,
        jobs: &crate::transport::Jobs,
        revoked: &crate::normal_revocation::Revoked,
    ) -> Result<crate::milestone_dispatch::Disposition, &'static str> {
        let message = self
            .claim
            .actual_claim
            .message_for(jobs)
            .ok_or("taskless result owner changed")?;
        let task_key = message.id.to_string();
        let reservation = &self.claim.prepared.reservation;
        let check_revoked = || {
            revoked
                .contains(&task_key)
                .map_err(|_| "taskless revocation unsettled")
        };
        if check_revoked()? {
            return self.settle_revoked(jobs, message).await;
        }
        let timeout = helper.timeout();
        let helper = helper.clone();
        let path =
            tokio::task::spawn_blocking(move || helper.path().map(std::path::Path::to_path_buf))
                .await
                .map_err(|_| "taskless helper verification supervisor unsettled")?
                .map_err(|_| "taskless exact helper unavailable before launch")?;
        let ready = self
            .claim
            .prepare_helper(path, config, timeout)
            .await
            .map_err(|_| "taskless helper READY unsettled")?;
        if check_revoked()? {
            drop(ready);
            return self.settle_revoked(jobs, message).await;
        }
        reservation
            .check_lifetime()
            .map_err(|_| "taskless apply owner unavailable")?;
        self.mark(SettlementStage::ApplyGranted);
        let pending = tokio::task::spawn_blocking(move || ready.apply())
            .await
            .map_err(|_| "taskless apply supervisor unsettled")?
            .map_err(|_| "taskless apply unsettled before commit grant")?;
        if check_revoked()? {
            drop(pending);
            return self.settle_revoked(jobs, message).await;
        }
        reservation
            .check_lifetime()
            .map_err(|_| "taskless commit owner unavailable")?;
        self.mark(SettlementStage::CommitMayHaveApplied);
        tokio::task::spawn_blocking(move || pending.commit())
            .await
            .map_err(|_| "taskless commit supervisor unsettled")?
            .map_err(|_| "taskless commit receipt unsettled")?;
        self.mark(SettlementStage::DatabaseCommitted);
        reservation
            .reconcile_schema()
            .await
            .map_err(|_| "taskless committed schema reconciliation unsettled")?;
        if !self
            .claim
            .verify_registry_readback()
            .await
            .map_err(|_| "taskless committed registry unavailable")?
        {
            return Err("taskless committed registry disagreed with the archive");
        }

        let completed = serde_json::json!({"status":"completed","filename":self.claim.prepared.expected_filename});
        self.mark(SettlementStage::ResultMayHavePublished);
        let stored = jobs
            .write_normal_backup(message, completed.clone(), self.owner.clone())
            .await
            .map_err(|_| "taskless result write unsettled")?;
        if !stored {
            return Err("taskless result write unacknowledged");
        }
        self.mark(SettlementStage::ResultAcknowledged);
        let readback = jobs
            .read_normal_backup(&task_key, self.owner.clone())
            .await
            .map_err(|_| "taskless result readback unsettled")?
            .ok_or("taskless result missing after acknowledgment")?;
        if readback.task_id != message.id
            || readback.state != crate::decode::State::Success
            || readback.result != completed
        {
            return Err("taskless result readback disagreed with the exact claim");
        }
        reservation
            .close()
            .await
            .map_err(|_| "taskless acknowledged SQL close unsettled")?;
        self.mark(SettlementStage::SqlClosed);
        Ok(crate::milestone_dispatch::Disposition::Completed(
            message.id,
        ))
    }

    async fn settle_revoked(
        &self,
        jobs: &crate::transport::Jobs,
        message: &crate::normal_backup_protocol::Message,
    ) -> Result<crate::milestone_dispatch::Disposition, &'static str> {
        self.mark(SettlementStage::ResultMayHavePublished);
        let acknowledged = jobs
            .write_normal_backup_revoked(message, self.owner.clone())
            .await
            .map_err(|_| "taskless revoked result unsettled")?;
        if !acknowledged {
            return Err("taskless revoked result unacknowledged");
        }
        self.mark(SettlementStage::ResultAcknowledged);
        self.claim
            .prepared
            .reservation
            .close()
            .await
            .map_err(|_| "taskless revoked SQL close unsettled")?;
        self.mark(SettlementStage::SqlClosed);
        Ok(crate::milestone_dispatch::Disposition::Revoked(message.id))
    }
}
