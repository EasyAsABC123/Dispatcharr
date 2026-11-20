//! Actual Selected worker integration for the explicitly enabled taskless scope.
use super::{retain, Disposition, Inner, Kind, TransportError};
use crate::{
    normal_backup_taskless::{Prepared, RetainedTasklessRestore, SettlementStage},
    transport::{BackupCandidate, ClaimedBackup},
};
use dispatcharr_store::{BackupRestoreError, TasklessRestoreReservation};
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};

type Owner = Arc<dyn Send + Sync>;

pub(super) enum Held {
    Reservation(Arc<TasklessRestoreReservation>),
    Prepared(Arc<Prepared>),
    ClaimUnsettled {
        _prepared: Arc<Prepared>,
        _claim: Option<ClaimedBackup>,
    },
    Claimed(Arc<RetainedTasklessRestore>),
}

impl Held {
    pub(super) fn stage(&self) -> SettlementStage {
        match self {
            Self::Reservation(_) => SettlementStage::Preparing,
            Self::Prepared(_) => SettlementStage::PreparedPreclaim,
            Self::ClaimUnsettled { .. } => SettlementStage::ClaimUnsettled,
            Self::Claimed(scope) => scope.stage(),
        }
    }
}

fn operational(reason: &'static str) -> Disposition {
    Disposition::Operational(reason.into())
}

fn transition(
    inner: &Inner,
    expected: Option<SettlementStage>,
    next: Option<Held>,
) -> Result<(), Disposition> {
    let mut pending = inner
        .taskless_pending
        .lock()
        .map_err(|_| operational("taskless ownership slot unavailable"))?;
    if pending.as_ref().map(Held::stage) != expected {
        return Err(operational("taskless ownership transition disagreed"));
    }
    *pending = next;
    Ok(())
}

async fn close_no_effect(inner: &Inner) -> Result<(), Disposition> {
    let (stage, reservation) = {
        let pending = inner
            .taskless_pending
            .lock()
            .map_err(|_| operational("taskless ownership slot unavailable"))?;
        match pending.as_ref() {
            Some(Held::Reservation(value)) => (SettlementStage::Preparing, value.clone()),
            Some(Held::Prepared(value)) => (SettlementStage::PreparedPreclaim, value.reservation()),
            _ => return Err(operational("taskless no-effect ownership unavailable")),
        }
    };
    reservation
        .close()
        .await
        .map_err(|_| operational("taskless no-effect SQL close unsettled"))?;
    transition(inner, Some(stage), None)
}

pub(super) async fn run(
    inner: &Inner,
    candidate: BackupCandidate,
    owner: Owner,
) -> Result<Disposition, Disposition> {
    let settings = inner
        .executor
        .taskless
        .get()
        .ok_or_else(|| operational("taskless restore settings unavailable"))?;
    let plugins = inner
        .executor
        .restore_plugins
        .get()
        .ok_or_else(|| operational("taskless restore plugin owner unavailable"))?;
    let Kind::Restore(filename) = candidate.message.kind.clone() else {
        return Err(operational("taskless restore candidate kind disagreed"));
    };
    let id = candidate.message.id;
    let deadline = tokio::time::Instant::now() + settings.helper.timeout();
    let reservation = match plugins
        .prepare_taskless_restore_reservation(
            inner.executor.config.database.clone(),
            owner.clone(),
            deadline,
        )
        .await
    {
        Ok(value) => Arc::new(value),
        Err(BackupRestoreError::Capacity) => return Ok(Disposition::Capacity),
        Err(BackupRestoreError::UnsupportedPlugins | BackupRestoreError::UnsupportedSchema) => {
            return reject(inner, candidate, owner, false).await;
        }
        Err(_) => {
            retain(inner, owner);
            return Err(operational("taskless SQL admission unsettled"));
        }
    };
    if let Err(error) = transition(inner, None, Some(Held::Reservation(reservation.clone()))) {
        if reservation.close().await.is_err() {
            retain(inner, reservation);
        }
        return Err(error);
    }
    let prepared = Prepared::inspect(
        reservation,
        id,
        filename.clone(),
        inner.executor.root.join(filename),
        inner.executor.scratch.clone(),
        inner.executor.config.clone(),
        inner.executor.runtime.clone(),
        settings.authority.clone(),
        deadline,
    )
    .await;
    let prepared = match prepared {
        Ok(value) => Arc::new(value),
        Err(_) => {
            close_no_effect(inner).await?;
            return reject(inner, candidate, owner, false).await;
        }
    };
    transition(
        inner,
        Some(SettlementStage::Preparing),
        Some(Held::Prepared(prepared.clone())),
    )?;
    if inner.closed.load(Ordering::Acquire) {
        close_no_effect(inner).await?;
        return Ok(Disposition::NotDue);
    }
    match inner.revoked.contains(&id.to_string()) {
        Ok(false) => {}
        Ok(true) => {
            close_no_effect(inner).await?;
            return reject(inner, candidate, owner, true).await;
        }
        Err(_) => {
            close_no_effect(inner).await?;
            return Err(operational("taskless revocation unsettled before claim"));
        }
    }
    let claim = match inner
        .jobs
        .claim_normal_backup(candidate, owner.clone())
        .await
    {
        Ok(value) => value,
        Err(TransportError::ClaimRace) => {
            close_no_effect(inner).await?;
            return Ok(Disposition::HeadChanged);
        }
        Err(TransportError::Capacity) => {
            close_no_effect(inner).await?;
            return Ok(Disposition::Capacity);
        }
        Err(_) => {
            // The real claim may have succeeded; continue owning the file,
            // SQL fence and original worker permit. Never retry or apply here.
            transition(
                inner,
                Some(SettlementStage::PreparedPreclaim),
                Some(Held::ClaimUnsettled {
                    _prepared: prepared,
                    _claim: None,
                }),
            )?;
            return Err(operational("taskless claim aftermath unsettled"));
        }
    };
    let claimed = match prepared.clone().bind_claim(claim) {
        Ok(value) => value,
        Err((_, claim)) => {
            transition(
                inner,
                Some(SettlementStage::PreparedPreclaim),
                Some(Held::ClaimUnsettled {
                    _prepared: prepared,
                    _claim: Some(*claim),
                }),
            )?;
            return Err(operational("taskless actual claim scope disagreed"));
        }
    };
    let held = Arc::new(RetainedTasklessRestore::new(claimed, owner));
    transition(
        inner,
        Some(SettlementStage::PreparedPreclaim),
        Some(Held::Claimed(held.clone())),
    )?;
    inner.advanced.notify_one();
    let outcome = held
        .apply_through_result(
            &settings.helper,
            inner.executor.config.clone(),
            &inner.jobs,
            &inner.revoked,
        )
        .await
        .map_err(operational)?;
    transition(inner, Some(SettlementStage::SqlClosed), None)?;
    Ok(outcome)
}

async fn reject(
    inner: &Inner,
    candidate: BackupCandidate,
    owner: Owner,
    already_revoked: bool,
) -> Result<Disposition, Disposition> {
    if inner.closed.load(Ordering::Acquire) {
        return Ok(Disposition::NotDue);
    }
    let message = match inner
        .jobs
        .claim_normal_backup(candidate, owner.clone())
        .await
    {
        Ok(value) => value.into_message(),
        Err(TransportError::ClaimRace) => return Ok(Disposition::HeadChanged),
        Err(TransportError::Capacity) => return Ok(Disposition::Capacity),
        Err(_) => {
            retain(inner, owner);
            return Err(operational("taskless rejected claim aftermath unsettled"));
        }
    };
    inner.advanced.notify_one();
    let result = async {
        if already_revoked
            || inner
                .revoked
                .contains(&message.id.to_string())
                .map_err(|_| operational("taskless revocation unsettled"))?
        {
            inner
                .jobs
                .write_normal_backup_revoked(&message, owner.clone())
                .await
                .map_err(|_| operational("taskless revoked result unsettled"))?;
            return Ok(Disposition::Revoked(message.id));
        }
        let failed = json!({
            "status": "failed",
            "error": "Native restore plugin modules require an actual restore-task context",
        });
        inner
            .jobs
            .write_normal_backup(&message, failed, owner.clone())
            .await
            .map_err(|_| operational("taskless rejected result unsettled"))?;
        Ok(Disposition::Completed(message.id))
    }
    .await;
    if result.is_err() {
        retain(inner, owner);
    }
    result
}
