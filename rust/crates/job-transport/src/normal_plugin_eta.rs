//! Source/Kombu delayed receipt ownership. Original authors retain full credit.
//! Waiting receipts hold ETA memory admission, never a SQL/execution-family slot.
use super::*;
use crate::plugin_eta::{Limits, Reservation};
pub(super) fn due_head(inner: &Inner) -> Option<Arc<[u8]>> {
    if inner.closed.load(Ordering::Acquire) {
        return None;
    }
    inner
        .pending
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .filter(|r| r.message().eta.is_some_and(|eta| eta <= chrono::Utc::now()))
        .min_by_key(|r| r.message().eta)
        .map(|r| Arc::from(r.receipt.queued_bytes.clone()))
}
pub(super) async fn reserve(
    inner: &Arc<Inner>,
    expected: Vec<u8>,
    owner: Arc<Owner>,
) -> Result<D, D> {
    let limits = Limits::default();
    let candidate = inner
        .jobs
        .peek_plugin_candidate(Some(&expected), owner.clone(), limits)
        .await
        .map_err(|e| match e {
            crate::transport::Error::ClaimRace => D::HeadChanged,
            crate::transport::Error::Capacity => D::Capacity,
            _ => D::Operational("Plugin ETA prepare failed".into()),
        })?
        .ok_or(D::HeadChanged)?;
    match inner
        .jobs
        .reserve_plugin_candidate_settled(candidate, owner.clone(), limits)
        .await
    {
        Ok(receipt) => {
            let id = receipt.message().id;
            // Transfer ownership only AFTER actual durable Redis reservation.
            // Keeping Owner here would retain the normal family/global admission.
            inner
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push_back(receipt.clone());
            if inner
                .revoked
                .contains(&id.to_string())
                .map_err(|_| D::Operational("Plugin ETA revoked intake read failed".into()))?
            {
                return announce_revoked(inner, &receipt, owner).await;
            }
            Ok(D::Reserved(id))
        }
        Err(crate::transport::Error::ClaimRace) => Ok(D::HeadChanged),
        Err(crate::transport::Error::Capacity) => Ok(D::Capacity),
        Err(_) => {
            retain(inner, owner);
            Err(D::Operational("Plugin ETA reservation unresolved".into()))
        }
    }
}
pub(super) async fn run_due(
    inner: Arc<Inner>,
    expected: Arc<[u8]>,
    caller: Arc<dyn Send + Sync>,
) -> Result<D, D> {
    if inner.closed.load(Ordering::Acquire) {
        return Ok(D::NotDue);
    }
    let slot = inner
        .slots
        .clone()
        .try_acquire_owned()
        .map_err(|_| D::Capacity)?;
    let owner = Arc::new(Owner {
        _slot: slot,
        caller,
        _inner: inner.clone(),
        admission: Mutex::new(None),
    });
    let held = owner.clone();
    let parent = inner.clone();
    match tokio::spawn(async move {
        let receipt = {
            inner
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .find(|r| {
                    r.receipt.queued_bytes.as_slice() == expected.as_ref()
                        && r.message().eta.is_some_and(|eta| eta <= chrono::Utc::now())
                })
                .cloned()
        };
        let Some(receipt) = receipt else {
            return Ok(D::NotDue);
        };
        if inner
            .revoked
            .contains(&receipt.message().id.to_string())
            .map_err(|_| D::Operational("Plugin ETA revoked read failed".into()))?
        {
            return announce_revoked(&inner, &receipt, owner).await;
        }
        let admission = match inner.executor.try_admit(owner.caller.clone()) {
            Ok(v) => Arc::new(v),
            Err(_) => return Ok(D::Capacity),
        };
        *owner.admission.lock().unwrap_or_else(|e| e.into_inner()) = Some(admission.clone());
        admission
            .preflight()
            .await
            .map_err(|_| D::UnsupportedDomain)?;
        if inner.closed.load(Ordering::Acquire) {
            return Ok(D::NotDue);
        }
        if inner
            .revoked
            .contains(&receipt.message().id.to_string())
            .map_err(|_| D::Operational("Plugin ETA revoked read failed".into()))?
        {
            return announce_revoked(&inner, &receipt, owner).await;
        }
        // Celery early ACK is at actual pool execution, never at timer intake.
        if inner
            .jobs
            .ack_plugin_eta(&receipt, owner.clone())
            .await
            .is_err()
        {
            retain(&inner, owner);
            return Err(D::Operational("Plugin ETA ACK unresolved".into()));
        }
        inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|r| r.delivery_tag() != receipt.delivery_tag());
        let message = receipt.message();
        let result = async {
            let value = admission.execute().await.map_err(|_| {
                D::Operational("Plugin ETA execution outside settled domain".into())
            })?;
            inner
                .jobs
                .write_plugin_refresh(message, value, Vec::new(), false, owner.clone())
                .await
                .map_err(|_| D::Operational("Plugin ETA backend failed".into()))?;
            Ok(D::Completed(message.id))
        }
        .await;
        if result.is_err() {
            retain(&inner, owner);
        }
        result
    })
    .await
    {
        Ok(v) => v,
        Err(_) => {
            retain(&parent, held);
            Err(D::Operational(
                "Plugin ETA execution join unresolved".into(),
            ))
        }
    }
}
async fn announce_revoked(
    inner: &Arc<Inner>,
    receipt: &Reservation,
    owner: Arc<Owner>,
) -> Result<D, D> {
    let id = receipt.message().id;
    // Request.execute_using_pool (including optimized subclass) checks revoked
    // before pool admission; _announce_revoked stores request metadata BEFORE ACK.
    let result = async {
        inner
            .jobs
            .write_plugin_refresh(
                receipt.message(),
                Value::Null,
                Vec::new(),
                true,
                owner.clone(),
            )
            .await
            .map_err(|_| {
                D::Operational("Plugin ETA revoked backend failed; unacked retained".into())
            })?;
        inner
            .jobs
            .ack_plugin_eta(receipt, owner.clone())
            .await
            .map_err(|_| D::Operational("Plugin ETA revoked ACK unresolved".into()))?;
        inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|r| r.delivery_tag() != receipt.delivery_tag());
        Ok(D::Revoked(id))
    }
    .await;
    if result.is_err() {
        retain(inner, owner);
    }
    result
}
pub(super) async fn restore_all(inner: &Arc<Inner>) -> Result<(), D> {
    loop {
        let next: Option<Reservation> = inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .front()
            .cloned();
        let Some(receipt) = next else { return Ok(()) };
        // Same actual queued arrival order as Kombu _delivered -> RPUSH.
        inner
            .jobs
            .restore_plugin_eta(&receipt, inner.clone())
            .await
            .map_err(|_| D::Operational("Plugin ETA warm restore unresolved".into()))?;
        inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop_front();
    }
}
