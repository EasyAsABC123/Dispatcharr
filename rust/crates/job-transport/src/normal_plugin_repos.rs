//! Bounded plain normal plugin refresh; original authors retain full credit.
//! Actual startup ETA is guarded until durable reservation/restore exists.
use crate::{
    milestone_dispatch::{Disposition as D, Family, Handler, Lifecycle},
    normal_revocation::Revoked,
    transport::Jobs,
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{AuthService, PluginRefreshAdmission, PluginRepositoryRefresh};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
#[derive(Clone, Debug)]
pub(crate) struct Message {
    pub id: Uuid,
}
pub(crate) fn parse(raw: &[u8]) -> Result<Message, ()> {
    if raw.is_empty() || raw.len() > crate::protocol::MAX_ENVELOPE {
        return Err(());
    }
    let mut frame = crate::clocked_recording_envelope::json_value(raw).map_err(|_| ())?;
    if frame["headers"]["task"] != "apps.plugins.tasks.refresh_plugin_repos" {
        return Err(());
    }
    // Never ACK-and-sleep or hold the FIFO head until ETA as if that matched
    // Celery's durable reservation and later-task intake. Leave raw untouched.
    if !frame["headers"]["eta"].is_null() {
        return Err(());
    }
    frame["headers"]["task"] = json!("apps.proxy.tasks.fetch_channel_stats");
    let common = crate::stats_protocol::parse(&serde_json::to_vec(&frame).map_err(|_| ())?)
        .map_err(|_| ())?;
    Ok(Message { id: common.task_id })
}
struct Inner {
    jobs: Jobs,
    executor: PluginRepositoryRefresh,
    revoked: Revoked,
    closed: AtomicBool,
    slots: Arc<Semaphore>,
    uncertain: Mutex<Vec<Arc<Owner>>>,
}
struct Owner {
    _slot: OwnedSemaphorePermit,
    caller: Arc<dyn Send + Sync>,
    _inner: Arc<Inner>,
    admission: Mutex<Option<Arc<PluginRefreshAdmission>>>,
}
#[derive(Clone)]
pub struct Prepared {
    inner: Arc<Inner>,
}
impl Prepared {
    pub fn new(jobs: Jobs, auth: AuthService, revoked: Revoked) -> Result<Self, D> {
        if !jobs.allows_defaults() {
            return Err(D::UnsupportedDomain);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                executor: PluginRepositoryRefresh::new(auth),
                revoked,
                closed: AtomicBool::new(false),
                slots: Arc::new(Semaphore::new(1)),
                uncertain: Mutex::new(Vec::with_capacity(1)),
            }),
        })
    }
    pub fn entry(&self) -> (Family, Arc<dyn Handler>) {
        (Family::PluginRefresh, Arc::new(self.clone()))
    }
    async fn run(&self, expected: Option<Arc<[u8]>>, caller: Arc<dyn Send + Sync>) -> Result<D, D> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(D::Operational("Plugin refresh admission closed".into()));
        }
        let slot = match self.inner.slots.clone().try_acquire_owned() {
            Ok(v) => v,
            Err(_) => return Ok(D::Capacity),
        };
        let owner = Arc::new(Owner {
            _slot: slot,
            caller,
            _inner: self.inner.clone(),
            admission: Mutex::new(None),
        });
        let inner = self.inner.clone();
        let join_owner = owner.clone();
        let join_inner = inner.clone();
        match tokio::spawn(async move {
            let candidate = match inner
                .jobs
                .peek_plugin_refresh(expected.as_deref(), owner.clone())
                .await
            {
                Ok(Some(v)) => v,
                Ok(None) => return Ok(D::Empty),
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(crate::transport::Error::UnsupportedTask) => return Err(D::UnsupportedHead),
                Err(_) => return Err(D::Operational("Plugin refresh peek failed".into())),
            };
            let admission = match inner.executor.try_admit(owner.caller.clone()) {
                Ok(v) => Arc::new(v),
                Err(_) => return Ok(D::Capacity),
            };
            *owner.admission.lock().unwrap_or_else(|v| v.into_inner()) = Some(admission.clone());
            admission
                .preflight()
                .await
                .map_err(|_| D::UnsupportedDomain)?;
            if inner.closed.load(Ordering::Acquire) {
                return Err(D::Operational("Plugin refresh closed before claim".into()));
            }
            let message = match inner
                .jobs
                .claim_plugin_refresh(candidate, owner.clone())
                .await
            {
                Ok(v) => v,
                Err(crate::transport::Error::ClaimRace) => return Ok(D::HeadChanged),
                Err(_) => {
                    retain(&inner, owner);
                    return Err(D::Operational("Plugin refresh claim ambiguous".into()));
                }
            };
            let result = async {
                if inner
                    .revoked
                    .contains(&message.id.to_string())
                    .map_err(|_| D::Operational("Plugin revoked-state read failed".into()))?
                {
                    inner
                        .jobs
                        .write_plugin_refresh(
                            &message,
                            Value::Null,
                            Vec::new(),
                            true,
                            owner.clone(),
                        )
                        .await
                        .map_err(|_| D::Operational("Plugin revoked backend failed".into()))?;
                    return Ok(D::Revoked(message.id));
                }
                let actual = admission.execute().await.map_err(|_| {
                    D::Operational("Plugin refresh outside settled supported domain".into())
                })?;
                inner
                    .jobs
                    .write_plugin_refresh(&message, actual, Vec::new(), false, owner.clone())
                    .await
                    .map_err(|_| D::Operational("Plugin refresh backend failed".into()))?;
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
            Ok(value) => value,
            Err(_) => {
                retain(&join_inner, join_owner);
                Err(D::Operational(
                    "Plugin refresh join failed; physical admission retained".into(),
                ))
            }
        }
    }
}
fn retain(inner: &Inner, owner: Arc<Owner>) {
    let mut saved = inner.uncertain.lock().unwrap_or_else(|v| v.into_inner());
    if !saved.iter().any(|v| Arc::ptr_eq(v, &owner)) {
        saved.push(owner);
    }
}
impl Handler for Prepared {
    fn supports_selected_head(&self) -> bool {
        true
    }
    fn run_selected(
        &self,
        expected: Arc<[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(Some(expected), owner))
    }
    fn run_once(&self, owner: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<D, D>> {
        Box::pin(self.run(None, owner))
    }
}
impl Lifecycle for Prepared {
    fn stop_admission(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }
    fn shutdown(&self, timeout: Duration) -> BoxFuture<'_, Result<(), D>> {
        Box::pin(async move {
            self.stop_admission();
            let held = tokio::time::timeout(timeout, self.inner.slots.clone().acquire_owned())
                .await
                .map_err(|_| D::Operational("Plugin refresh physical drain incomplete".into()))?
                .map_err(|_| D::Operational("Plugin refresh permits closed".into()))?;
            drop(held);
            Ok(())
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source_startup_eta_refuses_without_durable_reservation_authority() {
        let source: Value =
            serde_json::from_str(include_str!("../datasets/source-plugin-startup-wire.json"))
                .unwrap();
        let frame = &source["receipt"]["frame"];
        assert!(parse(&serde_json::to_vec(frame).unwrap()).is_err());
        // Derived plain-Beat root parser control, not an observed Source task run.
        let mut plain = frame.clone();
        plain["headers"]["eta"] = Value::Null;
        let parsed = parse(&serde_json::to_vec(&plain).unwrap()).unwrap();
        assert_eq!(parsed.id.to_string(), plain["headers"]["id"]);
        plain["headers"]["task"] = json!("core.tasks.check_for_version_update");
        assert!(parse(&serde_json::to_vec(&plain).unwrap()).is_err());
        plain = frame.clone();
        plain["headers"]["eta"] = Value::Null;
        plain["body"] = json!("W1sxXSwge30sIHt9XQ==");
        assert!(parse(&serde_json::to_vec(&plain).unwrap()).is_err());
    }
}
