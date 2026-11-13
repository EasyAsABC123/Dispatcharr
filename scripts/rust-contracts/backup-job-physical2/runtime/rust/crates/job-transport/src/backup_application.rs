//! Explicit Source16 create/status adapter. Original Dispatcharr authors retain
//! application credit and licensing. No Main assembly, scheduler, restore or
//! automatic intake; caller must supply qualified archive tools/clock/handler.
use crate::{
    decode::{Metadata, State},
    protocol::Task,
    transport::{Config, Error, Jobs},
    worker::{CreateHandler, Run, Worker},
};
use dispatcharr_contracts::BoxFuture;
use dispatcharr_store::{BackupFiles, BackupJobApi, StoreError};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct HttpOwner {
    _slot: OwnedSemaphorePermit,
}
struct Inner {
    jobs: Jobs,
    worker: Worker,
    http: Arc<Semaphore>,
    closed: AtomicBool,
}
#[derive(Clone)]
pub struct BackupApplication {
    inner: Arc<Inner>,
}
impl BackupApplication {
    /// Prepare narrow backup authority without starting a worker or HTTP route.
    pub async fn prepare(
        broker: &str,
        backend: &str,
        handler: Arc<dyn CreateHandler>,
    ) -> Result<Self, Error> {
        let jobs = Jobs::connect_backup(broker, backend, Config::default()).await?;
        let worker = Worker::new_backup(jobs.clone(), handler).map_err(|_| Error::Configuration)?;
        Ok(Self {
            inner: Arc::new(Inner {
                jobs,
                worker,
                http: Arc::new(Semaphore::new(4)),
                closed: AtomicBool::new(false),
            }),
        })
    }
    pub fn attach(&self, files: BackupFiles) -> BackupFiles {
        files.with_jobs(Arc::new(self.clone()))
    }
    /// Explicit one-job poll, preserving the existing retained physical owner
    /// through early ACK, actual blocking archive work and result publication.
    /// Unknown shared heads are not consumed. Dropping this caller is not abort.
    pub async fn run_once(
        &self,
        caller: Arc<dyn Send + Sync>,
    ) -> Result<Run, crate::worker::Error> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(crate::worker::Error::Closed);
        }
        self.inner.worker.run_once(caller).await
    }
    fn lease(&self) -> Result<Arc<HttpOwner>, StoreError> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(policy_error("Backup API closed"));
        }
        let slot = self
            .inner
            .http
            .clone()
            .try_acquire_owned()
            .map_err(|_| policy_error("Backup API capacity unavailable"))?;
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(policy_error("Backup API closed"));
        }
        Ok(Arc::new(HttpOwner { _slot: slot }))
    }
}
fn policy_error(detail: &str) -> StoreError {
    StoreError::StatusBody(503, json!({"detail":detail}))
}
fn transport_error(operation: &str, error: Error) -> StoreError {
    match error {
        Error::Closed | Error::Capacity => policy_error("Backup transport capacity unavailable"),
        Error::UnsupportedTask | Error::Protocol | Error::Decode(_) => {
            StoreError::Unsupported("Unobserved backup result grammar is not migrated")
        }
        // These bounded native diagnostics do not claim Source exception text.
        _ => StoreError::StatusBody(
            500,
            json!({"detail":format!("Failed to {operation}: Native backup transport unavailable")}),
        ),
    }
}
/// Source16 normal absent/SUCCESS projection. Raised Python exceptions and
/// tagged values retain an explicit gap; no fabricated exception text.
pub fn project_status(metadata: Option<Metadata>) -> Result<Value, StoreError> {
    let Some(meta) = metadata else {
        return Ok(json!({"state":"pending"}));
    };
    if !meta.tagged_values.is_empty() {
        return Err(StoreError::Unsupported(
            "Tagged backup result grammar is not migrated",
        ));
    }
    match meta.state {
        State::Pending => Ok(json!({"state":"pending"})),
        State::Started => Ok(json!({"state":"started"})),
        State::Retry => Ok(json!({"state":"retry"})),
        State::Success => {
            let result = meta.result.as_object().ok_or(StoreError::Unsupported(
                "Non-object backup result grammar is not migrated",
            ))?;
            if result.get("status") == Some(&json!("completed")) {
                Ok(json!({"state":"completed","result":meta.result}))
            } else {
                Ok(
                    json!({"state":"failed","error":result.get("error").cloned().unwrap_or(json!("Unknown error"))}),
                )
            }
        }
        State::Failure | State::Revoked => Err(StoreError::Unsupported(
            "Raised backup task exception grammar is not migrated",
        )),
    }
}
impl BackupJobApi for BackupApplication {
    fn create(&self, caller: Arc<dyn Send + Sync>) -> BoxFuture<'_, Result<String, StoreError>> {
        Box::pin(async move {
            let owner = Arc::new((self.lease()?, caller));
            let id = Uuid::new_v4();
            self.inner
                .jobs
                .publish(&Task::CreateBackup, id, owner)
                .await
                .map_err(|e| transport_error("start backup", e))?;
            Ok(id.to_string())
        })
    }
    fn status<'a>(
        &'a self,
        task_id: &'a str,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'a, Result<Value, StoreError>> {
        Box::pin(async move {
            let owner = Arc::new((self.lease()?, caller));
            let meta = self
                .inner
                .jobs
                .read_backup(task_id, owner)
                .await
                .map_err(|e| transport_error("get task status", e))?;
            project_status(meta)
        })
    }
    fn shutdown(&self, grace: Duration) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.inner.closed.store(true, Ordering::Release);
            self.inner.http.close();
            let deadline = tokio::time::Instant::now() + grace;
            while self.inner.http.available_permits() != 4 {
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if self.inner.worker.shutdown(remaining).await.is_err() {
                return false;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            self.inner.jobs.shutdown(remaining).await.is_ok()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn vectors() -> Value {
        serde_json::from_str(include_str!("backup_source16_vectors.json")).unwrap()
    }
    #[test]
    fn actual_source16_absent_and_completed_results_project_without_new_ids_or_clocks() {
        let v = vectors();
        assert_eq!(project_status(None).unwrap(), v["actual_pending"]);
        let raw = serde_json::to_vec(&v["actual_backend"]).unwrap();
        let id = Uuid::parse_str(v["actual_backend"]["task_id"].as_str().unwrap()).unwrap();
        let metadata = crate::decode::decode(&raw, id).unwrap();
        assert_eq!(
            project_status(Some(metadata)).unwrap(),
            v["actual_completed"]
        );
    }
    #[test]
    fn unobserved_nonobject_success_and_tagged_values_keep_explicit_gaps() {
        let v = vectors();
        let id = Uuid::parse_str(v["actual_backend"]["task_id"].as_str().unwrap()).unwrap();
        let mut metadata =
            crate::decode::decode(&serde_json::to_vec(&v["actual_backend"]).unwrap(), id).unwrap();
        metadata.result = json!([]);
        assert!(matches!(
            project_status(Some(metadata)),
            Err(StoreError::Unsupported(_))
        ));
        let mut metadata =
            crate::decode::decode(&serde_json::to_vec(&v["actual_backend"]).unwrap(), id).unwrap();
        metadata.tagged_values.push(crate::decode::TaggedValue {
            kind: "bytes".to_owned(),
            encoded_value: json!("x"),
        });
        assert!(matches!(
            project_status(Some(metadata)),
            Err(StoreError::Unsupported(_))
        ));
    }
}

#[cfg(test)]
#[path = "backup_source16_peer.rs"]
mod source16_peer;

#[cfg(test)]
#[path = "backup_job_physical_tests.rs"]
mod physical_job_tests;
