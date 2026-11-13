//! Normal inert local-VODLogo service.
//! Original Dispatcharr authors retain all application/algorithm/license credit.
//! No transport authority or task/graph registration follows from this service.
use dispatcharr_contracts::UpdateEventSink;
use dispatcharr_store::recording_local_artwork::{self as persistence, LocalArtworkStore};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum Error {
    Sql(persistence::Error),
    Event,
    Supervisor,
}
pub struct Service {
    store: Arc<LocalArtworkStore>,
    events: Arc<dyn UpdateEventSink>,
}
pub struct Report {
    recording: i64,
    changed: bool,
}
impl Report {
    pub fn recording(&self) -> i64 {
        self.recording
    }
    pub fn changed(&self) -> bool {
        self.changed
    }
    pub fn value(&self) -> &'static str {
        "ok"
    }
}
struct Receipt {
    _owner: Arc<dyn Send + Sync>,
    done: Option<oneshot::Sender<()>>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
impl Service {
    pub fn new(store: Arc<LocalArtworkStore>, events: Arc<dyn UpdateEventSink>) -> Self {
        Self { store, events }
    }
    pub async fn prepare(
        &self,
        recording: i64,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<persistence::Prepared, Error> {
        self.store
            .clone()
            .prepare(recording, owner)
            .await
            .map_err(Error::Sql)
    }
    /// Call only after the task's original raw CAS/ACK under the same retained
    /// admission/lineage owner. Caller cancellation cannot detach the owner's
    /// physical SQL, event or later result-writer obligations.
    pub async fn execute(
        self: Arc<Self>,
        prepared: persistence::Prepared,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Report, Error> {
        tokio::spawn(async move {
            let completed=self.store.clone().execute(prepared,owner.clone()).await.map_err(Error::Sql)?;
            if completed.changed(){
                let (tx,rx)=oneshot::channel();
                let receipt=Arc::new(Receipt{_owner:owner.clone(),done:Some(tx)});
                let observed=self.events.send_retained("updates",json!({"type":"update","data":{"success":true,"type":"recording_updated","recording_id":completed.recording()}}),receipt.clone()).await;
                drop(receipt);
                rx.await.map_err(|_|Error::Event)?;
                // No broad Source caught-event-error promotion from a positive
                // corpus. Unknown/unavailable errors remain operational pending
                // a separate actual failure authority, despite settled receipt.
                observed.map_err(|_|Error::Event)?;
            }
            drop(owner);
            Ok(Report{recording:completed.recording(),changed:completed.changed()})
        }).await.map_err(|_|Error::Supervisor)?
    }
}
