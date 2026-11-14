//! PRIVATE UNCOMPILED retained actual event publication. No blanket unavailable
//! ->normal-terminal policy and no implicit SQL/SystemEvent hook dispatcher.
use dispatcharr_contracts::{UpdateEventError, UpdateEventSink};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::oneshot;
#[derive(Debug)]
pub enum Error {
    Unavailable(UpdateEventError),
    Unsettled,
}
struct Receipt {
    done: Option<oneshot::Sender<()>>,
    _caller: Arc<dyn Send + Sync>,
}
impl Drop for Receipt {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}
pub(crate) async fn send(
    events: Arc<dyn UpdateEventSink>,
    message: Value,
    owner: Arc<dyn Send + Sync>,
) -> Result<(), Error> {
    let (tx, rx) = oneshot::channel();
    let receipt = Arc::new(Receipt {
        done: Some(tx),
        _caller: owner,
    });
    let observed = events
        .send_retained("updates", message, receipt.clone())
        .await;
    drop(receipt);
    // Logical2s backend availability is distinct from actual command settlement.
    rx.await.map_err(|_| Error::Unsettled)?;
    observed.map_err(Error::Unavailable)
}
pub(crate) fn started(channel: &str) -> Value {
    json!({"type":"update","data":{"success":true,"type":"recording_started","channel":channel}})
}
pub(crate) fn updated(recording: i64) -> Value {
    json!({"type":"update","data":{"success":true,"type":"recording_updated","recording_id":recording}})
}
pub(crate) fn ended(channel: &str) -> Value {
    json!({"type":"update","data":{"success":true,"type":"recording_ended","channel":channel}})
}
