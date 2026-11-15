//! Source send_epg_update on the actual shared sink; no synthetic event proof.
//! Original Dispatcharr/Channels authors retain message/algorithm credit.
use dispatcharr_contracts::UpdateEventSink;
use serde_json::{json, Value};
use std::sync::Arc;
#[derive(Clone)]
pub(super) struct Progress {
    source: i64,
    sink: Arc<dyn UpdateEventSink>,
    owner: Arc<dyn Send + Sync>,
}
impl Progress {
    pub fn new(source: i64, sink: Arc<dyn UpdateEventSink>, owner: Arc<dyn Send + Sync>) -> Self {
        Self {
            source,
            sink,
            owner,
        }
    }
    pub async fn parsing(&self, progress: i64, message: Option<String>) {
        let mut data = json!({"progress":progress,"type":"epg_refresh","source":self.source,"action":"parsing_programs"});
        if let Some(message) = message {
            data["message"] = Value::String(message)
        }
        // Source's plain Celery send_websocket_update catches channel send failures.
        // The real sink retains SAME caller through physical settlement; qualification
        // still requires actual received frames, never this attempted send.
        let _ = self
            .sink
            .send_retained(
                "updates",
                json!({"type":"update","data":data}),
                self.owner.clone(),
            )
            .await;
    }
}
