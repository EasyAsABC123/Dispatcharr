//! Real Source Channels group_send through the EXISTING shared UpdateEventSink.
//! Original Dispatcharr/Channels authors retain full algorithm credit.
use crate::StoreError;
use dispatcharr_contracts::UpdateEventSink;
use serde_json::Value;
use std::sync::Arc;
#[derive(Clone)]
pub struct PythonEventPort {
    sink: Arc<dyn UpdateEventSink>,
}
impl PythonEventPort {
    pub fn new(sink: Arc<dyn UpdateEventSink>) -> Self {
        Self { sink }
    }
    pub async fn group_send(
        &self,
        group: String,
        message: Value,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Value, StoreError> {
        // This is the actual publisher used by normal Main WebSockets. Its
        // transport carries owner through real Redis membership/queue writes,
        // late physical replies and shutdown; no extra event client exists.
        self.sink
            .send_retained(&group, message, owner)
            .await
            .map_err(|_| StoreError::Unsupported("Lazy Python Channels publication unavailable"))?;
        Ok(Value::Null)
    }
}
