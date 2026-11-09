//! Shared global basic statistics from the original metadata scan contract.
//! Original Dispatcharr authors and contributors retain full credit.
use crate::{project_status_at, StreamEngine};
use dispatcharr_contracts::{LiveStatsSnapshotReader, PlaybackError};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Weak},
    time::{SystemTime, UNIX_EPOCH},
};

impl StreamEngine {
    /// Keep only a weak reader: the application owns its lifecycle, and its
    /// snapshot provider may itself refer to this engine.
    pub fn set_live_statistics(&self, reader: Weak<dyn LiveStatsSnapshotReader>) {
        *self.inner.statistics_reader.lock().unwrap() = Some(reader);
    }
    pub(crate) fn live_statistics_reader(&self) -> Option<Arc<dyn LiveStatsSnapshotReader>> {
        self.inner
            .statistics_reader
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)
    }
    pub async fn global_live_statistics(&self) -> Result<Value, PlaybackError> {
        let Some(store) = &self.inner.clients else {
            return Ok(json!({"channels":[],"count":0}));
        };
        let mut channels = Vec::new();
        let mut cursor = 0;
        let mut pages = 0usize;
        let mut examined = 0usize;
        let mut bytes = 0usize;
        loop {
            let page = store
                .channel_statistics_page(cursor)
                .await
                .map_err(|_| PlaybackError::new(503, "Live metadata unavailable"))?;
            pages += 1;
            examined = examined.saturating_add(page.examined);
            bytes = bytes.saturating_add(page.payload_bytes.saturating_sub(1024));
            if pages > 20000 || examined > 50000 || bytes > 8 * 1024 * 1024 {
                return Err(PlaybackError::new(503, "Live metadata limits exceeded"));
            }
            cursor = page.cursor;
            for row in page.records {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                let mut status = project_status_at(
                    &row.channel_id,
                    &row.metadata,
                    row.index,
                    row.clients,
                    false,
                    now,
                );
                status["client_count"] = json!(row.client_count);
                self.apply_local_manager_status(&row.channel_id, false, now, &mut status);
                channels.push(status);
            }
            if cursor == 0 {
                break;
            }
        }
        Ok(json!({"count":channels.len(),"channels":channels}))
    }
}
