//! Private typed refresh preparation. The snapshot contains only owned semantic
//! data, never a PgRow, SQL connection, scoped AuthService or serializer owner.
//! Original Dispatcharr authors retain application and algorithm credit.
use super::{
    vod_api_owned_read::{FrozenSnapshot, ReadError, ReadWork},
    vod_api_provider_refresh_http as http,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[derive(Serialize, Deserialize)]
pub(super) struct SeriesView {
    pub parent: Value,
    pub account: Value,
    pub external_id: String,
}
#[derive(Serialize, Deserialize)]
pub(super) struct Plan {
    pub series: bool,
    pub id: i64,
    pub relation_id: i64,
    pub force: bool,
    pub query: Vec<(String, String)>,
    pub origin: String,
    pub series_view: Option<SeriesView>,
}
impl Plan {
    pub(super) fn freeze(self, work: &ReadWork) -> Result<FrozenSnapshot, ReadError> {
        let mut document = work.snapshot_document();
        serde_json::to_writer(&mut document, &self).map_err(|_| ReadError::Capacity)?;
        // The active Document is consumed before the broker accepts the payload.
        drop(self);
        work.freeze_snapshot(document.finish())
    }
    pub(super) fn consume(work: &ReadWork, snapshot: FrozenSnapshot) -> Result<Self, ReadError> {
        let bytes = work.consume_snapshot(snapshot)?;
        let value =
            http::admitted_value(&bytes, work.work_bytes() / 8).map_err(|error| match error {
                http::Fault::Capacity => ReadError::Capacity,
                _ => ReadError::WorkerFailed,
            })?;
        drop(bytes);
        serde_json::from_value(value).map_err(|_| ReadError::WorkerFailed)
    }
}
