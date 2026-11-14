//! Inert proposed completed-Refresh attachment, no Main/poller activation.
//! Source authority: actual parented Refresh->Series linkage, including repeat.
use crate::{
    configured_recording_worker::{Assembly, Family, Worker},
    recording_artwork::RefreshSeriesChild,
    refresh_protocol, series_protocol,
};
#[derive(Debug)]
pub enum Error {
    Capacity,
    UnsupportedLineage,
    Worker(crate::configured_recording_worker::Error),
}
#[derive(Clone, Copy, Debug)]
pub enum Producer {
    ModelOrCapturedRepeat,
    CapturedApiFalse,
}
pub fn prepare(
    producer: Producer,
    assembly: Assembly,
    refresh_frame: &[u8],
    completed_backend: &[u8],
    actual_series_head: &[u8],
) -> Result<Worker, Error> {
    if refresh_frame.len() > 65536
        || actual_series_head.len() > 65536
        || completed_backend.len() > crate::decode::MAX_RESULT_BYTES
    {
        return Err(Error::Capacity);
    }
    if !matches!(assembly.family, Family::Series) {
        return Err(Error::UnsupportedLineage);
    }
    let parent = refresh_protocol::parse(refresh_frame).map_err(|_| Error::UnsupportedLineage)?;
    let child =
        series_protocol::parse(actual_series_head).map_err(|_| Error::UnsupportedLineage)?;
    let capability = match producer {
        Producer::ModelOrCapturedRepeat => {
            RefreshSeriesChild::from_completed_refresh(&parent, completed_backend, &child)
        }
        Producer::CapturedApiFalse => {
            RefreshSeriesChild::from_completed_refresh_api(&parent, completed_backend, &child)
        }
    }
    .ok_or(Error::UnsupportedLineage)?;
    // The existing worker independently checks actual raw UUID spellings,
    // family/domain settings, current FIFO bytes and exact CAS before ACK.
    // This wrapper performs no Redis or SQL work and starts no intake.
    Worker::new_refresh_series_child(assembly, capability).map_err(Error::Worker)
}
