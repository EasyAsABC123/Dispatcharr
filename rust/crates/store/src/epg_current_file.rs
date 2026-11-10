//! Boundary for original Dispatcharr's current-programme file fallback.
//! Original authors retain all application and algorithm credit.
use crate::StoreError;
use bytes::Bytes;
use dispatcharr_contracts::BoxFuture;
use std::{path::PathBuf, sync::Arc};

/// Owned metadata snapshot. SQL connections must be released before lookup.
#[derive(Clone, Debug)]
pub struct CurrentFileRequest {
    pub epg_entry_id: i64,
    pub source_id: i64,
    pub source_type: String,
    pub tvg_id: String,
    pub file_path: Option<PathBuf>,
    pub extracted_file_path: Option<PathBuf>,
    pub cache_file_path: PathBuf,
}

#[derive(Debug)]
pub enum CurrentFileOutcome {
    /// Complete original file DTO; the byte owner retains its allocation lease.
    Found(Bytes),
    Missing,
    /// Permitted only after an acknowledged durable index-task publication.
    Parsing,
}

/// The implementation supervises physical SQL/file/broker work after caller
/// cancellation and retains `caller` until that work actually completes.
/// This interface alone does not enable dispatch or attach an application route.
pub trait CurrentFileLookup: Send + Sync {
    fn lookup(
        &self,
        request: CurrentFileRequest,
        caller: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CurrentFileOutcome, StoreError>>;
}
