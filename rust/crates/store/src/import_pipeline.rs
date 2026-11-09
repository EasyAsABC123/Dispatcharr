//! Bounded standard-M3U cache/batch bridge. Public refresh jobs remain gated.
//! Application and algorithm credit belongs to the original Dispatcharr authors.
use crate::{ImportCounts, ImportedStream, Store, StoreError, MAX_IMPORT_BATCH_BYTES};
use dispatcharr_ingest_core::{project_entry, CacheReadLimits, ProjectionContext, ValidatedCache};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{Arc, OnceLock},
};
use tokio::sync::{mpsc, Semaphore};

/// Captured refresh inputs, supplied under the caller's account task lock.
/// Selection of effective hash keys and enabled groups belongs to the job.
pub struct CacheBatchRequest {
    pub account_id: i64,
    pub path: PathBuf,
    pub groups: BTreeMap<String, i64>,
    pub hash_keys: Vec<String>,
    pub max_name_characters: usize,
    pub limits: CacheReadLimits,
}
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CacheBatchCounts {
    pub streams: ImportCounts,
    pub batches: usize,
}
struct Bytes(usize);
impl io::Write for Bytes {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(data.len())
            .ok_or_else(|| io::Error::other("M3U batch byte capacity"))?;
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn capacity() -> StoreError {
    StoreError::InvalidInput(json!({"detail":"Parsed M3U cache or batch capacity exceeded."}))
}
impl Store {
    /// Bridge only: no stale marking/deletion, notifications, auto-sync or job
    /// success status. An error can follow committed batches; callers must skip
    /// destructive completion and apply the source's retry/failure policy.
    /// Account filters are explicitly rejected until their engine is accepted.
    pub async fn import_standard_cache_batches(
        &self,
        request: CacheBatchRequest,
    ) -> Result<CacheBatchCounts, StoreError> {
        if request.groups.len() > 32768
            || request.hash_keys.len() > 6
            || request.max_name_characters > 512
        {
            return Err(capacity());
        }
        let account_type: Option<String> =
            sqlx::query_scalar("SELECT account_type FROM m3u_m3uaccount WHERE id=$1")
                .bind(request.account_id)
                .fetch_optional(self.pool())
                .await?;
        if account_type.as_deref() != Some("STD") {
            return Err(StoreError::Unsupported("Standard M3U cache bridge only"));
        }
        let filtered: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM m3u_m3ufilter WHERE m3u_account_id=$1)",
        )
        .bind(request.account_id)
        .fetch_one(self.pool())
        .await?;
        if filtered {
            return Err(StoreError::Unsupported(
                "M3U filter compatibility gate is incomplete",
            ));
        }
        static JOBS: OnceLock<Arc<Semaphore>> = OnceLock::new();
        let admission = Arc::new(
            JOBS.get_or_init(|| Arc::new(Semaphore::new(4)))
                .clone()
                .try_acquire_owned()
                .map_err(|_| capacity())?,
        );
        let _admission = admission.clone();
        let account_id = request.account_id;
        let (sender, mut receiver) = mpsc::channel::<Vec<ImportedStream>>(1);
        let producer = tokio::task::spawn_blocking(move || -> Result<(), String> {
            let _admission = admission;
            // Complete validation precedes the first batch delivery.
            let mut cache =
                ValidatedCache::open(&request.path, request.limits).map_err(|e| e.to_string())?;
            let keys: Vec<&str> = request.hash_keys.iter().map(String::as_str).collect();
            let context = ProjectionContext {
                account_id,
                account_type: "STD",
                groups: &request.groups,
                hash_keys: &keys,
                max_name_characters: request.max_name_characters,
            };
            let mut rows = Vec::new();
            let mut count = 0;
            let mut bytes = Bytes(MAX_IMPORT_BATCH_BYTES);
            cache
                .for_each(|entry| {
                    // Source slices raw entries before disabled-group selection,
                    // per-row failures and first-hash deduplication in the sink.
                    count += 1;
                    if let Ok(Some(row)) = project_entry(&entry, &context) {
                        let row = ImportedStream::from(row);
                        serde_json::to_writer(&mut bytes, &row).map_err(|e| e.to_string())?;
                        rows.push(row);
                    }
                    if count == 1500 {
                        sender
                            .blocking_send(std::mem::take(&mut rows))
                            .map_err(|_| "M3U batch consumer stopped".to_owned())?;
                        count = 0;
                        bytes = Bytes(MAX_IMPORT_BATCH_BYTES);
                    }
                    Ok(())
                })
                .map_err(|e| e.to_string())?;
            if count != 0 {
                sender
                    .blocking_send(rows)
                    .map_err(|_| "M3U batch consumer stopped".to_owned())?;
            }
            Ok(())
        });
        let mut counts = CacheBatchCounts::default();
        let mut failure = None;
        while let Some(rows) = receiver.recv().await {
            match self.import_m3u_batch(account_id, rows).await {
                Ok(batch) => {
                    counts.batches += 1;
                    counts.streams.created += batch.created;
                    counts.streams.updated += batch.updated;
                    counts.streams.unchanged += batch.unchanged;
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        // Dropping the receiver unblocks a producer waiting on bounded output.
        // Cancellation also drops it, while the worker retains admission until
        // the actual blocking file work and cleanup have ended.
        drop(receiver);
        let produced = producer.await;
        if let Some(error) = failure {
            return Err(error);
        }
        match produced {
            Ok(Ok(())) => Ok(counts),
            _ => Err(capacity()),
        }
    }
}
