//! Actual normal root-artwork producer/consumer operations over Main's existing
//! local-artwork Jobs. Original authors retain all credit. No extra pool.
use super::*;
use crate::recording_artwork_root::{self, Message};
#[derive(Clone)]
pub struct ArtworkRootPublisher {
    jobs: Jobs,
}
impl ArtworkRootPublisher {
    pub(crate) fn new(jobs: Jobs) -> Result<Self, Error> {
        if jobs.inner.authority != Authority::LocalArtwork {
            return Err(Error::Configuration);
        }
        Ok(Self { jobs })
    }
    /// The same already-admitted save signal waits for the bounded queue to
    /// drain. Only Capacity is safe to retry: real negative PUBLISH reply means
    /// no LPUSH, and a local operation-lease refusal sends no command. All wire
    /// ambiguity is returned unchanged, never replayed. The caller/supervisor
    /// retains this future's SQL session and owner across the bounded sleep.
    pub(crate) async fn publish_when_ready(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
        producer_stopped: &AtomicBool,
    ) -> Result<Published, Error> {
        loop {
            if producer_stopped.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            match self.publish(raw, owner.clone()).await {
                Err(Error::Capacity) => tokio::time::sleep(Duration::from_millis(100)).await,
                outcome => return outcome,
            }
        }
    }
    pub(crate) async fn publish(
        &self,
        raw: &[u8],
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Published, Error> {
        let message = recording_artwork_root::parse_signal(raw).ok_or(Error::Protocol)?;
        let maximum = self.jobs.inner.config.max_queued_jobs.to_string();
        // Fixed vocabulary and typed identity only; no frame, Redis key or error
        // text. Same existing opt-in and global record bound as DVR diagnostics.
        let diagnostic = (std::env::var("RUST_AUTO_STAGE_DIAGNOSTICS").as_deref() == Ok("1"))
            .then_some(
                dispatcharr_output_store::transport::CommandDiagnostic::ArtworkPublication {
                    task: message.task_id.as_u128(),
                },
            );
        match self
            .jobs
            .inner
            .broker
            .command_publication_classified_with_diagnostic::<i64, _>(
                PUBLISH,
                &["celery".into(), "_kombu.binding.celery".into()],
                &[raw, maximum.as_bytes()],
                self.jobs.lease(owner)?,
                diagnostic,
            )
            .await
        {
            Ok((n, _)) if n > 0 => Ok(Published {
                task_id: message.task_id,
                queue_length: n as u64,
            }),
            Ok((-2, _)) => Err(Error::Capacity),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousPublication(message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
}
pub(crate) struct RootArtworkCandidate {
    pub message: Message,
    raw: Vec<u8>,
    origin: Arc<Inner>,
}
impl Jobs {
    pub(crate) async fn peek_root_artwork(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<RootArtworkCandidate>, Error> {
        if self.inner.authority != Authority::LocalArtwork {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _physical) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        if let Some(expected) = expected {
            if expected.is_empty() || reply.0 != 1 || reply.1 != expected {
                return Err(Error::ClaimRace);
            }
        }
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(RootArtworkCandidate {
                message: recording_artwork_root::parse_signal(&reply.1)
                    .ok_or(Error::UnsupportedTask)?,
                raw: reply.1,
                origin: self.inner.clone(),
            })),
            _ => Err(Error::Corrupt),
        }
    }
    pub(crate) async fn claim_root_artwork(
        &self,
        candidate: RootArtworkCandidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<(), Error> {
        if self.inner.authority != Authority::LocalArtwork
            || !Arc::ptr_eq(&self.inner, &candidate.origin)
        {
            return Err(Error::UnsupportedTask);
        }
        match self
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[&candidate.raw],
                self.lease(owner)?,
            )
            .await
        {
            Ok((1, _)) => Ok(()),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub(crate) async fn write_root_artwork(
        &self,
        message: &Message,
        completed: &dispatcharr_store::recording_existing_artwork::Completed,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if self.inner.authority != Authority::LocalArtwork
            || completed.recording != message.recording
        {
            return Err(Error::UnsupportedTask);
        }
        self.write_result(
            message.task_id,
            serde_json::json!(completed.value),
            message.parent,
            vec![],
            owner,
        )
        .await
    }
}
impl Jobs {
    pub(crate) async fn peek_artwork_frame(
        &self,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Vec<u8>>, Error> {
        if self.inner.authority != Authority::LocalArtwork {
            return Err(Error::UnsupportedTask);
        }
        let (reply, _physical) = self
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(
                PEEK_CREATE,
                &["celery".into()],
                &[],
                self.lease(owner)?,
            )
            .await
            .map_err(map)?;
        match reply.0 {
            0 => Ok(None),
            1 => Ok(Some(reply.1)),
            _ => Err(Error::Corrupt),
        }
    }
}

#[cfg(test)]
#[path = "recording_artwork_backpressure_tests.rs"]
mod backpressure_tests;
