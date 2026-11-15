//! Dedicated opaque per-channel wrapper, not a new generic Jobs authority.
//! Registered as a CHILD of transport so only this implementation can access
//! its private inner transports. No public extraction/clone-as-Jobs method.
use super::*;
use crate::tvg_programme_protocol::{self, Envelope};
#[derive(Clone)]
pub struct TvgJobs {
    jobs: Jobs,
    normal: Arc<std::sync::atomic::AtomicBool>,
}
pub struct Candidate {
    pub(crate) message: Envelope,
    parent: Option<Uuid>,
    raw: Vec<u8>,
    origin: Arc<Inner>,
}
pub struct Claimed {
    message: Envelope,
    parent: Option<Uuid>,
    origin: Arc<Inner>,
}
#[derive(Clone, Copy, Debug)]
pub enum Normal {
    Null,
    MissingEpg,
    Busy,
}
impl TvgJobs {
    pub async fn connect(broker: &str, result: &str, config: Config) -> Result<Self, Error> {
        // Private reuse of the existing bounded connection/lease machinery;
        // wrapper methods expose ONLY this family. No Authority enum widening.
        Ok(Self {
            jobs: Jobs::connect_series(broker, result, config).await?,
            normal: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
    pub(crate) fn enable_normal_intake(&self) {
        self.normal
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub async fn peek(&self, owner: Arc<dyn Send + Sync>) -> Result<Option<Candidate>, Error> {
        self.peek_selected(None, owner).await
    }
    pub(crate) async fn peek_selected(
        &self,
        expected: Option<&[u8]>,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Option<Candidate>, Error> {
        let lease = self.jobs.lease(owner)?;
        let (reply, _held) = self
            .jobs
            .inner
            .broker
            .command_classified::<(i64, Vec<u8>), _>(PEEK_CREATE, &["celery".into()], &[], lease)
            .await
            .map_err(map)?;
        if let Some(expected) = expected {
            if expected.is_empty() || reply.0 != 1 || reply.1 != expected {
                return Err(Error::ClaimRace);
            }
        }
        match reply.0 {
            0 => Ok(None),
            1 => {
                let (message, parent) = if self.normal.load(std::sync::atomic::Ordering::Acquire) {
                    tvg_programme_protocol::parse_normal(&reply.1)
                } else {
                    tvg_programme_protocol::parse_initial(&reply.1).map(|message| (message, None))
                }
                .map_err(|_| Error::UnsupportedTask)?;
                Ok(Some(Candidate {
                    message,
                    parent,
                    raw: reply.1,
                    origin: self.jobs.inner.clone(),
                }))
            }
            _ => Err(Error::Corrupt),
        }
    }
    pub async fn claim(
        &self,
        candidate: Candidate,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<Claimed, Error> {
        if !Arc::ptr_eq(&candidate.origin, &self.jobs.inner) {
            return Err(Error::UnsupportedTask);
        }
        let lease = self.jobs.lease(owner)?;
        match self
            .jobs
            .inner
            .broker
            .command_classified::<i64, _>(
                CLAIM_CREATE,
                &["celery".into()],
                &[candidate.raw.as_slice()],
                lease,
            )
            .await
        {
            Ok((1, _)) => Ok(Claimed {
                message: candidate.message,
                parent: candidate.parent,
                origin: candidate.origin,
            }),
            Ok((0, _)) => Err(Error::ClaimRace),
            Ok(_) => Err(Error::Corrupt),
            Err(ClassifiedError::Backend) => Err(Error::AmbiguousClaim(candidate.message.task_id)),
            Err(error) => Err(map(error)),
        }
    }
    pub async fn write_normal(
        &self,
        claimed: Claimed,
        value: Normal,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !Arc::ptr_eq(&claimed.origin, &self.jobs.inner) {
            return Err(Error::UnsupportedTask);
        }
        let result = match value {
            Normal::Null => serde_json::Value::Null,
            Normal::MissingEpg => serde_json::json!("EPG not found"),
            Normal::Busy => serde_json::json!("Task already running"),
        };
        self.jobs
            .write_result(
                claimed.message.task_id,
                result,
                claimed.parent,
                Vec::new(),
                owner,
            )
            .await
    }
    pub(crate) async fn write_insert_failure(
        &self,
        claimed: Claimed,
        failure: crate::tvg_programme_terminal::InsertFailure,
        owner: Arc<dyn Send + Sync>,
    ) -> Result<bool, Error> {
        if !Arc::ptr_eq(&claimed.origin, &self.jobs.inner) {
            return Err(Error::UnsupportedTask);
        }
        let date = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.6f+00:00")
            .to_string();
        let mut bytes = failure
            .metadata(claimed.message.task_id, &date)
            .map_err(|_| Error::Protocol)?;
        if let Some(parent) = claimed.parent {
            let mut value: serde_json::Value =
                serde_json::from_slice(&bytes).map_err(|_| Error::Protocol)?;
            value["parent_id"] = serde_json::json!(parent);
            bytes = serde_json::to_vec(&value).map_err(|_| Error::Protocol)?;
            if bytes.len() > 262144 {
                return Err(Error::UnsupportedTask);
            }
        }
        self.jobs
            .write_metadata(claimed.message.task_id, bytes, owner)
            .await
    }
    pub async fn shutdown(&self, timeout: Duration) -> Result<(), Error> {
        self.jobs.shutdown(timeout).await
    }
}
