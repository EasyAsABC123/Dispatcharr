//! Existing bounded Celery identity, evaluated on the actual publishing thread.
use crate::{AuthService, StoreError};
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;
#[derive(Clone)]
pub struct ProducerIdentitySource {
    application: Arc<Semaphore>,
}
pub struct ProducerIdentity {
    pub reply_to: Uuid,
    pub origin: String,
}
impl AuthService {
    pub fn producer_identity_source(&self) -> ProducerIdentitySource {
        ProducerIdentitySource {
            application: self.user_agent_workers.clone(),
        }
    }
}
impl ProducerIdentitySource {
    /// Identity for a producer process that does not run HTTP authentication.
    pub fn for_producer_process() -> Self {
        Self {
            application: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn current(&self) -> Result<ProducerIdentity, StoreError> {
        let actual = crate::celery_metadata::identity(Arc::as_ptr(&self.application) as usize)?;
        Ok(ProducerIdentity {
            reply_to: Uuid::parse_str(&actual.reply_to)
                .map_err(|_| StoreError::Unsupported("Producer identity"))?,
            origin: actual.origin,
        })
    }
}

impl ProducerIdentitySource {
    /// Actual Celery direct task arguments are tuples, including singleton
    /// string filenames. Reuse the existing bounded Source saferepr policy.
    pub fn task_representations(
        &self,
        args: &serde_json::Value,
        kwargs: &serde_json::Value,
    ) -> Result<(String, String), StoreError> {
        Ok((
            crate::celery_metadata::arguments(args)?,
            crate::celery_metadata::keywords(kwargs)?,
        ))
    }
    /// Beat ModelEntry.args is a JSON list. Celery replaces an empty list with
    /// its empty tuple before rendering; nonempty lists retain list repr.
    pub fn periodic_representations(
        &self,
        args: &serde_json::Value,
        kwargs: &serde_json::Value,
    ) -> Result<(String, String), StoreError> {
        Self::periodic_value_representations(args, kwargs)
    }

    /// Pure Beat argument formatting for settings-only callers.
    pub fn periodic_value_representations(
        args: &serde_json::Value,
        kwargs: &serde_json::Value,
    ) -> Result<(String, String), StoreError> {
        Ok((
            crate::celery_metadata::periodic_arguments(args)?,
            crate::celery_metadata::keywords(kwargs)?,
        ))
    }
}
