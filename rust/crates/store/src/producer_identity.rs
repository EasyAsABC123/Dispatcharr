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
    pub fn current(&self) -> Result<ProducerIdentity, StoreError> {
        let actual = crate::celery_metadata::identity(Arc::as_ptr(&self.application) as usize)?;
        Ok(ProducerIdentity {
            reply_to: Uuid::parse_str(&actual.reply_to)
                .map_err(|_| StoreError::Unsupported("Producer identity"))?,
            origin: actual.origin,
        })
    }
}
