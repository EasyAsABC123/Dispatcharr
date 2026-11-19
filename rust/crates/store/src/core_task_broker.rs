//! Sealed Core publications to Main's real shared broker, separate from cache.
//! Original Dispatcharr/Celery authors retain all protocol/algorithm credit.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::BoxFuture;
use std::sync::Arc;
use uuid::Uuid;
/// Only the existing Store settings producer can construct a publication.
/// Neither callers nor a broker adapter can mint arbitrary envelopes here.
pub struct CoreTaskPublication {
    raw: Vec<u8>,
    id: Uuid,
}
impl CoreTaskPublication {
    pub(crate) fn new(raw: Vec<u8>, id: &str) -> Result<Self, StoreError> {
        if raw.is_empty() || raw.len() > 65536 {
            return Err(StoreError::Unsupported("Core publication capacity"));
        }
        Ok(Self {
            raw,
            id: Uuid::parse_str(id)
                .map_err(|_| StoreError::Unsupported("Core publication identity"))?,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.raw
    }
    pub fn task_id(&self) -> Uuid {
        self.id
    }
}
/// Only an acknowledged root or a proven refusal may let Inline Groups proceed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreProfilePublication {
    Acknowledged,
    CapacityRefused,
}
pub trait CoreTaskBroker: Send + Sync {
    /// Acknowledgement must follow the actual existing broker command reply.
    /// No retry/replay or cache DB fallback is authorized by this port.
    fn publish(
        &self,
        publication: CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), StoreError>>;
    /// Inline XC may continue after a proven capacity refusal. The default
    /// cannot prove that distinction and forwards every error unchanged.
    fn publish_profiles_root(
        &self,
        publication: CoreTaskPublication,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<CoreProfilePublication, StoreError>> {
        Box::pin(async move {
            self.publish(publication, owner).await?;
            Ok(CoreProfilePublication::Acknowledged)
        })
    }
}
impl AuthService {
    /// Early Core and later engine/selected Auth clones share THIS same cell.
    /// Bind once before HTTP serving; it owns no new SQL/Redis client or pool.
    pub fn bind_core_task_broker(&self, broker: Arc<dyn CoreTaskBroker>) -> Result<(), StoreError> {
        self.core_task_broker
            .set(broker)
            .map_err(|_| StoreError::Unsupported("Core broker already composed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::sync::Mutex;
    struct DeferredBroker {
        frames: Mutex<Vec<Vec<u8>>>,
        owners: Mutex<Vec<Arc<dyn Send + Sync>>>,
    }
    impl CoreTaskBroker for DeferredBroker {
        fn publish(
            &self,
            p: CoreTaskPublication,
            owner: Arc<dyn Send + Sync>,
        ) -> BoxFuture<'_, Result<(), StoreError>> {
            self.frames.lock().unwrap().push(p.bytes().to_vec());
            self.owners.lock().unwrap().push(owner);
            // A probe, not a fabricated successful Redis acknowledgement.
            Box::pin(async {
                Err(StoreError::Unsupported(
                    "test deferred physical broker reply",
                ))
            })
        }
    }
    fn auth() -> AuthService {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        AuthService::new(
            crate::Store::from_pool(pool),
            crate::AuthConfig::new("fixture-only-signing".into()).unwrap(),
        )
    }
    #[tokio::test]
    async fn early_core_clone_observes_late_main_broker_and_retains_original_writer() {
        let auth = auth();
        let mut early = auth.clone();
        early.normal_core_task_broker = true;
        let broker = Arc::new(DeferredBroker {
            frames: Mutex::new(Vec::new()),
            owners: Mutex::new(Vec::new()),
        });
        auth.bind_core_task_broker(broker.clone()).unwrap();
        let permit = Arc::new(
            early
                .user_agent_workers
                .clone()
                .try_acquire_owned()
                .unwrap(),
        );
        let caller = Arc::new(());
        let weak = Arc::downgrade(&caller);
        let scoped = early
            .with_request_writer_permit(permit)
            .unwrap()
            .with_retained_scope(caller.clone());
        drop(caller);
        let outcome = crate::core_settings_special_broker::publish(
            &scoped,
            &crate::core_settings_special_budget::Scratch::new(),
            "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change",
            json!([]),
            json!({}),
        )
        .await;
        assert!(outcome.is_err());
        drop(scoped);
        assert_eq!(broker.frames.lock().unwrap().len(), 1);
        assert!(weak.upgrade().is_some());
        assert_eq!(auth.user_agent_workers.available_permits(), 3);
        let value: Value = serde_json::from_slice(&broker.frames.lock().unwrap()[0]).unwrap();
        assert_eq!(
            value["headers"]["task"],
            "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change"
        );
        broker.owners.lock().unwrap().clear();
        assert!(weak.upgrade().is_none());
        assert_eq!(auth.user_agent_workers.available_permits(), 4);
    }
    #[tokio::test]
    async fn normal_core_without_main_broker_never_falls_back_to_cache() {
        let mut auth = auth();
        auth.normal_core_task_broker = true;
        let result = crate::core_settings_special_broker::publish(
            &auth,
            &crate::core_settings_special_budget::Scratch::new(),
            "apps.channels.tasks.reschedule_upcoming_recordings_for_offset_change",
            json!([]),
            json!({}),
        )
        .await;
        assert!(matches!(
            result,
            Err(StoreError::Unsupported(
                "Normal Core task broker has not been composed"
            ))
        ));
    }
}
