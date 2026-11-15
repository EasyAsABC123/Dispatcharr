//! Source compact manual/model hooks; SQL and application effects remain native.
//! Original Dispatcharr authors retain all application/algorithm credit.
use crate::{AuthService, StoreError, TaskSqlSession};
use dispatcharr_contracts::BoxFuture;
use serde_json::{json, Map, Value};
use std::sync::{Arc, Weak};
pub type Owner = Arc<dyn Send + Sync>;
/// A real shared refresh lock, never a caller-supplied token/ID receipt.
pub trait Lease: Send {
    fn release(self: Box<Self>) -> BoxFuture<'static, Result<(), Failure>>;
}
#[derive(Clone, Copy, Debug)]
pub enum Failure {
    Capacity,
    Settled,
    Unsupported,
    Unsettled,
}
impl Failure {
    pub(crate) fn store(self) -> StoreError {
        match self {
            Self::Capacity => StoreError::StatusBody(
                503,
                json!({"detail":"Compact numbering capacity is unavailable."}),
            ),
            Self::Settled => StoreError::UnhandledRestFault,
            Self::Unsupported => {
                StoreError::Unsupported("Compact numbering shared lock is unavailable")
            }
            Self::Unsettled => StoreError::StatusBody(
                503,
                json!({"detail":"Compact numbering operation physically unsettled."}),
            ),
        }
    }
}
pub type LockResult = Result<Option<Box<dyn Lease>>, Failure>;
pub trait Coordinator: Send + Sync {
    /// None is the actual broker's SET NX lock-held result, not capacity.
    fn acquire(&self, account: i64, owner: Owner) -> BoxFuture<'_, LockResult>;
}
impl AuthService {
    /// Late-bind the same Main coordinator to existing Auth/router clones.
    /// Weak storage avoids Auth -> Prepared -> Auth ownership cycles.
    pub fn bind_compact_coordinator(
        &self,
        coordinator: Arc<dyn Coordinator>,
    ) -> Result<(), StoreError> {
        self.compact_coordinator
            .set(Arc::downgrade(&coordinator))
            .map_err(|_| StoreError::Unsupported("Compact coordinator already bound"))
    }
    pub(crate) fn compact_port(&self) -> Option<Arc<dyn Coordinator>> {
        self.compact_coordinator.get().and_then(Weak::upgrade)
    }
    pub async fn manual_compact_group(
        &self,
        session: &TaskSqlSession,
        account: i64,
        group: Option<i64>,
        owner: Owner,
    ) -> Result<Value, StoreError> {
        let port = self
            .compact_port()
            .ok_or_else(|| Failure::Unsupported.store())?;
        let lease = port
            .acquire(account, owner)
            .await
            .map_err(Failure::store)?
            .ok_or_else(|| {
                StoreError::StatusBody(
                    409,
                    json!({"detail":"An M3U refresh is in progress for this account."}),
                )
            })?;
        let work=async {
            let relation=match group {
                Some(group)=>session.run(move|c|Box::pin(crate::compact_unhide::load_relation(c,account,group))).await??,
                None=>None,
            };
            let relation=relation.ok_or_else(||StoreError::StatusBody(404,json!({"detail":"Group is not associated with this account"})))?;
            let result=crate::auto_sync_compact::repack(session,account,&relation).await?;
            Ok::<_,StoreError>(json!({"assigned":result.assigned,"released":result.released,"failed":result.failed}))
        }.await;
        // Source catches a physically known release error. Unknown ownership
        // remains a real failure and is retained by the actual coordinator.
        match lease.release().await {
            Ok(()) | Err(Failure::Settled) => work,
            Err(error) => Err(error.store()),
        }
    }
    pub(crate) async fn compact_saved_on(
        &self,
        c: &mut sqlx::PgConnection,
        id: i64,
        data: &Map<String, Value>,
        owner: Owner,
    ) -> Result<(), StoreError> {
        crate::compact_unhide::saved(self, c, id, data, owner).await
    }
}
