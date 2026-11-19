//! Direct create-view Groups composition from Original Dispatcharr. No task root
//! is synthesized for this synchronous stage; dependency authors retain credit.
use crate::{AuthService, StoreError};
use dispatcharr_contracts::BoxFuture;
use std::sync::Arc;
pub type Owner = Arc<dyn Send + Sync>;
pub trait InlineGroups: Send + Sync {
    fn admit(&self, auth: &AuthService) -> Result<Box<dyn AdmittedInlineGroups>, StoreError>;
}

/// Owns a worker slot before create persists the account. Dropping an unused
/// admission returns the slot when validation or the save fails.
pub trait AdmittedInlineGroups: Send {
    fn run(
        self: Box<Self>,
        auth: AuthService,
        account: i64,
        enable_vod: bool,
        owner: Owner,
    ) -> BoxFuture<'static, Result<(), StoreError>>;
}
impl AuthService {
    pub fn bind_inline_xc_groups(&self, service: Arc<dyn InlineGroups>) -> Result<(), StoreError> {
        self.inline_xc_groups
            .set(Arc::downgrade(&service))
            .map_err(|_| StoreError::Unsupported("Inline XC Groups service already bound"))
    }
    pub(crate) fn inline_xc_groups_service(&self) -> Result<Arc<dyn InlineGroups>, StoreError> {
        self.inline_xc_groups
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or(StoreError::Unsupported(
                "Inline XC Groups service unavailable",
            ))
    }
}
