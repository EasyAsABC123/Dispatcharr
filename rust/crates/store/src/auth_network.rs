//! Shared grouped network policy for native authentication and permissions.
//! Original application credit remains with Dispatcharr contributors.
//! Actual cache-versus-SQL contrasts are source-gated separately.
use crate::{AuthService, StoreError, User};
use std::net::IpAddr;
impl AuthService {
    pub async fn source_network_allowed(
        &self,
        ip: IpAddr,
        key: &str,
        user: Option<&User>,
    ) -> Result<bool, StoreError> {
        let Some(cache) = self.notification_cache.as_ref() else {
            return self.store.network_allowed(ip, key, user).await;
        };
        let policy = cache
            .settings_group("network_access", || {
                self.store.source_group_value("network_access")
            })
            .await?;
        if !crate::xc_auth::system_allowed(&policy, key, ip)? {
            return Ok(false);
        }
        if let Some(user) = user {
            crate::xc_auth::user_allowed(user, key, ip)
        } else {
            Ok(true)
        }
    }
}
