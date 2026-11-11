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

impl AuthService {
    /// REST/UI permission context. XC calls retain their own JSON error DTOs.
    pub(crate) async fn source_ui_network_allowed(
        &self, ip: IpAddr, user: Option<&User>,
    ) -> Result<bool, StoreError> {
        let policy = match self.notification_cache.as_ref() {
            Some(cache) => cache.settings_group("network_access", || self.store.source_group_value("network_access")).await?,
            None => self.store.source_group_value("network_access").await?,
        };
        let allowed = crate::xc_auth::system_allowed(&policy, "UI", ip)
            .map_err(ui_permission_error)?;
        if !allowed { return Ok(false); }
        user.map_or(Ok(true), |user| crate::xc_auth::user_allowed(user, "UI", ip).map_err(ui_permission_error))
    }
}
fn ui_permission_error(error: StoreError) -> StoreError {
    match error {
        StoreError::StatusBody(500, _) => StoreError::UiPermissionFault,
        other => other,
    }
}
