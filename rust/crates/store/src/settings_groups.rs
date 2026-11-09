//! Original CoreSettings grouped-value defaults shared with Django cache readers.
//! Full application credit remains with original Dispatcharr contributors.
use crate::{AuthService, Store, StoreError};
use serde_json::{json, Value};
fn default_system() -> Value {
    json!({"time_zone":"UTC","max_system_events":100,"preferred_region":null,"auto_import_mapped_files":true,"enable_ip_lookup":true,"catchup_enabled":true,"log_max_mb":5,"log_keep":5,"log_persist":true})
}
impl Store {
    pub(crate) async fn source_group_value(&self, key: &str) -> Result<Value, StoreError> {
        let value = self.setting(key).await?;
        Ok(
            if value.as_object().is_some_and(|value| !value.is_empty()) {
                value
            } else if key == "system_settings" {
                default_system()
            } else {
                json!({})
            },
        )
    }
}
impl AuthService {
    /// Source CoreSettings.get_system_settings(): cached maps are returned as
    /// stored; an empty/nonobject/missing DB value uses the complete defaults.
    pub async fn system_settings(&self) -> Result<Value, StoreError> {
        if let Some(cache) = self.notification_cache.as_ref() {
            cache
                .settings_group("system_settings", || {
                    self.store.source_group_value("system_settings")
                })
                .await
        } else {
            self.store.source_group_value("system_settings").await
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn system_defaults_match_original_fixed_django_timezone_and_logging() {
        let defaults = default_system();
        assert_eq!(defaults.as_object().unwrap().len(), 9);
        assert_eq!(defaults["time_zone"], "UTC");
        assert_eq!(defaults["log_max_mb"], 5);
        assert_eq!(defaults["enable_ip_lookup"], true);
    }
}
