//! Original CoreSettings grouped-value defaults shared with Django cache readers.
//! Full application credit remains with original Dispatcharr contributors.
use crate::{AuthService, Store, StoreError};
use serde_json::{json, Value};
fn default_system() -> Value {
    json!({"time_zone":"UTC","max_system_events":100,"preferred_region":null,"auto_import_mapped_files":true,"enable_ip_lookup":true,"catchup_enabled":true,"log_max_mb":5,"log_keep":5,"log_persist":true})
}
fn default_stream() -> Value {
    json!({"default_user_agent":null,"default_stream_profile":null,"m3u_hash_key":"","default_output_format":"mpegts","hdhr_output_profile_id":null})
}
fn default_epg() -> Value {
    json!({"epg_match_mode":"default","epg_match_ignore_prefixes":[],"epg_match_ignore_suffixes":[],"epg_match_ignore_custom":[],"epg_history_min_days":1,"epg_history_max_days":30,"epg_default_prev_days":0,"epg_default_days":0})
}
impl Store {
    pub(crate) async fn source_group_value(&self, key: &str) -> Result<Value, StoreError> {
        let value = self.setting(key).await?;
        Ok(
            if value.as_object().is_some_and(|value| !value.is_empty()) {
                value
            } else if key == "system_settings" {
                default_system()
            } else if key == "stream_settings" {
                default_stream()
            } else if key == "epg_settings" {
                default_epg()
            } else if key == "reverse_proxy_auth" {
                json!({"enabled":false,"header":""})
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
        self.cached_setting_group("system_settings").await
    }
    /// Source CoreSettings.get_stream_settings(), including shared-cache
    /// version fencing and the original five defaults for an empty DB map.
    pub async fn source_stream_settings(&self) -> Result<Value, StoreError> {
        self.cached_setting_group("stream_settings").await
    }
    /// The original cached group is returned without overlaying a stored
    /// nonempty map; only an empty/missing group uses complete defaults.
    pub async fn source_epg_settings(&self) -> Result<Value, StoreError> {
        self.cached_setting_group("epg_settings").await
    }
    pub(crate) async fn cached_setting_group(&self, key: &str) -> Result<Value, StoreError> {
        if let Some(cache) = self.notification_cache.as_ref() {
            cache
                .settings_group(key, || self.store.source_group_value(key))
                .await
        } else {
            self.store.source_group_value(key).await
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stream_defaults_match_original_get_stream_settings() {
        assert_eq!(
            default_stream(),
            json!({"default_user_agent":null,"default_stream_profile":null,"m3u_hash_key":"","default_output_format":"mpegts","hdhr_output_profile_id":null})
        );
    }
    #[test]
    fn system_defaults_match_original_fixed_django_timezone_and_logging() {
        let defaults = default_system();
        assert_eq!(defaults.as_object().unwrap().len(), 9);
        assert_eq!(defaults["time_zone"], "UTC");
        assert_eq!(defaults["log_max_mb"], 5);
        assert_eq!(defaults["enable_ip_lookup"], true);
    }
}
