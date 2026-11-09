//! Existing Dispatcharr PostgreSQL/authentication compatibility.
//! Original application credit: Dispatcharr authors and contributors; see ATTRIBUTION.md.
mod account_writes;
mod accounts;
mod admin_writes;
mod app_data;
mod auth;
mod auth_events;
mod auth_network;
mod auth_throttle;
mod cache_codec;
mod channel_groups;
mod channel_memberships;
mod channel_profiles;
mod channel_relations;
mod channel_writes;
mod channels;
mod core_reads;
mod core_stream_writes;
mod core_utilities;
mod django_pickle;
mod epg_policy;
mod ip_lookup;
mod notifications;
mod python_action;
mod schema;
mod settings_groups;
mod user_agent_writes;
pub use epg_policy::{source_epg_history_policy, EpgHistoryPolicy, EpgPolicyError};
mod epg_staging;
pub use epg_staging::{GuideMapping, GuideStaging, GuideSwapCounts};
pub use ip_lookup::IpLookupConfig;
pub use schema::{initialize_empty_database, SchemaInitialization};
/// Source-compatible JSON decoder error text for management adapters.
pub fn source_json_error(raw: &[u8], error: serde_json::Error) -> String {
    startup_bootstrap::json_error(raw, error)
}

mod xc_auth;
pub use notifications::NotificationCache;
mod import_batches;
mod import_cleanup;
mod import_groups;
mod import_pipeline;
pub use import_cleanup::{GroupCleanupAccount, GroupCleanupCounts, StreamCleanupCounts};
pub use import_groups::{
    GroupImportCounts, ImportedGroup, MAX_IMPORT_GROUPS, MAX_IMPORT_GROUP_BYTES,
};
pub use import_pipeline::{CacheBatchCounts, CacheBatchRequest};
mod json_compat;
pub mod realtime;
mod startup;
mod startup_bootstrap;
pub use import_batches::{
    ImportCounts, ImportedStream, MAX_IMPORT_BATCH_BYTES, MAX_IMPORT_BATCH_ROWS,
};
mod image_proxy;
pub use image_proxy::SourceImageResponse;
mod html_filename;
mod logo_writes;
mod logos;
mod password_scrypt;
mod passwords;
mod playback;
mod provider_mutations;
mod provider_reads;
mod stream_validation;
mod stream_writes;
mod streams;
mod username;

pub use auth::{AuthConfig, AuthService, Claims, TokenPair};
pub use cache_codec::{decode_source_cache, encode_source_cache};
pub use channels::ChannelIdentity;
pub use passwords::verify_password;
pub use playback::{ProviderStream, ResolvedChannel};

use serde_json::Value;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::{net::IpAddr, time::Duration};

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

#[derive(Debug)]
pub enum StoreError {
    Database(sqlx::Error),
    Unauthorized(&'static str),
    AuthenticationBody(Value),
    Forbidden,
    PermissionDenied,
    NotFound,
    Unsupported(&'static str),
    RateLimited(u64),
    Throttled {
        body: Value,
        retry_after: Option<u64>,
        wait: Option<f64>,
    },
    XcDenied {
        status: u16,
        retry_after: Option<u64>,
    },
    InvalidInput(Value),
    StatusBody(u16, Value),
}

impl From<sqlx::Error> for StoreError {
    fn from(value: sqlx::Error) -> Self {
        Self::Database(value)
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Database(_) => formatter.write_str("Database unavailable"),
            Self::AuthenticationBody(_) => formatter.write_str("Authentication failed"),
            Self::Unauthorized(message) | Self::Unsupported(message) => {
                formatter.write_str(message)
            }
            Self::Forbidden => formatter.write_str("Forbidden"),
            Self::PermissionDenied => formatter.write_str("Permission denied"),
            Self::NotFound => formatter.write_str("Not found"),
            Self::XcDenied { status, .. } => {
                write!(formatter, "XC request denied with status {status}")
            }
            Self::RateLimited(wait) => write!(formatter, "Rate limited for {wait} seconds"),
            Self::Throttled { .. } => formatter.write_str("Authentication request throttled"),
            Self::InvalidInput(_) => formatter.write_str("Invalid input"),
            Self::StatusBody(status, _) => write!(formatter, "Request failed with status {status}"),
        }
    }
}

impl std::error::Error for StoreError {}

#[derive(Clone)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub password: String,
    pub is_active: bool,
    pub user_level: i32,
    pub stream_limit: i32,
    pub custom_properties: Value,
}

const USER_COLUMNS: &str = "id::bigint AS id, username, password, is_active, user_level, stream_limit, COALESCE(custom_properties, '{}'::jsonb) AS custom_properties";

fn user_from_row(row: sqlx::postgres::PgRow) -> Result<User, sqlx::Error> {
    Ok(User {
        id: row.try_get("id")?,
        username: row.try_get("username")?,
        password: row.try_get("password")?,
        is_active: row.try_get("is_active")?,
        user_level: row.try_get("user_level")?,
        stream_limit: row.try_get("stream_limit")?,
        custom_properties: row.try_get("custom_properties")?,
    })
}

impl Store {
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, StoreError> {
        if max_connections == 0 || max_connections > 64 {
            return Err(StoreError::Unsupported(
                "database pool must be 1..64 connections",
            ));
        }
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(60))
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn user_by_username(&self, username: &str) -> Result<Option<User>, StoreError> {
        let query = format!("SELECT {USER_COLUMNS} FROM accounts_user WHERE username=$1");
        sqlx::query(&query)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?
            .map(user_from_row)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn user_by_id(&self, id: i64) -> Result<Option<User>, StoreError> {
        let query = format!("SELECT {USER_COLUMNS} FROM accounts_user WHERE id=$1");
        sqlx::query(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(user_from_row)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn user_by_api_key(&self, key: &str) -> Result<Option<User>, StoreError> {
        // Django get() rejects ambiguous duplicate API keys. Do not choose one.
        let query = format!("SELECT {USER_COLUMNS} FROM accounts_user WHERE api_key=$1 LIMIT 2");
        let mut rows = sqlx::query(&query).bind(key).fetch_all(&self.pool).await?;
        if rows.len() != 1 {
            return Ok(None);
        }
        Ok(Some(user_from_row(rows.remove(0))?))
    }

    pub async fn setting(&self, key: &str) -> Result<Value, StoreError> {
        let value: Option<Value> =
            sqlx::query_scalar("SELECT value FROM core_coresettings WHERE key=$1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?;
        Ok(value.unwrap_or(Value::Null))
    }

    pub async fn network_allowed(
        &self,
        peer: IpAddr,
        group: &str,
        user: Option<&User>,
    ) -> Result<bool, StoreError> {
        let policy = self.setting("network_access").await?;
        let default = if group == "M3U_EPG" {
            "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7,fe80::/10"
        } else {
            "0.0.0.0/0,::/0"
        };
        if !cidrs_allow(
            policy.get(group).and_then(Value::as_str).unwrap_or(default),
            peer,
        ) {
            return Ok(false);
        }
        if let Some(raw) = user
            .and_then(|user| user.custom_properties.get("allowed_networks"))
            .and_then(|policies| policies.get(group))
            .and_then(Value::as_str)
            .filter(|raw| !raw.is_empty())
        {
            return Ok(cidrs_allow(raw, peer));
        }
        Ok(true)
    }
}

pub fn cidrs_allow(raw: &str, peer: IpAddr) -> bool {
    let peer = match peer {
        IpAddr::V6(value) => value.to_ipv4_mapped().map_or(peer, IpAddr::V4),
        _ => peer,
    };
    raw.split(',')
        .filter_map(|value| {
            let value = value.trim();
            value
                .parse::<ipnet::IpNet>()
                .ok()
                .or_else(|| value.parse::<IpAddr>().ok().map(ipnet::IpNet::from))
        })
        .any(|network| network.contains(&peer))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn networks_fail_closed_and_normalize_mapped_v4() {
        assert!(cidrs_allow(
            "192.168.0.0/16",
            "::ffff:192.168.2.3".parse().unwrap()
        ));
        assert!(!cidrs_allow(
            "invalid,10.0.0.0/8",
            "192.168.2.3".parse().unwrap()
        ));
        assert!(!cidrs_allow("", "127.0.0.1".parse().unwrap()));
        assert!(cidrs_allow("127.0.0.1", "127.0.0.1".parse().unwrap()));
        assert!(!cidrs_allow("127.0.0.1", "127.0.0.2".parse().unwrap()));
        assert!(cidrs_allow("::1", "::1".parse().unwrap()));
    }
}
