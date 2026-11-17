//! Existing Dispatcharr PostgreSQL/authentication compatibility.
//! Original application credit: Dispatcharr authors and contributors; see ATTRIBUTION.md.
mod account_writes;
mod accounts;
mod admin_writes;
mod app_data;
mod auth;
mod auth_events;
mod auth_network;
mod plugin_repositories;
pub use plugin_repositories::{
    PluginRefreshAdmission, PluginRepositories, PluginRepositoryRefresh,
};
mod rest_reads;
pub use rest_reads::{RestReadLimits, RetainedRestReads, RetainedRestSubject};
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
mod output_profile_writes;
pub use output_profile_writes::OutputProfileWrites;
mod core_utilities;
mod display_timezone;
mod django_pickle;
mod epg_policy;
mod epg_reads;
mod epg_source_writes;
pub use epg_reads::EpgReadResource;
mod epg_program_http;
pub use epg_program_http::{EpgProgramReadLimits, EpgProgramReads};
mod epg_current;
mod epg_current_http;
mod epg_grid_helpers;
mod epg_grid_window;
pub use epg_current::CurrentProgramSelection;
pub use epg_grid_helpers::{
    grid_channel_profile, grid_dummy_span, grid_encode_batch, grid_program_value,
};
pub use epg_grid_window::{
    grid_parse_datetime, grid_resolve_window, GridTimestamp, GridWindowError,
};
mod ip_lookup;
mod notifications;
mod python_action;
mod schema;
mod settings_groups;
pub use settings_groups::source_system_settings_defaults;
mod user_agent_writes;
pub use epg_policy::{source_epg_history_policy, EpgHistoryPolicy, EpgPolicyError};
mod epg_staging;
pub use epg_staging::{GuideMapping, GuideStaging, GuideSwapCounts};
pub use ip_lookup::IpLookupConfig;
pub use schema::{initialize_empty_database, SchemaInitialization};
mod backup_restore_schema;
pub use backup_restore_schema::{BackupRestoreAdmission, BackupRestoreError};
/// Source-compatible JSON decoder error text for management adapters.
pub fn source_json_error(raw: &[u8], error: serde_json::Error) -> String {
    startup_bootstrap::json_error(raw, error)
}

mod xc_auth;
mod xc_bounded_user;
pub use notifications::NotificationCache;
pub use xc_bounded_user::{BoundedXcUser, XcUserReadLimits};
mod import_batches;
mod import_cleanup;
mod import_groups;
mod import_pipeline;
pub use import_cleanup::{GroupCleanupAccount, GroupCleanupCounts, StreamCleanupCounts};
pub use import_groups::{
    GroupImportCounts, ImportedGroup, M3uGroupImportFailure, M3uGroupImportStage,
    MAX_IMPORT_GROUPS, MAX_IMPORT_GROUP_BYTES,
};
pub use import_pipeline::{CacheBatchCounts, CacheBatchRequest};
mod json_compat;
pub mod realtime;
mod startup;
mod startup_bootstrap;
pub use import_batches::{
    BoundedM3uBatchError, BoundedM3uSnapshotIssue, ImportCounts, ImportedStream, ImportedXcStream,
    M3uMutationStatement, ScannerM3uLookupCap, MAX_IMPORT_BATCH_BYTES, MAX_IMPORT_BATCH_ROWS,
};
mod image_proxy;
pub use image_proxy::SourceImageResponse;
mod html_filename;
mod logo_writes;
mod logos;
mod password_scrypt;
mod passwords;
mod playback;
mod provider_account_writes;
mod provider_mutations;
mod provider_profile_writes;
mod provider_reads;
mod provider_schedule;
mod stream_files;
mod stream_validation;
mod stream_writes;
mod streams;
mod username;
mod vod_category_settings;

pub use auth::{AuthConfig, AuthService, Claims, TokenPair};
pub use cache_codec::{decode_source_cache, encode_source_cache, source_cache_values_equal};
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
    /// Source UI permission raised before the view; render Django generic500.
    UiPermissionFault,
    /// Source unhandled REST fault; generic Django500, never auth failure401.
    UnhandledRestFault,
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
            Self::UiPermissionFault => formatter.write_str("UI permission failed"),
            Self::UnhandledRestFault => formatter.write_str("Unhandled REST request"),
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

// Job-free file APIs only; durable backup/restore jobs remain unsupported.
mod backup_files;
pub use backup_files::{BackupFiles, BackupJobApi};

#[cfg(test)]
extern crate self as dispatcharr_store;

mod epg_grid_db;
pub use epg_grid_db::GridChannel;

/// Pinned original Python HTML5 entity decoding; callers own input budgets.
pub fn source_html_unescape(value: &str) -> String {
    html_filename::unescape(value)
}

mod epg_grid_iso;

mod epg_current_file;
pub use epg_current_file::{CurrentFileLookup, CurrentFileOutcome, CurrentFileRequest};

#[cfg(test)]
mod backup_jobs_status;

mod epg_current_lookup;

// Bounded ordinary VOD reads; other VOD REST families remain separate gates.
mod vod_api_draft;
pub use vod_api_draft::source_browsable_csrf;
pub use vod_api_draft::{VodReadLimits, VodReads};

/// Safe operator component; no HTTP or destructive management routes.
pub mod operator_commands;

pub mod operator_create;

#[cfg(test)]
mod scanner_float_peer;

/// Python-compatible formatting reused by the bounded logo scanner only.
/// No arbitrary/non-finite float or cached-marker grammar authority.
pub fn source_scanner_epoch_string(value: f64) -> Option<String> {
    if !value.is_finite() || !(100_000_000.0..10_000_000_000.0).contains(&value) {
        return None;
    }
    // std::Display chooses the other shortest decimal on observed halfway
    // cases. Existing serde_json's round-to-even formatter matches Source.
    // This adapter retains its finite-epoch bounds; general JSON float
    // formatting has its own independently qualified compatibility profile.
    serde_json::Number::from_f64(value).map(|number| number.to_string())
}

#[cfg(test)]
mod json_float_peer;

/// Finite Python JSON float spelling shared by bounded source-derived DTOs.
/// Parsing, rounding, nonfinite behavior and the caller's DTO contract remain
/// separate responsibilities. This helper does not accept epoch markers.
pub fn source_finite_float_string(value: f64) -> Option<String> {
    value.is_finite().then(|| json_compat::float_string(value))
}

#[cfg(test)]
mod finite_float_public_tests {
    use super::source_finite_float_string;
    #[test]
    fn finite_adapter_preserves_qualified_spelling_and_rejects_nonfinite() {
        for (value, expected) in [
            (3.75, "3.75"),
            (4.12, "4.12"),
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1e-6, "1e-06"),
            (1e16, "1e+16"),
        ] {
            assert_eq!(source_finite_float_string(value).as_deref(), Some(expected));
        }
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(source_finite_float_string(value).is_none());
        }
    }
}

/// Source VOD proxy cache version: first eight lowercase MD5 hex characters
/// over the exact UTF-8 URL bytes, without parsing or normalization. Callers
/// must bound the input under their existing work/memory budget.
pub fn source_vod_image_version(value: &str) -> String {
    use md5::{Digest, Md5};
    format!("{:x}", Md5::digest(value.as_bytes()))[..8].to_owned()
}

#[cfg(test)]
mod vod_image_version_public_tests {
    use super::source_vod_image_version;
    #[test]
    fn actual_source_tcp38_image_version_vectors() {
        assert_eq!(
            source_vod_image_version("http://origin:8080/poster.png"),
            "6169e7b5"
        );
        assert_eq!(
            source_vod_image_version("http://origin:8080/series.png"),
            "97595d52"
        );
    }
    #[test]
    fn exact_bytes_are_not_url_normalized() {
        assert_ne!(
            source_vod_image_version("http://origin:8080/poster.png"),
            source_vod_image_version("http://origin:8080/poster.png ")
        );
        assert_ne!(
            source_vod_image_version("http://origin:8080/%E6%9D%B1"),
            source_vod_image_version("http://origin:8080/東")
        );
    }
}

pub mod recording_schedule;

pub mod recording_local_artwork;

mod core_settings_writes;
pub use core_settings_writes::{CoreSettingsGroupRead, CoreSettingsWrites};

pub mod recording_beat_cleanup;
pub mod recording_capture;

#[cfg(test)]
mod recording_sql_schema_peer;

// Private opt-in Source51 prototype. No Main/default router registration.
mod celery_metadata;
mod core_settings_special;
mod task_sql_executor;
pub use task_sql_executor::TaskSqlExecutor;
mod producer_identity;
pub use producer_identity::{ProducerIdentity, ProducerIdentitySource};
mod task_sql_session;
pub use task_sql_session::TaskSqlSession;
mod core_settings_special_broker;
mod core_settings_special_budget;
mod core_settings_special_effects;
mod core_settings_special_network;
pub use core_settings_special_effects::{
    CoreSettingsSpecialEffects, ProxyHealthKnobs, ProxySettingsRead,
};

pub mod tvg_programme;
pub mod tvg_programme_stage;

pub mod recording_existing_artwork;

pub mod recording_http;
mod recording_http_files;

mod recording_playback;
mod recording_playback_files;
pub use recording_playback::{RecordingPlayback, RecordingPlaybackOrigin};
pub mod core_task_offsets;
pub mod core_task_rehash;

pub mod channel_maintenance;
pub mod channel_maintenance_http;

mod core_task_broker;
pub use core_task_broker::{CoreTaskBroker, CoreTaskPublication};
pub mod xc_account_http;
mod xc_account_provider;
pub mod xc_account_tasks;

mod xc_live_provider;
pub mod xc_live_tasks;

mod provider_json_admission;

mod plugin_actions;
mod xc_live_filters;
mod xc_live_rollup;
pub use plugin_actions::{PluginActions, PluginRepositorySchedule};

#[doc(hidden)]
pub mod remote_m3u_io;

mod task_channel_collector;
pub use task_channel_collector::{ChannelDeletion, ChannelDeletionSelection};
mod plugin_python_host;
mod plugin_python_regex;
pub use plugin_python_host::{
    Error as PythonRegexError, NoSpawnRetirement, Receipt as PythonRegexReceipt,
};
pub use plugin_python_regex::{RegexExecutor, RegexNamedGroupRow, RegexNamedGroups};
// Actual normal auto-sync algorithm; dependency/SQL effects remain owned.
#[doc(hidden)]
pub mod auto_sync;
mod auto_sync_compact;
mod auto_sync_config;
mod auto_sync_create;
#[doc(hidden)]
pub mod auto_sync_epg;
mod auto_sync_existing;
mod auto_sync_language;
mod auto_sync_natural;
mod auto_sync_numbers;
mod auto_sync_orphans;
mod auto_sync_profiles;
mod auto_sync_regex;
mod auto_sync_resolve;
mod auto_sync_streams;
pub mod compact_numbering;
mod compact_unhide;

mod source_input_profile;

pub mod epg_connect;
pub mod epg_task_hooks;
pub mod sd_guide;
mod sd_guide_auth;
mod sd_guide_fetch;
mod sd_guide_http;
mod sd_guide_post;
mod sd_guide_posters;
mod sd_guide_programme;
mod sd_guide_state;
pub mod sd_source_http;
mod sd_source_writes;
mod sd_station_discovery;

mod redirect_playback;
mod sd_guide_progress;

// Lazy user-plugin compatibility ports; not activated by normal Main yet.
pub mod plugin_execution_host;
pub mod plugin_execution_ports;
pub mod plugin_process_guard;
pub mod plugin_publication_owners;
pub mod plugin_python_cache;
pub mod plugin_python_events;
pub mod plugin_python_registry;
pub mod plugin_python_sql;
pub mod plugin_python_sql_registry;
pub mod plugin_runtime;
pub mod plugin_trace_claims;
pub use plugin_execution_ports::{NativePluginJobs, NativePluginProcesses};
pub use plugin_publication_owners::PluginPublicationOwners;
pub use plugin_python_registry::{PluginTaskCapability, PluginTaskPolicy};
pub use plugin_runtime::{PluginRuntime, PluginTaskAdmission};

pub use plugin_execution_host::{
    Error as PluginHostError, Identity as PluginHostIdentity, Invocation as PluginInvocation,
};

mod plugin_enabled_control;
pub mod xmltv_download;

mod plugin_event_control;
mod plugin_hook_context;
mod plugin_hook_publications;
pub use plugin_hook_context::{
    NativeTaskTrail, NativeTaskTrailDelta, NativeTaskTrailEntry, NativeTaskTrailState,
    PluginHookChildReceipt, PluginHookReport, PluginHookTaskContext, PluginTemplateOutcome,
    PluginTemplateRender,
};
pub use plugin_runtime::PluginHookSession;

pub mod channel_epg_matching;

pub mod channel_epg_match_task;

pub mod channel_epg_match_http;

#[cfg(test)]
mod channel_matching_sql_peer;

pub mod m3u_refresh_all;

pub mod vod_ingestion;

pub mod comskip_http;
pub mod comskip_task;
