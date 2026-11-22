//! SQLite identity, settings, and ordered login writes for an existing adapted Original schema.
//! Original Dispatcharr authors retain full application credit.

use crate::{auth_events::audit_event_details, passwords::MAX_CREDENTIAL_BYTES, StoreError, User};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::types::Json;
use sqlx::{Executor, Row, SqlitePool};
use std::{path::Path, time::Duration};

const USER_BY_USERNAME: &str = "SELECT id, username, password, is_active, user_level, stream_limit, COALESCE(custom_properties, '{}') AS custom_properties FROM accounts_user WHERE username=?";
const USER_BY_ID: &str = "SELECT id, username, password, is_active, user_level, stream_limit, COALESCE(custom_properties, '{}') AS custom_properties FROM accounts_user WHERE id=?";
const SETTING: &str = "SELECT value FROM core_coresettings WHERE key=?";
const SCHEMA_MARKERS: &str = "SELECT COUNT(DISTINCT name) FROM django_migrations WHERE (app='dispatcharr_channels' AND name='0038_add_catchup_fields') OR (app='vod' AND name='0003_vodlogo_alter_movie_logo_alter_series_logo')";

const PINNED_MIGRATIONS: [(&str, &str); 184] = include!("../datasets/showmigrations-plan-order.in");
const SCOPED_TABLE_COUNT: i64 = 10;
const SCOPED_TABLES: &str =
    "SELECT count(*) FROM main.sqlite_schema WHERE type='table' AND name IN (
    'accounts_user', 'core_coresettings', 'core_systemevent', 'django_migrations',
    'dispatcharr_channels_channel', 'dispatcharr_channels_channelgroup',
    'dispatcharr_channels_channelgroupm3uaccount', 'dispatcharr_channels_channeloverride',
    'dispatcharr_channels_stream', 'm3u_m3uaccount'
)";
const MIGRATION_HISTORY: &str = "SELECT
    CASE WHEN length(CAST(app AS BLOB))<=255 THEN app ELSE '' END,
    CASE WHEN length(CAST(name AS BLOB))<=255 THEN name ELSE '' END
    FROM main.django_migrations LIMIT ?";

#[derive(Clone, Copy)]
enum SchemaScope {
    IdentityRead,
    Login,
    LoginAndGroups,
}

const COMPARE_HASH: &str = "UPDATE accounts_user SET password=? WHERE id=? AND password=?";
const RECORD_LAST_LOGIN: &str = "UPDATE accounts_user SET last_login=? WHERE username=?";
const INSERT_LOGIN_EVENT: &str = "INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) VALUES('login_success',?,NULL,NULL,?)";

#[cfg(feature = "sqlite-copied-data-runtime")]
const INSERT_LOGIN_FAILURE: &str = "INSERT INTO core_systemevent(event_type,timestamp,channel_id,channel_name,details) VALUES('login_failed',?,NULL,NULL,?)";

/// Outcome after attempting the post-verification successful login writes.
#[derive(Debug)]
pub enum LoginPersistence {
    NoRawUsernameMatch,
    Saved,
    LastLoginSavedAuditFailed(StoreError),
}

/// Bounded repository for the assessed identity and setting tables.
/// Main and authentication routes do not construct this component.
pub struct SqliteIdentity {
    pool: SqlitePool,
}

impl SqliteIdentity {
    /// Require an existing file with both relevant migration markers and
    /// the identity/settings query shape. This does not check other tables.
    pub async fn open_existing(path: &Path) -> Result<Self, StoreError> {
        Self::open(path, SchemaScope::IdentityRead).await
    }

    /// Require an existing compatible writable copy for owned login integration.
    pub async fn open_login_copy(path: &Path) -> Result<Self, StoreError> {
        Self::open(path, SchemaScope::Login).await
    }

    /// Open a writable, already migrated copy for the scoped login and
    /// Channel Groups collection. No schema or user writes occur on open.
    pub async fn open_login_groups_copy(path: &Path) -> Result<Self, StoreError> {
        Self::open(path, SchemaScope::LoginAndGroups).await
    }

    /// Admit a detached current file read-only, then check the same scoped
    /// statements in the bounded writable pool returned to the runtime owner.
    pub async fn open_current_login_groups(path: &Path) -> Result<Self, StoreError> {
        if !std::fs::symlink_metadata(path).is_ok_and(|entry| entry.is_file()) {
            return Err(StoreError::Unsupported(
                "SQLite requires an existing regular database file",
            ));
        }
        let probe = Self::pool_for(path, true).await?;
        let checked = probe.check_login_groups_schema().await;
        probe.close().await;
        checked?;
        Self::open_login_groups_copy(path).await
    }

    async fn open(path: &Path, scope: SchemaScope) -> Result<Self, StoreError> {
        let login_writes = !matches!(scope, SchemaScope::IdentityRead);
        let repository = Self::pool_for(path, !login_writes).await?;
        let checked = match scope {
            SchemaScope::LoginAndGroups => repository.check_login_groups_schema().await,
            _ => repository.check_schema(login_writes).await,
        };
        if let Err(error) = checked {
            repository.close().await;
            return Err(error);
        }
        Ok(repository)
    }

    async fn pool_for(path: &Path, read_only: bool) -> Result<Self, StoreError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .read_only(read_only)
            .create_if_missing(false)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(1));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(2))
            .connect_with(options)
            .await?;
        Ok(Self { pool })
    }

    async fn check_schema(&self, login_writes: bool) -> Result<(), StoreError> {
        let markers: i64 = sqlx::query_scalar(SCHEMA_MARKERS)
            .fetch_one(&self.pool)
            .await?;
        if markers != 2 {
            return Err(StoreError::Unsupported(
                "SQLite identity schema migration markers are missing",
            ));
        }
        let writes = [COMPARE_HASH, RECORD_LAST_LOGIN, INSERT_LOGIN_EVENT];
        let queries = [USER_BY_USERNAME, USER_BY_ID, SETTING]
            .into_iter()
            .chain(writes.into_iter().filter(|_| login_writes));
        for query in queries {
            self.pool.prepare(query).await?;
        }
        Ok(())
    }

    async fn check_login_groups_schema(&self) -> Result<(), StoreError> {
        // Read metadata and prepare the actual statements in one transaction
        // on the same bounded connection the caller will use.
        let mut snapshot = self.pool.begin().await?;
        let mut expected = PINNED_MIGRATIONS;
        expected.sort_unstable();
        let applied: Vec<(String, String)> = sqlx::query_as(MIGRATION_HISTORY)
            .bind(expected.len() as i64 + 1)
            .fetch_all(&mut *snapshot)
            .await?;
        let mut graph: Vec<_> = applied
            .iter()
            .map(|(app, name)| (app.as_str(), name.as_str()))
            .collect();
        graph.sort_unstable();
        if graph != expected {
            return Err(StoreError::Unsupported(
                "SQLite login and groups require the pinned migration history",
            ));
        }
        let tables: i64 = sqlx::query_scalar(SCOPED_TABLES)
            .fetch_one(&mut *snapshot)
            .await?;
        if tables != SCOPED_TABLE_COUNT {
            return Err(StoreError::Unsupported(
                "SQLite login and groups schema tables are missing",
            ));
        }
        let identity = [
            USER_BY_USERNAME,
            USER_BY_ID,
            SETTING,
            COMPARE_HASH,
            RECORD_LAST_LOGIN,
            INSERT_LOGIN_EVENT,
        ];
        let queries = identity
            .into_iter()
            .chain(crate::rest_reads::sqlite::READ_SCHEMA_QUERIES)
            .chain(crate::channel_groups::sqlite::COLLECTION_SCHEMA_QUERIES);
        for query in queries {
            snapshot.prepare(query).await?;
        }
        #[cfg(feature = "sqlite-copied-data-runtime")]
        {
            snapshot.prepare(INSERT_LOGIN_FAILURE).await?;
            snapshot
                .prepare(crate::rest_reads::sqlite::CHANNEL_PROFILE_COLLECTION_SQL)
                .await?;
            use crate::auth::sqlite_runtime::{
                application_data, core_catalog, epg_collections, m3u_accounts, profile,
            };
            for statement in profile::STATEMENTS
                .into_iter()
                .chain(application_data::STATEMENTS)
                .chain(core_catalog::Catalog::STATEMENTS)
                .chain(epg_collections::STATEMENTS)
                .chain([m3u_accounts::QUERY, crate::notifications::sqlite::QUERY])
                .chain(crate::auth::sqlite_runtime::stream_filter_options::STATEMENTS)
                .chain(crate::channel_groups::sqlite::writes::STATEMENTS)
            {
                snapshot.prepare(statement).await?;
            }
        }
        snapshot.commit().await?;
        Ok(())
    }

    pub async fn user_by_username(&self, username: &str) -> Result<Option<User>, StoreError> {
        sqlx::query(USER_BY_USERNAME)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?
            .map(user_from_row)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn user_by_id(&self, id: i64) -> Result<Option<User>, StoreError> {
        sqlx::query(USER_BY_ID)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(user_from_row)
            .transpose()
            .map_err(Into::into)
    }

    pub async fn setting(&self, key: &str) -> Result<Value, StoreError> {
        let row: Option<Json<Value>> = sqlx::query_scalar(SETTING)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map_or(Value::Null, |value| value.0))
    }

    /// Store the verified replacement only while the checked hash is still current.
    pub async fn compare_and_save_hash(
        &self,
        user: &User,
        replacement: &str,
    ) -> Result<bool, StoreError> {
        if user.password.len() > MAX_CREDENTIAL_BYTES || replacement.len() > MAX_CREDENTIAL_BYTES {
            return Err(StoreError::Unsupported(
                "SQLite login hash exceeds the supported size",
            ));
        }
        let updated = sqlx::query(COMPARE_HASH)
            .bind(replacement)
            .bind(user.id)
            .bind(&user.password)
            .execute(&self.pool)
            .await?;
        Ok(updated.rows_affected() == 1)
    }

    /// Run after ordinary password verification and active-account admission.
    ///
    /// The caller supplies the existing raw-username request interpretation.
    /// The saved clock commits before the best-effort audit, as in Original.
    /// Event dispatch and configured audit retention remain caller work.
    pub async fn record_login_success(
        &self,
        raw_username: &str,
        audit_user: Value,
        client_ip: &str,
        user_agent: &str,
        mut now: impl FnMut() -> DateTime<Utc>,
    ) -> Result<LoginPersistence, StoreError> {
        let last_login = django_datetime(now());
        let updated = sqlx::query(RECORD_LAST_LOGIN)
            .bind(last_login)
            .bind(raw_username)
            .execute(&self.pool)
            .await?;
        if updated.rows_affected() == 0 {
            return Ok(LoginPersistence::NoRawUsernameMatch);
        }
        let details = audit_event_details(audit_user, client_ip, user_agent, None);
        let result = sqlx::query(INSERT_LOGIN_EVENT)
            .bind(django_datetime(now()))
            .bind(Json(details))
            .execute(&self.pool)
            .await;
        Ok(match result {
            Ok(_) => LoginPersistence::Saved,
            Err(error) => LoginPersistence::LastLoginSavedAuditFailed(error.into()),
        })
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn record_login_failure(
        &self,
        user: Value,
        client_ip: &str,
        user_agent: &str,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        sqlx::query(INSERT_LOGIN_FAILURE)
            .bind(django_datetime(now))
            .bind(Json(audit_event_details(
                user,
                client_ip,
                user_agent,
                Some(reason),
            )))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn close(self) {
        self.pool.close().await;
    }
}

fn django_datetime(now: DateTime<Utc>) -> String {
    let format = if now.timestamp_subsec_micros() == 0 {
        "%Y-%m-%d %H:%M:%S"
    } else {
        "%Y-%m-%d %H:%M:%S%.6f"
    };
    now.naive_utc().format(format).to_string()
}

fn user_from_row(row: SqliteRow) -> Result<User, sqlx::Error> {
    Ok(User {
        id: row.try_get("id")?,
        username: row.try_get("username")?,
        password: row.try_get("password")?,
        is_active: row.try_get("is_active")?,
        user_level: row.try_get("user_level")?,
        stream_limit: row.try_get("stream_limit")?,
        custom_properties: row.try_get::<Json<Value>, _>("custom_properties")?.0,
    })
}

#[cfg(test)]
#[path = "sqlite_readiness_adapter_contract.rs"]
mod readiness_contract;
