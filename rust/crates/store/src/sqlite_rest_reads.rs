//! Held SQLite bearer reads for the scoped direct-data integration.
//! Original Dispatcharr authors retain all application credit.

use super::*;
use crate::{auth, auth_throttle, AuthConfig, NotificationCache};
use sqlx::{sqlite::SqliteRow, SqlitePool};
use tokio::sync::Semaphore;

const SUBJECT_SQL: &str = "SELECT id,is_active,user_level,stream_limit,CASE WHEN length(CAST(username AS BLOB))+length(CAST(password AS BLOB))<=2048 THEN username END AS username,CASE WHEN length(CAST(username AS BLOB))+length(CAST(password AS BLOB))<=2048 THEN password END AS password,length(CAST(COALESCE(custom_properties,'{}') AS BLOB)) AS properties_bytes,CASE WHEN length(CAST(COALESCE(custom_properties,'{}') AS BLOB))<=?2 THEN COALESCE(custom_properties,'{}') END AS properties_text FROM accounts_user WHERE id=?1 LIMIT 2";
const SQLITE_GROUP_SQL: &str = "SELECT value IS NULL AS sql_null,length(CAST(value AS BLOB)) AS bytes,CASE WHEN length(CAST(value AS BLOB))<=?2 THEN value END AS raw FROM core_coresettings WHERE key=?1";

pub(crate) const READ_SCHEMA_QUERIES: [&str; 2] = [SUBJECT_SQL, SQLITE_GROUP_SQL];

/// An actual request permit is required and remains held through SQL and
/// grouped network decisions. This type binds no HTTP listener.
pub struct RetainedSqliteRestReads {
    config: AuthConfig,
    cache: Option<NotificationCache>,
    pool: SqlitePool,
    permit: Arc<OwnedSemaphorePermit>,
    limits: RestReadLimits,
}

impl RetainedSqliteRestReads {
    pub fn from_owned_request(
        identity: &crate::sqlite_identity::SqliteIdentity,
        config: AuthConfig,
        cache: Option<NotificationCache>,
        owner: &Arc<Semaphore>,
        permit: Arc<OwnedSemaphorePermit>,
        limits: RestReadLimits,
    ) -> Result<Self, StoreError> {
        if !Arc::ptr_eq(owner, permit.semaphore()) {
            return Err(capacity());
        }
        let limits = limits.validated()?;
        let cache = cache.map(|cache| cache.with_retained_scope(permit.clone()));
        Ok(Self {
            config,
            cache,
            pool: identity.pool().clone(),
            permit,
            limits,
        })
    }

    /// Reuse the shared Redis admission budget. A full login owner must also
    /// handle lockout and audit side effects and the subsequent UI policy.
    pub async fn record_login_attempt(
        &self,
        request: &PlaybackRequest,
    ) -> Result<bool, StoreError> {
        auth_throttle::cached_auth_budget(
            self.cache.as_ref(),
            auth::source_client_ip_for_config(&self.config, request),
            auth_throttle::Scope::Login,
            true,
            false,
        )
        .await
    }

    pub async fn bearer_account_user(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        admin: bool,
    ) -> Result<RetainedRestSubject, StoreError> {
        if ["authorization", "x-api-key"].iter().any(|name| {
            headers
                .get(*name)
                .is_some_and(|value| value.to_str().is_err())
        }) {
            return Err(StoreError::Unauthorized("Invalid authentication header"));
        }
        let request = auth::request(peer, headers);
        let ip = auth::source_client_ip_for_config(&self.config, &request).address();
        authorize_ordered(
            self.authenticate_bearer(&request),
            |subject| async move {
                let allowed = self.ui_network_allowed(ip, &subject).await?;
                Ok((subject, allowed))
            },
            admin,
        )
        .await
    }

    /// Resolve the ordinary collection with the same held bearer request.
    /// A future application must still bind a qualified direct route.
    pub async fn channel_group_collection(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<axum::response::Response, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result =
                crate::channel_groups::sqlite::collection(&mut connection, subject.user()).await;
            settle(connection).await?;
            Ok(crate::channel_groups::collection_response(
                result?,
                axum::http::Method::GET,
            ))
        })
        .await
    }

    async fn authenticate_bearer(
        &self,
        request: &PlaybackRequest,
    ) -> Result<Option<RetainedRestSubject>, StoreError> {
        let length = request
            .header("authorization")
            .map_or(0, str::len)
            .saturating_add(request.header("x-api-key").map_or(0, str::len));
        if length > 8192 {
            return Err(capacity());
        }
        let headers: Vec<_> = request
            .header("authorization")
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        let Some(claims) = auth::bearer_claims(&self.config, &headers)? else {
            return Ok(None);
        };
        let id = claims.id().ok_or_else(capacity)?;
        let pool = self.pool.clone();
        let limits = self.limits;
        let row = supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = sqlx::query(SUBJECT_SQL)
                .bind(id)
                .bind(limits.max_raw_bytes as i32)
                .fetch_all(&mut *connection)
                .await;
            settle(connection).await?;
            let mut rows = result?;
            if !classify_count(rows.len())? {
                return Ok(None);
            }
            decode_sqlite_user(rows.remove(0), limits).map(Some)
        })
        .await?;
        let row = auth::active_bearer_subject(row)?;
        Ok(Some(RetainedRestSubject {
            user: row.user,
            retained_bytes: row.retained_bytes,
            _permit: self.permit.clone(),
        }))
    }

    async fn network_group(&self) -> Result<Value, StoreError> {
        let pool = self.pool.clone();
        let limits = self.limits;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = sqlx::query(SQLITE_GROUP_SQL)
                .bind("network_access")
                .bind(limits.max_raw_bytes as i32)
                .fetch_optional(&mut *connection)
                .await;
            settle(connection).await?;
            match result? {
                None => Ok(json!({})),
                Some(row) => {
                    let bytes: Option<i32> = row.try_get("bytes")?;
                    let raw: Option<String> = row.try_get("raw")?;
                    network_value(bytes, raw.as_deref(), row.try_get("sql_null")?, limits)
                }
            }
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn login_network_allowed(
        &self,
        request: &PlaybackRequest,
    ) -> Result<bool, StoreError> {
        let ip = auth::source_client_ip_for_config(&self.config, request).address();
        let cache = self.cache.as_ref().ok_or_else(capacity)?;
        let policy = cache
            .settings_group_bounded("network_access", || self.network_group())
            .await?;
        crate::xc_auth::system_allowed(&policy, "UI", ip)
    }

    async fn ui_network_allowed(
        &self,
        ip: IpAddr,
        subject: &RetainedRestSubject,
    ) -> Result<bool, StoreError> {
        if !Arc::ptr_eq(&subject._permit, &self.permit) {
            return Err(capacity());
        }
        let policy = match self.cache.as_ref() {
            Some(cache) => {
                cache
                    .settings_group_bounded("network_access", || self.network_group())
                    .await?
            }
            None => self.network_group().await?,
        };
        ui_policy_allows(&policy, ip, Some(subject.user()))
    }
}

async fn settle(
    mut connection: sqlx::pool::PoolConnection<sqlx::Sqlite>,
) -> Result<(), StoreError> {
    if connection.ping().await.is_err() {
        connection.close().await?;
    } else {
        connection.return_to_pool().await;
    }
    Ok(())
}

fn decode_sqlite_user(row: SqliteRow, limits: RestReadLimits) -> Result<ReadSubject, StoreError> {
    let (Some(username), Some(password)) = (
        row.try_get::<Option<String>, _>("username")?,
        row.try_get::<Option<String>, _>("password")?,
    ) else {
        return Err(capacity());
    };
    let scalars = UserScalars {
        id: row.try_get("id")?,
        username,
        password,
        is_active: row.try_get("is_active")?,
        user_level: row.try_get("user_level")?,
        stream_limit: row.try_get("stream_limit")?,
    };
    let raw: Option<String> = row.try_get("properties_text")?;
    decode_user(
        scalars,
        row.try_get("properties_bytes")?,
        raw.as_deref(),
        limits,
    )
}
