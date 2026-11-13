//! Opt-in bounded, physically retained REST subject/network reads.
//! Legacy authentication and Main remain on their existing read paths.
//! Original Dispatcharr authors retain application/algorithm credit.
use crate::{AuthService, StoreError, User, XcUserReadLimits};
use dispatcharr_contracts::PlaybackRequest;
use serde_json::{json, Value};
use sqlx::{Connection, Row};
use std::{future::Future, net::IpAddr, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone, Copy)]
pub struct RestReadLimits {
    pub max_raw_bytes: usize,
    pub max_nodes: usize,
    pub max_depth: usize,
    pub work_bytes: usize,
}
impl Default for RestReadLimits {
    fn default() -> Self {
        Self {
            max_raw_bytes: 65536,
            max_nodes: 2048,
            max_depth: 32,
            work_bytes: 4 * 1024 * 1024,
        }
    }
}
impl RestReadLimits {
    fn validated(self) -> Result<Self, StoreError> {
        if self.max_raw_bytes > 65536
            || self.max_nodes > 2048
            || self.max_depth > 32
            || self.work_bytes > 4 * 1024 * 1024
        {
            return Err(capacity());
        }
        self.scanner().validate().map_err(|_| capacity())?;
        Ok(self)
    }
    fn scanner(self) -> XcUserReadLimits {
        XcUserReadLimits {
            max_raw_bytes: self.max_raw_bytes,
            max_nodes: self.max_nodes,
            max_depth: self.max_depth,
            work_bytes: self.work_bytes,
        }
    }
}
fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"REST authentication read capacity unavailable."}),
    )
}
fn database_unavailable() -> StoreError {
    StoreError::StatusBody(503, json!({"error":"Database unavailable"}))
}
/// The real original request token stays with the subject's owned properties.
/// Callers borrow this User and explicitly retire it before metadata allocation.
pub struct RetainedRestSubject {
    user: User,
    pub retained_bytes: usize,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl RetainedRestSubject {
    pub fn user(&self) -> &User {
        &self.user
    }
}
#[derive(Clone)]
pub struct RetainedRestReads {
    auth: AuthService,
    permit: Arc<OwnedSemaphorePermit>,
    limits: RestReadLimits,
}
pub(crate) struct ReadSubject {
    pub(crate) user: User,
    pub(crate) retained_bytes: usize,
}

// Each row's raw JSON is capped atomically with its byte metadata. No separate
// length/read race. Username/password are bounded by the pinned SQL schema;
// their actual bytes also enter the scanner's preallocation quote.
const USER_COLUMNS:&str="id::bigint AS id,username,password,is_active,user_level,stream_limit,octet_length(COALESCE(custom_properties,'{}'::jsonb)::text) AS properties_bytes,CASE WHEN octet_length(COALESCE(custom_properties,'{}'::jsonb)::text)<=$2 THEN COALESCE(custom_properties,'{}'::jsonb)::text END AS properties_text";
const GROUP_SQL:&str="SELECT value IS NULL AS sql_null,octet_length(value::text) AS bytes,CASE WHEN octet_length(value::text)<=$2 THEN value::text END AS raw FROM core_coresettings WHERE key=$1";
struct UserScalars {
    id: i64,
    username: String,
    password: String,
    is_active: bool,
    user_level: i32,
    stream_limit: i32,
}
fn properties(
    bytes: i32,
    raw: Option<&str>,
    scalar_bytes: usize,
    limits: RestReadLimits,
) -> Result<(Value, usize), StoreError> {
    let limits = limits.validated()?;
    let raw = raw.ok_or_else(capacity)?;
    if bytes < 0
        || bytes as usize > limits.max_raw_bytes
        || raw.len() != bytes as usize
        || scalar_bytes > 2048
    {
        return Err(capacity());
    }
    crate::xc_bounded_user::checked_properties(raw, scalar_bytes, limits.scanner()).map_err(
        |error| match error {
            StoreError::StatusBody(503, _) => capacity(),
            other => other,
        },
    )
}
fn decode_user(
    scalars: UserScalars,
    bytes: i32,
    raw: Option<&str>,
    limits: RestReadLimits,
) -> Result<ReadSubject, StoreError> {
    let (custom_properties, retained_bytes) = properties(
        bytes,
        raw,
        scalars.username.len() + scalars.password.len(),
        limits,
    )?;
    Ok(ReadSubject {
        user: User {
            id: scalars.id,
            username: scalars.username,
            password: scalars.password,
            is_active: scalars.is_active,
            user_level: scalars.user_level,
            stream_limit: scalars.stream_limit,
            custom_properties,
        },
        retained_bytes,
    })
}
fn classify_count(count: usize) -> Result<bool, StoreError> {
    match count {
        0 | 2 => Ok(false),
        1 => Ok(true),
        _ => Err(database_unavailable()),
    }
}
fn network_value(
    bytes: Option<i32>,
    raw: Option<&str>,
    sql_null: bool,
    limits: RestReadLimits,
) -> Result<Value, StoreError> {
    // SQL NULL is forbidden by the pinned CoreSettings schema. Preserve a
    // database-unavailable failure if a different/corrupt schema supplies it.
    if sql_null {
        return Err(database_unavailable());
    }
    let (value, _quote) = properties(bytes.ok_or_else(database_unavailable)?, raw, 0, limits)?;
    Ok(if value.as_object().is_some_and(|v| !v.is_empty()) {
        value
    } else {
        json!({})
    })
}
// Each child belongs to the already admitted request: it carries the SAME
// original permit, never acquires another slot, and is not aborted when its
// observer disappears. SQL acquisition/read/Ready/return or close all finish
// inside this scope. Runtime shutdown/panic is a separate unqualified boundary.
async fn supervised_sql_read<T: Send + 'static>(
    original: Arc<OwnedSemaphorePermit>,
    work: impl Future<Output = Result<T, StoreError>> + Send + 'static,
) -> Result<T, StoreError> {
    tokio::spawn(async move {
        let _original = original;
        work.await
    })
    .await
    .map_err(|_| database_unavailable())?
}
impl AuthService {
    pub(crate) fn rest_writer(&self) -> Result<Arc<OwnedSemaphorePermit>, StoreError> {
        let permit = self.request_writer_permit.as_ref().ok_or_else(capacity)?;
        if !Arc::ptr_eq(permit.semaphore(), &self.user_agent_workers) {
            return Err(capacity());
        }
        Ok(permit.clone())
    }
    /// Explicit per-request opt-in. No token is acquired and no pool is created.
    pub fn retained_rest_reads(
        &self,
        limits: RestReadLimits,
    ) -> Result<RetainedRestReads, StoreError> {
        let permit = self.rest_writer()?;
        let limits = limits.validated()?;
        Ok(RetainedRestReads {
            auth: self.clone(),
            permit,
            limits,
        })
    }
    pub(crate) async fn rest_read_user_id(
        &self,
        id: i64,
        limits: Option<RestReadLimits>,
    ) -> Result<Option<ReadSubject>, StoreError> {
        let Some(limits) = limits else {
            return Ok(self.store.user_by_id(id).await?.map(|user| ReadSubject {
                user,
                retained_bytes: 0,
            }));
        };
        self.bounded_subject(id, None, limits).await
    }
    pub(crate) async fn rest_read_user_key(
        &self,
        key: &str,
        limits: Option<RestReadLimits>,
    ) -> Result<Option<ReadSubject>, StoreError> {
        let Some(limits) = limits else {
            return Ok(self
                .store
                .user_by_api_key(key)
                .await?
                .map(|user| ReadSubject {
                    user,
                    retained_bytes: 0,
                }));
        };
        self.bounded_subject(0, Some(key), limits).await
    }
    async fn bounded_subject(
        &self,
        id: i64,
        key: Option<&str>,
        limits: RestReadLimits,
    ) -> Result<Option<ReadSubject>, StoreError> {
        let original = self.rest_writer()?;
        let limits = limits.validated()?;
        if key.is_some_and(|key| key.len() > 8192) {
            return Err(capacity());
        }
        let key = key.map(str::to_owned);
        let auth = self.clone();
        supervised_sql_read(original, async move {
            auth.bounded_subject_owned(id, key.as_deref(), limits).await
        })
        .await
    }
    async fn bounded_subject_owned(
        &self,
        id: i64,
        key: Option<&str>,
        limits: RestReadLimits,
    ) -> Result<Option<ReadSubject>, StoreError> {
        let mut connection = self.store.pool().acquire().await?;
        let query = format!(
            "SELECT {USER_COLUMNS} FROM accounts_user WHERE {}=$1 LIMIT 2",
            if key.is_some() { "api_key" } else { "id" }
        );
        let result = if let Some(key) = key {
            sqlx::query(&query)
                .bind(key)
                .bind(limits.max_raw_bytes as i32)
                .fetch_all(&mut *connection)
                .await
        } else {
            sqlx::query(&query)
                .bind(id)
                .bind(limits.max_raw_bytes as i32)
                .fetch_all(&mut *connection)
                .await
        };
        // Errors may arrive before Ready. Keep the actual original token until the
        // same physical connection is returned or closed. No requery/second close.
        let ping = connection.ping().await;
        if ping.is_err() {
            connection.close().await?;
        } else {
            connection.return_to_pool().await;
        }
        let mut rows = result?;
        // Native legacy zero/two rows returns None BEFORE decoding either row.
        // Source duplicate .get() renderer/status is a separate UNMEASURED gate.
        if !classify_count(rows.len())? {
            return Ok(None);
        }
        let row = rows.remove(0);
        let scalars = UserScalars {
            id: row.try_get("id")?,
            username: row.try_get("username")?,
            password: row.try_get("password")?,
            is_active: row.try_get("is_active")?,
            user_level: row.try_get("user_level")?,
            stream_limit: row.try_get("stream_limit")?,
        };
        let bytes: i32 = row.try_get("properties_bytes")?;
        let raw: Option<String> = row.try_get("properties_text")?;
        decode_user(scalars, bytes, raw.as_deref(), limits).map(Some)
    }
}
impl RetainedRestReads {
    pub async fn authenticate(
        &self,
        request: &PlaybackRequest,
    ) -> Result<Option<RetainedRestSubject>, StoreError> {
        let headers = request
            .header("authorization")
            .map_or(0, str::len)
            .saturating_add(request.header("x-api-key").map_or(0, str::len));
        if headers > 8192 {
            return Err(capacity());
        }
        Ok(self
            .auth
            .authenticate_read_strategy(request, Some(self.limits))
            .await?
            .map(|subject| RetainedRestSubject {
                user: subject.user,
                retained_bytes: subject.retained_bytes,
                _permit: self.permit.clone(),
            }))
    }
    async fn network_group(&self) -> Result<Value, StoreError> {
        let original = self.auth.rest_writer()?;
        let reads = self.clone();
        supervised_sql_read(original, async move { reads.network_group_owned().await }).await
    }
    async fn network_group_owned(&self) -> Result<Value, StoreError> {
        let mut connection = self.auth.store.pool().acquire().await?;
        let result = sqlx::query(GROUP_SQL)
            .bind("network_access")
            .bind(self.limits.max_raw_bytes as i32)
            .fetch_optional(&mut *connection)
            .await;
        let ping = connection.ping().await;
        if ping.is_err() {
            connection.close().await?;
        } else {
            connection.return_to_pool().await;
        }
        match result? {
            None => Ok(json!({})),
            Some(row) => {
                let bytes: Option<i32> = row.try_get("bytes")?;
                let raw: Option<String> = row.try_get("raw")?;
                network_value(bytes, raw.as_deref(), row.try_get("sql_null")?, self.limits)
            }
        }
    }
    /// Same grouped-cache algorithm/budget as legacy; only its SQL miss loader
    /// changes. Read/capacity failures return before cache fill or auth penalties.
    pub async fn ui_network_allowed(
        &self,
        ip: IpAddr,
        subject: Option<&RetainedRestSubject>,
    ) -> Result<bool, StoreError> {
        if subject.is_some_and(|subject| !Arc::ptr_eq(&subject._permit, &self.permit)) {
            return Err(capacity());
        }
        let policy = match self.auth.notification_cache.as_ref() {
            Some(cache) => {
                cache
                    .settings_group("network_access", || self.network_group())
                    .await?
            }
            None => self.network_group().await?,
        };
        if !crate::xc_auth::system_allowed(&policy, "UI", ip)
            .map_err(crate::auth_network::ui_permission_error)?
        {
            return Ok(false);
        }
        subject.map_or(Ok(true), |subject| {
            crate::xc_auth::user_allowed(subject.user(), "UI", ip)
                .map_err(crate::auth_network::ui_permission_error)
        })
    }
}
#[cfg(test)]
#[path = "rest_reads_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "rest_reads_physical_tests.rs"]
mod physical_tests;
