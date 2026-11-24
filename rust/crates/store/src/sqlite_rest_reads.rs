//! Held SQLite bearer reads for the scoped direct-data integration.
//! Original Dispatcharr authors retain all application credit.

use super::*;
use crate::{auth, auth_throttle, AuthConfig, NotificationCache};

#[cfg(feature = "sqlite-copied-data-runtime")]
use futures_util::TryStreamExt;
use sqlx::{sqlite::SqliteRow, SqlitePool};
#[cfg(feature = "sqlite-copied-data-runtime")]
use std::collections::HashMap;
use tokio::sync::Semaphore;

#[cfg(feature = "sqlite-copied-data-runtime")]
pub(crate) enum ChannelRead {
    Collection,
    Detail(String),
    Projection(crate::channels::sqlite_list::ChannelProjection),
}

const SUBJECT_SQL: &str = "SELECT id,is_active,user_level,stream_limit,CASE WHEN length(CAST(username AS BLOB))+length(CAST(password AS BLOB))<=2048 THEN username END AS username,CASE WHEN length(CAST(username AS BLOB))+length(CAST(password AS BLOB))<=2048 THEN password END AS password,length(CAST(COALESCE(custom_properties,'{}') AS BLOB)) AS properties_bytes,CASE WHEN length(CAST(COALESCE(custom_properties,'{}') AS BLOB))<=?2 THEN COALESCE(custom_properties,'{}') END AS properties_text FROM accounts_user WHERE id=?1 LIMIT 2";
const SQLITE_GROUP_SQL: &str = "SELECT value IS NULL AS sql_null,length(CAST(value AS BLOB)) AS bytes,CASE WHEN length(CAST(value AS BLOB))<=?2 THEN value END AS raw FROM core_coresettings WHERE key=?1";

#[cfg(feature = "sqlite-copied-data-runtime")]
pub(crate) const CHANNEL_PROFILE_COLLECTION_SQL: &str = "
    SELECT p.id, p.name, m.channel_id
    FROM dispatcharr_channels_channelprofile p
    LEFT JOIN dispatcharr_channels_channelprofilemembership m
        ON m.channel_profile_id=p.id AND m.enabled
    WHERE ?1 OR EXISTS(
        SELECT 1 FROM accounts_user_channel_profiles up
        WHERE up.channelprofile_id=p.id AND up.user_id=?2
    )
    ORDER BY p.id,m.id";

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

#[cfg(feature = "sqlite-copied-data-runtime")]
async fn core_settings_rows(
    connection: &mut sqlx::SqliteConnection,
    id: Option<i64>,
) -> Result<Value, StoreError> {
    let rows =
        sqlx::query("SELECT id,key,name,value FROM core_coresettings WHERE (?1 IS NULL OR id=?1)")
            .bind(id)
            .fetch_all(connection)
            .await?;
    let mut settings = rows
        .into_iter()
        .map(|row| {
            let value = row.try_get::<Option<sqlx::types::Json<Value>>, _>("value")?;
            Ok(crate::core_reads::settings_value(
                row.try_get("id")?,
                row.try_get("key")?,
                row.try_get("name")?,
                value.map_or(Value::Null, |value| value.0),
            ))
        })
        .collect::<Result<Vec<_>, sqlx::Error>>()?;
    if id.is_some() {
        settings.pop().ok_or_else(|| {
            StoreError::StatusBody(
                404,
                json!({"detail": "No CoreSettings matches the given query."}),
            )
        })
    } else {
        Ok(Value::Array(settings))
    }
}

#[cfg(all(test, feature = "sqlite-copied-data-runtime"))]
#[tokio::test]
async fn copied_setup_writes_a_real_django_sqlite_administrator() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(include_str!("../datasets/sqlite-accounts_user.sql"))
        .execute(&pool)
        .await
        .unwrap();
    let budget = Arc::new(Semaphore::new(1));
    let permit = Arc::new(budget.try_acquire_owned().unwrap());
    let reads = RetainedSqliteRestReads {
        config: AuthConfig::new("copied-bootstrap-source-fixture".to_owned()).unwrap(),
        cache: None,
        pool: pool.clone(),
        permit,
        limits: RestReadLimits::default(),
    };
    assert!(!reads.bootstrap_admin_exists().await.unwrap());
    let body = axum::extract::Request::builder()
        .body(axum::body::Body::from(
            r#"{"username":"setup-user","password":"owned-input","email":"User@EXAMPLE.INVALID"}"#,
        ))
        .unwrap();
    let credentials = crate::startup_bootstrap::parse_credentials(body)
        .await
        .unwrap();
    let hash = crate::auth::hash_for_workers(&Arc::new(Semaphore::new(1)), credentials.password)
        .await
        .unwrap();
    assert!(hash.starts_with("pbkdf2_sha256$"));
    reads
        .bootstrap_create_admin(credentials.username, credentials.email, hash.clone())
        .await
        .unwrap();
    assert!(reads.bootstrap_admin_exists().await.unwrap());

    let (stored, name, mail, level, staff, superuser, active) =
        sqlx::query_as::<_, (String, String, String, i64, bool, bool, bool)>(
            "SELECT password,username,email,user_level,is_staff,is_superuser,is_active FROM accounts_user",
        )
        .fetch_one(&pool).await.unwrap();
    assert_eq!(stored, hash);
    assert_eq!(
        (name.as_str(), mail.as_str(), level),
        ("setup-user", "User@example.invalid", 10)
    );
    assert!(staff && superuser && active);
    assert!(matches!(
        reads.bootstrap_create_admin(name, mail, hash).await,
        Err(StoreError::Database(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM accounts_user")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    pool.close().await;
}

impl RetainedSqliteRestReads {
    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn single_from_stream(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        uri: axum::http::Uri,
        body: crate::auth::sqlite_runtime::single_from_stream::Body,
        sink: Option<crate::auth::sqlite_runtime::single_from_stream::Sink>,
    ) -> Result<axum::response::Response, StoreError> {
        use axum::response::IntoResponse;
        let subject = self
            .bearer_account_user(peer, headers.clone(), true)
            .await?;
        let sink = sink.ok_or(StoreError::StatusBody(
            503,
            json!({"detail":"Copied SQLite channel publication unavailable."}),
        ))?;
        if uri.query().is_some_and(|q| !q.is_empty()) {
            return Err(StoreError::Unsupported(
                "Copied SQLite single-create JSON query options unqualified",
            ));
        }
        if headers
            .get("accept")
            .is_some_and(|v| !matches!(v.to_str(), Ok("application/json" | "*/*")))
        {
            return Err(StoreError::Unsupported(
                "Copied SQLite single-create renderer unqualified",
            ));
        }
        let base = crate::channels::base_url(&headers, &uri)?;
        let pool = self.pool.clone();
        let limit = self.limits.work_bytes;
        let scope = self.permit.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let created = crate::auth::sqlite_runtime::single_from_stream::command(
                &mut connection,
                subject.user(),
                body,
                &base,
                limit,
            )
            .await;
            settle(connection).await?;
            let created = created?;
            // Source plain Channels send failures are logged after durable commit.
            if sink
                .send_retained("updates", created.event, scope)
                .await
                .is_err()
            {
                tracing::warn!("Copied SQLite channel creation publication failed after commit");
            }
            Ok((
                axum::http::StatusCode::CREATED,
                [
                    (axum::http::header::ALLOW, "POST, OPTIONS"),
                    (axum::http::header::CONTENT_TYPE, "application/json"),
                    (axum::http::header::VARY, "Accept, origin"),
                ],
                created.response,
            )
                .into_response())
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn bootstrap_admin_exists(&self) -> Result<bool, StoreError> {
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM accounts_user WHERE user_level >= 10)",
            )
            .fetch_one(&mut *connection)
            .await;
            settle(connection).await?;
            Ok(result?)
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn bootstrap_create_admin(
        &self,
        username: String,
        email: String,
        encoded: String,
    ) -> Result<(), StoreError> {
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = sqlx::query(
                "INSERT INTO accounts_user(password,last_login,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,avatar_config,stream_limit,api_key) VALUES(?1,NULL,1,?2,'','',?3,1,1,?4,10,'{}','{}',0,NULL)",
            )
            .bind(encoded)
            .bind(username)
            .bind(email)
            .bind(chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .execute(&mut *connection)
            .await;
            settle(connection).await?;
            Ok(result.map(|_| ())?)
        })
        .await
    }

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

    fn checked_bearer_request(
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<PlaybackRequest, StoreError> {
        if ["authorization", "x-api-key"].iter().any(|name| {
            headers
                .get(*name)
                .is_some_and(|value| value.to_str().is_err())
        }) {
            return Err(StoreError::Unauthorized("Invalid authentication header"));
        }
        Ok(auth::request(peer, headers))
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn authenticate_optional_bearer(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<(), StoreError> {
        self.authenticate_bearer(&Self::checked_bearer_request(peer, headers)?)
            .await?;
        Ok(())
    }

    pub async fn bearer_account_user(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        admin: bool,
    ) -> Result<RetainedRestSubject, StoreError> {
        let request = Self::checked_bearer_request(peer, headers)?;
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

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn core_settings_read(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        detail: Option<String>,
    ) -> Result<Value, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        if subject.user().user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        let id: Option<i64> = detail
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|_| StoreError::StatusBody(404, json!({"detail": "Not found."})))?;
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let results = core_settings_rows(&mut connection, id).await;
            settle(connection).await?;
            results
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn core_catalog_read(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        catalog: crate::auth::sqlite_runtime::core_catalog::Catalog,
    ) -> Result<Value, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        if subject.user().user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let results = catalog.read(&mut connection).await;
            settle(connection).await?;
            results
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn channel_group_action(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        action: crate::channel_groups::sqlite::writes::Action,
    ) -> Result<axum::response::Response, StoreError> {
        use crate::channel_groups::sqlite::writes::{run, Action};
        let admin = !matches!(&action, Action::Detail(..));
        let subject = self.bearer_account_user(peer, headers, admin).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = run(&mut connection, subject.user(), action).await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn channel_profile_collection(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<Value, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let pool = self.pool.clone();
        let work_bytes = self.limits.work_bytes;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result =
                collect_channel_profiles(&mut connection, subject.user(), work_bytes).await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn channel_streams(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        uri: &axum::http::Uri,
        params: HashMap<String, String>,
        projection: crate::auth::sqlite_runtime::streams::Projection,
    ) -> Result<Vec<u8>, StoreError> {
        let subject = self
            .bearer_account_user(peer, headers.clone(), false)
            .await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let base = crate::channels::base_url(&headers, uri)?;
        let admin = subject.user().user_level >= 10;
        let pool = self.pool.clone();
        let work_bytes = self.limits.work_bytes;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = projection
                .read(&mut connection, admin, &params, &base, work_bytes)
                .await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn channel_number_range(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        uri: &axum::http::Uri,
    ) -> Result<Value, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let Some((start, end)) = crate::channels::parse_number_range(uri)? else {
            return Ok(json!({"occupants": []}));
        };
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = crate::channels::sqlite_list::read_number_range(
                &mut connection,
                subject.user(),
                start,
                end,
            )
            .await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn channel_read(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        uri: &axum::http::Uri,
        params: HashMap<String, String>,
        operation: ChannelRead,
    ) -> Result<Value, StoreError> {
        let subject = self
            .bearer_account_user(peer, headers.clone(), false)
            .await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let detail = match &operation {
            ChannelRead::Detail(id) => Some(
                id.parse()
                    .map_err(|_| StoreError::StatusBody(404, json!({"detail":"Not found."})))?,
            ),
            _ => None,
        };
        let base = crate::channels::base_url(&headers, uri)?;
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = async {
                let mut transaction = connection.begin().await?;
                let read = match operation {
                    ChannelRead::Projection(projection) => {
                        crate::channels::sqlite_list::read_projection(
                            &mut transaction,
                            subject.user(),
                            &params,
                            projection,
                        )
                        .await
                    }
                    _ => {
                        crate::channels::sqlite_list::read(
                            &mut transaction,
                            subject.user(),
                            &params,
                            &base,
                            detail,
                        )
                        .await
                    }
                };
                if read.is_ok() {
                    transaction.commit().await?;
                } else {
                    transaction.rollback().await?;
                }
                read
            }
            .await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn epg_collection(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        collection: crate::auth::sqlite_runtime::epg_collections::Collection,
    ) -> Result<Vec<u8>, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let pool = self.pool.clone();
        let work_bytes = self.limits.work_bytes;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = collection.read(&mut connection, work_bytes).await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn provider_accounts(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<Vec<u8>, StoreError> {
        let subject = self.bearer_account_user(peer, headers, false).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let admin = subject.user().user_level >= 10;
        let pool = self.pool.clone();
        let work_bytes = self.limits.work_bytes;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result =
                crate::auth::sqlite_runtime::m3u_accounts::read(&mut connection, admin, work_bytes)
                    .await;
            settle(connection).await?;
            result
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn channel_logo_collection(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        uri: &axum::http::Uri,
    ) -> Result<Vec<u8>, StoreError> {
        use crate::auth::sqlite_runtime::channel_logos;
        let subject = self
            .bearer_account_user(peer, headers.clone(), false)
            .await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let params = channel_logos::params(uri)?;
        let base = crate::channels::base_url(&headers, uri)?;
        let origin = base.origin().ascii_serialization();
        let pool = self.pool.clone();
        let work_bytes = self.limits.work_bytes;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let selected = channel_logos::select(&mut connection, &params, &base, work_bytes).await;
            settle(connection).await?;
            let selected = selected?;
            tokio::task::spawn_blocking(move || {
                channel_logos::document(selected, &origin, work_bytes)
            })
            .await
            .map_err(|_| database_unavailable())?
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn notification_collection(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        include_dismissed: bool,
    ) -> Result<Vec<u8>, StoreError> {
        use crate::notifications::sqlite;
        let actor = self.notification_actor(peer, headers).await?;
        let rows = self.notification_rows(actor.user()).await?;
        sqlite::render(
            rows,
            include_dismissed,
            self.limits.work_bytes,
            |id, conditions| self.notification_visible(actor.user().id, id, conditions),
        )
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn notification_count(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<usize, StoreError> {
        let actor = self.notification_actor(peer, headers).await?;
        let rows = self.notification_rows(actor.user()).await?;
        crate::notifications::sqlite::unread_count(rows, |id, conditions| {
            self.notification_visible(actor.user().id, id, conditions)
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    async fn notification_rows(&self, actor: &User) -> Result<Vec<Value>, StoreError> {
        let pool = self.pool.clone();
        let actor_id = actor.id;
        let admin = actor.user_level >= 10;
        let limits = self.limits;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let rows = crate::notifications::sqlite::read(
                &mut connection,
                actor_id,
                admin,
                chrono::Utc::now(),
                limits,
            )
            .await;
            settle(connection).await?;
            rows
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    async fn notification_visible(
        &self,
        actor_id: i64,
        id: i64,
        conditions: Value,
    ) -> Result<bool, StoreError> {
        let cache = self.cache.as_ref().ok_or(StoreError::Unsupported(
            "Developer notification conditions require the original shared Redis cache.",
        ))?;
        crate::notifications::sqlite::developer_visible(cache, actor_id, id, &conditions, || {
            self.network_group()
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    async fn notification_actor(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<RetainedRestSubject, StoreError> {
        // Personal actions use Original IsAuthenticated without UI network admission.
        self.authenticate_bearer(&Self::checked_bearer_request(peer, headers)?)
            .await?
            .ok_or(StoreError::Unauthorized(
                "Authentication credentials were not provided.",
            ))
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn dismiss_notification(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        raw: String,
        body: axum::body::Bytes,
    ) -> Result<Value, StoreError> {
        use crate::notifications::{self, sqlite};

        let actor = self.notification_actor(peer, headers.clone()).await?;
        let id = notifications::key(raw)?;
        let limits = self.limits;
        let pool = self.pool.clone();
        let subject = actor.user().clone();
        let target = supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let target =
                sqlite::dismiss_target(&mut connection, id, &subject, chrono::Utc::now(), limits)
                    .await;
            settle(connection).await?;
            target
        })
        .await?;
        if let Some(conditions) = target.conditions()? {
            let cache = self.cache.as_ref().ok_or(StoreError::Unsupported(
                "Developer notification conditions require the original shared Redis cache.",
            ))?;
            if !sqlite::developer_visible(cache, actor.user().id, id, &conditions, || {
                self.network_group()
            })
            .await?
            {
                return Err(notifications::missing());
            }
        }

        let pool = self.pool.clone();
        let subject = actor.user().clone();
        supervised_sql_read(self.permit.clone(), async move {
            sqlite::dismiss(&pool, id, &subject, target, limits, &headers, &body).await
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn dismiss_all_notifications(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
    ) -> Result<Value, StoreError> {
        use crate::notifications::sqlite;

        let actor = self.notification_actor(peer, headers).await?;
        let rows = self.notification_rows(actor.user()).await?;
        let targets = sqlite::visible_rows(rows, false, |id, conditions| {
            self.notification_visible(actor.user().id, id, conditions)
        })
        .await?;
        let limits = self.limits;
        let pool = self.pool.clone();
        let subject = actor.user().clone();
        supervised_sql_read(self.permit.clone(), async move {
            sqlite::dismiss_all(&pool, &subject, targets, limits).await
        })
        .await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn stream_filter_options(
        &self,
        peer: SocketAddr,
        headers: HeaderMap,
        raw_query: Option<&str>,
    ) -> Result<Vec<u8>, StoreError> {
        use crate::auth::sqlite_runtime::stream_filter_options;
        let subject = self.bearer_account_user(peer, headers, false).await?;
        crate::channel_groups::collection_permission(subject.user())?;
        let filter = stream_filter_options::filter(raw_query)?;
        let pool = self.pool.clone();
        let limits = self.limits;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = stream_filter_options::read(&mut connection, limits, &filter).await;
            settle(connection).await?;
            result
        })
        .await
    }

    pub(crate) async fn authenticate_bearer(
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
        self.group_value("network_access").await
    }

    async fn group_value(&self, group: &'static str) -> Result<Value, StoreError> {
        let pool = self.pool.clone();
        let limits = self.limits;
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = sqlx::query(SQLITE_GROUP_SQL)
                .bind(group)
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
    pub(crate) async fn environment_system_settings(&self) -> Result<Value, StoreError> {
        let load = || async {
            let value = self.group_value("system_settings").await?;
            if value.as_object().is_some_and(|map| !map.is_empty()) {
                Ok(value)
            } else {
                Ok(crate::source_system_settings_defaults())
            }
        };
        match &self.cache {
            Some(cache) => cache.settings_group_bounded("system_settings", load).await,
            None => load().await,
        }
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) fn environment_request_cache(&self) -> Option<&NotificationCache> {
        self.cache.as_ref()
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn preview_viewer(
        &self,
        request: &PlaybackRequest,
    ) -> Result<RetainedRestSubject, StoreError> {
        let subject = self
            .authenticate_bearer(request)
            .await?
            .ok_or(StoreError::Unauthorized(
                "Authentication credentials were not provided.",
            ))?;
        let ip = auth::source_client_ip_for_config(&self.config, request).address();
        let cache = self.cache.as_ref().ok_or_else(capacity)?;
        let policy = cache
            .settings_group_bounded("network_access", || self.network_group())
            .await?;
        if !crate::xc_auth::system_allowed(&policy, "STREAMS", ip)? {
            return Err(StoreError::Forbidden);
        }
        if subject.user().user_level < 1 {
            return Err(StoreError::PermissionDenied);
        }
        Ok(subject)
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn preview_setting(&self, key: &'static str) -> Result<Value, StoreError> {
        self.group_value(key).await
    }

    #[cfg(feature = "sqlite-copied-data-runtime")]
    pub(crate) async fn preview_sql<T, F>(&self, run: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: for<'c> FnOnce(
                &'c mut sqlx::SqliteConnection,
            )
                -> dispatcharr_contracts::BoxFuture<'c, Result<T, StoreError>>
            + Send
            + 'static,
    {
        let pool = self.pool.clone();
        supervised_sql_read(self.permit.clone(), async move {
            let mut connection = pool.acquire().await?;
            let result = run(&mut connection).await;
            settle(connection).await?;
            result
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

#[cfg(feature = "sqlite-copied-data-runtime")]
async fn collect_channel_profiles(
    connection: &mut sqlx::SqliteConnection,
    user: &User,
    mut remaining: usize,
) -> Result<Value, StoreError> {
    let mut rows = sqlx::query_as::<_, (i64, String, Option<i64>)>(CHANNEL_PROFILE_COLLECTION_SQL)
        .bind(user.user_level == 10)
        .bind(user.id)
        .fetch(connection);
    let mut profiles = Vec::new();
    let mut current: Option<(i64, String, Vec<i64>)> = None;
    while let Some((id, name, channel)) = rows.try_next().await? {
        if current
            .as_ref()
            .is_none_or(|(previous, ..)| *previous != id)
        {
            remaining = remaining
                .checked_sub(2 * name.len() + 3 * std::mem::size_of::<Value>())
                .ok_or_else(capacity)?;
            if let Some((id, name, channels)) = current.take() {
                profiles.push(json!({"id":id,"name":name,"channels":channels}));
            }
            current = Some((id, name, Vec::new()));
        }
        if let Some(channel) = channel {
            remaining = remaining
                .checked_sub(std::mem::size_of::<Value>())
                .ok_or_else(capacity)?;
            if let Some((_, _, channels)) = current.as_mut() {
                channels.push(channel);
            }
        }
    }
    if let Some((id, name, channels)) = current {
        profiles.push(json!({"id":id,"name":name,"channels":channels}));
    }
    Ok(Value::Array(profiles))
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

#[cfg(all(test, feature = "sqlite-copied-data-runtime"))]
mod channel_profile_collection_tests {
    use super::*;
    use sqlx::SqliteConnection;

    fn user(id: i64, user_level: i32) -> crate::User {
        crate::User {
            id,
            username: String::new(),
            password: String::new(),
            is_active: true,
            user_level,
            stream_limit: 0,
            custom_properties: json!({}),
        }
    }

    #[tokio::test]
    async fn copied_channel_profiles_filter_assignments_and_preserve_enabled_membership_order() {
        let mut connection = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE dispatcharr_channels_channelprofile (id INTEGER PRIMARY KEY,name TEXT);
            CREATE TABLE dispatcharr_channels_channelprofilemembership
                (id INTEGER PRIMARY KEY,channel_profile_id INTEGER,channel_id INTEGER,enabled INTEGER);
            CREATE TABLE accounts_user_channel_profiles (user_id INTEGER,channelprofile_id INTEGER);
            INSERT INTO dispatcharr_channels_channelprofile VALUES (7,'Assigned'),(12,'Other'),(20,'Empty');
            INSERT INTO dispatcharr_channels_channelprofilemembership VALUES
                (1,7,42,1),(2,7,11,0),(3,7,18,1),(4,12,99,1);
            INSERT INTO accounts_user_channel_profiles VALUES (1,20),(1,7);",
        )
        .execute(&mut connection)
        .await
        .unwrap();
        let assigned = json!({"id":7,"name":"Assigned","channels":[42,18]});
        let empty = json!({"id":20,"name":"Empty","channels":[]});
        assert_eq!(
            collect_channel_profiles(&mut connection, &user(1, 1), 4096)
                .await
                .unwrap(),
            json!([assigned, empty])
        );
        assert_eq!(
            collect_channel_profiles(&mut connection, &user(2, 10), 4096)
                .await
                .unwrap(),
            json!([assigned, {"id":12,"name":"Other","channels":[99]}, empty])
        );
        assert_eq!(
            collect_channel_profiles(&mut connection, &user(2, 1), 4096)
                .await
                .unwrap(),
            json!([])
        );
        assert!(matches!(
            collect_channel_profiles(&mut connection, &user(1, 1), 1).await,
            Err(StoreError::StatusBody(503, _))
        ));
    }
}

#[cfg(all(test, feature = "sqlite-copied-data-runtime"))]
#[path = "sqlite_single_from_stream_tests.rs"]
mod single_from_stream_tests;
