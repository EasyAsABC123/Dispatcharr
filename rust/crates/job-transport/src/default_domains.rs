//! Bounded inert default-task library. Original Dispatcharr/Celery authors retain credit.
//! No Main registration or automatic intake; unsupported domains remain explicit.
use crate::default_protocol::{Kind, Normal};
use dispatcharr_contracts::BoxFuture;
use sqlx::PgPool;
use std::sync::Arc;
#[derive(Debug)]
pub enum Error {
    Unsupported,
    Sql(sqlx::Error),
    Panic,
    Event,
}
pub trait Domains: Send + Sync {
    fn preflight(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Error>>;
    fn execute(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Normal, Error>>;
}
#[derive(Clone)]
pub struct KnownDomains {
    pool: PgPool,
    active: Option<crate::active_notifications::Service>,
    sink: Option<Arc<dyn dispatcharr_contracts::UpdateEventSink>>,
}
impl KnownDomains {
    /// Constructor for explicitly assembled, inert default task services.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            sink: None,
            active: None,
        }
    }
    /// Use the existing application Channels sink; this starts no worker.
    pub fn with_notifications(
        pool: PgPool,
        sink: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    ) -> Self {
        Self {
            pool,
            sink: Some(sink),
            active: None,
        }
    }
    /// Explicit bounded active evaluation. No intake starts; multiple eligible
    /// profiles remain unsupported until task iteration ordering is qualified.
    pub fn with_active_notifications(
        pool: PgPool,
        sink: Arc<dyn dispatcharr_contracts::UpdateEventSink>,
    ) -> Self {
        let active = Some(crate::active_notifications::Service::new(
            pool.clone(),
            sink.clone(),
            Arc::new(crate::active_notifications::UtcNotificationClock),
        ));
        Self {
            pool,
            sink: Some(sink),
            active,
        }
    }
    pub fn with_notification_clock(
        mut self,
        clock: Arc<dyn crate::active_notifications::NotificationClock>,
    ) -> Self {
        self.active = self.active.map(|service| service.with_clock(clock));
        self
    }
    pub fn copied_fixture(pool: PgPool, database: &str) -> Result<Self, Error> {
        let options = pool.connect_options();
        if std::env::var("JOB_DISPOSABLE_FIXTURE").as_deref() != Ok("playback-fixture-only")
            || std::env::var("POSTGRES_DB").as_deref() != Ok(database)
            || !database.starts_with("rust_probe_backup_jobs_")
            || database.len() > 63
            || !database
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
            || options.get_database() != Some(database)
            || !matches!(
                options.get_host(),
                "localhost" | "127.0.0.1" | "::1" | "/var/run/postgresql"
            )
        {
            return Err(Error::Unsupported);
        }
        Ok(Self {
            pool,
            sink: None,
            active: None,
        })
    }
    async fn inspect(&self, kind: Kind, owner: Arc<dyn Send + Sync>) -> Result<(), Error> {
        match kind {
            // Pinned RedisBackend.cleanup inherits the empty BaseBackend method.
            // Existing backend result keys/TTLs are never scanned or changed.
            Kind::RedisBackendCleanup => Ok(()),
            Kind::InactiveAccountNotifications => {
                if self.sink.is_none() {
                    return Err(Error::Unsupported);
                }
                if let Some(active) = &self.active {
                    active.preflight(owner).await
                } else {
                    crate::inactive_notifications::preflight(self.pool.clone(), owner).await
                }
            }
            Kind::RecurringMaintenance => {
                let pool = self.pool.clone();
                tokio::spawn(async move {
                    let _owner=owner;
                    let mut connection=pool.acquire().await.map_err(Error::Sql)?;
                    // Source loops enabled rules. Empty EXISTS is the exact
                    // disabled-only domain; do not materialize unknown rows.
                    let observed=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_recurringrecordingrule WHERE enabled)").fetch_one(&mut *connection).await;
                    // Explicit close settles physical reply/cleanup before task
                    // admission can disappear, including server-error paths.
                    let closed=connection.close().await;
                    let active=observed.map_err(Error::Sql)?;
                    closed.map_err(Error::Sql)?;
                    if active {Err(Error::Unsupported)}else{Ok(())}
                }).await.map_err(|_|Error::Panic)?
            }
        }
    }
}
impl Domains for KnownDomains {
    fn preflight(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(self.inspect(kind, owner))
    }
    fn execute(
        &self,
        kind: Kind,
        owner: Arc<dyn Send + Sync>,
    ) -> BoxFuture<'_, Result<Normal, Error>> {
        Box::pin(async move {
            if kind == Kind::InactiveAccountNotifications {
                let sink = self.sink.clone().ok_or(Error::Unsupported)?;
                if let Some(active) = &self.active {
                    active.execute(owner).await?;
                } else {
                    crate::inactive_notifications::execute(self.pool.clone(), sink, owner).await?;
                }
                return Ok(Normal::Null);
            }
            self.inspect(kind, owner).await?;
            Ok(match kind {
                Kind::RedisBackendCleanup | Kind::InactiveAccountNotifications => Normal::Null,
                Kind::RecurringMaintenance => Normal::Zero,
            })
        })
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported => f.write_str("Unsupported"),
            Self::Event => f.write_str("Event"),
            Self::Panic => f.write_str("Panic"),
            Self::Sql(error) => write!(
                f,
                "SQL({})",
                error
                    .as_database_error()
                    .and_then(|v| v.code())
                    .as_deref()
                    .unwrap_or("operational")
            ),
        }
    }
}
