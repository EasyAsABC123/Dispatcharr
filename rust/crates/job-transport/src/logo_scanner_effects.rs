//! Unregistered concrete ports for the private logo-only scanner draft.
//! Not compiled or qualified. Original authors retain behavior/code credit.
use super::logo_scanner::{Effects, Error, FileEffect, Receipt};
use dispatcharr_contracts::{UpdateEventError, UpdateEventSink};
use dispatcharr_output_store::transport::{ClassifiedError, RedisTransport};
use serde_json::Value;
use sqlx::{Connection, PgConnection, PgPool};
use std::{future::Future, pin::Pin, sync::Arc};
type Answer<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;
/// Normal inert constructor: supplied pool/physical Redis transport/event sink.
/// This object starts no intake and grants no task publication authority.
pub struct RetainedEffects {
    pool: PgPool,
    cache: RedisTransport,
    sink: Arc<dyn UpdateEventSink>,
}
impl RetainedEffects {
    pub fn new(pool: PgPool, cache: RedisTransport, sink: Arc<dyn UpdateEventSink>) -> Self {
        Self { pool, cache, sink }
    }
    async fn connection(&self) -> Result<PgConnection, Error> {
        self.pool
            .acquire()
            .await
            .map(|c| c.detach())
            .map_err(|_| Error::Operational)
    }
    async fn close(connection: PgConnection) -> Result<(), Error> {
        connection.close().await.map_err(|_| Error::Operational)
    }
    fn server_error(error: &sqlx::Error) -> bool {
        // A physically replied PostgreSQL ERROR only; IO, pool/admission, protocol,
        // FATAL/PANIC and rollback/close uncertainty never become caught authority.
        error
            .as_database_error()
            .and_then(|e| e.try_downcast_ref::<sqlx::postgres::PgDatabaseError>())
            .is_some_and(|e| e.severity() == sqlx::postgres::PgSeverity::Error)
    }
}
impl Effects for RetainedEffects {
    fn no_index_sources<'a>(&'a self, receipt: Arc<Receipt>) -> Answer<'a, bool> {
        Box::pin(async move {
            let _receipt = receipt;
            let mut connection = self.connection().await?;
            let result=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM epg_epgsource s LEFT JOIN epg_epgsourceindex i ON i.source_id=s.id WHERE s.is_active AND s.source_type NOT IN ('dummy','schedules_direct') AND (i.source_id IS NULL OR i.data IS NULL))").fetch_one(&mut connection).await;
            Self::close(connection).await?;
            result.map(|exists| !exists).map_err(|_| Error::Operational)
        })
    }
    fn marker<'a>(&'a self, path: &'a str, receipt: Arc<Receipt>) -> Answer<'a, Option<String>> {
        Box::pin(async move {
            let key = format!("processed_file:{path}");
            // Bound before transferring the raw value. Plain Source GET differs in Lua
            // ACL/error framing; no terminal/cache permission parity is inferred here.
            let result=self.cache.command_classified::<Option<String>,_>("local n=redis.call('STRLEN',KEYS[1]); if n>64 then return redis.error_reply('OWNED_MARKER_OVERSIZE') end; return redis.call('GET',KEYS[1])",&[key],&[],receipt).await;
            result
                .map(|(value, _)| value)
                .map_err(|_| Error::Operational)
        })
    }
    fn exists<'a>(&'a self, path: &'a str, receipt: Arc<Receipt>) -> Answer<'a, bool> {
        Box::pin(async move {
            let _receipt = receipt;
            let mut connection = self.connection().await?;
            let result = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM dispatcharr_channels_logo WHERE url=$1)",
            )
            .bind(path)
            .fetch_one(&mut connection)
            .await;
            Self::close(connection).await?;
            result.map_err(|_| Error::Operational)
        })
    }
    fn get_or_create<'a>(
        &'a self,
        path: &'a str,
        name: &'a str,
        receipt: Arc<Receipt>,
    ) -> Answer<'a, FileEffect<()>> {
        Box::pin(async move {
            let _receipt = receipt;
            let mut connection = self.connection().await?;
            let found = sqlx::query_scalar::<_, i64>(
                "SELECT id::bigint FROM dispatcharr_channels_logo WHERE url=$1 LIMIT 2",
            )
            .bind(path)
            .fetch_all(&mut connection)
            .await;
            let found = match found {
                Ok(rows) => rows,
                Err(error) => {
                    let caught = Self::server_error(&error);
                    Self::close(connection).await?;
                    return if caught {
                        Ok(FileEffect::CaughtServerError)
                    } else {
                        Err(Error::Operational)
                    };
                }
            };
            if found.len() > 1 {
                Self::close(connection).await?;
                return Err(Error::Unsupported);
            }
            if !found.is_empty() {
                Self::close(connection).await?;
                return Ok(FileEffect::Settled(()));
            }
            let result = async {
                let mut transaction = connection.begin().await?;
                let inserted =
                    sqlx::query("INSERT INTO dispatcharr_channels_logo(name,url) VALUES($1,$2)")
                        .bind(name)
                        .bind(path)
                        .execute(&mut *transaction)
                        .await;
                match inserted {
                    Ok(_) => {
                        transaction.commit().await?;
                        Ok(())
                    }
                    Err(error) => {
                        // Rollback must succeed before a known server failure is considered caught.
                        if transaction.rollback().await.is_err() {
                            return Err(sqlx::Error::Protocol(
                                "owned logo rollback unsettled".into(),
                            ));
                        }
                        Err(error)
                    }
                }
            }
            .await;
            let caught = result.as_ref().err().is_some_and(Self::server_error);
            Self::close(connection).await?;
            match result {
                Ok(()) => Ok(FileEffect::Settled(())),
                Err(_) if caught => Ok(FileEffect::CaughtServerError),
                Err(_) => Err(Error::Operational),
            }
        })
    }
    fn set_marker<'a>(
        &'a self,
        path: &'a str,
        value: &'a str,
        receipt: Arc<Receipt>,
    ) -> Answer<'a, FileEffect<()>> {
        Box::pin(async move {
            if value.len() > 64 {
                return Err(Error::Unsupported);
            }
            let key = format!("processed_file:{path}");
            let result = self
                .cache
                .command_classified::<String, _>(
                    "return redis.call('SET',KEYS[1],ARGV[1],'EX',259200)",
                    &[key],
                    &[value.as_bytes()],
                    receipt,
                )
                .await;
            match result {
                Ok((_, _)) => Ok(FileEffect::Settled(())),
                Err(ClassifiedError::Server) => Ok(FileEffect::CaughtServerError),
                Err(_) => Err(Error::Operational),
            }
        })
    }
    fn summary<'a>(&'a self, event: Value, receipt: Arc<Receipt>) -> Answer<'a, FileEffect<()>> {
        Box::pin(async move {
            // Outer scanner supervisor waits for this exact receipt after future return.
            // Production sink must retain it through physical Channels publication.
            match self.sink.send_retained("updates", event, receipt).await {
                Ok(()) => Ok(FileEffect::Settled(())),
                Err(UpdateEventError::Unavailable) => Ok(FileEffect::CaughtServerError),
                Err(_) => Err(Error::Operational),
            }
        })
    }
}
