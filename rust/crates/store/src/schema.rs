//! Fresh-only original PostgreSQL schema/default bootstrap, without Python.
//! All original Dispatcharr application credit remains with its contributors.
use sqlx::{Connection, PgConnection};

const SCHEMA: &str = include_str!("../schema/original-1241b3-schema.sql");
const DEFAULTS: &str = include_str!("../schema/original-1241b3-defaults.sql");
const FRESH_TIMESTAMPS: &str = include_str!("../schema/original-1241b3-fresh-timestamps.sql");
const INIT_LOCK: i64 = 0x4469737061746368;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaInitialization {
    Created {
        tables: i64,
        migrations: i64,
    },
    /// Presence is deliberately not a claim that an arbitrary legacy schema is
    /// compatible. Existing relations are never altered by this initializer.
    SkippedExisting {
        objects: i64,
    },
}
/// Explicitly requested initialization of an empty database. The source-pinned
/// schema, defaults and sequence positions execute in one fenced transaction.
/// A private connection prevents pg_dump session settings leaking into a pool.
pub async fn initialize_empty_database(url: &str) -> Result<SchemaInitialization, sqlx::Error> {
    initialize_queries(url, SCHEMA, DEFAULTS, FRESH_TIMESTAMPS).await
}
async fn initialize_queries(
    url: &str,
    schema: &str,
    defaults: &str,
    timestamps: &str,
) -> Result<SchemaInitialization, sqlx::Error> {
    let mut connection = PgConnection::connect(url).await?;
    let mut transaction = connection.begin().await?;
    sqlx::query("SET LOCAL statement_timeout='180s'")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(INIT_LOCK)
        .execute(&mut *transaction)
        .await?;
    let existing:i64=sqlx::query_scalar("SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE left(n.nspname,3)<>'pg_' AND n.nspname<>'information_schema' AND c.relkind IN('r','p','v','m','S','f','c'))+(SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE left(n.nspname,3)<>'pg_' AND n.nspname<>'information_schema')+(SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE left(n.nspname,3)<>'pg_' AND n.nspname<>'information_schema' AND t.typtype IN('d','e'))").fetch_one(&mut *transaction).await?;
    if existing > 0 {
        transaction.rollback().await?;
        connection.close().await?;
        return Ok(SchemaInitialization::SkippedExisting { objects: existing });
    }
    sqlx::raw_sql(schema).execute(&mut *transaction).await?;
    sqlx::raw_sql(defaults).execute(&mut *transaction).await?;
    // Creation bookkeeping is fresh, while explicit expiry/scheduling dates
    // remain source data. Applied migration timestamps are not copied as old.
    sqlx::raw_sql(timestamps).execute(&mut *transaction).await?;
    let tables: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
            .fetch_one(&mut *transaction)
            .await?;
    let migrations: i64 = sqlx::query_scalar("SELECT count(*) FROM public.django_migrations")
        .fetch_one(&mut *transaction)
        .await?;
    if tables != 62 || migrations != 184 {
        return Err(sqlx::Error::Protocol(
            "Original bootstrap schema inventory validation failed".into(),
        ));
    }
    transaction.commit().await?;
    connection.close().await?;
    Ok(SchemaInitialization::Created { tables, migrations })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    #[ignore = "requires owned empty DATABASE_FAILURE_URL plus SCHEMA_DISPOSABLE_FIXTURE"]
    async fn default_failure_after_all_ddl_rolls_back_and_retry_initializes() {
        assert_eq!(
            std::env::var("SCHEMA_DISPOSABLE_FIXTURE").as_deref(),
            Ok("playback-fixture-only")
        );
        let url = std::env::var("DATABASE_FAILURE_URL").unwrap();
        assert!(
            initialize_queries(&url, SCHEMA, "SELECT 1/0", FRESH_TIMESTAMPS)
                .await
                .is_err()
        );
        let mut connection = PgConnection::connect(&url).await.unwrap();
        let absent: bool = sqlx::query_scalar("SELECT to_regclass('public.accounts_user') IS NULL")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert!(absent);
        let absent: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_trgm')",
        )
        .fetch_one(&mut connection)
        .await
        .unwrap();
        assert!(absent);
        connection.close().await.unwrap();
        assert_eq!(
            initialize_empty_database(&url).await.unwrap(),
            SchemaInitialization::Created {
                tables: 62,
                migrations: 184
            }
        );
    }
}
