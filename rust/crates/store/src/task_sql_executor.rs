//! Existing pooled mode or the one physically owned task connection.
use crate::TaskSqlSession;
use futures_util::{stream::BoxStream, TryStreamExt};
use sqlx::{
    postgres::{PgArguments, PgQueryResult, PgRow, PgStatement, PgTypeInfo},
    Describe, Either, Execute, Executor, PgConnection, PgPool, Postgres, Row, ValueRef,
};
use std::{fmt, future::Future, pin::Pin};
type FutureResult<'a, T> = Pin<Box<dyn Future<Output = Result<T, sqlx::Error>> + Send + 'a>>;
pub enum TaskSqlExecutor<'a> {
    Pool(&'a PgPool),
    Session(TaskSqlSession),
    Unavailable,
}
impl fmt::Debug for TaskSqlExecutor<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TaskSqlExecutor")
    }
}
fn lost(_: crate::StoreError) -> sqlx::Error {
    sqlx::Error::Protocol("Owned task SQL physical session failed".into())
}
fn bounded(row: &PgRow) -> Result<usize, sqlx::Error> {
    if row.len() > 256 {
        return Err(sqlx::Error::Protocol(
            "Owned task SQL column capacity".into(),
        ));
    }
    let mut bytes = 0usize;
    for index in 0..row.len() {
        let value = row.try_get_raw(index)?;
        if !value.is_null() {
            let n = value.as_bytes().map_err(sqlx::Error::Decode)?.len();
            if n > 65536 {
                return Err(sqlx::Error::Protocol("Owned task SQL cell capacity".into()));
            }
            bytes = bytes
                .checked_add(n)
                .filter(|v| *v <= 1048576)
                .ok_or_else(|| sqlx::Error::Protocol("Owned task SQL row capacity".into()))?;
        }
    }
    Ok(bytes)
}
async fn collect(
    connection: &mut PgConnection,
    sql: String,
    args: Option<PgArguments>,
) -> Result<Vec<Either<PgQueryResult, PgRow>>, sqlx::Error> {
    let mut stream: BoxStream<'_, _> = match args {
        Some(args) => (&mut *connection).fetch_many(sqlx::query_with(&sql, args)),
        None => (&mut *connection).fetch_many(sql.as_str()),
    };
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    while let Some(row) = stream.try_next().await? {
        if rows.len() >= 10000 {
            return Err(sqlx::Error::Protocol(
                "Owned task SQL result capacity".into(),
            ));
        }
        if let Either::Right(ref row) = row {
            bytes = bytes
                .checked_add(bounded(row)?)
                .filter(|v| *v <= 4 * 1024 * 1024)
                .ok_or_else(|| sqlx::Error::Protocol("Owned task SQL transfer capacity".into()))?;
        }
        rows.push(row);
    }
    Ok(rows)
}
impl<'c> Executor<'c> for TaskSqlExecutor<'c> {
    type Database = Postgres;
    fn fetch_many<'e, 'q: 'e, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            Self::Pool(pool) => pool.fetch_many(query),
            Self::Unavailable => Box::pin(futures_util::stream::once(async {
                Err(sqlx::Error::Protocol(
                    "Owned task SQL session is unavailable".into(),
                ))
            })),
            Self::Session(session) => {
                let sql = query.sql().to_owned();
                let args = query.take_arguments();
                Box::pin(
                    async_stream::try_stream! {let args=args.map_err(sqlx::Error::Encode)?;let result=session.run(move|connection|Box::pin(collect(connection,sql,args))).await.map_err(lost)??;for row in result{yield row;}},
                )
            }
        }
    }
    fn fetch_optional<'e, 'q: 'e, E>(self, mut query: E) -> FutureResult<'e, Option<PgRow>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        match self {
            Self::Pool(pool) => pool.fetch_optional(query),
            Self::Unavailable => Box::pin(async {
                Err(sqlx::Error::Protocol(
                    "Owned task SQL session is unavailable".into(),
                ))
            }),
            Self::Session(session) => {
                let sql = query.sql().to_owned();
                let args = query.take_arguments();
                Box::pin(async move {
                    let args = args.map_err(sqlx::Error::Encode)?;
                    session
                        .run(move |connection| {
                            Box::pin(async move {
                                let row = match args {
                                    Some(args) => {
                                        sqlx::query_with(&sql, args)
                                            .fetch_optional(&mut *connection)
                                            .await?
                                    }
                                    None => (&mut *connection).fetch_optional(sql.as_str()).await?,
                                };
                                if let Some(ref row) = row {
                                    bounded(row)?;
                                }
                                Ok::<_, sqlx::Error>(row)
                            })
                        })
                        .await
                        .map_err(lost)?
                })
            }
        }
    }
    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [PgTypeInfo],
    ) -> FutureResult<'e, PgStatement<'q>>
    where
        'c: 'e,
    {
        match self {
            Self::Pool(pool) => pool.prepare_with(sql, parameters),
            Self::Unavailable => Box::pin(async {
                Err(sqlx::Error::Protocol(
                    "Owned task SQL session is unavailable".into(),
                ))
            }),
            Self::Session(session) => {
                let sql = sql.to_owned();
                let parameters = parameters.to_vec();
                Box::pin(async move {
                    session
                        .run(move |connection| {
                            Box::pin(async move {
                                let statement =
                                    (&mut *connection).prepare_with(&sql, &parameters).await?;
                                Ok::<_, sqlx::Error>(sqlx::Statement::to_owned(&statement))
                            })
                        })
                        .await
                        .map_err(lost)?
                })
            }
        }
    }
    fn describe<'e, 'q: 'e>(self, sql: &'q str) -> FutureResult<'e, Describe<Postgres>>
    where
        'c: 'e,
    {
        match self {
            Self::Pool(pool) => pool.describe(sql),
            Self::Unavailable => Box::pin(async {
                Err(sqlx::Error::Protocol(
                    "Owned task SQL session is unavailable".into(),
                ))
            }),
            Self::Session(session) => {
                let sql = sql.to_owned();
                Box::pin(async move {
                    session
                        .run(move |connection| {
                            Box::pin(async move { (&mut *connection).describe(&sql).await })
                        })
                        .await
                        .map_err(lost)?
                })
            }
        }
    }
}
