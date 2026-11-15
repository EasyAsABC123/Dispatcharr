//! Lazy Python DBAPI callback on an ACTUAL already-owned TaskSqlSession.
//! No Django/libpq pool/SQL connection is opened by the compatibility child.
//! Original Dispatcharr/SQLx/psycopg authors retain all credit.
use crate::{StoreError, TaskSqlSession};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
#[derive(Clone)]
pub struct PythonSqlSession {
    session: TaskSqlSession,
}
impl PythonSqlSession {
    /// Caller supplies the exact session already admitted from the same Auth4
    /// and pool8. Closing it remains the owning operation's responsibility.
    pub fn new(session: TaskSqlSession) -> Self {
        Self { session }
    }
    pub async fn info(&self) -> Result<Value, StoreError> {
        self.session.run(|connection|Box::pin(async move{
            let(status,version,parameters)=connection.python_connection_info();
            json!({"transaction_status":status,"server_version":version,"parameters":parameters})
        })).await
    }
    pub async fn query(&self, sql: String) -> Result<Value, StoreError> {
        // Only actual explicit transaction termination bypasses a causal writer
        // loan. This releases the parent's own locks; it never adds a hidden
        // commit/rollback or permits concurrent parent business statements.
        let control = matches!(
            sql.trim()
                .trim_end_matches(';')
                .trim()
                .to_ascii_uppercase()
                .as_str(),
            "COMMIT" | "ROLLBACK"
        );
        if control {
            self.session
                .python_transaction_control(move |connection| {
                    Box::pin(Self::query_connection(connection, sql))
                })
                .await?
        } else {
            self.session
                .run(move |connection| Box::pin(Self::query_connection(connection, sql)))
                .await?
        }
    }
    async fn query_connection(
        connection: &mut sqlx::PgConnection,
        sql: String,
    ) -> Result<Value, StoreError> {
        let result = connection
            .python_simple_query(&sql, 4096, 2 * 1024 * 1024)
            .await?;
        let statements=result.statements.into_iter().map(|statement|{
                let columns=statement.columns.into_iter().map(|column|json!({"name":column.name,"oid":column.oid,"table_oid":column.table_oid,"table_column":column.table_column,"type_size":column.type_size,"type_modifier":column.type_modifier,"format":column.format})).collect::<Vec<_>>();
                let rows=statement.rows.into_iter().map(|row|row.into_iter().map(|v|v.map(|v|STANDARD.encode(v))).collect::<Vec<_>>()).collect::<Vec<_>>();
                json!({"columns":columns,"rows":rows,"command_tag":STANDARD.encode(statement.command_tag),"rows_affected":statement.rows_affected})
            }).collect::<Vec<_>>();
        let error = match result.server_error {
            None => Value::Null,
            Some(sqlx::Error::Database(error)) => {
                let pg = error
                    .try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                    .ok_or(StoreError::Unsupported(
                        "Python SQL bridge requires actual PostgreSQL ErrorResponse",
                    ))?;
                // libpq DiagnosticField ASCII codes, populated ONLY from
                // actual server diagnostics. psycopg constructs its own
                // real SQLSTATE subclass and diag object on the child side.
                let fields = pg
                    .python_diagnostic_fields()
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), Value::String(STANDARD.encode(value))))
                    .collect::<serde_json::Map<_, _>>();
                json!({"diagnostics":fields})
            }
            Some(error) => return Err(StoreError::Database(error)),
        };
        Ok::<_, StoreError>(
            json!({"statements":statements,"error":error,"transaction_status":result.transaction_status,"server_version":result.server_version_num,"parameters":result.parameters}),
        )
    }
}
