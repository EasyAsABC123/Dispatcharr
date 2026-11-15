//! Opt-in simple-query port on the SAME retained SQLx PostgreSQL connection.
//! SQLx original MIT/Apache authors retain credit. No connection/transaction/
//! pool is created. Pinned psycopg performs actual client-side adaptation and
//! text/OID decoding; this port preserves server wire fields without guesses.
use super::*;
use crate::message::{CommandComplete, DataRow, RowDescription};
use std::collections::BTreeMap;
#[derive(Debug)]
pub struct PythonColumn {
    pub name: String,
    pub oid: u32,
    pub table_oid: Option<u32>,
    pub table_column: Option<i16>,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}
#[derive(Debug, Default)]
pub struct PythonStatement {
    pub columns: Vec<PythonColumn>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// Actual CommandComplete bytes without the protocol trailing NUL.
    pub command_tag: Vec<u8>,
    pub rows_affected: u64,
}
#[derive(Debug)]
pub struct PythonQueryResult {
    pub statements: Vec<PythonStatement>,
    /// Actual ErrorResponse, received before the final ReadyForQuery. No
    /// invented Python error class, stack, SQLSTATE or exception text.
    pub server_error: Option<Error>,
    pub transaction_status: u8,
    pub server_version_num: Option<u32>,
    pub parameters: BTreeMap<String, String>,
}
impl PgConnection {
    pub fn python_connection_info(&self) -> (u8, Option<u32>, BTreeMap<String, String>) {
        let status = match self.inner.transaction_status {
            TransactionStatus::Idle => b'I',
            TransactionStatus::Transaction => b'T',
            TransactionStatus::Error => b'E',
        };
        (
            status,
            self.server_version_num(),
            self.inner.stream.parameter_statuses.clone(),
        )
    }
    /// Execute actual psycopg-mogrified SQL using PostgreSQL simple query
    /// protocol, preserving multiple statement results and zero-row metadata.
    /// The application must retain its TaskSqlSession owner through this call.
    /// Resource/protocol faults HARD CLOSE the physical socket before returning;
    /// callers cannot reuse a half-consumed result or pretend SQL completed.
    pub async fn python_simple_query(
        &mut self,
        query: &str,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<PythonQueryResult, Error> {
        if query.len() > 1024 * 1024
            || query.contains('\0')
            || max_rows == 0
            || max_rows > 65536
            || !(65536..=16 * 1024 * 1024).contains(&max_bytes)
        {
            return Err(err_protocol!("Python SQL bridge request capacity"));
        }
        self.wait_until_ready().await?;
        self.inner.stream.compatibility_frame_limit = Some(max_bytes);
        let result = self
            .python_simple_query_inner(query, max_rows, max_bytes)
            .await;
        self.inner.stream.compatibility_frame_limit = None;
        if result.is_err() {
            // This endpoint owns its physical future; do not return an open
            // socket with unread rows/COPY/protocol messages to the pool.
            let _ = self.inner.stream.shutdown().await;
        }
        result
    }
    async fn python_simple_query_inner(
        &mut self,
        query: &str,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<PythonQueryResult, Error> {
        self.inner.stream.write_msg(Query(query))?;
        self.inner.pending_ready_for_query_count += 1;
        self.inner.stream.flush().await?;
        let mut completed = Vec::new();
        let mut current = PythonStatement::default();
        let mut bytes = 0usize;
        let mut rows = 0usize;
        let mut server_error = None;
        loop {
            let message = match self.inner.stream.recv().await {
                Ok(message) => message,
                Err(error @ Error::Database(_)) => {
                    server_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            bytes = bytes
                .checked_add(message.contents.len())
                .ok_or_else(|| err_protocol!("Python SQL bridge result capacity"))?;
            if bytes > max_bytes {
                return Err(err_protocol!("Python SQL bridge result capacity"));
            }
            match message.format {
                BackendMessageFormat::RowDescription => {
                    let description: RowDescription = message.decode()?;
                    if description.fields.len() > 4096 {
                        return Err(err_protocol!("Python SQL bridge column capacity"));
                    }
                    current.columns = description
                        .fields
                        .into_iter()
                        .map(|f| PythonColumn {
                            name: f.name,
                            oid: f.data_type_id.0,
                            table_oid: f.relation_id.map(|v| v.0),
                            table_column: f.relation_attribute_no,
                            type_size: f.data_type_size,
                            type_modifier: f.type_modifier,
                            format: f.format,
                        })
                        .collect();
                }
                BackendMessageFormat::DataRow => {
                    let row: DataRow = message.decode()?;
                    rows += 1;
                    if rows > max_rows || row.values.len() != current.columns.len() {
                        return Err(err_protocol!("Python SQL bridge row capacity/description"));
                    }
                    current.rows.push(
                        (0..row.values.len())
                            .map(|i| row.get(i).map(<[u8]>::to_vec))
                            .collect(),
                    );
                }
                BackendMessageFormat::CommandComplete => {
                    let tag = message
                        .contents
                        .strip_suffix(&[0])
                        .ok_or_else(|| err_protocol!("Missing PostgreSQL command terminator"))?
                        .to_vec();
                    let complete: CommandComplete = message.decode()?;
                    current.command_tag = tag;
                    current.rows_affected = complete.rows_affected();
                    if completed.len() >= 128 {
                        return Err(err_protocol!("Python SQL bridge statement capacity"));
                    }
                    completed.push(std::mem::take(&mut current));
                }
                BackendMessageFormat::EmptyQueryResponse => {
                    completed.push(std::mem::take(&mut current));
                }
                BackendMessageFormat::ReadyForQuery => {
                    self.handle_ready_for_query(message)?;
                    let (transaction_status, server_version_num, parameters) =
                        self.python_connection_info();
                    return Ok(PythonQueryResult {
                        statements: completed,
                        server_error,
                        transaction_status,
                        server_version_num,
                        parameters,
                    });
                }
                // Cursor.copy, replication and pipeline protocols are not
                // silently approximated. Hard close above settles their socket.
                _ => {
                    return Err(err_protocol!(
                        "Unsupported PostgreSQL protocol in Python SQL bridge"
                    ))
                }
            }
        }
    }
}
