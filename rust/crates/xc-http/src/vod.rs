//! Private Source Factory38-backed category slice. TCP renderer/route gates pending.
//! Original Dispatcharr authors retain SQL/model/algorithm credit.
use crate::{
    project::JsonBytes,
    reader::{self, ReadLimit},
};
use dispatcharr_store::{Store, StoreError, User};
use futures_util::StreamExt;
use serde_json::Value;
use sqlx::Row;

pub(crate) fn enabled(props: &Value, series: bool) -> Result<bool, StoreError> {
    let key = if series {
        "vod_series_enabled"
    } else {
        "vod_movies_enabled"
    };
    match props {
        Value::Object(values) => Ok(values.get(key) != Some(&Value::Bool(false))),
        Value::Null | Value::Bool(false) => Ok(true),
        Value::String(value) if value.is_empty() => Ok(true),
        Value::Array(value) if value.is_empty() => Ok(true),
        Value::Number(value) if value.as_f64() == Some(0.0) => Ok(true),
        // The actual five users were mappings. Do not invent an XC renderer
        // result for uncaptured truthy nonmapping .get() failures.
        _ => Err(StoreError::Unsupported(
            "XC VOD user property shape is not yet Source-qualified",
        )),
    }
}

/// One bounded PG row and the existing gateway output grant; no second pool.
/// The supervised gateway owns this complete query through physical completion.
pub(crate) async fn write_categories(
    store: &Store,
    user: &User,
    series: bool,
    limit: &ReadLimit,
    out: &mut JsonBytes,
) -> Result<(), StoreError> {
    if !enabled(&user.custom_properties, series)? {
        return out.raw(b"[]");
    }
    let mut connection = store.pool().acquire().await?;
    let result = write_category_rows(&mut connection, series, limit, out).await;
    // Even error-result streams must reach ReadyForQuery before their physical
    // connection and the outer request grant can be released. No new pool or
    // business SQL statement is introduced by this protocol synchronization.
    if let Err(error) = sqlx::Connection::ping(&mut *connection).await {
        let _closed = connection.close().await;
        return Err(StoreError::from(error));
    }
    sqlx::Connection::shrink_buffers(&mut *connection);
    // Retain the original request/byte scope through the actual pool return.
    // Drop alone would spawn asynchronous release after losing our owner.
    connection.return_to_pool().await;
    drop(connection);
    result
}

async fn write_category_rows(
    connection: &mut sqlx::PgConnection,
    series: bool,
    limit: &ReadLimit,
    out: &mut JsonBytes,
) -> Result<(), StoreError> {
    let (table, kind) = if series {
        ("vod_m3useriesrelation", "series")
    } else {
        ("vod_m3umovierelation", "movie")
    };
    // The fixed table name is selected from two constants, never user input.
    // Keep Source's complete DISTINCT projection and Lower(name) ordering;
    // do not add an ID tiebreaker or movie adult predicate to categories.
    let sql = format!(
        "SELECT id,CASE WHEN octet_length(name)<=$2 THEN name END AS name FROM (\
         SELECT DISTINCT c.id::bigint AS id,c.name,c.category_type,c.created_at,c.updated_at,LOWER(c.name) AS sort_name \
         FROM vod_vodcategory c JOIN {table} r ON c.id=r.category_id \
         JOIN m3u_m3uaccount a ON r.m3u_account_id=a.id \
         WHERE c.category_type=$1 AND a.is_active) AS source_categories \
         ORDER BY sort_name LIMIT $3"
    );
    let maximum = limit
        .max_rows
        .checked_add(1)
        .and_then(|v| i64::try_from(v).ok())
        .ok_or_else(reader::capacity)?;
    // Keep oversized text out of the driver's physical row buffer. The inner
    // DISTINCT uses all Source columns; the outer projection changes no name,
    // ordering key, or successful row. NULL is an explicit capacity failure.
    let name_limit =
        i64::try_from(limit.max_metadata.saturating_sub(256)).map_err(|_| reader::capacity())?;
    let query = sqlx::query(&sql).bind(kind).bind(name_limit).bind(maximum);
    let mut rows = query.fetch(connection);
    let mut count = 0usize;
    let mut metadata = 0usize;
    out.raw(b"[")?;
    let mut failure = None;
    while let Some(item) = rows.next().await {
        let row = match item {
            Ok(row) => row,
            Err(error) => {
                if failure.is_none() {
                    failure = Some(StoreError::from(error));
                }
                continue;
            }
        };
        // An output/metadata/count error must not drop an unsettled query.
        // Keep this bounded owner and drain the at-most max_rows+1 stream
        // through its terminal protocol state without retaining more rows.
        if failure.is_some() {
            continue;
        }
        let projected = (|| -> Result<(), StoreError> {
            if count >= limit.max_rows {
                return Err(reader::capacity());
            }
            let name: Option<String> = row.try_get("name")?;
            let name = name.ok_or_else(reader::capacity)?;
            metadata = metadata
                .checked_add(name.capacity())
                .and_then(|v| v.checked_add(256))
                .ok_or_else(reader::capacity)?;
            if metadata > limit.max_metadata {
                return Err(reader::capacity());
            }
            if count > 0 {
                out.raw(b", ")?;
            }
            out.raw(b"{\"category_id\": ")?;
            out.string(&row.try_get::<i64, _>("id")?.to_string())?;
            out.raw(b", \"category_name\": ")?;
            out.string(&name)?;
            out.raw(b", \"parent_id\": 0}")?;
            count += 1;
            Ok(())
        })();
        if let Err(error) = projected {
            failure = Some(error);
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    out.raw(b"]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn captured_default_and_exact_false_flags() {
        assert!(enabled(&json!({}), false).unwrap());
        assert!(enabled(&json!({"vod_movies_enabled": 0}), false).unwrap());
        assert!(!enabled(&json!({"vod_movies_enabled": false}), false).unwrap());
        assert!(!enabled(&json!({"vod_series_enabled": false}), true).unwrap());
        assert!(enabled(&json!({"vod_series_enabled": false}), false).unwrap());
    }
    #[test]
    fn uncaptured_truthy_nonmapping_not_fake_success() {
        for props in [json!([1]), json!("truthy"), json!(true), json!(1)] {
            assert!(enabled(&props, false).is_err());
        }
    }
}
