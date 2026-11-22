//! Read the direct unpaginated Logo collection from the detached SQLite copy.
//! Original Dispatcharr authors retain application and serializer credit.
use crate::StoreError;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{types::Json, SqliteConnection};

const ROW: i64 = 65_536;
const QUERY: &str = "
    SELECT l.id,
           CASE WHEN length(CAST(l.name AS BLOB)) <= ?1 THEN l.name END,
           CASE WHEN length(CAST(l.url AS BLOB)) <= ?1 THEN l.url END,
           (SELECT count(*) FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id),
           (SELECT json_group_array(CASE WHEN length(CAST(name AS BLOB)) <= ?1
                                        THEN 'Channel: ' || name END)
            FROM (SELECT c.name FROM dispatcharr_channels_channel c
                  WHERE c.logo_id=l.id ORDER BY c.id LIMIT 5))
    FROM dispatcharr_channels_logo l ORDER BY l.name";

pub(crate) fn supports(query: Option<&str>) -> bool {
    query == Some("no_pagination=true")
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Copied SQLite Logo collection exceeds the read budget."}),
    )
}

pub(crate) async fn rows(
    connection: &mut SqliteConnection,
    mut remaining: usize,
) -> Result<Vec<Value>, StoreError> {
    let mut rows = sqlx::query_as::<
        _,
        (
            i64,
            Option<String>,
            Option<String>,
            i64,
            Json<Vec<Option<String>>>,
        ),
    >(QUERY)
    .bind(ROW)
    .fetch(connection);
    let mut selected = Vec::new();
    while let Some((id, name, url, channel_count, Json(names))) = rows.try_next().await? {
        let name = name.ok_or_else(capacity)?;
        let url = url.ok_or_else(capacity)?;
        let names = names
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(capacity)?;
        let name_bytes = names.iter().map(String::len).sum::<usize>();
        // Reserve both the bounded stored values and their projection scratch.
        remaining = remaining
            .checked_sub(512 + 2 * name.len() + 3 * url.len() + 2 * name_bytes)
            .ok_or_else(capacity)?;
        selected.push(json!({
            "id": id, "name": name, "url": url, "channel_count": channel_count, "channel_names": names,
        }));
    }
    Ok(selected)
}

pub(crate) fn document(
    logos: Vec<Value>,
    origin: &str,
    response_limit: usize,
) -> Result<Vec<u8>, StoreError> {
    let mut body = Vec::from(b"[".as_slice());
    for row in logos {
        let mut logo = crate::logos::serialize(row, origin);
        let ordered = json!({
            "id": logo["id"].take(),
            "name": logo["name"].take(),
            "url": logo["url"].take(),
            "cache_url": logo["cache_url"].take(),
            "channel_count": logo["channel_count"].take(),
            "is_used": logo["is_used"].take(),
            "channel_names": logo["channel_names"].take(),
        });
        let mut escaped = crate::json_compat::compact_json_bytes(&ordered)
            .map_err(|_| StoreError::UnhandledRestFault)
            .and_then(|encoded| {
                String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)
            })?;
        if escaped.contains(['\u{2028}', '\u{2029}']) {
            escaped = escaped
                .replace('\u{2028}', "\\u2028")
                .replace('\u{2029}', "\\u2029");
        }
        let comma = body.len() > 1;
        if body
            .len()
            .saturating_add(escaped.len())
            .saturating_add(1 + usize::from(comma))
            > response_limit
        {
            return Err(capacity());
        }
        if comma {
            body.push(b',');
        }
        body.extend_from_slice(escaped.as_bytes());
    }
    body.push(b']');
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Connection;

    #[tokio::test]
    async fn copied_logos_match_stock_order_usage_and_wire_fields() {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE dispatcharr_channels_logo(id INTEGER PRIMARY KEY,name TEXT,url TEXT);
             CREATE TABLE dispatcharr_channels_channel(id INTEGER PRIMARY KEY,logo_id INTEGER,name TEXT);
             INSERT INTO dispatcharr_channels_logo VALUES(7,'Zebra','xyz'),(3,'Alpha','abc');
             INSERT INTO dispatcharr_channels_channel VALUES
                (7,3,'Seven'),(6,3,'Six'),(5,3,'Five'),(4,3,'Four'),(3,3,'Three'),(2,3,'Two'),(1,NULL,'Unused');",
        )
        .execute(&mut db)
        .await
        .unwrap();
        let result = document(
            rows(&mut db, 8192).await.unwrap(),
            "http://localhost:8300",
            8192,
        )
        .unwrap();
        assert_eq!(
            std::str::from_utf8(&result).unwrap(),
            concat!(
                r#"[{"id":3,"name":"Alpha","url":"abc","cache_url":"http://localhost:8300/api/channels/logos/3/cache/?v=90015098","channel_count":6,"is_used":true,"channel_names":["Channel: Two","Channel: Three","Channel: Four","Channel: Five","Channel: Six","...and 1 more"]},"#,
                r#"{"id":7,"name":"Zebra","url":"xyz","cache_url":"http://localhost:8300/api/channels/logos/7/cache/?v=d16fb36f","channel_count":0,"is_used":false,"channel_names":[]}]"#,
            )
        );
        assert!(rows(&mut db, 256).await.is_err());
        assert!(document(
            rows(&mut db, 8192).await.unwrap(),
            "http://localhost:8300",
            64
        )
        .is_err());
    }

    #[test]
    fn copied_logos_only_admit_the_initial_unpaginated_query() {
        assert!(supports(Some("no_pagination=true")));
        for other in [
            None,
            Some("no_pagination=false"),
            Some("no_pagination=true&ids=1"),
        ] {
            assert!(!supports(other));
        }
    }
}
