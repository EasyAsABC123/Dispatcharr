//! Read the Logo collection from the detached SQLite copy.
//! Original Dispatcharr authors retain application and serializer credit.
use crate::{logos::page_window, StoreError};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{types::Json, Connection, QueryBuilder, Sqlite, SqliteConnection};
use std::collections::HashMap;
use url::Url;

const ROW: i64 = 65_536;
#[derive(Default)]
pub(crate) struct Options {
    values: HashMap<String, String>,
    // An empty selection means Original's invalid-digit case.
    ids: Option<Vec<i64>>,
}

pub(crate) fn params(uri: &axum::http::Uri) -> Result<Options, StoreError> {
    let pairs = crate::logos::params(uri);
    let ids = match crate::logos::ids(
        pairs
            .iter()
            .filter(|(key, _)| key == "ids")
            .map(|(_, value)| value.clone()),
    ) {
        Some(ids) if ids.is_empty() => None,
        Some(ids) => Some(
            ids.iter()
                .map(|id| id.parse().map_err(|_| StoreError::UnhandledRestFault))
                .collect::<Result<_, _>>()?,
        ),
        None => Some(Vec::new()),
    };
    let values = pairs.into_iter().collect::<HashMap<_, _>>();
    if values.keys().any(|key| {
        !matches!(
            key.as_str(),
            "page" | "page_size" | "no_pagination" | "used" | "ids" | "name"
        )
    }) {
        return Err(crate::logos::missing());
    }
    Ok(Options { values, ids })
}

fn filters<'a>(query: &mut QueryBuilder<'a, Sqlite>, options: &'a Options) {
    if let Some(ids) = &options.ids {
        query
            .push(" AND l.id IN (SELECT value FROM json_each(")
            .push_bind(Json(ids))
            .push("))");
    }
    if let Some(name) = options.values.get("name").filter(|name| !name.is_empty()) {
        query
            .push(" AND l.name LIKE ")
            .push_bind(crate::channels::like(name))
            .push(" ESCAPE '\\'");
    }
    if let Some(used @ ("true" | "false")) = options.values.get("used").map(String::as_str) {
        query
            .push(if used == "true" {
                " AND EXISTS"
            } else {
                " AND NOT EXISTS"
            })
            .push("(SELECT 1 FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id)");
    }
}

pub(crate) struct Collection {
    rows: Vec<Value>,
    page: Option<Page>,
}

struct Page {
    count: usize,
    next: Option<String>,
    previous: Option<String>,
}

pub(crate) async fn select(
    connection: &mut SqliteConnection,
    options: &Options,
    base: &Url,
    limit: usize,
) -> Result<Collection, StoreError> {
    if options
        .values
        .get("no_pagination")
        .is_some_and(|value| value == "true")
    {
        let rows = rows(connection, limit, options, -1, 0).await?;
        return Ok(Collection { rows, page: None });
    }

    // Django assembles count and results via its paginator. A copied file can
    // get concurrent writes, so keep the count and page rows in one snapshot.
    let mut transaction = connection.begin().await?;
    let mut count_query =
        QueryBuilder::new("SELECT count(*) FROM dispatcharr_channels_logo l WHERE true");
    filters(&mut count_query, options);
    let count: i64 = count_query
        .build_query_scalar()
        .fetch_one(&mut *transaction)
        .await?;
    let count = usize::try_from(count).map_err(|_| capacity())?;
    let (number, size, pages) = page_window(&options.values, count)?;
    let offset = i64::try_from((number - 1) * size).map_err(|_| capacity())?;
    let rows = rows(&mut transaction, limit, options, size as i64, offset).await?;
    transaction.commit().await?;
    Ok(Collection {
        rows,
        page: Some(Page {
            count,
            next: (number < pages).then(|| crate::channels::page_url(base, number + 1)),
            previous: crate::streams::previous(base, number),
        }),
    })
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Copied SQLite Logo collection exceeds the read budget."}),
    )
}

async fn rows(
    connection: &mut SqliteConnection,
    mut remaining: usize,
    options: &Options,
    size: i64,
    offset: i64,
) -> Result<Vec<Value>, StoreError> {
    let mut query = QueryBuilder::new(format!(
        "SELECT l.id,
                CASE WHEN length(CAST(l.name AS BLOB)) <= {ROW} THEN l.name END,
                CASE WHEN length(CAST(l.url AS BLOB)) <= {ROW} THEN l.url END,
                (SELECT count(*) FROM dispatcharr_channels_channel c WHERE c.logo_id=l.id),
                (SELECT json_group_array(CASE WHEN length(CAST(name AS BLOB)) <= {ROW}
                                             THEN 'Channel: ' || name END)
                 FROM (SELECT c.name FROM dispatcharr_channels_channel c
                       WHERE c.logo_id=l.id ORDER BY c.id LIMIT 5))
         FROM dispatcharr_channels_logo l WHERE true"
    ));
    filters(&mut query, options);
    query
        .push(" ORDER BY l.name LIMIT ")
        .push_bind(size)
        .push(" OFFSET ")
        .push_bind(offset);
    let mut rows = query
        .build_query_as::<(
            i64,
            Option<String>,
            Option<String>,
            i64,
            Json<Vec<Option<String>>>,
        )>()
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
    collection: Collection,
    origin: &str,
    response_limit: usize,
) -> Result<Vec<u8>, StoreError> {
    let paged = collection.page.is_some();
    let mut body = match collection.page {
        Some(Page {
            count,
            next,
            previous,
        }) => {
            let mut fields = crate::json_compat::compact_json_bytes(&json!({
                "count": count, "next": next, "previous": previous,
            }))
            .map_err(|_| StoreError::UnhandledRestFault)?;
            fields.pop();
            fields.extend_from_slice(br#","results":["#);
            fields
        }
        None => vec![b'['],
    };
    let end: &[u8] = if paged { b"]}" } else { b"]" };
    let prefix = body.len();
    for row in collection.rows {
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
        let comma = body.len() > prefix;
        if body
            .len()
            .saturating_add(escaped.len())
            .saturating_add(end.len() + usize::from(comma))
            > response_limit
        {
            return Err(capacity());
        }
        if comma {
            body.push(b',');
        }
        body.extend_from_slice(escaped.as_bytes());
    }
    if body.len().saturating_add(end.len()) > response_limit {
        return Err(capacity());
    }
    body.extend_from_slice(end);
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
            Collection {
                rows: rows(&mut db, 8192, &Options::default(), -1, 0)
                    .await
                    .unwrap(),
                page: None,
            },
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
        assert!(rows(&mut db, 256, &Options::default(), -1, 0)
            .await
            .is_err());
        assert!(document(
            Collection {
                rows: rows(&mut db, 8192, &Options::default(), -1, 0)
                    .await
                    .unwrap(),
                page: None
            },
            "http://localhost:8300",
            64
        )
        .is_err());
    }

    #[tokio::test]
    async fn copied_logo_pagination_preserves_default_and_followable_links() {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE dispatcharr_channels_logo(id INTEGER PRIMARY KEY,name TEXT,url TEXT);
             CREATE TABLE dispatcharr_channels_channel(id INTEGER PRIMARY KEY,logo_id INTEGER,name TEXT);
             WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<51)
             INSERT INTO dispatcharr_channels_logo SELECT n,printf('Logo %02d',n),printf('url-%d',n) FROM nums;
             INSERT INTO dispatcharr_channels_channel VALUES(1,1,'Owned Channel');",
        )
        .execute(&mut db)
        .await
        .unwrap();

        for (query, count, first_id, expected_next, expected_previous) in [
            ("", 50, 1, Some("?page=2"), None),
            ("?page=2", 1, 51, None, Some("")),
            (
                "?page_size=1&page=last",
                1,
                51,
                None,
                Some("?page=50&page_size=1"),
            ),
            (
                "?page=2&page_size=1",
                1,
                2,
                Some("?page=3&page_size=1"),
                Some("?page_size=1"),
            ),
            ("?page_size=100", 51, 1, None, None),
            ("?page_size=9999999999999999999999", 51, 1, None, None),
        ] {
            let route = format!("/api/channels/logos/{query}");
            let base = Url::parse(&format!("http://127.0.0.1:8185{route}")).unwrap();
            let options = params(&route.parse().unwrap()).unwrap();
            let page = select(&mut db, &options, &base, 100_000).await.unwrap();
            let doc = document(page, &base.origin().ascii_serialization(), 100_000).unwrap();
            let response: Value = serde_json::from_slice(&doc).unwrap();
            assert_eq!(response["count"], 51);
            assert_eq!(response["results"].as_array().unwrap().len(), count);
            assert_eq!(response["results"][0]["id"], first_id);
            let href = |suffix| format!("http://127.0.0.1:8185/api/channels/logos/{suffix}");
            assert_eq!(response["next"], json!(expected_next.map(href)));
            assert_eq!(response["previous"], json!(expected_previous.map(href)));
            if first_id == 1 {
                assert_eq!(
                    response["results"][0]["channel_names"],
                    json!(["Channel: Owned Channel"])
                );
            }
            assert!(doc.starts_with(br#"{"count":51,"next":"#));
        }
        let base = Url::parse("http://localhost/api/channels/logos/?page=52").unwrap();
        let options = params(&"/api/channels/logos/?page=52".parse().unwrap()).unwrap();
        assert!(matches!(
            select(&mut db, &options, &base, 8192).await,
            Err(StoreError::StatusBody(404, _))
        ));
        let base = Url::parse("http://localhost/api/channels/logos/").unwrap();
        sqlx::query("DELETE FROM dispatcharr_channels_logo")
            .execute(&mut db)
            .await
            .unwrap();
        let empty = select(&mut db, &Options::default(), &base, 8192)
            .await
            .unwrap();
        assert_eq!(
            document(empty, "http://localhost", 8192).unwrap(),
            br#"{"count":0,"next":null,"previous":null,"results":[]}"#
        );
    }

    #[tokio::test]
    async fn copied_logo_used_filter_counts_linked_rows_before_paging() {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE dispatcharr_channels_logo(id INTEGER PRIMARY KEY,name TEXT,url TEXT);
             CREATE TABLE dispatcharr_channels_channel(id INTEGER PRIMARY KEY,logo_id INTEGER,name TEXT);
             INSERT INTO dispatcharr_channels_logo VALUES
                (1,'Alpha','abc'),(2,'Beta','bcd'),(3,'Gamma','cde'),(4,'Zebra','xyz');
             INSERT INTO dispatcharr_channels_channel VALUES
                (1,1,'Linked'),(2,1,'Also linked'),(3,3,'Gamma Channel'),(4,NULL,'No logo');",
        )
        .execute(&mut db)
        .await
        .unwrap();

        for (query, count, first, channel_count, next, previous) in [
            ("used=true&page_size=100", 2, 1, 2, None, None),
            (
                "page_size=1&used=true",
                2,
                1,
                2,
                Some("page=2&page_size=1&used=true"),
                None,
            ),
            (
                "page=2&page_size=1&used=true",
                2,
                3,
                1,
                None,
                Some("page_size=1&used=true"),
            ),
            ("used=true&used=false", 2, 2, 0, None, None),
            ("used=false&used=TRUE", 4, 1, 2, None, None),
            ("used=", 4, 1, 2, None, None),
        ] {
            let route = format!("/api/channels/logos/?{query}");
            let base = Url::parse(&format!("http://localhost{route}")).unwrap();
            let options = params(&route.parse().unwrap()).unwrap();
            let collection = select(&mut db, &options, &base, 8192).await.unwrap();
            let body: Value =
                serde_json::from_slice(&document(collection, "http://localhost", 8192).unwrap())
                    .unwrap();
            let href = |part| format!("http://localhost/api/channels/logos/?{part}");
            assert_eq!(body["count"], count);
            assert_eq!(body["results"][0]["id"], first);
            assert_eq!(body["results"][0]["channel_count"], channel_count);
            assert_eq!(body["results"][0]["is_used"], channel_count != 0);
            assert_eq!(body["next"], json!(next.map(href)));
            assert_eq!(body["previous"], json!(previous.map(href)));
        }

        let bulk = "http://localhost/api/channels/logos/?no_pagination=true&used=false&page=99";
        let values = params(
            &"/api/channels/logos/?no_pagination=true&used=false&page=99"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let selected = select(&mut db, &values, &Url::parse(bulk).unwrap(), 8192)
            .await
            .unwrap();
        assert!(selected.page.is_none());
        assert_eq!(
            selected
                .rows
                .iter()
                .map(|row| row["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            [2, 4]
        );

        let bad = "http://localhost/api/channels/logos/?used=true&page=2";
        let values = params(&"/api/channels/logos/?used=true&page=2".parse().unwrap()).unwrap();
        assert!(matches!(
            select(&mut db, &values, &Url::parse(bad).unwrap(), 8192).await,
            Err(StoreError::StatusBody(404, _))
        ));

        sqlx::query("UPDATE dispatcharr_channels_channel SET logo_id=NULL")
            .execute(&mut db)
            .await
            .unwrap();
        let empty = "http://localhost/api/channels/logos/?used=true";
        let values = params(&"/api/channels/logos/?used=true".parse().unwrap()).unwrap();
        let selected = select(&mut db, &values, &Url::parse(empty).unwrap(), 8192)
            .await
            .unwrap();
        assert_eq!(
            document(selected, "http://localhost", 8192).unwrap(),
            br#"{"count":0,"next":null,"previous":null,"results":[]}"#
        );
    }

    #[test]
    fn copied_logos_apply_original_page_number_and_size_coercion() {
        let opts = |path: &str| params(&path.parse().unwrap()).unwrap();
        assert_eq!(
            page_window(
                &opts("/api/channels/logos/?page=%D9%A2&page_size=1_0").values,
                51
            )
            .unwrap(),
            (2, 10, 6)
        );
        assert_eq!(
            page_window(&opts("/api/channels/logos/?page_size=-1").values, 51).unwrap(),
            (1, 50, 2)
        );
        assert_eq!(
            page_window(&opts("/api/channels/logos/?page_size=999999").values, 2030).unwrap(),
            (1, 1000, 3)
        );
    }

    #[tokio::test]
    async fn copied_logos_bind_original_repeated_ids_and_literal_name_filters() {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE dispatcharr_channels_logo(id INTEGER PRIMARY KEY,name TEXT,url TEXT);
             CREATE TABLE dispatcharr_channels_channel(id INTEGER PRIMARY KEY,logo_id INTEGER,name TEXT);
             INSERT INTO dispatcharr_channels_logo VALUES
               (1,'Alpha_%','a'),(2,'ALPHA-one','b'),(3,'alpha_% second','c'),
               (9223372036854775807,'Maximum','d');
             INSERT INTO dispatcharr_channels_channel VALUES(1,3,'Owned Channel');",
        )
        .execute(&mut db)
        .await
        .unwrap();
        for (query, count, ids) in [
            ("ids=garbage", 4, vec![2, 1, i64::MAX, 3]),
            ("ids=%D9%A3&ids=1&ids=3&page_size=1", 2, vec![1]),
            ("ids=3&ids=2&name=alpha%5F%25&used=true", 1, vec![3]),
            ("name=ignored&name=alpha%5F%25", 2, vec![1, 3]),
            ("ids=1&ids=%C2%B2", 0, vec![]),
            ("ids=9223372036854775807", 1, vec![i64::MAX]),
        ] {
            let base =
                Url::parse(&format!("http://localhost/api/channels/logos/?{query}")).unwrap();
            let options =
                params(&format!("/api/channels/logos/?{query}").parse().unwrap()).unwrap();
            let output = select(&mut db, &options, &base, 8192).await.unwrap();
            assert_eq!(output.page.unwrap().count, count);
            let actual = output
                .rows
                .iter()
                .map(|row| row["id"].as_i64().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(actual, ids, "{query}");
        }
        for query in ["ids=9223372036854775808", "ids=1&ids=9223372036854775808"] {
            let uri = format!("/api/channels/logos/?{query}").parse().unwrap();
            assert!(matches!(params(&uri), Err(StoreError::UnhandledRestFault)));
        }
    }

    #[test]
    fn copied_logos_keep_unsupported_filters_outside_the_read_slice() {
        for query in ["", "?page=last", "?page_size=20", "?no_pagination=true"] {
            assert!(params(&format!("/api/channels/logos/{query}").parse().unwrap()).is_ok());
        }
        for query in ["?sort=1", "?format=html"] {
            assert!(matches!(
                params(&format!("/api/channels/logos/{query}").parse().unwrap()),
                Err(StoreError::StatusBody(404, _))
            ));
        }
    }
}
