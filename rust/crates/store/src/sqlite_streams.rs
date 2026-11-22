//! Initial copied SQLite Streams table reads. Original Dispatcharr authors retain application credit.
use crate::{
    channels::{self, sqlite_list},
    streams::{ordering_sql, page, previous},
    StoreError,
};
use axum::http::HeaderValue;
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Connection, SqliteConnection};
use std::collections::HashMap;
use url::Url;

const FROM: &str = " FROM dispatcharr_channels_stream s
    LEFT JOIN m3u_m3uaccount a ON a.id=s.m3u_account_id
    LEFT JOIN dispatcharr_channels_channelgroup g ON g.id=s.channel_group_id
    WHERE a.is_active IS NOT 0";
const TEXT_FIELDS: &[&str] = &[
    "name",
    "url",
    "logo_url",
    "tvg_id",
    "local_file",
    "stream_hash",
    "stream_stats",
    "updated_at",
    "last_seen",
    "stream_stats_updated_at",
];
const ROW_TEXT_LIMIT: i64 = 65_536;
const INITIAL_PARAMS: &[&str] = &["page", "page_size", "ordering"];

#[derive(Clone, Copy)]
pub(crate) enum Projection {
    Page,
    Ids,
}

impl Projection {
    pub(crate) fn allow(self) -> HeaderValue {
        match self {
            Self::Page => channels::COLLECTION_ALLOW,
            Self::Ids => HeaderValue::from_static("GET, HEAD, OPTIONS"),
        }
    }

    pub(crate) async fn read(
        self,
        db: &mut SqliteConnection,
        admin: bool,
        params: &HashMap<String, String>,
        base: &Url,
        limit: usize,
    ) -> Result<Vec<u8>, StoreError> {
        if params
            .keys()
            .any(|key| !INITIAL_PARAMS.contains(&key.as_str()))
        {
            return Err(StoreError::StatusBody(
                501,
                json!({"detail": "This copied SQLite Streams view supports unfiltered reads."}),
            ));
        }
        let ordering = ordering_sql(params);
        match self {
            Self::Page => read_page(db, admin, params, base, limit, &ordering).await,
            Self::Ids => read_ids(db, limit, &ordering).await,
        }
    }
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail": "Copied SQLite Streams exceed the read budget."}),
    )
}

fn stream_selection(admin: bool) -> String {
    // Non-admin reads omit the provider URL from both the SQL document and its byte census.
    let fields = channels::STREAM_FIELDS
        .iter()
        .copied()
        .filter(|field| admin || *field != "url")
        .collect::<Vec<_>>();
    let text_bytes = TEXT_FIELDS
        .iter()
        .copied()
        .filter(|field| admin || *field != "url")
        .map(|field| format!("COALESCE(length(CAST(s.{field} AS BLOB)),0)"))
        .collect::<Vec<_>>()
        .join("+");
    let row = sqlite_list::sqlite_json_object("s", &fields);
    format!("CASE WHEN {text_bytes} <= ?1 THEN {row} END")
}

async fn read_page(
    db: &mut SqliteConnection,
    admin: bool,
    params: &HashMap<String, String>,
    base: &Url,
    limit: usize,
    ordering: &str,
) -> Result<Vec<u8>, StoreError> {
    let mut transaction = db.begin().await?;
    let count: i64 = sqlx::query_scalar(&format!("SELECT count(*){FROM}"))
        .fetch_one(&mut *transaction)
        .await?;
    let (number, size, pages) = page(params, usize::try_from(count).map_err(|_| capacity())?)?;
    let sql = format!(
        "SELECT {}{FROM} ORDER BY {ordering} LIMIT ?2 OFFSET ?3",
        stream_selection(admin),
    );

    let mut document = crate::json_compat::compact_json_bytes(&json!({
        "count": count,
        "next": (number < pages).then(|| channels::page_url(base, number + 1)),
        "previous": previous(base, number),
    }))
    .map_err(|_| StoreError::UnhandledRestFault)?;
    document.pop();
    append(&mut document, br#","results":["#, limit)?;
    let mut remaining = limit;
    {
        let mut rows = sqlx::query_scalar::<_, Option<String>>(&sql)
            .bind(ROW_TEXT_LIMIT)
            .bind(size as i64)
            .bind(((number - 1) * size) as i64)
            .fetch(&mut *transaction);
        let mut first = true;
        while let Some(row) = rows.try_next().await? {
            let row = row.ok_or_else(capacity)?;
            remaining = remaining
                .checked_sub(row.len() + 512)
                .ok_or_else(capacity)?;
            let mut raw: Value =
                serde_json::from_str(&row).map_err(|_| StoreError::UnhandledRestFault)?;
            sqlite_list::normalize_stream_timestamps(&mut raw);
            let stream = channels::stream(&json!({"raw": raw}), admin, base);
            let encoded = crate::json_compat::compact_json_bytes(&stream)
                .map_err(|_| StoreError::UnhandledRestFault)?;
            let encoded = String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)?;
            let encoded = encoded
                .replace('\u{2028}', "\\u2028")
                .replace('\u{2029}', "\\u2029");
            if !first {
                append(&mut document, b",", limit)?;
            }
            append(&mut document, encoded.as_bytes(), limit)?;
            first = false;
        }
    }
    transaction.commit().await?;
    append(&mut document, b"]}", limit)?;
    Ok(document)
}

async fn read_ids(
    db: &mut SqliteConnection,
    limit: usize,
    ordering: &str,
) -> Result<Vec<u8>, StoreError> {
    let sql = format!("SELECT s.id{FROM} ORDER BY {ordering}");
    let mut rows = sqlx::query_scalar::<_, i64>(&sql).fetch(db);
    let mut document = vec![b'['];
    while let Some(id) = rows.try_next().await? {
        if document.len() > 1 {
            append(&mut document, b", ", limit)?;
        }
        append(&mut document, id.to_string().as_bytes(), limit)?;
    }
    append(&mut document, b"]", limit)?;
    Ok(document)
}

fn append(document: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result<(), StoreError> {
    if document.len().saturating_add(bytes.len()) > limit {
        return Err(capacity());
    }
    document.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> SqliteConnection {
        let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(
            "CREATE TABLE m3u_m3uaccount(id INTEGER PRIMARY KEY, name TEXT, is_active BOOLEAN);
             CREATE TABLE dispatcharr_channels_channelgroup(id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE dispatcharr_channels_stream(
                 id INTEGER PRIMARY KEY, name TEXT, url TEXT, logo_url TEXT, tvg_id TEXT,
                 local_file TEXT, current_viewers INTEGER DEFAULT 0,
                 updated_at TEXT DEFAULT '2026-01-02 03:04:05.120001',
                 m3u_account_id INTEGER, stream_profile_id INTEGER, is_custom BOOLEAN DEFAULT 0,
                 channel_group_id INTEGER, last_seen TEXT DEFAULT '2026-01-02 03:04:05',
                 stream_hash TEXT, stream_stats TEXT, stream_stats_updated_at TEXT,
                 is_stale BOOLEAN DEFAULT 0, is_adult BOOLEAN DEFAULT 0, stream_id INTEGER,
                 stream_chno REAL, is_catchup BOOLEAN DEFAULT 0, catchup_days INTEGER DEFAULT 0,
                 is_radio BOOLEAN DEFAULT 0
             );
             INSERT INTO m3u_m3uaccount VALUES (2,'Live',1),(3,'Old',0);
             INSERT INTO dispatcharr_channels_channelgroup VALUES (4,'Group');
             INSERT INTO dispatcharr_channels_stream
                 (id,name,url,m3u_account_id,channel_group_id,stream_stats,stream_chno,is_catchup)
             VALUES (3,'Inactive',NULL,3,4,NULL,3,0),
                    (2,'Alpha','http://fixture.invalid/placeholder',2,4,'{\"nested\":[true,2]}',2,1),
                    (1,'Zulu',NULL,NULL,NULL,NULL,NULL,0);",
        )
        .execute(&mut db)
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn copied_stream_page_uses_source_order_fields_pagination_and_permissions() {
        let mut db = fixture().await;
        let base = Url::parse(
            "http://localhost:8300/api/channels/streams/?page=1&page_size=1&ordering=name",
        )
        .unwrap();
        let mut params = base.query_pairs().into_owned().collect::<HashMap<_, _>>();
        let wire = Projection::Page
            .read(&mut db, false, &params, &base, 8192)
            .await
            .unwrap();
        assert!(!std::str::from_utf8(&wire)
            .unwrap()
            .contains("fixture.invalid"));
        assert!(!stream_selection(false).contains("s.url"));
        assert_eq!(wire, concat!(
            r#"{"count":2,"next":"http://localhost:8300/api/channels/streams/?ordering=name&page=2&page_size=1","previous":null,"results":["#,
            r#"{"id":2,"name":"Alpha","m3u_account":2,"logo_url":null,"tvg_id":null,"local_file":null,"current_viewers":0,"updated_at":"2026-01-02T03:04:05.120001Z","last_seen":"2026-01-02T03:04:05Z","is_stale":false,"is_adult":false,"stream_profile_id":null,"is_custom":false,"channel_group":4,"stream_hash":null,"stream_stats":{"nested":[true,2]},"stream_stats_updated_at":null,"stream_id":null,"stream_chno":2.0,"is_catchup":true,"catchup_days":0,"is_radio":false}]}"#,
        ).as_bytes());

        let admin = Projection::Page
            .read(&mut db, true, &params, &base, 8192)
            .await
            .unwrap();
        let admin: Value = serde_json::from_slice(&admin).unwrap();
        assert_eq!(
            admin["results"][0]["url"],
            "http://fixture.invalid/placeholder"
        );

        params.insert("page".into(), "last".into());
        let last = Projection::Page
            .read(&mut db, false, &params, &base, 8192)
            .await
            .unwrap();
        let last: Value = serde_json::from_slice(&last).unwrap();
        assert_eq!(last["results"][0]["id"], 1);
        assert_eq!(last["next"], Value::Null);
        assert_eq!(
            last["previous"],
            "http://localhost:8300/api/channels/streams/?ordering=name&page_size=1"
        );

        params.insert("page".into(), "3".into());
        assert!(matches!(
            Projection::Page
                .read(&mut db, false, &params, &base, 8192)
                .await,
            Err(StoreError::StatusBody(404, _))
        ));
    }

    #[tokio::test]
    async fn copied_stream_ids_keep_python_array_and_reject_unported_filters() {
        let mut db = fixture().await;
        let base = Url::parse("http://localhost:8300/api/channels/streams/ids/").unwrap();
        let mut params = HashMap::new();
        assert_eq!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 8192)
                .await
                .unwrap(),
            b"[1, 2]"
        );
        params.insert("ordering".into(), "name".into());
        params.insert("page_size".into(), "1".into());
        assert_eq!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 8192)
                .await
                .unwrap(),
            b"[2, 1]"
        );
        assert!(matches!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 4)
                .await,
            Err(StoreError::StatusBody(503, _))
        ));
        params.insert("hide_stale".into(), "true".into());
        for scope in [Projection::Page, Projection::Ids] {
            assert!(matches!(
                scope.read(&mut db, false, &params, &base, 8192).await,
                Err(StoreError::StatusBody(501, _))
            ));
        }
    }
}
