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
const SUPPORTED_PARAMS: &[&str] = &[
    "page",
    "page_size",
    "ordering",
    "name",
    "m3u_account",
    "ids",
];
const ACCOUNT_MATCH: &str = " AND s.m3u_account_id IN (SELECT value FROM json_each(?))";
const NAME_MATCH: &str = r" AND s.name LIKE ? ESCAPE '\'";

pub(super) fn name_pattern(value: Option<&str>) -> Result<Option<String>, StoreError> {
    let Some(name) = value.and_then(|value| {
        // CharField.strip() uses Python's whitespace set, including ASCII
        // information separators in addition to Unicode White_Space.
        let text =
            value.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c));
        (!text.is_empty()).then_some(text)
    }) else {
        return Ok(None);
    };
    if name.contains('\0') {
        return Err(StoreError::InvalidInput(
            json!({"name":["Null characters are not allowed."]}),
        ));
    }
    Ok(Some(channels::like(name)))
}

#[derive(Default)]
pub(crate) struct StreamFilter {
    pub(super) name: Option<String>,
    pub(super) accounts: Option<String>,
}

impl StreamFilter {
    pub(crate) fn new(name: Option<String>, accounts: Option<&str>) -> Result<Self, StoreError> {
        Ok(Self {
            name,
            accounts: accounts
                .filter(|csv| !csv.is_empty())
                .map(account_ids)
                .transpose()?,
        })
    }

    fn predicate(&self) -> &'static str {
        match (self.name.is_some(), self.accounts.is_some()) {
            (false, false) => "",
            (true, false) => NAME_MATCH,
            (false, true) => ACCOUNT_MATCH,
            (true, true) => {
                r" AND s.name LIKE ? ESCAPE '\' AND s.m3u_account_id IN (SELECT value FROM json_each(?))"
            }
        }
    }

    fn bindings(&self) -> impl Iterator<Item = &String> {
        self.name.iter().chain(&self.accounts)
    }
}

fn account_ids(csv: &str) -> Result<String, StoreError> {
    let mut ids = csv
        .split(',')
        .map(|item| {
            item.parse::<i64>()
                .ok()
                .filter(|id| *id > 0 && item.bytes().all(|byte| byte.is_ascii_digit()))
                .ok_or(StoreError::Unsupported(
                    "Copied SQLite Streams account IDs must be positive decimal integers.",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    ids.sort_unstable();
    ids.dedup();
    // Bind a single JSON array of integers even when React selects many accounts.
    serde_json::to_string(&ids).map_err(|_| StoreError::UnhandledRestFault)
}

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
        // Original StreamViewSet.list uses the model directly for nonempty ids:
        // no pagination, active-provider predicate, or ordinary list filters.
        if matches!(self, Self::Page) {
            if let Some(ids) = params.get("ids").filter(|value| !value.is_empty()) {
                return read_selected(db, admin, ids, base, limit).await;
            }
        }
        if params
            .keys()
            .any(|key| !SUPPORTED_PARAMS.contains(&key.as_str()))
        {
            return Err(StoreError::StatusBody(
                501,
                json!({"detail": "This copied SQLite Streams view supports the name and M3U account filters."}),
            ));
        }
        let name = name_pattern(params.get("name").map(String::as_str))?;
        let filter = StreamFilter::new(name, params.get("m3u_account").map(String::as_str))?;
        let ordering = ordering_sql(params);
        match self {
            Self::Page => read_page(db, admin, params, base, limit, &ordering, &filter).await,
            Self::Ids => read_ids(db, limit, &ordering, &filter).await,
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
    format!("CASE WHEN {text_bytes} <= ? THEN {row} END")
}

async fn read_page(
    db: &mut SqliteConnection,
    admin: bool,
    params: &HashMap<String, String>,
    base: &Url,
    limit: usize,
    ordering: &str,
    filter: &StreamFilter,
) -> Result<Vec<u8>, StoreError> {
    let mut transaction = db.begin().await?;
    let predicate = filter.predicate();
    let count_sql = format!("SELECT count(*){FROM}{predicate}");
    let mut count_query = sqlx::query_scalar(&count_sql);
    for value in filter.bindings() {
        count_query = count_query.bind(value);
    }
    let count: i64 = count_query.fetch_one(&mut *transaction).await?;
    let (number, size, pages) = page(params, usize::try_from(count).map_err(|_| capacity())?)?;
    let sql = format!(
        "SELECT {}{FROM}{predicate} ORDER BY {ordering} LIMIT ? OFFSET ?",
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
        let mut query = sqlx::query_scalar::<_, Option<String>>(&sql).bind(ROW_TEXT_LIMIT);
        for value in filter.bindings() {
            query = query.bind(value);
        }
        let mut rows = query
            .bind(size as i64)
            .bind(((number - 1) * size) as i64)
            .fetch(&mut *transaction);
        let mut first = true;
        while let Some(row) = rows.try_next().await? {
            let row = row.ok_or_else(capacity)?;
            remaining = remaining
                .checked_sub(row.len() + 512)
                .ok_or_else(capacity)?;
            let encoded = encode_stream(&row, admin, base)?;
            if !first {
                append(&mut document, b",", limit)?;
            }
            append(&mut document, &encoded, limit)?;
            first = false;
        }
    }
    transaction.commit().await?;
    append(&mut document, b"]}", limit)?;
    Ok(document)
}

// Reuse the exact existing field, URL-redaction and timestamp projection.
fn encode_stream(row: &str, admin: bool, base: &Url) -> Result<Vec<u8>, StoreError> {
    let mut raw: Value = serde_json::from_str(row).map_err(|_| StoreError::UnhandledRestFault)?;
    sqlite_list::normalize_stream_timestamps(&mut raw);
    let stream = channels::stream(&json!({"raw": raw}), admin, base);
    let encoded = crate::json_compat::compact_json_bytes(&stream)
        .map_err(|_| StoreError::UnhandledRestFault)?;
    let encoded = String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)?;
    Ok(encoded
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
        .into_bytes())
}

async fn read_selected(
    db: &mut SqliteConnection,
    admin: bool,
    ids: &str,
    base: &Url,
    limit: usize,
) -> Result<Vec<u8>, StoreError> {
    if ids.len() > limit {
        return Err(capacity());
    }
    // Match the existing Native PG integer parser; bind one JSON array instead
    // of interpolating IDs or constructing an unbounded SQL placeholder list.
    let ids = ids
        .split(',')
        .map(|value| value.trim().parse::<i64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| StoreError::StatusBody(500, json!({"detail":"Invalid stream filter."})))?;
    let ids = serde_json::to_string(&ids).map_err(|_| StoreError::UnhandledRestFault)?;
    let sql = format!(
        "SELECT {} FROM dispatcharr_channels_stream s \
         WHERE s.id IN (SELECT value FROM json_each(?)) ORDER BY s.updated_at DESC",
        stream_selection(admin),
    );
    let mut rows = sqlx::query_scalar::<_, Option<String>>(&sql)
        .bind(ROW_TEXT_LIMIT)
        .bind(ids)
        .fetch(db);
    let mut document = vec![b'['];
    let mut remaining = limit;
    let mut found = false;
    while let Some(row) = rows.try_next().await? {
        let row = row.ok_or_else(capacity)?;
        remaining = remaining
            .checked_sub(row.len() + 512)
            .ok_or_else(capacity)?;
        let encoded = encode_stream(&row, admin, base)?;
        if found {
            append(&mut document, b",", limit)?;
        }
        append(&mut document, &encoded, limit)?;
        found = true;
    }
    if !found {
        return Err(StoreError::StatusBody(
            404,
            json!({"detail":"No Stream matches the given query."}),
        ));
    }
    append(&mut document, b"]", limit)?;
    Ok(document)
}

async fn read_ids(
    db: &mut SqliteConnection,
    limit: usize,
    ordering: &str,
    filter: &StreamFilter,
) -> Result<Vec<u8>, StoreError> {
    let predicate = filter.predicate();
    let sql = format!("SELECT s.id{FROM}{predicate} ORDER BY {ordering}");
    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    for value in filter.bindings() {
        query = query.bind(value);
    }
    let mut rows = query.fetch(db);
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
    async fn selected_streams_override_is_a_model_ordered_unpaginated_array() {
        let mut db = fixture().await;
        sqlx::raw_sql("UPDATE dispatcharr_channels_stream SET updated_at='2026-01-01 00:00:00' WHERE id=1;
                       UPDATE dispatcharr_channels_stream SET updated_at='2026-01-02 00:00:00' WHERE id=2;
                       UPDATE dispatcharr_channels_stream SET updated_at='2026-01-03 00:00:00' WHERE id=3;")
            .execute(&mut db).await.unwrap();
        let base =
            Url::parse("http://localhost:8300/api/channels/streams/?ids=1,3,2,1,999").unwrap();
        let mut params: HashMap<_, _> = base.query_pairs().into_owned().collect();
        for (key, value) in [
            ("page", "-1"),
            ("page_size", "1"),
            ("name", "absent"),
            ("m3u_account", "bad"),
            ("ordering", "name"),
            ("assigned", "bad"),
        ] {
            params.insert(key.into(), value.into());
        }
        for admin in [false, true] {
            let bytes = Projection::Page
                .read(&mut db, admin, &params, &base, 16384)
                .await
                .unwrap();
            let result: Value = serde_json::from_slice(&bytes).unwrap();
            let rows = result.as_array().unwrap();
            assert_eq!(
                rows.iter()
                    .map(|row| row["id"].as_i64().unwrap())
                    .collect::<Vec<_>>(),
                [3, 2, 1]
            );
            assert_eq!(rows[1].get("url").is_some(), admin);
            assert_eq!(rows[1]["stream_stats"], json!({"nested":[true,2]}));
            assert_eq!(rows[1]["updated_at"], "2026-01-02T00:00:00Z");
        }
        // An empty ids value follows the ordinary paginated collection path.
        params.clear();
        params.insert("ids".into(), String::new());
        let page: Value = serde_json::from_slice(
            &Projection::Page
                .read(&mut db, false, &params, &base, 16384)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(page["count"], 2);
        assert!(page["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["id"] != 3));
        // The /streams/ids/ action is a separate filtered ID projection.
        params.insert("ids".into(), "3".into());
        let ids: Value = serde_json::from_slice(
            &Projection::Ids
                .read(&mut db, false, &params, &base, 16384)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(ids.as_array().unwrap().len(), 2);
        assert!(!ids.as_array().unwrap().contains(&json!(3)));
    }

    #[tokio::test]
    async fn selected_streams_missing_invalid_and_budget_controls_do_not_write() {
        let mut db = fixture().await;
        let before: i64 = sqlx::query_scalar("SELECT total_changes()")
            .fetch_one(&mut db)
            .await
            .unwrap();
        let base = Url::parse("http://localhost:8300/api/channels/streams/").unwrap();
        for (csv, status) in [
            ("999", 404),
            ("0,-1", 404),
            ("bad", 500),
            ("2,", 500),
            ("2) OR 1=1--", 500),
        ] {
            let params = HashMap::from([("ids".into(), csv.into())]);
            assert!(
                matches!(Projection::Page.read(&mut db,true,&params,&base,16384).await,
                Err(StoreError::StatusBody(code,_)) if code==status)
            );
        }
        let params = HashMap::from([("ids".into(), "1,2,3".into())]);
        assert!(matches!(
            Projection::Page
                .read(&mut db, true, &params, &base, 32)
                .await,
            Err(StoreError::StatusBody(503, _))
        ));
        let after: i64 = sqlx::query_scalar("SELECT total_changes()")
            .fetch_one(&mut db)
            .await
            .unwrap();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn copied_m3u_account_filter_preserves_visibility_and_option_hierarchy() {
        use crate::{auth::sqlite_runtime::stream_filter_options, RestReadLimits};

        let mut db = fixture().await;
        sqlx::raw_sql(
            "INSERT INTO m3u_m3uaccount VALUES (5,'Second',1),(6,'Unused',1);
             INSERT INTO dispatcharr_channels_channelgroup VALUES
                 (7,'Second group'),(8,'Disabled group'),(9,'Custom group');
             UPDATE dispatcharr_channels_stream SET channel_group_id=8 WHERE id=3;
             UPDATE dispatcharr_channels_stream SET channel_group_id=9 WHERE id=1;
             INSERT INTO dispatcharr_channels_stream(id,name,url,m3u_account_id,channel_group_id)
             VALUES (7,'Bravo','http://fixture.invalid/second',5,7),
                    (8,'Alpine','http://fixture.invalid/first',2,4)",
        )
        .execute(&mut db)
        .await
        .unwrap();
        let base = Url::parse(
            "http://localhost:8300/api/channels/streams/?page=1&page_size=1&ordering=name&m3u_account=2%2C3%2C2",
        )
        .unwrap();
        let mut params: HashMap<_, _> = base.query_pairs().into_owned().collect();
        let page = Projection::Page
            .read(&mut db, false, &params, &base, 8192)
            .await
            .unwrap();
        let page: Value = serde_json::from_slice(&page).unwrap();
        assert_eq!(page["count"], 2);
        assert_eq!(page["results"][0]["id"], 2);
        assert!(page["results"][0].get("url").is_none());
        assert_eq!(
            page["next"],
            "http://localhost:8300/api/channels/streams/?m3u_account=2%2C3%2C2&ordering=name&page=2&page_size=1"
        );
        params.insert("page".into(), "2".into());
        let page = Projection::Page
            .read(&mut db, true, &params, &base, 8192)
            .await
            .unwrap();
        let page: Value = serde_json::from_slice(&page).unwrap();
        assert_eq!(page["results"][0]["url"], "http://fixture.invalid/first");
        assert_eq!(
            page["previous"],
            "http://localhost:8300/api/channels/streams/?m3u_account=2%2C3%2C2&ordering=name&page_size=1"
        );

        for (selection, expected) in [
            ("2,3,2", b"[2, 8]".as_slice()),
            ("5,2", b"[2, 8, 7]".as_slice()),
            ("3,6,99", b"[]".as_slice()),
            ("", b"[2, 8, 7, 1]".as_slice()),
        ] {
            params.insert("m3u_account".into(), selection.into());
            assert_eq!(
                Projection::Ids
                    .read(&mut db, false, &params, &base, 8192)
                    .await
                    .unwrap(),
                expected
            );
        }
        for (query, expected) in [
            (
                "m3u_account=2,3,2",
                r#"{"groups":["Group"],"m3u_accounts":[{"id":2,"name":"Live"},{"id":5,"name":"Second"}]}"#,
            ),
            (
                "m3u_account=3,6,99",
                r#"{"groups":[],"m3u_accounts":[{"id":2,"name":"Live"},{"id":5,"name":"Second"}]}"#,
            ),
            (
                "m3u_account=5&channel_group=Group",
                r#"{"groups":["Second group"],"m3u_accounts":[{"id":2,"name":"Live"},{"id":5,"name":"Second"}]}"#,
            ),
            (
                "m3u_account=2&m3u_account=5",
                r#"{"groups":["Second group"],"m3u_accounts":[{"id":2,"name":"Live"},{"id":5,"name":"Second"}]}"#,
            ),
            (
                "m3u_account=",
                r#"{"groups":["Custom group","Group","Second group"],"m3u_accounts":[{"id":2,"name":"Live"},{"id":5,"name":"Second"}]}"#,
            ),
        ] {
            let filter = stream_filter_options::filter(Some(query)).unwrap();
            assert_eq!(
                stream_filter_options::read(&mut db, RestReadLimits::default(), &filter)
                    .await
                    .unwrap(),
                expected.as_bytes()
            );
        }
        params.insert("page".into(), "1".into());
        params.insert("m3u_account".into(), "2".into());
        params.insert("name".into(), "Alpha".into());
        assert_eq!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 8192)
                .await
                .unwrap(),
            b"[2]"
        );
        let page = Projection::Page
            .read(&mut db, false, &params, &base, 8192)
            .await
            .unwrap();
        let page: Value = serde_json::from_slice(&page).unwrap();
        assert_eq!(page["count"], 1);
        assert_eq!(page["results"][0]["id"], 2);
        assert_eq!(page["next"], Value::Null);
        assert!(page["results"][0].get("url").is_none());

        // Name filters both option dimensions, whereas selecting account
        // 2 limits only groups. Here only provider 5 has the requested name.
        let filter = stream_filter_options::filter(Some("name=Bravo&m3u_account=2")).unwrap();
        assert_eq!(
            stream_filter_options::read(&mut db, RestReadLimits::default(), &filter)
                .await
                .unwrap(),
            br#"{"groups":[],"m3u_accounts":[{"id":5,"name":"Second"}]}"#
        );
        params.insert("name".into(), "Bravo".into());
        assert_eq!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 8192)
                .await
                .unwrap(),
            b"[]"
        );
        params.insert("channel_group".into(), "Group".into());
        assert!(matches!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 8192)
                .await,
            Err(StoreError::StatusBody(501, _))
        ));
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
    async fn copied_name_filter_shares_literal_sqlite_matching_across_all_three_reads() {
        use crate::{auth::sqlite_runtime::stream_filter_options, RestReadLimits};

        let mut db = fixture().await;
        sqlx::raw_sql(
            r"INSERT INTO dispatcharr_channels_channelgroup VALUES (5,'Custom'),(6,'Inactive only');
              INSERT INTO dispatcharr_channels_stream (id,name,url,m3u_account_id,channel_group_id)
              VALUES (4,'Label ALPHA_% Before','http://fixture.invalid/filtered',2,4),
                     (5,'Name alpha_% After',NULL,NULL,5),
                     (6,'Name alphaXY After',NULL,NULL,5),
                     (7,'Inactive alpha_%',NULL,3,6),
                     (8,'Back\Slash Éclair',NULL,2,4),
                     (9,'O''Brien',NULL,2,4);",
        )
        .execute(&mut db)
        .await
        .unwrap();
        let base = Url::parse(
            "http://localhost:8300/api/channels/streams/?name=alpha_%25&ordering=name&page_size=1",
        )
        .unwrap();
        let mut params = base.query_pairs().into_owned().collect::<HashMap<_, _>>();
        let page = Projection::Page
            .read(&mut db, false, &params, &base, 8192)
            .await
            .unwrap();
        let page: Value = serde_json::from_slice(&page).unwrap();
        assert_eq!(page["count"], 2);
        assert_eq!(page["results"][0]["id"], 4);
        assert!(page["results"][0].get("url").is_none());
        assert_eq!(
            page["next"],
            "http://localhost:8300/api/channels/streams/?name=alpha_%25&ordering=name&page=2&page_size=1"
        );
        assert_eq!(
            Projection::Ids
                .read(&mut db, false, &params, &base, 8192)
                .await
                .unwrap(),
            b"[4, 5]"
        );
        let filter = stream_filter_options::filter(Some("name=ignored&name=+ALPHA_%25+")).unwrap();
        assert_eq!(
            stream_filter_options::read(&mut db, RestReadLimits::default(), &filter)
                .await
                .unwrap(),
            br#"{"groups":["Custom","Group"],"m3u_accounts":[{"id":2,"name":"Live"}]}"#
        );
        for (needle, ids) in [
            (r"Back\Slash", b"[8]".as_slice()),
            ("éclair", b"[]".as_slice()),
            ("Éclair", b"[8]".as_slice()),
            ("O'Brien", b"[9]".as_slice()),
        ] {
            params.insert("name".into(), needle.into());
            assert_eq!(
                Projection::Ids
                    .read(&mut db, false, &params, &base, 8192)
                    .await
                    .unwrap(),
                ids
            );
        }
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
