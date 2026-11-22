//! Original unfiltered StreamsTable options on the copied SQLite runtime.
//! Original Dispatcharr and DRF contributors retain full credit.
use crate::{RestReadLimits, StoreError};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use sqlx::{Connection, SqliteConnection};
use std::collections::HashMap;

const GROUPS: &str = r#"
    SELECT CASE WHEN length(CAST(g.name AS BLOB)) <= ?1 THEN g.name END
    FROM dispatcharr_channels_channelgroup g
    WHERE EXISTS (
        SELECT 1
        FROM dispatcharr_channels_stream s
        LEFT JOIN m3u_m3uaccount a ON a.id = s.m3u_account_id
        WHERE s.channel_group_id = g.id AND a.is_active IS NOT 0
    )
    ORDER BY g.name
"#;

const ACCOUNTS: &str = r#"
    SELECT a.id, CASE WHEN length(CAST(a.name AS BLOB)) <= ?1 THEN a.name END
    FROM m3u_m3uaccount a
    WHERE a.is_active AND EXISTS (
        SELECT 1 FROM dispatcharr_channels_stream s WHERE s.m3u_account_id = a.id
    )
    ORDER BY a.name
"#;

pub(crate) const STATEMENTS: [&str; 2] = [GROUPS, ACCOUNTS];

pub(crate) fn require_unfiltered(query: Option<&str>) -> Result<(), StoreError> {
    let params: HashMap<_, _> =
        url::form_urlencoded::parse(query.unwrap_or_default().as_bytes()).collect();
    let slow = [
        "name",
        "m3u_account",
        "m3u_account_name",
        "m3u_account_is_active",
        "tvg_id",
    ]
    .iter()
    .any(|key| params.get(*key).is_some_and(|value| !value.is_empty()));
    if slow {
        return Err(StoreError::Unsupported(
            "Copied SQLite stream filter options do not support active filters.",
        ));
    }
    Ok(())
}

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Copied SQLite stream filter options exceed the read budget."}),
    )
}

fn append(body: &mut Vec<u8>, fragment: &[u8], limit: usize) -> Result<(), StoreError> {
    if fragment.len() > limit.saturating_sub(body.len()) {
        return Err(capacity());
    }
    body.extend_from_slice(fragment);
    Ok(())
}

fn append_entry(body: &mut Vec<u8>, value: &Value, limit: usize) -> Result<(), StoreError> {
    let encoded = crate::json_compat::compact_json_bytes(value)
        .map_err(|_| StoreError::UnhandledRestFault)?;
    let text = String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)?;
    let escaped;
    let fragment = if text.contains(['\u{2028}', '\u{2029}']) {
        escaped = text
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
        escaped.as_bytes()
    } else {
        text.as_bytes()
    };
    if body.last() != Some(&b'[') {
        append(body, b",", limit)?;
    }
    append(body, fragment, limit)
}

async fn rows(
    connection: &mut SqliteConnection,
    limits: RestReadLimits,
) -> Result<Vec<u8>, StoreError> {
    let mut body = Vec::new();
    let limit = limits.work_bytes;
    let name_limit = limits.max_raw_bytes as i64;
    append(&mut body, br#"{"groups":["#, limit)?;
    {
        let mut names = sqlx::query_scalar::<_, Option<String>>(GROUPS)
            .bind(name_limit)
            .fetch(&mut *connection);
        while let Some(name) = names.try_next().await? {
            append_entry(&mut body, &json!(name.ok_or_else(capacity)?), limit)?;
        }
    }
    append(&mut body, br#"],"m3u_accounts":["#, limit)?;
    let mut accounts = sqlx::query_as::<_, (i64, Option<String>)>(ACCOUNTS)
        .bind(name_limit)
        .fetch(connection);
    while let Some((id, name)) = accounts.try_next().await? {
        append_entry(
            &mut body,
            &json!({"id":id,"name":name.ok_or_else(capacity)?}),
            limit,
        )?;
    }
    append(&mut body, b"]}", limit)?;
    Ok(body)
}

pub(crate) async fn read(
    connection: &mut SqliteConnection,
    limits: RestReadLimits,
) -> Result<Vec<u8>, StoreError> {
    // Keep both option arrays in one owned read snapshot.
    let mut transaction = connection.begin().await?;
    let result = rows(&mut transaction, limits).await;
    if result.is_ok() {
        transaction.commit().await?;
    } else {
        transaction.rollback().await?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqliteConnectOptions;
    use std::path::Path;

    #[test]
    fn uses_the_original_unfiltered_branch() {
        for query in [
            None,
            Some(""),
            Some("name="),
            Some("tvg_id=x&tvg_id="),
            Some("channel_group=17&search=words&hide_stale=true&unassigned=true"),
        ] {
            assert!(require_unfiltered(query).is_ok());
        }
        for query in [
            "name=word",
            "m3u_account=4",
            "m3u_account_name=x",
            "m3u_account_is_active=false",
            "tvg_id=+",
            "name=&name=x",
        ] {
            assert!(matches!(
                require_unfiltered(Some(query)),
                Err(StoreError::Unsupported(_))
            ));
        }
    }

    #[tokio::test]
    #[ignore = "requires a disposable fixture with the pinned copied SQLite schema"]
    async fn actual_copied_options_match_source_query() {
        let root = std::env::var_os("SQLITE_STREAM_OPTIONS_TEST_ROOT").unwrap();
        let root = Path::new(&root);
        let open = |name: &str| {
            SqliteConnectOptions::new()
                .filename(root.join(name))
                .read_only(true)
        };
        let mut connection = SqliteConnection::connect_with(&open("populated.db"))
            .await
            .unwrap();
        let expected = std::fs::read(root.join("expected.json")).unwrap();
        let body = read(&mut connection, RestReadLimits::default())
            .await
            .unwrap();
        assert_eq!(body, expected);
        let error = read(
            &mut connection,
            RestReadLimits {
                work_bytes: 40,
                ..RestReadLimits::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StoreError::StatusBody(503, _)));
        let error = read(
            &mut connection,
            RestReadLimits {
                max_raw_bytes: 8,
                ..RestReadLimits::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StoreError::StatusBody(503, _)));
        // Both refused reads must fully retire their SQLite transaction.
        assert_eq!(
            read(&mut connection, RestReadLimits::default())
                .await
                .unwrap(),
            expected
        );
        connection.close().await.unwrap();
        let mut empty = SqliteConnection::connect_with(&open("empty.db"))
            .await
            .unwrap();
        assert_eq!(
            read(&mut empty, RestReadLimits::default()).await.unwrap(),
            br#"{"groups":[],"m3u_accounts":[]}"#
        );
        empty.close().await.unwrap();
    }
}
