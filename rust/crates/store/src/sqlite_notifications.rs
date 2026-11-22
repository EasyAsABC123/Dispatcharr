//! Copied SQLite notification collection used by the original React Sidebar.
//! Original Dispatcharr contributors retain full application credit.
use super::*;
use crate::RestReadLimits;
use chrono::NaiveDateTime;
use futures_util::TryStreamExt;
use sqlx::SqliteConnection;

// A single snapshot reads visibility and per-user dismissal. Django stores
// naive UTC datetimes in SQLite, in a lexical UTC ordering with a space.
pub(crate) const QUERY: &str = r#"
    SELECT CASE WHEN
        length(CAST(n.notification_key AS BLOB)) + length(CAST(n.notification_type AS BLOB)) +
        length(CAST(n.priority AS BLOB)) + length(CAST(n.title AS BLOB)) +
        length(CAST(n.message AS BLOB)) + length(CAST(n.action_data AS BLOB)) +
        length(CAST(n.created_at AS BLOB)) + coalesce(length(CAST(n.expires_at AS BLOB)),0) +
        length(CAST(n.source AS BLOB)) <= ?4
    THEN json_object(
        'id',n.id, 'notification_key',n.notification_key,
        'notification_type',n.notification_type, 'priority',n.priority,
        'title',n.title, 'message',n.message, 'action_data',json(n.action_data),
        'is_active',json('true'), 'admin_only',json(CASE WHEN n.admin_only THEN 'true' ELSE 'false' END),
        'expires_at',n.expires_at, 'created_at',n.created_at,
        'is_dismissed',json(CASE WHEN EXISTS(
            SELECT 1 FROM core_notificationdismissal d WHERE d.notification_id=n.id AND d.user_id=?1
        ) THEN 'true' ELSE 'false' END), 'source',n.source
    ) END
    FROM core_systemnotification n
    WHERE n.is_active AND (n.expires_at IS NULL OR n.expires_at > ?3)
      AND (?2 OR NOT n.admin_only)
    ORDER BY n.priority DESC,n.created_at DESC
"#;

fn capacity() -> StoreError {
    StoreError::StatusBody(
        503,
        json!({"detail":"Copied SQLite notification collection exceeds the read budget."}),
    )
}

fn timestamp(value: &Value) -> Result<Value, StoreError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::String(text) => NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
            .map(|time| date(time.and_utc()))
            .map_err(|_| failed()),
        _ => Err(failed()),
    }
}

pub(crate) async fn read(
    connection: &mut SqliteConnection,
    user_id: i64,
    admin: bool,
    now: DateTime<Utc>,
    limits: RestReadLimits,
) -> Result<Vec<Value>, StoreError> {
    let mut rows = sqlx::query_scalar::<_, Option<String>>(QUERY)
        .bind(user_id)
        .bind(admin)
        .bind(now.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .bind(limits.max_raw_bytes as i64)
        .fetch(connection);
    let mut projected = Vec::new();
    let mut remaining = limits.work_bytes;
    while let Some(row) = rows.try_next().await? {
        let raw = row.ok_or_else(capacity)?;
        remaining = remaining.checked_sub(raw.len()).ok_or_else(capacity)?;
        let mut row: Value = serde_json::from_str(&raw).map_err(|_| failed())?;
        for field in ["created_at", "expires_at"] {
            row[field] = timestamp(&row[field])?;
        }
        projected.push(row);
    }
    Ok(projected)
}

pub(crate) async fn developer_visible<F, Fut>(
    cache: &NotificationCache,
    user: i64,
    id: i64,
    conditions: &Value,
    settings: F,
) -> Result<bool, StoreError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<Value, StoreError>>,
{
    cache
        .condition_visible(user, id, conditions, false, || {
            cache.settings_group_observe("network_access", &settings, true)
        })
        .await
}

pub(crate) async fn render<F, Fut>(
    rows: Vec<Value>,
    include_dismissed: bool,
    response_limit: usize,
    developer_visible: F,
) -> Result<Vec<u8>, StoreError>
where
    F: Fn(i64, Value) -> Fut,
    Fut: std::future::Future<Output = Result<bool, StoreError>>,
{
    let mut matching = Vec::new();
    for row in rows {
        if row["source"] == "developer" && truthy(Some(&row["action_data"])) {
            let action = row["action_data"].as_object().ok_or_else(failed)?;
            if let Some(conditions) = action.get("condition").filter(|value| truthy(Some(value))) {
                let id = row["id"].as_i64().ok_or_else(failed)?;
                if !developer_visible(id, conditions.clone()).await? {
                    continue;
                }
            }
        }
        if include_dismissed || row["is_dismissed"] != true {
            matching.push(row);
        }
    }
    let count = matching.len();
    let value = json!({"notifications":matching,"count":count,"unread_count":count});
    let encoded = crate::json_compat::compact_json_bytes(&value)
        .map_err(|_| StoreError::UnhandledRestFault)?;
    let mut encoded = String::from_utf8(encoded).map_err(|_| StoreError::UnhandledRestFault)?;
    if encoded.contains(['\u{2028}', '\u{2029}']) {
        encoded = encoded
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029");
    }
    if encoded.len() > response_limit {
        return Err(capacity());
    }
    Ok(encoded.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{sqlite::SqliteConnectOptions, Connection};
    use std::path::Path;

    async fn list(database: &mut SqliteConnection, user: i64, admin: bool, include: bool) -> Value {
        let now = "2026-01-01T00:00:00Z".parse().unwrap();
        let limits = RestReadLimits::default();
        let rows = read(database, user, admin, now, limits).await.unwrap();
        let encoded = render(
            rows,
            include,
            limits.work_bytes,
            |id, conditions| async move {
                assert_eq!(conditions, json!(["m3u_epg_network_insecure"]));
                match id {
                    91006 => Ok(false),
                    91007 => Ok(true),
                    _ => panic!("unexpected developer condition"),
                }
            },
        )
        .await
        .unwrap();
        serde_json::from_slice(&encoded).unwrap()
    }

    fn ids(body: &Value) -> Vec<i64> {
        body["notifications"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["id"].as_i64().unwrap())
            .collect()
    }

    #[test]
    fn include_dismissed_matches_original_last_query_value() {
        assert!(!include_dismissed(None));
        assert!(include_dismissed(Some("other=1&include_dismissed=True")));
        assert!(!include_dismissed(Some(
            "include_dismissed=true&include_dismissed=false"
        )));
    }

    #[tokio::test]
    #[ignore = "requires an owned, externally seeded current SQLite copy"]
    async fn current_copy_role_expiry_order_and_dismissal() {
        let path = std::env::var_os("SQLITE_NOTIFICATIONS_TEST_COPY").unwrap();
        let mut database = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(Path::new(&path))
                .read_only(true),
        )
        .await
        .unwrap();
        let admin: i64 = sqlx::query_scalar(
            "SELECT id FROM accounts_user WHERE user_level>=10 ORDER BY id LIMIT 1",
        )
        .fetch_one(&mut database)
        .await
        .unwrap();
        let standard: i64 = sqlx::query_scalar(
            "SELECT id FROM accounts_user WHERE user_level=1 ORDER BY id LIMIT 1",
        )
        .fetch_one(&mut database)
        .await
        .unwrap();
        let streamer: i64 = sqlx::query_scalar(
            "SELECT id FROM accounts_user WHERE user_level=0 ORDER BY id LIMIT 1",
        )
        .fetch_one(&mut database)
        .await
        .unwrap();
        let admins = list(&mut database, admin, true, false).await;
        assert_eq!(ids(&admins), [91001, 91007, 91002]);
        assert_eq!(admins["count"], 3);
        assert_eq!(admins["unread_count"], 3);
        assert_eq!(
            admins["notifications"][0]["created_at"],
            "2025-12-01T00:00:00.123456Z"
        );
        assert_eq!(
            admins["notifications"][0]["expires_at"],
            "2030-01-01T00:00:00Z"
        );
        assert_eq!(
            admins["notifications"][1]["action_data"],
            json!({"condition":["m3u_epg_network_insecure"]})
        );
        assert_eq!(
            ids(&list(&mut database, standard, false, false).await),
            [91007]
        );
        assert_eq!(
            ids(&list(&mut database, streamer, false, false).await),
            [91001, 91007]
        );
        let dismissed = list(&mut database, standard, false, true).await;
        assert_eq!(ids(&dismissed), [91001, 91007]);
        assert_eq!(dismissed["unread_count"], 2);
        assert_eq!(dismissed["notifications"][0]["is_dismissed"], true);
        assert_eq!(dismissed["notifications"][1]["is_dismissed"], false);
        assert_eq!(
            dismissed["notifications"][0]["message"],
            "Owned notice \u{2028}"
        );
        assert_eq!(
            dismissed["notifications"][0]["expires_at"],
            "2030-01-01T00:00:00Z"
        );
        let short = RestReadLimits {
            max_raw_bytes: 8,
            ..RestReadLimits::default()
        };
        let error = read(
            &mut database,
            admin,
            true,
            "2026-01-01T00:00:00Z".parse().unwrap(),
            short,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StoreError::StatusBody(503, _)));
        database.close().await.unwrap();
    }
}
