//! Copied SQLite notification collection used by the original React Sidebar.
//! Original Dispatcharr contributors retain full application credit.
use super::*;
use crate::RestReadLimits;
use chrono::NaiveDateTime;
use futures_util::TryStreamExt;
use sqlx::{SqliteConnection, SqlitePool};

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

const DISMISS_TARGET: &str = "
    SELECT CASE WHEN length(CAST(n.notification_key AS BLOB)) <= ?5 THEN n.notification_key END,
           n.source,
           CASE WHEN n.source != 'developer' THEN '{}'
                WHEN length(CAST(n.action_data AS BLOB)) <= ?5 THEN n.action_data END
    FROM core_systemnotification n
    WHERE n.id=?1 AND n.is_active AND (n.expires_at IS NULL OR n.expires_at > ?4)
      AND EXISTS(SELECT 1 FROM accounts_user a WHERE a.id=?2 AND a.is_active
                 AND (NOT n.admin_only OR (?3 AND a.user_level >= 10)))";

#[derive(PartialEq, Eq)]
pub(crate) struct DismissTarget {
    key: String,
    source: String,
    action_data: String,
}

pub(crate) async fn dismiss_target(
    connection: &mut SqliteConnection,
    id: i64,
    actor: &User,
    now: DateTime<Utc>,
    limits: RestReadLimits,
) -> Result<DismissTarget, StoreError> {
    let (key, source, action_data) =
        sqlx::query_as::<_, (Option<String>, String, Option<String>)>(DISMISS_TARGET)
            .bind(id)
            .bind(actor.id)
            .bind(actor.user_level >= 10)
            .bind(now.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .bind(limits.max_raw_bytes as i64)
            .fetch_optional(connection)
            .await?
            .ok_or_else(missing)?;
    let (Some(key), Some(action_data)) = (key, action_data) else {
        return Err(capacity());
    };
    Ok(DismissTarget {
        key,
        source,
        action_data,
    })
}

impl DismissTarget {
    pub(crate) fn conditions(&self) -> Result<Option<Value>, StoreError> {
        if self.source != "developer" {
            return Ok(None);
        }
        let action: Value = serde_json::from_str(&self.action_data).map_err(|_| failed())?;
        Ok(visible_conditions(&self.source, &action)?.cloned())
    }
}

fn visible_conditions<'a>(
    source: &str,
    action: &'a Value,
) -> Result<Option<&'a Value>, StoreError> {
    if source != "developer" || !truthy(Some(action)) {
        return Ok(None);
    }
    let action = action.as_object().ok_or_else(failed)?;
    Ok(action.get("condition").filter(|value| truthy(Some(value))))
}

pub(crate) async fn dismiss(
    pool: &SqlitePool,
    id: i64,
    actor: &User,
    previously_visible: DismissTarget,
    limits: RestReadLimits,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Value, StoreError> {
    let action = dismissal_action(headers, body)?;
    // The shared Redis condition check runs before acquiring the SQLite writer.
    // Refuse any target revision or access change observed before this write.
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let current = dismiss_target(&mut tx, id, actor, Utc::now(), limits).await?;
    if current != previously_visible {
        return Err(missing());
    }
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM core_notificationdismissal WHERE user_id=?1 AND notification_id=?2)",
    )
    .bind(actor.id)
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if !exists {
        sqlx::query(
            "INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken) VALUES(?1,?2,?3,?4)",
        )
        .bind(actor.id)
        .bind(id)
        .bind(Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .bind(action.text)
        .execute(&mut *tx)
        .await?;
    } else if action.replace_existing {
        sqlx::query(
            "UPDATE core_notificationdismissal SET action_taken=?3 WHERE user_id=?1 AND notification_id=?2",
        )
        .bind(actor.id)
        .bind(id)
        .bind(action.text)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(dismissal_response(&Value::String(current.key)))
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
        if let Some(conditions) = visible_conditions(
            row["source"].as_str().unwrap_or_default(),
            &row["action_data"],
        )? {
            let id = row["id"].as_i64().ok_or_else(failed)?;
            if !developer_visible(id, conditions.clone()).await? {
                continue;
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

    fn actor(id: i64, user_level: i32) -> User {
        User {
            id,
            username: format!("owned-{id}"),
            password: String::new(),
            user_level,
            is_active: true,
            stream_limit: 0,
            custom_properties: json!({}),
        }
    }

    async fn owned_dismissals() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE accounts_user(id INTEGER PRIMARY KEY, user_level INTEGER, is_active BOOLEAN);
             CREATE TABLE core_systemnotification(
               id INTEGER PRIMARY KEY, notification_key TEXT NOT NULL UNIQUE, source TEXT NOT NULL,
               action_data TEXT NOT NULL, is_active BOOLEAN NOT NULL, admin_only BOOLEAN NOT NULL,
               expires_at DATETIME
             );
             CREATE TABLE core_notificationdismissal(
               id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES accounts_user(id),
               notification_id INTEGER NOT NULL REFERENCES core_systemnotification(id),
               dismissed_at DATETIME NOT NULL, action_taken VARCHAR(50), UNIQUE(user_id,notification_id)
             );
             INSERT INTO accounts_user VALUES(1,1,1),(2,10,1),(3,1,0);
             INSERT INTO core_systemnotification VALUES
               (11,'owned-public','system','{}',1,0,NULL),
               (12,'owned-admin','system','{}',1,1,NULL),
               (13,'owned-expired','system','{}',1,0,'2020-01-01 00:00:00'),
               (14,'owned-disabled','system','{}',0,0,NULL),
               (15,'owned-condition','developer','{\"condition\":[\"local\"]}',1,0,NULL);
             INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken)
               VALUES(2,11,'2025-01-01 00:00:00','other-user');",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    async fn target(pool: &SqlitePool, id: i64, user: &User) -> Result<DismissTarget, StoreError> {
        let mut conn = pool.acquire().await.unwrap();
        dismiss_target(&mut conn, id, user, Utc::now(), RestReadLimits::default()).await
    }

    type DismissalRow = (i64, i64, i64, String, Option<String>);

    async fn saved(pool: &SqlitePool) -> Vec<DismissalRow> {
        sqlx::query_as(
            "SELECT id,user_id,notification_id,dismissed_at,action_taken
             FROM core_notificationdismissal ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn act(
        pool: &SqlitePool,
        id: i64,
        user: &User,
        body: &[u8],
    ) -> Result<Value, StoreError> {
        let headers = HeaderMap::from_iter([(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        )]);
        dismiss(
            pool,
            id,
            user,
            target(pool, id, user).await?,
            RestReadLimits::default(),
            &headers,
            body,
        )
        .await
    }

    #[tokio::test]
    async fn single_dismiss_is_actor_isolated_and_repeats_keep_row_identity() {
        let pool = owned_dismissals().await;
        let standard = actor(1, 1);
        let before = saved(&pool).await;
        let (a, b) = tokio::join!(
            act(&pool, 11, &standard, br#"{}"#),
            act(&pool, 11, &standard, br#"{}"#),
        );
        let expected = json!({"success":true,"message":"Notification dismissed","notification_key":"owned-public"});
        assert_eq!(a.unwrap(), expected);
        assert_eq!(b.unwrap(), expected);
        let first = saved(&pool).await;
        assert_eq!(first.len(), 2);
        assert_eq!(first[0], before[0]);
        assert_eq!((first[1].0, first[1].1, first[1].2), (2, 1, 11));
        assert!(first[1].4.is_none());
        assert!(NaiveDateTime::parse_from_str(&first[1].3, "%Y-%m-%d %H:%M:%S%.f").is_ok());
        assert_eq!(
            act(&pool, 11, &standard, br#"{"action_taken":"dismissed"}"#)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            act(&pool, 11, &standard, br#"{"action_taken":""}"#)
                .await
                .unwrap(),
            expected
        );
        let after = saved(&pool).await;
        let mut updated = first;
        updated[1].4 = Some("dismissed".into());
        assert_eq!(after, updated);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT seq FROM sqlite_sequence WHERE name='core_notificationdismissal'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            2,
        );
    }

    #[tokio::test]
    async fn single_dismiss_revalidates_visible_target_before_sql() {
        let pool = owned_dismissals().await;
        let standard = actor(1, 1);
        let admin = actor(2, 10);
        let before = saved(&pool).await;
        for id in [12, 13, 14, 16] {
            assert!(matches!(
                target(&pool, id, &standard).await,
                Err(StoreError::StatusBody(404, _))
            ));
        }
        assert!(matches!(
            target(&pool, 11, &actor(3, 1)).await,
            Err(StoreError::StatusBody(404, _))
        ));
        assert!(target(&pool, 12, &admin).await.is_ok());
        let approved = target(&pool, 15, &standard).await.unwrap();
        assert_eq!(approved.conditions().unwrap(), Some(json!(["local"])));
        sqlx::query("UPDATE core_systemnotification SET action_data='{}' WHERE id=15")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            dismiss(
                &pool,
                15,
                &standard,
                approved,
                RestReadLimits::default(),
                &HeaderMap::new(),
                b""
            )
            .await,
            Err(StoreError::StatusBody(404, _))
        ));
        assert_eq!(saved(&pool).await, before);
        act(&pool, 12, &admin, br#"{}"#).await.unwrap();
        assert_eq!(saved(&pool).await.len(), 2);
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
