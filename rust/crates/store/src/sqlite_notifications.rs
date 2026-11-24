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

pub(crate) async fn dismiss_all(
    pool: &SqlitePool,
    actor: &User,
    targets: Vec<Value>,
    limits: RestReadLimits,
) -> Result<Value, StoreError> {
    let count = targets.len();
    if count == 0 {
        return Ok(json!({"success": true, "dismissed_count": 0}));
    }

    // Check the same actor and selected targets after external condition
    // evaluation, with exclusive writer intent for the entire operation.
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    let level = sqlx::query_scalar::<_, i32>(
        "SELECT user_level FROM accounts_user WHERE id=?1 AND is_active",
    )
    .bind(actor.id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(missing)?;
    let current = read(
        &mut tx,
        actor.id,
        actor.user_level >= 10 && level >= 10,
        Utc::now(),
        limits,
    )
    .await?;
    let current = current
        .iter()
        .map(|row| Ok((row["id"].as_i64().ok_or_else(failed)?, row)))
        .collect::<Result<std::collections::HashMap<_, _>, StoreError>>()?;

    let mut pending = Vec::new();
    for selected in targets {
        let id = selected["id"].as_i64().ok_or_else(failed)?;
        let current = current.get(&id).ok_or_else(missing)?;
        if ["notification_key", "source", "action_data"]
            .iter()
            .any(|field| current[*field] != selected[*field])
        {
            return Err(missing());
        }
        if current["is_dismissed"] != true {
            let clock = Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            pending.push((id, clock));
        }
    }
    if !pending.is_empty() {
        let records = serde_json::to_string(&pending).map_err(|_| failed())?;
        sqlx::query(
            "INSERT INTO core_notificationdismissal(user_id,notification_id,dismissed_at,action_taken)
             SELECT ?1,json_extract(value,'$[0]'),json_extract(value,'$[1]'),NULL
             FROM json_each(?2) ORDER BY key",
        )
        .bind(actor.id)
        .bind(records)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    // Django reports the number selected before bulk_create(ignore_conflicts=True).
    Ok(json!({"success": true, "dismissed_count": count}))
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

pub(crate) async fn visible_rows<F, Fut>(
    rows: Vec<Value>,
    include_dismissed: bool,
    developer_visible: F,
) -> Result<Vec<Value>, StoreError>
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
    Ok(matching)
}

pub(crate) async fn unread_count<F, Fut>(
    rows: Vec<Value>,
    developer_visible: F,
) -> Result<usize, StoreError>
where
    F: Fn(i64, Value) -> Fut,
    Fut: std::future::Future<Output = Result<bool, StoreError>>,
{
    Ok(visible_rows(rows, false, developer_visible).await?.len())
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
    let matching = visible_rows(rows, include_dismissed, developer_visible).await?;
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
               expires_at DATETIME, notification_type TEXT NOT NULL DEFAULT 'info',
               priority TEXT NOT NULL DEFAULT 'normal', title TEXT NOT NULL DEFAULT 'Test title',
               message TEXT NOT NULL DEFAULT 'Test message',
               created_at DATETIME NOT NULL DEFAULT '2025-01-01 00:00:00'
             );
             CREATE TABLE core_notificationdismissal(
               id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL REFERENCES accounts_user(id),
               notification_id INTEGER NOT NULL REFERENCES core_systemnotification(id),
               dismissed_at DATETIME NOT NULL, action_taken VARCHAR(50), UNIQUE(user_id,notification_id)
             );
             INSERT INTO accounts_user VALUES(1,1,1),(2,10,1),(3,1,0);
             INSERT INTO core_systemnotification(id,notification_key,source,action_data,is_active,admin_only,expires_at)
             VALUES (11,'owned-public','system','{}',1,0,NULL),
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

    async fn bulk_targets(pool: &SqlitePool, user: &User) -> Vec<Value> {
        let records = read(
            &mut pool.acquire().await.unwrap(),
            user.id,
            user.user_level >= 10,
            Utc::now(),
            RestReadLimits::default(),
        )
        .await
        .unwrap();
        visible_rows(records, false, |id, _| async move { Ok(id == 16) })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn bulk_dismiss_checks_actor_visibility_and_preserves_existing_rows() {
        let pool = owned_dismissals().await;
        sqlx::query(
            r#"INSERT INTO core_systemnotification(id,notification_key,source,action_data,is_active,admin_only)
               VALUES (16,'owned-visible-condition','developer','{"condition":["local"]}',1,0)"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        let user = actor(1, 1);
        let admin = actor(2, 10);
        let existing = saved(&pool).await;
        let limits = RestReadLimits::default();
        let selected = bulk_targets(&pool, &user).await;
        assert_eq!(selected.len(), 2);
        assert_eq!(
            dismiss_all(&pool, &user, selected, limits).await.unwrap(),
            json!({"success": true, "dismissed_count": 2})
        );
        let first = saved(&pool).await;
        assert_eq!(first[0], existing[0]);
        assert_eq!(first.len(), 3);
        assert_eq!(
            first[1..]
                .iter()
                .map(|row| row.2)
                .collect::<std::collections::BTreeSet<_>>(),
            [11, 16].into_iter().collect()
        );
        assert!(first[1..].iter().all(|row| row.1 == user.id
            && row.4.is_none()
            && NaiveDateTime::parse_from_str(&row.3, "%Y-%m-%d %H:%M:%S%.f").is_ok()));
        let repeated = bulk_targets(&pool, &user).await;
        assert_eq!(
            dismiss_all(&pool, &user, repeated, limits).await.unwrap(),
            json!({"success": true, "dismissed_count": 0})
        );
        assert_eq!(saved(&pool).await, first);
        assert_eq!(seq(&pool).await, first.last().unwrap().0);
        let admin_visible = bulk_targets(&pool, &admin).await;
        assert_eq!(
            dismiss_all(&pool, &admin, admin_visible, limits)
                .await
                .unwrap(),
            json!({"success": true, "dismissed_count": 2})
        );
        let final_rows = saved(&pool).await;
        assert_eq!(final_rows[..3], first);
        assert_eq!(
            final_rows[3..]
                .iter()
                .map(|row| (row.1, row.2))
                .collect::<std::collections::BTreeSet<_>>(),
            [(2, 12), (2, 16)].into_iter().collect()
        );
    }

    async fn seq(pool: &SqlitePool) -> i64 {
        sqlx::query_scalar(
            "SELECT seq FROM sqlite_sequence WHERE name='core_notificationdismissal'",
        )
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn bulk_dismiss_revalidates_before_any_insert_and_preserves_same_actor_winner() {
        let pool = owned_dismissals().await;
        let user = actor(1, 1);
        let limits = RestReadLimits::default();
        let original = saved(&pool).await;
        let selected = bulk_targets(&pool, &user).await;
        assert_eq!(selected.len(), 1);
        sqlx::query("UPDATE accounts_user SET is_active=0 WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            dismiss_all(&pool, &user, selected, limits).await,
            Err(StoreError::StatusBody(404, _))
        ));
        assert_eq!(saved(&pool).await, original);
        assert_eq!(seq(&pool).await, 1);
        sqlx::query("UPDATE accounts_user SET is_active=1 WHERE id=1")
            .execute(&pool)
            .await
            .unwrap();
        let selected = bulk_targets(&pool, &user).await;
        act(&pool, 11, &user, br#"{"action_taken":"kept"}"#)
            .await
            .unwrap();
        let winner = saved(&pool).await;
        assert_eq!(
            dismiss_all(&pool, &user, selected, limits).await.unwrap(),
            json!({"success": true, "dismissed_count": 1})
        );
        assert_eq!(saved(&pool).await, winner);
        assert_eq!(seq(&pool).await, 2);
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
    async fn unread_count_respects_dismissal_visibility_and_copied_schema() {
        let mut database = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE accounts_user(id integer PRIMARY KEY)")
            .execute(&mut database)
            .await
            .unwrap();
        let schema = include_str!("../datasets/sqlite-core-notifications.sql");
        for line in schema.lines() {
            sqlx::query(line).execute(&mut database).await.unwrap();
        }
        for (id, source, conditions, active, admin, expires) in [
            (11, "system", "{}", true, false, None),
            (12, "system", "{}", true, true, None),
            (13, "system", "{}", true, false, Some("2020-01-01 00:00:00")),
            (14, "system", "{}", false, false, None),
            (
                15,
                "developer",
                r#"{"condition":["local"]}"#,
                true,
                false,
                None,
            ),
            (
                16,
                "developer",
                r#"{"condition":["local"]}"#,
                true,
                false,
                None,
            ),
        ] {
            sqlx::query(
                "INSERT INTO core_systemnotification VALUES (
                    ?1,?2,'system','low',?3,'Title','Message',?4,?5,?6,?7,
                    '2025-12-01 00:00:00','2025-12-01 00:00:00'
                )",
            )
            .bind(id)
            .bind(format!("owned-{id}"))
            .bind(source)
            .bind(conditions)
            .bind(active)
            .bind(admin)
            .bind(expires)
            .execute(&mut database)
            .await
            .unwrap();
        }
        sqlx::raw_sql(
            "INSERT INTO accounts_user VALUES (1),(2);
             INSERT INTO core_notificationdismissal(
                 dismissed_at,user_id,notification_id
             ) VALUES ('2025-12-01 00:00:00',1,11);",
        )
        .execute(&mut database)
        .await
        .unwrap();
        let now = "2026-01-01T00:00:00Z".parse().unwrap();
        let limits = RestReadLimits::default();
        for (user_id, admin, expected) in [(1, false, 1), (2, true, 3)] {
            let rows = read(&mut database, user_id, admin, now, limits)
                .await
                .unwrap();
            let count = unread_count(rows, |id, conditions| async move {
                assert_eq!(conditions, json!(["local"]));
                assert!(matches!(id, 15 | 16));
                Ok(id == 16)
            })
            .await
            .unwrap();
            assert_eq!(count, expected);
        }
        let dismissals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core_notificationdismissal")
            .fetch_one(&mut database)
            .await
            .unwrap();
        assert_eq!(dismissals, 1);
        database.close().await.unwrap();
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
