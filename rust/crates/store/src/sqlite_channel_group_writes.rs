//! Plain custom group writes on the existing copied SQLite runtime.
//! Original Dispatcharr authors and contributors retain application credit.
use super::super::{
    body_object, get_response, group_response, id, missing, name, write_error, Body,
    COLLECTION_ALLOW, DETAIL_ALLOW,
};
use super::{group_json, provider, truthy, StoreError, User, PROVIDERS};
use axum::{
    http::{Method, StatusCode},
    response::Response,
    Json,
};
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{Connection, Row, SqliteConnection};

pub(crate) enum Action {
    Detail(String, Method),
    Create(Body),
    Update(String, bool, Body),
    Delete(String),
}

const ROW: &str = "
    WITH members AS (
       SELECT c.user_level,c.is_adult FROM dispatcharr_channels_channel c
       LEFT JOIN dispatcharr_channels_channeloverride o ON o.channel_id=c.id
       WHERE COALESCE(o.channel_group_id,c.channel_group_id)=?1
    )
    SELECT g.name,(SELECT count(*) FROM members) AS channel_count,
       EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r
              JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id
              WHERE r.channel_group_id=g.id AND a.locked) AS is_custom,
       EXISTS(SELECT 1 FROM dispatcharr_channels_channelgroupm3uaccount r
              JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id
              WHERE r.channel_group_id=g.id AND NOT a.locked) AS has_provider
    FROM dispatcharr_channels_channelgroup g WHERE g.id=?1
       AND (?2 OR EXISTS(SELECT 1 FROM members WHERE user_level<=?3 AND (NOT ?4 OR NOT is_adult)))";
const INSERT: &str = "INSERT INTO dispatcharr_channels_channelgroup(name) VALUES(?1) RETURNING id";
const CUSTOM: &str = "SELECT id FROM m3u_m3uaccount WHERE name='custom' AND locked LIMIT 2";
const MARK: &str = "INSERT INTO dispatcharr_channels_channelgroupm3uaccount(
    channel_group_id,m3u_account_id,custom_properties,enabled,auto_channel_sync,
    auto_sync_channel_start,auto_sync_channel_end,last_seen,is_stale)
    VALUES(?1,?2,'{}',true,false,NULL,NULL,?3,false)";
const RENAME: &str = "UPDATE dispatcharr_channels_channelgroup SET name=?1 WHERE id=?2";
const DELETE: [&str; 5] = [
    "UPDATE dispatcharr_channels_channel SET channel_group_id=NULL WHERE channel_group_id=?1",
    "UPDATE dispatcharr_channels_channeloverride SET channel_group_id=NULL WHERE channel_group_id=?1",
    "UPDATE dispatcharr_channels_stream SET channel_group_id=NULL WHERE channel_group_id=?1",
    "DELETE FROM dispatcharr_channels_channelgroupm3uaccount WHERE channel_group_id=?1",
    "DELETE FROM dispatcharr_channels_channelgroup WHERE id=?1",
];
pub(crate) const STATEMENTS: [&str; 10] = [
    ROW, INSERT, CUSTOM, MARK, RENAME, DELETE[0], DELETE[1], DELETE[2], DELETE[3], DELETE[4],
];

async fn row(
    connection: &mut SqliteConnection,
    user: &User,
    key: i64,
) -> Result<sqlx::sqlite::SqliteRow, StoreError> {
    sqlx::query(ROW)
        .bind(key)
        .bind(user.user_level >= 10)
        .bind(user.user_level)
        .bind(truthy(user.custom_properties.get("hide_adult_content")))
        .fetch_optional(connection)
        .await?
        .ok_or_else(missing)
}

async fn detail(
    connection: &mut SqliteConnection,
    user: &User,
    key: i64,
) -> Result<Value, StoreError> {
    let group = row(connection, user, key).await?;
    let accounts = sqlx::query(PROVIDERS)
        .bind(key)
        .fetch_all(connection)
        .await?
        .into_iter()
        .map(provider)
        .collect::<Result<_, _>>()?;
    Ok(group_json(
        key,
        group.try_get("name")?,
        group.try_get("channel_count")?,
        group.try_get("is_custom")?,
        accounts,
    ))
}

fn plain_custom() -> StoreError {
    StoreError::Unsupported("SQLite copied runtime supports plain custom group writes")
}

pub(crate) async fn run(
    connection: &mut SqliteConnection,
    user: &User,
    action: Action,
) -> Result<Response, StoreError> {
    if let Action::Detail(raw, method) = action {
        let key = id(raw)?;
        let mut snapshot = connection.begin().await?;
        let result = detail(&mut snapshot, user, key).await;
        snapshot.commit().await?;
        return match result {
            Ok(value) => Ok(get_response(Json(value), method, DETAIL_ALLOW)),
            Err(error @ StoreError::StatusBody(404, _)) if method == Method::GET => {
                Ok(group_response(error, Some(DETAIL_ALLOW)))
            }
            Err(error) => Err(error),
        };
    }

    let mut tx = connection.begin_with("BEGIN IMMEDIATE").await?;
    let result = match action {
        Action::Create(body) => {
            let title = name(body_object(body)?, false)?.expect("required name validated");
            let custom = sqlx::query_scalar::<_, i64>(CUSTOM)
                .fetch_all(&mut *tx)
                .await?;
            let [custom] = custom.as_slice() else {
                return Err(plain_custom());
            };
            let key: i64 = sqlx::query_scalar(INSERT)
                .bind(title)
                .fetch_one(&mut *tx)
                .await
                .map_err(write_error)?;
            sqlx::query(MARK)
                .bind(key)
                .bind(custom)
                .bind(Utc::now().naive_utc())
                .execute(&mut *tx)
                .await?;
            group_response(
                (StatusCode::CREATED, Json(detail(&mut tx, user, key).await?)),
                Some(COLLECTION_ALLOW),
            )
        }
        Action::Update(raw, partial, body) => {
            let key = id(raw)?;
            let old = row(&mut tx, user, key).await?;
            let custom: bool = old.try_get("is_custom")?;
            if old.try_get::<bool, _>("has_provider")? {
                if custom {
                    return Err(plain_custom());
                }
                return Err(StoreError::InvalidInput(
                    json!({"error":"Cannot edit group with M3U account associations"}),
                ));
            }
            if !custom {
                return Err(plain_custom());
            }
            if let Some(title) = name(body_object(body)?, partial)? {
                sqlx::query(RENAME)
                    .bind(title)
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                    .map_err(write_error)?;
            }
            group_response(Json(detail(&mut tx, user, key).await?), Some(DETAIL_ALLOW))
        }
        Action::Delete(raw) => {
            let key = id(raw)?;
            let old = row(&mut tx, user, key).await?;
            if old.try_get::<i64, _>("channel_count")? != 0 {
                return Err(StoreError::InvalidInput(
                    json!({"error":"Cannot delete group with associated channels"}),
                ));
            }
            if old.try_get::<bool, _>("has_provider")? {
                return Err(StoreError::InvalidInput(
                    json!({"error":"Cannot delete group with M3U account associations"}),
                ));
            }
            if !old.try_get::<bool, _>("is_custom")? {
                return Err(plain_custom());
            }
            for query in DELETE {
                sqlx::query(query).bind(key).execute(&mut *tx).await?;
            }
            group_response(StatusCode::NO_CONTENT, Some(DETAIL_ALLOW))
        }
        Action::Detail(..) => unreachable!("detail handled before write transaction"),
    };
    tx.commit().await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    async fn json_response(response: Response, status: StatusCode, allow: &'static str) -> Value {
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[axum::http::header::ALLOW], allow);
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn body(title: &str) -> Body {
        Ok(Json(json!({"name": title})))
    }

    #[tokio::test]
    #[ignore = "requires disposable clones of the retained SQLite schema witnesses"]
    async fn ordinary_create_rename_reopen_delete_on_both_adapted_schemas() {
        let root = std::path::PathBuf::from(std::env::var_os("SQLITE_GROUP_OWNED_COPIES").unwrap());
        let user = User {
            id: 1,
            username: "fixture".into(),
            password: String::new(),
            is_active: true,
            user_level: 10,
            stream_limit: 0,
            custom_properties: json!({}),
        };
        for file in ["legacy.db", "fresh.db"] {
            let path = root.join(file);
            let open = || crate::sqlite_identity::SqliteIdentity::open_current_login_groups(&path);
            let initial = open().await.unwrap();
            let mut connection = initial.pool().acquire().await.unwrap();
            let before = super::super::collection(&mut connection, &user)
                .await
                .unwrap();
            let title = "Owned SQLite group 初回";
            let created = run(&mut connection, &user, Action::Create(body(title)))
                .await
                .unwrap();
            let value = json_response(created, StatusCode::CREATED, COLLECTION_ALLOW).await;
            let group_id = value["id"].as_i64().unwrap();
            let marker: (String, bool, bool, bool, bool, String) = sqlx::query_as(
                "SELECT a.name,a.locked,r.enabled,r.auto_channel_sync,r.is_stale,r.custom_properties
                 FROM dispatcharr_channels_channelgroupm3uaccount r
                 JOIN m3u_m3uaccount a ON a.id=r.m3u_account_id WHERE r.channel_group_id=?1",
            )
            .bind(group_id)
            .fetch_one(&mut *connection).await.unwrap();
            assert_eq!(
                marker,
                (
                    "custom".to_owned(),
                    true,
                    true,
                    false,
                    false,
                    "{}".to_owned()
                )
            );
            assert_eq!(
                value,
                group_json(group_id, title.to_owned(), 0, true, Vec::new())
            );
            let raw = group_id.to_string();
            assert!(matches!(
                run(&mut connection, &user, Action::Create(body(title))).await,
                Err(StoreError::InvalidInput(_))
            ));
            for (partial, value) in [
                (true, "Owned SQLite group updated"),
                (false, "Owned SQLite group reopened"),
            ] {
                let result = run(
                    &mut connection,
                    &user,
                    Action::Update(raw.clone(), partial, body(value)),
                )
                .await
                .unwrap();
                let typed = json_response(result, StatusCode::OK, DETAIL_ALLOW).await;
                assert_eq!(
                    typed,
                    group_json(group_id, value.to_owned(), 0, true, Vec::new())
                );
            }
            drop(connection);
            initial.close().await;
            let restarted = open().await.unwrap();
            let mut connection = restarted.pool().acquire().await.unwrap();
            let response = run(
                &mut connection,
                &user,
                Action::Detail(raw.clone(), Method::GET),
            )
            .await
            .unwrap();
            let value = json_response(response, StatusCode::OK, DETAIL_ALLOW).await;
            assert_eq!(value["name"], "Owned SQLite group reopened");
            let member = User {
                user_level: 1,
                ..user.clone()
            };
            let response = run(
                &mut connection,
                &member,
                Action::Detail(raw.clone(), Method::GET),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            let managed: i64 = sqlx::query_scalar("SELECT id FROM dispatcharr_channels_channelgroup WHERE name='Owned managed control'")
                .fetch_one(&mut *connection).await.unwrap();
            let managed = managed.to_string();
            assert!(matches!(
                run(
                    &mut connection,
                    &user,
                    Action::Update(managed.clone(), true, body("Other"))
                )
                .await,
                Err(StoreError::InvalidInput(_))
            ));
            assert!(matches!(
                run(&mut connection, &user, Action::Delete(managed)).await,
                Err(StoreError::InvalidInput(_))
            ));
            let response = run(&mut connection, &user, Action::Delete(raw.clone()))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NO_CONTENT);
            assert_eq!(response.headers()[axum::http::header::ALLOW], DETAIL_ALLOW);
            let response = run(&mut connection, &user, Action::Detail(raw, Method::GET))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
            assert_eq!(
                super::super::collection(&mut connection, &user)
                    .await
                    .unwrap(),
                before
            );
            drop(connection);
            restarted.close().await;
        }
    }
}
