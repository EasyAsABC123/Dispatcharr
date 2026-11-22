//! Copied SQLite account profile and administrator reads. Original Dispatcharr authors retain credit.
use super::*;
use axum::{
    body::{Body, HttpBody},
    extract::{FromRequest, Request},
    http::{header, HeaderValue, Method},
};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

const USER: &str = "SELECT id,username,api_key,email,user_level,custom_properties,avatar_config,
    stream_limit,is_staff,is_superuser,last_login,date_joined,first_name,last_name
    FROM accounts_user WHERE id=?";
const USERS: &str =
    "SELECT u.id,u.username,u.api_key,u.email,u.user_level,u.custom_properties,u.avatar_config,
    u.stream_limit,u.is_staff,u.is_superuser,u.last_login,u.date_joined,u.first_name,u.last_name,
    (SELECT json_group_array(channelprofile_id) FROM (
        SELECT channelprofile_id FROM accounts_user_channel_profiles p
        WHERE p.user_id=u.id ORDER BY channelprofile_id LIMIT 65537
    )) AS channel_profiles
    FROM accounts_user u ORDER BY u.id";
const PROFILES: &str = "SELECT channelprofile_id FROM accounts_user_channel_profiles
    WHERE user_id=? ORDER BY channelprofile_id LIMIT 65537";
const PATCH: &str = "UPDATE accounts_user SET
    first_name=COALESCE(?1,first_name),last_name=COALESCE(?2,last_name),email=COALESCE(?3,email),
    custom_properties=CASE WHEN ?4 THEN ?5 ELSE custom_properties END,
    password=COALESCE(?6,password) WHERE id=?7";
const PROPS: &str = "SELECT custom_properties FROM accounts_user WHERE id=?";
pub(crate) const STATEMENTS: [&str; 5] = [USER, USERS, PROFILES, PATCH, PROPS];

async fn load(db: &mut SqliteConnection, id: i64) -> Result<Value, StoreError> {
    let row = sqlx::query(USER)
        .bind(id)
        .fetch_optional(&mut *db)
        .await?
        .ok_or(StoreError::NotFound)?;
    let profiles: Vec<i64> = sqlx::query_scalar(PROFILES).bind(id).fetch_all(db).await?;
    serialize(row, profiles)
}

async fn load_users(db: &mut SqliteConnection) -> Result<Value, StoreError> {
    let rows = sqlx::query(USERS).fetch_all(db).await?;
    let values = rows
        .into_iter()
        .map(|row| {
            let profiles: sqlx::types::Json<Vec<i64>> = row.try_get("channel_profiles")?;
            serialize(row, profiles.0)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(values))
}

fn serialize(row: SqliteRow, profiles: Vec<i64>) -> Result<Value, StoreError> {
    if profiles.len() > 65_536 {
        return Err(StoreError::Unsupported(
            "Copied SQLite profile exceeds the supported size",
        ));
    }
    let json_field = |name| -> Result<Value, StoreError> {
        Ok(row
            .try_get::<Option<sqlx::types::Json<Value>>, _>(name)?
            .map_or(Value::Null, |value| value.0))
    };
    Ok(json!({
        "id": row.try_get::<i64,_>("id")?, "username": row.try_get::<String,_>("username")?,
        "api_key": row.try_get::<Option<String>,_>("api_key")?, "email": row.try_get::<String,_>("email")?,
        "user_level": row.try_get::<i32,_>("user_level")?, "channel_profiles": profiles,
        "custom_properties": json_field("custom_properties")?, "avatar_config": json_field("avatar_config")?,
        "stream_limit": row.try_get::<i32,_>("stream_limit")?, "is_staff": row.try_get::<bool,_>("is_staff")?,
        "is_superuser": row.try_get::<bool,_>("is_superuser")?,
        "last_login": crate::accounts::datetime(row.try_get("last_login")?),
        "date_joined": crate::accounts::datetime(row.try_get("date_joined")?),
        "first_name": row.try_get::<String,_>("first_name")?, "last_name": row.try_get::<String,_>("last_name")?
    }))
}

async fn update(runtime: &CopiedSqliteRuntime, id: i64, body: Value) -> Result<Value, StoreError> {
    let fields = crate::account_writes::owner_patch(body)?;
    let password = match fields.get("password").and_then(Value::as_str) {
        Some(value) => Some(hash_for_workers(&runtime.password_workers, value.to_owned()).await?),
        None => None,
    };
    let mut tx = runtime
        .identity
        .pool()
        .begin_with("BEGIN IMMEDIATE")
        .await?;
    let changes_properties = fields.contains_key("custom_properties");
    let properties = if changes_properties {
        let existing: Option<sqlx::types::Json<Value>> = sqlx::query_scalar(PROPS)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(StoreError::NotFound)?;
        crate::account_writes::merged_properties(
            existing.map(|row| row.0),
            fields.get("custom_properties"),
        )
        .map(sqlx::types::Json)
    } else {
        None
    };
    let updated = sqlx::query(PATCH)
        .bind(fields.get("first_name").and_then(Value::as_str))
        .bind(fields.get("last_name").and_then(Value::as_str))
        .bind(fields.get("email").and_then(Value::as_str))
        .bind(changes_properties)
        .bind(properties)
        .bind(password)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    if updated.rows_affected() != 1 {
        return Err(StoreError::NotFound);
    }
    let result = load(&mut tx, id).await?;
    tx.commit().await?;
    Ok(result)
}

fn wire(mut response: Response, allow: &'static str, head: bool) -> Response {
    let length = response.body().size_hint().exact();
    response
        .headers_mut()
        .insert(header::ALLOW, HeaderValue::from_static(allow));
    response
        .headers_mut()
        .insert(header::VARY, HeaderValue::from_static("Accept, origin"));
    if let Some(length) = length {
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string()).expect("bounded decimal response length"),
        );
    }
    if head {
        *response.body_mut() = Body::empty();
    }
    response
}

pub(super) async fn users(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let result: Result<Response, StoreError> = async {
        let held = runtime.allocate_read().await?;
        held.bearer_account_user(peer, headers, true).await?;
        let mut connection = runtime.identity.pool().acquire().await?;
        Ok(Json(load_users(&mut connection).await?).into_response())
    }
    .await;
    wire(
        result.unwrap_or_else(IntoResponse::into_response),
        "GET, POST, HEAD, OPTIONS",
        method == Method::HEAD,
    )
}

pub(super) async fn me(
    State(runtime): State<CopiedSqliteRuntime>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    incoming: Request,
) -> Response {
    let method = incoming.method().clone();
    let result = async {
        let held = runtime.allocate()?;
        // DRF uses a separate metadata action, restricted to administrators.
        let admin = !matches!(method, Method::GET | Method::HEAD | Method::PATCH);
        let subject = held.bearer_account_user(peer, incoming.headers().clone(), admin).await?;
        let id = subject.user().id;
        match method {
            Method::GET | Method::HEAD => {
                let mut transaction = runtime.identity.pool().begin().await?;
                let result = load(&mut transaction, id).await?;
                transaction.commit().await?;
                Ok(Json(result).into_response())
            }
            Method::PATCH => {
                let body = Json::<Value>::from_request(incoming, &()).await;
                let Json(body) = body.map_err(|_| StoreError::InvalidInput(json!({"detail":"JSON parse error"})))?;
                if !body.is_object() {
                    return Ok(crate::account_writes::nonobject_profile_patch());
                }
                Ok(Json(update(&runtime, id, body).await?).into_response())
            }
            Method::OPTIONS => Ok(Json(json!({
                "name": "Me", "description": "Handles CRUD operations for Users",
                "renders": ["application/json","text/html"],
                "parses": ["application/json","application/x-www-form-urlencoded","multipart/form-data"]
            })).into_response()),
            _ => Err(StoreError::StatusBody(405, json!({"detail":format!("Method \"{method}\" not allowed.")}))),
        }
    }.await;
    wire(
        result.unwrap_or_else(IntoResponse::into_response),
        "GET, PATCH, HEAD, OPTIONS",
        method == Method::HEAD,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    #[ignore = "requires an owned copy of the retained current synthetic SQLite fixture"]
    async fn admin_users_projection_reuses_self_serializer_and_head_shape() {
        let path =
            std::path::PathBuf::from(std::env::var_os("SQLITE_ADMIN_USERS_TEST_COPY").unwrap());
        let identity = crate::sqlite_identity::SqliteIdentity::open_current_login_groups(&path)
            .await
            .unwrap();
        let mut connection = identity.pool().acquire().await.unwrap();
        let serialized = load_users(&mut connection).await.unwrap();
        let users = serialized.as_array().unwrap();
        assert!(users.len() >= 3);
        assert!(users.iter().any(|row| row["user_level"] == 10));
        assert!(users.iter().any(|row| row["user_level"] == 1));
        assert!(users.iter().any(|row| row["channel_profiles"]
            .as_array()
            .is_some_and(|p| p.len() == 2)));
        assert!(users.iter().any(|row| row["avatar_config"].is_null()));
        assert!(users
            .iter()
            .all(|row| row.as_object().unwrap().len() == 15 && row.get("password").is_none()));
        let ids: Vec<_> = users
            .iter()
            .map(|row| row["id"].as_i64().unwrap())
            .collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
        for (id, expected) in ids.into_iter().zip(users) {
            assert_eq!(load(&mut connection, id).await.unwrap(), *expected);
        }
        let allow = "GET, POST, HEAD, OPTIONS";
        let get = wire(Json(serialized.clone()).into_response(), allow, false);
        let head = wire(Json(serialized).into_response(), allow, true);
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(get.status(), head.status());
        assert_eq!(get.headers(), head.headers());
        assert_eq!(head.headers()[header::ALLOW], allow);
        assert_eq!(head.headers()[header::VARY], "Accept, origin");
        let encoded = to_bytes(get.into_body(), 65536).await.unwrap();
        assert_eq!(
            head.headers()[header::CONTENT_LENGTH],
            encoded.len().to_string()
        );
        assert!(to_bytes(head.into_body(), 0).await.unwrap().is_empty());
        drop(connection);
        identity.close().await;
    }
}
